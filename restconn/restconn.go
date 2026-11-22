// Package restconn 是把 PowerSync 上传队列写进 PostgREST 的连接器（可直接用于你的项目）。
//
// 语义与官方 Supabase 连接器一致：
//
//	PUT    → POST   ?resolution=merge-duplicates   (upsert)
//	PATCH  → PATCH  ?id=eq.<id>
//	DELETE → DELETE ?id=eq.<id>
//
// 每批上传成功后取一次 legacy write checkpoint（/write-checkpoint2.json）再 Complete，
// 这样服务端会把本地写回显给客户端，cursor 继续前进。
//
// 用法：
//
//	conn := restconn.New(restconn.Config{Endpoint: ..., RPC: ..., Box: ..., Org: ..., RESTBase: ...})
//	db, _ := powersync.Open(ctx, powersync.Options{Filename: "app.db", Schema: sch, Connector: conn})
//
// 它在 SDK 内部被 examples/todo、examples/demo、cmd/pstable、examples/wailsapp 共用，
// 也是接入自建 PostgREST 后端的最小参考实现。
package restconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	powersync "github.com/zhuweizz/powersync_golang"
	"github.com/zhuweizz/powersync_golang/internal/psclient"
)

// Config 是连接器参数（与 examples/todo 的 -endpoint/-rpc/-box/-org/-rest 对应）。
type Config struct {
	Endpoint string // PowerSync 服务地址，如 http://host:8080
	RPC      string // fetch_credentials 的 RPC 地址
	Box      string // box_code
	Org      string // org_code
	RESTBase string // PostgREST base，如 http://host:8089/rest
	Logger   *slog.Logger
	Client   *http.Client // 可空，默认 http.DefaultClient

	// BatchSize 每批最多上传多少条（0 → 50）。
	BatchSize int
}

// Connector 实现 powersync.Connector：RPC 取 token + PostgREST 执行 CRUD。
type Connector struct {
	cfg   Config
	rest  *psclient.REST
	token string
}

// New 按配置创建连接器。
func New(cfg Config) *Connector {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	return &Connector{
		cfg:  cfg,
		rest: &psclient.REST{Base: strings.TrimRight(cfg.RESTBase, "/"), Client: cfg.Client},
	}
}

// REST 暴露底层 PostgREST 客户端（示例用它在后台核对数据）。
func (c *Connector) REST() *psclient.REST { return c.rest }

// FetchCredentials 通过 RPC 换 token（缓存，token 失效时库会再次调用）。
func (c *Connector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	if c.token == "" {
		tok, err := psclient.FetchToken(ctx, c.cfg.RPC, c.cfg.Box, c.cfg.Org)
		if err != nil {
			return powersync.Credentials{}, err
		}
		c.token = tok
		c.rest.Token = tok
	}
	return powersync.Credentials{Endpoint: strings.TrimRight(c.cfg.Endpoint, "/"), Token: c.token}, nil
}

// EnsureToken 确保已拿到 token（首次上传可能早于同步连接建立，此时 REST.Token 还是空）。
func (c *Connector) EnsureToken(ctx context.Context) error {
	if c.rest.Token != "" {
		return nil
	}
	_, err := c.FetchCredentials(ctx)
	return err
}

// UploadData 把 ps_crud 里的变更按顺序写到 PostgREST。
func (c *Connector) UploadData(ctx context.Context, db *powersync.DB) error {
	// 上传循环可能比首次 FetchCredentials 更早开始（例如启动时队列里已有积压），
	// 必须先拿 token，否则 PostgREST 会用 anon 角色，报 401 permission denied。
	if err := c.EnsureToken(ctx); err != nil {
		return err
	}
	batch, err := db.GetCrudBatch(ctx, c.cfg.BatchSize)
	if err != nil || batch == nil {
		return err
	}
	for _, e := range batch.Entries {
		if err := c.apply(ctx, e); err != nil {
			var httpErr *psclient.HTTPError
			if errors.As(err, &httpErr) {
				// 401/403 多半是 token 还没拿到/已过期：重试，别把数据丢掉。
				if httpErr.Status == http.StatusUnauthorized || httpErr.Status == http.StatusForbidden {
					c.token = ""
					c.rest.Token = ""
					return fmt.Errorf("上传 %s %s/%s: %w（已重置 token，下轮重试）", e.Op, e.Table, e.ID, err)
				}
				if httpErr.Fatal() {
					// 其它 4xx（id 不是 uuid、NOT NULL、唯一约束…）重试也没用：
					// 记录后丢弃这一条，避免堵死整个队列（生产环境建议落到自己的错误表）。
					c.cfg.Logger.Warn("上传被后端拒绝，已跳过",
						"op", e.Op, "table", e.Table, "id", e.ID, "err", err)
					continue
				}
			}
			return fmt.Errorf("上传 %s %s/%s: %w", e.Op, e.Table, e.ID, err)
		}
	}
	wc, err := c.writeCheckpoint(ctx, db)
	if err != nil {
		return err
	}
	return batch.Complete(ctx, wc)
}

func (c *Connector) apply(ctx context.Context, e powersync.CrudEntry) error {
	switch e.Op {
	case powersync.UpdateTypePut:
		row := make(map[string]any, len(e.Data)+1)
		for k, v := range e.Data {
			row[k] = v
		}
		row["id"] = e.ID
		return c.rest.Do(ctx, http.MethodPost, tablePath(e.Table), row,
			"resolution=merge-duplicates,return=minimal")
	case powersync.UpdateTypePatch:
		if len(e.Data) == 0 {
			return nil
		}
		return c.rest.Do(ctx, http.MethodPatch,
			tablePath(e.Table)+"?id=eq."+url.QueryEscape(e.ID), e.Data, "return=minimal")
	case powersync.UpdateTypeDelete:
		return c.rest.Do(ctx, http.MethodDelete,
			tablePath(e.Table)+"?id=eq."+url.QueryEscape(e.ID), nil, "return=minimal")
	default:
		return fmt.Errorf("未知 op %q", e.Op)
	}
}

// writeCheckpoint 取 legacy write checkpoint（服务端据此把本地写回显）。
func (c *Connector) writeCheckpoint(ctx context.Context, db *powersync.DB) (string, error) {
	clientID, err := db.ClientID(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(c.cfg.Endpoint, "/")+"/write-checkpoint2.json?client_id="+url.QueryEscape(clientID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Token "+c.token)
	req.Header.Set("Accept", "application/json")

	client := c.cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return "", fmt.Errorf("write-checkpoint2 HTTP %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		Data struct {
			WriteCheckpoint string `json:"write_checkpoint"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Data.WriteCheckpoint, nil
}

// WaitQueueEmpty 等到上传队列清空（超时返回剩余条数）。
func WaitQueueEmpty(ctx context.Context, db *powersync.DB, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		n, err := db.UploadQueue(ctx)
		if err != nil {
			return n, err
		}
		if n == 0 {
			return 0, nil
		}
		if time.Now().After(deadline) {
			return n, fmt.Errorf("上传队列在 %s 内仍有 %d 条", timeout, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tablePath(table string) string { return "/" + url.PathEscape(table) }
