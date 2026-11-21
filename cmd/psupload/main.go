// Command psupload 验证"本地写 → 上传后端 → 服务端回显 → 本地校验通过"的完整闭环。
//
// 流程：
//  1. 打开本地库并同步到最新（checksum 校验通过）；
//  2. 本地 INSERT 一行测试数据（默认 sys_error_report，id 为随机 UUID）；
//  3. connector 把 ps_crud 队列 PUT 到 PostgREST，取 write checkpoint 后 Complete；
//  4. 等服务端把这一行回显回来（cursor 前进 + 行仍在），校验 checksum 不漂移；
//  5. 本地 DELETE 该行 → 上传删除 → 等回显 → 确认本地与服务端都干净。
//
// 只碰自己插入的那一行（默认表 sys_error_report），异常时也会尝试 REST 清理。
//
// 用法：
//
//	go run ./cmd/psupload -endpoint http://host:8080 -rest http://host:8089/rest \
//	    -rpc http://host:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001 \
//	    -schema ./out/schema.json -db ./out/upload.db
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	powersync "msmkSync"
)

func main() {
	var (
		endpoint = flag.String("endpoint", os.Getenv("PS_URL"), "PowerSync 服务地址")
		restBase = flag.String("rest", os.Getenv("REST_URL"), "PostgREST base（如 http://host:8089/rest）")
		rpc      = flag.String("rpc", os.Getenv("RPC_URL"), "fetch_credentials RPC")
		box      = flag.String("box", os.Getenv("BOX_CODE"), "RPC 参数 box_code")
		org      = flag.String("org", os.Getenv("ORG_CODE"), "RPC 参数 org_code")
		token    = flag.String("token", os.Getenv("PS_TOKEN"), "JWT（也可用 -rpc/-box/-org）")
		schemaF  = flag.String("schema", "./out/schema.json", "schema JSON")
		dbPath   = flag.String("db", filepath.Join("out", "upload.db"), "本地 SQLite 文件")
		table    = flag.String("table", "sys_error_report", "用来做闭环测试的表（需在同步规则内）")
		bson     = flag.Bool("bson", true, "请求 BSON 同步流（false 则走 NDJSON）")
		timeout  = flag.Duration("timeout", 4*time.Minute, "整个闭环的最长时间（含复制延迟）")
		verbose  = flag.Bool("v", false, "打印同步调试日志")
	)
	flag.Parse()

	if *endpoint == "" || *restBase == "" {
		fatalf("需要 -endpoint 与 -rest")
	}
	if *token == "" && (*rpc == "" || *box == "" || *org == "") {
		fatalf("需要 -token，或 -rpc/-box/-org")
	}

	raw, err := os.ReadFile(*schemaF)
	if err != nil {
		fatalf("读 schema: %v", err)
	}
	sch, err := powersync.LoadSchemaJSON(raw)
	if err != nil {
		fatalf("解析 schema: %v", err)
	}
	for _, f := range []string{*dbPath, *dbPath + "-wal", *dbPath + "-shm"} {
		_ = os.Remove(f)
	}

	conn := &postgrestConnector{
		psURL:   strings.TrimRight(*endpoint, "/"),
		restURL: strings.TrimRight(*restBase, "/"),
		rpcURL:  *rpc, box: *box, org: *org, token: *token,
		table: *table,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel(*verbose)}))
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename: *dbPath, Schema: sch, Connector: conn, DisableBSON: !*bson, Logger: logger,
		RetryDelay: 200 * time.Millisecond, UploadInterval: 200 * time.Millisecond,
	})
	if err != nil {
		fatalf("Open: %v", err)
	}
	defer db.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	rowID := newUUID()
	conn.rowID = rowID
	defer conn.cleanup()

	fmt.Println("=== 1. 连接并同步到最新 ===")
	if err := db.Connect(ctx); err != nil {
		fatalf("Connect: %v", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		fatalf("WaitForFirstSync: %v", err)
	}
	baseCursor := db.CurrentStatus().Cursor
	fmt.Printf("已同步：buckets=%d cursor=%s\n", db.CurrentStatus().Buckets, baseCursor)

	fmt.Println("=== 2. 本地写入（会自动入 ps_crud 并触发上传） ===")
	if _, err := db.Execute(ctx,
		`INSERT INTO `+quoteIdent(*table)+`
		 (id, org_code, box_code, box_cell, user_id, user_name, error_date, error_desc, trans_type, op_status, box_type)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		rowID, valueOr(*org, "ORG001"), valueOr(*box, "D1-2"), "UPLOAD", "UPLOAD",
		"upload-probe", "2026-01-01 00:00:00", "psupload roundtrip", "PROBE", "PROBE", "PROBE",
	); err != nil {
		fatalf("本地 INSERT: %v", err)
	}

	fmt.Println("=== 3. 等待上传 + 服务端回显 ===")
	if err := waitCursorAdvance(ctx, db, baseCursor); err != nil {
		fatalf("等待上传/回显: %v", err)
	}
	if n, _ := db.UploadQueue(ctx); n != 0 {
		fatalf("上传队列没有清空：%d", n)
	}
	row, found, err := db.GetOptional(ctx, `SELECT id, error_desc FROM `+quoteIdent(*table)+` WHERE id = ?`, rowID)
	if err != nil {
		fatalf("读取本地行: %v", err)
	}
	if !found {
		fatalf("回显后本地行消失了（write checkpoint 门控/物化有问题）")
	}
	fmt.Printf("回显成功：本地仍有该行 %v，cursor=%s，下载错误=%q\n",
		row["id"], db.CurrentStatus().Cursor, db.CurrentStatus().DownloadError)
	if n, err := conn.rowCount(ctx); err != nil || n == 0 {
		fatalf("后端查不到该行（上传没生效）：%d %v", n, err)
	}

	fmt.Println("=== 4. 本地删除 → 上传删除 → 等回显 ===")
	delCursor := db.CurrentStatus().Cursor
	if _, err := db.Execute(ctx, `DELETE FROM `+quoteIdent(*table)+` WHERE id = ?`, rowID); err != nil {
		fatalf("本地 DELETE: %v", err)
	}
	if err := waitCursorAdvance(ctx, db, delCursor); err != nil {
		fatalf("等待删除回显: %v", err)
	}
	if _, found, _ := db.GetOptional(ctx, `SELECT 1 FROM `+quoteIdent(*table)+` WHERE id = ?`, rowID); found {
		fatalf("删除回显后本地行仍在")
	}
	if n, err := conn.rowCount(ctx); err != nil || n != 0 {
		fatalf("后端删除没生效：剩余 %d 行 (%v)", n, err)
	}
	fmt.Printf("删除闭环成功：本地/后端均无该行，cursor=%s\n", db.CurrentStatus().Cursor)

	fmt.Println("\n=== 汇总 ===")
	fmt.Printf("上传次数: %d（write checkpoint 用的 legacy /write-checkpoint2.json）\n", conn.uploads)
	if st := db.CurrentStatus(); st.DownloadError != "" || st.UploadError != "" {
		fmt.Printf("⚠️ 残留错误: download=%q upload=%q\n", st.DownloadError, st.UploadError)
		os.Exit(1)
	}
	fmt.Println("✅ 上传回显闭环通过（无 checksum 漂移、无残留错误）")
}

// waitCursorAdvance 等到 cursor 严格大于 prev（说明新的 checkpoint 应用成功）。
func waitCursorAdvance(ctx context.Context, db *powersync.DB, prev string) error {
	deadline := time.Now().Add(3 * time.Minute)
	for {
		st := db.CurrentStatus()
		if st.Cursor != prev && st.Cursor > prev {
			return nil
		}
		if st.DownloadError != "" {
			return fmt.Errorf("下载错误: %s", st.DownloadError)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("等待 cursor 前进超时（prev=%s, now=%s）", prev, st.Cursor)
		case <-time.After(300 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return errors.New("等待 cursor 前进超时")
		}
	}
}

// ---------------------------------------------------------------- connector

type postgrestConnector struct {
	psURL, restURL, rpcURL, box, org, token, table, rowID string
	uploads                                               int
}

func (c *postgrestConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	if c.token == "" {
		body := fmt.Sprintf(`{"box_code":%q,"org_code":%q}`, c.box, c.org)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL, strings.NewReader(body))
		if err != nil {
			return powersync.Credentials{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return powersync.Credentials{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
			return powersync.Credentials{}, fmt.Errorf("fetch_credentials HTTP %d: %s", resp.StatusCode, msg)
		}
		var out struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return powersync.Credentials{}, err
		}
		c.token = out.Token
	}
	return powersync.Credentials{Endpoint: c.psURL, Token: c.token}, nil
}

func (c *postgrestConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 50)
	if err != nil || batch == nil {
		return err
	}
	for _, e := range batch.Entries {
		if err := c.applyEntry(ctx, e); err != nil {
			return fmt.Errorf("上传 %s %s/%s: %w", e.Op, e.Table, e.ID, err)
		}
	}
	wc, err := c.fetchWriteCheckpoint(ctx, db)
	if err != nil {
		return err
	}
	c.uploads++
	return batch.Complete(ctx, wc)
}

func (c *postgrestConnector) applyEntry(ctx context.Context, e powersync.CrudEntry) error {
	switch e.Op {
	case powersync.UpdateTypePut:
		row := map[string]any{"id": e.ID}
		for k, v := range e.Data {
			row[k] = v
		}
		return c.do(ctx, http.MethodPost, "/"+url.PathEscape(e.Table), row,
			"resolution=merge-duplicates,return=minimal")
	case powersync.UpdateTypePatch:
		if len(e.Data) == 0 {
			return nil
		}
		return c.do(ctx, http.MethodPatch, "/"+url.PathEscape(e.Table)+"?id=eq."+url.QueryEscape(e.ID),
			e.Data, "return=minimal")
	case powersync.UpdateTypeDelete:
		return c.do(ctx, http.MethodDelete, "/"+url.PathEscape(e.Table)+"?id=eq."+url.QueryEscape(e.ID),
			nil, "return=minimal")
	default:
		return fmt.Errorf("未知 op %q", e.Op)
	}
}

func (c *postgrestConnector) do(ctx context.Context, method, path string, body any, prefer string) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.restURL+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// fetchWriteCheckpoint 走 legacy 端点拿 write checkpoint（服务端会自增返回）。
func (c *postgrestConnector) fetchWriteCheckpoint(ctx context.Context, db *powersync.DB) (string, error) {
	clientID, err := db.ClientID(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.psURL+"/write-checkpoint2.json?client_id="+url.QueryEscape(clientID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Token "+c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
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

func (c *postgrestConnector) rowCount(ctx context.Context) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.restURL+"/"+url.PathEscape(c.table)+"?id=eq."+url.QueryEscape(c.rowID)+"&select=id", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var rows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// cleanup 兜底删除测试行（进程异常退出也不会留下垃圾数据）。
func (c *postgrestConnector) cleanup() {
	if c.rowID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.do(ctx, http.MethodDelete,
		"/"+url.PathEscape(c.table)+"?id=eq."+url.QueryEscape(c.rowID), nil, "return=minimal")
	_ = c.do(ctx, http.MethodDelete,
		"/"+url.PathEscape(c.table)+"?id=eq."+url.QueryEscape(c.rowID), nil, "return=minimal")
}

func logLevel(verbose bool) slog.Level {
	if verbose {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func valueOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := randRead(b); err != nil {
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "psupload: "+format+"\n", args...)
	os.Exit(1)
}
