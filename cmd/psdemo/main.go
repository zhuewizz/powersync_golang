// Command psdemo 用公开 API（import "github.com/zhuewizz/powersync_golang"）对一个真实的 PowerSync 服务做冒烟验证：
// 打开本地库 → 建表 → Connect → 等首次同步 → 打印状态与各表行数。
//
// 用法：
//
//	go run ./cmd/psdemo -endpoint http://host:8080 \
//	    -rpc http://host:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001 \
//	    -schema ./out/schema.json -db ./out/demo.db
//
// 只读：UploadData 是空实现，不会向服务端写任何数据。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	powersync "github.com/zhuewizz/powersync_golang"
	"github.com/zhuewizz/powersync_golang/internal/psclient"
)

func main() {
	var (
		endpoint   = flag.String("endpoint", os.Getenv("PS_URL"), "PowerSync 服务地址")
		token      = flag.String("token", os.Getenv("PS_TOKEN"), "JWT（也可用 -rpc 换取）")
		rpc        = flag.String("rpc", os.Getenv("RPC_URL"), "fetch_credentials RPC")
		box        = flag.String("box", os.Getenv("BOX_CODE"), "RPC 参数 box_code")
		org        = flag.String("org", os.Getenv("ORG_CODE"), "RPC 参数 org_code")
		schema     = flag.String("schema", "", "schema JSON（官方 Schema.toJSON 形状）")
		dbPath     = flag.String("db", filepath.Join("out", "demo.db"), "本地 SQLite 文件")
		bson       = flag.Bool("bson", false, "请求 BSON 同步流")
		subscribe  = flag.String("subscribe", "", "逗号分隔的流名，连接前显式订阅（sync streams）")
		noDefaults = flag.Bool("no-defaults", false, "不请求默认流（include_defaults=false）")
		timeout    = flag.Duration("timeout", 90*time.Second, "等待首次同步的最长时间")
		verbose    = flag.Bool("v", false, "打印每张表行数")
	)
	flag.Parse()

	if *endpoint == "" || *schema == "" {
		fatalf("需要 -endpoint 与 -schema")
	}
	if *token == "" && (*rpc == "" || *box == "" || *org == "") {
		fatalf("需要 -token，或 -rpc/-box/-org")
	}
	raw, err := os.ReadFile(*schema)
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

	conn := &demoConnector{endpoint: strings.TrimRight(*endpoint, "/"), rpc: *rpc, box: *box, org: *org, token: *token}
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:  *dbPath,
		Schema:    sch,
		Connector: conn, DisableBSON: !*bson, DisableDefaultStreams: *noDefaults,
	})
	if err != nil {
		fatalf("Open: %v", err)
	}
	defer db.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if *subscribe != "" {
		for _, name := range strings.Split(*subscribe, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			sub, err := db.SubscribeStream(ctx, name, map[string]any{})
			if err != nil {
				fatalf("SubscribeStream(%s): %v", name, err)
			}
			fmt.Printf("已订阅流: %s (id=%d)\n", sub.Name, sub.ID)
		}
	}

	start := time.Now()
	if err := db.Connect(ctx); err != nil {
		fatalf("Connect: %v", err)
	}
	// 只下载、不上传：把上传间隔调大靠的是 connector 里 UploadData 返回 nil。
	if err := db.WaitForFirstSync(ctx); err != nil {
		fatalf("WaitForFirstSync: %v", err)
	}
	st := db.CurrentStatus()
	fmt.Printf("首次同步完成，用时 %s\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("状态: connected=%v buckets=%d cursor=%s lastSyncedAt=%s\n",
		st.Connected, st.Buckets, st.Cursor, st.LastSyncedAt.Format(time.RFC3339))
	if cid, err := db.ClientID(ctx); err == nil {
		fmt.Printf("client_id: %s\n", cid)
	}
	if n, err := db.UploadQueue(ctx); err == nil {
		fmt.Printf("上传队列: %d 条（只读 demo 不应有本地写）\n", n)
	}

	total := int64(0)
	tables := 0
	for _, tbl := range sch.Tables() {
		rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT count(*) AS n FROM "%s"`,
			strings.ReplaceAll(tbl.Name, `"`, `""`)))
		if err != nil {
			fmt.Printf("  %-28s 查询失败: %v\n", tbl.Name, err)
			continue
		}
		var n int64
		switch v := rows[0]["n"].(type) {
		case int64:
			n = v
		case float64:
			n = int64(v)
		}
		total += n
		tables++
		if *verbose {
			fmt.Printf("  %-28s %5d\n", tbl.Name, n)
		}
	}
	fmt.Printf("落库: %d 张表 / %d 行\n", tables, total)

	// 演示本地写 → 上传队列（不真的上传：UploadData 是空实现，这里只读出来看看）。
	if _, err := db.Execute(ctx, `INSERT INTO sys_config(id, cfg_key, cfg_value) VALUES(?,?,?)`,
		"demo-local-row", "demo_key", "demo_value"); err != nil {
		fmt.Printf("本地写（sys_config）失败: %v\n", err)
	} else {
		entry, err := db.GetNextCrudEntry(ctx)
		if err != nil {
			fatalf("GetNextCrudEntry: %v", err)
		}
		if entry != nil {
			b, _ := json.Marshal(entry)
			fmt.Printf("本地写入队样例: %s\n", b)
			batch, err := db.GetCrudBatch(ctx, 10)
			if err != nil {
				fatalf("GetCrudBatch: %v", err)
			}
			if err := batch.Complete(ctx); err != nil {
				fatalf("Complete: %v", err)
			}
			fmt.Println("已出队（demo 不真正上传）")
		}
	}

	db.Disconnect()
	if st := db.CurrentStatus(); st.DownloadError != "" {
		fmt.Printf("下载错误: %s\n", st.DownloadError)
		os.Exit(1)
	}
}

// demoConnector 只取凭据；上传是空操作（demo 不写服务端）。
type demoConnector struct {
	endpoint, rpc, box, org, token string
}

func (c *demoConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	tok := c.token
	if tok == "" {
		fetched, err := psclient.FetchToken(ctx, c.rpc, c.box, c.org)
		if err != nil {
			return powersync.Credentials{}, err
		}
		tok = fetched
		c.token = tok
	}
	return powersync.Credentials{Endpoint: c.endpoint, Token: tok}, nil
}

// UploadData 故意什么都不做（demo 只读）。
func (c *demoConnector) UploadData(ctx context.Context, db *powersync.DB) error { return nil }

func valueOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "psdemo: "+format+"\n", args...)
	os.Exit(1)
}
