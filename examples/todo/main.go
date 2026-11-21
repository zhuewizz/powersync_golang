// Command todo 是 msmkSync 的完整可运行示例。
//
// 默认是**离线模式**：内置一个假的 PowerSync 服务（回放 fixture.ndjson）+ 假后端，
// 不连任何真实服务，直接 `go run ./examples/todo` 就能跑完整个流程：
//
//	schema → 打开库 → 连接（下载）→ 监听查询/状态/网络 → 本地写 → 上传 → 删除
//
// 也可以连真实服务（只下载，上传是空实现）：
//
//	go run ./examples/todo -mode real -endpoint http://host:8080 \
//	    -rpc http://host:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	powersync "msmkSync"
	"msmkSync/internal/fixture"
	"msmkSync/internal/psclient"
)

//go:embed fixture.ndjson
var fixtureData []byte

func main() {
	var (
		mode     = flag.String("mode", "mock", "mock（内置假服务）或 real（连真实服务）")
		endpoint = flag.String("endpoint", os.Getenv("PS_URL"), "real 模式：PowerSync 服务地址")
		rpc      = flag.String("rpc", os.Getenv("RPC_URL"), "real 模式：fetch_credentials RPC")
		box      = flag.String("box", os.Getenv("BOX_CODE"), "real 模式：box_code")
		org      = flag.String("org", os.Getenv("ORG_CODE"), "real 模式：org_code")
		dbPath   = flag.String("db", filepath.Join("out", "example.db"), "本地数据库文件")
		timeout  = flag.Duration("timeout", 30*time.Second, "整体超时")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	_ = os.MkdirAll(filepath.Dir(*dbPath), 0o755)
	for _, f := range []string{*dbPath, *dbPath + "-wal", *dbPath + "-shm", *dbPath + ".lock"} {
		_ = os.Remove(f)
	}

	schema := powersync.NewSchema(
		powersync.Table{
			Name: "todos",
			Columns: []powersync.Column{
				{Name: "title", Type: powersync.TextColumn},
				{Name: "done", Type: powersync.IntegerColumn},
			},
		},
		powersync.Table{
			Name:      "local_notes",
			Columns:   []powersync.Column{{Name: "body", Type: powersync.TextColumn}},
			LocalOnly: true, // 只在本机：不同步、不入上传队列
		},
	)

	// ---- 1. 准备“服务端” ----
	var (
		conn      powersync.Connector
		netTarget string
	)
	switch *mode {
	case "mock":
		fx, err := fixture.FromBytes(fixtureData)
		if err != nil {
			fail("加载内置 fixture", err)
		}
		mockServer := httptest.NewServer(fx.Handler())
		defer mockServer.Close()
		conn = &mockConnector{endpoint: mockServer.URL}
		netTarget = mockServer.URL
		fmt.Printf("模式: mock（内置假服务 %s）\n", mockServer.URL)
	case "real":
		if *endpoint == "" || *rpc == "" || *box == "" || *org == "" {
			fail("real 模式参数不完整", errors.New("需要 -endpoint/-rpc/-box/-org（或对应环境变量）"))
		}
		conn = &realConnector{
			endpoint: strings.TrimRight(*endpoint, "/"),
			rpc:      *rpc, box: *box, org: *org,
			logger: logger,
		}
		netTarget = *endpoint
		fmt.Printf("模式: real（%s）\n", *endpoint)
	default:
		fail("未知 -mode", fmt.Errorf("%q", *mode))
	}

	// ---- 2. 打开数据库 ----
	db, err := powersync.Open(ctx, powersync.Options{
		Filename:  *dbPath,
		Schema:    schema,
		Connector: conn,
		Logger:    logger,
		// 网络探测：示例里调快一点，便于观察
		NetworkTarget:        netTarget,
		NetworkCheckInterval: 2 * time.Second,
		NetworkCheckTimeout:  time.Second,
	})
	if err != nil {
		fail("打开数据库", err)
	}
	defer db.Close(context.Background())
	fmt.Printf("数据库: %s\nschema: todos（同步）+ local_notes（仅本地）\n\n", *dbPath)

	// ---- 3. 三个实时监听：状态 / 网络 / 查询 ----
	go func() {
		ch, err := db.WatchStatus(ctx)
		if err != nil {
			return
		}
		for st := range ch {
			fmt.Printf("  [状态] connected=%v uploading=%v cursor=%s lastSynced=%s\n",
				st.Connected, st.Uploading, st.Cursor, fmtTime(st.LastSyncedAt))
		}
	}()
	go func() {
		ch, err := db.WatchNetworkStatus(ctx)
		if err != nil {
			return
		}
		for n := range ch {
			fmt.Printf("  [网络] online=%v target=%s latency=%s err=%s\n",
				n.Online, n.Target, n.Latency.Round(time.Millisecond), n.Err)
		}
	}()
	go func() {
		ch, err := db.WatchTables(ctx, []string{"todos"}, `SELECT count(*) AS n FROM todos`)
		if err != nil {
			return
		}
		for rows := range ch {
			fmt.Printf("  [查询] todos 行数 = %v\n", rows[0]["n"])
		}
	}()

	// ---- 4. 连接并等首次同步 ----
	fmt.Println("— 连接并等待首次同步 —")
	if err := db.Connect(ctx); err != nil {
		fail("Connect", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		fail("WaitForFirstSync", err)
	}
	time.Sleep(100 * time.Millisecond) // 等监听 goroutine 打印
	printRows(ctx, db, "服务器下发的初始数据")

	// ---- 5. 本地写 → 上传队列 ----
	fmt.Println("\n— 本地插入一行（会自动进入上传队列并触发上传）—")
	if _, err := db.Execute(ctx,
		`INSERT INTO todos(id, title, done) VALUES(?, ?, ?)`, "local-1", "本地新建的任务", 0); err != nil {
		fail("本地插入", err)
	}
	waitQueueEmpty(ctx, db)
	printRows(ctx, db, "插入之后")

	fmt.Println("\n— 批量插入两行（ExecuteBatch）—")
	if err := db.ExecuteBatch(ctx, `INSERT INTO todos(id,title,done) VALUES(?,?,?)`, [][]any{
		{"local-2", "批量任务 A", 1},
		{"local-3", "批量任务 B", 0},
	}); err != nil {
		fail("ExecuteBatch", err)
	}
	waitQueueEmpty(ctx, db)

	fmt.Println("\n— 删除一行 —")
	if _, err := db.Execute(ctx, `DELETE FROM todos WHERE id = ?`, "local-2"); err != nil {
		fail("删除", err)
	}
	waitQueueEmpty(ctx, db)

	fmt.Println("\n— 事务：改一行 + 写本地日志（local_only 表不入队）—")
	if err := db.WriteTransaction(ctx, func(ctx context.Context, tx *powersync.Tx) error {
		if _, err := tx.Execute(ctx, `UPDATE todos SET done = 1 WHERE id = ?`, "local-1"); err != nil {
			return err
		}
		_, err := tx.Execute(ctx, `INSERT INTO local_notes(id, body) VALUES(?, ?)`, "n1", "只在本机")
		return err
	}); err != nil {
		fail("WriteTransaction", err)
	}
	waitQueueEmpty(ctx, db)
	printRows(ctx, db, "最终数据")

	st := db.CurrentStatus()
	net := db.CurrentNetworkStatus()
	fmt.Printf("\n最终状态: connected=%v cursor=%s buckets=%d\n", st.Connected, st.Cursor, st.Buckets)
	fmt.Printf("网络状态: online=%v target=%s latency=%s\n", net.Online, net.Target, net.Latency.Round(time.Millisecond))
	if n, _ := db.UploadQueue(ctx); n != 0 {
		fmt.Printf("⚠️ 上传队列仍有 %d 条\n", n)
	}
	fmt.Println("\n✅ 示例跑完（数据库在", *dbPath, "）")
}

// ---------------------------------------------------------------- 连接器

// mockConnector：内置假服务 + 假后端（只打印上传内容，不真正落库）。
type mockConnector struct{ endpoint string }

func (c *mockConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	return powersync.Credentials{Endpoint: c.endpoint, Token: "mock-token"}, nil
}

func (c *mockConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 100)
	if err != nil || batch == nil {
		return err
	}
	for _, e := range batch.Entries {
		fmt.Printf("  [上传] %-6s %s/%s data=%v\n", e.Op, e.Table, e.ID, e.Data)
	}
	// 真实项目里：把 entries 写到你的后端，拿到 write checkpoint 后 batch.Complete(ctx, wc)
	return batch.Complete(ctx)
}

// realConnector：连真实服务（只下载；上传是空实现，避免示例误写生产数据）。
type realConnector struct {
	endpoint, rpc, box, org string
	logger                  *slog.Logger
	token                   string
}

func (c *realConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	if c.token == "" {
		tok, err := psclient.FetchToken(ctx, c.rpc, c.box, c.org)
		if err != nil {
			return powersync.Credentials{}, err
		}
		c.token = tok
	}
	return powersync.Credentials{Endpoint: c.endpoint, Token: c.token}, nil
}

func (c *realConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 100)
	if err != nil || batch == nil {
		return err
	}
	c.logger.Warn("real 模式示例不实现上传，直接出队", "entries", len(batch.Entries))
	return batch.Complete(ctx)
}

// ---------------------------------------------------------------- 工具

func printRows(ctx context.Context, db *powersync.DB, title string) {
	rows, err := db.GetAll(ctx, `SELECT id, title, done FROM todos ORDER BY id`)
	if err != nil {
		fmt.Printf("  查询失败: %v\n", err)
		return
	}
	fmt.Printf("  %s（%d 行）:\n", title, len(rows))
	for _, r := range rows {
		fmt.Printf("    %-8v %-16v done=%v\n", r["id"], r["title"], r["done"])
	}
}

func waitQueueEmpty(ctx context.Context, db *powersync.DB) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := db.UploadQueue(ctx); n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("  ⚠️ 上传队列没有在 5s 内清空")
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05")
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
	os.Exit(1)
}
