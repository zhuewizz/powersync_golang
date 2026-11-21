// Command todo 是 msmkSync 的完整可运行示例。
//
// 默认是**离线模式**：内置一个假的 PowerSync 服务（回放 fixture.ndjson）+ 假后端，
// 不连任何真实服务，直接 `go run ./examples/todo` 就能跑完整个流程。
//
// 连真实服务（只下载；上传是空实现）：
//
//	go run ./examples/todo -mode real \
//	    -endpoint http://120.24.7.22:8080 \
//	    -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
//	    -box D1-2 -org ORG001 \
//	    -schema ./out/schema.json \
//	    -tables sys_error_report,cutter_info
//
// 真实 schema 用官方 Schema.toJSON() 形状，可这样导出（复用 e:/cutter_client）：
//
//	node scripts/js-reference/dump-schema.mjs
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
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
		schemaF  = flag.String("schema", "", "schema JSON（官方 Schema.toJSON 形状）；real 模式通常必填")
		tablesF  = flag.String("tables", "", "只统计/打印这些表（逗号分隔），空=全部")
		dbPath   = flag.String("db", filepath.Join("out", "example.db"), "本地数据库文件")
		timeout  = flag.Duration("timeout", 90*time.Second, "整体超时")
		write    = flag.Bool("write", false, "real 模式下也做本地写/上传演示（默认只读）")
		limit    = flag.Int("limit", 3, "每张表打印多少行样例")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	_ = os.MkdirAll(filepath.Dir(*dbPath), 0o755)
	for _, f := range []string{*dbPath, *dbPath + "-wal", *dbPath + "-shm", *dbPath + ".lock"} {
		_ = os.Remove(f)
	}

	// ---- 0. schema：优先 -schema（真实库），否则用内置示例 schema ----
	schema, tableNames, err := loadSchema(*schemaF, *mode)
	if err != nil {
		fail("加载 schema", err)
	}

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
			fail("real 模式参数不完整", errors.New("需要 -endpoint/-rpc/-box/-org（或环境变量 PS_URL/RPC_URL/BOX_CODE/ORG_CODE）"))
		}
		if *schemaF == "" {
			fmt.Println("提示: real 模式建议用 -schema ./out/schema.json 传入真实 schema；" +
				"否则只能看到内置示例表 todos")
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

	// 关心的表：-tables 或 schema 里全部表
	wanted := tableNames
	if *tablesF != "" {
		wanted = nil
		for _, name := range strings.Split(*tablesF, ",") {
			if name = strings.TrimSpace(name); name != "" {
				wanted = append(wanted, name)
			}
		}
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
	fmt.Printf("数据库: %s\nschema: %d 张表；关注: %s\n\n", *dbPath, len(tableNames), strings.Join(wanted, ", "))

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
	if len(wanted) > 0 && hasTable(tableNames, wanted[0]) {
		go watchCount(ctx, db, wanted[0])
	}

	// ---- 4. 连接并等首次同步 ----
	fmt.Println("— 连接并等待首次同步 —")
	if err := db.Connect(ctx); err != nil {
		fail("Connect", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		fail("WaitForFirstSync", err)
	}
	time.Sleep(100 * time.Millisecond) // 等监听 goroutine 打印

	for _, name := range wanted {
		printTable(ctx, db, name, *limit)
	}

	// ---- 5. 写演示（mock 默认做；real 需要 -write）----
	doWrite := *mode == "mock" || *write
	if doWrite {
		writeTable := pickWriteTable(tableNames)
		runWriteDemo(ctx, db, writeTable, *mode)
	} else {
		fmt.Println("\n（real 模式默认只读；要演示写入/上传请加 -write）")
	}

	st := db.CurrentStatus()
	net := db.CurrentNetworkStatus()
	fmt.Printf("\n最终状态: connected=%v cursor=%s buckets=%d\n", st.Connected, st.Cursor, st.Buckets)
	fmt.Printf("网络状态: online=%v target=%s latency=%s\n", net.Online, net.Target, net.Latency.Round(time.Millisecond))
	if n, _ := db.UploadQueue(ctx); n != 0 {
		fmt.Printf("⚠️ 上传队列仍有 %d 条\n", n)
	}
	fmt.Println("\n✅ 示例跑完（数据库在", *dbPath, "）")
}

// ---------------------------------------------------------------- schema

func loadSchema(path, mode string) (*powersync.Schema, []string, error) {
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, err
		}
		sch, err := powersync.LoadSchemaJSON(raw)
		if err != nil {
			return nil, nil, err
		}
		tables := make([]string, 0, len(sch.Tables()))
		for _, t := range sch.Tables() {
			tables = append(tables, t.Name)
		}
		fmt.Printf("schema: 来自 %s（%d 张表）\n", path, len(tables))
		return sch, tables, nil
	}
	fmt.Println("schema: 使用内置示例（todos 同步 + local_notes 仅本地）")
	return powersync.NewSchema(
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
			LocalOnly: true,
		},
	), []string{"todos", "local_notes"}, nil
}

func hasTable(tables []string, name string) bool {
	for _, t := range tables {
		if t == name {
			return true
		}
	}
	return false
}

// pickWriteTable 选一张适合演示写入的表（优先 todos，其次 sys_error_report）。
func pickWriteTable(tables []string) string {
	for _, want := range []string{"todos", "sys_error_report"} {
		if hasTable(tables, want) {
			return want
		}
	}
	if len(tables) > 0 {
		return tables[0]
	}
	return "todos"
}

// ---------------------------------------------------------------- 演示

func watchCount(ctx context.Context, db *powersync.DB, table string) {
	ch, err := db.WatchTables(ctx, []string{table},
		fmt.Sprintf(`SELECT count(*) AS n FROM "%s"`, strings.ReplaceAll(table, `"`, `""`)))
	if err != nil {
		return
	}
	for rows := range ch {
		fmt.Printf("  [查询] %s 行数 = %v\n", table, rows[0]["n"])
	}
}

func printTable(ctx context.Context, db *powersync.DB, table string, limit int) {
	q := fmt.Sprintf(`SELECT * FROM "%s" LIMIT %d`, strings.ReplaceAll(table, `"`, `""`), limit)
	rows, err := db.GetAll(ctx, q)
	if err != nil {
		fmt.Printf("\n表 %s: 查询失败（该表可能不在 schema 里）：%v\n", table, err)
		return
	}
	var n int64
	if cnt, err := db.GetAll(ctx, fmt.Sprintf(`SELECT count(*) AS n FROM "%s"`,
		strings.ReplaceAll(table, `"`, `""`))); err == nil && len(cnt) == 1 {
		switch v := cnt[0]["n"].(type) {
		case int64:
			n = v
		case float64:
			n = int64(v)
		}
	}
	fmt.Printf("\n表 %s: %d 行（样例 %d 行）\n", table, n, len(rows))
	for _, r := range rows {
		fmt.Printf("    %s\n", formatRow(r))
	}
}

func formatRow(r map[string]any) string {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		s := fmt.Sprint(r[k])
		if len(s) > 40 {
			s = s[:40] + "…"
		}
		parts = append(parts, k+"="+s)
	}
	return strings.Join(parts, " ")
}

// runWriteDemo 在指定表上做：插入 → 等上传 → 更新 → 等上传 → 删除 → 等上传。
func runWriteDemo(ctx context.Context, db *powersync.DB, table, mode string) {
	id := "example-" + fmt.Sprint(time.Now().UnixNano())
	fmt.Printf("\n— 写入演示（表 %s，id=%s）—\n", table, id)

	if _, err := db.Execute(ctx,
		fmt.Sprintf(`INSERT INTO "%s"(id) VALUES(?)`, strings.ReplaceAll(table, `"`, `""`)), id); err != nil {
		fmt.Printf("  插入失败（跳过写入演示）：%v\n", err)
		return
	}
	waitQueueEmpty(ctx, db)

	if _, err := db.Execute(ctx,
		fmt.Sprintf(`UPDATE "%s" SET id = id WHERE id = ?`, strings.ReplaceAll(table, `"`, `""`)), id); err != nil {
		fmt.Printf("  更新失败：%v\n", err)
	} else {
		waitQueueEmpty(ctx, db)
	}

	if _, err := db.Execute(ctx,
		fmt.Sprintf(`DELETE FROM "%s" WHERE id = ?`, strings.ReplaceAll(table, `"`, `""`)), id); err != nil {
		fmt.Printf("  删除失败：%v\n", err)
	} else {
		waitQueueEmpty(ctx, db)
	}
	if mode == "real" {
		fmt.Println("  说明: real 模式的上传是空实现（只出队），不会写你的后端；本地演示行已删除")
	}
}

// ---------------------------------------------------------------- 连接器

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
		data, _ := json.Marshal(e.Data)
		fmt.Printf("  [上传] %-6s %s/%s data=%s\n", e.Op, e.Table, e.ID, truncate(string(data), 80))
	}
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
	for _, e := range batch.Entries {
		data, _ := json.Marshal(e.Data)
		fmt.Printf("  [上传-空实现] %-6s %s/%s data=%s\n", e.Op, e.Table, e.ID, truncate(string(data), 60))
	}
	c.logger.Warn("real 模式示例不实现上传，直接出队", "entries", len(batch.Entries))
	return batch.Complete(ctx)
}

// ---------------------------------------------------------------- 工具

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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
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
