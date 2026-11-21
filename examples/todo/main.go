// Command todo 是 msmkSync 的完整可运行示例。
//
// 默认是**离线模式**：内置一个假的 PowerSync 服务（回放 fixture.ndjson）+ 假后端，
// 不连任何真实服务，直接 `go run ./examples/todo` 就能跑完整个流程。
//
// 连真实服务（只下载）：
//
//	go run ./examples/todo -mode real \
//	    -endpoint http://120.24.7.22:8080 \
//	    -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
//	    -box D1-2 -org ORG001 \
//	    -schema ./out/schema.json \
//	    -tables sys_error_report,cutter_info
//
// 连真实服务并真正上传 + 演示"断网可用、恢复后自动同步"：
//
//	go run ./examples/todo -mode real -upload real -write \
//	    -endpoint http://120.24.7.22:8080 -rest http://120.24.7.22:8089/rest \
//	    -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
//	    -box D1-2 -org ORG001 -schema ./out/schema.json -tables sys_error_report,cutter_info
package main

import (
	"context"
	crand "crypto/rand"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	powersync "msmkSync"
	"msmkSync/internal/fixture"
	"msmkSync/internal/psclient"
	"msmkSync/internal/rowdiff"
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
		schemaF  = flag.String("schema", "", "schema JSON（官方 Schema.toJSON 形状）；real 模式必填")
		tablesF  = flag.String("tables", "", "只统计/打印这些表（逗号分隔），空=全部")
		dbPath   = flag.String("db", filepath.Join("out", "example.db"), "本地数据库文件")
		timeout  = flag.Duration("timeout", 120*time.Second, "整体超时")
		write    = flag.Bool("write", false, "real 模式下也做写入演示（默认只读）")
		upload   = flag.String("upload", "mock", "real 模式的上传方式：mock（打印后出队）或 real（真正写 PostgREST）")
		restBase = flag.String("rest", os.Getenv("REST_URL"), "upload=real 时的 PostgREST base，如 http://host:8089/rest")
		limit    = flag.Int("limit", 3, "每张表打印多少行样例")
		verbose  = flag.Bool("v", false, "打印库内部 debug 日志（同步行/checkpoint/物化行数）")
		cleanup  = flag.Bool("cleanup", false, "演示结束后删除探针行（默认**保留**在后台，方便查看）")
		verify   = flag.Bool("verify", true, "real 模式下把本地表与后台 REST 逐行比对（需 -rest）")
	)
	flag.Parse()

	lvl := slog.LevelWarn
	if *verbose {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	_ = os.MkdirAll(filepath.Dir(*dbPath), 0o755)
	for _, f := range []string{*dbPath, *dbPath + "-wal", *dbPath + "-shm", *dbPath + ".lock"} {
		_ = os.Remove(f)
	}

	// ---- 0. schema：优先 -schema（真实库），否则用内置示例 schema ----
	schema, tableNames, err := loadSchema(*schemaF)
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
			fail("real 模式参数不完整",
				errors.New("需要 -endpoint/-rpc/-box/-org（或环境变量 PS_URL/RPC_URL/BOX_CODE/ORG_CODE）"))
		}
		if *schemaF == "" {
			fail("real 模式需要 -schema", errors.New(
				"PowerSync 要求客户端声明 schema；请用 -schema ./out/schema.json"+
					"（由 node scripts/js-reference/dump-schema.mjs 从 AppSchema.ts 导出）"))
		}
		ep := strings.TrimRight(*endpoint, "/")
		switch *upload {
		case "real":
			if *restBase == "" {
				fail("upload=real 缺少 -rest", errors.New("需要 -rest http://host:8089/rest（或环境变量 REST_URL）"))
			}
			conn = &realUploadConnector{
				endpoint: ep, rpc: *rpc, box: *box, org: *org,
				rest: &psclient.REST{Base: strings.TrimRight(*restBase, "/")},
				log:  logger,
			}
			fmt.Println("上传: real（会真正写 PostgREST；探针行默认保留，-cleanup 才删）")
		default:
			conn = &realConnector{endpoint: ep, rpc: *rpc, box: *box, org: *org, logger: logger}
			fmt.Println("上传: mock（打印后出队，不写后端）")
		}
		netTarget = ep
		fmt.Printf("模式: real（%s）\n", ep)
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
		Debugf: func(format string, args ...any) {
			if *verbose {
				logger.Debug(fmt.Sprintf(format, args...))
			}
		},
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

	if skipped := db.SkippedUnknownOps(); skipped > 0 {
		fmt.Println()
		fmt.Printf("⚠️ 有 %d 行因表不在 schema 中被跳过（这些表本地没有数据）", skipped)
		if tables := db.SkippedTables(); len(tables) > 0 {
			fmt.Printf("：%s", strings.Join(tables, ", "))
		}
		fmt.Println()
		fmt.Println("   这些表不在客户端 schema 里，PowerSync 不会同步它们（要么加进 AppSchema.ts，要么从 sync rules 移除）")
	}

	for _, name := range wanted {
		printTable(ctx, db, name, *limit)
	}

	// ---- 4.5 校验：后台 REST → 本地（证明自动同步一致）----
	if *mode == "real" && *verify && *restBase != "" {
		if tok, err := psclient.FetchToken(ctx, *rpc, *box, *org); err == nil {
			verifyAgainstBackend(ctx, db, *restBase, tok, wanted, *limit)
		} else {
			fmt.Printf("\n（跳过校验：取 token 失败 %v）\n", err)
		}
	}

	// ---- 5. 写演示 ----
	doWrite := *mode == "mock" || *write
	if doWrite {
		writeTable := pickWriteTable(tableNames)
		switch {
		case *mode == "real" && *upload == "real" && writeTable != "sys_error_report":
			fmt.Printf("\n⚠️ upload=real 的写入演示仅支持 sys_error_report（当前是 %s），改为只读\n", writeTable)
		case *mode == "real" && *upload == "real":
			runOfflineFirstDemo(ctx, db, conn, writeTable, wanted, *box, *org, *cleanup, *limit)
		default:
			runWriteDemo(ctx, db, writeTable, *mode)
		}
	} else {
		fmt.Println("\n（real 模式默认只读；要演示写入请加 -write，要真正写后端再加 -upload real -rest ...）")
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

func loadSchema(path string) (*powersync.Schema, []string, error) {
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
	ch, err := db.WatchTables(ctx, []string{table}, `SELECT count(*) AS n FROM `+quoteIdent(table))
	if err != nil {
		return
	}
	for rows := range ch {
		fmt.Printf("  [查询] %s 行数 = %v\n", table, rows[0]["n"])
	}
}

func printTable(ctx context.Context, db *powersync.DB, table string, limit int) {
	rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT * FROM %s LIMIT %d`, quoteIdent(table), limit))
	if err != nil {
		fmt.Printf("\n表 %s: 查询失败（该表可能不在 schema 里）：%v\n", table, err)
		return
	}
	var n int64
	if cnt, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quoteIdent(table)); err == nil && len(cnt) == 1 {
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

// runWriteDemo 在指定表上做：插入 → 更新 → 删除（上传是 mock，只打印）。
func runWriteDemo(ctx context.Context, db *powersync.DB, table, mode string) {
	id := newID()
	fmt.Printf("\n— 写入演示（表 %s，id=%s，上传=mock）—\n", table, id)

	if _, err := db.Execute(ctx, `INSERT INTO `+quoteIdent(table)+`(id) VALUES(?)`, id); err != nil {
		fmt.Printf("  插入失败（跳过写入演示）：%v\n", err)
		return
	}
	waitQueueEmpty(ctx, db)

	if _, err := db.Execute(ctx, `UPDATE `+quoteIdent(table)+` SET id = id WHERE id = ?`, id); err != nil {
		fmt.Printf("  更新失败：%v\n", err)
	} else {
		waitQueueEmpty(ctx, db)
	}

	if _, err := db.Execute(ctx, `DELETE FROM `+quoteIdent(table)+` WHERE id = ?`, id); err != nil {
		fmt.Printf("  删除失败：%v\n", err)
	} else {
		waitQueueEmpty(ctx, db)
	}
	if mode == "real" {
		fmt.Println("  说明: 上传是 mock（只出队），不会写你的后端；本地演示行已删除")
	}
}

// runOfflineFirstDemo 演示离线优先闭环：
//
//  1. 已联机下载过 → 本地有数据（cutter_info 等）
//  2. 断开（模拟断网）→ 仍能读本地数据，作业不中断
//  3. 断网期间本地写 → 进入上传队列（待同步）
//  4. 恢复网络 → 自动上传到 PostgREST；默认**保留**后台这一行（-cleanup 才删）
func runOfflineFirstDemo(ctx context.Context, db *powersync.DB, conn powersync.Connector,
	writeTable string, wanted []string, box, org string, cleanup bool, limit int) {
	fmt.Println("\n===== 离线优先演示（断网可用 → 恢复后自动同步）=====")

	// 1) 断开同步，模拟断网
	db.Disconnect()
	time.Sleep(150 * time.Millisecond)
	fmt.Println("① 已断开同步（模拟断网）")

	// 2) 断网时读本地
	fmt.Println("② 断网状态下读本地数据（不依赖网络）：")
	for _, name := range wanted {
		if name == writeTable {
			continue
		}
		printTable(ctx, db, name, limit)
	}

	writer, ok := conn.(*realUploadConnector)
	if !ok {
		fmt.Println("   （当前连接器不支持真实上传，跳过写入演示）")
		return
	}

	// 3) 断网期间本地写
	id := newID()
	fmt.Printf("③ 断网状态下本地插入一行 %s/%s（进入上传队列）\n", writeTable, id)
	insertSQL := `INSERT INTO ` + quoteIdent(writeTable) + `
	  (id, org_code, box_code, box_cell, user_id, user_name, error_date, error_desc, trans_type, op_status, box_type)
	  VALUES(?,?,?,?,?,?,?,?,?,?,?)`
	if _, err := db.Execute(ctx, insertSQL,
		id, nonEmpty(org, "ORG001"), nonEmpty(box, "D1-2"), "EXAMPLE", "EXAMPLE",
		"examples/todo", "2026-01-01 00:00:00", "offline-first demo", "PROBE", "PROBE", "PROBE"); err != nil {
		fmt.Printf("   本地插入失败：%v\n", err)
		return
	}
	if n, _ := db.UploadQueue(ctx); n > 0 {
		fmt.Printf("   上传队列：%d 条待同步（本地数据已可用）\n", n)
	}
	rows, _ := db.GetAll(ctx, `SELECT id, error_desc FROM `+quoteIdent(writeTable)+` WHERE id=?`, id)
	fmt.Printf("   断网时本地已能读到：%v\n", rows)

	// 4) 恢复网络 → 自动上传
	fmt.Println("④ 恢复连接（Connect）→ 等待上传队列清空")
	if err := db.Connect(ctx); err != nil {
		fmt.Printf("   Connect 失败：%v\n", err)
		return
	}
	waitQueueEmpty(ctx, db)
	if n, _ := db.UploadQueue(ctx); n != 0 {
		fmt.Printf("   ⚠️ 上传队列仍有 %d 条\n", n)
	}
	if n, err := writer.rest.CountByID(ctx, writeTable, id); err == nil {
		fmt.Printf("   后台校验：该行存在 = %v\n", n > 0)
	} else {
		fmt.Printf("   后台校验失败：%v\n", err)
	}

	if cleanup {
		fmt.Println("⑤ -cleanup：删除探针行（本地 + 后台）")
		if _, err := db.Execute(ctx, `DELETE FROM `+quoteIdent(writeTable)+` WHERE id=?`, id); err != nil {
			fmt.Printf("   本地删除失败：%v\n", err)
		}
		waitQueueEmpty(ctx, db)
		_ = writer.rest.Do(ctx, http.MethodDelete,
			tablePath(writeTable)+"?id=eq."+url.QueryEscape(id), nil, "return=minimal")
		fmt.Println("   已删除")
	} else {
		fmt.Printf("⑤ 保留后台这一行（id=%s），方便查看同步结果\n", id)
		fmt.Printf("   手动清理：DELETE %s?id=eq.%s（带 Authorization: Bearer <token>）\n",
			writer.rest.Base+tablePath(writeTable), url.QueryEscape(id))
		fmt.Println("   或重新跑一次示例并加 -cleanup（只删它自己生成的行）")
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
		fmt.Printf("  [上传-mock] %-6s %s/%s data=%s\n", e.Op, e.Table, e.ID, truncate(string(data), 60))
	}
	c.logger.Warn("real 模式示例不实现上传，直接出队", "entries", len(batch.Entries))
	return batch.Complete(ctx)
}

// realUploadConnector：连真实服务，并且**真正**把 ps_crud 队列写进后端（PostgREST）。
//
// 与 cmd/psupload 同一套语义：PUT=upsert、PATCH=update、DELETE=delete；
// 上传成功后取 legacy write checkpoint 再出队（让服务端把本地写回显回来）。
type realUploadConnector struct {
	endpoint, rpc, box, org string
	rest                    *psclient.REST
	log                     *slog.Logger
	token                   string
}

func (c *realUploadConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	if c.token == "" {
		tok, err := psclient.FetchToken(ctx, c.rpc, c.box, c.org)
		if err != nil {
			return powersync.Credentials{}, err
		}
		c.token = tok
		c.rest.Token = tok
	}
	return powersync.Credentials{Endpoint: c.endpoint, Token: c.token}, nil
}

func (c *realUploadConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 50)
	if err != nil || batch == nil {
		return err
	}
	for _, e := range batch.Entries {
		if err := applyCrudEntry(ctx, c.rest, e); err != nil {
			var httpErr *psclient.HTTPError
			if errors.As(err, &httpErr) && httpErr.Fatal() {
				// 4xx（如 id 不是 uuid、NOT NULL 约束）重试也没用：记录后丢弃，避免堵住队列。
				fmt.Printf("  [上传-real] %-6s %s/%s 失败且不可重试，已丢弃: %v\n", e.Op, e.Table, e.ID, err)
				continue
			}
			return fmt.Errorf("上传 %s %s/%s: %w", e.Op, e.Table, e.ID, err)
		}
		fmt.Printf("  [上传-real] %-6s %s/%s\n", e.Op, e.Table, e.ID)
	}
	wc, err := fetchWriteCheckpoint(ctx, c.endpoint, c.token, db)
	if err != nil {
		return err
	}
	return batch.Complete(ctx, wc)
}

// applyCrudEntry 把一条 ps_crud 变更写到 PostgREST。
func applyCrudEntry(ctx context.Context, rest *psclient.REST, e powersync.CrudEntry) error {
	switch e.Op {
	case powersync.UpdateTypePut:
		row := map[string]any{"id": e.ID}
		for k, v := range e.Data {
			row[k] = v
		}
		return rest.Do(ctx, http.MethodPost, tablePath(e.Table), row, "resolution=merge-duplicates,return=minimal")
	case powersync.UpdateTypePatch:
		if len(e.Data) == 0 {
			return nil
		}
		return rest.Do(ctx, http.MethodPatch,
			tablePath(e.Table)+"?id=eq."+url.QueryEscape(e.ID), e.Data, "return=minimal")
	case powersync.UpdateTypeDelete:
		return rest.Do(ctx, http.MethodDelete,
			tablePath(e.Table)+"?id=eq."+url.QueryEscape(e.ID), nil, "return=minimal")
	default:
		return fmt.Errorf("未知 op %q", e.Op)
	}
}

func fetchWriteCheckpoint(ctx context.Context, endpoint, token string, db *powersync.DB) (string, error) {
	clientID, err := db.ClientID(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		endpoint+"/write-checkpoint2.json?client_id="+url.QueryEscape(clientID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Token "+token)
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

// ---------------------------------------------------------------- 工具

// newID 生成 UUIDv4（真实后端的主键列多为 uuid 类型，探针行也必须用它）。
func newID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", time.Now().UnixNano()%1e12)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func tablePath(table string) string { return "/" + url.PathEscape(table) }

func nonEmpty(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func waitQueueEmpty(ctx context.Context, db *powersync.DB) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := db.UploadQueue(ctx); n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("  ⚠️ 上传队列没有在 10s 内清空")
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

// verifyAgainstBackend 把本地表与后台 PostgREST 逐行逐字段比对，证明"后台数据已自动同步到本地"。
//
// 规则与 cmd/psdiff 一致：时间戳/数值字符串/jsonb 文本等序列化差异会归一化；
// 软删除的行（deleted_at 非空）后台视图不展示，按"预期差异"单独计数。
func verifyAgainstBackend(ctx context.Context, db *powersync.DB, base, token string, tables []string, limit int) {
	rest := &psclient.REST{Base: strings.TrimRight(base, "/"), Token: token}
	fmt.Println("\n===== 校验：后台 REST → 本地（逐行比对）=====")
	allOK := true

	for _, table := range tables {
		localRows, err := db.GetAll(ctx, `SELECT * FROM `+quoteIdent(table))
		if err != nil {
			fmt.Printf("  表 %-20s 本地查询失败（不在 schema？）: %v\n", table, err)
			allOK = false
			continue
		}

		var rawRemote []map[string]json.RawMessage
		if err := rest.GetJSON(ctx, tablePath(table)+"?select=*&limit=20000", &rawRemote); err != nil {
			fmt.Printf("  表 %-20s 后台查询失败: %v\n", table, err)
			allOK = false
			continue
		}

		remote := make([]rowdiff.Row, 0, len(rawRemote))
		remoteIDs := make(map[string]struct{}, len(rawRemote))
		for _, r := range rawRemote {
			id := rawID(r["id"])
			remoteIDs[id] = struct{}{}
			remote = append(remote, rowdiff.Row{Table: table, Row: r})
		}

		local := make([]rowdiff.Row, 0, len(localRows))
		softDeleted := 0
		for _, r := range localRows {
			m := make(map[string]json.RawMessage, len(r))
			for k, v := range r {
				b, err := json.Marshal(v)
				if err != nil {
					continue
				}
				m[k] = b
			}
			id := rawID(m["id"])
			if _, ok := remoteIDs[id]; !ok && isSoftDeletedRow(m) {
				softDeleted++
				continue // 后台视图中看不到的软删除行：预期差异
			}
			local = append(local, rowdiff.Row{Table: table, Row: m})
		}

		res := rowdiff.Compare(local, remote, limit)
		same, onlyLocal, onlyRemote, fieldDiff := res.Totals()
		status := "OK"
		if !res.OK() {
			status = "DIFF"
			allOK = false
		}
		fmt.Printf("  表 %-20s %s  本地=%d 后台=%d 一致=%d 本地多=%d 后台多=%d 字段不同=%d 软删除=%d\n",
			table, status, len(localRows), len(rawRemote), same, onlyLocal, onlyRemote, fieldDiff, softDeleted)
		for _, d := range res.Diffs {
			fmt.Printf("      %s/%s: %s\n", d.Table, d.ID, d.Detail)
		}
	}

	if allOK {
		fmt.Println("  ✅ 后台数据已完整同步到本地（逐行逐字段一致）")
	} else {
		fmt.Println("  ❌ 存在差异（见上）——常见原因：sync rules 还没覆盖该表/该行、或服务端复制延迟")
	}
}

// rawID 取 JSON 字符串值（去掉引号）。
func rawID(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.Trim(string(raw), `"`)
}

// isSoftDeletedRow 报告该行是否软删除（deleted_at 非空 / is_deleted 为真）。
func isSoftDeletedRow(row map[string]json.RawMessage) bool {
	for _, k := range []string{"deleted_at", "is_deleted", "is_del"} {
		raw, ok := row[k]
		if !ok {
			continue
		}
		switch canon := rowdiff.Canon(raw); canon {
		case "<nil>", "n:0", "s:", "s:0", "s:false":
		default:
			return true
		}
	}
	return false
}
