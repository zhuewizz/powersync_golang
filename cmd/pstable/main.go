// Command pstable 用 **SDK 自己的读路径**查看本地库里的数据（不需要任何 SQLite 图形工具）。
//
// 它走的是 powersync.Open + db.GetAll，也就是 Flutter/JS 端 get()/getAll() 的同一条路径，
// 所以能回答"SDK 到底能不能读到这张表、有多少行、内容是什么"。
//
//	go run ./cmd/pstable -db ./out/verify.db -ls
//	go run ./cmd/pstable -db ./out/verify.db -table cutter_info -limit 5
//	go run ./cmd/pstable -db ./out/verify.db -table cutter_info -format json
//	go run ./cmd/pstable -db ./out/verify.db -q "select mat_code, mat_name from cutter_info limit 10"
//
// 只读本地数据，不连接同步服务（不会上传、不会改库）。
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	powersync "github.com/zhuweizz/powersync_golang"
	"github.com/zhuweizz/powersync_golang/internal/psclient"
	"github.com/zhuweizz/powersync_golang/internal/rowdiff"
	"github.com/zhuweizz/powersync_golang/restconn"
)

func main() {
	var (
		dbPath  = flag.String("db", "", "本地库文件（必填）")
		schemaF = flag.String("schema", "", "schema JSON；缺省时从库里的 ps_kv.schema_json 自动读取")
		table   = flag.String("table", "", "要打印的表（可写视图名，如 cutter_info）")
		query   = flag.String("q", "", "自定义 SQL（select，只读）")
		execSQL = flag.String("exec", "", "执行写 SQL（INSERT/UPDATE/DELETE，分号分隔多条）")
		repl    = flag.Bool("i", false, "交互式 SQL（读写都可以；输入 .q 退出）")
		list    = flag.Bool("ls", false, "列出 schema 里的表 + 行数")
		limit   = flag.Int("limit", 20, "最多打印多少行（0=全部）")
		format  = flag.String("format", "table", "输出格式：table|json|csv")
		full    = flag.Bool("full", false, "不截断长字段")
		dryRun  = flag.Bool("dry-run", false, "只打印将要执行的写 SQL，不真正执行")

		upload    = flag.Bool("upload", false, "写入后连接服务并把上传队列同步到后端（需要 -endpoint/-rpc/-box/-org/-rest）")
		endpoint  = flag.String("endpoint", os.Getenv("PS_URL"), "PowerSync 服务地址")
		rpc       = flag.String("rpc", os.Getenv("RPC_URL"), "fetch_credentials RPC")
		box       = flag.String("box", os.Getenv("BOX_CODE"), "box_code")
		org       = flag.String("org", os.Getenv("ORG_CODE"), "org_code")
		restBase  = flag.String("rest", os.Getenv("REST_URL"), "PostgREST base，如 http://host:8089/rest")
		verifyT   = flag.String("verify-table", "", "上传后核对这些表（逗号分隔）本地与后端行数")
		syncDown  = flag.Bool("sync", false, "连接服务并等待首次同步（下载到本地；需要 -endpoint/-rpc/-box/-org）")
		report    = flag.Bool("report", false, "逐表打印行数 + 样例数据（配合 -sync 看同步结果）")
		verifyAll = flag.Bool("verify-all", false, "逐表比对本地与后端行数（需要 -sync 或 -upload 拿 token）")
	)
	flag.Parse()

	writes := *execSQL != "" || *repl
	if *dbPath == "" {
		fatalf("需要 -db")
	}
	if !*list && *table == "" && *query == "" && !writes && !*report && !*syncDown {
		fatalf("需要 -ls / -table / -q / -exec / -i / -report / -sync 之一")
	}
	if _, err := os.Stat(*dbPath); err != nil && !*syncDown {
		fatalf("打开 %s: %v（同步新库请加 -sync）", *dbPath, err)
	}
	if *upload {
		for name, v := range map[string]string{"endpoint": *endpoint, "rpc": *rpc, "box": *box, "org": *org, "rest": *restBase} {
			if v == "" {
				fatalf("-upload 需要 -%s（或环境变量 PS_URL/RPC_URL/BOX_CODE/ORG_CODE/REST_URL）", name)
			}
		}
	}

	if *upload || *syncDown {
		if *endpoint == "" || *rpc == "" || *box == "" || *org == "" {
			fatalf("-upload/-sync 需要 -endpoint/-rpc/-box/-org（或环境变量 PS_URL/RPC_URL/BOX_CODE/ORG_CODE）")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	sch, err := loadSchema(ctx, *dbPath, *schemaF)
	if err != nil {
		fatalf("加载 schema: %v", err)
	}

	opts := powersync.Options{
		Filename: *dbPath,
		Schema:   sch,
		// 只读查看不抢锁（可与运行中的 App 并存）；要写入则必须正常加锁。
		NoFileLock: !writes,
	}
	var conn *restconn.Connector
	if *upload || *syncDown {
		conn = restconn.New(restconn.Config{
			Endpoint: *endpoint, RPC: *rpc, Box: *box, Org: *org, RESTBase: *restBase,
		})
		opts.Connector = conn
	}

	db, err := powersync.Open(ctx, opts)
	if err != nil {
		fatalf("SDK 打开数据库: %v", err)
	}
	defer db.Close(context.Background())

	// 元信息走 stderr，避免污染 -format csv/json 的 stdout。
	mode := "离线只读"
	if writes {
		mode = "可读写（本地）"
	}
	if *upload {
		mode = "可读写 + 自动上传"
	}
	if *syncDown {
		mode = "同步下载 + 可读写"
	}
	fmt.Fprintf(os.Stderr, "数据库: %s\nschema: %d 张表（来自 %s）\n", *dbPath, len(sch.Tables()), schemaSource(*schemaF))
	fmt.Fprintf(os.Stderr, "模式: %s（走 SDK 读/写路径；写入会进入上传队列）\n", mode)

	if *upload {
		if err := db.Connect(ctx); err != nil {
			fatalf("Connect: %v", err)
		}
		defer db.Disconnect()
		fmt.Fprintln(os.Stderr, "同步: 已连接，写入会自动上传")
	}
	if *syncDown {
		start := time.Now()
		if err := db.Connect(ctx); err != nil {
			fatalf("Connect: %v", err)
		}
		defer db.Disconnect()
		fmt.Fprintln(os.Stderr, "同步: 已连接，等待首次同步完成…")
		if err := db.WaitForFirstSync(ctx); err != nil {
			fatalf("WaitForFirstSync: %v", err)
		}
		st := db.CurrentStatus()
		fmt.Fprintf(os.Stderr, "同步完成: 耗时 %s，cursor=%s，buckets=%d\n",
			time.Since(start).Round(time.Millisecond), st.Cursor, st.Buckets)
		if n := db.SkippedUnknownOps(); n > 0 {
			fmt.Fprintf(os.Stderr, "⚠️ 有 %d 行因表不在 schema 中被跳过：%s\n",
				n, strings.Join(db.SkippedTables(), ", "))
		}
	}

	if *verifyAll {
		if conn == nil {
			fatalf("-verify-all 需要 -sync（或 -upload）以便连接后端核对")
		}
		if err := conn.EnsureToken(ctx); err != nil {
			fatalf("取 token: %v", err)
		}
		verifyAllTables(ctx, db, conn.REST(), sch.Tables())
		return
	}

	if *report {
		reportTables(ctx, db, sch.Tables(), *limit)
		return
	}

	switch {
	case *list:
		listTables(ctx, db, sch.Tables())
	case *repl:
		runREPL(ctx, db, *format, *full, *limit, *upload, *dryRun)
	case *execSQL != "":
		runScript(ctx, db, *execSQL, *format, *full, *limit, *dryRun, *upload)
	case *table != "":
		sql := fmt.Sprintf(`SELECT * FROM %s`, quote(*table))
		if *limit > 0 {
			sql += fmt.Sprintf(` LIMIT %d`, *limit)
		}
		printRows(ctx, db, *table, sql, *format, *full, *limit)
	default:
		printRows(ctx, db, "query", *query, *format, *full, *limit)
	}

	if *upload {
		if n, err := restconn.WaitQueueEmpty(ctx, db, 30*time.Second); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️ %v（剩 %d 条）\n", err, n)
		} else {
			fmt.Fprintln(os.Stderr, "同步: 上传队列已清空")
		}
		if *verifyT != "" {
			if err := conn.EnsureToken(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "后端核对跳过（取 token 失败: %v）\n", err)
			} else {
				verifyTables(ctx, db, conn.REST(), *verifyT)
			}
		}
	}
}

// verifyTables 核对“本地行数 vs 后端行数”，证明写入真的同步过去了。
func verifyTables(ctx context.Context, db *powersync.DB, rest *psclient.REST, list string) {
	fmt.Fprintln(os.Stderr, "\n后端核对：")
	for _, t := range strings.Split(list, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		var local int64
		if rows, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quote(t)); err == nil && len(rows) == 1 {
			local = toInt64(rows[0]["n"])
		}
		var remote []map[string]json.RawMessage
		var remoteN int64 = -1
		if err := rest.GetJSON(ctx, "/"+url.PathEscape(t)+"?select=id&limit=100000", &remote); err == nil {
			remoteN = int64(len(remote))
		}
		switch {
		case remoteN < 0:
			fmt.Fprintf(os.Stderr, "  %-24s 本地=%d 后端=查询失败\n", t, local)
		case local == remoteN:
			fmt.Fprintf(os.Stderr, "  %-24s 本地=%d 后端=%d ✅ 一致\n", t, local, remoteN)
		default:
			fmt.Fprintf(os.Stderr, "  %-24s 本地=%d 后端=%d ⚠️ 不一致\n", t, local, remoteN)
		}
	}
}

// loadSchema 优先用 -schema；否则从库里的 ps_kv.schema_json 自动读取
// （SDK 每次 Open 都会把 schema 规范 JSON 记在 ps_kv，因此老库可以自描述）。
func loadSchema(ctx context.Context, dbPath, schemaFile string) (*powersync.Schema, error) {
	if schemaFile != "" {
		raw, err := os.ReadFile(schemaFile)
		if err != nil {
			return nil, err
		}
		return powersync.LoadSchemaJSON(raw)
	}
	raw, err := storedSchemaJSON(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("%w（也可以显式传 -schema ./out/schema.json）", err)
	}
	return powersync.LoadSchemaJSON([]byte(raw))
}

func schemaSource(schemaFile string) string {
	if schemaFile != "" {
		return schemaFile
	}
	return "库内 ps_kv.schema_json"
}

// runScript 执行 -exec 传入的 SQL（分号分隔多条）。
func runScript(ctx context.Context, db *powersync.DB, script, format string, full bool, limit int, dryRun, uploading bool) {
	for _, stmt := range splitStatements(script) {
		runStatement(ctx, db, stmt, format, full, limit, dryRun, uploading)
	}
}

// runREPL 交互式：读一行/多条 SQL 并执行；.q 退出、.tables 列表、.schema <表> 看列。
func runREPL(ctx context.Context, db *powersync.DB, format string, full bool, limit int, uploading, dryRun bool) {
	fmt.Fprintln(os.Stderr, "\n交互模式：输入 SQL 以 ; 结尾执行；.tables 列表；.q 退出")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Fprint(os.Stderr, "ps> ")
		} else {
			fmt.Fprint(os.Stderr, "...> ")
		}
		if !scanner.Scan() {
			fmt.Fprintln(os.Stderr)
			// 管道/文件里最后一条语句可能没写分号：EOF 时把残留内容执行掉。
			if rest := strings.TrimSpace(buf.String()); rest != "" {
				for _, stmt := range splitStatements(rest) {
					runStatement(ctx, db, stmt, format, full, limit, dryRun, uploading)
				}
			}
			return
		}
		line := strings.TrimPrefix(scanner.Text(), "\ufeff") // 去掉管道/文件开头的 UTF-8 BOM
		trimmed := strings.TrimSpace(line)
		if buf.Len() == 0 {
			switch strings.ToLower(trimmed) {
			case ".q", ".quit", ".exit", "exit", "quit":
				return
			case ".tables":
				listTables(ctx, db, db.Schema().Tables())
				continue
			}
			if strings.HasPrefix(trimmed, ".schema ") {
				showTableSchema(ctx, db, strings.TrimSpace(trimmed[len(".schema "):]))
				continue
			}
		}
		buf.WriteString(line)
		buf.WriteString("\n")
		if !strings.Contains(line, ";") {
			continue
		}
		for _, stmt := range splitStatements(buf.String()) {
			runStatement(ctx, db, stmt, format, full, limit, dryRun, uploading)
		}
		buf.Reset()
	}
}

// runStatement 执行单条语句：SELECT/PRAGMA/EXPLAIN 走查询，其余走 Execute（写入会进上传队列）。
func runStatement(ctx context.Context, db *powersync.DB, stmt, format string, full bool, limit int, dryRun, uploading bool) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return
	}
	if isQuery(stmt) {
		printRows(ctx, db, abbreviate(stmt, 60), stmt, format, full, limit)
		return
	}
	if dryRun {
		fmt.Printf("[dry-run] %s\n", stmt)
		return
	}
	n, err := db.Execute(ctx, stmt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "✗ %s\n  %v\n", abbreviate(stmt, 100), err)
		return
	}
	hint := ""
	if uploading {
		hint = "（已进上传队列，会自动上传）"
	} else if q, _ := db.UploadQueue(ctx); q > 0 {
		hint = fmt.Sprintf("（上传队列 %d 条；加 -upload 才会同步到后端）", q)
	}
	if n == 0 {
		// 同步表是“视图 + INSTEAD OF 触发器”，SQLite 拿不到行数（不是没写进去）。
		fmt.Printf("✓ 已执行  %s%s\n", abbreviate(stmt, 80), hint)
		return
	}
	fmt.Printf("✓ %d 行受影响  %s%s\n", n, abbreviate(stmt, 80), hint)
}

func isQuery(stmt string) bool {
	up := strings.ToUpper(strings.TrimLeft(stmt, "( \t\n"))
	for _, prefix := range []string{"SELECT", "WITH", "PRAGMA", "EXPLAIN"} {
		if strings.HasPrefix(up, prefix) {
			// INSERT ... WITH / UPDATE ... 不会这么开头，足够用。
			return true
		}
	}
	return false
}

// splitStatements 按分号切分（忽略字符串/注释里的分号）。
func splitStatements(script string) []string {
	var (
		out     []string
		cur     strings.Builder
		inStr   byte
		comment bool
	)
	for i := 0; i < len(script); i++ {
		c := script[i]
		switch {
		case comment:
			cur.WriteByte(c)
			if c == '\n' {
				comment = false
			}
		case inStr != 0:
			cur.WriteByte(c)
			if c == inStr {
				if i+1 < len(script) && script[i+1] == inStr {
					cur.WriteByte(script[i+1])
					i++
				} else {
					inStr = 0
				}
			}
		case c == '-' && i+1 < len(script) && script[i+1] == '-':
			comment = true
			cur.WriteString("--")
			i++
		case c == '\'' || c == '"' || c == '`':
			inStr = c
			cur.WriteByte(c)
		case c == ';':
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if strings.TrimSpace(cur.String()) != "" {
		out = append(out, cur.String())
	}
	return out
}

// showTableSchema 打印某张表的列（来自 SDK schema）。
func showTableSchema(ctx context.Context, db *powersync.DB, table string) {
	for _, t := range db.Schema().Tables() {
		if !strings.EqualFold(t.Name, table) {
			continue
		}
		fmt.Printf("表 %s（同步=%v，仅本地=%v）列：\n", t.Name, !t.LocalOnly, t.LocalOnly)
		fmt.Printf("  %-24s %s\n", "id", "主键（TEXT）")
		for _, c := range t.Columns {
			fmt.Printf("  %-24s %s\n", c.Name, c.Type)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "schema 里没有表 %q\n", table)
}

func abbreviate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// reportTables 逐表打印“行数 + 样例数据”（看同步结果用）。
func reportTables(ctx context.Context, db *powersync.DB, tables []powersync.Table, limit int) {
	if limit <= 0 {
		limit = 2
	}
	var total int64
	empty := 0
	fmt.Printf("\n%-30s %8s  样例\n%s\n", "表", "行数", strings.Repeat("-", 78))
	for _, t := range tables {
		var n int64
		if rows, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quote(t.Name)); err == nil && len(rows) == 1 {
			n = toInt64(rows[0]["n"])
		}
		total += n
		if n == 0 {
			empty++
		}

		sample := ""
		if n > 0 {
			if rows, err := db.GetAll(ctx, fmt.Sprintf(`SELECT * FROM %s LIMIT %d`, quote(t.Name), limit)); err == nil {
				lines := make([]string, 0, len(rows))
				for _, r := range rows {
					b, _ := json.Marshal(sortedRow(r))
					lines = append(lines, truncate(string(b), 180))
				}
				sample = strings.Join(lines, "\n"+strings.Repeat(" ", 41))
			}
		}
		fmt.Printf("%-30s %8d  %s\n", t.Name, n, sample)
	}
	fmt.Printf("%s\n%-30s %8d  （空表 %d 张，共 %d 张）\n",
		strings.Repeat("-", 78), "合计", total, empty, len(tables))
}

// sortedRow 把 map 变成按 key 有序的 map（json.Marshal 对 map 已按 key 排序，这里只是显式化）。
func sortedRow(r map[string]any) map[string]any { return r }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// storedSchemaJSON 从已有库里读回 schema（只读连接，不经过 SDK）。
// SDK 每次 Open 都会把规范 JSON 记进 ps_kv，所以老库也能自描述。
func storedSchemaJSON(ctx context.Context, dbPath string) (string, error) {
	dsn := "file:" + url.PathEscape(dbPath) + "?mode=ro"
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return "", err
	}
	defer sqlDB.Close()
	var raw string
	if err := sqlDB.QueryRowContext(ctx,
		`SELECT value FROM ps_kv WHERE key = 'schema_json'`).Scan(&raw); err != nil {
		return "", fmt.Errorf("读 ps_kv.schema_json: %w", err)
	}
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("库里的 ps_kv.schema_json 为空")
	}
	return raw, nil
}

func listTables(ctx context.Context, db *powersync.DB, tables []powersync.Table) {
	fmt.Printf("\n%-28s %-8s %8s\n", "表", "类型", "行数")
	fmt.Println(strings.Repeat("-", 48))
	total := 0
	for _, t := range tables {
		var n int64
		if rows, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quote(t.Name)); err == nil && len(rows) == 1 {
			n = toInt64(rows[0]["n"])
		}
		kind := "同步"
		if t.LocalOnly {
			kind = "仅本地"
		}
		mark := ""
		if n == 0 {
			mark = "  ← 空"
		}
		fmt.Printf("%-28s %-8s %8d%s\n", t.Name, kind, n, mark)
		total += int(n)
	}
	fmt.Printf("%-28s %-8s %8d\n", "合计", "", total)
}

func printRows(ctx context.Context, db *powersync.DB, label, sql, format string, full bool, limit int) {
	start := time.Now()
	rows, err := db.GetAll(ctx, sql)
	if err != nil {
		fatalf("查询失败: %v", err)
	}
	if len(rows) == 0 {
		fmt.Printf("\n%s: 0 行\n", label)
		return
	}

	cols := columnNames(rows)
	switch format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rows)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write(cols)
		for _, r := range rows {
			rec := make([]string, len(cols))
			for i, c := range cols {
				rec[i] = rawCell(r[c])
			}
			_ = w.Write(rec)
		}
		w.Flush()
	default:
		printAligned(cols, rows, full)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n%s: %d 行", label, len(rows))
	if limit > 0 && len(rows) == limit {
		fmt.Printf("（已按 -limit %d 截断）", limit)
	}
	fmt.Printf("，耗时 %s\n", elapsed.Round(time.Microsecond))
}

func printAligned(cols []string, rows []map[string]any, full bool) {
	const maxWidth = 42
	widths := make([]int, len(cols))
	cells := make([][]string, len(rows))
	for i, c := range cols {
		widths[i] = dispWidth(c)
	}
	for ri, r := range rows {
		cells[ri] = make([]string, len(cols))
		for ci, c := range cols {
			s := cell(r[c], full, maxWidth)
			if w := dispWidth(s); w > widths[ci] {
				widths[ci] = w
			}
			cells[ri][ci] = s
		}
	}
	pad := func(s string, w int) string { return s + strings.Repeat(" ", w-dispWidth(s)) }

	head := make([]string, len(cols))
	for i, c := range cols {
		head[i] = pad(c, widths[i])
	}
	fmt.Println(strings.Join(head, "  "))
	total := 0
	for _, w := range widths {
		total += w + 2
	}
	fmt.Println(strings.Repeat("-", total))
	for _, row := range cells {
		out := make([]string, len(row))
		for i, s := range row {
			out[i] = pad(s, widths[i])
		}
		fmt.Println(strings.Join(out, "  "))
	}
}

func columnNames(rows []map[string]any) []string {
	seen := map[string]struct{}{}
	var cols []string
	for _, r := range rows {
		for k := range r {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// cell 把值转成可读字符串；nil 显示为 NULL，空串显示为 ”（便于区分）。
func cell(v any, full bool, maxWidth int) string {
	var s string
	switch x := v.(type) {
	case nil:
		s = "NULL"
	case string:
		if x == "" {
			s = "''"
		} else {
			s = x
		}
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(v)
	}
	if !full && maxWidth > 0 && dispWidth(s) > maxWidth {
		s = truncateWidth(s, maxWidth-1) + "…"
	}
	return strings.ReplaceAll(s, "\n", " ")
}

// rawCell 给 csv 用：NULL 与空串都输出空（不写展示用的占位符）。
func rawCell(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}

func truncateWidth(s string, max int) string {
	w := 0
	for i, r := range s {
		w += runeWidth(r)
		if w > max {
			return s[:i]
		}
	}
	return s
}

func dispWidth(s string) int {
	w := 0
	for _, r := range s {
		w += runeWidth(r)
	}
	return w
}

func runeWidth(r rune) int {
	switch {
	case r < 0x1100:
		return 1
	case r >= 0x1100 && r <= 0x115F, // Hangul Jamo
		r >= 0x2E80 && r <= 0xA4CF, // CJK 部首/汉字/假名
		r >= 0xAC00 && r <= 0xD7A3, // Hangul
		r >= 0xF900 && r <= 0xFAFF, // CJK 兼容
		r >= 0xFE30 && r <= 0xFE6F,
		r >= 0xFF00 && r <= 0xFF60, // 全角
		r >= 0xFFE0 && r <= 0xFFE6,
		r >= 0x20000 && r <= 0x3FFFD:
		return 2
	}
	return 1
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "pstable: "+format+"\n", args...)
	os.Exit(1)
}

// verifyAllTables 校验下载侧：本地每一行都必须能在后端找到且字段一致。
//
// 为什么不直接比行数：同步规则本身会过滤（org_data 只放本 org、box_data 只放本柜），
// 后端行数通常**多于**本地，那是正常的。所以这里做“本地 ⊆ 后端”的子集校验：
//   - 本地缺/字段不同  → ❌ 真问题
//   - 只在后端存在      → ℹ️ 被 sync rules 过滤（正常）
//   - REST 不暴露该表   → ⚠️ 无法核对（如短别名 i/t、role）
func verifyAllTables(ctx context.Context, db *powersync.DB, rest *psclient.REST, tables []powersync.Table) {
	fmt.Printf("\n%-28s %8s %8s %10s %s\n%s\n", "表", "本地", "后端", "本地缺/差异", "后端多（规则过滤）", strings.Repeat("-", 78))
	var okN, filteredN, skipN, badN int
	for _, t := range tables {
		localSQL := `SELECT * FROM ` + quote(t.Name)
		if hasColumn(t, "deleted_at") {
			localSQL += ` WHERE deleted_at IS NULL`
		}
		localRows, err := db.GetAll(ctx, localSQL)
		if err != nil {
			fmt.Printf("%-28s %8s %8s %10s  ⚠️ 本地查询失败: %v\n", t.Name, "-", "-", "-", err)
			skipN++
			continue
		}
		local := make([]rowdiff.Row, 0, len(localRows))
		for _, r := range localRows {
			m := make(map[string]json.RawMessage, len(r))
			for k, v := range r {
				if b, err := json.Marshal(v); err == nil {
					m[k] = b
				}
			}
			local = append(local, rowdiff.Row{Table: t.Name, Row: m})
		}

		var remoteRaw []map[string]json.RawMessage
		if err := rest.GetJSON(ctx, "/"+url.PathEscape(t.Name)+"?select=*&limit=200000", &remoteRaw); err != nil {
			fmt.Printf("%-28s %8d %8s %10s  ⚠️ 后端不暴露/REST 不可达\n", t.Name, len(localRows), "-", "-")
			skipN++
			continue
		}
		remote := make([]rowdiff.Row, 0, len(remoteRaw))
		for _, r := range remoteRaw {
			remote = append(remote, rowdiff.Row{Table: t.Name, Row: r})
		}

		res := rowdiff.Compare(local, remote, 3)
		_, onlyLocal, onlyRemote, fieldDiff := res.Totals()
		switch {
		case onlyLocal > 0 || fieldDiff > 0:
			fmt.Printf("%-28s %8d %8d %10s  ❌ 本地缺 %d，字段不同 %d\n",
				t.Name, len(localRows), len(remoteRaw), "有问题", onlyLocal, fieldDiff)
			badN++
			for _, d := range res.Diffs {
				fmt.Printf("    %s/%s: %s\n", d.Table, d.ID, d.Detail)
			}
		case onlyRemote > 0:
			fmt.Printf("%-28s %8d %8d %10s  ℹ️ 后端多 %d（sync rules 过滤，正常）\n",
				t.Name, len(localRows), len(remoteRaw), "0/0", onlyRemote)
			filteredN++
		default:
			fmt.Printf("%-28s %8d %8d %10s  ✅\n", t.Name, len(localRows), len(remoteRaw), "0/0")
			okN++
		}
	}
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("共 %d 张表：✅ 完全一致 %d，ℹ️ 后端更多（规则过滤）%d，⚠️ 无法核对 %d，❌ 有问题 %d\n",
		len(tables), okN, filteredN, skipN, badN)
	if badN == 0 {
		fmt.Println("✅ 下载侧校验通过：本地每一行都能在后端找到，且字段一致")
	} else {
		fmt.Println("❌ 有表不一致：本地存在后端没有的行，或字段不同（检查 sync rules / 值归一化）")
	}
}

func hasColumn(t powersync.Table, name string) bool {
	for _, c := range t.Columns {
		if c.Name == name {
			return true
		}
	}
	return false
}
