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

	powersync "msmkSync"
)

func main() {
	var (
		dbPath  = flag.String("db", "", "本地库文件（必填）")
		schemaF = flag.String("schema", "", "schema JSON；缺省时从库里的 ps_kv.schema_json 自动读取")
		table   = flag.String("table", "", "要打印的表（可写视图名，如 cutter_info）")
		query   = flag.String("q", "", "自定义 SQL（select）")
		list    = flag.Bool("ls", false, "列出 schema 里的表 + 行数")
		limit   = flag.Int("limit", 20, "最多打印多少行（0=全部）")
		format  = flag.String("format", "table", "输出格式：table|json|csv")
		full    = flag.Bool("full", false, "不截断长字段")
	)
	flag.Parse()

	if *dbPath == "" {
		fatalf("需要 -db")
	}
	if !*list && *table == "" && *query == "" {
		fatalf("需要 -ls / -table / -q 之一")
	}
	if _, err := os.Stat(*dbPath); err != nil {
		fatalf("打开 %s: %v", *dbPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sch, err := loadSchema(ctx, *dbPath, *schemaF)
	if err != nil {
		fatalf("加载 schema: %v", err)
	}

	db, err := powersync.Open(ctx, powersync.Options{
		Filename:   *dbPath,
		Schema:     sch,
		NoFileLock: true, // 只读查看，不抢锁（避免与正在运行的 App 冲突）
	})
	if err != nil {
		fatalf("SDK 打开数据库: %v", err)
	}
	defer db.Close(context.Background())

	// 元信息走 stderr，避免污染 -format csv/json 的 stdout。
	fmt.Fprintf(os.Stderr, "数据库: %s\nschema: %d 张表（来自 %s）\n", *dbPath, len(sch.Tables()), schemaSource(*schemaF))
	fmt.Fprintln(os.Stderr, "模式: 离线只读（未连接同步服务；数据来自本地 SQLite）")

	switch {
	case *list:
		listTables(ctx, db, sch.Tables())
	case *table != "":
		sql := fmt.Sprintf(`SELECT * FROM %s`, quote(*table))
		if *limit > 0 {
			sql += fmt.Sprintf(` LIMIT %d`, *limit)
		}
		printRows(ctx, db, *table, sql, *format, *full, *limit)
	default:
		printRows(ctx, db, "query", *query, *format, *full, *limit)
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
