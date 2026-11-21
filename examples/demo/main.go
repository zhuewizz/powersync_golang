// Command demo 是**交互式**的双向同步演示：
//
//	go run ./examples/demo
//
// 流程（参数默认写死，和 examples/todo 一致）：
//  1. 验证：取 token、打开本地库、连接同步、等首次同步、检查有没有被跳过的表
//  2. 列出**有数据的表**（行数 > 0），带编号
//  3. 输入编号 → 打印该表的数据（默认最多 20 行，可 -all 打全部）
//  4. 回到列表，可继续看别的表；q 退出
//
// 同步是自动的：程序启动即连接，之后服务端的变更会持续下发；本程序只读不写。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	powersync "msmkSync"
	"msmkSync/examples/cutterschema"
	"msmkSync/internal/restconn"
)

func main() {
	var (
		dbPath   = flag.String("db", "out/demo.db", "本地数据库文件（不存在则新建并全量同步）")
		endpoint = flag.String("endpoint", envOr("PS_URL", "http://120.24.7.22:8080"), "PowerSync 服务地址")
		rpc      = flag.String("rpc", envOr("RPC_URL", "http://120.24.7.22:8089/rest/rpc/fetch_credentials"), "fetch_credentials RPC")
		box      = flag.String("box", envOr("BOX_CODE", "D1-2"), "box_code")
		org      = flag.String("org", envOr("ORG_CODE", "ORG001"), "org_code")
		rest     = flag.String("rest", envOr("REST_URL", "http://120.24.7.22:8089/rest"), "PostgREST base（仅用于展示，可留空）")
		limit    = flag.Int("limit", 20, "每张表最多打印多少行（0 = 全部）")
		all      = flag.Bool("all", false, "列出所有表（默认只列有数据的）")
		cols     = flag.String("cols", "", "只显示这些列（逗号分隔），如 id,mat_code,mat_name")
		once     = flag.Bool("once", false, "只同步并打印一次表列表就退出（非交互，便于脚本化）")
		timeout  = flag.Duration("timeout", 120*time.Second, "等待首次同步的超时")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// ---------------------------------------------------------------- 1. 打开 + 连接
	fmt.Println("===== 1/3 验证环境 =====")
	conn := restconn.New(restconn.Config{
		Endpoint: *endpoint, RPC: *rpc, Box: *box, Org: *org, RESTBase: *rest,
	})
	db, err := powersync.Open(ctx, powersync.Options{
		Filename:  *dbPath,
		Schema:    cutterschema.Schema(),
		Connector: conn,
	})
	if err != nil {
		fail("打开数据库", err)
	}
	defer db.Close(context.Background())

	fmt.Printf("  本地库: %s\n", *dbPath)
	fmt.Printf("  schema: %d 张表（来自 syncRules.yaml）\n", len(cutterschema.Schema().Tables()))

	start := time.Now()
	if err := db.Connect(ctx); err != nil {
		fail("Connect", err)
	}
	defer db.Disconnect()
	fmt.Println("  已连接，等待首次同步…")
	if err := db.WaitForFirstSync(ctx); err != nil {
		fail("等待首次同步", err)
	}
	st := db.CurrentStatus()
	fmt.Printf("  ✅ 首次同步完成：耗时 %s，cursor=%s，buckets=%d\n",
		time.Since(start).Round(time.Millisecond), st.Cursor, st.Buckets)
	if n := db.SkippedUnknownOps(); n > 0 {
		fmt.Printf("  ⚠️ 有 %d 行因表不在 schema 中被跳过：%s\n", n, strings.Join(db.SkippedTables(), ", "))
	} else {
		fmt.Println("  ✅ 没有被跳过的行（schema 覆盖了服务端下发的所有表）")
	}

	// ---------------------------------------------------------------- 2. 表清单
	tables, err := listTables(ctx, db, *all)
	if err != nil {
		fail("统计表", err)
	}
	printTableMenu(tables, *all)
	if len(tables) == 0 {
		fmt.Println("\n没有可展示的表，退出。")
		return
	}
	if *once {
		return
	}

	// ---------------------------------------------------------------- 3. 交互选择
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for {
		fmt.Printf("\n输入表编号查看数据（回车刷新列表，q 退出）：")
		if !in.Scan() {
			fmt.Println()
			return
		}
		choice := strings.TrimSpace(in.Text())
		switch strings.ToLower(choice) {
		case "q", "quit", "exit":
			fmt.Println("再见。")
			return
		case "":
			if tables, err = listTables(ctx, db, *all); err == nil {
				printTableMenu(tables, *all)
			}
			continue
		case "r", "refresh":
			if tables, err = listTables(ctx, db, *all); err == nil {
				printTableMenu(tables, *all)
			}
			continue
		}

		n, err := strconv.Atoi(choice)
		if err != nil || n < 1 || n > len(tables) {
			fmt.Printf("  ⚠️ 无效编号：%q（应该 1..%d，或 q 退出）\n", choice, len(tables))
			continue
		}
		showTable(ctx, db, tables[n-1], *limit, *cols)
	}
}

// aliasTables 是同步规则里用短别名（`AS i` 等）的表：服务端下发的 object_type 就是别名。
var aliasTables = map[string]bool{"d": true, "h": true, "i": true, "l": true, "r": true, "s": true, "t": true}

// tableInfo 是表清单里的一项。
type tableInfo struct {
	Name string
	Rows int64
}

// listTables 统计各表行数；默认只返回有数据的表（-all 则返回全部）。
func listTables(ctx context.Context, db *powersync.DB, includeEmpty bool) ([]tableInfo, error) {
	all := cutterschema.Schema().Tables()
	out := make([]tableInfo, 0, len(all))
	var total int64
	for _, t := range all {
		n, err := countRows(ctx, db, t.Name)
		if err != nil {
			continue // 表可能还没同步下来（或本地查询失败），跳过
		}
		total += n
		if !includeEmpty && n == 0 {
			continue
		}
		out = append(out, tableInfo{Name: t.Name, Rows: n})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Rows != out[j].Rows {
			return out[i].Rows > out[j].Rows // 行数多的排前面
		}
		return out[i].Name < out[j].Name
	})
	fmt.Printf("\n===== 2/3 表清单（%d 张%s，合计 %d 行）=====\n",
		len(out), map[bool]string{true: "（含空表）", false: "有数据"}[includeEmpty], total)
	return out, nil
}

func printTableMenu(tables []tableInfo, includeEmpty bool) {
	if len(tables) == 0 {
		fmt.Println("  （没有行数 > 0 的表；加 -all 可以看全部表）")
		return
	}
	fmt.Printf("%4s  %-32s %8s\n", "#", "表", "行数")
	fmt.Println(strings.Repeat("-", 48))
	hasAlias := false
	for i, t := range tables {
		mark := ""
		if aliasTables[t.Name] {
			mark = "  *"
			hasAlias = true
		}
		fmt.Printf("%4d  %-32s %8d%s\n", i+1, t.Name, t.Rows, mark)
	}
	if hasAlias {
		fmt.Println("\n  * 同步规则里的短别名表（`AS i` 之类）：实体分散在 material_*/tool_*/delivery_*，")
		fmt.Println("    服务端下发的 object_type 就是别名，所以只能归到这一张表。")
	}
}

// showTable 打印一张表的数据（列对齐，中文按 2 字符宽）。
func showTable(ctx context.Context, db *powersync.DB, t tableInfo, limit int, cols string) {
	proj := "*"
	if c := splitCSV(cols); len(c) > 0 {
		quoted := make([]string, 0, len(c))
		for _, name := range c {
			quoted = append(quoted, quoteIdent(name))
		}
		proj = strings.Join(quoted, ", ")
	}
	sql := `SELECT ` + proj + ` FROM ` + quoteIdent(t.Name)
	if limit > 0 {
		sql += fmt.Sprintf(` LIMIT %d`, limit)
	}
	start := time.Now()
	rows, err := db.GetAll(ctx, sql)
	if err != nil {
		fmt.Printf("\n  ❌ 查询 %s 失败：%v\n", t.Name, err)
		return
	}
	fmt.Printf("\n===== 3/3 表 %s（共 %d 行，显示 %d 行）=====\n", t.Name, t.Rows, len(rows))
	if len(rows) == 0 {
		fmt.Println("  （空表）")
		return
	}
	printRows(rows, 42)
	fmt.Printf("\n  查询耗时 %s\n", time.Since(start).Round(time.Microsecond))
}

// printRows 打一张对齐的表；maxWidth 控制单列最大宽度（0 = 不截断）。
func printRows(rows []map[string]any, maxWidth int) {
	cols := columnNames(rows)
	widths := make([]int, len(cols))
	cells := make([][]string, len(rows))
	for i, c := range cols {
		widths[i] = dispWidth(c)
	}
	for ri, r := range rows {
		cells[ri] = make([]string, len(cols))
		for ci, c := range cols {
			s := cellText(r[c], maxWidth)
			if w := dispWidth(s); w > widths[ci] {
				widths[ci] = w
			}
			cells[ri][ci] = s
		}
	}
	pad := func(s string, w int) string { return s + strings.Repeat(" ", w-dispWidth(s)) }

	head := make([]string, len(cols))
	total := 0
	for i, c := range cols {
		head[i] = pad(c, widths[i])
		total += widths[i] + 2
	}
	fmt.Println(strings.Join(head, "  "))
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
	// id 放最前面，方便对照
	sort.SliceStable(cols, func(i, j int) bool {
		if cols[i] == "id" {
			return true
		}
		if cols[j] == "id" {
			return false
		}
		return cols[i] < cols[j]
	})
	return cols
}

func cellText(v any, maxWidth int) string {
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
	case json.Number:
		s = x.String()
	default:
		s = fmt.Sprint(v)
	}
	s = strings.ReplaceAll(s, "\n", " ")
	if maxWidth > 0 && dispWidth(s) > maxWidth {
		s = truncateWidth(s, maxWidth-1) + "…"
	}
	return s
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
	case r >= 0x1100 && r <= 0x115F,
		r >= 0x2E80 && r <= 0xA4CF,
		r >= 0xAC00 && r <= 0xD7A3,
		r >= 0xF900 && r <= 0xFAFF,
		r >= 0xFE30 && r <= 0xFE6F,
		r >= 0xFF00 && r <= 0xFF60,
		r >= 0xFFE0 && r <= 0xFFE6,
		r >= 0x20000 && r <= 0x3FFFD:
		return 2
	}
	return 1
}

// countRows 统计一张表的行数（软删除的行不算，跟列表展示保持一致）。
func countRows(ctx context.Context, db *powersync.DB, table string) (int64, error) {
	// 大部分业务表都有 deleted_at；没有该列时退化为全表计数
	if rows, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quoteIdent(table)+` WHERE deleted_at IS NULL`); err == nil && len(rows) == 1 {
		return toInt64(rows[0]["n"]), nil
	}
	rows, err := db.GetAll(ctx, `SELECT count(*) AS n FROM `+quoteIdent(table))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.New("count 查询没有返回结果")
	}
	return toInt64(rows[0]["n"]), nil
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func splitCSV(s string) []string {
	out := make([]string, 0, 4)
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
	os.Exit(1)
}
