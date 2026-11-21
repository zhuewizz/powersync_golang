// Command psq 是开发用的小工具：直接对本地 SQLite 执行一条查询。
//
// 用途：核对 local-first 数据到底有没有落到本地库（不经过 SDK、不需要网络）。
//
//	go run ./cmd/psq -db ./out/verify.db -ls
//	go run ./cmd/psq -db ./out/verify.db -q "select count(*) as n from cutter_info"
//	go run ./cmd/psq -db ./out/verify.db -q "select id, mat_code, mat_name from cutter_info limit 3"
//	go run ./cmd/psq -db ./out/verify.db -q "..." -json
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

func main() {
	var (
		dbPath = flag.String("db", "", "SQLite 文件路径（必填，只读打开）")
		query  = flag.String("q", "", "要执行的 SQL（select）")
		ls     = flag.Bool("ls", false, "列出库里的表/视图（用户对象 + ps_* 内部对象）")
		asJSON = flag.Bool("json", false, "以 JSON 输出（每行一条）")
		snap   = flag.String("snapshot", "", "生成一致快照文件（VACUUM INTO），供图形工具打开")
		check  = flag.Bool("check", false, "执行 PRAGMA integrity_check")
	)
	flag.Parse()
	if *dbPath == "" {
		fatalf("需要 -db")
	}
	if *snap == "" && !*ls && !*check && *query == "" {
		fatalf("需要 -q / -ls / -check / -snapshot")
	}
	if _, err := os.Stat(*dbPath); err != nil {
		fatalf("打开 %s: %v", *dbPath, err)
	}

	// VACUUM INTO 需要写入新文件，因此快照模式不能用只读连接。
	mode := "ro"
	if *snap != "" {
		mode = "rw"
	}
	dsn := "file:" + url.PathEscape(*dbPath) + "?mode=" + mode
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		fatalf("打开数据库: %v", err)
	}
	defer db.Close()

	switch {
	case *snap != "":
		makeSnapshot(db, *snap)
	case *check:
		checkIntegrity(db)
	case *ls:
		listObjects(db)
	default:
		runQuery(db, *query, *asJSON)
	}
}

// makeSnapshot 用 VACUUM INTO 生成单文件快照：没有 -wal/-shm、不会锁原库，
// 可以直接用 DB Browser / SQLiteStudio 打开（避免 "database disk image is malformed"）。
func makeSnapshot(db *sql.DB, out string) {
	if _, err := os.Stat(out); err == nil {
		fatalf("%s 已存在，先删掉或换个名字", out)
	}
	if _, err := db.Exec(`VACUUM INTO ?`, out); err != nil {
		fatalf("VACUUM INTO %s: %v（若报只读/占用，先关闭正在使用该库的程序）", out, err)
	}
	info, err := os.Stat(out)
	if err != nil {
		fatalf("校验快照: %v", err)
	}
	fmt.Printf("已生成快照: %s（%.1f MB，单文件、无 WAL）\n", out, float64(info.Size())/(1<<20))
	fmt.Println("  用 DB Browser for SQLite / SQLiteStudio 直接打开它即可（不会锁原库）")
}

// checkIntegrity 执行完整性检查（排查 SQLITE_CORRUPT=11 这类错误）。
func checkIntegrity(db *sql.DB) {
	rows, err := db.Query(`PRAGMA integrity_check`)
	if err != nil {
		fatalf("integrity_check: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			fatalf("scan: %v", err)
		}
		fmt.Println("integrity_check:", line)
	}
}

func listObjects(db *sql.DB) {
	rows, err := db.Query(`SELECT type, name FROM sqlite_master
	  WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		fatalf("查询 sqlite_master: %v", err)
	}
	defer rows.Close()
	var user, internal []string
	for rows.Next() {
		var typ, name string
		if err := rows.Scan(&typ, &name); err != nil {
			fatalf("scan: %v", err)
		}
		line := fmt.Sprintf("%-6s %s", typ, name)
		if strings.HasPrefix(name, "ps_") || strings.HasPrefix(name, "powersync_") {
			internal = append(internal, line)
		} else {
			user = append(user, line)
		}
	}
	sort.Strings(user)
	sort.Strings(internal)

	fmt.Printf("用户表/视图（%d）：\n", len(user))
	printCounts(db, user)
	fmt.Printf("\n内部表（%d）：\n", len(internal))
	printCounts(db, internal)
}

func printCounts(db *sql.DB, lines []string) {
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		var n int64
		if err := db.QueryRow(`SELECT count(*) FROM ` + quote(fields[1])).Scan(&n); err != nil {
			fmt.Printf("  %s\n", line)
			continue
		}
		fmt.Printf("  %s  rows=%d\n", line, n)
	}
}

func runQuery(db *sql.DB, query string, asJSON bool) {
	rows, err := db.Query(query)
	if err != nil {
		fatalf("执行: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		fatalf("列信息: %v", err)
	}

	count := 0
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fatalf("scan: %v", err)
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[c] = string(b)
			} else {
				m[c] = vals[i]
			}
		}
		if asJSON {
			b, _ := json.Marshal(m)
			fmt.Println(string(b))
		} else {
			parts := make([]string, 0, len(cols))
			for _, c := range cols {
				parts = append(parts, fmt.Sprintf("%s=%v", c, truncate(fmt.Sprint(m[c]), 60)))
			}
			fmt.Println(strings.Join(parts, "  "))
		}
		count++
	}
	if err := rows.Err(); err != nil {
		fatalf("读取结果: %v", err)
	}
	if count == 0 {
		fmt.Println("(0 行)")
	}
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "psq: "+format+"\n", args...)
	os.Exit(1)
}
