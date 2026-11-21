package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"msmkSync/internal/rowdiff"
)

func main() {
	left := flag.String("a", "", "左侧 rows.jsonl（通常是官方 JS SDK 的落库导出）")
	right := flag.String("b", "", "右侧 rows.jsonl（通常是 Go 实现的落库导出）")
	samples := flag.Int("samples", 5, "每张表最多打印多少条差异样例")
	flag.Parse()

	if *left == "" || *right == "" {
		fatalf("需要 -a 与 -b")
	}
	l, err := load(*left)
	if err != nil {
		fatalf("读取 %s: %v", *left, err)
	}
	r, err := load(*right)
	if err != nil {
		fatalf("读取 %s: %v", *right, err)
	}

	res := rowdiff.Compare(l, r, *samples)
	for _, s := range res.Stats {
		status := "OK"
		if s.OnlyLeft > 0 || s.OnlyRight > 0 || s.Different > 0 {
			status = "DIFF"
		}
		fmt.Printf("%-28s %s  左侧=%d 右侧=%d 一致=%d 仅左=%d 仅右=%d 字段不同=%d\n",
			s.Table, status, s.Left, s.Right, s.Same, s.OnlyLeft, s.OnlyRight, s.Different)
	}
	for _, d := range res.Diffs {
		fmt.Printf("    %s/%s: %s\n", d.Table, d.ID, d.Detail)
	}

	same, onlyLeft, onlyRight, different := res.Totals()
	fmt.Printf("\n合计: 一致=%d 仅左=%d 仅右=%d 字段不同=%d\n", same, onlyLeft, onlyRight, different)
	if !res.OK() {
		os.Exit(1)
	}
	fmt.Println("✅ Go 与官方 SDK 落库结果一致")
}

func load(path string) ([]rowdiff.Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []rowdiff.Row
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 32<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec rowdiff.Row
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("非法 JSON 行: %w", err)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "pscompare: "+format+"\n", args...)
	os.Exit(1)
}
