package core

import (
	"fmt"
	"strings"
	"testing"
)

// SQLite 单个函数最多 127 个参数（=63 对键值），列多的表必须分段 json_object 再合并。
// 这里覆盖回归：70 列的表不能生成一条超长 json_object。
func TestWideTableJSONObjectChunking(t *testing.T) {
	tbl := Table{Name: "wide"}
	tbl.Columns = append(tbl.Columns, Column{Name: "id", Type: "TEXT"})
	for i := 0; i < 70; i++ {
		tbl.Columns = append(tbl.Columns, Column{Name: fmt.Sprintf("c%d", i), Type: "TEXT"})
	}
	expr := tbl.jsonObjectExpr("NEW")

	chunks := strings.Count(expr, "json_object(")
	if chunks < 2 {
		t.Fatalf("70 列应至少分成 2 段 json_object，实际 %d 段: %s", chunks, expr)
	}
	for _, part := range splitTopLevel(expr) {
		// 每段 json_object(...) 的参数个数 = 逗号数 + 1（这里只按顶层逗号粗算，够用）
		if n := strings.Count(part, ",") + 1; n > 126 {
			t.Fatalf("单次函数调用参数过多（%d）: %s", n, part)
		}
	}
	if !strings.Contains(expr, "powersync_json_merge(") {
		t.Fatalf("多段应使用 powersync_json_merge 合并: %s", expr)
	}
}

// splitTopLevel 粗切分合并调用的各段（仅测试用）。
func splitTopLevel(expr string) []string {
	inner := expr
	if i := strings.Index(inner, "powersync_json_merge("); i >= 0 {
		inner = inner[i+len("powersync_json_merge("):]
		inner = strings.TrimSuffix(inner, ")")
	}
	return strings.Split(inner, "), json_object(")
}
