package rowdiff_test

import (
	"encoding/json"
	"testing"

	"msmkSync/internal/rowdiff"
)

func row(table, id, field, value string) rowdiff.Row {
	return rowdiff.Row{
		Table: table,
		Row: map[string]json.RawMessage{
			"id":  json.RawMessage(`"` + id + `"`),
			field: json.RawMessage(value),
		},
	}
}

// TestCanonEquivalences 覆盖归一化规则（都来自真机差异）。
func TestCanonEquivalences(t *testing.T) {
	cases := []struct{ name, a, b string }{
		{"时间戳空格 vs T", `"2026-08-13 18:40:38.49294"`, `"2026-08-13T18:40:38.49294"`},
		{"时间戳小数位补齐", `"2026-08-13T18:40:38.5"`, `"2026-08-13T18:40:38.500000"`},
		{"数值字符串 vs 数字", `"2.00"`, `2`},
		{"jsonb 文本 vs 对象（键序+空白）", `"{\"b\": 1, \"a\": 2}"`, `{"a":2,"b":1}`},
		{"双层字符串 vs 空串", `"\"\""`, `""`},
		{"bool vs 0/1", `true`, `1`},
	}
	for _, c := range cases {
		if got, want := rowdiff.Canon(json.RawMessage(c.a)), rowdiff.Canon(json.RawMessage(c.b)); got != want {
			t.Errorf("%s: %s != %s（%s vs %s）", c.name, got, want, c.a, c.b)
		}
	}

	// 确实不同的值不能被归一化掉。
	if rowdiff.Canon(json.RawMessage(`"1.0"`)) == rowdiff.Canon(json.RawMessage(`"1x"`)) {
		t.Error("不同值被误判为相同")
	}
	if rowdiff.Canon(json.RawMessage(`"abc"`)) == rowdiff.Canon(json.RawMessage(`"abd"`)) {
		t.Error("不同字符串被误判为相同")
	}
}

// TestCompare 覆盖一致/缺失/字段不同三种情况。
func TestCompare(t *testing.T) {
	left := []rowdiff.Row{
		row("todos", "a", "title", `"x"`),
		row("todos", "b", "title", `"y"`),
	}
	right := []rowdiff.Row{
		row("todos", "a", "title", `"x"`),
		row("todos", "b", "title", `"y"`),
	}
	res := rowdiff.Compare(left, right, 5)
	if !res.OK() {
		t.Fatalf("相同数据应判定一致: %+v", res)
	}
	if same, _, _, _ := res.Totals(); same != 2 {
		t.Errorf("一致数 = %d, want 2", same)
	}

	// 字段不同
	right[0] = row("todos", "a", "title", `"CHANGED"`)
	res = rowdiff.Compare(left, right, 5)
	if res.OK() {
		t.Fatal("字段不同应判定失败")
	}
	if _, _, _, diff := res.Totals(); diff != 1 {
		t.Errorf("字段不同数 = %d, want 1", diff)
	}

	// 少一行（右侧缺失）：用未改动的 a 行
	res = rowdiff.Compare(left, []rowdiff.Row{row("todos", "a", "title", `"x"`)}, 5)
	if _, onlyLeft, _, _ := res.Totals(); onlyLeft != 1 {
		t.Errorf("仅左侧 = %d, want 1（b 行只在左侧）", onlyLeft)
	}
	if res.OK() {
		t.Fatal("缺行应判定失败")
	}

	// 左侧 NULL 列、右侧没有该列 → 视为一致
	l := []rowdiff.Row{{
		Table: "todos",
		Row:   map[string]json.RawMessage{"id": json.RawMessage(`"a"`), "note": json.RawMessage(`null`)},
	}}
	r := []rowdiff.Row{{
		Table: "todos",
		Row:   map[string]json.RawMessage{"id": json.RawMessage(`"a"`)},
	}}
	if res := rowdiff.Compare(l, r, 5); !res.OK() {
		t.Errorf("NULL vs 缺列应视为一致: %+v", res.Diffs)
	}
}
