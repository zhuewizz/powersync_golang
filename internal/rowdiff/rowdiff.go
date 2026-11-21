// Package rowdiff 比对两份"落库导出"（rows.jsonl），用于 Go 与官方 SDK 的双实现差分测试。
//
// 归一化规则（都来自真机踩坑，见 AGENTS.md §11.2）：
//   - 时间戳：`2026-08-13 18:40:38.49` 与 `2026-08-13T18:40:38.49` 视为相同；
//   - 数值被序列化成字符串：`"2.00"` 与 `2` 视为相同；
//   - jsonb 列以文本下发：`"{\"a\": 1}"` 与 `{"a":1}` 视为相同；
//   - PS 会把某些文本再包一层字符串：`"\"\""` 与 `""` 视为相同；
//   - NULL 列 vs 对方没有该列：视为相同。
package rowdiff

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Row 是一行落库数据。
type Row struct {
	Table string                     `json:"table"`
	Row   map[string]json.RawMessage `json:"row"`
}

// Diff 是一条差异。
type Diff struct {
	Table  string
	ID     string
	Detail string
}

// Stats 是单表的比对统计。
type Stats struct {
	Table     string
	Left      int
	Right     int
	Same      int
	OnlyLeft  int
	OnlyRight int
	Different int
}

// Result 是一次完整比对的结果。
type Result struct {
	Stats []Stats
	Diffs []Diff
}

// OK 报告是否完全一致。
func (r Result) OK() bool {
	for _, s := range r.Stats {
		if s.OnlyLeft > 0 || s.OnlyRight > 0 || s.Different > 0 {
			return false
		}
	}
	return true
}

// Totals 返回汇总计数。
func (r Result) Totals() (same, onlyLeft, onlyRight, different int) {
	for _, s := range r.Stats {
		same += s.Same
		onlyLeft += s.OnlyLeft
		onlyRight += s.OnlyRight
		different += s.Different
	}
	return
}

// Compare 比对两组行；diffs 最多每表保留 maxSamplesPerTable 条。
func Compare(left, right []Row, maxSamplesPerTable int) Result {
	lIdx := index(left)
	rIdx := index(right)

	tables := map[string]struct{}{}
	for t := range lIdx {
		tables[t] = struct{}{}
	}
	for t := range rIdx {
		tables[t] = struct{}{}
	}
	names := make([]string, 0, len(tables))
	for t := range tables {
		names = append(names, t)
	}
	sort.Strings(names)

	var res Result
	for _, table := range names {
		l, r := lIdx[table], rIdx[table]
		st := Stats{Table: table, Left: len(l), Right: len(r)}
		samples := 0

		ids := make([]string, 0, len(l))
		for id := range l {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			lrow := l[id]
			rrow, ok := r[id]
			if !ok {
				st.OnlyLeft++
				if samples < maxSamplesPerTable {
					res.Diffs = append(res.Diffs, Diff{table, id, "只在左侧（Go/参照端）存在"})
					samples++
				}
				continue
			}
			if d := fieldDiffs(lrow, rrow); len(d) > 0 {
				st.Different++
				if samples < maxSamplesPerTable {
					res.Diffs = append(res.Diffs, Diff{table, id, strings.Join(d, "; ")})
					samples++
				}
				continue
			}
			st.Same++
		}

		rIDs := make([]string, 0, len(r))
		for id := range r {
			rIDs = append(rIDs, id)
		}
		sort.Strings(rIDs)
		for _, id := range rIDs {
			if _, ok := l[id]; !ok {
				st.OnlyRight++
				if samples < maxSamplesPerTable {
					res.Diffs = append(res.Diffs, Diff{table, id, "只在右侧存在"})
					samples++
				}
			}
		}
		res.Stats = append(res.Stats, st)
	}
	return res
}

func index(rows []Row) map[string]map[string]map[string]json.RawMessage {
	out := map[string]map[string]map[string]json.RawMessage{}
	for _, r := range rows {
		id := string(rawString(r.Row["id"]))
		if r.Table == "" || id == "" {
			continue
		}
		if out[r.Table] == nil {
			out[r.Table] = map[string]map[string]json.RawMessage{}
		}
		out[r.Table][id] = r.Row
	}
	return out
}

func fieldDiffs(left, right map[string]json.RawMessage) []string {
	var diff []string
	keys := make([]string, 0, len(left))
	for k := range left {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rv, exists := right[k]
		if !exists {
			if Canon(left[k]) == "<nil>" {
				continue // NULL vs 缺列：视为一致
			}
			diff = append(diff, fmt.Sprintf("%s: 左=%s 右=<缺失>", k, Canon(left[k])))
		} else if lv, rvv := Canon(left[k]), Canon(rv); lv != rvv {
			diff = append(diff, fmt.Sprintf("%s: 左=%s 右=%s", k, lv, rvv))
		}
		if len(diff) >= 3 {
			return diff
		}
	}
	return diff
}

// Canon 把 JSON 值归一化成可比较字符串。
func Canon(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "<nil>"
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "!" + string(raw)
	}
	return canonValue(v)
}

func canonValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "<nil>"
	case bool:
		if t {
			return "n:1"
		}
		return "n:0"
	case json.Number:
		return "n:" + trimNumber(t.String())
	case string:
		if ts, ok := parseTimestamp(t); ok {
			return "t:" + ts
		}
		if n, ok := parseNumberString(t); ok {
			return "n:" + n
		}
		if obj, ok := parseJSONString(t); ok {
			return "j:" + obj
		}
		if len(t) >= 2 && t[0] == '"' && t[len(t)-1] == '"' {
			var inner string
			if err := json.Unmarshal([]byte(t), &inner); err == nil {
				return "s:" + inner
			}
		}
		return "s:" + t
	case map[string]any:
		return "j:" + canonicalJSON(t)
	case []any:
		return "j:" + canonicalJSON(t)
	default:
		b, _ := json.Marshal(v)
		return "o:" + string(b)
	}
}

var timestampLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
}

func parseTimestamp(s string) (string, bool) {
	if len(s) < 19 {
		return "", false
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(time.RFC3339Nano), true
		}
	}
	return "", false
}

func parseNumberString(s string) (string, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "", false
	}
	if _, err := strconv.ParseFloat(t, 64); err != nil {
		return "", false
	}
	return trimNumber(t), true
}

func parseJSONString(s string) (string, bool) {
	t := strings.TrimSpace(s)
	if len(t) < 2 || (t[0] != '{' && t[0] != '[') || !json.Valid([]byte(t)) {
		return "", false
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(t))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", false
	}
	return canonicalJSON(v), true
}

func canonicalJSON(v any) string {
	b, err := json.Marshal(normalizeJSON(v))
	if err != nil {
		return fmt.Sprintf("!%v", v)
	}
	return string(b)
}

func normalizeJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	case json.Number:
		return json.Number(trimNumber(t.String()))
	default:
		return v
	}
}

func trimNumber(s string) string {
	if strings.ContainsAny(s, "eE") {
		return s
	}
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		return "0"
	}
	return s
}

func rawString(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []byte(s)
	}
	return raw
}
