package core_test

import (
	"encoding/json"
	"testing"

	"msmkSync/internal/core"
)

// TestSQLFunctions 验证注册的 powersync_* 函数（子集）。
func TestSQLFunctions(t *testing.T) {
	db := openTestDB(t, true)

	var clientID, version string
	if err := db.SQL().QueryRow(`SELECT powersync_client_id(), powersync_version()`).Scan(&clientID, &version); err != nil {
		t.Fatalf("查询函数: %v", err)
	}
	if clientID != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("powersync_client_id() = %q", clientID)
	}
	if version != core.Version {
		t.Errorf("powersync_version() = %q, want %q", version, core.Version)
	}
	var rsVersion string
	if err := db.SQL().QueryRow(`SELECT powersync_rs_version()`).Scan(&rsVersion); err != nil || rsVersion != core.Version {
		t.Errorf("powersync_rs_version() = %q, %v", rsVersion, err)
	}

	cases := []struct{ old, new, want string }{
		// INSERT 场景：old 为空 → 返回非空列
		{`{}`, `{"a":1,"b":null,"c":"x"}`, `{"a":1,"c":"x"}`},
		// 只有变化的列
		{`{"a":1,"b":2}`, `{"a":1,"b":3}`, `{"b":3}`},
		// 字段被清空 → 补 null
		{`{"a":1,"b":2}`, `{"a":1}`, `{"b":null}`},
		// 新增字段
		{`{"a":1}`, `{"a":1,"b":2}`, `{"b":2}`},
		// 无变化
		{`{"a":1}`, `{"a":1}`, `{}`},
		// 嵌套对象按值比较
		{`{"o":{"x":1,"y":2}}`, `{"o":{"y":2,"x":1}}`, `{}`},
	}
	for _, c := range cases {
		var got string
		if err := db.SQL().QueryRow(`SELECT powersync_diff(?, ?)`, c.old, c.new).Scan(&got); err != nil {
			t.Fatalf("powersync_diff(%s,%s): %v", c.old, c.new, err)
		}
		if canonJSON(t, got) != canonJSON(t, c.want) {
			t.Errorf("powersync_diff(%s, %s) = %s, want %s", c.old, c.new, got, c.want)
		}
	}

	// json_merge：把分段 json_object 合并回一个对象
	var merged string
	if err := db.SQL().QueryRow(`SELECT powersync_json_merge('{"a":1}', '{"b":2}', NULL)`).Scan(&merged); err != nil {
		t.Fatalf("powersync_json_merge: %v", err)
	}
	if canonJSON(t, merged) != canonJSON(t, `{"a":1,"b":2}`) {
		t.Errorf("powersync_json_merge = %s", merged)
	}
}

// TestClearedColumnProducesNullPatch 验证"把列置空"会产生 {"col":null} 的 PATCH（core 语义）。
func TestClearedColumnProducesNullPatch(t *testing.T) {
	db := openTestDB(t, true)
	exec(t, db, `INSERT INTO todos(id, title, done) VALUES('a','x',1)`)
	exec(t, db, `UPDATE todos SET done = NULL WHERE id='a'`)

	entries := crud(t, db)
	if len(entries) != 2 {
		t.Fatalf("ps_crud = %+v", entries)
	}
	patch := entries[1]
	if patch.Op != "PATCH" {
		t.Fatalf("第二条应为 PATCH: %+v", patch)
	}
	if got := jsonString(t, patch.Data); got != `{"done":null}` {
		t.Errorf("清空列的 PATCH data = %s, want {\"done\":null}", got)
	}

	// 落盘 JSON 应包含全列（含 null），视图读出 null
	var stored string
	if err := db.SQL().QueryRow(`SELECT data FROM ps_data__todos WHERE id='a'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(stored), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["done"]; !ok {
		t.Errorf("落盘 JSON 应包含 done 键（即使是 null）: %s", stored)
	}
	var done any
	if err := db.SQL().QueryRow(`SELECT done FROM todos WHERE id='a'`).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if done != nil {
		t.Errorf("视图 done 应为 NULL，得到 %v", done)
	}
}

func canonJSON(t *testing.T, s string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("非法 JSON %s: %v", s, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
