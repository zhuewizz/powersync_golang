package core_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhuweizz/powersync_golang/internal/core"
)

func openSchema(t *testing.T, path, schemaJSON string) *core.DB {
	t.Helper()
	s, err := core.ParseSchema([]byte(schemaJSON))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	db, err := core.Open(context.Background(), core.Options{Path: path, Schema: s})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func tableData(t *testing.T, db *core.DB, table string) []map[string]any {
	t.Helper()
	rows, err := db.SQL().Query(`SELECT * FROM ` + quoteForTest(table) + ` ORDER BY id`)
	if err != nil {
		t.Fatalf("查询 %s: %v", table, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		m := map[string]any{}
		for i, c := range cols {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	return out
}

func quoteForTest(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

const schemaV1 = `{"tables":[{"name":"todos","columns":[
  {"name":"title","type":"TEXT"},{"name":"done","type":"INTEGER"}]}]}`

// TestReplaceSchemaAddColumn 验证 JSON 表加列：数据保留、新列为 NULL、视图更新。
func TestReplaceSchemaAddColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate.db")
	db := openSchema(t, path, schemaV1)

	exec(t, db, `INSERT INTO todos(id, title, done) VALUES('a','first',1)`)
	before := crud(t, db)
	if len(before) != 1 {
		t.Fatalf("ps_crud = %d", len(before))
	}

	next, err := core.ParseSchema([]byte(`{"tables":[{"name":"todos","columns":[
	  {"name":"title","type":"TEXT"},{"name":"done","type":"INTEGER"},{"name":"note","type":"TEXT"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceSchema(context.Background(), next); err != nil {
		t.Fatalf("ReplaceSchema: %v", err)
	}

	rows := tableData(t, db, "todos")
	if len(rows) != 1 || rows[0]["title"] != "first" {
		t.Fatalf("迁移后数据丢失: %+v", rows)
	}
	if v, ok := rows[0]["note"]; !ok || v != nil {
		t.Errorf("新列 note 应存在且为 NULL，得到 %+v", rows[0])
	}

	// ps_crud 不受影响（不应清空上传队列）
	if got := crud(t, db); len(got) != 1 {
		t.Errorf("迁移不应影响 ps_crud，得到 %d 条", len(got))
	}
	// ps_untyped 应清空
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM ps_untyped`).Scan(&n); err != nil || n != 0 {
		t.Errorf("ps_untyped 应为空，得到 %d (%v)", n, err)
	}
}

// TestReplaceSchemaJSONToDirect 验证 JSON 表切到 direct 表时数据保留。
func TestReplaceSchemaJSONToDirect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate2.db")
	db := openSchema(t, path, schemaV1)
	exec(t, db, `INSERT INTO todos(id, title, done) VALUES('a','first',1)`)

	next, err := core.ParseSchema([]byte(`{"tables":[{"name":"todos","direct":true,"columns":[
	  {"name":"title","type":"TEXT"},{"name":"done","type":"INTEGER"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceSchema(context.Background(), next); err != nil {
		t.Fatalf("ReplaceSchema: %v", err)
	}

	// direct 表：有真实表、无视图
	var views int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='view' AND name='todos'`).Scan(&views); err != nil {
		t.Fatal(err)
	}
	if views != 0 {
		t.Error("direct 表不应有视图")
	}
	rows := tableData(t, db, "todos")
	if len(rows) != 1 || rows[0]["title"] != "first" {
		t.Fatalf("JSON→direct 迁移丢数据: %+v", rows)
	}
}

// TestReplaceSchemaDirectToJSON 验证 direct 表切回 JSON 表时数据保留。
func TestReplaceSchemaDirectToJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate3.db")
	db := openSchema(t, path, `{"tables":[{"name":"todos","direct":true,"columns":[
	  {"name":"title","type":"TEXT"}]}]}`)
	exec(t, db, `INSERT INTO todos(id, title) VALUES('a','first')`)

	next, err := core.ParseSchema([]byte(schemaV1))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceSchema(context.Background(), next); err != nil {
		t.Fatalf("ReplaceSchema: %v", err)
	}
	rows := tableData(t, db, "todos")
	if len(rows) != 1 || rows[0]["title"] != "first" {
		t.Fatalf("direct→JSON 迁移丢数据: %+v", rows)
	}
	if v := rows[0]["done"]; v != nil {
		t.Errorf("新列 done 应为 NULL，得到 %v", v)
	}
}

// TestReplaceSchemaDropTable 验证从 schema 移除的表会连同视图/落盘表一起删掉。
func TestReplaceSchemaDropTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate4.db")
	db := openSchema(t, path, `{"tables":[
	  {"name":"todos","columns":[{"name":"title","type":"TEXT"}]},
	  {"name":"scratch","columns":[{"name":"v","type":"TEXT"}]}]}`)
	exec(t, db, `INSERT INTO scratch(id, v) VALUES('s1','tmp')`)

	next, err := core.ParseSchema([]byte(schemaV1))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceSchema(context.Background(), next); err != nil {
		t.Fatalf("ReplaceSchema: %v", err)
	}
	var n int
	if err := db.SQL().QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE name IN ('scratch','ps_data__scratch','ps_crud_insert_scratch')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("被移除的表仍残留 %d 个对象", n)
	}
}

// TestOpenMigratesChangedSchema 验证重新 Open 时 schema 变化会自动迁移。
func TestOpenMigratesChangedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate5.db")
	db := openSchema(t, path, schemaV1)
	exec(t, db, `INSERT INTO todos(id, title) VALUES('a','first')`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二次用新 schema（多一列）打开 → 自动迁移
	db2 := openSchema(t, path, `{"tables":[{"name":"todos","columns":[
	  {"name":"title","type":"TEXT"},{"name":"done","type":"INTEGER"},{"name":"extra","type":"TEXT"}]}]}`)
	rows := tableData(t, db2, "todos")
	if len(rows) != 1 || rows[0]["title"] != "first" {
		t.Fatalf("Open 自动迁移后数据丢失: %+v", rows)
	}
	if _, ok := rows[0]["extra"]; !ok {
		t.Errorf("新列 extra 未出现: %+v", rows[0])
	}
}
