package core_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

const testSchema = `{
  "tables": [
    {"name": "todos",
     "columns": [
       {"name": "title", "type": "TEXT"},
       {"name": "done", "type": "INTEGER"},
       {"name": "weight", "type": "REAL"}
     ],
     "indexes": [{"name": "todos_title", "columns": [{"name": "title", "ascending": true}]}],
     "local_only": false, "insert_only": false},
    {"name": "notes",
     "columns": [{"name": "body", "type": "TEXT"}],
     "local_only": true, "insert_only": false}
  ]
}`

func openTestDB(t *testing.T, withSchema bool) *core.DB {
	t.Helper()
	dir := t.TempDir()
	var schema *core.Schema
	if withSchema {
		s, err := core.ParseSchema([]byte(testSchema))
		if err != nil {
			t.Fatalf("ParseSchema: %v", err)
		}
		schema = s
	}
	db, err := core.Open(context.Background(), core.Options{
		Path:     filepath.Join(dir, "test.db"),
		Schema:   schema,
		ClientID: "11111111-1111-4111-8111-111111111111",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func exec(t *testing.T, db *core.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.SQL().ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func crud(t *testing.T, db *core.DB) []core.CrudEntry {
	t.Helper()
	entries, err := db.CrudBatch(context.Background(), 100)
	if err != nil {
		t.Fatalf("CrudBatch: %v", err)
	}
	return entries
}

// TestSchemaCreatesTablesAndIndexes 验证 schema DDL 能落到 SQLite。
func TestSchemaCreatesTablesAndIndexes(t *testing.T) {
	db := openTestDB(t, true)

	var n int
	if err := db.SQL().QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='index' AND name='todos_title'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("todos_title 索引未创建")
	}

	cols, err := tableColumns(db, "todos")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "title", "done", "weight"}
	if len(cols) != len(want) {
		t.Fatalf("todos 列 = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("todos 列 = %v, want %v", cols, want)
		}
	}
}

// jsonString 把 JSON 归一化成键有序的紧凑字符串（json_patch 的输出键顺序不稳定）。
func jsonString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("非法 JSON %s: %v", raw, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func tableColumns(db *core.DB, table string) ([]string, error) {
	rows, err := db.SQL().Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// TestLocalWritesGoToCrudQueue 验证触发器：PUT 只带非空列、PATCH 只带变化列、DELETE 不带 data。
func TestLocalWritesGoToCrudQueue(t *testing.T) {
	db := openTestDB(t, true)

	exec(t, db, `INSERT INTO todos(id, title, done) VALUES('a', 'first', 1)`)
	exec(t, db, `UPDATE todos SET done = 0 WHERE id = 'a'`)
	exec(t, db, `DELETE FROM todos WHERE id = 'a'`)

	entries := crud(t, db)
	if len(entries) != 3 {
		t.Fatalf("ps_crud 条数 = %d, want 3: %+v", len(entries), entries)
	}

	put, patch, del := entries[0], entries[1], entries[2]
	if put.Op != "PUT" || put.Table != "todos" || put.RowID != "a" {
		t.Errorf("PUT = %+v", put)
	}
	if got, want := jsonString(t, put.Data), jsonString(t, json.RawMessage(`{"title":"first","done":1}`)); got != want {
		t.Errorf("PUT data = %s, want %s（应只含非空列、不含 id）", got, want)
	}
	if patch.Op != "PATCH" || jsonString(t, patch.Data) != jsonString(t, json.RawMessage(`{"done":0}`)) {
		t.Errorf("PATCH = %s %s", patch.Op, patch.Data)
	}
	if del.Op != "DELETE" || len(del.Data) != 0 {
		t.Errorf("DELETE = %s data=%s（应无 data）", del.Op, del.Data)
	}
	if put.ID >= patch.ID || patch.ID >= del.ID {
		t.Errorf("ps_crud.id 应递增: %d %d %d", put.ID, patch.ID, del.ID)
	}
}

// TestLocalOnlyTableIsNotQueued 验证 local_only 表不产生上传数据。
func TestLocalOnlyTableIsNotQueued(t *testing.T) {
	db := openTestDB(t, true)
	exec(t, db, `INSERT INTO notes(id, body) VALUES('n1', 'local only')`)
	if got := crud(t, db); len(got) != 0 {
		t.Fatalf("local_only 表不应入队，得到 %+v", got)
	}
}

// TestApplyOps 验证下行数据落表且不产生上传队列，并推进 bucket 游标。
func TestApplyOps(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()

	ops := []protocol.Op{
		{OpID: 10, Op: protocol.OpPut, ObjectType: "todos", ObjectID: "a",
			Data: json.RawMessage(`{"id":"a","title":"from server","done":1,"unknown":"x"}`)},
		{OpID: 11, Op: protocol.OpPut, ObjectType: "todos", ObjectID: "b",
			Data: json.RawMessage(`"{\"id\":\"b\",\"title\":\"raw string form\"}"`)},
	}
	if err := db.ApplyOps(ctx, "bucket-1", ops, 11); err != nil {
		t.Fatalf("ApplyOps: %v", err)
	}

	if got := crud(t, db); len(got) != 0 {
		t.Fatalf("下行数据不应进入 ps_crud，得到 %+v", got)
	}

	var title string
	var done sql.NullInt64
	if err := db.SQL().QueryRow(`SELECT title, done FROM todos WHERE id='b'`).Scan(&title, &done); err != nil {
		t.Fatalf("读取 b: %v", err)
	}
	if title != "raw string form" {
		t.Errorf("b.title = %q", title)
	}
	if done.Valid {
		t.Errorf("b.done 未在 data 中出现，应为 NULL，得到 %d", done.Int64)
	}
	// schema 未声明的字段应被忽略（不报错）。
	var cnt int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM pragma_table_info('todos') WHERE name='unknown'`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Error("未知字段不应被建列")
	}

	cur, err := db.BucketCursor(ctx, "bucket-1")
	if err != nil || cur != 11 {
		t.Fatalf("BucketCursor = %d, %v; want 11", cur, err)
	}

	// REMOVE 删除行，且游标继续推进。
	if err := db.ApplyOps(ctx, "bucket-1", []protocol.Op{
		{OpID: 12, Op: protocol.OpRemove, ObjectType: "todos", ObjectID: "a"},
	}, 12); err != nil {
		t.Fatalf("ApplyOps remove: %v", err)
	}
	if err := db.SQL().QueryRow(`SELECT count(*) FROM todos WHERE id='a'`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Error("REMOVE 后行仍存在")
	}
	if cur, _ := db.BucketCursor(ctx, "bucket-1"); cur != 12 {
		t.Errorf("游标 = %d, want 12", cur)
	}
}

// TestApplyGuardIsReleased 验证应用事务结束后触发器恢复工作。
func TestApplyGuardIsReleased(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	if err := db.ApplyOps(ctx, "b", []protocol.Op{
		{OpID: 1, Op: protocol.OpPut, ObjectType: "todos", ObjectID: "a", Data: json.RawMessage(`{"id":"a","title":"srv"}`)},
	}, 1); err != nil {
		t.Fatal(err)
	}
	exec(t, db, `UPDATE todos SET title='mine' WHERE id='a'`)
	got := crud(t, db)
	if len(got) != 1 || got[0].Op != "PATCH" {
		t.Fatalf("应用之后本地写应入队，得到 %+v", got)
	}
}

// TestCompleteCrud 验证出队与 write checkpoint 记录。
func TestCompleteCrud(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	exec(t, db, `INSERT INTO todos(id, title) VALUES('a','x')`)
	exec(t, db, `INSERT INTO todos(id, title) VALUES('b','y')`)

	entries := crud(t, db)
	if len(entries) != 2 {
		t.Fatalf("entries = %d", len(entries))
	}
	if err := db.CompleteCrud(ctx, entries[0].ID, 0); err != nil {
		t.Fatal(err)
	}
	if got := crud(t, db); len(got) != 1 || got[0].RowID != "b" {
		t.Fatalf("出队后剩余 = %+v", got)
	}
	if err := db.CompleteCrud(ctx, entries[1].ID, 42); err != nil {
		t.Fatal(err)
	}
	// core v14 把 legacy target_op 迁到了 ps_kv 的 target_checkpoint_request_id。
	target, err := db.Meta(ctx, "target_checkpoint_request_id")
	if err != nil {
		t.Fatalf("读取 target_checkpoint_request_id: %v", err)
	}
	if target != "42" {
		t.Errorf("target_checkpoint_request_id = %q, want 42", target)
	}
}

// TestNoSchemaAutoCreatesColumns 验证没有 schema 时按行字段建列（工具/测试用）。
func TestNoSchemaAutoCreatesColumns(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.ApplyOps(context.Background(), "b", []protocol.Op{
		{OpID: 1, Op: protocol.OpPut, ObjectType: "adhoc", ObjectID: "1", Data: json.RawMessage(`{"id":"1","foo":"bar"}`)},
	}, 1); err != nil {
		t.Fatalf("ApplyOps: %v", err)
	}
	cols, err := tableColumns(db, "adhoc")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "foo" {
		t.Fatalf("adhoc 列 = %v", cols)
	}
}

// TestSchemaValidate 覆盖坏 schema。
func TestSchemaValidate(t *testing.T) {
	cases := []struct{ name, json string }{
		{"重复表名", `{"tables":[{"name":"a","columns":[]},{"name":"a","columns":[]}]}`},
		{"未知类型", `{"tables":[{"name":"a","columns":[{"name":"x","type":"FOO"}]}]}`},
		{"空列名", `{"tables":[{"name":"a","columns":[{"name":"","type":"TEXT"}]}]}`},
	}
	for _, c := range cases {
		if _, err := core.ParseSchema([]byte(c.json)); err == nil {
			t.Errorf("%s: 期望报错", c.name)
		}
	}

	// 缺 id 列时自动补一个 TEXT 主键。
	s, err := core.ParseSchema([]byte(`{"tables":[{"name":"a","columns":[{"name":"x","type":"TEXT"}]}]}`))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if cols := s.Tables[0].ColumnNames(); len(cols) != 2 || cols[0] != "id" {
		t.Fatalf("自动补 id 失败: %v", cols)
	}
}

// TestJSONTableLayout 验证默认（JSON 表）的落盘结构，与官方 core 对齐：
//
//	ps_data__todos(id, data) 表 + todos 视图 + INSTEAD OF 触发器。
func TestJSONTableLayout(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()

	mustQueryInt := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := db.SQL().QueryRow(q, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}

	// 落盘表 + 视图
	if n := mustQueryInt(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ps_data__todos'`); n != 1 {
		t.Fatalf("ps_data__todos 表不存在")
	}
	if n := mustQueryInt(`SELECT count(*) FROM sqlite_master WHERE type='view' AND name='todos'`); n != 1 {
		t.Fatalf("todos 视图不存在")
	}
	var viewSQL string
	if err := db.SQL().QueryRow(`SELECT sql FROM sqlite_master WHERE type='view' AND name='todos'`).Scan(&viewSQL); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(viewSQL, "powersync-auto-generated") {
		t.Errorf("视图缺少 core 的标记注释: %s", viewSQL)
	}
	if !strings.Contains(viewSQL, "json_extract(data, '$.title')") {
		t.Errorf("视图应通过 json_extract 暴露列: %s", viewSQL)
	}
	// 触发器是 INSTEAD OF（作用在视图上）
	if n := mustQueryInt(`SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='ps_crud_insert_todos' AND sql LIKE '%INSTEAD OF INSERT%'`); n != 1 {
		t.Fatalf("缺少 INSTEAD OF INSERT 触发器")
	}

	// 通过视图写入 → 落进 ps_data__todos（JSON），并进 ps_crud
	exec(t, db, `INSERT INTO todos(id, title, done) VALUES('a','first',1)`)
	var stored string
	if err := db.SQL().QueryRow(`SELECT data FROM ps_data__todos WHERE id='a'`).Scan(&stored); err != nil {
		t.Fatalf("读取 JSON 落盘: %v", err)
	}
	// 与 core 一致：落盘 data 是 json_object(<所有列>)，含 null 列、不含 id（id 单独存列）。
	if got := jsonString(t, json.RawMessage(stored)); got != jsonString(t, json.RawMessage(`{"title":"first","done":1,"weight":null}`)) {
		t.Errorf("落盘 JSON = %s", got)
	}
	entries := crud(t, db)
	if len(entries) != 1 || entries[0].Op != "PUT" || entries[0].RowID != "a" {
		t.Fatalf("ps_crud = %+v", entries)
	}

	// 视图读回
	var title string
	if err := db.SQL().QueryRow(`SELECT title FROM todos WHERE id='a'`).Scan(&title); err != nil || title != "first" {
		t.Fatalf("视图读取 = %q, %v", title, err)
	}

	// UPDATE → PATCH；DELETE → DELETE，且 JSON 行被删
	exec(t, db, `UPDATE todos SET done=0 WHERE id='a'`)
	exec(t, db, `DELETE FROM todos WHERE id='a'`)
	entries = crud(t, db)
	if len(entries) != 3 || entries[1].Op != "PATCH" || entries[2].Op != "DELETE" {
		t.Fatalf("ps_crud 序列 = %+v", entries)
	}
	if n := mustQueryInt(`SELECT count(*) FROM ps_data__todos`); n != 0 {
		t.Errorf("删除后 ps_data__todos 仍有 %d 行", n)
	}
	_ = ctx
}

// TestDirectTableOptIn 验证 direct:true 走真实列式表（无视图）。
func TestDirectTableOptIn(t *testing.T) {
	schemaJSON := `{"tables":[{"name":"metrics","direct":true,
	  "columns":[{"name":"name","type":"TEXT"},{"name":"value","type":"INTEGER"}]}]}`
	s, err := core.ParseSchema([]byte(schemaJSON))
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{
		Path:   filepath.Join(t.TempDir(), "direct.db"),
		Schema: s,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='view' AND name='metrics'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("direct 表不应有视图")
	}
	if err := db.SQL().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='metrics'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("direct 表应存在")
	}

	exec(t, db, `INSERT INTO metrics(id, name, value) VALUES('m1','cpu',42)`)
	entries := crud(t, db)
	if len(entries) != 1 || entries[0].Op != "PUT" {
		t.Fatalf("direct 表写应入队: %+v", entries)
	}
	if got := jsonString(t, entries[0].Data); got != `{"name":"cpu","value":42}` {
		t.Errorf("PUT data = %s", got)
	}

	// ApplyOps 也走列式表
	if err := db.ApplyOps(context.Background(), "b", []protocol.Op{{
		OpID: 1, Op: protocol.OpPut, ObjectType: "metrics", ObjectID: "m2",
		Data: json.RawMessage(`{"id":"m2","name":"mem","value":7}`),
	}}, 1); err != nil {
		t.Fatalf("ApplyOps: %v", err)
	}
	var value int
	if err := db.SQL().QueryRow(`SELECT value FROM metrics WHERE id='m2'`).Scan(&value); err != nil || value != 7 {
		t.Fatalf("direct 表落库 = %d, %v", value, err)
	}
}

// TestLocalOnlyJSONTable 验证 local_only 表用 ps_data_local__ 前缀且不入上传队列。
func TestLocalOnlyJSONTable(t *testing.T) {
	db := openTestDB(t, true) // notes 是 local_only
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ps_data_local__notes'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("local_only 表应落在 ps_data_local__notes")
	}
	exec(t, db, `INSERT INTO notes(id, body) VALUES('n1','local')`)
	if got := crud(t, db); len(got) != 0 {
		t.Fatalf("local_only 表不应入队: %+v", got)
	}
	var body string
	if err := db.SQL().QueryRow(`SELECT body FROM notes WHERE id='n1'`).Scan(&body); err != nil || body != "local" {
		t.Fatalf("读取 local_only = %q, %v", body, err)
	}
}

// TestApplyOpsToJSONStorage 验证下行 op 落到 ps_data__*，多余列被丢弃，视图可读。
func TestApplyOpsToJSONStorage(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	if err := db.ApplyOps(ctx, "b", []protocol.Op{{
		OpID: 5, Op: protocol.OpPut, ObjectType: "todos", ObjectID: "srv",
		Data: json.RawMessage(`{"id":"srv","title":"from server","done":1,"extra":"nope"}`),
	}}, 5); err != nil {
		t.Fatalf("ApplyOps: %v", err)
	}
	var stored string
	if err := db.SQL().QueryRow(`SELECT data FROM ps_data__todos WHERE id='srv'`).Scan(&stored); err != nil {
		t.Fatalf("读取落盘: %v", err)
	}
	if strings.Contains(stored, "extra") {
		t.Errorf("未声明的列不应落盘: %s", stored)
	}
	var title string
	if err := db.SQL().QueryRow(`SELECT title FROM todos WHERE id='srv'`).Scan(&title); err != nil || title != "from server" {
		t.Fatalf("视图读取 = %q, %v", title, err)
	}
	if got := crud(t, db); len(got) != 0 {
		t.Fatalf("下行数据不应入队: %+v", got)
	}
}

// TestIncludeMetadata 验证 include_metadata：
//   - 视图多出 _metadata / _deleted 两列（与 core 一样是 NULL）；
//   - 写操作把 NEW._metadata 带进 ps_crud 的 metadata 字段；
//   - "UPDATE ... SET _deleted = TRUE" 会变成带 metadata 的 DELETE。
func TestIncludeMetadata(t *testing.T) {
	schemaJSON := `{"tables":[{"name":"todos","include_metadata":true,
	  "columns":[{"name":"title","type":"TEXT"}]}]}`
	s, err := core.ParseSchema([]byte(schemaJSON))
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{
		Path:   filepath.Join(t.TempDir(), "meta.db"),
		Schema: s,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	var n int
	if err := db.SQL().QueryRow(
		`SELECT count(*) FROM pragma_table_info('todos') WHERE name IN ('_metadata','_deleted')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("视图应有 _metadata/_deleted 两列，实际 %d", n)
	}

	exec(t, db, `INSERT INTO todos(id, title, _metadata) VALUES('a','x','hint-1')`)
	exec(t, db, `UPDATE todos SET title='y', _metadata='hint-2' WHERE id='a'`)
	exec(t, db, `UPDATE todos SET _deleted = TRUE, _metadata='hint-3' WHERE id='a'`)

	entries, err := db.CrudBatch(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("ps_crud 条数 = %d, want 3: %+v", len(entries), entries)
	}
	want := []struct {
		op, meta string
	}{
		{"PUT", "hint-1"},
		{"PATCH", "hint-2"},
		{"DELETE", "hint-3"},
	}
	for i, w := range want {
		if entries[i].Op != w.op {
			t.Errorf("entries[%d].Op = %s, want %s", i, entries[i].Op, w.op)
		}
		if entries[i].Metadata != w.meta {
			t.Errorf("entries[%d].Metadata = %q, want %q", i, entries[i].Metadata, w.meta)
		}
	}

	// 删除生效，且落盘数据里没有 _metadata（与 core 一致：只在 ps_crud 里携带）
	var cnt int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM ps_data__todos`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Errorf("_deleted=TRUE 后行应被删除，实际还剩 %d", cnt)
	}
}

// TestWithoutMetadataHasNoExtraColumns 验证默认表不会多出 metadata 列。
func TestWithoutMetadataHasNoExtraColumns(t *testing.T) {
	db := openTestDB(t, true)
	var n int
	if err := db.SQL().QueryRow(
		`SELECT count(*) FROM pragma_table_info('todos') WHERE name LIKE '\_%' ESCAPE '\'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("默认表不应有 _metadata/_deleted 列，实际 %d", n)
	}
}

// TestWriteTransactionResetsTxIDOnPanic 验证事务 panic/回滚后不会留下过期 tx_id
// （否则后续普通本地写会被错误归到上一次事务里）。
func TestWriteTransactionResetsTxIDOnPanic(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()

	func() {
		defer func() { _ = recover() }()
		_ = db.WriteTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO todos(id,title) VALUES('boom','x')`); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	var current sql.NullInt64
	if err := db.SQL().QueryRow(`SELECT current_tx FROM ps_tx WHERE id = 1`).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current.Valid {
		t.Fatalf("panic 后 current_tx 应为 NULL，得到 %d", current.Int64)
	}

	// 普通写不应带 tx_id
	exec(t, db, `INSERT INTO todos(id,title) VALUES('plain','y')`)
	entries := crud(t, db)
	if len(entries) != 1 {
		t.Fatalf("ps_crud = %+v", entries)
	}
	if entries[0].TxID != nil {
		t.Errorf("事务外写不应有 tx_id，得到 %d", *entries[0].TxID)
	}
}

// TestClearAll 验证 DisconnectAndClear 底层：默认保留 local_only 表，可选一起清。
func TestClearAll(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	exec(t, db, `INSERT INTO todos(id, title) VALUES('a','x')`)
	exec(t, db, `INSERT INTO notes(id, body) VALUES('n1','local only')`) // notes 是 local_only

	if err := db.ClearAll(ctx, false); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM todos`).Scan(&n); err != nil || n != 0 {
		t.Errorf("todos 应被清空，得到 %d (%v)", n, err)
	}
	if err := db.SQL().QueryRow(`SELECT count(*) FROM notes`).Scan(&n); err != nil || n != 1 {
		t.Errorf("local_only 表默认应保留，得到 %d (%v)", n, err)
	}
	if err := db.SQL().QueryRow(`SELECT count(*) FROM ps_crud`).Scan(&n); err != nil || n != 0 {
		t.Errorf("ps_crud 应被清空，得到 %d (%v)", n, err)
	}

	// 再清一次，这次连 local 表也清
	if err := db.ClearAll(ctx, true); err != nil {
		t.Fatalf("ClearAll(local): %v", err)
	}
	if err := db.SQL().QueryRow(`SELECT count(*) FROM notes`).Scan(&n); err != nil || n != 0 {
		t.Errorf("WithClearLocalTables 后 notes 应为空，得到 %d (%v)", n, err)
	}
}

// TestSkippedUnknownCounts 验证"表不在 schema"时跳过物化并计数（不报错）。
// 注意：计数的是**被跳过物化的行**（PUT），REMOVE 本来就不会产生行，不计入。
func TestSkippedUnknownCounts(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	before := db.SkippedUnknown()
	if err := db.ApplyOps(ctx, "b", []protocol.Op{
		{OpID: 1, Op: protocol.OpPut, ObjectType: "ghost", ObjectID: "x",
			Data: json.RawMessage(`{"id":"x","a":1}`)},
		{OpID: 2, Op: protocol.OpPut, ObjectType: "ghost2", ObjectID: "y",
			Data: json.RawMessage(`{"id":"y"}`)},
	}, 2); err != nil {
		t.Fatalf("ApplyOps: %v", err)
	}
	if got := db.SkippedUnknown() - before; got != 2 {
		t.Errorf("SkippedUnknown 增量 = %d, want 2", got)
	}
	// oplog 仍然记录（checksum 不受影响）
	if rows, _, err := db.OplogStats(ctx, "b"); err != nil || rows == 0 {
		t.Errorf("oplog 应仍记录，rows=%d err=%v", rows, err)
	}
}
