package core_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhuweizz/powersync_golang/internal/core"
	"github.com/zhuweizz/powersync_golang/internal/protocol"
)

// 用真实 schema 复现：服务端回显（整行）后本地 UPDATE，行是否还在。
func TestReproRealSchemaEchoThenUpdate(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "out", "schema.json"))
	if err != nil {
		t.Skip("缺少 out/schema.json")
	}
	s, err := core.ParseSchema(raw)
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{Path: filepath.Join(t.TempDir(), "r.db"), Schema: s})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	count := func() int {
		var n int
		db.SQL().QueryRow(`SELECT count(*) FROM sys_error_report`).Scan(&n)
		return n
	}

	row := `{"id":"r1","org_code":"ORG001","box_code":"D1-2","box_cell":"X","user_id":"U","user_name":"N",
	  "error_date":"2026-01-01 00:00:00","error_desc":"x","trans_type":"T","op_status":"O","box_type":"B",
	  "created_at":"2026-01-01 00:00:00","updated_at":null,"extra_col":"v"}`
	if err := db.ApplyOps(ctx, "b", []protocol.Op{{
		OpID: 1, Op: protocol.OpPut, ObjectType: "sys_error_report", ObjectID: "r1",
		Subkey: "uuid/uuid", Checksum: 10, Data: []byte(row),
	}}, 1); err != nil {
		t.Fatal(err)
	}
	t.Logf("after echo: %d", count())
	if _, err := db.SQL().Exec(`UPDATE sys_error_report SET error_desc='updated' WHERE id='r1'`); err != nil {
		t.Fatal(err)
	}
	t.Logf("after update: %d", count())
	var data string
	_ = db.SQL().QueryRow(`SELECT data FROM ps_data__sys_error_report WHERE id='r1'`).Scan(&data)
	t.Logf("stored data: %s", data)
	if got := count(); got != 1 {
		t.Fatalf("行消失：%d", got)
	}
}
