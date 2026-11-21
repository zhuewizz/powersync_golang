package core_test

import (
	"context"
	"testing"
)

// TestPowersyncCrudCompat 验证与官方 core 的 powersync_crud 兼容层：
// 应用/触发器可以直接往 powersync_crud 写一行，同一事务内被搬进 ps_crud。
func TestPowersyncCrudCompat(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	_ = ctx

	// 表结构（列与 core 的 crud 虚拟表一致）
	cols, err := tableColumns(db, "powersync_crud")
	if err != nil {
		t.Fatalf("powersync_crud: %v", err)
	}
	want := []string{"op", "id", "type", "data", "old_values", "metadata", "options"}
	if len(cols) != len(want) {
		t.Fatalf("列 = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("列 = %v, want %v", cols, want)
		}
	}

	// 直接写 powersync_crud（模拟应用代码/官方触发器）
	exec(t, db, `INSERT INTO powersync_crud(op, id, type, data, metadata)
	             VALUES('PUT', 'a', 'todos', '{"title":"x"}', 'hint')`)
	entries := crud(t, db)
	if len(entries) != 1 {
		t.Fatalf("ps_crud = %+v", entries)
	}
	if entries[0].Op != "PUT" || entries[0].RowID != "a" || entries[0].Table != "todos" {
		t.Errorf("entry = %+v", entries[0])
	}
	if entries[0].Metadata != "hint" {
		t.Errorf("metadata = %q", entries[0].Metadata)
	}
	if got := jsonString(t, entries[0].Data); got != `{"title":"x"}` {
		t.Errorf("data = %s", got)
	}

	// 暂存行必须被搬走（否则会越积越多）
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM powersync_crud`).Scan(&n); err != nil || n != 0 {
		t.Errorf("powersync_crud 残留 %d 行 (%v)", n, err)
	}

	// 事务外的写也能正常入队
	exec(t, db, `INSERT INTO powersync_crud(op, id, type, data) VALUES('PATCH', 'a', 'todos', '{"title":"y"}')`)
	entries = crud(t, db)
	last := entries[len(entries)-1]
	if last.Op != "PATCH" || string(last.Data) != `{"title":"y"}` {
		t.Errorf("PATCH entry = %+v", last)
	}

	// old_values → old 字段
	exec(t, db, `INSERT INTO powersync_crud(op, id, type, data, old_values)
	             VALUES('PATCH', 'a', 'todos', '{"title":"z"}', '{"title":"y"}')`)
	entries = crud(t, db)
	last = entries[len(entries)-1]
	if string(last.Old) != `{"title":"y"}` {
		t.Errorf("old = %s", last.Old)
	}
}
