package core_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// TestConcurrentSchemaSwapAndApply 让"同步 goroutine 持续 ApplyOps"与"ReplaceSchema 换 schema"
// 并发执行（-race 下能抓出 db.schema 的数据竞争；schema 现在用 atomic.Pointer 保存）。
func TestConcurrentSchemaSwapAndApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "race.db")
	v1 := `{"tables":[{"name":"todos","columns":[{"name":"title","type":"TEXT"}]}]}`
	v2 := `{"tables":[{"name":"todos","columns":[{"name":"title","type":"TEXT"},{"name":"note","type":"TEXT"}]}]}`

	s1, err := core.ParseSchema([]byte(v1))
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{Path: path, Schema: s1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			// 与 ReplaceSchema 并发：错误可以接受，不能 panic/race。
			_ = db.ApplyOps(ctx, "bucket", []protocol.Op{{
				OpID:       protocol.OpID(i + 1),
				Op:         protocol.OpPut,
				ObjectType: "todos",
				ObjectID:   "row",
				Data:       []byte(`{"id":"row","title":"x"}`),
			}}, protocol.OpID(i+1))
			_ = db.Schema()
		}
	}()
	for i := 0; i < 5; i++ {
		raw := v1
		if i%2 == 0 {
			raw = v2
		}
		s, err := core.ParseSchema([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.ReplaceSchema(ctx, s); err != nil {
			t.Fatalf("ReplaceSchema #%d: %v", i, err)
		}
	}
	wg.Wait()

	// 最终 schema 生效：note 列可写可读
	if _, ok := db.Schema().Table("todos"); !ok {
		t.Fatal("最终 schema 丢了 todos")
	}
	if _, err := db.SQL().Exec(`INSERT INTO todos(id, title, note) VALUES('final','t','n')`); err != nil {
		t.Fatalf("最终 schema 缺少 note 列: %v", err)
	}
}
