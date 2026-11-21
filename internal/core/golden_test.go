package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// TestApplyGoldenStream 用真实抓包的 NDJSON 走一遍"解码 → 落表"，
// 验证协议层与本地库拼接后行为正确（表/行数、bucket 游标、不上传队列）。
func TestApplyGoldenStream(t *testing.T) {
	const schemaJSON = `{"tables":[
	  {"name":"box","columns":[{"name":"org_code","type":"TEXT"},{"name":"box_code","type":"TEXT"},{"name":"box_name","type":"TEXT"},{"name":"is_active","type":"INTEGER"}]},
	  {"name":"box_seq","columns":[{"name":"box_code","type":"TEXT"},{"name":"seq","type":"INTEGER"}]},
	  {"name":"org","columns":[{"name":"org_code","type":"TEXT"},{"name":"org_name","type":"TEXT"}]},
	  {"name":"sys_config","columns":[{"name":"org_code","type":"TEXT"},{"name":"box_type","type":"TEXT"}]},
	  {"name":"users","columns":[{"name":"org_code","type":"TEXT"},{"name":"user_name","type":"TEXT"}]}
	]}`
	schema, err := core.ParseSchema([]byte(schemaJSON))
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{
		Path:   filepath.Join(t.TempDir(), "golden.db"),
		Schema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f, err := os.Open(filepath.Join("..", "..", "testdata", "sync", "full-snapshot-raw.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	dec := protocol.NewDecoder(f, 0)
	bucket := ""
	var applied int
	for {
		line, err := dec.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		switch l := line.(type) {
		case *protocol.CheckpointLine:
			bucket = l.Buckets[0].Bucket
		case *protocol.DataLine:
			last := l.After
			if n := len(l.Ops); n > 0 {
				last = l.Ops[n-1].OpID
			}
			if err := db.ApplyOps(ctx, l.Bucket, l.Ops, last); err != nil {
				t.Fatalf("ApplyOps(%s): %v", l.Bucket, err)
			}
			applied += len(l.Ops)
		}
	}
	if applied == 0 {
		t.Fatal("golden 里没有应用到任何 op")
	}

	// golden 共 5 条 op：box/box_seq/org/sys_config/users 各 1。
	var rows int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM box`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("box 行数 = %d, want 1", rows)
	}
	var name string
	if err := db.SQL().QueryRow(`SELECT box_name FROM box`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "sample-box" {
		t.Errorf("box_name = %q, want sample-box", name)
	}

	cur, err := db.BucketCursor(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 101 {
		t.Errorf("bucket 游标 = %d, want 101（取最后一条 op 的 op_id）", cur)
	}

	entries, err := db.CrudBatch(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("下行数据不应进入上传队列，得到 %d 条", len(entries))
	}
}
