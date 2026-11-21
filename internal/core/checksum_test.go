package core_test

import (
	"context"
	"encoding/json"
	"testing"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// put/remove 构造 op，checksum 使用服务端语义：
// PUT 的 checksum 是"该行新状态"的校验和；REMOVE 的 checksum 是"被删行"校验和的负数
// （实测两个方向相加后回到空 bucket 的 0）。
func put(opID protocol.OpID, id string, ck int64, data string) protocol.Op {
	return protocol.Op{
		OpID: opID, Op: protocol.OpPut, ObjectType: "todos", ObjectID: id,
		Subkey: "sub1", Checksum: ck, Data: json.RawMessage(data),
	}
}

func remove(opID protocol.OpID, id string, ck int64) protocol.Op {
	return protocol.Op{
		OpID: opID, Op: protocol.OpRemove, ObjectType: "todos", ObjectID: id,
		Subkey: "sub1", Checksum: ck,
	}
}

// TestChecksumSupersedeAndRemove 锁定 add/op 的累计规则（与 powersync-sqlite-core 的
// insert_bucket_operations 逐行一致，已在真实服务上由 scripts/probe-checksum.sh 验证）：
//
//	首次 PUT(ck=100)：   add=0    op=100   total=100
//	同 key PUT(ck=250)： add=100  op=250   total=350   ← 注意不是 250
//	REMOVE(ck=-250)：    add=100  op=0     total=100
//
// 服务端的 bucket checksum 就是这条累计链的结果（不是"当前行校验和"），
// 所以删除后不会回到 0；不要按直觉改这段逻辑。
func TestChecksumSupersedeAndRemove(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	const bucket = "b-checksum"

	sum := func() uint32 {
		t.Helper()
		got, err := db.BucketChecksum(ctx, bucket)
		if err != nil {
			t.Fatalf("BucketChecksum: %v", err)
		}
		return got
	}

	// 1) 首次 PUT
	if err := db.StageOps(ctx, bucket, []protocol.Op{put(1, "a", 100, `{"id":"a","title":"v1"}`)}); err != nil {
		t.Fatal(err)
	}
	if got := sum(); got != 100 {
		t.Fatalf("首次 PUT 后校验和 = %d, want 100", got)
	}

	// 2) 同 key 更新（supersede）
	if err := db.StageOps(ctx, bucket, []protocol.Op{put(2, "a", 250, `{"id":"a","title":"v2"}`)}); err != nil {
		t.Fatal(err)
	}
	if got := sum(); got != 350 {
		t.Fatalf("supersede 后校验和 = %d, want 350（add=100 + op=250）", got)
	}
	if rows, _, _ := db.OplogStats(ctx, bucket); rows != 1 {
		t.Errorf("oplog 行数 = %d, want 1（旧行应被删掉）", rows)
	}

	// 3) 删除（REMOVE 的 checksum 是负数）
	if err := db.StageOps(ctx, bucket, []protocol.Op{remove(3, "a", -250)}); err != nil {
		t.Fatal(err)
	}
	if got := sum(); got != 100 {
		t.Fatalf("REMOVE 后校验和 = %d, want 100", got)
	}
	if rows, _, _ := db.OplogStats(ctx, bucket); rows != 0 {
		t.Errorf("oplog 行数 = %d, want 0", rows)
	}
}

// TestApplyCheckpointValidatesAndMaterializes 验证 checkpoint_complete 的处理顺序：
// 先校验，通过才物化；不通过则重置 bucket 且不动用户表。
func TestApplyCheckpointValidatesAndMaterializes(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	const bucket = "b-apply"

	ops := []protocol.Op{put(1, "a", 100, `{"id":"a","title":"v1"}`)}
	if err := db.StageOps(ctx, bucket, ops); err != nil {
		t.Fatal(err)
	}

	// 校验和错误：应返回 mismatch 并重置，不物化。
	bad, err := db.ApplyCheckpoint(ctx, 1,
		[]core.BucketTarget{{Name: bucket, Checksum: 999, Count: 1}},
		core.ApplyOptions{Validate: true, ResetOnMismatch: true})
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if len(bad) != 1 {
		t.Fatalf("mismatch = %+v, want 1 条", bad)
	}
	if bad[0].Actual != 100 || bad[0].Expected != 999 {
		t.Errorf("mismatch 内容 = %+v", bad[0])
	}
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM todos`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("校验失败不应物化，todos 行数 = %d", n)
	}
	if cur, _ := db.BucketCursor(ctx, bucket); cur != 0 {
		t.Errorf("重置后游标 = %d, want 0", cur)
	}
	if rows, _, _ := db.OplogStats(ctx, bucket); rows != 0 {
		t.Errorf("重置后 oplog 行数 = %d, want 0", rows)
	}

	// 重新 stage 并用正确 checksum 提交。
	if err := db.StageOps(ctx, bucket, ops); err != nil {
		t.Fatal(err)
	}
	bad, err = db.ApplyCheckpoint(ctx, 1,
		[]core.BucketTarget{{Name: bucket, Checksum: 100, Count: 1}},
		core.ApplyOptions{Validate: true, ResetOnMismatch: true})
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if len(bad) != 0 {
		t.Fatalf("不应有 mismatch: %+v", bad)
	}
	var title string
	if err := db.SQL().QueryRow(`SELECT title FROM todos WHERE id='a'`).Scan(&title); err != nil {
		t.Fatalf("物化失败: %v", err)
	}
	if title != "v1" {
		t.Errorf("title = %q", title)
	}
	if cur, _ := db.BucketCursor(ctx, bucket); cur != 1 {
		t.Errorf("游标 = %d, want 1", cur)
	}

	// 物化后本地写应能入队（守卫已释放）。
	exec(t, db, `UPDATE todos SET title='local' WHERE id='a'`)
	if entries := crud(t, db); len(entries) != 1 {
		t.Fatalf("物化后本地写未入队: %+v", entries)
	}
}

// TestApplyCheckpointRemoveMaterializes 验证 REMOVE 会真的删除用户表里的行
// （REMOVE 不写 oplog，靠 ps_updated_rows 触发删除）。
func TestApplyCheckpointRemoveMaterializes(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	const bucket = "b-remove"

	// 先同步出一行
	if err := db.StageOps(ctx, bucket, []protocol.Op{put(1, "a", 100, `{"id":"a","title":"v1"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplyCheckpoint(ctx, 1, []core.BucketTarget{{Name: bucket, Checksum: 100, Count: 1}},
		core.ApplyOptions{Validate: true, ResetOnMismatch: true}); err != nil {
		t.Fatal(err)
	}

	// 再下发 REMOVE：bucket 变空，期望 checksum 0
	if err := db.StageOps(ctx, bucket, []protocol.Op{remove(2, "a", -100)}); err != nil {
		t.Fatal(err)
	}
	bad, err := db.ApplyCheckpoint(ctx, 2, []core.BucketTarget{{Name: bucket, Checksum: 0, Count: 0}},
		core.ApplyOptions{Validate: true, ResetOnMismatch: true})
	if err != nil || len(bad) > 0 {
		t.Fatalf("ApplyCheckpoint: %v %+v", err, bad)
	}
	var n int
	if err := db.SQL().QueryRow(`SELECT count(*) FROM todos`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("REMOVE 后行仍在（count=%d）", n)
	}
}

// TestBucketsUseStagedCursor 锁定"请求游标用 last_op"这一语义：
// StageOps 之后（还没到 checkpoint_complete/物化），Buckets() 必须已经前进，
// 否则中途断流会重发同一批 data，checksum 累计会漂移。
func TestBucketsUseStagedCursor(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	const bucket = "b-staged"

	if err := db.StageOps(ctx, bucket, []protocol.Op{put(7, "a", 100, `{"id":"a","title":"v1"}`)}); err != nil {
		t.Fatal(err)
	}
	cursors, err := db.Buckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := cursors[bucket]; got != 7 {
		t.Fatalf("Buckets()[%q] = %d, want 7（应取 last_op，而不是 last_applied_op）", bucket, got)
	}
	if applied, _ := db.BucketCursor(ctx, bucket); applied != 0 {
		t.Fatalf("last_applied_op = %d, want 0（还没到 checkpoint_complete）", applied)
	}
}

// TestTargetCheckpoint 验证"本地写入要求 write checkpoint"的读写与清除。
func TestTargetCheckpoint(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()

	got, err := db.TargetCheckpoint(ctx)
	if err != nil || got != 0 {
		t.Fatalf("初始 target = %d, %v; want 0", got, err)
	}

	// 造一条待上传变更，然后带 write checkpoint 出队。
	exec(t, db, `INSERT INTO todos(id,title) VALUES('a','x')`)
	entries := crud(t, db)
	if len(entries) != 1 {
		t.Fatalf("ps_crud 条数 = %d", len(entries))
	}
	if err := db.CompleteCrud(ctx, entries[0].ID, 42); err != nil {
		t.Fatal(err)
	}
	if got, err := db.TargetCheckpoint(ctx); err != nil || got != 42 {
		t.Fatalf("target = %d, %v; want 42", got, err)
	}
	if got, _ := db.Meta(ctx, "target_checkpoint_request_id"); got != "42" {
		t.Errorf("ps_kv.target_checkpoint_request_id = %q, want 42（与 core v14 命名一致）", got)
	}

	if err := db.ClearTargetCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := db.TargetCheckpoint(ctx); got != 0 {
		t.Fatalf("清除后 target = %d, want 0", got)
	}
}
