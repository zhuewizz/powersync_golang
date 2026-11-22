package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/zhuewizz/powersync_golang/internal/protocol"
)

// TestStreamSubscriptions 覆盖订阅写入/查询/取消/过期清理。
func TestStreamSubscriptions(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()

	ttl := 30 * time.Minute
	prio := 0
	sub, err := db.SubscribeStream(ctx, "box_data", map[string]any{"box": "BOX-A"}, &ttl, &prio)
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if sub.ID == 0 || !sub.Active || sub.Name != "box_data" {
		t.Fatalf("订阅 = %+v", sub)
	}
	if sub.Parameters["box"] != "BOX-A" || sub.Priority == nil || *sub.Priority != 0 {
		t.Errorf("参数/优先级 = %+v", sub)
	}
	if sub.TTL == nil || *sub.TTL != ttl || sub.ExpiresAt == nil {
		t.Errorf("ttl/expires = %+v", sub)
	}

	// 同 name+params 再订阅：更新而不是插入新行
	if _, err := db.SubscribeStream(ctx, "box_data", map[string]any{"box": "BOX-A"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	active, err := db.ActiveStreams(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 {
		t.Fatalf("active 订阅数 = %d, want 1", len(active))
	}
	if active[0].TTL != nil {
		t.Errorf("重新订阅后 ttl 应清空: %+v", active[0])
	}

	// 默认参数（nil map）算 "null"
	if _, err := db.SubscribeStream(ctx, "org_data", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := db.StreamSubscription(ctx, "org_data", nil); got == nil || got.Name != "org_data" {
		t.Fatalf("默认参数订阅未找到: %+v", got)
	}
	if active, _ = db.ActiveStreams(ctx); len(active) != 2 {
		t.Fatalf("active = %d, want 2", len(active))
	}

	// 取消订阅 → 非 active；带 ttl 时从"现在"开始过期
	if ttl != 0 {
		if _, err := db.SubscribeStream(ctx, "box_data", map[string]any{"box": "BOX-A"}, &ttl, nil); err != nil {
			t.Fatal(err)
		}
	}
	sub, _ = db.StreamSubscription(ctx, "box_data", map[string]any{"box": "BOX-A"})
	if err := db.UnsubscribeStream(ctx, sub.ID); err != nil {
		t.Fatalf("UnsubscribeStream: %v", err)
	}
	got, _ := db.StreamSubscription(ctx, "box_data", map[string]any{"box": "BOX-A"})
	if got.Active {
		t.Error("取消后应 active=0")
	}
	if active, _ = db.ActiveStreams(ctx); len(active) != 1 {
		t.Errorf("取消后 active = %d, want 1", len(active))
	}

	// 过期清理（把 expires_at 拨到过去）
	if _, err := db.SQL().Exec(`UPDATE ps_stream_subscriptions SET expires_at = 1 WHERE stream_name='box_data'`); err != nil {
		t.Fatal(err)
	}
	n, err := db.PurgeExpiredStreams(ctx, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("PurgeExpiredStreams = %d, %v; want 1", n, err)
	}
	if got, _ := db.StreamSubscription(ctx, "box_data", map[string]any{"box": "BOX-A"}); got != nil {
		t.Error("过期订阅应被删除")
	}
}

// TestUpdateStreamSyncState 覆盖 checkpoint streams 信息回填。
func TestUpdateStreamSyncState(t *testing.T) {
	db := openTestDB(t, true)
	ctx := context.Background()
	if _, err := db.SubscribeStream(ctx, "box_data", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.UpdateStreamSyncState(ctx, []protocol.StreamState{
		{Name: "box_data", IsDefault: true},
	}, now); err != nil {
		t.Fatalf("UpdateStreamSyncState: %v", err)
	}
	sub, err := db.StreamSubscription(ctx, "box_data", nil)
	if err != nil || sub == nil {
		t.Fatalf("查询订阅: %v %+v", err, sub)
	}
	if !sub.IsDefault {
		t.Error("is_default 应为 true")
	}
	if sub.LastSyncedAt == nil {
		t.Fatal("last_synced_at 未回填")
	}
	if delta := sub.LastSyncedAt.Sub(now); delta < -time.Second || delta > time.Second {
		t.Errorf("last_synced_at = %v, want ~%v", sub.LastSyncedAt, now)
	}
}

// TestStreamTableLayout 验证表结构与 core migration 11 一致。
func TestStreamTableLayout(t *testing.T) {
	db := openTestDB(t, true)
	cols, err := tableColumns(db, "ps_stream_subscriptions")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "stream_name", "active", "is_default", "local_priority",
		"local_params", "ttl", "expires_at", "last_synced_at"}
	if len(cols) != len(want) {
		t.Fatalf("列 = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("列 = %v, want %v", cols, want)
		}
	}
}
