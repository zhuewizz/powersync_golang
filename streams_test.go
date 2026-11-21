package powersync_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	powersync "msmkSync"
)

const streamsFixtureHead = `{"checkpoint":{"last_op_id":"101","buckets":[` +
	`{"bucket":"50#hi[\"ORG001\"]","checksum":100,"count":1,"priority":3,"subscriptions":[{"default":1}]},` +
	`{"bucket":"50#lo[\"ORG001\"]","checksum":250,"count":1,"priority":0,"subscriptions":[{"default":1}]}` +
	`],"streams":[{"name":"hi","is_default":true,"errors":[]},{"name":"lo","is_default":false,"errors":[]}]}}` + "\n" +
	`{"data":{"bucket":"50#hi[\"ORG001\"]","after":"101","has_more":false,"data":[{"op_id":"100","op":"PUT","object_type":"todos","object_id":"a","checksum":100,"subkey":"s/a","data":"{\"id\":\"a\",\"title\":\"high\"}"}]}}` + "\n" +
	`{"data":{"bucket":"50#lo[\"ORG001\"]","after":"101","has_more":false,"data":[{"op_id":"101","op":"PUT","object_type":"todos","object_id":"b","checksum":250,"subkey":"s/b","data":"{\"id\":\"b\",\"title\":\"low\"}"}]}}` + "\n" +
	`{"partial_checkpoint_complete":{"last_op_id":"101","priority":3}}` + "\n"

// streamsServer 先回放 checkpoint + data + partial_checkpoint_complete，
// 然后**等测试放行**才发 checkpoint_complete（便于断言 partial 之后的状态）。
type streamsServer struct {
	*httptest.Server
	bodies        []string
	partialSent   chan struct{}
	allowComplete chan struct{}
}

func newStreamsServer(t *testing.T) *streamsServer {
	t.Helper()
	s := &streamsServer{partialSent: make(chan struct{}), allowComplete: make(chan struct{})}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.bodies = append(s.bodies, string(body))
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(streamsFixtureHead))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		close(s.partialSent)
		select {
		case <-s.allowComplete:
			_, _ = w.Write([]byte(`{"checkpoint_complete":{"last_op_id":"101"}}` + "\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(s.Close)
	return s
}

func openStreamsDB(t *testing.T, endpoint string) *powersync.DB {
	t.Helper()
	dbPath := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:       dbPath,
		Schema:         powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}}),
		Connector:      &fakeConnector{endpoint: endpoint},
		RetryDelay:     50 * time.Millisecond,
		UploadInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, dbPath)
	return db
}

// TestStreamsPartialCheckpoint 验证：
//   - partial_checkpoint_complete(priority=3) 只物化高优先级 bucket；
//   - 随后 checkpoint_complete 才把低优先级 bucket 补上；
//   - checkpoint.streams 回填订阅状态（hasSynced/isDefault）。
func TestStreamsPartialCheckpoint(t *testing.T) {
	srv := newStreamsServer(t)
	db := openStreamsDB(t, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := db.SubscribeStream(ctx, "lo", map[string]any{"org": "ORG001"},
		powersync.WithPriority(0), powersync.WithTTL(time.Hour))
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// 等 partial 处理完
	select {
	case <-srv.partialSent:
	case <-ctx.Done():
		t.Fatal("没等到 partial_checkpoint_complete")
	}
	waitFor(t, ctx, 3*time.Second, func() bool {
		rows, err := db.GetAll(ctx, `SELECT id FROM todos`)
		return err == nil && len(rows) == 1 && rows[0]["id"] == "a"
	})

	// 放行 checkpoint_complete → 低优先级 bucket 才被应用
	close(srv.allowComplete)
	if err := db.WaitForFirstSync(ctx); err != nil {
		t.Fatalf("WaitForFirstSync: %v", err)
	}
	waitFor(t, ctx, 3*time.Second, func() bool {
		rows, err := db.GetAll(ctx, `SELECT id FROM todos ORDER BY id`)
		return err == nil && len(rows) == 2 && rows[1]["id"] == "b"
	})

	// 请求体：include_defaults + 显式订阅
	if len(srv.bodies) == 0 {
		t.Fatal("没有捕获到同步请求")
	}
	var req struct {
		Streams *struct {
			IncludeDefaults bool `json:"include_defaults"`
			Subscriptions   []struct {
				Stream           string         `json:"stream"`
				Parameters       map[string]any `json:"parameters"`
				OverridePriority *int           `json:"override_priority"`
			} `json:"subscriptions"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(srv.bodies[0]), &req); err != nil {
		t.Fatalf("请求体非法: %v (%s)", err, srv.bodies[0])
	}
	if req.Streams == nil || !req.Streams.IncludeDefaults {
		t.Fatalf("请求应带 streams.include_defaults=true: %s", srv.bodies[0])
	}
	if len(req.Streams.Subscriptions) != 1 {
		t.Fatalf("订阅数 = %d: %s", len(req.Streams.Subscriptions), srv.bodies[0])
	}
	s := req.Streams.Subscriptions[0]
	if s.Stream != "lo" || s.Parameters["org"] != "ORG001" || s.OverridePriority == nil || *s.OverridePriority != 0 {
		t.Errorf("订阅请求字段 = %+v", s)
	}

	// 订阅状态回填
	state, err := sub.State(ctx)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if !state.HasSynced || state.LastSyncedAt == nil {
		t.Errorf("订阅应已同步过: %+v", state)
	}
	if state.IsDefault {
		t.Error("lo 不是默认流")
	}

	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if st, _ := sub.State(ctx); st.Active {
		t.Error("取消后应 active=false")
	}
	if subs, _ := db.StreamSubscriptions(ctx); len(subs) != 0 {
		t.Errorf("取消后 active 订阅 = %+v", subs)
	}
}

func waitFor(t *testing.T, ctx context.Context, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatal("条件未满足（超时）")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// streamsServerFor 只启动一个最小假服务（返回 up-to-date），
// 请求体通过 channel 交给测试，避免跨 goroutine 共享 slice 的 race。
func streamsServerFor(t *testing.T) (string, <-chan string) {
	t.Helper()
	bodies := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case bodies <- string(body):
		default:
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(
			`{"checkpoint":{"last_op_id":"101","buckets":[],"streams":[]}}` + "\n" +
				`{"checkpoint_complete":{"last_op_id":"101"}}` + "\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv.URL, bodies
}
