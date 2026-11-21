package sync

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// ---------------------------------------------------------------- 测试替身

// fakeStore 实现 Store，只做记录、不做真实落库。
type fakeStore struct {
	buckets      map[string]protocol.OpID
	crud         int64
	target       protocol.OpID
	staged       atomic.Int64
	checkpoints  atomic.Int64
	streamStates atomic.Int64
	purged       atomic.Int64
	resets       atomic.Int64
	applyErr     error
}

func newFakeStore() *fakeStore { return &fakeStore{buckets: map[string]protocol.OpID{}} }

func (s *fakeStore) StageOps(ctx context.Context, bucket string, ops []protocol.Op) error {
	s.staged.Add(int64(len(ops)))
	return nil
}

func (s *fakeStore) ApplyCheckpoint(ctx context.Context, lastOp protocol.OpID, targets []core.BucketTarget, opts core.ApplyOptions) ([]core.ChecksumMismatch, error) {
	s.checkpoints.Add(1)
	if s.applyErr != nil {
		return nil, s.applyErr
	}
	return nil, nil
}

func (s *fakeStore) Buckets(ctx context.Context) (map[string]protocol.OpID, error) {
	return s.buckets, nil
}

func (s *fakeStore) CrudPending(ctx context.Context) (int64, error) { return s.crud, nil }

func (s *fakeStore) TargetCheckpoint(ctx context.Context) (protocol.OpID, error) {
	return s.target, nil
}

func (s *fakeStore) ClearTargetCheckpoint(ctx context.Context) error {
	s.target = 0
	return nil
}

func (s *fakeStore) BucketChecksum(ctx context.Context, bucket string) (uint32, error) { return 0, nil }

func (s *fakeStore) ResetBucket(ctx context.Context, bucket string) error {
	s.resets.Add(1)
	return nil
}

func (s *fakeStore) ActiveStreams(ctx context.Context) ([]core.StreamSubscription, error) {
	return nil, nil
}

func (s *fakeStore) UpdateStreamSyncState(ctx context.Context, states []protocol.StreamState, syncedAt time.Time) error {
	s.streamStates.Add(1)
	return nil
}

func (s *fakeStore) PurgeExpiredStreams(ctx context.Context, now time.Time) (int64, error) {
	s.purged.Add(1)
	return 0, nil
}

type fakeCreds struct {
	endpoint    string
	token       string
	fetches     atomic.Int64
	invalidated atomic.Int64
}

func (c *fakeCreds) FetchCredentials(ctx context.Context) (Credentials, error) {
	c.fetches.Add(1)
	return Credentials{Endpoint: c.endpoint, Token: c.token}, nil
}

func (c *fakeCreds) InvalidateCredentials() { c.invalidated.Add(1) }

type fakeUploader struct {
	attempts atomic.Int64
	failures int64
}

func (u *fakeUploader) Upload(ctx context.Context) error {
	n := u.attempts.Add(1)
	if n <= u.failures {
		return fmt.Errorf("upload 第 %d 次失败（测试）", n)
	}
	return nil
}

func newSyncer(t *testing.T, store Store, creds CredentialsProvider, uploader Uploader) *Syncer {
	t.Helper()
	s, err := New(Options{
		Store:          store,
		Creds:          creds,
		Uploader:       uploader,
		Logger:         slog.New(slog.DiscardHandler), // 测试里不要刷日志
		RetryDelay:     10 * time.Millisecond,
		UploadInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("超时：%s", msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- 用例

// TestLineSourceSelection 验证按 Content-Type 选择 BSON / NDJSON 解码器。
func TestLineSourceSelection(t *testing.T) {
	ndjson := `{"token_expires_in":30}` + "\n"
	src, err := newLineSource("application/x-ndjson; charset=utf-8", strings.NewReader(ndjson), 0)
	if err != nil {
		t.Fatalf("ndjson: %v", err)
	}
	line, err := src.Next()
	if err != nil {
		t.Fatalf("ndjson Next: %v", err)
	}
	if l, ok := line.(*protocol.TokenExpiresInLine); !ok || l.Seconds != 30 {
		t.Fatalf("ndjson 行 = %#v", line)
	}

	// 手搓一个 BSON 文档：{"token_expires_in": 45}
	doc := bsonDoc(bsonInt32("token_expires_in", 45))
	src, err = newLineSource("application/vnd.powersync.bson-stream", strings.NewReader(string(doc)), 0)
	if err != nil {
		t.Fatalf("bson: %v", err)
	}
	line, err = src.Next()
	if err != nil {
		t.Fatalf("bson Next: %v", err)
	}
	if l, ok := line.(*protocol.TokenExpiresInLine); !ok || l.Seconds != 45 {
		t.Fatalf("bson 行 = %#v", line)
	}

	if _, err := newLineSource("text/html", strings.NewReader(""), 0); err == nil {
		t.Fatal("未知 Content-Type 应报错")
	}
}

// TestStaleCheckpoint 验证 write-checkpoint 门控判定。
func TestStaleCheckpoint(t *testing.T) {
	store := newFakeStore()
	s := newSyncer(t, store, &fakeCreds{}, &fakeUploader{})

	// 没有 target：永不 stale
	if stale, err := s.staleCheckpoint(context.Background(), "1"); err != nil || stale {
		t.Fatalf("无 target: stale=%v err=%v", stale, err)
	}

	store.target = 5
	cases := []struct {
		server string
		want   bool
	}{
		{"4", true}, {"5", false}, {"6", false},
	}
	for _, c := range cases {
		stale, err := s.staleCheckpoint(context.Background(), c.server)
		if err != nil {
			t.Fatalf("server=%s: %v", c.server, err)
		}
		if stale != c.want {
			t.Errorf("target=5 server=%s stale=%v, want %v", c.server, stale, c.want)
		}
	}

	// 非数字：不做门控（避免卡死）
	store.target = 5
	if stale, _ := s.staleCheckpoint(context.Background(), "not-a-number"); stale {
		t.Error("非数字 server id 不应判 stale")
	}
}

// TestDownloadRetriesThenSyncs 验证 500 → 重试 → 成功，并触发 FirstSync/状态。
func TestDownloadRetriesThenSyncs(t *testing.T) {
	var calls atomic.Int64
	const body = `{"checkpoint":{"last_op_id":"7","buckets":[{"bucket":"b1","checksum":0,"count":1,"priority":3}],"streams":[{"name":"s1","is_default":true}]}}` + "\n" +
		`{"data":{"bucket":"b1","after":"7","has_more":false,"data":[{"op_id":"7","op":"PUT","object_type":"todos","object_id":"a","checksum":0,"subkey":"s/a","data":"{\"id\":\"a\"}"}]}}` + "\n" +
		`{"checkpoint_complete":{"last_op_id":"7"}}` + "\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, body)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	store := newFakeStore()
	creds := &fakeCreds{endpoint: srv.URL}
	s := newSyncer(t, store, creds, &fakeUploader{})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	select {
	case <-s.FirstSync():
	case <-time.After(5 * time.Second):
		t.Fatal("重试后未完成首次同步")
	}
	if got := calls.Load(); got < 3 {
		t.Errorf("请求次数 = %d, want >=3（两次失败后成功）", got)
	}
	if store.staged.Load() != 1 {
		t.Errorf("staged = %d, want 1", store.staged.Load())
	}
	if store.checkpoints.Load() == 0 {
		t.Error("ApplyCheckpoint 没被调用")
	}
	if store.streamStates.Load() == 0 {
		t.Error("checkpoint.streams 未回填")
	}
	st := s.Status()
	if !st.Connected || st.Cursor != "7" || st.DownloadErr != "" {
		t.Errorf("状态 = %+v", st)
	}

	cancel()
	s.Stop()
}

// TestDownloadChecksumMismatchReconnects 验证 checksum 不一致时重连且计入状态。
func TestDownloadChecksumMismatchReconnects(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"checkpoint":{"last_op_id":"9","buckets":[],"streams":[]}}`+"\n")
		_, _ = io.WriteString(w, `{"checkpoint_complete":{"last_op_id":"9"}}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	// mismatchStore 让 ApplyCheckpoint 永远返回一条 checksum 不一致，验证会重连重试。
	s := newSyncer(t, &mismatchStore{fakeStore: newFakeStore()}, &fakeCreds{endpoint: srv.URL}, &fakeUploader{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	waitUntil(t, 3*time.Second, func() bool { return calls.Load() >= 2 }, "checksum mismatch 后应重连")
	s.Stop()
}

// mismatchStore 让 ApplyCheckpoint 永远返回一条 mismatch，模拟漂移。
type mismatchStore struct{ *fakeStore }

func (m *mismatchStore) ApplyCheckpoint(ctx context.Context, lastOp protocol.OpID, targets []core.BucketTarget, opts core.ApplyOptions) ([]core.ChecksumMismatch, error) {
	m.fakeStore.checkpoints.Add(1)
	return []core.ChecksumMismatch{{Bucket: "b1", Expected: 1, Actual: 2}}, nil
}

// TestUploadRetriesAndRecovers 验证上传失败重试、成功后清空错误。
func TestUploadRetriesAndRecovers(t *testing.T) {
	store := newFakeStore()
	store.crud = 1
	uploader := &fakeUploader{failures: 2}
	s := newSyncer(t, store, &fakeCreds{}, uploader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	waitUntil(t, 5*time.Second, func() bool { return uploader.attempts.Load() >= 3 }, "上传应重试到成功")
	waitUntil(t, 3*time.Second, func() bool { return s.Status().UploadErr == "" }, "成功后 UploadErr 应清空")
	waitUntil(t, 3*time.Second, func() bool { return !s.Status().Uploading }, "Uploading 应复位")
	s.Stop()
}

// TestStopIsPrompt 验证 Stop 能在连接仍打开时快速返回（取消会中断读流）。
func TestStopIsPrompt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"checkpoint":{"last_op_id":"1","buckets":[],"streams":[]}}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 保持连接，直到客户端断开
	}))
	defer srv.Close()

	s := newSyncer(t, newFakeStore(), &fakeCreds{endpoint: srv.URL}, &fakeUploader{})
	s.Start(context.Background())
	waitUntil(t, 3*time.Second, func() bool { return s.Status().Connected }, "应连上")

	done := make(chan struct{})
	go func() { s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop 未在 3s 内返回（可能在读流上阻塞）")
	}
	if st := s.Status(); st.Connected || st.Connecting {
		t.Errorf("Stop 后状态 = %+v", st)
	}
}

// ---------------------------------------------------------------- BSON 手搓工具

type bsonElem struct {
	typ  byte
	name string
	data []byte
}

func bsonDoc(elems ...bsonElem) []byte {
	var body []byte
	for _, e := range elems {
		body = append(body, e.typ)
		body = append(body, e.name...)
		body = append(body, 0)
		body = append(body, e.data...)
	}
	out := make([]byte, 4, 5+len(body))
	binary.LittleEndian.PutUint32(out, uint32(5+len(body)))
	out = append(out, body...)
	return append(out, 0)
}

func bsonInt32(name string, v int32) bsonElem {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return bsonElem{0x10, name, b}
}

// TestCheckpointDiffProvidesTargets 验证：checkpoint_diff 代替 checkpoint 行时，
// 用它给出的 checksum/count 作为目标（并开启校验），removed_buckets 会 ResetBucket。
func TestCheckpointDiffProvidesTargets(t *testing.T) {
	const body = `{"checkpoint_diff":{"last_op_id":"7","removed_buckets":["gone"],"updated_buckets":[{"bucket":"b1","checksum":100,"count":1,"priority":3,"subscriptions":[{"default":1}]}]}}` + "\n" +
		`{"data":{"bucket":"b1","after":"7","has_more":false,"data":[{"op_id":"7","op":"PUT","object_type":"todos","object_id":"a","checksum":100,"subkey":"s/a","data":"{\"id\":\"a\"}"}]}}` + "\n" +
		`{"checkpoint_complete":{"last_op_id":"7"}}` + "\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, body)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	store := &recordingStore{fakeStore: newFakeStore()}
	s := newSyncer(t, store, &fakeCreds{endpoint: srv.URL}, &fakeUploader{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	select {
	case <-s.FirstSync():
	case <-time.After(5 * time.Second):
		t.Fatal("未完成首次同步")
	}
	s.Stop()

	if got := store.fakeStore.resets.Load(); got != 1 {
		t.Errorf("removed_buckets 应 ResetBucket 一次，实际 %d", got)
	}
	if len(store.lastTargets) != 1 || store.lastTargets[0].Name != "b1" {
		t.Fatalf("targets = %+v", store.lastTargets)
	}
	if store.lastTargets[0].Checksum != 100 || store.lastTargets[0].Count != 1 {
		t.Errorf("应使用 checkpoint_diff 的 checksum/count: %+v", store.lastTargets[0])
	}
	if !store.lastOpts.Validate {
		t.Error("有 checkpoint_diff 时应开启 checksum 校验")
	}
}

// recordingStore 记录 ApplyCheckpoint 的入参。
type recordingStore struct {
	*fakeStore
	lastTargets []core.BucketTarget
	lastOpts    core.ApplyOptions
}

func (r *recordingStore) ApplyCheckpoint(ctx context.Context, lastOp protocol.OpID, targets []core.BucketTarget, opts core.ApplyOptions) ([]core.ChecksumMismatch, error) {
	r.lastTargets = append([]core.BucketTarget(nil), targets...)
	r.lastOpts = opts
	return r.fakeStore.ApplyCheckpoint(ctx, lastOp, targets, opts)
}
