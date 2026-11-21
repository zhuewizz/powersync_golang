package powersync_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	powersync "msmkSync"
)

// ---------------------------------------------------------------- 测试替身

type fakeConnector struct {
	endpoint   string
	mu         sync.Mutex
	fetches    int
	uploads    int
	uploadErr  error
	invalidCtl int
}

func (c *fakeConnector) FetchCredentials(ctx context.Context) (powersync.Credentials, error) {
	c.mu.Lock()
	c.fetches++
	c.mu.Unlock()
	return powersync.Credentials{Endpoint: c.endpoint, Token: "test-token"}, nil
}

func (c *fakeConnector) InvalidateCredentials() {
	c.mu.Lock()
	c.invalidCtl++
	c.mu.Unlock()
}

func (c *fakeConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 20)
	if err != nil {
		return err
	}
	if batch == nil {
		return nil
	}
	if err := c.uploadErr; err != nil {
		return err
	}
	c.mu.Lock()
	c.uploads++
	c.mu.Unlock()
	return batch.Complete(ctx)
}

func (c *fakeConnector) counts() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetches, c.uploads, c.invalidCtl
}

var bucketAfterRe = regexp.MustCompile(`"after":"([0-9]+)"`)

// fakeServer 回放 golden NDJSON：
//   - 请求里所有 bucket 的 after 都 >= 101 时，回 "已是最新"（只有 checkpoint）；
//   - 否则回全量 fixture；
//   - 回完保持连接（模拟服务端推送），直到客户端断开。
type fakeServer struct {
	*httptest.Server
	requests atomic.Int32
	authFail atomic.Int32 // >0 时先返回该次数的 401
}

func newFakeServer(t *testing.T, full, uptodate string) *fakeServer {
	t.Helper()
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join("testdata", "sync", name))
		if err != nil {
			t.Fatalf("读取 fixture: %v", err)
		}
		return string(b)
	}
	fullBody, upBody := read(full), read(uptodate)

	fs := &fakeServer{}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.requests.Add(1)
		if fs.authFail.Load() > 0 {
			fs.authFail.Add(-1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"PSYNC_S2106","status":401,"description":"Authentication required","name":"AuthorizationError"}}`))
			return
		}
		if r.URL.Path != "/sync/stream" {
			http.NotFound(w, r)
			return
		}
		var req struct {
			Buckets []struct {
				Name  string `json:"name"`
				After string `json:"after"`
			} `json:"buckets"`
		}
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		_ = json.Unmarshal(body, &req)

		upToDate := len(req.Buckets) > 0
		for _, b := range req.Buckets {
			if b.After < "101" && !(len(b.After) > 3) {
				upToDate = false
			}
			if len(b.After) > 3 {
				continue
			}
		}
		payload := fullBody
		if upToDate {
			payload = upBody
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 保持连接，直到客户端关闭
	}))
	t.Cleanup(fs.Close)
	return fs
}

func openTestDB(t *testing.T, endpoint string) (*powersync.DB, *fakeConnector) {
	t.Helper()
	conn := &fakeConnector{endpoint: endpoint}
	schema := powersync.NewSchema(powersync.Table{
		Name: "todos",
		Columns: []powersync.Column{
			{Name: "title", Type: powersync.TextColumn},
			{Name: "done", Type: powersync.IntegerColumn},
		},
		Indexes: []powersync.Index{{Name: "todos_title", Columns: []string{"title"}}},
	})
	dbPath := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:       dbPath,
		Schema:         schema,
		Connector:      conn,
		RetryDelay:     50 * time.Millisecond,
		UploadInterval: 50 * time.Millisecond,
		WatchInterval:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, dbPath)
	return db, conn
}

// tempDBPath 给每个测试一个独立目录。清理用 os.RemoveAll 尽力而为：
// Windows 上 SQLite 句柄释放可能滞后，用 t.TempDir 会因此把测试判失败。
func tempDBPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "powersync-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "app.db")
}

// cleanupDBPath 只负责关库（关库后文件句柄才释放）。
func cleanupDBPath(t *testing.T, db *powersync.DB, path string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.Close(ctx); err != nil {
			t.Logf("Close: %v", err)
		}
	})
}

// ---------------------------------------------------------------- 用例

// TestConnectDownloadsAndAppliesData 覆盖最小闭环：Connect → 下载 → 校验 → 落表。
func TestConnectDownloadsAndAppliesData(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	db, _ := openTestDB(t, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		t.Fatalf("WaitForFirstSync: %v", err)
	}

	rows, err := db.GetAll(ctx, `SELECT id, title, done FROM todos ORDER BY id`)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("todos 行数 = %d, want 2: %+v", len(rows), rows)
	}
	if rows[0]["title"] != "first" || rows[1]["title"] != "second" {
		t.Errorf("落库内容不对: %+v", rows)
	}

	st := db.CurrentStatus()
	if !st.Connected {
		t.Errorf("状态应为已连接: %+v", st)
	}
	if st.Cursor != "101" {
		t.Errorf("Cursor = %q, want 101", st.Cursor)
	}

	// 下行数据不应进入上传队列。
	if n, err := db.UploadQueue(ctx); err != nil || n != 0 {
		t.Errorf("上传队列 = %d (%v), want 0", n, err)
	}
}

// TestLocalWriteQueuesAndUploads 覆盖本地写 → ps_crud → connector 上传 → 出队。
func TestLocalWriteQueuesAndUploads(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	db, conn := openTestDB(t, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		t.Fatalf("WaitForFirstSync: %v", err)
	}

	// 本地写 → 触发上传
	if _, err := db.Execute(ctx, `INSERT INTO todos(id, title, done) VALUES(?, ?, ?)`, "local-1", "mine", 1); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, err := db.UploadQueue(ctx)
		if err != nil {
			t.Fatalf("UploadQueue: %v", err)
		}
		_, uploads, _ := conn.counts()
		if n == 0 && uploads > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("上传未完成：队列=%d uploads=%d", n, uploads)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, uploads, _ := conn.counts(); uploads == 0 {
		t.Error("connector.UploadData 未被调用")
	}
	if rows, err := db.GetAll(ctx, `SELECT title FROM todos WHERE id='local-1'`); err != nil || len(rows) != 1 {
		t.Fatalf("本地行丢了: %v %+v", err, rows)
	}
}

// TestCrudBatchSemantics 覆盖 GetCrudBatch / GetNextCrudTransaction / Complete。
func TestCrudBatchSemantics(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	db, _ := openTestDB(t, srv.URL)
	ctx := context.Background()

	if batch, err := db.GetCrudBatch(ctx, 10); err != nil || batch != nil {
		t.Fatalf("空队列应返回 nil: %+v %v", batch, err)
	}

	if err := db.WriteTransaction(ctx, func(ctx context.Context, tx *powersync.Tx) error {
		if _, err := tx.Execute(ctx, `INSERT INTO todos(id, title) VALUES('x','tx1')`); err != nil {
			return err
		}
		_, err := tx.Execute(ctx, `INSERT INTO todos(id, title) VALUES('y','tx1')`)
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction: %v", err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO todos(id, title) VALUES('z','tx2')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	txn, err := db.GetNextCrudTransaction(ctx)
	if err != nil || txn == nil {
		t.Fatalf("GetNextCrudTransaction: %+v %v", txn, err)
	}
	if len(txn.Entries) != 2 {
		t.Fatalf("事务内条数 = %d, want 2（同事务的写应聚合）: %+v", len(txn.Entries), txn.Entries)
	}
	for _, e := range txn.Entries {
		if e.Op != powersync.UpdateTypePut || e.Table != "todos" {
			t.Errorf("entry = %+v", e)
		}
	}
	if err := txn.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	batch, err := db.GetCrudBatch(ctx, 10)
	if err != nil || batch == nil {
		t.Fatalf("GetCrudBatch: %+v %v", batch, err)
	}
	if len(batch.Entries) != 1 || batch.Entries[0].ID != "z" {
		t.Fatalf("剩余 = %+v", batch.Entries)
	}
	if batch.HaveMore {
		t.Error("HaveMore 应为 false")
	}
	if err := batch.Complete(ctx, "42"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if n, _ := db.UploadQueue(ctx); n != 0 {
		t.Errorf("出队后队列 = %d, want 0", n)
	}
}

// TestQueryHelpers 覆盖 Execute/GetAll/GetOptional/WriteTransaction 的回滚。
func TestQueryHelpers(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	db, _ := openTestDB(t, srv.URL)
	ctx := context.Background()

	// 默认 JSON 表是视图，INSTEAD OF 触发器下 SQLite 报告 0 行受影响（见 query.go 注释）。
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('q1','a')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	row, found, err := db.GetOptional(ctx, `SELECT title FROM todos WHERE id='q1'`)
	if err != nil || !found || row["title"] != "a" {
		t.Fatalf("GetOptional = %+v %v %v", row, found, err)
	}
	if _, found, _ := db.GetOptional(ctx, `SELECT title FROM todos WHERE id='nope'`); found {
		t.Error("不存在的行应 found=false")
	}

	errBoom := errors.New("boom")
	err = db.WriteTransaction(ctx, func(ctx context.Context, tx *powersync.Tx) error {
		if _, err := tx.Execute(ctx, `INSERT INTO todos(id,title) VALUES('q2','b')`); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("WriteTransaction 应返回原错误: %v", err)
	}
	if _, found, _ := db.GetOptional(ctx, `SELECT 1 FROM todos WHERE id='q2'`); found {
		t.Error("事务回滚后不应留下数据")
	}
}

// TestWatchPollsForChanges 覆盖 Watch 的轮询推送。
func TestWatchPollsForChanges(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	db, _ := openTestDB(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	first := <-ch
	if len(first) != 1 {
		t.Fatalf("首次推送 = %+v", first)
	}
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('w1','w')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case rows := <-ch:
		if rows[0]["n"] != int64(1) {
			t.Errorf("变化后 = %+v", rows)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Watch 未推送变化")
	}
}

// TestUnauthorizedRefetchesCredentials 覆盖 401 → 重新取凭据 → 连接成功。
func TestUnauthorizedRefetchesCredentials(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	srv.authFail.Store(1)
	db, conn := openTestDB(t, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := db.WaitForFirstSync(ctx); err != nil {
		t.Fatalf("WaitForFirstSync: %v", err)
	}
	fetches, _, invalidated := conn.counts()
	if fetches < 2 {
		t.Errorf("401 后应重新取凭据：fetches=%d", fetches)
	}
	if invalidated == 0 {
		t.Error("401 后应调用 InvalidateCredentials")
	}
}

// TestSchemaJSONRoundTrip 覆盖 LoadSchemaJSON ↔ MarshalJSON。
func TestSchemaJSONRoundTrip(t *testing.T) {
	raw := `{"tables":[{"name":"todos","columns":[{"name":"title","type":"TEXT"},{"name":"done","type":"INTEGER"}],"indexes":[{"name":"todos_title","columns":[{"name":"title","ascending":true}]}],"local_only":false,"insert_only":false}],"raw_tables":[]}`
	s, err := powersync.LoadSchemaJSON([]byte(raw))
	if err != nil {
		t.Fatalf("LoadSchemaJSON: %v", err)
	}
	if tables := s.Tables(); len(tables) != 1 || len(tables[0].Columns) != 2 {
		t.Fatalf("解析结果 = %+v", s.Tables())
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(b), `"name":"todos"`) || !strings.Contains(string(b), `"type":"INTEGER"`) {
		t.Errorf("Marshal 结果 = %s", b)
	}
	if _, err := powersync.LoadSchemaJSON([]byte(`{"tables":[]}`)); err == nil {
		t.Error("空 schema 应报错")
	}
}

// echoConnector 上传时带上 write checkpoint（模拟"本地写入 → 服务端回显"链路）。
type echoConnector struct {
	fakeConnector
	writeCheckpoint string
	mu2             sync.Mutex
	completed       int
}

func (c *echoConnector) UploadData(ctx context.Context, db *powersync.DB) error {
	batch, err := db.GetCrudBatch(ctx, 20)
	if err != nil || batch == nil {
		return err
	}
	c.mu2.Lock()
	c.completed++
	c.mu2.Unlock()
	return batch.Complete(ctx, c.writeCheckpoint)
}

// TestStaleCheckpointIsSkippedUntilEcho 验证 write checkpoint 门控：
// 本地写入上传后（target=5），服务端先给一个 request_id=4 的 checkpoint（不含本地写），
// 客户端必须跳过物化并重连；收到 request_id=5 后才应用。
func TestStaleCheckpointIsSkippedUntilEcho(t *testing.T) {
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join("testdata", "sync", name))
		if err != nil {
			t.Fatalf("读取 fixture: %v", err)
		}
		return string(b)
	}
	staleBody := strings.ReplaceAll(read("apply-todos.ndjson"),
		`{"checkpoint_complete":{"last_op_id":"101"}}`,
		`{"checkpoint_complete":{"last_op_id":"101","checkpoint_request_id":"4"}}`)
	// 第二次及以后：数据已经 staged（after=101），真实服务端只会回 checkpoint，
	// 不会重发 data。这里构造“只有 checkpoint + 带 request_id 的 complete”。
	lines := strings.Split(strings.TrimSpace(read("apply-todos.ndjson")), "\n")
	freshBody := lines[0] + "\n" +
		`{"checkpoint_complete":{"last_op_id":"101","checkpoint_request_id":"5"}}` + "\n"

	var streams atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := streams.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		if n == 1 {
			_, _ = w.Write([]byte(staleBody))
		} else {
			// 后续连接：数据已 staged，只回 checkpoint（真实服务端在 after 之后不会再发旧数据）。
			_, _ = w.Write([]byte(freshBody))
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	// 注意：不要用 defer srv.Close()——那会在清理 DB 之前阻塞（客户端还连着）。
	t.Cleanup(srv.Close)

	conn := &echoConnector{writeCheckpoint: "5"}
	conn.endpoint = srv.URL
	schema := powersync.NewSchema(powersync.Table{
		Name:    "todos",
		Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}, {Name: "done", Type: powersync.IntegerColumn}},
	})
	dbPath := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:       dbPath,
		Schema:         schema,
		Connector:      conn,
		RetryDelay:     20 * time.Millisecond,
		UploadInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, dbPath)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// 先本地写并上传，建立 target=5。
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('local','mine')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn.mu2.Lock()
		done := conn.completed > 0
		conn.mu2.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("上传未完成")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 第一次同步会因 stale checkpoint 被跳过，之后重连应用。
	if err := db.WaitForFirstSync(ctx); err != nil {
		t.Fatalf("WaitForFirstSync: %v", err)
	}
	if n := streams.Load(); n < 2 {
		t.Errorf("应至少重连一次（实际 %d 次）", n)
	}
	rows, err := db.GetAll(ctx, `SELECT id FROM todos ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r["id"].(string))
	}
	if strings.Join(ids, ",") != "a,b,local" {
		t.Fatalf("落库 id = %v, want [a b local]", ids)
	}
}

// TestDatabaseLocked 验证公开 API 层面同一 DB 被两个实例打开会返回 ErrDatabaseLocked。
func TestDatabaseLocked(t *testing.T) {
	path := tempDBPath(t)
	schema := powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}})
	db1, err := powersync.Open(context.Background(), powersync.Options{Filename: path, Schema: schema, Connector: &fakeConnector{endpoint: "http://127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	t.Cleanup(func() { _ = db1.Close(context.Background()) })

	if _, err := powersync.Open(context.Background(), powersync.Options{Filename: path, Schema: schema, Connector: &fakeConnector{endpoint: "http://127.0.0.1:1"}}); !errors.Is(err, powersync.ErrDatabaseLocked) {
		t.Fatalf("Open #2 err = %v, want ErrDatabaseLocked", err)
	}

	// 关掉锁之后可以重开
	if err := db1.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db3, err := powersync.Open(context.Background(), powersync.Options{Filename: path, Schema: schema, Connector: &fakeConnector{endpoint: "http://127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("重新 Open: %v", err)
	}
	_ = db3.Close(context.Background())
}

// TestOpenWithoutConnector 验证可以只开本地库（不接同步），Connect 时才报 ErrNotConnected。
func TestOpenWithoutConnector(t *testing.T) {
	path := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename: path,
		Schema:   powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}}),
	})
	if err != nil {
		t.Fatalf("Open（无 Connector）: %v", err)
	}
	cleanupDBPath(t, db, path)

	ctx := context.Background()
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('a','x')`); err != nil {
		t.Fatalf("本地写: %v", err)
	}
	batch, err := db.GetCrudBatch(ctx, 10)
	if err != nil || batch == nil || len(batch.Entries) != 1 {
		t.Fatalf("本地队列: %+v %v", batch, err)
	}
	if err := batch.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := db.Connect(ctx); !errors.Is(err, powersync.ErrNotConnected) {
		t.Fatalf("Connect err = %v, want ErrNotConnected", err)
	}
}

// TestWatchPushesOnLocalWrite 验证本地写会**立即**推送（不依赖兜底轮询）。
// WatchInterval 特意设成 10s：如果走轮询，测试会超时。
func TestWatchPushesOnLocalWrite(t *testing.T) {
	path := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:      path,
		Schema:        powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}}),
		WatchInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, path)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := <-ch; got[0]["n"] != int64(0) {
		t.Fatalf("初值 = %+v", got)
	}

	start := time.Now()
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('a','x')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case rows := <-ch:
		if rows[0]["n"] != int64(1) {
			t.Errorf("推送值 = %+v", rows)
		}
		// 兜底轮询是 10s，所以 2s 内收到就说明是推送（-race 下留足余量防 flake）。
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("推送延迟 %v，看起来在走兜底轮询", d)
		}
	case <-ctx.Done():
		t.Fatal("本地写没有触发推送")
	}

	// 事务内的写也要推送
	if err := db.WriteTransaction(ctx, func(ctx context.Context, tx *powersync.Tx) error {
		if _, err := tx.Execute(ctx, `INSERT INTO todos(id,title) VALUES('b','y')`); err != nil {
			return err
		}
		_, err := tx.Execute(ctx, `INSERT INTO todos(id,title) VALUES('c','z')`)
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction: %v", err)
	}
	select {
	case rows := <-ch:
		if rows[0]["n"] != int64(3) {
			t.Errorf("事务后推送值 = %+v", rows)
		}
	case <-ctx.Done():
		t.Fatal("WriteTransaction 没有触发推送")
	}
}

// TestWatchPushesOnSyncApply 验证下行同步（/sync/stream 应用 checkpoint）也会推送。
func TestWatchPushesOnSyncApply(t *testing.T) {
	srv := newFakeServer(t, "apply-todos.ndjson", "up-to-date.ndjson")
	path := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:       path,
		Schema:         powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}, {Name: "done", Type: powersync.IntegerColumn}}}),
		Connector:      &fakeConnector{endpoint: srv.URL},
		WatchInterval:  10 * time.Second, // 只靠推送
		RetryDelay:     50 * time.Millisecond,
		UploadInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, path)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ch, err := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := <-ch; got[0]["n"] != int64(0) {
		t.Fatalf("初值 = %+v", got)
	}
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case rows := <-ch:
		if rows[0]["n"] != int64(2) {
			t.Errorf("同步后推送值 = %+v", rows)
		}
	case <-ctx.Done():
		t.Fatal("同步应用没有触发推送")
	}
}

// TestStreamsAlwaysSendsSubscriptions 验证即使没有显式订阅，请求体里
// streams.subscriptions 也必须存在（服务端要求该属性，缺了会 400）。
func TestStreamsAlwaysSendsSubscriptions(t *testing.T) {
	srv, bodies := streamsServerFor(t)
	path := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename:  path,
		Schema:    powersync.NewSchema(powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}}),
		Connector: &fakeConnector{endpoint: srv},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, path)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	var body string
	select {
	case body = <-bodies:
	case <-time.After(3 * time.Second):
		t.Fatal("没捕获到请求")
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatal(err)
	}
	streams, ok := raw["streams"].(map[string]any)
	if !ok {
		t.Fatalf("请求体应有 streams: %s", body)
	}
	subs, exists := streams["subscriptions"]
	if !exists {
		t.Fatalf("streams.subscriptions 属性必须存在（否则服务端 400）: %s", body)
	}
	if arr, ok := subs.([]any); !ok || len(arr) != 0 {
		t.Errorf("subscriptions = %#v, want []", subs)
	}
}

// TestWatchDoesNotResendUnchangedResults 验证"数据版本变了但查询结果没变"时不再推送
// （Watch 用结果集哈希去重，避免无关写入把 UI 刷起来）。
func TestWatchDoesNotResendUnchangedResults(t *testing.T) {
	path := tempDBPath(t)
	db, err := powersync.Open(context.Background(), powersync.Options{
		Filename: path,
		Schema: powersync.NewSchema(
			powersync.Table{Name: "todos", Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}}},
			powersync.Table{Name: "logs", Columns: []powersync.Column{{Name: "msg", Type: powersync.TextColumn}}},
		),
		WatchInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanupDBPath(t, db, path)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	ch, err := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := <-ch; got[0]["n"] != int64(0) {
		t.Fatalf("初值 = %+v", got)
	}

	// 写另一张表：版本号会变（触发 Watch 重查），但 count(*) 没变 → 不应推送
	if _, err := db.Execute(ctx, `INSERT INTO logs(id, msg) VALUES('l1','noise')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case rows := <-ch:
		t.Fatalf("结果未变化却推送了: %+v", rows)
	case <-time.After(300 * time.Millisecond):
		// 期望：静默
	}

	// 真正变化时才推送
	if _, err := db.Execute(ctx, `INSERT INTO todos(id,title) VALUES('a','x')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case rows := <-ch:
		if rows[0]["n"] != int64(1) {
			t.Errorf("推送值 = %+v", rows)
		}
	case <-ctx.Done():
		t.Fatal("真正变化没有推送")
	}
}
