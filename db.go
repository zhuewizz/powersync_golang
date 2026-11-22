package powersync

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/zhuewizz/powersync_golang/internal/core"
	"github.com/zhuewizz/powersync_golang/internal/netmon"
	syncpkg "github.com/zhuewizz/powersync_golang/internal/sync"
)

// Options 是 Open 的参数。
type Options struct {
	// Filename 是 SQLite 文件路径（必填）。已存在的库可以不给 Schema。
	Filename string
	// Schema 定义用户表；新库必填。
	Schema *Schema
	// Connector 提供凭据并实现上传。nil 表示只下载、不上传。
	Connector Connector
	// Logger 缺省用 slog.Default()。
	Logger *slog.Logger
	// HTTPClient 缺省用 http.DefaultClient。
	HTTPClient *http.Client
	// RetryDelay 是重连初始退避（缺省 1s，指数退避上限 30s）。
	RetryDelay time.Duration
	// UploadInterval 是上传队列轮询间隔（缺省 1s）。
	UploadInterval time.Duration
	// WatchInterval 是 Watch 的**兜底轮询**间隔（缺省 2s）；正常变化由触发器推送。
	WatchInterval time.Duration
	// ClientID 为空时自动生成并持久化到 ps_kv。
	ClientID string
	// NoFileLock 为 true 时不做跨进程文件锁（默认禁止同一 DB 被两个实例同时打开）。
	NoFileLock bool
	// LockTimeout 在锁被占用时重试等待（0 = 立即报 ErrDatabaseLocked）。
	LockTimeout time.Duration
	// DisableBSON 为 true 时只请求 NDJSON；默认请求 BSON 同步流
	// （体积约小 8%，服务端不支持时自动回落，落库结果与 NDJSON 逐行等价）。
	DisableBSON bool
	// DisableDefaultStreams 为 true 时不请求默认流（只有显式订阅的流会同步）。
	DisableDefaultStreams bool
	// AdditionalHeaders 附加到同步请求（例如自建网关需要的自定义鉴权头）。
	AdditionalHeaders map[string]string

	// NetworkTarget 是网络探测目标（host:port 或 URL）。为空时用同步用的服务端地址。
	NetworkTarget string
	// NetworkCheckInterval / NetworkCheckTimeout 是探测间隔与单次超时（默认 5s / 2s）。
	NetworkCheckInterval time.Duration
	NetworkCheckTimeout  time.Duration
	// DisableNetworkMonitor 为 true 时不启动网络监视器。
	// Debugf 接管底层调试日志（默认 nil=丢弃）；例如打印"物化时删除了哪一行"。
	Debugf                func(format string, args ...any)
	DisableNetworkMonitor bool
}

// DB 是一个 PowerSync 本地库 + 同步连接。
type DB struct {
	store         *core.DB
	syncer        *syncpkg.Syncer
	conn          Connector
	logger        *slog.Logger
	schema        *Schema
	watchInterval time.Duration
	netmon        *netmon.Monitor
	netTarget     string

	mu     sync.Mutex
	closed bool
}

// Open 打开（必要时创建）本地库并应用 schema。
func Open(ctx context.Context, opts Options) (*DB, error) {
	if opts.Filename == "" {
		return nil, ErrInvalidDirectory
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.WatchInterval <= 0 {
		opts.WatchInterval = 2 * time.Second
	}
	// 与官方 SDK 一致：实例化时必须提供客户端 schema。
	// 它是"本地有哪些表、每张表有哪些列"的唯一来源；服务端下发但 schema 未声明的表不会物化。
	if opts.Schema == nil {
		return nil, ErrSchemaRequired
	}
	cs, err := opts.Schema.toCore()
	if err != nil {
		return nil, err
	}
	store, err := core.Open(ctx, core.Options{
		Path:        opts.Filename,
		Schema:      cs,
		ClientID:    opts.ClientID,
		NoFileLock:  opts.NoFileLock,
		LockTimeout: opts.LockTimeout,
		Debugf:      opts.Debugf,
	})
	if err != nil {
		return nil, err
	}

	db := &DB{
		store:         store,
		conn:          opts.Connector,
		logger:        opts.Logger,
		schema:        opts.Schema,
		watchInterval: opts.WatchInterval,
	}

	// Connector 只在 Connect 时才必需：Open 一个纯本地库（或用 GetCrudBatch 自己上传）不需要它。
	creds := adaptConnector(opts.Connector)
	syncer, err := syncpkg.New(syncpkg.Options{
		Store:                 store,
		Creds:                 creds,
		Uploader:              uploaderFunc(func(ctx context.Context) error { return db.upload(ctx) }),
		Logger:                opts.Logger,
		HTTPClient:            opts.HTTPClient,
		RetryDelay:            opts.RetryDelay,
		UploadInterval:        opts.UploadInterval,
		DisableBSON:           opts.DisableBSON,
		DisableDefaultStreams: opts.DisableDefaultStreams,
		AdditionalHeaders:     opts.AdditionalHeaders,
		OnNetworkResult:       func(ok bool) { db.networkSignal() },
	})
	if err != nil {
		store.Close()
		return nil, err
	}
	db.syncer = syncer

	if !opts.DisableNetworkMonitor {
		db.netTarget = opts.NetworkTarget
		db.netmon = netmon.New(netmon.Options{
			Target:   db.networkProbeTarget,
			Interval: opts.NetworkCheckInterval,
			Timeout:  opts.NetworkCheckTimeout,
			OnChange: func(netmon.State) { db.store.NotifyDataChange() },
		})
	}
	return db, nil
}

// Connect 启动后台同步（下载 + 上传），不阻塞。
//
// ctx 只用于启动期的调用（连接建立后由 Disconnect/Close 停止），
// 传短生命周期的 ctx（如 HTTP 请求）不会终止同步。
func (db *DB) Connect(ctx context.Context) error {
	if err := db.check(); err != nil {
		return err
	}
	if db.conn == nil {
		return ErrNotConnected
	}
	runCtx := context.WithoutCancel(ctx)
	db.syncer.Start(runCtx)
	if db.netmon != nil {
		db.netmon.Start(runCtx)
	}
	db.syncer.TriggerUpload()
	return nil
}

// Disconnect 停止同步（本地数据保留）。
func (db *DB) Disconnect() {
	if db.syncer != nil {
		db.syncer.Stop()
	}
	if db.netmon != nil {
		db.netmon.Stop()
	}
}

// networkProbeTarget 返回网络探测目标：优先 Options.NetworkTarget，否则用同步服务地址。
func (db *DB) networkProbeTarget() string {
	if db.netTarget != "" {
		return db.netTarget
	}
	if db.syncer != nil {
		return db.syncer.Endpoint()
	}
	return ""
}

// networkSignal 让网络监视器立刻重测（同步连上/连不上时调用）。
func (db *DB) networkSignal() {
	if db.netmon != nil {
		db.netmon.Signal()
	}
}

// NetworkStatus 是网络可达性快照（与同步状态无关：只表示能不能连到服务端）。
type NetworkStatus struct {
	// Online 表示最近一次探测能否建立 TCP 连接。
	Online bool
	// CheckedAt 是最近一次探测时间。
	CheckedAt time.Time
	// Latency 是拨号耗时（离线时为 0）。
	Latency time.Duration
	// Target 是实际探测的地址。
	Target string
	// Err 是失败原因（在线时为空）。
	Err string
}

// CurrentNetworkStatus 返回网络状态快照。
func (db *DB) CurrentNetworkStatus() NetworkStatus {
	if db.netmon == nil {
		return NetworkStatus{}
	}
	st := db.netmon.State()
	return NetworkStatus{
		Online: st.Online, CheckedAt: st.CheckedAt, Latency: st.Latency,
		Target: st.Target, Err: st.Err,
	}
}

// WatchNetworkStatus 返回网络状态变化流（初值 + 每次可达性变化时推送）。
//
// 网络监视器在 Connect 时启动；未 Connect 时只有初值。
func (db *DB) WatchNetworkStatus(ctx context.Context) (<-chan NetworkStatus, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	ch := make(chan NetworkStatus, 1)
	go func() {
		defer close(ch)
		var last NetworkStatus
		first := true
		emit := func() bool {
			cur := db.CurrentNetworkStatus()
			if !first && cur.Online == last.Online && cur.Target == last.Target {
				return true
			}
			first = false
			last = cur
			select {
			case ch <- cur:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !emit() {
			return
		}
		for {
			var signal <-chan struct{}
			if db.netmon != nil {
				signal = db.netmon.Changes()
			}
			select {
			case <-ctx.Done():
				return
			case <-signal:
			case <-time.After(5 * time.Second): // 兜底
			}
			if !emit() {
				return
			}
		}
	}()
	return ch, nil
}

// Close 停止同步并关闭数据库。
func (db *DB) Close(ctx context.Context) error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	db.Disconnect()
	return db.store.Close()
}

// UpdateSchema 用新 schema 更新本地库（保留数据）。
//
// 内部走 core.ReplaceSchema：经 ps_untyped 暂存 → 重建对象 → 回填。
// JSON 表加列无需迁移数据；删除的列/表会丢数据。
func (db *DB) UpdateSchema(ctx context.Context, schema *Schema) error {
	if err := db.check(); err != nil {
		return err
	}
	if schema == nil {
		return ErrSchemaRequired
	}
	cs, err := schema.toCore()
	if err != nil {
		return err
	}
	if err := db.store.ReplaceSchema(ctx, cs); err != nil {
		return err
	}
	db.mu.Lock()
	db.schema = schema
	db.mu.Unlock()
	return nil
}

// ClientID 返回本地 client id。
func (db *DB) ClientID(ctx context.Context) (string, error) {
	if err := db.check(); err != nil {
		return "", err
	}
	return db.store.ClientID(ctx)
}

// Schema 返回打开时使用的 schema（可能为 nil）。
func (db *DB) Schema() *Schema { return db.schema }

// CurrentStatus 返回同步状态快照。
func (db *DB) CurrentStatus() SyncStatus {
	st := db.syncer.Status()
	return SyncStatus{
		Connected:     st.Connected,
		Connecting:    st.Connecting,
		Uploading:     st.Uploading,
		LastSyncedAt:  st.LastSyncedAt,
		Buckets:       st.Buckets,
		Cursor:        st.Cursor,
		DownloadError: st.DownloadErr,
		UploadError:   st.UploadErr,
	}
}

// WaitForFirstSync 等待首次同步完成（或 ctx 结束）。
func (db *DB) WaitForFirstSync(ctx context.Context) error {
	if err := db.check(); err != nil {
		return err
	}
	select {
	case <-db.syncer.FirstSync():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SkippedUnknownOps 返回因"表不在 schema"而被跳过物化的行数。
//
// 典型原因：连真实服务时忘了用 -schema 传入真实 schema（只建了示例表），
// 或 sync rules 里有 schema 未声明的表（如测试表）。跳过不影响同步/校验，但这些表本地没有数据。
func (db *DB) SkippedUnknownOps() int64 {
	if db.store == nil {
		return 0
	}
	return db.store.SkippedUnknown()
}

// SkippedTables 返回因不在 schema 中被跳过的表名（用于诊断"为什么本地没有某张表"）。
func (db *DB) SkippedTables() []string {
	if db.store == nil {
		return nil
	}
	return db.store.SkippedTables()
}

// TriggerUpload 请求尽快跑一次上传（本地写后 SDK 会自动触发，通常无需手动调用）。
func (db *DB) TriggerUpload() { db.syncer.TriggerUpload() }

// WatchStatus 返回同步状态变化流（初值立即可读，之后每次状态变化推送一次）。
func (db *DB) WatchStatus(ctx context.Context) (<-chan SyncStatus, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	ch := make(chan SyncStatus, 1)
	go func() {
		defer close(ch)
		var last SyncStatus
		first := true
		emit := func() bool {
			cur := db.CurrentStatus()
			if !first && cur == last {
				return true
			}
			first = false
			last = cur
			select {
			case ch <- cur:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !emit() {
			return
		}
		for {
			signal := db.syncer.StatusSignal()
			if !emit() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-signal:
			case <-time.After(2 * time.Second): // 兜底：捕捉漏掉的通知
			}
		}
	}()
	return ch, nil
}

// WaitForStatus 等待同步状态满足条件（例如 connected 或 lastSyncedAt 超过某时间）。
func (db *DB) WaitForStatus(ctx context.Context, pred func(SyncStatus) bool) error {
	ch, err := db.WatchStatus(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case st, ok := <-ch:
			if !ok {
				return ErrClosed
			}
			if pred(st) {
				return nil
			}
		}
	}
}

// DisconnectAndClear 断开同步并清空本地数据（默认保留 local_only 表，类似官方 disconnectAndClear）。
//
// 典型用法：切换账号/退出登录。schema、client_id 保留。
func (db *DB) DisconnectAndClear(ctx context.Context, opts ...ClearOption) error {
	if err := db.check(); err != nil {
		return err
	}
	var cfg clearConfig
	for _, o := range opts {
		o(&cfg)
	}
	db.Disconnect()
	if err := db.store.ClearAll(ctx, cfg.clearLocal); err != nil {
		return err
	}
	db.store.NotifyDataChange()
	return nil
}

// ClearOption 配置 DisconnectAndClear 的行为。
type ClearOption func(*clearConfig)

type clearConfig struct{ clearLocal bool }

// WithClearLocalTables 让 DisconnectAndClear 连 local_only 表一起清空（默认保留）。
func WithClearLocalTables() ClearOption {
	return func(c *clearConfig) { c.clearLocal = true }
}

// UploadQueue 返回待上传变更条数。
func (db *DB) UploadQueue(ctx context.Context) (int64, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	return db.store.CrudPending(ctx)
}

func (db *DB) upload(ctx context.Context) error {
	if db.conn == nil {
		return ErrNotConnected
	}
	return db.conn.UploadData(ctx, db)
}

func (db *DB) check() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return nil
}

// SyncStatus 是同步状态快照。
type SyncStatus struct {
	// Connected 表示当前有一条活跃的同步流。
	Connected bool
	// Connecting 表示正在建立连接。
	Connecting bool
	// Uploading 表示正在上传。
	Uploading bool
	// LastSyncedAt 是最近一次 checkpoint 应用成功的时间。
	LastSyncedAt time.Time
	// Buckets 是本地已知的 bucket 数。
	Buckets int
	// Cursor 是最近一次成功应用的服务端 last_op_id。
	Cursor string
	// DownloadError / UploadError 是最近一次错误（成功后清空）。
	DownloadError string
	UploadError   string
}

// ---------------------------------------------------------------- 适配层

type uploaderFunc func(ctx context.Context) error

func (f uploaderFunc) Upload(ctx context.Context) error { return f(ctx) }

func adaptConnector(c Connector) syncpkg.CredentialsProvider {
	if c == nil {
		return nilCredentials{}
	}
	return connectorCreds{c}
}

// nilCredentials 用于没有 Connector 的本地库：真正 Connect 时会报 ErrNotConnected。
type nilCredentials struct{}

func (nilCredentials) FetchCredentials(context.Context) (syncpkg.Credentials, error) {
	return syncpkg.Credentials{}, ErrNotConnected
}

func (nilCredentials) InvalidateCredentials() {}

type connectorCreds struct{ c Connector }

func (a connectorCreds) FetchCredentials(ctx context.Context) (syncpkg.Credentials, error) {
	creds, err := a.c.FetchCredentials(ctx)
	if err != nil {
		return syncpkg.Credentials{}, err
	}
	return syncpkg.Credentials{Endpoint: creds.Endpoint, Token: creds.Token}, nil
}

func (a connectorCreds) InvalidateCredentials() {
	if inv, ok := a.c.(CredentialInvalidator); ok {
		inv.InvalidateCredentials()
	}
}
