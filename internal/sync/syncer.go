// Package sync 实现同步循环：下载（/sync/stream）+ 上传（ps_crud 队列 → 使用方 connector）。
//
// 它只依赖 internal/protocol 与一个最小的 Store 接口（internal/core 满足），
// 公开 API 层（仓库根包 powersync）负责把 Connector 适配进来。
package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"msmkSync/internal/bson"
	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// Credentials 是一次同步使用的服务端地址与 JWT。
type Credentials struct {
	Endpoint string
	Token    string
}

// CredentialsProvider 负责取（并缓存）凭据。InvalidateCredentials 表示现有凭据已失效。
type CredentialsProvider interface {
	FetchCredentials(ctx context.Context) (Credentials, error)
	InvalidateCredentials()
}

// Uploader 把本地 ps_crud 队列推到后端（通常由使用方的 Connector.UploadData 实现）。
type Uploader interface {
	Upload(ctx context.Context) error
}

// Store 是同步循环需要的本地库能力（*core.DB 满足）。
type Store interface {
	StageOps(ctx context.Context, bucket string, ops []protocol.Op) error
	ApplyCheckpoint(ctx context.Context, lastOp protocol.OpID, targets []core.BucketTarget, opts core.ApplyOptions) ([]core.ChecksumMismatch, error)
	Buckets(ctx context.Context) (map[string]protocol.OpID, error)
	CrudPending(ctx context.Context) (int64, error)
	// TargetCheckpoint 是本地写入要求的 write checkpoint（0 表示没有要求）。
	TargetCheckpoint(ctx context.Context) (protocol.OpID, error)
	// ClearTargetCheckpoint 在回显到达后清除该要求。
	ClearTargetCheckpoint(ctx context.Context) error
	// BucketChecksum 返回本地累计校验和（调试日志用）。
	BucketChecksum(ctx context.Context, bucket string) (uint32, error)
	// ActiveStreams 返回本地 active 的流订阅（拼 streams 请求体）。
	ActiveStreams(ctx context.Context) ([]core.StreamSubscription, error)
	// UpdateStreamSyncState 用 checkpoint 的 streams 信息回填订阅状态。
	UpdateStreamSyncState(ctx context.Context, states []protocol.StreamState, syncedAt time.Time) error
	// PurgeExpiredStreams 删除已过期的非 active 订阅。
	PurgeExpiredStreams(ctx context.Context, now time.Time) (int64, error)
}

// Status 是同步状态的快照。
type Status struct {
	Connected    bool
	Connecting   bool
	Uploading    bool
	LastSyncedAt time.Time
	Buckets      int
	Cursor       string
	DownloadErr  string
	UploadErr    string
}

// Options 是 New 的参数。
type Options struct {
	Store    Store
	Creds    CredentialsProvider
	Uploader Uploader
	Logger   *slog.Logger
	// HTTPClient 缺省用 http.DefaultClient。
	HTTPClient *http.Client
	UserAgent  string
	// RetryDelay 是重连初始退避（缺省 1s，指数增长到 30s）。
	RetryDelay time.Duration
	// UploadInterval 是上传队列的轮询间隔（缺省 1s）。
	UploadInterval time.Duration
	// MaxLineSize 是单条 NDJSON 的上限（缺省 protocol.DefaultMaxLineSize）。
	MaxLineSize int
	// OnCheckpoint 在每次 checkpoint 应用成功后回调（可为 nil）。
	OnCheckpoint func(cursor protocol.OpID, buckets int)
	// DisableBSON 为 true 时只请求 NDJSON；默认请求 BSON 流
	// （application/vnd.powersync.bson-stream），服务端不支持会按 Content-Type 自动回落。
	DisableBSON bool
	// DisableDefaultStreams 为 true 时不请求默认流（streams.include_defaults=false）。
	DisableDefaultStreams bool
	// AdditionalHeaders 附加到同步请求上（如自定义网关的鉴权头）。
	AdditionalHeaders map[string]string
}

// Syncer 驱动下载与上传两个循环，可重复 Start/Stop。
type Syncer struct {
	opts      Options
	log       *slog.Logger
	uploadCh  chan struct{}
	first     chan struct{}
	firstOne  sync.Once
	statusSig broadcast

	mu     sync.Mutex
	status Status
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 创建 Syncer（不启动）。
func New(opts Options) (*Syncer, error) {
	if opts.Store == nil {
		return nil, errors.New("sync: 需要 Store")
	}
	if opts.Creds == nil {
		return nil, errors.New("sync: 需要 CredentialsProvider")
	}
	if opts.Uploader == nil {
		return nil, errors.New("sync: 需要 Uploader")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "powersync-go/0.0.0"
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = time.Second
	}
	if opts.UploadInterval <= 0 {
		opts.UploadInterval = time.Second
	}
	if opts.MaxLineSize <= 0 {
		opts.MaxLineSize = protocol.DefaultMaxLineSize
	}
	return &Syncer{
		opts:      opts,
		log:       opts.Logger,
		uploadCh:  make(chan struct{}, 1),
		first:     make(chan struct{}),
		statusSig: newBroadcast(),
	}, nil
}

// Start 在后台启动同步循环（非阻塞）；重复调用无副作用。
func (s *Syncer) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.downloadLoop(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.uploadLoop(ctx)
	}()
}

// Stop 停止同步循环并等待退出。
func (s *Syncer) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	s.wg.Wait()
	s.set(func(st *Status) {
		st.Connected = false
		st.Connecting = false
		st.Uploading = false
	})
}

// TriggerUpload 请求尽快跑一次上传（本地写之后调用）。
func (s *Syncer) TriggerUpload() {
	select {
	case s.uploadCh <- struct{}{}:
	default:
	}
}

// FirstSync 返回一个在首次 checkpoint 应用成功后关闭的 channel。
func (s *Syncer) FirstSync() <-chan struct{} { return s.first }

// Status 返回当前同步状态快照。
func (s *Syncer) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Syncer) set(fn func(*Status)) {
	s.mu.Lock()
	fn(&s.status)
	s.mu.Unlock()
	s.statusSig.notify()
}

// StatusSignal 返回一个在同步状态变化时被关闭的 channel（供 WatchStatus 使用）。
func (s *Syncer) StatusSignal() <-chan struct{} { return s.statusSig.wait() }

// ---------------------------------------------------------------- 下载循环

func (s *Syncer) downloadLoop(ctx context.Context) {
	delay := s.opts.RetryDelay
	for ctx.Err() == nil {
		err := s.streamIteration(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.log.Warn("sync iteration failed", "err", err)
			s.set(func(st *Status) {
				st.Connected = false
				st.DownloadErr = err.Error()
			})
			if !sleepCtx(ctx, delay) {
				return
			}
			if delay *= 2; delay > 30*time.Second {
				delay = 30 * time.Second
			}
			continue
		}
		delay = s.opts.RetryDelay
	}
}

// streamIteration 跑一条 /sync/stream 连接，直到服务端关闭或本地出错。
func (s *Syncer) streamIteration(ctx context.Context) error {
	creds, err := s.opts.Creds.FetchCredentials(ctx)
	if err != nil {
		return fmt.Errorf("取凭据: %w", err)
	}
	endpoint := strings.TrimRight(creds.Endpoint, "/")
	if endpoint == "" {
		return errors.New("凭据缺少 endpoint")
	}

	buckets, err := s.opts.Store.Buckets(ctx)
	if err != nil {
		return fmt.Errorf("读取本地 bucket: %w", err)
	}
	reqBuckets := make([]protocol.BucketRequest, 0, len(buckets))
	for name, after := range buckets {
		reqBuckets = append(reqBuckets, protocol.BucketRequest{Name: name, After: after})
	}
	sort.Slice(reqBuckets, func(i, j int) bool { return reqBuckets[i].Name < reqBuckets[j].Name })

	streamReq := protocol.StreamRequest{
		Buckets:         reqBuckets,
		IncludeChecksum: true,
		RawData:         true,
		AppMetadata:     map[string]string{},
		Parameters:      map[string]any{},
	}
	activeStreams, err := s.opts.Store.ActiveStreams(ctx)
	if err != nil {
		return fmt.Errorf("读取流订阅: %w", err)
	}
	if !s.opts.DisableDefaultStreams || len(activeStreams) > 0 {
		subs := &protocol.StreamSubscribes{
			IncludeDefaults: !s.opts.DisableDefaultStreams,
			Subscriptions:   []protocol.StreamSubscription{},
		}
		for _, sub := range activeStreams {
			params := sub.Parameters
			if params == nil {
				params = map[string]any{}
			}
			subs.Subscriptions = append(subs.Subscriptions, protocol.StreamSubscription{
				Stream:           sub.Name,
				Parameters:       params,
				OverridePriority: sub.Priority,
			})
		}
		streamReq.Streams = subs
	}
	body, err := json.Marshal(streamReq)
	if err != nil {
		return err
	}

	s.set(func(st *Status) { st.Connecting = true })
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/sync/stream", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+creds.Token)
	req.Header.Set("Content-Type", "application/json")
	if s.opts.DisableBSON {
		req.Header.Set("Accept", "application/x-ndjson")
	} else {
		req.Header.Set("Accept", "application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8")
	}
	req.Header.Set("X-User-Agent", s.opts.UserAgent)
	for k, v := range s.opts.AdditionalHeaders {
		req.Header.Set(k, v)
	}

	resp, err := s.opts.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("连接同步流: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		if e := protocol.DecodeErrorResponse(msg); e != nil {
			if e.Unauthorized() {
				s.opts.Creds.InvalidateCredentials()
			}
			detail := ""
			if e.Details != "" {
				detail = " (" + e.Details + ")"
			}
			return fmt.Errorf("同步流 HTTP %d: %s %s%s", resp.StatusCode, e.Code, e.Description, detail)
		}
		if resp.StatusCode == http.StatusUnauthorized {
			s.opts.Creds.InvalidateCredentials()
		}
		return fmt.Errorf("同步流 HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	s.set(func(st *Status) {
		st.Connecting = false
		st.Connected = true
		st.DownloadErr = ""
	})
	defer s.set(func(st *Status) { st.Connected = false })

	lines, err := newLineSource(resp.Header.Get("Content-Type"), resp.Body, s.opts.MaxLineSize)
	if err != nil {
		return err
	}
	var targets []core.BucketTarget
	// 同一连接内，服务端只在**流开始时**发一次 checkpoint 行；之后的新增批次只有
	// data + checkpoint_complete。此时 checkpoint 里的 checksum 还是旧目标，无法用于校验，
	// 只能延后到下一次 checkpoint（通常是重连后的首个 checkpoint）再验。
	haveCheckpoint := false
	seenBuckets := map[string]struct{}{}
	var checkpointStreams []protocol.StreamState
	for {
		line, err := lines.Next()
		if errors.Is(err, io.EOF) {
			return nil // 服务端正常关闭，外层重连
		}
		if err != nil {
			return fmt.Errorf("解析同步流: %w", err)
		}
		switch l := line.(type) {
		case *protocol.CheckpointLine:
			haveCheckpoint = true
			checkpointStreams = l.Streams
			s.log.Debug("checkpoint 行", "last_op_id", l.LastOpID.String(),
				"buckets", len(l.Buckets), "streams", len(l.Streams),
				"write_checkpoint", l.WriteCheckpoint)
			// legacy 模式：checkpoint 里带 write_checkpoint，若还没覆盖本地写入就跳过本批，
			// 保留已 staged 的 oplog，重连后由更新的一批数据一起物化。
			if l.WriteCheckpoint != "" {
				if stale, err := s.staleCheckpoint(ctx, l.WriteCheckpoint); err != nil {
					return err
				} else if stale {
					s.log.Info("checkpoint 未包含本地写入（等待回显），重连",
						"write_checkpoint", l.WriteCheckpoint)
					return nil
				}
			}
			targets = make([]core.BucketTarget, 0, len(l.Buckets))
			for _, b := range l.Buckets {
				if local, err := s.opts.Store.BucketChecksum(ctx, b.Bucket); err == nil {
					s.log.Debug("checkpoint 开始",
						"bucket", b.Bucket, "expected", b.Checksum32(), "local_before", local,
						"last_op_id", l.LastOpID.String())
				}
				targets = append(targets, core.BucketTarget{
					Name: b.Bucket, Checksum: b.Checksum32(), Count: b.Count, Priority: b.Priority,
				})
			}
		case *protocol.DataLine:
			seenBuckets[l.Bucket] = struct{}{}
			if len(l.Ops) > 0 {
				s.log.Debug("data 行", "bucket", l.Bucket, "after", l.After.String(),
					"has_more", l.HasMore, "ops", len(l.Ops),
					"last_op", l.Ops[len(l.Ops)-1].OpID.String(),
					"last_type", string(l.Ops[len(l.Ops)-1].Op))
			}
			if err := s.opts.Store.StageOps(ctx, l.Bucket, l.Ops); err != nil {
				return fmt.Errorf("写入 oplog（%s）: %w", l.Bucket, err)
			}
		case *protocol.CheckpointCompleteLine:
			if l.CheckpointRequestID != nil {
				stale, err := s.staleCheckpoint(ctx, *l.CheckpointRequestID)
				if err != nil {
					return err
				}
				if stale {
					// 本地写入还没被服务端包含进来：跳过物化，保留 staged oplog，重连等新 checkpoint。
					s.log.Info("checkpoint 未包含本地写入（等待回显），重连",
						"checkpoint_request_id", *l.CheckpointRequestID)
					return nil
				}
				if err := s.opts.Store.ClearTargetCheckpoint(ctx); err != nil {
					s.log.Warn("清除 target checkpoint 失败", "err", err)
				}
			}
			if !haveCheckpoint || len(targets) == 0 {
				targets = make([]core.BucketTarget, 0, len(seenBuckets))
				for name := range seenBuckets {
					targets = append(targets, core.BucketTarget{Name: name})
				}
			}
			validate := haveCheckpoint
			for _, t := range targets {
				if local, err := s.opts.Store.BucketChecksum(ctx, t.Name); err == nil {
					s.log.Debug("checkpoint 结束", "bucket", t.Name, "expected", t.Checksum,
						"local_after", local, "validated", validate)
				}
			}
			bad, err := s.opts.Store.ApplyCheckpoint(ctx, l.LastOpID, targets,
				core.ApplyOptions{Validate: validate, ResetOnMismatch: true, Priority: 3})
			if err != nil {
				return fmt.Errorf("应用 checkpoint: %w", err)
			}
			if len(bad) > 0 {
				// 校验和不符：相应 bucket 已被重置，重连让它从 after=0 重下。
				reasons := make([]string, len(bad))
				for i, m := range bad {
					reasons[i] = m.String()
				}
				s.log.Warn("checksum mismatch, restarting iteration",
					"details", strings.Join(reasons, "; "))
				return nil
			}
			haveCheckpoint = false
			for name := range seenBuckets {
				delete(seenBuckets, name)
			}
			if len(checkpointStreams) > 0 {
				if err := s.opts.Store.UpdateStreamSyncState(ctx, checkpointStreams, time.Now().UTC()); err != nil {
					s.log.Warn("回填流订阅状态失败", "err", err)
				}
				checkpointStreams = nil
			}
			if _, err := s.opts.Store.PurgeExpiredStreams(ctx, time.Now().UTC()); err != nil {
				s.log.Debug("清理过期流订阅失败", "err", err)
			}
			s.markSynced(ctx, l.LastOpID)
			s.TriggerUpload()
		case *protocol.PartialCheckpointCompleteLine:
			// 按优先级分批应用：只物化该优先级及以上的 bucket。
			if len(targets) == 0 {
				continue
			}
			bad, err := s.opts.Store.ApplyCheckpoint(ctx, l.LastOpID, targets,
				core.ApplyOptions{Validate: haveCheckpoint, ResetOnMismatch: true,
					Priority: l.Priority, MinPriority: l.Priority})
			if err != nil {
				return fmt.Errorf("应用 partial checkpoint: %w", err)
			}
			if len(bad) > 0 {
				s.log.Warn("checksum mismatch (partial)", "priority", l.Priority)
				return nil
			}
			s.log.Debug("partial checkpoint 已应用", "priority", l.Priority, "last_op_id", l.LastOpID.String())
			s.markSynced(ctx, l.LastOpID)
		case *protocol.TokenExpiresInLine:
			// 提前刷新：<= 30s 时后台换新 token（不影响当前连接）。
			if l.Seconds <= 30 {
				go func() {
					if _, err := s.opts.Creds.FetchCredentials(ctx); err != nil {
						s.log.Debug("prefetch credentials failed", "err", err)
					}
				}()
			}
		default:
			s.log.Debug("unknown sync line", "kind", line.Kind())
		}
	}
}

// lineSource 抽象 NDJSON / BSON 两种流，下游只处理 protocol.Line。
type lineSource interface {
	Next() (protocol.Line, error)
}

// newLineSource 按响应 Content-Type 选择解码器（bson → BSON，其余 NDJSON）。
func newLineSource(contentType string, body io.Reader, maxLineSize int) (lineSource, error) {
	switch {
	case strings.Contains(contentType, "bson"):
		return &bsonSource{dec: bson.NewDecoder(body)}, nil
	case strings.Contains(contentType, "ndjson") || contentType == "":
		return &ndjsonSource{dec: protocol.NewDecoder(body, maxLineSize)}, nil
	default:
		return nil, fmt.Errorf("不支持的流类型 %q（只支持 NDJSON / BSON）", contentType)
	}
}

type ndjsonSource struct{ dec *protocol.Decoder }

func (s *ndjsonSource) Next() (protocol.Line, error) { return s.dec.Next() }

type bsonSource struct{ dec *bson.Decoder }

func (s *bsonSource) Next() (protocol.Line, error) {
	doc, err := s.dec.Next()
	if err != nil {
		return nil, err
	}
	return protocol.ParseLine(doc)
}

// staleCheckpoint 判断服务端给出的 checkpoint 序号是否还没覆盖本地已上传的写入。
func (s *Syncer) staleCheckpoint(ctx context.Context, serverID string) (bool, error) {
	target, err := s.opts.Store.TargetCheckpoint(ctx)
	if err != nil {
		return false, err
	}
	if target == 0 {
		return false, nil
	}
	var got protocol.OpID
	if err := got.UnmarshalJSON([]byte(strconv.Quote(serverID))); err != nil {
		// 服务端给了非数字（理论上不会）：不做门控，避免卡死。
		return false, nil
	}
	return got < target, nil
}

func (s *Syncer) markSynced(ctx context.Context, cursor protocol.OpID) {
	buckets := len(snapshotKeys(ctx, s.opts.Store)) // 只查一次
	s.set(func(st *Status) {
		st.LastSyncedAt = time.Now().UTC()
		st.Cursor = cursor.String()
		st.Buckets = buckets
		st.DownloadErr = ""
	})
	s.firstOne.Do(func() { close(s.first) })
	if s.opts.OnCheckpoint != nil {
		s.opts.OnCheckpoint(cursor, buckets)
	}
}

func snapshotKeys(ctx context.Context, store Store) map[string]protocol.OpID {
	b, err := store.Buckets(ctx)
	if err != nil {
		return nil
	}
	return b
}

// ---------------------------------------------------------------- 上传循环

func (s *Syncer) uploadLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.UploadInterval)
	defer ticker.Stop()

	delay := s.opts.RetryDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.uploadCh:
		case <-ticker.C:
		}

		n, err := s.opts.Store.CrudPending(ctx)
		if err != nil {
			s.log.Warn("读取上传队列失败", "err", err)
			continue
		}
		if n == 0 {
			continue
		}

		s.set(func(st *Status) { st.Uploading = true })
		err = s.opts.Uploader.Upload(ctx)
		s.set(func(st *Status) { st.Uploading = false })
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("上传失败，稍后重试", "err", err)
			s.set(func(st *Status) { st.UploadErr = err.Error() })
			if !sleepCtx(ctx, delay) {
				return
			}
			if delay *= 2; delay > 30*time.Second {
				delay = 30 * time.Second
			}
			continue
		}
		delay = s.opts.RetryDelay
		s.set(func(st *Status) { st.UploadErr = "" })
	}
}

// broadcast 是"关闭即广播"的 channel 容器（状态/版本变化通知都用它）。
type broadcast struct {
	mu sync.Mutex
	ch chan struct{}
}

func newBroadcast() broadcast { return broadcast{ch: make(chan struct{})} }

func (b *broadcast) notify() {
	b.mu.Lock()
	close(b.ch)
	b.ch = make(chan struct{})
	b.mu.Unlock()
}

func (b *broadcast) wait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
