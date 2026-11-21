// Package netmon 做"网络是否可达"的实时探测。
//
// 做法：定期对目标地址做一次 TCP 拨号（默认每 5s，超时 2s），用最短的代价判断
// 到服务端的链路是否通；同步循环在连接成功/失败时会 Signal() 触发立即重测，
// 所以网络切换通常能在亚秒级反映出来。
//
// 它不判断"服务端是否正常"（那看 SyncStatus），只判断"网络通不通"。
package netmon

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// State 是一次探测的结果。
type State struct {
	// Online 表示最近一次探测是否能建立 TCP 连接。
	Online bool
	// CheckedAt 是本次探测的时间。
	CheckedAt time.Time
	// Latency 是拨号耗时（Online=false 时为 0）。
	Latency time.Duration
	// Target 是实际探测的地址（host:port）。
	Target string
	// Err 是失败原因（Online=true 时为空）。
	Err string
}

// Options 是 New 的参数。
type Options struct {
	// Target 返回要探测的地址（可以是 host:port 或 http(s):// URL）。返回空串时跳过本轮。
	Target func() string
	// Interval 是探测间隔，默认 5s。
	Interval time.Duration
	// Timeout 是单次拨号超时，默认 2s。
	Timeout time.Duration
	// OnChange 在状态变化时回调（可选）。
	OnChange func(State)
}

// Monitor 是网络状态监视器，可重复 Start/Stop。
type Monitor struct {
	opts Options

	mu    sync.Mutex
	state State
	ch    chan struct{} // 状态变化广播（关闭即通知）

	sig    chan struct{} // 立即重测信号
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 创建监视器（不启动）。
func New(opts Options) *Monitor {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	return &Monitor{
		opts: opts,
		ch:   make(chan struct{}),
		sig:  make(chan struct{}, 1),
	}
}

// Start 启动后台探测（非阻塞）。
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.opts.Interval)
		defer ticker.Stop()
		m.check(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.check(ctx)
			case <-m.sig:
				m.check(ctx)
			}
		}
	}()
}

// Stop 停止探测。
func (m *Monitor) Stop() {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	m.wg.Wait()
}

// State 返回最近一次探测结果。
func (m *Monitor) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Changes 返回一个在状态变化时被关闭的 channel（WatchNetworkStatus 用）。
func (m *Monitor) Changes() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ch
}

// Signal 请求立即重测一次（同步循环连接成功/失败时调用）。
func (m *Monitor) Signal() {
	select {
	case m.sig <- struct{}{}:
	default:
	}
}

func (m *Monitor) check(ctx context.Context) {
	target := ""
	if m.opts.Target != nil {
		target = NormalizeTarget(m.opts.Target())
	}
	if target == "" {
		// 还不知道往哪探测：保持原状态（通常是初始的 Offline）。
		m.setState(State{CheckedAt: time.Now().UTC(), Err: "target unknown"})
		return
	}

	dialCtx, cancel := context.WithTimeout(ctx, m.opts.Timeout)
	defer cancel()
	start := time.Now()
	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", target)
	latency := time.Since(start)

	st := State{CheckedAt: time.Now().UTC(), Target: target}
	if err != nil {
		st.Online = false
		st.Err = err.Error()
	} else {
		st.Online = true
		st.Latency = latency
		_ = conn.Close()
	}
	m.setState(st)
}

// setState 只在"可达性/目标"变化时广播，避免每 5s 一次无意义通知。
func (m *Monitor) setState(next State) {
	m.mu.Lock()
	prev := m.state
	m.state = next
	changed := prev.Online != next.Online || prev.Target != next.Target
	if changed {
		close(m.ch)
		m.ch = make(chan struct{})
	}
	m.mu.Unlock()
	if changed && m.opts.OnChange != nil {
		m.opts.OnChange(next)
	}
}

// NormalizeTarget 把 URL 或裸地址统一成 host:port（TCP 拨号用）。
//
//	https://sync.example.com     → sync.example.com:443
//	http://127.0.0.1:8080        → 127.0.0.1:8080
//	sync.example.com:9000        → sync.example.com:9000
//	sync.example.com             → sync.example.com:443
func NormalizeTarget(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			return ""
		}
		if u.Port() != "" {
			return u.Host
		}
		if u.Scheme == "http" {
			return net.JoinHostPort(u.Hostname(), "80")
		}
		return net.JoinHostPort(u.Hostname(), "443")
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return raw
	}
	return net.JoinHostPort(raw, "443")
}
