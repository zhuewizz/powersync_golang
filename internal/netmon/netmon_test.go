package netmon_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"msmkSync/internal/netmon"
)

func waitForState(t *testing.T, m *netmon.Monitor, d time.Duration, cond func(netmon.State) bool) netmon.State {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		st := m.State()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("条件未满足，最后状态 = %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMonitorTransitions 覆盖在线→离线（Signal 触发立即重测）。
func TestMonitorTransitions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.Listener.Addr().String()

	m := netmon.New(netmon.Options{
		Target:   func() string { return addr },
		Interval: time.Hour, // 只靠 Signal 触发
		Timeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	defer m.Stop()

	st := waitForState(t, m, 2*time.Second, func(s netmon.State) bool { return s.Online })
	if st.Target != addr || st.Latency <= 0 || st.Err != "" {
		t.Errorf("在线状态 = %+v", st)
	}

	// 关掉服务端 → Signal 后应立即变离线
	srv.Close()
	m.Signal()
	st = waitForState(t, m, 3*time.Second, func(s netmon.State) bool { return !s.Online })
	if st.Err == "" || st.CheckedAt.IsZero() {
		t.Errorf("离线状态 = %+v", st)
	}
}

// TestChangesBroadcast 验证 Changes() 只在可达性/目标变化时被关闭。
func TestChangesBroadcast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	m := netmon.New(netmon.Options{
		Target:   func() string { return srv.Listener.Addr().String() },
		Interval: 20 * time.Millisecond,
		Timeout:  time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	defer m.Stop()

	waitForState(t, m, 2*time.Second, func(s netmon.State) bool { return s.Online })
	// 已经在线：后续探测不应产生新通知
	ch := m.Changes()
	select {
	case <-ch:
		t.Fatal("状态没变化却通知了")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestTargetUnknown 验证拿不到目标时的状态与不 panic。
func TestTargetUnknown(t *testing.T) {
	m := netmon.New(netmon.Options{Target: func() string { return "" }, Interval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	m.Stop()
	if st := m.State(); st.Target != "" || st.Err == "" {
		t.Errorf("未知目标状态 = %+v", st)
	}
}

func TestNormalizeTarget(t *testing.T) {
	cases := map[string]string{
		"https://sync.example.com": "sync.example.com:443",
		"http://127.0.0.1:8080":    "127.0.0.1:8080",
		"http://127.0.0.1":         "127.0.0.1:80",
		"sync.example.com:9000":    "sync.example.com:9000",
		"sync.example.com":         "sync.example.com:443",
		"  ":                       "",
	}
	for in, want := range cases {
		if got := netmon.NormalizeTarget(in); got != want {
			t.Errorf("NormalizeTarget(%q) = %q, want %q", in, got, want)
		}
	}
}
