package powersync

import (
	"context"
	"time"
)

// StreamSubscription 是一个已订阅的同步流（sync streams）。
//
// 订阅会持久化在本地 ps_stream_subscriptions（与官方 SDK 同一张表），
// 同步连接每次建流时都会带上所有 active 订阅。
type StreamSubscription struct {
	db *DB
	// ID 是本地 ps_stream_subscriptions.id。
	ID int64
	// Name 是服务端流名。
	Name string
	// Parameters 是订阅参数（会原样发给服务端）。
	Parameters map[string]any
}

// StreamSubscriptionState 是订阅的当前状态（含服务端回填的信息）。
type StreamSubscriptionState struct {
	Name         string
	Parameters   map[string]any
	Active       bool
	IsDefault    bool
	HasSynced    bool
	Priority     *int
	TTL          *time.Duration
	ExpiresAt    *time.Time
	LastSyncedAt *time.Time
}

// SubscribeOption 配置订阅参数。
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	ttl      *time.Duration
	priority *int
}

// WithTTL 设置订阅的存活时间：取消订阅后超过该时间会被清理（服务端也可能据此驱逐）。
func WithTTL(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) { c.ttl = &d }
}

// WithPriority 覆盖该流的优先级（数值越小越优先，0 为最高）。
func WithPriority(p int) SubscribeOption {
	return func(c *subscribeConfig) { c.priority = &p }
}

// SubscribeStream 订阅一个同步流并持久化。
//
// 订阅后下一次同步连接就会带上它（Connect 中会自动重连/重建流时生效）。
func (db *DB) SubscribeStream(ctx context.Context, name string, params map[string]any, opts ...SubscribeOption) (*StreamSubscription, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	var cfg subscribeConfig
	for _, o := range opts {
		o(&cfg)
	}
	sub, err := db.store.SubscribeStream(ctx, name, params, cfg.ttl, cfg.priority)
	if err != nil {
		return nil, err
	}
	return &StreamSubscription{db: db, ID: sub.ID, Name: sub.Name, Parameters: sub.Parameters}, nil
}

// Unsubscribe 取消订阅（数据保留；带 TTL 时按 TTL 过期清理）。
func (s *StreamSubscription) Unsubscribe(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.store.UnsubscribeStream(ctx, s.ID)
}

// State 返回订阅的当前状态（是否已同步过、服务端是否标记为默认流等）。
func (s *StreamSubscription) State(ctx context.Context) (StreamSubscriptionState, error) {
	if s == nil || s.db == nil {
		return StreamSubscriptionState{}, nil
	}
	sub, err := s.db.store.StreamSubscription(ctx, s.Name, s.Parameters)
	if err != nil {
		return StreamSubscriptionState{}, err
	}
	if sub == nil {
		return StreamSubscriptionState{}, nil
	}
	return StreamSubscriptionState{
		Name:         sub.Name,
		Parameters:   sub.Parameters,
		Active:       sub.Active,
		IsDefault:    sub.IsDefault,
		HasSynced:    sub.LastSyncedAt != nil,
		Priority:     sub.Priority,
		TTL:          sub.TTL,
		ExpiresAt:    sub.ExpiresAt,
		LastSyncedAt: sub.LastSyncedAt,
	}, nil
}

// StreamSubscriptions 返回所有 active 的订阅。
func (db *DB) StreamSubscriptions(ctx context.Context) ([]StreamSubscriptionState, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	subs, err := db.store.ActiveStreams(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]StreamSubscriptionState, 0, len(subs))
	for _, sub := range subs {
		out = append(out, StreamSubscriptionState{
			Name:         sub.Name,
			Parameters:   sub.Parameters,
			Active:       sub.Active,
			IsDefault:    sub.IsDefault,
			HasSynced:    sub.LastSyncedAt != nil,
			Priority:     sub.Priority,
			TTL:          sub.TTL,
			ExpiresAt:    sub.ExpiresAt,
			LastSyncedAt: sub.LastSyncedAt,
		})
	}
	return out, nil
}
