package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zhuweizz/powersync_golang/internal/protocol"
)

// StreamSubscription 是 ps_stream_subscriptions 中的一行（core v11+ 的表结构）。
type StreamSubscription struct {
	ID           int64
	Name         string
	Parameters   map[string]any
	Active       bool
	IsDefault    bool
	Priority     *int
	TTL          *time.Duration
	ExpiresAt    *time.Time
	LastSyncedAt *time.Time
}

// streamTableDDL 与 powersync-sqlite-core 的 migration 11 一致。
const streamTableDDL = `CREATE TABLE IF NOT EXISTS ps_stream_subscriptions (
	id INTEGER NOT NULL PRIMARY KEY,
	stream_name TEXT NOT NULL,
	active INTEGER NOT NULL DEFAULT FALSE,
	is_default INTEGER NOT NULL DEFAULT FALSE,
	local_priority INTEGER,
	local_params TEXT NOT NULL DEFAULT 'null',
	ttl INTEGER,
	expires_at INTEGER,
	last_synced_at INTEGER,
	UNIQUE (stream_name, local_params)
) STRICT;`

// SubscribeStream 新增/更新一条流订阅（active=1），返回持久化后的记录。
//
// ttl 非空时记录过期时间（expires_at = now + ttl）；priority 非空时覆盖服务端优先级。
func (db *DB) SubscribeStream(ctx context.Context, name string, params map[string]any, ttl *time.Duration, priority *int) (*StreamSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("core: 流名不能为空")
	}
	paramsJSON, err := marshalParams(params)
	if err != nil {
		return nil, err
	}
	var ttlSeconds any
	var expiresAt any
	now := time.Now().UTC()
	if ttl != nil {
		ttlSeconds = int64(ttl.Seconds())
		expiresAt = now.Add(*ttl).UnixMicro()
	}
	var prio any
	if priority != nil {
		prio = *priority
	}

	_, err = db.sql.ExecContext(ctx,
		`INSERT INTO ps_stream_subscriptions(stream_name, active, local_priority, local_params, ttl, expires_at)
		 VALUES(?, 1, ?, ?, ?, ?)
		 ON CONFLICT(stream_name, local_params) DO UPDATE SET
		   active = 1, local_priority = excluded.local_priority,
		   ttl = excluded.ttl, expires_at = excluded.expires_at`,
		name, prio, paramsJSON, ttlSeconds, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("core: 订阅流 %q: %w", name, err)
	}
	return db.StreamSubscription(ctx, name, params)
}

// StreamSubscription 按 (name, params) 查订阅；不存在时返回 nil。
func (db *DB) StreamSubscription(ctx context.Context, name string, params map[string]any) (*StreamSubscription, error) {
	paramsJSON, err := marshalParams(params)
	if err != nil {
		return nil, err
	}
	row := db.sql.QueryRowContext(ctx,
		`SELECT id, stream_name, active, is_default, local_priority, local_params, ttl, expires_at, last_synced_at
		   FROM ps_stream_subscriptions WHERE stream_name = ? AND local_params = ?`, name, paramsJSON)
	sub, err := scanStreamSubscription(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return sub, err
}

// ActiveStreams 返回所有 active 的订阅（用于拼 streams 请求体）。
func (db *DB) ActiveStreams(ctx context.Context) ([]StreamSubscription, error) {
	rows, err := db.sql.QueryContext(ctx,
		`SELECT id, stream_name, active, is_default, local_priority, local_params, ttl, expires_at, last_synced_at
		   FROM ps_stream_subscriptions WHERE active = 1 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StreamSubscription
	for rows.Next() {
		sub, err := scanStreamSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sub)
	}
	return out, rows.Err()
}

// UnsubscribeStream 把订阅置为非 active（有 ttl 时从"现在"开始倒计时）。
func (db *DB) UnsubscribeStream(ctx context.Context, id int64) error {
	_, err := db.sql.ExecContext(ctx,
		`UPDATE ps_stream_subscriptions
		    SET active = 0,
		        expires_at = CASE WHEN ttl IS NULL THEN NULL ELSE (unixepoch('subsec') * 1000000) + ttl * 1000000 END
		  WHERE id = ?`, id)
	return err
}

// PurgeExpiredStreams 删除已过期的非 active 订阅，返回删除条数。
func (db *DB) PurgeExpiredStreams(ctx context.Context, now time.Time) (int64, error) {
	res, err := db.sql.ExecContext(ctx,
		`DELETE FROM ps_stream_subscriptions
		  WHERE active = 0 AND expires_at IS NOT NULL AND expires_at <= ?`, now.UnixMicro())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// UpdateStreamSyncState 用 checkpoint 里的 streams 信息更新订阅状态：
// is_default 以服务端为准；出现在 checkpoint 里的流更新 last_synced_at。
func (db *DB) UpdateStreamSyncState(ctx context.Context, states []protocol.StreamState, syncedAt time.Time) error {
	if len(states) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, st := range states {
		if st.Name == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE ps_stream_subscriptions SET is_default = ? WHERE stream_name = ?`,
			boolInt(st.IsDefault), st.Name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE ps_stream_subscriptions SET last_synced_at = ? WHERE stream_name = ? AND active = 1`,
			syncedAt.UnixMicro(), st.Name); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func marshalParams(params map[string]any) (string, error) {
	if len(params) == 0 {
		return "null", nil
	}
	b, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("core: 流参数不是合法 JSON: %w", err)
	}
	return string(b), nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanStreamSubscription(row rowScanner) (*StreamSubscription, error) {
	var (
		sub               StreamSubscription
		active, isDefault int
		paramsJSON        string
		prio              sql.NullInt64
		ttlSecs           sql.NullInt64
		expiresNull       sql.NullInt64
		lastNull          sql.NullInt64
	)
	if err := row.Scan(&sub.ID, &sub.Name, &active, &isDefault, &prio, &paramsJSON,
		&ttlSecs, &expiresNull, &lastNull); err != nil {
		return nil, err
	}
	sub.Active = active != 0
	sub.IsDefault = isDefault != 0
	if prio.Valid {
		p := int(prio.Int64)
		sub.Priority = &p
	}
	if paramsJSON != "" && paramsJSON != "null" {
		if err := json.Unmarshal([]byte(paramsJSON), &sub.Parameters); err != nil {
			return nil, fmt.Errorf("core: local_params 不是合法 JSON: %w", err)
		}
	}
	if ttlSecs.Valid && ttlSecs.Int64 > 0 {
		d := time.Duration(ttlSecs.Int64) * time.Second
		sub.TTL = &d
	}
	if expiresNull.Valid {
		t := time.UnixMicro(expiresNull.Int64).UTC()
		sub.ExpiresAt = &t
	}
	if lastNull.Valid {
		t := time.UnixMicro(lastNull.Int64).UTC()
		sub.LastSyncedAt = &t
	}
	return &sub, nil
}
