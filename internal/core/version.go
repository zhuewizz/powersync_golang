package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// 数据版本表：任何用户表写入（触发器）或同步物化（Go 侧）都会 +1。
// Watch 用它做"有变化"的零成本探测，避免轮询整条查询。
const versionTableDDL = `CREATE TABLE IF NOT EXISTS ps_internal_version (tbl TEXT PRIMARY KEY NOT NULL, v INTEGER NOT NULL);`

// migrateVersionTable 兼容旧库：早期版本是单列 (v)，没有 tbl 列 → 直接重建
// （版本号只是变更通知计数器，重置不影响正确性：Watch 以"启动时的快照"为基线）。
const migrateVersionTable = `SELECT count(*) FROM pragma_table_info('ps_internal_version') WHERE name = 'tbl'`

// versionSignal 是版本变化的广播器：bump 时关闭当前 channel 并换一个新的。
type versionSignal struct {
	mu sync.Mutex
	ch chan struct{}
}

func newVersionSignal() *versionSignal { return &versionSignal{ch: make(chan struct{})} }

func (s *versionSignal) notify() {
	s.mu.Lock()
	close(s.ch)
	s.ch = make(chan struct{})
	s.mu.Unlock()
}

func (s *versionSignal) wait() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ch
}

// Version 返回某张表的数据版本（不存在则为 0）。
func (db *DB) Version(ctx context.Context, table string) (int64, error) {
	var v int64
	err := db.sql.QueryRowContext(ctx,
		`SELECT v FROM ps_internal_version WHERE tbl = ?`, table).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// Versions 一次读多张表的版本（Watch 用来判断"相关表有没有变"）。
func (db *DB) Versions(ctx context.Context, tables []string) (map[string]int64, error) {
	out := make(map[string]int64, len(tables))
	for _, t := range tables {
		out[t] = 0
	}
	if len(tables) == 0 {
		return out, nil
	}
	args := make([]any, len(tables))
	for i, t := range tables {
		args[i] = t
	}
	rows, err := db.sql.QueryContext(ctx,
		"SELECT tbl, v FROM ps_internal_version WHERE tbl IN ("+placeholders(len(tables))+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		var n int64
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		out[t] = n
	}
	return out, rows.Err()
}

// NotifyDataChange 广播"数据变了"，让 Watch 立刻重查。
//
// 用户表写入的版本号由触发器负责 +1，但触发器无法通知 Go 侧的 channel，
// 所以公开 API 在写操作成功后调用这里。
func (db *DB) NotifyDataChange() { db.version.notify() }

// VersionSignal 返回一个在数据变化时被关闭的 channel（用于 Watch 推送）。
func (db *DB) VersionSignal() <-chan struct{} { return db.version.wait() }

// bumpVersionTx 在事务内给若干张表各 +1（与数据变更同一事务，保证原子）。
func bumpVersionTx(ctx context.Context, tx *sql.Tx, tables []string) error {
	for _, tbl := range tables {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ps_internal_version(tbl, v) VALUES(?, 1)
			 ON CONFLICT(tbl) DO UPDATE SET v = v + 1`, tbl); err != nil {
			return fmt.Errorf("core: bump version(%s): %w", tbl, err)
		}
	}
	return nil
}

// debugf 输出 core 内部调试日志（默认丢弃，`Options.Debugf` 可接管）。
func (db *DB) debugf(format string, args ...any) {
	if db.opts.Debugf != nil {
		db.opts.Debugf(format, args...)
	}
}
