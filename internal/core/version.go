package core

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// 数据版本表：任何用户表写入（触发器）或同步物化（Go 侧）都会 +1。
// Watch 用它做"有变化"的零成本探测，避免轮询整条查询。
const versionTableDDL = `CREATE TABLE IF NOT EXISTS ps_internal_version (v INTEGER NOT NULL);
INSERT OR IGNORE INTO ps_internal_version(v) VALUES (0);`

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

// Version 返回当前数据版本。
func (db *DB) Version(ctx context.Context) (int64, error) {
	var v int64
	err := db.sql.QueryRowContext(ctx, `SELECT v FROM ps_internal_version`).Scan(&v)
	return v, err
}

// NotifyDataChange 广播"数据变了"，让 Watch 立刻重查。
//
// 用户表写入的版本号由触发器负责 +1，但触发器无法通知 Go 侧的 channel，
// 所以公开 API 在写操作成功后调用这里。
func (db *DB) NotifyDataChange() { db.version.notify() }

// VersionSignal 返回一个在数据变化时被关闭的 channel（用于 Watch 推送）。
func (db *DB) VersionSignal() <-chan struct{} { return db.version.wait() }

// bumpVersion 在事务外让版本 +1（同步物化路径用，因为那条路径不经过用户触发器）。
func (db *DB) bumpVersion(ctx context.Context) error {
	if _, err := db.sql.ExecContext(ctx, `UPDATE ps_internal_version SET v = v + 1`); err != nil {
		return fmt.Errorf("core: bump version: %w", err)
	}
	db.version.notify()
	return nil
}

// bumpVersionTx 在事务内让版本 +1（与数据变更同一事务，保证原子）。
func bumpVersionTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `UPDATE ps_internal_version SET v = v + 1`)
	return err
}
