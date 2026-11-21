package powersync

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"time"
)

// Tx 是 WriteTransaction 内的事务句柄。
type Tx struct{ tx *sql.Tx }

// Execute 在事务内执行写语句，返回受影响行数。
func (tx *Tx) Execute(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := tx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// GetAll 在事务内查询全部行。
func (tx *Tx) GetAll(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// GetOptional 在事务内查询第一行；无行时 found=false。
func (tx *Tx) GetOptional(ctx context.Context, query string, args ...any) (map[string]any, bool, error) {
	rows, err := tx.GetAll(ctx, query, args...)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	return rows[0], true, nil
}

// Execute 执行写语句（INSERT/UPDATE/DELETE），返回受影响行数。
//
// 注意：默认的 JSON 表对外是**视图**，写操作走 INSTEAD OF 触发器，SQLite 对视图写入
// 恒报 0 行受影响（direct 表会返回真实行数）。需要确认写入是否生效请查询数据本身。
//
// 本地写会通过触发器自动进入上传队列（local_only 表除外）。
func (db *DB) Execute(ctx context.Context, query string, args ...any) (int64, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	res, err := db.store.SQL().ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	db.store.NotifyDataChange()
	if n > 0 {
		db.syncer.TriggerUpload()
	}
	return n, nil
}

// GetAll 查询全部行（值为 int64/float64/string/[]byte/nil）。
func (db *DB) GetAll(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	rows, err := db.store.SQL().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// GetOptional 查询第一行；无行时 found=false。
func (db *DB) GetOptional(ctx context.Context, query string, args ...any) (map[string]any, bool, error) {
	rows, err := db.GetAll(ctx, query, args...)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	return rows[0], true, nil
}

// WriteTransaction 在一个写事务里执行 fn（出错自动回滚）。
//
// 事务内的本地写会被合并成同一批上传数据（ps_crud.tx_id）。
func (db *DB) WriteTransaction(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	if err := db.check(); err != nil {
		return err
	}
	err := db.store.WriteTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return fn(ctx, &Tx{tx: tx})
	})
	if err != nil {
		return err
	}
	db.store.NotifyDataChange()
	db.syncer.TriggerUpload()
	return nil
}

// Watch 监听查询结果变化，变化时向返回的 channel 推送一次（初值会立即推送）。
//
// 实现方式：用户表的触发器 + 同步物化都会把 ps_internal_version +1，
// Watch 等这个版本信号（推送）而不是高频轮询；`Options.WatchInterval`
// 只作为兜底（默认 2s），用于捕捉漏通知的场景。channel 在 ctx 结束时关闭。
func (db *DB) Watch(ctx context.Context, query string, args ...any) (<-chan []map[string]any, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	ch := make(chan []map[string]any, 1)
	go func() {
		defer close(ch)
		var last uint64
		first := true
		emit := func() bool {
			rows, err := db.GetAll(ctx, query, args...)
			if err != nil {
				if ctx.Err() != nil {
					return false
				}
				db.logger.Warn("watch query failed", "err", err)
				return true
			}
			// 用 64 位哈希代替"把整个结果集 fmt.Sprint 成字符串"：
			// 大结果集下不再每次分配一个巨型字符串（除非内容确实变了才把 rows 发出去）。
			hash := hashRows(rows)
			if !first && hash == last {
				return true
			}
			first = false
			last = hash
			select {
			case ch <- rows:
				return true
			case <-ctx.Done():
				return false
			}
		}
		// 先取 signal 再查询：如果查询期间发生变更，signal 已关闭，下一轮立即重查；
		// 若反过来（先查再取），两者之间的变更要等兜底轮询才能被发现。
		for {
			signal := db.store.VersionSignal()
			if !emit() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-signal:
			case <-time.After(db.watchInterval):
			}
		}
	}()
	return ch, nil
}

// hashRows 对结果集做确定性 64 位哈希（键有序、值按类型编码）：
// 用于 `Watch` 判断"结果是否真的变了"，避免每次变更都分配大字符串。
//
// 碰撞概率（同一查询的两次结果之间）可忽略；即使碰撞，最坏结果是漏一次推送，
// 下一个变更/兜底轮询会补上。
func hashRows(rows []map[string]any) uint64 {
	h := fnv.New64a()
	var buf [64]byte
	keys := make([]string, 0, 16)   // 复用，避免每行分配
	scratch := make([]byte, 0, 256) // 复用，避免每次 Write 把 string 转成新切片
	for _, row := range rows {
		keys = keys[:0]
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			scratch = append(scratch[:0], k...)
			_, _ = h.Write(scratch)
			_, _ = h.Write([]byte{0})
			switch v := row[k].(type) {
			case nil:
				_, _ = h.Write([]byte{0})
			case int64:
				_, _ = h.Write(strconv.AppendInt(buf[:0], v, 10))
			case float64:
				_, _ = h.Write(strconv.AppendFloat(buf[:0], v, 'g', -1, 64))
			case string:
				scratch = append(scratch[:0], v...)
				_, _ = h.Write(scratch)
			case []byte:
				_, _ = h.Write(v)
			case bool:
				if v {
					_, _ = h.Write([]byte{1})
				} else {
					_, _ = h.Write([]byte{0})
				}
			default:
				_, _ = h.Write([]byte(fmt.Sprint(v)))
			}
			_, _ = h.Write([]byte{0xff})
		}
		_, _ = h.Write([]byte{0xfe}) // 行分隔
	}
	return h.Sum64()
}

// WatchTables 与 Watch 类似，但只在这些表的数据版本变化时才重新执行查询。
//
// 相比 `Watch`（任何写入都会重查再哈希去重），它能把"无关写入"的总开销降到一次
// 版本读取（O(表数)），适合大结果集/高频写入的场景。
//
// tables 用的是 schema 里的表名（本地写与同步物化都会让对应表的版本 +1）。
func (db *DB) WatchTables(ctx context.Context, tables []string, query string, args ...any) (<-chan []map[string]any, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	snapshot, err := db.store.Versions(ctx, tables)
	if err != nil {
		return nil, err
	}
	ch := make(chan []map[string]any, 1)
	go func() {
		defer close(ch)
		var last uint64
		first := true
		emit := func() bool {
			rows, err := db.GetAll(ctx, query, args...)
			if err != nil {
				if ctx.Err() != nil {
					return false
				}
				db.logger.Warn("watch query failed", "err", err)
				return true
			}
			hash := hashRows(rows)
			if !first && hash == last {
				return true
			}
			first = false
			last = hash
			select {
			case ch <- rows:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !emit() {
			return
		}
		for {
			signal := db.store.VersionSignal()
			cur, err := db.store.Versions(ctx, tables)
			if err != nil && ctx.Err() != nil {
				return
			}
			if !versionsEqual(cur, snapshot) {
				snapshot = cur
				if !emit() {
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-signal:
			case <-time.After(db.watchInterval):
			}
		}
	}()
	return ch, nil
}

func versionsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = normalizeValue(vals[i])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func normalizeValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}
