package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"msmkSync/internal/protocol"
)

// guardTable 是“正在应用同步数据”的标记表。触发器看到它有行就静默，
// 避免把下载下来的变更再写进上传队列（对应 core 的 C 侧 apply 标志）。
const guardTable = "ps_internal_apply"

// internalDDL 是与 powersync-sqlite-core 对齐的内部表（本包用到的子集）。
//
// ps_crud 的列顺序/含义与 core 一致：id 自增、data 为 op JSON、tx_id 可空。
var internalDDL = append([]string{
	`CREATE TABLE IF NOT EXISTS ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT);`,
	`CREATE TABLE IF NOT EXISTS ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB);`,
	`CREATE TABLE IF NOT EXISTS ps_crud(id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, tx_id INTEGER);`,
	`CREATE TABLE IF NOT EXISTS ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER);`,
	`INSERT OR IGNORE INTO ps_tx(id, current_tx, next_tx) VALUES(1, NULL, 1);`,
	// ps_buckets 对齐 powersync-sqlite-core v14（target_op 已迁到 ps_kv）。
	`CREATE TABLE IF NOT EXISTS ps_buckets(
	   id INTEGER PRIMARY KEY,
	   name TEXT NOT NULL,
	   last_applied_op INTEGER NOT NULL DEFAULT 0,
	   last_op INTEGER NOT NULL DEFAULT 0,
	   add_checksum INTEGER NOT NULL DEFAULT 0,
	   op_checksum INTEGER NOT NULL DEFAULT 0,
	   pending_delete INTEGER NOT NULL DEFAULT 0,
	   count_at_last INTEGER NOT NULL DEFAULT 0,
	   count_since_last INTEGER NOT NULL DEFAULT 0,
	   downloaded_size INTEGER NOT NULL DEFAULT 0);`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ps_buckets_name ON ps_buckets (name);`,
	// ps_oplog：bucket 存 ps_buckets.id；op 为 OpType 判别值（0=CLEAR,1=MOVE,2=PUT,3=REMOVE）。
	// 注意：当前 core 的插入语句不带 op 列，这里给了 DEFAULT 0 以便两边都能写。
	`CREATE TABLE IF NOT EXISTS ps_oplog(
	   bucket INTEGER NOT NULL,
	   op_id INTEGER NOT NULL,
	   op INTEGER NOT NULL DEFAULT 0,
	   row_type TEXT,
	   row_id TEXT,
	   key TEXT,
	   data TEXT,
	   hash INTEGER NOT NULL,
	   superseded INTEGER NOT NULL DEFAULT 0);`,
	`CREATE INDEX IF NOT EXISTS ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0;`,
	`CREATE INDEX IF NOT EXISTS ps_oplog_by_opid ON ps_oplog (bucket, op_id);`,
	`CREATE INDEX IF NOT EXISTS ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0;`,
	`CREATE TABLE IF NOT EXISTS ps_updated_rows(row_type TEXT NOT NULL, row_id TEXT NOT NULL, PRIMARY KEY(row_type, row_id));`,
	// ps_untyped 是 core 在 schema 变更时搬数据的暂存表；本实现只建表保持结构兼容。
	`CREATE TABLE IF NOT EXISTS ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id));`,
	`CREATE TABLE IF NOT EXISTS ps_sync_state(priority INTEGER NOT NULL PRIMARY KEY, last_synced_at TEXT NOT NULL);`,
	streamTableDDL,
	versionTableDDL,
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id INTEGER PRIMARY KEY);`, guardTable),
}, powersyncCrudDDL...)

// Options 是 Open 的参数。
type Options struct {
	// Path 是 SQLite 文件路径；":memory:" 也可（内部会强制单连接）。
	Path string
	// Schema 为 nil 时表示"由调用方自行建表"（仍会创建内部表）。
	Schema *Schema
	// ClientID 为空时自动生成 UUIDv4 存入 ps_kv。
	ClientID string
	// NoFileLock 为 true 时不加跨进程文件锁（默认加锁：同一 DB 被两个实例打开会报
	// ErrDatabaseLocked，避免两个同步引擎同时写坏数据）。
	NoFileLock bool
}

// DB 是本地库句柄。所有写操作串行化在单个连接上（modernc 下 WAL/内存库都需要）。
type DB struct {
	sql *sql.DB
	// schema 用原子指针：同步 goroutine 会并发读，ReplaceSchema 会整体替换。
	schemaPtr atomic.Pointer[Schema]
	opts      Options
	lock      *fileLock
	version   *versionSignal
	// skippedUnknown 记录因不在 schema 中被跳过物化的行数。
	skippedUnknown atomic.Int64
}

// Open 打开（必要时创建）本地库，并应用 schema。
func Open(ctx context.Context, opts Options) (*DB, error) {
	if opts.Path == "" {
		return nil, errors.New("core: Options.Path 不能为空")
	}
	if opts.Path != ":memory:" {
		// 便于重复运行：调用方显式删库；这里只确保目录存在。
		if dir := strings.TrimSuffix(opts.Path, "/"); dir != "" {
			if i := strings.LastIndexAny(dir, `/\`); i > 0 {
				_ = os.MkdirAll(dir[:i], 0o755)
			}
		}
	}
	dsn := opts.Path
	if opts.Path == ":memory:" {
		dsn = "file::memory:?_pragma=foreign_keys(1)"
	}
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("core: open %s: %w", opts.Path, err)
	}
	// 单连接：ps_internal_apply 守卫是按库全局的，多连接会互相干扰。
	sqlDB.SetMaxOpenConns(1)

	db := &DB{sql: sqlDB, opts: opts, version: newVersionSignal()}
	db.setSchema(opts.Schema)
	if !opts.NoFileLock && opts.Path != ":memory:" {
		lock, err := acquireFileLock(opts.Path)
		if err != nil {
			sqlDB.Close()
			return nil, err
		}
		db.lock = lock
	}
	clientID, err := db.init(ctx, opts.ClientID)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// SQL 函数在包初始化时已全局注册；这里只需把本库的 client id 暴露给 powersync_client_id()。
	setActiveClientID(clientID)

	// 已有库 + 传入 schema：与上次记录的 schema 不同则迁移（经 ps_untyped 搬数据）。
	if db.Schema() != nil {
		current, err := db.SchemaJSON()
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		previous, err := db.Meta(ctx, "schema_json")
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if previous != "" && previous != current {
			prevSchema, perr := ParseSchema([]byte(previous))
			if perr != nil {
				_ = db.Close()
				return nil, fmt.Errorf("core: 旧 schema 无法解析: %w", perr)
			}
			if err := db.replaceSchema(ctx, prevSchema, db.Schema()); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("core: 迁移 schema: %w", err)
			}
		} else if err := db.applySchemaDDL(ctx); err != nil {
			_ = db.Close()
			return nil, err
		}
		if err := db.SetMeta(ctx, "schema_json", current); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func (db *DB) init(ctx context.Context, clientID string) (string, error) {
	stmts := []string{`PRAGMA journal_mode=WAL;`, `PRAGMA foreign_keys=ON;`}
	stmts = append(stmts, internalDDL...)
	for _, s := range stmts {
		if _, err := db.sql.ExecContext(ctx, s); err != nil {
			return "", fmt.Errorf("core: exec %q: %w", firstLine(s), err)
		}
	}
	// 兼容旧库：ps_internal_version 曾是单列 (v)，现在按表 (tbl, v)。
	var hasTbl int
	if err := db.sql.QueryRowContext(ctx, migrateVersionTable).Scan(&hasTbl); err != nil {
		return "", err
	}
	if hasTbl == 0 {
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS ps_internal_version`,
			versionTableDDL,
		} {
			if _, err := db.sql.ExecContext(ctx, stmt); err != nil {
				return "", fmt.Errorf("core: 迁移 ps_internal_version: %w", err)
			}
		}
	}
	if _, err := db.sql.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, guardTable)); err != nil {
		return "", err
	}
	if clientID == "" {
		clientID = NewUUID()
	}
	if _, err := db.sql.ExecContext(ctx,
		`INSERT INTO ps_kv(key, value) VALUES('client_id', ?) ON CONFLICT(key) DO NOTHING`, clientID); err != nil {
		return "", fmt.Errorf("core: init ps_kv: %w", err)
	}
	var stored string
	if err := db.sql.QueryRowContext(ctx,
		`SELECT value FROM ps_kv WHERE key = 'client_id'`).Scan(&stored); err != nil {
		return "", fmt.Errorf("core: 读取 client_id: %w", err)
	}
	return stored, nil
}

// applySchemaDDL 建/补齐用户表对象（幂等；用于新建库或 schema 未变化的情况）。
func (db *DB) applySchemaDDL(ctx context.Context) error {
	s := db.Schema()
	if s == nil {
		return nil
	}
	for i := range s.Tables {
		for _, stmt := range s.Tables[i].DDL() {
			if _, err := db.sql.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("core: exec %q: %w", firstLine(stmt), err)
			}
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// Close 关闭底层连接并释放文件锁。
func (db *DB) Close() error {
	err := db.sql.Close()
	if db.lock != nil {
		db.lock.release()
		db.lock = nil
	}
	return err
}

// ErrDatabaseLocked 表示同一数据库文件已被另一个实例占用。
var ErrDatabaseLocked = errLocked

// SQL 暴露底层 *sql.DB（供工具/测试执行自己的 SQL；库内部不要依赖它）。
func (db *DB) SQL() *sql.DB { return db.sql }

// StorageName 返回某张表的落盘对象名：JSON 表为 ps_data__*，direct/adhoc 表为表名本身。
func (db *DB) StorageName(table string) string {
	if s := db.Schema(); s != nil {
		if t, ok := s.Table(table); ok && !t.Direct {
			return t.InternalName()
		}
	}
	return table
}

// SkippedUnknown 返回因不在 schema 中被跳过的 op 数。
func (db *DB) SkippedUnknown() int64 { return db.skippedUnknown.Load() }

// Schema 返回当前 schema（可能为 nil）。
func (db *DB) Schema() *Schema { return db.schemaPtr.Load() }

func (db *DB) setSchema(s *Schema) { db.schemaPtr.Store(s) }

// ClientID 返回本地 client id（ps_kv 中的 client_id）。
func (db *DB) ClientID(ctx context.Context) (string, error) {
	var v string
	err := db.sql.QueryRowContext(ctx, `SELECT value FROM ps_kv WHERE key='client_id'`).Scan(&v)
	return v, err
}

// ApplyOps 把某个 bucket 的一批 op 立即应用到本地库（不校验 checksum）。
//
// 内部等价于 StageOps + ApplyCheckpoint：先写 ps_oplog，再物化到用户表并推进游标。
// 真正的同步循环应使用 StageOps/ApplyCheckpoint，以便在 checkpoint_complete 处校验。
func (db *DB) ApplyOps(ctx context.Context, bucket string, ops []protocol.Op, lastApplied protocol.OpID) error {
	if err := db.StageOps(ctx, bucket, ops); err != nil {
		return err
	}
	_, err := db.ApplyCheckpoint(ctx, lastApplied,
		[]BucketTarget{{Name: bucket}}, ApplyOptions{})
	return err
}

// upsertRowTx 把一行 JSON 写进用户表（schema 未声明的表按配置跳过）。
func (db *DB) upsertRowTx(ctx context.Context, tx *sql.Tx, table, rowID string, data []byte) error {
	if table == "" || rowID == "" {
		return fmt.Errorf("core: 缺少 table/row id")
	}
	if s := db.Schema(); s != nil {
		if _, ok := s.Table(table); !ok {
			// 服务端的 sync rules 常常比 AppSchema 宽（例如多出 material_category 之类的表）。
			// core 对 schema 未声明的表不做物化，这里同样跳过并计数（oplog/checksum 不受影响）。
			db.skippedUnknown.Add(1)
			return nil
		}
	}
	var tbl *Table
	if s := db.Schema(); s != nil {
		tbl, _ = s.Table(table)
	}
	if tbl != nil && !tbl.Direct {
		// JSON 表：整行（含 id）以 JSON 存进 ps_data__*（与官方 core 一致），
		// 对外通过同名视图暴露列。
		var row map[string]any
		if len(data) > 0 {
			if err := json.Unmarshal(data, &row); err != nil {
				return fmt.Errorf("core: %s/%s: %w", table, rowID, err)
			}
		}
		if row == nil {
			row = map[string]any{}
		}
		if _, ok := row[PrimaryKeyColumn]; !ok {
			row[PrimaryKeyColumn] = rowID
		}
		// 只保留 schema 声明的列（服务端可能多下发列）。
		clean := make(map[string]any, len(tbl.Columns))
		for _, col := range tbl.Columns {
			if v, ok := row[col.Name]; ok {
				clean[col.Name] = v
			}
		}
		clean[PrimaryKeyColumn] = rowID
		payload, err := json.Marshal(clean)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s(id, data) VALUES(?, ?)
			 ON CONFLICT(id) DO UPDATE SET data = excluded.data`, quoteIdent(tbl.InternalName())),
			rowID, string(payload)); err != nil {
			return fmt.Errorf("core: upsert %s/%s: %w", tbl.InternalName(), rowID, err)
		}
		return nil
	}

	row, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("core: %s/%s: %w", table, rowID, err)
	}
	if _, ok := row[PrimaryKeyColumn]; !ok {
		row[PrimaryKeyColumn] = rowID
	}

	cols, values := db.upsertColumns(table, row)
	if len(cols) == 0 {
		return nil
	}
	if db.Schema() == nil {
		// 无 schema 模式（工具/测试）：按行字段惰性建表。
		if err := ensureTable(ctx, tx, table, cols); err != nil {
			return err
		}
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdent(c)
	}
	updates := make([]string, 0, len(cols))
	for _, c := range cols {
		if c == PrimaryKeyColumn {
			continue
		}
		updates = append(updates, fmt.Sprintf("%s = excluded.%s", quoteIdent(c), quoteIdent(c)))
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		quoteIdent(table), strings.Join(quoted, ", "), placeholders(len(cols)))
	if len(updates) > 0 {
		stmt += fmt.Sprintf(` ON CONFLICT(%s) DO UPDATE SET %s`, quoteIdent(PrimaryKeyColumn), strings.Join(updates, ", "))
	}
	if _, err := tx.ExecContext(ctx, stmt, values...); err != nil {
		return fmt.Errorf("core: upsert %s/%s: %w", table, rowID, err)
	}
	return nil
}

// ensureTable 在无 schema 模式下按列名建表（id 为主键）。
func ensureTable(ctx context.Context, tx *sql.Tx, table string, cols []string) error {
	defs := make([]string, 0, len(cols))
	for _, c := range cols {
		if c == PrimaryKeyColumn {
			defs = append(defs, fmt.Sprintf("%s TEXT NOT NULL PRIMARY KEY", quoteIdent(c)))
			continue
		}
		defs = append(defs, quoteIdent(c))
	}
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(table), strings.Join(defs, ", ")))
	return err
}

// upsertColumns 决定写哪些列：有 schema 时取其声明的列（忽略未知字段），
// 没有 schema 时按行内字段（列类型由 SQLite 动态决定）。
func (db *DB) upsertColumns(table string, row map[string]any) ([]string, []any) {
	var cols []string
	if s := db.Schema(); s != nil {
		if t, ok := s.Table(table); ok {
			for _, c := range t.Columns {
				if _, exists := row[c.Name]; exists {
					cols = append(cols, c.Name)
				}
			}
			return cols, valuesFor(row, cols)
		}
	}
	// 无 schema：按行字段（保持稳定顺序：id 优先）
	cols = append(cols, PrimaryKeyColumn)
	for k := range row {
		if k != PrimaryKeyColumn {
			cols = append(cols, k)
		}
	}
	sortStrings(cols[1:])
	return cols, valuesFor(row, cols)
}

func valuesFor(row map[string]any, cols []string) []any {
	out := make([]any, len(cols))
	for i, c := range cols {
		out[i] = normalizeValue(row[c])
	}
	return out
}

// normalizeValue 把 JSON 值转成适合 SQLite 的参数（json.Number → int64/float64）。
func normalizeValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		if t {
			return int64(1)
		}
		return int64(0)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case map[string]any, []any:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	default:
		return t
	}
}

// WriteTransaction 在单个事务内执行 fn。
//
// 事务期间会给本事务内写入 ps_crud 的行分配同一个 tx_id（对齐 core 的 ps_tx），
// 便于使用方按事务原子上传（GetNextCrudTransaction）。
func (db *DB) WriteTransaction(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// 先注册清理、后注册 Rollback：LIFO 保证先回滚（释放唯一连接）再清理 tx_id。
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = db.sql.ExecContext(cleanupCtx, `UPDATE ps_tx SET current_tx = NULL WHERE id = 1`)
	}()
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE ps_tx SET current_tx = next_tx, next_tx = next_tx + 1 WHERE id = 1`); err != nil {
		return fmt.Errorf("core: 分配 tx_id: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// CrudEntry 是上传队列中的一条本地变更（与 core 的 ps_crud.data 对应）。
type CrudEntry struct {
	ID       int64           `json:"client_id"`
	TxID     *int64          `json:"tx_id,omitempty"`
	Op       string          `json:"op"`
	Table    string          `json:"type"`
	RowID    string          `json:"id"`
	Data     json.RawMessage `json:"data,omitempty"`
	Old      json.RawMessage `json:"old,omitempty"`
	Metadata string          `json:"metadata,omitempty"`
}

// CrudBatch 返回最多 limit 条待上传变更（按 id 升序）。无数据时返回空切片。
func (db *DB) CrudBatch(ctx context.Context, limit int) ([]CrudEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.sql.QueryContext(ctx,
		`SELECT id, tx_id, data FROM ps_crud ORDER BY id ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CrudEntry
	for rows.Next() {
		var (
			id   int64
			txID sql.NullInt64
			raw  string
		)
		if err := rows.Scan(&id, &txID, &raw); err != nil {
			return nil, err
		}
		var wire struct {
			Op   string          `json:"op"`
			Type string          `json:"type"`
			ID   string          `json:"id"`
			Data json.RawMessage `json:"data"`
			Old  json.RawMessage `json:"old"`
			Meta string          `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(raw), &wire); err != nil {
			return nil, fmt.Errorf("core: ps_crud.id=%d 不是合法 JSON: %w", id, err)
		}
		e := CrudEntry{ID: id, Op: wire.Op, Table: wire.Type, RowID: wire.ID,
			Data: wire.Data, Old: wire.Old, Metadata: wire.Meta}
		if txID.Valid {
			v := txID.Int64
			e.TxID = &v
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CompleteCrud 把 id <= lastID 的变更出队（上传成功后调用）。
//
// writeCheckpoint 非空时写入 ps_buckets.target_op，让服务端把本地写入回显回来。
func (db *DB) CompleteCrud(ctx context.Context, lastID int64, writeCheckpoint protocol.OpID) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM ps_crud WHERE id <= ?`, lastID); err != nil {
		return err
	}
	if writeCheckpoint > 0 {
		// core v14：target_op 已从 ps_buckets 迁移到 ps_kv 的 target_checkpoint_request_id。
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ps_kv(key, value) VALUES(?, ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			keyTargetCheckpoint, writeCheckpoint.String()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetBucketCursor 记录某个 bucket 已应用到的 op_id（只前进，不回退）。
func (db *DB) SetBucketCursor(ctx context.Context, bucket string, applied protocol.OpID) error {
	if applied <= 0 {
		return nil
	}
	_, err := db.sql.ExecContext(ctx,
		`INSERT INTO ps_buckets(name, last_applied_op) VALUES(?, ?)
		 ON CONFLICT(name) DO UPDATE SET last_applied_op = MAX(ps_buckets.last_applied_op, excluded.last_applied_op)`,
		bucket, int64(applied))
	return err
}

func setBucketCursorTx(ctx context.Context, tx *sql.Tx, bucket string, applied protocol.OpID) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO ps_buckets(name, last_applied_op) VALUES(?, ?)
		 ON CONFLICT(name) DO UPDATE SET last_applied_op = MAX(ps_buckets.last_applied_op, excluded.last_applied_op)`,
		bucket, int64(applied))
	if err != nil {
		return fmt.Errorf("core: update bucket state: %w", err)
	}
	return nil
}

// Meta 读取 ps_kv 中的字符串值。
func (db *DB) Meta(ctx context.Context, key string) (string, error) {
	var v string
	err := db.sql.QueryRowContext(ctx, `SELECT value FROM ps_kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// SetMeta 写入 ps_kv 中的字符串值。
func (db *DB) SetMeta(ctx context.Context, key, value string) error {
	_, err := db.sql.ExecContext(ctx,
		`INSERT INTO ps_kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value)
	return err
}

// BucketCursor 返回某个 bucket 已应用到的 op_id（没有记录时为 0）。
func (db *DB) BucketCursor(ctx context.Context, bucket string) (protocol.OpID, error) {
	var v int64
	err := db.sql.QueryRowContext(ctx, `SELECT last_applied_op FROM ps_buckets WHERE name = ?`, bucket).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return protocol.OpID(v), err
}

// Buckets 返回全部 bucket 的**请求游标**：ps_buckets.last_op。
//
// 注意不是 last_applied_op：core 的 collect_bucket_requests 也用 last_op。
// 用 last_applied_op 会在中途断流时把同一批 data 重发一遍，导致 checksum 累计漂移。
func (db *DB) Buckets(ctx context.Context) (map[string]protocol.OpID, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT name, last_op FROM ps_buckets`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]protocol.OpID{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = protocol.OpID(v)
	}
	return out, rows.Err()
}

// decodeObject 解析 JSON 对象，数字保留为 json.Number（避免精度/类型漂移）。
func decodeObject(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// NewUUID 生成 UUIDv4（避免额外依赖；仅用于 client_id / 测试）。
func NewUUID() string {
	var b [16]byte
	if _, err := randRead(b[:]); err != nil {
		// 退化路径：用时间戳保证唯一性；正常不会走到这里。
		return "00000000-0000-4000-8000-" + strconv.FormatInt(nowUnixNano(), 10)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	const hex = "0123456789abcdef"
	out := make([]byte, 36)
	i := 0
	for x, v := range b {
		if x == 4 || x == 6 || x == 8 || x == 10 {
			out[i] = '-'
			i++
		}
		out[i] = hex[v>>4]
		out[i+1] = hex[v&0x0f]
		i += 2
	}
	return string(out)
}

// CrudPending 返回上传队列里待上传的条数（上传循环用来判断是否需要跑一次上传）。
func (db *DB) CrudPending(ctx context.Context) (int64, error) {
	var n int64
	err := db.sql.QueryRowContext(ctx, `SELECT count(*) FROM ps_crud`).Scan(&n)
	return n, err
}
