package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"msmkSync/internal/protocol"
)

// ps_oplog 的 op 判别值（与 powersync-sqlite-core 的 OpType 枚举顺序一致）。
const (
	opClear  = 0
	opMove   = 1
	opPut    = 2
	opRemove = 3
)

// ck32 把服务端下发的带符号 32 位 checksum 转成无符号视图。
func ck32(v int64) uint32 { return uint32(int32(v)) }

// ckStore 把无符号 checksum 以带符号 32 位形式存进 SQLite（对齐 core 的 bitcast_i32）。
func ckStore(v uint32) int64 { return int64(int32(v)) }

// bucketInfo 是 ps_buckets 里我们关心的字段。
type bucketInfo struct {
	id          int64
	lastApplied protocol.OpID
	add         uint32
	op          uint32
}

// lookupBucketTx 取（必要时创建）bucket 行。
func lookupBucketTx(ctx context.Context, tx *sql.Tx, name string) (*bucketInfo, error) {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ps_buckets(name) VALUES(?) ON CONFLICT(name) DO NOTHING`, name); err != nil {
		return nil, fmt.Errorf("core: ensure bucket %q: %w", name, err)
	}
	var info bucketInfo
	var lastApplied, add, op int64
	err := tx.QueryRowContext(ctx,
		`SELECT id, last_applied_op, add_checksum, op_checksum FROM ps_buckets WHERE name = ?`, name).
		Scan(&info.id, &lastApplied, &add, &op)
	if err != nil {
		return nil, fmt.Errorf("core: load bucket %q: %w", name, err)
	}
	info.lastApplied = protocol.OpID(lastApplied)
	info.add = ck32(add)
	info.op = ck32(op)
	return &info, nil
}

// oplogKey 生成 core 使用的 oplog key：`{object_type}/{object_id}/{subkey|"null"}`。
func oplogKey(op *protocol.Op) (key, rowType, rowID string) {
	rowType, rowID = op.Table(), op.RowID()
	if rowType == "" || rowID == "" {
		return "", rowType, rowID
	}
	sub := op.Subkey
	if sub == "" {
		// core 对缺省 subkey 统一写字符串 "null"（不是 SQL NULL）。
		sub = "null"
	}
	return rowType + "/" + rowID + "/" + sub, rowType, rowID
}

// StageOps 把一批 op 记进 ps_oplog 并累计 checksum（对应 core 的 insert_bucket_operations）。
//
// 这一步**不改用户表**：数据在 ApplyCheckpoint 收到 checkpoint_complete 时，
// 先校验 checksum 再统一物化（与 core 的 sync_local 保持一致）。
//
// 规则：
//   - PUT/REMOVE 先 supersede 同 key 的旧行：add += 旧 hash，op -= 旧 hash；
//   - REMOVE 不写 oplog，只 add += 本条 checksum；被 supersede 过的行打进 ps_updated_rows；
//   - MOVE 只 add += checksum；
//   - CLEAR 清空该 bucket 的 oplog，并把 add 重置为 CLEAR 的 checksum。
func (db *DB) StageOps(ctx context.Context, bucket string, ops []protocol.Op) error {
	if len(ops) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	info, err := lookupBucketTx(ctx, tx, bucket)
	if err != nil {
		return err
	}
	if err := db.stageOpsTx(ctx, tx, info, ops); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) stageOpsTx(ctx context.Context, tx *sql.Tx, info *bucketInfo, ops []protocol.Op) error {
	isEmpty := info.lastApplied == 0
	baseAdd, baseOp := info.add, info.op
	var addDelta, opDelta uint32
	sawClear := false

	selectHashes, err := tx.PrepareContext(ctx,
		`SELECT hash FROM ps_oplog WHERE bucket = ? AND key = ?`)
	if err != nil {
		return err
	}
	defer selectHashes.Close()
	deleteKey, err := tx.PrepareContext(ctx, `DELETE FROM ps_oplog WHERE bucket = ? AND key = ?`)
	if err != nil {
		return err
	}
	defer deleteKey.Close()
	insertOp, err := tx.PrepareContext(ctx,
		`INSERT INTO ps_oplog(bucket, op_id, op, key, row_type, row_id, data, hash, superseded)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)`)
	if err != nil {
		return err
	}
	defer insertOp.Close()
	markUpdated, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer markUpdated.Close()

	for i := range ops {
		o := &ops[i]
		switch o.Op {
		case protocol.OpPut, protocol.OpDelete, protocol.OpRemove:
			key, rowType, rowID := oplogKey(o)
			superseded := false
			if key != "" {
				rows, qerr := selectHashes.QueryContext(ctx, info.id, key)
				if qerr != nil {
					return qerr
				}
				for rows.Next() {
					var hash int64
					if qerr := rows.Scan(&hash); qerr != nil {
						rows.Close()
						return qerr
					}
					h := ck32(hash)
					addDelta += h
					opDelta -= h
					// core：只要 bucket 原来已有数据，就认为存在需要回放的旧值。
					if !isEmpty {
						superseded = true
					}
				}
				if qerr := rows.Err(); qerr != nil {
					rows.Close()
					return qerr
				}
				rows.Close()

				if _, derr := deleteKey.ExecContext(ctx, info.id, key); derr != nil {
					return derr
				}
			}

			if o.Op != protocol.OpPut {
				// REMOVE/DELETE：只累计 checksum，不写 oplog。
				addDelta += ck32(o.Checksum)
				if superseded && rowType != "" {
					if _, uerr := markUpdated.ExecContext(ctx, rowType, rowID); uerr != nil {
						return uerr
					}
				}
				continue
			}

			data, derr := o.DecodeData()
			if derr != nil {
				return fmt.Errorf("core: op %s data: %w", o.OpID, derr)
			}
			var keyVal any
			if key != "" {
				keyVal = key
			}
			if _, ierr := insertOp.ExecContext(ctx,
				info.id, int64(o.OpID), opPut, keyVal, rowType, rowID, string(data), ckStore(ck32(o.Checksum))); ierr != nil {
				return ierr
			}
			opDelta += ck32(o.Checksum)

		case protocol.OpMove:
			addDelta += ck32(o.Checksum)

		case protocol.OpClear:
			if _, derr := tx.ExecContext(ctx, `DELETE FROM ps_oplog WHERE bucket = ?`, info.id); derr != nil {
				return derr
			}
			// CLEAR 之后 add 直接等于该 op 的 checksum，op 归零。
			baseAdd, baseOp = ck32(o.Checksum), 0
			addDelta, opDelta = 0, 0
			isEmpty = true
			sawClear = true
		}
	}

	lastOp := int64(ops[len(ops)-1].OpID)
	newAdd := baseAdd + addDelta
	newOp := baseOp + opDelta
	// 注意：这里只推进 last_op；last_applied_op 在 ApplyCheckpoint 物化成功后推进
	// （core 也是这个顺序，否则物化时筛 op_id > last_applied_op 会一条都匹配不到）。
	stmt := `UPDATE ps_buckets SET
	          last_op = ?,
	          add_checksum = ?,
	          op_checksum = ?,
	          count_since_last = count_since_last + ?
	        WHERE id = ?`
	if sawClear {
		// core 在 CLEAR 时把 last_applied_op 归零，后续需要整桶重放。
		stmt = `UPDATE ps_buckets SET
		          last_op = ?, last_applied_op = 0,
		          add_checksum = ?, op_checksum = ?,
		          count_since_last = count_since_last + ?
		        WHERE id = ?`
		_, err := tx.ExecContext(ctx, stmt,
			lastOp, ckStore(newAdd), ckStore(newOp), len(ops), info.id)
		return err
	}
	_, err = tx.ExecContext(ctx, stmt,
		lastOp, ckStore(newAdd), ckStore(newOp), len(ops), info.id)
	return err
}

// ChecksumMismatch 描述一个 bucket 的本地累计校验和与服务端不一致。
type ChecksumMismatch struct {
	Bucket   string
	Expected uint32
	Actual   uint32
	Add      uint32
	Op       uint32
}

func (m ChecksumMismatch) String() string {
	return fmt.Sprintf("%s (expected %d, got %d = %d(add) + %d(op))",
		m.Bucket, m.Expected, m.Actual, m.Add, m.Op)
}

// BucketTarget 是 checkpoint 里单个 bucket 的目标状态。
type BucketTarget struct {
	Name     string
	Checksum uint32
	Count    int64
	Priority int
}

// ApplyOptions 控制 ApplyCheckpoint 的行为。
type ApplyOptions struct {
	// Validate 为 true 时校验本地累计 checksum，不一致则（ResetOnMismatch）重置 bucket。
	Validate bool
	// ResetOnMismatch 为 true 时，校验失败的 bucket 会被清空以便从 after=0 重下。
	ResetOnMismatch bool
	// Priority 写入 ps_sync_state（缺省 3，对应服务端的默认优先级）。
	Priority int
	// MinPriority > 0 时只应用 priority >= MinPriority 的 bucket（partial checkpoint）。
	MinPriority int
}

// ApplyCheckpoint 在 checkpoint_complete 时执行：校验 checksum → 物化 oplog → 推进游标。
//
// 返回的 mismatches 非空时表示这批数据不可信：对应 bucket 已被重置（若开启），
// 调用方应重新发起同步（这些 bucket 会从 after=0 重下）。
func (db *DB) ApplyCheckpoint(ctx context.Context, lastOp protocol.OpID, targets []BucketTarget, opts ApplyOptions) ([]ChecksumMismatch, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// partial checkpoint：只处理该优先级及以上的 bucket，其余保持 staged 等下次。
	if opts.MinPriority > 0 {
		filtered := targets[:0]
		for _, t := range targets {
			if t.Priority >= opts.MinPriority {
				filtered = append(filtered, t)
			}
		}
		targets = filtered
		if len(targets) == 0 {
			return nil, nil
		}
	}

	infos := make([]*bucketInfo, 0, len(targets))
	var mismatches []ChecksumMismatch
	for _, t := range targets {
		info, err := lookupBucketTx(ctx, tx, t.Name)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
		if !opts.Validate {
			continue
		}
		actual := info.add + info.op
		if actual != t.Checksum {
			mismatches = append(mismatches, ChecksumMismatch{
				Bucket: t.Name, Expected: t.Checksum, Actual: actual,
				Add: info.add, Op: info.op,
			})
		}
	}
	if len(mismatches) > 0 {
		if !opts.ResetOnMismatch {
			return mismatches, nil
		}
		for _, m := range mismatches {
			if err := resetBucketTx(ctx, tx, m.Bucket); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return mismatches, nil
	}

	// 物化：把每个 bucket 里 op_id > last_applied_op 的行，以及被 REMOVE 过的行，
	// 合并成"每行取最新一条 oplog"再写进用户表（对应 core 的 collect_full_operations）。
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT OR IGNORE INTO %s(id) VALUES(1)`, guardTable)); err != nil {
		return nil, fmt.Errorf("core: apply guard: %w", err)
	}
	touchedTables := map[string]struct{}{}
	for i, t := range targets {
		if err := db.materializeBucket(ctx, tx, infos[i], t, touchedTables); err != nil {
			// 物化失败时丢弃这一批 staged 状态，避免游标已推进但数据没落地、checksum 漂移。
			// 这些 bucket 会被重置，下一次迭代从 after=0 重下。
			for _, target := range targets {
				if rerr := resetBucketTx(ctx, tx, target.Name); rerr != nil {
					return nil, fmt.Errorf("core: materialize %s: %w (reset 也失败: %v)", t.Name, err, rerr)
				}
			}
			return nil, fmt.Errorf("core: materialize %s: %w", t.Name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, guardTable)); err != nil {
		return nil, fmt.Errorf("core: clear guard: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ps_updated_rows`); err != nil {
		return nil, err
	}

	priority := opts.Priority
	if priority == 0 {
		priority = 3
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO ps_sync_state(priority, last_synced_at) VALUES(?, ?)`,
		priority, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(touchedTables))
	for name := range touchedTables {
		tables = append(tables, name)
	}
	if err := bumpVersionTx(ctx, tx, tables); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	db.version.notify()
	return nil, nil
}

// materializeBucket 把某个 bucket 待应用的 oplog 行写进用户表。
func (db *DB) materializeBucket(ctx context.Context, tx *sql.Tx, info *bucketInfo, target BucketTarget, touched map[string]struct{}) error {
	const q = `
WITH updated AS (
  SELECT row_type, row_id FROM ps_oplog WHERE bucket = ?1 AND op_id > ?2
  UNION ALL
  SELECT row_type, row_id FROM ps_updated_rows
)
SELECT u.row_type, u.row_id,
       (SELECT CASE WHEN max(r.op_id) THEN r.data ELSE NULL END
          FROM ps_oplog r
         WHERE r.row_type = u.row_type AND r.row_id = u.row_id) AS data
  FROM updated u
 GROUP BY u.row_type, u.row_id`

	rows, err := tx.QueryContext(ctx, q, info.id, int64(info.lastApplied))
	if err != nil {
		return err
	}
	type pending struct {
		table string
		id    string
		data  sql.NullString
	}
	var items []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.table, &p.id, &p.data); err != nil {
			rows.Close()
			return err
		}
		items = append(items, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, p := range items {
		if p.table == "" || p.id == "" {
			continue
		}
		touched[p.table] = struct{}{}
		if !p.data.Valid {
			// 没有最新的 PUT（被 REMOVE 掉了）→ 从用户表删除。
			if _, err := tx.ExecContext(ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE %s = ?`, quoteIdent(db.StorageName(p.table)), quoteIdent(PrimaryKeyColumn)),
				p.id); err != nil {
				return fmt.Errorf("delete %s/%s: %w", p.table, p.id, err)
			}
			continue
		}
		if err := db.upsertRowTx(ctx, tx, p.table, p.id, []byte(p.data.String)); err != nil {
			return err
		}
	}

	// 推进游标并重置进度计数（完整 checkpoint 才重置，与 core 一致）。
	if _, err := tx.ExecContext(ctx,
		`UPDATE ps_buckets SET last_applied_op = last_op, count_since_last = 0, count_at_last = ?
		 WHERE id = ?`, target.Count, info.id); err != nil {
		return err
	}
	return nil
}

func resetBucketTx(ctx context.Context, tx *sql.Tx, bucket string) error {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM ps_buckets WHERE name = ?`, bucket).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ps_oplog WHERE bucket = ?`, id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`UPDATE ps_buckets SET last_applied_op = 0, last_op = 0, add_checksum = 0,
		   op_checksum = 0, count_at_last = 0, count_since_last = 0, downloaded_size = 0
		 WHERE id = ?`, id)
	return err
}

// ValidateCheckpoint 只做校验，不改数据（调试/断言用）。
func (db *DB) ValidateCheckpoint(ctx context.Context, want map[string]uint32) ([]ChecksumMismatch, error) {
	stmt, err := db.sql.PrepareContext(ctx,
		`SELECT add_checksum, op_checksum FROM ps_buckets WHERE name = ?`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var out []ChecksumMismatch
	for bucket, expected := range want {
		var add, op int64
		err := stmt.QueryRowContext(ctx, bucket).Scan(&add, &op)
		if errors.Is(err, sql.ErrNoRows) {
			if expected != 0 {
				out = append(out, ChecksumMismatch{Bucket: bucket, Expected: expected})
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		a, o := ck32(add), ck32(op)
		if actual := a + o; actual != expected {
			out = append(out, ChecksumMismatch{
				Bucket: bucket, Expected: expected, Actual: actual, Add: a, Op: o,
			})
		}
	}
	return out, nil
}

// BucketChecksum 返回本地累计校验和（add + op，按 32 位回绕）。
func (db *DB) BucketChecksum(ctx context.Context, bucket string) (uint32, error) {
	var add, op int64
	err := db.sql.QueryRowContext(ctx,
		`SELECT add_checksum, op_checksum FROM ps_buckets WHERE name = ?`, bucket).Scan(&add, &op)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return ck32(add) + ck32(op), nil
}

// ResetBucket 清空某个 bucket 的 oplog 与游标，使其下次从 after=0 重新同步。
func (db *DB) ResetBucket(ctx context.Context, bucket string) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := resetBucketTx(ctx, tx, bucket); err != nil {
		return err
	}
	return tx.Commit()
}

// TargetCheckpoint 返回本地写入所要求的 write checkpoint（ps_kv.target_checkpoint_request_id）。
//
// 上传成功后调用 CompleteCrud(writeCheckpoint) 写入该值：服务端必须先把这些本地写回显，
// 才能应用新的 checkpoint（否则本地刚写的行会先被旧数据覆盖，出现"闪烁"）。
func (db *DB) TargetCheckpoint(ctx context.Context) (protocol.OpID, error) {
	v, err := db.Meta(ctx, keyTargetCheckpoint)
	if err != nil || v == "" {
		return 0, err
	}
	var id protocol.OpID
	if err := id.UnmarshalJSON([]byte(strconv.Quote(v))); err != nil {
		return 0, nil // 非法值当没有
	}
	return id, nil
}

// ClearTargetCheckpoint 清除 write checkpoint 要求（回显已到达）。
func (db *DB) ClearTargetCheckpoint(ctx context.Context) error {
	_, err := db.sql.ExecContext(ctx, `DELETE FROM ps_kv WHERE key = ?`, keyTargetCheckpoint)
	return err
}

// keyTargetCheckpoint 是 ps_kv 中记录目标 write checkpoint 的键（core v14 的命名）。
const keyTargetCheckpoint = "target_checkpoint_request_id"

// OplogStats 返回某个 bucket 的 oplog 行数与游标（调试/测试用）。
func (db *DB) OplogStats(ctx context.Context, bucket string) (rows int64, lastApplied protocol.OpID, err error) {
	var id, last int64
	err = db.sql.QueryRowContext(ctx,
		`SELECT id, last_applied_op FROM ps_buckets WHERE name = ?`, bucket).Scan(&id, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if err = db.sql.QueryRowContext(ctx,
		`SELECT count(*) FROM ps_oplog WHERE bucket = ?`, id).Scan(&rows); err != nil {
		return 0, 0, err
	}
	return rows, protocol.OpID(last), nil
}
