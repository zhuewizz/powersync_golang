package core

import (
	"context"
	"fmt"
)

// ClearAll 清空本地数据：
//   - 删除所有用户表的行（JSON 表删 ps_data__* / ps_data_local__*，direct 表逐表 DELETE）；
//   - 清空 ps_oplog / ps_crud / ps_updated_rows / ps_buckets / ps_stream_subscriptions / ps_sync_state；
//   - 重置游标与 last_op_id；保留 schema、client_id、ps_tx。
//
// clearLocal=false 时保留 local_only 表的数据（对应官方 disconnectAndClear 的默认行为）。
// 调用方应先 Disconnect，避免同步循环同时写入。
func (db *DB) ClearAll(ctx context.Context, clearLocal bool) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	s := db.Schema()
	if s != nil {
		for i := range s.Tables {
			t := &s.Tables[i]
			if t.LocalOnly && !clearLocal {
				continue
			}
			var stmt string
			if t.Direct {
				stmt = fmt.Sprintf(`DELETE FROM %s`, quoteIdent(t.Name))
			} else {
				stmt = fmt.Sprintf(`DELETE FROM %s`, quoteIdent(t.InternalName()))
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("core: 清空 %s: %w", t.Name, err)
			}
		}
	}

	for _, stmt := range []string{
		`DELETE FROM ps_oplog`,
		`DELETE FROM ps_crud`,
		`DELETE FROM ps_updated_rows`,
		`DELETE FROM ps_buckets`,
		`DELETE FROM ps_stream_subscriptions`,
		`DELETE FROM ps_sync_state`,
		`DELETE FROM ps_untyped`,
		`DELETE FROM ps_kv WHERE key IN ('last_op_id', 'target_checkpoint_request_id')`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("core: %s: %w", stmt, err)
		}
	}
	if err := bumpVersionTx(ctx, tx, []string{""}); err != nil {
		return err
	}
	return tx.Commit()
}
