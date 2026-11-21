package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// ReplaceSchema 用新 schema 重建本地用户表，并在重建前后用 ps_untyped 搬运数据。
//
// 行为（对齐 core 的 move_from_ps_untyped / move_from_json 思路）：
//  1. 把当前所有用户表的数据暂存进 ps_untyped(type=表名, id, data=整行 JSON)；
//  2. 删除旧的视图 / 触发器 / 索引 / 落盘表；
//  3. 按新 schema 重建；
//  4. 把暂存数据写回（JSON 表直接回填 data；direct 表按列回填）；
//  5. 清空 ps_untyped / ps_updated_rows。
//
// 说明：
//   - JSON 表加列本来就不需要迁移（视图列是动态的），这里重建视图即可；
//   - 删除的列/表会丢数据（与 core 的 DROP COLUMN 行为一致）；
//   - ps_crud（待上传队列）不受影响。
func (db *DB) ReplaceSchema(ctx context.Context, next *Schema) error {
	return db.replaceSchema(ctx, db.Schema(), next)
}

func (db *DB) replaceSchema(ctx context.Context, prev, next *Schema) error {
	if next == nil {
		return fmt.Errorf("core: ReplaceSchema 需要非空 schema")
	}
	if err := next.Validate(); err != nil {
		return err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. 暂存现有数据
	old := prev
	if old != nil {
		for i := range old.Tables {
			if err := stageTable(ctx, tx, &old.Tables[i]); err != nil {
				return fmt.Errorf("core: 暂存 %s: %w", old.Tables[i].Name, err)
			}
		}
		// 2. 删除旧对象
		for i := range old.Tables {
			if err := dropTableObjects(ctx, tx, &old.Tables[i]); err != nil {
				return fmt.Errorf("core: 删除 %s 的旧对象: %w", old.Tables[i].Name, err)
			}
		}
	}

	// 3. 建新对象
	db.setSchema(next)
	// DDL 失败时把 schema 指针还原，避免 DB 认为新 schema 已生效。
	rollbackSchema := true
	defer func() {
		if rollbackSchema {
			db.setSchema(prev)
		}
	}()
	for i := range next.Tables {
		for _, stmt := range next.Tables[i].DDL() {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("core: 建 %s: %w", next.Tables[i].Name, err)
			}
		}
	}

	// 4. 回填数据
	for i := range next.Tables {
		if err := db.restoreTable(ctx, tx, &next.Tables[i]); err != nil {
			return fmt.Errorf("core: 回填 %s: %w", next.Tables[i].Name, err)
		}
	}

	// 5. 清空暂存
	if _, err := tx.ExecContext(ctx, `DELETE FROM ps_untyped`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ps_updated_rows`); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	rollbackSchema = false
	return nil
}

// stageTable 把一张表的当前数据写进 ps_untyped（JSON 表直接抄 data，direct 表拼 json_object）。
func stageTable(ctx context.Context, tx *sql.Tx, t *Table) error {
	storage := quoteIdent(t.Name)
	if !t.Direct {
		storage = quoteIdent(t.InternalName())
	}
	// 表可能还没建（schema 里新增但从未同步过）——跳过。
	var exists int
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`,
		t.InternalName()).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		// 老版本可能直接建了同名真实表；也检查一下。
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`,
			t.Name).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return nil
		}
	}
	if t.Direct {
		// direct 表：把列拼成 JSON 存进 ps_untyped。
		var cols []string
		for _, c := range t.Columns {
			cols = append(cols, fmt.Sprintf("%s,%s", sqlString(c.Name), quoteIdent(c.Name)))
		}
		stmt := fmt.Sprintf(
			`INSERT OR REPLACE INTO ps_untyped(type, id, data) SELECT ?, id, json_object(%s) FROM %s`,
			strings.Join(cols, ", "), storage)
		_, err := tx.ExecContext(ctx, stmt, t.Name)
		return err
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO ps_untyped(type, id, data) SELECT ?, id, data FROM `+storage, t.Name)
	return err
}

// restoreTable 把 ps_untyped 中该表的数据写回新对象。
func (db *DB) restoreTable(ctx context.Context, tx *sql.Tx, t *Table) error {
	if !t.Direct {
		_, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO `+quoteIdent(t.InternalName())+
				`(id, data) SELECT id, data FROM ps_untyped WHERE type = ?`, t.Name)
		return err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id, data FROM ps_untyped WHERE type = ?`, t.Name)
	if err != nil {
		return err
	}
	type staged struct {
		id   string
		data string
	}
	var items []staged
	for rows.Next() {
		var s staged
		if err := rows.Scan(&s.id, &s.data); err != nil {
			rows.Close()
			return err
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, s := range items {
		if err := db.upsertRowTx(ctx, tx, t.Name, s.id, []byte(s.data)); err != nil {
			return err
		}
	}
	return nil
}

// dropTableObjects 删除一张表的视图/触发器/索引/落盘表。
func dropTableObjects(ctx context.Context, tx *sql.Tx, t *Table) error {
	stmts := []string{
		fmt.Sprintf(`DROP TRIGGER IF EXISTS %s`, quoteIdent("ps_crud_insert_"+t.Name)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS %s`, quoteIdent("ps_crud_update_"+t.Name)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS %s`, quoteIdent("ps_crud_delete_"+t.Name)),
	}
	for _, idx := range t.Indexes {
		stmts = append(stmts, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, quoteIdent(idx.Name)))
	}
	if t.Direct {
		stmts = append(stmts, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdent(t.Name)))
	} else {
		stmts = append(stmts,
			fmt.Sprintf(`DROP VIEW IF EXISTS %s`, quoteIdent(t.ViewName())),
			fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdent(t.InternalName())))
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// SchemaJSON 返回当前 schema 的规范 JSON（用于 ps_kv 记录，判断是否需要迁移）。
func (db *DB) SchemaJSON() (string, error) {
	s := db.Schema()
	if s == nil {
		return "", nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
