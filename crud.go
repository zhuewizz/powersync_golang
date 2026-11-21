package powersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

// UpdateType 是本地变更类型。
type UpdateType string

const (
	// UpdateTypePut 是整行写入（本地 INSERT）。
	UpdateTypePut UpdateType = "PUT"
	// UpdateTypePatch 是部分更新（本地 UPDATE 只带变化列）。
	UpdateTypePatch UpdateType = "PATCH"
	// UpdateTypeDelete 是删除（本地 DELETE）。
	UpdateTypeDelete UpdateType = "DELETE"
)

// CrudEntry 是一条待上传的本地变更。
type CrudEntry struct {
	// ClientID 是本地自增序号（ps_crud.id）。
	ClientID int64
	// TxID 是同一次 WriteTransaction 的事务号（可能为 nil）。
	TxID *int64
	// Op 是变更类型。
	Op UpdateType
	// Table 是表名。
	Table string
	// ID 是行 id。
	ID string
	// Data 是变更数据：PUT 为全部非空列，PATCH 为变化列，DELETE 为 nil。
	Data map[string]any
	// Metadata 是写入时附带的行级元数据（需要 schema 开启 trackMetadata）。
	Metadata string
}

// CrudBatch 是一批待上传变更。
type CrudBatch struct {
	// Entries 是本次取出的变更（按 ClientID 升序）。
	Entries []CrudEntry
	// HaveMore 表示队列里还有更多数据。
	HaveMore bool

	db     *DB
	lastID int64
}

// Complete 把这一批变更出队（上传成功后调用）。
//
// writeCheckpoint 可选：服务端返回的 write checkpoint，用于让服务端把本地写入回显回来。
func (b *CrudBatch) Complete(ctx context.Context, writeCheckpoint ...string) error {
	if b == nil || b.db == nil {
		return errors.New("powersync: nil CrudBatch")
	}
	return b.db.completeCrud(ctx, b.lastID, writeCheckpoint)
}

// CrudTransaction 是一次事务内的变更集合（用 GetNextCrudTransaction 取）。
type CrudTransaction struct {
	// TxID 是事务号；为 nil 表示这些变更没有显式事务。
	TxID *int64
	// Entries 是同一事务内的变更。
	Entries []CrudEntry

	db     *DB
	lastID int64
}

// Complete 把该事务的变更出队。
func (t *CrudTransaction) Complete(ctx context.Context, writeCheckpoint ...string) error {
	if t == nil || t.db == nil {
		return errors.New("powersync: nil CrudTransaction")
	}
	return t.db.completeCrud(ctx, t.lastID, writeCheckpoint)
}

// GetCrudBatch 取最多 limit 条待上传变更；队列为空时返回 nil。
func (db *DB) GetCrudBatch(ctx context.Context, limit int) (*CrudBatch, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	rows, err := db.store.CrudBatch(ctx, limit+1) // 多取一条判断 HaveMore
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	haveMore := len(rows) > limit
	if haveMore {
		rows = rows[:limit]
	}
	entries, err := toCrudEntries(rows)
	if err != nil {
		return nil, err
	}
	return &CrudBatch{
		Entries:  entries,
		HaveMore: haveMore,
		db:       db,
		lastID:   entries[len(entries)-1].ClientID,
	}, nil
}

// GetNextCrudTransaction 取队首**同一事务**的变更；队列为空时返回 nil。
//
// 需要事务级原子上传时用它（否则用 GetCrudBatch）。
func (db *DB) GetNextCrudTransaction(ctx context.Context) (*CrudTransaction, error) {
	batch, err := db.GetCrudBatch(ctx, 1000)
	if err != nil || batch == nil {
		return nil, err
	}
	txID := batch.Entries[0].TxID
	entries := batch.Entries
	for i, e := range batch.Entries {
		if !sameTx(txID, e.TxID) {
			entries = batch.Entries[:i]
			break
		}
	}
	return &CrudTransaction{
		TxID:    txID,
		Entries: entries,
		db:      db,
		lastID:  entries[len(entries)-1].ClientID,
	}, nil
}

// GetNextCrudEntry 取队首的一条变更；队列为空时返回 nil。
func (db *DB) GetNextCrudEntry(ctx context.Context) (*CrudEntry, error) {
	batch, err := db.GetCrudBatch(ctx, 1)
	if err != nil || batch == nil {
		return nil, err
	}
	e := batch.Entries[0]
	return &e, nil
}

func (db *DB) completeCrud(ctx context.Context, lastID int64, writeCheckpoint []string) error {
	var wc string
	if len(writeCheckpoint) > 0 {
		wc = writeCheckpoint[0]
	}
	var op protocol.OpID
	if wc != "" {
		parsed, err := parseOpID(wc)
		if err != nil {
			return fmt.Errorf("powersync: 非法 write checkpoint %q: %w", wc, err)
		}
		op = parsed
	}
	if err := db.store.CompleteCrud(ctx, lastID, op); err != nil {
		return err
	}
	db.store.NotifyDataChange()
	return nil
}

func toCrudEntries(rows []core.CrudEntry) ([]CrudEntry, error) {
	out := make([]CrudEntry, 0, len(rows))
	for _, r := range rows {
		e := CrudEntry{
			ClientID: r.ID,
			TxID:     r.TxID,
			Op:       UpdateType(r.Op),
			Table:    r.Table,
			ID:       r.RowID,
			Metadata: r.Metadata,
		}
		if len(r.Data) > 0 && string(r.Data) != "null" {
			var m map[string]any
			if err := json.Unmarshal(r.Data, &m); err != nil {
				return nil, fmt.Errorf("powersync: ps_crud.id=%d 的 data 不是对象: %w", r.ID, err)
			}
			e.Data = m
		}
		out = append(out, e)
	}
	return out, nil
}

func parseOpID(s string) (protocol.OpID, error) {
	var id protocol.OpID
	if err := id.UnmarshalJSON([]byte(strconv.Quote(s))); err != nil {
		return 0, err
	}
	return id, nil
}

func sameTx(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
