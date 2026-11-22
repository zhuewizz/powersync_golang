package powersync

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zhuweizz/powersync_golang/internal/core"
)

// ColumnType 是用户表的列类型。
type ColumnType string

const (
	TextColumn    ColumnType = "TEXT"
	IntegerColumn ColumnType = "INTEGER"
	RealColumn    ColumnType = "REAL"
	NumericColumn ColumnType = "NUMERIC"
	BlobColumn    ColumnType = "BLOB"
)

// Column 是表的一列。主键固定名为 id（TEXT），无需在此声明。
type Column struct {
	Name string
	Type ColumnType
}

// Index 是表上的索引。
type Index struct {
	Name    string
	Columns []string
}

// Table 是一张同步表。
//
// LocalOnly 为 true 时只在本机存在：不参与同步，本地写入也不会进上传队列。
// InsertOnly 为 true 时本地只允许插入（当前实现仅记录该标记，未强制校验）。
type Table struct {
	Name string
	// ViewName 是对外暴露的视图名（JSON 表默认与 Name 相同）。
	ViewName string
	Columns  []Column
	Indexes  []Index
	// Direct 为 true 时建真实列式表（官方 core 的 alpha 特性）；默认 false = JSON 表 + 视图。
	Direct     bool
	LocalOnly  bool
	InsertOnly bool
	// IncludeMetadata 为 true 时视图多出 _metadata / _deleted 列，
	// 写操作可通过它们给 ps_crud 条目附带元数据。
	IncludeMetadata bool
}

// Schema 是一组表定义。
type Schema struct {
	tables []Table
}

// NewSchema 创建 schema。
func NewSchema(tables ...Table) *Schema { return &Schema{tables: tables} }

// Tables 返回表定义副本。
func (s *Schema) Tables() []Table {
	out := make([]Table, len(s.tables))
	copy(out, s.tables)
	return out
}

// LoadSchemaJSON 解析官方 SDK 的 Schema.toJSON() 形状：
//
//	{"tables":[{"name":"t","columns":[{"name":"a","type":"TEXT"}],
//	            "indexes":[{"name":"i","columns":[{"name":"a","ascending":true}]}],
//	            "local_only":false,"insert_only":false}],"raw_tables":[]}
//
// 这样可以直接把 JS/Dart 端导出的 schema 喂给 Go（见 scripts/js-reference/dump-schema.mjs）。
func LoadSchemaJSON(data []byte) (*Schema, error) {
	cs, err := core.ParseSchema(data)
	if err != nil {
		return nil, err
	}
	if len(cs.Tables) == 0 {
		return nil, errors.New("powersync: schema 不能为空")
	}
	tables := make([]Table, 0, len(cs.Tables))
	for _, t := range cs.Tables {
		cols := make([]Column, 0, len(t.Columns))
		for _, c := range t.Columns {
			if c.Name == core.PrimaryKeyColumn {
				continue // id 主键由实现负责
			}
			cols = append(cols, Column{Name: c.Name, Type: ColumnType(c.Type)})
		}
		idx := make([]Index, 0, len(t.Indexes))
		for _, i := range t.Indexes {
			names := make([]string, 0, len(i.Columns))
			for _, c := range i.Columns {
				names = append(names, c.Name)
			}
			idx = append(idx, Index{Name: i.Name, Columns: names})
		}
		tables = append(tables, Table{
			Name: t.Name, ViewName: t.ViewNameRaw, Columns: cols, Indexes: idx,
			Direct: t.Direct, LocalOnly: t.LocalOnly, InsertOnly: t.InsertOnly,
			IncludeMetadata: t.IncludeMetadata,
		})
	}
	return &Schema{tables: tables}, nil
}

// MarshalJSON 输出与官方 SDK 兼容的 schema JSON（便于调试与跨语言复用）。
func (s *Schema) MarshalJSON() ([]byte, error) {
	type wireColumn struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	type wireIndexColumn struct {
		Name      string `json:"name"`
		Ascending bool   `json:"ascending"`
		Type      string `json:"type,omitempty"`
	}
	type wireIndex struct {
		Name    string            `json:"name"`
		Columns []wireIndexColumn `json:"columns"`
	}
	type wireTable struct {
		Name       string       `json:"name"`
		ViewName   string       `json:"view_name,omitempty"`
		Columns    []wireColumn `json:"columns"`
		Indexes    []wireIndex  `json:"indexes"`
		Direct     bool         `json:"direct,omitempty"`
		LocalOnly  bool         `json:"local_only"`
		InsertOnly bool         `json:"insert_only"`
		Metadata   bool         `json:"include_metadata,omitempty"`
	}
	out := struct {
		Tables []wireTable `json:"tables"`
	}{Tables: make([]wireTable, 0, len(s.tables))}
	for _, t := range s.tables {
		wt := wireTable{Name: t.Name, ViewName: t.ViewName, LocalOnly: t.LocalOnly,
			InsertOnly: t.InsertOnly, Direct: t.Direct, Metadata: t.IncludeMetadata}
		for _, c := range t.Columns {
			wt.Columns = append(wt.Columns, wireColumn{Name: c.Name, Type: string(c.Type)})
		}
		for _, i := range t.Indexes {
			wi := wireIndex{Name: i.Name}
			for _, c := range i.Columns {
				wi.Columns = append(wi.Columns, wireIndexColumn{Name: c, Ascending: true})
			}
			wt.Indexes = append(wt.Indexes, wi)
		}
		out.Tables = append(out.Tables, wt)
	}
	return json.Marshal(out)
}

// toCore 转换成内部 schema。
func (s *Schema) toCore() (*core.Schema, error) {
	if s == nil {
		return nil, nil
	}
	if len(s.tables) == 0 {
		return nil, errors.New("powersync: schema 不能为空")
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	cs, err := core.ParseSchema(data)
	if err != nil {
		return nil, fmt.Errorf("powersync: schema 不可用: %w", err)
	}
	return cs, nil
}
