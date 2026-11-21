// Package core 实现本地 SQLite 的 PowerSync 兼容层（对齐 powersync-sqlite-core 的
// 内部表、视图与触发器语义，但不依赖 C 扩展）。
//
// 与官方 core 的差异（有意为之，见 AGENTS.md §5.1）：
//   - 同步应用期的"触发器静默"用普通表 ps_internal_apply 做守卫，而不是 C 侧全局变量；
//   - ps_crud 用普通表 + 触发器写入，而不是 core 的 crud 虚拟表；
//   - ps_untyped 只建表保持结构兼容，schema 变更时的搬迁逻辑尚未实现。
package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ColumnType 是 schema 中的列类型（沿用 PowerSync schema JSON 的大写写法）。
type ColumnType string

const (
	TypeText    ColumnType = "TEXT"
	TypeInteger ColumnType = "INTEGER"
	TypeReal    ColumnType = "REAL"
	TypeNumeric ColumnType = "NUMERIC"
	TypeBlob    ColumnType = "BLOB"
)

// Column 是用户表的一列。
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// IndexColumn 是索引中的一列。
type IndexColumn struct {
	Name      string `json:"name"`
	Ascending *bool  `json:"ascending,omitempty"`
	Type      string `json:"type,omitempty"`
}

// Index 是用户表上的索引。
type Index struct {
	Name    string        `json:"name"`
	Columns []IndexColumn `json:"columns"`
}

// Table 是 schema 中的一张用户表。
//
// 默认（Direct=false）与官方 SDK 一致：数据以 JSON 存在 `ps_data__<name>`（本地表为
// `ps_data_local__<name>`），对外暴露同名**视图**（可用 ViewName 改名），本地写通过
// INSTEAD OF 触发器转发。Direct=true 时才建真实列式表（core 的 alpha 特性）。
type Table struct {
	Name              string   `json:"name"`
	ViewNameRaw       string   `json:"view_name,omitempty"`
	Columns           []Column `json:"columns"`
	Indexes           []Index  `json:"indexes"`
	Direct            bool     `json:"direct,omitempty"`
	LocalOnly         bool     `json:"local_only"`
	InsertOnly        bool     `json:"insert_only"`
	IgnoreEmptyUpdate bool     `json:"ignore_empty_update"`
	// IncludeMetadata 为 true 时视图多出 _metadata / _deleted 两列，
	// 写操作可以把 NEW._metadata 带进 ps_crud（对应官方 include_metadata）。
	IncludeMetadata bool `json:"include_metadata"`
}

// PrimaryKeyColumn 是本实现假设的主键列名（与 Dart/JS SDK 的约定一致）。
const PrimaryKeyColumn = "id"

// ViewName 返回对外的视图名（默认与表名相同，与 core 的 view_name() 一致）。
func (t *Table) ViewName() string {
	if t.ViewNameRaw != "" {
		return t.ViewNameRaw
	}
	return t.Name
}

// InternalName 返回 JSON 表的数据落盘表名（core 的 internal_name()）。
func (t *Table) InternalName() string {
	if t.LocalOnly {
		return "ps_data_local__" + t.Name
	}
	return "ps_data__" + t.Name
}

// Schema 是一组用户表定义。JSON 形状与官方 SDK 的 Schema.toJSON() 一致：
//
//	{"tables":[{"name":"t","columns":[{"name":"a","type":"TEXT"}],"indexes":[...],
//	            "local_only":false,"insert_only":false,"direct":false}],"raw_tables":[]}
type Schema struct {
	Tables []Table `json:"tables"`
}

// ParseSchema 解析官方 schema JSON。
func ParseSchema(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("core: parse schema: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate 检查 schema 是否可落地：表名/列名唯一、类型已知、不缺少 id 列。
func (s *Schema) Validate() error {
	seen := map[string]struct{}{}
	for i := range s.Tables {
		t := &s.Tables[i]
		if t.Name == "" {
			return fmt.Errorf("core: schema table %d 缺少 name", i)
		}
		if _, dup := seen[t.Name]; dup {
			return fmt.Errorf("core: schema 重复表名 %q", t.Name)
		}
		if !isSimpleIdent(t.Name) {
			return fmt.Errorf("core: 表名 %q 含特殊字符（触发器需要裸标识符）", t.Name)
		}
		seen[t.Name] = struct{}{}
		if t.ViewNameRaw != "" && !isSimpleIdent(t.ViewNameRaw) {
			return fmt.Errorf("core: 表 %q 的 view_name %q 含特殊字符", t.Name, t.ViewNameRaw)
		}

		cols := map[string]struct{}{}
		hasID := false
		for _, c := range t.Columns {
			if c.Name == "" {
				return fmt.Errorf("core: 表 %q 有空列名", t.Name)
			}
			if _, dup := cols[c.Name]; dup {
				return fmt.Errorf("core: 表 %q 重复列 %q", t.Name, c.Name)
			}
			// SQLite 的触发器语法只支持 NEW.<裸标识符>，因此列名必须足够简单。
			if !isSimpleIdent(c.Name) {
				return fmt.Errorf("core: 表 %q 列名 %q 含特殊字符（触发器需要裸标识符）", t.Name, c.Name)
			}
			cols[c.Name] = struct{}{}
			if c.Name == PrimaryKeyColumn {
				hasID = true
			}
			switch c.Type {
			case TypeText, TypeInteger, TypeReal, TypeNumeric, TypeBlob:
			default:
				return fmt.Errorf("core: 表 %q 列 %q 类型未知 %q", t.Name, c.Name, c.Type)
			}
		}
		if !hasID {
			// 本实现要求每张表都有 id 列（PowerSync 同步协议按 id 定位行）。
			t.Columns = append([]Column{{Name: PrimaryKeyColumn, Type: TypeText}}, t.Columns...)
		}

		// SQLite 的索引与表共用一个命名空间：AppSchema 里出现过索引名与表名相同的情况
		// （如 depart 表上的 depart 索引），需要改名避免 "already a table named"。
		for j := range t.Indexes {
			idx := &t.Indexes[j]
			if idx.Name == "" {
				idx.Name = t.Name + "_" + strings.Join(indexColumnNames(idx), "_")
			}
			if _, clash := seen[idx.Name]; clash || idx.Name == t.Name {
				idx.Name = t.Name + "_" + idx.Name
			}
			seen[idx.Name] = struct{}{}
		}
	}
	return nil
}

func indexColumnNames(i *Index) []string {
	out := make([]string, len(i.Columns))
	for k, c := range i.Columns {
		out[k] = c.Name
	}
	return out
}

// Table 返回指定名字的表。
func (s *Schema) Table(name string) (*Table, bool) {
	for i := range s.Tables {
		if s.Tables[i].Name == name {
			return &s.Tables[i], true
		}
	}
	return nil, false
}

// ColumnNames 返回表的所有列名（含 id）。
func (t *Table) ColumnNames() []string {
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		out = append(out, c.Name)
	}
	return out
}

// Column 返回指定列。
func (t *Table) Column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// DDL 返回建表、建索引、建视图、建触发器所需的全部语句（幂等，可重复执行）。
//
// JSON 表（默认）按官方 core 的形状：
//
//	CREATE TABLE ps_data__todos(id TEXT PRIMARY KEY NOT NULL, data TEXT);
//	CREATE VIEW "todos"("id","title") AS SELECT id,
//	    CAST(json_extract(data, '$.title') as TEXT) FROM "ps_data__todos" -- powersync-auto-generated
//	CREATE TRIGGER "ps_crud_insert_todos" INSTEAD OF INSERT ON "todos" ... END;
func (t *Table) DDL() []string {
	var out []string
	out = append(out, t.createStorageSQL())
	for _, idx := range t.Indexes {
		out = append(out, t.indexSQL(idx))
	}
	if t.Direct {
		out = append(out,
			t.directTriggerSQL("insert", "AFTER INSERT ON"),
			t.directTriggerSQL("update", "AFTER UPDATE ON"),
			t.directTriggerSQL("delete", "AFTER DELETE ON"),
		)
		return out
	}

	out = append(out, t.createViewSQL())
	out = append(out, t.viewTriggerSQL("insert", "INSERT"))
	if !t.InsertOnly {
		out = append(out, t.viewTriggerSQL("update", "UPDATE"))
		out = append(out, t.viewTriggerSQL("delete", "DELETE"))
		if t.IncludeMetadata {
			out = append(out, t.deleteViaUpdateTriggerSQL())
		}
	}
	return out
}

// createStorageSQL 建数据落盘对象：JSON 表建 ps_data__*，direct 表直接建列式表。
func (t *Table) createStorageSQL() string {
	if !t.Direct {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY NOT NULL, data TEXT);",
			quoteIdent(t.InternalName()))
	}
	cols := make([]string, 0, len(t.Columns)+1)
	cols = append(cols, quoteIdent(PrimaryKeyColumn)+" TEXT PRIMARY KEY NOT NULL /* ps-managed */")
	for _, c := range t.Columns {
		if c.Name == PrimaryKeyColumn {
			continue
		}
		cols = append(cols, quoteIdent(c.Name)+" "+string(c.Type))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) STRICT;",
		quoteIdent(t.Name), strings.Join(cols, ", "))
}

// indexSQL JSON 表的索引建在 ps_data__* 上（与 core 一致，索引 json_extract 表达式）。
func (t *Table) indexSQL(i Index) string {
	cols := make([]string, 0, len(i.Columns))
	for _, c := range i.Columns {
		var expr string
		if t.Direct {
			expr = quoteIdent(c.Name)
		} else {
			typ := c.Type
			if typ == "" {
				typ = string(TypeText)
			}
			expr = fmt.Sprintf("CAST(json_extract(data, '$.%s') as %s)", c.Name, typ)
		}
		if c.Ascending != nil && !*c.Ascending {
			expr += " DESC"
		} else {
			expr += " ASC"
		}
		cols = append(cols, expr)
	}
	target := t.Name
	if !t.Direct {
		target = t.InternalName()
	}
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s);",
		quoteIdent(i.Name), quoteIdent(target), strings.Join(cols, ", "))
}

// createViewSQL 建对外视图（列名与列顺序与 core 的 powersync_view_sql 一致）。
func (t *Table) createViewSQL() string {
	names := make([]string, 0, len(t.Columns)+1)
	exprs := make([]string, 0, len(t.Columns)+1)
	names = append(names, quoteIdent("id"))
	exprs = append(exprs, "id")
	for _, c := range t.Columns {
		if c.Name == PrimaryKeyColumn {
			continue
		}
		names = append(names, quoteIdent(c.Name))
		exprs = append(exprs, fmt.Sprintf("CAST(json_extract(data, '$.%s') as %s)", c.Name, c.Type))
	}
	if t.IncludeMetadata {
		// 与 core 一致：_metadata / _deleted 在视图里是 NULL，写入时由 NEW._metadata 取值。
		names = append(names, quoteIdent("_metadata"), quoteIdent("_deleted"))
		exprs = append(exprs, "NULL", "NULL")
	}
	return fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s(%s) AS SELECT %s FROM %s -- powersync-auto-generated;",
		quoteIdent(t.ViewName()), strings.Join(names, ","), strings.Join(exprs, ", "),
		quoteIdent(t.InternalName()))
}

// viewTriggerSQL 生成 JSON 表上的 INSTEAD OF 触发器（与 core 的 views.rs 同形状）：
// 先写 ps_data__*（整行 JSON），再按需写 ps_crud（data 用 powersync_diff 计算）。
func (t *Table) viewTriggerSQL(kind, sqlVerb string) string {
	view := quoteIdent(t.ViewName())
	internal := quoteIdent(t.InternalName())
	guard := ""
	if !t.LocalOnly {
		guard = fmt.Sprintf(" WHEN NOT EXISTS (SELECT 1 FROM %s)", guardTable)
	}
	if kind == "update" && t.IncludeMetadata {
		// 带 metadata 的删除走 delete2 触发器，不在这里发 PATCH。
		guard += " AND NEW._deleted IS NOT TRUE"
	}
	var body string
	switch kind {
	case "insert":
		body = fmt.Sprintf("  INSERT INTO %s SELECT NEW.id, %s;\n", internal, t.jsonObjectExpr("NEW"))
		if !t.LocalOnly {
			body += t.crudInsertSQL("PUT", "NEW", t.jsonObjectExpr("NEW"))
		}
	case "update":
		body = fmt.Sprintf("  UPDATE %s SET data = %s WHERE id = NEW.id;\n", internal, t.jsonObjectExpr("NEW"))
		if !t.LocalOnly {
			body += t.crudUpdateSQL(t.jsonObjectExpr("OLD"), t.jsonObjectExpr("NEW"))
		}
	case "delete":
		body = fmt.Sprintf("  DELETE FROM %s WHERE id = OLD.id;\n", internal)
		if !t.LocalOnly {
			body += t.crudDeleteSQL("OLD")
		}
	}
	return fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s INSTEAD OF %s ON %s%s BEGIN\n%sEND;",
		quoteIdent(fmt.Sprintf("ps_crud_%s_%s", kind, t.Name)), sqlVerb, view, guard, body)
}

// directTriggerSQL 生成 direct 表上的 AFTER 触发器（表达式与 JSON 表一致）。
func (t *Table) directTriggerSQL(kind, timing string) string {
	var body string
	if !t.LocalOnly {
		switch kind {
		case "insert":
			body = t.crudInsertSQL("PUT", "NEW", t.jsonObjectExpr("NEW"))
		case "update":
			body = t.crudUpdateSQL(t.jsonObjectExpr("OLD"), t.jsonObjectExpr("NEW"))
		case "delete":
			body = t.crudDeleteSQL("OLD")
		}
	}
	body += "  UPDATE ps_internal_version SET v = v + 1;\n"
	return fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s %s %s BEGIN\n%sEND;",
		quoteIdent(fmt.Sprintf("ps_crud_%s_%s", kind, t.Name)), timing, quoteIdent(t.Name), body)
}

// crudInsertSQL/crudUpdateSQL/crudDeleteSQL 往 powersync_crud 写一行，
// 由 AFTER INSERT 桥接触发器搬进 ps_crud（等价于官方 core 的 crud 虚拟表）。
func (t *Table) crudInsertSQL(op, rowRef, data string) string {
	return t.insertCrudStmt(op, rowRef, "json(powersync_diff('{}', "+data+"))")
}

func (t *Table) crudUpdateSQL(oldData, newData string) string {
	return t.insertCrudStmt("PATCH", "NEW", "json(powersync_diff("+oldData+", "+newData+"))")
}

func (t *Table) crudDeleteSQL(rowRef string) string {
	return t.insertCrudStmt("DELETE", rowRef, "")
}

// insertCrudStmt 生成 `INSERT INTO powersync_crud(op,id,type,data,metadata) VALUES(...)`。
func (t *Table) insertCrudStmt(op, rowRef, dataExpr string) string {
	data := "NULL"
	if dataExpr != "" {
		data = dataExpr
	}
	meta := "NULL"
	if t.IncludeMetadata {
		meta = rowRef + "._metadata"
	}
	return fmt.Sprintf(
		"  INSERT INTO powersync_crud(op, id, type, data, metadata) VALUES(%s, %s.id, %s, %s, %s);\n",
		sqlString(op), rowRef, sqlString(t.Name), data, meta)
}

// deleteViaUpdateTriggerSQL：include_metadata 时支持 "UPDATE t SET _deleted = TRUE" 触发删除。
func (t *Table) deleteViaUpdateTriggerSQL() string {
	view := quoteIdent(t.ViewName())
	internal := quoteIdent(t.InternalName())
	return fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s INSTEAD OF UPDATE ON %s WHEN NEW._deleted IS TRUE BEGIN\n"+
		"  DELETE FROM %s WHERE id = OLD.id;\n"+
		"  %sEND;",
		quoteIdent("ps_crud_delete2_"+t.Name), view, internal,
		strings.TrimSpace(t.insertCrudStmt("DELETE", "NEW", ""))+"\n")
}

// jsonObjectExpr 生成 `json_object('col', <prefix>.col, ...)`。
//
// 超过 50 个键值对时按 core 的做法分段并用 powersync_json_merge 合并（避开 SQLite 的函数参数上限）。
func (t *Table) jsonObjectExpr(prefix string) string {
	const maxArgs = 50 // 每个 json_object 最多 50 个键值对
	var chunks []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			chunks = append(chunks, "json_object("+strings.Join(cur, ", ")+")")
			cur = nil
		}
	}
	for _, c := range t.Columns {
		if c.Name == PrimaryKeyColumn {
			continue // id 单独存列，不进 JSON（与 core 的 columns 不含 id 一致）
		}
		cur = append(cur, fmt.Sprintf("%s,%s.%s", sqlString(c.Name), prefix, c.Name))
		if len(cur) == maxArgs*2 {
			flush()
		}
	}
	flush()
	if len(chunks) == 0 {
		return "json_object()"
	}
	if len(chunks) == 1 {
		return chunks[0]
	}
	return "powersync_json_merge(" + strings.Join(chunks, ", ") + ")"
}

// sqlString 生成 SQL 字符串字面量（'...'，内部单引号转义）。
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// isSimpleIdent 判断是否是可用在 NEW.<name> 位置上的裸标识符。
func isSimpleIdent(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
