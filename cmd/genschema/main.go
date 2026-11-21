// Command genschema 从「PowerSync 同步规则 + PostgREST OpenAPI」生成客户端 schema。
//
// 为什么需要它：PowerSync 要求客户端声明 schema（本地有哪些表、每张表哪些列），
// 而 syncRules.yaml 只告诉我们"服务端会下发哪些表"，没有列信息。
// PostgREST 的 OpenAPI（GET / 根路径）是现成的列信息源，两者结合即可自动生成。
//
// 用法：
//
//	go run ./cmd/genschema \
//	    -rules E:/cutter/syncRules.yaml \
//	    -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
//	    -box D1-2 -org ORG001 \
//	    -out-go ./schema_cutter.go \
//	    -out-json ./out/schema-cutter.json
//
// 生成物：
//   - <out-go>：Go schema 函数（-pkg main 生成到示例/业务项目；-pkg powersync 生成到 SDK 根包）
//   - <out-json>：官方 Schema.toJSON() 形状，给 -schema 参数/其它语言/CI 用
//
// 例（生成到示例项目，SDK 根包保持项目无关）：
//
//	go run ./cmd/genschema -rules E:/cutter/syncRules.yaml -pkg main \n//	    -out-go ./examples/todo/schema_cutter.go -out-json ./examples/todo/schema-cutter.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	powersync "msmkSync"
	"msmkSync/internal/psclient"
)

func main() {
	var (
		rulesF   = flag.String("rules", "E:/cutter/syncRules.yaml", "syncRules.yaml 路径")
		openapiF = flag.String("openapi-file", "", "已有的 OpenAPI JSON（给了就不再请求）")
		restBase = flag.String("rest", "http://120.24.7.22:8089/rest", "PostgREST base（取 OpenAPI）")
		rpc      = flag.String("rpc", "http://120.24.7.22:8089/rest/rpc/fetch_credentials", "fetch_credentials RPC")
		box      = flag.String("box", "D1-2", "box_code")
		org      = flag.String("org", "ORG001", "org_code")
		token    = flag.String("token", "", "JWT（不给则用 RPC 换）")
		outGo    = flag.String("out-go", "./schema_cutter.go", "生成的 Go 文件")
		fnName   = flag.String("func", "", "导出的 schema 函数名（默认：pkg=powersync → CutterSchema，其它 → cutterSchema）")
		outJSON  = flag.String("out-json", "./out/schema-cutter.json", "生成的 schema JSON")
		pkgName  = flag.String("pkg", "powersync", "Go 包名（生成到别的项目/示例里就写 main）")
		fallback = flag.String("fallback", "./out/schema.json", "OpenAPI 缺表时的备用 schema JSON（官方 AppSchema 导出）")
		extra    = flag.String("extra", "role", "额外表（逗号分隔），如部署中的规则比本地 yaml 多出的表")
		streamF  = flag.String("stream", "", "抓包 NDJSON（可选）：给 OpenAPI 也没有的表推列（如 role）")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	raw, err := os.ReadFile(*rulesF)
	if err != nil {
		log.Fatalf("读同步规则: %v", err)
	}
	order, pgSchemas, err := parseSyncRules(string(raw))
	if err != nil {
		log.Fatalf("解析同步规则: %v", err)
	}
	// 同名的表出现在多个 PG schema 里是危险的（客户端没有 schema 概念，会撞成一张表）。
	for name, schemas := range pgSchemas {
		if len(schemas) > 1 {
			fmt.Printf("⚠️ 表 %s 同时出现在多个 PG schema（%s）：客户端会合并成一张表 %s\n",
				name, strings.Join(schemas, ", "), name)
		}
	}
	fmt.Printf("同步规则: %s（%d 张表）\n", *rulesF, len(order))

	openapi, err := loadOpenAPI(ctx, *openapiF, *restBase, *rpc, *box, *org, *token)
	if err != nil {
		log.Fatalf("加载 OpenAPI: %v", err)
	}
	fmt.Printf("OpenAPI: %d 张表有列定义\n", len(openapi))

	// 同步规则里的 `AS i` / `AS t` 这类别名：服务端会用**别名**当 object_type，
	// 所以客户端必须把别名也声明成表（列取被别名表的并集）。
	aliases := parseAliases(string(raw))
	if len(aliases) > 0 {
		names := make([]string, 0, len(aliases))
		for a := range aliases {
			names = append(names, a)
		}
		sortStrings(names)
		fmt.Printf("同步规则别名（需按表声明）：%s\n", strings.Join(names, ", "))
		for _, a := range names {
			for _, t := range aliases[a] {
				if _, ok := openapi[a]; ok {
					break
				}
				// 别名表的列 = 所有被别名表的列并集
				if def, ok := openapi[t]; ok {
					if openapi[a].Properties == nil {
						openapi[a] = openapiDef{Properties: map[string]propDef{}}
					}
					for name, p := range def.Properties {
						if _, exists := openapi[a].Properties[name]; !exists {
							openapi[a].Properties[name] = p
						}
					}
				}
			}
			order = append(order, a)
		}
	}

	// 部署中的规则可能比本地 yaml 新（例如服务端还发 role），可用 -extra 补上；
	// OpenAPI 里没有的表，用 -stream 抓包文件从实际数据推出列。
	for _, extra := range splitList(*extra) {
		if !contains(order, extra) {
			order = append(order, extra)
			fmt.Printf("额外表: %s\n", extra)
		}
	}
	inferred := map[string][]powersync.Column{}
	if *streamF != "" {
		inferred, err = inferFromStream(*streamF)
		if err != nil {
			log.Fatalf("解析抓包文件: %v", err)
		}
		fmt.Printf("抓包文件: %s（%d 张表可推断列）\n", *streamF, len(inferred))
	}

	cols := columnsForTables(order, openapi, *fallback, inferred)

	// 生成 Go 源码
	goSrc, jsonSrc, err := render(*pkgName, *fnName, *rulesF, order, pgSchemas, cols)
	if err != nil {
		log.Fatalf("生成: %v", err)
	}
	// 生成物先过 gofmt，保证 CI 的 gofmt -l 不会报警（注释对齐由它处理）。
	if formatted, err := format.Source([]byte(goSrc)); err == nil {
		goSrc = string(formatted)
	} else {
		log.Fatalf("生成代码无法 gofmt: %v", err)
	}
	if err := os.WriteFile(*outGo, []byte(goSrc), 0o644); err != nil {
		log.Fatalf("写 %s: %v", *outGo, err)
	}
	if err := os.MkdirAll(dir(*outJSON), 0o755); err != nil {
		log.Fatalf("-out-json 目录: %v", err)
	}
	if err := os.WriteFile(*outJSON, []byte(jsonSrc), 0o644); err != nil {
		log.Fatalf("写 %s: %v", *outJSON, err)
	}
	fmt.Printf("已生成:\n  %s（%d 张表 / %d 列）\n  %s\n",
		*outGo, len(order), countCols(cols), *outJSON)
}

// ---------------------------------------------------------------- 同步规则

var (
	fromRe  = regexp.MustCompile(`(?i)\bFROM\s+"?([A-Za-z_][A-Za-z0-9_]*)"?\.\s*"?([A-Za-z_][A-Za-z0-9_]*)"?`)
	aliasRe = regexp.MustCompile(`(?i)\bFROM\s+"?[A-Za-z_][A-Za-z0-9_]*"?\.\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)`)
)

// sqlKeywords 用于排除把 WHERE/ON 之类当成别名。
var sqlKeywords = map[string]bool{
	"where": true, "and": true, "or": true, "on": true, "join": true, "group": true,
	"order": true, "limit": true, "having": true, "union": true, "left": true, "right": true,
	"inner": true, "outer": true, "cross": true, "select": true, "as": true, "using": true,
}

// aliasInfo 记录 `FROM "s"."tbl" AS alias`：服务端会用**别名**作为 object_type，
// 所以客户端必须把别名也声明成表（列取被别名表的并集）。
func parseAliases(yaml string) map[string][]string {
	out := map[string][]string{}
	for _, line := range strings.Split(yaml, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, m := range aliasRe.FindAllStringSubmatch(line, -1) {
			table, alias := m[1], m[2]
			if alias == table || sqlKeywords[strings.ToLower(alias)] {
				continue
			}
			dup := false
			for _, t := range out[alias] {
				if t == table {
					dup = true
				}
			}
			if !dup {
				out[alias] = append(out[alias], table)
			}
		}
	}
	return out
}

// parseSyncRules 抽取所有 `FROM "schema"."table"` 里的表名（去重、保持出现顺序）。
//
// 只做这一件事就够了：bucket 划分、参数、过滤条件都是服务端的事，
// 客户端只关心"哪些表会出现"。
func parseSyncRules(yaml string) ([]string, map[string][]string, error) {
	seen := map[string]bool{}
	pgSchemas := map[string][]string{}
	var out []string
	for _, line := range strings.Split(yaml, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, m := range fromRe.FindAllStringSubmatch(line, -1) {
			pgSchema, table := m[1], m[2]
			// 客户端 schema 没有 PG schema（命名空间）的概念：只用表名，pg schema 仅作备注。
			if !contains(pgSchemas[table], pgSchema) {
				pgSchemas[table] = append(pgSchemas[table], pgSchema)
			}
			if seen[table] {
				continue
			}
			seen[table] = true
			out = append(out, table)
		}
	}
	if len(out) == 0 {
		return nil, nil, fmt.Errorf("没有匹配到 FROM \"schema\".\"table\"")
	}
	return out, pgSchemas, nil
}

// ---------------------------------------------------------------- OpenAPI

// propDef 是 OpenAPI definitions 里单个列的形态。
type propDef struct {
	Type   string `json:"type"`
	Format string `json:"format"`
}

type openapiDef struct {
	Properties map[string]propDef `json:"properties"`
}

func loadOpenAPI(ctx context.Context, file, restBase, rpc, box, org, token string) (map[string]openapiDef, error) {
	var raw []byte
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		raw = b
	} else {
		if token == "" {
			tok, err := psclient.FetchToken(ctx, rpc, box, org)
			if err != nil {
				return nil, fmt.Errorf("取 token: %w", err)
			}
			token = tok
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(restBase, "/")+"/", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s/ -> HTTP %d", restBase, resp.StatusCode)
		}
		var doc struct {
			Definitions map[string]openapiDef `json:"definitions"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
			return nil, err
		}
		return doc.Definitions, nil
	}
	var doc struct {
		Definitions map[string]openapiDef `json:"definitions"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return doc.Definitions, nil
}

// columnsForTables 为每张表确定列（名 + PowerSync 类型）。
//
// 优先级：OpenAPI > 备用 schema JSON > 只有 id（并告警）。
func columnsForTables(order []string, openapi map[string]openapiDef, fallbackFile string, inferred map[string][]powersync.Column) map[string][]powersync.Column {
	fb := map[string][]powersync.Column{}
	if fallbackFile != "" {
		if raw, err := os.ReadFile(fallbackFile); err == nil {
			if sch, err := powersync.LoadSchemaJSON(raw); err == nil {
				for _, t := range sch.Tables() {
					fb[t.Name] = t.Columns
				}
			}
		}
	}

	out := make(map[string][]powersync.Column, len(order))
	for _, table := range order {
		def, ok := openapi[table]
		if ok && len(def.Properties) > 0 {
			names := make([]string, 0, len(def.Properties))
			for name := range def.Properties {
				names = append(names, name)
			}
			sortStrings(names)
			cols := make([]powersync.Column, 0, len(names))
			for _, name := range names {
				if name == "id" {
					continue // 主键由实现负责
				}
				p := def.Properties[name]
				cols = append(cols, powersync.Column{Name: name, Type: psType(p.Format, p.Type)})
			}
			out[table] = cols
			continue
		}
		if cols, ok := fb[table]; ok {
			out[table] = cols
			continue
		}
		if cols, ok := inferred[table]; ok {
			out[table] = cols
			continue
		}
		fmt.Printf("⚠️ 表 %s 既不在 OpenAPI 也不在备用 schema 里：只同步 id（请补充定义）\n", table)
		out[table] = nil
	}
	return out
}

// psType 把 Postgres/OpenAPI 类型映射到 PowerSync 列类型。
func psType(format, typ string) powersync.ColumnType {
	f := strings.ToLower(format)
	t := strings.ToLower(typ)
	switch {
	case strings.Contains(f, "uuid"), strings.Contains(f, "character"), f == "text",
		strings.Contains(f, "timestamp"), strings.Contains(f, "date"), strings.Contains(f, "time"),
		strings.Contains(f, "json"), strings.Contains(f, "array"):
		return powersync.TextColumn
	case strings.Contains(f, "int"), strings.Contains(f, "serial"), strings.Contains(f, "bool"):
		return powersync.IntegerColumn
	case strings.Contains(f, "numeric"), strings.Contains(f, "decimal"),
		strings.Contains(f, "double"), strings.Contains(f, "real"):
		return powersync.RealColumn
	case strings.Contains(f, "bytea"), strings.Contains(f, "blob"):
		return powersync.BlobColumn
	}
	switch t {
	case "integer":
		return powersync.IntegerColumn
	case "number":
		return powersync.RealColumn
	case "boolean":
		return powersync.IntegerColumn
	}
	return powersync.TextColumn // string / array / object
}

// ---------------------------------------------------------------- 渲染

func render(pkg, fn, rulesFile string, order []string, pgSchemas map[string][]string, cols map[string][]powersync.Column) (string, string, error) {
	tables := make([]powersync.Table, 0, len(order))
	for _, name := range order {
		tables = append(tables, powersync.Table{Name: name, Columns: cols[name]})
	}
	sch := powersync.NewSchema(tables...)
	jsonSrc, err := json.MarshalIndent(sch, "", "  ")
	if err != nil {
		return "", "", err
	}

	samePkg := pkg == "powersync"
	if fn == "" {
		if samePkg {
			fn = "CutterSchema"
		} else {
			fn = "cutterSchema"
		}
	}
	// 跨包生成时给类型加 powersync. 前缀
	q := func(name string) string {
		if samePkg {
			return name
		}
		return "powersync." + name
	}

	var b strings.Builder
	b.WriteString("// Code generated by cmd/genschema from ")
	b.WriteString(filepathBase(rulesFile))
	b.WriteString("; DO NOT EDIT（要改表/列请改 syncRules.yaml 或生成器后重新生成）。\n\n")
	b.WriteString("package " + pkg + "\n\nimport (\n\t\"strings\"\n")
	if !samePkg {
		b.WriteString("\n\tpowersync \"msmkSync\"\n")
	}
	b.WriteString(")\n\n")
	fmt.Fprintf(&b, "// %s 是本项目的客户端 schema：表清单来自 PowerSync 同步规则（%s），\n", fn, filepathBase(rulesFile))
	b.WriteString("// 列定义来自 PostgREST OpenAPI。PowerSync 只会同步这里声明的表。\n")
	fmt.Fprintf(&b, "//\n// 共 %d 张表 / %d 列。\n", len(order), countCols(cols))
	b.WriteString("//\n// 注意：PG 的 schema（org/cutters/mats/tools/delivery）只是服务端命名空间；\n")
	b.WriteString("// 客户端表名**不带 schema 前缀**（同步流里的 object_type 就是裸表名）。\n")
	b.WriteString("// 每行末尾的注释只说明它在 PG 里属于哪个 schema，便于对照排查。\n")
	fmt.Fprintf(&b, "func %s() *%s {\n", fn, q("Schema"))
	fmt.Fprintf(&b, "\treturn %s(cutterTables()...)\n}\n\n", q("NewSchema"))
	fmt.Fprintf(&b, "func cutterTables() []%s {\n\treturn []%s{\n", q("Table"), q("Table"))
	for _, name := range order {
		specs := make([]string, 0, len(cols[name]))
		for _, c := range cols[name] {
			specs = append(specs, fmt.Sprintf("%q", string(c.Name)+":"+string(c.Type)))
		}
		comment := ""
		if pgs := pgSchemas[name]; len(pgs) > 0 {
			qualified := make([]string, 0, len(pgs))
			for _, pg := range pgs {
				qualified = append(qualified, pg+"."+name)
			}
			comment = "  // " + strings.Join(qualified, ", ")
		}
		if len(specs) == 0 {
			fmt.Fprintf(&b, "\t\t{Name: %q},%s\n", name, comment)
			continue
		}
		fmt.Fprintf(&b, "\t\t{Name: %q, Columns: cols(%s)},%s\n", name, strings.Join(specs, ", "), comment)
	}
	b.WriteString("\t}\n}\n\n")

	// cols 辅助函数：包内共用（跨包时类型也要限定）
	b.WriteString("// cols 把 \"列名:类型\" 展开成 []Column，让上面的表定义保持一行一张表。\n")
	fmt.Fprintf(&b, "func cols(specs ...string) []%s {\n", q("Column"))
	fmt.Fprintf(&b, "\tout := make([]%s, 0, len(specs))\n", q("Column"))
	b.WriteString("\tfor _, s := range specs {\n")
	b.WriteString("\t\tname, typ, ok := strings.Cut(s, \":\")\n")
	b.WriteString("\t\tif !ok {\n\t\t\tpanic(\"schema: 缺列类型: \" + s)\n\t\t}\n")
	fmt.Fprintf(&b, "\t\tout = append(out, %s{Name: name, Type: %s(typ)})\n", q("Column"), q("ColumnType"))
	b.WriteString("\t}\n\treturn out\n}\n")
	return b.String(), string(jsonSrc) + "\n", nil
}

// ---------------------------------------------------------------- 小工具

func countCols(cols map[string][]powersync.Column) int {
	n := 0
	for _, c := range cols {
		n += len(c)
	}
	return n
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func dir(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[:i]
	}
	return "."
}

func filepathBase(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[i+1:]
	}
	return p
}

var _ = url.PathEscape

// ---------------------------------------------------------------- 抓包推断

// inferFromStream 从抓包的 NDJSON 里按 object_type 汇总列（给 OpenAPI 也没有的表用，如 role）。
// 类型推断：整列都是数字 → INTEGER/REAL；布尔 → INTEGER；其余 TEXT。
func inferFromStream(path string) (map[string][]powersync.Column, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	type colStat struct {
		allInt   bool
		allNum   bool
		allBool  bool
		seen     int
		haveType bool
	}
	stats := map[string]map[string]*colStat{}

	add := func(table string, data map[string]json.RawMessage) {
		if stats[table] == nil {
			stats[table] = map[string]*colStat{}
		}
		for k, v := range data {
			if k == "id" {
				continue
			}
			st := stats[table][k]
			if st == nil {
				st = &colStat{allInt: true, allNum: true, allBool: true}
				stats[table][k] = st
			}
			st.seen++
			s := strings.TrimSpace(string(v))
			if s == "null" || s == "" {
				continue
			}
			var num json.Number
			if err := json.Unmarshal(v, &num); err != nil {
				st.allInt, st.allNum = false, false
			} else if strings.ContainsAny(num.String(), ".eE") {
				st.allInt = false
			}
			if s != "true" && s != "false" {
				st.allBool = false
			}
		}
	}

	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// data 行的形状：{"data":{"bucket":..,"after":..,"data":[<op>...]}}
		var frame struct {
			Data struct {
				Data []struct {
					ObjectType string                     `json:"object_type"`
					Data       map[string]json.RawMessage `json:"data"`
				} `json:"data"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			continue // 非 data 行（checkpoint 等）
		}
		for _, op := range frame.Data.Data {
			if op.Data != nil {
				add(op.ObjectType, op.Data)
			}
		}
	}

	out := make(map[string][]powersync.Column, len(stats))
	for table, cols := range stats {
		names := make([]string, 0, len(cols))
		for name := range cols {
			names = append(names, name)
		}
		sortStrings(names)
		list := make([]powersync.Column, 0, len(names))
		for _, name := range names {
			st := cols[name]
			typ := powersync.TextColumn
			switch {
			case st.allBool:
				typ = powersync.IntegerColumn
			case st.allInt:
				typ = powersync.IntegerColumn
			case st.allNum:
				typ = powersync.RealColumn
			}
			list = append(list, powersync.Column{Name: name, Type: typ})
		}
		out[table] = list
	}
	return out, nil
}

// ---------------------------------------------------------------- 小工具（续）

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
