// Command psdiff 是开发用的差分测试工具，不对外发布。
//
// 流程：
//  1. 用 internal/core 打开本地库（可按 schema 建真实用户表 + ps_crud 触发器）；
//  2. 按 /sync/stream 协议从本地游标续传，把 op 应用到真实表；
//  3. 再同步一次，验证 after 续传零增量；
//  4. 可选 -rest：把落库结果与 PostgREST（数据源真相）逐行逐字段比对。
//
// 用法：
//
//	go run ./cmd/psdiff -endpoint $PS_URL -rpc $RPC_URL -box D1-2 -org ORG001 \
//	    -schema ./out/schema.json -rest $REST_URL -out ./out
//
// schema.json 可用 scripts/js-reference/dump-schema.mjs 从 AppSchema.ts 导出；
// 不给 -schema 时退化为"按行字段惰性建表"（仅用于协议/落库验证）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"msmkSync/internal/bson"
	"msmkSync/internal/core"
	"msmkSync/internal/protocol"
)

func main() {
	var (
		endpoint   = flag.String("endpoint", os.Getenv("PS_URL"), "PowerSync 服务地址")
		token      = flag.String("token", os.Getenv("PS_TOKEN"), "JWT（也可用 -rpc 换取）")
		rpc        = flag.String("rpc", os.Getenv("RPC_URL"), "PostgREST 风格 fetch_credentials RPC")
		box        = flag.String("box", os.Getenv("BOX_CODE"), "RPC 参数 box_code")
		org        = flag.String("org", os.Getenv("ORG_CODE"), "RPC 参数 org_code")
		rest       = flag.String("rest", os.Getenv("REST_URL"), "可选：PostgREST base（如 http://host:8089/rest）")
		out        = flag.String("out", "./out", "输出目录")
		dbPath     = flag.String("db", "", "SQLite 文件路径（默认 <out>/go-reference.db）")
		schemaF    = flag.String("schema", "", "schema JSON（官方 Schema.toJSON 形状）；为空则按行字段惰性建表")
		stayOpen   = flag.Duration("timeout", 60*time.Second, "单次同步的最长等待时间")
		debug      = flag.Bool("debug", os.Getenv("PSDIFF_DEBUG") == "1", "打印请求体与每条 data 行")
		preferBSON = flag.Bool("bson", true, "请求 BSON 同步流（-bson=false 走 NDJSON）")
		keepDB     = flag.Bool("keep-db", false, "保留已有本地库（默认每次重建，便于复现基线）")
	)
	flag.Parse()

	if *endpoint == "" {
		fatalf("需要 -endpoint 或 PS_URL")
	}
	*endpoint = strings.TrimRight(*endpoint, "/")
	if *dbPath == "" {
		*dbPath = filepath.Join(*out, "go-reference.db")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatalf("创建输出目录: %v", err)
	}

	ctx := context.Background()
	tok := *token
	if tok == "" {
		var err error
		if tok, err = fetchToken(ctx, *rpc, *box, *org); err != nil {
			fatalf("取 token: %v", err)
		}
	}

	db, err := openDB(ctx, *dbPath, *schemaF, *keepDB)
	if err != nil {
		fatalf("打开本地库: %v", err)
	}
	defer db.Close()
	if db.Schema() != nil {
		fmt.Printf("schema: %d 张表\n", len(db.Schema().Tables))
	} else {
		fmt.Println("schema: 未提供（按行字段惰性建表）")
	}

	fmt.Printf("=== 第 1 次同步：本地 %d 个 bucket ===\n", bucketCount(db))
	r1, err := syncOnce(ctx, db, *endpoint, tok, *stayOpen, *debug, *preferBSON)
	if err != nil {
		fatalf("第 1 次同步: %v", err)
	}
	report("第 1 次", r1)

	fmt.Printf("\n=== 第 2 次同步（续传验证） ===\n")
	r2, err := syncOnce(ctx, db, *endpoint, tok, *stayOpen, *debug, *preferBSON)
	if err != nil {
		fatalf("第 2 次同步: %v", err)
	}
	report("第 2 次", r2)
	if r2.ops != 0 {
		fmt.Printf("⚠️  续传后仍有 %d 个 op（after 语义或游标保存有问题）\n", r2.ops)
	}

	rows, err := dumpRows(ctx, db)
	if err != nil {
		fatalf("导出落库结果: %v", err)
	}
	rowsPath := filepath.Join(*out, "rows.jsonl")
	if err := writeRows(rowsPath, rows); err != nil {
		fatalf("写 rows.jsonl: %v", err)
	}
	if err := writeStateJSON(ctx, db, filepath.Join(*out, "state.json")); err != nil {
		fatalf("写 state.json: %v", err)
	}
	fmt.Printf("\n落库结果: %d 张表 / %d 行 → %s\n", countTables(rows), len(rows), rowsPath)
	for _, line := range tableSummary(rows, 12) {
		fmt.Println("  " + line)
	}

	if n := db.SkippedUnknown(); n > 0 {
		fmt.Printf("\n跳过（不在 schema 中的表）: %d 个 op\n", n)
	}

	// 校验和对比：本地 add+op 应与服务端 checkpoint 一致。
	if len(r2.want) > 0 {
		fmt.Printf("\n校验和：服务端 bucket 数=%d\n", len(r2.want))
		bad := 0
		for bucket, want := range r2.want {
			got, err := db.BucketChecksum(ctx, bucket)
			if err != nil {
				fatalf("读取本地校验和: %v", err)
			}
			status := "OK"
			if got != want {
				status = "MISMATCH"
				bad++
			}
			fmt.Printf("  %-40s %s 本地=%d 服务端=%d\n", truncate(bucket, 40), status, got, want)
		}
		if bad > 0 {
			fmt.Printf("⚠️  %d 个 bucket 校验和不一致\n", bad)
		}
	}

	crud, err := db.CrudBatch(ctx, 10)
	if err != nil {
		fatalf("读取上传队列: %v", err)
	}
	fmt.Printf("\n上传队列: %d 条待上传（下行数据不应入队）\n", len(crud))
	for _, e := range crud {
		fmt.Printf("  #%d %s %s/%s\n", e.ID, e.Op, e.Table, e.RowID)
	}

	if *rest != "" {
		fmt.Printf("\n=== 与 PostgREST 比对：%s ===\n", *rest)
		if err := compareREST(ctx, strings.TrimRight(*rest, "/"), tok, rows); err != nil {
			fatalf("比对: %v", err)
		}
	}
}

// ---------------------------------------------------------------- 同步

type applyResult struct {
	dataLines  int
	ops        int
	checkpoint protocol.OpID
	streams    int
	keepalive  int
	buckets    int
	// want 是 checkpoint 声明的 bucket 校验和；mismatch 是本地累计校验和与之不符的 bucket。
	want      map[string]uint32
	targets   []core.BucketTarget
	mismatch  []string
	restarted bool
}

func report(label string, r applyResult) {
	fmt.Printf("%s: data 行=%d op=%d checkpoint=%s bucket=%d keepalive=%d\n",
		label, r.dataLines, r.ops, r.checkpoint, r.buckets, r.keepalive)
}

func bucketCount(db *core.DB) int {
	b, err := db.Buckets(context.Background())
	if err != nil {
		return 0
	}
	return len(b)
}

// syncOnce 跑一次完整 checkpoint，把 op 应用到本地库。
func syncOnce(ctx context.Context, db *core.DB, endpoint, token string, wait time.Duration, debug, preferBSON bool) (applyResult, error) {
	var res applyResult

	cursors, err := db.Buckets(ctx)
	if err != nil {
		return res, err
	}
	buckets := make([]protocol.BucketRequest, 0, len(cursors))
	for name, after := range cursors {
		buckets = append(buckets, protocol.BucketRequest{Name: name, After: after})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })

	body, err := json.Marshal(protocol.StreamRequest{
		Buckets:         buckets,
		IncludeChecksum: true,
		RawData:         true,
		AppMetadata:     map[string]string{},
		Parameters:      map[string]any{},
	})
	if err != nil {
		return res, err
	}
	if debug {
		fmt.Printf("  request=%s\n", body)
	}

	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/sync/stream", strings.NewReader(string(body)))
	if err != nil {
		return res, err
	}
	req.Header.Set("Authorization", "Token "+token)
	req.Header.Set("Content-Type", "application/json")
	if preferBSON {
		req.Header.Set("Accept", "application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8")
	} else {
		req.Header.Set("Accept", "application/x-ndjson")
	}
	req.Header.Set("X-User-Agent", "powersync-go/dev")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return res, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		if e := protocol.DecodeErrorResponse(msg); e != nil {
			return res, fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, e.Code, e.Description)
		}
		return res, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	src := newLineSource(resp)
	if debug {
		fmt.Printf("  流格式: %s\n", resp.Header.Get("Content-Type"))
	}
	for {
		line, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return res, err
		}
		switch l := line.(type) {
		case *protocol.CheckpointLine:
			res.buckets = len(l.Buckets)
			res.targets = make([]core.BucketTarget, 0, len(l.Buckets))
			res.want = make(map[string]uint32, len(l.Buckets))
			for _, b := range l.Buckets {
				res.targets = append(res.targets, core.BucketTarget{
					Name:     b.Bucket,
					Checksum: uint32(b.Checksum),
					Count:    b.Count,
					Priority: b.Priority,
				})
				res.want[b.Bucket] = uint32(b.Checksum)
			}
		case *protocol.DataLine:
			res.dataLines++
			if err := db.StageOps(ctx, l.Bucket, l.Ops); err != nil {
				return res, fmt.Errorf("stage %s: %w", l.Bucket, err)
			}
			res.ops += len(l.Ops)
			if debug {
				fmt.Printf("  staged bucket=%s after=%s has_more=%v ops=%d\n",
					l.Bucket, l.After, l.HasMore, len(l.Ops))
			}
		case *protocol.CheckpointCompleteLine:
			res.checkpoint = l.LastOpID
			bad, err := db.ApplyCheckpoint(ctx, l.LastOpID, res.targets,
				core.ApplyOptions{Validate: true, ResetOnMismatch: true, Priority: 3})
			if err != nil {
				return res, err
			}
			for _, m := range bad {
				res.mismatch = append(res.mismatch, m.String())
				fmt.Printf("  ! 校验和不符，已重置 bucket：%s\n", m)
			}
			if len(bad) > 0 {
				res.restarted = true
				return res, nil
			}
			if err := db.SetMeta(ctx, "last_op_id", l.LastOpID.String()); err != nil {
				return res, err
			}
			return res, nil
		case *protocol.TokenExpiresInLine:
			res.keepalive++
		default:
			if debug {
				fmt.Printf("  (忽略未知行 %s)\n", line.Kind())
			}
		}
	}
	return res, nil
}

// lineSource 抽象 NDJSON / BSON；按响应 Content-Type 选择。
type lineSource interface {
	Next() (protocol.Line, error)
}

type ndjsonSource struct{ dec *protocol.Decoder }

func (s *ndjsonSource) Next() (protocol.Line, error) { return s.dec.Next() }

type bsonSource struct{ dec *bson.Decoder }

func (s *bsonSource) Next() (protocol.Line, error) {
	doc, err := s.dec.Next()
	if err != nil {
		return nil, err
	}
	return protocol.ParseLine(doc)
}

func newLineSource(resp *http.Response) lineSource {
	if strings.Contains(resp.Header.Get("Content-Type"), "bson") {
		return &bsonSource{dec: bson.NewDecoder(resp.Body)}
	}
	return &ndjsonSource{dec: protocol.NewDecoder(resp.Body, 0)}
}

// ---------------------------------------------------------------- 本地库

func openDB(ctx context.Context, path, schemaPath string, keep bool) (*core.DB, error) {
	var schema *core.Schema
	if schemaPath != "" {
		raw, err := os.ReadFile(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("读 schema: %w", err)
		}
		if schema, err = core.ParseSchema(raw); err != nil {
			return nil, err
		}
	}
	if !keep {
		for _, f := range []string{path, path + "-wal", path + "-shm"} {
			_ = os.Remove(f)
		}
	}
	// 该服务端 sync rules 里有 AppSchema 未声明的表（i/t/material_*），core 会跳过物化。
	return core.Open(ctx, core.Options{Path: path, Schema: schema})
}

func writeStateJSON(ctx context.Context, db *core.DB, path string) error {
	buckets, err := db.Buckets(ctx)
	if err != nil {
		return err
	}
	lastOp, err := db.Meta(ctx, "last_op_id")
	if err != nil {
		return err
	}
	b := make(map[string]string, len(buckets))
	for name, after := range buckets {
		b[name] = after.String()
	}
	out := map[string]any{"last_op_id": lastOp, "buckets": b}
	data, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// ---------------------------------------------------------------- 导出与比对

type row struct {
	Table string                     `json:"table"`
	Row   map[string]json.RawMessage `json:"row"`
}

func dumpRows(ctx context.Context, db *core.DB) ([]row, error) {
	tables, err := userTables(ctx, db)
	if err != nil {
		return nil, err
	}
	var out []row
	for _, table := range tables {
		rs, err := db.SQL().QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`, quoteIdent(table)))
		if err != nil {
			return nil, fmt.Errorf("dump %s: %w", table, err)
		}
		cols, err := rs.Columns()
		if err != nil {
			rs.Close()
			return nil, err
		}
		for rs.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rs.Scan(ptrs...); err != nil {
				rs.Close()
				return nil, err
			}
			m := make(map[string]json.RawMessage, len(cols))
			for i, c := range cols {
				raw, err := rawFromValue(vals[i])
				if err != nil {
					rs.Close()
					return nil, fmt.Errorf("%s.%s: %w", table, c, err)
				}
				m[c] = raw
			}
			out = append(out, row{Table: table, Row: m})
		}
		if err := rs.Err(); err != nil {
			rs.Close()
			return nil, err
		}
		rs.Close()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return string(out[i].Row["id"]) < string(out[j].Row["id"])
	})
	return out, nil
}

// userTables 返回 schema 声明的表；没有 schema 时取 sqlite_master 里非 ps_ 前缀的表。
func userTables(ctx context.Context, db *core.DB) ([]string, error) {
	if s := db.Schema(); s != nil {
		out := make([]string, 0, len(s.Tables))
		for _, t := range s.Tables {
			out = append(out, t.Name)
		}
		sort.Strings(out)
		return out, nil
	}
	rows, err := db.SQL().QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'ps\_%' ESCAPE '\' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func rawFromValue(v any) (json.RawMessage, error) {
	switch t := v.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case int64:
		return json.RawMessage(strconv.FormatInt(t, 10)), nil
	case float64:
		return json.RawMessage(strconv.FormatFloat(t, 'g', -1, 64)), nil
	case bool:
		if t {
			return json.RawMessage("1"), nil
		}
		return json.RawMessage("0"), nil
	case []byte:
		if json.Valid(t) {
			return json.RawMessage(t), nil
		}
		b, err := json.Marshal(string(t))
		return json.RawMessage(b), err
	case string:
		if json.Valid([]byte(t)) && (strings.HasPrefix(strings.TrimSpace(t), "{") || strings.HasPrefix(strings.TrimSpace(t), "[")) {
			// 文本里存的是 JSON（jsonb 列），按对象输出，方便与 REST 比较。
			return json.RawMessage(t), nil
		}
		b, err := json.Marshal(t)
		return json.RawMessage(b), err
	default:
		b, err := json.Marshal(v)
		return json.RawMessage(b), err
	}
}

func writeRows(path string, rows []row) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

func countTables(rows []row) int {
	seen := map[string]struct{}{}
	for _, r := range rows {
		seen[r.Table] = struct{}{}
	}
	return len(seen)
}

func tableSummary(rows []row, limit int) []string {
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Table]++
	}
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] != counts[names[j]] {
			return counts[names[i]] > counts[names[j]]
		}
		return names[i] < names[j]
	})
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}
	var out []string
	for _, n := range names {
		out = append(out, fmt.Sprintf("%-32s %5d", n, counts[n]))
	}
	return out
}

// compareREST 把落库行与 PostgREST 返回的同 id 行逐字段比对。
func compareREST(ctx context.Context, base, token string, rows []row) error {
	byTable := map[string]map[string]map[string]json.RawMessage{}
	var order []string
	for _, r := range rows {
		if byTable[r.Table] == nil {
			byTable[r.Table] = map[string]map[string]json.RawMessage{}
			order = append(order, r.Table)
		}
		id := string(rawString(r.Row["id"]))
		if id == "" {
			fmt.Printf("  ! %s 有一条落库行没有 id 字段\n", r.Table)
			continue
		}
		byTable[r.Table][id] = r.Row
	}

	var totalMissing, totalMismatch, totalOK, skipped, totalSoftDeleted int
	for _, table := range order {
		remote, err := fetchRESTTable(ctx, base, token, table)
		if err != nil {
			fmt.Printf("  %-28s 跳过（%v）\n", table, err)
			skipped++
			continue
		}
		var ok, missing, mismatch, softDeleted int
		var samples []string
		for id, local := range byTable[table] {
			rr, found := remote[id]
			if !found {
				if isSoftDeleted(local) {
					softDeleted++
					continue
				}
				missing++
				if len(samples) < 3 {
					samples = append(samples, fmt.Sprintf("%s/%s: REST 无此行", table, id))
				}
				continue
			}
			diff := diffFields(local, rr)
			if len(diff) == 0 {
				ok++
				continue
			}
			mismatch++
			if len(samples) < 3 {
				samples = append(samples, fmt.Sprintf("%s/%s: %s", table, id, strings.Join(diff, "; ")))
			}
		}
		status := "OK"
		if missing > 0 || mismatch > 0 {
			status = "DIFF"
		}
		fmt.Printf("  %-28s %s  stream=%d rest=%d 一致=%d 缺失=%d 不一致=%d 软删除=%d\n",
			table, status, len(byTable[table]), len(remote), ok, missing, mismatch, softDeleted)
		for _, s := range samples {
			fmt.Printf("      %s\n", s)
		}
		totalOK += ok
		totalMissing += missing
		totalMismatch += mismatch
		totalSoftDeleted += softDeleted
	}

	fmt.Printf("\n合计: 一致=%d 缺失=%d 字段不一致=%d 软删除(预期不在 REST)=%d 跳过表=%d\n",
		totalOK, totalMissing, totalMismatch, totalSoftDeleted, skipped)
	if totalMissing > 0 || totalMismatch > 0 {
		return fmt.Errorf("差分比对失败：缺失 %d 行、字段不一致 %d 行", totalMissing, totalMismatch)
	}
	return nil
}

func fetchRESTTable(ctx context.Context, base, token, table string) (map[string]map[string]json.RawMessage, error) {
	u := base + "/" + url.PathEscape(table) + "?select=*&limit=20000"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return nil, fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var list []map[string]json.RawMessage
	if err := dec.Decode(&list); err != nil {
		return nil, err
	}
	out := make(map[string]map[string]json.RawMessage, len(list))
	for _, m := range list {
		if id := string(rawString(m["id"])); id != "" {
			out[id] = m
		}
	}
	return out, nil
}

// diffFields 返回字段级差异（只比较落库行里出现的字段）。
func diffFields(local, remote map[string]json.RawMessage) []string {
	var diff []string
	keys := make([]string, 0, len(local))
	for k := range local {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rv, exists := remote[k]
		if !exists {
			// 落库为 NULL、REST 里又没这个列（schema 声明了但库里不存在）→ 视为一致。
			if canon(local[k]) == "<nil>" {
				continue
			}
			diff = append(diff, fmt.Sprintf("%s: stream=%s rest=<missing>", k, canon(local[k])))
		} else if lv, rvv := canon(local[k]), canon(rv); lv != rvv {
			diff = append(diff, fmt.Sprintf("%s: stream=%s rest=%s", k, lv, rvv))
		}
		if len(diff) >= 3 {
			return diff
		}
	}
	return diff
}

// isSoftDeleted 报告该行是否为软删除（deleted_at 非空或 is_deleted 为真）。
func isSoftDeleted(row map[string]json.RawMessage) bool {
	for _, k := range []string{"deleted_at", "is_deleted", "is_del"} {
		raw, ok := row[k]
		if !ok {
			continue
		}
		switch canon(raw) {
		case "<nil>", "n:0", "s:", "s:0", "s:false":
		default:
			return true
		}
	}
	return false
}

// canon 把 JSON 值归一化成可比较字符串，屏蔽两种序列化风格的差异：
//   - 时间戳："2026-08-13 18:40:38.49294" vs "2026-08-13T18:40:38.49294"
//   - 数值："2.00"（numeric 列以文本下发）vs 2
//   - JSON 列："{\"a\": 1}"（jsonb 以文本下发）vs {"a":1}
func canon(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "<nil>"
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "!" + string(raw)
	}
	return canonValue(v)
}

func canonValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "<nil>"
	case bool:
		if t {
			return "n:1"
		}
		return "n:0"
	case json.Number:
		return "n:" + trimNumber(t.String())
	case string:
		if ts, ok := parseTimestamp(t); ok {
			return "t:" + ts
		}
		if n, ok := parseNumberString(t); ok {
			return "n:" + n
		}
		if obj, ok := parseJSONString(t); ok {
			return "j:" + obj
		}
		// PS 对 jsonb/文本列会把值再包一层字符串（实测 re_order_no: "\"\""）。
		if len(t) >= 2 && t[0] == '"' && t[len(t)-1] == '"' {
			var inner string
			if err := json.Unmarshal([]byte(t), &inner); err == nil {
				return "s:" + inner
			}
		}
		return "s:" + t
	case map[string]any:
		return "j:" + canonicalJSON(t)
	case []any:
		return "j:" + canonicalJSON(t)
	default:
		b, _ := json.Marshal(v)
		return "o:" + string(b)
	}
}

var timestampLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
}

func parseTimestamp(s string) (string, bool) {
	if len(s) < 19 {
		return "", false
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(time.RFC3339Nano), true
		}
	}
	return "", false
}

func parseNumberString(s string) (string, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "", false
	}
	if _, err := strconv.ParseFloat(t, 64); err != nil {
		return "", false
	}
	return trimNumber(t), true
}

func parseJSONString(s string) (string, bool) {
	t := strings.TrimSpace(s)
	if len(t) < 2 || (t[0] != '{' && t[0] != '[') || !json.Valid([]byte(t)) {
		return "", false
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(t))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", false
	}
	return canonicalJSON(v), true
}

func canonicalJSON(v any) string {
	b, err := json.Marshal(normalizeJSON(v))
	if err != nil {
		return fmt.Sprintf("!%v", v)
	}
	return string(b)
}

func normalizeJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	case json.Number:
		return json.Number(trimNumber(t.String()))
	default:
		return v
	}
}

func trimNumber(s string) string {
	if strings.ContainsAny(s, "eE") {
		return s
	}
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		return "0"
	}
	return s
}

func rawString(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []byte(s)
	}
	return raw
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// ---------------------------------------------------------------- token

func fetchToken(ctx context.Context, rpc, box, org string) (string, error) {
	if rpc == "" || box == "" || org == "" {
		return "", errors.New("需要 -token，或 -rpc/-box/-org")
	}
	body := fmt.Sprintf(`{"box_code":%q,"org_code":%q}`, box, org)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpc, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", errors.New("RPC 未返回 token")
	}
	return out.Token, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "psdiff: "+format+"\n", args...)
	os.Exit(1)
}
