package protocol_test

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhuweizz/powersync_golang/internal/protocol"
)

// testdata 目录在仓库根，与包目录隔了两层。
func goldenPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("..", "..", "testdata", "sync", name)
}

func decodeFile(t *testing.T, name string) []protocol.Line {
	t.Helper()
	f, err := os.Open(goldenPath(t, name))
	if err != nil {
		t.Fatalf("open golden: %v", err)
	}
	defer f.Close()

	dec := protocol.NewDecoder(f, 0)
	var lines []protocol.Line
	for {
		line, err := dec.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode line %d: %v", len(lines), err)
		}
		lines = append(lines, line)
	}
	return lines
}

func kindSeq(lines []protocol.Line) []protocol.Kind {
	out := make([]protocol.Kind, len(lines))
	for i, l := range lines {
		out[i] = l.Kind()
	}
	return out
}

func assertKinds(t *testing.T, lines []protocol.Line, want ...protocol.Kind) {
	t.Helper()
	got := kindSeq(lines)
	if len(got) != len(want) {
		t.Fatalf("got %d lines %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d kind = %q, want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

// TestGoldenCheckpoint 覆盖 checkpoint 行：bucket 名带参数、checksum 超过 2^31、
// priority/subscriptions 与 streams 状态。
func TestGoldenCheckpoint(t *testing.T) {
	lines := decodeFile(t, "full-snapshot-raw.ndjson")
	assertKinds(t, lines,
		protocol.KindCheckpoint,
		protocol.KindData,
		protocol.KindData,
		protocol.KindData,
		protocol.KindCheckpointComplete,
		protocol.KindTokenExpiresIn,
	)

	cp, ok := lines[0].(*protocol.CheckpointLine)
	if !ok {
		t.Fatalf("line 0 = %T, want *CheckpointLine", lines[0])
	}
	if cp.LastOpID != 101 {
		t.Errorf("last_op_id = %d, want 101", cp.LastOpID)
	}
	if len(cp.Buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(cp.Buckets))
	}
	b := cp.Buckets[0]
	if want := `50#org_data["ORG001"]`; b.Bucket != want {
		t.Errorf("bucket[0].name = %q, want %q", b.Bucket, want)
	}
	// checksum 实测为 uint32，最大值会超过 int32。
	if b.Checksum != 1070820748 {
		t.Errorf("bucket[0].checksum = %d, want 1070820748", b.Checksum)
	}
	if b.Count != 2 || b.Priority != 3 {
		t.Errorf("bucket[0] count/priority = %d/%d, want 2/3", b.Count, b.Priority)
	}
	if len(b.Subscriptions) != 1 || b.Subscriptions[0].Default == nil || *b.Subscriptions[0].Default != 0 {
		t.Errorf("bucket[0].subscriptions = %+v, want [{default:0}]", b.Subscriptions)
	}
	if len(cp.Streams) != 2 || cp.Streams[0].Name != "org_data" || !cp.Streams[0].IsDefault {
		t.Errorf("streams = %+v, want org_data(default)+box_data(default)", cp.Streams)
	}
	if len(cp.Streams[0].Errors) != 0 {
		t.Errorf("streams[0].errors = %v, want empty", cp.Streams[0].Errors)
	}
	if cp.WriteCheckpoint != "" {
		t.Errorf("write_checkpoint = %q, want empty in requests mode", cp.WriteCheckpoint)
	}
}

// TestGoldenDataLines 覆盖 data 行：after 左开语义的原始值、has_more、raw_data 的两种形态。
func TestGoldenDataLines(t *testing.T) {
	lines := decodeFile(t, "full-snapshot-raw.ndjson")

	first := lines[1].(*protocol.DataLine)
	if want := `50#box_data["D1-2","ORG001"]`; first.Bucket != want {
		t.Errorf("bucket = %q, want %q", first.Bucket, want)
	}
	if first.After != 50 || first.HasMore {
		t.Errorf("after/has_more = %d/%v, want 50/false", first.After, first.HasMore)
	}
	if len(first.Ops) != 2 {
		t.Fatalf("ops = %d, want 2", len(first.Ops))
	}
	op := first.Ops[0]
	if op.OpID != 10 || op.Op != protocol.OpPut {
		t.Errorf("op = %d/%s, want 10/PUT", op.OpID, op.Op)
	}
	if op.Table() != "box" || op.RowID() != "0465f750-0000-4000-8000-000000000001" {
		t.Errorf("table/rowid = %q/%q", op.Table(), op.RowID())
	}
	if op.Subkey == "" {
		t.Error("subkey 为空：新协议应带 subkey，若漏解析需检查字段名")
	}
	if op.Checksum != 3272744065 {
		t.Errorf("op.checksum = %d, want 3272744065（验证不走 int32）", op.Checksum)
	}
	data, err := op.DecodeData()
	if err != nil {
		t.Fatalf("DecodeData: %v", err)
	}
	var row struct {
		ID      string `json:"id"`
		OrgCode string `json:"org_code"`
	}
	if err := json.Unmarshal(data, &row); err != nil {
		t.Fatalf("data 解包后不是 JSON 对象: %v", err)
	}
	if row.ID != op.RowID() || row.OrgCode != "ORG001" {
		t.Errorf("row = %+v", row)
	}
	// raw_data=true 时原始 Data 必须是字符串字面量。
	if len(op.Data) == 0 || op.Data[0] != '"' {
		t.Errorf("raw_data=true 时 Data 应为 JSON 字符串，实际前缀 %q", string(op.Data[:min(10, len(op.Data))]))
	}

	// 同一 bucket 的第二、三行：has_more 分页。
	second := lines[2].(*protocol.DataLine)
	if !second.HasMore {
		t.Error("line 2 has_more 应为 true（同 bucket 分页样本）")
	}
	third := lines[3].(*protocol.DataLine)
	if third.Bucket != second.Bucket || third.HasMore {
		t.Errorf("line 3 = %q/%v, want 同一 bucket/false", third.Bucket, third.HasMore)
	}
	if !(second.After < third.After) {
		t.Errorf("分页后 after 应递增：%d → %d", second.After, third.After)
	}
}

// TestGoldenRawDataFalse 验证 raw_data=false 时 data 是对象。
func TestGoldenRawDataFalse(t *testing.T) {
	lines := decodeFile(t, "raw-data-false.ndjson")
	assertKinds(t, lines, protocol.KindCheckpoint, protocol.KindData, protocol.KindCheckpointComplete)

	op := lines[1].(*protocol.DataLine).Ops[0]
	if len(op.Data) == 0 || op.Data[0] != '{' {
		t.Fatalf("raw_data=false 时 Data 应为 JSON 对象，实际 %q", string(op.Data))
	}
	data, err := op.DecodeData()
	if err != nil {
		t.Fatalf("DecodeData: %v", err)
	}
	if string(data) != string(op.Data) {
		t.Errorf("DecodeData 不应改写对象形态：%s vs %s", data, op.Data)
	}
}

// TestGoldenUpToDate 覆盖"已是最新"：只有 checkpoint 与 checkpoint_complete。
func TestGoldenUpToDate(t *testing.T) {
	lines := decodeFile(t, "up-to-date.ndjson")
	assertKinds(t, lines, protocol.KindCheckpoint, protocol.KindCheckpointComplete, protocol.KindTokenExpiresIn)

	if got := lines[1].(*protocol.CheckpointCompleteLine).LastOpID; got != 101 {
		t.Errorf("checkpoint_complete.last_op_id = %d, want 101", got)
	}
	if got := lines[2].(*protocol.TokenExpiresInLine).Seconds; got != 7044 {
		t.Errorf("token_expires_in = %d, want 7044", got)
	}
}

// TestCheckpointCompleteWithRequestID 覆盖服务端返回 checkpoint_request_id 的变体。
func TestCheckpointCompleteWithRequestID(t *testing.T) {
	line, err := protocol.ParseLine([]byte(`{"checkpoint_complete":{"last_op_id":"42","checkpoint_request_id":"7"}}`))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	cc := line.(*protocol.CheckpointCompleteLine)
	if cc.LastOpID != 42 || cc.CheckpointRequestID == nil || *cc.CheckpointRequestID != "7" {
		t.Fatalf("got %+v", cc)
	}
}

// TestUnknownLineIsNotAnError 保证新增行类型不会中断同步。
func TestUnknownLineIsNotAnError(t *testing.T) {
	line, err := protocol.ParseLine([]byte(`{"some_future_event":{"x":1}}`))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	u, ok := line.(*protocol.UnknownLine)
	if !ok {
		t.Fatalf("got %T, want *UnknownLine", line)
	}
	if u.Kind() != "some_future_event" {
		t.Errorf("kind = %q", u.Kind())
	}
	if len(u.Raw()) == 0 {
		t.Error("Raw() 应保留原始字节")
	}
}

// TestParseLineErrors 覆盖坏行与超长行。
func TestParseLineErrors(t *testing.T) {
	if _, err := protocol.ParseLine([]byte(`{"data":`)); err == nil {
		t.Error("截断的 JSON 应报错")
	}
	if _, err := protocol.ParseLine([]byte(`{}`)); err == nil {
		t.Error("空对象应报错")
	}

	in := &infiniteReader{n: protocol.DefaultMaxLineSize + 1}
	dec := protocol.NewDecoder(in, 1024)
	_, err := dec.Next()
	if !errors.Is(err, protocol.ErrLineTooLong) {
		t.Errorf("err = %v, want ErrLineTooLong", err)
	}
}

type infiniteReader struct{ n int }

func (r *infiniteReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	if len(p) > r.n {
		p = p[:r.n]
	}
	for i := range p {
		p[i] = 'x'
	}
	r.n -= len(p)
	return len(p), nil
}

// TestDecoderLastLineWithoutNewline 保证末行无换行也能解析。
func TestDecoderLastLineWithoutNewline(t *testing.T) {
	dec := protocol.NewDecoder(strings.NewReader(`{"token_expires_in":30}`), 0)
	line, err := dec.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if line.(*protocol.TokenExpiresInLine).Seconds != 30 {
		t.Fatalf("got %+v", line)
	}
	if _, err := dec.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("第二次 Next = %v, want io.EOF", err)
	}
}

// TestStreamRequestJSON 固定请求体字段名与 after 的字符串序列化。
func TestStreamRequestJSON(t *testing.T) {
	prio := 0
	req := protocol.StreamRequest{
		Buckets: []protocol.BucketRequest{
			{Name: `50#org_data["ORG001"]`, After: 1234},
		},
		IncludeChecksum: true,
		RawData:         true,
		AppMetadata:     map[string]string{"k": "v"},
		ClientID:        "cid",
		Streams: &protocol.StreamSubscribes{
			IncludeDefaults: true,
			Subscriptions: []protocol.StreamSubscription{
				{Stream: "notes", Parameters: map[string]any{"org": "x"}, OverridePriority: &prio},
			},
		},
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"buckets":[{"name":"50#org_data[\"ORG001\"]","after":"1234"}],"include_checksum":true,"raw_data":true,"app_metadata":{"k":"v"},"client_id":"cid","streams":{"include_defaults":true,"subscriptions":[{"stream":"notes","parameters":{"org":"x"},"override_priority":0}]}}`
	if string(b) != want {
		t.Errorf("请求体不符\n got: %s\nwant: %s", b, want)
	}

	// after 必须能往返。
	var back protocol.StreamRequest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.Buckets[0].After != 1234 {
		t.Errorf("after 往返丢失: %d", back.Buckets[0].After)
	}
}

// TestErrorResponse 用真实 401 响应验证错误信封解析。
func TestErrorResponse(t *testing.T) {
	body, err := os.ReadFile(goldenPath(t, "error-401.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	e := protocol.DecodeErrorResponse(body)
	if e == nil {
		t.Fatal("DecodeErrorResponse 返回 nil")
	}
	if e.Code != "PSYNC_S2106" || e.Status != 401 || e.Name != "AuthorizationError" {
		t.Errorf("got %+v", e)
	}
	if !e.Unauthorized() {
		t.Error("Unauthorized() 应为 true")
	}
	if protocol.DecodeErrorResponse([]byte("not json")) != nil {
		t.Error("非 JSON 应返回 nil")
	}
}

// TestSignedChecksum 覆盖实测到的带符号 checksum（服务端 i32 序列化）。
func TestSignedChecksum(t *testing.T) {
	line, err := protocol.ParseLine([]byte(`{"checkpoint":{"last_op_id":"9","buckets":[{"bucket":"b","checksum":-96505511,"count":1}]}}`))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	b := line.(*protocol.CheckpointLine).Buckets[0]
	if b.Checksum != -96505511 {
		t.Errorf("checksum = %d, want -96505511", b.Checksum)
	}
	// -96505511 的无符号 32 位视图 = 2^32 - 96505511。
	if got, want := b.Checksum32(), uint32(4198461785); got != want {
		t.Errorf("Checksum32 = %d, want %d", got, want)
	}

	line, err = protocol.ParseLine([]byte(`{"data":{"bucket":"b","after":"1","data":[{"op_id":"1","op":"PUT","object_type":"x","object_id":"y","checksum":-1}]}}`))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	op := line.(*protocol.DataLine).Ops[0]
	if op.Checksum32() != 4294967295 {
		t.Errorf("op.Checksum32 = %d, want 4294967295", op.Checksum32())
	}
}

// TestGoldenPutThenRemove 覆盖实测到的 REMOVE op（删除同步）：
// op=REMOVE、data 为 JSON null、checksum 仍是 32 位值、subkey 与 PUT 相同。
func TestGoldenPutThenRemove(t *testing.T) {
	lines := decodeFile(t, "put-remove.ndjson")
	assertKinds(t, lines,
		protocol.KindCheckpoint,
		protocol.KindData,
		protocol.KindCheckpointComplete,
		protocol.KindData,
		protocol.KindCheckpointComplete,
	)

	// 实测该 checkpoint 的 bucket checksum 是负数（服务端按 i32 序列化）。
	if got := lines[0].(*protocol.CheckpointLine).Buckets[0].Checksum; got != -96505511 {
		t.Errorf("bucket checksum = %d, want -96505511", got)
	}

	put := lines[1].(*protocol.DataLine).Ops[0]
	del := lines[3].(*protocol.DataLine).Ops[0]
	if put.Op != protocol.OpPut || put.IsDelete() {
		t.Errorf("put.Op = %s", put.Op)
	}
	if del.Op != protocol.OpRemove || !del.IsDelete() {
		t.Fatalf("del.Op = %s, IsDelete=%v", del.Op, del.IsDelete())
	}
	if del.Table() != put.Table() || del.RowID() != put.RowID() {
		t.Errorf("REMOVE 应指向同一行：%s/%s vs %s/%s", del.Table(), del.RowID(), put.Table(), put.RowID())
	}
	if del.Subkey != put.Subkey {
		t.Errorf("subkey 变化：%q vs %q", del.Subkey, put.Subkey)
	}
	data, err := del.DecodeData()
	if err != nil {
		t.Fatalf("DecodeData: %v", err)
	}
	if data != nil {
		t.Errorf("REMOVE 的 data 应为 null，得到 %s", data)
	}
	// 删除也必须能推进游标（REMOVE 同样带 op_id）。
	if del.OpID != 24397 {
		t.Errorf("del.OpID = %d", del.OpID)
	}
}

// TestGoldenCheckpointDiff 用真实抓包验证 checkpoint_diff 解析（实测：增量 checkpoint 用它代替 checkpoint 行）。
func TestGoldenCheckpointDiff(t *testing.T) {
	lines := decodeFile(t, "checkpoint-diff.ndjson")
	assertKinds(t, lines, protocol.KindCheckpointDiff, protocol.KindData, protocol.KindCheckpointComplete)

	diff := lines[0].(*protocol.CheckpointDiffLine)
	if diff.LastOpID != 24457 {
		t.Errorf("last_op_id = %d", diff.LastOpID)
	}
	if len(diff.RemovedBuckets) != 0 {
		t.Errorf("removed_buckets = %v", diff.RemovedBuckets)
	}
	if len(diff.UpdatedBuckets) != 1 {
		t.Fatalf("updated_buckets = %d", len(diff.UpdatedBuckets))
	}
	b := diff.UpdatedBuckets[0]
	if b.Bucket != `50#box_data["D1-2","ORG001"]` || b.Checksum32() != 2134983222 || b.Count != 980 || b.Priority != 3 {
		t.Errorf("updated bucket = %+v", b)
	}
}
