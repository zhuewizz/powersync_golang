package bson_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/zhuweizz/powersync_golang/internal/bson"
	"github.com/zhuweizz/powersync_golang/internal/protocol"
)

// ---------------------------------------------------------------- 测试用编码器

type elem struct {
	name  string
	typ   byte
	value []byte
}

func doc(elems ...elem) []byte {
	body := make([]byte, 0, 64)
	for _, e := range elems {
		body = append(body, e.typ)
		body = append(body, e.name...)
		body = append(body, 0)
		body = append(body, e.value...)
	}
	out := make([]byte, 4, 5+len(body))
	binary.LittleEndian.PutUint32(out, uint32(5+len(body)))
	out = append(out, body...)
	return append(out, 0)
}

func eString(name, v string) elem {
	b := make([]byte, 4, 5+len(v))
	binary.LittleEndian.PutUint32(b, uint32(len(v)+1))
	b = append(b, v...)
	return elem{name, 0x02, append(b, 0)}
}
func eInt32(name string, v int32) elem {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return elem{name, 0x10, b}
}
func eInt64(name string, v int64) elem {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return elem{name, 0x12, b}
}
func eDouble(name string, v float64) elem {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return elem{name, 0x01, b}
}
func eBool(name string, v bool) elem {
	if v {
		return elem{name, 0x08, []byte{1}}
	}
	return elem{name, 0x08, []byte{0}}
}
func eNull(name string) elem { return elem{name, 0x0A, nil} }
func eDoc(name string, d []byte) elem {
	return elem{name, 0x03, d}
}
func eArray(name string, d []byte) elem {
	return elem{name, 0x04, d}
}
func eBinary(name string, sub byte, data []byte) elem {
	b := make([]byte, 5, 5+len(data))
	binary.LittleEndian.PutUint32(b, uint32(len(data)))
	b[4] = sub
	return elem{name, 0x05, append(b, data...)}
}
func eDateTime(name string, t time.Time) elem {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(t.UnixMilli()))
	return elem{name, 0x09, b}
}
func eObjectID(name string, hexBytes []byte) elem { return elem{name, 0x07, hexBytes} }
func eTimestamp(name string, seconds, inc uint32) elem {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(seconds)<<32|uint64(inc))
	return elem{name, 0x11, b}
}

// ---------------------------------------------------------------- 用例

// TestDecodeAllTypes 覆盖协议里可能出现（以及通用 BSON）的元素类型。
func TestDecodeAllTypes(t *testing.T) {
	raw := doc(
		eString("s", "hello"),
		eInt32("i32", 42),
		eInt64("i64", -9007199254740993),
		eDouble("d", 1.5),
		eBool("b", true),
		eNull("n"),
		eDoc("doc", doc(eString("inner", "x"), eInt32("k", -7))),
		eArray("arr", doc(eString("0", "a"), eString("1", "b"), eString("2", "c"))),
		eBinary("bin", 0x00, []byte(`{"json":true}`)),
		eBinary("binRaw", 0x00, []byte{0xff, 0x00}),
		eDateTime("ts", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		eObjectID("oid", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		eTimestamp("tstamp", 1700000000, 1),
	)

	out, err := bson.MarshalDocumentJSON(raw)
	if err != nil {
		t.Fatalf("MarshalDocumentJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("解码结果不是合法 JSON: %v (%s)", err, out)
	}

	if got["s"] != "hello" {
		t.Errorf("s = %v", got["s"])
	}
	if got["i32"] != float64(42) || got["i64"] != float64(-9007199254740993) {
		t.Errorf("整数 = %v / %v", got["i32"], got["i64"])
	}
	if got["d"] != 1.5 || got["b"] != true || got["n"] != nil {
		t.Errorf("d/b/n = %v / %v / %v", got["d"], got["b"], got["n"])
	}
	if inner, _ := got["doc"].(map[string]any); inner["inner"] != "x" || inner["k"] != float64(-7) {
		t.Errorf("doc = %v", got["doc"])
	}
	arr, _ := got["arr"].([]any)
	if len(arr) != 3 || arr[0] != "a" || arr[2] != "c" {
		t.Errorf("arr = %v", got["arr"])
	}
	// 合法 JSON 的二进制原样透传；非法 JSON 的转 base64。
	if bin, ok := got["bin"].(map[string]any); !ok || bin["json"] != true {
		t.Errorf("bin = %v", got["bin"])
	}
	if got["binRaw"] != "/wA=" {
		t.Errorf("binRaw = %v", got["binRaw"])
	}
	if got["ts"] != "2026-01-02T03:04:05Z" {
		t.Errorf("ts = %v", got["ts"])
	}
	if got["oid"] != "0102030405060708090a0b0c" {
		t.Errorf("oid = %v", got["oid"])
	}
	if got["tstamp"] != float64(1700000000) {
		t.Errorf("tstamp = %v", got["tstamp"])
	}
}

// TestDecoderRealCheckpointPair 用真机抓的 BSON/NDJSON 对照：
// 同一时刻同一请求，两种格式解析出的 checkpoint 必须等价。
func TestDecoderRealCheckpointPair(t *testing.T) {
	bsonRaw, err := os.ReadFile("../../testdata/sync/checkpoint-pair.bson")
	if err != nil {
		t.Fatalf("读取 bson fixture: %v", err)
	}
	ndjsonRaw, err := os.ReadFile("../../testdata/sync/checkpoint-pair.ndjson")
	if err != nil {
		t.Fatalf("读取 ndjson fixture: %v", err)
	}

	// BSON → 行
	var bsonLines []protocol.Line
	dec := bson.NewDecoder(bytes.NewReader(bsonRaw))
	for {
		doc, err := dec.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("bson Next: %v", err)
		}
		line, err := protocol.ParseLine(doc)
		if err != nil {
			t.Fatalf("ParseLine(bson): %v (%s)", err, doc)
		}
		bsonLines = append(bsonLines, line)
	}
	// NDJSON → 行
	var ndLines []protocol.Line
	nd := protocol.NewDecoder(bytes.NewReader(ndjsonRaw), 0)
	for {
		line, err := nd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ndjson Next: %v", err)
		}
		ndLines = append(ndLines, line)
	}

	if len(bsonLines) < 2 || len(ndLines) < 2 {
		t.Fatalf("行数异常: bson=%d ndjson=%d", len(bsonLines), len(ndLines))
	}
	b0, ok := bsonLines[0].(*protocol.CheckpointLine)
	if !ok {
		t.Fatalf("BSON 第 0 行 = %T", bsonLines[0])
	}
	n0, ok := ndLines[0].(*protocol.CheckpointLine)
	if !ok {
		t.Fatalf("NDJSON 第 0 行 = %T", ndLines[0])
	}

	if b0.LastOpID != n0.LastOpID {
		t.Errorf("last_op_id: bson=%s ndjson=%s", b0.LastOpID, n0.LastOpID)
	}
	if len(b0.Buckets) != len(n0.Buckets) {
		t.Fatalf("bucket 数: bson=%d ndjson=%d", len(b0.Buckets), len(n0.Buckets))
	}
	for i := range n0.Buckets {
		b, n := b0.Buckets[i], n0.Buckets[i]
		if b.Bucket != n.Bucket {
			t.Errorf("bucket[%d]: bson=%q ndjson=%q", i, b.Bucket, n.Bucket)
		}
		if b.Checksum32() != n.Checksum32() {
			t.Errorf("bucket[%d] checksum: bson=%d ndjson=%d", i, b.Checksum32(), n.Checksum32())
		}
		if b.Count != n.Count || b.Priority != n.Priority {
			t.Errorf("bucket[%d] count/priority: bson=%d/%d ndjson=%d/%d", i, b.Count, b.Priority, n.Count, n.Priority)
		}
	}
	if len(b0.Streams) != len(n0.Streams) {
		t.Errorf("streams 数: bson=%d ndjson=%d", len(b0.Streams), len(n0.Streams))
	}

	// 第 2 条（BSON 里可能带 keepalive）必须是 checkpoint_complete，且 last_op_id 一致。
	var bComplete, nComplete *protocol.CheckpointCompleteLine
	for _, l := range bsonLines {
		if c, ok := l.(*protocol.CheckpointCompleteLine); ok {
			bComplete = c
		}
	}
	for _, l := range ndLines {
		if c, ok := l.(*protocol.CheckpointCompleteLine); ok {
			nComplete = c
		}
	}
	if bComplete == nil || nComplete == nil {
		t.Fatalf("缺少 checkpoint_complete: bson=%v ndjson=%v", bComplete, nComplete)
	}
	if bComplete.LastOpID != nComplete.LastOpID {
		t.Errorf("complete last_op_id: bson=%s ndjson=%s", bComplete.LastOpID, nComplete.LastOpID)
	}
}

// TestDecoderStream 覆盖多文档连续读取与 EOF。
func TestDecoderStream(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(doc(eString("a", "1")))
	buf.Write(doc(eInt32("b", 2)))
	buf.Write([]byte{}) // 无尾随字节

	dec := bson.NewDecoder(&buf)
	first, err := dec.Next()
	if err != nil {
		t.Fatalf("第 1 个文档: %v", err)
	}
	if string(first) != `{"a":"1"}` {
		t.Errorf("第 1 个 = %s", first)
	}
	second, err := dec.Next()
	if err != nil {
		t.Fatalf("第 2 个文档: %v", err)
	}
	if string(second) != `{"b":2}` {
		t.Errorf("第 2 个 = %s", second)
	}
	if _, err := dec.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("结束时 err = %v, want io.EOF", err)
	}
}

// TestDecoderErrors 覆盖坏数据。
func TestDecoderErrors(t *testing.T) {
	cases := map[string][]byte{
		"长度过小":    {1, 0, 0, 0},
		"长度与内容不符": append([]byte{9, 0, 0, 0}, 0x0A),
		"缺少终止符":   {8, 0, 0, 0, 0x0A, 'x', 0, 0x01},
		"未知元素类型":  doc(elem{"x", 0x42, nil}),
	}
	for name, raw := range cases {
		if _, err := bson.DecodeDocument(raw); err == nil {
			t.Errorf("%s: 期望报错", name)
		}
	}

	// 空文档是合法的。
	if v, err := bson.DecodeDocument([]byte{5, 0, 0, 0, 0}); err != nil || len(v) != 0 {
		t.Errorf("空文档: %v %v", v, err)
	}

	// 流被截断：长度前缀说 100 字节，实际只给 10 字节。
	truncated := []byte{100, 0, 0, 0, 0x0A, 'x', 0, 0, 0, 0}
	if _, err := bson.NewDecoder(bytes.NewReader(truncated)).Next(); err == nil {
		t.Error("截断的文档应报错")
	}

	// 长度超过上限。
	huge := make([]byte, 4)
	binary.LittleEndian.PutUint32(huge, bson.MaxDocumentSize+1)
	if _, err := bson.NewDecoder(bytes.NewReader(huge)).Next(); !errors.Is(err, bson.ErrInvalidDocument) {
		t.Errorf("超大文档 err = %v", err)
	}
}
