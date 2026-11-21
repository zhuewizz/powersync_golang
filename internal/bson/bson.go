// Package bson 实现一个足够用的 BSON 解码器（只解码，不编码）。
//
// 用途：PowerSync 服务的 /sync/stream 支持 `application/vnd.powersync.bson-stream`，
// 每个文档自带 4 字节小端长度前缀，可以直接从流里按长度切分。解码成 JSON 之后
// 复用 internal/protocol 的行解析，保证与 NDJSON 走完全相同的代码路径。
package bson

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"
)

// ErrInvalidDocument 表示 BSON 结构非法（长度/类型/终止符不对）。
var ErrInvalidDocument = errors.New("bson: invalid document")

// MaxDocumentSize 是单个文档的上限（PowerSync 单行可达 MB 级）。
const MaxDocumentSize = 64 << 20

// Decoder 连续解码流里的 BSON 文档。
type Decoder struct {
	r io.Reader
}

// NewDecoder 创建解码器。
func NewDecoder(r io.Reader) *Decoder { return &Decoder{r: r} }

// Next 返回下一个文档的 JSON 编码；流结束时返回 io.EOF。
func (d *Decoder) Next() (json.RawMessage, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(d.r, lenBuf[:]); err != nil {
		// io.ReadFull 在"一个字节都没读到"时返回 io.EOF（正常结束）；
		// 只读到一部分时返回 io.ErrUnexpectedEOF（流被截断），必须原样上报。
		return nil, err
	}
	size := int(int32(binary.LittleEndian.Uint32(lenBuf[:])))
	if size < 5 || size > MaxDocumentSize {
		return nil, fmt.Errorf("%w: bad length %d", ErrInvalidDocument, size)
	}
	doc := make([]byte, size)
	copy(doc, lenBuf[:])
	if _, err := io.ReadFull(d.r, doc[4:]); err != nil {
		return nil, err
	}
	return MarshalDocumentJSON(doc)
}

// MarshalDocumentJSON 把单个 BSON 文档解码并序列化成 JSON 对象。
func MarshalDocumentJSON(doc []byte) (json.RawMessage, error) {
	v, err := DecodeDocument(doc)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// DecodeDocument 把 BSON 文档解码成 map（数组会解码成 []any）。
func DecodeDocument(doc []byte) (map[string]any, error) {
	if len(doc) < 5 {
		return nil, fmt.Errorf("%w: too short", ErrInvalidDocument)
	}
	if size := int(int32(binary.LittleEndian.Uint32(doc))); size != len(doc) {
		return nil, fmt.Errorf("%w: length mismatch (%d vs %d)", ErrInvalidDocument, size, len(doc))
	}
	if doc[len(doc)-1] != 0 {
		return nil, fmt.Errorf("%w: missing terminator", ErrInvalidDocument)
	}
	return decodeElements(doc[4 : len(doc)-1])
}

func decodeElements(buf []byte) (map[string]any, error) {
	out := make(map[string]any)
	for len(buf) > 0 {
		if buf[0] == 0 {
			break
		}
		typ := buf[0]
		name, rest, err := readCString(buf[1:])
		if err != nil {
			return nil, err
		}
		val, rest2, err := decodeValue(typ, rest)
		if err != nil {
			return nil, fmt.Errorf("bson: 字段 %q: %w", name, err)
		}
		out[name] = val
		buf = rest2
	}
	return out, nil
}

func readCString(buf []byte) (string, []byte, error) {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i]), buf[i+1:], nil
		}
	}
	return "", nil, fmt.Errorf("%w: unterminated cstring", ErrInvalidDocument)
}

func decodeValue(typ byte, buf []byte) (any, []byte, error) {
	switch typ {
	case 0x01: // double
		if len(buf) < 8 {
			return nil, nil, ErrInvalidDocument
		}
		bits := binary.LittleEndian.Uint64(buf)
		return math.Float64frombits(bits), buf[8:], nil
	case 0x02, 0x0D, 0x0E: // string / javascript code / symbol
		return readString(buf)
	case 0x03: // embedded document
		if len(buf) < 4 {
			return nil, nil, ErrInvalidDocument
		}
		size := int(int32(binary.LittleEndian.Uint32(buf)))
		if size < 5 || size > len(buf) {
			return nil, nil, ErrInvalidDocument
		}
		doc, err := DecodeDocument(buf[:size])
		return doc, buf[size:], err
	case 0x04: // array（key 是 "0","1",... 的文档）
		if len(buf) < 4 {
			return nil, nil, ErrInvalidDocument
		}
		size := int(int32(binary.LittleEndian.Uint32(buf)))
		if size < 5 || size > len(buf) {
			return nil, nil, ErrInvalidDocument
		}
		doc, err := DecodeDocument(buf[:size])
		if err != nil {
			return nil, nil, err
		}
		keys := make([]string, 0, len(doc))
		for k := range doc {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			a, errA := strconv.Atoi(keys[i])
			b, errB := strconv.Atoi(keys[j])
			if errA == nil && errB == nil {
				return a < b
			}
			return keys[i] < keys[j]
		})
		arr := make([]any, 0, len(keys))
		for _, k := range keys {
			arr = append(arr, doc[k])
		}
		return arr, buf[size:], nil
	case 0x05: // binary
		if len(buf) < 5 {
			return nil, nil, ErrInvalidDocument
		}
		size := int(int32(binary.LittleEndian.Uint32(buf)))
		if size < 0 || len(buf) < 5+size {
			return nil, nil, ErrInvalidDocument
		}
		subtype := buf[4]
		data := buf[5 : 5+size]
		if subtype == 0x00 {
			// 普通二进制：若是合法 UTF-8 JSON，按原样返回；否则 base64。
			if json.Valid(data) {
				return json.RawMessage(append([]byte(nil), data...)), buf[5+size:], nil
			}
		}
		return base64.StdEncoding.EncodeToString(data), buf[5+size:], nil
	case 0x06: // undefined
		return nil, buf, nil
	case 0x07: // ObjectId
		if len(buf) < 12 {
			return nil, nil, ErrInvalidDocument
		}
		return hex.EncodeToString(buf[:12]), buf[12:], nil
	case 0x08: // bool
		if len(buf) < 1 {
			return nil, nil, ErrInvalidDocument
		}
		return buf[0] != 0, buf[1:], nil
	case 0x09: // UTC datetime（毫秒）
		if len(buf) < 8 {
			return nil, nil, ErrInvalidDocument
		}
		ms := int64(binary.LittleEndian.Uint64(buf))
		return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano), buf[8:], nil
	case 0x0A: // null
		return nil, buf, nil
	case 0x0B: // regex
		pattern, rest, err := readCString(buf)
		if err != nil {
			return nil, nil, err
		}
		options, rest2, err := readCString(rest)
		if err != nil {
			return nil, nil, err
		}
		return map[string]any{"pattern": pattern, "options": options}, rest2, nil
	case 0x0C: // DBPointer
		if len(buf) < 4 {
			return nil, nil, ErrInvalidDocument
		}
		size := int(int32(binary.LittleEndian.Uint32(buf)))
		if size < 0 || len(buf) < 4+size+12 {
			return nil, nil, ErrInvalidDocument
		}
		return hex.EncodeToString(buf[4+size : 4+size+12]), buf[4+size+12:], nil
	case 0x0F: // code with scope
		if len(buf) < 4 {
			return nil, nil, ErrInvalidDocument
		}
		total := int(int32(binary.LittleEndian.Uint32(buf)))
		if total < 14 || total > len(buf) {
			return nil, nil, ErrInvalidDocument
		}
		code, rest, err := readString(buf[4:])
		if err != nil {
			return nil, nil, err
		}
		_ = rest
		return code, buf[total:], nil
	case 0x10: // int32
		if len(buf) < 4 {
			return nil, nil, ErrInvalidDocument
		}
		return int32(binary.LittleEndian.Uint32(buf)), buf[4:], nil
	case 0x11: // timestamp（高 32 位秒，低 32 位序号）
		if len(buf) < 8 {
			return nil, nil, ErrInvalidDocument
		}
		v := binary.LittleEndian.Uint64(buf)
		return int64(v >> 32), buf[8:], nil
	case 0x12: // int64
		if len(buf) < 8 {
			return nil, nil, ErrInvalidDocument
		}
		return int64(binary.LittleEndian.Uint64(buf)), buf[8:], nil
	case 0x13: // decimal128（按十六进制透传，当前协议不用）
		if len(buf) < 16 {
			return nil, nil, ErrInvalidDocument
		}
		return hex.EncodeToString(buf[:16]), buf[16:], nil
	case 0xFF: // min key
		return nil, buf, nil
	case 0x7F: // max key
		return nil, buf, nil
	default:
		return nil, nil, fmt.Errorf("%w: unsupported element type 0x%02x", ErrInvalidDocument, typ)
	}
}

func readString(buf []byte) (string, []byte, error) {
	if len(buf) < 4 {
		return "", nil, ErrInvalidDocument
	}
	size := int(int32(binary.LittleEndian.Uint32(buf)))
	if size < 1 || len(buf) < 4+size {
		return "", nil, ErrInvalidDocument
	}
	s := buf[4 : 4+size-1] // 末尾带 \0
	return string(s), buf[4+size:], nil
}
