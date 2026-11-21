package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Kind 是流内行类型的标识（即该行唯一顶层键）。
type Kind string

const (
	KindCheckpoint                Kind = "checkpoint"
	KindCheckpointDiff            Kind = "checkpoint_diff"
	KindData                      Kind = "data"
	KindCheckpointComplete        Kind = "checkpoint_complete"
	KindPartialCheckpointComplete Kind = "partial_checkpoint_complete"
	KindTokenExpiresIn            Kind = "token_expires_in"
)

// Line 是 /sync/stream 返回的一行。
type Line interface {
	// Kind 返回行类型；未知类型返回其顶层键名（见 UnknownLine）。
	Kind() Kind
	// Raw 返回该行的原始 JSON 字节。
	Raw() json.RawMessage
}

type lineBase struct{ raw json.RawMessage }

func (l lineBase) Raw() json.RawMessage { return l.raw }

// CheckpointLine 对应 {"checkpoint":{...}}：一个 checkpoint 的起点。
type CheckpointLine struct {
	lineBase
	Checkpoint
}

func (*CheckpointLine) Kind() Kind { return KindCheckpoint }

// CheckpointDiffLine 对应 {"checkpoint_diff":{...}}。
//
// 注意：该行在本次实测（2026-11）中未出现，字段名按上游 SDK 类型定义保留。
type CheckpointDiffLine struct {
	lineBase
	LastOpID        OpID          `json:"last_op_id"`
	UpdatedBuckets  []BucketState `json:"updated_buckets"`
	RemovedBuckets  []string      `json:"removed_buckets"`
	WriteCheckpoint string        `json:"write_checkpoint,omitempty"`
}

func (*CheckpointDiffLine) Kind() Kind { return KindCheckpointDiff }

// DataLine 对应 {"data":{...}}：单个 bucket 的一批 op。
//
// 同一个 checkpoint 内同一 bucket 可能出现多行，靠 After/HasMore 分页。
type DataLine struct {
	lineBase
	Bucket  string `json:"bucket"`
	After   OpID   `json:"after"`
	HasMore bool   `json:"has_more"`
	Ops     []Op   `json:"data"`
}

func (*DataLine) Kind() Kind { return KindData }

// CheckpointCompleteLine 对应 {"checkpoint_complete":{...}}。
type CheckpointCompleteLine struct {
	lineBase
	LastOpID OpID `json:"last_op_id"`
	// CheckpointRequestID 仅部分服务端版本返回。
	CheckpointRequestID *string `json:"checkpoint_request_id,omitempty"`
}

func (*CheckpointCompleteLine) Kind() Kind { return KindCheckpointComplete }

// PartialCheckpointCompleteLine 对应 {"partial_checkpoint_complete":{...}}。
//
// 未实测（按 priority 分批完成时使用）。
type PartialCheckpointCompleteLine struct {
	lineBase
	LastOpID OpID `json:"last_op_id"`
	Priority int  `json:"priority"`
}

func (*PartialCheckpointCompleteLine) Kind() Kind { return KindPartialCheckpointComplete }

// TokenExpiresInLine 对应 {"token_expires_in":N}：keepalive + 提前刷新凭据信号。
type TokenExpiresInLine struct {
	lineBase
	Seconds int64
}

func (*TokenExpiresInLine) Kind() Kind { return KindTokenExpiresIn }

// UnknownLine 保留无法识别的行，便于上层记录日志后继续处理后续行。
type UnknownLine struct {
	lineBase
	Name Kind
}

func (l *UnknownLine) Kind() Kind { return l.Name }

// ErrLineTooLong 表示单行超过 Decoder 的上限。
var ErrLineTooLong = errors.New("protocol: sync line exceeds max size")

// DecodeError 包装单行解析失败，保留原始字节以便日志与复现。
type DecodeError struct {
	Line []byte
	Err  error
}

func (e *DecodeError) Error() string {
	l := e.Line
	if len(l) > 200 {
		l = append(append([]byte{}, l[:200]...), "..."...)
	}
	return fmt.Sprintf("protocol: decode sync line: %v (line=%s)", e.Err, l)
}

func (e *DecodeError) Unwrap() error { return e.Err }

// DefaultMaxLineSize 是单行上限。实测单行最大 1.2 MB，这里留足余量。
const DefaultMaxLineSize = 32 << 20

// Decoder 按行解码 /sync/stream 的 NDJSON 流。
type Decoder struct {
	br  *bufio.Reader
	max int
}

// NewDecoder 创建解码器。maxLineSize <= 0 时使用 DefaultMaxLineSize。
func NewDecoder(r io.Reader, maxLineSize int) *Decoder {
	if maxLineSize <= 0 {
		maxLineSize = DefaultMaxLineSize
	}
	return &Decoder{br: bufio.NewReaderSize(r, 64<<10), max: maxLineSize}
}

// Next 返回下一行。流结束时返回 io.EOF。空白行会被跳过。
func (d *Decoder) Next() (Line, error) {
	for {
		raw, err := d.readLine()
		if err != nil {
			return nil, err
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			continue
		}
		line, perr := ParseLine(raw)
		if perr != nil {
			return nil, &DecodeError{Line: raw, Err: perr}
		}
		return line, nil
	}
}

func (d *Decoder) readLine() ([]byte, error) {
	var buf []byte
	for {
		chunk, err := d.br.ReadSlice('\n')
		if len(buf)+len(chunk) > d.max {
			return nil, ErrLineTooLong
		}
		buf = append(buf, chunk...)
		switch {
		case err == nil:
			return buf, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(buf) == 0 {
				return nil, io.EOF
			}
			return buf, nil
		default:
			return nil, err
		}
	}
}

// ParseLine 解析一行 NDJSON。未知顶层键返回 UnknownLine 而不是错误，
// 以便服务端新增行类型时客户端仍能继续同步。
func ParseLine(raw []byte) (Line, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, errors.New("empty JSON object")
	}

	base := lineBase{raw: append(json.RawMessage(nil), raw...)}

	// 按已知类型优先级匹配；多键行只取优先级最高者（服务端目前每行一个键）。
	if v, ok := fields[string(KindCheckpoint)]; ok {
		l := &CheckpointLine{lineBase: base}
		if err := json.Unmarshal(v, &l.Checkpoint); err != nil {
			return nil, fmt.Errorf("checkpoint: %w", err)
		}
		return l, nil
	}
	if v, ok := fields[string(KindData)]; ok {
		l := &DataLine{lineBase: base}
		if err := json.Unmarshal(v, l); err != nil {
			return nil, fmt.Errorf("data: %w", err)
		}
		return l, nil
	}
	if v, ok := fields[string(KindCheckpointComplete)]; ok {
		l := &CheckpointCompleteLine{lineBase: base}
		if err := json.Unmarshal(v, l); err != nil {
			return nil, fmt.Errorf("checkpoint_complete: %w", err)
		}
		return l, nil
	}
	if v, ok := fields[string(KindCheckpointDiff)]; ok {
		l := &CheckpointDiffLine{lineBase: base}
		if err := json.Unmarshal(v, l); err != nil {
			return nil, fmt.Errorf("checkpoint_diff: %w", err)
		}
		return l, nil
	}
	if v, ok := fields[string(KindPartialCheckpointComplete)]; ok {
		l := &PartialCheckpointCompleteLine{lineBase: base}
		if err := json.Unmarshal(v, l); err != nil {
			return nil, fmt.Errorf("partial_checkpoint_complete: %w", err)
		}
		return l, nil
	}
	if v, ok := fields[string(KindTokenExpiresIn)]; ok {
		var secs int64
		if err := json.Unmarshal(v, &secs); err != nil {
			return nil, fmt.Errorf("token_expires_in: %w", err)
		}
		return &TokenExpiresInLine{lineBase: base, Seconds: secs}, nil
	}

	// 未识别：取字典序最小的键作为名字，保证行为可复现。
	name := ""
	for k := range fields {
		if name == "" || k < name {
			name = k
		}
	}
	return &UnknownLine{lineBase: base, Name: Kind(name)}, nil
}
