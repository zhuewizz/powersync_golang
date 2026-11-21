// Package protocol 定义 PowerSync 自建服务的线协议类型与编解码。
//
// 这里只放 DTO 与解析逻辑，不含同步业务逻辑。字段名与实测线协议严格一致，
// 实测样本见仓库根目录 testdata/sync/。
//
// 兼容性提示：本项目对接的 sync-streams 协议使用 object_type/object_id/subkey，
// 与部分旧文档/旧 SDK 中的 type/id/key 不同。两种都在此支持，读取时用
// Op.Table/Op.RowID/Op.Subkey 取规范化值。
package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// OpID 是服务端的 64 位操作序号。线协议中一律以十进制字符串表示（"24387"），
// 但为兼容其他实现，解析时也接受 JSON 数字。始终以字符串形式序列化。
type OpID int64

// Zero is the OpID used for "sync this bucket from the start".
const Zero OpID = 0

func (o OpID) String() string { return strconv.FormatInt(int64(o), 10) }

func (o *OpID) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*o = 0
		return nil
	}
	if b[0] == '"' {
		s := string(b[1 : len(b)-1])
		if s == "" {
			*o = 0
			return nil
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("protocol: invalid op id %q: %w", s, err)
		}
		*o = OpID(v)
		return nil
	}
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("protocol: invalid op id %s: %w", b, err)
	}
	*o = OpID(v)
	return nil
}

func (o OpID) MarshalJSON() ([]byte, error) { return json.Marshal(o.String()) }

// OpType 是 bucket 内单条 op 的类型。
type OpType string

const (
	OpPut    OpType = "PUT"
	OpPatch  OpType = "PATCH" // 仅上传方向使用，下载方向未实测
	OpDelete OpType = "DELETE"
	OpRemove OpType = "REMOVE"
	OpMove   OpType = "MOVE"  // 未实测：预期用于 subkey 变更
	OpClear  OpType = "CLEAR" // 未实测：bucket 整体清空（首同步时可能出现）
)

// Op 是 bucket 内的一条变更。
type Op struct {
	OpID OpID   `json:"op_id"`
	Op   OpType `json:"op"`

	// sync-streams 协议字段（实测）
	ObjectType string `json:"object_type,omitempty"`
	ObjectID   string `json:"object_id,omitempty"`
	Subkey     string `json:"subkey,omitempty"`

	// 旧协议字段（未实测，仅兼容解析）
	LegacyType string `json:"type,omitempty"`
	LegacyID   string `json:"id,omitempty"`
	LegacyKey  string `json:"key,omitempty"`

	// Checksum 是该行新状态的 32 位校验和。服务端会把它序列化成带符号数
	// （实测出现过 -96505511 和 3272744065 两种），所以这里用 int64 承接，
	// 需要无符号语义时用 Checksum32()。
	Checksum int64 `json:"checksum,omitempty"`

	// Data 保留原始字节：raw_data=true 时为 JSON 字符串，否则为 JSON 对象。
	// 用 DecodeData 取到规范化的对象字节。
	Data json.RawMessage `json:"data,omitempty"`

	// Old / Metadata 仅在上传方向或开启跟踪时出现。
	Old      json.RawMessage `json:"old,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// Table 返回该 op 影响的表名（兼容新旧字段）。
func (o *Op) Table() string {
	if o.ObjectType != "" {
		return o.ObjectType
	}
	return o.LegacyType
}

// RowID 返回该 op 影响的行 id（兼容新旧字段）。
func (o *Op) RowID() string {
	if o.ObjectID != "" {
		return o.ObjectID
	}
	if o.LegacyID != "" {
		return o.LegacyID
	}
	return o.LegacyKey
}

// DecodeData 返回行数据字节（JSON 对象）。Data 为 JSON 字符串时先解包，
// 为对象时原样返回，为空时返回 nil。
func (o *Op) DecodeData() (json.RawMessage, error) {
	return unwrapJSONString(o.Data)
}

// Checksum32 返回校验和的无符号 32 位视图。
func (o *Op) Checksum32() uint32 { return uint32(o.Checksum) }

// Checksum32 返回校验和的无符号 32 位视图。
func (b *BucketState) Checksum32() uint32 { return uint32(b.Checksum) }

// IsDelete 报告该 op 是否删除行。
func (o *Op) IsDelete() bool { return o.Op == OpRemove || o.Op == OpDelete }

// unwrapJSONString 把 "{\"a\":1}" 还原成 {"a":1}；若已是对象则原样返回。
func unwrapJSONString(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] != '"' {
		return trimmed, nil
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return nil, fmt.Errorf("protocol: unwrap data: %w", err)
	}
	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("protocol: data payload is not valid JSON (%d bytes)", len(s))
	}
	return json.RawMessage(s), nil
}

// BucketState 是 checkpoint 中单个 bucket 的状态。
type BucketState struct {
	Bucket string `json:"bucket"`
	// Checksum 同 Op.Checksum：带符号 32 位，用 Checksum32() 取无符号值。
	Checksum int64 `json:"checksum"`
	Count    int64 `json:"count"`
	// Priority 是新协议的 priority bucket 分组（实测默认 3）。
	Priority      int                  `json:"priority,omitempty"`
	Subscriptions []BucketSubscription `json:"subscriptions,omitempty"`
}

// BucketSubscription 描述 bucket 由哪些订阅引入。实测形如 {"default":0|1}。
type BucketSubscription struct {
	Default  *int   `json:"default,omitempty"`
	Stream   string `json:"stream,omitempty"`
	Priority *int   `json:"priority,omitempty"`
}

// StreamState 是 checkpoint 中 stream 的状态。Errors 保留原始结构（未实测非空形态）。
type StreamState struct {
	Name      string            `json:"name"`
	IsDefault bool              `json:"is_default"`
	Errors    []json.RawMessage `json:"errors,omitempty"`
}

// Checkpoint 是一个 checkpoint 的完整描述。
type Checkpoint struct {
	LastOpID OpID          `json:"last_op_id"`
	Buckets  []BucketState `json:"buckets"`
	Streams  []StreamState `json:"streams,omitempty"`
	// WriteCheckpoint 在新式（checkpoint_request）模式下缺省，仅 legacy 模式出现。
	WriteCheckpoint string `json:"write_checkpoint,omitempty"`
}
