package protocol

import (
	"encoding/json"
	"net/http"
)

// StreamRequest 是 POST /sync/stream 的请求体（实测字段）。
//
// 注意：buckets 中未列出的 bucket 服务端会按 after=0 全量下发；after 语义为
// 左开区间（after=22775 → 从 22776 开始）。
type StreamRequest struct {
	Buckets []BucketRequest `json:"buckets"`
	// IncludeChecksum 请求服务端在 checkpoint 中返回 bucket checksum（实测总会返回）。
	IncludeChecksum bool `json:"include_checksum"`
	// RawData=true 时每行 op 的 data 为 JSON 字符串，false 时为 JSON 对象。
	RawData     bool              `json:"raw_data"`
	AppMetadata map[string]string `json:"app_metadata,omitempty"`
	Parameters  map[string]any    `json:"parameters,omitempty"`
	ClientID    string            `json:"client_id,omitempty"`
	Streams     *StreamSubscribes `json:"streams,omitempty"`
}

// BucketRequest 是单个 bucket 的同步起点。
type BucketRequest struct {
	Name  string `json:"name"`
	After OpID   `json:"after"`
}

// StreamSubscribes 是 sync streams 的订阅配置。
//
// 注意：实测服务端要求 `subscriptions` **属性必须存在**（哪怕空数组），
// 否则 400："/streams must have required property 'subscriptions'"。
type StreamSubscribes struct {
	IncludeDefaults bool                 `json:"include_defaults"`
	Subscriptions   []StreamSubscription `json:"subscriptions"`
}

// StreamSubscription 是一条显式 stream 订阅。
//
// 注意：实测服务端把 override_priority 当作**必填属性**（缺了会 400：
// "must have required property 'override_priority'"），所以这里不带 omitempty，
// 未设置时序列化成 null（表示用流自己的优先级）。
type StreamSubscription struct {
	Stream           string         `json:"stream"`
	Parameters       map[string]any `json:"parameters"`
	OverridePriority *int           `json:"override_priority"`
}

// CheckpointRequest 是 POST /sync/checkpoint-request 的请求体。
type CheckpointRequest struct {
	ClientID            string `json:"client_id"`
	CheckpointRequestID string `json:"checkpoint_request_id"`
}

// CheckpointRequestResponse 是 checkpoint-request 的响应。
type CheckpointRequestResponse struct {
	Data struct {
		CheckpointRequestID string `json:"checkpoint_request_id"`
	} `json:"data"`
}

// WriteCheckpointResponse 是 legacy GET /write-checkpoint2.json 的响应。
type WriteCheckpointResponse struct {
	Data struct {
		WriteCheckpoint OpID `json:"write_checkpoint"`
	} `json:"data"`
}

// Error 是服务端统一错误信封。
//
// 实测：{"error":{"code":"PSYNC_S2106","status":401,"description":"Authentication required",
// "name":"AuthorizationError"}}。部分错误还带 details/severity。
type Error struct {
	Code        string `json:"code"`
	Status      int    `json:"status"`
	Description string `json:"description"`
	Name        string `json:"name"`
	Details     string `json:"details,omitempty"`
	Severity    string `json:"severity,omitempty"`
}

// ErrorResponse 是错误信封的外层。
type ErrorResponse struct {
	Error Error `json:"error"`
}

// Unauthorized 报告该错误是否表示凭据无效/过期。
func (e *Error) Unauthorized() bool {
	return e.Status == http.StatusUnauthorized || e.Name == "AuthorizationError"
}

// DecodeErrorResponse 尝试解析错误响应体。解析失败时返回 nil。
func DecodeErrorResponse(body []byte) *Error {
	var r ErrorResponse
	if err := json.Unmarshal(body, &r); err != nil || r.Error.Status == 0 && r.Error.Code == "" {
		return nil
	}
	return &r.Error
}
