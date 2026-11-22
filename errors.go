package powersync

import (
	"context"
	"errors"

	"github.com/zhuweizz/powersync_golang/internal/core"
)

// 导出的哨兵错误，便于 errors.Is 判断。
var (
	// ErrClosed 表示数据库已关闭。
	ErrClosed = errors.New("powersync: database is closed")
	// ErrNotConnected 表示当前没有可用的同步连接（Connect 未调用或已 Disconnect）。
	ErrNotConnected = errors.New("powersync: not connected")
	// ErrSchemaRequired 表示没有提供 schema。PowerSync 要求实例化时必须给出客户端 schema
	// （官方 SDK 同样如此）：它决定本地建哪些表/视图，服务端下发但未声明的表会被跳过。
	ErrSchemaRequired = errors.New("powersync: Options.Schema is required（PowerSync 要求客户端必须声明 schema）")
	// ErrInvalidDirectory 表示 Filename 指向的目录不可用。
	ErrInvalidDirectory = errors.New("powersync: invalid database path")
	// ErrDatabaseLocked 表示同一数据库文件已被另一个实例（进程/标签页）打开。
	ErrDatabaseLocked = core.ErrDatabaseLocked
)

// Credentials 是同步用的服务端地址与 JWT。
type Credentials struct {
	// Endpoint 是 PowerSync 服务地址（如 https://example.com 或 http://host:8080）。
	Endpoint string
	// Token 是短时 JWT（服务端按 `Authorization: Token <jwt>` 校验）。
	Token string
}

// Connector 由使用方实现：提供凭据 + 把本地变更上传到后端。
//
// 典型实现：
//   - FetchCredentials：调用自己的登录/凭据接口拿到 PowerSync JWT；
//   - UploadData：db.GetCrudBatch / GetNextCrudTransaction 取队列，写自己的后端，
//     成功后调用 batch.Complete（传服务端返回的 write checkpoint 可选）。
type Connector interface {
	FetchCredentials(ctx context.Context) (Credentials, error)
	UploadData(ctx context.Context, db *DB) error
}

// CredentialInvalidator 可选实现：SDK 收到 401 时会调用它让实现丢弃缓存。
type CredentialInvalidator interface {
	InvalidateCredentials()
}
