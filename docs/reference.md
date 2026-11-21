# msmkSync 使用手册（Go 客户端）

> 面向使用者的完整 API 参考。结构对齐官方 [Dart/Flutter SDK 参考](https://docs.powersync.com/client-sdks/reference/flutter)，
> 内容以本仓库的实际实现与实测行为为准。
> 非技术读者的项目状态见 [`../STATUS.md`](../STATUS.md)；给 AI/维护者的硬性约定见 [`../AGENTS.md`](../AGENTS.md)。

**目录**

1. [快速开始](#1-快速开始)
2. [Schema 定义](#2-schema-定义)
3. [打开数据库（Options）](#3-打开数据库options)
4. [连接后端（Connector）](#4-连接后端connector)
5. [读写数据](#5-读写数据)
6. [监听查询（Watch）](#6-监听查询watch)
7. [同步状态](#7-同步状态)
8. [Sync Streams（流订阅）](#8-sync-streams流订阅)
9. [Schema 迁移](#9-schema-迁移)
10. [上传队列](#10-上传队列)
11. [生命周期与多进程](#11-生命周期与多进程)
12. [日志 / 自定义 HTTP](#12-日志--自定义-http)
13. [错误处理与排查](#13-错误处理与排查)
14. [与官方 Flutter 参考的差异](#14-与官方-flutter-参考的差异)

---

## 1. 快速开始

### 安装

```bash
go get msmkSync        # 模块名见 go.mod；包名为 powersync
```

```go
import powersync "msmkSync"
```

要求 **Go 1.25+**、`CGO_ENABLED=0`（SQLite 用 `modernc.org/sqlite`，不需要 cgo）。

### 四步接入

```go
// 1) 定义客户端 schema（表/列/索引）
sch := powersync.NewSchema(
    powersync.Table{
        Name:    "todos",
        Columns: []powersync.Column{
            {Name: "title", Type: powersync.TextColumn},
            {Name: "done",  Type: powersync.IntegerColumn},
        },
        Indexes: []powersync.Index{{Name: "todos_title", Columns: []string{"title"}}},
    },
)

// 2) 打开数据库
db, err := powersync.Open(ctx, powersync.Options{
    Filename:  "app.db",
    Schema:    sch,
    Connector: &myConnector{}, // 3) 见下节
})
if err != nil { /* 处理 */ }
defer db.Close(ctx)

// 4) 连接（可选：先订阅流）
db.Connect(ctx)
if err := db.WaitForFirstSync(ctx); err != nil { /* 超时/取消 */ }

// 之后就可以正常读写；所有写会自动进上传队列
```

---

## 2. Schema 定义

```go
type Table struct {
    Name       string
    ViewName   string      // 对外视图名，默认 = Name
    Columns    []Column
    Indexes    []Index
    Direct     bool        // true = 真实列式表；默认 false = JSON 表 + 视图（与官方一致）
    LocalOnly  bool        // 只在本机存在：不同步、不入上传队列
    InsertOnly bool        // 不生成 UPDATE/DELETE 触发器
    IncludeMetadata bool   // 视图多出 _metadata/_deleted，写操作可带 metadata
}

type Column struct {
    Name string
    Type ColumnType  // TextColumn / IntegerColumn / RealColumn / NumericColumn / BlobColumn
}

type Index struct {
    Name    string
    Columns []string
}
```

- 主键固定是 `id`（TEXT），不用声明；声明了也会被忽略。
- 默认（`Direct=false`）与官方 SDK 同构：数据存在 `ps_data__<表名>`，对外是同名视图，
  本地写通过 `INSTEAD OF` 触发器同时写落盘表和上传队列。
- `LocalOnly` 用 `ps_data_local__<表名>`，只在本机、不参与同步。
- 列名/表名必须是**简单标识符**（字母数字下划线），因为触发器要用 `NEW.<列>` 语法。

### 从官方 SDK 复用 schema

```go
raw, _ := os.ReadFile("schema.json") // 官方 Schema.toJSON() 的输出
sch, err := powersync.LoadSchemaJSON(raw)
```

导出方法见 `scripts/js-reference/dump-schema.mjs`（直接读 JS 端的 `AppSchema.ts`）。

### 可用类型

| Go 常量 | SQLite 类型 | 说明 |
| --- | --- | --- |
| `powersync.TextColumn` | TEXT | 默认 |
| `powersync.IntegerColumn` | INTEGER | |
| `powersync.RealColumn` | REAL | |
| `powersync.NumericColumn` | NUMERIC | |
| `powersync.BlobColumn` | BLOB | |

---

## 3. 打开数据库（Options）

```go
type Options struct {
    Filename  string    // 必填：SQLite 文件路径（:memory: 也支持）
    Schema    *Schema   // **必填**（PowerSync 要求客户端声明 schema）；老库也要传（用于校验/迁移）
    Connector Connector // 可选：不接同步时可以不传，Connect() 会返回 ErrNotConnected

    Logger     *slog.Logger // 默认 slog.Default()
    HTTPClient *http.Client // 默认 http.DefaultClient

    RetryDelay     time.Duration // 重连初始退避，默认 1s，指数增长上限 30s
    UploadInterval time.Duration // 上传队列轮询间隔，默认 1s
    WatchInterval  time.Duration // Watch 的兜底轮询，默认 2s（正常是推送）

    ClientID string // 默认自动生成 UUID 并存 ps_kv

    NoFileLock bool // true = 不加跨进程文件锁（默认加锁，见 §11）
    DisableBSON bool // true = 只用 NDJSON（默认请求 BSON，服务端不支持会回落）
    DisableDefaultStreams bool // true = 不请求默认流（只有显式订阅的流）
    AdditionalHeaders map[string]string // 附加到同步请求的头

    NetworkTarget         string        // 网络探测目标（host:port 或 URL），空=用同步地址
    NetworkCheckInterval  time.Duration // 探测间隔，默认 5s
    NetworkCheckTimeout   time.Duration // 单次拨号超时，默认 2s
    DisableNetworkMonitor bool          // true = 不启动网络监视器
}
```

要点：

- **Schema 是必填项**：`Open` 不给 schema 会返回 `ErrSchemaRequired`（与官方 SDK 一致）。
  schema 决定本地建哪些表/视图；服务端下发但未声明的表会被跳过物化（见 §13）。
- **Schema 变更会自动迁移**：`Open` 时若 `ps_kv.schema_json` 与传入 schema 不同，会经
  `ps_untyped` 暂存 → 重建 → 回填（加列/切 direct/删表都覆盖，删掉的列会丢数据）。
- **同一个 DB 文件只能被一个实例打开**（跨进程文件锁），第二个会返回 `ErrDatabaseLocked`。
  测试/特殊场景可用 `NoFileLock: true` 关掉。

---

## 4. 连接后端（Connector）

```go
type Credentials struct {
    Endpoint string // PowerSync 服务地址，如 https://sync.example.com 或 http://host:8080
    Token    string // 短时 JWT，按 `Authorization: Token <jwt>` 发送
}

type Connector interface {
    FetchCredentials(ctx context.Context) (Credentials, error)
    UploadData(ctx context.Context, db *DB) error
}

// 可选：实现后 401 时会收到回调，让实现丢弃 token 缓存
type CredentialInvalidator interface{ InvalidateCredentials() }
```

### FetchCredentials 注意事项

- SDK **不缓存** token，会在多次建流/重连时反复调用；实现里请自己做缓存。
- 服务端返回 `token_expires_in <= 30s` 时，SDK 会在后台提前调用一次（不影响当前连接）。
- 401 时 SDK 会调用 `InvalidateCredentials()`（若实现）然后重连重取。

### UploadData 实现

SDK 只负责"什么时候上传"，具体上传由你实现。典型流程：

```go
func (c *MyConnector) UploadData(ctx context.Context, db *powersync.DB) error {
    batch, err := db.GetCrudBatch(ctx, 100) // 队列空时返回 nil
    if err != nil || batch == nil {
        return err
    }
    for _, e := range batch.Entries {
        switch e.Op {
        case powersync.UpdateTypePut:
            // e.Data 是"非空列"；PUT 需要自己补 id
            err = upsert(ctx, e.Table, e.ID, e.Data)
        case powersync.UpdateTypePatch:
            // e.Data 只含变化列；被清空的字段是 null
            err = patch(ctx, e.Table, e.ID, e.Data)
        case powersync.UpdateTypeDelete:
            err = del(ctx, e.Table, e.ID)
        }
        if err != nil {
            return err // 返回错误 → SDK 退避后重试同一批
        }
    }
    // 上传成功后出队；writeCheckpoint 可选（见下）
    return batch.Complete(ctx)
}
```

- `CrudEntry` 字段：`ClientID`、`TxID`（同一次 `WriteTransaction` 的写共享）、`Op`（`PUT`/`PATCH`/`DELETE`）、
  `Table`、`ID`、`Data`（map）、`Old`、`Metadata`。
- **按事务原子上传**用 `db.GetNextCrudTransaction(ctx)`（同 `tx_id` 的一批）。
- `Complete(ctx, writeCheckpoint...)`：出队；若传入服务端返回的 write checkpoint，
  SDK 会等该 checkpoint 回来后才应用新的 checkpoint（避免本地刚写的行"闪烁"）。
  取 write checkpoint 的旧式接口：`GET /write-checkpoint2.json?client_id=...`；新式：`POST /sync/checkpoint-request`。
- 上传失败返回 error 即可（SDK 指数退避重试）；**不可重试**的错误（如约束冲突）应自己
  `batch.Complete` 丢弃并记录，避免堵住队列。

---

## 5. 读写数据

```go
// 写（返回受影响行数；注意：JSON 表是视图，SQLite 对视图写恒报 0）
n, err := db.Execute(ctx, `INSERT INTO todos(id, title, done) VALUES(?,?,?)`, "t1", "写代码", 0)

// 批量写（同一事务，同一 SQL）
err = db.ExecuteBatch(ctx, `INSERT INTO todos(id,title) VALUES(?,?)`, [][]any{
    {"t2", "a"}, {"t3", "b"},
})

// 查询多行
rows, err := db.GetAll(ctx, `SELECT * FROM todos WHERE done = ?`, 0) // []map[string]any

// 查询恰好一行：0 行 → ErrNoRows；多行 → ErrMultipleRows
row, err := db.Get(ctx, `SELECT * FROM todos WHERE id = ?`, "t1")

// 查询可选一行：found=false 表示没有
row, found, err := db.GetOptional(ctx, `SELECT * FROM todos WHERE id = ?`, "t1")

// 事务（出错自动回滚；事务内的写共享一个 tx_id）
err = db.WriteTransaction(ctx, func(ctx context.Context, tx *powersync.Tx) error {
    if _, err := tx.Execute(ctx, `UPDATE todos SET done=1 WHERE id=?`, "t1"); err != nil {
        return err
    }
    _, err := tx.Execute(ctx, `INSERT INTO audit(id, msg) VALUES(?,?)`, "a1", "done")
    return err
})
```

要点：

- 值类型：`int64` / `float64` / `string` / `[]byte`（返回时已转 string）/ `nil`。
- 所有写都会自动进上传队列（`LocalOnly` 表除外），并立刻触发 `Watch` 推送。
- **不要在 `WriteTransaction` 内部调用 `db.GetAll`/`db.Execute`**（连接是单连接，会等不到锁）；
  事务内请用 `tx.*`。

---

## 6. 监听查询（Watch）

```go
// 任何写入都会让 Watch 重查一次，结果没变则不推送（64 位哈希去重）
ch, err := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)
for rows := range ch { fmt.Println(rows) }

// 只关心某些表：这些表变了才重查（无关写入连查询都不跑，适合大结果集）
ch2, err := db.WatchTables(ctx, []string{"todos"}, `SELECT * FROM todos ORDER BY id`)
```

语义：

- 初值立即推送；之后每次**结果变化**推送一次（`map` 顺序无关，列顺序/类型差异敏感）。
- 底层是推送：用户表触发器 + 同步物化会更新 `ps_internal_version(tbl, v)`；
  只有 `tables` 里的表版本变了才重跑查询。
- `WatchInterval`（默认 2s）只是兜底轮询，防止漏通知。
- `ctx` 结束后 channel 关闭。
- 查询失败不会关闭 channel，只记日志（下个变更会重试）。

---

## 7. 同步状态

```go
type SyncStatus struct {
    Connected     bool
    Connecting    bool
    Uploading     bool
    LastSyncedAt  time.Time // 最近一次 checkpoint 应用成功的时间
    Buckets       int       // 本地已知 bucket 数
    Cursor        string    // 最近一次应用的 last_op_id
    DownloadError string    // 最近一次下载错误（成功后清空）
    UploadError   string
}

st := db.CurrentStatus()
skipped := db.SkippedUnknownOps()   // >0 表示有表因不在 schema 被跳过（排查用）
ch, err := db.WatchStatus(ctx)            // 状态变化流（初值 + 每次变化）
err = db.WaitForStatus(ctx, func(st powersync.SyncStatus) bool { return st.Connected })
err = db.WaitForFirstSync(ctx)            // 首次同步完成（等价于等一次 checkpoint 应用成功）
```

注意：

- `LastSyncedAt` 非零 ≈ 官方的 `hasSynced`。
- 目前**没有**下载进度百分比（官方 `downloading`），只有 `Buckets`/`Cursor`。
- `Connect(ctx)` 用 `context.WithoutCancel` 起后台循环：传一次性请求 ctx 不会立刻断开，
  停止请用 `Disconnect()` / `Close()`。

---

## 7.5 网络状态监控

同步状态回答“同步是否正常”，网络状态回答“**链路通不通**”（例如拔网线、Wi-Fi 掉线）。

```go
net := db.CurrentNetworkStatus()
// NetworkStatus{Online, CheckedAt, Latency, Target, Err}

ch, err := db.WatchNetworkStatus(ctx)   // 初值 + 可达性变化时推送
for n := range ch {
    log.Printf("online=%v target=%s latency=%s err=%s", n.Online, n.Target, n.Latency, n.Err)
}
```

- 探测方式：对目标地址做 **TCP 拨号**（默认每 5s，超时 2s），只看链路，不判断服务端健康。
- 同步循环在“连上/连不上”时会立刻触发一次重测，所以断网/恢复通常亚秒级反映。
- 目标地址：`Options.NetworkTarget`（host:port 或 URL）；不填则用同步服务地址（连过一次之后才有）。
- 关闭：`Options.DisableNetworkMonitor = true`；调参：`NetworkCheckInterval` / `NetworkCheckTimeout`。
- 401/400 这类“网络通但服务端拒绝”**不会**被当成离线。

---

## 8. Sync Streams（流订阅）

```go
// 订阅一个流（参数会原样发给服务端，一般是 sync rules 里声明的参数）
sub, err := db.SubscribeStream(ctx, "box_data",
    map[string]any{"box_code": "D1-2"},
    powersync.WithPriority(0),      // 数值越小越优先
    powersync.WithTTL(time.Hour),   // 取消后多久可被清理
)

st, err := sub.State(ctx)           // {Active, IsDefault, HasSynced, LastSyncedAt, ...}
err = sub.Unsubscribe(ctx)          // 取消订阅（数据保留）

list, err := db.StreamSubscriptions(ctx) // 所有 active 订阅
```

- 订阅持久化在 `ps_stream_subscriptions`（与官方 core 同一张表/同一列）。
- 下一次建流时随请求发出（`streams.include_defaults=true` + `subscriptions`）。
- 只想同步显式订阅的流：`Options.DisableDefaultStreams = true`。
- 服务端按优先级分批时（`partial_checkpoint_complete`），SDK 只物化该优先级及以上的 bucket。

---

## 9. Schema 迁移

```go
next := powersync.NewSchema( /* 新版表结构 */ )
err := db.UpdateSchema(ctx, next) // 保留数据的前提下升级（会经 ps_untyped 暂存 → 重建 → 回填）
```

- 也可以直接 `Open` 一个已有库并传新 schema —— 与上次记录的 schema 不同就会自动迁移。
- JSON 表加列**不需要**迁移数据（视图列是动态的）；删列/删表会丢数据。
- `Direct` 表与 JSON 表互切会搬数据。
- 迁移不会清空上传队列（`ps_crud`）。

---

## 10. 上传队列

```go
n, err := db.UploadQueue(ctx)        // 待上传条数
db.TriggerUpload()                   // 让上传循环尽快跑一次（本地写已自动触发）

batch, err := db.GetCrudBatch(ctx, 100)  // nil 表示队列空
txn, err := db.GetNextCrudTransaction(ctx) // 只取队首同一事务
```

- `PATCH` 的 `Data` 只含变化列；**被清空的列会是 `null`**（例如 `{"done":null}`）。
- `PUT` 的 `Data` 是"非空列"，不含 `id`（`ID` 单独给）。
- 开了 `IncludeMetadata` 的表，`CrudEntry.Metadata` 会带上写入时的 `_metadata`。
- 开了 `ignore_empty_update` 的表，空 `PATCH` 不会入队。

---

## 11. 生命周期与多进程

```go
db.Connect(ctx)                 // 启动下载+上传（非阻塞）
db.Disconnect()                 // 停止同步（本地数据保留）
err = db.Close(ctx)              // 停止并关闭

// 切换账号/退出登录：断开并清空数据（默认保留 local_only 表）
err = db.DisconnectAndClear(ctx)
err = db.DisconnectAndClear(ctx, powersync.WithClearLocalTables()) // 连 local_only 也清
```

- 库内部 `MaxOpenConns(1)`（modernc 下 WAL/内存库都需要串行）。
- 跨进程用 `<db>.lock`（Unix `flock` / Windows `LockFileEx`）：
  同一 DB 被第二个实例打开会返回 `ErrDatabaseLocked`；`Close()` 后即可重开。
- `:memory:` 不加文件锁。

---

## 12. 日志 / 自定义 HTTP

```go
db, _ := powersync.Open(ctx, powersync.Options{
    Filename:  "app.db",
    Schema:    sch,
    Connector: conn,
    Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
        Level: slog.LevelDebug, // Debug 会打印每条 checkpoint/data 行摘要
    })),
    HTTPClient: &http.Client{Timeout: 60 * time.Second},
    AdditionalHeaders: map[string]string{"X-Gateway-Key": "..."},
})
```

日志里不会出现 token 或完整行数据（行数据只在工具里打印，库本身不打印）。

---

## 13. 错误处理与排查

### 哨兵错误

| 错误 | 含义 / 处理 |
| --- | --- |
| `ErrDatabaseLocked` | 同一 DB 已被另一个实例打开 → 先关掉那个实例，或用 `NoFileLock` |
| `ErrNotConnected` | `Open` 没给 `Connector` 却调用了 `Connect`，或上传时没有 connector |
| `ErrClosed` | 数据库已 `Close` |
| `ErrSchemaRequired` | `UpdateSchema(nil)` |
| `ErrNoRows` / `ErrMultipleRows` | `Get` 期望恰好一行 |
| `ErrInvalidDirectory` | `Filename` 为空 |

### 常见现象

| 现象 | 原因 / 处理 |
| --- | --- |
| `Execute` 返回 0 行 | JSON 表是视图，SQLite 对视图写恒报 0（写入其实成功了，查一下数据确认） |
| 服务端返回 400 `must have required property 'subscriptions'` 或 `'override_priority'` | 自建服务对 `streams` 校验较严；本库已按实测补齐，若自己发请求需带上这两个属性 |
| 日志出现 `未处理的行 kind=checkpoint_diff` | 旧版本会忽略 diff 行；现版本已处理（updated→目标并校验，removed→ResetBucket） |
| 日志出现 `checksum mismatch, restarting iteration` | 本地数据与服务端漂移（或人为改动）→ SDK 自动重置该 bucket 从 `after=0` 重下；偶发一次属正常 |
| 服务端下发但表不在 schema | 会被跳过物化（`db.SkippedUnknownOps()`、`db.SkippedTables()` 可查，不报错）。**不会自动回填**，需把表加进 schema 后重建本地库（`DisconnectAndClear` 或删 .db）。实测：用 AppSchema 的 37 张表时，服务端仍会下发 `i` / `t` / `material_category` / `material_info`（sync rules 比 AppSchema 宽）→ 252 行被跳过 |
| 同步一直不完成 | 检查 token 有效期/权限、服务端是否支持该 endpoint（`/sync/stream`）、网络代理 |
| 上传一直重试 | `UploadData` 返回了错误；打印 `CurrentStatus().UploadError` 看具体原因 |

### 核对"数据到底有没有落到本地"

```bash
# 列出本地库里的表/视图 + 行数（只读打开，不联网）
go run ./cmd/psq -db ./out/verify.db -ls

# 直接查视图（和 SDK 里 db.GetAll 读的是同一份数据）
go run ./cmd/psq -db ./out/verify.db -q "select count(*) as n from cutter_info"
go run ./cmd/psq -db ./out/verify.db -q "select id, mat_code, mat_name from cutter_info limit 3"

# 后台与本地逐行比对（示例里的 -verify 也是这套规则）
go run ./examples/todo -mode real -rest http://host:8089/rest ... -verify
```

同步状态（每个 bucket 已应用到哪个 op）：
```bash
go run ./cmd/psq -db ./out/verify.db -q "select name, last_applied_op, op_checksum from ps_buckets"
```

### 用图形工具打开本地库（DB Browser / SQLiteStudio）

本地库是 **WAL** 模式，直接拿图形工具打开"正在被程序使用"的库文件，
容易看到 `11 - database disk image is malformed` 之类的报错（数据其实没坏）。
安全做法是先生成一份**单文件快照**再打开：

```bash
go run ./cmd/psq -db ./out/verify.db -snapshot ./out/verify-snapshot.db
# 然后用 DB Browser for SQLite / SQLiteStudio 打开 ./out/verify-snapshot.db
```

快照是 `VACUUM INTO` 的结果：`journal_mode=delete`、没有 `-wal`/`-shm`、不会锁原库。

注意：

- **不要复制 `-shm`**（它是进程间共享内存索引，复制过去必然不一致）；
  复制库时只带 `.db`（干净关闭后 WAL 已被 checkpoint 合并，`.db` 自包含）。
- 如果确认库本身有问题，用 `go run ./cmd/psq -db <file> -check` 跑 `PRAGMA integrity_check`。

### 自检工具

| 命令 | 用途 |
| --- | --- |
| `go run ./cmd/psq -db <db> -ls|-q|-check|-snapshot` | 只读查库/完整性检查/生成快照 |
| `go run ./cmd/psdemo -endpoint ... -rpc ... -schema ...` | 公开 API 冒烟（只读） |
| `go run ./cmd/psupload ...` | 上传回显闭环（写一行→上传→回显→删除） |
| `go run ./cmd/psdiff ... -rest ...` | 与后端 PostgREST 逐行比对 |
| `./scripts/run-js-diff.sh` | Go vs 官方 JS SDK 逐行比对 |

---

## 14. 与官方 Flutter 参考的差异

### 本库已有（对应官方条目）

| 官方条目 | 本库 |
| --- | --- |
| 定义 schema / 实例化数据库 | `NewSchema` / `Open` |
| 集成后端（fetchCredentials / uploadData） | `Connector` |
| 订阅同步流 | `SubscribeStream`（+ TTL/优先级） |
| `get()` / `getAll()` / `getOptional()` | `Get` / `GetAll` / `GetOptional` |
| `watch()` | `Watch` / `WatchTables` |
| `execute()` / `writeTransaction()` | `Execute` / `ExecuteBatch` / `WriteTransaction` |
| 等待最新服务器数据 | `WaitForFirstSync` / `WaitForStatus` |
| 配置日志 / 自定义 HTTP | `Logger` / `HTTPClient` / `AdditionalHeaders` |
| 网络状态 | `CurrentNetworkStatus` / `WatchNetworkStatus`（官方 Flutter 参考没有对应 API） |
| `disconnectAndClear()` | `DisconnectAndClear`（`WithClearLocalTables`） |
| `getUploadQueueStats()` | `UploadQueue`（只有条数，无按表统计） |
| Schema 升级 | `UpdateSchema` + 自动迁移 |

### 本库还没有

| 官方能力 | 影响 | 备注 |
| --- | --- | --- |
| 附件（attachments，本地文件缓存/上传/下载） | 无文件同步 | 业务当前只用图片 URL，不需要 |
| 加密数据库（SQLCipher） | — | 可自行在 Filename 上做磁盘加密 |
| 下载进度百分比（`downloading.total/downloaded`） | 只有 `Cursor`/`Buckets` | 需要可以加 |
| `waitForFirstSync({stream})` 按流等待 | 只能全局等待 | |
| 按表的上传队列统计 | `UploadQueue` 只给总数 | |
| 行级 `_metadata` 的全部官方细节 | 只支持"写入时携带 + 删除" | |
| ORM 集成 / Web 多标签页 | — | Go 端不需要 |
| `powersync_control` 等 SQL 控制面函数 | 官方 App 不能拿它当 C 扩展替身 | 双方仍可读写同一个库文件 |

---

## 常见问题（FAQ）

**Q：能只用作纯本地库、不接同步吗？**
可以。`Open` 不传 `Connector`，不调用 `Connect`；用 `GetAll`/`Execute`/`Watch` 即可，
上传队列也能自己读（`GetCrudBatch` + `Complete`）。

**Q：断网/服务端挂了会怎样？**
读写不受影响（全在本地）。SDK 后台按指数退避重连（1s→30s）；恢复后从本地游标续传。
上传失败同样退避重试，队列不丢。

**Q：数据会不会被覆盖丢？**
上传按 `ps_crud.id` 顺序、可重试；下载按 checkpoint 原子应用并校验 checksum。
本地写与上传队列入队在同一事务里完成。

**Q：可以用官方的 Dart/JS App 打开同一个库吗？**
库文件格式（`ps_data__*` + 视图 + 触发器 + `ps_*` 状态表）已对齐官方 core，
**读取**没问题；但官方 App 不能用它的 C 引擎在我们写的库上跑同步（缺 `powersync_control`），
两边各自同步同一份服务端数据是可行的。

**Q：升级本库要注意什么？**
内部表结构有小概率变化（如 `ps_internal_version`），`Open` 时会自动迁移；
`AGENTS.md` §5.2 记录了与官方 core 的差异，升级前建议跑一遍 `cmd/psdiff` 的对照。
