# AGENTS.md

给在本仓库工作的 AI 编码代理（以及新加入的开发者）的上下文与硬性约定。

## 0. 权威来源（改协议/表结构前必读）

本仓库不是凭空设计，而是**对齐上游实现**。遇到不一致时以上游为准：

| 上游仓库 | 用途 |
| --- | --- |
| [powersync-ja/powersync.dart](https://github.com/powersync-ja/powersync.dart) | 主参考：公共 API 形状、同步编排、同步状态机、附件（attachments）|
| [powersync-ja/powersync-dotnet](https://github.com/powersync-ja/powersync-dotnet) | 参考：**不依赖 C 扩展的原生实现**（与本项目最接近），CRUD/上传队列/流处理逻辑 |
| [powersync-ja/powersync-sqlite-core](https://github.com/powersync-ja/powersync-sqlite-core) | 参考：本地 SQLite **表结构、迁移、SQL 函数**语义（`migrations.rs`、`schema/`、`sync/line.rs`）|
| [powersync-ja/powersync-service](https://github.com/powersync-ja/powersync-service) | 服务端；自建服务的 `/sync/stream` 行为以它为准 |
| [PowerSync 文档](https://docs.powersync.com/) | 概念与公开协议说明 |

> 注意：官方**没有 Go SDK**。本仓库是社区/内部实现，必须自己保持与官方 SDK 的**线协议兼容**和（尽量）**本地库格式兼容**。

## 1. 项目定位

- 名称：`powersync_golang`（Go module：`msmkSync`，包名保持 `powersync`）
- 形态：**纯库**（library），不作为可执行程序发布；使用方 `import` 后自行提供 HTTP 客户端配置、后端 connector 与 UI 绑定。
- 目标：实现 PowerSync 官方协议的 Go 客户端——本地 SQLite 为权威数据源，通过 sync rules 生成的 buckets 与服务端做双向增量同步。
- 兼容目标：**PowerSync 自建服务**（self-hosted `powersync-service`）。
- 非目标（除非另行确认）：Web/浏览器端（WASM）、附件上传下载（attachments）、DevTools 协议、多标签页协调。

## 2. 环境与工具链

- Go 最低版本 **1.25**（`go.mod` 写 `go 1.25`），CI 至少覆盖 1.25 与最新稳定版。
  已验证可用的 SQLite 绑定是 `modernc.org/sqlite v1.34.5`（更高版本会抬高 `go` 指令，升级前先确认）。
- 本机 Go 位于 `D:\go`（当前 1.27.3）；若 `go` 报 `cannot find GOROOT directory: C:\Users\lenovo\.g\go`，先修环境：

  ```bash
  export GOROOT=/d/go
  export PATH="$GOROOT/bin:$PATH"
  go version
  ```

- **不引入 cgo**：默认 `CGO_ENABLED=0`，SQLite 绑定固定为 `modernc.org/sqlite`。
  因此**不能加载 `powersync` C 扩展**（`libpowersync`），本地 core 逻辑必须用 Go + SQL 自行实现（见 §5）。
- 要求可在 Windows / Linux / macOS 上交叉编译通过（`GOOS=linux,darwin,windows go build ./...`）。

## 3. 常用命令

```bash
gofmt -l .                      # 必须无输出
go vet ./...
go build ./...
go test ./...                   # 含 sync 逻辑的包必须有测试
go test -race ./...             # 改动并发/同步循环后必须跑
CGO_ENABLED=0 GOOS=linux go build ./...  # 纯 Go 校验

# 差分测试（需要可达的自建服务 + token）
node scripts/js-reference/dump-schema.mjs   # 从 e:/cutter_client 的 AppSchema.ts 导出 out/schema.json
go run ./cmd/psdiff -endpoint $PS_URL -rpc $RPC_URL -box D1-2 -org ORG001 \
    -schema ./out/schema.json -rest $REST_URL -out ./out   # 同步 + 落真实表 + 与 REST 逐行比对
node scripts/probe-remove.mjs           # 验证 PUT/REMOVE 增量（会插入并删除一行测试数据）
PS_URL=... RPC_URL=... BOX_CODE=... ORG_CODE=... ./scripts/probe-bson.sh  # NDJSON vs BSON 落库一致性
go run ./cmd/psdemo -endpoint $PS_URL -rpc $RPC_URL -box D1-2 -org ORG001 \
    -schema ./out/schema.json -db ./out/demo.db   # 公开 API 冒烟（只读）
go run ./cmd/psupload -endpoint $PS_URL -rest $REST_URL -rpc $RPC_URL -box D1-2 -org ORG001 \
    -schema ./out/schema.json -db ./out/upload.db # 上传回显闭环（写入并删除一行 sys_error_report）

# 双实现差分（离线假服务，无需真实后端；Linux/macOS/WSL/CI）
./scripts/run-js-diff.sh                 # 假服务 + JS SDK 落库 + Go 落库 + pscompare
```

提交前至少：`gofmt -l .`（空）、`go vet ./...`、`go test ./...`。

## 4. 目录结构

库代码即公开 API，放在模块根包；其余全部 `internal/`。

```
<module>/                      # 【已实现】package powersync —— 公开 API
  db.go                        # Open/Connect/Disconnect/Close/CurrentStatus/WaitForFirstSync
  schema.go                    # Schema/Table/Column/Index + LoadSchemaJSON（官方 Schema.toJSON）
  connector.go(→errors.go)     # Connector/Credentials/CredentialInvalidator
  query.go                     # Execute/GetAll/GetOptional/WriteTransaction/Watch
  crud.go                      # CrudEntry/CrudBatch/CrudTransaction + Complete
  powersync_test.go            # 用 httptest 回放 golden 的公开 API 测试
  internal/
    protocol/      # 【已存在】线协议 DTO + NDJSON 解码 + 请求体，含 golden 测试
    psclient/      # 工具共用的小 HTTP 客户端（RPC 取 token + PostgREST CRUD）
    netmon/        # 网络可达性探测（TCP 拨号 + 变化推送）
    core/          # 【已存在】schema → 真实表/索引/ps_crud 触发器；ps_oplog + checksum
                   #   校验；checkpoint 物化；上传队列与 bucket 游标
    db/            # 【待建】连接池封装、watch 变更通知、事务 API
    crud/          # 【待建】CrudBatch/CrudTransaction 语义层（core 已有底层读写）
    sync/          # 【已实现】同步循环：下载（/sync/stream）+ 上传（ps_crud 轮询）
    bson/          # 【已实现】BSON 流解码（无第三方依赖），解码成 JSON 后复用 protocol
    fixture/       # 【已实现】离线假 PowerSync 服务（回放 testdata NDJSON），供双实现差分
    rowdiff/       # 【已实现】两份 rows.jsonl 的逐行比对（序列化差异归一化）
    stream/        # （已并入 internal/core：ps_stream_subscriptions + 订阅状态）
    auth/          # token 获取、缓存、过期刷新、abort signal
    sqlite/        # modernc.org/sqlite 封装、注册 SQL 函数
    log/           # slog 封装
  examples/todo/   # 【已存在】完整可运行示例（mock 内置假服务 / real 连真服务）
  cmd/psdiff/      # 【已存在】开发用差分测试工具（同步 + 落库 + 与 PostgREST 比对）
  cmd/psdemo/      # 【已存在】公开 API 冒烟：连接真实服务并打印状态/表行数
  cmd/psupload/    # 【已存在】上传回显闭环：本地写 → PostgREST 上传 → 服务端回显 → 校验
  cmd/psfixture-server/ # 【已存在】离线假服务（给官方 JS SDK 用）
  cmd/pscompare/   # 【已存在】比对两份 rows.jsonl（CI 门禁）
  .github/workflows/ci.yml # go test/vet + 双实现差分 job
  README.md   # 面向使用者的快速开始 + 能力/限制
  docs/reference.md # 完整使用手册（对齐官方 Flutter 参考的中文 API 参考）
  STATUS.md   # 给非技术读者的项目状态一页纸
  CHANGELOG.md
  scripts/         # probe-sync.sh / probe-remove.mjs / probe-checksum.sh / probe-bson.sh /
                   # run-js-diff.sh / js-diff-container.sh / js-reference/（官方 SDK 对照端）
  testdata/sync/   # 真实抓包脱敏后的 golden NDJSON
```

规则：

- 只有确实需要对外暴露的类型/方法放在根包；内部实现一律 `internal/`。
- 禁止循环依赖方向：`sync` → `core`/`crud`/`protocol`/`db`；反向不可。
- `protocol` 包只放 DTO 与编解码，**不含业务逻辑**，字段名与上游 JSON 完全一致。

## 5. 关键架构决策

### 5.1 纯 Go = 自研 core（最大风险点）

官方 Dart/.NET SDK 把同步核心放在 `powersync` SQLite C 扩展里（`powersync_control()` / `powersync_replace_schema()`）。
我们不用 cgo，因此必须：

1. 用 Go 实现同步状态机（下载、checkpoint、oplog 应用）；
2. 用 Go 创建并维护与 core **等价**的内部表、触发器、视图与 SQL 函数（可用 `modernc.org/sqlite` 的 `RegisterScalarFunction` 提供 `powersync_client_id()` 之类，或干脆在 Go 层生成值）。

实现顺序建议：先打通「schema → 本地写 → crud 队列 → 上传 → 下载 → oplog 应用 → checkpoint」最小闭环，再加 sync streams / 优先级 / checksum / BSON。

**已完成（`internal/core`，单元测试覆盖）**：

- schema JSON（官方 `Schema.toJSON()` 形状）→ **与 core 相同的存储布局**：
  - JSON 表（默认，`direct:false`）：`ps_data__<name>(id TEXT PRIMARY KEY NOT NULL, data TEXT)`
    + 同名**视图**（列 = `CAST(json_extract(data,'$.col') as TYPE)`，带 `-- powersync-auto-generated` 标记）
    + `INSTEAD OF INSERT/UPDATE/DELETE` 触发器（先写 `ps_data__*`，再写 `ps_crud`）；
    `local_only` 表用 `ps_data_local__<name>` 且不入上传队列；
  - `direct:true`：真实列式表（`STRICT`，带 `/* ps-managed */` 标记）+ `AFTER` 触发器；
  - 索引：JSON 表建在 `ps_data__*` 上（`CAST(json_extract(data,'$.col') as TYPE)`），direct 表建在列上。
- 本地写 → `ps_crud`（`PUT` 只带非空列、`PATCH` 只带变化列、`DELETE` 无 data），带 `tx_id`；
- 下行数据两阶段（对齐 core）：
  1. `StageOps`（data 行）：只写 `ps_oplog` + `ps_updated_rows`，累计 `add_checksum`/`op_checksum`，**不动用户数据**；
  2. `ApplyCheckpoint`（`checkpoint_complete`）：先校验 checksum，通过才把 oplog 物化进存储（JSON 表写 `ps_data__*`，
     direct 表写列）并推进 `last_applied_op`；不一致则 `ResetBucket`（清 oplog/游标）让该 bucket 从 `after=0` 重下。
- **与 core 同名的 SQL 函数**（包初始化时全局注册，见 `internal/core/functions.go`）：
  `powersync_client_id()`、`powersync_diff(old,new)`（语义与 core 的 `diff.rs` 完全一致：先去 null、
  被清空的键补 null、只保留变化项）、`powersync_json_merge(...)`、`powersync_version()`/`powersync_rs_version()`；
  触发器已改用 core 的表达式写法：落盘 `json_object(<全部列>)`、上传 data 用 `json(powersync_diff(...))`，
  因此 **PATCH 会把被清空的列写成 `{"col":null}`**（之前漏了这种情况）；
- **schema 迁移**：`ReplaceSchema` / 公开的 `DB.UpdateSchema`；重新 `Open` 时若 `ps_kv.schema_json`
  与传入 schema 不同会自动迁移（经 `ps_untyped` 暂存 → 重建对象 → 回填，JSON↔direct、加列、删表都覆盖）；
- 应用期用普通表 `ps_internal_apply` 做守卫，使 direct 表的触发器静默；JSON 表的触发器在视图上，天然不会被同步写入触发；
- `CrudBatch`/`CompleteCrud`（write checkpoint 写 `ps_kv.target_checkpoint_request_id`）、
  `Buckets`/`SetBucketCursor`/`BucketChecksum`/`ValidateCheckpoint`/`ResetBucket`。

- **`include_metadata`**：视图多出 `_metadata`/`_deleted` 两列（与 core 一样取 NULL），
  写操作的 `NEW._metadata` 会进 `ps_crud` 的 `metadata` 字段；`UPDATE t SET _deleted = TRUE`
  会触发**带 metadata 的 DELETE**（`ps_crud_delete2_<table>` 触发器），普通 UPDATE 触发器会跳过这种写。

- **sync streams**：`ps_stream_subscriptions`（对齐 core migration 11 的列）+ 请求体 `streams`
  （`include_defaults` + `subscriptions`）、`partial_checkpoint_complete` 按优先级分批物化、
  checkpoint 的 `streams` 信息回填订阅状态、过期订阅清理；公开 API 有
  `SubscribeStream(..., WithTTL, WithPriority)` / `StreamSubscription.Unsubscribe/State` /
  `StreamSubscriptions()` / `Options.DisableDefaultStreams`。

- **`powersync_crud` 兼容层**：官方 core 里它是 crud 虚拟表；modernc 没有 vtab API，
  这里用「真实暂存表 + AFTER INSERT 桥接触发器」实现等价语义 —— 我们的触发器现在都往
  `powersync_crud(op,id,type,data,metadata)` 写，桥接器在同一事务里搬进 `ps_crud`（带 `tx_id`）
  并删除暂存行；应用代码也可以直接 `INSERT INTO powersync_crud ...`。
  （`options` 列不解释位标志，等价于官方默认 `ignore_empty_update=false`。）
- **多进程互斥**：`<db>.lock` 上的排它锁（Unix flock / Windows LockFileEx），同一 DB 被第二个实例打开
  返回 `ErrDatabaseLocked`（`Options.NoFileLock` 可关；`:memory:` 自动跳过）。
- **网络状态监控**：`internal/netmon` 每 5s 对服务端做一次 TCP 拨号（超时 2s），
  同步连接成功/失败时立即重测；`DB.CurrentNetworkStatus()` / `DB.WatchNetworkStatus(ctx)`；
  401/400 不算离线（只判断链路）。可用 `Options.NetworkTarget`/`NetworkCheckInterval`/
  `NetworkCheckTimeout`/`DisableNetworkMonitor` 调整。
- **Change 通知（Watch 真推送）**：用户表触发器与同步物化都会把 `ps_internal_version` +1；
  `DB.Watch` 等 `VersionSignal()` 被关闭就重查，兜底轮询（`WatchInterval`）默认 2s 只用于漏通知。
  本地写/同步应用 → 推送延迟实测 ~毫秒级（测试里把兜底设为 10s 仍能立即收到）。
- `Open` **强制要求 `Schema`**（与官方一致，`ErrSchemaRequired`）；schema 未声明的表只跳过物化
  （`SkippedUnknownOps()` / `SkippedTables()` 可诊断），不会自动回填；
不再强制要求 `Connector`（纯本地库/自管上传可用；`Connect()` 时才返回 `ErrNotConnected`）。

**尚未实现**（不要以为已有）：

- `powersync_control()` / `powersync_replace_schema()` / `powersync_create_raw_table_crud_trigger()` /
  `powersync_offline_sync_status()` 这些"控制面"函数（同步由 Go 层驱动，不经 SQL）；
- 附件（attachments）。

落地 schema 时已踩到的坑（已修复，勿回归）：

- SQLite 里**索引名与表名共用一个命名空间**：`AppSchema` 存在“depart 表 + depart 索引”，
  直接用会 `there is already a table named depart` → 需要在 `ParseSchema` 里自动改名；
- 触发器只能写 `NEW.<裸标识符>`，`NEW."col"` 是语法错误 → 列名必须是简单标识符（已校验）；
- 触发器不能引用 `temp` 库 → 守卫表必须建在主库，且连接池必须 `MaxOpenConns(1)`；
- `json_patch` 只接受两个参数 → 多列需要嵌套拼接；
- 表结构必须包含 `id` 列（否则自动补 `id TEXT PRIMARY KEY`）；
- **JSON 表写操作走视图**：`INSERT INTO <table>` 命中的是视图的 `INSTEAD OF` 触发器，
  SQLite 报告 **0 行受影响**（`RowsAffected`/`changes()` 都是 0）——公开 API 已在文档里注明；
- JSON 表本地写的 `data` **不含 id**（id 存在单独列），与 core 的触发器一致；同步下来的行则含 id
  （服务端就这样下发），视图两种都能读；
- **`last_applied_op` 只能在物化成功后推进**：`StageOps` 只写 `last_op`，否则物化时
  `op_id > last_applied_op` 一条都匹配不到（踩过，数据全空）；
- **checksum 校验必须放在 `checkpoint_complete`**，不能放在 `checkpoint` 行：
  checkpoint 行先于本批 data 到达，此时本地还没应用这批数据，必然误判（踩过）；
- `ps_buckets.target_op` 在 core v14 已删除 → 改用 `ps_kv.target_checkpoint_request_id`。

### 5.2 本地库格式兼容性

- **内部表名、列名、迁移版本号必须与 `powersync-sqlite-core/crates/core/src/migrations.rs` 对齐**，以便同一数据库文件可被官方 SDK 打开（迁移编号 `ps_migration.id`）。
- 只允许「读取时兼容」，不要擅自增删列；需要额外状态时新建自有表（前缀 `go_ps_`）并说明理由。例外：`ps_internal_apply`（同步期触发器守卫）与 `ps_internal_version`（变更通知）是本项目自有表，官方 core 不认它，但不影响读它的库。
- 用户表的存储布局已对齐 core（见 §7 与 §5.1）：JSON 表 = `ps_data__*` + 同名视图 + INSTEAD OF 触发器；
  视图 SQL 保留 `-- powersync-auto-generated` 标记，索引建在 `ps_data__*` 上。
- 与官方 SDK 的**已知差异**（共享同一个 DB 文件时需评估）：
  - `powersync_crud` 在 core 里是虚拟表，这里用真实表 + 桥接触发器实现（写入语义一致）；
  - SQL 函数只实现了读/计算类子集（`powersync_diff` 等）；`powersync_control` 等控制面函数缺失，
    用它们驱动的 SDK 不能复用本库的同步引擎（但可以读同一个库文件）；
  - 这些函数是**进程内注册**的（Go 包 init），外部 `sqlite3` CLI 里看不到。

### 5.3 CRUD 上传由使用方实现

SDK 只提供队列读取与确认，不做 HTTP 上传（与官方 SDK 一致）：

```go
batch, err := db.GetCrudBatch(ctx, 100)     // 无数据时返回 nil
if batch != nil {
    // 使用方上传 batch.Entries()（每条带 ClientID/TxID/Op/Table/RowID/Data）
    err = batch.Complete(ctx, optionalWriteCheckpoint)
}
```

同理 `GetNextCrudTransaction` 用于需要按事务原子上传的场景。

## 6. 线协议（下载侧）

所有请求相对 `credentials.Endpoint`（去掉尾部 `/`）拼接。

> 本节内容已在真实自建服务上实测（2026-11，`powersync-service`），脱敏样本见 `testdata/sync/`，
> 复测用 `scripts/probe-sync.sh`。与旧文档/其他语言 SDK 的类型定义冲突时，**以实测为准**（当前服务用的是
> sync-streams 协议：`object_type`/`object_id`/`subkey`、`has_more` 分页）。

### 6.1 请求头

```
Authorization: Token <jwt>
Content-Type: application/json
Accept: application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8   # 默认
# 只用 NDJSON（Options.DisableBSON=true / psdiff -bson=false）：
# Accept: application/x-ndjson
X-User-Agent: powersync-go/<version>
```

两种格式都已实现（`internal/bson` 手写解码器，无第三方依赖）：

- 按**响应 `Content-Type`** 选择解码器（`bson` → BSON，否则 NDJSON），服务端不支持 BSON 时自动回落；
- BSON 是长度前缀的连续文档，解码成 JSON 后复用 `protocol.ParseLine`，两条路径同一套行处理；
- 实测同一份数据：NDJSON 1.77 MB vs BSON 1.62 MB（约 -8%），落库结果逐行完全一致
  （`scripts/probe-bson.sh`）。

401 时刷新 token 并重试；不要在不刷新 token 的情况下无限重试。

### 6.2 `POST /sync/stream`

```json
{
  "buckets": [{"name": "50#org_data[\"ORG001\"]", "after": "1234"}],
  "include_checksum": true,
  "raw_data": true,
  "app_metadata": {"k": "v"},
  "parameters": {"user_id": "..."},
  "client_id": "<uuid from ps_kv>",
  "streams": {
    "include_defaults": true,
    "subscriptions": [
      {"stream": "notes", "parameters": {"org": "x"}, "override_priority": 0}
    ]
  }
}
```

- 带 `streams` 时，**`subscriptions` 属性必须存在**（空数组也要有）：
  缺了会 400 `/streams must have required property 'subscriptions'`；序列化成 `null` 也不行。
- `streams.subscriptions[]` 的 **`override_priority` 是必填属性**：缺失会被服务端 400
  （`/streams/subscriptions/0 must have required property 'override_priority'`）。未指定优先级时
  序列化成 `null`。`parameters` 也要给对象（`{}`），不要给 `null`。
- `buckets[].after` = 本地该 bucket 已应用到的**最后一条 op 的 `op_id`**（首次为 `"0"`），服务端按**左开区间**取（`after=22775` → 从 `22776` 开始）。
- ❗**请求里必须带上 checkpoint 里的全部 bucket**。实测：只列一个 bucket 时，其余 bucket 会从 `after=0` **全量重发**（单 bucket 就重发了 912 条 op），不是“不多发”而是“发更多”。
- 未识别的 bucket 名**不会报错**，直接被忽略（实测 `"no_such_bucket"` → 200 + 默认 streams）。
- 服务端返回 `200` + 流，**逐行**解析（实测单行最大 1.2 MB，需 `bufio.Reader.ReadBytes('\n')` 或 `json.Decoder` 流式读取，不要用默认 64 KB 的 `Scanner`；已封装在 `internal/protocol.Decoder`）。

### 6.3 流内行类型（**已对真实服务实测**，2026-11）

```jsonc
// 1) 每个 checkpoint 的首行：bucket 清单 + 优先级 + 流状态
{"checkpoint": {
  "last_op_id": "24387",
  "buckets": [{"bucket": "50#org_data[\"ORG001\"]", "checksum": 1070820748, "count": 734,
               "priority": 3, "subscriptions": [{"default": 0}]}],
  "streams": [{"name": "org_data", "is_default": true, "errors": []}]
}}

// 2) 数据行：**每个 bucket 可以出现多行**，靠 after + has_more 分页
{"data": {"bucket": "50#org_data[\"ORG001\"]", "after": "22775", "has_more": true,
          "data": [{"op_id": "22776", "op": "PUT",
                    "object_type": "org", "object_id": "<uuid>",
                    "checksum": 2931144126,
                    "subkey": "<uuid>/<uuid>",
                    "data": "{\"id\":\"...\"}"}]}}

// 2b) 删除：op=REMOVE，data 为 null，其余字段不变（subkey 与之前的 PUT 相同）
{"data": {"bucket": "50#box_data[\"D1-2\",\"ORG001\"]", "after": "24397", "has_more": false,
          "data": [{"op_id": "24397", "op": "REMOVE",
                    "object_type": "sys_error_report", "object_id": "<uuid>",
                    "checksum": 3029235767, "subkey": "<uuid>/<uuid>", "data": null}]}}

// 3) 结束与 keepalive
{"checkpoint_complete": {"last_op_id": "24387"}}
{"token_expires_in": 7062}
```

要点（全部来自实测，不要按 .NET/旧文档改写）：

- op 字段是 **`object_type` / `object_id` / `subkey`**（sync-streams 协议），**不是** `row_type` / `row_id` / `key`。
- `raw_data: true` → `data` 是**字符串**；`raw_data: false` → `data` 是**对象**；`REMOVE` → `data` 为 `null`。三种都要支持，解析层用 `json.RawMessage` + `DecodeData()`。
- `checksum` 是 32 位值，但服务端会按**带符号 i32** 序列化（实测同一连接里出现过 `3272744065` 和 `-96505511`）。Go 侧用 `int64` 承接、用 `Checksum32()` 取无符号视图；**不要**直接用 `uint32` 反序列化（会直接报错）。
- `op_id` / `last_op_id` / `write_checkpoint` / `after` 一律是**十进制字符串**（64 位），Go 侧用 `int64` 或 `string` 保存，不要转 float。
- ❗**data 行的 `after` 不是“已应用到的最新 op_id”**，而是本批的左开下界（= 本批请求的游标）。进度必须取 `data.data[last].op_id`，否则会永远重放同一批（`cmd/psdiff` 踩过这个坑）。
- 同一个 checkpoint 内可能有 1..N 条 `data` 行（实测同一 bucket 先 90 条 `has_more=true`，再 554 条 `has_more=false`），**客户端必须把它们累加到同一 bucket 再判定完成**。
- `token_expires_in` 是 keepalive，实测约 **20 s** 一次（7062 → 7044 → 7023），同时用于提前刷新 token。
- `checkpoint_complete` 里**没有** `checkpoint_request_id`（新版把 checkpoint request 拆到独立端点，见 6.4）。
- 未知行类型：记录日志后忽略，不要中断流（服务端会演进）。

### 6.4 其他端点

| 端点 | 用途 |
| --- | --- |
| `POST /sync/checkpoint-request`，body `{"client_id","checkpoint_request_id"}` → 实测 `{"data":{"checkpoint_request_id":"1"}}` | 新式 checkpoint 请求（`checkpoint_mode: requests`）|
| `GET /write-checkpoint2.json?client_id=<id>` → 实测 `{"data":{"write_checkpoint":"2"}}` | 旧式（legacy）checkpoint；返回值是会自增的字符串计数 |

- `checkpoint-request` 返回 `404` 表示服务端不支持新式模式 → 回退 legacy，而不是报错终止。
- 实测该服务 **没有** `/sync/rules`（404），不要依赖它探测配置。
- 错误信封统一为 `{"error":{"code":"PSYNC_S2106","status":401,"description":"...","name":"AuthorizationError"}}`；`code` 前缀 `PSYNC_` 用于分类，`status` 可映射到 Go 错误类型。见 `testdata/sync/error-401.json`。
- 服务端同时支持 BSON（`Accept: application/vnd.powersync.bson-stream`）——已实现，见 6.1。

### 6.5 已实测 vs 未实测

已实测（可直接依赖）：

- 全量快照、`buckets[].after` 左开语义、**未列出的 bucket 会全量重发**；
- 同 bucket 多 `data` 行 + `has_more`；`raw_data` true/false；`op=REMOVE`（`data:null`）；
- 32 位 checksum 的**带符号序列化**；keepalive ~20 s；401 错误信封；
- `POST /sync/checkpoint-request`、legacy `write-checkpoint2`、BSON 流（解码后与 NDJSON 逐行等价）；
- 复制延迟：测试盒上从 PostgREST/PG 写入到出现在流里约 **45–90 s**，探测脚本必须轮询（`scripts/probe-remove.mjs` 默认 240 s）。

**未实测**（实现前必须再抓包，不要凭文档假定字段）：

- ~~`checkpoint_diff`~~：**已实测**（2026-11，真实服务在上传后推送增量时下发）。形状：
  `{"checkpoint_diff":{"last_op_id":"24457","removed_buckets":[],"updated_buckets":[{"bucket":"50#box_data[\"D1-2\",\"ORG001\"]","checksum":2134983222,"count":980,"priority":3,"subscriptions":[{"default":1}]}]}}`。
  它**代替 checkpoint 行**出现，只给变化 bucket 的新 checksum/count。客户端处理：用 `updated_buckets` 作为
  应用目标（并据此校验 checksum），`removed_buckets` → `ResetBucket`（**已落库的行不会自动删除**，
  需要后续按 bucket 归属清理）。样本见 `testdata/sync/checkpoint-diff.ndjson`。
- `partial_checkpoint_complete`（按 priority 部分完成）。
- `checkpoint.write_checkpoint`（新式模式下缺省，疑似只在 legacy 模式出现）。
- `op= "MOVE"`（subkey 变更）。
- `checkpoint.streams[].errors` 非空时的字段形状（stream 配置错误）。
- 上传接口：本项目的上传由使用方通过自有后端完成，未测。

## 7. 本地 SQLite 内部结构

与 core 对齐的内部表（列定义见 `migrations.rs`，不要手写）：

| 表 | 作用 |
| --- | --- |
| `ps_migration` | 迁移版本记录（`id` = 上游迁移号）|
| `ps_kv` | 键值：`client_id`、`last_synced_at` 等 |
| `ps_buckets` | 每个 bucket 的 `last_applied_op` / `last_op` / `target_op` / checksum / `pending_delete` / 下载进度 |
| `ps_oplog` | bucket 内 op 日志（`bucket`=ps_buckets.id、`op_id`、`op`、`row_type`、`row_id`、`key`、`data`、`hash`、`superseded`）；同 key 新 op 会**删除**旧行 |
| `ps_crud` | 待上传变更队列（`id` 自增、`data` JSON、`tx_id`）|
| `powersync_crud` | 官方 core 的 crud 虚拟表；本实现为真实暂存表 + 桥接触发器，写入后立即搬进 `ps_crud` |
| `ps_tx` | 事务 id 分配 |
| `ps_untyped` | core 在 schema 变更时搬数据的暂存表；本实现只建表保持结构兼容 |
| `ps_updated_rows` | 需要刷新视图/通知的行 |
| `ps_sync_state` | 按 priority 的 `last_synced_at` |
| `ps_internal_apply` | 本项目自有：同步应用期的触发器守卫 |
| `ps_internal_version` | 本项目自有：数据版本号，触发器/同步物化写入，驱动 `Watch` 推送 |
| `ps_stream_subscriptions` | 流订阅：`stream_name`/`local_params`/`active`/`is_default`/`local_priority`/`ttl`/`expires_at`/`last_synced_at`（STRICT，对齐 core migration 11）|
| `ps_data__<table>` | JSON 表的真实落盘：`(id TEXT PRIMARY KEY NOT NULL, data TEXT)` |
| `ps_data_local__<table>` | `local_only` 表同上（不入上传队列） |
| `<table>`（视图） | 对外查询入口：`id` + `CAST(json_extract(data,'$.col') as TYPE)`；`include_metadata` 时再追加 `_metadata`/`_deleted`（NULL）|

用户表由 schema JSON 创建；JSON 表上的本地写走 **INSTEAD OF 触发器**（写 `ps_data__*` + `ps_crud`），
direct 表走 `AFTER` 触发器（写真实列 + `ps_crud`）。schema JSON 形状（与 Dart `Schema.toJson` 一致）：

```json
{"tables": [{
  "name": "todos",
  "view_name": null,
  "direct": true,
  "local_only": false,
  "insert_only": false,
  "ignore_empty_update": false,
  "include_metadata": false,
  "include_old": false,
  "columns": [{"name": "id", "type": "text"}],
  "indexes": [{"name": "todos_owner", "columns": [{"name": "owner_id", "ascending": true, "type": "text"}]}]
}]}
```

## 8. 同步语义（必须遵守）

- 本地 SQLite 是唯一事实来源；远端数据只能通过同步通道按 bucket 应用，不允许绕过。
- 一个 checkpoint 的完成判定 = 收到 `checkpoint_complete` **且** 本地已应用该 checkpoint 内所有 `data` 行（同 bucket 可能分多行、`has_more=true` 需继续请求）。
- 写事务与 `ps_crud` 入队必须在**同一事务**内完成（由触发器保证），避免丢更新。
- 上传按 `ps_crud.id` 升序、幂等重试；失败用指数退避 + 抖动，且可被 context 取消。
- 收到 `checkpoint` 行后：先把该 checkpoint 涉及的所有 `data` 行写入并推进 `last_applied_op`，再写 `last_op`/`target_op`，再更新 `ps_sync_state`。**顺序不可颠倒**。
- `checkpoint_complete` 才表示「可对使用方可见地完成一次同步」；此时才允许清理已被 superseded 的 oplog。
- 断线重连：用本地 `last_applied_op` 作为 `after` 续传，不得从头重放；**每次请求都带上本地已知的全部 bucket**（漏一个就会让它全量重发）。
- 游标（after）只能由「本批最后一条 op 的 `op_id`」推进（PUT/REMOVE 都带 op_id），**不能用 data 行的 `after`**——那是本批的左开下界，用它会让同一批被无限重放。
- 请求里的 `after` 必须取 **`ps_buckets.last_op`（已 staged）**，不是 `last_applied_op`（已物化）：
  core 的 `collect_bucket_requests` 也用 `last_op`。用 `last_applied_op` 会在中途断流时重发同一批 data，
  supersede 会重复累计 `add_checksum` → 下次 checkpoint 必然 checksum mismatch（踩过）。
- 物化失败（如 schema 对不上）要**重置该批 bucket**：否则游标已推进、数据没落地，checksum 会漂移。
- 上传完成后若拿到 `write_checkpoint`，用它设置 `target_op`，让服务端把本地写入回显回来（否则本地视图会「闪烁」）。
- checksum 不匹配（服务端 `checkpoint_diff` 中的 checksum 与本地累计值不符）→ 清空该 bucket 重新下载（`buckets[].after = "0"`）。
- 一个 bucket 若被服务端移除，标记 `pending_delete` 并在下次迭代中删除，不要立即 DROP 用户表。
- 所有状态变更都要对外发出状态事件（`connected`/`connecting`/`downloading`/`uploading`/`error`），UI 依赖它。

## 9. 公共 API（已实现，签名见根包）

行为约定（实现已按此处理）：

- `Connect(ctx)` 内部用 `context.WithoutCancel(ctx)` 起后台循环：传一次性的请求 ctx 不会立刻断开，
  停止要用 `Disconnect()`/`Close()`。
- `WriteTransaction` 会给事务内的 `ps_crud` 行打同一个 `tx_id`（`ps_tx`），
  `GetNextCrudTransaction` 因此能按事务原子上传。
- sync streams：`SubscribeStream(ctx, name, params, WithTTL/WithPriority...)` 写`ps_stream_subscriptions`，
  下一次建流时随请求发出；`partial_checkpoint_complete` 只物化该优先级及以上的 bucket，其余保持 staged。
- `Watch` 是**推送实现**：触发器/同步物化 bump `ps_internal_version` → 关闭版本 channel → Watch 重查并推送；
  `Options.WatchInterval`（默认 2s）只是兜底轮询，正常情况毫秒级。ctx 结束即关闭。
  结果用 **64 位 FNV 哈希**（键排序、复用 buffer）去重：1000 行结果集实测 **0 allocs、~119µs**
  （对比 `fmt.Sprint` 的 7002 allocs / ~571µs），无关写入不会产生推送。
- **版本号按表维护**（`ps_internal_version(tbl, v)`）：触发器只 bump 自己那张表，同步物化按实际
  触及的表 bump。`WatchTables(ctx, []string{"todos"}, ...)` 只有这些表的版本变了才**重新执行查询**
  （无关写入连查询都不跑）；`Watch` 保持"任何写入都重查 + 哈希去重"。
  `ps_internal_version` 旧结构（单列 `v`）会在 `Open` 时自动重建成按表结构（版本计数重置无害）。
- 上传循环按 `Options.UploadInterval`（默认 1s）轮询 `ps_crud`，本地写会自动触发一次；
  上传失败指数退避（1s→30s），401 会让 connector 的 `InvalidateCredentials` 被调用后重取凭据。
- schema 未声明的表（服务端 sync rules 更宽时）**跳过物化**并计数，不影响 checksum 与 oplog。

签名草案（实际以此为准）：

```go
db, err := powersync.Open(ctx, powersync.Options{
    Filename:  "app.db",
    Schema:    schema,          // 非 nil 时用于创建/迁移
    Connector: connector,       // FetchCredentials + UploadData
    Logger:    slog.Default(),
})
defer db.Close(ctx)

db.Connect(ctx)                 // 启动下载 + 上传循环（不阻塞）
db.Disconnect(ctx)
db.Execute(ctx, "INSERT ...", args...) / GetAll / GetOptional
db.WriteTransaction(ctx, func(tx *Tx) error { ... })
db.Watch(sql, params)           // 返回可关闭的变更流
db.CurrentStatus()              // 同步状态快照 + 订阅取消
db.WaitForFirstSync(ctx)        // 首次同步完成
```

- 所有阻塞方法接收 `context.Context` 作为第一个参数。
- `Options` 用结构体 + 可选字段，避免位置参数；以后加字段不破坏签名。
- 状态变更通过 `Status()` + `WatchStatus()`（channel 或 callback 二选一，固定一种）。
- 公开类型不暴露 SQLite 驱动类型（例如不要返回 `modernc.org/sqlite` 的 `*sql.Row`）。

## 10. 编码规范

- 遵循 Effective Go 与 [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)。
- 错误：`fmt.Errorf("...: %w", err)` 保留上下文；用 `errors.Is/As` 判断；库代码不 `panic`，不 `log.Fatal`。
- 可重试与不可重试错误分离（如 `ErrRetryable`、`ErrUnauthorized`、`ErrProtocol`）。
- `context.Context` 贯穿网络/DB/等待；不存进结构体；goroutine 必须有明确退出路径（`errgroup` / `context`）。
- 时间统一 `time.Time`（UTC）；DB 内以整数表示，选定单位后不得混用。
- 日志用 `log/slog`；**禁止**输出 token、密码、完整用户数据（`data` 字段需截断）。
- 导出标识符必须有 doc comment，以标识符名开头；包级注释说明职责。
- 接口在使用方定义，保持最小；不为「将来可能」加抽象。
- 性能敏感路径（行解析、oplog 应用）避免不必要的 `map[string]any` 拷贝；能复用 buffer 就复用。

## 11. 测试

现状：
- `internal/protocol`：golden 单测（解析/请求体/错误信封）；
- `internal/core`：schema / 视图 / 触发器 / oplog / checksum / 物化 / 上传队列 / `powersync_*` 函数 /
  schema 迁移（JSON↔direct、加列、删表、重新 Open 自动迁移）、`include_metadata`、
  sync streams（订阅/取消/过期清理/状态回填/partial checkpoint）、文件锁、`powersync_crud`
  兼容层（含 `options` bit0 = ignore_empty_update）、`WriteTransaction` panic 后 tx_id 清理、
  schema 并发替换、按表版本号单测，含真实 golden 回放（38 个用例）；
- `internal/netmon` 4 个用例（在线/离线切换、变化广播、目标未知、地址归一化）；
- `examples/todo`：可直接 `go run` 的完整示例（默认离线 mock；不参与单测，`go build ./...` 兜底）；
- `internal/sync` 直接单测（74% 覆盖）：流格式选择（NDJSON/BSON）、write-checkpoint 门控、
  500 重试后成功、checksum 不一致重连、上传失败重试恢复、`Stop()` 不阻塞；
- `internal/psclient` 单测（83% 覆盖）：token RPC 四条失败路径、REST Do/GetJSON/CountByID；
- 根包另有 `BenchmarkWatchChangeDetection`（Watch 变更检测开销）。
- 根包 `powersync`：用 `httptest` 回放 golden 的公开 API 测试（Connect、上传、CRUD、Watch 推送
  （本地写/事务/同步应用三种来源）、401 重取凭据、schema 往返、无 Connector 的本地库、文件锁、
  sync streams 请求体/partial checkpoint）共 14 个；
- `cmd/psdiff` / `cmd/psdemo`：可重复的端到端工具（前者差分比对，后者公开 API 冒烟）。

- 表驱动测试优先；测试文件与被测包同目录（`_test.go`）。
- SQLite 用临时文件或 `:memory:`（注意 `modernc` 下 `:memory:` 与连接池的坑，实测必须 `SetMaxOpenConns(1)`）。
- 服务端用 `httptest.Server` 回放 `testdata/sync/*.ndjson`，覆盖：冷启动全量、同 bucket 多 `data` 行 + `has_more` 续传、已是最新（只有 checkpoint）、`raw_data` true/false、PUT→REMOVE、负 checksum、401 刷新 token、`token_expires_in`、未知行忽略、断线重连。
- core 的 checksum 规则是**累计链**而非"当前行哈希之和"（`TestChecksumSupersedeAndRemove` 锁定了具体值），
  改这段逻辑前先看测试注释与 `scripts/probe-checksum.sh`。
- 协议解析用 golden 文件（`testdata/sync/*.ndjson`）。新增/修改 golden 必须来自真实抓包（`scripts/probe-sync.sh` / `probe-remove.mjs`）并**脱敏 + 裁剪**，说明见 `testdata/sync/README.md`。
- 未实测的行类型（`checkpoint_diff`、`MOVE`、`partial_checkpoint_complete`）不要自己编造 golden，先在真实服务上造场景抓包。
- 禁止 `time.Sleep` 编排测试；时钟与网络通过接口注入。
- 并发相关改动必须 `go test -race` 通过。

### 11.1 差分测试基线（2026-11 实测可重复）

`cmd/psdiff` 对自建服务 `D1-2 / ORG001` 的结果（改动解析/落库逻辑后应保持一致）：

```
第 1 次同步：data 行=3 op=1680 checkpoint=24423 bucket=2
第 2 次同步（续传）：data 行=0 op=0
落库：27 张表 / 1391 行（真实表，非 JSON 存储）
跳过（不在 schema 中的表）: 252 个 op（i/t/material_*）
上传队列: 0 条（下行数据未被触发器再次入队）
校验和：两个 bucket 的本地 add+op == 服务端 checkpoint checksum（全部 OK）
与 PostgREST 比对：一致=1372  缺失=0  字段不一致=0  软删除=15  跳过表=1
```

上传回显闭环（`cmd/psupload`，同一服务，表 `sys_error_report`，跑完自动清理）：

```
=== 3. 等待上传 + 服务端回显 ===
回显成功：本地仍有该行 c04ca5b5-…，cursor=24418，下载错误=""
=== 4. 本地删除 → 上传删除 → 等回显 ===
删除闭环成功：本地/后端均无该行，cursor=24419
上传次数: 2（write checkpoint 用的 legacy /write-checkpoint2.json）
✅ 上传回显闭环通过（无 checksum 漂移、无残留错误）
```

这一跑同时验证了：PUT/DELETE 上传 → 服务端回显 → 本地物化 → checksum 不漂移 → 队列清空。

sync streams（`cmd/psdemo -subscribe box_data`，同一服务）：

```
已订阅流: box_data (id=1)
首次同步完成，用时 1.121s
状态: connected=true buckets=2 cursor=24425
落库: 37 张表 / 1391 行

# 加 -no-defaults（include_defaults=false）→ 只同步显式订阅的流
已订阅流: box_data (id=1)
状态: connected=true buckets=1 cursor=24425
落库: 37 张表 / 775 行
```

BSON vs NDJSON（`scripts/probe-bson.sh`，同一服务、同一 schema）：

```
合计: 一致=1391 仅左=0 仅右=0 字段不同=0
✅ Go 与官方 SDK 落库结果一致
```

（两边分别走 `application/x-ndjson` 与 `application/vnd.powersync.bson-stream`，op 数/checksum 完全相同。）

公开 API 冒烟（`cmd/psdemo`，同一服务）：

```
首次同步完成，用时 1.407s
状态: connected=true buckets=2 cursor=24403
落库: 37 张表 / 1391 行
上传队列: 0 条
本地写入队样例: {"ClientID":1,"Op":"PUT","Table":"sys_config","Data":{"cfg_key":"demo_key",...}}
```

增量场景另用 `scripts/probe-checksum.sh` 验证（INSERT → PATCH → DELETE 一轮，实测通过）：

- 3 条 op（PUT 24401、PUT 24402、REMOVE 24403）应用后，下一次 checkpoint 的 checksum 与本
  地累计完全一致 → `supersede` + `REMOVE` 的累计规则正确。
- 人为把 `ps_buckets.add_checksum` 改掉 12345，再跑 `psdiff`：会在 checkpoint 处报
  `校验和不符 … (expected 1070820748, got 1070833093 = …(add) + …(op))`，
  随后 `ResetBucket` → 从 `after=0` 重下 1660 条 op → 再同步时重新变 OK（恢复路径已验证）。

- 15 行“软删除”是 `deleted_at` 非空的 `work_code_bom` 行：bucket 会下发，PostgREST 视图不展示 —— 属于预期差异。
- 跳过表：`role` 在同步规则里但 REST 不暴露（404）；`i`/`t` 是 sync rules 里的垃圾表，schema 没声明 → core 会跳过物化。⚠️ 建议让服务端清理。
- 比对器把“落库为 NULL + REST 无此列”视为一致（`AppSchema` 声明了部分库里已不存在的列）。

### 11.2 双实现差分（Go vs 官方 JS SDK，离线）

`scripts/run-js-diff.sh` 会在**本地启动一个假 PowerSync 服务**（`cmd/psfixture-server` → `internal/fixture`，回放
`testdata/sync/*.ndjson`），然后让两套实现同步同一份数据并逐行比对：

```
假服务 (127.0.0.1:18080)  ──/sync/stream──▶  Go 实现（cmd/psdiff）      → out/jsdiff/go/rows.jsonl
                          └──────────────▶  官方 @powersync/node       → out/jsdiff/js/rows.jsonl
                                                          ↓
                                              cmd/pscompare（internal/rowdiff）
```

- 两侧用**同一份 schema JSON**（`testdata/schema/todos.json`），保证列集一致，差异只可能来自实现。
- `internal/rowdiff` 负责归一化两端已知的序列化差异（时间戳空格/T、`"2.00"` vs `2`、jsonb 文本 vs 对象、
  `"\"\""` vs `""`、NULL vs 缺列），差异会打印到表/行/字段级别并以非 0 退出码失败。
- **已本机跑通**（2026-11，Windows + WSL2/kiri，未用 Docker Desktop）：基线如下
  （`psfixture-server` + Go `psdiff` vs 官方 `@powersync/node` 1.3.1）：

  ```
  Go:  1 张表 / 2 行
  JS:  tables=1 rows=2 (todos: 2), syncMs≈50, connected=true
  比对: todos OK  一致=2 仅左=0 仅右=0 字段不同=0  ✅
  ```

  官方 SDK 建出来的库与我们的**完全同构**：`ps_data__todos` + 视图 `todos`，`ps_oplog` 2 行、
  `ps_buckets.op_checksum=350/last_applied_op=101` 与我们一致；它还往
  `ps_stream_subscriptions` 写了一条 `{stream_name:"test_data",active:1,is_default:1,local_params:"null"}`
  ——证明这张表的结构/语义是兼容的（我们只记录显式订阅，不记录默认流，见 §5.2）。
- **环境要求**：`@powersync/node` 需要 **glibc 的 Node ≥ 22.15**（undici 7 用 zlib zstd）
  和 glibc 版 `libpowersync_*.so`；Alpine(musl) 与 Node 20/21 都会挂在 `undici` 或 `.so dlopen` 上。
- **跑法**：
  - Linux/macOS/CI：`./scripts/run-js-diff.sh`（Node ≥ 22.15）
  - 只有 WSL2/kiri（无 Docker Desktop）：`./scripts/js-diff-container.sh`
    （用已缓存的 glibc 镜像如 `postgres:18` + 官方 Node tarball；`kiri` 的 daemon DNS 可能坏，
    所以不要在脚本里依赖 `kiri pull`）
  - Windows 原生：`better-sqlite3` 缺 ClangCL → 不可行，脚本会提前提示
- CI：`.github/workflows/ci.yml` 的 `js-diff` job（ubuntu + Node 22）执行同一脚本，产物上传 artifact。

## 12. 依赖策略

允许的依赖（新增需说明理由）：

- `modernc.org/sqlite`（唯一 SQLite 实现）
- `golang.org/x/sync`（`errgroup`）
- `github.com/google/uuid`（或自写 UUID，二选一，不要两个都引）
- 标准库优先：`net/http`、`encoding/json`、`log/slog`

禁止：任何需要 cgo 的库、任何重量级 ORM、任何会隐藏 SQL 的查询构建器。

## 13. 提交与协作

- 提交信息遵循 Conventional Commits：`feat(sync): ...`、`fix(oplog): ...`、`test(protocol): ...`、`chore:`。
- 一个提交只做一件事；格式化/重命名单独提交。
- 不提交：`*.db`、`*.db-wal`、`.env`、测试抓包中的真实 token、IDE 私有配置。

## 14. 对代理的要求

- 改协议或表结构前，先查 §0 的上游源码，并在 PR/回复中给出出处（文件 + 版本）。
- 改动前先读相关文件，不要凭猜测改 API 或字段名。
- 修改后自行运行 `gofmt -l .`、`go vet ./...`、`go test ./...`（并发改动加 `-race`）并报告结果。
- 不要擅自升级/降级依赖，不要引入 cgo，不要新增第三方库而不说明理由。
- 不要删除或重写与本任务无关的代码、注释、测试。
- 大范围重构（改公共 API、改表结构、改同步循环）先给计划再动手。

## 15. 未决问题（需产品/负责人确认）

1. 模块路径：已定为 `module msmkSync`（包名仍为 `powersync`）。若今后发布到远端仓库，
   建议改成 `github.com/<org>/msmkSync`（Go 要求模块路径可解析）。
   （公开 API、同步循环、上传回显闭环、BSON、视图层、`include_metadata`、sync streams、
   文件锁、Watch 推送、双实现差分基线**均已完成/实测通过**。）
2. 双实现差分（Go vs 官方 JS SDK）已就绪：`scripts/run-js-diff.sh` + `cmd/pscompare` + CI job。
   **待确认**：CI 首次跑通后的基线数字（表/行数）要回填到 §11.2。
3. BSON 流 —— **已实现**（`Options.PreferBSON`，默认关闭；`scripts/probe-bson.sh` 验证与 NDJSON 落库完全一致）。
4. 是否需要**附件（attachments）**支持，还是由使用方自行实现。
5. 是否需要 sync streams 的 TTL / 优先级 / 动态订阅全量能力，还是先只支持静态 buckets（`include_defaults` + 显式订阅）。
6. 是否要求与官方 SDK **共用同一数据库文件**（决定 §5.2 的严格程度）。
7. 是否需要 legacy `/sync` 增量 API（`checkpoint_token` + `has_more`）以兼容旧版自建服务。
8. 是否提供 `powersync` CLI 或示例程序（当前定位为纯库）。
