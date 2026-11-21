# msmkSync (Go)

用**纯 Go** 实现的 [PowerSync](https://www.powersync.com/) 客户端：本地 SQLite 为权威数据源，
通过 sync rules 生成的 buckets 与自建 PowerSync 服务做双向增量同步。

- 模块：`msmkSync`（包名仍是 `powersync`，所以是 `import powersync "msmkSync"`）
- 形态：**库**，不提供 CLI 可执行文件
- Go：**1.25+**，`CGO_ENABLED=0`（SQLite 绑定用 `modernc.org/sqlite`）
- 目标服务端：自建 `powersync-service`（已对真实实例做端到端验证）

> 这是社区实现，不是官方 SDK。官方没有 Go SDK；本仓库对齐官方协议与本地库格式，
> 详见 [`AGENTS.md`](./AGENTS.md)（给 AI/新人看的硬性约定与实测记录）。

## 能力

| 分类 | 已实现 |
| --- | --- |
| 协议 | `POST /sync/stream`（**NDJSON 与 BSON**，默认 BSON）、`checkpoint`/`data`/`checkpoint_complete`/`partial_checkpoint_complete`/`token_expires_in`、`/sync/checkpoint-request`、legacy `write-checkpoint2.json` |
| 本地库 | 与 `powersync-sqlite-core` 同构：`ps_data__*` + 同名视图 + INSTEAD OF 触发器、`ps_oplog`、`ps_buckets` checksum、`ps_crud`（`powersync_crud` 兼容层）、`ps_kv`、`ps_tx`、`ps_untyped`（schema 迁移）、`ps_stream_subscriptions`、`include_metadata` |
| 同步 | 下载 + 上传双循环、断点续传（`last_op`）、checksum 校验与漂移恢复、上传回显门控、指数退避、token 预取、sync streams（订阅/TTL/优先级）、多进程文件锁、**网络状态实时监控**（TCP 探测 + 事件推送） |
| 公开 API | `Open`/`Connect`/`Close`、`Execute`/`GetAll`/`GetOptional`/`WriteTransaction`、`Watch` / `WatchTables`（**推送** + 表级版本 + 64 位哈希去重、0 分配）、`GetCrudBatch`/`GetNextCrudTransaction`、`SubscribeStream`、`CurrentStatus`/`WaitForFirstSync` |

## 快速开始

### Windows PowerShell：先修 GOROOT

如果 `go` 报 `cannot find GOROOT directory: C:\Users\<你>\.g\go`，**根因通常是 PowerShell profile**
（`C:\Users\<你>\Documents\WindowsPowerShell\Microsoft.PowerShell_profile.ps1`）里有：

```powershell
$env:GOROOT="$HOME\.g\go"          # g 管理的目录，实际并不存在
$env:Path=-join("$HOME\.g\bin;", "$env:GOROOT\bin;", "$env:Path")
```

一次性修好（自动补回退逻辑，并备份 profile 为 `*.bak-时间戳`）：

```powershell
cd E:\powersync_golang
. .\scripts\go-env.ps1 -FixProfile     # 也可以不加 -FixProfile，只影响当前会话
go version                              # 应输出 D:\go 下的版本
```

或手动改 profile（把上面两行换成）：

```powershell
if (Test-Path "$HOME\.g\go\src\runtime") { $env:GOROOT = "$HOME\.g\go" }
if (-not $env:GOROOT -or -not (Test-Path "$env:GOROOT\src\runtime")) { $env:GOROOT = 'D:\go' }
$env:Path = "$env:GOROOT\bin;$env:Path"
```

> PowerShell **不用** `\` 续行；换行要写在反引号 `` ` `` 后面，或者干脆写成一行。

直接跑（参数已写死在示例里，无需任何 flag）：

```powershell
. .\scripts\go-env.ps1          # 只在 go 报 GOROOT 错时需要
go run ./examples/todo
```

它会：连接真实服务 → 全量同步 65 张表 → 打印每张表行数（和关键表样例）→
演示“断网读本地 + 断网写 + 恢复后自动上传” → 后台校验。

需要换环境时再传 flag（环境变量优先：`PS_URL`/`REST_URL`/`RPC_URL`/`BOX_CODE`/`ORG_CODE`）：

```powershell
go run ./examples/todo -cleanup                      # 演示完删搬针行
go run ./examples/todo -box D1-1 -db .\out\d11.db    # 换柜/换库
go run ./examples/todo -mode mock                     # 完全离线（内置假服务，todos schema）
```

### 在自己的代码里

```go
import powersync "msmkSync"

sch := powersync.NewSchema(
    powersync.Table{
        Name:    "todos",
        Columns: []powersync.Column{{Name: "title", Type: powersync.TextColumn}},
        Indexes: []powersync.Index{{Name: "todos_title", Columns: []string{"title"}}},
    },
)

db, err := powersync.Open(ctx, powersync.Options{
    Filename:  "app.db",
    Schema:    sch,
    Connector: myConnector{}, // 实现 FetchCredentials + UploadData
})
defer db.Close(ctx)

db.Connect(ctx)                       // 后台跑下载+上传
db.WaitForFirstSync(ctx)

db.Execute(ctx, `INSERT INTO todos(id, title) VALUES(?, ?)`, "t1", "写代码")
rows, _ := db.GetAll(ctx, `SELECT * FROM todos`)

ch, _ := db.Watch(ctx, `SELECT count(*) AS n FROM todos`)        // 任何写入→重查+哈希去重
ch2, _ := db.WatchTables(ctx, []string{"todos"}, sql, args...)   // 只有 todos 变才重查（大结果集推荐）
```

### Connector

```go
type Connector interface {
    // 返回 PowerSync 服务地址与短时 JWT。
    FetchCredentials(ctx context.Context) (Credentials, error)
    // 把本地 ps_crud 队列上传到你自己的后端，成功后 batch.Complete(ctx, writeCheckpoint)
    UploadData(ctx context.Context, db *DB) error
}
```

`GetCrudBatch` / `GetNextCrudTransaction` 的条目形如
`{Op: PUT|PATCH|DELETE, Table, ID, Data, Metadata}`；`PATCH` 会包含被清空字段的 `null`。

也可以用 `powersync.LoadSchemaJSON()` 直接读取官方 SDK 的 `Schema.toJSON()`（见
`scripts/js-reference/dump-schema.mjs`）。

## 示例

```bash
go run ./examples/todo            # 离线完整示例（内置假服务，几秒内跑完）
```

详见 [`examples/README.md`](./examples/README.md)。

## 客户端 schema 从哪里来

PowerSync 要求客户端声明 schema（本地有哪些表、每张表哪些列），**只有声明过的表才会同步**。
本仓库的做法：

1. 表清单 → PowerSync 同步规则（`syncRules.yaml`）里的 `FROM "schema"."table"`；
2. 列定义 → PostgREST 的 OpenAPI（`GET <rest>/` 的 `definitions`）；
3. 生成 → `examples/cutterschema/schema.go`（包 `cutterschema`，`Schema()`，一行一张表，可手改）
   + `examples/cutterschema/schema.json`（官方 `Schema.toJSON()` 形状，给 `-schema` 用）。

> SDK 根包保持**项目无关**：只有通用 API，不含任何业务 schema；业务 schema 属于使用方
> （示例里放在 `examples/todo/`）。换项目时用 `cmd/genschema -pkg cutterschema -func Schema -out-go <你的项目>/schema.go`。
>
> 客户端只认**裸表名**（同步流里 `object_type` 就是 `cutter_info`）；PG 的 schema 分组、
> PostgREST 的 `schema:api` 都是服务端/网关的事，客户端 schema 只是“本地有哪些表、哪些列”的声明。
> `syncRules.yaml` 只在服务端，构建期拿来发现表名，**运行时不依赖**。

```bash
# 重新生成（服务端表结构变了就跑一次）
go run ./cmd/genschema \
  -rules E:/cutter/syncRules.yaml \
  -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001 \
  -pkg cutterschema -func Schema -out-go ./examples/cutterschema/schema.go \
  -out-json ./examples/cutterschema/schema.json

# 全量同步 + 逐表打印“行数 + 样例”
go run ./cmd/pstable -db ./out/cutter.db -schema ./examples/cutterschema/schema.json \
  -sync -report -limit 1 \
  -endpoint http://120.24.7.22:8080 -rest http://120.24.7.22:8089/rest \
  -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001
```

实测：65 张表 / 1644 行，2 个 bucket（`org_data` + `box_data`），首次同步约 2.4s。

⚠️ 两个**必须知道**的同步规则细节（否则会“少数据”）：

- 同步规则里写了 `AS i` 这类**短别名**时，服务端下发的 `object_type` 是**别名**而不是表名；
  且 `i` 同时被 `material_inventory` / `tool_inventory` / `delivery_inventory` 复用。
  客户端只能把它们当一张表 `i`（列取并集）。**建议后端去掉这些别名**，让每张表用自己的名字。
- 部署中的规则可能比本地 yaml 新（例如服务端还下发 `role`）。生成器用 `-extra role`
  并把抓包文件喂给 `-stream` 从实际数据推断列。

## 工具与脚本

| 命令 | 用途 |
| --- | --- |
| `cmd/psdiff` | 对真实服务同步 + 落真实表 + 与 PostgREST 逐行比对（差分回归） |
| `cmd/psdemo` | 公开 API 冒烟（只读），支持 `-bson` / `-subscribe` / `-no-defaults` |
| `cmd/psupload` | 上传回显闭环：本地写 → PostgREST → 服务端回显 → 校验 → 删除 |
| `cmd/psfixture-server` | 离线假服务（回放 `testdata/sync/*.ndjson`），给官方 JS SDK 做对照 |
| `cmd/pscompare` | 比对两份 `rows.jsonl`（双实现差分门禁） |
| `cmd/psq` | 直接查本地 SQLite（`-ls`/`-q`/`-check`/**`-snapshot`**，核对落库或给图形工具做快照） |
| `examples/demo` | **交互式演示**：先验证 → 列出有数据的表 → 选表看数据（`go run ./examples/demo`） |
| `cmd/pstable` | **用 SDK 读/写路径**操作本地库：`-ls`/`-table`/`-q` 查询、`-exec` 写入/删除、`-i` 交互、`-upload` 自动上传、`-sync -report` 同步并逐表打印行数+样例 |
| `cmd/genschema` | 从 syncRules.yaml + PostgREST OpenAPI 生成客户端 schema（`-pkg main` 可生成到任意项目） |
| `scripts/go-env.ps1` | 修 GOROOT/PATH：`. .scriptsgo-env.ps1`（当前会话）或 `-FixProfile`（永久改 profile） |
| `scripts/verify-sync.ps1` | **一条命令验证双向同步**：下载+逐表校验 → 新增/修改/删除各回写后端并断言（`-KeepRow` 保留探针行） |
| `scripts/uninstall-g.ps1` | 卸载 g（Go 版本管理器）：清 PATH/profile、删 `E:gvm` 与 `%USERPROFILE%.g`（`-WhatIf` 可预览） |
| `scripts/ps-write.ps1` | Windows 一键写操作：`-Action insert/update/delete/query/list/exec` + 自动上传 + 前后端行数核对 |
| `scripts/run-js-diff.sh` | Go vs 官方 `@powersync/node` 双实现差分（Node ≥ 22.15） |
| `scripts/js-diff-container.sh` | 同上，WSL2/kiri 容器内跑（无需 Docker Desktop） |
| `scripts/probe-*.sh` | 对真实服务验证增量/checksum/BSON 等场景 |

## 测试

```bash
go test -race ./...     # 8 个包、79 个用例，覆盖率 65%~86%
```

- 协议解析用 `testdata/sync/*.ndjson|bson`（真实抓包，已脱敏裁剪）
- 公开 API 用 `httptest` 回放同一批 golden
- 双实现差分：同一假服务 → Go 与官方 JS SDK 各同步一遍 → 逐行比对（本地/CI 均可跑）

## 目录

```
db.go schema.go query.go crud.go streams.go connector/errors   # 公开 API
internal/protocol   线协议 DTO + NDJSON 解码
internal/bson       BSON 解码（无第三方依赖）
internal/core       本地库：schema/视图/触发器/oplog/checksum/物化/迁移/文件锁
internal/sync       下载+上传循环
internal/fixture    离线假服务（双实现差分用）
internal/rowdiff    落库导出比对
internal/psclient   工具共用的小 HTTP 客户端（RPC 取 token + PostgREST CRUD）
internal/restconn   示例连接器：RPC 取 token + 把 ps_crud 队列写进 PostgREST（examples 与 cmd 共用）
cmd/psq             只读查询本地 SQLite 的小工具（核对落库）
cmd/pstable         用 SDK 读/写路径操作本地库（查询/写入/删除/上传）
cmd/…               工具；examples/… 可运行示例；scripts/… 探针与 CI 辅助；testdata/… 抓包样本
```

## 已知限制

- 不提供 SQL 控制面函数（`powersync_control` / `powersync_replace_schema` /
  `powersync_create_raw_table_crud_trigger`），同步状态机在 Go 层 —— 因此官方 SDK 不能
  用它当 C 扩展替代品（但**可以读同一个库文件**，格式已对齐）。
- `powersync_crud` 用真实表 + 桥接触发器实现（core 是虚拟表），不解释 `options` 位。
- 本地写不维护 `ps_updated_rows`（物化走 oplog，不依赖它）。
- 附件（attachments）未实现。
- 单进程内 `MaxOpenConns(1)`；跨进程用 `<db>.lock` 文件锁，第二个实例会报 `ErrDatabaseLocked`。

## 许可

MIT，见 [`LICENSE`](./LICENSE)。

## 文档

- **使用手册（完整 API 参考，中文）**：[`docs/reference.md`](./docs/reference.md)
- 给非技术读者的项目状态：[`STATUS.md`](./STATUS.md)

- 架构与硬性约定：[`AGENTS.md`](./AGENTS.md)（含服务端实测行为、踩坑记录、兼容性差异）
- 变更记录：[`CHANGELOG.md`](./CHANGELOG.md)
