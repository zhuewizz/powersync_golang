# FAQ

> 面向使用者的常见问题。协议细节见 `AGENTS.md` §6（真实服务实测）、使用手册见 `docs/reference.md`。

---

## Q1. 客户端和 PowerSync 服务之间是 WebSocket 还是 HTTP 轮询？

**都不是纯轮询：下载走一条 HTTP 长响应流（chunked），上传走普通 HTTP 请求，本地变化靠 SQLite 触发器推送。全程没有 WebSocket。**

### 1）下载（服务端 → 本地）：HTTP 流式响应

```
POST /sync/stream
Authorization: Token <jwt>
Content-Type: application/json
Accept: application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8
```

服务端返回 `200` + **chunked 流**（不是 WebSocket，也不是 SSE），一次连接持续下发：

```jsonc
{"checkpoint":{...}}                       // 每批开头的 bucket 清单 + checksum
{"data":{...,"data":[op,...]}}             // 数据行（同 bucket 可能多条、has_more 分页）
{"checkpoint_complete":{...}}              // 本批完成
{"token_expires_in":7062}                  // keepalive（实测约 20s 一次）
```

- 客户端持续读流；断线后用本地游标（`ps_buckets.last_op`）重连**续传**，不会全量重拉；
- 服务端支持 BSON 时体积约小 8%（`Accept` 里协商），落库结果与 NDJSON 逐行一致；
- 请求里**必须带上本地已知的全部 bucket**（漏一个，服务端会把它从 0 全量重发）。

### 2）鉴权：HTTP + RPC

```
POST /rest/rpc/fetch_credentials  {"box_code": "...", "org_code": "..."}  →  {"token": "<jwt>"}
```

收到 `401` 时通过 connector 的 `InvalidateCredentials` 重取 token 再重试（不会无脑重试）。

### 3）上传（本地写 → 你的后端）：普通 HTTP 请求

SDK 只负责队列（`ps_crud`），**HTTP 由使用方的 connector 实现**。仓库里的 `msmkSync/restconn`
是 PostgREST 参考实现：

```
PUT    → POST   /<table>  ?resolution=merge-duplicates     (upsert)
PATCH  → PATCH  /<table>  ?id=eq.<id>
DELETE → DELETE /<table>  ?id=eq.<id>
成功后 GET /write-checkpoint2.json?client_id=…  →  batch.Complete(ctx, writeCheckpoint)
```

触发方式：本地写入**立刻唤醒**一次；另外每 **1s** 轮询一次队列兜底（`Options.UploadInterval`）；
失败按指数退避（1s→30s，带抖动、可被 ctx 取消）。

### 4）其它"轮询"（都不是同步数据）

| 用途 | 方式 | 频率 |
| --- | --- | --- |
| 网络在线判断 | **TCP 拨号**（不是 HTTP） | 5s，超时 2s |
| 本地数据变化（`Watch`） | SQLite 触发器 + 版本号**推送**；轮询仅兜底 | 兜底 2s（实测推送毫秒级） |
| 桌面 UI ↔ Go（Wails） | Wails IPC（WebView2 host object） | 即时 |

> 一句话：**客户端 ↔ 服务 = HTTP 长响应流（NDJSON/BSON）；上传 = 普通 HTTP（由你的 connector 决定）；
> 本地变化 = 触发器推送；轮询只做兜底。**

---

## Q3. 连接参数（服务地址 / box / org）放在哪里？代码里能写死吗？

**不要写死**。本仓库的代码/脚本/示例里都已经没有硬编码的地址、柜号、组织，按优先级从这三处取：

| 方式 | 适用 | 说明 |
| --- | --- | --- |
| 环境变量 | 命令行工具、CI | `PS_URL` / `RPC_URL` / `REST_URL` / `BOX_CODE` / `ORG_CODE` |
| `config.json` | 桌面应用 | 放在**可执行文件同目录**，界面「⚙ 设置」保存后自动写入（已 gitignore） |
| 界面填写 | Wails 桌面版 | 首次启动若没配置会自动弹出设置框 |

`config.json` 例：

```json
{
  "endpoint": "http://<your-powersync-host>:8080",
  "rpc": "http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials",
  "rest": "http://<your-postgrest-host>:8089/rest",
  "box": "<box_code>",
  "org": "<org_code>"
}
```

PowerShell 里设环境变量（当前窗口有效）：

```powershell
$env:PS_URL="http://<your-powersync-host>:8080"
$env:REST_URL="http://<your-postgrest-host>:8089/rest"
$env:RPC_URL="http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials"
$env:BOX_CODE="<box_code>"; $env:ORG_CODE="<org_code>"
```

---

## Q2. 在别的目录新建一个项目，需要哪些 SDK 包 / 依赖？

结论：**运行时只需要 SDK 根包 `msmkSync`（一个 import）**；要注意的就是 `go.mod` 的两行和"schema 要自己生成"。

### 最小依赖

```
msmkSync                    ← 唯一的必需依赖（包名 powersync）
  └─ modernc.org/sqlite     ← SQLite 驱动（纯 Go，无 cgo），由 SDK 自动带出
  └─ golang.org/x/sys       ← 文件锁（flock / LockFileEx）
```

可选（按需）：

| 包 | 什么时候需要 |
| --- | --- |
| `msmkSync/restconn` | 后端是 PostgREST，想直接用现成的上传连接器（PUT/PATCH/DELETE + write-checkpoint） |
| `msmkSync/cmd/genschema` | **构建期**工具：从 `syncRules.yaml` + PostgREST OpenAPI 生成客户端 schema（运行时不依赖） |
| `msmkSync/examples/cutterschema` | 仅本仓库这个项目（cutter）的 schema，新项目请自己生成 |

> ❌ 不要 import `msmkSync/internal/...`（Go 的 internal 规则不允许外部模块引用；其中 `internal/psclient`
> 之类的工具只服务于本仓库）。`restconn` 已经提到公共路径就是因为这个原因。

### 从 GitHub 用（已发布）

SDK 已推到 <https://github.com/zhuewizz/powersync_golang>（private，tag `v0.1.0`）：

```bash
# 私有仓库要先配好凭据（GitHub 登录 / PAT），并让 go 走私有模块
export GOPRIVATE=github.com/zhuewizz/*        # PowerShell: $env:GOPRIVATE="github.com/zhuewizz/*"

mkdir myapp && cd myapp
go mod init myapp
go get github.com/zhuewizz/powersync_golang@v0.1.0
```

代码里：

```go
import powersync "github.com/zhuewizz/powersync_golang"
```

本地开发（改 SDK 同时改 App）仍推荐 clone 两个仓库 + replace：

```
require github.com/zhuewizz/powersync_golang v0.0.0
replace github.com/zhuewizz/powersync_golang => ../powersync_golang
```

### 新建项目的步骤

```bash
mkdir myapp && cd myapp
go mod init myapp                 # Go >= 1.25
# 指向本地的 SDK（尚未发布到远端仓库时用 replace）
go mod edit -require=msmkSync@v0.0.0
go mod edit -replace=msmkSync=../powersync_golang
go get msmkSync
go mod tidy
```

`go.mod` 大致长这样：

```
module myapp

go 1.25

require github.com/zhuewizz/powersync_golang v0.0.0

replace github.com/zhuewizz/powersync_golang => ../powersync_golang
```

### 客户端代码（最小可用）

```go
package main

import (
	"context"
	"log"

	powersync "github.com/zhuewizz/powersync_golang"
	"github.com/zhuewizz/powersync_golang/restconn"
)

func main() {
	ctx := context.Background()

	// 1) schema：PowerSync 强制要求（没声明的表不会同步）。见下面"生成 schema"。
	sch := mustSchema()

	// 2) connector：取 token + 上传（不实现上传可给 nil，Connect 时会报 ErrNotConnected）
	conn := restconn.New(restconn.Config{
		Endpoint: "http://<your-powersync-host>:8080",
		RPC:      "http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials",
		Box:      "<box_code>",
		Org:      "<org_code>",
		RESTBase: "http://<your-postgrest-host>:8089/rest",
	})

	// 3) 打开本地库（SQLite 是本地唯一事实来源）
	db, err := powersync.Open(ctx, powersync.Options{
		Filename:  "app.db",
		Schema:    sch,
		Connector: conn,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	// 4) 连接：后台跑下载流 + 上传循环
	if err := db.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()
	if err := db.WaitForFirstSync(ctx); err != nil {
		log.Fatal(err)
	}

	// 5) 读 / 写（写会自动进上传队列）
	rows, _ := db.GetAll(ctx, `SELECT * FROM cutter_info LIMIT 5`)
	log.Println(rows)
	if _, err := db.Execute(ctx, `INSERT INTO sys_error_report(id, org_code, box_code, error_desc) VALUES(?,?,?,?)`,
		newUUID(), "<org_code>", "<box_code>", "hello"); err != nil {
		log.Println(err)
	}

	// 6) 变化推送（本地写和同步下发都会触发）
	ch, _ := db.Watch(ctx, `SELECT count(*) AS n FROM cutter_info`)
	for r := range ch {
		log.Println("行数:", r[0]["n"])
	}
}
```

### 生成 schema（新项目必做）

```bash
cd E:/powersync_golang
go run ./cmd/genschema \
  -rules E:/cutter/syncRules.yaml \
  -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials -box <box_code> -org <org_code> \
  -extra role -stream ./out/stream-raw.ndjson \
  -pkg main -func mySchema \
  -out-go ../myapp/schema.go -out-json ../myapp/schema.json
```

- 生成的 `schema.go` 是"表 + 列"声明（一行一张表，可手改）；`schema.json` 给 `-schema` 参数/其它语言用；
- 服务端表结构变了就重新生成一次；运行时不依赖 `syncRules.yaml`。

### 常见坑

| 现象 | 原因 / 处理 |
| --- | --- |
| `ErrSchemaRequired` | 没有传 `Schema`；PowerSync 要求客户端声明 schema |
| 某张表完全没有数据 | 表不在客户端 schema 里 → 被跳过；看 `db.SkippedTables()`，补进 schema 后需重同步（删库或 `DisconnectAndClear`） |
| `ErrDatabaseLocked` | 同一 `app.db` 已被另一个实例同步；等待（`Options.LockTimeout`）、换库文件，或只读场景用 `NoFileLock: true` |
| `go: cannot find GOROOT`（Windows） | PowerShell profile 里的 `GOROOT` 坏了；`. .\scripts\go-env.ps1` 或 `-FixProfile` |
| 需要 cgo | 不需要。SDK 固定用 `modernc.org/sqlite`，`CGO_ENABLED=0 GOOS=linux go build ./...` 可交叉编译 |
| 想用官方 `powersync` C 扩展 | 不支持（与纯 Go 目标冲突）；同步逻辑已在 Go 里实现 |
| 需要附件上传下载 | 未实现，由使用方自行处理 |

### 打包/发布注意

- 库代码全部在根包，其余在 `internal/`；发布时把 module 路径改成 `github.com/<org>/msmkSync` 即可
  （当前 `module github.com/zhuewizz/powersync_golang` 只能在本地用 `replace` 引用）。
- 不用带 `cmd/`、`examples/`、`scripts/`、`testdata/`（工具与示例，不影响 import）。
