# 示例

## todo（完整流程）

一个可以直接 `go run` 的完整示例，演示：

1. 定义 schema（同步表 `todos` + 仅本地表 `local_notes`）
2. 打开数据库（文件锁、schema 自动落地）
3. 连接 PowerSync 服务并等待首次同步
4. 三个实时监听：同步状态、网络状态、查询结果（推送，不是轮询）
5. 本地写（单条 / 批量 / 删除 / 事务）→ 自动进入上传队列 → 上传出队
6. 打印最终数据与状态

### 方式一：离线跑（推荐先跑这个）

内置一个假的 PowerSync 服务（回放 `fixture.ndjson`）+ 假后端，**不连任何真实服务**：

```bash
go run ./examples/todo
```

预期输出（节选）：

```
模式: mock（内置假服务 http://127.0.0.1:xxxxx）
  [网络] online=true target=127.0.0.1:xxxxx latency=1ms err=
  [状态] connected=true uploading=false cursor=101 lastSynced=18:21:01
  [查询] todos 行数 = 2
  [上传] PUT    todos/local-1 data=map[done:0 title:本地新建的任务]
  ...
✅ 示例跑完（数据库在 out/example.db）
```

数据库写在 `out/example.db`，可以重复跑（每次会先删掉）。

### 方式二：连真实服务（看真实表）

最简单：加 `-cutter` 用内置的 65 张表 schema（来自 `syncRules.yaml`，见 `schema_cutter.go`）：

**PowerShell（Windows）**：先用 `. .\scripts\go-env.ps1` 修 GOROOT，且**不用 `\` 续行**：

```powershell
. .\scripts\go-env.ps1
go run ./examples/todo -mode real -cutter -endpoint http://120.24.7.22:8080 -rest http://120.24.7.22:8089/rest -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials -box D1-2 -org ORG001 -tables cutter_info,sys_error_report,org
```

**bash/Git Bash**：

```bash
go run ./examples/todo -mode real -cutter \
  -endpoint http://120.24.7.22:8080 \
  -rest http://120.24.7.22:8089/rest \
  -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
  -box D1-2 -org ORG001 \
  -tables cutter_info,sys_error_report,org
```

输出：

```
schema: 内置 cutter（65 张表，来自 syncRules.yaml；见 schema_cutter.go）
===== 校验：后台 REST → 本地（逐行比对）=====
  表 cutter_info          OK  本地=60 后台=60 一致=60
  表 sys_error_report     OK  本地=2  后台=2  一致=2
```

也可以自己导出 schema（旧路子，只含 AppSchema.ts 里的 37 张表）：

```bash
# 先导出真实 schema（复用 e:/cutter_client 的 AppSchema.ts）
node scripts/js-reference/dump-schema.mjs        # 生成 out/schema.json

go run ./examples/todo -mode real \
  -endpoint http://120.24.7.22:8080 \
  -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials \
  -box D1-2 -org ORG001 \
  -schema ./out/schema.json \
  -tables sys_error_report,cutter_info
```

实测输出（节选）：

```
schema: 来自 ./out/schema.json（37 张表）
模式: real（http://120.24.7.22:8080）
  [网络] online=true target=120.24.7.22:8080 latency=22ms err=
  [状态] connected=true cursor=24429 lastSynced=18:42:58

表 sys_error_report: 0 行（样例 0 行）
表 cutter_info: 60 行（样例 3 行）
    id=03b01053-… mat_name=按钮开关 mat_pn=PN-BTN-GRN org_code=ORG001 …
（real 模式默认只读；要演示写入/上传请加 -write）
```

> - **real 模式必须带 `-schema`**（PowerSync 要求客户端声明 schema）；mock 模式用内置示例 schema。
> - **schema 决定本地有哪些表**：不带 `-schema` 时只有内置示例表 `todos`，
>   真实服务的其它表会被跳过（示例会打印"有 N 行因为表不在 schema 中被跳过"）。
> - 默认 `-upload mock`：`UploadData` 打印后直接出队，不写后端；`-write` 只是本地插入/更新/删除演示。
> - 真实项目请参考 [`docs/reference.md`](../docs/reference.md) 第 4 节的 Connector 实现。

### 方式二点五：验证"后台数据自动同步到本地"

只要 `-mode real -rest ... -schema ...`，示例会在首次同步后自动做**逐行逐字段校验**
（`-verify` 默认开启）：把本地表与后台 PostgREST 对比，证明数据确实同步下来了。

```bash
go run ./examples/todo -mode real   -endpoint http://120.24.7.22:8080 -rest http://120.24.7.22:8089/rest   -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials   -box D1-2 -org ORG001   -schema ./out/schema.json -tables cutter_info,sys_error_report
```

实测输出：

```
===== 校验：后台 REST → 本地（逐行比对）=====
  表 cutter_info          OK  本地=60 后台=60 一致=60 本地多=0 后台多=0 字段不同=0 软删除=0
  表 sys_error_report     OK  本地=1  后台=1  一致=1  本地多=0 后台多=0 字段不同=0 软删除=0
  ✅ 后台数据已完整同步到本地（逐行逐字段一致）
```

比对规则与 `cmd/psdiff` 一致：时间戳空格/T、数值字符串 vs 数字、jsonb 文本 vs 对象都会归一化；
`deleted_at` 非空的软删除行后台视图不展示，按"预期差异"单独计数。
加 `-verify=false` 可关闭。

### 方式三：连真实服务 + 真正上传 + 离线优先演示

```bash
go run ./examples/todo -mode real -upload real -write   -endpoint http://120.24.7.22:8080 -rest http://120.24.7.22:8089/rest   -rpc http://120.24.7.22:8089/rest/rpc/fetch_credentials   -box D1-2 -org ORG001   -schema ./out/schema.json -tables sys_error_report
```

实测输出：

```
上传: real（会真正写 PostgREST；演示行结束会删除）
— 真实上传演示（表 sys_error_report，id=fc8b09f6-…）—
  [上传-real] PUT    sys_error_report/fc8b09f6-…
  [后端] 插入后该行存在: true
  [上传-real] PATCH  sys_error_report/fc8b09f6-…
  [上传-real] DELETE sys_error_report/fc8b09f6-…
  [后端] 删除后该行存在: false
最终状态: connected=true cursor=24432 buckets=2
✅ 示例跑完
```

现在这条命令演示的是**离线优先闭环**（对应实际作业场景）：

```
===== 离线优先演示（断网可用 → 恢复后自动同步）=====
① 已断开同步（模拟断网）
② 断网状态下读本地数据（不依赖网络）：
表 cutter_info: 60 行（直接从本地 SQLite 读，断网也能用）
③ 断网状态下本地插入一行 sys_error_report/<uuid>（进入上传队列）
   上传队列：1 条待同步（本地数据已可用）
④ 恢复连接（Connect）→ 等待上传队列清空
  [上传-real] PUT    sys_error_report/<uuid>
   后台校验：该行存在 = true
⑤ 保留后台这一行（id=...），方便查看同步结果
```

行为说明：

- **默认保留后台数据**：探针行插入后会留在 `sys_error_report` 里方便你查看；
  加 `-cleanup` 才会删除（且只删它自己生成的 id）。
- 断网期间：本地读写完全正常（SQLite 本地库），写操作进入上传队列等待；
  恢复后自动按顺序上传（PUT/PATCH/DELETE），每批成功后取 `write-checkpoint2.json` 再 `Complete`。
- 探针行 id 用 **UUIDv4**（真实后端主键是 `uuid` 列；普通字符串会 400）。
- HTTP **4xx** 视为不可重试：打印后丢弃该批，避免堵住队列；5xx/网络错误仍退避重试。
- 写入演示只支持 `sys_error_report`（其它表列/约束不确定）；选错会提示并改为只读。
- 想手动作业数据（如 `cutter_info`）：直接改本地数据，然后把 `UploadData` 里的
  `applyCrudEntry` 换成你自己的后端接口即可（示例已给出 PostgREST 版本）。

### 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-mode` | `mock` | `mock` / `real` |
| `-db` | `out/example.db` | 本地数据库路径 |
| `-schema` | 空（内置示例 schema） | 真实 schema JSON（官方 `Schema.toJSON()` 形状） |
| `-tables` | 空（schema 全部） | 只统计/打印这些表（逗号分隔） |
| `-write` | `false` | real 模式下也做写入/上传演示（mock 默认就做） |
| `-cleanup` | `false` | 演示结束删除探针行（默认**保留**在后台） |
| `-verify` | `true` | real 模式下把本地表与后台 REST 逐行比对（需 `-rest`） |
| `-upload` | `mock` | real 模式的上传方式：`mock`（打印后出队）/ `real`（真正写 PostgREST） |
| `-rest` | 环境变量 `REST_URL` | `upload=real` 时的 PostgREST base，如 `http://host:8089/rest` |
| `-limit` | `3` | 每张表打印多少行样例 |
| `-timeout` | `90s` | 整体超时 |
| `-endpoint` `-rpc` `-box` `-org` | 环境变量 `PS_URL` `RPC_URL` `BOX_CODE` `ORG_CODE` | real 模式参数 |

### 代码重点

| 位置 | 说明 |
| --- | --- |
| `fixture.ndjson` | 内置假服务的回放数据（一个 bucket、两行 todos） |
| `db.WatchStatus` / `db.WatchNetworkStatus` / `db.WatchTables` | 三种推送式监听 |
| `db.Execute` / `db.ExecuteBatch` / `db.WriteTransaction` | 三种写法（都自动入上传队列） |
| `powersync.Table{LocalOnly: true}` | 只在本机、不同步的表 |
| `mockConnector` / `realConnector` | Connector 的两种实现（上传逻辑在 `UploadData` 里） |
