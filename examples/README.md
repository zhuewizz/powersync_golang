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

### 方式三：连真实服务并**真正上传**

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

行为说明：

- `PUT` → PostgREST upsert（`resolution=merge-duplicates`）、`PATCH` → update、`DELETE` → delete；
  每批上传成功后取 legacy `write-checkpoint2.json` 再 `Complete`（让服务端回显本地写，cursor 会前进）。
- 探针行 id 用 **UUIDv4**（真实后端主键多为 `uuid` 列；用普通字符串会 400）。
- HTTP **4xx**（如约束冲突）视为不可重试：打印后丢弃该批，避免堵住队列；5xx/网络错误仍会退避重试。
- 演示行（只碰自己生成的 id）在结束时删除；异常退出时还有一次兜底删除。
- 只支持在 `sys_error_report` 上演示真实上传（其它表缺列/约束不确定）；选错会提示并改为只读。

### 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-mode` | `mock` | `mock` / `real` |
| `-db` | `out/example.db` | 本地数据库路径 |
| `-schema` | 空（内置示例 schema） | 真实 schema JSON（官方 `Schema.toJSON()` 形状） |
| `-tables` | 空（schema 全部） | 只统计/打印这些表（逗号分隔） |
| `-write` | `false` | real 模式下也做写入/上传演示（mock 默认就做） |
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
