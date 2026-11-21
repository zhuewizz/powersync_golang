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

> - `real` 模式的 `UploadData` 是**空实现**（打印后直接出队），不会写你的后端；
>   加 `-write` 会做“插入 → 更新 → 删除”的本地演示，同样不写后端。
> - 真实项目请参考 [`docs/reference.md`](../docs/reference.md) 第 4 节的 Connector 实现。

### 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-mode` | `mock` | `mock` / `real` |
| `-db` | `out/example.db` | 本地数据库路径 |
| `-schema` | 空（内置示例 schema） | 真实 schema JSON（官方 `Schema.toJSON()` 形状） |
| `-tables` | 空（schema 全部） | 只统计/打印这些表（逗号分隔） |
| `-write` | `false` | real 模式下也做写入/上传演示（mock 默认就做） |
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
