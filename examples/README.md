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

### 方式二：连真实服务（只下载）

```bash
go run ./examples/todo -mode real \
  -endpoint http://<host>:8080 \
  -rpc http://<host>:8089/rest/rpc/fetch_credentials \
  -box D1-2 -org ORG001
```

> 示例的 `real` 模式下 `UploadData` 是空实现（打印后直接出队），避免误写生产数据。
> 真实项目请参考 [`docs/reference.md`](../docs/reference.md) 第 4 节的 Connector 实现。

### 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-mode` | `mock` | `mock` / `real` |
| `-db` | `out/example.db` | 本地数据库路径 |
| `-timeout` | `30s` | 整体超时 |
| `-endpoint` `-rpc` `-box` `-org` | 环境变量 `PS_URL` `RPC_URL` `BOX_CODE` `ORG_CODE` | real 模式参数 |

### 代码重点

| 位置 | 说明 |
| --- | --- |
| `fixture.ndjson` | 内置假服务的回放数据（一个 bucket、两行 todos） |
| `db.WatchStatus` / `db.WatchNetworkStatus` / `db.WatchTables` | 三种推送式监听 |
| `db.Execute` / `db.ExecuteBatch` / `db.WriteTransaction` | 三种写法（都自动入上传队列） |
| `powersync.Table{LocalOnly: true}` | 只在本机、不同步的表 |
| `mockConnector` / `realConnector` | Connector 的两种实现（上传逻辑在 `UploadData` 里） |
