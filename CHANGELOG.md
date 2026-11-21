# Changelog

模块：`msmkSync`（包名 `powersync`）。许可：MIT。

## 0.1.0 — 2026-11

首个可用版本：纯 Go 的 PowerSync 客户端，已对真实自建服务做端到端验证。

### 新增

- **协议**：`/sync/stream`（NDJSON + BSON，**默认 BSON**，按响应 `Content-Type` 自动回落）、
  `checkpoint` / `data` / `checkpoint_complete` / `partial_checkpoint_complete` / `token_expires_in`、
  `POST /sync/checkpoint-request`、legacy `GET /write-checkpoint2.json`、401 重取凭据。
- **本地库（对齐 `powersync-sqlite-core`）**：`ps_data__*` + 同名视图 + INSTEAD OF 触发器、
  `ps_oplog`、`ps_buckets`（checksum/游标）、`ps_crud`、`ps_kv`、`ps_tx`、`ps_untyped`（schema 迁移）、
  `ps_updated_rows`、`ps_sync_state`、`ps_stream_subscriptions`、`powersync_crud` 兼容层、
  `include_metadata`（`_metadata`/`_deleted` + 带 metadata 的删除）、
  `direct:true` 列式表、`local_only` 表。
- **SQL 函数**：`powersync_diff` / `powersync_json_merge` / `powersync_client_id` /
  `powersync_version` / `powersync_rs_version`（语义对齐 core 的 `diff.rs`）。
- **同步**：下载 + 上传循环、断点续传、checksum 累积与漂移恢复（重置后从 `after=0` 重下）、
  上传回显门控（write checkpoint）、指数退避、token 预取、sync streams 订阅/TTL/优先级、
  按优先级分批应用、多进程 `.lock` 文件锁。
- **公开 API**：`Open/Connect/Disconnect/Close`、`Execute/GetAll/GetOptional/WriteTransaction`、
  `Watch`（触发器驱动推送 + 64 位哈希去重（0 分配），兜底轮询 2s）、`WatchTables`（表级版本，
  无关写入不重查）、`WatchStatus`/`WaitForStatus`、`Get`/`ExecuteBatch`/`DisconnectAndClear`、
  `AdditionalHeaders`、`powersync_crud.options` bit0 = `ignore_empty_update`、`GetCrudBatch/GetNextCrudTransaction`、
  `SubscribeStream`（`WithTTL`/`WithPriority`）、`CurrentStatus`/`WaitForFirstSync`、
  `UpdateSchema`、`LoadSchemaJSON`。
- **网络状态监控**：`CurrentNetworkStatus` / `WatchNetworkStatus`（TCP 拨号探测 + 变化推送，
  同步失败/成功时立即重测）、`Options.NetworkTarget`/`NetworkCheckInterval`/`NetworkCheckTimeout`/
  `DisableNetworkMonitor`。
- **完整示例**：`examples/todo`（默认离线 mock，内置假服务与假后端；可切到真实服务）。
- **使用手册**：`docs/reference.md`（对齐官方 Flutter SDK 参考的中文 API 文档，含与官方差异清单）。
- **工具**：`cmd/psq`（只读查询本地 SQLite；`-check` 完整性检查；`-snapshot` 用 `VACUUM INTO` 生成快照供图形工具打开）；`cmd/psdiff`（真机差分）、`cmd/psdemo`（冒烟）、`cmd/psupload`（上传回显闭环）、
  `cmd/psfixture-server`（离线假服务）、`cmd/pscompare`（双实现落库比对）；
  `scripts/probe-*.sh`、`scripts/run-js-diff.sh`、`scripts/js-diff-container.sh`。
- **工程**：`internal/sync` / `internal/psclient` 直接单测（覆盖率 74%/83%）、工具共用
  `internal/psclient`（去掉三份重复的 token/REST 样板）、`BenchmarkWatchChangeDetection`。
- **CI**：`go vet` / `gofmt` / `go test -race` / 无 cgo 构建 + Go vs 官方 JS SDK 差分 job。

### 服务端实测发现（已在 `AGENTS.md` 记录）

- `data.after` 是本批的**左开下界**，不是进度；进度必须取本批最后一条 `op_id`。
- 未列出的 bucket 会被**全量重发**；请求必须带全部已游标化的 bucket。
- 请求游标要用 `ps_buckets.last_op`（staged），不是 `last_applied_op`。
- checksum 是**带符号 int32**；bucket checksum 是累计链（删除后不会回到原值）。
- checksum 校验必须在 `checkpoint_complete`；同连接内后续批次没有新的 `checkpoint` 行，需跳过校验。
- 32 位 checksum、`REMOVE` 的负 checksum、`REMOVE` 的 `data:null`。
- `streams.subscriptions` 与其中的 `override_priority` 都是**必填属性**（否则 400）。
- BSON 与 NDJSON 逐行等价（约 -8% 体积）。

### 已知限制

见 `README.md`「已知限制」与 `AGENTS.md` §5.2。
