# testdata/sync

对**真实自建 PowerSync 服务**抓包后**脱敏 + 裁剪**的协议样本，用于解析层单测与 golden 对比。

- 来源：`powersync-service` 自建实例（`POST /sync/stream`），2026-11 抓取。
- 脱敏：真实业务值已替换为 `sample-*` / 占位 UUID；行数与 op 数已裁剪。
- 因此**不能**用于校验 checksum 累计逻辑（checksum 值与 op 内容已不匹配），只用于验证**结构与解析**。

| 文件 | 场景 |
| --- | --- |
| `full-snapshot-raw.ndjson` | 冷启动全量：`checkpoint` → 多个 `data`（含同 bucket 分片 + `has_more`）→ `checkpoint_complete` → `token_expires_in`；`raw_data=true`，`data` 为 JSON **字符串** |
| `up-to-date.ndjson` | 已同步到最新：只有 `checkpoint` + `checkpoint_complete` + keepalive |
| `raw-data-false.ndjson` | `raw_data=false`：`data` 为 JSON **对象** |
| `put-remove.ndjson` | 增量删除：PUT 后 REMOVE（`data:null`，subkey 不变）；含实测的**负 checksum**（-96505511）|
| `apply-todos.ndjson` | **双实现差分**用的固定 fixture（一个 bucket / 2 行 / checksum 自洽），配 `testdata/schema/todos.json` |
| `error-401.json` | 鉴权失败的统一错误信封 `{"error":{"code","status","description","name"}}` |

注意：`put-remove.ndjson` 是把**两次连接**（插入后的 PUT、删除后的 REMOVE）拼在一个文件里，仅用于解析测试。
`cmd/psdiff` 的端到端基线见 AGENTS.md §11.1；`scripts/probe-remove.mjs` 用于重新抓这两个场景
（该服务端复制延迟实测 45–90 s，脚本默认轮询 240 s）。

重新抓包：

```bash
PS_URL=http://<host>:8080 PS_TOKEN=<jwt> ./scripts/probe-sync.sh
# 增量 PUT/REMOVE：
PS_URL=... REST_URL=... PS_TOKEN=<jwt> node scripts/probe-remove.mjs
# 或从 PostgREST RPC 换 token：
PS_URL=... RPC_URL=.../rest/rpc/fetch_credentials BOX_CODE=... ORG_CODE=... ./scripts/probe-sync.sh
```

新增样本时同样要脱敏（尤其 `cam_pass`、token、姓名、手机号）并裁剪到几十行以内。
