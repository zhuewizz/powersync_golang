# 示例：examples/todo

## 直接运行（参数已写死，无需 -schema/-endpoint）

```powershell
cd E:\powersync_golang
. .\scripts\go-env.ps1          # 只在 go 报 GOROOT 错时需要
go run ./examples/todo
```

默认行为（全部写死在 `main.go` 的 flag 默认值里）：

| 参数 | 默认值 |
| --- | --- |
| `-mode` | `real`（连 http://<your-powersync-host>:8080） |
| `-cutter` | `true`（内置 65 张表 schema） |
| `-rest` / `-rpc` | http://<your-postgrest-host>:8089/rest / …/rpc/fetch_credentials |
| `-box` / `-org` | <box_code> / <org_code> |
| `-db` | `out/example.db`（**每次启动先删掉**，保证全量重同步） |
| `-write` / `-upload` | `true` / `real`（真写 PostgREST；探针行默认保留） |
| `-detail` | `cutter_info,sys_error_report`（表多时只对这些表打样例） |

跑完会依次输出：

1. 连接 → 首次同步（cursor / buckets / 耗时）
2. **每张表行数总表**（65 张表 + 合计 + 空表数）
3. 关键表样例数据（`-detail` 指定的表）
4. 离线优先演示：断开 → 断网读本地 → 断网写（入队）→ 恢复 → 自动上传 → 后台校验
5. 探针行**保留**在后台（`-cleanup` 才删）

## 常用覆盖

```powershell
go run ./examples/todo -cleanup                     # 演示完删掉探针行
go run ./examples/todo -tables cutter_info -limit 5 # 只看一张表、多打几行
go run ./examples/todo -mode mock                   # 完全离线（内置假服务 + todos schema）
go run ./examples/todo -verify                      # 额外逐行比对后台 REST（表多时很冗长）
go run ./examples/todo -db .\out\mine.db -box D1-1   # 换库/换柜
```

## 交互式版：examples/demo

```powershell
go run ./examples/demo                 # 交互：先验证，再列出有数据的表，选一个看数据
go run ./examples/demo -cols "id,mat_code,mat_name"   # 只看几列
go run ./examples/demo -all            # 连空表一起列
go run ./examples/demo -limit 0        # 表数据全打出来
go run ./examples/demo -once           # 只同步+打印表清单，非交互（脚本用）
```

运行效果：

```
===== 1/3 验证环境 =====
  本地库: out/demo.db
  schema: 65 张表（来自 syncRules.yaml）
  已连接，等待首次同步…
  ✅ 首次同步完成：耗时 2.187s，cursor=24501，buckets=2
  ✅ 没有被跳过的行（schema 覆盖了服务端下发的所有表）

===== 2/3 表清单（32 张有数据，合计 1626 行）=====
   #  表                                      行数
------------------------------------------------
   1  cutter_transaction                    309
   2  work_code_bom                         182
   ...
   5  i                                     120  *
  11  cutter_info                            60

输入表编号查看数据（回车刷新列表，q 退出）：11

===== 3/3 表 cutter_info（共 60 行，显示 20 行）=====
id                                    mat_code  mat_name           mat_pn
-----------------------------------------------------------------------------------
03b01053-a96c-440f-b1fc-50c6a97...    MAT00075  按钮开关           PN-BTN-GRN
```

`*` 标记的是同步规则里的**短别名表**（`AS i` 之类），实体分散在 material_*/tool_*/delivery_*。

## 方式二：手动指定参数（原始用法，留作参考）

最简单：加 `-cutter` 用内置的 65 张表 schema（来自 `syncRules.yaml`，见 `examples/cutterschema`）：

**PowerShell（Windows）**：先用 `. .\scripts\go-env.ps1` 修 GOROOT，且**不用 `\` 续行**：

```powershell
. .\scripts\go-env.ps1
go run ./examples/todo -mode real -cutter -endpoint http://<your-powersync-host>:8080 -rest http://<your-postgrest-host>:8089/rest -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials -box <box_code> -org <org_code> -tables cutter_info,sys_error_report,org
```

**bash/Git Bash**：

```bash
go run ./examples/todo -mode real -cutter \
  -endpoint http://<your-powersync-host>:8080 \
  -rest http://<your-postgrest-host>:8089/rest \
  -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials \
  -box <box_code> -org <org_code> \
  -tables cutter_info,sys_error_report,org
```

输出：

```
schema: 内置 cutter（65 张表，来自 syncRules.yaml；见 examples/cutterschema）
===== 校验：后台 REST → 本地（逐行比对）=====
  表 cutter_info          OK  本地=60 后台=60 一致=60
  表 sys_error_report     OK  本地=2  后台=2  一致=2
```

也可以自己导出 schema（旧路子，只含 AppSchema.ts 里的 37 张表）：

```bash
# 先导出真实 schema（复用 e:/cutter_client 的 AppSchema.ts）
node scripts/js-reference/dump-schema.mjs        # 生成 out/schema.json

go run ./examples/todo -mode real \
  -endpoint http://<your-powersync-host>:8080 \
  -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials \
  -box <box_code> -org <org_code> \
  -schema ./out/schema.json \
  -tables sys_error_report,cutter_info
```

实测输出（节选）：

```
schema: 来自 ./out/schema.json（37 张表）
模式: real（http://<your-powersync-host>:8080）
  [网络] online=true target=<your-host>:8080 latency=22ms err=
  [状态] connected=true cursor=24429 lastSynced=18:42:58

表 sys_error_report: 0 行（样例 0 行）
表 cutter_info: 60 行（样例 3 行）
    id=03b01053-… mat_name=按钮开关 mat_pn=PN-BTN-GRN org_code=<org_code> …
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
go run ./examples/todo -mode real   -endpoint http://<your-powersync-host>:8080 -rest http://<your-postgrest-host>:8089/rest   -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials   -box <box_code> -org <org_code>   -schema ./out/schema.json -tables cutter_info,sys_error_report
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
go run ./examples/todo -mode real -upload real -write   -endpoint http://<your-powersync-host>:8080 -rest http://<your-postgrest-host>:8089/rest   -rpc http://<your-postgrest-host>:8089/rest/rpc/fetch_credentials   -box <box_code> -org <org_code>   -schema ./out/schema.json -tables sys_error_report
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
