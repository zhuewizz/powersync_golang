# examples/wailsapp — Wails v3 桌面版双向同步演示

把 `msmkSync` 的双向同步做成一个可点的桌面应用（Wails v3.1 + WebView2 + 原生 JS，无框架）。

## 功能

| 区域 | 内容 |
| --- | --- |
| 顶栏 | 连接状态、网络延迟、cursor、buckets、待上传队列、最近同步时间；重连 / 刷新 / 新增 |
| 左栏 | **所有表清单**（行数倒序，可过滤"只看有数据"，短别名表标 `*`），点一下看数据 |
| 右侧 | 当前表的数据表格（分页、每页条数）、每行「编辑 / 删除」、`核对后端`（本地 vs 后端行数） |
| 弹窗 | 新增/编辑表单：按 schema 渲染每一列（带类型），支持"写 NULL" |
| 事件 | 本地写、同步下来的数据变化都会自动刷新（`data:changed`）；状态每秒推送（`sync:status`） |

数据源与 `examples/demo` 完全一致：`examples/cutterschema`（来自 `syncRules.yaml`，65 张表）+ 真实服务端。

## 运行

```powershell
cd E:\powersync_golang\examples\wailsapp

# 开发（热重载，需要 wails3 CLI）
wails3 dev

# 打包
wails3 build          # 产物：bin\wailsapp.exe

# 不开窗口的自检（CI / 快速验证 Go 侧）
go run . -selftest            # 同步 + 打印表清单
go run . -selftest -writes    # 再跑一遍 增→改→删（sys_error_report）
go run . -selftest -db out/x.db
```

服务端参数默认写死（可用环境变量覆盖）：`PS_URL` / `RPC_URL` / `REST_URL` / `BOX_CODE` / `ORG_CODE`。

## 结构

```
examples/wailsapp/
├── main.go              # Wails app：窗口、事件注册、-selftest/-writes
├── service.go           # 暴露给前端的服务：Status/Tables/Columns/Rows/Insert/Update/Delete/Watch/Check/SyncNow
├── frontend/
│   ├── index.html
│   └── src/{main.js,style.css}
└── build/               # Wails 打包配置（模板自带）
```

## 依赖说明

- 这是一个**独立 Go module**（`go.mod` 里 `replace msmkSync => ../..`），
  所以 Wails 的依赖不会污染 SDK 根模块。
- 后端能力全部通过 `msmkSync` 公共 API（`powersync.Open` / `GetAll` / `Execute` / `WatchTables`）
  + 公共包 `msmkSync/restconn`（PostgREST 上传连接器）。

## 已知限制

- 同一个数据库文件只能被一个实例同步（见 `docs/reference.md` §3）：
  再开一个窗口会自动降级为**只读浏览**（不连接同步）。
- 65 张表里含同步规则里的短别名表（`i`/`t`/`l`/`r`/`s`/`h`/`d`）：它们是被 `AS i` 这类写法合并的实体，
  建议后端把别名去掉（那样每张实体表会各自出现）。
