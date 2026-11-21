#!/usr/bin/env bash
# 双实现差分测试：离线假服务 + 官方 JS SDK + Go 实现，逐行比对落库结果。
#
# 需要：Go 1.25+、Node 22+（`scripts/js-reference` 里要能装好 @powersync/node + better-sqlite3）。
# 在 Linux/macOS/WSL 上可跑；Windows 原生因 better-sqlite3 编译问题通常不适用（CI 用 ubuntu）。
#
# 用法：
#   ./scripts/run-js-diff.sh                 # 默认 fixture: testdata/sync/apply-todos.ndjson
#   FIXTURE=... SCHEMA=... ./scripts/run-js-diff.sh
set -euo pipefail

ADDR="${ADDR:-127.0.0.1:18080}"
OUT="${OUT:-./out/jsdiff}"
FIXTURE="${FIXTURE:-testdata/sync/apply-todos.ndjson}"
SCHEMA="${SCHEMA:-testdata/schema/todos.json}"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

mkdir -p "$OUT/go" "$OUT/js" "$OUT/bin"

echo "=== 0. 构建假服务 & JS 依赖检查 ==="
if ! command -v node >/dev/null 2>&1; then
  echo "需要 Node 22.15+（undici 7 依赖 zlib zstd）。" >&2
  echo "没有 Node 或版本过低时，可用容器方式： ./scripts/js-diff-container.sh" >&2
  exit 1
fi
NODE_MAJOR=$(node -p 'process.versions.node.split(".")[0]')
if [ "$NODE_MAJOR" -lt 22 ]; then
  echo "当前 Node $(node -v) 太旧：@powersync/node 的 undici 需要 Node >= 22.15。" >&2
  echo "可用容器方式： ./scripts/js-diff-container.sh" >&2
  exit 1
fi
go build -o "$OUT/bin/psfixture-server" ./cmd/psfixture-server
if [ ! -d scripts/js-reference/node_modules/@powersync/node ]; then
  echo "缺少 scripts/js-reference 依赖，先执行： (cd scripts/js-reference && npm ci)" >&2
  exit 1
fi
if ! (cd scripts/js-reference && node -e "require('better-sqlite3')" >/dev/null 2>&1); then
  echo "当前环境加载不了 better-sqlite3（@powersync/node 必需）。" >&2
  echo "Windows 原生通常装不上；请在 Linux/macOS/WSL 或 CI 里跑本脚本。" >&2
  exit 1
fi

echo "=== 1. 启动假服务（$FIXTURE） ==="
"$OUT/bin/psfixture-server" -addr "$ADDR" -fixture "$FIXTURE" > "$OUT/fixture-server.log" 2>&1 &
SRV_PID=$!
cleanup() {
  kill "$SRV_PID" 2>/dev/null || true
  wait "$SRV_PID" 2>/dev/null || true
}
trap cleanup EXIT
for _ in $(seq 1 100); do
  if curl -fsS "http://$ADDR/healthz" > /dev/null 2>&1; then break; fi
  sleep 0.1
done

echo "=== 2. Go 实现落库 ==="
go run ./cmd/psdiff \
  -endpoint "http://$ADDR" -token fixture \
  -schema "$SCHEMA" -db "$OUT/go/ref.db" -out "$OUT/go" -timeout 30s

echo "=== 3. 官方 JS SDK 落库 ==="
(
  cd scripts/js-reference
  PS_URL="http://$ADDR" PS_TOKEN=fixture SCHEMA_JSON="../../$SCHEMA" \
    OUT="$(cd ../.. && pwd)/$OUT/js" DBFILE="$(cd ../.. && pwd)/$OUT/js/reference.db" \
    node sync.mjs
)

echo "=== 4. 逐行比对 ==="
go run ./cmd/pscompare -a "$OUT/js/rows.jsonl" -b "$OUT/go/rows.jsonl"
