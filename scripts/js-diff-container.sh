#!/usr/bin/env bash
# 在没有 Docker Desktop 的机器上用 kiri（WSL2 内）跑双实现差分。
#
# 背景：`@powersync/node` 需要 glibc 的 Node ≥ 22.15（undici 7 用 zlib zstd）
# 和 glibc 版的 `libpowersync_*.so`，所以不能用 Alpine。
# 这里用一个已缓存的 glibc 镜像（默认 postgres:18）+ 官方 Node tarball，
# 全部在容器内完成：假服务 → Go 落库 → 官方 SDK 落库 → 逐行比对。
#
# 用法：
#   ./scripts/js-diff-container.sh
#   IMAGE=debian:bookworm ./scripts/js-diff-container.sh
set -euo pipefail

IMAGE="${IMAGE:-postgres:18}"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$ROOT/out/jsdiff/bin"

echo "=== 构建 linux 静态二进制（CGO_ENABLED=0） ==="
(cd "$ROOT" && CGO_ENABLED=0 GOOS=linux go build -o out/jsdiff/bin/psfixture-server ./cmd/psfixture-server \
  && CGO_ENABLED=0 GOOS=linux go build -o out/jsdiff/bin/psdiff ./cmd/psdiff \
  && CGO_ENABLED=0 GOOS=linux go build -o out/jsdiff/bin/pscompare ./cmd/pscompare)

cat > "$ROOT/out/jsdiff/in-container.sh" <<'INNER'
set -e
export DEBIAN_FRONTEND=noninteractive
command -v curl >/dev/null 2>&1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq curl ca-certificates xz-utils >/dev/null 2>&1; }
if [ ! -x /opt/node/bin/node ]; then
  NODE_V=$(curl -fsSL https://nodejs.org/dist/index.json | tr ',' '\n' | grep -o '"version":"v22\.[0-9.]*"' | head -1 | cut -d'"' -f4)
  echo "安装 Node $NODE_V（需要 >= 22.15 才有 zstd）"
  mkdir -p /opt/node
  curl -fsSL -o /tmp/node.tar.xz "https://nodejs.org/dist/$NODE_V/node-$NODE_V-linux-x64.tar.xz"
  tar -xJf /tmp/node.tar.xz -C /opt/node --strip-components=1
fi
export PATH=/opt/node/bin:$PATH
echo "node $(node --version) / npm $(npm --version)"

cd /w/scripts/js-reference
if [ ! -d node_modules/@powersync/node ]; then
  apt-get install -y -qq build-essential python3 >/dev/null 2>&1
  npm ci --no-audit --no-fund 2>&1 | tail -3
fi
node -e 'require("better-sqlite3"); console.log("better-sqlite3 OK")'

mkdir -p /w/out/jsdiff/go /w/out/jsdiff/js
/w/out/jsdiff/bin/psfixture-server -addr 127.0.0.1:18080 -fixture /w/testdata/sync/apply-todos.ndjson > /w/out/jsdiff/fixture.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for _ in 1 2 3 4 5 6 7 8 9 10; do curl -fsS -m 1 http://127.0.0.1:18080/healthz >/dev/null 2>&1 && break; sleep 0.3; done

echo "=== Go 实现 ==="
/w/out/jsdiff/bin/psdiff -endpoint http://127.0.0.1:18080 -token fixture \
  -schema /w/testdata/schema/todos.json -db /w/out/jsdiff/go/ref.db -out /w/out/jsdiff/go -timeout 20s \
  | grep -E "第 [12] 次|落库结果" || true

echo "=== 官方 JS SDK ==="
PS_URL=http://127.0.0.1:18080 PS_TOKEN=fixture \
SCHEMA_JSON=/w/testdata/schema/todos.json \
OUT=/w/out/jsdiff/js DBFILE=/w/out/jsdiff/js/reference.db \
node sync.mjs 2>&1 | tail -15

echo "=== 逐行比对 ==="
/w/out/jsdiff/bin/pscompare -a /w/out/jsdiff/js/rows.jsonl -b /w/out/jsdiff/go/rows.jsonl
INNER

# WSL 里路径是 /mnt/e/...；kiri 与 docker CLI 兼容
WSL_PATH=$(cd "$ROOT" && wslpath -a . 2>/dev/null || echo "$ROOT")
wsl.exe -e sh -lc "kiri run --rm -v '$WSL_PATH:/w' -w /w $IMAGE bash /w/out/jsdiff/in-container.sh"
