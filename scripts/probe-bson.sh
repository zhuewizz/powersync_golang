#!/usr/bin/env bash
# 验证 BSON 流与 NDJSON 流解析结果完全一致（对照真实服务）。
#
# 做法：同一服务、同一 schema，跑两遍 cmd/psdiff：
#   - 一遍走 NDJSON（默认）
#   - 一遍走 BSON（-bson）
# 再用 cmd/pscompare 比对两份落库导出。BSON 解码有问题会立刻在这里暴露。
#
# 用法：
#   PS_URL=... RPC_URL=... BOX_CODE=D1-2 ORG_CODE=ORG001 ./scripts/probe-bson.sh
set -uo pipefail

: "${PS_URL:?需要 PS_URL}" "${RPC_URL:?需要 RPC_URL}" "${BOX_CODE:?}" "${ORG_CODE:?}"
SCHEMA="${SCHEMA:-./out/schema.json}"
OUT="${OUT:-./out/bson-diff}"
[ -f "$SCHEMA" ] || { echo "缺少 schema：$SCHEMA（先跑 node scripts/js-reference/dump-schema.mjs）" >&2; exit 1; }

mkdir -p "$OUT"
run() { # $1=子目录 $2...=额外参数
  local dir="$OUT/$1"; shift
  echo "=== psdiff -> $dir ==="
  go run ./cmd/psdiff -endpoint "$PS_URL" -rpc "$RPC_URL" -box "$BOX_CODE" -org "$ORG_CODE" \
    -schema "$SCHEMA" -db "$dir/ref.db" -out "$dir" -timeout 60s "$@" 2>&1 \
    | grep -E "第 [12] 次|落库结果|校验和：|OK  |MISMATCH" || true
}

run ndjson
run bson -bson

echo
echo "=== 比对 NDJSON vs BSON 的落库结果 ==="
go run ./cmd/pscompare -a "$OUT/ndjson/rows.jsonl" -b "$OUT/bson/rows.jsonl"
