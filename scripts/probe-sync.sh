#!/usr/bin/env bash
# 对真实 PowerSync（自建服务）做协议探针，输出原始 NDJSON 以便比对 testdata/sync/*.ndjson。
#
# 用法一（已有 token）：
#   PS_URL=http://host:8080 PS_TOKEN=<jwt> ./scripts/probe-sync.sh
#
# 用法二（从 PostgREST 风格 RPC 取 token，参考 e:/cutter_client）：
#   PS_URL=http://host:8080 \
#   RPC_URL=http://host:8089/rest/rpc/fetch_credentials \
#   BOX_CODE=D1-2 ORG_CODE=ORG001 ./scripts/probe-sync.sh
#
# 可选：
#   OUT_DIR=./.probe        输出目录（默认 .probe，已在 .gitignore 中）
#   HOLD_SECONDS=15         单次流保持时长
#   BUCKETS='[...]'         自定义 buckets JSON（默认 []，即由服务端返回默认 streams）
#
# 注意：输出包含真实业务数据，**不要提交到仓库**；需要提交前先脱敏并裁剪。
set -euo pipefail

: "${PS_URL:?需要 PS_URL，例如 http://127.0.0.1:8080}"
OUT_DIR="${OUT_DIR:-./.probe}"
HOLD_SECONDS="${HOLD_SECONDS:-15}"
mkdir -p "$OUT_DIR"

if [ -z "${PS_TOKEN:-}" ]; then
  : "${RPC_URL:?需要 PS_TOKEN，或提供 RPC_URL+BOX_CODE+ORG_CODE 来换取 token}"
  : "${BOX_CODE:?}" "${ORG_CODE:?}"
  PS_TOKEN=$(curl -fsS -m 20 -X POST "$RPC_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"$BOX_CODE\",\"org_code\":\"$ORG_CODE\"}" \
    | sed -E 's/.*"token"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')
  [ "${#PS_TOKEN}" -gt 50 ] || { echo "取 token 失败" >&2; exit 1; }
fi

BUCKETS="${BUCKETS:-[]}"
PS_URL="${PS_URL%/}"

stream() { # $1=名字 $2=body
  local out="$OUT_DIR/$1.ndjson"
  echo "--- $1 -> $out"
  curl -sS --max-time "$HOLD_SECONDS" -D "$OUT_DIR/$1.headers" \
    -X POST "$PS_URL/sync/stream" \
    -H "Authorization: Token $PS_TOKEN" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/x-ndjson' \
    -d "$2" -o "$out" -w 'http=%{http_code} bytes=%{size_download}\n' || true
  [ -s "$out" ] || { echo "  (空响应)"; return; }
  if command -v node >/dev/null 2>&1; then
    node -e '
      const fs=require("fs");
      const ls=fs.readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean);
      console.log("  lines:",ls.length);
      for(const l of ls){
        const o=JSON.parse(l), k=Object.keys(o)[0], v=o[k];
        if(k==="data") console.log("  data   bucket="+v.bucket+" after="+v.after+" has_more="+v.has_more+" ops="+v.data.length);
        else console.log("  "+k.padEnd(6)+" "+JSON.stringify(v).slice(0,160));
      }' "$out"
  else
    wc -l < "$out"
  fi
}

echo "=== 1. 冷启动全量（buckets 空） ==="
stream full "{\"buckets\":$BUCKETS,\"include_checksum\":true,\"raw_data\":true,\"app_metadata\":{},\"parameters\":{}}"

echo "=== 2. raw_data=false（data 为对象而非字符串） ==="
stream object "{\"buckets\":$BUCKETS,\"include_checksum\":true,\"raw_data\":false}"

echo "=== 3. 鉴权失败基线 ==="
curl -sS -m 10 -o "$OUT_DIR/error-401.json" -w 'http=%{http_code}\n' \
  -X POST "$PS_URL/sync/stream" -H 'Content-Type: application/json' \
  -d '{"buckets":[]}' || true
cat "$OUT_DIR/error-401.json" 2>/dev/null; echo

echo "=== 4. checkpoint-request（新式） ==="
CID="${CLIENT_ID:-11111111-2222-3333-4444-555555555555}"
curl -sS -m 10 -o "$OUT_DIR/checkpoint-request.json" -w 'http=%{http_code}\n' \
  -X POST "$PS_URL/sync/checkpoint-request" \
  -H "Authorization: Token $PS_TOKEN" -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d "{\"client_id\":\"$CID\",\"checkpoint_request_id\":\"1\"}" || true
cat "$OUT_DIR/checkpoint-request.json" 2>/dev/null; echo

echo "=== 5. legacy write-checkpoint2 ==="
curl -sS -m 10 -o "$OUT_DIR/write-checkpoint2.json" -w 'http=%{http_code}\n' \
  -H "Authorization: Token $PS_TOKEN" -H 'Accept: application/json' \
  "$PS_URL/write-checkpoint2.json?client_id=$CID" || true
cat "$OUT_DIR/write-checkpoint2.json" 2>/dev/null; echo

echo "=== 完成：原始输出在 $OUT_DIR（含真实数据，勿提交） ==="
