#!/usr/bin/env bash
# 用真实的 PUT/PATCH/DELETE 验证本地 oplog checksum 累计与服务端是否一致。
#
# 原理：cmd/psdiff 每次同步都会在收到 checkpoint 行时校验「本地 add+op」与该 checkpoint
# 声明的 bucket checksum。因此只要：
#   1) 基线同步 → 2) 经 REST 改一行 → 3) 再次同步应用变更 → 4) 再同步做校验
# 若第 4 步没有 "校验和不符/重置"，说明 supersede/REMOVE 的累计规则是对的。
#
# 用法：
#   PS_URL=... REST_URL=... RPC_URL=... BOX_CODE=D1-2 ORG_CODE=ORG001 ./scripts/probe-checksum.sh
set -uo pipefail
: "${PS_URL:?}" "${REST_URL:?}"
TABLE="${TABLE:-sys_error_report}"
OUT="${OUT:-./out}"
WAIT="${WAIT:-90}"       # 等复制延迟（实测 45–90s）
PSDIFF="${PSDIFF:-go run ./cmd/psdiff}"

if [ -z "${PS_TOKEN:-}" ]; then
  : "${RPC_URL:?需要 PS_TOKEN 或 RPC_URL+BOX_CODE+ORG_CODE}"
  PS_TOKEN=$(curl -fsS -m 20 -X POST "$RPC_URL" -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"${BOX_CODE:?}\",\"org_code\":\"${ORG_CODE:?}\"}" \
    | sed -E 's/.*"token"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')
fi
REST_URL="${REST_URL%/}"
ID=$(node -e 'console.log(crypto.randomUUID())')

sync() { # $1=label
  $PSDIFF -endpoint "$PS_URL" -token "$PS_TOKEN" -schema "$OUT/schema.json" \
    -db "$OUT/checksum.db" -keep-db -timeout "${SYNC_TIMEOUT:-25s}" -out "$OUT" 2>&1 \
    | tee "$OUT/checksum-$1.log" \
    | grep -E "第 [12] 次|校验和不符|MISMATCH|OK  " || true
}
mismatch() { grep -qE "校验和不符|MISMATCH" "$OUT/checksum-$1.log"; }

cleanup() {
  [ -n "${INSERTED:-}" ] && curl -sS -m 15 -o /dev/null -X DELETE \
    -H "Authorization: Bearer $PS_TOKEN" "$REST_URL/$TABLE?id=eq.$ID" && echo "清理：已删除测试行 $ID"
}
trap cleanup EXIT

row() { # $1=desc
  printf '{"id":"%s","org_code":"%s","box_code":"%s","box_cell":"CK","user_id":"CK","user_name":"checksum","error_date":"2026-01-01 00:00:00","error_desc":"%s","trans_type":"CK","op_status":"CK","box_type":"CK"}' \
    "$ID" "${ORG_CODE:-ORG001}" "${BOX_CODE:-D1-2}" "$1"
}

echo "=== 0. 基线同步（首次会全量，校验应 OK） ==="
sync base

echo "=== 1. INSERT 测试行 ==="
code=$(curl -sS -m 20 -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $PS_TOKEN" \
  -H 'Content-Type: application/json' -d "$(row first)" "$REST_URL/$TABLE")
echo "INSERT -> $code"; [ "$code" = "201" ] || { echo "插入失败，放弃" >&2; exit 2; }
INSERTED=1

echo "=== 2. UPDATE 该行（触发同 key 的 PUT supersede） ==="
sleep 30
code=$(curl -sS -m 20 -o /dev/null -w '%{http_code}' -X PATCH -H "Authorization: Bearer $PS_TOKEN" \
  -H 'Content-Type: application/json' -d '{"error_desc":"second"}' "$REST_URL/$TABLE?id=eq.$ID")
echo "PATCH -> $code"

echo "=== 3. DELETE 该行（触发 REMOVE） ==="
sleep 30
code=$(curl -sS -m 20 -o /dev/null -w '%{http_code}' -X DELETE -H "Authorization: Bearer $PS_TOKEN" \
  "$REST_URL/$TABLE?id=eq.$ID")
echo "DELETE -> $code"; [ "$code" = "204" ] && INSERTED=""

echo "=== 4. 等复制后应用变更（最多 ${WAIT}s 内定期同步） ==="
deadline=$(( $(date +%s) + WAIT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  sync apply
  if grep -q "op=0" "$OUT/checksum-apply.log"; then break; fi
  sleep 10
done

echo "=== 5. 再同步一次做校验（此处出现 MISMATCH 即累计规则错） ==="
sync verify
if mismatch verify; then
  echo "❌ 校验和不一致：本地 oplog 累计与服务端不符"
  grep -A3 "校验和不符" "$OUT/checksum-verify.log" | head -10
  exit 1
fi
echo "✅ 校验和一致（PUT supersede + REMOVE 的累计规则正确）"
