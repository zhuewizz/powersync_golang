#!/usr/bin/env node
/**
 * 探测下载侧的 REMOVE op 与 checkpoint_diff 行。
 *
 * 做法（只动一张无关表、只删自己插入的那一行）：
 *   1. 读 cmd/psdiff 导出的 out/state.json（每个 bucket 已应用到的 op_id）；
 *   2. 用**全部** bucket + 当前 after 同步一次，确认零增量（顺带验证 after 续传）；
 *   3. 通过 PostgREST 往一张表插一行测试数据（默认 sys_error_report）；
 *   4. 同步 → 期望看到该 id 的 PUT；
 *   5. 删除该行 → 同步 → 期望看到 REMOVE（以及是否出现 checkpoint_diff）；
 *   6. 无论成败都清理（除非 KEEP=1）。
 *
 * 用法：
 *   PS_URL=http://host:8080 REST_URL=http://host:8089/rest \
 *   RPC_URL=http://host:8089/rest/rpc/fetch_credentials BOX_CODE=D1-2 ORG_CODE=ORG001 \
 *     node scripts/probe-remove.mjs
 *
 * 可选环境变量：STATE（默认 ./out/state.json）、TABLE（默认 sys_error_report）、
 *              WAIT（每步最长秒数，默认 15）、KEEP=1、OUT（保存原始 NDJSON 的目录）
 */
import fs from 'node:fs';
import path from 'node:path';

const PS_URL = (process.env.PS_URL ?? '').replace(/\/$/, '');
const REST_URL = (process.env.REST_URL ?? '').replace(/\/$/, '');
const STATE_FILE = process.env.STATE ?? './out/state.json';
const TABLE = process.env.TABLE ?? 'sys_error_report';
const WAIT_MS = Number(process.env.WAIT ?? 45) * 1000;
// 该服务端实例的复制延迟实测可达 30s+，所以用轮询而不是单次等待。
const DEADLINE_MS = Number(process.env.DEADLINE ?? 180) * 1000;
const KEEP = process.env.KEEP === '1';
const OUT = process.env.OUT ?? './out/probe-remove';

if (!PS_URL || !REST_URL) throw new Error('需要 PS_URL 与 REST_URL');
if (!fs.existsSync(STATE_FILE)) {
  throw new Error(`缺少 ${STATE_FILE}：先跑 go run ./cmd/psdiff -out ./out ... 生成增量游标`);
}

fs.mkdirSync(OUT, { recursive: true });
const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
const buckets = { ...state.buckets };

async function getToken() {
  if (process.env.PS_TOKEN) return process.env.PS_TOKEN;
  const { RPC_URL, BOX_CODE, ORG_CODE } = process.env;
  if (!RPC_URL || !BOX_CODE || !ORG_CODE) throw new Error('需要 PS_TOKEN，或 RPC_URL+BOX_CODE+ORG_CODE');
  const res = await fetch(RPC_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ box_code: BOX_CODE, org_code: ORG_CODE }),
  });
  if (!res.ok) throw new Error(`fetch_credentials HTTP ${res.status}`);
  const { token } = await res.json();
  if (!token) throw new Error('fetch_credentials 未返回 token');
  return token;
}

const token = await getToken();

/** 用当前 buckets 同步一次，期间把游标推进到实际应用的最后一个 op_id。 */
async function sync(label) {
  const body = JSON.stringify({
    buckets: Object.entries(buckets).map(([name, after]) => ({ name, after })),
    include_checksum: true,
    raw_data: true,
  });
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), WAIT_MS);
  const lines = [];
  let buf = '';
  try {
    const res = await fetch(`${PS_URL}/sync/stream`, {
      method: 'POST',
      signal: ac.signal,
      headers: {
        Authorization: `Token ${token}`,
        'Content-Type': 'application/json',
        Accept: 'application/x-ndjson',
        'X-User-Agent': 'powersync-go/probe',
      },
      body,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const raw = buf.slice(0, i);
        buf = buf.slice(i + 1);
        if (!raw.trim()) continue;
        const line = JSON.parse(raw);
        lines.push(line);
        const kind = Object.keys(line)[0];
        if (kind === 'data') {
          // 注意：data 行的 after 只是本批的左开下界，游标要取本批最后一条 op 的 op_id。
          const ops = line.data.data;
          if (ops.length) buckets[line.data.bucket] = ops[ops.length - 1].op_id;
        }
        if (kind === 'checkpoint_complete') ac.abort();
      }
    }
  } catch (e) {
    if (!ac.signal.aborted) throw e;
  } finally {
    clearTimeout(timer);
  }
  fs.writeFileSync(
    path.join(OUT, `${label}.ndjson`),
    lines.map((l) => JSON.stringify(l)).join('\n') + (lines.length ? '\n' : ''),
  );
  return lines;
}

function summarize(lines) {
  const counts = {};
  let ops = 0;
  let checkpointDiff = null;
  let fullResend = 0;
  const touched = new Map();
  for (const line of lines) {
    const kind = Object.keys(line)[0];
    counts[kind] = (counts[kind] ?? 0) + 1;
    if (kind === 'checkpoint_diff') checkpointDiff = line.checkpoint_diff;
    if (kind !== 'data') continue;
    if (String(line.data.after) === '0') fullResend++;
    for (const op of line.data.data) {
      ops++;
      touched.set(op.object_id ?? op.id, { op: op.op, table: op.object_type ?? op.type, op_id: op.op_id });
    }
  }
  return { counts, ops, checkpointDiff, fullResend, touched };
}

/** 反复同步直到命中条件或超时；返回最后一次汇总 + 累计命中的 op。 */
async function waitFor(label, predicate) {
  const deadline = Date.now() + DEADLINE_MS;
  const touchedAll = new Map();
  let last = { counts: {}, ops: 0, checkpointDiff: null, fullResend: 0, touched: touchedAll };
  let round = 0;
  while (Date.now() < deadline) {
    round++;
    const lines = await sync(label);
    last = summarize(lines);
    for (const [id, v] of last.touched) touchedAll.set(id, v);
    last.touched = touchedAll;
    if (last.checkpointDiff) last.checkpointDiff = last.checkpointDiff;
    const hit = touchedAll.get(rowId);
    console.log(
      `  轮次 ${round}: +ops=${last.ops} data行=${last.counts.data ?? 0} 从0重发=${last.fullResend} 当前命中=${JSON.stringify(hit ?? null)}`,
    );
    if (hit && predicate(touchedAll)) return last;
  }
  return last;
}

const rowId = crypto.randomUUID();
const body = JSON.stringify({
  id: rowId,
  org_code: process.env.ORG_CODE ?? 'ORG001',
  box_code: process.env.BOX_CODE ?? 'D1-2',
  box_cell: 'PROBE',
  user_id: 'PROBE',
  user_name: 'probe-remove',
  error_date: '2026-01-01 00:00:00',
  error_desc: 'psdiff probe row (safe to delete)',
  trans_type: 'PROBE',
  op_status: 'PROBE',
  box_type: 'PROBE',
});

let inserted = false;
const rest = (init = {}, qs = '') =>
  fetch(`${REST_URL}/${TABLE}${qs}`, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', ...(init.headers ?? {}) },
  });

async function cleanup() {
  if (!inserted || KEEP) return;
  const del = await rest({ method: 'DELETE' }, `?id=eq.${rowId}`);
  const left = await rest({}, `?id=eq.${rowId}&select=id`).then((r) => r.json());
  console.log(`清理：DELETE -> HTTP ${del.status}，REST 剩余行数 ${Array.isArray(left) ? left.length : '?'}`);
}

try {
  console.log(`表=${TABLE} 测试行 id=${rowId}`);
  console.log(`bucket 游标：${JSON.stringify(buckets)}`);

  console.log('\n=== 0. 基线（应零增量） ===');
  const base = summarize(await sync('base'));
  console.log(
    `  ops=${base.ops} data行=${base.counts.data ?? 0} 从0重发=${base.fullResend} checkpoint_diff=${base.checkpointDiff ? '有' : '无'}`,
  );

  console.log('\n=== 1. 插入测试行 ===');
  const ins = await rest({ method: 'POST', body, headers: { Prefer: 'return=representation' } });
  const insBody = await ins.text();
  console.log(`  INSERT -> HTTP ${ins.status} ${ins.status >= 300 ? insBody.slice(0, 200) : ''}`);
  if (ins.status >= 300) throw new Error('插入失败，未写入任何数据');
  inserted = true;

  console.log(`\n=== 2. 等待 PUT（轮询，最多 ${DEADLINE_MS / 1000}s） ===`);
  const put = await waitFor('put', (t) => (t.get(rowId)?.op ?? '') === 'PUT');
  const seenPut = put.touched.get(rowId);

  console.log('\n=== 3. 删除测试行 ===');
  const del = await rest({ method: 'DELETE' }, `?id=eq.${rowId}`);
  console.log(`  DELETE -> HTTP ${del.status}`);
  inserted = false;

  console.log(`\n=== 4. 等待 REMOVE（轮询，最多 ${DEADLINE_MS / 1000}s） ===`);
  const rm = await waitFor('remove', (t) => (t.get(rowId)?.op ?? '') === 'REMOVE');
  const seenRemove = rm.touched.get(rowId);

  console.log('\n=== 汇总 ===');
  console.log(`  PUT 收到: ${seenPut ? '是' : '否'}`);
  console.log(`  REMOVE 收到: ${seenRemove?.op === 'REMOVE' ? '是' : '否'}`);
  console.log(`  checkpoint_diff 出现: ${base.checkpointDiff || put.checkpointDiff || rm.checkpointDiff ? '是' : '否'}`);
  console.log(`  原始 NDJSON: ${OUT}/`);
  if (!seenPut) console.log('  提示：未收到 PUT —— 该表可能不在同步规则内，换 TABLE= 再试');
} finally {
  await cleanup();
}
