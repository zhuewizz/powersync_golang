#!/usr/bin/env node
/**
 * 差分测试参照端：用官方 @powersync/node SDK 同步真实服务端，把落库结果导出成
 * schema.json + rows.jsonl，供 Go 端（cmd/psdiff）比对。
 *
 * 用法：
 *   PS_URL=http://host:8080 PS_TOKEN=<jwt> node sync.mjs            # 已有 token
 *   PS_URL=... RPC_URL=.../rest/rpc/fetch_credentials BOX_CODE=<box_code> ORG_CODE=<org_code> node sync.mjs
 *
 * 可选：
 *   APP_SCHEMA=/e/cutter_client/src/library/powersync/AppSchema.ts   参照 schema 源文件
 *   OUT=./out                                                       输出目录
 *   DBFILE=./out/reference.db                                       参照端 SQLite 文件
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(process.env.OUT ?? path.join(here, 'out'));
const DBFILE = path.resolve(process.env.DBFILE ?? path.join(OUT, 'reference.db'));
const APP_SCHEMA = process.env.APP_SCHEMA ?? 'E:/cutter_client/src/library/powersync/AppSchema.ts';

fs.mkdirSync(OUT, { recursive: true });
for (const f of [DBFILE, `${DBFILE}-wal`, `${DBFILE}-shm`]) fs.rmSync(f, { force: true });

async function getToken() {
  if (process.env.PS_TOKEN) return process.env.PS_TOKEN;
  const rpc = process.env.RPC_URL;
  const box = process.env.BOX_CODE;
  const org = process.env.ORG_CODE;
  if (!rpc || !box || !org) {
    throw new Error('需要 PS_TOKEN，或 RPC_URL+BOX_CODE+ORG_CODE');
  }
  const res = await fetch(rpc, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ box_code: box, org_code: org }),
  });
  if (!res.ok) throw new Error(`fetch_credentials HTTP ${res.status}`);
  const { token } = await res.json();
  if (!token) throw new Error('fetch_credentials 未返回 token');
  return token;
}

/**
 * 复用 e:/cutter_client 的 AppSchema.ts：只把 `@powersync/web` 换成 `@powersync/node`，
 * 生成到 OUT 下再由 Node 原生 TS 支持加载，避免 schema 两份漂移。
 */
/**
 * 从 PowerSync 规范 schema JSON 构造 JS Schema（CI 里用同一份 testdata/schema/*.json
 * 喂给 Go 与 JS，保证两边表结构完全一致）。 */
function schemaFromJson(mod, json) {
  const { Schema, Table, column } = mod;
  const tables = {};
  for (const t of json.tables) {
    const cols = {};
    for (const c of t.columns) {
      const type = String(c.type ?? 'TEXT').toUpperCase();
      cols[c.name] = type === 'INTEGER' ? column.integer : type === 'REAL' ? column.real : column.text;
    }
    const options = {
      localOnly: !!t.local_only,
      insertOnly: !!t.insert_only,
      indexes: Object.fromEntries((t.indexes ?? []).map((i) => [i.name, i.columns.map((c) => c.name)])),
    };
    tables[t.name] = new Table(cols, options);
  }
  return new Schema(tables);
}

async function loadSchemaFromAppSchema() {
  const src = fs.readFileSync(APP_SCHEMA, 'utf8');
  const rewritten = src.replace(/from\s+['"]@powersync\/web['"]/g, `from '@powersync/node'`);
  if (rewritten === src) throw new Error(`AppSchema 中未找到 @powersync/web 导入：${APP_SCHEMA}`);
  // gen 必须放在含 node_modules 的目录，否则解析不到 @powersync/node。
  const gen = path.join(here, 'out', 'AppSchema.gen.ts');
  fs.mkdirSync(path.dirname(gen), { recursive: true });
  fs.writeFileSync(gen, rewritten);
  const loaded = await import(pathToFileURL(gen).href);
  if (!loaded.AppSchema) throw new Error('AppSchema.gen.ts 未导出 AppSchema');
  return loaded.AppSchema;
}

const token = await getToken();
const endpoint = (process.env.PS_URL ?? '').replace(/\/$/, '');
if (!endpoint) throw new Error('需要 PS_URL');

const nodeSDK = await import('@powersync/node');
const { PowerSyncDatabase } = nodeSDK;
let AppSchema;
if (process.env.SCHEMA_JSON) {
  const json = JSON.parse(fs.readFileSync(process.env.SCHEMA_JSON, 'utf8'));
  AppSchema = schemaFromJson(nodeSDK, json);
  console.log('schema 来自 SCHEMA_JSON:', process.env.SCHEMA_JSON, '表数', json.tables.length);
} else {
  AppSchema = await loadSchemaFromAppSchema();
}

const db = new PowerSyncDatabase({ schema: AppSchema, database: { dbFilename: DBFILE } });
await db.init();
// 只下载、不实现上传（差分测试不写服务端）。用 legacy connector 形状兼容各版本 SDK。
await db.connect({
  fetchCredentials: async () => ({ endpoint, token }),
  uploadData: async () => {},
});

const t0 = Date.now();
await db.waitForFirstSync();
const syncMs = Date.now() - t0;
const status = db.currentStatus;
await db.disconnect();

const tables = (
  await db.getAll(
    // JSON 表在 SDK 里是**视图**（ps_data__* + 视图），所以 table 和 view 都要导。
    // 用 substr 过滤前缀，避免 SQL 的 ESCAPE 在模板字符串里转义。
    `SELECT name FROM sqlite_master
      WHERE type IN ('table','view')
        AND substr(name,1,3) <> 'ps_'
        AND substr(name,1,7) <> 'sqlite_'
      ORDER BY name`,
  )
).map((r) => r.name);

const schema = { tables: [] };
const rows = [];
for (const table of tables) {
  const cols = (await db.getAll(`PRAGMA table_info(${quoteIdent(table)})`)).map((c) => ({
    name: c.name,
    type: String(c.type ?? '').toLowerCase(),
    notnull: !!c.notnull,
    pk: !!c.pk,
  }));
  schema.tables.push({ name: table, columns: cols });

  const data = await db.getAll(`SELECT * FROM ${quoteIdent(table)}`);
  data.sort((a, b) => String(a.id ?? '').localeCompare(String(b.id ?? '')));
  for (const row of data) rows.push({ table, row });
}

fs.writeFileSync(path.join(OUT, 'schema.json'), JSON.stringify(schema, null, 1));
fs.writeFileSync(
  path.join(OUT, 'rows.jsonl'),
  rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : ''),
);

const byTable = {};
for (const r of rows) byTable[r.table] = (byTable[r.table] ?? 0) + 1;
console.log(
  JSON.stringify(
    {
      ok: true,
      endpoint,
      syncMs,
      connected: status?.connected,
      lastSyncedAt: status?.lastSyncedAt ?? null,
      tables: tables.length,
      rows: rows.length,
      byTable,
      out: OUT,
    },
    null,
    1,
  ),
);
await db.close();

function quoteIdent(name) {
  return '"' + String(name).replaceAll('"', '""') + '"';
}
