#!/usr/bin/env node
/**
 * 从 e:/cutter_client 的 AppSchema.ts 导出 PowerSync 规范 schema JSON
 * （`Schema.toJSON()` 的形状），供 cmd/psdiff 建真实表用。
 *
 * 用法：APP_SCHEMA=/e/cutter_client/src/library/powersync/AppSchema.ts \
 *        OUT=./out node scripts/js-reference/dump-schema.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(process.env.OUT ?? path.join(here, 'out'));
const APP_SCHEMA = process.env.APP_SCHEMA ?? 'E:/cutter_client/src/library/powersync/AppSchema.ts';

fs.mkdirSync(OUT, { recursive: true });

// 复用 AppSchema.ts，只把 @powersync/web 换成 @powersync/node（类型定义同源）。
// 生成文件必须放在含 node_modules 的目录下，否则解析不到 @powersync/node。
const gen = path.join(here, 'out', 'AppSchema.gen.ts');
fs.mkdirSync(path.dirname(gen), { recursive: true });
fs.writeFileSync(
  gen,
  fs.readFileSync(APP_SCHEMA, 'utf8').replace(/from\s+['"]@powersync\/web['"]/g, `from '@powersync/node'`),
);
const { AppSchema } = await import(pathToFileURL(gen).href);
const json = typeof AppSchema.toJSON === 'function' ? AppSchema.toJSON() : AppSchema;
const file = path.join(OUT, 'schema.json');
fs.writeFileSync(file, JSON.stringify(json, null, 1));
console.log(
  JSON.stringify({ ok: true, tables: json.tables.length, columns: json.tables.reduce((n, t) => n + t.columns.length, 0), file }),
);
