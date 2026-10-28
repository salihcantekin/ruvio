// SPDX-License-Identifier: MIT
// Coverage: every command in docs/compatibility-matrix.json has a typed method, and every
// typed method is called by at least one live test.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { RuvioClient, Subscriber } from '../src/index.js';
import { TransactionImpl } from '../src/pipeline.js';

const here = dirname(fileURLToPath(import.meta.url));
// build/test -> clients/node -> repo root
const nodeRoot = join(here, '..', '..');
const repoRoot = join(nodeRoot, '..', '..');

type Owner = 'client' | 'subscriber' | 'transaction';

/** Owner prefix in the "node" field of command_methods.json; no prefix means the client. */
const PREFIXES: Record<string, Owner> = { Subscriber: 'subscriber', Transaction: 'transaction' };

/** Matrix command -> [owner, typed method], read from clients/testdata/command_methods.json. */
function loadTable(): [string, Owner, string][] {
  const file = JSON.parse(readFileSync(join(nodeRoot, '..', 'testdata', 'command_methods.json'), 'utf8')) as {
    commands: Record<string, { node?: string | string[] }>;
  };
  const rows: [string, Owner, string][] = [];
  for (const [name, entry] of Object.entries(file.commands)) {
    const specs = entry.node === undefined ? [] : Array.isArray(entry.node) ? entry.node : [entry.node];
    for (const spec of specs) {
      const dot = spec.indexOf('.');
      const owner = dot < 0 ? 'client' : PREFIXES[spec.slice(0, dot)];
      assert.ok(owner !== undefined, `${name}: unknown owner in ${spec}`);
      rows.push([name, owner, spec.slice(dot + 1)]);
    }
  }
  return rows;
}

const TABLE = loadTable();

/** Option forms that change what Ruvio does, with their dedicated methods. */
const OPTIONS: [string, string][] = [
  ['SET ... IFEQ', 'set'],
  ['SET ... IFGT/IFLT', 'set'],
  ['INCRBY ... MAX', 'incrbyMax'],
  ['LIMIT ... SLIDING', 'limitSliding'],
  ['LIMIT ... TOKENS', 'limitTokens'],
  ['LIMIT ... SLIDING WITHRETRY', 'limitSlidingWithRetry'],
  ['LIMIT ... TOKENS WITHRETRY', 'limitTokensWithRetry'],
  ['FILLGET ... WAIT', 'fillget'],
  ['FILLSET ... SWR', 'fillset'],
  ['GETEX key ms', 'getex'],
  ['DELEX key IFEQ v', 'delex'],
  ['IDEM COMPLETE', 'idemComplete'],
  ['IDEM ABORT', 'idemAbort'],
  ['JSON.INDEX DROP', 'jsonIndexDrop'],
  ['JSON.INDEX LIST', 'jsonIndexList'],
  ['CHANGES START', 'changesStart'],
  ['XADD DELAY', 'xadd'],
  ['XPENDING extended', 'xpendingRange'],
  ['ZADD INCR', 'zaddIncr'],
];

const protos: Record<Owner, object> = {
  client: RuvioClient.prototype,
  subscriber: Subscriber.prototype,
  transaction: TransactionImpl.prototype,
};

function has(owner: Owner, method: string): boolean {
  let p: object | null = protos[owner];
  while (p !== null) {
    const d = Object.getOwnPropertyDescriptor(p, method);
    if (d !== undefined && typeof d.value === 'function') return true;
    p = Object.getPrototypeOf(p);
  }
  return false;
}

test('every matrix command has a typed method', () => {
  const matrix = JSON.parse(readFileSync(join(repoRoot, 'docs', 'compatibility-matrix.json'), 'utf8')) as {
    commands: { name: string }[];
  };
  const missing: string[] = [];
  let covered = 0;
  for (const { name } of matrix.commands) {
    const rows = TABLE.filter(([command]) => command === name);
    if (rows.length === 0) missing.push(name + ' (no command_methods.json entry)');
    const absent = rows.filter(([, owner, method]) => !has(owner, method));
    for (const [, owner, method] of absent) missing.push(`${name} -> ${owner}.${method}`);
    if (rows.length > 0 && absent.length === 0) covered++;
  }
  assert.deepEqual(missing, [], 'commands without a typed method');
  const names = new Set(matrix.commands.map(({ name }) => name));
  const stale = [...new Set(TABLE.map(([name]) => name))].filter((name) => !names.has(name));
  assert.deepEqual(stale, [], 'command_methods.json entries not in the matrix');
  for (const [form, method] of OPTIONS) assert.ok(has('client', method), form);
  console.log(`coverage: ${covered}/${matrix.commands.length} matrix commands have typed methods`);
});

test('every typed method is called by a live test', () => {
  const dir = join(nodeRoot, 'test');
  const src = readdirSync(dir)
    .filter((f) => f.startsWith('live-') && f.endsWith('.ts'))
    .map((f) => readFileSync(join(dir, f), 'utf8'))
    .join('\n');
  const unused = [...TABLE.map(([, , m]) => m), ...OPTIONS.map(([, m]) => m)].filter(
    (m) => !new RegExp(`\\.${m}\\(`).test(src),
  );
  assert.deepEqual([...new Set(unused)], [], 'typed methods never exercised against the live server');
});
