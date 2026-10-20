// SPDX-License-Identifier: MIT
// Runs against the replicated pair scripts/run_with_servers.sh starts: a 2-shard primary on
// RUVIO_REPLICATED_ADDR and its 2-shard replica on RUVIO_REPLICA_ADDR (shard i on port + i).
// Skipped without them.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClusterClient, RuvioClient, hashSlot, type ReadPolicy } from '../src/index.js';
import { sleep } from './helpers.js';

const PRIMARY = process.env.RUVIO_REPLICATED_ADDR;
const REPLICA = process.env.RUVIO_REPLICA_ADDR;
const SKIP = PRIMARY && REPLICA ? false : 'RUVIO_REPLICATED_ADDR / RUVIO_REPLICA_ADDR not set';

function split(address: string): { host: string; port: number } {
  const i = address.lastIndexOf(':');
  return { host: address.slice(0, i), port: Number(address.slice(i + 1)) };
}

/** One shard of the replica, without cluster routing. */
function shard(key: string): RuvioClient {
  const { host, port } = split(REPLICA!);
  return new RuvioClient({ host, port: port + (hashSlot(key) >= 8192 ? 1 : 0) });
}

async function getCommands(c: RuvioClient): Promise<number> {
  const info = await c.info();
  const line = info.split(/\r?\n/).find((l) => l.startsWith('get_commands:'));
  assert.ok(line, 'INFO reports get_commands');
  return Number(line.slice('get_commands:'.length));
}

async function waitForCopy(c: RuvioClient, key: string, value: string): Promise<void> {
  const end = Date.now() + 10_000;
  while ((await c.get(key)) !== value) {
    assert.ok(Date.now() < end, 'the replica did not copy ' + key);
    await sleep(20);
  }
}

/** Writes `key` through a client with `policy`, then counts the GETs the replica shard serves for one read. */
async function replicaGets(policy: ReadPolicy, key: string, value: string): Promise<number> {
  const db = new ClusterClient({ ...split(PRIMARY!), readPolicy: policy });
  const onReplica = shard(key);
  try {
    await db.set(key, value);
    await waitForCopy(onReplica, key, value);
    const before = await getCommands(onReplica);
    assert.equal(await db.get(key), value);
    return (await getCommands(onReplica)) - before;
  } finally {
    db.disconnect();
    onReplica.disconnect();
  }
}

test('live replica reads: prefer-replica reads from the replica', { skip: SKIP }, async () => {
  assert.equal(await replicaGets('prefer-replica', 'node:replica-reads:prefer', 'v1'), 1);
});

test('live replica reads: replica policy reads from the replica', { skip: SKIP }, async () => {
  assert.equal(await replicaGets('replica', 'node:replica-reads:only', 'v2'), 1);
});

test('live replica reads: primary policy keeps reads on the primary', { skip: SKIP }, async () => {
  assert.equal(await replicaGets('primary', 'node:replica-reads:primary', 'v3'), 0);
});

test('durability option: a write is acknowledged once a replica has it on disk', { skip: SKIP }, async () => {
  const c = new RuvioClient({ ...split(PRIMARY!), durability: { replicas: 1, timeoutMs: 5000 } });
  try {
    const key = 'durable:' + Date.now();
    assert.equal(await c.set(key, 'v'), 'OK');
    assert.equal(await c.get(key), 'v');
    assert.equal(await c.del(key), 1);
  } finally {
    c.disconnect();
  }
});
