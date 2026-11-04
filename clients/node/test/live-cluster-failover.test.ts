// SPDX-License-Identifier: MIT
// A cluster client leaves a demoted primary after its READONLY reply, on real servers.
// scripts/run_with_servers.sh starts two 2-shard nodes that can each be primary or replica
// (RUVIO_CLUSTER_FAILOVER_ADDRS, shard 0 of each; their replication addresses in
// RUVIO_CLUSTER_FAILOVER_REPLICATION; both comma separated, primary first). No coordinator runs: the
// test swaps the roles itself with REPLICAOF, as a coordinator does. Skipped without the variables.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioClient, RuvioError, createClusterClient } from '../src/index.js';
import { until } from './helpers.js';

interface Addr {
  host: string;
  port: number;
}

function split(name: string): Addr[] | null {
  const value = process.env[name];
  if (!value) return null;
  const out = value.split(',').map((part) => {
    const i = part.lastIndexOf(':');
    return { host: part.slice(0, i), port: Number(part.slice(i + 1)) };
  });
  return out.length === 2 && out.every((a) => a.host !== '' && a.port > 0) ? out : null;
}

const CLIENTS = split('RUVIO_CLUSTER_FAILOVER_ADDRS');
const REPLICATION = split('RUVIO_CLUSTER_FAILOVER_REPLICATION');
const SKIP = CLIENTS && REPLICATION ? false : 'RUVIO_CLUSTER_FAILOVER_ADDRS / RUVIO_CLUSTER_FAILOVER_REPLICATION not set';

function direct(address: Addr): RuvioClient {
  return new RuvioClient({ ...address, followPrimary: false, commandTimeoutMs: 5000, connectTimeoutMs: 2000 });
}

async function isMaster(address: Addr): Promise<boolean> {
  const c = direct(address);
  try {
    const [role] = await c.role();
    return role === 'master';
  } finally {
    c.disconnect();
  }
}

test('live cluster failover: a cluster client reloads its slots after READONLY from a demoted primary', { skip: SKIP }, async () => {
  const pair = CLIENTS!;
  const replication = REPLICATION!;
  // Whichever node is the primary now, so the test can run again on the same servers.
  const [old, next] = (await isMaster(pair[0]!)) ? [0, 1] : [1, 0];
  // One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
  const keys = ['cluster-failover:node:' + process.pid + ':{a}', 'cluster-failover:node:' + process.pid + ':{b}'];
  const client = createClusterClient({ ...pair[old]!, commandTimeoutMs: 5000, connectTimeoutMs: 2000 });
  const onOld = direct(pair[old]!);
  const onNew = direct(pair[next]!);
  try {
    await client.connect();
    for (const key of keys) assert.equal(await client.set(key, 'before'), 'OK');
    // A replica that still copies refuses to be promoted without FORCE.
    for (const key of keys) {
      const end = Date.now() + 10_000;
      while ((await onNew.get(key)) !== 'before') {
        assert.ok(Date.now() < end, 'the replica to hold ' + key);
        await new Promise((r) => setTimeout(r, 20));
      }
    }
    await onNew.replicaOfNoOne();
    await onOld.replicaOf(replication[next]!.host, replication[next]!.port);
    let demoted = false;
    const poll = setInterval(() => void isMaster(pair[old]!).then((m) => (demoted = !m), () => undefined), 50);
    try {
      await until(() => demoted, 'the old primary to become a replica');
    } finally {
      clearInterval(poll);
    }

    // The demoted node names its new primary once that primary has accepted it.
    const started = Date.now();
    for (;;) {
      try {
        assert.equal(await client.set(keys[0]!, 'after'), 'OK');
        break;
      } catch (e) {
        assert.ok(e instanceof RuvioError && e.code === 'READONLY', String(e));
        assert.ok(Date.now() - started < 5000, `still refused after ${Date.now() - started} ms: ${String(e)}`);
        await new Promise((r) => setTimeout(r, 20));
      }
    }
    assert.equal(await onNew.get(keys[0]!), 'after');
    // The other shard's slots moved too: no write is refused once the table is reloaded.
    for (let round = 0; round < 10; round++) {
      for (const key of keys) assert.equal(await client.set(key, 'v' + round), 'OK');
    }
    for (const key of keys) assert.equal(await onNew.get(key), 'v9');
  } finally {
    client.disconnect();
    onOld.disconnect();
    onNew.disconnect();
  }
});
