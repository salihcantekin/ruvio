// SPDX-License-Identifier: MIT
// Read policy over scripted fake nodes: reads go to one replica per shard, everything else to the
// primary; an unreachable or LOADING replica is skipped until the next slot table load.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClusterClient, RuvioError, hashSlot, type ReadPolicy } from '../src/index.js';
import { arr, bulk, err, fakeServer, int, ok, type FakeServer } from './fake-server.js';
import { rejects } from './helpers.js';

const LOADING = err('LOADING Ruvio is loading the dataset in memory');

/** CLUSTER SLOTS for one primary owning every slot, with `replicas` listed after it (entries as given). */
function slotsWith(primary: number, replicas: (number | string)[]): string {
  const node = (port: number, id: string) => arr(bulk('127.0.0.1'), int(port), bulk(id));
  const listed = replicas.map((r) => (typeof r === 'number' ? node(r, 'r' + r) : r));
  return arr(arr(int(0), int(16383), node(primary, 'p'), ...listed));
}

interface Pair {
  primary: FakeServer;
  replica: FakeServer;
  close(): Promise<void>;
}

/** A primary and a replica; `replicaHandler` may answer first on the replica. */
async function pair(replicaHandler: (a: string[]) => string | undefined = () => undefined, listed?: (replica: number) => (number | string)[]): Promise<Pair> {
  let slots = '';
  let queuing = false;
  const primary = await fakeServer((a) => {
    if (a[0] === 'CLUSTER') return slots;
    if (a[0] === 'MULTI') queuing = true;
    if (a[0] === 'EXEC') return ((queuing = false), arr(bulk('p')));
    if (queuing && a[0] !== 'MULTI') return '+QUEUED\r\n';
    if (a[0] === 'GET') return bulk('p');
    if (a[0] === 'EXISTS' || a[0] === 'DEL') return int(1);
    return ok;
  });
  const replica = await fakeServer((a) => {
    const x = replicaHandler(a);
    if (x !== undefined) return x;
    if (a[0] === 'GET') return bulk('r');
    if (a[0] === 'EXISTS') return int(2);
    if (a[0] === 'STRLEN') return int(7);
    return ok;
  });
  slots = slotsWith(primary.port, listed ? listed(replica.port) : [replica.port]);
  return { primary, replica, close: async () => (await primary.close(), await replica.close()) };
}

async function withPolicy(p: Pair, readPolicy: ReadPolicy, fn: (c: ClusterClient) => Promise<void>): Promise<void> {
  const c = new ClusterClient({ host: '127.0.0.1', port: p.primary.port, readPolicy });
  try {
    await fn(c);
  } finally {
    c.disconnect();
    await p.close();
  }
}

const names = (s: FakeServer) => s.seen.map((a) => a[0]);

/** The port of a server that is no longer listening. */
async function closedPort(): Promise<number> {
  const s = await fakeServer(() => ok);
  const port = s.port;
  await s.close();
  return port;
}

test('read policy: CLUSTER SLOTS replicas are kept, malformed entries skipped', async () => {
  const p = await pair(undefined, (replica) => [replica, replica, bulk('not-an-array'), arr(bulk('127.0.0.1'), bulk('x')), arr(bulk('h2'), int(70000))]);
  await withPolicy(p, 'prefer-replica', async (c) => {
    await c.connect();
    const [node] = c.nodes;
    assert.deepEqual(node!.replicas, [{ host: '127.0.0.1', port: p.replica.port }]);
    assert.equal(node!.reader?.port, p.replica.port);
  });
});

test('read policy: reads go to the replica after READONLY, writes to the primary', async () => {
  const p = await pair();
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.equal(await c.get('k'), 'r');
    assert.equal(await c.exists('k'), 2);
    await c.set('k', 'v');
    assert.equal(await c.del('k'), 1);
    assert.deepEqual(names(p.replica), ['READONLY', 'GET', 'EXISTS']);
    assert.deepEqual(names(p.primary), ['CLUSTER', 'SET', 'DEL']);
  });
});

test('read policy: a pipeline goes to the replica only when every command reads', async () => {
  const p = await pair();
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.deepEqual(await c.pipeline().get('a').strlen('b').exec(), ['r', 7]);
    assert.deepEqual(names(p.replica), ['READONLY', 'GET', 'STRLEN']);
    await c.pipeline().set('a', '1').get('a').exec();
    assert.deepEqual(names(p.replica), ['READONLY', 'GET', 'STRLEN']);
    assert.deepEqual(names(p.primary), ['CLUSTER', 'SET', 'GET']);
  });
});

test('read policy: keyless, transaction and blocking commands stay on the primary', async () => {
  const p = await pair();
  await withPolicy(p, 'replica', async (c) => {
    await c.ping();
    await c.multi().get('k').exec();
    await c.blpop(['q'], 0.01);
    assert.deepEqual(names(p.replica), []);
    assert.deepEqual(names(p.primary).slice(0, 2), ['CLUSTER', 'PING']);
  });
});

test('read policy: split multi-key commands use the rule per part', async () => {
  const p = await pair();
  const a = 'a1';
  let b = 'b1';
  for (let i = 0; hashSlot(b) === hashSlot(a); i++) b = 'b' + i;
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.equal(await c.exists(a, b), 4);
    assert.equal(await c.del(a, b), 2);
    assert.deepEqual(names(p.replica), ['READONLY', 'EXISTS', 'EXISTS']);
    assert.deepEqual(names(p.primary), ['CLUSTER', 'DEL', 'DEL']);
  });
});

test('read policy: the default never opens a replica connection', async () => {
  const p = await pair();
  await withPolicy(p, 'primary', async (c) => {
    assert.equal(await c.get('k'), 'p');
    assert.deepEqual(await c.pipeline().get('a').get('b').exec(), ['p', 'p']);
    assert.deepEqual(names(p.replica), []);
    assert.equal(c.nodes[0]!.replicas.length, 0);
  });
});

test('read policy: an unreachable replica falls back to the primary, or fails under "replica"', async () => {
  const dead = await closedPort();

  const preferring = await pair(undefined, () => [dead]);
  await withPolicy(preferring, 'prefer-replica', async (c) => {
    assert.equal(await c.get('k'), 'p');
    assert.equal(await c.get('k'), 'p');
    assert.deepEqual(names(preferring.primary), ['CLUSTER', 'GET', 'GET']);
  });

  const piped = await pair(undefined, () => [dead]);
  await withPolicy(piped, 'prefer-replica', async (c) => {
    assert.deepEqual(await c.pipeline().get('a').get('b').exec(), ['p', 'p']);
    assert.deepEqual(names(piped.primary), ['CLUSTER', 'GET', 'GET']);
  });

  const only = await pair(undefined, () => [dead]);
  await withPolicy(only, 'replica', async (c) => {
    await rejects(c.get('k'));
    const e = await rejects(c.get('k'));
    assert.ok(e instanceof RuvioError && e.message.includes('no replica'), e.message);
    assert.deepEqual(names(only.primary), ['CLUSTER']);
  });
});

test('read policy: a LOADING replica is skipped, its read goes to the primary', async () => {
  const p = await pair((a) => (a[0] === 'GET' ? LOADING : undefined));
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.equal(await c.get('k'), 'p');
    assert.equal(await c.get('k'), 'p');
    assert.deepEqual(names(p.replica), ['READONLY', 'GET']);
    assert.deepEqual(names(p.primary), ['CLUSTER', 'GET', 'GET']);
  });
});

test('read policy: a LOADING pipeline read is sent again to the primary', async () => {
  const p = await pair((a) => (a[0] === 'GET' ? LOADING : undefined));
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.deepEqual(await c.pipeline().get('a').strlen('a').exec(), ['p', 7]);
    assert.deepEqual(names(p.primary), ['CLUSTER', 'GET']);
  });
});

test('read policy: "replica" returns the LOADING error and then reports no replica', async () => {
  const p = await pair((a) => (a[0] === 'GET' ? LOADING : undefined));
  await withPolicy(p, 'replica', async (c) => {
    const first = await rejects(c.get('k'));
    assert.equal(first.code, 'LOADING');
    const second = await rejects(c.get('k'));
    assert.ok(second.message.includes('no replica'), second.message);
    assert.deepEqual(names(p.primary), ['CLUSTER']);
  });
});

test('read policy: "replica" without any replica fails with "no replica"', async () => {
  const p = await pair(undefined, () => []);
  await withPolicy(p, 'replica', async (c) => {
    const e = await rejects(c.get('k'));
    assert.ok(e instanceof RuvioError && e.message.includes('no replica'), e.message);
    await c.set('k', 'v');
  });
});

test('read policy: "prefer-replica" without any replica reads from the primary', async () => {
  const p = await pair(undefined, () => []);
  await withPolicy(p, 'prefer-replica', async (c) => {
    assert.equal(await c.get('k'), 'p');
  });
});

test('read policy: an invalid value throws', () => {
  assert.throws(() => new ClusterClient({ readPolicy: 'both' as ReadPolicy }), TypeError);
});
