// SPDX-License-Identifier: MIT
// Seed addresses over scripted fake servers: connect order, reconnect rotation, option
// validation, and slot table reloads from other nodes and from the seeds.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as net from 'node:net';
import { ClusterClient, ConnectionError, RuvioClient } from '../src/index.js';
import { bulk, err, fakeServer, ok, slotsReply, type FakeServer } from './fake-server.js';
import { rejects, sleep } from './helpers.js';

async function closedPort(): Promise<number> {
  const s = net.createServer();
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const port = (s.address() as net.AddressInfo).port;
  await new Promise<void>((r) => s.close(() => r()));
  return port;
}

const pong = (a: string[]) => (a[0] === 'PING' ? '+PONG\r\n' : ok);
const at = (port: number) => '127.0.0.1:' + port;

test('connect skips a seed that cannot be reached', async () => {
  const dead = await closedPort();
  const live = await fakeServer(pong);
  const c = new RuvioClient({ seeds: [at(dead), at(live.port)], connectTimeoutMs: 500 });
  try {
    assert.equal(await c.ping(), 'PONG');
    assert.equal(live.count('PING'), 1);
  } finally {
    c.disconnect();
    await live.close();
  }
});

test('connect fails with the last connect error when no seed can be reached', async () => {
  const c = new RuvioClient({ seeds: [at(await closedPort()), at(await closedPort())] });
  const e = await rejects(c.connect());
  assert.ok((e as unknown) instanceof ConnectionError && /ECONNREFUSED/.test(e.message), String(e));
  c.disconnect();
});

test('a refused AUTH stops at once and does not try the next seed', async () => {
  const first = await fakeServer((a) => (a[0] === 'AUTH' ? err('WRONGPASS invalid password') : ok));
  const second = await fakeServer(() => ok);
  const c = new RuvioClient({ seeds: [at(first.port), at(second.port)], password: 'x', reconnect: false });
  try {
    const e = await rejects(c.connect());
    assert.ok(e instanceof ConnectionError && /WRONGPASS/.test(e.message), String(e));
    assert.equal(second.seen.length, 0);
  } finally {
    c.disconnect();
    await first.close();
    await second.close();
  }
});

test('a dropped connection moves on to the next seed', async () => {
  const first = await fakeServer((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : a[0] === 'GET' ? bulk('first') : pong(a)));
  const second = await fakeServer((a) => (a[0] === 'GET' ? bulk('second') : pong(a)));
  const c = new RuvioClient({ seeds: [at(first.port), at(second.port)] });
  const restored: number[] = [];
  c.on('connect', () => restored.push(restored.length));
  try {
    assert.equal(await c.get('k'), 'first');
    assert.ok((await rejects(c.dbsize())) instanceof ConnectionError);
    // The first seed still accepts connections, but the reconnect starts after it.
    assert.equal(await c.get('k'), 'second');
    assert.equal(restored.length, 2);
    // The subscriber opens on the seed the command connection is on.
    const sub = c.subscriber();
    await sub.connect();
    assert.equal(second.count('PING'), 1);
    assert.equal(first.count('PING'), 0);
    sub.close();
  } finally {
    c.disconnect();
    await first.close();
    await second.close();
  }
});

test('a reconnect goes round the whole seed list and ends with the address that dropped', async () => {
  const first = await fakeServer((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : a[0] === 'GET' ? bulk('first') : ok));
  const dead = await closedPort();
  const c = new RuvioClient({ seeds: [at(first.port), at(dead)] });
  try {
    assert.equal(await c.get('k'), 'first');
    assert.ok((await rejects(c.dbsize())) instanceof ConnectionError);
    assert.equal(await c.get('k'), 'first');
  } finally {
    c.disconnect();
    await first.close();
  }
});

test('an empty seed host or a bad port is refused when the client is created', () => {
  for (const seeds of [[''], [':6379'], ['host'], ['host:0'], ['host:70000'], ['host:x'], ['[::1]'], ['::1:6379'], ['127.0.0.1:1', '']]) {
    assert.throws(() => new RuvioClient({ seeds }), TypeError, JSON.stringify(seeds));
    assert.throws(() => new ClusterClient({ seeds }), TypeError, JSON.stringify(seeds));
  }
  assert.throws(() => new RuvioClient({ seeds: ['127.0.0.1:1'], path: '/tmp/x.sock' }), TypeError);
  assert.throws(() => new ClusterClient({ seeds: ['127.0.0.1:1'], path: '/tmp/x.sock' }), TypeError);
  new RuvioClient({ seeds: ['[::1]:6379', 'example.com:6380'] }).disconnect();
});

// ---- cluster ----------------------------------------------------------------------------------

interface Pair {
  nodes: FakeServer[];
  /** Index of the node that owns every slot in the table the live node reports. */
  owner: number;
  close(): Promise<void>;
}

/** Two fake nodes; CLUSTER SLOTS names `owner` for all slots. */
async function pair(): Promise<Pair> {
  const nodes: FakeServer[] = [];
  const p: Pair = { nodes, owner: 0, close: async () => void (await Promise.all(nodes.map((n) => n.close()))) };
  for (let i = 0; i < 2; i++) {
    nodes.push(
      await fakeServer((a) => {
        if (a[0] === 'CLUSTER') return slotsReply([[0, 16383, nodes[p.owner]!.port]]);
        if (a[0] === 'GET') return bulk('node' + i);
        return pong(a);
      }),
    );
  }
  return p;
}

test('a lost node reloads the slot table from a seed', async () => {
  const p = await pair();
  const c = new ClusterClient({ seeds: [at(p.nodes[0]!.port), at(p.nodes[1]!.port)] });
  try {
    assert.equal(await c.get('key'), 'node0');
    p.owner = 1;
    await p.nodes[0]!.close();
    await sleep(30);
    assert.equal(await c.get('key'), 'node1');
    assert.deepEqual(c.nodes.map((n) => n.port), [p.nodes[1]!.port]);
  } finally {
    c.disconnect();
    await p.close();
  }
});

test('a keyless command whose node is gone goes to the first reachable node of the new table', async () => {
  const p = await pair();
  const c = new ClusterClient({ seeds: [at(p.nodes[0]!.port), at(p.nodes[1]!.port)] });
  try {
    assert.equal(await c.ping(), 'PONG');
    p.owner = 1;
    await p.nodes[0]!.close();
    await sleep(30);
    assert.equal(await c.ping(), 'PONG');
    assert.equal(p.nodes[1]!.count('PING') >= 1, true);
  } finally {
    c.disconnect();
    await p.close();
  }
});

test('a failed reload leaves no seed nodes behind', async () => {
  const p = await pair();
  const dead = await closedPort();
  const c = new ClusterClient({ seeds: [at(p.nodes[0]!.port), at(dead)] });
  try {
    assert.equal(await c.get('key'), 'node0');
    await p.nodes[0]!.close();
    await sleep(30);
    assert.ok((await rejects(c.get('key'))) instanceof ConnectionError);
    assert.deepEqual(c.nodes.map((n) => n.port), [p.nodes[0]!.port]);
  } finally {
    c.disconnect();
    await p.close();
  }
});

test('the first slot table comes from the first reachable seed', async () => {
  const p = await pair();
  const dead = await closedPort();
  const c = new ClusterClient({ seeds: [at(dead), at(p.nodes[1]!.port)] });
  try {
    p.owner = 1;
    assert.equal(await c.get('key'), 'node1');
    assert.deepEqual(c.nodes.map((n) => n.port), [p.nodes[1]!.port]);
  } finally {
    c.disconnect();
    await p.close();
  }
});

test('seeds on a standalone server behave like a plain client with failover', async () => {
  const handler = (name: string) => (a: string[]) =>
    a[0] === 'CLUSTER' ? err('ERR This instance has cluster support disabled') : a[0] === 'GET' ? bulk(name) : ok;
  const first = await fakeServer(handler('first'));
  const second = await fakeServer(handler('second'));
  const c = new ClusterClient({ seeds: [at(first.port), at(second.port)] });
  try {
    assert.equal(await c.get('k'), 'first');
    assert.equal(c.isCluster, false);
  } finally {
    c.disconnect();
    await first.close();
    await second.close();
  }
});
