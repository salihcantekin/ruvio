// SPDX-License-Identifier: MIT
// A standalone client with several seeds (or a coordinator name) finds the primary again after a
// failover, against scripted nodes that answer ROLE, refuse writes with READONLY when they are
// replicas, and name a primary for SENTINEL. test/live-failover.test.ts does it on real servers.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as net from 'node:net';
import { ClusterClient, ConnectionError, RuvioClient, RuvioError, type ClientOptions } from '../src/index.js';
import { arr, bulk, err, fakeServer, int, ok, type FakeServer } from './fake-server.js';
import { rejects, until, within } from './helpers.js';

type Mode =
  | { kind: 'master' }
  /** A replica; it reports this primary in ROLE. */
  | { kind: 'replica'; primary?: number }
  /** Says master but refuses writes anyway. */
  | { kind: 'liar' }
  /** A coordinator that names this primary. */
  | { kind: 'names'; port: number };

const MASTER: Mode = { kind: 'master' };
const REPLICA: Mode = { kind: 'replica' };
const WRITES = new Set(['SET', 'INCR', 'DEL']);
const READONLY = 'READONLY You can\'t write against a read only replica.';

interface Node {
  readonly port: number;
  readonly server: FakeServer;
  become(mode: Mode): void;
  /** Closes every client socket (the listener stays). */
  drop(): void;
  count(name: string): number;
  total(): number;
}

const nodes: Node[] = [];
const extra: (() => Promise<void>)[] = [];
const clients: RuvioClient[] = [];

/** Closes what a test opened, whatever happened in it. */
async function cleanup(): Promise<void> {
  for (const c of clients.splice(0)) c.disconnect();
  for (const n of nodes.splice(0)) await n.server.close();
  for (const f of extra.splice(0)) await f();
}

async function start(initial: Mode): Promise<Node> {
  let mode = initial;
  const socks = new Set<net.Socket>();
  const server = await fakeServer((a, sock) => {
    if (!socks.has(sock)) {
      socks.add(sock);
      sock.on('close', () => socks.delete(sock));
    }
    const name = a[0]!;
    if (name === 'CLUSTER') return err('ERR This instance has cluster support disabled');
    if (name === 'ROLE') {
      if (mode.kind === 'master' || mode.kind === 'liar') return arr(bulk('master'), int(0), arr());
      if (mode.kind === 'replica') return arr(bulk('slave'), bulk('127.0.0.1'), int(mode.primary ?? 0), bulk('connected'), int(0));
      return err('ERR unknown command');
    }
    if (name === 'SENTINEL') return mode.kind === 'names' ? arr(bulk('127.0.0.1'), bulk(String(mode.port))) : '*-1\r\n';
    if ((mode.kind === 'replica' || mode.kind === 'liar') && WRITES.has(name)) return err(READONLY);
    if (name === 'PING') return '+PONG\r\n';
    if (name === 'GET') return '$-1\r\n';
    if (name === 'INCR') return int(1);
    if (name === 'EXEC') return '*0\r\n';
    return ok;
  });
  const node: Node = {
    port: server.port,
    server,
    become: (m) => {
      mode = m;
    },
    drop: () => {
      for (const s of socks) s.destroy();
    },
    count: (name) => server.count(name),
    total: () => server.seen.length,
  };
  nodes.push(node);
  return node;
}

/** A port nothing listens on. */
async function deadPort(): Promise<number> {
  const s = net.createServer();
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const port = (s.address() as net.AddressInfo).port;
  await new Promise<void>((r) => s.close(() => r()));
  return port;
}

const at = (port: number) => '127.0.0.1:' + port;

function connect(ports: number[], extraOptions: ClientOptions = {}): RuvioClient {
  const c = new RuvioClient({
    seeds: ports.map(at),
    connectTimeoutMs: 1000,
    commandTimeoutMs: 5000,
    ...extraOptions,
  });
  clients.push(c);
  return c;
}

async function readonly(p: Promise<unknown>): Promise<void> {
  const e = await rejects(p);
  assert.ok(e instanceof RuvioError && e.code === 'READONLY', String(e));
}

async function scenario(body: () => Promise<void>): Promise<void> {
  try {
    await within(body(), 'the scenario', 30_000);
  } finally {
    await cleanup();
  }
}

test('a READONLY write follows the new primary and is sent once more', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const next = await start(REPLICA);
    const c = connect([old.port, next.port]);
    await c.set('k', '1');
    assert.deepEqual([old.count('SET'), next.count('SET')], [1, 0]);

    old.become({ kind: 'replica', primary: next.port });
    next.become(MASTER);
    assert.equal(await c.set('k', '2'), 'OK');
    // The refused write went to the old node once, then to the new primary once.
    assert.deepEqual([old.count('SET'), next.count('SET')], [2, 1]);

    await c.set('k', '3');
    assert.deepEqual([old.count('SET'), next.count('SET')], [2, 2]);
  }));

test('a healthy connection sends nothing extra', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(REPLICA);
    const c = connect([a.port, b.port]);
    for (let i = 0; i < 50; i++) await c.set('k', 'v');
    await Promise.all(Array.from({ length: 50 }, () => c.get('k')));
    await c.pipeline().set('a', '1').get('a').exec();
    assert.equal(a.total(), 102);
    assert.equal(b.total(), 0);
  }));

test('a replica names its primary when that is no seed', () =>
  scenario(async () => {
    const replica = await start(MASTER);
    const hidden = await start(MASTER);
    const c = connect([replica.port, await deadPort()]);
    await c.ping();
    replica.become({ kind: 'replica', primary: hidden.port });
    await c.set('k', 'v');
    assert.deepEqual([replica.count('SET'), hidden.count('SET')], [1, 1]);
  }));

test('followPrimary false returns READONLY and sends no ROLE', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = connect([a.port, b.port], { followPrimary: false });
    await c.ping();
    a.become({ kind: 'replica', primary: b.port });
    await readonly(c.set('k', 'v'));
    assert.deepEqual([a.count('ROLE'), b.total()], [0, 0]);
  }));

test('one seed and no coordinator name: READONLY is returned and nothing is probed', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const c = connect([a.port]);
    a.become(REPLICA);
    await readonly(c.set('k', 'v'));
    assert.equal(a.count('ROLE'), 0);
    assert.equal(a.count('SET'), 1);
  }));

test('a reconnect takes the node that says master, not the next seed', () =>
  scenario(async () => {
    const first = await start(MASTER);
    const second = await start(MASTER);
    const third = await start(MASTER);
    const c = connect([first.port, second.port, third.port]);
    await c.ping();
    const events: string[] = [];
    c.on('disconnect', (_e, address) => events.push('lost ' + address));
    c.on('connect', (address) => events.push('up ' + address));

    // The second seed became a replica while the connection was down.
    second.become({ kind: 'replica', primary: third.port });
    first.become({ kind: 'replica', primary: third.port });
    first.drop();

    assert.ok((await rejects(c.ping())) instanceof ConnectionError);
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.deepEqual([first.count('SET'), second.count('SET'), third.count('SET')], [0, 0, 1]);
    assert.deepEqual(events, ['lost ' + at(first.port), 'up ' + at(third.port)]);
  }));

test('a reconnect without any primary still connects to the first seed that answers', () =>
  scenario(async () => {
    const first = await start(MASTER);
    const second = await start(MASTER);
    const c = connect([first.port, second.port]);
    await c.ping();
    first.become(REPLICA);
    second.become(REPLICA);
    first.drop();
    assert.ok((await rejects(c.ping())) instanceof ConnectionError);
    // The reconnect starts after the seed that dropped.
    assert.equal(await c.ping(), 'PONG');
    assert.equal(second.count('PING'), 1);
  }));

test('a failed reconnect goes round the seeds and the next command asks for the primary again', () =>
  scenario(async () => {
    const first = await start(MASTER);
    const second = await start(REPLICA);
    const dead = await deadPort();
    const c = connect([first.port, dead, second.port]);
    await c.ping();
    first.become(REPLICA);
    second.become(MASTER);
    first.drop();
    assert.ok((await rejects(c.ping())) instanceof ConnectionError);
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.equal(second.count('SET'), 1);
  }));

test('without any primary the READONLY error comes back after one round when reconnect is off', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = connect([a.port, b.port], { reconnect: false });
    await c.ping();
    a.become(REPLICA);
    b.become(REPLICA);
    await readonly(c.set('k', 'v'));
    assert.deepEqual([a.count('SET'), b.count('SET')], [1, 0]);
    // The connection stayed where it was.
    assert.equal(await c.ping(), 'PONG');
    assert.equal(b.count('PING'), 0);
  }));

test('rounds go on with the backoff until a primary shows up', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = connect([a.port, b.port]);
    await c.ping();
    a.become(REPLICA);
    b.become(REPLICA);
    const write = c.set('k', 'v');
    // Two rounds have asked both nodes and found none; now the second node is promoted.
    await until(() => a.count('ROLE') >= 2 && b.count('ROLE') >= 2, 'two rounds');
    b.become(MASTER);
    assert.equal(await write, 'OK');
    assert.deepEqual([a.count('SET'), b.count('SET')], [1, 1]);
  }));

test('a write refused twice is not sent a third time', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const liar = await start(MASTER);
    const c = connect([old.port, liar.port]);
    await c.ping();
    old.become(REPLICA);
    liar.become({ kind: 'liar' });
    await readonly(c.set('k', 'v'));
    assert.deepEqual([old.count('SET'), liar.count('SET')], [1, 1]);
    // No round for the retry: only the one for the first refusal asked ROLE.
    assert.equal(liar.count('ROLE'), 1);
  }));

test('a write refused inside a transaction follows but is not sent again', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const next = await start(REPLICA);
    const c = connect([old.port, next.port]);
    await c.call('MULTI');
    old.become({ kind: 'replica', primary: next.port });
    next.become(MASTER);
    await readonly(c.call('SET', 'k', 'v'));
    assert.equal(next.count('SET'), 0);
    // The client moved: the next command is not inside the lost transaction.
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.equal(next.count('SET'), 1);
  }));

test('after EXEC a refused write is sent again', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const next = await start(REPLICA);
    const c = connect([old.port, next.port]);
    await c.call('MULTI');
    await c.call('EXEC');
    old.become({ kind: 'replica', primary: next.port });
    next.become(MASTER);
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.equal(next.count('SET'), 1);
  }));

test('a READONLY reply inside a pipeline is returned at its index and not sent again', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const next = await start(REPLICA);
    const c = connect([old.port, next.port]);
    await c.ping();
    old.become({ kind: 'replica', primary: next.port });
    next.become(MASTER);
    const out = await c.pipeline().get('a').set('k', 'v').get('b').exec();
    assert.equal(out[0], null);
    assert.ok(out[1] instanceof RuvioError && out[1].code === 'READONLY', String(out[1]));
    assert.equal(out[2], null);
    assert.equal(next.count('SET'), 0);
    // The connection moved to the primary.
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.deepEqual([old.count('SET'), next.count('SET')], [1, 1]);
  }));

test('writes refused at the same time are each sent once to the new primary', () =>
  scenario(async () => {
    const old = await start(MASTER);
    const next = await start(REPLICA);
    const c = connect([old.port, next.port]);
    await c.ping();
    old.become({ kind: 'replica', primary: next.port });
    next.become(MASTER);
    const all = await Promise.all([c.set('a', '1'), c.set('b', '2'), c.incr('n')]);
    assert.deepEqual(all, ['OK', 'OK', 1]);
    assert.deepEqual([next.count('SET'), next.count('INCR')], [2, 1]);
    // One search for all of them.
    assert.equal(old.count('ROLE') + next.count('ROLE'), 1);
  }));

test('only an error that is READONLY alone or followed by a space starts a search', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const fixed = ['READONLYX nope', 'LOADING copying', 'MASTERDOWN link down', 'NOREPLICAS too few', 'ERR READONLY'];
    let next = 0;
    const odd = await fakeServer((args) => (args[0] === 'SET' ? err(fixed[next++ % fixed.length]!) : args[0] === 'ROLE' ? arr(bulk('master'), int(0), arr()) : ok));
    extra.push(() => odd.close());
    const c = connect([odd.port, a.port]);
    for (let i = 0; i < fixed.length; i++) {
      const e = await rejects(c.set('k', 'v'));
      assert.ok(e instanceof RuvioError, String(e));
    }
    assert.equal(odd.count('ROLE'), 0);
    assert.equal(odd.count('SET'), fixed.length);
    assert.equal(b.total() + a.total(), 0);
  }));

test('a refused AUTH on a probe fails the write at once with that error', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const strict = await fakeServer((args) => (args[0] === 'AUTH' ? err('WRONGPASS invalid password') : ok));
    extra.push(() => strict.close());
    const c = connect([a.port, strict.port], { password: 's3' });
    await c.ping();
    a.become(REPLICA);
    const e = await rejects(c.set('k', 'v'));
    assert.ok(e instanceof RuvioError && e.code === 'WRONGPASS', String(e));
    assert.equal(strict.seen[0]![0], 'AUTH');
    // The connection stayed where it was.
    assert.equal(await c.ping(), 'PONG');
    assert.equal(a.count('PING'), 2);
  }));

test('a probe sends AUTH then ROLE, without SELECT, CLIENT SETNAME or anything else', () =>
  scenario(async () => {
    const seen: string[][] = [];
    const a = await start(MASTER);
    const b = await fakeServer((args) => {
      seen.push(args);
      if (args[0] === 'ROLE') return arr(bulk('master'), int(0), arr());
      return ok;
    });
    extra.push(() => b.close());
    const c = connect([a.port, b.port], { password: 's3', username: 'app', db: 2, name: 'worker' });
    await c.ping();
    a.become(REPLICA);
    b.seen.length = 0;
    await c.set('k', 'v');
    // b was probed (AUTH, ROLE) and then connected for real (AUTH, SELECT, CLIENT, SET).
    const names = b.seen.map((x) => x[0]);
    assert.deepEqual(names, ['AUTH', 'ROLE', 'AUTH', 'SELECT', 'CLIENT', 'SET']);
    assert.deepEqual(b.seen[0], ['AUTH', 'app', 's3']);
  }));

test('a coordinator name finds the primary at connect and after a failover', () =>
  scenario(async () => {
    const coordinator = await start({ kind: 'names', port: 0 });
    const one = await start(MASTER);
    const two = await start(REPLICA);
    coordinator.become({ kind: 'names', port: one.port });
    const c = connect([coordinator.port], { sentinelMaster: 'main' });
    await c.set('k', '1');
    assert.deepEqual([one.count('SET'), two.count('SET')], [1, 0]);
    assert.equal(coordinator.count('SENTINEL'), 1);
    assert.equal(coordinator.server.seen.find((a) => a[0] === 'SENTINEL')!.join(' '), 'SENTINEL GET-MASTER-ADDR-BY-NAME main'.replace('GET-MASTER-ADDR-BY-NAME', 'get-master-addr-by-name'));

    one.become(REPLICA);
    two.become(MASTER);
    coordinator.become({ kind: 'names', port: two.port });
    await c.set('k', '2');
    assert.deepEqual([one.count('SET'), two.count('SET')], [2, 1]);

    // After a dropped socket the coordinator is asked again.
    one.become(MASTER);
    two.become(REPLICA);
    coordinator.become({ kind: 'names', port: one.port });
    two.drop();
    assert.ok((await rejects(c.ping())) instanceof ConnectionError);
    await c.set('k', '3');
    assert.deepEqual([one.count('SET'), two.count('SET')], [3, 1]);

    // A subscriber asks the coordinator itself.
    const sub = c.subscriber();
    await sub.connect();
    sub.close();
    assert.ok(coordinator.count('SENTINEL') >= 4);
  }));

test('a coordinator that names no primary fails the connect with a ConnectionError', () =>
  scenario(async () => {
    const coordinator = await start(MASTER);
    const c = connect([coordinator.port], { sentinelMaster: 'main' });
    const e = await rejects(c.connect());
    assert.ok((e as unknown) instanceof ConnectionError && /main/.test(e.message), String(e));
  }));

test('a coordinator that names a node that is no master is skipped', () =>
  scenario(async () => {
    const slow = await start(REPLICA);
    const good = await start(MASTER);
    const bad = await start({ kind: 'names', port: slow.port });
    const fine = await start({ kind: 'names', port: good.port });
    const c = connect([bad.port, fine.port], { sentinelMaster: 'main' });
    await c.set('k', 'v');
    assert.deepEqual([slow.count('SET'), good.count('SET')], [0, 1]);
    assert.equal(bad.count('SENTINEL'), 1);
  }));

test('sentinelMaster cannot be combined with path', () => {
  assert.throws(() => new RuvioClient({ path: '/tmp/x.sock', sentinelMaster: 'main' }), TypeError);
});

test('a hung seed costs one bounded wait', () =>
  scenario(async () => {
    const silentSockets = new Set<net.Socket>();
    const silent = net.createServer((s) => {
      silentSockets.add(s);
      s.on('error', () => undefined);
    });
    await new Promise<void>((r) => silent.listen(0, '127.0.0.1', r));
    extra.push(
      () =>
        new Promise<void>((r) => {
          for (const s of silentSockets) s.destroy();
          silent.close(() => r());
        }),
    );
    const live = await start(MASTER);
    const other = await start(REPLICA);
    const c = connect([live.port, (silent.address() as net.AddressInfo).port, other.port], { connectTimeoutMs: 300 });
    await c.ping();
    live.become({ kind: 'replica', primary: other.port });
    other.become(MASTER);
    const started = Date.now();
    assert.equal(await c.set('k', 'v'), 'OK');
    assert.ok(Date.now() - started < 3000, 'took ' + (Date.now() - started) + ' ms');
  }));

test('disconnect while the primary is searched for rejects the waiting command and leaves nothing running', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = connect([a.port, b.port]);
    await c.ping();
    a.become(REPLICA);
    b.become(REPLICA);
    const write = c.set('k', 'v');
    const failed = rejects(write);
    await until(() => a.count('ROLE') >= 1, 'the first round');
    c.disconnect();
    const e = await failed;
    assert.ok(e instanceof ConnectionError, String(e));
  }));

test('a socket that drops while the primary is searched for returns the READONLY error', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = connect([a.port, b.port]);
    await c.ping();
    a.become(REPLICA);
    b.become(REPLICA);
    const failed = rejects(c.set('k', 'v'));
    await until(() => a.count('ROLE') >= 1, 'the first round');
    a.drop();
    const e = await failed;
    assert.ok(e instanceof RuvioError && e.code === 'READONLY', String(e));
  }));

test('a cluster client does not follow', () =>
  scenario(async () => {
    const a = await start(MASTER);
    const b = await start(MASTER);
    const c = new ClusterClient({ seeds: [at(a.port), at(b.port)], connectTimeoutMs: 1000, commandTimeoutMs: 5000 });
    try {
      await c.ping();
      a.become(REPLICA);
      b.become(MASTER);
      await readonly(c.set('k', 'v'));
      assert.equal(a.count('ROLE'), 0);
    } finally {
      c.disconnect();
    }
  }));
