// SPDX-License-Identifier: MIT
// Connection and client edge cases against scripted fake servers: URL options, handshake,
// reconnects, closed clients, timeouts, malformed replies, helpers (locks, leases, getOrCreate,
// idempotency, iterators), pipelines and transactions.

import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as net from 'node:net';
import netCjs from 'node:net';
import { syncBuiltinESMExports } from 'node:module';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  AcquireTimeoutError,
  ConnectionError,
  IdempotencyConflictError,
  IdempotencyPendingError,
  ReplyError,
  RuvioClient,
  RuvioError,
  createClient,
  type ClientOptions,
} from '../src/index.js';
import { blockingWait, WAIT_FOREVER } from '../src/blocking.js';
import { DEFAULT_COMMAND_TIMEOUT_MS } from '../src/connection.js';
import { arr, bulk, err, fakeServer, int, ok, type FakeServer, type Handler } from './fake-server.js';
import { rejects, sleep, until } from './helpers.js';

async function withClient<T>(handler: Handler, opts: ClientOptions, fn: (c: RuvioClient, s: FakeServer) => Promise<T>): Promise<T> {
  const s = await fakeServer(handler);
  const c = new RuvioClient({ host: '127.0.0.1', port: s.port, ...opts });
  try {
    return await fn(c, s);
  } finally {
    c.disconnect();
    await s.close();
  }
}

const internals = (c: RuvioClient) => (c as unknown as { conn: any; options: any });

/** A port nothing listens on. */
async function deadPort(): Promise<number> {
  const s = net.createServer();
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const port = (s.address() as net.AddressInfo).port;
  await new Promise<void>((r) => s.close(() => r()));
  return port;
}

// ---- options and handshake ------------------------------------------------------------------

test('URL options: credentials (percent-decoded), database, default port and TLS schemes', async () => {
  const s = await fakeServer(() => ok);
  const c = new RuvioClient(`redis://us%40er:p%3Ass@127.0.0.1:${s.port}/3`);
  try {
    assert.equal(await c.ping(), 'OK');
    assert.deepEqual(s.seen.slice(0, 3), [['AUTH', 'us@er', 'p:ss'], ['SELECT', '3'], ['PING']]);
  } finally {
    c.disconnect();
    await s.close();
  }
  const only = new RuvioClient(`ruvio://:secret@127.0.0.1:1`);
  assert.equal(internals(only).options.password, 'secret');
  assert.equal(internals(only).options.username, undefined);
  const d = internals(new RuvioClient('redis://example.test')).options;
  assert.deepEqual(d, { host: 'example.test', port: 6379 });
  assert.equal(internals(new RuvioClient('rediss://example.test:7000')).options.tls, true);
  assert.equal(internals(new RuvioClient('ruvios://example.test')).options.tls, true);
  assert.equal(internals(new RuvioClient('redis://h/')).options.db, undefined, 'an empty path selects nothing');
  assert.equal(internals(new RuvioClient('redis://h/0')).options.db, 0, 'db 0 is the default');
  assert.equal(internals(createClient({ port: 5 })).options.port, 5);
  assert.throws(() => new RuvioClient('not a url'));
});

test('handshake: AUTH with username, RESP3 HELLO with AUTH, SELECT and SETNAME in order', async () => {
  await withClient(
    (a) => (a[0] === 'HELLO' ? '%1\r\n+server\r\n+ruvio\r\n' : ok),
    { protocol: 3, username: 'me', password: 'pw', db: 2, name: 'nm' },
    async (c, s) => {
      await c.ping();
      assert.deepEqual(s.seen.slice(0, 4), [['HELLO', '3', 'AUTH', 'me', 'pw'], ['SELECT', '2'], ['CLIENT', 'SETNAME', 'nm'], ['PING']]);
    },
  );
  await withClient(() => ok, { protocol: 3 }, async (c, s) => {
    await c.ping();
    assert.deepEqual(s.seen[0], ['HELLO', '3']);
  });
  await withClient(() => ok, { password: 'pw' }, async (c, s) => {
    await c.ping();
    assert.deepEqual(s.seen[0], ['AUTH', 'pw']);
  });
});

test('durability: the method and the connect option send CLIENT DURABILITY', async () => {
  await withClient(() => ok, {}, async (c, s) => {
    assert.equal(await c.clientDurability(1), 'OK');
    assert.equal(await c.clientDurability(2, 750), 'OK');
    assert.deepEqual(s.seen, [['CLIENT', 'DURABILITY', 'REPLICAS', '1'], ['CLIENT', 'DURABILITY', 'REPLICAS', '2', 'TIMEOUT', '750']]);
  });
  await withClient(() => ok, { db: 2, name: 'nm', durability: { replicas: 1, timeoutMs: 5000 } }, async (c, s) => {
    await c.ping();
    assert.deepEqual(s.seen.slice(0, 4), [['SELECT', '2'], ['CLIENT', 'SETNAME', 'nm'], ['CLIENT', 'DURABILITY', 'REPLICAS', '1', 'TIMEOUT', '5000'], ['PING']]);
  });
  await withClient(() => ok, { durability: { replicas: 3 } }, async (c, s) => {
    await c.ping();
    assert.deepEqual(s.seen.slice(0, 2), [['CLIENT', 'DURABILITY', 'REPLICAS', '3'], ['PING']]);
  });
  await withClient(() => ok, {}, async (c, s) => {
    await c.ping();
    assert.deepEqual(s.seen, [['PING']], 'nothing is sent by default');
  });
  await withClient((a) => (a[0] === 'CLIENT' ? err('ERR CLIENT DURABILITY needs replication on this server') : ok), { durability: { replicas: 1 } }, async (c) => {
    await assert.rejects(c.ping());
  });
});

test('handshake: a rejected AUTH fails the waiting command and the next command retries it', async () => {
  let bad = true;
  await withClient(
    (a) => (a[0] === 'AUTH' ? (bad ? err('WRONGPASS invalid username-password pair') : ok) : ok),
    { password: 'nope' },
    async (c, s) => {
      const e = await rejects(c.ping());
      assert.ok(e instanceof ConnectionError, String(e));
      assert.match(e.message, /WRONGPASS/);
      assert.ok((e as unknown as { cause: unknown }).cause instanceof RuvioError);
      assert.equal(s.count('PING'), 1, 'the command was sent before the reply to AUTH was read');
      bad = false;
      assert.equal(await c.ping(), 'OK');
      assert.equal(s.count('AUTH'), 2);
    },
  );
});

test('select() is remembered: a reconnect selects the same database again', async () => {
  await withClient((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : ok), {}, async (c, s) => {
    await c.select(5);
    assert.equal(internals(c).conn.db, 5);
    await rejects(c.dbsize());
    await c.ping();
    assert.deepEqual(s.seen.filter((a) => a[0] === 'SELECT'), [['SELECT', '5'], ['SELECT', '5'], ['SELECT', '5']]);
    const dup = c.duplicate({ name: 'dup' });
    assert.equal(internals(dup).options.db, 5);
    assert.equal(internals(dup).options.name, 'dup');
    assert.equal(internals(dup).options.host, '127.0.0.1');
    dup.disconnect();
  });
});

// ---- lifecycle and events -------------------------------------------------------------------

test('events: connect, disconnect with the cause, connectError, and off()', async () => {
  const s = await fakeServer((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : ok));
  const c = new RuvioClient({ host: '127.0.0.1', port: s.port });
  const log: string[] = [];
  const onConnect = () => log.push('connect');
  assert.equal(c.on('connect', onConnect), c, 'on() chains');
  c.on('disconnect', (e: unknown) => log.push('disconnect:' + (e as Error).message));
  try {
    await c.ping();
    assert.deepEqual(log, ['connect']);
    const e = await rejects(c.dbsize());
    assert.ok(e instanceof ConnectionError);
    await sleep(10);
    assert.equal(log.length, 2);
    assert.match(log[1]!, /^disconnect:ruvio: connection closed/);
    assert.equal(c.off('connect', onConnect), c);
    await c.ping();
    assert.equal(log.length, 2, 'a removed listener is not called again');
  } finally {
    c.disconnect();
    await s.close();
  }
  const port = await deadPort();
  const dead = new RuvioClient({ host: '127.0.0.1', port });
  const errors: unknown[] = [];
  const connectErrors: unknown[] = [];
  dead.on('error', (e: unknown) => errors.push(e));
  dead.on('connectError', (e: unknown) => connectErrors.push(e));
  const e = await rejects(dead.get('k'));
  assert.ok(e instanceof ConnectionError && /ECONNREFUSED/.test(e.message), String(e));
  assert.equal(errors.length, 1);
  assert.equal(connectErrors.length, 1);
  dead.disconnect();
});

test('connect(): handshake round trip, instant when idle and connected, rejected when closed', async () => {
  await withClient(() => ok, { db: 1 }, async (c, s) => {
    assert.equal(internals(c).conn.isOpen, false);
    await c.connect();
    assert.equal(internals(c).conn.isOpen, true);
    assert.equal(internals(c).conn.isConnected, true);
    assert.deepEqual(s.seen.map((a) => a[0]), ['SELECT', 'PING']);
    await c.connect(); // connected and idle: nothing more is sent
    assert.equal(s.count('PING'), 1);
    internals(c).conn.flushNow(); // nothing buffered: harmless
    c.disconnect();
    assert.equal(internals(c).conn.isClosed, true);
    const e = await rejects(c.connect());
    assert.ok(e instanceof ConnectionError && /closed/.test(e.message));
  });
});

test('a closed client rejects commands, pipelines and transactions with ConnectionError', async () => {
  await withClient(() => ok, {}, async (c) => {
    await c.ping();
    c.disconnect();
    for (const p of [c.get('k'), c.pipeline().get('a').get('b').exec(), c.multi().set('a', '1').exec(), c.binary.get('k')]) {
      const e = await rejects(p as Promise<unknown>);
      assert.ok(e instanceof ConnectionError && /client is closed/.test(e.message), String(e));
    }
    assert.equal(await c.pipeline().exec().then((r) => r.length), 0, 'an empty pipeline never touches the socket');
    assert.equal(c.inFlight, 0);
  });
});

test('reconnect: false closes the client for good after a drop', async () => {
  await withClient((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : ok), { reconnect: false }, async (c, s) => {
    await c.ping();
    await rejects(c.dbsize());
    await sleep(10);
    const e = await rejects(c.ping());
    assert.ok(e instanceof ConnectionError && /closed/.test(e.message));
    assert.equal(s.count('PING'), 1);
  });
});

test('quit(): waits for in-flight replies; tolerates a drop while waiting; no-op when closed or never opened', async () => {
  await withClient(
    (a, sock) => {
      if (a[0] === 'GET') {
        setTimeout(() => sock.write(bulk('late')), 40);
        return ''; // '' (not null) keeps the fake server parsing the pipelined PING behind it
      }
      if (a[0] === 'PING') {
        setTimeout(() => sock.write(ok), 80); // replies stay in order: after the slow GET
        return '';
      }
      return ok;
    },
    {},
    async (c) => {
      const p = c.get('k');
      await c.quit();
      assert.equal(await p, 'late');
      await c.quit(); // already closed
    },
  );
  await withClient((a, sock) => (a[0] === 'DBSIZE' ? (sock.destroy(), null) : ok), {}, async (c) => {
    const p = c.dbsize();
    await c.quit(); // the drop rejects the PING that quit() queued; quit still resolves
    assert.ok((await rejects(p)) instanceof ConnectionError);
    assert.equal(internals(c).conn.isClosed, true);
  });
  const fresh = new RuvioClient({ host: '127.0.0.1', port: 1 });
  await fresh.quit(); // never opened
  await fresh[Symbol.asyncDispose]();
});

test('connect timeout destroys the socket and rejects the command', async () => {
  // A socket that never connects and swallows writes, like a SYN that is never answered.
  class Hung extends net.Socket {
    override write(): boolean {
      return true;
    }
    override cork(): void {}
    override uncork(): void {}
  }
  const fake = new Hung();
  mock.method(netCjs, 'connect', () => fake);
  syncBuiltinESMExports();
  try {
    const c = new RuvioClient({ host: '127.0.0.1', port: 9, connectTimeoutMs: 20 });
    const e = await rejects(c.ping());
    assert.ok(e instanceof ConnectionError && /connect timeout/.test(e.message), String(e));
    c.disconnect();
  } finally {
    mock.restoreAll();
    syncBuiltinESMExports();
  }
});

test('commandTimeoutMs fails the connection when a peer never answers, and the next command reconnects', async () => {
  let silent = true;
  await withClient(
    () => (silent ? null : '+PONG\r\n'),
    { commandTimeoutMs: 100 },
    async (c) => {
      const started = Date.now();
      const e = await rejects(c.ping());
      assert.ok(e instanceof ConnectionError && /command timeout/.test(String(e.cause ?? e.message)), String(e));
      assert.ok(Date.now() - started < 3000);
      silent = false;
      assert.ok(await c.ping());
      c.disconnect();
    },
  );
});

test('commandTimeoutMs defaults to 30 s, and 0 turns it off', () => {
  assert.equal(DEFAULT_COMMAND_TIMEOUT_MS, 30_000);
  const on = new RuvioClient({ host: '127.0.0.1', port: 9 });
  const off = new RuvioClient({ host: '127.0.0.1', port: 9, commandTimeoutMs: 0 });
  assert.notEqual(internals(on).conn.watchdog, null);
  assert.equal(internals(off).conn.watchdog, null);
  on.disconnect();
  off.disconnect();
});

test('commandTimeoutMs: a blocking command waits its own timeout on top', async () => {
  await withClient(
    (args, sock) => {
      if (args[0] !== 'BLPOP') return null; // anything else is never answered
      setTimeout(() => sock.write('*-1\r\n'), 1000); // an empty list: nil when BLPOP's 1 s runs out
      return null;
    },
    { commandTimeoutMs: 200 },
    async (c) => {
      const started = Date.now();
      assert.equal(await c.blpop('k', 1), null);
      const took = Date.now() - started;
      assert.ok(took >= 900 && took < 3000, `took ${took} ms`);
      // The plain limit is back for the next command.
      const e = await rejects(c.ping());
      assert.ok(e instanceof ConnectionError && /command timeout/.test(String(e.cause ?? e.message)), String(e));
      c.disconnect();
    },
  );
});

test('blockingWait: the wait each blocking command adds to the command timeout', () => {
  const enc = (...a: string[]): Buffer => Buffer.from(arr(...a.map(bulk)), 'latin1');
  const cases: [string[], number][] = [
    [['GET', 'k'], 0],
    [['SET', 'k', 'v'], 0],
    [['blpop', 'a', 'b', '1'], 1000],
    [['BLPOP', 'k', '0'], WAIT_FOREVER],
    [['BLPOP', 'k', '0.5'], 500],
    [['BLPOP', 'k', 'soon'], WAIT_FOREVER],
    [['BLPOP', 'k', '-1'], WAIT_FOREVER],
    [['BRPOP', 'k', '2'], 2000],
    [['BZPOPMIN', 'k', '1.5'], 1500],
    [['BZPOPMAX', 'k', '3'], 3000],
    [['BRPOPLPUSH', 'a', 'b', '4'], 4000],
    [['BLMOVE', 'a', 'b', 'LEFT', 'RIGHT', '0.25'], 250],
    [['BLMPOP', '2', '2', 'a', 'b', 'LEFT'], 2000],
    [['BZMPOP', '0', '1', 'z', 'MIN'], WAIT_FOREVER],
    [['XREAD', 'STREAMS', 's', '$'], 0],
    [['XREAD', 'COUNT', '1', 'BLOCK', '700', 'STREAMS', 's', '$'], 700],
    [['XREAD', 'BLOCK', '0', 'STREAMS', 's', '$'], WAIT_FOREVER],
    [['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 's', '>'], 0],
    [['XREADGROUP', 'GROUP', 'BLOCK', 'c', 'STREAMS', 's', '>'], 0],
    [['XREADGROUP', 'GROUP', 'g', 'c', 'BLOCK', '50', 'STREAMS', 's', '>'], 50],
    [['WAIT', '1', '100'], 100],
    [['WAIT', '1', '0'], WAIT_FOREVER],
    [['WAITAOF', '1', '0', '200'], 200],
    [['FILLGET', 'k', '1000'], 0],
    [['FILLGET', 'k', '1000', 'WAIT', '300'], 300],
    [['FILLGET', 'k', '1000', 'WAIT', '0'], WAIT_FOREVER],
    [['MONITOR'], WAIT_FOREVER],
    [['SUBSCRIBE', 'news'], WAIT_FOREVER],
    [['PSUBSCRIBE', 'n*'], WAIT_FOREVER],
    [['SSUBSCRIBE', 'news'], WAIT_FOREVER],
    [['PUBLISH', 'news', 'x'], 0],
    [['BLPOP'], WAIT_FOREVER],
  ];
  for (const [a, want] of cases) {
    const b = enc(...a);
    assert.equal(blockingWait(b, 0, b.length), want, a.join(' '));
  }
  // A pipeline takes its widest wait; any wait-forever wins.
  const pipe = Buffer.concat([enc('GET', 'k'), enc('BLPOP', 'k', '2'), enc('XREAD', 'BLOCK', '100', 'STREAMS', 's', '$')]);
  assert.equal(blockingWait(pipe, 0, pipe.length), 2000);
  const forever = Buffer.concat([enc('BLPOP', 'k', '2'), enc('BLPOP', 'k', '0')]);
  assert.equal(blockingWait(forever, 0, forever.length), WAIT_FOREVER);
  const junk = Buffer.from('garbage');
  assert.equal(blockingWait(junk, 0, junk.length), 0);
});

test('TLS options: a server that does not speak TLS fails the connection', async () => {
  const open = new Set<net.Socket>();
  const srv = net.createServer((s) => {
    open.add(s);
    s.on('error', () => undefined);
    s.end('this is not a TLS server\r\n');
  });
  await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r));
  const port = (srv.address() as net.AddressInfo).port;
  try {
    for (const tls of [true, { rejectUnauthorized: false }] as const) {
      const c = new RuvioClient({ host: '127.0.0.1', port, tls, connectTimeoutMs: 2000 });
      const e = await rejects(c.ping());
      assert.ok(e instanceof ConnectionError, String(e));
      c.disconnect();
    }
  } finally {
    for (const s of open) s.destroy();
    await new Promise<void>((r) => srv.close(() => r()));
  }
});

test('unix socket path', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'ruvio-'));
  const path = join(dir, 's.sock');
  const srv = net.createServer((s) => {
    s.on('error', () => undefined);
    s.on('data', () => s.write('+PONG\r\n'));
  });
  await new Promise<void>((r) => srv.listen(path, r));
  const c = new RuvioClient({ path });
  try {
    assert.equal(await c.ping(), 'PONG');
  } finally {
    c.disconnect();
    await new Promise<void>((r) => srv.close(() => r()));
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---- malformed and unexpected replies -------------------------------------------------------

test('a protocol error fails the pending commands, drops the socket, and the next command reconnects', async () => {
  let junk = true;
  await withClient(
    (a, sock) => {
      if (a[0] === 'GET' && junk) {
        sock.write('?junk\r\n');
        return null;
      }
      return a[0] === 'GET' ? bulk('fine') : ok;
    },
    {},
    async (c) => {
      const e = await rejects(c.get('k'));
      assert.ok(e instanceof ConnectionError && /unexpected reply type/.test(e.message), String(e));
      junk = false;
      assert.equal(await c.get('k'), 'fine');
    },
  );
});

test('a reply nobody asked for is reported as an "unexpected" event, not delivered to a later command', async () => {
  await withClient((a) => (a[0] === 'PING' ? '+PONG\r\n+STRAY\r\n' : bulk('v')), {}, async (c) => {
    const stray: unknown[] = [];
    c.on('unexpected', (v: unknown) => stray.push(v));
    assert.equal(await c.ping(), 'PONG');
    await until(() => stray.length >= 1, 'the stray reply');
    assert.deepEqual(stray, ['STRAY']);
    assert.equal(await c.get('k'), 'v');
  });
});

test('a reply of the wrong shape rejects only that command with ReplyError', async () => {
  await withClient(
    (a) => (a[0] === 'FILLGET' ? arr(bulk('NONSENSE'), bulk('x')) : a[0] === 'PING' ? '+PONG\r\n' : ok),
    {},
    async (c) => {
      const [bad, good] = await Promise.allSettled([c.fillget('k', 10), c.ping()]);
      assert.equal(bad.status, 'rejected');
      assert.ok((bad as PromiseRejectedResult).reason instanceof ReplyError);
      assert.equal(good.status === 'fulfilled' && good.value, 'PONG');
      const e2 = await rejects(c.fillget('k', 10));
      assert.match(e2.message, /unexpected state NONSENSE/);
    },
  );
});

test('RESP3 push frames arrive as "push" events and never as command replies', async () => {
  await withClient(
    (a) => (a[0] === 'GET' ? '>2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n' + bulk('v') : ok),
    {},
    async (c) => {
      const pushes: unknown[] = [];
      c.on('push', (d: unknown) => pushes.push(d));
      assert.equal(await c.get('k'), 'v');
      assert.deepEqual(pushes, [['invalidate', ['k']]]);
    },
  );
});

test('set: IFGT and IFLT go before the expiry', async () => {
  await withClient(
    () => ok,
    {},
    async (c, s) => {
      assert.equal(await c.set('wm', 5, { ifGt: true, px: 9 }), 'OK');
      assert.deepEqual(s.seen.at(-1), ['SET', 'wm', '5', 'IFGT', 'PX', '9']);
      assert.equal(await c.set('wm', -5, { ifLt: true }), 'OK');
      assert.deepEqual(s.seen.at(-1), ['SET', 'wm', '-5', 'IFLT']);
    },
  );
});

// ---- Lock / Lease / Permit ------------------------------------------------------------------

test('lock: retries until free, renew/release/isReleased, dispose, withLock cleanup and timeout', async () => {
  let taken = 2; // the first two NX attempts find the key held
  await withClient(
    (a) => {
      if (a[0] === 'SET' && a.includes('NX')) return taken-- > 0 ? '$-1\r\n' : ok;
      if (a[0] === 'SET') return ok; // renew (IFEQ)
      if (a[0] === 'DELEX') return int(1);
      return ok;
    },
    {},
    async (c, s) => {
      const lock = await c.acquireLock('job', 5000, { waitMs: 1000, retryMs: 5, token: 'tok' });
      assert.ok(lock !== null);
      assert.equal(s.count('SET'), 3, 'two refusals, then the grant');
      assert.deepEqual(s.seen[2], ['SET', 'job', 'tok', 'PX', '5000', 'NX']);
      assert.equal(await lock.renew(), true);
      assert.deepEqual(s.seen.at(-1), ['SET', 'job', 'tok', 'IFEQ', 'tok', 'PX', '5000']);
      assert.equal(await lock.renew(9000), true);
      assert.deepEqual(s.seen.at(-1)!.slice(-2), ['PX', '9000']);
      assert.equal(lock.isReleased, false);
      assert.equal(await lock.release(), true);
      assert.equal(lock.isReleased, true);
      assert.equal(await lock.release(), false, 'second release does nothing');
      assert.equal(await lock.renew(), false, 'a released lock cannot be renewed');
      assert.equal(s.count('DELEX'), 1);

      const l2 = await c.acquireLock('job2', 1000);
      assert.ok(l2);
      await l2[Symbol.asyncDispose]();
      assert.equal(l2.isReleased, true);

      // withLock releases even when fn throws
      await assert.rejects(
        c.withLock('job3', 1000, async () => {
          throw new Error('inside');
        }),
        /inside/,
      );
      assert.equal(s.count('DELEX'), 3);
      assert.equal(await c.withLock('job4', 1000, async (l) => l.key + '!'), 'job4!');

      // a held lock: null without waiting, AcquireTimeoutError from withLock
      taken = Infinity;
      assert.equal(await c.acquireLock('busy', 1000), null);
      const t0 = Date.now();
      assert.equal(await c.acquireLock('busy', 1000, { waitMs: 60, retryMs: 10 }), null);
      assert.ok(Date.now() - t0 >= 55);
      const e = await rejects(c.withLock('busy', 1000, async () => 1));
      assert.ok(e instanceof AcquireTimeoutError);
      assert.equal(e.name, 'AcquireTimeoutError');
      assert.match(e.message, /busy/);
    },
  );
});

test('lease and semaphore: waiting loops, release once, dispose', async () => {
  let leases = 2;
  let slots = 1;
  await withClient(
    (a) => {
      if (a[0] === 'LEASE') return leases-- > 0 ? '$-1\r\n' : int(41);
      if (a[0] === 'SEMAPHORE') return slots-- > 0 ? '$-1\r\n' : bulk('permit-1');
      if (a[0] === 'RELEASE') return int(1);
      return ok;
    },
    {},
    async (c, s) => {
      assert.equal(await c.acquireLease('l', 1000), null, 'no wait: one try');
      const lease = await c.acquireLease('l', 1000, { waitMs: 500, retryMs: 5 });
      assert.ok(lease);
      assert.equal(lease.token, 41);
      assert.equal(s.count('LEASE'), 3);
      assert.equal(await lease.release(), true);
      assert.equal(await lease.release(), false);
      assert.equal(s.count('RELEASE'), 1);
      const lease2 = await c.acquireLease('l', 1000);
      await lease2![Symbol.asyncDispose]();
      assert.equal(s.count('RELEASE'), 2);

      assert.equal(await c.acquireSemaphore('s', 1, 1000), null);
      const permit = await c.acquireSemaphore('s', 1, 1000, { waitMs: 500, retryMs: 5 });
      assert.ok(permit);
      assert.equal(permit.token, 'permit-1');
      assert.equal(await permit.release(), true);
      assert.equal(await permit.release(), false);
      slots = 0;
      const p2 = await c.acquireSemaphore('s', 1, 1000);
      await p2![Symbol.asyncDispose]();
      assert.equal(s.count('RELEASE'), 4);
    },
  );
});

test('rateLimit maps LIMIT replies', async () => {
  let next: string = int(4);
  await withClient((a) => (a[0] === 'LIMIT' ? next : ok), {}, async (c) => {
    assert.deepEqual(await c.rateLimit('k', 5, 1000), { allowed: true, remaining: 4 });
    next = '$-1\r\n';
    assert.deepEqual(await c.rateLimit('k', 5, 1000), { allowed: false, remaining: 0 });
  });
});

test('limit modes send their keywords and read their replies', async () => {
  let next: string = int(3);
  const sent: string[][] = [];
  await withClient(
    (a) => {
      if (a[0] !== 'LIMIT') return ok;
      sent.push(a);
      return next;
    },
    {},
    async (c) => {
      assert.equal(await c.limitSliding('k', 5, 1000), 3);
      assert.deepEqual(sent.pop(), ['LIMIT', 'k', '5', '1000', 'SLIDING']);
      assert.equal(await c.limitTokens('k', 10, 2000, 2), 3);
      assert.deepEqual(sent.pop(), ['LIMIT', 'k', '10', '2000', 'TOKENS', 'COST', '2']);
      assert.equal(await c.limitTokens('k', 10, 2000), 3);
      assert.deepEqual(sent.pop(), ['LIMIT', 'k', '10', '2000', 'TOKENS', 'COST', '1']);
      next = '$-1\r\n';
      assert.equal(await c.limitSliding('k', 5, 1000), null);

      next = '*2\r\n:2\r\n:0\r\n';
      assert.deepEqual(await c.limitSlidingWithRetry('k', 5, 1000), { allowed: true, remaining: 2, retryAfterMs: 0 });
      assert.deepEqual(sent.pop(), ['LIMIT', 'k', '5', '1000', 'SLIDING', 'WITHRETRY']);
      next = '*2\r\n:-1\r\n:250\r\n';
      assert.deepEqual(await c.limitTokensWithRetry('k', 10, 2000), { allowed: false, remaining: 0, retryAfterMs: 250 });
      assert.deepEqual(sent.pop(), ['LIMIT', 'k', '10', '2000', 'TOKENS', 'COST', '1', 'WITHRETRY']);
    },
  );
});

// ---- getOrCreate / idempotent ---------------------------------------------------------------

test('getOrCreate: hit, fill (with and without stale window), stale with and without refresher, busy timeout', async () => {
  let reply = '';
  await withClient(
    (a) => {
      if (a[0] === 'FILLGET') return reply;
      if (a[0] === 'FILLSET') return int(1);
      return ok;
    },
    {},
    async (c, s) => {
      reply = arr(bulk('HIT'), bulk('cached'));
      assert.equal(await c.getOrCreate('k', 1000, () => 'never'), 'cached');

      reply = arr(bulk('FILL'), int(7));
      assert.equal(await c.getOrCreate('k', 1000, async () => 'built'), 'built');
      assert.deepEqual(s.seen.at(-1), ['FILLSET', 'k', 'built', '1000', '7']);
      assert.equal(await c.getOrCreate('k', 1000, () => Buffer.from('buf'), { staleMs: 50 }), 'buf');
      assert.deepEqual(s.seen.at(-1), ['FILLSET', 'k', 'buf', '1000', '7', 'SWR', '50']);

      // stale + token: the old value is returned at once, the refresh runs in the background
      reply = arr(bulk('STALE'), bulk('old'), int(9));
      let refreshed = false;
      assert.equal(
        await c.getOrCreate('k', 1000, () => {
          refreshed = true;
          return 'fresh';
        }),
        'old',
      );
      // The refresh runs in the background: wait for its FILLSET.
      await until(() => refreshed && s.seen.at(-1)?.[0] === 'FILLSET', 'the background FILLSET');
      assert.deepEqual(s.seen.at(-1), ['FILLSET', 'k', 'fresh', '1000', '9']);
      // a failing background refresh is swallowed
      assert.equal(
        await c.getOrCreate('k', 1000, () => {
          throw new Error('refresh failed');
        }),
        'old',
      );
      await sleep(10);
      // stale without token: somebody else refreshes
      reply = arr(bulk('STALE'), bulk('old2'));
      const before = s.count('FILLSET');
      assert.equal(await c.getOrCreate('k', 1000, () => 'never'), 'old2');
      await sleep(10);
      assert.equal(s.count('FILLSET'), before);

      // busy for good: gives up after waitMs
      reply = arr(bulk('BUSY'), '$-1\r\n');
      const t0 = Date.now();
      const e = await rejects(c.getOrCreate('k', 1000, () => 'x', { waitMs: 60 }));
      assert.ok(e instanceof AcquireTimeoutError && /getOrCreate timed out/.test(e.message));
      assert.ok(Date.now() - t0 >= 55);
      // busy, then the value shows up
      let calls = 0;
      s.seen.length = 0;
      await withClient(
        (a) => (a[0] === 'FILLGET' ? (++calls < 4 ? arr(bulk('BUSY'), '$-1\r\n') : arr(bulk('HIT'), bulk('arrived'))) : ok),
        {},
        async (c2) => {
          assert.equal(await c2.getOrCreate('k', 1000, () => 'never'), 'arrived');
          assert.equal(calls, 4);
        },
      );
    },
  );
});

test('idempotent: executed, replayed, conflict, pending without and with a wait', async () => {
  let state = 'acquired';
  let result: string | null = null;
  await withClient(
    (a) => {
      if (a[0] === 'IDEM' && a[2] === 'BEGIN') return result === null ? arr(bulk(state)) : arr(bulk(state), bulk(result));
      if (a[0] === 'IDEM') return int(1);
      return ok;
    },
    {},
    async (c, s) => {
      const r = await c.idempotent('k', 'fp', 1000, async () => 'done', { owner: 'me' });
      assert.deepEqual(r, { result: 'done', replayed: false });
      assert.deepEqual(s.seen.at(-1), ['IDEM', 'k', 'COMPLETE', 'fp', 'me', 'done']);
      state = 'completed';
      result = 'stored';
      assert.deepEqual(await c.idempotent('k', 'fp', 1000, () => 'never'), { result: 'stored', replayed: true });
      result = null;
      assert.deepEqual(await c.idempotent('k', 'fp', 1000, () => 'never'), { result: '', replayed: true });
      state = 'conflict';
      const e = await rejects(c.idempotent('k', 'other', 1000, () => 'never'));
      assert.ok(e instanceof IdempotencyConflictError && e.name === 'IdempotencyConflictError');
      state = 'pending';
      const p = await rejects(c.idempotent('k', 'fp', 1000, () => 'never'));
      assert.ok(p instanceof IdempotencyPendingError && p.name === 'IdempotencyPendingError');
      // pending, then completed by the other owner while we wait
      const attempts = s.count('IDEM');
      setTimeout(() => {
        state = 'completed';
        result = 'by-other';
      }, 40);
      assert.deepEqual(await c.idempotent('k', 'fp', 1000, () => 'never', { waitMs: 1000, retryMs: 5 }), { result: 'by-other', replayed: true });
      assert.ok(s.count('IDEM') > attempts + 1, 'it polled');
      // still pending when the wait ends
      state = 'pending';
      result = null;
      const t0 = Date.now();
      await assert.rejects(c.idempotent('k', 'fp', 1000, () => 'never', { waitMs: 50, retryMs: 5 }), IdempotencyPendingError);
      assert.ok(Date.now() - t0 >= 45);
    },
  );
});

// ---- iterators and small helpers ------------------------------------------------------------

test('scan iterators walk every page', async () => {
  const pages = (name: string, items: string[][]) => {
    let i = 0;
    return (a: string[]) => {
      if (a[0] !== name) return ok;
      const page = items[i++]!;
      return arr(bulk(i < items.length ? String(i) : '0'), arr(...page.map(bulk)));
    };
  };
  const mk = (name: string, items: string[][]): Handler => pages(name, items);
  await withClient(mk('SCAN', [['a', 'b'], ['c'], []]), {}, async (c, s) => {
    const out: string[] = [];
    for await (const k of c.scanIterator({ match: '*', count: 2 })) out.push(k);
    assert.deepEqual(out, ['a', 'b', 'c']);
    assert.deepEqual(s.seen.map((a) => a[1]), ['0', '1', '2']);
    assert.deepEqual(s.seen[0], ['SCAN', '0', 'MATCH', '*', 'COUNT', '2']);
  });
  await withClient(mk('SSCAN', [['m1'], ['m2']]), {}, async (c) => {
    const out: string[] = [];
    for await (const m of c.sscanIterator('s')) out.push(m);
    assert.deepEqual(out, ['m1', 'm2']);
  });
  await withClient(mk('HSCAN', [['f1', 'v1'], ['f2', 'v2']]), {}, async (c) => {
    const out: [string, string][] = [];
    for await (const e of c.hscanIterator('h')) out.push(e);
    assert.deepEqual(out, [['f1', 'v1'], ['f2', 'v2']]);
  });
  await withClient(mk('ZSCAN', [['m1', '1'], ['m2', '2.5']]), {}, async (c) => {
    const out: unknown[] = [];
    for await (const e of c.zscanIterator('z')) out.push(e);
    assert.deepEqual(out, [{ member: 'm1', score: 1 }, { member: 'm2', score: 2.5 }]);
  });
  await withClient(mk('SCAN', [['x', 'y']]), {}, async (c) => {
    const it = c.scanIterator();
    assert.deepEqual((await it.next()).value, 'x');
    await it.return(undefined); // breaking out early leaves nothing dangling
    assert.deepEqual(await c.ping(), 'OK');
  });
});

test('seenBefore inverts BF.MADD; pushRecent/recent/mightContain use the underlying commands', async () => {
  await withClient(
    (a) => {
      if (a[0] === 'BF.MADD') return arr(int(1), int(0), int(1));
      if (a[0] === 'BF.MEXISTS') return arr(int(0), int(1));
      if (a[0] === 'LPUSHCAP') return int(3);
      if (a[0] === 'LRANGE') return arr(bulk('new'), bulk('old'));
      return ok;
    },
    {},
    async (c, s) => {
      assert.deepEqual(await c.seenBefore('bf', 'a', 'b', 'c'), [false, true, false]);
      assert.deepEqual(await c.mightContain('bf', 'a', 'b'), [false, true]);
      assert.equal(await c.pushRecent('r', 10, 'x', 'y'), 3);
      assert.deepEqual(s.seen.at(-1), ['LPUSHCAP', 'r', '10', 'x', 'y']);
      assert.deepEqual(await c.recent('r', 2), ['new', 'old']);
      assert.deepEqual(s.seen.at(-1), ['LRANGE', 'r', '0', '1']);
    },
  );
});

test('client misc: binary view is cached and shares the connection; isolated runs on its own socket; subscriber options', async () => {
  await withClient((a) => (a[0] === 'GET' ? bulk('héllo') : ok), { name: 'main' }, async (c, s) => {
    assert.equal(c.binary, c.binary);
    assert.deepEqual(await c.binary.get('k'), Buffer.from('héllo'));
    assert.equal(await c.get('k'), 'héllo');
    const seenAddrs = await c.isolated(async (d) => {
      assert.notEqual(d, c);
      return d.get('k');
    });
    assert.equal(seenAddrs, 'héllo');
    assert.equal(s.seen.filter((a) => a[0] === 'CLIENT').length, 2, 'the isolated client opened (and handshook) its own socket');
    await assert.rejects(
      c.isolated(async () => {
        throw new Error('fn failed');
      }),
      /fn failed/,
    );
    const sub = c.subscriber({ reconnectDelayMs: 7 });
    assert.equal((sub as unknown as { baseDelay: number }).baseDelay, 7);
    assert.equal((sub as unknown as { conn: { opts: { name: string; port: number } } }).conn.opts.name, 'main');
    sub.close();
    assert.equal(c.inFlight, 0);
    await c[Symbol.asyncDispose]();
    assert.equal(internals(c).conn.isClosed, true);
  });
});

// ---- pipelines and transactions -------------------------------------------------------------

test('pipeline: errors stay in their slots; execStrict throws the first; a throwing transform lands in its slot', async () => {
  await withClient(
    (a) => {
      if (a[0] === 'GET') return err('WRONGTYPE nope');
      if (a[0] === 'FILLGET') return arr(bulk('NONSENSE'), bulk('x'));
      if (a[0] === 'INCR') return int(3);
      return ok;
    },
    {},
    async (c) => {
      const p = c.pipeline();
      assert.equal(p.length, 0);
      p.incr('n').get('k').fillget('k', 5).set('a', '1');
      assert.equal(p.length, 4);
      const res = await p.exec();
      assert.equal(p.length, 0, 'exec drains the queue');
      assert.equal(res[0], 3);
      assert.ok(res[1] instanceof RuvioError && (res[1] as RuvioError).code === 'WRONGTYPE');
      assert.ok(res[2] instanceof ReplyError);
      assert.equal(res[3], 'OK');
      const strict = c.pipeline().incr('n').get('k');
      await assert.rejects(strict.execStrict(), (e: unknown) => e instanceof RuvioError && e.code === 'WRONGTYPE');
      assert.deepEqual(await c.pipeline().incr('n').incr('n').execStrict(), [3, 3]);
      assert.deepEqual(await c.pipeline().exec(), []);
    },
  );
});

test('pipeline: an unencodable argument throws at queue time and leaves the rest intact', async () => {
  await withClient((a) => (a[0] === 'EXEC' ? '*1\r\n+OK\r\n' : ok), {}, async (c, s) => {
    const p = c.pipeline().set('a', '1');
    assert.throws(() => p.set('b', Number.NaN as unknown as string));
    p.set('c', '3');
    assert.equal(p.length, 2);
    assert.deepEqual(await p.exec(), ['OK', 'OK']);
    assert.deepEqual(s.seen.filter((a) => a[0] === 'SET').map((a) => a[1]), ['a', 'c']);
    const t = c.multi().set('a', '1');
    assert.throws(() => t.set('b', Number.NaN as unknown as string));
    t.discard();
    assert.deepEqual(await t.set('z', '1').exec().then((r) => r?.length), 1);
    // the same on the client itself: a rejected promise, and the connection stays usable
    const e = await rejects(c.set('k', Number.NaN as unknown as string));
    assert.ok(e instanceof Error);
    assert.equal(await c.ping(), 'OK');
  });
});

test('pipeline: a dropped connection rejects exec() once for every slot', async () => {
  await withClient((a, sock) => (a[1] === 'die' ? (sock.destroy(), null) : ok), {}, async (c) => {
    const e = await rejects(c.pipeline().set('a', '1').get('die').get('c').exec());
    assert.ok(e instanceof ConnectionError);
    assert.equal(await c.ping(), 'OK');
  });
});

test('transaction: results, WATCH abort, MULTI refusal, EXECABORT with queue errors, short EXEC replies, transform failures', async () => {
  let mode = 'ok';
  await withClient(
    (a) => {
      switch (a[0]) {
        case 'MULTI':
          return mode === 'multi-err' ? err('ERR MULTI calls can not be nested') : ok;
        case 'SET':
          return mode === 'queue-err' || mode === 'short' ? err('ERR syntax error') : '+QUEUED\r\n';
        case 'GET':
        case 'FILLGET':
        case 'INCR':
          return '+QUEUED\r\n';
        case 'EXEC':
          if (mode === 'abort') return '*-1\r\n';
          if (mode === 'queue-err') return err('EXECABORT Transaction discarded because of previous errors.');
          if (mode === 'abort-nil') return '*-1\r\n';
          if (mode === 'short') return arr(bulk('v'));
          if (mode === 'tf') return arr(arr(bulk('NONSENSE'), bulk('x')), int(7));
          return arr(ok.trim() === '+OK' ? '+OK\r\n' : '', bulk('v'));
      }
      return ok;
    },
    {},
    async (c) => {
      const r = await c.multi().set('a', '1').get('a').exec();
      assert.deepEqual(r, ['OK', 'v']);

      mode = 'abort-nil';
      assert.equal(await c.multi().set('a', '1').exec(), null, 'a watched key changed');

      mode = 'multi-err';
      const e1 = await rejects(c.multi().set('a', '1').exec());
      assert.ok(e1 instanceof RuvioError && /nested/.test(e1.message));

      mode = 'queue-err';
      const e2 = await rejects(c.multi().set('a', '1').set('b', '2').exec());
      assert.ok(e2 instanceof RuvioError && e2.code === 'EXECABORT');
      assert.match(e2.message, /previous errors\. \(ERR syntax error; ERR syntax error\)/);
      assert.equal((e2 as unknown as { queueErrors: unknown[] }).queueErrors.length, 2);

      mode = 'short';
      // SET refused at queue time, GET queued: EXEC answers only for the GET
      const r3 = await c.multi().set('a', '1').get('a').exec();
      assert.ok(r3![0] instanceof RuvioError, 'the refusal keeps its slot');
      assert.equal(r3![1], 'v');

      mode = 'tf';
      const r4 = await c.multi().fillget('k', 1).incr('n').exec();
      assert.ok(r4![0] instanceof ReplyError, 'a transform failure is that command\'s result');
      assert.equal(r4![1], 7);
    },
  );
});

test('a transaction whose EXEC reply is not an array is rejected instead of hanging', async () => {
  await withClient(
    () => ok,
    {},
    async (c) => {
      const e = await rejects(c.multi().set('a', '1').exec());
      assert.ok(e instanceof ReplyError && /EXEC returned an unexpected reply/.test(e.message));
    },
  );
});
