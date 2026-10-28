// SPDX-License-Identifier: MIT
// Live: strings, Ruvio coordination primitives, keys/keyspace, bitmaps, HyperLogLog.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioError } from '../src/index.js';
import { SKIP, live, rejects, settle, sleep } from './helpers.js';

test('strings', { skip: SKIP }, async () => {
  const c = await live(1);
  try {
    assert.equal(await c.set('s', 'v'), 'OK');
    assert.equal(await c.get('s'), 'v');
    assert.equal(await c.get('missing'), null);
    assert.equal(await c.set('s', 'w', { nx: true }), null);
    assert.equal(await c.set('s', 'w', { xx: true, px: 10000 }), 'OK');
    assert.equal(await c.set('s', 'x', { get: true }), 'w');
    assert.equal(await c.set('s', 'y', { keepTtl: true }), 'OK');
    assert.equal(await c.set('s2', 'v', { ex: 100 }), 'OK');
    assert.equal(await c.set('s3', 'v', { exat: Math.floor(Date.now() / 1000) + 100 }), 'OK');
    assert.equal(await c.set('s4', 'v', { pxat: Date.now() + 100000 }), 'OK');
    // SET IFEQ
    assert.equal(await c.set('s', 'z', { ifEq: 'nope' }), null);
    assert.equal(await c.set('s', 'z', { ifEq: 'y', px: 5000 }), 'OK');
    // SET IFGT / IFLT
    assert.equal(await c.set('wm', 10, { ifGt: true }), 'OK');
    assert.equal(await c.set('wm', 10, { ifGt: true }), null);
    assert.equal(await c.set('wm', 11, { ifGt: true, px: 5000 }), 'OK');
    assert.equal(await c.set('wm', 12, { ifLt: true }), null);
    assert.equal(await c.set('wm', -1, { ifLt: true }), 'OK');
    assert.equal(await c.get('wm'), '-1');
    assert.equal(await c.get('s'), 'z');
    // binary safety
    const bin = Buffer.from([0, 1, 2, 255, 13, 10]);
    await c.set('b', bin);
    assert.deepEqual(await c.getBuffer('b'), bin);
    assert.deepEqual(await c.binary.get('b'), bin);
    assert.deepEqual(await c.mgetBuffer('b', 'nokey'), [bin, null]);
    // unicode
    await c.set('u', 'ğüşıöç 😀');
    assert.equal(await c.get('u'), 'ğüşıöç 😀');
    assert.equal(await c.mset({ m1: 'a', m2: 'b' }), 'OK');
    assert.equal(await c.mset([['m3', 'c']]), 'OK');
    assert.deepEqual(await c.mget('m1', 'm2', 'm3', 'none'), ['a', 'b', 'c', null]);
    assert.equal(await c.msetnx({ m1: 'x', m9: 'y' }), false);
    assert.equal(await c.msetnx({ m8: 'x', m9: 'y' }), true);
    assert.equal(await c.getset('m1', 'aa'), 'a');
    assert.equal(await c.append('m1', 'bb'), 4);
    assert.equal(await c.strlen('m1'), 4);
    assert.equal(await c.incr('n'), 1);
    assert.equal(await c.decr('n'), 0);
    assert.equal(await c.incrby('n', 10), 10);
    assert.equal(await c.decrby('n', 3), 7);
    assert.equal(await c.incrbyMax('n', 2, 10), 9);
    assert.equal(await c.incrbyMax('n', 2, 10), null);
    assert.equal(await c.get('n'), '9');
    assert.equal(await c.incrbyfloat('f', 1.5), 1.5);
    assert.equal(await c.setex('ex', 100, 'v'), 'OK');
    assert.ok((await c.ttl('ex')) > 90);
    assert.equal(await c.psetex('pex', 100000, 'v'), 'OK');
    assert.ok((await c.pttl('pex')) > 90000);
    assert.equal(await c.setnx('nx', 'a'), true);
    assert.equal(await c.setnx('nx', 'b'), false);
    assert.equal(await c.getdel('nx'), 'a');
    assert.equal(await c.exists('nx'), 0);
    await c.set('r', 'Hello World');
    assert.equal(await c.getrange('r', 0, 4), 'Hello');
    assert.equal(await c.substr('r', 6, -1), 'World');
    assert.equal(await c.setrange('r', 6, 'Ruvio'), 11);
    assert.equal(await c.get('r'), 'Hello Ruvio');
    // GETEX (ms)
    assert.equal(await c.getex('r', 50000), 'Hello Ruvio');
    const pt = await c.pttl('r');
    assert.ok(pt > 40000 && pt <= 50000);
    assert.equal(await c.getex('nokey', 1000), null);
    // DELEX IFEQ
    assert.equal(await c.delex('r', 'other'), false);
    assert.equal(await c.delex('r', 'Hello Ruvio'), true);
    assert.equal(await c.exists('r'), 0);
    // SETV
    assert.equal(await c.setv('ver', 'a'), 1);
    assert.equal(await c.setv('ver', 'b', 1), 2);
    assert.ok((await rejects(c.setv('ver', 'c', 1))) instanceof RuvioError);
    assert.equal(await c.get('ver'), 'b');
    // TAKE
    await c.set('stock', 10);
    assert.equal(await c.take('stock', 3), 7);
    assert.equal(await c.take('stock', 30), null);
    assert.equal(await c.take('nostock', 1), null);

    await c.set('{w}a', 10);
    assert.deepEqual(await c.transfer('{w}a', '{w}b', 4), [6, 4]);
    assert.equal(await c.transfer('{w}a', '{w}b', 7), null);
    assert.equal(await c.transfer('{w}a', '{w}b', 3, 6), null);
    assert.deepEqual(await c.transfer('{w}a', '{w}b', 2, 6), [4, 6]);
    await assert.rejects(c.transfer('{w}a', '{w}a', 1));
    await c.hset('th', { f: 10 });
    assert.deepEqual(await c.htransfer('th', 'f', 'g', 4), [6, 4]);
    assert.equal(await c.htransfer('th', 'f', 'g', 3, 5), null);
    // errors
    await c.hset('h', 'f', 'v');
    const e = await rejects(c.get('h'));
    assert.ok(e instanceof RuvioError);
    assert.equal(e.code, 'WRONGTYPE');
  } finally {
    await c.quit();
  }
});

test('FILLGET / FILLSET and getOrCreate', { skip: SKIP }, async () => {
  const c = await live(1);
  try {
    const first = await c.fillget('fk', 2000);
    assert.equal(first.state, 'fill');
    assert.ok(first.token);
    const busy = await c.fillget('fk', 2000);
    assert.equal(busy.state, 'busy');
    assert.equal(await c.fillset('fk', 'val', 10000, first.token!), true);
    assert.deepEqual(await c.fillget('fk', 2000), { state: 'hit', value: 'val', token: null });
    assert.equal(await c.fillset('fk', 'val', 10000, '999999'), false);
    // SWR: fresh 50 ms, then stale for 10 s
    const f2 = await c.fillget('swr', 2000);
    assert.equal(await c.fillset('swr', 'old', 50, f2.token!, 10000), true);
    await sleep(80);
    const st = await c.fillget('swr', 2000);
    assert.equal(st.state, 'stale');
    assert.equal(st.value, 'old');
    // WAIT form on a dedicated connection: filler is the waiter itself (marker lapses).
    const d = c.duplicate();
    const w1 = await d.fillget('wk', 100);
    assert.equal(w1.state, 'fill');
    const w2 = await d.fillget('wk', 100, 300);
    assert.ok(['fill', 'busy'].includes(w2.state));
    await d.quit();

    // getOrCreate: 20 concurrent callers, the factory runs once.
    let runs = 0;
    const factory = async () => {
      runs++;
      await sleep(30);
      return 'built';
    };
    const results = await Promise.all(Array.from({ length: 20 }, () => c.getOrCreate('goc', 10000, factory)));
    assert.equal(runs, 1);
    assert.ok(results.every((r) => r === 'built'));
    assert.equal(await c.getOrCreate('goc', 10000, factory), 'built');
    assert.equal(runs, 1);
    // stale-while-revalidate through the helper
    assert.equal(await c.getOrCreate('goc2', 50, () => 'v1', { staleMs: 10000 }), 'v1');
    await sleep(80);
    assert.equal(await c.getOrCreate('goc2', 10000, () => 'v2', { staleMs: 10000 }), 'v1');
    // The refresh runs in the background: poll for it.
    for (const end = Date.now() + 10_000; (await c.get('goc2')) !== 'v2' && Date.now() < end; ) await sleep(10);
    assert.equal(await c.get('goc2'), 'v2');
  } finally {
    await c.quit();
  }
});

test('LIMIT, ONCE, LEASE, SEMAPHORE, RELEASE, IDEM, CHANGES and their helpers', { skip: SKIP }, async () => {
  const c = await live(2);
  try {
    assert.equal(await c.limit('lim', 2, 5000), 1);
    assert.equal(await c.limit('lim', 2, 5000), 0);
    assert.equal(await c.limit('lim', 2, 5000), null);
    assert.deepEqual(await c.rateLimit('rl', 1, 5000), { allowed: true, remaining: 0 });
    assert.deepEqual(await c.rateLimit('rl', 1, 5000), { allowed: false, remaining: 0 });
    assert.equal(await c.limitSliding('sl', 2, 60000), 1);
    assert.equal(await c.limitSliding('sl', 2, 60000), 0);
    const refused = await c.limitSlidingWithRetry('sl', 2, 60000);
    assert.ok(!refused.allowed && refused.remaining === 0 && refused.retryAfterMs > 0);
    assert.equal(await c.limitTokens('tk', 5, 60000, 2), 3);
    assert.deepEqual(await c.limitTokensWithRetry('tk', 5, 60000, 3), { allowed: true, remaining: 0, retryAfterMs: 0 });
    assert.equal(await c.limitTokens('tk', 5, 60000), null);

    assert.equal(await c.once('o', 5000), true);
    assert.equal(await c.once('o', 5000), false);

    const t1 = await c.lease('le', 5000);
    assert.equal(typeof t1, 'number');
    assert.equal(await c.lease('le', 5000), null);
    assert.equal(await c.release('le', t1! + 100), false);
    assert.equal(await c.renew('le', t1! + 100, 5000), false);
    assert.equal(await c.renew('le', t1!, 9000), true);
    assert.equal(await c.release('le', t1!), true);
    assert.equal(await c.renew('le', t1!, 9000), false);
    const lease = await c.acquireLease('le', 5000);
    assert.ok(lease && lease.token > t1!);
    assert.equal(await c.acquireLease('le', 5000), null);
    await lease!.release();
    {
      await using l2 = await c.acquireLease('le', 5000);
      assert.ok(l2);
    }
    assert.ok(await c.lease('le', 5000), 'released by await using');

    const s1 = await c.semaphore('sem', 2, 5000);
    const s2 = await c.semaphore('sem', 2, 5000);
    assert.ok(s1 && s2 && s1 !== s2);
    assert.equal(await c.semaphore('sem', 2, 5000), null);
    assert.equal(await c.renew('sem', s1!, 9000), true);
    assert.equal(await c.semaphore('sem', 2, 5000), null);
    assert.equal(await c.release('sem', s1!), true);
    const permit = await c.acquireSemaphore('sem', 2, 5000);
    assert.ok(permit);
    assert.equal(await c.acquireSemaphore('sem', 2, 5000, { waitMs: 60, retryMs: 20 }), null);
    assert.equal(await permit!.release(), true);
    assert.equal(await permit!.release(), false);

    // IDEM
    assert.deepEqual(await c.idemBegin('idem', 'fp', 'me', 10000), { state: 'acquired', result: null });
    assert.equal((await c.idemBegin('idem', 'fp', 'me', 10000)).state, 'pending');
    assert.equal((await c.idemBegin('idem', 'other', 'me', 10000)).state, 'conflict');
    assert.equal(await c.idemComplete('idem', 'fp', 'me', 'result-1'), 1);
    assert.deepEqual(await c.idemBegin('idem', 'fp', 'me', 10000), { state: 'completed', result: 'result-1' });
    let calls = 0;
    const r1 = await c.idempotent('idem2', 'fp', 10000, async () => {
      calls++;
      return 'charged';
    });
    const r2 = await c.idempotent('idem2', 'fp', 10000, async () => {
      calls++;
      return 'charged-again';
    });
    assert.deepEqual(r1, { result: 'charged', replayed: false });
    assert.deepEqual(r2, { result: 'charged', replayed: true });
    assert.equal(calls, 1);
    await assert.rejects(c.idempotent('idem2', 'fp-other', 10000, () => 'x'), { name: 'IdempotencyConflictError' });

    // ABORT frees a pending record, never a completed one, and only for its owner
    assert.equal((await c.idemBegin('idem-abort', 'fp', 'o1', 10000)).state, 'acquired');
    assert.equal(await c.idemAbort('idem-abort', 'fp', 'o2'), false);
    assert.equal(await c.idemAbort('idem-abort', 'other', 'o1'), false);
    assert.equal(await c.idemAbort('idem-abort', 'fp', 'o1'), true);
    assert.equal((await c.idemBegin('idem-abort', 'fp', 'o2', 10000)).state, 'acquired');
    assert.equal(await c.idemComplete('idem-abort', 'fp', 'o2', 'kept'), 1);
    assert.equal(await c.idemAbort('idem-abort', 'fp', 'o2'), false);
    assert.deepEqual(await c.idemBegin('idem-abort', 'fp', 'o3', 10000), { state: 'completed', result: 'kept' });
    // the helper flag frees the key after a failing fn; without it the key stays pending
    const boom = async (): Promise<string> => {
      throw new Error('boom');
    };
    await assert.rejects(c.idempotent('idem-flag', 'fp', 10000, boom, { abortOnError: true }), { message: 'boom' });
    assert.deepEqual(await c.idempotent('idem-flag', 'fp', 10000, () => 'again'), { result: 'again', replayed: false });
    await assert.rejects(c.idempotent('idem-keep', 'fp', 10000, boom), { message: 'boom' });
    await assert.rejects(c.idempotent('idem-keep', 'fp', 10000, () => 'x'), { name: 'IdempotencyPendingError' });

    // CHANGES
    const cur = await c.changesStart();
    await c.set('watched', '1');
    const notes = await c.changes(cur);
    assert.ok(notes.length >= 1);
    assert.equal(typeof notes[0]!.seq, 'number');
    assert.ok(notes.some((n) => n.note.includes('watched')));
  } finally {
    await c.quit();
  }
});

test('distributed lock helper', { skip: SKIP }, async () => {
  const c = await live(2);
  try {
    const lock = await c.acquireLock('lock', 5000);
    assert.ok(lock);
    assert.equal(await c.acquireLock('lock', 5000), null);
    assert.equal(await c.acquireLock('lock', 5000, { waitMs: 60, retryMs: 20 }), null);
    assert.equal(await lock!.renew(10000), true);
    assert.ok((await c.pttl('lock')) > 5000);
    // someone else's lock is never deleted
    await c.set('lock', 'stolen');
    assert.equal(await lock!.renew(), false);
    assert.equal(await lock!.release(), false);
    assert.equal(await c.get('lock'), 'stolen');
    await c.del('lock');
    {
      await using l = await c.acquireLock('lock', 5000);
      assert.ok(l);
      assert.equal(await c.exists('lock'), 1);
    }
    assert.equal(await c.exists('lock'), 0, 'await using released it');
    const v = await c.withLock('lock', 5000, async () => 42);
    assert.equal(v, 42);
    assert.equal(await c.exists('lock'), 0);
  } finally {
    await c.quit();
  }
});

test('keys and keyspace', { skip: SKIP }, async () => {
  const c = await live(3);
  try {
    await c.mset({ a: '1', b: '2', c: '3', 'p:1': 'x', 'p:2': 'y' });
    assert.equal(await c.exists('a', 'b', 'zz'), 2);
    assert.equal(await c.type('a'), 'string');
    assert.equal(await c.del('a'), 1);
    assert.equal(await c.unlink('b', 'zz'), 1);
    assert.equal(await c.rename('c', 'c2'), 'OK');
    assert.equal(await c.renamenx('c2', 'p:1'), false);
    assert.equal(await c.dbsize(), 3);
    const page = await c.scan(0, { match: 'p:*', count: 100 });
    assert.equal(typeof page.cursor, 'string');
    const all: string[] = [];
    for await (const k of c.scanIterator({ match: 'p:*', count: 1 })) all.push(k);
    assert.deepEqual(all.sort(), ['p:1', 'p:2']);
    assert.deepEqual((await c.keys('p:*')).sort(), ['p:1', 'p:2']);
    assert.ok(await c.randomkey());
    assert.equal(await c.touch('p:1', 'p:2', 'none'), 2);
    assert.equal(await c.expire('p:1', 100), true);
    assert.equal(await c.pexpire('p:1', 200000), true);
    const now = Math.floor(Date.now() / 1000);
    assert.equal(await c.expireat('p:2', now + 1000), true);
    assert.ok(Math.abs((await c.expiretime('p:2')) - (now + 1000)) <= 1);
    assert.equal(await c.pexpireat('p:2', (now + 2000) * 1000), true);
    assert.equal(await c.pexpiretime('p:2'), (now + 2000) * 1000);
    assert.ok((await c.ttl('p:2')) > 1000);
    assert.equal(await c.persist('p:2'), true);
    assert.equal(await c.ttl('p:2'), -1);
    assert.equal(await c.copy('p:2', 'p:3'), true);
    assert.equal(await c.copy('p:2', 'p:3'), false);
    assert.equal(await c.copy('p:2', 'p:3', { replace: true }), true);
    assert.equal(await c.objectEncoding('p:2'), 'embstr');
    assert.ok((await settle(c.objectIdletime('p:2'))) !== undefined);
    assert.ok((await settle(c.objectFreq('p:2'))) !== undefined);
    assert.ok(((await c.memoryUsage('p:2')) ?? 0) > 0);
    assert.ok(((await c.memoryUsage('p:2', 5)) ?? 0) > 0);
    const payload = await c.dump('p:2');
    assert.ok(Buffer.isBuffer(payload));
    assert.equal(await c.restore('p:9', 0, payload!), 'OK');
    assert.equal(await c.get('p:9'), 'y');
    assert.equal(await c.restore('p:9', 0, payload!, { replace: true, absTtl: false, idleTime: 1, freq: 1 }), 'OK');
    assert.equal(await c.move('p:9', 4), true);
    assert.equal(await c.exists('p:9'), 0);
    const d4 = c.duplicate({ db: 4 });
    assert.equal(await d4.get('p:9'), 'y');
    await d4.flushdb();
    await d4.quit();
    // SWAPDB between two scratch databases
    const d5 = c.duplicate({ db: 5 });
    await d5.flushdb();
    await d5.set('only5', '1');
    assert.equal(await c.swapdb(5, 6), 'OK');
    assert.equal(await d5.exists('only5'), 0);
    await c.swapdb(5, 6);
    assert.equal(await d5.exists('only5'), 1);
    await d5.flushdb(true);
    await d5.quit();
    assert.equal(await c.flushdb(), 'OK');
    assert.equal(await c.dbsize(), 0);
    // SELECT is remembered across reconnects
    const s = c.duplicate();
    await s.select(7);
    await s.set('sel', '7');
    s.conn.destroySocket();
    await sleep(10);
    assert.equal(await s.get('sel'), '7');
    await s.flushdb();
    await s.quit();
  } finally {
    await c.quit();
  }
});

test('FLUSHALL on an isolated server database set', { skip: SKIP }, async () => {
  // FLUSHALL clears every database of this test server; run it on a client that owns nothing else.
  const c = await live(15);
  try {
    await c.set('x', '1');
    assert.equal(await c.flushall(), 'OK');
    assert.equal(await c.dbsize(), 0);
  } finally {
    await c.quit();
  }
});

test('bitmaps and hyperloglog', { skip: SKIP }, async () => {
  const c = await live(8);
  try {
    assert.equal(await c.setbit('bm', 7, 1), 0);
    assert.equal(await c.getbit('bm', 7), 1);
    assert.equal(await c.bitcount('bm'), 1);
    assert.equal(await c.bitcount('bm', 0, 0), 1);
    assert.equal(await c.bitcount('bm', 0, 7, 'BIT'), 1);
    assert.equal(await c.bitpos('bm', 1), 7);
    assert.equal(await c.bitpos('bm', 1, 0, 0, 'BYTE'), 7);
    await c.set('b1', 'ab');
    await c.set('b2', 'AB');
    assert.equal(await c.bitop('AND', 'b3', 'b1', 'b2'), 2);
    assert.deepEqual(await c.bitfield('bf', 'SET', 'u8', 0, 200, 'GET', 'u8', 0, 'OVERFLOW', 'SAT', 'INCRBY', 'u8', 0, 100), [0, 200, 255]);
    assert.deepEqual(await c.bitfield('bf', 'OVERFLOW', 'FAIL', 'INCRBY', 'u8', 0, 1), [null]);
    assert.deepEqual(await c.bitfieldRo('bf', ['u8', 0], ['u4', 0]), [255, 15]);
    assert.equal(await c.pfadd('hll', 'a', 'b', 'c'), true);
    assert.equal(await c.pfadd('hll', 'a'), false);
    await c.pfadd('hll2', 'c', 'd');
    assert.equal(await c.pfcount('hll'), 3);
    assert.equal(await c.pfcount('hll', 'hll2'), 4);
    assert.equal(await c.pfmerge('hll3', 'hll', 'hll2'), 'OK');
    assert.equal(await c.pfcount('hll3'), 4);
  } finally {
    await c.quit();
  }
});

test('ttl on create: the deadline is set once and kept', { skip: SKIP }, async () => {
  const c = await live(1);
  try {
    const within = async (key: string, lo: number, hi: number) => {
      const t = await c.pttl(key);
      assert.ok(t > lo && t <= hi, `${key} pttl ${t}`);
    };
    await c.incr('t:incr', { ttl: 100 });
    await within('t:incr', 90_000, 100_000);
    await c.incr('t:incr', { ttl: 5000 }); // existing key keeps its deadline
    await within('t:incr', 90_000, 100_000);
    await c.decr('t:decr', { ttl: 100 });
    await within('t:decr', 90_000, 100_000);
    assert.equal(await c.incrby('t:by', 3, { ttl: 100 }), 3);
    assert.equal(await c.decrby('t:dby', 3, { ttl: 0.5 }), -3);
    await within('t:dby', 0, 500);
    assert.equal(await c.incrbyMax('t:max', 2, 10, { ttl: 100 }), 2);
    await within('t:max', 90_000, 100_000);
    assert.equal(await c.incrbyfloat('t:f', 1.5, { ttl: 100 }), 1.5);
    await within('t:f', 90_000, 100_000);
    assert.equal(await c.setbit('t:bit', 7, 1, { ttl: 100 }), 0);
    await within('t:bit', 90_000, 100_000);
    await c.setbit('t:bit', 8, 1, { ttl: 5000 });
    await within('t:bit', 90_000, 100_000);
    // a non-positive TTL never reaches the server
    assert.ok((await rejects(c.incr('t:bad', { ttl: 0 }))) instanceof RangeError);
    assert.equal(await c.exists('t:bad'), 0);
  } finally {
    await c.quit();
  }
});

test('EXPIRE conditions', { skip: SKIP }, async () => {
  const c = await live(1);
  try {
    await c.set('e', 'v');
    assert.equal(await c.expire('e', 100, 'XX'), false); // no deadline yet
    assert.equal(await c.expire('e', 100, 'LT'), true); // no deadline counts as infinite
    assert.equal(await c.expire('e', 100, 'NX'), false); // has a deadline now
    assert.equal(await c.expire('e', 50, 'GT'), false);
    assert.equal(await c.expire('e', 200, 'GT'), true);
    assert.equal(await c.pexpire('e', 300_000, 'GT'), true);
    assert.equal(await c.pexpire('e', 400_000, 'LT'), false);
    const now = Date.now();
    assert.equal(await c.pexpireat('e', now + 100_000, 'LT'), true);
    assert.equal(await c.expireat('e', Math.floor(now / 1000) + 500, 'LT'), false);
    assert.equal(await c.expireat('e', Math.floor(now / 1000) + 50, 'LT'), true);
    assert.equal(await c.expire('missing', 10, 'NX'), false);
    const t = await c.pttl('e');
    assert.ok(t > 0 && t <= 51_000, `pttl ${t}`);
  } finally {
    await c.quit();
  }
});
