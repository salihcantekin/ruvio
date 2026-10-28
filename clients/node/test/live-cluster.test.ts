// SPDX-License-Identifier: MIT
// Live cluster routing against a 3-shard server (RUVIO_CLUSTER_ADDR = first shard's address), and the
// standalone fallback against a plain server (RUVIO_TEST_ADDR).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClusterClient, RuvioError, hashSlot, createClusterClient, type PubSubMessage } from '../src/index.js';
import { _forceStaleTable } from '../src/cluster.js';
import { CLUSTER_ADDR, CSKIP, SKIP, addr, clusterAddr, rejects, sleep, until, within } from './helpers.js';

async function cluster(): Promise<ClusterClient> {
  const c = createClusterClient(clusterAddr());
  await c.connect();
  await c.flushall();
  return c;
}

/** Keys that share nothing: spread over many slots. */
const spread = (n: number, prefix = 'key:') => Array.from({ length: n }, (_, i) => prefix + i);

test('cluster: topology, slot math against CLUSTER KEYSLOT, keys spread over all shards', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    assert.ok(c.isCluster);
    assert.equal(c.nodes.length, 3);
    assert.deepEqual(c.nodes.map((n) => n.port), [0, 1, 2].map((i) => clusterAddr().port + i));
    for (const k of ['foo', 'bar', '{tag}x', 'key:77', 'ключ', 'a{b}c', '{}x']) {
      assert.equal(hashSlot(k), await c.clusterKeyslot(k), k);
    }
    const ks = spread(300);
    await Promise.all(ks.map((k, i) => c.set(k, 'v' + i)));
    const got = await Promise.all(ks.map((k) => c.get(k)));
    assert.deepEqual(got, ks.map((_, i) => 'v' + i));
    // every shard holds some, and together exactly all
    const sizes = await Promise.all(c.nodes.map((n) => n.client.dbsize()));
    assert.ok(sizes.every((s) => s > 0), 'every shard got keys: ' + sizes);
    assert.equal(sizes.reduce((a, b) => a + b, 0), 300);
    assert.equal(await c.dbsize(), 300, 'DBSIZE fans out and sums');
    // each key sits on the node the slot table names
    for (const k of ks.slice(0, 20)) {
      assert.equal(await c.nodeFor(k).client.exists(k), 1);
    }
    assert.equal(c.redirectCount, 0, 'a fresh table never needs MOVED');
    assert.ok((await c.keys('key:1*')).length > 10, 'KEYS fans out');
    assert.deepEqual(await c.binary.get('key:3'), Buffer.from('v3'));
  } finally {
    await c.quit();
  }
});

test('cluster: cross-slot MGET / MSET / DEL / UNLINK / EXISTS / TOUCH split and reassemble in caller order', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const ks = spread(60, 'm:');
    const slots = new Set(ks.map(hashSlot));
    assert.ok(slots.size > 20);
    await c.mset(Object.fromEntries(ks.map((k, i) => [k, 'val' + i])));
    assert.equal(await c.dbsize(), 60);
    // reversed and shuffled order, with a missing key and a duplicate in the middle
    const order = [...ks].reverse();
    order.splice(7, 0, 'm:missing');
    order.splice(20, 0, ks[3]!);
    const r = await c.mget(...order);
    assert.deepEqual(r, order.map((k) => (k === 'm:missing' ? null : 'val' + ks.indexOf(k))));
    assert.deepEqual(await c.binary.mget(...order.slice(0, 3)), order.slice(0, 3).map((k) => Buffer.from('val' + ks.indexOf(k))));
    assert.equal(await c.exists(...ks, 'm:missing'), 60);
    assert.equal(await c.touch(...ks), 60);
    assert.equal(await c.del(...ks.slice(0, 20)), 20);
    assert.equal(await c.unlink(...ks.slice(20, 40), 'm:missing'), 20);
    assert.equal(await c.exists(...ks), 20);
    assert.equal(await c.del(...ks), 20);
    assert.equal(await c.dbsize(), 0);
    // same-slot (hashtag) multi-key goes out as one command
    await c.mset({ '{t}a': '1', '{t}b': '2' });
    assert.deepEqual(await c.mget('{t}a', '{t}b'), ['1', '2']);
    // commands that cannot be split fail fast, nothing sent
    const a = ks[0]!, b = ks.find((k) => hashSlot(k) !== hashSlot(a))!;
    const e = await rejects(c.sunion(a, b));
    assert.equal(e.code, 'CROSSSLOT');
    assert.equal((await rejects(c.rename(a, b))).code, 'CROSSSLOT');
    assert.equal((await rejects(c.msetnx({ [a]: '1', [b]: '2' }))).code, 'CROSSSLOT');
    assert.equal((await rejects(c.bitop('AND', a, b))).code, 'CROSSSLOT');
    assert.equal(await c.dbsize(), 2, 'the refused commands wrote nothing');
    assert.equal(await c.sunionstore('{t}d', '{t}x', '{t}y'), 0);
  } finally {
    await c.quit();
  }
});

test('cluster: multi-key commands routed by their real key lists', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    await c.sadd('{s}1', 'a', 'b');
    await c.sadd('{s}2', 'b', 'c');
    assert.deepEqual((await c.sinter('{s}1', '{s}2')).sort(), ['b']);
    assert.equal(await c.sintercard(['{s}1', '{s}2']), 1);
    assert.equal(await c.sunionstore('{s}u', '{s}1', '{s}2'), 3);
    await c.zadd('{z}1', 1, 'a');
    await c.zadd('{z}2', 2, 'a');
    assert.equal(await c.zunionstore('{z}d', ['{z}1', '{z}2']), 1);
    assert.deepEqual((await c.zunion(['{z}1', '{z}2'])).length, 1);
    await c.set('{b}1', 'x');
    assert.equal(await c.bitop('OR', '{b}d', '{b}1'), 1);
    assert.equal(await c.rename('{b}1', '{b}2'), 'OK');
    assert.equal(await c.lpush('{l}1', 'v'), 1);
    assert.equal(await c.lmove('{l}1', '{l}2', 'LEFT', 'RIGHT'), 'v');
    assert.equal(await c.eval("return ruvio.call('GET', KEYS[1])", ['{b}2'], []), 'x');
    assert.equal(await c.eval('return 7', [], []), 7, 'keyless script runs on any node');
    assert.deepEqual(await c.lmpop(['{l}9', '{l}2'], 'LEFT'), { key: '{l}2', elements: ['v'] });
  } finally {
    await c.quit();
  }
});

test('cluster: pipelines group by node and keep the caller order', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const ks = spread(90, 'p:');
    const p = c.pipeline();
    ks.forEach((k, i) => p.set(k, 'x' + i));
    ks.forEach((k) => p.get(k));
    ks.forEach((k) => p.incr('ctr:' + k));
    p.mget(...ks.slice(0, 10)); // cross-slot inside a pipeline: split
    p.sunion('{a}x', '{b}y'); // cross-slot, not splittable: an error in its own slot
    p.dbsize();
    p.ping();
    const r = await p.exec();
    assert.equal(r.length, 90 * 3 + 4);
    assert.deepEqual(r.slice(0, 90), ks.map(() => 'OK'));
    assert.deepEqual(r.slice(90, 180), ks.map((_, i) => 'x' + i));
    assert.deepEqual(r.slice(180, 270), ks.map(() => 1));
    assert.deepEqual(r[270], ks.slice(0, 10).map((_, i) => 'x' + i));
    assert.ok(r[271] instanceof RuvioError && (r[271] as RuvioError).code === 'CROSSSLOT');
    assert.equal(r[272], 180);
    assert.equal(r[273], 'PONG');
    await assert.rejects(c.pipeline().get('a').sunion('{a}x', '{b}y').execStrict(), RuvioError);
    assert.deepEqual(await c.pipeline().exec(), []);
    // the other split commands inside a pipeline: sent per slot, merged reply at their index
    const [s0, s1, s2] = spread(3, 'ps:');
    const sp = await c.pipeline().mset({ [s0!]: 'm0', [s1!]: 'm1' }).exists(s0!, s1!, s2!).get(s1!).touch(s0!, s2!).unlink(s0!, s2!).del(s1!, s2!).exec();
    assert.deepEqual(sp, ['OK', 2, 'm1', 1, 1, 1]);
    // a pipeline over a stale table still gets every answer: the server forwards a
    // misrouted key to its owner instead of answering MOVED
    const hot = ks.slice(0, 30);
    _forceStaleTable(c, c.nodes[0]!);
    const q = c.pipeline();
    hot.forEach((k) => q.get(k));
    assert.deepEqual(await q.exec(), hot.map((_, i) => 'x' + i));
  } finally {
    await c.quit();
  }
});

test('cluster: a stale slot table still gets the right answers', { skip: CSKIP }, async () => {
  // Ruvio forwards a misrouted key to its owner instead of answering MOVED, so nothing
  // below is redirected; MOVED handling itself is covered by the fake-server tests.
  const c = await cluster();
  try {
    const ks = spread(60, 'mv:');
    await Promise.all(ks.map((k, i) => c.set(k, 'v' + i)));
    _forceStaleTable(c, c.nodes[1]!); // every slot now "belongs" to the second shard
    const r0 = c.redirectCount;
    assert.equal(await c.get(ks[0]!), 'v0');
    const got = await Promise.all(ks.map((k) => c.get(k)));
    assert.deepEqual(got, ks.map((_, i) => 'v' + i));
    assert.equal(c.redirectCount, r0, 'nothing was redirected');
    // writes and split commands work on a stale table too
    _forceStaleTable(c, c.nodes[2]!);
    await Promise.all(ks.map((k) => c.set(k, 'w')));
    _forceStaleTable(c, c.nodes[0]!);
    assert.deepEqual(await c.mget(...ks.slice(0, 8)), ks.slice(0, 8).map(() => 'w'));
    assert.equal(await c.del(...ks), 60);
    // explicit refresh
    _forceStaleTable(c, c.nodes[0]!);
    await c.refreshSlots();
    const b = c.redirectCount;
    await c.get(ks[5]!);
    assert.equal(c.redirectCount, b);
  } finally {
    await c.quit();
  }
});

test('cluster: transactions need one slot', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const r = await c.multi().set('{tx}a', '1').incr('{tx}n').get('{tx}a').exec();
    assert.deepEqual(r, ['OK', 1, '1']);
    const e = await rejects(c.multi().set('{tx}a', '1').set('other-slot-key', '2').exec());
    assert.equal(e.code, 'CROSSSLOT');
    assert.equal(await c.get('other-slot-key'), null, 'nothing was sent');
    assert.deepEqual(await c.multi().ping().exec(), ['PONG'], 'keyless transaction runs on the seed');
    // WATCH on the owning node's own connection
    await c.set('{tx}w', '1');
    const out = await c.isolated('{tx}w', async (n) => {
      await n.watch('{tx}w');
      const t = n.multi().incr('{tx}w');
      return t.exec();
    });
    assert.deepEqual(out, [2]);
  } finally {
    await c.quit();
  }
});

test('cluster: helpers (lock, lease, semaphore, rate limit, getOrCreate, idempotency, recent, bloom, json) run unchanged', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    // lock
    const lock = await c.acquireLock('lock:a', 5000);
    assert.ok(lock);
    assert.equal(await c.acquireLock('lock:a', 5000), null);
    assert.equal(await lock!.renew(), true);
    assert.equal(await lock!.release(), true);
    let ran = 0;
    await c.withLock('lock:b', 5000, async () => {
      ran++;
    });
    assert.equal(ran, 1);
    // lease + semaphore
    const lease = await c.acquireLease('lease:a', 5000);
    assert.ok(lease);
    assert.equal(await c.acquireLease('lease:a', 5000), null);
    assert.equal(await lease!.release(), true);
    const p1 = await c.acquireSemaphore('sem:a', 2, 5000);
    const p2 = await c.acquireSemaphore('sem:a', 2, 5000);
    assert.ok(p1 && p2);
    assert.equal(await c.acquireSemaphore('sem:a', 2, 5000), null);
    await p1!.release();
    assert.ok(await c.acquireSemaphore('sem:a', 2, 5000));
    // rate limit
    const r1 = await c.rateLimit('rl:a', 2, 5000);
    const r2 = await c.rateLimit('rl:a', 2, 5000);
    const r3 = await c.rateLimit('rl:a', 2, 5000);
    assert.deepEqual([r1.allowed, r2.allowed, r3.allowed], [true, true, false]);
    // cache-aside with a stampede
    let calls = 0;
    const vals = await Promise.all(
      Array.from({ length: 8 }, () =>
        c.getOrCreate('cache:a', 5000, async () => {
          calls++;
          await sleep(30);
          return 'computed';
        }),
      ),
    );
    assert.deepEqual(vals, Array(8).fill('computed'));
    assert.equal(calls, 1);
    // idempotency
    let runs = 0;
    const i1 = await c.idempotent('idem:a', 'fp', 5000, () => (++runs, 'result'));
    const i2 = await c.idempotent('idem:a', 'fp', 5000, () => (++runs, 'result'));
    assert.deepEqual([i1.replayed, i2.replayed, i2.result, runs], [false, true, 'result', 1]);
    // recent items, bloom, json
    await c.pushRecent('recent:a', 3, '1', '2', '3', '4');
    assert.deepEqual(await c.recent('recent:a', 10), ['4', '3', '2']);
    assert.deepEqual(await c.seenBefore('bf:a', 'x', 'y'), [false, false]);
    assert.deepEqual(await c.seenBefore('bf:a', 'x', 'z'), [true, false]);
    await c.jsonSetAs('json:a', '$', { n: 1, s: ['a'] });
    assert.deepEqual(await c.jsonGetAs('json:a'), { n: 1, s: ['a'] });
    // ... and the keys really live on different shards
    const owners = new Set(['lock:a', 'lease:a', 'sem:a', 'rl:a', 'cache:a', 'idem:a', 'recent:a', 'bf:a', 'json:a'].map((k) => c.nodeFor(k).addr));
    assert.ok(owners.size >= 2);
  } finally {
    await c.quit();
  }
});

test('cluster: blocking commands use a dedicated connection to the owning node', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const key = '{blk}q';
    const popped = c.blpop([key], 5); // parks its own connection, not the shared one
    await sleep(50);
    // the shared connection keeps serving everything else, including the same node
    assert.equal(await c.set('{blk}other', '1'), 'OK');
    assert.equal(await c.ping(), 'PONG');
    const owner = c.nodeFor(key);
    await c.rpush(key, 'hello');
    assert.deepEqual(await popped, [key, 'hello']);
    assert.equal(owner.idle.length, 1, 'the dedicated connection is pooled for reuse');
    assert.equal(await c.blpop([key], 0.1), null, 'timeout answers nil');
    // two parked at once, on different nodes
    const k2 = Array.from({ length: 50 }, (_, i) => 'blk2:' + i).find((k) => c.nodeFor(k) !== owner)!;
    const [x, y] = [c.blpop([key], 5), c.blpop([k2], 5)];
    await sleep(30);
    await c.rpush(k2, 'two');
    await c.rpush(key, 'one');
    assert.deepEqual([await x, await y], [[key, 'one'], [k2, 'two']]);
    // XREAD BLOCK follows the same rule. It reads after a known ID rather than `$`: the XADD
    // may reach the node before the blocking read does, and `$` would then miss it.
    const first = (await c.xadd('{blk}s', '*', { f: 'first' }))!;
    const xr = c.xread({ '{blk}s': first }, { block: 10_000 });
    await sleep(30);
    await c.xadd('{blk}s', '*', { f: 'v' });
    assert.ok(await xr);
  } finally {
    await c.quit();
  }
});

test('cluster: SCAN iterates every node; KEYS / FLUSHDB fan out', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const ks = spread(250, 'sc:');
    await Promise.all(ks.map((k) => c.set(k, '1')));
    const seen: string[] = [];
    for await (const k of c.scanIterator({ count: 20 })) seen.push(k);
    assert.deepEqual(seen.sort(), [...ks].sort());
    const one = await c.scan('0');
    assert.ok(one.items.length <= 250, 'a plain scan() reaches a single node');
    assert.equal((await c.keys('sc:*')).length, 250);
    // per-key iterators route by key
    await c.sadd('{h}set', 'a', 'b', 'c');
    const members: string[] = [];
    for await (const m of c.sscanIterator('{h}set')) members.push(m);
    assert.deepEqual(members.sort(), ['a', 'b', 'c']);
    assert.equal(await c.flushdb(), 'OK');
    assert.equal(await c.dbsize(), 0);
  } finally {
    await c.quit();
  }
});

test('cluster: pub/sub on the seed or on the channel owner', { skip: CSKIP }, async () => {
  const c = await cluster();
  const sub = await c.subscriberFor('{ps}chan');
  const got: PubSubMessage<string>[] = [];
  try {
    sub.on('message', (m: PubSubMessage<string>) => got.push(m));
    await sub.ssubscribe('{ps}chan');
    assert.equal(await c.spublish('{ps}chan', 'hello'), 1);
    await until(() => got.length >= 1, 'the sharded delivery');
    assert.equal(got[0]?.message, 'hello');
    // plain SUBSCRIBE on the seed node, PUBLISH from the same node's shared connection
    const seed = c.subscriber();
    const seedGot: string[] = [];
    seed.on('message', (m: PubSubMessage<string>) => seedGot.push(m.message));
    await seed.subscribe('plain');
    await c.publish('plain', 'p1');
    await until(() => seedGot.length >= 1, 'the delivery on the seed');
    assert.deepEqual(seedGot, ['p1']);
    seed.close();
  } finally {
    sub.close();
    await c.quit();
  }
});

test('cluster: losing one node connection does not break the others', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const ks = spread(30, 'loss:');
    await Promise.all(ks.map((k) => c.set(k, 'v')));
    const victim = c.nodes[1]!;
    const id = await victim.client.clientId();
    const killer = c.nodes[0]!.client.duplicate();
    // kill the victim's shared connection from a separate connection to the same node
    const killerV = victim.client.duplicate();
    const dropped = new Promise((r) => victim.client.on('disconnect', r));
    assert.equal(await killerV.clientKill({ id, skipMe: true }), 1);
    killerV.disconnect();
    killer.disconnect();
    // The GETs below go out once the client has seen the close, so none rides the dead socket.
    await within(dropped, "the victim's connection to close");
    const results = await Promise.allSettled(ks.map((k) => c.get(k)));
    const fine = results.filter((r) => r.status === 'fulfilled').length;
    assert.ok(fine >= ks.length - 1, 'at most the one in-flight call per dead connection can fail');
    assert.deepEqual(await Promise.all(ks.map((k) => c.get(k))), ks.map(() => 'v'));
    const c2 = c.duplicate();
    assert.equal(await c2.get('loss:1'), 'v');
    await c2.quit();
  } finally {
    await c.quit();
  }
});

test('cluster client against a plain server falls back to standalone', { skip: SKIP }, async () => {
  const c = new ClusterClient({ ...addr(), db: 8 });
  try {
    await c.connect();
    assert.equal(c.isCluster, false);
    assert.equal(await c.flushdb(), 'OK');
    assert.equal(await c.set('a', '1'), 'OK');
    assert.deepEqual(await c.mget('a', 'b', 'nope'), ['1', null, null]);
    assert.equal(await c.mset({ x: '1', y: '2', zzz: '3' }), 'OK');
    assert.equal(await c.del('x', 'y', 'zzz'), 3);
    assert.deepEqual(await c.pipeline().set('k', 'v').get('k').exec(), ['OK', 'v']);
    assert.deepEqual(await c.multi().incr('n').incr('n').exec(), [1, 2]);
    // cross-slot multi-key works as on any standalone server
    assert.equal(await c.sadd('s1', 'a'), 1);
    assert.equal(await c.sadd('s2', 'a'), 1);
    assert.deepEqual(await c.sinter('s1', 's2'), ['a']);
    assert.equal((await c.acquireLock('lk', 1000))?.token.length, 32);
    assert.equal(await c.dbsize(), 6);
    const keys: string[] = [];
    for await (const k of c.scanIterator()) keys.push(k);
    assert.equal(keys.length, 6);
    assert.deepEqual(await c.binary.get('a'), Buffer.from('1'));
    assert.equal(await c.inFlight, 0);
  } finally {
    await c.flushdb().catch(() => undefined);
    await c.quit();
  }
  // a seed that is a cluster node is refused when `cluster: true` is asked of a standalone server
  const strict = new ClusterClient({ ...addr(), cluster: true });
  try {
    await assert.rejects(strict.get('a'), RuvioError);
  } finally {
    strict.disconnect();
  }
});

void CLUSTER_ADDR;

test('cluster: RUVIO MOVESLOTS STATUS answers and a range already in place is refused', { skip: CSKIP }, async () => {
  const c = await cluster();
  try {
    const status = await c.moveSlotsStatus();
    assert.ok('state' in status, JSON.stringify(status));
    // Slot 0 is on shard 0: nothing to move, and the server says why.
    await assert.rejects(c.moveSlots(0, 10, 0), /already on that shard/);
  } finally {
    await c.quit();
  }
});
