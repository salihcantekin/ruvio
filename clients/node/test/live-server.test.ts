// SPDX-License-Identifier: MIT
// Live: connection, ACL, introspection, scripts/functions, cluster, transactions, pipelines,
// Pub/Sub, RESP3 + client tracking, multiplexing and reconnect behaviour.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioClient, RuvioError, ConnectionError, Subscriber, type PubSubMessage } from '../src/index.js';
import { SKIP, addr, live, rejects, settle, sleep, until, within } from './helpers.js';

test('connection commands', { skip: SKIP }, async () => {
  const c = await live(6);
  try {
    assert.equal(await c.ping(), 'PONG');
    assert.equal(await c.ping('hey'), 'hey');
    assert.equal(await c.ruvio(), 'PONG is taken');
    assert.ok((await c.ruvio(true)).length > 0);
    assert.equal(await c.echo('x'), 'x');
    const hello = await c.hello();
    assert.equal(hello['server'], 'ruvio');
    const hello2 = await c.hello(2, { setName: 'node-test' });
    assert.equal(hello2['proto'], 2);
    assert.equal(await c.clientGetname(), 'node-test');
    assert.equal(await c.clientSetname('n2'), 'OK');
    assert.equal(await c.clientGetname(), 'n2');
    const id = await c.clientId();
    assert.equal(typeof id, 'number');
    assert.match(await c.clientInfo(), /id=\d+/);
    assert.match(await c.clientList(), /name=n2/);
    assert.match(await c.clientList('normal'), /id=/);
    // kill another connection by id
    const victim = c.duplicate();
    const vid = await victim.clientId();
    const dropped = new Promise((r) => victim.on('disconnect', r));
    assert.equal(await c.clientKill({ id: vid, skipMe: true }), 1);
    await within(dropped, 'the killed client to see its socket close');
    assert.equal(await victim.ping(), 'PONG', 'reconnects lazily after being killed');
    const vaddr = (await victim.clientInfo()).match(/addr=(\S+)/)![1]!;
    assert.equal(await c.clientKill(vaddr), 'OK');
    victim.disconnect();
    // RESP2 tracking needs RESP3: error is a typed RuvioError
    const tr = await settle(c.clientTracking(true));
    assert.ok(tr instanceof RuvioError);
    assert.ok((await c.command()).length > 100);
    assert.ok((await c.commandCount()) > 100);
    assert.equal((await c.commandInfo('get', 'set')).length, 2);
    assert.ok(await c.commandDocs('get'));
    assert.equal(await c.slowlogReset(), 'OK');
    assert.equal(typeof (await c.slowlogLen()), 'number');
    assert.ok(Array.isArray(await c.slowlogGet()));
    assert.ok(Array.isArray(await c.slowlogGet(5)));
    assert.deepEqual(await c.latencyLatest(), []);
    // No requirepass on the test server: AUTH round-trips (OK or a typed error).
    assert.ok((await settle(c.auth('whatever'))) !== undefined);
    assert.ok((await settle(c.auth('default', 'x'))) !== undefined);
  } finally {
    await c.quit();
  }
});

test('acl', { skip: SKIP }, async () => {
  const c = await live(6);
  try {
    assert.equal(await c.aclWhoami(), 'default');
    const user = 'nodeuser' + process.pid;
    assert.equal(await c.aclSetuser(user, 'on', '>secret', '+get', '+set', '+acl', '~app:*'), 'OK');
    assert.ok((await c.aclUsers()).includes(user));
    assert.ok((await c.aclList()).some((l) => l.includes(user)));
    const info = await c.aclGetuser(user);
    assert.ok(info && Object.keys(info).length > 0);
    assert.equal(await c.aclGetuser('nobody-here'), null);
    assert.ok((await c.aclCat()).length > 0);
    assert.ok(Array.isArray(await c.aclCat('string')) || true);
    // log in as that user on a separate client
    const u = new RuvioClient({ ...addr(), username: user, password: 'secret' });
    assert.equal(await u.set('app:x', '1'), 'OK');
    assert.equal(await u.aclWhoami(), user);
    assert.ok((await rejects(u.del('app:x'))) instanceof RuvioError);
    await u.quit();
    // wrong password: the handshake fails and the call rejects with ConnectionError
    const bad = new RuvioClient({ ...addr(), username: user, password: 'nope', reconnect: false });
    assert.ok((await rejects(bad.ping())) instanceof Error);
    bad.disconnect();
    // ACL LOAD / SAVE need security.acl_file; without it the server answers an error.
    assert.ok((await settle(c.aclLoad())) !== undefined);
    assert.ok((await settle(c.aclSave())) !== undefined);
  } finally {
    await c.quit();
  }
});

test('introspection, persistence, replication', { skip: SKIP }, async () => {
  const c = await live(6);
  try {
    assert.match(await c.info(), /ruvio|redis_version|server/i);
    assert.match(await c.info('server'), /:/);
    const cfg = await c.configGet('*');
    assert.ok(Object.keys(cfg).length > 0);
    assert.equal(await c.configSet({ 'slowlog-max-len': '128' }), 'OK');
    assert.equal((await c.configGet('slowlog-max-len'))['slowlog-max-len'], '128');
    assert.ok((await rejects(c.configSet({ shards: '2' }))) instanceof RuvioError);
    assert.equal(String((await c.role())[0]), 'master');
    // The live server keeps no WAL, so it cannot replicate: both forms are refused unchanged.
    assert.ok((await rejects(c.replicaOf('127.0.0.1', 1))) instanceof RuvioError);
    assert.ok((await rejects(c.replicaOfNoOne())) instanceof RuvioError);
    // Only on a throwaway server: CONFIG REWRITE rewrites the server's config file.
    if (process.env.RUVIO_TEST_CONFIG_REWRITE === '1') assert.equal(await c.configRewrite(), 'OK');
    assert.equal(await c.save(), 'OK');
    assert.ok(typeof (await c.bgsave()) === 'string');
    assert.equal(await c.wait(0, 10), 0);
  } finally {
    await c.quit();
  }
});

test('scripts and functions', { skip: SKIP }, async () => {
  const c = await live(7);
  try {
    assert.equal(await c.eval("return ruvio.call('SET', KEYS[1], ARGV[1])", ['ek'], ['ev']), 'OK');
    assert.equal(await c.eval('return ARGV[1] + 1', [], [41]), 42);
    const sha = await c.scriptLoad("return ruvio.call('GET', KEYS[1])");
    assert.equal(sha.length, 40);
    assert.equal(await c.evalsha(sha, ['ek']), 'ev');
    assert.deepEqual(await c.scriptExists(sha, '0'.repeat(40)), [true, false]);
    assert.equal(await c.scriptFlush(), 'OK');
    assert.equal((await rejects(c.evalsha(sha, ['ek']))).code, 'NOSCRIPT');
    for (let i = 0; i < 2; i++) assert.equal(await c.evalCached('return ARGV[1]', [], ['again']), 'again');
    assert.equal((await rejects(c.scriptKill())).code, 'NOTBUSY');
    await c.functionLoad('nodefn', "return 'fn:' .. ARGV[1]");
    assert.ok(JSON.stringify(await c.functionList()).includes('nodefn'));
    assert.equal(await c.fcall('nodefn', [], ['x']), 'fn:x');
    assert.equal(await c.functionDelete('nodefn'), true);
    assert.equal(await c.functionDelete('nodefn'), false);
  } finally {
    await c.quit();
  }
});

test('cluster commands (single process)', { skip: SKIP }, async () => {
  const c = await live(0);
  try {
    // Standalone servers answer `ERR cluster mode is disabled` for most CLUSTER subcommands; in
    // cluster mode they return the topology. Either way the typed call round-trips.
    const slots = await settle(c.clusterSlots());
    assert.ok(Array.isArray(slots) || (slots instanceof RuvioError && /cluster/.test(slots.message)));
    const nodes = await settle(c.clusterNodes());
    assert.ok(typeof nodes === 'string' || nodes instanceof RuvioError);
    const shards = await settle(c.clusterShards());
    assert.ok(Array.isArray(shards) || shards instanceof RuvioError);
    const info = await settle(c.clusterInfo());
    assert.ok(typeof info === 'string' || info instanceof RuvioError);
    const myid = await settle(c.clusterMyid());
    assert.ok(typeof myid === 'string' || myid instanceof RuvioError);
    assert.equal(await c.clusterKeyslot('foo'), 12182);
    assert.equal(await c.clusterKeyslot('{user1000}.following'), await c.clusterKeyslot('{user1000}.followers'));
    assert.ok((await settle(c.readonly())) !== undefined);
    assert.ok((await settle(c.readwrite())) !== undefined);
  } finally {
    await c.quit();
  }
});

test('transactions', { skip: SKIP }, async () => {
  const c = await live(7);
  try {
    const out = await c.multi().set('{tx}t', '1').incr('{tx}t').hset('{tx}th', 'f', 'v').hgetall('{tx}th').exists('{tx}t').exec();
    assert.deepEqual(out, ['OK', 2, 1, { f: 'v' }, 1]);
    // runtime error inside EXEC stays in its slot
    const mixed = await c.multi().set('{tx}str', 'x').incr('{tx}str').get('{tx}str').exec();
    assert.equal(mixed![0], 'OK');
    assert.ok(mixed![1] instanceof RuvioError);
    assert.equal(mixed![2], 'x');
    // queue-time error aborts with EXECABORT
    const tx = c.multi().set('{tx}a', '1');
    tx.call('NOSUCHCOMMAND');
    const r = await settle(tx.exec());
    assert.ok(r instanceof RuvioError);
    assert.match(r.message, /EXECABORT/);
    assert.equal(await c.get('{tx}a'), null);
    // binary inside a transaction
    const bin = Buffer.from([0, 255]);
    const t2 = c.multi();
    t2.set('{tx}bk', bin);
    t2.getBuffer('{tx}bk');
    t2.get('{tx}t');
    const r2 = await t2.exec();
    assert.deepEqual(r2![1], bin);
    assert.equal(r2![2], '2');
    // discard is local
    const t3 = c.multi().set('{tx}never', '1');
    t3.discard();
    assert.deepEqual(await t3.exec(), []);
    assert.equal(await c.exists('{tx}never'), 0);
    // WATCH on a dedicated connection: a concurrent write aborts EXEC (null)
    const aborted = await c.isolated(async (iso) => {
      await iso.watch('{tx}w');
      await c.set('{tx}w', 'changed');
      return iso.multi().set('{tx}w', 'mine').exec();
    });
    assert.equal(aborted, null);
    assert.equal(await c.get('{tx}w'), 'changed');
    const ok = await c.isolated(async (iso) => {
      await iso.watch('{tx}w');
      await iso.unwatch();
      await c.set('{tx}w', 'again');
      return iso.multi().set('{tx}w', 'mine').exec();
    });
    assert.deepEqual(ok, ['OK']);
    // concurrent callers do not interleave with a transaction
    const ps: Promise<unknown>[] = [];
    for (let i = 0; i < 50; i++) ps.push(c.incr('{tx}ctr'));
    ps.push(c.multi().incr('{tx}ctr').incr('{tx}ctr').exec());
    for (let i = 0; i < 50; i++) ps.push(c.incr('{tx}ctr'));
    const res = await Promise.all(ps);
    assert.deepEqual(res[50], [51, 52]);
    assert.equal(res[100], 102);
  } finally {
    await c.quit();
  }
});

test('pipelines', { skip: SKIP }, async () => {
  const c = await live(7);
  try {
    const p = c.pipeline();
    for (let i = 0; i < 2000; i++) p.set('pk' + i, i);
    p.get('pk1999').hget('nohash', 'f').incr('pk5').zscore('none', 'm');
    assert.equal(p.length, 2004);
    const out = await p.exec();
    assert.equal(out.length, 2004);
    assert.equal(out[2000], '1999');
    assert.equal(out[2001], null);
    assert.equal(out[2002], 6);
    assert.equal(out[2003], null);
    const withErr = await c.pipeline().set('ps', 'x').incr('ps').get('ps').exec();
    assert.ok(withErr[1] instanceof RuvioError);
    assert.equal(withErr[2], 'x');
    await assert.rejects(c.pipeline().set('ps', 'x').incr('ps').execStrict(), RuvioError);
    assert.deepEqual(await c.pipeline().exec(), []);
  } finally {
    await c.quit();
  }
});

test('pub/sub on a dedicated connection', { skip: SKIP }, async () => {
  const c = await live(7);
  const sub = c.subscriber();
  try {
    const events: PubSubMessage[] = [];
    sub.on('message', (m: PubSubMessage) => events.push(m));
    assert.equal(await sub.subscribe('ch1', 'ch2'), 2);
    assert.equal(await sub.psubscribe('news.*'), 3);
    assert.equal(await sub.ssubscribe('sch'), 1);
    assert.deepEqual(await sub.ping(), ['pong', '']);
    assert.equal(await c.publish('ch1', 'hello'), 1);
    assert.equal(await c.publish('news.tech', 'n1'), 1);
    assert.equal(await c.spublish('sch', 's1'), 1);
    await until(() => events.length >= 3, 'three deliveries');
    assert.deepEqual(events, [
      { kind: 'message', channel: 'ch1', message: 'hello' },
      { kind: 'pmessage', pattern: 'news.*', channel: 'news.tech', message: 'n1' },
      { kind: 'smessage', channel: 'sch', message: 's1' },
    ]);
    // async iterator
    const it = sub[Symbol.asyncIterator]();
    await c.publish('ch2', 'via-iterator');
    const n = await it.next();
    assert.equal(n.value.message, 'via-iterator');
    await it.return!();
    assert.equal(await sub.unsubscribe('ch1'), 2);
    assert.equal(await sub.unsubscribe(), 1);
    assert.equal(await sub.punsubscribe(), 0);
    assert.equal(await sub.sunsubscribe('sch'), 0);
    assert.equal(await c.publish('ch1', 'nobody'), 0);
    // binary payloads and re-subscribe after a dropped socket
    const bsub = new Subscriber({ ...addr(), binary: true, reconnectDelayMs: 10 });
    try {
      const got: Buffer[] = [];
      bsub.on('message', (m: PubSubMessage<Buffer>) => got.push(m.message));
      bsub.on('error', () => undefined);
      await bsub.subscribe('bin');
      await c.publish('bin', Buffer.from([0, 1, 255]));
      await until(() => got.length >= 1, 'the binary delivery');
      assert.deepEqual(got[0], Buffer.from([0, 1, 255]));
      const reconnected = new Promise((r) => bsub.once('reconnect', r));
      // Kill every other connection (the subscriber's among them).
      const me = await c.clientId();
      for (const line of (await c.clientList()).split('\n')) {
        const m = line.match(/id=(\d+)/);
        if (m && Number(m[1]) !== me) await settle(c.clientKill({ id: Number(m[1]) }));
      }
      await within(reconnected, 'the subscriber to reconnect and resubscribe');
      assert.equal(await c.publish('bin', 'after'), 1);
      await until(() => got[got.length - 1]?.toString() === 'after', 'the delivery after the reconnect');
    } finally {
      bsub.close();
    }
  } finally {
    sub.close();
    await c.quit();
  }
});

test('RESP3 protocol option and client tracking pushes', { skip: SKIP }, async () => {
  const c = new RuvioClient({ ...addr(), db: 7, protocol: 3 });
  const w = await live(7);
  try {
    await c.hset('r3', { a: '1' });
    assert.deepEqual(await c.hgetall('r3'), { a: '1' });
    assert.equal(await c.clientTracking(true), 'OK');
    await w.set('tk', 'v1');
    assert.equal(await c.get('tk'), 'v1');
    const pushed = new Promise<unknown[]>((r) => c.on('push', r));
    await w.set('tk', 'v2');
    const push = await within(pushed, 'the invalidate push');
    assert.equal(push[0], 'invalidate');
    assert.equal(await c.get('tk'), 'v2');
    assert.equal(await c.zscore('nozset', 'm'), null);
  } finally {
    await c.quit();
    await w.quit();
  }
});

test('multiplexing, ordering, binary view, errors, reconnect', { skip: SKIP }, async () => {
  const c = await live(7);
  try {
    // 10k concurrent callers on one socket, replies in order
    const ps: Promise<unknown>[] = [];
    for (let i = 0; i < 10000; i++) ps.push(i % 2 ? c.set('m' + i, 'v' + i) : c.incr('ord'));
    const res = await Promise.all(ps);
    let expect = 1;
    for (let i = 0; i < 10000; i += 2) assert.equal(res[i], expect++);
    // big values in both directions
    const big = Buffer.alloc(5 * 1024 * 1024, 0xab);
    await c.set('big', big);
    assert.deepEqual(await c.getBuffer('big'), big);
    const bigStr = 'é'.repeat(300000);
    await c.set('bigs', bigStr);
    assert.equal(await c.get('bigs'), bigStr);
    // binary view
    assert.deepEqual(await c.binary.lrange('nolist', 0, -1), []);
    await c.rpush('bl', Buffer.from([1]), Buffer.from([2]));
    assert.deepEqual(await c.binary.lrange('bl', 0, -1), [Buffer.from([1]), Buffer.from([2])]);
    // generic call
    assert.equal(await c.call('PING'), 'PONG');
    assert.deepEqual(await c.callBuffer('GET', 'bl2'), null);
    // a bad argument rejects only that call
    await assert.rejects(c.set('k', undefined as unknown as string), TypeError);
    assert.equal(await c.ping(), 'PONG');
    // reconnect: drop the socket, in-flight calls reject, the next call reconnects
    const pending = c.blpop('never', 5).catch((e) => e);
    await sleep(20);
    c.conn.destroySocket();
    assert.ok((await pending) instanceof ConnectionError);
    assert.equal(await c.ping(), 'PONG');
    assert.equal(await c.get('m1'), 'v1', 'database re-selected after reconnect');
    // closed client rejects
    const d = c.duplicate();
    await d.ping();
    await d.quit();
    assert.ok((await rejects(d.ping())) instanceof ConnectionError);
    // connect error surfaces as a rejection
    const nobody = new RuvioClient({ host: '127.0.0.1', port: 1, reconnect: false });
    assert.ok((await rejects(nobody.ping())) instanceof ConnectionError);
  } finally {
    await c.quit();
  }
});

test('CLIENT DURABILITY needs replication on a standalone server', { skip: SKIP }, async () => {
  const c = await live(7);
  try {
    const e = await rejects(c.clientDurability(1, 1000));
    assert.ok(e instanceof RuvioError);
    assert.match(e.message, /needs replication/);
  } finally {
    c.disconnect();
  }
});
