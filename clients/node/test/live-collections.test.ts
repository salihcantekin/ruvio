// SPDX-License-Identifier: MIT
// Live: lists, sets, hashes (incl. field TTLs), sorted sets, geo.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { SKIP, live } from './helpers.js';

test('lists', { skip: SKIP }, async () => {
  const c = await live(9);
  try {
    assert.equal(await c.rpush('l', 'a', 'b', 'c'), 3);
    assert.equal(await c.lpush('l', 'z'), 4);
    assert.deepEqual(await c.lrange('l', 0, -1), ['z', 'a', 'b', 'c']);
    assert.equal(await c.lpushx('nol', 'x'), 0);
    assert.equal(await c.rpushx('l', 'd'), 5);
    assert.equal(await c.llen('l'), 5);
    assert.equal(await c.lindex('l', 1), 'a');
    assert.equal(await c.lpop('l'), 'z');
    assert.deepEqual(await c.lpop('l', 2), ['a', 'b']);
    assert.equal(await c.rpop('l'), 'd');
    assert.deepEqual(await c.rpop('l', 5), ['c']);
    assert.equal(await c.lpop('l'), null);
    await c.rpush('l', '1', '2', '3', '2', '4');
    assert.equal(await c.ltrim('l', 0, 3), 'OK');
    assert.equal(await c.linsert('l', 'BEFORE', '3', '2.5'), 5);
    assert.equal(await c.lset('l', 0, 'one'), 'OK');
    assert.equal(await c.lrem('l', 0, '2'), 2);
    assert.deepEqual(await c.lrange('l', 0, -1), ['one', '2.5', '3']);
    assert.equal(await c.lpos('l', '3'), 2);
    assert.deepEqual(await c.lpos('l', '3', { count: 0 }), [2]);
    assert.equal(await c.lmove('l', 'l2', 'LEFT', 'RIGHT'), 'one');
    assert.equal(await c.rpoplpush('l', 'l2'), '3');
    assert.deepEqual(await c.lrange('l2', 0, -1), ['3', 'one']);
    assert.deepEqual(await c.lmpop(['none', 'l2'], 'LEFT', 1), { key: 'l2', elements: ['3'] });
    assert.equal(await c.lmpop(['none'], 'LEFT'), null);
    // LPUSHCAP / RPUSHCAP and the "recent items" helper
    assert.equal(await c.lpushcap('cap', 3, 'a', 'b', 'c', 'd'), 3);
    assert.deepEqual(await c.lrange('cap', 0, -1), ['d', 'c', 'b']);
    assert.equal(await c.rpushcap('rcap', 2, 'a', 'b', 'c'), 2);
    assert.deepEqual(await c.lrange('rcap', 0, -1), ['b', 'c']);
    for (let i = 0; i < 10; i++) await c.pushRecent('recent', 5, 'e' + i);
    assert.deepEqual(await c.recent('recent', 3), ['e9', 'e8', 'e7']);
    assert.equal(await c.llen('recent'), 5);
    // blocking forms on a dedicated connection
    const b = c.duplicate();
    try {
      await c.rpush('bq', 'x');
      assert.deepEqual(await b.blpop('bq', 1), ['bq', 'x']);
      await c.rpush('bq', 'y');
      assert.deepEqual(await b.brpop(['nobq', 'bq'], 1), ['bq', 'y']);
      assert.equal(await b.blpop('bq', 0.05), null);
      await c.rpush('bq', 'z');
      assert.equal(await b.blmove('bq', 'bq2', 'LEFT', 'LEFT', 1), 'z');
      await c.rpush('bq', 'w');
      assert.deepEqual(await b.blmpop(1, ['bq'], 'RIGHT', 5), { key: 'bq', elements: ['w'] });
      const waiting = b.blpop('later', 2);
      setTimeout(() => void c.rpush('later', 'v'), 30);
      assert.deepEqual(await waiting, ['later', 'v']);
    } finally {
      await b.quit();
    }
  } finally {
    await c.quit();
  }
});

test('sets', { skip: SKIP }, async () => {
  const c = await live(9);
  try {
    assert.equal(await c.sadd('s1', 'a', 'b', 'c'), 3);
    assert.equal(await c.sadd('s2', 'b'), 1);
    await c.sadd('s2', 'c', 'd');
    assert.equal(await c.srem('s1', 'zz'), 0);
    assert.equal(await c.sismember('s1', 'a'), true);
    assert.deepEqual(await c.smismember('s1', 'a', 'x'), [true, false]);
    assert.equal(await c.scard('s1'), 3);
    assert.deepEqual((await c.smembers('s1')).sort(), ['a', 'b', 'c']);
    assert.deepEqual((await c.sinter('s1', 's2')).sort(), ['b', 'c']);
    // The matrix says sorted; SUNION was observed unsorted, so compare as sets.
    assert.deepEqual((await c.sunion('s1', 's2')).sort(), ['a', 'b', 'c', 'd']);
    assert.deepEqual(await c.sdiff('s1', 's2'), ['a']);
    assert.equal(await c.sinterstore('si', 's1', 's2'), 2);
    assert.equal(await c.sunionstore('su', 's1', 's2'), 4);
    assert.equal(await c.sdiffstore('sd', 's1', 's2'), 1);
    assert.equal(await c.sintercard(['s1', 's2']), 2);
    assert.equal(await c.sintercard(['s1', 's2'], 1), 1);
    assert.equal(await c.smove('s1', 's3', 'a'), true);
    assert.ok(['b', 'c'].includes((await c.srandmember('s1'))!));
    assert.equal((await c.srandmember('s1', 5)).length, 2);
    assert.ok(['b', 'c'].includes((await c.spop('s1'))!));
    assert.equal((await c.spop('su', 2)).length, 2);
    for (let i = 0; i < 30; i++) await c.sadd('big', 'm' + i);
    const page = await c.sscan('big', 0, { count: 10 });
    assert.ok(page.items.length > 0);
    const seen = new Set<string>();
    for await (const m of c.sscanIterator('big', { count: 7 })) seen.add(m);
    assert.equal(seen.size, 30);
  } finally {
    await c.quit();
  }
});

test('hashes', { skip: SKIP }, async () => {
  const c = await live(10);
  try {
    assert.equal(await c.hset('h', 'f1', 'v1'), 1);
    assert.equal(await c.hset('h', { f2: 'v2', f3: 3 }), 2);
    assert.equal(await c.hget('h', 'f3'), '3');
    assert.deepEqual(await c.hgetBuffer('h', 'f1'), Buffer.from('v1'));
    assert.deepEqual(await c.hgetall('h'), { f1: 'v1', f2: 'v2', f3: '3' });
    assert.deepEqual(await c.binary.hgetall('h'), { f1: Buffer.from('v1'), f2: Buffer.from('v2'), f3: Buffer.from('3') });
    assert.deepEqual(await c.hmget('h', 'f1', 'nope'), ['v1', null]);
    assert.equal(await c.hlen('h'), 3);
    assert.equal(await c.hexists('h', 'f2'), true);
    assert.deepEqual(await c.hkeys('h'), ['f1', 'f2', 'f3']);
    assert.deepEqual(await c.hvals('h'), ['v1', 'v2', '3']);
    assert.equal(await c.hincrby('h', 'f3', 4), 7);
    assert.equal(await c.hincrbyfloat('h', 'f3', 0.5), 7.5);
    assert.equal(await c.hsetnx('h', 'f1', 'x'), false);
    assert.equal(await c.hsetnx('h', 'f9', 'x'), true);
    assert.equal(await c.hmset('hm', { a: '1', b: '2' }), 'OK');
    assert.deepEqual(await c.hmget('hm', 'a', 'b'), ['1', '2']);
    assert.equal(await c.hstrlen('h', 'f1'), 2);
    assert.equal(await c.hdel('h', 'f9', 'none'), 1);
    const page = await c.hscan('h', 0, { match: 'f*', count: 10 });
    assert.ok(page.items.length > 0);
    const fields: string[] = [];
    for await (const [f] of c.hscanIterator('h', { count: 1 })) fields.push(f);
    assert.deepEqual(fields, ['f1', 'f2', 'f3']);
    assert.ok(['f1', 'f2', 'f3'].includes((await c.hrandfield('h'))!));
    assert.equal((await c.hrandfield('h', 2)).length, 2);
    const withValues = await c.hrandfield('h', 2, true);
    assert.equal(withValues.length, 2);
    assert.equal(withValues[0]!.length, 2);
    // field TTLs
    assert.deepEqual(await c.hexpire('h', 100, ['f1', 'nope']), [1, -2]);
    assert.deepEqual(await c.hexpire('h', 50, ['f1'], 'GT'), [0]);
    assert.deepEqual(await c.hpexpire('h', 200000, ['f2']), [1]);
    const now = Math.floor(Date.now() / 1000);
    assert.deepEqual(await c.hexpireat('h', now + 1000, ['f3']), [1]);
    assert.deepEqual(await c.hpexpireat('h', (now + 2000) * 1000, ['f3'], 'XX'), [1]);
    const ttl = await c.httl('h', 'f1', 'f2');
    assert.ok(ttl[0]! > 90 && ttl[1]! > 190);
    assert.ok((await c.hpttl('h', 'f2'))[0]! > 190000);
    assert.equal((await c.hexpiretime('h', 'f3'))[0], now + 2000);
    assert.equal((await c.hpexpiretime('h', 'f3'))[0], (now + 2000) * 1000);
    assert.deepEqual(await c.hpersist('h', 'f1', 'nope'), [1, -2]);
    assert.deepEqual(await c.httl('h', 'f1'), [-1]);
  } finally {
    await c.quit();
  }
});

test('sorted sets', { skip: SKIP }, async () => {
  const c = await live(11);
  try {
    assert.equal(await c.zadd('z', 1, 'a'), 1);
    assert.equal(await c.zadd('z', [{ score: 2, member: 'b' }, [3, 'c']]), 2);
    assert.equal(await c.zadd('z', 5, 'a', { xx: true, ch: true }), 1);
    assert.equal(await c.zadd('z', 1, 'a', { gt: true }), 0);
    assert.equal(await c.zaddIncr('z', 1, 'a'), 6);
    assert.equal(await c.zaddIncr('z', 1, 'new', { xx: true }), null);
    assert.deepEqual(await c.zrange('z', 0, -1), ['b', 'c', 'a']);
    assert.deepEqual(await c.zrange('z', '(1', '+inf', { by: 'SCORE', limit: { offset: 0, count: 2 } }), ['b', 'c']);
    assert.deepEqual(await c.zrange('z', 0, 0, { rev: true }), ['a']);
    assert.deepEqual(await c.zrangeWithScores('z', 0, 0), [{ member: 'b', score: 2 }]);
    assert.deepEqual(await c.zrevrange('z', 0, 1), ['a', 'c']);
    assert.deepEqual(await c.zrevrangeWithScores('z', 0, 0), [{ member: 'a', score: 6 }]);
    assert.deepEqual(await c.zrangebyscore('z', 2, 3), ['b', 'c']);
    assert.deepEqual(await c.zrangebyscore('z', '-inf', '+inf', { offset: 1, count: 1 }), ['c']);
    assert.deepEqual(await c.zrangebyscoreWithScores('z', 6, 6), [{ member: 'a', score: 6 }]);
    await c.zadd('lex', [[0, 'a'], [0, 'b'], [0, 'c']]);
    assert.deepEqual(await c.zrangebylex('lex', '[b', '+'), ['b', 'c']);
    assert.deepEqual(await c.zrevrangebylex('lex', '+', '-', { offset: 0, count: 1 }), ['c']);
    assert.equal(await c.zlexcount('lex', '-', '+'), 3);
    assert.equal(await c.zremrangebylex('lex', '[a', '[a'), 1);
    assert.equal(await c.zincrby('z', 0.5, 'b'), 2.5);
    assert.equal(await c.zcard('z'), 3);
    assert.equal(await c.zscore('z', 'b'), 2.5);
    assert.equal(await c.zscore('z', 'none'), null);
    assert.deepEqual(await c.zmscore('z', 'b', 'none'), [2.5, null]);
    assert.equal(await c.zrank('z', 'b'), 0);
    assert.equal(await c.zrevrank('z', 'b'), 2);
    assert.equal(await c.zrank('z', 'none'), null);
    assert.equal(await c.zcount('z', '-inf', '+inf'), 3);
    assert.equal(await c.zrem('z', 'c', 'none'), 1);
    await c.zadd('z2', [[1, 'b'], [10, 'x']]);
    assert.equal(await c.zunionstore('zu', ['z', 'z2'], { weights: [1, 2], aggregate: 'SUM' }), 3);
    assert.equal(await c.zscore('zu', 'b'), 4.5);
    assert.equal(await c.zinterstore('zi', ['z', 'z2'], { aggregate: 'MAX' }), 1);
    assert.equal(await c.zdiffstore('zd', ['z', 'z2']), 1);
    assert.deepEqual(await c.zunion(['z', 'z2']), ['b', 'a', 'x']);
    assert.deepEqual(await c.zunionWithScores(['z', 'z2']), [{ member: 'b', score: 3.5 }, { member: 'a', score: 6 }, { member: 'x', score: 10 }]);
    assert.deepEqual(await c.zinter(['z', 'z2']), ['b']);
    assert.deepEqual(await c.zinterWithScores(['z', 'z2']), [{ member: 'b', score: 3.5 }]);
    assert.deepEqual(await c.zdiff(['z', 'z2']), ['a']);
    assert.deepEqual(await c.zdiffWithScores(['z', 'z2']), [{ member: 'a', score: 6 }]);
    assert.ok(['a', 'b'].includes((await c.zrandmember('z'))!));
    assert.equal((await c.zrandmember('z', 2)).length, 2);
    assert.equal((await c.zrandmember('z', 1, true))[0]!.score > 0, true);
    for (let i = 0; i < 25; i++) await c.zadd('zs', i, 'm' + i);
    assert.ok((await c.zscan('zs', 0, { count: 5 })).items.length > 0);
    let n = 0;
    for await (const e of c.zscanIterator('zs', { count: 4 })) {
      assert.equal(e.member, 'm' + e.score);
      n++;
    }
    assert.equal(n, 25);
    assert.equal(await c.zremrangebyrank('zs', 0, 4), 5);
    assert.equal(await c.zremrangebyscore('zs', '(20', '+inf'), 4);
    assert.deepEqual(await c.zpopmin('zs'), [{ member: 'm5', score: 5 }]);
    await c.zadd('due', [[1, 'a'], [2, 'b'], [3, 'c'], [9, 'd']]);
    assert.deepEqual(await c.zpopbyscore('due', 1), [{ member: 'a', score: 1 }]);
    assert.equal(await c.zmovedue('due', 'ready', 3, 5), 2);
    assert.deepEqual(await c.lrange('ready', 0, -1), ['b', 'c']);
    assert.deepEqual(await c.zpopbyscore('due', 3, '-inf', 5), []);
    assert.deepEqual(await c.zpopmax('zs', 2), [{ member: 'm20', score: 20 }, { member: 'm19', score: 19 }]);
    const b = c.duplicate();
    try {
      assert.deepEqual(await b.bzpopmin('zs', 1), { key: 'zs', member: 'm6', score: 6 });
      assert.deepEqual(await b.bzpopmax(['none', 'zs'], 1), { key: 'zs', member: 'm18', score: 18 });
      assert.equal(await b.bzpopmin('none', 0.05), null);
    } finally {
      await b.quit();
    }
  } finally {
    await c.quit();
  }
});

test('geo', { skip: SKIP }, async () => {
  const c = await live(12);
  try {
    assert.equal(
      await c.geoadd('g', [
        { longitude: 13.361389, latitude: 38.115556, member: 'Palermo' },
        { longitude: 15.087269, latitude: 37.502669, member: 'Catania' },
      ]),
      2,
    );
    assert.equal(await c.geoadd('g', [{ longitude: 13.361389, latitude: 38.115556, member: 'Palermo' }], { xx: true, ch: true }), 0);
    const d = (await c.geodist('g', 'Palermo', 'Catania', 'km'))!;
    assert.ok(Math.abs(d - 166.27) < 0.1, String(d));
    assert.equal(await c.geodist('g', 'Palermo', 'Nowhere'), null);
    const [h] = await c.geohash('g', 'Palermo');
    assert.equal(h!.length, 11);
    const [p, missing] = await c.geopos('g', 'Palermo', 'Nowhere');
    assert.ok(Math.abs(p![0] - 13.361389) < 1e-4);
    assert.equal(missing, null);
    assert.deepEqual(await c.geosearch('g', { fromLonLat: [15, 37], byRadius: { radius: 200, unit: 'km' }, order: 'ASC' }), ['Catania', 'Palermo']);
    const rich = await c.geosearch('g', { fromMember: 'Palermo', byBox: { width: 400, height: 400, unit: 'km' }, withDist: true, withCoord: true, withHash: true, count: 1, order: 'ASC' });
    assert.equal(rich.length, 1);
    const m = rich[0] as { member: string; distance: number; coordinates: [number, number]; hash: number };
    assert.equal(m.member, 'Palermo');
    assert.equal(m.distance, 0);
    assert.ok(m.hash > 0);
    assert.ok(Math.abs(m.coordinates[1] - 38.115556) < 1e-4);
    assert.equal(await c.geosearchstore('gs', 'g', { fromLonLat: [15, 37], byRadius: { radius: 200, unit: 'km' }, storeDist: true }), 2);
    assert.deepEqual(await c.georadius('g', 15, 37, 100, 'km'), ['Catania']);
    const r = await c.georadius('g', 15, 37, 200, 'km', { withDist: true, order: 'ASC', count: 2 });
    assert.equal((r as { member: string }[])[0]!.member, 'Catania');
    assert.deepEqual(await c.georadiusbymember('g', 'Palermo', 10, 'km'), ['Palermo']);
    assert.equal(await c.georadiusbymember('g', 'Palermo', 500, 'km', { store: 'gstore' }), 2);
  } finally {
    await c.quit();
  }
});

test('ttl on create: hash, sorted set, geo', { skip: SKIP }, async () => {
  const c = await live(2);
  try {
    const within = async (key: string) => {
      const t = await c.pttl(key);
      assert.ok(t > 90_000 && t <= 100_000, `${key} pttl ${t}`);
    };
    assert.equal(await c.hincrby('t:h', 'f', 2, { ttl: 100 }), 2);
    await within('t:h');
    await c.hincrby('t:h', 'f', 1, { ttl: 5000 });
    await within('t:h');
    assert.equal(await c.zincrby('t:zi', 1.5, 'm', { ttl: 100 }), 1.5);
    await within('t:zi');
    await c.zincrby('t:zi', 1, 'm', { ttl: 5000 });
    await within('t:zi');
    assert.equal(await c.zadd('t:z', 1, 'a', { ttl: 100 }), 1);
    await within('t:z');
    assert.equal(await c.zadd('t:z', [[2, 'b']], { ttl: 5000 }), 1);
    await within('t:z');
    assert.equal(await c.zaddIncr('t:z2', 3, 'a', { ttl: 100 }), 3);
    await within('t:z2');
    assert.equal(await c.geoadd('t:g', [{ longitude: 13.36, latitude: 38.11, member: 'p' }], { ttl: 100 }), 1);
    await within('t:g');
    await c.geoadd('t:g', [{ longitude: 15.08, latitude: 37.5, member: 'q' }], { ttl: 5000 });
    await within('t:g');
  } finally {
    await c.quit();
  }
});
