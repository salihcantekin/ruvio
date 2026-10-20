// SPDX-License-Identifier: MIT
// Live: Bloom filters, JSON (incl. JSON.INDEX / JSON.FIND), streams and consumer groups.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioError } from '../src/index.js';
import { SKIP, live, rejects, sleep } from './helpers.js';

test('bloom', { skip: SKIP }, async () => {
  const c = await live(13);
  try {
    assert.equal(await c.bfReserve('bf', 0.01, 1000), 'OK');
    assert.equal(await c.bfReserve('bf2', 0.01, 100, { expansion: 4 }), 'OK');
    assert.equal(await c.bfReserve('bf3', 0.01, 2, { nonScaling: true }), 'OK');
    assert.equal(await c.bfAdd('bf', 'a'), true);
    assert.equal(await c.bfAdd('bf', 'a'), false);
    assert.deepEqual(await c.bfMadd('bf', 'a', 'b'), [false, true]);
    assert.deepEqual(await c.bfInsert('bfi', ['x', 'y'], { capacity: 50, error: 0.001 }), [true, true]);
    const e = await rejects(c.bfInsert('nobf', ['x'], { noCreate: true }));
    assert.ok(e instanceof RuvioError);
    assert.equal(await c.bfExists('bf', 'b'), true);
    assert.equal(await c.bfExists('bf', 'zzz'), false);
    assert.deepEqual(await c.bfMexists('bf', 'a', 'zzz'), [true, false]);
    assert.deepEqual(await c.bfMexists('missing', 'a'), [false]);
    assert.equal(await c.bfCard('bf'), 2);
    const info = await c.bfInfo('bf');
    assert.equal(info['Capacity'], 1000);
    assert.equal(await c.bfInfo('bf', 'Capacity'), 1000);
    // "seen before" helper
    assert.deepEqual(await c.seenBefore('seen', 'u1', 'u2'), [false, false]);
    assert.deepEqual(await c.seenBefore('seen', 'u2', 'u3'), [true, false]);
    assert.deepEqual(await c.mightContain('seen', 'u1', 'nope'), [true, false]);
  } finally {
    await c.quit();
  }
});

interface User {
  name: string;
  age: number;
  tags: string[];
  active: boolean;
}

test('json', { skip: SKIP }, async () => {
  const c = await live(13);
  try {
    const u: User = { name: 'ada', age: 36, tags: ['x'], active: true };
    assert.equal(await c.jsonSetAs('u', '$', u), true);
    assert.deepEqual(await c.jsonGetAs<User>('u'), u);
    assert.equal(await c.jsonGetAs<number>('u', '$.age'), 36);
    assert.equal(await c.jsonGetAs('nouser'), null);
    assert.equal(await c.jsonGet('u', '$.name'), '"ada"');
    assert.equal(await c.jsonSet('u', '$.name', '"bob"', 'NX'), false);
    assert.equal(await c.jsonSet('u', '$.extra', '1', 'NX'), true);
    assert.equal(await c.jsonSet('u', '$.extra', '2', 'XX'), true);
    assert.equal(await c.jsonType('u', '$.tags'), 'array');
    assert.equal(await c.jsonType('u', '$.nothing'), null);
    assert.equal(await c.jsonStrlen('u', '$.name'), 3);
    assert.equal(await c.jsonStrappend('u', '$.name', '"!"'), 4);
    assert.equal(await c.jsonArrlen('u', '$.tags'), 1);
    assert.equal(await c.jsonArrappend('u', '$.tags', '"y"', '"z"'), 3);
    assert.equal(await c.jsonArrinsert('u', '$.tags', 0, '"w"'), 4);
    assert.equal(await c.jsonArrindex('u', '$.tags', '"y"'), 2);
    assert.equal(await c.jsonArrindex('u', '$.tags', '"nope"', 0, 0), -1);
    assert.equal(await c.jsonArrpop('u', '$.tags'), '"z"');
    assert.equal(await c.jsonArrpop('u', '$.tags', 0), '"w"');
    assert.equal(await c.jsonArrtrim('u', '$.tags', 0, 0), 1);
    assert.deepEqual(await c.jsonObjkeys('u'), ['name', 'age', 'tags', 'active', 'extra']);
    assert.equal(await c.jsonObjlen('u'), 5);
    assert.equal(await c.jsonNumincrby('u', '$.age', 2), 38);
    assert.equal(await c.jsonNummultby('u', '$.age', 0.5), 19);
    assert.equal(await c.jsonToggle('u', '$.active'), false);
    assert.equal(await c.jsonMerge('u', '$', '{"age":null,"city":"x"}'), 'OK');
    assert.equal(await c.jmerge('u', '$', '{"zip":1}'), 'OK');
    const doc = await c.jsonGetAs<Record<string, unknown>>('u');
    assert.equal(doc!['age'], undefined);
    assert.equal(doc!['city'], 'x');
    assert.equal(doc!['zip'], 1);
    assert.equal(await c.jsonClear('u', '$.tags'), 1);
    assert.equal(await c.jsonDel('u', '$.zip'), 1);
    assert.equal(await c.jsonForget('u', '$.city'), 1);
    await c.jsonSetAs('u2', '$', { name: 'eve' });
    assert.deepEqual(await c.jsonMget(['u', 'u2', 'none'], '$.name'), ['"ada!"', '"eve"', null]);
    assert.equal(await c.jsonDel('u2'), 1);
    assert.equal((await rejects(c.jsonArrpop('u'))).code, 'WRONGTYPE');
    await c.jsonSet('arr', '$', '[]');
    assert.equal(await c.jsonArrpop('arr'), null);
  } finally {
    await c.quit();
  }
});

test('JSON.INDEX and JSON.FIND (database 0 only)', { skip: SKIP }, async () => {
  const c = await live(0);
  const idx = 'nodeidx' + process.pid;
  try {
    assert.equal(await c.jsonIndexCreate(idx, 'nusr:', '$.city', 'string'), 'OK');
    await c.jsonSetAs('nusr:1', '$', { city: 'izmir' });
    await c.jsonSetAs('nusr:2', '$', { city: 'izmir' });
    await c.jsonSetAs('nusr:3', '$', { city: 'ankara' });
    let found: string[] = [];
    for (const end = Date.now() + 15_000; Date.now() < end; ) {
      try {
        found = await c.jsonFind(idx, 'izmir');
        break;
      } catch (e) {
        if (!(e instanceof RuvioError)) throw e; // index still building
        await sleep(20);
      }
    }
    assert.deepEqual(found, ['nusr:1', 'nusr:2']);
    assert.deepEqual(await c.jsonFind(idx, 'izmir', { count: 1 }), ['nusr:1']);
    assert.deepEqual(await c.jsonFind(idx, 'izmir', { after: 'nusr:1' }), ['nusr:2']);
    assert.ok((await c.jsonIndexList()).some((l) => l.startsWith(idx)));
  } finally {
    await c.jsonIndexDrop(idx).catch(() => undefined);
    await c.del('nusr:1', 'nusr:2', 'nusr:3').catch(() => undefined);
    await c.quit();
  }
});

test('streams and consumer groups', { skip: SKIP }, async () => {
  const c = await live(14);
  try {
    const id1 = await c.xadd('st', '*', { a: '1' });
    assert.match(id1!, /^\d+-\d+$/);
    const id2 = await c.xadd('st', '*', ['b', '2']);
    assert.equal(await c.xadd('nost', '*', { a: 1 }, { noMkStream: true }), null);
    await c.xadd('st', '*', { c: '3' }, { maxLen: 10, approximate: true });
    assert.equal(await c.xlen('st'), 3);
    const all = await c.xrange('st');
    assert.deepEqual(all[0], { id: id1, fields: [['a', '1']] });
    assert.equal((await c.xrange('st', '-', '+', 1)).length, 1);
    assert.deepEqual((await c.xrevrange('st', '+', '-', 1))[0]!.fields, [['c', '3']]);
    await c.call('XADD', 'sd', '1-0', 'f', '1', 'g', '', 'f', '3');
    const want = [['f', '1'], ['g', ''], ['f', '3']];
    assert.deepEqual((await c.xrange('sd'))[0]!.fields, want);
    assert.deepEqual((await c.xrevrange('sd', '+', '-'))[0]!.fields, want);
    assert.deepEqual((await c.xread({ sd: '0' }))![0]!.entries[0]!.fields, want);
    const read = await c.xread({ st: '0' }, { count: 2 });
    assert.equal(read![0]!.key, 'st');
    assert.equal(read![0]!.entries.length, 2);
    assert.equal(await c.xread([['st', '$']], { block: 30 }), null);
    assert.equal(await c.xdel('st', id2!, id2!), 1);
    assert.equal(await c.xtrim('st', { maxLen: 1 }), 1);
    assert.equal(await c.xtrim('st', { minId: '0-1' }), 0);
    await c.xadd('st', '*', { d: '4' }, { minId: '0-1' });
    assert.equal(await c.xsetid('st', '99999999999999-0'), 'OK');

    // consumer groups
    assert.equal(await c.xgroupCreate('gs', 'g', '$', { mkStream: true }), 'OK');
    assert.equal(await c.xgroupCreateconsumer('gs', 'g', 'c1'), true);
    assert.equal(await c.xgroupCreateconsumer('gs', 'g', 'c1'), false);
    const e1 = await c.xadd('gs', '*', { job: '1' });
    const e2 = await c.xadd('gs', '*', { job: '2' });
    const got = await c.xreadgroup('g', 'c1', 'gs', { count: 10 });
    assert.deepEqual(got![0]!.entries.map((e) => e.id), [e1, e2]);
    const sum = await c.xpending('gs', 'g');
    assert.equal(sum.count, 2);
    assert.equal(sum.smallest, e1);
    assert.deepEqual(sum.consumers, [{ name: 'c1', pending: 2 }]);
    const rows = await c.xpendingRange('gs', 'g', '-', '+', 10);
    assert.equal(rows.length, 2);
    assert.equal(rows[0]!.consumer, 'c1');
    assert.equal(rows[0]!.deliveries, 1);
    assert.equal((await c.xpendingRange('gs', 'g', '-', '+', 10, { idle: 0, consumer: 'c1' })).length, 2);
    const claimed = await c.xclaim('gs', 'g', 'c2', 0, [e1!]);
    assert.equal((claimed as { id: string }[])[0]!.id, e1);
    assert.deepEqual(await c.xclaim('gs', 'g', 'c2', 0, [e1!], { justId: true, retryCount: 3 }), [e1]);
    const auto = await c.xautoclaim('gs', 'g', 'c3', 0, '0-0', { count: 10 });
    assert.equal(auto.next, '0-0');
    assert.equal(auto.entries.length, 2);
    const autoIds = await c.xautoclaim('gs', 'g', 'c3', 0, '0-0', { justId: true });
    assert.deepEqual(autoIds.entries, [e1, e2]);
    assert.equal(await c.xack('gs', 'g', e1!, e1!), 1);
    const info = await c.xinfoStream('gs');
    assert.equal(info['length'], 2);
    const groups = await c.xinfoGroups('gs');
    assert.equal(groups[0]!['name'], 'g');
    const consumers = await c.xinfoConsumers('gs', 'g');
    assert.ok(consumers.length >= 1);
    assert.equal(await c.xgroupSetid('gs', 'g', '0'), 'OK');
    assert.equal(await c.xgroupDelconsumer('gs', 'g', 'c3'), 1);
    // NOACK and DELAY
    await c.xgroupCreate('gs', 'g2', '$');
    // The 3 s delay leaves the read right after it a wide margin; the entry then shows up
    // within a deadline.
    await c.xadd('gs', '*', { x: 1 }, { delayMs: 3000 });
    assert.equal(await c.xreadgroup('g2', 'c', 'gs', { noAck: true }), null);
    let delayed: Awaited<ReturnType<typeof c.xreadgroup>> = null;
    for (const end = Date.now() + 15_000; delayed === null && Date.now() < end; ) {
      await sleep(50);
      delayed = await c.xreadgroup('g2', 'c', 'gs', { noAck: true });
    }
    assert.equal(delayed![0]!.entries.length, 1);
    assert.equal((await c.xpending('gs', 'g2')).count, 0);
    // blocking group read on a dedicated connection
    const b = c.duplicate();
    try {
      const waiting = b.xreadgroup('g2', 'c', 'gs', { block: 10_000 });
      setTimeout(() => void c.xadd('gs', '*', { late: 1 }), 30);
      const late = await waiting;
      assert.deepEqual(late![0]!.entries[0]!.fields, [['late', '1']]);
    } finally {
      await b.quit();
    }
    assert.equal(await c.xgroupDestroy('gs', 'g2'), true);
    assert.equal(await c.xgroupDestroy('gs', 'g2'), false);
  } finally {
    await c.quit();
  }
});

test('ttl on create: stream, json, bloom filter', { skip: SKIP }, async () => {
  const c = await live(3);
  try {
    const within = async (key: string) => {
      const t = await c.pttl(key);
      assert.ok(t > 90_000 && t <= 100_000, `${key} pttl ${t}`);
    };
    assert.ok(await c.xadd('t:s', '*', { a: 1 }, { ttl: 100 }));
    await within('t:s');
    await c.xadd('t:s', '*', { a: 2 }, { ttl: 5000 });
    await within('t:s');
    assert.equal(await c.jsonSet('t:j', '$', '{"a":1}', undefined, { ttl: 100 }), true);
    await within('t:j');
    await c.jsonSetAs('t:j', '$', { a: 2 }, undefined, { ttl: 5000 });
    await within('t:j');
    assert.deepEqual(await c.bfInsert('t:b', ['x'], { ttl: 100 }), [true]);
    await within('t:b');
    await c.bfInsert('t:b', ['y'], { ttl: 5000 });
    await within('t:b');
  } finally {
    await c.quit();
  }
});
