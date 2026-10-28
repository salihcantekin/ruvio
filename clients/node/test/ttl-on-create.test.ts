// SPDX-License-Identifier: MIT
// TTL on create and EXPIRE conditions: the exact arguments each method sends, against a scripted server.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ExpireCondition, RuvioClient } from '../src/index.js';
import { fakeServer, int } from './fake-server.js';
import { rejects } from './helpers.js';

/** Runs `fn` against a server that answers every command with `:1`, and returns the commands it saw. */
async function sent(fn: (c: RuvioClient) => Promise<unknown>): Promise<string[][]> {
  const s = await fakeServer((args) => (args[0] === 'HELLO' ? undefined : int(1)));
  const c = new RuvioClient({ host: '127.0.0.1', port: s.port });
  try {
    await fn(c).catch(() => undefined);
    return s.seen.filter((a) => a[0] !== 'HELLO');
  } finally {
    c.disconnect();
    await s.close();
  }
}

test('EXPIRE family sends the optional condition last', async () => {
  const seen = await sent(async (c) => {
    await c.expire('k', 10);
    await c.expire('k', 10, 'NX');
    await c.pexpire('k', 1500, ExpireCondition.Xx);
    await c.expireat('k', 1_900_000_000, 'GT');
    await c.pexpireat('k', 1_900_000_000_000, 'lt' as ExpireCondition);
  });
  assert.deepEqual(seen, [
    ['EXPIRE', 'k', '10'],
    ['EXPIRE', 'k', '10', 'NX'],
    ['PEXPIRE', 'k', '1500', 'XX'],
    ['EXPIREAT', 'k', '1900000000', 'GT'],
    ['PEXPIREAT', 'k', '1900000000000', 'lt'],
  ]);
});

test('ttl option: whole seconds go as EX, fractions as PX', async () => {
  const seen = await sent(async (c) => {
    await c.incr('k', { ttl: 60 });
    await c.incr('k', { ttl: 1.5 });
    await c.incr('k', {});
    await c.decr('k', { ttl: 2 });
    await c.incrby('k', 5, { ttl: 3 });
    await c.decrby('k', 5, { ttl: 0.25 });
    await c.incrbyMax('k', 1, 10, { ttl: 4 });
    await c.incrbyMax('k', 1, 10);
    await c.incrbyfloat('k', 0.5, { ttl: 5 });
    await c.hincrby('k', 'f', 2, { ttl: 6 });
    await c.zincrby('k', 1.5, 'm', { ttl: 7 });
    await c.setbit('k', 9, 1, { ttl: 8 });
  });
  assert.deepEqual(seen, [
    ['INCR', 'k', 'EX', '60'],
    ['INCR', 'k', 'PX', '1500'],
    ['INCR', 'k'],
    ['DECR', 'k', 'EX', '2'],
    ['INCRBY', 'k', '5', 'EX', '3'],
    ['DECRBY', 'k', '5', 'PX', '250'],
    ['INCRBY', 'k', '1', 'EX', '4', 'MAX', '10'],
    ['INCRBY', 'k', '1', 'MAX', '10'],
    ['INCRBYFLOAT', 'k', '0.5', 'EX', '5'],
    ['HINCRBY', 'k', 'f', '2', 'EX', '6'],
    ['ZINCRBY', 'k', '1.5', 'm', 'EX', '7'],
    ['SETBIT', 'k', '9', '1', 'EX', '8'],
  ]);
});

test('ttl option on zadd, geoadd, xadd, jsonSet and bfInsert', async () => {
  const seen = await sent(async (c) => {
    await c.zadd('z', 1, 'a', { ttl: 30 });
    await c.zadd('z', [[1, 'a'], { member: 'b', score: 2 }], { nx: true, ch: true, ttl: 0.5 });
    await c.zaddIncr('z', 2, 'a', { gt: true, ttl: 9 });
    await c.geoadd('g', [{ longitude: 13.4, latitude: 38.1, member: 'p' }], { ttl: 10, nx: true, ch: true });
    await c.xadd('s', '*', { a: 1 }, { ttl: 11, noMkStream: true, maxLen: 5, delayMs: 20 });
    await c.jsonSet('j', '$', '{}', undefined, { ttl: 12 });
    await c.jsonSet('j', '$', '{}', 'NX', { ttl: 12 });
    await c.jsonSetAs('j', '$', { a: 1 }, 'XX', { ttl: 0.1 });
    await c.bfInsert('b', ['x', 'y'], { ttl: 13, capacity: 100, error: 0.01 });
  });
  assert.deepEqual(seen, [
    ['ZADD', 'z', 'EX', '30', '1', 'a'],
    ['ZADD', 'z', 'NX', 'CH', 'PX', '500', '1', 'a', '2', 'b'],
    ['ZADD', 'z', 'GT', 'EX', '9', 'INCR', '2', 'a'],
    ['GEOADD', 'g', 'EX', '10', 'NX', 'CH', '13.4', '38.1', 'p'],
    ['XADD', 's', 'EX', '11', 'NOMKSTREAM', 'MAXLEN', '5', 'DELAY', '20', '*', 'a', '1'],
    ['JSON.SET', 'j', '$', '{}', 'EX', '12'],
    ['JSON.SET', 'j', '$', '{}', 'NX', 'EX', '12'],
    ['JSON.SET', 'j', '$', '{"a":1}', 'XX', 'PX', '100'],
    ['BF.INSERT', 'b', 'EX', '13', 'CAPACITY', '100', 'ERROR', '0.01', 'ITEMS', 'x', 'y'],
  ]);
});

test('an invalid ttl rejects before anything is sent, and the client stays usable', async () => {
  const s = await fakeServer((args) => (args[0] === 'HELLO' ? undefined : int(1)));
  const c = new RuvioClient({ host: '127.0.0.1', port: s.port });
  try {
    for (const ttl of [0, -1, NaN, Infinity, -Infinity]) {
      assert.ok((await rejects(c.incr('k', { ttl }))) instanceof RangeError);
      assert.ok((await rejects(c.zadd('z', 1, 'a', { ttl }))) instanceof RangeError);
      assert.ok((await rejects(c.jsonSet('j', '$', '1', 'NX', { ttl }))) instanceof RangeError);
    }
    assert.equal(await c.incr('k'), 1);
    assert.deepEqual(s.seen.filter((a) => a[0] !== 'HELLO'), [['INCR', 'k']]);
    assert.throws(() => c.pipeline().incr('k', { ttl: 0 }), RangeError);
  } finally {
    c.disconnect();
    await s.close();
  }
});

test('ttl options queue in pipelines and transactions', async () => {
  const seen = await sent(async (c) => {
    await c.pipeline().incr('k', { ttl: 5 }).expire('k', 9, 'NX').exec();
    await c.multi().hincrby('h', 'f', 1, { ttl: 5 }).exec();
  });
  assert.deepEqual(seen.map((a) => a.join(' ')), ['INCR k EX 5', 'EXPIRE k 9 NX', 'MULTI', 'HINCRBY h f 1 EX 5', 'EXEC']);
});
