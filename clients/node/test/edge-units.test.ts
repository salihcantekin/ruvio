// SPDX-License-Identifier: MIT
// Unit tests without a server: error classes, reply transforms, the parser's rare RESP3 types and
// chunk boundaries, encoder slab handling, and the Symbol.dispose polyfill.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import * as vm from 'node:vm';
import { ConnectionError, Encoder, NEED_MORE, Parser, PushMessage, ReplyError, RuvioError, cmd } from '../src/index.js';
import { T, str, toRecord } from '../src/transforms.js';

// ---- errors ---------------------------------------------------------------------------------

test('error classes: names, codes and inheritance', () => {
  const e = new RuvioError('WRONGTYPE Operation against a key holding the wrong kind of value');
  assert.equal(e.code, 'WRONGTYPE');
  assert.equal(e.name, 'RuvioError');
  assert.equal(e.command, undefined);
  e.command = 'GET';
  assert.equal(e.command, 'GET');
  assert.ok(e instanceof Error);
  assert.equal(new RuvioError('NOSPACE').code, 'NOSPACE', 'a message without a space is all code');
  assert.equal(new RuvioError('').code, '');
  assert.equal(new RuvioError('ERR  double space').code, 'ERR');
  const c = new ConnectionError('gone');
  assert.equal(c.name, 'ConnectionError');
  assert.ok(c instanceof Error && !(c instanceof RuvioError));
  const r = new ReplyError('odd shape');
  assert.equal(r.name, 'ReplyError');
  assert.ok(r instanceof Error && !(r instanceof RuvioError));
  assert.match(String(r), /ReplyError: odd shape/);
});

// ---- polyfill -------------------------------------------------------------------------------

test('polyfill defines Symbol.asyncDispose / dispose when the runtime lacks them, and leaves existing ones', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const file = join(here, '..', 'src', 'polyfill.js');
  const code = readFileSync(file, 'utf8').replace(/^export \{\};?$/m, '');
  const run = (sym: Record<string, unknown>) => {
    const sandbox = vm.createContext({ Symbol: sym, Object });
    vm.runInContext(code, sandbox, { filename: pathToFileURL(file).href });
  };
  const bare = Object.assign(function Symbol() {}, { for: Symbol.for }) as unknown as Record<string, unknown>;
  run(bare);
  assert.equal(bare['asyncDispose'], Symbol.for('nodejs.asyncDispose'));
  assert.equal(bare['dispose'], Symbol.for('nodejs.dispose'));
  const mine = Symbol('mine');
  const partial = Object.assign(function Symbol() {}, { for: Symbol.for, asyncDispose: mine }) as unknown as Record<string, unknown>;
  run(partial);
  assert.equal(partial['asyncDispose'], mine, 'an existing symbol is kept');
  assert.equal(partial['dispose'], Symbol.for('nodejs.dispose'));
  // in this runtime both exist (natively or through the polyfill) and are usable as method names
  assert.equal(typeof (Symbol as unknown as { asyncDispose: symbol }).asyncDispose, 'symbol');
});

// ---- transforms -----------------------------------------------------------------------------

const t = T as unknown as Record<string, (v: any) => any>;

test('str converts every reply shape to text', () => {
  assert.equal(str('a'), 'a');
  assert.equal(str(Buffer.from('héllo')), 'héllo');
  const big = Buffer.from('xxhelloxx');
  assert.equal(str(big.subarray(2, 7)), 'hello', 'honours the view offset');
  assert.equal(str(new Uint8Array([104, 105])), 'hi');
  assert.equal(str(null), '');
  assert.equal(str(undefined), '');
  assert.equal(str(12), '12');
  assert.equal(str(10n), '10');
  assert.equal(str(true), 'true');
});

test('scalar transforms', () => {
  assert.deepEqual([1, true, 'OK', 0, false, 'no', null].map(t.bool), [true, true, true, false, false, false, false]);
  assert.equal(t.okBool(null), false);
  assert.equal(t.okBool('OK'), true);
  assert.equal(t.okBool(0), true, 'any non-nil reply is truthy for SET-like commands');
  assert.deepEqual(t.boolArr([1, 0, true, false, null]), [true, false, true, false, false]);
  assert.equal(t.float(null), null);
  assert.equal(t.float(1.5), 1.5);
  assert.equal(t.float('2.25'), 2.25);
  assert.equal(t.float(Buffer.from('-3')), -3);
  assert.equal(t.float('inf'), Infinity);
  assert.equal(t.float('+inf'), Infinity);
  assert.equal(t.float('-inf'), -Infinity);
  assert.ok(Number.isNaN(t.float('nan')));
  assert.deepEqual(t.floatArr(['1', null, 'inf', 2]), [1, null, Infinity, 2]);
  assert.equal(t.int(5), 5);
  assert.equal(t.int('42'), 42);
  assert.equal(t.int(Buffer.from('-7')), -7);
  assert.equal(t.intOrNull(null), null);
  assert.equal(t.intOrNull(3), 3);
  assert.equal(t.intOrNull('9'), 9);
  assert.equal(t.token(null), null);
  assert.equal(t.token(7), '7');
  assert.equal(t.token(123456789012345678901234567890n), '123456789012345678901234567890');
  assert.equal(t.token('abc'), 'abc');
  assert.equal(t.token(Buffer.from('tok')), 'tok');
  assert.equal(t.jsonBool(null), null);
  assert.deepEqual([1, 'true', true, 0, 'false', false].map(t.jsonBool), [true, true, true, false, false, false]);
  assert.equal(t.jsonNum(null), null);
  assert.equal(t.jsonNum('12.5'), 12.5);
  assert.equal(t.jsonNum(Buffer.from('3')), 3);
  assert.deepEqual(t.strs(null), []);
  assert.deepEqual(t.strs(['a']), ['a']);
  assert.equal(t.lines(Buffer.from('x\ny')), 'x\ny');
  assert.deepEqual(t.bitfield([1, null]), [1, null]);
});

test('map-like transforms accept RESP2 arrays, RESP3 maps and null', () => {
  assert.deepEqual(toRecord(null), {});
  assert.deepEqual(toRecord(undefined), {});
  assert.deepEqual(toRecord(['a', 1, Buffer.from('b'), 2]), { a: 1, b: 2 });
  assert.deepEqual(toRecord(['a', 1, 'dangling']), { a: 1 }, 'odd trailing element is ignored');
  assert.deepEqual(toRecord(new Map<unknown, unknown>([['a', 1], [Buffer.from('k'), 2], [5, 3]])), { a: 1, k: 2, '5': 3 });
  const o = { x: 1 };
  assert.equal(toRecord(o), o, 'a plain object passes through');
  assert.deepEqual(t.map(['a', 'b']), { a: 'b' });
  assert.deepEqual(t.mapArr(null), []);
  assert.deepEqual(t.mapArr([['a', 1], new Map([['b', 2]]), null]), [{ a: 1 }, { b: 2 }, {}]);
  assert.deepEqual(t.pairs(['a', 1, 'b', 2, 'odd']), [['a', 1], ['b', 2]]);
  assert.deepEqual(t.pairs({ a: 1, b: 2 }), [['a', 1], ['b', 2]]);
  assert.deepEqual(t.pairs([]), []);
});

test('scan transforms', () => {
  assert.deepEqual(t.scan([Buffer.from('17'), ['a', 'b']]), { cursor: '17', items: ['a', 'b'] });
  assert.deepEqual(t.scan(['0', null]), { cursor: '0', items: [] });
  assert.deepEqual(t.hscan(['5', ['f', 'v', 'g', 'w']]), { cursor: '5', items: [['f', 'v'], ['g', 'w']] });
  assert.deepEqual(t.hscan(['0', { f: 'v' }]), { cursor: '0', items: [['f', 'v']] }, 'RESP3 map page');
  assert.deepEqual(t.zscan(['3', ['m', '1.5', 'n', 'inf']]), {
    cursor: '3',
    items: [{ member: 'm', score: 1.5 }, { member: 'n', score: Infinity }],
  });
  assert.deepEqual(t.zscan(['0', null]), { cursor: '0', items: [] });
});

test('sorted-set and pop transforms', () => {
  assert.deepEqual(t.zentries(null), []);
  assert.deepEqual(t.zentries([]), []);
  assert.deepEqual(t.zentries(['a', '1', 'b', '-inf', 'odd']), [{ member: 'a', score: 1 }, { member: 'b', score: -Infinity }]);
  assert.deepEqual(t.zentries([['a', 1.5], ['b', '2']]), [{ member: 'a', score: 1.5 }, { member: 'b', score: 2 }], 'RESP3 nested pairs');
  assert.equal(t.zentry(null), null);
  assert.equal(t.zentry([]), null);
  assert.deepEqual(t.zentry(['m', '4']), { member: 'm', score: 4 });
  assert.equal(t.bzpop(null), null);
  assert.deepEqual(t.bzpop(['k', 'm', '2.5']), { key: 'k', member: 'm', score: 2.5 });
  assert.equal(t.kv(null), null);
  assert.deepEqual(t.kv(['k', 'v', 'extra']), ['k', 'v']);
  assert.equal(t.lmpop(null), null);
  assert.deepEqual(t.lmpop(['k', ['a', 'b']]), { key: 'k', elements: ['a', 'b'] });
  assert.deepEqual(t.lmpop(['k', null]), { key: 'k', elements: [] });
});

test('stream transforms', () => {
  const e1 = ['1-0', ['f', 'v', 'g', 'w']];
  const e2 = ['2-0', null];
  assert.deepEqual(t.stream(null), []);
  assert.deepEqual(t.stream([e1, e2]), [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }, { id: '2-0', fields: [] }]);
  assert.deepEqual(t.stream([[Buffer.from('3-0'), [Buffer.from('k'), 'v']]]), [{ id: '3-0', fields: [['k', 'v']] }]);
  // repeated names and numeric names keep their order
  assert.deepEqual(t.stream([['4-0', ['f', '1', '2', 'x', 'f', '3', '1', 'y']]]), [
    { id: '4-0', fields: [['f', '1'], ['2', 'x'], ['f', '3'], ['1', 'y']] },
  ]);
  assert.deepEqual(t.streamOrIds(null), []);
  assert.deepEqual(t.streamOrIds(['1-0', '2-0']), ['1-0', '2-0'], 'XAUTOCLAIM JUSTID answers ids');
  assert.deepEqual(t.streamOrIds([e1, null]), [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }], 'deleted entries are dropped');
  assert.deepEqual(t.streamOrIds([]), []);
  assert.equal(t.xread(null), null);
  assert.deepEqual(t.xread({ s: [e1] }), [{ key: 's', entries: [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }] }]);
  assert.deepEqual(t.xread([['s', [e1]]]), [{ key: 's', entries: [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }] }]);
  // an entry that came back without its wrapping list is accepted
  assert.deepEqual(t.xread([['s', e1]]), [{ key: 's', entries: [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }] }]);
  assert.deepEqual(t.xread([['s', null]]), [{ key: 's', entries: [] }]);
  assert.deepEqual(t.autoclaim(['0-0', [e1], ['9-0']]), { next: '0-0', entries: [{ id: '1-0', fields: [['f', 'v'], ['g', 'w']] }], deleted: ['9-0'] });
  assert.deepEqual(t.autoclaim(['5-0', ['1-0'], null]), { next: '5-0', entries: ['1-0'], deleted: [] });
  assert.deepEqual(t.pendingSummary([3, '1-0', '3-0', [['c1', '2'], ['c2', Buffer.from('1')]]]), {
    count: 3,
    smallest: '1-0',
    greatest: '3-0',
    consumers: [{ name: 'c1', pending: 2 }, { name: 'c2', pending: 1 }],
  });
  assert.deepEqual(t.pendingSummary([0, null, null, null]), { count: 0, smallest: null, greatest: null, consumers: [] });
  assert.deepEqual(t.pendingRows([['1-0', 'c', 250, 2]]), [{ id: '1-0', consumer: 'c', idleMs: 250, deliveries: 2 }]);
});

test('Ruvio primitive transforms', () => {
  assert.deepEqual(t.fill(['HIT', 'v']), { state: 'hit', value: 'v', token: null });
  assert.deepEqual(t.fill(['FILL', 77]), { state: 'fill', value: null, token: '77' });
  assert.deepEqual(t.fill(['BUSY', null]), { state: 'busy', value: null, token: null });
  assert.deepEqual(t.fill(['STALE', 'old', 'tok']), { state: 'stale', value: 'old', token: 'tok' });
  assert.deepEqual(t.fill([Buffer.from('STALE'), 'old']), { state: 'stale', value: 'old', token: null });
  for (const bad of [null, 'HIT', [], ['HIT']]) assert.throws(() => t.fill(bad), ReplyError, JSON.stringify(bad));
  assert.throws(() => t.fill(['WHAT', 'x']), (e: unknown) => e instanceof ReplyError && /unexpected state WHAT/.test((e as Error).message));
  assert.deepEqual(t.idem(['acquired']), { state: 'acquired', result: null });
  assert.deepEqual(t.idem(['completed', 'stored']), { state: 'completed', result: 'stored' });
  assert.deepEqual(t.idem(['pending', 'ignored']), { state: 'pending', result: null });
  assert.deepEqual(t.changes([[1, 'set a'], [2, Buffer.from('del b')]]), [{ seq: 1, note: 'set a' }, { seq: 2, note: 'del b' }]);
  assert.deepEqual(t.geopos([['13.5', '38.25'], null, ['1']]), [[13.5, 38.25], null, null]);
});

// ---- parser ---------------------------------------------------------------------------------

function parseAll(bytes: Buffer, binary = false): unknown[] {
  const p = new Parser();
  p.binary = binary;
  p.feed(bytes);
  const out: unknown[] = [];
  for (;;) {
    const v = p.next();
    if (v === NEED_MORE) return out;
    out.push(v);
  }
}

const FRAMES: [string, unknown][] = [
  ['+OK\r\n', 'OK'],
  ['+PONG\r\n', 'PONG'],
  ['+\r\n', ''],
  ['-ERR boom\r\n', new RuvioError('ERR boom')],
  [':0\r\n', 0],
  [':-42\r\n', -42],
  [':+5\r\n', 5],
  [':9007199254740991\r\n', 9007199254740991],
  [':-9007199254740991\r\n', -9007199254740991],
  [':9007199254740992\r\n', 9007199254740992n],
  [':12345678901234567890\r\n', 12345678901234567890n],
  ['$0\r\n\r\n', ''],
  ['$-1\r\n', null],
  ['$5\r\nhello\r\n', 'hello'],
  ['$2\r\n\r\n\r\n', '\r\n'],
  ['*-1\r\n', null],
  ['*0\r\n', []],
  ['%0\r\n', {}],
  ['~0\r\n', []],
  ['>0\r\n', new PushMessage([])],
  ['_\r\n', null],
  ['#t\r\n', true],
  ['#f\r\n', false],
  [',1.5\r\n', 1.5],
  [',-2\r\n', -2],
  [',inf\r\n', Infinity],
  [',-inf\r\n', -Infinity],
  ['(123456789012345678901234567890\r\n', 123456789012345678901234567890n],
  ['(-5\r\n', -5n],
  ['!9\r\nSYNTAX no\r\n', new RuvioError('SYNTAX no')],
  ['=15\r\ntxt:hello world\r\n', 'hello world'],
  ['=4\r\ntxt:\r\n', ''],
  ['*3\r\n$1\r\na\r\n:2\r\n*1\r\n$-1\r\n', ['a', 2, [null]]],
  ['%2\r\n+a\r\n:1\r\n+b\r\n*1\r\n:2\r\n', { a: 1, b: [2] }],
  ['%1\r\n:1\r\n+x\r\n', new Map<unknown, unknown>([[1, 'x']])],
  ['~3\r\n:1\r\n:2\r\n:3\r\n', [1, 2, 3]],
  ['>3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n', new PushMessage(['message', 'c', 'm'])],
  ['|1\r\n+ttl\r\n:3\r\n$2\r\nhi\r\n', 'hi'],
  ['|0\r\n+after\r\n', 'after'],
  ['*2\r\n|1\r\n+k\r\n+v\r\n:1\r\n:2\r\n', [1, 2]],
  ['*2\r\n:1\r\n-ERR in array\r\n', [1, new RuvioError('ERR in array')]],
  ['*1\r\n*1\r\n*1\r\n*1\r\n:7\r\n', [[[[7]]]]],
];

test('parser: every frame type, whole and split at every byte boundary', () => {
  for (const [wire, expect] of FRAMES) {
    const whole = parseAll(Buffer.from(wire, 'latin1'));
    assert.equal(whole.length, 1, wire);
    assert.deepEqual(whole[0], expect, wire);
    // two-chunk splits at every offset
    const bytes = Buffer.from(wire, 'latin1');
    for (let cut = 1; cut < bytes.length; cut++) {
      const p = new Parser();
      p.feed(Buffer.from(bytes.subarray(0, cut)));
      let v = p.next();
      assert.equal(v, NEED_MORE, wire + ' @' + cut);
      p.feed(Buffer.from(bytes.subarray(cut)));
      v = p.next();
      assert.deepEqual(v, expect, wire + ' @' + cut);
      assert.equal(p.next(), NEED_MORE);
      assert.ok(p.idle, wire + ' idle after a full reply');
    }
    // byte at a time
    const p = new Parser();
    let got: unknown = NEED_MORE;
    for (let i = 0; i < bytes.length; i++) {
      p.feed(Buffer.from(bytes.subarray(i, i + 1)));
      got = p.next();
      if (i < bytes.length - 1) assert.equal(got, NEED_MORE, wire + ' byte ' + i);
    }
    assert.deepEqual(got, expect, wire);
  }
});

test('parser: several replies in one chunk, and an unfinished tail carried to the next', () => {
  const p = new Parser();
  p.feed(Buffer.from(':1\r\n+two\r\n$5\r\nthr'));
  assert.equal(p.next(), 1);
  assert.equal(p.next(), 'two');
  assert.equal(p.next(), NEED_MORE);
  assert.equal(p.idle, false, 'an unfinished bulk is buffered');
  p.feed(Buffer.from('ee\r\n*2\r\n:1\r\n'));
  assert.equal(p.next(), 'three');
  assert.equal(p.next(), NEED_MORE);
  assert.equal(p.idle, false, 'an open array keeps the parser busy');
  p.feed(Buffer.from(':2\r\n'));
  assert.deepEqual(p.next(), [1, 2]);
  assert.equal(p.next(), NEED_MORE);
  assert.ok(p.idle);
  assert.equal(new Parser().idle, true);
});

test('parser: binary mode keeps bulk, simple and verbatim strings as Buffers', () => {
  const [a, b, c, d] = parseAll(Buffer.from('$3\r\n\x00\xff\x80\r\n+word\r\n=7\r\ntxt:abc\r\n*1\r\n$1\r\nz\r\n', 'latin1'), true);
  assert.deepEqual(a, Buffer.from([0, 255, 128]));
  assert.deepEqual(b, Buffer.from('word'));
  assert.deepEqual(c, Buffer.from('abc'));
  assert.deepEqual(d, [Buffer.from('z')]);
  assert.deepEqual(parseAll(Buffer.from('+OK\r\n'), true), ['OK'], 'OK stays a string');
});

test('parser: protocol violations throw RuvioError', () => {
  for (const wire of ['?x\r\n', '&1\r\n', '\x00\r\n', '*1\r\n?\r\n']) {
    const p = new Parser();
    p.feed(Buffer.from(wire, 'latin1'));
    assert.throws(() => p.next(), (e: unknown) => e instanceof RuvioError && /unexpected reply type byte/.test((e as Error).message), JSON.stringify(wire));
  }
  for (const wire of [':12x\r\n', '$1x\r\nab\r\n', '*a\r\n']) {
    const p = new Parser();
    p.feed(Buffer.from(wire, 'latin1'));
    assert.throws(() => p.next(), (e: unknown) => e instanceof RuvioError && /bad integer/.test((e as Error).message), JSON.stringify(wire));
  }
});

test('parser: a bulk or aggregate over the limit throws before it is read', () => {
  for (const wire of ['$67108865\r\n', '*1048577\r\n', '%1048577\r\n', '=67108865\r\n', '*1\r\n$67108865\r\n']) {
    const p = new Parser();
    p.feed(Buffer.from(wire, 'latin1'));
    assert.throws(() => p.next(), (e: unknown) => e instanceof RuvioError && /over the limit/.test((e as Error).message), JSON.stringify(wire));
  }
});

test('parser: reset drops a half-read reply and a huge accumulator', () => {
  const p = new Parser();
  p.feed(Buffer.from('*2\r\n$3\r\nab'));
  assert.equal(p.next(), NEED_MORE);
  p.reset();
  assert.ok(p.idle);
  p.feed(Buffer.from(':5\r\n'));
  assert.equal(p.next(), 5);
  // a >1 MiB unfinished reply grows the accumulator; reset releases it
  const big = Buffer.alloc(2 << 20, 97);
  const q = new Parser();
  q.feed(Buffer.from('$' + big.length + '\r\n'));
  assert.equal(q.next(), NEED_MORE);
  q.feed(big.subarray(0, big.length - 10));
  assert.equal(q.next(), NEED_MORE);
  q.reset();
  q.feed(Buffer.from('+OK\r\n'));
  assert.equal(q.next(), 'OK');
});

test('parser: a large bulk and a large array arriving in odd-sized chunks', () => {
  const big = 'é'.repeat(100_000);
  const wire = Buffer.concat([
    Buffer.from('$' + Buffer.byteLength(big) + '\r\n'),
    Buffer.from(big),
    Buffer.from('\r\n*3\r\n'),
    Buffer.from('$-1\r\n:5\r\n$3\r\nabc\r\n'),
  ]);
  const p = new Parser();
  const out: unknown[] = [];
  for (let i = 0; i < wire.length; i += 7919) {
    p.feed(Buffer.from(wire.subarray(i, i + 7919)));
    for (let v = p.next(); v !== NEED_MORE; v = p.next()) out.push(v);
  }
  assert.deepEqual(out, [big, [null, 5, 'abc']]);
});

// ---- encoder --------------------------------------------------------------------------------

test('encoder: pending, frag, and a fresh slab when the current one is nearly full', () => {
  const chunks: Buffer[] = [];
  const e = new Encoder((c) => chunks.push(Buffer.from(c)), 1024);
  assert.equal(e.pending, 0);
  e.begin(cmd('SET'), 2);
  e.arg('k');
  e.arg('v'.repeat(600));
  e.endFrame();
  assert.ok(e.pending > 600);
  e.flush(); // < 512 bytes of slab left: the encoder switches to a new slab
  assert.equal(e.pending, 0);
  assert.equal(chunks.length, 1);
  e.flush(); // nothing pending: no empty chunk
  assert.equal(chunks.length, 1);
  e.begin(cmd('GET'), 1);
  e.arg('k');
  e.endFrame();
  e.flush();
  assert.equal(chunks.length, 2);
  assert.equal(chunks[1]!.toString(), '*2\r\n$3\r\nGET\r\n$1\r\nk\r\n');
  assert.ok(chunks[0]!.toString().startsWith('*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$600\r\n' + 'v'.repeat(600) + '\r\n'));

  // raw fragments: short (byte loop) and long (copy)
  const plain = new Encoder(null, 64);
  plain.begin(cmd('X'), 2);
  plain.frag(Buffer.from('$1\r\na\r\n'));
  plain.frag(Buffer.from('$20\r\n' + 'b'.repeat(20) + '\r\n'));
  plain.endFrame();
  assert.equal(plain.take().toString(), '*3\r\n$1\r\nX\r\n$1\r\na\r\n$20\r\n' + 'b'.repeat(20) + '\r\n');
  // a fragment larger than the slab grows it
  const small = new Encoder(null, 16);
  small.begin(cmd('X'), 1);
  small.frag(Buffer.from('$40\r\n' + 'c'.repeat(40) + '\r\n'));
  small.endFrame();
  assert.equal(small.take().toString(), '*2\r\n$1\r\nX\r\n$40\r\n' + 'c'.repeat(40) + '\r\n');
});

test('encoder: abortFrame discards only the frame in progress', () => {
  const e = new Encoder(null, 256);
  e.begin(cmd('SET'), 2);
  e.arg('keep');
  e.arg('me');
  e.endFrame();
  e.begin(cmd('SET'), 2);
  e.arg('half');
  assert.equal(e.abortFrame(), true);
  e.begin(cmd('GET'), 1);
  e.arg('k');
  e.endFrame();
  assert.equal(e.take().toString(), '*3\r\n$3\r\nSET\r\n$4\r\nkeep\r\n$2\r\nme\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n');
});
