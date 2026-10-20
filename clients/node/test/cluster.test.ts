// SPDX-License-Identifier: MIT
// Cluster routing without a server: slot math, key extraction for every command shape,
// and MOVED / refresh / fallback behaviour against a scripted fake server.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ClusterClient, ConnectionError, RuvioError, hashSlot, createClusterClient } from '../src/index.js';
import { ROUTED_COMMANDS, keysOfFrame } from '../src/cluster.js';
import { COMMAND_KEY_RULES } from '../src/command-keys.js';
import { Encoder, cmd } from '../src/encoder.js';
import { arr, bulk, err, fakeServer, int, ok, slotsReply, type FakeServer } from './fake-server.js';

function frame(...words: (string | number)[]): Buffer {
  const e = new Encoder(null, 1024);
  e.begin(cmd(String(words[0])), words.length - 1);
  for (const w of words.slice(1)) e.arg(w);
  e.endFrame();
  return e.take();
}
const keys = (...w: (string | number)[]) => keysOfFrame(frame(...w));

/** First key `prefix<i>` whose slot lies in [lo, hi]. */
function keyIn(lo: number, hi: number, prefix = 'k', skip = 0): string {
  for (let i = 0; ; i++) {
    const k = prefix + i;
    const s = hashSlot(k);
    if (s >= lo && s <= hi && skip-- <= 0) return k;
  }
}

test('hash slots match the reference values and honour hashtags', () => {
  assert.equal(hashSlot('foo'), 12182);
  assert.equal(hashSlot('123456789'), 0x31c3 % 16384);
  assert.equal(hashSlot('{user1000}.following'), hashSlot('{user1000}.followers'));
  assert.equal(hashSlot('{user1000}.x'), hashSlot('user1000'));
  assert.equal(hashSlot('a{}b'), hashSlot('a{}b'), 'empty tag hashes the whole key');
  assert.notEqual(hashSlot('a{}b'), hashSlot(''));
  assert.equal(hashSlot('{a}{b}'), hashSlot('a'), 'first tag wins');
  assert.equal(hashSlot('x{a'), hashSlot('x{a'), 'unclosed tag hashes the whole key');
  assert.equal(hashSlot('{a'), hashSlot(Buffer.from('{a')));
  assert.equal(hashSlot('héllo'), hashSlot(Buffer.from('héllo', 'utf8')));
  for (const k of ['', 'a', 'zzzz', 'x'.repeat(1000)]) assert.ok(hashSlot(k) >= 0 && hashSlot(k) < 16384);
});

test('key extraction follows the Redis key positions', () => {
  assert.deepEqual(keys('GET', 'a'), ['a']);
  assert.deepEqual(keys('get', 'a'), ['a'], 'names are case-insensitive');
  assert.deepEqual(keys('MGET', 'a', 'b'), ['a', 'b']);
  assert.deepEqual(keys('MSET', 'a', '1', 'c', '2'), ['a', 'c']);
  assert.deepEqual(keys('MSETNX', 'a', '1', 'c', '2'), ['a', 'c']);
  assert.deepEqual(keys('DEL', 'a', 'b', 'c'), ['a', 'b', 'c']);
  assert.deepEqual(keys('RENAME', 'a', 'b'), ['a', 'b']);
  assert.deepEqual(keys('COPY', 'a', 'b', 'DB', 1), ['a', 'b']);
  assert.deepEqual(keys('SMOVE', 'a', 'b', 'm'), ['a', 'b']);
  assert.deepEqual(keys('LMOVE', 'a', 'b', 'LEFT', 'RIGHT'), ['a', 'b']);
  assert.deepEqual(keys('BLMOVE', 'a', 'b', 'LEFT', 'RIGHT', 0), ['a', 'b']);
  assert.deepEqual(keys('GEOSEARCHSTORE', 'dst', 'src', 'FROMMEMBER', 'm'), ['dst', 'src']);
  assert.deepEqual(keys('BLPOP', 'a', 'b', 5), ['a', 'b']);
  assert.deepEqual(keys('BZPOPMIN', 'a', 5), ['a']);
  assert.deepEqual(keys('EVAL', 'return 1', 2, 'k1', 'k2', 'arg'), ['k1', 'k2']);
  assert.deepEqual(keys('EVAL', 'return 1', 0, 'arg'), []);
  assert.deepEqual(keys('EVALSHA', 'abc', 1, 'k1', 'x'), ['k1']);
  assert.deepEqual(keys('FCALL', 'fn', 1, 'k'), ['k']);
  assert.deepEqual(keys('LMPOP', 2, 'a', 'b', 'LEFT'), ['a', 'b']);
  assert.deepEqual(keys('BLMPOP', 0, 1, 'a', 'LEFT'), ['a']);
  assert.deepEqual(keys('ZUNION', 2, 'a', 'b', 'WITHSCORES'), ['a', 'b']);
  assert.deepEqual(keys('ZUNIONSTORE', 'dst', 2, 'a', 'b', 'WEIGHTS', 1, 2), ['dst', 'a', 'b']);
  assert.deepEqual(keys('ZDIFFSTORE', 'dst', 1, 'a'), ['dst', 'a']);
  assert.deepEqual(keys('SINTERCARD', 2, 'a', 'b', 'LIMIT', 1), ['a', 'b']);
  assert.deepEqual(keys('XREAD', 'COUNT', 2, 'STREAMS', 's1', 's2', '0', '0'), ['s1', 's2']);
  assert.deepEqual(keys('XREADGROUP', 'GROUP', 'g', 'c', 'BLOCK', 10, 'STREAMS', 's1', '>'), ['s1']);
  assert.deepEqual(keys('BITOP', 'AND', 'dst', 'a', 'b'), ['dst', 'a', 'b']);
  assert.deepEqual(keys('JSON.MGET', 'a', 'b', '$.x'), ['a', 'b']);
  assert.deepEqual(keys('XGROUP', 'CREATE', 's', 'g', '$'), ['s']);
  assert.deepEqual(keys('XINFO', 'STREAM', 's'), ['s']);
  assert.deepEqual(keys('OBJECT', 'ENCODING', 'k'), ['k']);
  assert.deepEqual(keys('MEMORY', 'USAGE', 'k'), ['k']);
  assert.deepEqual(keys('WATCH', 'a', 'b'), ['a', 'b']);
  assert.deepEqual(keys('PFMERGE', 'd', 'a', 'b'), ['d', 'a', 'b']);
  assert.deepEqual(keys('SSUBSCRIBE', 'c1', 'c2'), ['c1', 'c2']);
  assert.deepEqual(keys('SPUBLISH', 'chan', 'hi'), ['chan']);
  for (const k of ['LEASE', 'LIMIT', 'ONCE', 'IDEM', 'LPUSHCAP', 'DELEX', 'FILLGET', 'BF.MADD', 'JSON.SET', 'XADD', 'SETV', 'TAKE', 'SEMAPHORE', 'RELEASE']) {
    assert.deepEqual(keys(k, 'the-key', 'x', 'y'), ['the-key'], k);
  }
  // keyless
  for (const n of ['PING', 'PUBLISH', 'WAIT', 'CHANGES', 'RUVIO', 'JSON.INDEX', 'JSON.FIND', 'SCAN', 'KEYS', 'DBSIZE', 'FLUSHDB', 'INFO', 'CLIENT', 'CLUSTER', 'SELECT', 'LATENCY', 'MULTI', 'EXEC']) {
    assert.deepEqual(keys(n, 'a', 'b'), [], n);
  }
  assert.deepEqual(keys('GET'), [], 'no argument, no key');
  assert.deepEqual(keys('EVAL', 'x', 'nan', 'a'), [], 'unparsable numkeys');
  // numkeys bigger than the arguments present is clamped
  assert.deepEqual(keys('ZUNION', 5, 'a', 'b'), ['a', 'b']);
});

test('every command in the compatibility matrix has a key rule', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const repoRoot = join(here, '..', '..', '..', '..');
  const matrix = JSON.parse(readFileSync(join(repoRoot, 'docs', 'compatibility-matrix.json'), 'utf8')) as {
    commands: { name: string; same_slot: boolean }[];
  };
  const names = new Set(matrix.commands.map((c) => c.name.split(' ')[0]!));
  // Every command with a split / fan-out / blocking flag is a real command (typo guard) ...
  const extra = new Set(['BRPOPLPUSH']);
  for (const n of ROUTED_COMMANDS.keys()) assert.ok(COMMAND_KEY_RULES.has(n) || extra.has(n), 'unknown command in routing flags: ' + n);
  // ... and every command of the matrix takes its key positions from the generated table.
  for (const n of names) assert.ok(COMMAND_KEY_RULES.has(n), n + ' has no key rule');
  console.log(`routing: ${names.size} command names in the matrix, ${COMMAND_KEY_RULES.size} with a key rule, ${ROUTED_COMMANDS.size} with a routing flag`);
});

// ---- fake-server behaviour ------------------------------------------------------------------

/** Two fake nodes: A owns slots 0-8191, B owns 8192-16383. */
async function twoNodes(opts: { aTable?: 'ok' | 'stale' } = {}) {
  let a!: FakeServer, b!: FakeServer;
  let loads = 0;
  const owner = (key: string) => (hashSlot(key) <= 8191 ? 'A' : 'B');
  const mk = (me: 'A' | 'B') => async () =>
    fakeServer((args) => {
      const cmdName = args[0]!;
      if (cmdName === 'CLUSTER') {
        // A "stale" node answers the first CLUSTER SLOTS with a wrong table (B owns one slot only).
        const stale = me === 'A' && opts.aTable === 'stale' && loads++ === 0;
        const ranges: [number, number, number][] = stale
          ? [[0, 16382, a.port], [16383, 16383, b.port]]
          : [[0, 8191, a.port], [8192, 16383, b.port]];
        return slotsReply(ranges);
      }
      if (cmdName === 'GET' || cmdName === 'SET') {
        const k = args[1]!;
        if (owner(k) !== me) {
          const o = owner(k) === 'A' ? a : b;
          return err(`MOVED ${hashSlot(k)} 127.0.0.1:${o.port}`);
        }
        return cmdName === 'GET' ? bulk(me + ':' + k) : ok;
      }
      if (cmdName === 'MGET') return arr(...args.slice(1).map((k) => bulk(me + ':' + k)));
      if (cmdName === 'DEL') return int(args.length - 1);
      return ok;
    });
  a = await mk('A')();
  b = await mk('B')();
  return { a, b, owner };
}

test('routes by slot over a fake two-node cluster; no extra traffic after the table is loaded', async () => {
  const { a, b } = await twoNodes();
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port });
  try {
    const lo = keyIn(0, 8191), hi = keyIn(8192, 16383);
    assert.equal(await c.get(lo), 'A:' + lo);
    assert.equal(await c.get(hi), 'B:' + hi);
    assert.ok(c.isCluster);
    assert.equal(c.nodes.length, 2);
    assert.equal(b.count('GET'), 1);
    assert.equal(a.count('CLUSTER'), 1);
    assert.equal(a.count('GET'), 1);
    assert.equal(c.redirectCount, 0);
    // binary view routes the same way
    assert.deepEqual(await c.binary.get(hi), Buffer.from('B:' + hi));
  } finally {
    c.disconnect();
    await a.close();
    await b.close();
  }
});

test('MOVED: patch the slot, retry on the new node, later calls go straight there', async () => {
  const { a, b } = await twoNodes({ aTable: 'stale' });
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port });
  try {
    const hi = keyIn(8192, 16383);
    // A claims every slot, so the first GET for B's key lands on A and is redirected.
    assert.equal(await c.get(hi), 'B:' + hi);
    assert.equal(a.count('GET'), 1);
    assert.equal(b.count('GET'), 1);
    assert.equal(c.redirectCount, 1);
    assert.equal(c.refreshCount, 0, 'one moved slot to a known node: no full refresh');
    assert.equal(await c.get(hi), 'B:' + hi);
    assert.equal(a.count('GET'), 1, 'the patched table sends it straight to B');
    // Another command moved to a known node patches its own slot too, still without a refresh.
    const hi2 = keyIn(8192, 16383, 'z');
    assert.equal(await c.get(hi2), 'B:' + hi2);
    assert.equal(c.redirectCount, 2);
    assert.equal(c.refreshCount, 0);
    assert.equal(a.count('CLUSTER'), 1);
    // After an explicit refresh the whole table is right: no more redirects.
    await c.refreshSlots();
    const before = c.redirectCount;
    for (let i = 0; i < 20; i++) await c.get('q' + i);
    assert.equal(c.redirectCount, before);
  } finally {
    c.disconnect();
    await a.close();
    await b.close();
  }
});

/** Three fake nodes; GET on node `i` answers with `get(i, key, ports)`, CLUSTER SLOTS with `table`. */
async function threeNodes(get: (i: number, key: string, ports: number[]) => string) {
  const nodes: FakeServer[] = [];
  const ports: number[] = [];
  // Node 0 owns almost everything, 1 and 2 own one slot each: all three are known to the client.
  const table = (): [number, number, number][] => [[0, 16381, ports[0]!], [16382, 16382, ports[1]!], [16383, 16383, ports[2]!]];
  for (let i = 0; i < 3; i++) {
    const n = await fakeServer((args) => {
      if (args[0] === 'CLUSTER') return slotsReply(table());
      if (args[0] === 'GET') return get(i, args[1]!, ports);
      return ok;
    });
    nodes.push(n);
    ports.push(n.port);
  }
  return { nodes, close: () => Promise.all(nodes.map((n) => n.close())) };
}

test('MOVED: the first redirect to a known node patches the slot; a second one for the same command reloads the table', async () => {
  // 0 -> 1 -> 2: the key's slot is reported moved twice before node 2 answers.
  const { nodes, close } = await threeNodes((i, k, ports) => (i < 2 ? err(`MOVED ${hashSlot(k)} 127.0.0.1:${ports[i + 1]}`) : bulk('C:' + k)));
  const c = new ClusterClient({ host: '127.0.0.1', port: nodes[0]!.port });
  try {
    await c.connect();
    const k = keyIn(0, 16381);
    assert.equal(await c.get(k), 'C:' + k);
    assert.equal(c.redirectCount, 2);
    assert.equal(c.refreshCount, 1, 'only the second MOVED reloads the table');
    assert.equal(nodes[0]!.count('CLUSTER'), 2);
    assert.deepEqual(nodes.map((n) => n.count('GET')), [1, 1, 1]);
    assert.equal(c.nodeFor(k).port, nodes[2]!.port, 'the slot points at the last named node');
  } finally {
    c.disconnect();
    await close();
  }
});

test('MOVED is followed at most three times per command; the fourth comes back as the error', async () => {
  // 0 -> 1 -> 2 -> 0 -> ...: the redirects never settle.
  const { nodes, close } = await threeNodes((i, k, ports) => err(`MOVED ${hashSlot(k)} 127.0.0.1:${ports[(i + 1) % 3]}`));
  const c = new ClusterClient({ host: '127.0.0.1', port: nodes[0]!.port });
  try {
    const k = keyIn(0, 16381);
    await assert.rejects(c.get(k), (e: unknown) => e instanceof RuvioError && e.code === 'MOVED');
    assert.deepEqual(nodes.map((n) => n.count('GET')), [2, 1, 1], 'the first send and three retries');
    assert.equal(c.redirectCount, 3);
    // the count is per command: the next command gets its own three redirects
    await assert.rejects(c.get(k), (e: unknown) => e instanceof RuvioError && e.code === 'MOVED');
    assert.equal(c.redirectCount, 6);
  } finally {
    c.disconnect();
    await close();
  }
});

test('MOVED to an unknown node refreshes the table', async () => {
  const c2 = await fakeServer((args) => (args[0] === 'GET' ? bulk('C') : ok));
  let aSlots: [number, number, number][] | null = null;
  let moved = false;
  const a = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return slotsReply(aSlots!);
    if (args[0] === 'GET') return moved ? err(`MOVED ${hashSlot(args[1]!)} 127.0.0.1:${c2.port}`) : bulk('A');
    return ok;
  });
  aSlots = [[0, 16383, a.port]];
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port });
  try {
    assert.equal(await c.get('x'), 'A'); // loads the table: A owns all
    moved = true;
    aSlots = [[0, 16383, c2.port]]; // the cluster changed: C owns everything now
    assert.equal(await c.get('x'), 'C');
    assert.equal(c.refreshCount, 1);
    assert.deepEqual(c.nodes.map((n) => n.port), [c2.port]);
    assert.equal(await c.get('y'), 'C');
    assert.equal(a.count('GET'), 2, 'after the refresh, no more traffic to A');
  } finally {
    c.disconnect();
    await a.close();
    await c2.close();
  }
});

test('a transport failure is never replayed and does not break other nodes', async () => {
  let drop = true;
  const { a, b } = await twoNodes();
  // Replace B by a node that kills the socket on the first GET.
  await b.close();
  const b2 = await fakeServer((args, sock) => {
    if (args[0] === 'GET' && drop) {
      sock.destroy();
      return null;
    }
    return args[0] === 'GET' ? bulk('B2') : ok;
  });
  const a2 = await fakeServer((args) =>
    args[0] === 'CLUSTER' ? slotsReply([[0, 8191, a2.port], [8192, 16383, b2.port]]) : args[0] === 'GET' ? bulk('A2') : ok,
  );
  await a.close();
  const c = new ClusterClient({ host: '127.0.0.1', port: a2.port });
  try {
    const lo = keyIn(0, 8191), hi = keyIn(8192, 16383);
    assert.equal(await c.get(lo), 'A2');
    await assert.rejects(c.get(hi), (e: unknown) => e instanceof ConnectionError);
    assert.equal(b2.count('GET'), 1, 'the failed command was not replayed');
    assert.equal(await c.get(lo), 'A2', 'the other node is unaffected');
    drop = false;
    assert.equal(await c.get(hi), 'B2', 'reconnects lazily');
  } finally {
    c.disconnect();
    await a2.close();
    await b2.close();
  }
});

test('cross-slot commands fail fast without sending anything', async () => {
  const { a, b } = await twoNodes();
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port });
  try {
    await c.connect();
    const lo = keyIn(0, 8191), hi = keyIn(8192, 16383);
    const before = a.seen.length + b.seen.length;
    for (const p of [c.sunion(lo, hi), c.rename(lo, hi), c.sinterstore('{x}d', lo, hi), c.msetnx({ [lo]: '1', [hi]: '2' }), c.bitop('AND', 'dst', lo, hi)]) {
      await assert.rejects(p, (e: unknown) => e instanceof RuvioError && e.code === 'CROSSSLOT');
    }
    assert.equal(a.seen.length + b.seen.length, before, 'nothing reached a server');
    // same slot via hashtag is fine
    await c.sunion('{t}a', '{t}b');
  } finally {
    c.disconnect();
    await a.close();
    await b.close();
  }
});

test('split commands: one sub-command per slot, replies in caller order', async () => {
  const { a, b } = await twoNodes();
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port });
  try {
    const ks = ['a1', 'b2', 'c3', 'd4', 'e5', 'f6', 'g7', 'h8'];
    const r = await c.mget(...ks);
    assert.deepEqual(r, ks.map((k) => (hashSlot(k) <= 8191 ? 'A:' : 'B:') + k));
    assert.equal(await c.del(...ks), ks.length, 'DEL counts are summed');
    const nSlots = new Set(ks.map(hashSlot)).size;
    assert.equal(a.count('MGET') + b.count('MGET'), nSlots, 'one MGET per distinct slot');
  } finally {
    c.disconnect();
    await a.close();
    await b.close();
  }
});

test('standalone fallback: "cluster mode is disabled" gives a plain client', async () => {
  const s = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return err('ERR cluster mode is disabled');
    if (args[0] === 'GET') return bulk('v:' + args[1]);
    if (args[0] === 'MGET') return arr(...args.slice(1).map((k) => bulk('v:' + k)));
    return ok;
  });
  const c = createClusterClient({ host: '127.0.0.1', port: s.port });
  try {
    assert.equal(await c.get('a'), 'v:a');
    assert.equal(c.isCluster, false);
    assert.equal(s.count('CLUSTER'), 1);
    // keys of different slots go to the one server as a single command
    assert.deepEqual(await c.mget('a', 'b', 'zzz'), ['v:a', 'v:b', 'v:zzz']);
    assert.equal(s.count('MGET'), 1);
    assert.deepEqual(await c.pipeline().get('a').get('b').exec(), ['v:a', 'v:b']);
    assert.equal(await c.get('c'), 'v:c');
    assert.equal(s.count('CLUSTER'), 1, 'asked only once');
  } finally {
    c.disconnect();
    await s.close();
  }
  const s2 = await fakeServer((args) => (args[0] === 'CLUSTER' ? err('ERR cluster mode is disabled') : ok));
  const strict = createClusterClient({ host: '127.0.0.1', port: s2.port, cluster: true });
  try {
    await assert.rejects(strict.get('a'), (e: unknown) => e instanceof RuvioError);
  } finally {
    strict.disconnect();
    await s2.close();
  }
});

test('a failed topology load rejects every waiting call and is retried by the next one', async () => {
  let fail = true;
  const s = await fakeServer((args, sock) => {
    if (args[0] === 'CLUSTER') {
      if (fail) {
        sock.destroy();
        return null;
      }
      return slotsReply([[0, 16383, s.port]]);
    }
    return args[0] === 'GET' ? bulk('ok') : ok;
  });
  const c = new ClusterClient({ host: '127.0.0.1', port: s.port });
  try {
    const rs = await Promise.allSettled([c.get('a'), c.get('b')]);
    assert.ok(rs.every((r) => r.status === 'rejected'));
    fail = false;
    assert.equal(await c.get('a'), 'ok');
  } finally {
    c.disconnect();
    await s.close();
  }
});

// ---- ASK and TRYAGAIN -----------------------------------------------------------------------

const isErr = (code: string) => (e: unknown) => e instanceof RuvioError && e.code === code;

test('ASK sends ASKING and the command to the named node and leaves the slot map alone', async () => {
  let asks = 1;
  const b = await fakeServer((args) => (args[0] === 'GET' ? bulk('B:' + args[1]) : ok));
  // A's first GET answers ASK, later ones the value.
  const a2 = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return slotsReply([[0, 8191, a2.port], [8192, 16383, b.port]]);
    if (args[0] === 'GET') return asks-- > 0 ? err(`ASK ${hashSlot(args[1]!)} 127.0.0.1:${b.port}`) : bulk('A:' + args[1]);
    return ok;
  });
  const c = new ClusterClient({ host: '127.0.0.1', port: a2.port, commandTimeoutMs: 5000 });
  try {
    const k = keyIn(0, 8191);
    assert.equal(await c.get(k), 'B:' + k);
    assert.deepEqual(b.seen.map((x) => x[0]), ['ASKING', 'GET'], 'ASKING comes first, then the command');
    assert.deepEqual(b.seen[1], ['GET', k]);
    assert.equal(c.redirectCount, 1);
    assert.equal(c.refreshCount, 0);
    assert.equal(await c.get(k), 'A:' + k, 'the slot is still owned by A');
    assert.equal(a2.count('GET'), 2);
    assert.equal(b.count('GET'), 1);
  } finally {
    c.disconnect();
    await a2.close();
    await b.close();
  }
});

test('ASK to a node the table does not list is followed', async () => {
  const x = await fakeServer((args) => (args[0] === 'GET' ? bulk('X') : ok));
  const a = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return slotsReply([[0, 16383, a.port]]);
    if (args[0] === 'GET') return err(`ASK ${hashSlot(args[1]!)} 127.0.0.1:${x.port}`);
    return ok;
  });
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port, commandTimeoutMs: 5000 });
  try {
    assert.equal(await c.get('foo'), 'X');
    assert.deepEqual(x.seen.map((s) => s[0]), ['ASKING', 'GET']);
    assert.equal(c.refreshCount, 0);
  } finally {
    c.disconnect();
    await a.close();
    await x.close();
  }
});

test('TRYAGAIN is retried after a short wait and the later success is returned', async () => {
  let n = 0;
  const a = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return slotsReply([[0, 16383, a.port]]);
    if (args[0] === 'GET') return n++ < 1 ? err('TRYAGAIN slot is being handed over') : bulk('v');
    return ok;
  });
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port, commandTimeoutMs: 5000 });
  try {
    await c.connect();
    const t0 = process.hrtime.bigint();
    assert.equal(await c.get('k'), 'v');
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    assert.ok(ms >= 4, `waited ${ms} ms before the retry`);
    assert.equal(a.count('GET'), 2);
  } finally {
    c.disconnect();
    await a.close();
  }
});

test('TRYAGAIN is retried seven times; the eighth answer is returned to the caller', async () => {
  const a = await fakeServer((args) => {
    if (args[0] === 'CLUSTER') return slotsReply([[0, 16383, a.port]]);
    if (args[0] === 'GET') return err('TRYAGAIN slot is being handed over');
    return ok;
  });
  const c = new ClusterClient({ host: '127.0.0.1', port: a.port, commandTimeoutMs: 5000 });
  try {
    await c.connect();
    const t0 = process.hrtime.bigint();
    await assert.rejects(c.get('k'), isErr('TRYAGAIN'));
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    assert.equal(a.count('GET'), 8, 'the first send and seven retries');
    assert.ok(ms >= 630, `waited ${ms} ms in total (5 + 10 + 20 + 40 + 80 + 160 + 320)`);
  } finally {
    c.disconnect();
    await a.close();
  }
});

test('ASK and MOVED share one budget of three redirects; the fourth comes back as the error', async () => {
  const seq = (ports: number[]) => [`ASK # 127.0.0.1:${ports[1]}`, `MOVED # 127.0.0.1:${ports[2]}`, `ASK # 127.0.0.1:${ports[1]}`, `ASK # 127.0.0.1:${ports[2]}`];
  let n = 0;
  const { nodes, close } = await threeNodes((_i, k, ports) => err(seq(ports)[n++]!.replace('#', String(hashSlot(k)))));
  const c = new ClusterClient({ host: '127.0.0.1', port: nodes[0]!.port, commandTimeoutMs: 5000 });
  try {
    await c.connect();
    await assert.rejects(c.get(keyIn(0, 16381)), isErr('ASK'));
    assert.equal(n, 4, 'one send and three followed redirects');
    assert.equal(c.redirectCount, 3);
  } finally {
    c.disconnect();
    await close();
  }
});
