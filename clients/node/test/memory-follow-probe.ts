// SPDX-License-Identifier: MIT
// Measurements for memory-follow.test.ts. It runs as its own process (node --expose-gc with a large young
// generation) so that a garbage collection can be forced and does not fall inside a measured loop; it prints
// one JSON object on stdout. Modes: `alloc` and `retain` need live servers (RUVIO_FAILOVER_ADDRS, primary
// first), `failover` uses scripted nodes.

import * as net from 'node:net';
import { performance, PerformanceObserver } from 'node:perf_hooks';
import { RuvioClient } from '../src/index.js';
import { arr, bulk, err, fakeServer, int, ok, type FakeServer } from './fake-server.js';

const gc = (globalThis as { gc?: () => void }).gc;
if (gc === undefined) throw new Error('run with --expose-gc');

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Heap and external bytes in use, after collections have settled. */
async function settled(): Promise<number> {
  gc!();
  await sleep(10);
  gc!();
  const m = process.memoryUsage();
  return m.heapUsed + m.external;
}

const now = (): number => {
  const m = process.memoryUsage();
  return m.heapUsed + m.external;
};

const gcAt: number[] = [];
new PerformanceObserver((list) => {
  for (const e of list.getEntries()) gcAt.push(e.startTime);
}).observe({ entryTypes: ['gc'] });

/** Bytes allocated per call of `body`, over `n` calls; null when a collection ran inside the loop. */
async function perOp(n: number, calls: number, body: () => Promise<void>): Promise<number | null> {
  await settled();
  const t0 = performance.now();
  const h0 = now();
  for (let i = 0; i < n; i++) await body();
  const h1 = now();
  const t1 = performance.now();
  await sleep(5);
  if (gcAt.some((t) => t >= t0 && t <= t1)) return null;
  return (h1 - h0) / (n * calls);
}

function addrs(): string[] {
  const list = process.env.RUVIO_FAILOVER_ADDRS;
  if (!list) throw new Error('RUVIO_FAILOVER_ADDRS not set');
  return list.split(',');
}

function open(following: boolean): RuvioClient {
  const all = addrs();
  return new RuvioClient({ seeds: following ? all : [all[0]!], commandTimeoutMs: 5000, connectTimeoutMs: 2000 });
}

/** Internal state of a following connection that must be empty when nothing is pending. */
function inner(c: RuvioClient): { kept: number; pending: number; refused: number; search: boolean } {
  const k = c.conn as unknown as { rFB: (Buffer | null)[] | null; count: number; refused: unknown[] | null; refusedScope: unknown };
  return {
    kept: k.rFB === null ? 0 : k.rFB.filter((f) => f !== null).length,
    pending: k.count,
    refused: k.refused === null ? 0 : k.refused.length,
    search: k.refusedScope !== null,
  };
}

const median = (xs: number[]): number => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;

async function workload(c: RuvioClient, rounds: number): Promise<void> {
  const keys = Array.from({ length: 50 }, (_, i) => 'mem:k' + i);
  for (let i = 0; i < rounds; i++) {
    await c.set('mem:a', 'value-' + i);
    await c.get('mem:a');
    await Promise.all(keys.map((k) => c.get(k)));
    if (i % 10 === 0) {
      const t = c.multi();
      t.set('mem:t', String(i));
      t.get('mem:t');
      await t.exec();
      await c.incr('mem:a').catch(() => undefined); // a wrong-type error reply on the success-adjacent path
    }
  }
}

async function alloc(): Promise<unknown> {
  const out: Record<string, number[]> = { seqSingle: [], seqFollow: [], batchSingle: [], batchFollow: [] };
  const keys = Array.from({ length: 50 }, (_, i) => 'mem:k' + i);
  for (let round = 0; round < 4; round++) {
    for (const following of [false, true]) {
      const c = open(following);
      await c.set('mem:a', 'value-12345');
      for (let i = 0; i < 60; i++) await c.get('mem:a'), await Promise.all(keys.map((k) => c.get(k)));
      const seq = await perOp(500, 2, async () => {
        await c.set('mem:a', 'value-12345');
        await c.get('mem:a');
      });
      const batch = await perOp(20, 50, async () => {
        await Promise.all(keys.map((k) => c.get(k)));
      });
      c.disconnect();
      if (round === 0) continue; // the first round warms the JIT up
      if (seq !== null) out[following ? 'seqFollow' : 'seqSingle']!.push(seq);
      if (batch !== null) out[following ? 'batchFollow' : 'batchSingle']!.push(batch);
    }
  }
  return out;
}

async function retain(): Promise<unknown> {
  const result: Record<string, unknown> = {};
  for (const following of [false, true]) {
    const c = open(following);
    await workload(c, 60);
    const marks: number[] = [await settled()];
    for (let step = 0; step < 3; step++) {
      await workload(c, 120);
      marks.push(await settled());
    }
    result[following ? 'follow' : 'single'] = { marks, inner: inner(c) };
    c.disconnect();
  }
  return result;
}

// ---- scripted nodes -------------------------------------------------------------------------

interface Node {
  port: number;
  server: FakeServer;
  mode: 'master' | 'replica';
  primary: number;
}

async function node(mode: 'master' | 'replica', primary = 0): Promise<Node> {
  const n = { mode, primary } as Node;
  n.server = await fakeServer((a) => {
    const name = a[0]!;
    if (name === 'ROLE') return n.mode === 'master' ? arr(bulk('master'), int(0), arr()) : arr(bulk('slave'), bulk('127.0.0.1'), int(n.primary), bulk('connected'), int(0));
    if (name === 'CLUSTER') return err('ERR This instance has cluster support disabled');
    if (n.mode === 'replica' && (name === 'SET' || name === 'INCR')) return err('READONLY You can\'t write against a read only replica.');
    if (name === 'GET') return '$-1\r\n';
    if (name === 'INCR') return int(1);
    return ok;
  });
  n.port = n.server.port;
  return n;
}

async function hung(): Promise<{ port: number; close(): Promise<void> }> {
  const socks = new Set<net.Socket>();
  const server = net.createServer((s) => {
    socks.add(s);
    s.on('error', () => undefined);
    s.on('close', () => socks.delete(s));
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return {
    port: (server.address() as net.AddressInfo).port,
    close: () =>
      new Promise<void>((r) => {
        for (const s of socks) s.destroy();
        server.close(() => r());
      }),
  };
}

type Counts = Record<string, number>;
function counts(): Counts {
  const c: Counts = {};
  for (const t of process.getActiveResourcesInfo()) c[t] = (c[t] ?? 0) + 1;
  // The probe's own bookkeeping, not the client's.
  delete c['Immediate'];
  delete c['FSReqCallback'];
  // The probe's own stdio and parent pipe.
  delete c['PipeWrap'];
  delete c['TTYWrap'];
  return c;
}

/** Waits (bounded) until the active resources are exactly `want`, and returns what they are then. */
async function reach(want: Counts, ms = 5000): Promise<Counts> {
  const end = Date.now() + ms;
  for (;;) {
    const c = counts();
    if (JSON.stringify(sorted(c)) === JSON.stringify(sorted(want)) || Date.now() > end) return c;
    await sleep(10);
  }
}
const sorted = (c: Counts): [string, number][] => Object.entries(c).sort(([a], [b]) => a.localeCompare(b));

function clear(...nodes: Node[]): void {
  for (const n of nodes) n.server.seen.length = 0;
}

async function failover(): Promise<unknown> {
  const out: Record<string, unknown> = {};
  const a = await node('master');
  const b = await node('replica', a.port);
  const nodes = [a, b];
  await sleep(50);
  const idle = counts();
  const seeds = nodes.map((n) => '127.0.0.1:' + n.port);

  // 1. Many failovers: each swaps the roles, the next write is refused, found the primary, and sent again.
  const c = new RuvioClient({ seeds, connectTimeoutMs: 1000, commandTimeoutMs: 5000 });
  await c.set('k', '0');
  let master = a;
  let other = b;
  const marks: number[] = [];
  let atWarm: Counts = {};
  const cycles = 40;
  for (let i = 1; i <= cycles; i++) {
    [master, other] = [other, master];
    master.mode = 'master';
    other.mode = 'replica';
    other.primary = master.port;
    // A few writes at once meet the refusal together: they are held, then sent on the new connection.
    const r = await Promise.all([c.set('k', String(i)), c.set('k', String(i)), c.set('k', String(i))]);
    if (r.some((x) => x !== 'OK')) throw new Error('write failed in cycle ' + i);
    clear(a, b);
    if (i === 10) atWarm = await reach(counts());
    if (i === 10 || i === 25 || i === cycles) marks.push(await settled());
  }
  out.cycles = cycles;
  out.heapMarks = marks;
  out.inner = inner(c);
  const busy = await reach(atWarm);
  out.handlesWarm = atWarm;
  out.handlesAfterCycles = busy;
  c.disconnect();
  out.handlesAfterClose = await reach(idle);
  out.handlesIdle = idle;

  // 2. A search that finds no primary (both nodes replicas, one round): the READONLY error, nothing left open.
  a.mode = 'replica';
  b.mode = 'replica';
  const d = new RuvioClient({ seeds, connectTimeoutMs: 1000, commandTimeoutMs: 5000, reconnect: false });
  for (let i = 0; i < 20; i++) {
    const e = await d.set('k', 'x').then(() => null, (x: unknown) => x as { code?: string });
    if (e === null || e.code !== 'READONLY') throw new Error('expected READONLY, got ' + String(e));
  }
  out.noPrimaryInner = inner(d);
  d.disconnect();
  out.handlesAfterNoPrimary = await reach(idle);

  // 3. The client is closed while a search waits between rounds: held commands fail, timers and probes go.
  a.mode = 'master';
  b.mode = 'replica';
  b.primary = a.port;
  const f = new RuvioClient({ seeds, connectTimeoutMs: 1000, commandTimeoutMs: 5000 });
  await f.get('k');
  a.mode = 'replica';
  a.primary = 0;
  const held = Array.from({ length: 5 }, () => f.set('k', 'y').then(() => 'ok', (x: unknown) => (x as Error).constructor.name));
  await sleep(1000); // the rounds so far found nothing; the search is in its 800 ms wait, which ends about 500 ms from now
  const searching = inner(f).search;
  f.disconnect();
  out.closeDuringSearch = { searching, results: await Promise.all(held), inner: inner(f) };
  // Only a short grace for the sockets to finish closing: a timer left running would still be pending.
  out.handlesAfterCloseDuringSearch = await reach(idle, 100);

  // 4. A seed that accepts and never answers: the probe gives up after its timeout and closes the socket.
  const h = await hung();
  const g = new RuvioClient({ seeds: ['127.0.0.1:' + a.port, '127.0.0.1:' + h.port], connectTimeoutMs: 200, commandTimeoutMs: 200, reconnect: false });
  const started = Date.now();
  const e = await g.set('k', 'z').then(() => null, (x: unknown) => x as { code?: string });
  out.hung = { code: e?.code ?? null, ms: Date.now() - started };
  g.disconnect();
  await h.close();
  out.handlesAfterHung = await reach(idle);

  for (const n of nodes) await n.server.close();
  return out;
}

const modes: Record<string, () => Promise<unknown>> = { alloc, retain, failover };
const mode = process.argv[2] ?? '';
const run = modes[mode];
if (run === undefined) throw new Error('unknown mode ' + mode);
run().then(
  (r) => {
    process.stdout.write(JSON.stringify(r) + '\n');
    process.exit(0);
  },
  (e) => {
    process.stderr.write(String(e && (e as Error).stack) + '\n');
    process.exit(1);
  },
);
