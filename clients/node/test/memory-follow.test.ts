// SPDX-License-Identifier: MIT
// Memory of a client that follows a failover (two seeds, followPrimary on) against a client with one seed.
// The measurements run in a separate process (memory-follow-probe.ts, started with --expose-gc and a 64 MB young
// generation so that a collection can be forced and none runs inside a measured loop); this file starts it and
// holds the bounds. The live tests need RUVIO_FAILOVER_ADDRS (scripts/run_with_servers.sh) and skip without it;
// the failover test uses scripted nodes.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const PROBE = fileURLToPath(new URL('./memory-follow-probe.js', import.meta.url));
const FLAGS = ['--expose-gc', '--min-semi-space-size=64', '--max-semi-space-size=64'];
const LIVE = (process.env.RUVIO_FAILOVER_ADDRS ?? '').split(',').length === 2 ? false : 'RUVIO_FAILOVER_ADDRS not set';

function probe<T>(mode: string): Promise<T> {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, [...FLAGS, PROBE, mode], { timeout: 150_000, maxBuffer: 1 << 20 }, (error, stdout, stderr) => {
      if (error) reject(new Error('probe ' + mode + ' failed: ' + error.message + '\n' + stderr));
      else resolve(JSON.parse(stdout) as T);
    });
  });
}

const median = (xs: number[]): number => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;

interface Alloc {
  seqSingle: number[];
  seqFollow: number[];
  batchSingle: number[];
  batchFollow: number[];
}

// Measured (three runs, medians of three samples after a warm-up round): bytes (heap + external) per command are
// 1330-1360 one command at a time and 500-520 in a batch of 50 for one seed; following measured -10 to +0
// (sequential) and +12 to +19 (batch) bytes above that. A per-command copy of a frame costs about 230 bytes, over
// both margins (64 and 80).
test('following adds no allocation per command on a healthy connection', { skip: LIVE, timeout: 150_000 }, async () => {
  const r = await probe<Alloc>('alloc');
  assert.ok(r.seqSingle.length >= 2 && r.seqFollow.length >= 2 && r.batchSingle.length >= 2 && r.batchFollow.length >= 2, 'too many runs were disturbed by a collection: ' + JSON.stringify(r));
  const report = JSON.stringify(r);
  assert.ok(median(r.seqFollow) - median(r.seqSingle) <= 64, 'one command at a time: ' + report);
  assert.ok(median(r.batchFollow) - median(r.batchSingle) <= 80, 'batches of 50: ' + report);
  assert.ok(median(r.seqFollow) < 1600 && median(r.batchFollow) < 700, 'absolute size: ' + report);
});

interface Retain {
  single: { marks: number[]; inner: Inner };
  follow: { marks: number[]; inner: Inner };
}
interface Inner {
  kept: number;
  pending: number;
  refused: number;
  search: boolean;
}

// Heap after a forced collection, taken after 60 rounds of the workload and then after
// each 120 more (about 7 000 commands each). Measured growth over the last two steps: 27-45 KB for either kind,
// which is warm-up of the JIT and caches; a frame kept per command fails the bound (250 KB).
test('following keeps no frames and the heap stays flat over many commands', { skip: LIVE, timeout: 150_000 }, async () => {
  const r = await probe<Retain>('retain');
  for (const kind of ['single', 'follow'] as const) {
    const { marks, inner } = r[kind];
    const growth = marks[3]! - marks[1]!;
    assert.ok(growth < 250 * 1024, kind + ' heap grew by ' + growth + ' bytes: ' + JSON.stringify(marks));
    assert.deepEqual(inner, { kept: 0, pending: 0, refused: 0, search: false }, kind + ' connection state');
  }
  assert.ok(r.follow.marks[3]! - r.single.marks[3]! < 512 * 1024, 'following holds more than one seed: ' + JSON.stringify(r));
});

interface Failover {
  cycles: number;
  heapMarks: number[];
  inner: Inner;
  handlesIdle: Record<string, number>;
  handlesWarm: Record<string, number>;
  handlesAfterCycles: Record<string, number>;
  handlesAfterClose: Record<string, number>;
  noPrimaryInner: Inner;
  handlesAfterNoPrimary: Record<string, number>;
  closeDuringSearch: { searching: boolean; results: string[]; inner: Inner };
  handlesAfterCloseDuringSearch: Record<string, number>;
  hung: { code: string | null; ms: number };
  handlesAfterHung: Record<string, number>;
}

// Handle counts are exact (process.getActiveResourcesInfo(), counted again until they settle); the heap bound
// is for the 30 failovers after the first 10: measured growth 60 KB.
test('failover searches leave no sockets, timers or held commands behind', { timeout: 150_000 }, async () => {
  const r = await probe<Failover>('failover');
  const open = { kept: 0, pending: 0, refused: 0, search: false };
  assert.deepEqual(r.inner, open);
  assert.deepEqual(r.handlesAfterCycles, r.handlesWarm, 'handles grew over ' + r.cycles + ' failovers');
  assert.deepEqual(r.handlesAfterClose, r.handlesIdle, 'handles left after close');
  assert.ok(r.heapMarks[2]! - r.heapMarks[0]! < 150 * 1024, 'heap over the failovers: ' + JSON.stringify(r.heapMarks));
  assert.deepEqual(r.noPrimaryInner, open);
  assert.deepEqual(r.handlesAfterNoPrimary, r.handlesIdle, 'handles left after searches that found no primary');
  assert.equal(r.closeDuringSearch.searching, true, 'the search was not running when the client closed');
  assert.deepEqual(r.closeDuringSearch.results, Array(5).fill('ConnectionError'));
  assert.deepEqual(r.closeDuringSearch.inner, open);
  assert.deepEqual(r.handlesAfterCloseDuringSearch, r.handlesIdle, 'handles left after closing during a search');
  assert.equal(r.hung.code, 'READONLY');
  assert.ok(r.hung.ms < 3000, 'a silent seed held the search for ' + r.hung.ms + ' ms');
  assert.deepEqual(r.handlesAfterHung, r.handlesIdle, 'handles left after a seed that never answers');
});
