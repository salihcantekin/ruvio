// SPDX-License-Identifier: MIT
// Probe for scripts/seed_failover.py: connects with the seeds in RUVIO_SEEDS, reads `seed:name`,
// waits for a line on stdin (the first server is stopped by then) and reads it again.
//
//   npm run build:test
//   python3 scripts/seed_failover.py target/debug/ruvio -- node clients/node/build/bench/seed-probe.js

import * as readline from 'node:readline';
import { RuvioClient } from '../src/index.js';

const seeds = (process.env.RUVIO_SEEDS ?? '').split(',').filter((s) => s !== '');
if (seeds.length === 0) throw new Error('RUVIO_SEEDS is set by the script');

const client = new RuvioClient({ seeds, connectTimeoutMs: 1000 });
console.log('first ' + ((await client.get('seed:name')) ?? ''));

const lines = readline.createInterface({ input: process.stdin });
await new Promise<void>((resolve) => lines.once('line', () => resolve()));
lines.close();

for (let i = 0; i < 50; i++) {
  try {
    console.log('after ' + ((await client.get('seed:name')) ?? ''));
    client.disconnect();
    process.exit(0);
  } catch {
    await new Promise((r) => setTimeout(r, 100));
  }
}
process.exit(1);
