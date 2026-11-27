// SPDX-License-Identifier: MIT
// Probe for scripts/client_faults.py: writes fault:<name>:<i> = i with durability replicas 1, reads
// each key back, and keeps going through a failover until a line arrives on stdin. Environment:
// RUVIO_SEEDS, RUVIO_PROBE_NAME and, for TLS, RUVIO_PROBE_CA (PEM path). Output, one line each:
// ready, ack <i>, err <i> <text>, bad <i> <value>, done.
//
//   npm run build:test
//   python3 scripts/client_faults.py target/debug/ruvio

import { readFileSync } from 'node:fs';
import { RuvioClient } from '../src/index.js';

const name = process.env.RUVIO_PROBE_NAME ?? '';
const ca = process.env.RUVIO_PROBE_CA;
const client = new RuvioClient({
  seeds: (process.env.RUVIO_SEEDS ?? '').split(',').filter((s) => s !== ''),
  durability: { replicas: 1 },
  ...(ca ? { tls: { ca: readFileSync(ca) } } : {}),
});

let stop = false;
process.stdin.once('data', () => {
  stop = true;
});

console.log('ready');
for (let i = 1; !stop; ) {
  const key = `fault:${name}:${i}`;
  try {
    await client.set(key, String(i));
    const got = await client.get(key);
    if (got !== String(i)) console.log(`bad ${i} ${got}`);
    console.log(`ack ${i}`);
    i++;
  } catch (error) {
    console.log(`err ${i} ${(error as Error).constructor.name}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
console.log('done');
client.disconnect();
process.exit(0);
