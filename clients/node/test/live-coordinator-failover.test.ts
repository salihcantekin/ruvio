// SPDX-License-Identifier: MIT
// Clients follow a failover that real coordinators carry out. scripts/run_with_servers.sh starts a
// group of two data nodes (RUVIO_COORDINATOR_GROUP_ADDRS, their pids in RUVIO_COORDINATOR_GROUP_PIDS)
// watched by three `ruvio coordinator` processes (RUVIO_COORDINATOR_ADDRS, the group named
// RUVIO_COORDINATOR_MASTER). The test kills the primary with SIGKILL, so the group serves one run of
// the script. Skipped without the variables.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioClient } from '../src/index.js';
import { within } from './helpers.js';

// The coordinators notice the kill after down_after_ms (1 s) and promote within a few ticks.
const PATIENCE_MS = 15_000;
/** Each client's first write after the kill comes within this, the seeds client included: the replica that refused is taken once it is promoted, not after the rounds run out (about 7 s). */
const FOLLOW_BOUND_MS = 5_000;

const list = (name: string): string[] | null => (process.env[name] ? process.env[name]!.split(',') : null);

const COORDINATORS = list('RUVIO_COORDINATOR_ADDRS');
const MASTER = process.env['RUVIO_COORDINATOR_MASTER'];
const MEMBERS = list('RUVIO_COORDINATOR_GROUP_ADDRS');
const PIDS = list('RUVIO_COORDINATOR_GROUP_PIDS');
const SKIP = COORDINATORS && MASTER && MEMBERS && PIDS ? false : 'RUVIO_COORDINATOR_* not set';

function direct(address: string): RuvioClient {
  const i = address.lastIndexOf(':');
  return new RuvioClient({ host: address.slice(0, i), port: Number(address.slice(i + 1)), commandTimeoutMs: 5000, connectTimeoutMs: 1000 });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test('live coordinator failover: clients reach the new primary after the coordinators fail over', { skip: SKIP }, async () => {
  const opened: RuvioClient[] = [];
  const open = (c: RuvioClient): RuvioClient => {
    opened.push(c);
    return c;
  };
  try {
    await within(
      (async () => {
        const members = MEMBERS!.map((address) => open(direct(address)));
        const roles = await Promise.all(members.map((m) => m.role()));
        const old = roles.findIndex((role) => role[0] === 'master');
        const next = 1 - old;
        assert.ok(old >= 0 && roles[next]![0] !== 'master', 'the group does not have exactly one primary: ' + JSON.stringify(roles));

        // One client asks the coordinators for the primary, the other has the data nodes as seeds.
        const clients = {
          sentinelMaster: open(new RuvioClient({ seeds: COORDINATORS!, sentinelMaster: MASTER!, commandTimeoutMs: 5000, connectTimeoutMs: 1000 })),
          seeds: open(new RuvioClient({ seeds: MEMBERS!, commandTimeoutMs: 5000, connectTimeoutMs: 1000 })),
        };
        const key = (name: string) => 'coordinated:node:' + process.pid + ':' + name;
        for (const [name, client] of Object.entries(clients)) assert.equal(await client.set(key(name), 'before'), 'OK');
        // Only a replica that holds every acknowledged write is promoted.
        const end = Date.now() + PATIENCE_MS;
        for (const name of Object.keys(clients)) {
          while ((await members[next]!.get(key(name))) !== 'before') {
            assert.ok(Date.now() < end, 'the replica did not hold the first writes');
            await sleep(20);
          }
        }

        process.kill(Number(PIDS![old]), 'SIGKILL');
        const started = Date.now();

        // Every client writes at once, so none waits for another.
        const took = await Promise.all(
          Object.entries(clients).map(async ([name, client]) => {
            for (;;) {
              try {
                assert.equal(await client.set(key(name), 'after'), 'OK');
                return [name, Date.now() - started] as const;
              } catch (e) {
                assert.ok(Date.now() - started < PATIENCE_MS, name + ': no write succeeded: ' + String(e));
              }
              await sleep(50);
            }
          }),
        );
        console.log('first write after the kill (ms):', Object.fromEntries(took));
        for (const [name, ms] of took) assert.ok(ms < FOLLOW_BOUND_MS, name + ': the first write took ' + ms + ' ms');

        assert.equal((await members[next]!.role())[0], 'master');
        for (const name of Object.keys(clients)) assert.equal(await members[next]!.get(key(name)), 'after');
      })(),
      'the coordinator failover scenario',
      60_000,
    );
  } finally {
    for (const c of opened) c.disconnect();
  }
});
