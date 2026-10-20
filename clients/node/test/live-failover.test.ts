// SPDX-License-Identifier: MIT
// A client with two seeds follows a failover on real servers. scripts/run_with_servers.sh starts a
// pair that can each be primary or replica (RUVIO_FAILOVER_ADDRS, their replication addresses in
// RUVIO_FAILOVER_REPLICATION, both comma separated, primary first); no coordinator runs, the test
// swaps the roles itself with REPLICAOF, as a coordinator does. Skipped without the variables.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as net from 'node:net';
import { RuvioClient, RuvioError } from '../src/index.js';
import { until, within } from './helpers.js';

interface Addr {
  host: string;
  port: number;
}

function split(name: string): Addr[] | null {
  const value = process.env[name];
  if (!value) return null;
  const out = value.split(',').map((part) => {
    const i = part.lastIndexOf(':');
    return { host: part.slice(0, i), port: Number(part.slice(i + 1)) };
  });
  return out.length === 2 && out.every((a) => a.host !== '' && a.port > 0) ? out : null;
}

const CLIENTS = split('RUVIO_FAILOVER_ADDRS');
const REPLICATION = split('RUVIO_FAILOVER_REPLICATION');
const SKIP = CLIENTS && REPLICATION ? false : 'RUVIO_FAILOVER_ADDRS / RUVIO_FAILOVER_REPLICATION not set';

const text = (a: Addr) => a.host + ':' + a.port;

/** One command over a raw RESP socket; resolves the reply bytes as text once a whole reply has arrived (bounded). */
function raw(address: Addr, args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host: address.host, port: address.port });
    let data = '';
    const timer = setTimeout(() => finish(new Error('no reply to ' + args[0] + ' from ' + text(address))), 5000);
    const finish = (error: Error | null, value = ''): void => {
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve(value);
    };
    socket.on('error', (e) => finish(e));
    socket.on('close', () => finish(new Error('closed before the reply to ' + args[0])));
    socket.once('connect', () => {
      let out = '*' + args.length + '\r\n';
      for (const a of args) out += '$' + Buffer.byteLength(a) + '\r\n' + a + '\r\n';
      socket.write(out);
    });
    socket.on('data', (d) => {
      data += d.toString('latin1');
      if (complete(data)) finish(null, data);
    });
  });
}

/** True once `data` holds a whole reply of the kinds used here (a line, or an array whose first element is a bulk string). */
function complete(data: string): boolean {
  if (data[0] === '+' || data[0] === '-' || data[0] === ':') return data.includes('\r\n');
  if (data[0] === '*') {
    const m = /^\*\d+\r\n\$(\d+)\r\n/.exec(data);
    return m !== null && data.length >= m[0].length + Number(m[1]) + 2;
  }
  return false;
}

async function isMaster(address: Addr): Promise<boolean> {
  return /^\*\d+\r\n\$6\r\nmaster\r\n/.test(await raw(address, ['ROLE']));
}

function direct(address: Addr): RuvioClient {
  return new RuvioClient({ ...address, commandTimeoutMs: 5000, connectTimeoutMs: 2000 });
}

test('live failover: a client follows the primary after the roles are swapped', { skip: SKIP }, async () => {
  const pair = CLIENTS!;
  const replication = REPLICATION!;
  // Whichever node is the primary now, so the test can run again on the same servers.
  const [old, next] = (await isMaster(pair[0]!)) ? [0, 1] : [1, 0];
  const seeds = [text(pair[old]!), text(pair[next]!)];
  const key = 'failover:node:' + process.pid;
  const opened: RuvioClient[] = [];
  const open = (c: RuvioClient): RuvioClient => {
    opened.push(c);
    return c;
  };
  try {
    await within(
      (async () => {
        const client = open(new RuvioClient({ seeds, commandTimeoutMs: 5000, connectTimeoutMs: 2000 }));
        const onNext = open(direct(pair[next]!));
        await client.set(key, 'before');
        // A replica that still copies refuses to be promoted without FORCE.
        const end = Date.now() + 10_000;
        while ((await onNext.get(key)) !== 'before') {
          assert.ok(Date.now() < end, 'the replica did not hold the first write');
          await new Promise((r) => setTimeout(r, 20));
        }

        assert.match(await raw(pair[next]!, ['REPLICAOF', 'NO', 'ONE']), /^\+OK/);
        assert.match(await raw(pair[old]!, ['REPLICAOF', replication[next]!.host, String(replication[next]!.port)]), /^\+OK/);

        const started = Date.now();
        assert.equal(await client.set(key, 'after'), 'OK');
        assert.ok(Date.now() - started < 5000, 'took ' + (Date.now() - started) + ' ms');
        assert.equal(await onNext.get(key), 'after');

        // Without the option the same situation is the READONLY error.
        const stuck = open(new RuvioClient({ seeds, followPrimary: false, commandTimeoutMs: 5000, connectTimeoutMs: 2000 }));
        // It connects to the first seed, which is now the replica.
        try {
          await stuck.set(key, 'never');
          assert.fail('expected READONLY');
        } catch (e) {
          assert.ok(e instanceof RuvioError && e.code === 'READONLY', String(e));
        }

        // A dropped socket: the reconnect starts at the old primary (now a replica) but takes the node that answers master.
        const id = await client.clientId();
        let dropped = false;
        client.on('disconnect', () => {
          dropped = true;
        });
        await raw(pair[next]!, ['CLIENT', 'KILL', 'ID', String(id)]);
        await until(() => dropped, 'the client to see its socket closed');
        // Right after the reconnect, before any write: the client's own socket is on the primary.
        const role = await client.role();
        assert.equal(role[0], 'master', JSON.stringify(role));
        assert.equal(await client.set(key, 'again'), 'OK');
        assert.equal(await onNext.get(key), 'again');
      })(),
      'the failover scenario',
      60_000,
    );
  } finally {
    for (const c of opened) c.disconnect();
  }
});
