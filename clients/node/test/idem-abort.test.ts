// SPDX-License-Identifier: MIT
// IDEM ABORT: the exact arguments, the reply mapping and the idempotent() flag, against a scripted server.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RuvioClient } from '../src/index.js';
import { fakeServer, int } from './fake-server.js';

async function withServer(
  abortReply: string,
  fn: (c: RuvioClient, seen: () => string[][]) => Promise<void>,
): Promise<void> {
  const s = await fakeServer((args) => {
    if (args[0] === 'HELLO') return undefined;
    if (args[2] === 'BEGIN') return '*1\r\n$8\r\nacquired\r\n';
    if (args[2] === 'ABORT') return abortReply;
    return int(1);
  });
  const c = new RuvioClient({ host: '127.0.0.1', port: s.port });
  try {
    await fn(c, () => s.seen.filter((a) => a[0] !== 'HELLO'));
  } finally {
    c.disconnect();
    await s.close();
  }
}

test('idemAbort sends IDEM key ABORT fingerprint owner and maps 1/0 to a boolean', async () => {
  await withServer(int(1), async (c, seen) => {
    assert.equal(await c.idemAbort('k', 'fp', 'me'), true);
    assert.deepEqual(seen(), [['IDEM', 'k', 'ABORT', 'fp', 'me']]);
  });
  await withServer(int(0), async (c) => {
    assert.equal(await c.idemAbort('k', 'fp', 'me'), false);
  });
});

test('idempotent sends ABORT only with abortOnError and always rethrows the work error', async () => {
  for (const abortOnError of [false, true]) {
    await withServer(int(1), async (c, seen) => {
      await assert.rejects(
        c.idempotent('k', 'fp', 1000, () => Promise.reject(new Error('boom')), { owner: 'me', abortOnError }),
        { message: 'boom' },
      );
      assert.equal(seen().some((a) => a[2] === 'ABORT'), abortOnError);
    });
  }
});

test('a failed abort does not hide the work error', async () => {
  await withServer('-ERR INPUT bad\r\n', async (c) => {
    await assert.rejects(
      c.idempotent('k', 'fp', 1000, () => Promise.reject(new Error('boom')), { owner: 'me', abortOnError: true }),
      { message: 'boom' },
    );
  });
});
