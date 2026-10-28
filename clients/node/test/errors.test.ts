// SPDX-License-Identifier: MIT
// The retry classification every client shares: which failures a retry can get past and which may have run.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionError, ReplyError, RuvioError, isTransient, mayHaveRun } from '../src/index.js';

const TRANSIENT_REPLIES = [
  'LOADING Ruvio is loading the dataset in memory',
  'MASTERDOWN Link with MASTER is down',
  'CLUSTERDOWN The cluster is down',
  'TRYAGAIN slot is moving',
  "READONLY You can't write against a read only replica.",
  'NOREPLICAS Not enough good replicas to write.',
];

test('retryable replies and connection failures are transient', () => {
  for (const reply of TRANSIENT_REPLIES) assert.equal(isTransient(new RuvioError(reply)), true, reply);
  for (const reply of ['WRONGTYPE Operation against a key', 'OOM command not allowed', 'ERR syntax error', 'READONLYX x', 'LOAD']) {
    assert.equal(isTransient(new RuvioError(reply)), false, reply);
  }
  assert.equal(isTransient(new ConnectionError('ruvio: command timeout after 5 ms')), true);
  assert.equal(isTransient(new ReplyError('unexpected reply')), false);
  assert.equal(isTransient(new DOMException('aborted', 'AbortError')), false);
  assert.equal(isTransient(new Error('bad')), false);
  assert.equal(isTransient(undefined), false);
});

test('only a refusing server reply surely did not run', () => {
  assert.equal(mayHaveRun(new RuvioError('WRONGTYPE Operation against a key')), false);
  assert.equal(mayHaveRun(new RuvioError('TRYAGAIN slot is moving')), false);
  assert.equal(mayHaveRun(new RuvioError('READONLY replica')), false);
  assert.equal(mayHaveRun(new RuvioError('NOREPLICAS Not enough good replicas to write.')), true);
  assert.equal(mayHaveRun(new RuvioError('NOREPLICAS')), true);
  assert.equal(mayHaveRun(new ConnectionError('ruvio: connection closed')), true);
  assert.equal(mayHaveRun(new ReplyError('unexpected reply')), true);
});
