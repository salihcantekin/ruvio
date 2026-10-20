// SPDX-License-Identifier: MIT
// Runs against the multi-node cluster scripts/run_with_servers.sh starts: node a
// (RUVIO_MULTINODE_A_ADDR, slots 0-8191), node b (RUVIO_MULTINODE_B_ADDR, slots 8192-16383)
// and a node with an older map (RUVIO_MULTINODE_STALE_ADDR) that gives every slot to node a.
// Skipped without them.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClusterClient, RuvioClient, hashSlot } from '../src/index.js';

const A = process.env.RUVIO_MULTINODE_A_ADDR;
const B = process.env.RUVIO_MULTINODE_B_ADDR;
const STALE = process.env.RUVIO_MULTINODE_STALE_ADDR;
const SKIP =
  A && B && STALE
    ? false
    : 'RUVIO_MULTINODE_A_ADDR / RUVIO_MULTINODE_B_ADDR / RUVIO_MULTINODE_STALE_ADDR not set';

function split(address: string): { host: string; port: number } {
  const i = address.lastIndexOf(':');
  return { host: address.slice(0, i), port: Number(address.slice(i + 1)) };
}

/** The key's value read from one node, without cluster routing. */
async function getOnNode(address: string, key: string): Promise<string | null> {
  const node = new RuvioClient(split(address));
  try {
    return await node.get(key);
  } finally {
    node.disconnect();
  }
}

test('live multi-node: follows MOVED from a node with an older map', { skip: SKIP }, async () => {
  // Node b's slot (12182) and node a's (5061).
  const onB = '{foo}node:multinode';
  const onA = '{bar}node:multinode';
  assert.equal(hashSlot(onB), 12182);
  assert.equal(hashSlot(onA), 5061);

  // Seeded with the stale node, the client first sends node b's key to node a, which
  // answers MOVED.
  const db = new ClusterClient(split(STALE!));
  try {
    await db.set(onB, 'b');
    await db.set(onA, 'a');
    assert.equal(await db.get(onB), 'b');
    assert.equal(await db.get(onA), 'a');
  } finally {
    db.disconnect();
  }

  // Each key is on the node that owns it.
  assert.equal(await getOnNode(B!, onB), 'b');
  assert.equal(await getOnNode(A!, onA), 'a');
});
