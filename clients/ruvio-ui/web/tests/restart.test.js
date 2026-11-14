import { test } from "node:test";
import assert from "node:assert/strict";
import { stopCost, shutdownArgs, restartAdvice, waitUntilBack, runIdOf } from "../src/restart.js";

test("memory durability says that every key is lost and asks for NOSAVE", () => {
  assert.equal(stopCost("memory").saves, false);
  assert.match(stopCost("memory").text, /Every key on this node is lost/);
  assert.deepEqual(shutdownArgs("memory"), ["SHUTDOWN", "NOSAVE"]);
});

test("saving profiles and an unknown one send a plain SHUTDOWN", () => {
  for (const profile of ["snapshot", "everysec", "always", undefined, "other"]) {
    assert.deepEqual(shutdownArgs(profile), ["SHUTDOWN"]);
  }
  assert.match(stopCost("everysec").text, /sync window/);
  assert.match(stopCost(undefined).text, /unknown/);
});

test("the advice depends on cluster and role", () => {
  assert.match(restartAdvice({ role: "primary" }, true), /writes pause/);
  assert.match(restartAdvice({ role: "replica" }, true), /catches up/);
  assert.match(restartAdvice({}, false), /supervisor/);
});

const clockOf = () => { const clock = { t: 0 }; return { clock, now: () => clock.t, sleep: async (ms) => { clock.t += ms; } }; };

test("waitUntilBack returns once the node failed a probe and answers again", async () => {
  const { clock, now, sleep } = clockOf();
  const answers = ["a", null, null, "a"];
  const result = await waitUntilBack(async () => { const next = answers.shift(); if (next === null) throw new Error("down"); return next; }, { before: "a", timeoutMs: 10000, now, sleep });
  assert.equal(result.ok, true);
  assert.equal(clock.t, 3000);
});

test("waitUntilBack sees a restart quicker than the polling interval by the new run id", async () => {
  const { now, sleep } = clockOf();
  const answers = ["old", "old", "new"];
  const result = await waitUntilBack(async () => answers.shift(), { before: "old", timeoutMs: 10000, now, sleep });
  assert.equal(result.ok, true);
  assert.equal(result.seconds, 2);
});

test("waitUntilBack gives up at the deadline and says whether the node ever went down", async () => {
  let c = clockOf();
  const up = await waitUntilBack(async () => "same", { before: "same", timeoutMs: 3000, now: c.now, sleep: c.sleep });
  assert.deepEqual([up.ok, up.wentDown], [false, false]);
  c = clockOf();
  const down = await waitUntilBack(async () => { throw new Error("down"); }, { before: "same", timeoutMs: 3000, now: c.now, sleep: c.sleep });
  assert.deepEqual([down.ok, down.wentDown], [false, true]);
});

test("runIdOf reads the run id from an INFO reply", () => {
  assert.equal(runIdOf({ kind: "bulk", text: "# Server\r\nruvio_version:1\r\nrun_id:abc123\r\nx:1" }), "abc123");
  assert.equal(runIdOf({ kind: "bulk", text: "nothing" }), "");
});
