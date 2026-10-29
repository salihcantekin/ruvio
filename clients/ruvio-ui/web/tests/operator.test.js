import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { families, findTasks } from "../src/catalog.js";
import { scanOwners, scanSelected, metric, bytes, execute } from "../src/operator.js";
import { parseInfoText, sum } from "../src/monitor.js";
import { present } from "../src/value.js";
import { api } from "../src/api.js";

const bulk = (text) => ({ kind: "bulk", text });
const array = (...items) => ({ kind: "array", items });
const command = (name) => families.flatMap((family) => family.commands).find((item) => item.name === name);
const matrix = JSON.parse(readFileSync(new URL("../../../../docs/compatibility-matrix.json", import.meta.url)));

test("every form is backed by a supported compatibility-matrix command", () => {
  const supported = new Set(matrix.commands.filter((item) => item.status === "supported").map((item) => item.name));
  for (const family of families) {
    for (const item of family.commands) {
      const fields = Object.fromEntries(item.fields.map((field) => [field.name, field.lines ? "field\nvalue" : "1"]));
      const args = item.build(fields);
      assert.ok(supported.has(args[0]) || supported.has(args.slice(0, 2).join(" ")), item.name);
    }
  }
});

test("job search finds intent and command names", () => {
  for (const [query, name] of [
    ["locking", "LEASE"], ["locking", "RELEASE"], ["conditional write", "SETV"],
    ["rate limiting", "LIMIT"], ["idempotency", "ONCE"], ["inventory", "TAKE"],
    ["idempotency", "IDEM BEGIN"], ["idempotency", "IDEM COMPLETE"],
    ["sliding session", "GETEX"], ["delayed job", "XADD DELAY"], ["change feed", "CHANGES"],
    ["strings", "SET"], ["lists", "LPUSH"], ["sets", "SADD"], ["hashes", "HSET"],
    ["sorted sets", "ZADD"], ["streams", "XADD"], ["HGET", "HGET"],
    ["semaphore", "SEMAPHORE"], ["capacity", "INCRBY MAX"], ["field ttl", "HEXPIRE"], ["field ttl", "HTTL"], ["priority queue", "ZPOPMIN"], ["leaderboard", "ZREMRANGEBYRANK"],
  ]) assert.ok(findTasks(query).some((item) => item.command.name === name), `${query}: ${name}`);
});

test("new command forms use Ruvio syntax and plain SET is unconditional", () => {
  assert.deepEqual(command("SET").build({ key: "k", value: "v", token: "3", version: "9" }), ["SET", "k", "v"]);
  assert.deepEqual(command("SETV").build({ key: "k", value: "v" }), ["SETV", "k", "v"]);
  assert.deepEqual(command("SETV").build({ key: "k", value: "v", version: "2" }), ["SETV", "k", "v", "2"]);
  assert.deepEqual(command("LEASE").build({ key: "k", ms: "50" }), ["LEASE", "k", "50"]);
  assert.deepEqual(command("RELEASE").build({ key: "k", token: "1" }), ["RELEASE", "k", "1"]);
  assert.deepEqual(command("LIMIT").build({ key: "k", max: "10", ms: "1000" }), ["LIMIT", "k", "10", "1000"]);
  assert.deepEqual(command("ONCE").build({ key: "k", ms: "1000" }), ["ONCE", "k", "1000"]);
  assert.deepEqual(command("IDEM BEGIN").build({ key: "k", fingerprint: "fp", owner: "token", ms: "1000" }), ["IDEM", "k", "BEGIN", "fp", "token", "1000"]);
  assert.deepEqual(command("IDEM COMPLETE").build({ key: "k", fingerprint: "fp", owner: "token", value: "" }), ["IDEM", "k", "COMPLETE", "fp", "token", ""]);
  assert.deepEqual(command("TAKE").build({ key: "k", count: "2" }), ["TAKE", "k", "2"]);
  assert.deepEqual(command("GETEX").build({ key: "k", ms: "1000" }), ["GETEX", "k", "1000"]);
  assert.deepEqual(command("XADD DELAY").build({ key: "k", ms: "1000", fields: "task\nemail" }), ["XADD", "k", "DELAY", "1000", "*", "task", "email"]);
  assert.deepEqual(command("CHANGES START").build({}), ["CHANGES", "START"]);
  assert.deepEqual(command("CHANGES").build({ cursor: "9" }), ["CHANGES", "9"]);
  assert.throws(() => command("XADD").build({ key: "k", fields: "field" }), /Every field needs a value/);
  assert.deepEqual(command("HEXPIRE").build({ key: "h", seconds: "60", when: "", fields: "a\nb" }), ["HEXPIRE", "h", "60", "FIELDS", "2", "a", "b"]);
  assert.deepEqual(command("HEXPIRE").build({ key: "h", seconds: "60", when: "GT", fields: "a" }), ["HEXPIRE", "h", "60", "GT", "FIELDS", "1", "a"]);
  assert.deepEqual(command("HTTL").build({ key: "h", fields: "a" }), ["HTTL", "h", "FIELDS", "1", "a"]);
  assert.deepEqual(command("HPERSIST").build({ key: "h", fields: "a" }), ["HPERSIST", "h", "FIELDS", "1", "a"]);
  assert.throws(() => command("HTTL").build({ key: "h", fields: "" }), /at least one field/);
  assert.deepEqual(command("SEMAPHORE").build({ key: "s", max: "3", ms: "5000" }), ["SEMAPHORE", "s", "3", "5000"]);
  assert.deepEqual(command("INCRBY MAX").build({ key: "c", by: "2", max: "10" }), ["INCRBY", "c", "2", "MAX", "10"]);
  assert.deepEqual(command("ZPOPMIN").build({ key: "z" }), ["ZPOPMIN", "z", "1"]);
  assert.deepEqual(command("ZPOPMAX").build({ key: "z", count: "3" }), ["ZPOPMAX", "z", "3"]);
  assert.deepEqual(command("ZREMRANGEBYRANK").build({ key: "z" }), ["ZREMRANGEBYRANK", "z", "0", "-11"]);
  assert.deepEqual(command("ZREMRANGEBYSCORE").build({ key: "z", max: "(5" }), ["ZREMRANGEBYSCORE", "z", "-inf", "(5"]);
});

test("all-shard SCAN crosses empty owner pages without losing the continuation", async () => {
  const calls = [];
  const pages = [array(bulk("owner:1"), array()), array(bulk("owner:2"), array(bulk("a"))), array(bulk("owner:3"), array(bulk("b")))];
  const result = await scanOwners(["SCAN", "0", "COUNT", "2"], async (args) => {
    calls.push(args);
    return pages.shift();
  });
  assert.deepEqual(calls.map((args) => args[1]), ["0", "owner:1", "owner:2"]);
  assert.deepEqual(result, array(bulk("owner:3"), array(bulk("a"), bulk("b"))));
});

test("SCAN handles completion, refuses malformed replies, and propagates server errors", async () => {
  assert.deepEqual(await scanOwners(["SCAN", "0"], async () => array(bulk("0"), array())), array(bulk("0"), array()));
  await assert.rejects(scanOwners(["SCAN", "5"], async () => array(bulk("5"), array())), /did not advance/);
  await assert.rejects(scanOwners(["SCAN", "0"], async () => bulk("bad")), /Unexpected SCAN/);
  await assert.rejects(scanOwners(["SCAN", "0"], async () => { throw new Error("NOPERM"); }), /NOPERM/);
});

test("selected-owner scans keep node-local continuation and deduplicate keys", async () => {
  const nodes = [{ host: "a", port: 1 }, { host: "b", port: 2 }];
  const calls = [];
  const command = async (args, node) => {
    calls.push([args[1], node.host]);
    return node.host === "a" ? array(bulk("0"), array(bulk("a"))) : array(bulk("0"), array(bulk("a"), bulk("b")));
  };
  const first = await scanSelected(["SCAN", "0", "COUNT", "1"], nodes, command);
  assert.equal(first.items[0].text, "selected:1:0");
  const next = await scanSelected(["SCAN", first.items[0].text, "COUNT", "40"], nodes, command);
  assert.equal(next.items[0].text, "0");
  assert.deepEqual(calls, [["0", "a"], ["0", "b"]]);
  assert.deepEqual(next.items[1].items.map((item) => item.text), ["a", "b"]);
  await assert.rejects(scanSelected(["SCAN", "selected:9:0"], nodes, command), /Invalid/);
});

test("metrics use charged memory, preserve zero, and do not fabricate incomplete totals", () => {
  const info = parseInfoText("# Memory\r\ncharged_memory_bytes:1024\r\n# Keyspace\r\nkeys:0\r\n");
  assert.equal(metric(info, "keys"), 0);
  assert.equal(metric(info, "total_commands_processed"), null);
  assert.equal(sum([{ info }, { info: { keys: "4" } }], "keys"), 4);
  assert.equal(sum([{ info }, { info: null }], "keys"), null);
  assert.equal(sum([], "keys"), null);
  assert.equal(metric({ keys: "bad" }, "keys"), null);
  assert.equal(bytes(metric(info, "charged_memory_bytes")), "1.0 KiB");
  assert.equal(parseInfoText(""), null);
});

test("change-feed notes are not mislabelled as SCAN keys", () => {
  const feed = array(array({ kind: "integer", value: 4 }, bulk("SET k")));
  assert.equal(present("CHANGES", feed).type, "tree");
  assert.equal(present("SCAN", array(bulk("0"), array(bulk("k")))).type, "scan");
});

test("HTTP, transport and RESP errors remain failures; a next request still works", async (t) => {
  const mock = t.mock.method(globalThis, "fetch");
  mock.mock.mockImplementationOnce(async () => new Response(JSON.stringify({ ok: false, error: "WRONGTYPE" })));
  await assert.rejects(api("/api/command", { args: ["GET", "k"] }), /WRONGTYPE/);
  mock.mock.mockImplementationOnce(async () => new Response(JSON.stringify({ ok: true, value: { kind: "error", text: "ERR denied" } })));
  await assert.rejects(execute(["GET", "k"]), /ERR denied/);
  mock.mock.mockImplementationOnce(async () => { throw new Error("network disconnected"); });
  await assert.rejects(api("/api/defaults"), /network disconnected/);
  mock.mock.mockImplementationOnce(async () => new Response(JSON.stringify({ ok: true, value: bulk("ok") })));
  assert.deepEqual(await execute(["GET", "k"]), bulk("ok"));
});

test("command arguments are always sent as text, even when a number input produced a number", async (t) => {
  const mock = t.mock.method(globalThis, "fetch", async () => new Response(JSON.stringify({ ok: true, value: bulk("ok") })));
  await execute(["EXPIRE", "k", 3600]);
  assert.deepEqual(JSON.parse(mock.mock.calls[0].arguments[1].body).args, ["EXPIRE", "k", "3600"]);
});

test("read-only HTTP failures retry but writes are never replayed", async (t) => {
  const mock = t.mock.method(globalThis, "fetch");
  let attempts = 0;
  mock.mock.mockImplementation(async () => {
    attempts += 1;
    if (attempts === 1 || attempts === 3) throw new TypeError("Failed to fetch");
    return new Response(JSON.stringify(attempts === 2
      ? { ok: true, value: bulk("ok") }
      : { ok: false, error: "NOPERM" }));
  });
  assert.deepEqual(await execute(["GET", "k"]), bulk("ok"));
  assert.equal(mock.mock.callCount(), 2);
  await assert.rejects(execute(["INCR", "k"]), /Failed to fetch/);
  assert.equal(mock.mock.callCount(), 3);
  await assert.rejects(execute(["GET", "k"]), /NOPERM/);
  assert.equal(mock.mock.callCount(), 4);
});

test("JSON forms validate documents and use the server path dialect", async () => {
  const set = command("JSON.SET");
  assert.deepEqual(set.build({ key: "doc", path: "", value: '{ "a": [1, 2] }' }), ["JSON.SET", "doc", "$", '{"a":[1,2]}']);
  assert.deepEqual(set.build({ key: "doc", path: "$.a[0]", value: "7" }), ["JSON.SET", "doc", "$.a[0]", "7"]);
  assert.throws(() => set.build({ key: "doc", path: "$", value: "{oops" }), /not valid JSON/);
  assert.throws(() => set.build({ key: "doc", path: "$", value: "  " }), /Enter the value/);
  assert.deepEqual(command("JSON.GET").build({ key: "doc" }), ["JSON.GET", "doc", "$"]);
  assert.deepEqual(command("JSON.DEL").build({ key: "doc", path: "" }), ["JSON.DEL", "doc"]);
  assert.deepEqual(command("JSON.DEL").build({ key: "doc", path: "$.a" }), ["JSON.DEL", "doc", "$.a"]);
  assert.deepEqual(command("JSON.MGET").build({ keys: "a\nb", path: "$.n" }), ["JSON.MGET", "a", "b", "$.n"]);
  assert.deepEqual(command("JSON.NUMINCRBY").build({ key: "doc", path: "$.n", by: "2.5" }), ["JSON.NUMINCRBY", "doc", "$.n", "2.5"]);
  assert.deepEqual(command("JSON.FIND").build({ name: "i", value: "x", count: "5", after: "k" }), ["JSON.FIND", "i", "VALUE", "x", "COUNT", "5", "AFTER", "k"]);
  assert.ok(findTasks("json").length >= 15);
  assert.ok(findTasks("merge a patch").some((item) => item.command.name === "JSON.MERGE"));
});

test("JSON helpers address only what the server can address", async () => {
  const { childPath, kindOf, documentOf, preview, describe, entries } = await import("../src/json.js");
  assert.equal(childPath("$", "name"), "$.name");
  assert.equal(childPath("$.a", 2), "$.a[2]");
  assert.equal(childPath("$", "has space"), null);
  assert.equal(childPath("$", "a.b"), null);
  assert.equal(childPath(null, "x"), null);
  assert.equal(kindOf(null), "null");
  assert.equal(kindOf([]), "array");
  assert.equal(kindOf({}), "object");
  assert.equal(describe([1, 2]), "array · 2 items");
  assert.equal(describe({ a: 1 }), "object · 1 key");
  assert.equal(preview("x".repeat(100), 10), `"xxxxxxxxx…`);
  assert.deepEqual(entries(["a"]), [[0, "a"]]);
  assert.equal(documentOf("JSON.GET", bulk('{"a":1}')).value.a, 1);
  assert.equal(documentOf("JSON.GET", bulk("3")), null);
  assert.equal(documentOf("JSON.GET", bulk("not json")), null);
  assert.equal(documentOf("GET", bulk('{"a":1}')), null);
  assert.equal(present("JSON.GET", bulk('[1,2]')).type, "json");
  assert.equal(present("JSON.GET", bulk('"text"')).type, "scalar");
});
