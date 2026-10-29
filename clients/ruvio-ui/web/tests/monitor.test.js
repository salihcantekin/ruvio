import { test } from "node:test";
import assert from "node:assert/strict";
import {
  probeTargets, readingsFrom, primaries, sum, replicationRows, groupsOf, notices, coordinatorModel,
  failoverEvents, snapshot, keepRecent, chartData, formatValue, CHARTS, DEFAULT_CHARTS, rangeText, slotsCovered, isCluster,
} from "../src/monitor.js";

const bulk = (text) => ({ kind: "bulk", text });
const array = (...items) => ({ kind: "array", items });
const fieldList = (object) => array(...Object.entries(object).flatMap(([name, value]) => [bulk(name), bulk(String(value))]));
const info = (fields) => Object.entries(fields).map(([name, value]) => `${name}:${value}`).join("\r\n");

const shards = [
  { id: 0, host: "a", port: 7000, ranges: [[0, 99], [200, 8191]], slots: 8092, replicas: [{ host: "a2", port: 7000, offset: 9 }] },
  { id: 1, host: "a", port: 7001, ranges: [[100, 199], [8192, 16383]], slots: 8292, replicas: [] },
];

test("every primary and replica is probed once, whatever ranges a shard owns", () => {
  const targets = probeTargets([...shards, { ...shards[0], id: 2 }]);
  assert.deepEqual(targets.map((item) => `${item.host}:${item.port}/${item.role}/${item.shard}`), ["a:7000/primary/0", "a2:7000/replica/0", "a:7001/primary/1"]);
});

test("keys and memory add up over primaries only, and a missing value is not zero", () => {
  const targets = probeTargets(shards);
  const readings = readingsFrom(targets, [
    { reachable: true, info: info({ keys: 10, charged_memory_bytes: 100, wal_last_sequence: 50, slave0: "ip=a2,port=7000,state=online,offset=45,lag=1" }) },
    { reachable: true, info: info({ keys: 10, charged_memory_bytes: 100, slave_repl_offset: 45, master_link_status: "up" }) },
    { reachable: true, info: info({ keys: 5, charged_memory_bytes: 40 }) },
  ]);
  assert.equal(sum(primaries(readings), "keys"), 15);
  assert.equal(sum(primaries(readings), "charged_memory_bytes"), 140);
  const broken = readingsFrom(targets, [{ reachable: true, info: info({ keys: 1 }) }, {}, { reachable: false, error: "refused" }]);
  assert.equal(sum(primaries(broken), "keys"), null);
  const [first] = replicationRows(shards, readings);
  assert.equal(first.replicas[0].behind, 5);
  assert.equal(first.replicas[0].ackLag, 1);
  assert.equal(first.replicas[0].link, "up");
});

test("shards group by cluster-group, else by process", () => {
  const targets = probeTargets(shards);
  const readings = readingsFrom(targets, [
    { reachable: true, info: info({ run_id: "r1" }) },
    { reachable: true, info: info({ run_id: "r2" }) },
    { reachable: true, info: info({ run_id: "r1" }) },
  ]);
  assert.deepEqual(groupsOf(shards, readings).map((group) => [group.label, group.shards.length, group.slots]), [["a:7000", 2, 16384]]);
  readings[0].group = "east";
  readings[2].group = "west";
  assert.deepEqual(groupsOf(shards, readings).map((group) => [group.label, group.index]), [["east", 0], ["west", 1]]);
});

test("notices name unreachable nodes, fenced primaries, broken links and map versions", () => {
  const targets = probeTargets(shards);
  const readings = readingsFrom(targets, [
    { reachable: true, topology_version: 3, info: info({ replication_fenced: 1 }) },
    { reachable: true, topology_version: 4, info: info({ master_link_status: "down", master_sync_in_progress: 1 }) },
    { reachable: false, error: "refused" },
  ]);
  const texts = notices(shards, readings).map((item) => `${item.level}: ${item.text}`);
  assert.ok(texts[0].startsWith("danger"));
  assert.ok(texts.some((text) => text.includes("a:7001 (shard 1 primary) is not answering")));
  assert.ok(texts.some((text) => text.includes("refuses writes")));
  assert.ok(texts.some((text) => text.includes("lost its link")));
  assert.ok(texts.some((text) => text.includes("full copy")));
  assert.ok(texts.some((text) => text.includes("versions (3, 4)")));
  assert.equal(slotsCovered(shards), 16384);
  assert.ok(notices(shards.slice(0, 1), []).some((item) => item.text.includes("8,092 of 16,384")));
});

const chart = (id) => CHARTS.find((item) => item.id === id);

test("rates come from counter deltas, gauges add up, and a restarted node is left out", () => {
  const targets = probeTargets(shards);
  const point = (at, a, replica, b) => snapshot(readingsFrom(targets, [
    { reachable: true, info: info({ run_id: "p1", total_commands_processed: a, charged_memory_bytes: 100, total_connections_received: a }) },
    { reachable: true, info: info({ run_id: "p2", total_commands_processed: replica, charged_memory_bytes: 100 }) },
    { reachable: true, info: info({ run_id: "p1", total_commands_processed: b, charged_memory_bytes: 50, total_connections_received: a }) },
  ], at), at);
  const history = [point(0, 100, 10, 0), point(2000, 300, 30, 40), point(4000, 500, 50, 10)];
  const commands = chartData(chart("commands"), history).series[0].points.map((item) => item.value);
  assert.deepEqual(commands, [100 + 10 + 20, 100 + 10]);
  // Memory is a gauge over primaries only: the replica's copy is not counted.
  assert.deepEqual(chartData(chart("memory"), history).series[0].points.map((item) => item.value), [150, 150, 150]);
  // A process-wide counter is taken once per process, not once per shard.
  assert.deepEqual(chartData(chart("connections"), history).series[0].points.map((item) => item.value), [100, 100]);
  // Narrowed to one shard's nodes.
  assert.deepEqual(chartData(chart("commands"), history, { keys: ["a:7001"] }).series[0].points.map((item) => item.value), [20, null]);
  const perNode = chartData(chart("commands"), history, { view: "nodes" });
  assert.deepEqual(perNode.series.map((series) => series.label), ["Shard 0 \u00b7 a:7000", "Replica of 0 \u00b7 a2:7000", "Shard 1 \u00b7 a:7001"]);
  assert.equal(chartData(chart("memory-breakdown"), history).stacked, true);
  assert.deepEqual(chartData(chart("memory"), history, { windowMs: 2000 }).series[0].points.map((item) => item.at), [2000, 4000]);
});

test("per-node lines stop at seven plus Other, so colors never repeat", () => {
  const many = Array.from({ length: 10 }, (_, index) => ({ id: index, host: "h", port: 7000 + index, ranges: [], slots: 0, replicas: [] }));
  const targets = probeTargets(many);
  const history = [snapshot(readingsFrom(targets, targets.map((_, index) => ({ reachable: true, info: info({ connected_clients: index }) })), 0), 0)];
  const series = chartData(chart("clients"), history, { view: "nodes" }).series;
  assert.equal(series.length, 8);
  assert.equal(series[7].label, "Other 3 nodes");
  assert.equal(series[7].points[0].value, 0 + 1 + 2);
});

test("history keeps the last 30 minutes and the default charts exist", () => {
  let history = [];
  for (const at of [0, 60_000, 31 * 60_000]) history = keepRecent(history, { at, nodes: {} });
  assert.deepEqual(history.map((item) => item.at), [60_000, 31 * 60_000]);
  assert.ok(DEFAULT_CHARTS.length >= 5 && DEFAULT_CHARTS.every((id) => chart(id)));
  assert.equal(formatValue("bytes", 1536, true), "1.5 KiB/s");
  assert.equal(formatValue("count", 12500), "12.5k");
  assert.equal(formatValue("count", null), "Not reported");
});

test("coordinator reports become groups, a leader and seen failovers", () => {
  const report = (primary, epoch) => ({
    configured: true,
    source: "c3:26400",
    coordinators: [
      { address: "c1:26400", ok: false, error: "refused" },
      { address: "c3:26400", ok: true, info: `coordinator_id:3\r\nleader_id:3\r\nis_leader:1\r\nfailovers_total:1\r\nlast_failover_unix_ms:5\r\ngroup_main:primary=${primary},epoch=${epoch},members_answering=1/2,state=ok,state_ms=1200\r\n` },
    ],
    masters: array(fieldList({ name: "main", ip: "n1", port: 6379 })),
    replicas: { main: array(fieldList({ name: "n2:6379", ip: "n2", port: 6379 })) },
    status: fieldList({ state: "none", moved_ranges: 0 }),
  });
  const before = coordinatorModel(report("n1:6379", 1));
  assert.equal(before.leader.id, "3");
  assert.equal(before.reachable, 1);
  assert.deepEqual([before.groups[0].answering, before.groups[0].members, before.groups[0].state, before.groups[0].stateMs], [1, 2, "ok", 1200]);
  assert.equal(before.groups[0].replicas[0].ip, "n2");
  assert.equal(before.status.moved_ranges, "0");
  const after = coordinatorModel(report("n2:6379", 2));
  assert.deepEqual(failoverEvents(before, after, 7), [{ at: 7, group: "main", from: "n1:6379", to: "n2:6379", epoch: 2 }]);
  assert.deepEqual(failoverEvents(before, before), []);
  assert.deepEqual(coordinatorModel({ configured: false }), { configured: false });
});

test("ranges read as a short line", () => {
  assert.equal(rangeText(shards[0]), "0–99, 200–8191");
  assert.equal(rangeText({ ranges: [[1, 1], [3, 4], [6, 9]] }), "1, 3–4 +1 more");
  assert.equal(rangeText({ ranges: [] }), "No slots");
});

test("one process with several shards is not a cluster; replicas, groups, processes or coordinators are", () => {
  const alone = shards.map((shard) => ({ ...shard, replicas: [] }));
  const targets = probeTargets(alone);
  const sameProcess = readingsFrom(targets, [{ reachable: true, info: info({ run_id: "r1" }) }, { reachable: true, info: info({ run_id: "r1" }) }]);
  assert.equal(isCluster(alone, sameProcess, false), false);
  assert.equal(isCluster(alone, [], false), false);
  assert.equal(isCluster(alone, sameProcess, true), true);
  assert.equal(isCluster(shards, sameProcess, false), true);
  const twoProcesses = readingsFrom(targets, [{ reachable: true, info: info({ run_id: "r1" }) }, { reachable: true, info: info({ run_id: "r2" }) }]);
  assert.equal(isCluster(alone, twoProcesses, false), true);
  sameProcess[0].group = "east";
  assert.equal(isCluster(alone, sameProcess, false), true);
});
