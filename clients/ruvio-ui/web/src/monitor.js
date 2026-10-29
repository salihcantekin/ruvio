// Live cluster readings: which nodes to ask, and what their INFO replies add up to.
// Pure functions, so the Node checks can cover them without a browser.
import { metric } from "./operator.js";

export const SLOT_COUNT = 16384;

export const nodeKey = (node) => `${node.host}:${node.port}`;

// Every primary and replica of the topology once, primary first, with the shard it serves.
export function probeTargets(shards) {
  const targets = new Map();
  const add = (node, extra) => {
    const key = nodeKey(node);
    if (!targets.has(key)) targets.set(key, { host: node.host, port: node.port, ...extra });
  };
  for (const shard of shards) {
    add(shard, { shard: shard.id, role: "primary" });
    for (const replica of shard.replicas ?? []) add(replica, { shard: shard.id, role: "replica" });
  }
  return [...targets.values()];
}

export function parseInfoText(text) {
  const info = {};
  for (const line of String(text ?? "").split(/\r?\n/)) {
    const colon = line.indexOf(":");
    if (colon > 0 && !line.startsWith("#")) info[line.slice(0, colon)] = line.slice(colon + 1);
  }
  return Object.keys(info).length ? info : null;
}

// One probe reply per target, in target order.
export function readingsFrom(targets, replies, at = Date.now()) {
  return targets.map((node, index) => {
    const reply = replies[index] ?? {};
    const info = parseInfoText(reply.info);
    return {
      node,
      at,
      reachable: !!reply.reachable,
      info,
      ms: reply.ms ?? null,
      error: reply.error || (reply.reachable && !info ? "INFO returned no metrics." : ""),
      topologyVersion: reply.topology_version ?? null,
      group: reply.group ?? null,
    };
  });
}

export const primaries = (readings) => readings.filter((reading) => reading.node.role === "primary");
export const replicas = (readings) => readings.filter((reading) => reading.node.role === "replica");

// A sum that is only reported when every reading has the field; a missing value is never zero.
export function sum(readings, field) {
  const values = readings.map((reading) => metric(reading.info, field));
  return values.length && values.every((value) => value !== null)
    ? values.reduce((total, value) => total + value, 0)
    : null;
}

// `slave0:ip=…,port=…,state=online,offset=…,lag=…` lines of a primary's INFO.
export function replicaLines(info) {
  return Object.entries(info ?? {})
    .filter(([name]) => /^slave\d+$/.test(name))
    .map(([, value]) => Object.fromEntries(value.split(",").map((part) => part.split("="))))
    .map((line) => ({ ...line, port: Number(line.port), offset: Number(line.offset), lag: Number(line.lag) }));
}

// Per shard: the primary's reading and each replica with how far behind it is.
export function replicationRows(shards, readings) {
  const byKey = new Map(readings.map((reading) => [nodeKey(reading.node), reading]));
  return shards.map((shard) => {
    const primary = byKey.get(nodeKey(shard)) ?? null;
    const sequence = metric(primary?.info, "wal_last_sequence");
    const announced = replicaLines(primary?.info);
    return {
      shard,
      primary,
      sequence,
      replicas: (shard.replicas ?? []).map((replica) => {
        const reading = byKey.get(nodeKey(replica)) ?? null;
        const applied = metric(reading?.info, "slave_repl_offset");
        const line = announced.find((item) => item.port === replica.port) ?? null;
        return {
          node: replica,
          reading,
          applied,
          behind: sequence !== null && applied !== null ? Math.max(0, sequence - applied) : null,
          link: reading?.info?.master_link_status ?? null,
          lastIo: metric(reading?.info, "master_last_io_seconds_ago"),
          syncing: reading?.info?.master_sync_in_progress === "1",
          ackLag: line && Number.isFinite(line.lag) ? line.lag : null,
        };
      }),
    };
  });
}

// Shards grouped by the process (or cluster group) that serves them. A node's group comes from
// its `cluster-group` setting; without one the shards of one process share its run_id.
export function groupsOf(shards, readings) {
  const byKey = new Map(readings.map((reading) => [nodeKey(reading.node), reading]));
  const groups = [];
  for (const shard of shards) {
    const reading = byKey.get(nodeKey(shard));
    const name = reading?.group ?? null;
    const runId = reading?.info?.run_id ?? null;
    const key = name ? `group:${name}` : runId ? `run:${runId}` : `node:${nodeKey(shard)}`;
    let group = groups.find((item) => item.key === key);
    if (!group) {
      group = { key, name, label: name ?? `${shard.host}:${shard.port}`, shards: [] };
      groups.push(group);
    }
    group.shards.push(shard);
  }
  return groups.map((group, index) => ({
    ...group,
    index,
    slots: group.shards.reduce((total, shard) => total + (shard.slots ?? 0), 0),
  }));
}

// A cluster is more than one server process, a replica, or coordinators in front. One process
// with several shards is not: its shards and slots already show on Overview.
export function isCluster(shards, readings, coordinatorsConfigured) {
  if (coordinatorsConfigured) return true;
  if (shards.some((shard) => shard.replicas?.length)) return true;
  const processes = new Set();
  for (const reading of readings) {
    if (reading.group) return true;
    if (reading.info?.run_id) processes.add(reading.info.run_id);
  }
  return processes.size > 1;
}

export function slotsCovered(shards) {
  return shards.reduce((total, shard) => total + (shard.slots ?? 0), 0);
}

// Things an operator should look at, most serious first. Each has a level (danger or warn).
export function notices(shards, readings) {
  const list = [];
  const label = (reading) => `${nodeKey(reading.node)} (shard ${reading.node.shard} ${reading.node.role})`;
  for (const reading of readings) {
    const info = reading.info;
    if (!reading.reachable) list.push({ level: "danger", text: `${label(reading)} is not answering.` });
    if (info?.ruvio_persistence_state === "error") list.push({ level: "danger", text: `${label(reading)} reports a persistence error.` });
    if (info?.replication_fenced === "1") list.push({ level: "danger", text: `${label(reading)} was replaced by a newer primary and refuses writes.` });
    if (info?.ruvio_readiness && info.ruvio_readiness !== "ready") list.push({ level: "warn", text: `${label(reading)} is ${info.ruvio_readiness}.` });
    if (reading.node.role === "replica" && info?.master_link_status === "down") list.push({ level: "warn", text: `${label(reading)} has lost its link to the primary.` });
    if (info?.master_sync_in_progress === "1") list.push({ level: "warn", text: `${label(reading)} is taking a full copy of its primary.` });
  }
  const versions = [...new Set(readings.filter((reading) => reading.reachable && reading.topologyVersion !== null).map((reading) => reading.topologyVersion))];
  if (versions.length > 1) list.push({ level: "warn", text: `Nodes serve different cluster map versions (${versions.sort((a, b) => a - b).join(", ")}). A map is still spreading, or a node did not take it.` });
  const covered = slotsCovered(shards);
  if (shards.length && covered !== SLOT_COUNT) list.push({ level: "warn", text: `The shards cover ${covered.toLocaleString()} of ${SLOT_COUNT.toLocaleString()} slots.` });
  return list.sort((a, b) => (a.level === b.level ? 0 : a.level === "danger" ? -1 : 1));
}

// A flat RESP field list (`[name, value, name, value…]`) as an object.
export function fields(value) {
  const items = value?.kind === "array" ? value.items : [];
  const out = {};
  for (let index = 0; index + 1 < items.length; index += 2) {
    out[items[index]?.text ?? ""] = items[index + 1]?.text ?? (items[index + 1]?.value != null ? String(items[index + 1].value) : "");
  }
  return out;
}

// `group_<name>:primary=…,epoch=…,members_answering=n/m,state=…` lines of a coordinator's INFO.
export function coordinatorGroups(info) {
  return Object.entries(info ?? {})
    .filter(([name]) => name.startsWith("group_"))
    .map(([name, value]) => {
      const parts = Object.fromEntries(value.split(",").map((part) => {
        const equals = part.indexOf("=");
        return equals < 0 ? [part, ""] : [part.slice(0, equals), part.slice(equals + 1)];
      }));
      const [answering, members] = String(parts.members_answering ?? "").split("/").map(Number);
      return {
        name: name.slice("group_".length),
        primary: parts.primary || null,
        epoch: Number(parts.epoch ?? 0),
        answering: Number.isFinite(answering) ? answering : null,
        members: Number.isFinite(members) ? members : null,
        state: parts.state ?? null,
        stateMs: parts.state_ms != null ? Number(parts.state_ms) : null,
      };
    });
}

// The /api/coordinators reply as the Cluster screen shows it.
export function coordinatorModel(report) {
  if (!report?.configured) return { configured: false };
  const coordinators = (report.coordinators ?? []).map((entry) => {
    const info = entry.ok ? parseInfoText(entry.info) : null;
    return {
      address: entry.address,
      ok: entry.ok,
      error: entry.error ?? "",
      id: info?.coordinator_id ?? null,
      leaderId: info?.leader_id ?? null,
      leader: info?.is_leader === "1",
      failovers: metric(info, "failovers_total"),
      lastFailover: metric(info, "last_failover_unix_ms"),
      info,
    };
  });
  const source = coordinators.find((entry) => entry.address === report.source) ?? null;
  const masters = (report.masters?.kind === "array" ? report.masters.items : []).map(fields);
  const groups = coordinatorGroups(source?.info).map((group) => ({
    ...group,
    master: masters.find((item) => item.name === group.name) ?? null,
    replicas: (report.replicas?.[group.name]?.kind === "array" ? report.replicas[group.name].items : []).map(fields),
  }));
  return {
    configured: true,
    coordinators,
    source,
    leader: coordinators.find((entry) => entry.leader) ?? null,
    groups,
    status: report.status?.kind === "array" ? fields(report.status) : null,
    statusError: report.status?.kind === "error" ? report.status.text : "",
    reachable: coordinators.filter((entry) => entry.ok).length,
  };
}

// Failovers this panel saw: a group whose agreed primary or epoch changed between two reports.
export function failoverEvents(previous, next, at = Date.now()) {
  if (!previous?.configured || !next?.configured) return [];
  return next.groups.flatMap((group) => {
    const old = previous.groups.find((item) => item.name === group.name);
    if (!old || (old.primary === group.primary && old.epoch === group.epoch)) return [];
    return [{ at, group: group.name, from: old.primary, to: group.primary, epoch: group.epoch }];
  });
}

export function duration(ms) {
  if (ms == null || !Number.isFinite(ms)) return "Not reported";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = Math.round(ms / 1000);
  if (seconds < 120) return `${seconds} s`;
  const minutes = Math.round(seconds / 60);
  return minutes < 120 ? `${minutes} min` : `${Math.round(minutes / 60)} h`;
}


// The charts the Dashboard offers. Every one is drawn from the INFO reply each refresh already
// reads, so choosing more charts sends nothing more to the server.
// kind: "rate" plots the change per second of a counter, "gauge" plots the value itself.
// scope: "primaries" adds up primaries only (replicas hold copies), "all" every node.
// process: the counter is process-wide (every shard of a process reports the same total), so
// it is taken once per process.
export const CHARTS = [
  { id: "commands", group: "Throughput", title: "Commands per second", kind: "rate", unit: "count", scope: "all", series: [["total_commands_processed", "Commands"]], hint: "Includes this panel's own reads", default: true },
  { id: "command-mix", group: "Throughput", title: "Command mix per second", kind: "rate", unit: "count", scope: "all", stacked: true, series: [["get_commands", "GET"], ["set_commands", "SET"], ["increment_commands", "INCR/DECR"], ["other_commands", "Other"]], default: true },
  { id: "network", group: "Network", title: "Network bytes per second", kind: "rate", unit: "bytes", scope: "all", series: [["total_net_input_bytes", "In"], ["total_net_output_bytes", "Out"]], default: true },
  { id: "connections", group: "Network", title: "Connections accepted per second", kind: "rate", unit: "count", scope: "all", process: true, series: [["total_connections_received", "Accepted"], ["rejected_connections", "Rejected"]] },
  { id: "clients", group: "Network", title: "Connected clients", kind: "gauge", unit: "count", scope: "all", series: [["connected_clients", "Clients"]], default: true },
  { id: "memory", group: "Memory", title: "Charged memory", kind: "gauge", unit: "bytes", scope: "primaries", series: [["charged_memory_bytes", "Charged"]], hint: "Store memory, not process RSS", default: true },
  { id: "memory-breakdown", group: "Memory", title: "Memory breakdown", kind: "gauge", unit: "bytes", scope: "primaries", stacked: true, series: [["key_memory_bytes", "Keys"], ["value_memory_bytes", "Values"], ["collection_memory_bytes", "Collections"], ["index_memory_bytes", "Index"], ["pooled_value_bytes", "Reusable buffers"], ["persistence_memory_bytes", "Persistence buffers"], ["lazyfree_pending_bytes", "Waiting to be freed"]], default: true },
  { id: "keys", group: "Keyspace", title: "Keys", kind: "gauge", unit: "count", scope: "primaries", series: [["keys", "Keys"], ["expires", "With expiry"]], default: true },
  { id: "expiry", group: "Keyspace", title: "Expired and evicted per second", kind: "rate", unit: "count", scope: "primaries", series: [["expired_keys", "Expired"], ["evicted_keys", "Evicted"]] },
  { id: "wal", group: "Persistence", title: "WAL records per second", kind: "rate", unit: "count", scope: "primaries", series: [["wal_last_sequence", "Records"]] },
  { id: "unsynced", group: "Persistence", title: "Bytes not yet on disk", kind: "gauge", unit: "bytes", scope: "all", series: [["wal_unsynced_bytes", "Unsynced WAL"], ["snapshot_journal_bytes", "Snapshot journal"]] },
  { id: "buffers", group: "Buffers", title: "Largest buffers seen", kind: "gauge", unit: "bytes", scope: "all", series: [["input_buffer_peak_bytes", "Input"], ["output_buffer_peak_bytes", "Output"], ["pubsub_queue_peak_bytes", "Pub/Sub queue"]], hint: "Peaks since start" },
  { id: "replication", group: "Replication", title: "Replication send lag", kind: "gauge", unit: "count", scope: "primaries", series: [["replication_send_lag", "Records not yet sent"]], hint: "Records the primaries still have to send" },
];
export const DEFAULT_CHARTS = CHARTS.filter((chart) => chart.default).map((chart) => chart.id);
const FIELDS = [...new Set(CHARTS.flatMap((chart) => chart.series.map(([field]) => field)))];

// What the charts need from one refresh, per node.
export function snapshot(readings, at = Date.now()) {
  return {
    at,
    nodes: Object.fromEntries(readings.map((reading) => [nodeKey(reading.node), {
      shard: reading.node.shard,
      primary: reading.node.role === "primary",
      run: reading.info?.run_id ?? nodeKey(reading.node),
      values: Object.fromEntries(FIELDS.map((field) => [field, metric(reading.info, field)])),
    }])),
  };
}

// Keeps the readings of the last `keepMs` (and at most `limit` of them).
export function keepRecent(history, point, keepMs = 30 * 60 * 1000, limit = 2000) {
  const next = [...history, point].filter((item) => item.at >= point.at - keepMs);
  return next.length > limit ? next.slice(next.length - limit) : next;
}

// The nodes one chart adds up: replicas only for "all" charts, one shard per process for
// process-wide counters, and only `keys` when the scope is narrowed.
function chartNodes(chart, point, keys) {
  const seen = new Set();
  return Object.entries(point.nodes).filter(([key, node]) => {
    if (keys.length && !keys.includes(key)) return false;
    if (chart.scope === "primaries" && !node.primary) return false;
    if (chart.process) {
      if (seen.has(node.run)) return false;
      seen.add(node.run);
    }
    return true;
  });
}

// One value per point for `field` over `entries` of a point: the sum, or null when a node did
// not report it. For a rate, the change per second since the previous point (a counter that
// went down, after a restart, leaves that node out).
function valueAt(chart, field, history, index, keys, only = null) {
  const point = history[index];
  const entries = chartNodes(chart, point, keys).filter(([key]) => !only || key === only);
  if (!entries.length) return null;
  if (chart.kind === "gauge") {
    const values = entries.map(([, node]) => node.values[field]);
    return values.every((value) => value !== null && value !== undefined) ? values.reduce((a, b) => a + b, 0) : null;
  }
  const before = history[index - 1];
  const seconds = before ? (point.at - before.at) / 1000 : 0;
  if (seconds <= 0) return null;
  let total = 0;
  let compared = 0;
  for (const [key, node] of entries) {
    const now = node.values[field];
    const then = before.nodes[key]?.values[field];
    if (now == null || then == null || now < then) continue;
    total += (now - then) / seconds;
    compared += 1;
  }
  return compared ? total : null;
}

// The lines one chart draws over the last `windowMs`. "total" adds the chosen nodes up per
// series; "nodes" draws the chart's first series once per node (at most 7 nodes, the rest
// summed as "Other", so colors never repeat).
export function chartData(chart, history, { keys = [], view = "total", windowMs = 15 * 60 * 1000 } = {}) {
  if (!history.length) return { series: [], stacked: false };
  const end = history[history.length - 1].at;
  const indexes = history.map((point, index) => index).filter((index) => history[index].at >= end - windowMs && (chart.kind === "gauge" || index > 0));
  const line = (label, value) => ({ label, points: indexes.map((index) => ({ at: history[index].at, value: value(index) })) });
  if (view === "nodes") {
    const [field, name] = chart.series[0];
    const last = history[history.length - 1];
    const nodes = chartNodes(chart, last, keys).map(([key, node]) => ({ key, node, latest: valueAt(chart, field, history, history.length - 1, keys, key) ?? 0 }));
    nodes.sort((a, b) => b.latest - a.latest);
    const shown = nodes.length > 8 ? nodes.slice(0, 7) : nodes;
    const rest = nodes.length > 8 ? nodes.slice(7).map((item) => item.key) : [];
    const series = shown.map(({ key, node }) => line(`${node.primary ? `Shard ${node.shard}` : `Replica of ${node.shard}`} \u00b7 ${key}`, (index) => valueAt(chart, field, history, index, keys, key)));
    if (rest.length) series.push(line(`Other ${rest.length} nodes`, (index) => valueAt(chart, field, history, index, rest)));
    return { series, stacked: false, field: name };
  }
  return { series: chart.series.map(([field, label]) => line(label, (index) => valueAt(chart, field, history, index, keys))), stacked: !!chart.stacked };
}

export function formatValue(unit, value, perSecond = false) {
  if (value == null || !Number.isFinite(value)) return "Not reported";
  const suffix = perSecond ? "/s" : "";
  if (unit === "bytes") {
    if (Math.abs(value) < 1024) return `${Math.round(value)} B${suffix}`;
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let size = value / 1024;
    let step = 0;
    while (Math.abs(size) >= 1024 && step < units.length - 1) {
      size /= 1024;
      step += 1;
    }
    return `${size.toFixed(size >= 100 ? 0 : 1)} ${units[step]}${suffix}`;
  }
  if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M${suffix}`;
  if (Math.abs(value) >= 1e4) return `${(value / 1e3).toFixed(1)}k${suffix}`;
  return `${value >= 100 || Number.isInteger(value) ? Math.round(value).toLocaleString() : value.toFixed(1)}${suffix}`;
}

// `0–4095, 8192–9000 +2 more`: a shard's ranges in a short line.
export function rangeText(shard, limit = 2) {
  const ranges = shard?.ranges ?? [];
  if (!ranges.length) return "No slots";
  const shown = ranges.slice(0, limit).map(([start, end]) => (start === end ? `${start}` : `${start}–${end}`)).join(", ");
  return ranges.length > limit ? `${shown} +${ranges.length - limit} more` : shown;
}
