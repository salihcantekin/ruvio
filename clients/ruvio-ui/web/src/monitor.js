// Live cluster readings: which nodes to ask, and what their INFO replies add up to.
// Pure functions, so the Node checks can cover them without a browser.
import { metric } from "./operator.js";

export const SLOT_COUNT = 16384;
export const HISTORY_LIMIT = 180;

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

export function pushHistory(history, point, limit = HISTORY_LIMIT) {
  const next = [...history, point];
  return next.length > limit ? next.slice(next.length - limit) : next;
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

export function rate(value) {
  if (value == null) return "Not reported";
  return value >= 100 ? Math.round(value).toLocaleString() : value.toFixed(1);
}

// What the trend charts need from one refresh, per node.
export function snapshot(readings, at = Date.now()) {
  return {
    at,
    nodes: Object.fromEntries(readings.map((reading) => [nodeKey(reading.node), {
      primary: reading.node.role === "primary",
      commands: metric(reading.info, "total_commands_processed"),
      memory: metric(reading.info, "charged_memory_bytes"),
      clients: metric(reading.info, "connected_clients"),
    }])),
  };
}

// Commands per second, charged memory (primaries) and connected clients over time, for the
// nodes in `keys` (every node when empty). A point is null when a node did not report it.
export function trendSeries(history, keys = []) {
  const wanted = (key) => !keys.length || keys.includes(key);
  const total = (point, field, onlyPrimaries) => {
    const values = Object.entries(point.nodes)
      .filter(([key, node]) => wanted(key) && (!onlyPrimaries || node.primary))
      .map(([, node]) => node[field]);
    return values.length && values.every((value) => value !== null) ? values.reduce((a, b) => a + b, 0) : null;
  };
  const ops = [];
  for (let index = 1; index < history.length; index += 1) {
    const before = history[index - 1];
    const after = history[index];
    const seconds = (after.at - before.at) / 1000;
    let sumRate = 0;
    let compared = 0;
    for (const [key, node] of Object.entries(after.nodes)) {
      const old = before.nodes[key];
      if (!wanted(key) || !old || node.commands === null || old.commands === null || node.commands < old.commands || seconds <= 0) continue;
      sumRate += (node.commands - old.commands) / seconds;
      compared += 1;
    }
    ops.push({ at: after.at, value: compared ? sumRate : null });
  }
  return {
    ops,
    memory: history.map((point) => ({ at: point.at, value: total(point, "memory", true) })),
    clients: history.map((point) => ({ at: point.at, value: total(point, "clients", false) })),
  };
}

// `0–4095, 8192–9000 +2 more`: a shard's ranges in a short line.
export function rangeText(shard, limit = 2) {
  const ranges = shard?.ranges ?? [];
  if (!ranges.length) return "No slots";
  const shown = ranges.slice(0, limit).map(([start, end]) => (start === end ? `${start}` : `${start}–${end}`)).join(", ");
  return ranges.length > limit ? `${shown} +${ranges.length - limit} more` : shown;
}
