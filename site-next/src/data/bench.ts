// Build-time access to the repository's own measurement records and command list.
// Charts never carry numbers of their own: everything is read from these files.
import fs from "node:fs";
import path from "node:path";

const repo = path.resolve(process.cwd(), "..");
const readJson = (rel: string) => JSON.parse(fs.readFileSync(path.join(repo, rel), "utf8"));

// Records name the host as "Azure F16as_v6, AMD ..."; say plainly that the AMD CPU is an Azure VM.
const azureVm = (m: string) => m.replace(/^Azure (?!VM)/, "Azure VM ");

export type Row = { label: string; ruvio: number; redis: number; ratio: number; ratioLo: number; ratioHi: number; rounds: number };
export type MemRow = { label: string; ruvioKib: number; redisKib: number; saved: number };
export type Platform = {
  id: "mac" | "azure";
  name: string;
  short: string;
  file: string;
  date: string;
  machine: string;
  durability: string;
  shards: number;
  rounds: number;
  pinning: string;
  commit: string;
  clients: number;
  pipeline: number;
  redisVersion: string;
  rows: Row[];
  memory: MemRow[];
  geomean: number;
  atPar: number;
  faster: number;
  memSaved: number;
  memLower: number;
};

const cleanLabel = (s: string) => s.replace(/\s*\(baseline, for scale\)/, "");

function load(id: Platform["id"], file: string, name: string, short: string, part?: string): Platform {
  const d = readJson(file);
  // The Azure record keeps the command grid under `features`, next to its own host and method fields.
  if (part) {
    const runs = d[part].rows[0]?.samples?.ruvio?.length ?? 1;
    Object.assign(d, d[part], {
      redis_version: `v=${String(d.build?.redis ?? "").match(/\d+\.\d+\.\d+/)?.[0] ?? ""}`,
      config: { rounds: runs, clients: 50, pipeline: 16 },
      environment: { machine: d.host?.description ?? "", durability: "memory", shards: 1, commit: d.build?.ruvio_commit ?? "", pinning: "server on cpu 0, load generator on cpus 2 and 4" },
    });
  }
  // ratioLo/ratioHi: the slowest Ruvio round against the fastest Redis round, and the reverse.
  const rows: Row[] = d.rows.map((r: any) => {
    const sr: number[] = r.samples?.ruvio ?? [r.ruvio], sx: number[] = r.samples?.redis ?? [r.redis];
    return {
      label: cleanLabel(r.label), ruvio: r.ruvio, redis: r.redis, ratio: r.ruvio / r.redis,
      ratioLo: Math.min(...sr) / Math.max(...sx), ratioHi: Math.max(...sr) / Math.min(...sx), rounds: sr.length,
    };
  });
  const memory: MemRow[] = d.memory.map((m: any) => ({
    label: m.label,
    ruvioKib: m.ruvio.delta_kib,
    redisKib: m.redis.delta_kib,
    saved: 1 - m.ruvio.delta_kib / m.redis.delta_kib,
  }));
  const geomean = Math.exp(rows.reduce((a, r) => a + Math.log(r.ratio), 0) / rows.length);
  const memR = memory.reduce((a, m) => a + m.ruvioKib, 0);
  const memX = memory.reduce((a, m) => a + m.redisKib, 0);
  const redisVersion = (String(d.redis_version).match(/v=([\d.]+)/) || [])[1] ?? "";
  return {
    id, name, short, file, date: d.date,
    // The site names the Mac by its architecture, not its model.
    machine: String(d.environment?.machine ?? "").replace(/Apple M\d+( Pro| Max)?/, "Apple silicon"),
    durability: d.environment?.durability ?? "", shards: d.environment?.shards ?? 1, rounds: d.config.rounds,
    pinning: d.environment?.pinning ?? "", commit: d.build?.commit ?? d.environment?.commit ?? "",
    clients: d.config.clients, pipeline: d.config.pipeline, redisVersion,
    rows, memory, geomean,
    atPar: rows.filter((r) => r.ratio >= 0.97).length,
    faster: rows.filter((r) => r.ratio >= 1).length,
    memSaved: 1 - memR / memX,
    memLower: memory.filter((m) => m.ruvioKib < m.redisKib).length,
  };
}

export const platforms: Platform[] = [
  load("mac", "benchmark/records/mac-2026-10-28.json", "Mac (ARM)", "Mac"),
  load("azure", "benchmark/records/x86-azure-2026-10-11.json", "Azure VM (x86)", "Azure", "features"),
];
export const primary = platforms[0];

// Throughput and p99 under load on one Azure VM: client counts, pipelining, fsync and shard counts.
const azure = readJson("benchmark/records/x86-azure-2026-10-11.json");
export type LoadRow = { label: string; shards: number; ruvio: { rps: number; p99: number }; redis: { rps: number; p99: number } };
export const loadRun = {
  file: "benchmark/records/x86-azure-2026-10-11.json",
  date: azure.date as string,
  machine: `Azure VM, ${azure.host.cpu}, ${azure.host.vcpus} vCPU`,
  commit: azure.build.ruvio_commit as string,
  rows: azure.concurrency.map((c: any) => ({
    label: c.label, shards: c.ruvio_shards,
    ruvio: { rps: c.ruvio.rps, p99: c.ruvio.p99_ms }, redis: { rps: c.redis.rps, p99: c.redis.p99_ms },
  })) as LoadRow[],
};

// Command list checked by CI.
const matrix = readJson("docs/compatibility-matrix.json");
export type Command = { name: string; family: string; limitations: string };
export const commands: Command[] = matrix.commands.map((c: any) => ({ name: c.name, family: c.family, limitations: c.limitations ?? "" }));

const opsFamilies = new Set(["connection", "scripts", "acl", "cluster", "pubsub", "transactions", "introspection", "replication"]);
const keyFamilies = new Set(["keys", "keyspace"]);
export type FamilyKind = "data" | "keys" | "ops";
export const families: { family: string; count: number; kind: FamilyKind }[] = Object.entries(
  commands.reduce<Record<string, number>>((acc, c) => ((acc[c.family] = (acc[c.family] ?? 0) + 1), acc), {}),
)
  .map(([family, count]) => ({ family, count, kind: (keyFamilies.has(family) ? "keys" : opsFamilies.has(family) ? "ops" : "data") as FamilyKind }))
  .sort((a, b) => b.count - a.count);


// Formatting helpers shared by charts and tables.
export const fmtOps = (v: number) => (v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : `${Math.round(v / 1e3)}K`);
export const fmtX = (v: number) => `${v.toFixed(2)}×`;
export const fmtMiB = (kib: number) => (kib / 1024).toFixed(1);
export const pct = (v: number) => `${Math.round(v * 100)}%`;

// Durability defaults, read from the example configuration that ships with the server.
// The server release shown on the site: the version in the root Cargo.toml.
const cargoVersion = /^version\s*=\s*"([^"]+)"/m.exec(fs.readFileSync(path.join(repo, "Cargo.toml"), "utf8"));
if (!cargoVersion) throw new Error("Cargo.toml has no version");
export const serverVersion = cargoVersion[1];

const exampleToml = fs.readFileSync(path.join(repo, "ruvio.example.toml"), "utf8");
const tomlNumber = (key: string) => {
  const m = exampleToml.match(new RegExp(`^\\s*${key}\\s*=\\s*(\\d+)`, "m"));
  if (!m) throw new Error(`ruvio.example.toml has no ${key}`);
  return Number(m[1]);
};
export const durabilityDefaults = {
  syncIntervalMs: tomlNumber("sync_interval_ms"),
  snapshotIntervalMs: tomlNumber("snapshot_interval_ms"),
};

// Whole machine: Ruvio with N shards in one process against one Redis process on one core and a
// Redis Cluster of N nodes on the same N cores (scripts/shard_compare.py). Rates are the sum over
// the shards; "perCore" divides Ruvio's total by N and compares it with the single Redis.
export type ScaleRow = { label: string; ruvio: number; redis1: number; cluster: number; vsCluster: number; perCore: number };
export type ScaleMem = { label: string; ruvioKib: number; redis1Kib: number; clusterKib: number; perShardVsOne: number; vsCluster: number };
export type Scale = {
  file: string; date: string; shards: number; pipeline: number; clients: number; rounds: number; durability: string;
  machine: string; commit: string; redisVersion: string;
  rows: ScaleRow[]; memory: ScaleMem[];
  geomeanCluster: number; geomeanPerCore: number; atParCluster: number; fasterThanCluster: number;
  memGeomeanPerShard: number; memGeomeanCluster: number;
  row: (name: string) => ScaleRow;
};

function loadScale(file: string): Scale {
  const d = readJson(file);
  const n: number = d.config.shards;
  const rows: ScaleRow[] = d.rows
    .filter((r: any) => r.ruvio && r.redis1 && r.rediscl)
    .map((r: any) => ({
      label: cleanLabel(r.label), ruvio: r.ruvio, redis1: r.redis1, cluster: r.rediscl,
      vsCluster: r.ruvio / r.rediscl, perCore: r.ruvio / n / r.redis1,
    }));
  const memory: ScaleMem[] = (d.memory ?? []).map((m: any) => ({
    label: m.label, ruvioKib: m.ruvio.delta_kib, redis1Kib: m.redis1.delta_kib, clusterKib: m.rediscl.delta_kib,
    perShardVsOne: m.ruvio.delta_kib / (m.ruvio.copies ?? n) / m.redis1.delta_kib,
    vsCluster: m.ruvio.delta_kib / m.rediscl.delta_kib,
  }));
  const gm = (xs: number[]) => (xs.length ? Math.exp(xs.reduce((a, x) => a + Math.log(x), 0) / xs.length) : 1);
  const steady = rows.filter((r) => !/PUBLISH/.test(r.label));
  const redisVersion = (String(d.redis_version).match(/v=([\d.]+)/) || [])[1] ?? "";
  return {
    file, date: d.date, shards: n, pipeline: d.config.pipeline, clients: d.config.clients, rounds: d.config.rounds,
    durability: d.config.durability, machine: azureVm(d.host?.description ?? d.host?.cpu ?? ""), commit: d.build?.ruvio_commit ?? "", redisVersion,
    rows, memory,
    geomeanCluster: gm(steady.map((r) => r.vsCluster)), geomeanPerCore: gm(steady.map((r) => r.perCore)),
    atParCluster: rows.filter((r) => r.vsCluster >= 0.97).length, fasterThanCluster: rows.filter((r) => r.vsCluster >= 1).length,
    memGeomeanPerShard: gm(memory.map((m) => m.perShardVsOne)), memGeomeanCluster: gm(memory.map((m) => m.vsCluster)),
    row: (name: string) => {
      const found = rows.find((r) => r.label === name);
      if (!found) throw new Error(`${file} has no row ${name}`);
      return found;
    },
  };
}

export const scale = loadScale("benchmark/records/x86-azure-2026-10-16-8-shards-final.json");
export const scaleDeep = loadScale("benchmark/records/x86-azure-2026-10-16-8-shards-p64-after.json");
export const scaleEverysec = loadScale("benchmark/records/x86-azure-2026-10-16-8-shards-everysec-after.json");

// Shard scaling with a cluster-aware client on both sides, and the one-port path before and after the
// forwarding wake fix (benchmark/azure/shard-bench.sh). Medians over the rounds.
type ShardRow = { shards: number; system: string; client: string; set: number; get: number };
const shardFile = "benchmark/records/x86-azure-2026-10-26-shard-scaling.json";
const shardRaw = readJson(shardFile);
const shardRow = (shards: number, system: string, client: string): ShardRow => {
  const found = (shardRaw.rows as ShardRow[]).find((r) => r.shards === shards && r.system === system && r.client === client);
  if (!found) throw new Error(`${shardFile} has no row ${shards} ${system} ${client}`);
  return found;
};
export const shardScaling = {
  file: shardFile,
  date: shardRaw.date as string,
  machine: azureVm(shardRaw.host.description as string),
  commit: shardRaw.build.ruvio_commit as string,
  rounds: shardRaw.config.rounds as number,
  pipeline: shardRaw.config.pipeline as number,
  redisVersion: ((String(shardRaw.redis_version).match(/v=([\d.]+)/) || [])[1] ?? "") as string,
  // 8 shards are left out: eight load threads were the limit for both systems there.
  routed: [1, 4].map((n) => ({ shards: n, ruvio: shardRow(n, "ruvio", "cluster-aware"), redis: shardRow(n, "redis", "cluster-aware") })),
  onePort: [4, 8].map((n) => ({ shards: n, before: shardRow(n, "ruvio_before", "one-port"), after: shardRow(n, "ruvio", "one-port") })),
  oneShard: (shardRaw.one_shard.rows as { label: string; redis: number; ruvio: number }[]).map((r) => ({ ...r, ratio: r.ruvio / r.redis })),
};

// One Ruvio command against the Lua script that does the same on Redis (scripts/bench_scripted_equivalents.py).
export type ScriptedRow = { label: string; ruvio: number; redis: number; ratio: number; ruvioP50: number; redisP50: number; ruvioP99: number; redisP99: number };
export type ScriptedMem = { label: string; ruvioBytes: number; redisBytes: number; ratio: number };
const scriptedFile = "benchmark/records/mac-scripted-2026-10-19.json";
const scriptedRaw = readJson(scriptedFile);
export const scripted = {
  file: scriptedFile,
  date: scriptedRaw.date as string,
  machine: `${String(scriptedRaw.host.cpu).replace(/Apple M\d+( Pro| Max)?/, "Apple silicon")}, ${scriptedRaw.host.cores} cores`,
  commit: scriptedRaw.build.commit as string,
  clients: scriptedRaw.config.clients as number,
  pipeline: scriptedRaw.config.pipeline as number,
  rounds: scriptedRaw.config.rounds as number,
  redisVersion: ((String(scriptedRaw.redis_version).match(/v=([\d.]+)/) || [])[1] ?? "") as string,
  rows: scriptedRaw.rows
    .filter((r: any) => !r.skipped && !/for scale/.test(r.label))
    .map((r: any) => ({
      label: r.label.replace(/\s*\(\d+ due\)/, ""), ruvio: r.ruvio, redis: r.redis, ratio: r.ruvio / r.redis,
      ruvioP50: r.ruvio_p50, redisP50: r.redis_p50, ruvioP99: r.ruvio_p99, redisP99: r.redis_p99,
    })) as ScriptedRow[],
  memory: scriptedRaw.memory.map((m: any) => ({
    label: m.label,
    ruvioBytes: (m.ruvio.delta_kib * 1024) / m.keys, redisBytes: (m.redis.delta_kib * 1024) / m.keys,
    ratio: m.ruvio.delta_kib / m.redis.delta_kib,
  })) as ScriptedMem[],
};
