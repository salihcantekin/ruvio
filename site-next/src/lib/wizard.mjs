// The setup wizard's engine: answers in, a deployment plan and ready-to-paste files out.
// Plain JavaScript with no imports so the page script, the build and `node --test` all load it.
// Every environment variable and file key it writes is checked against the server's schema
// (src/data/config-schema.json, printed by `ruvio --print-config-schema`) by checkSchema().

export const IMAGE = "salihcantekin/ruvio";
export const UI_IMAGE = "salihcantekin/ruvio-ui:latest";
export const SLOTS = 16384;

/** Variables the published image sets itself (Dockerfile); a check of a plan starts from them. */
export const IMAGE_ENV = {
  RUVIO_ADDR: "0.0.0.0:6379",
  RUVIO_DATA_DIR: "/data",
  RUVIO_PROTECTED_MODE: "false",
};

/** Coordinator file keys the wizard writes (docs/COORDINATOR.md). */
export const COORDINATOR_KEYS = ["id", "listen", "data_dir", "peers", "secret", "auth", "ping_interval_ms", "down_after_ms"];

export const TOPOLOGIES = ["single", "replicas", "failover", "cluster"];
export const DURABILITY = ["memory", "snapshot", "everysec", "always"];
export const FORMATS = ["compose", "bash", "powershell", "docker-run"];

/** @typedef {{
 *   topology: "single"|"replicas"|"failover"|"cluster",
 *   shards: number, replicas: number, groups: number, coordinators: number,
 *   durability: "memory"|"snapshot"|"everysec"|"always",
 *   usage: "store"|"cache", maxmemoryMb: number,
 *   placement: "one"|"many", access: "local"|"network", host: string, hosts: string[],
 *   password: boolean, safeWrites: boolean, ui: boolean,
 *   format: "compose"|"bash"|"powershell"|"docker-run",
 * }} Answers */

/** @returns {Answers} */
export function defaultAnswers() {
  return {
    topology: "single",
    shards: 1,
    replicas: 2,
    groups: 2,
    coordinators: 3,
    durability: "everysec",
    usage: "store",
    maxmemoryMb: 0,
    placement: "one",
    access: "local",
    host: "",
    hosts: [],
    password: true,
    safeWrites: false,
    ui: false,
    format: "compose",
  };
}

const clamp = (value, low, high, fallback) => {
  const n = Math.trunc(Number(value));
  return Number.isFinite(n) ? Math.min(high, Math.max(low, n)) : fallback;
};

/**
 * Applies the rules between answers and says what it changed: replication needs a write-ahead
 * log, a cache needs a memory limit, `docker run` fits one node only, and so on.
 * @param {Partial<Answers>} input
 * @returns {{ answers: Answers, notes: string[] }}
 */
export function normalize(input) {
  const a = { ...defaultAnswers(), ...input };
  const notes = [];

  if (!TOPOLOGIES.includes(a.topology)) a.topology = "single";
  if (!DURABILITY.includes(a.durability)) a.durability = "everysec";
  if (!FORMATS.includes(a.format)) a.format = "compose";
  a.shards = clamp(a.shards, 1, 8, 1);
  a.replicas = clamp(a.replicas, 1, 2, 2);
  a.groups = clamp(a.groups, 2, 4, 2);
  a.coordinators = a.coordinators === 5 ? 5 : 3;
  a.maxmemoryMb = clamp(a.maxmemoryMb, 0, 1 << 22, 0);
  a.hosts = (Array.isArray(a.hosts) ? a.hosts : []).map((h) => String(h).trim());
  a.host = String(a.host ?? "").trim();

  const replicated = a.topology !== "single";

  if (replicated && (a.durability === "memory" || a.durability === "snapshot")) {
    notes.push("Replicas copy the write-ahead log, so durability is set to everysec.");
    a.durability = "everysec";
  }
  if (a.usage === "cache" && a.maxmemoryMb === 0) {
    a.maxmemoryMb = 1024;
    notes.push("A cache evicts keys only under a memory limit, so the limit is set to 1024 MiB.");
  }
  if (!replicated) a.placement = "one";
  if (a.format === "docker-run" && a.topology !== "single") {
    a.format = "compose";
    notes.push("Several containers need Compose or a script; the output is Docker Compose.");
  }
  if (a.placement === "many") a.access = "network";
  if (!a.password && a.access !== "local") {
    a.password = true;
    notes.push("A server other machines can reach needs a password, so one is generated.");
  }
  if (a.topology === "single") a.safeWrites = false;

  return { answers: a, notes };
}

/** Names of the data nodes, in order: one per member of every group. */
function memberNames(a) {
  if (a.topology === "single") return [["ruvio"]];
  if (a.topology === "cluster") {
    return Array.from({ length: a.groups }, (_, g) => {
      const letter = String.fromCharCode(97 + g);
      return Array.from({ length: 1 + a.replicas }, (_, m) => `ruvio-${letter}${m + 1}`);
    });
  }
  return [Array.from({ length: 1 + a.replicas }, (_, m) => `ruvio-${m + 1}`)];
}

/** The slot range of group `g` of `count`: equal runs, the last one taking the rest. */
export function slotRange(g, count) {
  const size = Math.floor(SLOTS / count);
  const first = g * size;
  const last = g === count - 1 ? SLOTS - 1 : first + size - 1;
  return `${first}-${last}`;
}

/**
 * The deployment: every data node with its ports and environment, every coordinator with its
 * file, the cluster map, and which machine runs what.
 * @param {Answers} a  normalized answers
 * @param {{ version: string, password?: string, replicationPassword?: string, coordinatorSecret?: string }} env
 */
export function plan(a, { version }) {
  const many = a.placement === "many";
  const groups = memberNames(a);
  const nodes = [];
  const machines = [];
  let index = 0;

  const machineOf = (i) => {
    const host = many ? a.hosts[i] || `HOST_${i + 1}` : a.access === "network" ? a.host || "YOUR_SERVER_IP" : "127.0.0.1";
    if (!machines.includes(host)) machines.push(host);
    return host;
  };

  groups.forEach((members, g) => {
    members.forEach((name, m) => {
      const i = index++;
      const single = a.topology === "single";
      const port = many || single ? 6379 : 7000 + i * 10;
      const replicationPort = many ? 26379 : 27000 + i * 10;
      const machine = machineOf(i);
      // Inside Compose a node is reached by its service name; across machines by its host.
      const address = many ? machine : name;
      nodes.push({
        name,
        group: a.topology === "cluster" ? String.fromCharCode(97 + g) : a.topology === "single" ? "" : "main",
        primary: m === 0,
        machine,
        port,
        replicationPort,
        address: `${address}:${port}`,
        replication: `${address}:${replicationPort}`,
        primaryReplication: "",
        env: {},
      });
    });
  });

  for (const node of nodes) {
    const primary = nodes.find((n) => n.group === node.group && n.primary);
    node.primaryReplication = primary.replication;
  }

  const coordinators = [];
  if (a.topology === "failover" || a.topology === "cluster") {
    const count = Math.min(a.coordinators, many ? Math.max(3, nodes.length) : a.coordinators);
    for (let j = 1; j <= count; j++) {
      const machine = many ? nodes[(j - 1) % nodes.length].machine : nodes[0].machine;
      const port = many ? 26400 : 26400 + j;
      coordinators.push({ id: j, name: `coordinator-${j}`, machine, port, address: `${many ? machine : `coordinator-${j}`}:${port}` });
    }
  }

  const topology = a.topology === "cluster" ? clusterMap(a, nodes) : null;

  for (const node of nodes) node.env = nodeEnv(a, node, nodes, coordinators);
  for (const c of coordinators) c.file = coordinatorFile(a, c, coordinators, nodes);

  return {
    answers: a,
    version,
    image: `${IMAGE}:${version}`,
    nodes,
    coordinators,
    topology,
    machines,
    many,
  };
}

/** The `cluster_topology` text, written exactly as `ruvio coordinator --check` prints it. */
function clusterMap(a, nodes) {
  let text = "version = 1\n";
  for (let g = 0; g < a.groups; g++) {
    const name = String.fromCharCode(97 + g);
    const first = nodes.find((n) => n.group === name);
    text += `\n[[groups]]\nname = "${name}"\naddress = "${first.address}"\nshards = ${a.shards}\nslots = ["${slotRange(g, a.groups)}"]\n`;
  }
  return text;
}

function nodeEnv(a, node, nodes, coordinators) {
  const env = {};
  const many = a.placement === "many";

  if (node.port !== 6379) env.RUVIO_ADDR = `0.0.0.0:${node.port}`;
  if (a.shards > 1) env.RUVIO_SHARDS = String(a.shards);
  env.RUVIO_DURABILITY = a.durability;
  if (a.password) env.RUVIO_REQUIREPASS = "${RUVIO_PASSWORD}";
  else env.RUVIO_ALLOW_UNAUTHENTICATED = "true";

  // The host the cluster replies (CLUSTER SLOTS, MOVED) hand to clients.
  const advertise = a.topology === "cluster" ? (many ? node.machine : node.name) : node.machine;
  if (a.shards > 1 || a.topology !== "single") env.RUVIO_ADVERTISE_HOST = advertise;

  if (a.usage === "cache" || a.maxmemoryMb > 0) env.RUVIO_MAXMEMORY = String(a.maxmemoryMb * 1024 * 1024);
  if (a.usage === "cache") env.RUVIO_MAXMEMORY_POLICY = "allkeys-lru";

  if (a.topology !== "single") {
    env.RUVIO_REPLICATION_ROLE = node.primary ? "primary" : "replica";
    env.RUVIO_REPLICATION_LISTEN = `0.0.0.0:${node.replicationPort}`;
    if (!node.primary) env.RUVIO_REPLICATION_PRIMARY = node.primaryReplication;
    env.RUVIO_REPLICATION_PASSWORD = "${RUVIO_REPLICATION_PASSWORD}";
    if (a.safeWrites) {
      env.RUVIO_MIN_REPLICAS_TO_WRITE = "1";
      env.RUVIO_MIN_REPLICAS_MAX_LAG = "10";
    }
  }
  if (a.topology === "cluster") {
    env.RUVIO_CLUSTER_TOPOLOGY = "/etc/ruvio/topology.toml";
    env.RUVIO_CLUSTER_GROUP = node.group;
  }
  void nodes;
  void coordinators;
  return env;
}

function coordinatorFile(a, c, coordinators, nodes) {
  const peers = coordinators.map((p) => `"${p.id}@${p.address}"`).join(", ");
  let text =
    `id = ${c.id}\n` +
    `listen = "0.0.0.0:${c.port}"\n` +
    `data_dir = "/data/coordinator"\n` +
    `peers = [${peers}]\n` +
    `# RUVIO_COORDINATOR_SECRET overrides this value.\n` +
    `secret = ""\n`;
  if (a.password) text += `auth = "\${RUVIO_PASSWORD}"\n`;
  text += `ping_interval_ms = 500\ndown_after_ms = 3000\n`;

  const groupNames = [...new Set(nodes.map((n) => n.group))];
  groupNames.forEach((name, g) => {
    const members = nodes.filter((n) => n.group === name);
    text += `\n[[groups]]\nname = "${name}"\n`;
    if (a.topology === "cluster") text += `shards = ${a.shards}\nslots = ["${slotRange(g, groupNames.length)}"]\n`;
    text += "members = [\n";
    for (const m of members) text += `  { address = "${m.address}", replication = "${m.replication}" },\n`;
    text += "]\n";
  });
  return text;
}

// ---------------------------------------------------------------------------------------------
// Rendering

const yamlQuote = (value) => `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

function portRange(first, count, bind) {
  const spec = count > 1 ? `${first}-${first + count - 1}:${first}-${first + count - 1}` : `${first}:${first}`;
  return bind ? `${bind}:${spec}` : spec;
}

/** Docker Compose for one machine (all of the plan) or for `machine` only. */
function composeFile(p, machine) {
  const a = p.answers;
  const bind = a.access === "local" ? "127.0.0.1" : "";
  const nodes = p.nodes.filter((n) => !machine || n.machine === machine);
  const coordinators = p.coordinators.filter((c) => !machine || c.machine === machine);
  const durable = a.durability !== "memory";
  const lines = [];

  lines.push(`name: ruvio${machine && p.machines.length > 1 ? `-${p.machines.indexOf(machine) + 1}` : ""}`);
  if (p.topology) {
    lines.push("configs:");
    lines.push("  topology:");
    lines.push("    content: |");
    for (const l of p.topology.trimEnd().split("\n")) lines.push(l ? `      ${l}` : "");
    for (const c of coordinators) {
      lines.push(`  ${c.name}:`);
      lines.push("    content: |");
      for (const l of c.file.trimEnd().split("\n")) lines.push(l ? `      ${l}` : "");
    }
  } else if (coordinators.length) {
    lines.push("configs:");
    for (const c of coordinators) {
      lines.push(`  ${c.name}:`);
      lines.push("    content: |");
      for (const l of c.file.trimEnd().split("\n")) lines.push(l ? `      ${l}` : "");
    }
  }
  lines.push("services:");
  for (const n of nodes) {
    lines.push(`  ${n.name}:`);
    lines.push(`    image: ${p.image}`);
    lines.push("    restart: unless-stopped");
    lines.push("    environment:");
    for (const [k, v] of Object.entries(n.env)) lines.push(`      ${k}: ${yamlQuote(v)}`);
    const ports = [portRange(n.port, a.shards, bind)];
    if (p.many && a.topology !== "single") ports.push(portRange(n.replicationPort, a.shards, ""));
    lines.push(`    ports: [${ports.map(yamlQuote).join(", ")}]`);
    const volumes = durable ? [`${n.name}:/data`] : [];
    if (volumes.length) lines.push(`    volumes: [${volumes.map(yamlQuote).join(", ")}]`);
    if (p.topology) lines.push("    configs: [{ source: topology, target: /etc/ruvio/topology.toml }]");
  }
  for (const c of coordinators) {
    lines.push(`  ${c.name}:`);
    lines.push(`    image: ${p.image}`);
    lines.push("    restart: unless-stopped");
    lines.push(`    command: ["coordinator", "--config", "/etc/ruvio/coordinator.toml"]`);
    lines.push("    environment:");
    lines.push(`      RUVIO_COORDINATOR_SECRET: "\${RUVIO_COORDINATOR_SECRET}"`);
    lines.push(`    ports: [${yamlQuote(portRange(c.port, 1, bind))}]`);
    // The image's health check pings the data port, which a coordinator does not open.
    lines.push(`    healthcheck: { test: ["CMD", "ruvio-ctl", "ping", "--address", "127.0.0.1:${c.port}"] }`);
    lines.push(`    volumes: ["${c.name}:/data"]`);
    lines.push(`    configs: [{ source: ${c.name}, target: /etc/ruvio/coordinator.toml }]`);
  }
  if (a.ui && (!machine || machine === p.machines[0])) {
    const target = p.nodes[0];
    lines.push("  ui:");
    lines.push(`    image: ${UI_IMAGE}`);
    lines.push("    restart: unless-stopped");
    lines.push("    environment:");
    lines.push(`      RUVIO_UI_TARGET_HOST: ${yamlQuote(p.many ? target.machine : target.name)}`);
    lines.push(`      RUVIO_UI_TARGET_PORT: "${target.port}"`);
    if (a.password) lines.push(`      RUVIO_UI_TARGET_PASSWORD: "\${RUVIO_PASSWORD}"`);
    lines.push(`    ports: ["127.0.0.1:8080:8080"]`);
  }
  if (!p.many) {
    // redis-cli on the Compose network, where the node names in cluster replies resolve; it
    // starts only with `docker compose run`, so nothing has to be installed on the machine.
    const first = p.nodes[0];
    lines.push("  cli:");
    lines.push("    image: redis:8-alpine");
    lines.push('    profiles: ["tools"]');
    lines.push(`    entrypoint: ["redis-cli"${a.topology === "cluster" ? ', "-c"' : ""}, "-h", "${first.name}", "-p", "${first.port}"${a.password ? ', "--no-auth-warning", "-a", "${RUVIO_PASSWORD}"' : ""}]`);
  }
  const named = [...nodes.filter(() => durable).map((n) => n.name), ...coordinators.map((c) => c.name)];
  if (named.length) {
    lines.push("volumes:");
    for (const v of named) lines.push(`  ${v}:`);
  }
  return lines.join("\n") + "\n";
}

/** The `.env` file Compose reads: the secrets, kept out of the Compose file. */
function envFile(p, secrets) {
  const a = p.answers;
  const lines = ["# Secrets for compose.yml. Keep this file private."];
  if (a.password) lines.push(`RUVIO_PASSWORD=${secrets.password}`);
  if (a.topology !== "single") lines.push(`RUVIO_REPLICATION_PASSWORD=${secrets.replicationPassword}`);
  if (p.coordinators.length) lines.push(`RUVIO_COORDINATOR_SECRET=${secrets.coordinatorSecret}`);
  return lines.length > 1 ? lines.join("\n") + "\n" : "";
}

/** The `docker run` line of a single node. */
function dockerRun(p, secrets, shell) {
  const a = p.answers;
  const n = p.nodes[0];
  const bind = a.access === "local" ? "127.0.0.1:" : "";
  const cont = shell === "powershell" ? " `" : " \\";
  const parts = [`docker run -d --name ruvio --restart unless-stopped`];
  parts.push(`-p ${bind}${a.shards > 1 ? `6379-${6378 + a.shards}:6379-${6378 + a.shards}` : "6379:6379"}`);
  for (const [k, v] of Object.entries(n.env)) {
    const value = v === "${RUVIO_PASSWORD}" ? secrets.password : v;
    parts.push(`-e ${k}=${value}`);
  }
  if (a.durability !== "memory") parts.push("-v ruvio-data:/data");
  parts.push(p.image);
  return parts.join(`${cont}\n  `) + "\n";
}

/** What to run after `up` to see the deployment answer. Nothing needs installing: redis-cli runs in a container. */
export function verifySteps(p, secrets) {
  const a = p.answers;
  const first = p.nodes[0];
  const auth = a.password ? ` --no-auth-warning -a ${secrets.password}` : "";
  const coordinatorAuth = `--no-auth-warning -a ${secrets.coordinatorSecret}`;

  if (a.format === "docker-run") {
    return [{ text: "Check that the server answers:", code: `docker run --rm --network container:ruvio redis:8-alpine redis-cli${auth} PING` }];
  }
  if (p.many) {
    const cli = (host, port, rest) => `docker run --rm redis:8-alpine redis-cli -h ${host} -p ${port} ${rest}`;
    const steps = [
      { text: `On ${first.machine}, start its file first, then the others. From any machine, check the primary and its replicas:`, code: cli(first.machine, first.port, `${auth.trim()} ${a.topology === "cluster" ? "CLUSTER SHARDS" : "ROLE"}`.trim()) },
    ];
    if (p.coordinators.length) {
      const c = p.coordinators[0];
      steps.push({
        text: "Ask the coordinators which member is the primary:",
        code: cli(c.machine, c.port, `${coordinatorAuth} SENTINEL get-master-addr-by-name ${first.group}`),
      });
    }
    return steps;
  }
  if (a.topology === "cluster") {
    return [
      { text: "Write and read a key through the cluster (redis-cli follows MOVED to the owning group):", code: "docker compose run --rm cli SET hello world\ndocker compose run --rm cli GET hello" },
      { text: "Every group, its slots and its replicas:", code: "docker compose run --rm cli CLUSTER SHARDS" },
    ];
  }
  const steps = [{ text: "Check that the server answers:", code: "docker compose run --rm cli PING" }];
  if (a.topology !== "single") {
    steps.push({ text: "The primary lists its replicas once they have copied it:", code: "docker compose run --rm cli ROLE" });
  }
  if (p.coordinators.length) {
    const c = p.coordinators[0];
    steps.push({
      text: "Ask the coordinators for the primary, stop it, and ask again: about 4 s later another member has taken over.",
      code:
        `docker compose run --rm --entrypoint redis-cli cli -h ${c.name} -p ${c.port} ${coordinatorAuth} SENTINEL get-master-addr-by-name ${first.group}\n` +
        `docker compose stop ${first.name}`,
    });
  }
  return steps;
}

/** The addresses an application gives its Ruvio client. */
export function clientSeeds(p) {
  const a = p.answers;
  const local = a.access === "local" && !p.many;
  return p.nodes.map((n) => `${local ? "127.0.0.1" : n.machine}:${n.port}`);
}

/**
 * The output for one format: the files (or the one script) per machine.
 * @returns {{ machine: string, files: { name: string, lang: string, content: string }[] }[]}
 */
export function render(p, secrets) {
  const a = p.answers;
  const machines = p.many ? p.machines : [""];
  return machines.map((machine) => {
    const compose = composeFile(p, machine);
    const env = envFile(p, secrets);
    if (a.format === "docker-run") {
      return { machine, files: [{ name: "Terminal", lang: "bash", content: dockerRun(p, secrets, "bash") }] };
    }
    if (a.format === "bash") {
      let s = "#!/usr/bin/env bash\n# Writes compose.yml and .env into ./ruvio, then starts Ruvio. Needs Docker with Compose 2.23 or later.\nset -euo pipefail\nmkdir -p ruvio && cd ruvio\n";
      if (env) s += `cat > .env <<'EOF'\n${env}EOF\nchmod 600 .env\n`;
      s += `cat > compose.yml <<'EOF'\n${compose}EOF\ndocker compose up -d\ndocker compose ps\n`;
      return { machine, files: [{ name: "setup.sh", lang: "bash", content: s }] };
    }
    if (a.format === "powershell") {
      let s = "# Writes compose.yml and .env into .\\ruvio, then starts Ruvio. Needs Docker Desktop with Compose 2.23 or later.\n$ErrorActionPreference = 'Stop'\nNew-Item -ItemType Directory -Force -Path ruvio | Out-Null\nSet-Location ruvio\n";
      if (env) s += `@'\n${env}'@ | Set-Content -Encoding ascii -NoNewline .env\n`;
      s += `@'\n${compose}'@ | Set-Content -Encoding utf8 -NoNewline compose.yml\ndocker compose up -d\ndocker compose ps\n`;
      return { machine, files: [{ name: "setup.ps1", lang: "powershell", content: s }] };
    }
    const files = [];
    if (env) files.push({ name: ".env", lang: "ini", content: env });
    files.push({ name: "compose.yml", lang: "yaml", content: compose });
    return { machine, files };
  });
}

// ---------------------------------------------------------------------------------------------
// Secrets and the schema check

/** A random password of `length` URL-safe characters, from `crypto.getRandomValues`. */
export function randomSecret(length = 24) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
  const bytes = new Uint8Array(length);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => alphabet[b % alphabet.length]).join("");
}

/** Every variable a plan can set, for checkSchema(). */
export const NODE_ENV_NAMES = [
  "RUVIO_ADDR", "RUVIO_DATA_DIR", "RUVIO_PROTECTED_MODE", "RUVIO_SHARDS", "RUVIO_DURABILITY",
  "RUVIO_REQUIREPASS", "RUVIO_ALLOW_UNAUTHENTICATED", "RUVIO_ADVERTISE_HOST", "RUVIO_MAXMEMORY",
  "RUVIO_MAXMEMORY_POLICY", "RUVIO_REPLICATION_ROLE", "RUVIO_REPLICATION_LISTEN",
  "RUVIO_REPLICATION_PRIMARY", "RUVIO_REPLICATION_PASSWORD", "RUVIO_MIN_REPLICAS_TO_WRITE",
  "RUVIO_MIN_REPLICAS_MAX_LAG", "RUVIO_CLUSTER_TOPOLOGY", "RUVIO_CLUSTER_GROUP",
];

/**
 * Throws when the wizard writes a variable or a value the server's schema does not know.
 * @param {{ settings: { env: string|null, type: string, values: string[] }[] }} schema
 */
export function checkSchema(schema) {
  const byEnv = new Map(schema.settings.filter((s) => s.env).map((s) => [s.env, s]));
  const missing = NODE_ENV_NAMES.filter((name) => !byEnv.has(name));
  if (missing.length) throw new Error(`setup wizard: the server has no ${missing.join(", ")}`);
  const enums = { RUVIO_DURABILITY: DURABILITY, RUVIO_MAXMEMORY_POLICY: ["allkeys-lru"], RUVIO_REPLICATION_ROLE: ["primary", "replica"] };
  for (const [name, used] of Object.entries(enums)) {
    const known = byEnv.get(name).values;
    const unknown = used.filter((v) => !known.includes(v));
    if (unknown.length) throw new Error(`setup wizard: ${name} has no value ${unknown.join(", ")}`);
  }
}
