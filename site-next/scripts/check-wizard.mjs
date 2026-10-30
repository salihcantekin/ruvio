// Runs every setup the wizard can produce through the server's own checks:
// `ruvio --check-config` for each data node and `ruvio coordinator --check` for each
// coordinator, and compares the cluster map with the one the coordinators print.
//
//   node site-next/scripts/check-wizard.mjs target/debug/ruvio
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as wizard from "../src/lib/wizard.mjs";

const binary = path.resolve(process.argv[2] ?? "target/debug/ruvio");
const here = path.dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(fs.readFileSync(path.join(here, "../src/data/config-schema.json"), "utf8"));
const secrets = { password: "check-password", replicationPassword: "check-replication", coordinatorSecret: "check-secret" };
const fill = (text) =>
  text
    .replaceAll("${RUVIO_PASSWORD}", secrets.password)
    .replaceAll("${RUVIO_REPLICATION_PASSWORD}", secrets.replicationPassword)
    .replaceAll("${RUVIO_COORDINATOR_SECRET}", secrets.coordinatorSecret);

wizard.checkSchema(schema);

/** Every combination that changes what a plan writes. */
function* presets() {
  for (const topology of wizard.TOPOLOGIES) {
    for (const shards of [1, 3]) {
      for (const placement of topology === "single" ? ["one"] : ["one", "many"]) {
        for (const usage of ["store", "cache"]) {
          for (const password of [true, false]) {
            const base = { topology, shards, placement, usage, password, safeWrites: !password, durability: password ? "always" : "snapshot" };
            yield base;
            if (topology === "cluster") yield { ...base, groups: 3, replicas: 1, coordinators: 5 };
            if (topology === "single") yield { ...base, access: "network", host: "10.0.0.5", durability: "memory" };
          }
        }
      }
    }
  }
}

const work = fs.mkdtempSync(path.join(os.tmpdir(), "ruvio-wizard-"));
let checked = 0;
const failures = [];

function run(args, env) {
  return spawnSync(binary, args, { env: { ...env, PATH: process.env.PATH ?? "" }, encoding: "utf8", timeout: 20_000 });
}

for (const input of presets()) {
  const { answers } = wizard.normalize({ ...input, hosts: ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6", "10.0.0.7", "10.0.0.8", "10.0.0.9"] });
  const plan = wizard.plan(answers, { version: "0.0.0" });
  const label = JSON.stringify(input);
  const dir = fs.mkdtempSync(path.join(work, "plan-"));
  const topology = path.join(dir, "topology.toml");

  if (plan.topology) fs.writeFileSync(topology, plan.topology);
  // Rendering must not throw for any format.
  for (const format of wizard.FORMATS) wizard.render({ ...plan, answers: { ...answers, format } }, secrets);

  for (const node of plan.nodes) {
    const env = { ...wizard.IMAGE_ENV, RUVIO_CONFIG: path.join(dir, "absent.toml") };
    for (const [key, value] of Object.entries(node.env)) env[key] = fill(value);
    env.RUVIO_DATA_DIR = path.join(dir, node.name);
    if (plan.topology) env.RUVIO_CLUSTER_TOPOLOGY = topology;
    const result = run(["--check-config"], env);
    checked++;
    if (result.status !== 0) failures.push(`${label} ${node.name}: ${result.stderr || result.error}`);
  }

  for (const coordinator of plan.coordinators) {
    const file = path.join(dir, `${coordinator.name}.toml`);
    fs.writeFileSync(file, fill(coordinator.file));
    const result = run(["coordinator", "--config", file, "--check"], { RUVIO_COORDINATOR_SECRET: secrets.coordinatorSecret });
    checked++;
    if (result.status !== 0) {
      failures.push(`${label} ${coordinator.name}: ${result.stderr || result.error}`);
      continue;
    }
    const map = result.stdout.slice(result.stdout.indexOf("\n") + 1);
    if ((plan.topology ?? "") !== map) {
      failures.push(`${label} ${coordinator.name}: the coordinators expect another cluster map:\n${map}\nthe wizard wrote:\n${plan.topology}`);
    }
  }
}

fs.rmSync(work, { recursive: true, force: true });

if (failures.length) {
  console.error(failures.join("\n\n"));
  console.error(`\n${failures.length} of ${checked} checks failed`);
  process.exit(1);
}
console.log(`setup wizard: ${checked} node and coordinator configs accepted by ${path.basename(binary)}`);
