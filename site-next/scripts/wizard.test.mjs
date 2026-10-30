// node --test site-next/scripts/wizard.test.mjs
import assert from "node:assert/strict";
import fs from "node:fs";
import { test } from "node:test";
import * as wizard from "../src/lib/wizard.mjs";

const secrets = { password: "p1", replicationPassword: "p2", coordinatorSecret: "p3" };
const build = (input) => {
  const { answers, notes } = wizard.normalize(input);
  return { plan: wizard.plan(answers, { version: "9.9.9" }), notes };
};

test("the schema the server printed knows every variable the wizard writes", () => {
  const schema = JSON.parse(fs.readFileSync(new URL("../src/data/config-schema.json", import.meta.url), "utf8"));
  assert.doesNotThrow(() => wizard.checkSchema(schema));
  const broken = { settings: schema.settings.filter((s) => s.env !== "RUVIO_CLUSTER_GROUP") };
  assert.throws(() => wizard.checkSchema(broken), /RUVIO_CLUSTER_GROUP/);
});

test("a single server keeps port 6379 and needs no replication settings", () => {
  const { plan } = build({ topology: "single", shards: 4, durability: "memory" });
  assert.equal(plan.nodes.length, 1);
  assert.equal(plan.nodes[0].port, 6379);
  assert.equal(plan.nodes[0].env.RUVIO_SHARDS, "4");
  assert.equal(plan.nodes[0].env.RUVIO_ADVERTISE_HOST, "127.0.0.1");
  assert.equal(plan.nodes[0].env.RUVIO_REPLICATION_ROLE, undefined);
  assert.equal(plan.coordinators.length, 0);
});

test("replication forces a write-ahead log and says so", () => {
  const { plan, notes } = build({ topology: "replicas", durability: "memory" });
  assert.equal(plan.answers.durability, "everysec");
  assert.match(notes.join(" "), /everysec/);
  const [primary, ...replicas] = plan.nodes;
  assert.equal(primary.env.RUVIO_REPLICATION_ROLE, "primary");
  for (const r of replicas) assert.equal(r.env.RUVIO_REPLICATION_PRIMARY, primary.replication);
});

test("a cache gets a memory limit and an evicting policy", () => {
  const { plan } = build({ usage: "cache" });
  assert.equal(plan.nodes[0].env.RUVIO_MAXMEMORY, String(1024 * 1024 * 1024));
  assert.equal(plan.nodes[0].env.RUVIO_MAXMEMORY_POLICY, "allkeys-lru");
});

test("a reachable server always gets a password", () => {
  const { plan, notes } = build({ access: "network", password: false });
  assert.equal(plan.answers.password, true);
  assert.ok(notes.length > 0);
  assert.equal(plan.nodes[0].env.RUVIO_REQUIREPASS, "${RUVIO_PASSWORD}");
});

test("a cluster splits every slot once and names each group by its first member", () => {
  const { plan } = build({ topology: "cluster", groups: 3, shards: 2 });
  assert.deepEqual([0, 1, 2].map((g) => wizard.slotRange(g, 3)), ["0-5460", "5461-10921", "10922-16383"]);
  assert.match(plan.topology, /name = "a"\naddress = "ruvio-a1:7000"\nshards = 2\nslots = \["0-5460"\]/);
  assert.equal(new Set(plan.nodes.map((n) => n.port)).size, plan.nodes.length);
  for (const c of plan.coordinators) assert.match(c.file, /slots = \["10922-16383"\]/);
});

test("separate machines get one Compose file each, with the replication ports published", () => {
  const hosts = ["10.0.0.1", "10.0.0.2", "10.0.0.3"];
  const { plan } = build({ topology: "failover", placement: "many", hosts });
  assert.deepEqual(plan.machines, hosts);
  const out = wizard.render(plan, secrets);
  assert.equal(out.length, 3);
  const second = out[1].files.find((f) => f.name === "compose.yml").content;
  assert.match(second, /ruvio-2:/);
  assert.doesNotMatch(second, /ruvio-1:\n/);
  assert.match(second, /"26379:26379"/);
  assert.match(second, /RUVIO_REPLICATION_PRIMARY: "10\.0\.0\.1:26379"/);
  assert.match(second, /coordinator-2:/);
});

test("secrets stay in .env, never in compose.yml", () => {
  const { plan } = build({ topology: "failover" });
  const [{ files }] = wizard.render(plan, secrets);
  const env = files.find((f) => f.name === ".env").content;
  const compose = files.find((f) => f.name === "compose.yml").content;
  assert.match(env, /RUVIO_PASSWORD=p1\nRUVIO_REPLICATION_PASSWORD=p2\nRUVIO_COORDINATOR_SECRET=p3/);
  for (const secret of ["p1", "p2", "p3"]) assert.doesNotMatch(compose, new RegExp(`\\b${secret}\\b`));
});

test("scripts write both files with quoted here-documents and start Compose", () => {
  for (const format of ["bash", "powershell"]) {
    const { plan } = build({ topology: "cluster", format });
    const [{ files }] = wizard.render(plan, secrets);
    assert.equal(files.length, 1);
    const script = files[0].content;
    assert.match(script, /docker compose up -d/);
    assert.match(script, format === "bash" ? /<<'EOF'/ : /@'\n/);
    assert.match(script, /\$\{RUVIO_PASSWORD\}/, "Compose, not the shell, fills in the password");
  }
});

test("docker run is offered for one server only", () => {
  const { plan, notes } = build({ topology: "failover", format: "docker-run" });
  assert.equal(plan.answers.format, "compose");
  assert.ok(notes.length > 0);
  const single = build({ format: "docker-run" }).plan;
  const [{ files }] = wizard.render(single, secrets);
  assert.match(files[0].content, /^docker run -d --name ruvio/);
  assert.match(files[0].content, /-e RUVIO_REQUIREPASS=p1/);
});

test("random secrets use the whole length and differ", () => {
  const a = wizard.randomSecret();
  assert.equal(a.length, 24);
  assert.notEqual(a, wizard.randomSecret());
});
