import { test, expect } from "@playwright/test";

const bulk = (text) => ({ kind: "bulk", text });
const integer = (value) => ({ kind: "integer", value });
const array = (...items) => ({ kind: "array", items });
const shards = [
  { id: 0, host: "127.0.0.1", port: 6380, ranges: [[0, 8191]], slots: 8192, replicas: [] },
  { id: 1, host: "127.0.0.1", port: 6381, ranges: [[8192, 16383]], slots: 8192, replicas: [] },
];
const infoText = (port) => `run_id:r1\r\nruvio_backend:evented\r\nruvio_health:ok\r\nruvio_readiness:ready\r\nruvio_persistence_state:ok\r\nruvio_durability:memory\r\nconnected_clients:3\r\nget_commands:4\r\nset_commands:5\r\nincrement_commands:6\r\nother_commands:7\r\ntotal_commands_processed:22\r\nkeys:${port === 6381 ? 8 : 12}\r\ncharged_memory_bytes:1024\r\n`;

async function setup(page, options = {}) {
  const requests = [];
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  let queue = [];
  let session = null;
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const body = route.request().postDataJSON();
    requests.push({ path, body, sessionId: route.request().headers()["x-ruvio-session"] });
    let result = { ok: true };
    if (path === "/api/defaults") result = { ok: true, host: "127.0.0.1", port: 6380, password: "" };
    else if (path === "/api/session") result = { ok: true, connected: !!session, coordinators: !!options.coordinators, ...session };
    else if (path === "/api/coordinators") result = options.coordinators?.() ?? { ok: true, configured: false };
    else if (path === "/api/probe") {
      // The same INFO a pinned INFO command would answer, so command overrides drive both.
      result.nodes = body.nodes.map((node) => {
        const override = options.command?.(["INFO"], node);
        if (override?.ok === false) return { host: node.host, port: node.port, reachable: false, error: override.error };
        return { host: node.host, port: node.port, reachable: true, ms: 1, topology_version: 0, info: override?.value?.text ?? infoText(node.port) };
      });
    }
    else if (path === "/api/connect") session = { host: body.host, port: body.port, username: body.username };
    else if (path === "/api/disconnect") session = null;
    else if (path === "/api/shards") result.shards = options.shards ?? shards;
    else if (path === "/api/command") {
      const args = body.args;
      const override = options.command?.(args, body.shard);
      if (override) result = override;
      else if (args[0] === "INFO") {
        result.value = bulk(infoText(body.shard?.port));
      } else if (args[0] === "SCAN") {
        result.value = !body.shard && args[1] === "0"
          ? array(bulk("owner:1"), array())
          : array(bulk("0"), array(bulk("app:hello"), bulk("app:list")));
      } else if (args[0] === "TYPE") result.value = bulk(args[1] === "app:list" ? "list" : "string");
      else if (args[0] === "TTL") result.value = integer(-1);
      else if (args[0] === "GET") result.value = bulk("Hello Ruvio");
      else if (args[0] === "LLEN") result.value = integer(2);
      else if (args[0] === "LRANGE") result.value = array(bulk("first"), bulk("second"));
      else if (args[0] === "ACL" && args[1] === "USERS") result.value = array(bulk("default"), bulk("app"));
      else if (args[0] === "ACL" && args[1] === "GETUSER") result.value = array(bulk("flags"), bulk("on"), bulk("passwords"), bulk("1"), bulk("commands"), bulk("+@read"), bulk("keys"), bulk("~app:*"));
      else result.value = bulk("OK");
    } else if (path === "/api/pubsub/publish") {
      result.receivers = 1;
      queue.push({ kind: "message", channel: body.channel, text: body.message });
    } else if (path === "/api/pubsub/poll") {
      result.messages = queue;
      queue = [];
    } else if (path === "/api/pubsub/reset") queue = [];
    await route.fulfill({ json: result });
  });
  await page.goto("/");
  await expect(page.getByLabel("Port", { exact: true })).toHaveValue("6380");
  return { requests, errors };
}

async function signIn(page) {
  await page.getByRole("button", { name: "Open operator panel" }).click();
  await expect(page.getByRole("heading", { name: "Overview", exact: true })).toBeVisible();
  await expect(page.locator(".metric-card").first()).toContainText("2 / 2");
}

async function nav(page, name) {
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("button", { name, exact: true }).click();
}

async function selectShard(page, id) {
  const dropdown = page.locator(".scope-dropdown");
  if (!(await dropdown.evaluate((element) => element.open))) await dropdown.locator("summary").click();
  await dropdown.getByRole("checkbox", { name: new RegExp(`Shard ${id}`) }).check();
  await dropdown.locator("summary").click();
}

async function noOverflow(page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}

test("sign-in, live overview, direct navigation and phone layout", async ({ page }, testInfo) => {
  const { requests, errors } = await setup(page);
  await noOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("sign-in.png"), fullPage: true });
  await signIn(page);
  expect(requests.find((item) => item.path === "/api/connect").body).toEqual({ host: "127.0.0.1", port: 6380, username: "", password: "" });
  await expect(page.locator(".metric-card").nth(1)).toContainText("20");
  await expect(page.locator(".metric-card").nth(2)).toContainText("2.0 KiB");
  await expect(page.locator(".metric-card").nth(3)).toContainText("44");
  await noOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("overview.png"), fullPage: true });
  for (const [tab, heading] of [["Data", "Data"], ["Tasks", "Tasks"], ["Pub/Sub", "Pub/Sub"], ["ACL", "ACL users"]]) {
    await nav(page, tab);
    await expect(page.getByRole("heading", { name: heading, exact: true })).toBeVisible();
    await noOverflow(page);
    await page.screenshot({ path: testInfo.outputPath(`${tab.replace("/", "-")}.png`), fullPage: true });
  }
  expect(errors).toEqual([]);
});

test("navigation and session activity can collapse and the activity footer stays docked", async ({ page }) => {
  const { errors } = await setup(page);
  await signIn(page);
  const sidebarToggle = page.getByRole("button", { name: "Collapse navigation" });
  await sidebarToggle.click();
  await expect(page.getByRole("button", { name: "Expand navigation" })).toHaveAttribute("aria-expanded", "false");
  await page.getByRole("button", { name: "Expand navigation" }).click();
  await expect(page.getByRole("navigation", { name: "Main navigation" }).getByRole("button", { name: "Data" })).toBeVisible();

  const footer = page.getByRole("region", { name: "Session activity" });
  await expect(footer).toHaveCSS("position", "sticky");
  const before = await footer.boundingBox();
  await page.locator(".body").evaluate((element) => { element.scrollTop = element.scrollHeight; });
  const after = await footer.boundingBox();
  expect(after.y).toBe(before.y);
  await expect(footer).toBeInViewport();
  await expect(page.getByRole("button", { name: "Expand session activity" })).toContainText("1 / 100");
  await page.getByRole("button", { name: "Expand session activity" }).click();
  await expect(page.getByRole("button", { name: "Collapse session activity" })).toHaveAttribute("aria-expanded", "true");
  await page.getByRole("button", { name: "Collapse session activity" }).click();
  await expect(page.getByRole("button", { name: "Expand session activity" })).toHaveAttribute("aria-expanded", "false");
  expect(errors).toEqual([]);
});

test("credential modes are passed unchanged and reconnect starts at overview", async ({ page }) => {
  const { requests } = await setup(page);
  await page.getByLabel("Password", { exact: false }).fill("secret");
  await signIn(page);
  expect(requests.find((item) => item.path === "/api/connect").body).toMatchObject({ username: "", password: "secret" });
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await page.getByLabel("User", { exact: false }).fill("app");
  await page.getByLabel("Password", { exact: false }).fill("app-secret");
  await signIn(page);
  expect(requests.filter((item) => item.path === "/api/connect").at(-1).body).toMatchObject({ username: "app", password: "app-secret" });
});

test("all owners SCAN, pinned data and pinned command forms", async ({ page }) => {
  const { requests, errors } = await setup(page);
  await signIn(page);
  await nav(page, "Data");
  await expect(page.locator(".key-list button")).toHaveCount(2);
  expect(requests.filter((item) => item.body?.args?.[0] === "SCAN").map((item) => [item.body.args[1], item.body.shard])).toEqual([["0", null], ["owner:1", null]]);
  await selectShard(page, 1);
  await expect(page.locator(".key-list button")).toHaveCount(2);
  await page.locator(".key-list button").filter({ hasText: "app:hello" }).click();
  await expect(page.locator(".detail .result")).toHaveText("Hello Ruvio");
  expect(requests.filter((item) => item.body?.args?.[0] === "GET").at(-1).body.shard.port).toBe(6381);
  await nav(page, "Tasks");
  await page.getByLabel("Lock key").fill("lock:one");
  await page.getByLabel("Lifetime (milliseconds)").fill("5000");
  await page.locator("form").getByRole("button", { name: "Acquire a lock", exact: true }).click();
  await expect(page.getByLabel("Command result")).toContainText("OK");
  expect(requests.find((item) => item.body?.args?.[0] === "LEASE").body).toEqual({ args: ["LEASE", "lock:one", "5000"], shard: { host: "127.0.0.1", port: 6381 } });
  expect(errors).toEqual([]);
});

test("WRONGTYPE remains visible while choosing the next action; SET needs no token", async ({ page }) => {
  let reject = true;
  const { requests, errors } = await setup(page, { command: (args) => {
    if (args[0] === "GET" && reject) return { ok: false, error: "WRONGTYPE Operation against a key holding the wrong kind of value" };
  } });
  await signIn(page);
  await nav(page, "Tasks");
  await page.getByLabel("Find a task or command").fill("strings");
  await page.getByRole("region", { name: "Task search results" }).getByRole("button", { name: /Read a string/ }).click();
  await page.getByLabel("Key", { exact: true }).fill("app:list");
  await page.locator("form").getByRole("button", { name: "Read a string", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("WRONGTYPE");
  await page.getByRole("group", { name: "Strings actions" }).getByRole("button", { name: /Write a string/ }).click();
  await expect(page.getByRole("alert")).toContainText("WRONGTYPE");
  await expect(page.getByLabel(/token|version/i)).toHaveCount(0);
  reject = false;
  await page.getByLabel("Key", { exact: true }).fill("app:string");
  await page.getByLabel("Value", { exact: true }).fill("new value");
  await page.getByLabel(/Lifetime \(seconds\)/).fill("30");
  await page.locator("form").getByRole("button", { name: "Write a string", exact: true }).click();
  await expect(page.getByRole("alert")).toHaveCount(0);
  expect(requests.find((item) => item.body?.args?.[0] === "SET").body.args).toEqual(["SET", "app:string", "new value", "EX", "30"]);
  expect(errors).toEqual([]);
});

test("data deletion error remains actionable and requires confirmation", async ({ page }) => {
  let reject = true;
  const { errors } = await setup(page, { command: (args) => args[0] === "DEL" && reject ? { ok: false, error: "NOPERM delete denied" } : null });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").filter({ hasText: "app:hello" }).click();
  await page.getByRole("button", { name: "Delete", exact: true }).click();
  await page.getByRole("button", { name: "Confirm delete", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("NOPERM");
  reject = false;
  await page.getByRole("button", { name: "Confirm delete", exact: true }).click();
  await expect(page.locator(".key-list button")).toHaveCount(1);
  expect(errors).toEqual([]);
});

test("Pub/Sub publishes on selected scope and resets subscriptions on shard change", async ({ page }) => {
  const { requests, errors } = await setup(page);
  await signIn(page);
  await nav(page, "Pub/Sub");
  await page.getByLabel("Channel", { exact: true }).fill("orders.created");
  await page.getByRole("button", { name: "Subscribe to channel", exact: true }).click();
  await expect(page.locator(".subscription")).toContainText("orders.created");
  await page.getByLabel("Message", { exact: true }).fill("hello");
  await page.getByRole("button", { name: "Publish message", exact: true }).click();
  await expect(page.locator(".message pre")).toHaveText("hello");
  await selectShard(page, 1);
  await expect(page.locator(".subscription")).toHaveCount(0);
  await expect(page.locator(".message")).toHaveCount(0);
  await page.getByLabel("Channel", { exact: true }).fill("pinned");
  await page.getByRole("button", { name: "Subscribe to channel", exact: true }).click();
  await expect(page.locator(".subscription")).toHaveCount(1);
  expect(requests.filter((item) => item.path === "/api/pubsub/subscribe").at(-1).body.shard.port).toBe(6381);
  expect(requests.some((item) => item.path === "/api/pubsub/reset")).toBe(true);
  expect(errors).toEqual([]);
});

test("Pub/Sub listens for key events on the chosen database and shard", async ({ page }) => {
  const { requests, errors } = await setup(page);
  await signIn(page);
  await nav(page, "Pub/Sub");
  await page.getByLabel("Database", { exact: true }).fill("4");
  await page.getByRole("button", { name: "Listen for expired", exact: true }).click();
  await expect(page.locator(".subscription")).toContainText("__keyevent@4__:expired");

  const request = requests.find((item) => item.path === "/api/pubsub/subscribe");
  expect(request.body.sharded_channels).toEqual(["__keyevent@4__:expired"]);
  expect(request.body.channels).toEqual([]);
  expect(request.body.patterns).toEqual([]);

  await selectShard(page, 1);
  await page.getByRole("button", { name: "Listen for evicted", exact: true }).click();
  await expect.poll(() => requests.filter((item) => item.path === "/api/pubsub/subscribe").length).toBe(2);
  const pinnedRequest = requests.filter((item) => item.path === "/api/pubsub/subscribe").at(-1);
  expect(pinnedRequest.body.sharded_channels).toEqual(["__keyevent@4__:evicted"]);
  expect(pinnedRequest.body.shard.port).toBe(6381);
  expect(errors).toEqual([]);
});

test("ACL directory loads immediately and changes remain explicitly temporary", async ({ page }) => {
  const { requests, errors } = await setup(page);
  await signIn(page);
  await nav(page, "ACL");
  await expect(page.locator(".user-list button")).toHaveCount(2);
  await expect(page.getByText("Live changes only.", { exact: true })).toBeVisible();
  await expect(page.getByText(/Put users that must survive a restart/)).toContainText("ruvio.toml");
  await page.locator(".user-list button").filter({ hasText: "app" }).click();
  await expect(page.getByRole("heading", { name: "Access for app" })).toBeVisible();
  await page.getByLabel("User", { exact: true }).fill("worker");
  await page.getByLabel("Rules", { exact: false }).fill("on\n>secret\n~app:*\n+@read");
  await page.getByRole("button", { name: "Create or change a user", exact: true }).click();
  await expect(page.getByLabel("Command result")).toContainText("OK");
  expect(requests.find((item) => item.body?.args?.[1] === "SETUSER").body.args).toEqual(["ACL", "SETUSER", "worker", "on", ">secret", "~app:*", "+@read"]);
  expect(requests.some((item) => item.body?.args?.join(" ") === "ACL SAVE")).toBe(false);
  expect(errors).toEqual([]);
});

test("unavailable metrics are not zero and HTTP errors leave forms usable", async ({ page }) => {
  await setup(page, { command: (args, shard) => {
    if (["INFO", "PING"].includes(args[0]) && shard?.port === 6381) return { ok: false, error: "connection refused" };
  } });
  await page.getByRole("button", { name: "Open operator panel" }).click();
  await expect(page.locator(".metric-card").first()).toContainText("1 / 2");
  await expect(page.locator(".metric-card").nth(1)).toContainText("Not reported");
  await expect(page.locator(".notices")).toContainText("127.0.0.1:6381 (shard 1 primary) is not answering");
  await nav(page, "Tasks");
  await page.getByLabel("Lock key").fill("lock");
  await page.getByLabel("Lifetime (milliseconds)").fill("500");
  await page.route("**/api/command", (route) => route.fulfill({ status: 503, json: { ok: false, error: "UI bridge unavailable" } }));
  await page.locator("form").getByRole("button", { name: "Acquire a lock", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("UI bridge unavailable");
  await expect(page.locator("form").getByRole("button", { name: "Acquire a lock", exact: true })).toBeEnabled();
});

test("failed value pagination keeps the old page and retry reads the same next slice", async ({ page }) => {
  let reject = true;
  const { requests, errors } = await setup(page, { command: (args) => {
    if (args[0] === "LLEN") return { ok: true, value: integer(80) };
    if (args[0] === "LRANGE") {
      if (args[2] === "40" && reject) return { ok: false, error: "ERR temporary read failure" };
      return { ok: true, value: array(...Array.from({ length: 40 }, (_, index) => bulk(`item-${Number(args[2]) + index}`))) };
    }
  } });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").filter({ hasText: "app:list" }).click();
  await expect(page.locator(".detail table tbody tr").first()).toContainText("item-0");
  await page.locator(".detail").getByRole("button", { name: "Next", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("temporary read failure");
  await expect(page.locator(".detail table tbody tr").first()).toContainText("item-0");
  reject = false;
  await page.locator(".detail").getByRole("button", { name: "Next", exact: true }).click();
  await expect(page.locator(".detail table tbody tr").first()).toContainText("item-40");
  expect(requests.filter((item) => item.body?.args?.[0] === "LRANGE").map((item) => item.body.args[2])).toEqual(["0", "40", "40"]);
  expect(errors).toEqual([]);
});

test("live readings refresh, pause, and remain accessible after scrolling", async ({ page }) => {
  let keys = 1;
  await setup(page, { command: (args) => args[0] === "INFO"
    ? { ok: true, value: bulk(`keys:${keys}\r\ncharged_memory_bytes:0\r\ntotal_commands_processed:0\r\n`) }
    : null });
  await signIn(page);
  await expect(page.locator(".metric-card").nth(1)).toContainText("2");
  keys = 2;
  await expect(page.locator(".metric-card").nth(1)).toContainText("4", { timeout: 8000 });
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.locator(".live-label")).toContainText("Paused");
  keys = 3;
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.locator(".metric-card").nth(1)).toContainText("6");
  await page.evaluate(() => document.querySelector(".body").scrollTo(0, 100000));
  await nav(page, "Data");
  await expect(page.getByRole("heading", { name: "Data", exact: true })).toBeInViewport();
});

test("reload restores the session and sign-out retains only host and port", async ({ page }) => {
  const { requests } = await setup(page);
  await signIn(page);
  const id = requests.find((item) => item.path === "/api/connect").sessionId;
  await page.reload();
  await expect(page.getByRole("heading", { name: "Overview", exact: true })).toBeVisible();
  expect(requests.filter((item) => item.path === "/api/connect")).toHaveLength(1);
  expect(requests.filter((item) => item.path === "/api/disconnect")).toHaveLength(0);
  expect(requests.filter((item) => item.path === "/api/session").at(-1).sessionId).toBe(id);
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await page.reload();
  await expect(page.getByLabel("Host", { exact: true })).toHaveValue("127.0.0.1");
  await expect(page.getByLabel("Port", { exact: true })).toHaveValue("6380");
  await expect(page.getByLabel("Password", { exact: false })).toHaveValue("");
  expect(await page.evaluate(() => localStorage.getItem("ruvio.connection"))).toBe('{"host":"127.0.0.1","port":6380}');
});

test("overview interval and pause survive navigation and reload", async ({ page }) => {
  await setup(page);
  await signIn(page);
  await page.getByLabel("Refresh interval").selectOption("30");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await nav(page, "Data");
  await nav(page, "Overview");
  await expect(page.getByLabel("Refresh interval")).toHaveValue("30");
  await expect(page.getByRole("button", { name: "Resume", exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByLabel("Refresh interval")).toHaveValue("30");
  await expect(page.locator(".live-label")).toContainText("Paused");
});

test("data selection leaves key pane mounted and reports collection length", async ({ page }) => {
  const { requests } = await setup(page);
  await signIn(page);
  await nav(page, "Data");
  await expect(page.locator(".key-list button")).toHaveCount(2);
  await page.locator(".key-pane").evaluate((element) => { window.originalKeyPane = element; });
  const scans = requests.filter((item) => item.body?.args?.[0] === "SCAN").length;
  await page.locator(".key-list button").filter({ hasText: "app:list" }).click();
  await expect(page.locator(".detail-head")).toContainText("2 items total");
  await expect(page.locator(".key-pane button[type=submit]")).toHaveText("Find");
  expect(await page.locator(".key-pane").evaluate((element) => element === window.originalKeyPane)).toBe(true);
  expect(requests.filter((item) => item.body?.args?.[0] === "SCAN")).toHaveLength(scans);
});

test("multiple shard selection filters overview and scans while keyed writes route normally", async ({ page }) => {
  const { requests } = await setup(page);
  await signIn(page);
  await selectShard(page, 0);
  await expect(page.locator(".metric-card").nth(1)).toContainText("12");
  await selectShard(page, 1);
  await expect(page.locator(".metric-card").nth(1)).toContainText("20");
  await nav(page, "Data");
  await expect(page.locator(".key-list button")).toHaveCount(2);
  const scans = requests.filter((item) => item.body?.args?.[0] === "SCAN");
  expect(scans.map((item) => item.body.shard.port)).toEqual([6380, 6381]);
  await nav(page, "Tasks");
  await page.getByLabel("Lock key").fill("lock:one");
  await page.getByLabel("Lifetime (milliseconds)").fill("5000");
  await page.locator("form").getByRole("button", { name: "Acquire a lock", exact: true }).click();
  // The click returns before the request reaches the mock: wait for it rather than read it at once.
  await expect.poll(() => requests.find((item) => item.body?.args?.[0] === "LEASE")?.body).toBeTruthy();
  expect(requests.find((item) => item.body?.args?.[0] === "LEASE").body.shard).toBeNull();
});

test("read transport failures are retried without showing a page error", async ({ page }) => {
  await setup(page);
  await signIn(page);
  await nav(page, "Data");
  let attempts = 0;
  await page.route("**/api/command", async (route) => {
    if (route.request().postDataJSON().args[0] !== "GET") return route.fallback();
    attempts += 1;
    if (attempts === 1) return route.abort("connectionreset");
    return route.fulfill({ json: { ok: true, value: bulk("recovered") } });
  });

  await page.locator(".key-list button").filter({ hasText: "app:hello" }).click();
  await expect(page.locator(".detail .result")).toHaveText("recovered");
  await expect(page.getByRole("alert")).toHaveCount(0);
  expect(attempts).toBe(2);
});

test("refresh keeps the old value until the replacement is ready", async ({ page }) => {
  const { requests } = await setup(page);
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").filter({ hasText: "app:hello" }).click();
  await expect(page.locator(".detail .result")).toHaveText("Hello Ruvio");
  const resultTop = await page.locator(".detail .result").evaluate((element) => element.offsetTop);
  const signOut = page.getByRole("button", { name: "Sign out", exact: true });
  const signOutOpacity = await signOut.evaluate((element) => getComputedStyle(element).opacity);
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  await page.route("**/api/command", async (route) => {
    if (route.request().postDataJSON().args[0] !== "GET") return route.fallback();
    await held;
    await route.fulfill({ json: { ok: true, value: bulk("updated") } });
  });
  const scans = requests.filter((item) => item.body?.args?.[0] === "SCAN").length;
  await page.locator(".detail").getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.locator(".detail .result")).toHaveText("Hello Ruvio");
  await expect(page.locator(".key-pane button[type=submit]")).toBeEnabled();
  expect(await page.locator(".detail .result").evaluate((element) => element.offsetTop)).toBe(resultTop);
  expect(await signOut.evaluate((element) => getComputedStyle(element).opacity)).toBe(signOutOpacity);
  expect(await signOut.getAttribute("disabled")).toBeNull();
  release();
  await expect(page.locator(".detail .result")).toHaveText("updated");
  expect(requests.filter((item) => item.body?.args?.[0] === "SCAN")).toHaveLength(scans);
});

test("hash totals come from HLEN rather than the current page", async ({ page }) => {
  await setup(page, { command: (args) => {
    if (args[0] === "TYPE") return { ok: true, value: bulk("hash") };
    if (args[0] === "HLEN") return { ok: true, value: integer(123) };
    if (args[0] === "HSCAN") return { ok: true, value: array(bulk("10"), array(bulk("field"), bulk("value"))) };
  } });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").first().click();
  await expect(page.locator(".detail-head")).toContainText("123 fields total");
  await expect(page.locator(".detail tbody tr")).toHaveCount(1);
});

const document = { name: "Ada", age: 36, ok: true, tags: ["a", "b"], addr: { city: "X", zip: null }, "odd key": 1 };

function jsonBackend(sent) {
  let text = JSON.stringify(document);
  return (args) => {
    if (args[0] === "TYPE") return { ok: true, value: bulk("json") };
    if (args[0] === "MEMORY") return { ok: true, value: integer(300) };
    if (args[0] === "JSON.GET") {
      sent?.push(args);
      const path = args[2];
      if (!path || path === "$") return { ok: true, value: bulk(text) };
      if (path === "$.addr") return { ok: true, value: bulk(JSON.stringify(document.addr)) };
      return { ok: true, value: { kind: "null" } };
    }
    if (["JSON.SET", "JSON.DEL", "JSON.NUMINCRBY", "JSON.TOGGLE", "JSON.ARRAPPEND", "JSON.CLEAR"].includes(args[0])) {
      sent?.push(args);
      return { ok: true, value: args[0] === "JSON.SET" ? { kind: "simple", text: "OK" } : integer(1) };
    }
  };
}

test("a JSON key opens as a tree, with raw text and path reads", async ({ page }, testInfo) => {
  const sent = [];
  const { errors } = await setup(page, { command: jsonBackend(sent) });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").first().click();
  await expect(page.locator(".detail-head")).toContainText("json");
  const tree = page.getByRole("tree", { name: "JSON document" });
  await expect(tree).toContainText('"name"');
  await expect(tree).toContainText('"Ada"');
  await expect(tree).toContainText("array · 2 items");
  await expect(page.locator(".detail")).toContainText("object · 6 keys");
  await tree.getByRole("button", { name: "Collapse" }).first().click();
  await expect(tree).not.toContainText('"Ada"');
  await tree.getByRole("button", { name: "Expand" }).first().click();
  await expect(tree).toContainText('"Ada"');
  await page.getByRole("tab", { name: "Raw" }).click();
  await expect(page.locator(".jraw")).toContainText('"name": "Ada"');
  await page.getByRole("tab", { name: "Tree" }).click();
  await page.getByLabel("Path", { exact: true }).fill("$.addr");
  await page.getByRole("button", { name: "Read path" }).click();
  await expect(page.getByLabel("Path result")).toContainText('"city": "X"');
  await page.screenshot({ path: testInfo.outputPath("json-tree.png"), fullPage: true });
  await noOverflow(page);
  expect(errors).toEqual([]);
});

const lastWrite = (sent) => sent.filter((item) => item[0] !== "JSON.GET").at(-1);

test("JSON values are edited, incremented and deleted by path; bad JSON is rejected before sending", async ({ page }) => {
  const sent = [];
  await setup(page, { command: jsonBackend(sent) });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").first().click();
  const tree = page.getByRole("tree", { name: "JSON document" });
  await tree.getByRole("button", { name: /"name"/ }).click();
  const draft = page.getByLabel("Value as JSON");
  await expect(draft).toHaveValue('"Ada"');
  await draft.fill('"Grace"');
  await page.getByRole("button", { name: "Save value" }).click();
  await expect(page.locator(".jdoc").getByRole("status")).toContainText("Saved $.name");
  await expect.poll(() => lastWrite(sent)).toEqual(["JSON.SET", "app:hello", "$.name", '"Grace"']);
  await draft.fill("{broken");
  await page.getByRole("button", { name: "Save value" }).click();
  await expect(page.getByRole("alert")).toContainText("Not valid JSON");
  expect(sent.filter((item) => item[0] === "JSON.SET")).toHaveLength(1);
  await tree.getByRole("button", { name: /"age"/ }).click();
  await page.getByLabel("Amount").fill("2");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await expect.poll(() => lastWrite(sent)).toEqual(["JSON.NUMINCRBY", "app:hello", "$.age", "2"]);
  await tree.getByRole("button", { name: /"ok"/ }).click();
  await page.getByRole("button", { name: "Toggle" }).click();
  await expect.poll(() => lastWrite(sent)).toEqual(["JSON.TOGGLE", "app:hello", "$.ok"]);
  await tree.getByRole("button", { name: /"tags"/ }).click();
  await page.getByLabel("Value to append").fill('"c"');
  await page.getByRole("button", { name: "Append" }).click();
  await expect.poll(() => lastWrite(sent)).toEqual(["JSON.ARRAPPEND", "app:hello", "$.tags", '"c"']);
  await expect(page.locator(".jdoc").getByRole("status")).toContainText("Appended to $.tags");
  await expect(tree.getByRole("button", { name: /"odd key"/ })).toBeDisabled();
  await tree.getByRole("button", { name: /"addr"/ }).click();
  await page.getByRole("button", { name: "Delete path" }).click();
  await expect.poll(() => lastWrite(sent)).toEqual(["JSON.DEL", "app:hello", "$.addr"]);
});

test("a large JSON document is read by path instead of loaded whole", async ({ page }) => {
  const sent = [];
  const backend = jsonBackend(sent);
  await setup(page, { command: (args) => (args[0] === "MEMORY" ? { ok: true, value: integer(9 * 1024 * 1024) } : backend(args)) });
  await signIn(page);
  await nav(page, "Data");
  await page.locator(".key-list button").first().click();
  await expect(page.locator(".detail")).toContainText("not loaded whole");
  expect(sent.filter((item) => item[0] === "JSON.GET")).toHaveLength(0);
  await page.getByLabel("Path", { exact: true }).fill("$.addr");
  await page.getByRole("button", { name: "Read path" }).click();
  await expect(page.getByLabel("Path result")).toContainText('"city": "X"');
});

test("JSON task replies render as a tree and forms reject invalid JSON", async ({ page }) => {
  await setup(page, { command: (args) => (args[0] === "JSON.GET" ? { ok: true, value: bulk('{"a":{"b":[1,2]}}') } : undefined) });
  await signIn(page);
  await nav(page, "Tasks");
  await page.getByRole("searchbox").fill("json");
  await page.getByRole("button", { name: /Read a document or path/ }).first().click();
  await page.getByLabel("Document", { exact: true }).fill("doc");
  await page.getByRole("button", { name: "Read a document or path" }).last().click();
  await expect(page.locator(".response .jtree")).toContainText('"a"');
  await page.getByRole("button", { name: /Write a document or path/ }).first().click();
  await page.getByLabel("Document", { exact: true }).fill("doc");
  await page.locator(".workbench textarea").fill("{nope");
  await page.getByRole("button", { name: "Write a document or path" }).last().click();
  await expect(page.locator(".response")).toContainText("not valid JSON");
});

test("a shard with several ranges is counted once and its replicas are probed", async ({ page }) => {
  const split = [
    { id: 0, host: "127.0.0.1", port: 6380, ranges: [[0, 99], [200, 8191]], slots: 8092, replicas: [{ host: "127.0.0.1", port: 6390, offset: 3 }] },
    { id: 1, host: "127.0.0.1", port: 6381, ranges: [[100, 199], [8192, 16383]], slots: 8292, replicas: [] },
  ];
  const { requests, errors } = await setup(page, { shards: split, command: (args, node) => {
    if (args[0] !== "INFO") return null;
    if (node?.port === 6390) return { ok: true, value: bulk("keys:12\r\ncharged_memory_bytes:1024\r\ntotal_commands_processed:5\r\nslave_repl_offset:40\r\nmaster_link_status:up\r\n") };
    if (node?.port === 6380) return { ok: true, value: bulk("keys:12\r\ncharged_memory_bytes:1024\r\ntotal_commands_processed:22\r\nwal_last_sequence:50\r\n") };
    return null;
  } });
  await signIn(page);
  await expect(page.locator(".metric-card").nth(1)).toContainText("20");
  await expect(page.locator(".metric-card").first()).toContainText("Replicas: 1 / 1 answering");
  await expect(page.locator(".distribution-row:not(.head)")).toHaveCount(2);
  await expect(page.locator(".distribution-row").nth(1)).toContainText("10 behind");
  const probe = requests.find((item) => item.path === "/api/probe").body;
  expect(probe.nodes.map((node) => node.port)).toEqual([6380, 6390, 6381]);
  expect(probe.extended).toBe(true);
  await expect(page.locator(".slot-segment")).toHaveCount(4);
  expect(errors).toEqual([]);
});

test("the cluster view reads groups and coordinators without sending commands", async ({ page }, testInfo) => {
  const fieldList = (object) => array(...Object.entries(object).flatMap(([name, value]) => [bulk(name), bulk(String(value))]));
  const { requests, errors } = await setup(page, { coordinators: () => ({
    ok: true,
    configured: true,
    source: "c2:26400",
    coordinators: [
      { address: "c1:26400", ok: false, error: "Connection refused" },
      { address: "c2:26400", ok: true, info: "coordinator_id:2\r\nleader_id:2\r\nis_leader:1\r\nfailovers_total:1\r\nlast_failover_unix_ms:0\r\ngroup_main:primary=127.0.0.1:6380,epoch=3,members_answering=2/3,state=waiting,state_ms=4000\r\n" },
    ],
    masters: array(fieldList({ name: "main", ip: "127.0.0.1", port: 6380 })),
    replicas: { main: array() },
    status: fieldList({ state: "moving", id: "m1", first: 100, last: 199, from: "main", to: "other", shard: 0, moved_ranges: 2 }),
  }) });
  await signIn(page);
  await nav(page, "Cluster");
  await expect(page.getByRole("heading", { name: "Cluster", exact: true })).toBeVisible();
  await expect(page.locator(".coordinator-list .badge.leader")).toHaveCount(1);
  await expect(page.locator(".coordinator-list")).toContainText("Connection refused");
  await expect(page.getByRole("table", { name: "Groups the coordinators watch" })).toContainText("Waiting for a replica with every committed write");
  await expect(page.locator(".move-status").first()).toContainText("Moving slots 100–199 from main to other");
  await expect(page.locator(".group-panel")).toHaveCount(1);
  await noOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("cluster.png"), fullPage: true });
  expect(requests.filter((item) => item.path === "/api/command")).toEqual([]);
  expect(errors).toEqual([]);
});

test("the dashboard draws the chosen charts from the probe readings and remembers the choice", async ({ page }, testInfo) => {
  let commands = 0;
  const { requests, errors } = await setup(page, { command: (args) => {
    if (args[0] !== "INFO") return null;
    commands += 500;
    return { ok: true, value: bulk(`run_id:r1\r\ntotal_commands_processed:${commands}\r\nget_commands:${commands / 2}\r\nset_commands:${commands / 2}\r\nincrement_commands:0\r\nother_commands:0\r\ntotal_net_input_bytes:${commands * 40}\r\ntotal_net_output_bytes:${commands * 10}\r\nconnected_clients:4\r\ncharged_memory_bytes:2048\r\nkey_memory_bytes:512\r\nvalue_memory_bytes:1536\r\nkeys:12\r\nexpires:2\r\n`) };
  } });
  await signIn(page);
  await page.getByRole("navigation", { name: "Tools" }).getByRole("button", { name: "Dashboard", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Dashboard", exact: true })).toBeVisible();
  await page.getByLabel("Refresh interval").selectOption("2");
  await expect(page.locator(".chart-card")).toHaveCount(7);
  await expect(page.locator(".chart-card").filter({ hasText: "Commands per second" }).locator("svg")).toBeVisible({ timeout: 8000 });
  await expect(page.locator(".chart-card").filter({ hasText: "Charged memory" }).locator(".chart-now")).toContainText("4.0 KiB");
  await page.locator(".chart-picker summary").click();
  await page.getByRole("group", { name: "Charts to show" }).getByLabel("Memory breakdown").uncheck();
  await page.getByRole("group", { name: "Charts to show" }).getByLabel("Largest buffers seen").check();
  await page.locator(".chart-picker summary").click();
  await expect(page.locator(".chart-card")).toHaveCount(7);
  await expect(page.locator(".chart-card").filter({ hasText: "Memory breakdown" })).toHaveCount(0);
  await page.getByRole("button", { name: "Per node", exact: true }).click();
  await expect(page.locator(".chart-card").filter({ hasText: "Connected clients" }).locator(".chart-legend li")).toHaveCount(2);
  await noOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("dashboard.png"), fullPage: true });
  await page.reload();
  await page.getByRole("navigation", { name: "Tools" }).getByRole("button", { name: "Dashboard", exact: true }).click();
  await expect(page.locator(".chart-card").filter({ hasText: "Largest buffers seen" })).toHaveCount(1);
  await expect(page.getByRole("button", { name: "Per node", exact: true })).toHaveAttribute("aria-pressed", "true");
  // Every chart comes from the probe: the dashboard sends no command of its own.
  expect(requests.filter((item) => item.path === "/api/command")).toEqual([]);
  expect(errors).toEqual([]);
});
