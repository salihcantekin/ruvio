<script>
  import { onMount } from "svelte";
  import "./app.css";
  import Icon from "./Icon.svelte";
  let scopeMenu = $state();
  // Close header dropdowns (shard picker, notices) on a click outside them or on Escape.
  function closeScopeMenu(event) {
    for (const menu of document.querySelectorAll("details.scope-dropdown[open]")) {
      if (event.type === "keydown" ? event.key === "Escape" : !menu.contains(event.target)) menu.open = false;
    }
  }
  import { api } from "./api.js";
  import { execute, nodeId, scanOwners, scanSelected } from "./operator.js";
  import { preference, savePreference } from "./preferences.js";
  import { summarize } from "./value.js";
  import Overview from "./Overview.svelte";
  import Tasks from "./Tasks.svelte";
  import Browser from "./Browser.svelte";
  import PubSub from "./PubSub.svelte";
  import Users from "./Users.svelte";
  import Console from "./Console.svelte";
  import ThemeToggle from "./ThemeToggle.svelte";

  const navigation = [
    { id: "overview", label: "Overview", icon: "overview" },
    { id: "data", label: "Data", icon: "data" },
    { id: "tasks", label: "Tasks", icon: "tasks" },
    { id: "pubsub", label: "Pub/Sub", icon: "pubsub" },
    { id: "users", label: "ACL", icon: "users" },
  ];
  const dock = { id: "console", label: "Console", icon: "terminal" };
  const logLimit = 100;
  let host = $state("127.0.0.1");
  let port = $state(6379);
  let username = $state("");
  let password = $state("");
  let connected = $state(false);
  let restoring = $state(true);
  let gateError = $state("");
  let busyGate = $state(false);
  let section = $state("overview");
  let shards = $state([]);
  let selectedIds = $state([]);
  let selectedNodes = $derived(shards.filter((item) => selectedIds.includes(nodeId(item))));
  let shard = $derived(selectedNodes.length === 1 ? selectedNodes[0] : null);
  let overviewNodes = $derived(selectedNodes.length ? selectedNodes : shards);
  let topologyError = $state("");
  let topologyNotice = $state("");
  let sessionError = $state("");
  let switching = $state(false);
  let pubBusy = $state(false);
  let activeCommands = $state(0);
  let pubsubDatabase = $state("0");
  let scopeBusy = $derived(switching || pubBusy || activeCommands > 0);
  let task = $state("locking");
  let lines = $state([]);
  let consoleOpen = $state(false);
  let sidebarCollapsed = $state(false);
  let scope = $derived(selectedIds.length ? selectedIds.join(",") : "all");
  let scopeLabel = $derived(shard ? nodeId(shard) : selectedNodes.length ? `${selectedNodes.length} selected shards` : "All shards");

  onMount(() => {
    let active = true;
    async function restore() {
      try {
        const session = await api("/api/session");
        if (!active) return;
        if (session.connected) {
          host = session.host;
          port = session.port;
          username = session.username;
          connected = true;
          await loadShards();
        } else {
          const defaults = await api("/api/defaults");
          if (!active) return;
          const last = preference("connection", {});
          // Host/port set through RUVIO_UI_TARGET_* win over the remembered connection.
          const remembered = defaults.pinned ? {} : last;
          host = remembered.host ?? defaults.host ?? host;
          port = remembered.port ?? defaults.port ?? port;
          password = defaults.password ?? password;
        }
      } catch (error) {
        if (active) gateError = `Could not restore connection: ${error.message}`;
      } finally {
        if (active) restoring = false;
      }
    }
    restore();
    return () => { active = false; };
  });

  function note(command, detail, ok = true, ms = null, location = scopeLabel) {
    lines = [{ time: new Date().toLocaleTimeString(), command, detail, ok, ms, location }, ...lines].slice(0, logLimit);
  }

  async function connect(event) {
    event.preventDefault();
    busyGate = true;
    gateError = "";
    try {
      await api("/api/connect", { host, port: Number(port), username, password });
      savePreference("connection", { host, port: Number(port) });
      password = "";
      connected = true;
      section = "overview";
      selectedIds = [];
      lines = [];
      sessionError = "";
      note("CONNECT", `${username || "default"} @ ${host}:${port}`);
      await loadShards();
    } catch (error) {
      gateError = error.message;
    } finally {
      busyGate = false;
    }
  }

  async function loadShards() {
    topologyError = "";
    try {
      const result = await api("/api/shards");
      shards = result.shards;
      topologyNotice = result.warning || "";
    } catch (error) {
      topologyError = `Could not discover shards: ${error.message}`;
    }
  }

  async function pickShards(ids) {
    if (scopeBusy) return;
    switching = true;
    sessionError = "";
    try {
      // A subscription must never silently remain attached to the previous node.
      await api("/api/pubsub/reset", {});
      selectedIds = ids;
      note("SCOPE", ids.length ? ids.join(", ") : "All shards");
    } catch (error) {
      sessionError = `Could not change scope: ${error.message}`;
    } finally {
      switching = false;
    }
  }

  async function disconnect() {
    if (scopeBusy) return;
    sessionError = "";
    switching = true;
    try {
      await api("/api/disconnect", {});
      connected = false;
      password = "";
      shards = [];
      lines = [];
    } catch (error) {
      sessionError = `Could not sign out: ${error.message}`;
    } finally {
      switching = false;
    }
  }

  async function run(args) {
    activeCommands += 1;
    const destination = shard ? { host: shard.host, port: shard.port } : null;
    const location = scopeLabel;
    const started = performance.now();
    const label = args[0] === "ACL" || args[0] === "XGROUP" ? args.slice(0, 2).join(" ") : args[0];
    try {
      const value = selectedNodes.length > 1 && args[0] === "SCAN"
        ? await scanSelected(args, selectedNodes)
        : !destination && args[0] === "SCAN"
        ? await scanOwners(args, (page) => execute(page, destination))
        : await execute(args, destination);
      note(label, args[0] === "ACL" ? "Completed" : summarize(value), true, performance.now() - started, location);
      return value;
    } catch (error) {
      note(label, error.message, false, performance.now() - started, location);
      throw error;
    } finally {
      activeCommands -= 1;
    }
  }

  function navigate(id) {
    section = id;
    for (const el of document.querySelectorAll(".body, .workspace")) el.scrollTo({ top: 0, behavior: "instant" });
  }

  function openTask(id) {
    task = id;
    navigate("tasks");
  }
</script>

<svelte:window onclick={closeScopeMenu} onkeydown={closeScopeMenu} />

{#if restoring}
  <main class="gate"><p role="status">Restoring session...</p></main>
{:else if !connected}
  <main class="gate">
    <div class="gate-theme"><ThemeToggle /></div>
    <div class="gate-intro">
      <div class="mark"><img class="logo" src="/mark.png" alt="" /> ruvio <span class="product-label">OPERATOR</span></div>
      <h1>Your data.<br />A clearer view.</h1>
      <p>See shard health, explore data, and manage the work running on Ruvio.</p>
      <div class="connection-path"><span>Browser</span><span>HTTP</span><span>Operator</span><span>RESP</span><span>Ruvio</span></div>
      <p class="hint">One binary. A direct connection to your server.</p>
    </div>
    <form class="panel gate-form" onsubmit={connect}>
      <div><p class="eyebrow">CONNECT TO RUVIO</p><h2>Sign in to your server</h2></div>
      <p class="muted">Use your server ACL credentials. There is no separate operator account.</p>
      <div class="host-fields">
        <label>Host<input bind:value={host} required autocomplete="off" spellcheck="false" placeholder="127.0.0.1" /></label>
        <label>Port<input bind:value={port} type="number" min="1" max="65535" required /></label>
      </div>
      <label>User <span class="optional">optional</span><input bind:value={username} placeholder="default" autocomplete="username" /></label>
      <label>Password <span class="optional">optional</span><input bind:value={password} type="password" autocomplete="current-password" /></label>
      <p class="hint">Both empty: connect as default without AUTH. Password only: AUTH password. User and password: AUTH user password.</p>
      {#if gateError}<div class="error-box" role="alert">{gateError}</div>{/if}
      <button class="primary" disabled={busyGate}>{busyGate ? "Connecting..." : "Open operator panel"} <span aria-hidden="true">&rarr;</span></button>
    </form>
    <p class="gate-foot">Ruvio operator &middot; Memory-first. Owner-aware.</p>
  </main>
{:else}
  <div class="shell">
    <header class="top">
      <div class="mark"><img class="logo" src="/mark.png" alt="" /> ruvio <span class="product-label">OPERATOR</span></div>
      <div class="who"><span class="status-dot"></span><span>{host}:{port}</span><span class="badge">{username || "default"}</span></div>
      {#if topologyError || topologyNotice}
        <details class="scope-dropdown notice-dropdown" class:alert={topologyError}>
          <summary><Icon name="info" /> {topologyError ? "Problem" : "Notice"}</summary>
          <div class="scope-menu" role="status">
            {#if topologyError}<p class="err">{topologyError}</p><button onclick={loadShards}>Retry discovery</button>{/if}
            {#if topologyNotice}<p>{topologyNotice}</p>{/if}
          </div>
        </details>
      {/if}
      <details class="scope-dropdown" bind:this={scopeMenu}>
        <summary>{scopeLabel} <span class="count">{shards.length}</span></summary>
        <div class="scope-menu" role="group" aria-label="Shard scope">
          <button class:on={!selectedIds.length} disabled={scopeBusy} onclick={() => pickShards([])}>All shards</button>
          {#each shards as item}
            <label><input type="checkbox" checked={selectedIds.includes(nodeId(item))} disabled={scopeBusy} onchange={(event) => pickShards(event.currentTarget.checked ? [...selectedIds, nodeId(item)] : selectedIds.filter((id) => id !== nodeId(item)))} /><span>Shard {item.id}<small>{nodeId(item)} &middot; {item.start}&ndash;{item.end}</small></span></label>
          {/each}
          <p class="hint">Multiple selections filter Overview and Data scans. Keyed commands still route to their owner. One selection pins commands to that shard.</p>
        </div>
      </details>
      <ThemeToggle />
      <button class="quiet" onclick={disconnect} aria-disabled={scopeBusy}>Sign out</button>
    </header>
    <div class="body">
      <aside class="sidebar" class:collapsed={sidebarCollapsed}>
        <button class="quiet sidebar-toggle" title={sidebarCollapsed ? "Expand navigation" : "Collapse navigation"} aria-label={sidebarCollapsed ? "Expand navigation" : "Collapse navigation"} aria-expanded={!sidebarCollapsed} onclick={() => (sidebarCollapsed = !sidebarCollapsed)}>
          <Icon name={sidebarCollapsed ? "chevronsRight" : "chevronsLeft"} /><span class="sidebar-toggle-label">{sidebarCollapsed ? "Menu" : "Collapse"}</span>
        </button>
        <p class="eyebrow nav-label">WORKSPACE</p>
        {#snippet navButton(item)}
          <button class:on={section === item.id} aria-current={section === item.id ? "page" : undefined} aria-label={item.label} title={sidebarCollapsed ? item.label : undefined} onclick={() => navigate(item.id)}>
            <span class="nav-icon" aria-hidden="true"><Icon name={item.icon} /></span><span class="nav-text">{item.label}</span>
          </button>
        {/snippet}
        <nav aria-label="Main navigation">
          {#each navigation as item}{@render navButton(item)}{/each}
        </nav>
        <nav class="nav-dock" aria-label="Tools">{@render navButton(dock)}</nav>
        <div class="sidebar-foot"><span class="status-dot"></span><span class="sidebar-foot-label"> HTTP to RESP</span><p>Connected as {username || "default"}</p></div>
      </aside>
      <main class="workspace">
        {#if sessionError}<div class="error-box" role="alert">{sessionError}</div>{/if}
        <div class="screen">
          {#if section === "overview"}
            <Overview shards={overviewNodes} {shard} {openTask} {lines} onData={() => navigate("data")} />
          {:else if section === "data"}
            <header class="page-head"><div><p class="eyebrow">EXPLORE & MANAGE</p><h1>Data</h1><p>Find a key, inspect its value, or manage its lifetime.</p></div><button onclick={() => openTask("strings")}>Write data <span aria-hidden="true">&rarr;</span></button></header>
            {#key scope}<Browser {run} />{/key}
          {:else if section === "tasks"}
            {#key scope}<Tasks {run} initial={task} {scopeLabel} />{/key}
          {:else if section === "users"}
            <header class="page-head"><div><p class="eyebrow">ACCESS CONTROL</p><h1>ACL users</h1><p>Inspect access and update live server users.</p></div><span class="badge">Signed in as {username || "default"}</span></header>
            {#key scope}<Users {run} />{/key}
          {/if}
          <div hidden={section !== "console"}>
            <header class="page-head"><div><p class="eyebrow">RUN COMMANDS</p><h1>Console</h1><p>Type server commands and read the replies.</p></div></header>
            <Console {run} {scopeLabel} endpoint={shard ? nodeId(shard) : `${host}:${port}`} />
          </div>
          {#key scope}
            <div hidden={section !== "pubsub"}>
              <PubSub target={shard ? { host: shard.host, port: shard.port } : null} {note} endpoint={shard ? nodeId(shard) : `${host}:${port}`} bind:busy={pubBusy} bind:database={pubsubDatabase} />
            </div>
          {/key}
        </div>
      </main>
    </div>
    <section class="console" aria-label="Session activity">
      <header>
        <button class="quiet console-toggle" aria-expanded={consoleOpen} aria-label={consoleOpen ? "Collapse session activity" : "Expand session activity"} onclick={() => (consoleOpen = !consoleOpen)}><span aria-hidden="true">{consoleOpen ? "\u2212" : "+"}</span> Session activity <span class="badge">{lines.length} / {logLimit}</span></button>
        <span class="hint">This panel only</span>
        <button class="quiet" onclick={() => (lines = [])}>Clear</button>
      </header>
      {#if consoleOpen}
        <div class="feed">
          {#each lines as line}
            <div class="log-line" class:err={!line.ok}><span class="muted">{line.time}</span><code>{line.command}</code><span>{line.detail}</span><span class="hint">{line.location}{line.ms == null ? "" : ` / ${Math.round(line.ms)} ms`}</span></div>
          {/each}
          {#if !lines.length}<p class="empty">Commands run from this panel will appear here.</p>{/if}
        </div>
      {/if}
    </section>
  </div>
{/if}
