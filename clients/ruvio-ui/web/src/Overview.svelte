<script>
  import { nodeId, metric, number, bytes } from "./operator.js";
  import { primaries, replicas, sum, notices, groupsOf, replicationRows, rangeText } from "./monitor.js";
  import { taskFamilies } from "./catalog.js";
  import Icon from "./Icon.svelte";
  import LiveControls from "./LiveControls.svelte";
  import SlotMap from "./SlotMap.svelte";
  import { preference, savePreference } from "./preferences.js";
  const taskIcons = { locking: "lock", conditional: "layers", limiting: "clock", idempotency: "key", inventory: "cart", sessions: "users", jobs: "tasks", changes: "list", json: "doc", strings: "doc", keys: "key", lists: "list" };
  let { shards, selected, readings, openTask, onData, onCluster, onDashboard, lines, interval = $bindable(), paused = $bindable(), refreshing, updated, probeError, refresh } = $props();
  let shardsOpen = $state(preference("overview.shardsOpen", false) === true);
  let selectedIds = $derived(new Set(selected.map((item) => item.id)));
  let visible = $derived(selected.length ? readings.filter((item) => selectedIds.has(item.node.shard)) : readings);
  let visibleShards = $derived(selected.length ? shards.filter((item) => selectedIds.has(item.id)) : shards);
  let owners = $derived(primaries(visible));
  let copies = $derived(replicas(visible));
  let up = $derived(owners.filter((item) => item.reachable).length);
  let commands = $derived(sum(visible, "total_commands_processed"));
  let alerts = $derived(notices(visibleShards, visible));
  let groups = $derived(groupsOf(shards, readings));
  let rows = $derived(replicationRows(visibleShards, readings));
  let distribution = $derived(rows.map((row) => ({ ...row, keys: metric(row.primary?.info, "keys"), memory: metric(row.primary?.info, "charged_memory_bytes") })));
  let maxKeys = $derived(Math.max(1, ...distribution.map((row) => row.keys ?? 0)));
  let maxMemory = $derived(Math.max(1, ...distribution.map((row) => row.memory ?? 0)));
  let commandGroups = [
    { label: "Reads", field: "get_commands" },
    { label: "Writes", field: "set_commands" },
    { label: "Increments", field: "increment_commands" },
    { label: "Other", field: "other_commands" },
  ];
  const share = (value, max) => `${value == null ? 0 : Math.max(2, (value / max) * 100)}%`;
</script>

<header class="page-head">
  <div><p class="eyebrow">LIVE PICTURE</p><h1>Overview</h1><p>Your Ruvio, at a glance.</p></div>
  <LiveControls bind:interval bind:paused {refreshing} {refresh} ready={shards.length > 0} />
</header>
{#if probeError}<div class="error-box" role="alert">{probeError}</div>{/if}
<div class="metrics">
  <article class="metric-card"><p>Shards responding</p><strong>{owners.length ? `${up} / ${owners.length}` : "--"}</strong><span class="hint">{!owners.length ? "Waiting for shard discovery" : copies.length ? `Replicas: ${copies.filter((item) => item.reachable).length} / ${copies.length} answering` : up === owners.length ? "All discovered shards reachable" : "Some shards need attention"}</span></article>
  <article class="metric-card"><p>Keys</p><strong class:unavailable={sum(owners, "keys") === null}>{number(sum(owners, "keys"))}</strong><button class="text-button" onclick={onData}>Explore data &rarr;</button></article>
  <article class="metric-card"><p>Charged memory</p><strong class:unavailable={sum(owners, "charged_memory_bytes") === null}>{bytes(sum(owners, "charged_memory_bytes"))}</strong><span class="hint">Store memory of the primaries, not process RSS</span></article>
  <article class="metric-card"><p>Commands processed</p><strong class:unavailable={commands === null}>{number(commands)}</strong><span class="hint">Server lifetime, primaries and replicas</span></article>
</div>
<p class="metrics-caption">{selected.length ? `Metrics for ${selected.map(nodeId).join(", ")}` : "Keys and memory summed over primaries; replicas hold copies"} &middot; {updated ? `Last checked ${updated}` : "First reading in progress"} &middot; Missing metrics are never counted as zero.</p>

{#if alerts.length}
  <section class="panel notices" aria-label="Needs attention">
    <div class="section-heading"><h2>Needs attention</h2><span class="badge">{alerts.length}</span></div>
    <ul>{#each alerts as alert}<li class={alert.level}><Icon name="info" /><span>{alert.text}</span></li>{/each}</ul>
  </section>
{/if}

<button class="panel dashboard-link" onclick={onDashboard}><span class="quick-icon"><Icon name="chart" /></span><span><strong>Dashboard</strong><span class="hint">Time charts of throughput, memory breakdown, network bytes, persistence and replication. Pick the charts you want.</span></span><span aria-hidden="true">&rarr;</span></button>

<section class="panel">
  <div class="section-heading"><div><h2>Slots and data</h2><p class="hint">Who owns each hash slot, and how keys and memory are spread</p></div>{#if onCluster}<button class="text-button" onclick={onCluster}>Cluster view &rarr;</button>{/if}</div>
  <SlotMap {shards} {groups} />
  <div class="distribution" class:single={!onCluster} role="table" aria-label="Keys and memory by shard">
    <div class="distribution-row head" role="row"><span role="columnheader">Shard</span><span role="columnheader">Slots</span><span role="columnheader">Keys</span><span role="columnheader">Memory</span>{#if onCluster}<span role="columnheader">Replicas</span>{/if}</div>
    {#each distribution as row}
      <div class="distribution-row" role="row">
        <span role="cell" data-label="Shard"><strong>Shard {row.shard.id}</strong><small class="mono">{nodeId(row.shard)}</small></span>
        <span role="cell" data-label="Slots">{(row.shard.slots ?? 0).toLocaleString()}<small>{rangeText(row.shard)}</small></span>
        <span role="cell" data-label="Keys" class="bar-cell"><span class="bar" style="width: {share(row.keys, maxKeys)}"></span>{number(row.keys)}</span>
        <span role="cell" data-label="Memory" class="bar-cell"><span class="bar" style="width: {share(row.memory, maxMemory)}"></span>{bytes(row.memory)}</span>
        {#if onCluster}<span role="cell" data-label="Replicas">{row.replicas.length ? `${row.replicas.filter((item) => item.reading?.reachable).length} / ${row.replicas.length}` : "None"}{#if row.replicas.some((item) => item.behind)}<small class="warning">{Math.max(...row.replicas.map((item) => item.behind ?? 0)).toLocaleString()} behind</small>{/if}</span>{/if}
      </div>
    {/each}
  </div>
</section>

<section class="panel">
  <div class="section-heading">
    <div class="shard-head">
      <h2><button class="shard-toggle" aria-expanded={shardsOpen} aria-controls="shard-grid" onclick={() => { shardsOpen = !shardsOpen; savePreference("overview.shardsOpen", shardsOpen); }}>
        <Icon name={shardsOpen ? "chevronDown" : "chevronRight"} />Shard health
      </button></h2>
      <p class="hint">Availability, storage, and persistence by owner</p>
    </div>
    <span class="badge">{#if owners.length}{up} of {owners.length} up{:else}{visibleShards.length} discovered{/if}</span>
  </div>
  <div class="shard-grid" id="shard-grid" hidden={!shardsOpen}>
    {#each rows as row}
      {@const reading = row.primary}
      {@const info = reading?.info}
      {@const unhealthy = info && (info.ruvio_persistence_state === "error" || info.replication_fenced === "1" || (info.ruvio_readiness && info.ruvio_readiness !== "ready") || (info.ruvio_health && info.ruvio_health !== "ok"))}
      <article class="shard-card">
        <div class="section-heading"><strong>Shard {row.shard.id}</strong><span class="health" class:err={!reading?.reachable || unhealthy} class:warning={reading?.reachable && !info}><span class="status-dot" class:down={!reading?.reachable || unhealthy}></span>{!reading ? "Waiting" : !reading.reachable ? "Unavailable" : unhealthy ? "Needs attention" : !info ? "Reachable / no metrics" : "Up"}</span></div>
        <p class="mono muted">{nodeId(row.shard)}</p>
        <dl><div><dt>Keys</dt><dd>{number(metric(info, "keys"))}</dd></div><div><dt>Memory</dt><dd>{bytes(metric(info, "charged_memory_bytes"))}</dd></div><div><dt>Persistence</dt><dd>{info?.ruvio_persistence_state || "Not reported"}</dd></div><div><dt>Durability</dt><dd>{info?.ruvio_durability || "Not reported"}</dd></div></dl>
        <dl>
          {#each [{ label: "Connected clients", field: "connected_clients" }, { label: "Expiring keys", field: "expires" }, { label: "Expired keys", field: "expired_keys" }, { label: "Evicted keys", field: "evicted_keys" }, { label: "WAL sequence", field: "wal_last_sequence" }] as item}
            {#if metric(info, item.field) !== null}<div><dt>{item.label}</dt><dd>{number(metric(info, item.field))}</dd></div>{/if}
          {/each}
          {#if metric(info, "wal_unsynced_bytes") !== null}<div><dt>Unsynced WAL</dt><dd>{bytes(metric(info, "wal_unsynced_bytes"))}</dd></div>{/if}
          {#if info?.replication_role}<div><dt>Replication</dt><dd>{info.replication_role}</dd></div>{/if}
        </dl>
        {#if row.replicas.length}
          <dl>
            {#each row.replicas as copy}
              <div><dt class="mono">{nodeId(copy.node)}</dt><dd>{!copy.reading ? "Waiting" : !copy.reading.reachable ? "Unavailable" : copy.syncing ? "Full copy" : copy.link === "down" ? "Link down" : copy.behind === null ? "Up" : copy.behind === 0 ? "In sync" : `${copy.behind.toLocaleString()} behind`}</dd></div>
            {/each}
          </dl>
        {/if}
        <dl>
          {#each [{ label: "Key bytes", field: "key_memory_bytes" }, { label: "Value bytes", field: "value_memory_bytes" }, { label: "Collections", field: "collection_memory_bytes" }, { label: "Index capacity", field: "index_memory_bytes" }, { label: "Reusable buffers", field: "pooled_value_bytes" }] as item}
            {#if metric(info, item.field) !== null}<div><dt>{item.label}</dt><dd>{bytes(metric(info, item.field))}</dd></div>{/if}
          {/each}
        </dl>
        <p class="hint">Slots {rangeText(row.shard, 3)}{reading?.ms == null ? "" : ` / INFO ${Math.round(reading.ms)} ms`}</p>
        {#if reading?.error}<div class="error-box" role="alert">{reading.error}</div>{/if}
      </article>
    {/each}
    {#if !readings.length}<p class="empty">{refreshing ? "Reading each shard..." : "No readings yet. Discover shards or refresh to retry."}</p>{/if}
  </div>
</section>

<section class="quick-section">
  <div class="section-heading"><div><h2>What do you need to do?</h2><p class="hint">Purpose-built forms. Real server commands.</p></div><button class="text-button" onclick={() => openTask("strings")}>All tasks &rarr;</button></div>
  <div class="quick-grid">
    {#each taskFamilies.slice(0, 8) as item}
      <button class="quick-card" onclick={() => openTask(item.id)}><span class="quick-icon"><Icon name={taskIcons[item.id] ?? "bolt"} /></span><strong>{item.label}</strong><span>{item.description}</span><code>{item.commands[0].name}{item.id === "locking" ? " / RELEASE" : ""}</code><span class="quick-arrow" aria-hidden="true">&nearr;</span></button>
    {/each}
  </div>
</section>
<div class="overview-bottom">
  <section class="panel"><div class="section-heading"><h2>Command activity</h2><span class="hint">Server counters</span></div>
    {#each commandGroups as group}
      {@const value = sum(visible, group.field)}
      <div class="counter-row"><span>{group.label}</span><strong>{number(value)}</strong></div>
    {/each}
    <p class="hint">Counts include other clients and this panel's reads. GET, SET and INCR have separate counters; other commands are grouped together.</p>
  </section>
  <section class="panel"><div class="section-heading"><h2>Recent panel activity</h2><span class="hint">This session</span></div>
    {#each lines.slice(0, 5) as line}
      <div class="activity-row"><span class="status-dot" class:down={!line.ok}></span><code>{line.command}</code><span class="hint">{line.time}</span></div>
    {/each}
    {#if !lines.length}<p class="empty">Your actions will appear here.</p>{/if}
    <p class="hint">Open Session activity below for replies and errors.</p>
  </section>
</div>
