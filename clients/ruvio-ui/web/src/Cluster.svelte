<script>
  // Read-only picture of the cluster: groups, shards, slots, replicas and, when the UI is given
  // their addresses, the coordinators. Nothing here changes the cluster.
  import { nodeId, metric, number } from "./operator.js";
  import { SLOT_COUNT, groupsOf, replicationRows, slotsCovered, replicas, duration, rangeText, nodeKey } from "./monitor.js";
  import LiveControls from "./LiveControls.svelte";
  import SlotMap from "./SlotMap.svelte";
  let { shards, readings, coordinator, coordinatorsConfigured, failovers, topologyNotice, interval = $bindable(), paused = $bindable(), refreshing, updated, probeError, refresh, reload } = $props();
  let groups = $derived(groupsOf(shards, readings));
  let rows = $derived(replicationRows(shards, readings));
  let byShard = $derived(new Map(rows.map((row) => [row.shard.id, row])));
  let copies = $derived(replicas(readings));
  let covered = $derived(slotsCovered(shards));
  let versions = $derived([...new Set(readings.filter((item) => item.reachable && item.topologyVersion !== null).map((item) => item.topologyVersion))].sort((a, b) => a - b));
  let status = $derived(coordinator?.status ?? null);
  const color = (index) => (index < 8 ? `var(--series-${index + 1})` : "var(--muted)");
  const stateLabel = { ok: "OK", lease_running: "Waiting for the old primary's lease", waiting: "Waiting for a replica with every committed write", no_candidate: "No replica can take over" };
  const coordinatorGroup = (group) => coordinator?.groups?.find((item) => item.name === group.name) ?? null;
  const when = (ms) => (ms ? new Date(ms).toLocaleString() : "Never");

  function copyState(copy) {
    if (!copy.reading) return { text: "Waiting", level: "" };
    if (!copy.reading.reachable) return { text: "Unavailable", level: "err" };
    if (copy.syncing) return { text: "Full copy in progress", level: "warning" };
    if (copy.link === "down") return { text: "Link to primary down", level: "err" };
    if (copy.behind === null) return { text: "Up", level: "" };
    return copy.behind === 0 ? { text: "In sync", level: "" } : { text: `${copy.behind.toLocaleString()} records behind`, level: "warning" };
  }
</script>

<header class="page-head">
  <div><p class="eyebrow">TOPOLOGY</p><h1>Cluster</h1><p>Groups, shards, slots and replicas. Read only.</p></div>
  <div class="actions"><LiveControls bind:interval bind:paused {refreshing} {refresh} ready={shards.length > 0} /><button class="quiet" onclick={reload}>Reload topology</button></div>
</header>
{#if probeError}<div class="error-box" role="alert">{probeError}</div>{/if}
{#if topologyNotice}<p class="notice">{topologyNotice}</p>{/if}

<div class="metrics">
  <article class="metric-card"><p>Groups</p><strong>{groups.length || "--"}</strong><span class="hint">{groups.some((group) => group.name) ? "Named by each node's cluster-group" : "One per server process"}</span></article>
  <article class="metric-card"><p>Shards</p><strong>{shards.length || "--"}</strong><span class="hint">Primaries that own slots</span></article>
  <article class="metric-card"><p>Replicas answering</p><strong>{copies.length ? `${copies.filter((item) => item.reachable).length} / ${copies.length}` : "None"}</strong><span class="hint">{copies.length ? "Replicas that finished copying" : "No replica has finished copying"}</span></article>
  <article class="metric-card"><p>Slots covered</p><strong class:warning={shards.length && covered !== SLOT_COUNT}>{shards.length ? `${covered.toLocaleString()}` : "--"}</strong><span class="hint">of {SLOT_COUNT.toLocaleString()} &middot; map version {versions.length ? versions.join(", ") : "not reported"}</span></article>
</div>
<p class="metrics-caption">{updated ? `Last checked ${updated}` : "First reading in progress"} &middot; Rebalancing, slot moves and failovers are made with the coordinator commands; this screen does not send them.</p>

<section class="panel">
  <div class="section-heading"><div><h2>Hash slots</h2><p class="hint">Each range at its place, colored by group</p></div></div>
  <SlotMap {shards} {groups} />
</section>

<section class="panel">
  <div class="section-heading"><div><h2>Coordinators</h2><p class="hint">Automatic failover and slot moves</p></div>{#if coordinator?.configured}<span class="badge">{coordinator.reachable} of {coordinator.coordinators.length} answering</span>{/if}</div>
  {#if !coordinatorsConfigured}
    <p class="notice">Give the UI process the coordinators' addresses to see the leader, each group's agreed primary and failovers: <code>RUVIO_UI_COORDINATORS=host:port,host:port</code> and <code>RUVIO_UI_COORDINATOR_SECRET</code>. The secret stays in the UI process, and the UI only sends read commands to the coordinators.</p>
  {:else if !coordinator}
    <p class="empty">Reading the coordinators...</p>
  {:else}
    {#if coordinator.error}<div class="error-box" role="alert">{coordinator.error}</div>{/if}
    <div class="coordinator-list">
      {#each coordinator.coordinators as entry}
        <article class="shard-card">
          <div class="section-heading"><strong>{entry.id ? `Coordinator ${entry.id}` : entry.address}</strong>{#if entry.leader}<span class="badge leader">Leader</span>{/if}<span class="health" class:err={!entry.ok}><span class="status-dot" class:down={!entry.ok}></span>{entry.ok ? "Up" : "Unavailable"}</span></div>
          <p class="mono muted">{entry.address}</p>
          {#if entry.ok}<dl><div><dt>Failovers it recorded</dt><dd>{number(entry.failovers)}</dd></div><div><dt>Last one</dt><dd>{when(entry.lastFailover)}</dd></div></dl>{:else}<div class="error-box" role="alert">{entry.error}</div>{/if}
        </article>
      {/each}
    </div>
    {#if !coordinator.leader && coordinator.reachable}<p class="notice">No coordinator reports itself as the leader right now; an election may be running.</p>{/if}
    {#if coordinator.groups.length}
      <div class="distribution" role="table" aria-label="Groups the coordinators watch">
        <div class="distribution-row head coord" role="row"><span role="columnheader">Group</span><span role="columnheader">Agreed primary</span><span role="columnheader">Epoch</span><span role="columnheader">Members answering</span><span role="columnheader">State</span></div>
        {#each coordinator.groups as group}
          <div class="distribution-row coord" role="row">
            <span role="cell" data-label="Group"><strong>{group.name}</strong></span>
            <span role="cell" data-label="Agreed primary" class="mono">{group.primary || "None agreed"}</span>
            <span role="cell" data-label="Epoch">{group.epoch}</span>
            <span role="cell" data-label="Members answering" class:warning={group.answering !== null && group.answering < group.members}>{group.answering ?? "?"} / {group.members ?? "?"}</span>
            <span role="cell" data-label="State" class:warning={group.state && group.state !== "ok"}>{group.state ? stateLabel[group.state] ?? group.state : "Not reported"}{#if group.stateMs != null}<small>for {duration(group.stateMs)}</small>{/if}</span>
          </div>
        {/each}
      </div>
    {/if}
    {#if status}
      <div class="move-status">
        <h3>Slot moves</h3>
        {#if status.state === "moving"}
          <p>Moving slots <strong>{status.first}&ndash;{status.last}</strong> from {status.from} to {status.to} (shard {status.shard}).</p>
        {:else}
          <p>{status.state === "none" ? "No move has run under this leader." : `Last move: ${status.state}${status.first ? `, slots ${status.first}–${status.last} from ${status.from} to ${status.to}` : ""}.`}</p>
        {/if}
        <p class="hint">Ranges moved between groups since the cluster started: {status.moved_ranges ?? "not reported"}{#if status.rebalance} &middot; Rebalance {status.rebalance}: {status.rebalance_moves ?? 0} moves done, {status.rebalance_left ?? 0} left{#if status.rebalance_error} ({status.rebalance_error}){/if}{/if}</p>
      </div>
    {:else if coordinator.statusError}
      <p class="hint">Slot move status unavailable: {coordinator.statusError}</p>
    {/if}
    <div class="move-status">
      <h3>Failovers seen by this panel</h3>
      {#each failovers as event}
        <p><strong>{event.group}</strong>: {event.from || "none"} &rarr; {event.to || "none"} (epoch {event.epoch}) <span class="hint">{new Date(event.at).toLocaleTimeString()}</span></p>
      {:else}
        <p class="hint">None since this tab started watching. Coordinators report only their failover count and the time of the last one.</p>
      {/each}
    </div>
  {/if}
</section>

{#each groups as group}
  {@const watched = coordinatorGroup(group)}
  <section class="panel group-panel">
    <div class="section-heading">
      <div><h2><span class="swatch" style="background: {color(group.index)}"></span>{group.name ? `Group ${group.name}` : group.label}</h2><p class="hint">{group.shards.length} {group.shards.length === 1 ? "shard" : "shards"} &middot; {group.slots.toLocaleString()} slots ({((group.slots / SLOT_COUNT) * 100).toFixed(1)}%)</p></div>
      {#if watched}<span class="badge" class:warning={watched.answering !== null && watched.answering < watched.members}>Epoch {watched.epoch} &middot; {watched.answering ?? "?"} / {watched.members ?? "?"} members answering</span>{/if}
    </div>
    <div class="shard-grid">
      {#each group.shards as shard}
        {@const row = byShard.get(shard.id)}
        {@const reading = row?.primary}
        <article class="shard-card">
          <div class="section-heading"><strong>Shard {shard.id}</strong><span class="health" class:err={reading && !reading.reachable}><span class="status-dot" class:down={reading && !reading.reachable}></span>{!reading ? "Waiting" : reading.reachable ? "Primary up" : "Primary unavailable"}</span></div>
          <p class="mono muted">{nodeId(shard)}</p>
          <dl>
            <div><dt>Slots</dt><dd>{(shard.slots ?? 0).toLocaleString()}</dd></div>
            <div><dt>Ranges</dt><dd>{rangeText(shard, 4)}</dd></div>
            <div><dt>Keys</dt><dd>{number(metric(reading?.info, "keys"))}</dd></div>
            <div><dt>WAL sequence</dt><dd>{number(row?.sequence ?? null)}</dd></div>
            {#if Number(reading?.info?.replication_epoch) > 0}<div><dt>History epoch</dt><dd>{reading.info.replication_epoch}</dd></div>{/if}
          </dl>
          {#if row?.replicas.length}
            <ul class="replica-list">
              {#each row.replicas as copy}
                {@const state = copyState(copy)}
                <li><span class="mono">{nodeKey(copy.node)}</span><span class={state.level}>{state.text}</span>{#if copy.ackLag !== null}<small>acknowledged {copy.ackLag} s ago</small>{/if}</li>
              {/each}
            </ul>
          {:else}
            <p class="hint">No replica has finished copying this shard.</p>
          {/if}
          {#if reading?.error}<div class="error-box" role="alert">{reading.error}</div>{/if}
        </article>
      {/each}
    </div>
  </section>
{/each}
