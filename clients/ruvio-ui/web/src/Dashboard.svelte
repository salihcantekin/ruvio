<script>
  // Time charts of the INFO fields every refresh already reads. Choosing charts, the window or
  // the view only changes what the browser draws; the server sees one INFO per node per refresh.
  import { CHARTS, DEFAULT_CHARTS, chartData, nodeKey } from "./monitor.js";
  import { preference, savePreference } from "./preferences.js";
  import LiveControls from "./LiveControls.svelte";
  import TimeChart from "./TimeChart.svelte";
  let { readings, history, selected, interval = $bindable(), paused = $bindable(), refreshing, updated, probeError, refresh, ready } = $props();
  const known = new Set(CHARTS.map((chart) => chart.id));
  const saved = preference("dashboard.charts", DEFAULT_CHARTS);
  let chosen = $state(Array.isArray(saved) ? saved.filter((id) => known.has(id)) : DEFAULT_CHARTS);
  const windows = [{ minutes: 5, label: "5 min" }, { minutes: 15, label: "15 min" }, { minutes: 30, label: "30 min" }];
  const savedWindow = Number(preference("dashboard.window", 15));
  let minutes = $state(windows.some((item) => item.minutes === savedWindow) ? savedWindow : 15);
  let view = $state(preference("dashboard.view", "total") === "nodes" ? "nodes" : "total");
  let groups = [...new Set(CHARTS.map((chart) => chart.group))];
  let selectedIds = $derived(new Set(selected.map((item) => item.id)));
  let keys = $derived(selected.length ? readings.filter((item) => selectedIds.has(item.node.shard)).map((item) => nodeKey(item.node)) : []);
  let shown = $derived(CHARTS.filter((chart) => chosen.includes(chart.id)));
  let span = $derived(history.length > 1 ? (history[history.length - 1].at - history[0].at) / 60000 : 0);

  $effect(() => {
    savePreference("dashboard.charts", chosen);
    savePreference("dashboard.window", minutes);
    savePreference("dashboard.view", view);
  });

  function toggle(id, on) {
    chosen = on ? CHARTS.map((chart) => chart.id).filter((item) => item === id || chosen.includes(item)) : chosen.filter((item) => item !== id);
  }
</script>

<header class="page-head">
  <div><p class="eyebrow">TIME CHARTS</p><h1>Dashboard</h1><p>Throughput, memory, network and persistence over time.</p></div>
  <LiveControls bind:interval bind:paused {refreshing} {refresh} {ready} />
</header>
{#if probeError}<div class="error-box" role="alert">{probeError}</div>{/if}

<div class="dashboard-bar">
  <details class="scope-dropdown chart-picker">
    <summary>Charts <span class="count">{chosen.length} / {CHARTS.length}</span></summary>
    <div class="scope-menu" role="group" aria-label="Charts to show">
      {#each groups as group}
        <p class="eyebrow">{group.toUpperCase()}</p>
        {#each CHARTS.filter((chart) => chart.group === group) as chart}
          <label><input type="checkbox" checked={chosen.includes(chart.id)} onchange={(event) => toggle(chart.id, event.currentTarget.checked)} /><span>{chart.title}</span></label>
        {/each}
      {/each}
      <div class="actions"><button class="quiet" onclick={() => (chosen = [...DEFAULT_CHARTS])}>Defaults</button><button class="quiet" onclick={() => (chosen = CHARTS.map((chart) => chart.id))}>All</button></div>
    </div>
  </details>
  <label class="refresh-interval">Window<select bind:value={minutes}>{#each windows as item}<option value={item.minutes}>{item.label}</option>{/each}</select></label>
  <div class="segmented" role="group" aria-label="Lines">
    <button class:on={view === "total"} aria-pressed={view === "total"} onclick={() => (view = "total")}>Total</button>
    <button class:on={view === "nodes"} aria-pressed={view === "nodes"} onclick={() => (view = "nodes")}>Per node</button>
  </div>
</div>
<p class="metrics-caption">
  {selected.length ? `Nodes of ${selected.length} selected ${selected.length === 1 ? "shard" : "shards"}` : "Every node"} &middot;
  {view === "nodes" ? "One line per node for each chart's first value" : "Values added up; memory and keys over primaries only"} &middot;
  {updated ? `Last checked ${updated}` : "First reading in progress"} &middot;
  {span >= 1 ? `${Math.floor(span)} min of readings kept in this tab` : "Readings are kept in this tab only, up to 30 min"}
</p>

{#if shown.length}
  <div class="chart-grid">
    {#each shown as chart (chart.id)}
      <TimeChart {chart} data={chartData(chart, history, { keys, view, windowMs: minutes * 60 * 1000 })} onRemove={() => toggle(chart.id, false)} />
    {/each}
  </div>
{:else}
  <div class="empty-state panel"><p>No chart chosen. Open <strong>Charts</strong> to pick what to watch.</p></div>
{/if}
