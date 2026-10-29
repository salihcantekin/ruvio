<script>
  // Refresh interval, pause and manual refresh for the live readings (Overview and Cluster).
  let { interval = $bindable(), paused = $bindable(), refreshing, refresh, ready } = $props();
</script>

<div class="actions"><label class="refresh-interval">Refresh interval<select bind:value={interval}>{#each [2, 5, 10, 30, 60] as seconds}<option value={seconds}>{seconds}s</option>{/each}</select></label><span class="live-label"><span class="status-dot" class:idle={paused}></span>{paused ? "Paused" : `Live / ${interval}s`}</span><button class="quiet" onclick={() => (paused = !paused)}>{paused ? "Resume" : "Pause"}</button><button disabled={refreshing || !ready} onclick={refresh}>{refreshing ? "Refreshing..." : "Refresh"}</button></div>
