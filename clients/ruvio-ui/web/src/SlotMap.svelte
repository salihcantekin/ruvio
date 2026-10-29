<script>
  // The 16,384 hash slots as one strip: each range drawn at its place, colored by the group
  // (or process) that serves it, with a gap between neighbouring ranges.
  import { SLOT_COUNT, nodeKey } from "./monitor.js";
  let { shards, groups } = $props();
  let active = $state(null);
  const color = (index) => (index < 8 ? `var(--series-${index + 1})` : "var(--muted)");
  let segments = $derived(groups.flatMap((group) => group.shards.flatMap((shard) =>
    (shard.ranges ?? []).map(([start, end]) => ({ start, end, shard, group })))).sort((a, b) => a.start - b.start));
  const ticks = [0, 4096, 8192, 12288, 16383];
</script>

<div class="slot-map">
  <div class="slot-strip" role="group" aria-label="Hash slot owners">
    {#each segments as segment}
      <button
        type="button"
        class="slot-segment"
        style="left: {(segment.start / SLOT_COUNT) * 100}%; width: {((segment.end - segment.start + 1) / SLOT_COUNT) * 100}%; background: {color(segment.group.index)}"
        aria-label="Slots {segment.start} to {segment.end}: shard {segment.shard.id}, {segment.group.label}, {nodeKey(segment.shard)}"
        onpointerenter={() => (active = segment)}
        onpointerleave={() => (active = null)}
        onfocus={() => (active = segment)}
        onblur={() => (active = null)}
      ></button>
    {/each}
  </div>
  <div class="slot-ticks" aria-hidden="true">{#each ticks as tick}<span style="left: {(tick / (SLOT_COUNT - 1)) * 100}%">{tick.toLocaleString()}</span>{/each}</div>
  <p class="slot-detail hint" aria-live="polite">
    {#if active}<strong>Slots {active.start.toLocaleString()}&ndash;{active.end.toLocaleString()}</strong> &middot; shard {active.shard.id} &middot; {active.group.label} &middot; <span class="mono">{nodeKey(active.shard)}</span>{:else}Point at a range to see its owner.{/if}
  </p>
  <ul class="slot-legend">
    {#each groups as group}
      <li><span class="swatch" style="background: {color(group.index)}"></span>{group.label}<span class="hint">{group.shards.length} {group.shards.length === 1 ? "shard" : "shards"} &middot; {group.slots.toLocaleString()} slots</span></li>
    {/each}
  </ul>
</div>
