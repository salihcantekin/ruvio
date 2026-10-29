<script>
  // One Dashboard chart: lines, or stacked areas for parts of a whole, over time. Pointer or
  // keyboard (arrow keys) moves a cursor that reads every series at that moment.
  import { formatValue } from "./monitor.js";
  let { chart, data, onRemove } = $props();
  const height = 190;
  const pad = { left: 62, right: 12, top: 10, bottom: 24 };
  let width = $state(0);
  let cursor = $state(null);
  const color = (index) => `var(--series-${index + 1})`;
  const perSecond = $derived(chart.kind === "rate");
  const show = (value) => formatValue(chart.unit, value, perSecond);
  let times = $derived(data.series[0]?.points.map((point) => point.at) ?? []);
  // For stacked charts each series sits on top of the ones before it.
  let layers = $derived.by(() => {
    if (!data.stacked) return data.series.map((series) => series.points.map((point) => ({ low: null, high: point.value })));
    const base = times.map(() => 0);
    return data.series.map((series) => series.points.map((point, index) => {
      if (point.value == null || base[index] == null) {
        base[index] = null;
        return { low: null, high: null };
      }
      const low = base[index];
      base[index] = low + point.value;
      return { low, high: base[index] };
    }));
  });
  let top = $derived.by(() => {
    const highs = layers.flat().map((item) => item.high).filter((value) => value != null);
    const max = highs.length ? Math.max(...highs) : 0;
    if (max <= 0) return 1;
    // Byte axes round in binary units (64 MiB, not 67.1 MB), counts in decimal ones.
    const base = chart.unit === "bytes" ? 1024 ** Math.max(0, Math.floor(Math.log(max) / Math.log(1024))) : 1;
    const scaled = max / base;
    const step = 10 ** Math.floor(Math.log10(scaled));
    return [1, 2, 2.5, 5, 10].map((factor) => factor * step * base).find((value) => value >= max);
  });
  let start = $derived(times[0] ?? 0);
  let span = $derived(Math.max(1, (times[times.length - 1] ?? 0) - start));
  let plotWidth = $derived(Math.max(10, width - pad.left - pad.right));
  const plotHeight = height - pad.top - pad.bottom;
  const x = (at) => pad.left + ((at - start) / span) * plotWidth;
  const y = (value) => pad.top + plotHeight - (value / top) * plotHeight;

  function segments(points) {
    // Breaks the path where a value is missing rather than drawing it as zero.
    const runs = [];
    let run = [];
    points.forEach((point, index) => {
      if (point == null) {
        if (run.length) runs.push(run);
        run = [];
      } else run.push(index);
    });
    if (run.length) runs.push(run);
    return runs;
  }
  function linePath(layer) {
    return segments(layer.map((item) => item.high)).map((run) => run.map((index, step) => `${step ? "L" : "M"}${x(times[index]).toFixed(1)},${y(layer[index].high).toFixed(1)}`).join("")).join("");
  }
  function areaPath(layer) {
    return segments(layer.map((item) => item.high)).map((run) => {
      const upper = run.map((index, step) => `${step ? "L" : "M"}${x(times[index]).toFixed(1)},${y(layer[index].high).toFixed(1)}`).join("");
      const lower = [...run].reverse().map((index) => `L${x(times[index]).toFixed(1)},${y(layer[index].low).toFixed(1)}`).join("");
      return `${upper}${lower}Z`;
    }).join("");
  }
  function pick(event) {
    if (!times.length) return;
    const box = event.currentTarget.getBoundingClientRect();
    const at = start + ((event.clientX - box.left - pad.left) / plotWidth) * span;
    let best = 0;
    times.forEach((time, index) => { if (Math.abs(time - at) < Math.abs(times[best] - at)) best = index; });
    cursor = best;
  }
  function key(event) {
    if (!times.length) return;
    if (event.key === "ArrowLeft") cursor = Math.max(0, (cursor ?? times.length) - 1);
    else if (event.key === "ArrowRight") cursor = Math.min(times.length - 1, (cursor ?? -1) + 1);
    else if (event.key === "Escape") cursor = null;
    else return;
    event.preventDefault();
  }
  let latestIndex = $derived(times.length - 1);
  let readIndex = $derived(cursor ?? latestIndex);
  let hasData = $derived(data.series.some((series) => series.points.some((point) => point.value != null)));
  const clock = (at) => new Date(at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
</script>

<article class="chart-card">
  <header class="chart-head">
    <div><h3>{chart.title}</h3>{#if chart.hint}<p class="hint">{chart.hint}</p>{/if}</div>
    {#if data.series.length === 1}<strong class="chart-now">{hasData ? show(data.series[0].points[readIndex]?.value) : ""}</strong>{/if}
    {#if onRemove}<button class="quiet chart-remove" aria-label="Hide {chart.title}" title="Hide this chart" onclick={onRemove}>&times;</button>{/if}
  </header>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
  <div class="chart-plot" bind:clientWidth={width} tabindex="0" role="img" aria-label="{chart.title}{hasData ? `, latest ${data.series.map((series) => `${series.label} ${show(series.points[latestIndex]?.value)}`).join(", ")}` : ", waiting for readings"}. Use the arrow keys to read earlier values." onpointermove={pick} onpointerleave={() => (cursor = null)} onkeydown={key} onblur={() => (cursor = null)}>
    {#if width && hasData && times.length > 1}
      <svg {width} {height} aria-hidden="true">
        {#each [0, 0.5, 1] as fraction}
          <line class="chart-grid" x1={pad.left} x2={width - pad.right} y1={y(top * fraction)} y2={y(top * fraction)} />
          <text class="chart-label" x={pad.left - 8} y={y(top * fraction) + 4} text-anchor="end">{formatValue(chart.unit, top * fraction)}</text>
        {/each}
        {#each [0, 0.5, 1] as fraction, index}
          <text class="chart-label" x={pad.left + plotWidth * fraction} y={height - 6} text-anchor={["start", "middle", "end"][index]}>{clock(start + span * fraction)}</text>
        {/each}
        {#each layers as layer, index}
          {#if data.stacked}<path class="chart-area" d={areaPath(layer)} fill={color(index)} />{/if}
          <path class="chart-line" d={linePath(layer)} stroke={color(index)} />
        {/each}
        {#if cursor != null}
          <line class="chart-cursor" x1={x(times[cursor])} x2={x(times[cursor])} y1={pad.top} y2={pad.top + plotHeight} />
          {#each layers as layer, index}
            {#if layer[cursor]?.high != null}<circle class="chart-dot" cx={x(times[cursor])} cy={y(layer[cursor].high)} r="4" fill={color(index)} />{/if}
          {/each}
        {/if}
      </svg>
      {#if cursor != null}
        <div class="chart-tip" style="left: {Math.min(Math.max(x(times[cursor]), 90), width - 90)}px">
          <strong>{clock(times[cursor])}</strong>
          {#each [...data.series].reverse() as series, reversed}
            {@const index = data.series.length - 1 - reversed}
            <span><i style="background: {color(index)}"></i>{series.label}<b>{show(series.points[cursor]?.value)}</b></span>
          {/each}
        </div>
      {/if}
    {:else}
      <p class="hint chart-wait">{hasData ? "Collecting readings: the line starts after the second refresh." : chart.kind === "rate" ? "Waiting for two readings to work out a rate." : "No node reports this yet."}</p>
    {/if}
  </div>
  {#if data.series.length > 1}
    <ul class="chart-legend">
      {#each data.series as series, index}
        <li><i style="background: {color(index)}"></i>{series.label}<b>{show(series.points[readIndex]?.value)}</b></li>
      {/each}
    </ul>
  {/if}
</article>
