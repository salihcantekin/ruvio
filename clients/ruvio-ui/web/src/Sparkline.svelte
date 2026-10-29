<script>
  // One series over time, drawn as a thin line. Pointer or focus shows the value at that time.
  let { label, points, format, hint = "" } = $props();
  const width = 300;
  const height = 72;
  let active = $state(null);
  let values = $derived(points.filter((point) => point.value != null));
  let range = $derived.by(() => {
    if (!values.length) return null;
    const ys = values.map((point) => point.value);
    const max = Math.max(...ys);
    const min = Math.min(0, ...ys);
    const start = values[0].at;
    const span = Math.max(1, values[values.length - 1].at - start);
    return { max: max === min ? min + 1 : max, min, start, span };
  });
  const x = (point) => ((point.at - range.start) / range.span) * width;
  const y = (point) => height - 4 - ((point.value - range.min) / (range.max - range.min)) * (height - 10);
  let path = $derived(range && values.length > 1 ? values.map((point, index) => `${index ? "L" : "M"}${x(point).toFixed(1)},${y(point).toFixed(1)}`).join("") : "");
  let latest = $derived(values.length ? values[values.length - 1] : null);
  let shown = $derived(active ?? latest);

  function pick(event) {
    if (!range || !values.length) return;
    const box = event.currentTarget.getBoundingClientRect();
    const at = range.start + ((event.clientX - box.left) / box.width) * range.span;
    active = values.reduce((best, point) => (Math.abs(point.at - at) < Math.abs(best.at - at) ? point : best), values[0]);
  }
</script>

<article class="trend">
  <div class="trend-head">
    <p>{label}</p>
    <strong class:unavailable={!shown}>{shown ? format(shown.value) : "Not reported"}</strong>
  </div>
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="trend-plot" onpointermove={pick} onpointerleave={() => (active = null)}>
    {#if path}
      <svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" role="img" aria-label="{label}, last {values.length} readings, latest {format(latest.value)}">
        <line class="trend-base" x1="0" x2={width} y1={height - 4} y2={height - 4} />
        <path class="trend-line" d={path} vector-effect="non-scaling-stroke" />
        {#if active}
          <line class="trend-cursor" x1={x(active)} x2={x(active)} y1="0" y2={height} vector-effect="non-scaling-stroke" />
        {/if}
      </svg>
      {#if active}<span class="trend-dot" style="left: {(x(active) / width) * 100}%; top: {(y(active) / height) * 100}%"></span>{/if}
    {:else}
      <p class="hint trend-wait">{values.length ? "One reading so far" : "Waiting for readings"}</p>
    {/if}
  </div>
  <p class="hint">{active ? new Date(active.at).toLocaleTimeString() : hint}</p>
</article>
