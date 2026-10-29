<script>
  // Line icons on a 24px grid; stroke and size come from CSS (.icon in app.css).
  const paths = {
    overview: "M3 12h4l3-8 4 16 3-8h4",
    data: "M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6",
    tasks: "M9 6h11M9 12h11M9 18h11M3.5 6l1.2 1.2L7 5M3.5 12l1.2 1.2L7 11M3.5 18l1.2 1.2L7 17",
    pubsub: "M4 12h4M16 12h4M12 4v4M12 16v4M12 9.5a2.5 2.5 0 100 5 2.5 2.5 0 000-5z",
    users: "M12 3l7 3v5c0 4.5-3 8-7 10-4-2-7-5.5-7-10V6l7-3zM9.5 12l2 2 3.5-4",
    key: "M14 10a4 4 0 11-1.2-2.8M13 9l8 8M18 14l2 2",
    bolt: "M13 3L5 14h6l-1 7 8-11h-6l1-7z",
    clock: "M12 4a8 8 0 100 16 8 8 0 000-16zM12 8v4l3 2",
    list: "M8 6h12M8 12h12M8 18h12M4 6h.01M4 12h.01M4 18h.01",
    doc: "M7 3h7l4 4v14H7V3zM14 3v4h4M10 12h5M10 16h5",
    lock: "M6 11h12v9H6v-9zM8 11V8a4 4 0 118 0v3",
    layers: "M12 3l9 5-9 5-9-5 9-5zM3 13l9 5 9-5",
    cart: "M3 4h2l2 11h11l2-8H6M9 20h.01M17 20h.01",
    info: "M12 4a8 8 0 100 16 8 8 0 000-16zM12 11v5M12 8h.01",
    terminal: "M4 5h16v14H4V5zM7 10l3 2-3 2M12 15h5",
    expand: "M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5",
    shrink: "M9 4v5H4M15 4v5h5M9 20v-5H4M15 20v-5h5",
    chevronsLeft: "M11 17l-5-5 5-5M18 17l-5-5 5-5",
    chevronsRight: "M13 17l5-5-5-5M6 17l5-5-5-5",
    chevronDown: "M6 9l6 6 6-6",
    chevronRight: "M9 6l6 6-6 6",
    dot: "M12 8a4 4 0 100 8 4 4 0 000-8z",
    chart: "M4 4v16h16M8 15l3-4 3 2 5-6",
    cluster: "M12 3a2.5 2.5 0 100 5 2.5 2.5 0 000-5zM5 16a2.5 2.5 0 100 5 2.5 2.5 0 000-5zM19 16a2.5 2.5 0 100 5 2.5 2.5 0 000-5zM10.8 7.6L6.2 16M13.2 7.6l4.6 8.4M7.5 18.5h9",
  };
  let { name } = $props();
</script>

<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d={paths[name] ?? paths.dot} /></svg>
