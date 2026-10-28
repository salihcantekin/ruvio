<script>
  import { applyTheme, nextMode, setMode, storedMode } from "./theme.js";

  let mode = $state(storedMode());
  const label = { auto: "system", light: "light", dark: "dark" };

  $effect(() => {
    const query = matchMedia("(prefers-color-scheme: light)");
    const follow = () => mode === "auto" && applyTheme("auto");
    query.addEventListener("change", follow);
    return () => query.removeEventListener("change", follow);
  });

  function cycle() {
    mode = nextMode(mode);
    setMode(mode);
  }
</script>

<button class="quiet theme-toggle" type="button" title={`Theme: ${label[mode]}`} aria-label={`Theme: ${label[mode]}. Switch to ${label[nextMode(mode)]}`} onclick={cycle}>
  <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
    {#if mode === "light"}
      <circle cx="12" cy="12" r="4" /><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    {:else if mode === "dark"}
      <path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z" />
    {:else}
      <rect x="3" y="4" width="18" height="12" rx="2" /><path d="M8 20h8M12 16v4" />
    {/if}
  </svg>
</button>
