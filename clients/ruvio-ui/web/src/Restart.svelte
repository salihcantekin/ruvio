<script>
  import { execute, nodeId } from "./operator.js";
  import { stopCost, shutdownArgs, restartAdvice, waitUntilBack, runIdOf } from "./restart.js";
  import Icon from "./Icon.svelte";
  let { readings, clustered = false } = $props();
  let nodes = $derived(readings.filter((item) => item.reachable));
  let asking = $state(null);
  let understood = $state(false);
  let status = $state({});

  const target = (node) => ({ host: node.host, port: node.port });

  function ask(reading) {
    asking = asking === nodeId(reading.node) ? null : nodeId(reading.node);
    understood = false;
  }

  async function restart(reading) {
    const id = nodeId(reading.node);
    const profile = reading.info?.ruvio_durability;
    asking = null;
    status = { ...status, [id]: { kind: "working", text: "Stopping..." } };
    try {
      await execute(shutdownArgs(profile), target(reading.node));
    } catch (error) {
      status = { ...status, [id]: { kind: "error", text: `Not stopped: ${error.message}` } };
      return;
    }
    status = { ...status, [id]: { kind: "working", text: "Stopped. Waiting for it to answer again..." } };
    const result = await waitUntilBack(async () => runIdOf(await execute(["INFO", "server"], target(reading.node))), { before: reading.info?.run_id });
    status = {
      ...status,
      [id]: result.ok
        ? { kind: "ok", text: `Back after ${result.seconds.toFixed(1)} s.` }
        : { kind: "error", text: result.wentDown ? "It stopped but did not come back in two minutes. A supervisor has to start it." : "It still answers: it may be draining clients (RUVIO_SHUTDOWN_GRACE_MS)." },
    };
  }
</script>

<section class="panel" aria-label="Restart nodes">
  <div class="section-heading"><div><h2>Restart</h2><p class="hint">Stops a node the way <code>SIGTERM</code> does: clients drain, the log is flushed and a final checkpoint is written. A supervisor starts it again. To restart on a schedule, use a systemd timer or cron job that runs <code>ruvio-ctl restart --yes</code>.</p></div></div>
  {#if !nodes.length}
    <p class="hint">No node is answering.</p>
  {:else}
    <ul class="restart-list">
      {#each nodes as reading (nodeId(reading.node))}
        {@const id = nodeId(reading.node)}
        {@const profile = reading.info?.ruvio_durability}
        {@const cost = stopCost(profile)}
        <li>
          <div class="restart-row">
            <span><strong class="mono">{id}</strong> <small>{reading.node.role ?? "node"} &middot; {profile ?? "durability unknown"}</small></span>
            <button type="button" class="danger" disabled={status[id]?.kind === "working"} aria-expanded={asking === id} onclick={() => ask(reading)}>Restart...</button>
          </div>
          {#if asking === id}
            <div class="error-box" role="alertdialog" aria-label="Confirm restart of {id}">
              <p>{cost.text}</p>
              <p class="hint">{restartAdvice(reading.node, clustered)}</p>
              {#if !cost.saves}<label class="check"><input type="checkbox" bind:checked={understood} /> I understand every key on this node is lost.</label>{/if}
              <div class="actions"><button class="danger" type="button" disabled={!cost.saves && !understood} onclick={() => restart(reading)}>Restart {id}</button><button type="button" onclick={() => (asking = null)}>Cancel</button></div>
            </div>
          {/if}
          {#if status[id]}<p class="hint restart-status {status[id].kind}" role="status"><Icon name="info" /> {status[id].text}</p>{/if}
        </li>
      {/each}
    </ul>
  {/if}
</section>

<style>
  .restart-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 0.75rem; }
  .restart-row { display: flex; justify-content: space-between; align-items: center; gap: 1rem; flex-wrap: wrap; }
  .restart-status.error { color: var(--danger, #c0392b); }
  .check { display: flex; gap: 0.5rem; align-items: center; margin: 0.5rem 0; }
</style>
