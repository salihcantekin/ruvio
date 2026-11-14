// What stopping a node costs, per durability profile, and the commands a restart sends.

const COSTS = {
  memory: { saves: false, text: "Durability memory: nothing is saved. Every key on this node is lost." },
  snapshot: { saves: true, text: "Durability snapshot: a final checkpoint is written when it stops. A crash would lose writes after the last SAVE." },
  everysec: { saves: true, text: "Durability everysec: the log is flushed and a final checkpoint written when it stops. A crash would lose up to one sync window." },
  always: { saves: true, text: "Durability always: every acknowledged write is on disk." },
};

const UNKNOWN = { saves: true, text: "The durability profile is unknown. Check what the node saves before stopping it." };

/** { saves, text } for a profile name such as `everysec`. */
export function stopCost(profile) {
  return COSTS[String(profile ?? "").toLowerCase()] ?? UNKNOWN;
}

/** The command that restarts a node: under `memory` the server wants NOSAVE as the confirmation. */
export function shutdownArgs(profile) {
  return stopCost(profile).saves ? ["SHUTDOWN"] : ["SHUTDOWN", "NOSAVE"];
}

/** A line that tells an operator what else to consider before stopping this node. */
export function restartAdvice(node, clustered) {
  if (!clustered) return "Clients are refused until the node is back; it has to be started again by its supervisor (systemd, Docker, Kubernetes).";
  if (node?.role === "primary") return "This is a primary: writes pause until a replica takes over or it is back. With replicas, switch over first (see Upgrading in the cluster guide).";
  return "A replica: reads it served move to other nodes while it is away, and it catches up from its log when it returns.";
}

/**
 * Poll `probe` until the node answers again; resolves with { ok, seconds }. `probe` resolves with
 * the node's run id. The node is back once it answers after a failed probe or with another run id
 * than `before`: a restart quicker than the polling interval fails no probe at all.
 */
export async function waitUntilBack(probe, { before, timeoutMs = 120000, everyMs = 1000, now = () => Date.now(), sleep = (ms) => new Promise((done) => setTimeout(done, ms)) } = {}) {
  const started = now();
  let wentDown = false;
  while (now() - started < timeoutMs) {
    let id = null;
    try {
      id = await probe();
    } catch {
      wentDown = true;
    }
    if (id !== null && (wentDown || (before != null && id !== before))) return { ok: true, seconds: (now() - started) / 1000 };
    await sleep(everyMs);
  }
  return { ok: false, seconds: (now() - started) / 1000, wentDown };
}

/** The `run_id:` line of an `INFO` reply value ({ kind: "bulk", text }). */
export function runIdOf(value) {
  const match = /^run_id:(.*)$/m.exec(value?.text ?? "");
  return match ? match[1].trim() : "";
}
