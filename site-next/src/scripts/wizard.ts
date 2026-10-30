// The setup wizard page: reads the form, runs the engine (src/lib/wizard.mjs) and shows the files.
// Everything stays in this tab: the answers go into the address (#...) so a link reproduces them,
// the passwords never do.
import * as wizard from "../lib/wizard.mjs";

type Answers = ReturnType<typeof wizard.defaultAnswers>;
type Plan = ReturnType<typeof wizard.plan>;
type Secrets = { password: string; replicationPassword: string; coordinatorSecret: string };

function start(root: HTMLElement) {
  const form = root.querySelector<HTMLFormElement>("[data-questions]")!;
  const version = root.dataset.version ?? "latest";
  const $ = <T extends Element>(selector: string) => root.querySelector<T>(selector)!;
  const newSecrets = (): Secrets => ({
    password: wizard.randomSecret(),
    replicationPassword: wizard.randomSecret(),
    coordinatorSecret: wizard.randomSecret(),
  });
  let secrets = newSecrets();

  const radio = (name: string) => form.querySelector<HTMLInputElement>(`input[name="${name}"]:checked`)?.value;
  const field = <T extends HTMLInputElement | HTMLTextAreaElement = HTMLInputElement>(name: string) =>
    form.querySelector<T>(`[name="${name}"]`)!;

  function read(): Partial<Answers> {
    return {
      topology: radio("topology") as Answers["topology"],
      shards: Number(radio("shards")),
      groups: Number(radio("groups")),
      replicas: Number(radio("replicas")),
      coordinators: Number(radio("coordinators")),
      durability: radio("durability") as Answers["durability"],
      usage: radio("usage") as Answers["usage"],
      maxmemoryMb: Number(field("maxmemoryMb").value || 0),
      placement: radio("placement") as Answers["placement"],
      access: radio("access") as Answers["access"],
      host: field("host").value,
      hosts: field<HTMLTextAreaElement>("hosts").value.split(/\s+/).filter(Boolean),
      password: field("password").checked,
      safeWrites: field("safeWrites").checked,
      ui: field("ui").checked,
      format: radio("format") as Answers["format"],
    };
  }

  function write(a: Answers) {
    for (const name of ["topology", "shards", "groups", "replicas", "coordinators", "durability", "usage", "placement", "access", "format"]) {
      const value = String(a[name as keyof Answers]);
      for (const input of form.querySelectorAll<HTMLInputElement>(`input[name="${name}"]`)) input.checked = input.value === value;
    }
    if (document.activeElement !== field("maxmemoryMb")) field("maxmemoryMb").value = String(a.maxmemoryMb);
    field("password").checked = a.password;
    field("safeWrites").checked = a.safeWrites;
    field("ui").checked = a.ui;
  }

  function fromHash(): Partial<Answers> {
    const params = new URLSearchParams(location.hash.slice(1));
    const out: Record<string, unknown> = {};
    for (const [key, value] of params) {
      if (key === "hosts") out.hosts = value.split(",").filter(Boolean);
      else if (["password", "safeWrites", "ui"].includes(key)) out[key] = value === "1";
      else if (["shards", "groups", "replicas", "coordinators", "maxmemoryMb"].includes(key)) out[key] = Number(value);
      else out[key] = value;
    }
    return out as Partial<Answers>;
  }

  function toHash(a: Answers) {
    const d = wizard.defaultAnswers() as Record<string, unknown>;
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(a)) {
      const text = Array.isArray(value) ? value.join(",") : typeof value === "boolean" ? (value ? "1" : "0") : String(value);
      const base = Array.isArray(d[key]) ? (d[key] as string[]).join(",") : typeof d[key] === "boolean" ? (d[key] ? "1" : "0") : String(d[key]);
      if (text !== base) params.set(key, text);
    }
    const hash = params.toString();
    history.replaceState(null, "", hash ? `#${hash}` : location.pathname);
  }

  /** Shows the questions and options that apply to these answers. */
  function arrange(a: Answers) {
    for (const el of root.querySelectorAll<HTMLElement>("[data-show]")) {
      const show = el.dataset.show!.split(" ");
      let visible = show.includes(a.topology);
      if (show.includes("access")) visible = a.placement === "one";
      el.hidden = !visible;
    }
    const replicated = a.topology !== "single";
    for (const input of form.querySelectorAll<HTMLInputElement>('input[name="durability"]')) {
      input.disabled = replicated && (input.value === "memory" || input.value === "snapshot");
    }
    for (const input of form.querySelectorAll<HTMLInputElement>('input[name="format"]')) {
      input.disabled = input.value === "docker-run" && replicated;
    }
    for (const input of form.querySelectorAll<HTMLInputElement>('input[name="coordinators"]')) {
      input.disabled = input.value === "5" && a.placement === "many" && nodeCount(a) < 5;
    }
    let number = 0;
    for (const q of root.querySelectorAll<HTMLElement>("fieldset[data-question]")) {
      if (!q.hidden) q.querySelector(".q-no")!.textContent = String(++number).padStart(2, "0");
    }
    $<HTMLElement>('[data-field="hosts"]').hidden = a.placement !== "many";
    $<HTMLElement>('[data-field="host"]').hidden = !(a.placement === "one" && a.access === "network");
    $<HTMLElement>("[data-hosts-count]").textContent = `${nodeCount(a)} needed`;
  }

  const nodeCount = (a: Answers) => (a.topology === "single" ? 1 : (1 + a.replicas) * (a.topology === "cluster" ? a.groups : 1));

  function update() {
    const { answers, notes } = wizard.normalize(read());
    write(answers);
    arrange(answers);
    toHash(answers);
    const plan = wizard.plan(answers, { version });
    showSummary(plan, notes);
    showFiles(plan);
    showVerify(plan);
  }

  function showSummary(p: Plan, notes: string[]) {
    const a = p.answers;
    const items: string[] = [];
    const ports = (n: Plan["nodes"][number]) => (a.shards > 1 ? `${n.port}-${n.port + a.shards - 1}` : `${n.port}`);
    const first = p.nodes[0];
    if (a.topology === "single") {
      items.push(`One Ruvio server with ${a.shards} shard${a.shards > 1 ? "s" : ""} on port${a.shards > 1 ? "s" : ""} ${ports(first)}.`);
    } else if (a.topology === "cluster") {
      items.push(`${a.groups} groups of ${1 + a.replicas} nodes (${p.nodes.length} data nodes, ${a.shards} shard${a.shards > 1 ? "s" : ""} each); each group owns ${Math.floor(wizard.SLOTS / a.groups).toLocaleString("en")} hash slots.`);
    } else {
      items.push(`A primary (${first.name}) and ${a.replicas} replica${a.replicas > 1 ? "s" : ""}, ${a.shards} shard${a.shards > 1 ? "s" : ""} each; replicas copy the write-ahead log and serve reads.`);
    }
    if (p.coordinators.length) {
      items.push(`${p.coordinators.length} coordinators promote a replica about 4 s after a primary stops answering (measured 3.6-3.8 s in Docker) and point the others at it.`);
    } else if (a.topology === "replicas") {
      items.push("If the primary is lost you promote a replica with REPLICAOF NO ONE; pick Automatic failover to have it done for you.");
    }
    const loss: Record<string, string> = {
      memory: "A restart starts empty: nothing is written to disk.",
      snapshot: "A crash loses the writes since the last snapshot (every 60 s).",
      everysec: "A crash loses about the last second of writes.",
      always: "A crash loses no acknowledged write; each write waits for the disk.",
    };
    items.push(loss[a.durability]);
    if (a.topology !== "single") {
      items.push(
        a.safeWrites
          ? "A primary cut off from its replicas stops taking writes after 10 s. For writes that must survive a failover, the client sends CLIENT DURABILITY REPLICAS 1."
          : "Replication is asynchronous: a failover can lose the last writes the old primary acknowledged, as in Redis.",
      );
    }
    if (a.usage === "cache") items.push(`Least recently used keys are evicted above ${a.maxmemoryMb.toLocaleString("en")} MiB.`);
    else if (a.maxmemoryMb > 0) items.push(`Writes are refused above ${a.maxmemoryMb.toLocaleString("en")} MiB; nothing is evicted.`);
    items.push(
      a.password
        ? "Passwords were generated in this browser and are only in the files below. Keep the .env file or the script private."
        : "No password: the ports are published on 127.0.0.1 only. Turn on Password before anything else can reach it.",
    );
    if (a.ui) items.push("The Operator UI opens on http://127.0.0.1:8080.");
    if (p.many) {
      const span = (port: number) => (a.shards > 1 ? `${port}-${port + a.shards - 1}` : `${port}`);
      items.push(
        `One file per machine: ${p.machines.join(", ")}. Open ports ${span(6379)} (clients) and ${span(26379)} (replication)${p.coordinators.length ? " and 26400 (coordinators)" : ""} between them, and only the client ports to your apps.`,
      );
    }
    if (!p.many && a.topology !== "single") {
      items.push(
        a.topology === "cluster"
          ? `Cluster replies (MOVED, CLUSTER SLOTS) name the nodes by container name. Run your app in the same Compose project, or let this machine resolve the names with one hosts-file line: 127.0.0.1 ${p.nodes.map((n) => n.name).join(" ")}`
          : "The coordinators name the nodes by container name, so apps on this machine connect with the published ports listed below; given all of them, the first-party clients find the new primary after a failover.",
      );
    }
    $<HTMLUListElement>("[data-summary]").innerHTML = items.map((t) => `<li>${escape(t)}</li>`).join("");
    const notesEl = $<HTMLUListElement>("[data-notes]");
    notesEl.hidden = notes.length === 0;
    notesEl.innerHTML = notes.map((n) => `<li>${escape(n)}</li>`).join("");
  }

  function showFiles(p: Plan) {
    const out = wizard.render(p, secrets);
    const host = $<HTMLElement>("[data-files]");
    host.innerHTML = out
      .map((m) => {
        const runs = [...p.nodes.filter((n) => n.machine === m.machine).map((n) => n.name), ...p.coordinators.filter((c) => c.machine === m.machine).map((c) => c.name)];
        const head = m.machine ? `<h3 class="machine">On ${escape(m.machine)} <span>(${escape(runs.join(", "))})</span></h3>` : "";
        return head + m.files.map((f) => codeBlock(f.name, f.lang, f.content)).join("");
      })
      .join("");
    wireCopy(host, () => track("wizard_copy", { topology: p.answers.topology, format: p.answers.format, placement: p.answers.placement }));
  }

  function showVerify(p: Plan) {
    const steps = wizard.verifySteps(p, secrets);
    const list = $<HTMLOListElement>("[data-verify]");
    list.innerHTML = steps.map((s) => `<li>${escape(s.text)}<pre class="term"><code>${highlight(s.code, "bash")}</code></pre></li>`).join("");
    $<HTMLElement>("[data-seeds]").textContent = wizard.clientSeeds(p).join(", ");
  }

  form.addEventListener("input", update);
  form.addEventListener("change", update);
  $<HTMLButtonElement>("[data-new-secrets]").addEventListener("click", () => {
    secrets = newSecrets();
    update();
  });
  const share = $<HTMLButtonElement>("[data-share]");
  share.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(location.href);
      share.textContent = "Link copied (no passwords in it)";
    } catch {
      share.textContent = "Copy the address bar";
    }
    setTimeout(() => (share.textContent = "Copy link to these answers"), 2200);
  });

  const initial = wizard.normalize(fromHash()).answers;
  write(initial);
  field("host").value = initial.host;
  field<HTMLTextAreaElement>("hosts").value = initial.hosts.join("\n");
  update();
}

const labels: Record<string, string> = { yaml: "YAML", bash: "Bash", powershell: "PowerShell", ini: "Env" };

function codeBlock(name: string, lang: string, content: string) {
  return `<figure class="term code-block"><figcaption class="term-bar"><span class="label"><b>${labels[lang] ?? lang}</b><span>${escape(name)}</span></span><button type="button" class="copy" data-copy>Copy</button></figcaption><pre tabindex="0"><code>${highlight(content, lang)}</code></pre></figure>`;
}

/** A Google Analytics event when the site's analytics are on (PUBLIC_GA_ID); no answer values beyond the choices. */
function track(name: string, params: Record<string, string>) {
  const layer = (window as unknown as { dataLayer?: unknown[] }).dataLayer;
  if (!layer) return;
  (function gtag(..._args: unknown[]) {
    // gtag.js reads an Arguments object, as the snippet in Analytics.astro pushes.
    layer.push(arguments);
  })("event", name, params);
}

function wireCopy(host: HTMLElement, copied: () => void) {
  for (const btn of host.querySelectorAll<HTMLButtonElement>("[data-copy]")) {
    if (!navigator.clipboard) {
      btn.hidden = true;
      continue;
    }
    btn.addEventListener("click", async () => {
      const code = btn.closest(".code-block")?.querySelector("code")?.textContent ?? "";
      try {
        await navigator.clipboard.writeText(code);
        btn.textContent = "Copied";
        copied();
      } catch {
        btn.textContent = "Copy failed";
      }
      setTimeout(() => (btn.textContent = "Copy"), 1600);
    });
  }
}

function escape(text: string) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/** A small line highlighter for the generated files: comments, keys, strings, variables. */
function highlight(code: string, lang: string) {
  return code
    .replace(/\n$/, "")
    .split("\n")
    .map((line) => highlightLine(line, lang))
    .join("\n");
}

function highlightLine(line: string, lang: string) {
  const span = (cls: string, text: string) => `<span class="tk-${cls}">${escape(text)}</span>`;
  const comment = line.match(/^(\s*)(#.*)$/);
  if (comment) return escape(comment[1]) + span("c", comment[2]);
  let out = "";
  let rest = line;
  const key =
    lang === "yaml" ? rest.match(/^(\s*-?\s*)([\w.-]+)(:)(?=\s|$)/) : lang === "ini" ? rest.match(/^()([A-Z_]+)(=)/) : rest.match(/^(\s*)([a-z_]+)(\s*=)(?=\s)/);
  if (key && (lang === "yaml" || lang === "ini" || /^\s*[a-z_]+\s*=/.test(rest))) {
    out += escape(key[1]) + span("k", key[2]) + escape(key[3]);
    rest = rest.slice(key[0].length);
  }
  const command = rest.match(/^(\s*)(docker|cat|mkdir|chmod|set|Set-Content|New-Item|Set-Location|redis-cli)\b/);
  if (command && lang !== "yaml" && lang !== "ini") {
    out += escape(command[1]) + span("x", command[2]);
    rest = rest.slice(command[0].length);
  }
  const token = /("(?:[^"\\]|\\.)*"|'[^']*'|\$\{[A-Z_]+\}|\$[A-Za-z_]+|(?<![\w.-])\d+(?![\w.-]))/g;
  let last = 0;
  for (const m of rest.matchAll(token)) {
    out += escape(rest.slice(last, m.index));
    const t = m[0];
    out += t.startsWith('"') || t.startsWith("'") ? span("s", t) : t.startsWith("$") ? span("v", t) : /^\d/.test(t) ? span("n", t) : span("x", t);
    last = m.index! + t.length;
  }
  return out + escape(rest.slice(last));
}

const root = document.querySelector<HTMLElement>("[data-wizard]");
if (root) start(root);
