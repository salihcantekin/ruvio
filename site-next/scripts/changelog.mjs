#!/usr/bin/env node
// Builds src/data/changelog.json from the repository's git history for the /changelog/ page.
// Reads the first-parent history of main and dev (remote refs first, then local branches) and the
// release tags, newest first, and writes one JSON file. The output is not committed.
// A checkout without the history (shallow clone, no git) writes { available: false } and the page
// links to GitHub instead, so the build never fails on it. Plain Node, no dependencies.

import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SITE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPO_ROOT = path.resolve(SITE_ROOT, "..");
const OUT = path.join(SITE_ROOT, "src/data/changelog.json");
const BRANCHES = ["main", "dev"];
const DAYS = 3650; // how far back the pages go; they are split into pages of PAGE_SIZE entries
const MAX_ENTRIES = 5000;
const NOTE_SECTIONS = 5; // newest sections of the server CHANGELOG.md shown at the top of the page

const git = (...args) => execFileSync("git", args, { cwd: REPO_ROOT, encoding: "utf8", maxBuffer: 64 << 20 }).trim();
const tryGit = (...args) => {
  try { return git(...args); } catch { return null; }
};

/** origin/<name> when it exists (what is published), else the local branch. */
const resolve = (name) =>
  [`refs/remotes/origin/${name}`, `refs/heads/${name}`].find((ref) => tryGit("rev-parse", "--verify", "-q", ref)) ?? null;

/** Days are UTC so the grouping does not depend on each committer's time zone. */
const day = (unix) => new Date(Number(unix) * 1000).toISOString().slice(0, 10);

// Fields are separated by US (0x1f) and records by RS (0x1e), so subjects and bodies can hold anything.
const FORMAT = ["%H", "%h", "%P", "%ct", "%s", "%b"].join("%x1f") + "%x1e";

function firstParentLog(ref, since) {
  const out = git("log", ref, "--first-parent", `--since=${since}`, `--format=${FORMAT}`);
  return out
    .split("\x1e")
    .map((r) => r.replace(/^\n/, ""))
    .filter(Boolean)
    .map((r) => {
      const [sha, short, parents, time, subject, body] = r.split("\x1f");
      return { sha, short, parents: parents.split(" ").filter(Boolean), time: Number(time), date: day(time), subject, body: body ?? "" };
    });
}

/** "site: add x" -> { area: "site", text: "Add x" }; text without a prefix keeps area null. */
function splitArea(subject) {
  const m = /^([A-Za-z][\w .+/-]{0,23}):\s+(.+)$/.exec(subject);
  if (!m) return { area: null, text: subject };
  return { area: m[1], text: m[2][0].toUpperCase() + m[2].slice(1) };
}

const clean = (s) => s.replace(/\s*\[skip ci\]/gi, "").trim();

/** Turns one commit into a page entry, or null for merges that only sync branches. */
function entryFor(c) {
  const subject = clean(c.subject);
  if (c.parents.length > 1) {
    const pr = /^Merge pull request #(\d+) from [^/\s]+\/(\S+)/.exec(subject);
    if (pr) {
      const title = clean(c.body.split("\n").find((l) => l.trim()) ?? pr[2]);
      const count = Number(tryGit("rev-list", "--count", `${c.parents[0]}..${c.parents[1]}`) ?? 0);
      return { kind: "pr", pr: Number(pr[1]), from: pr[2], ...splitArea(title), count };
    }
    const promote = /^Merge (?:branch '?dev'? into main|dev)(?::\s*(.+))?$/.exec(subject);
    if (promote) {
      const count = Number(tryGit("rev-list", "--count", "--first-parent", `${c.parents[0]}..${c.parents[1]}`) ?? 0);
      return { kind: "promote", area: null, text: promote[1] ? promote[1][0].toUpperCase() + promote[1].slice(1) : "Dev merged into main", count };
    }
    // "Merge branch 'dev' of ...", "Merge remote-tracking branch 'origin/dev' into x": no change of their own.
    if (/^Merge (remote-tracking )?branch /.test(subject)) return null;
  }
  return { kind: "commit", ...splitArea(subject) };
}

function build() {
  if (tryGit("rev-parse", "--is-inside-work-tree") !== "true") return { reason: "no git checkout" };
  if (tryGit("rev-parse", "--is-shallow-repository") === "true") return { reason: "shallow clone" };
  const refs = Object.fromEntries(BRANCHES.map((b) => [b, resolve(b)]).filter(([, r]) => r));
  if (!refs.main && !refs.dev) return { reason: "no main or dev branch" };

  const newest = Math.max(...Object.values(refs).map((r) => Number(git("log", "-1", "--format=%ct", r))));
  const since = day(newest - DAYS * 86400);
  const onMain = refs.main ? new Set(git("rev-list", refs.main, `--since=${since}`).split("\n")) : new Set();

  // One row per commit; a commit on both first-parent lines carries both branch names.
  const rows = new Map();
  for (const [branch, ref] of Object.entries(refs)) {
    for (const c of firstParentLog(ref, since)) {
      const row = rows.get(c.sha);
      if (row) { row.branches.push(branch); continue; }
      const e = entryFor(c);
      if (e) rows.set(c.sha, { sha: c.sha, short: c.short, time: c.time, date: c.date, branches: [branch], ...e });
    }
  }
  for (const r of rows.values()) r.onMain = onMain.has(r.sha);

  // Release tags (ruvio-0.3.4, clients/go/v0.2.0, ruvio-ui-0.1.9) as markers on their day.
  const tags = (tryGit("for-each-ref", "refs/tags", "--format=%(refname:short)\x1f%(creatordate:unix)") ?? "")
    .split("\n")
    .filter(Boolean)
    .map((l) => l.split("\x1f"))
    .filter(([, t]) => t && day(t) >= since)
    .map(([tag, time]) => ({ kind: "release", tag, label: releaseLabel(tag), time: Number(time), date: day(time) }));

  const all = [...rows.values(), ...tags].sort((a, b) => b.time - a.time).slice(0, MAX_ENTRIES);

  // Collapse runs of version-bump commits ("Set release versions to ...") into the newest of the run.
  const entries = [];
  for (const e of all) {
    const prev = entries.at(-1);
    if (prev && isBump(e) && isBump(prev) && prev.date === e.date) { prev.collapsed = (prev.collapsed ?? 1) + 1; continue; }
    entries.push(e);
  }

  for (const e of entries) { delete e.time; delete e.branches; }
  const tips = Object.fromEntries(Object.entries(refs).map(([b, r]) => [b, git("rev-parse", "--short", r)]));
  return { since, tips, entries };
}

const isBump = (e) => e.kind === "commit" && /^Set release versions to /.test(e.text);

function releaseLabel(tag) {
  let m;
  if ((m = /^ruvio-(\d[\w.-]*)$/.exec(tag))) return `Server ${m[1]}`;
  if ((m = /^ruvio-ui-(\d[\w.-]*)$/.exec(tag))) return `Operator UI ${m[1]}`;
  if ((m = /^clients\/(\w+)\/v?(\d[\w.-]*)$/.exec(tag))) return `${m[1][0].toUpperCase()}${m[1].slice(1)} client ${m[2]}`;
  return tag;
}

/** The newest sections of the server's CHANGELOG.md: { title, date, items } with items as Markdown lines. */
function releaseNotes() {
  let text;
  try { text = readFileSync(path.join(REPO_ROOT, "CHANGELOG.md"), "utf8"); } catch { return []; }
  const sections = [];
  for (const line of text.split("\n")) {
    const h = /^## (.+?)(?:\s+-\s+(\d{4}-\d{2}-\d{2}))?\s*$/.exec(line);
    if (h) { sections.push({ title: h[1], date: h[2] ?? null, items: [] }); continue; }
    const cur = sections.at(-1);
    if (!cur) continue;
    if (line.startsWith("- ")) cur.items.push(line.slice(2).trim());
    else if (/^\s+\S/.test(line) && cur.items.length) cur.items[cur.items.length - 1] += ` ${line.trim()}`;
  }
  return sections.filter((s) => s.items.length).slice(0, NOTE_SECTIONS);
}

let data;
try {
  const r = build();
  data = r.entries ? { available: true, generated: new Date().toISOString(), ...r } : { available: false, reason: r.reason };
} catch (err) {
  data = { available: false, reason: String(err.message ?? err).split("\n")[0] };
}
data.notes = releaseNotes();
mkdirSync(path.dirname(OUT), { recursive: true });
writeFileSync(OUT, JSON.stringify(data));
console.log(
  data.available
    ? `changelog: ${data.entries.length} entries since ${data.since}`
    : `changelog: history not available (${data.reason}); the page links to GitHub`,
);
