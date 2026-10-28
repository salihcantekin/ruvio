// Shared by the changelog pages (rendered at build time) and their search (rendered in the browser),
// so an entry looks the same in both. The data comes from scripts/changelog.mjs.
import { site } from "../config/site";

export type Entry = {
  kind: "commit" | "pr" | "promote" | "release";
  date: string;
  sha?: string; short?: string; onMain?: boolean;
  area?: string | null; text?: string;
  pr?: number; from?: string; count?: number; collapsed?: number;
  tag?: string; label?: string;
};
export type Day = { date: string; entries: Entry[] };

export const PAGE_SIZE = 100; // entries per page

export const esc = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export const longDate = (d: string) =>
  new Date(`${d}T00:00:00Z`).toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });

export const branchOf = (e: Entry) => (e.kind === "release" || e.onMain ? "main" : "dev");

const commits = (n: number) => `${n} ${n === 1 ? "commit" : "commits"}`;

/** One <li> of a day's list. */
export function entryHtml(e: Entry): string {
  const gh = site.github;
  if (e.kind === "release") {
    return (
      `<li class="entry release" data-branch="main"><span class="badge rel">release</span>` +
      `<a class="text" href="${gh}/releases/tag/${esc(e.tag!)}">${esc(e.label!)}</a><code class="sha">${esc(e.tag!)}</code></li>`
    );
  }
  const branch = branchOf(e);
  const badge = e.kind === "promote" ? `<span class="badge promote">dev → main</span>` : `<span class="badge ${branch}">${branch}</span>`;
  let meta = "";
  if (e.kind === "pr") meta = `<a href="${gh}/pull/${e.pr}">#${e.pr}</a> from <code>${esc(e.from!)}</code>, ${commits(e.count ?? 0)}`;
  else if (e.kind === "promote" && e.count) meta = `${e.count} changes from dev`;
  else if (e.collapsed) meta = `${e.collapsed} version updates`;
  return (
    `<li class="entry ${e.kind}" data-branch="${branch}">${badge}<span class="text">` +
    (e.area ? `<span class="area">${esc(e.area)}</span>` : "") +
    esc(e.text ?? "") +
    (meta ? `<span class="meta">${meta}</span>` : "") +
    `</span><a class="sha" href="${gh}/commit/${e.sha}" aria-label="Commit ${e.short}">${e.short}</a></li>`
  );
}

/** A run of entries grouped under day headings, newest first. */
export function daysHtml(entries: Entry[]): string {
  let out = "";
  let open = "";
  for (const e of entries) {
    if (e.date !== open) {
      if (open) out += "</ul></li>";
      open = e.date;
      out += `<li class="day" data-day><h3 class="day-h"><time datetime="${e.date}">${longDate(e.date)}</time></h3><ul class="entries">`;
    }
    out += entryHtml(e);
  }
  return open ? out + "</ul></li>" : out;
}

/** Text a search matches against. */
export const searchText = (e: Entry) =>
  [e.area, e.text, e.label, e.tag, e.from, e.pr && `#${e.pr}`, e.short, e.date].filter(Boolean).join(" ").toLowerCase();
