import { GROUPS, allGroups, slugFor } from "./docs-nav.mjs";

// Which icon each docs page and group uses. The sidebar gets them from generated CSS (see sidebarCss),
// so no script runs in the browser. Icon paths live in src/config/docs-icons.mjs.
import { ICONS } from "../src/config/docs-icons.mjs";
export { ICONS };

const GROUP_ICON = {
  Start: "compass",
  Concepts: "lightbulb",
  Operate: "server",
  Clients: "code",
  Integrations: "puzzle",
  Reference: "book",
  Performance: "zap",
  Contributing: "wrench",
};

const PAGE_ICON = {
  "": "home",
  setup: "rocket",
  "ui-connections": "plug",
  configuration: "sliders",
  portability: "package",
  "support-matrix": "table",
  import: "download",
  architecture: "layers",
  "connection-model": "network",
  "data-types": "database",
  "logical-databases": "copy",
  "storage-layout": "drive",
  "memory-accounting": "gauge",
  idem: "repeat",
  recovery: "lifebuoy",
  coordinator: "shield",
  resilience: "shield",
  observability: "activity",
  limits: "gauge",
  "threat-model": "alert",
  "supply-chain": "link",
  "client-behavior": "compare",
  commands: "terminal",
  compatibility: "compare",
  performance: "zap",
  benchmarking: "timer",
  profiling: "search",
  developing: "wrench",
  testing: "flask",
  "docker-resilience-tests": "container",
  toolchain: "cpu",
  releasing: "tag",
  "release-evidence": "clipboard",
  roadmap: "map",
  evidence: "file",
  "integrations/dotnet/caching": "database",
  "integrations/dotnet/output-caching": "zap",
  "integrations/dotnet/rate-limiting": "gauge",
  "integrations/dotnet/idempotency": "repeat",
  "integrations/dotnet/distributed-lock": "lock",
  "integrations/dotnet/data-protection": "shield",
};

// Slug and group label for every listed page, walking nested groups.
function walk(items, group, out) {
  for (const i of items) {
    if (i.items) walk(i.items, group, out);
    else out.push({ slug: i.generated ?? slugFor(i.src), group });
  }
}
export const pages = [];
for (const g of allGroups()) walk(g.items, g.label, pages);
const groupOf = new Map(pages.map((p) => [p.slug, p.group]));

export function pageInfo(slug) {
  const group = groupOf.get(slug);
  let icon = PAGE_ICON[slug];
  if (!icon && slug.startsWith("evidence/")) icon = "file";
  if (!icon && slug.startsWith("clients/")) icon = "code";
  if (!icon && /^integrations\/[^/]+$/.test(slug)) icon = "puzzle";
  icon ??= GROUP_ICON[group] ?? "file";
  return { icon, group };
}

export function svg(name, extra = "") {
  return `<svg class="di" viewBox="0 0 24 24" aria-hidden="true"${extra}><path d="${ICONS[name]}"/></svg>`;
}

const maskUrl = (name) =>
  `url("data:image/svg+xml,${encodeURIComponent(
    `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='#000' stroke-width='1.9' stroke-linecap='round' stroke-linejoin='round'><path d='${ICONS[name]}'/></svg>`,
  )}")`;

/** CSS that puts an icon in front of every docs sidebar link and group title. */
export function sidebarCss() {
  const used = new Set();
  const rules = [];
  const slugs = new Set(["", ...pages.map((p) => p.slug)]);
  for (const slug of slugs) {
    const { icon } = pageInfo(slug);
    used.add(icon);
    rules.push(`.sidebar-content a[href="/docs/${slug ? slug + "/" : ""}"]{--ic:var(--i-${icon})}`);
  }
  // Groups: match the details element by its first link. A group whose first child is a nested group is matched through it.
  const groupRule = (label, first, nested) => {
    const icon = GROUP_ICON[label];
    if (!icon) return;
    used.add(icon);
    const sel = nested ? `details:has(> ul > li > details > ul > li > a[href="${first}"])` : `details:has(> ul > li > a[href="${first}"])`;
    rules.push(`.sidebar-content ${sel} > summary{--ic:var(--i-${icon})}`);
  };
  const firstSlug = (items) => {
    const i = items[0];
    return i.items ? { slug: firstSlug(i.items).slug, nested: true } : { slug: i.generated ?? slugFor(i.src), nested: false };
  };
  for (const g of allGroups()) {
    const { slug, nested } = firstSlug(g.items);
    groupRule(g.label, `/docs/${slug}/`, nested);
  }
  // Nested groups: a .NET package group and the measurement records.
  used.add("package");
  rules.push(`.sidebar-content details:has(> ul > li > a[href="/docs/integrations/dotnet/"]) > summary{--ic:var(--i-package)}`);
  used.add("file");
  rules.push(`.sidebar-content details:has(> ul > li > a[href="/docs/evidence/"]) > summary{--ic:var(--i-file)}`);
  // The changelog link points outside /docs/ but sits in the Reference group, so it keeps an icon.
  used.add("tag");
  rules.push(`.sidebar-content a[href="/changelog/"]{--ic:var(--i-tag)}`);
  const vars = [...used].map((n) => `--i-${n}:${maskUrl(n)};`).join("");
  const base =
    `.sidebar-content :is(a,summary .group-label)::before{content:"";display:inline-block;flex:none;width:1.05em;height:1.05em;margin-right:.6em;vertical-align:-.17em;` +
    `background:currentColor;opacity:.78;-webkit-mask:var(--ic) center/contain no-repeat;mask:var(--ic) center/contain no-repeat}` +
    `.sidebar-content summary .group-label::before{margin-right:.55em;opacity:.9}` +
    `.sidebar-content :is(a,summary){--ic:none}.sidebar-content a:not([href^="/docs/"], [href="/changelog/"])::before{display:none}` +
    `.sidebar-content summary .group-label{display:inline-flex;align-items:center}`;
  return `:root{${vars}}${base}${rules.join("")}`;
}
