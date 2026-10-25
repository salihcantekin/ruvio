import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { INTEGRATION_ECOSYSTEMS } from "./integrations.mjs";

// Which repository files become docs pages, where they go, and how the sidebar groups them.
// Read by scripts/sync-docs.mjs (to generate the pages and the landing page) and by
// astro.config.mjs (to build the Starlight sidebar). Paths are relative to the repo root.

export const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

/** Slug rule shared with every page that links into the docs: lowercase, underscores to hyphens. */
export function slugFor(src) {
  const special = {
    "docs/README.md": "",
    "docs/evidence/README.md": "evidence",
  };
  if (src in special) return special[src];
  const readme = README_PAGES.find((c) => c.src === src);
  if (readme) return readme.slug;
  const m = src.match(/^docs\/(?:(evidence)\/)?([^/]+)\.md$/);
  if (!m) throw new Error(`No slug rule for ${src}`);
  const base = m[2].toLowerCase().replace(/_/g, "-");
  return m[1] ? `${m[1]}/${base}` : base;
}

// Client READMEs. `title` replaces the centered README heading; `client` is the slug in
// `clients` of src/config/site.ts.
export const CLIENT_PAGES = [
  { src: "clients/README.md", slug: "clients", title: "Clients", label: "Overview" },
  { src: "clients/ruvio-client/README.md", slug: "clients/rust", title: "Rust client", label: "Rust", client: "rust" },
  { src: "clients/csharp/Ruvio.Client/README.md", slug: "clients/csharp", title: "C# client", label: "C#", client: "csharp" },
  {
    src: "clients/csharp/Ruvio.Client.AspNetCore/README.md",
    slug: "clients/csharp-aspnetcore",
    title: "C# client: ASP.NET Core registration",
    label: "C#: ASP.NET Core",
    client: "csharp",
  },
  { src: "clients/go/README.md", slug: "clients/go", title: "Go client", label: "Go", client: "go" },
  { src: "clients/java/README.md", slug: "clients/java", title: "Java client", label: "Java", client: "java" },
  { src: "clients/python/README.md", slug: "clients/python", title: "Python client", label: "Python", client: "python" },
  { src: "clients/node/README.md", slug: "clients/node", title: "Node.js client", label: "Node.js", client: "node" },
];

export const INTEGRATION_PAGES = INTEGRATION_ECOSYSTEMS.flatMap((e) => [
  { src: `${e.dir}/README.md`, slug: `integrations/${e.id}`, title: `${e.label} integrations`, label: "Overview", ecosystem: e.id },
  ...e.packages.map((p) => ({
    src: `${e.dir}/${p.package}/README.md`,
    slug: `integrations/${e.id}/${p.slug}`,
    title: p.title,
    label: p.label,
    package: p.package,
    registry: e.registry,
    ecosystem: e.id,
  })),
]);

/** Every README that becomes a docs page: client libraries and integration packages. */
export const README_PAGES = [...CLIENT_PAGES, ...INTEGRATION_PAGES];

// The generated command reference has no single markdown source.
export const COMMANDS = { generated: "commands", label: "Commands", title: "Commands" };

// Sidebar groups. An item is a source file (`src`), the generated command page, or a nested group.
// `label` is the short sidebar name; the page title still comes from the source's first heading.
export const GROUPS = [
  {
    label: "Start",
    items: [
      { src: "docs/SETUP.md", label: "Setup" },
      { src: "docs/UI_CONNECTIONS.md", label: "Connecting the Operator UI" },
      { src: "docs/CONFIGURATION.md", label: "Configuration" },
      { src: "docs/PORTABILITY.md", label: "Portability" },
      { src: "docs/SUPPORT_MATRIX.md", label: "Support matrix" },
      { src: "docs/IMPORT.md", label: "Import from Redis" },
      { src: "docs/FAQ.md", label: "FAQ" },
    ],
  },
  {
    label: "Concepts",
    items: [
      { src: "docs/ARCHITECTURE.md", label: "Architecture" },
      { src: "docs/CONNECTION_MODEL.md", label: "Connection model" },
      { src: "docs/DATA_TYPES.md", label: "Data types" },
      { src: "docs/LOGICAL_DATABASES.md", label: "Logical databases" },
      { src: "docs/STORAGE_LAYOUT.md", label: "Storage layout" },
      { src: "docs/MEMORY_ACCOUNTING.md", label: "Memory accounting" },
      { src: "docs/IDEM.md", label: "Idempotency (IDEM)" },
    ],
  },
  {
    label: "Operate",
    items: [
      { src: "docs/RECOVERY.md", label: "Recovery and backup" },
      { src: "docs/COORDINATOR.md", label: "Automatic failover" },
      { src: "docs/RESILIENCE.md", label: "Resilience tests" },
      { src: "docs/OBSERVABILITY.md", label: "Observability" },
      { src: "docs/LIMITS.md", label: "Limits" },
      { src: "docs/THREAT_MODEL.md", label: "Threat model" },
      { src: "docs/SUPPLY_CHAIN.md", label: "Supply chain" },
    ],
  },
  {
    label: "Clients",
    items: [
      ...CLIENT_PAGES.map((c) => ({ src: c.src, label: c.label })),
      { src: "docs/CLIENT_BEHAVIOR.md", label: "Client behavior" },
    ],
  },
  {
    label: "Integrations",
    items: INTEGRATION_ECOSYSTEMS.map((e) => ({
      label: e.label,
      items: INTEGRATION_PAGES.filter((p) => p.ecosystem === e.id).map((p) => ({ src: p.src, label: p.label })),
    })),
  },
  {
    label: "Reference",
    items: [
      { generated: COMMANDS.generated, label: COMMANDS.label },
      { src: "docs/COMPATIBILITY.md", label: "Redis compatibility" },
    ],
  },
  {
    label: "Performance",
    items: [
      { src: "docs/PERFORMANCE.md", label: "Performance and memory" },
      { src: "docs/BENCHMARKING.md", label: "Benchmarking" },
      { src: "docs/PROFILING.md", label: "Profiling" },
      { label: "Measurement records", collapsed: true, items: evidenceItems() },
    ],
  },
  {
    label: "Contributing",
    items: [
      { src: "docs/DEVELOPING.md", label: "Developing" },
      { src: "docs/TESTING.md", label: "Testing" },
      { src: "docs/DOCKER_RESILIENCE_TESTS.md", label: "Docker resilience tests" },
      { src: "docs/TOOLCHAIN.md", label: "Toolchain" },
      { src: "docs/RELEASING.md", label: "Releasing" },
      { src: "docs/RELEASE_EVIDENCE.md", label: "Release evidence" },
      { src: "docs/ROADMAP.md", label: "Roadmap" },
    ],
  },
];

// Dated measurement records, newest first, then any undated file; the README is the group index.
function evidenceItems() {
  const dir = path.join(REPO_ROOT, "docs/evidence");
  const files = readdirSync(dir).filter((f) => f.endsWith(".md") && f !== "README.md").sort().reverse();
  return [
    { src: "docs/evidence/README.md", label: "All records" },
    ...files.map((f) => {
      const src = `docs/evidence/${f}`;
      const date = f.match(/^(\d{4}-\d{2}-\d{2})/)?.[1];
      const h1 = readFileSync(path.join(REPO_ROOT, src), "utf8").match(/^# (.+)$/m)?.[1] ?? f;
      const title = h1.replace(/`/g, "").replace(/,?\s*\d{4}-\d{2}-\d{2}$/, "");
      return { src, label: date ? `${date}: ${title}` : title };
    }),
  ];
}

// docs/*.md files that are not pages, with the reason (reported by sync-docs).
export const EXCLUDED = {
  "docs/README.md": "replaced by the generated landing page, which links every section",
};

/** Every source listed in GROUPS. */
function listedSources(items = GROUPS) {
  return items.flatMap((i) => (i.items ? listedSources(i.items) : i.src ? [i.src] : []));
}

/** docs/*.md files that no group lists and that are not excluded; they still become pages. */
export function unlistedDocs() {
  const listed = new Set(listedSources());
  return readdirSync(path.join(REPO_ROOT, "docs"))
    .filter((f) => f.endsWith(".md"))
    .map((f) => `docs/${f}`)
    .filter((src) => !listed.has(src) && !(src in EXCLUDED));
}

/** Groups plus an "Other" group for unlisted docs, so a new docs file is never lost. */
export function allGroups() {
  const extra = unlistedDocs();
  if (!extra.length) return GROUPS;
  return [...GROUPS, { label: "Other", items: extra.map((src) => ({ src, label: null })) }];
}

/** Starlight sidebar config built from the groups. */
export function starlightSidebar() {
  const toEntry = (item) => {
    if (item.items) {
      return { label: item.label, collapsed: Boolean(item.collapsed), items: item.items.map(toEntry) };
    }
    const slug = item.generated ?? slugFor(item.src);
    const entry = { slug: slug ? `docs/${slug}` : "docs" };
    if (item.label) entry.label = item.label;
    return entry;
  };
  // Pages outside the docs that belong in the menu, by group: the changelog is a marketing page at /changelog/.
  const extra = { Reference: [{ label: "Changelog", link: "/changelog/" }] };
  return [
    { label: "Overview", slug: "docs" },
    ...allGroups().map((g) => ({ label: g.label, items: [...g.items.map(toEntry), ...(extra[g.label] ?? [])] })),
  ];
}
