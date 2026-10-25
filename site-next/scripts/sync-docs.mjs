#!/usr/bin/env node
// Generates the Starlight docs content in src/content/docs/docs/ from the repository:
// docs/*.md, docs/evidence/*.md, the client READMEs and docs/compatibility-matrix.json.
// The repository files are the single source; the output is not committed (see .gitignore).
// Plain Node, no dependencies. Run by `npm run dev` and `npm run build`.

import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pageInfo, pages as iconPages, svg } from "./docs-icons.mjs";
import { COMMANDS, EXCLUDED, README_PAGES, REPO_ROOT, allGroups, slugFor } from "./docs-nav.mjs";

const SITE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(SITE_ROOT, "src/content/docs/docs");
const GITHUB = "https://github.com/salihcantekin/ruvio";
const BRANCH = "main";

const { clients: siteClients } = await import(path.join(SITE_ROOT, "src/config/site.ts"));

// ---------------------------------------------------------------- page map

/** Every repo file that becomes a page: src -> { slug, kind }. */
const pages = new Map();
for (const f of readdirSync(path.join(REPO_ROOT, "docs"))) {
  if (!f.endsWith(".md")) continue;
  const src = `docs/${f}`;
  if (src in EXCLUDED) continue;
  pages.set(src, { slug: slugFor(src), kind: "doc" });
}
for (const f of readdirSync(path.join(REPO_ROOT, "docs/evidence"))) {
  if (f.endsWith(".md")) pages.set(`docs/evidence/${f}`, { slug: slugFor(`docs/evidence/${f}`), kind: "doc" });
}
for (const c of README_PAGES) {
  if (!existsSync(path.join(REPO_ROOT, c.src))) throw new Error(`README not found: ${c.src}`);
  pages.set(c.src, { slug: c.slug, kind: "readme", readme: c });
}

const urlFor = (slug) => (slug ? `/docs/${slug}/` : "/docs/");

// The docs README is excluded as a page, but links to it go to the landing page.
const linkTargets = new Map([...pages].map(([src, p]) => [src, urlFor(p.slug)]));
linkTargets.set("docs/README.md", "/docs/");

// ---------------------------------------------------------------- markdown helpers

const HTML_TAGS = new Set(
  ("a abbr b blockquote br caption code col colgroup dd del details div dl dt em figcaption figure h1 h2 h3 h4 h5 h6 " +
    "hr i img ins kbd li ol p picture pre q s samp small source span strong sub summary sup table tbody td tfoot th " +
    "thead tr u ul var video")
    .split(" "),
);

/** Runs `fn` on each line outside fenced code blocks; `fn(line)` returns the new line. */
function mapProse(text, fn) {
  let fence = null;
  return text
    .split("\n")
    .map((line) => {
      const m = line.match(/^\s*(`{3,}|~{3,})/);
      if (m) {
        if (!fence) fence = m[1];
        else if (m[1][0] === fence[0] && m[1].length >= fence.length) fence = null;
        return line;
      }
      return fence ? line : fn(line);
    })
    .join("\n");
}

/** Applies `fn` to the parts of a line that are not inline code spans. */
function mapOutsideInlineCode(line, fn) {
  return line
    .split(/(`+[^`]*?`+)/)
    .map((part, i) => (i % 2 ? part : fn(part)))
    .join("");
}

/** `<placeholder>` text that is not an HTML tag would be swallowed as raw HTML; escape it. */
function escapeStrayTags(part) {
  return part.replace(/<(\/?)([A-Za-z][A-Za-z0-9-]*)(?=[\s/>])([^>]*)>/g, (all, close, name, rest) =>
    HTML_TAGS.has(name.toLowerCase()) ? all : `&lt;${close}${name}${rest}&gt;`,
  );
}

/** Rewrites one link target found in `fromSrc`. */
function rewriteTarget(target, fromSrc) {
  if (/^([a-z][a-z0-9+.-]*:|#|\/\/)/i.test(target)) return target;
  // Already a docs URL (the title pass in rewriteLinks rewrote it); leave it alone.
  if (/^\/docs\/(?:[^#]*\/)?(?:#.*)?$/.test(target)) return target;
  const hashAt = target.indexOf("#");
  const filePart = hashAt === -1 ? target : target.slice(0, hashAt);
  const anchor = hashAt === -1 ? "" : target.slice(hashAt);
  if (!filePart) return target;
  const repoPath = path.posix.normalize(path.posix.join(path.posix.dirname(fromSrc), decodeURI(filePart)));
  if (repoPath.startsWith("..")) {
    console.warn(`sync-docs: link leaves the repository: ${target} in ${fromSrc}`);
    return target;
  }
  const page = linkTargets.get(repoPath.replace(/\/$/, ""));
  if (page) return page + anchor;
  const abs = path.join(REPO_ROOT, repoPath);
  if (!existsSync(abs)) console.warn(`sync-docs: link to a missing file: ${target} in ${fromSrc}`);
  const kind = existsSync(abs) && statSync(abs).isDirectory() ? "tree" : "blob";
  return `${GITHUB}/${kind}/${BRANCH}/${encodeURI(repoPath.replace(/\/$/, ""))}${anchor}`;
}

/** Page titles by URL, so a link written as a file name ("CONFIGURATION.md") reads as the page title. */
const titleByUrl = new Map();

function rewriteLinks(part, fromSrc) {
  return part
    .replace(/\[(`?[\w./-]+\.md(?:#[\w-]+)?`?)\]\((<[^>]+>|[^)\s]+)\)/g, (all, label, target) => {
      const out = rewriteTarget(target.startsWith("<") ? target.slice(1, -1) : target, fromSrc);
      const title = titleByUrl.get(out.split("#")[0]);
      return `[${title ?? label}](${out})`;
    })
    .replace(/(\]\()(<[^>]+>|[^)\s]+)((?:\s+"[^"]*")?\))/g, (all, open, target, close) => {
      const bare = target.startsWith("<") ? target.slice(1, -1) : target;
      return open + rewriteTarget(bare, fromSrc) + close;
    })
    .replace(/\b(href)="([^"]+)"/g, (all, attr, target) => `${attr}="${rewriteTarget(target, fromSrc)}"`)
    .replace(/\b(src)="([^"]+)"/g, (all, attr, target) => {
      const out = rewriteTarget(target, fromSrc);
      return `${attr}="${out.replace(`${GITHUB}/blob/`, "https://raw.githubusercontent.com/salihcantekin/ruvio/")}"`;
    });
}

/** GitHub alert blockquotes (`> [!NOTE]`) become Starlight asides. */
function convertAlerts(text) {
  const kinds = { NOTE: "note", TIP: "tip", IMPORTANT: "note", WARNING: "caution", CAUTION: "danger" };
  const lines = text.split("\n");
  const out = [];
  for (let i = 0; i < lines.length; i++) {
    const m = lines[i].match(/^>\s*\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*$/);
    if (!m) {
      out.push(lines[i]);
      continue;
    }
    out.push(`:::${kinds[m[1]]}`);
    while (i + 1 < lines.length && lines[i + 1].startsWith(">")) out.push(lines[++i].replace(/^>\s?/, ""));
    out.push(":::");
  }
  return out.join("\n");
}

/** Plain text of a markdown or HTML snippet, for titles and descriptions. */
function plain(s) {
  // Strip tags until stable so nested input like "<scr<b>ipt>" cannot reassemble a tag.
  let previous;
  do {
    previous = s;
    s = s.replace(/<[^>]*>/g, "");
  } while (s !== previous);
  return s
    .replace(/!\[[^\]]*\]\([^)]*\)/g, "")
    .replace(/\[([^\]]+)\]\([^)]*\)/g, "$1")
    .replace(/`([^`]*)`/g, "$1")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/(^|\W)[*_]([^*_]+)[*_](?=\W|$)/g, "$1$2")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

/** First plain paragraph of a body, shortened to a meta description. */
function firstParagraph(body) {
  let fence = false;
  const paras = [];
  let cur = [];
  for (const line of body.split("\n")) {
    if (/^\s*(```|~~~)/.test(line)) {
      fence = !fence;
      if (cur.length) paras.push(cur), (cur = []);
      continue;
    }
    if (fence) continue;
    // Only the introduction, before the first section; a page that opens with a heading uses the text under it.
    if (/^#{1,6} /.test(line)) {
      if (paras.length || cur.length) break;
      continue;
    }
    if (!line.trim()) {
      if (cur.length) paras.push(cur), (cur = []);
      continue;
    }
    cur.push(line);
  }
  if (cur.length) paras.push(cur);
  // Skip tables, lists, quotes, HTML and lead-ins that end with a colon.
  const para = paras.find((p) => !/^\s*([|#>*<:-]|\d+\.|\[!)/.test(p[0]) && !/:\s*$/.test(p[p.length - 1]));
  return para ? plain(para.join(" ")) : "";
}

function shorten(text, max = 180) {
  if (text.length <= max) return text;
  const cut = text.slice(0, max);
  const sentence = cut.match(/^(.*[.!?])\s/);
  if (sentence && sentence[1].length > 60) return sentence[1];
  return cut.replace(/\s+\S*$/, "") + "…";
}

function firstSentence(text) {
  const m = text.match(/^(.+?[.!?])(\s|$)/);
  return shorten(m ? m[1] : text, 160);
}

/** Splits `# Title` (first H1 outside code) from the body; drops breadcrumb lines above it. */
function splitTitle(text) {
  const lines = text.split("\n");
  let fence = false;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*(```|~~~)/.test(lines[i])) fence = !fence;
    if (!fence && /^# /.test(lines[i])) {
      return { title: plain(lines[i].slice(2)), body: lines.slice(i + 1).join("\n") };
    }
  }
  return { title: null, body: text };
}

/** Removes a client or integration README's centered logo, title, badges and link row (everything above the first `---`). */
function stripClientChrome(text) {
  if (!text.trimStart().startsWith("<p align=\"center\">")) return { body: text, tagline: "", registryUrl: "" };
  const lines = text.split("\n");
  const rule = lines.findIndex((l) => l.trim() === "---");
  const chrome = lines.slice(0, rule).join("\n");
  const paras = [...chrome.matchAll(/<p align="center">([\s\S]*?)<\/p>/g)].map((m) => m[1]);
  const tagline = paras.find((p) => !p.includes("<img") && !p.includes("·")) ?? "";
  const badges = paras.find((p) => p.includes("<img alt=")) ?? "";
  const registryUrl = badges.match(/href="([^"]+)"/)?.[1] ?? "";
  return { body: lines.slice(rule + 1).join("\n"), tagline: plain(tagline), registryUrl };
}

function frontmatter(fields, source) {
  const lines = ["---", `# Generated by site-next/scripts/sync-docs.mjs from ${source}. Do not edit; edit the source.`];
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    if (typeof v === "object") {
      lines.push(`${k}:`);
      for (const [k2, v2] of Object.entries(v)) lines.push(`  ${k2}: ${JSON.stringify(v2)}`);
    } else lines.push(`${k}: ${JSON.stringify(v)}`);
  }
  lines.push("---", "");
  return lines.join("\n");
}

function write(slug, content) {
  // A slug with pages below it becomes that folder's index page.
  const sectionIndex = [...pages.values()].some((p) => p.slug.startsWith(`${slug}/`));
  const file = path.join(OUT, slug ? `${sectionIndex ? `${slug}/index` : slug}.md` : "index.md");
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, content);
}

// ---------------------------------------------------------------- pages

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });

const meta = new Map(); // src -> { title, description, url }

for (const [src, page] of pages) {
  const title = page.kind === "readme" ? page.readme.title : splitTitle(readFileSync(path.join(REPO_ROOT, src), "utf8")).title;
  if (title) titleByUrl.set(urlFor(page.slug), title);
}

for (const [src, page] of pages) {
  let text = readFileSync(path.join(REPO_ROOT, src), "utf8").replace(/\r\n/g, "\n");
  let title;
  let description;
  let lead = "";

  if (page.kind === "readme") {
    const { body, tagline, registryUrl } = stripClientChrome(text);
    text = body;
    title = page.readme.title;
    description = tagline;
    // Integration pages name their package; client pages take it from src/config/site.ts.
    const info = siteClients.find((c) => c.slug === page.readme.client);
    const pkg = page.readme.package ?? (src.includes("AspNetCore") ? "Ruvio.Client.AspNetCore" : info?.package);
    const registry = page.readme.registry ?? info?.registry;
    const dir = path.posix.dirname(src);
    if (pkg && registry && registryUrl) {
      lead =
        `Package: [\`${pkg}\`](${registryUrl}) on ${registry} · ` +
        `[Source on GitHub](${GITHUB}/tree/${BRANCH}/${dir})\n\n`;
    }
  } else {
    const split = splitTitle(text);
    title = split.title ?? path.basename(src, ".md");
    text = split.body;
  }

  text = convertAlerts(text);
  text = mapProse(text, (line) => mapOutsideInlineCode(line, (part) => rewriteLinks(escapeStrayTags(part), src)));
  text = text.replace(/^\n+/, "");
  description = shorten(description || firstParagraph(text));

  meta.set(src, { title, description, url: urlFor(page.slug) });
  write(
    page.slug,
    frontmatter({ title, description, editUrl: `${GITHUB}/edit/${BRANCH}/${src}` }, src) + lead + text.trimEnd() + "\n",
  );
}

// Icon and section of every page, read by the page title component (src/components/docs/DocsPageTitle.astro).
writeFileSync(
  path.join(SITE_ROOT, "src/config/docs-pages.json"),
  JSON.stringify(Object.fromEntries(["", ...iconPages.map((p) => p.slug)].map((slug) => [slug, pageInfo(slug)])), null, 1) + "\n",
);

// ---------------------------------------------------------------- command reference

const matrix = JSON.parse(readFileSync(path.join(REPO_ROOT, "docs/compatibility-matrix.json"), "utf8"));
{
  const familyNames = {
    acl: "ACL",
    bitmap: "Bitmaps",
    bloom: "Bloom filters",
    geo: "Geo",
    hyperloglog: "HyperLogLog",
    json: "JSON",
    pubsub: "Pub/Sub",
    scripts: "Scripting",
    zsets: "Sorted sets",
  };
  // Same rule as the heading ids Starlight generates (github-slugger).
  const anchor = (s) => s.toLowerCase().replace(/[^\p{L}\p{N}\s-]/gu, "").replace(/\s/g, "-");
  const nameOf = (f) => familyNames[f] ?? f.charAt(0).toUpperCase() + f.slice(1);
  const cell = (s) => escapeStrayTags(String(s ?? "")).replace(/[\\|]/g, "\\$&").replace(/\s*\n\s*/g, " ");

  const byFamily = new Map();
  for (const c of matrix.commands) {
    if (!byFamily.has(c.family)) byFamily.set(c.family, []);
    byFamily.get(c.family).push(c);
  }
  const families = [...byFamily].sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]));
  const statuses = new Set(matrix.commands.map((c) => c.status));
  const showStatus = statuses.size > 1 || !statuses.has("supported");
  const matrixUrl = `${GITHUB}/blob/${BRANCH}/docs/compatibility-matrix.json`;
  const checkUrl = `${GITHUB}/blob/${BRANCH}/scripts/check_compatibility_matrix.py`;

  let md = "";
  md +=
    `Every command the server accepts, ${matrix.commands.length} in ${families.length} families, ` +
    `read from [\`docs/compatibility-matrix.json\`](${matrixUrl}). ` +
    `CI runs [\`scripts/check_compatibility_matrix.py\`](${checkUrl}), which fails when this list and the ` +
    `command parser or the [compatibility contract](/docs/compatibility/) disagree, and when a command has no test coverage.\n\n`;
  md += `Notes are the limitations recorded for each command in the matrix.\n\n`;
  md += families.map(([f, cmds]) => `[${nameOf(f)}](#${anchor(nameOf(f))}) (${cmds.length})`).join(" · ") + "\n\n";

  for (const [f, cmds] of families) {
    md += `## ${nameOf(f)}\n\n`;
    md += `${cmds.length} ${cmds.length === 1 ? "command" : "commands"}.\n\n`;
    md += showStatus ? "| Command | Status | Notes |\n| --- | --- | --- |\n" : "| Command | Notes |\n| --- | --- |\n";
    for (const c of cmds) {
      const notes = cell(c.limitations);
      md += showStatus ? `| \`${c.name}\` | ${cell(c.status)} | ${notes} |\n` : `| \`${c.name}\` | ${notes} |\n`;
    }
    md += "\n";
  }
  if (matrix.unsupported?.length) {
    md += `## Not supported\n\n| Command | Reason |\n| --- | --- |\n`;
    for (const u of matrix.unsupported) md += `| \`${u.name}\` | ${cell(u.reason)} |\n`;
    md += "\n";
  }
  write(
    COMMANDS.generated,
    frontmatter(
      {
        title: COMMANDS.title,
        description: `The ${matrix.commands.length} commands Ruvio supports, by family, with their limits compared with Redis.`,
        editUrl: false,
      },
      "docs/compatibility-matrix.json",
    ) + md,
  );
  meta.set(`generated:${COMMANDS.generated}`, {
    title: COMMANDS.title,
    description: `Every supported command by family, generated from the compatibility matrix.`,
    url: urlFor(COMMANDS.generated),
  });
}

// ---------------------------------------------------------------- landing page

{
  const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const slugOf = (i) => (i.items ? slugOf(i.items[0]) : (i.generated ?? slugFor(i.src)));
  let html = `<div class="docs-index not-content">\n`;
  for (const group of allGroups()) {
    html += `<section class="docs-index-group">\n<h2>${svg(pageInfo(slugOf(group.items[0])).icon)}${esc(group.label)}</h2>\n<ul>\n`;
    for (const item of group.items) {
      if (item.items) {
        const first = meta.get(item.items[0].src);
        html += `<li>${svg(pageInfo(slugOf(item.items[0])).icon)}<a href="${first.url}">${esc(item.label)}</a><span>${esc(firstSentence(first.description))}</span></li>\n`;
        continue;
      }
      const m = meta.get(item.src ?? `generated:${item.generated}`);
      if (!m) throw new Error(`No page for sidebar item ${item.src ?? item.generated}`);
      html += `<li>${svg(pageInfo(slugOf(item)).icon)}<a href="${m.url}">${esc(m.title)}</a>`;
      if (m.description) html += `<span>${esc(firstSentence(m.description))}</span>`;
      html += `</li>\n`;
    }
    html += `</ul>\n</section>\n`;
  }
  html += `</div>\n`;
  write(
    "",
    frontmatter(
      {
        title: "Documentation",
        description: "Guides and reference for the Ruvio server and its client libraries.",
        editUrl: false,
        tableOfContents: false,
      },
      "docs/, clients/ and scripts/docs-nav.mjs",
    ) +
      `Guides and reference for the Ruvio server and its clients. Each page is generated from the ` +
      `[\`docs/\`](${GITHUB}/tree/${BRANCH}/docs) folder and the client READMEs in the repository.\n\n` +
      html,
  );
}

const excluded = Object.entries(EXCLUDED).map(([f, why]) => `${f} (${why})`);
console.log(`sync-docs: ${pages.size + 2} pages written to ${path.relative(SITE_ROOT, OUT)}; excluded: ${excluded.join("; ") || "none"}`);
