#!/usr/bin/env node
// Writes the files for LLM tools (https://llmstxt.org) into public/ (gitignored):
//   llms.txt            short intro (src/llms-intro.md) + every docs page with its size
//   llms/<slug>.txt     one plain-Markdown file per docs page, so a tool fetches only what it needs
//   llms-full.txt       every page of the listed sections in one file, under "Optional"
// It reads the pages that scripts/sync-docs.mjs generated, so it must run after it.
// Plain Node, no dependencies. Run by `npm run dev` and `npm run build`.

import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { allGroups, slugFor } from "./docs-nav.mjs";

const SITE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DOCS = path.join(SITE_ROOT, "src/content/docs/docs");
const PUBLIC = path.join(SITE_ROOT, "public");
const { site, clients } = await import(path.join(SITE_ROOT, "src/config/site.ts"));

// Sidebar groups whose pages get a text file; the others are linked under "Optional" only.
const SECTIONS = ["Start", "Concepts", "Clients", "Integrations", "Reference", "Operate"];
// Text that must never reach a published file (site wording rules).
const FORBIDDEN = [/M4 Pro/i];
const LLMS_TXT_MAX = 24 * 1024;

const absolute = (url) => (url.startsWith("/") ? site.url + url : url);
const kb = (bytes) => `${Math.max(1, Math.round(bytes / 1024))} KB`;

/** The generated page for a docs slug: { title, description, body }. */
function readPage(slug) {
  const candidates = slug ? [`${slug}.md`, `${slug}/index.md`] : ["index.md"];
  const file = candidates.map((c) => path.join(DOCS, c)).find(existsSync);
  if (!file) throw new Error(`llms: generated page not found for /docs/${slug}/ (run sync-docs first)`);
  const text = readFileSync(file, "utf8");
  const m = text.match(/^---\n([\s\S]*?)\n---\n/);
  const fm = m ? m[1] : "";
  const field = (k) => {
    const v = fm.match(new RegExp(`^${k}: (.*)$`, "m"))?.[1];
    return v ? JSON.parse(v) : "";
  };
  let body = m ? text.slice(m[0].length) : text;
  // Site-relative links become absolute so the text works outside the site.
  body = body
    .replace(/\]\((\/[^)\s]*)/g, (all, url) => `](${absolute(url)}`)
    .replace(/\b(href|src)="(\/[^"]*)"/g, (all, attr, url) => `${attr}="${absolute(url)}"`);
  return { title: field("title"), description: field("description"), body: body.trim() };
}

/** Flattens a sidebar group into page entries: { slug, url }. */
function pagesOf(items) {
  return items.flatMap((item) => {
    if (item.items) return pagesOf(item.items);
    const slug = item.generated ?? slugFor(item.src);
    return [{ slug, url: `${site.url}/docs/${slug ? `${slug}/` : ""}` }];
  });
}

rmSync(path.join(PUBLIC, "llms"), { recursive: true, force: true });
mkdirSync(path.join(PUBLIC, "llms"), { recursive: true });

const sections = [];
const optional = [];
for (const group of allGroups()) {
  if (!SECTIONS.includes(group.label)) {
    // One link per sidebar entry; a nested group (such as the measurement records) links its first page.
    const top = group.items.map((item) => pagesOf(item.items ? [item.items[0]] : [item])[0]);
    optional.push({ label: group.label, pages: top.map((p) => ({ ...p, ...readPage(p.slug) })) });
    continue;
  }
  const pages = pagesOf(group.items).map((p) => ({ ...p, ...readPage(p.slug) }));
  for (const p of pages) {
    p.file = `llms/${p.slug}.txt`;
    p.text = `# ${p.title}\n\nSource: ${p.url} (part of the Ruvio documentation, index: ${site.url}/llms.txt)\n\n${p.body}\n`;
    mkdirSync(path.dirname(path.join(PUBLIC, p.file)), { recursive: true });
    writeFileSync(path.join(PUBLIC, p.file), p.text);
  }
  sections.push({ label: group.label, pages });
}
const missing = SECTIONS.filter((label) => !sections.some((s) => s.label === label));
if (missing.length) throw new Error(`llms: sidebar group(s) not found: ${missing.join(", ")}`);

const full =
  `# Ruvio documentation (all pages)\n\nGenerated from ${site.url}/docs/. ` +
  `Prefer the single-page files listed in ${site.url}/llms.txt.\n\n` +
  sections.flatMap((s) => s.pages.map((p) => p.text)).join("\n---\n\n");
writeFileSync(path.join(PUBLIC, "llms-full.txt"), full);

// ---------------------------------------------------------------- llms.txt

const firstSentence = (s) => (s.match(/^(.+?[.!?])(\s|$)/)?.[1] ?? s).replace(/\s+/g, " ");

let index = readFileSync(path.join(SITE_ROOT, "src/llms-intro.md"), "utf8").trimEnd() + "\n\n";
index += "## Client libraries\n\n";
for (const c of clients) index += `- ${c.language}: \`${c.package}\` (${c.registry}), docs ${site.url}/docs/clients/${c.slug}/\n`;
index += "\nEvery page below is also a plain-Markdown file; the link goes to it and the size (uncompressed) is shown. The HTML page is at the Source line inside it.\n";
for (const s of sections) {
  index += `\n## ${s.label}\n\n`;
  index += s.pages.map((p) => `- [${p.title}](${site.url}/${p.file}) (${kb(Buffer.byteLength(p.text))})${p.description ? `: ${firstSentence(p.description)}` : ""}`).join("\n") + "\n";
}
index += `\n## Optional\n\n`;
index += `- [All pages in one file](${site.url}/llms-full.txt) (${kb(Buffer.byteLength(full))}): large; open only when the task needs many of the pages above at once.\n`;
for (const o of optional) for (const p of o.pages) index += `- [${o.label}: ${p.title}](${p.url})\n`;
index += `- [Blog](${site.url}/blog/): worked examples of the Ruvio commands.\n`;
writeFileSync(path.join(PUBLIC, "llms.txt"), index);

// ---------------------------------------------------------------- checks

for (const [name, text] of [["llms.txt", index], ["llms-full.txt", full]]) {
  for (const re of FORBIDDEN) if (re.test(text)) throw new Error(`llms: ${name} contains forbidden text ${re}`);
}
const size = statSync(path.join(PUBLIC, "llms.txt")).size;
if (size > LLMS_TXT_MAX) throw new Error(`llms: llms.txt is ${kb(size)}, over the ${kb(LLMS_TXT_MAX)} limit`);
const pageCount = sections.reduce((n, s) => n + s.pages.length, 0);
console.log(`llms: llms.txt ${kb(size)}, ${pageCount} page files, llms-full.txt ${kb(Buffer.byteLength(full))}`);
