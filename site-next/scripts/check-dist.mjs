#!/usr/bin/env node
// Fails the build when a docs page came out without a body (heading and "Edit page" link only).
// A code-highlighting error can empty a page while astro build still exits 0, so the
// built HTML is checked after every build. Plain Node, no dependencies.

import { readFileSync, readdirSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DOCS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../dist/docs");

function* pages(dir) {
  for (const name of readdirSync(dir)) {
    const p = path.join(dir, name);
    if (statSync(p).isDirectory()) yield* pages(p);
    else if (name === "index.html") yield p;
  }
}

const empty = [];
let count = 0;
for (const file of pages(DOCS)) {
  const html = readFileSync(file, "utf8");
  if (!html.includes("sl-markdown-content")) continue;
  count++;
  if (/sl-markdown-content[^"]*">\s*<\/div>/.test(html)) empty.push(path.relative(DOCS, path.dirname(file)) || "/");
}
if (count === 0) {
  console.error("check-dist: no docs pages found in dist/docs");
  process.exit(1);
}
if (empty.length) {
  console.error(`check-dist: ${empty.length} docs page(s) have an empty body:\n  ${empty.join("\n  ")}`);
  process.exit(1);
}
console.log(`check-dist: ${count} docs pages have a body`);
