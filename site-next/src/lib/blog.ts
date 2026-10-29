// Blog posts: listing order, reading time, and the Markdown body turned into HTML pieces.
// The body is rendered here rather than by Astro's Markdown pipeline so code blocks get the same
// build-time highlighting and frame as every other code box on the site (src/lib/highlight.ts).
import { getCollection, type CollectionEntry } from "astro:content";
import { createMarkdownProcessor } from "@astrojs/markdown-remark";
import { highlight, languages, type Lang } from "./highlight";
import { ICONS } from "../config/docs-icons.mjs";

export type IconName = keyof typeof ICONS;
export const iconNames = Object.keys(ICONS) as [IconName, ...IconName[]];
/** A line icon from the site's set (src/config/docs-icons.mjs) as inline SVG. */
export const icon = (name: string) => {
  const d = (ICONS as Record<string, string>)[name];
  if (!d) throw new Error(`Unknown icon "${name}" (see src/config/docs-icons.mjs)`);
  return `<svg class="bi" viewBox="0 0 24 24" aria-hidden="true"><path d="${d}"/></svg>`;
};

export type Post = CollectionEntry<"blog">;

/** Published posts, newest first. Drafts show in `astro dev` only. */
export async function posts(): Promise<Post[]> {
  const all = await getCollection("blog", (p) => import.meta.env.DEV || !p.data.draft);
  // Same day: reverse file-name order, so the listing does not change from build to build.
  return all.sort((a, b) => b.data.date.getTime() - a.data.date.getTime() || b.id.localeCompare(a.id));
}

/** Minutes to read at about 220 words a minute, code included. */
export const readingMinutes = (body = "") => Math.max(1, Math.round(body.split(/\s+/).filter(Boolean).length / 220));

export const longDate = (d: Date) =>
  d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });

/**
 * GitHub-style alerts become icon panels: a blockquote that starts with [!NOTE], [!TIP],
 * [!WARNING] or [!IMPORTANT], optionally followed by a title on the same line.
 */
const NOTES: Record<string, { label: string; icon: IconName }> = {
  NOTE: { label: "Note", icon: "info" },
  TIP: { label: "Tip", icon: "lightbulb" },
  WARNING: { label: "Watch out", icon: "alert" },
  IMPORTANT: { label: "Important", icon: "check" },
};
const notes = (html: string) =>
  html.replace(/<blockquote>\s*<p>\[!(NOTE|TIP|WARNING|IMPORTANT)\][ \t]*([^\n<]*)\n?([\s\S]*?)<\/blockquote>/g, (_, kind: string, title: string, rest: string) => {
    const n = NOTES[kind];
    const body = rest.trim().startsWith("</p>") ? rest.trim().slice(4) : `<p>${rest}`;
    return (
      `<aside class="note note-${kind.toLowerCase()}"><span class="note-icon">${icon(n.icon)}</span>` +
      `<div class="note-body"><p class="note-title">${title.trim() || n.label}</p>${body}</div></aside>`
    );
  });

/**
 * `<!-- cards -->` before a list turns it into a grid of cards. Each item reads
 * `{icon} **Title** text`, for example `{lock} **Fenced** Every grant returns a token.`
 */
const cards = (html: string) =>
  html.replace(/<!--\s*cards\s*-->\s*<ul>([\s\S]*?)<\/ul>/g, (_, items: string) => {
    const lis = [...items.matchAll(/<li>([\s\S]*?)<\/li>/g)].map(([, li]) => {
      const m = li.trim().replace(/^<p>|<\/p>$/g, "").match(/^\{([a-z]+)\}\s*<strong>([\s\S]*?)<\/strong>\s*([\s\S]*)$/);
      if (!m) throw new Error(`A card must read "{icon} **Title** text": ${li.trim().slice(0, 80)}`);
      return `<li class="bcard"><span class="bcard-icon">${icon(m[1])}</span><b>${m[2]}</b><span>${m[3]}</span></li>`;
    });
    return `<ul class="bcards" role="list">${lis.join("")}</ul>`;
  });

/** One piece of a post body: HTML, or a placeholder the page fills with a component. */
export type Piece = { html: string } | { scene: string } | { facts: true };

let processor: ReturnType<typeof createMarkdownProcessor> | undefined;

const unescape = (s: string) =>
  s
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(Number(d)))
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&amp;/g, "&");

// A fenced block names its language: ```redis, ```csharp, ... (the keys of `languages`).
const CODE = /<pre><code class="language-([\w-]+)">([\s\S]*?)<\/code><\/pre>/g;

async function frameCode(html: string): Promise<string> {
  const blocks = [...html.matchAll(CODE)];
  const framed = await Promise.all(
    blocks.map(async ([, lang, code]) => {
      if (!(lang in languages)) throw new Error(`Blog code block uses unknown language "${lang}" (see src/lib/highlight.ts)`);
      const inner = await highlight(unescape(code), lang as Lang);
      return (
        `<figure class="term code-block" data-lang="${lang}"><figcaption class="term-bar">` +
        `<span class="label"><b>${languages[lang as Lang]}</b></span>` +
        `<button type="button" class="copy" data-copy hidden>Copy</button></figcaption>` +
        `<pre tabindex="0"><code>${inner}</code></pre></figure>`
      );
    }),
  );
  let i = 0;
  // Wide tables scroll inside their frame instead of widening the page.
  return html.replace(CODE, () => framed[i++]).replace(/<table>([\s\S]*?)<\/table>/g, '<div class="table-wrap"><table>$1</table></div>');
}

/**
 * The post body as HTML pieces. `<!-- scene: id -->` on its own line places an animation and
 * `<!-- facts -->` the strip of facts from the front matter.
 */
export async function renderBody(body: string): Promise<{ pieces: Piece[]; headings: { slug: string; text: string }[] }> {
  processor ??= createMarkdownProcessor({ syntaxHighlight: false });
  const { code, metadata } = await (await processor).render(body);
  const html = cards(notes(await frameCode(code)));
  const pieces: Piece[] = [];
  const marker = /<!--\s*(scene:\s*([a-z-]+)|facts)\s*-->/g;
  let last = 0;
  for (const m of html.matchAll(marker)) {
    pieces.push({ html: html.slice(last, m.index) });
    pieces.push(m[2] ? { scene: m[2] } : { facts: true });
    last = m.index! + m[0].length;
  }
  pieces.push({ html: html.slice(last) });
  return {
    pieces: pieces.filter((p) => !("html" in p) || p.html.trim()),
    headings: metadata.headings.filter((h) => h.depth === 2).map(({ slug, text }) => ({ slug, text })),
  };
}
