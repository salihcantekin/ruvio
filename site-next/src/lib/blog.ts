// Blog posts: listing order, reading time, and the Markdown body turned into HTML pieces.
// The body is rendered here rather than by Astro's Markdown pipeline so code blocks get the same
// build-time highlighting and frame as every other code box on the site (src/lib/highlight.ts).
import { getCollection, type CollectionEntry } from "astro:content";
import { createMarkdownProcessor } from "@astrojs/markdown-remark";
import { highlight, languages, type Lang } from "./highlight";

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
export async function renderBody(body: string): Promise<Piece[]> {
  processor ??= createMarkdownProcessor({ syntaxHighlight: false });
  const { code } = await (await processor).render(body);
  const html = await frameCode(code);
  const pieces: Piece[] = [];
  const marker = /<!--\s*(scene:\s*([a-z-]+)|facts)\s*-->/g;
  let last = 0;
  for (const m of html.matchAll(marker)) {
    pieces.push({ html: html.slice(last, m.index) });
    pieces.push(m[2] ? { scene: m[2] } : { facts: true });
    last = m.index! + m[0].length;
  }
  pieces.push({ html: html.slice(last) });
  return pieces.filter((p) => !("html" in p) || p.html.trim());
}
