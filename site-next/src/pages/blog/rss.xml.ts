// /blog/rss.xml: an RSS 2.0 feed of the published posts, newest first.
import type { APIRoute } from "astro";
import { site } from "../../config/site";
import { posts } from "../../lib/blog";

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export const GET: APIRoute = async () => {
  const items = (await posts())
    .map((p) => {
      const url = `${site.url}/blog/${p.id}/`;
      return `<item><title>${esc(p.data.title)}</title><link>${url}</link><guid>${url}</guid><pubDate>${p.data.date.toUTCString()}</pubDate><description>${esc(p.data.description)}</description>${p.data.tags.map((t) => `<category>${esc(t)}</category>`).join("")}</item>`;
    })
    .join("");
  const xml = `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Ruvio blog</title><link>${site.url}/blog/</link><description>Notes from building Ruvio.</description><language>en</language>${items}</channel></rss>`;
  return new Response(xml, { headers: { "Content-Type": "application/rss+xml; charset=utf-8" } });
};
