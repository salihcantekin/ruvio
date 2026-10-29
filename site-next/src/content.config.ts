import { defineCollection, z } from "astro:content";
import { glob } from "astro/loaders";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";
import { ICONS } from "./config/docs-icons.mjs";

// Scenes a post may show: the ids of src/components/animations/UseCaseScene.astro.
const scenes = [
  "rate-limits", "locks", "cache", "rebalance", "multi-node", "balances", "delayed", "idempotency", "changes", "inventory", "replica-reads",
] as const;

export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
  // Blog posts: one Markdown file per post in src/content/blog/; the file name is the URL slug.
  blog: defineCollection({
    loader: glob({ base: "./src/content/blog", pattern: "*.md" }),
    schema: z.object({
      title: z.string(),
      description: z.string(),
      date: z.coerce.date(),
      author: z.string().default("Salih Cantekin"),
      tags: z.array(z.string()).min(1),
      // The animation at the top of the post and on its card in the listing.
      scene: z.enum(scenes),
      // The small thumbnail: a line icon from src/config/docs-icons.mjs.
      icon: z.enum(Object.keys(ICONS) as [string, ...string[]]),
      // Commands the post is about, shown as chips on its card and at its end.
      commands: z.array(z.string()).default([]),
      // Up to four short facts shown as a strip where the body says <!-- facts -->.
      facts: z.array(z.object({ value: z.string(), label: z.string() })).max(4).default([]),
      draft: z.boolean().default(false),
    }),
  }),
};
