// The whole changelog history for the search on /changelog/, fetched on the first search only.
import type { APIRoute } from "astro";
import data from "../../data/changelog.json";

export const GET: APIRoute = () => new Response(JSON.stringify(data.available ? data.entries : []), {
  headers: { "Content-Type": "application/json" },
});
