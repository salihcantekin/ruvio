// Build-time syntax highlighting for every code box on the marketing pages (CodeBlock, CodeTabs).
// Shiki with the Night Owl theme, the same family the docs use, so code looks alike in both parts.
// Output is plain spans with inline colours: no client-side JavaScript and no theme stylesheet.
import { createHighlighter, type BundledLanguage, type Highlighter, type ThemeRegistration } from "shiki";
import nightOwl from "@shikijs/themes/night-owl";
import { redis, redisSession } from "./grammars";

/** Languages a code box accepts, with the label shown in its frame. */
export const languages = {
  redis: "redis-cli",
  "redis-session": "Terminal",
  shell: "Shell",
  rust: "Rust",
  csharp: "C#",
  go: "Go",
  java: "Java",
  kotlin: "Kotlin",
  python: "Python",
  typescript: "Node.js",
  toml: "TOML",
  json: "JSON",
  sql: "SQL",
} as const;

export type Lang = keyof typeof languages;

// Night Owl's comment grey is too faint on our code background (under 4.5:1); lift it.
const COMMENT = "#8d9bb3";
export const theme: ThemeRegistration = {
  ...nightOwl,
  name: "ruvio-night-owl",
  tokenColors: nightOwl.tokenColors?.map((rule) => {
    const scopes = Array.isArray(rule.scope) ? rule.scope : [rule.scope ?? ""];
    return scopes.some((s) => /(^|\s|,)comment/.test(s)) ? { ...rule, settings: { ...rule.settings, foreground: COMMENT } } : rule;
  }),
};

let highlighter: Promise<Highlighter> | undefined;
const getHighlighter = () =>
  (highlighter ??= createHighlighter({
    themes: [theme],
    langs: ["shellscript", "rust", "csharp", "go", "java", "kotlin", "python", "typescript", "toml", "json", "sql", redis, redisSession],
  }));

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** The inner HTML of a <code> element: one coloured span per token, lines joined by newlines. */
export async function highlight(code: string, lang: Lang): Promise<string> {
  const hl = await getHighlighter();
  // "redis" and "redis-session" are registered above but are not in Shiki's bundled-language type.
  const { tokens } = hl.codeToTokens(code.replace(/^\n+|\s+$/g, ""), { lang: lang as BundledLanguage, theme: theme.name! });
  return tokens
    .map((line) =>
      line
        // Colour only: Night Owl's italics are dropped, as in the docs' code blocks.
        .map((t) => `<span style="color:${t.color}">${esc(t.content)}</span>`)
        .join(""),
    )
    .join("\n");
}
