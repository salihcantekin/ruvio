// One place for the site's name, links and menus. Header, footer and the docs header read from here.

export const site = {
  name: "Ruvio",
  url: "https://ruviodb.com",
  description:
    "Ruvio is a Rust key-value server that speaks the Redis protocol: one process for caching, locks, conditional writes, change feeds and delayed work.",
  image: "/images/ruvio-social.png",
  imageAlt: "Ruvio: a Redis-compatible server in Rust",
  github: "https://github.com/salihcantekin/ruvio",
  releases: "https://github.com/salihcantekin/ruvio/releases/latest",
  discussions: "https://github.com/salihcantekin/ruvio/discussions",
  dockerHub: "https://hub.docker.com/r/salihcantekin/ruvio",
};

export type NavItem = { label: string; href: string };

export const mainNav: NavItem[] = [
  { label: "Product", href: "/#product" },
  { label: "Use cases", href: "/use-cases/" },
  { label: "Benchmarks", href: "/benchmarks/" },
  { label: "Clients", href: "/docs/clients/" },
  { label: "Integrations", href: "/integrations/" },
  { label: "Docs", href: "/docs/" },
];

export const footerNav: { title: string; items: NavItem[] }[] = [
  {
    title: "Product",
    items: [
      { label: "Overview", href: "/" },
      { label: "Use cases", href: "/use-cases/" },
      { label: "Interactive examples", href: "/examples/" },
      { label: "Benchmarks", href: "/benchmarks/" },
      { label: "Long-run tests", href: "/testing/" },
      { label: "Get started", href: "/get-started/" },
      { label: "Changelog", href: "/changelog/" },
    ],
  },
  {
    title: "Docs",
    items: [
      { label: "Documentation", href: "/docs/" },
      { label: "Configuration", href: "/docs/configuration/" },
      { label: "Commands", href: "/docs/commands/" },
      { label: "Clients", href: "/docs/clients/" },
      { label: "Integrations", href: "/integrations/" },
      { label: "FAQ", href: "/docs/faq/" },
    ],
  },
  {
    title: "Community",
    items: [
      { label: "GitHub", href: site.github },
      { label: "Discussions", href: site.discussions },
      { label: "Releases", href: site.releases },
      { label: "Docker Hub", href: site.dockerHub },
      { label: "Contact", href: "/contact/" },
    ],
  },
];

// First-party clients: each has a typed method for every listed command.
// `lang` is the highlighting language of their snippets (see src/lib/highlight.ts).
export const clients = [
  { language: "Rust", package: "ruvio-client", registry: "crates.io", slug: "rust", lang: "rust" },
  { language: "C#", package: "Ruvio.Client", registry: "NuGet", slug: "csharp", lang: "csharp" },
  { language: "Go", package: "github.com/salihcantekin/ruvio/clients/go", registry: "Go modules", slug: "go", lang: "go" },
  { language: "Java", package: "io.github.salihcantekin:ruvio-client", registry: "Maven Central", slug: "java", lang: "java" },
  { language: "Python", package: "ruvio-client", registry: "PyPI", slug: "python", lang: "python" },
  { language: "Node.js", package: "ruvio-client", registry: "npm", slug: "node", lang: "typescript" },
] as const;
