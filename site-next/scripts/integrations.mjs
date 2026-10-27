// Integration packages, grouped by ecosystem so another language is one more entry here.
// Plain data with no file access: read by scripts/docs-nav.mjs (docs pages and sidebar) and by
// src/data/integrations.ts (the /integrations/ page and the use cases).
// Each ecosystem has an overview README in `dir` and one README per package; pages live under
// /docs/integrations/<id>/. `package` is the registry id shown in the page lead.
export const INTEGRATION_ECOSYSTEMS = [
  {
    id: "dotnet",
    label: ".NET",
    dir: "integrations/dotnet",
    registry: "NuGet",
    packages: [
      { package: "Ruvio.Extensions.Caching", slug: "caching", title: "Distributed cache (IDistributedCache)", label: "Distributed cache" },
      { package: "Ruvio.AspNetCore.OutputCaching", slug: "output-caching", title: "Output caching", label: "Output caching" },
      { package: "Ruvio.AspNetCore.RateLimiting", slug: "rate-limiting", title: "Rate limiting", label: "Rate limiting" },
      { package: "Ruvio.AspNetCore.Idempotency", slug: "idempotency", title: "HTTP idempotency", label: "Idempotency" },
      { package: "Ruvio.Extensions.Idempotency", slug: "idempotent-executor", title: "Run once per key (outside HTTP)", label: "Run once" },
      { package: "Ruvio.AspNetCore.DistributedLock", slug: "distributed-lock", title: "Distributed locks", label: "Distributed locks" },
      { package: "Ruvio.AspNetCore.DataProtection", slug: "data-protection", title: "Data Protection key ring", label: "Data Protection" },
    ],
  },
];
