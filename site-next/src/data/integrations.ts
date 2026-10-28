// Framework integration packages for the /integrations/ page and the use-case pages.
// The package list (ecosystems, ids, docs slugs) lives in scripts/integrations.mjs, which also drives
// their docs pages; this file adds what the marketing pages show. A new language is one ecosystem
// entry there plus its packages' details here. Examples are trimmed from each package's README.
import { INTEGRATION_ECOSYSTEMS } from "../../scripts/integrations.mjs";
import type { Lang } from "../lib/highlight";

export type Example = { lang: Lang; code: string };
type Details = {
  summary: string;
  /** Use-case ids (see src/pages/use-cases.astro) this package serves. */
  useCases: string[];
  example: Example;
  /** A use case that is better shown with a different snippet than `example`. */
  caseExamples?: Record<string, Example>;
};
type EcosystemDetails = {
  language: string;
  install: (pkg: string) => string;
  installLang: Lang;
  packageUrl: (pkg: string) => string;
  /** File name shown on the examples. */
  exampleFile: string;
  base: { package: string; docs: string; note: string };
};

const cs = (code: string): Example => ({ lang: "csharp", code });

const ecosystemDetails: Record<string, EcosystemDetails> = {
  dotnet: {
    language: "C#",
    install: (pkg) => `dotnet add package ${pkg}`,
    installLang: "shell",
    packageUrl: (pkg) => `https://www.nuget.org/packages/${pkg}`,
    exampleFile: "Program.cs",
    base: {
      package: "Ruvio.Client.AspNetCore",
      docs: "/docs/clients/csharp-aspnetcore/",
      note: "registers the shared connection and IRuvioClient once; every package below borrows it.",
    },
  },
};

const details: Record<string, Details> = {
  "Ruvio.Extensions.Caching": {
    summary:
      "Binary-safe IDistributedCache for web, worker and console apps. Store strings and models as JSON without touching byte[]. GetOrCreate builds a missing entry once across every instance with FILLGET and FILLSET.",
    useCases: ["cache", "sessions"],
    example: cs(`builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioDistributedCache(options => options.InstanceName = "orders");

// One caller across every instance runs the factory; the others receive its result.
var entry = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10) };

// A plain string: no byte[] conversion on your side.
string page = await cache.GetOrCreateStringAsync("invoice:42", token => RenderAsync(token), entry);

// A model stored as JSON; pass your JsonSerializerOptions, or a source-generated JsonTypeInfo<T> for AOT.
Invoice invoice = await cache.GetOrCreateJsonAsync(
    "invoice-model:42", token => LoadInvoiceAsync(token), AppJson.Default.Invoice, entry);

// Already have a JSON string? Store it as a string.
await cache.SetStringAsync("invoice-json:42", json, entry);`),
    caseExamples: {
      sessions: cs(`builder.Services.AddRuvioDistributedCache(o => o.InstanceName = "orders-session-v1");
builder.Services.AddSession(o => o.IdleTimeout = TimeSpan.FromMinutes(20));

var app = builder.Build();
app.UseRouting();
app.UseSession();
app.MapGet("/visits", async (HttpContext context) =>
{
    await context.Session.LoadAsync(context.RequestAborted);
    var visits = (context.Session.GetInt32("visits") ?? 0) + 1;
    context.Session.SetInt32("visits", visits);
    return visits;
});`),
    },
  },
  "Ruvio.AspNetCore.OutputCaching": {
    summary:
      "ASP.NET Core output-cache store. The framework keeps its policies; Ruvio stores the responses and evicts a whole tag with one command, without scans.",
    useCases: ["cache"],
    example: cs(`builder.Services.AddRuvioClient("127.0.0.1", 6379);
builder.Services.AddRuvioOutputCache(o => o.InstanceName = "catalog-production-v1");

var app = builder.Build();
app.UseRouting();
app.UseOutputCache();
app.MapGet("/catalog", () => new { version = 1 })
    .CacheOutput(policy => policy.Expire(TimeSpan.FromSeconds(30)).Tag("catalog"));

// Invalidate every entry with the tag, on every instance:
await store.EvictByTagAsync("catalog", cancellationToken);`),
  },
  "Ruvio.AspNetCore.RateLimiting": {
    summary:
      "Distributed budgets per endpoint with one atomic LIMIT call. Define named policies once and refer to them by name; give a policy its own partition (IP for one endpoint, tenant for another), chain several limits, cap concurrent requests, set a default policy for every endpoint, and shape the rejection answer. Rejected requests get 429 with Retry-After; endpoints without the attribute pay nothing. Choose what happens when Ruvio is unreachable: throw, answer 503, or let requests through, with an optional circuit breaker.",
    useCases: ["rate-limits"],
    example: cs(`builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioRateLimiting(options =>
{
    options.KeyPrefix = "orders:rate:v1:";

    // Default budget owner for every policy that does not name its own: here the tenant.
    options.PartitionKey = context => context.User.FindFirst("tenant_id")?.Value ?? "";

    // A request without a tenant shares one budget instead of failing. It is never unlimited.
    // The default (Throw) treats a blank key as a configuration error.
    options.EmptyPartition = RuvioEmptyPartitionMode.SharedBucket;

    // 100 requests a minute per tenant (uses options.PartitionKey).
    options.AddPolicy("create-order", permits: 100, window: TimeSpan.FromMinutes(1));

    // A policy can count something else: this one is per client IP, whatever the tenant is.
    options.AddPolicy("login", permits: 5, window: TimeSpan.FromMinutes(1),
        partitionKey: context => context.Connection.RemoteIpAddress?.ToString() ?? "");

    // Chained limits: a request must pass all of them, in this order. Put the strictest first,
    // because a request refused by a later limit has already used a permit of the earlier ones.
    options.AddPolicy("search", policy => policy
        .Limit(20, TimeSpan.FromSeconds(10),
            partitionKey: context => context.Connection.RemoteIpAddress?.ToString() ?? "")  // per IP
        .Limit(1000, TimeSpan.FromMinutes(1), algorithm: RuvioRateLimitAlgorithm.SlidingWindow)  // per tenant
        .Concurrency(maxConcurrent: 8, maxRequestDuration: TimeSpan.FromSeconds(30)));  // 8 at once per tenant

    // Applies to every routed endpoint that has no limit of its own. Opt out with DisableRuvioRateLimit().
    options.DefaultPolicy = "create-order";

    // Runs after the default 429 and Retry-After are set; change the status or write a body here.
    options.OnRejected = async (context, rejection) =>
    {
        await context.Response.WriteAsJsonAsync(new
        {
            error = "rate_limited",
            policy = rejection.Policy,
            retryAfterSeconds = (int)rejection.RetryAfter.TotalSeconds,
        });
    };
});

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.UseRuvioRateLimiting();

app.MapPost("/login", HandleLogin).WithRuvioRateLimit("login");
app.MapGet("/search", HandleSearch).WithRuvioRateLimit("search");
app.MapPost("/orders", HandleOrder).RequireAuthorization();   // limited by DefaultPolicy
app.MapGet("/health", () => "ok").DisableRuvioRateLimit();     // never limited`),
  },
  "Ruvio.AspNetCore.Idempotency": {
    summary:
      "HTTP request deduplication and response replay on the native IDEM command. A retried request with the same Idempotency-Key gets the stored response instead of running twice. Choose what happens when Ruvio is unreachable: throw, answer 503, or run unchecked, with an optional circuit breaker.",
    useCases: ["idempotency"],
    example: cs(`builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders-production-v1"; // stable across replicas
    options.ScopeSelector = context => context.User.FindFirst("tenant_id")?.Value;
    options.Retention = TimeSpan.FromHours(24);
});

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization(); // must run before idempotency, including on replays
app.UseRuvioIdempotency();
app.MapPost("/orders", HandleOrder)
    .RequireAuthorization()
    .WithRuvioIdempotency();`),
  },
  "Ruvio.Extensions.Idempotency": {
    summary:
      "Run work once per key outside HTTP: message consumers, background jobs, gRPC handlers. A redelivered message gets the stored result instead of running the work again; a running or failed attempt and other content under the same key are reported.",
    useCases: ["idempotency"],
    example: cs(`builder.Services.AddRuvioIdempotentExecutor(o => o.KeyPrefix = "payments:");

IdempotentResult<Receipt> result = await executor.ExecuteOnceAsync(
    key: message.Id,
    fingerprint: message.ContentHash,
    work: ct => ChargeAsync(message, ct),
    typeInfo: AppJson.Default.Receipt,
    retention: TimeSpan.FromDays(1),
    cancellationToken);

if (result.Outcome == IdempotentOutcome.Pending)
    await message.AbandonAsync();   // another consumer runs it; try again later`),
  },
  "Ruvio.AspNetCore.DistributedLock": {
    summary:
      "Locks with owner-checked release, automatic renewal and optional fencing tokens, a hosted service that runs on one instance at a time, plus a [RuvioLock] endpoint attribute: 409 when taken, request cancelled if the lock is lost. Choose what happens when Ruvio is unreachable: 503, throw, or run without the lock, with an optional circuit breaker.",
    useCases: ["locks"],
    example: cs(`services.AddRuvioDistributedLock(options => options.KeyPrefix = "myapp:lock:");

await using IRuvioLock? held = await locks.TryAcquireAsync(
    "nightly-report", lease: TimeSpan.FromSeconds(30), wait: TimeSpan.FromSeconds(5));

if (held is null) return;               // someone else runs it

await RunReportAsync(held.LockLost);    // stops if the lock is lost mid-run

// Or keep a background worker running on exactly one instance; another takes over if it stops.
services.AddRuvioSingleton<OutboxPublisher>("outbox-publisher");

// Or guard an endpoint: one request per order at a time, across every instance.
app.UseRuvioLock();
app.MapPost("/orders/{id}/pay", PayAsync).WithMetadata(new RuvioLockAttribute("order:{id}"));`),
  },
  "Ruvio.AspNetCore.DataProtection": {
    summary:
      "Durable Data Protection key ring, so every instance can read the others' cookies and protected payloads. Encryption of the ring is required by default.",
    useCases: ["sessions"],
    example: cs(`// The key ring on its own durable server; the cache can stay on a memory-only one.
builder.Services.AddKeyedRuvioClient("key-ring", builder.Configuration.GetSection("RuvioDurable"));

builder.Services.AddDataProtection()
    .SetApplicationName("orders-production")
    .PersistKeysToRuvio(o =>
    {
        o.ClientKey = "key-ring";
        o.Key = "orders:production:data-protection:v1";
    })
    .ProtectKeysWithCertificate(certificate);`),
  },
};

export const ecosystems = INTEGRATION_ECOSYSTEMS.map((e) => {
  const extra = ecosystemDetails[e.id];
  if (!extra) throw new Error(`src/data/integrations.ts has no details for ecosystem ${e.id}`);
  return {
    ...e,
    ...extra,
    docs: `/docs/integrations/${e.id}/`,
    packages: e.packages.map((p) => {
      const d = details[p.package];
      if (!d) throw new Error(`src/data/integrations.ts has no details for ${p.package}`);
      return { ...p, ...d, anchor: p.package.toLowerCase().replace(/\./g, "-"), docs: `/docs/integrations/${e.id}/${p.slug}/` };
    }),
  };
});

export type Ecosystem = (typeof ecosystems)[number];
export type Integration = Ecosystem["packages"][number];

/** The packages that serve a use case, per ecosystem, with the snippet to show for that case. */
export function integrationsFor(caseId: string) {
  return ecosystems.flatMap((e) => {
    const packages = e.packages.filter((p) => p.useCases.includes(caseId));
    if (!packages.length) return [];
    const first = packages[0];
    return [{ ecosystem: e, packages, example: first.caseExamples?.[caseId] ?? first.example }];
  });
}
