<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.AspNetCore.OutputCaching <sub>NuGet</sub></h1>

<p align="center">Native ASP.NET Core output-cache store with distributed tag invalidation.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.OutputCaching"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.AspNetCore.OutputCaching?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.OutputCaching"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.AspNetCore.OutputCaching?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

ASP.NET Core 8 `IOutputCacheStore` backed by Ruvio. This is HTTP **output caching**,
not `IDistributedCache` and not response replay for side-effecting requests.
ASP.NET Core still owns the output-cache policies, serialization, HTTP variation
and eligibility rules.

## Setup

Reference this package and `Ruvio.Client.AspNetCore`, then:

```csharp
using Microsoft.AspNetCore.OutputCaching;
using Ruvio.AspNetCore.OutputCaching;
using Ruvio.Client.AspNetCore;

builder.Services.AddRuvioClient("127.0.0.1", 6379);
builder.Services.AddRuvioOutputCache(o =>
{
    o.InstanceName = "catalog-production-v1";
    o.MaxValueBytes = 1024 * 1024;
});

var app = builder.Build();
app.UseRouting();
// If configured, authentication/authorization and CORS belong before output caching.
app.UseOutputCache();
app.MapGet("/catalog", () => new { version = 1 })
    .CacheOutput(policy => policy.Expire(TimeSpan.FromSeconds(30)).Tag("catalog"));
```

`AddRuvioOutputCache` calls `AddOutputCache` and **replaces** the registered
`IOutputCacheStore`, including the framework's memory store. Call it after another
provider registration when switching stores. Further `AddOutputCache` calls can
configure normal framework policies. The borrowed `IRuvioClient` is resolved on
the first storage operation, not when middleware is constructed. The provider
does not own or dispose it.

To invalidate a tag, inject `IOutputCacheStore`:

```csharp
await store.EvictByTagAsync("catalog", cancellationToken);
```

All application instances sharing output entries must use the same namespace,
limits, HTTP host/scheme/path-base handling and cache policies. Configure trusted
forwarded headers correctly behind a reverse proxy; do not remove host variation
to conceal a proxy configuration problem.

## Tag semantics and costs

- Untagged publication: one binary `SET PX`. Untagged hit or any missing entry:
  one `GET`.
- Tagged publication: one `GET`/`PTTL` pipeline, then native `SET` or `PEXPIRE`
  for tags whose lifetime is shorter than the entry, then one `SET PX` of the
  entry. A tag TTL is extended, never shortened. The steps are separate commands.
- Tagged hit: one `GET` of the entry, then one `MGET` of its tag generations.
  A missing or changed generation means a cache miss.
- Eviction: one `DEL` of the tag generation, independent of the number of entries.
  No key scan, reverse membership set, timer, local dictionary or background task.
  Old entry bytes remain until their original TTL; they are no longer served.
- Recreating a tag generates a new random token. Expiry, eviction and re-creation
  cannot make entries from an older generation valid again.

The `GET` and tag validation are separate operations. A concurrent invalidation
after validation can race with an already in-flight response, as with ordinary
cache reads. Publishing stale application data **after** invalidation can also
cache it again; this provider does not serialize database reads with cache writes.
Framework request coalescing remains process-local, not a distributed lock.

**Cluster tradeoff:** one `InstanceName` occupies one hash slot so tagged
publication and generation lifetime updates remain atomic. It does not spread
one application's output cache across all shards. Different namespaces can land
on different shards, but cannot share tag invalidation. Do not use this package
as a claim of full-cluster throughput for one namespace.

Keys and tags are SHA-256 hashed; raw HTTP cache keys are not sent as server key
names. Payloads remain binary, without Base64. Record overhead is 6 bytes plus
96 bytes per tag; tagged reads copy 32 bytes per tag into a bounded validation
argument. Returning `byte[]` requires extracting the payload from its envelope.
The framework also incurs its normal buffering and serialization costs.

Defaults: 1 MiB serialized value, 4096 UTF-8 bytes per input key/tag, at most
32 tags. Supported configured value range: 1 byte through 8 MiB, subject also to
server protocol/script limits. Entry lifetimes are 1 ms through 365 days, rounded
down to milliseconds. Empty values and null/empty tag arrays are supported.
Coordinate `OutputCacheOptions.MaximumBodySize` with `MaxValueBytes`; the latter
includes framework serialization overhead, not just HTTP body bytes.

No activity occurs unless the feature is registered and a caching policy invokes
storage. Unselected endpoints do not resolve the Ruvio client or create records.
The provider throws on transport errors, wrong types, malformed records and size
violations; it does not silently reinterpret errors as misses or retry writes.
ASP.NET Core controls how its middleware logs/handles store exceptions.

Use a dedicated namespace, not existing Redis output-cache entries. Tag scripts
require this repository's deadline-preserving Lua rollback fix. Eviction or
replica state loss can reduce cache hit rates. Replica rollback can also lose tag
invalidations and bring back stale responses; this is not durable application
state or an authorization boundary. Never enable caching of user-specific content
without an appropriate trusted variation policy.

## A separate connection

By default the store uses the unkeyed `IRuvioClient`. To give it a connection of its
own (another server, database or set of options), register one with
`AddKeyedRuvioClient` from `Ruvio.Client.AspNetCore` and name it in `ClientKey`:

```csharp
builder.Services.AddKeyedRuvioClient("pages", "cache-host", 6379);
builder.Services.AddRuvioOutputCache(o => o.ClientKey = "pages");
```

The keyed client is resolved on first use, like the unkeyed one.

## When Ruvio is unreachable

ASP.NET Core's output-cache middleware already treats a store error on a read or a write as a miss and serves the
endpoint's own response. What `Outage` adds:

- `FailOpen` also drops `EvictByTagAsync` calls instead of throwing. An eviction that was dropped is not repeated, so the
  entries it should have removed can be served again once Ruvio is back; use it only when stale pages are acceptable.
  `Throw` (the default) and `FailClosed` raise from `EvictByTagAsync`.
- A circuit breaker (`options.Outage.CircuitBreaker`) stops every request from waiting out a timeout on a Ruvio that is
  down: an open circuit answers reads as misses at once and skips writes.

```csharp
builder.Services.AddRuvioOutputCache(options =>
{
    options.Outage.Mode = RuvioOutageMode.FailOpen;
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();
});
```

See [When Ruvio is unreachable](../README.md#when-ruvio-is-unreachable) for the modes and the circuit breaker.

## Development

```bash
RUVIO_TEST_ADDR=127.0.0.1:16382 dotnet test integrations/dotnet/Ruvio.AspNetCore.OutputCaching.Tests -c Release
dotnet pack integrations/dotnet/Ruvio.AspNetCore.OutputCaching -c Release -o dist
```

Live tests include two clients/shards, tag re-creation, concurrent eviction, the
32-tag/1-MiB boundary, and real ASP.NET Core output caching across two HTTP hosts.
Without `RUVIO_TEST_ADDR`, live cases are explicitly skipped.
