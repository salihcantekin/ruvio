---
title: "The boring parts of an ASP.NET Core API, on one server"
description: "Retried POSTs that must not charge twice, per-tenant rate limits, and a cache that does not stampede. Three NuGet packages put them on Ruvio with a few lines in Program.cs."
date: 2026-10-29
tags: [.NET, Integrations]
scene: idempotency
icon: plug
commands: [IDEM, LIMIT, FILLGET, FILLSET]
facts:
  - { value: "3", label: "packages in this post, all on NuGet" }
  - { value: "1", label: "shared connection they all borrow from Ruvio.Client.AspNetCore" }
  - { value: "429", label: "with Retry-After, answered for you when a budget runs out" }
---

Most of the APIs I have shipped needed the same three things sooner or later. A client retries a POST and the order must not be created twice. One noisy tenant must not eat everybody's capacity. And the product page cache must not fall over when its hottest key expires. None of this is the interesting part of the product, and all of it is easy to get subtly wrong.

Ruvio already has a command for each of them (`IDEM`, `LIMIT`, `FILLGET`). The .NET packages wrap those commands in the shapes ASP.NET Core developers already know: middleware, endpoint filters and `IDistributedCache`. This post is a tour of the three I use the most.

<!-- facts -->

## One connection for everything

Every integration borrows the same client, registered once:

```csharp
builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
```

That comes from `Ruvio.Client.AspNetCore`. If you forget it, the app stops at startup and the error names the missing call, instead of failing on the first request in production.

```shell
dotnet add package Ruvio.AspNetCore.Idempotency
dotnet add package Ruvio.AspNetCore.RateLimiting
dotnet add package Ruvio.Extensions.Caching
```

## 1. Idempotent endpoints

A mobile client on a bad network sends `POST /orders`, times out, and sends it again. With `Ruvio.AspNetCore.Idempotency` the second request with the same `Idempotency-Key` header gets the stored response of the first one instead of running the handler again:

```csharp
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders-production-v1"; // stable across replicas
    options.ScopeSelector = context => context.User.FindFirst("tenant_id")?.Value;
    options.Retention = TimeSpan.FromHours(24);
    // Optional: a handler that throws frees the key, so a retry runs at once.
    options.AbortOnException = true;
});

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization(); // must run before idempotency, including on replays
app.UseRuvioIdempotency();
app.MapPost("/orders", HandleOrder)
    .RequireAuthorization()
    .WithRuvioIdempotency();
```

A retry that arrives while the first request is still running can wait for it and replay its result. A key reused with a *different* request body is refused with 409 or 422, which has caught more client bugs for me than I would like to admit.

> [!WARNING] Not exactly-once for the outside world
> Replay protects your API, not your payment provider. The handler can still crash between charging the card and storing the response, so pass your own idempotency key to the provider too.

For work outside HTTP, such as message consumers and background jobs, `Ruvio.Extensions.Idempotency` does the same with `ExecuteOnceAsync`.

## 2. Rate limits per tenant, per IP, or both

`Ruvio.AspNetCore.RateLimiting` gives you named policies backed by one `LIMIT` call each, so the budget is shared by every instance of your app:

```csharp
builder.Services.AddRuvioRateLimiting(options =>
{
    options.KeyPrefix = "orders:rate:v1:";
    options.PartitionKey = context => context.User.FindFirst("tenant_id")?.Value ?? "";

    // 100 requests a minute per tenant.
    options.AddPolicy("create-order", permits: 100, window: TimeSpan.FromMinutes(1));

    // This one counts per client IP, whatever the tenant is.
    options.AddPolicy("login", permits: 5, window: TimeSpan.FromMinutes(1),
        partitionKey: context => context.Connection.RemoteIpAddress?.ToString() ?? "");
});

app.UseRuvioRateLimiting();
app.MapPost("/login", HandleLogin).WithRuvioRateLimit("login");
app.MapGet("/health", () => "ok").DisableRuvioRateLimit();
```

A rejected request gets 429 with a `Retry-After` header. Policies can also be chained (per IP *and* per tenant), cap concurrent requests, and set a default for every endpoint.

> [!TIP] Decide what an outage means
> If Ruvio is unreachable you choose the behaviour: throw, answer 503, or let requests through. An optional circuit breaker stops calling a server that is down. The same switch exists in the idempotency and caching packages.

<!-- scene: rate-limits -->

## 3. A distributed cache that refills once

`Ruvio.Extensions.Caching` is an `IDistributedCache`, so existing code keeps working. The helpers on top are where it gets nicer: strings and JSON models without `byte[]`, and a `GetOrCreate` that runs your factory in exactly one caller across every instance, using `FILLGET` and `FILLSET` underneath.

```csharp
builder.Services.AddRuvioDistributedCache(options => options.InstanceName = "orders");

// Serve the last good value for an hour while one caller refreshes it, and tag it.
var resilient = new RuvioCacheEntryOptions
{
    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
    StaleFor = TimeSpan.FromHours(1),
    Tags = ["customer:7"],
};
Invoice cached = await cache.GetOrCreateJsonAsync(
    "invoice-model:42", token => LoadInvoiceAsync(token), AppJson.Default.Invoice, resilient);
await cache.RemoveByTagAsync("customer:7");   // every entry tagged customer:7 is gone
```

`StaleFor` is stale-while-revalidate and fail-safe in one option. While the entry is stale, one caller refreshes it and everyone else gets the old value at once. If the factory throws, the stale value is served instead of an error. The [stampede post](/blog/cache-stampedes-fillget/) shows what happens on the wire.

## What else is there

<!-- cards -->
- {layers} **Output caching** `Ruvio.AspNetCore.OutputCaching` stores ASP.NET Core output-cache responses and evicts a whole tag with one command.
- {lock} **Distributed locks** `Ruvio.AspNetCore.DistributedLock`: owner-checked release, automatic renewal, optional fencing tokens, and a hosted service that runs on one instance at a time.
- {shield} **Data Protection keys** `Ruvio.AspNetCore.DataProtection` keeps an encrypted key ring in Ruvio, so every instance can read the others' cookies.

All of them share the client and the outage options described above. The [integrations page](/integrations/) has a short example for each one, and the full READMEs are in the [docs](/docs/integrations/dotnet/).

> [!NOTE] Other languages
> These packages are .NET only for now. The six clients (Rust, C#, Go, Java, Python, Node.js) have a typed method for every command used here, so the same patterns are a few lines in any of them.
