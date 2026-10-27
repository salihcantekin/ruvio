<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.AspNetCore.RateLimiting <sub>NuGet</sub></h1>

<p align="center">Endpoint-opt-in distributed fixed-window rate limiting.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.RateLimiting"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.AspNetCore.RateLimiting?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.RateLimiting"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.AspNetCore.RateLimiting?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Opt-in distributed, first-request-anchored fixed-window HTTP budgets using
Ruvio's atomic `LIMIT`. This is middleware, not an implementation of the
in-process ASP.NET Core `RateLimiter`. There is no waiting queue or background task.

```csharp
using Ruvio.AspNetCore.RateLimiting;
using Ruvio.Client.AspNetCore;

builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioRateLimiting(options =>
{
    options.KeyPrefix = "orders:rate:v1:";
    options.PartitionKey = context =>
        context.User.FindFirst("tenant_id")?.Value
        ?? throw new InvalidOperationException("An authenticated tenant is required.");
});

// After building the application:
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.UseRuvioRateLimiting();
app.MapPost("/orders", HandleOrder)
    .RequireAuthorization()
    .WithRuvioRateLimit("create-order", permits: 100, window: TimeSpan.FromMinutes(1));
```

`AddRuvioClient` registers the connection owner and the shared database-scoped
`IRuvioClient`. This middleware does not own or dispose the connection. A missing
partition is a configuration error, not an anonymous shared bucket. Configure an explicit constant for global
limits. Partition values must be trusted and bounded, not arbitrary client headers.
The partition and policy are hashed; raw identifiers are not stored in keys or
telemetry. Every application should have its own prefix.

### Named policies

Define each budget once and refer to it by name, as with the built-in ASP.NET Core rate limiter:

```csharp
builder.Services.AddRuvioRateLimiting(options =>
{
    options.PartitionKey = context => context.User.FindFirst("tenant_id")?.Value ?? "anonymous";
    options.AddPolicy("create-order", permits: 100, window: TimeSpan.FromMinutes(1));
    options.AddPolicy("search", permits: 30, window: TimeSpan.FromSeconds(10), RuvioRateLimitAlgorithm.SlidingWindow);
});

app.MapPost("/orders", HandleOrder).WithRuvioRateLimit("create-order");   // or [RuvioRateLimit("create-order")]
```

An unknown name throws `InvalidOperationException` when the request arrives, before Ruvio is called. A duplicate name or
a non-positive permit count or window is rejected in `AddPolicy`. The inline forms (`WithRuvioRateLimit("p", 100, window)`
and `[RuvioRateLimit("p", 100, 60)]`) need no registration and keep working; a named policy and an inline one with the same
name share one bucket, so keep their settings the same.

Only opted-in endpoints resolve services or call Ruvio. Other endpoints return
the next delegate's task directly: no async state machine, hashing, timers, network
requests, or per-request heap allocation in the bypass path. On an opted-in endpoint
there is one atomic command, no client-side counter and no extra deadline lookup.
There is no change to the server's ordinary GET/SET/INCR paths.

Use the same permit count and window for every endpoint/instance sharing a policy.
Changing settings during a live window does not reset its stored state. Rejected
requests return 429 and a conservative full-window `Retry-After`, not an exact reset
time. Backend failures propagate to the application's exception handler; configure
that handler to return an appropriate 503. The middleware never silently fails open
or retries a possibly-applied `LIMIT`.

Fixed window is the default. To avoid the burst at a window edge, pick another
algorithm: `[RuvioRateLimit("api", 100, 60, Algorithm = RuvioRateLimitAlgorithm.SlidingWindow)]`
weighs the previous window, and `TokenBucket` treats the permits as a bucket that
refills over the window (`.WithRuvioRateLimit("api", 100, TimeSpan.FromMinutes(1), RuvioRateLimitAlgorithm.TokenBucket)`).
Both keep a fixed amount of state per key and return an exact `Retry-After`. A policy
keeps the algorithm it was created with, so a new algorithm needs a new policy name.
These two need a Ruvio server with the `LIMIT` modes.

Eviction, data loss, deletion and failover can reset a budget. Use suitable memory
and durability policies; this is not a billing/quota ledger. Fixed windows allow
bursts around window boundaries.

Optional OpenTelemetry subscriptions: ActivitySource and Meter
`Ruvio.AspNetCore.RateLimiting`. Counters are accepted, rejected and errors; no
user/key tags. No exporters or telemetry worker are installed by this package.

Build/package with `dotnet pack -c Release`. Targets .NET 8; requires the client
and server `LIMIT` command. The package is versioned separately from the server.

## When Ruvio is unreachable

By default a storage error reaches your exception handling (`Outage.Mode = Throw`). Choose what to do instead:

```csharp
builder.Services.AddRuvioRateLimiting(options =>
{
    options.PartitionKey = context => context.User.Identity?.Name ?? "anonymous";
    options.Outage.Mode = RuvioOutageMode.FailOpen;   // or FailClosed: 503, the request does not run
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();   // optional: stop calling Ruvio for 30 s after 5 failures in a row
});
```

`FailOpen` lets requests through **unlimited** while Ruvio is away, and the budgets are whatever Ruvio holds once it is
back. `FailClosed` answers `503` with `Retry-After` while a circuit is open. An error reply such as `WRONGTYPE` is never
treated as an outage. See [When Ruvio is unreachable](../README.md#when-ruvio-is-unreachable) for the modes and the circuit breaker.
