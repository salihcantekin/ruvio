<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.Extensions.Caching <sub>NuGet</sub></h1>

<p align="center"><code>IDistributedCache</code> provider for web, worker and console applications.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.Extensions.Caching"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.Extensions.Caching?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.Extensions.Caching"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.Extensions.Caching?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Binary-safe `IDistributedCache` for .NET 8, using Microsoft.Extensions packages only;
there is no ASP.NET Core shared-framework dependency.

```csharp
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Ruvio.Extensions.Caching;

// Connect during application startup. Keep the owner alive for the app lifetime.
var connection = await RuvioConnection.ConnectAsync("localhost", 6379);
IRuvioClient client = await connection.GetDatabaseAsync(0);
services.AddSingleton<RuvioConnection>(_ => connection);
services.AddSingleton<IRuvioConnection>(provider => provider.GetRequiredService<RuvioConnection>());
services.AddSingleton<IRuvioClient>(provider =>
{
    _ = provider.GetRequiredService<RuvioConnection>();
    return client;
});
services.AddRuvioDistributedCache(options => options.InstanceName = "orders");
// Resolve IDistributedCache through DI in application code.
await cache.SetAsync("invoice:42", new byte[] { 0, 255, 128 },
    new DistributedCacheEntryOptions
    {
        SlidingExpiration = TimeSpan.FromMinutes(5),
        AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1)
    }, cancellationToken);
```

The application owns and disposes the connection. This provider neither connects
nor disposes the client it borrows.
An injected substitute must implement `IRuvioClient.ExecuteBinaryAsync`; the default
interface implementation explicitly rejects unsupported binary commands. Registration
uses `TryAdd`, preserving an existing `IDistributedCache`. Options are validated at
registration and construction and then snapshotted; no hot reload is performed.

## Strings and models, not just `byte[]`

`IDistributedCache` only speaks `byte[]`, so the package adds extension methods on it (they work on any
`IDistributedCache`). On `RuvioDistributedCache` (what `AddRuvioDistributedCache` registers) strings and JSON are encoded
into the stored record and decoded from the reply directly, with no extra `byte[]` copy; `RuvioDistributedCache` also has
`GetStringAsync`/`SetStringAsync`/`GetJsonAsync`/`SetJsonAsync` as instance methods if you inject the class itself:

```csharp
// plain string (Microsoft's SetStringAsync/GetStringAsync also work); this one adds cross-process single build
string html = await cache.GetOrCreateStringAsync("page:1", ct => RenderAsync(ct), entryOptions);

// an object as JSON: default options, your JsonSerializerOptions, or a source-generated JsonTypeInfo<T> (AOT/trim safe)
await cache.SetJsonAsync("order:7", order, entryOptions, jsonOptions);
Order? o = await cache.GetJsonAsync<Order>("order:7", jsonOptions);       // null on a miss
Order  p = await cache.GetOrCreateJsonAsync("order:8", ct => LoadAsync(ct), AppJson.Default.Order, entryOptions);

// already have a JSON string? store it as a string
await cache.SetStringAsync("order:9", json, entryOptions);
```

## Stampede protection

`IDistributedCache` has no way to say "build this once", so the provider adds
`GetOrCreateAsync(key, factory, options)` (an instance method, and an extension on
`IDistributedCache` that falls back to plain get/set on other caches):

```csharp
byte[] page = await cache.GetOrCreateAsync(
    "invoice:42",
    async token => await RenderAsync(token),
    new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10) });
```

It polls plain `FILLGET`. One caller across every process runs the
factory and stores the result with `FILLSET`; the others are held by the server and
receive the stored value. With 20 concurrent callers the factory runs once.

* `FillTimeout` (default 30 s) is how long the builder owns the fill marker. If the factory
  throws or the process dies, the next caller can build once the marker lapses.
* `FillWait` (default 10 s) is how long other callers wait. A caller that waits it out builds
  and stores the value itself, so a slow builder degrades to today's behaviour, never to an error.
* The stored entry is the same envelope as `SetAsync`, so `Get`, sliding renewal and
  expiry behave identically. An entry without any expiry is stored with a placeholder TTL and
  then persisted (one extra command, only on a miss).
* A stale period (below) adds stale-while-revalidate and fail-safe on top of this.

## Stale values, fail-safe and tags

Pass `RuvioCacheEntryOptions` (a `DistributedCacheEntryOptions`) wherever entry options are taken:

```csharp
var entry = new RuvioCacheEntryOptions
{
    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),  // fresh for 5 minutes
    StaleFor = TimeSpan.FromHours(1),                            // then kept as a fallback for an hour
    Tags = ["customer:7", "invoices"],
};

Invoice invoice = await cache.GetOrCreateJsonAsync("invoice:42", LoadInvoiceAsync, AppJson.Default.Invoice, entry);

// Later, when customer 7 changes: every entry tagged with it is gone for every reader.
await cache.RemoveByTagAsync("customer:7");
```

**`StaleFor`** keeps the entry after its fresh period ends. In that window `GetOrCreate*`:

* returns the stale value at once to every caller but one, and that one caller runs the factory and stores the
  new value (`FILLSET ... SWR`), so a busy key never makes callers wait for a rebuild;
* returns the stale value when the factory throws (fail-safe), unless the caller's token was cancelled. Each such
  fallback adds one to the `ruvio.cache.fail_safe` counter on the `Ruvio.Extensions.Caching` meter.

A plain `Get` treats a stale entry as a miss and leaves it stored. `StaleFor` needs an absolute or relative
expiration (the end of the fresh period) and cannot be combined with `SlidingExpiration`; both are checked before
anything is sent.

**`Tags`** (up to `MaxTagsPerEntry`, default 8, at most 64) let `RemoveByTagAsync(tag)` remove every entry written
with that tag. Removal is one `DEL` of the tag's generation key, with no scan: the entries stay stored until they
expire, but no read returns them again. The extension `RemoveByTagAsync` on `IDistributedCache` throws
`NotSupportedException` for other caches.

Costs, paid only by entries that use these options:

* A tagged write first reads each tag's generation (`GET` and `PTTL` in one pipeline) and creates or extends the
  missing ones (a second pipeline, only when needed). A tagged read adds one `MGET` of the entry's tags. In a
  cluster the tag keys live on their own slots, so these pipelines can reach several shards.
* Each tag adds 96 bytes to the stored entry and one small key (`<InstanceName>:ruvio:cache-tag:v1:<sha256>`)
  shared by all its entries. A tag key lives as long as the longest entry written with it.
* A stale period adds one header field and keeps the entry stored for `StaleFor` longer.

> [!WARNING]
> Entries written with a stale period or tags use a newer stored format (`RDC2`) that earlier package versions
> cannot read: they throw on it. Upgrade every instance that shares an `InstanceName` before using these options.
> Entries written without them keep the old format, so upgrading alone changes nothing.

## Contract and expiration

* Empty keys and zero-byte values are valid; null keys, values, and entry options are
  rejected. A missing key returns null; an empty value returns an empty array.
* All byte values round-trip directly as RESP bulk strings, including invalid UTF-8.
  Keys and namespaces are strict UTF-8. Do not modify input bytes until Set completes.
* Set is one `SET`, with `PX` or `PXAT` when the entry expires. Get and Refresh
  are one `GET`. A sliding entry then sends one short `EVAL` that applies
  `PEXPIREAT` only when the stored bytes are still the ones just read, so a
  replacement does not receive the previous lifetime. Remove is one `DEL`.
  Get renews sliding expiration before returning; Refresh does not return the payload.
* Sliding renewal never extends the absolute deadline. When both absolute options
  are specified, the earlier deadline wins. Relative absolute expiration is converted
  to a UTC deadline in `SetAsync` before dispatch, using the client's clock. Explicit
  and relative absolute expiration both require synchronized client/server UTC clocks.
  Queueing/network time consumes the remaining lifetime; deadlines already past on the
  server produce no live entry. Sliding-only expiration uses the server's relative TTL.
* Millisecond resolution: positive durations below 1 ms are rejected; fractional
  milliseconds are rounded down. Absolute deadlines are rounded down to Unix ms and
  must be future at dispatch. Expired entries are never revived by a read or refresh.
* Set writes the final envelope and its expiry in one `SET`. Sliding renewal
  uses the client clock and never passes the stored absolute deadline.
* Server/protocol/corrupt-envelope errors propagate; they never become cache misses.
  Cancellation propagates, but cancellation/disconnection after sending cannot prove
  whether a mutation ran. **No transport-failure mutation replay** is performed.
  Atomic isolation is not a rollback guarantee: server resource/script-limit failures
  after a script has begun mutating can affect the entry or its TTL. Older Ruvio
  servers without expiration-aware script rollback do not restore native key TTLs.
  Treat a failed mutation/renewal as an uncertain outcome and reconcile or remove the
  affected entry; do not rely on its previous expiration or blindly retry the mutation.
  Explicit MOVED redirects may be followed because the server rejected that command.
  Sync methods block on their async equivalent on the thread pool (avoiding caller
  synchronization-context deadlocks). This adds scheduling overhead; prefer async code.

## Storage, limits, and operations

The physical key is `<InstanceName>:ruvio:cache:v1:<key>`. Reserve this namespace for
this provider; do not edit the stored values or TTLs with other clients. Cluster hash
tags in keys/namespaces retain their normal meaning. Scripts declare exactly one key
and work with the client's cluster routing.

One native string entry contains `RDC1\n<absolute-ms-or-0>\n<sliding-ms-or-0>\n<payload>`.
The envelope costs **9–37 bytes** (ASCII decimal fields), not base64 expansion. An entry
with a stale period or tags is stored as
`RDC2\n<absolute>\n<sliding>\n<fresh-until>\n<tag-count>\n<96 bytes per tag><payload>`. Native
key/object/index overhead is additional and implementation-dependent. Persistent
entries have no TTL; apart from tag keys (only with `Tags`) no extra metadata keys, hash fields, polling tasks, timers, or
server changes are introduced. Sliding-only entries use only native expiry plus the
in-value sliding duration. The provider logs no keys or values; its
metrics are counters without key tags (see Metrics).

Defaults: 1 MiB payload and 1024 UTF-8 bytes for the **full namespaced key**.
`MaxValueBytes` (zero allows only empty values) and `MaxKeyBytes` are configurable.
The client RESP bulk ceiling is 64 MiB; the provider reserves 64 bytes for the envelope.
Server protocol/value/request limits and script limits must also accommodate the
envelope and the `SET` command; increasing provider limits does not raise
server limits. Only a sliding renewal sends `EVAL`, and that script is one fixed
text cached on the shard.

## A separate connection

By default the cache uses the unkeyed `IRuvioClient`. To give it a connection of its
own (another server, database or set of options), register one with
`AddKeyedRuvioClient` from `Ruvio.Client.AspNetCore` and name it in `ClientKey`:

```csharp
services.AddKeyedRuvioClient("cache", "cache-host", 6379);
services.AddRuvioDistributedCache(o => { o.ClientKey = "cache"; o.InstanceName = "orders"; });
```

## Metrics

Meter `Ruvio.Extensions.Caching` counts lookups: `ruvio.cache.hit` and `ruvio.cache.miss`, tagged `operation`
(`get` for `Get*`/`Refresh*`, `get_or_create` for `GetOrCreate*`). An expired entry counts as a miss. The tags carry no keys
or values. With no listener attached each lookup pays one field read. With `StaleFor`, a stale value served while
another caller refreshes counts as a hit, a refresh as a miss, and a stale value returned because the factory threw
adds one to `ruvio.cache.fail_safe`.

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(m => m.AddMeter("Ruvio.Extensions.Caching"));
```

## Startup checks

A configuration mistake stops the host while it starts, before the server takes a request, with the fix named in the error. This package checks:

- the client is registered: `AddRuvioClient(...)`, or `AddKeyedRuvioClient(key, ...)` when `ClientKey` is set. Only the registration is checked, no connection is opened, so a Ruvio that is down at start still follows the `Outage` settings; skipped when another `IDistributedCache` was registered first, which then stays.

See [Startup checks](../README.md#startup-checks) for how the checks run.

## When Ruvio is unreachable

By default a storage error reaches the caller (`Outage.Mode = Throw`). With `FailOpen` the cache steps aside:

```csharp
builder.Services.AddRuvioDistributedCache(options =>
{
    options.InstanceName = "orders";
    options.Outage.Mode = RuvioOutageMode.FailOpen;
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();   // optional
});
```

A read is a miss, a write and a removal are dropped (a dropped removal can leave a stale entry that is read again once Ruvio
is back), and `GetOrCreate*` runs its factory without caching, exactly once. An exception thrown by your factory is never
taken for an outage. `FailClosed` behaves as `Throw`: a cache has no response to refuse. Session state stored through the
cache is lost for the requests made during an outage in `FailOpen`.

See [When Ruvio is unreachable](../README.md#when-ruvio-is-unreachable) for the modes and the circuit breaker.

### Watching the circuit

Each change of the circuit is logged through `ILogger` under the category `Ruvio.Extensions.Caching`: a Warning when it opens (with the break
duration), Information for the trial call and when it closes. The client counts the changes in
`ruvio_client_circuit_transitions`. To react in your own code, for a health check or an alert, subscribe to the breaker
after setting it:

```csharp
builder.Services.AddRuvioDistributedCache(options =>
{
    options.Outage.Mode = RuvioOutageMode.FailOpen;
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();
    // Subscribe after setting CircuitBreaker: setting it again replaces the breaker.
    options.Outage.Breaker!.StateChanged += (_, change) =>
        RuvioHealth.Down = change.Current == RuvioCircuitState.Open;   // your own flag, read by a health check
});
```

The handler runs after the state changed, outside the breaker's lock; an exception it throws is caught and does not reach
the call.

## Tests

```sh
dotnet test integrations/dotnet/Ruvio.Extensions.Caching.Tests
RUVIO_TEST_ADDR=127.0.0.1:6379 dotnet test integrations/dotnet/Ruvio.Extensions.Caching.Tests
```

Live tests are explicitly skipped unless `RUVIO_TEST_ADDR` is set. They use unique
namespaces and clean up their own keys (never FLUSHDB). Use an isolated disposable
server for the live suite; it exercises expiration and concurrent writers.
