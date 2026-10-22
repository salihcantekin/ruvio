<p align="center">
  <a href="https://ruviodb.com/"><img src="../../docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">C# client and ASP.NET Core registration</h1>

<p align="center">The .NET projects under <code>clients/csharp</code>: the <a href="Ruvio.Client/README.md">Ruvio.Client</a> SDK and its ASP.NET Core registration.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.Client?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.Client?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/csharp/">C# guide</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">Integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

| Project | NuGet package | What it is |
| --- | --- | --- |
| [`Ruvio.Client`](Ruvio.Client/README.md) | `Ruvio.Client` | RESP2 client with typed methods for every server command, single-connection multiplexing and cluster routing |
| [`Ruvio.Client.AspNetCore`](Ruvio.Client.AspNetCore/README.md) | `Ruvio.Client.AspNetCore` | Dependency-injection registration, configuration binding and an opt-in `PING` health check |
| `Ruvio.Client.Generators` | none | Source generator that emits the verb encoders used by `Ruvio.Client` |
| `Ruvio.Client.Tests`, `Ruvio.Client.AspNetCore.Tests` | none | xUnit tests; live tests run only when `RUVIO_TEST_ADDR` is set |
| [`Ruvio.Client.Benchmarks`](Ruvio.Client.Benchmarks/README.md) | none | BenchmarkDotNet comparisons |
| `Ruvio.Client.Tour` | none | Calls nearly every client method against a local server (it selects database 15 and flushes it) |

```bash
dotnet add package Ruvio.Client
```

```csharp
using Ruvio.Client;

await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();
await db.SetAsync("session:ada", "hello");
string? value = await db.GetStringAsync("session:ada");
```

## JSON and text values

Values are `string` or a model serialized with a source-generated `JsonTypeInfo<T>`, so callers do not handle `byte[]`. The `byte[]` members (`GetAsync`, `DumpAsync`, `FillResult.Value`) stay for binary data.

```csharp
await db.SetJsonAsync("profile:7", profile, AppJson.Default.Profile, TimeSpan.FromMinutes(5));
Profile? p = await db.GetJsonAsync("profile:7", AppJson.Default.Profile);

FillResult<Profile> r = await db.FillGetJsonAsync("profile:7", TimeSpan.FromSeconds(10), AppJson.Default.Profile);
if (r.State == FillState.Fill)
    await db.FillSetJsonAsync("profile:7", await LoadAsync(), TimeSpan.FromMinutes(5), r.Token!.Value, AppJson.Default.Profile);
```

On a single connection, prefer `GetOrCreateAsync`: it polls with `FillGetAsync` and no `wait`, so it never holds the connection. `FillGetAsync(..., wait: ...)` holds the connection until the value is stored or the wait ends, and every other command on that connection waits behind it. Use `wait` only on a connection kept for it.

## TTL on create

These commands take a `TimeSpan ttl` that is applied only when the call creates the key. A key that already exists keeps its own deadline. Whole seconds are sent as `EX`, anything with a sub-second part as `PX` (rounded up to milliseconds). A zero or negative `ttl` throws `ArgumentOutOfRangeException` before anything is sent.

| Methods | Command |
| --- | --- |
| `IncrementAsync`, `DecrementAsync`, `IncrementByAsync`, `DecrementByAsync`, `IncrementByFloatAsync`, `IncrementWithLimitAsync` | `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `INCRBY ... MAX` |
| `IncrementHashFieldAsync`, `IncrementSortedSetAsync`, `SetBitAsync` | `HINCRBY`, `ZINCRBY`, `SETBIT` |
| `AddSortedSetAsync(key, ttl, flags, ...)`, `AddGeoAsync(key, ttl, ...)`, `AddStreamEntryAsync(key, ttl, id, ...)` | `ZADD`, `GEOADD`, `XADD` |
| `JsonSetAsync(key, path, json, mode, ttl)`, `BloomInsertOptions.Ttl` | `JSON.SET`, `BF.INSERT` |

```csharp
long hits = await db.IncrementAsync("rate:ada", TimeSpan.FromSeconds(60)); // the 60 second window starts with the first hit
await db.AddSortedSetAsync("board:daily", TimeSpan.FromDays(1), SortedSetAddFlags.None, ("ada", 120));
```

`ExpireAsync` and `ExpireAtAsync` also take an `ExpireCondition` (`Nx`, `Xx`, `Gt`, `Lt`). A key without a deadline counts as an infinite deadline for `Gt` and `Lt`, and a failed condition returns `false`.

```csharp
bool extended = await db.ExpireAsync("session:ada", TimeSpan.FromMinutes(30), ExpireCondition.Gt);
```

Run the tests from this folder with `dotnet test`. Start a local server first and set `RUVIO_TEST_ADDR=127.0.0.1:6379` to include the live tests. The client is MIT licensed ([license](../LICENSE)). The [.NET integrations](../../integrations/dotnet/README.md) build on it.
