<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.AspNetCore.DistributedLock <sub>NuGet</sub></h1>

<p align="center">Distributed locks with owner-checked release and automatic renewal, plus a <code>[RuvioLock]</code> endpoint attribute.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.DistributedLock"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.AspNetCore.DistributedLock?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.DistributedLock"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.AspNetCore.DistributedLock?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Distributed mutual exclusion on Ruvio. A lock is one key holding a random owner token, and
every step is a single server command that only the owner can win:

| Step | Command |
| --- | --- |
| acquire | `SET key token NX PX lease` |
| renew | `SET key token IFEQ token PX lease` |
| release | `DELEX key IFEQ token` |

Release and renewal can never touch a lock another owner took after yours expired, which is
the bug a plain `DEL` has. This needs a Ruvio server with `SET … IFEQ` and `DELEX`.

```csharp
services.AddRuvioDistributedLock(options => options.KeyPrefix = "myapp:lock:");

await using IRuvioLock? held = await locks.TryAcquireAsync(
    "nightly-report", lease: TimeSpan.FromSeconds(30), wait: TimeSpan.FromSeconds(5));

if (held is null) return;               // someone else runs it

await RunReportAsync(held.LockLost);    // stops if the lock is lost mid-run
```

- `TryAcquireAsync` returns null when the lock stayed taken; `AcquireAsync` throws `TimeoutException`.
- With `AutoRenew` (default) the lease is renewed every third of its length. If a whole lease
  passes without a successful renewal, or a renewal finds another owner, `IsHeld` turns false and
  `LockLost` is cancelled; pass that token to the guarded work.
- Disposing releases the lock; a failed release is swallowed because the lease expires on its own.
- `RuvioLockOptions.TimeProvider` drives the renewal timer and the acquisition wait. Tests can pass a
  `FakeTimeProvider` to run a lease through its life without waiting; the lease itself still expires on the server's clock.

## ASP.NET Core: `[RuvioLock]` for endpoints

**Why.** Some endpoints must never run twice at once for the same thing: charging an order,
numbering an invoice, importing a file, closing a day. A `lock` or `SemaphoreSlim` only protects
one instance. With two pods, or a double click that reaches a different pod, both requests run.
`[RuvioLock]` moves the guard into Ruvio so it holds across every instance, and removes the
acquire / try-finally / renew / map-to-HTTP boilerplate.

```csharp
builder.Services.AddRuvioDistributedLock();          // needs an IRuvioClient registered
app.UseRouting();
app.UseAuthentication();
app.UseRuvioLock();                                  // after routing and auth

// minimal API
app.MapPost("/orders/{id}/pay", PayAsync).WithMetadata(new RuvioLockAttribute("order:{id}"));

// controller
[RuvioLock("nightly-import", leaseSeconds: 120, WaitMilliseconds = 0)]
[HttpPost("import")]
public Task<IActionResult> Import(CancellationToken ct) => ...;   // ct is cancelled if the lock is lost
```

| Behaviour | Detail |
| --- | --- |
| Name | Fixed (`"nightly-import"`) or template: `{id}` = route value, then query value; `{user}` = user name. An unresolved placeholder answers 400. |
| Taken | Waits `WaitMilliseconds` (default 0), then answers `RejectStatusCode` (default 409) with `Retry-After`; your code does not run. |
| Running | The lease is renewed automatically; the lock is released when the response finishes, also when the endpoint throws or the client disconnects. |
| Lock lost | `HttpContext.RequestAborted` is replaced by a token that also fires when the lock is lost, so EF Core, `HttpClient` and your code stop instead of running next to the new owner. |
| Server unreachable | Fails closed with 503 instead of running unguarded. |
| Cost | Endpoints without the attribute return the next delegate's task directly: no allocation. |

### Telemetry

Meter and ActivitySource `Ruvio.AspNetCore.DistributedLock` (same shape as the rate-limiting
package): counters `ruvio.lock.acquired`, `ruvio.lock.rejected` (contention), `ruvio.lock.lost`
(unhealthy server link) and `ruvio.lock.errors`, plus histogram `ruvio.lock.wait_duration` (ms).
Fixed names are tagged `resource`; templated names are not, to keep cardinality bounded.
```csharp
builder.Services.AddOpenTelemetry().WithMetrics(m => m.AddMeter(RuvioLockTelemetry.Name));
```

## When Ruvio is unreachable

If the lock cannot be asked for, `Outage.Mode` decides. The default (`Throw`) keeps today's behavior: the `[RuvioLock]`
middleware answers `503` and the request does not run, while `WithDistributedLock` endpoints let the storage exception
reach your exception handling.

```csharp
builder.Services.AddRuvioDistributedLock(options =>
{
    options.Outage.Mode = RuvioOutageMode.FailClosed;   // 503 on both paths, Retry-After while a circuit is open
    // options.Outage.Mode = RuvioOutageMode.FailOpen;  // run the request with no lock: not exclusive while Ruvio is away
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();   // optional
});
```

In the middleware, `Throw` and `FailClosed` differ only in the `Retry-After` header, and any storage error (not only an
outage) answers `503`, as before; `FailOpen` bypasses on real outages only. `IRuvioLock` calls made in code still throw.
A lock lost while the request runs is still reported through `LockLost`.

See [When Ruvio is unreachable](../README.md#when-ruvio-is-unreachable) for the modes and the circuit breaker.

## Limits

- One lock lives on one server. If a failover loses the key, two owners can overlap for up to a
  lease. Work that must never overlap should also check a version or fencing value on the resource
  it writes. This package does not issue fencing tokens: handing out an increasing number and
  taking the lock are two commands, so they cannot be made atomic here.
- Locks are not reentrant. Acquiring a held resource again from the same process waits like any caller.
- Lease times are wall-clock server TTLs; keep the lease well above the longest pause your process can have.
