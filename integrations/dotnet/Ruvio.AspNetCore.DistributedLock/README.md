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

## Fencing tokens

A lock alone cannot stop an owner that paused (a long GC, a frozen VM) past its lease from writing after the
next owner took over. With `Fenced` on, each lock carries a number that only grows for that resource, and the
resource you write can refuse a write that carries a smaller number than one it already saw.
`Fenced` needs a server that has `LEASE`, `RENEW` and `RELEASE`.

```csharp
services.AddRuvioDistributedLock(options => options.Fenced = true);

await using IRuvioLock held = await locks.AcquireAsync("ledger", TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));

// UPDATE ledger SET ..., fence = @token WHERE id = @id AND fence < @token
await SaveAsync(held.FencingToken!.Value, held.LockLost);
```

- The lock then uses the server's `LEASE` (renewed with `RENEW`, freed with `RELEASE`) instead of `SET NX`.
  The token survives expiry, release, a restart and a failover: a later owner always gets a larger one.
- `[RuvioLock]` endpoints read it with `HttpContext.GetRuvioLock()?.FencingToken`.
- Without `Fenced`, `FencingToken` is null. Other `IRuvioLock` implementations get null from the interface's default.
- **Turn it on for every instance at once.** A resource a plain lock holds is taken for a fenced one, and the
  other way round, so the two never run together. A lease's key goes away when it is released or expires, so
  names such as `order:{id}` leave nothing behind and a plain lock can take the name again afterwards.
  Servers before this change kept the key: there, use fencing for a bounded set of names.

## One instance at a time: `AddRuvioSingleton`

Background work that must run on exactly one instance (a queue poller, a scheduler, a cache warmer):

```csharp
services.AddRuvioDistributedLock();
services.AddRuvioSingleton<OutboxPublisher>("outbox-publisher");

public sealed class OutboxPublisher(AppDb db) : IRuvioSingletonWorker
{
    public async Task ExecuteAsync(IRuvioLock leadership, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await PublishPendingAsync(db, stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}
```

Every instance asks for the lock every `RetryInterval` (5 s by default, one command per instance). The one
that gets it resolves the worker in a new scope and runs it; the lease (`Lease`, 30 s by default) is renewed
while it runs. `stoppingToken` is cancelled when the host stops or the lock is lost. When the work returns,
throws (the error is logged) or the instance stops, the lock is released and another instance takes over.
A crashed instance holds the work up for at most one lease. Needs `AutoRenew` (the default); the host refuses
to start without it. With `Fenced` on, `leadership.FencingToken` is set.

## Once per period: `RunOncePerPeriodAsync`

```csharp
// Every instance runs this at startup or on its own timer; only the first in each hour sends the report.
bool ran = await locks.RunOncePerPeriodAsync("hourly-report", TimeSpan.FromHours(1), SendReportAsync, ct);
```

One `ONCE` command: the first caller sets a marker that lasts `period` and runs the work; the rest get false.
The period starts at that first call, not on the hour. The marker is set before the work runs, so a failed run
is not repeated in the same period; use `AddRuvioSingleton` or a lock when a failure must be retried.

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

## A separate connection

By default the lock provider uses the unkeyed `IRuvioClient`. To give it a connection of its
own (another server, database or set of options), register one with
`AddKeyedRuvioClient` from `Ruvio.Client.AspNetCore` and name it in `ClientKey`:

```csharp
services.AddKeyedRuvioClient("locks", "lock-host", 6379);
services.AddRuvioDistributedLock(o => o.ClientKey = "locks");
```

## Startup checks

A configuration mistake stops the host while it starts, before the server takes a request, with the fix named in the error. This package checks:

- the client is registered: `AddRuvioClient(...)`, or `AddKeyedRuvioClient(key, ...)` when `ClientKey` is set. Only the registration is checked, no connection is opened, so a Ruvio that is down at start still follows the `Outage` settings; skipped when a `RuvioLockProvider` was registered first, which then stays;
- `UseRuvioLock()` throws at once when `AddRuvioDistributedLock` was not called;
- every `[RuvioLock]` endpoint is checked once the pipeline is built: a reject status outside 4xx/5xx or a negative wait stops start, all bad endpoints in one error. `WithDistributedLock` checks its lease, wait and status when the endpoint is mapped.

See [Startup checks](../README.md#startup-checks) for how the checks run.

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

### Watching the circuit

Each change of the circuit is logged through `ILogger` under the category `Ruvio.AspNetCore.DistributedLock`: a Warning when it opens (with the break
duration), Information for the trial call and when it closes. The client counts the changes in
`ruvio_client_circuit_transitions`. To react in your own code, for a health check or an alert, subscribe to the breaker
after setting it:

```csharp
builder.Services.AddRuvioDistributedLock(options =>
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

## Limits

- One lock lives on one server. If a failover loses the key, two owners can overlap for up to a
  lease. Work that must never overlap should also check a fencing token (see "Fencing tokens") or a
  version on the resource it writes.
- Locks are not reentrant. Acquiring a held resource again from the same process waits like any caller.
- Lease times are wall-clock server TTLs; keep the lease well above the longest pause your process can have.
