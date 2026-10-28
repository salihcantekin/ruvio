<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.Extensions.Idempotency <sub>NuGet</sub></h1>

<p align="center">Run work once per key in message consumers, background jobs and gRPC handlers; a repeat gets the stored result.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.Extensions.Idempotency"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.Extensions.Idempotency?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.Extensions.Idempotency"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.Extensions.Idempotency?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Queues deliver a message more than once, jobs get retried, and clients resend calls. This package runs the
work for a key once and gives every repeat the stored result, using Ruvio's native `IDEM` command. It has no
ASP.NET Core dependency; for HTTP endpoints use `Ruvio.AspNetCore.Idempotency`.

```csharp
builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioIdempotentExecutor(o => o.KeyPrefix = "payments:");

// In a consumer:
IdempotentResult<Receipt> result = await executor.ExecuteOnceAsync(
    key: message.Id,
    fingerprint: message.ContentHash,                  // null when the id alone identifies the work
    work: ct => ChargeAsync(message, ct),
    typeInfo: AppJson.Default.Receipt,                 // source-generated JSON, trimming and AOT safe
    retention: TimeSpan.FromDays(1),
    cancellationToken);

switch (result.Outcome)
{
    case IdempotentOutcome.Executed:                   // ran now
    case IdempotentOutcome.Replayed:                   // ran before; result.Value is the stored receipt
        await message.CompleteAsync();
        break;
    case IdempotentOutcome.Pending:                    // another consumer is running it, or a run failed
        await message.AbandonAsync();                  // try again later
        break;
    case IdempotentOutcome.Conflict:                   // same id, different content
        await message.DeadLetterAsync();
        break;
}
```

| Overload | Stores |
| --- | --- |
| `ExecuteOnceAsync(key, fingerprint, ct => Task<byte[]>)` | the bytes as they are |
| `ExecuteOnceAsync<T>(key, fingerprint, ct => Task<T>, JsonTypeInfo<T>)` | `T` as UTF-8 JSON |
| `ExecuteOnceAsync(key, fingerprint, ct => Task)` | nothing (an empty result); returns the `IdempotentOutcome` |

## How it works

One call is one `IDEM key BEGIN fingerprint owner retention` and, only when it owns the record, the work and
one `IDEM key COMPLETE fingerprint owner result`. The owner is a new random token per call, so only that call
can complete the record. A repeat while the work runs gets `Pending`; after it, `Replayed` with the result;
with another fingerprint, `Conflict`. The record and its result live for the retention (1 ms to 7 days, whole
milliseconds; `DefaultRetention` is 1 day) and are then forgotten: a repeat after that runs the work again.

Results are limited to 4 MiB by the server. Keys are `KeyPrefix` + your key; reserve that prefix for this
package, because ordinary writes (`SET`, `DEL`, expiry changes) to those keys break the guarantees.
`ClientKey` selects a connection registered with `AddKeyedRuvioClient`.

## Failures: at most once, not exactly once

- **The work throws.** The exception reaches you and, by default, the record stays pending until its retention
  ends. Calls in that time get `Pending` and do not run the work. Choose the retention with this in mind: a short
  one lets a failed message be processed again sooner, a long one catches repeats for longer. To free the key at
  once instead, see below.
- **The work outlives the retention.** The record expired while it ran, so the result is not stored:
  `Executed` comes back with `Stored = false`, and another call may already have run the work too.
- **The process dies mid-work, or Ruvio is unreachable.** Like a throw: pending until the retention ends. A
  storage error after the work ran is raised and never retried, because the result may have been stored.

### Freeing the key when the work throws

```csharp
builder.Services.AddRuvioIdempotentExecutor(o =>
{
    o.KeyPrefix = "payments:";
    o.AbortOnException = true;
});
```

With `AbortOnException` (off by default; it needs a server that has `IDEM ABORT`, otherwise one warning is logged and nothing is freed), a work function that throws or is cancelled makes the executor send
`IDEM key ABORT fingerprint owner` before the exception reaches you, so the next call with that key runs the
work at once. The server removes the record only while it is still pending with this call's owner, so a result
stored by another call is never removed. Turn it on only for work that throws before it takes effect, or that is
safe to run again: the next call runs it again. If the abort itself fails (Ruvio unreachable) the work's exception
still reaches you and the record stays pending until its retention ends. Needs a server with `IDEM ... ABORT`.
Nothing is sent when the work succeeds or when the option is off.

Side effects outside Ruvio (a payment, an email) still happen once per run of the work. Pass the key to the
external system as its own idempotency key where it has one.

## Telemetry

Meter `Ruvio.Extensions.Idempotency`, counter `ruvio.idempotency.outcomes` tagged `outcome`
(`executed`, `replayed`, `pending`, `conflict`, and `aborted` for a record freed by `AbortOnException`). No keys or results are recorded; with no listener a call pays
one field read.

## Startup checks

A configuration mistake stops the host while it starts, before the server takes a request, with the fix named in the error. This package checks:

- the client is registered: `AddRuvioClient(...)`, or `AddKeyedRuvioClient(key, ...)` when `ClientKey` is set. Only the registration is checked and no connection is opened, so a Ruvio that is down at start does not stop it. Skipped when an executor was registered first, which then stays.

See [Startup checks](../README.md#startup-checks) for how the checks run.

## Tests

```bash
dotnet test integrations/dotnet/Ruvio.Extensions.Idempotency.Tests
# Also run against a live Ruvio:
RUVIO_TEST_ADDR=127.0.0.1:6379 dotnet test integrations/dotnet/Ruvio.Extensions.Idempotency.Tests
```
