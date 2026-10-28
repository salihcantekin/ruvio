# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.DistributedLock`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- A timeout or socket error while renewing now counts as a failed renewal (the lock is reported lost after a whole lease without success), and a failed release in `DisposeAsync` or the singleton worker is swallowed instead of escaping; the lease expires on its own.
- Needs Ruvio.Client 0.4.0; `Fenced` needs a server with `LEASE`, `RENEW` and `RELEASE`.
- Docs: the README lists what this package checks while the host starts, and shows how to watch the circuit breaker (logs, `ruvio_client_circuit_transitions`, a `StateChanged` handler).
- With `Outage.CircuitBreaker` set, the circuit's changes are logged through `ILogger` (category `Ruvio.AspNetCore.DistributedLock`): Warning when it opens, with the break duration; Information for the trial call and when it closes. Nothing is logged without a breaker.
- Startup checks: host start fails when the client (or the keyed client for `ClientKey`) is not registered, when an endpoint's `[RuvioLock]` has a reject status outside 4xx/5xx or a negative wait, and `UseRuvioLock` throws without `AddRuvioDistributedLock`. `WithDistributedLock` checks its lease, wait and status when the endpoint is mapped. No connection is opened for the checks; requests are unchanged.
- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the lock provider can use its own server or database. Null (the default) keeps the unkeyed `IRuvioClient`.
- `RuvioLockOptions.Fenced` (off by default): locks use the server's `LEASE`, `RENEW` and `RELEASE`, and `IRuvioLock.FencingToken` carries a number that only grows per resource, for the guarded resource to refuse writes from an owner whose lease ran out. A resource held by a plain lock counts as taken (`WRONGTYPE`). A released or expired lease removes its key on servers with that fix; older servers keep it, so use a bounded set of names there (see the README). `FencingToken` is a default interface member (null), so other `IRuvioLock` implementations keep compiling.
- `[RuvioLock]` endpoints can read their lock (and its fencing token) with `HttpContext.GetRuvioLock()`; it returned null for them before.
- `AddRuvioSingleton<TWorker>(name)`: a hosted service that runs an `IRuvioSingletonWorker` on one instance at a time and hands it to another instance when it stops, fails or loses the lock.
- `RuvioLockProvider.RunOncePerPeriodAsync(name, period, work)`: runs work at most once per period across instances, with the server's `ONCE`.
- A missing `AddRuvioDistributedLock` registration now fails the request with the usual "no service registered" error instead of a `NullReferenceException`.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged: the `[RuvioLock]` middleware answers 503, `WithDistributedLock` raises; `FailClosed`, 503 on both; `FailOpen`, the request runs with no lock) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

## 0.1.1 - 2026-10-16

- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.0

- First release.
