# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.DistributedLock`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- A missing `AddRuvioDistributedLock` registration now fails the request with the usual "no service registered" error instead of a `NullReferenceException`.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged: the `[RuvioLock]` middleware answers 503, `WithDistributedLock` raises; `FailClosed`, 503 on both; `FailOpen`, the request runs with no lock) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

## 0.1.1 - 2026-10-16

- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.0

- First release.
