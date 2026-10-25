# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.Idempotency`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged; `FailClosed`, 503; `FailOpen`, the handler runs without idempotency) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

## 0.1.2 - 2026-10-17

- `WithRuvioIdempotency()` endpoint extension, an alternative to the attribute for minimal APIs and groups.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
