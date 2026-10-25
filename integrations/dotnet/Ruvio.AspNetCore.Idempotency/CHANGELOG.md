# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.Idempotency`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Less allocation per idempotent request: the request body is read into, and the response is buffered in, storage rented from the shared array pool (grown as needed, cleared when returned) instead of a new array each time, and the response buffer no longer reserves the full `MaxResponseBodyBytes` up front. Limits and behaviour are unchanged.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged; `FailClosed`, 503; `FailOpen`, the handler runs without idempotency) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

## 0.1.2 - 2026-10-17

- `WithRuvioIdempotency()` endpoint extension, an alternative to the attribute for minimal APIs and groups.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
