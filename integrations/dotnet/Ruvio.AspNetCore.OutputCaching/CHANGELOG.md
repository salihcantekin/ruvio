# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.OutputCaching`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Needs Ruvio.Client 0.4.0.
- Docs: the README lists what this package checks while the host starts, and shows how to watch the circuit breaker (logs, `ruvio_client_circuit_transitions`, a `StateChanged` handler).
- With `Outage.CircuitBreaker` set, the circuit's changes are logged through `ILogger` (category `Ruvio.AspNetCore.OutputCaching`): Warning when it opens, with the break duration; Information for the trial call and when it closes. Nothing is logged without a breaker.
- Startup check: host start fails when the client (or the keyed client for `ClientKey`) is not registered, instead of on the first cache lookup. No connection is opened for the check.
- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the store can use its own server or database. Null (the default) keeps the unkeyed `IRuvioClient`.
- Metrics: `ruvio.output_cache.hit`, `ruvio.output_cache.miss` (tagged `reason`: `absent` or `tag_evicted`) and `ruvio.output_cache.tag_evict`. No keys or values are recorded; with no listener a lookup pays one field read.
- `options.Outage`: `FailOpen` drops `EvictByTagAsync` calls instead of throwing when Ruvio is unreachable, and an optional circuit breaker stops requests from waiting on a Ruvio that is down. Default (`Throw`) unchanged. See the README.

## 0.1.2 - 2026-10-16

- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
