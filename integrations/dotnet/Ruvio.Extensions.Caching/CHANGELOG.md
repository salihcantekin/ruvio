# Changelog

Notable changes to the NuGet package `Ruvio.Extensions.Caching`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `options.Outage`: `FailOpen` makes reads misses and drops writes and removals when Ruvio is unreachable (`GetOrCreate*` runs its factory without caching), and an optional circuit breaker stops calls to a Ruvio that is down. Default (`Throw`) unchanged. See the README.

## 0.1.2 - 2026-10-17

- `IDistributedCache` helpers beyond `byte[]`: `GetOrCreateStringAsync`, `SetJsonAsync`/`GetJsonAsync`/`GetOrCreateJsonAsync` (and sync `SetJson`/`GetJson`) taking `JsonSerializerOptions` or a source-generated `JsonTypeInfo<T>`. On `RuvioDistributedCache` they write straight into the stored record and read straight from the reply, with no intermediate `byte[]`; the class also gets `GetStringAsync`/`SetStringAsync`/`GetJsonAsync`/`SetJsonAsync` instance methods.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
