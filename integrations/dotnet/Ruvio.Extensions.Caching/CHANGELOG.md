# Changelog

Notable changes to the NuGet package `Ruvio.Extensions.Caching`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Docs: the README says the cache polls plain `FILLGET`.
- Needs Ruvio.Client 0.4.0.
- Docs: the README lists what this package checks while the host starts, and shows how to watch the circuit breaker (logs, `ruvio_client_circuit_transitions`, a `StateChanged` handler).
- With `Outage.CircuitBreaker` set, the circuit's changes are logged through `ILogger` (category `Ruvio.Extensions.Caching`): Warning when it opens, with the break duration; Information for the trial call and when it closes. Nothing is logged without a breaker. The package now references `Microsoft.Extensions.Logging.Abstractions`.
- Startup check: host start fails when the client (or the keyed client for `ClientKey`) is not registered, instead of on the first cache call. Skipped when another `IDistributedCache` was registered first. No connection is opened for the check.
- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the cache can use its own server or database. Null (the default) keeps the unkeyed `IRuvioClient`.
- `RuvioCacheEntryOptions.StaleFor`: stale-while-revalidate and fail-safe for `GetOrCreate*` (one caller refreshes while the rest get the stale value; a throwing factory falls back to it, counted by `ruvio.cache.fail_safe`). `RuvioCacheEntryOptions.Tags` and `RemoveByTagAsync(tag)`: remove every entry with a tag in one `DEL`. Entries that use either are written in a new format older package versions cannot read; upgrade every instance first. Entries without them are unchanged. See the README.
- Hit and miss metrics: `ruvio.cache.hit` and `ruvio.cache.miss` counters on the `Ruvio.Extensions.Caching` meter, tagged `operation` (`get` or `get_or_create`); an expired entry is a miss. No keys or values are recorded; with no listener a lookup pays one field read.
- Fewer allocations per call: `GetJsonAsync`, `SetAsync`, `SetStringAsync` and `SetJsonAsync` no longer allocate a closure and delegate for each call. Behaviour is unchanged.
- `options.Outage`: `FailOpen` makes reads misses and drops writes and removals when Ruvio is unreachable (`GetOrCreate*` runs its factory without caching), and an optional circuit breaker stops calls to a Ruvio that is down. Default (`Throw`) unchanged. See the README.

## 0.1.2 - 2026-10-17

- `IDistributedCache` helpers beyond `byte[]`: `GetOrCreateStringAsync`, `SetJsonAsync`/`GetJsonAsync`/`GetOrCreateJsonAsync` (and sync `SetJson`/`GetJson`) taking `JsonSerializerOptions` or a source-generated `JsonTypeInfo<T>`. On `RuvioDistributedCache` they write straight into the stored record and read straight from the reply, with no intermediate `byte[]`; the class also gets `GetStringAsync`/`SetStringAsync`/`GetJsonAsync`/`SetJsonAsync` instance methods.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
