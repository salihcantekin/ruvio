# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.RateLimiting`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- With `Outage.CircuitBreaker` set, the circuit's changes are logged through `ILogger` (category `Ruvio.AspNetCore.RateLimiting`): Warning when it opens, with the break duration; Information for the trial call and when it closes. Nothing is logged without a breaker.
- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the middleware can use its own server or database, like the other integrations. Null (the default) keeps the unkeyed `IRuvioClient`. Host start fails when no client is registered under that key.
- Startup checks: host start fails when `IRuvioClient` is not registered, when an endpoint names a policy never added with `AddPolicy`, or when an inline limit has no partition key (every bad endpoint is listed), and `UseRuvioRateLimiting` throws without `AddRuvioRateLimiting`. Before, these failed on the first matching request. No connection is opened for the checks; requests are unchanged.
- Per-policy partition: `AddPolicy(name, permits, window, partitionKey)` gives a policy its own key (for example the client IP on a login endpoint) that replaces `options.PartitionKey` for it. `options.PartitionKey` is now only required when some limit has no key of its own.
- Chained limits and concurrency: `AddPolicy(name, policy => policy.Limit(...).Limit(...).Concurrency(...))`. A request must pass every limit, in the order added; `Concurrency` caps requests in flight with `SEMAPHORE` and releases the slot after the endpoint runs. Single-limit policies are unchanged (one command, same key).
- `options.DefaultPolicy` applies a named policy to every routed endpoint without its own limit; `[DisableRuvioRateLimit]` and `.DisableRuvioRateLimit()` opt an endpoint or group out. Without a default policy nothing changes and the bypass path does no extra work.
- `options.EmptyPartition`: `Throw` (default, unchanged) or `SharedBucket`, which puts all requests with a blank partition into one budget per policy.
- `options.OnRejected`: runs after the 429 and `Retry-After` are set, so it can change the status or write a body.

- Named policies: define them once with `options.AddPolicy(name, permits, window, algorithm)` in `AddRuvioRateLimiting`, then refer to them by name only: `[RuvioRateLimit("api")]` or `.WithRuvioRateLimit("api")`. Settings are read from the options at request time (one dictionary lookup); an unknown name throws `InvalidOperationException`. Endpoints that spell out permits and window work as before and share the same bucket as a named policy of the same name.
- `RuvioRateLimitingMiddleware` now takes `IOptions<RuvioRateLimitingOptions>` in its constructor and reads the options once, not on every limited request (the pipeline builds it through dependency injection, so `UseRuvioRateLimiting` is unchanged; code that calls `new RuvioRateLimitingMiddleware(next)` must pass the options). The client is still resolved per request.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged; `FailClosed`, 503; `FailOpen`, the request runs unlimited) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

- `Algorithm` on `[RuvioRateLimit]` and an `algorithm` argument on `WithRuvioRateLimit`: fixed window (default, unchanged), `SlidingWindow` or `TokenBucket`. The other two send `LIMIT ... WITHRETRY`, so `Retry-After` is the exact wait. Needs a Ruvio server with the LIMIT modes.

## 0.1.2 - 2026-10-17

- `WithRuvioRateLimit(policy, permits, window)` endpoint extension, an alternative to the attribute for minimal APIs and groups.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
