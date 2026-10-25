# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.RateLimiting`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `RuvioRateLimitingMiddleware` now takes `IOptions<RuvioRateLimitingOptions>` in its constructor and reads the options once, not on every limited request (the pipeline builds it through dependency injection, so `UseRuvioRateLimiting` is unchanged; code that calls `new RuvioRateLimitingMiddleware(next)` must pass the options). The client is still resolved per request.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged; `FailClosed`, 503; `FailOpen`, the request runs unlimited) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

- `Algorithm` on `[RuvioRateLimit]` and an `algorithm` argument on `WithRuvioRateLimit`: fixed window (default, unchanged), `SlidingWindow` or `TokenBucket`. The other two send `LIMIT ... WITHRETRY`, so `Retry-After` is the exact wait. Needs a Ruvio server with the LIMIT modes.

## 0.1.2 - 2026-10-17

- `WithRuvioRateLimit(policy, permits, window)` endpoint extension, an alternative to the attribute for minimal APIs and groups.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
