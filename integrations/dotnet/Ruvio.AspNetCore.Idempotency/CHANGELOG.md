# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.Idempotency`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the middleware can use its own server or database. Null (the default) keeps the unkeyed `IRuvioClient`.
- `options.AbortOnException` (off by default): when the handler throws, the pending record is given up with `IDEM ... ABORT`, so a retry with the same key runs at once instead of getting 409 until the retention ends. Only a pending record of this request is removed. Turn it on only for handlers whose work did not take effect when they throw, or is safe to repeat. Needs a server with `IDEM ABORT`. See the README.
- New options, with today's behavior as the default. `MismatchStatusCode` (409; set 422 for the IETF `Idempotency-Key` draft) for a key reused with a different request; `PendingWait` (zero) lets a request whose key is still running wait and replay the first result instead of answering 409; `KeyHeader`; `ReplayHeaders` (add `Location` for 201 responses; cookie, authentication, CORS and transport headers are refused). A 409 for a running request now carries `Retry-After: 1` and a replay carries `Idempotent-Replayed: true`. `[RuvioIdempotency]` and the new `WithRuvioIdempotency(e => ...)` take per-endpoint `RetentionSeconds`, `KeyHeader`, `KeyRequired` and `PendingWaitMilliseconds`. Unmarked endpoints are unchanged.
- README: lists the existing `ruvio.idempotency.states` counter and its tags.
- Less allocation per idempotent request: the request body is read into, and the response is buffered in, storage rented from the shared array pool (grown as needed, cleared when returned) instead of a new array each time, and the response buffer no longer reserves the full `MaxResponseBodyBytes` up front. Limits and behaviour are unchanged.
- `options.Outage`: choose what happens when Ruvio is unreachable (`Throw`, the default and unchanged; `FailClosed`, 503; `FailOpen`, the handler runs without idempotency) and optionally stop calling Ruvio after repeated failures with a circuit breaker. See the README.

## 0.1.2 - 2026-10-17

- `WithRuvioIdempotency()` endpoint extension, an alternative to the attribute for minimal APIs and groups.
- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
