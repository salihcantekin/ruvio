# Changelog

Notable changes to the NuGet package `Ruvio.Extensions.Idempotency`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `RuvioIdempotentExecutorOptions.AbortOnException` (off by default): when the work throws or is cancelled, the pending record is given up with `IDEM ... ABORT`, so the next call with the same key runs at once instead of getting `Pending` until the retention ends. The exception still reaches the caller; a failed abort leaves the record pending. Each freed record counts as `outcome = aborted`. Needs a server with `IDEM ... ABORT`.
- First version: `RuvioIdempotentExecutor.ExecuteOnceAsync` runs work once per key outside HTTP (message consumers, background jobs, gRPC) with the native `IDEM` command and replays the stored result (bytes, JSON through `JsonTypeInfo<T>`, or none) to repeats; `Pending` and `Conflict` report a running or failed attempt and other input under the same key. `AddRuvioIdempotentExecutor` registers it (`KeyPrefix`, `DefaultRetention`, `ClientKey`). Counter `ruvio.idempotency.outcomes`.
