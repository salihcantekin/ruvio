# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.OutputCaching`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `options.Outage`: `FailOpen` drops `EvictByTagAsync` calls instead of throwing when Ruvio is unreachable, and an optional circuit breaker stops requests from waiting on a Ruvio that is down. Default (`Throw`) unchanged. See the README.

## 0.1.2 - 2026-10-16

- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
