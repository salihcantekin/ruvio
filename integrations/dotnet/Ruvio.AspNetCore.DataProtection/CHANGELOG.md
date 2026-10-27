# Changelog

Notable changes to the NuGet package `Ruvio.AspNetCore.DataProtection`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `ClientKey` option: names a connection registered with `AddKeyedRuvioClient`, so the key-ring repository can use its own server or database. Null (the default) keeps the unkeyed `IRuvioClient`.
- README: explains why the key ring has no outage mode (fail open or closed) like the other integrations.

## 0.1.2 - 2026-10-16

- Built against Ruvio.Client 0.3.3 (cheaper cluster routing, on-demand reply reading, opt-in `DedicatedReaderThread`). No change to this package's own API.

## 0.1.1

Earlier versions were not recorded in a changelog.
