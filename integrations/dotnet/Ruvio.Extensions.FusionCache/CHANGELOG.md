# Changelog

Notable changes to the NuGet package `Ruvio.Extensions.FusionCache`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- First version: `RuvioBackplane` (a FusionCache backplane over sharded Pub/Sub) and `WithRuvioBackplane()` on the FusionCache builder. Needs Ruvio.Client 0.4.0 and FusionCache 2.1.0 or newer.
