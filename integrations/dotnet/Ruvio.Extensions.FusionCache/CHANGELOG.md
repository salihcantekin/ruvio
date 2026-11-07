# Changelog

Notable changes to the NuGet package `Ruvio.Extensions.FusionCache`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Docs: the README says what was checked against a server restart, two cluster nodes and a failover. After a failover the Pub/Sub subscription is not restored (the client retries the old primary's address); this corrects the earlier text.
- First version: `RuvioBackplane` (a FusionCache backplane over sharded Pub/Sub) and `WithRuvioBackplane()` on the FusionCache builder. Needs Ruvio.Client 0.4.0 and FusionCache 2.1.0 or newer.
