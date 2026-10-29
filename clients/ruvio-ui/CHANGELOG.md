# Changelog

Notable changes to `ruvio-ui`, the web console (Docker image and host binaries). New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- New **Cluster** screen, read only: groups (by each node's `cluster-group`, else by process), shards with every slot range, a slot map, primaries and replicas with how far each replica is behind, and the cluster map version every node serves.
- Optional coordinator view: set `RUVIO_UI_COORDINATORS` (`host:port,host:port`) and `RUVIO_UI_COORDINATOR_SECRET` to show the leader, each group's agreed primary, epoch, members answering and state, the slot move or rebalance in progress, and the failovers seen while the screen is open. The secret never reaches the browser, and the UI sends the coordinators only `INFO`, `SENTINEL masters`, `SENTINEL replicas` and `COORDINATOR STATUS`.
- Overview: commands per second, charged memory and connected clients as trend lines (kept while the tab is open), keys and memory per shard, a slot map, and a "Needs attention" list (a node not answering, a persistence error, a fenced primary, a replica whose link is down or that takes a full copy, nodes on different cluster map versions, slots not covered).
- Fixed: a shard that owns several slot ranges (after `COORDINATOR MOVE`, `COORDINATOR REBALANCE` or `RUVIO MOVESLOTS`) was listed once per range, so Overview counted its keys and memory more than once and a scan of the selected shards read it twice. Shards now come from `CLUSTER SHARDS`, numbered by their first slot, and keys and memory are summed over primaries only.
- Fixed: one slow or unreachable node no longer holds up every other request. Each connection has its own lock, node readings are taken in parallel with a 1 s connect and 2 s read limit, and a node whose connect failed is not dialled again for 3 s.
- After a failover or slot move raises the cluster map version, the UI reads the topology again by itself.

## 0.2.0 - 2026-10-29

- TLS to the target: set `RUVIO_UI_TLS=true` to connect to the Ruvio server over TLS, with the certificate chain and host name checked. `RUVIO_UI_TLS_CA` (PEM file of the CA to trust), `RUVIO_UI_TLS_CLIENT_CERT` and `RUVIO_UI_TLS_CLIENT_KEY` (mutual TLS), `RUVIO_UI_TLS_SERVER_NAME` and `RUVIO_UI_TLS_INSECURE=true` (skip verification, tests only) refine it. Without `RUVIO_UI_TLS` the UI connects as before.
- Security: `GET /api/defaults` no longer returns `RUVIO_UI_TARGET_PASSWORD`. The UI applies it itself when you sign in to the configured target host and port with an empty password field, so the sign-in form now shows an empty password. API calls without an `x-ruvio-session` header are refused (the page and its assets do not need one) instead of sharing one `default` session. Every response carries `X-Content-Type-Options`, `X-Frame-Options: DENY`, a `frame-ancestors 'none'` content security policy and `Referrer-Policy`. The UI still connects to any host and port typed into the sign-in form; publish its port only to machines you trust.
- New workbench form for `SET … IFGT` and `IFLT` (keep the highest or lowest number). The forms for TRANSFER, HTRANSFER, RENEW, ZPOPBYSCORE, ZMOVEDUE, LIMIT SLIDING and TOKENS, and the lifetime on INCR were already there.

## 0.1.9 - 2026-10-17

- New Dusk Rust theme, matching the project site. Every color and font is in `web/src/tokens.css`.
- New **Console** screen: run server commands with formatted, size-capped replies, copy and download of the full result, confirmation before destructive commands, history, and full screen.
- Command arguments are always sent as text. Setting an expiry no longer fails with "invalid type: integer".
- Shard health is collapsible and starts collapsed; shard notices moved to a header button.
- When the server names its shards `127.0.0.1` and you signed in with another host, shards are reached through that host. See `docs/UI_CONNECTIONS.md`.

## 0.1.8 - 2026-10-17

No recorded changes.

## 0.1.7

Earlier versions were not recorded in a changelog.
