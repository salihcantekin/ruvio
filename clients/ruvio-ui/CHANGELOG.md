# Changelog

Notable changes to `ruvio-ui`, the web console (Docker image and host binaries). New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

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
