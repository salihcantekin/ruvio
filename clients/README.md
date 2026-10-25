<p align="center">
  <a href="https://ruviodb.com/"><img src="../docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio clients</h1>

<p align="center">First-party application clients for six languages. They speak RESP2, never send <code>CLIENT SETINFO</code>, cover every command in <code>docs/compatibility-matrix.json</code> and route a sharded server from one seed (Rust and C# discover it by default; Go, Java, Python and Node.js turn it on with an option).</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/">Clients guide</a> ·
  <a href="https://ruviodb.com/get-started/">Packages</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/README.md">Integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

## Choose a client

| Language | Package | Install | Tests |
| --- | --- | --- | --- |
| Rust | [`ruvio-client`](ruvio-client/README.md) | `cargo add ruvio-client` | `cargo test --manifest-path clients/ruvio-client/Cargo.toml` |
| C# | [`Ruvio.Client`](csharp/Ruvio.Client/README.md) | `dotnet add package Ruvio.Client` | `dotnet test` on `Ruvio.Client.Tests` and `Ruvio.Client.AspNetCore.Tests` |
| Go | [`clients/go`](go/README.md) | `go get github.com/salihcantekin/ruvio/clients/go` | `go test ./...` |
| Java | [`ruvio-client`](java/README.md) | `io.github.salihcantekin:ruvio-client` (Maven Central) | `mvn -B test` |
| Python | [`ruvio-client`](python/README.md) | `pip install ruvio-client` | `python -m pytest` |
| Node.js | [`ruvio-client`](node/README.md) | `npm install ruvio-client` | `npm test` |

The matrix counts a subcommand such as `CLIENT ID` as its own command; the client spec in `testdata/commands.json` has one entry per command name (`CLIENT`), so it lists fewer entries. The client count is the six language directories; `ruvio-ui` is not a client.

## Timeouts

Every client has the same two limits, both on by default. The connect timeout (5 s) bounds opening a socket. The command timeout (30 s) bounds the wait for a reply, so a half-open peer (the network dropped without a reset) fails a call instead of blocking it until the socket drops. Set the command timeout to the client's "off" value in the table to wait forever.

A blocking command gets its own wait on top of the command timeout, so it never fails just because it waited as asked: `BLPOP k 5` may take 5 s plus the command timeout. A wait of `0` (block forever) means no deadline for that call. The wait is read from: the last argument of `BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `BRPOPLPUSH` and `BLMOVE`, and the first argument of `BLMPOP` and `BZMPOP` (seconds, fractions allowed); `BLOCK ms` of `XREAD` and `XREADGROUP` (no `BLOCK`: not blocking); the last argument of `WAIT` and `WAITAOF`, and `WAIT ms` of `FILLGET key fill_ms WAIT ms` (milliseconds). An argument the client cannot parse also means no deadline. A pipeline or transaction that holds a blocking command gets the largest wait among them. A Pub/Sub (or `MONITOR`) connection never times out while it waits for messages.

| Client | Connect timeout (default 5 s) | Command timeout (default 30 s); off value | On expiry |
| --- | --- | --- | --- |
| Rust | `ClientOptions::connect_timeout` | `ClientOptions::command_timeout` (or `set_read_timeout`); `None` | `Error::Io` |
| C# | `ConnectTimeout` | `CommandTimeout`; `TimeSpan.Zero` | `TimeoutException` |
| Go | `Options.DialTimeout` | `Options.CommandTimeout` (or a context deadline); a negative value | `context.DeadlineExceeded` |
| Java | `connectTimeoutMillis` | `commandTimeoutMillis` (synchronous calls and the async futures); `0` | `RuvioException` |
| Python | `connect_timeout` | `socket_timeout` (sync client; asyncio: wrap calls in `asyncio.wait_for`); `None` | `ruvio.TimeoutError` |
| Node.js | `connectTimeoutMs` | `commandTimeoutMs`; `0` | `ConnectionError` |

## TLS

Plain TCP is the default everywhere. TLS is always an explicit choice, and once it is on the client checks the server's certificate chain and host name; skipping the check takes a separate, clearly named option. A server turns TLS on with `tls_cert` and `tls_key` (and `tls_client_ca` plus `tls_client_auth` to demand client certificates); see `docs/CONFIGURATION.md`.

| Client | Turn it on | Private CA / client certificate | Skip verification (tests only) |
| --- | --- | --- | --- |
| Rust | cargo feature `tls`, then `ClientOptions::tls = Some(TlsOptions::default())` | `TlsOptions::ca_pem`, `client_cert_pem`, `client_key_pem` | `TlsOptions::insecure_skip_verify` |
| Node.js | `tls: true`, `rediss://` or `ruvios://` | see the Node.js README | see the Node.js README |
| Go | `Options.TLS = &tls.Config{}` | `tls.Config.RootCAs`, `Certificates` | `tls.Config.InsecureSkipVerify` |
| C#, Java, Python | not yet | not yet | not yet |

The Rust `tls` feature pulls in `rustls` (with the `ring` provider) and `webpki-roots`; the default build of `ruvio-client` still has no dependencies. Without a private CA the built-in public roots are used; with `ca_pem` set only that CA is trusted. `ruvio-ui` is built with the feature (`RUVIO_UI_TLS`, see `docs/UI_CONNECTIONS.md`). A connect to a server whose certificate fails the check returns `Error::Io`; asking for TLS in a build without the feature returns `Error::Protocol`. Measured: the TLS tests in `clients/ruvio-client/tests/tls_live.rs` against live servers; the latency TLS adds: not measured.

Also here: [`ruvio-ui`](ruvio-ui/README.md), the browser operator UI (AGPL-3.0). The clients are MIT licensed ([license](LICENSE)).

Live tests skip unless `RUVIO_TEST_ADDR` is set; cluster tests need `RUVIO_CLUSTER_ADDR` (a server started with `shards = 3` and `port_mode = "per-shard"`). Details are in each client's README.

## C# and .NET

`Ruvio.Client.AspNetCore` registers one `RuvioClient` as `RuvioClient`, `IRuvioClient` (commands), and `IRuvioConnection` (reconnect and dispose).

Separately packaged [.NET integrations](../integrations/dotnet/README.md) add `IDistributedCache`, ASP.NET Core output caching, Data Protection key-ring storage and opt-in HTTP rate limiting and idempotency. They depend on the client; the client does not depend on framework integrations.

## Run the client suites

```bash
cargo build --bin ruvio
RUVIO_ADDR=127.0.0.1:16382 RUVIO_DATA_DIR=/tmp/ruvio-clients ./target/debug/ruvio
python3 scripts/run_client_suites.py --port 16382
```

The script runs the Rust and C# unit tests. Set `RUVIO_TEST_ADDR` (the script does this) so `clients/ruvio-client/tests/live.rs` also talks to that process.

## Connection behavior (Rust and C#)

- Pub/Sub uses a **second TCP socket**. `GET`, `SET`, `PING`, and `PUBLISH` stay on the command connection.
- A dropped socket, including a shard socket, is opened again before the next command. The command that sees the drop fails.
- A command whose socket dropped before any reply byte is not sent again, reads included, in every client; the next command opens the socket again.
- Subscriptions are sent again on the new Pub/Sub socket.
- A seed list (`Seeds` / `seeds`) can replace the single host and port in every client. Reconnects start with the seed after the one that dropped; a cluster reloads its slot map from another known node or seed. See [docs/CLIENT_BEHAVIOR.md](../docs/CLIENT_BEHAVIOR.md).
- A read policy (`primary` by default, `prefer-replica`, `replica`) lets a cluster client send read-only commands to the replicas the server lists in `CLUSTER SLOTS`. Replication is asynchronous, so such a read may miss recent writes. See [docs/CLIENT_BEHAVIOR.md](../docs/CLIENT_BEHAVIOR.md#reads-from-replicas).
- Retry is on by default. Set `Reconnect` / `reconnect` to false and call `ReconnectAsync` / `reconnect`, or pass a delay function. Null / `None` stops the retry.
- Connection lost and restored events carry the host, port, and whether the socket is the subscriber. ASP.NET Core binds the retry settings and can attach those events when the singleton opens.

## Tours

A tour of nearly every method, against a local server:

```bash
cargo run --manifest-path clients/ruvio-client/Cargo.toml --example tour -- 127.0.0.1 6379
dotnet run --project clients/csharp/Ruvio.Client.Tour -- 127.0.0.1 6379
```

> [!WARNING]
> On a standalone server the tour uses database 15. The C# tour finishes with `FLUSHALL`. Do not point either program at a server you need to keep.
