# Connecting the Operator UI to Ruvio

The Operator UI is a small web server. It opens its own connection to Ruvio and
shows the result in your browser, so the address you give it is an address *as
seen from the UI process*, not from your browser. This page covers the setups
people actually use.

| Setup | Host to enter in the UI |
| --- | --- |
| UI binary and Ruvio on the same machine | `127.0.0.1` |
| UI in Docker, Ruvio as a binary on the same machine | `host.docker.internal` |
| UI and Ruvio in one Docker Compose file | the Ruvio service name, for example `ruvio` |
| Ruvio on another machine | that machine's name or IP |

## Ruvio's own address

A Ruvio binary listens on `127.0.0.1:6379` unless told otherwise. That address
is reachable only from the same machine, and a Docker container is not the same
machine. To let a container reach it, bind a non-loopback address and secure it:

```text
# Linux or macOS
./ruvio --address 0.0.0.0:6379 --requirepass change-me

# Windows PowerShell
.\ruvio.exe --address 0.0.0.0:6379 --requirepass change-me
```

The same settings exist as environment variables (`RUVIO_ADDR`,
`RUVIO_REQUIREPASS`) and in `ruvio.toml` (`address`, `security.requirepass`).
Without a password, a non-loopback bind keeps protected mode on and rejects
clients that are not on loopback, which includes the container. On a network you
already trust you can instead set `RUVIO_ALLOW_UNAUTHENTICATED=true` and
`RUVIO_PROTECTED_MODE=false`.

Binding to all addresses can make Windows Defender Firewall ask for permission.
Allow it for private networks, or add a rule for the ports Ruvio uses:

```text
New-NetFirewallRule -DisplayName "Ruvio" -Direction Inbound -Protocol TCP -LocalPort 6379-6380 -Action Allow
```

## UI in Docker, Ruvio on the same machine

```text
docker run --rm -p 8080:8080 \
  -e RUVIO_UI_TARGET_HOST=host.docker.internal \
  -e RUVIO_UI_TARGET_PORT=6379 \
  -e RUVIO_UI_TARGET_PASSWORD=change-me \
  salihcantekin/ruvio-ui:linux
```

Open `http://127.0.0.1:8080`. Inside the container `localhost` is the container
itself; `host.docker.internal` is the machine that runs Docker. Docker Desktop on
Windows and macOS defines that name. On Linux add
`--add-host=host.docker.internal:host-gateway` to the `docker run` command.

The `RUVIO_UI_TARGET_*` variables only fill in the sign-in form. The UI sends `RUVIO_UI_TARGET_PASSWORD` to Ruvio when you sign in to that host and port with an empty password field; it is never sent to the browser. Anyone who can open the UI port can sign in that way, so publish the port only to machines you trust (`-p 127.0.0.1:8080:8080`). You can leave
them out and type the host, port and password on the sign-in screen. When
`RUVIO_UI_TARGET_HOST` or `RUVIO_UI_TARGET_PORT` is set, it overrides the
address the browser remembers from an earlier sign-in. If the form still shows
an old host, update the UI image.

If you do not need a container, the UI is also released as a plain binary for
Windows, macOS and Linux. It runs on the same machine as Ruvio and connects to
`127.0.0.1`, with no Docker networking involved.

## More than one shard

A server started with `--shards 2` listens on the bind port and the next one:
shard 0 on `6379`, shard 1 on `6380`. You only sign in to one of them. Any shard
port serves every key, because a command for a key owned by another shard is
forwarded to it.

After sign-in the UI asks that server for its shard map (`CLUSTER SLOTS`) and
then opens a connection to each shard at the host and port the server names.
That is what fills the Shard health cards.

- The host in the map is the server's `advertised_host` (`RUVIO_ADVERTISE_HOST`,
  `--advertised-host`), which is `127.0.0.1` by default. Set it to a name the UI
  can reach: `host.docker.internal` for a UI container on the same machine, the
  service name in Docker Compose, or the machine's name for a remote server.
- If the server still names `127.0.0.1` and you signed in with another host, the
  UI reaches the shards through the host you signed in with and shows a notice.
  This is a fallback; setting `advertised_host` is the permanent fix.
- Every shard port must be reachable, not only the one you sign in to: publish
  `-p 6379-6380:6379-6380` for a Ruvio container, and allow both ports in the
  firewall. If a published port differs from the bind port, also set
  `RUVIO_ADVERTISE_PORT` to the published port of shard 0 (shard `i` is that
  port plus `i`).

Two shards in one server, UI in Docker, Windows:

```text
.\ruvio.exe --address 0.0.0.0:6379 --shards 2 --requirepass change-me --advertised-host host.docker.internal
```

## When it does not connect

| Message or sign | Likely cause | Fix |
| --- | --- | --- |
| `Connection refused` for every shard, sign-in worked | The server names its shards `127.0.0.1` and the UI is not on that machine | Set `RUVIO_ADVERTISE_HOST`; update the UI to a release with the loopback fallback |
| `Connection refused` at sign-in | Ruvio binds only `127.0.0.1`, or the firewall blocks the port | Bind `0.0.0.0`, allow the port |
| `NOAUTH` or `DENIED Ruvio is running in protected mode` | Non-loopback client without a password | Set `requirepass` and give the UI the password |
| Only one shard appears, notice says cluster mode is disabled | The server is older than 0.3.3, or runs one shard | Run Ruvio 0.3.3 or newer with `--shards 2` |
| A shard shows `Up` but empty metrics | Another program owns that port, for example a Redis on `6380` | Free the port or move the other program |
| `host.docker.internal` does not resolve (Linux) | Docker on Linux does not define it | Add `--add-host=host.docker.internal:host-gateway` |

See also [SETUP](SETUP.md), [CONFIGURATION](CONFIGURATION.md) and the
[connection model](CONNECTION_MODEL.md).
