# Ruvio

Ruvio is a memory-first key-value server that speaks the Redis protocol (RESP2). `redis-cli` and existing RESP clients connect to it. It adds jobs that usually need a second system: leases and fencing tokens, semaphores, conditional writes, a change feed and delayed stream entries.

- Source: https://github.com/salihcantekin/ruvio
- Documentation: https://ruviodb.com/
- Supported commands: https://ruviodb.com/docs/commands/
- Performance and memory results: https://ruviodb.com/benchmarks/ and https://ruviodb.com/benchmarks/#memory
- Security reports: https://github.com/salihcantekin/ruvio/blob/main/SECURITY.md

## Run it

```bash
docker run --rm -p 6379:6379 \
  -e RUVIO_REQUIREPASS=change-me \
  -v ruvio-data:/data \
  salihcantekin/ruvio:linux

redis-cli -h 127.0.0.1 -p 6379 -a change-me PING
```

The default durability is `memory`. For a persistent deployment set `RUVIO_DURABILITY` (`snapshot`, `everysec` or `always`) and keep the `/data` volume. Settings are described at https://ruviodb.com/docs/configuration/.

## Tags

`linux` and `latest` are the same image (linux/amd64 and linux/arm64). A version tag such as `0.3.0` is the same image for that release.

## Not claimed

Ruvio implements a listed subset of Redis, not the full surface. There is no cluster bus or online slot migration. Automatic failover of a primary and its replicas runs in separate `ruvio coordinator` processes from the same image (`docker run … salihcantekin/ruvio coordinator --config …`); see the coordinator docs.

## License

The server is licensed under the GNU Affero General Public License v3.0 (`AGPL-3.0-only`); the text is in the image at `/usr/local/share/ruvio/LICENSE`. Third-party notices are in `/usr/local/share/ruvio/THIRD_PARTY_NOTICES`. The client libraries are MIT licensed.
