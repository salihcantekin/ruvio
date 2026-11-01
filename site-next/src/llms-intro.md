# Ruvio

> Ruvio is an in-memory key-value server written in Rust that speaks the Redis protocol (RESP2, and RESP3 after `HELLO 3`). `redis-cli` and existing RESP clients connect. Besides the usual strings, hashes, lists, sets, sorted sets, streams, JSON, bloom filters and geo, it has single commands for jobs that usually need a script or a second system: locks with fencing tokens, rate limits, request de-duplication, delayed jobs, cache refills and balance transfers.

## How to read this site

- Start with this file. It is small and lists every part of the documentation.
- When you need detail on one topic, open only that page's file below. Each one is plain Markdown and its size is shown next to it.
- Open `llms-full.txt` (under Optional) only when the task really needs many pages at once. It is large.
- Command syntax and limits are in the Reference pages (Commands, Redis Compatibility Contract). Ruvio's own commands (`LEASE`, `SEMAPHORE`, `RENEW`, `LIMIT`, `IDEM`, `ONCE`, `TAKE`, `TRANSFER`, `HTRANSFER`, `SETV`, `SET ... IFEQ|IFGT|IFLT`, `DELEX ... IFEQ`, `INCRBY ... MAX`, `FILLGET`/`FILLSET`, `ZPOPBYSCORE`, `ZMOVEDUE`, `LPUSHCAP`/`RPUSHCAP`, `XADD ... DELAY`, `CHANGES`, `CLIENT DURABILITY`) are not Redis commands; do not guess their arguments, read them there.

## Key facts

- A default process listens on `127.0.0.1:6379`. Quick start with Docker: `docker run --rm -p 6379:6379 -e RUVIO_REQUIREPASS=change-me -v ruvio-data:/data salihcantekin/ruvio:linux`. The image needs `RUVIO_REQUIREPASS` (or an ACL password) because its port is not loopback.
- Durability is chosen at startup: `memory` (the default), `snapshot`, `everysec` or `always`. Persistence needs `RUVIO_DURABILITY` and a data directory (`RUVIO_DATA_DIR`, `/data` in the image).
- Each key is owned by one shard. Commands with several keys need the keys in the same hash slot (use a `{tag}` in the key names).
- Replication, a cluster of 16384 slots, and automatic failover with `ruvio coordinator` are built in.
- Store money and balances as integers in the smallest unit (`INCRBY`, `TAKE`, `TRANSFER`), not with `INCRBYFLOAT`.
- License: server AGPL-3.0, client libraries MIT.
