[Ruvio](../../README.md) / [Docs](../README.md) / [Evidence](README.md) / Eviction in a memory cgroup and six clients through failovers

# Eviction in a memory cgroup and six clients through failovers

Two local checks on one cloud container (Linux 6.18, 4 vCPUs, 16 GiB, cgroup v1 memory controller), debug build of `dev` at `3976f14`, Redis from the distribution package. Nothing here is a speed number: the server is a debug build and the load is a Python client. Not measured: release builds, other kernels, macOS or Windows, runs longer than the times below, Azure disks.

## Sustained writes at the `maxmemory` ceiling

`scripts/eviction_cgroup.py`: a fresh server in its own cgroup with a hard limit of 256 MiB, `maxmemory` 64 MiB, 2 shards, durability `memory`; four writers set 1 KiB values over a key space of about 308,000 keys for 30 s while a fifth thread rewrites and rereads 1,000 hot keys. For `volatile-*` policies 4,000 keys without a TTL are written first. The same load runs against `redis-server` with the same `maxmemory` and policy in its own cgroup.

| Policy | Ruvio RSS idle → peak | RSS middle third → last third | Evicted | Writes refused (`OOM`) | Policy check | Redis RSS idle → peak |
| --- | --- | --- | --- | --- | --- | --- |
| `allkeys-lru` | 33.5 → 87.7 MiB | 86.2 → 87.2 MiB | 228,899 | 0 | hot set 100 % kept | 14.3 → 76.8 MiB |
| `allkeys-lfu` | 33.2 → 87.7 MiB | 85.7 → 86.3 MiB | 226,822 | 0 | hot set 100 % kept | 14.3 → 76.8 MiB |
| `volatile-lru` | 37.1 → 86.6 MiB | 84.9 → 86.1 MiB | 249,779 | 7 of 614,585 | 4,000 keys without a TTL kept, hot set 100 % | 19.7 → 76.8 MiB |
| `volatile-ttl` | 37.5 → 86.8 MiB | 84.4 → 84.5 MiB | 332,552 | 0 | keys without a TTL kept; survivors' mean TTL 3,297 s against 1,849 s written | 19.7 → 76.9 MiB |
| `noeviction` | 32.8 → 81.7 MiB | 81.4 → 81.6 MiB | 0 | 4,459,595 | 3,000 of 3,000 sampled acknowledged keys present, reads served | 14.4 → 76.7 MiB |

In every run the process stayed alive, the cgroup recorded no OOM kill and no failed charge, and RSS stayed far under the 256 MiB limit and flat. The five-run repeat at 6 s per policy (25 policy runs, no Redis) passed every check.

What the numbers say, and what they do not:

- At the same `maxmemory` setting Ruvio's RSS peak is about 6 to 14 % above Redis's (81.7 to 87.7 MiB against 76.7 to 76.9 MiB). The setting is not the same budget: `maxmemory` bounds the data the shards hold in Ruvio ([MEMORY_ACCOUNTING](../MEMORY_ACCOUNTING.md)), and the total in Redis. The idle difference is 17 to 19 MiB in this debug build with two shards, larger than the peak difference of 5 to 11 MiB, so the growth under load was smaller in Ruvio (about 54 MiB against about 62 MiB). Whether a release build has the same idle baseline is not measured. The script reports a higher peak as a warning, not a failure, unless `--strict-redis` is given.
- `volatile-lru` refused 7 writes in 614,585 (0.001 %). [MEMORY_ACCOUNTING](../MEMORY_ACCOUNTING.md) says a `volatile-*` policy refuses a write when the 2 ms eviction time runs out; the script allows one in a thousand. An earlier run of the same policy refused 4.
- The write rate (about 20,000 per second for Ruvio, about 135,000 for Redis) comes from a debug build against a release Redis. It is not a comparison.

## Six clients through two failovers, TLS off and on

`scripts/client_faults.py`: three data nodes (`always`, one shard) and three coordinators (`down_after_ms` 1000, ping 200 ms) on free ports. Each client's probe is given the three node addresses as seeds (no coordinator name), sends `CLIENT DURABILITY REPLICAS 1`, writes `fault:<client>:<i>`, reads it back, and continues for 4 s; then the primary is killed with SIGKILL, the killed node comes back as a replica, 4 s later the new primary is killed, and so on once more. With TLS the data nodes serve TLS to clients and the coordinators dial them with `node_tls`; replication stays plain.

| TLS | Client | Acknowledged writes | Failed attempts | First acknowledged write after kill 1 / kill 2 | Longest gap between acknowledgements |
| --- | --- | --- | --- | --- | --- |
| off | Rust | 561 | 2 | 1.60 s / 1.60 s | 1.62 s |
| off | C# | 549 | 2 | 1.61 s / 1.59 s | 1.63 s |
| off | Go | 563 | 2 | 1.56 s / 1.56 s | 1.57 s |
| off | Java | 550 | 2 | 1.58 s / 1.57 s | 1.58 s |
| off | Python | 560 | 2 | 1.56 s / 1.55 s | 1.57 s |
| off | Node | 559 | 2 | 1.56 s / 1.56 s | 1.58 s |
| on | Rust | 554 | 2 | 1.67 s / 1.64 s | 1.67 s |
| on | C# | 547 | 2 | 1.65 s / 1.59 s | 1.66 s |
| on | Go | 553 | 2 | 1.58 s / 1.58 s | 1.60 s |
| on | Java | 541 | 2 | 1.71 s / 1.65 s | 1.73 s |
| on | Python | 550 | 2 | 1.59 s / 1.58 s | 1.60 s |
| on | Node | 551 | 2 | 1.61 s / 1.61 s | 1.63 s |

Every run: every acknowledged key was on the final primary with its value, no read returned another value, the probe stopped within 15 s of being asked, and each kill cost exactly one failed attempt (the request in flight). The check limit is 5 s to the first acknowledged write after a kill (the limit is an argument of the script, not a measured property). TLS added up to about 0.1 s to the recovery in these runs; one run per cell, so a difference that small is not established.

`TRYAGAIN` is a slot move, not a failover, and these runs do not move slots. The 635 ms retry budget (7 retries after 5, 10, 20, 40, 80, 160 and 320 ms; [CLIENT_BEHAVIOR](../CLIENT_BEHAVIOR.md)) was checked only with each client's scripted fake-server tests, which passed: Rust `fake_cluster` (3), C# `TryAgain` (3), Go `TestFake*TryAgain` (all), Java `ClusterUnitTest#tryAgain*` (3), Python `-k tryagain` (7), Node `cluster-edge` (19 in the file, 3 on `TRYAGAIN`). A live slot move with each client is not measured.

## Method notes

- The cgroup is created per run under `/sys/fs/cgroup/memory` and removed afterwards; the script needs root and says so when it cannot create one (exit 2).
- Both scripts take free ports below 50,000: a server with several shards also reserves the port 10,000 above its own and refuses to start when that is above 65,535. Ports above 55,535 made 1 in 5 starts fail until this was handled; the server's own message is `shard data or cluster bus port range exceeds 65535`.
