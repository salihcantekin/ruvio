# Benchmark results

Published rows are the evented runtime. That is the only runtime.

## Hash field TTL and sorted-set pops, Apple silicon Mac, 2026-10-01

Same host as the section above. No Docker, no Redis comparison, not bare-metal Linux. Base is `e985d7c` (main). New is that build plus `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`, `ZPOPMIN`, `ZPOPMAX`, `BZPOPMIN`, `BZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, and `ZREMRANGEBYLEX`. Release binaries, default features (evented, mimalloc), durability `memory`, 1 shard, loopback.

Throughput is paired: both servers run at once on two ports, and each `redis-benchmark` run alternates between them, 15 pairs per command, `-n 1000000 -c 50 -P 16 -d 64 -t TEST`, fixed key.

| Command | Base median | New median | Paired new/base median | New ahead |
| --- | ---: | ---: | ---: | ---: |
| SET | 2,577,320 | 2,518,892 | 0.993 | 6 / 15 |
| GET | 2,747,253 | 2,832,861 | 1.003 | 8 / 15 |
| INCR | 2,631,579 | 2,652,520 | 0.987 | 5 / 15 |
| HSET | 1,841,621 | 1,779,360 | 0.987 | 4 / 15 |
| ZADD | 2,061,856 | 2,012,072 | 0.989 | 5 / 15 |
| LPUSH | 1,795,332 | 1,769,912 | 0.983 | 4 / 15 |

Every new median sits inside the base range for its command, so this is no measurable change. `HSET`, `ZADD`, and `LPUSH` lean about 1–2% lower in this run; `SET` and `INCR`, whose code did not change, lean the same way. The laptop cannot resolve that.

New-only throughput, median of 3 fresh-server rounds, same flags: `HGET` on a plain hash 2,309,469; the same `HGET` with 100,000 timed hashes elsewhere in the shard 2,283,105; `HPEXPIRE ... FIELDS 1` 1,555,210; `ZPOPMIN` 2,257,336.

Memory is `ps` RSS after the load minus the empty process, fresh process per sample, 3 samples, base and new alternating. Every sample matched its median.

| Workload | Base | New |
| --- | ---: | ---: |
| `SET`, 1,000,000 × 64 B | +84.0 MiB | +84.0 MiB |
| `INCR`, 1,000,000 keys | +24.5 MiB | +24.6 MiB |
| `HSET`, 80,000 × 4 fields | +26.7 MiB | +26.8 MiB |
| `ZADD`, 80,000 × 4 | +6.8 MiB | +6.9 MiB |
| `HSET` 80,000 × 4, one field with a TTL | | +40.7 MiB |
| `HSET` 80,000 × 4, four fields with a TTL | | +47.3 MiB |

A hash without a field TTL keeps the main layout. A timed hash costs about 180 bytes for its first timed field and about 30 bytes for each further one, on this probe.

## SEMAPHORE and INCRBY MAX, Apple silicon Mac, 2026-10-01

Same host as the sections above. No Docker, no Redis comparison, not bare-metal Linux. Base is `99d5c91`; new adds `SEMAPHORE` and `INCRBY key n MAX limit`. Release binaries, evented, mimalloc, durability `memory`, 1 shard, loopback.

Existing commands, paired: both servers run at once, 7 alternating pairs per command, `-n 1000000 -c 50 -P 16 -d 64`, fixed key.

| Command | Base median | New median | Paired new/base | New ahead |
| --- | ---: | ---: | ---: | ---: |
| SET | 2,702,703 | 2,840,909 | 1.034 | 5 / 7 |
| GET | 2,801,120 | 2,754,821 | 0.983 | 2 / 7 |
| INCR | 2,739,726 | 2,777,778 | 1.000 | 3 / 7 |
| INCRBY | 2,840,909 | 2,816,902 | 1.003 | 4 / 7 |

Every new median sits inside the base range: no measurable change. The hot parser still takes `INCRBY key n` directly; only the five-argument `MAX` form goes through the general path.

New commands, median of 3 fresh-server rounds, same flags:

| Command | req/s |
| --- | ---: |
| `INCRBY cap 1 MAX <i64 max>` (accepted) | 1,834,862 |
| `INCRBY cap 1 MAX 0` (refused, no write) | 2,398,082 |
| `SEMAPHORE sem:__rand_int__ 4 60000`, `-r 1000000` (grants, mostly new keys) | 757,002 |
| `SEMAPHORE full 8 60000` (refused once full, no write) | 2,155,172 |

Memory: `ps` RSS after 100,000 keys minus the empty process. The `SET`, `LEASE`, `ONCE`, `LIMIT`, and `INCRBY ... MAX` rows are 3 fresh processes each, every sample equal to its median. The two `SEMAPHORE` rows are one later sample, after the first holder moved into the object.

| Workload | RSS | Per key |
| --- | ---: | ---: |
| `SET` 64-byte value | +9.0 MiB | 95 B |
| `LEASE` (no deadline) | +7.4 MiB | 78 B |
| `ONCE` (deadline, no state) | +17.7 MiB | 185 B |
| `LIMIT` (deadline, 8-byte count) | +16.5 MiB | 173 B |
| `SEMAPHORE`, 1 holder | +18.2 MiB | 190 B |
| `SEMAPHORE`, 4 holders | +23.4 MiB | 245 B |
| `INCRBY ... MAX` counter | +3.1 MiB | 32 B |

A semaphore with one holder keeps that holder in the object, so that case does not allocate a second slice. A second holder allocates an exact slice, 16 bytes each, with no spare capacity. The two semaphore rows are one later sample of that layout. The other rows are the three-sample run above them. Most of a semaphore's cost is still the deadline record every self-expiring key carries, the same as `ONCE` and `LIMIT`.

## Native Redis comparison, Apple silicon Mac, 2026-10-07

**Host/build:** Apple silicon Mac, 12 cores, 24 GiB, macOS 26.6.2, arm64. Native Ruvio 0.2.10 release binary rebuilt from `203f4fc` **with uncommitted TTL deadline-index accounting changes**; Redis 8.10.2. The working tree is not a reproducible commit. Both servers ran as separate processes on loopback, one Ruvio shard with `memory` durability, Redis with RDB and AOF disabled. This is not a WAL, Docker, multi-shard, or networked deployment comparison.

**Throughput:** `python3 scripts/redis_compare.py --out target/redis-compare`, three rounds, 50 clients, pipeline 16, 64-byte values, alternating Ruvio/Redis measurement order within each round. Medians of `redis-benchmark` requests/s; most scenarios ran 1,000,000 requests per round, `LRANGE 100/300/500/600` ran 200k/100k/60k/50k, and `MSET` ran 500k. Both processes remain up throughout the three rounds. The `LRANGE` lists were checked nonempty on both servers (`LLEN` 200k/300k/360k/410k in round one). Total comparison including the memory probe: 75.8 s.

| Command | Ruvio | Redis 8.10.2 | Ruvio / Redis |
| --- | ---: | ---: | ---: |
| PING (bulk) | 2.67M req/s | 2.67M req/s | 1.00× |
| SET | 2.61M req/s | 2.16M req/s | 1.21× |
| GET | 2.74M req/s | 2.38M req/s | 1.15× |
| INCR | 2.88M req/s | 2.57M req/s | 1.12× |
| LPUSH | 2.18M req/s | 1.79M req/s | 1.22× |
| RPUSH | 2.21M req/s | 1.88M req/s | 1.17× |
| LPOP | 2.02M req/s | 1.74M req/s | 1.16× |
| RPOP | 2.03M req/s | 1.80M req/s | 1.13× |
| SADD | 2.28M req/s | 2.35M req/s | 0.97× |
| HSET | 2.17M req/s | 1.82M req/s | 1.19× |
| SPOP | 2.51M req/s | 2.77M req/s | 0.91× |
| ZADD | 2.30M req/s | 1.88M req/s | 1.22× |
| LRANGE 100 | 0.22M req/s | 0.22M req/s | 1.00× |
| LRANGE 300 | 0.08M req/s | 0.07M req/s | 1.04× |
| LRANGE 500 | 0.04M req/s | 0.04M req/s | 1.03× |
| LRANGE 600 | 0.04M req/s | 0.03M req/s | 1.15× |
| MSET | 1.40M req/s | 0.43M req/s | 3.27× |

Rates are rounded; use the unrounded samples for ratios. Ratios within 0.90–1.10× are treated as ties given local host variation. These mostly fixed-key throughput tests do not establish keyspace scalability, tail latency, TTL churn, or a production performance advantage.

**Distinct-string memory:** separate fresh processes, one connection, pipeline 200, 1,000,000 unique keys `k0`…`k999999` and 64-byte values. The loader verified every `+OK` response and `DBSIZE` on each server before sampling `ps` RSS. RSS delta subtracts the idle process, not all host/runtime overhead.

| RSS | Ruvio | Redis 8.10.2 |
| --- | ---: | ---: |
| Idle | 9,440 KiB | 7,104 KiB |
| Loaded | 94,368 KiB | 139,408 KiB |
| Loaded − idle | 82.9 MiB | 129.2 MiB |
| Ruvio / Redis delta | 0.64× | |

**Equal-pass churn:** `python3 scripts/memory_churn.py --passes 2500 --pause-ms 10` in separate fresh processes, plus a `--mode data` run with the same parameters. The `full` mode adds reconnects and unread 20,000-PING bursts to the repeated SET/DEL work; `data` uses a persistent connection without those bursts. The reported RSS increments are relative to each process's own idle RSS; peaks were sampled every 50 ms and may miss transients.

| Workload | Ruvio time / Redis time | Ruvio peak RSS increase | Redis peak RSS increase | Post-FLUSHALL RSS increase (Ruvio / Redis) |
| --- | ---: | ---: | ---: | ---: |
| Full, 2,500 passes | 39.167 / 38.359 s | +1.094 MiB | +0.297 MiB | +1.188 / +0.438 MiB |
| Data only, 2,500 passes | 35.922 / 36.174 s | +0.297 MiB | +0.234 MiB | +0.391 / +0.375 MiB |

The distinct-string load favors Ruvio by RSS delta, but **mixed connection/slow-client churn favors Redis** in this short RSS probe. Data-only churn is much closer. After FLUSHALL Ruvio reports zero charged live bytes, yet RSS stays above baseline; this alone does not prove a leak or explain which runtime/allocator/socket buffers retained memory. Redis `used_memory` and Ruvio `charged_memory_bytes` have different definitions. A single host, one data distribution, sequential runs, and no p99 or sustained cross-platform profiling limit these conclusions.

## Capped Docker Desktop comparison, 2026-10-01

Docker Desktop on the same Apple silicon Mac. This is a Linux VM, not the native table above and not bare metal. Both containers had the same cap: `--cpus=4 --memory=2g --memory-swap=2g`. Probe names were `ruvio-cmp-20261001` (port 6391) and `redis-cmp-20261001` (port 6392). They were removed after the run. Do not treat these names as the local `ruvio` or `redis` containers.

Ruvio image `ruvio-cmp:20261001`, built from `3188283` with default features (evented, mimalloc). Process env: durability `memory`, 1 shard, `RUVIO_CONFIG=/tmp/ruvio.toml`, `RUVIO_DATA_DIR=/tmp`. Redis image `redis:8-alpine` 8.10.2, `--save "" --appendonly no --protected-mode no --bind 0.0.0.0`.

Throughput is one sample per command. Ruvio ran first, then Redis. Same `redis-benchmark` flags as the native comparison: 1,000,000 requests, 50 clients, pipeline 16, 64-byte values, fixed key. A ratio from 0.90× to 1.10× is inside the ±10% host drift seen on this machine.

| Command | Ruvio | Redis 8.10.2 | Ruvio / Redis |
| --- | ---: | ---: | ---: |
| PING (bulk) | 1,129,944 req/s | 991,080 req/s | 1.14× |
| SET | 1,033,058 req/s | 961,539 req/s | 1.07× |
| GET | 1,088,139 req/s | 882,613 req/s | 1.23× |
| INCR | 885,740 req/s | 992,064 req/s | 0.89× |
| LPUSH | 933,707 req/s | 534,188 req/s | 1.75× |
| RPUSH | 980,392 req/s | 824,402 req/s | 1.19× |
| LPOP | 998,004 req/s | 529,101 req/s | 1.89× |
| RPOP | 1,017,294 req/s | 964,320 req/s | 1.05× |
| SADD | 1,023,541 req/s | 928,505 req/s | 1.10× |
| HSET | 965,251 req/s | 884,956 req/s | 1.09× |
| SPOP | 906,618 req/s | 1,014,199 req/s | 0.89× |
| ZADD | 965,251 req/s | 943,396 req/s | 1.02× |
| LRANGE 100 | 967,118 req/s | 531,915 req/s | 1.82× |
| LRANGE 300 | 1,013,171 req/s | 519,481 req/s | 1.95× |
| LRANGE 500 | 977,517 req/s | 512,558 req/s | 1.91× |
| LRANGE 600 | 971,817 req/s | 526,316 req/s | 1.85× |
| MSET | 383,730 req/s | 458,295 req/s | 0.84× |

Memory inside the container is `VmRSS` from `/proc/1/status` (KiB), not macOS `ps` RSS. Both processes were empty, then loaded with 1,000,000 explicit keys `k0`…`k999999`, 64-byte values, pipeline 200. The delta is that live set.

| | Ruvio | Redis 8.10.2 |
| --- | ---: | ---: |
| Idle VmRSS | 11,796 KiB | 27,916 KiB |
| After 1,000,000 strings | 103,436 KiB | 156,268 KiB |
| Delta | 89.5 MiB | 125.3 MiB |
| Ruvio / Redis | 0.71× | |

## Native JSON vs RedisJSON, 2026-10-07

Apple silicon Mac, macOS 26.6.2 (arm64), native release binaries on loopback, no Docker. Ruvio build `7aebdaf` (evented, `memory` durability, one shard) versus Redis 8.10.2 with the installed `rejson.so` RedisJSON module (`--save "" --appendonly no`). `redis-benchmark` 8.10.2 measured three rounds per operation, alternating which server ran first. Rates below are median requests per second, not production guarantees. The clients used equivalent simple paths but different path syntax for scalar reads (`$.meta.value` / `$.score` on Ruvio, `.meta.value` / `.score` on RedisJSON). The commands and response formats are not identical in every case.

First workload: 20,000 distinct keys, each with the same 87-byte JSON object (`name`, `active`, `score`, and nested `meta`); 100,000 requests per operation per round, 50 clients, pipeline 16. Root SET overwrote keys, rather than creating a fresh 100,000-key live set. Field operations read or overwrite `score`.

| Operation | Ruvio req/s | RedisJSON req/s | Ruvio / RedisJSON |
| --- | ---: | ---: | ---: |
| Root SET | 877,193 | 813,008 | 1.08× |
| Root GET | 2,040,816 | 529,101 | 3.86× |
| Field GET | 1,388,889 | 510,204 | 2.72× |
| Field SET | 1,098,901 | 469,484 | 2.34× |

One `ps` RSS sample per process, measured after loading the 20,000 keys and before the timed operations: Ruvio 9,712 → 14,480 KiB (**+4,768 KiB**); RedisJSON 8,320 → 15,456 KiB (**+7,136 KiB**). These are process-level resident-memory deltas for this workload only, not bytes per JSON document or a peak-memory measurement.

Second workload: one document with a large `bulk` string and a small `meta.value` field. Sizes below describe the `bulk` string, so the complete JSON is slightly larger. Three alternating rounds, 25 clients, pipeline 8; `JSON.GET`/`JSON.SET` target the existing scalar field, and `JSON.MERGE` merges `{"value":"..."}` into `meta`. The 1 and 64 KiB samples used 20,000 field GETs, 20,000 or 5,000 field SETs and MERGEs, and 2,000 root GETs per round. The 1 MiB row uses the longer follow-up run: 100,000 field GETs/SETs, 2,000 MERGEs, and 5,000 root GETs per round. Field SET uses a 12-digit random string; MERGE updates that same field. Rates and p99 are medians across rounds.

| Bulk size | Operation | Ruvio req/s | RedisJSON req/s | Ruvio p99 ms | RedisJSON p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | Field GET | 1,176,471 | 434,783 | 0.167 | 0.431 |
| 1 KiB | Field SET | 952,381 | 377,359 | 0.207 | 0.495 |
| 1 KiB | Field MERGE | 800,000 | 408,163 | 0.255 | 0.455 |
| 1 KiB | Root GET | 2,000,000 | 500,000 | 0.071 | 0.367 |
| 64 KiB | Field GET | 196,078 | 434,783 | 1.023 | 0.431 |
| 64 KiB | Field SET | 135,135 | 416,667 | 1.479 | 0.455 |
| 64 KiB | Field MERGE | 138,889 | 454,545 | 1.447 | 0.415 |
| 64 KiB | Root GET | 125,000 | 38,462 | 0.095 | 2.935 |
| 1 MiB | Field GET | 1,298,701 | 420,168 | 0.119 | 0.447 |
| 1 MiB | Field SET | 1,111,111 | 359,712 | 0.159 | 0.527 |
| 1 MiB | Field MERGE | 5,571 | 400,000 | 35.647 | 0.423 |
| 1 MiB | Root GET | 7,112 | 2,491 | 0.247 | 54.143 |

**Interpretation:** the 1 MiB scalar field benefits from indexed compact patches, but structural `JSON.MERGE` rebuilds the document and is about **72× slower** than RedisJSON in this case. The 64 KiB document is below the 256 KiB indexing threshold; its field operations are about 2–3× slower than RedisJSON. Root reads win in these particular shapes, but this does not establish an advantage for every document. Short, highly pipelined single-document samples (especially 1–2,000 requests) can have coarse throughput resolution. RSS was not measured for the large-document comparison. JSON paths and `JSON.MERGE` behavior are not a blanket claim of RedisJSON feature compatibility. See [the JSON roadmap](../ROADMAP.md#opt-in-document-and-search-features) for remaining acceptance work.

## Bare metal

The Cherry EPYC 7313P run on 2026-09-28 — machine, pins, Redis comparison, durability before and after the WAL group-commit change, and the live `kill -9` harness — is in [2026-09-28-bare-metal.md](2026-09-28-bare-metal.md). The string RSS gap from that run, and how to shrink it, is in [2026-09-28-string-memory-density.md](2026-09-28-string-memory-density.md). Docker Desktop numbers above are not that evidence.