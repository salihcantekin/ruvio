[Ruvio](../../README.md) / [Docs](../README.md) / [Measurement records](README.md) / Azure short check

# Azure: instruction counts and a short comparison against Redis

One short run on one Azure x86 VM: the `hot_path` instruction counts against the committed baseline, then a one-round `redis-benchmark` comparison of eight commands at pipeline 16 and pipeline 1, with Redis 8.10.2 on the same host. Raw data: [x86-azure-2026-11-27-short-p16.json](../../benchmark/records/x86-azure-2026-11-27-short-p16.json), [x86-azure-2026-11-27-short-p1.json](../../benchmark/records/x86-azure-2026-11-27-short-p1.json), [x86-azure-2026-11-27-hot-path-instructions.log](../../benchmark/records/x86-azure-2026-11-27-hot-path-instructions.log). Script: [scripts/redis_compare.py](../../scripts/redis_compare.py).

## Conditions (read before quoting any number)

- Host: Azure VM `Standard_D4ads_v6`, AMD EPYC 9V74, 4 vCPUs, 16 GiB, Ubuntu 24.04, kernel 7.0.0-1020-azure. Not pinned. The client and both servers share the four vCPUs.
- Ruvio: `dev` at `3976f145`, plain `release` profile (no PGO), default settings (memory durability, 1 shard). Redis 8.10.2 built from the source tarball with `make`, default settings.
- Load: `redis-benchmark` 8.10.2, 50 clients, 64 B values, 1,000,000 requests per command, **one round** per pipeline depth (server order not alternated), memory after 200,000 unique 64-byte strings.
- Instruction counts: `cargo bench --bench hot_path` (iai-callgrind 0.16.1, valgrind 3.22.0), compared with `scripts/check_instruction_baseline.py`.
- Not measured: more than one round (no spread, no median), latency (p50, p99), durability on, more than one shard, other commands, PGO builds, a separate client machine. One round on a shared VM can move a row by several percent; do not read a ratio of 1.00 to 1.10 as a win.

## Instruction counts

All six commands equal the baseline exactly (0.0 % over it, limit +2 %).

| Command | Instructions | Baseline |
| --- | ---: | ---: |
| `get` | 477 | 477 |
| `hset` | 671 | 671 |
| `incr` | 836 | 836 |
| `lpush` | 602 | 602 |
| `set` | 690 | 690 |
| `zadd` | 1,841 | 1,841 |

## Throughput, pipeline 16

Requests per second in thousands.

| Command | Ruvio | Redis | Ruvio / Redis |
| --- | ---: | ---: | ---: |
| SET | 1,376 | 1,258 | 1.09× |
| GET | 1,460 | 1,497 | 0.98× |
| INCR | 1,517 | 1,420 | 1.07× |
| LPUSH | 1,292 | 1,122 | 1.15× |
| LPOP | 1,185 | 1,079 | 1.10× |
| SADD | 1,548 | 1,425 | 1.09× |
| HSET | 1,548 | 1,229 | 1.26× |
| ZADD | 1,582 | 1,253 | 1.26× |

`GET` is the one row below 1.00× (0.98×), a gap this single round cannot tell from noise. Earlier Azure records with alternating rounds are the better source for per-command ratios.

## Throughput, pipeline 1

Requests per second in thousands. Every row, for both servers, sits between 94 and 98 thousand. That is the limit of this harness on this VM (50 clients on loopback with the client sharing the four vCPUs), not the speed of either server, so these ratios (0.97× to 1.00×) say only that neither server was the bottleneck.

| Command | Ruvio | Redis | Ruvio / Redis |
| --- | ---: | ---: | ---: |
| SET | 94.9 | 97.1 | 0.98× |
| GET | 95.2 | 96.5 | 0.99× |
| INCR | 94.1 | 97.1 | 0.97× |
| LPUSH | 96.1 | 96.8 | 0.99× |
| LPOP | 96.3 | 97.7 | 0.99× |
| SADD | 96.4 | 96.6 | 1.00× |
| HSET | 95.3 | 97.0 | 0.98× |
| ZADD | 95.9 | 95.9 | 1.00× |

## Memory

Resident set size, 200,000 unique 64-byte strings (the pipeline-16 run; the pipeline-1 run gave the same within 0.3 %).

| | Ruvio | Redis |
| --- | ---: | ---: |
| Idle | 11,892 KiB | 8,824 KiB |
| After 200,000 strings | 30,416 KiB | 35,016 KiB |
| Growth | 18.1 MiB | 25.6 MiB |

Growth per key is 0.71× of Redis; the idle size is 3 MiB larger. At this key count the idle difference is part of the total, so the ratio of totals (0.87×) is closer to one than the ratio of growth.
