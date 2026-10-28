[Ruvio](../../README.md) / [Docs](../README.md) / [Measurement records](README.md) / Shard scaling

# Shard scaling with a cluster-aware client, and the one-port path

`SET` and `GET` at 1, 4 and 8 shards on one Azure VM, Ruvio against Redis with the same client routing on both sides, plus the path of a client that does not route (every connection on one port, the server forwards). Raw data, every round: [benchmark/records/x86-azure-2026-10-26-shard-scaling.json](../../benchmark/records/x86-azure-2026-10-26-shard-scaling.json). Script: [benchmark/azure/shard-bench.sh](../../benchmark/azure/shard-bench.sh).

## Conditions (read before quoting any number)

- Host: Azure F16as_v6, AMD EPYC 9V74 (virtualized), 16 cores without SMT, Ubuntu 24.04. Servers on cpus 8-15, load on cpus 0-7, loopback. VM up 24 minutes.
- Ruvio: plain `release`, `memory` durability, shards pinned to cpus 8 to 7+N. "Now" is `bfc23fe` (the forwarding wake fix, merged to `dev` as #233); "before" is `a5a0a56`, the `dev` head before it. Redis 8.10.2 from source, no AOF, no RDB: one plain process for one shard, a Redis Cluster of N masters (no replicas) on cpus 8 to 7+N otherwise.
- Load: one `memtier_benchmark` 2.1.2, 8 threads, 7 connections per thread and node, pipeline 16, 32 B values, 100,000 keys, random key order, 5 s per row after a fill. Cluster-aware rows use `--cluster-mode` on both systems. Three rounds; the tables show the median.
- Noise: the same row moves by up to 2× between rounds (all rounds are in the JSON). At 8 shards both systems swing between about 5.3M and 8.6M from round to round: eight load threads are the limit there, so the 8-shard cluster-aware row says nothing about the servers and is not used below.

## Cluster-aware client

| Shards | Ruvio SET | Redis SET | Ratio | Ruvio GET | Redis GET | Ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.25M | 1.10M | 1.14× | 1.53M | 1.03M | 1.49× |
| 4 | 6.81M | 3.05M | 2.24× | 5.44M | 3.55M | 1.53× |

The `dev` binary before the fix gave the same cluster-aware numbers within noise (4 shards: 6.73M `SET`, 6.70M `GET`): the fix only touches forwarding.

## One port, a client that does not route

Every connection on the first shard's port; that shard forwards (N-1)/N of the commands. Before the fix the forwarding shard spent 38% of its time waking the other shards' threads (one wake per send); now it wakes each of them once per turn.

| Shards | SET before | SET now | GET before | GET now |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 0.65M | 0.88M | 0.66M | 1.05M |
| 8 | 0.67M | 1.22M | 0.54M | 0.99M |

This path stays below the cluster-aware one: one shard still reads and writes every socket. Clients that route (all six Ruvio clients do in cluster mode) do not take it.

## One shard, `redis-benchmark`

`redis-benchmark` 8.10.2, 4 threads, 50 clients, pipeline 16, 3,000,000 requests per row, three rounds (identical to the shown precision).

| Command | Ruvio | Redis | Ratio |
| --- | ---: | ---: | ---: |
| `SET` | 1.09M | 856K | 1.27× |
| `GET` | 1.09M | 999K | 1.09× |
| `INCR` | 1.09M | 923K | 1.18× |
| `LPUSH` | 923K | 923K | 1.00× |
| `LPOP` | 857K | 857K | 1.00× |
| `GET` of one key | 1.20M | 1.09M | 1.10× |

At about 1.09M the load generator may be the limit for Ruvio; the ratios are a floor.

## Why an earlier run on the same VM type looked different

A run earlier the same day sent all Ruvio load from one memtier to the first port with random keys, and gave Redis one memtier per process. That compares the one-port path of Ruvio with client-side partitioning on Redis; its shard-scaling numbers are not comparable and are not kept. Its single-shard rows came from one round with the server on cpu 0, which also carries the system's interrupts.

Not measured: more load threads at 8 shards, `everysec`, bare metal.
