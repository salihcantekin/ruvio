[Ruvio](../../README.md) / [Docs](../README.md) / Measurement records

# Measurement records

Each file is one dated measurement: the machine, the build, the method and the numbers as they were recorded that day. A record is not updated afterwards, so its commit, its numbers and its "open" notes describe that date only. A new measurement goes in a new file named `YYYY-MM-DD-topic.md`. Rules for measuring are in [PERFORMANCE](../PERFORMANCE.md) and [BENCHMARKING](../BENCHMARKING.md).

Current numbers are on the [performance](https://ruviodb.com/benchmarks/) and [memory](https://ruviodb.com/benchmarks/#memory) pages.

| Date | Record | What it measured |
| --- | --- | --- |
| 2026-10-26 | [Shard scaling](2026-10-26-azure-shard-scaling.md) | `SET`/`GET` at 1, 4 and 8 shards with a cluster-aware client against Redis and a Redis Cluster, the one-port forwarding path before and after its fix, and one-shard rows, on one Azure EPYC 9V74 VM |
| 2026-10-16 | [Eight shards against Redis](2026-10-16-azure-8-shards.md) | 60 commands and 12 data structures, Ruvio with eight shards against one Redis core and an eight-node Redis Cluster on one Azure EPYC 9V74 VM; one-shard control run |
| 2026-10-16 | [Client cluster routing cost](2026-10-16-client-cluster-routing.md) | CPU and allocation per request on the cluster path against the plain path, before and after, for all six clients on a 4-shard local server |
| 2026-10-11 | [Azure command comparison](2026-10-11-azure-command-comparison.md) | 60 commands and 12 data structures against Redis 8.10.2, throughput and memory, one Azure EPYC VM |
| 2026-10-01 to 2026-10-07 | [Benchmark results](2026-10-07-benchmark-results.md) | Native Redis comparison and native JSON vs RedisJSON on an Apple M4 Pro, capped Docker Desktop comparison, hash-field TTL, sorted-set pop, `SEMAPHORE` and `INCRBY MAX` commands |
| 2026-10-03 | [.NET scenario lab](2026-10-03-dotnet-scenario-lab.md) | k6 load on a .NET API using Ruvio or Redis through the C# client, persistence on and off, client multiplexing |
| 2026-09-28 | [Bare metal](2026-09-28-bare-metal.md) | Cherry EPYC 7313P Linux host: matrix against Redis, durability profiles, 15-minute soaks, `kill -9` trials |
| 2026-09-28 | [String memory density](2026-09-28-string-memory-density.md) | Where a string key paid memory in the Cherry soak, and the order of work that followed |
