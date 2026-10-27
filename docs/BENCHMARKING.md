# Benchmarking

`./scripts/benchmark.sh` builds the evented server. That is the only runtime.

## Prerequisites

- Docker Desktop or another running Docker daemon.
- `redis-benchmark` and `redis-cli` on the host.

On macOS, both clients are installed by `brew install redis`.

## Native Redis comparison

```bash
cargo build --release --bin ruvio
python3 scripts/redis_compare.py --out target/redis-compare
```

**What it starts:** fresh native Ruvio (`memory`, 1 shard) and Redis servers on loopback. No Docker.

**Load:** 17 `redis-benchmark` tests with `-c 50 -P 16 -d 64`. Each test runs 3 rounds in alternating order, and the result is the median.

**Memory:** RSS after loading 1,000,000 64-byte strings.

**LRANGE check:** `LLEN mylist` is read after each LRANGE test, so a test cannot pass against an empty list.

**Outputs:** `results.json`, `table.md`, and `table.html` (the same rows as an HTML table).

**Run time:** about 75 s on an Apple silicon Mac.

**Options:**

- `--tests`, `--rounds`, and `--memory-keys` give a shorter probe.
- `--redis-server` and `--redis-benchmark` select the binaries.

## Run

```bash
./scripts/benchmark.sh
```

`Dockerfile.linux` builds that server. The script refuses to measure if `INFO ruvio_backend` is not `evented`. Docker runs do not need `seccomp=unconfined`.

```bash
CPU_LIMIT=1 ./scripts/benchmark.sh
```

The Redis side of this command remains a single Redis server. Treat it as a local scaling probe, not a Redis Cluster comparison.

For a balanced Ruvio-only scaling measurement, start a multi-shard server and run one load generator per shard concurrently:

```bash
python3 scripts/benchmark_shards.py \
	--base-port 6379 --shards 4 \
	--requests-per-shard 200000 --clients-per-shard 25 --pipeline 16
```

The script selects a deterministic hash tag owned by each shard, keeps client count constant per shard, and reports aggregate RPS plus the slowest shard p99. This avoids the uneven node distribution observed with `redis-benchmark --cluster`.

Tune the workload through environment variables:

```bash
REQUESTS=500000 CLIENTS=100 PIPELINE=32 DATA_SIZE=128 ./scripts/benchmark.sh
```

## Mac/Docker Regression Runs

Docker Desktop results are suitable for detecting large regressions on the
same machine and environment. They are not portable performance claims.

Collect at least five samples without rebuilding or restarting the containers
between samples:

```bash
SAMPLES=5 ./scripts/benchmark.sh
```

The run writes:

- one Redis and Ruvio CSV per sample,
- `environment.txt` with host, Docker, workload, image, and Git metadata,
- `summary.json` for machine comparison,
- `summary.csv` for quick inspection,
- one `INFO` snapshot per Ruvio shard.

The summary uses medians and retains sample minima and maxima. It also records
the median Ruvio/Redis throughput ratio as an environmental control; it does
not turn the run into a product comparison.
Redis-first and Ruvio-first order alternates between samples to reduce a fixed
thermal/order bias.

Create a machine-local baseline:

```bash
SAMPLES=5 \
WRITE_BASELINE=benchmark/baselines/mac-evented.json \
./scripts/benchmark.sh
```

Compare a later run:

```bash
SAMPLES=5 \
BASELINE=benchmark/baselines/mac-evented.json \
MAX_RPS_REGRESSION_PERCENT=10 \
MAX_P99_REGRESSION_PERCENT=15 \
./scripts/benchmark.sh
```

Thresholds are optional. The command exits unsuccessfully when a compatible
baseline exceeds one. Comparison is skipped, rather than producing misleading
deltas, when the host CPU/OS, Docker version, workload, resource limits,
backend, shard count, or allocator differs.

Use a fixed `RESULT_DIR` when another tool needs a predictable location:

```bash
RESULT_DIR=benchmark/results/current SAMPLES=5 ./scripts/benchmark.sh
```

Compare allocators without changing the workload:

```bash
RUVIO_ALLOCATOR=system CPU_LIMIT=1 ./scripts/benchmark.sh
RUVIO_ALLOCATOR=mimalloc CPU_LIMIT=1 ./scripts/benchmark.sh
```

The script:

1. Builds the evented image.
2. Starts `redis:8-alpine` with snapshots and AOF disabled.
3. Applies identical CPU and memory limits to both containers.
4. Warms both servers before measurement.
5. Measures `SET`, `GET`, and `INCR` with identical client and pipeline settings.
6. Repeats measurements according to `SAMPLES`.
7. Writes raw CSV, summaries, server metrics, and environment metadata under
   `benchmark/results/`.

Matching Redis on Docker Desktop is not a bare-metal result.

## Whole machine: shards against a Redis Cluster

`scripts/shard_compare.py` runs the 60-command comparison with the whole machine: Ruvio with N shards
in one process (`port_mode = "per-shard"`), a Redis Cluster of N masters on the same N cores, and one
Redis process on one core. One `redis-benchmark` per shard, pinned to its own client core, sends the
row's command to that shard's port with the keys inside a hash tag the shard owns, so no command
crosses shards and no client routes; rates are summed over the shards. The memory table loads N
copies of each shape into the N-shard systems, routed by slot, so a shard holds what the single
process holds.

```bash
python3 scripts/shard_compare.py --ruvio target/release/ruvio --redis-server redis-server \
    --redis-cli redis-cli --redis-benchmark redis-benchmark --shards 8 \
    --server-cpus 8-15 --client-cpus 0-7 --rounds 3 --out target/shard-compare
# --durability everysec --data-root /mnt/data   Ruvio everysec against Redis appendfsync everysec on a real disk
# --pipeline 64                                 when one client core per shard is the limit (it is for GET/SET)
# --only PUBLISH --no-memory                    one row, no memory table
```

Raise the open-file limit first (`ulimit -n 65536`): eight shards times fifty clients exceed 1024.
Linux only (`taskset`). The record of the first run, with a one-shard control run and the profiles
that explained the rows below 1×, is `docs/evidence/2026-10-16-azure-8-shards.md`.

## Client routing cost

Each client has the same tool for the CPU a request costs on the cluster path. Point it at a per-shard server (`shards = 4`, `port_mode = "per-shard"`); every tool prints `plain`, `cluster` and `one-node` rows (user and system CPU nanoseconds per request, allocation where the runtime reports it) at pipeline depths 16 and 64, plus plain depth 4, in about a minute. Run the clients one after another, never at the same time.

| Client | Command |
| --- | --- |
| Rust | `cargo run --release --example cluster_cost -- --port 7000` (in `clients/ruvio-client`) |
| C# | `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0 dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --cluster-cost 7000` (`RUVIO_DEDICATED_READER=1` measures `DedicatedReaderThread`) |
| Go | `RUVIO_COST_ADDR=127.0.0.1:7000 go test -run TestClusterCost -v -count=1` (in `clients/go`) |
| Java | `mvn -q test-compile`, then `java -cp target/classes:target/test-classes io.github.salihcantekin.ruvio.RoutingCostBench 127.0.0.1:7000` (in `clients/java`) |
| Node | `RUVIO_BENCH_ADDR=127.0.0.1:7000 npm run bench:cluster-cost` (in `clients/node`) |
| Python | `python benchmarks/bench.py --cluster-cost 7000` (in `clients/python`) |

`one-node` against `plain` is the routing code's own cost; `cluster` adds the writes and reads of the extra shards. The routing rules are in [CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md#routing-cost) and the latest numbers in [evidence/2026-10-16-client-cluster-routing.md](evidence/2026-10-16-client-cluster-routing.md).

## Single-core ceiling

The Mac alignment run (`-c 50 -P 16`, combined `set,get,incr`) and
`scripts/benchmark.sh` stay as they are. They are not a single-core ceiling.

`scripts/ceiling.sh` measures one shard thread on the host, with no Docker
client. It walks pipeline 16, 32, 64, 128 at 50 clients, then clients
100, 200, 400, separately for `SET`, `GET`, and `INCR`. A point that fills
90% of one core ends the search. A point under 105% of the previous point
ends that phase. Three samples at the candidate produce the median.

A command is `saturated` when the median server core percent is at least 90.
`python3 scripts/ceiling_compare.py left.json right.json` prints
`COMMAND left/right` only for commands that are `saturated` in both files
from this grid. A `client-bound` command is not a side of the ratio.
The historical Mac alignment number is not an input.

```bash
./scripts/ceiling.sh
python3 scripts/ceiling_compare.py \
  benchmark/results/ceiling-windows/summary.json \
  benchmark/results/ceiling-mac/summary.json
```

## Interpretation

The server executes on the shard owner. Redis in the Docker script is the
environmental control, not a product claim.

Do not compare only requests per second. Before making product claims, collect p50, p95, p99, and p99.9 latency, CPU utilization, RSS, allocation counts, and saturation behavior on a dedicated Linux host.

For hardware-counter and call-graph collection, follow [PROFILING.md](PROFILING.md).

## Allocation Churn

Exercise repeated SET/DEL reuse of byte-value buffers:

```bash
python3 scripts/churn_benchmark.py --port 6379 --operations 200000 --value-size 4096
redis-cli -p 6379 INFO | grep pooled_value_bytes
```

This workload is intentionally different from `redis-benchmark`: it tests allocator churn and the bounded shard-local buffer pool rather than steady-state hot-key throughput.