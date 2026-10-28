[Ruvio](../README.md) / Scripts

# Scripts

## Which command when

| When | Command |
| --- | --- |
| Before every push | `check.sh` (`check.sh --install-hook` runs it from `git push`) |
| Tests of one area | `test_area.sh <store\|protocol\|engine\|persistence\|server\|client-rust\|client-csharp> [filter]` |
| A client suite against live servers | `run_with_servers.sh <ruvio-binary> <base-port> -- <test command>` |
| Full Rust suite (several minutes) | `test.sh` / `test.ps1`, or `cargo test --lib --bins --tests` |

Build tests in the debug profile, or with `--profile release-fast` when a script needs optimized code; the `release` profile is only for published measurements. [docs/DEVELOPING.md](../docs/DEVELOPING.md) has the details and [docs/TESTING.md](../docs/TESTING.md) says which checks CI runs.

## All scripts

Most scripts take the path to a built `ruvio` binary and start their own server. Shared helpers (RESP framing, `free_port`, `wait_for_port`) are in `_ruvio.py`; a new script imports them from there, and an older script with its own copy moves over when it is next changed. Read a script's header before running it; benchmark scripts expect a quiet machine, and fault scripts marked "root" change the host. One-off experiments that produced a recorded result once are in [attic/](attic/README.md).

### Checks and gates

| Script | Purpose |
| --- | --- |
| `check.sh` | Pre-push gate: matrix drift, `cargo check --all-targets`, clippy, the standards tests, the zero-cost guards |
| `test.sh`, `test.ps1` | Format, the whole Rust suite and a store smoke; options for clippy and resilience runs |
| `test_area.sh` | Tests of one area, or a client suite against `RUVIO_HOST:RUVIO_PORT` |
| `run_with_servers.sh` | Starts a standalone server and a 3-shard cluster, exports `RUVIO_TEST_ADDR`, runs a command |
| `run_client_suites.py` | Rust and C# client suites plus the .NET integrations against one server |
| `check_compatibility_matrix.py` | Matrix against the parser, the table, the docs, coverage files, client manifests, and command counts stated in the site and READMEs |
| `check_instruction_baseline.py` | iai-callgrind counts against `benchmark/baselines/instructions.json` (PR gate, +2%) |
| `check_hygiene.sh` | Scans the lines a branch adds (`check_hygiene.sh . origin/dev`, after `git add -N .`) or the whole tree for planning ids, Turkish text and misnamed tests |
| `check_package_metadata.py` | Package descriptions, links and licenses (Python 3.11+) |
| `resp_conformance.py` | RESP2 conformance smoke |
| `redis_differential.py`, `geo_differential.py`, `slab_fuzz.py` | Replies compared with a Redis instance |
| `cluster_smoke.py`, `hot_keys_smoke.py` | Multi-shard routing smokes |
| `run_sanitizers.sh` | AddressSanitizer and Miri (nightly, Linux) |
| `test_*.py`, `test_ceiling_logic.sh` | Unit tests of the scripts themselves |

### Release

| Script | Purpose |
| --- | --- |
| `release_version.py` | Bumps or sets a package version in every manifest, the matrix pin, Python's `__version__` and the package changelog; `--install-pins` moves the server download line |
| `central_upload.sh` | Builds and uploads the Maven Central bundle |
| `release_artifacts.sh`, `generate_sbom.py` | Checksums and a CycloneDX SBOM (run by hand; the release workflow does not call them yet) |
| `collect_third_party_notices.py` | Writes `licenses/THIRD_PARTY_NOTICES` |

### Durability and faults

| Script | Purpose |
| --- | --- |
| `model_check.py` (+ `model_check_shim.c`, `model_check_mutants/`, `model_check_repros/`) | Model-based randomized checker with crashes and fault injection |
| `durability_scenarios.py`, `kill9_harness.py`, `kill9_shards.py` | Crash and restart checks per durability profile |
| `coordinator_faults.py` | `ruvio coordinator` failover under partitions (TCP proxy), a killed coordinator leader, no majority, restart and load; under a minute |
| `disk_faults.py`, `enospc_check.py` | WAL write errors, slow and full disks (Linux, root) |
| `native_resilience.py`, `docker_resilience.py` | Crash, recovery and filesystem faults on native processes or containers |
| `cdc_resume.py`, `worker_repl_check.py`, `json_replica_backlog_smoke.py`, `json_replica_cap_smoke.py` | CDC resume, replication with the sync worker, replica backlogs |
| `dump_restore_check.py`, `fill_stampede.py` | DUMP/RESTORE round trip, FILLGET single flight |
| `soak.py`, `memory_backpressure.py`, `tls_storm.py` | Soak, a non-reading client, TLS reconnect storms |

### Benchmarks and profiling

| Script | Purpose |
| --- | --- |
| `redis_compare.py`, `redis_compare_features.py`, `docker_compare.py` | Ruvio vs Redis tables published on the site |
| `gen_client_keys.py` | Writes each client's command key table from `clients/testdata/commands.json`; `--check` runs in `check.sh` |
| `client_stubs.py` | Prints a method skeleton per client, and the `command_methods.json` row, for a command already in `clients/testdata/commands.json` |
| `bench_matrix.py`, `perf_matrix.py`, `benchmark.sh`, `benchmark_regression.py`, `pinned_run.py` | Throughput matrices and repeatable samples |
| `bench_added_commands.py`, `bench_scripted_equivalents.py`, `bench_checkpoint_latency.py`, `bench_concurrency_durability.py`, `json_compare.py` | Feature, checkpoint, durability and JSON costs |
| `benchmark_shards.py`, `benchmark_shard_skew.py`, `fairness_multishard.py`, `fairness_probe.py` | Shard scaling and tail fairness |
| `memory_churn.py`, `churn_benchmark.py`, `compare_lru_reads.py`, `rss_docker.py`, `mixed_docker.py` | Memory, eviction and mixed workloads |
| `ceiling.sh`, `ceiling_compare.py`, `ceiling_logic.sh` | Single-core ceiling |
| `perf_compare.py`, `command_cost.py`, `mac_cost.py`, `profile_linux.sh` | Instructions, counters and CPU per request |
| `hot_symbols.py`, `pgo.sh` | Hot-function link order, profile-guided build experiments |
