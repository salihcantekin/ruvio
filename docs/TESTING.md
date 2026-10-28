# Testing and Evidence

This page summarizes what has been exercised, how the checks are run, and
which production claims still need evidence. A test surface being present in
CI is not the same as a recent successful run. Each result below is labeled by
where and how it was obtained. Runs of hours on a multi-node cluster under load and
injected faults, made with [tests/soak](../tests/soak), are summarized on the
[long-run tests](https://ruviodb.com/testing/) page.

## Local gate

`scripts/check.sh` runs the tests that tie hand-kept lists to their one source in about
two minutes on a warm debug build: the command table against the parser, router, ACL
names, script and fan-out decisions; WAL record and snapshot kinds against the format
version; the settings template against `CONFIG GET`; `INFO` fields against `docs/OBSERVABILITY.md`; value kinds against `TYPE` and
`SCAN TYPE`; and the hot parser against the general one. It also checks the
compatibility matrix and builds every target. `scripts/check.sh --install-hook` runs it
before every `git push` from that clone. The toolchain is pinned in `rust-toolchain.toml`.

## Automated test surface

`.github/workflows/pr-gate.yml` runs automatically for pull requests and
pushes to `main` and `dev` on Linux. It checks Rust formatting and core tests,
`scripts/check.sh`, hot-path instruction counts, Python benchmark-regression
units, a real two-shard routing smoke, and every client suite (Rust, C# with the
.NET integrations, Go on its `go.mod` floor, Node, Python 3.9 and 3.13, Java)
against live servers started by `scripts/run_with_servers.sh`. Live client
tests skip themselves without `RUVIO_TEST_ADDR`, so a suite run without a
server proves only the offline tests. The script also starts three
`ruvio coordinator` processes over a two-node group, and each client has one
live test that kills that group's primary and follows the failover; it adds
about 5-8 s to each client suite (see `docs/CLIENT_BEHAVIOR.md`, *Tests*). The extended workflow at
`.github/workflows/ci.yml` is **manual-dispatch only**. When dispatched, it
defines these additional checks:

| Area | Checks in the workflow |
| --- | --- |
| Rust and portability | Formatting, Clippy with default and all features, all-target Rust tests, and a store smoke example on macOS, Linux, and Windows |
| Protocol and compatibility | RESP conformance, Redis differential behavior for the supported command subset, compatibility-matrix drift, and multi-shard routing smoke |
| Clients | Live-server test suites of all six clients (Rust, C# with the .NET integrations, Go with `-race`, Java, Node, Python), each against a standalone server and a 3-shard per-shard cluster |
| Applications | The C# layout check, a build of every .NET project, the C# and Rust tours against both servers, the site (`site-next`) and the operator UI (web build and host binary) |
| .NET integration packages | Package-specific tests and package builds, using a separate manually dispatched workflow; only the first-party .NET client and ASP.NET Core client tests are in the automatic gate |
| Persistence and recovery | WAL recovery, snapshots, checksums and corruption handling, missing-segment detection, TTL restart behavior, backup/restore, and frozen-format compatibility tests |
| Supply chain | `cargo-deny`, release-version checks, SBOM generation, and security-policy checks |
| Runtime safety | RESP parser fuzzing, AddressSanitizer library tests, and selected Miri tests |

When dispatching it, the `jobs` input picks jobs (`all`, or a comma list of
`test`, `supply-chain`, `container`, `clients`, `apps`, `fuzz`, `asan`, `miri`) and `os`
picks the system for the `test` job, so a change can be checked by the jobs that
cover it without paying for the rest. Pick `dev` as the branch in the Actions tab to check
the development branch before it goes to `main`.

See the workflow file for the exact commands and job boundaries. A local
re-run of the principal Rust suite is:

```bash
cargo test --lib --bins --tests
```

## Nightly fault scenarios

`.github/workflows/nightly.yml` (every night at 02:17 UTC from `main`, and by manual dispatch) runs the coordinator fault scenarios on real
processes and real network partitions, each scenario three times by default (`runs` input):

- `scripts/coordinator_faults.py ./ruvio partition leader minority restart loss durable stall coord-partition`:
  three data nodes, three coordinators and TCP proxies on one machine. `durable` expects no
  acknowledged write lost; `loss` allows only the last writes before the crash to be lost.
- `scripts/docker_partition.sh ./ruvio leader lease-off lease-on`: Docker networks cut with
  `docker network disconnect`. `lease-on` asserts that the old primary refuses writes before the
  other side promotes a replica and acknowledges none after its first refusal; `lease-off` prints the
  same numbers without asserting them.

Each job has a 25-minute limit and builds nothing but `--profile release-fast --bin ruvio`. A
failed scheduled run opens one issue labelled `nightly-failure` (later failures comment on it).
Both scripts run locally with the same arguments; the Docker one needs `docker`, `redis-cli` and
`python3`, and runs the binary in `ubuntu:24.04` (`RUVIO_DOCKER_IMAGE`), so build it on a glibc no
newer than that image's.

## Test rules

A test that fails or hangs once in a thousand runs is a broken test. There is no list of
known flaky tests and no re-run: a red run is investigated, the cause is fixed, and the fixed
test is run in a loop to show it. Every test, in Rust and in the six clients, follows these
rules.

**Time.** A unit test finishes in well under a second; a few build large tables and take a
few seconds. An integration test file takes seconds, and all of them together a few minutes.
CI runs the Rust tests with [cargo-nextest](https://nexte.st) (`.config/nextest.toml`): each
test in its own process, a unit test killed at 30 s and an integration test at 2 minutes, so a
hang fails with the test's name instead of stopping the job.

**Ports.** A test that starts a server hands it a listener it already bound to
`127.0.0.1:0`. When the server must bind by itself (a subprocess, ports per shard, a
replication listener, a restart on the same address) the test takes a block from
`test_ports::reserve()` and holds it for the server's whole life. A port picked by binding and
closing a socket, a fixed port, or a port in the OS's ephemeral range (Linux 32768-60999,
macOS and Windows 49152-65535) can be taken by a parallel test before the server binds it.

**Waiting.** No sleep followed by an assertion. A test waits for the condition itself (a
reply, an `INFO` field, a file) by polling it against a deadline of 15-30 s, which a passing
test never reaches. No upper bound on elapsed time unless the bound is far from both the
normal and the failing run; a counter is better. Every blocking read, accept, join or wait has
a deadline, so a bug fails the test instead of hanging it.

**Isolation.** Tests that run in parallel never share keys, databases, files, process-wide
flags or environment variables. Random input is seeded and the seed is printed on failure.

**Miri and sanitizers.** Miri runs only where unsafe code lives (the hash-table fork and the
slot tables on it), with sizes cut down under `cfg(miri)`; AddressSanitizer runs the library
tests and fails on any leak.

## Risk areas and their test groups

Each area below is a class of defect that has reached a release candidate before. The
groups are written to catch the class, not only the instance that was found: they walk the
whole command table, every scan command, every entry path or every encoding. Run all of them
on Linux with:

```bash
cargo test --lib --bins --tests
```

and one group by naming its file or filter, for example
`cargo test --test command_surface_live` or `cargo test --lib scan_churn`.

Build tests with the default debug profile, or `--profile release-fast` when a test needs
optimized code. Never use `--release` for tests: that profile is fat LTO with one codegen
unit, so every rebuild re-optimizes the whole crate (minutes instead of seconds). The same
goes for pass/fail scripts (`durability_scenarios.py`, `model_check.py`, `disk_faults.py` and
the other crash and fault checks), which default to `target/release-fast/ruvio`. Only
measurements that compare against Redis or publish numbers use `target/release`.

No test may hang. Every wait in a test has a deadline: sockets get read and write timeouts,
polling loops stop after a fixed time and fail with what they were waiting for, and a
spawned server is killed when the test ends. libtest reports any test still running after
60 seconds; treat that report as a bug in the test. `scripts/test_area.sh` stops cargo after
`RUVIO_TEST_TIMEOUT` seconds (default 600), and every CI job has a `timeout-minutes` limit,
so a hang fails the run instead of stalling it.

| Risk area | What must hold | Where it is tested |
| --- | --- | --- |
| Protocol limits | Every numeric argument of every command at the integer extremes neither aborts the server nor drops the connection; oversized frames, arguments and argument counts get an error and keep the connection; byte limits above 2 GiB are refused; input before AUTH is capped | `tests/command_surface_live.rs` (`every_command_survives_extreme_numbers_and_keeps_the_connection`, `oversized_input_before_auth_is_refused_and_the_server_keeps_serving`); `tests/parse_matrix.rs`; `random_draw_counts_past_the_limit_are_refused`, `protocol_limits_above_two_gib_are_rejected` and the `evented_oversized_*` / `evented_frame_with_too_many_arguments_*` tests in `src/protocol/tests.rs` and `src/runtime/evented/tests/connection.rs` |
| Authentication and ACL side doors | No command runs before AUTH, and a denied command or key is refused, directly, inside MULTI, from a script and for a key owned by another shard behind one port; feeds that name keys of every pattern are refused to key-restricted users; SLOWLOG hides passwords | `tests/command_surface_live.rs` (`no_command_runs_before_auth_on_any_entry_path`, `a_user_denied_a_command_is_refused_on_every_entry_path`, `a_key_restricted_user_cannot_reach_other_keys_on_any_entry_path`); `src/acl.rs`, `src/runtime/session.rs`, `src/runtime/evented/tests/single_port.rs`, `src/script.rs` |
| Iteration under change | SCAN, HSCAN, SSCAN and ZSCAN end and return every element present for the whole walk while tables convert, grow, migrate and shrink; random draws reach every element evenly in every encoding | `src/engine/tests/scan_churn.rs`; `src/store/tests/` (`*scan*`, `*random*` in `keyspace.rs`, `sets.rs`, `hashes.rs`, `sorted_sets.rs`), `src/slots.rs`, `src/engine/tests/zset.rs` |
| Persistence and crashes | A kill at any point recovers every acknowledged write as a prefix, expired keys stay expired, WAL write and sync failures (injected) keep the log consistent, torn tails are cut, checkpoint rounds never reuse or mix files, an interrupted restore blocks startup, ACL and config files are written atomically, temporary files are swept, a logged write replay cannot apply is an error | `tests/process_live.rs` (`a_kill_at_any_point_recovers_every_acknowledged_write_as_a_prefix`), `tests/recovery_live.rs`, `src/persistence/crash.rs` (`killed_process_recovers_at_crash_sites`), `src/persistence/wal.rs`, `src/persistence/snapshot.rs`, `src/persistence/tooling.rs`, `src/engine/tests/replay_failures.rs`, `src/runtime/evented/tests/wal.rs` |
| Replication | Mixed handshake versions fail loudly, a changed history forces a full resync, a replica never expires or evicts by itself | `src/replication.rs`, `src/runtime/evented/tests/replica.rs`, `src/engine/tests/expire.rs`, `src/engine/tests/evict.rs` |
| Memory limit | With an `allkeys` policy a write that fits is never refused while older keys can be evicted, for any value size, shard count and durability profile; the limit is split across shards; unsynced log bytes do not evict keys; output buffers, MULTI queues and Lua memory are capped | `tests/memory_limit_live.rs`, `tests/memory_budget.rs`, `src/engine/tests/evict.rs`, `src/output.rs`, `src/transaction.rs`, `src/script.rs` |
| Blocking clients and connections | A blocked client that hangs up (plain or TLS) gives up its waiter; idle servers do not spin; SIGHUP does not stop the server; running out of descriptors keeps existing clients and recovers for new ones | `src/runtime/evented/tests/hangups.rs`, `src/runtime/evented/tests/connection.rs`, `src/runtime/mod.rs`, `tests/process_live.rs` |
| Crash, restart and disk-fault contracts under a random workload | After kill -9, SIGTERM, a simulated power loss, a torn WAL write or an injected `EIO`/`ENOSPC` on WAL or checkpoint files, the recovered keyspace is a cut of the acknowledged history that the profile allows (`always`: every acknowledged write; `everysec`: all but the last sync window; `snapshot`: at least the last `SAVE`; `memory`: empty), with no holes, no resurrected or expired keys and multi-key commands all-or-nothing; no write is acknowledged after a WAL sync failure; live replies match a reference model; a replica converges to its primary | `scripts/model_check.py` (with the LD_PRELOAD fault shim `scripts/model_check_shim.c`); seeded smoke runs in `tests/model_check_live.rs` (`a_short_seeded_model_check_run_passes`, `the_model_checker_reports_a_server_that_loses_writes`) |
| Growth traps | One command over many elements, and many commands on one growing value, stay close to linear (ratio of thread CPU time at n and 8n) | `src/engine/tests/complexity.rs`; FLUSHALL and lazy-free hand-off in `src/lazy_free.rs` and `src/store/tests/shrink.rs`; hash field TTL sweeps in `src/store/tests/field_ttl.rs` |

The live groups (`tests/*_live.rs`) start the real `ruvio` binary on free local ports and
finish in seconds. A test marked `#[ignore]` names a known gap in its reason; run it with
`cargo test -- --ignored <name>`.

## Long runs on a multi-node cluster

[`tests/soak/`](../tests/soak/README.md) runs a two-group cluster (three hosts, each with one
coordinator and one member of each group) for minutes locally or hours on four Azure VMs built
from Bicep. Workers write counters, lists and registers with and without `CLIENT DURABILITY`
and record every operation; faults (killed or frozen primaries and replicas, a killed
coordinator leader, a whole host killed, a host cut off with nftables, netem delay and loss)
come on a schedule, and after the run every key is read back and the history is checked for
lost acknowledged writes, duplicates, values never written, order violations and stale reads.
A run writes `status.json` (and uploads it to Azure Blob when a SAS URL is configured). The
checker's unit tests run in the PR gate (`python3 tests/soak/test_soak.py`); the runs themselves
are started by hand. Local, debug build: `plans/local-smoke.json` (60 s, every local fault kind) ends with
no durable write lost (lost `default` writes around a killed primary are counted as warnings); `plans/local-restart-as-configured.json` reproduces
[#142](https://github.com/salihcantekin/ruvio/issues/142). The Azure path is not measured yet.

## Long model-check sessions

What these runs cover and what they found is summarized in [RESILIENCE.md](RESILIENCE.md).

`scripts/model_check.py` drives several connections with random commands over a small
keyspace (strings, TTLs, lists, hashes with field TTLs, sets, sorted sets, streams, MULTI,
EVAL, RENAME, FLUSHDB, SAVE/BGSAVE, BLPOP with a pusher, ACL-denied commands), compares every
reply with a reference model, and ends each epoch with a kill -9, a SIGTERM, a live check or
an injected disk fault. After each restart it checks the recovered keyspace against the
profile's promise. It needs Python 3 and a C compiler (for the fault shim); no Python packages.

```bash
cargo build --profile release-fast --bin ruvio
python3 scripts/model_check.py --seed 1 --duration 3600 --config all \
    --bin target/release-fast/ruvio --results target/model-check \
    --work-dir /var/tmp/model-check --keep-going
```

`--config` takes one name, a comma list or `all` (`memory-1`, `everysec-1`, `everysec-2`,
`always-1`, `always-worker`, `always-2`, `snapshot-1`, `snapshot-2`, `replica`,
`replica-always`, `maxmemory`); `--duration` is nominal work (1000 commands per second), split
evenly between them, so wall time follows the machine. The seed is printed and fixes the whole
schedule: the epochs, the events, where a disk fault starts and whether a kill is SIGKILL or
SIGTERM. Which commands overlap and where inside a write a kill lands still depend on timing. A failure writes a directory under `--results` with `summary.txt` (seed, config, the
command that repeats the run), `diff.txt` (expected against actual), `oplog.txt` (the last
commands with their replies), and the server log, config and injected-fault log.
`--known save-hang` lets a run continue past the known multi-shard `SAVE` wait under a
checkpoint fault; `--events` and `--no-faults` narrow what an epoch can end with.

Besides kill -9, SIGTERM and injected `EIO`/`ENOSPC`, the `everysec` and `always` configs end
some epochs with a simulated power loss (`powerloss`: the shim logs WAL write ranges and
successful syncs, and after kill -9 the bytes written since a file's last sync are zeroed) or a
torn write (`tear`: the server dies after a random prefix of one WAL write, so a crash can land
between the records of one batch). Most tears first let the server go idle, send it one
`MULTI` of writes or a `RENAME`, and cut the first, second or third WAL write after it, so a
command logged as several records shows whether they go in together. The power-loss model
covers WAL file contents only, not directory entries or checkpoint files. A replica has
converged only when its `DBSIZE` also equals the primary's: reads hide a logically expired
key, but a replica keeps it until the primary's delete arrives.

`scripts/model_check_mutants/` measures how many bugs the checker finds. Each `.patch` there
puts one realistic bug into the server; `run.py` applies each to a scratch worktree, builds it
and runs the checker with fixed seeds over the configs where the bug can show (`--every-seed`
reports how many seeds caught it and the median time), and `--base-check` runs the unmodified
base the same way as a control:

```bash
python3 scripts/model_check_mutants/run.py --scratch /tmp/mutants --duration 60 --seeds 1,2,3 \
    --every-seed --base-check --port-base 34000
```

## Run tests by environment

The same short test entry point is available from native Windows PowerShell,
macOS/Linux shells, and WSL:

```powershell
.\scripts\test.ps1
.\scripts\test.ps1 -Clippy
.\scripts\test.ps1 -NativeResilience
.\scripts\test.ps1 -DockerResilience
```

```bash
./scripts/test.sh
./scripts/test.sh --clippy
./scripts/test.sh --native-resilience
./scripts/test.sh --docker-resilience
```

The default runs formatting, all-target Rust tests, and the store smoke
example. Add `-Clippy` / `--clippy` to run Clippy. `NativeResilience` runs
randomized in-flight process-crash recovery against the native binary for
`everysec` and `always`; the PowerShell entry point builds the Windows binary
itself. `DockerResilience` builds a fresh Linux image and runs the bounded
filesystem, crash, replica, and 30-second soak cases.

| Environment | Recommended checks | Notes |
| --- | --- | --- |
| macOS (native) | `./scripts/test.sh --native-resilience` | Exercises the macOS server process and filesystem; not physical power loss |
| Linux or WSL | `./scripts/test.sh --native-resilience` | Native WSL tests use the Linux filesystem; Docker adds isolated Linux-container fault tests |
| Windows PowerShell | `.\scripts\test.ps1 -NativeResilience` | Uses the Windows server executable and native filesystem |
| Docker Desktop / Linux Docker Engine | `.\scripts\test.ps1 -DockerResilience` or `./scripts/test.sh --docker-resilience` | Requires Docker Desktop in Linux-container mode or a Linux Docker Engine |

Do not put the repository or Ruvio data directory under `/mnt/c` for WSL
filesystem durability testing; use the WSL Linux filesystem. Docker's bounded
30-second workload is a smoke test, not the proposed 24-hour soak. No 24-hour
Docker soak is part of this test plan.

## .NET integration scenario lab

An experimental two-host lab composes the ASP.NET Core output-cache,
idempotency, and distributed rate-limit packages with Ruvio, k6,
OpenTelemetry, Prometheus, Grafana, Tempo, and Linux host metrics. Its
automated suite runs a functional check, sustained traffic, and one bounded
API-to-Ruvio network partition. A separate guarded 28-minute stress profile
ramps to 3,000 target iterations/second, steps down, and rises again; it
records integration-level storage call counts and durations, API process
working set/CPU time, Ruvio process RSS, and k6 stage tags. This configured arrival rate is not
the measured capacity of Ruvio, the SUT, or the load generator. The first
30-minute two-host VPS result is
recorded in the [.NET scenario lab record](evidence/2026-10-03-dotnet-scenario-lab.md) (the raw
k6 summaries are not part of this repository).
The fault/recovery checks passed, but the run dropped load iterations, had
four failed checks, and reported inconsistent threshold status; it is
diagnostic evidence, not a clean release gate. See
[`tests/scenario-lab/README.md`](../tests/scenario-lab/README.md) for topology,
firewall requirements, storage metric names, safety stops, and operator
commands. The high-load profile has not yet been run against the VPS.

## Manually run Docker resilience scenarios

On 2026-10-03, the five scenarios in
[`DOCKER_RESILIENCE_TESTS.md`](DOCKER_RESILIENCE_TESTS.md) passed on Docker
Engine 29.8.0 (`linux/aarch64`), using image
`ruvio:resilience-test` (`sha256:56dcb352...`):

| Scenario | Observed result |
| --- | --- |
| Size-limited tmpfs reaches `ENOSPC` | 9,216 acknowledged writes recovered after space was released and the server restarted; writes during the fault returned `MISCONF` |
| WAL shard directory made read-only | 3 acknowledged writes recovered after permissions were restored and the server restarted; writes during the fault returned `MISCONF` |
| Server process receives `SIGKILL` | 100/100 acknowledged writes recovered after restart |
| Replica network interruption and promotion | After a 100-write partition, all 200 writes caught up in 0.371 seconds; all remained after promotion |
| Mixed-workload soak | 30 seconds; 48,413 iterations / 145,239 `SET`/`GET`/`INCR` commands |

## Native in-flight process-crash run

On 2026-10-03, the current Apple silicon Mac ran three randomized kills during
concurrent pipelined writes for each WAL-backed durability profile. All six
trials recovered a continuous key prefix. `always` preserved every observed
acknowledged write; in-flight records beyond the client-observed replies were
also recovered in these samples.

| Profile | Trials | Acknowledged writes preserved | Recovered keys |
| --- | ---: | ---: | ---: |
| `everysec` | 3/3 | Yes | 297, 576, 810 |
| `always` | 3/3 | Yes | 592, 621, 320 |

The exact report is `target/native-resilience/20261003T155337Z-91181.json`
(local, ignored build output). This is native macOS process termination, not
host power loss or physical-device validation. Repeat on Windows/WSL using
the environment-specific commands above before treating those platforms as
independently exercised.

The exact local Docker JSON report is
`target/docker-resilience/20261003T155213Z-90102.json` (ignored build output,
not committed evidence). Re-run the documented scenarios to create a report
in your own `target/` directory.

These are Linux-container process/filesystem checks. They do not simulate host
power loss or establish physical-device flush semantics. Docker Desktop runs
Linux containers in a VM; this evidence is not bare-metal certification. The
Docker filesystem fault scenarios were not separately exercised against the
native macOS filesystem; native macOS coverage is limited to the in-flight
process-crash tests above.

## Performance and memory measurements

Ruvio has dated, reproducible benchmark records rather than a single general
performance claim. [`evidence/README.md`](evidence/README.md) lists them,
newest first: the 2026-10-11 Azure command and memory comparison, native and
Docker Desktop runs from 2026-10-01 to 2026-10-07, the .NET scenario lab, and
the 2026-09-28 Cherry EPYC Linux run with Redis comparisons, durability
profiles, two 15-minute soaks and `SIGKILL` recovery trials. Current numbers
are on the site's [benchmarks page](https://ruviodb.com/benchmarks/).

Laptop and Docker Desktop measurements are useful for repeatable functional
checks and same-machine regression detection. They are not substitutes for
published production-hardware latency or durability evidence.

## Evidence still needed

| Evidence gap | What remains |
| --- | --- |
| Host and device failure | Power-loss testing and validation of flush behavior on production-like Linux filesystems and storage |
| Long-duration stability | A 24-hour mixed-workload soak with resource and recovery evidence; the tooling is in `tests/soak/`, no long run yet |
| Native macOS faults | Process/filesystem fault scenarios against a Ruvio process running directly on macOS, rather than inside Docker's Linux VM |
| Production performance | Published Linux bare-metal p50/p95/p99 latency and repeatable throughput measurements |
| Failure operations | Repeated failover/promotion drills with measured recovery time and client behavior; promotion is currently manual |
| Release provenance | Signed build provenance in addition to checksums and the generated SBOM |

Use [`RELEASE_EVIDENCE.md`](RELEASE_EVIDENCE.md) and the
[production-preview roadmap](ROADMAP.md) as the detailed release gates. Update
this summary when a new test produces reproducible evidence; record the date,
build/image, platform, workload, outcome, and limitations.
