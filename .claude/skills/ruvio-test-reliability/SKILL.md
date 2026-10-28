---
name: ruvio-test-reliability
description: Use when writing or changing a Ruvio test, or investigating a red or hung CI or test run; zero-flaky rules for ports, waiting, time limits, isolation, Miri and ASan.
---

# Test reliability

A test that fails or hangs once in a thousand runs is a broken test. There is no list of known
flaky tests and no "run it again". A red run is investigated, the root cause is fixed, and the
fix is shown in a short loop. Unit tests are very short; all integration tests together take a
few minutes. The same rules are in "Test rules" in `docs/TESTING.md`.

## 1. When a run is red

1. Get the failing test and its message from the log (only the failed job's log).
2. Do not re-run it and call it fixed. Find the interleaving or timing that breaks it in the
   code.
3. Match it to one of the patterns below, fix the cause, then prove it as in section 6.
4. If the cause is a real product bug, fix the product and say so plainly in the pull request.
5. Never make the run green by skipping, ignoring, disabling or retrying the test.

## 2. Ports

- A test that starts a server in-process hands it a listener already bound to `127.0.0.1:0`
  (`serve_with`, `spawn_primary_on` and similar). It never picks a port number and releases it.
- When the server must bind by itself (a subprocess, a port per shard, a replication listener,
  a restart on the same address), take `test_ports::reserve()` (`src/test_ports.rs`, also
  used through `tests/common`): a block of consecutive ports from a fixed range, locked across
  processes. Hold the `PortBlock` for the server's whole life, restarts included. A test that
  spawns a subprocess does it under `test_ports::spawn_lock()`, so the child cannot inherit a
  probing socket and hold the port.
- An address that must refuse connections is `127.0.0.1:1`, not a released port.
- Forbidden: bind to 0, close and hand the number over; fixed ports; ports in the OS
  ephemeral range (Linux 32768-60999, macOS and Windows 49152-65535); "retry on another port"
  loops.
- Client suites run through `scripts/run_with_servers.sh` with fixed base ports (17400 and up);
  readiness is a PING/PONG from the server it started (`scripts/_ruvio.py wait-ready`), not just
  a TCP connect.

## 3. Waiting and time

- No `sleep` followed by an `assert`. Poll the condition itself (a reply, an `INFO` field, a
  file) against a deadline of 15-30 s, which a passing test never reaches.
- No upper bound on elapsed time unless it is far from both the normal and the failing run; a
  counter is better.
- No 100-250 ms TTLs, 2 s read timeouts or "the message arrives within X ms". Slow runners,
  ASan and macOS fsync exceed them. When a test needs TTL timing, send the SET and the read in
  the same batch (the server reads its clock once per batch).
- Every blocking read, accept, join, receive or wait has a deadline, so a bug fails the test
  instead of hanging it. Join server threads with a bound (`join_within`).
- A `run`/`serve` call that is expected to fail gets an already stopped `ShutdownToken`, so it
  cannot run forever if it starts by mistake.
- Inject faults through a handle the test releases (for example `arm_held_sync_fault()`), not
  through a fixed sleep.

## 4. Isolation

- Tests running in parallel never share keys, database numbers, files, process-wide flags or
  environment variables. Test-only static flags are keyed by data directory.
- A live client test that runs FLUSHDB or FLUSHALL uses its own database (Rust client:
  `common::claim_database()`). In per-shard mode FLUSHDB clears only its own shard.
- Random input is seeded and the failure message prints the seed; the seed reproduces the run
  exactly (no wall-clock-dependent events).
- A statistical check has a false-alarm probability below 1e-9.

## 5. CI, nextest, Miri, ASan

- CI and `pr-gate` run the Rust tests with `cargo nextest run --profile ci`
  (`.config/nextest.toml`): each test in its own process; a unit test is killed at 30 s and an
  integration test at 2 minutes, and fails by name. A test near the limit needs fixing, not a
  higher limit.
- One process per test means anything lazily initialised per process (calibration,
  `OnceLock`) happens inside the test; write for that (for example call `Stamp::calibrate()`
  first).
- `flock` belongs to the open file: a forked child holds the lock until it execs. A test that
  takes the lock again waits with a deadline.
- Miri runs only where unsafe code lives (`hashbrown::` and `slots::`, about a minute), with
  sizes cut down under `cfg(miri)`. Safe code is not added to Miri; FFI (Lua, mimalloc,
  sockets) does not run under Miri and is covered by ASan. Locally, run Miri with `timeout`
  (`scripts/run_sanitizers.sh miri`).
- ASan fails on every leak. `Box::leak` is acceptable only when each value leaks once and stays
  reachable from a static.

## 6. Proving a fix

- Run only the tests you touched, with a filter, and bound each command with `timeout`. No
  full suite and no `--release`.
- Run a race-prone test in a loop of about 5 repetitions, ideally with the machine under load,
  and report "N/N passed". Longer loops are left to CI.
- Write down what you could not verify (Windows- or macOS-specific behaviour) as
  "not verified".

## 7. Client tests

- Every client step in CI has a job time limit, and every framework has a hang guard: Python
  socket timeouts and the 120 s limit in `clients/python/tests/conftest.py`, the JUnit default
  timeout and the surefire process limit in Java, `dotnet test --blame-hang-timeout 5m`, Node
  test timeouts, `go test -timeout`.
- Do not send `XREAD BLOCK` with `$` after a sleep; read from an explicit ID. Timed checks such
  as delayed `XADD` use a wide margin and polling.
- Fake servers join their accept thread in `stop()`; no fixed sleep.
- Order events with channels or gates, not `sleep`; wait with the framework's `until` or
  `wait_for` helpers and a deadline.
- Parallel live tests never share a database.
- Allocation-counting tests do not depend on GC or JIT timing: non-parallel collection, the
  minimum of several measurements.
