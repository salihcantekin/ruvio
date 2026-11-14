# Release Evidence

This page records which Production Preview P0 gates are automatable in this
repository and which remain blocked on hardware or wall-clock time. Unchecked
roadmap items are not claimed complete.

For a user-facing summary of test coverage, observed Docker results, benchmark
evidence, and remaining gates, see [Testing and Evidence](TESTING.md) and the
[testing guide](TESTING.md).

## Automatable gates (defined in CI)

- `cargo fmt --check`, `cargo clippy -D warnings`, and `cargo test` on macOS,
  Linux, and Windows.
- RESP conformance (`scripts/resp_conformance.py`).
- Redis differential tests for the supported command subset
  (`scripts/redis_differential.py`).
- Compatibility-matrix drift check
  (`scripts/check_compatibility_matrix.py`).
- First-party `ruvio-client` and `Ruvio.Client` tests
  (`scripts/run_client_suites.py`).
- `cargo deny check` against `deny.toml`, CycloneDX SBOM generation, and
  `SECURITY.md` private-reporting policy (`scripts/test_supply_chain.py`).
- WAL recovery, snapshot atomic publish/load, checksum/corruption rejection,
  missing-segment discontinuity, expired-key restart, TTL commands, `noeviction`
  OOM, AUTH, `ruvio-ctl` backup/restore/integrity checks, a backup restored
  into a new directory and opened as a server restart, and a frozen v1
  checkpoint directory opened as a prior release.

Build checksums after a release compile:

```bash
cargo build --locked --release --bin ruvio --bin ruvio-ctl
./scripts/release_artifacts.sh
```

## Documented blockers

| Gate | Why it is not marked complete |
| --- | --- |
| Randomized `kill -9` crash harness during append/sync/rename | Live SET harness passed on the Cherry EPYC host: 12 trials each of `everysec` and `always`, process `kill -9` during pipelined SET, restart, prefix check. `always` kept every acknowledged key. Named crash sites still abort inside `persistence::crash::tests`. This is not a power-loss or snapshot-rename kill. See `docs/evidence/2026-09-28-bare-metal.md`. |
| Full-disk and permission-denied injection | Docker tmpfs `ENOSPC`, WAL-directory permission, process crash, replica network interruption/promotion, and bounded soak scenarios are available via `scripts/docker_resilience.py`. Local Docker recovered every acknowledged write and caught up 200/200 replica keys after a network partition. Native macOS randomized in-flight crash tests also passed 3 trials each for `everysec` and `always`. Real block-device and host-filesystem behavior still need production-like Linux validation. |
| 24-hour mixed-workload soak | Not run yet. A 12-hour multi-node run on Azure (`tests/soak/plans/azure-12h.json`, 34 faults, 25.2 million checked operations) passed all 17 checks; see the [long-run tests](https://ruviodb.com/testing/) page. The 30-second Docker workload is only a bounded smoke test. |
| String RSS within the same band as Redis | The gap found in the 2026-09-28 Cherry soak (`docs/evidence/2026-09-28-string-memory-density.md`) was worked down. Later short runs are below Redis: total RSS 0.77× for 16-byte and 0.84× for 128-byte strings in `docs/evidence/2026-10-11-azure-command-comparison.md`. The 15-minute four-core soak itself was not re-run. |
| Bare-metal p50/p95/p99/p99.9 publication | One Cherry EPYC 7313P run is recorded in `docs/evidence/2026-09-28-bare-metal.md`. Frequency boost was disabled. The 99.902% redis-benchmark bucket is not a computed p99.9. Container and laptop numbers are still not release evidence. |
| SLSA/provenance attestations | `scripts/release_artifacts.sh` produces checksums and a CycloneDX SBOM when run by hand. The `release` workflow writes its own checksums and attaches no SBOM yet. Signed provenance is not wired yet. |

The extended `ci.yml` workflow is manually dispatched; its Docker,
cross-platform, fuzz, sanitizer, and supply-chain jobs are not automatic. The
short, read-only `pr-gate.yml` workflow runs on pull requests and pushes to
`main`, covering formatting, core tests, `scripts/check.sh` (which includes the
compatibility-matrix validator), the instruction-count gate, client tests and
local two-shard routing regressions. A workflow definition is not a passed
run; consult the check result for the exact commit. Run the main Rust suite
with `cargo test --lib --bins --tests` and inspect `.github/workflows/ci.yml`
for the remaining commands. Do not
treat Docker Desktop throughput as a substitute for the bare-metal performance
P0.

Run the manual process/filesystem fault scenarios and inspect their JSON
reports with the instructions in `docs/DOCKER_RESILIENCE_TESTS.md`. These
results are useful container-level evidence only; they do not close the
host-power-loss or physical-storage durability gates.
