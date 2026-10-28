---
name: ruvio-measure
description: Use when a Ruvio change needs a before/after speed or memory measurement; builds the base and the change, runs the quick harness on both and writes one table.
---

# Before/after measurement

The rule (`docs/PERFORMANCE.md`): for the same data, RSS does not grow and the command does
not get slower. One sample decides nothing; repeat it.

## What to measure

- The commands the change touches, plus `set`, `get` and `incr` as a control.
- Speed (requests per second or latency) and memory (`DBSIZE` and RSS for the same key set).
- For hot-path changes on Linux: instruction counts as well (see the last section).

## Steps

1. **Base build without disturbing your branch.** Do not `git stash`. Use a worktree of the
   branch you started from and reuse it next time:
   ```bash
   git worktree add .worktrees/ruvio-base origin/dev     # once; later: git -C .worktrees/ruvio-base checkout --detach origin/dev
   (cd .worktrees/ruvio-base && cargo build --profile release-fast)
   ```
   `.worktrees/` is ignored by git.
2. **Change build.** In your branch: `cargo build --profile release-fast`. Both builds use the
   same profile. A `--release` build (fat LTO) takes far longer than the 5-minute budget; build
   it only for numbers that will be published, and only with the maintainer's approval.
3. **Run both binaries in turn**, on separate ports, alternating which goes first, at least
   three rounds:
   - `python3 scripts/bench_matrix.py --quick` (small scale, under 5 minutes), or
   - `python3 scripts/redis_compare.py --tests <list> --rounds 3` for a few redis-benchmark tests.
   - Memory: load the same key set into each, record `DBSIZE` and `ps -o rss= -p <pid>`.
   Read each script's header for its binary and port options before running it.
4. Nothing else builds or benchmarks on the machine while you measure.
5. Write raw output under `target/measure/<change-name>/` and report only this table:

| Scenario | Base | Change | Ratio |
| --- | ---: | ---: | ---: |

   A ratio between 0.95 and 1.05 is reported as "no difference".
6. Put the table in the pull request under "How I checked it", with the machine, OS and build
   profile. A number that goes into the docs needs a dated record in `docs/evidence/`
   (see `docs/evidence/README.md`).

## Decision

- Speed or RSS more than 5% worse: stop, explain the cause in one paragraph, and do not merge.
- Do not mix macOS and Linux results in one table. A laptop or Docker Desktop result is a
  same-machine comparison, not a published number.
- If something cannot be measured here, write "not measured" next to it.

## Instruction counts (Linux)

The PR gate compares instruction counts against `benchmark/baselines/instructions.json`
with a +2% tolerance (at least 32 instructions):

```bash
cargo bench --bench hot_path > target/measure/iai.log 2>&1      # needs valgrind and iai-callgrind-runner 0.16.1
python3 scripts/check_instruction_baseline.py target/measure/iai.log
```

The baseline is measured on a GitHub runner; counts from another machine differ by several
percent. An intentional change refreshes the baseline through the CI workflow's
`instruction-baseline` job, which the maintainer starts, and the pull request states why.
macOS has no valgrind: write "instruction count not measured" and let the PR gate measure it.

## Other harnesses

`scripts/README.md` ("Benchmarks and profiling") lists the rest: `perf_compare.py` and
`command_cost.py` for instructions per request, `memory_churn.py` for memory under churn,
`benchmark_shards.py` for shard scaling, `profile_linux.sh` for CPU profiles.
