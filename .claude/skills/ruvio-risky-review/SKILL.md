---
name: ruvio-risky-review
description: Use before merging a Ruvio change that touches unsafe code, persistence/WAL/snapshots, memory layout, hash tables or cross-thread code; runs a fresh-eyes review.
---

# Fresh-eyes review of a risky change

## Which changes need it

Any change that adds or edits `unsafe`, or touches:

- `src/persistence/` (WAL, codec, snapshots, sync worker), `src/mutation.rs`, `src/dump.rs`,
  `src/changes.rs`, `src/replication.rs`;
- memory layout: `Value`, `Entry`, `src/arena.rs`, `src/chunked.rs`, `src/store/`, the value log;
- hash tables and indexes: `src/hashbrown/`, `src/index.rs`, `src/slots.rs`, `src/id_map.rs`;
- thread boundaries: channels between shards, the sync worker, replication, the coordinator.

## How

- The review is not done in the context that wrote the change. Start a fresh session or
  subagent and give it only the diff (`git diff origin/dev...HEAD`) and the relevant section of
  `docs/ARCHITECTURE.md`, `docs/STORAGE_LAYOUT.md` or `docs/RECOVERY.md`. Do not give it the
  author's explanation.
- The reviewer answers the checklist item by item. Each finding names file:line and a concrete
  failure scenario (the inputs or interleaving that break it). Without a scenario it is not a
  finding.
- A human reviewer on the pull request is still required; this review comes first.

## Checklist

1. **Memory and addressing:** offset and length overflow, `u32` limits, `as` casts, page and
   block boundaries, large-block markers.
2. **`unsafe`:** every block has a `// SAFETY:` comment, the claim in it is true, and a Miri
   test covers it. New unsafe code adds small tests of its module to the Miri filters in
   `scripts/run_sanitizers.sh`.
3. **Persistence order:** validate, then record, then apply. On a WAL error the state is
   unchanged. On recovery, replaying a record gives the same result as the first time.
4. **Crash points:** what remains if the process dies between write, sync, rotate and rename;
   whether `killed_process_recovers_at_crash_sites` (`src/persistence/crash.rs`) covers the new
   point.
5. **Incremental operations:** inserts, deletes, RENAME, FLUSH and SWAPDB during a cursor,
   rehash or snapshot walk; no key is skipped or visited twice.
6. **Bounded work:** an upper bound on work per call or per tick.
7. **Thread boundary:** shard data does not cross to another thread; channels are bounded;
   the behaviour when a channel is full is defined and tested.
8. **Zero cost:** `tests/zero_alloc.rs` and `tests/layout.rs` are green; no branch on the hot
   path for a feature that is off.
9. **Format compatibility:** old snapshot and WAL files still open (fixtures under
   `tests/fixtures/persistence/`); a new record type bumped `FORMAT_MINOR`.

## Checks to run

```bash
scripts/test_area.sh persistence > target/t.log 2>&1; tail -30 target/t.log
cargo test --test zero_alloc --test layout 2>&1 | tail -15
timeout 300 scripts/run_sanitizers.sh miri > target/miri.log 2>&1; tail -30 target/miri.log   # Linux, nightly toolchain
```

Miri and AddressSanitizer may not be available locally; then write "not verified locally"
and rely on the CI `miri` and `asan` jobs, which the maintainer starts.

## Result

List the findings by severity in the pull request description. A critical finding blocks the
merge until it is fixed and the review is run again on the new diff.
