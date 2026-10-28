---
name: ruvio-add-command
description: Use when adding or changing a Ruvio server command; walks the zero-cost rule, command table, tests, clients and compatibility matrix in a fixed order.
---

# Adding or changing a server command

Goal: a new command costs nothing when it is not used, and every command is added through the
same steps. The full map of files a command has to reach is the "Adding a command" table in
`docs/DEVELOPING.md`; this skill is the order of work and the checks around it.

## Before you start

- Confirm the command does not exist yet: `rg -n '<COMMAND>' src/protocol src/engine docs/compatibility-matrix.json`.
- Find the closest existing command of the same family and follow its shape. Its table row in
  `src/protocol/table.rs`, its parser and its engine file are the template.

## Steps (in order, each one small)

1. **Variant and table row.** Add the `Command` variant in `src/protocol/mod.rs` and one row in
   `src/protocol/table.rs`: name, arity, flags (write, read-only, blocking, admin), key
   positions (first, last, step) and ACL categories. The parser reads `Args` and borrows the
   arguments; it does not allocate a `Vec`.
2. **Owned copy, validation, routing, ACL.** `src/protocol/command.rs`, `src/protocol/validate.rs`,
   `src/routing.rs`, `src/acl.rs`. The compiler and the `consistency.rs` tests point at each
   missed place.
3. **Data model.** Reuse an existing `Value` arm when it fits. When new state is needed:
   - a new `Value` arm is boxed, so `Value` stays at most 24 bytes (`tests/layout.rs`);
   - state that is not per key lives in an `Option<Box<_>>` or a side table created on first use.
4. **Execution.** `src/engine/<family>.rs` and its arm in `src/engine/dispatch.rs`. Work per call
   is bounded: work that grows with N is capped by a COUNT, done incrementally or moved off the
   shard. Never walk the whole keyspace in one call. The reply is written straight into the
   output buffer, with no intermediate `Vec`.
5. **Keyless commands.** Decide them for scripts (`src/script.rs`) and fan-out
   (`src/runtime/evented/forward.rs`); the tests named in `docs/DEVELOPING.md` fail until you do.
6. **Durability.** A write produces a `Mutation` (`src/mutation.rs`) that records the result,
   not arithmetic to recompute on replay. A new WAL record type needs a `FORMAT_MINOR` bump in
   `src/persistence/codec.rs` and a fixture under `tests/fixtures/persistence/`. This makes the
   change a risky one: run the `ruvio-risky-review` skill before merge.
7. **Tests.** In `src/engine/tests/<family>.rs`: the correct reply, wrong type, missing key and
   boundary values. If Redis has the command, add it to `scripts/redis_differential.py`.
8. **Clients.** Follow "Adding a client method" in `docs/DEVELOPING.md`:
   `RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec`, `python3 scripts/gen_client_keys.py`,
   one row in `clients/testdata/command_methods.json`, and a method in each of the six clients
   (`python3 scripts/client_stubs.py <COMMAND> --reply integer|bulk|ok|array` prints skeletons).
   Each client gets a live test.
9. **Docs and changelogs.** A row in `docs/compatibility-matrix.json`, `docs/COMPATIBILITY.md`
   and `docs/DATA_TYPES.md`; a line under `## Unreleased` in the root `CHANGELOG.md` and in each
   client's `CHANGELOG.md`.

## Verification commands

Keep output short; read the tail of a log rather than the whole stream.

```bash
cargo check 2>&1 | tail -20
scripts/test_area.sh protocol <command> > target/t.log 2>&1; tail -30 target/t.log
scripts/test_area.sh engine <command> > target/t.log 2>&1; tail -30 target/t.log
cargo test --test zero_alloc --test layout 2>&1 | tail -15
python3 scripts/check_compatibility_matrix.py
scripts/check.sh
```

Then try the command against a real server (`target/debug/ruvio`) with a few calls, and run
the client live tests through `scripts/run_with_servers.sh target/debug/ruvio 17400 -- <test command>`.

## Not done if

- the `size_of` ceiling in `tests/layout.rs` had to change;
- `tests/zero_alloc.rs` is red, or GET/SET/INCR gained a branch or lookup for the new command;
- the command walks the whole keyspace in one call;
- a client, the matrix or a changelog was left out;
- the only evidence is that it compiles.
