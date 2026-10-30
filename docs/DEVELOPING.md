[Ruvio](../README.md) / [Docs](README.md) / Developing

# Developing Ruvio

Where the code lives, how to check a change quickly, and every place a new command, setting or client method has to reach. Read [CONTRIBUTING.md](../CONTRIBUTING.md) for the hot-path rules and [PERFORMANCE.md](PERFORMANCE.md) before changing a command path.

## Repository map

| Path | What it is |
| --- | --- |
| `src/` | The server crate (`ruvio`, `ruvio-ctl`). AGPL-3.0 |
| `tests/` | Integration tests, including the zero-cost guards `zero_alloc.rs` and `layout.rs` |
| `benches/hot_path.rs` | Instruction-count bench behind the PR gate's +2% limit |
| `clients/` | Rust, C#, Go, Java, Node and Python clients, plus `ruvio-ui`. MIT except `ruvio-ui` |
| `integrations/dotnet/` | ASP.NET Core and `IDistributedCache` packages on top of the C# client. MIT |
| `Directory.Build.props`, `Directory.Packages.props` | Settings and package metadata shared by every .NET project, and the one version of each NuGet dependency (a `PackageReference` has no `Version`) |
| `docs/` | Reference docs and `compatibility-matrix.json`, the list of supported commands |
| `scripts/` | Local gates, release helpers, fault and benchmark harnesses ([scripts/README.md](../scripts/README.md)) |
| `site-next/` | The website ([ruviodb.com](https://ruviodb.com)): Astro, docs generated from `docs/`, `clients/` and `integrations/` by `scripts/sync-docs.mjs`; the `/changelog/` page shows the newest sections of the server's `CHANGELOG.md` and is built from the first-parent history of `main` and `dev` and the release tags by `scripts/changelog.mjs`, 100 entries per page, with a search that fetches `/changelog/entries.json` on first use (a shallow clone gets a page that links to GitHub instead); `/blog/` lists the Markdown posts in `site-next/src/content/blog/` (one file per post, front matter checked by `src/content.config.ts`; `<!-- scene: id -->` places an animation and `<!-- facts -->` the fact strip; `<!-- clients: id -->` after a code block adds the client tabs of that use case from `src/data/client-examples.ts` and links its framework packages; text CC BY 4.0, see the folder's `LICENSE`); published by the manual `site-azure` workflow. Measurement records it charts are in `benchmark/records/` |
| `CLAUDE.md`, `.claude/` | The working agreement and skills for Claude Code sessions (see [Working with Claude Code](#working-with-claude-code)) |
| `.github/workflows/` | `pr-gate` runs on pull requests and pushes to `main`; every other workflow is started by hand |

## Server module map

Each shard is owned by one thread: it parses, records to the WAL, applies and replies. Cross-shard work is a message to the owner, never shared state.

| Area | Modules | Notes |
| --- | --- | --- |
| Network and sessions | `runtime/evented/` (`connection.rs`, `forward.rs`, `wal_group.rs`, `checkpoint.rs`, `replica.rs`), `runtime/session.rs`, `runtime/cluster.rs`, `tls.rs` | `connection.rs` is the hottest async code; measure before restructuring it |
| Protocol | `protocol/` (`resp.rs`, `table.rs`, `parse/`, `direct.rs`, `hot.rs`, `command.rs`, `validate.rs`, `consistency.rs`), `routing.rs`, `slots.rs` | `table.rs` is the one row per command; `consistency.rs` tests the hand-kept lists against it |
| Execution | `engine/` (one file per family, dispatch in `engine/dispatch.rs`), `transaction.rs`, `script.rs`, `lua_library.rs`, `lua_pattern.rs` | |
| Storage | `store/` (values, collections, eviction, expiry), `index.rs`, `arena.rs`, `chunked.rs`, `hot_store/`, `stream.rs` (packed blocks in `stream/blocks.rs`), `id_map.rs`, `json.rs` (field indexes in `json/index.rs`), `geo.rs`, `hashbrown/` | `hashbrown/` is a trimmed copy of hashbrown 0.17.1 (`HashTable` only); its README lists every change |
| Durability | `mutation.rs` (the change vocabulary), `persistence/` (`wal.rs`, `codec.rs`, `snapshot.rs`, `sync_worker.rs`), `dump.rs`, `changes.rs` (CDC) | Format changes need a format-minor bump and a fixture |
| Replication | `replication.rs`, `runtime/evented/replica.rs` | The preflight matches list every `OwnedMutation`; a new variant does not compile until classified |
| Coordinator | `coordinator/` (`config.rs`, `raft.rs`, `monitor.rs`, `resp.rs`), behind the default `coordinator` feature | A separate process (`ruvio coordinator`); it reaches data nodes only through RESP commands, never through server internals |
| Operations | `config.rs`, `acl.rs`, `security.rs`, `metrics.rs`, `latency.rs`, `slowlog.rs`, `keystats.rs`, `memory.rs`, `import.rs`, `shutdown.rs` | |

## Fast loop

| When | Run | Time on a warm build |
| --- | --- | --- |
| While editing | `cargo check` | seconds |
| One area | `scripts/test_area.sh <store\|protocol\|engine\|persistence\|server> [filter]` | seconds to a minute |
| Before every push | `scripts/check.sh` (install once with `scripts/check.sh --install-hook`) | about a minute |
| A command path changed | `cargo test --test zero_alloc --test layout` | seconds |
| Full suite | `cargo test --lib --bins --tests` | several minutes; not needed per change |
| A client against a live server | `scripts/run_with_servers.sh target/debug/ruvio 17400 -- <test command>` | depends on the suite |

Build tests in the debug profile, or with `--profile release-fast` when they need optimized code. The `release` profile (fat LTO, one codegen unit) is only for binaries whose numbers are published. Live client tests skip themselves when `RUVIO_TEST_ADDR` is unset; `run_with_servers.sh` sets it and starts a standalone server plus a 3-shard cluster.

## Adding a command

The compiler or a test catches a missed step where the last column says so; the other steps are on you.

| Step | Where | Caught by |
| --- | --- | --- |
| 1. Variant | `Command` in `src/protocol/mod.rs` | compiler (every `match` on `Command`) |
| 2. Table row: arity, flags, key positions, ACL categories, parser (a function that reads `Args`) | `src/protocol/table.rs` (`CommandSpec::migrated`); the parser in `src/protocol/table/parsers.rs` or `src/protocol/parse/` | `consistency.rs` tests (`scripts/check.sh`) |
| 3. Owned copy and validation | `src/protocol/command.rs` (`into_owned_measured`), `src/protocol/validate.rs` | compiler |
| 4. Routing | `src/routing.rs` (`command_slot`, `command_keys`) | compiler, `consistency.rs` |
| 5. ACL name | `src/acl.rs` | compiler, `consistency.rs` |
| 6. Execution | `src/engine/<family>.rs`, dispatch arm in `src/engine/dispatch.rs` | compiler |
| 7. Keyless commands: scripts and fan-out | `KEYLESS_IN_SCRIPTS` / `KEYLESS_REFUSED_IN_SCRIPTS` in `src/script.rs`, the fan-out list in `src/runtime/evented/forward.rs` | `every_keyless_command_is_decided_for_scripts`, `every_keyless_command_is_decided_for_fan_out` |
| 8. Durability group (optional speed) | `src/runtime/evented/wal_group.rs` | not caught: an unlisted command takes the slower, correct path |
| 9. Tests | `src/engine/tests/<family>.rs` | |
| 10. Matrix and docs | `docs/compatibility-matrix.json`, `docs/COMPATIBILITY.md`, `docs/DATA_TYPES.md` | `scripts/check_compatibility_matrix.py` |
| 11. Client command spec: arity, flags and where the keys are, for the clients' routing | `RULES` in `src/protocol/client_spec.rs` when a first/last/step range cannot place the keys; then `RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec` rewrites `clients/testdata/commands.json` and `key_vectors.json`, and `python3 scripts/gen_client_keys.py` rewrites the six clients' key tables from them | `client_command_spec_matches_the_server`, `gen_client_keys.py --check` (`scripts/check.sh`); each client's key-vector test |
| 12. Clients | see "Adding a client method" | each client's coverage test |

A write that needs a new WAL record also touches:

| Step | Where | Caught by |
| --- | --- | --- |
| Mutation | `Mutation`, `OwnedMutation`, `to_owned` and the write-key arm in `src/mutation.rs` | compiler |
| Encoding | a `record::` number, borrowed and owned encoders, decoder, `FORMAT_MINOR`/`RECORD_MINORS` and a fixture in `src/persistence/codec.rs` | `borrowed_encoding_matches_owned_encoding`, `every_record_type_the_decoder_knows_has_a_format_minor`, `version_one_wal*` |
| Replay, replication, CDC, DUMP | `src/persistence/mod.rs`, `src/replication.rs`, `src/changes.rs`, `src/dump.rs` | compiler |

A change to persistence, the WAL, unsafe code or memory layout gets a second, independent review before it merges.

## Adding a setting

| Step | Where |
| --- | --- |
| Field and default | `AppConfig` and its `Default` in `src/config.rs` |
| File key | the matching `*FileConfig` struct in `src/config.rs` |
| Environment variable | `apply_environment` in `src/config.rs` (`RUVIO_<NAME>`) |
| Command-line flag (optional) | `Cli` in `src/bin/ruvio.rs`, `apply_overrides` in `src/config.rs` |
| `CONFIG GET` | `introspect` in `src/config.rs` |
| Live or restart-only | `LIVE_SETTINGS` or `RESTART_ONLY_SETTINGS` in `src/config/live.rs`. A live setting also needs a `set_live` arm and a line in `copy_live_settings` there, and the shard must read it from `ShardState::live` (a `Cell` if it is read per command, `live.with` otherwise), never from `published_config`; `live::apply` in `src/runtime/evented/live.rs` pushes it into the store, limiter or slowlog if they keep their own copy. Restart-only is the default for anything that moves data, opens a listener or changes persistence |
| `--write-config` | `overlay_config` in `src/config.rs` |
| Template | `ruvio.example.toml` (its values are tested against the code defaults) |
| Schema | a row in `SETTINGS` in `src/config/schema.rs`, then `cargo run --bin ruvio -- --print-config-schema > site-next/src/data/config-schema.json` (the site's setup wizard reads that file) |
| Docs | `docs/CONFIGURATION.md` (the key and a row in the environment table, with its `CONFIG SET` name or `no` in the `CONFIG SET` column, and a row in the table under *Changing settings at runtime* for a live one; `the_configuration_tables_show_every_live_setting` checks both); the site's configuration page is generated from it |

`every_config_get_name_is_live_or_restart_only` fails until a new `CONFIG GET` name is in one of the two lists, and `every_live_setting_accepts_its_config_get_value` until a live one has a `set_live` arm. `every_template_setting_answers_config_get`, `every_file_setting_is_documented`, `every_environment_variable_is_documented` and `every_command_line_flag_is_documented` (all in `scripts/check.sh`) catch a template value, file key, variable or flag that is missing; the site page is checked by hand. `the_schema_lists_every_environment_variable`, `the_schema_lists_every_file_setting`, `schema_variable_and_file_key_set_the_same_setting`, `the_site_schema_file_is_current` and `the_config_schema_names_every_setting_flag` fail until the schema row is there and the JSON is regenerated.

## Adding a client method

Every client has a coverage test that loads `docs/compatibility-matrix.json` and fails when a command has no method. The method each client sends a command with is one row of `clients/testdata/command_methods.json`, shared by all six coverage tests: a new command adds that one row (sorted by command, one field per client), not a line in six tables.

| Client | Method | Coverage test | Live test |
| --- | --- | --- | --- |
| Rust | `clients/ruvio-client/src/commands.rs` or `extra.rs` | `tests/coverage.rs` | `tests/live*.rs` |
| C# | `Ruvio.Client/Commands/*.cs` and `IRuvioClient*.cs` | `Ruvio.Client.Tests/MatrixCoverageTests.cs` | `Ruvio.Client.Tests` |
| Go | `cmds_*.go` | `coverage_test.go` | `live_*_test.go` |
| Java | `commands/*Commands.java`, then `python3 clients/java/tools/generate.py` | `internal/CoverageTest.java` | `*LiveTest.java` |
| Node | `src/commands.ts`, `src/command-names.ts` | `test/coverage.test.ts` | `test/live-*.test.ts` |
| Python | `ruvio/_commands.py` | `tests/test_coverage.py` | `tests/test_live.py` |

`python3 scripts/client_stubs.py <COMMAND> --reply integer|bulk|ok|array` prints a starting skeleton for each client. Cluster key routing needs no hand edit: every client reads the key table `scripts/gen_client_keys.py` writes from `clients/testdata/commands.json`, and tests it against `clients/testdata/key_vectors.json`. The Java blocking facade and command words are generated; the PR gate runs `generate.py --check`. Cluster routing, redirects and retries are compared across clients in [CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md); every client's slot function is tested against `clients/testdata/slots.json`.

## Working with Claude Code

The repository carries the project files for [Claude Code](https://docs.claude.com/en/docs/claude-code/overview), so every contributor's session starts with the same rules.

| File | What it is |
| --- | --- |
| `CLAUDE.md` | The working agreement every session reads first: the time budget, how to verify, the test, performance and safety rules, git flow, and which doc to read for what |
| `.claude/settings.json` | Shared permissions: build, test and read-only commands are allowed; destructive and publishing commands (force-push, tags, `cargo publish`, release workflows, `--release` test builds) are denied, and a deny wins over an allow |
| `.claude/skills/<name>/SKILL.md` | Procedures for recurring tasks (adding a command, measuring, a risky-change review, test reliability, hygiene, release preparation), loaded when a task matches their description |

Personal overrides go in `.claude/settings.local.json`, which git ignores; do not widen the shared file for one machine. To add a skill, create `.claude/skills/ruvio-<name>/SKILL.md` with YAML front matter (`name:` equal to the folder name, and a one-sentence `description:` that says when to use it), keep the body in English, point to existing docs and scripts rather than copying them, check that every path it names exists, list it in the skills table in `CLAUDE.md`, and run `scripts/check_hygiene.sh`.

## Releases

See [RELEASING.md](RELEASING.md). A change users will notice gets a line under `## Unreleased` in the package's `CHANGELOG.md`. Versions are set by `scripts/release_version.py`, from the `release` workflow or by hand for the .NET integrations; it also moves the changelog notes under the new version. Never move a published tag.
