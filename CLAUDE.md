# Ruvio: working agreement for Claude Code

Ruvio is a Redis-compatible server written in Rust, with six first-party clients and a set
of .NET integrations. This file is the project's working agreement for any Claude Code
session in this repository. It does not repeat the docs; it says which one to read and which
rules are not negotiable.

## Read before you change anything

| Topic | Where |
| --- | --- |
| Repository and server module maps; every place a new command, setting or client method has to reach | [docs/DEVELOPING.md](docs/DEVELOPING.md) |
| Pull request rules, hot-path rules, licensing, the C# layout rules | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Speed and memory rules, rejected designs, how to prove a change costs nothing | [docs/PERFORMANCE.md](docs/PERFORMANCE.md) |
| Test rules (time, ports, waiting, isolation), what CI runs | [docs/TESTING.md](docs/TESTING.md) |
| Workflows, release targets, changelogs | [docs/RELEASING.md](docs/RELEASING.md) |
| Which script to run when | [scripts/README.md](scripts/README.md) |

Read the part you need (`rg -n` first, then the line range), not whole directories.

## Speed budget

- No single command, build or test run takes more than about 5 minutes. Anything longer
  needs the maintainer's explicit approval, with the expected time stated up front.
- Never run the full suite (`cargo test --lib --bins --tests`, `scripts/test.sh`) without the
  maintainer's explicit approval.
- Build tests in the debug profile, or with `--profile release-fast` when they need optimized
  code. Never `--release`: the `release` profile is fat LTO with one codegen unit and rebuilds
  the whole crate on every change. It is only for binaries whose numbers are published.
- Per change, run only the touched area: `scripts/test_area.sh <area> [filter]`
  (areas: `store`, `protocol`, `engine`, `persistence`, `server`, `client-rust`,
  `client-csharp`). When a command path changed, also run
  `cargo test --test zero_alloc --test layout`.
- Run `scripts/check.sh` before every push. `scripts/check.sh --install-hook` installs it as a
  pre-push hook.

## Verification

- "It compiled" is not evidence. Prove behaviour with a targeted test, or with a short run
  against a real server. Client live tests skip without `RUVIO_TEST_ADDR`; run them with
  `scripts/run_with_servers.sh target/debug/ruvio 17400 -- <test command>`.
- Anything that cannot be measured in your environment (instruction counts without valgrind,
  Windows or macOS behaviour, production hardware) is written down as "not measured" or
  "not verified". Never claim it.

## Zero flaky tests

- Every wait has a deadline; no test may hang. No sleep followed by an assertion: poll the
  condition against a deadline.
- No fixed ports. Hand the server a listener bound to `127.0.0.1:0`, or take a block from the
  reserved port helpers (`test_ports::reserve()`, used through `tests/common`).
- Never make a test pass by skipping, disabling or re-running it. A red run is investigated
  and its cause fixed. See the `ruvio-test-reliability` skill.

## Performance

- Every command is at least as fast as Redis and uses no more memory.
- An unused feature costs nothing on the hot path: no added branch, allocation or lookup when
  it is off.
- `Value` stays 24 bytes (`tests/layout.rs`). Do not widen `Entry`.
- Shards are single-owner. Cross-shard work is a message to the owning shard, never shared
  state.
- Measure before and after on the hot path; the harnesses are in `scripts/`. The PR gate's
  instruction-count check allows +2% (`scripts/check_instruction_baseline.py`).

## Safety

- New `unsafe` only in a measured bottleneck, with a `// SAFETY:` comment that states why it
  holds.
- Changes to persistence, the WAL or snapshots, memory layout, hash tables or cross-thread
  code get an independent fresh-eyes review before merge (`ruvio-risky-review` skill).

## Public repository hygiene

- Everything in the repository is English: code, comments, docs, commit messages and pull
  request text.
- No internal planning identifiers anywhere: no task numbers, phase names, design-record
  numbers or plan file names. The local planning folders stay untracked (`.gitignore`).
- Every user-visible change, documentation included, gets a line under `## Unreleased` in the
  `CHANGELOG.md` of the package it changes: the root one for the server; each client and
  integration has its own.
- Docs describe the code as it is. A number in the docs comes from a recorded measurement in
  `docs/evidence/`.
- `scripts/check_hygiene.sh . origin/dev` (after `git add -N .`) must print `hygiene: clean`.

## Git flow

- Work on a feature branch from `dev`. Open a pull request into `dev` using
  `.github/PULL_REQUEST_TEMPLATE.md`. Merge commits, not squash. Delete the branch after merge.
- `main` receives `dev` only as a release merge decided by the maintainer.
- Never push to `main`, never push tags, never force-push a shared branch.
- Never run a release or publish workflow (`release.yml`, `integration-packages.yml`,
  `site-azure.yml`) and never publish a package.

## Clients

- The Rust, C#, Go, Java, Python and Node clients have zero runtime dependencies.
- A new server feature reaches every client, its README, the docs site and
  `docs/compatibility-matrix.json` (`python3 scripts/check_compatibility_matrix.py`).
- New client features get live-server tests that run in CI.

## Style

- `cargo fmt` on touched Rust files; clippy stays clean.
- C# layout: `dotnet run -c Release --project scripts/csharp-format`.
- Test names follow each language's convention: Rust and Python snake_case, Go `TestXxx`,
  C# PascalCase, Java camelCase, Node descriptive sentences.

## Skills for recurring tasks

The skills in `.claude/skills/` hold the step-by-step procedures:

| Skill | Use it when |
| --- | --- |
| `ruvio-add-command` | Adding or changing a server command |
| `ruvio-measure` | A change needs a before/after speed or memory measurement |
| `ruvio-performance-rules` | Writing or changing code in the server, a client or an integration |
| `ruvio-public-hygiene` | Before opening a pull request or merging: ids, language, names, docs vs code |
| `ruvio-release` | The maintainer prepares a release |
| `ruvio-risky-review` | A change touches unsafe code, persistence, layout, hash tables or threads |
| `ruvio-test-reliability` | Writing tests, or investigating a red or hung run |
| `ruvio-work-habits` | Running a multi-step task: context, planning, verification, parallel work |

`.claude/settings.json` allows the common build, test and read-only commands and denies
destructive and publishing ones. Personal overrides go in `.claude/settings.local.json`,
which is not committed.
