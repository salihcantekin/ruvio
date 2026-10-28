---
name: ruvio-work-habits
description: Use when running a multi-step Ruvio task; covers the time budget, small context, when to plan, a check after every step, screenshot checks for UI and parallel worktrees.
---

# Working habits for multi-step tasks

The project rules come first: move fast, verify for real but briefly, and write "not measured"
for what cannot be measured. These habits keep a long task on track.

## 0. Time budget

- No command, measurement or test run takes more than about 5 minutes. Anything longer only
  when the maintainer asks for it, with the expected time stated first.
- The full suite (`cargo test --lib --bins --tests`) runs only with the maintainer's explicit
  approval. It is not part of a plan by default, including before a release merge.
- **Tests are never built with `--release`.** The `release` profile is fat LTO with one
  codegen unit and re-optimises the whole crate on every change. Tests build in the debug
  profile (incremental, seconds) or with `--profile release-fast` when they need optimized
  code.
- Pass/fail harnesses (`durability_scenarios.py`, `model_check.py`, `disk_faults.py`,
  `slab_fuzz.py` and similar) use `target/release-fast/ruvio`. `target/release` is only for
  measurements that produce published numbers against Redis (`redis_compare.py`,
  `bench_matrix.py`, `ceiling.sh`, `profile_linux.sh`, `pgo.sh`), built once for that purpose.
- Run long jobs in the background with a time limit and keep working meanwhile.

## 1. Keep the context small

- One task per session or branch. When the subject changes, start fresh.
- Read the part of a file you need: `rg -n` first, then a line range. Cut long output:
  `> target/x.log 2>&1; tail -30 target/x.log`.
- Large logs, benchmark output or CI logs: summarise them (or let a subagent read them and
  return only the summary) instead of pasting them into the conversation.

## 2. Plan only large changes

- A large structural change (hot path, persistence format, shard model, a multi-file
  refactor): a short impact analysis first, with the steps, the risks and how each step will be
  verified, approved by the maintainer before coding.
- A small, reversible change: do it, without asking for a plan.
- Ask a question only when the answer changes the goal or an irreversible step; ask once, with
  options and a recommendation.

## 3. A check after every step

Put a check right after each "do" step in the task list, and do not move on until it passes.

| What changed | Check right after |
| --- | --- |
| A server command | the targeted test (`scripts/test_area.sh engine <name>`) and a few calls against a real server |
| Code near the hot path | `cargo test --test zero_alloc --test layout` (debug or `--profile release-fast`) |
| A client method | that language's live test through `scripts/run_with_servers.sh target/debug/ruvio 17400 -- <test command>` |
| A doc | the command or setting it describes exists in the code (`rg`), and its links resolve |
| A site page or `ruvio-ui` | the screenshot loop (section 4) |
| A workflow YAML | `python3 -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" <file>`, and the trigger is `workflow_dispatch` only |

"It compiled" is not evidence. Say plainly what you could not verify.

## 4. Screenshot loop for the site and the UI

When `site-next/` or `clients/ruvio-ui/` changes, do two or three rounds before showing it:
take a screenshot, look at it, fix, take it again. Check desktop (1280 px) and phone (390 px)
widths and make sure the console shows no errors. `clients/ruvio-ui/web` has Playwright tests
(`npm run test:browser`); for the site, build it (`npm run build` in `site-next/`) and capture
the page with Playwright or a headless browser. If no browser is available, write
"not verified visually". Errors from external resources (fonts, version badges) that the
sandbox cannot reach are not page errors; look at page errors and local files.

## 5. Stop early when the approach is wrong

- If the same failure survives two attempts, stop: change the approach and write down the root
  cause.
- If the result is only "good enough", try for better. When the maintainer pushes back, the
  second attempt is a different approach, not a patch of the first.

## 6. Make lessons stick

When a correction or a recurring trap comes up, propose an update to `CLAUDE.md` or to the
relevant skill in `.claude/skills/` in the same pull request or a follow-up one. Keep
`CLAUDE.md` short: rules every session needs; procedures belong in a skill.

## 7. Parallel work

- Two sessions never write to the same checkout. Give each parallel task its own worktree:
  `git worktree add .worktrees/<name> -b <branch> dev` (`.worktrees/` is ignored by git).
  When the branch is merged into `dev`, remove the worktree and delete the branch.
- Subagents that write to the same files run in separate worktrees; read-only ones do not
  need one.
- A subagent does not know the project rules by itself: put the time budget and the
  build-profile rule from section 0 in its brief.
- Watch a pull request's checks instead of polling with `sleep`, and do not start CI runs
  needlessly (Actions minutes cost money).

## 8. Permissions

`.claude/settings.json` allows the common build, test and read-only commands and denies
destructive and publishing ones (deny wins over allow). Personal additions go in
`.claude/settings.local.json`, which is not committed. Do not widen the shared file to get
past a denied command; a denied command is a step for the maintainer.
