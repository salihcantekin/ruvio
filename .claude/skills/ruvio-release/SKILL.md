---
name: ruvio-release
description: Use when the maintainer prepares a Ruvio release; covers version bumps, changelogs, pre-checks and which release workflow target publishes what.
---

# Release preparation

`docs/RELEASING.md` is the reference (workflows, targets, preview releases, checklist). If this
skill and that file disagree, the file and `.github/workflows/release.yml` are right.

## Fixed rules

- Only the maintainer starts a release. A Claude session prepares and checks; it never runs
  `release.yml`, `integration-packages.yml` or `site-azure.yml`, never publishes a package,
  never pushes a tag and never merges into `main`. `.claude/settings.json` denies those
  commands.
- Every workflow starts by hand (`workflow_dispatch`). Only `pr-gate` also runs on pull requests
  and pushes to `main` and `dev`, and `nightly` runs on a schedule; neither publishes anything.
  Do not add automatic triggers.
- `main` receives `dev` only as a release merge decided by the maintainer.
- The server crate (`ruvio`) is not published to crates.io (`publish = false`). The server
  ships as Docker images and as Linux, macOS and Windows binary archives.
- Actions minutes cost money: pick only the target that is needed, not `all`.
- The site reads published versions from the registries when a page loads; a release needs no
  page edit.

## What each target publishes

| Target | Result |
| --- | --- |
| `docker` | `salihcantekin/ruvio` image |
| `docker-ui` | `salihcantekin/ruvio-ui` image |
| `rust-client` | crate `ruvio-client` |
| `nuget` | `Ruvio.Client`, `Ruvio.Client.AspNetCore` |
| `server-binaries` | `ruvio` and `ruvio-ctl` archives |
| `ui-host` | `ruvio-ui` host archives |
| `npm`, `pypi`, `maven` | the Node, Python and Java clients |

The Go client has no registry: its version is the git tag `clients/go/vX.Y.Z`, which the
maintainer pushes. The .NET integrations go through `integration-packages`. Version inputs:
`server_version`, `rust_client_version`, `csharp_version` (both NuGet packages together),
`ui_version`, `node_version`, `python_version`, `java_version`. Empty means the next patch;
`docker`/`server-binaries` and `docker-ui`/`ui-host` share a version and refuse an empty one
when run alone.

## Steps

1. **What ships.** List the packages with something under `## Unreleased` in their
   `CHANGELOG.md` (root for the server, one per client and integration).
2. **Short pre-checks** (each well under the time budget):
   ```bash
   python3 scripts/check_package_metadata.py      # Python 3.11+
   python3 scripts/test_release_version.py
   cargo fmt --all --check
   git add -N . && bash scripts/check_hygiene.sh . origin/main
   ```
   and confirm `pr-gate` is green on the commit to release from; `release` does not check it.
3. **Integration packages only:** bump and commit the version first, on a branch with a pull
   request into `dev`:
   ```bash
   python3 scripts/release_version.py <Package> --write            # next patch
   python3 scripts/release_version.py <Package> --set 0.2.0 --write
   ```
4. **Hand over.** Give the maintainer one table (Package | Current | Next | Target | Inputs) and
   stop. The maintainer starts the workflow from the Actions tab.
5. **Verify after the run** when asked: the run finished green, and the registry shows the new
   version (crates.io, NuGet, Docker Hub, npm, PyPI, Maven Central). Report Package | Old |
   New | Verified; anything you could not see is "not verified".

## When something goes wrong

- `release_version.py` reports "not a major.minor.patch version": the input must be `x.y.z`.
- npm, PyPI and Maven need their environments and trusted publishing set up by the
  maintainer; if they are not, that target is skipped and the report says why.
- A re-run of a failed preview publishes the same number again; registries other than NuGet
  refuse it. A new run is needed instead.
- Starting a server with `--address` or `--data-dir` in the repository writes `ruvio.toml`;
  it is ignored by git and is never committed.
- CI is red: do not release. Fix the cause and run it again. Never turn it green by skipping a
  test.
