# Releasing

No release runs on its own. Every workflow in `.github/workflows/` is started by hand from the Actions tab (`workflow_dispatch`). Only `pr-gate` also runs on pull requests and on pushes to `main` and `dev`, and `nightly` runs on a schedule (02:17 UTC, from `main`); neither publishes anything.

| Workflow | What it does |
| --- | --- |
| `pr-gate` | Format, tests, client suites, package metadata check |
| `ci` | Wider checks: three operating systems, clippy, supply chain, every client against a standalone and a cluster server, the applications (tours, site, UI), fuzz, container and fault tests |
| `release` | Publishes the packages below; choose one target or `all` |
| `integration-packages` | Builds and optionally publishes the .NET integration packages |
| `site-azure` | Builds `site-next/` and publishes it to Azure Static Web Apps; target `production` builds `main` (ruviodb.com), target `dev` builds `dev` to the Ruvio-Dev app (secret `AZURE_STATIC_WEB_APPS_API_TOKEN_DEV`, noindex) |

## What `release` publishes

| Target | Result | Needs |
| --- | --- | --- |
| `docker` | `salihcantekin/ruvio` image (amd64, arm64) | `DOCKERHUB_TOKEN` |
| `docker-ui` | `salihcantekin/ruvio-ui` image (amd64, arm64) | `DOCKERHUB_TOKEN` |
| `rust-client` | crate `ruvio-client` | `CARGO_REGISTRY_TOKEN` |
| `nuget` | `Ruvio.Client`, `Ruvio.Client.AspNetCore` | NuGet trusted publishing |
| `server-binaries` | `ruvio` and `ruvio-ctl` archives for Linux x64 and arm64, macOS arm64, Windows x64 | none |
| `ui-host` | `ruvio-ui` host archives for Linux x64, macOS arm64, Windows x64 | none |
| `npm`, `pypi`, `maven` | the Node.js, Python and Java clients | trusted publishing (npm, PyPI) and Central Portal secrets (Maven), each behind its own environment |

The Go client has no registry; Go modules are versions of a git tag. The server itself is not published as a crate (`publish = false` in `Cargo.toml`).

Binary archives are workflow artifacts. Turn on `create_github_release` to attach them, with a `SHA256SUMS` file, to a GitHub Release. The binaries use the release profile (fat LTO, no profile-guided optimization). Performance numbers come from the builds described on each results page, so check those before quoting one for a downloaded binary.

Leave a version input empty to bump that package's patch number, or fill it to publish that exact version. `docker` and `server-binaries` share the server version, and `docker-ui` and `ui-host` share the UI version; run alone, these four targets refuse an empty version, so the two halves of a pair cannot ship under different numbers. The site shows the current published versions by asking the registries when a page loads. `server-binaries` (and `all`) also point the `v=x.y.z` download line in `README.md` at the server version, and the version commit carries it.

## Preview releases

Start `release` (or `integration-packages`) from the `dev` branch to publish previews. Every selected package gets its next patch with a preview suffix and the run number, `X.Y.Z-preview.N` (PyPI `X.Y.Z.devN`, Maven `X.Y.Z-rc.N`, which Maven sorts before the release; a filled version input sets `X.Y.Z`). The versions are written into each job's checkout only: nothing is committed to `dev`, and CHANGELOG notes stay under "Unreleased".

| Target | Preview |
| --- | --- |
| `nuget`, `rust-client`, integration packages | a pre-release version (`dotnet add package ... --prerelease`, `cargo add ruvio-client@X.Y.Z-preview.N`) |
| `maven` | `X.Y.Z-rc.N`; Central has no pre-release flag, so depend on that exact version |
| `npm` | published under the `preview` dist-tag (`npm install ruvio-client@preview`); `latest` is unchanged |
| `pypi` | a development release (`pip install --pre ruvio-client`) |
| `docker`, `docker-ui` | the `X.Y.Z-preview.N` and `preview` tags; `latest` and `linux` stay on the last release, and the Docker Hub description is not touched |
| `server-binaries`, `ui-host` | archives named with the preview version; with `create_github_release` the GitHub Release is marked pre-release |

Started from any other branch, the workflows publish releases as before.

The run number makes the version, so re-running a failed preview publishes the same number again (NuGet skips duplicates; crates.io, npm and PyPI refuse them): start a new run instead. An integration preview depends on the `Ruvio.Client` version in its repository (a project reference), which is the last client release, not a client preview.

## Changelogs

Every published package has a `CHANGELOG.md` next to its manifest (the server's is at the repository root, the C# client's in `clients/csharp/`, each .NET integration's in its project folder). Add a line under `## Unreleased` in the same change as the code. When `release_version.py --write` sets a new version, it moves those lines under `## <version> - <date>` and opens an empty `Unreleased`; a release with nothing recorded gets "No recorded changes."

## Integration packages

`integration-packages` does not choose versions. Bump the package first and commit the result:

```bash
python3 scripts/release_version.py Ruvio.AspNetCore.RateLimiting --write          # next patch
python3 scripts/release_version.py Ruvio.AspNetCore.RateLimiting --set 0.2.0 --write
```

With `publish` ticked, the workflow stops when NuGet already has the version in the project file, because the push skips duplicates and would otherwise pass without publishing. An integration references `Ruvio.Client` as a project, so its package depends on the client version in the repository; publish that client version first.

## Checklist

| Step | How |
| --- | --- |
| Changes recorded | `## Unreleased` in each package's `CHANGELOG.md` lists what users will notice |
| Branch green | `pr-gate` passed on the commit you release from; `release` does not check this |
| Version | Leave the input empty for the next patch, or fill it; the pair rule above applies to `docker`/`server-binaries` and `docker-ui`/`ui-host` |
| Server release | Run `server-binaries` with `create_github_release` on, then `docker` with the same `server_version` |
| Go client | No workflow: move its `Unreleased` notes under the version by hand and push the tag `clients/go/vX.Y.Z`. A published tag is never moved |
| Integration packages | Bump with `release_version.py`, commit, then run `integration-packages` with `publish` |
| npm | A trusted publish can land as "staged"; approve it on npmjs.com |
| Site | Run `site-azure` after the version commit if the site should show the new version |

