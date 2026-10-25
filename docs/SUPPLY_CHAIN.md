# Supply Chain

This policy covers how Ruvio audits Rust dependencies, what licenses are
accepted, how a software bill of materials is produced, and how to report a
vulnerability.

## Auditing

`deny.toml` at the repository root is the machine-readable policy.
`cargo deny check` evaluates:

- **advisories** — RustSec vulnerabilities and yanked crates fail the check
- **licenses** — only the listed OSI-style permissive licenses
- **bans** — no version wildcards; multiple versions of the same crate warn
- **sources** — crates.io only; git and unknown registries are denied

The PR gate runs `cargo deny check bans licenses sources` on every pull request, and the
nightly workflow runs `cargo deny check advisories` (a new RustSec entry should not fail an
unrelated pull request; a failing night opens the `nightly-failure` issue). The manual CI
workflow runs the full check. Locally:

```bash
cargo install cargo-deny --locked
cargo deny check
```

A new advisory that blocks the graph is a release blocker until the crate is
upgraded, replaced, or an explicit `ignore` with a reason is added in
`deny.toml`. Do not ignore without a tracking note.

The process does not depend on Glommio. Production TLS PEM parsing uses
`rustls-pki-types`, not `rustls-pemfile`.

The server crate declares `license = "AGPL-3.0-only"`, the text of the repository
[`LICENSE`](../LICENSE). The operator UI declares `AGPL-3.0-only`
([`clients/ruvio-ui/LICENSE`](../clients/ruvio-ui/LICENSE)). The Rust client
crate and the .NET packages declare `license = "MIT"` (see
[`clients/LICENSE`](../clients/LICENSE) and
[`integrations/LICENSE`](../integrations/LICENSE)).
`scripts/check_package_metadata.py` checks these declarations and the registry
descriptions before a release. `cargo-deny` still skips unpublished path members
through `[licenses.private]`.

## SBOM

`scripts/generate_sbom.py` emits CycloneDX 1.5 JSON from `cargo metadata`.
`scripts/release_artifacts.sh` always writes `sbom.cdx.json` next to the
release binaries and checksums. The `release` workflow does not call it yet, so
a published GitHub Release carries checksums but no SBOM. If `cyclonedx-cargo` is installed it also
writes `sbom.cyclonedx-cargo.json` from that tool.

```bash
python3 scripts/generate_sbom.py --output dist/sbom.cdx.json
./scripts/release_artifacts.sh
```

The generated bill of materials lists crate name, version, and purl for every
package in the resolved graph, including optional Linux `io-uring` crates.
Signed SLSA provenance is not wired yet; see `docs/RELEASE_EVIDENCE.md`.

## Keeping dependencies and images current

Dependabot opens weekly pull requests for GitHub Actions, the Docker base images (server and
`ruvio-ui`, both pinned by digest), the Rust crates of the server, the Rust client and the UI, and
the UI's npm packages. The Go, Java, Node, Python and C# clients have no runtime dependencies, so
Dependabot does not watch them.

## Scanner image

`.oss-scanner/` holds the files Anthropic's [OSS Scanner](https://github.com/anthropics/oss-scanner)
reads when a project is enrolled there: `Dockerfile` (installs the pinned
toolchain, fetches every crate, builds `ruvio` and `ruvio-ctl` in debug, and
compiles the library tests) and `threat_model.md` (a short guide that points to
`docs/THREAT_MODEL.md`). Enrolment is a pull request to that repository and is
separate from this one. The files have no effect on the PR gate, CI, or any
release artifact. The image can be checked the way the scanner builds it with
`tools/check` from the OSS Scanner repository.

## Vulnerability reporting

Private reports go through GitHub Security Advisories. See `SECURITY.md`.
Do not file a public issue for an unreleased security defect.
