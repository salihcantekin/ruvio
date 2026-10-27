#!/usr/bin/env python3
"""Choose a package version and write it into the manifest.

An empty --set bumps the patch number already in the file. A set value is
written as given. The chosen version is printed on stdout.

With --write and a new version, the package's CHANGELOG.md "Unreleased"
section becomes the section of that version. --install-pins also rewrites the
`v=x.y.z` download line in the README and the site (server only; pass it when
the release publishes the server binaries).

--preview N prints a preview version instead: the next patch (or --set) with a
preview suffix, `X.Y.Z-preview.N` (`X.Y.Z.devN` for PyPI, which has no
pre-release label of that name, and `X.Y.Z-rc.N` for Maven, which would sort
`preview` after the release). --stamp VERSION writes a version into the
package files only, with no CHANGELOG move and no pins; the release workflow
stamps preview versions this way and never commits them.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
PREVIEW = re.compile(r"^\d+\.\d+\.\d+(-preview\.\d+|-rc\.\d+|\.dev\d+)$")

INTEGRATIONS = ROOT / "integrations" / "dotnet"

PACKAGES = {
    "server": {
        "cargo": [ROOT / "Cargo.toml"],
        "locks": [(ROOT / "Cargo.lock", "ruvio")],
        "csproj": [],
        "changelog": ROOT / "CHANGELOG.md",
        "pins": [ROOT / "README.md", ROOT / "docs" / "SETUP.md"],
    },
    "rust-client": {
        "cargo": [ROOT / "clients" / "ruvio-client" / "Cargo.toml"],
        "locks": [
            (ROOT / "clients" / "ruvio-client" / "Cargo.lock", "ruvio-client"),
            (ROOT / "clients" / "ruvio-ui" / "Cargo.lock", "ruvio-client"),
        ],
        "csproj": [],
        "matrix": "ruvio-client",
        "changelog": ROOT / "clients" / "ruvio-client" / "CHANGELOG.md",
    },
    "csharp": {
        "cargo": [],
        "locks": [],
        "csproj": [
            ROOT / "clients" / "csharp" / "Ruvio.Client" / "Ruvio.Client.csproj",
            ROOT
            / "clients"
            / "csharp"
            / "Ruvio.Client.AspNetCore"
            / "Ruvio.Client.AspNetCore.csproj",
        ],
        "matrix": "ruvio-csharp",
        "changelog": ROOT / "clients" / "csharp" / "CHANGELOG.md",
    },
    "ui": {
        "cargo": [ROOT / "clients" / "ruvio-ui" / "Cargo.toml"],
        "locks": [(ROOT / "clients" / "ruvio-ui" / "Cargo.lock", "ruvio-ui")],
        "csproj": [],
        "changelog": ROOT / "clients" / "ruvio-ui" / "CHANGELOG.md",
    },
    "node": {
        "file": ROOT / "clients" / "node" / "package.json",
        "kind": "node",
        "changelog": ROOT / "clients" / "node" / "CHANGELOG.md",
    },
    "python": {
        "file": ROOT / "clients" / "python" / "pyproject.toml",
        "kind": "python",
        "dunder": ROOT / "clients" / "python" / "ruvio" / "__init__.py",
        "changelog": ROOT / "clients" / "python" / "CHANGELOG.md",
    },
    "java": {
        "file": ROOT / "clients" / "java" / "pom.xml",
        "kind": "java",
        "changelog": ROOT / "clients" / "java" / "CHANGELOG.md",
    },
}

# The .NET integration packages are versioned one by one, under their package id.
for _name in (
    "Ruvio.Extensions.Caching",
    "Ruvio.AspNetCore.RateLimiting",
    "Ruvio.AspNetCore.Idempotency",
    "Ruvio.AspNetCore.OutputCaching",
    "Ruvio.AspNetCore.DataProtection",
    "Ruvio.AspNetCore.DistributedLock",
    "Ruvio.Extensions.Idempotency",
):
    PACKAGES[_name] = {
        "cargo": [],
        "locks": [],
        "csproj": [INTEGRATIONS / _name / f"{_name}.csproj"],
        "changelog": INTEGRATIONS / _name / "CHANGELOG.md",
    }


MATRIX = ROOT / "docs" / "compatibility-matrix.json"


def write_matrix_pin(path: Path, client_id: str, version: str) -> None:
    """Keep the client version pinned in the compatibility matrix equal to the manifest."""
    text = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'("id": "' + re.escape(client_id) + r'"[^}]*?"version": ")([^"]+)(")', re.S
    )
    text, count = pattern.subn(lambda m: m.group(1) + version + m.group(3), text, count=1)

    if count != 1:
        raise ValueError(f"{path} has no client {client_id}")

    path.write_text(text, encoding="utf-8")


UNRELEASED = "## Unreleased"
PIN = re.compile(r"(?m)^v=\d+\.\d+\.\d+$")


def release_changelog(path: Path, version: str, date: str) -> None:
    """Turn the "Unreleased" section into the section of `version`, and open a new empty one."""
    text = path.read_text(encoding="utf-8")
    start = text.find(UNRELEASED + "\n")

    if start < 0:
        raise ValueError(f"{path} has no '{UNRELEASED}' section")

    body_start = start + len(UNRELEASED) + 1
    end = text.find("\n## ", body_start)
    end = len(text) if end < 0 else end + 1
    body = text[body_start:end].strip() or "No recorded changes."
    section = f"{UNRELEASED}\n\n## {version} - {date}\n\n{body}\n"

    if end < len(text):
        section += "\n"

    path.write_text(text[:start] + section + text[end:], encoding="utf-8")


def write_install_pins(paths: list[Path], version: str) -> None:
    """Point the `v=x.y.z` line of each download snippet at `version`."""
    for path in paths:
        text, count = PIN.subn(f"v={version}", path.read_text(encoding="utf-8"))

        if count == 0:
            raise ValueError(f"{path} has no v=x.y.z line")

        path.write_text(text, encoding="utf-8")


def preview_version(package: str, base: str, number: int) -> str:
    """The preview version of `package` built on `base` (`X.Y.Z`): SemVer pre-release, or a
    PEP 440 development release for PyPI."""
    if SEMVER.fullmatch(base) is None:
        raise ValueError(f"not a major.minor.patch version: {base}")

    if number < 1:
        raise ValueError(f"preview number must be positive: {number}")

    kind = PACKAGES[package].get("kind")

    if kind == "python":
        return f"{base}.dev{number}"

    # Maven sorts an unknown qualifier such as `preview` after the release; `rc` sorts before it.
    if kind == "java":
        return f"{base}-rc.{number}"

    return f"{base}-preview.{number}"


def stamp_version(package: str, version: str) -> None:
    """Writes `version` into the package's manifests (and lock files) only."""
    if PREVIEW.fullmatch(version) is None and SEMVER.fullmatch(version) is None:
        raise ValueError(f"not a release or preview version: {version}")

    spec = PACKAGES[package]

    if "kind" in spec:
        write_other_version(spec, version)
        return

    for path in spec["cargo"]:
        write_cargo_version(path, version)

    for path, name in spec["locks"]:
        write_lock_version(path, name, version)

    for path in spec["csproj"]:
        write_csproj_version(path, version)


def bump_patch(version: str) -> str:
    match = SEMVER.fullmatch(version)

    if match is None:
        raise ValueError(f"not a major.minor.patch version: {version}")

    major, minor, patch = match.groups()

    return f"{major}.{minor}.{int(patch) + 1}"


def normalize(version: str) -> str:
    text = version.strip()

    if text.startswith("v"):
        text = text[1:]

    if SEMVER.fullmatch(text) is None:
        raise ValueError(f"not a major.minor.patch version: {version}")

    return text


def read_cargo_version(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith('version = "'):
            return line.split('"', 2)[1]

    raise ValueError(f"no package version in {path}")


def write_cargo_version(path: Path, version: str) -> None:
    current = read_cargo_version(path)
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace(f'version = "{current}"', f'version = "{version}"', 1), encoding="utf-8")


def write_lock_version(path: Path, name: str, version: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    replaced = False

    for index, line in enumerate(lines[:-1]):
        if line.strip() != "[[package]]":
            continue

        if lines[index + 1].strip() != f'name = "{name}"':
            continue

        lines[index + 2] = f'version = "{version}"\n'
        replaced = True
        break

    if not replaced:
        raise ValueError(f"{path} has no package {name}")

    path.write_text("".join(lines), encoding="utf-8")


def read_csproj_version(path: Path) -> str:
    match = re.search(r"<Version>([^<]+)</Version>", path.read_text(encoding="utf-8"))

    if match is None:
        raise ValueError(f"no Version in {path}")

    return match.group(1)


def write_csproj_version(path: Path, version: str) -> None:
    current = read_csproj_version(path)
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace(f"<Version>{current}</Version>", f"<Version>{version}</Version>", 1), encoding="utf-8")


def read_other_version(spec: dict) -> str:
    text = spec["file"].read_text(encoding="utf-8")
    kind = spec["kind"]

    if kind == "node":
        return json.loads(text)["version"]

    if kind == "python":
        match = re.search(r'^version = "([^"]+)"', text, re.M)
    else:
        match = re.search(r"<artifactId>ruvio-client</artifactId>\s*<version>([^<]+)</version>", text)

    if match is None:
        raise ValueError(f"no version in {spec['file']}")

    return match.group(1)


def write_other_version(spec: dict, version: str) -> None:
    path = spec["file"]
    current = read_other_version(spec)
    text = path.read_text(encoding="utf-8")
    kind = spec["kind"]

    if kind == "node":
        text = text.replace(f'"version": "{current}"', f'"version": "{version}"', 1)
        lock = path.with_name("package-lock.json")

        if lock.exists():
            lock_text = lock.read_text(encoding="utf-8")
            lock_text = lock_text.replace(f'"version": "{current}"', f'"version": "{version}"', 2)
            lock.write_text(lock_text, encoding="utf-8")
    elif kind == "python":
        text = text.replace(f'version = "{current}"', f'version = "{version}"', 1)
        dunder = spec.get("dunder")

        if dunder is not None and dunder.exists():
            module = dunder.read_text(encoding="utf-8")
            module, count = re.subn(
                r'(?m)^__version__ = "[^"]+"', f'__version__ = "{version}"', module, count=1
            )

            if count != 1:
                raise ValueError(f"no __version__ in {dunder}")

            dunder.write_text(module, encoding="utf-8")
    else:
        text = text.replace(
            f"<artifactId>ruvio-client</artifactId>\n  <version>{current}</version>",
            f"<artifactId>ruvio-client</artifactId>\n  <version>{version}</version>",
            1,
        )

    path.write_text(text, encoding="utf-8")


def current_version(package: str) -> str:
    spec = PACKAGES[package]

    if "kind" in spec:
        return read_other_version(spec)

    if spec["cargo"]:
        return read_cargo_version(spec["cargo"][0])

    return read_csproj_version(spec["csproj"][0])


def apply_version(
    package: str,
    requested: str,
    write: bool,
    install_pins: bool = False,
    date: str | None = None,
) -> str:
    current = current_version(package)
    spec = PACKAGES[package]

    if requested.strip():
        chosen = normalize(requested)
    elif write:
        chosen = bump_patch(current)
    else:
        chosen = current

    if write and install_pins:
        if "pins" not in spec:
            raise ValueError(f"{package} has no download pins")

        write_install_pins(spec["pins"], chosen)

    if not write or chosen == current:
        return chosen

    if "changelog" in spec:
        release_changelog(
            spec["changelog"], chosen, date or datetime.date.today().isoformat()
        )

    if "kind" in spec:
        write_other_version(spec, chosen)
        return chosen

    for path in spec["cargo"]:
        write_cargo_version(path, chosen)

    for path, name in spec["locks"]:
        write_lock_version(path, name, chosen)

    for path in spec["csproj"]:
        write_csproj_version(path, chosen)

    if "matrix" in spec:
        write_matrix_pin(MATRIX, spec["matrix"], chosen)

    return chosen


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("package", choices=sorted(PACKAGES))
    parser.add_argument("--set", default="", help="Exact version. Empty bumps the patch.")
    parser.add_argument("--write", action="store_true")
    parser.add_argument(
        "--install-pins",
        action="store_true",
        help="With --write, also point the README and site download lines at the version.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        metavar="N",
        help="Print the preview version N of the next patch (or of --set); writes nothing.",
    )
    parser.add_argument(
        "--stamp",
        metavar="VERSION",
        help="Write VERSION into the package files only (no CHANGELOG, no pins).",
    )
    args = parser.parse_args()

    if args.stamp:
        stamp_version(args.package, args.stamp)
        print(args.stamp)
        return

    if args.preview is not None:
        current = current_version(args.package)
        base = normalize(args.set) if args.set.strip() else bump_patch(current)
        print(preview_version(args.package, base, args.preview))
        return

    print(apply_version(args.package, args.set, args.write, args.install_pins))


if __name__ == "__main__":
    main()
