#!/usr/bin/env python3
"""Fail CI when the compatibility matrix drifts from parser, docs, or coverage."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "docs" / "compatibility-matrix.json"
COMPATIBILITY_PATH = ROOT / "docs" / "COMPATIBILITY.md"
DATA_TYPES_PATH = ROOT / "docs" / "DATA_TYPES.md"
PARSE_PATH = ROOT / "src" / "protocol" / "parse" / "mod.rs"
TABLE_PATH = ROOT / "src" / "protocol" / "table.rs"

COMMAND_NAME = re.compile(r"[A-Z][A-Z0-9._]*(?: [A-Z][A-Z0-9._]*)*")
TABLE_NAME = re.compile(r"CommandSpec::migrated\(\s*\"([a-z0-9._]+)\"")
NAME_CASE = re.compile(r"name\.eq_ignore_ascii_case\(b\"([A-Z0-9.]+)\"\)")
COMPOSITE = {
    "ACL": (
        "ACL WHOAMI",
        "ACL USERS",
        "ACL LIST",
        "ACL GETUSER",
        "ACL CAT",
        "ACL SETUSER",
        "ACL LOAD",
        "ACL SAVE",
    ),
    "CLUSTER": (
        "CLUSTER SLOTS",
        "CLUSTER NODES",
        "CLUSTER SHARDS",
        "CLUSTER INFO",
        "CLUSTER MYID",
        "CLUSTER KEYSLOT",
    ),
    "CONFIG": ("CONFIG GET", "CONFIG SET", "CONFIG REWRITE"),
    "RUVIO": ("RUVIO", "RUVIO MOVESLOTS"),
    "SCRIPT": (
        "SCRIPT LOAD",
        "SCRIPT EXISTS",
        "SCRIPT FLUSH",
        "SCRIPT KILL",
    ),
    "FUNCTION": (
        "FUNCTION LOAD",
        "FUNCTION LIST",
        "FUNCTION DELETE",
    ),
    "XGROUP": (
        "XGROUP CREATE",
        "XGROUP CREATECONSUMER",
        "XGROUP DESTROY",
        "XGROUP SETID",
        "XGROUP DELCONSUMER",
    ),
    "XINFO": (
        "XINFO STREAM",
        "XINFO GROUPS",
        "XINFO CONSUMERS",
    ),
    "CLIENT": (
        "CLIENT ID",
        "CLIENT LIST",
        "CLIENT INFO",
        "CLIENT KILL",
        "CLIENT GETNAME",
        "CLIENT SETNAME",
        "CLIENT TRACKING",
        "CLIENT DURABILITY",
    ),
}
DATA_TYPE_FAMILIES = {
    "strings",
    "lists",
    "sets",
    "hashes",
    "zsets",
    "bloom",
    "streams",
    "transactions",
    "scripts",
}


def load_matrix(path: Path = MATRIX_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_module(root: Path, relative: str) -> str | None:
    """Source of a file, or of every `.rs` file under a directory written as `src/x/`.

    A path that no longer exists is reported, so a module split must update the matrix.
    """
    path = root / relative
    if relative.endswith("/") and path.is_dir():
        return "\n".join(
            file.read_text(encoding="utf-8") for file in sorted(path.rglob("*.rs"))
        )
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return None


# Where each client's released version lives, so the matrix pin cannot go stale.
CLIENT_MANIFESTS = {
    "ruvio-client": ("clients/ruvio-client/Cargo.toml", r'(?m)^version\s*=\s*"([^"]+)"'),
    "ruvio-csharp": (
        "clients/csharp/Ruvio.Client/Ruvio.Client.csproj",
        r"<Version>([^<]+)</Version>",
    ),
}


def manifest_version(root: Path, client_id: str) -> str | None:
    entry = CLIENT_MANIFESTS.get(client_id)
    if entry is None or not (root / entry[0]).is_file():
        return None
    found = re.search(entry[1], (root / entry[0]).read_text(encoding="utf-8"))
    return found.group(1) if found else None


def extract_parser_commands(source: str, table: str = "") -> set[str]:
    start = source.index("pub(crate) fn parse_arguments")
    names = set(NAME_CASE.findall(source[start:]))
    names.update(name.upper() for name in TABLE_NAME.findall(table))
    commands: set[str] = set()
    for name in names:
        if name in COMPOSITE:
            commands.update(COMPOSITE[name])
        else:
            commands.add(name)
    return commands


def extract_table_commands(markdown: str, heading: str, next_heading: str) -> set[str]:
    section = markdown.split(heading, 1)[1].split(next_heading, 1)[0]
    commands: set[str] = set()
    for line in section.splitlines():
        if not line.startswith("|") or "---" in line or line.startswith("| Family") or line.startswith("| Type"):
            continue
        columns = [column.strip() for column in line.strip().strip("|").split("|")]
        if len(columns) < 2:
            continue
        for match in re.finditer(r"`([^`]+)`", columns[1]):
            name = match.group(1)
            if COMMAND_NAME.fullmatch(name):
                commands.add(name)
    return commands


def command_mentioned(name: str, text: str) -> bool:
    if " " in name:
        return name in text
    return re.search(rf'(?<![A-Z.]){re.escape(name)}(?![A-Z.])', text) is not None


def validate(matrix: dict, root: Path = ROOT) -> list[str]:
    errors: list[str] = []
    if matrix.get("version") != 1:
        errors.append("matrix version must be 1")
    protocol = matrix.get("protocol") or {}
    if protocol.get("baseline") != "RESP2":
        errors.append("protocol.baseline must be RESP2")
    if protocol.get("resp3") is not True:
        errors.append("protocol.resp3 must be true once HELLO is supported")

    commands = matrix.get("commands") or []
    by_name = {}
    for command in commands:
        name = command.get("name")
        if not name or not COMMAND_NAME.fullmatch(name):
            errors.append(f"invalid command name: {name!r}")
            continue
        if name in by_name:
            errors.append(f"duplicate command {name}")
        by_name[name] = command
        if command.get("status") != "supported":
            errors.append(f"{name} status must be supported")
        if not command.get("coverage"):
            errors.append(f"{name} is missing coverage")
        if not isinstance(command.get("same_slot"), bool):
            errors.append(f"{name} same_slot must be a boolean")

    parser_commands = extract_parser_commands(
        (root / "src" / "protocol" / "parse" / "mod.rs").read_text(encoding="utf-8"),
        (root / "src" / "protocol" / "table.rs").read_text(encoding="utf-8"),
    )
    matrix_names = set(by_name)
    if parser_commands != matrix_names:
        missing = ", ".join(sorted(parser_commands - matrix_names))
        extra = ", ".join(sorted(matrix_names - parser_commands))
        if missing:
            errors.append(f"matrix missing parser commands: {missing}")
        if extra:
            errors.append(f"matrix lists commands the parser does not accept: {extra}")

    compatibility = extract_table_commands(
        (root / "docs" / "COMPATIBILITY.md").read_text(encoding="utf-8"),
        "## Current Commands",
        "## Data Semantics",
    )
    if compatibility != matrix_names:
        missing = ", ".join(sorted(matrix_names - compatibility))
        extra = ", ".join(sorted(compatibility - matrix_names))
        if missing:
            errors.append(f"COMPATIBILITY.md supported column missing: {missing}")
        if extra:
            errors.append(f"COMPATIBILITY.md lists extra commands: {extra}")

    data_types = extract_table_commands(
        (root / "docs" / "DATA_TYPES.md").read_text(encoding="utf-8"),
        "## Supported Commands",
        "## Current Limits",
    )
    typed = {
        command["name"]
        for command in commands
        if command.get("family") in DATA_TYPE_FAMILIES
    }
    if typed - data_types:
        errors.append(
            "DATA_TYPES.md missing: " + ", ".join(sorted(typed - data_types))
        )

    for command in commands:
        name = command["name"]
        for relative in command.get("coverage") or []:
            text = read_module(root, relative)
            if text is None:
                errors.append(f"{name} coverage file missing: {relative}")
                continue
            if not command_mentioned(name, text):
                errors.append(f"{name} is not mentioned in {relative}")

    client_coverage = matrix.get("client_coverage") or []
    if sorted(client_coverage) != client_coverage:
        errors.append("client_coverage must be sorted")
    unknown = set(client_coverage) - matrix_names
    if unknown:
        errors.append("client_coverage unknown commands: " + ", ".join(sorted(unknown)))

    clients = matrix.get("clients") or []
    expected_ids = {"ruvio-client", "ruvio-csharp"}
    ids = {client.get("id") for client in clients}
    if ids != expected_ids:
        errors.append(f"clients must cover {sorted(expected_ids)}, got {sorted(ids)}")
    for client in clients:
        suite = client.get("suite")
        if not suite or not (root / suite).is_file():
            errors.append(f"client suite missing: {suite}")
            continue
        text = (root / suite).read_text(encoding="utf-8")
        for name in client_coverage:
            if not command_mentioned(name, text):
                errors.append(f"{client['id']} suite does not cover {name}")
        if client.get("protocol") != "RESP2":
            errors.append(f"{client.get('id')} must pin RESP2")
        if not client.get("version"):
            errors.append(f"{client.get('id')} is missing a pinned version")
        released = manifest_version(root, client.get("id"))
        if released is not None and client.get("version") != released:
            errors.append(
                f"{client.get('id')} pins {client.get('version')} but the package is {released}"
            )

    errors.extend(stated_command_counts(len(commands), root))
    return errors


# Phrases that state how many commands the matrix holds, in the site and the READMEs.
COUNT_PHRASES = [
    re.compile(r"\b(\d{2,4}) of \1 commands"),
    re.compile(r"\b[Aa]ll (\d{2,4}) (?:matrix |listed )?commands"),
    re.compile(r"\bthe (\d{2,4})-command (?:matrix|check)"),
    re.compile(r"\b(\d{2,4}) (?:matrix|listed) commands"),
    re.compile(r"\*\*(\d{2,4})\*\* commands"),
    re.compile(r"\((\d{2,4})/(\d{2,4})\b"),
]


def stated_command_counts(count: int, root: Path = ROOT) -> list[str]:
    """A page that says "287 of 287 commands" must say the matrix's real count."""
    files = list((root / "site").glob("*.html")) + [root / "README.md"]
    files += [path for path in (root / "docs").glob("*.md")]
    files += [
        path
        for path in (root / "clients").glob("**/README.md")
        if "node_modules" not in path.parts and "target" not in path.parts
    ]
    errors = []

    for path in sorted(files):
        if not path.exists():
            continue

        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for pattern in COUNT_PHRASES:
                for match in pattern.finditer(line):
                    numbers = {int(group) for group in match.groups()}

                    if numbers != {count}:
                        errors.append(
                            f"{path.relative_to(root)}:{line_number} says {match.group(0)!r}; "
                            f"the matrix has {count} commands"
                        )

    return errors


def main() -> int:
    errors = validate(load_matrix())
    if errors:
        print("compatibility matrix check failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(
        f"compatibility matrix ok: {len(load_matrix()['commands'])} commands, "
        f"{len(load_matrix()['clients'])} client suites"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
