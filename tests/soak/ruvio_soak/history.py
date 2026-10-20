"""Checks an operation history written by `workload` (see its docstring for the format).

What counts as wrong:
- lost: an acknowledged write missing from the final state (a counter below its acknowledged
  increments, an acknowledged list value missing, a register below its last acknowledged
  version). Allowed in the `default` profile (counted and reported), never in `durable`.
- excess: a counter above acknowledged + unknown increments (an increment applied twice).
- phantom: a list value or register version that was never written, or one whose write was
  reported as certainly not applied.
- duplicate: a list value present twice.
- order: list values out of the order their single writer sent them.
- stale read: a register read that began after version v was acknowledged and returned an older
  one. Expected without the primary lease while an old primary is cut off, so it warns.
- backwards read: one reader saw a register go back to an older version.
- unread: a key whose final read failed; nothing can be said about it, so it fails the run.

The verdict is `fail` for any excess, phantom, duplicate, order, unread, or a loss in `durable`;
`warn` for losses in `default` or stale/backwards reads; `ok` otherwise."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

COUNTS = ("lost", "excess", "phantom", "duplicate", "order", "stale_read", "backwards_read", "unread")


def load(path: Path) -> list[dict]:
    entries = []
    with open(path) as file:
        for line in file:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    # A line cut by a crash of the controller; only the last one can be.
                    continue
    return entries


def pair(entries: list[dict]) -> list[dict]:
    """One record per operation: invoke time, completion time, outcome. An operation without a
    completion (the controller stopped) is unknown."""
    open_ops: dict[tuple[str, int], dict] = {}
    operations = []
    for entry in entries:
        key = (entry["p"], entry["i"])
        if entry["type"] == "invoke":
            open_ops[key] = {
                "p": entry["p"], "f": entry["f"], "k": entry["k"], "pr": entry["pr"],
                "v": entry.get("v"), "start": entry["t"], "end": None, "type": "info",
            }
        elif key in open_ops:
            op = open_ops.pop(key)
            op["type"], op["end"] = entry["type"], entry["t"]
            if entry["type"] == "ok":
                op["v"] = entry.get("v")
            operations.append(op)
    operations.extend(open_ops.values())
    operations.sort(key=lambda op: op["start"])
    return operations


def check(entries: list[dict]) -> dict:
    operations = pair(entries)
    by_key: dict[str, list[dict]] = defaultdict(list)
    for op in operations:
        by_key[op["k"]].append(op)
    profiles: dict[str, dict] = defaultdict(lambda: dict.fromkeys(COUNTS, 0) | {"ok": 0, "fail": 0, "info": 0, "keys": 0})
    examples: list[str] = []

    def note(profile, count, amount, text):
        if amount:
            profiles[profile][count] += amount
            if len(examples) < 20:
                examples.append(f"{profile} {count}: {text}")

    for key, ops in by_key.items():
        profile = ops[0]["pr"]
        stats = profiles[profile]
        stats["keys"] += 1
        writes = [op for op in ops if op["f"] in ("incr", "append", "write")]
        for op in ops:
            if op["f"] != "final":
                stats[op["type"]] += 1
        finals = [op for op in ops if op["f"] == "final"]
        final = finals[-1] if finals else None
        if not writes:
            continue
        if final is None or final["type"] != "ok":
            note(profile, "unread", 1, f"{key}: final read failed")
            continue
        kind = writes[0]["f"]
        acked = [op for op in writes if op["type"] == "ok"]
        unknown = [op for op in writes if op["type"] == "info"]
        failed = [op for op in writes if op["type"] == "fail"]
        if kind == "incr":
            value = final["v"]
            note(profile, "lost", max(0, len(acked) - value), f"{key}: {value} < {len(acked)} acknowledged")
            note(profile, "excess", max(0, value - len(acked) - len(unknown)),
                 f"{key}: {value} > {len(acked)} acknowledged + {len(unknown)} unknown")
        elif kind == "append":
            values = final["v"]
            present = set(values)
            acked_values = {op["v"] for op in acked}
            possible = acked_values | {op["v"] for op in unknown}
            lost = sorted(acked_values - present)
            note(profile, "lost", len(lost), f"{key}: acknowledged {lost[:5]} missing")
            phantom = sorted(present - possible)
            note(profile, "phantom", len(phantom), f"{key}: {phantom[:5]} never acknowledged or pending"
                 + (" (some reported as not applied)" if set(phantom) & {op["v"] for op in failed} else ""))
            note(profile, "duplicate", len(values) - len(present), f"{key}: duplicated values")
            out_of_order = sum(1 for a, b in zip(values, values[1:]) if b <= a)
            note(profile, "order", out_of_order, f"{key}: values out of order")
        elif kind == "write":
            written = {op["v"] for op in acked} | {op["v"] for op in unknown} | {0}
            top = max((op["v"] for op in acked), default=0)
            note(profile, "lost", 1 if final["v"] < top else 0, f"{key}: final {final['v']} < acknowledged {top}")
            if final["v"] not in written:
                note(profile, "phantom", 1, f"{key}: final {final['v']} was never written")
            reads = [op for op in ops if op["f"] == "read" and op["type"] == "ok"]
            acked_by_end = sorted((op["end"], op["v"]) for op in acked)
            last_seen: dict[str, int] = {}
            for read in reads:
                if read["v"] not in written:
                    note(profile, "phantom", 1, f"{key}: read {read['v']} was never written")
                floor = 0
                for end, version in acked_by_end:
                    if end >= read["start"]:
                        break
                    floor = max(floor, version)
                if read["v"] < floor:
                    note(profile, "stale_read", 1, f"{key}: read {read['v']} after {floor} was acknowledged")
                previous = last_seen.get(read["p"], 0)
                if read["v"] < previous:
                    note(profile, "backwards_read", 1, f"{key}: {read['p']} read {read['v']} after {previous}")
                last_seen[read["p"]] = read["v"]
    verdict = "ok"
    problems = []
    for profile, stats in sorted(profiles.items()):
        for count in ("excess", "phantom", "duplicate", "order", "unread"):
            if stats[count]:
                verdict = "fail"
                problems.append(f"{profile}: {stats[count]} {count}")
        if stats["lost"]:
            if profile == "durable":
                verdict = "fail"
            elif verdict == "ok":
                verdict = "warn"
            problems.append(f"{profile}: {stats['lost']} lost acknowledged write(s)")
        for count in ("stale_read", "backwards_read"):
            if stats[count]:
                verdict = "warn" if verdict == "ok" else verdict
                problems.append(f"{profile}: {stats[count]} {count.replace('_', ' ')}(s)")
    return {"verdict": verdict, "problems": problems, "profiles": dict(profiles), "examples": examples,
            "operations": len(operations)}


def check_file(path: Path) -> dict:
    return check(load(path))
