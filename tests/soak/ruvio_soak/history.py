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
`warn` for losses in `default` or stale/backwards reads; `ok` otherwise.

Every finding keeps its time (the monotonic clock of the history and of `events.jsonl`), so
`attribute` can tie it to the fault window it fell in. A finding outside every window has no
injected cause and is listed as unexplained.

A long history does not fit in memory as operations: `check_files` first splits the segments into
buckets by key (one pass), then checks one bucket at a time and adds the results up."""

from __future__ import annotations

import json
import shutil
import tempfile
import zlib
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

COUNTS = ("lost", "excess", "phantom", "duplicate", "order", "stale_read", "backwards_read", "unread")


def entries(path: Path):
    """The history file's entries, one at a time, so a long run's history is never held as
    parsed JSON all at once."""
    with open(path) as file:
        for line in file:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # A line cut by a crash of the controller; only the last one can be.
                    continue


def load(path: Path) -> list[dict]:
    return list(entries(path))


class Op:
    """One operation: invoke time, completion time, outcome. Slots keep a multi-million
    operation history within the controller's memory."""

    __slots__ = ("p", "f", "k", "pr", "v", "start", "end", "type")

    def __init__(self, p, f, k, pr, v, start):
        self.p, self.f, self.k, self.pr, self.v, self.start = p, f, k, pr, v, start
        self.end, self.type = None, "info"


def pair(entries) -> list[Op]:
    """One record per operation. An operation without a completion (the controller stopped) is
    unknown. `entries` may be any iterable, read once."""
    open_ops: dict[tuple[str, int], Op] = {}
    operations = []
    for entry in entries:
        key = (entry["p"], entry["i"])
        if entry["type"] == "invoke":
            open_ops[key] = Op(entry["p"], entry["f"], entry["k"], entry["pr"], entry.get("v"), entry["t"])
        elif key in open_ops:
            op = open_ops.pop(key)
            op.type, op.end = entry["type"], entry["t"]
            if entry["type"] == "ok":
                op.v = entry.get("v")
            operations.append(op)
    operations.extend(open_ops.values())
    operations.sort(key=lambda op: op.start)
    return operations


MAX_FINDINGS = 100000  # times kept for attribution; counts are always exact


def check(entries) -> dict:
    return verdict(analyse(entries))


def analyse(entries) -> dict:
    """Counts, examples and timed findings of one set of keys (no verdict yet)."""
    operations = pair(entries)
    by_key: dict[str, list[dict]] = defaultdict(list)
    for op in operations:
        by_key[op.k].append(op)
    profiles: dict[str, dict] = defaultdict(lambda: dict.fromkeys(COUNTS, 0) | {"ok": 0, "fail": 0, "info": 0, "keys": 0})
    examples: list[str] = []
    findings: list[list] = []  # [profile, count, mono_ns, key]

    def note(profile, count, amount, text, times=()):
        if amount:
            profiles[profile][count] += amount
            if len(examples) < 20:
                examples.append(f"{profile} {count}: {text}")
            for t in list(times)[:amount]:
                if t is not None and len(findings) < MAX_FINDINGS:
                    findings.append([profile, count, t, text.split(": ", 1)[0]])

    for key, ops in by_key.items():
        profile = ops[0].pr
        stats = profiles[profile]
        stats["keys"] += 1
        writes = [op for op in ops if op.f in ("incr", "append", "write")]
        for op in ops:
            if op.f != "final":
                stats[op.type] += 1
        finals = [op for op in ops if op.f == "final"]
        final = finals[-1] if finals else None
        if not writes:
            continue
        if final is None or final.type != "ok":
            note(profile, "unread", 1, f"{key}: final read failed", [final.start if final else None])
            continue
        kind = writes[0].f
        acked = [op for op in writes if op.type == "ok"]
        unknown = [op for op in writes if op.type == "info"]
        failed = [op for op in writes if op.type == "fail"]
        if kind == "incr":
            value = final.v
            # Which increments were lost is unknown; the newest acknowledged ones are the
            # likeliest (a tail lost with a primary), so their times stand for the loss.
            lost = max(0, len(acked) - value)
            note(profile, "lost", lost, f"{key}: {value} < {len(acked)} acknowledged",
                 [op.end for op in acked[len(acked) - lost:]] if lost else ())
            note(profile, "excess", max(0, value - len(acked) - len(unknown)),
                 f"{key}: {value} > {len(acked)} acknowledged + {len(unknown)} unknown", [final.start])
        elif kind == "append":
            values = final.v
            present = set(values)
            acked_values = {op.v for op in acked}
            possible = acked_values | {op.v for op in unknown}
            lost = sorted(acked_values - present)
            end_of = {op.v: op.end for op in acked}
            note(profile, "lost", len(lost), f"{key}: acknowledged {lost[:5]} missing", [end_of[v] for v in lost])
            phantom = sorted(present - possible)
            note(profile, "phantom", len(phantom), f"{key}: {phantom[:5]} never acknowledged or pending"
                 + (" (some reported as not applied)" if set(phantom) & {op.v for op in failed} else ""),
                 [final.start] * len(phantom))
            note(profile, "duplicate", len(values) - len(present), f"{key}: duplicated values",
                 [final.start] * (len(values) - len(present)))
            out_of_order = sum(1 for a, b in zip(values, values[1:]) if b <= a)
            note(profile, "order", out_of_order, f"{key}: values out of order", [final.start] * out_of_order)
        elif kind == "write":
            written = {op.v for op in acked} | {op.v for op in unknown} | {0}
            top = max((op.v for op in acked), default=0)
            top_end = next((op.end for op in acked if op.v == top), None)
            note(profile, "lost", 1 if final.v < top else 0, f"{key}: final {final.v} < acknowledged {top}", [top_end])
            if final.v not in written:
                note(profile, "phantom", 1, f"{key}: final {final.v} was never written", [final.start])
            reads = [op for op in ops if op.f == "read" and op.type == "ok"]
            # Acknowledged versions by completion time, with the running maximum, so each read
            # finds the newest version acknowledged before it began with one bisection.
            acked_by_end = sorted((op.end, op.v) for op in acked)
            ends = [end for end, _ in acked_by_end]
            floors = []
            for _, version in acked_by_end:
                floors.append(max(version, floors[-1]) if floors else version)
            last_seen: dict[str, int] = {}
            for read in reads:
                if read.v not in written:
                    note(profile, "phantom", 1, f"{key}: read {read.v} was never written", [read.start])
                before = bisect_left(ends, read.start)
                floor = floors[before - 1] if before else 0
                if read.v < floor:
                    note(profile, "stale_read", 1, f"{key}: read {read.v} after {floor} was acknowledged", [read.start])
                previous = last_seen.get(read.p, 0)
                if read.v < previous:
                    note(profile, "backwards_read", 1, f"{key}: {read.p} read {read.v} after {previous}", [read.start])
                last_seen[read.p] = read.v
    return {"profiles": {k: dict(v) for k, v in profiles.items()}, "examples": examples,
            "findings": findings, "operations": len(operations)}


def combine(parts: list[dict]) -> dict:
    total = {"profiles": {}, "examples": [], "findings": [], "operations": 0}
    for part in parts:
        total["operations"] += part["operations"]
        for profile, stats in part["profiles"].items():
            into = total["profiles"].setdefault(profile, dict.fromkeys(stats, 0))
            for name, value in stats.items():
                into[name] = into.get(name, 0) + value
        total["examples"].extend(part["examples"][: max(0, 40 - len(total["examples"]))])
        total["findings"].extend(part["findings"][: max(0, MAX_FINDINGS - len(total["findings"]))])
    return total


def verdict(result: dict) -> dict:
    profiles = result["profiles"]
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
    return dict(result, verdict=verdict, problems=problems)


def check_file(path: Path) -> dict:
    return check(entries(path))


def check_files(paths: list[Path], buckets: int = 1, work: Path | None = None) -> dict:
    """Checks a history split over several files. With `buckets` > 1 the entries are first
    spread over that many files by key (CRC-32), then each is checked on its own, so memory
    holds one bucket's operations at a time."""
    paths = [Path(p) for p in paths]
    if buckets <= 1:
        return verdict(analyse(e for path in paths for e in entries(path)))
    directory = Path(tempfile.mkdtemp(prefix="soak-buckets-", dir=work))
    try:
        files = [open(directory / f"bucket-{n:03d}.jsonl", "w", buffering=1 << 20) for n in range(buckets)]
        for path in paths:
            with open(path) as source:
                for line in source:
                    # The key is the only field a bucket needs; reading it without a full parse
                    # keeps this pass cheap. Entries are written by `json.dumps` with "k" quoted.
                    start = line.find('"k":"')
                    if start < 0:
                        continue
                    end = line.find('"', start + 5)
                    files[zlib.crc32(line[start + 5:end].encode()) % buckets].write(line)
        for file in files:
            file.close()
        parts = [analyse(entries(directory / f"bucket-{n:03d}.jsonl")) for n in range(buckets)]
    finally:
        shutil.rmtree(directory, ignore_errors=True)
    return verdict(combine(parts))


def fault_windows(events_path: Path, grace_s: float = 30.0) -> list[dict]:
    """Each injected fault as {kind, start, end} in monotonic ns: from the `fault` event to its
    `healed` (or `not-healed`) event plus `grace_s`, the time clients need to find the new
    primary and their retries to drain."""
    windows, open_fault = [], None
    if not Path(events_path).exists():
        return windows
    for entry in entries(Path(events_path)):
        if entry.get("event") == "fault":
            open_fault = {"kind": entry.get("kind"), "start": entry["mono"], "end": None}
        elif entry.get("event") in ("healed", "not-healed") and open_fault is not None:
            open_fault["end"] = entry["mono"] + int(grace_s * 1e9)
            windows.append(open_fault)
            open_fault = None
    if open_fault is not None:
        open_fault["end"] = None  # still open when the events stopped: runs to the end
        windows.append(open_fault)
    return windows


def attribute(result: dict, windows: list[dict]) -> dict:
    """{profile: {count: {"by_fault": {kind: n}, "unexplained": n, "examples": [...]}}} for the
    timed findings, each matched to the fault window it fell in (the earliest one when windows
    overlap)."""
    starts = [w["start"] for w in windows]
    out: dict = {}
    for profile, count, t, key in result.get("findings", []):
        slot = out.setdefault(profile, {}).setdefault(count, {"by_fault": {}, "unexplained": 0, "examples": []})
        index = bisect_left(starts, t + 1) - 1
        kind = None
        while index >= 0:
            window = windows[index]
            if window["start"] <= t and (window["end"] is None or t <= window["end"]):
                kind = window["kind"]
            index -= 1
            if kind or index < len(windows) - 50:  # windows are short; no need to look far back
                break
        if kind:
            slot["by_fault"][kind] = slot["by_fault"].get(kind, 0) + 1
        else:
            slot["unexplained"] += 1
            if len(slot["examples"]) < 10:
                slot["examples"].append({"key": key, "mono": t})
    return out
