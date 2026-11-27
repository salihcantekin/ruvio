"""Rule checks for the history `primitives` writes (the format is in its docstring). The
linearizability of every lock key is checked separately by `tests/soak/porcupine`.

What counts as wrong:
- duplicate_token / token_regression: two grants of one key got the same token, or a grant that
  began after another grant finished got a smaller or equal token.
- double_holder: a grant whose whole call lies inside the time another holder certainly held the
  lock (from its grant's reply to its release call, or to `ttl` after its grant call) and that
  brought a lease above 1 holder, or a semaphore above `max` holders.
- lost_hold: a release of a still-live hold answered 0.
- sum_mismatch: the final balances of an account group do not add up to what was put in. Every
  `TRANSFER` is one atomic record, so this holds whatever happened to the transfers.
- negative: a balance below zero, in an acknowledged reply or at the end.
- lost / excess: an account that ends below or above what its acknowledged transfers allow, with
  every unknown transfer counted as applied or not.
- double_acquire: `IDEM BEGIN` answered `acquired` to two attempts on one request.
- double_effect: a request's effect counter ended above 1, or above the effects acknowledged or
  unknown.
- lost_effect: an acknowledged effect missing at the end.
- bad_replay: a `completed` answer that is not the result that was completed.

The verdict is `fail` for any count in the `durable` profile, `warn` in `default` (a primary that
is cut off can still answer, and an acknowledged write can be lost in a failover, without the
primary lease or `CLIENT DURABILITY`); `ok` otherwise. `sum_mismatch`, `negative`, `duplicate_token`,
`double_acquire` and `bad_replay` fail in both profiles: no failover explains them."""

from __future__ import annotations

import json
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

FUNCTIONS = {"lease", "semaphore", "release", "init", "transfer", "begin", "effect", "complete", "final"}
COUNTS = (
    "duplicate_token", "token_regression", "double_holder", "lost_hold", "sum_mismatch", "negative",
    "lost", "excess", "double_acquire", "double_effect", "lost_effect", "bad_replay", "unread",
)
ALWAYS_FAIL = {"sum_mismatch", "negative", "duplicate_token", "double_acquire", "bad_replay", "unread"}
INFINITY = float("inf")


class Op:
    __slots__ = ("p", "f", "k", "pr", "args", "v", "start", "end", "type")

    def __init__(self, entry):
        self.p, self.f, self.k, self.pr = entry["p"], entry["f"], entry["k"], entry["pr"]
        self.args, self.v, self.start, self.end, self.type = entry.get("v"), None, entry["t"], INFINITY, "info"


def operations(paths: list[Path]) -> list[Op]:
    """The primitives' operations, paired. An operation without a completion is unknown."""
    open_ops: dict[tuple[str, int], Op] = {}
    done: list[Op] = []
    for path in paths:
        with open(path) as file:
            for line in file:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("f") not in FUNCTIONS:
                    continue
                ident = (entry["p"], entry["i"])
                if entry["type"] == "invoke":
                    open_ops[ident] = Op(entry)
                elif ident in open_ops:
                    op = open_ops.pop(ident)
                    op.type, op.end, op.v = entry["type"], entry["t"], entry.get("v")
                    done.append(op)
    done.extend(open_ops.values())
    done.sort(key=lambda op: op.start)
    return done


class Result:
    def __init__(self):
        self.counts: dict[str, dict[str, int]] = defaultdict(lambda: dict.fromkeys(COUNTS, 0))
        self.examples: list[str] = []

    def note(self, profile: str, count: str, text: str, amount: int = 1) -> None:
        if amount:
            self.counts[profile][count] += amount
            if len(self.examples) < 30:
                self.examples.append(f"{profile} {count}: {text}")


def _holds(grants: list[Op], releases: dict, ttl_ns: dict, result: Result):
    """For each granted op the time interval in which it certainly held the lock."""
    holds = []
    for grant in grants:
        release = releases.get((grant.p, grant.v))
        end = grant.start + ttl_ns[grant.k]
        if release is not None:
            end = min(end, release.start)
            if release.type == "ok" and release.v == 0 and release.end < grant.start + ttl_ns[grant.k]:
                result.note(grant.pr, "lost_hold", f"{grant.k}: token {grant.v} was gone when released")
        if end > grant.end:
            holds.append((grant.end, end, grant))
    return holds


def check_locks(ops: list[Op], result: Result) -> None:
    by_key: dict[str, list[Op]] = defaultdict(list)
    releases: dict[tuple[str, int], Op] = {}
    ttl_ns: dict[str, int] = {}
    maximum: dict[str, int] = {}
    for op in ops:
        if op.f in ("lease", "semaphore"):
            by_key[op.k].append(op)
            maximum[op.k], ttl_ns[op.k] = op.args[0], op.args[1] * 1_000_000
        elif op.f == "release" and op.type != "fail":
            releases.setdefault((op.p, op.args), op)
    for key, all_ops in by_key.items():
        grants = [op for op in all_ops if op.type == "ok" and op.v is not None]
        profile = all_ops[0].pr
        seen: dict[int, Op] = {}
        for grant in grants:
            if grant.v in seen:
                result.note(profile, "duplicate_token", f"{key}: token {grant.v} granted twice")
            seen[grant.v] = grant
        # A grant that began after another ended must have a larger token.
        by_end = sorted(grants, key=lambda op: op.end)
        ends = [op.end for op in by_end]
        best, index = (0, None), 0
        for grant in sorted(grants, key=lambda op: op.start):
            while index < len(by_end) and ends[index] < grant.start:
                if by_end[index].v > best[0]:
                    best = (by_end[index].v, by_end[index])
                index += 1
            if best[1] is not None and grant.v <= best[0]:
                result.note(profile, "token_regression", f"{key}: token {grant.v} after {best[0]} had been returned")
        holds = sorted(_holds(grants, releases, ttl_ns, result), key=lambda hold: hold[0])
        starts = [hold[0] for hold in holds]
        for grant in grants:
            # Holders that certainly held the lock during the whole call of this grant.
            count = 0
            for start, end, other in holds[: bisect_left(starts, grant.start)]:
                if other is not grant and end > grant.end:
                    count += 1
            if count >= maximum[key]:
                result.note(profile, "double_holder", f"{key}: token {grant.v} granted while {count} holders held it (max {maximum[key]})")


def check_transfers(ops: list[Op], result: Result) -> None:
    groups: dict[str, dict] = {}
    for op in ops:
        if op.f == "init" and op.type == "ok":
            groups[op.k] = {"init": dict(op.v), "pr": op.pr, "ok": defaultdict(int), "unknown": defaultdict(int), "in": defaultdict(int), "out": defaultdict(int)}
    for op in ops:
        if op.f == "transfer" and op.k in groups and op.type in ("ok", "info"):
            source, destination, amount = op.args
            group = groups[op.k]
            if op.type == "ok":
                if op.v is not None:
                    if min(op.v) < 0:
                        result.note(op.pr, "negative", f"{op.k}: reply {op.v}")
                    group["ok"][source] -= amount
                    group["ok"][destination] += amount
            else:
                group["out"][source] += amount
                group["in"][destination] += amount
    finals: dict[str, Op] = {op.k: op for op in ops if op.f == "final" and op.type != "info" and op.pr}
    for name, group in groups.items():
        total = 0
        readable = True
        for account, start in group["init"].items():
            final = finals.get(account)
            if final is None or final.type != "ok":
                result.note(group["pr"], "unread", f"{account}: final read failed")
                readable = False
                continue
            balance = final.v
            total += balance
            if balance < 0:
                result.note(group["pr"], "negative", f"{account}: ends at {balance}")
            expected = start + group["ok"][account]
            result.note(group["pr"], "lost", f"{account}: {balance} < {expected} - {group['out'][account]} unknown out", max(0, expected - group["out"][account] - balance) > 0)
            result.note(group["pr"], "excess", f"{account}: {balance} > {expected} + {group['in'][account]} unknown in", max(0, balance - expected - group["in"][account]) > 0)
        if readable and total != sum(group["init"].values()):
            result.note(group["pr"], "sum_mismatch", f"{name}: {total} != {sum(group['init'].values())}")


def check_idem(ops: list[Op], result: Result) -> None:
    acquired: dict[str, int] = defaultdict(int)
    effects: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    completed: dict[str, str] = {}
    profile: dict[str, str] = {}
    for op in ops:
        if op.f not in ("begin", "effect", "complete"):
            continue
        profile[op.k] = op.pr
        if op.f == "begin" and op.type == "ok":
            status = op.v[0]
            if status == "acquired":
                acquired[op.k] += 1
            elif status == "completed":
                completed.setdefault(op.k, op.v[1])
        elif op.f == "effect":
            effects[op.k][0 if op.type == "ok" else 1] += 1
    for key, number in acquired.items():
        result.note(profile[key], "double_acquire", f"{key}: acquired {number} times", number > 1)
    for key, result_text in completed.items():
        expected = f"done-{key.rsplit(':', 1)[1]}"
        result.note(profile[key], "bad_replay", f"{key}: replayed {result_text!r}", result_text != expected)
    for op in ops:
        if op.f != "final" or op.k not in effects and op.k not in profile:
            continue
        ok, unknown = effects[op.k]
        if op.type != "ok":
            result.note(op.pr, "unread", f"{op.k}: effect counter unreadable")
            continue
        result.note(op.pr, "double_effect", f"{op.k}: effect counter {op.v} (> 1 or > acknowledged + unknown)", op.v > 1 or op.v > ok + unknown)
        result.note(op.pr, "lost_effect", f"{op.k}: counter {op.v} < {ok} acknowledged", op.v < ok)


def check_file(paths) -> dict:
    ops = operations(_paths(paths))
    result = Result()
    check_locks(ops, result)
    check_transfers(ops, result)
    check_idem(ops, result)
    verdict, problems = "ok", []
    for profile, counts in sorted(result.counts.items()):
        for name, number in counts.items():
            if not number:
                continue
            serious = profile == "durable" or name in ALWAYS_FAIL
            problems.append(f"{profile}: {number} {name}")
            if serious:
                verdict = "fail"
            elif verdict == "ok":
                verdict = "warn"
    seen = defaultdict(int)
    for op in ops:
        seen[(op.pr, op.f, op.type)] += 1
    return {
        "verdict": verdict,
        "problems": problems,
        "counts": {profile: counts for profile, counts in result.counts.items()},
        "examples": result.examples,
        "operations": {f"{pr}/{f}/{t}": n for (pr, f, t), n in sorted(seen.items())},
    }


PORCUPINE = Path(__file__).resolve().parent.parent / "porcupine"


def _paths(paths) -> list[Path]:
    return [Path(paths)] if isinstance(paths, (str, Path)) else [Path(p) for p in paths]


def linearizability(paths, timeout_s: int = 120) -> dict:
    """Runs the Go checker in `tests/soak/porcupine` on the history. Without a Go toolchain the
    check is reported as `not run`, never as passed."""
    import shutil
    import subprocess

    if shutil.which("go") is None:
        return {"ran": False, "reason": "no go toolchain", "illegal": [], "unknown": []}
    try:
        out = subprocess.run(
            ["go", "run", ".", str(timeout_s), *map(str, _paths(paths))], cwd=PORCUPINE, capture_output=True, text=True, timeout=timeout_s + 120
        )
        return {"ran": True, **json.loads(out.stdout)}
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as error:
        return {"ran": False, "reason": str(error)[:200], "illegal": [], "unknown": []}


def check_history(paths) -> dict:
    """The rule checks plus the linearizability check of the lock keys."""
    result = check_file(paths)
    porcupine = linearizability(paths)
    result["linearizability"] = porcupine
    for key in porcupine["illegal"]:
        result["problems"].append(f"linearizability: {key} is not linearizable")
        result["verdict"] = "fail"
    if not porcupine["ran"]:
        result["problems"].append(f"linearizability not checked: {porcupine['reason']}")
        if result["verdict"] == "ok":
            result["verdict"] = "warn"
    elif porcupine["unknown"]:
        result["problems"].append(f"linearizability undecided (timeout) for {len(porcupine['unknown'])} keys")
        if result["verdict"] == "ok":
            result["verdict"] = "warn"
    return result
