"""Turns a finished run directory into `report.json` and `report.md`: every criterion of a
production-readiness run with its measured value, its threshold and `pass`, `fail` or
`not_measured`. Nothing here is a judgement call: a criterion without data is `not_measured`,
never `pass`.

Windows (from `events.jsonl`): `quiet_a` is the fault-free stretch before the first fault,
without its first `settle_s` (data still filling, allocator warming up); `quiet_b` is the
fault-free stretch after the last fault, without its first `settle_s`. A run without faults
compares the first and the last third of the load after `settle_s` instead. A fault window runs
from its `fault` event to its `healed` event plus `grace_s`.

    python3 tests/soak/run.py report <run directory>"""

from __future__ import annotations

import bisect
import json
import statistics
import time
from collections import defaultdict
from pathlib import Path

from . import latency as lat
from .history import entries
from .resp import key_slot

PRIMARY_FAULTS = ("kill-primary", "kill-host", "isolate-host", "restart-all-data")


def read(path: Path):
    return list(entries(path)) if path.exists() else []


# A primary stops answering this long before the coordinators fail it over (`down_after`):
# writes it acknowledged then may be lost in the failover.
FAILOVER_BEFORE_S = 10.0


class Run:
    def __init__(self, directory: Path, settle_s: float = 3600, grace_s: float = 30):
        self.directory = Path(directory)
        self.plan = json.loads((self.directory / "plan.json").read_text())
        self.status = json.loads((self.directory / "status.json").read_text()) if (self.directory / "status.json").exists() else {}
        self.check = json.loads((self.directory / "check.json").read_text()) if (self.directory / "check.json").exists() else None
        self.events = read(self.directory / "events.jsonl")
        self.metrics = read(self.directory / "metrics.jsonl")
        self.hosts = read(self.directory / "hosts.jsonl")
        self.latency = read(self.directory / "latency.jsonl")
        self.background = [e for path in sorted(self.directory.glob("background-latency-*.jsonl")) for e in read(path)]
        self.findings = read(self.directory / "findings.jsonl")
        self.grace_s = grace_s
        self.windows = self._fault_windows()
        start = next((e["t"] for e in self.events if e["event"] == "load-start"), None)
        stop = next((e["t"] for e in self.events if e["event"] == "load-stop"), None)
        self.load = (start, stop)
        first = min((w[0] for w in self.windows), default=stop)
        last = max((w[1] for w in self.windows), default=start)
        settle = min(settle_s, 0.5 * ((first or 0) - (start or 0))) if start and first else settle_s
        self.quiet_a = (start + settle, first) if start and first and first - start > settle else None
        settle_b = min(settle_s, 0.5 * ((stop or 0) - (last or 0))) if stop and last else settle_s
        self.quiet_b = (last + settle_b, stop) if stop and last and stop - last > settle_b else None
        if not self.windows and start and stop:
            # No fault ran: the first and the last third of the load after the settle time, so
            # what grows with the data (checkpoints, the log) shows between them.
            begin = start + min(settle_s, 0.5 * (stop - start))
            third = (stop - begin) / 3
            self.quiet_a = (begin, begin + third)
            self.quiet_b = (stop - third, stop)

    def _fault_windows(self) -> list[tuple[float, float, str]]:
        """(start, end, kind) in epoch seconds."""
        windows, current = [], None
        for e in self.events:
            if e["event"] == "fault":
                current = (e["t"], e.get("kind"))
            elif e["event"] in ("healed", "not-healed") and current:
                windows.append((current[0], e["t"] + self.grace_s, current[1]))
                current = None
        if current:
            windows.append((current[0], float("inf"), current[1]))
        return windows

    def in_fault(self, start: float, end: float) -> bool:
        return any(w[0] < end and start < w[1] for w in self.windows)

    @staticmethod
    def inside(t: float, window) -> bool:
        return window is not None and window[0] <= t < window[1]


def criterion(results, ident, name, status, value=None, threshold=None, details=None):
    results.append({"id": ident, "name": name, "status": status, "value": value, "threshold": threshold,
                    "details": details})


def merged(entries_, window, prefix=None) -> dict[int, int]:
    histogram: dict[int, int] = {}
    for e in entries_:
        if Run.inside(e["t"], window) and (prefix is None or e["series"].startswith(prefix)):
            lat.merge(histogram, e.get("hist", {}))
    return histogram


def number(value) -> float | None:
    """A sample's value as a number: `INFO` fields arrive as strings."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return float(value) if "." in value else int(value)
        except ValueError:
            return None
    return None


def per_process(metrics, field, window) -> dict[str, list[float]]:
    values = defaultdict(list)
    for m in metrics:
        value = number(m.get(field))
        if Run.inside(m["t"], window) and value is not None:
            values[m["process"]].append(value)
    return values


def series_of(metrics, field, window) -> dict[str, list[tuple[float, float]]]:
    values = defaultdict(list)
    for m in metrics:
        value = number(m.get(field))
        if Run.inside(m["t"], window) and value is not None:
            values[m["process"]].append((m["t"], value))
    return values


def overhead_series(metrics, window) -> dict[str, list[tuple[float, float]]]:
    """Per data node, anonymous memory per byte the server counts for its data: a node whose
    data keeps growing grows with it, which is not a leak, while a leak raises this ratio."""
    values = defaultdict(list)
    for m in metrics:
        anon, charged = number(m.get("rss_anon_kib")), number(m.get("charged_memory_bytes"))
        if Run.inside(m["t"], window) and anon is not None and charged:
            values[m["process"]].append((m["t"], anon * 1024 / charged))
    return values


def slope(points) -> float | None:
    if len(points) < 3 or points[-1][0] - points[0][0] < 600:
        return None
    n = len(points)
    mt = sum(t for t, _ in points) / n
    mv = sum(v for _, v in points) / n
    var = sum((t - mt) ** 2 for t, _ in points)
    return sum((t - mt) * (v - mv) for t, v in points) / var * 3600 if var else 0.0


def evaluate(run: Run) -> dict:
    results: list[dict] = []
    check = run.check
    profiles = (check or {}).get("profiles", {})
    attribution = (check or {}).get("attribution", {})
    always_bad = ("excess", "phantom", "duplicate", "order", "unread")

    # 1-4: data
    if check is None:
        for ident in (1, 2, 3, 4):
            criterion(results, ident, "data checks", "not_measured", details="no check.json")
    else:
        durable = profiles.get("durable")
        if durable is None:
            criterion(results, 1, "durable: no anomaly of any kind", "not_measured")
        else:
            bad = {k: durable.get(k, 0) for k in ("lost", "stale_read", "backwards_read") + always_bad if durable.get(k)}
            criterion(results, 1, "durable: no anomaly of any kind", "fail" if bad else "pass", bad or 0, 0)
        details, failed = {}, False
        reattributed = _reattributed(run) if run.findings else {}
        for profile, stats in profiles.items():
            if profile == "durable":
                continue
            bad = {k: stats[k] for k in always_bad if stats.get(k)}
            unexplained = {k: v["unexplained"] for k, v in attribution.get(profile, {}).items() if v["unexplained"]}
            if run.findings:
                again = reattributed.get(profile, {})
                unexplained = {k: v["unexplained"] for k, v in again.items() if v["unexplained"]}
                by_failover = {k: v["uninjected_failover"] for k, v in again.items() if v["uninjected_failover"]}
            else:
                by_failover = None
            by_fault = {k: v["by_fault"] for k, v in attribution.get(profile, {}).items()}
            details[profile] = {"never_allowed": bad, "unexplained": unexplained, "lost": stats.get("lost", 0),
                                "stale_read": stats.get("stale_read", 0), "by_fault": by_fault,
                                "by_uninjected_failover": by_failover}
            failed = failed or bool(bad) or bool(unexplained)
        criterion(results, 2, "default and client: losses and stale reads only inside primary faults or failovers", "fail" if failed else "pass",
                  details=details)
        restarts = [w for w in run.windows if w[2] == "restart-all-data"]
        if not restarts:
            criterion(results, 3, "full restart: no acknowledged write lost on the `always` group", "not_measured", details="no restart-all-data fault ran")
        else:
            g1 = next((g for g in run.plan.get("cluster", {}).get("groups", [])), None)
            low, high = (0, 8191) if g1 is None else (int(x) for x in g1["slots"].split("-"))
            lost = [f for f in run.findings if f[1] == "lost" and low <= key_slot(f[3]) <= high
                    and any(w[2] == "restart-all-data" for w in run.windows if _mono_in(run, f[2], w))]
            criterion(results, 3, "full restart: no acknowledged write lost on the `always` group", "fail" if lost else "pass",
                      len(lost), 0, {"restarts": len(restarts)})
        moves = [w for w in run.windows if w[2] == "migrate-slots"]
        if not moves:
            criterion(results, 4, "slot moves: no anomaly during a move", "not_measured", details="no migrate-slots fault ran")
        else:
            during = sum(v["by_fault"].get("migrate-slots", 0) for p in attribution.values() for v in p.values())
            criterion(results, 4, "slot moves: no anomaly during a move", "fail" if during else "pass", during, 0, {"moves": len(moves)})

    # 5-8: availability
    healed = [e for e in run.events if e["event"] == "healed"]
    not_healed = [e for e in run.events if e["event"] == "not-healed"]
    after = _recovery_after_fault(run)
    if not after and not not_healed:
        criterion(results, 5, RECOVERY_NAME, "not_measured")
    else:
        by_kind = defaultdict(list)
        for kind, seconds in after:
            by_kind[kind].append(seconds)
        # A slot move is not a down time: the whole move counts, and it has its own ceiling.
        moves = by_kind.get("migrate-slots", [])
        rest = [seconds for kind, seconds in after if kind != "migrate-slots"]
        worst = max(rest, default=0)
        median = statistics.median(rest) if rest else None
        ok = (not not_healed and worst <= RECOVERY_MAX_S and (median or 0) <= RECOVERY_MEDIAN_S
              and max(moves, default=0) <= MOVE_MAX_S)
        criterion(results, 5, RECOVERY_NAME, "pass" if ok else "fail",
                  {"max_s": worst, "median_s": median, "move_max_s": max(moves, default=None),
                   "not_healed": len(not_healed)},
                  {"max_s": RECOVERY_MAX_S, "median_s": RECOVERY_MEDIAN_S, "move_max_s": MOVE_MAX_S},
                  {k: {"n": len(v), "max_s": max(v), "median_s": statistics.median(v)} for k, v in sorted(by_kind.items())})
    exits = [e for e in run.events if e["event"] == "unexpected-exit"]
    logs = run.status.get("log_counts")
    panics = sum(c.get("panic", 0) for c in (logs or {}).values())
    if logs is None:
        criterion(results, 6, "no unexpected exit, no panic", "fail" if exits else "not_measured", len(exits), 0, "no log counts")
    else:
        criterion(results, 6, "no unexpected exit, no panic", "fail" if exits or panics else "pass",
                  {"unexpected_exits": len(exits), "panics": panics}, 0,
                  {"errors": {k: v.get("error", 0) for k, v in logs.items()}, "warnings": {k: v.get("warn", 0) for k, v in logs.items()}})
    outside = defaultdict(int)
    minutes = 0
    for e in run.latency + run.background:
        # The minute [t, t + 60) overlaps no fault window (each already ends 30 s after its heal).
        if run.load[0] and Run.inside(e["t"], (run.load[0] + 60, run.load[1] or float("inf"))) and not run.in_fault(e["t"], e["t"] + 60):
            minutes += 1
            for outcome, n in e.get("counts", {}).items():
                if outcome != "ok":
                    outside[f"{e['series']}:{outcome}"] += n
    if not minutes:
        criterion(results, 7, "no failed or unknown operation outside fault windows", "not_measured")
    else:
        criterion(results, 7, "no failed or unknown operation outside fault windows", "fail" if outside else "pass",
                  sum(outside.values()), 0, dict(outside) or None)
    failovers = _failovers(run)
    primary_faults = sum(1 for w in run.windows if w[2] in PRIMARY_FAULTS)
    if failovers is None:
        criterion(results, 8, "failovers only from injected faults", "not_measured")
    else:
        # A primary fault can fail over at most every group whose primary it touched (kill-host,
        # isolate-host, restart-all: up to two groups); anything else is a failover nobody caused.
        allowed = 2 * primary_faults
        uninjected = _uninjected_failovers(run)
        ok = failovers <= allowed and not uninjected
        criterion(results, 8, "failovers only from injected faults", "pass" if ok else "fail",
                  {"failovers": failovers, "outside_faults": [time.strftime("%H:%M:%S", time.gmtime(t)) for t in uninjected]},
                  f"<= {allowed} (2 x {primary_faults} primary faults), none outside a fault window")

    # 9-12: performance
    base = merged(run.latency, run.quiet_a)
    late = merged(run.latency, run.quiet_b)
    if not base or not late:
        criterion(results, 9, "p99/p99.9 in the last quiet window within 20 % of the first", "not_measured")
    else:
        a, b = lat.summary(base), lat.summary(late)
        bad = [k for k in ("p99_ms", "p999_ms") if b[k] > a[k] * 1.2 + 0.2]
        criterion(results, 9, "p99/p99.9 in the last quiet window within 20 % of the first", "fail" if bad else "pass",
                  {"first": a, "last": b}, "last <= 1.2 x first + 0.2 ms", {"per_series": _per_series(run.latency, run.quiet_a, run.quiet_b)})
    if not base:
        criterion(results, 10, "p99 back to the quiet baseline within 60 s after each fault", "not_measured")
    else:
        limit = lat.summary(base)["p99_ms"] * 1.5 + 0.2
        slow = []
        for e in healed:
            window = (e["t"] + 60, e["t"] + 180)
            if run.in_fault(window[0], window[1]):
                continue  # the next fault already started: no clean minute to judge
            p99 = lat.summary(merged(run.latency, window)).get("p99_ms")
            if p99 is not None and p99 > limit:
                slow.append({"kind": e["kind"], "t": e["t"], "p99_ms": p99})
        criterion(results, 10, "p99 back to the quiet baseline within 60 s after each fault", "fail" if slow else "pass",
                  len(slow), f"p99 <= {limit:.2f} ms", slow[:20] or None)
    # The background load runs closed loop, so its rate follows the latency of the nodes that
    # hold the primaries: minutes are compared only with minutes of the same primary placement.
    placement = _placements(run)
    groups: dict[tuple, list[int]] = defaultdict(list)
    for minute in sorted({int(e["t"]) for e in run.background
                          if (Run.inside(e["t"], run.quiet_a) or Run.inside(e["t"], run.quiet_b))}):
        groups[placement(minute)].append(minute)
    totals = defaultdict(int)
    for e in run.background:
        totals[int(e["t"])] += e["counts"].get("ok", 0)
    judged = []
    for key, minutes in groups.items():
        rates = [totals[m] for m in minutes][1:-1]  # a run of minutes starts and ends partial
        if len(rates) >= 5:
            mean = statistics.mean(rates)
            spread = (max(rates) - min(rates)) / 2 / mean if mean else 1
            judged.append({"primaries": list(key), "minutes": len(rates), "ops_per_min_mean": round(mean),
                           "half_spread": round(spread, 4)})
    if not judged:
        criterion(results, 11, THROUGHPUT_NAME, "not_measured")
    else:
        worst = max(j["half_spread"] for j in judged)
        criterion(results, 11, THROUGHPUT_NAME, "pass" if worst <= 0.05 else "fail",
                  {"half_spread": worst, "placements": len(judged)}, 0.05, judged)
    lag = _replica_lag_s(run)
    if lag is None:
        criterion(results, 12, "replica lag p99 < 1 s in quiet windows", "not_measured")
    else:
        criterion(results, 12, "replica lag p99 < 1 s in quiet windows", "pass" if lag["p99_s"] < 1 else "fail", lag, 1)

    # 13-16: resources
    memory = _memory(run)
    criterion(results, 13, "no memory growth: data nodes and coordinators", memory["status"], memory.get("value"),
              "anon B <= 1.1 x A (+2 MiB) or anon/data ratio B <= 1.1 x A; in B the anon/data ratio rises < 2 %/h (data nodes) or anon < 512 KiB/h (coordinator)",
              memory.get("details"))
    fds = _flat(run, "fds_internal", 5)
    clients = _flat(run, "connected_clients_n", 50)
    threads = _flat(run, "threads", 2)
    status = "not_measured" if fds is None or threads is None else ("pass" if not fds["grew"] and not threads["grew"] else "fail")
    criterion(results, 14, "open descriptors (without client connections) and threads do not grow", status,
              {"fds": fds, "threads": threads}, "B mean <= A mean + 5 fds, + 2 threads", {"client_connections": clients})
    disk = _disk(run)
    criterion(results, 15, "WAL bounded, checkpoint in step with the data, free disk >= 20 %", disk["status"], disk.get("value"),
              "WAL B max <= 1.5 x A max + 64 MiB; checkpoint/data B <= 1.5 x A + 0.1; free >= 20 %", disk.get("details"))
    raft = per_process([m for m in run.metrics if m["process"].endswith("coordinator")], "data_dir_bytes",
                       (run.load[0] or 0, run.load[1] or float("inf")))
    if not raft:
        criterion(results, 16, "coordinator Raft data bounded (< 64 MiB growth)", "not_measured")
    else:
        growth = {p: max(v) - min(v) for p, v in raft.items()}
        criterion(results, 16, "coordinator Raft data bounded (< 64 MiB growth)",
                  "pass" if max(growth.values()) < 64 << 20 else "fail", growth, 64 << 20)

    # 17: the measurement itself
    gap = run.status.get("metric_gap_fraction")
    offsets = [abs(h["clock_offset_s"]) for h in run.hosts if isinstance(h.get("clock_offset_s"), (int, float))]
    busy = [h["cpu_busy"] for h in run.hosts if h.get("host") == "controller" and isinstance(h.get("cpu_busy"), (int, float))]
    uploads = run.status.get("upload_errors", 0)
    value = {"metric_gap_fraction": gap, "clock_offset_max_s": max(offsets, default=None),
             "controller_cpu_p95": _p(busy, 0.95), "upload_errors": uploads, "check_done": check is not None}
    if gap is None or not offsets or not busy:
        status = "not_measured"
    else:
        status = "pass" if gap < 0.01 and max(offsets) < 0.01 and _p(busy, 0.95) < 0.7 and check is not None else "fail"
    criterion(results, 17, "measurement complete: gaps < 1 %, clock < 10 ms, controller CPU p95 < 70 %, check done", status, value)

    statuses = [r["status"] for r in results]
    verdict = "fail" if "fail" in statuses else ("incomplete" if "not_measured" in statuses else "pass")
    return {"run_id": run.status.get("run_id"), "verdict": verdict, "windows": {
        "load": run.load, "quiet_a": run.quiet_a, "quiet_b": run.quiet_b, "faults": len(run.windows)},
        "criteria": results, "hourly": _hourly(run)}


RECOVERY_NAME = "every fault recovers: after the fault ends, max <= 15 s, median <= 5 s; a slot move <= 60 s"
RECOVERY_MAX_S = 15
RECOVERY_MEDIAN_S = 5
MOVE_MAX_S = 60


def _recovery_after_fault(run: Run) -> list[tuple[str, float]]:
    """Each healed fault's recovery time counted from the end of the fault: `recovery_s` runs
    from the injection, so it holds the fault's own down time, which the plan draws at random."""
    out = []
    down = 0.0
    for e in run.events:
        if e["event"] == "fault":
            down = float(e.get("down_s") or 0)
        elif e["event"] == "healed":
            out.append((e["kind"], round(max(0.0, float(e["recovery_s"]) - down), 2)))
            down = 0.0
    return out


THROUGHPUT_NAME = "background throughput steady (+-5 %) in quiet windows, per primary placement"


def _placements(run: Run):
    """A function from an epoch second to the primaries named by the latest metrics sample at
    or before it (a sorted tuple of process names; empty before the first sample)."""
    samples: dict[float, set[str]] = defaultdict(set)
    for m in run.metrics:
        if "replication_role" in m:
            samples[m["t"]]
            if m.get("replication_role") == "primary":
                samples[m["t"]].add(m["process"])
    times = sorted(samples)
    keys = [tuple(sorted(samples[t])) for t in times]

    def at(t: float) -> tuple:
        index = bisect.bisect_right(times, t) - 1
        return keys[index] if index >= 0 else ()

    return at


def _wall(run: Run, mono: int) -> float | None:
    """A finding's monotonic time in epoch seconds: the events carry both clocks."""
    pairs = [(e["mono"], e["t"]) for e in run.events if "mono" in e]
    if not pairs:
        return None
    m0, t0 = min(pairs, key=lambda p: abs(p[0] - mono))
    return t0 + (mono - m0) / 1e9


def _mono_in(run: Run, mono: int, window) -> bool:
    """Whether a finding's monotonic time falls in an epoch window."""
    t = _wall(run, mono)
    return t is not None and window[0] <= t <= window[1]


def _failover_times(run: Run) -> list[float]:
    """Epoch seconds of each failover the coordinators recorded (`last_failover_unix_ms`), as
    far as the samples show them: two failovers between samples count once here."""
    seen = set()
    for m in run.metrics:
        if m["process"].endswith("coordinator"):
            value = number(m.get("last_failover_unix_ms"))
            if value and (run.load[0] is None or value / 1000 >= run.load[0]):
                seen.add(int(value))
    times = []
    for ms in sorted(seen):
        if not times or ms / 1000 - times[-1] > 5:
            times.append(ms / 1000)
    return times


def _uninjected_failovers(run: Run) -> list[float]:
    """Failovers outside every fault window (a fault's window starts at the fault)."""
    return [t for t in _failover_times(run) if not any(w[0] - 1 <= t <= w[1] for w in run.windows)]


def _reattributed(run: Run) -> dict:
    """{profile: {count: {"uninjected_failover": n, "unexplained": n}}} for the findings outside
    every fault window: one from `FAILOVER_BEFORE_S` before an uninjected failover to the
    window's grace after it is tied to that failover, which criterion 8 judges; the rest stay
    unexplained."""
    failovers = _uninjected_failovers(run)
    out: dict = {}
    for profile, count, mono, _key in run.findings:
        t = _wall(run, mono)
        if t is None or any(w[0] <= t <= w[1] for w in run.windows):
            continue
        slot = out.setdefault(profile, {}).setdefault(count, {"uninjected_failover": 0, "unexplained": 0})
        if any(f - FAILOVER_BEFORE_S <= t <= f + run.grace_s for f in failovers):
            slot["uninjected_failover"] += 1
        else:
            slot["unexplained"] += 1
    return out


def _p(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def _per_series(entries_, a, b):
    out = {}
    for series in sorted({e["series"] for e in entries_}):
        ha = merged([e for e in entries_ if e["series"] == series], a)
        hb = merged([e for e in entries_ if e["series"] == series], b)
        if ha and hb:
            out[series] = {"first_p99_ms": lat.summary(ha)["p99_ms"], "last_p99_ms": lat.summary(hb)["p99_ms"]}
    return out


def _failovers(run: Run):
    """The increase of the leader's `failovers_total`, summed over leaders (each counts what it
    recorded while leading)."""
    per = defaultdict(list)
    for m in run.metrics:
        if m["process"].endswith("coordinator") and str(m.get("failovers_total", "")).isdigit():
            per[m["process"]].append(int(m["failovers_total"]))
    if not per:
        return None
    total = 0
    for values in per.values():
        # A restarted coordinator starts from 0 again: add every increase between samples.
        total += sum(max(0, b - a) for a, b in zip(values, values[1:]))
    return total


def _replica_lag_s(run: Run):
    """Seconds a replica trails its primary: the difference of their `wal_last_sequence`
    divided by the primary's write rate, per sample in the quiet windows."""
    samples = defaultdict(dict)  # (group, rounded t) -> {process: (role, seq)}
    for m in run.metrics:
        if not (Run.inside(m["t"], run.quiet_a) or Run.inside(m["t"], run.quiet_b)):
            continue
        seq = m.get("wal_last_sequence")
        if seq is None or not str(seq).isdigit():
            continue
        group = m["process"].split("-", 1)[1]
        samples[(group, round(m["t"] / 10))][m["process"]] = (m.get("replication_role"), int(seq), m["t"])
    lags = []
    previous: dict[str, tuple[float, int]] = {}
    for (group, _), members in sorted(samples.items(), key=lambda kv: kv[0][1]):
        primary = next(((p, v) for p, v in members.items() if v[0] == "primary"), None)
        if primary is None:
            continue
        name, (_, seq, t) = primary
        rate = None
        if name in previous and t > previous[name][0]:
            rate = (seq - previous[name][1]) / (t - previous[name][0])
        previous[name] = (t, seq)
        if not rate or rate <= 0:
            continue
        for process, (role, rseq, _) in members.items():
            if role == "replica":
                lags.append(max(0, seq - rseq) / rate)
    if not lags:
        return None
    return {"p50_s": round(_p(lags, 0.5), 3), "p99_s": round(_p(lags, 0.99), 3), "max_s": round(max(lags), 3), "samples": len(lags)}


def _memory(run: Run) -> dict:
    if run.quiet_a is None or run.quiet_b is None:
        return {"status": "not_measured", "details": "no quiet window at the start or the end"}
    a_anon = per_process(run.metrics, "rss_anon_kib", run.quiet_a)
    b_anon = per_process(run.metrics, "rss_anon_kib", run.quiet_b)
    charged_a = per_process(run.metrics, "charged_memory_bytes", run.quiet_a)
    charged_b = per_process(run.metrics, "charged_memory_bytes", run.quiet_b)
    b_series = series_of(run.metrics, "rss_anon_kib", run.quiet_b)
    b_overhead = overhead_series(run.metrics, run.quiet_b)
    if not a_anon or not b_anon:
        return {"status": "not_measured", "details": "no anonymous memory samples"}
    details, failed = {}, False
    for process in sorted(set(a_anon) & set(b_anon)):
        a, b = statistics.mean(a_anon[process]), statistics.mean(b_anon[process])
        entry = {"anon_a_kib": round(a), "anon_b_kib": round(b), "slope_b_kib_h": None}
        grew = b > 1.1 * a + 2048
        if grew and process in charged_a and process in charged_b:
            ca, cb = statistics.mean(charged_a[process]), statistics.mean(charged_b[process])
            if ca and cb:
                ra, rb = a * 1024 / ca, b * 1024 / cb
                entry.update(overhead_a=round(ra, 3), overhead_b=round(rb, 3))
                grew = rb > 1.1 * ra
        # Data nodes: once the server counts its data, the slope of memory per byte of data
        # (at most 2 % of the ratio per hour); otherwise the absolute slope.
        overhead = b_overhead.get(process, [])
        if len(overhead) >= 3:
            s = slope(overhead)
            mean = statistics.mean(v for _, v in overhead)
            entry["overhead_slope_per_h"] = None if s is None else round(s, 4)
            climbing = s is not None and s > 0.02 * mean
        else:
            s = slope(b_series.get(process, []))
            entry["slope_b_kib_h"] = None if s is None else round(s, 1)
            limit = 512 if process.endswith("coordinator") else 1024
            climbing = s is not None and s > limit
        entry["status"] = "fail" if grew or climbing else "pass"
        failed = failed or grew or climbing
        details[process] = entry
    return {"status": "fail" if failed else "pass", "value": {p: d["status"] for p, d in details.items()}, "details": details}


def _by_role(metrics, field, window) -> dict[str, list[float]]:
    """Values keyed by group and role (`g1:primary`, `g1:replica`) for data nodes and by process
    for coordinators: a primary holds more connections than a replica, and failovers move the
    roles between processes."""
    values = defaultdict(list)
    for m in metrics:
        if field == "connected_clients_n" and str(m.get("connected_clients", "")).isdigit():
            m = dict(m, connected_clients_n=int(m["connected_clients"]))
        if field == "fds_internal" and isinstance(m.get("fds"), int):
            # Descriptors besides client connections: clients keep pooled connections to a
            # node that stopped being the primary, which is the clients' choice, not a leak.
            clients = int(m["connected_clients"]) if str(m.get("connected_clients", "")).isdigit() else 0
            m = dict(m, fds_internal=m["fds"] - clients)
        if Run.inside(m["t"], window) and isinstance(m.get(field), (int, float)):
            if m["process"].endswith("coordinator"):
                values[m["process"]].append(m[field])
            elif m.get("replication_role") in ("primary", "replica"):
                values[f"{m['process'].split('-', 1)[1]}:{m['replication_role']}"].append(m[field])
    return values


def _flat(run: Run, field: str, allowance: float):
    if run.quiet_a is None or run.quiet_b is None:
        return None
    a, b = _by_role(run.metrics, field, run.quiet_a), _by_role(run.metrics, field, run.quiet_b)
    if not a or not b:
        return None
    grew = {p: (round(statistics.mean(a[p]), 1), round(statistics.mean(b[p]), 1)) for p in set(a) & set(b)
            if statistics.mean(b[p]) > statistics.mean(a[p]) + allowance}
    return {"grew": grew, "processes": len(set(a) & set(b))}


def _disk(run: Run) -> dict:
    """The WAL is bounded on its own; a checkpoint holds the data, so it is judged against
    what the server counts for its data (`charged_memory_bytes`) when that is known."""
    if run.quiet_a is None or run.quiet_b is None:
        return {"status": "not_measured", "details": "no quiet windows"}

    def peak(window, field):
        out = defaultdict(int)
        for m in run.metrics:
            value = number(m.get(field))
            if Run.inside(m["t"], window) and value is not None and not m["process"].endswith("coordinator"):
                out[m["process"]] = max(out[m["process"]], int(value))
        return out

    wal_a, wal_b = peak(run.quiet_a, "wal_bytes"), peak(run.quiet_b, "wal_bytes")
    cp_a, cp_b = peak(run.quiet_a, "checkpoint_bytes"), peak(run.quiet_b, "checkpoint_bytes")
    data_a, data_b = peak(run.quiet_a, "charged_memory_bytes"), peak(run.quiet_b, "charged_memory_bytes")
    free = [h["disk_avail_bytes"] / h["disk_size_bytes"] for h in run.hosts
            if isinstance(h.get("disk_avail_bytes"), int) and h.get("disk_size_bytes")]
    if not wal_a or not wal_b or not free:
        return {"status": "not_measured", "details": "no WAL or disk samples"}
    grew = {}
    for p in set(wal_a) & set(wal_b):
        if wal_b[p] > 1.5 * wal_a[p] + (64 << 20):
            grew[p] = {"wal": (wal_a[p], wal_b[p])}
        a_cp, b_cp = cp_a.get(p, 0), cp_b.get(p, 0)
        if data_a.get(p) and data_b.get(p):
            ratio_a, ratio_b = a_cp / data_a[p], b_cp / data_b[p]
            if ratio_b > 1.5 * ratio_a + 0.1:
                grew.setdefault(p, {})["checkpoint_per_data"] = (round(ratio_a, 3), round(ratio_b, 3))
        elif b_cp > 1.5 * a_cp + (64 << 20):
            grew.setdefault(p, {})["checkpoint"] = (a_cp, b_cp)
    ok = not grew and min(free) >= 0.2
    return {"status": "pass" if ok else "fail", "value": {"grew": grew, "min_free_fraction": round(min(free), 3)},
            "details": {"wal_a": dict(wal_a), "wal_b": dict(wal_b), "checkpoint_a": dict(cp_a),
                        "checkpoint_b": dict(cp_b), "data_a": dict(data_a), "data_b": dict(data_b)}}


def _hourly(run: Run) -> list[dict]:
    """Per hour of load: checked-load p50/p99/p99.9, operations, non-ok outcomes, faults, and the
    data nodes' mean anonymous memory."""
    if not run.load[0]:
        return []
    out = []
    start, stop = run.load[0], run.load[1] or max((e["t"] for e in run.latency), default=run.load[0])
    hour = 0
    while start + hour * 3600 < stop:
        window = (start + hour * 3600, min(stop, start + (hour + 1) * 3600))
        histogram = merged(run.latency, window)
        counts = defaultdict(int)
        for e in run.latency + run.background:
            if Run.inside(e["t"], window):
                for k, v in e["counts"].items():
                    counts["ok" if k == "ok" else "not_ok"] += v
        anon = [m["rss_anon_kib"] for m in run.metrics if Run.inside(m["t"], window)
                and not m["process"].endswith("coordinator") and isinstance(m.get("rss_anon_kib"), int)]
        out.append({"hour": hour, **lat.summary(histogram), "ok": counts["ok"], "not_ok": counts["not_ok"],
                    "faults": sum(1 for w in run.windows if Run.inside(w[0], window)),
                    "data_anon_mean_kib": round(statistics.mean(anon)) if anon else None})
        hour += 1
    return out


def markdown(report: dict) -> str:
    lines = [f"# Soak report {report['run_id']}", "", f"Verdict: **{report['verdict']}**", "",
             "| # | Criterion | Status | Value | Threshold |", "| --- | --- | --- | --- | --- |"]
    for r in report["criteria"]:
        value = json.dumps(r["value"], separators=(",", ":")) if r["value"] is not None else ""
        lines.append(f"| {r['id']} | {r['name']} | {r['status']} | `{value[:300]}` | {r['threshold'] if r['threshold'] is not None else ''} |")
    lines += ["", "## Per hour", "", "| Hour | p50 ms | p99 ms | p99.9 ms | max ms | ok | not ok | faults | data anon KiB |",
              "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for h in report["hourly"]:
        lines.append(f"| {h['hour']} | {h['p50_ms']} | {h['p99_ms']} | {h['p999_ms']} | {h['max_ms']} | {h['ok']} | {h['not_ok']} | {h['faults']} | {h['data_anon_mean_kib']} |")
    lines += ["", "## Details", "", "```json", json.dumps([{k: r[k] for k in ("id", "details")} for r in report["criteria"]], indent=1)[:60000], "```", ""]
    return "\n".join(lines)


def write(directory: Path, settle_s: float = 3600) -> dict:
    report = evaluate(Run(directory, settle_s=settle_s))
    (Path(directory) / "report.json").write_text(json.dumps(report, indent=2))
    (Path(directory) / "report.md").write_text(markdown(report))
    return report
