"""Latency and throughput per minute: each operation's time from send to reply lands in a
log-scale histogram keyed by (series, minute); a finished minute goes to a JSON-lines file as
counts, percentiles and the sparse histogram itself, so hours can be merged exactly later.

Buckets: 16 per power of two from 1 µs (about 4.4 % wide), up to about 70 s. Python's GIL makes
the lock cheap next to the network call it measures."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

SUB = 16  # buckets per power of two
MAX_BUCKET = 26 * SUB  # 2**26 µs is about 67 s


def bucket(micros: float) -> int:
    if micros < 1:
        return 0
    exponent = int(micros).bit_length() - 1
    base = 1 << exponent
    return min(MAX_BUCKET, exponent * SUB + int((micros - base) * SUB / base))


def bucket_upper(index: int) -> float:
    """The upper edge of a bucket in µs (what a percentile reports: never optimistic)."""
    exponent, sub = divmod(index, SUB)
    base = 1 << exponent
    return base + base * (sub + 1) / SUB


def percentile(histogram: dict[int, int], fraction: float) -> float | None:
    total = sum(histogram.values())
    if not total:
        return None
    rank = fraction * total
    seen = 0
    for index in sorted(histogram):
        seen += histogram[index]
        if seen >= rank:
            return round(bucket_upper(index) / 1000, 3)
    return round(bucket_upper(max(histogram)) / 1000, 3)


def merge(target: dict[int, int], source: dict) -> dict[int, int]:
    for index, count in source.items():
        target[int(index)] = target.get(int(index), 0) + count
    return target


def summary(histogram: dict[int, int]) -> dict:
    """Percentiles in milliseconds."""
    return {
        "p50_ms": percentile(histogram, 0.50),
        "p90_ms": percentile(histogram, 0.90),
        "p99_ms": percentile(histogram, 0.99),
        "p999_ms": percentile(histogram, 0.999),
        "max_ms": round(bucket_upper(max(histogram)) / 1000, 3) if histogram else None,
    }


class Recorder:
    """`add(series, outcome, seconds)` from any thread; `flush()` writes every finished minute
    (all minutes with `final=True`). Outcomes are free-form names (`ok`, `fail`, `info`, an
    error class) counted per minute; only `ok` operations go into the histogram."""

    def __init__(self, path: Path, source: str = ""):
        self.path, self.source = path, source
        self.lock = threading.Lock()
        self.minutes: dict[tuple[str, int], dict] = {}
        self.file = open(path, "a", buffering=1)
        self.last: dict[str, dict] = {}  # the newest written minute per series, for status.json

    def add(self, series: str, outcome: str, seconds: float) -> None:
        minute = int(time.time() // 60)
        with self.lock:
            slot = self.minutes.get((series, minute))
            if slot is None:
                slot = self.minutes[(series, minute)] = {"counts": {}, "hist": {}}
            slot["counts"][outcome] = slot["counts"].get(outcome, 0) + 1
            if outcome == "ok":
                index = bucket(seconds * 1e6)
                slot["hist"][index] = slot["hist"].get(index, 0) + 1

    def flush(self, final: bool = False) -> None:
        current = int(time.time() // 60)
        with self.lock:
            done = sorted(key for key in self.minutes if final or key[1] < current)
            slots = [(key, self.minutes.pop(key)) for key in done]
        for (series, minute), slot in slots:
            entry = {"t": minute * 60, "series": series, "source": self.source, "counts": slot["counts"],
                     "n": sum(slot["counts"].values()), **summary(slot["hist"]), "hist": slot["hist"]}
            self.file.write(json.dumps(entry, separators=(",", ":")) + "\n")
            self.last[series] = {k: entry[k] for k in ("t", "n", "p50_ms", "p99_ms", "p999_ms", "max_ms")}
        self.file.flush()

    def close(self) -> None:
        self.flush(final=True)
        self.file.close()


class Flusher(threading.Thread):
    """Flushes a recorder every few seconds until `stop` is set, then once more with every
    minute."""

    def __init__(self, recorder: Recorder, stop: threading.Event, every: float = 5.0):
        super().__init__(name="latency", daemon=True)
        self.recorder, self.stop, self.every = recorder, stop, every

    def run(self) -> None:
        while not self.stop.wait(self.every):
            self.recorder.flush()
        self.recorder.flush(final=True)
