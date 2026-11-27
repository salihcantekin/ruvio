"""Checks a node's data disk before a run, so a bad VM or disk is replaced instead of judged:
8 KiB writes, each followed by `fdatasync`, the pattern of the write-ahead log, for a few
minutes. Reports the rate and the latency; with a target rate it passes only when the disk
reaches 80 % of it and no single sync took longer than a second.

This file is self-contained (standard library only): the ssh mode sends its source to each node
over stdin, so nothing has to be installed there.

    python3 tests/soak/run.py preflight --dir /var/lib/ruvio --seconds 120 --target-iops 800
    python3 tests/soak/run.py preflight --hosts 10.42.1.11,10.42.1.12,10.42.1.13 --target-iops 800"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

BLOCK = 8192
RING_BYTES = 64 << 20          # the file wraps here: the check never fills the disk
MIN_SHARE_OF_TARGET = 0.8
MAX_SYNC_S = 1.0               # one sync longer than this is a stall


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    return sorted_values[min(len(sorted_values) - 1, int(fraction * len(sorted_values)))]


def measure(directory: str, seconds: float, clock=time.perf_counter, sync=getattr(os, "fdatasync", os.fsync)) -> dict:
    """Writes and syncs blocks in a file under `directory` for `seconds`, then removes it."""
    path = os.path.join(directory, f".ruvio-preflight-{os.getpid()}")
    block = os.urandom(BLOCK)
    latencies: list[float] = []
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        started = clock()
        offset = 0
        while clock() - started < seconds:
            begin = clock()
            os.pwrite(descriptor, block, offset)
            sync(descriptor)
            latencies.append(clock() - begin)
            offset = (offset + BLOCK) % RING_BYTES
        elapsed = clock() - started
    finally:
        os.close(descriptor)
        try:
            os.unlink(path)
        except OSError:
            pass
    ordered = sorted(latencies)
    return {"syncs": len(latencies), "seconds": round(elapsed, 2),
            "iops": round(len(latencies) / elapsed, 1) if elapsed > 0 else 0.0,
            "p50_ms": round(percentile(ordered, 0.5) * 1000, 3), "p99_ms": round(percentile(ordered, 0.99) * 1000, 3),
            "max_ms": round((ordered[-1] if ordered else 0.0) * 1000, 3),
            "stalls_over_1s": sum(1 for v in latencies if v > MAX_SYNC_S)}


def judge(result: dict, target_iops: float | None) -> dict:
    """Adds `ok` and `problems`. Without a target only a stall or an empty run fails."""
    problems = []
    if not result["syncs"]:
        problems.append("no sync completed")
    if result["stalls_over_1s"]:
        problems.append(f"{result['stalls_over_1s']} sync(s) took over {MAX_SYNC_S:.0f} s (longest {result['max_ms']} ms)")
    if target_iops and result["iops"] < MIN_SHARE_OF_TARGET * target_iops:
        problems.append(f"{result['iops']} syncs/s is under {MIN_SHARE_OF_TARGET:.0%} of the target {target_iops:g}")
    return {**result, "target_iops": target_iops, "ok": not problems, "problems": problems}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=".", help="a directory on the disk to check (the data disk)")
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--target-iops", type=float)
    options = parser.parse_args(argv)
    print(json.dumps(judge(measure(options.dir, options.seconds), options.target_iops)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
