#!/usr/bin/env python3
"""Ruvio vs Redis for the commands added after the first comparison.

Reuses the server launcher and RSS reader of scripts/redis_compare.py. Each row
runs one `redis-benchmark` invocation with a custom command against both servers
(rounds alternate which one goes first, median reported). A second table loads
the same small structures into both and compares resident memory.

    cargo build --profile release-fast --bin ruvio
    python3 scripts/redis_compare_features.py --ruvio target/release-fast/ruvio

Sizes are small on purpose (about two minutes in total). Numbers from a shared
host are for comparing the two servers with each other only.
Writes results.json and table.md into --out.
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis_compare as rc

ROOT = Path(__file__).resolve().parent.parent
LIST_100 = [f"e{i}" for i in range(100)]

# (label, benchmark argv, requests, note). `__rand_int__` is filled by redis-benchmark (-r).
# `setup` is shared; commands run on both servers before the first round.
SETUP = [
    ("RPUSH", "lst", *LIST_100),
    ("SET", "k", "x" * 64),
    ("SET", "bm", "0"),
    ("SETBIT", "bm", "7999", "1"),
    ("SET", "bf", "0"),
    ("BITFIELD", "bf", "SET", "u8", "1000", "1"),
    ("SET", "ctr", "0"),
    ("SET", "ttlk", "x", "EX", "100000"),
    ("SET", "fl", "1.5"),
    ("HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"),
    ("SADD", "s", "a", "b", "c", "d"),
    ("ZADD", "z", "1", "a", "2", "b", "3", "c"),
    ("RPUSH", "lst2", "a", "b", "c"),
    ("HSET", "hm", *[x for i in range(10) for x in (f"field{i}", f"value-{i}")]),
    ("RPUSH", "lm", *[f"element-{i}" for i in range(20)]),
]

FEATURES = [
    ("GET (baseline, for scale)", ["GET", "k"], 300_000, "not a new command"),
    ("SET (baseline, for scale)", ["SET", "sk", "x"], 300_000, "not a new command"),
    ("LPOS (100 elements, hit in the middle)", ["LPOS", "lst", "e50"], 300_000, ""),
    ("LSET (100 elements)", ["LSET", "lst", "50", "v"], 300_000, ""),
    ("LINSERT (pivot missing, scans 100)", ["LINSERT", "lst", "BEFORE", "nopivot", "x"], 300_000, ""),
    ("LREM (value missing, scans 100)", ["LREM", "lst", "1", "nothing"], 300_000, ""),
    ("LMOVE (rotate a 100-element list)", ["LMOVE", "lst", "lst", "LEFT", "RIGHT"], 300_000, ""),
    ("BITFIELD INCRBY u8 (always writes)", ["BITFIELD", "bf", "INCRBY", "u8", "800", "1"], 300_000, "Redis refuses zero-padded offsets, so the offset is fixed"),
    ("BITFIELD GET u8", ["BITFIELD", "bf", "GET", "u8", "800"], 300_000, ""),
    ("SETBIT (1 KiB bitmap, same bit)", ["SETBIT", "bm", "5000", "1"], 300_000, "no-op after the first write"),
    ("GETBIT (1 KiB bitmap)", ["GETBIT", "bm", "5000"], 300_000, ""),
    ("BITCOUNT (1 KiB bitmap)", ["BITCOUNT", "bm"], 300_000, ""),
    ("PFADD", ["PFADD", "hll", "__rand_int__"], 300_000, ""),
    ("PFCOUNT", ["PFCOUNT", "hll"], 300_000, ""),
    ("COPY ... REPLACE (64 B string)", ["COPY", "k", "k2", "REPLACE"], 300_000, ""),
    ("DUMP (64 B string)", ["DUMP", "k"], 300_000, "formats differ: Ruvio's is not RDB"),
    ("EXPIRETIME", ["EXPIRETIME", "k"], 300_000, ""),
    ("TOUCH", ["TOUCH", "k"], 300_000, ""),
    ("OBJECT ENCODING", ["OBJECT", "ENCODING", "k"], 300_000, ""),
    ("MEMORY USAGE", ["MEMORY", "USAGE", "k"], 300_000, ""),
    ("GETRANGE", ["GETRANGE", "k", "0", "31"], 300_000, ""),
    ("SETRANGE", ["SETRANGE", "k", "4", "xx"], 300_000, ""),
    ("SETEX", ["SETEX", "ex:__rand_int__", "100", "v"], 300_000, ""),
    ("MSET (3 keys)", ["MSET", "ma", "1", "mb", "2", "mc", "3"], 300_000, ""),
    ("MGET (3 keys)", ["MGET", "k", "ctr", "ma"], 300_000, ""),
    ("DEL (missing key)", ["DEL", "nokey"], 300_000, ""),
    ("EXISTS", ["EXISTS", "k"], 300_000, ""),
    ("TYPE", ["TYPE", "k"], 300_000, ""),
    ("TTL", ["TTL", "ttlk"], 300_000, ""),
    ("PTTL", ["PTTL", "ttlk"], 300_000, ""),
    ("PERSIST (key without TTL)", ["PERSIST", "k"], 300_000, ""),
    ("EXPIRE (key with TTL)", ["EXPIRE", "ttlk", "100000"], 300_000, ""),
    ("APPEND (1 byte)", ["APPEND", "ap", "x"], 300_000, "value grows"),
    ("INCRBYFLOAT", ["INCRBYFLOAT", "fl", "1.5"], 300_000, "Ruvio prints 16 digits"),
    ("HSET (existing field)", ["HSET", "h", "f1", "v1"], 300_000, ""),
    ("HGET", ["HGET", "h", "f1"], 300_000, ""),
    ("HMGET (2 fields)", ["HMGET", "h", "f1", "f2"], 300_000, ""),
    ("HSTRLEN", ["HSTRLEN", "h", "f1"], 300_000, ""),
    ("LPUSH", ["LPUSH", "lp", "x"], 300_000, "list grows"),
    ("LLEN", ["LLEN", "lst2"], 300_000, ""),
    ("LRANGE 0 2", ["LRANGE", "lst2", "0", "2"], 300_000, ""),
    ("SADD (existing member)", ["SADD", "s", "a"], 300_000, ""),
    ("SISMEMBER", ["SISMEMBER", "s", "a"], 300_000, ""),
    ("SRANDMEMBER", ["SRANDMEMBER", "s"], 300_000, ""),
    ("ZADD (existing member, same score)", ["ZADD", "z", "1", "a"], 300_000, ""),
    ("ZINCRBY", ["ZINCRBY", "z", "1", "a"], 300_000, ""),
    ("ZSCORE", ["ZSCORE", "z", "a"], 300_000, ""),
    ("RENAME (same key)", ["RENAME", "k", "k"], 300_000, ""),
    ("PUBLISH (no subscribers)", ["PUBLISH", "ch", "m"], 300_000, ""),
    ("HGET (10-field hash)", ["HGET", "hm", "field7"], 300_000, ""),
    ("HSET (10-field hash, existing field)", ["HSET", "hm", "field7", "value-7"], 300_000, ""),
    ("HDEL (10-field hash, missing field)", ["HDEL", "hm", "nofield"], 300_000, ""),
    ("HGETALL (10-field hash)", ["HGETALL", "hm"], 300_000, ""),
    ("LINDEX (20-item list)", ["LINDEX", "lm", "10"], 300_000, ""),
    ("LRANGE 0 9 (20-item list)", ["LRANGE", "lm", "0", "9"], 300_000, ""),
    ("LSET (20-item list)", ["LSET", "lm", "5", "element-5"], 300_000, ""),
    ("GEOADD", ["GEOADD", "geo", "13.361389", "38.115556", "m__rand_int__"], 200_000, ""),
    ("GEOSEARCH (radius 50 km, COUNT 10)", ["GEOSEARCH", "geo", "FROMLONLAT", "13.36", "38.11", "BYRADIUS", "50", "km", "COUNT", "10"], 100_000, "all points share one coordinate: worst case"),
]

# Ruvio only. The Redis column runs the closest single command instead and says so.
RUVIO_ONLY = [
    ("GETEX (set a 100 s deadline)", ["GETEX", "k", "100000"], ["GETEX", "k", "PX", "100000"], "Ruvio's GETEX takes milliseconds only; each side uses its own syntax"),
    ("FILLGET (value present)", ["FILLGET", "fillkey", "5000"], ["GET", "fillkey"], "Redis column: plain GET"),
]

MEMORY_CASES = [
    ("20,000 lists of 20 elements", "lists"),
    ("5,000 HyperLogLogs of 100 elements", "hll"),
    ("20,000 bitmaps of 128 B (SETBIT)", "bitmaps"),
    ("100,000 BITFIELD counters (u16)", "counters"),
    ("100,000 geo points in one key", "geo"),
    ("100,000 strings of 16 B", "str16"),
    ("100,000 strings of 128 B", "str128"),
    ("100,000 strings of 16 B with TTL", "str16ttl"),
    ("100,000 one-element lists", "list1"),
    ("50,000 small hashes (3 fields)", "hash3"),
    ("50,000 small sets (4 members)", "set4"),
    ("50,000 small sorted sets (3 members)", "zset3"),
]


def frame(*parts: str) -> bytes:
    data = [part.encode() for part in parts]
    return b"*%d\r\n" % len(data) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in data)


def send_all(port: int, frames: list[bytes], batch: int = 200) -> None:
    """Pipelines the frames in batches; a PING after each batch marks the end of its replies."""
    with socket.create_connection(("127.0.0.1", port)) as sock:
        for first in range(0, len(frames), batch):
            sock.sendall(b"".join(frames[first:first + batch]) + frame("PING"))
            received = bytearray()

            while not received.endswith(b"+PONG\r\n"):
                chunk = sock.recv(1 << 16)

                if not chunk:
                    raise ConnectionError("server closed during load")

                received.extend(chunk)

            if b"\r\n-" in b"\r\n" + bytes(received):
                raise RuntimeError(f"error reply during load: {bytes(received)[:200]!r}")


def one(port: int, *parts: str) -> bytes:
    return rc.command(port, *[p.encode() for p in parts])


def custom(args: argparse.Namespace, port: int, argv: list[str], requests: int) -> float:
    cmd = [
        args.redis_benchmark, "-h", "127.0.0.1", "-p", str(port),
        "-n", str(requests), "-c", str(args.clients), "-P", str(args.pipeline),
        "-r", "1000", "--csv", *argv,
    ]
    output = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout

    for line in output.splitlines():
        fields = [field.strip('"') for field in line.split(",")]

        if len(fields) >= 2 and fields[0] != "test":
            return float(fields[1])

    raise RuntimeError(f"no rate for {argv} on port {port}: {output!r}")


def memory_frames(kind: str) -> list[bytes]:
    if kind == "lists":
        return [frame("RPUSH", f"l:{i}", *[f"element-{j}" for j in range(20)]) for i in range(20_000)]

    if kind == "hll":
        return [frame("PFADD", f"h:{i}", *[f"m{j}" for j in range(100)]) for i in range(5_000)]

    if kind == "bitmaps":
        return [frame("SETBIT", f"b:{i}", "1023", "1") for i in range(20_000)]

    if kind == "counters":
        return [frame("BITFIELD", f"c:{i}", "SET", "u16", "0", str(i % 65536)) for i in range(100_000)]

    if kind == "str16":
        return [frame("SET", f"s:{i:08d}", "v" * 16) for i in range(100_000)]

    if kind == "str128":
        return [frame("SET", f"s:{i:08d}", "v" * 128) for i in range(100_000)]

    if kind == "str16ttl":
        return [frame("SET", f"s:{i:08d}", "v" * 16, "EX", "100000") for i in range(100_000)]

    if kind == "list1":
        return [frame("RPUSH", f"l:{i:08d}", "element-0") for i in range(100_000)]

    if kind == "hash3":
        return [frame("HSET", f"h:{i:08d}", "f1", "v1", "f2", "v2", "f3", "v3") for i in range(50_000)]

    if kind == "set4":
        return [frame("SADD", f"s:{i:08d}", "a", "b", "c", "d") for i in range(50_000)]

    if kind == "zset3":
        return [frame("ZADD", f"z:{i:08d}", "1", "a", "2", "b", "3", "c") for i in range(50_000)]

    return [
        frame("GEOADD", "g", f"{(i % 3600) / 10 - 180:.1f}", f"{(i % 1700) / 10 - 85:.1f}", f"p{i}")
        for i in range(100_000)
    ]


def measure_memory(args: argparse.Namespace, name: str, workdir: Path, kind: str) -> dict:
    server = rc.start(args, name, workdir)

    try:
        time.sleep(0.5)
        idle = server.rss_kib()
        send_all(server.port, memory_frames(kind))
        time.sleep(0.5)
        loaded = server.rss_kib()
    finally:
        server.stop()

    return {"idle_kib": idle, "loaded_kib": loaded, "delta_kib": loaded - idle}


def rate(value: float | None) -> str:
    return "n/a" if value is None else f"{value / 1_000_000:.2f}M/s" if value >= 1_000_000 else f"{value / 1000:.0f}k/s"


def render(results: dict) -> str:
    lines = ["| Command | Ruvio | Redis | Ruvio / Redis | Per-round ratio (min-max) | Note |", "| --- | ---: | ---: | ---: | ---: | --- |"]

    for row in results["rows"]:
        ratio = "" if row["redis"] is None else f"{row['ruvio'] / row['redis']:.2f}×"
        pairs = [a / b for a, b in zip(row["samples"]["ruvio"], row["samples"]["redis"]) if b == b and a == a and b]
        spread = f"{min(pairs):.2f}-{max(pairs):.2f}×" if pairs else ""
        lines.append(f"| {row['label']} | {rate(row['ruvio'])} | {rate(row['redis'])} | {ratio} | {spread} | {row['note']} |")

    lines += ["", "| Structure | Ruvio delta | Redis delta | Ruvio / Redis | Idle RSS Ruvio / Redis |", "| --- | ---: | ---: | ---: | ---: |"]

    for row in results["memory"]:
        ruvio, redis = row["ruvio"]["delta_kib"], row["redis"]["delta_kib"]
        ratio = f"{ruvio / redis:.2f}×" if redis > 0 else "n/a"
        idle = f"{row['ruvio']['idle_kib'] / 1024:.1f} / {row['redis']['idle_kib'] / 1024:.1f} MiB"
        lines.append(f"| {row['label']} | {ruvio / 1024:.1f} MiB | {redis / 1024:.1f} MiB | {ratio} | {idle} |")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release-fast/ruvio")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--ruvio-port", type=int, default=6421)
    parser.add_argument("--redis-port", type=int, default=6422)
    parser.add_argument("--rounds", type=int, default=5, help="alternating-order rounds; the table shows the median and the min-max of the per-round ratio")
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--data-size", type=int, default=64)
    parser.add_argument("--scale", type=float, default=1.0, help="multiply every request count (0.2 for a smoke run)")
    parser.add_argument("--no-memory", action="store_true")
    parser.add_argument(
        "--row-pause", type=float, default=3.0 if sys.platform == "darwin" else 0.0,
        help="seconds to wait after each row; macOS runs out of ephemeral ports without it",
    )
    parser.add_argument("--out", type=Path, default=ROOT / "target/redis-compare-features")
    args = parser.parse_args()

    if not args.ruvio.exists():
        raise SystemExit(f"{args.ruvio} not found; build it with --profile release-fast")

    args.out.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="redis-compare-features-"))
    started = time.monotonic()
    servers = {name: rc.start(args, name, workdir) for name in ("ruvio", "redis")}
    rows = []

    try:
        for server in servers.values():
            send_all(server.port, [frame(*parts) for parts in SETUP])
            send_all(server.port, [frame("PFADD", "hll", *[f"s{i}" for i in range(1000)])])
            send_all(server.port, [frame("GEOADD", "geo", "13.361389", "38.115556", "seed")])

        # FILLGET needs a stored value on Ruvio: take the token, then FILLSET.
        token = one(servers["ruvio"].port, "FILLGET", "fillkey", "5000").split(b"\r\n")[-2].decode()
        one(servers["ruvio"].port, "FILLSET", "fillkey", "v", "60000", token)
        one(servers["redis"].port, "SET", "fillkey", "v")

        plan = [(label, argv, argv, reqs, note) for label, argv, reqs, note in FEATURES]
        plan += [(label, ruvio_argv, redis_argv, 300_000, note) for label, ruvio_argv, redis_argv, note in RUVIO_ONLY]

        for label, ruvio_argv, redis_argv, requests, note in plan:
            requests = max(10_000, int(requests * args.scale))
            samples = {"ruvio": [], "redis": []}

            for round_index in range(args.rounds):
                order = ["ruvio", "redis"] if round_index % 2 == 0 else ["redis", "ruvio"]

                for name in order:
                    argv = ruvio_argv if name == "ruvio" else redis_argv

                    try:
                        samples[name].append(custom(args, servers[name].port, argv, requests))
                    except RuntimeError as error:
                        samples[name].append(float("nan"))
                        note = note or f"{name}: {str(error)[:60]}"

            ruvio = statistics.median(samples["ruvio"])
            redis = statistics.median(samples["redis"])
            rows.append({
                "label": label, "ruvio": ruvio, "redis": None if redis != redis else redis,
                "note": note, "samples": samples,
            })
            print(f"{label:<42} ruvio {ruvio:>12,.0f}  redis {redis:>12,.0f}", flush=True)
            time.sleep(args.row_pause)
    finally:
        for server in servers.values():
            server.stop()

    memory = []

    if not args.no_memory:
        for label, kind in MEMORY_CASES:
            result = {name: measure_memory(args, name, workdir, kind) for name in ("ruvio", "redis")}
            memory.append({"label": label, **result})
            print(f"{label:<42} ruvio {result['ruvio']['delta_kib'] / 1024:>7.1f} MiB  redis {result['redis']['delta_kib'] / 1024:>7.1f} MiB", flush=True)

    shutil.rmtree(workdir, ignore_errors=True)
    results = {
        "date": time.strftime("%Y-%m-%d"),
        "host": rc.host_info(),
        "build": rc.git_build(),
        "redis_version": rc.version([args.redis_server, "--version"]),
        "config": {"rounds": args.rounds, "clients": args.clients, "pipeline": args.pipeline, "scale": args.scale},
        "rows": rows,
        "memory": memory,
        "seconds": round(time.monotonic() - started, 1),
    }
    markdown = render(results)
    (args.out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.out / "table.md").write_text(markdown)
    print()
    print(markdown)
    print(f"{results['seconds']} s, written to {args.out}")


if __name__ == "__main__":
    main()
