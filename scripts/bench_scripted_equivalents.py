#!/usr/bin/env python3
"""One Ruvio command against the Lua script that does the same on Redis.

Covers TRANSFER, HTRANSFER, SET IFGT, ZPOPBYSCORE, ZMOVEDUE, TTL on create and RENEW.
Each row runs `redis-benchmark` on both servers (alternating which goes first, median of
the rounds): throughput with pipelining, then p50/p99 latency with one request in flight
per connection. A second table compares resident memory per key.

    cargo build --profile release-fast --bin ruvio
    python3 scripts/bench_scripted_equivalents.py

Redis runs the script through EVALSHA (the common case). Numbers from a shared host
compare the two servers with each other only. Writes results.json and table.md.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis_compare as rc
import redis_compare_features as rf

ROOT = Path(__file__).resolve().parent.parent
BIG = "1000000000000"

LUA = {
    "transfer": "local s=tonumber(redis.call('GET',KEYS[1])) local n=tonumber(ARGV[1]) "
                "if not s or s<n then return false end "
                "return {redis.call('DECRBY',KEYS[1],n),redis.call('INCRBY',KEYS[2],n)}",
    "htransfer": "local s=tonumber(redis.call('HGET',KEYS[1],ARGV[1])) local n=tonumber(ARGV[3]) "
                 "if not s or s<n then return false end "
                 "return {redis.call('HINCRBY',KEYS[1],ARGV[1],-n),redis.call('HINCRBY',KEYS[1],ARGV[2],n)}",
    "ifgt": "local c=redis.call('GET',KEYS[1]) if not c or tonumber(ARGV[1])>tonumber(c) then "
            "return redis.call('SET',KEYS[1],ARGV[1]) end return false",
    "pop": "local r=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,ARGV[2]) "
           "if #r>0 then redis.call('ZREM',KEYS[1],unpack(r)) end return r",
    "move": "local r=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,ARGV[2]) "
            "if #r>0 then redis.call('ZREM',KEYS[1],unpack(r)) redis.call('RPUSH',KEYS[2],unpack(r)) end "
            "return #r",
    "incr_ex": "local v=redis.call('INCR',KEYS[1]) if v==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end return v",
    "lease_renew": "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end return 0",
    "limit_fixed": "local c=redis.call('INCR',KEYS[1]) if c==1 then redis.call('PEXPIRE',KEYS[1],ARGV[2]) end "
                   "if c>tonumber(ARGV[1]) then return false end return tonumber(ARGV[1])-c",
    "limit_sliding": "local t=redis.call('TIME') local ms=t[1]*1000+math.floor(t[2]/1000) local w=tonumber(ARGV[2]) "
                     "local cur=math.floor(ms/w) local ck=KEYS[1]..':'..cur local pk=KEYS[1]..':'..(cur-1) "
                     "local c=tonumber(redis.call('GET',ck) or '0') local p=tonumber(redis.call('GET',pk) or '0') "
                     "local est=p*(1-(ms%w)/w)+c if est+1>tonumber(ARGV[1]) then return false end "
                     "redis.call('INCR',ck) redis.call('PEXPIRE',ck,w*2) return math.floor(tonumber(ARGV[1])-est-1)",
    "limit_tokens": "local t=redis.call('TIME') local ms=t[1]*1000+math.floor(t[2]/1000) local cap=tonumber(ARGV[1]) "
                    "local refill=tonumber(ARGV[2]) local h=redis.call('HMGET',KEYS[1],'t','ts') "
                    "local tok=tonumber(h[1]) local ts=tonumber(h[2]) "
                    "if not tok then tok=cap ts=ms else local add=math.floor((ms-ts)/refill) "
                    "if add>0 then tok=math.min(cap,tok+add) ts=ts+add*refill end end "
                    "if tok<1 then return false end tok=tok-1 "
                    "redis.call('HSET',KEYS[1],'t',tok,'ts',ts) redis.call('PEXPIRE',KEYS[1],cap*refill) return tok",
    "sem_renew": "if redis.call('ZSCORE',KEYS[1],ARGV[1]) then redis.call('ZADD',KEYS[1],'XX',ARGV[2],ARGV[1]) return 1 end return 0",
}
SHA = {name: hashlib.sha1(body.encode()).hexdigest() for name, body in LUA.items()}


def zset_frames(key: str, count: int, score: int = 1) -> list[bytes]:
    """`count` members of score `score` plus their index, 500 per ZADD."""
    frames = []

    for first in range(0, count, 500):
        parts = []

        for i in range(first, min(first + 500, count)):
            parts += [str(score + i % 1000), f"m{i}"]

        frames.append(rf.frame("ZADD", key, *parts))

    return frames


def token_of(port: int, *argv: str) -> str:
    reply = rf.one(port, *argv).split(b"\r\n")
    return reply[1].decode() if reply[0].startswith(b"$") else reply[0][1:].decode()


# Each row: label, key space for __rand_int__, requests, setup(server name, port) -> argv builder inputs.
# setup returns the argv for that server; it may load data first.
def rows(args: argparse.Namespace):
    n = args.pop_members

    def transfer(name, port):
        rf.send_all(port, [rf.frame("SET", "{t}a", BIG), rf.frame("SET", "{t}b", "0")])
        return ["TRANSFER", "{t}a", "{t}b", "1"] if name == "ruvio" else ["EVALSHA", SHA["transfer"], "2", "{t}a", "{t}b", "1"]

    def transfer_low(name, port):
        rf.send_all(port, [rf.frame("SET", "{t}c", "0"), rf.frame("SET", "{t}d", "0")])
        return ["TRANSFER", "{t}c", "{t}d", "1"] if name == "ruvio" else ["EVALSHA", SHA["transfer"], "2", "{t}c", "{t}d", "1"]

    def htransfer(name, port):
        rf.send_all(port, [rf.frame("HSET", "{h}", "from", BIG, "to", "0")])
        return ["HTRANSFER", "{h}", "from", "to", "1"] if name == "ruvio" else ["EVALSHA", SHA["htransfer"], "1", "{h}", "from", "to", "1"]

    def ifgt(name, port):
        return ["SET", "w:__rand_int__", "1__rand_int__", "IFGT"] if name == "ruvio" else ["EVALSHA", SHA["ifgt"], "1", "w:__rand_int__", "1__rand_int__"]

    def pop(count, due="1000000000"):
        def setup(name, port):
            rf.send_all(port, zset_frames("z", n))
            return ["ZPOPBYSCORE", "z", due, "COUNT", str(count)] if name == "ruvio" else ["EVALSHA", SHA["pop"], "1", "z", due, str(count)]
        return setup

    def move(count):
        def setup(name, port):
            rf.send_all(port, zset_frames("z", n))
            return ["ZMOVEDUE", "z", "q", "1000000000", "COUNT", str(count)] if name == "ruvio" else ["EVALSHA", SHA["move"], "2", "z", "q", "1000000000", str(count)]
        return setup

    def incr_ex(name, port):
        return ["INCR", "c:__rand_int__", "EX", "60"] if name == "ruvio" else ["EVALSHA", SHA["incr_ex"], "1", "c:__rand_int__", "60"]

    def incr_plain(name, port):
        return ["INCR", "p:__rand_int__"]

    def lease_renew(name, port):
        if name == "ruvio":
            token = token_of(port, "LEASE", "lk", "600000")
            return ["RENEW", "lk", token, "600000"]

        rf.send_all(port, [rf.frame("SET", "lk", "tok", "PX", "600000")])
        return ["EVALSHA", SHA["lease_renew"], "1", "lk", "tok", "600000"]

    def sem_renew(name, port):
        if name == "ruvio":
            token = token_of(port, "SEMAPHORE", "sk", "10", "600000")
            return ["RENEW", "sk", token, "600000"]

        rf.send_all(port, [rf.frame("ZADD", "sk", "9999999999999", "tok")])
        return ["EVALSHA", SHA["sem_renew"], "1", "sk", "tok", "9999999999999"]

    def limit(mode, redis_script, ruvio_args, redis_args):
        def setup(name, port):
            if name == "ruvio":
                return ["LIMIT", "lim:__rand_int__", *ruvio_args]
            return ["EVALSHA", SHA[redis_script], "1", "lim:__rand_int__", *redis_args]
        return setup

    return [
        ("TRANSFER (enough)", 1000, 300_000, transfer),
        ("TRANSFER (source too low)", 1000, 300_000, transfer_low),
        ("HTRANSFER (enough)", 1000, 300_000, htransfer),
        ("SET IFGT (random keys, half hit)", 1000, 300_000, ifgt),
        (f"ZPOPBYSCORE COUNT 1 ({n} due)", 1000, n // 2, pop(1)),
        (f"ZPOPBYSCORE COUNT 10 ({n} due)", 1000, n // 20, pop(10)),
        ("ZPOPBYSCORE nothing due", 1000, 300_000, pop(10, due="0")),
        (f"ZMOVEDUE COUNT 1 ({n} due)", 1000, n // 2, move(1)),
        (f"ZMOVEDUE COUNT 10 ({n} due)", 1000, n // 20, move(10)),
        ("INCR (plain, for scale)", 1000, 300_000, incr_plain),
        ("INCR EX 60 (1000 keys, mostly existing)", 1000, 300_000, incr_ex),
        ("INCR EX 60 (new keys)", 10_000_000, 300_000, incr_ex),
        ("LIMIT plain (fixed window, allowed)", 1000, 300_000, limit("plain", "limit_fixed", ["1000000000", "60000"], ["1000000000", "60000"])),
        ("LIMIT SLIDING (allowed)", 1000, 300_000, limit("sliding", "limit_sliding", ["1000000000", "60000", "SLIDING"], ["1000000000", "60000"])),
        ("LIMIT SLIDING (mostly refused, max 5)", 1000, 300_000, limit("sliding", "limit_sliding", ["5", "60000", "SLIDING"], ["5", "60000"])),
        ("LIMIT TOKENS (allowed)", 1000, 300_000, limit("tokens", "limit_tokens", ["1000000", "60000", "TOKENS"], ["1000000", "60000"])),
        ("LIMIT TOKENS (mostly refused, capacity 5)", 1000, 300_000, limit("tokens", "limit_tokens", ["5", "60000", "TOKENS"], ["5", "60000"])),
        ("RENEW lease", 1000, 300_000, lease_renew),
        ("RENEW semaphore slot", 1000, 300_000, sem_renew),
    ]


def bench(args, port: int, argv: list[str], requests: int, rand: int, clients: int, pipeline: int) -> dict:
    cmd = [
        args.redis_benchmark, "-h", "127.0.0.1", "-p", str(port), "-n", str(requests),
        "-c", str(clients), "-P", str(pipeline), "-r", str(rand), "--csv", *argv,
    ]
    output = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.replace("\r", "\n")
    header = None

    for line in output.splitlines():
        fields = [f.strip('"') for f in line.split(",")]

        if fields[0] == "test":
            header = fields
        elif header and len(fields) >= len(header):
            row = dict(zip(header, fields))
            return {
                "rps": float(row["rps"]),
                "p50": float(row.get("p50_latency_ms", "nan")),
                "p99": float(row.get("p99_latency_ms", "nan")),
            }

    raise RuntimeError(f"no result for {argv[:3]} on {port}: {output[:200]!r}")


def command_works(port: int, argv: list[str]) -> str | None:
    """Runs the command once; returns the error text when the server rejects it."""
    reply = rf.one(port, *[a.replace("__rand_int__", "1") for a in argv])
    return reply.decode(errors="replace").strip() if reply.startswith(b"-") else None


MEMORY_CASES = [
    ("100,000 counters INCR ... EX (vs INCR + EXPIRE)", "incr_ex"),
    ("100,000 leases (vs SET ... PX)", "lease"),
    ("100,000 sliding-window limiters (vs two-counter Lua)", "sliding"),
    ("100,000 token buckets (vs hash + PEXPIRE Lua)", "tokens"),
    ("50,000 one-holder semaphores (vs one-member ZSET)", "sem"),
]


def memory_frames(kind: str, name: str) -> list[bytes]:
    if kind == "incr_ex":
        if name == "ruvio":
            return [rf.frame("INCR", f"c:{i:08d}", "EX", "100000") for i in range(100_000)]
        return [f for i in range(100_000) for f in (rf.frame("INCR", f"c:{i:08d}"), rf.frame("EXPIRE", f"c:{i:08d}", "100000"))]

    if kind == "lease":
        if name == "ruvio":
            return [rf.frame("LEASE", f"l:{i:08d}", "600000") for i in range(100_000)]
        return [rf.frame("SET", f"l:{i:08d}", "1234567890123456", "PX", "600000") for i in range(100_000)]

    if kind in ("sliding", "tokens"):
        if name == "ruvio":
            mode = "SLIDING" if kind == "sliding" else "TOKENS"
            return [rf.frame("LIMIT", f"q:{i:08d}", "100", "60000", mode) for i in range(100_000)]

        body = LUA["limit_sliding" if kind == "sliding" else "limit_tokens"]
        return [rf.frame("EVAL", body, "1", f"q:{i:08d}", "100", "60000") for i in range(100_000)]

    if name == "ruvio":
        return [rf.frame("SEMAPHORE", f"s:{i:08d}", "10", "600000") for i in range(50_000)]
    return [rf.frame("ZADD", f"s:{i:08d}", "9999999999999", "1234567890123456") for i in range(50_000)]


def measure_memory(args, name: str, workdir: Path, kind: str) -> dict:
    server = rc.start(args, name, workdir)

    try:
        time.sleep(0.5)
        idle = server.rss_kib()
        rf.send_all(server.port, memory_frames(kind, name))
        time.sleep(0.5)
        loaded = server.rss_kib()
    finally:
        server.stop()

    return {"idle_kib": idle, "loaded_kib": loaded, "delta_kib": loaded - idle}


def rate(value: float) -> str:
    return f"{value / 1_000_000:.2f}M" if value >= 1_000_000 else f"{value / 1000:.0f}k"


def render(results: dict) -> str:
    lines = [
        "| Command (Ruvio) vs Lua (Redis) | Ruvio ops/s | Redis ops/s | Ruvio / Redis | p50 ms (R / Redis) | p99 ms (R / Redis) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in results["rows"]:
        if row.get("skipped"):
            lines.append(f"| {row['label']} | skipped: {row['skipped']} | | | | |")
            continue

        ratio = row["ruvio"] / row["redis"]
        lines.append(
            f"| {row['label']} | {rate(row['ruvio'])} | {rate(row['redis'])} | {ratio:.2f}x | "
            f"{row['ruvio_p50']:.2f} / {row['redis_p50']:.2f} | {row['ruvio_p99']:.2f} / {row['redis_p99']:.2f} |"
        )

    lines += ["", "| Structure | Ruvio delta | Redis delta | Ruvio / Redis | Bytes per key (Ruvio / Redis) |", "| --- | ---: | ---: | ---: | ---: |"]

    for row in results["memory"]:
        ruvio, redis = row["ruvio"]["delta_kib"], row["redis"]["delta_kib"]
        keys = row["keys"]
        lines.append(
            f"| {row['label']} | {ruvio / 1024:.1f} MiB | {redis / 1024:.1f} MiB | {ruvio / redis:.2f}x | "
            f"{ruvio * 1024 / keys:.0f} / {redis * 1024 / keys:.0f} |"
        )

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release-fast/ruvio")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--ruvio-port", type=int, default=6431)
    parser.add_argument("--redis-port", type=int, default=6432)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--pop-members", type=int, default=100_000, help="due members loaded before each ZPOPBYSCORE/ZMOVEDUE round")
    parser.add_argument("--only", nargs="*", help="substrings of row labels to run")
    parser.add_argument("--no-memory", action="store_true")
    parser.add_argument("--row-pause", type=float, default=3.0 if sys.platform == "darwin" else 0.0)
    parser.add_argument("--out", type=Path, default=ROOT / "target/bench-scripted")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="bench-scripted-"))
    started = time.monotonic()
    servers = {name: rc.start(args, name, workdir) for name in ("ruvio", "redis")}
    out_rows = []

    try:
        for body in LUA.values():
            rf.one(servers["redis"].port, "SCRIPT", "LOAD", body)

        for label, rand, requests, setup in rows(args):
            if args.only and not any(w.lower() in label.lower() for w in args.only):
                continue

            samples = {"ruvio": [], "redis": []}
            lat = {"ruvio": [], "redis": []}
            skipped = None

            for round_index in range(args.rounds + 1):  # the last round is the P=1 latency pass
                order = ["ruvio", "redis"] if round_index % 2 == 0 else ["redis", "ruvio"]

                for name in order:
                    server = servers[name]
                    rf.one(server.port, "FLUSHALL")
                    argv = setup(name, server.port)

                    if round_index == 0 and name == "ruvio":
                        error = command_works(server.port, argv)
                        if error:
                            skipped = error[:60]
                            break

                        rf.one(server.port, "FLUSHALL")
                        argv = setup(name, server.port)

                    if round_index < args.rounds:
                        samples[name].append(bench(args, server.port, argv, requests, rand, args.clients, args.pipeline)["rps"])
                    else:
                        latency = bench(args, server.port, argv, min(requests, 60_000), rand, args.clients, 1)
                        lat[name].append(latency)

                if skipped:
                    break

            if skipped:
                out_rows.append({"label": label, "skipped": skipped})
                print(f"{label:<44} skipped: {skipped}", flush=True)
                continue

            row = {
                "label": label,
                "ruvio": statistics.median(samples["ruvio"]), "redis": statistics.median(samples["redis"]),
                "samples": samples,
                "ruvio_p50": lat["ruvio"][0]["p50"], "ruvio_p99": lat["ruvio"][0]["p99"],
                "redis_p50": lat["redis"][0]["p50"], "redis_p99": lat["redis"][0]["p99"],
                "ruvio_p1_rps": lat["ruvio"][0]["rps"], "redis_p1_rps": lat["redis"][0]["rps"],
            }
            out_rows.append(row)
            print(
                f"{label:<44} ruvio {row['ruvio']:>11,.0f}  redis {row['redis']:>11,.0f}  x{row['ruvio'] / row['redis']:.2f}"
                f"  p50 {row['ruvio_p50']:.2f}/{row['redis_p50']:.2f}  p99 {row['ruvio_p99']:.2f}/{row['redis_p99']:.2f}",
                flush=True,
            )
            time.sleep(args.row_pause)
    finally:
        for server in servers.values():
            server.stop()

    memory = []

    if not args.no_memory:
        for label, kind in MEMORY_CASES:
            result = {name: measure_memory(args, name, workdir, kind) for name in ("ruvio", "redis")}
            keys = 50_000 if kind == "sem" else 100_000
            memory.append({"label": label, "keys": keys, **result})
            print(f"{label:<52} ruvio {result['ruvio']['delta_kib'] / 1024:>7.1f} MiB  redis {result['redis']['delta_kib'] / 1024:>7.1f} MiB", flush=True)

    shutil.rmtree(workdir, ignore_errors=True)
    results = {
        "date": time.strftime("%Y-%m-%d"), "host": rc.host_info(), "build": rc.git_build(),
        "redis_version": rc.version([args.redis_server, "--version"]),
        "config": {"rounds": args.rounds, "clients": args.clients, "pipeline": args.pipeline},
        "rows": out_rows, "memory": memory, "seconds": round(time.monotonic() - started, 1),
    }
    markdown = render(results)
    (args.out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.out / "table.md").write_text(markdown)
    print()
    print(markdown)
    print(f"{results['seconds']} s, written to {args.out}")


if __name__ == "__main__":
    main()
