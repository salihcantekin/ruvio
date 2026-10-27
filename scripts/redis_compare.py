#!/usr/bin/env python3
"""Native Ruvio vs Redis comparison on one host, the table published on the site.

Starts a release Ruvio and a Redis server on loopback, runs one
`redis-benchmark` invocation per command against each, and records RSS for
1,000,000 unique 64-byte strings. Rounds alternate which server goes first;
the table reports the median of the rounds.

    cargo build --release --bin ruvio
    python3 scripts/redis_compare.py --out target/redis-compare

Writes results.json, table.md, and table.html into --out. About two minutes on
an Apple silicon Mac with the defaults.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (redis-benchmark test, label, requests). LRANGE replies carry 100-600
# elements, so those tests use fewer requests to keep the run short.
TESTS = [
    ("ping_mbulk", "PING (bulk)", 1_000_000),
    ("set", "SET", 1_000_000),
    ("get", "GET", 1_000_000),
    ("incr", "INCR", 1_000_000),
    ("lpush", "LPUSH", 1_000_000),
    ("rpush", "RPUSH", 1_000_000),
    ("lpop", "LPOP", 1_000_000),
    ("rpop", "RPOP", 1_000_000),
    ("sadd", "SADD", 1_000_000),
    ("hset", "HSET", 1_000_000),
    ("spop", "SPOP", 1_000_000),
    ("zadd", "ZADD", 1_000_000),
    ("lrange_100", "LRANGE 100", 200_000),
    ("lrange_300", "LRANGE 300", 100_000),
    ("lrange_500", "LRANGE 500", 60_000),
    ("lrange_600", "LRANGE 600", 50_000),
    ("mset", "MSET", 500_000),
]


def wait_ping(port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as sock:
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")

                if sock.recv(16).startswith(b"+PONG"):
                    return
        except OSError:
            pass

        time.sleep(0.05)

    raise TimeoutError(f"no PONG on port {port}")


class Server:
    def __init__(self, name: str, argv: list[str], port: int, cwd: Path) -> None:
        self.name = name
        self.port = port
        self.proc = subprocess.Popen(
            argv,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        wait_ping(port)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()

            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def rss_kib(self) -> int:
        status = Path(f"/proc/{self.proc.pid}/status")

        if status.exists():
            for line in status.read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])

        output = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(self.proc.pid)],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(output.stdout.strip())


def start(args: argparse.Namespace, name: str, workdir: Path) -> Server:
    directory = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=workdir))

    if name == "ruvio":
        # Ruvio writes ruvio.toml into its working directory, so it runs in a scratch dir.
        argv = [
            str(args.ruvio),
            "--address",
            f"127.0.0.1:{args.ruvio_port}",
            "--durability",
            "memory",
            "--shards",
            "1",
            "--data-dir",
            str(directory / "data"),
        ]
        return Server(name, argv, args.ruvio_port, directory)

    argv = [
        args.redis_server,
        "--bind",
        "127.0.0.1",
        "--port",
        str(args.redis_port),
        "--save",
        "",
        "--appendonly",
        "no",
        "--dir",
        str(directory),
    ]
    return Server(name, argv, args.redis_port, directory)


def command(port: int, *parts: bytes) -> bytes:
    frame = b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n%s\r\n" % (len(part), part) for part in parts
    )

    with socket.create_connection(("127.0.0.1", port)) as sock:
        sock.sendall(frame)
        return sock.recv(4096)


def bench(args: argparse.Namespace, port: int, test: str, requests: int) -> float:
    argv = [
        args.redis_benchmark,
        "-h",
        "127.0.0.1",
        "-p",
        str(port),
        "-n",
        str(requests),
        "-c",
        str(args.clients),
        "-P",
        str(args.pipeline),
        "-d",
        str(args.data_size),
        "-t",
        test,
        "--csv",
    ]
    output = subprocess.run(argv, capture_output=True, text=True, check=True).stdout

    for line in output.splitlines():
        fields = [field.strip('"') for field in line.split(",")]

        if len(fields) < 2 or fields[0] == "test" or "needed" in fields[0]:
            continue

        return float(fields[1])

    raise RuntimeError(f"no rate for {test} on port {port}: {output!r}")


def load_strings(port: int, keys: int, value_size: int, pipeline: int) -> None:
    value = b"x" * value_size

    with socket.create_connection(("127.0.0.1", port)) as sock:
        for first in range(0, keys, pipeline):
            last = min(first + pipeline, keys)
            batch = b"".join(
                b"*3\r\n$3\r\nSET\r\n$%d\r\nk%d\r\n$%d\r\n%s\r\n"
                % (len(b"k%d" % index), index, value_size, value)
                for index in range(first, last)
            )
            sock.sendall(batch)
            expected = b"+OK\r\n" * (last - first)
            received = bytearray()

            while len(received) < len(expected):
                chunk = sock.recv(len(expected) - len(received))

                if not chunk:
                    raise ConnectionError("server closed during load")

                received.extend(chunk)

            if received != expected:
                raise RuntimeError(f"unexpected SET response during load at key {first}")

    count = command(port, b"DBSIZE")
    if count != f":{keys}\r\n".encode():
        raise RuntimeError(f"expected {keys} loaded keys, got {count!r}")


def memory(args: argparse.Namespace, name: str, workdir: Path) -> dict:
    server = start(args, name, workdir)

    try:
        time.sleep(1)
        idle = server.rss_kib()
        load_strings(server.port, args.memory_keys, args.data_size, 200)
        time.sleep(1)
        loaded = server.rss_kib()
    finally:
        server.stop()

    return {"idle_kib": idle, "loaded_kib": loaded, "delta_kib": loaded - idle}


def host_info() -> dict:
    info = {"os": f"{platform.system()} {platform.release()}", "arch": platform.machine()}

    if platform.system() == "Darwin":
        def sysctl(key: str) -> str:
            return subprocess.run(
                ["sysctl", "-n", key], capture_output=True, text=True
            ).stdout.strip()

        info["cpu"] = sysctl("machdep.cpu.brand_string")
        info["cores"] = int(sysctl("hw.ncpu"))
        info["memory_gib"] = round(int(sysctl("hw.memsize")) / 2**30)
        info["os"] = "macOS " + subprocess.run(
            ["sw_vers", "-productVersion"], capture_output=True, text=True
        ).stdout.strip()
    else:
        cpuinfo = Path("/proc/cpuinfo")

        if cpuinfo.exists():
            for line in cpuinfo.read_text().splitlines():
                if line.startswith("model name"):
                    info["cpu"] = line.split(":", 1)[1].strip()
                    break

        info["cores"] = os.cpu_count()

    return info


def version(argv: list[str]) -> str:
    return subprocess.run(argv, capture_output=True, text=True).stdout.strip()


def git_build() -> dict:
    def git(*parts: str) -> str:
        return subprocess.run(
            ["git", *parts], cwd=ROOT, capture_output=True, text=True
        ).stdout.strip()

    return {"commit": git("rev-parse", "--short", "HEAD"), "dirty": bool(git("status", "--porcelain", "--untracked-files=no"))}


def millions(rate: float) -> str:
    return f"{rate / 1_000_000:.2f}M req/s"


def render(results: dict) -> tuple[str, str]:
    md = [
        "| Command | Ruvio | Redis | Ruvio / Redis |",
        "| --- | ---: | ---: | ---: |",
    ]
    html = []

    for row in results["rows"]:
        ruvio, redis = row["ruvio"], row["redis"]
        ratio = f"{ruvio / redis:.2f}×"
        md.append(f"| {row['label']} | {millions(ruvio)} | {millions(redis)} | {ratio} |")
        html.append(
            f"          <tr><td>{row['label']}</td><td>{millions(ruvio)}</td>"
            f"<td>{millions(redis)}</td><td>{ratio}</td></tr>"
        )

    mem = results["memory"]
    ruvio_mem, redis_mem = mem["ruvio"], mem["redis"]
    md += [
        "",
        "| | Ruvio | Redis |",
        "| --- | ---: | ---: |",
        f"| Idle RSS | {ruvio_mem['idle_kib']:,} KiB | {redis_mem['idle_kib']:,} KiB |",
        f"| After {results['config']['memory_keys']:,} strings | {ruvio_mem['loaded_kib']:,} KiB | {redis_mem['loaded_kib']:,} KiB |",
        f"| Delta | {ruvio_mem['delta_kib'] / 1024:.1f} MiB | {redis_mem['delta_kib'] / 1024:.1f} MiB |",
        f"| Ruvio / Redis | {ruvio_mem['delta_kib'] / redis_mem['delta_kib']:.2f}× | |",
    ]
    return "\n".join(md) + "\n", "\n".join(html) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release/ruvio")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--ruvio-port", type=int, default=6411)
    parser.add_argument("--redis-port", type=int, default=6412)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--data-size", type=int, default=64)
    parser.add_argument("--memory-keys", type=int, default=1_000_000)
    parser.add_argument("--tests", help="comma-separated subset of redis-benchmark tests")
    parser.add_argument("--out", type=Path, default=ROOT / "target/redis-compare")
    args = parser.parse_args()

    if not args.ruvio.exists():
        raise SystemExit(f"{args.ruvio} not found; run cargo build --release --bin ruvio")

    tests = TESTS

    if args.tests:
        wanted = set(args.tests.split(","))
        tests = [test for test in TESTS if test[0] in wanted]

    args.out.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="redis-compare-"))
    started = time.monotonic()
    samples: dict[str, dict[str, list[float]]] = {
        test: {"ruvio": [], "redis": []} for test, _, _ in tests
    }
    list_lengths: dict[str, int] = {}

    servers = {name: start(args, name, workdir) for name in ("ruvio", "redis")}

    try:
        for round_index in range(args.rounds):
            order = ["ruvio", "redis"] if round_index % 2 == 0 else ["redis", "ruvio"]

            for test, label, requests in tests:
                for name in order:
                    rate = bench(args, servers[name].port, test, requests)
                    samples[test][name].append(rate)

                    if test.startswith("lrange") and round_index == 0:
                        reply = command(servers[name].port, b"LLEN", b"mylist")
                        list_lengths[f"{name}:{test}"] = int(reply[1:].split(b"\r\n")[0])

                print(
                    f"round {round_index + 1} {label:<12} "
                    f"ruvio {samples[test]['ruvio'][-1]:>12,.0f}  "
                    f"redis {samples[test]['redis'][-1]:>12,.0f}",
                    flush=True,
                )
    finally:
        for server in servers.values():
            server.stop()

    mem = {name: memory(args, name, workdir) for name in ("ruvio", "redis")}
    shutil.rmtree(workdir, ignore_errors=True)

    results = {
        "date": time.strftime("%Y-%m-%d"),
        "host": host_info(),
        "build": git_build(),
        "redis_version": version([args.redis_server, "--version"]),
        "redis_benchmark_version": version([args.redis_benchmark, "--version"]),
        "config": {
            "rounds": args.rounds,
            "clients": args.clients,
            "pipeline": args.pipeline,
            "data_size": args.data_size,
            "memory_keys": args.memory_keys,
            "requests": {test: requests for test, _, requests in tests},
        },
        "lrange_list_lengths": list_lengths,
        "rows": [
            {
                "test": test,
                "label": label,
                "ruvio": statistics.median(samples[test]["ruvio"]),
                "redis": statistics.median(samples[test]["redis"]),
                "samples": samples[test],
            }
            for test, label, _ in tests
        ],
        "memory": mem,
        "seconds": round(time.monotonic() - started, 1),
    }

    markdown, html = render(results)
    (args.out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.out / "table.md").write_text(markdown)
    (args.out / "table.html").write_text(html)
    print()
    print(markdown)
    print(f"{results['seconds']} s, written to {args.out}")


if __name__ == "__main__":
    main()
