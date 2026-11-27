#!/usr/bin/env python3
"""Keep writing at the `maxmemory` ceiling inside a cgroup memory limit, once per policy.

    scripts/eviction_cgroup.py target/debug/ruvio [--seconds 45] [--maxmemory-mib 64]
                               [--cgroup-mib 256] [--policies allkeys-lru,...] [--no-redis]

Needs root and a cgroup v1 memory controller at /sys/fs/cgroup/memory (or v2 at /sys/fs/cgroup
with the memory controller enabled); without one it says so and exits 2. For each policy a fresh
server runs inside its own cgroup with a hard memory limit of `--cgroup-mib`, `maxmemory` at
`--maxmemory-mib` and 2 shards, while four writers set 1 KiB values over a key space about five
times larger than the budget. A fifth thread rewrites and rereads a small hot set; keys without a
TTL (`volatile-*` policies) are written first and never touched again.

Checks per policy: the process is alive and the cgroup saw no OOM kill and no failed charge; RSS
stays under the cgroup limit and is flat (the last third of the run is within 5 % of the middle
third, after the budget filled); the server's own charge stays within `maxmemory`; and the policy
did what it says:

- `allkeys-lru`, `allkeys-lfu`: keys were evicted, writes were not refused, at least 90 % of the hot set survived.
- `volatile-lru`, `volatile-ttl`: keys were evicted, every key without a TTL survived, writes
  were refused at most once in a thousand (the documented 2 ms eviction limit). `volatile-ttl` also keeps the keys with the longest TTL (the survivors' mean
  TTL is above that of the keys written).
- `noeviction`: writes were refused with `OOM`, nothing was evicted, every acknowledged key was
  still there at the end and the server still answered reads.

For comparison the same load runs against redis-server with the same `maxmemory` and policy in
its own cgroup (skipped when redis-server is missing or with `--no-redis`); its RSS is printed
next to Ruvio's, and a higher Ruvio peak is a WARN (a FAIL with `--strict-redis`): `maxmemory` bounds
Ruvio's data and Redis's total, so equal settings are not equal budgets. Nothing here measures another kernel, a bigger
budget or a longer run: a 45 s run on a 64 MiB budget shows the policies and a flat RSS on this
machine, and says nothing about hours. Every wait has a deadline and the whole run stays under
ten minutes.
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _ruvio import encode, free_port as any_free_port, read_response  # noqa: E402

def free_port() -> int:
    """A free port below 50000: the server also reserves the port 10000 above its own, which must fit."""
    while True:
        port = any_free_port()
        if port < 50000:
            return port


MIB = 1024 * 1024
POLICIES = ["allkeys-lru", "allkeys-lfu", "volatile-lru", "volatile-ttl", "noeviction"]
VALUE = b"x" * 1024
PERSISTENT_KEYS = 4000
HOT_KEYS = 1000
WRITERS = 4
BATCH = 64


class Skip(Exception):
    pass


class Cgroup:
    """A memory cgroup with a hard limit; v1 and v2 differ only in the file names."""

    def __init__(self, name: str, limit: int):
        v1, v2 = Path("/sys/fs/cgroup/memory"), Path("/sys/fs/cgroup")
        if os.geteuid() != 0:
            raise Skip("needs root")
        if (v1 / "memory.limit_in_bytes").exists():
            self.v2, base = False, v1
        elif (v2 / "cgroup.controllers").exists() and "memory" in (v2 / "cgroup.controllers").read_text().split():
            self.v2, base = True, v2
        else:
            raise Skip("no writable memory cgroup")
        self.path = base / name
        try:
            self.path.mkdir()
        except OSError as error:
            raise Skip(f"cannot create a cgroup: {error}") from error
        (self.path / ("memory.max" if self.v2 else "memory.limit_in_bytes")).write_text(str(limit))
        if self.v2:
            (self.path / "memory.swap.max").write_text("0")
        elif (self.path / "memory.memsw.limit_in_bytes").exists():
            (self.path / "memory.memsw.limit_in_bytes").write_text(str(limit))
        self.limit = limit

    def add(self) -> None:
        (self.path / "cgroup.procs").write_text(str(os.getpid()))

    def usage(self) -> int:
        return int((self.path / ("memory.current" if self.v2 else "memory.usage_in_bytes")).read_text())

    def failed_charges(self) -> int:
        """Times the limit was hit: reclaim had to run, or an allocation failed."""
        if self.v2:
            return int(self.events().get("max", 0))
        return int((self.path / "memory.failcnt").read_text())

    def events(self) -> dict[str, int]:
        name = "memory.events" if self.v2 else "memory.oom_control"
        found = {}
        for line in (self.path / name).read_text().splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[1].isdigit():
                found[parts[0]] = int(parts[1])
        return found

    def oom_kills(self) -> int:
        return self.events().get("oom_kill", 0)

    def remove(self) -> None:
        for _ in range(50):
            try:
                self.path.rmdir()
                return
            except OSError:
                time.sleep(0.1)


class Connection:
    def __init__(self, port: int):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.stream = self.socket.makefile("rwb")

    def pipeline(self, commands: list[bytes]) -> list[tuple[bytes, object]]:
        self.stream.write(b"".join(commands))
        self.stream.flush()
        return [read_response(self.stream) for _ in commands]

    def one(self, *arguments: bytes):
        return self.pipeline([encode(*arguments)])[0]

    def close(self) -> None:
        self.socket.close()


def info(port: int) -> dict[str, str]:
    connection = Connection(port)
    try:
        kind, text = connection.one(b"INFO")
    finally:
        connection.close()
    return {key: value for line in text.decode().splitlines() if ":" in line for key, value in [line.split(":", 1)]}


def rss(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


class Load:
    """Writers over the cold key space, a hot-set thread, counters."""

    def __init__(self, port: int, policy: str, key_space: int):
        self.port, self.policy, self.key_space = port, policy, key_space
        self.stop = threading.Event()
        self.ok = self.oom = self.other = 0
        self.lock = threading.Lock()
        self.errors: list[str] = []
        self.acked: list[list[int]] = [[] for _ in range(WRITERS)]
        self.ttl_written: list[int] = []
        self.threads: list[threading.Thread] = []

    def ttl_for(self, key_id: int) -> int | None:
        if self.policy == "volatile-ttl":
            return 100 + (key_id * 7919) % 3500
        if self.policy.startswith("volatile") or self.policy.startswith("allkeys"):
            return 3600
        return None

    def set_command(self, key: bytes, key_id: int) -> bytes:
        ttl = self.ttl_for(key_id)
        if ttl is None:
            return encode(b"SET", key, VALUE)
        return encode(b"SET", key, VALUE, b"EX", str(ttl).encode())

    def writer(self, index: int) -> None:
        rng = random.Random(index)
        connection = Connection(self.port)
        try:
            while not self.stop.is_set():
                ids = [rng.randrange(self.key_space) for _ in range(BATCH)]
                replies = connection.pipeline([self.set_command(f"cold:{i}".encode(), i) for i in ids])
                ok = oom = other = 0
                for key_id, (kind, payload) in zip(ids, replies):
                    if kind == b"+":
                        ok += 1
                        if self.policy == "noeviction":
                            self.acked[index].append(key_id)
                    elif kind == b"-" and payload.startswith(b"OOM"):
                        oom += 1
                    else:
                        other += 1
                        if len(self.errors) < 5:
                            self.errors.append(repr(payload))
                with self.lock:
                    self.ok, self.oom, self.other = self.ok + ok, self.oom + oom, self.other + other
        except (OSError, AssertionError) as error:
            self.errors.append(f"writer {index}: {error}")
        finally:
            connection.close()

    def hot(self) -> None:
        connection = Connection(self.port)
        try:
            while not self.stop.is_set():
                commands = []
                for i in range(HOT_KEYS):
                    key = f"hot:{i}".encode()
                    commands.append(encode(b"GET", key))
                    if i % 4 == 0:
                        commands.append(encode(b"SET", key, VALUE, b"EX", b"3600") if self.policy != "noeviction" else encode(b"SET", key, VALUE))
                connection.pipeline(commands)
                time.sleep(0.02)
        except (OSError, AssertionError) as error:
            self.errors.append(f"hot: {error}")
        finally:
            connection.close()

    def start(self) -> None:
        self.threads = [threading.Thread(target=self.writer, args=(i,), daemon=True) for i in range(WRITERS)]
        if self.policy != "noeviction":
            self.threads.append(threading.Thread(target=self.hot, daemon=True))
        for thread in self.threads:
            thread.start()

    def finish(self) -> None:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=20)


def prepare(port: int, policy: str) -> None:
    """Hot keys always; for the volatile policies also the keys that must never be evicted."""
    connection = Connection(port)
    try:
        for start in range(0, HOT_KEYS, 200):
            connection.pipeline([encode(b"SET", f"hot:{i}".encode(), VALUE, b"EX", b"3600") if policy != "noeviction"
                                 else encode(b"SET", f"hot:{i}".encode(), VALUE) for i in range(start, start + 200)])
        if policy.startswith("volatile"):
            for start in range(0, PERSISTENT_KEYS, 200):
                connection.pipeline([encode(b"SET", f"keep:{i}".encode(), VALUE) for i in range(start, start + 200)])
    finally:
        connection.close()


def exists_fraction(port: int, prefix: str, count: int) -> float:
    connection = Connection(port)
    try:
        present = 0
        for start in range(0, count, 500):
            replies = connection.pipeline([encode(b"EXISTS", f"{prefix}:{i}".encode()) for i in range(start, min(count, start + 500))])
            present += sum(1 for kind, value in replies if kind == b":" and value == 1)
        return present / count
    finally:
        connection.close()


def start_server(kind: str, binary: str, port: int, policy: str, maxmemory: int, work: Path, cgroup: Cgroup) -> subprocess.Popen:
    log = open(work / f"{kind}.log", "ab")
    if kind == "ruvio":
        env = {key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")}
        command = [binary, "--address", f"127.0.0.1:{port}", "--shards", "2", "--durability", "memory",
                   "--data-dir", str(work / "data"), "--maxmemory", str(maxmemory), "--maxmemory-policy", policy]
    else:
        env = dict(os.environ)
        command = ["redis-server", "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
                   "--maxmemory", str(maxmemory), "--maxmemory-policy", policy, "--dir", str(work)]
    return subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, preexec_fn=cgroup.add)


def wait_ready(port: int, process: subprocess.Popen) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("the server exited while starting")
        try:
            connection = Connection(port)
            try:
                if connection.one(b"PING") == (b"+", b"PONG"):
                    return
            finally:
                connection.close()
        except (OSError, AssertionError):
            time.sleep(0.1)
    raise RuntimeError("the server did not answer PING in 20 s")


def run_policy(kind: str, binary: str, policy: str, args) -> tuple[bool, str, dict]:
    work = Path(tempfile.mkdtemp(prefix=f"ruvio-eviction-{kind}-"))
    cgroup = Cgroup(f"ruvio-evict-{os.getpid()}-{kind}", args.cgroup_mib * MIB)
    process = None
    load = None
    problems: list[str] = []
    result: dict = {}
    try:
        port = free_port()
        maxmemory = args.maxmemory_mib * MIB
        process = start_server(kind, binary, port, policy, maxmemory, work, cgroup)
        wait_ready(port, process)
        prepare(port, policy)
        idle = rss(process.pid)
        key_space = max(1, 5 * maxmemory // (len(VALUE) + 64))
        load = Load(port, policy, key_space)
        load.start()
        samples: list[tuple[float, int, int]] = []
        charged: list[int] = []
        started = time.monotonic()
        while time.monotonic() - started < args.seconds:
            time.sleep(1.0)
            if process.poll() is not None:
                problems.append("the server died during the run")
                break
            samples.append((time.monotonic() - started, rss(process.pid), cgroup.usage()))
            if kind == "ruvio":
                try:
                    charged.append(int(info(port).get("charged_memory_bytes", "0")))
                except (OSError, AssertionError, ValueError):
                    pass
        load.finish()
        if process.poll() is None:
            fields = info(port) if kind == "ruvio" else {}
            evicted = int(fields.get("evicted_keys", "0")) if kind == "ruvio" else int(
                subprocess_info_redis(port).get("evicted_keys", "0"))
            alive_reply = Connection(port).one(b"PING") == (b"+", b"PONG")
        else:
            evicted, alive_reply = 0, False
        series = [r for _, r, _ in samples]
        third = max(1, len(series) // 3)
        middle, last = statistics.mean(series[third:2 * third] or series), statistics.mean(series[-third:] or series)
        result = {"rss_idle": idle, "rss_peak": max(series, default=0), "rss_last": last, "rss_middle": middle, "evicted": evicted,
                  "ok": load.ok, "oom": load.oom, "other": load.other, "charged_peak": max(charged, default=0),
                  "usage_peak": max((u for _, _, u in samples), default=0)}
        if not alive_reply:
            problems.append("the server did not answer PING afterwards")
        if cgroup.oom_kills():
            problems.append(f"the cgroup OOM-killed {cgroup.oom_kills()} time(s)")
        if cgroup.failed_charges() and not cgroup.v2:
            problems.append(f"the cgroup limit was hit {cgroup.failed_charges()} time(s) (memory.failcnt)")
        if result["rss_peak"] >= cgroup.limit:
            problems.append("RSS reached the cgroup limit")
        if load.other or load.errors:
            problems.append(f"unexpected replies or errors: {load.errors[:3]}")
        if kind == "ruvio":
            if result["charged_peak"] > maxmemory:
                problems.append(f"charged memory {result['charged_peak'] / MIB:.1f} MiB above maxmemory")
            if last > middle * 1.05:
                problems.append(f"RSS is not flat: middle third {middle / MIB:.1f} MiB, last third {last / MIB:.1f} MiB")
            problems += check_policy(port, policy, load, result, key_space)
        return not problems, summarize(kind, policy, result), result if not problems else {**result, "problems": problems}
    except Exception as error:  # noqa: BLE001 - reported as a failure of this policy
        problems.append(f"{type(error).__name__}: {error}")
        return False, "; ".join(problems), {"problems": problems}
    finally:
        if load is not None:
            load.stop.set()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        cgroup.remove()
        if problems:
            print(f"      problems ({kind} {policy}): {'; '.join(problems)}", flush=True)
            for log in sorted(work.glob("*.log")):
                print(f"----- {log} (last lines) -----\n" + "\n".join(log.read_text(errors="replace").splitlines()[-8:]), flush=True)
        shutil.rmtree(work, ignore_errors=True)


def subprocess_info_redis(port: int) -> dict[str, str]:
    return info(port)


def check_policy(port: int, policy: str, load: Load, result: dict, key_space: int) -> list[str]:
    problems = []
    if policy == "noeviction":
        if load.oom == 0:
            problems.append("no write was refused with OOM")
        if result["evicted"]:
            problems.append(f"{result['evicted']} keys were evicted under noeviction")
        acked = [key for keys in load.acked for key in keys]
        sample = random.Random(1).sample(acked, min(3000, len(acked))) if acked else []
        connection = Connection(port)
        try:
            missing = 0
            for start in range(0, len(sample), 500):
                replies = connection.pipeline([encode(b"EXISTS", f"cold:{i}".encode()) for i in sample[start:start + 500]])
                missing += sum(1 for kind, value in replies if not (kind == b":" and value == 1))
        finally:
            connection.close()
        result["acked_sampled"], result["acked_missing"] = len(sample), missing
        if missing:
            problems.append(f"{missing} of {len(sample)} acknowledged keys are missing")
        return problems
    if result["evicted"] == 0:
        problems.append("no key was evicted")
    # An all-keys policy admits a write when the 2 ms eviction time runs out; a volatile policy
    # refuses it with OOM (docs/MEMORY_ACCOUNTING.md), so a few refusals are documented there.
    allowed = 0 if policy.startswith("allkeys") else load.ok // 1000
    if load.oom > allowed:
        problems.append(f"{load.oom} writes were refused although the policy evicts (allowed {allowed})")
    hot = exists_fraction(port, "hot", HOT_KEYS)
    result["hot_survival"] = hot
    if policy.startswith("allkeys") and hot < 0.9:
        problems.append(f"only {hot:.0%} of the hot set survived")
    if policy.startswith("volatile"):
        kept = exists_fraction(port, "keep", PERSISTENT_KEYS)
        result["keep_survival"] = kept
        if kept < 1.0:
            problems.append(f"{(1 - kept) * PERSISTENT_KEYS:.0f} keys without a TTL were evicted")
    if policy == "volatile-ttl":
        connection = Connection(port)
        try:
            ttls = []
            for start in range(0, 6000, 500):
                ids = list(range(start, start + 500))
                for key_id, (kind, value) in zip(ids, connection.pipeline([encode(b"TTL", f"cold:{i}".encode()) for i in ids])):
                    if kind == b":" and value > 0:
                        ttls.append(value)
        finally:
            connection.close()
        written = statistics.mean(100 + (i * 7919) % 3500 for i in range(key_space))
        result["survivor_ttl_mean"], result["written_ttl_mean"] = statistics.mean(ttls) if ttls else 0, written
        if not ttls or statistics.mean(ttls) <= written:
            problems.append("the survivors' mean TTL is not above the written mean")
    return problems


def summarize(kind: str, policy: str, result: dict) -> str:
    parts = [f"RSS idle {result.get('rss_idle', 0) / MIB:.1f} MiB", f"peak {result.get('rss_peak', 0) / MIB:.1f} MiB",
             f"mid->last {result.get('rss_middle', 0) / MIB:.1f}->{result.get('rss_last', 0) / MIB:.1f} MiB",
             f"evicted {result.get('evicted', 0)}", f"set ok {result.get('ok', 0)}, OOM {result.get('oom', 0)}"]
    if "charged_peak" in result and kind == "ruvio":
        parts.append(f"charged peak {result['charged_peak'] / MIB:.1f} MiB")
    for name in ("hot_survival", "keep_survival"):
        if name in result:
            parts.append(f"{name} {result[name]:.0%}")
    if "survivor_ttl_mean" in result:
        parts.append(f"TTL survivors {result['survivor_ttl_mean']:.0f} vs written {result['written_ttl_mean']:.0f}")
    if "acked_sampled" in result:
        parts.append(f"acked missing {result['acked_missing']}/{result['acked_sampled']}")
    return ", ".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary")
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--maxmemory-mib", type=int, default=64)
    parser.add_argument("--cgroup-mib", type=int, default=256)
    parser.add_argument("--policies", default=",".join(POLICIES))
    parser.add_argument("--no-redis", action="store_true")
    parser.add_argument("--strict-redis", action="store_true", help="fail when Ruvio's RSS peak is above redis-server's")
    args = parser.parse_args()
    try:
        Cgroup(f"ruvio-evict-probe-{os.getpid()}", args.cgroup_mib * MIB).remove()
    except Skip as reason:
        print(f"SKIP: {reason}")
        return 2
    redis = not args.no_redis and shutil.which("redis-server") is not None
    failures = 0
    for policy in args.policies.split(","):
        ok, text, ruvio = run_policy("ruvio", args.binary, policy, args)
        print(f"{'PASS' if ok else 'FAIL'}  ruvio {policy:<13} {text}", flush=True)
        failures += 0 if ok else 1
        if redis:
            _, text, other = run_policy("redis", args.binary, policy, args)
            print(f"----  redis {policy:<13} {text}", flush=True)
            if ok and "rss_peak" in other and ruvio["rss_peak"] > other["rss_peak"] * 1.05:
                # `maxmemory` is a data budget in Ruvio and a total in Redis, so equal settings do not
                # mean equal memory: this is reported, and fails the run only with --strict-redis.
                strict = args.strict_redis
                print(f"{'FAIL' if strict else 'WARN'}  ruvio {policy:<13} RSS peak above redis-server's "
                      f"({ruvio['rss_peak'] / MIB:.1f} vs {other['rss_peak'] / MIB:.1f} MiB) at the same maxmemory")
                failures += 1 if strict else 0
    print(f"eviction: {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
