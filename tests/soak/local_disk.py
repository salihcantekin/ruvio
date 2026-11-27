#!/usr/bin/env python3
"""Disk and crash scenarios on this machine, on loop-mounted ext4 disks (needs root, `losetup`,
`mkfs.ext4`, `mount` and `fsfreeze`; without them each scenario says so and exits 2). Every
scenario is under ten minutes and costs nothing. A loop disk freezes cleanly, a cloud disk does
not: this proves what the server does when a disk stops or fails, not how Azure disks behave.

    python3 tests/soak/local_disk.py freeze   --binary target/debug/ruvio [--duration 420]
    python3 tests/soak/local_disk.py kill9    --binary target/debug/ruvio [--rounds 40]
    python3 tests/soak/local_disk.py diskfull --binary target/debug/ruvio
    python3 tests/soak/local_disk.py walcorrupt --binary target/debug/ruvio

`freeze`: three hosts, each on its own disk, a soak run without plan faults; every host's disk is
frozen twice for 8 s. Passes when no failover happened outside a fault and no acknowledged write
was lost (soak criteria 1, 2, 6 and 8). `kill9`: a node with `always` is killed with SIGKILL at
random moments while one writer sets keys, with a checkpoint every 300 ms; every acknowledged
key must be there after the restart. `diskfull`: the server on a 24 MiB disk is written until the
disk is full: it must refuse writes with an error, stay up and serve reads, and write again once
space is freed. `walcorrupt`: a log with a cut last record, one damaged byte and an empty
segment: the server must not answer with a wrong value, whether it starts or refuses to, and a dropped
unfinished end of the log must be in its log."""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ruvio_soak.resp import Connection, ReplyError, Unknown  # noqa: E402

HERE = Path(__file__).resolve().parent
FREEZE_S = 8.0
NEEDED_TOOLS = ("losetup", "mkfs.ext4", "mount", "umount", "fsfreeze")


class Skip(Exception):
    pass


def require_root_tools() -> None:
    if os.geteuid() != 0:
        raise Skip("needs root")
    missing = [t for t in NEEDED_TOOLS if shutil.which(t) is None]
    if missing:
        raise Skip(f"missing tools: {', '.join(missing)}")


def sh(*command, check=True) -> subprocess.CompletedProcess:
    return subprocess.run(command, check=check, capture_output=True, text=True)


class Disk:
    """A sparse image formatted as ext4 and mounted at `mount`."""

    def __init__(self, image: Path, mount: Path, megabytes: int):
        self.image, self.mount, self.megabytes, self.mounted = image, mount, megabytes, False

    def __enter__(self) -> "Disk":
        self.mount.mkdir(parents=True, exist_ok=True)
        with open(self.image, "wb") as f:
            f.truncate(self.megabytes << 20)
        sh("mkfs.ext4", "-q", "-F", str(self.image))
        sh("mount", "-o", "loop", str(self.image), str(self.mount))
        self.mounted = True
        return self

    def freeze(self) -> None:
        sh("fsfreeze", "-f", str(self.mount))

    def unfreeze(self) -> None:
        sh("fsfreeze", "-u", str(self.mount), check=False)

    def __exit__(self, *exc) -> None:
        if self.mounted:
            self.unfreeze()
            for _ in range(10):
                if sh("umount", str(self.mount), check=False).returncode == 0:
                    break
                time.sleep(0.5)
            else:
                sh("umount", "-l", str(self.mount), check=False)
            self.mounted = False


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """One `ruvio` process on `data_dir` (durability `always`, one shard)."""

    def __init__(self, binary: str, data_dir: Path, work: Path, extra_env: dict[str, str] | None = None):
        self.binary, self.data_dir, self.work, self.extra = binary, data_dir, work, extra_env or {}
        self.port = free_port()
        self.process: subprocess.Popen | None = None
        self.log = work / "node.log"
        work.mkdir(parents=True, exist_ok=True)

    def start(self, ready_timeout: float = 20.0) -> bool:
        env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{self.port}", RUVIO_DURABILITY="always",
                   RUVIO_DATA_DIR=str(self.data_dir), RUVIO_SHARDS="1", **self.extra)
        log = open(self.log, "ab")
        self.process = subprocess.Popen([self.binary], env=env, cwd=self.work, stdout=log, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        log.close()
        deadline = time.monotonic() + ready_timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                return False
            try:
                c = Connection("127.0.0.1", self.port, None, 1.0)
                try:
                    if text(c.call("PING")) == "PONG":
                        return True
                finally:
                    c.close()
            except (OSError, Unknown, ReplyError):
                time.sleep(0.1)
        return False

    def connect(self, timeout: float = 5.0) -> Connection:
        return Connection("127.0.0.1", self.port, None, timeout)

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def kill9(self) -> None:
        if self.alive():
            self.process.send_signal(signal.SIGKILL)
        if self.process is not None:
            self.process.wait()

    def stop(self) -> None:
        if self.alive():
            self.process.send_signal(signal.SIGCONT)
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.kill9()

    def log_tail(self, lines: int = 8) -> str:
        try:
            return "".join(self.log.read_text(errors="replace").splitlines(True)[-lines:])
        except OSError:
            return ""


def check(results: list[dict], name: str, ok: bool, detail=None) -> None:
    results.append({"check": name, "ok": bool(ok), "detail": detail})
    print(f"  {'ok  ' if ok else 'FAIL'} {name}" + (f": {detail}" if detail is not None and not ok else ""), flush=True)


def text(value) -> str | None:
    return value.decode() if isinstance(value, bytes) else value


# ---------------------------------------------------------------------------------- kill9


def scenario_kill9(options, base: Path) -> list[dict]:
    results: list[dict] = []
    random.seed(options.seed)
    with Disk(base / "d.img", base / "mnt", 256) as disk:
        node = Node(options.binary, disk.mount / "data", base / "work", {"RUVIO_SNAPSHOT_INTERVAL_MS": "300"})
        acked: dict[str, str] = {}
        counter = 0
        started_in = []
        try:
            for round_ in range(options.rounds):
                began = time.monotonic()
                if not node.start():
                    check(results, f"round {round_}: the server started", False, node.log_tail())
                    break
                started_in.append(round(time.monotonic() - began, 2))
                c = node.connect()
                missing = []
                for key, value in acked.items():
                    try:
                        if text(c.call("GET", key)) != value:
                            missing.append(key)
                    except (Unknown, ReplyError, OSError):
                        missing.append(key)
                if missing:
                    check(results, f"round {round_}: every acknowledged key is present", False, f"{len(missing)} missing, e.g. {missing[:3]}")
                    break
                stop_at = time.monotonic() + random.uniform(0.2, 1.5)
                while time.monotonic() < stop_at:
                    key, value = f"k{counter}", f"v{counter}"
                    counter += 1
                    try:
                        c.call("SET", key, value)
                        acked[key] = value
                    except (Unknown, ReplyError, OSError):
                        break
                c.close()
                node.kill9()
            else:
                check(results, f"{options.rounds} kill -9 rounds: every acknowledged key survived every restart", True)
                check(results, "acknowledged writes were made", len(acked) > options.rounds, len(acked))
        finally:
            node.kill9()
        results.append({"check": "info", "ok": True, "detail": {"acked": len(acked), "start_s_max": max(started_in, default=None)}})
    return results


# -------------------------------------------------------------------------------- diskfull


def scenario_diskfull(options, base: Path) -> list[dict]:
    results: list[dict] = []
    with Disk(base / "d.img", base / "mnt", 24) as disk:
        # Space the server cannot give back by itself (a deleted key is a log record too, and
        # needs space): this file is removed to make room again.
        ballast = disk.mount / "ballast"
        ballast.write_bytes(b"\0" * (6 << 20))
        node = Node(options.binary, disk.mount / "data", base / "work")
        check(results, "the server started", node.start(), node.log_tail())
        if not node.alive():
            return results
        c = node.connect()
        value = "x" * 8192
        acked, refused_at, refusal = [], None, None
        for i in range(20000):
            try:
                c.call("SET", f"k{i}", value)
                acked.append(f"k{i}")
            except ReplyError as error:
                refused_at, refusal = i, ("error reply", str(error))
                break
            except (Unknown, OSError) as error:
                refused_at, refusal = i, ("closed connection", str(error))
                break
        check(results, "the disk filled and a write was refused", refused_at is not None, len(acked))
        check(results, "the refused write is answered with an error that says the outcome is unknown",
              refusal is not None and refusal[0] == "error reply" and "outcome of this write is unknown" in refusal[1], refusal)
        check(results, "the server is still running", node.alive(), node.log_tail())
        if node.alive():
            try:
                c2 = node.connect()
                got = text(c2.call("GET", acked[0] if acked else "k0"))
                check(results, "reads still work", got == value and text(c2.call("PING")) == "PONG", got)
                c2.close()
            except (Unknown, ReplyError, OSError) as error:
                check(results, "reads still work", False, str(error))
        check(results, "the log says why writes are refused", "No space left" in node.log_tail(12), node.log_tail(12))
        # A restart while the disk is still full must fail loudly or work, never lose data silently.
        node.kill9()
        started_full = node.start(15)
        results.append({"check": "restart while the disk is still full", "ok": True, "detail": {"started": started_full, "log": node.log_tail(3)[-300:]}})
        print(f"       restart on a full disk: started={started_full}", flush=True)
        if not started_full:
            check(results, "a restart that cannot write says so", "No space left" in node.log_tail(6), node.log_tail(6))
        node.kill9()
        ballast.unlink()
        check(results, "the server restarts once space is freed", node.start(30), node.log_tail())
        if node.alive():
            c5 = node.connect()
            lost = []
            for key in acked:
                try:
                    if text(c5.call("GET", key)) != value:
                        lost.append(key)
                except (Unknown, ReplyError, OSError):
                    lost.append(key)
            check(results, "keys acknowledged before the disk filled survive", not lost, f"{len(lost)} lost of {len(acked)}")
            try:
                c5.call("SET", "after-space", "ok")
                check(results, "writes work again once space is freed", text(c5.call("GET", "after-space")) == "ok")
            except (Unknown, ReplyError, OSError) as error:
                check(results, "writes work again once space is freed", False, str(error))
            c5.close()
        node.kill9()
    return results


# ------------------------------------------------------------------------------ walcorrupt


def wal_segments(data: Path) -> list[Path]:
    return sorted(data.glob("wal/*/*.wal"))


def scenario_walcorrupt(options, base: Path) -> list[dict]:
    results: list[dict] = []
    reference = base / "reference"
    node = Node(options.binary, reference, base / "w0")
    if not node.start():
        check(results, "the server started", False, node.log_tail())
        return results
    c = node.connect()
    count = 1000
    for i in range(count):
        c.call("SET", f"k{i}", f"v{i}")
    c.close()
    node.kill9()
    segments = wal_segments(reference)
    check(results, "the log has a segment", bool(segments), [str(p) for p in segments])
    if not segments:
        return results

    def variant(name: str, damage, warns: bool = False) -> None:
        copy = base / f"data-{name}"
        shutil.copytree(reference, copy)
        damage(wal_segments(copy)[-1])
        n = Node(options.binary, copy, base / f"w-{name}")
        started = n.start(30)
        wrong, present = [], 0
        if started:
            cc = n.connect()
            for i in range(count):
                try:
                    got = text(cc.call("GET", f"k{i}"))
                except (Unknown, ReplyError, OSError):
                    got = "<error>"
                if got is None:
                    continue
                present += 1
                if got != f"v{i}":
                    wrong.append(i)
            cc.close()
        n.kill9()
        tail = n.log_tail(4)
        if warns:
            check(results, f"{name}: the log says the unfinished end of the WAL was dropped",
                  any(w in n.log.read_text(errors="replace") for w in ("unfinished end of the WAL", "dropped the last WAL segment")), n.log_tail(6))
        check(results, f"{name}: no key has a wrong value", not wrong, wrong[:5])
        check(results, f"{name}: the server either started or said why it did not",
              started or any(w in tail.lower() for w in ("wal", "log", "corrupt", "checksum", "record")), tail)
        results.append({"check": f"{name}: outcome", "ok": True, "detail": {"started": started, "keys_present": present, "of": count, "log": tail[-300:]}})
        print(f"       {name}: started={started} keys={present}/{count}", flush=True)

    # A segment is preallocated and zero-filled past its last record, so damage has to land
    # inside the written part: its end is the last non-zero byte.
    def written_end(path: Path) -> int:
        return len(path.read_bytes().rstrip(b"\0"))

    def cut_last_bytes(path: Path) -> None:
        # Cuts into the last record and zeroes the rest, as a crash mid-write leaves it.
        end = written_end(path)
        with open(path, "r+b") as f:
            f.seek(end - 5)
            f.write(b"\0" * 5)

    def flip_middle_byte(path: Path) -> None:
        with open(path, "r+b") as f:
            f.seek(written_end(path) // 2)
            byte = f.read(1)
            f.seek(-1, os.SEEK_CUR)
            f.write(bytes([byte[0] ^ 0xFF]))

    def empty_segment(path: Path) -> None:
        with open(path, "r+b") as f:
            f.truncate(0)

    variant("cut-last-record", cut_last_bytes, warns=True)
    variant("damaged-byte", flip_middle_byte)
    variant("empty-segment", empty_segment, warns=True)
    return results


# ---------------------------------------------------------------------------------- freeze


def freeze_plan(duration: int) -> dict:
    return {
        "name": "local-freeze", "seed": 3, "duration_s": duration, "warmup_s": 20, "restart_as": "mixed",
        "cluster": {"ping_interval_ms": 500, "down_after_ms": 3000, "primary_lease_ms": 2000, "snapshot_interval_ms": 20000},
        "load": {"rate_per_worker": 25, "keys_per_worker": 8, "counters": 2, "appenders": 2, "registers": 2,
                 "readers_per_register": 2, "max_list_len": 20000,
                 "profiles": {"default": {}, "durable": {"replicas": 1}}},
        "faults": {"kinds": [], "start_s": 100000},
        "metrics_interval_s": 5, "status_interval_s": 30, "check_buckets": 1, "report_settle_s": 30,
        "startup_timeout_s": 90,
    }


def wait_for_event(directory: Path, name: str, timeout: float) -> float | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for path in directory.glob("*/events.jsonl"):
            try:
                for line in path.read_text().splitlines():
                    event = json.loads(line)
                    if event.get("event") == name:
                        return event["t"]
            except (OSError, ValueError):
                pass
        time.sleep(1)
    return None


def scenario_freeze(options, base: Path) -> list[dict]:
    results: list[dict] = []
    work, out = base / "work", base / "runs"
    plan = base / "plan.json"
    plan.write_text(json.dumps(freeze_plan(options.duration)))
    disks = [Disk(base / f"host{i}.img", work / f"host{i}", 512) for i in range(3)]
    freezes: list[dict] = []
    try:
        for disk in disks:
            disk.__enter__()
        run = subprocess.Popen([sys.executable, str(HERE / "run.py"), "run", str(plan), "--backend", "local", "--work", str(work),
                                "--binary", options.binary, "--out", str(out)], stdout=open(base / "run.log", "w"), stderr=subprocess.STDOUT)
        started = wait_for_event(out, "load-start", 240)
        check(results, "the cluster started and the load began", started is not None, (base / "run.log").read_text()[-400:] if (base / "run.log").exists() else "")
        if started is not None:
            start = time.time()
            order = [0, 1, 2, 0, 1, 2]
            gap = max(20.0, (options.duration - 60) / len(order))
            for number, host in enumerate(order):
                wait = start + 30 + number * gap - time.time()
                if wait > 0 and run.poll() is None:
                    time.sleep(wait)
                if run.poll() is not None:
                    break
                began = time.time()
                disks[host].freeze()
                time.sleep(FREEZE_S)
                disks[host].unfreeze()
                freezes.append({"host": host, "t": round(began, 1), "seconds": FREEZE_S})
                print(f"  froze host{host} for {FREEZE_S:.0f} s", flush=True)
        try:
            run.wait(timeout=options.duration + 300)
        except subprocess.TimeoutExpired:
            run.kill()
            check(results, "the run finished in time", False)
    finally:
        for disk in reversed(disks):
            disk.__exit__()
    directories = sorted(out.glob("*/report.json"))
    check(results, "the run produced a report", bool(directories))
    if directories:
        report = json.loads(directories[-1].read_text())
        statuses = {c["id"]: c["status"] for c in report["criteria"]}
        (base / "freezes.json").write_text(json.dumps(freezes))
        check(results, "every disk was frozen twice", len(freezes) == 6, len(freezes))
        for ident, name in ((1, "durable: no anomaly"), (2, "default and client: no loss outside faults"),
                            (6, "no process exit or panic"), (8, "no failover outside a fault")):
            check(results, f"criterion {ident} ({name})", statuses.get(ident) == "pass", {"status": statuses.get(ident),
                  "value": next((c["value"] for c in report["criteria"] if c["id"] == ident), None)})
        results.append({"check": "info", "ok": True, "detail": {"criteria": statuses, "verdict": report["verdict"], "freezes": freezes,
                                                                 "report": str(directories[-1])}})
    return results


SCENARIOS = {"freeze": scenario_freeze, "kill9": scenario_kill9, "diskfull": scenario_diskfull, "walcorrupt": scenario_walcorrupt}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--binary", required=True)
    parser.add_argument("--base", help="a directory for images, data and logs (default: a new temporary one)")
    parser.add_argument("--duration", type=int, default=420, help="freeze: the load's length in seconds")
    parser.add_argument("--rounds", type=int, default=40, help="kill9: kill -9 rounds")
    parser.add_argument("--seed", type=int, default=1)
    options = parser.parse_args(argv)
    try:
        require_root_tools()
    except Skip as reason:
        print(f"skipped: {reason}")
        return 2
    base = Path(options.base) if options.base else Path(tempfile.mkdtemp(prefix="ruvio-local-disk-"))
    base.mkdir(parents=True, exist_ok=True)
    print(f"{options.scenario} in {base}", flush=True)
    results = SCENARIOS[options.scenario](options, base)
    (base / "results.json").write_text(json.dumps(results, indent=2))
    failed = [r for r in results if not r["ok"]]
    print(f"{options.scenario}: {'FAIL' if failed else 'pass'} ({len(results) - len(failed)} ok, {len(failed)} failed); results in {base}/results.json")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
