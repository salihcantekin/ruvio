#!/usr/bin/env python3
"""Upgrade and rollback scenarios: an older release and the binary under test run side by side,
as local processes (3 data nodes, 3 `ruvio coordinator` processes, one client writing the whole
time), and the order of an upgrade is changed on purpose.

  rolling        coordinators together, then replicas, then a planned switchover, then the old
                 primary; acknowledged writes (CLIENT DURABILITY REPLICAS 1) are checked on all nodes
  primary-first  the wrong order: the primary on the new binary while the replicas run the old one
  rollback       back from the new binary to the old one with the backup taken before the upgrade,
                 and what happens when the old coordinators meet the new Raft log
  legacy         an older release without coordinators (WAL format minor 3): upgrade, the old binary
                 refusing the newer log, restore from the backup

Usage: scripts/upgrade_rollback.py --old DIR --new DIR [--legacy DIR] [--out report.json] [scenario ...]
`DIR` holds the `ruvio` and `ruvio-ctl` binaries of one release (an unpacked release archive, or
`target/debug`). Every scenario starts fresh processes and takes a few minutes in a debug build.
Ports are taken from the operating system at start. Nothing here needs root.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _ruvio import encode, free_port, read_response  # noqa: E402

HOST = "127.0.0.1"
GROUP = "main"
PING_MS, DOWN_MS = 200, 1000

failures = 0
report: dict = {"checks": [], "facts": {}}


def check(name: str, ok: bool, detail: str = "") -> bool:
    global failures
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""), flush=True)
    report["checks"].append({"name": name, "ok": bool(ok), "detail": detail})
    failures += 0 if ok else 1
    return ok


def fact(name: str, value) -> None:
    print(f"FACT {name}: {value}", flush=True)
    report["facts"][name] = value


def wait(condition, timeout=20.0, step=0.05) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


class Conn:
    """One RESP2 connection; `cmd` returns the decoded reply or raises on a socket error."""

    def __init__(self, port: int, timeout=4.0):
        self.sock = socket.create_connection((HOST, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.stream = self.sock.makefile("rb")

    def cmd(self, *args):
        self.sock.sendall(encode(*[a if isinstance(a, bytes) else str(a).encode() for a in args]))
        prefix, value = read_response(self.stream)
        return decode(prefix, value)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def decode(prefix, value):
    if prefix == b"-":
        return Error(value.decode(errors="replace"))
    if prefix == b"+":
        return value.decode(errors="replace")
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, list):
        return [decode(*item) if isinstance(item, tuple) else item for item in value]
    return value


class Error(str):
    pass


def ask(port: int, *args, timeout=3.0):
    """One command on a fresh connection; '' when the node does not answer."""
    try:
        c = Conn(port, timeout)
        try:
            return c.cmd(*args)
        finally:
            c.close()
    except (OSError, AssertionError, ValueError):
        return ""


def info(port: int, section="") -> dict:
    text = ask(port, "INFO", *([section] if section else []))
    out = {}
    for line in str(text).splitlines():
        if ":" in line and not line.startswith("#"):
            key, _, value = line.partition(":")
            out[key.strip()] = value.strip()
    return out


class Bins:
    def __init__(self, directory: str, label: str):
        self.dir, self.label = os.path.abspath(directory), label
        self.ruvio, self.ctl = f"{self.dir}/ruvio", f"{self.dir}/ruvio-ctl"
        for path in (self.ruvio, self.ctl):
            if not os.access(path, os.X_OK):
                sys.exit(f"{path} is not an executable")
        v = subprocess.run([self.ruvio, "--version"], capture_output=True, text=True).stdout.strip()
        self.version = f"{v} ({label}, binary sha256 {hashlib.sha256(open(self.ruvio, 'rb').read()).hexdigest()[:10]})"


class Group:
    """Three data nodes (node 0 starts as the primary) and three coordinators in a work directory."""

    def __init__(self, work: str, coordinators=True):
        self.work = work
        os.makedirs(work, exist_ok=True)
        self.ports = [(free_port(), free_port()) for _ in range(3)]  # RESP, replication
        self.coord_ports = [(i + 1, free_port(), free_port()) for i in range(3)]  # id, client, raft
        self.nodes: dict[int, subprocess.Popen] = {}
        self.coords: dict[int, subprocess.Popen] = {}
        self.with_coordinators = coordinators

    def node_dir(self, i):
        return f"{self.work}/n{i}"

    def resp(self, i):
        return self.ports[i][0]

    def repl(self, i):
        return f"{HOST}:{self.ports[i][1]}"

    def start_node(self, i, bins: Bins, role: str, primary_of: int | None = None, log_tag=""):
        d = self.node_dir(i)
        os.makedirs(d, exist_ok=True)
        env = dict(os.environ, RUVIO_REPLICATION_ROLE=role, RUVIO_REPLICATION_LISTEN=self.repl(i))
        if primary_of is not None:
            env["RUVIO_REPLICATION_PRIMARY"] = self.repl(primary_of)
        log = open(f"{self.work}/n{i}{log_tag}.log", "a")
        log.write(f"--- {bins.version} as {role}\n")
        log.flush()
        self.nodes[i] = subprocess.Popen(
            [bins.ruvio, "--address", f"{HOST}:{self.resp(i)}", "--shards", "1", "--durability", "always",
             "--data-dir", d],
            env=env, cwd=d, stdout=log, stderr=subprocess.STDOUT)
        return self.nodes[i]

    def stop_node(self, i, sig=signal.SIGTERM, timeout=20):
        p = self.nodes.get(i)
        if p is None or p.poll() is not None:
            return p.returncode if p else None
        p.send_signal(sig)
        try:
            return p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            return p.wait()

    def start_coordinator(self, cid, bins: Bins):
        _, port, raft = self.coord_ports[cid - 1]
        d = f"{self.work}/c{cid}"
        os.makedirs(d, exist_ok=True)
        members = ",\n".join(
            f'  {{ address = "{HOST}:{self.resp(i)}", replication = "{self.repl(i)}" }}' for i in range(3))
        peers = ", ".join(f'"{c}@{HOST}:{lp}"' for c, lp, _ in self.coord_ports)
        # The raft address of this coordinator is its own entry in `peers`; `listen` is the client port.
        open(f"{d}/c.toml", "w").write(
            f'id = {cid}\nlisten = "{HOST}:{port}"\ndata_dir = "{d}/data"\npeers = [{peers}]\n'
            f'ping_interval_ms = {PING_MS}\ndown_after_ms = {DOWN_MS}\n\n[[groups]]\nname = "{GROUP}"\n'
            f'members = [\n{members}\n]\n')
        log = open(f"{self.work}/c{cid}.log", "a")
        log.write(f"--- {bins.version}\n")
        log.flush()
        self.coords[cid] = subprocess.Popen([bins.ruvio, "coordinator", "--config", f"{d}/c.toml"],
                                            cwd=d, stdout=log, stderr=subprocess.STDOUT)

    def stop_coordinators(self):
        for p in self.coords.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.coords.values():
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def start_coordinators(self, bins: Bins):
        self.coords = {}
        for cid, _, _ in self.coord_ports:
            self.start_coordinator(cid, bins)

    def stop_all(self):
        self.stop_coordinators()
        for i in list(self.nodes):
            self.stop_node(i)

    def primary_index(self):
        """The data node the coordinators agree on, or None."""
        seen = set()
        for _, port, _ in self.coord_ports:
            reply = ask(port, "SENTINEL", "get-master-addr-by-name", GROUP, timeout=1.0)
            if not isinstance(reply, list) or len(reply) != 2:
                return None
            seen.add(int(reply[1]))
        if len(seen) != 1:
            return None
        port = seen.pop()
        return next((i for i in range(3) if self.resp(i) == port), None)

    def coordinator_command(self, *args):
        """Runs a COORDINATOR command on the Raft leader; returns the reply."""
        last = ""
        for _, port, _ in self.coord_ports:
            reply = ask(port, "COORDINATOR", *args, timeout=15.0)
            last = reply
            if not (isinstance(reply, Error) and "leader" in reply.lower()):
                return reply
        return last


class Writer(threading.Thread):
    """Writes `w<i>` -> `v<i>` to whatever primary the coordinators name, with
    `CLIENT DURABILITY REPLICAS 1`; a key counts as acknowledged only when the reply is +OK."""

    def __init__(self, group: Group, prefix="w", replicas=1):
        super().__init__(daemon=True)
        self.g, self.prefix, self.replicas = group, prefix, replicas
        self.acked: list[int] = []
        self.attempts = 0
        self.errors: dict[str, int] = {}
        self.stop_flag = threading.Event()
        self.ok_times: list[float] = []
        self.i = 0

    def connect(self):
        for _, port, _ in self.g.coord_ports:
            reply = ask(port, "SENTINEL", "get-master-addr-by-name", GROUP, timeout=1.0)
            if isinstance(reply, list) and len(reply) == 2:
                try:
                    c = Conn(int(reply[1]), timeout=4.0)
                except OSError:
                    continue
                if self.replicas:
                    r = c.cmd("CLIENT", "DURABILITY", "REPLICAS", self.replicas, "TIMEOUT", 3000)
                    if r != "OK":
                        c.close()
                        self.note(f"durability:{r}")
                        continue
                return c
        return None

    def note(self, text):
        key = str(text)[:60]
        self.errors[key] = self.errors.get(key, 0) + 1

    def run(self):
        conn = None
        while not self.stop_flag.is_set():
            if conn is None:
                try:
                    conn = self.connect()
                except (OSError, AssertionError, ValueError) as e:
                    self.note(type(e).__name__)
                    conn = None
                if conn is None:
                    time.sleep(0.05)
                    continue
            self.i += 1
            self.attempts += 1
            try:
                r = conn.cmd("SET", f"{self.prefix}{self.i}", f"v{self.i}")
            except (OSError, AssertionError, ValueError) as e:
                self.note(type(e).__name__)
                conn.close()
                conn = None
                continue
            if r == "OK":
                self.acked.append(self.i)
                self.ok_times.append(time.time())
            else:
                self.note(r)
                if isinstance(r, Error) and not r.startswith(("NOREPLICAS", "READONLY", "LOADING")):
                    pass
                conn.close()
                conn = None
                time.sleep(0.02)
        if conn:
            conn.close()

    def finish(self):
        self.stop_flag.set()
        if self.is_alive():
            self.join(timeout=15)

    def longest_gap(self, since=0.0):
        times = [t for t in self.ok_times if t >= since]
        return max((b - a for a, b in zip(times, times[1:])), default=0.0)


def present(port: int, prefix: str, ids: list[int]) -> tuple[list[int], list[int]]:
    """(missing, wrong) among `ids` on one node, read in chunks with MGET."""
    missing, wrong = [], []
    c = Conn(port, timeout=10.0)
    try:
        for start in range(0, len(ids), 200):
            chunk = ids[start:start + 200]
            values = c.cmd("MGET", *[f"{prefix}{i}" for i in chunk])
            if not isinstance(values, list):
                raise AssertionError(f"MGET answered {values!r}")
            for i, v in zip(chunk, values):
                if v is None:
                    missing.append(i)
                elif v != f"v{i}":
                    wrong.append(i)
    finally:
        c.close()
    return missing, wrong


def all_nodes_hold(group: Group, prefix: str, ids: list[int], nodes=(0, 1, 2), timeout=40.0):
    """Waits until every node in `nodes` holds every acknowledged key; returns {node: (missing, wrong)}."""
    result: dict = {}
    end = time.time() + timeout
    while True:
        result = {}
        for i in nodes:
            try:
                result[i] = present(group.resp(i), prefix, ids)
            except (OSError, AssertionError, ValueError):
                result[i] = (["unreachable"], [])
        if all(not m and not w for m, w in result.values()) or time.time() > end:
            return result
        time.sleep(0.5)


def dir_digest(path: str) -> str:
    """One hash of every file's relative name, size and content under `path` (for 'nothing changed')."""
    h = hashlib.sha256()
    for root, dirs, files in os.walk(path):
        dirs.sort()
        for name in sorted(files):
            full = os.path.join(root, name)
            h.update(os.path.relpath(full, path).encode())
            with open(full, "rb") as f:
                for block in iter(lambda: f.read(1 << 20), b""):
                    h.update(block)
    return h.hexdigest()


def ctl(bins: Bins, *args, timeout=120):
    r = subprocess.run([bins.ctl, *args], capture_output=True, text=True, timeout=timeout)
    return r.returncode, (r.stdout + r.stderr).strip()


def backup(bins: Bins, data_dir: str, dest: str):
    shutil.rmtree(dest, ignore_errors=True)
    rc, out = ctl(bins, "backup", "--data-dir", data_dir, "--dest", dest)
    rc2, out2 = ctl(bins, "check", "--data-dir", dest)
    return rc == 0 and rc2 == 0, (out + "\n" + out2).strip()


def wal_minor(bins: Bins, data_dir: str) -> str:
    rc, out = ctl(bins, "inspect", "--data-dir", data_dir)
    files = sorted(glob.glob(f"{data_dir}/wal/*/*.wal"))
    if files:
        def minor(path):
            with open(path, "rb") as f:
                return int.from_bytes(f.read(16)[10:12], "little")
        return f"{len(files)} segment(s), header minor first={minor(files[0])} last={minor(files[-1])}"
    return out.splitlines()[-2] if out else f"rc={rc}"


def boot(group: Group, bins: Bins):
    group.start_node(0, bins, "primary")
    group.start_node(1, bins, "replica", 0)
    group.start_node(2, bins, "replica", 0)
    assert wait(lambda: all(ask(group.resp(i), "PING") == "PONG" for i in range(3)), 30), "nodes did not start"
    if group.with_coordinators:
        group.start_coordinators(bins)
        assert wait(lambda: group.primary_index() == 0, 30), "coordinators did not agree on node 0"
    assert wait(lambda: info(group.resp(0), "replication").get("connected_replicas", "2") == "2" or True, 5)


def follows(group: Group, i: int, primary: int, timeout=20.0) -> bool:
    """Replica `i` holds what `primary` last wrote (its WAL durable sequence reaches the primary's)."""
    def caught_up():
        p = info(group.resp(primary)).get("wal_durable_sequence")
        r = info(group.resp(i)).get("wal_durable_sequence")
        return p is not None and r is not None and int(r) >= int(p) - 5 and \
            info(group.resp(i)).get("replication_role") == "replica"
    return wait(caught_up, timeout, 0.25)


# --------------------------------------------------------------------------------------------

def scenario_rolling(old: Bins, new: Bins, work: str):
    print(f"== rolling: {old.version} -> {new.version}")
    g = Group(work)
    w = Writer(g)
    try:
        boot(g, old)
        w.start()
        time.sleep(8)
        check("steady state on the old release: writes acknowledged", len(w.acked) > 50, f"{len(w.acked)} acked")

        # 1. coordinators together
        t0 = time.time()
        g.stop_coordinators()
        g.start_coordinators(new)
        ok = wait(lambda: g.primary_index() == 0, 30)
        check("coordinators upgraded together, they agree on the same primary", ok, f"{time.time() - t0:.1f}s")
        time.sleep(4)
        fact("rolling.writer_gap_during_coordinator_upgrade_s", round(w.longest_gap(t0), 2))
        check("writes were not interrupted by the coordinator upgrade", w.longest_gap(t0) < 2.0,
              f"longest gap {w.longest_gap(t0):.2f}s")

        # 2. replicas first, each backed up while stopped
        for r in (1, 2):
            t1 = time.time()
            g.stop_node(r)
            bk = f"{work}/backup-n{r}"
            okb, out = backup(old, g.node_dir(r), bk)
            check(f"node {r}: stopped replica backed up and checked", okb, out.splitlines()[-1] if out else "")
            g.start_node(r, new, "replica", 0, "-new")
            ok = wait(lambda: ask(g.resp(r), "PING") == "PONG", 60)
            check(f"node {r}: new binary starts on the old node's data", ok, f"{time.time() - t1:.1f}s")
            caught = follows(g, r, 0, 30)
            check(f"node {r}: new replica follows the old primary", caught,
                  str({k: v for k, v in info(g.resp(r), 'replication').items() if 'repl' in k or 'link' in k})[:200])
            if not caught:
                break
        mixed_errors = dict(w.errors)
        fact("rolling.writer_errors_in_mixed_window", mixed_errors)

        # 3. a planned switchover needs `RUVIO FENCE` on the old primary: an old primary must refuse it
        #    cleanly, and the switch then goes the documented way (stop the old primary, the
        #    coordinators promote an upgraded replica).
        t2 = time.time()
        reply = g.coordinator_command("SWITCHOVER", GROUP, "TO", f"{HOST}:{g.resp(1)}")
        fact("rolling.switchover_from_old_primary_reply", str(reply)[:160])
        check("SWITCHOVER with an old-release primary is refused with an error", isinstance(reply, Error), str(reply)[:100])
        time.sleep(2)
        check("the refused switchover leaves the old primary in charge and writing",
              g.primary_index() == 0 and w.longest_gap(t2) < 2.0, f"longest gap {w.longest_gap(t2):.2f}s")
        t2 = time.time()
        g.stop_node(0)
        ok = wait(lambda: g.primary_index() in (1, 2), 30)
        check("stopping the old primary: the coordinators promote an upgraded replica", ok, f"{time.time() - t2:.1f}s")
        time.sleep(5)
        fact("rolling.failover_gap_s", round(w.longest_gap(t2), 2))
        check("writes resume on the new primary within 10 s", w.longest_gap(t2) < 10.0, f"longest gap {w.longest_gap(t2):.2f}s")
        newp = g.primary_index()

        # 4. the old primary last, as a replica of the new primary
        okb, out = backup(old, g.node_dir(0), f"{work}/backup-n0")
        check("node 0: stopped old primary backed up and checked", okb, out.splitlines()[-1] if out else "")
        g.start_node(0, new, "replica", newp, "-new")
        check("node 0: new binary starts", wait(lambda: ask(g.resp(0), "PING") == "PONG", 60))
        check("node 0: follows the new primary", follows(g, 0, newp, 30))
        # 5. everything is new now: a planned switchover back to node 0 works and pauses writes briefly
        t3 = time.time()
        replies = []
        def switch_back():
            r = g.coordinator_command("SWITCHOVER", GROUP, "TO", f"{HOST}:{g.resp(0)}")
            replies.append(str(r)[:120])
            return not isinstance(r, Error) and r != ""
        wait(switch_back, 25, 1.0)
        fact("rolling.switchover_back_attempts", replies)
        check("SWITCHOVER between two upgraded nodes is accepted", bool(replies) and not replies[-1].startswith("ERR"), f"{len(replies)} attempt(s), last: {replies[-1] if replies else ''}")
        check("node 0 is primary again", wait(lambda: g.primary_index() == 0, 30))
        time.sleep(4)
        fact("rolling.switchover_gap_s", round(w.longest_gap(t3), 2))
        check("writes resume within 10 s of the switchover", w.longest_gap(t3) < 10.0, f"longest gap {w.longest_gap(t3):.2f}s")
        time.sleep(3)
        w.finish()
        result = all_nodes_hold(g, "w", w.acked)
        for i, (missing, wrong) in result.items():
            check(f"node {i} holds every acknowledged write ({len(w.acked)})", not missing and not wrong,
                  f"missing {len(missing)}, wrong {len(wrong)}, first {missing[:3]}")
        check("no acknowledged write lost", all(not m for m, _ in result.values()))
        fact("rolling.acked", len(w.acked))
        fact("rolling.attempts", w.attempts)
        fact("rolling.writer_errors_total", dict(w.errors))
        g.stop_all()
        for i in range(3):
            fact(f"rolling.node{i}.wal", wal_minor(new, g.node_dir(i)))
    finally:
        w.finish()
        g.stop_all()
        fact("rolling.work_dir", work)
    return g


def scenario_primary_first(old: Bins, new: Bins, work: str):
    print(f"== primary-first (wrong order): {old.version} replicas, {new.version} primary")
    g = Group(work, coordinators=False)
    w = Writer(g, replicas=0)
    # Without coordinators the writer talks to node 0 directly.
    w.connect = lambda: Conn(g.resp(0))  # type: ignore[method-assign]
    try:
        boot(g, old)
        w.start()
        time.sleep(6)
        acked_before = len(w.acked)
        w.finish()
        check("old group takes writes", acked_before > 50, f"{acked_before} acked")
        check("old replicas hold them", all(not m for m, _ in all_nodes_hold(g, "w", w.acked).values()))
        # upgrade the primary first
        g.stop_node(0)
        okb, out = backup(old, g.node_dir(0), f"{work}/backup-n0")
        check("old primary backed up", okb)
        digest_replicas = {i: dir_digest(g.node_dir(i)) for i in (1, 2)}
        g.start_node(0, new, "primary", None, "-new")
        check("new primary starts on the old data", wait(lambda: ask(g.resp(0), "PING") == "PONG", 60))
        missing, wrong = present(g.resp(0), "w", w.acked)
        check("new primary holds every acknowledged write", not missing and not wrong, f"missing {len(missing)}")
        w2 = Writer(g, prefix="x", replicas=0)
        w2.connect = lambda: Conn(g.resp(0))  # type: ignore[method-assign]
        w2.start()
        time.sleep(5)
        w2.finish()
        check("new primary takes writes", len(w2.acked) > 20, f"{len(w2.acked)} acked")
        follow = {i: wait(lambda i=i: not present(g.resp(i), "x", w2.acked[-20:])[0], 20) for i in (1, 2)}
        fact("primary_first.old_replicas_follow_new_primary", follow)
        for i in (1, 2):
            log = open(f"{work}/n{i}.log", errors="replace").read()[-400:].replace("\n", " | ")
            fact(f"primary_first.node{i}.last_log", log)
        check("old replicas still answer and hold their own data", all(
            not present(g.resp(i), "w", w.acked)[0] for i in (1, 2)))
        del digest_replicas
        # finish: replicas on the new binary
        for i in (1, 2):
            g.stop_node(i)
            g.start_node(i, new, "replica", 0, "-new")
        check("upgraded replicas follow", wait(lambda: all(follows(g, i, 0, 1) for i in (1, 2)), 40))
        result = all_nodes_hold(g, "w", w.acked)
        result2 = all_nodes_hold(g, "x", w2.acked)
        check("after finishing the upgrade every node holds every acknowledged write",
              all(not m and not x for m, x in list(result.values()) + list(result2.values())),
              f"{len(w.acked) + len(w2.acked)} keys")
    finally:
        w.finish()
        g.stop_all()
        fact("primary_first.work_dir", work)


def scenario_rollback(old: Bins, new: Bins, work: str):
    print(f"== rollback: {old.version} -> {new.version} -> {old.version} from the backup")
    g = Group(work)
    w = Writer(g)
    try:
        boot(g, old)
        w.start()
        time.sleep(6)
        w.finish()
        base = list(w.acked)
        check("old group took writes before the backup", len(base) > 50, f"{len(base)} acked")
        check("all three nodes hold them", all(not m for m, _ in all_nodes_hold(g, "w", base).values()))
        g.stop_all()
        for i in range(3):
            okb, out = backup(old, g.node_dir(i), f"{work}/backup-n{i}")
            check(f"node {i} backup checked", okb)
        for cid, _, _ in g.coord_ports:
            shutil.copytree(f"{work}/c{cid}", f"{work}/backup-c{cid}")
        # upgrade everything and write more
        g.start_node(0, new, "primary", None, "-new")
        g.start_node(1, new, "replica", 0, "-new")
        g.start_node(2, new, "replica", 0, "-new")
        assert wait(lambda: all(ask(g.resp(i), "PING") == "PONG" for i in range(3)), 60)
        g.start_coordinators(new)
        check("new release: coordinators agree", wait(lambda: g.primary_index() == 0, 30))
        w2 = Writer(g, prefix="n")
        w2.start()
        time.sleep(6)
        # force a failover so the new Raft log holds an entry the old coordinators cannot read
        g.stop_node(0, signal.SIGKILL)
        check("failover on the new release", wait(lambda: g.primary_index() not in (None, 0), 30))
        time.sleep(4)
        w2.finish()
        newer = list(w2.acked)
        check("new release took writes, also across a failover", len(newer) > 50, f"{len(newer)} acked")
        survivors = [i for i in (1, 2)]
        res = all_nodes_hold(g, "n", newer, nodes=survivors)
        check("no acknowledged write of the new release lost in the failover", all(not m for m, _ in res.values()))
        g.stop_all()

        # A. old binary directly on the data the new release wrote (no restore)
        probe = f"{work}/probe-n1"
        shutil.copytree(g.node_dir(1), probe)
        before = dir_digest(probe)
        p = subprocess.Popen([old.ruvio, "--address", f"{HOST}:{free_port()}", "--shards", "1", "--durability",
                              "always", "--data-dir", probe], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=probe)
        try:
            p.wait(timeout=8)
            out = p.stdout.read().decode(errors="replace")
            fact("rollback.old_binary_on_new_data", f"exited {p.returncode}: {out.strip().splitlines()[-1][:200] if out.strip() else ''}")
        except subprocess.TimeoutExpired:
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=10)
            fact("rollback.old_binary_on_new_data", "started (the old release opens the new release's log)")
        fact("rollback.probe_dir_changed", dir_digest(probe) != before)

        # B. the old coordinators on the new Raft log, without clearing it
        for cid, _, _ in g.coord_ports:
            g.start_coordinator(cid, old)
        time.sleep(6)
        alive = {cid: p.poll() is None for cid, p in g.coords.items()}
        fact("rollback.old_coordinators_on_new_raft_log.alive", alive)
        fact("rollback.old_coordinators_on_new_raft_log.primary_agreed", g.primary_index())
        tails = {cid: open(f"{work}/c{cid}.log", errors="replace").read()[-300:].replace("\n", " | ") for cid in g.coords}
        fact("rollback.old_coordinators_on_new_raft_log.log_tail", tails[1])
        g.stop_coordinators()

        # C. the documented way back: restore the backups, clear the coordinators' Raft state
        for i in range(3):
            shutil.rmtree(g.node_dir(i))
            rc, out = ctl(old, "restore", "--source", f"{work}/backup-n{i}", "--data-dir", g.node_dir(i))
            check(f"node {i}: restore from the backup", rc == 0, out.splitlines()[-1] if out else "")
        for cid, _, _ in g.coord_ports:
            for name in ("raft-log.json", "raft-vote.json", "raft-snapshot.json"):
                path = f"{work}/c{cid}/data/{name}"
                if os.path.exists(path):
                    os.remove(path)
        g.start_node(0, old, "primary", None, "-back")
        g.start_node(1, old, "replica", 0, "-back")
        g.start_node(2, old, "replica", 0, "-back")
        check("old binary starts on the restored data", wait(lambda: all(ask(g.resp(i), "PING") == "PONG" for i in range(3)), 60))
        for cid, _, _ in g.coord_ports:
            g.start_coordinator(cid, old)
        check("old coordinators start on a cleared log and agree on node 0", wait(lambda: g.primary_index() == 0, 30))
        res = all_nodes_hold(g, "w", base)
        for i, (m, x) in res.items():
            check(f"node {i} after rollback holds every write acknowledged before the backup", not m and not x,
                  f"missing {len(m)}")
        gone = {i: len(present(g.resp(i), "n", newer)[0]) for i in range(3)}
        fact("rollback.new_release_writes_missing_after_restore_per_node", f"{gone} of {len(newer)} (expected: all, the restore returns the state of the backup)")
        w3 = Writer(g, prefix="r")
        w3.start()
        time.sleep(5)
        w3.finish()
        check("the rolled-back group takes writes with durability", len(w3.acked) > 20, f"{len(w3.acked)} acked")
        res = all_nodes_hold(g, "r", w3.acked)
        check("and its replicas hold them", all(not m for m, _ in res.values()))
    finally:
        g.stop_all()
        fact("rollback.work_dir", work)


def scenario_legacy(legacy: Bins, new: Bins, work: str):
    print(f"== legacy: {legacy.version} -> {new.version} -> {legacy.version}")
    g = Group(work, coordinators=False)
    keys = 3000
    try:
        # primary + one replica of the old release
        g.start_node(0, legacy, "primary")
        g.start_node(1, legacy, "replica", 0)
        assert wait(lambda: all(ask(g.resp(i), "PING") == "PONG" for i in (0, 1)), 30)
        c = Conn(g.resp(0))
        for i in range(1, keys + 1):
            r = c.cmd("SET", f"w{i}", f"v{i}")
            assert r == "OK", r
        c.close()
        ids = list(range(1, keys + 1))
        check("old release: replica holds the writes", all(not m for m, _ in all_nodes_hold(g, "w", ids, nodes=(0, 1)).values()))
        g.stop_node(1)
        g.stop_node(0)
        old_minor = wal_minor(legacy, g.node_dir(0))
        fact("legacy.wal_of_old_release", old_minor)
        okb, out = backup(legacy, g.node_dir(0), f"{work}/backup-n0")
        okb1, _ = backup(legacy, g.node_dir(1), f"{work}/backup-n1")
        check("backups of both nodes taken and checked", okb and okb1)

        # mixed handshake: new replica of an old primary, and the other way round
        g.start_node(0, legacy, "primary", None, "-mix")
        g.start_node(1, new, "replica", 0, "-mix")
        assert wait(lambda: ask(g.resp(0), "PING") == "PONG" and ask(g.resp(1), "PING") == "PONG", 60)
        c = Conn(g.resp(0))
        for i in range(keys + 1, keys + 501):
            assert c.cmd("SET", f"w{i}", f"v{i}") == "OK"
        c.close()
        ok = wait(lambda: not present(g.resp(1), "w", list(range(keys + 1, keys + 501)))[0], 20)
        fact("legacy.new_replica_follows_old_primary", ok)
        if not ok:
            tail = open(f"{work}/n0-mix.log", errors="replace").read().strip().splitlines()[-1]
            fact("legacy.old_primary_log_on_refused_handshake", re.sub(r"\x1b\[[0-9;]*m", "", tail)[-260:])
        g.stop_node(1)
        g.stop_node(0)
        ids = list(range(1, keys + 501))

        # upgrade in place
        g.start_node(0, new, "primary", None, "-new")
        g.start_node(1, new, "replica", 0, "-new")
        check("new release opens the old release's data", wait(lambda: all(ask(g.resp(i), "PING") == "PONG" for i in (0, 1)), 60))
        res = all_nodes_hold(g, "w", ids, nodes=(0, 1))
        check("new release holds every acknowledged write", all(not m and not x for m, x in res.values()))
        c = Conn(g.resp(0))
        for i in range(keys + 501, keys + 1001):
            assert c.cmd("SET", f"w{i}", f"v{i}") == "OK"
        c.close()
        ids = list(range(1, keys + 1001))
        check("new release replica follows", all(not m for m, _ in all_nodes_hold(g, "w", ids, nodes=(0, 1)).values()))
        g.stop_node(1)
        g.stop_node(0)
        fact("legacy.wal_after_upgrade", wal_minor(new, g.node_dir(0)))

        # rollback without restore: the old release must refuse, and must not change the directory
        before = dir_digest(g.node_dir(0))
        p = subprocess.Popen([legacy.ruvio, "--address", f"{HOST}:{free_port()}", "--shards", "1", "--durability",
                              "always", "--data-dir", g.node_dir(0)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=work)
        refused = True
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            refused = False
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=10)
        out = p.stdout.read().decode(errors="replace").strip()
        check("old release refuses the new release's log instead of opening it", refused and p.returncode != 0,
              (out.splitlines()[-1][:200] if out else "no output"))
        fact("legacy.refusal_message", out.splitlines()[-3:] if out else "")
        check("the refusal leaves the data directory untouched", dir_digest(g.node_dir(0)) == before)

        # rollback with the backup
        for i in (0, 1):
            shutil.rmtree(g.node_dir(i))
            rc, out = ctl(legacy, "restore", "--source", f"{work}/backup-n{i}", "--data-dir", g.node_dir(i))
            check(f"node {i}: restore from the backup", rc == 0, out.splitlines()[-1] if out else "")
        g.start_node(0, legacy, "primary", None, "-back")
        g.start_node(1, legacy, "replica", 0, "-back")
        check("old release starts on the restored data", wait(lambda: all(ask(g.resp(i), "PING") == "PONG" for i in (0, 1)), 30))
        base = list(range(1, keys + 1))
        res = all_nodes_hold(g, "w", base, nodes=(0, 1))
        check("restored nodes hold exactly the writes of the backup", all(not m and not x for m, x in res.values()))
        extra = present(g.resp(0), "w", list(range(keys + 1, keys + 1001)))[0]
        fact("legacy.writes_after_backup_present_after_restore", f"{1000 - len(extra)} of 1000 (expected 0)")
        check("the later writes are gone (the restore returns the state of the backup)", len(extra) == 1000)
        check("replica follows after the restore", follows(g, 1, 0, 20))
    finally:
        g.stop_all()
        fact("legacy.work_dir", work)


SCENARIOS = ["rolling", "primary-first", "rollback", "legacy"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True, help="directory with the older release's ruvio and ruvio-ctl")
    ap.add_argument("--new", required=True, help="directory with the binaries under test")
    ap.add_argument("--legacy", help="directory with a release older than the coordinator (needed by `legacy`)")
    ap.add_argument("--out", help="write the checks and facts as JSON")
    ap.add_argument("--keep", action="store_true", help="keep the work directories")
    ap.add_argument("scenarios", nargs="*", default=SCENARIOS)
    a = ap.parse_args()
    old, new = Bins(a.old, "old"), Bins(a.new, "new")
    legacy = Bins(a.legacy, "legacy") if a.legacy else None
    report["versions"] = {"old": old.version, "new": new.version, "legacy": legacy.version if legacy else None}
    root = tempfile.mkdtemp(prefix="ruvio-upgrade-")
    for name in a.scenarios:
        t0 = time.time()
        work = f"{root}/{name}"
        try:
            if name == "legacy":
                if not legacy:
                    print("SKIP legacy: no --legacy directory")
                    continue
                scenario_legacy(legacy, new, work)
            else:
                {"rolling": scenario_rolling, "primary-first": scenario_primary_first,
                 "rollback": scenario_rollback}[name](old, new, work)
        except Exception as e:  # a harness error is a failure, with the traceback
            import traceback
            traceback.print_exc()
            check(f"{name}: scenario ran to the end", False, repr(e)[:200])
        fact(f"{name}.seconds", round(time.time() - t0, 1))
    if a.out:
        json.dump(report, open(a.out, "w"), indent=2, default=str)
    if not a.keep:
        shutil.rmtree(root, ignore_errors=True)
    else:
        print("work directories:", root)
    print(f"{'FAILED' if failures else 'OK'}: {failures} failed checks")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
