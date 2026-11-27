#!/usr/bin/env python3
"""Six clients write and read through two failovers, with TLS off and on.

    scripts/client_faults.py target/debug/ruvio [--tls off|on|both] [--clients rust,go,...]
                             [--recovery-budget 5] [--hold 4]

For each TLS mode and each client the script starts three data nodes (one primary, two replicas)
and three coordinators on free ports, then runs the client's `fault_probe` (see `PROBES`), which
writes `fault:<client>:<i>` = `i` with durability replicas 1 and reads every key back. After a
warm-up the primary is killed with SIGKILL; the coordinators promote a replica, the killed node
comes back as a replica, and the new primary is killed too (the second failover). The probe uses
the seed list only, so `follow_primary` (the default) has to find each new primary.

Checks per client and TLS mode, from what the probe printed and from the servers afterwards:
no acknowledged write is missing on the final primary, no read returned another value, the first
acknowledged write after each kill came within `--recovery-budget` seconds, the probe stopped
when asked (no hang), and its failed attempts are listed by error class. TLS runs use a
throwaway CA from `scripts/make_test_tls.sh` and `node_tls` on the coordinators.

Before the run, build every probe once (the commands are in each probe's header); a client whose
probe is not built is reported as skipped, not as passed. Ports are free ports, nothing is fixed.
A run is about 30 seconds per client and mode.
"""

from __future__ import annotations

import argparse
import collections
import os
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _ruvio import encode, free_port as any_free_port, read_response  # noqa: E402

import socket  # noqa: E402

def free_port() -> int:
    """A free port below 50000: the server also reserves the port 10000 above its own, which must fit."""
    while True:
        port = any_free_port()
        if port < 50000:
            return port


ROOT = Path(__file__).resolve().parent.parent
CLIENTS_DIR = ROOT / "clients"
PROBE_DIR = Path(os.environ.get("RUVIO_PROBE_DIR", "/tmp/p"))


def probe_commands() -> dict[str, list[str]]:
    """The command of each client's probe; a client whose build is missing is left out."""
    candidates = {
        "rust": [str(ROOT / "target" / "debug" / "examples" / "fault_probe")],
        "go": [str(PROBE_DIR / "faultprobe")],
        "python": [sys.executable, "-I", str(CLIENTS_DIR / "python" / "benchmarks" / "fault_probe.py")],
        "node": ["node", str(CLIENTS_DIR / "node" / "build" / "bench" / "fault-probe.js")],
        "java": ["java", "-cp", str(PROBE_DIR / "java"), "io.github.salihcantekin.ruvio.FaultProbe"],
        "csharp": ["dotnet", str(PROBE_DIR / "cs" / "Ruvio.Client.FaultProbe.dll")],
    }
    found = {}
    for name, command in candidates.items():
        built = Path(command[-1]) if name in ("rust", "go", "python", "node", "csharp") else PROBE_DIR / "java"
        if name == "rust" and not built.exists():
            built = ROOT / "clients" / "ruvio-client" / "target" / "debug" / "examples" / "fault_probe"
            command = [str(built)]
        if built.exists():
            found[name] = command
    return found


class Cluster:
    """Three data nodes and three coordinators, TLS on the clients' port when `ca` is set."""

    def __init__(self, binary: str, work: Path, tls: dict[str, Path] | None):
        self.binary, self.work, self.tls = binary, work, tls
        self.nodes = [(free_port(), free_port()) for _ in range(3)]
        self.coords = [(i + 1, free_port()) for i in range(3)]
        self.node_procs: dict[int, subprocess.Popen] = {}
        self.coord_procs: dict[int, subprocess.Popen] = {}
        self.logs: list[Path] = []

    def redis_cli(self, port: int, *args: str, timeout: float = 3.0) -> str:
        command = ["redis-cli", "-p", str(port)]
        if self.tls:
            command += ["--tls", "--cacert", str(self.tls["ca"])]
        try:
            return subprocess.run(command + list(args), capture_output=True, text=True, timeout=timeout).stdout.strip()
        except subprocess.TimeoutExpired:
            return ""

    def coordinator_cli(self, port: int, *args: str) -> str:
        try:
            return subprocess.run(["redis-cli", "-p", str(port), *args], capture_output=True, text=True, timeout=3).stdout.strip()
        except subprocess.TimeoutExpired:
            return ""

    def start_node(self, i: int, role: str, primary: int | None) -> None:
        resp, repl = self.nodes[i]
        directory = self.work / f"n{i}"
        directory.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, RUVIO_REPLICATION_ROLE=role, RUVIO_REPLICATION_LISTEN=f"127.0.0.1:{repl}")
        for name in [key for key in env if key.startswith("RUVIO_") and key.startswith(("RUVIO_TLS", "RUVIO_SEEDS"))]:
            del env[name]
        if primary is not None:
            env["RUVIO_REPLICATION_PRIMARY"] = f"127.0.0.1:{primary}"
        if self.tls:
            env["RUVIO_TLS_CERT"], env["RUVIO_TLS_KEY"] = str(self.tls["cert"]), str(self.tls["key"])
        log = self.work / f"n{i}.log"
        self.logs.append(log)
        self.node_procs[i] = subprocess.Popen(
            [self.binary, "--address", f"127.0.0.1:{resp}", "--shards", "1", "--durability", "always",
             "--data-dir", str(directory)],
            env=env, stdout=open(log, "ab"), stderr=subprocess.STDOUT)

    def start_coordinator(self, cid: int, port: int) -> None:
        directory = self.work / f"c{cid}"
        directory.mkdir(parents=True, exist_ok=True)
        members = ",\n".join(
            f'  {{ address = "127.0.0.1:{resp}", replication = "127.0.0.1:{repl}" }}' for resp, repl in self.nodes)
        peers = ", ".join(f'"{i}@127.0.0.1:{p}"' for i, p in self.coords)
        tls = f'node_tls = true\ntls_ca = "{self.tls["ca"]}"\n' if self.tls else ""
        (directory / "c.toml").write_text(
            f'id = {cid}\nlisten = "127.0.0.1:{port}"\ndata_dir = "{directory}/data"\npeers = [{peers}]\n'
            f'ping_interval_ms = 200\ndown_after_ms = 1000\n{tls}\n[[groups]]\nname = "main"\nmembers = [\n{members}\n]\n')
        log = self.work / f"c{cid}.log"
        self.logs.append(log)
        self.coord_procs[cid] = subprocess.Popen(
            [self.binary, "coordinator", "--config", str(directory / "c.toml")],
            stdout=open(log, "ab"), stderr=subprocess.STDOUT)

    def primary_index(self, port: int) -> int | None:
        answer = self.coordinator_cli(port, "SENTINEL", "get-master-addr-by-name", "main").split()
        if len(answer) != 2:
            return None
        for i, (resp, _) in enumerate(self.nodes):
            if int(answer[1]) == resp:
                return i
        return None

    def agreed_primary(self) -> int | None:
        views = {self.primary_index(port) for _, port in self.coords}
        return views.pop() if len(views) == 1 else None

    def start(self) -> None:
        self.start_node(0, "primary", None)
        self.start_node(1, "replica", self.nodes[0][1])
        self.start_node(2, "replica", self.nodes[0][1])
        require(lambda: all(self.redis_cli(resp, "PING") == "PONG" for resp, _ in self.nodes), 20, "nodes did not answer PING")
        for cid, port in self.coords:
            self.start_coordinator(cid, port)
        require(lambda: self.agreed_primary() == 0, 20, "coordinators did not agree on the first primary")

    def stop(self) -> None:
        processes = list(self.node_procs.values()) + list(self.coord_procs.values())
        for process in processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def kill(self, i: int) -> None:
        self.node_procs[i].send_signal(signal.SIGKILL)
        self.node_procs[i].wait()


def require(condition, seconds: float, message: str) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except Exception:  # noqa: BLE001 - the condition is polled until the deadline
            pass
        time.sleep(0.05)
    raise RuntimeError(message)


def read_all(cluster: Cluster, port: int, name: str, last: int) -> list[bytes | None]:
    """GET fault:<name>:1..last in one pipeline from one node."""
    raw = socket.create_connection(("127.0.0.1", port), timeout=10)
    if cluster.tls:
        context = ssl.create_default_context(cafile=str(cluster.tls["ca"]))
        raw = context.wrap_socket(raw, server_hostname="127.0.0.1")
    with raw:
        stream = raw.makefile("rwb")
        stream.write(b"".join(encode(b"GET", f"fault:{name}:{i}".encode()) for i in range(1, last + 1)))
        stream.flush()
        values = []
        for _ in range(last):
            kind, payload = read_response(stream)
            values.append(payload if kind == b"$" else None)
        return values


class Probe:
    """A running probe: its lines with the time each arrived."""

    def __init__(self, command: list[str], env: dict[str, str]):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, env=env)
        self.lines: list[tuple[float, str]] = []
        self.lock = threading.Lock()
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self) -> None:
        for line in self.process.stdout:
            with self.lock:
                self.lines.append((time.monotonic(), line.strip()))

    def snapshot(self) -> list[tuple[float, str]]:
        with self.lock:
            return list(self.lines)

    def has(self, word: str) -> bool:
        return any(text.split(" ")[0] == word for _, text in self.snapshot())

    def acks_after(self, moment: float) -> float | None:
        for at, text in self.snapshot():
            if at >= moment and text.startswith("ack "):
                return at
        return None


def run_one(binary: str, client: str, command: list[str], tls_mode: str, certs: dict[str, Path] | None,
            budget: float, hold: float) -> tuple[bool, str]:
    work = Path(tempfile.mkdtemp(prefix=f"ruvio-client-faults-{client}-"))
    cluster = Cluster(binary, work, certs if tls_mode == "on" else None)
    probe = None
    problems: list[str] = []
    try:
        cluster.start()
        env = dict(os.environ, RUVIO_SEEDS=",".join(f"127.0.0.1:{resp}" for resp, _ in cluster.nodes),
                   RUVIO_PROBE_NAME=client)
        for name in [key for key in env if key.startswith("RUVIO_") and key not in ("RUVIO_SEEDS", "RUVIO_PROBE_NAME")]:
            del env[name]
        if cluster.tls:
            env["RUVIO_PROBE_CA"] = str(cluster.tls["ca"])
        probe = Probe(command, env)
        require(lambda: probe.has("ack"), 30, "the probe wrote nothing before the first fault")
        time.sleep(hold)

        kills: list[tuple[float, float | None]] = []
        primary = 0
        for round_number in range(2):
            killed_at = time.monotonic()
            cluster.kill(primary)
            require(lambda: cluster.agreed_primary() not in (None, primary), 20, "no new primary")
            new_primary = cluster.agreed_primary()
            require(lambda: probe.acks_after(killed_at + 0.001) is not None, 30,
                    f"the {client} client wrote nothing after the kill #{round_number + 1}")
            kills.append((killed_at, probe.acks_after(killed_at + 0.001)))
            # The killed node returns as a replica of the new primary.
            cluster.start_node(primary, "replica", cluster.nodes[new_primary][1])
            require(lambda: cluster.redis_cli(cluster.nodes[primary][0], "PING") == "PONG", 20, "node did not come back")
            time.sleep(hold)
            primary = new_primary

        probe.process.stdin.write("stop\n")
        probe.process.stdin.flush()
        try:
            probe.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            problems.append("the probe did not stop within 15 s (hang)")
            probe.process.kill()
        lines = probe.snapshot()
        acked = sorted(int(text.split()[1]) for _, text in lines if text.startswith("ack "))
        errors = collections.Counter(text.split()[2] if len(text.split()) > 2 else "?" for _, text in lines if text.startswith("err "))
        bad = [text for _, text in lines if text.startswith("bad ")]
        if not acked or acked != list(range(1, len(acked) + 1)):
            problems.append("acks are not 1..N in order")
        # Wait until the final primary holds everything the replicas acknowledged, then compare.
        final = cluster.agreed_primary()
        values = read_all(cluster, cluster.nodes[final][0], client, len(acked)) if final is not None else []
        missing = [i for i, value in enumerate(values, 1) if value != str(i).encode()]
        if final is None or missing:
            problems.append(f"{len(missing)} acknowledged writes missing on the final primary (first: {missing[:3]})")
        if bad:
            problems.append(f"{len(bad)} reads returned another value (first: {bad[0]})")
        recoveries = [None if after is None else after - at for at, after in kills]
        for number, recovery in enumerate(recoveries, 1):
            if recovery is None or recovery > budget:
                problems.append(f"failover #{number}: first write acknowledged after "
                                f"{'never' if recovery is None else f'{recovery:.2f} s'} (budget {budget} s)")
        ack_times = [at for at, text in lines if text.startswith("ack ")]
        gaps = [b - a for a, b in zip(ack_times, ack_times[1:])]
        summary = (f"{len(acked)} acked, {sum(errors.values())} failed attempts {dict(errors) or ''}, "
                   f"first write after kill: {', '.join(f'{r:.2f} s' for r in recoveries if r is not None)}, "
                   f"longest gap between acks {max(gaps, default=0):.2f} s")
        return not problems, summary + ("" if not problems else " | " + "; ".join(problems))
    except Exception as error:  # noqa: BLE001 - reported as a failure of this client
        return False, f"{type(error).__name__}: {error}" + ("".join(f" | {p}" for p in problems))
    finally:
        if probe is not None and probe.process.poll() is None:
            probe.process.kill()
        cluster.stop()
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary")
    parser.add_argument("--tls", choices=["off", "on", "both"], default="both")
    parser.add_argument("--clients", default="rust,csharp,go,java,python,node")
    parser.add_argument("--recovery-budget", type=float, default=5.0)
    parser.add_argument("--hold", type=float, default=4.0, help="seconds of load before and between faults")
    args = parser.parse_args()

    commands = probe_commands()
    requested = [name for name in args.clients.split(",") if name]
    modes = ["off", "on"] if args.tls == "both" else [args.tls]
    certs_dir = Path(tempfile.mkdtemp(prefix="ruvio-client-faults-tls-"))
    subprocess.run([str(ROOT / "scripts" / "make_test_tls.sh"), str(certs_dir)], check=True)
    certs = {"ca": certs_dir / "ca.pem", "cert": certs_dir / "server.pem", "key": certs_dir / "server.key"}
    failures = skipped = 0
    try:
        for mode in modes:
            for client in requested:
                if client not in commands:
                    print(f"SKIP  tls={mode:<3} {client:<6} probe not built")
                    skipped += 1
                    continue
                ok, summary = run_one(args.binary, client, commands[client], mode, certs, args.recovery_budget, args.hold)
                print(f"{'PASS' if ok else 'FAIL'}  tls={mode:<3} {client:<6} {summary}", flush=True)
                failures += 0 if ok else 1
    finally:
        shutil.rmtree(certs_dir, ignore_errors=True)
    print(f"client faults: {failures} failed, {skipped} skipped")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
