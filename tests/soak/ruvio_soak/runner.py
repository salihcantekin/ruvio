"""Runs one plan: start the cluster, load with history, inject faults on a schedule, then stop
the load, heal everything, read every key and check the history.

A plan is JSON:
  name, duration_s (load and faults), warmup_s, seed,
  cluster: overrides of `Cluster` (ping_interval_ms, down_after_ms, primary_lease_ms, groups),
  load: see `workload.start`,
  faults: {interval_s: [min, max], down_s: [min, max], kinds: [...], netem: "delay 100ms ...",
           recovery_timeout_s, sequence: [{kind, group, host, down}, ...] run first, in order},
  metrics_interval_s, status_interval_s, artifact_interval_s, rss_min_window_s,
  rss_warn_percent_per_hour.

Every `artifact_interval_s` (default 600) the events and metrics go to Blob gzipped, and before
the history check the whole history does too, so a controller that dies or a VM deleted early
never takes the run's evidence with it.

`restart_as`: "replica" (default) or "configured", see `Runner.start`.

Fault kinds: kill-primary, kill-replica, stall-replica, kill-coordinator-leader, kill-host,
isolate-host, netem. A kind the backend cannot inject is recorded as skipped."""

from __future__ import annotations

import gzip
import io
import json
import random
import signal
import threading
import time
from pathlib import Path

from . import history as checker
from . import workload
from .backends import Unsupported
from .cluster import COORDINATOR, Cluster
from .metrics import Collector, node_info
from .resp import ReplyError, Unknown, call_once, key_slot
from .status import Status


def probe_key(slots: str) -> str:
    """A key in the group's slot range, for the health check's write."""
    first, last = (int(x) for x in slots.split("-"))
    for n in range(100000):
        key = f"soak:probe:{n}"
        if first <= key_slot(key) <= last:
            return key
    raise ValueError(f"no probe key for slots {slots}")


class Runner:
    def __init__(self, plan: dict, cluster: Cluster, backend, directory: Path, run_id: str, uploader=None, log=print):
        self.plan, self.cluster, self.backend, self.directory = plan, cluster, backend, directory
        self.run_id, self.uploader, self.log = run_id, uploader, log
        self.random = random.Random(plan.get("seed", 1))
        self.down: set[tuple[int, str]] = set()
        self.status = Status(directory, run_id, plan, backend.name, uploader)
        self.events = open(directory / "events.jsonl", "a", buffering=1)
        self.stop_load = threading.Event()
        self.stop_metrics = threading.Event()
        faults = plan.get("faults", {})
        self.recovery_timeout = float(faults.get("recovery_timeout_s", 60))

    # --- cluster state -------------------------------------------------------------------
    def event(self, name: str, **fields) -> None:
        # `mono` is the clock of the history file's `t`, so events and operations line up.
        entry = {"t": round(time.time(), 3), "mono": time.monotonic_ns(), "event": name, **fields}
        self.events.write(json.dumps(entry) + "\n")
        self.log(f"[{time.strftime('%H:%M:%S')}] {name} {fields if fields else ''}")

    def coordinator_primary(self, coordinator: int, group: str) -> int | None:
        try:
            reply = call_once(
                self.cluster.address(coordinator, COORDINATOR),
                "SENTINEL", "get-master-addr-by-name", group,
                password=self.cluster.secret,
            )
        except (OSError, ReplyError, Unknown):
            return None
        if not reply or len(reply) != 2:
            return None
        return self.cluster.host_of(group, f"{reply[0].decode()}:{reply[1].decode()}")

    def primary(self, group: str) -> int | None:
        """The primary every answering coordinator names, if they agree."""
        named = {self.coordinator_primary(c, group) for c in range(len(self.cluster.hosts)) if (c, COORDINATOR) not in self.down}
        named.discard(None)
        return named.pop() if len(named) == 1 else None

    def leader(self) -> int | None:
        leaders = []
        for c in range(len(self.cluster.hosts)):
            info = node_info(self.cluster, c, COORDINATOR)
            if info and info.get("is_leader") == "1":
                leaders.append(c)
        return leaders[0] if len(leaders) == 1 else None

    def healthy(self) -> str | None:
        """None when every process runs, one coordinator leads, the coordinators agree on each
        group's primary, that member reports `replication_role:primary` and every other member
        `replica`. Otherwise the first thing that is not so."""
        for host, role in self.cluster.processes():
            if not self.backend.running(host, role):
                return f"host{host} {role} not running"
        if self.leader() is None:
            return "no single coordinator leader"
        for g in self.cluster.groups:
            primary = self.primary(g.name)
            if primary is None:
                return f"coordinators do not agree on {g.name}'s primary"
            owner = self.cluster.address(primary, g.name)
            first_slot = int(g.slots.split("-")[0])
            for host in range(len(self.cluster.hosts)):
                info = node_info(self.cluster, host, g.name)
                role = info.get("replication_role") if info else None
                if role != ("primary" if host == primary else "replica"):
                    return f"host{host} {g.name} reports role {role}"
                advertised = self.slot_owner(host, g.name, first_slot)
                if advertised != owner:
                    return f"host{host} {g.name} names {advertised} for slot {first_slot}, the coordinators {owner}"
            try:
                reply = call_once(owner, "SET", probe_key(g.slots), "1", password=self.cluster.password)
            except (OSError, ReplyError, Unknown) as error:
                reply = str(error)
            if reply != "OK":
                return f"{g.name}'s primary {owner} refuses a write: {reply}"
        return None

    def slot_owner(self, host: int, group: str, slot: int) -> str | None:
        """The primary that member `host` of `group` names for `slot` in `CLUSTER SLOTS`."""
        try:
            ranges = call_once(self.cluster.address(host, group), "CLUSTER", "SLOTS", password=self.cluster.password)
        except (OSError, ReplyError, Unknown):
            return None
        for entry in ranges or []:
            if entry[0] <= slot <= entry[1]:
                return f"{entry[2][0].decode()}:{entry[2][1]}"
        return None

    def wait_healthy(self, timeout: float) -> tuple[bool, float, str]:
        start = time.monotonic()
        reason = "not checked"
        while time.monotonic() - start < timeout:
            reason = self.healthy()
            if reason is None:
                return True, time.monotonic() - start, ""
            time.sleep(0.25)
        return False, time.monotonic() - start, reason

    def start(self, host: int, role: str, restart: bool = True) -> None:
        """Starts a process. With the plan's `restart_as` = "replica" (the default), a data node
        that comes back starts as a replica of its group's agreed primary, as an operator would
        start it after a failover; "configured" starts it in the role its first start had (a
        restarted first primary then starts as a primary)."""
        follow = None
        if restart and role != COORDINATOR and self.plan.get("restart_as", "replica") == "replica":
            primary = None
            deadline = time.monotonic() + 10
            while primary is None and time.monotonic() < deadline:
                primary = self.primary(role)
                if primary is None:
                    time.sleep(0.2)
            if primary is not None and primary != host:
                follow = self.cluster.replication_address(primary, role)
        self.backend.start(host, role, follow)
        self.down.discard((host, role))
        self.collector.restarted(host, role)

    def check_processes(self) -> None:
        """A process that is not running and was not stopped on purpose exited by itself."""
        for host, role in self.cluster.processes():
            if (host, role) not in self.down and not self.backend.running(host, role):
                self.status.problem(f"host{host} {role} exited unexpectedly")
                self.event("unexpected-exit", host=host, role=role)
                self.start(host, role)

    # --- faults ----------------------------------------------------------------------------
    def inject(self, kind: str, group: str | None = None, host: int | None = None, down: float | None = None) -> bool:
        """Injects one fault and heals it; returns False when the backend cannot. `group`,
        `host` and `down` are chosen at random unless given (a plan's `sequence`)."""
        if not self.backend.supports(kind):
            self.event("fault-skipped", kind=kind, reason="backend")
            self.status.fault(kind, injected=False)
            return False
        faults = self.plan.get("faults", {})
        low, high = faults.get("down_s", [2, 8])
        down = self.random.uniform(low, high) if down is None else float(down)
        hosts = list(range(len(self.cluster.hosts)))
        chosen = host
        group = group or self.random.choice(self.cluster.groups).name
        killed: list[tuple[int, str]] = []
        stalled: list[tuple[int, str]] = []
        network_host = None
        if kind in ("kill-primary", "kill-replica", "stall-replica"):
            primary = self.primary(group)
            if primary is None:
                self.event("fault-skipped", kind=kind, reason=f"no agreed primary for {group}")
                self.status.fault(kind, injected=False)
                return False
            if kind == "kill-primary":
                host = primary
            elif chosen is not None and chosen != primary:
                host = chosen
            else:
                host = self.random.choice([h for h in hosts if h != primary])
            (stalled if kind == "stall-replica" else killed).append((host, group))
        elif kind == "kill-coordinator-leader":
            leader = self.leader()
            if leader is None:
                self.event("fault-skipped", kind=kind, reason="no single leader")
                self.status.fault(kind, injected=False)
                return False
            killed.append((leader, COORDINATOR))
        elif kind == "kill-host":
            host = self.random.choice(hosts) if chosen is None else chosen
            killed.extend((host, role) for role in self.cluster.roles())
        elif kind in ("isolate-host", "netem"):
            network_host = self.random.choice(hosts) if chosen is None else chosen
        else:
            raise ValueError(f"unknown fault kind {kind}")

        watchdog = int(down + self.recovery_timeout + 60)
        try:
            self.event("fault", kind=kind, group=group, killed=killed, stalled=stalled, host=network_host, down_s=round(down, 2))
            for host, role in killed:
                self.down.add((host, role))
                self.backend.signal(host, role, signal.SIGKILL)
            for host, role in stalled:
                self.backend.signal(host, role, signal.SIGSTOP)
            if kind == "isolate-host":
                self.backend.isolate(network_host, watchdog)
            elif kind == "netem":
                self.backend.netem(network_host, faults.get("netem", "delay 100ms 20ms loss 2%"), watchdog)
        except Unsupported as error:
            self.event("fault-skipped", kind=kind, reason=str(error))
            self.status.fault(kind, injected=False)
            return False
        started = time.monotonic()
        self.stop_load.wait(down)
        for host, role in stalled:
            self.backend.signal(host, role, signal.SIGCONT)
        if network_host is not None:
            self.backend.heal(network_host)
        for host, role in killed:
            self.start(host, role)
        ok, took, reason = self.wait_healthy(self.recovery_timeout)
        recovery = time.monotonic() - started
        self.status.fault(kind, injected=True, recovery_s=recovery)
        self.event("healed" if ok else "not-healed", kind=kind, recovery_s=round(recovery, 2), reason=reason)
        if not ok:
            self.status.problem(f"{kind}: the cluster did not recover within {self.recovery_timeout:.0f}s after the heal ({reason})")
        return ok

    # --- the run ---------------------------------------------------------------------------
    def save_status(self, phase: str | None = None) -> None:
        if phase:
            self.status.data["phase"] = phase
        self.status.data["operations"] = dict(self.history.counts)
        window = float(self.plan.get("rss_min_window_s", 1800))
        slopes = self.collector.rss_slopes(window)
        charged = self.collector.charged_slopes(window)
        self.status.data["rss_slope_percent_per_hour"] = slopes
        self.status.data["charged_slope_percent_per_hour"] = charged
        limit = float(self.plan.get("rss_warn_percent_per_hour", 1.0))
        for name, slope in slopes.items():
            if slope > limit and self.status.data["phase"] in ("load", "final"):
                data = f", data {charged[name]:.2f}%/h" if name in charged else ""
                self.status.problem(f"{name}: RSS grows {slope:.2f}%/h{data}", severity="warn")
        self.status.save()

    def upload(self, *names: str) -> None:
        """Uploads run files to Blob, `.jsonl` gzipped. A copy is taken first, so the writers keep
        appending while it uploads."""
        if not self.uploader:
            return
        for name in names:
            path = self.directory / name
            if not path.exists():
                continue
            if name.endswith(".jsonl"):
                # Compressed in chunks: a long run's history is over a gigabyte uncompressed.
                buffer = io.BytesIO()
                with open(path, "rb") as source, gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6) as target:
                    while chunk := source.read(1 << 20):
                        target.write(chunk)
                self.uploader.put(name + ".gz", buffer.getvalue(), "application/gzip")
            else:
                self.uploader.put(name, path.read_bytes())

    def run(self) -> dict:
        plan = self.plan
        self.history = workload.History(self.directory / "history.jsonl")
        self.collector = Collector(self.cluster, self.backend, self.directory / "metrics.jsonl",
                                   float(plan.get("metrics_interval_s", 10)), self.stop_metrics)
        self.status.data["cluster"] = self.cluster.public()
        try:
            self.status.save()
            self.event("deploy")
            self.backend.deploy()
            for host, role in self.cluster.processes():
                if role != COORDINATOR:
                    self.start(host, role, restart=False)
            for host in range(len(self.cluster.hosts)):
                self.start(host, COORDINATOR, restart=False)
            ok, took, reason = self.wait_healthy(float(plan.get("startup_timeout_s", 90)))
            if not ok:
                self.status.problem(f"the cluster did not start: {reason}")
                self.save_status("failed")
                return self.status.data
            self.event("cluster-up", seconds=round(took, 2))
            self.collector.start()
            workers = workload.start(self.history, self.cluster.seeds(), self.cluster.password,
                                     plan.get("load", {}), self.stop_load, self.run_id.replace("-", ""))
            self.save_status("warmup")
            self.stop_load.wait(float(plan.get("warmup_s", 10)))
            self.save_status("load")
            self.fault_loop()
            self.stop_load.set()
            for worker in workers:
                worker.join(timeout=30)
            self.final()
        except KeyboardInterrupt:
            self.status.problem("interrupted", severity="warn")
            self.stop_load.set()
            self.save_status("interrupted")
        except Exception as error:
            self.status.problem(f"controller error: {error!r}")
            self.stop_load.set()
            self.save_status("failed")
            raise
        finally:
            self.stop_metrics.set()
            self.backend.stop_all()
            self.history.close()
            self.events.close()
        return self.status.data

    def fault_loop(self) -> None:
        plan = self.plan
        faults = plan.get("faults", {})
        kinds = faults.get("kinds", [])
        low, high = faults.get("interval_s", [60, 120])
        end = time.monotonic() + float(plan.get("duration_s", 60))
        status_every = float(plan.get("status_interval_s", 60))
        next_status = time.monotonic() + status_every
        artifacts_every = float(plan.get("artifact_interval_s", 600))
        next_artifacts = time.monotonic() + artifacts_every
        next_fault = time.monotonic() + self.random.uniform(low, high)
        sequence = list(faults.get("sequence", []))
        while time.monotonic() < end:
            self.check_processes()
            if (sequence or kinds) and time.monotonic() >= next_fault:
                step = sequence.pop(0) if sequence else {"kind": self.random.choice(kinds)}
                if not self.inject(**step) and self.status.data["verdict"] == "fail":
                    self.event("faults-stopped", reason="the cluster did not recover")
                    kinds, sequence = [], []
                next_fault = time.monotonic() + self.random.uniform(low, high)
            if time.monotonic() >= next_status:
                self.history.flush()
                self.save_status()
                next_status = time.monotonic() + status_every
            if time.monotonic() >= next_artifacts:
                self.events.flush()
                self.upload("events.jsonl", "metrics.jsonl")
                next_artifacts = time.monotonic() + artifacts_every
            time.sleep(0.5)

    def final(self) -> None:
        self.save_status("final")
        for host in range(len(self.cluster.hosts)):
            self.backend.heal(host)
        for host, role in list(self.down):
            self.start(host, role)
        ok, _, reason = self.wait_healthy(self.recovery_timeout)
        if not ok:
            self.status.problem(f"the cluster was not healthy before the final reads: {reason}")
        unread = workload.final_reads(self.history, self.cluster.seeds(), self.cluster.password)
        self.event("final-reads", unread=unread)
        self.events.flush()
        # The evidence goes out before the check, which is the slowest step and the one most
        # likely to run out of memory on a long history.
        self.save_status("uploading")
        self.upload("history.jsonl", "events.jsonl", "metrics.jsonl")
        self.save_status("checking")
        started = time.monotonic()
        try:
            result = checker.check_file(self.directory / "history.jsonl")
        except MemoryError:
            self.status.problem("the history check ran out of memory; run `run.py check` on the uploaded history")
            self.save_status("done")
            return
        self.event("checked", seconds=round(time.monotonic() - started, 1), operations=result["operations"])
        (self.directory / "check.json").write_text(json.dumps(result, indent=2))
        self.status.data["check"] = {k: result[k] for k in ("verdict", "problems", "operations")}
        self.status.data["check"]["profiles"] = result["profiles"]
        self.status.raise_verdict(result["verdict"])
        for problem in result["problems"]:
            self.status.problem(problem, severity=result["verdict"] if result["verdict"] != "ok" else "warn")
        self.upload("check.json", "events.jsonl")
        self.save_status("done")
