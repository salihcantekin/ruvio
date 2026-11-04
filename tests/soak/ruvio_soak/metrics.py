"""Samples every process every few seconds: `INFO` fields over RESP (every byte count the server
reports about its data, buffers and persistence) and the process memory from the backend (RSS,
peak, anonymous and file-backed parts, threads). Writes `metrics.jsonl` and keeps each process's
RSS and the server's own memory count since its last start, from which `rss_slopes` and
`charged_slopes` estimate a climb in percent per hour. RSS climbing with a flat charged memory
points at the allocator or a leak; both climbing together is the data growing."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from .cluster import COORDINATOR, Cluster
from .resp import Connection, ReplyError, Unknown, call_once, parse_info

NODE_FIELDS = (
    "replication_role",
    "replication_epoch",
    "replication_replicas",
    "replication_send_lag",
    "connected_clients",
    "keys",
    "total_commands_processed",
    "wal_unsynced_bytes",
    "wal_last_sequence",
    "wal_durable_sequence",
    "replication_committed_sequence",
    "replication_replica_sequence",
    "replication_fenced",
    "slave_repl_offset",
    "master_link_status",
    "master_last_io_seconds_ago",
    "primary_lease",
    "primary_lease_remaining_ms",
    "rejected_connections",
    "total_connections_received",
    "expired_keys",
    "evicted_keys",
    "snapshot_in_progress",
    "snapshot_aborts_total",
    "snapshot_overload_skips_total",
    "recovered_wal_records",
    "ruvio_health",
)
COORDINATOR_FIELDS = ("coordinator_id", "leader_id", "is_leader", "failovers_total", "last_failover_unix_ms")


_last_cpu: list[int] = []


def controller_stats() -> dict:
    """This machine's busy CPU share since the last call (all processes: the load, the
    background workers, the checker), load and free memory: a busy controller would make the
    cluster look slow."""
    stats: dict = {}
    try:
        fields = [int(x) for x in open("/proc/stat").readline().split()[1:]]
        idle, total = fields[3] + fields[4], sum(fields)
        if _last_cpu:
            busy = 1 - (idle - _last_cpu[0]) / max(1, total - _last_cpu[1])
            stats["cpu_busy"] = round(busy, 4)
        _last_cpu[:] = [idle, total]
        stats["load1"] = os.getloadavg()[0]
        for line in open("/proc/meminfo"):
            if line.startswith("MemAvailable:"):
                stats["mem_available_kib"] = int(line.split()[1])
    except (OSError, ValueError):
        pass
    return stats


def node_info(cluster: Cluster, host: int, role: str) -> dict[str, str] | None:
    password = cluster.secret if role == COORDINATOR else cluster.password
    try:
        return parse_info(call_once(cluster.address(host, role), "INFO", password=password))
    except (OSError, ReplyError, Unknown):
        return None


def ping_ms(cluster: Cluster, host: int, role: str, count: int = 5) -> float | None:
    """The fastest of `count` PINGs from this machine to the process, on one connection opened
    first: the network round trip plus the least the process takes to answer. A primary that
    answers slower than another (a farther host, a busy node) shows here."""
    password = cluster.secret if role == COORDINATOR else cluster.password
    host_name, port = cluster.address(host, role).rsplit(":", 1)
    try:
        connection = Connection(host_name, int(port), password, 2.0)
    except OSError:
        return None
    try:
        connection.call("PING")
        best = None
        for _ in range(count):
            started = time.perf_counter()
            connection.call("PING")
            elapsed = (time.perf_counter() - started) * 1000
            best = elapsed if best is None else min(best, elapsed)
        return round(best, 3)
    except (OSError, ReplyError, Unknown):
        return None
    finally:
        connection.close()


def pick(role: str, info: dict[str, str]) -> dict:
    if role == COORDINATOR:
        picked = {k: info[k] for k in COORDINATOR_FIELDS if k in info}
        picked.update({k: v for k, v in info.items() if k.startswith("group_")})
        return picked
    picked = {k: info[k] for k in NODE_FIELDS if k in info}
    picked.update({k: v for k, v in info.items() if ("memory" in k or k.endswith("_bytes")) and not k.endswith("_human")})
    return picked


class Collector(threading.Thread):
    """Besides the memory and the picked INFO fields every `interval`, each sample carries the
    backend's process stats (CPU ticks, open descriptors, data directory, WAL and checkpoint
    bytes), and `hosts.jsonl` gets each host's load, free memory and disk, network bytes and
    clock offset. Every `full_info_every`-th sample writes the whole `INFO` of every process to
    `info.jsonl`. `down()` names the processes stopped on purpose, so a sample that cannot reach
    any other process counts as a gap."""

    def __init__(self, cluster: Cluster, backend, path: Path, interval: float, stop: threading.Event,
                 full_info_every: int = 6, down=lambda: set()):
        super().__init__(name="metrics", daemon=True)
        self.cluster, self.backend, self.interval, self.stop = cluster, backend, interval, stop
        self.file = open(path, "a", buffering=1)
        self.hosts_file = open(path.with_name("hosts.jsonl"), "a", buffering=1)
        self.info_file = open(path.with_name("info.jsonl"), "a", buffering=1)
        self.full_info_every, self.down = max(1, full_info_every), down
        self.samples = 0
        self.gaps = 0  # process samples that got no INFO from a process not stopped on purpose
        self.expected = 0
        self.rss: dict[str, list[tuple[float, int]]] = {}
        self.anon: dict[str, list[tuple[float, int]]] = {}
        self.charged: dict[str, list[tuple[float, int]]] = {}
        self.lock = threading.Lock()

    def restarted(self, host: int, role: str) -> None:
        """Starts a new RSS series for a process that was started again."""
        with self.lock:
            for series in (self.rss, self.anon, self.charged):
                series.pop(f"host{host}-{role}", None)

    def sample(self) -> None:
        now = time.time()
        full = self.samples % self.full_info_every == 0
        self.samples += 1
        down = set(self.down())
        self.hosts_file.write(json.dumps({"t": round(now, 1), "host": "controller", **controller_stats()},
                                         separators=(",", ":")) + "\n")
        for host in range(len(self.cluster.hosts)):
            try:
                stats, machine = self.backend.stats(host)
            except Exception as error:  # a sample must never stop the collector
                stats, machine = {}, {"error": repr(error)[:200]}
            self.hosts_file.write(json.dumps({"t": round(now, 1), "host": host, **machine}, separators=(",", ":")) + "\n")
            for role in self.cluster.roles():
                name = f"host{host}-{role}"
                process = dict(stats.get(role, {}))
                if "rss_kib" not in process:
                    process.update(self.backend.memory(host, role) or {})
                rss = process.get("rss_kib")
                info = node_info(self.cluster, host, role)
                entry = {"t": round(now, 1), "process": name, "rss_kib": rss, "up": info is not None}
                entry.update(process)
                if info is not None:
                    entry.update(pick(role, info))
                    entry["ping_ms"] = ping_ms(self.cluster, host, role)
                    if full:
                        self.info_file.write(json.dumps({"t": round(now, 1), "process": name, "info": info},
                                                        separators=(",", ":")) + "\n")
                if (host, role) not in down:
                    self.expected += 1
                    if info is None:
                        self.gaps += 1
                        entry["gap"] = True
                self.file.write(json.dumps(entry, separators=(",", ":")) + "\n")
                charged = info.get("charged_memory_bytes") if info else None
                anon = process.get("rss_anon_kib")
                with self.lock:
                    if rss is not None:
                        self.rss.setdefault(name, []).append((now, rss))
                    if anon is not None:
                        self.anon.setdefault(name, []).append((now, anon))
                    if charged is not None and charged.isdigit():
                        self.charged.setdefault(name, []).append((now, int(charged)))

    def run(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            self.sample()
            self.stop.wait(max(0.0, self.interval - (time.monotonic() - started)))
        for file in (self.file, self.hosts_file, self.info_file):
            file.close()

    def anon_slopes_kib(self, min_window_s: float, skip_s: float = 900) -> dict[str, float]:
        """Anonymous memory slope in KiB per hour since each process's last start, leaving out
        its first `skip_s` (allocator warm-up). Absolute, because a percentage of a 6 MiB
        process makes warm-up look like a leak."""
        with self.lock:
            series = {name: list(points) for name, points in self.anon.items()}
        slopes = {}
        for name, points in series.items():
            if not points:
                continue
            points = [p for p in points if p[0] - points[0][0] >= skip_s]
            if len(points) < 3 or points[-1][0] - points[0][0] < min_window_s:
                continue
            slopes[name] = round(slope_per_hour(points), 1)
        return slopes

    def gap_fraction(self) -> float:
        return round(self.gaps / self.expected, 5) if self.expected else 0.0

    def rss_slopes(self, min_window_s: float) -> dict[str, float]:
        """Least-squares RSS slope in percent of the series' mean per hour, for each process
        whose current series spans at least `min_window_s` seconds."""
        return self._slopes(self.rss, min_window_s)

    def charged_slopes(self, min_window_s: float) -> dict[str, float]:
        """The same for the server's own count of its memory (`charged_memory_bytes`)."""
        return self._slopes(self.charged, min_window_s)

    def _slopes(self, source: dict[str, list[tuple[float, int]]], min_window_s: float) -> dict[str, float]:
        slopes = {}
        with self.lock:
            series = {name: list(points) for name, points in source.items()}
        for name, points in series.items():
            if len(points) < 3 or points[-1][0] - points[0][0] < min_window_s:
                continue
            slopes[name] = round(slope_percent_per_hour(points), 3)
        return slopes


def slope_per_hour(points: list[tuple[float, float]]) -> float:
    """Least-squares slope in units per hour."""
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    covariance = sum((t - mean_t) * (v - mean_v) for t, v in points)
    variance = sum((t - mean_t) ** 2 for t, _ in points)
    return covariance / variance * 3600 if variance else 0.0


def slope_percent_per_hour(points: list[tuple[float, int]]) -> float:
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    covariance = sum((t - mean_t) * (v - mean_v) for t, v in points)
    variance = sum((t - mean_t) ** 2 for t, _ in points)
    if variance == 0 or mean_v == 0:
        return 0.0
    return covariance / variance * 3600 / mean_v * 100
