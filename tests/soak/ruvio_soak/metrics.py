"""Samples every process every few seconds: `INFO` fields over RESP (every byte count the server
reports about its data, buffers and persistence) and the process memory from the backend (RSS,
peak, anonymous and file-backed parts, threads). Writes `metrics.jsonl` and keeps each process's
RSS and the server's own memory count since its last start, from which `rss_slopes` and
`charged_slopes` estimate a climb in percent per hour. RSS climbing with a flat charged memory
points at the allocator or a leak; both climbing together is the data growing."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from .cluster import COORDINATOR, Cluster
from .resp import ReplyError, Unknown, call_once, parse_info

NODE_FIELDS = (
    "replication_role",
    "replication_epoch",
    "replication_replicas",
    "replication_send_lag",
    "connected_clients",
    "keys",
    "total_commands_processed",
    "wal_unsynced_bytes",
)
COORDINATOR_FIELDS = ("leader_id", "is_leader", "failovers_total")


def node_info(cluster: Cluster, host: int, role: str) -> dict[str, str] | None:
    password = cluster.secret if role == COORDINATOR else cluster.password
    try:
        return parse_info(call_once(cluster.address(host, role), "INFO", password=password))
    except (OSError, ReplyError, Unknown):
        return None


def pick(role: str, info: dict[str, str]) -> dict:
    if role == COORDINATOR:
        picked = {k: info[k] for k in COORDINATOR_FIELDS if k in info}
        picked.update({k: v for k, v in info.items() if k.startswith("group_")})
        return picked
    picked = {k: info[k] for k in NODE_FIELDS if k in info}
    picked.update({k: v for k, v in info.items() if ("memory" in k or k.endswith("_bytes")) and not k.endswith("_human")})
    return picked


class Collector(threading.Thread):
    def __init__(self, cluster: Cluster, backend, path: Path, interval: float, stop: threading.Event):
        super().__init__(name="metrics", daemon=True)
        self.cluster, self.backend, self.interval, self.stop = cluster, backend, interval, stop
        self.file = open(path, "a", buffering=1)
        self.rss: dict[str, list[tuple[float, int]]] = {}
        self.charged: dict[str, list[tuple[float, int]]] = {}
        self.lock = threading.Lock()

    def restarted(self, host: int, role: str) -> None:
        """Starts a new RSS series for a process that was started again."""
        with self.lock:
            self.rss.pop(f"host{host}-{role}", None)
            self.charged.pop(f"host{host}-{role}", None)

    def sample(self) -> None:
        now = time.time()
        for host, role in self.cluster.processes():
            name = f"host{host}-{role}"
            memory = self.backend.memory(host, role) or {}
            rss = memory.get("rss_kib")
            info = node_info(self.cluster, host, role)
            entry = {"t": round(now, 1), "process": name, "rss_kib": rss, "up": info is not None}
            entry.update(memory)
            if info is not None:
                entry.update(pick(role, info))
            self.file.write(json.dumps(entry, separators=(",", ":")) + "\n")
            charged = info.get("charged_memory_bytes") if info else None
            with self.lock:
                if rss is not None:
                    self.rss.setdefault(name, []).append((now, rss))
                if charged is not None and charged.isdigit():
                    self.charged.setdefault(name, []).append((now, int(charged)))

    def run(self) -> None:
        while not self.stop.is_set():
            self.sample()
            self.stop.wait(self.interval)
        self.file.close()

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


def slope_percent_per_hour(points: list[tuple[float, int]]) -> float:
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    covariance = sum((t - mean_t) * (v - mean_v) for t, v in points)
    variance = sum((t - mean_t) ** 2 for t, _ in points)
    if variance == 0 or mean_v == 0:
        return 0.0
    return covariance / variance * 3600 / mean_v * 100
