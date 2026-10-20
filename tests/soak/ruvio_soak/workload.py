"""Load that records every operation in a history file (JSON lines), for `history.check`.

Each entry is `{"p": worker, "i": op, "type": "invoke"|"ok"|"fail"|"info", "f": function,
"k": key, "v": value, "pr": profile, "t": ns}`. `ok` means acknowledged, `fail` means certainly not
applied, `info` means the outcome is unknown. Every key has one writer, so its values are a
sequence the checker can order: counters get `INCR`, lists get `RPUSH` of 1, 2, 3, ..., registers
get `SET` of 1, 2, 3, ... and are read by other workers.

Profiles: `default` (plain connections) and `durable` (`CLIENT DURABILITY REPLICAS n`)."""

from __future__ import annotations

import json
import random
import threading
import time
from pathlib import Path

from .resp import ClusterClient, Refused, ReplyError, Unknown


class History:
    def __init__(self, path: Path):
        self.path = path
        self.file = open(path, "a", buffering=1 << 16)
        self.lock = threading.Lock()
        self.counts = {"ok": 0, "fail": 0, "info": 0}

    def add(self, entry: dict) -> None:
        entry["t"] = time.monotonic_ns()
        line = json.dumps(entry, separators=(",", ":"))
        with self.lock:
            self.file.write(line + "\n")
            if entry["type"] in self.counts:
                self.counts[entry["type"]] += 1

    def flush(self) -> None:
        with self.lock:
            self.file.flush()

    def close(self) -> None:
        with self.lock:
            self.file.close()


def _setup(profile: str, replicas: int):
    return [("CLIENT", "DURABILITY", "REPLICAS", str(replicas))] if profile == "durable" else []


class Worker(threading.Thread):
    """Runs `step` about `rate` times a second until `stop` is set."""

    def __init__(self, name, history, seeds, password, profile, replicas, rate, stop):
        super().__init__(name=name, daemon=True)
        self.history, self.profile, self.rate, self.stop = history, profile, rate, stop
        self.client = ClusterClient(seeds, password, setup=_setup(profile, replicas))
        self.op = 0

    def record(self, function, key, value, call):
        """Runs `call()` between an invoke and its outcome; returns the reply or None."""
        self.op += 1
        base = {"p": self.name, "i": self.op, "f": function, "k": key, "pr": self.profile}
        self.history.add(dict(base, type="invoke", v=value))
        try:
            reply = call()
        except Refused as error:
            self.history.add(dict(base, type="fail", v=value, e=str(error)[:120]))
            return None
        except (Unknown, ReplyError) as error:
            # A plain error reply on these commands is not expected; treat it as unknown so the
            # checker never assumes it did not apply.
            self.history.add(dict(base, type="info", v=value, e=str(error)[:120]))
            return None
        self.history.add(dict(base, type="ok", v=self.result(function, value, reply)))
        return reply

    @staticmethod
    def result(function, value, reply):
        if function == "read":
            return int(reply) if reply is not None else 0
        return value

    def run(self):
        period = 1.0 / self.rate if self.rate > 0 else 0
        next_at = time.monotonic()
        while not self.stop.is_set():
            self.step()
            next_at += period
            delay = next_at - time.monotonic()
            if delay > 0:
                self.stop.wait(delay)
            else:
                next_at = time.monotonic()
        self.client.close()


class CounterWorker(Worker):
    def __init__(self, *args, keys):
        super().__init__(*args)
        self.keys = keys

    def step(self):
        key = random.choice(self.keys)
        self.record("incr", key, 1, lambda: self.client.execute(key, "INCR", key))


class AppendWorker(Worker):
    def __init__(self, *args, keys):
        super().__init__(*args)
        self.keys = keys
        self.next = {key: 1 for key in keys}

    def step(self):
        key = random.choice(self.keys)
        value = self.next[key]
        self.next[key] += 1
        self.record("append", key, value, lambda: self.client.execute(key, "RPUSH", key, value))


class RegisterWriter(Worker):
    def __init__(self, *args, keys):
        super().__init__(*args)
        self.keys = keys
        self.next = {key: 1 for key in keys}

    def step(self):
        key = random.choice(self.keys)
        value = self.next[key]
        self.next[key] += 1
        self.record("write", key, value, lambda: self.client.execute(key, "SET", key, value))


class RegisterReader(Worker):
    def __init__(self, *args, keys):
        super().__init__(*args)
        self.keys = keys

    def step(self):
        key = random.choice(self.keys)
        self.record("read", key, None, lambda: self.client.execute(key, "GET", key))


def start(history, seeds, password, load: dict, stop: threading.Event, run_tag: str) -> list[Worker]:
    """Starts the workers the plan's `load` section asks for, for each profile."""
    workers: list[Worker] = []
    rate = float(load.get("rate_per_worker", 20))
    keys_per_worker = int(load.get("keys_per_worker", 4))
    for profile, settings in load.get("profiles", {"default": {}}).items():
        replicas = int(settings.get("replicas", 1))
        args = lambda name: (f"{profile}-{name}", history, seeds, password, profile, replicas, rate, stop)  # noqa: E731
        key = lambda kind, w, k: f"soak:{run_tag}:{profile}:{kind}:{w}:{k}"  # noqa: E731
        for w in range(int(load.get("counters", 1))):
            workers.append(CounterWorker(*args(f"counter{w}"), keys=[key("counter", w, k) for k in range(keys_per_worker)]))
        for w in range(int(load.get("appenders", 1))):
            workers.append(AppendWorker(*args(f"append{w}"), keys=[key("list", w, k) for k in range(keys_per_worker)]))
        for w in range(int(load.get("registers", 1))):
            keys = [key("register", w, k) for k in range(keys_per_worker)]
            workers.append(RegisterWriter(*args(f"register{w}"), keys=keys))
            for r in range(int(load.get("readers_per_register", 1))):
                workers.append(RegisterReader(*args(f"register{w}-reader{r}"), keys=keys))
    for worker in workers:
        worker.start()
    return workers


def final_reads(history: History, seeds, password, deadline: float = 60.0) -> int:
    """Reads every key the history wrote, once the cluster is quiet; returns the number of keys
    that could not be read (each is recorded as `fail`)."""
    keys: dict[str, str] = {}
    profiles: dict[str, str] = {}
    history.flush()
    with open(history.path) as file:
        for line in file:
            entry = json.loads(line)
            if entry["type"] == "invoke" and entry["f"] in ("incr", "append", "write"):
                keys[entry["k"]] = entry["f"]
                profiles[entry["k"]] = entry["pr"]
    client = ClusterClient(seeds, password, deadline=deadline)
    missing = 0
    for op, (key, function) in enumerate(sorted(keys.items()), start=1):
        base = {"p": "final", "i": op, "f": "final", "k": key, "pr": profiles[key]}
        history.add(dict(base, type="invoke", v=None))
        try:
            if function == "append":
                reply = client.execute(key, "LRANGE", key, 0, -1)
                value = [int(v) for v in reply or []]
            else:
                reply = client.execute(key, "GET", key)
                value = int(reply) if reply is not None else 0
        except (Refused, Unknown, ReplyError) as error:
            history.add(dict(base, type="fail", v=None, e=str(error)[:120]))
            missing += 1
            continue
        history.add(dict(base, type="ok", v=value))
    client.close()
    history.flush()
    return missing
