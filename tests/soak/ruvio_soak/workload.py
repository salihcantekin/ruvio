"""Load that records every operation in a history file (JSON lines), for `history.check`.

Each entry is `{"p": worker, "i": op, "type": "invoke"|"ok"|"fail"|"info", "f": function,
"k": key, "v": value, "pr": profile, "t": ns}`. `ok` means acknowledged, `fail` means certainly not
applied, `info` means the outcome is unknown. Every key has one writer, so its values are a
sequence the checker can order: counters get `INCR`, lists get `RPUSH` of 1, 2, 3, ..., registers
get `SET` of 1, 2, 3, ... and are read by other workers.

Profiles: `default` (plain connections) and `durable` (`CLIENT DURABILITY REPLICAS n`), plus
`client` when the plan asks for it: the same counters and registers through the official Python
client (`clients/python`, cluster mode, following failovers), where any exception counts as an
unknown outcome.

The history is written in segments (`history-0000.jsonl`, ...); `History.rotate` closes the
current one so it can be uploaded while the run goes on. Every operation's latency also goes to
the latency recorder, per profile and function."""

from __future__ import annotations

import json
import random
import sys
import threading
import time
from pathlib import Path

from .resp import ClusterClient, Refused, ReplyError, Unknown

WRITES = ("incr", "append", "write")


class History:
    """Appends entries to `directory/history-NNNN.jsonl`; `rotate` starts the next segment and
    returns the closed one. Keeps the written keys (key -> function, profile) for the final
    reads, so they never need the files read back."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.segment = 0
        self.path = self._path(0)
        self.file = open(self.path, "a", buffering=1 << 16)
        self.lock = threading.Lock()
        self.counts = {"ok": 0, "fail": 0, "info": 0}
        self.written: dict[str, tuple[str, str]] = {}

    def _path(self, segment: int) -> Path:
        return self.directory / f"history-{segment:04d}.jsonl"

    def paths(self) -> list[Path]:
        return [self._path(n) for n in range(self.segment + 1) if self._path(n).exists()]

    def add(self, entry: dict) -> None:
        entry["t"] = time.monotonic_ns()
        line = json.dumps(entry, separators=(",", ":"))
        with self.lock:
            self.file.write(line + "\n")
            if entry["type"] in self.counts:
                self.counts[entry["type"]] += 1
            if entry["type"] == "invoke" and entry["f"] in WRITES and entry["k"] not in self.written:
                self.written[entry["k"]] = (entry["f"], entry["pr"])

    def rotate(self) -> Path:
        with self.lock:
            closed = self.path
            self.file.close()
            self.segment += 1
            self.path = self._path(self.segment)
            self.file = open(self.path, "a", buffering=1 << 16)
        return closed

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

    def __init__(self, name, history, seeds, password, profile, replicas, rate, stop, latency=None):
        super().__init__(name=name, daemon=True)
        self.history, self.profile, self.rate, self.stop = history, profile, rate, stop
        self.latency = latency
        self.client = self.make_client(seeds, password, profile, replicas)
        self.op = 0

    def make_client(self, seeds, password, profile, replicas):
        return ClusterClient(seeds, password, setup=_setup(profile, replicas))

    def timed(self, function, outcome, started):
        if self.latency is not None:
            self.latency.add(f"{self.profile}:{function}", outcome, time.monotonic() - started)

    def record(self, function, key, value, call):
        """Runs `call()` between an invoke and its outcome; returns the reply or None."""
        self.op += 1
        base = {"p": self.name, "i": self.op, "f": function, "k": key, "pr": self.profile}
        self.history.add(dict(base, type="invoke", v=value))
        started = time.monotonic()
        try:
            reply = call()
        except Refused as error:
            self.timed(function, "fail", started)
            self.history.add(dict(base, type="fail", v=value, e=str(error)[:120]))
            return None
        except (Unknown, ReplyError) as error:
            # A plain error reply on these commands is not expected; treat it as unknown so the
            # checker never assumes it did not apply.
            self.timed(function, "info", started)
            self.history.add(dict(base, type="info", v=value, e=str(error)[:120]))
            return None
        except Exception as error:  # the official client's own exceptions: outcome unknown
            if not isinstance(self, OfficialMixin):
                raise
            self.timed(function, "info", started)
            self.history.add(dict(base, type="info", v=value, e=f"{type(error).__name__}: {error}"[:120]))
            return None
        self.timed(function, "ok", started)
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
    """RPUSH 1, 2, 3, ... With `max_len`, a list that reached it is left as it is and the
    worker goes on with a fresh key (`<key>:<generation>`), so the data stays bounded and its
    growth never hides an allocator or leak problem."""

    def __init__(self, *args, keys, max_len=0):
        super().__init__(*args)
        self.keys = list(keys)
        self.base = list(keys)
        self.generation = [0] * len(keys)
        self.next = {key: 1 for key in keys}
        self.max_len = max_len

    def step(self):
        index = random.randrange(len(self.keys))
        key = self.keys[index]
        value = self.next[key]
        if self.max_len and value > self.max_len:
            self.generation[index] += 1
            key = self.keys[index] = f"{self.base[index]}:{self.generation[index]}"
            self.next[key] = value = 1
        self.next[key] = value + 1
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


class OfficialMixin:
    """Uses the official Python client in cluster mode instead of the tool's own client."""

    client_path: str | None = None

    def make_client(self, seeds, password, profile, replicas):
        if self.client_path and self.client_path not in sys.path:
            sys.path.insert(0, self.client_path)
        import ruvio  # noqa: PLC0415 (only when the plan asks for the official client)

        return OfficialClient(ruvio.Ruvio(seeds=list(seeds), password=password, cluster=True))


class OfficialClient:
    """`execute(key, *command)` over the official client, the shape the workers call."""

    def __init__(self, client):
        self.inner = client

    def execute(self, key, *arguments):
        reply = self.inner.execute_command(*arguments)
        return reply.encode() if isinstance(reply, str) else reply

    def close(self):
        try:
            self.inner.close()
        except Exception:
            pass


class OfficialCounter(OfficialMixin, CounterWorker):
    pass


class OfficialRegisterWriter(OfficialMixin, RegisterWriter):
    pass


class OfficialRegisterReader(OfficialMixin, RegisterReader):
    pass


def start(history, seeds, password, load: dict, stop: threading.Event, run_tag: str, latency=None) -> list[Worker]:
    """Starts the workers the plan's `load` section asks for, for each profile. `load.official`
    ({"path": "clients/python", "counters": n, "registers": n, "readers_per_register": n})
    adds the `client` profile through the official client."""
    workers: list[Worker] = []
    rate = float(load.get("rate_per_worker", 20))
    keys_per_worker = int(load.get("keys_per_worker", 4))
    max_len = int(load.get("max_list_len", 0))
    for profile, settings in load.get("profiles", {"default": {}}).items():
        replicas = int(settings.get("replicas", 1))
        args = lambda name: (f"{profile}-{name}", history, seeds, password, profile, replicas, rate, stop, latency)  # noqa: E731
        key = lambda kind, w, k: f"soak:{run_tag}:{profile}:{kind}:{w}:{k}"  # noqa: E731
        for w in range(int(load.get("counters", 1))):
            workers.append(CounterWorker(*args(f"counter{w}"), keys=[key("counter", w, k) for k in range(keys_per_worker)]))
        for w in range(int(load.get("appenders", 1))):
            workers.append(AppendWorker(*args(f"append{w}"), keys=[key("list", w, k) for k in range(keys_per_worker)], max_len=max_len))
        for w in range(int(load.get("registers", 1))):
            keys = [key("register", w, k) for k in range(keys_per_worker)]
            workers.append(RegisterWriter(*args(f"register{w}"), keys=keys))
            for r in range(int(load.get("readers_per_register", 1))):
                workers.append(RegisterReader(*args(f"register{w}-reader{r}"), keys=keys))
    official = load.get("official")
    if official:
        OfficialMixin.client_path = official.get("path")
        profile = "client"
        args = lambda name: (f"{profile}-{name}", history, seeds, password, profile, 0, rate, stop, latency)  # noqa: E731
        key = lambda kind, w, k: f"soak:{run_tag}:{profile}:{kind}:{w}:{k}"  # noqa: E731
        for w in range(int(official.get("counters", 1))):
            workers.append(OfficialCounter(*args(f"counter{w}"), keys=[key("counter", w, k) for k in range(keys_per_worker)]))
        for w in range(int(official.get("registers", 1))):
            keys = [key("register", w, k) for k in range(keys_per_worker)]
            workers.append(OfficialRegisterWriter(*args(f"register{w}"), keys=keys))
            for r in range(int(official.get("readers_per_register", 1))):
                workers.append(OfficialRegisterReader(*args(f"register{w}-reader{r}"), keys=keys))
    for worker in workers:
        worker.start()
    return workers


def final_reads(history: History, seeds, password, deadline: float = 60.0) -> int:
    """Reads every key the history wrote, once the cluster is quiet; returns the number of keys
    that could not be read (each is recorded as `fail`)."""
    history.flush()
    with history.lock:
        written = dict(history.written)
    client = ClusterClient(seeds, password, deadline=deadline)
    missing = 0
    for op, (key, (function, profile)) in enumerate(sorted(written.items()), start=1):
        base = {"p": "final", "i": op, "f": "final", "k": key, "pr": profile}
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
