"""Background load: production-like traffic around the checked workload. Not recorded in the
history (its values are not checked); its latency, throughput and errors per minute go to
`background-latency.jsonl`, so a slowdown or a burst of errors under faults is still seen.

Each process (`processes`) runs its own `ClusterClient` at `rate` operations a second over
`keys` keys (`bg:<n>`, spread over every slot). The key's number picks its type and command:

| key % 10 | type | read (80 %) | write (20 %) |
| --- | --- | --- | --- |
| 0-3 | string | GET | SET (value of `value_bytes`) |
| 4 | string with TTL | GET | SET ... EX 30-300 (keys expire and come back) |
| 5 | hash, 10 fields | HGET | HSET |
| 6 | sorted set, 20 members | ZRANGE 0 4 | ZADD |
| 7 | set, 20 members | SISMEMBER | SADD |
| 8 | stream | XLEN | XADD MAXLEN ~ 100 |
| 9 | counter | GET | INCR |

Every type is bounded (fields, members and stream length are capped), so once every key was
written the data stops growing and memory can be compared hour to hour."""

from __future__ import annotations

import multiprocessing
import random
import signal
import time
from pathlib import Path

from .latency import Recorder
from .resp import ClusterClient, Refused, ReplyError, Unknown


def operation(rng: random.Random, n: int, value: bytes, read: bool):
    """(series, key, command) for key number `n`."""
    key = f"bg:{n}"
    kind = n % 10
    if kind <= 3:
        return ("get", key, ("GET", key)) if read else ("set", key, ("SET", key, value))
    if kind == 4:
        return ("get", key, ("GET", key)) if read else ("setex", key, ("SET", key, value, "EX", rng.randint(30, 300)))
    if kind == 5:
        field = f"f{rng.randrange(10)}"
        return ("hget", key, ("HGET", key, field)) if read else ("hset", key, ("HSET", key, field, value))
    if kind == 6:
        member = f"m{rng.randrange(20)}"
        return ("zrange", key, ("ZRANGE", key, 0, 4)) if read else ("zadd", key, ("ZADD", key, rng.randrange(1000), member))
    if kind == 7:
        member = f"m{rng.randrange(20)}"
        return ("sismember", key, ("SISMEMBER", key, member)) if read else ("sadd", key, ("SADD", key, member))
    if kind == 8:
        return ("xlen", key, ("XLEN", key)) if read else ("xadd", key, ("XADD", key, "MAXLEN", "~", 100, "*", "v", value[:16]))
    return ("get", key, ("GET", key)) if read else ("incr", key, ("INCR", key))


def worker(index: int, seeds, password, settings: dict, directory: str, stop) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    rng = random.Random(index)
    rate = float(settings.get("rate", 1000))
    keys = int(settings.get("keys", 100000))
    reads = float(settings.get("read_fraction", 0.8))
    value = b"x" * int(settings.get("value_bytes", 100))
    recorder = Recorder(Path(directory) / f"background-latency-{index}.jsonl", source=f"background{index}")
    # A shorter deadline than the checked workload's: background errors are counted, not chased.
    client = ClusterClient(seeds, password, deadline=float(settings.get("deadline_s", 5)))
    period = 1.0 / rate
    next_at = time.monotonic()
    last_flush = time.monotonic()
    while not stop.is_set():
        series, key, command = operation(rng, rng.randrange(keys), value, rng.random() < reads)
        started = time.monotonic()
        try:
            client.execute(key, *command)
            outcome = "ok"
        except Refused:
            outcome = "fail"
        except Unknown:
            outcome = "info"
        except ReplyError as error:
            outcome = "error:" + str(error).split(" ", 1)[0][:20]
        except OSError:
            outcome = "info"
        recorder.add(f"bg:{series}", outcome, time.monotonic() - started)
        now = time.monotonic()
        if now - last_flush >= 5:
            recorder.flush()
            last_flush = now
        next_at += period
        delay = next_at - now
        if delay > 0:
            time.sleep(delay)
        elif delay < -1:
            next_at = now  # behind by more than a second: do not burst to catch up
    recorder.close()
    client.close()


class Background:
    """Starts `settings["processes"]` worker processes; `stop()` ends them."""

    def __init__(self, seeds, password, settings: dict, directory: Path):
        self.context = multiprocessing.get_context("fork")
        self.stop_event = self.context.Event()
        self.processes = [
            self.context.Process(target=worker, args=(n, list(seeds), password, settings, str(directory), self.stop_event),
                                 name=f"background{n}", daemon=True)
            for n in range(int(settings.get("processes", 1)))
        ]

    def start(self) -> None:
        for process in self.processes:
            process.start()

    def alive(self) -> int:
        return sum(1 for p in self.processes if p.is_alive())

    def stop(self, timeout: float = 30) -> None:
        self.stop_event.set()
        for process in self.processes:
            process.join(timeout)
            if process.is_alive():
                process.kill()
