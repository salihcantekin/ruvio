"""Load for the primitives that make a correctness promise: `LEASE` and `SEMAPHORE` (fencing
tokens, exclusive holders), `TRANSFER` (one atomic move between two integers) and `IDEM` (one
effect per request). Every operation goes to the same history file as `workload`, with the same
`invoke` / `ok` / `fail` / `info` entries, so the same faults apply. `primitives_check` and
`tests/soak/porcupine` read it.

Entries (`f`, key `k`, invoke value, `ok` value):

| f | k | invoke `v` | `ok` `v` |
| --- | --- | --- | --- |
| `lease`, `semaphore` | the lock key | `[max, ttl_ms]` (`max` is 1 for `lease`) | token, or null when refused |
| `release` | the lock key | token | 1 or 0 |
| `init` | the account group | `{account key: balance}` | same |
| `transfer` | the account group | `[source, destination, amount]` | `[source, destination]` balances, or null |
| `begin` | the request key | `[fingerprint, owner]` | `[status, result?]` |
| `effect` | the request key | null | the effect counter after the increment |
| `complete` | the request key | `[fingerprint, owner, result]` | 1 or 0 |
| `final` | an account key or a request key | null | the balance, or the effect counter |

`fail` (certainly not applied) is only recorded for a command that was refused before it was
sent, never for a release, so a holder either released or its lock expires after `ttl_ms`."""

from __future__ import annotations

import itertools
import random
import threading
import time

from .resp import ClusterClient, Refused, ReplyError, Unknown
from .workload import History, Worker, _setup

LOCK_TTL_MS = 30_000
IDEM_TTL_MS = 3_600_000
IDEM_EPOCH_S = 1800   # half the record's time to live
_init_ids = itertools.count(1)
_accounts: dict[str, str] = {}  # account key -> profile, for the final reads
_requests: dict[str, str] = {}  # IDEM request key -> profile


def _token(reply):
    return int(reply) if reply is not None else None


class PrimitiveWorker(Worker):
    """A worker whose `result` depends on the command it ran."""

    def result(self, function, value, reply):  # type: ignore[override]
        if function in ("lease", "semaphore"):
            return _token(reply)
        if function in ("release", "complete", "effect"):
            return int(reply)
        if function == "transfer":
            return [int(x) for x in reply] if reply is not None else None
        if function == "begin":
            return [x.decode() if isinstance(x, bytes) else x for x in reply]
        return value


class LockWorker(PrimitiveWorker):
    """Takes the lock, holds it for a short while, releases it."""

    def __init__(self, *args, keys, kind, maximum, hold_s):
        super().__init__(*args)
        self.keys, self.kind, self.maximum, self.hold_s = keys, kind, maximum, hold_s

    def step(self):
        key = random.choice(self.keys)
        if self.kind == "lease":
            command = ("LEASE", key, LOCK_TTL_MS)
        else:
            command = ("SEMAPHORE", key, self.maximum, LOCK_TTL_MS)
        token = self.record(self.kind, key, [self.maximum, LOCK_TTL_MS], lambda: self.client.execute(key, *command))
        if token is None:
            return
        self.stop.wait(random.uniform(*self.hold_s))
        # A release is retried until it has an answer, so it is never recorded as not applied.
        for _ in range(5):
            self.op += 1
            base = {"p": self.name, "i": self.op, "f": "release", "k": key, "pr": self.profile}
            self.history.add(dict(base, type="invoke", v=int(token)))
            try:
                reply = self.client.execute(key, "RELEASE", key, int(token))
            except (Refused, Unknown, ReplyError) as error:
                self.history.add(dict(base, type="info", v=int(token), e=str(error)[:120]))
                if self.stop.wait(0.2):
                    return
                continue
            self.history.add(dict(base, type="ok", v=int(reply)))
            return


class TransferWorker(PrimitiveWorker):
    """Moves small amounts between the accounts of one group (one hash slot)."""

    def __init__(self, *args, group, accounts):
        super().__init__(*args)
        self.group, self.accounts = group, accounts

    def step(self):
        source, destination = random.sample(self.accounts, 2)
        amount = random.randint(1, 50)
        self.record("transfer", self.group, [source, destination, amount],
                    lambda: self.client.execute(source, "TRANSFER", source, destination, amount))


class IdemWorker(PrimitiveWorker):
    """Several workers race to start the same request; the one that is told `acquired` runs the
    effect (an `INCR`) and completes it. The others must see `pending` or `completed`."""

    def __init__(self, *args, requests, tag):
        super().__init__(*args)
        self.requests, self.tag = requests, tag

    def step(self):
        number = random.randrange(len(self.requests))
        # A request is only started again while its record must still be there: the request
        # keys change every IDEM_EPOCH_S, so no key outlives IDEM_TTL_MS in a long run (a second
        # `acquired` after the record expired is correct, not a double acquire).
        epoch = int(time.time() // IDEM_EPOCH_S)
        key = f"{self.requests[number]}e{epoch}"
        _requests.setdefault(key, self.profile)
        suffix = key.rsplit(":", 1)[1]
        fingerprint = f"fp-{suffix}"
        owner = f"{self.name}-{self.op + 1}"
        reply = self.record("begin", key, [fingerprint, owner],
                            lambda: self.client.execute(key, "IDEM", key, "BEGIN", fingerprint, owner, IDEM_TTL_MS))
        if not reply or reply[0] != b"acquired":
            return
        counter = effect_key(key)
        if self.record("effect", key, None, lambda: self.client.execute(counter, "INCR", counter)) is None:
            return
        self.record("complete", key, [fingerprint, owner, f"done-{suffix}"],
                    lambda: self.client.execute(key, "IDEM", key, "COMPLETE", fingerprint, owner, f"done-{suffix}"))


def effect_key(request_key: str) -> str:
    """The counter an `IdemWorker` increments for a request (`...:idem:<n>` -> `...:idemfx:<n>`)."""
    return request_key.replace(":idem:", ":idemfx:")


def start(history: History, seeds, password, load: dict, stop: threading.Event, run_tag: str) -> list[Worker]:
    """Starts the workers the plan's `load.primitives` section asks for, for each profile:
    `leases`, `semaphores`, `semaphore_max`, `lock_workers` (per lock key set), `accounts`,
    `transfer_workers`, `idem_requests`, `idem_workers`."""
    settings = load.get("primitives")
    workers: list[Worker] = []
    if not settings:
        return workers
    rate = float(settings.get("rate_per_worker", 10))
    hold_s = tuple(settings.get("hold_s", [0.05, 0.4]))
    for profile, profile_settings in load.get("profiles", {"default": {}}).items():
        replicas = int(profile_settings.get("replicas", 1))
        args = lambda name: (f"{profile}-{name}", history, seeds, password, profile, replicas, rate, stop)  # noqa: E731
        tag = f"{run_tag}:{profile}"
        leases = [f"soak:{tag}:lease:{n}" for n in range(int(settings.get("leases", 2)))]
        semaphores = [f"soak:{tag}:semaphore:{n}" for n in range(int(settings.get("semaphores", 2)))]
        maximum = int(settings.get("semaphore_max", 3))
        for w in range(int(settings.get("lock_workers", 4))):
            workers.append(LockWorker(*args(f"lease{w}"), keys=leases, kind="lease", maximum=1, hold_s=hold_s))
            workers.append(LockWorker(*args(f"semaphore{w}"), keys=semaphores, kind="semaphore", maximum=maximum, hold_s=hold_s))
        balance = int(settings.get("balance", 1000))
        for w in range(int(settings.get("transfer_groups", 2))):
            group = "{" + f"tx:{tag}:{w}" + "}"
            accounts = [f"{group}:a{i}" for i in range(int(settings.get("accounts", 5)))]
            _init_accounts(history, seeds, password, profile, group, accounts, balance)
            for t in range(int(settings.get("transfer_workers", 3))):
                workers.append(TransferWorker(*args(f"transfer{w}-{t}"), group=group, accounts=accounts))
        requests = [f"soak:{tag}:idem:{n}" for n in range(int(settings.get("idem_requests", 40)))]
        _requests.update({request: profile for request in requests})
        for w in range(int(settings.get("idem_workers", 3))):
            workers.append(IdemWorker(*args(f"idem{w}"), requests=requests, tag=tag))
    for worker in workers:
        worker.start()
    return workers


def _init_accounts(history, seeds, password, profile, group, accounts, balance) -> None:
    _accounts.update({account: profile for account in accounts})
    client = ClusterClient(seeds, password, deadline=30.0)
    base = {"p": "init", "i": next(_init_ids), "f": "init", "k": group, "pr": profile}
    value = {account: balance for account in accounts}
    history.add(dict(base, type="invoke", v=value))
    try:
        for account in accounts:
            client.execute(account, "SET", account, balance)
    except (Refused, Unknown, ReplyError) as error:
        history.add(dict(base, type="fail", v=value, e=str(error)[:120]))
    else:
        history.add(dict(base, type="ok", v=value))
    client.close()


def final_reads(history: History, seeds, password, deadline: float = 60.0) -> int:
    """Reads every account and every request's effect counter once the cluster is quiet; returns
    how many could not be read."""
    history.flush()
    client = ClusterClient(seeds, password, deadline=deadline)
    missing = 0
    keys = [(k, p, k) for k, p in sorted(_accounts.items())] + [(k, p, effect_key(k)) for k, p in sorted(_requests.items())]
    for number, (key, profile, source) in enumerate(keys, start=1):
        base = {"p": "primitives-final", "i": number, "f": "final", "k": key, "pr": profile}
        history.add(dict(base, type="invoke", v=None))
        try:
            reply = client.execute(source, "GET", source)
        except (Refused, Unknown, ReplyError) as error:
            history.add(dict(base, type="fail", v=None, e=str(error)[:120]))
            missing += 1
            continue
        history.add(dict(base, type="ok", v=int(reply) if reply is not None else 0))
    client.close()
    history.flush()
    return missing
