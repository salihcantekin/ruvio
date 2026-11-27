#!/usr/bin/env python3
"""Model-based randomized checker for Ruvio: random ops against a reference model, crashes,
restarts and injected disk faults, with the recovered state checked against what the
durability profile promises.

    python3 scripts/model_check.py --seed 7 --duration 300 --config all \\
        --bin target/release-fast/ruvio --results /tmp/model-results

Several connections send random commands over a small keyspace. Commands that run at the same
time never share a key, so every key has one known order of operations; commands on disjoint
keys overlap freely. Each command ends acknowledged, refused, or unknown (the connection died
before the reply). After every restart the checker looks for a cut of each shard's history that
explains the recovered keyspace:

- a key's value equals the model after some prefix of that key's operations;
- an operation that touched several keys (MULTI, EVAL, RENAME, FLUSHDB) is in for all or none;
- the cut is closed under real time: if an included write was sent after another write on the
  same shard was acknowledged, that one is included too (no holes);
- the profile sets the lower bound: `always` and a clean shutdown keep every acknowledged
  write, `everysec` every write acknowledged before the sync window, `snapshot` every write
  acknowledged before the last successful SAVE, and `memory` keeps nothing.

Live replies are compared with the model as commands complete. Disk faults come from an
LD_PRELOAD shim (scripts/model_check_shim.c) compiled into the work directory. The shim also
drives two crash forms for the WAL profiles: a power loss (kill -9, then the WAL bytes written
since their file's last fsync are zeroed) and a torn write (the server dies after a random prefix
of one WAL write, so a crash lands between or inside the records of one batch). A failure writes
a self-contained repro directory under --results.

The seed fixes the run's schedule: how many epochs, how many commands each one sends, which
event ends it and after how many commands a disk fault starts. No part of the schedule reads the
clock, so `--seed N` replays the same epochs and events on a fast or a slow machine; --duration
is nominal work (OPS_PER_SECOND commands per second) and the wall time follows the machine.
What stays up to OS timing is which commands overlap, so which keys are free for the next one,
and where inside a write a kill lands.
"""

from __future__ import annotations

import argparse
import collections
import ctypes
import hashlib
import json
import math
import os
import random
import re
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SHIM_SOURCE = REPO / "scripts" / "model_check_shim.c"

# Milliseconds of slack around a TTL deadline inside which a key may or may not exist.
TTL_PRESENCE_SLACK_MS = 500
# Allowed difference between a model deadline and the PTTL a server reports.
TTL_VALUE_SLACK_MS = 1200
# Length of a WAL segment header (`WAL_HEADER_LEN` in src/persistence/codec.rs).
WAL_HEADER_BYTES = 52
# Keys with a deadline this close to now are not touched by new commands.
QUARANTINE_MS = 600
# Commands one nominal second of --duration sends. A debug build on a loaded runner keeps up.
OPS_PER_SECOND = 1000
# Ports one run uses from its base: the primary's shards, the replica's shards at
# REPLICA_PORT_OFFSET and the replication listener's at REPLICATION_PORT_OFFSET.
PORT_BLOCK = 16
REPLICA_PORT_OFFSET = 5
REPLICATION_PORT_OFFSET = 10

SCRIPT = (
    "local v = ruvio.call('GET', KEYS[1]) if not v then v = '' end "
    "ruvio.call('SET', KEYS[2], v .. ARGV[1]) return v"
)
SCRIPT_B = SCRIPT.encode()

# Commands Redis does not flag `denyoom`: they only free (or move) memory, so a full
# noeviction store still runs them. An OOM reply to one of these is a failure.
RUNS_WHEN_FULL = {b"DEL", b"UNLINK", b"GETDEL", b"LPOP", b"RPOP", b"BLPOP", b"HDEL", b"SREM",
                  b"ZREM", b"XDEL", b"PERSIST", b"RENAME"}


class Failure(Exception):
    """A violated expectation; `kind` names the class, `detail` explains it."""

    def __init__(self, kind: str, detail: str, diff: str = ""):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail
        self.diff = diff


# ---------------------------------------------------------------------------------------------
# RESP


class Err:
    __slots__ = ("msg",)

    def __init__(self, msg: str):
        self.msg = msg

    def __repr__(self):
        return f"Err({self.msg!r})"

    def __eq__(self, other):
        return isinstance(other, Err) and other.msg == self.msg

    def __hash__(self):
        return hash(("err", self.msg))

    @property
    def code(self) -> str:
        return self.msg.split(" ", 1)[0] if self.msg else ""


class Incomplete(Exception):
    pass


def encode(argv) -> bytes:
    out = [b"*%d\r\n" % len(argv)]

    for arg in argv:
        if not isinstance(arg, bytes):
            arg = str(arg).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(arg), arg))

    return b"".join(out)


def parse_one(buf: bytes | bytearray, pos: int):
    if pos >= len(buf):
        raise Incomplete
    end = buf.find(b"\r\n", pos)

    if end < 0:
        raise Incomplete
    kind = buf[pos]
    line = bytes(buf[pos + 1 : end])

    if kind == 43:  # +
        return line.decode(errors="replace"), end + 2
    if kind == 45:  # -
        return Err(line.decode(errors="replace")), end + 2
    if kind == 58:  # :
        return int(line), end + 2
    if kind == 36:  # $
        n = int(line)

        if n < 0:
            return None, end + 2
        if len(buf) < end + 2 + n + 2:
            raise Incomplete
        return bytes(buf[end + 2 : end + 2 + n]), end + 4 + n
    if kind == 42:  # *
        n = int(line)

        if n < 0:
            return None, end + 2
        items = []
        p = end + 2

        for _ in range(n):
            item, p = parse_one(buf, p)
            items.append(item)
        return items, p
    raise ValueError(f"bad RESP byte {kind!r} at {pos}: {bytes(buf[pos:pos + 40])!r}")


class BlockingClient:
    """A plain request/response client for setup, dumps and probes."""

    def __init__(self, port: int, timeout: float = 10.0, user: tuple[str, str] | None = None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = bytearray()

        if user:
            reply = self.call("AUTH", user[0], user[1])

            if isinstance(reply, Err):
                raise OSError(f"AUTH failed: {reply}")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def pipeline(self, commands: list) -> list:
        self.sock.sendall(b"".join(encode(c) for c in commands))
        replies = []
        pos = 0

        while len(replies) < len(commands):
            try:
                value, pos = parse_one(self.buf, pos)
                replies.append(value)
            except Incomplete:
                chunk = self.sock.recv(1 << 20)

                if not chunk:
                    raise ConnectionError("server closed the connection")
                self.buf += chunk
        del self.buf[:pos]
        return replies

    def call(self, *argv):
        return self.pipeline([list(argv)])[0]


# ---------------------------------------------------------------------------------------------
# Keys and slots


def crc16(data: bytes) -> int:
    crc = 0

    for byte in data:
        crc ^= byte << 8

        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(key: bytes) -> int:
    start = key.find(b"{")

    if start >= 0:
        end = key.find(b"}", start + 1)

        if end > start + 1:
            key = key[start + 1 : end]
    return crc16(key) % 16384


def shard_of(key: bytes, shards: int) -> int:
    slot = key_slot(key)

    for shard in range(shards - 1, -1, -1):
        if slot >= shard * 16384 // shards:
            return shard
    return 0


# ---------------------------------------------------------------------------------------------
# Reference model of the commands used.
#
# A key state is a tuple (type, data, deadline_ms) or None. Data is immutable:
# string bytes; list tuple; hash tuple of sorted (field, value, field_deadline|None);
# set frozenset; zset tuple of sorted (member, score); stream tuple of (id, fields tuple).


def alive(state, now: float):
    """The state as a read at `now` sees it, or None."""

    if state is None:
        return None
    kind, data, deadline = state

    if deadline is not None and deadline <= now:
        return None
    if kind == "hash":
        fields = tuple(f for f in data if f[2] is None or f[2] > now)

        if not fields:
            return None
        if len(fields) != len(data):
            return (kind, fields, deadline)
    return state


class Alts(tuple):
    """Several possible states of one key (a command ran around a TTL deadline)."""


def deadlines_of(state):
    if state is None:
        return []
    if isinstance(state, Alts):
        return [d for option in state for d in deadlines_of(option)]
    out = [state[2]] if state[2] is not None else []

    if state[0] == "hash":
        out.extend(f[2] for f in state[1] if f[2] is not None)
    return out


def changes_live_state(op, key, state) -> bool:
    """False when acknowledging `op` promised nothing a restart could lose:

    - `state` is what a read of `key` already saw when `op` was sent: the op only dropped a key
      or hash fields past their deadline (a DEL that replied 0 on an expired key), which a
      restart drops again from the logged deadline;
    - a push whose element the blocked BLPOP returned: handed straight to the waiting client,
      the list never existed and nothing is logged."""

    if op.kind == "pair-push" and op.pair is not None and op.pair.status == "ok" \
            and op.pair.replies and op.pair.replies[0] is not None:
        return False
    base = op.base.get(key)
    seen = {alive(option, op.send_t) for option in (base if isinstance(base, Alts) else (base,))}
    states = state if isinstance(state, Alts) else (state,)

    return len(seen) != 1 or any(option not in seen for option in states)


class Loose:
    """A predicted reply compared by `mode` instead of equality."""

    __slots__ = ("mode", "value")

    def __init__(self, mode: str, value):
        self.mode = mode
        self.value = value

    def __repr__(self):
        return f"Loose({self.mode}, {self.value!r})"


# Counter values next to a digit carry, a sign change or an i64 limit.
COUNTER_EDGES = [0, -1, 1, 9, -9, 99, -99, 999, -10, 2**31 - 1, 2**32 - 1, 2**63 - 2, 2**63 - 1,
                 -(2**63) + 1, -(2**63)]

WRONGTYPE = Err("WRONGTYPE")
ANY_ERROR = Err("*")


def to_int(data: bytes):
    try:
        if data != data.strip() or (len(data) > 1 and data[:1] == b"0") or data.startswith(b"+"):
            return None
        value = int(data)

        if data.startswith(b"-0") or not (-(2**63) <= value < 2**63):
            return None
        return value
    except ValueError:
        return None


class Engine:
    """Runs commands against `view` (a dict of key states) at time `now` (unix ms)."""

    def __init__(self, view: dict, now: float, keys_all=None):
        self.view = view
        self.now = now
        self.keys_all = keys_all

    def get(self, key):
        return alive(self.view.get(key), self.now)

    def put(self, key, state):
        self.view[key] = state

    def run(self, argv: list[bytes]):
        name = argv[0].upper().decode()
        handler = getattr(self, "c_" + name, None)

        if handler is None:
            raise ValueError(f"model has no {name}")
        return handler(argv[1:])

    def typed(self, key, kind):
        state = self.get(key)

        if state is None:
            return None, False
        if state[0] != kind:
            return state, True
        return state, False

    # strings
    def c_GET(self, a):
        state, wrong = self.typed(a[0], "string")

        if wrong:
            return WRONGTYPE
        return None if state is None else state[1]

    def c_SET(self, a):
        deadline = None

        if len(a) > 2:
            opt = a[2].upper()
            amount = int(a[3])
            deadline = self.now + (amount * 1000 if opt == b"EX" else amount)
        self.put(a[0], ("string", a[1], deadline))
        return "OK"

    def c_INCR(self, a):
        state, wrong = self.typed(a[0], "string")

        if wrong:
            return WRONGTYPE
        if state is None:
            self.put(a[0], ("string", b"1", None))
            return 1
        value = to_int(state[1])

        if value is None or value + 1 >= 2**63:
            return ANY_ERROR
        self.put(a[0], ("string", str(value + 1).encode(), state[2]))
        return value + 1

    def c_INCRBY(self, a):
        return self._increment(a[0], int(a[1]))

    def c_DECRBY(self, a):
        return self._increment(a[0], -int(a[1]))

    def _increment(self, key, delta):
        state, wrong = self.typed(key, "string")

        if wrong:
            return WRONGTYPE
        value = 0 if state is None else to_int(state[1])

        if value is None or not -(2**63) <= value + delta < 2**63:
            return ANY_ERROR
        self.put(key, ("string", str(value + delta).encode(), None if state is None else state[2]))
        return value + delta

    def c_APPEND(self, a):
        state, wrong = self.typed(a[0], "string")

        if wrong:
            return WRONGTYPE
        data = (state[1] if state else b"") + a[1]
        self.put(a[0], ("string", data, state[2] if state else None))
        return len(data)

    # keys
    def c_DEL(self, a):
        count = 0

        for key in a:
            if self.get(key) is not None:
                count += 1
            self.put(key, None)
        return count

    def c_EXISTS(self, a):
        return sum(1 for key in a if self.get(key) is not None)

    def c_EXPIRE(self, a):
        return self._expire(a[0], int(a[1]) * 1000)

    def c_PEXPIRE(self, a):
        return self._expire(a[0], int(a[1]))

    def _expire(self, key, ms):
        state = self.get(key)

        if state is None:
            self.put(key, None)
            return 0
        self.put(key, (state[0], state[1], self.now + ms))
        return 1

    def c_PERSIST(self, a):
        state = self.get(a[0])

        if state is None or state[2] is None:
            return 0
        self.put(a[0], (state[0], state[1], None))
        return 1

    def c_PTTL(self, a):
        state = self.get(a[0])

        if state is None:
            return -2
        if state[2] is None:
            return -1
        return Loose("ttl", state[2] - self.now)

    def c_TYPE(self, a):
        state = self.get(a[0])
        return "none" if state is None else state[0]

    def c_RENAME(self, a):
        state = self.get(a[0])

        if state is None:
            return ANY_ERROR
        self.put(a[0], None)
        self.put(a[1], state)
        return "OK"

    def c_FLUSHDB(self, a):
        for key in self.keys_all:
            self.put(key, None)
        return "OK"

    # lists
    def _push(self, a, left):
        state, wrong = self.typed(a[0], "list")

        if wrong:
            return WRONGTYPE
        items = list(state[1]) if state else []

        for item in a[1:]:
            if left:
                items.insert(0, item)
            else:
                items.append(item)
        self.put(a[0], ("list", tuple(items), state[2] if state else None))
        return len(items)

    def c_LPUSH(self, a):
        return self._push(a, True)

    def c_RPUSH(self, a):
        return self._push(a, False)

    def c_LPOP(self, a):
        state, wrong = self.typed(a[0], "list")

        if wrong:
            return WRONGTYPE
        if state is None:
            return None
        head = state[1][0]
        rest = state[1][1:]
        self.put(a[0], ("list", rest, state[2]) if rest else None)
        return head

    def c_LRANGE(self, a):
        state, wrong = self.typed(a[0], "list")

        if wrong:
            return WRONGTYPE
        return [] if state is None else list(state[1])

    def c_LLEN(self, a):
        state, wrong = self.typed(a[0], "list")

        if wrong:
            return WRONGTYPE
        return 0 if state is None else len(state[1])

    # hashes
    def c_HSET(self, a):
        state, wrong = self.typed(a[0], "hash")

        if wrong:
            return WRONGTYPE
        fields = {f: (v, d) for f, v, d in state[1]} if state else {}
        added = 0

        for i in range(1, len(a), 2):
            if a[i] not in fields:
                added += 1
            fields[a[i]] = (a[i + 1], None)
        data = tuple(sorted((f, v, d) for f, (v, d) in fields.items()))
        self.put(a[0], ("hash", data, state[2] if state else None))
        return added

    def c_HDEL(self, a):
        state, wrong = self.typed(a[0], "hash")

        if wrong:
            return WRONGTYPE
        if state is None:
            return 0
        fields = {f: (v, d) for f, v, d in state[1]}
        removed = 0

        for field in a[1:]:
            if field in fields:
                del fields[field]
                removed += 1
        data = tuple(sorted((f, v, d) for f, (v, d) in fields.items()))
        self.put(a[0], ("hash", data, state[2]) if data else None)
        return removed

    def c_HGETALL(self, a):
        state, wrong = self.typed(a[0], "hash")

        if wrong:
            return WRONGTYPE
        return Loose("hash", {} if state is None else {f: v for f, v, _ in state[1]})

    def c_HPEXPIRE(self, a):
        # HPEXPIRE key ms FIELDS n f...
        state, wrong = self.typed(a[0], "hash")
        names = a[4:]

        if wrong:
            return WRONGTYPE
        if state is None:
            return [-2] * len(names)
        fields = {f: (v, d) for f, v, d in state[1]}
        out = []

        for field in names:
            if field in fields:
                fields[field] = (fields[field][0], self.now + int(a[1]))
                out.append(1)
            else:
                out.append(-2)
        data = tuple(sorted((f, v, d) for f, (v, d) in fields.items()))
        self.put(a[0], ("hash", data, state[2]))
        return out

    # sets
    def c_SADD(self, a):
        state, wrong = self.typed(a[0], "set")

        if wrong:
            return WRONGTYPE
        members = set(state[1]) if state else set()
        before = len(members)
        members.update(a[1:])
        self.put(a[0], ("set", frozenset(members), state[2] if state else None))
        return len(members) - before

    def c_SREM(self, a):
        state, wrong = self.typed(a[0], "set")

        if wrong:
            return WRONGTYPE
        if state is None:
            return 0
        members = set(state[1])
        before = len(members)
        members.difference_update(a[1:])
        self.put(a[0], ("set", frozenset(members), state[2]) if members else None)
        return before - len(members)

    def c_SMEMBERS(self, a):
        state, wrong = self.typed(a[0], "set")

        if wrong:
            return WRONGTYPE
        return Loose("set", frozenset() if state is None else state[1])

    # sorted sets
    def c_ZADD(self, a):
        state, wrong = self.typed(a[0], "zset")

        if wrong:
            return WRONGTYPE
        members = dict(state[1]) if state else {}
        added = 0

        for i in range(1, len(a), 2):
            member = a[i + 1]

            if member not in members:
                added += 1
            members[member] = float(a[i])
        self.put(a[0], ("zset", tuple(sorted(members.items())), state[2] if state else None))
        return added

    def c_ZINCRBY(self, a):
        state, wrong = self.typed(a[0], "zset")

        if wrong:
            return WRONGTYPE
        members = dict(state[1]) if state else {}
        score = members.get(a[2], 0.0) + float(a[1])
        members[a[2]] = score
        self.put(a[0], ("zset", tuple(sorted(members.items())), state[2] if state else None))
        return Loose("score", score)

    def c_ZREM(self, a):
        state, wrong = self.typed(a[0], "zset")

        if wrong:
            return WRONGTYPE
        if state is None:
            return 0
        members = dict(state[1])
        removed = 0

        for member in a[1:]:
            if member in members:
                del members[member]
                removed += 1
        self.put(a[0], ("zset", tuple(sorted(members.items())), state[2]) if members else None)
        return removed

    def c_ZRANGE(self, a):
        state, wrong = self.typed(a[0], "zset")

        if wrong:
            return WRONGTYPE
        if state is None:
            return []
        return Loose("zrange", sorted(state[1], key=lambda item: (item[1], item[0])))

    # streams
    def c_XADD(self, a):
        state, wrong = self.typed(a[0], "stream")

        if wrong:
            return WRONGTYPE
        entries = state[1] if state else ()
        self.put(a[0], ("stream", entries + ((a[1], tuple(a[2:])),), state[2] if state else None))
        return a[1]

    def c_XDEL(self, a):
        state, wrong = self.typed(a[0], "stream")

        if wrong:
            return WRONGTYPE
        if state is None:
            return 0
        entries = tuple(e for e in state[1] if e[0] not in a[1:])
        removed = len(state[1]) - len(entries)
        self.put(a[0], ("stream", entries, state[2]))
        return removed

    def c_XLEN(self, a):
        state, wrong = self.typed(a[0], "stream")

        if wrong:
            return WRONGTYPE
        return 0 if state is None else len(state[1])

    def c_XRANGE(self, a):
        state, wrong = self.typed(a[0], "stream")

        if wrong:
            return WRONGTYPE
        return [] if state is None else [[eid, list(fields)] for eid, fields in state[1]]

    # scripting
    def c_EVAL(self, a):
        # Only the fixed script SCRIPT is modelled: KEYS[2] = (GET KEYS[1] or '') .. ARGV[1].
        assert a[0] == SCRIPT_B
        src, dst, arg = a[2], a[3], a[4]
        state, wrong = self.typed(src, "string")

        if wrong:
            return ANY_ERROR
        value = state[1] if state else b""
        self.put(dst, ("string", value + arg, None))
        return value


def same_reply(predicted, actual) -> bool:
    if isinstance(predicted, Loose):
        mode, value = predicted.mode, predicted.value

        if mode == "ttl":
            return isinstance(actual, int) and actual >= 0 and abs(actual - value) <= TTL_VALUE_SLACK_MS
        if mode == "score":
            try:
                return abs(float(actual) - value) < 1e-9
            except (TypeError, ValueError):
                return False
        if mode == "hash":
            if not isinstance(actual, list) or len(actual) % 2:
                return False
            return dict(zip(actual[::2], actual[1::2])) == value and len(actual) == 2 * len(value)
        if mode == "set":
            return isinstance(actual, list) and frozenset(actual) == value and len(actual) == len(value)
        if mode == "zrange":
            if not isinstance(actual, list) or len(actual) != 2 * len(value):
                return False
            for i, (member, score) in enumerate(value):
                try:
                    if actual[2 * i] != member or abs(float(actual[2 * i + 1]) - score) > 1e-9:
                        return False
                except (TypeError, ValueError):
                    return False
            return True
        if mode == "any":
            return True
        if mode == "oneof":
            return any(same_reply(option, actual) for option in value)
        raise ValueError(mode)
    if isinstance(predicted, Err):
        if not isinstance(actual, Err):
            return False
        return predicted.msg == "*" or actual.code == predicted.msg.split(" ", 1)[0]
    if isinstance(predicted, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(predicted)
            and all(same_reply(p, a) for p, a in zip(predicted, actual))
        )
    return predicted == actual


# ---------------------------------------------------------------------------------------------
# Comparing a dumped key with a model state


def describe(state) -> str:
    if state is None:
        return "absent"
    if isinstance(state, Alts):
        return " OR ".join(describe(option) for option in state)
    kind, data, deadline = state
    text = repr(data)

    if len(text) > 160:
        text = text[:150] + f"...({len(text)} chars, digest {hashlib.blake2b(text.encode(), digest_size=6).hexdigest()})"
    return f"{kind} {text}" + (f" deadline={deadline:.0f}" if deadline is not None else "")


def norm_data(kind, data):
    if kind == "zset":
        return tuple(sorted((m, float(s)) for m, s in data))
    return data


def match_state(model, actual, t0: float, t1: float) -> bool:
    """True when `actual` (a dumped key read between t0 and t1, deadlines in absolute ms) is a
    state the model state `model` can show at that time."""

    lo, hi = t0 - TTL_PRESENCE_SLACK_MS, t1 + TTL_PRESENCE_SLACK_MS

    if isinstance(model, Alts):
        return any(match_state(option, actual, t0, t1) for option in model)
    if model is None:
        return actual is None
    kind, data, deadline = model

    if deadline is not None and deadline < lo:
        return actual is None
    key_may_vanish = deadline is not None and deadline <= hi

    if kind == "hash":
        required = {}
        optional = {}

        for field, value, fdl in data:
            if fdl is None or fdl > hi:
                required[field] = (value, fdl)
            elif fdl >= lo:
                optional[field] = (value, fdl)
        if not required and not optional:
            return actual is None
        if actual is None:
            return key_may_vanish or not required
        if actual[0] != "hash":
            return False
        got = {f: (v, d) for f, v, d in actual[1]}

        for field, (value, fdl) in required.items():
            if field not in got or got[field][0] != value:
                return False
            if not ttl_close(fdl, got[field][1]):
                return False
        for field, (value, fdl) in got.items():
            if field in required:
                continue
            if field not in optional or optional[field][0] != value:
                return False
        return ttl_close(deadline, actual[2])
    if actual is None:
        return key_may_vanish
    if actual[0] != kind:
        return False
    if norm_data(kind, actual[1]) != norm_data(kind, data):
        return False
    return ttl_close(deadline, actual[2])


def ttl_close(model_deadline, actual_deadline) -> bool:
    if model_deadline is None or actual_deadline is None:
        return model_deadline is None and actual_deadline is None
    return abs(model_deadline - actual_deadline) <= TTL_VALUE_SLACK_MS


def dump(port: int, keys: list[bytes], user=None, timeout: float = 30.0) -> tuple[dict, float, float, list]:
    """Reads every key atomically (one MULTI per key) and the full key list. Returns
    (states, t0, t1, unexpected keys). Deadlines are absolute unix ms."""

    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        client = None

        try:
            client = BlockingClient(port, timeout=10, user=user)
            t0 = time.time() * 1000
            commands = []

            for key in keys:
                commands += [
                    ["MULTI"],
                    ["TYPE", key],
                    ["PTTL", key],
                    ["GET", key],
                    ["LRANGE", key, 0, -1],
                    ["HGETALL", key],
                    ["SMEMBERS", key],
                    ["ZRANGE", key, 0, -1, "WITHSCORES"],
                    ["XRANGE", key, "-", "+"],
                    ["EXEC"],
                ]
            commands.append(["KEYS", "*"])
            replies = client.pipeline(commands)
            busy = [r for r in replies if isinstance(r, Err) and r.code in ("LOADING", "MASTERDOWN", "BUSY")]

            if busy:
                last_error = busy[0]
                client.close()
                time.sleep(0.1)
                continue
            states = {}
            hashes = []

            for index, key in enumerate(keys):
                exec_reply = replies[index * 10 + 9]

                if not isinstance(exec_reply, list) or len(exec_reply) != 8:
                    raise Failure("dump", f"bad EXEC reply for {key!r}: {exec_reply!r}")
                kind, pttl, s, lst, hsh, st, zs, xs = exec_reply
                t_key = time.time() * 1000

                if isinstance(kind, Err) or isinstance(pttl, Err):
                    raise Failure("dump", f"TYPE/PTTL failed for {key!r}: {kind!r} {pttl!r}")
                dl = None if pttl < 0 else t0 + pttl

                if kind == "none":
                    states[key] = None
                elif kind == "string":
                    states[key] = ("string", s, dl)
                elif kind == "list":
                    states[key] = ("list", tuple(lst), dl)
                elif kind == "hash":
                    states[key] = ("hash", dict(zip(hsh[::2], hsh[1::2])), dl)
                    hashes.append(key)
                elif kind == "set":
                    states[key] = ("set", frozenset(st), dl)
                elif kind == "zset":
                    states[key] = ("zset", tuple(sorted((m, float(sc)) for m, sc in zip(zs[::2], zs[1::2]))), dl)
                elif kind == "stream":
                    states[key] = ("stream", tuple((e[0], tuple(e[1])) for e in xs), dl)
                else:
                    raise Failure("dump", f"unexpected type {kind!r} for {key!r}")
                del t_key
            if hashes:
                commands = []

                for key in hashes:
                    fields = list(states[key][1].keys())
                    commands += [["HPTTL", key, "FIELDS", len(fields), *fields]]
                ttl_replies = client.pipeline(commands)

                for key, ttls in zip(hashes, ttl_replies):
                    _, values, dl = states[key]
                    fields = list(values.keys())

                    if isinstance(ttls, Err):
                        raise Failure("dump", f"HPTTL failed for {key!r}: {ttls!r}")
                    data = []

                    for field, ttl in zip(fields, ttls):
                        if ttl == -2:
                            continue  # expired between the two reads
                        data.append((field, values[field], None if ttl < 0 else t0 + ttl))
                    states[key] = ("hash", tuple(sorted(data)), dl) if data else None
            t1 = time.time() * 1000
            known = set(keys)
            unexpected = [k for k in replies[-1] if k not in known] if isinstance(replies[-1], list) else []
            client.close()
            return states, t0, t1, unexpected
        except (OSError, ConnectionError) as error:
            last_error = error
            if client:
                client.close()
            time.sleep(0.1)
    raise Failure("dump", f"could not read the keyspace on port {port}: {last_error}")


# ---------------------------------------------------------------------------------------------
# Operations and the cut checker


class Op:
    __slots__ = (
        "id", "conn", "label", "cmds", "keys", "write", "send_t", "recv_t", "send_ev", "recv_ev",
        "replies", "status", "pred", "effects", "base", "kind", "pair", "denied", "flex", "note",
        "shards",
    )

    def __init__(self, label, cmds, keys, write, kind="plain"):
        self.id = 0
        self.conn = None
        self.label = label
        self.cmds = cmds
        self.keys = keys
        self.write = write
        self.send_t = self.recv_t = None
        self.send_ev = self.recv_ev = None
        self.replies = []
        self.status = "pending"
        self.pred = None
        self.effects = {}
        self.base = {}
        self.kind = kind
        self.pair = None
        self.denied = False
        self.flex = False
        self.note = ""
        self.shards = set()

    def line(self) -> str:
        cmd = " | ".join(" ".join(short(x) for x in c) for c in self.cmds)
        reply = short_reply(self.replies)
        t = f"{self.send_t / 1000:.3f}" if self.send_t else "-"
        return f"#{self.id} c{self.conn} t={t} ev={self.send_ev}-{self.recv_ev} {self.status:<7} {cmd} -> {reply}"


def short(value) -> str:
    if isinstance(value, bytes):
        text = value.decode(errors="replace")
    else:
        text = str(value)
    return text if len(text) <= 40 else f"{text[:20]}..({len(text)})"


def short_reply(value) -> str:
    """The reply with each long string cut, so every element (a hash field, a list item)
    stays visible."""

    def cut(item):
        if isinstance(item, bytes) and len(item) > 40:
            return f"{item[:20]!r}..({len(item)})"
        if isinstance(item, list):
            return "[" + ", ".join(cut(x) for x in item) + "]"
        return repr(item)

    text = cut(value)
    return text if len(text) <= 2000 else text[:2000] + "..."


INF = float("inf")


class Entry:
    """One state change of one key: the op that made it and the state after it."""

    __slots__ = ("op", "state", "send_ev", "recv_ev")

    def __init__(self, op, state, send_ev=None, recv_ev=None):
        self.op = op
        self.state = state
        self.send_ev = op.send_ev if send_ev is None else send_ev
        self.recv_ev = op.recv_ev if recv_ev is None else recv_ev


def find_cut(keys, init, hist, actual, t0, t1, must, never):
    """Least cut (one prefix length per key) that explains `actual`, or a Failure.

    `must(entry)` marks entries the profile requires; `never(entry)` entries that may not be
    present. Every constraint is an upward implication, so raising each key to its smallest
    matching prefix until nothing changes finds the least solution if one exists."""

    reasons = {}
    candidates = {}
    position = collections.defaultdict(dict)  # op id -> key -> 1-based index
    ok_recv = {}
    p = {}

    for key in keys:
        entries = hist.get(key, [])
        upper = len(entries)

        for i, entry in enumerate(entries):
            position[entry.op.id][key] = i + 1

            if never(entry):
                upper = min(upper, i)
        states = [init.get(key)] + [e.state for e in entries]
        candidates[key] = [i for i in range(upper + 1) if match_state(states[i], actual.get(key), t0, t1)]
        lower = 0

        for i, entry in enumerate(entries):
            if must(entry):
                lower = i + 1
        ok_recv[key] = [(e.recv_ev, i + 1) for i, e in enumerate(entries) if e.op.status == "ok"]
        p[key] = 0
        reasons[key] = []
        raise_to(key, lower, p, candidates, reasons, hist, init, actual, "profile lower bound")

    changed = True

    while changed:
        changed = False
        frontier = -1

        for key in keys:
            for entry in hist.get(key, [])[: p[key]]:
                frontier = max(frontier, entry.send_ev)

                for other, index in position[entry.op.id].items():
                    if other != key and p[other] < index:
                        raise_to(other, index, p, candidates, reasons, hist, init, actual,
                                 f"atomic with #{entry.op.id} present on {short(key)}")
                        changed = True
        for key in keys:
            evs = ok_recv[key]
            need = 0

            for recv_ev, index in evs:
                if recv_ev < frontier:
                    need = index
                else:
                    break
            if p[key] < need:
                raise_to(key, need, p, candidates, reasons, hist, init, actual,
                         f"acknowledged before a later write (event {frontier}) that is present")
                changed = True
    return p


def raise_to(key, need, p, candidates, reasons, hist, init, actual, why):
    for index in candidates[key]:
        if index >= need:
            if index != p[key]:
                reasons[key].append(f"raised to {index}: {why}")
            p[key] = index
            return
    entries = hist.get(key, [])
    lines = [f"key {short(key)}: no state at or after prefix {need} matches ({why})",
             f"  actual: {describe(actual.get(key))}",
             f"  candidates: {candidates[key]}",
             f"  history ({len(entries)} entries, prefix 0 = {describe(init.get(key))}):"]
    start = max(0, need - 6)

    for i, entry in enumerate(entries[start:], start=start + 1):
        lines.append(f"   [{i}] #{entry.op.id} {entry.op.status} ev={entry.send_ev}-{entry.recv_ev} "
                     f"-> {describe(entry.state)}")
        if i > need + 12:
            lines.append("   ...")
            break
    lines += [f"  {r}" for r in reasons[key]]
    raise Failure("state", f"recovered value of {short(key)} matches no allowed prefix", "\n".join(lines))


# ---------------------------------------------------------------------------------------------
# Configurations


CONFIGS = {
    "memory-1": dict(profile="memory", shards=1),
    "everysec-1": dict(profile="everysec", shards=1),
    "everysec-2": dict(profile="everysec", shards=2),
    "always-1": dict(profile="always", shards=1, sync_worker="off"),
    "always-worker": dict(profile="always", shards=1, sync_worker="on"),
    "always-2": dict(profile="always", shards=2, sync_worker="off"),
    "snapshot-1": dict(profile="snapshot", shards=1),
    "snapshot-2": dict(profile="snapshot", shards=2),
    "replica": dict(profile="everysec", shards=1, replica=True),
    "replica-always": dict(profile="always", shards=1, replica=True, sync_worker="off"),
    "maxmemory": dict(profile="everysec", shards=1, maxmemory=True),
}


class Server:
    def __init__(self, checker, name, port, data_dir, role="off", repl_port=None, primary_port=None):
        self.checker = checker
        self.name = name
        self.port = port
        self.data_dir = data_dir
        self.role = role
        self.repl_port = repl_port
        self.primary_port = primary_port
        self.proc = None
        self.dir = data_dir.parent
        self.log_path = self.dir / f"{name}.log"
        self.config_path = self.dir / f"{name}.toml"
        self.control = self.dir / f"{name}.fault"
        self.fault_log = self.dir / f"{name}.faults"
        self.track_log = self.dir / f"{name}.track"
        self.starts = 0

    def write_config(self):
        c = self.checker
        lines = [
            f'address = "127.0.0.1:{self.port}"',
            f"shards = {c.cfg['shards']}",
            'metrics_address = ""',
            "shutdown_grace_ms = 5000",
            "[persistence]",
            f'profile = "{"memory" if c.args.mutant == "lose-writes" else c.cfg["profile"]}"',
            f'data_dir = "{self.data_dir}"',
            f"wal_segment_bytes = {c.params['wal_segment_bytes']}",
            f"sync_interval_ms = {c.params['sync_interval_ms']}",
            f"snapshot_interval_ms = {c.params['snapshot_interval_ms']}",
            f"snapshot_journal_bytes = {c.params['snapshot_journal_bytes']}",
        ]
        if self.role != "off":
            lines += ["[replication]", f'role = "{self.role}"']
            if self.role == "primary":
                lines.append(f'listen = "127.0.0.1:{self.repl_port}"')
            else:
                lines.append(f'primary = "127.0.0.1:{self.primary_port}"')
        if c.cfg.get("maxmemory"):
            lines += [
                "[memory]",
                f"max_shard_bytes = {c.params['max_shard_bytes']}",
                "reserved_headroom_bytes = 65536",
                'maxmemory_policy = "noeviction"',
            ]
        lines += [
            "[[security.users]]",
            'name = "limited"',
            'rules = "on >limited-secret ~{g0}* +@all -flushdb -flushall"',
        ]
        self.config_path.write_text("\n".join(lines) + "\n")

    def start(self, wait=True):
        self.write_config()
        self.control.unlink(missing_ok=True)
        env = dict(os.environ)
        env.pop("RUVIO_CONFIG", None)
        env["RUVIO_FAULT_CONTROL"] = str(self.control)
        env["RUVIO_FAULT_LOG"] = str(self.fault_log)
        env["RUVIO_TRACK_LOG"] = str(self.track_log)

        if self.checker.shim:
            env["LD_PRELOAD"] = str(self.checker.shim)
        env["RUVIO_SYNC_WORKER"] = self.checker.cfg.get("sync_worker", "auto")
        self.starts += 1

        with self.log_path.open("a") as log:
            log.write(f"\n===== model_check: start #{self.starts} at {time.time():.3f} =====\n")
        handle = self.log_path.open("a")
        # The server stays in the checker's process group, so killing that group (the Rust
        # smoke test does on a timeout) ends the servers too, and it dies with the checker.
        self.proc = subprocess.Popen(
            [self.checker.bin, "--config", str(self.config_path)],
            env=env,
            cwd=self.dir,
            stdout=handle,
            stderr=subprocess.STDOUT,
            preexec_fn=die_with_parent,
        )
        handle.close()

        if wait:
            self.wait_ready()

    def wait_ready(self, timeout=60.0):
        end = time.time() + timeout

        while time.time() < end:
            if self.proc.poll() is not None:
                raise Failure("startup", f"{self.name} exited with {self.proc.returncode} during startup")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.5) as sock:
                    sock.settimeout(2)
                    sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                    data = sock.recv(64)

                    if data.startswith(b"+PONG"):
                        return
            except OSError:
                pass
            time.sleep(0.03)
        raise Failure("startup", f"{self.name} did not answer PING within {timeout:.0f} s")

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
        if self.proc:
            self.proc.wait(timeout=20)
        self.log_marker("SIGKILL")

    def sigterm(self, timeout=30.0):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.send_signal(signal.SIGKILL)
                self.proc.wait(timeout=20)
                raise Failure("shutdown", f"{self.name} did not exit within {timeout:.0f} s of SIGTERM")
        self.log_marker(f"SIGTERM, exit {self.proc.returncode if self.proc else '?'}")

    def log_marker(self, text):
        with self.log_path.open("a") as log:
            log.write(f"===== model_check: {text} at {time.time():.3f} =====\n")

    def set_fault(self, line: str | None):
        if line is None:
            self.control.unlink(missing_ok=True)
        else:
            tmp = self.control.with_suffix(".tmp")
            tmp.write_text(line + "\n")
            tmp.replace(self.control)
        time.sleep(0.004)  # the shim re-reads the control file at most once per millisecond

    def faults_since(self, since_ms: float) -> list[tuple[float, str, str, str, int]]:
        """Injected failures as (unix ms, call, errno, path, file size at the call)."""

        if not self.fault_log.exists():
            return []
        out = []
        # The shim logs whole milliseconds: a failure in the same millisecond as `since_ms`
        # is logged at its floor.
        since_ms = math.floor(since_ms)

        for line in self.fault_log.read_text().splitlines():
            parts = line.split(" ")

            if len(parts) == 5 and float(parts[0]) >= since_ms:
                out.append((float(parts[0]), parts[1], parts[2], parts[3], int(parts[4])))
        return out

    def panicked(self) -> str | None:
        if not self.log_path.exists():
            return None
        text = self.log_path.read_text(errors="replace")

        for marker in ("panicked at", "RUST_BACKTRACE", "fatal runtime error"):
            if marker in text:
                index = text.index(marker)
                return text[max(0, index - 300) : index + 600]
        return None


class Conn:
    def __init__(self, index, port, user=None):
        self.index = index
        self.port = port
        self.user = user
        self.sock = None
        self.buf = bytearray()
        self.pending = collections.deque()
        self.dead = False
        self.limited = user is not None

    def connect(self):
        self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if self.user:
            self.sock.settimeout(5)
            client_buf = bytearray()
            self.sock.sendall(encode(["AUTH", self.user[0], self.user[1]]))

            while b"\r\n" not in client_buf:
                chunk = self.sock.recv(256)

                if not chunk:
                    raise ConnectionError("closed during AUTH")
                client_buf += chunk
            if not client_buf.startswith(b"+OK"):
                raise ConnectionError(f"AUTH refused: {bytes(client_buf)!r}")
        self.sock.setblocking(False)
        self.buf = bytearray()
        self.pending.clear()
        self.dead = False

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None


class Checker:
    def __init__(self, args, name, seed, duration, ports, work, results):
        self.args = args
        self.name = name
        self.cfg = CONFIGS[name]
        self.seed = seed
        self.duration = duration
        self.ports = ports
        self.work = work
        self.results = results
        self.bin = str(Path(args.bin).resolve())
        self.rng = random.Random(f"{seed}:{name}")
        # The epochs and their events come from their own generator: how many random numbers
        # the commands take depends on timing (which keys are free), the schedule must not.
        self.schedule = random.Random(f"{seed}:{name}:schedule")
        self.shards = self.cfg["shards"]
        self.shim = args.shim
        self.op_seq = 0
        self.ev = 0
        self.log = collections.deque(maxlen=args.log_tail)
        self.notes = []
        self.stats = collections.Counter()
        # Explicit stream ids only have to grow; a constant start keeps the values the seed's.
        self.stream_seq = 1_000_000_000_000
        self.value_seq = 0
        rng = self.rng
        profile = self.cfg["profile"]
        self.params = dict(
            # Big values (maxmemory) need records below the segment size.
            wal_segment_bytes=rng.choice([1 << 20, 256 << 20] if self.cfg.get("maxmemory") else
                                         [65536, 1 << 20, 256 << 20]),
            sync_interval_ms=rng.choice([100, 300, 1000]) if profile == "everysec" else 1000,
            snapshot_interval_ms=rng.choice([0, 300, 1500]) if profile != "memory" else 0,
            snapshot_journal_bytes=rng.choice([4096, 65536, 64 << 20]),
            # Small enough that the big values of the maxmemory config reach it.
            max_shard_bytes=rng.choice([768 << 10, 1536 << 10]),
            clients=rng.randint(3, 6),
        )
        if args.wal_segment_bytes:
            self.params["wal_segment_bytes"] = args.wal_segment_bytes
        self.keyspace()

    # -- keyspace
    def keyspace(self):
        groups = 6 if not self.cfg.get("maxmemory") else 8
        self.groups = [[f"{{g{g}}}k{j}".encode() for j in range(5)] for g in range(groups)]
        self.keys = [k for group in self.groups for k in group]
        self.probe_keys = {}
        n = 0

        while len(self.probe_keys) < self.shards:
            key = f"{{p{n}}}probe".encode()
            self.probe_keys.setdefault(shard_of(key, self.shards), key)
            n += 1
        self.all_keys = self.keys + sorted(self.probe_keys.values())
        self.shard = {k: shard_of(k, self.shards) for k in self.all_keys}

    # -- logging
    def note(self, text):
        line = f"[{time.time():.3f}] {text}"
        self.log.append("## " + text)
        self.notes.append(line)

        if self.args.verbose:
            print(f"  {self.name}: {text}", flush=True)

    def next_value(self, prefix="v") -> bytes:
        self.value_seq += 1
        prefix = prefix.decode() if isinstance(prefix, bytes) else prefix
        return f"{prefix}{self.value_seq}".encode()

    # -- servers
    def setup_servers(self):
        base = self.ports
        root = self.work / self.name
        shutil.rmtree(root, ignore_errors=True)
        (root / "primary").mkdir(parents=True)
        self.root = root

        assert self.shards <= REPLICA_PORT_OFFSET, "the port block holds five shards per server"
        if self.cfg.get("replica"):
            repl_port = base + REPLICATION_PORT_OFFSET
            self.primary = Server(self, "primary", base, root / "primary" / "data", "primary", repl_port=repl_port)
            (root / "replica").mkdir(parents=True)
            self.replica = Server(self, "replica", base + REPLICA_PORT_OFFSET, root / "replica" / "data", "replica",
                                  primary_port=repl_port)
        else:
            self.primary = Server(self, "server", base, root / "primary" / "data")
            self.replica = None

    # -- main
    def run(self):
        self.setup_servers()
        self.primary.start()

        if self.replica:
            self.replica.start()
        self.state = {k: None for k in self.all_keys}
        self.new_epoch(time.time() * 1000)
        plan = self.plan_epochs()
        self.note(f"plan: {len(plan)} epochs of {', '.join(str(ops) for ops in plan)} commands")
        epoch = 0

        try:
            for ops in plan:
                epoch += 1
                self.epoch_number = epoch
                self.one_epoch(ops)
                self.housekeeping()
        finally:
            for server in (self.primary, self.replica):
                if server and server.alive():
                    server.kill9()
        self.stats["epochs"] = epoch

    def plan_epochs(self) -> list[int]:
        """Commands per epoch, from the seed alone: epochs of 0.8 to 4 nominal seconds until
        --duration is used up."""

        plan = []
        left = self.duration

        while left > 0:
            seconds = min(self.schedule.uniform(0.8, 4.0), max(0.3, left))
            plan.append(max(1, round(seconds * OPS_PER_SECOND)))
            left -= seconds
        return plan

    def housekeeping(self):
        size = sum(f.stat().st_size for f in self.root.rglob("*") if f.is_file())

        if size > self.args.max_data_mb << 20:
            self.note(f"data dir is {size >> 20} MiB, taking a SAVE to trim the WAL")
            client = BlockingClient(self.primary.port)
            client.call("SAVE")
            client.close()

    def new_epoch(self, now_ms):
        """The current model state becomes the base; histories restart."""

        self.init = dict(self.state)
        self.hist = collections.defaultdict(list)
        self.saves = []  # (send_ev, recv_ev, ok)
        self.refused_saves = []  # (send_t, recv_t) of each SAVE refused with an error
        self.ambiguous = set()
        self.epoch_start = now_ms
        self.fault = None  # dict when a disk fault is active this epoch
        self.refused_any = False
        self.writes_blocked = False

    # -- connections
    def connect_all(self):
        self.conns = []
        n = self.params["clients"]

        for i in range(n):
            conn = Conn(i, self.primary.port)
            conn.connect()
            self.conns.append(conn)
        limited = Conn(n, self.primary.port, user=("limited", "limited-secret"))
        limited.connect()
        self.conns.append(limited)
        self.locks = collections.Counter()
        self.outstanding = 0
        self.pairs = []  # BLPOP pushes waiting to be sent: (time, op)

    def close_all(self):
        for conn in self.conns:
            conn.close()

    # -- one epoch: workload, then an event
    def one_epoch(self, ops):
        rng = self.schedule
        self.connect_all()
        self.depth = rng.choice([1, 1, 2, 4])
        events = ["kill9"] * 4 + ["sigterm"] * 2 + ["check"] * 2
        profile = self.cfg["profile"]

        if profile != "memory" and self.shim and not self.cfg.get("replica") and not self.args.no_faults:
            events += ["disk"] * 3

            if profile in ("everysec", "always"):
                events += ["powerloss"] * 2 + ["tear"] * 2
        if self.cfg.get("replica"):
            events += ["kill9-replica"] * 2 + ["sigterm-replica"]
        if self.args.events:
            events = [e for e in events if e in self.args.events] or [self.args.events[0]]
        event = rng.choice(events)
        first_op = self.op_seq
        fault_at = first_op + round(ops * rng.uniform(0.3, 0.8)) if event == "disk" else None
        stop_at = first_op + ops
        how = "kill9" if event == "kill9" or rng.random() < 0.6 else "sigterm"
        self.note(f"epoch {self.epoch_number}: {ops} commands, then {event}")
        self.stats[f"event:{event}"] += 1

        if event == "powerloss":
            # Record what reaches the WAL and what is synced, to drop the rest at the kill.
            self.primary.track_log.unlink(missing_ok=True)
            self.primary.set_fault(f"track - {self.primary.data_dir}/wal/")

        # Counted in commands sent, not seconds: the schedule is the seed's on any machine.
        # Something must be sent now and then; a stall that long is a failure, as a reply
        # that late is.
        progress = (self.op_seq, time.time())

        while self.op_seq < stop_at:
            if fault_at is not None and self.op_seq >= fault_at:
                fault_at = None
                self.start_disk_fault()
            self.pump(0.002)
            self.check_alive()

            if self.fault and self.fault.get("ended"):
                break
            if self.op_seq != progress[0]:
                progress = (self.op_seq, time.time())
            elif time.time() - progress[1] > self.args.hang_seconds:
                raise Failure("stall", f"no command could be sent for {self.args.hang_seconds} s "
                              f"({self.op_seq - first_op} of {ops} in epoch {self.epoch_number})",
                              self.probe_server())
        if self.fault:
            self.finish_disk_fault()
        if event in ("kill9", "disk"):
            self.restart_primary(how)
        elif event in ("powerloss", "tear"):
            self.restart_primary(event)
        elif event == "sigterm":
            self.restart_primary("sigterm")
        elif event == "check":
            self.drain()
            self.live_check()
        elif event in ("kill9-replica", "sigterm-replica"):
            self.drain()
            self.close_all()
            self.collect_unknown()
            self.note(f"{event}")
            if event == "kill9-replica":
                self.replica.kill9()
            else:
                self.replica.sigterm()
            self.replica.start()
            self.live_check(connected=False)
        if self.replica:
            self.replica_converges()

    def check_alive(self):
        for server in (self.primary, self.replica):
            if server and not server.alive():
                raise Failure("crash", f"{server.name} exited unexpectedly with {server.proc.returncode}")

    # -- issuing
    def pump(self, timeout):
        self.fill()
        socks = [c.sock for c in self.conns if c.pending and not c.dead]
        readable = []

        if socks:
            try:
                readable, _, _ = select.select(socks, [], [], timeout)
            except (OSError, ValueError):
                readable = socks
        else:
            time.sleep(timeout)
        by_sock = {c.sock: c for c in self.conns if c.sock}

        for sock in readable:
            conn = by_sock.get(sock)

            if conn is None or conn.dead:
                continue
            try:
                chunk = sock.recv(1 << 20)
            except BlockingIOError:
                continue
            except OSError:
                chunk = b""
            if not chunk:
                self.conn_died(conn)
                continue
            conn.buf += chunk
            self.consume(conn)
        now = time.time()

        for when, op in list(self.pairs):
            if now >= when:
                self.pairs.remove((when, op))
                self.send(op, self.pick_other_conn(op.pair.conn))

        for conn in self.conns:
            if conn.pending and not conn.dead:
                op = conn.pending[0]
                limit = self.args.hang_seconds * 1000 + (1000 if op.kind == "pair-pop" else 0)

                if now * 1000 - op.send_t > limit:
                    raise Failure("hang", f"#{op.id} got no reply within {self.args.hang_seconds} s",
                                  op.line() + "\n\n" + self.probe_server())

    def consume(self, conn):
        pos = 0

        while conn.pending:
            op = conn.pending[0]

            try:
                value, pos = parse_one(conn.buf, pos)
            except Incomplete:
                break
            op.replies.append(value)

            if len(op.replies) == len(op.cmds):
                conn.pending.popleft()
                self.complete(op)
        del conn.buf[:pos]

    def conn_died(self, conn):
        conn.dead = True
        ops = list(conn.pending)
        conn.pending.clear()

        if self.tearing:
            # The server is dying on purpose; restart_primary settles the rest.
            for op in ops:
                self.finish_unknown(op)
            conn.close()
            return

        self.note(f"connection {conn.index} closed by the server with {len(ops)} commands in flight")
        self.stats["conn_died_in_fault" if self.fault else "conn_died"] += 1
        for op in ops:
            self.finish_unknown(op)
        conn.close()

        try:
            conn.connect()
        except OSError as error:
            raise Failure("connection", f"cannot reconnect connection {conn.index}: {error}")
        if not self.fault:
            raise Failure("connection", f"the server closed connection {conn.index} outside a fault",
                          "\n".join(op.line() for op in ops))

    def pick_other_conn(self, index):
        others = [c for c in self.conns if c.index != index and not c.limited and not c.dead]
        return min(others, key=lambda c: len(c.pending))

    def fill(self):
        if self.fault and self.fault.get("ended"):
            return
        for conn in self.conns:
            if conn.dead or len(conn.pending) >= self.depth:
                continue
            if self.barrier is not None:
                break
            op = self.generate(conn)

            if op is None:
                continue
            if op.kind == "barrier":
                self.barrier = op
                break
            self.send(op, conn)
        if self.barrier is not None and self.outstanding == 0 and not self.pairs:
            op = self.barrier
            self.barrier = None
            conn = self.conns[0] if not self.conns[0].dead else self.pick_other_conn(-1)
            self.send(op, conn)

    barrier = None

    def free(self, keys) -> bool:
        now = time.time() * 1000

        for key in keys:
            if self.locks[key] or key in self.ambiguous:
                return False
            for deadline in deadlines_of(self.state.get(key)):
                if now - QUARANTINE_MS <= deadline <= now + QUARANTINE_MS:
                    return False
        return True

    def send(self, op, conn):
        self.op_seq += 1
        self.ev += 1
        op.id = self.op_seq
        op.conn = conn.index
        op.send_t = time.time() * 1000
        op.send_ev = self.ev
        op.shards = {self.shard[k] for k in op.keys} if op.kind != "barrier" else {0}

        for key in op.keys:
            self.locks[key] += 1
        if op.kind != "pair-push":
            op.base = {k: self.state.get(k) for k in op.keys}
        self.predict(op, op.send_t)
        self.outstanding += 1
        conn.pending.append(op)

        try:
            conn.sock.sendall(b"".join(encode(c) for c in op.cmds))
        except BlockingIOError:
            conn.sock.setblocking(True)
            conn.sock.sendall(b"".join(encode(c) for c in op.cmds))
            conn.sock.setblocking(False)
        except OSError:
            pass  # the reply loop sees the closed connection
        self.stats["sent"] += 1

    def predict(self, op, now):
        """Fills op.pred (expected replies) and op.effects (changed key states) at time `now`."""

        if op.kind in ("pair-pop", "pair-push", "save"):
            return
        view = dict(op.base)
        engine = Engine(view, now, keys_all=op.keys)

        if op.denied:
            op.pred = None
            op.effects = {}
            return
        if op.kind == "multi":
            results = [engine.run(c) for c in op.cmds[1:-1]]
            op.pred = ["OK"] + ["QUEUED"] * (len(op.cmds) - 2) + [results]
        else:
            op.pred = [engine.run(c) for c in op.cmds]
        op.effects = {k: alive(v, now) if v is not None and v[0] == "hash" else v
                      for k, v in view.items() if v != op.base.get(k)}

    # -- completion
    def complete(self, op):
        if any(isinstance(r, Err) and "did not confirm this write" in r.msg for r in flatten(op.replies)):
            # The log write failed: the command may or may not be applied, like a dead connection.
            if not self.fault:
                raise Failure("refused", f"#{op.id} {op.label}: the server could not confirm a write "
                              "while no fault was injected", op.line())
            self.stats["unconfirmed"] += 1
            self.finish_unknown(op)
            return
        self.ev += 1
        op.recv_ev = self.ev
        op.recv_t = time.time() * 1000
        self.outstanding -= 1
        persistence = [r for r in flatten(op.replies) if isinstance(r, Err) and r.code in ("MISCONF",)]
        oom = [r for r in flatten(op.replies) if isinstance(r, Err) and (r.code == "OOM" or "OOM" in r.msg)]

        if op.kind == "save":
            self.log.append(op.line())
            self.complete_save(op)
            self.unlock(op)
            return
        if op.kind in ("pair-pop", "pair-push"):
            self.complete_pair_part(op)
            self.log.append(op.line())
            return
        if persistence:
            op.status = "refused"
            self.refused_any = True
            self.stats["refused"] += 1
            self.on_refused(op)
            self.log.append(op.line())
            self.unlock(op)
            return
        if self.cfg.get("maxmemory") and op.label == "EVAL" and \
                any(isinstance(r, Err) and "Error running script" in r.msg for r in op.replies):
            # An OOM inside a script surfaces as a generic script error.
            self.stats["deviation:script-oom-masked"] += 1
            oom = True
        if oom and op.kind == "plain" and op.cmds[0][0].upper() in RUNS_WHEN_FULL:
            op.status = "oom"
            self.log.append(op.line())
            raise Failure("oom", f"#{op.id}: {op.label} was refused with OOM; a command that only "
                          "frees memory must run on a full store", op.line())
        if oom and self.cfg.get("maxmemory") and op.kind != "multi":
            op.status = "oom"
            self.stats["oom"] += 1
            self.log.append(op.line())
            self.unlock(op)
            return
        if op.denied:
            self.complete_denied(op)
            self.unlock(op)
            return
        options = self.outcomes(op, op.replies)

        if not options:
            self.predict(op, op.send_t)
            op.status = "ok"
            self.log.append(op.line())
            raise Failure(
                "reply",
                f"#{op.id} {op.label}: the server's reply differs from the model",
                f"command: {op.line()}\npredicted: {op.pred!r}\nkey states before: "
                + "; ".join(f"{short(k)}={describe(v)}" for k, v in op.base.items()),
            )
        op.status = "ok"
        self.stats["ok"] += 1

        if len(options) == 1:
            op.effects = options[0]
            self.commit(op)
        else:
            self.commit_alternatives(op, options, op.recv_ev)
        self.log.append(op.line())
        self.unlock(op)

    def outcomes(self, op, replies):
        """The distinct effects of `op` that explain `replies` (None: any reply). The command ran
        somewhere between its send and its reply; when a deadline of one of its keys falls in
        that window, it may have run before or after the key (or field) expired."""

        lo, hi = op.send_t - TTL_PRESENCE_SLACK_MS, op.recv_t + TTL_PRESENCE_SLACK_MS
        near = sorted({d for v in op.base.values() for d in deadlines_of(v) if lo <= d <= hi})
        times = [op.send_t]

        if near:
            times = [lo] + [t for d in near for t in (d - 1, d + 1)] + [hi]
        if op.recv_t - op.send_t > TTL_VALUE_SLACK_MS / 2:
            # A slow command (held behind a SAVE or a disk fault) sets relative deadlines from
            # when it ran, not from when it was sent.
            step = TTL_VALUE_SLACK_MS / 2
            times += [op.send_t + step * i for i in range(1, int((op.recv_t - op.send_t) / step) + 1)]
            times.append(op.recv_t)
        options = []

        for now in times:
            self.predict(op, now)

            if replies is None or same_reply(op.pred, replies):
                if op.effects not in options:
                    options.append(op.effects)
        if near:
            op.note = "deadline during command"
        return options

    def commit_alternatives(self, op, options, recv_ev):
        """Several outcomes explain the reply: each key gets every possible state and is not
        used again until a check settles it."""

        if len(options) > 1 and op.status != "refused":
            self.stats["ambiguous_ttl"] += 1
        for key in sorted({k for effects in options for k in effects}):
            states = tuple(dict.fromkeys(effects.get(key, op.base.get(key)) for effects in options))
            state = states[0] if len(states) == 1 else Alts(states)
            self.hist[key].append(Entry(op, state, recv_ev=recv_ev))
            self.state[key] = states[0]
            self.ambiguous.add(key)

    def unlock(self, op):
        for key in op.keys:
            self.locks[key] -= 1

    def commit(self, op):
        for key, state in op.effects.items():
            self.state[key] = state
            self.hist[key].append(Entry(op, state))

    def on_refused(self, op):
        """A persistence error: the op may or may not survive; its keys become ambiguous. A
        refused write can be rolled back while later writes go through (a write error is not
        sticky), so a later write being present does not imply this one is: each key may also
        keep its state from before the op."""

        self.commit_alternatives(op, self.outcomes(op, None) + [{}], op.recv_ev)

        if not self.fault:
            raise Failure("refused", f"#{op.id} was refused with a persistence error while no fault was injected",
                          op.line())
        self.writes_blocked = True

    def finish_unknown(self, op):
        self.ev += 1
        op.recv_ev = None
        op.status = "unknown"
        self.outstanding -= 1

        if op.kind in ("pair-pop", "pair-push"):
            self.complete_pair_part(op)
        elif op.kind != "save" and not op.denied:
            op.recv_t = time.time() * 1000
            self.commit_alternatives(op, self.outcomes(op, None), INF)
        elif op.denied:
            # A denied MULTI may still run its allowed commands (see complete_denied).
            if op.kind == "multi":
                view = dict(op.base)
                engine = Engine(view, op.send_t)

                for argv in op.cmds[1:-1]:
                    if all(self.shard_ok_for_limited(k) for k in argv[1:2]):
                        engine.run(argv)
                for key, state in view.items():
                    if state != op.base.get(key):
                        self.hist[key].append(Entry(op, state, recv_ev=INF))
            self.ambiguous.update(op.keys)
        self.log.append(op.line())
        self.unlock(op)

    def shard_ok_for_limited(self, key) -> bool:
        return key.startswith(b"{g0}")

    def complete_denied(self, op):
        replies = op.replies
        flat = flatten(replies)

        if op.kind == "multi":
            # The denied command is refused at queue time. Redis then aborts EXEC; record which
            # behaviour the server shows and model the commands that ran.
            exec_reply = replies[-1]

            if isinstance(exec_reply, Err):
                op.status = "ok"
                self.log.append(op.line())
                return
            if isinstance(exec_reply, list):
                if "NOPERM inside MULTI does not abort EXEC" not in " ".join(self.notes):
                    self.note("NOPERM inside MULTI does not abort EXEC; the other queued commands run")
                self.stats["deviation:multi-noperm"] += 1
                ran = [c for c, r in zip(op.cmds[1:-1], replies[1:-1]) if r == "QUEUED"]
                view = dict(op.base)
                engine = Engine(view, op.send_t)
                expect = []

                for argv, actual in zip(ran, exec_reply):
                    before = dict(view)
                    expect.append(engine.run(argv))

                    if isinstance(actual, Err) and actual.code == "OOM" and self.cfg.get("maxmemory"):
                        if argv[0].upper() in RUNS_WHEN_FULL:
                            raise Failure("oom", f"#{op.id}: {argv[0].decode()} inside EXEC was refused "
                                          "with OOM; a command that only frees memory must run",
                                          op.line())
                        view.clear()
                        view.update(before)
                        expect[-1] = actual

                if not same_reply(expect, exec_reply):
                    raise Failure("reply", f"#{op.id}: EXEC after a refused command ran something unexpected",
                                  op.line())
                op.effects = {k: v for k, v in view.items() if v != op.base.get(k)}
                op.status = "ok"
                self.commit(op)
                self.log.append(op.line())
                return
        if not any(isinstance(r, Err) and r.code == "NOPERM" for r in flat):
            op.status = "ok"
            self.log.append(op.line())
            raise Failure("acl", f"#{op.id}: a command outside the user's ACL was not refused with NOPERM",
                          op.line())
        op.status = "ok"
        self.log.append(op.line())

    # BLPOP and its pusher: the pop is applied after the push when BLPOP returns the element.
    def complete_pair_part(self, op):
        if op.status == "pending":
            op.status = "done"
        pop = op if op.kind == "pair-pop" else op.pair
        push = pop.pair

        if pop.status == "pending" or push.status == "pending":
            return
        self.resolve_pair(pop, push)

    def part_outcome(self, op):
        """'ok', 'refused', 'unknown' or 'unsent' for one half of a BLPOP pair."""

        if op.status in ("unknown", "unsent"):
            return op.status
        reply = op.replies[0]

        if isinstance(reply, Err) and reply.code == "MISCONF":
            return "refused"
        if isinstance(reply, Err) and self.cfg.get("maxmemory") and (reply.code == "OOM" or "OOM" in reply.msg):
            if op.kind == "pair-pop":
                raise Failure("oom", f"#{op.id}: BLPOP was refused with OOM; a pop must run on a full "
                              "store", op.line())
            self.stats["oom"] += 1
            return "oom"
        return "ok"

    def resolve_pair(self, pop, push):
        key = pop.keys[0]
        push.status = self.part_outcome(push)
        pop.status = self.part_outcome(pop)
        self.unlock(pop)

        if push.status != "unsent":
            self.unlock(push)
        if push.status in ("unsent", "oom"):
            # Never pushed (or refused by maxmemory): the list stays empty.
            if pop.status == "ok" and pop.replies[0] is not None:
                raise Failure("reply", f"#{pop.id}: BLPOP returned an element nobody pushed", pop.line())
            return
        value = push.cmds[0][2]

        if push.status == "ok":
            if push.replies[0] not in (0, 1):
                raise Failure("reply", f"#{push.id}: RPUSH to an empty list replied {push.replies[0]!r}",
                              push.line())
            if push.replies[0] == 0:
                self.stats["deviation:push-to-blocked-replies-0"] += 1
        popped = None

        if pop.status == "oom":
            popped = False
        elif pop.status == "ok":
            reply = pop.replies[0]

            if reply is None:
                popped = False
            elif reply == [key, value]:
                popped = True
            else:
                raise Failure("reply", f"#{pop.id}: BLPOP returned {reply!r}, expected [{key!r}, {value!r}] or nil",
                              pop.line())
        pushed = ("list", (value,), None)
        self.hist[key].append(Entry(push, pushed, recv_ev=push.recv_ev if push.status == "ok" else INF))

        if popped is not False:
            self.hist[key].append(Entry(pop, None, send_ev=push.send_ev,
                                        recv_ev=pop.recv_ev if pop.status == "ok" else INF))
        if push.status == "ok" and popped is not None:
            self.state[key] = None if popped else pushed
        else:
            self.ambiguous.add(key)
        if "refused" in (push.status, pop.status):
            self.refused_any = True
            self.writes_blocked = True

    def complete_save(self, op):
        reply = op.replies[0] if op.replies else None
        ok = reply in ("OK", "Background saving started")

        if not ok and not (isinstance(reply, Err)):
            raise Failure("reply", f"#{op.id}: unexpected {op.label} reply {reply!r}", op.line())
        if isinstance(reply, Err) and not self.fault and reply.code not in ("ERR",):
            raise Failure("reply", f"#{op.id}: {op.label} failed without a fault: {reply!r}", op.line())
        if isinstance(reply, Err) and not self.fault:
            # A checkpoint already running is a legitimate refusal; anything else is not.
            if "progress" not in reply.msg and "already" not in reply.msg:
                raise Failure("reply", f"#{op.id}: {op.label} failed without a fault: {reply!r}", op.line())
        op.status = "ok" if ok else "refused"

        if ok and op.label == "SAVE" and self.fault and self.fault.get("target") in ("checkpoints", "both") \
                and "removed" not in self.fault:
            hits = [f for f in self.primary.faults_since(op.send_t - 1) if "checkpoints" in f[3] and f[0] <= op.recv_t]
            hits = self.unexplained_checkpoint_hits(op, hits)

            if hits:
                raise Failure("fsync-ok", f"#{op.id}: {op.label} returned OK although a checkpoint "
                              f"{hits[0][1]} failed with {hits[0][2]} while it ran",
                              f"{op.line()}\nsent {op.send_t:.0f} replied {op.recv_t:.0f} (unix ms)\n"
                              f"fault: {self.fault['line']} from {self.fault['started']:.0f}\n"
                              + "\n".join(" ".join(map(str, h)) for h in hits[:10]))
        if not ok and op.label == "SAVE":
            self.refused_saves.append((op.send_t, op.recv_t))
        # BGSAVE answers when the checkpoint starts, so only SAVE sets a durability bound.
        self.saves.append((op.send_ev, op.recv_ev, ok and op.label == "SAVE", op.id))

    def newest_published_during(self, op):
        """The newest generation whose MANIFEST was written while `op` waited (after it was
        sent, before its reply came back), else None. A round published after the reply
        does not count: it cannot be the one that answered `op`. Read when `op` completes."""

        newest = None

        try:
            current = int.from_bytes((self.primary.data_dir / "CURRENT").read_bytes()[8:16], "little")
            entries = list((self.primary.data_dir / "checkpoints").iterdir())
        except OSError:
            return None
        for entry in entries:
            try:
                generation = int(entry.name)
                written = (entry / "MANIFEST").stat().st_mtime * 1000
            except (OSError, ValueError):
                continue
            # The reply time is taken a moment after the reply was read: 1 ms of slack.
            if generation <= current and op.send_t <= written <= op.recv_t + 1 \
                    and (newest is None or generation > newest):
                newest = generation
        return newest

    def unexplained_checkpoint_hits(self, op, hits):
        """The checkpoint faults during an acknowledged SAVE that its own checkpoint may have
        hit. A shard runs one checkpoint at a time, so a SAVE sent after this one runs after
        it, in a newer generation: when such a SAVE was refused and a generation's faults fall
        inside its wait, the newest such generation is its failure, not this one's. Each
        refused SAVE explains one generation."""

        by_generation = collections.defaultdict(list)

        for hit in hits:
            m = re.search(r"/checkpoints/(\d+)/", hit[3])
            by_generation[int(m.group(1)) if m else -1].append(hit)
        # Several shards: the round that answered the SAVE was published while it waited,
        # so it is at or before the newest one published then, and a newer generation that
        # failed is a later checkpoint (a pending BGSAVE or scheduled round) that a shard
        # began in the moment before the reply went out. (One shard: see below.)
        published = self.newest_published_during(op) if self.shards > 1 else None

        if published is not None:
            for generation in [g for g in by_generation if g > published]:
                del by_generation[generation]
        later = [(s, r) for s, r in self.refused_saves if s > op.send_t]

        for generation in sorted(by_generation, reverse=True):
            if generation < 0:
                break
            first = by_generation[generation][0][0]
            claim = next((w for w in later if w[0] <= first <= w[1]), None)

            if claim is None:
                break
            later.remove(claim)
            del by_generation[generation]
        published = self.published_during(op) if self.shards == 1 else None

        if published is not None:
            # One shard writes the SAVE's checkpoint before it replies. A generation published
            # while the SAVE waited was its own, and a newer one (a scheduled checkpoint that
            # started before the reply was flushed) is not the SAVE's.
            for generation in [g for g in by_generation if g > published]:
                del by_generation[generation]
        return [h for g in sorted(by_generation) for h in by_generation[g]]

    def published_during(self, op):
        """The oldest checkpoint generation whose MANIFEST was written between the op's send
        and its reply, or None."""

        found = []

        for manifest in Path(self.primary.data_dir, "checkpoints").glob("*/MANIFEST"):
            try:
                written = manifest.stat().st_mtime * 1000
            except FileNotFoundError:
                continue
            if op.send_t - 1 <= written <= op.recv_t + 1 and manifest.parent.name.isdigit():
                found.append(int(manifest.parent.name))
        return min(found, default=None)

    # -- generation
    def generate(self, conn):
        rng = self.rng

        if conn.limited:
            if rng.random() < 0.85:
                return None
            return self.gen_limited()
        if self.writes_blocked:
            return self.gen_read()
        w = rng.random()

        if w < 0.004 and not self.cfg.get("maxmemory"):
            # Every connection is on the first shard's port, and in per-shard mode FLUSHDB
            # empties only the shard it arrives on.
            own = [key for key in self.all_keys if self.shard[key] == 0]
            return Op("FLUSHDB", [[b"FLUSHDB"]], own, True, kind="barrier")
        if w < 0.012:
            label = rng.choice(["SAVE", "BGSAVE"])
            return Op(label, [[label.encode()]], [], True, kind="save")
        if w < 0.03:
            return self.gen_pair(conn)
        if w < 0.08 and not self.cfg.get("maxmemory"):
            return self.gen_multi()
        if w < 0.11:
            return self.gen_eval()
        if w < 0.13:
            return self.gen_rename()
        if w < 0.35:
            return self.gen_read()
        return self.gen_write()

    def pick_key(self, group=None):
        rng = self.rng
        group = group if group is not None else rng.randrange(len(self.groups))
        return rng.choice(self.groups[group])

    def command_for(self, key, write: bool):
        """A random single-key command (argv) on `key`."""

        rng = self.rng
        state = alive(self.state.get(key), time.time() * 1000)
        kind = state[0] if state else rng.choice(["string", "list", "hash", "set", "zset", "stream"])

        if rng.random() < 0.1:
            kind = rng.choice(["string", "list", "hash", "set", "zset", "stream"])  # WRONGTYPE now and then
        if not write:
            return {
                "string": [b"GET", key],
                "list": [b"LRANGE", key, b"0", b"-1"],
                "hash": [b"HGETALL", key],
                "set": [b"SMEMBERS", key],
                "zset": [b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"],
                "stream": rng.choice([[b"XLEN", key], [b"XRANGE", key, b"-", b"+"]]),
            }[kind] if rng.random() < 0.85 else rng.choice([[b"PTTL", key], [b"TYPE", key], [b"EXISTS", key]])
        r = rng.random()

        if r < 0.06:
            return [b"DEL", key]
        if r < 0.10:
            return [b"PEXPIRE", key, str(rng.randint(700, 3000)).encode()]
        if r < 0.12:
            return [b"EXPIRE", key, str(rng.randint(1, 3)).encode()]
        if r < 0.15:
            return [b"PERSIST", key]
        big = self.cfg.get("maxmemory") and rng.random() < 0.6
        value = self.next_value() + (b"x" * rng.randint(10_000, 100_000) if big else b"")

        if kind == "string":
            c = rng.random()
            size = len(state[1]) if state and state[0] == "string" else 0

            if size > (200_000 if self.cfg.get("maxmemory") else 20_000):
                c = 0  # keep values (and their log records) bounded: overwrite

            if c < 0.4:
                return [b"SET", key, value]
            if c < 0.55:
                return [b"SET", key, value, b"PX", str(rng.randint(700, 3000)).encode()]
            if c < 0.6:
                return [b"SET", key, value, b"EX", str(rng.randint(1, 3)).encode()]
            if c < 0.65:
                # Counters next to a carry, a sign change or an i64 limit.
                return [b"SET", key, str(rng.choice(COUNTER_EDGES)).encode()]
            if c < 0.74:
                return [b"INCR", key]
            if c < 0.8:
                command = rng.choice([b"INCRBY", b"DECRBY"])
                delta = rng.choice([1, 1, 2, 9, 10, 11, 99, 100, 2**62, 2**63 - 1])
                return [command, key, str(delta if rng.random() < 0.8 else -delta).encode()]
            return [b"APPEND", key, value]
        if kind == "list":
            size = len(state[1]) if state and state[0] == "list" else 0
            c = rng.random()

            if c < 0.3 + size / 40:
                return [b"LPOP", key]
            return [rng.choice([b"LPUSH", b"RPUSH"]), key, value] + ([self.next_value()] if c > 0.9 else [])
        if kind == "hash":
            fields = [f"f{rng.randint(0, 5)}".encode() for _ in range(rng.randint(1, 2))]
            c = rng.random()

            if c < 0.25:
                return [b"HDEL", key, *fields]
            if c < 0.4:
                return [b"HPEXPIRE", key, str(rng.randint(700, 3000)).encode(), b"FIELDS",
                        str(len(fields)).encode(), *fields]
            out = [b"HSET", key]

            for field in dict.fromkeys(fields):
                out += [field, self.next_value() + (b"x" * rng.randint(2000, 30000) if big else b"")]
            return out
        if kind == "set":
            members = [f"m{rng.randint(0, 9)}".encode() for _ in range(rng.randint(1, 3))]
            return [b"SREM" if rng.random() < 0.35 else b"SADD", key, *members]
        if kind == "zset":
            member = f"m{rng.randint(0, 9)}".encode()
            c = rng.random()

            if c < 0.3:
                return [b"ZREM", key, member]
            if c < 0.55:
                return [b"ZINCRBY", key, str(rng.randint(-3, 5)).encode(), member]
            return [b"ZADD", key, str(rng.randint(-20, 20)).encode(), member]
        if kind == "stream":
            if rng.random() < 0.25 and state and state[0] == "stream" and state[1]:
                return [b"XDEL", key, rng.choice(state[1])[0]]
            self.stream_seq += 1
            return [b"XADD", key, f"{self.stream_seq}-1".encode(), b"f", value]
        raise AssertionError(kind)

    def gen_write(self):
        key = self.pick_key()

        if not self.free([key]):
            return None
        argv = self.command_for(key, True)
        return Op(argv[0].decode(), [argv], [key], True)

    def gen_read(self):
        key = self.pick_key()

        if not self.free([key]):
            return None
        argv = self.command_for(key, False)
        return Op(argv[0].decode(), [argv], [key], False)

    def gen_multi(self):
        rng = self.rng
        group = rng.randrange(len(self.groups))
        keys = rng.sample(self.groups[group], rng.randint(1, 3))

        if not self.free(keys):
            return None
        cmds = [[b"MULTI"]] + [self.command_for(rng.choice(keys), rng.random() < 0.7)
                               for _ in range(rng.randint(2, 4))] + [[b"EXEC"]]
        return Op("MULTI", cmds, keys, True, kind="multi")

    def gen_eval(self):
        rng = self.rng
        group = rng.randrange(len(self.groups))
        src, dst = rng.choice(self.groups[group]), rng.choice(self.groups[group])
        keys = list(dict.fromkeys([src, dst]))

        if not self.free(keys):
            return None
        source = alive(self.state.get(src), time.time() * 1000)

        if source and source[0] == "string" and len(source[1]) > 20_000:
            return None
        return Op("EVAL", [[b"EVAL", SCRIPT_B, b"2", src, dst, self.next_value(b"e")]], keys, True)

    def gen_rename(self):
        rng = self.rng
        group = rng.randrange(len(self.groups))
        src, dst = rng.sample(self.groups[group], 2)

        if not self.free([src, dst]):
            return None
        return Op("RENAME", [[b"RENAME", src, dst]], [src, dst], True)

    def gen_pair(self, conn):
        key = self.pick_key()

        if not self.free([key]) or alive(self.state.get(key), time.time() * 1000) is not None:
            return None
        if self.pairs or sum(1 for c in self.conns if not c.limited) < 2:
            return None
        if conn.pending:
            return None  # BLPOP blocks the connection; keep it from delaying other commands
        pop = Op("BLPOP", [[b"BLPOP", key, b"1"]], [key], True, kind="pair-pop")
        push = Op("RPUSH", [[b"RPUSH", key, self.next_value(b"b")]], [key], True, kind="pair-push")
        pop.pair = push
        push.pair = pop
        self.pairs.append((time.time() + self.rng.uniform(0, 0.15), push))
        return pop

    def gen_limited(self):
        """Commands from a user limited to `{g0}*` keys without FLUSHDB: allowed ones go through the
        model, denied ones must be refused and change nothing."""

        rng = self.rng
        r = rng.random()

        if self.writes_blocked:
            return None
        if r < 0.5:
            key = self.pick_key(0)

            if not self.free([key]):
                return None
            argv = self.command_for(key, rng.random() < 0.6)
            return Op(argv[0].decode(), [argv], [key], True)
        other = self.pick_key(rng.randrange(1, len(self.groups)))
        own = self.pick_key(0)

        if r < 0.65:
            if not self.free([other]):
                return None
            op = Op("SET(denied)", [[b"SET", other, self.next_value(b"denied")]], [other], True)
        elif r < 0.75:
            op = Op("FLUSHDB(denied)", [[b"FLUSHDB"]], [], True)
        elif r < 0.88:
            if not self.free([other]):
                return None
            op = Op("EVAL(denied)", [[b"EVAL", SCRIPT_B, b"2", other, other, b"x"]], [other], True)
        else:
            if not self.free([own, other]):
                return None
            op = Op("MULTI(denied)", [[b"MULTI"], [b"SET", own, self.next_value(b"d")],
                                      [b"SET", other, self.next_value(b"denied")], [b"EXEC"]],
                    [own, other], True, kind="multi")
            op.base = {}
        op.denied = True
        return op

    # -- drain and restarts
    def drain(self, timeout=30.0, fail=True):
        end = time.time() + timeout
        self.barrier = None

        while (self.outstanding or self.pairs) and time.time() < end:
            self.pump_no_fill(0.01)
            self.check_alive()

        if self.outstanding and fail:
            stuck = [op.line() for c in self.conns for op in c.pending]
            raise Failure("hang", f"{self.outstanding} commands got no reply within {timeout:.0f} s",
                          "\n".join(stuck[:40]) + "\n\n" + self.probe_server())

    def pump_no_fill(self, timeout):
        fill = self.fill
        self.fill = lambda: None

        try:
            self.pump(timeout)
        finally:
            self.fill = fill

    def collect_unknown(self):
        for conn in self.conns:
            for op in list(conn.pending):
                self.finish_unknown(op)
            conn.pending.clear()
        for _, push in self.pairs:
            push.status = "unsent"
            self.complete_pair_part(push)
        self.pairs = []
        self.barrier = None

    def restart_primary(self, how):
        kill_t = time.time() * 1000
        self.note(f"{how} primary with {self.outstanding} commands in flight")

        if how == "tear":
            self.tear_wal_write()
            kill_t = time.time() * 1000
            how = "kill9"
        elif how in ("kill9", "powerloss"):
            self.primary.kill9()
        else:
            self.primary.sigterm()
        self.close_all()
        self.collect_unknown()
        panic = self.primary.panicked()

        if panic:
            raise Failure("panic", "the server log shows a panic", panic)
        if how == "powerloss":
            self.primary.set_fault(None)
            self.drop_unsynced()
        self.primary.start()
        self.verify_recovery(how, kill_t)

    tearing = False

    def tear_wal_write(self):
        """A WAL write stops after a random prefix of its bytes and the server dies with it: a
        crash inside a batch, between or inside its records. Most tears first drain the
        server, send one multi-record write and cut its first, second or third WAL write."""

        # Both drawn before waiting, so the schedule takes the same numbers however that goes.
        wants_target = self.schedule.random() < 0.6
        # Each record is its own write: letting one or two through lands the cut on a later
        # record of the same command, between the records of a write split in two.
        target_skip = self.schedule.choice([0, 1, 1, 2])
        targeted = wants_target and self.go_idle(2.0)
        skip = target_skip if targeted else 0
        self.primary.set_fault(f"tear {skip} {self.primary.data_dir}/wal/")
        self.tearing = True
        end = time.time() + 3

        try:
            if targeted:
                # Alone on the server, one multi-key write is the next WAL write: a cut inside
                # it shows whether its records go in together.
                self.stats["tear:" + self.send_multi_record_write()] += 1
                settle = time.time() + 1

                while self.primary.alive() and time.time() < settle:
                    self.pump_no_fill(0.002)
            while self.primary.alive() and time.time() < end:
                try:
                    self.pump(0.002)
                except ValueError:
                    break  # no live connection left to send a BLPOP's push on
        finally:
            self.tearing = False
        self.stats["tear:" + ("cut" if not self.primary.alive() else "no-write")] += 1
        self.primary.kill9()
        self.primary.set_fault(None)

    def go_idle(self, timeout) -> bool:
        """Waits, sending nothing new, until no command is in flight. False on timeout."""

        end = time.time() + timeout

        while time.time() < end:
            if not any(c.pending for c in self.conns if not c.dead) and not self.pairs:
                return True
            try:
                self.pump_no_fill(0.005)
            except ValueError:
                return False
        return False

    def send_multi_record_write(self) -> str:
        """Sends one MULTI of writes, or a RENAME of a live string, on an idle connection."""

        conn = next((c for c in self.conns if not c.dead and not c.pending), None)

        if conn is None:
            return "targeted-no-conn"
        rng = self.rng
        now = time.time() * 1000

        for _ in range(20):
            keys = rng.choice(self.groups)
            sources = [k for k in keys if (alive(self.state.get(k), now) or ("",))[0] == "string"]

            if sources and rng.random() < 0.5:
                src = rng.choice(sources)
                dst = rng.choice([k for k in keys if k != src])

                if self.free([src, dst]):
                    self.send(Op("RENAME", [[b"RENAME", src, dst]], [src, dst], True), conn)
                    return "targeted-rename"
                continue
            chosen = rng.sample(keys, min(len(keys), rng.randint(2, 3)))

            if self.free(chosen):
                cmds = [[b"MULTI"]] + [self.command_for(rng.choice(chosen), True)
                                       for _ in range(rng.randint(3, 5))] + [[b"EXEC"]]
                self.send(Op("MULTI", cmds, chosen, True, kind="multi"), conn)
                return "targeted-multi"
        return "targeted-none"

    def drop_unsynced(self):
        """Power loss: what reached a WAL file but no fsync/fdatasync after it is lost. The
        shim logged both since the epoch began; those ranges are zeroed (a WAL file is
        preallocated, so lost pages read back as zeros)."""

        pending = collections.defaultdict(list)
        log = self.primary.track_log

        for line in (log.read_text().splitlines() if log.exists() else []):
            parts = line.split(" ")

            if parts[0] == "S" and len(parts) == 2:
                pending.pop(parts[1], None)
            elif parts[0] == "W" and len(parts) == 4:
                pending[parts[1]].append((int(parts[2]), int(parts[3])))
        dropped = 0

        for path, ranges in pending.items():
            try:
                with open(path, "r+b") as handle:
                    size = os.fstat(handle.fileno()).st_size

                    for start, end in ranges:
                        end = min(end, size)

                        if end > start:
                            handle.seek(start)
                            handle.write(b"\0" * (end - start))
                            dropped += end - start
            except FileNotFoundError:
                continue
        log.unlink(missing_ok=True)
        self.note(f"power loss dropped {dropped} unsynced WAL bytes in {len(pending)} files")
        self.stats["powerloss:bytes_dropped"] += dropped

    def must_rule(self, how, kill_t):
        profile = self.cfg["profile"]

        if how == "live":
            return lambda e: e.op.status == "ok"
        if profile == "memory":
            return None
        if profile == "always" or how == "sigterm":
            return lambda e: e.op.status == "ok"
        if profile == "everysec":
            window = self.params["sync_interval_ms"] + self.args.everysec_slack_ms
            limit = kill_t - window

            if self.fault and self.fault.get("first_hit"):
                limit = min(limit, self.fault["first_hit"] - window)
            return lambda e: e.op.status == "ok" and e.op.recv_t is not None and e.op.recv_t <= limit
        if profile == "snapshot":
            ok_saves = [s for s in self.saves if s[2]]

            if not ok_saves:
                return lambda e: False
            bound = ok_saves[-1][0]
            return lambda e: e.op.status == "ok" and e.recv_ev is not None and e.recv_ev < bound
        raise AssertionError(profile)

    def never_rule(self, how, shard):
        """Entries on `shard` that may not survive a restart. A refused write's records on a
        shard whose WAL sync failed are cut. An op over several shards is refused when any of
        them fails, but its part on a shard whose sync succeeded is durable, so only the failed
        shards' parts are ruled out."""

        strict = (
            how == "kill9"
            and self.cfg["profile"] == "always"
            and self.cfg.get("sync_worker") == "off"
            and self.fault is not None
            and self.fault.get("class") == "sync"
        )
        if strict:
            if shard in self.fault.get("wal_sync_shards", set()):
                return lambda e: e.op.status == "refused"
        return lambda e: False

    def verify_recovery(self, how, kill_t):
        states, t0, t1, unexpected = dump(self.primary.port, self.all_keys)

        if unexpected:
            raise Failure("state", f"unexpected keys after {how}: {unexpected[:10]!r}")
        profile = self.cfg["profile"]

        if profile == "memory":
            present = [k for k, v in states.items() if v is not None]

            if present:
                raise Failure("state", f"memory profile kept {len(present)} keys across a restart",
                              "\n".join(f"{short(k)} = {describe(states[k])}" for k in present[:20]))
            self.adopt(states, t0)
            return
        self.check_states(states, t0, t1, how, kill_t)
        self.adopt(states, t0)

    def check_states(self, states, t0, t1, how, kill_t):
        must = self.must_rule(how, kill_t)
        strict_lost = []

        for shard in range(self.shards):
            keys = [k for k in self.all_keys if self.shard[k] == shard]
            never = self.never_rule(how, shard)
            cut = find_cut(keys, self.init, self.hist, states, t0, t1, must, never)

            for key in keys:
                for entry in self.hist.get(key, [])[cut[key]:]:
                    if entry.op.status == "ok":
                        strict_lost.append((key, entry))
        self.stats[f"checks:{how}"] += 1

        if strict_lost and how != "live" and self.cfg["profile"] == "everysec":
            # A process kill keeps what the kernel accepted; everysec acknowledges after the write.
            try:
                for shard in range(self.shards):
                    keys = [k for k in self.all_keys if self.shard[k] == shard]
                    find_cut(keys, self.init, self.hist, states, t0, t1, lambda e: e.op.status == "ok",
                              self.never_rule(how, shard))
            except Failure as failure:
                if how == "kill9" and not self.args.lenient_everysec:
                    raise Failure("everysec-loss",
                                  f"{len(strict_lost)} acknowledged writes lost by a process kill "
                                  "(everysec acknowledges after the write reaches the kernel)",
                                  failure.diff)

    def adopt(self, states, t0):
        """The recovered (and accepted) state becomes the model."""

        model = {}

        for key in self.all_keys:
            state = states.get(key)

            if state is not None and state[0] == "hash":
                state = ("hash", tuple(sorted(state[1])), state[2])
            model[key] = state
        self.state = model
        self.new_epoch(t0)

    def live_check(self, connected=True):
        """No restart: the live state must show every acknowledged write."""

        if connected:
            self.close_all()
            self.collect_unknown()
        states, t0, t1, unexpected = dump(self.primary.port, self.all_keys)

        if unexpected:
            raise Failure("state", f"unexpected keys: {unexpected[:10]!r}")
        for shard in range(self.shards):
            keys = [k for k in self.all_keys if self.shard[k] == shard]
            find_cut(keys, self.init, self.hist, states, t0, t1, lambda e: e.op.status == "ok",
                     lambda e: False)
        self.stats["checks:live"] += 1
        # Without ambiguous keys the history stays, so a later crash is checked against the
        # whole epoch.
        if self.ambiguous:
            # Settle ambiguous keys from what the server shows now.
            self.adopt(states, t0)

    def replica_converges(self, timeout=30.0):
        self.close_all()
        self.collect_unknown()
        end = time.time() + timeout
        primary, _, _, _ = dump(self.primary.port, self.all_keys)
        last = None

        while time.time() < end:
            try:
                replica, t0, t1, unexpected = dump(self.replica.port, self.all_keys, timeout=5)
            except Failure as failure:
                last = failure.detail
                time.sleep(0.2)
                continue
            diff = [k for k in self.all_keys
                    if not match_state(primary.get(k), replica.get(k), t0, t1) and
                    not match_state(replica.get(k), primary.get(k), t0, t1)]

            if not diff and not unexpected:
                # Reads hide a logically expired key, but DBSIZE counts it: a replica keeps an
                # expired key until the primary's delete reaches it, so a delete the primary
                # never sent shows up only here.
                sizes = self.dbsizes()

                if sizes[0] == sizes[1]:
                    self.stats["replica_converged"] += 1
                    return
                last = f"DBSIZE primary={sizes[0]!r} replica={sizes[1]!r}"
                time.sleep(0.25)
                primary, _, _, _ = dump(self.primary.port, self.all_keys)
                continue
            last = "\n".join(f"{short(k)}: primary={describe(primary.get(k))} replica={describe(replica.get(k))}"
                             for k in diff[:20]) + (f"\nunexpected on replica: {unexpected}" if unexpected else "")
            time.sleep(0.25)
            primary, _, _, _ = dump(self.primary.port, self.all_keys)
        raise Failure("replica", f"replica did not converge to the primary within {timeout:.0f} s", last or "")

    def dbsizes(self):
        sizes = []

        for server in (self.primary, self.replica):
            client = None

            try:
                client = BlockingClient(server.port, timeout=5)
                sizes.append(client.call("DBSIZE"))
            except OSError as error:
                sizes.append(f"unreachable: {error}")
            finally:
                if client:
                    client.close()
        return sizes

    def probe_server(self) -> str:
        """What a fresh connection sees: PING and GET latency per key group, and snapshot state."""

        lines = []

        for group in self.groups:
            key = group[0]
            try:
                client = BlockingClient(self.primary.port, timeout=3)
                t = time.time()
                reply = client.call("GET", key)
                lines.append(f"fresh GET {short(key)} (shard {self.shard[key]}): "
                             f"{short_reply(reply)} in {(time.time() - t) * 1000:.0f} ms")
                client.close()
            except OSError as error:
                lines.append(f"fresh GET {short(key)} (shard {self.shard[key]}): {error}")
        try:
            client = BlockingClient(self.primary.port, timeout=3)
            info = client.call("INFO", "persistence")
            client.close()
            wanted = ("ruvio_persistence_state", "snapshot_in_progress", "snapshot_journal_bytes",
                      "snapshot_aborts_total", "snapshot_overload_skips_total", "wal_last_sequence",
                      "wal_durable_sequence", "ruvio_shard_id")
            text = info.decode(errors="replace") if isinstance(info, bytes) else repr(info)
            lines += [line for line in text.splitlines() if line.split(":")[0] in wanted]
        except OSError as error:
            lines.append(f"INFO: {error}")
        return "\n".join(lines)

    # -- disk faults
    def start_disk_fault(self):
        rng = self.schedule
        profile = self.cfg["profile"]
        targets = ["checkpoints"] if profile == "snapshot" else ["wal", "wal", "checkpoints", "both"]
        target = rng.choice(targets)
        klass = rng.choice(["sync", "write"])
        errno_name = "ENOSPC" if klass == "write" and rng.random() < 0.5 else "EIO"
        partial = klass == "write" and rng.random() < 0.4
        paths = {"wal": "/wal/", "checkpoints": "/checkpoints/", "both": "/wal/,/checkpoints/"}[target]
        line = f"{klass} {errno_name} {','.join(str(self.primary.data_dir) + p for p in paths.split(','))}"

        if partial:
            line += " partial"
        self.fault = dict(target=target, **{"class": klass}, errno=errno_name, partial=partial,
                          started=time.time() * 1000, line=line)
        self.note(f"disk fault on: {line}")
        self.stats[f"fault:{klass}-{errno_name}-{target}"] += 1
        self.primary.set_fault(line)
        self.fault["until"] = time.time() + rng.uniform(0.3, 1.2)

        if target in ("checkpoints", "both"):
            # Make the checkpoint path run while the fault is on.
            conn = self.pick_other_conn(-1)
            self.send(Op("SAVE", [[b"SAVE"]], [], True, kind="save"), conn)

    def finish_disk_fault(self):
        fault = self.fault

        while time.time() < fault["until"]:
            self.pump(0.005)
            self.check_alive()
        self.writes_blocked = True
        self.drain(timeout=5.0 if "save-hang" in self.args.known else 30.0, fail=False)

        if self.outstanding:
            pending = [op for c in self.conns for op in c.pending]

            # Replies to commands pipelined with a waiting SAVE are held until the batch ends.
            blocked = [c for c in self.conns if c.pending]

            if not all(any(op.kind == "save" for op in c.pending) for c in blocked) \
                    or "save-hang" not in self.args.known:
                raise Failure("hang", f"{len(pending)} commands got no reply during a disk fault",
                              "\n".join(op.line() for op in pending[:40]) + "\n\n" + self.probe_server())
            self.stats["known:save-hang"] += 1
            self.primary.set_fault(None)
            fault["removed"] = time.time() * 1000
            self.drain()
        hits = self.primary.faults_since(fault["started"])
        fault["hits"] = hits
        fault["first_hit"] = hits[0][0] if hits else None
        self.primary.set_fault(None)
        fault.setdefault("removed", time.time() * 1000)
        self.note(f"disk fault off: {len(hits)} injected failures, refused={self.refused_any}")
        self.stats["fault_hits"] += len(hits)
        # A failed sync of a segment holding only its header (a segment being created) loses
        # no record; the server may retry it in a fresh segment, so it does not make writes stop.
        record_syncs = [h for h in hits if h[1] in ("fsync", "fdatasync") and h[4] > WAL_HEADER_BYTES]
        self.stats["fault:header-only-sync"] += sum(
            1 for h in hits if h[1] in ("fsync", "fdatasync") and "/wal/" in h[3] and 0 <= h[4] <= WAL_HEADER_BYTES)
        wal_sync_hits = {int(m.group(1)) for h in record_syncs
                         for m in [re.search(r"/wal/shard-(\d+)/", h[3])] if m}
        fault["wal_sync_shards"] = wal_sync_hits
        # Acknowledged writes sent after a WAL sync failure on their shard break the contract.
        for key, entries in self.hist.items():
            for entry in entries:
                op = entry.op

                if op.status != "ok" or not op.write or not changes_live_state(op, key, entry.state):
                    continue
                for shard in op.shards:
                    first = min((h[0] for h in record_syncs if f"/wal/shard-{shard:04d}/" in h[3]),
                                default=None)

                    if first is not None and op.send_t > first + self.args.sticky_margin_ms:
                        raise Failure("fsync-ok", f"#{op.id} was acknowledged {op.send_t - first:.0f} ms after a "
                                      f"WAL sync failure on shard {shard}", op.line())
        # Sticky refusal after a sync failure; resume after a write failure.
        self.connect_all()

        for shard in range(self.shards):
            key = self.probe_keys[shard]
            sync_failed = shard in wal_sync_hits
            deadline = time.time() + (2.0 if sync_failed else 15.0)
            freed = False
            delete_refused = False

            while True:
                op = Op("SET(probe)", [[b"SET", key, self.next_value(b"probe")]], [key], True)
                self.send(op, self.conns[0])
                self.drain()

                if sync_failed:
                    if op.status == "ok":
                        raise Failure("fsync-ok", f"shard {shard} accepted a write after its WAL sync failed "
                                      "and the disk recovered (writes must stay refused until restart)",
                                      op.line())
                    break
                if op.status == "ok":
                    break
                if op.status == "oom":
                    # A full noeviction store refuses the probe before the WAL is asked. Deletes
                    # still run when full: free the shard's keys, then probe again.
                    self.stats["resume-probe-oom"] += 1
                    keys = [k for k in self.all_keys if self.shard[k] == shard and self.free([k])]

                    if freed or (not keys and not delete_refused):
                        # A written DEL shows writes resumed; keys the model cannot delete
                        # (ambiguous ones) may still fill the store.
                        self.stats["resume-probe-still-full"] += 1
                        break
                    if keys:
                        delete = Op("DEL", [[b"DEL", *keys]], keys, True)
                        self.send(delete, self.conns[0])
                        self.drain()

                        if delete.status == "ok":
                            freed = True
                            continue
                        # Refused (the write fault still lingers): its keys turn ambiguous, so
                        # keep probing with SET until the deadline.
                        delete_refused = True
                if time.time() > deadline:
                    raise Failure("resume", f"shard {shard} still refuses writes 15 s after the write fault "
                                  "was removed", op.line())
                time.sleep(0.2)
        fault["ended"] = True

    # -- reporting
    def write_repro(self, failure: Failure) -> Path:
        out = self.results / f"{self.name}-seed{self.seed}-{int(time.time())}"
        out.mkdir(parents=True, exist_ok=True)
        command = (f"python3 scripts/model_check.py --seed {self.args.seed} --config {self.name} "
                   f"--duration {self.args.duration_for_repro} --bin {self.args.bin} --results {self.results}")
        if self.args.wal_segment_bytes:
            command += f" --wal-segment-bytes {self.args.wal_segment_bytes}"
        summary = [
            f"seed: {self.args.seed} (config seed {self.seed})",
            f"config: {self.name} {json.dumps(self.cfg)}",
            f"params: {json.dumps(self.params)}",
            f"failure: {failure.kind}: {failure.detail}",
            f"epoch: {getattr(self, 'epoch_number', 0)}",
            f"repro: {command}",
            "",
            "notes:",
            *self.notes[-40:],
            "",
            "stats: " + json.dumps(self.stats),
        ]
        (out / "summary.txt").write_text("\n".join(summary) + "\n")
        (out / "diff.txt").write_text(failure.diff + "\n")
        (out / "oplog.txt").write_text("\n".join(self.log) + "\n")
        (out / "traceback.txt").write_text(traceback.format_exc())

        for server in (self.primary, self.replica):
            if server is None:
                continue
            for path in (server.log_path, server.config_path, server.fault_log):
                if path.exists():
                    shutil.copy(path, out / path.name)
            # The data directory as the failure left it (the server is stopped first).
            if server.alive():
                server.kill9()
            size = sum(f.stat().st_size for f in server.data_dir.rglob("*") if f.is_file()) \
                if server.data_dir.exists() else 0

            if 0 < size <= self.args.max_data_mb << 20:
                shutil.copytree(server.data_dir, out / f"{server.name}-data", ignore=shutil.ignore_patterns("LOCK"))
        return out


def flatten(value):
    if isinstance(value, list):
        for item in value:
            yield from flatten(item)
    else:
        yield value


# Loaded before any fork: the child between fork and exec only calls into it.
LIBC = ctypes.CDLL(None) if sys.platform.startswith("linux") else None
PR_SET_PDEATHSIG = 1


def die_with_parent():
    """In a server between fork and exec: on Linux, SIGKILL it when the checker dies, so a
    checker killed for any reason leaves no server behind on its ports."""

    if LIBC is not None:
        LIBC.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)


def free_port_block(start: int, count: int) -> int:
    for base in range(start, 65000 - count, count):
        try:
            sockets = []

            for port in range(base, base + count):
                s = socket.socket()
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("127.0.0.1", port))
                sockets.append(s)
            for s in sockets:
                s.close()
            return base
        except OSError:
            for s in sockets:
                s.close()
    raise RuntimeError("no free port block")


def build_shim(work: Path) -> Path | None:
    out = work / "model_check_shim.so"
    compiler = shutil.which("cc") or shutil.which("gcc")

    if compiler is None:
        print("model_check: no C compiler; disk faults are off", flush=True)
        return None
    subprocess.run([compiler, "-O2", "-shared", "-fPIC", "-o", str(out), str(SHIM_SOURCE), "-ldl", "-lpthread"],
                   check=True)
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=None, help="random seed (printed; default: from time)")
    parser.add_argument("--duration", type=float, default=60.0,
                        help=f"nominal seconds of work ({OPS_PER_SECOND} commands each), split over the configs")
    parser.add_argument("--config", default="all", help="a config name, a comma list, or all: " + ", ".join(CONFIGS))
    parser.add_argument("--bin", default=str(REPO / "target" / "release-fast" / "ruvio"))
    parser.add_argument("--results", default=str(REPO / "target" / "model-check"))
    parser.add_argument("--work-dir", default=None, help="server data dirs (default: a temporary directory)")
    parser.add_argument("--port-base", type=int, default=None,
                        help=f"first of {PORT_BLOCK} consecutive ports the caller keeps free for this run; "
                        "used as given (default: a free block from 27000)")
    parser.add_argument("--max-data-mb", type=int, default=256, help="take a SAVE when data dirs grow past this")
    parser.add_argument("--log-tail", type=int, default=4000, help="op log lines kept for a repro")
    parser.add_argument("--everysec-slack-ms", type=int, default=1000, help="extra loss window for everysec")
    parser.add_argument("--sticky-margin-ms", type=int, default=200,
                        help="time after a WAL sync failure before an acknowledged write is a violation")
    parser.add_argument("--lenient-everysec", action="store_true",
                        help="only check the sync-interval window for everysec after kill -9")
    parser.add_argument("--no-faults", action="store_true", help="no disk faults")
    parser.add_argument("--wal-segment-bytes", type=int, default=None,
                        help="WAL segment size instead of a random one; a small one rotates often, so "
                        "a sync fails during a rotation")
    parser.add_argument("--events", type=lambda text: text.split(","), default=None,
                        help="restrict epoch-ending events (comma list of kill9, sigterm, check, disk, "
                        "powerloss, tear, kill9-replica, sigterm-replica)")
    parser.add_argument("--known", type=lambda text: set(text.split(",")), default=set(),
                        help="tolerate known product bugs so a run can go on: save-hang (SAVE with "
                        "several shards waits while checkpoint files cannot be written)")
    parser.add_argument("--hang-seconds", type=float, default=15.0, help="a reply slower than this is a hang")
    parser.add_argument("--keep-going", action="store_true", help="after a failure, start the config again")
    parser.add_argument("--mutant", choices=["lose-writes"], default=None,
                        help="self-test: run the server with the memory profile while checking the configured "
                        "one, so every restart must be reported")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    if args.seed is None:
        args.seed = int(time.time()) % 1000000
    names = list(CONFIGS) if args.config == "all" else args.config.split(",")

    for name in names:
        if name not in CONFIGS:
            parser.error(f"unknown config {name!r}; known: {', '.join(CONFIGS)}")
    if not Path(args.bin).exists():
        parser.error(f"no binary at {args.bin}")
    args.duration_for_repro = round(args.duration / len(names), 1)
    print(f"model_check: seed {args.seed}, configs {','.join(names)}, {args.duration:.0f} s, bin {args.bin}",
          flush=True)
    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)
    own_work = args.work_dir is None
    work = Path(tempfile.mkdtemp(prefix="model-check-")) if own_work else Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    args.shim = None if args.no_faults else build_shim(work)
    failures = 0
    slice_seconds = args.duration / len(names)
    totals = {}

    # Configs run one after another and every server is gone before the next starts, so one
    # block serves them all, as it serves a server's restarts.
    port_base = args.port_base if args.port_base is not None else free_port_block(27000, PORT_BLOCK)

    try:
        for name in names:
            started = time.time()
            round_index = 0

            while True:
                seed = args.seed if round_index == 0 else args.seed * 1000 + round_index
                checker = Checker(args, name, seed, slice_seconds, port_base, work, results)
                failed = False

                try:
                    checker.run()
                    print(f"ok   {name}: {checker.stats['epochs']} epochs, {checker.op_seq} commands sent, "
                          f"{checker.stats['ok']} acknowledged ops, "
                          f"{checker.stats['refused']} refused, {checker.stats['oom']} OOM, "
                          f"{dict((k, v) for k, v in checker.stats.items() if ':' in k)}", flush=True)
                except Failure as failure:
                    failed = True
                    failures += 1
                    out = checker.write_repro(failure)
                    print(f"FAIL {name} seed {seed}: {failure.kind}: {failure.detail}\n     repro: {out}", flush=True)
                    for server in (checker.primary, checker.replica):
                        if server and server.alive():
                            server.kill9()
                round_index += 1
                shutil.rmtree(work / name, ignore_errors=True)
                # A clean run did the config's planned work. After a failure --keep-going starts
                # again with the next seed while the config's share of the duration lasts.
                if not (failed and args.keep_going and time.time() - started < slice_seconds):
                    break
            totals[name] = time.time() - started
    finally:
        if own_work:
            shutil.rmtree(work, ignore_errors=True)
    print("model_check: time per config: " + ", ".join(f"{k} {v:.0f}s" for k, v in totals.items()), flush=True)
    print(f"model_check: {'FAILED' if failures else 'passed'} ({failures} failures), seed {args.seed}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
