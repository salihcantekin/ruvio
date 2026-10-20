# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Cluster routing logic shared by the sync and asyncio clients (pure functions, no I/O).

Commands arrive at the router as complete RESP frames (``bytes``). Only the command name and
the first key are read for the common single-key case; every other command is split into
arguments and run through :func:`plan`.
"""

from __future__ import annotations

import binascii
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from ._command_keys import (
    COMMAND_KEY_RULES,
    RULE_KEY_AND_STORE,
    RULE_KEYWORD_HALF,
    RULE_NONE,
    RULE_NUMKEYS,
    RULE_RANGE,
)
from ._resp import encode, encode_args
from .errors import CrossSlotError, ResponseError, RuvioError

SLOT_COUNT = 16384

#: ``-MOVED`` / ``-ASK`` redirects followed per command (one budget) before the error is returned
MAX_REDIRECTS = 3

#: sleeps in seconds before the 1st, 2nd and 3rd re-send of a command answered ``-TRYAGAIN``
TRYAGAIN_DELAYS = (0.005, 0.010, 0.020)

#: ``ASKING`` as a frame, sent in front of the command an ``-ASK`` redirect names
ASKING = b"*1\r\n$6\r\nASKING\r\n"

_crc16 = binascii.crc_hqx  # CRC-CCITT with poly 0x1021 and init 0 == CRC16/XMODEM, in C


def key_slot(key: Union[str, bytes]) -> int:
    """Hash slot (0 to 16383) of a key: CRC16-XMODEM of the key, or of its first non-empty ``{hash tag}``.

    Same rule as the server and ``CLUSTER KEYSLOT``, computed locally. Keys sharing a hash tag share a
    slot, which multi-key commands and transactions need on a multi-shard server.

    Args:
        key: The key (``str`` is encoded as UTF-8).

    Returns:
        The slot number.

    Example:
        key_slot("{user:1}:profile") == key_slot("{user:1}:cart")   # -> True
    """
    if type(key) is not bytes:
        key = key.encode("utf-8", "surrogateescape") if isinstance(key, str) else bytes(key)
    o = key.find(b"{")
    if o >= 0:
        c = key.find(b"}", o + 1)
        if c > o + 1:
            key = key[o + 1 : c]
    return _crc16(key, 0) & 0x3FFF


# --------------------------------------------------------------------------- read policy


class ReadPolicy(str, Enum):
    """Where a cluster client sends commands that only read.

    ``PRIMARY`` (the default) sends every command to the primary of its slot. ``PREFER_REPLICA`` sends reads
    to one replica of the shard and falls back to the primary when that replica cannot be used. ``REPLICA``
    sends reads to a replica only and fails when there is none. Replication is asynchronous, so a read from
    a replica may miss a recent write, including your own. Plain strings (``"primary"``,
    ``"prefer-replica"``, ``"replica"``) are accepted wherever a policy is.

    Example:
        client = ClusterRuvio("127.0.0.1", 7000, read_policy=ReadPolicy.PREFER_REPLICA)
    """

    PRIMARY = "primary"
    PREFER_REPLICA = "prefer-replica"
    REPLICA = "replica"


#: internal policy codes kept in the routing state: 0 = primary, tested once per command
POLICY_PRIMARY = 0
POLICY_PREFER = 1
POLICY_REPLICA = 2


def policy_code(value: Union[None, str, ReadPolicy]) -> int:
    """The internal code of a read policy given as a :class:`ReadPolicy` or a string (None = primary)."""
    if value is None:
        return POLICY_PRIMARY
    name = str(getattr(value, "value", value)).lower().replace("_", "-")
    if name == "primary":
        return POLICY_PRIMARY
    if name == "prefer-replica":
        return POLICY_PREFER
    if name == "replica":
        return POLICY_REPLICA
    raise ValueError(f"unknown read_policy {value!r}: use 'primary', 'prefer-replica' or 'replica'")


# --------------------------------------------------------------------------- frames


def frame_head(data: bytes) -> Tuple[bytes, Optional[bytes]]:
    """``(name, first_argument)`` of an encoded command without parsing the rest."""
    i = data.find(b"\r\n")
    argc = int(data[1:i])
    j = data.find(b"\r\n", i + 2)
    n = int(data[i + 3 : j])
    s = j + 2
    name = data[s : s + n]
    if argc < 2:
        return name, None
    p = s + n + 2
    k = data.find(b"\r\n", p)
    m = int(data[p + 1 : k])
    return name, data[k + 2 : k + 2 + m]


def split_frame(data: bytes) -> List[bytes]:
    """All arguments of an encoded command (the command name is element 0)."""
    i = data.find(b"\r\n")
    pos = i + 2
    out: List[bytes] = []
    find = data.find
    for _ in range(int(data[1:i])):
        j = find(b"\r\n", pos)
        s = j + 2
        e = s + int(data[pos + 1 : j])
        out.append(data[s:e])
        pos = e + 2
    return out


# --------------------------------------------------------------------------- key positions

# Key positions come from the server's command table (_command_keys.py, generated). A command
# that is not in it has exactly one key: argument 1. Sharded Pub/Sub has no keys there, but its
# channels hash to slots like keys, so they are routed as keys here.
_KEY_RULES: Dict[bytes, Tuple[Any, ...]] = {
    **COMMAND_KEY_RULES,
    b"SPUBLISH": (RULE_RANGE, 1, 1, 1, (), 0),
    b"SSUBSCRIBE": (RULE_RANGE, 1, -1, 1, (), 0),
    b"SUNSUBSCRIBE": (RULE_RANGE, 1, -1, 1, (), 0),
}

# routing kinds; a command that is not listed (its rule is "argument 1", or it has no rule) takes
# the single-key fast path
_KEYLESS = 1  # no key: any node
_KEYS = 2  # keys placed by its _KEY_RULES rule
# commands with their own routing
_FAN_SUM = 20  # DBSIZE: ask every node, add the integers
_FAN_FIRST = 21  # FLUSHDB, AUTH ...: run on every node, return one reply
_FAN_CAT = 22  # KEYS: ask every node, concatenate
_SCAN = 23
_SLOT0 = 24  # PUBLISH: classic pub/sub meets on the owner of slot 0
_TXN = 25  # MULTI / EXEC / DISCARD sent bare
_COND = 26  # CLIENT / SCRIPT / FUNCTION: only some sub-commands run everywhere

SPEC: Dict[bytes, int] = {
    n: _KEYLESS if r[0] == RULE_NONE else _KEYS for n, r in _KEY_RULES.items() if r[:4] != (RULE_RANGE, 1, 1, 1)
}
SPEC.update({b"DBSIZE": _FAN_SUM, b"FLUSHDB": _FAN_FIRST, b"FLUSHALL": _FAN_FIRST, b"AUTH": _FAN_FIRST,
             b"SELECT": _FAN_FIRST, b"KEYS": _FAN_CAT, b"SCAN": _SCAN, b"PUBLISH": _SLOT0})
SPEC.update({b"MULTI": _TXN, b"EXEC": _TXN, b"DISCARD": _TXN})
SPEC.update({b"CLIENT": _COND, b"SCRIPT": _COND, b"FUNCTION": _COND})

# (command, sub-command) pairs of CLIENT / SCRIPT / FUNCTION that must run on every node
_FAN_SUB = frozenset(
    (
        (b"CLIENT", b"SETNAME"),
        (b"SCRIPT", b"LOAD"),
        (b"SCRIPT", b"FLUSH"),
        (b"FUNCTION", b"LOAD"),
        (b"FUNCTION", b"DELETE"),
        (b"FUNCTION", b"FLUSH"),
    )
)

#: server flag bits of the command table (see scripts/gen_client_keys.py)
_FLAG_READONLY = 2
_FLAG_BLOCKING = 16

#: commands that only read and may go to a replica: flagged readonly and not blocking, with ordinary
#: key routing (fan-out, SCAN, transaction and slot-0 Pub/Sub commands keep their own paths)
READS = frozenset(
    n
    for n, r in _KEY_RULES.items()
    if r[5] & _FLAG_READONLY and not r[5] & _FLAG_BLOCKING and SPEC.get(n, _KEYS) in (_KEYLESS, _KEYS)
)

# Fast routing of an ordinary command straight from its encoded frame: one regex match reads the
# command name and the length of argument 1; _PLAIN remembers per spelling of a name whether it
# is routed by "argument 1" (not in SPEC), so no upper-cased copy is made per command.
_HEAD = re.compile(rb"\*\d+\r\n\$\d+\r\n([A-Za-z0-9_.|:-]*)\r\n(?:\$(\d+)\r\n|\Z)").match
_PLAIN: Dict[bytes, bool] = {}


def _classify(name: bytes) -> bool:
    up = name.upper()
    plain = up not in SPEC and up != b"FILLGET"  # FILLGET may park the connection (asyncio client)
    if len(_PLAIN) < 4096:
        _PLAIN[name] = plain
    return plain


_READ: Dict[bytes, bool] = {}


def is_read(data: bytes) -> bool:
    """Whether an encoded command is a read (see :data:`READS`); the name's spelling is cached."""
    m = _HEAD(data)
    if m is None:
        return False
    name = m.group(1)
    read = _READ.get(name)
    if read is None:
        read = name.upper() in READS
        if len(_READ) < 4096:
            _READ[name] = read
    return read


def all_reads(frames: List[bytes]) -> bool:
    """Whether every command of a pipeline is a read."""
    return all(map(is_read, frames))


def node_index(data: bytes, table: List[int]) -> Optional[int]:
    """Index of the node that owns an ordinary command (``-1`` = the seed). None when the frame is
    not an ordinary single-key command (special routing, odd spelling): use :func:`plan` then."""
    m = _HEAD(data)
    if m is None:
        return None
    name, n = m.groups()
    plain = _PLAIN.get(name)
    if plain is None:
        plain = _classify(name)
    if not plain:
        return None
    if n is None:
        return -1
    e = m.end()
    key = data[e : e + int(n)]
    o = key.find(b"{")
    if o >= 0:
        c = key.find(b"}", o + 1)
        if c > o + 1:
            key = key[o + 1 : c]
    return table[_crc16(key, 0) & 0x3FFF]


def route_pipeline(frames: List[bytes], table: List[int], bufs: List[List[bytes]]) -> Optional[List[int]]:
    """One pass over a pipeline of ordinary commands: appends each frame to ``bufs[node index]``
    (``bufs`` has one list per node plus one for the seed, last, which ``-1`` addresses) and returns
    the node index of every command. None when any command needs :func:`pipeline_groups`; ``bufs``
    is then in an unspecified state and must be cleared."""
    idx: List[int] = []
    add = idx.append
    head = _HEAD
    names = _PLAIN
    crc = _crc16
    for f in frames:
        m = head(f)
        if m is None:
            return None
        name, n = m.groups()
        plain = names.get(name)
        if plain is None:
            plain = _classify(name)
        if not plain:
            return None
        if n is None:
            ni = -1
        else:
            e = m.end()
            key = f[e : e + int(n)]
            o = key.find(b"{")
            if o >= 0:
                c = key.find(b"}", o + 1)
                if c > o + 1:
                    key = key[o + 1 : c]
            ni = table[crc(key, 0) & 0x3FFF]
        bufs[ni].append(f)
        add(ni)
    return idx


#: multi-key commands whose keys may live on several nodes: split per node and re-assembled
SPLITTABLE = frozenset((b"MGET", b"MSET", b"DEL", b"UNLINK", b"EXISTS", b"TOUCH"))

#: commands that park the connection until data arrives
BLOCKING = frozenset((b"BLPOP", b"BRPOP", b"BRPOPLPUSH", b"BLMOVE", b"BLMPOP", b"BZPOPMIN", b"BZPOPMAX", b"BZMPOP"))

PLAN_SLOT = 0  # (PLAN_SLOT, slot or None)
PLAN_SPLIT = 1  # (PLAN_SPLIT, keys)
PLAN_FAN = 2  # (PLAN_FAN, SPEC kind)
PLAN_SCAN = 3

FAN_SUM = _FAN_SUM
FAN_FIRST = _FAN_FIRST
FAN_CAT = _FAN_CAT

_UNSUPPORTED_TXN = (
    "MULTI/EXEC/DISCARD cannot be sent one by one on a cluster client (the queued commands may belong to "
    "different nodes); use pipeline(transaction=True) or transaction(fn, *watch_keys)"
)


def _count(word: bytes) -> int:
    """A key count as the server reads it: decimal digits with an optional ``+``, else 0."""
    if word[:1] == b"+":
        word = word[1:]
    return int(word) if word.isdigit() else 0


def command_keys(name: bytes, args: List[bytes]) -> List[bytes]:
    """The keys of a command in argument order (empty for keyless commands).

    ``name`` is the upper-case first word, ``args`` the whole argument vector including it."""
    rule = _KEY_RULES.get(name)
    if rule is None:
        return args[1:2]
    kind, a, b, c, words, _ = rule
    n = len(args)
    if kind == RULE_RANGE:
        return args[a : (n + b if b < 0 else b) + 1 : c]
    if kind == RULE_NUMKEYS:
        keys = args[a + 1 : a + 1 + _count(args[a])] if a < n else []
        return [args[b]] + keys if 0 < b < n else keys
    if kind == RULE_KEYWORD_HALF:
        for i in range(1, n):
            if args[i].upper() == words[0]:
                return args[i + 1 : i + 1 + (n - i - 1) // 2]
        return []
    if kind == RULE_KEY_AND_STORE:
        keys = args[a : a + 1]
        i = a + 1
        while i + 1 < n:
            if args[i].upper() in words:
                keys.append(args[i + 1])
                i += 1
            i += 1
        return keys
    return []


def plan(name: bytes, args: List[bytes]) -> Tuple[Any, ...]:
    """Decides how a command is routed. Raises :class:`CrossSlotError` before anything is sent."""
    kind = SPEC.get(name)
    if kind is None:
        return (PLAN_SLOT, key_slot(args[1]) if len(args) > 1 else None)
    if kind == _KEYLESS:
        return (PLAN_SLOT, None)
    if kind == _SLOT0:
        return (PLAN_SLOT, 0)
    if kind == _TXN:
        raise RuvioError(_UNSUPPORTED_TXN)
    if kind == _SCAN:
        return (PLAN_SCAN,)
    if kind == _FAN_SUM or kind == _FAN_FIRST or kind == _FAN_CAT:
        return (PLAN_FAN, kind)
    if kind == _COND:
        if len(args) > 1 and (name, args[1].upper()) in _FAN_SUB:
            return (PLAN_FAN, _FAN_FIRST)
        return (PLAN_SLOT, None)
    keys = command_keys(name, args)
    if not keys:
        return (PLAN_SLOT, None)
    first = key_slot(keys[0])
    for k in keys[1:]:
        if key_slot(k) != first:
            if name in SPLITTABLE:
                return (PLAN_SPLIT, keys)
            raise CrossSlotError(
                f"CROSSSLOT {name.decode()} keys don't hash to the same slot (nothing was sent)"
            )
    return (PLAN_SLOT, first)


def pipeline_slot(frame: bytes) -> Optional[int]:
    """Slot of one command inside a transaction (None = keyless). Fan-out and cross-slot commands
    are refused: a transaction runs on one node."""
    name, key = frame_head(frame)
    name = name.upper()
    kind = SPEC.get(name)
    if kind is None:
        return None if key is None else key_slot(key)
    p = plan(name, split_frame(frame))
    if p[0] == PLAN_SLOT:
        return p[1]
    if p[0] == PLAN_SPLIT:
        raise CrossSlotError(
            f"CROSSSLOT {name.decode()} keys don't hash to the same slot; a transaction must use keys in one slot"
        )
    raise RuvioError(f"{name.decode()} fans out to every node and cannot be pipelined on a cluster client")


#: one sub-command of a split command inside a pipeline:
#: ``(pipeline index, command name, key count, key positions, frame)``
Sub = Tuple[int, bytes, int, List[int], bytes]


def pipeline_groups(
    frames: List[bytes], table: List[int]
) -> Tuple[Dict[int, Tuple[List[int], List[bytes]]], Optional[List[Sub]]]:
    """Groups pipelined frames by node index (``-1`` = keyless: the seed), in one pass.

    ``table`` is the slot -> node index list. A cross-slot MGET/MSET/DEL/... is split per slot like
    the plain command: its sub-commands get positions ``len(frames) + j`` where ``j`` indexes the
    returned sub list (None when nothing was split); :func:`merge_pipeline` puts the merged reply
    back at the command's own index. Raises before anything is sent if a command cannot be routed
    (other cross-slot multi-key command, fan-out command)."""
    groups: Dict[int, Tuple[List[int], List[bytes]]] = {}
    subs: Optional[List[Sub]] = None
    spec = SPEC
    for i, f in enumerate(frames):
        name, key = frame_head(f)
        if not name.isupper():
            name = name.upper()
        if name in spec:
            args = split_frame(f)
            p = plan(name, args)
            if p[0] == PLAN_SPLIT:
                if subs is None:
                    subs = []
                keys = p[1]
                for ni, parts in split_groups(name, args, keys, table.__getitem__).items():
                    g = groups.get(ni)
                    if g is None:
                        g = groups[ni] = ([], [])
                    for pos, sub in parts:
                        g[0].append(len(frames) + len(subs))
                        g[1].append(sub)
                        subs.append((i, name, len(keys), pos, sub))
                continue
            if p[0] != PLAN_SLOT:
                raise RuvioError(f"{name.decode()} fans out to every node and cannot be pipelined on a cluster client")
            slot = p[1]
        else:
            slot = None if key is None else key_slot(key)
        ni = -1 if slot is None else table[slot]
        g = groups.get(ni)
        if g is None:
            g = groups[ni] = ([], [])
        g[0].append(i)
        g[1].append(f)
    return groups, subs


def merge_pipeline(replies: List[Any], n: int, subs: List[Sub]) -> List[Any]:
    """Merges the replies of split sub-commands (``replies[n:]``) into one reply per command."""
    out = replies[:n]
    j = 0
    while j < len(subs):
        i, name, nkeys = subs[j][0], subs[j][1], subs[j][2]
        parts: List[Tuple[List[int], Any]] = []
        while j < len(subs) and subs[j][0] == i:
            parts.append((subs[j][3], replies[n + j]))
            j += 1
        out[i] = merge_split(name, nkeys, parts)
    return out


# --------------------------------------------------------------------------- split commands


def split_groups(
    name: bytes, args: List[bytes], keys: List[bytes], node_of: Any
) -> Dict[Any, List[Tuple[List[int], bytes]]]:
    """Splits MGET/DEL/MSET... into one sub-command per slot (the server wants one slot per command)
    and groups the sub-commands by owning node.

    ``node_of(slot)`` returns a hashable node handle. Returns ``{node: [(key positions, frame)]}``."""
    pairs = name == b"MSET"
    by_slot: Dict[int, List[int]] = {}
    for i, k in enumerate(keys):
        by_slot.setdefault(key_slot(k), []).append(i)
    out: Dict[Any, List[Tuple[List[int], bytes]]] = {}
    for slot, pos in by_slot.items():
        if pairs:
            sub: List[bytes] = []
            for i in pos:
                sub.append(args[1 + 2 * i])
                sub.append(args[2 + 2 * i])
        else:
            sub = [keys[i] for i in pos]
        out.setdefault(node_of(slot), []).append((pos, encode_args((name, *sub))))
    return out


def merge_split(name: bytes, nkeys: int, parts: List[Tuple[List[int], Any]]) -> Any:
    """Re-assembles the per-node replies (``parts`` = ``[(positions, reply)]``) in the caller's order."""
    for _, r in parts:
        if isinstance(r, ResponseError):
            return r
    if name == b"MGET":
        merged: List[Any] = [None] * nkeys
        for pos, r in parts:
            for i, v in zip(pos, r):
                merged[i] = v
        return merged
    if name == b"MSET":
        return "OK"
    return sum(r for _, r in parts)


# --------------------------------------------------------------------------- SCAN across nodes


def scan_prepare(args: List[bytes], nodes: int) -> Tuple[int, bytes]:
    """The client cursor is ``server_cursor * nodes + node``: 0 starts on the first node and comes
    back as 0 only after the last one. Returns ``(node index, frame for that node)``."""
    try:
        cursor = int(args[1])
    except (IndexError, ValueError):
        raise ResponseError("ERR invalid cursor") from None
    if cursor < 0:
        raise ResponseError("ERR invalid cursor")
    node = cursor % nodes
    return node, encode_args((args[0], str(cursor // nodes).encode(), *args[2:]))


def scan_finish(reply: Any, node: int, nodes: int) -> Any:
    if not isinstance(reply, (list, tuple)) or len(reply) != 2:
        return reply
    nxt = int(reply[0])
    new = nxt * nodes + node if nxt else (node + 1 if node + 1 < nodes else 0)
    text = str(new)
    return [text.encode() if type(reply[0]) is bytes else text, reply[1]]


# --------------------------------------------------------------------------- topology

CLUSTER_SLOTS = encode("CLUSTER SLOTS", ())


class Topology:
    """16384-entry slot -> node index table plus the node addresses (in slot order)."""

    __slots__ = ("slots", "nodes", "replicas")

    def __init__(
        self,
        slots: List[int],
        nodes: List[Tuple[str, int]],
        replicas: Optional[List[List[Tuple[str, int]]]] = None,
    ) -> None:
        self.slots = slots
        self.nodes = nodes
        #: per node of ``nodes``: the replicas ``CLUSTER SLOTS`` lists after it
        self.replicas = replicas if replicas is not None else [[] for _ in nodes]


def _text(v: Any) -> str:
    return v.decode() if type(v) is bytes else str(v)


def parse_slots(reply: Any, seed_host: str) -> Optional[Topology]:
    """Reads a ``CLUSTER SLOTS`` reply. None for an error or an empty map."""
    if not isinstance(reply, list) or not reply:
        return None
    slots = [-1] * SLOT_COUNT
    nodes: List[Tuple[str, int]] = []
    replicas: List[List[Tuple[str, int]]] = []
    index: Dict[Tuple[str, int], int] = {}
    try:
        for rng in sorted(reply, key=lambda r: int(r[0])):
            first, last, primary = int(rng[0]), int(rng[1]), rng[2]
            host = _text(primary[0])
            if host in ("", "0.0.0.0", "::"):
                host = seed_host
            addr = (host, int(primary[1]))
            idx = index.get(addr)
            if idx is None:
                idx = index[addr] = len(nodes)
                nodes.append(addr)
                replicas.append([])
            for extra in rng[3:]:  # replicas follow the primary; one that cannot be read is skipped
                try:
                    if not isinstance(extra, list):
                        continue
                    rhost = _text(extra[0])
                    if rhost in ("", "0.0.0.0", "::"):
                        rhost = seed_host
                    raddr = (rhost, int(extra[1]))
                except (IndexError, TypeError, ValueError):
                    continue
                if raddr not in replicas[idx]:
                    replicas[idx].append(raddr)
            for s in range(max(0, first), min(SLOT_COUNT - 1, last) + 1):
                slots[s] = idx
    except (IndexError, TypeError, ValueError):
        return None
    return Topology(slots, nodes, replicas)


def parse_moved(err: BaseException) -> Optional[Tuple[int, str, int]]:
    """``(slot, host, port)`` of a ``MOVED slot host:port`` error; None for anything else."""
    return _parse_redirect(err, "MOVED ")


def parse_ask(err: BaseException) -> Optional[Tuple[int, str, int]]:
    """``(slot, host, port)`` of an ``ASK slot host:port`` error; None for anything else."""
    return _parse_redirect(err, "ASK ")


def is_tryagain(err: Any) -> bool:
    """True for a ``TRYAGAIN`` error reply (a ``ResponseError``; any other value is False)."""
    if not isinstance(err, BaseException):
        return False
    msg = err.args[0] if err.args else ""
    return isinstance(msg, str) and msg.startswith("TRYAGAIN")


def _parse_redirect(err: BaseException, prefix: str) -> Optional[Tuple[int, str, int]]:
    msg = err.args[0] if err.args else ""
    if not isinstance(msg, str) or not msg.startswith(prefix):
        return None
    parts = msg.split()
    if len(parts) < 3:
        return None
    host, _, port = parts[2].rpartition(":")
    try:
        return int(parts[1]), host, int(port)
    except ValueError:
        return None
