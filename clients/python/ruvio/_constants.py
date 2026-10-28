# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Typed keyword constants for command arguments.

Every class is a ``str`` enum, so a member can be passed anywhere the plain string is
accepted (``unit=GeoUnit.KILOMETERS`` and ``unit="km"`` send the same bytes) and
existing calls keep working. Each member carries its wire bytes, built once at import;
the RESP encoder reads them directly, so a member costs no more than a ``bytes``
argument (no ``str`` conversion, no encode, no allocation per call).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Set, Tuple, Union

__all__ = [
    "Keyword",
    "GeoUnit",
    "GeoOrder",
    "InsertPosition",
    "ListSide",
    "ExpireCondition",
    "BitOperation",
    "BitUnit",
    "Aggregate",
    "KeyType",
    "BitFieldOp",
    "Overflow",
    "InfoSection",
    "ClientType",
    "ChangeType",
    "TrimStrategy",
    "TrimMode",
    "StreamTrim",
    "StreamId",
    "ScoreBound",
    "LexBound",
    "change_types",
    "score_exclusive",
    "lex_inclusive",
    "lex_exclusive",
    "bit_type",
    "signed",
    "unsigned",
]

KEYWORD_TYPES: Set[type] = set()
"""Every keyword enum class; the encoder checks ``type(arg) in KEYWORD_TYPES``."""


class Keyword(str, Enum):
    """Base of the keyword enums: a ``str`` whose wire bytes are encoded once."""

    def __new__(cls, value: str) -> "Keyword":
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj._wire = value.encode()  # type: ignore[attr-defined]
        return obj

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        KEYWORD_TYPES.add(cls)

    def __str__(self) -> str:
        return self._value_  # type: ignore[no-any-return]

    __format__ = str.__format__

    @property
    def wire(self) -> bytes:
        """The pre-encoded argument bytes (``b"km"``)."""
        return self._wire  # type: ignore[attr-defined, no-any-return]


class GeoUnit(Keyword):
    """Distance unit of ``GEODIST``, ``GEOSEARCH*`` and ``GEORADIUS*``: ``"m"``, ``"km"``, ``"ft"`` or ``"mi"``.

    Example: ``r.geosearch("cities", member="A", radius=5, unit=GeoUnit.KILOMETERS)``.
    """

    METERS = "m"
    KILOMETERS = "km"
    FEET = "ft"
    MILES = "mi"


class GeoOrder(Keyword):
    """Result order of the GEO search commands (``sort=``): ``"ASC"`` or ``"DESC"``.

    Example: ``r.geosearch("cities", member="A", radius=5, sort=GeoOrder.ASC)``.
    """

    ASC = "ASC"
    DESC = "DESC"


class InsertPosition(Keyword):
    """Where ``LINSERT`` puts the value relative to the pivot: ``"BEFORE"`` or ``"AFTER"``.

    Example: ``r.linsert("q", InsertPosition.BEFORE, "b", "a")``.
    """

    BEFORE = "BEFORE"
    AFTER = "AFTER"


class ListSide(Keyword):
    """End of a list for ``LMOVE``, ``BLMOVE``, ``LMPOP`` and ``BLMPOP``: ``"LEFT"`` or ``"RIGHT"``.

    Example: ``r.lmove("src", "dst", ListSide.LEFT, ListSide.RIGHT)``.
    """

    LEFT = "LEFT"
    RIGHT = "RIGHT"


class ExpireCondition(Keyword):
    """Condition of ``EXPIRE``, ``PEXPIRE``, ``EXPIREAT`` and ``PEXPIREAT`` (``condition=``).

    ``NX`` sets the deadline only when the key has none, ``XX`` only when it has one, ``GT`` only when the
    new deadline is later and ``LT`` only when it is earlier. A key without a deadline counts as infinite.
    A failed condition replies ``False``.

    Example: ``r.expire("k", 60, condition=ExpireCondition.NX)``.
    """

    NX = "NX"
    XX = "XX"
    GT = "GT"
    LT = "LT"


class BitOperation(Keyword):
    """Operation of ``BITOP``: ``"AND"``, ``"OR"``, ``"XOR"`` or ``"NOT"``.

    Example: ``r.bitop(BitOperation.AND, "dest", "a", "b")``.
    """

    AND = "AND"
    OR = "OR"
    XOR = "XOR"
    NOT = "NOT"


class BitUnit(Keyword):
    """Range unit of ``BITCOUNT`` (``mode=``): ``"BYTE"`` or ``"BIT"``.

    Example: ``r.bitcount("k", 0, 7, BitUnit.BIT)``.
    """

    BYTE = "BYTE"
    BIT = "BIT"


class Aggregate(Keyword):
    """Score combination of ``ZUNION*`` / ``ZINTER*`` (``aggregate=``): ``"SUM"``, ``"MIN"`` or ``"MAX"``.

    Example: ``r.zunionstore("dst", ["a", "b"], aggregate=Aggregate.MAX)``.
    """

    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"


class KeyType(Keyword):
    """Value of ``SCAN ... TYPE`` (``type=``): ``string``, ``json``, ``list``, ``set``, ``hash``, ``zset``,
    ``bloomfilter``, ``stream``, ``lease``, ``limit``, ``semaphore`` or ``once``.

    Example: ``r.scan(0, type=KeyType.HASH)``.
    """

    STRING = "string"
    JSON = "json"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    BLOOM_FILTER = "bloomfilter"
    STREAM = "stream"
    LEASE = "lease"
    LIMIT = "limit"
    SEMAPHORE = "semaphore"
    ONCE = "once"


class BitFieldOp(Keyword):
    """Sub-command word of ``BITFIELD``: ``"GET"``, ``"SET"`` or ``"INCRBY"``.

    Example: ``r.bitfield("k", BitFieldOp.INCRBY, unsigned(8), 0, 10)``.
    """

    GET = "GET"
    SET = "SET"
    INCRBY = "INCRBY"


class Overflow(Keyword):
    """Overflow policy after ``BITFIELD ... OVERFLOW``: ``"WRAP"``, ``"SAT"`` or ``"FAIL"``.

    Example: ``r.bitfield("k", "OVERFLOW", Overflow.SAT, BitFieldOp.INCRBY, unsigned(8), 0, 10)``.
    """

    WRAP = "WRAP"
    SAT = "SAT"
    FAIL = "FAIL"


_SIGNED = tuple(f"i{n}" for n in range(0, 65))
_UNSIGNED = tuple(f"u{n}" for n in range(0, 64))


def signed(bits: int) -> str:
    """``BITFIELD`` type word for a signed integer of ``bits`` bits (1-64): ``signed(8)`` is ``"i8"``."""
    if not 1 <= bits <= 64:
        raise ValueError("signed bit width must be 1-64")
    return _SIGNED[bits]


def unsigned(bits: int) -> str:
    """``BITFIELD`` type word for an unsigned integer of ``bits`` bits (1-63): ``unsigned(8)`` is ``"u8"``."""
    if not 1 <= bits <= 63:
        raise ValueError("unsigned bit width must be 1-63")
    return _UNSIGNED[bits]


def bit_type(bits: int, is_signed: bool = False) -> str:
    """``BITFIELD`` type word: ``bit_type(8)`` is ``"u8"``, ``bit_type(16, True)`` is ``"i16"``."""
    return signed(bits) if is_signed else unsigned(bits)


class InfoSection(Keyword):
    """Section name of ``INFO`` (``section=``). Names the server does not know give an empty reply.

    Values: ``server``, ``stats``, ``buffers``, ``memory``, ``memorybreakdown``, ``keyspace``, ``snapshot``,
    ``cluster``, ``replication``, ``commandcounters``, ``latencystats``, ``logs``, ``slabs``, ``all``,
    ``default`` and ``prometheus`` (Prometheus text format instead of sections).

    Example: ``ruvio.parse_info(r.info(InfoSection.MEMORY_BREAKDOWN))``.
    """

    SERVER = "server"
    STATS = "stats"
    BUFFERS = "buffers"
    MEMORY = "memory"
    MEMORY_BREAKDOWN = "memorybreakdown"
    KEYSPACE = "keyspace"
    SNAPSHOT = "snapshot"
    CLUSTER = "cluster"
    REPLICATION = "replication"
    COMMAND_COUNTERS = "commandcounters"
    LATENCY_STATS = "latencystats"
    LOGS = "logs"
    SLABS = "slabs"
    ALL = "all"
    DEFAULT = "default"
    PROMETHEUS = "prometheus"


class ClientType(Keyword):
    """Connection type of ``CLIENT LIST TYPE`` (``type=``): ``normal``, ``master``, ``replica`` or ``pubsub``.

    Example: ``r.client_list(ClientType.PUBSUB)``.
    """

    NORMAL = "normal"
    MASTER = "master"
    REPLICA = "replica"
    PUBSUB = "pubsub"


class ChangeType(Keyword):
    """Event kind of ``CHANGES SUBSCRIBE ... TYPES``: ``put``, ``json``, ``del``, ``expire``, ``rename``, ``list``,
    ``set``, ``hash``, ``zset``, ``stream``, ``flush`` or ``other``.

    Join several with :func:`change_types`.

    Example: ``r.execute_command("CHANGES", "SUBSCRIBE", "TYPES", change_types(ChangeType.PUT, ChangeType.DEL))``.
    """

    PUT = "put"
    JSON = "json"
    DEL = "del"
    EXPIRE = "expire"
    RENAME = "rename"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    STREAM = "stream"
    FLUSH = "flush"
    OTHER = "other"


def change_types(*types: Union[ChangeType, str]) -> str:
    """Comma-joined list for ``CHANGES SUBSCRIBE ... TYPES``: ``change_types(ChangeType.PUT, "del")`` is ``"put,del"``.

    Raises:
        ValueError: No type given.

    Example: ``r.execute_command("CHANGES", "SUBSCRIBE", "TYPES", change_types(ChangeType.PUT, ChangeType.DEL))``.
    """
    if not types:
        raise ValueError("give at least one change type")
    return ",".join(str(t) for t in types)


class TrimStrategy(Keyword):
    """Trim rule of ``XADD`` / ``XTRIM``: ``MAXLEN`` (keep this many entries) or ``MINID`` (drop lower ids)."""

    MAXLEN = "MAXLEN"
    MINID = "MINID"


class TrimMode(Keyword):
    """Trim mode word of ``XADD`` / ``XTRIM``: ``"="`` (exact) or ``"~"`` (approximate; Ruvio trims exactly either way)."""

    EXACT = "="
    APPROXIMATE = "~"


class StreamTrim:
    """One stream trim option for ``xadd(trim=...)`` and ``xtrim(trim=...)``: strategy, mode and threshold.

    The server accepts ``MAXLEN`` and ``MINID``, each with ``=`` or ``~``; it does not accept ``LIMIT``.
    The argument tuple is built once at construction and sent as is.

    Example::

        r.xadd("events", {"type": "signup"}, trim=StreamTrim.maxlen(10_000, TrimMode.APPROXIMATE))
        r.xtrim("events", trim=StreamTrim.minid("1700000000000-0"))
    """

    __slots__ = ("strategy", "mode", "threshold", "args")

    def __init__(self, strategy: TrimStrategy, threshold: Union[int, str], mode: TrimMode = TrimMode.EXACT) -> None:
        """``strategy`` with its ``threshold`` (an ``int`` for MAXLEN, an id for MINID) and ``mode``."""
        self.strategy = TrimStrategy(strategy)
        self.mode = TrimMode(mode)
        self.threshold = threshold
        self.args: Tuple[Any, ...] = (self.strategy, self.mode, threshold)

    @classmethod
    def maxlen(cls, count: int, mode: TrimMode = TrimMode.EXACT) -> "StreamTrim":
        """``MAXLEN mode count``: keep at most ``count`` newest entries. Example: ``StreamTrim.maxlen(1000)``."""
        return cls(TrimStrategy.MAXLEN, count, mode)

    @classmethod
    def minid(cls, id: str, mode: TrimMode = TrimMode.EXACT) -> "StreamTrim":  # noqa: A002
        """``MINID mode id``: drop entries with a lower id. Example: ``StreamTrim.minid("1700000000000-0")``."""
        return cls(TrimStrategy.MINID, id, mode)

    def __repr__(self) -> str:
        return f"StreamTrim({self.strategy.value} {self.mode.value} {self.threshold})"


class StreamId(Keyword):
    """Special stream ids accepted by the stream commands.

    ``NEW_ENTRY`` (``"*"``) lets ``XADD`` pick the id; ``LAST`` (``"$"``) is the newest entry (``XREAD``,
    ``XGROUP CREATE``); ``NEW_MESSAGES`` (``">"``) asks ``XREADGROUP`` for never-delivered entries; ``MIN``
    (``"-"``) and ``MAX`` (``"+"``) are the range ends of ``XRANGE`` / ``XREVRANGE`` / ``XPENDING``.

    Example: ``r.xrange("events", StreamId.MIN, StreamId.MAX)``.
    """

    NEW_ENTRY = "*"
    LAST = "$"
    NEW_MESSAGES = ">"
    MIN = "-"
    MAX = "+"


class ScoreBound(Keyword):
    """Infinite sorted-set score bounds for ``ZRANGEBYSCORE``, ``ZCOUNT``, ``ZREMRANGEBYSCORE`` ...

    Example: ``r.zrangebyscore("z", ScoreBound.NEGATIVE_INFINITY, score_exclusive(10))``.
    """

    NEGATIVE_INFINITY = "-inf"
    POSITIVE_INFINITY = "+inf"


class LexBound(Keyword):
    """Open lexicographic range ends for ``ZRANGEBYLEX``, ``ZLEXCOUNT`` and ``ZREMRANGEBYLEX``.

    ``MIN`` is ``"-"`` (before every member) and ``MAX`` is ``"+"`` (after every member).

    Example: ``r.zrangebylex("z", LexBound.MIN, lex_inclusive("m"))``.
    """

    MIN = "-"
    MAX = "+"


def score_exclusive(score: Union[int, float]) -> str:
    """Exclusive score bound: ``score_exclusive(10)`` is ``"(10"`` (strictly greater / less than 10)."""
    return f"({score!r}" if isinstance(score, float) else f"({score}"


def lex_inclusive(member: str) -> str:
    """Inclusive lexicographic bound: ``lex_inclusive("m")`` is ``"[m"``."""
    return "[" + member


def lex_exclusive(member: str) -> str:
    """Exclusive lexicographic bound: ``lex_exclusive("m")`` is ``"(m"``."""
    return "(" + member
