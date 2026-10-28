# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Python client for Ruvio, a Redis-compatible server with extra native commands.

Two clients share the same typed command methods:

* ``ruvio.Ruvio`` - synchronous client (one socket, not thread-safe).
* ``ruvio.asyncio.Ruvio`` - asyncio client (one pipelined, multiplexed connection shared by every
  coroutine). Its command methods return awaitables.

``ruvio.ClusterRuvio`` (and ``ruvio.asyncio.ClusterRuvio``) route each key to the shard that owns it;
``Ruvio(..., cluster=True)`` builds one. Reading help for a command: ``help(ruvio.Ruvio.set)`` or hover
in your IDE; each method lists its arguments, return value, errors and an example.

Keyword arguments such as the GEO unit, list side or ``BITOP`` operation have typed constants
(:class:`GeoUnit`, :class:`ListSide`, :class:`BitOperation` ...); plain strings are accepted too.

Also exported: :func:`key_slot` (hash slot of a key), :func:`parse_url`, :func:`parse_info`, the result
tuples (:class:`FillResult`, :class:`IdemResult`, :class:`GeoResult`, :class:`StreamEntry`,
:class:`PendingSummary`, :class:`PendingEntry`, :class:`AutoClaimResult`, :class:`Message`) and the
exceptions, all deriving from :class:`RuvioError`.

Example:
    from ruvio import Ruvio

    with Ruvio("127.0.0.1", 6379) as client:
        client.set("greeting", "hello", ex=60)
        print(client.get("greeting"))
"""

from ._commands import parse_info
from ._constants import (
    Aggregate,
    BitFieldOp,
    BitOperation,
    BitUnit,
    ChangeType,
    ClientType,
    GeoOrder,
    GeoUnit,
    InfoSection,
    InsertPosition,
    KeyType,
    LexBound,
    ListSide,
    ExpireCondition,
    Overflow,
    ScoreBound,
    StreamId,
    StreamTrim,
    TrimMode,
    TrimStrategy,
    bit_type,
    change_types,
    lex_exclusive,
    lex_inclusive,
    score_exclusive,
    signed,
    unsigned,
)
from ._models import (
    AutoClaimResult,
    FillResult,
    GeoResult,
    IdemResult,
    LimitOutcome,
    Message,
    PendingEntry,
    PendingSummary,
    StreamEntry,
)
from ._cluster import ReadPolicy, key_slot
from .client import Lock, Pipeline, PubSub, Ruvio, parse_url
from .cluster import ClusterRuvio
from .errors import (
    BusyError,
    ConnectionError,
    CrossSlotError,
    ExecAbortError,
    LockError,
    MovedError,
    NoGroupError,
    NoScriptError,
    ProtocolError,
    ResponseError,
    RuvioError,
    TimeoutError,
    WatchError,
    WrongTypeError,
    is_transient,
    may_have_run,
)

__version__ = "0.1.2"

__all__ = [
    "Ruvio",
    "ClusterRuvio",
    "ReadPolicy",
    "key_slot",
    "Pipeline",
    "PubSub",
    "Lock",
    "parse_url",
    "parse_info",
    "Aggregate",
    "BitFieldOp",
    "BitOperation",
    "BitUnit",
    "ChangeType",
    "ClientType",
    "GeoOrder",
    "GeoUnit",
    "InfoSection",
    "InsertPosition",
    "KeyType",
    "LexBound",
    "ListSide",
    "ExpireCondition",
    "Overflow",
    "ScoreBound",
    "StreamId",
    "StreamTrim",
    "TrimMode",
    "TrimStrategy",
    "bit_type",
    "change_types",
    "lex_exclusive",
    "lex_inclusive",
    "score_exclusive",
    "signed",
    "unsigned",
    "AutoClaimResult",
    "FillResult",
    "GeoResult",
    "LimitOutcome",
    "IdemResult",
    "Message",
    "PendingEntry",
    "PendingSummary",
    "StreamEntry",
    "is_transient",
    "may_have_run",
    "BusyError",
    "ConnectionError",
    "CrossSlotError",
    "ExecAbortError",
    "LockError",
    "MovedError",
    "NoGroupError",
    "NoScriptError",
    "ProtocolError",
    "ResponseError",
    "RuvioError",
    "TimeoutError",
    "WatchError",
    "WrongTypeError",
    "__version__",
]
