# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Small result types returned by typed commands.

They are :class:`typing.NamedTuple` classes, so they unpack like tuples (``state, value, token = r``)
and also have named attributes (``r.state``).
"""

from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

__all__ = [
    "FillResult",
    "LimitOutcome",
    "IdemResult",
    "StreamEntry",
    "GeoResult",
    "PendingSummary",
    "PendingEntry",
    "AutoClaimResult",
    "Message",
    "Value",
]

Value = Union[str, bytes]
"""A reply string: ``str`` on a decoding client, ``bytes`` on ``client.raw`` / ``decode_responses=False``."""


class LimitOutcome(NamedTuple):
    """Outcome of the ``*_with_retry`` limit calls (see :meth:`ruvio._commands.Commands.limit_sliding_with_retry`).

    Attributes:
        allowed: ``True`` when this call was let through.
        remaining: Allowance left after this call (``0`` when refused).
        retry_after_ms: Milliseconds until the same call would pass (``0`` when it was let through);
            fit for a ``Retry-After`` header.
    """

    allowed: bool
    remaining: int
    retry_after_ms: int


class FillResult(NamedTuple):
    """Outcome of ``FILLGET`` (see :meth:`ruvio._commands.Commands.fillget`).

    Attributes:
        state: ``"HIT"``, ``"FILL"``, ``"BUSY"`` or ``"STALE"``.
        value: The cached value for ``HIT`` and ``STALE``; ``None`` otherwise.
        token: For ``FILL``: the token to pass to ``fillset``. For ``STALE``: a token when this caller must
            refresh the value, ``None`` when someone else already does. ``None`` for ``HIT`` and ``BUSY``.

    * ``HIT``: use ``value``.
    * ``FILL``: build the value and call ``fillset`` with ``token``.
    * ``BUSY``: another caller is filling; ask again shortly.
    * ``STALE``: ``value`` is a stale copy; refresh it if ``token`` is set.
    """

    state: str
    value: Optional[Value]
    token: Optional[Value]


class IdemResult(NamedTuple):
    """Outcome of ``IDEM BEGIN`` (see :meth:`ruvio._commands.Commands.idem_begin`).

    Attributes:
        state: ``"acquired"`` (run the operation now), ``"pending"`` (same request still running),
            ``"conflict"`` (different fingerprint) or ``"completed"``.
        result: The stored result for ``"completed"``, otherwise ``None``.
    """

    state: str
    result: Optional[Value]


class StreamEntry(NamedTuple):
    """One stream entry.

    Attributes:
        id: The entry id as text, ``"<ms>-<seq>"``.
        fields: ``(field, value)`` pairs in stored order; a field name may repeat.
    """
    id: str
    fields: List[Tuple[Any, Any]]


class GeoResult(NamedTuple):
    """One ``GEOSEARCH``/``GEORADIUS`` row when ``withdist``, ``withhash`` or ``withcoord`` is used.

    Attributes:
        member: The member name.
        dist: Distance from the center in the requested unit, or ``None`` if ``withdist`` was not set.
        hash: The raw 52-bit geohash integer, or ``None`` if ``withhash`` was not set.
        coord: ``(longitude, latitude)``, or ``None`` if ``withcoord`` was not set.
    """

    member: Value
    dist: Optional[float]
    hash: Optional[int]
    coord: Optional[Tuple[float, float]]


class PendingSummary(NamedTuple):
    """Summary form of ``XPENDING`` (see :meth:`ruvio._commands.Commands.xpending`).

    Attributes:
        count: Number of pending entries in the group.
        min_id: Smallest pending id, ``None`` when there are none.
        max_id: Greatest pending id, ``None`` when there are none.
        consumers: ``[(consumer_name, pending_count), ...]``.
    """
    count: int
    min_id: Optional[str]
    max_id: Optional[str]
    consumers: List[Tuple[str, int]]


class PendingEntry(NamedTuple):
    """One row of the extended ``XPENDING``.

    Attributes:
        id: Entry id.
        consumer: Name of the consumer that holds it.
        idle_ms: Milliseconds since it was last delivered.
        deliveries: How many times it was delivered.
    """
    id: str
    consumer: str
    idle_ms: int
    deliveries: int


class AutoClaimResult(NamedTuple):
    """Reply of ``XAUTOCLAIM`` (see :meth:`ruvio._commands.Commands.xautoclaim`).

    Attributes:
        next_id: Id to continue scanning from; ``"0-0"`` when the scan is complete.
        claimed: Claimed :class:`StreamEntry` objects (or ids with ``justid``).
        deleted: Pending ids whose stream entry no longer exists.
    """
    next_id: str
    claimed: List[Any]
    deleted: List[str]


class Message(NamedTuple):
    """A Pub/Sub delivery.

    Attributes:
        kind: ``message``, ``pmessage`` (pattern match), ``smessage`` (sharded channel), ``subscribe``,
            ``unsubscribe``, ``psubscribe``, ``punsubscribe``, ``ssubscribe``, ``sunsubscribe`` or ``pong``.
            Any non-message reply carries kind ``reply``.
        channel: The channel (``None`` for ``pong``).
        data: The payload; for subscription confirmations, the number of active subscriptions.
        pattern: The matching pattern for ``pmessage``, otherwise ``None``.
    """

    kind: str
    channel: Optional[Value]
    data: Any
    pattern: Optional[Value] = None
