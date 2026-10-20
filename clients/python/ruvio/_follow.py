# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Following a failover: the parts the sync and asyncio clients share.

A standalone client with several seeds, or with ``sentinel_master``, asks the nodes which one is the
primary (``ROLE``, or ``SENTINEL get-master-addr-by-name`` with a coordinator name) when it reconnects and
after a ``READONLY`` reply. This module holds the reply parsing, the probe timing and the waits between
rounds; the sockets are in :mod:`ruvio.client` and :mod:`ruvio.asyncio.client`.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from .errors import ResponseError

Address = Tuple[str, int]

#: Rounds a ``READONLY`` reply may start, and the waits between them (100 ms doubling to a 2 s cap, 7.1 s in all).
ROUNDS = 8
WAITS = (0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0)

MASTER = "master"
REPLICA = "slave"


def is_readonly(error: BaseException) -> bool:
    """True for a ``READONLY`` error reply: the text is ``READONLY`` alone or ``READONLY`` and a space."""
    return isinstance(error, ResponseError) and error.code == "READONLY"


def probe_wait(connect_timeout: Optional[float], command_timeout: Optional[float]) -> Optional[float]:
    """The reply wait of a probe: the smaller of the connect timeout and the command timeout (the connect
    timeout when the command timeout is off, and the command timeout when the connect timeout is off)."""
    if command_timeout is None:
        return connect_timeout
    if connect_timeout is None:
        return command_timeout
    return min(connect_timeout, command_timeout)


def _text(v: Any) -> Optional[str]:
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return None
    return v if isinstance(v, str) else None


def _port(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (bytes, str)):
        try:
            v = int(v)
        except ValueError:
            return None
    if isinstance(v, int) and 0 < v < 65536:
        return v
    return None


def parse_role(reply: Any) -> Optional[Tuple[str, Optional[Address]]]:
    """Reads a ``ROLE`` reply: ``("master", None)``, ``("slave", (host, port))`` with the address the replica
    reports for its primary (``None`` when it reports no host or port 0), or ``None`` for anything else."""
    if not isinstance(reply, list) or not reply:
        return None
    kind = _text(reply[0])
    if kind is None:
        return None
    kind = kind.lower()
    if kind == MASTER:
        return MASTER, None
    if kind != REPLICA:
        return None
    host = _text(reply[1]) if len(reply) > 1 else None
    port = _port(reply[2]) if len(reply) > 2 else None
    if not host or port is None:
        return REPLICA, None
    return REPLICA, (host, port)


def parse_master_addr(reply: Any) -> Optional[Address]:
    """Reads a ``SENTINEL get-master-addr-by-name`` reply: an array of host and port (a string port is fine)."""
    if not isinstance(reply, list) or len(reply) != 2:
        return None
    host = _text(reply[0])
    port = _port(reply[1])
    if not host or port is None:
        return None
    return host, port


def ordered(seeds: List[Address], start: int) -> List[Address]:
    """The seeds in order from ``start``, wrapping around."""
    n = len(seeds)
    return [seeds[(start + k) % n] for k in range(n)]
