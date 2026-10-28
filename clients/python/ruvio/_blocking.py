# Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.
"""How long a blocking command may legitimately wait, so the command timeout can add it.

The rule is the same in every Ruvio client: a command that carries its own wait gets
``wait + socket_timeout``; a wait of ``0`` (block forever) or an argument that does not parse means no
deadline for that call.

- ``BLPOP``, ``BRPOP``, ``BZPOPMIN``, ``BZPOPMAX``, ``BRPOPLPUSH``, ``BLMOVE``: last argument, seconds.
- ``BLMPOP``, ``BZMPOP``: first argument, seconds.
- ``XREAD``, ``XREADGROUP``: ``BLOCK ms`` (absent: not blocking).
- ``WAIT``, ``WAITAOF``: last argument, milliseconds.
- ``FILLGET key fill_ms WAIT wait_ms``: ``wait_ms``.
- ``SUBSCRIBE``, ``PSUBSCRIBE``, ``SSUBSCRIBE``, ``MONITOR``: push mode, no deadline.
"""

from typing import Iterable, List, Optional, Tuple

#: The wait of a command that blocks with no bound.
FOREVER = -1.0

# (name length << 8) | lower-case first letter of every name below; GET, SET, PING, ... miss it.
_CANDIDATES = frozenset(
    (len(n) << 8) | (ord(n[0]) | 0x20)
    for n in (
        "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX", "BRPOPLPUSH", "BLMOVE", "BLMPOP", "BZMPOP",
        "XREAD", "XREADGROUP", "WAIT", "WAITAOF", "FILLGET",
        "MONITOR", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE",
    )
)
_LAST_SECONDS = frozenset((b"BLPOP", b"BRPOP", b"BZPOPMIN", b"BZPOPMAX", b"BRPOPLPUSH", b"BLMOVE"))
_FIRST_SECONDS = frozenset((b"BLMPOP", b"BZMPOP"))
_LAST_MILLIS = frozenset((b"WAIT", b"WAITAOF"))
_PUSH = frozenset((b"MONITOR", b"SUBSCRIBE", b"PSUBSCRIBE", b"SSUBSCRIBE"))


def blocking_wait(data: bytes, p: int = 0) -> float:
    """The wait, in seconds, the encoded command at ``data[p:]`` adds to the command timeout:
    ``0`` when it does not block, :data:`FOREVER` when it may block with no bound. A plain command
    costs a length and first-letter check."""
    try:
        i = p + 4 if data[p + 2] == 13 else data.index(b"\r\n", p + 1) + 2  # past "*<argc>\r\n"
        if data[i + 2] == 13:  # "$<1 digit>\r\n"
            n = data[i + 1] - 48
            s = i + 4
        elif data[i + 3] == 13:  # "$<2 digits>\r\n"
            n = (data[i + 1] - 48) * 10 + data[i + 2] - 48
            s = i + 5
        else:
            return 0.0
        if ((n << 8) | (data[s] | 0x20)) not in _CANDIDATES:
            return 0.0
    except (IndexError, ValueError):
        return 0.0
    return _classify(data, p)[0]


def _classify(data: bytes, start: int = 0) -> Tuple[float, int]:
    """Parses one command from ``start``: its wait and the offset after it (-1 when it does not parse)."""
    args = _args(data, start)
    if args is None:
        return 0.0, -1
    parts, end = args
    name = parts[0].upper()
    rest = parts[1:]
    if name in _LAST_SECONDS:
        return _seconds(rest[-1] if rest else None), end
    if name in _FIRST_SECONDS:
        return _seconds(rest[0] if rest else None), end
    if name in _LAST_MILLIS:
        return _millis(rest[-1] if rest else None), end
    if name == b"XREAD" or name == b"XREADGROUP":
        # XREADGROUP GROUP <group> <consumer>: names that may spell BLOCK
        i = 0 if name == b"XREAD" else 3
        while i < len(rest):
            word = rest[i].upper()
            if word == b"STREAMS":
                break
            if word == b"BLOCK":
                return _millis(rest[i + 1] if i + 1 < len(rest) else None), end
            i += 1
        return 0.0, end
    if name == b"FILLGET":
        if len(rest) >= 4 and rest[2].upper() == b"WAIT":
            return _millis(rest[3]), end
        return 0.0, end
    if name in _PUSH:
        return FOREVER, end
    return 0.0, end


def _args(data: bytes, p: int) -> Optional[Tuple[List[bytes], int]]:
    try:
        e = data.index(b"\r\n", p)
        if data[p] != 42:  # '*'
            return None
        count = int(data[p + 1 : e])
        p = e + 2
        out = []
        for _ in range(count):
            e = data.index(b"\r\n", p)
            if data[p] != 36:  # '$'
                return None
            n = int(data[p + 1 : e])
            p = e + 2
            if p + n + 2 > len(data):
                return None
            out.append(data[p : p + n])
            p += n + 2
        return (out, p) if out else None
    except (IndexError, ValueError):
        return None


def _seconds(arg: Optional[bytes]) -> float:
    try:
        v = float(arg)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return FOREVER
    return v if 0 < v < float("inf") else FOREVER


def _millis(arg: Optional[bytes]) -> float:
    try:
        v = int(arg)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return FOREVER
    return v / 1000.0 if v > 0 else FOREVER


def widest(a: float, b: float) -> float:
    """The wider of two waits."""
    if a == FOREVER or b == FOREVER:
        return FOREVER
    return a if a > b else b


def pipeline_wait(frames: Iterable[bytes]) -> float:
    """The widest wait of a list of encoded commands (one per item)."""
    w = 0.0
    for f in frames:
        x = blocking_wait(f)
        if x:
            w = widest(w, x)
            if w == FOREVER:
                return w
    return w


def frames_wait(data: bytes) -> float:
    """The widest wait of the commands concatenated in ``data``."""
    p = 0
    w = 0.0
    while 0 <= p < len(data):
        x = blocking_wait(data, p)
        if x:
            w = widest(w, x)
            if w == FOREVER:
                return w
        p = _skip(data, p)
    return w


def _skip(data: bytes, p: int) -> int:
    try:
        e = data.index(b"\r\n", p)
        if data[p] != 42:
            return -1
        count = int(data[p + 1 : e])
        p = e + 2
        for _ in range(count):
            e = data.index(b"\r\n", p)
            p = e + 2 + int(data[p + 1 : e]) + 2
        return p
    except (IndexError, ValueError):
        return -1
