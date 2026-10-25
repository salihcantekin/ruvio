# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""RESP2/RESP3 encoder and an in-place, resumable reply parser.

The encoder turns ``(name, args)`` into one ``bytes`` object. Command names are
pre-encoded once (``*N`` is the only per-call header), small integers come from
a table of complete bulk fragments, and bulk length headers come from a table.

The parser works on a reusable ``bytearray``: the socket ``recv_into`` (sync) or
``BufferedProtocol.get_buffer`` (asyncio) writes straight into it, and
:meth:`Parser.gets` reads one complete reply in place. A reply that is not
complete yet keeps its partially built aggregates on a small stack, so a large
array arriving in many chunks is never re-parsed from its first element.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ._constants import KEYWORD_TYPES
from .errors import ProtocolError, ResponseError, make_error

# What one reply may announce, the same in every client: a bulk string up to 64 MiB, an aggregate of up to 1,048,576 elements.
MAX_BULK_BYTES = 64 * 1024 * 1024
MAX_ELEMENTS = 1 << 20

__all__ = ["encode", "head", "Parser", "NEED", "Push", "HiredisParser", "make_parser"]

_CRLF = b"\r\n"
_TABLE = 4096
_BULK_LEN = [b"$%d\r\n" % i for i in range(_TABLE)]
_ARRAY_LEN = [b"*%d\r\n" % i for i in range(_TABLE)]
_SMALL_INTS = 65536
_INT_FRAG = [b"$%d\r\n%d\r\n" % (len(str(i)), i) for i in range(_SMALL_INTS)]

Head = Tuple[int, bytes]
_HEADS: Dict[str, Head] = {}


def head(name: str) -> Head:
    """Pre-encodes ``name`` ("GET", "XGROUP CREATE", ...) as ``(word_count, fragment)``."""
    cached = _HEADS.get(name)
    if cached is None:
        words = name.split(" ")
        cached = (len(words), b"".join(_BULK_LEN[len(w)] + w.encode() + _CRLF for w in words))
        _HEADS[name] = cached
    return cached


def _bulk(a: Any, parts: List[bytes]) -> None:
    """Slow path for argument types other than str/bytes/small int."""
    t = type(a)
    if t in KEYWORD_TYPES:
        b = a._wire
    elif t is int or t is bool:
        b = b"%d" % a
    elif t is float:
        b = repr(a).encode()
    elif t is bytearray or t is memoryview:
        b = bytes(a)
    elif isinstance(a, str):
        b = a.encode("utf-8", "surrogateescape")
    elif isinstance(a, (bytes, bytearray, memoryview)):
        b = bytes(a)
    elif isinstance(a, int):
        b = b"%d" % a
    elif isinstance(a, float):
        b = repr(a).encode()
    else:
        raise TypeError(f"cannot send {t.__name__} as a RESP argument; pass str, bytes, int or float")
    n = len(b)
    parts.append(_BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n)
    parts.append(b)
    parts.append(_CRLF)


def encode(name: str, args: Tuple[Any, ...]) -> bytes:
    """Encodes one command. ``name`` may hold sub-command words ("CLIENT LIST")."""
    h = _HEADS.get(name)
    if h is None:
        h = head(name)
    if len(args) == 1:  # fast path: one str/bytes argument (GET, INCR, TTL, ...)
        b = args[0]
        if type(b) is str:
            b = b.encode("utf-8", "surrogateescape")
        if type(b) is bytes and len(b) < _TABLE:
            return b"".join((_ARRAY_LEN[h[0] + 1], h[1], _BULK_LEN[len(b)], b, _CRLF))
    count = h[0] + len(args)
    parts = [_ARRAY_LEN[count] if count < _TABLE else b"*%d\r\n" % count, h[1]]
    append = parts.append
    for a in args:
        t = type(a)
        if t is str:
            b = a.encode("utf-8", "surrogateescape")
        elif t is bytes:
            b = a
        elif t is int and 0 <= a < _SMALL_INTS:
            append(_INT_FRAG[a])
            continue
        elif t in KEYWORD_TYPES:
            b = a._wire
        else:
            _bulk(a, parts)
            continue
        n = len(b)
        append(_BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n)
        append(b)
        append(_CRLF)
    return b"".join(parts)


def encode_args(args: Tuple[Any, ...]) -> bytes:
    """Encodes a command given entirely as arguments (``execute`` escape hatch)."""
    count = len(args)
    parts = [_ARRAY_LEN[count] if count < _TABLE else b"*%d\r\n" % count]
    for a in args:
        if type(a) is int and 0 <= a < _SMALL_INTS:
            parts.append(_INT_FRAG[a])
        elif type(a) is str:
            b = a.encode("utf-8", "surrogateescape")
            parts.append(_BULK_LEN[len(b)] if len(b) < _TABLE else b"$%d\r\n" % len(b))
            parts.append(b)
            parts.append(_CRLF)
        else:
            _bulk(a, parts) if type(a) is not bytes else parts.extend(
                (_BULK_LEN[len(a)] if len(a) < _TABLE else b"$%d\r\n" % len(a), a, _CRLF)
            )
    return b"".join(parts)


class _Need:
    __slots__ = ()

    def __repr__(self) -> str:
        return "NEED"


NEED: Any = _Need()
"""Returned by :meth:`Parser.gets` when the buffer does not hold a complete reply yet."""


class Push(list):  # type: ignore[type-arg]
    """A RESP3 out-of-band push (``>``), e.g. a CLIENT TRACKING invalidation."""


class Parser:
    """Resumable RESP2/RESP3 reply parser over a reusable buffer."""

    __slots__ = ("buf", "mv", "start", "end", "stack", "decode")

    def __init__(self, size: int = 65536, decode: bool = True) -> None:
        self.buf = bytearray(size)
        self.mv = memoryview(self.buf)
        self.start = 0
        self.end = 0
        self.stack: List[list] = []  # type: ignore[type-arg]
        self.decode = decode

    # -- buffer management -------------------------------------------------
    def get_buffer(self, min_free: int = 16384) -> memoryview:
        """Free tail of the buffer for ``recv_into`` (compacts/grows when needed)."""
        start = self.start
        end = self.end
        if start == end:
            self.start = self.end = start = end = 0
        size = len(self.buf)
        if size - end < min_free:
            used = end - start
            if start and size - used >= min_free:
                self.buf[0:used] = self.buf[start:end]
            else:
                grown = bytearray(max(size * 2, used + min_free))
                grown[0:used] = self.mv[start:end]
                self.mv.release()
                self.buf = grown
                self.mv = memoryview(grown)
            self.start = 0
            self.end = end = used
        return self.mv[end:]

    def feed(self, data: bytes) -> None:
        """Copies ``data`` into the buffer (tests and the non-buffered path)."""
        n = len(data)
        view = self.get_buffer(n)
        view[:n] = data
        self.end += n

    def has_data(self) -> bool:
        return self.start != self.end or bool(self.stack)

    def reset(self) -> None:
        self.start = self.end = 0
        self.stack.clear()

    # -- parsing -----------------------------------------------------------
    def gets(self) -> Any:
        """Returns one complete reply, or :data:`NEED`. Errors are returned, not raised."""
        pos = self.start
        end = self.end
        if pos >= end:
            return NEED
        buf = self.buf
        stack = self.stack
        while True:
            nl = buf.find(b"\r\n", pos, end)
            if nl < 0:
                break
            t = buf[pos]
            if t == 36:  # '$' bulk string
                n = int(buf[pos + 1 : nl])
                if n > MAX_BULK_BYTES:
                    raise ProtocolError(f"bulk length {n} is over the limit of {MAX_BULK_BYTES}")
                if n < 0:
                    v: Any = None
                    pos = nl + 2
                else:
                    s = nl + 2
                    e = s + n
                    if e + 2 > end:
                        break
                    # bytearray slicing beats memoryview slicing for small values;
                    # large values go through the view to avoid a second copy.
                    if self.decode:
                        try:
                            v = buf[s:e].decode() if n < 4096 else str(self.mv[s:e], "utf-8")
                        except UnicodeDecodeError:
                            v = str(self.mv[s:e], "utf-8", "surrogateescape")
                    else:
                        v = bytes(buf[s:e]) if n < 4096 else bytes(self.mv[s:e])
                    pos = e + 2
            elif t == 58:  # ':' integer
                v = int(buf[pos + 1 : nl])
                pos = nl + 2
            elif t == 43:  # '+' simple string
                v = "OK" if nl == pos + 3 and buf[pos + 1] == 79 else buf[pos + 1 : nl].decode("utf-8", "replace")
                pos = nl + 2
            elif t == 42 or t == 37 or t == 126 or t == 62:  # '*' '%' '~' '>'
                n = int(buf[pos + 1 : nl])
                if n > MAX_ELEMENTS:
                    raise ProtocolError(f"element count {n} is over the limit of {MAX_ELEMENTS}")
                pos = nl + 2
                if n > 0:
                    if t == 37:
                        n *= 2
                    # Fast path: consume flat bulk/integer elements in one loop; anything
                    # else (nested aggregate, error, partial element) goes to the stack.
                    items: List[Any] = []
                    add = items.append
                    i = 0
                    decode = self.decode
                    while i < n:
                        if pos + 4 >= end:
                            break
                        t2 = buf[pos]
                        if t2 == 36:
                            # one/two digit lengths without find()/int()
                            if buf[pos + 2] == 13:
                                m = buf[pos + 1] - 48
                                nl = pos + 2
                            elif buf[pos + 3] == 13 and buf[pos + 1] != 45:
                                m = buf[pos + 1] * 10 + buf[pos + 2] - 528
                                nl = pos + 3
                            else:
                                nl = buf.find(b"\r\n", pos, end)
                                if nl < 0:
                                    break
                                m = int(buf[pos + 1 : nl])
                                if m > MAX_BULK_BYTES:
                                    raise ProtocolError(f"bulk length {m} is over the limit of {MAX_BULK_BYTES}")
                            if m < 0:
                                add(None)
                                pos = nl + 2
                            else:
                                s = nl + 2
                                e = s + m
                                if e + 2 > end or m >= 4096:
                                    break
                                if decode:
                                    try:
                                        add(buf[s:e].decode())
                                    except UnicodeDecodeError:
                                        add(buf[s:e].decode("utf-8", "surrogateescape"))
                                else:
                                    add(bytes(buf[s:e]))
                                pos = e + 2
                        elif t2 == 58:
                            nl = buf.find(b"\r\n", pos, end)
                            if nl < 0:
                                break
                            add(int(buf[pos + 1 : nl]))
                            pos = nl + 2
                        else:
                            break
                        i += 1
                    if i < n:
                        stack.append([t, n - i, items])
                        continue
                    if t == 42:
                        v = items
                    elif t == 37:
                        it = iter(items)
                        try:
                            v = dict(zip(it, it))
                        except TypeError:
                            v = items
                    elif t == 62:
                        v = Push(items)
                    else:
                        v = items
                elif n < 0:
                    v = None
                elif t == 37:
                    v = {}
                elif t == 62:
                    v = Push()
                else:
                    v = []
            elif t == 45:  # '-' error
                v = make_error(buf[pos + 1 : nl].decode("utf-8", "replace"))
                pos = nl + 2
            elif t == 95:  # '_' null
                v = None
                pos = nl + 2
            elif t == 35:  # '#' boolean
                v = buf[pos + 1] == 116
                pos = nl + 2
            elif t == 44:  # ',' double
                v = float(bytes(buf[pos + 1 : nl]))
                pos = nl + 2
            elif t == 40:  # '(' big number
                v = int(buf[pos + 1 : nl])
                pos = nl + 2
            elif t == 61 or t == 33:  # '=' verbatim, '!' blob error
                n = int(buf[pos + 1 : nl])
                if n > MAX_BULK_BYTES:
                    raise ProtocolError(f"bulk length {n} is over the limit of {MAX_BULK_BYTES}")
                s = nl + 2
                e = s + n
                if e + 2 > end:
                    break
                text = str(self.mv[s:e], "utf-8", "replace")
                v = make_error(text) if t == 33 else (text[4:] if len(text) >= 4 and text[3] == ":" else text)
                pos = e + 2
            else:
                raise ProtocolError(f"unexpected RESP type byte {t!r} ({chr(t)!r})")
            # attach v to the open aggregates, closing the ones that are now full
            while stack:
                top = stack[-1]
                top[2].append(v)
                top[1] -= 1
                if top[1]:
                    break
                stack.pop()
                kind = top[0]
                items = top[2]
                if kind == 42:
                    v = items
                elif kind == 37:
                    it = iter(items)
                    try:
                        v = dict(zip(it, it))
                    except TypeError:  # unhashable key: keep the flat pairs
                        v = items
                elif kind == 62:
                    v = Push(items)
                else:
                    v = items
            else:
                if pos >= end:
                    self.start = self.end = 0
                else:
                    self.start = pos
                return v
        self.start = pos
        return NEED


class HiredisParser:
    """Same interface as :class:`Parser`, backed by the optional ``hiredis`` package."""

    def __init__(self, size: int = 65536, decode: bool = True) -> None:
        import hiredis  # noqa: PLC0415 - optional dependency

        self._hiredis = hiredis
        self._push = getattr(hiredis, "PushNotification", Push)
        self.buf = bytearray(size)
        self.mv = memoryview(self.buf)
        self.start = 0
        self._dec = decode
        self._reader = self._new_reader()

    def _new_reader(self) -> Any:
        r = self._hiredis.Reader(protocolError=ProtocolError, replyError=ResponseError, notEnoughData=NEED)
        if self._dec:
            r.set_encoding("utf-8", "surrogateescape")
        return r

    @property
    def decode(self) -> bool:
        return self._dec

    @decode.setter
    def decode(self, value: bool) -> None:
        if value is not self._dec and value != self._dec:
            self._dec = value
            self._reader.set_encoding("utf-8" if value else None, "surrogateescape")

    def get_buffer(self, min_free: int = 16384) -> memoryview:
        return self.mv

    @property
    def end(self) -> int:
        return 0

    @end.setter
    def end(self, value: int) -> None:
        if value:
            self._reader.feed(self.mv, 0, value)

    def feed(self, data: bytes) -> None:
        self._reader.feed(data)

    def has_data(self) -> bool:
        return bool(self._reader.has_data())

    def reset(self) -> None:
        self._reader = self._new_reader()

    def gets(self) -> Any:
        v = self._reader.gets()
        if type(v) is self._push:
            return Push(v)
        if type(v) is ResponseError:
            return make_error(v.args[0])
        if type(v) is list:
            _fix_errors(v)
        return v


def _fix_errors(items: list) -> None:  # type: ignore[type-arg]
    for i, item in enumerate(items):
        if type(item) is ResponseError:
            items[i] = make_error(item.args[0])
        elif type(item) is list:
            _fix_errors(item)


def hiredis_available() -> bool:
    try:
        import hiredis  # noqa: F401, PLC0415

        return hasattr(hiredis.Reader, "set_encoding")
    except ImportError:
        return False


def make_parser(kind: str = "auto", decode: bool = True) -> Any:
    """``kind``: "python", "hiredis", or "auto" (hiredis when importable, else Python)."""
    if kind == "hiredis" or (kind == "auto" and hiredis_available()):
        return HiredisParser(decode=decode)
    return Parser(decode=decode)
