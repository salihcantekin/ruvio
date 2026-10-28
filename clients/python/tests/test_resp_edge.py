# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Codec edge cases (no server): argument subclasses, chunk-boundary parsing, RESP3 corners and the
hiredis-backed parser, driven through a fake ``hiredis`` module so it runs without the C package."""

from __future__ import annotations

import enum
import sys
import types

import pytest

from ruvio import _resp
from ruvio._resp import NEED, HiredisParser, Parser, Push, encode, encode_args, head, make_parser
from ruvio.errors import (
    BusyError,
    CrossSlotError,
    MovedError,
    NoGroupError,
    ProtocolError,
    ResponseError,
    WrongTypeError,
    make_error,
)


# --------------------------------------------------------------------------- encoder


class S(str):
    pass


class B(bytes):
    pass


class I(int):  # noqa: E742
    pass


class F(float):
    pass


class Color(enum.IntEnum):
    RED = 7


def test_encode_subclasses_and_slow_path_types():
    # subclasses miss the exact-type fast path and go through the isinstance chain
    assert encode("X", (S("ab"),)) == b"*2\r\n$1\r\nX\r\n$2\r\nab\r\n"
    assert encode("X", (B(b"ab"), I(5), F(1.5))) == b"*4\r\n$1\r\nX\r\n$2\r\nab\r\n$1\r\n5\r\n$3\r\n1.5\r\n"
    assert encode("X", (Color.RED,)) == b"*2\r\n$1\r\nX\r\n$1\r\n7\r\n"
    # negative / huge ints, bool and float take the slow path
    assert encode("X", (-1, 65536, True, False, 0.1)) == (
        b"*6\r\n$1\r\nX\r\n$2\r\n-1\r\n$5\r\n65536\r\n$1\r\n1\r\n$1\r\n0\r\n$3\r\n0.1\r\n"
    )
    # empty string and empty bytes are valid arguments
    assert encode("X", ("", b"")) == b"*3\r\n$1\r\nX\r\n$0\r\n\r\n$0\r\n\r\n"
    assert encode("GET", ("",)) == b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n"
    for bad in (None, [1], {"a": 1}, object(), 1 + 2j):
        with pytest.raises(TypeError, match="cannot send"):
            encode("X", (bad,))


def test_encode_one_arg_fast_path_boundaries():
    # fast path applies below the 4096-byte table; the slow path must give the same bytes above it
    for n in (0, 1, 4095, 4096, 4097):
        v = "a" * n
        expect = b"*2\r\n$1\r\nX\r\n$%d\r\n%s\r\n" % (n, v.encode())
        assert encode("X", (v,)) == expect
        assert encode("X", (v.encode(),)) == expect
    # argument counts around the array-length table
    for n in (4094, 4095, 4096):
        out = encode("X", ("a",) * n)
        assert out.startswith(b"*%d\r\n" % (n + 1))


def test_head_caches_and_counts_words():
    assert head("XGROUP CREATE") == (2, b"$6\r\nXGROUP\r\n$6\r\nCREATE\r\n")
    assert head("XGROUP CREATE") is head("XGROUP CREATE")
    assert head("PING") == (1, b"$4\r\nPING\r\n")


def test_encode_args_all_types():
    big = "x" * 5000
    bigb = b"y" * 5000
    out = encode_args(("SET", "k", -1, 70000, 1.5, bytearray(b"ab"), b"", big, bigb, True))
    assert out == (
        b"*10\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n-1\r\n$5\r\n70000\r\n$3\r\n1.5\r\n$2\r\nab\r\n$0\r\n\r\n"
        + b"$5000\r\n" + big.encode() + b"\r\n$5000\r\n" + bigb + b"\r\n$1\r\n1\r\n"
    )
    assert encode_args(()) == b"*0\r\n"
    assert encode_args(tuple(range(4100))).startswith(b"*4100\r\n")
    with pytest.raises(TypeError):
        encode_args(("a", None))


def test_need_repr_and_push_type():
    assert repr(NEED) == "NEED"
    assert isinstance(Push([1]), list)


# --------------------------------------------------------------------------- python parser


def _cases():
    """(wire bytes, expected value when decoding) pairs that exercise every parser path."""
    big = "é" * 3000  # 6000 bytes: beyond the small-bulk fast path
    return [
        (b"+OK\r\n", "OK"),
        (b"+PONG\r\n", "PONG"),
        (b"+O\r\n", "O"),
        (b":-12\r\n", -12),
        (b"$0\r\n\r\n", ""),
        (b"$-1\r\n", None),
        (b"_\r\n", None),
        (b"*-1\r\n", None),
        (b"*0\r\n", []),
        (b"%0\r\n", {}),
        (b"~0\r\n", []),
        (b">0\r\n", Push()),
        (b"#t\r\n", True),
        (b"#f\r\n", False),
        (b",-2.5\r\n", -2.5),
        (b",-inf\r\n", float("-inf")),
        (b"(-5\r\n", -5),
        (b"=7\r\ntxt:abc\r\n", "abc"),
        (b"=3\r\nabc\r\n", "abc"),  # no "xxx:" prefix: returned whole
        (b"=2\r\nab\r\n", "ab"),
        (b"$%d\r\n%s\r\n" % (len(big.encode()), big.encode()), big),
        (b"*3\r\n$100\r\n" + b"a" * 100 + b"\r\n$-1\r\n$3\r\nabc\r\n", ["a" * 100, None, "abc"]),
        (b"*2\r\n$5000\r\n" + b"b" * 5000 + b"\r\n:7\r\n", ["b" * 5000, 7]),
        (b"*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n$1\r\nz\r\n", [[1, 2], ["z"]]),
        (b"*3\r\n:1\r\n+two\r\n$5\r\nthree\r\n", [1, "two", "three"]),
        (b"%2\r\n$1\r\na\r\n*2\r\n:1\r\n:2\r\n$1\r\nb\r\n%1\r\n+x\r\n:3\r\n", {"a": [1, 2], "b": {"x": 3}}),
        (b"~3\r\n:1\r\n:2\r\n:3\r\n", [1, 2, 3]),
        (b">3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nv\r\n", Push(["message", "c", "v"])),
        (b"*2\r\n%1\r\n+k\r\n+v\r\n$-1\r\n", [{"k": "v"}, None]),
        (b"*1\r\n_\r\n", [None]),
        (b"*1\r\n#t\r\n", [True]),
        (b"*2\r\n,1.5\r\n(7\r\n", [1.5, 7]),
    ]


@pytest.mark.parametrize("decode", [True, False])
def test_every_case_survives_every_split_point(decode):
    def norm(v):
        if isinstance(v, str) and not decode:
            return v.encode()
        if isinstance(v, list):
            return type(v)(norm(x) for x in v)
        if isinstance(v, dict):
            return {norm(k): norm(x) for k, x in v.items()}
        return v

    for wire, expect in _cases():
        if not decode and (b"+" in wire or wire[:1] == b"="):
            continue  # simple and verbatim strings are always text: covered by decode=True
        want = norm(expect)
        # whole
        p = Parser(decode=decode)
        p.feed(wire)
        got = p.gets()
        assert got == want, wire
        assert type(got) is type(want), wire
        assert p.gets() is NEED and not p.has_data()
        # every two-chunk split, then one byte at a time
        for cut in range(1, len(wire)):
            p = Parser(size=16, decode=decode)
            p.feed(wire[:cut])
            first = p.gets()
            if first is not NEED:  # a complete reply can only appear once the whole frame is there
                assert wire[:cut] == wire
            p.feed(wire[cut:])
            got = first if first is not NEED else p.gets()
            assert got == want, (wire, cut)
        p = Parser(size=8, decode=decode)
        got = NEED
        for i in range(len(wire)):
            p.feed(wire[i : i + 1])
            got = p.gets()
            if i < len(wire) - 1:
                assert got is NEED, (wire, i)
        assert got == want, wire


def test_pipelined_replies_in_one_buffer_and_bytes_left_over():
    p = Parser()
    p.feed(b":1\r\n+two\r\n*1\r\n:3\r\n$4\r\nfo")
    assert p.gets() == 1
    assert p.gets() == "two"
    assert p.gets() == [3]
    assert p.gets() is NEED
    assert p.has_data()  # the partial bulk is kept
    p.feed(b"ur\r\n")
    assert p.gets() == "four"
    assert not p.has_data()


def test_error_replies_are_values_with_specific_types():
    p = Parser()
    p.feed(
        b"-MOVED 3999 127.0.0.1:6381\r\n-ASK 1 h:1\r\n-CROSSSLOT nope\r\n-NOGROUP g\r\n-BUSYGROUP x\r\n-BUSY y\r\n"
        b"-ERR\r\n!21\r\nSYNTAX invalid syntax\r\n!10\r\nWRONGTYPE \r\n"
    )
    out = []
    while (v := p.gets()) is not NEED:
        out.append(v)
    assert [type(v) for v in out] == [
        MovedError, MovedError, CrossSlotError, NoGroupError, BusyError, BusyError, ResponseError, ResponseError,
        WrongTypeError,
    ]
    assert out[0].code == "MOVED" and out[6].code == "ERR" and out[7].message == "SYNTAX invalid syntax"
    assert out[7].code == "SYNTAX"


def test_error_nested_in_array_does_not_abort_the_array():
    p = Parser()
    p.feed(b"*3\r\n:1\r\n-ERR boom\r\n*1\r\n-WRONGTYPE x\r\n")
    v = p.gets()
    assert v[0] == 1 and type(v[1]) is ResponseError and type(v[2][0]) is WrongTypeError


def test_map_with_unhashable_key_keeps_flat_pairs():
    p = Parser()
    p.feed(b"%1\r\n*1\r\n:1\r\n:2\r\n")  # key is an array -> cannot be a dict key
    assert p.gets() == [[1], 2]
    p.feed(b"*1\r\n%1\r\n*1\r\n:1\r\n:2\r\n")  # same through the stack path
    assert p.gets() == [[[1], 2]]


def test_invalid_utf8_decodes_with_surrogateescape_everywhere():
    p = Parser()
    p.feed(b"$2\r\n\xff\xfe\r\n*1\r\n$2\r\n\xff\xfe\r\n")
    a, b = p.gets(), p.gets()
    assert a.encode("utf-8", "surrogateescape") == b"\xff\xfe"
    assert b[0].encode("utf-8", "surrogateescape") == b"\xff\xfe"
    big = b"\xff" * 5000
    p.feed(b"$5000\r\n" + big + b"\r\n")
    assert p.gets().encode("utf-8", "surrogateescape") == big


@pytest.mark.parametrize(
    "wire",
    [b"?x\r\n", b"\x00\r\n", b"&1\r\n", b"*1\r\n?\r\n"],
)
def test_unknown_type_byte_raises_protocol_error(wire):
    p = Parser()
    p.feed(wire)
    with pytest.raises(ProtocolError, match="unexpected RESP type byte"):
        p.gets()


@pytest.mark.parametrize(
    "wire",
    [b"$67108865\r\n", b"*1048577\r\n", b"%1048577\r\n", b"=67108865\r\n", b"*1\r\n$67108865\r\n"],
)
def test_oversized_headers_raise_protocol_error(wire):
    p = Parser()
    p.feed(wire)
    with pytest.raises(ProtocolError, match="over the limit"):
        p.gets()


@pytest.mark.parametrize("wire", [b"$abc\r\n", b":xyz\r\n", b"*x\r\n", b",nan-ish\r\n"])
def test_garbage_numbers_raise_value_error(wire):
    p = Parser()
    p.feed(wire)
    with pytest.raises(ValueError):
        p.gets()


def test_get_buffer_compacts_then_grows_and_reset_clears():
    p = Parser(size=64)
    p.feed(b":1\r\n" * 10)  # 40 bytes
    assert p.gets() == 1  # start moves forward
    assert p.start == 4
    # free tail is < min_free but moving the data to the front makes room: compaction, no growth
    view = p.get_buffer(28)
    assert p.start == 0 and p.end == 36 and len(p.buf) == 64 and len(view) == 28
    # now demand more than the whole buffer can give: it grows
    p.get_buffer(500)
    assert len(p.buf) >= 536
    for _ in range(9):
        assert p.gets() == 1
    assert p.gets() is NEED
    # an open aggregate counts as data until reset
    p.feed(b"*2\r\n:1\r\n")
    assert p.gets() is NEED and p.has_data()
    p.reset()
    assert not p.has_data() and p.start == p.end == 0 and p.stack == []
    p.feed(b":9\r\n")
    assert p.gets() == 9


def test_gets_on_empty_buffer_is_need():
    assert Parser().gets() is NEED


def test_decode_flag_toggles_between_replies():
    p = Parser()
    p.feed(b"$1\r\na\r\n")
    assert p.gets() == "a"
    p.decode = False
    p.feed(b"$1\r\na\r\n*1\r\n$1\r\nb\r\n")
    assert p.gets() == b"a" and p.gets() == [b"b"]


# --------------------------------------------------------------------------- hiredis-backed parser


class _FakeReader:
    """Behaves like ``hiredis.Reader`` but is built on the pure-Python parser."""

    def __init__(self, protocolError=None, replyError=None, notEnoughData=False, **kw):
        self.not_enough = notEnoughData
        self.reply_error = replyError
        self.p = Parser(decode=False)
        self.encoding = None
        self.fed = []

    def set_encoding(self, encoding=None, errors=None):
        self.encoding = encoding
        self.p.decode = encoding is not None

    def feed(self, data, offset=0, length=None):
        data = bytes(data)
        chunk = data[offset:] if length is None else data[offset : offset + length]
        self.fed.append(len(chunk))
        self.p.feed(chunk)

    def has_data(self):
        return self.p.has_data()

    def gets(self):
        v = self.p.gets()
        if v is NEED:
            return self.not_enough
        return self._conv(v)

    def _conv(self, v):
        if isinstance(v, Push):
            return _PushNotification(self._conv(x) for x in v)
        if isinstance(v, ResponseError):
            return self.reply_error(v.args[0])  # hiredis builds the plain replyError class
        if type(v) is list:
            return [self._conv(x) for x in v]
        return v


class _PushNotification(list):
    pass


@pytest.fixture
def fake_hiredis(monkeypatch):
    mod = types.ModuleType("hiredis")
    mod.Reader = _FakeReader
    mod.PushNotification = _PushNotification
    monkeypatch.setitem(sys.modules, "hiredis", mod)
    return mod


def test_hiredis_available_and_make_parser_choices(fake_hiredis, monkeypatch):
    assert _resp.hiredis_available()
    assert type(make_parser("auto")) is HiredisParser
    assert type(make_parser("hiredis")) is HiredisParser
    assert type(make_parser("python")) is Parser
    assert make_parser("python", decode=False).decode is False
    # a hiredis without set_encoding (too old) is not used by "auto"
    class Old:  # a hiredis too old to have set_encoding
        pass

    fake_hiredis.Reader = Old
    assert not _resp.hiredis_available()
    assert type(make_parser("auto")) is Parser
    # not installed at all
    monkeypatch.setitem(sys.modules, "hiredis", None)
    assert not _resp.hiredis_available()
    assert type(make_parser("auto")) is Parser
    with pytest.raises(ImportError):
        make_parser("hiredis")


def test_hiredis_parser_converts_replies(fake_hiredis):
    p = HiredisParser()
    assert p.decode is True and p.end == 0 and p.get_buffer() is p.mv
    p.feed(b"+OK\r\n:5\r\n$3\r\nabc\r\n*2\r\n-WRONGTYPE bad\r\n*1\r\n-MOVED 1 h:2\r\n-ERR top\r\n>2\r\n$1\r\na\r\n:1\r\n")
    assert p.has_data()
    assert p.gets() == "OK" and p.gets() == 5 and p.gets() == "abc"
    nested = p.gets()
    assert type(nested[0]) is WrongTypeError and type(nested[1][0]) is MovedError
    top = p.gets()
    assert type(top) is ResponseError and top.code == "ERR"
    push = p.gets()
    assert type(push) is Push and push == ["a", 1]
    assert p.gets() is NEED
    assert not p.has_data()


def test_hiredis_parser_buffer_protocol_and_decode_switch(fake_hiredis):
    p = HiredisParser(size=128, decode=False)
    view = p.get_buffer(16)
    view[:9] = b"$3\r\nabc\r\n"
    p.end += 9  # the setter feeds the reader from the shared buffer
    assert p.end == 0
    assert p.gets() == b"abc"
    p.end = 0  # zero bytes: nothing is fed
    assert p.gets() is NEED
    p.decode = True
    assert p.decode is True and p._reader.encoding == "utf-8"
    p.decode = True  # unchanged: no second set_encoding
    p.feed(b"$1\r\nz\r\n")
    assert p.gets() == "z"
    p.decode = False
    assert p._reader.encoding is None
    p.feed(b"$1\r\nz\r\n")
    assert p.gets() == b"z"


def test_hiredis_parser_reset_drops_buffered_data(fake_hiredis):
    p = HiredisParser()
    p.feed(b"*2\r\n:1\r\n")
    assert p.gets() is NEED and p.has_data()
    old = p._reader
    p.reset()
    assert p._reader is not old and not p.has_data()
    assert p._reader.encoding == "utf-8"  # the decode setting survives a reset
    p.feed(b":4\r\n")
    assert p.gets() == 4


def test_hiredis_without_push_support_falls_back_to_ruvio_push(fake_hiredis):
    del fake_hiredis.PushNotification
    p = HiredisParser()
    assert p._push is Push


def test_make_error_codes_by_first_word():
    assert type(make_error("MOVED 1 a:1")) is MovedError
    assert type(make_error("NOGROUP")) is NoGroupError  # no space: the whole message is the code
    assert type(make_error("whatever")) is ResponseError
    assert make_error("A B C").code == "A"
