# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Keyword enums: value tables and wire bytes equal to the plain-string path (no server needed)."""

from __future__ import annotations

import pytest

import ruvio
from ruvio import (
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
    change_types,
    lex_exclusive,
    lex_inclusive,
    score_exclusive,
    signed,
    unsigned,
)
from ruvio._resp import encode, encode_args

from test_command_encoding import Rec

TABLES = [
    (GeoUnit, ["m", "km", "ft", "mi"]),
    (GeoOrder, ["ASC", "DESC"]),
    (InsertPosition, ["BEFORE", "AFTER"]),
    (ListSide, ["LEFT", "RIGHT"]),
    (ExpireCondition, ["NX", "XX", "GT", "LT"]),
    (BitOperation, ["AND", "OR", "XOR", "NOT"]),
    (BitUnit, ["BYTE", "BIT"]),
    (Aggregate, ["SUM", "MIN", "MAX"]),
    (KeyType, ["string", "json", "list", "set", "hash", "zset", "bloomfilter", "stream", "lease", "limit", "semaphore", "once"]),
    (BitFieldOp, ["GET", "SET", "INCRBY"]),
    (Overflow, ["WRAP", "SAT", "FAIL"]),
    (
        InfoSection,
        ["server", "stats", "buffers", "memory", "memorybreakdown", "keyspace", "snapshot", "cluster", "replication",
         "commandcounters", "latencystats", "logs", "slabs", "all", "default", "prometheus"],
    ),
    (ClientType, ["normal", "master", "replica", "pubsub"]),
    (ChangeType, ["put", "json", "del", "expire", "rename", "list", "set", "hash", "zset", "stream", "flush", "other"]),
    (TrimStrategy, ["MAXLEN", "MINID"]),
    (TrimMode, ["=", "~"]),
    (StreamId, ["*", "$", ">", "-", "+"]),
    (ScoreBound, ["-inf", "+inf"]),
    (LexBound, ["-", "+"]),
]


@pytest.mark.parametrize("cls,values", TABLES)
def test_value_tables(cls, values):
    assert [m.value for m in cls] == values
    for m in cls:
        assert isinstance(m, str) and m == m.value and str(m) == m.value and f"{m}" == m.value
        assert m.wire == m.value.encode()
        assert getattr(ruvio, cls.__name__) is cls


@pytest.mark.parametrize("cls,values", TABLES)
def test_encoding_equals_string_path(cls, values):
    for m in cls:
        assert encode("X", ("k", m, 1)) == encode("X", ("k", m.value, 1))
        assert encode("X", (m,)) == encode("X", (m.value,))
        assert encode_args(("X", m)) == encode_args(("X", m.value))


def test_bit_types():
    assert signed(8) == "i8" and signed(64) == "i64" and unsigned(1) == "u1" and unsigned(63) == "u63"
    for bad in (0, 65):
        with pytest.raises(ValueError):
            signed(bad)
    with pytest.raises(ValueError):
        unsigned(64)


def _same(call, reply=None):
    a, b = Rec(reply), Rec(reply)
    call(a, True)
    call(b, False)
    assert a.frame == b.frame


def test_commands_accept_enum_and_str():
    _same(lambda r, e: r.geodist("k", "a", "b", GeoUnit.KILOMETERS if e else "km"))
    _same(lambda r, e: r.geosearch("k", member="a", radius=5, unit=GeoUnit.MILES if e else "mi", sort=GeoOrder.DESC if e else "DESC"))
    _same(lambda r, e: r.georadius("k", 1.0, 2.0, 5, unit=GeoUnit.FEET if e else "ft", sort=GeoOrder.ASC if e else "ASC"))
    _same(lambda r, e: r.linsert("k", InsertPosition.BEFORE if e else "BEFORE", "p", "v"))
    _same(lambda r, e: r.lmove("a", "b", ListSide.RIGHT if e else "RIGHT", ListSide.LEFT if e else "LEFT"))
    _same(lambda r, e: r.blmove("a", "b", 1, ListSide.RIGHT if e else "RIGHT", ListSide.LEFT if e else "LEFT"))
    _same(lambda r, e: r.lmpop(["a"], ListSide.RIGHT if e else "RIGHT", 2))
    _same(lambda r, e: r.blmpop(1, ["a"], ListSide.RIGHT if e else "RIGHT"))
    _same(lambda r, e: r.scan(0, type=KeyType.ZSET if e else "zset"), [b"0", []])
    _same(lambda r, e: r.bitop(BitOperation.XOR if e else "XOR", "d", "a", "b"))
    _same(lambda r, e: r.bitcount("k", 0, 7, BitUnit.BIT if e else "BIT"))
    _same(lambda r, e: r.zunionstore("d", ["a", "b"], aggregate=Aggregate.MAX if e else "MAX"))
    _same(
        lambda r, e: r.bitfield("k", "OVERFLOW", Overflow.SAT if e else "SAT", BitFieldOp.INCRBY if e else "INCRBY", unsigned(8), 0, 10)
    )


def test_wave2_commands_accept_enum_and_str():
    _same(lambda r, e: r.info(InfoSection.MEMORY_BREAKDOWN if e else "memorybreakdown"), "x")
    _same(lambda r, e: r.client_list(ClientType.PUBSUB if e else "pubsub"), "x")
    _same(lambda r, e: r.xrange("s", StreamId.MIN if e else "-", StreamId.MAX if e else "+"))
    _same(lambda r, e: r.xadd("s", {"a": "1"}, StreamId.NEW_ENTRY if e else "*"))
    _same(lambda r, e: r.xgroup_create("s", "g", StreamId.LAST if e else "$"))
    _same(lambda r, e: r.zrangebyscore("z", ScoreBound.NEGATIVE_INFINITY if e else "-inf", score_exclusive(5) if e else "(5"))
    _same(lambda r, e: r.zrangebylex("z", LexBound.MIN if e else "-", lex_inclusive("m") if e else "[m"))


def test_stream_trim_wire_equals_old_path():
    for mode, word in ((TrimMode.EXACT, "="), (TrimMode.APPROXIMATE, "~")):
        a, b = Rec(), Rec()
        a.xtrim("s", trim=StreamTrim.maxlen(100, mode))
        b.xtrim("s", maxlen=100) if word == "=" else b.xtrim("s", maxlen=100, approximate=True)
        if word == "~":
            assert a.frame == b.frame
        assert a.args == ["XTRIM", "s", "MAXLEN", word, "100"]
        a.xtrim("s", trim=StreamTrim.minid("5-0", mode))
        assert a.args == ["XTRIM", "s", "MINID", word, "5-0"]
        a.xadd("s", {"f": "v"}, trim=StreamTrim.maxlen(10, mode))
        assert a.args == ["XADD", "s", "MAXLEN", word, "10", "*", "f", "v"]
    a, b = Rec(), Rec()
    a.xadd("s", {"f": "v"}, trim=StreamTrim.minid("5-0", TrimMode.APPROXIMATE))
    b.xadd("s", {"f": "v"}, minid="5-0", approximate=True)
    assert a.frame == b.frame
    with pytest.raises(ValueError):
        a.xtrim("s", maxlen=1, trim=StreamTrim.maxlen(1))
    with pytest.raises(ValueError):
        a.xadd("s", {"f": "v"}, minid="1-0", trim=StreamTrim.maxlen(1))


def test_helpers():
    assert change_types(ChangeType.PUT, ChangeType.DEL, "json") == "put,del,json"
    with pytest.raises(ValueError):
        change_types()
    assert score_exclusive(10) == "(10" and score_exclusive(1.5) == "(1.5"
    assert lex_inclusive("m") == "[m" and lex_exclusive("m") == "(m"
