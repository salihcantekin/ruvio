# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""TTL on create (``ttl=``) and EXPIRE conditions: the exact arguments each method sends."""

from __future__ import annotations

from datetime import timedelta

import pytest

from ruvio import ExpireCondition

from test_command_encoding import Rec


@pytest.mark.parametrize(
    "ttl,wire",
    [
        (60, ["EX", "60"]),
        (60.0, ["EX", "60"]),
        (1.5, ["PX", "1500"]),
        (0.25, ["PX", "250"]),
        (timedelta(seconds=90), ["EX", "90"]),
        (timedelta(milliseconds=1500), ["PX", "1500"]),
        (timedelta(minutes=2), ["EX", "120"]),
    ],
)
def test_ttl_wire_forms(ttl, wire):
    r = Rec(reply=1)
    r.incr("k", ttl=ttl)
    assert r.args == ["INCR", "k", *wire]


@pytest.mark.parametrize("ttl", [0, -1, 0.0, -0.5, 0.0004, timedelta(0), timedelta(seconds=-1), float("nan"), float("inf")])
def test_ttl_zero_or_negative_is_rejected_before_sending(ttl):
    r = Rec(reply=1)
    with pytest.raises(ValueError):
        r.incr("k", ttl=ttl)
    assert r.frame == b""


@pytest.mark.parametrize("ttl", ["10", True, None.__class__])
def test_ttl_wrong_type_is_rejected(ttl):
    r = Rec(reply=1)
    with pytest.raises(TypeError):
        r.decr("k", ttl=ttl)
    assert r.frame == b""


def test_no_ttl_keeps_the_old_frames():
    r = Rec(reply=1)
    r.incr("k")
    assert r.args == ["INCR", "k"]
    r.decr("k")
    assert r.args == ["DECR", "k"]
    r.incrby("k", 2)
    assert r.args == ["INCRBY", "k", "2"]
    r.incrby("k", 2, max=5)
    assert r.args == ["INCRBY", "k", "2", "MAX", "5"]
    r.decrby("k", 3)
    assert r.args == ["DECRBY", "k", "3"]


def test_counter_commands():
    r = Rec(reply=1)
    r.incr("k", ttl=10)
    assert r.args == ["INCR", "k", "EX", "10"]
    r.decr("k", ttl=10)
    assert r.args == ["DECR", "k", "EX", "10"]
    r.incrby("k", 2, ttl=timedelta(milliseconds=500))
    assert r.args == ["INCRBY", "k", "2", "PX", "500"]
    r.incrby("k", 2, max=9, ttl=30)
    assert r.args == ["INCRBY", "k", "2", "EX", "30", "MAX", "9"]
    r.decrby("k", 4, ttl=30)
    assert r.args == ["DECRBY", "k", "4", "EX", "30"]
    r.reply = "1.5"
    r.incrbyfloat("k", 1.5, ttl=2.5)
    assert r.args == ["INCRBYFLOAT", "k", "1.5", "PX", "2500"]
    r.reply = 1
    r.hincrby("h", "f", 3, ttl=60)
    assert r.args == ["HINCRBY", "h", "f", "3", "EX", "60"]
    r.reply = "2"
    r.zincrby("z", 2, "m", ttl=60)
    assert r.args == ["ZINCRBY", "z", "2", "m", "EX", "60"]
    r.reply = 0
    r.setbit("b", 7, 1, ttl=60)
    assert r.args == ["SETBIT", "b", "7", "1", "EX", "60"]


def test_zadd_ttl_goes_before_the_other_options():
    r = Rec(reply=1)
    r.zadd("z", {"a": 1}, nx=True, ch=True, ttl=60)
    assert r.args == ["ZADD", "z", "EX", "60", "NX", "CH", "1", "a"]
    r.zadd("z", [("a", 1), ("b", 2)], gt=True, ttl=0.5)
    assert r.args == ["ZADD", "z", "PX", "500", "GT", "1", "a", "2", "b"]
    r.zadd("z", {"a": 1})
    assert r.args == ["ZADD", "z", "1", "a"]


def test_geoadd_ttl_goes_before_the_other_options():
    r = Rec(reply=1)
    r.geoadd("g", [(13.4, 38.1, "p")], xx=True, ch=True, ttl=timedelta(seconds=5))
    assert r.args == ["GEOADD", "g", "EX", "5", "XX", "CH", "13.4", "38.1", "p"]


def test_xadd_ttl_goes_among_the_options_before_the_id():
    r = Rec(reply="1-1")
    r.xadd("s", {"f": "v"}, ttl=60)
    assert r.args == ["XADD", "s", "EX", "60", "*", "f", "v"]
    r.xadd("s", {"f": "v"}, id="5-1", maxlen=10, nomkstream=True, delay_ms=7, ttl=1.5)
    assert r.args == ["XADD", "s", "PX", "1500", "NOMKSTREAM", "DELAY", "7", "MAXLEN", "10", "5-1", "f", "v"]


def test_json_set_ttl_is_last():
    r = Rec(reply="OK")
    r.json_set("j", "$", "{}", ttl=60)
    assert r.args == ["JSON.SET", "j", "$", "{}", "EX", "60"]
    r.json_set("j", "$", "{}", nx=True, ttl=0.5)
    assert r.args == ["JSON.SET", "j", "$", "{}", "NX", "PX", "500"]
    r.json_set("j", "$.a", "1", xx=True)
    assert r.args == ["JSON.SET", "j", "$.a", "1", "XX"]
    r.json_set_obj("j", {"a": 1}, ttl=9)
    assert r.args == ["JSON.SET", "j", "$", '{"a":1}', "EX", "9"]


def test_bf_insert_ttl_goes_first_among_the_options():
    r = Rec(reply=[1, 0])
    assert r.bf_insert("b", ["x", "y"], capacity=100, error=0.01, nonscaling=True, ttl=60) == [True, False]
    assert r.args == ["BF.INSERT", "b", "EX", "60", "CAPACITY", "100", "ERROR", "0.01", "NONSCALING", "ITEMS", "x", "y"]
    r.bf_insert("b", ["x"])
    assert r.args == ["BF.INSERT", "b", "ITEMS", "x"]


@pytest.mark.parametrize(
    "method,cmd,value",
    [("expire", "EXPIRE", 10), ("pexpire", "PEXPIRE", 10), ("expireat", "EXPIREAT", 99), ("pexpireat", "PEXPIREAT", 99)],
)
def test_expire_conditions(method, cmd, value):
    r = Rec(reply=1)
    assert getattr(r, method)("k", value) is True
    assert r.args == [cmd, "k", str(value)]
    for cond in ExpireCondition:
        assert getattr(r, method)("k", value, condition=cond) is True
        assert r.args == [cmd, "k", str(value), cond.value]
    getattr(r, method)("k", value, condition="GT")
    assert r.args == [cmd, "k", str(value), "GT"]
    r.reply = 0
    assert getattr(r, method)("k", value, condition=ExpireCondition.NX) is False
