# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Typed-command encoding and reply conversion without a server: a recording client captures the
frame each method builds and feeds a canned reply through the method's converter."""

from __future__ import annotations

import pytest

from ruvio import _cluster as cl
from ruvio._commands import Commands
from ruvio._models import GeoResult, StreamEntry
from ruvio._resp import encode


class Rec(Commands):
    """Records the last frame; ``reply`` is what the "server" answers (converted like a real client)."""

    def __init__(self, reply=None):
        self.frame = b""
        self.reply = reply

    def _x(self, conv, data, decode=None):
        self.frame = data
        return self.reply if conv is None else conv(self.reply)

    @property
    def args(self):
        return [a.decode("utf-8", "surrogateescape") for a in cl.split_frame(self.frame)]


# --------------------------------------------------------------------------- hot paths match the generic encoder

LONG = "x" * 5000
KEYS = ["k", "é", "", b"bk", b"", LONG, LONG.encode(), "{tag}k"]
VALUES = ["v", b"\x00\xff", "", 7, -1, 2.5, LONG, LONG.encode(), True]


@pytest.mark.parametrize("key", KEYS)
def test_get_incr_delete_fast_paths_equal_the_generic_encoding(key):
    r = Rec()
    r.get(key)
    assert r.frame == encode("GET", (key,))
    r.incr(key)
    assert r.frame == encode("INCR", (key,))
    r.delete(key)
    assert r.frame == encode("DEL", (key,))


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("value", VALUES)
def test_plain_set_equals_the_generic_encoding(key, value):
    r = Rec(reply="OK")
    assert r.set(key, value) is True
    assert r.frame == encode("SET", (key, value))


def test_set_with_a_non_text_key_takes_the_generic_path():
    r = Rec(reply="OK")
    assert r.set(5, "v") is True and r.frame == encode("SET", (5, "v"))
    assert r.set(b"k", 5) is True and r.frame == encode("SET", (b"k", 5))
    r.reply = None
    assert r.set("k", "v", nx=True) is False  # NX refused: nil reply


def test_client_durability_encoding():
    r = Rec(reply="OK")
    assert r.client_durability(1) is True
    assert r.frame == encode("CLIENT DURABILITY", ("REPLICAS", 1))
    assert r.client_durability(2, 750) is True
    assert r.frame == encode("CLIENT DURABILITY", ("REPLICAS", 2, "TIMEOUT", 750))
    assert r.client_durability(0) is True
    assert r.args == ["CLIENT", "DURABILITY", "REPLICAS", "0"]


def test_delete_variants():
    r = Rec()
    r.delete("a", "b")
    assert r.args == ["DEL", "a", "b"]
    r.delete()  # no keys: the server answers the arity error
    assert r.args == ["DEL"]
    r.delete(5)
    assert r.args == ["DEL", "5"]
    r.delete(bytearray(b"ba"))
    assert r.args == ["DEL", "ba"]


@pytest.mark.parametrize("key,field", [("h", "f"), ("é", "ü"), ("h", b"f"), (b"h", "f"), ("h", 5), (LONG, "f"), ("h", LONG)])
def test_hget_fast_path_equals_the_generic_encoding(key, field):
    r = Rec()
    r.hget(key, field)
    assert r.frame == encode("HGET", (key, field))


# --------------------------------------------------------------------------- option encodings


def test_set_options_in_wire_order():
    r = Rec(reply="OK")
    r.set("k", "v", ex=1, px=2, exat=3, pxat=4, keepttl=True, nx=True, xx=True, ifeq="old")
    assert r.args == ["SET", "k", "v", "IFEQ", "old", "EX", "1", "PX", "2", "EXAT", "3", "PXAT", "4", "KEEPTTL", "NX", "XX"]
    r.reply = "OK"
    r.set("k", 5, px=9, ifgt=True)
    assert r.args == ["SET", "k", "5", "IFGT", "PX", "9"]
    r.set("k", -5, iflt=True)
    assert r.args == ["SET", "k", "-5", "IFLT"]
    r.reply = "prev"
    assert r.set("k", "v", get=True) == "prev"
    assert r.args == ["SET", "k", "v", "GET"]
    r.reply = None
    assert r.set("k", "v", xx=True) is False


def test_hello_forms():
    r = Rec(reply=["server", "ruvio", "proto", 3])
    assert r.hello() == {"server": "ruvio", "proto": 3} and r.args == ["HELLO"]
    r.hello(3)
    assert r.args == ["HELLO", "3"]
    r.hello(3, password="pw")  # the user defaults to "default"
    assert r.args == ["HELLO", "3", "AUTH", "default", "pw"]
    r.hello(3, "me", "pw", "nm")
    assert r.args == ["HELLO", "3", "AUTH", "me", "pw", "SETNAME", "nm"]
    r.hello(2, setname="nm")
    assert r.args == ["HELLO", "2", "SETNAME", "nm"]
    r.reply = {"server": "ruvio"}  # RESP3 map reply
    assert r.hello(3) == {"server": "ruvio"}


def test_client_kill_forms():
    r = Rec(reply=1)
    r.client_kill("1.2.3.4:5", legacy=True)
    assert r.args == ["CLIENT", "KILL", "1.2.3.4:5"]
    r.client_kill(id=7)
    assert r.args == ["CLIENT", "KILL", "ID", "7"]
    r.client_kill("1.2.3.4:5", id=7, skipme=False)
    assert r.args == ["CLIENT", "KILL", "ID", "7", "ADDR", "1.2.3.4:5", "SKIPME", "no"]
    r.client_kill(skipme=True)
    assert r.args == ["CLIENT", "KILL", "SKIPME", "yes"]
    r.client_kill()
    assert r.args == ["CLIENT", "KILL"]


def test_acl_commands():
    r = Rec(reply="OK")
    assert r.acl_setuser("app", "on", ">pw", "+get", "~app:*") is True
    assert r.args == ["ACL", "SETUSER", "app", "on", ">pw", "+get", "~app:*"]
    assert r.acl_load() is True and r.args == ["ACL", "LOAD"]
    assert r.acl_save() is True and r.args == ["ACL", "SAVE"]
    r.reply = [b"flags", [b"on"], b"passwords", []]
    assert r.acl_getuser("app") == {"flags": [b"on"], "passwords": []}
    r.reply = None
    assert r.acl_getuser("nobody") is None


def test_list_pop_forms():
    r = Rec(reply=None)
    assert r.lmpop(["a", "b"], "RIGHT", 2) is None
    assert r.args == ["LMPOP", "2", "a", "b", "RIGHT", "COUNT", "2"]
    r.blmpop(1.5, "a", count=3)
    assert r.args == ["BLMPOP", "1.5", "1", "a", "LEFT", "COUNT", "3"]
    r.blmpop(0, ["a", b"b"])
    assert r.args == ["BLMPOP", "0", "2", "a", "b", "LEFT"]
    r.reply = ["a", ["x", "y"]]
    assert r.lmpop("a") == ("a", ["x", "y"])
    r.blpop(["a", "b"], 2)
    assert r.args == ["BLPOP", "a", "b", "2"]
    r.reply = None
    assert r.blpop("a") is None


def test_zrange_forms_and_limit_validation():
    r = Rec(reply=["a", "1", "b", "2.5"])
    assert r.zrange("z", "(1", "+inf", byscore=True, rev=True, offset=1, count=2, withscores=True) == [("a", 1.0), ("b", 2.5)]
    assert r.args == ["ZRANGE", "z", "(1", "+inf", "BYSCORE", "REV", "LIMIT", "1", "2", "WITHSCORES"]
    r.zrange("z", "[a", "[z", bylex=True)
    assert r.args == ["ZRANGE", "z", "[a", "[z", "BYLEX"]
    r.reply = [["a", 1], ["b", 2.5]]  # RESP3 nested [member, score] pairs
    assert r.zrange("z", 0, -1, withscores=True) == [("a", 1.0), ("b", 2.5)]
    r.reply = []
    assert r.zrange("z", 0, -1, withscores=True) == []
    for kw in ({"offset": 1}, {"count": 1}):
        with pytest.raises(ValueError, match="together"):
            r.zrange("z", 0, 1, byscore=True, **kw)


def test_geo_radius_flags_and_replies():
    r = Rec(reply=[["m", "1.5", 4040, ["13.5", "38.1"]]])
    rows = r.georadius("g", 15, 37, 200, "km", withcoord=True, withdist=True, withhash=True, count=5, any=True, sort="ASC")
    assert r.args == [
        "GEORADIUS", "g", "15", "37", "200", "km", "WITHCOORD", "WITHDIST", "WITHHASH", "ASC", "COUNT", "5", "ANY",
    ]
    assert rows == [GeoResult("m", 1.5, 4040, (13.5, 38.1))]
    r.reply = [["m", "9.5"]]
    assert r.georadius("g", 1, 2, 3, withdist=True) == [GeoResult("m", 9.5, None, None)]
    r.reply = ["a", "b"]
    assert r.georadius("g", 1, 2, 3) == ["a", "b"]  # no flags: plain members
    r.reply = 3
    assert r.georadius("g", 1, 2, 3, store="dst") == 3  # STORE returns the count untouched
    assert r.args[-2:] == ["STORE", "dst"]
    r.georadius("g", 1, 2, 3, storedist="dd")
    assert r.args[-2:] == ["STOREDIST", "dd"]
    r.reply = []
    r.georadius("g", 1, 2, 3, withhash=True, count=2)
    assert r.args == ["GEORADIUS", "g", "1", "2", "3", "m", "WITHHASH", "COUNT", "2"]  # ANY only with COUNT


def test_json_arrindex_and_numeric_replies():
    r = Rec(reply=1)
    r.json_arrindex("k", "$", "2", 1, 5)
    assert r.args == ["JSON.ARRINDEX", "k", "$", "2", "1", "5"]
    r.json_arrindex("k", "$", "2", 1)
    assert r.args == ["JSON.ARRINDEX", "k", "$", "2", "1"]
    r.json_arrindex("k", "$", "2", stop=5)  # stop without start is not sent
    assert r.args == ["JSON.ARRINDEX", "k", "$", "2"]
    r.reply = "[6]"
    assert r.json_numincrby("k", "$.n", 1) == 6  # a "$" path answers an array of one
    r.reply = "6.5"
    assert r.json_numincrby("k", "$.n", 0.5) == 6.5
    r.reply = "[1,2]"
    assert r.json_numincrby("k", "$..n", 1) == [1, 2]  # several matches stay a list
    r.reply = None
    assert r.json_numincrby("k", "$.n", 1) is None


def test_stream_reads_and_reply_shapes():
    r = Rec(reply=None)
    assert r.xread({"s": "0"}) is None
    r.reply = [["s", [["1-0", ["f", "v"]], ["2-0", None]]]]  # RESP2: list of [stream, entries]
    assert r.xread({"s": "0"}, count=2, block=10) == [("s", [StreamEntry("1-0", [("f", "v")]), StreamEntry("2-0", [])])]
    assert r.args == ["XREAD", "COUNT", "2", "BLOCK", "10", "STREAMS", "s", "0"]
    r.reply = {"s": [["1-0", ["f", "v"]]]}  # RESP3: map of stream -> entries
    assert r.xreadgroup("g", "c", {"s": ">"}, count=3, block=0, noack=True) == [("s", [StreamEntry("1-0", [("f", "v")])])]
    assert r.args == ["XREADGROUP", "GROUP", "g", "c", "COUNT", "3", "BLOCK", "0", "NOACK", "STREAMS", "s", ">"]
    r.xreadgroup("g", "c", {"s": ">"})
    assert r.args == ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]
    r.reply = None
    assert r.xrange("s", "-", "+") == []  # a missing stream is an empty range


def test_xinfo_stream_entries_become_stream_entries():
    r = Rec(reply=["length", 2, "first-entry", ["1-0", ["f", "v"]], "last-entry", None, "groups", 0])
    info = r.xinfo_stream("s")
    assert info["first-entry"] == StreamEntry("1-0", [("f", "v")])
    assert info["last-entry"] is None and info["length"] == 2
    r.reply = {"length": 0, "first-entry": None}
    assert r.xinfo_stream("s")["first-entry"] is None


def test_stream_fields_keep_order_and_repeats():
    r = Rec(reply=[["1-0", ["f", "1", "g", "", "f", "3"]]])
    assert r.xrange("s", "-", "+")[0].fields == [("f", "1"), ("g", ""), ("f", "3")]
    r.reply = [["s", [["1-0", ["f", "1", "f", "2"]]]]]
    assert r.xread({"s": "0"})[0][1][0].fields == [("f", "1"), ("f", "2")]


def test_hrandfield_with_values_keeps_repeated_fields():
    r = Rec(reply=["a", "1", "a", "1"])
    assert r.hrandfield("h", -2, withvalues=True) == [("a", "1"), ("a", "1")]
    r.reply = [["a", "1"], ["a", "1"]]  # RESP3 nested pairs
    assert r.hrandfield("h", -2, withvalues=True) == [("a", "1"), ("a", "1")]


def test_bloom_info_accepts_both_reply_shapes():
    r = Rec(reply=["Capacity", 100, "Size", 5])
    assert r.bf_info("b") == {"Capacity": 100, "Size": 5}
    r.reply = {"Capacity": 100}
    assert r.bf_info("b") == {"Capacity": 100}


def test_flush_variants_and_geo_without_hash():
    r = Rec(reply="OK")
    assert r.flushdb() is True and r.args == ["FLUSHDB"]
    assert r.flushdb(asynchronous=True) is True and r.args == ["FLUSHDB", "ASYNC"]
    assert r.flushall() is True and r.args == ["FLUSHALL"]
    assert r.flushall(asynchronous=True) is True and r.args == ["FLUSHALL", "ASYNC"]
    r.reply = [["m", "2.5", ["1", "2"]]]
    assert r.georadius("g", 1, 2, 3, withcoord=True, withdist=True) == [GeoResult("m", 2.5, None, (1.0, 2.0))]
    r.reply = [["m", ["1", "2"]]]
    assert r.georadius("g", 1, 2, 3, withcoord=True) == [GeoResult("m", None, None, (1.0, 2.0))]


def test_limit_modes_encode_their_keywords_and_convert_the_retry_reply():
    r = Rec(3)
    assert r.limit_sliding("k", 5, 1000) == 3
    assert r.args == ["LIMIT", "k", "5", "1000", "SLIDING"]
    assert r.limit_tokens("k", 10, 2000, 2) == 3
    assert r.args == ["LIMIT", "k", "10", "2000", "TOKENS", "COST", "2"]
    assert r.limit_tokens("k", 10, 2000) == 3
    assert r.args[-2:] == ["COST", "1"]

    r = Rec([2, 0])
    assert r.limit_sliding_with_retry("k", 5, 1000) == (True, 2, 0)
    assert r.args == ["LIMIT", "k", "5", "1000", "SLIDING", "WITHRETRY"]
    r = Rec([-1, 250])
    out = r.limit_tokens_with_retry("k", 10, 2000)
    assert (out.allowed, out.remaining, out.retry_after_ms) == (False, 0, 250)
    assert r.args == ["LIMIT", "k", "10", "2000", "TOKENS", "COST", "1", "WITHRETRY"]


def test_due_commands_encode_their_options():
    r = Rec(reply=[b"m", b"2"])

    assert r.zpopbyscore("z", "(5", min=1, count=3) == [(b"m", 2.0)]
    assert r.args == ["ZPOPBYSCORE", "z", "(5", "MIN", "1", "COUNT", "3", "WITHSCORES"]

    r = Rec(reply=2)

    assert r.zmovedue("{t}z", "{t}l", "+inf", count=4) == 2
    assert r.args == ["ZMOVEDUE", "{t}z", "{t}l", "+inf", "COUNT", "4"]


def test_transfer_commands_encode_the_optional_cap_and_return_a_pair():
    r = Rec(reply=[6, 4])
    assert r.transfer("a", "b", 4) == (6, 4)
    assert r.args == ["TRANSFER", "a", "b", "4"]
    assert r.transfer("a", "b", 4, max=9) == (6, 4)
    assert r.args == ["TRANSFER", "a", "b", "4", "MAX", "9"]
    assert r.htransfer("h", "f", "g", 1, max=5) == (6, 4)
    assert r.args == ["HTRANSFER", "h", "f", "g", "1", "MAX", "5"]
    r.reply = None
    assert r.transfer("a", "b", 4) is None
    assert r.htransfer("h", "f", "g", 1) is None


# --------------------------------------------------------------------------- IDEM ABORT


@pytest.mark.parametrize("reply,expected", [(1, True), (0, False)])
def test_idem_abort_encoding_and_reply(reply, expected):
    r = Rec(reply)
    assert r.idem_abort("k", "fp", "me") is expected
    assert r.args == ["IDEM", "k", "ABORT", "fp", "me"]


class _IdemStub:
    """Just the three methods ``idempotent`` calls, recording whether ABORT was sent."""

    def __init__(self, abort_fails=False):
        self.aborts = []
        self.abort_fails = abort_fails

    def idem_begin(self, key, fingerprint, owner, ttl_ms):
        from ruvio import IdemResult

        return IdemResult("acquired", None)

    def idem_complete(self, key, fingerprint, owner, result):
        return 1

    def idem_abort(self, key, fingerprint, owner):
        self.aborts.append((key, fingerprint, owner))
        if self.abort_fails:
            raise ConnectionError("down")
        return True


@pytest.mark.parametrize("flag", [False, True])
@pytest.mark.parametrize("abort_fails", [False, True])
def test_idempotent_aborts_only_when_asked_and_keeps_the_error(flag, abort_fails):
    import ruvio

    stub = _IdemStub(abort_fails)

    def boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        ruvio.Ruvio.idempotent(stub, "k", "fp", 1000, boom, owner="me", abort_on_error=flag)
    assert stub.aborts == ([("k", "fp", "me")] if flag else [])


@pytest.mark.parametrize("flag", [False, True])
def test_async_idempotent_aborts_only_when_asked(flag):
    import asyncio

    import ruvio.asyncio

    stub = _IdemStub()

    async def begin(*a):
        return stub.idem_begin(*a)

    async def abort(*a):
        return stub.idem_abort(*a)

    stub.idem_begin_async, stub.idem_abort_async = begin, abort

    class AStub:
        idem_begin = staticmethod(begin)
        idem_abort = staticmethod(abort)

    async def boom():
        raise RuntimeError("boom")

    async def run():
        await ruvio.asyncio.Ruvio.idempotent(AStub(), "k", "fp", 1000, boom, owner="me", abort_on_error=flag)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(run())
    assert stub.aborts == ([("k", "fp", "me")] if flag else [])
