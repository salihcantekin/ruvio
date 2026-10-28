# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Cluster routing without a Ruvio server: slot hashing, key extraction for every matrix command,
and the router's behavior against scripted fake servers (sync and asyncio clients)."""

from __future__ import annotations

import json
import os
import itertools

import pytest

import ruvio
import ruvio.asyncio
from ruvio import _cluster as cl
from ruvio._commands import Commands
from conftest import SyncAsAsync
from fakecluster import FakeCluster, bulk, err, standalone_node

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MATRIX = os.path.join(ROOT, "docs", "compatibility-matrix.json")


# --------------------------------------------------------------------------- slots


def test_slot_matches_redis_vectors():
    # published Redis cluster vectors / values verified against `CLUSTER KEYSLOT` on a Ruvio server
    assert cl.key_slot("foo") == 12182
    assert cl.key_slot("{a}b") == 15495
    assert cl.key_slot("user:1000") == 1649
    assert cl.key_slot("héllo") == 11061
    assert cl.key_slot(b"123456789") == 0x31C3 % 16384
    assert ruvio.key_slot("foo") == 12182


def test_hashtags():
    assert cl.key_slot("{user1000}.following") == cl.key_slot("{user1000}.followers") == cl.key_slot("user1000")
    assert cl.key_slot("foo{}{bar}") == cl.key_slot("foo{}{bar}")  # empty first tag: whole key is hashed
    assert cl.key_slot("foo{}{bar}") != cl.key_slot("bar")
    assert cl.key_slot("foo{{bar}}zap") == cl.key_slot("{bar")  # first '{' to the first '}' after it
    assert cl.key_slot("foo{bar}{zap}") == cl.key_slot("bar")
    assert cl.key_slot("{") == cl.key_slot("{") and cl.key_slot("a}b{c") == cl.key_slot("a}b{c")
    assert cl.key_slot(b"\xff\xfe") == cl.key_slot(b"\xff\xfe".decode("utf-8", "surrogateescape"))


def test_frame_parsing():
    from ruvio._resp import encode

    frame = encode("SET", ("k{x}", "v" * 5000, "PX", 100))
    assert cl.frame_head(frame) == (b"SET", b"k{x}")
    assert cl.split_frame(frame) == [b"SET", b"k{x}", b"v" * 5000, b"PX", b"100"]
    assert cl.frame_head(encode("PING", ())) == (b"PING", None)
    assert cl.split_frame(encode("XGROUP CREATE", ("s", "g", "$"))) == [b"XGROUP", b"CREATE", b"s", b"g", b"$"]


# --------------------------------------------------------------------------- key extraction


class _Rec(Commands):
    """Records the frame of the last typed call instead of sending it."""

    def __init__(self) -> None:
        self.frame = b""

    def _x(self, conv, data, decode=None):
        self.frame = data
        return None


def _route(method, *args, **kw):
    r = _Rec()
    getattr(r, method)(*args, **kw)
    args = cl.split_frame(r.frame)
    name = args[0].upper()
    try:
        p = cl.plan(name, args)
    except ruvio.RuvioError as exc:
        p = exc
    return name, p, cl.command_keys(name, args)


KEYS = [  # (typed method, arguments, expected keys)
    ("get", ("a",), ["a"]),
    ("set", ("a", "v"), ["a"]),
    ("mget", ("a", "b", "c"), ["a", "b", "c"]),
    ("mset", ({"a": 1, "b": 2},), ["a", "b"]),
    ("msetnx", ({"a": 1, "b": 2},), ["a", "b"]),
    ("delete", ("a", "b"), ["a", "b"]),
    ("exists", ("a", "b"), ["a", "b"]),
    ("rename", ("a", "b"), ["a", "b"]),
    ("copy", ("a", "b"), ["a", "b"]),
    ("smove", ("a", "b", "m"), ["a", "b"]),
    ("lmove", ("a", "b", "LEFT", "RIGHT"), ["a", "b"]),
    ("rpoplpush", ("a", "b"), ["a", "b"]),
    ("sinter", ("a", "b"), ["a", "b"]),
    ("sinterstore", ("d", "a", "b"), ["d", "a", "b"]),
    ("sintercard", (["a", "b"],), ["a", "b"]),
    ("zunionstore", ("d", ["a", "b"]), ["d", "a", "b"]),
    ("zinterstore", ("d", ["a", "b"]), ["d", "a", "b"]),
    ("zunion", (["a", "b"],), ["a", "b"]),
    ("lmpop", (["a", "b"], "LEFT"), ["a", "b"]),
    ("blmpop", (1.0, ["a", "b"], "LEFT"), ["a", "b"]),
    ("blpop", (["a", "b"], 1), ["a", "b"]),
    ("brpop", (["a", "b"], 1), ["a", "b"]),
    ("bzpopmin", (["a", "b"], 1), ["a", "b"]),
    ("bitop", ("AND", "d", "a", "b"), ["d", "a", "b"]),
    ("pfcount", ("a", "b"), ["a", "b"]),
    ("pfmerge", ("d", "a", "b"), ["d", "a", "b"]),
    ("eval", ("return 1", ["k1", "k2"], ["x"]), ["k1", "k2"]),
    ("eval", ("return 1", [], ["x"]), []),
    ("evalsha", ("abc", ["k1"], []), ["k1"]),
    ("fcall", ("f", ["k1", "k2"], []), ["k1", "k2"]),
    ("watch", ("a", "b"), ["a", "b"]),
    ("json_mget", (["a", "b"], "$.x"), ["a", "b"]),
    ("geosearchstore", ("d", "s", "m", None, None, 5), ["d", "s"]),
    ("object_encoding", ("a",), ["a"]),
    ("memory_usage", ("a",), ["a"]),
    ("xgroup_create", ("s", "g", "$"), ["s"]),
    ("xinfo_stream", ("s",), ["s"]),
    ("xread", ({"s1": "0", "s2": "0"},), ["s1", "s2"]),
    ("xreadgroup", ("g", "c", {"s1": ">", "s2": ">"}), ["s1", "s2"]),
    ("hset", ("h", "f", "v"), ["h"]),
    ("spublish", ("ch", "m"), ["ch"]),
    ("fillget", ("k", 1000), ["k"]),
    ("limit", ("k", 5, 1000), ["k"]),
    ("bf_add", ("k", "i"), ["k"]),
    ("json_get", ("k",), ["k"]),
    ("ping", (), []),
    ("dbsize", (), []),
    ("client_list", (), []),
    ("cluster_keyslot", ("k",), []),
    ("scan", (0,), []),
    ("publish", ("ch", "m"), []),
    ("info", (), []),
    ("script_load", ("return 1",), []),
]


@pytest.mark.parametrize("method,args,keys", KEYS)
def test_command_keys(method, args, keys):
    try:
        name, p, got = _route(method, *args)
    except TypeError:
        pytest.skip(f"signature of {method} differs")
    assert [k.decode() for k in got] == keys, name


def test_route_plans():
    assert _route("get", "a")[1] == (cl.PLAN_SLOT, cl.key_slot("a"))
    assert _route("ping")[1] == (cl.PLAN_SLOT, None)
    assert _route("publish", "c", "m")[1] == (cl.PLAN_SLOT, 0)
    assert _route("dbsize")[1] == (cl.PLAN_FAN, cl.FAN_SUM)
    assert _route("keys", "*")[1] == (cl.PLAN_FAN, cl.FAN_CAT)
    assert _route("flushdb")[1] == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert _route("script_load", "return 1")[1] == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert _route("script_kill")[1] == (cl.PLAN_SLOT, None)
    assert _route("client_setname", "n")[1] == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert _route("client_list")[1] == (cl.PLAN_SLOT, None)
    assert _route("scan", 0)[1] == (cl.PLAN_SCAN,)
    assert _route("mget", "{t}a", "{t}b")[1] == (cl.PLAN_SLOT, cl.key_slot("t"))
    assert _route("mget", "a", "b")[1][0] == cl.PLAN_SPLIT
    assert _route("mset", {"a": 1, "b": 2})[1][0] == cl.PLAN_SPLIT
    for m, a in (("rename", ("a", "b")), ("sinter", ("a", "b")), ("bitop", ("AND", "d", "a", "b")),
                 ("zunionstore", ("d", ["a", "b"])), ("msetnx", ({"a": 1, "b": 2},)), ("watch", ("a", "b"))):
        assert isinstance(_route(m, *a)[1], ruvio.CrossSlotError), m
    for m in ("multi", "exec", "discard"):
        p = _route(m)[1]
        assert isinstance(p, ruvio.RuvioError) and not isinstance(p, ruvio.ResponseError), m


def test_pipeline_slot_rules():
    from ruvio._resp import encode

    assert cl.pipeline_slot(encode("GET", ("a",))) == cl.key_slot("a")
    assert cl.pipeline_slot(encode("PING", ())) is None
    with pytest.raises(ruvio.CrossSlotError):
        cl.pipeline_slot(encode("MGET", ("a", "b")))
    with pytest.raises(ruvio.RuvioError):
        cl.pipeline_slot(encode("DBSIZE", ()))


# Matrix names that have exactly one key, argument 1 (the default rule). Every other matrix command
# must be listed in cl.SPEC (keyless / multi-key / special routing).
SINGLE_KEY = set(
    """GET SETV TAKE DELEX GETEX LIMIT IDEM ONCE LEASE SEMAPHORE RELEASE SET GETSET APPEND STRLEN INCR DECR INCRBY
    DECRBY TYPE MOVE EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST LPUSH LPUSHCAP RPUSHCAP RPUSH LPOP RPOP LLEN
    LINDEX LRANGE LTRIM SADD SREM SISMEMBER SCARD SMEMBERS SPOP SRANDMEMBER HSET HGET HDEL HLEN HGETALL HMGET HEXISTS
    HKEYS HVALS HINCRBY HMSET HSETNX HSTRLEN HSCAN HEXPIRE HPEXPIRE HEXPIREAT HPEXPIREAT HTTL HPTTL HEXPIRETIME
    HPEXPIRETIME HPERSIST ZADD ZRANGE ZREVRANGE ZRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZREM ZINCRBY ZCARD ZSCORE
    ZRANK ZREVRANK ZPOPMIN ZPOPMAX ZPOPBYSCORE ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX GEOADD GEODIST GEOHASH GEOPOS
    GEOSEARCH BF.RESERVE BF.ADD BF.MADD BF.INSERT BF.INFO BF.CARD BF.EXISTS BF.MEXISTS JSON.GET JSON.SET JSON.DEL
    XADD XLEN XRANGE XREVRANGE XDEL XSETID XTRIM XACK XPENDING XAUTOCLAIM XCLAIM SSCAN ZSCAN JSON.MERGE JSON.TYPE
    JSON.STRLEN JSON.ARRLEN JSON.OBJKEYS JSON.NUMINCRBY JSON.TOGGLE JSON.ARRAPPEND JSON.CLEAR JSON.OBJLEN
    JSON.STRAPPEND JSON.NUMMULTBY JSON.ARRINSERT JSON.ARRPOP JSON.ARRTRIM JSON.ARRINDEX JSON.FORGET JMERGE SETBIT
    BITFIELD BITFIELD_RO GETBIT BITCOUNT BITPOS PFADD FILLGET FILLSET SETEX PSETEX SETNX GETDEL INCRBYFLOAT GETRANGE
    SUBSTR SETRANGE SMISMEMBER ZCOUNT ZLEXCOUNT ZMSCORE ZRANDMEMBER HRANDFIELD HINCRBYFLOAT LINSERT LSET LREM LPOS
    LPUSHX RPUSHX EXPIRETIME PEXPIRETIME DUMP RESTORE SPUBLISH SETV HTRANSFER RENEW""".split()
)


def test_every_matrix_command_has_a_routing_rule():
    with open(MATRIX, encoding="utf-8") as f:
        names = [c["name"] for c in json.load(f)["commands"]]
    unrouted = []
    for n in names:
        first = n.split(" ")[0].encode()
        if first in cl.SPEC:
            assert first.decode() not in SINGLE_KEY, f"{first!r} is both in SPEC and the single-key table"
        elif first.decode() not in SINGLE_KEY:
            unrouted.append(n)
    assert not unrouted, f"no routing rule for: {unrouted}"
    # nothing stale in the single-key table
    first_words = {n.split(" ")[0] for n in names}
    assert not (SINGLE_KEY - first_words), SINGLE_KEY - first_words


# --------------------------------------------------------------------------- topology / MOVED


def test_parse_slots_and_moved():
    reply = [
        [8192, 16383, ["127.0.0.1", 7002, "b"]],
        [0, 8191, [b"127.0.0.1", 7001, "a"]],
    ]
    t = cl.parse_slots(reply, "seed")
    assert t.nodes == [("127.0.0.1", 7001), ("127.0.0.1", 7002)]  # slot order, not reply order
    assert t.slots[0] == 0 and t.slots[8191] == 0 and t.slots[8192] == 1 and t.slots[16383] == 1
    assert cl.parse_slots([[0, 16383, ["", 7001, "a"]]], "seedhost").nodes == [("seedhost", 7001)]
    assert cl.parse_slots(None, "x") is None and cl.parse_slots([], "x") is None
    assert cl.parse_moved(ruvio.MovedError("MOVED 3999 127.0.0.1:6381")) == (3999, "127.0.0.1", 6381)
    assert cl.parse_moved(ruvio.MovedError("MOVED 3999 [::1]:6381")) == (3999, "[::1]", 6381)
    assert cl.parse_moved(ruvio.ResponseError("ERR nope")) is None


def test_scan_cursor_math():
    assert cl.scan_prepare([b"SCAN", b"0"], 3)[0] == 0
    node, frame = cl.scan_prepare([b"SCAN", b"7", b"MATCH", b"x*"], 3)
    assert node == 1 and cl.split_frame(frame) == [b"SCAN", b"2", b"MATCH", b"x*"]
    assert cl.scan_finish([b"0", []], 0, 3) == [b"1", []]  # node 0 done -> start node 1
    assert cl.scan_finish([b"0", []], 2, 3) == [b"0", []]  # last node done -> 0
    assert cl.scan_finish(["5", ["k"]], 1, 3) == ["16", ["k"]]
    with pytest.raises(ruvio.ResponseError):
        cl.scan_prepare([b"SCAN", b"abc"], 3)


# --------------------------------------------------------------------------- fake-server behavior


@pytest.fixture(params=["sync", "async"])
def make(request):
    """``make(addr, **kw)`` builds a cluster client of the parametrized kind (awaitable API)."""
    def build(addr, **kw):
        host, port = addr.rsplit(":", 1)
        if request.param == "sync":
            return SyncAsAsync(ruvio.Ruvio(host, int(port), cluster=True, **kw))
        return ruvio.asyncio.Ruvio(host, int(port), cluster=True, **kw)

    yield build


def st(c):
    return c._c._cl if isinstance(c, SyncAsAsync) else c._cl


def pipe(c, **kw):
    return c._c.pipeline(**kw) if isinstance(c, SyncAsAsync) else c.pipeline(**kw)


def key_in(lo, hi, prefix="k"):
    for i in itertools.count():
        k = f"{prefix}{i}"
        if lo <= cl.key_slot(k) <= hi:
            return k


@pytest.fixture
def cluster():
    c = FakeCluster([(0, 8191), (8192, 16383)])
    yield c
    c.close()


async def test_routes_to_the_owner_only(make, cluster):
    c = make(cluster.nodes[0].addr)
    a, b = key_in(0, 8191), key_in(8192, 16383)
    assert await c.set(a, "1") is True
    assert await c.set(b, "2") is True
    assert await c.get(a) == "1" and await c.get(b) == "2"
    assert [x[0] for x in cluster.nodes[0].commands() if x[0] in ("SET", "GET")] == ["SET", "GET"]
    assert [x[0] for x in cluster.nodes[1].commands() if x[0] in ("SET", "GET")] == ["SET", "GET"]
    assert not any(x[1] == b for x in cluster.nodes[0].commands("GET"))
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1  # discovered once
    await c.aclose()


async def test_cross_slot_fails_fast_and_sends_nothing(make, cluster):
    c = make(cluster.nodes[0].addr)
    await c.ping()
    before = [len(n.log) for n in cluster.nodes]
    a, b = key_in(0, 8191), key_in(8192, 16383)
    for call in (c.rename(a, b), c.sinter(a, b), c.msetnx({a: 1, b: 2}), c.bitop("AND", a, a + "x", b)):
        with pytest.raises(ruvio.CrossSlotError) as ei:
            await call
        assert ei.value.code == "CROSSSLOT"
    p = pipe(c)
    p.set(a, 1).sinter(a, b)
    with pytest.raises(ruvio.CrossSlotError):
        await (_maybe(p.execute()))
    t = pipe(c, transaction=True)
    t.set(a, 1).set(b, 2)
    with pytest.raises(ruvio.CrossSlotError):
        await _maybe(t.execute())
    t = pipe(c, transaction=True)
    t.mget(a, b)  # split in a plain pipeline, refused in a transaction
    with pytest.raises(ruvio.CrossSlotError, match="one slot"):
        await _maybe(t.execute())
    with pytest.raises(ruvio.RuvioError):
        await c.multi()
    assert [len(n.log) for n in cluster.nodes] == before
    await c.aclose()


async def _maybe(x):
    import inspect

    return await x if inspect.isawaitable(x) else x


async def test_split_commands_reassemble_in_caller_order(make, cluster):
    c = make(cluster.nodes[0].addr)
    keys = [key_in(0, 8191, "p"), key_in(8192, 16383, "q"), key_in(0, 8191, "r"), key_in(8192, 16383, "s"), key_in(0, 8191, "t")]
    keys = list(dict.fromkeys(keys + [key_in(0, 8191, "u")]))
    assert await c.mset({k: f"v{i}" for i, k in enumerate(keys)}) is True
    got = await c.mget(*keys, "missing-key")
    assert got == [f"v{i}" for i in range(len(keys))] + [None]
    assert await c.mget(keys[3], keys[0], keys[3]) == ["v3", "v0", "v3"]
    assert await c.exists(*keys, "missing-key") == len(keys)
    assert await c.delete(*keys[:3]) == 3
    assert await c.mget(*keys[:3]) == [None, None, None]
    assert await c.touch(*keys[3:]) == len(keys) - 3
    assert await c.unlink(*keys[3:]) == len(keys) - 3
    # no sub-command ever mixed slots (the fake node answers CROSSSLOT otherwise)
    await c.aclose()


async def test_moved_patches_one_slot_and_retries(make, cluster):
    # stale map: node0 is believed to own up to slot 12000, node1 from 12001
    cluster.advertised = [(0, 12000), (12001, 16383)]
    c = make(cluster.nodes[0].addr)
    k = key_in(8192, 12000)  # really node1, believed node0
    cluster.nodes[1].store[k] = "there"
    assert await c.get(k) == "there"
    assert len(cluster.nodes[0].commands("GET")) == 1  # one MOVED
    assert len(cluster.nodes[1].commands("GET")) == 1  # one retry
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1  # a single moved slot: no refresh
    assert await c.get(k) == "there"  # the slot was patched: straight to node1
    assert len(cluster.nodes[0].commands("GET")) == 1
    assert len(cluster.nodes[1].commands("GET")) == 2
    await c.aclose()


async def test_moved_for_another_command_patches_again_without_a_reload(make, cluster):
    cluster.advertised = [(0, 12000), (12001, 16383)]
    c = make(cluster.nodes[0].addr)
    k1 = key_in(8192, 12000, "m")
    k2 = next(k for k in (key_in(8192, 12000, f"n{i}") for i in range(50)) if cl.key_slot(k) != cl.key_slot(k1))
    await c.set(k1, "1")
    await c.set(k2, "2")  # a first MOVED for this command too: one more slot patched, no reload
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1
    assert st(c).slots[cl.key_slot(k1)] == 1 and st(c).slots[cl.key_slot(k2)] == 1
    assert st(c).slots[8192] == 0  # the rest of the stale range is untouched
    await c.aclose()


async def test_second_moved_for_one_command_reloads_the_whole_table(make, cluster):
    c = make(cluster.nodes[0].addr)
    k = key_in(0, 8191)
    await c.ping()
    n0, n1 = cluster.nodes
    orig = cluster._handle
    sent = []

    def handle(node, args):
        if args[0].upper() == "GET":
            sent.append(node)
            if len(sent) == 1:  # node0 sends it to node1, node1 sends it back
                return err(f"MOVED {cl.key_slot(k)} {n1.addr}")
            if len(sent) == 2:
                return err(f"MOVED {cl.key_slot(k)} {n0.addr}")
            return bulk("here")
        return orig(node, args)

    n0.handler = n1.handler = handle
    assert await c.get(k) == "here"
    assert sent == [n0, n1, n0]
    # first MOVED: the slot was patched only; second MOVED: CLUSTER SLOTS was read again
    assert len(n0.commands("CLUSTER")) == 2 and not n1.commands("CLUSTER")
    assert st(c).slots[cl.key_slot(k)] == 0
    await c.aclose()


async def test_moved_is_followed_at_most_three_times(make, cluster):
    c = make(cluster.nodes[0].addr)
    k = key_in(0, 8191)
    await c.ping()
    # both nodes now claim the other owns the slot: ping-pong must stop after three redirects
    cluster.owner_ranges = [(5000, 6000), (0, 4999)]
    cluster.owner_ranges = [(0, 8191), (8192, 16383)]
    n0, n1 = cluster.nodes
    orig = cluster._handle

    def pingpong(node, args):
        if args[0].upper() == "GET":
            other = n1 if node is n0 else n0
            return err(f"MOVED {cl.key_slot(args[1])} {other.addr}")
        return orig(node, args)

    n0.handler = n1.handler = pingpong
    with pytest.raises(ruvio.MovedError):
        await c.get(k)
    assert len(n0.commands("GET")) + len(n1.commands("GET")) == 1 + cl.MAX_REDIRECTS == 4
    await c.aclose()


async def test_transport_failure_is_not_replayed_and_isolates_the_node(make, cluster):
    c = make(cluster.nodes[0].addr)
    a = key_in(0, 8191)
    b = "dropme"  # the fake closes the socket when it sees GET dropme
    owner = cluster.owner(cl.key_slot(b))
    other = cluster.nodes[0] if owner is cluster.nodes[1] else cluster.nodes[1]
    ak = a if other is cluster.nodes[0] else key_in(8192, 16383)
    await c.set(ak, "x")
    with pytest.raises(ruvio.ConnectionError):
        await c.get(b)
    assert len(owner.commands("GET")) == 1  # sent exactly once, never replayed
    assert await c.get(ak) == "x"  # the other node is unaffected
    owner.handler = lambda node, args: bulk("back") if args[0].upper() == "GET" else cluster._handle(node, args)
    assert await c.get(b) == "back"  # lazy reconnect
    assert owner.connections >= 2
    await c.aclose()


async def test_pipeline_groups_by_node_and_keeps_order(make, cluster):
    c = make(cluster.nodes[0].addr)
    ks = [key_in(0, 8191, "a"), key_in(8192, 16383, "b"), key_in(0, 8191, "c"), key_in(8192, 16383, "d")]
    p = pipe(c)
    for i, k in enumerate(ks):
        p.set(k, str(i))
    for k in reversed(ks):
        p.get(k)
    res = await _maybe(p.execute())
    assert res == [True] * 4 + ["3", "2", "1", "0"]
    assert [x[0] for x in cluster.nodes[0].commands() if x[0] in ("SET", "GET")] == ["SET", "SET", "GET", "GET"]
    await c.aclose()


async def test_pipeline_recovers_from_stale_slots(make, cluster):
    cluster.advertised = [(0, 12000), (12001, 16383)]
    c = make(cluster.nodes[0].addr)
    ks = [key_in(8192, 12000, f"x{i}") for i in range(4)]
    p = pipe(c)
    for i, k in enumerate(ks):
        p.set(k, str(i))
    for k in ks:
        p.get(k)
    assert await _maybe(p.execute()) == [True] * 4 + ["0", "1", "2", "3"]
    await c.aclose()


async def test_pipeline_splits_cross_slot_commands_like_the_plain_command(make, cluster):
    c = make(cluster.nodes[0].addr)
    a, b, a2 = key_in(0, 8191, "a"), key_in(8192, 16383, "b"), key_in(0, 8191, "c")
    p = pipe(c)
    p.mset({a: "1", b: "2", a2: "3"}).get(b).mget(b, "nokey", a, a2).exists(a, b, "nokey").ping()
    p.touch(a, b).unlink(a2, b).delete(a, b)
    res = await _maybe(p.execute())
    assert res == [True, "2", ["2", None, "1", "3"], 2, "PONG", 2, 2, 1]
    # no sub-command ever mixed slots (the fake node answers CROSSSLOT otherwise), none was sent whole
    for n in cluster.nodes:
        for x in n.commands("MGET") + n.commands("MSET") + n.commands("DEL"):
            assert len({cl.key_slot(k) for k in (x[1::2] if x[0] == "MSET" else x[1:])}) == 1
    await c.aclose()


async def test_pipeline_split_command_follows_moved_per_sub_command(make, cluster):
    cluster.advertised = [(0, 12000), (12001, 16383)]
    c = make(cluster.nodes[0].addr)
    stale, near = key_in(8192, 12000, "s"), key_in(0, 8191, "n")  # both believed on node0
    cluster.nodes[1].store[stale] = "far"
    cluster.nodes[0].store[near] = "near"
    p = pipe(c)
    p.mget(stale, near).get(near)
    assert await _maybe(p.execute()) == [["far", "near"], "near"]
    assert [x[1:] for x in cluster.nodes[1].commands("MGET")] == [[stale]]  # only the moved part was re-sent
    await c.aclose()


async def test_transaction_goes_to_one_node(make, cluster):
    c = make(cluster.nodes[0].addr)
    k1, k2 = "{tx}one", "{tx}two"
    owner = cluster.owner(cl.key_slot("tx"))
    t = pipe(c, transaction=True)
    t.set(k1, 1).set(k2, 2)
    await _maybe(t.execute())
    names = [x[0] for x in owner.commands()]
    assert names.count("MULTI") == 1 and names.count("EXEC") == 1
    other = cluster.nodes[0] if owner is cluster.nodes[1] else cluster.nodes[1]
    assert not other.commands("MULTI")
    await c.aclose()


async def test_standalone_server_falls_back(make):
    node = standalone_node()
    try:
        c = make(node.addr)
        assert await c.set("a", "1") is True and await c.set("b", "2") is True
        assert await c.mget("a", "b") == ["1", "2"]  # no splitting, no routing
        assert st(c).mode == 2
        assert [x[0] for x in node.commands() if x[0] == "CLUSTER"] == ["CLUSTER"]  # asked exactly once
        await c.aclose()
    finally:
        node.close()


def test_cluster_false_is_the_plain_client():
    assert type(ruvio.Ruvio("127.0.0.1", 1)) is ruvio.Ruvio
    assert type(ruvio.asyncio.Ruvio("127.0.0.1", 1)) is ruvio.asyncio.Ruvio
    assert type(ruvio.Ruvio("127.0.0.1", 1, cluster=True)) is ruvio.ClusterRuvio
    assert type(ruvio.asyncio.Ruvio("127.0.0.1", 1, cluster=True)) is ruvio.asyncio.ClusterRuvio
    with pytest.raises(ValueError):
        ruvio.Ruvio(unix_socket_path="/tmp/x", cluster=True)


async def test_moved_to_a_node_missing_from_the_map_refreshes_and_retries(make):
    three = FakeCluster([(0, 5000), (5001, 10000), (10001, 16383)])
    try:
        three.advertised = [(0, 5000), (5001, 16383), (10001, 16383)]
        three.hidden = {2}  # CLUSTER SLOTS does not list node2, though it owns the top slots
        c = make(three.nodes[0].addr)
        k = key_in(10001, 16383)
        three.nodes[2].store[k] = "far"
        assert await c.get(k) == "far"  # node1 -> MOVED node2 (unknown) -> refresh -> retry on node2
        assert len(three.nodes[0].commands("CLUSTER")) + len(three.nodes[1].commands("CLUSTER")) >= 2
        assert len(three.nodes[2].commands("GET")) == 1
        await c.aclose()
    finally:
        three.close()
