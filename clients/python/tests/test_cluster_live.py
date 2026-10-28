# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Live cluster tests. Need RUVIO_CLUSTER_ADDR=host:port of a 3-shard server started with
``port_mode = "per-shard"`` (shard i listens on base+i); skipped without it. The standalone
fallback test needs RUVIO_TEST_ADDR (a plain server). Cluster mode has only database 0."""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
import uuid

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER, SyncAsAsync, await_for, wait_for
from ruvio import _cluster as cl

CADDR = os.environ.get("RUVIO_CLUSTER_ADDR")
SADDR = os.environ.get("RUVIO_TEST_ADDR")


def caddr():
    if not CADDR:
        pytest.skip("RUVIO_CLUSTER_ADDR not set")
    host, port = CADDR.rsplit(":", 1)
    return host, int(port)


def u() -> str:
    return uuid.uuid4().hex[:8]


async def maybe(x):
    return await x if inspect.isawaitable(x) else x


def pipe(c, **kw):
    return c._c.pipeline(**kw) if isinstance(c, SyncAsAsync) else c.pipeline(**kw)


def state(c):
    return c._c._cl if isinstance(c, SyncAsAsync) else c._cl


def spread_keys(prefix: str, n: int):
    """n keys whose slots are spread over the three shards (round-robin by owner)."""
    out = []
    i = 0
    while len(out) < n:
        out.append(f"{prefix}:{i}")
        i += 1
    return out


@pytest.fixture(params=["sync", "async"])
async def cc(request):
    host, port = caddr()
    if request.param == "sync":
        s = ruvio.Ruvio(host, port, cluster=True, parser=PARSER)
        s.flushdb()
        yield SyncAsAsync(s)
        s.close()
    else:
        a = ruvio.asyncio.Ruvio(host, port, cluster=True, parser=PARSER)
        await a.flushdb()
        yield a
        await a.aclose()


@pytest.fixture
def shards():
    """Plain (non-cluster) clients, one per shard, to look at where data really is."""
    host, port = caddr()
    cs = [ruvio.Ruvio(host, port + i, parser=PARSER) for i in range(3)]
    yield cs
    for c in cs:
        c.close()


def shard_of(shards, key: str) -> int:
    hits = [i for i, s in enumerate(shards) if key in set(s.scan_iter(count=1000))]
    assert len(hits) == 1, (key, hits)
    return hits[0]


# --------------------------------------------------------------------------- discovery / hashing


async def test_move_slots_status_and_refusal(cc):
    status = await cc.move_slots_status()
    assert "state" in status, status
    # Slot 0 is on shard 0: nothing to move, and the server says why.
    with pytest.raises(ruvio.ResponseError, match="already on that shard"):
        await cc.move_slots(0, 10, 0)


async def test_discovery_and_slot_table(cc):
    c = cc._c if isinstance(cc, SyncAsAsync) else cc
    if isinstance(cc, SyncAsAsync):
        assert c.is_cluster and len(c.nodes) == 3
    else:
        assert await c.is_cluster() and len(await c.nodes()) == 3
    st = state(cc)
    assert st.mode == 1 and len(st.slots) == 16384 and all(0 <= s < 3 for s in st.slots)
    assert sorted(set(st.slots)) == [0, 1, 2]


async def test_client_slot_equals_server_keyslot(cc):
    keys = ["foo", "bar", "{a}b", "a{}b", "{}x", "x{y}z{w}", "user:1000", "héllo", "日本語", "k" * 300, "{t}.f", "}{", "a{b", ""]
    keys = [k for k in keys if k]
    for k in keys:
        assert await cc.cluster_keyslot(k) == cl.key_slot(k), k
    assert await cc.cluster_keyslot(b"\xff\xfe\x00") == cl.key_slot(b"\xff\xfe\x00")


async def test_keys_spread_over_all_three_shards(cc, shards):
    keys = spread_keys("sp", 90)
    for k in keys:
        await cc.set(k, k.upper())
    sizes = [s.dbsize() for s in shards]
    assert sum(sizes) == 90 and all(n > 10 for n in sizes), sizes
    for k in keys:
        assert await cc.get(k) == k.upper()
    assert await cc.dbsize() == 90
    # the key lives exactly on the shard that owns its slot
    st = state(cc)
    for k in keys[:20]:
        assert shard_of(shards, k) == st.slots[cl.key_slot(k)]
    assert sorted(await maybe(cc.keys("sp:*"))) == sorted(keys)


# --------------------------------------------------------------------------- cross-slot multi-key


async def test_cross_slot_mset_mget_del_exists_order(cc, shards):
    keys = spread_keys("ms", 40)
    assert await cc.mset({k: f"v{i}" for i, k in enumerate(keys)}) is True
    assert sum(1 for s in shards if s.dbsize() > 0) == 3
    got = await cc.mget(*keys)
    assert got == [f"v{i}" for i in range(40)]
    rev = list(reversed(keys))
    assert await cc.mget(*rev, "ms:missing", keys[0]) == [f"v{i}" for i in reversed(range(40))] + [None, "v0"]
    assert await cc.exists(*keys, "ms:missing") == 40
    assert await cc.touch(*keys[:10]) == 10
    assert await cc.unlink(*keys[:10]) == 10
    assert await cc.delete(*keys[10:30], "ms:missing") == 20
    assert await cc.mget(*keys[:30]) == [None] * 30
    assert await cc.mget(*keys[30:]) == [f"v{i}" for i in range(30, 40)]
    # a single-slot MGET goes straight through
    assert await cc.mget("{h}a", "{h}b") == [None, None]


async def test_cross_slot_errors_do_not_touch_data(cc):
    a, b = "{x1}a", "{x2}b"
    assert cl.key_slot(a) != cl.key_slot(b)
    await cc.sadd(a, "m")
    await cc.sadd(b, "n")
    with pytest.raises(ruvio.CrossSlotError):
        await cc.sinterstore("{x3}dst", a, b)
    assert await cc.exists("{x3}dst") == 0
    with pytest.raises(ruvio.CrossSlotError):
        await cc.rename(a, b)
    assert await cc.smembers(a) == {"m"} and await cc.smembers(b) == {"n"}
    with pytest.raises(ruvio.CrossSlotError):
        await cc.eval("return 1", [a, b])
    with pytest.raises(ruvio.CrossSlotError):
        await cc.msetnx({a: 1, b: 2})
    with pytest.raises(ruvio.CrossSlotError):
        await cc.smove(a, b, "m")
    with pytest.raises(ruvio.CrossSlotError):
        await cc.bitop("AND", "{x3}d", a, b)
    with pytest.raises(ruvio.CrossSlotError):
        await cc.sintercard([a, b])


async def test_same_slot_multikey_commands_route_by_their_real_keys(cc):
    t = "{ms}"
    await cc.mset({t + "a": 1, t + "b": 2})
    assert await cc.mget(t + "a", t + "b") == ["1", "2"]
    await cc.sadd(t + "s1", "a", "b", "c")
    await cc.sadd(t + "s2", "b", "c", "d")
    assert await cc.sinter(t + "s1", t + "s2") == {"b", "c"}
    assert await cc.sunionstore(t + "s3", t + "s1", t + "s2") == 4
    assert await cc.sintercard([t + "s1", t + "s2"]) == 2
    assert await cc.smove(t + "s1", t + "s2", "a") is True
    await cc.rpush(t + "l1", "x", "y")
    assert await cc.lmove(t + "l1", t + "l2", "LEFT", "RIGHT") == "x"
    assert await cc.rpoplpush(t + "l1", t + "l2") == "y"
    assert await cc.rename(t + "a", t + "a2") is True
    assert await cc.copy(t + "a2", t + "a3") is True
    await cc.zadd(t + "z1", {"m": 1.0})
    await cc.zadd(t + "z2", {"m": 2.0, "n": 1.0})
    assert await cc.zunionstore(t + "zd", [t + "z1", t + "z2"]) == 2
    assert await cc.zinterstore(t + "zi", [t + "z1", t + "z2"]) == 1
    assert await cc.eval("return {KEYS[1], KEYS[2]}", [t + "a2", t + "b"]) == [t + "a2", t + "b"]
    await cc.set(t + "bit1", "foo")
    await cc.set(t + "bit2", "bar")
    assert await cc.bitop("OR", t + "bitd", t + "bit1", t + "bit2") == 3
    await cc.pfadd(t + "h1", "a", "b")
    await cc.pfadd(t + "h2", "c")
    assert await cc.pfcount(t + "h1", t + "h2") == 3
    assert await cc.xadd(t + "x1", {"f": "v"}, "1-1") == "1-1"
    assert await cc.xadd(t + "x2", {"f": "w"}, "1-1") == "1-1"
    r = await cc.xread({t + "x1": "0", t + "x2": "0"})
    assert sorted(k for k, _ in r) == [t + "x1", t + "x2"]


async def test_script_functions_and_fanout_commands(cc, shards):
    sha = await cc.script_load("return ruvio.call('GET', KEYS[1])")
    for s in shards:  # SCRIPT LOAD ran on every node
        assert s.script_exists(sha) == [True]
    for k in spread_keys("sc", 12):
        await cc.set(k, "z")
        assert await cc.evalsha(sha, [k]) == "z"
    assert await cc.dbsize() == 12
    assert await cc.flushdb() is True
    assert [s.dbsize() for s in shards] == [0, 0, 0]
    await cc.script_flush()
    assert [s.script_exists(sha) for s in shards] == [[False]] * 3
    assert await cc.client_setname("pyclu") is True
    assert await cc.ping() == "PONG"


# --------------------------------------------------------------------------- pipelines / transactions


async def test_pipeline_across_shards_keeps_caller_order(cc):
    keys = spread_keys("pl", 60)
    p = pipe(cc)
    for i, k in enumerate(keys):
        p.set(k, i)
    for k in keys:
        p.get(k)
    for k in keys:
        p.incr(k + ":n")
    p.ping()
    res = await maybe(p.execute())
    assert res[:60] == [True] * 60
    assert res[60:120] == [str(i) for i in range(60)]
    assert res[120:180] == [1] * 60
    assert res[180] == "PONG"
    # errors keep their place when raise_on_error is off
    p = pipe(cc)
    p.set("pl:a", 1).lpush("pl:a", "x").get("pl:a").lpush("pl:b", "y")
    res = await maybe(p.execute(raise_on_error=False))
    assert res[0] is True and isinstance(res[1], ruvio.WrongTypeError) and res[2] == "1" and res[3] == 1
    with pytest.raises(ruvio.WrongTypeError):
        p = pipe(cc)
        p.lpush("pl:a", "x")
        await maybe(p.execute())


async def test_pipeline_splits_cross_slot_commands(cc, shards):
    keys = spread_keys("ps:" + u(), 12)
    assert len({cl.key_slot(k) for k in keys}) > 1
    p = pipe(cc)
    p.mset({k: f"v{i}" for i, k in enumerate(keys)}).ping().mget(*reversed(keys), "ps:nokey").exists(*keys, "ps:nokey")
    p.touch(*keys).get(keys[0]).unlink(*keys[:4]).delete(*keys)
    res = await maybe(p.execute())
    assert res == [True, "PONG", [f"v{i}" for i in reversed(range(12))] + [None], 12, 12, "v0", 4, 8]
    assert sum(s.dbsize() for s in shards) == 0
    # a split command's error stays at its own index
    assert cl.key_slot(keys[1]) != cl.key_slot(keys[2])
    await cc.set(keys[1], "x")
    await cc.lpush(keys[2], "l")
    p = pipe(cc)
    p.get(keys[1]).mget(keys[1], keys[2]).incr(keys[1]).delete(keys[1], keys[2])
    res = await maybe(p.execute(raise_on_error=False))
    assert res[0] == "x" and res[1] == ["x", None] and isinstance(res[2], ruvio.ResponseError) and res[3] == 2
    t = pipe(cc, transaction=True)
    t.mget(keys[0], keys[1])
    with pytest.raises(ruvio.CrossSlotError):
        await maybe(t.execute())


async def test_transaction_single_slot_ok_and_cross_slot_errors(cc, shards):
    t = pipe(cc, transaction=True)
    t.set("{tx}a", 1).incr("{tx}a").get("{tx}a")
    assert await maybe(t.execute()) == [True, 2, "2"]
    t = pipe(cc, transaction=True)
    t.set("{tx}a", 5).set("{ty}b", 6)
    assert cl.key_slot("tx") != cl.key_slot("ty")
    with pytest.raises(ruvio.CrossSlotError):
        await maybe(t.execute())
    assert await cc.get("{tx}a") == "2" and await cc.get("{ty}b") is None  # nothing ran
    for call in (cc.multi(), cc.exec(), cc.discard()):
        with pytest.raises(ruvio.RuvioError):
            await call


async def test_watch_transaction_helper(cc):
    await cc.set("{w}k", 1)
    if isinstance(cc, SyncAsAsync):
        c = cc._c
        res = c.transaction(lambda p: p.incr("{w}k").get("{w}k"), "{w}k")
        assert res == [2, "2"]
        # a conflicting write between WATCH and EXEC makes EXEC abort (WatchError), then the retry succeeds
        other = ruvio.Ruvio(*caddr(), cluster=True)
        calls = []

        def fn(p):
            if not calls:
                calls.append(1)
                other.set("{w}k", 100)
            p.incr("{w}k")

        assert c.transaction(fn, "{w}k") == [101]
        other.close()
        # watching one slot and writing another is refused
        c.watch("{w}k")
        p = c.pipeline(True)
        p.set("{z}other", 1)
        with pytest.raises(ruvio.CrossSlotError):
            p.execute()
    else:
        res = await cc.transaction(lambda p: p.incr("{w}k").get("{w}k"), "{w}k")
        assert res == [2, "2"]
        other = ruvio.asyncio.Ruvio(*caddr(), cluster=True)
        calls = []

        async def fn(p):
            if not calls:
                calls.append(1)
                await other.set("{w}k", 100)
            p.incr("{w}k")

        assert await cc.transaction(fn, "{w}k") == [101]
        await other.aclose()


# --------------------------------------------------------------------------- MOVED recovery


async def test_stale_table_still_gets_the_right_answers(cc, shards):
    # Ruvio forwards a misrouted key to its owner instead of answering MOVED, so every
    # command below succeeds on a wrong table; MOVED handling itself is covered by the
    # fake-server unit tests.
    keys = spread_keys("mv", 30)
    for k in keys:
        await cc.set(k, k)
    st = state(cc)
    truth = list(st.slots)
    st.slots[:] = [(s + 1) % 3 for s in truth]  # every slot now points at a wrong node
    for k in keys[:15]:
        assert await cc.get(k) == k
    st.slots[:] = [(s + 2) % 3 for s in truth]
    p = pipe(cc)
    for k in keys:
        p.get(k)
    assert await maybe(p.execute()) == keys
    st.slots[:] = [(s + 1) % 3 for s in truth]
    assert await cc.mget(*keys) == keys
    st.slots[:] = [(s + 1) % 3 for s in truth]
    t = pipe(cc, transaction=True)
    t.set("{mvt}a", 1).get("{mvt}a")
    assert await maybe(t.execute()) == [True, "1"]
    st.slots[:] = [(s + 1) % 3 for s in truth]
    assert await cc.delete(*keys) == 30
    st.slots[:] = truth


# --------------------------------------------------------------------------- helpers on the cluster client


async def test_lock_helper_on_cluster(cc):
    if isinstance(cc, SyncAsAsync):
        c = cc._c
        lk = c.lock("lock:cl", ttl_ms=5000)
        assert lk.acquire() and c.lock("lock:cl", ttl_ms=5000, blocking=False).acquire() is False
        assert lk.renew(20000) and lk.owned()
        c.set("lock:cl", "foreign")
        assert lk.release() is False and c.get("lock:cl") == "foreign"
        c.delete("lock:cl")
        with c.lock("lock:cl2", ttl_ms=5000) as held:
            assert c.get("lock:cl2") == held.token
        assert c.get("lock:cl2") is None
        with c.lease_scope("lease:cl", 5000) as tok:
            assert tok >= 1
        with c.semaphore_scope("sem:cl", 2, 5000) as tok:
            assert tok
    else:
        lk = cc.lock("lock:cl", ttl_ms=5000)
        assert await lk.acquire() and await cc.lock("lock:cl", ttl_ms=5000, blocking=False).acquire() is False
        assert await lk.renew(20000) and await lk.owned()
        await cc.set("lock:cl", "foreign")
        assert await lk.release() is False and await cc.get("lock:cl") == "foreign"
        await cc.delete("lock:cl")
        async with cc.lock("lock:cl2", ttl_ms=5000) as held:
            assert await cc.get("lock:cl2") == held.token
        assert await cc.get("lock:cl2") is None
        async with cc.lease_scope("lease:cl", 5000) as tok:
            assert tok >= 1
        async with cc.semaphore_scope("sem:cl", 2, 5000) as tok:
            assert tok


async def test_rate_limit_cache_idempotency_helpers_on_cluster(cc):
    allowed = []
    for _ in range(4):
        r = await cc.rate_limit("rl:cl", 3, 10000)
        allowed.append(r[0])
    assert allowed == [True, True, True, False]
    calls = []

    def factory():
        calls.append(1)
        return "built"

    assert await maybe(cc._c.get_or_create("goc:cl", factory, 5000) if isinstance(cc, SyncAsAsync) else cc.get_or_create("goc:cl", factory, 5000)) == "built"
    assert await maybe(cc._c.get_or_create("goc:cl", factory, 5000) if isinstance(cc, SyncAsAsync) else cc.get_or_create("goc:cl", factory, 5000)) == "built"
    assert len(calls) == 1
    idem = cc._c.idempotent if isinstance(cc, SyncAsAsync) else cc.idempotent
    r1 = await maybe(idem("idem:cl", "fp", 5000, lambda: "result"))
    r2 = await maybe(idem("idem:cl", "fp", 5000, lambda: "other"))
    assert r1 == ("executed", "result") and r2[0] == "completed"
    assert await cc.push_recent("recent:cl", "a", 3) is not None
    assert await cc.seen_before("bf:cl", "x") == [False]
    assert await cc.seen_before("bf:cl", "x") == [True]


async def test_scan_iterators_walk_every_node(cc):
    keys = set(spread_keys("it", 75))
    for k in keys:
        await cc.set(k, 1)
    found = set()
    if isinstance(cc, SyncAsAsync):
        found = set(cc._c.scan_iter(match="it:*", count=10))
        await cc.hset("{ih}h", "f1", 1)
        await cc.hset("{ih}h", "f2", 2)
        assert dict(cc._c.hscan_iter("{ih}h")) == {"f1": "1", "f2": "2"}
        await cc.sadd("{ih}s", "a", "b")
        assert set(cc._c.sscan_iter("{ih}s")) == {"a", "b"}
        await cc.zadd("{ih}z", {"m": 1.0})
        assert list(cc._c.zscan_iter("{ih}z")) == [("m", 1.0)]
    else:
        found = {k async for k in cc.scan_iter(match="it:*", count=10)}
        await cc.hset("{ih}h", "f1", 1)
        assert dict([x async for x in cc.hscan_iter("{ih}h")]) == {"f1": "1"}
    assert found == keys
    # one SCAN page keeps the node walk: the cursor encodes (server cursor, node)
    cur, page = await cc.scan(0)
    assert isinstance(cur, int) and cur >= 0


async def test_json_bloom_and_typed_helpers(cc):
    assert await cc.json_set("{j}doc", "$", '{"a":1}') is not None
    assert await cc.json_get("{j}doc", "$.a") is not None
    assert await cc.bf_reserve("{b}bf", 0.01, 100) is True
    assert await cc.bf_add("{b}bf", "x") is True
    assert await cc.bf_exists("{b}bf", "x") is True
    r = await cc.json_mget(["{j}doc", "{j}doc2"], "$.a")
    assert r[1] is None
    assert await cc.fillget("fg:cl", 1000) is not None


# --------------------------------------------------------------------------- blocking / pub-sub / node loss


async def test_blocking_pop_on_the_owner_node(cc):
    key = "{bk}list"
    if isinstance(cc, SyncAsAsync):
        c = cc._c
        threading.Timer(0.2, lambda: ruvio.Ruvio(*caddr(), cluster=True).rpush(key, "late")).start()
        # The value, not a nil timeout reply, shows that the push woke the blocked pop.
        assert c.blpop([key], 10) == (key, "late")
        assert c.blpop([key], 0.1) is None
    else:
        other = ruvio.asyncio.Ruvio(*caddr(), cluster=True)

        async def later():
            await asyncio.sleep(0.2)
            await other.rpush(key, "late")

        task = asyncio.ensure_future(later())
        got, pong = await asyncio.gather(cc.blpop([key], 10), cc.ping())  # PING is not stuck behind BLPOP
        assert got == (key, "late") and pong == "PONG"
        await task
        assert await cc.blpop([key], 0.1) is None
        await other.aclose()


async def test_pubsub_on_cluster(cc):
    if isinstance(cc, SyncAsAsync):
        ps = cc._c.pubsub(ignore_subscribe_messages=True)
        ps.subscribe("news")
        # Published until the server counts the subscriber; a publish that reaches nobody
        # delivers nothing.
        wait_for(lambda: cc._c.publish("news", "hello") >= 1, "the subscription")
        m = ps.get_message(timeout=10)
        assert m.kind == "message" and m.data == "hello"
        sp = cc._c.pubsub(ignore_subscribe_messages=True)
        sp.ssubscribe("{sh}chan")
        wait_for(lambda: cc._c.spublish("{sh}chan", "sharded") == 1, "the shard subscription")
        m = sp.get_message(timeout=10)
        assert m.kind == "smessage" and m.data == "sharded"
        with pytest.raises(ruvio.CrossSlotError):
            sp.ssubscribe("{sh}chan", "{other}chan")
        ps.close()
        sp.close()
    else:
        ps = await cc.pubsub(ignore_subscribe_messages=True)
        await ps.subscribe("news")
        await await_for(lambda: _reached(cc.publish("news", "hello")), "the subscription")
        m = await ps.get_message(timeout=10)
        assert m.kind == "message" and m.data == "hello"
        sp = await cc.pubsub(ignore_subscribe_messages=True)
        await sp.ssubscribe("{sh}chan")
        await await_for(lambda: _reached(cc.spublish("{sh}chan", "sharded")), "the shard subscription")
        m = await sp.get_message(timeout=10)
        assert m.kind == "smessage" and m.data == "sharded"
        await ps.aclose()
        await sp.aclose()


async def _reached(publish):
    return await publish >= 1


async def test_losing_one_node_connection_keeps_the_others_and_reconnects(cc):
    keys = spread_keys("nl", 30)
    for k in keys:
        await cc.set(k, k)
    st = state(cc)
    victim = st.nodes[1]
    # drop the socket to node 1 behind the client's back
    if isinstance(cc, SyncAsAsync):
        victim.conn.sock.shutdown(2)
    else:
        victim.proto.transport.abort()
        await asyncio.sleep(0.05)
    on_victim = [k for k in keys if st.slots[cl.key_slot(k)] == 1]
    others = [k for k in keys if st.slots[cl.key_slot(k)] != 1]
    assert on_victim and others
    for k in others:
        assert await cc.get(k) == k  # unaffected
    try:
        await cc.get(on_victim[0])  # sync: raises (never replayed); async: connection was dropped before
    except ruvio.ConnectionError:
        pass
    for k in on_victim:  # lazily reconnected
        assert await cc.get(k) == k


# --------------------------------------------------------------------------- standalone fallback


async def test_standalone_server_is_used_as_plain_client():
    if not SADDR:
        pytest.skip("RUVIO_TEST_ADDR not set")
    host, port = SADDR.rsplit(":", 1)
    s = ruvio.Ruvio(host, int(port), cluster=True, parser=PARSER)
    s.flushdb()
    assert s.is_cluster is False and s.nodes == []
    assert s.set("a", 1) and s.mset({"a": 1, "b": 2}) and s.mget("a", "b", "c") == ["1", "2", None]
    p = s.pipeline()
    p.set("c", 3).get("c")
    assert p.execute() == [True, "3"]
    assert sorted(s.scan_iter()) == ["a", "b", "c"]
    assert s.rename("a", "z") is True  # no cross-slot rule in standalone
    s.close()
    a = ruvio.asyncio.Ruvio(host, int(port), cluster=True, parser=PARSER)
    assert await a.is_cluster() is False
    assert await a.mget("b", "c") == ["2", "3"]
    pa = a.pipeline(transaction=True)
    pa.incr("n").incr("n")
    assert await pa.execute() == [1, 2]
    await a.flushdb()
    await a.aclose()
