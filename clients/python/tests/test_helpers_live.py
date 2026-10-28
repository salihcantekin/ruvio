# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Live tests for helpers, pub/sub, RESP3, multiplexing and reconnect (sync and asyncio)."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid

import pytest

import ruvio
import ruvio.asyncio
from conftest import await_for, wait_for


def _u() -> str:
    return uuid.uuid4().hex[:8]


# ----------------------------------------------------------------------------- sync helpers
def test_sync_lock(sync_client):
    r = sync_client
    lk = r.lock("lock:a", ttl_ms=5000)
    assert lk.acquire() is True
    other = r.lock("lock:a", ttl_ms=5000, blocking=False)
    assert other.acquire() is False
    assert other.acquire(blocking=True, timeout=0.1) is False
    assert lk.renew(20000) is True and r.pttl("lock:a") > 10000
    assert lk.owned() is True
    # someone else's lock must never be released by us
    r.set("lock:a", "foreign")
    assert lk.renew() is False
    assert lk.release() is False
    assert r.get("lock:a") == "foreign"
    r.delete("lock:a")
    with r.lock("lock:b", ttl_ms=5000) as held:
        assert r.get("lock:b") == held.token
        with pytest.raises(ruvio.LockError):
            with r.lock("lock:b", ttl_ms=5000, blocking=False):
                pass
    assert r.get("lock:b") is None


def test_sync_lease_semaphore_scopes(sync_client):
    r = sync_client
    with r.lease_scope("lease:x", 5000) as token:
        assert isinstance(token, int)
        with pytest.raises(ruvio.LockError):
            with r.lease_scope("lease:x", 5000):
                pass
    with r.lease_scope("lease:x", 5000) as token2:  # released on exit, so available again
        assert token2 > token
    with r.semaphore_scope("sem:x", 2, 5000) as a, r.semaphore_scope("sem:x", 2, 5000) as b:
        assert a != b
        with pytest.raises(ruvio.LockError):
            with r.semaphore_scope("sem:x", 2, 5000):
                pass
    with r.semaphore_scope("sem:x", 1, 5000):
        pass


def test_sync_get_or_create_stampede(sync_client):
    key = "goc:" + _u()
    calls = []
    results = []

    def factory():
        calls.append(1)
        time.sleep(0.1)
        return "computed"

    def worker():
        c = sync_client.dedicated()
        results.append(c.get_or_create(key, factory, ttl_ms=10000, fill_ms=5000))
        c.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == ["computed"] * 8
    assert len(calls) == 1
    assert sync_client.get_or_create(key, factory, ttl_ms=10000) == "computed"
    assert len(calls) == 1


def test_sync_idempotent_and_rate_limit(sync_client):
    r = sync_client
    key = "idem:" + _u()
    runs = []
    assert r.idempotent(key, "fp", 10000, lambda: runs.append(1) or "done") == ("executed", "done")
    assert r.idempotent(key, "fp", 10000, lambda: runs.append(1) or "again") == ("completed", "done")
    assert r.idempotent(key, "other", 10000, lambda: "x")[0] == "conflict"
    assert runs == [1]

    def boom():
        raise RuntimeError("boom")

    # abort_on_error frees the key after a failing fn; without it the key stays pending
    with pytest.raises(RuntimeError):
        r.idempotent(key + ":a", "fp", 10000, boom, abort_on_error=True)
    assert r.idempotent(key + ":a", "fp", 10000, lambda: "again") == ("executed", "again")
    with pytest.raises(RuntimeError):
        r.idempotent(key + ":k", "fp", 10000, boom)
    assert r.idempotent(key + ":k", "fp", 10000, lambda: "x") == ("pending", None)
    assert [r.rate_limit("rl:" + key, 3, 10000) for _ in range(4)] == [(True, 2), (True, 1), (True, 0), (False, 0)]


def test_sync_pubsub(sync_client):
    ps = sync_client.pubsub()
    ps.subscribe("news")
    m = ps.get_message(timeout=10)
    assert m.kind == "subscribe" and m.channel == "news" and m.data == 1
    ps.psubscribe("ev.*")
    assert ps.get_message(timeout=10).kind == "psubscribe"
    ps.ssubscribe("shard")
    assert ps.get_message(timeout=10).kind == "ssubscribe"
    assert sync_client.publish("news", "hello") == 1
    assert ps.get_message(timeout=10) == ruvio.Message("message", "news", "hello")
    assert sync_client.publish("ev.x", "p") == 1
    assert ps.get_message(timeout=10) == ruvio.Message("pmessage", "ev.x", "p", "ev.*")
    assert sync_client.spublish("shard", "s") == 1
    m = ps.get_message(timeout=10)
    assert m.kind == "smessage" and m.data == "s"
    ps.ping()
    assert ps.get_message(timeout=10).kind == "pong"
    assert ps.get_message(timeout=0.05) is None
    ps.unsubscribe("news")
    assert ps.get_message(timeout=10).kind == "unsubscribe"
    ps.punsubscribe()
    assert ps.get_message(timeout=10).kind == "punsubscribe"
    ps.sunsubscribe("shard")
    assert ps.get_message(timeout=10).kind == "sunsubscribe"
    ps.close()
    # resubscribe after reconnect
    ps2 = sync_client.pubsub(ignore_subscribe_messages=True)
    ps2.subscribe("again")
    ps2.get_message(timeout=0.2)
    ps2._conn.close()  # simulate a dropped socket
    ps2.get_message(timeout=0.2)  # reopens and re-sends SUBSCRIBE
    # Published until the server counts the resubscribed socket; a publish that reaches nobody
    # delivers nothing.
    wait_for(lambda: sync_client.publish("again", "x") == 1, "the resubscribe")
    assert ps2.get_message(timeout=10).data == "x"
    ps2.close()


def test_sync_resp3_and_tracking(sync_client):
    pushes = []
    host, port = sync_client._conn.opts.host, sync_client._conn.opts.port
    r = ruvio.Ruvio(host, port, db=11, on_push=pushes.append)
    assert r.hello(3)["proto"] == 3
    r.hset("h3", mapping={"a": "1"})
    assert r.hgetall("h3") == {"a": "1"}  # RESP3 map
    r.zadd("z3", {"m": 1.5})
    assert r.zrange("z3", 0, -1, withscores=True) == [("m", 1.5)]
    assert r.zscore("z3", "m") == 1.5  # RESP3 double
    assert r.client_tracking(True) is True
    r.set("tracked", "1")
    r.get("tracked")
    sync_client.set("tracked", "2")  # another connection invalidates
    # Reading a reply also drains a push that came before it.
    wait_for(lambda: r.ping() == "PONG" and any("tracked" in str(p) for p in pushes), "the invalidate push")
    assert r.client_tracking(False) is True
    r.close()


def test_sync_reconnect_and_timeout(sync_client):
    r = sync_client
    r.set("rc", "1")
    victim = r.client_id()
    killer = r.dedicated()
    assert killer.client_kill(id=victim) == 1
    with pytest.raises(ruvio.ConnectionError):
        r.get("rc")
    assert r.get("rc") == "1"  # reopened, SELECT re-applied
    killer.close()
    host, port = r._conn.opts.host, r._conn.opts.port
    slow = ruvio.Ruvio(host, port, db=11, socket_timeout=0.05)
    # A blocking command waits its own timeout on top of socket_timeout.
    assert slow.blpop("never", 0.3) is None
    assert slow.ping() == "PONG"
    slow.close()
    with pytest.raises(ruvio.ConnectionError):
        ruvio.Ruvio("127.0.0.1", 1, connect_timeout=0.5).ping()


def test_sync_transaction_watch(sync_client):
    r = sync_client
    r.set("bal", "10")

    def body(pipe):
        bal = int(r.get("bal"))
        pipe.set("bal", bal - 3)

    assert r.transaction(body, "bal") == [True]
    assert r.get("bal") == "7"
    with pytest.raises(ruvio.WatchError):
        r.watch("bal")
        r.dedicated().set("bal", "0")
        p = r.pipeline(transaction=True)
        p.set("bal", "1")
        p.execute()
    # a command refused at queue time makes EXEC answer EXECABORT and run nothing
    p = r.pipeline(transaction=True)
    p.set("x", "1").execute_command("NOSUCHCOMMAND").get("x")
    with pytest.raises(ruvio.ExecAbortError):
        p.execute()
    assert r.get("x") is None


def test_sync_from_url_and_raw(sync_client):
    host, port = sync_client._conn.opts.host, sync_client._conn.opts.port
    r = ruvio.Ruvio.from_url(f"ruvio://{host}:{port}/11", client_name="py-url")
    assert r.client_getname() == "py-url"
    r.set("raw", "é")
    assert r.raw.get("raw") == "é".encode()
    assert r.raw.hgetall("nope") == {}
    r.close()
    assert ruvio.parse_url("redis://u:p%40ss@h:1/3") == {"host": "h", "port": 1, "db": 3, "username": "u", "password": "p@ss"}


def test_sync_large_values(sync_client):
    big = "x" * (3 * 1024 * 1024)
    sync_client.set("big", big)
    assert sync_client.get("big") == big
    items = [f"item-{i}" * 10 for i in range(20000)]
    sync_client.rpush("biglist", *items)
    assert sync_client.lrange("biglist", 0, -1) == items
    p = sync_client.pipeline()
    for i in range(2000):
        p.set(f"pk{i}", i)
    for i in range(2000):
        p.get(f"pk{i}")
    res = p.execute()
    assert res[2000:] == [str(i) for i in range(2000)]


# ----------------------------------------------------------------------------- async helpers
async def test_async_concurrency_order(async_client):
    r = async_client
    await r.set("n", "0")
    keys = [f"k{i}" for i in range(500)]
    await asyncio.gather(*[r.set(k, k) for k in keys])
    got = await asyncio.gather(*[r.get(k) for k in keys])
    assert got == keys
    incs = await asyncio.gather(*[r.incr("n") for _ in range(300)])
    assert sorted(incs) == list(range(1, 301))
    # mixed types and errors interleaved: each caller gets its own reply
    await r.lpush("lst", "x")
    results = await asyncio.gather(r.get("k1"), r.get("lst"), r.incr("n"), return_exceptions=True)
    assert results[0] == "k1" and isinstance(results[1], ruvio.WrongTypeError) and results[2] == 301
    # pipelines share the connection with concurrent single commands
    p = r.pipeline()
    for i in range(100):
        p.get(f"k{i}")
    res, single = await asyncio.gather(p.execute(), r.get("k499"))
    assert res == keys[:100] and single == "k499"


async def test_async_cancel_keeps_stream_in_sync(async_client):
    r = async_client
    await r.set("a", "1")
    t = asyncio.ensure_future(r.blpop("never", 0.3))
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert await r.get("a") == "1"  # the BLPOP reply (nil) was dropped, not delivered to GET


async def test_async_lock_and_scopes(async_client):
    r = async_client
    async with r.lock("alock", ttl_ms=5000) as lk:
        assert await r.get("alock") == lk.token
        assert await lk.renew(10000) is True
        assert await lk.owned() is True
        other = r.lock("alock", 5000, blocking=False)
        assert await other.acquire() is False
        with pytest.raises(ruvio.LockError):
            async with r.lock("alock", 5000, blocking=True, timeout=0.1):
                pass
    assert await r.get("alock") is None
    lk = r.lock("alock2", 5000)
    assert await lk.acquire()
    await r.set("alock2", "foreign")
    assert await lk.release() is False and await r.get("alock2") == "foreign"
    async with r.lease_scope("alease", 5000) as tok:
        with pytest.raises(ruvio.LockError):
            async with r.lease_scope("alease", 5000):
                pass
    async with r.lease_scope("alease", 5000) as tok2:
        assert tok2 > tok
    async with r.semaphore_scope("asem", 1, 5000):
        with pytest.raises(ruvio.LockError):
            async with r.semaphore_scope("asem", 1, 5000):
                pass


async def test_async_get_or_create_stampede(async_client):
    r = async_client
    key = "agoc:" + _u()
    calls = []

    async def factory():
        calls.append(1)
        await asyncio.sleep(0.1)
        return "v"

    res = await asyncio.gather(*[r.get_or_create(key, factory, ttl_ms=10000, fill_ms=5000) for _ in range(30)])
    assert res == ["v"] * 30 and len(calls) == 1
    assert await r.get_or_create(key, lambda: "sync-factory", ttl_ms=1000) == "v"


async def test_async_idempotent(async_client):
    r = async_client
    key = "aidem:" + _u()

    async def work():
        return "ok"

    assert await r.idempotent(key, "fp", 10000, work) == ("executed", "ok")
    assert await r.idempotent(key, "fp", 10000, work) == ("completed", "ok")

    async def boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await r.idempotent(key + ":a", "fp", 10000, boom, abort_on_error=True)
    assert await r.idempotent(key + ":a", "fp", 10000, work) == ("executed", "ok")
    with pytest.raises(RuntimeError):
        await r.idempotent(key + ":k", "fp", 10000, boom)
    assert await r.idempotent(key + ":k", "fp", 10000, work) == ("pending", None)


async def test_async_pubsub(async_client):
    r = async_client
    ps = await r.pubsub()
    await ps.subscribe("ch")
    assert (await ps.get_message(timeout=10)).kind == "subscribe"
    await ps.psubscribe("p.*")
    assert (await ps.get_message(timeout=10)).kind == "psubscribe"
    await ps.ssubscribe("sch")
    assert (await ps.get_message(timeout=10)).kind == "ssubscribe"
    assert await r.publish("ch", "m1") == 1
    assert await ps.get_message(timeout=10) == ruvio.Message("message", "ch", "m1")
    assert await r.publish("p.1", "m2") == 1
    assert (await ps.get_message(timeout=10)).pattern == "p.*"
    assert await r.spublish("sch", "m3") == 1
    assert (await ps.get_message(timeout=10)).kind == "smessage"
    await ps.ping("x")
    assert (await ps.get_message(timeout=10)).kind == "pong"
    assert await ps.get_message(timeout=0.05) is None
    await ps.unsubscribe()
    assert (await ps.get_message(timeout=10)).kind == "unsubscribe"
    await ps.punsubscribe("p.*")
    assert (await ps.get_message(timeout=10)).kind == "punsubscribe"
    await ps.sunsubscribe()
    assert (await ps.get_message(timeout=10)).kind == "sunsubscribe"
    await ps.aclose()
    async with await r.pubsub(ignore_subscribe_messages=True) as ps2:
        await ps2.subscribe("it")
        # Published until the server counts the subscriber; a publish that reaches nobody delivers
        # nothing.
        await await_for(lambda: _published(r, "it", "a"), "the subscription")
        await r.publish("it", "b")
        got = []
        async for m in ps2.listen():
            got.append(m.data)
            if len(got) == 2:
                break
        assert got == ["a", "b"]


async def test_async_reconnect(async_client):
    r = async_client
    await r.set("rc", "1")
    victim = await r.client_id()
    killer = r.dedicated()
    assert await killer.client_kill(id=victim) == 1
    await killer.aclose()
    await await_for(lambda: r._st.proto is None or r._st.proto.closed, "the client to see the kill")
    assert await r.get("rc") == "1"  # lost connection noticed; next command reconnects
    assert await r.client_id() != victim
    bad = ruvio.asyncio.Ruvio("127.0.0.1", 1, connect_timeout=0.5)
    with pytest.raises(ruvio.ConnectionError):
        await bad.ping()


async def test_async_resp3_push(async_client):
    pushes = []
    o = async_client._st.opts
    r = ruvio.asyncio.Ruvio(o.host, o.port, db=11, on_push=pushes.append)
    assert (await r.hello(3))["proto"] == 3
    await r.hset("h3", mapping={"a": "1"})
    assert await r.hgetall("h3") == {"a": "1"}
    assert await r.client_tracking(True) is True
    await r.set("t3", "1")
    await r.get("t3")
    await async_client.set("t3", "2")
    await await_for(lambda: pushes, "the invalidate push")
    assert any("t3" in str(p) for p in pushes)
    assert await r.ping() == "PONG"
    await r.aclose()


async def test_async_transaction_helper(async_client):
    r = async_client
    await r.set("bal", "10")

    async def body(pipe):
        pipe.decrby("bal", 4)

    assert await r.transaction(body, "bal") == [6]
    p = r.pipeline(transaction=True)
    p.set("x", "1").execute_command("NOSUCHCOMMAND").get("x")
    with pytest.raises(ruvio.ExecAbortError):
        await p.execute()
    assert await r.get("x") is None


async def test_async_raw_and_url(async_client):
    o = async_client._st.opts
    r = ruvio.asyncio.Ruvio.from_url(f"ruvio://{o.host}:{o.port}/11")
    async with r:
        await r.set("b", b"\x00\x01")
        assert await r.raw.get("b") == b"\x00\x01"
        assert await r.raw.dump("b") is not None


async def test_async_large_and_pipeline(async_client):
    r = async_client
    big = "y" * (2 * 1024 * 1024)
    await r.set("big", big)
    gets = await asyncio.gather(*[r.get("big") for _ in range(3)])
    assert gets == [big] * 3
    items = [str(i) for i in range(20000)]
    await r.rpush("bl", *items)
    assert await r.lrange("bl", 0, -1) == items


def test_sync_select_survives_reconnect(sync_client):
    r = sync_client.dedicated()
    r.select(12)
    r.set("sel", "12")
    r._conn.close()  # drop the socket; the next command reconnects
    assert r.get("sel") == "12"
    r.delete("sel")
    r.close()


async def test_async_select_survives_reconnect(async_client):
    r = async_client.dedicated()
    await r.select(12)
    await r.set("sel", "12")
    r._st.proto.transport.close()
    await asyncio.sleep(0.02)
    assert await r.get("sel") == "12"
    await r.delete("sel")
    await r.aclose()


async def _published(client, channel, message):
    return await client.publish(channel, message) == 1
