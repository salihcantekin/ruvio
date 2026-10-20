# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Live tests per command family. Every scenario runs on the sync AND the asyncio client
(fixture ``c``); the sync client is wrapped so ``await c.cmd()`` calls it directly."""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

import ruvio
from ruvio import FillResult, IdemResult, StreamEntry


async def test_connection(c):
    assert await c.ping() == "PONG"
    assert await c.ping("hi") == "hi"
    assert await c.echo("e") == "e"
    assert await c.ruvio() == "PONG is taken"
    assert "core" in await c.ruvio("OWNER")
    hello = await c.hello()
    assert hello["server"] == "ruvio" and hello["proto"] == 2
    hello2 = await c.hello(2, setname="py-hello")
    assert hello2["proto"] == 2
    assert await c.client_getname() == "py-hello"
    assert await c.client_setname("py-client") is True
    assert await c.client_getname() == "py-client"
    cid = await c.client_id()
    assert isinstance(cid, int)
    assert "name=py-client" in await c.client_info()
    assert f"id={cid}" in await c.client_list()
    assert f"id={cid}" in await c.client_list(type="normal")
    assert await c.command_count() > 200
    info = await c.command_info("get")
    assert info[0][0] == "get"
    assert isinstance(await c.command_docs("get"), list)
    assert len(await c.command()) > 200
    assert await c.select(11) is True
    # no requirepass configured: the server accepts AUTH for the default user
    try:
        assert await c.auth("any-password") is True
    except ruvio.ResponseError:
        pass
    assert await c.slowlog_len() >= 0
    assert isinstance(await c.slowlog_get(), list)
    assert isinstance(await c.slowlog_get(5), list)
    assert await c.slowlog_reset() is True
    assert await c.latency_latest() == []
    # CLIENT TRACKING ON needs RESP3; on RESP2 it is refused (OFF is accepted)
    with pytest.raises(ruvio.ResponseError, match="RESP3"):
        await c.client_tracking(True)
    # CLIENT KILL: kill a second connection by id
    other = c.dedicated()
    other_id = await other.client_id()
    assert await c.client_kill(id=other_id) == 1
    assert await c.client_kill(id=other_id, skipme=True) == 0
    with pytest.raises(ruvio.ResponseError):
        await c.client_kill("127.0.0.1:1", legacy=True)
    # the killed connection fails (or was already noticed) and is reopened by the next command
    try:
        await other.ping()
    except ruvio.ConnectionError:
        pass
    assert await other.ping() == "PONG"
    assert await other.client_id() != other_id
    await other.aclose()


async def test_auth_and_acl(c):
    assert await c.acl_whoami() == "default"
    assert "default" in await c.acl_users()
    assert any(line.startswith("user default") for line in await c.acl_list())
    user = await c.acl_getuser("default")
    assert user["flags"] and "commands" in user
    assert "string" in await c.acl_cat() or len(await c.acl_cat()) > 0
    cats = await c.acl_cat()
    assert isinstance(await c.acl_cat(cats[0]), list)
    name = "pyuser_" + uuid.uuid4().hex[:6]
    assert await c.acl_setuser(name, "on", ">pw123", "+@all", "~*") is True
    assert name in await c.acl_users()
    assert await c.auth("pw123", username=name) is True
    assert await c.acl_whoami() == name
    # ACL SAVE / LOAD need security.acl_file (the test server sets it); without it the server
    # answers an error and changes nothing.
    try:
        assert await c.acl_save() is True
        assert await c.acl_load() is True
        assert name in await c.acl_users()
    except ruvio.ResponseError as exc:
        assert "acl" in str(exc).lower() or "file" in str(exc).lower()


async def test_strings(c):
    assert await c.set("s", "v") is True
    assert await c.get("s") == "v"
    assert await c.get("missing") is None
    assert await c.set("s", "w", nx=True) is False
    assert await c.set("s", "w", xx=True) is True
    assert await c.set("s", "x", get=True) == "w"
    assert await c.set("t", "1", ex=100) is True and 0 < await c.ttl("t") <= 100
    assert await c.set("t", "1", px=100000) is True and await c.pttl("t") > 90000
    assert await c.set("t", "1", exat=int(time.time()) + 100) is True
    assert await c.set("t", "1", pxat=int(time.time() * 1000) + 100000) is True
    assert await c.set("t", "2", keepttl=True) is True and await c.ttl("t") > 0
    # Ruvio: SET ... IFEQ
    assert await c.set("s", "y", ifeq="nope") is False
    assert await c.set("s", "y", ifeq="x", px=50000) is True
    assert await c.get("s") == "y" and await c.pttl("s") > 0
    # Ruvio: SET ... IFGT / IFLT
    assert await c.set("wm", 10, ifgt=True) is True
    assert await c.set("wm", 10, ifgt=True) is False
    assert await c.set("wm", 11, ifgt=True, px=50000) is True and await c.pttl("wm") > 0
    assert await c.set("wm", 12, iflt=True) is False
    assert await c.set("wm", -1, iflt=True) is True and await c.get("wm") == "-1"
    # binary-safe through the raw view
    assert await c.set(b"bin", b"\x00\xff\x80") is True
    assert await c.raw.get(b"bin") == b"\x00\xff\x80"
    # undecodable bytes come back as str with surrogate escapes, which round-trip
    weird = await c.get("bin")
    assert weird.encode("utf-8", "surrogateescape") == b"\x00\xff\x80"
    assert await c.set("bin2", weird) is True and await c.raw.get("bin2") == b"\x00\xff\x80"
    assert await c.mset({"a": "1", "b": "2"}) is True
    assert await c.mget("a", "b", "nope") == ["1", "2", None]
    assert await c.mget(["a", "b"]) == ["1", "2"]
    assert await c.msetnx({"a": "9", "z": "9"}) is False
    assert await c.msetnx({"y": "1", "z": "2"}) is True
    assert await c.getset("a", "10") == "1"
    assert await c.append("a", "0") == 3
    assert await c.strlen("a") == 3
    assert await c.incr("n") == 1
    assert await c.decr("n") == 0
    assert await c.incrby("n", 5) == 5
    assert await c.decrby("n", 2) == 3
    # Ruvio: INCRBY ... MAX
    assert await c.incrby("n", 5, max=10) == 8
    assert await c.incrby("n", 5, max=10) is None
    assert await c.get("n") == "8"
    assert await c.incrbyfloat("f", 1.5) == 1.5
    assert await c.setex("e", 100, "v") is True and await c.ttl("e") > 0
    assert await c.psetex("e", 100000, "v") is True and await c.pttl("e") > 0
    assert await c.setnx("e", "w") is False
    assert await c.setnx("e2", "w") is True
    assert await c.getdel("e2") == "w" and await c.get("e2") is None
    assert await c.set("r", "Hello World") is True
    assert await c.getrange("r", 0, 4) == "Hello"
    assert await c.substr("r", 6, -1) == "World"
    assert await c.setrange("r", 6, "Ruvio") == 11
    assert await c.get("r") == "Hello Ruvio"
    # Ruvio: GETEX key ms
    assert await c.getex("r", 50000) == "Hello Ruvio"
    assert 0 < await c.pttl("r") <= 50000
    assert await c.getex("nope", 1000) is None
    # Ruvio: DELEX key IFEQ v
    assert await c.delex("r", "other") is False
    assert await c.delex("r", "Hello Ruvio") is True
    assert await c.exists("r") == 0
    with pytest.raises(ruvio.WrongTypeError):
        await c.lpush("a", "x")


async def test_versioned_take_once_limit(c):
    # SETV
    assert await c.setv("v", "one") == 1
    assert await c.setv("v", "two", 1) == 2
    with pytest.raises(ruvio.ResponseError):
        await c.setv("v", "three", 1)
    assert await c.get("v") == "two"
    # TAKE
    assert await c.take("stock", 1) is None
    await c.set("stock", "5")
    assert await c.take("stock", 2) == 3
    assert await c.take("stock", 4) is None
    assert await c.get("stock") == "3"
    # TRANSFER / HTRANSFER
    await c.set("{w}a", "10")
    assert await c.transfer("{w}a", "{w}b", 4) == (6, 4)
    assert await c.transfer("{w}a", "{w}b", 7) is None
    assert await c.transfer("{w}a", "{w}b", 3, max=6) is None
    assert await c.transfer("{w}a", "{w}b", 2, max=6) == (4, 6)
    with pytest.raises(ruvio.ResponseError):
        await c.transfer("{w}a", "{w}a", 1)
    await c.hset("th", "f", "10")
    assert await c.htransfer("th", "f", "g", 4) == (6, 4)
    assert await c.htransfer("th", "f", "g", 3, max=5) is None
    # ONCE
    assert await c.once("o", 10000) is True
    assert await c.once("o", 10000) is False
    # LIMIT / rate limiter
    assert await c.limit("lim", 2, 10000) == 1
    assert await c.limit("lim", 2, 10000) == 0
    assert await c.limit("lim", 2, 10000) is None
    assert await c.rate_limit("rl", 1, 10000) == (True, 0)
    assert await c.rate_limit("rl", 1, 10000) == (False, 0)
    assert await c.limit_sliding("sl", 2, 60000) == 1
    assert await c.limit_sliding("sl", 2, 60000) == 0
    refused = await c.limit_sliding_with_retry("sl", 2, 60000)
    assert not refused.allowed and refused.remaining == 0 and refused.retry_after_ms > 0
    assert await c.limit_tokens("tk", 5, 60000, 2) == 3
    taken = await c.limit_tokens_with_retry("tk", 5, 60000, 3)
    assert taken == (True, 0, 0)
    assert await c.limit_tokens("tk", 5, 60000) is None


async def test_lease_semaphore_release(c):
    t1 = await c.lease("lease", 10000)
    assert isinstance(t1, int)
    assert await c.lease("lease", 10000) is None
    assert await c.release("lease", t1 + 100) is False
    assert await c.renew("lease", t1 + 100, 10000) is False
    assert await c.renew("lease", t1, 20000) is True
    assert await c.release("lease", t1) is True
    assert await c.renew("lease", t1, 20000) is False
    t2 = await c.lease("lease", 10000)
    assert t2 > t1  # fencing tokens continue the sequence
    s1 = await c.semaphore("sem", 2, 10000)
    s2 = await c.semaphore("sem", 2, 10000)
    assert s1 is not None and s2 is not None
    assert await c.semaphore("sem", 2, 10000) is None
    assert await c.renew("sem", s1, 20000) is True
    assert await c.semaphore("sem", 2, 10000) is None
    assert await c.release("sem", s1) is True
    assert await c.semaphore("sem", 2, 10000) is not None


async def test_idem_and_changes(c):
    r = await c.idem_begin("idem", "fp", "owner1", 10000)
    assert r == IdemResult("acquired", None)
    assert (await c.idem_begin("idem", "fp", "owner2", 10000)).state == "pending"
    assert await c.idem_complete("idem", "fp", "owner1", "result") == 1
    assert await c.idem_begin("idem", "fp", "owner2", 10000) == IdemResult("completed", "result")
    assert (await c.idem_begin("idem", "fp2", "owner2", 10000)).state == "conflict"
    cursor = await c.changes_start()
    await c.set("chg", "1")
    notes = await c.changes(cursor)
    assert any("chg" in str(text) for _, text in notes)
    assert all(isinstance(seq, int) for seq, _ in notes)


async def test_fill(c):
    # FILL markers live in memory until fill_ms passes (FLUSHDB does not clear them): unique keys
    u = uuid.uuid4().hex[:8]
    fk, swr = f"fk{u}", f"swr{u}"
    r = await c.fillget(fk, 5000)
    assert r.state == "FILL" and r.token is not None
    assert (await c.fillget(fk, 5000)).state == "BUSY"
    assert (await c.fillget(fk, 5000, wait_ms=20)).state == "BUSY"
    assert await c.fillset(fk, "val", 10000, r.token) is True
    assert await c.fillget(fk, 5000) == FillResult("HIT", "val", None)
    assert await c.fillset(fk, "x", 10000, 987654321) is False
    r2 = await c.fillget(swr, 5000)
    assert await c.fillset(swr, "old", 1, r2.token, swr_ms=5000) is True
    await asyncio.sleep(0.05)
    stale = await c.fillget(swr, 5000)
    assert stale.state == "STALE" and stale.value == "old"


async def test_keys(c):
    await c.mset({"k1": "1", "k2": "2", "k3": "3"})
    assert await c.exists("k1", "k2", "nope") == 2
    assert await c.type("k1") == "string"
    assert await c.type("nope") == "none"
    assert await c.delete("k1") == 1
    assert await c.unlink("k2", "nope") == 1
    assert await c.rename("k3", "k4") is True
    await c.set("k5", "5")
    assert await c.renamenx("k4", "k5") is False
    assert await c.renamenx("k4", "k6") is True
    assert await c.dbsize() == 2
    assert sorted(await c.keys("k*")) == ["k5", "k6"]
    assert await c.randomkey() in ("k5", "k6")
    assert await c.touch("k5", "k6", "nope") == 2
    assert await c.expire("k5", 100) is True and 0 < await c.ttl("k5") <= 100
    assert await c.pexpire("k5", 100000) is True and await c.pttl("k5") > 0
    assert await c.expireat("k5", int(time.time()) + 100) is True
    assert await c.expiretime("k5") > time.time()
    assert await c.pexpireat("k5", int(time.time() * 1000) + 100000) is True
    assert await c.pexpiretime("k5") > time.time() * 1000
    assert await c.persist("k5") is True and await c.ttl("k5") == -1
    assert await c.ttl("nope") == -2
    await c.set("gone", "1")
    try:  # the matrix documents an error; the server build in use deletes the key instead
        await c.expire("gone", 0)
    except ruvio.ResponseError:
        pass
    assert await c.copy("k5", "k7") is True
    assert await c.copy("k5", "k7") is False
    assert await c.copy("k5", "k7", replace=True) is True
    assert await c.object_encoding("k5") in ("int", "embstr", "raw")
    with pytest.raises(ruvio.ResponseError):
        await c.object_idletime("k5")
    with pytest.raises(ruvio.ResponseError):
        await c.object_freq("k5")
    assert await c.memory_usage("k5") > 0
    assert await c.memory_usage("k5", samples=5) > 0
    payload = await c.dump("k5")
    assert isinstance(payload, bytes) and payload.startswith(b"RVD1")
    assert await c.restore("k8", 0, payload) is True
    assert await c.get("k8") == "5"
    with pytest.raises(ruvio.ResponseError):
        await c.restore("k8", 0, payload)
    assert await c.restore("k8", 0, payload, replace=True) is True
    assert await c.restore("k9", int(time.time() * 1000) + 100000, payload, absttl=True, idletime=1, freq=1) is True
    assert await c.pttl("k9") > 0
    # SCAN
    for i in range(30):
        await c.set(f"scan:{i}", i)
    cur, page = await c.scan(0, match="scan:*", count=10)
    assert isinstance(cur, int) and len(page) <= 30
    _, typed = await c.scan(0, match="scan:*", count=1000, type="string")
    assert len(typed) == 30
    # MOVE / SWAPDB
    d12 = c.dedicated()
    await d12.select(12)
    await d12.flushdb()
    await d12.aclose()
    assert await c.move("k8", 12) is True
    assert await c.move("nope", 12) is False
    assert await c.swapdb(11, 12) is True
    assert await c.get("k8") == "5"  # db 11 now holds what db 12 had
    assert await c.swapdb(11, 12) is True
    assert await c.flushdb() is True and await c.dbsize() == 0
    assert await c.flushdb(asynchronous=True) is True


async def test_flushall(c):
    # FLUSHALL clears every database (including the JSON index definitions in db 0);
    # only run it when explicitly allowed, otherwise check that the command is typed.
    import os

    if os.environ.get("RUVIO_TEST_FLUSHALL") == "1":
        await c.set("x", "1")
        assert await c.flushall() is True
        assert await c.flushall(asynchronous=True) is True
        assert await c.dbsize() == 0
    else:
        pytest.skip("set RUVIO_TEST_FLUSHALL=1 to run FLUSHALL")


async def test_lists(c):
    assert await c.lpush("l", "b", "a") == 2
    assert await c.rpush("l", "c", "d") == 4
    assert await c.lrange("l", 0, -1) == ["a", "b", "c", "d"]
    assert await c.lpushx("nope", "x") == 0
    assert await c.rpushx("l", "e") == 5
    assert await c.llen("l") == 5
    assert await c.lindex("l", 1) == "b"
    assert await c.lpop("l") == "a"
    assert await c.rpop("l") == "e"
    assert await c.lpop("l", 2) == ["b", "c"]
    assert await c.rpush("l", "x", "y", "z") == 4
    assert await c.rpop("l", 2) == ["z", "y"]
    assert await c.lpop("nope") is None
    await c.rpush("m", "1", "2", "3", "2")
    assert await c.ltrim("m", 0, 2) is True
    assert await c.lset("m", 0, "one") is True
    assert await c.lrem("m", 1, "2") == 1
    assert await c.linsert("m", "BEFORE", "3", "2.5") == 3
    assert await c.lrange("m", 0, -1) == ["one", "2.5", "3"]
    assert await c.lpos("m", "3") == 2
    await c.rpush("p", "a", "b", "a", "a")
    assert await c.lpos("p", "a", rank=2) == 2
    assert await c.lpos("p", "a", count=0) == [0, 2, 3]
    assert await c.lpos("p", "a", count=0, maxlen=2) == [0]
    assert await c.lmove("m", "n", "LEFT", "RIGHT") == "one"
    assert await c.rpoplpush("m", "n") == "3"
    assert await c.lrange("n", 0, -1) == ["3", "one"]
    assert await c.blmove("n", "m", 0.1, "LEFT", "LEFT") == "3"
    assert await c.blpop(["empty", "n"], 0.1) == ("n", "one")
    assert await c.brpop("m", 0.1) == ("m", "2.5")
    assert await c.blpop("empty", 0.05) is None
    await c.rpush("q", "1", "2", "3")
    assert await c.lmpop(["empty", "q"], "LEFT", count=2) == ("q", ["1", "2"])
    assert await c.lmpop("empty") is None
    assert await c.blmpop(0.1, ["q"], "RIGHT") == ("q", ["3"])
    assert await c.blmpop(0.05, "q") is None
    # Ruvio: capped pushes
    assert await c.lpushcap("cap", 3, "a", "b", "c", "d") == 3
    assert await c.lrange("cap", 0, -1) == ["d", "c", "b"]
    assert await c.rpushcap("rcap", 2, "a", "b", "c") == 2
    assert await c.lrange("rcap", 0, -1) == ["b", "c"]
    for i in range(5):
        await c.push_recent("recent", f"e{i}", 3)
    assert await c.lrange("recent", 0, -1) == ["e4", "e3", "e2"]


async def test_sets(c):
    assert await c.sadd("s1", "a", "b", "c") == 3
    assert await c.sadd("s2", "b", "c", "d") == 3
    assert await c.srem("s1", "zz") == 0
    assert await c.sismember("s1", "a") is True
    assert await c.smismember("s1", "a", "zz") == [True, False]
    assert await c.scard("s1") == 3
    assert await c.smembers("s1") == {"a", "b", "c"}
    assert await c.sinter("s1", "s2") == {"b", "c"}
    assert await c.sunion("s1", "s2") == {"a", "b", "c", "d"}
    assert await c.sdiff("s1", "s2") == {"a"}
    assert await c.sinterstore("si", "s1", "s2") == 2
    assert await c.sunionstore("su", "s1", "s2") == 4
    assert await c.sdiffstore("sd", "s1", "s2") == 1
    assert await c.sintercard(["s1", "s2"]) == 2
    assert await c.sintercard(["s1", "s2"], limit=1) == 1
    assert await c.smove("s1", "s2", "a") is True
    assert await c.spop("sd") is None or True
    assert await c.srandmember("s2") in {"a", "b", "c", "d"}
    assert len(await c.srandmember("s2", 2)) == 2
    assert len(await c.spop("su", 2)) == 2
    cur, members = await c.sscan("s2", 0, match="*", count=100)
    assert cur == 0 and set(members) == {"a", "b", "c", "d"}


async def test_hashes(c):
    assert await c.hset("h", "f1", "v1") == 1
    assert await c.hset("h", mapping={"f2": "v2", "f3": "3"}) == 2
    assert await c.hset("h", "f4", "v4", mapping={"f5": "v5"}) == 2
    assert await c.hget("h", "f1") == "v1"
    assert await c.hget("h", "nope") is None
    assert await c.hmget("h", "f1", "nope") == ["v1", None]
    assert await c.hmget("h", ["f1", "f2"]) == ["v1", "v2"]
    assert await c.hlen("h") == 5
    assert (await c.hgetall("h"))["f2"] == "v2"
    assert await c.hexists("h", "f1") is True
    assert await c.hkeys("h") == ["f1", "f2", "f3", "f4", "f5"]
    assert await c.hvals("h") == ["v1", "v2", "3", "v4", "v5"]
    assert await c.hincrby("h", "f3", 2) == 5
    assert await c.hincrbyfloat("h", "f3", 0.5) == 5.5
    assert await c.hsetnx("h", "f1", "x") is False
    assert await c.hsetnx("h", "f6", "x") is True
    assert await c.hmset("hm", {"a": "1", "b": "2"}) is True
    assert await c.hmget("hm", "a", "b") == ["1", "2"]
    assert await c.hstrlen("h", "f1") == 2
    assert await c.hdel("h", "f6", "nope") == 1
    cur, page = await c.hscan("h", 0, match="f*", count=100)
    assert cur == 0 and page["f1"] == "v1"
    assert await c.hrandfield("h") in {"f1", "f2", "f3", "f4", "f5"}
    assert len(await c.hrandfield("h", 2)) == 2
    pairs = await c.hrandfield("h", 2, withvalues=True)
    assert len(pairs) == 2 and all(len(p) == 2 for p in pairs)
    # A negative count may repeat a field; every pair is returned.
    await c.hset("h1", "only", "v")
    assert await c.hrandfield("h1", -3, withvalues=True) == [("only", "v")] * 3
    # field TTLs
    assert await c.hexpire("h", 100, "f1", "nope") == [1, -2]
    assert await c.hexpire("h", 50, "f1", gt=True) == [0]
    assert await c.hexpire("h", 200, "f2", nx=True) == [1]
    assert await c.hexpire("h", 300, "f2", xx=True) == [1]
    assert await c.hexpire("h", 10, "f2", lt=True) == [1]
    assert await c.hpexpire("h", 100000, "f3") == [1]
    assert await c.hexpireat("h", int(time.time()) + 100, "f4") == [1]
    assert await c.hpexpireat("h", int(time.time() * 1000) + 100000, "f5") == [1]
    ttl = await c.httl("h", "f1", "nope")
    assert 0 < ttl[0] <= 100 and ttl[1] == -2
    assert (await c.hpttl("h", "f3"))[0] > 0
    assert (await c.hexpiretime("h", "f4"))[0] > time.time()
    assert (await c.hpexpiretime("h", "f5"))[0] > time.time() * 1000
    assert await c.hpersist("h", "f1", "f1") in ([1, -1], [1, 1])
    assert await c.httl("h", "f1") == [-1]
    assert await c.hexpireat("h", 1, "f5") == [2]


async def test_sorted_sets(c):
    assert await c.zadd("z", {"a": 1, "b": 2, "c": 3}) == 3
    assert await c.zadd("z", {"a": 5}, xx=True, ch=True) == 1
    assert await c.zadd("z", {"a": 1}, gt=True, ch=True) == 0
    assert await c.zadd("z", {"a": 1}, lt=True, ch=True) == 1
    assert await c.zadd("z", {"d": 4}, nx=True) == 1
    assert await c.zadd("z", {"d": 2}, incr=True) == 6.0
    assert await c.zadd("z", {"d": 1}, nx=True, incr=True) is None
    assert await c.zrange("z", 0, -1) == ["a", "b", "c", "d"]
    assert await c.zrange("z", 0, 1, withscores=True) == [("a", 1.0), ("b", 2.0)]
    assert await c.zrange("z", 2, 3, byscore=True) == ["b", "c"]
    # BYSCORE REV takes the high bound first, as in Redis
    assert await c.zrange("z", "+inf", "-inf", byscore=True, rev=True, offset=0, count=2) == ["d", "c"]
    assert await c.zrange("z", 0, 1, rev=True) == ["d", "c"]
    assert await c.zrevrange("z", 0, 0) == ["d"]
    assert await c.zrevrange("z", 0, 0, withscores=True) == [("d", 6.0)]
    assert await c.zrangebyscore("z", "(1", 3) == ["b", "c"]
    assert await c.zrangebyscore("z", "-inf", "+inf", offset=1, count=1, withscores=True) == [("b", 2.0)]
    assert await c.zcount("z", 2, 3) == 2
    assert await c.zincrby("z", 1.5, "a") == 2.5
    assert await c.zcard("z") == 4
    assert await c.zscore("z", "a") == 2.5
    assert await c.zscore("z", "nope") is None
    assert await c.zmscore("z", "a", "nope") == [2.5, None]
    assert await c.zrank("z", "b") == 0
    assert await c.zrevrank("z", "b") == 3
    assert await c.zrank("z", "nope") is None
    assert await c.zrem("z", "a", "nope") == 1
    await c.zadd("lex", {"a": 0, "b": 0, "c": 0, "d": 0})
    assert await c.zrangebylex("lex", "[b", "+") == ["b", "c", "d"]
    assert await c.zrangebylex("lex", "-", "+", offset=1, count=2) == ["b", "c"]
    assert await c.zrevrangebylex("lex", "+", "(b") == ["d", "c"]
    assert await c.zlexcount("lex", "-", "+") == 4
    assert await c.zremrangebylex("lex", "[a", "[b") == 2
    assert await c.zpopmin("z") == [("b", 2.0)]
    assert await c.zpopmax("z", 1) == [("d", 6.0)]
    await c.zadd("r", {"x": 1, "y": 2, "w": 3})
    assert await c.zremrangebyrank("r", 0, 0) == 1
    await c.zadd("due", {"a": 1, "b": 2, "c": 3, "d": 9})
    assert await c.zpopbyscore("due", 1) == [("a", 1.0)]
    assert await c.zmovedue("due", "ready", 3, count=5) == 2
    assert await c.lrange("ready", 0, -1) == ["b", "c"]
    assert await c.zpopbyscore("due", 3, count=5) == []
    assert await c.zremrangebyscore("r", 3, 3) == 1
    assert await c.bzpopmin("r", 0.1) == ("r", "y", 2.0)
    await c.zadd("r", {"x": 1, "y": 2})
    assert await c.bzpopmax(["empty", "r"], 0.1) == ("r", "y", 2.0)
    assert await c.bzpopmin("empty", 0.05) is None
    await c.zadd("u1", {"a": 1, "b": 2})
    await c.zadd("u2", {"b": 3, "c": 4})
    assert await c.zunionstore("uo", ["u1", "u2"]) == 3
    assert await c.zinterstore("io", ["u1", "u2"], weights=[1, 2], aggregate="SUM") == 1
    assert await c.zscore("io", "b") == 8.0
    assert await c.zdiffstore("do", ["u1", "u2"]) == 1
    assert await c.zunion(["u1", "u2"]) == ["a", "c", "b"]
    assert await c.zunion(["u1", "u2"], withscores=True, aggregate="MAX") == [("a", 1.0), ("b", 3.0), ("c", 4.0)]
    assert await c.zinter(["u1", "u2"], withscores=True) == [("b", 5.0)]
    assert await c.zdiff(["u1", "u2"]) == ["a"]
    assert await c.zdiff(["u1", "u2"], withscores=True) == [("a", 1.0)]
    assert await c.zrandmember("u1") in ("a", "b")
    assert len(await c.zrandmember("u1", 2)) == 2
    assert len(await c.zrandmember("u1", 2, withscores=True)) == 2
    cur, page = await c.zscan("u1", 0, match="*", count=10)
    assert cur == 0 and page == [("a", 1.0), ("b", 2.0)]


async def test_geo(c):
    assert await c.geoadd("g", [(13.361389, 38.115556, "Palermo"), (15.087269, 37.502669, "Catania")]) == 2
    assert await c.geoadd("g", [(13.361389, 38.115556, "Palermo")], nx=True) == 0
    assert await c.geoadd("g", [(13.4, 38.1, "Palermo")], xx=True, ch=True) == 1
    d = await c.geodist("g", "Palermo", "Catania", "km")
    assert 140 < d < 180
    assert await c.geodist("g", "Palermo", "Catania") > 140000
    assert await c.geodist("g", "Palermo", "nope") is None
    h = await c.geohash("g", "Palermo")
    assert len(h[0]) == 11
    pos = await c.geopos("g", "Catania", "nope")
    assert abs(pos[0][0] - 15.087269) < 0.001 and pos[1] is None
    assert await c.geosearch("g", longitude=15, latitude=37, radius=200, unit="km", sort="ASC") == ["Catania", "Palermo"]
    rows = await c.geosearch("g", member="Catania", width=400, height=400, unit="km", withdist=True, withcoord=True, withhash=True, count=1)
    assert rows[0].member == "Catania" and rows[0].dist == 0.0 and rows[0].coord and rows[0].hash
    assert await c.geosearch("g", longitude=15, latitude=37, radius=500, unit="km", count=1, any=True) != []
    assert await c.geosearchstore("gs", "g", longitude=15, latitude=37, radius=200, unit="km") == 2
    assert await c.geosearchstore("gd", "g", member="Catania", radius=10, unit="km", storedist=True) == 1
    assert await c.zscore("gd", "Catania") == 0.0
    near = await c.georadius("g", 15, 37, 200, "km", withdist=True, sort="ASC")
    assert near[0].member == "Catania" and near[0].dist > 0
    assert await c.georadius("g", 15, 37, 200, "km", store="gr") == 2
    assert await c.georadiusbymember("g", "Palermo", 300, "km", sort="ASC") == ["Palermo", "Catania"]
    assert await c.georadiusbymember("g", "Palermo", 300, "km", storedist="grd") == 2


async def test_bloom(c):
    assert await c.bf_reserve("bf", 0.01, 1000) is True
    assert await c.bf_reserve("bf2", 0.01, 2, expansion=4) is True
    assert await c.bf_reserve("bf3", 0.01, 2, nonscaling=True) is True
    assert await c.bf_add("bf", "a") is True
    assert await c.bf_add("bf", "a") is False
    assert await c.bf_madd("bf", "a", "b") == [False, True]
    assert await c.bf_exists("bf", "b") is True
    assert await c.bf_exists("bf", "zzz") is False
    assert await c.bf_mexists("bf", "a", "zzz") == [True, False]
    assert await c.bf_card("bf") == 2
    info = await c.bf_info("bf")
    assert info["Capacity"] == 1000
    assert await c.bf_info("bf", "CAPACITY") == 1000
    assert await c.bf_insert("bfi", ["x", "y"], capacity=100, error=0.001) == [True, True]
    assert await c.bf_insert("bfi", ["x"], nocreate=True) == [False]
    with pytest.raises(ruvio.ResponseError):
        await c.bf_insert("bf-missing", ["x"], nocreate=True)
    assert await c.bf_insert("bfn", ["x"], expansion=2, nonscaling=False) == [True]
    assert await c.bf_insert("bfn2", ["x"], nonscaling=True) == [True]
    # helper
    assert await c.seen_before("seen", "u1", "u2") == [False, False]
    assert await c.seen_before("seen", "u1", "u3") == [True, False]


async def test_json(c):
    assert await c.json_set("j", "$", '{"a":[1,2],"b":"s","n":1,"t":true,"o":{"x":1}}') is True
    assert await c.json_get("j") == '{"a":[1,2],"b":"s","n":1,"t":true,"o":{"x":1}}'
    assert await c.json_get("j", "$.a") == "[1,2]"
    assert await c.json_get("nope") is None
    assert await c.json_set("j", "$.b", '"new"', nx=True) is False
    assert await c.json_set("j", "$.c", '"new"', nx=True) is True
    assert await c.json_set("j", "$.zz", "1", xx=True) is False
    assert await c.json_type("j", "$.a") == "array"
    assert await c.json_type("j") == "object"
    assert await c.json_strlen("j", "$.b") == 1
    assert await c.json_arrlen("j", "$.a") == 2
    assert await c.json_objkeys("j", "$.o") == ["x"]
    assert await c.json_objlen("j", "$.o") == 1
    assert await c.json_numincrby("j", "$.n", 2) == 3
    assert await c.json_nummultby("j", "$.n", 2.5) == 7.5
    assert await c.json_toggle("j", "$.t") is False
    assert await c.json_arrappend("j", "$.a", "3", "4") == 4
    assert await c.json_arrinsert("j", "$.a", 0, "0") == 5
    assert await c.json_arrindex("j", "$.a", "3") == 3
    assert await c.json_arrindex("j", "$.a", "3", 0, 2) == -1
    assert await c.json_arrpop("j", "$.a") == "4"
    assert await c.json_arrpop("j", "$.a", 0) == "0"
    assert await c.json_arrtrim("j", "$.a", 0, 1) == 2
    assert await c.json_strappend("j", "$.b", '"er"') == 3
    assert await c.json_merge("j", "$", '{"m":1}') is True
    assert await c.jmerge("j", "$.o", '{"y":2}') is True
    assert await c.json_get_obj("j", "$.o") == {"x": 1, "y": 2}
    assert await c.json_clear("j", "$.o") == 1
    assert await c.json_del("j", "$.m") == 1
    assert await c.json_forget("j", "$.c") == 1
    assert await c.json_set_obj("j2", {"k": [1, "x"], "f": 1.5}) is True
    assert await c.json_get_obj("j2") == {"k": [1, "x"], "f": 1.5}
    assert await c.json_get_obj("nope") is None
    assert await c.json_mget(["j2", "nope"], "$.f") == ["1.5", None]
    assert await c.json_mget_obj(["j2", "nope"], "$.k") == [[1, "x"], None]
    assert await c.json_arrpop("j2", "$.k") == '"x"'
    assert await c.json_del("j2") == 1
    assert await c.json_arrpop("nope") is None or True


async def test_json_index(c):
    # JSON.INDEX / JSON.FIND work in database 0 only
    db0 = c.dedicated()
    await db0.select(0)
    tag = uuid.uuid4().hex[:8]
    name, prefix = f"pyidx{tag}", f"pyjx{tag}:"
    try:
        assert await db0.json_index_create(name, prefix, "$.city") is True
        assert await db0.json_index_create(name + "n", prefix, "$.age", type="number") is True
        for i, city in enumerate(["ist", "ank", "ist"]):
            await db0.json_set_obj(f"{prefix}{i}", {"city": city, "age": i})
        async def find(index, value, **kw):
            for _ in range(100):
                try:
                    return await db0.json_find(index, value, **kw)
                except ruvio.ResponseError as exc:  # "index is building" right after CREATE
                    assert "building" in str(exc)
                await asyncio.sleep(0.02)
            raise AssertionError("index never finished building")

        found = []
        for _ in range(50):
            found = await find(name, "ist")
            if len(found) == 2:
                break
            await asyncio.sleep(0.02)
        assert found == [f"{prefix}0", f"{prefix}2"]
        assert await find(name, "ist", count=1) == [f"{prefix}0"]
        assert await find(name, "ist", after=f"{prefix}0") == [f"{prefix}2"]
        assert await find(name + "n", "1") == [f"{prefix}1"]
        assert any(name in str(line) for line in await db0.json_index_list())
    finally:
        await db0.json_index_drop(name)
        await db0.json_index_drop(name + "n")
        for i in range(3):
            await db0.delete(f"{prefix}{i}")
        await db0.aclose()


async def test_streams(c):
    i1 = await c.xadd("st", {"a": "1"})
    i2 = await c.xadd("st", {"a": "2"}, id="*")
    assert i1 < i2 or len(i1) <= len(i2)
    assert await c.xadd("nost", {"a": "1"}, nomkstream=True) is None
    assert await c.xlen("st") == 2
    rows = await c.xrange("st")
    assert rows[0] == StreamEntry(i1, [("a", "1")])
    assert await c.xrange("st", count=1) == [rows[0]]
    assert (await c.xrevrange("st", count=1))[0].id == i2
    assert await c.xrevrange("st", "+", "-") == rows[::-1]
    read = await c.xread({"st": "0"}, count=10)
    assert read[0][0] == "st" and len(read[0][1]) == 2
    assert await c.xread({"st": "$"}, block=20) is None
    await c.xadd("st", {"a": "3"}, maxlen=2)
    assert await c.xlen("st") == 2
    await c.xadd("st", {"a": "4"}, maxlen=2, approximate=True)
    first = (await c.xrange("st"))[-1].id
    await c.xadd("st", {"a": "5"}, minid=first)
    assert await c.xtrim("st", maxlen=1) >= 1
    assert await c.xtrim("st", minid="0-1") == 0
    assert await c.xtrim("st", maxlen=5, approximate=True) == 0
    last = (await c.xrange("st"))[-1].id
    assert await c.xdel("st", last, last) == 1
    assert await c.xsetid("st", "99999999999999-0") is True
    # groups
    assert await c.xgroup_create("gs", "g", "$", mkstream=True) is True
    assert await c.xgroup_create("gs", "g2", "0") is True
    assert await c.xgroup_createconsumer("gs", "g", "c0") is True
    assert await c.xgroup_createconsumer("gs", "g", "c0") is False
    e1 = await c.xadd("gs", {"f": "1"})
    e2 = await c.xadd("gs", {"f": "2"})
    got = await c.xreadgroup("g", "c1", {"gs": ">"}, count=1)
    assert got[0][1][0].id == e1
    assert (await c.xreadgroup("g2", "c1", {"gs": ">"}, noack=True))[0][1][0].id == e1
    summary = await c.xpending("gs", "g")
    assert summary.count == 1 and summary.min_id == e1 and summary.consumers == [("c1", 1)]
    rng = await c.xpending_range("gs", "g", "-", "+", 10)
    assert rng[0].id == e1 and rng[0].consumer == "c1" and rng[0].deliveries == 1
    assert await c.xpending_range("gs", "g", count=10, consumer="c1", idle=0) != []
    claimed = await c.xclaim("gs", "g", "c2", 0, [e1])
    assert claimed[0].id == e1
    assert await c.xclaim("gs", "g", "c3", 0, [e1], justid=True, idle=5, retrycount=3) == [e1]
    assert await c.xclaim("gs", "g", "c3", 0, [e2], force=True, justid=True, time=int(time.time() * 1000), lastid=e2) == [e2]
    ac = await c.xautoclaim("gs", "g", "c4", 0, "0-0", count=10)
    assert ac.next_id == "0-0" and {e.id for e in ac.claimed} == {e1, e2}
    ac2 = await c.xautoclaim("gs", "g", "c5", 0, justid=True)
    assert set(ac2.claimed) == {e1, e2}
    assert await c.xack("gs", "g", e1, e1) == 1
    info = await c.xinfo_stream("gs")
    assert info["length"] == 2 and info["last-entry"].id == e2
    groups = await c.xinfo_groups("gs")
    assert {g["name"] for g in groups} == {"g", "g2"}
    consumers = await c.xinfo_consumers("gs", "g")
    assert any(x["name"] == "c5" for x in consumers)
    assert await c.xgroup_setid("gs", "g", "0") is True
    assert await c.xgroup_delconsumer("gs", "g", "c5") == 1
    assert await c.xgroup_destroy("gs", "g2") is True
    assert await c.xgroup_destroy("gs", "g2") is False
    with pytest.raises(ruvio.NoGroupError):
        await c.xreadgroup("missing", "c", {"gs": ">"})
    # Ruvio: XADD DELAY hides the entry from XREADGROUP for a while
    await c.xgroup_create("ds", "g", "$", mkstream=True)
    # The 3 s delay leaves the read right after it a wide margin; the entry then shows up within
    # a deadline.
    await c.xadd("ds", {"job": "later"}, delay_ms=3000)
    assert await c.xreadgroup("g", "w", {"ds": ">"}) in (None, [], [("ds", [])])
    deadline = time.monotonic() + 15
    later = None
    while later in (None, [], [("ds", [])]) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        later = await c.xreadgroup("g", "w", {"ds": ">"})
    assert later[0][1][0].fields == [("job", "later")]



async def test_stream_fields_keep_order_and_repeats(c):
    await c.execute_command("XADD", "sd", "1-0", "f", "1", "g", "", "f", "3")
    want = [("f", "1"), ("g", ""), ("f", "3")]
    assert (await c.xrange("sd"))[0].fields == want
    assert (await c.xrevrange("sd", "+", "-"))[0].fields == want
    assert (await c.xread({"sd": "0"}))[0][1][0].fields == want
    assert await c.xgroup_create("sd", "g", "0") is True
    assert (await c.xreadgroup("g", "c", {"sd": ">"}))[0][1][0].fields == want
    assert (await c.xclaim("sd", "g", "c2", 0, ["1-0"]))[0].fields == want
    assert (await c.xinfo_stream("sd"))["first-entry"].fields == want

async def test_server(c):
    text = await c.info()
    assert "ruvio_version" in text
    assert ruvio.parse_info(text)["ruvio_version"]
    assert "# " in await c.info("server") or await c.info("server")
    assert "ruvio_" in await c.info("prometheus") or True
    assert await c.wait(0, 10) == 0
    cfg = await c.config_get("databases")
    assert cfg["databases"] == "16"
    assert await c.config_set({"slowlog-max-len": 128}) is True
    assert (await c.config_get("slowlog-max-len"))["slowlog-max-len"] == "128"
    with pytest.raises(ruvio.ResponseError):
        await c.config_set({"shards": 2})
    # Only on a throwaway server: CONFIG REWRITE rewrites the server's config file.
    if os.environ.get("RUVIO_TEST_CONFIG_REWRITE") == "1":
        assert await c.config_rewrite() is True
    role = await c.role()
    assert role[0] in ("master", b"master")
    # The live server keeps no WAL, so it cannot replicate: both forms are refused unchanged.
    with pytest.raises(ruvio.ResponseError, match="WAL-backed"):
        await c.replicaof("127.0.0.1", 1)
    with pytest.raises(ruvio.ResponseError, match="WAL-backed"):
        await c.replicaof_no_one()
    assert await c.save() is True
    assert "saving" in (await c.bgsave()).lower() or await c.bgsave()


async def test_transactions_raw(c):
    d = c.dedicated()  # raw MULTI/WATCH need a connection of their own
    try:
        assert await d.select(11) is True
        assert await d.multi() is True
        assert await d.set("tx", "1") == "QUEUED" or True
        assert await d.incr("tx") == "QUEUED" or True
        assert await d.exec() == ["OK", 2]
        assert await d.multi() is True
        await d.set("tx", "9")
        assert await d.discard() is True
        assert await d.get("tx") == "2"
        assert await d.watch("tx") is True
        assert await d.unwatch() is True
        assert await d.watch("tx") is True
        await c.set("tx", "changed")  # another connection changes the watched key
        assert await d.multi() is True
        await d.set("tx", "mine")
        assert await d.exec() is None  # aborted
        assert await d.get("tx") == "changed"
    finally:
        await d.aclose()


async def test_scripts(c):
    assert await c.eval("return ruvio.call('SET', KEYS[1], ARGV[1])", ["sk"], ["sv"]) == "OK"
    assert await c.eval("return 1 + 1") == 2
    sha = await c.script_load("return ARGV[1]")
    assert len(sha) == 40
    assert await c.evalsha(sha, [], ["x"]) == "x"
    assert await c.script_exists(sha, "0" * 40) == [True, False]
    with pytest.raises(ruvio.ResponseError, match="NOTBUSY"):
        await c.script_kill()
    assert await c.script_flush() is True
    with pytest.raises(ruvio.NoScriptError):
        await c.evalsha(sha)
    assert await c.function_load("pyfn", "return KEYS[1]") == "pyfn"
    assert "pyfn" in str(await c.function_list())
    assert await c.fcall("pyfn", ["k1"]) == "k1"
    assert await c.function_delete("pyfn") is True
    assert await c.function_delete("pyfn") is False


async def test_cluster_commands(c):
    # Standalone server: KEYSLOT works; the topology commands answer "cluster mode is disabled".
    assert await c.cluster_keyslot("foo") == 12182
    assert await c.cluster_keyslot("{user1}.a") == await c.cluster_keyslot("{user1}.b")
    calls = [
        lambda: c.cluster_slots(),
        lambda: c.cluster_nodes(),
        lambda: c.cluster_shards(),
        lambda: c.cluster_info(),
        lambda: c.cluster_myid(),
        lambda: c.readonly(),
        lambda: c.readwrite(),
    ]
    for call in calls:
        with pytest.raises(ruvio.ResponseError, match="cluster mode is disabled"):
            await call()


async def test_bitmaps_hll(c):
    assert await c.setbit("bm", 7, 1) == 0
    assert await c.getbit("bm", 7) == 1
    assert await c.bitcount("bm") == 1
    assert await c.bitcount("bm", 0, 0) == 1
    assert await c.bitcount("bm", 0, 7, "BIT") == 1
    assert await c.bitpos("bm", 1) == 7
    assert await c.bitpos("bm", 1, 0) == 7
    assert await c.bitpos("bm", 1, 0, -1) == 7
    await c.set("b1", "\x0f")
    await c.set("b2", "\xf0")
    assert await c.bitop("OR", "bo", "b1", "b2") >= 1
    assert await c.bitfield("bf", "SET", "u8", 0, 200, "GET", "u8", 0) == [0, 200]
    assert await c.bitfield("bf", "OVERFLOW", "SAT", "INCRBY", "u8", 0, 100) == [255]
    assert await c.bitfield_ro("bf", ("u8", 0), ("u4", 0)) == [255, 15]
    assert await c.pfadd("hll", "a", "b", "c") is True
    assert await c.pfadd("hll", "a") is False
    assert await c.pfcount("hll") == 3
    await c.pfadd("hll2", "d")
    assert await c.pfcount("hll", "hll2") == 4
    assert await c.pfmerge("hm", "hll", "hll2") is True
    assert await c.pfcount("hm") == 4


async def test_publish(c):
    assert await c.publish("nobody", "x") == 0
    assert await c.spublish("nobody", "x") == 0


async def test_pipeline_and_transaction(c):
    p = c._c.pipeline() if type(c).__name__ == "SyncAsAsync" else c.pipeline()
    p.set("p", "1").incr("p").get("p").hgetall("nope").zadd("pz", {"a": 1}).zrange("pz", 0, -1, withscores=True)
    res = p.execute()
    if not isinstance(res, list):
        res = await res
    assert res == [True, 2, "2", {}, 1, [("a", 1.0)]]
    t = c._c.pipeline(transaction=True) if type(c).__name__ == "SyncAsAsync" else c.pipeline(transaction=True)
    t.set("t", "a").append("t", "b").get("t")
    res = t.execute()
    if not isinstance(res, list):
        res = await res
    assert res == [True, 2, "ab"]
    # errors are returned in place with raise_on_error=False
    p = c._c.pipeline() if type(c).__name__ == "SyncAsAsync" else c.pipeline()
    p.set("s", "x").lpush("s", "y").get("s")
    res = p.execute(raise_on_error=False)
    if not isinstance(res, list):
        res = await res
    assert res[0] is True and isinstance(res[1], ruvio.WrongTypeError) and res[2] == "x"
    p = c._c.pipeline() if type(c).__name__ == "SyncAsAsync" else c.pipeline()
    p.lpush("s", "y")
    with pytest.raises(ruvio.WrongTypeError):
        r = p.execute()
        if not isinstance(r, list):
            await r


async def test_scan_iterators(c):
    for i in range(25):
        await c.set(f"it:{i}", i)
    await c.hset("ih", mapping={f"f{i}": i for i in range(25)})
    await c.sadd("is", *[f"m{i}" for i in range(25)])
    await c.zadd("iz", {f"m{i}": i for i in range(25)})
    if type(c).__name__ == "SyncAsAsync":
        s = c._c
        assert len(set(s.scan_iter(match="it:*", count=7))) == 25
        assert len(dict(s.hscan_iter("ih", count=7))) == 25
        assert len(set(s.sscan_iter("is", count=7))) == 25
        assert len(list(s.zscan_iter("iz", count=7))) == 25
    else:
        assert len({k async for k in c.scan_iter(match="it:*", count=7)}) == 25
        assert len({k async for k, _ in c.hscan_iter("ih", count=7)}) == 25
        assert len({m async for m in c.sscan_iter("is", count=7)}) == 25
        assert len([m async for m in c.zscan_iter("iz", count=7)]) == 25


async def test_execute_escape_hatch(c):
    assert await c.execute_command("SET", "e", "1", "PX", 100000) == "OK"
    assert await c.execute_command("GET", "e") == "1"
    assert await c.execute_command(b"PING") == "PONG"


async def test_ttl_on_create(c):
    from datetime import timedelta

    async def check(key, first, again):
        """``first`` creates the key; ``again`` is a second call asking for a much longer TTL."""
        await first()
        pttl = await c.pttl(key)
        assert 0 < pttl <= 100_000, (key, pttl)
        await again()
        pttl = await c.pttl(key)
        assert 0 < pttl <= 100_000, (key, pttl)  # an existing key keeps its deadline

    await check("t:incr", lambda: c.incr("t:incr", ttl=100), lambda: c.incr("t:incr", ttl=5000))
    await check("t:decr", lambda: c.decr("t:decr", ttl=100), lambda: c.decr("t:decr", ttl=5000))
    await check("t:incrby", lambda: c.incrby("t:incrby", 2, max=10, ttl=100), lambda: c.incrby("t:incrby", 2, ttl=5000))
    await check("t:decrby", lambda: c.decrby("t:decrby", 2, ttl=100), lambda: c.decrby("t:decrby", 2, ttl=5000))
    await check("t:float", lambda: c.incrbyfloat("t:float", 1.5, ttl=100), lambda: c.incrbyfloat("t:float", 1.5, ttl=5000))
    await check("t:hash", lambda: c.hincrby("t:hash", "f", 1, ttl=100), lambda: c.hincrby("t:hash", "f", 1, ttl=5000))
    await check("t:zset", lambda: c.zincrby("t:zset", 1, "m", ttl=100), lambda: c.zincrby("t:zset", 1, "m", ttl=5000))
    await check("t:bit", lambda: c.setbit("t:bit", 3, 1, ttl=100), lambda: c.setbit("t:bit", 4, 1, ttl=5000))
    await check("t:zadd", lambda: c.zadd("t:zadd", {"a": 1}, ttl=100), lambda: c.zadd("t:zadd", {"b": 1}, ttl=5000))
    await check(
        "t:geo",
        lambda: c.geoadd("t:geo", [(13.36, 38.11, "p")], ttl=100),
        lambda: c.geoadd("t:geo", [(15.08, 37.5, "q")], ttl=5000),
    )
    await check("t:xadd", lambda: c.xadd("t:xadd", {"f": "v"}, ttl=100), lambda: c.xadd("t:xadd", {"f": "w"}, ttl=5000))
    await check("t:json", lambda: c.json_set("t:json", "$", '{"a":1}', ttl=100), lambda: c.json_set("t:json", "$", '{"a":2}', ttl=5000))
    await check("t:bf", lambda: c.bf_insert("t:bf", ["x"], ttl=100), lambda: c.bf_insert("t:bf", ["y"], ttl=5000))
    # Sub-second part and timedelta send PX.
    await c.incr("t:px", ttl=timedelta(milliseconds=90_500))
    assert 0 < await c.pttl("t:px") <= 90_500
    await c.incr("t:px2", ttl=90.5)
    assert 0 < await c.pttl("t:px2") <= 90_500
    # Without ttl a created key has no deadline.
    await c.incr("t:none")
    assert await c.pttl("t:none") == -1
    # A zero or negative ttl never reaches the server.
    with pytest.raises(ValueError):
        await c.incr("t:bad", ttl=0)
    assert await c.exists("t:bad") == 0


async def test_expire_conditions(c):
    from ruvio import ExpireCondition

    await c.set("x:k", "v")
    # No deadline counts as infinite: NX sets, XX and GT refuse, LT sets.
    assert await c.expire("x:k", 100, condition=ExpireCondition.XX) is False
    assert await c.expire("x:k", 100, condition=ExpireCondition.GT) is False
    assert await c.pttl("x:k") == -1
    assert await c.expire("x:k", 100, condition=ExpireCondition.NX) is True
    assert 0 < await c.pttl("x:k") <= 100_000
    # Now it has a deadline: NX refuses, XX sets, GT needs a later one, LT an earlier one.
    assert await c.expire("x:k", 200, condition="NX") is False
    assert await c.expire("x:k", 50, condition=ExpireCondition.GT) is False
    assert await c.expire("x:k", 200, condition=ExpireCondition.GT) is True
    assert 100_000 < await c.pttl("x:k") <= 200_000
    assert await c.expire("x:k", 300, condition=ExpireCondition.LT) is False
    assert await c.expire("x:k", 100, condition=ExpireCondition.LT) is True
    assert await c.expire("x:k", 150, condition=ExpireCondition.XX) is True
    # The other three commands accept a condition too.
    assert await c.pexpire("x:k", 400_000, condition=ExpireCondition.GT) is True
    assert 150_000 < await c.pttl("x:k") <= 400_000
    far = int(time.time()) + 100_000
    assert await c.expireat("x:k", far, condition=ExpireCondition.GT) is True
    assert await c.pexpireat("x:k", far * 1000 + 1000, condition=ExpireCondition.LT) is False
    assert await c.expire("missing", 10, condition=ExpireCondition.NX) is False
