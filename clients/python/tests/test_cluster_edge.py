# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Cluster client edge cases against scripted fake nodes (sync and asyncio): discovery fallbacks,
refresh failures, malformed redirects, dead nodes in scatter/gather, SCAN walking, transaction
redirects, connection-wide state propagation, blocking commands and Pub/Sub node selection."""

from __future__ import annotations

import asyncio
import inspect
import itertools
import socket
import threading
import time

import pytest

import ruvio
import ruvio.asyncio
from conftest import SyncAsAsync
from fakecluster import DROP, OK, FakeCluster, FakeNode, array, bulk, err, integer
from ruvio import _cluster as cl


class XCluster(FakeCluster):
    """FakeCluster whose per-command behavior can be overridden: ``extra[NAME](cluster, node, args)``
    returns bytes (or DROP), or None to fall through to the default handling."""

    def __init__(self, ranges, extra=None):
        self.extra = extra or {}
        super().__init__(ranges)

    def _handle(self, node, args):
        f = self.extra.get(args[0].upper())
        if f is not None:
            r = f(self, node, args)
            if r is not None:
                return r
        return super()._handle(node, args)

    def guard(self, node, key):
        """-MOVED unless ``node`` owns ``key``."""
        owner = self.owner(cl.key_slot(key))
        return None if owner is node else err(f"MOVED {cl.key_slot(key)} {owner.addr}")


def key_in(lo, hi, prefix="k"):
    for i in itertools.count():
        k = f"{prefix}{i}"
        if lo <= cl.key_slot(k) <= hi:
            return k


def closed_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(params=["sync", "async"])
def kind(request):
    return request.param


@pytest.fixture
def make(kind):
    made = []

    def build(addr, **kw):
        host, port = addr.rsplit(":", 1)
        if kind == "sync":
            c = SyncAsAsync(ruvio.Ruvio(host, int(port), cluster=True, **kw))
        else:
            c = ruvio.asyncio.Ruvio(host, int(port), cluster=True, **kw)
        made.append(c)
        return c

    yield build


@pytest.fixture
def fc():
    made = []

    def build(ranges=((0, 8191), (8192, 16383)), extra=None):
        c = XCluster(list(ranges), extra)
        made.append(c)
        return c

    yield build
    for c in made:
        c.close()


def st(c):
    return c._c._cl if isinstance(c, SyncAsAsync) else c._cl


def opts_of(node):
    return node.conn.opts if hasattr(node, "conn") else node.opts


async def prop(c, name):
    """``is_cluster`` / ``nodes`` are properties on the sync client and coroutines on the asyncio one."""
    v = getattr(c, name)
    return await v() if callable(v) else v


async def maybe(x):
    return await x if inspect.isawaitable(x) else x


def pipe(c, **kw):
    return c._c.pipeline(**kw) if isinstance(c, SyncAsAsync) else c.pipeline(**kw)


# --------------------------------------------------------------------------- discovery fallbacks


async def test_single_node_map_is_a_standalone_fallback(make, fc):
    cluster = fc(ranges=[(0, 16383)])
    c = make(cluster.nodes[0].addr)
    assert await prop(c, "is_cluster") is False
    assert await prop(c, "nodes") == []
    assert await c.set("{t}a", "1") is True and await c.set("{t}b", "2") is True
    assert await c.mget("{t}a", "{t}b") == ["1", "2"]  # forwarded as one command, no splitting
    assert [x[1:] for x in cluster.nodes[0].commands("MGET")] == [["{t}a", "{t}b"]]
    await c.refresh_slots()  # nothing to refresh in standalone mode
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1
    await c.aclose()


async def test_garbage_cluster_slots_reply_falls_back_to_standalone(make, fc):
    for bad in (b"*0\r\n", b"*1\r\n*1\r\n$1\r\nx\r\n", b"+OK\r\n", b"*1\r\n*3\r\n:0\r\n:100\r\n*1\r\n$1\r\nh\r\n"):
        cluster = fc(extra={"CLUSTER": lambda cl_, n, a, bad=bad: bad})
        c = make(cluster.nodes[0].addr)
        assert await prop(c, "is_cluster") is False
        assert await c.ping() == "PONG"  # plain single-connection behavior from now on
        await c.aclose()


async def test_unreachable_seed_raises_connection_error(make):
    c = make(f"127.0.0.1:{closed_port()}")
    with pytest.raises(ruvio.ConnectionError):
        await c.get("k")
    with pytest.raises(ruvio.ConnectionError):
        await prop(c, "is_cluster")
    assert st(c).mode == 0  # still undiscovered: the next command tries again


async def test_refresh_slots_discovers_first_and_follows_topology_changes(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    await c.refresh_slots()  # mode 0: performs the discovery
    assert st(c).mode == 1 and len(cluster.nodes[0].commands("CLUSTER")) == 1
    assert await prop(c, "is_cluster") is True
    assert await prop(c, "nodes") == [("127.0.0.1", cluster.nodes[0].port), ("127.0.0.1", cluster.nodes[1].port)]
    k = key_in(8192, 12000)
    cluster.advertised = [(0, 12000), (12001, 16383)]
    await c.refresh_slots()
    assert st(c).slots[cl.key_slot(k)] == 0
    await c.aclose()


async def test_refresh_failures_keep_the_old_table(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    await c.ping()
    before = list(st(c).slots)
    for bad in (err("ERR cluster support disabled"), b"*0\r\n", b"+OK\r\n"):
        for n in cluster.nodes:
            n.extra = None
        cluster.extra["CLUSTER"] = lambda cl_, n, a, bad=bad: bad
        await c.refresh_slots()  # nobody gives a usable map: no exception, table untouched
        assert st(c).slots == before
    cluster.extra["CLUSTER"] = lambda cl_, n, a: DROP
    await c.refresh_slots()
    assert st(c).slots == before
    cluster.extra.clear()
    k = key_in(8192, 16383)
    assert await c.set(k, "v") is True  # the client is still usable
    await c.aclose()


async def test_refresh_skips_a_dead_seed_and_uses_another_node(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    await c.ping()
    seed_calls = []

    def slots(cl_, node, args):
        if node is cluster.nodes[0]:
            seed_calls.append(1)
            return DROP
        return None

    cluster.extra["CLUSTER"] = slots
    cluster.advertised = [(0, 12000), (12001, 16383)]
    await c.refresh_slots()
    assert seed_calls  # the seed was tried first and failed
    assert st(c).slots[10000] == 0 and st(c).slots[12001] == 1  # the table came from node1
    await c.aclose()


async def test_topology_that_drops_a_node_closes_its_connection(make, fc):
    cluster = fc(ranges=[(0, 5460), (5461, 10922), (10923, 16383)])
    c = make(cluster.nodes[0].addr)
    k2 = key_in(10923, 16383)
    assert await c.set(k2, "x") is True  # opens a connection to node2
    gone = cluster.nodes[2].port
    cluster.hidden = {2}  # CLUSTER SLOTS no longer lists node2 ...
    cluster.advertised = [(0, 8191), (8192, 16383), (0, 0)]
    cluster.owner_ranges = [(0, 8191), (8192, 16383), (0, 0)]  # ... and its slots belong to node1 now
    await c.refresh_slots()
    assert len(await prop(c, "nodes")) == 2
    assert gone not in {n.port for n in st(c).by_addr.values()}
    assert await c.set(k2, "y") is True  # served by the surviving owner
    await c.aclose()


# --------------------------------------------------------------------------- redirects and routing


async def test_malformed_moved_is_surfaced_not_followed(make, fc):
    cluster = fc(extra={"GET": lambda c_, n, a: err("MOVED not-a-slot")})
    c = make(cluster.nodes[0].addr)
    with pytest.raises(ruvio.MovedError, match="MOVED not-a-slot"):
        await c.get("k")
    cluster.extra["GET"] = lambda c_, n, a: err("MOVED 5")  # too few fields
    with pytest.raises(ruvio.MovedError):
        await c.get("k")
    cluster.extra["GET"] = lambda c_, n, a: err("MOVED 5 host-without-port")
    with pytest.raises(ruvio.MovedError):
        await c.get("k")
    await c.aclose()


def bounded(kind, make, addr):
    """A cluster client whose every command is bounded: socket timeout (sync) or ``wait_for`` (asyncio, below)."""
    return make(addr, **({"socket_timeout": 5} if kind == "sync" else {}))


async def within(coro, seconds=10):
    return await asyncio.wait_for(coro, seconds)


async def test_ask_sends_asking_then_the_command_and_leaves_the_slot_map_alone(kind, make, fc):
    k = key_in(8192, 16383)
    slot = cl.key_slot(k)

    def ask(cl_, node, args):
        return err(f"ASK {slot} {cl_.nodes[1].addr}") if node is cl_.nodes[0] else None

    cluster = fc(extra={"GET": ask})
    cluster.nodes[1].store[k] = "from-b"
    c = bounded(kind, make, cluster.nodes[0].addr)
    await within(c.ping())
    st(c).slots[slot] = 0  # node0 is the owner on the map
    assert await within(c.get(k)) == "from-b"
    assert cluster.nodes[1].commands() == [["ASKING"], ["GET", k]]
    assert len(cluster.nodes[0].commands("GET")) == 1
    assert st(c).slots[slot] == 0  # an ASK does not touch the map
    assert await within(c.get(k)) == "from-b"
    assert len(cluster.nodes[0].commands("GET")) == 2  # the next command goes to the owner again
    await c.aclose()


async def test_ask_to_a_node_not_in_the_map_opens_a_connection(kind, make, fc):
    k = key_in(8192, 16383)
    seen = []

    def stranger(node, args):
        seen.append(args)
        return bulk("asked") if args[0].upper() == "GET" else OK

    other = FakeNode(stranger)
    try:
        cluster = fc(extra={"GET": lambda cl_, node, args: err(f"ASK {cl.key_slot(k)} {other.addr}")})
        c = bounded(kind, make, cluster.nodes[0].addr)
        await within(c.ping())
        st(c).slots[cl.key_slot(k)] = 0
        assert await within(c.get(k)) == "asked"
        assert seen == [["ASKING"], ["GET", k]]
        await c.aclose()
    finally:
        other.close()


async def test_asking_error_reply_is_returned_instead_of_the_command_reply(kind, make, fc):
    k = key_in(8192, 16383)

    def handler(cl_, node, args):
        if node is cl_.nodes[0] and args[0] == "GET":
            return err(f"ASK {cl.key_slot(k)} {cl_.nodes[1].addr}")
        return err("ERR asking refused") if args[0] == "ASKING" else None

    cluster = fc(extra={"GET": handler, "ASKING": handler})
    c = bounded(kind, make, cluster.nodes[0].addr)
    await within(c.ping())
    st(c).slots[cl.key_slot(k)] = 0
    with pytest.raises(ruvio.ResponseError, match="asking refused"):
        await within(c.get(k))
    assert await within(c.ping()) in (True, "PONG")  # the connection is still in step
    await c.aclose()


async def test_tryagain_is_retried_after_a_short_sleep(kind, make, fc):
    k = key_in(8192, 16383)
    sent = []

    def tryagain(cl_, node, args):
        sent.append(node)
        return err("TRYAGAIN slot is being handed over") if len(sent) == 1 else None

    cluster = fc(extra={"GET": tryagain})
    cluster.nodes[1].store[k] = "ok"
    c = bounded(kind, make, cluster.nodes[0].addr)
    t0 = time.monotonic()
    assert await within(c.get(k)) == "ok"
    assert time.monotonic() - t0 >= 0.0045
    assert len(sent) == 2 and sent[0] is sent[1] is cluster.nodes[1]
    await c.aclose()


async def test_tryagain_eight_times_gives_the_error_after_seven_retries(kind, make, fc):
    k = key_in(8192, 16383)
    cluster = fc(extra={"GET": lambda cl_, node, args: err("TRYAGAIN slot is being handed over")})
    c = bounded(kind, make, cluster.nodes[0].addr)
    t0 = time.monotonic()
    with pytest.raises(ruvio.ResponseError, match="^TRYAGAIN") as info:
        await within(c.get(k))
    assert not isinstance(info.value, ruvio.MovedError)
    assert time.monotonic() - t0 >= 0.634  # 5 + 10 + 20 + 40 + 80 + 160 + 320 ms
    assert len(cluster.nodes[1].commands("GET")) == 8
    await c.aclose()


async def test_ask_and_moved_share_one_redirect_budget(kind, make, fc):
    k = key_in(8192, 16383)
    slot = cl.key_slot(k)
    script = []

    def scripted(cl_, node, args):
        script.append(node)
        n0, n1 = cl_.nodes
        steps = [f"ASK {slot} {n1.addr}", f"MOVED {slot} {n0.addr}", f"ASK {slot} {n1.addr}", f"ASK {slot} {n1.addr}"]
        return err(steps[len(script) - 1])

    cluster = fc(extra={"GET": scripted})
    c = bounded(kind, make, cluster.nodes[0].addr)
    await within(c.ping())
    st(c).slots[slot] = 0
    with pytest.raises(ruvio.MovedError, match="^ASK "):  # the 4th redirect goes to the caller
        await within(c.get(k))
    n0, n1 = cluster.nodes
    assert script == [n0, n1, n0, n1]
    await c.aclose()


async def test_redirect_to_an_unreachable_node_fails_with_connection_error(make, fc):
    dead = closed_port()
    k = key_in(8192, 16383)
    cluster = fc(extra={"GET": lambda c_, n, a: err(f"MOVED {cl.key_slot(a[1])} 127.0.0.1:{dead}")})
    c = make(cluster.nodes[0].addr)
    await c.set(k, "warm")  # opens the connection to the owner first (async: takes the warm path)
    with pytest.raises(ruvio.ConnectionError):
        await c.get(k)
    await c.aclose()


async def test_keyless_and_lowercase_commands(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    k = key_in(8192, 16383)
    assert await c.execute_command("set", k, "v") == "OK"  # lower case name: normalised for routing
    assert await c.execute_command("get", k) == "v"
    assert cluster.nodes[1].store[k] == "v" and not cluster.nodes[0].commands("SET")
    assert await c.execute_command("FOOBAR") == "OK"  # no key at all: goes to the seed
    assert cluster.nodes[0].commands("FOOBAR")
    assert await c.execute_command(b"get", k) == "v"
    await c.aclose()


async def test_slot_without_owner_goes_to_the_seed_and_learns_from_moved(make, fc):
    cluster = fc()
    cluster.advertised = [(0, 8191), (8192, 16000)]  # slots 16001.. have no owner in the map
    c = make(cluster.nodes[0].addr)
    k = key_in(16001, 16383)
    cluster.nodes[1].store[k] = "tail"
    assert await c.get(k) == "tail"
    assert len(cluster.nodes[0].commands("GET")) == 1  # unmapped -> seed -> MOVED -> node1
    assert await c.get(k) == "tail"
    assert len(cluster.nodes[0].commands("GET")) == 1  # the slot is mapped now
    await c.aclose()


async def test_blocking_command_follows_a_redirect(make, fc):
    k = key_in(8192, 16383)

    def blpop(cl_, node, args):
        g = cl_.guard(node, args[1])
        return g if g is not None else array([bulk(args[1]), bulk("popped")])

    cluster = fc(extra={"BLPOP": blpop})
    c = make(cluster.nodes[0].addr)
    await c.ping()
    st(c).slots[cl.key_slot(k)] = 0  # stale: node0 does not own it
    assert await c.blpop(k, 1) == (k, "popped")
    assert len(cluster.nodes[0].commands("BLPOP")) == 1 and len(cluster.nodes[1].commands("BLPOP")) == 1
    other = key_in(0, 8191)
    assert await c.blpop([other], 1) == (other, "popped")
    await c.aclose()


async def test_blocking_variants_use_the_owner(make, fc):
    extra = {
        "XREAD": lambda c_, n, a: array([]),
        "FILLGET": lambda c_, n, a: array([bulk("HIT"), bulk("v")]),
    }
    cluster = fc(extra=extra)
    c = make(cluster.nodes[0].addr)
    k = key_in(8192, 16383)
    assert await c.fillget(k, 1000, 50) == ("HIT", "v", None)
    assert [x for x in cluster.nodes[1].commands("FILLGET")][0][-2:] == ["WAIT", "50"]
    await c.execute_command("XREAD", "BLOCK", 10, "STREAMS", k, "$")
    assert cluster.nodes[1].commands("XREAD")
    await c.aclose()


async def test_concurrent_first_commands_open_one_connection_per_node(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    await c.ping()
    ks = [key_in(8192, 16383, f"c{i}-") for i in range(10)]
    res = await asyncio.gather(*[maybe(c.set(k, "v")) for k in ks])
    assert all(res)
    deadline = time.monotonic() + 1
    while cluster.nodes[1].connections < 1 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert cluster.nodes[1].connections == 1
    await c.aclose()


# --------------------------------------------------------------------------- dead nodes in fan-out and scatter


async def test_split_command_with_a_dead_node_raises_and_leaves_live_node_usable(make, fc):
    cluster = fc(extra={"MGET": lambda c_, n, a: DROP if n is c_.nodes[1] else None})
    c = make(cluster.nodes[0].addr)
    a, b = key_in(0, 8191), key_in(8192, 16383)
    await c.set(a, "A")
    with pytest.raises(ruvio.ConnectionError):
        await c.mget(a, b)
    assert await c.get(a) == "A"  # the healthy node's connection stayed aligned
    cluster.extra.clear()
    assert await c.mget(a, b) == ["A", None]  # and the dead one reconnects on demand
    await c.aclose()


async def test_split_command_reports_an_error_reply_from_one_node(make, fc):
    cluster = fc(extra={"MGET": lambda c_, n, a: err("ERR boom") if n is c_.nodes[1] else None})
    c = make(cluster.nodes[0].addr)
    a, b = key_in(0, 8191), key_in(8192, 16383)
    with pytest.raises(ruvio.ResponseError, match="boom"):
        await c.mget(a, b)
    await c.aclose()


async def test_split_redirect_failure_is_reported_as_the_commands_error(make, fc):
    # node0 says "moved to node1" for a key whose node1 handler then fails: the error reaches the caller
    k0, k1 = key_in(0, 8191), key_in(8192, 16383)

    def mget(cl_, node, args):
        if node is cl_.nodes[0] and args[1:] == [k0]:
            return err(f"MOVED {cl.key_slot(k0)} {cl_.nodes[1].addr}")
        if node is cl_.nodes[1] and args[1:] == [k0]:
            return err("ERR cannot serve that")
        return None

    cluster = fc(extra={"MGET": mget})
    c = make(cluster.nodes[0].addr)
    with pytest.raises(ruvio.ResponseError, match="cannot serve"):
        await c.mget(k0, k1)
    await c.aclose()


async def test_fan_out_errors(make, fc):
    cluster = fc(extra={"DBSIZE": lambda c_, n, a: integer(3 if n is c_.nodes[0] else 4)})
    c = make(cluster.nodes[0].addr)
    assert await c.dbsize() == 7
    cluster.extra["DBSIZE"] = lambda c_, n, a: err("ERR busy") if n is c_.nodes[1] else integer(1)
    with pytest.raises(ruvio.ResponseError, match="busy"):
        await c.dbsize()
    cluster.extra["DBSIZE"] = lambda c_, n, a: DROP if n is c_.nodes[1] else integer(1)
    with pytest.raises(ruvio.ConnectionError):
        await c.dbsize()
    cluster.extra["FLUSHDB"] = lambda c_, n, a: err("ERR readonly") if n is c_.nodes[0] else None
    with pytest.raises(ruvio.ResponseError, match="readonly"):
        await c.flushdb()
    await c.aclose()


async def test_pipeline_with_a_dead_node_raises_after_reading_the_live_one(make, fc):
    cluster = fc(extra={"GET": lambda c_, n, a: DROP if n is c_.nodes[1] and a[1].startswith("dead") else None})
    c = make(cluster.nodes[0].addr)
    a = key_in(0, 8191)
    b = key_in(8192, 16383, "dead")
    p = pipe(c)
    p.set(a, "1").get(b)
    with pytest.raises(ruvio.ConnectionError):
        await maybe(p.execute())
    assert await c.get(a) == "1"
    await c.aclose()


async def test_pipeline_redirect_error_is_kept_per_command(make, fc):
    k = key_in(8192, 16383)

    def get(cl_, node, args):
        if args[1] != k:
            return None
        if node is cl_.nodes[1]:
            return err(f"MOVED {cl.key_slot(k)} {cl_.nodes[0].addr}")
        return err("ERR node0 refuses")

    cluster = fc(extra={"GET": get})
    c = make(cluster.nodes[0].addr)
    p = pipe(c)
    p.set(key_in(0, 8191), "1").get(k)
    res = await maybe(p.execute(raise_on_error=False))
    assert res[0] is True and isinstance(res[1], ruvio.ResponseError) and "refuses" in str(res[1])
    await c.aclose()


async def test_pipeline_keyless_commands_go_to_the_seed(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    p = pipe(c)
    p.ping().ping()
    assert await maybe(p.execute()) == ["PONG", "PONG"]
    assert len(cluster.nodes[0].commands("PING")) == 2 and not cluster.nodes[1].commands("PING")
    await c.aclose()


# --------------------------------------------------------------------------- SCAN


async def test_scan_walks_every_node_with_a_composite_cursor(make, fc):
    def scan(cl_, node, args):
        idx = cl_.nodes.index(node)
        cursor = int(args[1])
        if cursor == 0:
            return array([bulk("5"), array([bulk(f"n{idx}-a")])])
        return array([bulk("0"), array([bulk(f"n{idx}-b")])])

    cluster = fc(extra={"SCAN": scan})
    c = make(cluster.nodes[0].addr)
    seen, cursor = [], 0
    for _ in range(10):
        cursor, keys = await c.scan(cursor, match="n*")
        seen += keys
        if cursor == 0:
            break
    assert seen == ["n0-a", "n0-b", "n1-a", "n1-b"]
    assert cursor == 0
    # every sub-call carried the per-node server cursor, never the composite one
    assert [x[1] for x in cluster.nodes[0].commands("SCAN")] == ["0", "5"]
    assert cluster.nodes[0].commands("SCAN")[0][2:] == ["MATCH", "n*"]
    await c.aclose()


async def test_scan_rejects_bad_cursors_and_passes_node_errors(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    for bad in ("abc", -1, "-5"):
        with pytest.raises(ruvio.ResponseError, match="invalid cursor"):
            await c.execute_command("SCAN", bad)
    with pytest.raises(ruvio.ResponseError, match="invalid cursor"):
        await c.execute_command("SCAN")
    cluster.extra["SCAN"] = lambda c_, n, a: err("ERR scan broke")
    with pytest.raises(ruvio.ResponseError, match="scan broke"):
        await c.scan(0)
    cluster.extra["SCAN"] = lambda c_, n, a: b"+OK\r\n"  # a non-array reply is returned as is
    assert await c.execute_command("SCAN", 0) == "OK"
    await c.aclose()


# --------------------------------------------------------------------------- transactions and WATCH


async def test_transaction_without_keys_runs_on_the_seed(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    t = pipe(c, transaction=True)
    t.ping()
    await maybe(t.execute())
    assert [x[0] for x in cluster.nodes[0].commands() if x[0] in ("MULTI", "EXEC")] == ["MULTI", "EXEC"]
    assert not cluster.nodes[1].commands("MULTI")
    await c.aclose()


async def test_transaction_is_redirected_when_the_slot_moved(make, fc):
    cluster = fc()
    cluster.advertised = [(0, 12000), (12001, 16383)]
    c = make(cluster.nodes[0].addr)
    k = key_in(8192, 12000)  # advertised on node0, owned by node1
    t = pipe(c, transaction=True)
    t.set(k, "v")
    await maybe(t.execute())
    assert [x[0] for x in cluster.nodes[0].commands() if x[0] in ("MULTI", "SET", "EXEC")] == ["MULTI", "SET", "EXEC"]
    assert [x[0] for x in cluster.nodes[1].commands() if x[0] in ("MULTI", "SET", "EXEC")] == ["MULTI", "SET", "EXEC"]
    await c.aclose()


async def test_transaction_redirect_gives_up_after_three_redirects(make, fc):
    k = key_in(0, 8191)

    def set_(cl_, node, args):
        other = cl_.nodes[1] if node is cl_.nodes[0] else cl_.nodes[0]
        return err(f"MOVED {cl.key_slot(k)} {other.addr}")

    cluster = fc(extra={"SET": set_})
    c = make(cluster.nodes[0].addr)
    t = pipe(c, transaction=True)
    t.set(k, "v")
    with pytest.raises(ruvio.MovedError):
        await maybe(t.execute())
    total = len(cluster.nodes[0].commands("MULTI")) + len(cluster.nodes[1].commands("MULTI"))
    assert total == 1 + cl.MAX_REDIRECTS  # the original attempt and three redirects
    await c.aclose()


async def test_watch_unwatch_and_transaction_node_check(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    a, b = key_in(0, 8191), key_in(8192, 16383)
    assert await c.watch(a) is True
    assert st(c).watch is not None
    assert await c.unwatch() is True  # sent to the node that holds the WATCH
    assert st(c).watch is None
    assert len(cluster.nodes[0].commands("UNWATCH")) == 1
    assert await c.unwatch() is True  # nothing watched: keyless, goes to the seed
    assert len(cluster.nodes[0].commands("UNWATCH")) == 2
    await c.watch(a)
    t = pipe(c, transaction=True)
    t.set(b, 1)
    with pytest.raises(ruvio.CrossSlotError, match="different nodes"):
        await maybe(t.execute())
    assert st(c).watch is None  # a refused transaction forgets the watch
    await c.watch(a)
    t = pipe(c, transaction=True)
    t.set(a, 1)
    await maybe(t.execute())
    assert st(c).watch is None
    # a failing WATCH does not record a node
    cluster.extra["WATCH"] = lambda c_, n, a_: err("ERR no watching")
    with pytest.raises(ruvio.ResponseError):
        await c.watch(a)
    assert st(c).watch is None
    await c.aclose()


async def test_pipeline_of_fan_or_cross_slot_commands_is_refused_before_sending(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    await c.ping()
    before = [len(n.log) for n in cluster.nodes]
    a, b = key_in(0, 8191), key_in(8192, 16383)
    for build in (lambda q: q.dbsize(), lambda q: q.flushdb(), lambda q: q.keys("*"), lambda q: q.sunion(a, b),
                  lambda q: q.rename(a, b)):
        q = pipe(c)
        q.ping()
        build(q)
        with pytest.raises(ruvio.RuvioError):
            await maybe(q.execute())
    assert [len(n.log) for n in cluster.nodes] == before
    await c.aclose()


async def test_pipeline_split_command_error_stays_at_its_index(make, fc):
    a, b = key_in(0, 8191), key_in(8192, 16383)
    cluster = fc(extra={"MGET": lambda c_, n, args: err("ERR mget broke") if n is c_.nodes[1] else None})
    c = make(cluster.nodes[0].addr)
    p = pipe(c)
    p.set(a, "1").mget(a, b).get(a).delete(a, b)
    res = await maybe(p.execute(raise_on_error=False))
    assert res[0] is True and res[2] == "1" and res[3] == 1
    assert isinstance(res[1], ruvio.ResponseError) and "mget broke" in str(res[1])
    p = pipe(c)
    p.mget(a, b)
    with pytest.raises(ruvio.ResponseError, match="mget broke"):
        await maybe(p.execute())
    await c.aclose()


async def test_pipeline_split_command_with_a_dead_node_raises(make, fc):
    a, b = key_in(0, 8191), key_in(8192, 16383)
    cluster = fc(extra={"EXISTS": lambda c_, n, args: DROP if n is c_.nodes[1] else None})
    c = make(cluster.nodes[0].addr)
    p = pipe(c)
    p.set(a, "1").exists(a, b)
    with pytest.raises(ruvio.ConnectionError):
        await maybe(p.execute())
    assert len(cluster.nodes[1].commands("EXISTS")) == 1  # not re-sent
    assert await c.get(a) == "1"
    await c.aclose()


# --------------------------------------------------------------------------- connection-wide state


async def test_select_setname_and_auth_reach_every_node_and_later_connections(make, fc):
    ok = lambda c_, n, a: OK  # noqa: E731 - the fake would treat the first argument as a key
    cluster = fc(extra={"SELECT": ok, "AUTH": ok, "CLIENT": ok})
    c = make(cluster.nodes[0].addr)
    assert await c.select(3) is True
    assert await c.client_setname("me") is True
    assert await c.auth("pw", "user") is True
    for n in st(c).nodes:
        o = opts_of(n)
        assert (o.db, o.client_name, o.password, o.username) == (3, "me", "pw", "user")
    assert all(cluster.nodes[i].commands("SELECT") for i in (0, 1))
    # a node connection opened later (here: after a full close) replays the handshake
    await c.aclose()
    k = key_in(8192, 16383)
    await c.set(k, "v")
    names = [x[0] for x in cluster.nodes[1].log]
    assert names[-4:-1] == ["AUTH", "SELECT", "CLIENT"] and names[-1] == "SET"
    await c.aclose()


async def test_raw_view_shares_state_and_returns_bytes(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    k = key_in(8192, 16383)
    await c.set(k, "v")
    assert await c.raw.get(k) == b"v"
    assert await c.get(k) == "v"
    assert st(c.raw) is st(c)
    p = c.raw
    r = pipe(p).get(k)
    assert await maybe(r.execute()) == [b"v"]
    await c.aclose()


async def test_connect_dedicated_and_close_cycles(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    me = await c.connect()
    assert me is c or me is c._c
    assert st(c).mode == 1
    await c.connect()
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1  # discovered once
    d = c.dedicated()
    assert type(d._c if isinstance(d, SyncAsAsync) else d).__name__ == "ClusterRuvio"
    assert st(d) is not st(c)
    k = key_in(8192, 16383)
    assert await d.set(k, "x") is True and await c.get(k) == "x"
    await d.aclose()
    await c.aclose()
    await c.aclose()  # idempotent
    assert await c.get(k) == "x"  # usable again: sockets reopen lazily
    await c.aclose()


async def test_connect_on_a_standalone_server(make):
    from fakecluster import standalone_node

    node = standalone_node()
    try:
        c = make(node.addr)
        await c.connect()
        assert st(c).mode == 2
        await c.connect()  # second connect on standalone mode is a no-op
        assert await c.set("a", "1") is True
        d = c.dedicated()
        assert await d.get("a") == "1"
        await d.aclose()
        await c.aclose()
    finally:
        node.close()


# --------------------------------------------------------------------------- Pub/Sub node selection


def chan_on(node_index, cluster, prefix="ch"):
    lo, hi = cluster.owner_ranges[node_index]
    return key_in(lo, hi, prefix)


async def wait_for(node, name, timeout=2.0):
    """Subscribe commands are fire-and-forget: wait until the node has logged one."""
    end = time.monotonic() + timeout
    while not node.commands(name) and time.monotonic() < end:
        await asyncio.sleep(0.01)
    return node.commands(name)


async def _pubsub(c):
    return await maybe(c.pubsub())


async def test_pubsub_discovers_and_targets_the_slot_zero_owner(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)  # nothing discovered yet
    ps = await _pubsub(c)
    assert st(c).mode == 1
    o = ps._conn.opts if hasattr(ps, "_conn") else ps._opts
    assert (o.host, o.port) == ("127.0.0.1", cluster.nodes[0].port)
    await maybe(ps.close())
    await c.aclose()


async def test_ssubscribe_goes_to_the_channel_owner_and_refuses_a_second_node(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    ps = await _pubsub(c)
    far, near = chan_on(1, cluster), chan_on(0, cluster)
    await maybe(ps.ssubscribe(far))  # the session has no subscriptions yet: it moves to the owner
    assert await wait_for(cluster.nodes[1], "SSUBSCRIBE")
    with pytest.raises(ruvio.RuvioError, match="separate pubsub"):
        await maybe(ps.ssubscribe(near))
    far2 = key_in(*cluster.owner_ranges[1], prefix="other")
    await maybe(ps.ssubscribe(far2))  # same node: fine
    with pytest.raises(ruvio.CrossSlotError):
        await maybe(ps.ssubscribe(far, near))
    await maybe(ps.close())
    await c.aclose()


async def test_ssubscribe_after_classic_subscribe_must_stay_on_the_same_node(make, fc):
    cluster = fc(extra={"SSUBSCRIBE": lambda c_, n, a: OK if len(a) < 2 else None})
    c = make(cluster.nodes[0].addr)
    ps = await _pubsub(c)
    await maybe(ps.subscribe("news"))
    with pytest.raises(ruvio.RuvioError, match="separate pubsub"):
        await maybe(ps.ssubscribe(chan_on(1, cluster)))
    await maybe(ps.ssubscribe(chan_on(0, cluster)))  # slot-0 owner: allowed
    await maybe(ps.ssubscribe())  # no channels: passes straight through
    await maybe(ps.close())
    await c.aclose()


async def test_pubsub_on_a_standalone_fallback_is_a_plain_session(make):
    from fakecluster import standalone_node

    node = standalone_node()
    try:
        c = make(node.addr)
        ps = await _pubsub(c)
        await maybe(ps.ssubscribe("a", "b"))  # no slot rules without a cluster
        assert await wait_for(node, "SSUBSCRIBE")
        await maybe(ps.close())
        await c.aclose()
    finally:
        node.close()


# --------------------------------------------------------------------------- sync-only transport behavior


def sync_cluster(cluster, **kw):
    return ruvio.Ruvio("127.0.0.1", cluster.nodes[0].port, cluster=True, **kw)


def test_sync_lazy_false_discovers_in_the_constructor(fc):
    cluster = fc()
    c = sync_cluster(cluster, lazy=False)
    assert c._cl.mode == 1 and len(cluster.nodes[0].commands("CLUSTER")) == 1
    assert c.is_cluster is True and c.nodes[0][1] == cluster.nodes[0].port
    assert c.connect() is c
    c.close()


def test_sync_reply_timeout_becomes_timeout_error_and_node_recovers(fc):
    slow = {"on": True}

    def get(cl_, node, args):
        if slow["on"] and node is cl_.nodes[1]:
            time.sleep(0.4)
        return None

    cluster = fc(extra={"GET": get})
    c = sync_cluster(cluster, socket_timeout=0.1)
    k = key_in(8192, 16383)
    with pytest.raises(ruvio.TimeoutError, match="timed out"):
        c.get(k)
    slow["on"] = False
    time.sleep(0.5)
    assert c.get(k) is None  # fresh socket for that node
    c.close()


def test_sync_non_socket_exception_passes_through_and_drops_the_socket(fc):
    cluster = fc(extra={"GET": lambda c_, n, a: b">1\r\n$1\r\nx\r\n$-1\r\n"})

    def boom(_):
        raise RuntimeError("callback bug")

    c = sync_cluster(cluster, on_push=boom)
    k = key_in(8192, 16383)
    c.ping()
    with pytest.raises(RuntimeError, match="callback bug"):
        c.get(k)
    node = c._cl.by_addr[("127.0.0.1", cluster.nodes[1].port)]
    assert node.conn.sock is None
    c.close()


def test_sync_write_failure_on_one_node_is_reported_by_scatter(fc):
    cluster = fc()
    c = sync_cluster(cluster)
    a, b = key_in(0, 8191), key_in(8192, 16383)
    c.mset({a: "1", b: "2"})
    victim = c._cl.nodes[1]
    victim.conn.sock.close()  # the next sendall on that node fails locally
    with pytest.raises(ruvio.ConnectionError):
        c.mget(a, b)
    assert victim.conn.sock is None
    assert c.mget(a, b) == ["1", "2"]
    # same through a pipeline
    victim.conn.sock.close()
    p = c.pipeline()
    p.get(a).get(b)
    with pytest.raises(ruvio.ConnectionError):
        p.execute()
    c.close()


def test_sync_node_connection_closed_between_commands_is_reopened(fc):
    cluster = fc()
    c = sync_cluster(cluster)
    a = key_in(0, 8191)
    c.set(a, "1")
    c._cl.nodes[0].conn.close()
    assert c.get(a) == "1"
    c.close()
    c.close()


def test_sync_pubsub_retargets_before_connecting_only(fc):
    cluster = fc()
    c = sync_cluster(cluster)
    ps = c.pubsub()
    far = chan_on(1, cluster)
    ps.ssubscribe(far)
    assert ps._conn.opts.port == cluster.nodes[1].port
    ps.close()
    c.close()


# --------------------------------------------------------------------------- asyncio-only behavior


async def test_async_cluster_rejects_a_unix_socket():
    with pytest.raises(ValueError, match="host:port"):
        ruvio.asyncio.Ruvio(unix_socket_path="/tmp/x.sock", cluster=True)


async def test_async_cluster_connect_modes_and_dedicated(fc):
    cluster = fc()
    c = ruvio.asyncio.Ruvio("127.0.0.1", cluster.nodes[0].port, cluster=True)
    assert await c.connect() is c
    assert await c.is_cluster() is True
    d = c.dedicated()
    assert isinstance(d, ruvio.asyncio.ClusterRuvio) and d._cl is not c._cl
    await d.aclose()
    await c.close()


async def test_async_moved_retry_can_be_cancelled(fc):
    k = key_in(8192, 16383)
    gate = threading.Event()

    def get(cl_, node, args):
        if node is cl_.nodes[0]:
            return err(f"MOVED {cl.key_slot(k)} {cl_.nodes[1].addr}")
        gate.wait(2)
        return bulk("late")

    cluster = fc(extra={"GET": get})
    c = ruvio.asyncio.Ruvio("127.0.0.1", cluster.nodes[0].port, cluster=True)
    await c.ping()
    st(c).slots[cl.key_slot(k)] = 0
    t = asyncio.ensure_future(c.get(k))
    await asyncio.sleep(0.2)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    gate.set()
    await asyncio.sleep(0.1)
    await c.aclose()


# --------------------------------------------------------------------------- first-use paths on a fresh client


async def test_nodes_property_discovers_on_first_use(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    assert len(await prop(c, "nodes")) == 2
    assert st(c).mode == 1
    await c.aclose()


async def test_concurrent_first_commands_discover_once(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    ks = [key_in(0, 16383, f"f{i}-") for i in range(8)]
    res = await asyncio.gather(*[maybe(c.set(k, "v")) for k in ks])
    assert all(res)
    assert len(cluster.nodes[0].commands("CLUSTER")) == 1
    await c.aclose()


async def test_first_command_without_a_key_goes_to_the_seed(make, fc):
    cluster = fc()
    c = make(cluster.nodes[0].addr)
    assert await c.execute_command("FOOBAR") == "OK"
    assert cluster.nodes[0].commands("FOOBAR") and not cluster.nodes[1].commands("FOOBAR")
    assert st(c).mode == 1
    await c.aclose()


async def test_first_pipeline_on_a_standalone_server_is_a_plain_pipeline(make):
    from fakecluster import standalone_node

    node = standalone_node()
    try:
        c = make(node.addr)
        p = pipe(c)
        p.set("a", "1").set("b", "2").mget("a", "b")
        assert await maybe(p.execute()) == [True, True, ["1", "2"]]  # no per-slot grouping or splitting
        assert st(c).mode == 2
        p = pipe(c)
        p.get("a")
        assert await maybe(p.execute()) == ["1"]
        await c.aclose()
    finally:
        node.close()


async def test_seed_missing_from_the_slot_map_is_still_reachable(make, fc):
    cluster = fc(ranges=[(0, 5460), (5461, 10922), (10923, 16383)])
    cluster.hidden = {0}  # the seed serves slots but CLUSTER SLOTS does not list it
    cluster.advertised = [(0, 5460), (5461, 10922), (10923, 16383)]
    c = make(cluster.nodes[0].addr)
    k_seed = key_in(0, 5460)
    k_other = key_in(5461, 10922)
    assert await c.set(k_seed, "s") is True  # unmapped slot -> seed
    assert await c.set(k_other, "o") is True
    assert cluster.nodes[0].store.get(k_seed) == "s"
    nodes = await prop(c, "nodes")
    assert ("127.0.0.1", cluster.nodes[0].port) not in nodes and len(nodes) == 2
    await c.refresh_slots()  # a refresh keeps the seed usable too
    assert await c.get(k_seed) == "s"
    await c.aclose()
