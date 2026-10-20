# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Seed addresses: connecting, reconnecting and slot table reloads against scripted fake servers
(sync and asyncio clients)."""

from __future__ import annotations

import socket
from typing import List

import pytest

import ruvio
import ruvio.asyncio
from conftest import SyncAsAsync
from fakecluster import DROP, OK, FakeNode, array, bulk, err, integer
from test_cluster_unit import key_in, st


def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def value_node(value: str) -> FakeNode:
    """A standalone server whose ``GET`` answers ``value``; ``node.store['drop']`` makes it close
    the connection instead of answering."""

    def handle(node: FakeNode, args: List[str]) -> object:
        name = args[0].upper()
        if name == "GET":
            return DROP if node.store.get("drop") else bulk(value)
        if name == "CLUSTER":
            return err("ERR cluster mode is disabled")
        return OK

    return FakeNode(handle)


def slots_reply(*rows) -> bytes:
    """``CLUSTER SLOTS`` for ``(first, last, port)`` rows."""
    return array(
        [array([integer(lo), integer(hi), array([bulk("127.0.0.1"), integer(port), bulk(f"n{port}")])]) for lo, hi, port in rows]
    )


@pytest.fixture(params=["sync", "async"])
def build(request):
    """``build(seeds, cluster=False, **kw)`` makes a client of the parametrized kind (awaitable API)."""

    def make(seeds, cluster=False, **kw):
        kw.setdefault("connect_timeout", 2.0)
        if request.param == "sync":
            return SyncAsAsync(ruvio.Ruvio(seeds=seeds, cluster=cluster, **kw))
        return ruvio.asyncio.Ruvio(seeds=seeds, cluster=cluster, **kw)

    return make


async def test_connect_skips_a_seed_that_cannot_be_reached(build):
    live = value_node("live")
    try:
        c = build([("127.0.0.1", closed_port()), f"127.0.0.1:{live.port}"])
        assert await c.get("k") == "live"
        assert live.connections == 1
        await c.aclose()
    finally:
        live.close()


async def test_connect_fails_with_the_last_connect_error_when_no_seed_answers(build):
    a, b = closed_port(), closed_port()
    c = build([("127.0.0.1", a), ("127.0.0.1", b)])
    with pytest.raises(ruvio.ConnectionError) as info:
        await c.get("k")
    assert str(b) in str(info.value)


async def test_a_refused_auth_stops_at_once(build):
    refusing = FakeNode(lambda node, args: err("WRONGPASS invalid password") if args[0].upper() == "AUTH" else OK)
    second = value_node("second")
    try:
        c = build([f"127.0.0.1:{refusing.port}", f"127.0.0.1:{second.port}"], password="x")
        with pytest.raises(ruvio.ResponseError):
            await c.get("k")
        assert second.connections == 0
    finally:
        refusing.close()
        second.close()


async def test_a_dropped_connection_moves_on_to_the_next_seed(build):
    first, last = value_node("first"), value_node("last")
    try:
        c = build([f"127.0.0.1:{first.port}", ("127.0.0.1", closed_port()), ("127.0.0.1", last.port)], follow_primary=False)  # probes would add connections
        assert await c.get("k") == "first"
        first.store["drop"] = "1"
        with pytest.raises(ruvio.ConnectionError):
            await c.get("k")  # the command that hit the drop still fails
        got = None
        for _ in range(3):
            try:
                got = await c.get("k")
                break
            except ruvio.ConnectionError:
                continue
        assert got == "last"
        assert first.connections == 1
        await c.aclose()
    finally:
        first.close()
        last.close()


async def test_a_reconnect_ends_with_the_address_that_dropped(build):
    a, b = value_node("a"), value_node("b")
    try:
        c = build([("127.0.0.1", a.port), ("127.0.0.1", b.port)])
        assert await c.get("k") == "a"
        a.store["drop"] = "1"
        with pytest.raises(ruvio.ConnectionError):
            await c.get("k")
        assert await c.get("k") == "b"
        b.store["drop"] = "1"
        a.store.clear()
        with pytest.raises(ruvio.ConnectionError):
            await c.get("k")
        assert await c.get("k") == "a"  # b dropped, so the attempt starts with a
        await c.aclose()
    finally:
        a.close()
        b.close()


@pytest.mark.parametrize(
    "seeds",
    [[":6379"], ["host"], ["host:0"], ["host:70000"], ["host:x"], [("", 6379)], [("host", 0)], ["[]:6379"]],
)
def test_an_empty_seed_host_or_bad_port_is_refused(seeds):
    with pytest.raises(ValueError):
        ruvio.Ruvio(seeds=seeds)
    with pytest.raises(ValueError):
        ruvio.asyncio.Ruvio(seeds=seeds)


def test_seed_forms_and_defaults():
    for client in (ruvio.Ruvio(seeds=["[::1]:7000", ("example.org", 7001)]), ruvio.asyncio.Ruvio(seeds=["[::1]:7000", ("example.org", 7001)])):
        o = client._conn.opts if hasattr(client, "_conn") else client._st.opts
        assert o.seeds == [("::1", 7000), ("example.org", 7001)]
        assert (o.host, o.port) == ("::1", 7000)
    for client in (ruvio.Ruvio("h", 1), ruvio.Ruvio(seeds=[]), ruvio.Ruvio(seeds=None)):
        assert client._conn.opts.seeds is None
    assert ruvio.Ruvio("h", 1)._conn.opts.host == "h"


# --------------------------------------------------------------------------- cluster


class Lost:
    """A cluster whose node ``x`` is stopped after the client discovered it. ``y`` is reachable but
    cannot answer ``CLUSTER SLOTS``; ``z`` is a seed that knows the new map (``z`` and ``y``)."""

    def __init__(self) -> None:
        self.y = FakeNode(lambda node, args: err("CLUSTERDOWN not available") if args[0].upper() == "CLUSTER" else bulk("y"))
        self.z = FakeNode(self._z)
        self.x = FakeNode(self._x)

    def _x(self, node: FakeNode, args: List[str]) -> object:
        if args[0].upper() == "CLUSTER":
            return slots_reply((0, 8191, node.port), (8192, 16383, self.y.port))
        return bulk("x")

    def _z(self, node: FakeNode, args: List[str]) -> object:
        if args[0].upper() == "CLUSTER":
            return slots_reply((0, 8191, node.port), (8192, 16383, self.y.port))
        return bulk("z")

    def close(self) -> None:
        for n in (self.x, self.y, self.z):
            n.close()


@pytest.fixture
def lost():
    cluster = Lost()
    yield cluster
    cluster.close()


async def _first_good(c, key):
    for _ in range(3):
        try:
            return await c.get(key)
        except ruvio.ConnectionError:
            continue
    return None


async def test_a_lost_node_reloads_the_slot_map_from_a_seed(build, lost):
    c = build([("127.0.0.1", lost.x.port), ("127.0.0.1", lost.z.port)], cluster=True)
    key = key_in(0, 8191)
    assert await c.get(key) == "x"
    lost.x.close()
    assert await _first_good(c, key) == "z"  # the new map names z as the owner
    assert ("127.0.0.1", lost.z.port) in st(c).by_addr
    assert lost.x.port not in {n.port for n in st(c).nodes}
    await c.aclose()


async def test_a_keyless_command_whose_node_is_gone_goes_to_the_first_reachable_node(build, lost):
    c = build([("127.0.0.1", lost.x.port), ("127.0.0.1", lost.z.port)], cluster=True)
    assert await c.get(key_in(0, 8191)) == "x"
    lost.x.close()
    for _ in range(3):
        try:
            await c.ping()
            break
        except ruvio.ConnectionError:
            continue
    assert lost.z.commands("PING")
    await c.aclose()


async def test_a_failed_reload_leaves_no_seed_endpoints_behind(build, lost):
    gone = closed_port()
    c = build([("127.0.0.1", lost.x.port), ("127.0.0.1", gone)], cluster=True)
    key = key_in(0, 8191)
    assert await c.get(key) == "x"
    known = set(st(c).by_addr)
    lost.x.close()
    for _ in range(2):
        with pytest.raises(ruvio.ConnectionError):
            await c.get(key)
    assert set(st(c).by_addr) == known
    assert ("127.0.0.1", gone) not in st(c).by_addr
    await c.aclose()


async def test_the_first_reachable_seed_answers_the_slot_map(build, lost):
    c = build([("127.0.0.1", closed_port()), ("127.0.0.1", lost.x.port)], cluster=True)
    assert await c.get(key_in(0, 8191)) == "x"
    assert st(c).mode == 1
    assert ("127.0.0.1", lost.x.port) in st(c).by_addr
    await c.aclose()
