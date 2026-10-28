# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Reads from replicas against scripted fake servers: a 2-shard primary where each shard lists one replica
in ``CLUSTER SLOTS`` (sync and asyncio clients)."""

from __future__ import annotations

import inspect
import itertools
import socket

import pytest

import ruvio
import ruvio.asyncio
from ruvio import _cluster as cl
from conftest import SyncAsAsync
from fakecluster import DROP, OK, FakeNode, array, bulk, err, integer, standalone_node


async def _maybe(x):
    return await x if inspect.isawaitable(x) else x


def key_in(lo, hi, prefix="k"):
    for i in itertools.count():
        k = f"{prefix}{i}"
        if lo <= cl.key_slot(k) <= hi:
            return k


def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Replicated:
    """Two primaries (slots 0-8191 and 8192-16383), each answering ``CLUSTER SLOTS`` with its replica."""

    def __init__(self, with_replicas=True, replica_ports=None):
        self.primaries = [FakeNode(self._primary), FakeNode(self._primary)]
        self.replicas = [FakeNode(self._replica), FakeNode(self._replica)]
        self.with_replicas = with_replicas
        self.replica_ports = replica_ports
        self.loading = False
        self.drop_once = False

    def _slots(self):
        rows = []
        for i, (lo, hi) in enumerate([(0, 8191), (8192, 16383)]):
            row = [integer(lo), integer(hi), array([bulk("127.0.0.1"), integer(self.primaries[i].port), bulk(f"p{i}")])]
            if self.with_replicas:
                port = self.replica_ports[i] if self.replica_ports else self.replicas[i].port
                row.append(array([bulk("127.0.0.1"), integer(port), bulk(f"r{i}")]))
            rows.append(array(row))
        return array(rows)

    def _primary(self, node, args):
        name = args[0].upper()
        if name == "CLUSTER":
            return self._slots()
        if name == "GET":
            return bulk("primary")
        if name == "MGET":
            return array([bulk("p") for _ in args[1:]])
        if name in ("EXISTS", "DEL"):
            return integer(len(args) - 1)
        if name == "STRLEN":
            return integer(1)
        if name == "BLPOP":
            return array([bulk(args[1]), bulk("v")])
        return OK

    def _replica(self, node, args):
        name = args[0].upper()
        if name == "READONLY":
            return OK
        if self.loading:
            return err("LOADING Ruvio is loading the dataset in memory")
        if name == "GET":
            if args[1] == "dropme" and self.drop_once:
                self.drop_once = False
                return DROP
            return bulk("replica")
        if name == "MGET":
            return array([bulk("r") for _ in args[1:]])
        if name == "EXISTS":
            return integer(10 * (len(args) - 1))
        if name == "STRLEN":
            return integer(2)
        return err("READONLY You can't write against a read only replica.")

    def close(self):
        for n in self.primaries + self.replicas:
            n.close()


@pytest.fixture
def pair():
    p = Replicated()
    yield p
    p.close()


@pytest.fixture(params=["sync", "async"])
def make(request):
    def build(addr, **kw):
        host, port = addr.rsplit(":", 1)
        if request.param == "sync":
            return SyncAsAsync(ruvio.Ruvio(host, int(port), cluster=True, **kw))
        return ruvio.asyncio.Ruvio(host, int(port), cluster=True, **kw)

    yield build


def pipe(c):
    return c._c.pipeline() if isinstance(c, SyncAsAsync) else c.pipeline()


def state(c):
    return c._c._cl if isinstance(c, SyncAsAsync) else c._cl


A, B = key_in(0, 8191, "a"), key_in(8192, 16383, "b")


# --------------------------------------------------------------------------- parsing and options


def test_cluster_slots_keeps_replicas_and_skips_bad_entries():
    reply = [
        [0, 8191, ["h1", 1, "p"], ["r1", 11, "x"], ["r2", 12, "y"], ["r1", 11, "dup"], ["bad"], "junk", ["r3", "no"]],
        [8192, 16383, ["h2", 2, "q"]],
    ]
    t = cl.parse_slots(reply, "seed")
    assert t.nodes == [("h1", 1), ("h2", 2)]
    assert t.replicas == [[("r1", 11), ("r2", 12)], []]
    assert cl.parse_slots([[0, 16383, ["", 1, "p"], ["", 2, "r"]]], "seedhost").replicas == [[("seedhost", 2)]]


def test_read_policy_values():
    assert cl.policy_code(None) == cl.policy_code("primary") == cl.policy_code(ruvio.ReadPolicy.PRIMARY) == 0
    assert cl.policy_code("prefer-replica") == cl.policy_code(ruvio.ReadPolicy.PREFER_REPLICA) == 1
    assert cl.policy_code("replica") == cl.policy_code(ruvio.ReadPolicy.REPLICA) == 2
    assert ruvio.asyncio.ReadPolicy is ruvio.ReadPolicy
    with pytest.raises(ValueError):
        ruvio.Ruvio(cluster=True, read_policy="nearest")
    with pytest.raises(ValueError):
        ruvio.asyncio.Ruvio(cluster=True, read_policy="nearest")


def test_only_non_blocking_readonly_commands_count_as_reads():
    for name in (b"GET", b"HGETALL", b"MGET", b"EXISTS", b"ZRANGE"):
        assert name in cl.READS
    for name in (b"SET", b"DEL", b"BLPOP", b"XREAD", b"EVAL", b"MULTI", b"SCAN", b"NOSUCH"):
        assert name not in cl.READS
    from ruvio._resp import encode

    assert cl.is_read(encode("GET", ("k",))) and cl.is_read(encode("get", ("k",)))
    assert not cl.is_read(encode("SET", ("k", "v")))


# --------------------------------------------------------------------------- routing


async def test_reads_go_to_the_replica_and_writes_to_the_primary(make, pair):
    c = make(pair.primaries[0].addr, read_policy=ruvio.ReadPolicy.PREFER_REPLICA)
    assert await c.get(A) == "replica" and await c.get(B) == "replica"
    assert await c.set(A, "1") is True
    assert await c.mget(A, B) == ["r", "r"]  # each part reads from its shard's replica
    assert await c.exists(A, B) == 20
    assert await c.delete(A, B) == 2  # the parts of a write go to the primaries
    assert [x[0] for n in pair.primaries for x in n.log if x[0] != "CLUSTER"] == ["SET", "DEL", "DEL"]
    assert not [x for n in pair.replicas for x in n.log if x[0] in ("SET", "DEL")]
    assert len(pair.replicas[0].commands("GET")) == 1 and len(pair.replicas[0].commands("MGET")) == 1
    await c.aclose()


async def test_blocking_commands_stay_on_the_primary(make, pair):
    c = make(pair.primaries[0].addr, read_policy="prefer-replica")
    assert await c.blpop(A, timeout=1) == (A, "v")
    assert not pair.replicas[0].commands("BLPOP") and len(pair.primaries[0].commands("BLPOP")) == 1
    await c.aclose()


async def test_mixed_pipeline_goes_to_primaries_and_all_read_pipeline_to_replicas(make, pair):
    c = make(pair.primaries[0].addr, read_policy="prefer-replica")
    p = pipe(c)
    p.set(A, "1")
    p.get(A)
    p.get(B)
    assert await _maybe(p.execute()) == [True, "primary", "primary"]
    assert not [x for n in pair.replicas for x in n.log]  # no replica connection was needed
    p = pipe(c)
    p.get(A)
    p.get(B)
    p.strlen(A)
    assert await _maybe(p.execute()) == ["replica", "replica", 2]
    assert [x[0] for x in pair.replicas[0].commands()] == ["READONLY", "GET", "STRLEN"]
    assert [x[0] for x in pair.replicas[1].commands()] == ["READONLY", "GET"]
    assert len(pair.primaries[0].commands("GET")) == 1
    await c.aclose()


async def test_readonly_is_the_first_command_of_every_replica_connection(make, pair):
    c = make(pair.primaries[0].addr, read_policy="replica")
    assert await c.get(A) == "replica"
    assert pair.replicas[0].log[0] == ["READONLY"]
    pair.drop_once = True
    key = "dropme"
    owner = pair.replicas[cl.key_slot(key) // 8192]
    with pytest.raises(ruvio.ConnectionError):
        await c.get(key)  # the socket dropped after the command was sent: it fails, nothing is re-sent
    assert len(owner.commands("GET")) == (2 if owner is pair.replicas[0] else 1)
    before = owner.connections
    assert await c.get(key) == "replica"
    assert owner.connections == before + 1
    assert [x[0] for x in owner.log].count("READONLY") == owner.connections
    await c.aclose()


async def test_the_default_policy_opens_no_replica_connection(make, pair):
    c = make(pair.primaries[0].addr)
    assert await c.get(A) == "primary"
    assert await c.mget(A, B) == ["p", "p"]
    assert pair.replicas[0].connections == pair.replicas[1].connections == 0
    assert state(c).reps == [] and state(c).policy == 0
    await c.aclose()


# --------------------------------------------------------------------------- fallback


async def test_an_unreachable_replica_falls_back_under_prefer_and_fails_under_replica(make):
    pair = Replicated(replica_ports=[closed_port(), closed_port()])
    try:
        c = make(pair.primaries[0].addr, read_policy="prefer-replica", connect_timeout=1)
        assert await c.get(A) == "primary"
        assert await c.mget(A, B) == ["p", "p"]
        p = pipe(c)
        p.get(A)
        p.get(B)
        assert await _maybe(p.execute()) == ["primary", "primary"]
        await c.aclose()
        c = make(pair.primaries[0].addr, read_policy="replica", connect_timeout=1)
        with pytest.raises(ruvio.ConnectionError):
            await c.get(A)
        await c.aclose()
    finally:
        pair.close()


async def test_a_loading_replica_is_skipped_and_the_read_goes_to_the_primary(make, pair):
    pair.loading = True
    c = make(pair.primaries[0].addr, read_policy="prefer-replica")
    assert await c.get(A) == "primary"
    assert await c.get(A) == "primary"
    assert len(pair.replicas[0].commands("GET")) == 1  # skipped until the next slot map load
    assert await c.mget(A, B) == ["p", "p"]
    await c.aclose()


async def test_a_loading_replica_fails_the_read_under_replica(make, pair):
    pair.loading = True
    c = make(pair.primaries[0].addr, read_policy="replica")
    with pytest.raises(ruvio.ResponseError, match="LOADING"):
        await c.get(A)
    with pytest.raises(ruvio.RuvioError, match="no replica"):
        await c.get(A)
    assert not pair.primaries[0].commands("GET")
    await c.aclose()


async def test_the_replica_is_used_again_after_a_slot_map_load(make, pair):
    pair.loading = True
    c = make(pair.primaries[0].addr, read_policy="prefer-replica")
    assert await c.get(A) == "primary"
    pair.loading = False
    await _maybe(c.refresh_slots())
    assert await c.get(A) == "replica"
    await c.aclose()


async def test_replica_policy_without_a_replica_fails_and_prefer_uses_the_primary(make):
    pair = Replicated(with_replicas=False)
    try:
        c = make(pair.primaries[0].addr, read_policy="replica")
        with pytest.raises(ruvio.RuvioError, match="no replica"):
            await c.get(A)
        assert await c.set(A, "1") is True  # writes do not need one
        await c.aclose()
        c = make(pair.primaries[0].addr, read_policy="prefer-replica")
        assert await c.get(A) == "primary"
        await c.aclose()
    finally:
        pair.close()


async def test_a_standalone_server_ignores_the_policy(make):
    node = standalone_node()
    try:
        c = make(node.addr, read_policy="replica")
        assert await c.set("a", "1") is True and await c.get("a") == "1"
        await c.aclose()
    finally:
        node.close()
