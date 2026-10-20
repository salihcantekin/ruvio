# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Following a failover against scripted nodes that answer ``ROLE``, refuse writes with ``READONLY`` when they
are replicas, and (for the coordinator case) name a primary for ``SENTINEL`` (sync and asyncio clients).
``test_failover_live.py`` does the same against real servers."""

from __future__ import annotations

import inspect
import socket
import time
from typing import List

import pytest

import ruvio
import ruvio.asyncio
import ruvio.asyncio.client as aclient
import ruvio.client as sclient
from conftest import DEADLINE, SyncAsAsync, await_for, wait_for
from fakecluster import DROP, OK, FakeNode, array, bulk, err, integer

WRITES = ("SET", "INCR", "DEL")


def handle(node: FakeNode, args: List[str]) -> object:
    """``node.store["mode"]`` is ``master``, ``replica`` (``store["primary"]`` is the port it reports),
    ``liar`` (says master, refuses writes) or ``names`` (a coordinator; ``store["primary"]`` is the port)."""
    name = args[0].upper()
    mode = node.store.get("mode", "master")
    primary = node.store.get("primary", "0")
    if name == "AUTH" and node.store.get("refuse_auth"):
        return err("WRONGPASS invalid username-password pair")
    if name == "ROLE":
        if mode in ("master", "liar", "names"):
            return array([bulk("master"), integer(0), array([])])
        return array([bulk("slave"), bulk("127.0.0.1"), integer(int(primary)), bulk("connected"), integer(0)])
    if name == "SENTINEL":
        return array([bulk("127.0.0.1"), bulk(primary)]) if mode == "names" and primary != "0" else b"*-1\r\n"
    if name in WRITES and mode in ("replica", "liar"):
        return err("READONLY You can't write against a read only replica.")
    if name == "GET":
        return bulk("v")
    if name == "INCR":
        return integer(1)
    if name == "EXEC":
        return array([])
    return OK


def node(mode: str = "master", primary: object = 0) -> FakeNode:
    n = FakeNode(handle)
    become(n, mode, primary)
    return n


def become(n: FakeNode, mode: str, primary: object = 0) -> None:
    n.store["mode"] = mode
    n.store["primary"] = str(getattr(primary, "port", primary))


def dead_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def drop_connections(n: FakeNode) -> None:
    for c in list(n._conns):
        try:
            c.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def seen(n: FakeNode, name: str) -> int:
    return len(n.commands(name))


def seeds(*nodes) -> List[str]:
    return [n if isinstance(n, str) else n.addr for n in nodes]


@pytest.fixture(params=["sync", "async"])
def build(request, monkeypatch):
    """``build(seeds, **kw)`` makes a client of the parametrized kind (awaitable API); the waits between the
    rounds of a ``READONLY`` search are short so the whole backoff fits in a test."""
    monkeypatch.setattr(sclient, "WAITS", (0.01,) * 7)
    monkeypatch.setattr(aclient, "WAITS", (0.01,) * 7)
    made = []

    def make(addresses, **kw):
        kw.setdefault("connect_timeout", 1.0)
        if request.param == "sync":
            c = SyncAsAsync(ruvio.Ruvio(seeds=addresses, socket_timeout=5.0, **kw))
        else:
            c = ruvio.asyncio.Ruvio(seeds=addresses, **kw)
        made.append(c)
        return c

    yield make
    for c in made:
        if request.param == "sync":
            c._c.close()
        elif c._st.proto is not None and c._st.proto.transport is not None:
            c._st.proto.transport.close()


def is_sync(c) -> bool:
    return getattr(c, "kind", "async") == "sync"


async def lose_connection(c, n: FakeNode) -> None:
    """Closes the sockets ``n`` holds and returns once the client knows (the command that finds a sync
    socket dropped fails; the asyncio client sees it by itself)."""
    drop_connections(n)
    if is_sync(c):
        with pytest.raises(ruvio.ConnectionError):
            await c.ping()
    else:
        await await_for(lambda: c._st.proto is None, "the client to see its socket closed")


def assert_readonly(exc_info) -> None:
    assert exc_info.value.code == "READONLY"


async def test_a_readonly_write_follows_the_new_primary_and_is_sent_once_more(build):
    old, new = node("master"), node("replica")
    try:
        c = build(seeds(old, new))
        assert await c.set("k", "1") is True
        assert (seen(old, "SET"), seen(new, "SET")) == (1, 0)
        become(old, "replica", new)
        become(new, "master")
        assert await c.set("k", "2") is True
        # refused once on the old node, then once on the new primary
        assert (seen(old, "SET"), seen(new, "SET")) == (2, 1)
        assert await c.set("k", "3") is True
        assert (seen(old, "SET"), seen(new, "SET")) == (2, 2)
    finally:
        old.close()
        new.close()


async def test_a_healthy_connection_sends_nothing_extra(build):
    live, other = node("master"), node("replica")
    try:
        c = build(seeds(live, other))
        for _ in range(50):
            await c.set("k", "v")
        assert len(live.log) == 50
        assert len(other.log) == 0
        assert live.connections == 1 and other.connections == 0
    finally:
        live.close()
        other.close()


async def test_a_replica_names_its_primary_when_that_is_no_seed(build):
    replica, hidden = node("master"), node("master")
    try:
        c = build([replica.addr, f"127.0.0.1:{dead_port()}"])
        assert await c.ping()
        become(replica, "replica", hidden)
        assert await c.set("k", "v") is True
        assert (seen(replica, "SET"), seen(hidden, "SET")) == (1, 1)
    finally:
        replica.close()
        hidden.close()


async def test_a_reported_primary_that_does_not_answer_is_ignored(build):
    replica, other = node("master"), node("replica")
    try:
        c = build([replica.addr, other.addr])
        assert await c.ping()
        become(replica, "replica", dead_port())
        become(other, "master")
        assert await c.set("k", "v") is True
        assert (seen(replica, "SET"), seen(other, "SET")) == (1, 1)
    finally:
        replica.close()
        other.close()


async def test_follow_primary_off_returns_readonly_and_sends_no_role(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second), follow_primary=False)
        assert await c.ping()
        become(first, "replica", second)
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert_readonly(info)
        assert (seen(first, "ROLE"), len(second.log)) == (0, 0)
    finally:
        first.close()
        second.close()


async def test_one_seed_sends_no_role(build):
    only = node("master")
    try:
        c = build(seeds(only))
        assert await c.ping()
        become(only, "replica", 0)
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert_readonly(info)
        assert seen(only, "ROLE") == 0
        assert only.connections == 1
    finally:
        only.close()


async def test_a_reconnect_takes_the_node_that_says_master_not_the_next_seed(build):
    first, second, third = node("master"), node("master"), node("master")
    try:
        c = build(seeds(first, second, third))
        assert await c.ping()
        become(second, "replica", third)
        become(first, "replica", third)
        await lose_connection(c, first)
        assert await c.set("k", "v") is True
        assert (seen(first, "SET"), seen(second, "SET"), seen(third, "SET")) == (0, 0, 1)
    finally:
        first.close()
        second.close()
        third.close()


async def test_a_reconnect_without_any_primary_still_connects_to_the_first_seed_that_answers(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second))
        assert await c.ping()
        become(first, "replica")
        become(second, "replica")
        await lose_connection(c, first)
        assert await c.ping()
        assert seen(second, "PING") == 1  # the seed after the one that dropped, as before
    finally:
        first.close()
        second.close()


async def test_a_reconnect_restores_the_database_on_the_primary(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second), db=3)
        assert await c.ping()
        become(first, "replica", second)
        await lose_connection(c, first)
        assert await c.ping()
        # the probe sent ROLE only; the real connection SELECT and then the command
        assert [a[0].upper() for a in second.log] == ["ROLE", "SELECT", "PING"]
        assert second.connections == 2
    finally:
        first.close()
        second.close()


async def test_without_any_primary_the_readonly_error_comes_back_after_eight_rounds(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second))
        assert await c.ping()
        become(first, "replica")
        become(second, "replica")
        started = time.monotonic()
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert_readonly(info)
        assert time.monotonic() - started < DEADLINE
        assert (seen(first, "SET"), seen(second, "SET")) == (1, 0)
        # eight rounds, each asks both nodes once (the replica reports no primary)
        assert (seen(first, "ROLE"), seen(second, "ROLE")) == (8, 8)
        # the connection stayed where it was
        become(first, "master")
        assert await c.set("k", "v") is True
        assert seen(first, "SET") == 2
    finally:
        first.close()
        second.close()


def test_the_default_backoff_is_eight_rounds_over_seven_point_one_seconds():
    from ruvio import _follow

    assert _follow.ROUNDS == 8
    assert _follow.WAITS == (0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0)
    assert round(sum(_follow.WAITS), 6) == 7.1


async def test_a_write_refused_twice_is_not_sent_a_third_time(build):
    old, liar = node("master"), node("master")
    try:
        c = build(seeds(old, liar))
        assert await c.ping()
        become(old, "replica")
        become(liar, "liar")
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert_readonly(info)
        assert (seen(old, "SET"), seen(liar, "SET")) == (1, 1)
    finally:
        old.close()
        liar.close()


async def test_a_master_that_just_refused_is_not_taken_again(build):
    """The node that answered READONLY can still say ``master``; the round skips it."""
    liar, real = node("master"), node("master")
    try:
        c = build(seeds(liar, real))
        assert await c.ping()
        become(liar, "liar")
        assert await c.set("k", "v") is True
        assert (seen(liar, "SET"), seen(real, "SET")) == (1, 1)
    finally:
        liar.close()
        real.close()


async def test_a_write_refused_inside_a_transaction_follows_but_is_not_sent_again(build):
    old, new = node("master"), node("replica")
    try:
        c = build(seeds(old, new))
        assert await c.multi() is True
        become(old, "replica", new)
        become(new, "master")
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert_readonly(info)
        assert seen(new, "SET") == 0
        # the client moved: the next command is not inside the lost transaction
        assert await c.set("k", "v") is True
        assert seen(new, "SET") == 1
        assert seen(new, "MULTI") == 0
    finally:
        old.close()
        new.close()


async def test_a_raw_multi_through_execute_command_counts_as_a_transaction(build):
    old, new = node("master"), node("replica")
    try:
        c = build(seeds(old, new))
        await c.execute_command("MULTI")
        become(old, "replica", new)
        become(new, "master")
        with pytest.raises(ruvio.ResponseError):
            await c.set("k", "v")
        assert seen(new, "SET") == 0
    finally:
        old.close()
        new.close()


async def test_after_exec_a_refused_write_is_sent_again(build):
    old, new = node("master"), node("replica")
    try:
        c = build(seeds(old, new))
        await c.multi()
        await c.exec()
        become(old, "replica", new)
        become(new, "master")
        assert await c.set("k", "v") is True
        assert seen(new, "SET") == 1
    finally:
        old.close()
        new.close()


async def test_a_pipeline_reply_at_an_index_is_returned_and_nothing_is_resent(build):
    old, new = node("master"), node("replica")
    try:
        c = build(seeds(old, new))
        assert await c.ping()
        become(old, "replica", new)
        become(new, "master")
        pipe = (c._c if is_sync(c) else c).pipeline()
        pipe.set("a", "1").get("a")
        result = pipe.execute(raise_on_error=False)
        if inspect.isawaitable(result):
            result = await result
        assert isinstance(result[0], ruvio.ResponseError) and result[0].code == "READONLY"
        assert result[1] == "v"
        assert seen(old, "SET") == 1
        assert len(new.log) == 0
        assert seen(old, "ROLE") == 0
    finally:
        old.close()
        new.close()


async def test_another_error_is_not_followed(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second))
        assert await c.ping()
        first.handler = lambda n, a: err("LOADING Ruvio is loading the dataset in memory") if a[0].upper() == "SET" else handle(n, a)
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert info.value.code == "LOADING"
        assert seen(first, "ROLE") == 0
    finally:
        first.close()
        second.close()


async def test_a_refused_auth_during_the_search_fails_with_that_error(build):
    first, second = node("master"), node("master")
    try:
        c = build(seeds(first, second), password="pw")
        assert await c.ping()
        become(first, "replica", second)
        second.store["refuse_auth"] = "1"
        with pytest.raises(ruvio.ResponseError) as info:
            await c.set("k", "v")
        assert info.value.code == "WRONGPASS"
    finally:
        first.close()
        second.close()


async def test_a_hung_seed_costs_one_bounded_wait(build):
    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)  # accepts into the backlog and never answers
    live, other = node("master"), node("replica")
    try:
        c = build([live.addr, f"127.0.0.1:{silent.getsockname()[1]}", other.addr], connect_timeout=0.3)
        assert await c.ping()
        become(live, "replica", other)
        become(other, "master")
        started = time.monotonic()
        assert await c.set("k", "v") is True
        assert time.monotonic() - started < 3.0
        assert seen(other, "SET") == 1
    finally:
        silent.close()
        live.close()
        other.close()


# ------------------------------------------------------------------ coordinator name
async def test_a_coordinator_name_finds_the_primary_at_connect_and_after_a_failover(build):
    coordinator = node("names")
    one, two = node("master"), node("replica")
    try:
        become(coordinator, "names", one)
        c = build(seeds(coordinator), sentinel_master="main")
        assert await c.set("k", "1") is True
        assert (seen(one, "SET"), seen(two, "SET")) == (1, 0)
        assert coordinator.commands("SENTINEL") == [["SENTINEL", "get-master-addr-by-name", "main"]]

        become(one, "replica")
        become(two, "master")
        become(coordinator, "names", two)
        assert await c.set("k", "2") is True
        assert (seen(one, "SET"), seen(two, "SET")) == (2, 1)

        # after a dropped socket the coordinator is asked again
        become(one, "master")
        become(two, "replica")
        become(coordinator, "names", one)
        await lose_connection(c, two)
        assert await c.set("k", "3") is True
        assert (seen(one, "SET"), seen(two, "SET")) == (3, 1)
    finally:
        coordinator.close()
        one.close()
        two.close()


async def test_a_coordinator_that_names_a_replica_is_not_taken(build):
    coordinator = node("names")
    replica, real = node("replica"), node("master")
    try:
        become(coordinator, "names", replica)
        c = build(seeds(coordinator, f"127.0.0.1:{dead_port()}"), sentinel_master="main")
        with pytest.raises(ruvio.ProtocolError) as info:
            await c.ping()
        assert "main" in str(info.value)
        become(coordinator, "names", real)
        assert await c.set("k", "v") is True
        assert seen(real, "SET") == 1
    finally:
        coordinator.close()
        replica.close()
        real.close()


async def test_a_coordinator_that_names_no_primary_fails_the_connect(build):
    coordinator = node("master")  # answers SENTINEL with a nil reply
    try:
        c = build(seeds(coordinator), sentinel_master="main")
        with pytest.raises(ruvio.ProtocolError) as info:
            await c.ping()
        assert "main" in str(info.value)
    finally:
        coordinator.close()


async def test_a_coordinator_name_with_the_option_off_connects_to_the_address_as_given(build):
    plain = node("master")
    try:
        c = build(seeds(plain), sentinel_master="main", follow_primary=False)
        assert await c.ping()
        assert seen(plain, "SENTINEL") == 0
    finally:
        plain.close()


# ------------------------------------------------------------------ pub/sub and options
def test_pubsub_reconnect_follows_the_primary():
    first, second = node("master"), node("master")
    try:
        client = ruvio.Ruvio(seeds=seeds(first, second), connect_timeout=1.0, socket_timeout=5.0)
        client.ping()
        with client.pubsub() as ps:
            ps.subscribe("news")
            wait_for(lambda: seen(first, "SUBSCRIBE") == 1, "the first subscribe")
            become(first, "replica", second)
            ps._conn.close()  # the subscriber socket is gone
            ps.subscribe("more")  # reopens, asks for the primary and subscribes again there
            wait_for(lambda: seen(second, "SUBSCRIBE") == 2, "both subscribes on the primary")
            assert seen(first, "SUBSCRIBE") == 1
        client.close()
    finally:
        first.close()
        second.close()


def test_option_validation():
    with pytest.raises(ValueError):
        ruvio.Ruvio(sentinel_master="")
    with pytest.raises(ValueError):
        ruvio.Ruvio(unix_socket_path="/tmp/x.sock", sentinel_master="main")
    with pytest.raises(ValueError):
        ruvio.asyncio.Ruvio(sentinel_master="")
    assert ruvio.Ruvio()._conn.opts.follow_primary is True
    assert ruvio.Ruvio()._conn.opts.sentinel_master is None
    assert ruvio.Ruvio(follow_primary=False, sentinel_master="main")._conn.opts.sentinel_master is None


async def test_dedicated_keeps_the_options():
    c = ruvio.Ruvio(seeds=["127.0.0.1:1", "127.0.0.1:2"], follow_primary=False)
    assert c.dedicated()._conn.opts.follow_primary is False
    s = ruvio.Ruvio(host="127.0.0.1", port=1, sentinel_master="main")
    d = s.dedicated()
    assert d._conn.opts.sentinel_master == "main"
    assert d._conn.coord == [("127.0.0.1", 1)]


def test_role_and_sentinel_replies_are_read_as_documented():
    from ruvio._follow import is_readonly, parse_master_addr, parse_role

    assert parse_role(["master", 0, []]) == ("master", None)
    assert parse_role(["slave", "10.0.0.1", 7000, "connected", 0]) == ("slave", ("10.0.0.1", 7000))
    assert parse_role([b"slave", b"10.0.0.1", b"7000"]) == ("slave", ("10.0.0.1", 7000))
    assert parse_role(["slave", "", 7000]) == ("slave", None)
    assert parse_role(["slave", "10.0.0.1", 0]) == ("slave", None)
    assert parse_role(["slave"]) == ("slave", None)
    assert parse_role(["sentinel", ["m"]]) is None
    assert parse_role("master") is None
    assert parse_role([]) is None
    assert parse_master_addr(["10.0.0.1", "7000"]) == ("10.0.0.1", 7000)
    assert parse_master_addr(["10.0.0.1", 7000]) == ("10.0.0.1", 7000)
    assert parse_master_addr(None) is None
    assert parse_master_addr(["10.0.0.1"]) is None
    assert is_readonly(ruvio.ResponseError("READONLY"))
    assert is_readonly(ruvio.ResponseError("READONLY You can't write against a read only replica."))
    assert not is_readonly(ruvio.ResponseError("READONLYX nope"))
    assert not is_readonly(ruvio.ResponseError("LOADING"))


async def test_a_following_asyncio_client_returns_the_same_future_as_a_plain_one():
    import asyncio

    live, other = node("master"), node("replica")
    try:
        following = ruvio.asyncio.Ruvio(seeds=seeds(live, other), connect_timeout=1.0)
        plain = ruvio.asyncio.Ruvio(seeds=seeds(live, other), connect_timeout=1.0, follow_primary=False)
        try:
            assert following._st.follow and not plain._st.follow
            await following.connect()
            await plain.connect()
            f1, f2 = following.get("k"), plain.get("k")
            assert type(f1) is type(f2) and isinstance(f1, asyncio.Future)
            assert not asyncio.iscoroutine(f1)
            await asyncio.wait_for(asyncio.gather(f1, f2), DEADLINE)
        finally:
            await following.aclose()
            await plain.aclose()
    finally:
        live.close()
        other.close()


async def test_refused_futures_are_completed_in_place_after_one_shared_search():
    import asyncio

    old, new = node("master"), node("replica")
    try:
        c = ruvio.asyncio.Ruvio(seeds=seeds(old, new), connect_timeout=1.0)
        try:
            assert await c.ping()
            become(old, "replica", new)
            become(new, "master")
            futs = [c.set("k%d" % i, "v") for i in range(5)]  # pipelined: none awaited yet
            assert all(type(f) is asyncio.Future for f in futs)
            done = []
            futs[0].add_done_callback(lambda f: done.append(f.result()))
            cancelled = futs[1]
            cancelled.cancel()
            res = await asyncio.wait_for(asyncio.gather(*futs[2:]), DEADLINE)
            assert res == [True, True, True]
            await asyncio.wait_for(futs[0], DEADLINE)
            assert done == [True]
            assert cancelled.cancelled()
            assert seen(old, "SET") == 5 and seen(new, "SET") == 4
            assert seen(new, "ROLE") == 1  # one search served every refusal
        finally:
            await c.aclose()
    finally:
        old.close()
        new.close()
