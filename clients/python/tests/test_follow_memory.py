# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Memory of a client that follows a failover (two seeds, ``follow_primary`` on) against a client with one seed,
sync and asyncio.

The live tests need ``RUVIO_FAILOVER_ADDRS`` (primary first, from ``scripts/run_with_servers.sh``) and are skipped
without it. They measure with :mod:`tracemalloc`: the transient bytes one command needs (peak above the level
before it) and what is still allocated after a forced collection. The failover tests use scripted nodes and count
sockets, threads, tasks and futures, which must be back where they were after many failovers and after close.
"""

from __future__ import annotations

import asyncio
import gc
import os
import socket
import statistics
import sys
import threading
import time
import tracemalloc
from typing import Any, Callable, List, Tuple

import pytest

import ruvio
import ruvio.asyncio
import ruvio.asyncio.client as aclient
import ruvio.client as sclient
from conftest import await_for
from test_follow_failover import become, node, seeds

KINDS = ["sync", "async"]


def pair() -> List[Tuple[str, int]]:
    value = os.environ.get("RUVIO_FAILOVER_ADDRS")
    if not value:
        pytest.skip("RUVIO_FAILOVER_ADDRS not set")
    out = []
    for part in value.split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    assert len(out) == 2
    return out


def make(kind: str, addresses: List[str], **kw: Any) -> Any:
    kw.setdefault("connect_timeout", 2.0)
    if kind == "sync":
        kw.setdefault("socket_timeout", 5.0)
        return ruvio.Ruvio(seeds=addresses, **kw)
    return ruvio.asyncio.Ruvio(seeds=addresses, **kw)


async def call(kind: str, c: Any, name: str, *args: Any) -> Any:
    r = getattr(c, name)(*args)
    return await r if kind == "async" else r


async def close(kind: str, c: Any) -> None:
    if kind == "sync":
        c.close()
    else:
        await c.aclose()


async def live_pair(kind: str) -> Tuple[Any, Any]:
    """A client on one seed and a following client on both, the primary first."""
    ports = pair()
    # The pair may have swapped roles in an earlier test; the primary is the node that answers master.
    texts = [f"{h}:{p}" for h, p in ports]
    for first in range(2):
        probe = make(kind, [texts[first]])
        try:
            role = await call(kind, probe, "role")
        finally:
            await close(kind, probe)
        if role[0] in ("master", b"master"):
            break
    order = [texts[first], texts[1 - first]]
    return make(kind, [order[0]]), make(kind, order)


# ---- healthy path ------------------------------------------------------------------------------------


async def transient(kind: str, c: Any, body: Callable[[], Any], n: int) -> int:
    """Median over ``n`` calls of the bytes the call holds at its peak above what was held before it."""
    out = []
    for _ in range(n):
        base = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        r = body()
        if kind == "async":
            await r
        out.append(tracemalloc.get_traced_memory()[1] - base)
    return int(statistics.median(out))


# Measured (Python 3.13, three runs each): the peak above the level before the call is the same for one seed and
# for two, to the byte and in every run: get 307 (sync) / 727 (asyncio), set 369 / 789, mget of 50 keys 21 412 for
# both. Tracemalloc counts exactly, so the margin only has to absorb a different interpreter version; a frame
# copied or kept per command adds at least its own size (30-100 bytes) to the peak.
@pytest.mark.parametrize("kind", KINDS)
async def test_following_adds_no_transient_allocation_per_command(kind):
    single, follow = await live_pair(kind)
    try:
        for c in (single, follow):
            await call(kind, c, "set", "mem:a", "value-12345")
            for _ in range(300):
                await call(kind, c, "get", "mem:a")
        results = {}
        tracemalloc.start(1)
        try:
            for name, c in (("single", single), ("follow", follow)):
                get = await transient(kind, c, lambda c=c: c.get("mem:a"), 1000)
                put = await transient(kind, c, lambda c=c: c.set("mem:a", "value-12345"), 1000)
                many = await transient(kind, c, lambda c=c: c.mget(*["mem:k%d" % i for i in range(50)]), 300)
                results[name] = (get, put, many)
        finally:
            tracemalloc.stop()
        print(results)
        for i, what in enumerate(("get", "set", "mget of 50")):
            extra = results["follow"][i] - results["single"][i]
            assert extra <= 16, f"{what}: following peaks {extra} bytes above one seed: {results}"
    finally:
        await close(kind, single)
        await close(kind, follow)


# ---- retention ---------------------------------------------------------------------------------------


async def workload(kind: str, c: Any, rounds: int) -> None:
    keys = ["mem:k%d" % i for i in range(20)]
    for i in range(rounds):
        await call(kind, c, "set", "mem:a", "value-%d" % i)
        await call(kind, c, "get", "mem:a")
        await call(kind, c, "mget", *keys)
        if kind == "async":
            await asyncio.gather(*[c.get(k) for k in keys])
        else:
            p = c.pipeline()
            for k in keys:
                p.get(k)
            p.execute()
        if i % 10 == 0:
            await call(kind, c, "set", "mem:s", str(i))
            try:
                await call(kind, c, "incr", "mem:s2x")  # fine; and an error reply below
                await call(kind, c, "lpush", "mem:a", "x")  # WRONGTYPE
            except ruvio.ResponseError:
                pass


def ruvio_bytes(snapshot: "tracemalloc.Snapshot") -> int:
    """Bytes allocated by ruvio's own code that are still alive in ``snapshot``."""
    here = os.path.dirname(ruvio.__file__)
    keep = snapshot.filter_traces([tracemalloc.Filter(True, os.path.join(here, "*"))])
    return sum(s.size for s in keep.statistics("filename"))


def settled() -> Tuple[int, int, int]:
    """After a forced collection: bytes alive that ruvio's code allocated, all traced bytes, allocated blocks."""
    gc.collect()
    gc.collect()
    return ruvio_bytes(tracemalloc.take_snapshot()), tracemalloc.get_traced_memory()[0], sys.getallocatedblocks()


# After 200 rounds (about 1 400 commands) the memory is taken after a forced collection, then after each 300
# further rounds (about 2 100 commands each); the growth over the last two steps (about 4 200 commands) was 0-32
# bytes of ruvio's own allocations, 0.1-9 KB of all traced memory (interpreter and asyncio caches settling,
# also for one seed) and 20-70 allocated blocks, for either kind of client. A frame kept per command would add
# hundreds of KB, thousands of blocks and tens of KB of ruvio's own.
@pytest.mark.parametrize("kind", KINDS)
async def test_following_retains_nothing_over_many_commands(kind):
    single, follow = await live_pair(kind)
    try:
        for name, c in (("single", single), ("follow", follow)):
            await workload(kind, c, 200)
            tracemalloc.start(8)
            try:
                seen = [settled()]
                for _ in range(3):
                    await workload(kind, c, 300)
                    seen.append(settled())
            finally:
                tracemalloc.stop()
            print(name, kind, seen)
            own = seen[3][0] - seen[1][0]
            traced = seen[3][1] - seen[1][1]
            blocks = seen[3][2] - seen[1][2]
            assert own < 2 * 1024, f"{name}: ruvio's own allocations grew by {own} bytes: {seen}"
            assert traced < 64 * 1024, f"{name}: traced memory grew by {traced} bytes: {seen}"
            assert blocks < 1500, f"{name}: allocated blocks grew by {blocks}: {seen}"
        # Nothing is held for a pending reply or a refusal.
        if kind == "async":
            st = follow._st
            assert len(st.proto.pending) == 0 and st.refusals == [] and st.task is None
        else:
            assert follow._conn.multi is False and follow._conn.once is False
    finally:
        await close(kind, single)
        await close(kind, follow)


# ---- failover ----------------------------------------------------------------------------------------


NODE_PORTS: set = set()


def open_sockets() -> int:
    """Client-side sockets that are open in this process and connected to a scripted node (the nodes' own accepted
    sockets stay open in the node until it closes, so they are not counted)."""
    n = 0
    for o in gc.get_objects():
        if isinstance(o, socket.socket) and o.fileno() != -1:
            try:
                peer = o.getpeername()
                if isinstance(peer, tuple) and peer[1] in NODE_PORTS:
                    n += 1
            except OSError:
                pass
    return n


def census() -> Tuple[int, int, int, int]:
    gc.collect()
    tasks = len(asyncio.all_tasks()) if _loop_running() else 0
    futures = sum(1 for o in gc.get_objects() if isinstance(o, asyncio.Future))
    return open_sockets(), threading.active_count(), tasks, futures


def _loop_running() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


async def settle_to(want: Tuple[int, int, int, int], what: str, kind: str) -> None:
    """The scripted nodes close their side of a connection in their own threads, so wait (bounded) for the counts."""
    deadline = time.monotonic() + 5.0
    while True:
        got = census()
        # Futures of finished tasks can stay until the next collection; the socket and thread counts must match exactly.
        if got[:3] == want[:3] and got[3] <= want[3] + 2:
            return
        if time.monotonic() > deadline:
            pytest.fail(f"{what}: sockets, threads, tasks, futures are {got}, expected {want}")
        if kind == "async":
            await asyncio.sleep(0.02)
        else:
            time.sleep(0.02)


@pytest.fixture
def quick(monkeypatch):
    """The waits between the rounds of a READONLY search are short, so a whole search fits in a test."""
    monkeypatch.setattr(sclient, "WAITS", (0.01,) * 7)
    monkeypatch.setattr(aclient, "WAITS", (0.01,) * 7)


# Counts are exact; the bytes are what ruvio's own code still holds after the failovers, measured at the 10th
# and the 60th: 0-2 KB apart. Every failover swaps the roles, so the next write is refused, found the new
# primary through probe sockets, and is sent again (three writes at once, so three refusals are held together).
@pytest.mark.parametrize("kind", KINDS)
async def test_many_failovers_leave_no_sockets_tasks_or_memory_behind(kind, quick):
    a, b = node("master"), node("master")
    become(b, "replica", a)
    NODE_PORTS.update({a.port, b.port})
    idle = None
    c = None
    try:
        await asyncio.sleep(0.05) if kind == "async" else time.sleep(0.05)
        idle = census()
        c = make(kind, seeds(a, b))
        await call(kind, c, "set", "k", "0")
        master, other = a, b
        tracemalloc.start(8)
        warm = None
        sizes = []
        try:
            for i in range(1, 61):
                master, other = other, master
                become(master, "master")
                become(other, "replica", master)
                if kind == "async":
                    r = await asyncio.gather(c.set("k", str(i)), c.set("k", str(i)), c.set("k", str(i)))
                else:
                    r = [c.set("k", str(i)) for _ in range(3)]
                assert all(x is True for x in r), r
                a.log.clear()
                b.log.clear()
                if i == 10:
                    gc.collect()
                    warm = census()
                    sizes.append(ruvio_bytes(tracemalloc.take_snapshot()))
            gc.collect()
            sizes.append(ruvio_bytes(tracemalloc.take_snapshot()))
        finally:
            tracemalloc.stop()
        assert warm is not None
        await settle_to(warm, "after 60 failovers", kind)
        assert sizes[1] - sizes[0] < 8 * 1024, f"ruvio holds {sizes[1] - sizes[0]} more bytes: {sizes}"
        if kind == "async":
            st = c._st
            assert st.refusals == [] and st.task is None and len(st.proto.pending) == 0
        await close(kind, c)
        c = None
        await settle_to(idle, "after close", kind)
    finally:
        if c is not None:
            await close(kind, c)
        a.close()
        b.close()
        NODE_PORTS.clear()


@pytest.mark.parametrize("kind", KINDS)
async def test_searches_that_find_no_primary_or_hit_a_silent_seed_leave_nothing_open(kind, quick):
    a, b = node("replica"), node("replica")
    NODE_PORTS.update({a.port, b.port})
    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)  # accepts at the kernel and never answers
    NODE_PORTS.add(silent.getsockname()[1])
    try:
        await asyncio.sleep(0.05) if kind == "async" else time.sleep(0.05)
        idle = census()
        c = make(kind, seeds(a, b))
        for _ in range(15):
            with pytest.raises(ruvio.ResponseError) as info:
                await call(kind, c, "set", "k", "v")
            assert info.value.code == "READONLY"
        await close(kind, c)
        await settle_to(idle, "after searches that found no primary", kind)

        extra = {"socket_timeout": 0.2} if kind == "sync" else {}
        c = make(kind, [a.addr, f"127.0.0.1:{silent.getsockname()[1]}"], connect_timeout=0.2, **extra)
        started = time.monotonic()
        with pytest.raises(ruvio.ResponseError) as info:
            await call(kind, c, "set", "k", "v")
        assert info.value.code == "READONLY"
        assert time.monotonic() - started < 3.0
        await close(kind, c)
        await settle_to(idle, "after a seed that never answers", kind)
    finally:
        silent.close()
        a.close()
        b.close()
        NODE_PORTS.clear()


async def test_closing_an_asyncio_client_during_a_search_fails_the_held_commands_and_opens_nothing(monkeypatch):
    # Real waits (100 ms, 200 ms, ...): the search is sleeping between rounds when the client closes.
    a, b = node("master"), node("replica")
    become(b, "replica", 0)
    NODE_PORTS.update({a.port, b.port})
    c = None
    try:
        await asyncio.sleep(0.05)
        idle = census()
        c = ruvio.asyncio.Ruvio(seeds=seeds(a, b), connect_timeout=1.0)
        await c.set("k", "0")
        become(a, "replica", 0)  # nobody is primary now
        tasks = [asyncio.ensure_future(c.set("k", "y")) for _ in range(5)]
        await asyncio.sleep(0.3)
        assert c._st.task is not None, "the search was not running when the client closed"
        await c.aclose()
        results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
        assert all(isinstance(r, ruvio.RuvioError) for r in results), results
        await await_for(lambda: c._st.task is None, "the search task to end")
        assert c._st.proto is None, "closing the client during a search left it connected"
        assert c._st.refusals == []
        del tasks, results  # their futures are this test's own
        await settle_to(idle, "after closing during a search", "async")
    finally:
        if c is not None:
            await c.aclose()
        a.close()
        b.close()
        NODE_PORTS.clear()
