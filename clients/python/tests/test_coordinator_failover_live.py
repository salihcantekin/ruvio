# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Clients follow a failover that real coordinators carry out. scripts/run_with_servers.sh starts a group of two
data nodes (RUVIO_COORDINATOR_GROUP_ADDRS, their pids in RUVIO_COORDINATOR_GROUP_PIDS) watched by three
`ruvio coordinator` processes (RUVIO_COORDINATOR_ADDRS, the group named RUVIO_COORDINATOR_MASTER). The test
kills the primary with SIGKILL, so the group serves one run of the script; the sync and asyncio clients share
that one kill. Skipped without the variables."""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from typing import Any, List

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER, wait_for
from test_failover_live import IO_TIMEOUT, Address, Raw, is_master

# The coordinators notice the kill after down_after_ms (1 s) and promote within a few ticks.
PATIENCE = 15.0
# Each client's first write after the kill comes within this, the seeds client included: the replica that
# refused is taken once it is promoted, not after the rounds run out (about 7 s).
FOLLOW_BOUND = 5.0


def addresses(name: str) -> List[Address]:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} not set")
    out = []
    for part in value.split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


async def test_clients_reach_the_new_primary_after_the_coordinators_fail_over():
    coordinators = addresses("RUVIO_COORDINATOR_ADDRS")
    members = addresses("RUVIO_COORDINATOR_GROUP_ADDRS")
    master = os.environ["RUVIO_COORDINATOR_MASTER"]
    pids = [int(pid) for pid in os.environ["RUVIO_COORDINATOR_GROUP_PIDS"].split(",")]
    old, new = (0, 1) if is_master(members[0]) else (1, 0)
    assert is_master(members[old]) and not is_master(members[new]), "the group does not have exactly one primary"

    # Through the coordinators and with the data nodes as seeds, each as a sync and an asyncio client.
    sync = {
        "sync sentinel_master": ruvio.Ruvio(
            seeds=coordinators, sentinel_master=master, parser=PARSER, socket_timeout=IO_TIMEOUT, connect_timeout=1.0
        ),
        "sync seeds": ruvio.Ruvio(seeds=members, parser=PARSER, socket_timeout=IO_TIMEOUT, connect_timeout=1.0),
    }
    asynchronous = {
        "asyncio sentinel_master": ruvio.asyncio.Ruvio(
            seeds=coordinators, sentinel_master=master, parser=PARSER, connect_timeout=1.0
        ),
        "asyncio seeds": ruvio.asyncio.Ruvio(seeds=members, parser=PARSER, connect_timeout=1.0),
    }
    tag = uuid.uuid4().hex[:8]
    keys = {name: f"coordinated:{tag}:{name}" for name in [*sync, *asynchronous]}
    on_new = Raw(members[new])
    try:
        for name, client in sync.items():
            assert client.set(keys[name], "before") is True
        for name, client in asynchronous.items():
            assert await client.set(keys[name], "before") is True
        # Only a replica that holds every acknowledged write is promoted.
        wait_for(
            lambda: all(on_new.send("GET", key) == "before" for key in keys.values()),
            "the replica to hold the first writes",
            PATIENCE,
        )

        os.kill(pids[old], signal.SIGKILL)
        started = time.monotonic()

        def write_sync(name: str) -> float:
            while True:
                try:
                    assert sync[name].set(keys[name], "after") is True
                    return time.monotonic() - started
                except (ruvio.ConnectionError, ruvio.ResponseError, OSError) as error:
                    assert time.monotonic() - started < PATIENCE, f"{name}: no write succeeded: {error!r}"
                time.sleep(0.05)

        async def write_async(name: str) -> float:
            while True:
                try:
                    assert await asynchronous[name].set(keys[name], "after") is True
                    return time.monotonic() - started
                except (ruvio.ConnectionError, ruvio.ResponseError, OSError) as error:
                    assert time.monotonic() - started < PATIENCE, f"{name}: no write succeeded: {error!r}"
                await asyncio.sleep(0.05)

        # Every client writes at once, so none waits for another.
        took: List[Any] = await asyncio.gather(
            *(asyncio.to_thread(write_sync, name) for name in sync),
            *(write_async(name) for name in asynchronous),
        )
        print("first write after the kill:", {name: f"{s:.2f} s" for name, s in zip([*sync, *asynchronous], took)})
        for name, s in zip([*sync, *asynchronous], took):
            assert s < FOLLOW_BOUND, f"{name}: the first write took {s:.2f} s"

        assert on_new.send("ROLE")[0] == "master"
        for key in keys.values():
            assert on_new.send("GET", key) == "after"
    finally:
        on_new.close()
        for client in sync.values():
            client.close()
        for client in asynchronous.values():
            await client.aclose()
