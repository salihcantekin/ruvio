# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Reads from replicas against the replicated pair scripts/run_with_servers.sh starts: a 2-shard primary on
RUVIO_REPLICATED_ADDR and its 2-shard replica on RUVIO_REPLICA_ADDR (replica shard i listens on port + i).
Skipped when either is not set."""

from __future__ import annotations

import inspect
import os
import uuid

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER, await_for
from ruvio import _cluster as cl

PRIMARY = os.environ.get("RUVIO_REPLICATED_ADDR")
REPLICA = os.environ.get("RUVIO_REPLICA_ADDR")


def addresses():
    if not PRIMARY or not REPLICA:
        pytest.skip("RUVIO_REPLICATED_ADDR / RUVIO_REPLICA_ADDR not set")
    host, port = PRIMARY.rsplit(":", 1)
    rhost, rport = REPLICA.rsplit(":", 1)
    return host, int(port), rhost, int(rport)


async def maybe(x):
    return await x if inspect.isawaitable(x) else x


def get_commands(info) -> int:
    for line in str(info).splitlines():
        if line.startswith("get_commands:"):
            return int(line.split(":", 1)[1])
    raise AssertionError("INFO reports get_commands")


async def reads_through(kind, policy, expected):
    """Writes a key through a client with ``policy``, waits for the replica shard to copy it, and checks
    whether the GET reached that shard (``expected`` more GETs there)."""
    host, port, rhost, rport = addresses()
    key = f"python:replica-reads:{policy}:{uuid.uuid4().hex[:8]}"
    shard_port = rport + int(cl.key_slot(key) >= 8192)
    if kind == "sync":
        client = ruvio.Ruvio(host, port, cluster=True, read_policy=policy, parser=PARSER)
        direct = ruvio.Ruvio(rhost, shard_port, parser=PARSER)
    else:
        client = ruvio.asyncio.Ruvio(host, port, cluster=True, read_policy=policy, parser=PARSER)
        direct = ruvio.asyncio.Ruvio(rhost, shard_port, parser=PARSER)
    try:
        assert await maybe(client.set(key, "v1")) is True
        await await_for(lambda: direct.get(key) == "v1" if kind == "sync" else _copied(direct, key), "the replica to copy the key")
        before = get_commands(await maybe(direct.info()))
        assert await maybe(client.get(key)) == "v1"
        assert get_commands(await maybe(direct.info())) == before + expected
    finally:
        await maybe(client.delete(key))
        await maybe(client.aclose() if kind == "async" else client.close())
        await maybe(direct.aclose() if kind == "async" else direct.close())


async def _copied(direct, key):
    return await direct.get(key) == "v1"


@pytest.mark.parametrize("kind", ["sync", "async"])
@pytest.mark.parametrize("policy,expected", [("prefer-replica", 1), ("replica", 1), ("primary", 0)])
async def test_a_read_reaches_the_replica_only_under_a_replica_policy(kind, policy, expected):
    await reads_through(kind, policy, expected)
