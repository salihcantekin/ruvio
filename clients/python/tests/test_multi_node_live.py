# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""A multi-node cluster scripts/run_with_servers.sh starts: node a (RUVIO_MULTINODE_A_ADDR, slots 0-8191), node b
(RUVIO_MULTINODE_B_ADDR, slots 8192-16383) and a node with an older map (RUVIO_MULTINODE_STALE_ADDR) that gives every
slot to node a. Skipped when any is not set."""

from __future__ import annotations

import inspect
import os

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER
from ruvio import _cluster as cl

NAMES = ("RUVIO_MULTINODE_A_ADDR", "RUVIO_MULTINODE_B_ADDR", "RUVIO_MULTINODE_STALE_ADDR")


def addresses():
    values = [os.environ.get(name) for name in NAMES]
    if not all(values):
        pytest.skip(" / ".join(NAMES) + " not set")
    return [(host, int(port)) for host, port in (value.rsplit(":", 1) for value in values)]


async def maybe(x):
    return await x if inspect.isawaitable(x) else x


@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_follows_moved_from_a_node_with_an_older_map(kind):
    a, b, stale = addresses()
    # node b's slot (12182) and node a's (5061)
    on_b, on_a = f"{{foo}}python:multinode:{kind}", f"{{bar}}python:multinode:{kind}"
    assert (cl.key_slot(on_b), cl.key_slot(on_a)) == (12182, 5061)
    module = ruvio if kind == "sync" else ruvio.asyncio

    # Seeded with the stale node, the client first sends node b's key to node a, which answers MOVED.
    client = module.Ruvio(*stale, cluster=True, parser=PARSER)
    try:
        assert await maybe(client.set(on_b, "b")) is True
        assert await maybe(client.set(on_a, "a")) is True
        assert await maybe(client.get(on_b)) == "b"
        assert await maybe(client.get(on_a)) == "a"
    finally:
        await maybe(client.aclose() if kind == "async" else client.close())

    # each key is on the node that owns it
    for node, key, value in ((b, on_b, "b"), (a, on_a, "a")):
        direct = module.Ruvio(*node, parser=PARSER)
        try:
            assert await maybe(direct.get(key)) == value
        finally:
            await maybe(direct.aclose() if kind == "async" else direct.close())
