# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""A cluster client leaves a demoted primary after its READONLY reply, on real servers.
scripts/run_with_servers.sh starts two 2-shard nodes that can each be primary or replica
(RUVIO_CLUSTER_FAILOVER_ADDRS, shard 0 of each; their replication addresses in
RUVIO_CLUSTER_FAILOVER_REPLICATION; both comma separated, primary first). No coordinator runs: the
test swaps the roles itself with REPLICAOF, as a coordinator does. Skipped without the variables."""

from __future__ import annotations

import os
import time
import uuid
from typing import List, Tuple

import pytest

import ruvio
from conftest import await_for, wait_for
from test_failover_live import PATIENCE, Raw, close, is_master, kind, make, maybe  # noqa: F401 (kind is a fixture)

Address = Tuple[str, int]


def cluster_pair() -> Tuple[List[Address], List[Address]]:
    def split(name: str) -> List[Address]:
        value = os.environ.get(name)
        if not value:
            pytest.skip(f"{name} not set")
        out = []
        for part in value.split(","):
            host, port = part.rsplit(":", 1)
            out.append((host, int(port)))
        assert len(out) == 2, name
        return out

    return split("RUVIO_CLUSTER_FAILOVER_ADDRS"), split("RUVIO_CLUSTER_FAILOVER_REPLICATION")


async def test_a_cluster_client_reloads_its_slots_after_readonly_from_a_demoted_primary(kind):
    clients, replication = cluster_pair()
    # Whichever node is the primary now, so the test can run again on the same servers.
    old, new = (0, 1) if is_master(clients[0]) else (1, 0)
    tag = uuid.uuid4().hex[:8]
    # One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
    keys = [f"cluster-failover:{kind}:{tag}:{{a}}", f"cluster-failover:{kind}:{tag}:{{b}}"]
    client = make(kind, [clients[old]], cluster=True, follow_primary=False)
    on_old, on_new = Raw(clients[old]), Raw(clients[new])
    try:
        assert (client.is_cluster if kind == "sync" else await client.is_cluster()) is True
        for key in keys:
            assert await maybe(client.set(key, "before")) is True
        # A replica that still copies refuses to be promoted: wait until it holds the first writes.
        wait_for(lambda: all(on_new.send("GET", key) == "before" for key in keys), "the replica to hold the writes", PATIENCE)

        on_new.send("REPLICAOF", "NO", "ONE")
        on_old.send("REPLICAOF", replication[new][0], replication[new][1])
        # The demoted node names its new primary once that primary has accepted it.
        wait_for(lambda: on_old.send("ROLE")[0] == "slave", "the old primary to become a replica", PATIENCE)

        async def written() -> bool:
            try:
                return await maybe(client.set(keys[0], "after")) is True
            except ruvio.ResponseError as error:
                assert error.code == "READONLY", error
                return False

        started = time.monotonic()
        await await_for(written, "a write through the cluster client to succeed", 5.0)
        assert time.monotonic() - started < 5.0, time.monotonic() - started
        assert on_new.send("GET", keys[0]) == "after"
        # The other shard's slots moved too: no write is refused once the table is reloaded.
        for round in range(10):
            for key in keys:
                assert await maybe(client.set(key, f"v{round}")) is True
        assert all(on_new.send("GET", key) == "v9" for key in keys)
    finally:
        on_old.close()
        on_new.close()
        await close(kind, client)
