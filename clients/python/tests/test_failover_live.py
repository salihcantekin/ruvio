# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""A client with two seeds follows a failover on real servers. scripts/run_with_servers.sh starts a pair that
can each be primary or replica (RUVIO_FAILOVER_ADDRS, their replication addresses in
RUVIO_FAILOVER_REPLICATION, both comma separated, primary first); no coordinator runs, the test swaps the roles
itself with REPLICAOF, as a coordinator does. Skipped without the variables."""

from __future__ import annotations

import os
import socket
import time
import uuid
from typing import Any, List, Tuple

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER, await_for, wait_for

PATIENCE = 10.0
IO_TIMEOUT = 5.0

Address = Tuple[str, int]


def pair() -> Tuple[List[Address], List[Address]]:
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

    return split("RUVIO_FAILOVER_ADDRS"), split("RUVIO_FAILOVER_REPLICATION")


class Raw:
    """A plain RESP socket: sends one command, reads one reply. Deliberately not the client under test."""

    def __init__(self, address: Address) -> None:
        self.sock = socket.create_connection(address, timeout=IO_TIMEOUT)
        self.buf = b""

    def close(self) -> None:
        self.sock.close()

    def _line(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n: int) -> bytes:
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def _reply(self) -> Any:
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n).decode()
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._reply() for _ in range(n)]
        raise AssertionError(line)

    def send(self, *args: Any) -> Any:
        parts = [str(a).encode() for a in args]
        self.sock.sendall(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts))
        return self._reply()


def is_master(address: Address) -> bool:
    raw = Raw(address)
    try:
        return raw.send("ROLE")[0] == "master"
    finally:
        raw.close()


async def maybe(x: Any) -> Any:
    import inspect

    return await x if inspect.isawaitable(x) else x


@pytest.fixture(params=["sync", "async"])
def kind(request):
    return request.param


def make(kind: str, seeds: List[Address], **kw: Any):
    if kind == "sync":
        return ruvio.Ruvio(seeds=seeds, parser=PARSER, socket_timeout=IO_TIMEOUT, **kw)
    return ruvio.asyncio.Ruvio(seeds=seeds, parser=PARSER, **kw)


async def close(kind: str, client: Any) -> None:
    if kind == "sync":
        client.close()
    else:
        await client.aclose()


async def test_a_client_follows_the_primary_after_the_roles_are_swapped(kind):
    clients, replication = pair()
    # Whichever node is the primary now, so the test can run again on the same servers.
    old, new = (0, 1) if is_master(clients[0]) else (1, 0)
    seeds = [clients[old], clients[new]]
    key = f"failover:{kind}:{uuid.uuid4().hex[:8]}"
    client = make(kind, seeds)
    stuck = make(kind, seeds, follow_primary=False)
    on_old, on_new = Raw(clients[old]), Raw(clients[new])
    try:
        assert await maybe(client.set(key, "before")) is True
        # A replica that still copies refuses to be promoted: wait until it holds the first write.
        wait_for(lambda: on_new.send("GET", key) == "before", "the replica to hold the first write", PATIENCE)

        on_new.send("REPLICAOF", "NO", "ONE")
        on_old.send("REPLICAOF", replication[new][0], replication[new][1])

        started = time.monotonic()
        assert await maybe(client.set(key, "after")) is True
        assert time.monotonic() - started < 5.0, time.monotonic() - started
        assert on_new.send("GET", key) == "after"

        # Without the option the same situation is the READONLY error.
        with pytest.raises(ruvio.ResponseError) as info:
            await maybe(stuck.set(key, "never"))
        assert info.value.code == "READONLY", info.value

        # A dropped socket: the reconnect starts at the old primary (now a replica) but takes the node that
        # answers master.
        client_id = await maybe(client.client_id())
        on_new.send("CLIENT", "KILL", "ID", client_id)

        async def closed() -> bool:
            try:
                await maybe(client.ping())
            except ruvio.ConnectionError:
                return True
            return False

        await await_for(closed, "the client to see its socket closed", PATIENCE)
        # Right after the reconnect, before any write: the client's own socket is on the primary.
        role = await maybe(client.execute_command("ROLE"))
        assert role[0] == "master", role
        assert await maybe(client.set(key, "again")) is True
        assert on_new.send("GET", key) == "again"
    finally:
        on_old.close()
        on_new.close()
        await close(kind, client)
        await close(kind, stuck)
