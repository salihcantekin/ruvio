"""The asyncio client's command timeout: no reply within ``socket_timeout`` fails the waiting commands and closes the
connection, and a blocking command's own wait is added to it. Runs against an in-process server, no Ruvio server."""

import asyncio
import time

import pytest

import ruvio
import ruvio.asyncio

pytestmark = pytest.mark.asyncio


async def _serve(reply_after: float, *, once: bool = False):
    """A server that answers every command with ``+OK`` after ``reply_after`` seconds (``None``: never)."""
    seen = []

    async def handle(reader, writer):
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                seen.append(data)
                if reply_after is None:
                    continue
                await asyncio.sleep(reply_after)
                writer.write(b"+OK\r\n" * data.count(b"*"))
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1], seen


async def test_a_silent_server_fails_the_command_after_the_timeout():
    server, port, _ = await _serve(None)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=0.2)
    try:
        start = time.monotonic()
        with pytest.raises(ruvio.TimeoutError):
            await client.get("k")
        assert 0.15 <= time.monotonic() - start < 2.0
    finally:
        await client.aclose()
        server.close()


async def test_every_command_waiting_on_the_connection_fails_together():
    server, port, _ = await _serve(None)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=0.2)
    try:
        results = await asyncio.gather(client.get("a"), client.get("b"), return_exceptions=True)
        assert all(isinstance(r, ruvio.TimeoutError) for r in results)
    finally:
        await client.aclose()
        server.close()


async def test_a_slow_reply_inside_the_timeout_succeeds():
    server, port, _ = await _serve(0.1)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=1.0)
    try:
        assert await client.set("k", "v") is True
    finally:
        await client.aclose()
        server.close()


async def test_a_blocking_command_gets_its_own_wait_on_top():
    server, port, _ = await _serve(0.6)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=0.2)
    try:
        with pytest.raises(ruvio.TimeoutError):
            await client.get("k")  # 0.6 s reply, 0.2 s allowed
        await client.execute_command("BLPOP", "k", 1)  # 1 s block + 0.2 s allowed
    finally:
        await client.aclose()
        server.close()


async def test_a_wait_of_zero_has_no_deadline():
    server, port, _ = await _serve(0.5)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=0.2)
    try:
        await client.execute_command("BLPOP", "k", 0)
    finally:
        await client.aclose()
        server.close()


async def test_none_turns_the_timeout_off():
    server, port, _ = await _serve(0.4)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=None)
    try:
        assert await client.set("k", "v") is True
    finally:
        await client.aclose()
        server.close()


async def test_the_next_command_opens_the_connection_again():
    server, port, _ = await _serve(0.5)
    client = ruvio.asyncio.Ruvio("127.0.0.1", port, socket_timeout=0.2)
    try:
        with pytest.raises(ruvio.TimeoutError):
            await client.set("k", "v")
        with pytest.raises(ruvio.TimeoutError):
            await client.set("k", "v")  # a new connection, which times out the same way
    finally:
        await client.aclose()
        server.close()
