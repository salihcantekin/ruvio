"""TLS against the servers scripts/run_with_servers.sh starts: RUVIO_TLS_ADDR, RUVIO_TLS_CLIENT_AUTH_ADDR and the PEM
files RUVIO_TLS_CA, RUVIO_TLS_CLIENT_CERT and RUVIO_TLS_CLIENT_KEY. Skipped without them. Every case runs on the sync and
on the asyncio client."""

from __future__ import annotations

import os
import ssl

import pytest

import ruvio
import ruvio.asyncio

ADDR = os.environ.get("RUVIO_TLS_ADDR")
AUTH_ADDR = os.environ.get("RUVIO_TLS_CLIENT_AUTH_ADDR")
CA = os.environ.get("RUVIO_TLS_CA")
CERT = os.environ.get("RUVIO_TLS_CLIENT_CERT")
KEY = os.environ.get("RUVIO_TLS_CLIENT_KEY")

pytestmark = pytest.mark.skipif(not (ADDR and CA), reason="RUVIO_TLS_ADDR and RUVIO_TLS_CA are not set")


def _address(text):
    host, _, port = text.rpartition(":")
    return host, int(port)


def sync(addr=ADDR, **options):
    host, port = _address(addr)
    options.setdefault("socket_timeout", 10.0)
    options.setdefault("connect_timeout", 5.0)
    return ruvio.Ruvio(host, port, **options)


def aio(addr=ADDR, **options):
    host, port = _address(addr)
    options.setdefault("socket_timeout", 10.0)
    options.setdefault("connect_timeout", 5.0)
    return ruvio.asyncio.Ruvio(host, port, **options)


async def run(client, call):
    """Runs ``call(client)`` on either client kind and awaits the result when it is a coroutine."""
    result = call(client)
    if hasattr(result, "__await__"):
        result = await result
    return result


@pytest.fixture(params=["sync", "asyncio"])
def make(request):
    return sync if request.param == "sync" else aio


async def close(client):
    result = client.close()
    if hasattr(result, "__await__"):
        await result


async def test_a_verified_connection_runs_commands(make):
    client = make(tls=True, tls_ca_file=CA)
    try:
        assert await run(client, lambda c: c.set("tls:key", "value")) is True
        assert await run(client, lambda c: c.get("tls:key")) == "value"
        big = "x" * 300_000
        await run(client, lambda c: c.set("tls:big", big))
        assert await run(client, lambda c: c.get("tls:big")) == big
        # A blocking command widens the socket timeout and puts it back.
        assert await run(client, lambda c: c.blpop("tls:missing", 1)) is None
        assert await run(client, lambda c: c.get("tls:key")) == "value"
    finally:
        await close(client)


async def test_pubsub_runs_over_tls(make):
    client = make(tls=True, tls_ca_file=CA)
    try:
        ps = await run(client, lambda c: c.pubsub())
        await run(ps, lambda p: p.subscribe("tls-ch"))
        assert await run(client, lambda c: c.publish("tls-ch", "hi")) >= 1
        for _ in range(5):
            message = await run(ps, lambda p: p.get_message(timeout=5.0))
            assert message is not None
            if message.kind == "message":
                assert message.data == "hi"
                break
        else:
            pytest.fail("no message over TLS")
        await close(ps)
    finally:
        await close(client)


async def test_a_ready_ssl_context_is_used_as_it_is(make):
    context = ssl.create_default_context(cafile=CA)
    client = make(tls=context)
    try:
        assert await run(client, lambda c: c.ping()) == "PONG"
    finally:
        await close(client)


async def test_an_untrusted_certificate_is_refused(make):
    # The system roots do not know the test CA.
    client = make(tls=True)
    try:
        with pytest.raises(ruvio.ConnectionError, match="(?i)certificate"):
            await run(client, lambda c: c.ping())
    finally:
        await close(client)


async def test_a_name_the_certificate_lacks_is_refused(make):
    client = make(tls=True, tls_ca_file=CA, tls_server_name="elsewhere.example")
    try:
        with pytest.raises(ruvio.ConnectionError):
            await run(client, lambda c: c.ping())
    finally:
        await close(client)


async def test_a_dns_name_can_be_checked_while_connecting_by_address(make):
    client = make(tls=True, tls_ca_file=CA, tls_server_name="localhost")
    try:
        assert await run(client, lambda c: c.ping()) == "PONG"
    finally:
        await close(client)


async def test_skipping_verification_is_an_explicit_choice(make):
    client = make(tls=True, tls_insecure_skip_verify=True)
    try:
        assert await run(client, lambda c: c.ping()) == "PONG"
    finally:
        await close(client)


@pytest.mark.skipif(not (AUTH_ADDR and CERT and KEY), reason="client certificate servers are not set")
async def test_a_server_that_wants_a_client_certificate_gets_one(make):
    client = make(AUTH_ADDR, tls=True, tls_ca_file=CA, tls_cert_file=CERT, tls_key_file=KEY)
    try:
        assert await run(client, lambda c: c.ping()) == "PONG"
    finally:
        await close(client)


@pytest.mark.skipif(not AUTH_ADDR, reason="client certificate servers are not set")
async def test_a_missing_client_certificate_is_refused(make):
    client = make(AUTH_ADDR, tls=True, tls_ca_file=CA)
    try:
        with pytest.raises(ruvio.RuvioError):
            await run(client, lambda c: c.ping())
    finally:
        await close(client)


async def test_plain_tcp_to_a_tls_port_fails(make):
    client = make(socket_timeout=2.0)
    try:
        with pytest.raises(ruvio.RuvioError):
            await run(client, lambda c: c.ping())
    finally:
        await close(client)


def test_a_tls_option_without_tls_is_a_mistake():
    with pytest.raises(ValueError, match="need tls=True"):
        ruvio.Ruvio(tls_ca_file="ca.pem")
    with pytest.raises(ValueError, match="need tls=True"):
        ruvio.asyncio.Ruvio(tls_insecure_skip_verify=True)
    with pytest.raises(ValueError, match="unix_socket_path"):
        ruvio.Ruvio(tls=True, unix_socket_path="/tmp/ruvio.sock")


def test_tls_urls_turn_tls_on():
    assert ruvio.parse_url("ruvios://localhost:6380")["tls"] is True
    assert ruvio.parse_url("rediss://localhost")["tls"] is True
    assert "tls" not in ruvio.parse_url("ruvio://localhost")
