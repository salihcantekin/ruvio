# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""CLIENT DURABILITY against the replicated primary on RUVIO_REPLICATED_ADDR (skipped when not set) and the
standalone server on RUVIO_TEST_ADDR, which has no replication and refuses it."""

from __future__ import annotations

import os
import uuid

import pytest

import ruvio
import ruvio.asyncio
from conftest import PARSER
from ruvio import ResponseError

PRIMARY = os.environ.get("RUVIO_REPLICATED_ADDR")
STANDALONE = os.environ.get("RUVIO_TEST_ADDR")


def primary():
    if not PRIMARY:
        pytest.skip("RUVIO_REPLICATED_ADDR not set")
    host, port = PRIMARY.rsplit(":", 1)
    return host, int(port)


def standalone():
    if not STANDALONE:
        pytest.skip("RUVIO_TEST_ADDR not set")
    host, port = STANDALONE.rsplit(":", 1)
    return host, int(port)


def test_sync_durability_option_writes_and_reads_back():
    host, port = primary()
    key = f"python:durability:{uuid.uuid4().hex[:8]}"
    c = ruvio.Ruvio(host, port, durability_replicas=1, durability_timeout_ms=5000, parser=PARSER)
    try:
        assert c.set(key, "v") is True
        assert c.get(key) == "v"
    finally:
        c.close()


async def test_async_durability_option_writes_and_reads_back():
    host, port = primary()
    key = f"python:durability:{uuid.uuid4().hex[:8]}"
    c = ruvio.asyncio.Ruvio(host, port, durability_replicas=1, durability_timeout_ms=5000, parser=PARSER)
    try:
        assert await c.set(key, "v") is True
        assert await c.get(key) == "v"
    finally:
        await c.aclose()


def test_sync_client_durability_method_on_the_replicated_primary():
    host, port = primary()
    c = ruvio.Ruvio(host, port, parser=PARSER)
    try:
        assert c.client_durability(1, 5000) is True
        assert c.set(f"python:durability:{uuid.uuid4().hex[:8]}", "v") is True
        assert c.client_durability(0) is True
    finally:
        c.close()


def test_sync_standalone_server_refuses_durability():
    host, port = standalone()
    c = ruvio.Ruvio(host, port, parser=PARSER)
    try:
        with pytest.raises(ResponseError, match="needs replication"):
            c.client_durability(1)
        with pytest.raises(ResponseError, match="needs replication"):
            c.client_durability(1, 100)
    finally:
        c.close()
    d = ruvio.Ruvio(host, port, durability_replicas=1, parser=PARSER)
    with pytest.raises(ResponseError, match="needs replication"):
        d.ping()


async def test_async_standalone_server_refuses_durability():
    host, port = standalone()
    c = ruvio.asyncio.Ruvio(host, port, parser=PARSER)
    try:
        with pytest.raises(ResponseError, match="needs replication"):
            await c.client_durability(1)
    finally:
        await c.aclose()
