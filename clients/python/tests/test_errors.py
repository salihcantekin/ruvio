"""The retry classification every client shares: which failures a retry can get past and which may have run."""

import asyncio

import pytest

from ruvio import (
    ConnectionError,
    ProtocolError,
    ResponseError,
    TimeoutError,
    is_transient,
    may_have_run,
)
from ruvio.errors import make_error

TRANSIENT_REPLIES = [
    "LOADING Ruvio is loading the dataset in memory",
    "MASTERDOWN Link with MASTER is down",
    "CLUSTERDOWN The cluster is down",
    "TRYAGAIN slot is moving",
    "READONLY You can't write against a read only replica.",
    "NOREPLICAS Not enough good replicas to write.",
]


@pytest.mark.parametrize("reply", TRANSIENT_REPLIES)
def test_retryable_replies_are_transient(reply):
    assert is_transient(make_error(reply))


@pytest.mark.parametrize(
    "reply", ["WRONGTYPE Operation against a key", "OOM command not allowed", "ERR syntax error", "READONLYX x", "LOAD"]
)
def test_other_replies_are_not_transient(reply):
    assert not is_transient(make_error(reply))


def test_connection_failures_are_transient_and_others_are_not():
    assert is_transient(ConnectionError("connection closed by server"))
    assert is_transient(TimeoutError("timed out waiting for the reply"))
    assert not is_transient(ProtocolError("bad frame"))
    assert not is_transient(asyncio.CancelledError())
    assert not is_transient(ValueError("bad"))


def test_only_a_refusing_server_reply_surely_did_not_run():
    assert not may_have_run(make_error("WRONGTYPE Operation against a key"))
    assert not may_have_run(make_error("TRYAGAIN slot is moving"))
    assert not may_have_run(ResponseError("READONLY replica"))
    assert may_have_run(make_error("NOREPLICAS Not enough good replicas to write."))
    assert may_have_run(make_error("NOREPLICAS"))
    assert may_have_run(ConnectionError("reset"))
    assert may_have_run(TimeoutError("slow"))
    assert may_have_run(ProtocolError("bad frame"))
    assert may_have_run(asyncio.CancelledError())
