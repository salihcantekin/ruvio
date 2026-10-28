# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Connection, error-path and helper edge cases for the sync and asyncio clients, against scripted
fake servers (no Ruvio server needed): handshake, timeouts, dropped sockets, malformed replies,
reconnects, cancellation, locks, get_or_create, transactions, scan iterators and Pub/Sub."""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import types

import pytest

import ruvio
import ruvio.asyncio
from fakecluster import DROP, OK, FakeNode, array, bulk, err, integer
from ruvio import (
    ConnectionError as RConnectionError,
    ExecAbortError,
    LockError,
    ProtocolError,
    ResponseError,
    RuvioError,
    TimeoutError as RTimeoutError,
    WatchError,
)
from ruvio.client import _Options, parse_url


def script(table, default=OK):
    """Handler for a FakeNode: ``table`` maps "NAME" or "NAME SUB" (upper case) to bytes or a callable."""

    def handler(node, args):
        for key in (" ".join(args[:2]).upper(), args[0].upper()):
            if key in table:
                r = table[key]
                return r(node, args) if callable(r) else r
        return default

    return handler


@pytest.fixture
def node_factory():
    nodes = []

    def make(table=None, default=OK):
        n = FakeNode(script(table or {}, default))
        nodes.append(n)
        return n

    yield make
    for n in nodes:
        n.close()


def wait_connections(node, count, timeout=2.0):
    end = time.monotonic() + timeout
    while node.connections < count and time.monotonic() < end:
        time.sleep(0.005)
    return node.connections


def wait_log(node, count, timeout=2.0):
    end = time.monotonic() + timeout
    while len(node.log) < count and time.monotonic() < end:
        time.sleep(0.005)
    return [x[0] for x in node.log]


def host_port(node):
    return "127.0.0.1", node.port


# --------------------------------------------------------------------------- URL / options


def test_parse_url_forms():
    assert parse_url("ruvio://h:7/3") == {"host": "h", "port": 7, "db": 3}
    assert parse_url("redis://u:p%40ss@h") == {"host": "h", "port": 6379, "username": "u", "password": "p@ss"}
    assert parse_url("tcp://:secret@h:9") == {"host": "h", "port": 9, "password": "secret"}
    assert parse_url("ruvio://") == {"host": "127.0.0.1", "port": 6379}
    assert parse_url("ruvio://h/") == {"host": "h", "port": 6379}  # empty path: no db
    assert parse_url("ruvio://h/0") == {"host": "h", "port": 6379, "db": 0}
    assert parse_url("unix:///tmp/ruvio.sock") == {"unix_socket_path": "/tmp/ruvio.sock"}
    assert parse_url("unix:///tmp/r.sock") == {"unix_socket_path": "/tmp/r.sock"}
    with pytest.raises(ValueError, match="unsupported URL scheme"):
        parse_url("http://h")
    with pytest.raises(ValueError, match="unsupported URL scheme"):
        parse_url("h:6379")
    with pytest.raises(ValueError):
        parse_url("ruvio://h/notadb")


def test_from_url_kwargs_override_the_url():
    c = ruvio.Ruvio.from_url("ruvio://urlhost:1234/2", host="override", socket_timeout=3)
    assert (c._conn.opts.host, c._conn.opts.port, c._conn.opts.db, c._conn.opts.socket_timeout) == ("override", 1234, 2, 3)
    a = ruvio.asyncio.Ruvio.from_url("ruvio://urlhost:1234", db=5)
    assert (a._st.opts.host, a._st.opts.port, a._st.opts.db) == ("urlhost", 1234, 5)


def test_handshake_command_order():
    def frames(**kw):
        return [d for _, d in _Options(**kw).handshake()]

    assert frames() == []
    assert frames(db=0) == []  # db 0 needs no SELECT
    assert frames(password="p") == [b"*2\r\n$4\r\nAUTH\r\n$1\r\np\r\n"]
    assert frames(username="u", password="p") == [b"*3\r\n$4\r\nAUTH\r\n$1\r\nu\r\n$1\r\np\r\n"]
    assert frames(username="u") == []  # a user without a password sends nothing
    assert len(frames(password="p", db=2, client_name="n")) == 3
    d = _Options(host="h", port=1).as_dict()
    assert d["host"] == "h" and d["on_push"] is None and set(d) == set(_Options.__slots__)


# --------------------------------------------------------------------------- sync: connecting


def test_sync_handshake_runs_on_every_connect(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n), db=4, password="pw", username="me", client_name="nm")
    assert c.ping() is not None
    assert [x[0] for x in n.log] == ["AUTH", "SELECT", "CLIENT", "PING"]
    assert n.log[0] == ["AUTH", "me", "pw"] and n.log[1] == ["SELECT", "4"] and n.log[2] == ["CLIENT", "SETNAME", "nm"]
    c.close()
    c.ping()  # reopens and repeats the handshake
    assert [x[0] for x in n.log[4:]] == ["AUTH", "SELECT", "CLIENT", "PING"]
    c.close()


def test_sync_durability_option_is_sent_after_the_handshake(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n), db=4, client_name="nm", durability_replicas=1, durability_timeout_ms=250)
    c.ping()
    assert n.log[1:3] == [["CLIENT", "SETNAME", "nm"], ["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"]]
    c.close()
    c.ping()  # a reconnect sends it again
    assert n.log[-2] == ["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"]
    plain = node_factory()
    d = ruvio.Ruvio(*host_port(plain), durability_replicas=2)
    d.ping()
    assert plain.log[0] == ["CLIENT", "DURABILITY", "REPLICAS", "2"]
    off = node_factory()
    ruvio.Ruvio(*host_port(off)).ping()
    assert off.commands("CLIENT") == []  # not set: nothing is sent


def test_sync_durability_refused_on_connect_fails_the_connect(node_factory):
    n = node_factory({"CLIENT DURABILITY": err("ERR CLIENT DURABILITY needs replication on this server")})
    c = ruvio.Ruvio(*host_port(n), durability_replicas=1)
    with pytest.raises(ResponseError, match="needs replication"):
        c.ping()
    assert c._conn.sock is None


def test_sync_handshake_error_closes_the_socket(node_factory):
    n = node_factory({"AUTH": err("WRONGPASS invalid username-password pair")})
    c = ruvio.Ruvio(*host_port(n), password="bad")
    with pytest.raises(ResponseError, match="WRONGPASS"):
        c.ping()
    assert c._conn.sock is None  # no half-open connection is kept
    with pytest.raises(ResponseError):
        c.ping()  # and every command retries the handshake
    assert len(n.commands("AUTH")) == 2


def test_sync_handshake_connection_dropped(node_factory):
    n = node_factory({"AUTH": DROP})
    c = ruvio.Ruvio(*host_port(n), password="x")
    with pytest.raises(RConnectionError):
        c.ping()
    assert c._conn.sock is None


def test_sync_lazy_false_connects_in_constructor(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n), lazy=False)
    assert wait_connections(n, 1) == 1 and c._conn.sock is not None
    assert c.connect() is c and wait_connections(n, 1) == 1  # already connected: no second socket
    c.close()
    assert c.connect() is c and wait_connections(n, 2) == 2
    c.close()


def test_sync_connection_refused_raises_connection_error():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nobody listens here now
    c = ruvio.Ruvio("127.0.0.1", port)
    with pytest.raises(RConnectionError, match="cannot connect"):
        c.ping()
    with pytest.raises(RConnectionError):
        ruvio.Ruvio("127.0.0.1", port, lazy=False)
    with pytest.raises(RConnectionError):
        ruvio.Ruvio(unix_socket_path="/nonexistent/ruvio.sock").ping()


def test_sync_connect_timeout_is_a_timeout_error(monkeypatch):
    class Hung:
        def __init__(self, *a):
            pass

        def settimeout(self, t):
            pass

        def connect(self, path):
            raise socket.timeout("timed out")

    def hung_connect(*a, **k):
        raise socket.timeout("timed out")

    fake = types.SimpleNamespace(
        socket=Hung, AF_UNIX=socket.AF_UNIX, SOCK_STREAM=socket.SOCK_STREAM, create_connection=hung_connect,
        IPPROTO_TCP=socket.IPPROTO_TCP, TCP_NODELAY=socket.TCP_NODELAY, timeout=socket.timeout,
    )
    monkeypatch.setattr("ruvio.client.socket", fake)
    with pytest.raises(RTimeoutError, match="timed out"):
        ruvio.Ruvio("10.255.255.1", 9, connect_timeout=0.01).ping()
    with pytest.raises(RTimeoutError):
        ruvio.Ruvio(unix_socket_path="/tmp/whatever.sock", connect_timeout=0.01).ping()
    assert issubclass(RTimeoutError, RConnectionError)  # one except clause handles both


def test_sync_unix_socket_round_trip(tmp_path):
    path = str(tmp_path / "r.sock")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(1)

    def serve():
        conn, _ = srv.accept()
        conn.recv(100)
        conn.sendall(b"+PONG\r\n")
        conn.close()

    threading.Thread(target=serve, daemon=True).start()
    try:
        with ruvio.Ruvio(unix_socket_path=path) as c:
            assert c.ping() is not None
    finally:
        srv.close()
        os.unlink(path)


# --------------------------------------------------------------------------- sync: failures mid-command


def test_sync_timeout_closes_the_socket_and_the_next_command_reconnects(node_factory):
    slow = {"on": True}

    def maybe_slow(node, args):
        if slow["on"]:
            time.sleep(0.4)
        return b"+PONG\r\n"

    n = node_factory({"PING": maybe_slow})
    c = ruvio.Ruvio(*host_port(n), socket_timeout=0.1)
    with pytest.raises(RTimeoutError, match="timed out waiting for the reply"):
        c.ping()
    assert c._conn.sock is None
    slow["on"] = False
    time.sleep(0.5)  # let the stale reply of the first connection die with it
    assert c.ping() is not None
    assert n.connections == 2
    c.close()


def test_sync_socket_timeout_defaults_to_thirty_seconds():
    assert ruvio.Ruvio()._conn.opts.socket_timeout == 30.0
    assert ruvio.Ruvio(socket_timeout=None)._conn.opts.socket_timeout is None


def test_sync_blocking_command_waits_its_own_timeout_on_top(node_factory):
    def blpop(node, args):
        time.sleep(1.0)  # an empty list: nil when BLPOP's 1 s runs out
        return b"*-1\r\n"

    n = node_factory({"BLPOP": blpop})
    c = ruvio.Ruvio(*host_port(n), socket_timeout=0.2)
    started = time.monotonic()
    assert c.blpop("k", 1) is None
    assert 0.9 <= time.monotonic() - started < 3
    assert c._conn.sock.gettimeout() == 0.2  # the plain limit is back
    assert len(c.pipeline().blpop("k", 1).execute()) == 1  # a pipeline gets the wait too
    assert c._conn.sock.gettimeout() == 0.2
    c.close()


def _enc(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a.encode()) for a in args)


@pytest.mark.parametrize(
    "args,want",
    [
        (("GET", "k"), 0.0),
        (("SET", "k", "v"), 0.0),
        (("blpop", "a", "b", "1"), 1.0),
        (("BLPOP", "k", "0"), -1.0),
        (("BLPOP", "k", "0.5"), 0.5),
        (("BLPOP", "k", "soon"), -1.0),
        (("BLPOP", "k", "-1"), -1.0),
        (("BRPOP", "k", "2"), 2.0),
        (("BZPOPMIN", "k", "1.5"), 1.5),
        (("BZPOPMAX", "k", "3"), 3.0),
        (("BRPOPLPUSH", "a", "b", "4"), 4.0),
        (("BLMOVE", "a", "b", "LEFT", "RIGHT", "0.25"), 0.25),
        (("BLMPOP", "2", "2", "a", "b", "LEFT"), 2.0),
        (("BZMPOP", "0", "1", "z", "MIN"), -1.0),
        (("XREAD", "STREAMS", "s", "$"), 0.0),
        (("XREAD", "COUNT", "1", "BLOCK", "700", "STREAMS", "s", "$"), 0.7),
        (("XREAD", "BLOCK", "0", "STREAMS", "s", "$"), -1.0),
        (("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"), 0.0),
        (("XREADGROUP", "GROUP", "BLOCK", "c", "STREAMS", "s", ">"), 0.0),
        (("XREADGROUP", "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">"), 0.05),
        (("WAIT", "1", "100"), 0.1),
        (("WAIT", "1", "0"), -1.0),
        (("WAITAOF", "1", "0", "200"), 0.2),
        (("FILLGET", "k", "1000"), 0.0),
        (("FILLGET", "k", "1000", "WAIT", "300"), 0.3),
        (("FILLGET", "k", "1000", "WAIT", "0"), -1.0),
        (("MONITOR",), -1.0),
        (("SUBSCRIBE", "news"), -1.0),
        (("PSUBSCRIBE", "n*"), -1.0),
        (("SSUBSCRIBE", "news"), -1.0),
        (("PUBLISH", "news", "x"), 0.0),
        (("BLPOP",), -1.0),
    ],
)
def test_blocking_wait_table(args, want):
    from ruvio._blocking import FOREVER, blocking_wait, frames_wait

    assert FOREVER == -1.0
    assert blocking_wait(_enc(*args)) == want
    assert frames_wait(_enc(*args)) == want


def test_blocking_wait_pipelines():
    from ruvio._blocking import FOREVER, blocking_wait, frames_wait, pipeline_wait

    frames = [_enc("GET", "k"), _enc("BLPOP", "k", "2"), _enc("XREAD", "BLOCK", "100", "STREAMS", "s", "$")]
    assert pipeline_wait(frames) == 2.0
    assert frames_wait(b"".join(frames)) == 2.0
    assert pipeline_wait([_enc("BLPOP", "k", "2"), _enc("BLPOP", "k", "0")]) == FOREVER
    assert frames_wait(_enc("BLPOP", "k", "2") + _enc("BLPOP", "k", "0")) == FOREVER
    assert pipeline_wait([_enc("GET", "k")]) == 0.0
    assert blocking_wait(b"garbage") == 0.0
    assert frames_wait(b"garbage") == 0.0
    assert blocking_wait(ruvio._resp.encode("BLPOP", ("k", 1.5))) == 1.5


def test_sync_server_closes_connection_mid_command(node_factory):
    n = node_factory({"GET": DROP})
    c = ruvio.Ruvio(*host_port(n))
    with pytest.raises(RConnectionError, match="closed by server"):
        c.get("k")
    assert c._conn.sock is None
    n.handler = script({})
    assert c.set("k", "v") is True  # reconnects transparently on the next command
    c.close()


def test_sync_socket_error_on_send_becomes_connection_error(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n))
    c.ping()
    c._conn.sock.close()  # the next sendall fails with an OSError (EBADF)
    with pytest.raises(RConnectionError):
        c.get("k")
    assert c._conn.sock is None
    assert c.ping() is not None
    c.close()


def test_sync_error_reply_does_not_close_the_connection(node_factory):
    n = node_factory({"GET": err("WRONGTYPE nope")})
    c = ruvio.Ruvio(*host_port(n))
    with pytest.raises(ruvio.WrongTypeError):
        c.get("k")
    assert c._conn.sock is not None
    c.close()


def test_sync_on_push_callback_and_its_failure(node_factory):
    seen = []
    push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n"
    n = node_factory({"GET": push + bulk("v")})
    c = ruvio.Ruvio(*host_port(n), on_push=seen.append)
    assert c.get("k") == "v"
    assert seen == [["invalidate", ["k"]]]
    c.close()
    # no callback: pushes are skipped silently
    c2 = ruvio.Ruvio(*host_port(n))
    assert c2.get("k") == "v"
    c2.close()

    def boom(_):
        raise RuntimeError("callback bug")

    c3 = ruvio.Ruvio(*host_port(n), on_push=boom)
    with pytest.raises(RuntimeError, match="callback bug"):
        c3.get("k")
    assert c3._conn.sock is None  # the stream position is unknown, so the socket is dropped
    c4 = ruvio.Ruvio(*host_port(n), on_push=boom)
    with pytest.raises(RuntimeError):
        c4.pipeline().get("k").execute()
    assert c4._conn.sock is None


def test_sync_malformed_reply_is_a_protocol_error(node_factory):
    n = node_factory({"GET": b"?garbage\r\n"})
    c = ruvio.Ruvio(*host_port(n))
    with pytest.raises(ProtocolError):
        c.get("k")
    assert c._conn.sock is None


def test_sync_conversion_error_surfaces_and_keeps_the_stream_aligned(node_factory):
    n = node_factory({"FILLGET": b"*0\r\n", "PING": b"+PONG\r\n"})
    c = ruvio.Ruvio(*host_port(n))
    with pytest.raises(IndexError):
        c.fillget("k", 10)
    assert c.ping() is not None  # the reply was consumed, the connection is still in sync
    c.close()


def test_sync_pipeline_failures(node_factory):
    mode = {"m": "ok"}

    def ping(node, args):
        if mode["m"] == "drop":
            return DROP
        if mode["m"] == "slow":
            time.sleep(0.4)
        return b"+PONG\r\n"

    n = node_factory({"PING": ping})
    c = ruvio.Ruvio(*host_port(n), socket_timeout=0.1)
    assert len(c.pipeline().ping().ping().execute()) == 2
    assert c.pipeline().execute() == []  # empty pipeline sends nothing
    mode["m"] = "slow"
    with pytest.raises(RTimeoutError, match="pipeline replies"):
        c.pipeline().ping().ping().execute()
    assert c._conn.sock is None
    mode["m"] = "drop"
    with pytest.raises(RConnectionError):
        c.pipeline().ping().execute()
    mode["m"] = "ok"
    c._conn.open()
    c._conn.sock.close()
    with pytest.raises(RConnectionError):
        c.pipeline().ping().execute()
    assert c.pipeline().ping().execute() == ["PONG"]
    c.close()


def test_sync_pipeline_context_manager_and_reset(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n))
    with c.pipeline() as p:
        p.set("a", 1)
        assert len(p) == 1
    assert len(p) == 0 and not n.commands("SET")  # leaving without execute() discards the queue
    p = c.pipeline()
    p.set("a", 1).set("b", 2)
    assert p.execute() == [True, True] and len(p) == 0
    c.close()


def test_sync_context_manager_raw_and_dedicated(node_factory):
    n = node_factory({"GET": bulk("v")})
    with ruvio.Ruvio(*host_port(n), db=2, client_name="x", decode_responses=True) as c:
        assert c.get("k") == "v"
        assert c.raw.get("k") == b"v"
        assert c.get("k") == "v"  # the raw view flipped the decode flag only for its own calls
        d = c.dedicated()
        assert d is not c and d._conn is not c._conn and d._decode is True
        assert (d._conn.opts.db, d._conn.opts.client_name) == (2, "x")
        d.close()
    assert c._conn.sock is None
    c.close()  # closing twice is harmless


def test_sync_auth_select_and_setname_are_remembered(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n))
    assert c.auth("pw", "user") is True and c.select(5) is True and c.client_setname("cn") is True
    o = c._conn.opts
    assert (o.password, o.username, o.db, o.client_name) == ("pw", "user", 5, "cn")
    c.close()
    c.ping()
    assert n.log[-4:-1] == [["AUTH", "user", "pw"], ["SELECT", "5"], ["CLIENT", "SETNAME", "cn"]]
    c.close()


# --------------------------------------------------------------------------- sync: transaction


def test_sync_transaction_retries_on_watch_error_then_gives_up(node_factory):
    execs = []

    def exec_(node, args):
        execs.append(1)
        return b"*-1\r\n" if len(execs) < 3 else array([OK])

    n = node_factory({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": exec_, "UNWATCH": err("ERR unwatch broke")})
    c = ruvio.Ruvio(*host_port(n))
    calls = []

    def fn(pipe):
        calls.append(1)
        pipe.set("k", 1)

    assert c.transaction(fn, "k") == [True]
    assert len(calls) == 3 and len(n.commands("WATCH")) == 3
    assert len(n.commands("UNWATCH")) == 3  # an error from UNWATCH is swallowed

    n.handler = script({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": b"*-1\r\n"})
    with pytest.raises(RuvioError, match="retries exhausted"):
        c.transaction(fn, "k", retries=2)
    # no watched keys: no WATCH / UNWATCH at all
    n.log.clear()
    with pytest.raises(RuvioError, match="retries exhausted"):
        c.transaction(fn, retries=1)
    assert not n.commands("WATCH") and not n.commands("UNWATCH")
    with pytest.raises(RuvioError, match="retries exhausted"):
        c.transaction(fn, "k", retries=0)  # zero attempts
    c.close()


def test_sync_transaction_propagates_other_errors_and_still_unwatches(node_factory):
    n = node_factory({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": b"*-1\r\n"})
    c = ruvio.Ruvio(*host_port(n))

    def fn(pipe):
        raise KeyError("user code failed")

    with pytest.raises(KeyError):
        c.transaction(fn, "k")
    assert len(n.commands("UNWATCH")) == 1
    c.close()


# --------------------------------------------------------------------------- sync: helpers


def fill(state, *rest):
    return array([bulk(state), *[bulk(x) for x in rest]])


def test_sync_get_or_create_paths(node_factory):
    n = node_factory({"FILLSET": integer(1)})
    c = ruvio.Ruvio(*host_port(n))

    n.handler = script({"FILLGET": fill("HIT", "cached")})
    assert c.get_or_create("k", lambda: "never", 1000) == "cached"

    n.handler = script({"FILLGET": fill("FILL", "tok"), "FILLSET": integer(1)})
    assert c.get_or_create("k", lambda: "built", 1000, swr_ms=50) == "built"
    assert n.commands("FILLSET")[-1] == ["FILLSET", "k", "built", "1000", "tok", "SWR", "50"]

    n.handler = script({"FILLGET": fill("STALE", "old", "tok"), "FILLSET": integer(1)})
    assert c.get_or_create("k", lambda: b"fresh", 1000) == b"fresh"  # the refresher builds a new one

    n.handler = script({"FILLGET": fill("STALE", "old")})
    assert c.get_or_create("k", lambda: "never", 1000) == "old"  # somebody else refreshes: serve stale

    n.handler = script({"FILLGET": fill("BUSY")})
    t0 = time.monotonic()
    with pytest.raises(RTimeoutError, match="still being filled"):
        c.get_or_create("k", lambda: "never", 1000, timeout=0.15, poll_ms=10)
    assert 0.1 < time.monotonic() - t0 < 2
    assert len(n.commands("FILLGET")) > 4  # it polled with backoff, not once

    # BUSY first, then the value shows up
    replies = [fill("BUSY"), fill("BUSY"), fill("HIT", "late")]
    n.handler = script({"FILLGET": lambda node, args: replies.pop(0)})
    assert c.get_or_create("k", lambda: "never", 1000, poll_ms=1) == "late"

    # a factory failure leaves the fill token to expire; the error reaches the caller
    n.handler = script({"FILLGET": fill("FILL", "tok")})

    def bad():
        raise ValueError("factory failed")

    with pytest.raises(ValueError):
        c.get_or_create("k", bad, 1000)
    c.close()


def test_sync_idempotent_states(node_factory):
    n = node_factory({"IDEM": lambda node, a: array([bulk("acquired")]) if a[2] == "BEGIN" else integer(1)})
    c = ruvio.Ruvio(*host_port(n))
    assert c.idempotent("k", "fp", 1000, lambda: "res", owner="me") == ("executed", "res")
    assert n.commands("IDEM")[-1] == ["IDEM", "k", "COMPLETE", "fp", "me", "res"]
    n.handler = script({"IDEM": array([bulk("completed"), bulk("stored")])})
    assert c.idempotent("k", "fp", 1000, lambda: "never") == ("completed", "stored")
    n.handler = script({"IDEM": array([bulk("pending")])})
    assert c.idempotent("k", "fp", 1000, lambda: "never") == ("pending", None)
    n.handler = script({"IDEM": array([bulk("conflict")])})
    assert c.idempotent("k", "fp", 1000, lambda: "never") == ("conflict", None)
    c.close()


def pages(*pgs):
    """Handler returning the given (cursor, items-bytes) pages one by one; the last repeats."""
    state = {"i": 0}

    def h(node, args):
        cur, items = pgs[min(state["i"], len(pgs) - 1)]
        state["i"] += 1
        return array([bulk(cur), array(items)])

    return h


def test_sync_scan_iterators_walk_every_page(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n))
    n.handler = script({"SCAN": pages(("7", [bulk("a"), bulk("b")]), ("0", [bulk("c")]))})
    assert list(c.scan_iter(match="*", count=2, type="string")) == ["a", "b", "c"]
    assert n.commands("SCAN")[0] == ["SCAN", "0", "MATCH", "*", "COUNT", "2", "TYPE", "string"]
    assert n.commands("SCAN")[1][1] == "7"  # the cursor from page one is sent for page two
    n.handler = script({"SSCAN": pages(("3", [bulk("m1")]), ("0", [bulk("m2")]))})
    assert list(c.sscan_iter("s", "m*", 5)) == ["m1", "m2"]
    n.handler = script({"HSCAN": pages(("3", [bulk("f1"), bulk("v1")]), ("0", [bulk("f2"), bulk("v2")]))})
    assert list(c.hscan_iter("h")) == [("f1", "v1"), ("f2", "v2")]
    n.handler = script({"ZSCAN": pages(("3", [bulk("m1"), bulk("1.5")]), ("0", [bulk("m2"), bulk("2")]))})
    assert list(c.zscan_iter("z", count=1)) == [("m1", 1.5), ("m2", 2.0)]
    n.handler = script({"SCAN": pages(("0", []))})
    assert list(c.scan_iter()) == []  # an empty page with cursor 0 ends the walk
    c.close()


# --------------------------------------------------------------------------- sync: lock and scopes


def test_sync_lock_edge_cases(node_factory):
    state = {"held": False}

    def set_(node, args):
        if "NX" in args and state["held"]:
            return b"$-1\r\n"
        if "NX" in args:
            state["held"] = True
        return OK

    n = node_factory({"SET": set_, "DELEX": integer(1), "GET": b"$-1\r\n"})
    c = ruvio.Ruvio(*host_port(n))
    lk = c.lock("job", ttl_ms=500, retry_ms=1)
    assert lk.renew() is False and lk.release() is False and lk.owned() is False  # not acquired yet
    assert lk.acquire() is True
    assert lk.renew() is True and lk.renew(900) is True
    assert n.commands("SET")[-1][-2:] == ["PX", "900"]
    assert lk.owned() is False  # fake GET returns nil: somebody else owns it now
    other = c.lock("job", blocking=False)
    assert other.acquire() is False  # held, not blocking
    t0 = time.monotonic()
    assert c.lock("job", timeout=0.05, retry_ms=5).acquire() is False
    assert time.monotonic() - t0 >= 0.05
    assert c.lock("job").acquire(blocking=False) is False
    assert c.lock("job", blocking=False).acquire(blocking=True, timeout=0.02) is False
    with pytest.raises(LockError, match="could not acquire lock"):
        with c.lock("job", blocking=False):
            pass
    assert lk.release() is True and lk.token is None
    n.handler = script({"SET": OK, "DELEX": integer(0)})
    with c.lock("job2") as held:
        assert held.token is not None
    assert held.token is None  # __exit__ released it; DELEX returning 0 is not an error
    c.close()


def test_sync_scopes(node_factory):
    n = node_factory({"LEASE": integer(11), "SEMAPHORE": integer(5), "RELEASE": integer(1)})
    c = ruvio.Ruvio(*host_port(n))
    with c.lease_scope("l", 1000) as tok:
        assert tok == 11
    assert n.commands("RELEASE")[-1] == ["RELEASE", "l", "11"]
    with c.semaphore_scope("s", 3, 1000) as tok:
        assert tok == 5
    assert n.commands("SEMAPHORE")[-1] == ["SEMAPHORE", "s", "3", "1000"]
    n.handler = script({"LEASE": b"$-1\r\n", "SEMAPHORE": b"_\r\n"})
    with pytest.raises(LockError, match="lease 'l' is not available"):
        with c.lease_scope("l", 1000):
            pass
    with pytest.raises(LockError, match="semaphore 's' is not available"):
        with c.semaphore_scope("s", 1, 1000):
            pass
    scope = c.lease_scope("l", 1000)
    scope.__exit__(None, None, None)  # exiting a scope that never entered releases nothing
    assert len(n.commands("RELEASE")) == 2
    c.close()


# --------------------------------------------------------------------------- sync: pubsub


def test_sync_pubsub_messages_and_kinds(node_factory):
    sub = lambda kind: (lambda node, a: array([bulk(kind), bulk(a[1] if len(a) > 1 else None), integer(1)]))  # noqa: E731
    n = node_factory({
        "SUBSCRIBE": lambda node, a: sub("subscribe")(node, a) + array([bulk("message"), bulk(a[1]), bulk("hi")]),
        "PSUBSCRIBE": lambda node, a: sub("psubscribe")(node, a)
        + array([bulk("pmessage"), bulk(a[1]), bulk("ch"), bulk("pm")]),
        "SSUBSCRIBE": lambda node, a: sub("ssubscribe")(node, a) + array([bulk("smessage"), bulk(a[1]), bulk("sm")]),
        "PING": array([bulk("pong"), bulk("")]),
        "UNSUBSCRIBE": sub("unsubscribe"),
    })
    c = ruvio.Ruvio(*host_port(n))
    ps = c.pubsub()
    assert ps.get_message(timeout=0) is None  # connects lazily, nothing to read yet
    ps.subscribe("a")
    ps.psubscribe("p*")
    ps.ssubscribe("s")
    kinds = []
    for _ in range(6):
        m = ps.get_message(timeout=2)
        kinds.append((m.kind, m.channel, m.data, m.pattern))
    assert ("subscribe", "a", 1, None) in kinds
    assert ("message", "a", "hi", None) in kinds
    assert ("pmessage", "ch", "pm", "p*") in kinds
    assert ("smessage", "s", "sm", None) in kinds
    ps.ping()
    m = ps.get_message(timeout=2)
    assert m.kind == "pong" and m.channel is None
    ps.unsubscribe()  # no arguments: forget every channel
    assert ps.channels == {} and ps.patterns == {"p*": None}
    ps.punsubscribe()
    ps.sunsubscribe()
    assert ps.patterns == {} and ps.shard_channels == {}
    ps.close()
    c.close()


def test_sync_pubsub_ignore_subscribe_messages_and_reply_kinds(node_factory):
    n = node_factory({
        "SUBSCRIBE": array([bulk("subscribe"), bulk("a"), integer(1)]) + array([bulk("message"), bulk("a"), bulk("x")]),
        "PING": array([bulk("pong")]) + b":42\r\n",
    })
    c = ruvio.Ruvio(*host_port(n))
    with c.pubsub(ignore_subscribe_messages=True) as ps:
        ps.subscribe("a")
        m = ps.get_message(timeout=2)
        assert (m.kind, m.data) == ("message", "x")  # the subscribe confirmation was skipped
        ps.ping()
        m = ps.get_message(timeout=2)
        assert m.kind == "pong" and m.data is None  # a bare pong has no payload
        m = ps.get_message(timeout=2)
        assert m.kind == "reply" and m.data == 42  # a non-array reply is wrapped
        assert ps.get_message(timeout=0.05) is None  # nothing pending: None after the timeout
    c.close()


def test_sync_pubsub_listen_yields_until_closed(node_factory):
    n = node_factory({"SUBSCRIBE": array([bulk("message"), bulk("a"), bulk("1")]) + array([bulk("message"), bulk("a"), bulk("2")])})
    c = ruvio.Ruvio(*host_port(n))
    ps = c.pubsub()
    ps.subscribe("a")
    it = ps.listen()
    assert [next(it).data for _ in range(2)] == ["1", "2"]
    ps.close()
    c.close()


def test_sync_pubsub_error_reply_raises(node_factory):
    n = node_factory({"SUBSCRIBE": err("NOPERM this user has no permissions")})
    c = ruvio.Ruvio(*host_port(n))
    ps = c.pubsub()
    ps.subscribe("a")
    with pytest.raises(ResponseError, match="NOPERM"):
        ps.get_message(timeout=2)
    ps.close()
    c.close()


def test_sync_pubsub_connection_loss_and_resubscribe(node_factory):
    n = node_factory({"PING": DROP, "SUBSCRIBE": array([bulk("subscribe"), bulk("a"), integer(1)])})
    c = ruvio.Ruvio(*host_port(n))
    ps = c.pubsub()
    ps.subscribe("a")
    ps.psubscribe("p*")
    ps.ssubscribe("s")
    ps.get_message(timeout=2)
    ps.ping()
    with pytest.raises(RConnectionError, match="closed by server"):
        for _ in range(10):  # unread confirmations come first, then the close is seen
            ps.get_message(timeout=2)
    assert ps._conn.sock is None
    n.handler = script({})
    n.log.clear()
    ps.get_message(timeout=0.05)  # reconnects and replays the three subscriptions
    assert wait_log(n, 3) == ["SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE"]
    # failure of the socket itself, in send and in receive
    ps._conn.sock.close()
    with pytest.raises(RConnectionError):
        ps.subscribe("b")
    assert ps._conn.sock is None
    ps.get_message(timeout=0.05)
    ps.close()
    c.close()


# --------------------------------------------------------------------------- asyncio: connecting


async def test_async_durability_option_is_sent_after_the_handshake(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n), client_name="nm", durability_replicas=1, durability_timeout_ms=250)
    await c.ping()
    assert n.log[:2] == [["CLIENT", "SETNAME", "nm"], ["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"]]
    await c.aclose()
    await c.ping()
    assert n.log[-2] == ["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"]
    plain = node_factory()
    d = ruvio.asyncio.Ruvio(*host_port(plain), durability_replicas=2)
    await d.ping()
    assert plain.log[0] == ["CLIENT", "DURABILITY", "REPLICAS", "2"]
    await d.aclose()
    refused = node_factory({"CLIENT DURABILITY": err("ERR CLIENT DURABILITY needs replication on this server")})
    e = ruvio.asyncio.Ruvio(*host_port(refused), durability_replicas=1)
    with pytest.raises(ResponseError, match="needs replication"):
        await e.ping()
    await c.aclose()


async def test_async_handshake_and_reconnect(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n), db=4, password="pw", username="me", client_name="nm")
    await c.ping()
    assert n.log[:3] == [["AUTH", "me", "pw"], ["SELECT", "4"], ["CLIENT", "SETNAME", "nm"]]
    await c.aclose()
    await c.ping()
    assert [x[0] for x in n.log[4:]] == ["AUTH", "SELECT", "CLIENT", "PING"]
    await c.aclose()


async def test_async_handshake_error_and_drop(node_factory):
    n = node_factory({"AUTH": err("WRONGPASS bad")})
    c = ruvio.asyncio.Ruvio(*host_port(n), password="x")
    with pytest.raises(ResponseError, match="WRONGPASS"):
        await c.ping()
    with pytest.raises(ResponseError):
        await c.ping()  # nothing half-open is cached: the handshake runs again
    assert len(n.commands("AUTH")) == 2
    n2 = node_factory({"AUTH": DROP})
    c2 = ruvio.asyncio.Ruvio(*host_port(n2), password="x")
    with pytest.raises(RConnectionError):
        await c2.ping()
    await c.aclose()
    await c2.aclose()


async def test_async_connection_refused_and_timeout(monkeypatch):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with pytest.raises(RConnectionError, match="cannot connect"):
        await ruvio.asyncio.Ruvio("127.0.0.1", port).ping()
    with pytest.raises(RConnectionError, match="cannot connect"):
        await ruvio.asyncio.Ruvio(unix_socket_path="/nonexistent/x.sock").ping()

    async def hung(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(asyncio, "wait_for", hung)
    with pytest.raises(RTimeoutError, match="timed out"):
        await ruvio.asyncio.Ruvio("10.255.255.1", 9, connect_timeout=0.01).ping()


async def test_async_unix_socket(tmp_path):
    path = str(tmp_path / "a.sock")

    async def handle(reader, writer):
        await reader.read(100)
        writer.write(b"+PONG\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(handle, path)
    try:
        async with ruvio.asyncio.Ruvio(unix_socket_path=path) as c:
            assert await c.ping() is not None
    finally:
        server.close()
        await server.wait_closed()


async def test_async_concurrent_first_commands_share_one_connection(node_factory):
    n = node_factory({"GET": bulk("v")})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    assert await asyncio.gather(*[c.get(f"k{i}") for i in range(20)]) == ["v"] * 20
    assert wait_connections(n, 1) == 1
    c2 = ruvio.asyncio.Ruvio(*host_port(n))
    await asyncio.gather(*[c2.connect() for _ in range(5)])
    assert (await c2.connect()) is c2 and wait_connections(n, 2) == 2
    c3 = ruvio.asyncio.Ruvio(*host_port(n))
    r = await asyncio.gather(*[c3.pipeline().get("a").get("b").execute() for _ in range(5)])
    assert r == [["v", "v"]] * 5 and wait_connections(n, 3) == 3
    for x in (c, c2, c3):
        await x.aclose()


async def test_async_aclose_without_connection_and_twice(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    await c.aclose()  # never connected
    await c.close()
    async with c:
        await c.ping()
    assert c._st.proto is None
    await c.aclose()


# --------------------------------------------------------------------------- asyncio: failures in flight


async def test_async_connection_loss_fails_every_pending_command_then_reconnects(node_factory):
    gate = threading.Event()

    def hold(node, args):
        gate.wait(2)
        return DROP

    n = node_factory({"GET": hold})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    t1 = asyncio.ensure_future(c.get("a"))
    t2 = asyncio.ensure_future(c.get("b"))
    p = asyncio.ensure_future(c.pipeline().get("c").get("d").execute())
    await asyncio.sleep(0.05)
    gate.set()
    for t in (t1, t2, p):
        with pytest.raises(RConnectionError):
            await t
    n.handler = script({"GET": bulk("back")})
    assert await c.get("a") == "back"
    assert n.connections == 2
    await c.aclose()


async def test_async_protocol_error_fails_the_command_and_resets_the_connection(node_factory):
    n = node_factory({"GET": b"?junk\r\n"})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    with pytest.raises(ProtocolError):
        await c.get("k")
    await asyncio.sleep(0.05)
    assert c._st.proto is None
    n.handler = script({"GET": bulk("ok")})
    assert await c.get("k") == "ok"
    await c.aclose()


async def test_async_converter_exception_is_delivered_to_the_caller_only(node_factory):
    n = node_factory({"FILLGET": b"*0\r\n", "PING": b"+PONG\r\n"})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    bad, good = await asyncio.gather(c.fillget("k", 1), c.ping(), return_exceptions=True)
    assert isinstance(bad, IndexError) and good == "PONG"
    await c.aclose()


async def test_async_push_with_and_without_callback(node_factory):
    push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n"
    n = node_factory({"GET": push + bulk("v"), "PING": push})
    seen = []
    c = ruvio.asyncio.Ruvio(*host_port(n), on_push=seen.append)
    assert await c.get("k") == "v"
    assert seen == [["invalidate", ["k"]]]
    await c.aclose()
    # an unsolicited push while idle, no callback registered: ignored, the client keeps working
    n.handler = script({"PING": push + b"+PONG\r\n", "GET": bulk("v")})
    c2 = ruvio.asyncio.Ruvio(*host_port(n))
    assert await c2.ping() == "PONG"
    n.handler = script({"GET": bulk("v")})
    assert await c2.get("k") == "v"
    await c2.aclose()


async def test_async_cancelled_command_does_not_desync_the_stream(node_factory):
    def slow_get(node, args):
        if args[1] == "slow":
            time.sleep(0.2)
        return bulk(args[1])

    n = node_factory({"GET": slow_get})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    t = asyncio.ensure_future(c.get("slow"))
    await asyncio.sleep(0.05)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert await c.get("fast") == "fast"  # the late reply for "slow" was discarded, not given to this call
    await c.aclose()


async def test_async_large_concurrent_writes_are_coalesced_and_flushed(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    payload = "x" * 3000
    res = await asyncio.gather(*[c.set(f"k{i}", payload) for i in range(100)])  # > 64 KiB in one tick
    assert all(res) and len(n.commands("SET")) == 100
    assert all(len(x[2]) == 3000 for x in n.commands("SET"))
    await c.aclose()


async def test_async_flush_after_close_drops_the_buffered_write(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    await c.ping()
    proto = c._st.proto
    proto.flush()  # nothing buffered: no-op
    proto.pending.extend([(None, None, True)] * 2)  # pretend replies are in flight so send() buffers
    proto.send(b"*1\r\n$4\r\nPING\r\n")
    assert proto.wbuf is not None
    proto.closed = True
    proto.flush()
    assert proto.wbuf is None
    proto.pending.clear()
    proto.closed = False
    await c.aclose()


async def test_async_select_auth_setname_are_remembered(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    assert await c.auth("pw") is True and await c.select(3) is True and await c.client_setname("cn") is True
    o = c._st.opts
    assert (o.password, o.username, o.db, o.client_name) == ("pw", None, 3, "cn")
    await c.aclose()
    await c.ping()
    assert n.log[-4:-1] == [["AUTH", "pw"], ["SELECT", "3"], ["CLIENT", "SETNAME", "cn"]]
    await c.aclose()


async def test_async_raw_dedicated_and_pipeline_context(node_factory):
    n = node_factory({"GET": bulk("v")})
    async with ruvio.asyncio.Ruvio(*host_port(n), db=2) as c:
        assert await c.raw.get("k") == b"v" and await c.get("k") == "v"
        d = c.dedicated()
        assert d._st is not c._st and d._st.opts.db == 2 and d._decode is True
        await d.aclose()
        async with c.pipeline() as p:
            p.get("k")
            assert len(p) == 1
        assert len(p) == 0
        assert await c.pipeline().execute() == []
        assert (await c.pipeline().get("a").get("b").execute()) == ["v", "v"]


# --------------------------------------------------------------------------- asyncio: transaction


async def test_async_transaction_retry_exhaustion_and_async_fn(node_factory):
    n = node_factory({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": b"*-1\r\n"})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    calls = []

    async def afn(pipe):
        calls.append(1)
        pipe.set("k", 1)

    with pytest.raises(RuvioError, match="retries exhausted"):
        await c.transaction(afn, "k", retries=3)
    assert len(calls) == 3 and len(n.commands("WATCH")) == 3

    n.handler = script({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": array([b"+OK\r\n"])})
    assert await c.transaction(lambda p: p.set("k", 1)) == [True]  # sync fn, nothing watched
    n.log.clear()
    assert await c.transaction(afn, "k") == [True]
    assert [x[0] for x in n.log][:2] == ["WATCH", "MULTI"]

    def raising(pipe):
        raise KeyError("boom")

    with pytest.raises(KeyError):
        await c.transaction(raising, "k")
    await c.aclose()


async def test_async_transaction_exec_abort_is_not_retried(node_factory):
    n = node_factory({"MULTI": OK, "SET": err("ERR syntax error"), "EXEC": err("EXECABORT Transaction discarded")})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    with pytest.raises(ResponseError):
        await c.transaction(lambda p: p.set("k", 1))
    with pytest.raises(ResponseError):
        await c.pipeline(transaction=True).set("k", 1).execute()
    with pytest.raises(ExecAbortError):
        await c.pipeline(transaction=True).set("k", 1).execute(raise_on_error=False)
    await c.aclose()


# --------------------------------------------------------------------------- asyncio: helpers


async def test_async_get_or_create_paths(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))

    n.handler = script({"FILLGET": fill("HIT", "cached")})
    assert await c.get_or_create("k", lambda: "never", 1000) == "cached"

    n.handler = script({"FILLGET": fill("FILL", "tok"), "FILLSET": integer(1)})

    async def afactory():
        await asyncio.sleep(0)
        return "async-built"

    assert await c.get_or_create("k", afactory, 1000) == "async-built"
    assert await c.get_or_create("k", lambda: "sync-built", 1000, swr_ms=5) == "sync-built"

    n.handler = script({"FILLGET": fill("STALE", "old", "tok"), "FILLSET": integer(1)})
    assert await c.get_or_create("k", lambda: "fresh", 1000) == "fresh"
    n.handler = script({"FILLGET": fill("STALE", "old")})
    assert await c.get_or_create("k", lambda: "never", 1000) == "old"

    n.handler = script({"FILLGET": fill("BUSY")})
    with pytest.raises(RTimeoutError, match="still being filled"):
        await c.get_or_create("k", lambda: "never", 1000, timeout=0.1, poll_ms=5)

    replies = [fill("BUSY"), fill("HIT", "late")]
    n.handler = script({"FILLGET": lambda node, a: replies.pop(0)})
    assert await c.get_or_create("k", lambda: "never", 1000, poll_ms=1) == "late"
    await c.aclose()


async def test_async_idempotent_states(node_factory):
    n = node_factory({"IDEM": lambda node, a: array([bulk("acquired")]) if a[2] == "BEGIN" else integer(1)})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    assert await c.idempotent("k", "fp", 1000, lambda: "r", owner="o") == ("executed", "r")

    async def afn():
        return "async-r"

    assert await c.idempotent("k", "fp", 1000, afn) == ("executed", "async-r")
    assert n.commands("IDEM")[-1][-1] == "async-r"
    n.handler = script({"IDEM": array([bulk("completed"), bulk("stored")])})
    assert await c.idempotent("k", "fp", 1000, lambda: "never") == ("completed", "stored")
    n.handler = script({"IDEM": array([bulk("conflict")])})
    assert await c.idempotent("k", "fp", 1000, lambda: "never") == ("conflict", None)
    await c.aclose()


async def test_async_scan_iterators_walk_every_page(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    n.handler = script({"SCAN": pages(("9", [bulk("a")]), ("0", [bulk("b")]))})
    assert [k async for k in c.scan_iter(match="*", count=1, type="string")] == ["a", "b"]
    n.handler = script({"SSCAN": pages(("9", [bulk("a")]), ("0", [bulk("b")]))})
    assert [k async for k in c.sscan_iter("s", "*", 1)] == ["a", "b"]
    n.handler = script({"HSCAN": pages(("9", [bulk("f"), bulk("v")]), ("0", [bulk("g"), bulk("w")]))})
    assert [k async for k in c.hscan_iter("h", "*", 1)] == [("f", "v"), ("g", "w")]
    n.handler = script({"ZSCAN": pages(("9", [bulk("m"), bulk("1")]), ("0", [bulk("n"), bulk("2")]))})
    assert [k async for k in c.zscan_iter("z", "*", 1)] == [("m", 1.0), ("n", 2.0)]
    n.handler = script({"SCAN": pages(("0", []))})
    assert [k async for k in c.scan_iter()] == []
    await c.aclose()


async def test_async_lock_edge_cases(node_factory):
    state = {"held": False}

    def set_(node, args):
        if "NX" in args and state["held"]:
            return b"$-1\r\n"
        if "NX" in args:
            state["held"] = True
        return OK

    n = node_factory({"SET": set_, "DELEX": integer(1), "GET": b"$-1\r\n"})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    lk = c.lock("job", ttl_ms=500, retry_ms=1)
    assert await lk.renew() is False and await lk.release() is False and await lk.owned() is False
    assert await lk.acquire() is True
    assert await lk.renew() is True and await lk.renew(900) is True
    assert n.commands("SET")[-1][-2:] == ["PX", "900"]
    assert await lk.owned() is False
    assert await c.lock("job", blocking=False).acquire() is False
    t0 = time.monotonic()
    assert await c.lock("job", timeout=0.05, retry_ms=5).acquire() is False
    assert time.monotonic() - t0 >= 0.05
    assert await c.lock("job").acquire(blocking=False) is False
    with pytest.raises(LockError, match="could not acquire lock"):
        async with c.lock("job", blocking=False):
            pass
    assert await lk.release() is True and lk.token is None
    n.handler = script({"SET": OK, "DELEX": integer(0)})
    async with c.lock("job2") as held:
        assert held.token is not None
    assert held.token is None
    await c.aclose()


async def test_async_scopes(node_factory):
    n = node_factory({"LEASE": integer(11), "SEMAPHORE": integer(5), "RELEASE": integer(1)})
    c = ruvio.asyncio.Ruvio(*host_port(n))
    async with c.lease_scope("l", 1000) as tok:
        assert tok == 11
    async with c.semaphore_scope("s", 3, 1000) as tok:
        assert tok == 5
    assert [x[2] for x in n.commands("RELEASE")] == ["11", "5"]
    n.handler = script({"LEASE": b"$-1\r\n", "SEMAPHORE": b"_\r\n"})
    with pytest.raises(LockError, match="lease 'l' is not available"):
        async with c.lease_scope("l", 1000):
            pass
    with pytest.raises(LockError, match="semaphore 's' is not available"):
        async with c.semaphore_scope("s", 1, 1000):
            pass
    scope = c.lease_scope("l", 1000)
    await scope.__aexit__(None, None, None)
    assert len(n.commands("RELEASE")) == 2
    await c.aclose()


# --------------------------------------------------------------------------- asyncio: pubsub


async def test_async_pubsub_messages_resubscribe_and_loss(node_factory):
    n = node_factory({
        "SUBSCRIBE": array([bulk("subscribe"), bulk("a"), integer(1)]) + array([bulk("message"), bulk("a"), bulk("hi")]),
        "PSUBSCRIBE": array([bulk("psubscribe"), bulk("p*"), integer(2)])
        + array([bulk("pmessage"), bulk("p*"), bulk("ch"), bulk("pm")]),
        "SSUBSCRIBE": array([bulk("ssubscribe"), bulk("s"), integer(3)]),
        "PING": array([bulk("pong"), bulk("hello")]),
        "UNSUBSCRIBE": array([bulk("unsubscribe"), bulk("a"), integer(0)]),
        "QUIT": DROP,
    })
    c = ruvio.asyncio.Ruvio(*host_port(n))
    ps = await c.pubsub()
    await ps.connect()  # already connected: no second connection
    assert wait_connections(n, 1) == 1
    await ps.subscribe("a")
    await ps.psubscribe("p*")
    await ps.ssubscribe("s")
    got = [await ps.get_message(timeout=2) for _ in range(5)]
    assert [(m.kind, m.channel, m.data, m.pattern) for m in got] == [
        ("subscribe", "a", 1, None),
        ("message", "a", "hi", None),
        ("psubscribe", "p*", 2, None),
        ("pmessage", "ch", "pm", "p*"),
        ("ssubscribe", "s", 3, None),
    ]
    await ps.ping("hello")
    m = await ps.get_message(timeout=2)
    assert (m.kind, m.data) == ("pong", "hello")
    assert await ps.get_message(timeout=0.05) is None
    await ps.unsubscribe()
    await ps.punsubscribe()
    await ps.sunsubscribe()
    assert not ps.channels and not ps.patterns and not ps.shard_channels
    # a lost connection surfaces once, then the next call reconnects
    await ps.subscribe("a")
    await ps.get_message(timeout=2)
    await ps.get_message(timeout=2)
    n.handler = script({"PING": DROP})
    await ps.ping()
    with pytest.raises(RConnectionError):
        for _ in range(10):
            await ps.get_message(timeout=2)
    assert ps._proto is None
    n.handler = script({"SUBSCRIBE": array([bulk("subscribe"), bulk("a"), integer(1)])})
    n.log.clear()
    await ps.psubscribe("x*")  # reconnects, replays "a", then sends the new pattern
    assert wait_log(n, 2) == ["SUBSCRIBE", "PSUBSCRIBE"]
    await ps.aclose()
    await ps.close()
    await c.aclose()


async def test_async_pubsub_ignore_subscribe_listen_and_context(node_factory):
    n = node_factory({
        "SUBSCRIBE": array([bulk("subscribe"), bulk("a"), integer(1)])
        + array([bulk("message"), bulk("a"), bulk("1")])
        + array([bulk("message"), bulk("a"), bulk("2")]),
    })
    c = ruvio.asyncio.Ruvio(*host_port(n))
    async with await c.pubsub(ignore_subscribe_messages=True) as ps:
        await ps.subscribe("a")
        seen = []
        async for m in ps.listen():
            seen.append(m.data)
            if len(seen) == 2:
                break
        assert seen == ["1", "2"]
    assert ps._proto is None
    async with ruvio.asyncio.PubSub(c._st.opts, True) as ps2:  # __aenter__ connects
        assert ps2._proto is not None
    await c.aclose()


# --------------------------------------------------------------------------- pipeline / transaction replies (sync + asyncio)


@pytest.fixture(params=["sync", "async"])
def pc(request, node_factory):
    """``(client, node)`` of the parametrized kind, closed afterwards."""
    n = node_factory()

    def build(table):
        n.handler = script(table)
        if request.param == "sync":
            from conftest import SyncAsAsync

            return SyncAsAsync(ruvio.Ruvio(*host_port(n)))
        return ruvio.asyncio.Ruvio(*host_port(n))

    return build, n  # every test closes its client with maybe_close()


async def run(c, pipeline):
    """Executes a pipeline of either client kind."""
    import inspect

    r = pipeline.execute()
    return await r if inspect.isawaitable(r) else r


def new_pipe(c, **kw):
    from conftest import SyncAsAsync

    return c._c.pipeline(**kw) if isinstance(c, SyncAsAsync) else c.pipeline(**kw)


async def test_pipeline_multi_refused_raises_that_error(pc):
    build, n = pc
    c = build({"MULTI": err("ERR MULTI calls can not be nested")})
    p = new_pipe(c, transaction=True)
    p.set("k", 1)
    with pytest.raises(ResponseError, match="nested"):
        await run(c, p)
    await maybe_close(c)


async def maybe_close(c):
    import inspect

    r = c.aclose()
    if inspect.isawaitable(r):
        await r


async def test_transaction_queue_time_error_beats_a_generic_exec_error(pc):
    build, n = pc
    c = build({"MULTI": OK, "SET": err("ERR syntax error"), "GET": b"+QUEUED\r\n", "EXEC": err("ERR something else")})
    p = new_pipe(c, transaction=True)
    p.set("k", 1).get("k")
    with pytest.raises(ResponseError, match="syntax error") as ei:
        await run(c, p)
    assert type(ei.value) is ResponseError  # the queued command's own error, not a wrapper
    await maybe_close(c)


async def test_exec_abort_reports_the_first_queued_error(pc):
    build, n = pc
    abort = err("EXECABORT Transaction discarded because of previous errors.")
    c = build({"MULTI": OK, "SET": err("ERR syntax error"), "EXEC": abort})
    p = new_pipe(c, transaction=True)
    p.set("k", 1)
    with pytest.raises(ExecAbortError, match=r"discarded.*\(ERR syntax error\)"):
        await run(c, p)
    n.handler = script({"MULTI": OK, "GET": b"+QUEUED\r\n", "EXEC": abort})
    p = new_pipe(c, transaction=True)
    p.get("k")
    with pytest.raises(ExecAbortError) as ei:
        await run(c, p)
    assert "(" not in str(ei.value)  # nothing to append when no queued command failed
    await maybe_close(c)


async def test_exec_nil_is_a_watch_error(pc):
    build, n = pc
    c = build({"MULTI": OK, "SET": b"+QUEUED\r\n", "EXEC": b"*-1\r\n"})
    p = new_pipe(c, transaction=True)
    p.set("k", 1)
    with pytest.raises(WatchError):
        await run(c, p)
    await maybe_close(c)


async def test_exec_answering_fewer_replies_keeps_refusals_in_place(pc):
    build, n = pc
    c = build({"MULTI": OK, "SET": b"+QUEUED\r\n", "GET": err("ERR refused at queue time"), "EXEC": array([OK])})
    p = new_pipe(c, transaction=True)
    p.set("a", 1).get("b")
    res = await run_no_raise(c, p)
    assert res[0] is True and isinstance(res[1], ResponseError) and "refused" in str(res[1])
    await maybe_close(c)


async def run_no_raise(c, pipeline):
    import inspect

    r = pipeline.execute(raise_on_error=False)
    return await r if inspect.isawaitable(r) else r


async def test_pipeline_collects_errors_and_conversion_failures(pc):
    build, n = pc
    c = build({"SET": OK, "GET": err("WRONGTYPE nope"), "INCR": err("ERR not an integer"), "FILLGET": b"*0\r\n", "PING": b"+PONG\r\n"})
    p = new_pipe(c)
    p.set("a", 1).get("a").incr("a").fillget("a", 10).ping()
    res = await run_no_raise(c, p)
    assert res[0] is True and res[4] == "PONG"
    assert type(res[1]) is ruvio.WrongTypeError and type(res[2]) is ResponseError
    assert isinstance(res[3], IndexError)  # a converter failure is reported per command
    p = new_pipe(c)
    p.set("a", 1).get("a").incr("a")
    with pytest.raises(ruvio.WrongTypeError):  # the first failure wins
        await run(c, p)
    p = new_pipe(c)
    p.ping().fillget("a", 10)
    with pytest.raises(IndexError):
        await run(c, p)
    # the connection is still aligned after all of that
    p = new_pipe(c)
    p.ping().ping()
    assert await run(c, p) == ["PONG", "PONG"]
    await maybe_close(c)


# --------------------------------------------------------------------------- small remaining branches


def test_sync_close_swallows_an_oserror_from_the_socket():
    class Bad:
        def close(self):
            raise OSError("already gone")

    c = ruvio.Ruvio("127.0.0.1", 1)
    c._conn.sock = Bad()
    c.close()  # must not raise
    assert c._conn.sock is None


def test_sync_raw_pipeline_decodes_to_bytes_and_back(node_factory):
    n = node_factory({"GET": bulk("v")})
    c = ruvio.Ruvio(*host_port(n))
    assert c.get("k") == "v"
    assert c.raw.pipeline().get("k").get("k").execute() == [b"v", b"v"]  # parser flag flips for the batch
    assert c.pipeline().get("k").execute() == ["v"]
    c.close()


def test_sync_pubsub_receive_oserror_is_a_connection_error(node_factory):
    n = node_factory()
    c = ruvio.Ruvio(*host_port(n))
    ps = c.pubsub()
    ps.get_message(timeout=0)  # connect

    class Broken:
        def __init__(self, real):
            self.real = real

        def settimeout(self, t):
            pass

        def recv_into(self, buf):
            raise ConnectionResetError("reset by peer")

        def close(self):
            self.real.close()

    ps._conn.sock = Broken(ps._conn.sock)
    with pytest.raises(RConnectionError, match="reset by peer"):
        ps.get_message(timeout=0.1)
    assert ps._conn.sock is None
    ps.close()
    c.close()


async def test_async_write_buffer_flushes_at_the_size_cap(node_factory):
    n = node_factory()
    c = ruvio.asyncio.Ruvio(*host_port(n))
    await c.ping()  # connected: later calls take the buffered-write path
    payload = "x" * 3000
    futs = [c.set(f"k{i}", payload) for i in range(60)]  # 180 KB queued in a single loop iteration
    proto = c._st.proto
    assert proto.wbuf is None or proto.wsize < 65536  # a flush already happened at the cap
    assert all(await asyncio.gather(*futs))
    assert len(n.commands("SET")) == 60
    await c.aclose()


def test_python_parser_partial_integer_inside_an_array_waits_for_more():
    from ruvio._resp import NEED, Parser

    p = Parser()
    p.feed(b"*2\r\n$1\r\na\r\n:12345")  # integer element without its CRLF yet
    assert p.gets() is NEED
    p.feed(b"6\r\n")
    assert p.gets() == ["a", 123456]
