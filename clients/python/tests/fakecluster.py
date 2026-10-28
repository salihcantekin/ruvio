# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Scripted fake RESP servers for the cluster unit tests (threads, blocking sockets, no Ruvio server)."""

from __future__ import annotations

import socket
import threading
from typing import Callable, Dict, List, Optional, Tuple

from ruvio import key_slot

DROP = object()  # a handler returns this to make the fake server close the connection without replying


def bulk(v: Optional[str]) -> bytes:
    if v is None:
        return b"$-1\r\n"
    b = v.encode()
    return b"$%d\r\n%s\r\n" % (len(b), b)


def integer(n: int) -> bytes:
    return b":%d\r\n" % n


def array(items: List[bytes]) -> bytes:
    return b"*%d\r\n" % len(items) + b"".join(items)


OK = b"+OK\r\n"


def err(msg: str) -> bytes:
    return b"-" + msg.encode() + b"\r\n"


class FakeNode:
    """One listening socket. ``log`` records every command it received (as lists of str)."""

    def __init__(self, handler: Callable[["FakeNode", List[str]], object]) -> None:
        self.handler = handler
        self.log: List[List[str]] = []
        self.store: Dict[str, str] = {}
        self.connections = 0
        self._lock = threading.Lock()
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self._closed = False
        self._conns: List[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    @property
    def addr(self) -> str:
        return f"127.0.0.1:{self.port}"

    def commands(self, name: Optional[str] = None) -> List[List[str]]:
        with self._lock:
            return [c for c in self.log if name is None or c[0].upper() == name.upper()]

    def _accept(self) -> None:
        while not self._closed:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            if self._closed:
                conn.close()
                return
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._conns.append(conn)
            with self._lock:
                self.connections += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        f = conn.makefile("rb")
        try:
            while True:
                line = f.readline()
                if not line:
                    return
                n = int(line[1:])
                args = []
                for _ in range(n):
                    ln = int(f.readline()[1:])
                    args.append(f.read(ln + 2)[:-2].decode("utf-8", "surrogateescape"))
                with self._lock:
                    self.log.append(args)
                reply = self.handler(self, args)
                if reply is DROP:
                    conn.close()
                    return
                conn.sendall(reply)  # type: ignore[arg-type]
        except (OSError, ValueError):
            return

    def close(self) -> None:
        self._closed = True
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=1).close()  # wakes the blocked accept
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        for c in self._conns:
            try:
                c.shutdown(socket.SHUT_RDWR)  # ends the serving thread, which holds its own reference
            except OSError:
                pass
            try:
                c.close()
            except OSError:
                pass


class FakeCluster:
    """Nodes that own slot ranges. ``advertised`` is what ``CLUSTER SLOTS`` reports (it may be
    stale); ``owner`` is what the nodes enforce with ``-MOVED``."""

    def __init__(self, ranges: List[Tuple[int, int]]) -> None:
        self.nodes: List[FakeNode] = [FakeNode(self._handle) for _ in ranges]
        self.owner_ranges = list(ranges)
        self.advertised = list(ranges)
        self.hidden: set = set()  # node indexes left out of CLUSTER SLOTS

    def owner(self, slot: int) -> FakeNode:
        for (lo, hi), node in zip(self.owner_ranges, self.nodes):
            if lo <= slot <= hi:
                return node
        raise AssertionError(slot)

    def slots_reply(self) -> bytes:
        rows = []
        for i, ((lo, hi), node) in enumerate(zip(self.advertised, self.nodes)):
            if i in self.hidden:
                continue
            rows.append(array([integer(lo), integer(hi), array([bulk("127.0.0.1"), integer(node.port), bulk(f"node{i}")])]))
        return array(rows)

    def _handle(self, node: FakeNode, args: List[str]) -> object:
        name = args[0].upper()
        if name == "CLUSTER" and args[1].upper() == "SLOTS":
            return self.slots_reply()
        if name == "PING":
            return b"+PONG\r\n"
        if name == "GET" and args[1] == "dropme":
            return DROP
        keys = args[1:] if name in ("MGET", "DEL", "EXISTS", "UNLINK", "TOUCH") else args[1::2] if name == "MSET" else args[1:2]
        slots = {key_slot(k) for k in keys}
        if len(slots) > 1:
            return err("CROSSSLOT Keys in request do not hash to the same slot")
        if slots:
            owner = self.owner(slots.pop())
            if owner is not node:
                return err(f"MOVED {key_slot(keys[0])} {owner.addr}")
        if name == "GET":
            return bulk(node.store.get(args[1]))
        if name == "SET":
            node.store[args[1]] = args[2]
            return OK
        if name == "MGET":
            return array([bulk(node.store.get(k)) for k in args[1:]])
        if name == "MSET":
            for k, v in zip(args[1::2], args[2::2]):
                node.store[k] = v
            return OK
        if name in ("DEL", "UNLINK"):
            return integer(sum(1 for k in args[1:] if node.store.pop(k, None) is not None))
        if name in ("EXISTS", "TOUCH"):
            return integer(sum(1 for k in args[1:] if k in node.store))
        if name == "INCR":
            node.store[args[1]] = str(int(node.store.get(args[1], "0")) + 1)
            return integer(int(node.store[args[1]]))
        if name == "MULTI":
            return OK
        if name == "EXEC":
            return array([])
        if name == "SSUBSCRIBE":
            return array([bulk("ssubscribe"), bulk(args[1]), integer(1)])
        return OK

    def close(self) -> None:
        for n in self.nodes:
            n.close()


def standalone_node() -> FakeNode:
    """A server that answers ``CLUSTER SLOTS`` like a standalone Ruvio."""

    def handle(node: FakeNode, args: List[str]) -> object:
        name = args[0].upper()
        if name == "CLUSTER":
            return err("ERR cluster mode is disabled")
        if name == "SET":
            node.store[args[1]] = args[2]
            return OK
        if name == "GET":
            return bulk(node.store.get(args[1]))
        if name == "MGET":
            return array([bulk(node.store.get(k)) for k in args[1:]])
        return OK

    return FakeNode(handle)
