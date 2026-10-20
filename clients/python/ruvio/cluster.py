# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Synchronous cluster client: ``Ruvio(host, port, cluster=True)`` or :class:`ClusterRuvio`.

On the first command (or ``connect()``) the seed is asked for ``CLUSTER SLOTS``. A server that
answers ``-ERR cluster mode is disabled`` (or reports a single node) is used as a plain standalone
server from then on. Otherwise a 16384-entry slot-to-node table is built and every command is sent
to the node that owns its key, over one lazily opened socket per node.

Ruvio shards are slot owners inside one process (``shards = N``; with ``port_mode = "per-shard"``,
the default, shard ``i`` listens on the base port plus ``i``). A server forwards a command for a key it
does not own, so a plain client works against any one port, with an extra hop; the cluster client
avoids the hop and also fans out the keyless commands (``DBSIZE``, ``KEYS``, ``FLUSHDB`` ...) that a
single port answers for its own shard only.

Only this class pays for routing: ``Ruvio(...)`` without ``cluster=True`` is untouched.
"""

from __future__ import annotations

import random
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

from . import _cluster as cl
from ._blocking import FOREVER, blocking_wait, frames_wait, pipeline_wait
from ._commands import Conv
from ._pipeline import _EXEC, _MULTI
from .client import PubSub, Ruvio, _Conn, _Options, _ReplicaOptions
from .errors import ConnectionError, CrossSlotError, MovedError, ResponseError, RuvioError, TimeoutError

__all__ = ["ClusterRuvio"]


class _Node:
    __slots__ = ("host", "port", "conn", "idx", "replica", "primary")

    def __init__(self, host: str, port: int, conn: _Conn) -> None:
        self.host = host
        self.port = port
        self.conn = conn
        self.idx = -1
        self.replica = False  # a replica chosen for reads (READONLY is sent when its socket opens)
        self.primary = -1  # for a replica: the index of the node it serves


class _State:
    """Routing state, shared by a client and its ``raw`` view."""

    __slots__ = ("mode", "slots", "nodes", "by_addr", "seed", "watch", "targets", "bufs", "seeds", "policy", "reps", "rnodes")

    def __init__(self, seed: _Node, seeds: Optional[List[Tuple[str, int]]] = None) -> None:
        self.seeds = seeds  # configured seed addresses, asked for the slot table when no known node answers
        self.mode = 0  # 0 = not discovered yet, 1 = cluster, 2 = standalone fallback
        self.slots: List[int] = []
        self.nodes: List[_Node] = []
        self.by_addr: Dict[Tuple[str, int], _Node] = {(seed.host, seed.port): seed}
        self.seed = seed
        self.watch: Optional[_Node] = None
        self.targets: List[_Node] = [seed]  # nodes + the seed last: index -1 is the seed
        self.bufs: List[List[bytes]] = [[]]  # per-target frame lists, reused by every pipeline
        self.policy = cl.POLICY_PRIMARY  # where reads go; anything but primary adds the replica paths
        self.reps: List[Optional[_Node]] = []  # per node index: the replica its reads go to (None = none usable)
        self.rnodes: Dict[Tuple[str, int], _Node] = {}  # the replica nodes of the last load, by address


def _fail(conn: _Conn, exc: BaseException) -> BaseException:
    conn.close()
    if isinstance(exc, socket.timeout):
        return TimeoutError("timed out waiting for the reply")
    if isinstance(exc, ConnectionError):
        return exc
    if isinstance(exc, OSError):
        return ConnectionError(str(exc))
    return exc


class ClusterRuvio(Ruvio):
    """Cluster-aware synchronous client. Same methods as :class:`ruvio.Ruvio`; routing is automatic.

    Create it with ``ClusterRuvio(host, port)`` or ``Ruvio(host, port, cluster=True)``; the arguments are
    those of :class:`ruvio.Ruvio` (a Unix socket is not allowed). ``host``/``port`` is any one shard (the
    seed). Like the plain client it is not thread-safe.

    Routing rules:

    * Single-key commands go to the owner of the key's slot (CRC16 of the key, or of its ``{hash tag}``,
      modulo 16384; see :func:`ruvio.key_slot`). Keyless commands go to the seed node.
    * ``mget``/``mset``/``delete``/``unlink``/``exists``/``touch`` over several slots are split per node, sent
      to all nodes before any reply is read, and put back in your order. This is **not atomic** across
      shards.
    * Any other command whose keys span slots raises :class:`ruvio.CrossSlotError` before anything is
      sent (``rename``, ``copy``, ``lmove``, ``sinterstore``, ``bitop``, ``eval`` ...). Put the keys in
      one slot with a hash tag such as ``{user:1}:profile`` and ``{user:1}:cart``.
    * ``dbsize`` adds up all shards; ``keys`` joins them; ``flushdb``/``flushall`` run on every shard;
      ``auth``, ``select`` and ``client_setname`` are applied to every node; ``scan`` walks the shards one
      after the other (the cursor encodes the shard, treat it as opaque).
    * A pipeline is grouped per node and the replies come back in your order. A pipeline holding a
      command with keys in several slots is refused before sending. A transaction
      (``pipeline(transaction=True)``) must use one slot; ``watch`` pins the node.
    * ``MOVED`` and ``ASK`` replies are followed up to 3 times per command in all (a Ruvio server forwards
      instead of sending ``MOVED`` for keys, so this matters for other topologies); ``ASK`` sends ``ASKING`` first
      and does not change the slot table. ``TRYAGAIN`` is retried up to 3 times after short sleeps (single commands; a pipeline gets it as that
      command's error).
    * A node socket that fails is closed and reopened by the next command for that node. A command that
      hit a transport failure is never sent again.
    * With ``read_policy=ReadPolicy.PREFER_REPLICA`` or ``REPLICA`` (default ``PRIMARY``), commands the server
      flags read-only and not blocking go to one replica of the key's shard, picked at random whenever the slot
      table loads; a pipeline goes to replicas only when every command in it is a read. A replica that cannot
      be connected or answers ``LOADING`` is skipped until the next load: ``PREFER_REPLICA`` uses the primary
      then, ``REPLICA`` fails. Replication is asynchronous, so a read may miss a recent write, your own included.
    * With more than one shard only database 0 exists.

    A standalone or single-shard server is detected on the first command and then used as a plain client.

    Example:
        from ruvio import ClusterRuvio

        client = ClusterRuvio("127.0.0.1", 7000)
        client.set("user:1", "Ada")
        client.mget("user:1", "user:2")      # split over shards if needed
        client.dbsize()                      # sum over all shards
    """

    __slots__ = ("_cl",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Creates the client. Arguments are those of :class:`ruvio.Ruvio` (``cluster`` is implied).

        Raises:
            ValueError: ``unix_socket_path`` was given; cluster mode needs ``host``/``port``.
            ConnectionError: ``lazy=False`` and the seed cannot be reached.
        """
        super().__init__(*args, **kwargs)
        o = self._conn.opts
        if o.unix_socket_path:
            raise ValueError("cluster mode needs host:port, not a unix socket")
        self._cl = _State(_Node(o.host, o.port, self._conn), o.seeds)
        self._cl.policy = cl.policy_code(o.read_policy)
        if self._conn.sock is not None:  # lazy=False connected the seed already
            self._discover()

    # ---------------------------------------------------------------- transport
    def _rt(self, conn: _Conn, data: bytes, dec: bool) -> Any:
        """One round trip on ``conn``; returns the raw reply (a ``ResponseError`` is a value)."""
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not dec:
            p.decode = dec
        t = conn.opts.socket_timeout
        wait = frames_wait(data) if t is not None else 0.0
        try:
            if wait:
                conn.sock.settimeout(None if wait == FOREVER else t + wait)  # type: ignore[union-attr]
            conn.sock.sendall(data)  # type: ignore[union-attr]
            reply = conn.read()
            if wait:
                conn.sock.settimeout(t)  # type: ignore[union-attr]
            return reply
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _write(self, conn: _Conn, data: bytes, dec: bool) -> None:
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not dec:
            p.decode = dec
        try:
            conn.sock.sendall(data)  # type: ignore[union-attr]
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _read(self, conn: _Conn, n: int) -> List[Any]:
        try:
            return [conn.read() for _ in range(n)]
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _scatter(self, jobs: List[Tuple[_Conn, bytes, int]], dec: bool, wait: float = 0.0) -> List[Any]:
        """Writes every job to its node first, then reads: the nodes work in parallel.

        ``wait`` is the widest wait of a blocking command among the jobs (see ``_blocking``): the reads
        get it on top of ``socket_timeout``. Returns one entry per job (a list of raw replies, or the
        exception for a dead node)."""
        sent: List[Any] = []
        for conn, data, n in jobs:
            try:
                self._write(conn, data, dec)
                sent.append(n)
            except (ConnectionError, OSError) as exc:
                sent.append(exc)
        out: List[Any] = []
        for (conn, _, _), n in zip(jobs, sent):
            if isinstance(n, BaseException):
                out.append(n)
                continue
            t = conn.opts.socket_timeout
            try:
                if wait and t is not None and conn.sock is not None:
                    conn.sock.settimeout(None if wait == FOREVER else t + wait)
                out.append(self._read(conn, n))
                if wait and t is not None and conn.sock is not None:
                    conn.sock.settimeout(t)
            except (ConnectionError, OSError) as exc:
                out.append(exc)
        return out

    # ---------------------------------------------------------------- topology
    def _new_node(self, host: str, port: int) -> _Node:
        o = _Options(**self._conn.opts.as_dict())
        o.host = host
        o.port = port
        o.seeds = None
        o.stop_following()
        return _Node(host, port, _Conn(o))

    def _discover(self) -> None:
        c = self._cl
        reply = self._rt(c.seed.conn, cl.CLUSTER_SLOTS, True)
        self._follow_seed(c.seed)
        topo = cl.parse_slots(reply, c.seed.host) if not isinstance(reply, ResponseError) else None
        if topo is None or len(topo.nodes) < 2:
            c.mode = 2  # standalone (or a one-node map): plain single-connection behavior
            return
        c.seed.conn.opts.seeds = None  # from now on this socket is the node it connected to
        c.seed.conn.opts.stop_following()
        self._apply(topo)
        c.mode = 1

    def _follow_seed(self, node: _Node) -> None:
        """Points the seed node at the address its socket connected to (the first reachable seed)."""
        o = node.conn.opts
        if (o.host, o.port) != (node.host, node.port):
            by = self._cl.by_addr
            if by.get((node.host, node.port)) is node:
                del by[(node.host, node.port)]
            node.host, node.port = o.host, o.port
            by[(node.host, node.port)] = node

    def _apply(self, topo: cl.Topology) -> None:
        c = self._cl
        old = c.by_addr
        nodes: List[_Node] = []
        by: Dict[Tuple[str, int], _Node] = {}
        for addr in topo.nodes:
            n = old.get(addr)
            if n is None:
                n = self._new_node(*addr)
            n.idx = len(nodes)
            nodes.append(n)
            by[addr] = n
        if (c.seed.host, c.seed.port) not in by:
            by[(c.seed.host, c.seed.port)] = c.seed
        for addr, n in old.items():
            if addr not in by:
                n.conn.close()
        for n in by.values():
            if n not in nodes:
                n.idx = -1
        c.nodes = nodes
        c.by_addr = by
        c.slots = topo.slots
        c.targets = [*nodes, c.seed]
        c.bufs = [[] for _ in c.targets]
        if c.policy:
            self._pick_replicas(topo)

    def _pick_replicas(self, topo: cl.Topology) -> None:
        """Each node's reads go to one of its replicas, picked at random per client (one number per load)
        so many clients spread over the replicas, and kept until the next load."""
        c = self._cl
        old = c.rnodes
        pick = random.getrandbits(32)
        reps: List[Optional[_Node]] = []
        rnodes: Dict[Tuple[str, int], _Node] = {}
        for i, found in enumerate(topo.replicas):
            node = None
            if found:
                addr = found[pick % len(found)]
                node = rnodes.get(addr) or old.get(addr)
                if node is None:
                    node = self._new_node(*addr)
                    node.conn.opts = _ReplicaOptions(**node.conn.opts.as_dict())
                    node.replica = True
                node.primary = i
                rnodes[addr] = node
            reps.append(node)
        for addr, node in old.items():
            if addr not in rnodes:
                node.conn.close()
        c.reps = reps
        c.rnodes = rnodes

    def _refresh(self) -> bool:
        """Reloads the slot table. Nodes with an open socket are asked first; when none answers, every
        other known node and then every seed is opened (one connect attempt each) until one returns a
        slot table. A seed that was only opened for this reload does not stay in the node list."""
        c = self._cl
        tried = set()
        known: List[_Node] = []
        for node in [c.seed, *c.nodes, *c.by_addr.values()]:
            if id(node) not in tried:
                tried.add(id(node))
                known.append(node)
        for node in known:
            if node.conn.sock is not None and self._ask_slots(node):
                return True
        for node in known:
            if node.conn.sock is None and self._open_and_ask(node):
                return True
        for host, port in c.seeds or ():
            if (host, port) in c.by_addr:
                continue
            node = self._new_node(host, port)
            c.by_addr[(host, port)] = node
            if self._open_and_ask(node):
                return True
            node.conn.close()
            if c.by_addr.get((host, port)) is node:
                del c.by_addr[(host, port)]
        return False

    def _ask_slots(self, node: _Node) -> bool:
        try:
            reply = self._rt(node.conn, cl.CLUSTER_SLOTS, True)
        except (ConnectionError, OSError):
            return False
        topo = None if isinstance(reply, ResponseError) else cl.parse_slots(reply, node.host)
        if topo is None or len(topo.nodes) < 1:
            return False
        self._apply(topo)
        return True

    def _open_and_ask(self, node: _Node) -> bool:
        try:
            node.conn.open()
        except (ConnectionError, ResponseError):
            return False
        return self._ask_slots(node)

    def refresh_slots(self) -> None:
        """Re-reads ``CLUSTER SLOTS`` and rebuilds the slot table.

        Normally not needed: ``MOVED`` replies patch or reload the table. Call it after you changed the
        topology. Does nothing on a standalone server.
        """
        if self._cl.mode == 0:
            self._discover()
        elif self._cl.mode == 1:
            self._refresh()

    @property
    def is_cluster(self) -> bool:
        """True when the seed answered ``CLUSTER SLOTS`` with a multi-node map (discovers on first use).

        Returns:
            ``False`` for a standalone or single-shard server.
        """
        if self._cl.mode == 0:
            self._discover()
        return self._cl.mode == 1

    @property
    def nodes(self) -> List[Tuple[str, int]]:
        """``(host, port)`` of the known nodes in slot order (discovers on first use).

        Returns:
            A list of tuples; empty on a standalone server.
        """
        if self._cl.mode == 0:
            self._discover()
        return [(n.host, n.port) for n in self._cl.nodes]

    def _owner(self, slot: Optional[int]) -> _Node:
        c = self._cl
        if slot is None:
            return c.seed
        i = c.slots[slot]
        return c.nodes[i] if i >= 0 else c.seed

    # ---------------------------------------------------------------- routing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                self._discover()
            if c.mode == 2:
                return Ruvio._x(self, conv, data, decode)
        dec = self._decode if decode is None else decode
        ni = cl.node_index(data, c.slots)
        if ni is not None:
            if c.policy and ni >= 0 and cl.is_read(data):
                reply = self._read_replica(ni, data, dec, None)
            else:
                # the common case: an ordinary command, routed from its frame with no copy kept for MOVED
                conn = c.targets[ni].conn
                if conn.sock is None:
                    conn = self._open_target(ni, data)
                p = conn.parser
                if p.decode is not dec:
                    p.decode = dec
                t = conn.opts.socket_timeout
                wait = blocking_wait(data) if t is not None else 0.0
                try:
                    if wait:
                        conn.sock.settimeout(None if wait == FOREVER else t + wait)  # type: ignore[union-attr]
                    conn.sock.sendall(data)  # type: ignore[union-attr]
                    reply = conn.read()
                    if wait:
                        conn.sock.settimeout(t)  # type: ignore[union-attr]
                except BaseException as exc:
                    raise _fail(conn, exc) from None
                if reply.__class__ is MovedError:
                    reply = self._redirect(reply, data, dec)
        else:
            name, _ = cl.frame_head(data)
            reply = self._special(name.upper(), data, dec)
        if isinstance(reply, ResponseError):
            if cl.is_tryagain(reply):  # only an error reply gets here
                reply = self._redirect(reply, data, dec)
            if isinstance(reply, ResponseError):
                raise reply
        return reply if conv is None else conv(reply)

    def _open_target(self, ni: int, data: bytes) -> _Conn:
        """Opens the socket of target ``ni`` for an ordinary command. When the node cannot be
        reached the slot table is reloaded and the command goes to the owner the new table names (a
        keyless one to the first reachable node)."""
        c = self._cl
        conn = c.targets[ni].conn
        try:
            conn.open()
            return conn
        except ConnectionError as exc:
            error = exc
        if not self._refresh():
            raise error
        ni2 = cl.node_index(data, c.slots)
        if ni2 is None or ni2 < 0:
            return self._first_reachable(error)
        return self._ensure_open(c.targets[ni2].conn)

    def _primary(self, ni: int, data: bytes, dec: bool) -> Any:
        """An ordinary command on the primary of target ``ni`` (the path of :meth:`_x`, as a method)."""
        conn = self._cl.targets[ni].conn
        if conn.sock is None:
            conn = self._open_target(ni, data)
        reply = self._rt(conn, data, dec)
        if reply.__class__ is MovedError:
            reply = self._redirect(reply, data, dec)
        return reply

    # ---------------------------------------------------------------- reads from replicas
    def _replica(self, ni: int) -> Optional[_Node]:
        """The open replica that reads of node ``ni`` go to; None sends them to the primary. A replica
        that cannot be opened is skipped until the next load; under the replica policy that raises."""
        c = self._cl
        node = c.reps[ni] if 0 <= ni < len(c.reps) else None
        if node is not None:
            if node.conn.sock is not None:
                return node
            try:
                node.conn.open()
                return node
            except (ConnectionError, ResponseError):
                c.reps[ni] = None
                if c.policy == cl.POLICY_REPLICA:
                    raise
                return None
        if c.policy == cl.POLICY_REPLICA:
            host, port = (c.nodes[ni].host, c.nodes[ni].port) if 0 <= ni < len(c.nodes) else ("?", 0)
            raise RuvioError(f"no replica is known for the node {host}:{port} (read_policy 'replica')")
        return None

    def _loading(self, node: _Node, reply: Any) -> bool:
        """Whether a replica's reply is ``-LOADING`` (it is still copying its primary); it is then skipped
        until the next load."""
        if not isinstance(reply, ResponseError) or not str(reply).startswith(("LOADING", "MASTERDOWN")):
            return False
        c = self._cl
        if 0 <= node.primary < len(c.reps) and c.reps[node.primary] is node:
            c.reps[node.primary] = None
        return True

    def _read_replica(self, ni: int, data: bytes, dec: bool, slot: Optional[int]) -> Any:
        """A read for node ``ni`` under a replica read policy. ``slot`` is None for an ordinary command."""
        node = self._replica(ni)
        if node is not None:
            reply = self._rt(node.conn, data, dec)
            if not self._loading(node, reply) or self._cl.policy == cl.POLICY_REPLICA:
                return reply
        return self._primary(ni, data, dec) if slot is None else self._single(slot, data, dec)

    def _read_node(self, slot: int) -> _Node:
        """The node a read part of a split command goes to."""
        ni = self._cl.slots[slot]
        if ni >= 0:
            node = self._replica(ni)
            if node is not None:
                return node
        return self._owner(slot)

    def _read_conn(self, i: int) -> _Conn:
        """The connection for target ``i`` of an all-read pipeline (the seed is the last target)."""
        c = self._cl
        if i < len(c.nodes):
            node = self._replica(i)
            if node is not None:
                return node.conn
        return c.targets[i].conn

    def _open_owner(self, slot: Optional[int]) -> _Conn:
        """Like :meth:`_open_target` for a slot (``None`` = keyless)."""
        c = self._cl
        conn = self._owner(slot).conn
        try:
            conn.open()
            return conn
        except ConnectionError as exc:
            error = exc
        if not self._refresh():
            raise error
        if slot is None or c.slots[slot] < 0:
            return self._first_reachable(error)
        return self._ensure_open(self._owner(slot).conn)

    @staticmethod
    def _ensure_open(conn: _Conn) -> _Conn:
        if conn.sock is None:
            conn.open()
        return conn

    def _first_reachable(self, error: BaseException) -> _Conn:
        """The first node of the reloaded table that opens becomes the keyless-command node."""
        c = self._cl
        for node in c.nodes:
            if node.conn.sock is None:
                try:
                    node.conn.open()
                except (ConnectionError, ResponseError):
                    continue
            c.seed = node
            c.targets[-1] = node
            return node.conn
        raise error

    def _redirect(self, err: MovedError, data: bytes, dec: bool) -> Any:
        """Follows ``-MOVED`` and ``-ASK`` (one budget of ``MAX_REDIRECTS`` per command) and re-sends after
        ``-TRYAGAIN`` (``TRYAGAIN_DELAYS``, one sleep before each re-send); the last error is returned when
        a budget runs out. ``ASK`` sends ``ASKING`` and the command to the named node and leaves the table alone."""
        reply: Any = err
        hops = moved = tries = 0
        while True:
            if cl.is_tryagain(reply):
                if tries == len(cl.TRYAGAIN_DELAYS):
                    return reply
                node = self._route_node(data)
                if node is None:
                    return reply
                time.sleep(cl.TRYAGAIN_DELAYS[tries])
                tries += 1
                reply = self._rt(node.conn, data, dec)
            else:
                if hops == cl.MAX_REDIRECTS:
                    return reply
                ask = cl.parse_ask(reply)
                if ask is not None:
                    reply = self._rt_asking(self._ask_node(ask).conn, data, dec)
                else:
                    mv = cl.parse_moved(reply)
                    if mv is None:
                        return reply
                    reply = self._rt(self._moved_to(mv, moved).conn, data, dec)
                    moved += 1
                hops += 1
            if reply.__class__ is not MovedError and not cl.is_tryagain(reply):
                return reply

    def _route_node(self, data: bytes) -> Optional[_Node]:
        """The node the current table names for a command's frame (the seed for a keyless one); None for a
        command that is split over several nodes or sent to all of them."""
        c = self._cl
        ni = cl.node_index(data, c.slots)
        if ni is not None:
            return c.targets[ni]
        name, _ = cl.frame_head(data)
        args = cl.split_frame(data)
        p = cl.plan(name.upper(), args)
        if p[0] == cl.PLAN_SLOT:
            return self._owner(p[1])
        if p[0] == cl.PLAN_SPLIT and p[1]:
            return self._owner(cl.key_slot(p[1][0])) if len({cl.key_slot(k) for k in p[1]}) == 1 else None
        return None

    def _ask_node(self, ask: Tuple[int, str, int]) -> _Node:
        """The node an ``ASK`` names; one that is not known yet gets a connection, the table is not touched."""
        _, host, port = ask
        c = self._cl
        node = c.by_addr.get((host, port))
        if node is None:
            node = self._new_node(host, port)
            c.by_addr[(host, port)] = node
        return node

    def _rt_asking(self, conn: _Conn, data: bytes, dec: bool) -> Any:
        """``ASKING`` and the command in one write; the ``ASKING`` reply is read and dropped unless it is an error."""
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not dec:
            p.decode = dec
        t = conn.opts.socket_timeout
        wait = frames_wait(data) if t is not None else 0.0
        try:
            if wait:
                conn.sock.settimeout(None if wait == FOREVER else t + wait)  # type: ignore[union-attr]
            conn.sock.sendall(cl.ASKING + data)  # type: ignore[union-attr]
            first = conn.read()
            reply = conn.read()
            if wait:
                conn.sock.settimeout(t)  # type: ignore[union-attr]
        except BaseException as exc:
            raise _fail(conn, exc) from None
        return first if isinstance(first, ResponseError) else reply

    def _moved_to(self, mv: Tuple[int, str, int], hop: int) -> _Node:
        """The node a ``MOVED`` names. The command's first ``MOVED`` to a node in the table points
        that one slot at it; an unknown node, or a second ``MOVED`` for the same command, reloads
        the whole table."""
        slot, host, port = mv
        c = self._cl
        node = c.by_addr.get((host, port))
        if node is None or node.idx < 0 or hop:
            self._refresh()
            node = c.by_addr.get((host, port))
            if node is None:
                node = self._new_node(host, port)
                c.by_addr[(host, port)] = node
        else:
            c.slots[slot] = node.idx
        return node

    def _single(self, slot: Optional[int], data: bytes, dec: bool) -> Any:
        conn = self._owner(slot).conn
        if conn.sock is None:
            conn = self._open_owner(slot)
        reply = self._rt(conn, data, dec)
        if reply.__class__ is MovedError:
            reply = self._redirect(reply, data, dec)
        return reply

    def _special(self, name: bytes, data: bytes, dec: bool) -> Any:
        args = cl.split_frame(data)
        p = cl.plan(name, args)
        kind = p[0]
        c = self._cl
        if kind == cl.PLAN_SLOT:
            if name == b"UNWATCH" and c.watch is not None:
                node, c.watch = c.watch, None
                return self._rt(node.conn, data, dec)
            if c.policy and p[1] is not None and name in cl.READS and c.slots[p[1]] >= 0:
                reply = self._read_replica(c.slots[p[1]], data, dec, p[1])
            else:
                reply = self._single(p[1], data, dec)
            if name == b"WATCH" and not isinstance(reply, ResponseError):
                c.watch = self._owner(p[1])
            return reply
        if kind == cl.PLAN_SPLIT:
            return self._split(name, args, p[1], dec)
        if kind == cl.PLAN_SCAN:
            return self._scan(args, dec)
        return self._fan(p[1], data, dec)

    def _fan(self, mode: int, data: bytes, dec: bool) -> Any:
        c = self._cl
        jobs = [(n.conn, data, 1) for n in c.nodes]
        results = self._scatter(jobs, dec)
        replies: List[Any] = []
        for r in results:
            if isinstance(r, BaseException):
                raise r
            if isinstance(r[0], ResponseError):
                return r[0]
            replies.append(r[0])
        if mode == cl.FAN_SUM:
            return sum(replies)
        if mode == cl.FAN_CAT:
            out: List[Any] = []
            for r in replies:
                out.extend(r)
            return out
        return replies[0]

    def _split(self, name: bytes, args: List[bytes], keys: List[bytes], dec: bool) -> Any:
        c = self._cl
        reads = c.policy and name in cl.READS
        groups = cl.split_groups(name, args, keys, self._read_node if reads else self._owner_by_slot)
        order = list(groups.items())
        results = self._scatter([(node.conn, b"".join(f for _, f in subs), len(subs)) for node, subs in order], dec)
        parts: List[Tuple[List[int], Any]] = []
        failure: Optional[BaseException] = None
        for (node, subs), r in zip(order, results):
            if isinstance(r, BaseException):
                failure = failure or r
                continue
            for (pos, frame), reply in zip(subs, r):
                if node.replica and self._loading(node, reply) and c.policy != cl.POLICY_REPLICA:
                    reply = self._rt(self._ensure_open(c.nodes[node.primary].conn), frame, dec)
                if reply.__class__ is MovedError:
                    reply = self._redirect(reply, frame, dec)
                parts.append((pos, reply))
        if failure is not None:
            raise failure
        return cl.merge_split(name, len(keys), parts)

    def _owner_by_slot(self, slot: int) -> _Node:
        return self._owner(slot)

    def _scan(self, args: List[bytes], dec: bool) -> Any:
        c = self._cl
        try:
            node_i, frame = cl.scan_prepare(args, len(c.nodes))
        except ResponseError as e:
            return e
        reply = self._rt(c.nodes[node_i].conn, frame, dec)
        if isinstance(reply, ResponseError):
            return reply
        return cl.scan_finish(reply, node_i, len(c.nodes))

    # ---------------------------------------------------------------- pipelines
    def _pipeline_wait(self, payload: List[bytes]) -> float:
        return pipeline_wait(payload) if self._conn.opts.socket_timeout is not None else 0.0

    def _batch(self, payload: List[bytes], count: int) -> List[Any]:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                self._discover()
            if c.mode == 2:
                return Ruvio._batch(self, payload, count)
        dec = self._decode
        tx = payload[0] is _MULTI and payload[-1] is _EXEC
        if tx:
            return self._batch_tx(payload, dec)
        n = len(payload)
        bufs = c.bufs
        try:
            idx = cl.route_pipeline(payload, c.slots, bufs)  # one pass; None = a command needs its own path
            if idx is not None:
                order = [i for i, b in enumerate(bufs) if b]
                if c.policy and cl.all_reads(payload):  # reads only: each node's replica takes its part
                    jobs = [(self._read_conn(i), b"".join(bufs[i]), len(bufs[i])) for i in order]
                else:
                    jobs = [(c.targets[i].conn, b"".join(bufs[i]), len(bufs[i])) for i in order]
                results = self._scatter(jobs, dec, self._pipeline_wait(payload))
        finally:
            for b in bufs:
                b.clear()
        if idx is None:
            return self._batch_split(payload, dec)
        failure = next((r for r in results if isinstance(r, BaseException)), None)
        if failure is not None:
            raise failure
        if len(order) == 1:
            replies = results[0]
        else:
            nxt: List[Any] = [None] * len(bufs)
            for i, r in zip(order, results):
                nxt[i] = iter(r).__next__
            replies = [nxt[i]() for i in idx]
        if MovedError in set(map(type, replies)):
            for i, v in enumerate(replies):
                if v.__class__ is MovedError:
                    replies[i] = self._redirect(v, payload[i], dec)
        return replies

    def _batch_split(self, payload: List[bytes], dec: bool) -> List[Any]:
        """A pipeline holding commands with their own routing (split, keyless, fan-out): grouped per node."""
        c = self._cl
        groups, subs = cl.pipeline_groups(payload, c.slots)  # raises before anything is sent
        order = [(c.targets[ni], pos, frames) for ni, (pos, frames) in groups.items()]
        results = self._scatter([(node.conn, b"".join(frames), len(frames)) for node, _, frames in order], dec, self._pipeline_wait(payload))
        n = len(payload)
        replies: List[Any] = [None] * (n if subs is None else n + len(subs))
        failure: Optional[BaseException] = None
        for (node, pos, frames), r in zip(order, results):
            if isinstance(r, BaseException):
                failure = failure or r
                continue
            for i, v in zip(pos, r):
                replies[i] = v
        if failure is not None:
            raise failure
        for i, v in enumerate(replies):
            if v.__class__ is MovedError:
                replies[i] = self._redirect(v, payload[i] if i < n else subs[i - n][4], dec)  # type: ignore[index]
        return replies if subs is None else cl.merge_pipeline(replies, n, subs)

    def _batch_tx(self, payload: List[bytes], dec: bool) -> List[Any]:
        c = self._cl
        slot: Optional[int] = None
        for f in payload[1:-1]:
            s = cl.pipeline_slot(f)
            if s is None:
                continue
            if slot is None:
                slot = s
            elif s != slot:
                raise CrossSlotError("CROSSSLOT the commands of a transaction must use keys in one slot (nothing was sent)")
        node = self._owner(slot)
        if c.watch is not None and c.watch is not node:
            c.watch = None
            raise CrossSlotError("CROSSSLOT the watched keys and the transaction keys live on different nodes")
        c.watch = None
        data = b"".join(payload)
        for hop in range(cl.MAX_REDIRECTS + 1):
            replies = self._read_after_write(node.conn, data, len(payload), dec)
            moved = next((r for r in replies if r.__class__ is MovedError), None)
            mv = None if moved is None or hop == cl.MAX_REDIRECTS else cl.parse_moved(moved)
            if mv is None:
                return replies
            # nothing ran (MOVED commands are refused at queue time): the whole transaction follows
            node = self._moved_to(mv, hop)
        return replies

    def _read_after_write(self, conn: _Conn, data: bytes, n: int, dec: bool) -> List[Any]:
        self._write(conn, data, dec)
        return self._read(conn, n)

    # ---------------------------------------------------------------- connection-wide state
    def _share_opts(self) -> None:
        o = self._conn.opts
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values(), *c.rnodes.values()]}.values():
            no = n.conn.opts
            no.password, no.username, no.db, no.client_name = o.password, o.username, o.db, o.client_name

    def select(self, db: int) -> Any:
        """``SELECT db`` on every node; remembered for reconnects.

        A multi-shard server only has database 0, so any other number raises.

        Args:
            db: Database number.

        Returns:
            ``True``.
        """
        ok = super().select(db)
        self._share_opts()
        return ok

    def client_setname(self, name: str) -> Any:
        """``CLIENT SETNAME name`` on every node; remembered for reconnects.

        Args:
            name: The name.

        Returns:
            ``True``.
        """
        ok = super().client_setname(name)
        self._share_opts()
        return ok

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        """``AUTH [username] password`` on every node; remembered for reconnects.

        Args:
            password: The password.
            username: ACL user; omit for the default user.

        Returns:
            ``True``.
        """
        ok = super().auth(password, username)
        self._share_opts()
        return ok

    @property
    def raw(self) -> "ClusterRuvio":
        """A view on the same nodes and slot table whose replies are ``bytes``."""
        view = ClusterRuvio.__new__(ClusterRuvio)
        view._conn = self._conn
        view._cl = self._cl
        view._decode = False
        return view

    def connect(self) -> "ClusterRuvio":
        """Opens the seed socket and discovers the slot table.

        Returns:
            The client itself.

        Raises:
            ConnectionError: The seed is unreachable.
        """
        if self._conn.sock is None:
            self._conn.open()
        if self._cl.mode == 0:
            self._discover()
        return self

    def close(self) -> None:
        """Closes every node socket. The client can be used again."""
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values(), *c.rnodes.values()]}.values():
            n.conn.close()
        c.watch = None

    def dedicated(self) -> "ClusterRuvio":
        """A new cluster client (own sockets) with the same options, for ``WATCH`` or blocking commands.

        Returns:
            A new :class:`ClusterRuvio`; close it when done.
        """
        opts = self._conn.opts.as_dict()
        opts["seeds"] = self._cl.seeds
        return ClusterRuvio(decode_responses=self._decode, cluster=True, **opts)

    def pubsub(self, ignore_subscribe_messages: bool = False) -> "ClusterPubSub":
        """Pub/Sub session on one node.

        Classic channels and patterns meet on the owner of slot 0 (where ``publish`` goes). The first
        ``ssubscribe`` connects to the owner of the channel's slot; all sharded channels of a session must live on
        that node.

        Args:
            ignore_subscribe_messages: Hide subscription confirmations.

        Returns:
            A :class:`ruvio.PubSub`.

        Raises:
            CrossSlotError: (from ``ssubscribe``) channels of one call are in different slots.
            RuvioError: (from ``ssubscribe``) the channel lives on another node than this session; open a
                second ``pubsub()``.
        """
        if self._cl.mode == 0:
            self._discover()
        return ClusterPubSub(self, ignore_subscribe_messages)


class ClusterPubSub(PubSub):
    """Pub/Sub session of a cluster client.

Classic subscriptions live on the owner of slot 0, sharded ones on the channel's owner.
    """
    def __init__(self, client: ClusterRuvio, ignore_subscribe_messages: bool = False) -> None:
        """Creates the session. Use :meth:`ClusterRuvio.pubsub` instead.

        Args:
            client: The cluster client.
            ignore_subscribe_messages: Hide subscription confirmations.
        """
        c = client._cl
        node = client._owner(0) if c.mode == 1 else c.seed
        opts = _Options(**client._conn.opts.as_dict())
        opts.host, opts.port = node.host, node.port
        if c.mode == 1:
            opts.seeds = None
        else:
            opts.seeds = c.seeds
        super().__init__(opts, client._decode, ignore_subscribe_messages)
        if c.mode != 1:
            self._conn.pos = client._conn.resume()
        self._client = client

    def ssubscribe(self, *channels: Any) -> None:
        """``SSUBSCRIBE`` on the node that owns the channels' slot.

        Args:
            *channels: Channel names; all must hash to the same slot.

        Raises:
            CrossSlotError: The channels are in different slots.
            RuvioError: The session is already connected to another node.
        """
        c = self._client._cl
        if c.mode == 1 and channels:
            slots = {cl.key_slot(ch) for ch in channels}
            if len(slots) > 1:
                raise CrossSlotError("CROSSSLOT sharded channels of one SSUBSCRIBE must share a slot")
            node = self._client._owner(slots.pop())
            o = self._conn.opts
            if self._conn.sock is None and not self.shard_channels and not self.channels and not self.patterns:
                o.host, o.port = node.host, node.port
            elif (o.host, o.port) != (node.host, node.port):
                raise RuvioError(
                    f"this Pub/Sub session is connected to {o.host}:{o.port} but the channel lives on "
                    f"{node.host}:{node.port}; open a separate pubsub() for it"
                )
        super().ssubscribe(*channels)
