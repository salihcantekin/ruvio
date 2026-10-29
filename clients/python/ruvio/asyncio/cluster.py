# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""asyncio cluster client: ``Ruvio(host, port, cluster=True)`` or :class:`ClusterRuvio`.

Same routing rules as the sync :class:`ruvio.cluster.ClusterRuvio`. Every node gets one lazily
opened, multiplexed connection (the same pipelined protocol object as the standalone client), so many
coroutines share each node connection. Commands that park a connection (``blpop``, ``xread`` with
``block``, ``fillget`` with ``wait_ms`` ...) run on a short-lived dedicated connection to the owning
node so they never stall the shared one.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, Awaitable, Dict, List, Optional, Tuple

from .. import _cluster as cl
from .._blocking import pipeline_wait
from .._commands import Conv
from .._pipeline import _EXEC, _MULTI
from ..client import _Options, _ReplicaOptions
from ..errors import ConnectionError, CrossSlotError, MovedError, ResponseError, RuvioError
from .client import PubSub, Ruvio, _Batch, _open

__all__ = ["ClusterRuvio"]

_frame_head = cl.frame_head


class _Node:
    __slots__ = ("host", "port", "opts", "proto", "lock", "idx", "replica", "primary")

    def __init__(self, host: str, port: int, opts: _Options) -> None:
        self.host = host
        self.port = port
        self.opts = opts
        self.proto: Any = None
        self.lock: Optional[asyncio.Lock] = None
        self.idx = -1
        self.replica = False  # a replica chosen for reads (READONLY is sent when its connection opens)
        self.primary = -1  # for a replica: the index of the node it serves


class _State:
    __slots__ = ("mode", "slots", "nodes", "by_addr", "seed", "watch", "gate", "targets", "policy", "reps", "rnodes")

    def __init__(self, seed: _Node) -> None:
        self.mode = 0
        self.slots: List[int] = []
        self.nodes: List[_Node] = []
        self.by_addr: Dict[Tuple[str, int], _Node] = {(seed.host, seed.port): seed}
        self.seed = seed
        self.watch: Optional[_Node] = None
        self.gate: Optional[asyncio.Lock] = None  # serializes discovery / refresh
        self.targets: List[_Node] = [seed]  # nodes + the seed last: index -1 is the seed
        self.policy = cl.POLICY_PRIMARY  # where reads go; anything but primary adds the replica paths
        self.reps: List[Optional[_Node]] = []  # per node index: the replica its reads go to (None = none usable)
        self.rnodes: Dict[Tuple[str, int], _Node] = {}  # the replica nodes of the last load, by address


class _RoutedFuture(asyncio.Future):  # type: ignore[type-arg]
    """The future of a single-key command handed to the shared connection. It behaves like any
    future; only a ``-MOVED`` / ``-ASK`` / ``-TRYAGAIN`` reply is intercepted (in ``set_exception``) and followed to the node
    the reply names. This keeps the warm path free of an extra coroutine."""

    __slots__ = ("owner", "data", "conv", "dec")

    def set_exception(self, exc: BaseException) -> None:
        if (exc.__class__ is MovedError or (exc.__class__ is ResponseError and cl.is_tryagain(exc))) and not self.done():
            asyncio.ensure_future(self.owner._retry_moved(self, exc), loop=self.get_loop())
        else:
            asyncio.Future.set_exception(self, exc)


def _consume(fut: "asyncio.Future[Any]") -> None:
    if not fut.cancelled():
        fut.exception()


def _is_blocking(name: bytes, args: List[bytes]) -> bool:
    if name in cl.BLOCKING:
        return True
    if name == b"XREAD" or name == b"XREADGROUP":
        return any(a.upper() == b"BLOCK" for a in args)
    if name == b"FILLGET":
        return any(a.upper() == b"WAIT" for a in args)
    return False


class ClusterRuvio(Ruvio):
    """Cluster-aware asyncio client. Same methods as :class:`ruvio.asyncio.Ruvio`; routing is automatic.

    Create it with ``ClusterRuvio(host, port)`` or ``Ruvio(host, port, cluster=True)``; ``host``/``port`` is any
    one shard (the seed). The slot table is discovered on the first command or ``await client.connect()``; a
    standalone server is detected there and used as a plain single connection.

    Routing rules (the same as :class:`ruvio.ClusterRuvio`):

    * Single-key commands go to the owner of the key's slot; keyless commands go to the seed node.
    * ``mget``/``mset``/``delete``/``unlink``/``exists``/``touch`` over several slots are split per node, run
      concurrently and merged in your order. Not atomic across shards.
    * Any other command whose keys span slots raises :class:`ruvio.CrossSlotError` before sending; use a
      ``{hash tag}`` to keep related keys in one slot.
    * ``dbsize`` adds up all shards, ``keys`` joins them, ``flushdb``/``flushall`` run on every shard,
      ``auth``/``select``/``client_setname`` apply to every node, ``scan`` walks shards in turn (opaque cursor).
    * Pipelines are grouped per node; transactions must use one slot.
    * ``MOVED`` and ``ASK`` are followed up to 3 times per command in all, ``TRYAGAIN`` is retried up to 7 times (sleeps doubling from 5 ms to 320 ms). Failed node connections are reopened by the next
      command; a failed command is never re-sent.
    * With ``read_policy=ReadPolicy.PREFER_REPLICA`` or ``REPLICA`` (default ``PRIMARY``), commands the server
      flags read-only and not blocking go to one replica of the key's shard, picked at random whenever the slot
      table loads; a pipeline goes to replicas only when every command in it is a read. A replica that cannot
      be connected or answers ``LOADING`` is skipped until the next load: ``PREFER_REPLICA`` uses the primary
      then, ``REPLICA`` fails. Replication is asynchronous, so a read may miss a recent write, your own included.

    Note:
        ``is_cluster`` and ``nodes`` are coroutines here (``await client.nodes()``), unlike the sync client
        where they are properties.

    Example:
        from ruvio.asyncio import ClusterRuvio

        async with ClusterRuvio("127.0.0.1", 7000) as client:
            await client.set("user:1", "Ada")
            print(await client.dbsize())
    """

    __slots__ = ("_cl",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Creates the client. Arguments are those of :class:`ruvio.asyncio.Ruvio` (``cluster`` is implied).

        Raises:
            ValueError: ``unix_socket_path`` was given; cluster mode needs ``host``/``port``.
        """
        super().__init__(*args, **kwargs)
        o = self._st.opts
        if o.unix_socket_path:
            raise ValueError("cluster mode needs host:port, not a unix socket")
        self._cl = _State(_Node(o.host, o.port, o))
        self._cl.policy = cl.policy_code(o.read_policy)

    # ---------------------------------------------------------------- connections
    async def _open_node(self, node: _Node) -> Any:
        proto = node.proto
        if proto is not None and not proto.closed:
            return proto
        if node.lock is None:
            node.lock = asyncio.Lock()
        async with node.lock:
            proto = node.proto
            if proto is None or proto.closed:
                proto = await _open(node.opts, node.opts.on_push)

                def lost(p: Any, node: _Node = node) -> None:
                    if node.proto is p:
                        node.proto = None

                proto.on_lost = lost
                node.proto = proto
            return proto

    async def _send(self, node: _Node, data: bytes, conv: Conv, dec: bool) -> Any:
        """One round trip on the node's shared connection. Raises on error replies."""
        proto = node.proto
        if proto is None or proto.closed:
            proto = await self._open_node(node)
        fut = proto.loop.create_future()
        proto.pending.append((fut, conv, dec, None))
        proto.send(data)
        return await fut

    async def _send_blocking(self, node: _Node, data: bytes, conv: Conv, dec: bool) -> Any:
        proto = await _open(node.opts, None)
        try:
            fut = proto.loop.create_future()
            proto.pending.append((fut, conv, dec, None))
            proto.transport.write(data)
            return await fut
        finally:
            proto.on_lost = None
            proto.transport.close()

    async def _node_batch(self, node: _Node, frames: List[bytes], dec: bool) -> List[Any]:
        proto = node.proto
        if proto is None or proto.closed:
            proto = await self._open_node(node)
        return await self._node_write(proto, frames, dec)

    def _node_write(self, proto: Any, frames: List[bytes], dec: bool) -> "asyncio.Future[List[Any]]":
        """Queues ``frames`` on an open connection and returns the future of their replies."""
        fut = proto.loop.create_future()
        n = len(frames)
        idle = not proto.pending and proto.wbuf is None
        entry = (_Batch(fut, n), None, dec, None)
        proto.pending.extend([entry] * n)
        data = frames[0] if n == 1 else b"".join(frames)
        wait = pipeline_wait(frames) if proto.timeout is not None else 0.0
        if idle:
            if proto.timeout is not None:
                proto.watch(wait)
            proto.transport.write(data)
        else:
            proto.send(data, wait)
        return fut

    # ---------------------------------------------------------------- topology
    def _new_node(self, host: str, port: int) -> _Node:
        o = _Options(**self._st.opts.as_dict())
        o.host = host
        o.port = port
        o.seeds = None
        o.stop_following()
        return _Node(host, port, o)

    async def _discover(self) -> None:
        c = self._cl
        if c.gate is None:
            c.gate = asyncio.Lock()
        async with c.gate:
            if c.mode != 0:
                return
            # the seed rides the base client's connection so a standalone fallback keeps using it
            await Ruvio.connect(self)
            try:
                reply = await Ruvio._x(self, None, cl.CLUSTER_SLOTS, True)
            except ResponseError:
                c.mode = 2
                return
            self._follow_seed()
            topo = cl.parse_slots(reply, c.seed.host)
            if topo is None or len(topo.nodes) < 2:
                c.mode = 2
                return
            self._detach_seed_list()
            self._st.follow = False
            self._st.opts.stop_following()
            proto = self._st.proto
            if proto is not None:
                self._adopt_seed(proto)
            self._apply(topo)
            c.mode = 1

    def _follow_seed(self) -> None:
        """Points the seed node at the address of the seed the base connection is on."""
        st = self._st
        seeds = st.opts.seeds
        if not seeds or st.cur.at < 0:
            return
        host, port = seeds[st.cur.at]
        seed = self._cl.seed
        if (host, port) != (seed.host, seed.port):
            by = self._cl.by_addr
            if by.get((seed.host, seed.port)) is seed:
                del by[(seed.host, seed.port)]
            seed.host, seed.port = host, port
            by[(host, port)] = seed

    def _detach_seed_list(self) -> None:
        """In cluster mode the seed node reconnects to its own address, not through the seed list."""
        seed = self._cl.seed
        if seed.opts.seeds:
            o = _Options(**seed.opts.as_dict())
            o.host, o.port, o.seeds = seed.host, seed.port, None
            o.stop_following()
            seed.opts = o

    def _adopt_seed(self, proto: Any) -> None:
        seed = self._cl.seed
        seed.proto = proto
        orig = proto.on_lost

        def lost(p: Any, orig: Any = orig, seed: _Node = seed) -> None:
            if orig is not None:
                orig(p)
            if seed.proto is p:
                seed.proto = None

        proto.on_lost = lost

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
            if addr not in by and n is not c.seed and n.proto is not None:
                n.proto.on_lost = None
                n.proto.transport.close()
                n.proto = None
        for n in by.values():
            if n not in nodes:
                n.idx = -1
        c.nodes = nodes
        c.by_addr = by
        c.slots = topo.slots
        c.targets = [*nodes, c.seed]
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
                    node.opts = _ReplicaOptions(**node.opts.as_dict())
                    node.replica = True
                node.primary = i
                rnodes[addr] = node
            reps.append(node)
        for addr, node in old.items():
            if addr not in rnodes and node.proto is not None:
                node.proto.on_lost = None
                node.proto.transport.close()
                node.proto = None
        c.reps = reps
        c.rnodes = rnodes

    async def _refresh(self) -> bool:
        """Reloads the slot table. Nodes with an open connection are asked first; when none answers,
        every other known node and then every seed is opened (one connect attempt each) until one
        returns a slot table. A seed that was only opened for this reload does not stay in the node list."""
        c = self._cl
        known: List[_Node] = []
        seen = set()
        for node in [c.seed, *c.nodes, *c.by_addr.values()]:
            if id(node) not in seen:
                seen.add(id(node))
                known.append(node)
        for node in known:
            if node.proto is not None and not node.proto.closed and await self._ask_slots(node):
                return True
        for node in known:
            if (node.proto is None or node.proto.closed) and await self._ask_slots(node):
                return True
        for host, port in self._st.opts.seeds or ():
            if (host, port) in c.by_addr:
                continue
            node = self._new_node(host, port)
            c.by_addr[(host, port)] = node
            if await self._ask_slots(node):
                return True
            proto, node.proto = node.proto, None
            if proto is not None and proto.transport is not None:
                proto.on_lost = None
                proto.transport.close()
            if c.by_addr.get((host, port)) is node:
                del c.by_addr[(host, port)]
        return False

    async def _ask_slots(self, node: _Node) -> bool:
        try:
            reply = await self._send(node, cl.CLUSTER_SLOTS, None, True)
        except (ConnectionError, OSError, ResponseError):
            return False
        topo = cl.parse_slots(reply, node.host)
        if topo is None:
            return False
        self._apply(topo)
        return True

    async def refresh_slots(self) -> None:
        """Re-reads ``CLUSTER SLOTS`` and rebuilds the slot table.

        Normally not needed: ``MOVED`` replies patch or reload the table. Does nothing on a standalone server.
        """
        if self._cl.mode == 0:
            await self._discover()
        elif self._cl.mode == 1:
            await self._refresh()

    async def is_cluster(self) -> bool:
        """Tells whether the seed answered ``CLUSTER SLOTS`` with a multi-node map (discovers on first use).

        Returns:
            ``True`` for a multi-shard server, ``False`` for a standalone or single-shard one.
        """
        if self._cl.mode == 0:
            await self._discover()
        return self._cl.mode == 1

    async def nodes(self) -> List[Tuple[str, int]]:
        """``(host, port)`` of the known nodes in slot order (discovers on first use).

        Returns:
            A list of tuples; empty on a standalone server.
        """
        if self._cl.mode == 0:
            await self._discover()
        return [(n.host, n.port) for n in self._cl.nodes]

    def _owner(self, slot: Optional[int]) -> _Node:
        c = self._cl
        if slot is None:
            return c.seed
        i = c.slots[slot]
        return c.nodes[i] if i >= 0 else c.seed

    # ---------------------------------------------------------------- routing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Awaitable[Any]:
        c = self._cl
        if c.mode == 2:  # standalone fallback: exactly the plain client's path
            return Ruvio._x(self, conv, data, decode)
        if c.mode == 1:
            ni = cl.node_index(data, c.slots)
            if ni is not None:
                if c.policy and ni >= 0 and cl.is_read(data):
                    return self._route(conv, data, decode)
                # warm path: no coroutine, the node's pending-reply future is returned directly
                node = c.targets[ni]
                proto = node.proto
                if proto is not None and not proto.closed:
                    dec = self._decode if decode is None else decode
                    fut = _RoutedFuture(loop=proto.loop)
                    fut.owner = self
                    fut.data = data
                    fut.conv = conv
                    fut.dec = dec
                    proto.pending.append((fut, conv, dec, None))
                    proto.send(data)
                    return fut
        return self._route(conv, data, decode)

    async def _retry_moved(self, fut: "_RoutedFuture", err: MovedError) -> None:
        try:
            reply = await self._redirect(err, fut.data, fut.conv, fut.dec, False)
        except asyncio.CancelledError:
            fut.cancel()
        except BaseException as exc:  # noqa: BLE001 - delivered to the caller
            if not fut.done():
                asyncio.Future.set_exception(fut, exc)
        else:
            if not fut.done():
                asyncio.Future.set_result(fut, reply)

    async def _route(self, conv: Conv, data: bytes, decode: Optional[bool]) -> Any:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                await self._discover()
            if c.mode == 2:
                return await Ruvio._x(self, conv, data, decode)
        dec = self._decode if decode is None else decode
        ni = cl.node_index(data, c.slots)
        if ni is not None:
            if c.policy and ni >= 0 and cl.is_read(data):
                return await self._route_read(ni, conv, data, dec, None)
            return await self._route_ordinary(ni, conv, data, dec)
        name, _ = _frame_head(data)
        return await self._special(name.upper(), data, conv, dec)

    async def _route_ordinary(self, ni: int, conv: Conv, data: bytes, dec: bool) -> Any:
        """An ordinary command on the node of target ``ni``."""
        node = self._cl.targets[ni]
        proto = node.proto
        if proto is None or proto.closed:
            proto = await self._open_target(ni, data)
        fut = proto.loop.create_future()
        proto.pending.append((fut, conv, dec, None))
        proto.send(data)
        try:
            return await fut
        except ResponseError as err:
            if err.__class__ is MovedError or cl.is_tryagain(err):
                return await self._redirect(err, data, conv, dec, False)
            raise

    # ---------------------------------------------------------------- reads from replicas
    async def _replica(self, ni: int) -> Optional[_Node]:
        """The replica that reads of node ``ni`` go to, its connection open; None sends them to the
        primary. A replica that cannot be opened is skipped until the next load; under the replica
        policy that raises."""
        c = self._cl
        node = c.reps[ni] if 0 <= ni < len(c.reps) else None
        if node is not None:
            if node.proto is not None and not node.proto.closed:
                return node
            try:
                await self._open_node(node)
                return node
            except (ConnectionError, ResponseError):
                if 0 <= ni < len(c.reps) and c.reps[ni] is node:
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

    async def _route_read(self, ni: int, conv: Conv, data: bytes, dec: bool, slot: Optional[int]) -> Any:
        """A read for node ``ni`` under a replica read policy. ``slot`` is None for an ordinary command."""
        node = await self._replica(ni)
        if node is not None:
            try:
                return await self._send(node, data, conv, dec)
            except ResponseError as exc:
                if exc.__class__ is MovedError or cl.is_tryagain(exc):
                    return await self._redirect(exc, data, conv, dec, False)
                if not self._loading(node, exc) or self._cl.policy == cl.POLICY_REPLICA:
                    raise
        if slot is None:
            return await self._route_ordinary(ni, conv, data, dec)
        return await self._single(slot, data, conv, dec, False)

    async def _open_target(self, ni: int, data: bytes) -> Any:
        """Opens the connection of target ``ni`` for an ordinary command. When the node cannot be
        reached the slot table is reloaded and the command goes to the owner the new table names (a
        keyless one to the first reachable node)."""
        c = self._cl
        try:
            return await self._open_node(c.targets[ni])
        except ConnectionError as exc:
            error = exc
        if not await self._refresh():
            raise error
        ni2 = cl.node_index(data, c.slots)
        if ni2 is None or ni2 < 0:
            return await self._first_reachable(error)
        return await self._open_node(c.targets[ni2])

    async def _open_owner(self, slot: Optional[int]) -> _Node:
        """Like :meth:`_open_target` for a slot (``None`` = keyless); returns the node that is open."""
        c = self._cl
        node = self._owner(slot)
        try:
            await self._open_node(node)
            return node
        except ConnectionError as exc:
            error = exc
        if not await self._refresh():
            raise error
        if slot is None or c.slots[slot] < 0:
            await self._first_reachable(error)
            return c.seed
        node = self._owner(slot)
        await self._open_node(node)
        return node

    async def _first_reachable(self, error: BaseException) -> Any:
        """The first node of the reloaded table that opens becomes the keyless-command node."""
        c = self._cl
        for node in c.nodes:
            try:
                proto = await self._open_node(node)
            except (ConnectionError, ResponseError):
                continue
            c.seed = node
            c.targets[-1] = node
            return proto
        raise error

    async def _redirect(
        self, err: ResponseError, data: bytes, conv: Conv, dec: bool, blocking: bool, clock: Optional[cl.TryAgainClock] = None
    ) -> Any:
        """Follows ``-MOVED`` and ``-ASK`` (one budget of ``MAX_REDIRECTS`` per command) and re-sends after
        ``-TRYAGAIN`` (``TRYAGAIN_DELAYS``, one ``asyncio.sleep`` before each re-send); the last error is raised
        when a budget runs out. ``ASK`` sends ``ASKING`` and the command to the named node and leaves the table alone."""
        hops = moved = tries = 0
        while True:
            ask = None
            if cl.is_tryagain(err):
                node = self._route_node(data)
                if node is None or tries == len(cl.TRYAGAIN_DELAYS):
                    raise err
                # One command keeps the fixed schedule; a pipeline's commands share a clock.
                pause = cl.TRYAGAIN_DELAYS[tries] if clock is None else clock.pause(tries)
                if pause is None:
                    raise err
                if pause > 0:
                    await asyncio.sleep(pause)
                tries += 1
            else:
                if hops == cl.MAX_REDIRECTS:
                    raise err
                ask = cl.parse_ask(err)
                if ask is not None:
                    node = self._ask_node(ask)
                else:
                    mv = cl.parse_moved(err)
                    if mv is None:
                        raise err
                    node = await self._moved_to(mv, moved)
                    moved += 1
                hops += 1
            try:
                if ask is not None:
                    return await self._send_asking(node, data, conv, dec, blocking)
                if blocking:
                    return await self._send_blocking(node, data, conv, dec)
                return await self._send(node, data, conv, dec)
            except ResponseError as e:
                if e.__class__ is not MovedError and not cl.is_tryagain(e):
                    raise
                err = e

    def _route_node(self, data: bytes) -> Optional[_Node]:
        """The node the current table names for a command's frame (the seed for a keyless one); None for a
        command that is split over several nodes or sent to all of them."""
        c = self._cl
        ni = cl.node_index(data, c.slots)
        if ni is not None:
            return c.targets[ni]
        name, _ = _frame_head(data)
        p = cl.plan(name.upper(), cl.split_frame(data))
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

    async def _send_asking(self, node: _Node, data: bytes, conv: Conv, dec: bool, blocking: bool) -> Any:
        """``ASKING`` and the command in one write; the ``ASKING`` reply is dropped unless it is an error."""
        if blocking:
            proto = await _open(node.opts, None)
        else:
            proto = node.proto
            if proto is None or proto.closed:
                proto = await self._open_node(node)
        try:
            afut = proto.loop.create_future()
            fut = proto.loop.create_future()
            proto.pending.append((afut, None, dec, None))
            proto.pending.append((fut, conv, dec, None))
            if blocking:
                proto.transport.write(cl.ASKING + data)
            else:
                proto.send(cl.ASKING + data)
            try:
                await afut
            except BaseException:
                fut.add_done_callback(_consume)  # the command still gets its reply: read it, drop it
                raise
            return await fut
        finally:
            if blocking:
                proto.on_lost = None
                proto.transport.close()

    async def _moved_to(self, mv: Tuple[int, str, int], hop: int) -> _Node:
        """The node a ``MOVED`` names. The command's first ``MOVED`` to a node in the table points
        that one slot at it; an unknown node, or a second ``MOVED`` for the same command, reloads
        the whole table."""
        slot, host, port = mv
        c = self._cl
        node = c.by_addr.get((host, port))
        if node is None or node.idx < 0 or hop:
            await self._refresh()
            node = c.by_addr.get((host, port))
            if node is None:
                node = self._new_node(host, port)
                c.by_addr[(host, port)] = node
        else:
            c.slots[slot] = node.idx
        return node

    async def _single(self, slot: Optional[int], data: bytes, conv: Conv, dec: bool, blocking: bool) -> Any:
        node = self._owner(slot)
        try:
            if blocking:
                return await self._send_blocking(node, data, conv, dec)
            if node.proto is None or node.proto.closed:
                node = await self._open_owner(slot)
            return await self._send(node, data, conv, dec)
        except ResponseError as err:
            if err.__class__ is MovedError or cl.is_tryagain(err):
                return await self._redirect(err, data, conv, dec, blocking)
            raise

    async def _special(self, name: bytes, data: bytes, conv: Conv, dec: bool) -> Any:
        args = cl.split_frame(data)
        p = cl.plan(name, args)
        kind = p[0]
        c = self._cl
        if kind == cl.PLAN_SLOT:
            if name == b"UNWATCH" and c.watch is not None:
                node, c.watch = c.watch, None
                return await self._send(node, data, conv, dec)
            blocking = _is_blocking(name, args)
            if c.policy and not blocking and p[1] is not None and name in cl.READS and c.slots[p[1]] >= 0:
                reply = await self._route_read(c.slots[p[1]], conv, data, dec, p[1])
            else:
                reply = await self._single(p[1], data, conv, dec, blocking)
            if name == b"WATCH":
                c.watch = self._owner(p[1])
            return reply
        if kind == cl.PLAN_SPLIT:
            return self._conv(conv, await self._split(name, args, p[1], dec))
        if kind == cl.PLAN_SCAN:
            return self._conv(conv, await self._scan(args, dec))
        return self._conv(conv, await self._fan(p[1], data, dec))

    @staticmethod
    def _conv(conv: Conv, reply: Any) -> Any:
        if isinstance(reply, ResponseError):
            raise reply
        return reply if conv is None else conv(reply)

    async def _gather(self, coros: List[Awaitable[Any]]) -> List[Any]:
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException) and not isinstance(r, ResponseError):
                raise r
        return list(results)

    async def _fan(self, mode: int, data: bytes, dec: bool) -> Any:
        results = await self._gather([self._send(n, data, None, dec) for n in self._cl.nodes])
        for r in results:
            if isinstance(r, ResponseError):
                raise r
        if mode == cl.FAN_SUM:
            return sum(results)
        if mode == cl.FAN_CAT:
            out: List[Any] = []
            for r in results:
                out.extend(r)
            return out
        return results[0]

    async def _split(self, name: bytes, args: List[bytes], keys: List[bytes], dec: bool) -> Any:
        c = self._cl
        reads = c.policy and name in cl.READS
        if reads:
            for ni in {c.slots[cl.key_slot(k)] for k in keys}:
                await self._replica(ni)  # opens the replicas; one that cannot be opened is dropped
        groups = cl.split_groups(name, args, keys, self._read_owner_by_slot if reads else self._owner_by_slot)
        order = list(groups.items())
        results = await self._gather([self._node_batch(node, [f for _, f in subs], dec) for node, subs in order])
        parts: List[Tuple[List[int], Any]] = []
        for (node, subs), r in zip(order, results):
            if isinstance(r, BaseException):
                raise r
            for (pos, frame), reply in zip(subs, r):
                if node.replica and self._loading(node, reply) and c.policy != cl.POLICY_REPLICA:
                    try:
                        reply = await self._send(c.nodes[node.primary], frame, None, dec)
                    except MovedError as err:
                        reply = err
                    except ResponseError as exc:
                        reply = exc
                if reply.__class__ is MovedError:
                    try:
                        reply = await self._redirect(reply, frame, None, dec, False)
                    except ResponseError as e:
                        reply = e
                parts.append((pos, reply))
        return cl.merge_split(name, len(keys), parts)

    def _owner_by_slot(self, slot: int) -> _Node:
        return self._owner(slot)

    def _read_owner_by_slot(self, slot: int) -> _Node:
        """The node a read part of a split command goes to: the replica of the slot's node when it is usable."""
        c = self._cl
        ni = c.slots[slot]
        node = c.reps[ni] if 0 <= ni < len(c.reps) else None
        return node if node is not None else self._owner(slot)

    async def _scan(self, args: List[bytes], dec: bool) -> Any:
        c = self._cl
        try:
            node_i, frame = cl.scan_prepare(args, len(c.nodes))
        except ResponseError as e:
            return e
        reply = await self._send(c.nodes[node_i], frame, None, dec)
        return cl.scan_finish(reply, node_i, len(c.nodes))

    # ---------------------------------------------------------------- pipelines
    def _batch(self, payload: List[bytes]) -> "Awaitable[List[Any]]":
        if self._cl.mode == 2:
            return Ruvio._batch(self, payload)
        return self._cbatch(payload)

    async def _cbatch(self, payload: List[bytes]) -> List[Any]:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                await self._discover()
            if c.mode == 2:
                return await Ruvio._batch(self, payload)
        dec = self._decode
        if payload[0] is _MULTI and payload[-1] is _EXEC:
            return await self._batch_tx(payload, dec)
        bufs: List[List[bytes]] = [[] for _ in c.targets]  # fresh: other coroutines may run while we await
        idx = cl.route_pipeline(payload, c.slots, bufs)  # one pass; None = a command needs its own path
        if idx is None:
            return await self._batch_split(payload, dec)
        order = [i for i, b in enumerate(bufs) if b]
        targets = c.targets
        if c.policy and cl.all_reads(payload):  # reads only: each node's replica takes its part
            targets = list(targets)
            for i in order:
                if i < len(bufs) - 1:  # the last target is the seed (keyless commands)
                    node = await self._replica(i)
                    if node is not None:
                        targets[i] = node
        if len(order) == 1:
            replies = await self._node_batch(targets[order[0]], payload, dec)
        else:
            nodes = [targets[i] for i in order]
            if all(n.proto is not None and not n.proto.closed for n in nodes):
                # every connection is open: queue all the writes now, then wait for the replies
                futs = [self._node_write(n.proto, bufs[i], dec) for n, i in zip(nodes, order)]
                results = await asyncio.gather(*futs, return_exceptions=True)
            else:
                results = await self._gather([self._node_batch(n, bufs[i], dec) for n, i in zip(nodes, order)])
            failure = next((r for r in results if isinstance(r, BaseException)), None)
            if failure is not None:
                raise failure
            nxt: List[Any] = [None] * len(bufs)
            for i, r in zip(order, results):
                nxt[i] = iter(r).__next__
            replies = [nxt[i]() for i in idx]
        if MovedError in set(map(type, replies)):
            clock = cl.TryAgainClock()
            for i, v in enumerate(replies):
                if v.__class__ is MovedError:
                    try:
                        replies[i] = await self._redirect(v, payload[i], None, dec, False, clock)
                    except ResponseError as e:
                        replies[i] = e
        return replies

    async def _batch_split(self, payload: List[bytes], dec: bool) -> List[Any]:
        """A pipeline holding commands with their own routing (split, keyless, fan-out): grouped per node."""
        c = self._cl
        groups, subs = cl.pipeline_groups(payload, c.slots)  # raises before anything is sent
        order = [(c.targets[ni], pos, frames) for ni, (pos, frames) in groups.items()]
        results = await self._gather([self._node_batch(node, frames, dec) for node, _, frames in order])
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
        clock = cl.TryAgainClock()
        for i, v in enumerate(replies):
            if v.__class__ is MovedError:
                frame = payload[i] if i < n else subs[i - n][4]  # type: ignore[index]
                try:
                    replies[i] = await self._redirect(v, frame, None, dec, False, clock)
                except ResponseError as e:
                    replies[i] = e
        return replies if subs is None else cl.merge_pipeline(replies, n, subs)

    async def _batch_tx(self, payload: List[bytes], dec: bool) -> List[Any]:
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
        replies: List[Any] = []
        for hop in range(cl.MAX_REDIRECTS + 1):
            replies = await self._node_batch(node, payload, dec)
            moved = next((r for r in replies if r.__class__ is MovedError), None)
            mv = None if moved is None or hop == cl.MAX_REDIRECTS else cl.parse_moved(moved)
            if mv is None:
                break
            # nothing ran (MOVED commands are refused at queue time): the whole transaction follows
            node = await self._moved_to(mv, hop)
        return replies

    # ---------------------------------------------------------------- connection-wide state
    def _share_opts(self) -> None:
        o = self._st.opts
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values(), *c.rnodes.values()]}.values():
            no = n.opts
            no.password, no.username, no.db, no.client_name = o.password, o.username, o.db, o.client_name

    async def select(self, db: int) -> Any:  # type: ignore[override]
        """``SELECT db`` on every node; remembered for reconnects. A multi-shard server only has database 0.

        Args:
            db: Database number.

        Returns:
            ``True`` (awaitable).
        """
        ok = await super().select(db)
        self._share_opts()
        return ok

    async def client_setname(self, name: str) -> Any:  # type: ignore[override]
        """``CLIENT SETNAME name`` on every node; remembered for reconnects.

        Args:
            name: The name.

        Returns:
            ``True`` (awaitable).
        """
        ok = await super().client_setname(name)
        self._share_opts()
        return ok

    async def auth(self, password: str, username: Optional[str] = None) -> Any:  # type: ignore[override]
        """``AUTH [username] password`` on every node; remembered for reconnects.

        Args:
            password: The password.
            username: ACL user; omit for the default user.

        Returns:
            ``True`` (awaitable).
        """
        ok = await super().auth(password, username)
        self._share_opts()
        return ok

    @property
    def raw(self) -> "ClusterRuvio":
        """A view on the same nodes and slot table whose replies are ``bytes``."""
        view = ClusterRuvio.__new__(ClusterRuvio)
        view._st = self._st
        view._cl = self._cl
        view._decode = False
        return view

    async def connect(self) -> "ClusterRuvio":
        """Discovers the slot table (opening the seed connection).

        Returns:
            The client itself.

        Raises:
            ConnectionError: The seed is unreachable.
        """
        if self._cl.mode == 0:
            await self._discover()
        elif self._cl.mode == 2:
            await Ruvio.connect(self)
        return self

    async def aclose(self) -> None:
        """Closes every node connection. The client can be used again. ``close`` is an alias."""
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values(), *c.rnodes.values()]}.values():
            proto, n.proto = n.proto, None
            if proto is not None and proto.transport is not None:
                proto.transport.close()
        c.watch = None
        await Ruvio.aclose(self)

    close = aclose

    def dedicated(self) -> "ClusterRuvio":
        """A new cluster client (own connections) with the same options, for ``WATCH`` and blocking commands.

        Returns:
            A new :class:`ClusterRuvio`; close it with ``await worker.aclose()``.
        """
        opts = self._st.opts.for_constructor()
        return ClusterRuvio(decode_responses=self._decode, cluster=True, **opts)

    async def pubsub(self, ignore_subscribe_messages: bool = False) -> "ClusterPubSub":  # type: ignore[override]
        """Creates and connects a Pub/Sub session on one node (``session = await client.pubsub()``).

        Classic channels and patterns meet on the owner of slot 0 (where ``publish`` goes). A first
        ``ssubscribe`` connects to the owner of the channel's slot.

        Args:
            ignore_subscribe_messages: Hide subscription confirmations.

        Returns:
            A connected :class:`ruvio.asyncio.PubSub`.
        """
        if self._cl.mode == 0:
            await self._discover()
        ps = ClusterPubSub(self, ignore_subscribe_messages)
        await ps.connect()
        return ps


class ClusterPubSub(PubSub):
    """Pub/Sub session of an asyncio cluster client.

Classic subscriptions live on the owner of slot 0, sharded ones on the channel's owner.
    """
    def __init__(self, client: ClusterRuvio, ignore_subscribe_messages: bool = False) -> None:
        """Creates the session. Use ``await ClusterRuvio.pubsub()`` instead.

        Args:
            client: The cluster client.
            ignore_subscribe_messages: Hide subscription confirmations.
        """
        c = client._cl
        node = client._owner(0) if c.mode == 1 else c.seed
        opts = _Options(**client._st.opts.as_dict())
        opts.host, opts.port = node.host, node.port
        if c.mode == 1:
            opts.seeds = None
        super().__init__(opts, client._decode, ignore_subscribe_messages)
        if c.mode != 1:
            self._cur.pos = client._st.cur.resume()
        self._client = client

    async def ssubscribe(self, *channels: Any) -> None:
        """``SSUBSCRIBE`` on the node that owns the channels' slot.

        Args:
            *channels: Channel names; all must hash to the same slot.

        Raises:
            CrossSlotError: The channels are in different slots.
            RuvioError: The session already has subscriptions on another node; open a second ``pubsub()``.
        """
        c = self._client._cl
        if c.mode == 1 and channels:
            slots = {cl.key_slot(ch) for ch in channels}
            if len(slots) > 1:
                raise CrossSlotError("CROSSSLOT sharded channels of one SSUBSCRIBE must share a slot")
            node = self._client._owner(slots.pop())
            o = self._opts
            if (o.host, o.port) != (node.host, node.port):
                if self.channels or self.patterns or self.shard_channels:
                    raise RuvioError(
                        f"this Pub/Sub session is connected to {o.host}:{o.port} but the channel lives on "
                        f"{node.host}:{node.port}; open a separate pubsub() for it"
                    )
                await self.aclose()
                o.host, o.port = node.host, node.port
        await super().ssubscribe(*channels)
