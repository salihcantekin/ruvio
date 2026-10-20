# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""asyncio Ruvio client: one multiplexed, pipelined connection shared by all coroutines.

Main entry points (also exported from :mod:`ruvio.asyncio`):

* :class:`Ruvio` - the client; every server command is a method returning an awaitable
  (``value = await client.get("k")``). The commands are documented on
  :class:`ruvio._commands.Commands`; their examples are written without ``await``.
* :class:`Pipeline` - batches commands into one write, optionally as ``MULTI``/``EXEC``.
* :class:`PubSub` - a Pub/Sub session on its own connection.
* :class:`Lock` - a distributed lock.

How it works:

* Every command appends ``(future, converter, decode)`` to a FIFO queue and writes its encoded
  frame. Replies arrive in order, so the reader resolves the futures in order.
* The reader is an :class:`asyncio.BufferedProtocol`: the event loop reads straight into the parser's
  reusable buffer.
* On an idle connection a frame is written at once. While replies are in flight, frames queued in the
  same loop iteration are joined and written together (capped at 64 KiB per write).

Example:
    import asyncio
    from ruvio.asyncio import Ruvio

    async def main():
        async with Ruvio("127.0.0.1", 6379) as client:
            await client.set("greeting", "hello")
            print(await client.get("greeting"))

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections import deque
from typing import Any, AsyncIterator, Awaitable, Callable, Deque, Dict, List, Optional, Sequence, Tuple, Union

from .._commands import Commands, Conv, KeyT, KeyTypeT
from .._models import Message
from .._pipeline import PipelineBase
from .._resp import NEED, Push, encode, make_parser
from .._cluster import ReadPolicy, policy_code
from .._follow import MASTER, ROUNDS, WAITS, Address, parse_master_addr, parse_role, probe_wait
from ..client import _SUB_KINDS, _message, _Options, _parse_seeds, parse_url
from ..errors import ConnectionError, LockError, ProtocolError, ResponseError, RuvioError, TimeoutError, WatchError

__all__ = ["Ruvio", "Pipeline", "PubSub", "Lock"]

_WRITE_CAP = 65536


class _Batch:
    """Collects ``count`` consecutive replies into one future (pipelines)."""

    __slots__ = ("fut", "left", "items")

    def __init__(self, fut: "asyncio.Future[Any]", count: int) -> None:
        self.fut = fut
        self.left = count
        self.items: List[Any] = []

    def add(self, v: Any) -> None:
        self.items.append(v)
        self.left -= 1
        if not self.left and not self.fut.done():
            self.fut.set_result(self.items)


class _Proto(asyncio.BufferedProtocol):
    def __init__(self, parser: Any, loop: asyncio.AbstractEventLoop, on_push: Optional[Callable[[Any], None]]) -> None:
        self.parser = parser
        self.loop = loop
        self.pending: Deque[Tuple[Any, Conv, bool]] = deque()
        self.transport: Optional[asyncio.Transport] = None
        self.wbuf: Optional[List[bytes]] = None
        self.wsize = 0
        self.on_push = on_push
        self.closed = False
        self.on_lost: Optional[Callable[["_Proto"], None]] = None
        self.lost_exc: Optional[BaseException] = None

    # -- transport callbacks -------------------------------------------------
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def get_buffer(self, sizehint: int) -> memoryview:
        return self.parser.get_buffer(16384)

    def buffer_updated(self, nbytes: int) -> None:
        p = self.parser
        p.end += nbytes
        pending = self.pending
        while True:
            if pending:
                dec = pending[0][2]
                if p.decode is not dec:
                    p.decode = dec
            try:
                v = p.gets()
            except Exception as exc:  # protocol error: the stream is unusable
                self._fail(exc)
                if self.transport is not None:
                    self.transport.close()
                return
            if v is NEED:
                return
            if not pending or type(v) is Push:
                if self.on_push is not None:
                    self.on_push(v)
                continue
            target, conv, _ = pending.popleft()
            if target.__class__ is _Batch:
                target.add(v)
                continue
            if target.done():  # cancelled by the caller; the reply is dropped
                continue
            if isinstance(v, ResponseError):
                target.set_exception(v)
            elif conv is None:
                target.set_result(v)
            else:
                try:
                    target.set_result(conv(v))
                except Exception as exc:  # noqa: BLE001
                    target.set_exception(exc)

    def eof_received(self) -> Optional[bool]:
        return False

    def connection_lost(self, exc: Optional[BaseException]) -> None:
        self.closed = True
        self.wbuf = None
        self._fail(ConnectionError(f"connection lost: {exc}" if exc else "connection closed by server"))
        if self.on_lost is not None:
            self.on_lost(self)

    def _fail(self, exc: BaseException) -> None:
        self.lost_exc = exc
        pending = self.pending
        while pending:
            target = pending.popleft()[0]
            fut = target.fut if target.__class__ is _Batch else target
            if not fut.done():
                fut.set_exception(exc)

    # -- writing -------------------------------------------------------------
    def send(self, data: bytes) -> None:
        wbuf = self.wbuf
        if wbuf is not None:
            wbuf.append(data)
            self.wsize += len(data)
            if self.wsize >= _WRITE_CAP:
                self.flush()
        elif len(self.pending) <= 1:
            self.transport.write(data)  # type: ignore[union-attr]
        else:
            self.wbuf = [data]
            self.wsize = len(data)
            self.loop.call_soon(self.flush)

    def flush(self) -> None:
        wbuf = self.wbuf
        if wbuf is None:
            return
        self.wbuf = None
        if self.closed:
            return
        self.transport.write(wbuf[0] if len(wbuf) == 1 else b"".join(wbuf))  # type: ignore[union-attr]


class _Cursor:
    """Where the next connect starts in the seed list: ``pos`` is the seed after the one last connected
    (so a reconnect ends with the address that dropped), ``at`` the seed connected to (-1 before the
    first connect). A failed open leaves both alone."""

    __slots__ = ("pos", "at", "addrs", "coord", "multi", "once")

    def __init__(self, opts: Optional[_Options] = None) -> None:
        self.pos = 0
        self.at = -1
        #: The addresses a connect tries in order: the seeds, plus any primary a replica reported; with
        #: ``sentinel_master`` just the primary last found (empty before the first connect).
        self.addrs: Optional[List[Address]] = None
        #: With ``sentinel_master``, the coordinators to ask.
        self.coord: Optional[List[Address]] = None
        #: True between an acknowledged ``MULTI`` and ``EXEC``/``DISCARD`` sent through the client.
        self.multi = False
        #: True while the command a ``READONLY`` reply refused is sent its second time.
        self.once = False
        if opts is not None:
            if opts.sentinel_master is not None:
                self.addrs = []
                self.coord = list(opts.seeds) if opts.seeds else [(opts.host, opts.port)]
            elif opts.seeds:
                self.addrs = list(opts.seeds)

    def following(self, opts: _Options) -> bool:
        """Following is active: the option is on and there are at least two seeds or a coordinator name."""
        if not opts.follow_primary:
            return False
        if opts.sentinel_master is not None:
            return True
        return bool(opts.seeds) and len(self.addrs or ()) > 1

    def address(self) -> Optional[Address]:
        """The address the connection is on, or ``None`` before the first connect."""
        if self.addrs is not None and self.at >= 0:
            return self.addrs[self.at]
        return None

    def resume(self) -> int:
        """The seed index a new connection that should join this one starts at."""
        return self.at if self.at >= 0 else self.pos


class _State:
    """Connection state shared by a client and its ``raw`` view."""

    __slots__ = ("opts", "proto", "lock", "loop", "cur", "follow", "mover")

    def __init__(self, opts: _Options) -> None:
        self.cur = _Cursor(opts)
        self.opts = opts
        #: Following a failover is active (a cluster client turns it off once it routes by slot).
        self.follow = self.cur.following(opts)
        #: Serializes the moves to a primary, so many refused commands run one search.
        self.mover: Optional[asyncio.Lock] = None
        self.proto: Optional[_Proto] = None
        self.lock: Optional[asyncio.Lock] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None


async def _dial(
    opts: _Options, host: str, port: int, on_push: Optional[Callable[[Any], None]]
) -> _Proto:  # noqa: E501
    loop = asyncio.get_running_loop()
    parser = make_parser(opts.parser or "auto")
    try:
        if opts.unix_socket_path:
            coro = loop.create_unix_connection(lambda: _Proto(parser, loop, on_push), opts.unix_socket_path)
        else:
            coro = loop.create_connection(lambda: _Proto(parser, loop, on_push), host, port)
        _, proto = await asyncio.wait_for(coro, opts.connect_timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"connect to {host}:{port} timed out") from exc
    except OSError as exc:
        raise ConnectionError(f"cannot connect to {host}:{port}: {exc}") from exc
    return proto


async def _take(
    opts: _Options, cur: _Cursor, address: Address, on_push: Optional[Callable[[Any], None]]
) -> _Proto:
    """Connects to ``address`` (a node that answered ``master``) and makes it the seed the connection is on."""
    proto = await _dial(opts, address[0], address[1], on_push)
    if opts.sentinel_master is not None:
        cur.addrs = [address]
        cur.at = 0
        cur.pos = 0
        return proto
    addrs = cur.addrs
    if address not in addrs:  # type: ignore[operator]
        addrs.append(address)  # type: ignore[union-attr]
    i = addrs.index(address)  # type: ignore[union-attr]
    opts.host, opts.port = address
    cur.at = i
    cur.pos = (i + 1) % len(addrs)  # type: ignore[arg-type]
    return proto


async def _probe(address: Address, opts: _Options, command: str, args: Tuple[Any, ...]) -> Any:
    """One short-lived connection for one question. The reply, or ``None`` when the address cannot be
    reached, times out, drops or answers an error. A refused ``AUTH`` raises, as connecting does."""
    po = _Options(**opts.as_dict())
    po.host, po.port = address
    po.seeds = None
    po.db = 0
    po.client_name = None
    po.durability_replicas = None
    po.durability_timeout_ms = None
    po.on_push = None
    po.stop_following()
    wait = probe_wait(opts.connect_timeout, None)
    total = None if wait is None or opts.connect_timeout is None else opts.connect_timeout + wait
    try:
        proto = await asyncio.wait_for(_open(po), total)
    except ResponseError:
        raise
    except (RuvioError, OSError, ValueError, asyncio.TimeoutError):
        return None
    try:
        fut = proto.loop.create_future()
        proto.pending.append((fut, None, True))
        proto.send(encode(command, args))
        return await asyncio.wait_for(fut, wait)
    except (RuvioError, OSError, ValueError, asyncio.TimeoutError):
        return None
    finally:
        proto.transport.close()  # type: ignore[union-attr]


async def _find_primary(nodes: List[Address], start: int, avoid: Optional[Address], opts: _Options) -> Optional[Address]:
    """One round: the address of a node that answered ``master``, asking ``nodes`` in order from ``start``
    (a replica's reported primary is asked once, after them), or ``None``. With ``sentinel_master`` the nodes
    are coordinators, each asked for the master's address, which ``ROLE`` must confirm. ``avoid`` is the
    address that just answered ``READONLY``. Raises only when ``AUTH`` is refused."""
    n = len(nodes)
    queue = [nodes[(start + k) % n] for k in range(n)]
    name = opts.sentinel_master
    if name is not None:
        for coordinator in queue:
            named = parse_master_addr(await _probe(coordinator, opts, "SENTINEL", ("get-master-addr-by-name", name)))
            if named is not None and named != avoid:
                role = parse_role(await _probe(named, opts, "ROLE", ()))
                if role is not None and role[0] == MASTER:
                    return named
        return None
    seen = set(queue)
    i = 0
    while i < len(queue):
        address = queue[i]
        i += 1
        role = parse_role(await _probe(address, opts, "ROLE", ()))
        if role is None:
            continue
        if role[0] == MASTER:
            if address != avoid:
                return address
        elif role[1] is not None and role[1] not in seen:
            seen.add(role[1])
            queue.append(role[1])
    return None


async def _connect(opts: _Options, on_push: Optional[Callable[[Any], None]], cur: Optional[_Cursor]) -> _Proto:
    """A connection (no handshake yet). The first connect keeps the seed order (a coordinator name asks the
    coordinators instead); a reconnect first asks which node is the primary and falls back to the seed order."""
    seeds = opts.seeds
    sentinel = opts.sentinel_master is not None and cur is not None
    if cur is not None and cur.following(opts):
        if sentinel or cur.at >= 0:
            found = await _find_primary(cur.coord if sentinel else cur.addrs, cur.pos, None, opts)  # type: ignore[arg-type]
            if found is not None:
                try:
                    return await _take(opts, cur, found, on_push)
                except ConnectionError:
                    if sentinel and cur.at < 0:
                        raise
            elif sentinel and cur.at < 0:
                raise ProtocolError(f"no coordinator names a primary for {opts.sentinel_master!r}")
    if sentinel:
        if not cur.addrs:  # type: ignore[union-attr]
            raise ConnectionError("no primary to connect to")
        proto = await _dial(opts, cur.addrs[0][0], cur.addrs[0][1], on_push)  # type: ignore[union-attr]
        cur.at = 0  # type: ignore[union-attr]
        return proto
    if seeds:
        if cur is None:
            cur = _Cursor(opts)
        addrs = cur.addrs if cur.addrs is not None else list(seeds)
        n = len(addrs)
        error: Optional[ConnectionError] = None
        for k in range(n):
            i = (cur.pos + k) % n
            try:
                proto = await _dial(opts, addrs[i][0], addrs[i][1], on_push)
            except ConnectionError as exc:
                error = exc
                continue
            cur.at = i
            cur.pos = (i + 1) % n
            return proto
        raise error  # type: ignore[misc]
    return await _dial(opts, opts.host, opts.port, on_push)


async def _open(
    opts: _Options, on_push: Optional[Callable[[Any], None]] = None, cur: Optional[_Cursor] = None
) -> _Proto:
    """Opens a connection and runs the handshake. With seeds, one connect attempt per seed from
    ``cur.pos`` round to the seed before it; a seed that cannot be reached is skipped, the last connect
    error is raised when none answers, and a refused handshake raises at once. With following active
    a reconnect (and, with a coordinator name, the first connect) looks for the primary first."""
    if cur is not None:
        cur.multi = False
    return await _handshake(await _connect(opts, on_push, cur), opts)


async def _handshake(proto: _Proto, opts: _Options) -> _Proto:
    """Runs ``AUTH``/``SELECT``/``CLIENT SETNAME``/``CLIENT DURABILITY`` on a new connection; closes it on failure."""
    loop = proto.loop
    hs = opts.handshake()
    if hs:
        futs = []
        for conv, data in hs:
            fut = loop.create_future()
            proto.pending.append((fut, conv, True))
            futs.append(fut)
        proto.transport.write(b"".join(d for _, d in hs))  # type: ignore[union-attr]
        try:
            for fut in futs:
                await fut
        except BaseException:
            proto.transport.close()  # type: ignore[union-attr]
            raise
    return proto


class Ruvio(Commands):
    """asyncio Ruvio client: ``await client.get("k")``. One connection is shared by every coroutine.

    Commands from different tasks are pipelined on the shared connection, so one client serves a whole
    application. Blocking commands (``blpop``, ``xread`` with ``block``, ``fillget`` with ``wait_ms`` ...)
    hold the shared connection for everyone; run them on :meth:`dedicated`. A lost connection fails the
    in-flight commands with :class:`ruvio.ConnectionError` and is reopened by the next command (the failed
    commands are never re-sent).

    Args:
        host: Server host (default ``127.0.0.1``); the seed address when ``cluster=True``.
        port: Server port (default 6379).
        db: Logical database selected on connect (default 0).
        password: Password for ``AUTH``, sent on every (re)connect.
        username: ACL user for ``AUTH``; omit for the default user.
        client_name: Name sent with ``CLIENT SETNAME`` on connect.
        durability_replicas: When set (above 0), ``CLIENT DURABILITY REPLICAS n`` is sent on every connect: the server
            holds each reply until ``n`` replicas have the writes on disk. Needs a server with replication.
        durability_timeout_ms: Milliseconds the server waits for the replicas (its default is 5000); used only with
            ``durability_replicas``.
        connect_timeout: Seconds allowed to open the connection (default 5.0, ``None`` for no limit). There is
            no per-command socket timeout; wrap calls in :func:`asyncio.wait_for` instead.
        decode_responses: ``True`` (default) returns ``str``, ``False`` returns ``bytes``.
        unix_socket_path: Connect to this Unix socket instead of host/port.
        parser: ``"auto"`` (hiredis when installed), ``"python"`` or ``"hiredis"``.
        on_push: Callback for RESP3 push messages (for example client-tracking ``invalidate``).
        cluster: ``True`` returns :class:`ruvio.asyncio.ClusterRuvio`, which routes each key to its shard over
            one connection per node. A standalone server is detected on the first command.
        seeds: Addresses to try in order instead of ``host``/``port``: ``"host:port"`` strings (IPv6 as
            ``"[::1]:6379"``) or ``(host, port)`` tuples. Connecting keeps the first seed that answers; one that
            cannot be reached is skipped, while a refused ``AUTH`` or ``SELECT`` raises at once. After a lost
            connection the next reconnect starts with the seed after the one that dropped. With
            ``cluster=True`` the seeds are also asked for the slot table when no known node answers.
        read_policy: Cluster clients only: where reads go. A :class:`ruvio.ReadPolicy` or its string value:
            ``"primary"`` (default), ``"prefer-replica"`` or ``"replica"``. See
            :class:`ruvio.asyncio.ClusterRuvio`. Ignored by a standalone server and without ``cluster=True``.
        follow_primary: ``True`` (default): a standalone client with at least two ``seeds`` (or a
            ``sentinel_master``) finds the primary again after a failover. A reconnect connects to the node whose
            ``ROLE`` is ``master`` (falling back to the seed order), and a ``READONLY`` reply makes the client look for
            the primary, move to it and send the refused command once more (not inside a transaction opened with
            ``multi()``, and never again after a second refusal). Pipelines are not resent. ``False`` keeps the plain
            seed order and returns ``READONLY`` as an error. With one seed and no ``sentinel_master`` nothing changes.
            A cluster client does not use it. While following is active a command method returns a coroutine
            instead of a future; await it as usual.
        sentinel_master: Name of the master to ask coordinators for. The ``seeds`` (or ``host``/``port``) are then
            coordinators: the client asks each with ``SENTINEL get-master-addr-by-name`` and confirms the answer with
            ``ROLE``; the first connect fails with :class:`ruvio.ProtocolError` when none names a primary.
            Needs ``follow_primary``.

    The connection opens on the first command, or on ``await client.connect()`` / ``async with``.

    Example:
        client = Ruvio("127.0.0.1", 6379)
        await client.set("k", "v", ex=60)
        values = await asyncio.gather(client.get("k"), client.get("other"))
        await client.aclose()
    """

    __slots__ = ("_st", "_decode")

    def __new__(cls, *args: Any, cluster: bool = False, **kwargs: Any) -> "Ruvio":
        # ``Ruvio(..., cluster=True)`` builds the cluster-routing subclass; the plain class is untouched.
        """Builds the cluster-routing subclass when ``cluster=True`` is passed, a plain client otherwise."""
        if cluster and cls is Ruvio:
            from .cluster import ClusterRuvio

            cls = ClusterRuvio  # type: ignore[assignment]
        return object.__new__(cls)

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        username: Optional[str] = None,
        client_name: Optional[str] = None,
        durability_replicas: Optional[int] = None,
        durability_timeout_ms: Optional[int] = None,
        connect_timeout: Optional[float] = 5.0,
        decode_responses: bool = True,
        unix_socket_path: Optional[str] = None,
        parser: str = "auto",
        on_push: Optional[Callable[[List[Any]], None]] = None,
        cluster: bool = False,
        seeds: Optional[Sequence[Union[str, Tuple[str, int]]]] = None,
        read_policy: Union[None, str, ReadPolicy] = None,
        follow_primary: bool = True,
        sentinel_master: Optional[str] = None,
    ) -> None:
        """Creates the client (nothing is connected yet). See the class docstring for every argument.

        Args:
            host: Server host.
            port: Server port.
            db: Database selected on connect.
            password: Password for ``AUTH``.
            username: ACL user for ``AUTH``.
            client_name: Connection name set on connect.
            durability_replicas: Replicas that must have the writes on disk before a reply (``CLIENT DURABILITY``).
            durability_timeout_ms: Milliseconds to wait for them; ``None`` keeps the server default.
            connect_timeout: Seconds to open the connection; ``None`` for no limit.
            decode_responses: Return ``str`` (``True``) or ``bytes`` (``False``).
            unix_socket_path: Unix socket to use instead of host/port.
            parser: ``"auto"``, ``"python"`` or ``"hiredis"``.
            on_push: Callback for RESP3 push messages.
            cluster: Route by key over several shards.
            seeds: ``"host:port"`` strings or ``(host, port)`` tuples tried in order in place of
                ``host``/``port``.
            read_policy: ``"primary"`` (default), ``"prefer-replica"`` or ``"replica"``; cluster clients only.
            follow_primary: Find the primary again after a failover (default ``True``).
            sentinel_master: Master name to ask coordinators for; see the class docstring.

        Raises:
            ValueError: A seed has an empty host or a port outside 1-65535, ``read_policy`` is unknown, or
                ``sentinel_master`` is empty or combined with ``unix_socket_path``.
        """
        seed_list = _parse_seeds(seeds)
        if seed_list:
            if unix_socket_path:
                raise ValueError("seeds cannot be combined with unix_socket_path")
            host, port = seed_list[0]
        policy_code(read_policy)  # rejects an unknown value now rather than at the first command
        if sentinel_master is not None:
            if not isinstance(sentinel_master, str) or not sentinel_master:
                raise ValueError("sentinel_master must be a non-empty string")
            if unix_socket_path:
                raise ValueError("sentinel_master cannot be combined with unix_socket_path")
            if not follow_primary:
                sentinel_master = None  # off restores the plain connect
        self._st = _State(
            _Options(
                host=host,
                port=port,
                seeds=seed_list,
                db=db,
                password=password,
                username=username,
                client_name=client_name,
                durability_replicas=durability_replicas,
                durability_timeout_ms=durability_timeout_ms,
                connect_timeout=connect_timeout,
                unix_socket_path=unix_socket_path,
                parser=parser,
                on_push=on_push,
                read_policy=read_policy,
                follow_primary=bool(follow_primary),
                sentinel_master=sentinel_master,
            )
        )
        self._decode = decode_responses

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "Ruvio":
        """Creates a client from a URL, see :func:`ruvio.parse_url` for the accepted forms.

        Args:
            url: For example ``"ruvio://:secret@localhost:6379/1"`` or ``"unix:///tmp/ruvio.sock"``.
            **kwargs: Any other constructor argument; these override values taken from the URL.

        Returns:
            The new client (connects on first use).

        Raises:
            ValueError: Unsupported URL scheme.
        """
        opts = parse_url(url)
        opts.update(kwargs)
        return cls(**opts)

    # ---------------------------------------------------------------- plumbing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        proto = self._st.proto
        if proto is None:
            return self._connect_then(conv, data, decode)
        fut = proto.loop.create_future()
        proto.pending.append((fut, conv, self._decode if decode is None else decode))
        proto.send(data)
        if self._st.follow:
            return self._guarded(fut, conv, data, decode, proto)
        return fut

    async def _guarded(self, fut: "asyncio.Future[Any]", conv: Conv, data: bytes, decode: Optional[bool], proto: _Proto) -> Any:
        """Awaits the reply; a ``READONLY`` error moves the connection to the primary and sends the command once more."""
        try:
            return await fut
        except ResponseError as exc:
            if exc.code != "READONLY":
                raise
            refused = exc
        st = self._st
        retry = not st.cur.multi
        if st.proto is proto:
            if not await self._move(proto):
                raise refused
        elif st.proto is None:
            try:
                await self.connect()
            except RuvioError:
                raise refused from None
        if not retry:
            raise refused
        now = st.proto
        if now is None:
            raise refused
        again = now.loop.create_future()
        now.pending.append((again, conv, self._decode if decode is None else decode))
        now.send(data)
        return await again

    async def _move(self, proto: _Proto) -> bool:
        """After a ``READONLY`` reply: rounds that avoid the address that answered, with the backoff between
        them, until a primary is found and the connection is moved to it. ``False`` when none is."""
        st = self._st
        if st.mover is None:
            st.mover = asyncio.Lock()
        async with st.mover:
            if st.proto is not proto:  # another command already moved it
                return True
            opts, cur = st.opts, st.cur
            refused = cur.address()
            sentinel = opts.sentinel_master is not None
            nodes = cur.coord if sentinel else cur.addrs
            for attempt in range(ROUNDS):
                if attempt:
                    await asyncio.sleep(WAITS[attempt - 1])
                found = await _find_primary(nodes, 0 if sentinel else cur.pos, refused, opts)  # type: ignore[arg-type]
                if found is None:
                    continue
                try:
                    new = await _handshake(await _take(opts, cur, found, opts.on_push), opts)
                except ConnectionError:
                    continue
                if st.proto is not proto:
                    new.transport.close()  # type: ignore[union-attr]
                    return True
                self._install(new)
                cur.multi = False
                if proto.transport is not None:
                    proto.transport.close()
                return True
            return False

    def _install(self, proto: _Proto) -> None:
        st = self._st

        def lost(p: _Proto, st: _State = st) -> None:
            if st.proto is p:
                st.proto = None

        proto.on_lost = lost
        st.proto = proto

    async def _connect_then(self, conv: Conv, data: bytes, decode: Optional[bool]) -> Any:
        await self.connect()
        return await self._x(conv, data, decode)

    async def connect(self) -> "Ruvio":
        """Opens the connection (and runs ``AUTH``/``SELECT``/``CLIENT SETNAME``/``CLIENT DURABILITY``) if it is not open yet.

        Safe to call from several tasks at once; one of them opens it.

        Returns:
            The client itself.

        Raises:
            ConnectionError: The server is unreachable or rejects the handshake.
            TimeoutError: ``connect_timeout`` elapsed.
        """
        st = self._st
        if st.proto is not None:
            return self
        if st.lock is None:
            st.lock = asyncio.Lock()
        async with st.lock:
            if st.proto is None:
                self._install(await _open(st.opts, st.opts.on_push, st.cur))
        return self

    def _batch(self, payload: List[bytes]) -> "Awaitable[List[Any]]":
        proto = self._st.proto
        if proto is None:
            return self._connect_batch(payload)
        fut = proto.loop.create_future()
        idle = not proto.pending and proto.wbuf is None
        # one deque slot per expected reply, all feeding the same collector
        entry = (_Batch(fut, len(payload)), None, self._decode)
        proto.pending.extend([entry] * len(payload))
        data = b"".join(payload)
        if idle:
            proto.transport.write(data)  # type: ignore[union-attr]
        else:
            proto.send(data)
        return fut

    async def _connect_batch(self, payload: List[bytes]) -> List[Any]:
        await self.connect()
        return await self._batch(payload)

    async def select(self, db: int) -> Any:  # type: ignore[override]
        """``SELECT db`` - switches the database of the whole shared connection; remembered for reconnects.

        Every coroutine using this client sees the change, so prefer passing ``db`` to the constructor.

        Args:
            db: Database number, ``0`` to ``databases - 1``.

        Returns:
            ``True`` (awaitable).

        Raises:
            ResponseError: ``db`` is out of range or the server has several shards.
        """
        ok = await super().select(db)
        self._st.opts.db = db
        return ok

    async def client_setname(self, name: str) -> Any:  # type: ignore[override]
        """``CLIENT SETNAME name`` - names the shared connection; remembered for reconnects.

        Args:
            name: The name (empty clears it).

        Returns:
            ``True`` (awaitable).
        """
        ok = await super().client_setname(name)
        self._st.opts.client_name = name
        return ok

    async def auth(self, password: str, username: Optional[str] = None) -> Any:  # type: ignore[override]
        """``AUTH [username] password`` - authenticates the shared connection; remembered for reconnects.

        Args:
            password: The password.
            username: ACL user; omit for the default user.

        Returns:
            ``True`` (awaitable).

        Raises:
            ResponseError: Wrong credentials.
        """
        ok = await super().auth(password, username)
        self._st.opts.password = password
        self._st.opts.username = username
        return ok

    async def multi(self) -> Any:  # type: ignore[override]
        """``MULTI`` - raw transaction start; remembered so a ``READONLY`` reply inside it is not resent.

        Returns:
            ``True`` (awaitable).
        """
        ok = await super().multi()
        self._st.cur.multi = True
        return ok

    async def exec(self) -> Any:  # type: ignore[override]
        """``EXEC`` - runs the queued commands of a raw ``MULTI`` and ends the transaction.

        Returns:
            A list with one reply per queued command, or ``None`` when a watched key changed (awaitable).
        """
        try:
            return await super().exec()
        finally:
            self._st.cur.multi = False

    async def discard(self) -> Any:  # type: ignore[override]
        """``DISCARD`` - drops the queued commands of a raw ``MULTI`` and ends the transaction.

        Returns:
            ``True`` (awaitable).
        """
        try:
            return await super().discard()
        finally:
            self._st.cur.multi = False

    def execute_command(self, *args: Any) -> Any:
        """Sends any command and returns the raw reply (awaitable); see :meth:`Commands.execute_command`.

        Args:
            *args: Command name followed by its arguments.

        Returns:
            The server reply as parsed.
        """
        if args and self._st.follow:
            name = args[0]
            name = (name.decode("ascii", "replace") if isinstance(name, bytes) else str(name)).upper()
            if name == "MULTI" or name == "EXEC" or name == "DISCARD":
                return self._execute_tracked(name, args)
        return super().execute_command(*args)

    async def _execute_tracked(self, name: str, args: Tuple[Any, ...]) -> Any:
        cur = self._st.cur
        try:
            reply = await super().execute_command(*args)
        except BaseException:
            if name != "MULTI":
                cur.multi = False
            raise
        cur.multi = name == "MULTI"
        return reply

    @property
    def raw(self) -> "Ruvio":
        """A view on the same connection whose replies are ``bytes`` (binary safe).

        The view shares the connection, so commands from both objects stay ordered.

        Example:
            blob = await client.raw.get("blob")
        """
        view = Ruvio.__new__(Ruvio)
        view._st = self._st
        view._decode = False
        return view

    async def aclose(self) -> None:
        """Closes the connection. Commands in flight fail with :class:`ruvio.ConnectionError`; the client can
        be used again (the next command reconnects). ``close`` is an alias.
        """
        proto = self._st.proto
        self._st.proto = None
        if proto is not None and proto.transport is not None:
            proto.transport.close()
            await asyncio.sleep(0)

    close = aclose

    async def __aenter__(self) -> "Ruvio":
        """Connects and returns the client, so ``async with Ruvio(...) as client:`` closes it on exit."""
        return await self.connect()

    async def __aexit__(self, *exc: Any) -> None:
        """Closes the connection."""
        await self.aclose()

    def dedicated(self) -> "Ruvio":
        """Creates a new client with its own connection and the same options.

        Use it for blocking commands and ``WATCH`` so they do not hold the shared connection.

        Returns:
            A new, not yet connected client. Close it with ``await worker.aclose()`` or ``async with``.

        Example:
            async with client.dedicated() as worker:
                item = await worker.blpop("jobs", timeout=30)
        """
        return Ruvio(decode_responses=self._decode, **{k: v for k, v in self._st.opts.as_dict().items() if k != "socket_timeout"})

    def pipeline(self, transaction: bool = False) -> "Pipeline":
        """Creates a pipeline: commands are queued locally and sent in one write.

        Command methods on the pipeline are not awaited; they return the pipeline. ``await pipe.execute()``
        sends the batch. Use ``transaction=True`` for ``MULTI``/``EXEC`` (keys must share a slot).

        Args:
            transaction: ``True`` to run the batch as ``MULTI``/``EXEC``.

        Returns:
            A :class:`Pipeline`.

        Example:
            results = await client.pipeline().set("a", 1).incr("a").get("a").execute()
            # -> [True, 2, "2"]
        """
        return Pipeline(self, transaction)

    async def pubsub(self, ignore_subscribe_messages: bool = False) -> "PubSub":
        """Creates and connects a Pub/Sub session on its own connection.

This is a coroutine: ``ps = await client.pubsub()``.

        Args:
            ignore_subscribe_messages: Hide the ``subscribe``/``unsubscribe`` confirmation messages.

        Returns:
            A connected :class:`PubSub`.

        Example:
            async with await client.pubsub() as ps:
                await ps.subscribe("news")
                msg = await ps.get_message(timeout=1.0)
        """
        ps = PubSub(self._st.opts, self._decode, ignore_subscribe_messages)
        ps._cur.pos = self._st.cur.resume()
        if self._st.opts.sentinel_master is None and self._st.cur.addrs is not None:
            ps._cur.addrs = list(self._st.cur.addrs)  # includes a primary a replica reported
        await ps.connect()
        return ps

    # ---------------------------------------------------------------- helpers
    def lock(
        self, name: KeyT, ttl_ms: int = 30000, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> "Lock":
        """Creates a distributed lock on key ``name``: ``async with client.lock("job", 10000) as lk:``.

        Uses ``SET NX PX`` to acquire, ``SET IFEQ PX`` to renew and ``DELEX IFEQ`` to release, with a random
        token, so releasing never deletes another holder's lock.

        Args:
            name: Lock key.
            ttl_ms: Lifetime in milliseconds if never released (default 30000).
            blocking: Wait for the lock when it is held (default ``True``).
            timeout: Seconds to wait when blocking; ``None`` waits forever.
            retry_ms: Milliseconds between attempts (default 50).

        Returns:
            A :class:`Lock` (not yet acquired).
        """
        return Lock(self, name, ttl_ms, blocking, timeout, retry_ms)

    def lease_scope(self, key: KeyT, ttl_ms: int) -> "_Scope":
        """Async context manager around ``LEASE``: takes the lease on enter and ``RELEASE`` on exit.

        Args:
            key: Lease key.
            ttl_ms: Lease duration in milliseconds.

        Returns:
            An async context manager whose ``as`` value is the fencing token (``int``).

        Raises:
            LockError: (on enter) the lease is held by someone else.

        Example:
            async with client.lease_scope("job:nightly", 30000) as token:
                await run_job(token)
        """
        return _Scope(self, key, lambda: self.lease(key, ttl_ms), "lease")

    def semaphore_scope(self, key: KeyT, max: int, ttl_ms: int) -> "_Scope":  # noqa: A002
        """Async context manager around ``SEMAPHORE``: takes a slot on enter and ``RELEASE`` on exit.

        Args:
            key: Semaphore key.
            max: Number of concurrent holders allowed.
            ttl_ms: Hold time of the slot in milliseconds.

        Returns:
            An async context manager whose ``as`` value is the holder token (``int``).

        Raises:
            LockError: (on enter) all slots are taken.
        """
        return _Scope(self, key, lambda: self.semaphore(key, max, ttl_ms), "semaphore")

    async def get_or_create(
        self,
        key: KeyT,
        factory: Callable[[], Union[str, bytes, Awaitable[Union[str, bytes]]]],
        ttl_ms: int,
        fill_ms: int = 10000,
        swr_ms: Optional[int] = None,
        timeout: float = 10.0,
        poll_ms: float = 5.0,
    ) -> Any:
        """Cache-aside with stampede protection on ``FILLGET``/``FILLSET``; ``factory`` may be sync or async.

        On a miss one caller runs ``factory``; the others poll plain ``FILLGET`` with a growing delay
        (``WAIT`` would park the shared connection) until the value appears. If the winner fails, its marker
        lapses after ``fill_ms`` and another caller takes over.

        Args:
            key: Cache key.
            factory: Zero-argument function or coroutine function returning the value (``str`` or ``bytes``).
            ttl_ms: Fresh lifetime of the cached value in milliseconds.
            fill_ms: How long the winner may take to build the value (default 10000).
            swr_ms: Optional stale-while-revalidate window in milliseconds.
            timeout: Seconds to wait for another caller's value (default 10.0).
            poll_ms: First polling delay in milliseconds (default 5; doubles up to 100).

        Returns:
            The cached or newly built value.

        Raises:
            TimeoutError: Another caller still holds the fill after ``timeout`` seconds.

        Example:
            page = await client.get_or_create("page:home", render_home, ttl_ms=60000)
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        delay = poll_ms / 1000.0
        while True:
            r = await self.fillget(key, fill_ms)
            state = r.state
            if state == "HIT":
                return r.value
            if state == "FILL" or (state == "STALE" and r.token is not None):
                value = factory()
                if inspect.isawaitable(value):
                    value = await value
                await self.fillset(key, value, ttl_ms, r.token, swr_ms)
                return value
            if state == "STALE":
                return r.value
            if loop.time() >= deadline:
                raise TimeoutError(f"get_or_create: {key!r} still being filled after {timeout}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.1)

    async def idempotent(
        self,
        key: KeyT,
        fingerprint: str,
        ttl_ms: int,
        fn: Callable[[], Union[str, bytes, Awaitable[Union[str, bytes]]]],
        owner: Optional[str] = None,
    ) -> Tuple[str, Any]:
        """Runs ``fn`` (sync or async) at most once per key, using ``IDEM BEGIN``/``COMPLETE``.

        See :meth:`ruvio.Ruvio.idempotent`. If ``fn`` raises, the record stays pending until ``ttl_ms``.

        Args:
            key: Operation key, scoped to the tenant and operation.
            fingerprint: Digest of the request inputs.
            ttl_ms: Retention in milliseconds, 1 to 604800000.
            fn: Zero-argument function or coroutine function returning the result (``str`` or ``bytes``).
            owner: Attempt token; random when omitted.

        Returns:
            ``("executed", result)``, ``("completed", stored_result)``, ``("pending", None)`` or ``("conflict", None)``.
        """
        owner = owner or uuid.uuid4().hex
        r = await self.idem_begin(key, fingerprint, owner, ttl_ms)
        if r.state == "acquired":
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            await self.idem_complete(key, fingerprint, owner, result)
            return "executed", result
        return r.state, r.result

    async def transaction(self, fn: Callable[["Pipeline"], Any], *watch: KeyT, retries: int = 16) -> List[Any]:
        """``WATCH`` + ``MULTI``/``EXEC`` with retries, on a private dedicated connection (``WATCH`` is per connection).

        ``fn`` gets a transaction pipeline bound to that private connection and may be sync or async. Read
        current values through this client (a different connection), then queue the writes on the pipeline;
        if a watched key changed before ``EXEC`` the function runs again.

        Args:
            fn: Callable (or coroutine function) taking the pipeline.
            *watch: Keys to ``WATCH`` (same slot on a cluster).
            retries: Maximum attempts (default 16).

        Returns:
            The results of the executed pipeline.

        Raises:
            RuvioError: Retries exhausted.
            ExecAbortError: A queued command was refused.

        Example:
            async def add_credit(pipe):
                balance = int(await client.get("balance") or 0)
                pipe.set("balance", balance + 10)

            await client.transaction(add_credit, "balance")
        """
        conn = self.dedicated()
        try:
            for _ in range(retries):
                if watch:
                    await conn.watch(*watch)
                pipe = Pipeline(conn, True)
                try:
                    r = fn(pipe)
                    if inspect.isawaitable(r):
                        await r
                    return await pipe.execute()
                except WatchError:
                    continue
            raise RuvioError("transaction retries exhausted")
        finally:
            await conn.aclose()

    async def scan_iter(
        self, match: Optional[str] = None, count: Optional[int] = None, type: Optional[KeyTypeT] = None  # noqa: A002
    ) -> AsyncIterator[Any]:
        """Async iterator over all keys, calling :meth:`scan` page by page.

        Args:
            match: Glob pattern for key names.
            count: Work hint per page.
            type: Only keys of this type, as ``TYPE`` reports it.

        Yields:
            Key names (a key may repeat if the table was rebuilt during the walk).

        Example:
            async for key in client.scan_iter(match="user:*"):
                ...
        """
        cursor = 0
        while True:
            cursor, keys = await self.scan(cursor, match, count, type)
            for k in keys:
                yield k
            if cursor == 0:
                return

    async def hscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Tuple[Any, Any]]:
        """Async iterator over the fields of a hash, calling :meth:`hscan` page by page.

        Args:
            key: The hash key.
            match: Glob pattern for field names.
            count: Work hint per page.

        Yields:
            ``(field, value)`` tuples.
        """
        cursor = 0
        while True:
            cursor, page = await self.hscan(key, cursor, match, count)
            for item in page.items():
                yield item
            if cursor == 0:
                return

    async def sscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Any]:
        """Async iterator over the members of a set, calling :meth:`sscan` page by page.

        Args:
            key: The set key.
            match: Glob pattern for members.
            count: Work hint per page.

        Yields:
            Members.
        """
        cursor = 0
        while True:
            cursor, page = await self.sscan(key, cursor, match, count)
            for m in page:
                yield m
            if cursor == 0:
                return

    async def zscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Tuple[Any, float]]:
        """Async iterator over a sorted set, calling :meth:`zscan` page by page.

        Args:
            key: The sorted-set key.
            match: Glob pattern for members.
            count: Work hint per page.

        Yields:
            ``(member, score)`` tuples.
        """
        cursor = 0
        while True:
            cursor, page = await self.zscan(key, cursor, match, count)
            for m in page:
                yield m
            if cursor == 0:
                return


class Pipeline(PipelineBase):
    """A batch of queued commands sent in one write; created by :meth:`Ruvio.pipeline`.

    Command methods queue the command and return the pipeline (do not ``await`` them); only
    :meth:`execute` is awaited. ``async with client.pipeline() as pipe:`` clears unexecuted commands on exit.

    Example:
        async with client.pipeline(transaction=True) as pipe:
            pipe.incr("a").incr("b")
            a, b = await pipe.execute()
    """

    __slots__ = ()

    async def execute(self, raise_on_error: bool = True) -> List[Any]:
        """Sends the queued commands and returns their results.

        Args:
            raise_on_error: ``True`` (default) raises the first error after all replies were read. ``False``
                returns exception objects in place of failed results.

        Returns:
            One converted result per command, in order ([] for an empty pipeline).

        Raises:
            WatchError: Transaction only - a watched key changed, nothing ran.
            ExecAbortError: Transaction only - a queued command was refused, nothing ran.
            ResponseError: A command failed (``raise_on_error=True``).
            CrossSlotError: Cluster client - keys span several slots (checked before sending).
        """
        if not self._cmds:
            return []
        payload = self._payload()
        try:
            raw = await self._client._batch(payload)
            return self._finish(raw, raise_on_error)
        finally:
            self.reset()

    async def __aenter__(self) -> "Pipeline":
        """Returns the pipeline for use in an ``async with`` block."""
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Clears queued commands that were not executed."""
        self.reset()


class Lock:
    """Async distributed lock with a random token; created by :meth:`Ruvio.lock`.

    Release never deletes another holder's lock. The lock expires after ``ttl_ms``: renew it with
    :meth:`renew` during long work.

    Example:
        async with client.lock("report", ttl_ms=10000, timeout=5):
            await build_report()
    """

    __slots__ = ("client", "name", "ttl_ms", "blocking", "timeout", "retry_ms", "token")

    def __init__(
        self, client: Ruvio, name: KeyT, ttl_ms: int, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> None:
        """Creates a lock object (does not acquire).

        Args:
            client: The asyncio client.
            name: Lock key.
            ttl_ms: Lifetime in milliseconds if never released.
            blocking: Default for :meth:`acquire`.
            timeout: Default maximum seconds to wait.
            retry_ms: Milliseconds between attempts.
        """
        self.client = client
        self.name = name
        self.ttl_ms = ttl_ms
        self.blocking = blocking
        self.timeout = timeout
        self.retry_ms = retry_ms
        self.token: Optional[str] = None

    async def acquire(self, blocking: Optional[bool] = None, timeout: Optional[float] = None) -> bool:
        """Tries to take the lock (``SET NX PX``), retrying every ``retry_ms`` while blocking.

        Args:
            blocking: Wait until the lock is free; defaults to the value given at creation.
            timeout: Maximum seconds to wait; defaults to the creation value (``None`` waits forever).

        Returns:
            ``True`` when acquired, ``False`` otherwise.
        """
        blocking = self.blocking if blocking is None else blocking
        timeout = self.timeout if timeout is None else timeout
        token = uuid.uuid4().hex
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if await self.client.set(self.name, token, px=self.ttl_ms, nx=True):
                self.token = token
                return True
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            await asyncio.sleep(self.retry_ms / 1000.0)

    async def renew(self, ttl_ms: Optional[int] = None) -> bool:
        """Extends the TTL, only while this token still holds the lock.

        Args:
            ttl_ms: New lifetime in milliseconds (default: the lock's ``ttl_ms``).

        Returns:
            ``True`` when extended, ``False`` when the lock is not held by this token.
        """
        if self.token is None:
            return False
        return bool(await self.client.set(self.name, self.token, ifeq=self.token, px=ttl_ms or self.ttl_ms))

    async def release(self) -> bool:
        """``DELEX name IFEQ token`` - releases the lock.

        Returns:
            ``True`` when released, ``False`` when it expired, belongs to someone else, or was never acquired.
        """
        token, self.token = self.token, None
        if token is None:
            return False
        return bool(await self.client.delex(self.name, token))

    async def owned(self) -> bool:
        """Tells whether the key still holds this lock's token (one ``GET``).

        Returns:
            ``True`` if this lock object still owns the key.
        """
        return self.token is not None and (await self.client.get(self.name)) == self.token

    async def __aenter__(self) -> "Lock":
        """Acquires the lock (blocking) for an ``async with`` block.

        Raises:
            LockError: The lock could not be acquired within ``timeout``.
        """
        if not await self.acquire():
            raise LockError(f"could not acquire lock {self.name!r}")
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Releases the lock."""
        await self.release()


class _Scope:
    __slots__ = ("client", "key", "acquire", "what", "token")

    def __init__(self, client: Ruvio, key: KeyT, acquire: Callable[[], Any], what: str) -> None:
        self.client = client
        self.key = key
        self.acquire = acquire
        self.what = what
        self.token: Optional[int] = None

    async def __aenter__(self) -> int:
        token = await self.acquire()
        if token is None:
            raise LockError(f"{self.what} {self.key!r} is not available")
        self.token = token
        return token

    async def __aexit__(self, *exc: Any) -> None:
        if self.token is not None:
            await self.client.release(self.key, self.token)
            self.token = None


class PubSub:
    """Pub/Sub session on its own connection; created by ``await client.pubsub()``.

    Received messages are queued; read them with :meth:`get_message` or ``async for msg in ps.listen()``.
    After a reconnect the subscriptions in :attr:`channels`, :attr:`patterns` and :attr:`shard_channels` are
    sent again. Messages are not stored. Patterns use ``*`` and ``?``.

    Attributes:
        channels: Channels subscribed with :meth:`subscribe` (a dict used as an ordered set).
        patterns: Patterns subscribed with :meth:`psubscribe`.
        shard_channels: Channels subscribed with :meth:`ssubscribe`.
        ignore_subscribe_messages: When ``True``, confirmation messages are skipped.

    Example:
        async with await client.pubsub() as ps:
            await ps.subscribe("news")
            async for msg in ps.listen():
                print(msg.channel, msg.data)
    """

    def __init__(self, opts: _Options, decode: bool, ignore_subscribe_messages: bool = False) -> None:
        """Creates a session. Use ``await client.pubsub()`` instead of calling this directly.

        Args:
            opts: Connection options of the client.
            decode: Return ``str`` (``True``) or ``bytes``.
            ignore_subscribe_messages: Skip subscribe/unsubscribe confirmations.
        """
        self._opts = opts
        self._cur = _Cursor(opts)
        self._decode = decode
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._queue: "asyncio.Queue[Any]" = asyncio.Queue()
        self._proto: Optional[_Proto] = None
        self.channels: Dict[Any, None] = {}
        self.patterns: Dict[Any, None] = {}
        self.shard_channels: Dict[Any, None] = {}

    async def connect(self) -> None:
        """Opens the connection (if not open) and sends the stored subscriptions again.

        Raises:
            ConnectionError: The server is unreachable.
        """
        if self._proto is not None and not self._proto.closed:
            return
        proto = await _open(self._opts, self._queue.put_nowait, self._cur)
        proto.parser.decode = self._decode
        proto.on_lost = lambda p: self._queue.put_nowait(p.lost_exc or ConnectionError("pubsub connection lost"))
        self._proto = proto
        for name, items in (("SUBSCRIBE", self.channels), ("PSUBSCRIBE", self.patterns), ("SSUBSCRIBE", self.shard_channels)):
            if items:
                proto.transport.write(encode(name, tuple(items)))  # type: ignore[union-attr]

    async def _send(self, name: str, args: Tuple[Any, ...]) -> None:
        if self._proto is None or self._proto.closed:
            await self.connect()
        self._proto.transport.write(encode(name, args))  # type: ignore[union-attr]

    async def subscribe(self, *channels: Any) -> None:
        """``SUBSCRIBE channel [channel ...]`` - listens to exact channels.

        Args:
            *channels: Channel names. Confirmations arrive as ``subscribe`` messages.
        """
        await self._send("SUBSCRIBE", channels)
        self.channels.update(dict.fromkeys(channels))

    async def unsubscribe(self, *channels: Any) -> None:
        """``UNSUBSCRIBE [channel ...]`` - stops listening to channels.

        Args:
            *channels: Channel names; none means every exact channel (patterns stay).
        """
        for c in channels or list(self.channels):
            self.channels.pop(c, None)
        await self._send("UNSUBSCRIBE", channels)

    async def psubscribe(self, *patterns: Any) -> None:
        """``PSUBSCRIBE pattern [pattern ...]`` - listens to channels matching glob patterns (``*``, ``?``).

        Args:
            *patterns: Patterns. Matches arrive as ``pmessage`` messages with ``pattern`` set.
        """
        await self._send("PSUBSCRIBE", patterns)
        self.patterns.update(dict.fromkeys(patterns))

    async def punsubscribe(self, *patterns: Any) -> None:
        """``PUNSUBSCRIBE [pattern ...]`` - stops pattern subscriptions.

        Args:
            *patterns: Patterns; none means every pattern.
        """
        for p in patterns or list(self.patterns):
            self.patterns.pop(p, None)
        await self._send("PUNSUBSCRIBE", patterns)

    async def ssubscribe(self, *channels: Any) -> None:
        """``SSUBSCRIBE channel [channel ...]`` - listens to sharded channels (sent with ``spublish``).

        All channels of one call must hash to the same slot. Messages arrive as ``smessage``.

        Args:
            *channels: Channel names.
        """
        await self._send("SSUBSCRIBE", channels)
        self.shard_channels.update(dict.fromkeys(channels))

    async def sunsubscribe(self, *channels: Any) -> None:
        """``SUNSUBSCRIBE [channel ...]`` - stops sharded subscriptions.

        Args:
            *channels: Channel names; none means all sharded channels.
        """
        for c in channels or list(self.shard_channels):
            self.shard_channels.pop(c, None)
        await self._send("SUNSUBSCRIBE", channels)

    async def ping(self, message: Optional[str] = None) -> None:
        """``PING [message]`` - sends a ping on the subscription connection.

        Args:
            message: Optional text echoed back. The answer arrives as a ``pong`` message.
        """
        await self._send("PING", () if message is None else (message,))

    async def get_message(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Waits for the next message.

        Args:
            timeout: Seconds to wait; ``None`` (default) waits forever, ``0`` only checks the queue.

        Returns:
            A :class:`Message` ``(kind, channel, data, pattern)``, or ``None`` on timeout.

        Raises:
            ConnectionError: The connection was lost.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(0.0, deadline - loop.time())
            try:
                v = await asyncio.wait_for(self._queue.get(), remaining)
            except asyncio.TimeoutError:
                return None
            if isinstance(v, BaseException):
                self._proto = None
                raise v
            m = _message(v)
            if self.ignore_subscribe_messages and m.kind in _SUB_KINDS:
                continue
            return m

    async def listen(self) -> AsyncIterator[Message]:
        """Async generator over messages; ends only when you stop iterating or the connection fails.

        Yields:
            :class:`Message` objects.

        Raises:
            ConnectionError: The connection was lost.
        """
        while True:
            m = await self.get_message()
            if m is not None:
                yield m

    async def aclose(self) -> None:
        """Closes the subscription connection. ``close`` is an alias."""
        proto, self._proto = self._proto, None
        if proto is not None and proto.transport is not None:
            proto.on_lost = None
            proto.transport.close()

    close = aclose

    async def __aenter__(self) -> "PubSub":
        """Connects and returns the session for an ``async with`` block."""
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Closes the session."""
        await self.aclose()

