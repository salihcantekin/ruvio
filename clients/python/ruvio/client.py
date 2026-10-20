# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Synchronous Ruvio client: one socket, a reusable ``recv_into`` buffer, no threads.

Main entry points:

* :class:`Ruvio` - the client; every server command is a method (see :class:`ruvio._commands.Commands`).
* :class:`Pipeline` - batches commands into one write, optionally as ``MULTI``/``EXEC``.
* :class:`PubSub` - a Pub/Sub session on its own socket.
* :class:`Lock` - a distributed lock built on ``SET NX PX`` / ``DELEX``.
* :func:`parse_url` - turns a ``ruvio://`` URL into constructor arguments.

Example:
    from ruvio import Ruvio

    client = Ruvio("127.0.0.1", 6379)
    client.set("greeting", "hello")
    print(client.get("greeting"))   # hello
    client.close()
"""

from __future__ import annotations

import socket
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union
from urllib.parse import unquote, urlparse

from ._blocking import FOREVER, blocking_wait, pipeline_wait
from ._cluster import ReadPolicy, policy_code
from ._commands import Commands, Conv, KeyT, KeyTypeT, _s
from ._follow import MASTER, ROUNDS, WAITS, Address, parse_master_addr, parse_role, probe_wait
from ._models import Message
from ._pipeline import PipelineBase
from ._resp import NEED, Push, encode, make_parser
from .errors import ConnectionError, LockError, ProtocolError, ResponseError, RuvioError, TimeoutError

__all__ = ["Ruvio", "Pipeline", "PubSub", "Lock", "parse_url"]


def parse_url(url: str) -> Dict[str, Any]:
    """Turns a connection URL into keyword arguments for :class:`Ruvio`.

    Accepted forms: ``ruvio://[user:pass@]host[:port][/db]`` (``redis://`` and ``tcp://`` are the
    same), and ``unix:///path/to/socket``. The port defaults to 6379, the database to 0. User and
    password are percent-decoded. TLS URLs (``rediss://``) are not supported.

    Args:
        url: The URL.

    Returns:
        A dict with some of ``host``, ``port``, ``db``, ``username``, ``password`` and
        ``unix_socket_path``.

    Raises:
        ValueError: Unsupported scheme, or a database part that is not an integer.

    Example:
        parse_url("ruvio://app:s3cret@cache.local:6380/2")
        # -> {"host": "cache.local", "port": 6380, "db": 2, "username": "app", "password": "s3cret"}
    """
    u = urlparse(url)
    opts: Dict[str, Any] = {}
    if u.scheme == "unix":
        opts["unix_socket_path"] = u.path
    else:
        if u.scheme not in ("ruvio", "redis", "tcp"):
            raise ValueError(f"unsupported URL scheme {u.scheme!r}")
        opts["host"] = u.hostname or "127.0.0.1"
        opts["port"] = u.port or 6379
        path = u.path.lstrip("/")
        if path:
            opts["db"] = int(path)
    if u.username:
        opts["username"] = unquote(u.username)
    if u.password:
        opts["password"] = unquote(u.password)
    return opts


def _parse_seeds(seeds: Optional[Sequence[Union[str, Tuple[str, int]]]]) -> Optional[List[Tuple[str, int]]]:
    """Normalizes a seed list into ``(host, port)`` pairs; ``None`` for no seeds.

    Each entry is ``"host:port"`` (IPv6 as ``"[::1]:6379"``) or a ``(host, port)`` tuple.

    Raises:
        ValueError: An entry has an empty host or a port outside 1-65535.
        TypeError: An entry is neither a string nor a ``(host, port)`` pair.
    """
    if seeds is None:
        return None
    if isinstance(seeds, str):
        seeds = [seeds]
    out: List[Tuple[str, int]] = []
    for entry in seeds:
        if isinstance(entry, str):
            host, sep, text = entry.rpartition(":")
            if not sep:
                raise ValueError(f"seed {entry!r} must be host:port")
            if host.startswith("[") and host.endswith("]"):
                host = host[1:-1]
            try:
                port = int(text)
            except ValueError:
                raise ValueError(f"seed {entry!r} has an invalid port") from None
        elif isinstance(entry, tuple) and len(entry) == 2:
            host, port = entry
            if isinstance(port, bool) or not isinstance(port, int) or not isinstance(host, str):
                raise TypeError(f"seed {entry!r} must be (host: str, port: int)")
        else:
            raise TypeError(f"seed {entry!r} must be a 'host:port' string or a (host, port) tuple")
        if not host:
            raise ValueError(f"seed {entry!r} has an empty host")
        if not 0 < port < 65536:
            raise ValueError(f"seed {entry!r} has a port outside 1-65535")
        out.append((host, port))
    return out or None


class _Options:
    __slots__ = (
        "host",
        "port",
        "seeds",
        "db",
        "username",
        "password",
        "client_name",
        "durability_replicas",
        "durability_timeout_ms",
        "socket_timeout",
        "connect_timeout",
        "unix_socket_path",
        "parser",
        "on_push",
        "read_policy",
        "follow_primary",
        "sentinel_master",
    )

    def __init__(self, **kw: Any) -> None:
        for name in _Options.__slots__:  # not self.__slots__: a subclass adds no fields
            setattr(self, name, kw.get(name))

    def handshake(self) -> List[Tuple[Conv, bytes]]:
        cmds: List[Tuple[Conv, bytes]] = []
        if self.password is not None:
            if self.username is not None:
                cmds.append((None, encode("AUTH", (self.username, self.password))))
            else:
                cmds.append((None, encode("AUTH", (self.password,))))
        if self.db:
            cmds.append((None, encode("SELECT", (self.db,))))
        if self.client_name:
            cmds.append((None, encode("CLIENT SETNAME", (self.client_name,))))
        if self.durability_replicas:
            if self.durability_timeout_ms is None:
                cmds.append((None, encode("CLIENT DURABILITY", ("REPLICAS", self.durability_replicas))))
            else:
                cmds.append(
                    (None, encode("CLIENT DURABILITY", ("REPLICAS", self.durability_replicas, "TIMEOUT", self.durability_timeout_ms)))
                )
        return cmds

    def as_dict(self) -> Dict[str, Any]:
        return {n: getattr(self, n) for n in _Options.__slots__}

    def stop_following(self) -> None:
        """A cluster node keeps its own address: it never looks for a primary."""
        self.follow_primary = False
        self.sentinel_master = None


class _ReplicaOptions(_Options):
    """Options of a replica connection of a cluster client: ``READONLY`` follows the handshake, on every open."""

    __slots__ = ()

    def handshake(self) -> List[Tuple[Conv, bytes]]:
        return [*super().handshake(), (None, encode("READONLY", ()))]


class _Conn:
    """The socket plus its parser. Shared by a client and its ``raw`` view.

    With seeds, ``pos`` is the seed an open starts at (the one after the seed last connected, so a
    reconnect ends with the address that dropped) and ``at`` the seed the socket is on (-1 before the
    first connect). A failed open leaves both alone.
    """

    __slots__ = ("sock", "parser", "opts", "pos", "at", "addrs", "coord", "multi", "once")

    def __init__(self, opts: _Options) -> None:
        self.opts = opts
        self.sock: Optional[socket.socket] = None
        self.parser: Any = None
        self.pos = 0
        self.at = -1
        #: The addresses a connect tries in order: the seeds, plus any primary a replica reported; with
        #: ``sentinel_master`` just the primary last found (empty before the first connect).
        self.addrs: Optional[List[Address]] = [] if opts.sentinel_master is not None else (list(opts.seeds) if opts.seeds else None)
        #: With ``sentinel_master``, the coordinators to ask.
        self.coord: Optional[List[Address]] = (list(opts.seeds) if opts.seeds else [(opts.host, opts.port)]) if opts.sentinel_master is not None else None
        #: True between an acknowledged ``MULTI`` and ``EXEC``/``DISCARD`` sent through the client.
        self.multi = False
        #: True while the command a ``READONLY`` reply refused is sent its second time.
        self.once = False

    def resume(self) -> int:
        """The seed index a new connection that should join this one starts at."""
        return self.at if self.at >= 0 else self.pos

    def _dial(self, host: Optional[str] = None, port: Optional[int] = None) -> socket.socket:
        o = self.opts
        if host is None:
            host, port = o.host, o.port
        try:
            if o.unix_socket_path:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(o.connect_timeout)
                sock.connect(o.unix_socket_path)
            else:
                sock = socket.create_connection((host, port), timeout=o.connect_timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(o.socket_timeout)
        except socket.timeout as exc:
            raise TimeoutError(f"connect to {host}:{port} timed out") from exc
        except OSError as exc:
            raise ConnectionError(f"cannot connect to {host}:{port}: {exc}") from exc
        return sock

    def _dial_seeds(self, seeds: List[Address]) -> socket.socket:
        """One connect attempt per seed, from ``pos`` round to the seed before it; the last connect
        error when none answers. ``opts.host``/``opts.port`` follow the seed that connected."""
        o = self.opts
        n = len(seeds)
        error: Optional[ConnectionError] = None
        for k in range(n):
            i = (self.pos + k) % n
            o.host, o.port = seeds[i]
            try:
                sock = self._dial()
            except ConnectionError as exc:
                error = exc
                continue
            self.at = i
            self.pos = (i + 1) % n
            return sock
        raise error  # type: ignore[misc]

    def follows(self) -> bool:
        """Following is active (see :meth:`_following`)."""
        return self._following()

    def move_to_primary(self, sleep: Callable[[float], None]) -> bool:
        """After a ``READONLY`` reply: rounds that avoid the address that answered, ``sleep`` between them, until
        a primary is found and the connection is moved to it. ``False`` when none is, the connection unchanged."""
        o = self.opts
        # kept across rounds: cleared once the refusing node says it is a replica, so its promotion is taken
        refused: List[Optional[Address]] = [self.address()]
        sentinel = o.sentinel_master is not None
        nodes = self.coord if sentinel else self.addrs
        for attempt in range(ROUNDS):
            if attempt:
                sleep(WAITS[attempt - 1])
            found = find_primary(nodes, 0 if sentinel else self.pos, refused, o)  # type: ignore[arg-type]
            if found is not None:
                try:
                    self.move(found)
                    return True
                except ConnectionError:
                    pass
        return False

    def _following(self) -> bool:
        """Following is active: the option is on and there are at least two seeds or a coordinator name."""
        o = self.opts
        if not o.follow_primary:
            return False
        if o.sentinel_master is not None:
            return True
        return bool(o.seeds) and len(self.addrs or ()) > 1  # type: ignore[arg-type]

    def _connect(self) -> socket.socket:
        """A socket for a new connection. The first connect keeps the seed order (a coordinator name asks the
        coordinators instead); a reconnect first asks which node is the primary and falls back to the seed order."""
        o = self.opts
        if self._following():
            sentinel = o.sentinel_master is not None
            if sentinel or self.at >= 0:
                found = find_primary(self.coord if sentinel else self.addrs, self.pos, [None], o)  # type: ignore[arg-type]
                if found is not None:
                    try:
                        return self.take(found)
                    except ConnectionError:
                        if sentinel and self.at < 0:
                            raise
                elif sentinel and self.at < 0:
                    raise ProtocolError(f"no coordinator names a primary for {o.sentinel_master!r}")
        if o.sentinel_master is not None:
            if not self.addrs:
                raise ConnectionError("no primary to connect to")
            sock = self._dial(*self.addrs[0])  # the last primary; the coordinators are no data nodes
            self.at = 0
            return sock
        if o.seeds:
            return self._dial_seeds(self.addrs)  # type: ignore[arg-type]
        return self._dial()

    def take(self, address: Address) -> socket.socket:
        """Connects to ``address`` (a node that answered ``master``) and makes it the seed the connection is on."""
        o = self.opts
        sock = self._dial(address[0], address[1])
        if o.sentinel_master is not None:
            self.addrs = [address]
            self.at = 0
            self.pos = 0
            return sock
        addrs = self.addrs
        if address not in addrs:  # type: ignore[operator]
            addrs.append(address)  # type: ignore[union-attr]
        i = addrs.index(address)  # type: ignore[union-attr]
        o.host, o.port = address
        self.at = i
        self.pos = (i + 1) % len(addrs)  # type: ignore[arg-type]
        return sock

    def address(self) -> Optional[Address]:
        """The address the socket is on, or ``None`` before the first connect."""
        if self.addrs is not None and self.at >= 0:
            return self.addrs[self.at]
        return None

    def open(self) -> None:
        self.multi = False
        self._start(self._connect())

    def move(self, address: Address) -> None:
        """Replaces the socket by a new connection to ``address`` (a node that answered ``master``), handshake
        included. The old socket is closed once the new one connects; ``ConnectionError`` leaves it alone."""
        sock = self.take(address)
        self.close()
        self.multi = False
        self._start(sock)

    def _start(self, sock: socket.socket) -> None:
        o = self.opts
        self.sock = sock
        self.parser = make_parser(o.parser or "auto")
        hs = o.handshake()
        if hs:
            try:
                sock.sendall(b"".join(d for _, d in hs))
                for _ in hs:
                    r = self.read()
                    if isinstance(r, ResponseError):
                        raise r
            except BaseException:
                self.close()
                raise

    def close(self) -> None:
        sock = self.sock
        self.sock = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def read(self) -> Any:
        p = self.parser
        while True:
            v = p.gets()
            if v is not NEED:
                if type(v) is Push:
                    cb = self.opts.on_push
                    if cb is not None:
                        cb(v)
                    continue
                return v
            n = self.sock.recv_into(p.get_buffer())  # type: ignore[union-attr]
            if not n:
                raise ConnectionError("connection closed by server")
            p.end += n


def _probe(address: Address, o: _Options, command: str, args: Tuple[Any, ...]) -> Any:
    """One short-lived socket for one question. The reply, or ``None`` when the address cannot be
    reached, times out, drops or answers an error. A refused ``AUTH`` raises, as connecting does."""
    po = _Options(**o.as_dict())
    po.host, po.port = address
    po.seeds = None
    po.db = 0
    po.client_name = None
    po.durability_replicas = None
    po.durability_timeout_ms = None
    po.on_push = None
    po.stop_following()
    po.socket_timeout = probe_wait(o.connect_timeout, o.socket_timeout)
    c = _Conn(po)
    try:
        try:
            c.open()
        except ResponseError:
            raise
        except (RuvioError, OSError, ValueError):
            return None
        try:
            c.sock.sendall(encode(command, args))  # type: ignore[union-attr]
            reply = c.read()
        except (RuvioError, OSError, ValueError):
            return None
        return None if isinstance(reply, ResponseError) else reply
    finally:
        c.close()


def find_primary(nodes: List[Address], start: int, avoid: List[Optional[Address]], o: _Options) -> Optional[Address]:
    """One round: the address of a node that answered ``master``, asking ``nodes`` in order from ``start``
    (a replica's reported primary is asked once, after them), or ``None``. With ``sentinel_master`` the nodes
    are coordinators, each asked for the master's address, which ``ROLE`` must confirm. ``avoid[0]`` is the
    address that just answered ``READONLY``; it is not taken while it still says ``master`` (a replaced primary
    does) and is cleared once it answers ``ROLE`` as a replica, so a later round takes it when it is promoted.
    Raises only when ``AUTH`` is refused."""
    n = len(nodes)
    queue = [nodes[(start + k) % n] for k in range(n)]
    name = o.sentinel_master
    if name is not None:
        for coordinator in queue:
            named = parse_master_addr(_probe(coordinator, o, "SENTINEL", ("get-master-addr-by-name", name)))
            if named is not None:
                role = parse_role(_probe(named, o, "ROLE", ()))
                if role is None:
                    continue
                if role[0] != MASTER:
                    if named == avoid[0]:
                        avoid[0] = None
                elif named != avoid[0]:
                    return named
        return None
    seen = set(queue)
    i = 0
    while i < len(queue):
        address = queue[i]
        i += 1
        role = parse_role(_probe(address, o, "ROLE", ()))
        if role is None:
            continue
        if role[0] == MASTER:
            if address != avoid[0]:
                return address
            continue
        if address == avoid[0]:
            avoid[0] = None
        if role[1] is not None and role[1] not in seen:
            seen.add(role[1])
            queue.append(role[1])
    return None


class Ruvio(Commands):
    """Synchronous Ruvio client over one TCP (or Unix) socket.

    Every server command is a method (see :class:`ruvio._commands.Commands` for the full list, with
    arguments, replies and examples). Replies are converted to Python values; error replies raise
    :class:`ruvio.RuvioError` subclasses. A dropped socket is reopened before the next command; the
    command that found it dropped raises and is never sent twice.

    The client is **not thread-safe**: use one client per thread, or guard it with a lock.

    Args:
        host: Server host (default ``127.0.0.1``). On a multi-shard server this is the seed
            address; with ``cluster=True`` the client finds the other shards itself.
        port: Server port (default 6379).
        db: Logical database selected on connect (default 0). Multi-shard servers have only database 0.
        password: Password for ``AUTH``, sent on every (re)connect.
        username: ACL user for ``AUTH``; omit for the default user.
        client_name: Name sent with ``CLIENT SETNAME`` on connect.
        durability_replicas: When set (above 0), ``CLIENT DURABILITY REPLICAS n`` is sent on every connect: the server
            holds each reply until ``n`` replicas have the writes on disk. Needs a server with replication.
        durability_timeout_ms: Milliseconds the server waits for the replicas (its default is 5000); used only with
            ``durability_replicas``.
        socket_timeout: Seconds a send or receive may take (default 30.0); ``None`` waits forever. On
            expiry the socket is closed and :class:`ruvio.TimeoutError` is raised. A blocking command gets
            its own wait on top (``blpop(k, 5)`` may take 5 s plus this), and one that waits forever
            (timeout ``0``) has no limit. Pub/Sub sockets have none either.
        connect_timeout: Seconds allowed to open the socket (default 5.0, ``None`` for no limit).
        decode_responses: ``True`` (default) returns ``str``; ``False`` returns ``bytes``. ``client.raw``
            gives the opposite view of the same connection.
        unix_socket_path: Connect to this Unix socket instead of ``host``/``port``.
        parser: ``"auto"`` (default: hiredis if installed, else the built-in parser), ``"python"`` or
            ``"hiredis"``.
        on_push: Callback for RESP3 out-of-band push messages (for example client-tracking
            ``invalidate``), called with the push as a list.
        lazy: ``True`` (default) connects on the first command; ``False`` connects now and raises
            :class:`ruvio.ConnectionError` if the server is unreachable.
        cluster: ``True`` returns a :class:`ruvio.ClusterRuvio`, which routes each key to its shard.
            A standalone server is detected on the first command and used as usual.
        seeds: Addresses to try in order instead of ``host``/``port``: ``"host:port"`` strings (IPv6 as
            ``"[::1]:6379"``) or ``(host, port)`` tuples. Connecting keeps the first seed that answers; one that
            cannot be reached is skipped, while a refused ``AUTH`` or ``SELECT`` raises at once. After a
            dropped connection the next reconnect starts with the seed after the one that dropped. With
            ``cluster=True`` the seeds are also asked for the slot table when no known node answers.
        read_policy: Cluster clients only: where reads go. A :class:`ruvio.ReadPolicy` or its string value:
            ``"primary"`` (default), ``"prefer-replica"`` or ``"replica"``. See :class:`ruvio.ClusterRuvio`.
            Ignored by a standalone server and by a client without ``cluster=True``.
        follow_primary: ``True`` (default): a standalone client with at least two ``seeds`` (or a
            ``sentinel_master``) finds the primary again after a failover. A reconnect connects to the node whose
            ``ROLE`` is ``master`` (falling back to the seed order), and a ``READONLY`` reply makes the client look for
            the primary, move to it and send the refused command once more (not inside a transaction opened with
            ``multi()``, and never again after a second refusal). Pipelines are not resent. ``False`` keeps the plain
            seed order and returns ``READONLY`` as an error. With one seed and no ``sentinel_master`` nothing changes.
            A cluster client does not use it.
        sentinel_master: Name of the master to ask coordinators for. The ``seeds`` (or ``host``/``port``) are then
            coordinators: the client asks each with ``SENTINEL get-master-addr-by-name`` and confirms the answer with
            ``ROLE``; the first connect fails with :class:`ruvio.ProtocolError` when none names a primary.
            Needs ``follow_primary``.

    Example:
        from ruvio import Ruvio

        with Ruvio("127.0.0.1", 6379, decode_responses=True) as client:
            client.set("k", "v", ex=60)
            client.get("k")   # -> "v"
    """

    __slots__ = ("_conn", "_decode")

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
        socket_timeout: Optional[float] = 30.0,
        connect_timeout: Optional[float] = 5.0,
        decode_responses: bool = True,
        unix_socket_path: Optional[str] = None,
        parser: str = "auto",
        on_push: Optional[Callable[[List[Any]], None]] = None,
        lazy: bool = True,
        cluster: bool = False,
        seeds: Optional[Sequence[Union[str, Tuple[str, int]]]] = None,
        read_policy: Union[None, str, ReadPolicy] = None,
        follow_primary: bool = True,
        sentinel_master: Optional[str] = None,
    ) -> None:
        """Creates the client. See the class docstring for the meaning of every argument.

        Args:
            host: Server host.
            port: Server port.
            db: Database selected on connect.
            password: Password for ``AUTH``.
            username: ACL user for ``AUTH``.
            client_name: Connection name set on connect.
            durability_replicas: Replicas that must have the writes on disk before a reply (``CLIENT DURABILITY``).
            durability_timeout_ms: Milliseconds to wait for them; ``None`` keeps the server default.
            socket_timeout: Seconds for a send/receive (default 30.0, plus a blocking command's own wait);
                ``None`` waits forever.
            connect_timeout: Seconds to open the socket; ``None`` for no limit.
            decode_responses: Return ``str`` (``True``) or ``bytes`` (``False``).
            unix_socket_path: Unix socket to use instead of host/port.
            parser: ``"auto"``, ``"python"`` or ``"hiredis"``.
            on_push: Callback for RESP3 push messages.
            lazy: Connect on first use (``True``) or immediately (``False``).
            cluster: Route by key over several shards (see :class:`ruvio.ClusterRuvio`).
            seeds: ``"host:port"`` strings or ``(host, port)`` tuples tried in order in place of
                ``host``/``port``.
            read_policy: ``"primary"`` (default), ``"prefer-replica"`` or ``"replica"``; cluster clients only.
            follow_primary: Find the primary again after a failover (default ``True``).
            sentinel_master: Master name to ask coordinators for; see the class docstring.

        Raises:
            ValueError: A seed has an empty host or a port outside 1-65535, ``read_policy`` is unknown, or
                ``sentinel_master`` is empty or combined with ``unix_socket_path``.
            ConnectionError: ``lazy=False`` and the server cannot be reached or rejects the handshake.
            ProtocolError: ``lazy=False``, ``sentinel_master`` is set and no coordinator names a primary.
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
        opts = _Options(
            host=host,
            port=port,
            seeds=seed_list,
            db=db,
            password=password,
            username=username,
            client_name=client_name,
            durability_replicas=durability_replicas,
            durability_timeout_ms=durability_timeout_ms,
            socket_timeout=socket_timeout,
            connect_timeout=connect_timeout,
            unix_socket_path=unix_socket_path,
            parser=parser,
            on_push=on_push,
            read_policy=read_policy,
            follow_primary=bool(follow_primary),
            sentinel_master=sentinel_master,
        )
        self._conn = _Conn(opts)
        self._decode = decode_responses
        if not lazy:
            self._conn.open()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "Ruvio":
        """Creates a client from a URL, see :func:`parse_url` for the accepted forms.

        Args:
            url: For example ``"ruvio://:secret@localhost:6379/1"`` or ``"unix:///tmp/ruvio.sock"``.
            **kwargs: Any other constructor argument; these override values taken from the URL.

        Returns:
            The new client (not yet connected unless ``lazy=False`` is passed).

        Raises:
            ValueError: Unsupported URL scheme.

        Example:
            client = Ruvio.from_url("ruvio://localhost:6379/0", decode_responses=False)
        """
        opts = parse_url(url)
        opts.update(kwargs)
        return cls(**opts)

    # ---------------------------------------------------------------- plumbing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        conn = self._conn
        sock = conn.sock
        if sock is None:
            conn.open()
            sock = conn.sock
        p = conn.parser
        dec = self._decode if decode is None else decode
        if p.decode is not dec:
            p.decode = dec
        t = conn.opts.socket_timeout
        wait = blocking_wait(data) if t is not None else 0.0
        try:
            if wait:
                sock.settimeout(None if wait == FOREVER else t + wait)  # type: ignore[union-attr]
            sock.sendall(data)  # type: ignore[union-attr]
            reply = conn.read()
            if wait:
                sock.settimeout(t)  # type: ignore[union-attr]
        except socket.timeout as exc:
            conn.close()
            raise TimeoutError("timed out waiting for the reply") from exc
        except OSError as exc:
            conn.close()
            if isinstance(exc, ConnectionError):
                raise
            raise ConnectionError(str(exc)) from exc
        except BaseException:
            conn.close()
            raise
        if isinstance(reply, ResponseError):
            if reply.code == "READONLY" and conn.follows() and not conn.once:
                return self._after_readonly(conv, data, decode, reply)
            raise reply
        if conv is None:
            return reply
        return conv(reply)

    def _after_readonly(self, conv: Conv, data: bytes, decode: Optional[bool], refused: ResponseError) -> Any:
        """A write met a replica: looks for the primary (up to ``ROUNDS`` rounds with the backoff between), moves the
        connection to it and sends the refused command once more. Inside a transaction the connection moves but the
        command is not sent again; when no primary is found the original error is raised."""
        conn = self._conn
        retry = not conn.multi
        if not conn.move_to_primary(sleep=time.sleep):
            raise refused
        if not retry:
            raise refused
        conn.once = True
        try:
            return Ruvio._x(self, conv, data, decode)
        finally:
            conn.once = False

    def _batch(self, payload: List[bytes], count: int) -> List[Any]:
        conn = self._conn
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not self._decode:
            p.decode = self._decode
        t = conn.opts.socket_timeout
        wait = pipeline_wait(payload) if t is not None else 0.0
        try:
            sock = conn.sock
            if wait:
                sock.settimeout(None if wait == FOREVER else t + wait)  # type: ignore[union-attr]
            sock.sendall(b"".join(payload))  # type: ignore[union-attr]
            replies = [conn.read() for _ in range(count)]
            if wait:
                sock.settimeout(t)  # type: ignore[union-attr]
            return replies
        except socket.timeout as exc:
            conn.close()
            raise TimeoutError("timed out waiting for pipeline replies") from exc
        except OSError as exc:
            conn.close()
            if isinstance(exc, ConnectionError):
                raise
            raise ConnectionError(str(exc)) from exc
        except BaseException:
            conn.close()
            raise

    def select(self, db: int) -> Any:
        """``SELECT db`` - switches the logical database of this client.

        The number is remembered, so a reconnect selects it again. See :meth:`Commands.select`.

        Args:
            db: Database number, ``0`` to ``databases - 1``.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``db`` is out of range, or the server has several shards (only database 0).
        """
        ok = super().select(db)
        self._conn.opts.db = db
        return ok

    def client_setname(self, name: str) -> Any:
        """``CLIENT SETNAME name`` - names this connection; remembered for reconnects.

        Args:
            name: The name (empty clears it).

        Returns:
            ``True``.
        """
        ok = super().client_setname(name)
        self._conn.opts.client_name = name
        return ok

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        """``AUTH [username] password`` - authenticates; the credentials are remembered for reconnects.

        Args:
            password: The password.
            username: ACL user; omit for the default user.

        Returns:
            ``True``.

        Raises:
            ResponseError: Wrong credentials.
        """
        ok = super().auth(password, username)
        self._conn.opts.password = password
        self._conn.opts.username = username
        return ok

    def multi(self) -> Any:
        """``MULTI`` - raw transaction start; remembered so a ``READONLY`` reply inside it is not resent.

        Returns:
            ``True``.
        """
        ok = super().multi()
        self._conn.multi = True
        return ok

    def exec(self) -> Any:
        """``EXEC`` - runs the queued commands of a raw ``MULTI`` and ends the transaction.

        Returns:
            A list with one reply per queued command, or ``None`` when a watched key changed.
        """
        try:
            return super().exec()
        finally:
            self._conn.multi = False

    def discard(self) -> Any:
        """``DISCARD`` - drops the queued commands of a raw ``MULTI`` and ends the transaction.

        Returns:
            ``True``.
        """
        try:
            return super().discard()
        finally:
            self._conn.multi = False

    def execute_command(self, *args: Any) -> Any:
        """Sends any command and returns the raw reply; see :meth:`Commands.execute_command`.

        Args:
            *args: Command name followed by its arguments.

        Returns:
            The server reply as parsed.
        """
        conn = self._conn
        if args and conn.follows():
            name = args[0]
            name = (name.decode("ascii", "replace") if isinstance(name, bytes) else str(name)).upper()
            if name == "MULTI":
                reply = super().execute_command(*args)
                conn.multi = True
                return reply
            if name == "EXEC" or name == "DISCARD":
                try:
                    return super().execute_command(*args)
                finally:
                    conn.multi = False
        return super().execute_command(*args)

    @property
    def raw(self) -> "Ruvio":
        r"""A view on the same connection whose replies are ``bytes`` (binary safe).

        Use it for values that are not valid UTF-8. The view shares the socket, so commands from both
        objects are ordered on one connection.

        Example:
            client.raw.get("blob")   # -> b"\x00\xff..."
        """
        view = Ruvio.__new__(Ruvio)
        view._conn = self._conn
        view._decode = False
        return view

    def connect(self) -> "Ruvio":
        """Opens the socket now if it is not open (commands also do this lazily).

        Returns:
            The client itself.

        Raises:
            ConnectionError: The server is unreachable or rejects ``AUTH``/``SELECT``.
            TimeoutError: ``connect_timeout`` elapsed.
        """
        if self._conn.sock is None:
            self._conn.open()
        return self

    def close(self) -> None:
        """Closes the socket. The client can be used again; the next command reconnects."""
        self._conn.close()

    def __enter__(self) -> "Ruvio":
        """Returns the client, so ``with Ruvio(...) as client:`` closes the socket on exit."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Closes the socket."""
        self.close()

    def dedicated(self) -> "Ruvio":
        """Creates a new client with its own socket and the same options.

        Use it for ``WATCH`` and for blocking commands (``blpop``, ``xread`` with ``block`` ...) so they do not hold up
        other work. The options include the database, name and credentials last set on this client.

        Returns:
            A new, not yet connected client (a cluster client for a cluster client). Close it when done.

        Example:
            with client.dedicated() as worker:
                item = worker.blpop("jobs", timeout=30)
        """
        opts = self._conn.opts.as_dict()
        return Ruvio(decode_responses=self._decode, **opts)

    def pipeline(self, transaction: bool = False) -> "Pipeline":
        """Creates a pipeline: commands are queued locally and sent in one write.

        Every command method on the pipeline returns the pipeline, so calls chain. ``execute()`` sends
        the batch and returns one converted result per command. Use ``transaction=True`` to wrap the batch
        in ``MULTI``/``EXEC`` (atomic; keys must share a slot, and a ``WATCH`` made earlier on this client
        applies).

        Args:
            transaction: ``True`` to run the batch as ``MULTI``/``EXEC``.

        Returns:
            A :class:`Pipeline`.

        Example:
            results = client.pipeline().set("a", 1).incr("a").get("a").execute()
            # -> [True, 2, "2"]
        """
        return Pipeline(self, transaction)

    def transaction(self, fn: Callable[["Pipeline"], Any], *watch: KeyT, retries: int = 16) -> List[Any]:
        """Optimistic transaction: ``WATCH`` keys, build commands, ``EXEC``, retry if a watched key changed.

        ``fn`` receives a transaction pipeline. Read current values through this client (``self``,
        the same connection that holds the ``WATCH``), then queue the writes on the pipeline. If a watched
        key changed before ``EXEC`` the whole function runs again.

        Args:
            fn: Callable taking the pipeline; queue commands on it. Its return value is ignored.
            *watch: Keys to ``WATCH`` (all in one slot on a cluster).
            retries: Maximum attempts (default 16).

        Returns:
            The results of the executed pipeline, one per queued command.

        Raises:
            RuvioError: Retries exhausted.
            ExecAbortError: A queued command was refused.

        Example:
            def add_credit(pipe):
                balance = int(client.get("balance") or 0)
                pipe.set("balance", balance + 10)

            client.transaction(add_credit, "balance")
        """
        from .errors import WatchError

        for _ in range(retries):
            if watch:
                self.watch(*watch)
            pipe = Pipeline(self, True)
            try:
                fn(pipe)
                return pipe.execute()
            except WatchError:
                continue
            finally:
                if watch:
                    try:
                        self.unwatch()
                    except RuvioError:
                        pass
        raise RuvioError("transaction retries exhausted")

    def pubsub(self, ignore_subscribe_messages: bool = False) -> "PubSub":
        """Creates a Pub/Sub session on its own socket (the client's socket is not used).

        Args:
            ignore_subscribe_messages: Hide the ``subscribe``/``unsubscribe`` confirmation messages.

        Returns:
            A :class:`PubSub`.

        Example:
            with client.pubsub() as ps:
                ps.subscribe("news")
                msg = ps.get_message(timeout=1.0)
        """
        ps = PubSub(self._conn.opts, self._decode, ignore_subscribe_messages)
        ps._conn.pos = self._conn.resume()
        if self._conn.opts.sentinel_master is None and self._conn.addrs is not None:
            ps._conn.addrs = list(self._conn.addrs)  # includes a primary a replica reported
        return ps

    # ---------------------------------------------------------------- helpers
    def lock(
        self, name: KeyT, ttl_ms: int = 30000, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> "Lock":
        """Creates a distributed lock on key ``name``.

Acquire is ``SET NX PX``, renew is ``SET IFEQ PX`` and release is ``DELEX IFEQ``.

        The lock holds a random token, so releasing never deletes another holder's lock. Use it as a
        context manager (``with client.lock("job"):``) or call :meth:`Lock.acquire` and :meth:`Lock.release`.

        Args:
            name: Lock key.
            ttl_ms: Lifetime in milliseconds if never released (default 30000). Renew with :meth:`Lock.renew`.
            blocking: Wait for the lock when it is held (default ``True``).
            timeout: Seconds to wait when blocking; ``None`` waits forever.
            retry_ms: Milliseconds between attempts (default 50).

        Returns:
            A :class:`Lock` (not yet acquired).

        Example:
            with client.lock("report", ttl_ms=10000, timeout=5):
                build_report()
        """
        return Lock(self, name, ttl_ms, blocking, timeout, retry_ms)

    def lease_scope(self, key: KeyT, ttl_ms: int) -> "_Scope":
        """Context manager around ``LEASE``: takes the lease on enter and ``RELEASE`` on exit.

        Args:
            key: Lease key.
            ttl_ms: Lease duration in milliseconds.

        Returns:
            A context manager whose ``as`` value is the fencing token (``int``).

        Raises:
            LockError: (on enter) the lease is held by someone else.

        Example:
            with client.lease_scope("job:nightly", 30000) as token:
                run_job(token)
        """
        return _Scope(self, key, lambda: self.lease(key, ttl_ms), "lease")

    def semaphore_scope(self, key: KeyT, max: int, ttl_ms: int) -> "_Scope":  # noqa: A002
        """Context manager around ``SEMAPHORE``: takes a slot on enter and ``RELEASE`` on exit.

        Args:
            key: Semaphore key.
            max: Number of concurrent holders allowed.
            ttl_ms: Hold time of the slot in milliseconds.

        Returns:
            A context manager whose ``as`` value is the holder token (``int``).

        Raises:
            LockError: (on enter) all slots are taken.

        Example:
            with client.semaphore_scope("uploads", 3, 60000):
                upload()
        """
        return _Scope(self, key, lambda: self.semaphore(key, max, ttl_ms), "semaphore")

    def get_or_create(
        self,
        key: KeyT,
        factory: Callable[[], Union[str, bytes]],
        ttl_ms: int,
        fill_ms: int = 10000,
        swr_ms: Optional[int] = None,
        timeout: float = 10.0,
        poll_ms: float = 5.0,
    ) -> Any:
        """Cache-aside with stampede protection, built on ``FILLGET``/``FILLSET``.

        On a miss exactly one caller (across all processes) runs ``factory``; the others poll plain
        ``FILLGET`` with a short growing delay (``FILLGET ... WAIT`` would park the connection) until the
        value appears. If the winner crashes, its marker lapses after ``fill_ms`` and another caller takes over.
        If ``factory`` raises, the exception propagates and others wait until the marker lapses.

        Args:
            key: Cache key.
            factory: Zero-argument callable returning the value (``str`` or ``bytes``).
            ttl_ms: Fresh lifetime of the cached value in milliseconds.
            fill_ms: How long the winner may take to build the value (default 10000).
            swr_ms: Optional stale-while-revalidate window in milliseconds; a stale value is returned at once
                while one caller refreshes it.
            timeout: Seconds to wait for another caller's value (default 10.0).
            poll_ms: First polling delay in milliseconds (default 5; doubles up to 100).

        Returns:
            The cached or newly built value.

        Raises:
            TimeoutError: Another caller still holds the fill after ``timeout`` seconds.

        Example:
            page = client.get_or_create("page:home", lambda: render_home(), ttl_ms=60000)
        """
        deadline = time.monotonic() + timeout
        delay = poll_ms / 1000.0
        while True:
            r = self.fillget(key, fill_ms)
            state = r.state
            if state == "HIT":
                return r.value
            if state == "FILL" or (state == "STALE" and r.token is not None):
                value = factory()
                self.fillset(key, value, ttl_ms, r.token, swr_ms)
                return value
            if state == "STALE":
                return r.value
            if time.monotonic() >= deadline:
                raise TimeoutError(f"get_or_create: {key!r} still being filled after {timeout}s")
            time.sleep(delay)
            delay = min(delay * 2, 0.1)

    def idempotent(
        self, key: KeyT, fingerprint: str, ttl_ms: int, fn: Callable[[], Union[str, bytes]], owner: Optional[str] = None
    ) -> Tuple[str, Any]:
        """Runs ``fn`` at most once per key, using ``IDEM BEGIN``/``COMPLETE``.

        If ``fn`` raises, the record stays pending until ``ttl_ms`` and retries report ``"pending"``; use a
        retention that matches your retry horizon. This is not exactly-once for external side effects.

        Args:
            key: Operation key, scoped to the tenant and operation (for example ``"order:42"``).
            fingerprint: Digest of the request inputs. The same key with another fingerprint is a conflict.
            ttl_ms: Retention in milliseconds, 1 to 604800000 (7 days).
            fn: Zero-argument callable returning the result (``str`` or ``bytes``) to store.
            owner: Attempt token; a random one is generated when omitted.

        Returns:
            ``("executed", result)`` when ``fn`` ran now, ``("completed", stored_result)`` when an earlier run
            finished, ``("pending", None)`` while another owner runs it, or ``("conflict", None)`` for a
            different fingerprint.

        Example:
            state, result = client.idempotent("pay:77", digest, 3600000, lambda: charge_card())
        """
        owner = owner or uuid.uuid4().hex
        r = self.idem_begin(key, fingerprint, owner, ttl_ms)
        if r.state == "acquired":
            result = fn()
            self.idem_complete(key, fingerprint, owner, result)
            return "executed", result
        return r.state, r.result

    def scan_iter(self, match: Optional[str] = None, count: Optional[int] = None, type: Optional[KeyTypeT] = None) -> Iterator[Any]:  # noqa: A002
        """Iterates over all keys by calling :meth:`scan` page by page.

        Args:
            match: Glob pattern for key names.
            count: Work hint per page.
            type: Only keys of this type: a :class:`KeyType` or its string as ``TYPE`` reports it.

        Yields:
            Key names. A key may be yielded twice if the table was rebuilt during the walk.

        Example:
            for key in client.scan_iter(match="user:*", count=500):
                ...
        """
        cursor = 0
        while True:
            cursor, keys = self.scan(cursor, match, count, type)
            yield from keys
            if cursor == 0:
                return

    def hscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Tuple[Any, Any]]:
        """Iterates over the fields of a hash by calling :meth:`hscan` page by page.

        Args:
            key: The hash key.
            match: Glob pattern for field names.
            count: Work hint per page.

        Yields:
            ``(field, value)`` tuples.
        """
        cursor = 0
        while True:
            cursor, page = self.hscan(key, cursor, match, count)
            yield from page.items()
            if cursor == 0:
                return

    def sscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Any]:
        """Iterates over the members of a set by calling :meth:`sscan` page by page.

        Args:
            key: The set key.
            match: Glob pattern for members.
            count: Work hint per page.

        Yields:
            Members.
        """
        cursor = 0
        while True:
            cursor, page = self.sscan(key, cursor, match, count)
            yield from page
            if cursor == 0:
                return

    def zscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Tuple[Any, float]]:
        """Iterates over a sorted set by calling :meth:`zscan` page by page.

        Args:
            key: The sorted-set key.
            match: Glob pattern for members.
            count: Work hint per page.

        Yields:
            ``(member, score)`` tuples.
        """
        cursor = 0
        while True:
            cursor, page = self.zscan(key, cursor, match, count)
            yield from page
            if cursor == 0:
                return


class Pipeline(PipelineBase):
    """A batch of queued commands sent in one write; created by :meth:`Ruvio.pipeline`.

    Every command method (``set``, ``get`` ...) queues the command and returns the pipeline. Nothing
    is sent until :meth:`execute`. After ``execute`` (or on leaving a ``with`` block) the queue is cleared.
    ``len(pipe)`` is the number of queued commands.

    Example:
        with client.pipeline(transaction=True) as pipe:
            pipe.incr("a").incr("b")
            a, b = pipe.execute()
    """

    __slots__ = ()

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        """Sends the queued commands and returns their results.

        Args:
            raise_on_error: ``True`` (default) raises the first error after all replies were read. ``False``
                returns exception objects in place of failed results.

        Returns:
            One converted result per command, in order ([] for an empty pipeline). In a transaction, the
            results of the commands between ``MULTI`` and ``EXEC``.

        Raises:
            WatchError: Transaction only - a watched key changed, nothing ran.
            ExecAbortError: Transaction only - a queued command was refused, nothing ran.
            ResponseError: A command failed (``raise_on_error=True``).
            CrossSlotError: Cluster client - a command's keys, or a transaction's keys, span several slots
                (checked before sending).
        """
        if not self._cmds:
            return []
        payload = self._payload()
        try:
            raw = self._client._batch(payload, len(payload))
            return self._finish(raw, raise_on_error)
        finally:
            self.reset()

    def __enter__(self) -> "Pipeline":
        """Returns the pipeline for use in a ``with`` block."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Clears queued commands that were not executed."""
        self.reset()


class Lock:
    """Distributed lock with a random token; created by :meth:`Ruvio.lock`.

    Acquire with ``SET key token NX PX ttl``; renew with ``SET ... IFEQ``; release with ``DELEX ... IFEQ``,
    so a holder never deletes another holder's lock. If the TTL runs out while the work is still running,
    the lock is lost: renew it periodically, and treat :meth:`owned` as a hint, not a guarantee.

    Example:
        lock = client.lock("report", ttl_ms=10000)
        if lock.acquire(blocking=False):
            try:
                build_report()
            finally:
                lock.release()
    """

    __slots__ = ("client", "name", "ttl_ms", "blocking", "timeout", "retry_ms", "token")

    def __init__(
        self, client: Ruvio, name: KeyT, ttl_ms: int, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> None:
        """Creates a lock object (does not acquire).

        Args:
            client: The client to use.
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

    def acquire(self, blocking: Optional[bool] = None, timeout: Optional[float] = None) -> bool:
        """Tries to take the lock.

        Args:
            blocking: Wait until the lock is free; defaults to the value given at creation.
            timeout: Maximum seconds to wait; defaults to the value given at creation (``None`` waits forever).

        Returns:
            ``True`` when acquired, ``False`` when not (non-blocking or timed out).
        """
        blocking = self.blocking if blocking is None else blocking
        timeout = self.timeout if timeout is None else timeout
        token = uuid.uuid4().hex
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self.client.set(self.name, token, px=self.ttl_ms, nx=True):
                self.token = token
                return True
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            time.sleep(self.retry_ms / 1000.0)

    def renew(self, ttl_ms: Optional[int] = None) -> bool:
        """Extends the TTL, only while this token still holds the lock.

        Args:
            ttl_ms: New lifetime in milliseconds (default: the lock's ``ttl_ms``).

        Returns:
            ``True`` when extended, ``False`` when the lock is not held by this token.
        """
        if self.token is None:
            return False
        return bool(self.client.set(self.name, self.token, ifeq=self.token, px=ttl_ms or self.ttl_ms))

    def release(self) -> bool:
        """``DELEX name IFEQ token`` - releases the lock.

        Returns:
            ``True`` when released, ``False`` when the lock expired, belongs to another holder, or was never acquired.
        """
        token, self.token = self.token, None
        if token is None:
            return False
        return bool(self.client.delex(self.name, token))

    def owned(self) -> bool:
        """Tells whether the key still holds this lock's token (one ``GET``).

        Returns:
            ``True`` if this lock object still owns the key.
        """
        return self.token is not None and self.client.get(self.name) == self.token

    def __enter__(self) -> "Lock":
        """Acquires the lock (blocking) for a ``with`` block.

        Raises:
            LockError: The lock could not be acquired within ``timeout``.
        """
        if not self.acquire():
            raise LockError(f"could not acquire lock {self.name!r}")
        return self

    def __exit__(self, *exc: Any) -> None:
        """Releases the lock."""
        self.release()


class _Scope:
    __slots__ = ("client", "key", "acquire", "what", "token")

    def __init__(self, client: Ruvio, key: KeyT, acquire: Callable[[], Any], what: str) -> None:
        self.client = client
        self.key = key
        self.acquire = acquire
        self.what = what
        self.token: Optional[int] = None

    def __enter__(self) -> int:
        token = self.acquire()
        if token is None:
            raise LockError(f"{self.what} {self.key!r} is not available")
        self.token = token
        return token

    def __exit__(self, *exc: Any) -> None:
        if self.token is not None:
            self.client.release(self.key, self.token)
            self.token = None


def _message(v: Any) -> Message:
    if type(v) is not list and type(v) is not Push:
        return Message("reply", None, v)
    kind = _s(v[0])
    if kind == "pmessage":
        return Message(kind, v[2], v[3], v[1])
    if kind == "pong":
        return Message(kind, None, v[1] if len(v) > 1 else None)
    return Message(kind, v[1], v[2] if len(v) > 2 else None)


_SUB_KINDS = frozenset(("subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe"))


class PubSub:
    """Pub/Sub session on a dedicated socket; created by :meth:`Ruvio.pubsub`.

    After a reconnect the subscriptions in :attr:`channels`, :attr:`patterns` and :attr:`shard_channels` are
    sent again. Messages are not stored: a subscriber that is away misses them. A subscriber that cannot keep
    up is disconnected by the server. Patterns use ``*`` and ``?``.

    Attributes:
        channels: Channels subscribed with :meth:`subscribe` (a dict used as an ordered set).
        patterns: Patterns subscribed with :meth:`psubscribe`.
        shard_channels: Channels subscribed with :meth:`ssubscribe`.
        ignore_subscribe_messages: When ``True``, confirmation messages are skipped.

    Example:
        with client.pubsub() as ps:
            ps.subscribe("news")
            for msg in ps.listen():
                print(msg.channel, msg.data)
    """

    def __init__(self, opts: _Options, decode: bool, ignore_subscribe_messages: bool = False) -> None:
        """Creates a session. Use :meth:`Ruvio.pubsub` instead of calling this directly.

        Args:
            opts: Connection options of the client.
            decode: Return ``str`` (``True``) or ``bytes``.
            ignore_subscribe_messages: Skip subscribe/unsubscribe confirmations.
        """
        o = _Options(**opts.as_dict())
        o.socket_timeout = None
        self._conn = _Conn(o)
        self._decode = decode
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.channels: Dict[Any, None] = {}
        self.patterns: Dict[Any, None] = {}
        self.shard_channels: Dict[Any, None] = {}

    def _ensure(self) -> None:
        c = self._conn
        if c.sock is None:
            c.open()
            c.parser.decode = self._decode
            for name, items in (("SUBSCRIBE", self.channels), ("PSUBSCRIBE", self.patterns), ("SSUBSCRIBE", self.shard_channels)):
                if items:
                    c.sock.sendall(encode(name, tuple(items)))  # type: ignore[union-attr]

    def _send(self, name: str, args: Tuple[Any, ...]) -> None:
        self._ensure()
        try:
            self._conn.sock.sendall(encode(name, args))  # type: ignore[union-attr]
        except OSError as exc:
            self._conn.close()
            raise ConnectionError(str(exc)) from exc

    def subscribe(self, *channels: Any) -> None:
        """``SUBSCRIBE channel [channel ...]`` - listens to exact channels.

        Args:
            *channels: Channel names. Confirmations arrive as ``subscribe`` messages.
        """
        self._send("SUBSCRIBE", channels)
        self.channels.update(dict.fromkeys(channels))

    def unsubscribe(self, *channels: Any) -> None:
        """``UNSUBSCRIBE [channel ...]`` - stops listening to channels.

        Args:
            *channels: Channel names; none means every exact channel (patterns stay).
        """
        for c in channels or list(self.channels):
            self.channels.pop(c, None)
        self._send("UNSUBSCRIBE", channels)

    def psubscribe(self, *patterns: Any) -> None:
        """``PSUBSCRIBE pattern [pattern ...]`` - listens to channels matching glob patterns (``*``, ``?``).

        Args:
            *patterns: Patterns. Matches arrive as ``pmessage`` messages with ``pattern`` set.
        """
        self._send("PSUBSCRIBE", patterns)
        self.patterns.update(dict.fromkeys(patterns))

    def punsubscribe(self, *patterns: Any) -> None:
        """``PUNSUBSCRIBE [pattern ...]`` - stops pattern subscriptions.

        Args:
            *patterns: Patterns; none means every pattern (exact channels stay).
        """
        for p in patterns or list(self.patterns):
            self.patterns.pop(p, None)
        self._send("PUNSUBSCRIBE", patterns)

    def ssubscribe(self, *channels: Any) -> None:
        """``SSUBSCRIBE channel [channel ...]`` - listens to sharded channels (sent with ``spublish``).

        All channels of one call must hash to the same slot. Messages arrive as ``smessage``. Keyspace
        notifications (``__keyspace@<db>__:<key>``) are also received this way.

        Args:
            *channels: Channel names.
        """
        self._send("SSUBSCRIBE", channels)
        self.shard_channels.update(dict.fromkeys(channels))

    def sunsubscribe(self, *channels: Any) -> None:
        """``SUNSUBSCRIBE [channel ...]`` - stops sharded subscriptions.

        Args:
            *channels: Channel names; none means all sharded channels.
        """
        for c in channels or list(self.shard_channels):
            self.shard_channels.pop(c, None)
        self._send("SUNSUBSCRIBE", channels)

    def ping(self, message: Optional[str] = None) -> None:
        """``PING [message]`` - sends a ping on the subscription socket.

        Args:
            message: Optional text echoed back. The answer arrives as a ``pong`` message.
        """
        self._send("PING", () if message is None else (message,))

    def get_message(self, timeout: Optional[float] = 0.0) -> Optional[Message]:
        """Returns the next message, or ``None`` if none arrives in time.

        Args:
            timeout: Seconds to wait. ``0.0`` (default) only checks what has already arrived, ``None`` waits forever.

        Returns:
            A :class:`Message` ``(kind, channel, data, pattern)`` or ``None``. ``kind`` is ``message``,
            ``pmessage``, ``smessage``, ``pong`` or a subscription confirmation.

        Raises:
            ConnectionError: The connection closed.
        """
        self._ensure()
        c = self._conn
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            p = c.parser
            v = p.gets()
            if v is NEED:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                c.sock.settimeout(remaining)  # type: ignore[union-attr]
                try:
                    n = c.sock.recv_into(p.get_buffer())  # type: ignore[union-attr]
                except (socket.timeout, BlockingIOError):
                    return None
                except OSError as exc:
                    c.close()
                    raise ConnectionError(str(exc)) from exc
                if not n:
                    c.close()
                    raise ConnectionError("connection closed by server")
                p.end += n
                continue
            if isinstance(v, ResponseError):
                raise v
            m = _message(v)
            if self.ignore_subscribe_messages and m.kind in _SUB_KINDS:
                continue
            return m

    def listen(self) -> Iterator[Message]:
        """Blocking generator over messages; never ends unless the connection fails or you stop iterating.

        Yields:
            :class:`Message` objects.

        Raises:
            ConnectionError: The connection closed.
        """
        while True:
            m = self.get_message(timeout=None)
            if m is not None:
                yield m

    def close(self) -> None:
        """Closes the subscription socket."""
        self._conn.close()

    def __enter__(self) -> "PubSub":
        """Returns the session for a ``with`` block."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Closes the session."""
        self.close()
