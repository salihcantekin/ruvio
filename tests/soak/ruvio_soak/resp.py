"""A RESP2 connection with timeouts, and a slot-routing cluster client.

The cluster client tells apart a command that certainly did not run (refused connection, MOVED,
ASK, TRYAGAIN, READONLY, LOADING, CLUSTERDOWN) from one whose outcome is unknown (the connection
broke after the command was sent, a timeout, NOREPLICAS). The history checker relies on that
difference, so every new error reply must be placed in one of the two lists below.
"""

from __future__ import annotations

import socket
import time

SLOTS = 16384

# Error replies after which the command certainly did not run; the client retries them.
RETRY_ERRORS = ("MOVED", "ASK", "TRYAGAIN", "READONLY", "LOADING", "CLUSTERDOWN", "MASTERDOWN")
# Error replies after which the command may have run. NOREPLICAS is here because with
# CLIENT DURABILITY the write stays applied when no replica confirms it in time.
UNKNOWN_ERRORS = ("NOREPLICAS",)


class ReplyError(Exception):
    """An error reply (`-...`)."""

    @property
    def code(self) -> str:
        return str(self).split(" ", 1)[0]


class Unknown(Exception):
    """The command was sent and its outcome is unknown."""


class Refused(Exception):
    """The command certainly did not run."""


def encode(*arguments) -> bytes:
    parts = [f"*{len(arguments)}\r\n".encode()]
    for argument in arguments:
        data = argument if isinstance(argument, bytes) else str(argument).encode()
        parts.extend((f"${len(data)}\r\n".encode(), data, b"\r\n"))
    return b"".join(parts)


def _read(stream):
    prefix = stream.read(1)
    line = stream.readline()
    if not prefix or not line.endswith(b"\r\n"):
        raise ConnectionError("connection closed")
    payload = line[:-2]
    if prefix == b"+":
        return payload.decode()
    if prefix == b"-":
        return ReplyError(payload.decode(errors="replace"))
    if prefix == b":":
        return int(payload)
    if prefix == b"$":
        length = int(payload)
        if length < 0:
            return None
        data = stream.read(length + 2)
        if len(data) != length + 2:
            raise ConnectionError("connection closed")
        return data[:-2]
    if prefix == b"*":
        count = int(payload)
        if count < 0:
            return None
        return [_read(stream) for _ in range(count)]
    raise ConnectionError(f"unexpected reply prefix {prefix!r}")


class Connection:
    """One connection. `call` raises `ReplyError` for an error reply, `OSError` when the
    connection cannot be opened, and `Unknown` when it breaks after the command was sent."""

    def __init__(self, host: str, port: int, password: str | None = None, timeout: float = 3.0):
        self.address = f"{host}:{port}"
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.stream = self.socket.makefile("rwb")
        if password:
            self.call("AUTH", password)

    def call(self, *arguments):
        try:
            self.stream.write(encode(*arguments))
            self.stream.flush()
            reply = _read(self.stream)
        except (OSError, ConnectionError, ValueError) as error:
            self.close()
            raise Unknown(f"{self.address}: {error}") from error
        if isinstance(reply, ReplyError):
            raise reply
        return reply

    def close(self) -> None:
        try:
            self.socket.close()
        except OSError:
            pass


def call_once(address: str, *arguments, password: str | None = None, timeout: float = 2.0):
    """Opens a connection, sends one command, closes. Raises like `Connection.call`."""
    host, port = address.rsplit(":", 1)
    connection = Connection(host, int(port), password, timeout)
    try:
        return connection.call(*arguments)
    finally:
        connection.close()


def parse_info(text) -> dict[str, str]:
    if isinstance(text, bytes):
        text = text.decode(errors="replace")
    fields = {}
    for line in (text or "").splitlines():
        if ":" in line and not line.startswith("#"):
            key, value = line.split(":", 1)
            fields[key.strip()] = value.strip()
    return fields


def _crc16(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
            crc &= 0xFFFF
    return crc


def key_slot(key) -> int:
    data = key if isinstance(key, bytes) else str(key).encode()
    start = data.find(b"{")
    if start >= 0:
        end = data.find(b"}", start + 1)
        if end > start + 1:
            data = data[start + 1 : end]
    return _crc16(data) % SLOTS


class ClusterClient:
    """Routes each command by its key's slot, follows MOVED and ASK, and retries the errors in
    `RETRY_ERRORS` with doubling waits until `deadline` seconds have passed.

    `setup` is a list of commands sent on every new connection after AUTH (for example
    `("CLIENT", "DURABILITY", "REPLICAS", "1")`)."""

    def __init__(self, seeds, password=None, setup=(), timeout=3.0, deadline=15.0):
        self.seeds = list(seeds)
        self.password = password
        self.setup = list(setup)
        self.timeout = timeout
        self.deadline = deadline
        self.connections: dict[str, Connection] = {}
        self.slots: list[str | None] = [None] * SLOTS
        self.retries = 0

    def _connection(self, address: str) -> Connection:
        connection = self.connections.get(address)
        if connection is None:
            host, port = address.rsplit(":", 1)
            connection = Connection(host, int(port), self.password, self.timeout)
            try:
                for command in self.setup:
                    connection.call(*command)
            except (ReplyError, Unknown) as error:
                connection.close()
                raise OSError(f"{address}: setup failed: {error}") from error
            self.connections[address] = connection
        return connection

    def _drop(self, address: str) -> None:
        connection = self.connections.pop(address, None)
        if connection is not None:
            connection.close()

    def refresh(self) -> bool:
        """Reloads the slot map from the first seed or known node that answers."""
        candidates = list(dict.fromkeys(self.seeds + [a for a in self.slots if a]))
        for address in candidates:
            try:
                ranges = self._connection(address).call("CLUSTER", "SLOTS")
            except (OSError, ReplyError, Unknown):
                self._drop(address)
                continue
            slots: list[str | None] = [None] * SLOTS
            for entry in ranges or []:
                first, last, primary = entry[0], entry[1], entry[2]
                host = primary[0].decode() if isinstance(primary[0], bytes) else primary[0]
                owner = f"{host}:{primary[1]}"
                for slot in range(first, last + 1):
                    slots[slot] = owner
            self.slots = slots
            return True
        return False

    def execute(self, key, *arguments):
        """Runs one command that touches `key`. Returns its reply; raises `Refused` when it
        certainly did not run before the deadline, `Unknown` when its outcome is unknown, or
        `ReplyError` for any other error reply (the command ran and failed)."""
        slot = key_slot(key)
        end = time.monotonic() + self.deadline
        wait = 0.005
        asking: str | None = None
        last = "no node for the slot"
        while True:
            address = asking or self.slots[slot]
            if address is None:
                if not self.refresh():
                    last = "no node answered CLUSTER SLOTS"
                address = self.slots[slot]
            if address is not None:
                try:
                    connection = self._connection(address)
                except OSError as error:
                    last = str(error)
                    self._drop(address)
                    self.slots = [None] * SLOTS
                else:
                    try:
                        if asking:
                            connection.call("ASKING")
                        return connection.call(*arguments)
                    except Unknown:
                        self._drop(address)
                        self.slots = [None] * SLOTS
                        raise
                    except ReplyError as error:
                        code = error.code
                        asking = None
                        if code == "MOVED":
                            self.slots[slot] = str(error).split()[2]
                            self.retries += 1
                            continue
                        if code == "ASK":
                            asking = str(error).split()[2]
                            self.retries += 1
                            continue
                        if code in UNKNOWN_ERRORS:
                            raise Unknown(str(error)) from error
                        if code not in RETRY_ERRORS:
                            raise
                        last = str(error)
                        if code in ("READONLY", "CLUSTERDOWN", "MASTERDOWN"):
                            self.slots = [None] * SLOTS
            if time.monotonic() >= end:
                raise Refused(last)
            self.retries += 1
            time.sleep(wait)
            wait = min(wait * 2, 0.5)

    def close(self) -> None:
        for address in list(self.connections):
            self._drop(address)
