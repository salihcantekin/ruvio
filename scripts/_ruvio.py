"""Helpers shared by the scripts in this folder: RESP framing, a free port, waiting for a server.

New scripts import from here instead of writing their own; older ones move over when they are
next changed. Run a script from anywhere: Python puts the script's own folder on `sys.path`.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time


def encode(*arguments: bytes) -> bytes:
    """One RESP2 command frame."""
    frame = [f"*{len(arguments)}\r\n".encode()]
    for argument in arguments:
        frame.extend((f"${len(argument)}\r\n".encode(), argument, b"\r\n"))
    return b"".join(frame)


def read_response(stream):
    """Reads one reply from a buffered binary stream as `(prefix, value)`; arrays nest."""
    prefix = stream.read(1)
    line = stream.readline()
    if not prefix or not line.endswith(b"\r\n"):
        raise AssertionError("truncated RESP response")
    payload = line[:-2]
    if prefix in (b"+", b"-"):
        return prefix, payload
    if prefix == b":":
        return prefix, int(payload)
    if prefix == b"$":
        length = int(payload)
        if length == -1:
            return prefix, None
        value = stream.read(length)
        assert stream.read(2) == b"\r\n"
        return prefix, value
    if prefix == b"*":
        count = int(payload)
        if count < 0:
            return prefix, None
        return prefix, [read_response(stream) for _ in range(count)]
    raise AssertionError(f"unsupported response prefix: {prefix!r}")


def request(stream, *arguments: bytes):
    """Sends one command and reads its reply."""
    stream.write(encode(*arguments))
    stream.flush()
    return read_response(stream)


def free_port() -> int:
    """A port that was free a moment ago. Another process can take it before the caller binds
    it, so a server started on it should be checked (see `wait_for_port`)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_for_port(host: str, port: int, timeout: float = 20.0, process=None) -> None:
    """Waits until `host:port` accepts a connection. With `process` (a `subprocess.Popen`), fails
    as soon as that process exits."""
    deadline = time.monotonic() + timeout
    while True:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"process exited with {process.returncode} before {host}:{port} opened")
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{host}:{port} did not open within {timeout} s")
            time.sleep(0.02)


def _alive(process, pid) -> bool:
    if process is not None and process.poll() is not None:
        return False
    if pid is not None:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
    return True


def wait_ready(
    port: int,
    host: str = "127.0.0.1",
    timeout: float = 20.0,
    process=None,
    pid: int | None = None,
    data_dir: str | None = None,
    cluster: bool = False,
) -> None:
    """Waits until `host:port` answers `PING` with `+PONG`.

    A connect that the peer closes at once (a container port proxy before the server listens),
    a truncated reply, a refused connection and an error reply such as `-LOADING` all mean "not
    yet", until `timeout` seconds pass. With `process` (a `subprocess.Popen`) or `pid`, fails as
    soon as that process exits. With `data_dir`, the server must also report that directory in
    `CONFIG GET dir`, so a stranger already on the port is never taken for ours. With `cluster`,
    `CLUSTER INFO` must also report `cluster_state:ok`.
    """
    deadline = time.monotonic() + timeout
    last = "no answer"
    while True:
        if not _alive(process, pid):
            raise RuntimeError(f"the server for {host}:{port} exited before it answered PING")
        try:
            with socket.create_connection((host, port), timeout=2) as client:
                stream = client.makefile("rwb")
                reply = request(stream, b"PING")
                if reply != (b"+", b"PONG"):
                    last = f"PING answered {reply!r}"
                elif data_dir is not None and not _serves(stream, data_dir):
                    last = f"another server answers on {host}:{port}"
                elif cluster and b"cluster_state:ok" not in (request(stream, b"CLUSTER", b"INFO")[1] or b""):
                    last = "cluster_state is not ok yet"
                else:
                    return
        except (OSError, AssertionError, ValueError) as error:
            last = f"{type(error).__name__}: {error}"
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{host}:{port} did not answer PING with PONG within {timeout} s ({last})")
        time.sleep(0.05)


def wait_replicas(port: int, replicas: int, host: str = "127.0.0.1", timeout: float = 20.0) -> None:
    """Waits until every range in `CLUSTER SLOTS` on `host:port` lists `replicas` replicas."""
    deadline = time.monotonic() + timeout
    last = "no answer"
    while True:
        try:
            with socket.create_connection((host, port), timeout=2) as client:
                prefix, ranges = request(client.makefile("rwb"), b"CLUSTER", b"SLOTS")
                if prefix != b"*" or not ranges:
                    last = f"CLUSTER SLOTS answered {prefix!r}"
                elif all(len(entry[1]) >= 3 + replicas for entry in ranges):
                    return
                else:
                    last = "a range has too few replicas"
        except (OSError, AssertionError, ValueError, TypeError) as error:
            last = f"{type(error).__name__}: {error}"
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{host}:{port} did not list {replicas} replica(s) within {timeout} s ({last})")
        time.sleep(0.05)


def wait_primary(
    port: int, master: str, primary: str, host: str = "127.0.0.1", timeout: float = 20.0, pid: int | None = None
) -> None:
    """Waits until the coordinator on `host:port` names `primary` (`host:port`) for `master` in
    `SENTINEL get-master-addr-by-name`. With `pid`, fails as soon as that process exits."""
    deadline = time.monotonic() + timeout
    last = "no answer"
    while True:
        if not _alive(None, pid):
            raise RuntimeError(f"the coordinator for {host}:{port} exited before it named a primary")
        try:
            with socket.create_connection((host, port), timeout=2) as client:
                prefix, value = request(
                    client.makefile("rwb"), b"SENTINEL", b"get-master-addr-by-name", master.encode()
                )
                if prefix == b"*" and value and len(value) == 2:
                    named = f"{value[0][1].decode()}:{value[1][1].decode()}"
                    if named == primary:
                        return
                    last = f"names {named}"
                else:
                    last = f"answered {prefix!r} {value!r}"
        except (OSError, AssertionError, ValueError, TypeError, AttributeError) as error:
            last = f"{type(error).__name__}: {error}"
        if time.monotonic() >= deadline:
            raise TimeoutError(f"the coordinator {host}:{port} did not name {primary} within {timeout} s ({last})")
        time.sleep(0.05)


def _serves(stream, data_dir: str) -> bool:
    prefix, value = request(stream, b"CONFIG", b"GET", b"dir")
    if prefix != b"*" or not value or len(value) < 2 or value[1][1] is None:
        return False
    reported = os.fsdecode(value[1][1])
    return os.path.realpath(reported) == os.path.realpath(data_dir)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Wait until a Ruvio server answers PING with PONG.")
    commands = parser.add_subparsers(dest="command", required=True)
    ready = commands.add_parser("wait-ready")
    ready.add_argument("--host", default="127.0.0.1")
    ready.add_argument("--port", type=int, required=True)
    ready.add_argument("--timeout", type=float, default=20.0)
    ready.add_argument("--pid", type=int, help="fail at once when this process exits")
    ready.add_argument("--data-dir", help="the data directory the server must report")
    ready.add_argument("--cluster", action="store_true", help="also wait for cluster_state:ok")
    replicas = commands.add_parser("wait-replicas")
    replicas.add_argument("--host", default="127.0.0.1")
    replicas.add_argument("--port", type=int, required=True)
    replicas.add_argument("--timeout", type=float, default=20.0)
    replicas.add_argument("--replicas", type=int, default=1)
    primary = commands.add_parser("wait-primary")
    primary.add_argument("--host", default="127.0.0.1")
    primary.add_argument("--port", type=int, required=True)
    primary.add_argument("--timeout", type=float, default=20.0)
    primary.add_argument("--pid", type=int, help="fail at once when this process exits")
    primary.add_argument("--master", required=True, help="the group name")
    primary.add_argument("--primary", required=True, help="the host:port the coordinator must name")
    options = parser.parse_args(argv)
    if options.command == "wait-primary":
        try:
            wait_primary(
                options.port, options.master, options.primary, host=options.host, timeout=options.timeout, pid=options.pid
            )
        except (RuntimeError, TimeoutError) as error:
            print(error, file=sys.stderr)
            return 1
        return 0
    if options.command == "wait-replicas":
        try:
            wait_replicas(options.port, options.replicas, host=options.host, timeout=options.timeout)
        except TimeoutError as error:
            print(error, file=sys.stderr)
            return 1
        return 0
    try:
        wait_ready(
            options.port,
            host=options.host,
            timeout=options.timeout,
            pid=options.pid,
            data_dir=options.data_dir,
            cluster=options.cluster,
        )
    except (RuntimeError, TimeoutError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
