#!/usr/bin/env python3
"""Live check of a client's seed list: stop the server it is on, and it must move to the next seed.

    scripts/seed_failover.py <ruvio-binary> -- <probe command...>

Starts two standalone servers, stores `seed:name` = `a` on the first and `b` on the second, and
runs the probe with `RUVIO_SEEDS=127.0.0.1:<first>,127.0.0.1:<second>`. The probe protocol, the
same in every client:

1. connect with the seed list, `GET seed:name`, print `first <value>` and wait for a line on stdin;
2. after that line (the first server is stopped by then), `GET seed:name` again, up to 50 tries
   100 ms apart because the command that hits the drop fails, then print `after <value>` and exit 0.

The check passes when the probe printed `first a` and `after b`. The whole run is limited to
60 seconds; the servers are stopped and their data removed afterwards.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _ruvio import free_port, request, wait_ready  # noqa: E402

import socket  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LIMIT_SECONDS = 60


def start(binary: str, work: Path, name: str) -> tuple[subprocess.Popen, int]:
    port = free_port()
    config = (ROOT / "ruvio.example.toml").read_text()
    config = "\n".join(
        "shutdown_grace_ms = 500" if line.startswith("shutdown_grace_ms") else line
        for line in config.splitlines()
    )
    (work / f"{name}.toml").write_text(config + "\n")
    log = open(work / f"{name}.log", "wb")
    process = subprocess.Popen(
        [
            binary,
            "--config",
            str(work / f"{name}.toml"),
            "--address",
            f"127.0.0.1:{port}",
            "--data-dir",
            str(work / name),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    wait_ready(port, process=process, data_dir=str(work / name))
    with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
        stream = client.makefile("rwb")
        assert request(stream, b"SET", b"seed:name", name.encode()) == (b"+", b"OK")
    return process, port


def stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def read_line(probe: subprocess.Popen, expected: str, deadline: float) -> str:
    result: list[str] = []
    reader = threading.Thread(target=lambda: result.append(probe.stdout.readline()), daemon=True)
    reader.start()
    reader.join(max(0.0, deadline - time.monotonic()))
    if not result:
        raise TimeoutError(f"the probe printed no `{expected}` line in time")
    line = result[0].strip()
    if not line.startswith(expected + " "):
        raise AssertionError(f"expected `{expected} <value>`, the probe printed {line!r}")
    return line.split(" ", 1)[1]


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] != "--":
        print(__doc__, file=sys.stderr)
        return 2
    binary, command = argv[0], argv[2:]
    work = Path(tempfile.mkdtemp(prefix="ruvio-seeds-"))
    servers: list[subprocess.Popen] = []
    probe = None
    ok = False
    deadline = time.monotonic() + LIMIT_SECONDS
    try:
        first, first_port = start(binary, work, "a")
        servers.append(first)
        second, second_port = start(binary, work, "b")
        servers.append(second)

        environment = dict(os.environ, RUVIO_SEEDS=f"127.0.0.1:{first_port},127.0.0.1:{second_port}")
        probe = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=environment
        )
        before = read_line(probe, "first", deadline)
        stop(first)
        probe.stdin.write("go\n")
        probe.stdin.flush()
        after = read_line(probe, "after", deadline)
        probe.wait(timeout=max(1.0, deadline - time.monotonic()))
        if (before, after, probe.returncode) != ("a", "b", 0):
            raise AssertionError(f"probe saw first={before!r} after={after!r}, exit {probe.returncode}")
        print(f"seed failover ok: first {before}, after {after}")
        ok = True
        return 0
    except (AssertionError, TimeoutError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"seed failover failed: {error}", file=sys.stderr)
        return 1
    finally:
        if probe is not None and probe.poll() is None:
            probe.kill()
            probe.wait()
        for server in servers:
            stop(server)
        if ok:
            shutil.rmtree(work, ignore_errors=True)
        else:
            for log in sorted(work.glob("*.log")):
                print(f"----- {log} -----\n{log.read_text(errors='replace')}", file=sys.stderr)
            print(f"server logs kept in {work}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
