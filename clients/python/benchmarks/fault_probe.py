#!/usr/bin/env python3
"""Probe for ``scripts/client_faults.py``: writes ``fault:<name>:<i>`` = ``i`` with durability
replicas 1, reads each key back, and keeps going through a failover until a line arrives on stdin.

    python3 scripts/client_faults.py target/debug/ruvio

Environment: ``RUVIO_SEEDS`` (comma separated), ``RUVIO_PROBE_NAME`` and, for TLS, ``RUVIO_PROBE_CA``
(PEM path). Output, one line each: ``ready``, ``ack <i>``, ``err <i> <text>``, ``bad <i> <value>``
and ``done``. The same protocol in every client.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import select  # noqa: E402

from ruvio import Ruvio  # noqa: E402


def stopped() -> bool:
    return bool(select.select([sys.stdin], [], [], 0)[0]) and sys.stdin.readline() != ""


def main() -> int:
    name = os.environ["RUVIO_PROBE_NAME"]
    ca = os.environ.get("RUVIO_PROBE_CA")
    options = {"seeds": os.environ["RUVIO_SEEDS"].split(","), "durability_replicas": 1}
    if ca:
        options.update(tls=True, tls_ca_file=ca)
    client = Ruvio(**options)
    print("ready", flush=True)
    i = 1
    while not stopped():
        key = f"fault:{name}:{i}"
        try:
            client.set(key, str(i))
            got = client.get(key)
            got = got.decode() if isinstance(got, bytes) else got
            if got != str(i):
                print(f"bad {i} {got}", flush=True)
            print(f"ack {i}", flush=True)
            i += 1
        except Exception as error:  # noqa: BLE001 - every failure is reported and the same key retried
            print(f"err {i} {type(error).__name__}", flush=True)
            time.sleep(0.02)
    print("done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
