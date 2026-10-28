#!/usr/bin/env python3
"""Probe for ``scripts/seed_failover.py``: connects with the seeds in ``RUVIO_SEEDS``, reads
``seed:name``, waits for a line on stdin (the first server is stopped by then) and reads it again.

    python3 scripts/seed_failover.py target/debug/ruvio -- \
        python3 clients/python/benchmarks/seed_probe.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from ruvio import Ruvio  # noqa: E402


def main() -> int:
    client = Ruvio(seeds=os.environ["RUVIO_SEEDS"].split(","), connect_timeout=1.0)
    print("first", client.get("seed:name") or "", flush=True)
    sys.stdin.readline()
    for _ in range(50):
        try:
            value = client.get("seed:name")
        except Exception:
            time.sleep(0.1)
            continue
        print("after", value or "", flush=True)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
