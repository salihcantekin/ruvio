"""Five-minute tour of the built-in building blocks against a running server.

    python3 examples/09_building_blocks_tour.py [host] [port]

Start a server first (`deploy/local.sh up`, or `ruvio` with its defaults). The tour uses
keys that start with `tour:`, a stream called `tour:jobs` and a sorted set called
`tour:due`, and deletes them at the end; it never calls FLUSHALL. `CHANGES SUBSCRIBE`
reads the write-ahead log, so that step is skipped with a note unless the server runs
with a durability profile other than `memory` (for example RUVIO_DURABILITY=everysec
together with RUVIO_DATA_DIR).
"""

import socket
import sys
import time


def encode(*args):
    parts = [b"*" + str(len(args)).encode() + b"\r\n"]
    for arg in args:
        arg = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.append(b"$" + str(len(arg)).encode() + b"\r\n" + arg + b"\r\n")
    return b"".join(parts)


def read(stream):
    prefix, line = stream.read(1), stream.readline()[:-2]
    if prefix in (b"+", b"-"):
        return line.decode()
    if prefix == b":":
        return int(line)
    if prefix == b"$":
        if int(line) < 0:
            return None
        value = stream.read(int(line) + 2)[:-2]
        return value.decode(errors="replace")
    if prefix == b"*":
        return None if int(line) < 0 else [read(stream) for _ in range(int(line))]
    raise RuntimeError(f"unexpected reply {prefix!r}")


class Conn:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.stream = self.sock.makefile("rb")

    def call(self, *args):
        print("> " + " ".join(str(a) for a in args))
        self.sock.sendall(encode(*args))
        reply = read(self.stream)
        print(f"  {reply!r}")
        return reply


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379
    c = Conn(host, port)
    feed = Conn(host, port)
    print("# CHANGES SUBSCRIBE FROM NOW: opened first, read back at the end")
    feed.sock.sendall(encode("CHANGES", "SUBSCRIBE", "FROM", "NOW", "MATCH", "tour:*"))
    first = read(feed.stream)
    print(f"  cursor {first!r}")
    if isinstance(first, str) and first.startswith("ERR"):
        print("  skipped: this server has no write-ahead log (durability memory)")
        feed = None
    print("\n# LEASE: one holder at a time, with a fencing token that only grows")
    token = c.call("LEASE", "tour:job", 30000)
    c.call("LEASE", "tour:job", 30000)
    c.call("RENEW", "tour:job", token, 30000)
    c.call("RELEASE", "tour:job", token)

    print("\n# LIMIT: a sliding-window rate limit in one call")
    for _ in range(3):
        c.call("LIMIT", "tour:api", 2, 60000, "SLIDING")

    print("\n# IDEM: a retry of a finished request gets the saved result")
    c.call("IDEM", "tour:order", "BEGIN", "digest", "attempt-1", 60000)
    c.call("IDEM", "tour:order", "BEGIN", "digest", "attempt-2", 60000)
    c.call("IDEM", "tour:order", "COMPLETE", "digest", "attempt-1", "saved-result")
    c.call("IDEM", "tour:order", "BEGIN", "digest", "attempt-3", 60000)

    print("\n# XADD DELAY: the consumer group sees the entry after the delay")
    c.call("XGROUP", "CREATE", "tour:jobs", "workers", "$", "MKSTREAM")
    c.call("XADD", "tour:jobs", "DELAY", 300, "*", "kind", "send-reminder")
    c.call("XREADGROUP", "GROUP", "workers", "w1", "STREAMS", "tour:jobs", ">")
    time.sleep(0.4)
    c.call("XREADGROUP", "GROUP", "workers", "w1", "STREAMS", "tour:jobs", ">")

    print("\n# ZMOVEDUE: move the due members of a sorted set to a list in one command")
    c.call("ZADD", "tour:due", 1, "job:1", 2, "job:2", 9999999999, "job:later")
    c.call("ZMOVEDUE", "tour:due", "tour:ready", 100, "COUNT", 100)
    c.call("LRANGE", "tour:ready", 0, -1)

    print("\n# SET IFGT: a watermark that only moves forward")
    c.call("SET", "tour:mark", 10)
    c.call("SET", "tour:mark", 7, "IFGT")
    c.call("SET", "tour:mark", 12, "IFGT")

    if feed:
        print("\n# CHANGES SUBSCRIBE: every write above was logged; read the feed back")
        c.call("SET", "tour:watched", "hello")
        feed.sock.settimeout(3)
        for _ in range(60):
            push = read(feed.stream)
            print(f"  push: {push!r}")
            if isinstance(push, list) and "tour:watched" in push:
                break

    c.call("DEL", "tour:job", "tour:api", "tour:order", "tour:jobs", "tour:due",
           "tour:ready", "tour:mark", "tour:watched")


if __name__ == "__main__":
    main()
