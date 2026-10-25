#!/usr/bin/env python3
"""A full disk under a running `always` server (Linux, root): every acknowledged key must survive.

Mounts a small ext4 loop file, runs Ruvio on it, writes until the disk is full, checks the server answers
and refuses writes cleanly, kills it, frees space and restarts: every acknowledged key must be there.
    sudo python3 scripts/enospc_check.py target/release-fast/ruvio [on|off|auto] [size_mib]
"""
import os, signal, socket, subprocess, sys, tempfile, time

BIN = os.path.abspath(sys.argv[1]); MODE = sys.argv[2] if len(sys.argv) > 2 else "on"; SIZE = int(sys.argv[3]) if len(sys.argv) > 3 else 48
PORT = 6851
img_fd, img = tempfile.mkstemp(suffix=".img", dir="/var/tmp"); os.close(img_fd); mnt = tempfile.mkdtemp(prefix="enospc-", dir="/mnt" if os.path.isdir("/mnt") else "/tmp")


def sh(*a, **k):
    return subprocess.run(a, check=k.pop("check", True), capture_output=True, text=True, **k)


def enc(*a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in (y if isinstance(y, bytes) else str(y).encode() for y in a))


SERVERS = []


def start():
    env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{PORT}", RUVIO_SHARDS="1", RUVIO_DURABILITY="always", RUVIO_DATA_DIR=mnt + "/data", RUVIO_SYNC_WORKER=MODE)
    log = open(mnt + "/../enospc-server.log", "ab")
    p = subprocess.Popen([BIN], env=env, cwd="/tmp", stdout=log, stderr=subprocess.STDOUT)
    SERVERS.append(p)
    for _ in range(100):
        try: socket.create_connection(("127.0.0.1", PORT), timeout=1).close(); return p
        except OSError: time.sleep(0.1)
    raise SystemExit("server did not start: see enospc-server.log")


def read_reply(s):
    buf = b""
    while not buf.endswith(b"\r\n"):
        c = s.recv(65536)
        if not c: return None
        buf += c
    if buf[:1] == b"$" and not buf.startswith(b"$-1"):
        need = int(buf[1:buf.index(b"\r\n")]) + len(buf[:buf.index(b"\r\n") + 2]) + 2
        while len(buf) < need: buf += s.recv(65536)
    return buf


try:
    sh("truncate", "-s", f"{SIZE}M", img); sh("mkfs.ext4", "-q", "-F", img); sh("mount", "-o", "loop", img, mnt)
    with open(mnt + "/reserve", "wb") as f:  # space handed back before the restart
        f.write(b"\0" * (4 << 20)); f.flush(); os.fsync(f.fileno())
    p = start(); s = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    acked, first_error, errors, closed_at = [], None, 0, None
    value = b"v" * 900
    t0 = time.time()
    i = 0
    while time.time() - t0 < 60 and errors < 50:
        key = f"k{i}"; i += 1
        try:
            s.sendall(enc("SET", key, value + key.encode())); r = read_reply(s)
        except OSError:
            r = None
        if r is None:
            closed_at = key; break
        if r.startswith(b"+OK"): acked.append(key)
        else:
            errors += 1; first_error = first_error or r[:80]
    print(f"acked {len(acked)} keys ({len(acked) * 900 // 1024} KiB payload) in {time.time() - t0:.1f}s; error replies {errors}; first error reply: {first_error}; connection closed at {closed_at}")
    # The writing connection may have been closed by the server: probe on fresh connections.
    alive, got, wr = None, None, None
    try:
        s2 = socket.create_connection(("127.0.0.1", PORT), timeout=10)
        s2.sendall(enc("PING")); alive = read_reply(s2)
        s2.sendall(enc("GET", acked[-1])); got = read_reply(s2)
        s2.sendall(enc("SET", "after", "x")); wr = read_reply(s2)
    except OSError as e:
        print("probe connection failed:", e)
    print("server answers PING after the disk filled:", alive is not None and alive.startswith(b"+PONG"))
    print("last acknowledged key readable:", got is not None and (value + acked[-1].encode()) in got)
    print("write while full ->", wr[:70] if wr else None)
    alive = alive is not None and alive.startswith(b"+PONG")
    p.send_signal(signal.SIGKILL); p.wait()
    os.remove(mnt + "/reserve")
    p = start(); s = socket.create_connection(("127.0.0.1", PORT), timeout=30); lost = 0
    for key in acked:
        s.sendall(enc("GET", key)); r = read_reply(s)
        if r is None or (value + key.encode()) not in r: lost += 1
    print(f"after kill -9 and restart with space freed: acked keys lost = {lost} of {len(acked)}")
    s.sendall(enc("SET", "after", "x")); print("write after freeing space ->", read_reply(s)[:20])
    p.kill(); p.wait()
    print("RESULT", "PASS" if lost == 0 and alive else "FAIL")
finally:
    for proc in SERVERS:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    sh("umount", mnt, check=False); sh("rm", "-rf", mnt, img, check=False)
