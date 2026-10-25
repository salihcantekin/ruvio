#!/usr/bin/env python3
"""Latency of an established TLS connection while many new TLS connections handshake (reconnect storm).
Usage: tls_storm.py <ruvio-binary> <cert.pem> <key.pem> [seconds]. One shard; prints PING p50/p99/max with and without the storm."""
import multiprocessing, os, socket, ssl, statistics, subprocess, sys, tempfile, time
BIN, CERT, KEY = [os.path.abspath(a) for a in sys.argv[1:4]]; SECS = float(sys.argv[4]) if len(sys.argv) > 4 else 5
PORT = 6741
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.minimum_version = ssl.TLSVersion.TLSv1_2; ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
d = tempfile.mkdtemp()
env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{PORT}", RUVIO_SHARDS="1", RUVIO_TLS_CERT=CERT, RUVIO_TLS_KEY=KEY, RUVIO_DATA_DIR=d + "/data")
p = subprocess.Popen([BIN], env=env, cwd=d, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def connect():
    s = socket.create_connection(("127.0.0.1", PORT), timeout=5); return ctx.wrap_socket(s)
for _ in range(100):
    try: victim = connect(); break
    except OSError: time.sleep(0.1)
def ping():
    t = time.perf_counter(); victim.sendall(b"*1\r\n$4\r\nPING\r\n"); victim.recv(16); return (time.perf_counter() - t) * 1e3
def measure(label, secs):
    xs = []; end = time.time() + secs
    while time.time() < end: xs.append(ping())
    xs.sort(); print(f"{label}: n={len(xs)} p50 {xs[len(xs)//2]:.3f} ms  p99 {xs[int(len(xs)*.99)]:.3f} ms  max {xs[-1]:.3f} ms", flush=True)
def storm(stop, done):
    # Separate processes: Python threads would hold the GIL and slow the victim loop itself.
    n = 0
    while not stop.is_set():
        try: c = connect(); c.close(); n += 1
        except OSError: pass
    with done.get_lock(): done.value += n
def cpu(pid):
    out = subprocess.run(["ps", "-o", "utime=,stime=", "-p", str(pid)], capture_output=True, text=True).stdout.split()
    def sec(t):
        parts = t.replace("-", ":").split(":"); v = 0.0
        for x in parts: v = v * 60 + float(x)
        return v
    return sum(sec(x) for x in out)
measure("quiet", SECS)
mp = multiprocessing.get_context("fork")
stop = mp.Event(); done = mp.Value("i", 0)
c0 = cpu(p.pid); workers = [mp.Process(target=storm, args=(stop, done)) for _ in range(3)]
for w in workers: w.start()
time.sleep(0.5); t0 = time.time(); measure("storm", SECS); el = time.time() - t0
stop.set()
for w in workers: w.join()
c1 = cpu(p.pid)
print(f"handshakes {done.value} in ~{el+0.5:.1f}s, server cpu per handshake ~{(c1-c0)/max(done.value,1)*1e3:.2f} ms")
p.terminate()
