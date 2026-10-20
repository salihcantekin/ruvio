#!/usr/bin/env python3
"""Fault scenarios for `ruvio coordinator`, each on fresh processes (3 data nodes, 3
coordinators, a TCP proxy between the coordinators and each data node):
  partition   the primary is cut from the coordinators only (clients and replicas still reach it)
  leader      the coordinator leader is killed, then the primary
  minority    two of three coordinators are down: no failover may happen
  restart     every coordinator restarts; the agreed primary is kept
  loss        a writer runs while the primary is killed: acknowledged writes lost
  durable     the same with CLIENT DURABILITY REPLICAS 1: no acknowledged write is lost
  client      redis-py's Sentinel client follows the failover (skipped without redis-py)"""
import os, signal, socket, subprocess, sys, threading, time, shutil

# Usage: scripts/coordinator_faults.py target/debug/ruvio [scenario ...]
# Every scenario takes a few seconds; all of them well under a minute.
RUVIO = sys.argv[1]
SCENARIOS = sys.argv[2:] or ["partition", "leader", "minority", "restart", "loss", "durable", "client"]
WORK = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"ruvio-coordinator-faults-{os.getpid()}")
NODES = [(18300, 38300), (18320, 38320), (18340, 38340)]
PROXY = [18500, 18520, 18540]           # coordinators reach node i through PROXY[i]
COORDS = [(1, 28300), (2, 28301), (3, 28302)]
SH = 1

class Proxy:
    def __init__(self, listen, target):
        self.target, self.cut, self.conns = target, False, []
        self.server = socket.socket(); self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", listen)); self.server.listen(64)
        threading.Thread(target=self.accept, daemon=True).start()
    def accept(self):
        while True:
            try: c, _ = self.server.accept()
            except OSError: return
            if self.cut: c.close(); continue
            try: u = socket.create_connection(("127.0.0.1", self.target), timeout=1)
            except OSError: c.close(); continue
            self.conns += [c, u]
            for a, b in ((c, u), (u, c)): threading.Thread(target=self.pipe, args=(a, b), daemon=True).start()
    def pipe(self, a, b):
        try:
            while True:
                d = a.recv(65536)
                if not d or self.cut: break
                b.sendall(d)
        except OSError: pass
        for s in (a, b):
            try: s.close()
            except OSError: pass
    def set_cut(self, cut):
        self.cut = cut
        if cut:
            for s in self.conns:
                try: s.close()
                except OSError: pass
            self.conns = []

def cli(port, *a, t=3):
    try: return subprocess.run(["redis-cli", "-p", str(port), *a], capture_output=True, text=True, timeout=t).stdout.strip()
    except subprocess.TimeoutExpired: return ""

def wait(c, t=20, step=0.05):
    end = time.time() + t
    while time.time() < end:
        try:
            if c(): return True
        except Exception: pass
        time.sleep(step)
    return False

def start_node(i, role, primary=None, extra=None):
    resp, repl = NODES[i]; d = f"{WORK}/n{i}"; os.makedirs(d, exist_ok=True)
    e = dict(os.environ, RUVIO_REPLICATION_ROLE=role, RUVIO_REPLICATION_LISTEN=f"127.0.0.1:{repl}", **(extra or {}))
    if primary: e["RUVIO_REPLICATION_PRIMARY"] = f"127.0.0.1:{primary}"
    return subprocess.Popen([RUVIO, "--config", f"{d}/r.toml", "--address", f"127.0.0.1:{resp}", "--shards", str(SH),
                             "--durability", "always", "--data-dir", d], env=e, stdout=open(f"{d}.log", "a"), stderr=subprocess.STDOUT)

def start_coord(cid, port):
    d = f"{WORK}/c{cid}"; os.makedirs(d, exist_ok=True)
    members = ",\n".join(f'  {{ address = "127.0.0.1:{PROXY[i]}", replication = "127.0.0.1:{NODES[i][1]}" }}' for i in range(3))
    peers = ", ".join(f'"{i}@127.0.0.1:{p}"' for i, p in COORDS)
    open(f"{d}/c.toml", "w").write(f'id = {cid}\nlisten = "127.0.0.1:{port}"\ndata_dir = "{d}/data"\npeers = [{peers}]\nping_interval_ms = 200\ndown_after_ms = 1000\n\n[[groups]]\nname = "main"\nmembers = [\n{members}\n]\n')
    return subprocess.Popen([RUVIO, "coordinator", "--config", f"{d}/c.toml"], stdout=open(f"{d}.log", "a"), stderr=subprocess.STDOUT)

def addr(port): return cli(port, "SENTINEL", "get-master-addr-by-name", "main").split()
def primary_index(port):
    a = addr(port)
    return PROXY.index(int(a[1])) if len(a) == 2 else None

def setup(extra=None):
    shutil.rmtree(WORK, ignore_errors=True); os.makedirs(WORK)
    proxies = [Proxy(PROXY[i], NODES[i][0]) for i in range(3)]
    nodes = [start_node(0, "primary", extra=extra), start_node(1, "replica", NODES[0][1], extra), start_node(2, "replica", NODES[0][1], extra)]
    assert wait(lambda: all(cli(r, "PING") == "PONG" for r, _ in NODES))
    coords = {cid: start_coord(cid, p) for cid, p in COORDS}
    assert wait(lambda: all(primary_index(p) == 0 for _, p in COORDS)), "coordinators did not agree"
    return proxies, nodes, coords

def stop(procs):
    for p in procs:
        if p.poll() is None: p.send_signal(signal.SIGTERM)
    for p in procs:
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill()

failures = 0

def check(name, ok):
    global failures
    print(("PASS " if ok else "FAIL ") + name)
    failures += 0 if ok else 1
    return ok

def run(SCEN):
    print(f"== {SCEN}")
    proxies, nodes, coords = setup({"RUVIO_MIN_REPLICAS_TO_WRITE": "1", "RUVIO_MIN_REPLICAS_MAX_LAG": "2"} if SCEN == "partition" else None)
    try:
        if SCEN == "partition":
            cli(NODES[0][0], "SET", "before", "1")
            t0 = time.time(); proxies[0].set_cut(True)
            moved = wait(lambda: primary_index(COORDS[0][1]) not in (None, 0), 20)
            check(f"coordinators fail over a primary they cannot reach ({time.time()-t0:.2f}s)", moved)
            new = primary_index(COORDS[0][1])
            check("the new primary takes writes", wait(lambda: cli(NODES[new][0], "SET", "after", "1") == "OK", 10))
            # The old primary is still reachable by clients; its replicas were repointed, so
            # min-replicas-to-write 1 stops it within the lag (2 s) plus the 100 ms check.
            t1 = time.time()
            fenced = wait(lambda: cli(NODES[0][0], "SET", "split", "1").startswith("NOREPLICAS"), 10)
            check(f"the cut-off old primary refuses writes {time.time()-t1:.2f}s after the failover", fenced)
            proxies[0].set_cut(False)
            check("healed: the old primary follows the new one", wait(lambda: cli(NODES[0][0], "GET", "after") == "1", 20))
            check("the split-brain write never reaches the group", cli(NODES[new][0], "EXISTS", "split") == "0")
        elif SCEN == "leader":
            # Kill one coordinator at a time; each time the remaining two still fail over.
            lead = [cid for cid, port in COORDS if "is_leader:1" in cli(port, "INFO")]
            assert len(lead) == 1, lead
            stopped = coords.pop(lead[0]); stopped.send_signal(signal.SIGKILL); stopped.wait()
            alive = [port for cid, port in COORDS if cid != lead[0]][0]
            t0 = time.time(); nodes[0].send_signal(signal.SIGKILL); nodes[0].wait()
            ok = wait(lambda: primary_index(alive) not in (None, 0), 20); check(f"two of three coordinators fail over ({time.time()-t0:.2f}s)", ok)
        elif SCEN == "minority":
            for cid in (2, 3):
                coords[cid].send_signal(signal.SIGKILL); coords[cid].wait(); del coords[cid]
            nodes[0].send_signal(signal.SIGKILL); nodes[0].wait()
            time.sleep(4)
            check("one coordinator alone does not fail over", primary_index(COORDS[0][1]) == 0)
            check("no replica was promoted", all(cli(NODES[i][0], "SET", "x", "1").startswith("READONLY") for i in (1, 2)))
        elif SCEN == "restart":
            stop(list(coords.values()))
            coords = {cid: start_coord(cid, p) for cid, p in COORDS}
            check("after a restart the agreed primary is kept", wait(lambda: all(primary_index(p) == 0 for _, p in COORDS), 20))
            nodes[0].send_signal(signal.SIGKILL); nodes[0].wait()
            check("and the restarted coordinators still fail over", wait(lambda: primary_index(COORDS[0][1]) not in (None, 0), 20))
        elif SCEN == "client":
            # A Redis client library with Sentinel support follows the failover by itself.
            try:
                import redis
                from redis.sentinel import Sentinel
            except ImportError:
                print("SKIP client: redis-py is not installed (pip install redis)")
                return
            sentinel = Sentinel([("127.0.0.1", p) for _, p in COORDS], socket_timeout=1)
            check("redis-py discovers the primary", sentinel.discover_master("main") == ("127.0.0.1", PROXY[0]))
            master = sentinel.master_for("main", socket_timeout=1)
            check("redis-py writes through it", master.set("k", "v") is True)
            nodes[0].send_signal(signal.SIGKILL); nodes[0].wait(); t0 = time.time()
            def write():
                try:
                    return master.set("after", "1")
                except (redis.ConnectionError, redis.TimeoutError, redis.ReadOnlyError):
                    return False
            ok = wait(write, 20)
            check(f"redis-py writes to the new primary {time.time()-t0:.2f}s after the kill", ok)
        elif SCEN in ("loss", "durable"):
            acked, stop_flag = [], threading.Event()
            def writer():
                s = socket.create_connection(("127.0.0.1", NODES[0][0])); f = s.makefile("rb"); i = 0
                if SCEN == "durable":
                    s.sendall(b"*4\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n1\r\n")
                    assert f.readline().startswith(b"+OK")
                # A durable reply waits for a replica's fsync, so that writer sends batches of 32.
                batch = 32 if SCEN == "durable" else 1
                while not stop_flag.is_set():
                    ks = [f"w{i + j}".encode() for j in range(batch)]
                    try: s.sendall(b"".join(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n" % (len(k), k) for k in ks))
                    except OSError: return  # the primary was killed
                    for k in ks:
                        try: line = f.readline()
                        except OSError: return
                        if not line: return
                        if line.startswith(b"+OK"): acked.append(k.decode())
                    i += batch
            th = threading.Thread(target=writer, daemon=True); th.start()
            time.sleep(1.0)
            nodes[0].send_signal(signal.SIGKILL); nodes[0].wait(); stop_flag.set(); th.join(timeout=5)
            assert wait(lambda: primary_index(COORDS[0][1]) not in (None, 0), 20)
            new = primary_index(COORDS[0][1]); time.sleep(0.5)
            keys = subprocess.run(["redis-cli", "-p", str(NODES[new][0])], input="".join(f"EXISTS {k}\n" for k in acked), capture_output=True, text=True).stdout.split()
            lost = keys.count("0")
            print(f"acknowledged writes {len(acked)}, missing on the new primary {lost}")
            if SCEN == "durable":
                check(f"no write acknowledged with CLIENT DURABILITY REPLICAS 1 is lost ({len(acked)} acknowledged)", len(acked) > 0 and lost == 0)
            else:
                check("lost writes are only the last ones before the crash", lost == 0 or all(k == "0" for k in keys[-lost:]))
    finally:
        stop(list(coords.values()) + nodes)
        for p in proxies:
            # shutdown wakes the accept thread; close alone leaves the port listening.
            try: p.server.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            p.server.close()


for scenario in SCENARIOS:
    run(scenario)
shutil.rmtree(WORK, ignore_errors=True)
sys.exit(1 if failures else 0)
