#!/usr/bin/env bash
# Real network partitions of a coordinator-managed group, in Docker (3 data nodes, 3 coordinators).
#
#   leader     the coordinator leader is cut from the other coordinators only (it still reaches
#              the data nodes), then the primary is killed: the majority fails over, the new
#              primary stays primary, and after the heal the old leader agrees.
#   lease-off  {old primary n1 + replica n2} | {replica n3 + coordinators}, a client writing to
#              n1 the whole time, every node with min-replicas-to-write 1, no primary lease.
#              Reports how many acknowledged writes the heal throws away (expected: many).
#   lease-on   the same with primary_lease_ms on the coordinators and primary-lease on the nodes:
#              n1 must refuse writes before n3 is promoted, and no write acknowledged after the
#              first refusal may exist; only writes from the cut to the first refusal can be lost.
#
# Usage: scripts/docker_partition.sh path/to/ruvio [leader|lease-off|lease-on ...]   (default: all three)
# Needs docker, redis-cli and python3 on the host. The binary runs in RUVIO_DOCKER_IMAGE
# (default ubuntu:24.04), so build it on a glibc no newer than that image's.
# Each mode takes about a minute. Exit status 0 when every PASS line was printed.
set -u
BIN=$(realpath "$1"); shift
MODES=${*:-leader lease-off lease-on}
IMG=${RUVIO_DOCKER_IMAGE:-ubuntu:24.04}
S=s3cret
P=pw
fail=0
check() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
wait_for() { local t=$1; shift; local end=$((SECONDS+t)); while [ $SECONDS -lt $end ]; do eval "$@" && return 0; sleep 0.2; done; return 1; }
now() { python3 -c 'import time; print(time.monotonic())'; }

# The work directory holds a copy of the binary: executables must not sit on a noexec mount.
workdir() {
  W=${TMPDIR:-/tmp}/ruvio-docker-partition-$1
  rm -rf "$W"; mkdir -p "$W"; cp "$BIN" "$W/ruvio"
}

coord_toml() { # file id ping down extra-line member-prefix
  cat > "$1" <<EOF
id = $2
listen = "0.0.0.0:26400"
insecure_no_secret = true
data_dir = "/data/coordinator"
peers = ["1@r1:26400", "2@r2:26400", "3@r3:26400"]
ping_interval_ms = $3
down_after_ms = $4
$5

[[groups]]
name = "main"
members = [
  { address = "${6}1:6379", replication = "${6}1:26379" },
  { address = "${6}2:6379", replication = "${6}2:26379" },
  { address = "${6}3:6379", replication = "${6}3:26379" },
]
EOF
}

# --- leader: two networks, rp-data (everyone) and rp-raft (coordinators only, aliases r1..r3).
leader_cleanup() {
  docker rm -f rp-n1 rp-n2 rp-n3 rp-c1 rp-c2 rp-c3 >/dev/null 2>&1
  docker network rm rp-data rp-raft >/dev/null 2>&1
}
run_leader() {
  echo "== leader"
  workdir leader; leader_cleanup
  docker network create rp-data >/dev/null; docker network create rp-raft >/dev/null
  for i in 1 2 3; do
    role=replica; prim="-e RUVIO_REPLICATION_PRIMARY=rp-n1:26379"
    [ $i = 1 ] && { role=primary; prim=""; }
    docker run -d --name rp-n$i --network rp-data -p 127.0.0.1:1638$i:6379 -v "$W/ruvio":/ruvio:ro \
      -e RUVIO_ALLOW_UNAUTHENTICATED=true -e RUVIO_PROTECTED_MODE=false -e RUVIO_REPLICATION_LISTEN=0.0.0.0:26379 \
      -e RUVIO_REPLICATION_PASSWORD=$P -e RUVIO_REPLICATION_ROLE=$role $prim \
      $IMG /ruvio --address 0.0.0.0:6379 --shards 1 --durability always --data-dir /data >/dev/null
  done
  for i in 1 2 3; do
    coord_toml "$W/c$i.toml" $i 200 1000 "" rp-n
    docker create --name rp-c$i --network rp-data -p 127.0.0.1:2640$i:26400 -v "$W/ruvio":/ruvio:ro \
      -v "$W/c$i.toml":/c.toml:ro -e RUVIO_COORDINATOR_SECRET=$S -e RUVIO_REPLICATION_PASSWORD=$P \
      $IMG /ruvio coordinator --config /c.toml >/dev/null
    docker network connect --alias r$i rp-raft rp-c$i
    docker start rp-c$i >/dev/null
  done
  cc() { timeout 3 redis-cli -p 2640$1 --no-auth-warning -a $S "${@:2}" 2>/dev/null; }
  nc_() { timeout 3 redis-cli -p 1638$1 "${@:2}" 2>/dev/null; }
  role() { nc_ $1 ROLE | head -1; }
  master() { cc $1 SENTINEL get-master-addr-by-name main | head -1; }

  wait_for 30 '[ "$(master 1)" = rp-n1 ] && [ "$(master 2)" = rp-n1 ] && [ "$(master 3)" = rp-n1 ]'
  check "three coordinators agree on rp-n1" $?
  lead=""; wait_for 15 'for i in 1 2 3; do cc $i INFO | grep -q "is_leader:1" && lead=$i; done; [ -n "$lead" ]'
  others=$(for i in 1 2 3; do [ $i != "$lead" ] && echo $i; done | tr '\n' ' ')
  echo "leader: rp-c$lead"
  nc_ 1 SET before 1 >/dev/null

  t0=$SECONDS
  docker network disconnect rp-raft rp-c$lead
  newlead=""
  wait_for 15 'for i in $others; do cc $i INFO | grep -q "is_leader:1" && newlead=$i; done; [ -n "$newlead" ]'
  check "the majority elects a new leader ($((SECONDS-t0))s)" $?

  docker kill rp-n1 >/dev/null; t1=$SECONDS
  wait_for 20 '[ "$(role 2)" = master ] || [ "$(role 3)" = master ]'
  check "the majority fails over the killed primary ($((SECONDS-t1))s)" $?
  newp=2; [ "$(role 3)" = master ] && newp=3
  flaps=0; end=$((SECONDS+6))
  while [ $SECONDS -lt $end ]; do [ "$(role $newp)" = master ] || flaps=$((flaps+1)); sleep 0.2; done
  check "rp-n$newp stays primary while the cut-off leader is alive (flaps=$flaps)" $([ $flaps = 0 ] && echo 0 || echo 1)
  check "the new primary takes writes" $([ "$(nc_ $newp SET after 1)" = OK ] && echo 0 || echo 1)

  docker network connect --alias r$lead rp-raft rp-c$lead
  wait_for 20 '[ "$(master $lead)" = "rp-n$newp" ]'; check "healed: the old leader agrees with the majority" $?
  docker start rp-n1 >/dev/null
  wait_for 30 '[ "$(role 1)" = slave ]'; check "the old primary comes back as a replica" $?
  wait_for 10 '[ "$(nc_ 1 GET after)" = 1 ]'; check "and has the write made after the failover" $?
  for c in rp-c1 rp-c2 rp-c3 rp-n1; do docker logs $c > "$W/$c.log" 2>&1; done
  leader_cleanup
}

# --- lease-off / lease-on: rp-all (everyone; the cut removes n3 and the coordinators from it),
# rp-left (n1, n2: their replication link survives the cut), rp-right (n3 + coordinators).
lease_cleanup() {
  docker rm -f rp-l1 rp-l2 rp-l3 rp-lc1 rp-lc2 rp-lc3 >/dev/null 2>&1
  docker network rm rp-all rp-left rp-right >/dev/null 2>&1
}
run_lease() {
  MODE=$1
  echo "== lease-$MODE"
  workdir lease-$MODE; lease_cleanup
  docker network create rp-all >/dev/null; docker network create rp-left >/dev/null; docker network create rp-right >/dev/null
  lease_env="-e RUVIO_PRIMARY_LEASE=no -e RUVIO_PRIMARY_LEASE_COORDINATORS=1,2,3"; lease_toml=""
  if [ "$MODE" = on ]; then lease_env="-e RUVIO_PRIMARY_LEASE=yes -e RUVIO_PRIMARY_LEASE_COORDINATORS=1,2,3"; lease_toml="primary_lease_ms = 2000"; fi
  # n3 is created on rp-right so its published port keeps working after the cut.
  for i in 1 2 3; do
    role=replica; prim="-e RUVIO_REPLICATION_PRIMARY=rp-l1:26379"
    [ $i = 1 ] && { role=primary; prim=""; }
    first=rp-all; [ $i = 3 ] && first=rp-right
    docker create --name rp-l$i --hostname rp-l$i --network $first -p 127.0.0.1:1648$i:6379 -v "$W/ruvio":/ruvio:ro \
      -e RUVIO_ALLOW_UNAUTHENTICATED=true -e RUVIO_PROTECTED_MODE=false \
      -e RUVIO_REPLICATION_LISTEN=0.0.0.0:26379 -e RUVIO_REPLICATION_PASSWORD=$P \
      -e RUVIO_MIN_REPLICAS_TO_WRITE=1 -e RUVIO_MIN_REPLICAS_MAX_LAG=10 $lease_env \
      -e RUVIO_REPLICATION_ROLE=$role $prim $IMG /ruvio --address 0.0.0.0:6379 --shards 1 --durability always --data-dir /data >/dev/null
    case $i in
      1|2) docker network connect --alias rp-l$i rp-left rp-l$i ;;
      3) docker network connect --alias rp-l3 rp-all rp-l3 ;;
    esac
    docker start rp-l$i >/dev/null
  done
  for i in 1 2 3; do
    coord_toml "$W/c$i.toml" $i 500 3000 "$lease_toml" rp-l
    docker create --name rp-lc$i --network rp-right --network-alias r$i -p 127.0.0.1:2650$i:26400 \
      -v "$W/ruvio":/ruvio:ro -v "$W/c$i.toml":/c.toml:ro \
      -e RUVIO_COORDINATOR_SECRET=$S -e RUVIO_REPLICATION_PASSWORD=$P $IMG /ruvio coordinator --config /c.toml >/dev/null
    docker network connect --alias r$i rp-all rp-lc$i
    docker start rp-lc$i >/dev/null
  done
  cc() { timeout 3 redis-cli -p 2650$1 --no-auth-warning -a $S "${@:2}" 2>/dev/null; }
  nc_() { timeout 3 redis-cli -p 1648$1 "${@:2}" 2>/dev/null; }
  role() { nc_ $1 ROLE | head -1; }
  master() { cc $1 SENTINEL get-master-addr-by-name main | head -1; }

  wait_for 40 '[ "$(master 1)" = rp-l1 ] && [ "$(master 2)" = rp-l1 ] && [ "$(master 3)" = rp-l1 ]'; check "three coordinators agree on rp-l1" $?
  wait_for 30 '[ "$(nc_ 1 SET before 1)" = OK ]'; check "rp-l1 takes writes" $?
  wait_for 30 '[ "$(nc_ 3 GET before)" = 1 ] && [ "$(nc_ 2 GET before)" = 1 ]'; check "both replicas copy rp-l1" $?

  # A client on the host writes w<i> to rp-l1 every 10 ms and logs each reply with a monotonic
  # timestamp; it also logs when rp-l3 first answers ROLE master.
  cat > "$W/drive.py" <<'PY'
import socket, sys, time
out, stop_file = sys.argv[1:3]
def conn(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    return s, s.makefile("rb")
def cmd(sf, *args):
    s, f = sf
    s.sendall(b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in [x.encode() for x in args]))
    line = f.readline()
    if not line: raise OSError("closed")
    if line[:1] == b"*":
        items = []
        for _ in range(int(line[1:])):
            l = f.readline()
            items.append(f.readline().strip().decode() if l[:1] == b"$" else l[1:].strip().decode())
        return items
    if line[:1] == b"$":
        return None if int(line[1:]) < 0 else f.readline().strip().decode()
    return line[1:].decode().strip() if line[:1] == b"+" else line.decode().strip()
w = r3 = promoted = None
log = open(out, "w"); i = 0
while True:
    try:
        open(stop_file).close(); break
    except FileNotFoundError:
        pass
    now = time.monotonic()
    try:
        if w is None: w = conn(16481)
        reply = cmd(w, "SET", "w%d" % i, str(i))
    except Exception as e:
        reply = "ERR-conn " + str(e); w = None
    log.write("%f w%d %s\n" % (now, i, reply)); i += 1
    if promoted is None:
        try:
            if r3 is None: r3 = conn(16483)
            role = cmd(r3, "ROLE")
            if isinstance(role, list) and role and role[0] == "master":
                promoted = time.monotonic(); log.write("%f PROMOTED -\n" % promoted)
        except Exception:
            r3 = None
    log.flush()
    time.sleep(0.01)
PY
  rm -f "$W/stop"
  python3 "$W/drive.py" "$W/writes.log" "$W/stop" &
  DRIVER=$!
  sleep 2
  CUT=$(now); STEPS=""
  for c in rp-l3 rp-lc1 rp-lc2 rp-lc3; do
    docker network disconnect rp-all $c
    STEPS="$STEPS $c=$(now)"
  done
  # The lease stops being renewed by a majority once the second coordinator is cut off.
  MAJ=$(echo $STEPS | tr ' ' '\n' | grep rp-lc2= | cut -d= -f2)

  wait_for 40 '[ "$(role 3)" = master ]'; check "rp-l3 is promoted" $?
  # rp-l3's replicas are on the other side, so with min-replicas-to-write 1 it refuses with the
  # min-replicas error until the heal; with the lease on it must first get past the lease gate.
  newp_open() { case "$(nc_ 3 SET onnew 1)" in OK|"NOREPLICAS Not enough good replicas"*) return 0;; *) return 1;; esac; }
  wait_for 30 newp_open; check "rp-l3 write gate past the lease" $?
  sleep 3
  touch "$W/stop"; wait $DRIVER

  for c in rp-l3 rp-lc1 rp-lc2 rp-lc3; do docker network connect rp-all $c; done
  wait_for 40 '[ "$(role 1)" = slave ]'; check "healed: rp-l1 follows rp-l3" $?
  wait_for 40 '[ "$(nc_ 3 SET onnew 1)" = OK ]'; check "healed: rp-l3 takes writes with its replicas back" $?
  wait_for 40 '[ "$(nc_ 1 GET onnew)" = 1 ]'; check "healed: rp-l1 copied the new primary" $?

  python3 - "$W/writes.log" "$MAJ" "$MODE" <<'PY'
import subprocess, sys
log, cut, mode = sys.argv[1], float(sys.argv[2]), sys.argv[3]
acked, first_refusal, promoted = [], None, None
for line in open(log):
    t, key, reply = line.rstrip("\n").split(" ", 2); t = float(t)
    if key == "PROMOTED": promoted = t; continue
    if t < cut: continue
    if reply == "OK": acked.append((t, key))
    elif first_refusal is None: first_refusal = (t, reply)
out = subprocess.run(["redis-cli", "-p", "16483"], input="".join("EXISTS %s\n" % k for _, k in acked),
                     capture_output=True, text=True).stdout.split()
lost = [acked[i] for i, v in enumerate(out) if v == "0"]
f = lambda t: "-" if t is None else "%.2fs" % (t - cut)
refused_at = first_refusal[0] if first_refusal else None
after_refusal = sum(1 for t, _ in acked if refused_at is not None and t > refused_at)
print("RESULT mode=%s acked_after_cut=%d lost_after_heal=%d first_refusal=%s promoted=%s acked_after_first_refusal=%d refusal=%s"
      % (mode, len(acked), len(lost), f(refused_at), f(promoted), after_refusal, first_refusal[1] if first_refusal else "-"))
print("(times from the moment the second coordinator was cut off from rp-l1)")
ok = True
if mode == "on":
    def check(name, good):
        global ok
        print(("PASS " if good else "FAIL ") + name); ok = ok and good
    check("rp-l1 refuses writes before rp-l3 is promoted", refused_at is not None and promoted is not None and refused_at < promoted)
    check("rp-l1 acknowledges no write after its first refusal", after_refusal == 0)
    check("only writes from the cut to the first refusal are lost", all(t <= refused_at for t, _ in lost) if refused_at else False)
sys.exit(0 if ok else 1)
PY
  check "lease-$MODE summary" $?
  for c in rp-lc1 rp-lc2 rp-lc3 rp-l1; do docker logs $c > "$W/$c.log" 2>&1; done
  lease_cleanup
}

for m in $MODES; do
  case $m in
    leader) run_leader ;;
    lease-off) run_lease off ;;
    lease-on) run_lease on ;;
    *) echo "unknown mode $m (leader, lease-off, lease-on)"; exit 2 ;;
  esac
done
exit $fail
