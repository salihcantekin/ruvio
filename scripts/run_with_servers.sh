#!/usr/bin/env bash
# Run a client test command against throwaway Ruvio servers.
#
#   scripts/run_with_servers.sh <ruvio-binary> <base-port> -- <command...>
#
# Starts a standalone server on <base-port>, a 3-shard server (per-shard
# ports <base-port>+10 .. +12), and a 2-shard primary (<base-port>+20, +21) with a
# 2-shard replica (<base-port>+30, +31) that is listed in the primary's CLUSTER SLOTS.
# A multi-node cluster: node a (<base-port>+50, +51, slots 0-8191) and node b (+60, +61,
# slots 8192-16383), two shards each, plus a one-shard node (+70) whose older map gives every
# slot to node a, so a client seeded with it is answered MOVED by node a for node b's keys.
# A failover pair: two single-shard nodes, each able to be primary or replica (client ports
# <base-port>+80, +81; replication ports +82, +83), node 1 the primary and node 2 its replica.
# Exports RUVIO_TEST_ADDR / RUVIO_ADDR / RUVIO_CLUSTER_ADDR / RUVIO_REPLICATED_ADDR
# (the primary) / RUVIO_REPLICA_ADDR (the replica) / RUVIO_MULTINODE_A_ADDR /
# RUVIO_MULTINODE_B_ADDR / RUVIO_MULTINODE_STALE_ADDR / RUVIO_FAILOVER_ADDRS
# (the failover pair, comma separated, primary first) / RUVIO_FAILOVER_REPLICATION (their
# replication addresses, same order), and RUVIO_TEST_CONFIG_REWRITE=1,
# for the command, and stops every server afterwards.
# The repository ruvio.example.toml is copied, never edited.
#
# Each port must answer PING with PONG from the server this script started
# (checked through its pid and CONFIG GET dir) before the command runs. When
# the command fails, the server logs are printed and kept.
set -euo pipefail

binary="$1"
base="$2"
shift 2
[ "$1" = "--" ] && shift

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
pids=()

print_logs() {
  for log in "$work"/*.log; do
    [ -f "$log" ] || continue
    echo "----- $log -----" >&2
    cat "$log" >&2
  done
}

# Stops one server: SIGTERM, up to 10 s to exit, then SIGKILL.
stop() {
  local pid="$1"

  kill "$pid" 2>/dev/null || return 0
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "server $pid did not stop within 10 s; killing it" >&2
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status=$?

  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && stop "$pid"
  done
  if [ "$status" -ne 0 ]; then
    print_logs
    echo "server logs kept in $work" >&2
  else
    rm -rf "$work"
  fi
  exit "$status"
}
trap cleanup EXIT

# A short shutdown grace: a client that leaves connections open must not hold up the stop.
sed 's/^shutdown_grace_ms = .*/shutdown_grace_ms = 2000/' "$root/ruvio.example.toml" >"$work/standalone.toml"
sed 's/^shards = 1/shards = 3\
port_mode = "per-shard"/' "$work/standalone.toml" >"$work/cluster.toml"
# The replicated pair keeps a WAL (replication needs one); its replication ports are
# <base-port>+40, +41.
sed -e 's/^shards = 1/shards = 2\
port_mode = "per-shard"/' -e 's/^profile = "memory"/profile = "everysec"/' \
  "$work/standalone.toml" >"$work/replicated.toml"
perl -pe "s/^role = \"off\"/role = \"primary\"\nlisten = \"127.0.0.1:$((base + 40))\"/" \
  "$work/replicated.toml" >"$work/primary.toml"
perl -pe "s/^role = \"off\"/role = \"replica\"\nprimary = \"127.0.0.1:$((base + 40))\"/" \
  "$work/replicated.toml" >"$work/replica.toml"

"$binary" --config "$work/standalone.toml" --address "127.0.0.1:$base" --data-dir "$work/standalone" >"$work/standalone.log" 2>&1 &
pids+=("$!")
"$binary" --config "$work/cluster.toml" --address "127.0.0.1:$((base + 10))" --data-dir "$work/cluster" >"$work/cluster.log" 2>&1 &
pids+=("$!")
"$binary" --config "$work/primary.toml" --address "127.0.0.1:$((base + 20))" --data-dir "$work/primary" >"$work/primary.log" 2>&1 &
pids+=("$!")
"$binary" --config "$work/replica.toml" --address "127.0.0.1:$((base + 30))" --data-dir "$work/replica" >"$work/replica.log" 2>&1 &
pids+=("$!")

# The multi-node cluster's maps (see `cluster_topology` in docs/CONFIGURATION.md).
sed 's/^shards = 1/shards = 2/' "$work/standalone.toml" >"$work/multinode.toml"
cat >"$work/topology.toml" <<MAP
version = 2

[[groups]]
name = "a"
address = "127.0.0.1:$((base + 50))"
shards = 2
slots = ["0-8191"]

[[groups]]
name = "b"
address = "127.0.0.1:$((base + 60))"
shards = 2
slots = ["8192-16383"]
MAP
cat >"$work/stale-topology.toml" <<MAP
version = 1

[[groups]]
name = "a"
address = "127.0.0.1:$((base + 50))"
shards = 2
slots = ["0-16383"]

[[groups]]
name = "stale"
address = "127.0.0.1:$((base + 70))"
shards = 1
slots = []
MAP
for node in a b; do
  port=$((base + 50))
  [ "$node" = b ] && port=$((base + 60))
  RUVIO_CLUSTER_TOPOLOGY="$work/topology.toml" RUVIO_CLUSTER_GROUP="$node" \
    "$binary" --config "$work/multinode.toml" --address "127.0.0.1:$port" --data-dir "$work/node-$node" >"$work/node-$node.log" 2>&1 &
  pids+=("$!")
done
RUVIO_CLUSTER_TOPOLOGY="$work/stale-topology.toml" RUVIO_CLUSTER_GROUP=stale \
  "$binary" --config "$work/standalone.toml" --address "127.0.0.1:$((base + 70))" --data-dir "$work/node-stale" >"$work/node-stale.log" 2>&1 &
pids+=("$!")

# The failover pair: replication is set through the environment, as the coordinator tests do.
RUVIO_REPLICATION_ROLE=primary RUVIO_REPLICATION_LISTEN="127.0.0.1:$((base + 82))" \
  "$binary" --config "$work/standalone.toml" --address "127.0.0.1:$((base + 80))" --durability always --data-dir "$work/failover-1" >"$work/failover-1.log" 2>&1 &
pids+=("$!")
RUVIO_REPLICATION_ROLE=replica RUVIO_REPLICATION_LISTEN="127.0.0.1:$((base + 83))" RUVIO_REPLICATION_PRIMARY="127.0.0.1:$((base + 82))" \
  "$binary" --config "$work/standalone.toml" --address "127.0.0.1:$((base + 81))" --durability always --data-dir "$work/failover-2" >"$work/failover-2.log" 2>&1 &
pids+=("$!")

ready() {
  python3 "$root/scripts/_ruvio.py" wait-ready --timeout 30 --port "$@"
}
ready "$base" --pid "${pids[0]}" --data-dir "$work/standalone"
for port in $((base + 10)) $((base + 11)) $((base + 12)); do
  ready "$port" --pid "${pids[1]}" --data-dir "$work/cluster"
done
ready "$((base + 10))" --pid "${pids[1]}" --cluster
for port in $((base + 20)) $((base + 21)); do
  ready "$port" --pid "${pids[2]}" --data-dir "$work/primary"
done
for port in $((base + 30)) $((base + 31)); do
  ready "$port" --pid "${pids[3]}" --data-dir "$work/replica"
done
for port in $((base + 50)) $((base + 51)); do
  ready "$port" --pid "${pids[4]}" --data-dir "$work/node-a"
done
for port in $((base + 60)) $((base + 61)); do
  ready "$port" --pid "${pids[5]}" --data-dir "$work/node-b"
done
ready "$((base + 70))" --pid "${pids[6]}" --data-dir "$work/node-stale"
ready "$((base + 80))" --pid "${pids[7]}" --data-dir "$work/failover-1"
ready "$((base + 81))" --pid "${pids[8]}" --data-dir "$work/failover-2"
python3 "$root/scripts/_ruvio.py" wait-replicas --timeout 30 --port "$((base + 20))" --replicas 1

export RUVIO_ADDR="127.0.0.1:$base"
export RUVIO_TEST_ADDR="127.0.0.1:$base"
export RUVIO_CLUSTER_ADDR="127.0.0.1:$((base + 10))"
export RUVIO_REPLICATED_ADDR="127.0.0.1:$((base + 20))"
export RUVIO_REPLICA_ADDR="127.0.0.1:$((base + 30))"
export RUVIO_MULTINODE_A_ADDR="127.0.0.1:$((base + 50))"
export RUVIO_MULTINODE_B_ADDR="127.0.0.1:$((base + 60))"
export RUVIO_FAILOVER_REPLICATION="127.0.0.1:$((base + 82)),127.0.0.1:$((base + 83))"
export RUVIO_FAILOVER_ADDRS="127.0.0.1:$((base + 80)),127.0.0.1:$((base + 81))"
export RUVIO_MULTINODE_STALE_ADDR="127.0.0.1:$((base + 70))"
# The servers run on throwaway copies of the config file, so live tests may rewrite it.
export RUVIO_TEST_CONFIG_REWRITE=1
"$@"
