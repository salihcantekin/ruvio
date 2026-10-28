#!/usr/bin/env bash
# Runs on the bench VM: fair shard scaling, Ruvio vs Redis, plus the forwarding fix (dev vs PR #233).
#   shard-bench.sh [secs-per-row] [rounds]
# Servers on cores 8-15 (away from core 0, which carries the OS and IRQs), memtier on cores 0-7.
# Needs: ~/ruvio-dev and ~/ruvio-pr (release binaries), ~/redis-server, ~/redis-cli, ~/memtier_benchmark.
set -uo pipefail
SECS=${1:-5}; ROUNDS=${2:-3}
OUT=$HOME/results; mkdir -p "$OUT"; DATA=$HOME/bench-data
BASE=7000
ulimit -n 65536

stop() {
  pkill -x ruvio-dev; pkill -x ruvio-pr; pkill -x redis-server; sleep 1
  rm -rf "$DATA"; mkdir -p "$DATA"
}

start_ruvio() { # bin N
  mkdir -p "$DATA/rv"; cd "$DATA/rv"
  nohup "$HOME/$1" --address 127.0.0.1:$BASE --max-connections-per-ip 10000 --shards "$2" \
    --cpu-set "8-$((7 + $2))" --durability memory > "$OUT/$1.log" 2>&1 &
  cd - >/dev/null; sleep 2
}

start_redis() { # N: one plain process for N=1, an N-master cluster otherwise
  local n=$1 cluster=no nodes=""
  [ "$n" -gt 1 ] && cluster=yes
  for i in $(seq 0 $((n - 1))); do
    mkdir -p "$DATA/rd-$i"
    nohup taskset -c $((8 + i)) "$HOME/redis-server" --port $((BASE + i)) --bind 127.0.0.1 \
      --protected-mode no --dir "$DATA/rd-$i" --appendonly no --save "" \
      --cluster-enabled $cluster --cluster-config-file nodes.conf > "$OUT/redis$i.log" 2>&1 &
    nodes="$nodes 127.0.0.1:$((BASE + i))"
  done
  sleep 1
  if [ "$cluster" = yes ]; then
    yes yes | "$HOME/redis-cli" --cluster create $nodes --cluster-replicas 0 > "$OUT/cluster-create.log" 2>&1
    sleep 2
  fi
}

# One memtier, 8 threads on cores 0-7, 7 connections per thread and node, P16, 32 B values.
load() { # op cluster(yes|no) -> ops/s
  local ratio=1:0 extra=""
  [ "$1" = GET ] && ratio=0:1
  [ "$2" = yes ] && extra="--cluster-mode"
  taskset -c 0-7 "$HOME/memtier_benchmark" -s 127.0.0.1 -p $BASE -t 8 -c 7 --pipeline=16 -d 32 \
    --ratio=$ratio --key-maximum=100000 --key-pattern=R:R --test-time="$SECS" --hide-histogram $extra 2>/dev/null |
    awk -v op="$1" '($1=="Sets" && op=="SET") || ($1=="Gets" && op=="GET") {printf "%.0f\n", $2}'
}

prefill() { # cluster(yes|no): GET rows read existing keys
  taskset -c 0-7 "$HOME/memtier_benchmark" -s 127.0.0.1 -p $BASE -t 8 -c 4 --pipeline=64 -d 32 --ratio=1:0 \
    --key-maximum=100000 --key-pattern=S:S --requests=allkeys --hide-histogram $([ "$1" = yes ] && echo --cluster-mode) \
    >/dev/null 2>&1
}

T=$OUT/shard-scaling.tsv
echo -e "N\tsystem\tclient\tround\tset_ops\tget_ops" > "$T"
for round in $(seq 1 "$ROUNDS"); do
  for n in 1 4 8; do
    # Fair: client-routed on both sides (Redis Cluster vs Ruvio per-shard, memtier --cluster-mode).
    cl=yes; [ "$n" = 1 ] && cl=no
    stop; start_redis "$n"; prefill $cl
    echo -e "$n\tredis\troutes\t$round\t$(load SET $cl)\t$(load GET $cl)" | tee -a "$T"
    for bin in ruvio-dev ruvio-pr; do
      stop; start_ruvio $bin "$n"; prefill $cl
      echo -e "$n\t$bin\troutes\t$round\t$(load SET $cl)\t$(load GET $cl)" | tee -a "$T"
      # Not cluster-aware: every connection on port 7000, the server forwards (N-1)/N.
      if [ "$n" -gt 1 ]; then
        echo -e "$n\t$bin\tone-port\t$round\t$(load SET no)\t$(load GET no)" | tee -a "$T"
      fi
    done
  done
done

# One shard, the rows that read below Redis on 2026-10-26 (redis-benchmark, c50 P16).
C=$OUT/one-shard.tsv
echo -e "system\tround\tcommand\trps" > "$C"
for round in $(seq 1 "$ROUNDS"); do
  for sys in redis ruvio-dev; do
    stop
    if [ $sys = redis ]; then start_redis 1; else start_ruvio ruvio-dev 1; fi
    "$HOME/redis-cli" -p $BASE set fillkey value >/dev/null
    for cmd in "SET k:__rand_int__ v" "GET k:__rand_int__" "INCR c:__rand_int__" "LPUSH l v" "LPOP l" "GET fillkey"; do
      r=$(taskset -c 0-7 "$HOME/redis-benchmark" -p $BASE --threads 4 -c 50 -P 16 -r 100000 -n 3000000 --csv $cmd 2>/dev/null |
        tail -1 | tr -d '"' | cut -d, -f2)
      echo -e "$sys\t$round\t$cmd\t$r" | tee -a "$C"
    done
  done
done
stop
