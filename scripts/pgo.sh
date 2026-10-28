#!/usr/bin/env bash
# Profile-guided optimization for the ruvio server binary.
#
#   scripts/pgo.sh [test-count-scale]
#
# Steps: instrumented build -> train the server under the `memory` and `everysec`
# profiles with redis-benchmark -> merge the profiles -> optimized build.
#
# Needs llvm-profdata from the llvm-tools rustup component:
#   rustup component add llvm-tools-preview
#
# Not part of the release build. Measured against the plain release build on the
# 60-command comparison (scripts/redis_compare_features.py, pipeline 16): about
# 4% slower on a 2 vCPU x86_64 server and equal on an Apple silicon Mac, because most
# of those commands are not in the training mix.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

host="${RUVIO_HOST:-127.0.0.1}"
port="${RUVIO_PORT:-6930}"
scale="${1:-1}"
bench_bin="${REDIS_BENCHMARK:-redis-benchmark}"
work="$(mktemp -d)"
profiles="$work/profiles"
server_pid=""
trap '[ -n "$server_pid" ] && kill -INT "$server_pid" 2>/dev/null; rm -rf "$work"' EXIT

sysroot="$(rustc --print sysroot)"
profdata="$(find "$sysroot" -name llvm-profdata -type f | head -1)"
if [ -z "$profdata" ]; then
	echo "llvm-profdata not found. Install: rustup component add llvm-tools-preview" >&2
	exit 1
fi

n() { echo $(($1 * scale)); }

# 1. Instrumented build.
RUSTFLAGS="-Cprofile-generate=$profiles" CARGO_TARGET_DIR="$work/instrumented" \
	cargo build --release --bin ruvio
instrumented="$work/instrumented/release/ruvio"

bench() { "$bench_bin" -h "$host" -p "$port" -c 50 -d 64 -q "$@"; }

for durability in memory everysec; do
	mkdir -p "$work/data-$durability"
	# RUVIO_CONFIG keeps the server from writing ruvio.toml into the checkout.
	RUVIO_CONFIG="$work/ruvio-$durability.toml" \
		LLVM_PROFILE_FILE="$profiles/ruvio-$durability-%p.profraw" "$instrumented" \
		--address "$host:$port" --shards 1 --durability "$durability" \
		--data-dir "$work/data-$durability" &
	server_pid=$!
	sleep 2

	# 2. Preload, then the write and read mix at P1 and P16.
	bench -n "$(n 1000000)" -P 16 -r 1000000 -t set
	bench -n "$(n 500000)" -P 16 -r 100000 HSET myhash field:__rand_int__ v
	for pipeline in 1 16; do
		bench -n "$(n 300000)" -P "$pipeline" -r 1000000 \
			-t set,get,incr,lpush,rpush,lpop,rpop,sadd,spop,hset,zadd,zpopmin,mset,xadd,lrange_100,lrange_300,lrange_600
		bench -n "$(n 200000)" -P "$pipeline" -r 1000000 \
			EVAL "return ruvio.call('set',KEYS[1],ARGV[1])" 1 ev:__rand_int__ v
		bench -n "$(n 200000)" -P "$pipeline" -r 1000000 \
			EVAL "return ruvio.call('get',KEYS[1])" 1 key:__rand_int__
	done
	# Reads get the largest share: without it the optimized build was slower on GET.
	bench -n "$(n 4000000)" -P 16 -r 1000000 -t get
	bench -n "$(n 600000)" -P 1 -r 1000000 -t get
	for pipeline in 1 16; do
		bench -n "$(n 600000)" -P "$pipeline" -r 1000000 \
			MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__
		bench -n "$(n 600000)" -P "$pipeline" -r 100000 HGET myhash field:__rand_int__
		bench -n "$(n 600000)" -P "$pipeline" -r 1000000 EXISTS key:__rand_int__
	done

	# 3. SIGINT exits cleanly and writes the profile; SHUTDOWN does not.
	kill -INT "$server_pid"
	wait "$server_pid" 2>/dev/null || true
	server_pid=""
done

# 4. Merge the profiles and build the optimized binary.
"$profdata" merge -o "$work/ruvio.profdata" "$profiles"/*.profraw
RUSTFLAGS="-Cprofile-use=$work/ruvio.profdata" cargo build --locked --release --bin ruvio
echo "PGO binary: target/release/ruvio"
