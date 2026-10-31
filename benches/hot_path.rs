//! Instruction counts for one command on a warm shard (Linux, valgrind).
//!
//! Each benchmark gets a shard that holds only the key it measures. The key table's hash seed is
//! random per process, and with more keys in the table a seed that gives two keys the same probe
//! tag adds a key comparison to the lookup (about 100 instructions), so the count would change
//! from run to run. The shard is boxed so the harness moves a pointer, not the whole struct, and
//! each benchmark returns it and the output buffer so that dropping them is not counted: neither
//! is a command's cost. The list is warmed with a push and a pop so the size class the measured
//! push lands in already has a page; the first page of a class costs one zeroed 4 KiB allocation,
//! shared by many keys in a real shard.
//!
//! Run: `cargo bench --bench hot_path` (needs `valgrind` and
//! `cargo install iai-callgrind-runner --version 0.16.1`). CI compares the
//! totals with `benchmark/baselines/instructions.json` through
//! `scripts/check_instruction_baseline.py`.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use ruvio::{KvShard, engine::execute_hot_into, protocol::HotCommand};

const KEY: &[u8] = b"bench:key";
const COUNTER: &[u8] = b"bench:counter";
const HASH: &[u8] = b"bench:hash";
const ZSET: &[u8] = b"bench:zset";
const LIST: &[u8] = b"bench:list";
const VALUE: &[u8] = b"0123456789abcdef";

type Shard = (Box<KvShard>, Vec<u8>);

fn empty() -> Shard {
    (Box::new(KvShard::new()), Vec::with_capacity(4096))
}

/// Runs a `GET` of `key` once so the measured call sees no first-use setup.
fn touched((mut shard, mut output): Shard, key: &[u8]) -> Shard {
    execute_hot_into(&mut shard, HotCommand::Get(key), &mut output).unwrap();
    output.clear();
    (shard, output)
}

fn string() -> Shard {
    let (mut shard, output) = empty();

    shard.set_bytes(KEY, VALUE);
    touched((shard, output), KEY)
}

fn counter() -> Shard {
    let (mut shard, output) = empty();

    shard.increment(COUNTER, 1).unwrap();
    touched((shard, output), COUNTER)
}

fn hash() -> Shard {
    let (mut shard, output) = empty();

    shard.hash_set(HASH, &[(b"f".as_slice(), VALUE)]).unwrap();
    (shard, output)
}

fn zset() -> Shard {
    let (mut shard, output) = empty();

    shard.sorted_set_add(ZSET, 1.0, b"m").unwrap();
    (shard, output)
}

fn list() -> Shard {
    let (mut shard, output) = empty();

    shard.list_push_front(LIST, &[VALUE]).unwrap();
    shard.list_push_front(LIST, &[VALUE]).unwrap();
    shard.list_pop_front(LIST).unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::get(string())]
fn get((mut shard, mut output): Shard) -> Shard {
    execute_hot_into(&mut shard, black_box(HotCommand::Get(KEY)), &mut output).unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::set(string())]
fn set((mut shard, mut output): Shard) -> Shard {
    execute_hot_into(
        &mut shard,
        black_box(HotCommand::Set(KEY, VALUE, None)),
        &mut output,
    )
    .unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::incr(counter())]
fn incr((mut shard, mut output): Shard) -> Shard {
    execute_hot_into(
        &mut shard,
        black_box(HotCommand::Increment(COUNTER, 1)),
        &mut output,
    )
    .unwrap();
    (shard, output)
}

#[library_benchmark]
#[bench::hset(hash())]
fn hset((mut shard, output): Shard) -> (Shard, usize) {
    let added = shard
        .hash_set(HASH, &[(black_box(b"f".as_slice()), VALUE)])
        .unwrap();
    ((shard, output), added)
}

#[library_benchmark]
#[bench::zadd(zset())]
fn zadd((mut shard, output): Shard) -> (Shard, bool) {
    let added = shard.sorted_set_add(ZSET, black_box(2.0), b"m").is_ok();
    ((shard, output), added)
}

#[library_benchmark]
#[bench::lpush(list())]
fn lpush((mut shard, output): Shard) -> (Shard, usize) {
    let length = shard.list_push_front(LIST, &[black_box(VALUE)]).unwrap();
    ((shard, output), length)
}

library_benchmark_group!(name = hot_path; benchmarks = get, set, incr, hset, zadd, lpush);
main!(library_benchmark_groups = hot_path);
