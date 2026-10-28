use super::*;
use crate::time::TestClock;
use std::collections::BTreeMap;

fn charges_match_a_recount(shard: &KvShard<TestClock>) {
    let index: usize = std::iter::once(&shard.entries)
        .chain(shard.databases.iter().flatten())
        .map(|map| map.charged_bytes())
        .sum();
    let deadlines: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();

    assert_eq!(shard.memory_usage.index_bytes, index + deadlines);
}

mod eviction;
mod expiry;
mod fence;
mod field_ttl;
mod hashes;
mod json;
mod keyspace;
mod lists;
mod memory;
mod sets;
mod shrink;
mod slot_watch;
mod snapshot;
mod sorted_sets;
mod strings;
mod value_log;
mod watch_and_notices;
