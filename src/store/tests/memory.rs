use super::*;

/// Native runs use thousands of entries to show the cost curve. Miri
/// interprets every step, so it keeps the same 4× ratio at a small size.
fn benchmark_counts() -> (usize, usize) {
    if cfg!(miri) {
        (32, 128)
    } else {
        (2_000, 8_000)
    }
}

#[test]
fn tracks_per_mutation_memory_deltas_and_pool_transfers() {
    let mut shard = KvShard::new();
    let empty = shard.memory_usage();

    shard.set_bytes(b"key", &[7; 4_096]);
    let inserted = shard.memory_usage();
    let insert_delta = inserted.delta_from(empty);

    // Keys are charged with their key-log record: header and slot id.
    assert_eq!(insert_delta.entries, 1);
    assert_eq!(insert_delta.key_bytes, 3 + 8);
    assert!(insert_delta.value_bytes >= 4_096);
    assert!(insert_delta.charged_bytes() > 4_096);

    assert!(shard.delete(b"key"));
    let deleted = shard.memory_usage();
    let delete_delta = deleted.delta_from(inserted);

    assert_eq!(delete_delta.entries, -1);
    assert_eq!(delete_delta.key_bytes, -(3 + 8));
    assert!(delete_delta.value_bytes <= -4_096);
    assert!(delete_delta.pooled_bytes >= 4_096);

    shard.set_bytes(b"next", &[8; 4_096]);
    let reused = shard.memory_usage();
    let reuse_delta = reused.delta_from(deleted);

    assert_eq!(reuse_delta.entries, 1);
    assert_eq!(reuse_delta.key_bytes, 4 + 8);
    assert!(reuse_delta.value_bytes >= 4_096);
    assert!(reuse_delta.pooled_bytes <= -4_096);
}

#[test]
fn tracks_collection_payload_and_capacity_charges() {
    let mut shard = KvShard::new();
    let before = shard.memory_usage();

    shard
        .list_push_front(b"jobs", &[b"first".to_vec(), b"second".to_vec()])
        .unwrap();
    let list_usage = shard.memory_usage();

    assert_eq!(list_usage.entries, 1);
    // The key and its key-log record: header and slab slot id.
    assert_eq!(list_usage.key_bytes, 4 + 8);
    assert!(list_usage.value_bytes >= b"firstsecond".len());
    // A small list lives in an index slab record: its framing is part of `value_bytes`.
    assert_eq!(list_usage.collection_bytes, 0);

    shard
        .hash_set(b"user", &[(b"name".as_slice(), b"ada".as_slice())])
        .unwrap();
    let hash_usage = shard.memory_usage();

    assert!(hash_usage.value_bytes >= b"nameada".len());
    assert_eq!(hash_usage.collection_bytes, list_usage.collection_bytes);

    shard.delete(b"jobs");
    shard.delete(b"user");
    let after = shard.memory_usage();
    let total_delta = after.delta_from(before);

    assert_eq!(total_delta.entries, 0);
    assert_eq!(total_delta.key_bytes, 0);
    assert_eq!(total_delta.value_bytes, 0);
    assert_eq!(total_delta.collection_bytes, 0);
}

#[test]
fn deadline_index_charges_owned_key_bytes_until_addressed() {
    let mut deadlines = DeadlineDb::default();
    let key = b"unaddressed";
    assert_eq!(
        deadlines.set(key, None, UnixMillis::new(100), |_| None),
        None
    );
    let id = deadlines.id_of(key, |_| None).unwrap();
    let with_owned = deadlines.charged_bytes();
    deadlines.note_address(id, 1);
    assert_eq!(deadlines.charged_bytes(), with_owned - key.len());
    assert!(deadlines.remove(key, |_| Some(&key[..])));
    assert_eq!(deadlines.charged_bytes(), 0);
}

#[test]
fn deadline_index_charges_track_capacity_renewal_and_release() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let baseline = shard.memory_usage().index_bytes;
    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        shard.set_bytes(key.as_bytes(), b"v");
    }
    let untimed = shard.memory_usage().index_bytes;
    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }
    let timed = shard.memory_usage().index_bytes;
    assert!(timed > untimed);
    let capacity = shard.deadlines[0].charged_bytes();
    assert!(capacity >= 1_000 * size_of::<DeadlineRecord>());
    assert_eq!(timed, untimed + capacity);

    for id in 0..1_000 {
        let key = format!("key:{id:04}");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(20_000)));
    }
    assert_eq!(shard.memory_usage().index_bytes, timed);
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
    assert!(shard.memory_admission_would_fail(1));

    clock.set(20_000);
    while shard.expire_cycle(100) > 0 {}
    assert_eq!(shard.memory_usage().index_bytes, baseline);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn deadline_charge_cache_tracks_multiple_databases_and_rewrites() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));
    let check = |shard: &KvShard<TestClock>| {
        let total: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();
        assert_eq!(shard.deadline_index_bytes, total);
        assert_eq!(
            shard.memory_usage().index_bytes,
            shard.entries.charged_bytes() + shard.parked_index_bytes + total
        );
    };
    shard.enter(255);
    shard.set_bytes(b"timed", b"value");
    assert!(shard.expire_at(b"timed", UnixMillis::new(10_000)));
    check(&shard);
    shard.enter(1);
    shard.set_bytes(b"other", b"value");
    assert!(shard.expire_at(b"other", UnixMillis::new(20_000)));
    check(&shard);
    shard.enter(0);
    for id in 0..100 {
        shard.set_bytes(format!("plain:{id}").as_bytes(), b"value");
    }
    check(&shard);
    shard.swap_databases(1, 255);
    check(&shard);
    let detached = shard.take_database(1);
    check(&shard);
    shard.restore_database(detached);
    check(&shard);
    shard.enter(1);
    assert!(shard.persist(b"timed"));
    check(&shard);
    shard.flush_all();
    check(&shard);
}

#[test]
fn detached_databases_keep_their_deadline_index_and_flushes_clear_every_charge() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for id in 0..500 {
        let key = format!("timed:{id}");

        shard.set_bytes(key.as_bytes(), b"value");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(5_000 + id)));
    }

    shard.set_bytes(b"plain", b"value");
    let usage = shard.memory_usage();

    // The only database of the shard is detached on the running totals and comes back
    // with its deadlines still pointing at its keys.
    let detached = shard.take_database(0);

    assert_eq!(shard.memory_usage().key_bytes, 0);
    assert_eq!(shard.memory_usage().value_bytes, 0);
    shard.restore_database(detached);
    assert_eq!(shard.memory_usage().key_bytes, usage.key_bytes);
    assert_eq!(shard.memory_usage().value_bytes, usage.value_bytes);
    assert_eq!(shard.deadline(0, b"timed:7"), Some(UnixMillis::new(5_007)));
    clock.set(5_250);
    assert_eq!(shard.get(b"timed:200"), None);
    assert_eq!(shard.get(b"timed:300"), Some(&b"value"[..]));

    // A second database: the walk still charges the one taken out alone.
    shard.enter(2);
    shard.set_bytes(b"other", b"value");
    assert!(shard.expire_at(b"other", UnixMillis::new(9_000)));
    shard.enter(0);
    let before = shard.memory_usage().key_bytes;
    let detached = shard.take_database(2);

    // The key and its record: a header and a value-log span.
    assert_eq!(shard.memory_usage().key_bytes, before - b"other".len() - 10);
    shard.restore_database(detached);
    assert_eq!(shard.memory_usage().key_bytes, before);

    shard.flush_all();
    let after = shard.memory_usage();

    assert_eq!(
        (after.key_bytes, after.value_bytes, after.collection_bytes),
        (0, 0, 0)
    );
    assert_eq!(shard.expiration.keys_with_ttl, 0);
    shard.enter(2);
    assert_eq!(shard.deadline(2, b"other"), None);
}

#[test]
fn storage_charges_match_entries_across_expiry_databases_and_snapshot() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(MemoryLimits {
        hash_max_compact_entries: 1,
        ..MemoryLimits::default()
    });
    let check = |shard: &KvShard<TestClock>| {
        let mut entries = 0;
        let mut keys = 0;
        let mut values = 0;
        let mut collections = 0;
        let mut index_bytes = 0;
        for (_, map) in shard.database_maps() {
            entries += map.len();
            index_bytes += map.charged_bytes();
            for (key, hit) in map.entries() {
                let charge = charge_of(key, hit);
                keys += charge.key_bytes;
                values += charge.value_bytes;
                collections += charge.collection_bytes;
            }
        }
        let usage = shard.memory_usage();
        assert_eq!(usage.entries, entries);
        assert_eq!(usage.key_bytes, keys);
        assert_eq!(usage.value_bytes, values);
        assert_eq!(usage.collection_bytes, collections);
        let deadline_bytes: usize = shard.deadlines.iter().map(DeadlineDb::charged_bytes).sum();
        assert_eq!(shard.deadline_index_bytes, deadline_bytes);
        assert_eq!(usage.index_bytes, index_bytes + deadline_bytes);
        assert_eq!(usage.pooled_bytes, shard.byte_pool.cached_bytes);
    };
    check(&shard);
    shard.set_bytes(b"reused", &[3; 4_096]);
    shard.set_bytes(b"reused", &[7; 8_192]);
    shard.set_bytes(b"reused", b"tiny");
    check(&shard);
    shard.set_bytes(b"long", &[1; 100]);
    shard.set_range(b"long", 150, b"tail").unwrap();
    check(&shard);
    shard.enter(1);
    shard
        .hash_set(b"fields", &[(b"a", b"v"), (b"b", b"v")])
        .unwrap();
    assert_eq!(shard.hash_is_compact(b"fields"), Ok(Some(false)));
    shard.list_push_back(b"jobs", &[b"item"]).unwrap();
    assert!(shard.expire_at(b"jobs", UnixMillis::new(2_000)));
    check(&shard);
    shard.begin_snapshot(16_384).unwrap();
    shard.capture_snapshot_key(b"fields");
    let db1_journal = shard.memory_usage().snapshot_journal_bytes;
    assert!(db1_journal > 0);
    check(&shard);
    shard.enter(0);
    shard.capture_snapshot_key(b"reused");
    assert!(shard.memory_usage().snapshot_journal_bytes > db1_journal);
    check(&shard);
    shard.delete(b"reused");
    check(&shard);
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(8), 1);
    check(&shard);
    let snapshot = shard.finish_snapshot();
    assert_eq!(shard.memory_usage().snapshot_journal_bytes, 0);
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.database == 1 && entry.key == b"fields")
    );
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.database == 0 && entry.key == b"reused")
    );
    check(&shard);
    shard.apply_eviction(1, b"fields");
    assert_eq!(shard.memory_stats().evicted_keys, 1);
    check(&shard);
    shard.flush_all();
    check(&shard);
    let usage = shard.memory_usage();
    assert_eq!(usage.entries, 0);
    assert_eq!(
        usage.key_bytes + usage.value_bytes + usage.collection_bytes + usage.index_bytes,
        0
    );
}

#[test]
fn reclaiming_logs_keeps_a_parked_database_charge_exact() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    shard.enter(1);
    for index in 0..4_000 {
        let key = format!("key:{index:05}");

        shard.set_bytes(key.as_bytes(), b"v");
    }
    // Sparse key-log pages, and recency cells on the keys that stay.
    for index in 0..4_000 {
        let key = format!("key:{index:05}");

        if index % 8 == 0 {
            shard.get(key.as_bytes());
        } else {
            assert!(shard.delete(key.as_bytes()));
        }
    }
    shard.enter(0);
    shard.set_bytes(b"active", b"v");

    let mut done = false;

    for _ in 0..1_000 {
        let migrated = shard.migrate_indexes(std::time::Duration::from_millis(5));

        done = migrated && shard.reclaim_logs(std::time::Duration::from_millis(5));
        if done {
            break;
        }
    }
    assert!(done, "reclaim did not finish");
    charges_match_a_recount(&shard);
    shard.enter(1);
    charges_match_a_recount(&shard);
}

#[test]
fn switching_the_eviction_policy_keeps_parked_charges_exact() {
    let mut shard = KvShard::with_clock(TestClock::new(1_000));

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    shard.enter(1);
    for index in 0..100 {
        let key = format!("key:{index}");

        shard.set_bytes(key.as_bytes(), b"v");
        shard.get(key.as_bytes());
    }
    shard.enter(0);
    shard.set_bytes(b"active", b"v");

    for _ in 0..3 {
        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLfu);
        charges_match_a_recount(&shard);
        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
        charges_match_a_recount(&shard);
    }

    shard.enter(1);
    charges_match_a_recount(&shard);
    shard.enter(0);
    charges_match_a_recount(&shard);
}

#[test]
fn string_charges_count_records_and_allocator_blocks() {
    let mut shard = KvShard::new();
    let key = b"0123456789abcdef";

    shard.set_bytes(key, &[1; 64]);
    assert_eq!(shard.key_memory_usage(key), Some(16 + 10 + 64));

    shard.set_bytes(key, &[1; 100]);
    assert_eq!(shard.key_memory_usage(key), Some(16 + 8 + 112 + 24));

    // A shorter value reuses the large buffer, which stays charged.
    shard.set_bytes(key, &[2; 100_000]);
    let large = shard.key_memory_usage(key).unwrap();

    assert_eq!(large, 16 + 8 + alloc_size(100_000) + 24);
    shard.set_bytes(key, &[3; 100]);
    assert_eq!(shard.key_memory_usage(key), Some(large));
    assert!(shard.delete(key));

    shard.set_bytes(key, b"12345");
    assert_eq!(shard.key_memory_usage(key), Some(16 + 10 + 5));

    assert!(shard.delete(key));
    shard.increment(key, 5).unwrap();
    assert_eq!(shard.key_memory_usage(key), Some(16 + 12));
}

/// Every write that skips a lookup (a new string charged from the value in hand, a
/// counter written with the read's hash) must leave the shard's totals equal to a
/// recount of what the index holds, on both index kinds.
fn new_key_writes_charge_like_a_recount<I: StorageIndex>(index: I) {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_index_and_clock(index, clock.clone());
    let check = |shard: &KvShard<TestClock, I>| {
        let (mut keys, mut values, mut collections) = (0, 0, 0);
        let mut entries = 0;

        for (_, map) in shard.database_maps() {
            entries += map.len();

            for (key, hit) in map.entries() {
                let charge = charge_of(key, hit);

                keys += charge.key_bytes;
                values += charge.value_bytes;
                collections += charge.collection_bytes;
            }
        }

        let usage = shard.memory_usage();

        assert_eq!(usage.entries, entries);
        assert_eq!(usage.key_bytes, keys);
        assert_eq!(usage.value_bytes, values);
        assert_eq!(usage.collection_bytes, collections);
    };

    // New short, empty and long strings.
    shard.set_bytes(b"short", b"value");
    shard.set_bytes(b"empty", b"");
    shard.set_bytes(b"long", &[4; 5_000]);
    check(&shard);
    assert_eq!(shard.get(b"short"), Some(&b"value"[..]));
    assert_eq!(shard.get(b"long").map(<[u8]>::len), Some(5_000));

    // Overwrites in place, larger, and across the short/long boundary.
    shard.set_bytes(b"short", b"v");
    shard.set_bytes(b"short", &[5; 40]);
    shard.set_bytes(b"short", &[6; 5_000]);
    shard.set_bytes(b"long", b"tiny");
    check(&shard);

    // Many new keys in a pipeline, including enough to grow the table.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..600)
        .map(|n| (format!("m:{n}").into_bytes(), vec![b'x'; n % 50]))
        .collect();

    shard.set_many_bytes(&pairs);
    check(&shard);
    assert_eq!(shard.get(b"m:49"), Some(&[b'x'; 49][..]));

    // A string over another type, and over a key whose deadline has passed.
    shard.list_push_back(b"list", &[b"item"]).unwrap();
    shard.set_bytes(b"list", b"now a string");
    assert!(shard.expire_at(b"short", UnixMillis::new(1_500)));
    clock.set(2_000);
    shard.set_bytes(b"short", b"after expiry");
    check(&shard);
    assert_eq!(shard.get(b"short"), Some(&b"after expiry"[..]));
    assert_eq!(shard.get(b"list"), Some(&b"now a string"[..]));

    // Counters: new, in place, from a string, and a wrong type left untouched.
    assert_eq!(shard.increment(b"counter", 5), Ok(5));
    assert_eq!(shard.increment(b"counter", -7), Ok(-2));
    shard.set_bytes(b"text", b"41");
    assert_eq!(shard.increment(b"text", 1), Ok(42));
    shard.list_push_back(b"jobs", &[b"item"]).unwrap();
    assert!(shard.increment(b"jobs", 1).is_err());
    check(&shard);
    assert_eq!(shard.copied_value(b"counter"), Some(b"-2".to_vec()));
    assert_eq!(shard.copied_value(b"text"), Some(b"42".to_vec()));
}

#[test]
fn new_key_writes_charge_like_a_recount_on_the_hash_index() {
    new_key_writes_charge_like_a_recount(crate::index::AHashIndex::default());
}

#[test]
fn new_key_writes_charge_like_a_recount_on_the_tree_index() {
    new_key_writes_charge_like_a_recount(crate::index::BTreeIndex::default());
}

#[test]
fn repeated_list_pushes_charge_payload_without_rescanning() {
    let value = vec![b'x'; 64];
    let push = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for _ in 0..count {
            shard
                .list_push_front(b"jobs", std::slice::from_ref(&value))
                .unwrap();
        }

        let elapsed = started.elapsed();
        assert_eq!(shard.memory_usage().value_bytes, count * value.len());

        for _ in 0..count {
            assert_eq!(shard.list_pop_front(b"jobs").unwrap().unwrap().len(), 64);
        }

        assert_eq!(shard.memory_usage().entries, 0);
        assert_eq!(shard.memory_usage().value_bytes, 0);
        assert_eq!(shard.memory_usage().collection_bytes, 0);
        elapsed
    };

    // Four times the elements. A full recount grows about sixteenfold;
    // an incremental charge stays near fourfold.
    let (small_count, large_count) = benchmark_counts();
    let small = push(small_count);
    let large = push(large_count);

    assert!(
        large < small.saturating_mul(8),
        "list push scaled too fast: {small:?} -> {large:?}"
    );
}

#[test]
fn repeated_set_and_sorted_set_inserts_do_not_rescan_members() {
    let member = |index: usize| format!("member-{index:04}").into_bytes();
    let time_set = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for index in 0..count {
            assert_eq!(shard.set_add(b"tags", &[member(index)]).unwrap(), 1);
        }

        let elapsed = started.elapsed();
        let payload: usize = (0..count).map(|index| member(index).len()).sum();
        assert_eq!(shard.memory_usage().value_bytes, payload);
        elapsed
    };

    let time_sorted = |count: usize| {
        let mut shard = KvShard::new();
        let started = std::time::Instant::now();

        for index in 0..count {
            assert!(
                shard
                    .sorted_set_add(b"board", index as f64, &member(index))
                    .unwrap()
            );
        }

        let elapsed = started.elapsed();
        let payload: usize = (0..count).map(|index| member(index).len()).sum();
        assert_eq!(shard.memory_usage().value_bytes, payload);
        elapsed
    };

    let (small_count, large_count) = benchmark_counts();
    let small_set = time_set(small_count);
    let large_set = time_set(large_count);

    assert!(
        large_set < small_set.saturating_mul(8),
        "set insert scaled too fast: {small_set:?} -> {large_set:?}"
    );
    let small_sorted = time_sorted(small_count);
    let large_sorted = time_sorted(large_count);

    assert!(
        large_sorted < small_sorted.saturating_mul(8),
        "sorted set insert scaled too fast: {small_sorted:?} -> {large_sorted:?}"
    );
}

#[test]
fn physical_memory_accounting_retains_passively_expired_entries() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");
    shard.expire_at(b"key", UnixMillis::new(20));
    let live_usage = shard.memory_usage();

    clock.set(20);
    assert_eq!(shard.get(b"key"), None);
    assert_eq!(shard.memory_usage(), live_usage);

    shard.set_bytes(b"key", b"new");
    assert_eq!(shard.memory_usage().entries, 1);
    assert_eq!(shard.get(b"key"), Some(&b"new"[..]));
}
