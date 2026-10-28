use super::*;

#[test]
fn expiration_uses_persistable_absolute_time() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"session", b"value");

    assert!(shard.expire_at(b"session", UnixMillis::new(2_000)));
    assert_eq!(shard.expires_at(b"session"), Some(UnixMillis::new(2_000)));
    clock.set(1_999);
    assert_eq!(shard.get(b"session"), Some(&b"value"[..]));
    assert_eq!(shard.len(), 1);

    clock.set(2_000);
    assert_eq!(shard.get(b"session"), None);
    assert_eq!(shard.expires_at(b"session"), None);
    assert_eq!(shard.len(), 1);
    assert_eq!(shard.expire_due(), 1);
    assert!(shard.is_empty());
}

#[test]
fn expired_values_behave_as_missing_for_future_mutations() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.list_push_front(b"key", &[b"item".to_vec()]).unwrap();
    assert!(shard.expire_at(b"key", UnixMillis::new(20)));

    clock.set(20);
    assert_eq!(shard.increment(b"key", 1), Ok(1));
    assert_eq!(shard.copied_value(b"key").as_deref(), Some(&b"1"[..]));
    assert_eq!(shard.expires_at(b"key"), None);
}

#[test]
fn clock_is_not_read_for_values_without_expiration() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");

    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert_eq!(shard.len(), 1);
    assert_eq!(clock.reads(), 0);

    assert!(shard.expire_at(b"key", UnixMillis::new(2_000)));
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert!(clock.reads() > 0);
}

#[test]
fn persist_clears_an_existing_deadline() {
    let clock = TestClock::new(100);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"key", b"value");
    assert!(shard.expire_at(b"key", UnixMillis::new(200)));

    assert!(shard.persist(b"key"));
    clock.set(500);
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
    assert!(!shard.persist(b"key"));
}

#[test]
fn ttl_reports_missing_persistent_and_remaining_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    assert_eq!(shard.pttl(b"missing"), -2);
    shard.set_bytes(b"key", b"value");
    assert_eq!(shard.ttl_seconds(b"key"), -1);
    assert!(shard.expire_at(b"key", UnixMillis::new(3_500)));
    assert_eq!(shard.pttl(b"key"), 2_500);
    assert_eq!(
        shard.ttl_seconds(b"key"),
        3,
        "Redis rounds to the nearest second"
    );
    clock.set(3_500);
    assert_eq!(shard.pttl(b"key"), -2);
}

#[test]
fn bounded_active_expiry_removes_a_budgeted_number_of_stale_keys() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    shard.set_bytes(b"c", b"3");
    shard.expire_at(b"a", UnixMillis::new(20));
    shard.expire_at(b"b", UnixMillis::new(20));
    shard.expire_at(b"c", UnixMillis::new(40));
    clock.set(20);
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.memory_usage().entries, 3);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 3);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.memory_usage().entries, 2);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);
    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(shard.memory_usage().entries, 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 2);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1);
    assert_eq!(shard.get(b"c"), Some(&b"3"[..]));
}

#[test]
fn deadline_heap_matches_reference_across_replacements_and_removals() {
    let mut deadlines = DeadlineDb::default();
    let mut reference = BTreeMap::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;

    for _ in 0..5_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = format!("key-{}", seed % 256);
        if seed & 3 == 0 {
            assert_eq!(
                deadlines.remove(key.as_bytes(), |_| None),
                reference.remove(key.as_bytes()).is_some()
            );
        } else {
            let at = UnixMillis::new(1_000 + (seed >> 8) % 4_000);
            let previous = deadlines.set(key.as_bytes(), None, at, |_| None);
            assert_eq!(previous, reference.insert(key.into_bytes(), at));
        }
        assert_eq!(deadlines.len, reference.len());
        assert_eq!(deadlines.heap.len(), reference.len());
        // `min_at` is a lower bound until `first_due` re-places moved deadlines.
        assert!(deadlines.min_at() <= reference.values().min().copied());
        for (position, &id) in deadlines.heap.iter().enumerate() {
            assert_eq!(
                deadlines.records[id as usize].as_ref().unwrap().position() as usize,
                position
            );
        }
    }

    for at in 1_000..5_000 {
        while let Some(key) = deadlines.first_due(UnixMillis::new(at), |_| unreachable!()) {
            assert_eq!(reference.remove(key.as_ref()), Some(UnixMillis::new(at)));
            assert!(deadlines.remove(&key, |_| None));
        }
    }
    assert!(deadlines.heap.is_empty());
    assert!(reference.is_empty());
}

#[test]
fn active_expiry_prioritizes_due_keys_behind_future_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    for index in 0..128 {
        let key = format!("future-{index}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }
    shard.set_bytes(b"due", b"v");
    assert!(shard.expire_at(b"due", UnixMillis::new(2_000)));
    clock.set(2_000);

    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 128);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    assert_eq!(shard.get(b"due"), None);
    assert_eq!(shard.get(b"future-0"), Some(&b"v"[..]));
}

#[test]
fn active_expiry_reorders_extended_and_shortened_deadlines() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    for key in [b"extended".as_slice(), b"shortened", b"other"] {
        shard.set_bytes(key, b"v");
        assert!(shard.expire_at(key, UnixMillis::new(3_000)));
    }
    assert!(shard.expire_at(b"extended", UnixMillis::new(10_000)));
    assert!(shard.expire_at(b"shortened", UnixMillis::new(2_000)));
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.get(b"shortened"), None);
    assert_eq!(shard.get(b"other"), Some(&b"v"[..]));
    assert_eq!(shard.get(b"extended"), Some(&b"v"[..]));
    clock.set(3_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1);
    assert_eq!(shard.get(b"other"), None);
    clock.set(10_000);
    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn active_expiry_reaches_later_databases_when_an_earlier_one_fills_the_budget() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for index in 0..8 {
        let key = format!("future-{index}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(10_000)));
    }

    shard.enter(1);
    shard.set_bytes(b"due", b"v");
    assert!(shard.expire_at(b"due", UnixMillis::new(2_000)));
    shard.enter(0);
    clock.set(2_000);

    let removed = (0..2).map(|_| shard.expire_cycle(1)).sum::<usize>();
    assert_eq!(removed, 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    shard.enter(1);
    assert_eq!(shard.get(b"due"), None);
}

#[test]
fn active_expiry_respects_replaced_and_persisted_deadlines_across_databases() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_bytes(b"past", b"v");
    assert!(shard.expire_at(b"past", UnixMillis::new(999)));
    shard.set_bytes(b"kept", b"v");
    assert!(shard.expire_at(b"kept", UnixMillis::new(2_000)));
    assert!(shard.persist(b"kept"));

    shard.enter(1);
    shard.set_bytes(b"later", b"v");
    assert!(shard.expire_at(b"later", UnixMillis::new(2_000)));
    assert!(shard.expire_at(b"later", UnixMillis::new(3_000)));
    shard.set_bytes(b"equal", b"v");
    assert!(shard.expire_at(b"equal", UnixMillis::new(2_000)));
    shard.enter(0);

    assert_eq!(shard.expire_cycle(1), 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 1);
    clock.set(1_999);
    assert_eq!(shard.expire_cycle(8), 0);
    clock.set(2_000);
    assert_eq!(shard.expire_cycle(8), 1);
    shard.enter(1);
    assert_eq!(shard.get(b"equal"), None);
    assert_eq!(shard.get(b"later"), Some(&b"v"[..]));
    shard.enter(0);
    assert_eq!(shard.get(b"kept"), Some(&b"v"[..]));
    clock.set(3_000);
    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(shard.expiration_stats().expired_keys_active, 3);
    assert_eq!(shard.expiration_stats().expired_keys_passive, 0);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
}

#[test]
fn expired_values_are_hidden_from_reads_and_mutations_before_active_cleanup() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.enter(1);
    shard.set_bytes(b"string", b"old");
    shard.list_push_back(b"list", &[b"old"]).unwrap();
    shard.set_add(b"set", &[b"old"]).unwrap();
    shard.hash_set(b"hash", &[(b"field", b"old")]).unwrap();
    shard.sorted_set_add(b"sorted", 1.0, b"old").unwrap();
    shard
        .stream_append(b"stream", 1, 0, &[(b"field", b"old")])
        .unwrap();
    shard.bloom_reserve(b"bloom", 0.01, 1_000, 2).unwrap();
    shard.bloom_add(b"bloom", b"old").unwrap();
    for key in [
        b"string".as_slice(),
        b"list",
        b"set",
        b"hash",
        b"sorted",
        b"stream",
        b"bloom",
    ] {
        assert!(shard.expire_at(key, UnixMillis::new(2_000)));
    }
    clock.set(2_000);
    assert_eq!(shard.expiration_stats().expired_keys_active, 0);
    assert_eq!(shard.get(b"string"), None);
    assert_eq!(shard.list_index(b"list", 0), Ok(None));
    assert_eq!(shard.set_contains(b"set", b"old"), Ok(false));
    assert_eq!(shard.hash_get(b"hash", b"field"), Ok(None));
    assert_eq!(shard.sorted_set_score(b"sorted", b"old"), Ok(None));
    assert_eq!(shard.bloom_contains(b"bloom", b"old"), Ok(false));
    assert_eq!(shard.stream_len(b"stream"), Ok(0));
    assert_eq!(shard.increment(b"string", 1), Ok(1));
    assert_eq!(shard.list_push_back(b"list", &[b"new"]), Ok(1));
    assert_eq!(shard.set_add(b"set", &[b"new"]), Ok(1));
    assert_eq!(shard.hash_set(b"hash", &[(b"field", b"new")]), Ok(1));
    assert_eq!(shard.sorted_set_add(b"sorted", 2.0, b"new"), Ok(true));
    assert_eq!(shard.bloom_add(b"bloom", b"new"), Ok(BloomAdd::Added));
    assert_eq!(shard.copied_value(b"string"), Some(b"1".to_vec()));
    assert_eq!(shard.list_index(b"list", 0), Ok(Some(&b"new"[..])));
    assert_eq!(shard.set_members(b"set").unwrap(), vec![&b"new"[..]]);
    assert_eq!(shard.hash_get(b"hash", b"field"), Ok(Some(&b"new"[..])));
    assert_eq!(shard.sorted_set_score(b"sorted", b"old"), Ok(None));
    assert_eq!(shard.expiration_stats().expired_keys_passive, 7);
    assert_eq!(shard.expire_cycle(16), 0);
}

#[test]
fn equal_deadline_storm_drains_with_a_fixed_budget_and_keeps_other_keys_live() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_bytes(b"persistent", b"ok");

    for db in 0..2 {
        shard.enter(db);
        for index in 0..64 {
            let key = format!("due-{index}");
            shard.set_bytes(key.as_bytes(), b"v");
            assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(2_000)));
        }
    }

    shard.enter(0);
    clock.set(2_000);
    for _ in 0..16 {
        assert_eq!(shard.expire_cycle(8), 8);
        assert_eq!(shard.get(b"persistent"), Some(&b"ok"[..]));
    }
    assert_eq!(shard.expire_cycle(8), 0);
    assert_eq!(shard.expiration_stats().expired_keys_active, 128);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    shard.enter(1);
    assert_eq!(shard.len(), 0);
}

#[test]
fn sampled_time_stays_fixed_until_the_batch_ends() {
    let clock = TestClock::new(1_000);
    let shard = KvShard::with_clock(clock.clone());

    shard.sample_time();
    clock.set(5_000);
    assert_eq!(shard.now_unix_millis(), UnixMillis::new(1_000));
    shard.clear_time_sample();
    assert_eq!(shard.now_unix_millis(), UnixMillis::new(5_000));
}

#[test]
fn a_lagging_replica_needs_the_primary_clock_floor_before_promotion() {
    let primary_wall = crate::time::TestWallClock::new(1_000, 1_000);
    let mut primary = KvShard::with_clock(primary_wall.clone());
    primary.set_bytes(b"soon", b"value");
    assert!(primary.expire_at(b"soon", UnixMillis::new(1_250)));
    let transferred = primary.snapshot_entries();

    primary_wall.set(1_500);
    assert_eq!(primary.get(b"soon"), None);
    let replica_wall = crate::time::TestWallClock::new(900, 900);
    let mut replica = KvShard::with_clock(replica_wall.clone());
    for entry in transferred.clone() {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(replica.get(b"soon"), Some(b"value".as_slice()));

    let mut promoted = KvShard::with_clock(crate::time::TestWallClock::new(900, 1_500));
    for entry in transferred {
        promoted.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(promoted.get(b"soon"), None);
}

#[test]
fn large_lease_and_limit_deadlines_saturate_without_wrapping() {
    let clock = TestClock::new(1);
    let mut shard = KvShard::with_clock(clock.clone());
    let (token, lease_end) = shard.lease_plan(b"lease", u64::MAX).unwrap().unwrap();
    assert_eq!(lease_end, u64::MAX);
    shard
        .lease_put(b"lease", token, lease_end, "LEASE")
        .unwrap();
    assert_eq!(shard.pttl(b"lease"), i64::MAX);

    clock.set(u64::MAX - 5);
    assert_eq!(shard.pttl(b"lease"), 5);
    let LimitDecision::Allow {
        window_end: Some(end),
        ..
    } = shard.plan_limit(b"limit", 2, 20).unwrap()
    else {
        panic!("expected an open rate-limit window");
    };
    assert_eq!(end, u64::MAX);
    shard.put_limit(b"limit", 1).unwrap();
    assert!(shard.expire_at(b"limit", UnixMillis::new(end)));
    assert_eq!(shard.pttl(b"limit"), 5);
    clock.set(u64::MAX);
    assert_eq!(shard.pttl(b"lease"), -2);
    assert_eq!(shard.pttl(b"limit"), -2);
}

#[test]
fn future_deadlines_skip_the_active_expire_scan() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for index in 0..1_000 {
        let key = index.to_string();

        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(50_000)));
    }

    assert_eq!(shard.expire_cycle(16), 0);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 1_000);
    assert_eq!(shard.len(), 1_000);

    clock.set(50_000);
    assert_eq!(shard.expire_cycle(16), 16);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 984);
    assert_eq!(shard.expire_cycle(16), 16);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 968);
}

#[test]
fn deleting_the_earliest_deadline_keeps_a_later_key_until_it_is_due() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"early", b"1");
    shard.set_bytes(b"later", b"2");
    assert!(shard.expire_at(b"early", UnixMillis::new(2_000)));
    assert!(shard.expire_at(b"later", UnixMillis::new(5_000)));
    assert!(shard.delete(b"early"));

    clock.set(3_000);
    assert_eq!(shard.expire_cycle(16), 0);
    assert_eq!(shard.get(b"later"), Some(&b"2"[..]));

    clock.set(5_000);
    assert_eq!(shard.expire_cycle(16), 1);
    assert_eq!(shard.get(b"later"), None);
}

#[test]
fn expiration_metrics_separate_passive_active_and_ttl_gauge() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_bytes(b"session", b"token");
    shard.set_bytes(b"cache", b"v");
    shard.set_bytes(b"keep", b"alive");
    assert!(shard.expire_at(b"session", UnixMillis::new(20)));
    assert!(shard.expire_at(b"cache", UnixMillis::new(20)));
    assert!(shard.expire_at(b"keep", UnixMillis::new(40)));
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 0,
            expired_keys_passive: 0,
            expired_keys_active: 0,
            keys_with_ttl: 3,
        }
    );

    assert!(shard.persist(b"keep"));
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);

    clock.set(20);
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);
    assert_eq!(shard.expiration_stats().expired_keys, 0);

    assert!(!shard.delete(b"session"));
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 1,
            expired_keys_passive: 1,
            expired_keys_active: 0,
            keys_with_ttl: 1,
        }
    );

    assert_eq!(shard.expire_cycle(8), 1);
    assert_eq!(
        shard.expiration_stats(),
        ExpirationStats {
            expired_keys: 2,
            expired_keys_passive: 1,
            expired_keys_active: 1,
            keys_with_ttl: 0,
        }
    );
    assert_eq!(shard.get(b"keep"), Some(&b"alive"[..]));

    shard.set_bytes(b"fresh", b"1");
    assert!(shard.expire_at(b"fresh", UnixMillis::new(50)));
    shard.set_bytes(b"fresh", b"2");
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    assert_eq!(shard.pttl(b"fresh"), -1);
}

#[test]
fn deadline_lookup_shrinks_after_most_timed_keys_are_removed() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    for index in 0..1_000 {
        let key = format!("t{index:04}");
        shard.set_bytes(key.as_bytes(), b"v");
        assert!(shard.expire_at(key.as_bytes(), UnixMillis::new(100_000)));
    }
    let grown = shard.deadlines[0].lookup.num_buckets();
    for index in 0..990 {
        assert!(shard.delete(format!("t{index:04}").as_bytes()));
    }

    assert!(shard.deadlines[0].lookup.num_buckets() * 16 <= grown);
    for index in 990..1_000 {
        assert!(shard.ttl_seconds(format!("t{index:04}").as_bytes()) > 0);
    }
}

#[test]
fn active_expire_budget_limits_one_cycle() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock.clone());
    let limits = MemoryLimits {
        active_expire_keys: 1,
        ..MemoryLimits::default()
    };

    shard.apply_memory_limits(limits);
    shard.set_bytes(b"a", b"1");
    shard.set_bytes(b"b", b"2");
    assert!(shard.expire_at(b"a", UnixMillis::new(20)));
    assert!(shard.expire_at(b"b", UnixMillis::new(20)));
    clock.set(20);
    assert_eq!(shard.memory_usage().entries, 2);
    assert_eq!(shard.expire_due(), 1);
    assert_eq!(shard.memory_usage().entries, 1);
}

mod ttl_hint_properties {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Op {
        Set(u8),
        Expire(u8),
        Persist(u8),
        Increment(u8),
        AddMembers(u8, u8),
        AddScores(u8, u8),
        Rename(u8, u8),
        Delete(u8),
        Enter(u8),
        Swap(u8, u8),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                3 => (0u8..8).prop_map(Op::Set),
                4 => (0u8..8).prop_map(Op::Expire),
                2 => (0u8..8).prop_map(Op::Persist),
                1 => (0u8..8).prop_map(Op::Increment),
                3 => (0u8..8, 1u8..200).prop_map(|(key, count)| Op::AddMembers(key, count)),
                3 => (0u8..8, 1u8..120).prop_map(|(key, count)| Op::AddScores(key, count)),
                2 => (0u8..8, 0u8..8).prop_map(|(from, to)| Op::Rename(from, to)),
                2 => (0u8..8).prop_map(Op::Delete),
                1 => (0u8..3).prop_map(Op::Enter),
                1 => (0u8..3, 0u8..3).prop_map(|(left, right)| Op::Swap(left, right)),
            ],
            1..300,
        )
    }

    fn key(id: u8) -> Vec<u8> {
        format!("key:{id}").into_bytes()
    }

    /// A key with a deadline record must carry the index's TTL bit.
    fn assert_hint_covers_deadlines(shard: &KvShard) {
        for (db, index) in shard.database_maps() {
            for (key, _) in index.entries() {
                if shard.deadline(db, key).is_some() {
                    let (_, hint) = index.lookup_ttl(key).unwrap();

                    assert!(
                        hint,
                        "db {db} key {key:?} has a deadline but a clear TTL bit"
                    );
                }
            }
        }
    }

    proptest! {
        #[test]
        fn clear_ttl_bit_means_no_deadline(ops in ops()) {
            let mut shard = KvShard::new();
            let far = UnixMillis::new(shard.now_unix_millis().get() + 10_000_000_000);

            for op in ops {
                match op {
                    Op::Set(id) => shard.set_bytes(&key(id), b"v"),
                    Op::Expire(id) => {
                        shard.expire_at(&key(id), far);
                    }
                    Op::Persist(id) => {
                        shard.persist(&key(id));
                    }
                    Op::Increment(id) => {
                        let _ = shard.increment(&key(id), 1);
                    }
                    Op::AddMembers(id, count) => {
                        let members: Vec<Vec<u8>> =
                            (0..count).map(|n| vec![n, 7, 7, 7, 7, 7, 7, 7]).collect();

                        let _ = shard.set_add(&key(id), &members);
                    }
                    Op::AddScores(id, count) => {
                        for n in 0..count {
                            let _ = shard.sorted_set_add(&key(id), f64::from(n), &[n, 9, 9, 9, 9, 9, 9, 9]);
                        }
                    }
                    Op::Rename(from, to) => {
                        shard.rename(&key(from), &key(to));
                    }
                    Op::Delete(id) => {
                        shard.delete(&key(id));
                    }
                    Op::Enter(db) => shard.enter(db),
                    Op::Swap(left, right) => shard.swap_databases(left, right),
                }

                assert_hint_covers_deadlines(&shard);
            }
        }
    }
}

#[test]
fn deadlines_moved_later_still_expire_in_order() {
    let mut deadlines = DeadlineDb::default();
    let mut reference = BTreeMap::new();
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;

    for _ in 0..4_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = format!("key-{}", seed % 300);
        let at = UnixMillis::new(1_000 + (seed >> 8) % 4_000);
        deadlines.set(key.as_bytes(), None, at, |_| None);
        reference.insert(key.into_bytes(), at);
    }

    // Reap step by step: every due key comes out, none early, each exactly once.
    for now in (1_000..=5_000).step_by(250) {
        let now = UnixMillis::new(now);

        while let Some(key) = deadlines.first_due(now, |_| unreachable!("owned keys")) {
            let at = reference.remove(&*key).expect("reaped once");
            assert!(at <= now);
            assert!(deadlines.remove(&key, |_| None));
        }

        assert!(reference.values().all(|&at| at > now));
        assert!(deadlines.min_at().is_none_or(|lowest| lowest > now));
    }

    assert!(reference.is_empty());
    assert_eq!(size_of::<Option<DeadlineRecord>>(), 32);
}

#[test]
fn active_expiry_drops_a_stale_deadline_instead_of_spinning() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    for key in [b"stale".as_slice(), b"due"] {
        shard.set_bytes(key, b"v");
        assert!(shard.expire_at(key, UnixMillis::new(2_000)));
    }

    // A deadline record whose key no longer says it may expire: the reaper cannot
    // remove the key through it.
    let active = shard.active;
    shard.mark_ttl_hint(active, b"stale", false);
    clock.set(2_000);

    assert_eq!(shard.expire_cycle(10), 1, "only the real expiry counts");
    assert_eq!(shard.get(b"due"), None);
    assert_eq!(shard.expiration_stats().keys_with_ttl, 0);
    assert!(
        !shard.soonest_deadline_due(),
        "no backlog is left to spin on"
    );
    assert_eq!(shard.expire_cycle(10), 0);
    assert_eq!(shard.get(b"stale"), Some(&b"v"[..]));
}
