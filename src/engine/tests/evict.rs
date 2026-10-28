use super::*;

#[test]
fn noeviction_rejects_writes_over_the_shard_budget() {
    let mut shard = KvShard::new();

    shard.set_memory_limit(Some(48));
    let response = execute(&mut shard, Command::set(owned(b"key"), owned(&[7; 128])));

    assert_eq!(
        response,
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
    );
    assert!(shard.is_empty());
}

#[test]
fn noeviction_rejection_does_not_charge_snapshot_and_recovers_after_delete() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"old", &[1; 64]);
    let used = shard.memory_usage().charged_bytes();
    shard.set_memory_limit(Some(used + 16));
    shard.begin_snapshot(4_096).unwrap();
    let with_snapshot = shard.memory_usage().charged_bytes();

    for _ in 0..2 {
        let response = execute(&mut shard, Command::set(owned(b"old"), owned(&[2; 128])));
        assert_eq!(
            response,
            Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
        );
        assert_eq!(shard.memory_usage().charged_bytes(), with_snapshot);
        assert_eq!(shard.get(b"old"), Some(&[1; 64][..]));
    }

    shard.finish_snapshot();
    assert!(shard.delete(b"old"));
    assert_eq!(
        execute(&mut shard, Command::set(owned(b"new"), owned(b"ok"))),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"new"), Some(&b"ok"[..]));
    assert!(shard.memory_usage().charged_bytes() <= used + 16);
}

#[test]
fn transient_buffer_charges_block_writes_and_release_admission() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"existing", b"value");
    let storage = shard.memory_usage().charged_bytes();
    shard.set_memory_limit(Some(storage + 128));
    shard.set_aux_memory(64, 64, 64);

    assert_eq!(
        execute(&mut shard, Command::set(owned(b"new"), owned(b"ok"))),
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
    );
    // Unsynced WAL bytes are reported, not charged: they are in the file already.
    assert_eq!(shard.memory_usage().charged_bytes(), storage + 128);
    assert_eq!(shard.memory_usage().persistence_bytes, 64);
    assert_eq!(shard.get(b"new"), None);

    shard.set_aux_memory(0, 0, 0);
    assert_eq!(
        execute(&mut shard, Command::set(owned(b"new"), owned(b"ok"))),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"new"), Some(&b"ok"[..]));
}

#[test]
fn eviction_churn_reuses_index_tombstones_instead_of_doubling_the_index() {
    let (capacity, preload, writes) = if cfg!(miri) {
        (64, 110, 2_000)
    } else {
        (4_096, 7_000, 25_000)
    };
    let run = |limited: bool| {
        let mut shard = KvShard::with_capacity(capacity);

        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
        // Near the 63/64 load limit removals leave tombstones in full groups.
        for index in 0..preload {
            let key = format!("k{index:06}");
            execute(&mut shard, Command::set(owned(key.as_bytes()), owned(b"v")));
        }
        let index_bytes = shard.memory_usage().index_bytes;

        if limited {
            shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
        }
        for index in 0..writes {
            let key = format!("n{index:06}");
            if !limited {
                let victim = shard.pick_eviction_victim(&[], false).unwrap();
                shard.apply_eviction(victim.0, &victim.1);
            }
            let response = execute(&mut shard, Command::set(owned(key.as_bytes()), owned(b"v")));
            assert_eq!(response, Response::Simple(b"OK"));
        }

        // The first admitted write may evict a few extra keys for its overhead estimate.
        assert!(
            (preload - (preload / 64).max(EVICTION_TURN_BUDGET)..=preload).contains(&shard.len()),
            "limited={limited}"
        );
        (index_bytes, shard.memory_usage().index_bytes)
    };

    // Without a memory limit the table keeps hashbrown's growth policy.
    let (before, after) = run(false);
    assert!(after > before);

    // The index charge also counts the recency cells, which may grow by a page while the
    // key log churns: the table itself must not grow.
    let (before, after) = run(true);
    assert!(after <= before + 4096, "{after} > {before}");
}

#[test]
fn allkeys_lru_evicts_the_cold_key_and_records_the_delete() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    execute(&mut shard, Command::set(owned(b"hot"), owned(b"1")));
    execute(&mut shard, Command::set(owned(b"cold"), owned(&[7; 128])));
    clock.set(10_000);
    execute(&mut shard, Command::Get(owned(b"hot")));
    let used = shard.memory_usage().charged_bytes();

    shard.set_memory_limit(Some(used));
    let mut sink = RecordingSink::default();
    let response = execute_with_sink(
        &mut shard,
        Command::set(owned(b"new"), owned(b"3")),
        &mut sink,
    )
    .unwrap();

    assert_eq!(response, Response::Simple(b"OK"));
    assert_eq!(shard.get(b"cold"), None);
    assert_eq!(shard.get(b"hot"), Some(&b"1"[..]));
    assert_eq!(shard.get(b"new"), Some(&b"3"[..]));
    assert_eq!(shard.memory_stats().evicted_keys, 1);
    assert_eq!(
        sink.mutations.first(),
        Some(&RecordedMutation::Delete(vec![b"cold".to_vec()]))
    );
}

#[test]
fn hot_get_keeps_the_recent_key_under_lru_eviction() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    execute(&mut shard, Command::set(owned(b"hot"), owned(b"1")));
    execute(&mut shard, Command::set(owned(b"cold"), owned(&[7; 128])));
    clock.set(10_000);

    let mut output = Vec::new();
    execute_hot_into(&mut shard, HotCommand::Get(b"hot"), &mut output).unwrap();
    assert_eq!(output, b"$1\r\n1\r\n");

    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
    execute(&mut shard, Command::set(owned(b"new"), owned(b"3")));

    assert_eq!(shard.get(b"cold"), None);
    assert_eq!(shard.get(b"hot"), Some(&b"1"[..]));
    assert_eq!(shard.get(b"new"), Some(&b"3"[..]));
}

/// The hot GET stamps a read through the record its lookup found; it must leave the
/// same metadata as a separate `touch` of the key, for every kind of hit and miss.
#[test]
fn hot_get_stamps_reads_like_touch() {
    for policy in [
        crate::EvictionPolicy::AllKeysLru,
        crate::EvictionPolicy::AllKeysLfu,
    ] {
        let clock = crate::time::TestClock::new(1_000);
        let mut hot = KvShard::with_clock(clock.clone());
        let mut touched = KvShard::with_clock(clock.clone());

        for shard in [&mut hot, &mut touched] {
            shard.set_eviction_policy(policy);
            shard.set_bytes(b"string", b"value");
            shard.set_bytes(b"counter", b"42");
            shard.set_bytes(b"due", b"gone");
            shard.expire_at(b"due", crate::time::UnixMillis::new(6_000));
            execute(&mut *shard, command_of(&[b"HSET", b"hash", b"f", b"v"]));
        }

        // Reads over a few minutes: the LFU counter climbs and ages, the LRU stamp moves.
        for round in 0..200u64 {
            clock.set(1_000 + round * 1_000);

            for key in [&b"string"[..], b"counter", b"hash", b"due", b"missing"] {
                let mut output = Vec::new();
                execute_hot_into(&mut hot, HotCommand::Get(key), &mut output).unwrap();
                touched.touch(key);
            }
        }

        clock.set(1_000 + 200 * 1_000 + 7_000);
        for key in [&b"string"[..], b"counter", b"hash"] {
            assert_eq!(
                hot.idle_seconds(key),
                touched.idle_seconds(key),
                "{policy:?}"
            );
            assert_eq!(
                hot.access_frequency(key),
                touched.access_frequency(key),
                "{policy:?}"
            );
        }
        if policy == crate::EvictionPolicy::AllKeysLfu {
            assert!(hot.access_frequency(b"string").unwrap() > 5);
        } else {
            assert_eq!(hot.idle_seconds(b"string"), Some(8));
        }
    }
}

#[test]
fn eviction_policies_preserve_write_keys_and_record_victims_before_mutating() {
    for policy in [
        crate::EvictionPolicy::AllKeysLru,
        crate::EvictionPolicy::AllKeysLfu,
        crate::EvictionPolicy::VolatileLru,
        crate::EvictionPolicy::VolatileTtl,
    ] {
        let clock = crate::time::TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock);
        shard.set_eviction_policy(policy);
        execute(&mut shard, Command::set(owned(b"protected"), owned(b"old")));
        execute(&mut shard, Command::set(owned(b"victim"), owned(&[7; 512])));
        if policy.volatile_only() {
            assert!(shard.expire_at(b"victim", crate::time::UnixMillis::new(20_000)));
        }
        shard.apply_memory_limits(crate::MemoryLimits {
            max_shard_bytes: shard.memory_usage().charged_bytes(),
            reserved_headroom_bytes: 0,
            eviction_policy: policy,
            maxmemory_samples: 16,
            ..Default::default()
        });

        let failure = execute_with_sink(
            &mut shard,
            Command::set(owned(b"protected"), owned(&[8; 64])),
            &mut FailingSink,
        );
        assert!(failure.is_err(), "{policy:?}");
        assert_eq!(shard.get(b"victim"), Some(&[7; 512][..]));
        assert_eq!(shard.get(b"protected"), Some(&b"old"[..]));
        assert_eq!(shard.memory_stats().evicted_keys, 0);

        let mut sink = RecordingSink::default();
        let response = execute_with_sink(
            &mut shard,
            Command::set(owned(b"protected"), owned(&[8; 64])),
            &mut sink,
        )
        .unwrap();
        assert_eq!(response, Response::Simple(b"OK"), "{policy:?}");
        assert_eq!(shard.get(b"victim"), None, "{policy:?}");
        assert_eq!(shard.get(b"protected"), Some(&[8; 64][..]));
        assert_eq!(shard.memory_stats().evicted_keys, 1);
        assert_eq!(
            sink.mutations,
            vec![
                RecordedMutation::Delete(vec![b"victim".to_vec()]),
                RecordedMutation::SetBytes(b"protected".to_vec(), vec![8; 64]),
            ],
            "{policy:?}"
        );
    }
}

#[test]
fn eviction_admission_reclaims_expired_bytes_before_scanning_candidates() {
    for policy in [
        crate::EvictionPolicy::AllKeysLru,
        crate::EvictionPolicy::AllKeysLfu,
        crate::EvictionPolicy::VolatileLru,
        crate::EvictionPolicy::VolatileTtl,
    ] {
        for expired_database in [0, 1] {
            let clock = crate::time::TestClock::new(1_000);
            let mut shard = KvShard::with_clock(clock.clone());
            shard.set_eviction_policy(policy);
            shard.enter(expired_database);
            shard.set_bytes(b"expired", &[7; 512]);
            assert!(shard.expire_at(b"expired", crate::time::UnixMillis::new(2_000)));
            shard.enter(0);
            shard.apply_memory_limits(crate::MemoryLimits {
                max_shard_bytes: shard.memory_usage().charged_bytes(),
                reserved_headroom_bytes: 0,
                eviction_policy: policy,
                active_expire_keys: 0,
                ..Default::default()
            });
            clock.set(2_000);

            let mut sink = RecordingSink::default();
            let response = execute_with_sink(
                &mut shard,
                Command::set(owned(b"new"), owned(&[8; 64])),
                &mut sink,
            )
            .unwrap();
            assert_eq!(
                response,
                Response::Simple(b"OK"),
                "{policy:?} DB{expired_database}"
            );
            assert_eq!(shard.get(b"new"), Some(&[8; 64][..]));
            assert_eq!(shard.memory_stats().eviction_scans, 0);
            assert_eq!(shard.memory_stats().evicted_keys, 0);
            assert_eq!(
                sink.mutations,
                vec![RecordedMutation::SetBytes(b"new".to_vec(), vec![8; 64])]
            );
            shard.enter(expired_database);
            assert_eq!(shard.get(b"expired"), None);
        }
    }
}

#[test]
fn admission_reclaims_due_key_behind_future_ttls_without_evicting_live_keys() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    for index in 0..128 {
        let key = format!("future-{index}");
        shard.set_bytes(key.as_bytes(), &[7; 64]);
        assert!(shard.expire_at(key.as_bytes(), crate::time::UnixMillis::new(10_000)));
    }
    shard.set_bytes(b"due", &[8; 512]);
    assert!(shard.expire_at(b"due", crate::time::UnixMillis::new(2_000)));
    shard.apply_memory_limits(crate::MemoryLimits {
        max_shard_bytes: shard.memory_usage().charged_bytes(),
        reserved_headroom_bytes: 0,
        eviction_policy: crate::EvictionPolicy::AllKeysLru,
        active_expire_keys: 0,
        ..Default::default()
    });
    clock.set(2_000);

    let mut sink = RecordingSink::default();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::set(owned(b"new"), owned(&[9; 64])),
            &mut sink,
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.memory_stats().evicted_keys, 0);
    assert_eq!(shard.memory_stats().eviction_scans, 0);
    assert_eq!(shard.get(b"future-0"), Some(&[7; 64][..]));
    assert_eq!(shard.get(b"due"), None);
    assert_eq!(shard.get(b"new"), Some(&[9; 64][..]));
}

#[test]
fn eviction_policies_reject_when_only_the_write_key_is_eligible() {
    for policy in [
        crate::EvictionPolicy::AllKeysLru,
        crate::EvictionPolicy::AllKeysLfu,
        crate::EvictionPolicy::VolatileLru,
        crate::EvictionPolicy::VolatileTtl,
    ] {
        let clock = crate::time::TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock);
        execute(
            &mut shard,
            Command::set(owned(b"protected"), owned(&[7; 512])),
        );
        if policy.volatile_only() {
            assert!(shard.expire_at(b"protected", crate::time::UnixMillis::new(20_000)));
        }
        shard.apply_memory_limits(crate::MemoryLimits {
            max_shard_bytes: shard.memory_usage().charged_bytes(),
            reserved_headroom_bytes: 0,
            eviction_policy: policy,
            ..Default::default()
        });

        let mut sink = RecordingSink::default();
        let response = execute_with_sink(
            &mut shard,
            Command::set(owned(b"protected"), owned(&[8; 64])),
            &mut sink,
        )
        .unwrap();
        assert_eq!(
            response,
            Response::Error(b"OOM command not allowed when used memory > 'maxmemory'."),
            "{policy:?}"
        );
        assert!(sink.mutations.is_empty(), "{policy:?}");
        assert_eq!(shard.get(b"protected"), Some(&[7; 512][..]), "{policy:?}");
        assert_eq!(shard.memory_stats().evicted_keys, 0, "{policy:?}");
    }
}

#[test]
fn write_larger_than_the_shard_budget_is_rejected_without_eviction() {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    execute(&mut shard, Command::set(owned(b"keep"), owned(b"1")));
    shard.set_memory_limit(Some(64));
    let response = execute(&mut shard, Command::set(owned(b"huge"), owned(&[9; 256])));

    assert_eq!(
        response,
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
    );
    assert_eq!(shard.get(b"keep"), Some(&b"1"[..]));
    assert_eq!(shard.memory_stats().evicted_keys, 0);
}

#[test]
fn snapshot_journal_over_bound_aborts_the_snapshot_and_applies_the_write() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"key", &[1; 64]);
    shard.begin_snapshot(8).unwrap();
    let response = execute(&mut shard, Command::set(owned(b"key"), owned(b"next")));

    assert_eq!(response, Response::Simple(b"OK"));
    assert_eq!(shard.get(b"key"), Some(&b"next"[..]));
    assert_eq!(shard.snapshot_aborts(), 1);
}

fn filled_lru_shard(keys: usize) -> KvShard {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);

    for index in 0..keys {
        shard.set_bytes(format!("key:{index:07}").as_bytes(), &[b'x'; 100]);
    }

    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
    shard
}

#[test]
fn large_write_is_admitted_and_maintenance_pays_the_eviction_debt() {
    let keys = if cfg!(miri) { 300 } else { 60_000 };
    let mut shard = filled_lru_shard(keys);
    let limit = shard.memory_usage().charged_bytes();
    let big = vec![b'y'; limit / 3];
    let mut sink = RecordingSink::default();

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::set(owned(b"big"), owned(&big)),
            &mut sink
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert!(shard.get(b"big") == Some(big.as_slice()));

    let mut slices = 0;

    while evict_debt(&mut shard, &mut sink, std::time::Duration::from_millis(2)).unwrap() {
        slices += 1;
        assert!(slices < 100_000, "the debt is paid");
    }

    assert!(!shard.eviction_debt());
    assert!(shard.memory_usage().charged_bytes() <= limit);
    assert!(
        shard.get(b"big") == Some(big.as_slice()),
        "the admitted value is kept"
    );

    // Eviction followed the bytes needed: about a third of the small keys went, not all.
    let evicted = keys + 1 - shard.len();

    assert_eq!(
        sink.mutations
            .iter()
            .filter(|m| matches!(m, RecordedMutation::Delete(_)))
            .count(),
        evicted
    );
    assert!(
        evicted > keys / 4 && evicted < keys / 2,
        "evicted {evicted} of {keys}"
    );
}

fn set_big(shard: &mut KvShard, sink: &mut RecordingSink, key: &[u8], bytes: usize) -> bool {
    let value = vec![b'y'; bytes];

    match execute_with_sink(shard, Command::set(owned(key), owned(&value)), sink).unwrap() {
        Response::Simple(b"OK") => true,
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.") => false,
        other => panic!("unexpected reply {other:?}"),
    }
}

fn pay_debt(shard: &mut KvShard, sink: &mut RecordingSink) {
    let mut slices = 0;

    while evict_debt(shard, sink, std::time::Duration::from_millis(2)).unwrap() {
        slices += 1;
        assert!(slices < 100_000, "the debt is paid");
    }
}

#[test]
fn large_writes_made_while_a_debt_is_owed_are_admitted_up_to_the_bound() {
    let keys = if cfg!(miri) { 300 } else { 60_000 };
    let mut shard = filled_lru_shard(keys);
    let limit = shard.memory_usage().charged_bytes();
    let mut sink = RecordingSink::default();
    // Too much to evict within one write's time budget (thousands of keys each).
    let big = limit / 5;

    assert!(set_big(&mut shard, &mut sink, b"big:1", big));
    assert!(shard.eviction_debt());

    // A second one while the first is still owed: the shard is under a quarter over.
    assert!(set_big(&mut shard, &mut sink, b"big:2", big));
    assert!(shard.bytes_over_limit(0) > limit / EVICTION_DEBT_DIVISOR);

    // A third would take the overshoot further past the bound: refused, nothing written.
    let logged = sink.mutations.len();

    assert!(!set_big(&mut shard, &mut sink, b"big:3", big));
    assert!(shard.get(b"big:3").is_none());
    assert!(
        !sink.mutations[logged..]
            .iter()
            .any(|mutation| !matches!(mutation, RecordedMutation::Delete(_))),
        "the refused write is not logged"
    );

    // Maintenance pays the debt off; the same write is then admitted.
    pay_debt(&mut shard, &mut sink);
    assert!(shard.memory_usage().charged_bytes() <= limit);
    assert!(set_big(&mut shard, &mut sink, b"big:3", big));
    pay_debt(&mut shard, &mut sink);
    assert!(shard.memory_usage().charged_bytes() <= limit);
    assert!(
        shard.get(b"big:3").is_some(),
        "the last admitted value is kept"
    );
}

#[test]
fn small_write_made_while_a_debt_is_owed_frees_only_its_own_size() {
    let keys = if cfg!(miri) { 300 } else { 60_000 };
    let mut shard = filled_lru_shard(keys);
    let limit = shard.memory_usage().charged_bytes();
    let mut sink = RecordingSink::default();

    assert!(set_big(&mut shard, &mut sink, b"big", limit / 5));
    assert!(shard.eviction_debt());

    for index in 0..100 {
        let charged = shard.memory_usage().charged_bytes();
        let stored = shard.len();
        let key = format!("small:{index}");

        assert!(set_big(&mut shard, &mut sink, key.as_bytes(), 200));
        // A few keys of about its size go, not a whole time budget of them, and the
        // debt does not grow.
        assert!(
            stored + 1 - shard.len() <= 8,
            "evicted {}",
            stored + 1 - shard.len()
        );
        assert!(shard.memory_usage().charged_bytes() <= charged);
        assert!(shard.eviction_debt());
    }

    pay_debt(&mut shard, &mut sink);
    assert!(shard.memory_usage().charged_bytes() <= limit);
    assert_eq!(
        sink.mutations
            .iter()
            .filter(|m| matches!(m, RecordedMutation::Delete(_)))
            .count(),
        keys + 101 - shard.len(),
        "every eviction is logged as a delete"
    );
}

#[test]
fn volatile_policy_still_refuses_a_write_it_cannot_make_room_for_in_time() {
    let keys = if cfg!(miri) { 300 } else { 60_000 };
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::VolatileLru);
    for index in 0..keys {
        let key = format!("key:{index:07}");

        shard.set_bytes(key.as_bytes(), &[b'x'; 100]);
        assert!(shard.expire_at(key.as_bytes(), crate::time::UnixMillis::new(u64::MAX / 2)));
    }
    let limit = shard.memory_usage().charged_bytes();

    shard.set_memory_limit(Some(limit));

    let mut sink = RecordingSink::default();

    assert!(!set_big(&mut shard, &mut sink, b"big", limit / 5));
    assert!(shard.get(b"big").is_none());
    assert!(!shard.eviction_debt());
}

#[test]
fn write_the_evictable_data_cannot_cover_is_refused_before_evicting() {
    let mut shard = filled_lru_shard(if cfg!(miri) { 100 } else { 5_000 });
    let limit = shard.memory_usage().charged_bytes();
    let keys = shard.len();
    let mut sink = RecordingSink::default();
    // Fits the shard budget, but index bytes are not evictable.
    let huge = vec![b'z'; limit - 1];

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::set(owned(b"huge"), owned(&huge)),
            &mut sink
        )
        .unwrap(),
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
    );
    assert_eq!(
        shard.len(),
        keys,
        "nothing was evicted for a write that cannot fit"
    );
    assert!(sink.mutations.is_empty());
    assert!(!shard.eviction_debt());
}

#[test]
fn small_write_that_fits_after_eviction_is_admitted_without_debt() {
    let mut shard = filled_lru_shard(if cfg!(miri) { 100 } else { 5_000 });
    let limit = shard.memory_usage().charged_bytes();

    assert_eq!(
        execute(&mut shard, Command::set(owned(b"new"), owned(&[1; 300]))),
        Response::Simple(b"OK")
    );
    assert!(!shard.eviction_debt());
    assert!(shard.memory_usage().charged_bytes() <= limit);
}

#[test]
fn keys_evicted_inside_a_refused_transaction_still_reach_the_log() {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
    shard.log_removals(true);
    for index in 0..200 {
        in_db(
            &mut shard,
            &mut log,
            0,
            Command::set(
                owned(format!("key:{index:07}").as_bytes()),
                owned(&[b'x'; 100]),
            ),
        );
    }
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));

    let refused = execute_transaction(
        &mut shard,
        &mut FailingSink,
        &mut (),
        1,
        vec![Command::set(owned(b"big"), owned(&[b'y'; 2_000]))],
        0,
    );

    assert!(refused.is_err());
    assert!(shard.get(b"big").is_none(), "the write was rolled back");
    assert!(shard.len() < 200, "the evictions stand");

    // The next command logs the deletes the refused batch carried.
    in_db(&mut shard, &mut log, 0, Command::DbSize);
    assert_eq!(replayed(&log).len(), shard.len());
}

#[test]
fn a_replica_over_its_memory_limit_neither_evicts_nor_refuses() {
    for policy in [
        crate::EvictionPolicy::AllKeysLru,
        crate::EvictionPolicy::VolatileLru,
        crate::EvictionPolicy::NoEviction,
    ] {
        let mut replica = KvShard::new();

        replica.follow_primary(true);
        replica.apply_memory_limits(crate::MemoryLimits {
            max_shard_bytes: 4096,
            reserved_headroom_bytes: 0,
            eviction_policy: policy,
            maxmemory_samples: 16,
            ..Default::default()
        });

        // The primary's writes apply whatever the replica's own limit says.
        for index in 0..64u32 {
            crate::persistence::apply_mutation(
                &mut replica,
                0,
                &crate::OwnedMutation::SetBytes {
                    key: format!("key:{index}").into_bytes(),
                    value: vec![7; 512],
                },
            )
            .unwrap();
        }

        let charged = replica.memory_usage().charged_bytes();

        assert!(
            charged > 4096 * 4,
            "{policy:?}: accounting still counts the data"
        );

        // Nothing evicts on its own, through a write's admission or a debt.
        let mut sink = RecordingSink::default();
        let admitted = execute_with_sink(
            &mut replica,
            command_of(&[b"SET", b"key:0", b"v"]),
            &mut sink,
        )
        .unwrap()
        .into_owned();

        assert_eq!(admitted, Response::Simple(b"OK"), "{policy:?}");
        assert!(!evict_debt(&mut replica, &mut sink, std::time::Duration::from_secs(1)).unwrap());
        assert_eq!(
            execute(&mut replica, Command::DbSize),
            Response::Integer(64)
        );
        assert_eq!(replica.memory_stats().evicted_keys, 0, "{policy:?}");
        assert!(
            !sink
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, RecordedMutation::Delete(_))),
            "{policy:?}"
        );

        // The primary's deletes drive its memory down.
        for index in 0..32u32 {
            crate::persistence::apply_mutation(
                &mut replica,
                0,
                &crate::OwnedMutation::Delete {
                    keys: vec![format!("key:{index}").into_bytes()],
                },
            )
            .unwrap();
        }
        assert!(replica.memory_usage().charged_bytes() < charged);
        assert_eq!(
            execute(&mut replica, Command::DbSize),
            Response::Integer(32)
        );
    }
}

#[test]
fn unsynced_wal_bytes_are_reported_but_never_evict_keys() {
    let mut shard = KvShard::new();

    shard.apply_memory_limits(crate::MemoryLimits {
        max_shard_bytes: 64 * 1024,
        reserved_headroom_bytes: 0,
        eviction_policy: crate::EvictionPolicy::AllKeysLru,
        maxmemory_samples: 16,
        ..Default::default()
    });
    for index in 0..8u32 {
        execute(
            &mut shard,
            command_of(&[b"SET", format!("key:{index}").as_bytes(), &[7; 512]]),
        );
    }

    // A maintenance sample taken during a write burst: far over the limit, all of it
    // already in the WAL file.
    shard.set_aux_memory(0, 0, 10 << 20);

    assert_eq!(shard.memory_usage().persistence_bytes, 10 << 20);
    assert_eq!(
        execute(&mut shard, command_of(&[b"SET", b"key:new", &[8; 512]])),
        Response::Simple(b"OK")
    );
    assert_eq!(execute(&mut shard, Command::DbSize), Response::Integer(9));
    assert_eq!(shard.memory_stats().evicted_keys, 0);
}

/// A `noeviction` shard holding one key of each kind, with its limit set
/// one byte under what it uses: the store is over its limit.
fn full_noeviction_shard() -> KvShard {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    for parts in [
        &[&b"SET"[..], b"string", &[1; 256]][..],
        &[b"SET", b"unlinked", &[2; 256]],
        &[b"HSET", b"hash", b"field", b"value", b"other", b"value"],
        &[b"SADD", b"set", b"a", b"b", b"c"],
        &[b"RPUSH", b"list", b"head", b"tail"],
        &[b"ZADD", b"zset", b"1", b"a", b"2", b"b"],
    ] {
        assert!(!matches!(
            execute(&mut shard, command_of(parts)),
            Response::Error(_)
        ));
    }
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() - 1));

    shard
}

fn is_oom(response: &Response<'_>) -> bool {
    matches!(response, Response::Error(message) if message.starts_with(b"OOM"))
}

#[test]
fn a_full_noeviction_store_refuses_growth_but_accepts_deletes() {
    let mut shard = full_noeviction_shard();

    for parts in [
        &[&b"SET"[..], b"new", &[3; 256]][..],
        &[b"HSET", b"hash", b"grown", &[3; 256]],
        &[b"SADD", b"set", &[3; 256]],
        &[b"LPUSH", b"list", &[3; 256]],
        &[b"ZADD", b"zset", b"3", &[3; 256]],
        &[b"EXPIRE", b"list", b"100"],
    ] {
        let response = execute(&mut shard, command_of(parts));
        assert!(is_oom(&response), "{parts:?}: {response:?}");
    }

    assert_eq!(
        execute(&mut shard, command_of(&[b"HDEL", b"hash", b"field"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"SREM", b"set", b"a"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"ZREM", b"zset", b"a"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"LPOP", b"list"])),
        Response::Bulk(Some(Cow::Borrowed(b"head")))
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"PEXPIRE", b"zset", b"0"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"EXISTS", b"zset"])),
        Response::Integer(0)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"UNLINK", b"unlinked"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"DEL", b"string"])),
        Response::Integer(1)
    );
    assert_eq!(shard.get(b"string"), None);
    assert_eq!(shard.get(b"unlinked"), None);

    // The deletes made room again.
    assert_eq!(
        execute(&mut shard, command_of(&[b"SET", b"new", b"ok"])),
        Response::Simple(b"OK")
    );
}

#[test]
fn a_full_noeviction_store_accepts_a_delete_on_the_hot_path() {
    let mut shard = full_noeviction_shard();
    let mut output = Vec::new();

    execute_hot_into(
        &mut shard,
        HotCommand::Set(b"new", &[3; 256], None),
        &mut output,
    )
    .unwrap();
    assert!(output.starts_with(b"-OOM "), "{output:?}");

    output.clear();
    execute_hot_into(&mut shard, HotCommand::Delete(b"string"), &mut output).unwrap();
    assert_eq!(output, b":1\r\n");
    assert_eq!(shard.get(b"string"), None);
}

#[test]
fn a_full_noeviction_store_keeps_snapshot_copies_of_deleted_keys() {
    let mut shard = full_noeviction_shard();

    let before = shard.snapshot_entries();
    shard.begin_snapshot(1 << 20).unwrap();

    assert_eq!(
        execute(&mut shard, command_of(&[b"DEL", b"string"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"HDEL", b"hash", b"field"])),
        Response::Integer(1)
    );
    assert_ne!(shard.snapshot_entries(), before);
    assert_eq!(shard.finish_snapshot(), before);
}

#[test]
fn smove_on_a_full_noeviction_store_is_refused() {
    let mut shard = KvShard::new();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    execute(&mut shard, command_of(&[b"SADD", b"set", b"a", b"b"]));
    execute(&mut shard, command_of(&[b"SADD", b"ints", b"1", b"2"]));
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() - 1));

    // A new destination key, or a member that moves an integer set to a larger encoding:
    // both grow memory, so SMOVE keeps the OOM check.
    for parts in [
        &[&b"SMOVE"[..], b"set", b"fresh", b"a"][..],
        &[b"SMOVE", b"set", b"ints", b"a"],
    ] {
        let response = execute(&mut shard, command_of(parts));
        assert!(is_oom(&response), "{parts:?}: {response:?}");
    }
    assert_eq!(
        execute(&mut shard, command_of(&[b"SCARD", b"set"])),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"EXISTS", b"fresh"])),
        Response::Integer(0)
    );
}
