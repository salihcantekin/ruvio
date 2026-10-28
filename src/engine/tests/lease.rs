use super::*;

#[test]
/// LEASE, RELEASE, SETV, and CHANGES allocate only after they are used. XADD DELAY hides one entry.
fn lease_setv_changes_and_delay_stay_off_the_unused_path() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    assert_eq!(
        execute(&mut shard, Command::set(owned(b"plain"), owned(b"v"))),
        Response::Simple(b"OK")
    );
    assert!(shard.changes_after(0).is_empty());
    let first = match execute(
        &mut shard,
        Command::Lease {
            key: owned(b"lock"),
            ttl_ms: 500,
        },
    ) {
        Response::Integer(token) => token,
        other => panic!("expected a token, got {other:?}"),
    };

    assert_eq!(first, 1_000 << 6);
    assert_eq!(
        execute(
            &mut shard,
            Command::Lease {
                key: owned(b"lock"),
                ttl_ms: 500,
            },
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Release {
                key: owned(b"lock"),
                token: first as u64,
            },
        ),
        Response::Integer(1)
    );

    match execute(
        &mut shard,
        Command::Lease {
            key: owned(b"lock"),
            ttl_ms: 500,
        },
    ) {
        Response::Integer(token) => assert!(token > first, "a new lease never reuses a token"),
        other => panic!("expected a token, got {other:?}"),
    }
    assert_eq!(
        execute(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"12"),
                expected: None,
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"doc"))),
        Response::Bulk(Some(Cow::Owned(b"12".to_vec())))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"11"),
                expected: Some(1),
            },
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"10"),
                expected: Some(1),
            },
        ),
        Response::Error(b"ERR version mismatch")
    );
    assert_eq!(
        execute(&mut shard, Command::Changes(None)),
        Response::Integer(0)
    );
    execute(&mut shard, Command::set(owned(b"order"), owned(b"paid")));
    match execute(&mut shard, Command::Changes(Some(0))) {
        Response::Exec(rows) => assert_eq!(rows.len(), 1),
        other => panic!("{other:?}"),
    }

    execute(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"jobs"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            min_id: None,
            delay_ms: Some(5_000),
            nomkstream: false,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
    );
    execute(
        &mut shard,
        Command::StreamGroupCreate {
            key: owned(b"jobs"),
            group: owned(b"g"),
            start: crate::stream::GroupStart::Id(0, 0),
            mkstream: false,
        },
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::StreamGroupRead {
                key: owned(b"jobs"),
                group: owned(b"g"),
                consumer: owned(b"c"),
                count: None,
                block: None,
                noack: true,
            },
        ),
        Response::NullArray
    );
    clock.set(7_000);
    match execute(
        &mut shard,
        Command::StreamGroupRead {
            key: owned(b"jobs"),
            group: owned(b"g"),
            consumer: owned(b"c"),
            count: None,
            block: None,
            noack: true,
        },
    ) {
        Response::Exec(rows) => assert_eq!(rows.len(), 1),
        other => panic!("{other:?}"),
    }
}

#[test]
fn lease_setv_changes_and_delay_hold_their_own_contracts() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();
    let token = |response: Response<'_>| match response {
        Response::Integer(token) => token as u64,
        other => panic!("expected a token, got {other:?}"),
    };

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::set(owned(b"plain"), owned(b"v")),
            &mut sink,
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"plain"),
                ttl_ms: 500,
            },
            &mut sink
        )
        .unwrap(),
        wrong_type()
    );
    let first = token(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"lock"),
                ttl_ms: 500,
            },
            &mut sink,
        )
        .unwrap(),
    );

    assert_eq!(first, 1_000 << 6);
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"lock")), &mut sink).unwrap(),
        Response::Simple(b"lease")
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"lock")), &mut sink).unwrap(),
        wrong_type()
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"lock")), &mut sink).unwrap(),
        Response::Integer(500)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Release {
                key: owned(b"lock"),
                token: 9,
            },
            &mut sink
        )
        .unwrap(),
        Response::Integer(0)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"lock"),
                ttl_ms: 500,
            },
            &mut sink
        )
        .unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Release {
                key: owned(b"lock"),
                token: first,
            },
            &mut sink
        )
        .unwrap(),
        Response::Integer(1)
    );
    clock.set(2_000);
    // A released lease takes its key with it.
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"lock")), &mut sink).unwrap(),
        Response::Integer(-2)
    );
    let other = token(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"other"),
                ttl_ms: 500,
            },
            &mut sink,
        )
        .unwrap(),
    );

    let second = token(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"lock"),
                ttl_ms: 500,
            },
            &mut sink,
        )
        .unwrap(),
    );

    assert!(second > first, "an expired lease never reuses a token");
    assert!(second > other);
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Del(vec![owned(b"lock")].into()),
            &mut sink
        )
        .unwrap(),
        Response::Integer(1)
    );
    let third = token(
        execute_with_sink(
            &mut shard,
            Command::Lease {
                key: owned(b"lock"),
                ttl_ms: 500,
            },
            &mut sink,
        )
        .unwrap(),
    );

    assert!(third > second, "a deleted lease never restarts the token");

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"12"),
                expected: Some(1),
            },
            &mut sink
        )
        .unwrap(),
        Response::Error(b"ERR version mismatch")
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"12"),
                expected: None,
            },
            &mut sink
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"doc")), &mut sink).unwrap(),
        Response::Simple(b"string")
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"11"),
                expected: Some(1),
            },
            &mut sink
        )
        .unwrap(),
        Response::Integer(2)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"10"),
                expected: Some(1),
            },
            &mut sink
        )
        .unwrap(),
        Response::Error(b"ERR version mismatch")
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"doc")), &mut sink).unwrap(),
        Response::Bulk(Some(Cow::Owned(b"11".to_vec())))
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Increment(owned(b"doc"), 1), &mut sink).unwrap(),
        wrong_type()
    );
    execute_with_sink(
        &mut shard,
        Command::ListPush(owned(b"list"), vec![owned(b"a")].into()),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"list"),
                value: owned(b"no"),
                expected: None,
            },
            &mut sink
        )
        .unwrap(),
        wrong_type()
    );
    execute_with_sink(
        &mut shard,
        Command::set(owned(b"doc"), owned(b"plain")),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetVersioned {
                key: owned(b"doc"),
                value: owned(b"again"),
                expected: None,
            },
            &mut sink
        )
        .unwrap(),
        Response::Integer(1)
    );

    assert_eq!(
        execute_with_sink(&mut shard, Command::Changes(Some(0)), &mut sink).unwrap(),
        Response::Exec(vec![])
    );
    execute_with_sink(
        &mut shard,
        Command::set(owned(b"before"), owned(b"x")),
        &mut sink,
    )
    .unwrap();
    assert!(matches!(
        execute_with_sink(&mut shard, Command::Changes(Some(0)), &mut sink).unwrap(),
        Response::Exec(rows) if rows.is_empty()
    ));
    assert_eq!(
        execute_with_sink(&mut shard, Command::Changes(None), &mut sink).unwrap(),
        Response::Integer(0)
    );

    for index in 0..1_100 {
        let key = format!("n{index}");

        execute_with_sink(
            &mut shard,
            Command::set(owned(key.as_bytes()), owned(b"v")),
            &mut sink,
        )
        .unwrap();
    }

    match execute_with_sink(&mut shard, Command::Changes(Some(0)), &mut sink).unwrap() {
        Response::Exec(rows) => {
            assert_eq!(rows.len(), 1024);
            assert!(
                matches!(rows.first(), Some(Response::Exec(parts)) if matches!(parts.first(), Some(Response::Integer(77))))
            );
        }

        other => panic!("{other:?}"),
    }

    assert_eq!(
        execute_with_sink(&mut shard, Command::Changes(None), &mut sink).unwrap(),
        Response::Integer(1_100)
    );
    let before_incr =
        match execute_with_sink(&mut shard, Command::Changes(None), &mut sink).unwrap() {
            Response::Integer(cursor) => cursor,
            other => panic!("{other:?}"),
        };

    execute_with_sink(
        &mut shard,
        Command::Increment(owned(b"counter"), 1),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(&mut shard, Command::Changes(None), &mut sink).unwrap(),
        Response::Integer(before_incr)
    );

    execute_with_sink(
        &mut shard,
        Command::StreamAdd {
            key: owned(b"jobs"),
            id: StreamAddId::Explicit(1, 0),
            max_len: None,
            min_id: None,
            delay_ms: Some(5_000),
            nomkstream: false,
            fields: vec![(owned(b"field"), owned(b"a"))],
        },
        &mut sink,
    )
    .unwrap();
    match execute_with_sink(
        &mut shard,
        Command::StreamRead {
            count: None,
            block: None,
            keys: vec![(owned(b"jobs"), crate::stream::StreamCursor::After(0, 0))],
        },
        &mut sink,
    )
    .unwrap()
    {
        Response::Exec(rows) => assert_eq!(rows.len(), 1),
        other => panic!("delayed entry stays visible to XREAD: {other:?}"),
    }

    execute_with_sink(
        &mut shard,
        Command::StreamDelete {
            key: owned(b"jobs"),
            ids: vec![(1, 0)],
        },
        &mut sink,
    )
    .unwrap();
    let entry = shard
        .snapshot_entries()
        .into_iter()
        .find(|entry| entry.key == b"jobs")
        .unwrap();
    let crate::store::SnapshotValue::Stream { delayed, .. } = entry.value else {
        panic!("expected the stream");
    };

    assert!(delayed.is_empty());

    let mut failed = FailingSink;
    let missed = execute_with_sink(
        &mut shard,
        Command::Lease {
            key: owned(b"new-lock"),
            ttl_ms: 500,
        },
        &mut failed,
    );

    assert!(missed.is_err());
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"new-lock")), &mut sink).unwrap(),
        Response::Simple(b"none")
    );

    // On the wall clock the lease has long run out: replay keeps it while expiry is paused,
    // like any key with a past deadline, and drops it once expiry resumes.
    let mut restored = KvShard::new();
    let previous = restored.pause_expiration_for_replay();

    for (database, mutation) in &sink.records {
        crate::persistence::apply_mutation(&mut restored, *database, mutation).unwrap();
    }

    assert_eq!(
        restored.value_kind(b"other"),
        Some(crate::store::ValueKind::Lease)
    );
    restored.resume_expiration_after_replay(previous);
    assert_eq!(restored.value_kind(b"other"), None);
    assert_eq!(
        restored.value_kind(b"doc"),
        Some(crate::store::ValueKind::Versioned)
    );
    assert_eq!(restored.value_kind(b"new-lock"), None);
}

#[test]
fn semaphore_admits_max_holders_and_frees_a_slot_on_release_or_expiry() {
    // SEMAPHORE RELEASE
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();
    let sem = |ttl_ms| Command::Semaphore {
        key: owned(b"jobs"),
        max: 2,
        ttl_ms,
    };

    let token = |response: Response<'_>| match response {
        Response::Integer(token) => token as u64,
        other => panic!("expected a token, got {other:?}"),
    };

    let first = token(execute_with_sink(&mut shard, sem(5_000), &mut sink).unwrap());
    let second = token(execute_with_sink(&mut shard, sem(1_000), &mut sink).unwrap());

    assert_eq!(first, 1_000 << 6);
    assert_eq!(second, first + 1);

    let records = sink.records.len();

    assert_eq!(
        execute_with_sink(&mut shard, sem(5_000), &mut sink).unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Release {
                key: owned(b"jobs"),
                token: 999,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(0)
    );
    assert_eq!(
        sink.records.len(),
        records,
        "a full or wrong-token call writes nothing"
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"jobs")), &mut sink).unwrap(),
        Response::Simple(b"semaphore")
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"jobs")), &mut sink).unwrap(),
        wrong_type()
    );

    clock.set(2_000);

    let third = token(execute_with_sink(&mut shard, sem(5_000), &mut sink).unwrap());

    assert!(third > second, "the expired holder freed its slot");
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Release {
                key: owned(b"jobs"),
                token: first,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );

    assert_eq!(
        sink.records
            .iter()
            .filter(|(_, mutation)| matches!(mutation, OwnedMutation::SemaphoreAcquire { .. }))
            .count(),
        3
    );

    let mut durable = KvShard::new();
    let mut log = DatabaseLog::default();
    let long = Command::Semaphore {
        key: owned(b"jobs"),
        max: 2,
        ttl_ms: 4_000_000_000_000,
    };

    let kept = token(execute_with_sink(&mut durable, long.clone(), &mut log).unwrap());
    let dropped = token(execute_with_sink(&mut durable, long.clone(), &mut log).unwrap());

    execute_with_sink(
        &mut durable,
        Command::Release {
            key: owned(b"jobs"),
            token: dropped,
        },
        &mut log,
    )
    .unwrap();

    let mut restored = replayed(&log);

    assert!(restored.semaphore_holds(b"jobs", kept));
    assert!(!restored.semaphore_holds(b"jobs", dropped));

    clock.set(7_001);
    assert_eq!(
        shard.value_kind(b"jobs"),
        None,
        "the key leaves with its last holder"
    );

    let fresh = token(execute_with_sink(&mut shard, sem(1_000), &mut sink).unwrap());

    assert!(
        fresh > third,
        "tokens never repeat after the key is created again"
    );

    execute_with_sink(
        &mut shard,
        Command::set(owned(b"plain"), owned(b"v")),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Semaphore {
                key: owned(b"plain"),
                max: 1,
                ttl_ms: 1,
            },
            &mut sink,
        )
        .unwrap(),
        wrong_type()
    );
}

#[test]
fn capped_increment_refuses_past_the_limit_without_a_write() {
    // INCRBY MAX
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let capped = |delta, limit| Command::IncrementCapped {
        key: owned(b"seats"),
        delta,
        limit,
    };

    assert_eq!(
        execute_with_sink(&mut shard, capped(3, 5), &mut sink).unwrap(),
        Response::Integer(3)
    );
    assert_eq!(
        execute_with_sink(&mut shard, capped(2, 5), &mut sink).unwrap(),
        Response::Integer(5)
    );

    let records = sink.records.len();

    assert_eq!(
        execute_with_sink(&mut shard, capped(1, 5), &mut sink).unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(
        sink.records.len(),
        records,
        "a refused increment writes nothing"
    );
    assert_eq!(
        execute_with_sink(&mut shard, capped(-2, 5), &mut sink).unwrap(),
        Response::Integer(3)
    );
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::SetInteger { value: 3, .. }))
    ));
    assert_eq!(
        parse_command(b"*5\r\n$6\r\nINCRBY\r\n$1\r\nk\r\n$1\r\n1\r\n$3\r\nmax\r\n$1\r\n9\r\n")
            .unwrap()
            .unwrap()
            .0,
        Command::IncrementCapped {
            key: owned(b"k"),
            delta: 1,
            limit: 9,
        }
    );
}

#[test]
fn limit_and_once_cost_only_the_keys_that_use_them() {
    // LIMIT and ONCE
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::set(owned(b"plain"), owned(b"v")),
            &mut sink,
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"durable"),
                max: 3,
                window_ms: 4_000_000_000_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(2)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Once {
                key: owned(b"idem"),
                ttl_ms: 4_000_000_000_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"plain"),
                max: 5,
                window_ms: 60_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut sink,
        )
        .unwrap(),
        wrong_type()
    );

    for expected in [4_i64, 3, 2, 1, 0] {
        assert_eq!(
            execute_with_sink(
                &mut shard,
                Command::Limit {
                    key: owned(b"login"),
                    max: 5,
                    window_ms: 60_000,
                    mode: crate::protocol::LimitMode::Fixed,
                },
                &mut sink,
            )
            .unwrap(),
            Response::Integer(expected)
        );
    }

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"login"),
                max: 5,
                window_ms: 60_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Bulk(None)
    );

    // The window's first use logs the count and its deadline as one transaction.
    let logged: Vec<&OwnedMutation> = sink
        .records
        .iter()
        .flat_map(|(_, mutation)| match mutation {
            OwnedMutation::Transaction { mutations } => mutations.iter().collect::<Vec<_>>(),
            other => vec![other],
        })
        .collect();
    let login_puts = logged
        .iter()
        .filter(
            |mutation| matches!(mutation, OwnedMutation::LimitPut { key, .. } if key == b"login"),
        )
        .count();
    let login_windows = logged
        .iter()
        .filter(
            |mutation| matches!(mutation, OwnedMutation::ExpireAt { key, .. } if key == b"login"),
        )
        .count();

    assert_eq!((login_puts, login_windows), (5, 1));
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"login")), &mut sink).unwrap(),
        Response::Simple(b"limit")
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"login")), &mut sink).unwrap(),
        wrong_type()
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"login")), &mut sink).unwrap(),
        Response::Integer(60_000)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"other"),
                max: 1,
                window_ms: 1_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(0)
    );
    clock.set(61_000);
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"login"),
                max: 5,
                window_ms: 60_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(4)
    );

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Once {
                key: owned(b"plain"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        wrong_type()
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Once {
                key: owned(b"pay"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Once {
                key: owned(b"pay"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(0)
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"pay")), &mut sink).unwrap(),
        Response::Simple(b"once")
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"pay")), &mut sink).unwrap(),
        wrong_type()
    );
    clock.set(66_000);
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Once {
                key: owned(b"pay"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );

    let mut failed = FailingSink;

    assert!(
        execute_with_sink(
            &mut shard,
            Command::Limit {
                key: owned(b"fresh"),
                max: 2,
                window_ms: 1_000,
                mode: crate::protocol::LimitMode::Fixed,
            },
            &mut failed,
        )
        .is_err()
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Type(owned(b"fresh")), &mut sink).unwrap(),
        Response::Simple(b"none")
    );

    let restored = replayed(&sink);

    assert_eq!(
        restored.value_kind(b"durable"),
        Some(crate::store::ValueKind::Limit)
    );
    assert_eq!(
        restored.expires_at(b"durable"),
        Some(UnixMillis::new(4_000_000_001_000))
    );
    assert_eq!(
        restored.value_kind(b"idem"),
        Some(crate::store::ValueKind::Once)
    );
    assert_eq!(restored.value_kind(b"fresh"), None);
}

#[test]
fn release_during_a_snapshot_keeps_held_leases_and_semaphores_in_the_snapshot() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    let Response::Integer(lease) = execute(
        &mut shard,
        Command::Lease {
            key: owned(b"lock"),
            ttl_ms: 60_000,
        },
    ) else {
        panic!("the lease was granted");
    };
    let Response::Integer(holder) = execute(
        &mut shard,
        Command::Semaphore {
            key: owned(b"sem"),
            max: 2,
            ttl_ms: 60_000,
        },
    ) else {
        panic!("the semaphore was acquired");
    };
    let before = shard.snapshot_entries();
    shard.begin_snapshot(1 << 20).unwrap();

    assert_eq!(
        execute(
            &mut shard,
            Command::Release {
                key: owned(b"sem"),
                token: holder as u64,
            },
        ),
        Response::Integer(1)
    );

    assert_eq!(
        execute(
            &mut shard,
            Command::Release {
                key: owned(b"lock"),
                token: lease as u64,
            },
        ),
        Response::Integer(1)
    );
    assert_ne!(shard.snapshot_entries(), before);
    assert_eq!(shard.finish_snapshot(), before);
}

#[test]
fn renew_moves_a_live_holders_deadline_and_never_creates_one() {
    // RENEW
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();
    let renew = |key: &[u8], token: u64, ttl_ms: u64| Command::Renew {
        key: owned(key),
        token,
        ttl_ms,
    };

    assert_eq!(
        execute_with_sink(&mut shard, renew(b"lock", 1, 5_000), &mut sink).unwrap(),
        Response::Integer(0),
        "a missing key has no holder"
    );
    assert!(sink.records.is_empty());
    assert_eq!(shard.value_kind(b"lock"), None);

    let Response::Integer(first) = execute_with_sink(
        &mut shard,
        Command::Lease {
            key: owned(b"lock"),
            ttl_ms: 1_000,
        },
        &mut sink,
    )
    .unwrap() else {
        panic!("the lease was granted");
    };
    let first = first as u64;

    assert_eq!(first, 1_000 << 6);

    let records = sink.records.len();

    assert_eq!(
        execute_with_sink(&mut shard, renew(b"lock", first + 1, 5_000), &mut sink).unwrap(),
        Response::Integer(0),
        "another token is not the holder"
    );
    assert_eq!(
        sink.records.len(),
        records,
        "a refused renew writes nothing"
    );

    clock.set(1_500);

    assert_eq!(
        execute_with_sink(&mut shard, renew(b"lock", first, 5_000), &mut sink).unwrap(),
        Response::Integer(1)
    );
    assert_eq!(sink.records.len(), records + 1);

    clock.set(3_000);

    macro_rules! lease {
        () => {
            execute(
                &mut shard,
                Command::Lease {
                    key: owned(b"lock"),
                    ttl_ms: 1_000,
                },
            )
        };
    }

    assert_eq!(
        lease!(),
        Response::Bulk(None),
        "still held after the first TTL"
    );

    clock.set(6_500);

    assert_eq!(
        execute_with_sink(&mut shard, renew(b"lock", first, 5_000), &mut sink).unwrap(),
        Response::Integer(0),
        "an expired holder cannot renew"
    );
    match lease!() {
        Response::Integer(token) => assert!(token as u64 > first, "the token sequence moved on"),
        other => panic!("expected a token, got {other:?}"),
    }
}

#[test]
fn renew_extends_one_semaphore_holder_without_taking_a_new_slot() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let sem = |ttl_ms| Command::Semaphore {
        key: owned(b"jobs"),
        max: 2,
        ttl_ms,
    };
    let Response::Integer(first) = execute(&mut shard, sem(1_000)) else {
        panic!("first holder");
    };
    let Response::Integer(second) = execute(&mut shard, sem(10_000)) else {
        panic!("second holder");
    };

    assert_eq!(
        execute(
            &mut shard,
            Command::Renew {
                key: owned(b"jobs"),
                token: first as u64,
                ttl_ms: 10_000,
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, sem(1_000)),
        Response::Bulk(None),
        "renewing did not free or add a slot"
    );

    clock.set(5_000);

    assert!(
        matches!(execute(&mut shard, sem(1_000)), Response::Bulk(None)),
        "the renewed holder outlived its first TTL"
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Release {
                key: owned(b"jobs"),
                token: first as u64,
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Release {
                key: owned(b"jobs"),
                token: second as u64,
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(shard.value_kind(b"jobs"), None);
}

#[test]
fn renew_on_another_type_is_a_wrong_type_error() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    execute(&mut shard, Command::set(owned(b"plain"), owned(b"v")));

    assert_eq!(
        execute(
            &mut shard,
            Command::Renew {
                key: owned(b"plain"),
                token: 1,
                ttl_ms: 1_000,
            },
        ),
        wrong_type()
    );
}

#[test]
fn renew_during_a_snapshot_keeps_held_leases_and_semaphores_in_the_snapshot() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    let Response::Integer(lease) = execute(
        &mut shard,
        Command::Lease {
            key: owned(b"lock"),
            ttl_ms: 60_000,
        },
    ) else {
        panic!("the lease was granted");
    };
    let Response::Integer(holder) = execute(
        &mut shard,
        Command::Semaphore {
            key: owned(b"sem"),
            max: 2,
            ttl_ms: 60_000,
        },
    ) else {
        panic!("the semaphore was acquired");
    };
    let before = shard.snapshot_entries();
    shard.begin_snapshot(1 << 20).unwrap();

    for (key, token) in [(&b"sem"[..], holder as u64), (&b"lock"[..], lease as u64)] {
        assert_eq!(
            execute(
                &mut shard,
                Command::Renew {
                    key: owned(key),
                    token,
                    ttl_ms: 120_000,
                },
            ),
            Response::Integer(1)
        );
    }

    assert_ne!(shard.snapshot_entries(), before);
    assert_eq!(shard.finish_snapshot(), before);
}

#[test]
fn a_released_or_expired_lease_takes_its_key_with_it() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();
    let lease = |key: &'static [u8], ttl_ms| Command::Lease {
        key: owned(key),
        ttl_ms,
    };
    let token = |response: Response<'_>| match response {
        Response::Integer(token) => token as u64,
        other => panic!("expected a token, got {other:?}"),
    };

    let first = token(execute_with_sink(&mut shard, lease(b"lock", 5_000), &mut sink).unwrap());

    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"lock")), &mut sink).unwrap(),
        Response::Integer(5_000)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Release {
                key: owned(b"lock"),
                token: first,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert!(!shard.contains_key(b"lock"));
    assert_eq!(
        sink.records.last().map(|(_, mutation)| mutation),
        Some(&OwnedMutation::Delete {
            keys: vec![b"lock".to_vec()]
        }),
        "a release is recorded as a plain delete"
    );

    // The next holder still gets a larger token without the old key as a floor.
    let second = token(execute_with_sink(&mut shard, lease(b"lock", 100), &mut sink).unwrap());

    assert!(second > first);

    clock.set(1_100);
    assert!(!shard.contains_key(b"lock"));
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"lock")), &mut sink).unwrap(),
        Response::Integer(-2)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SetNx(owned(b"lock"), owned(b"v")),
            &mut sink,
        )
        .unwrap(),
        Response::Integer(1),
        "an expired lease leaves the name free for any type"
    );
}

#[test]
fn a_lease_in_an_older_log_expires_with_its_deadline() {
    // Logs written before a release became a delete hold it as a lease ending at release time.
    let clock = crate::time::TestClock::new(2_000);
    let mut shard = KvShard::with_clock(clock);

    shard.lease_put(b"held", 7, 5_000, "LEASE").unwrap();
    shard.lease_put(b"released", 8, 1_500, "LEASE").unwrap();
    assert!(shard.contains_key(b"held"));
    assert!(!shard.contains_key(b"released"));
    assert_eq!(shard.pttl(b"held"), 3_000);

    shard.lease_put(b"held", 7, 2_000, "RELEASE").unwrap();
    assert!(!shard.contains_key(b"held"));

    let (token, _) = shard.lease_plan(b"held", 10).unwrap().unwrap();

    assert!(token > 8, "replayed tokens still raise the floor");
}
