use super::*;

#[test]
fn expire_ttl_and_persist_follow_redis_integer_replies() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"missing"))),
        Response::Integer(-2)
    );
    execute(&mut shard, Command::set(owned(b"key"), owned(b"value")));
    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"key"))),
        Response::Integer(-1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Expire {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"key"),
                milliseconds: 60_000,
            }
        ),
        Response::Integer(1)
    );
    let ttl = match execute(&mut shard, Command::PTtl(owned(b"key"))) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    };

    assert!(ttl > 0 && ttl <= 60_000);
    assert_eq!(
        execute(&mut shard, Command::Persist(owned(b"key"))),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::PTtl(owned(b"key"))),
        Response::Integer(-1)
    );
}

#[test]
fn expired_keys_remain_invisible_across_command_paths_without_active_sweeps() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(crate::MemoryLimits {
        active_expire_keys: 0,
        ..Default::default()
    });

    execute(&mut shard, Command::set(owned(b"text"), owned(b"old")));
    execute(
        &mut shard,
        Command::HashSet {
            key: owned(b"hash"),
            pairs: vec![(owned(b"field"), owned(b"old"))],
        },
    );
    for key in [b"text".as_slice(), b"hash"] {
        assert!(shard.expire_at(key, crate::time::UnixMillis::new(2_000)));
    }
    clock.set(2_000);

    assert_eq!(execute(&mut shard, Command::DbSize), Response::Integer(2));
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"text"))),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"hash"))),
        Response::Simple(b"none")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Exists(vec![owned(b"text"), owned(b"hash")].into())
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Scan {
                cursor: 0,
                pattern: None,
                count: 10,
                type_name: None,
                slot: None,
            },
        ),
        Response::Scan {
            cursor: 0,
            keys: vec![]
        }
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"hash"),
                field: owned(b"field")
            },
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Move {
                key: owned(b"text"),
                db: 1,
            },
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Rename {
                source: owned(b"text"),
                destination: owned(b"moved")
            },
        ),
        Response::Error(b"ERR no such key")
    );
    assert_eq!(
        execute(&mut shard, Command::GetSet(owned(b"text"), owned(b"new"))),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"text"))),
        Response::Bulk(Some(owned(b"new")))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"hash"),
                pairs: vec![(owned(b"field"), owned(b"new"))],
            },
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"hash"),
                field: owned(b"field")
            }
        ),
        Response::Bulk(Some(owned(b"new")))
    );
    assert_eq!(shard.expiration_stats().expired_keys_passive, 2);
    assert_eq!(shard.expiration_stats().expired_keys_active, 0);
}

#[test]
fn expired_hash_fields_are_hidden_before_a_maintenance_tick() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(crate::MemoryLimits {
        active_expire_keys: 0,
        ..Default::default()
    });
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"hash"),
                pairs: vec![
                    (owned(b"stale"), owned(b"old")),
                    (owned(b"live"), owned(b"value"))
                ],
            }
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            field_expire(b"hash", 2_000, FieldExpireCondition::Always, &[b"stale"])
        ),
        integers(&[1])
    );
    clock.set(2_000);
    assert_eq!(
        execute(
            &mut shard,
            Command::HashScan {
                key: owned(b"hash"),
                cursor: 0,
                pattern: None,
                count: 10,
            },
        ),
        Response::Scan {
            cursor: 0,
            keys: vec![owned(b"live"), owned(b"value")],
        }
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"hash"),
                field: owned(b"stale")
            }
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, field_ttl(b"hash", &[b"stale", b"live"])),
        integers(&[-2, -1])
    );
    assert_eq!(
        execute(&mut shard, Command::HashLen(owned(b"hash"))),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"hash"),
                pairs: vec![(owned(b"stale"), owned(b"new"))],
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"hash"),
                field: owned(b"stale")
            }
        ),
        Response::Bulk(Some(owned(b"new")))
    );
}

#[test]
fn snapshot_restore_does_not_resurrect_expired_hash_fields() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    execute(
        &mut shard,
        Command::HashSet {
            key: owned(b"hash"),
            pairs: vec![
                (owned(b"stale"), owned(b"old")),
                (owned(b"live"), owned(b"value")),
            ],
        },
    );
    execute(
        &mut shard,
        field_expire(b"hash", 2_000, FieldExpireCondition::Always, &[b"stale"]),
    );
    shard.begin_snapshot(4_096).unwrap();
    shard.capture_snapshot_key(b"hash");
    clock.set(2_000);

    for entries in [shard.snapshot_entries(), shard.finish_snapshot()] {
        let mut restored = KvShard::with_clock(clock.clone());
        for entry in entries {
            restored.restore_snapshot_entry(entry).unwrap();
        }
        assert_eq!(
            execute(
                &mut restored,
                Command::HashGet {
                    key: owned(b"hash"),
                    field: owned(b"stale"),
                }
            ),
            Response::Bulk(None)
        );
        assert_eq!(
            execute(
                &mut restored,
                Command::HashGet {
                    key: owned(b"hash"),
                    field: owned(b"live"),
                }
            ),
            Response::Bulk(Some(owned(b"value")))
        );
    }
}

#[test]
#[allow(clippy::type_complexity)]
fn expired_values_behave_like_missing_keys_across_command_families() {
    let cases: &[(&[&[u8]], &[&[u8]])] = &[
        (&[b"GET", b"key"], &[b"SET", b"key", b"old"]),
        (&[b"GETEX", b"key", b"5000"], &[b"SET", b"key", b"old"]),
        (&[b"MGET", b"key"], &[b"SET", b"key", b"old"]),
        (&[b"STRLEN", b"key"], &[b"SET", b"key", b"old"]),
        (&[b"GETSET", b"key", b"new"], &[b"SET", b"key", b"old"]),
        (&[b"APPEND", b"key", b"new"], &[b"SET", b"key", b"old"]),
        (&[b"INCR", b"key"], &[b"SET", b"key", b"42"]),
        (&[b"DEL", b"key"], &[b"SET", b"key", b"old"]),
        (&[b"LPUSH", b"key", b"new"], &[b"LPUSH", b"key", b"old"]),
        (&[b"LPOP", b"key"], &[b"LPUSH", b"key", b"old"]),
        (
            &[b"LRANGE", b"key", b"0", b"-1"],
            &[b"LPUSH", b"key", b"old"],
        ),
        (&[b"LINDEX", b"key", b"0"], &[b"LPUSH", b"key", b"old"]),
        (&[b"SADD", b"key", b"new"], &[b"SADD", b"key", b"old"]),
        (&[b"SMEMBERS", b"key"], &[b"SADD", b"key", b"old"]),
        (&[b"SPOP", b"key"], &[b"SADD", b"key", b"old"]),
        (&[b"SUNION", b"key"], &[b"SADD", b"key", b"old"]),
        (
            &[b"HSET", b"key", b"f", b"new"],
            &[b"HSET", b"key", b"f", b"old"],
        ),
        (&[b"HGETALL", b"key"], &[b"HSET", b"key", b"f", b"old"]),
        (&[b"HKEYS", b"key"], &[b"HSET", b"key", b"f", b"old"]),
        (&[b"HVALS", b"key"], &[b"HSET", b"key", b"f", b"old"]),
        (&[b"HMGET", b"key", b"f"], &[b"HSET", b"key", b"f", b"old"]),
        (&[b"HDEL", b"key", b"f"], &[b"HSET", b"key", b"f", b"old"]),
        (&[b"HSCAN", b"key", b"0"], &[b"HSET", b"key", b"f", b"old"]),
        (
            &[b"ZADD", b"key", b"2", b"new"],
            &[b"ZADD", b"key", b"1", b"old"],
        ),
        (
            &[b"ZRANGE", b"key", b"0", b"-1"],
            &[b"ZADD", b"key", b"1", b"old"],
        ),
        (
            &[b"ZSCORE", b"key", b"old"],
            &[b"ZADD", b"key", b"1", b"old"],
        ),
        (&[b"ZPOPMIN", b"key"], &[b"ZADD", b"key", b"1", b"old"]),
        (
            &[b"GEOADD", b"key", b"13", b"42", b"new"],
            &[b"GEOADD", b"key", b"12", b"41", b"old"],
        ),
        (
            &[b"GEOPOS", b"key", b"old"],
            &[b"GEOADD", b"key", b"12", b"41", b"old"],
        ),
        (
            &[b"XADD", b"key", b"1-0", b"f", b"new"],
            &[b"XADD", b"key", b"1-0", b"f", b"old"],
        ),
        (&[b"XLEN", b"key"], &[b"XADD", b"key", b"1-0", b"f", b"old"]),
        (
            &[b"XRANGE", b"key", b"-", b"+"],
            &[b"XADD", b"key", b"1-0", b"f", b"old"],
        ),
        (&[b"BF.ADD", b"key", b"new"], &[b"BF.ADD", b"key", b"old"]),
        (
            &[b"BF.EXISTS", b"key", b"old"],
            &[b"BF.ADD", b"key", b"old"],
        ),
        (&[b"LEASE", b"key", b"5000"], &[b"SET", b"key", b"old"]),
        (
            &[b"LIMIT", b"key", b"2", b"5000"],
            &[b"SET", b"key", b"old"],
        ),
        (&[b"ONCE", b"key", b"5000"], &[b"SET", b"key", b"old"]),
        (
            &[b"IDEM", b"key", b"BEGIN", b"fingerprint", b"owner", b"5000"],
            &[b"SET", b"key", b"old"],
        ),
        (
            &[b"SEMAPHORE", b"key", b"2", b"5000"],
            &[b"SET", b"key", b"old"],
        ),
    ];

    for &(words, setup) in cases {
        let mut missing = KvShard::with_clock(crate::time::TestClock::new(2_000));
        let clock = crate::time::TestClock::new(1_000);
        let mut expired = KvShard::with_clock(clock.clone());
        expired.apply_memory_limits(crate::MemoryLimits {
            active_expire_keys: 0,
            ..Default::default()
        });
        let parse = |words: &[&[u8]]| {
            crate::protocol::parse_arguments(words.iter().map(|word| owned(word)).collect())
                .unwrap_or_else(|error| panic!("{}: {error}", String::from_utf8_lossy(words[0])))
        };
        execute(&mut expired, parse(setup));
        assert!(expired.expire_at(b"key", crate::time::UnixMillis::new(2_000)));
        clock.set(2_000);

        let expected = execute(&mut missing, parse(words));
        let actual = execute(&mut expired, parse(words));
        assert_eq!(actual, expected, "{}", String::from_utf8_lossy(words[0]));
        assert_eq!(
            expired.value_kind(b"key"),
            missing.value_kind(b"key"),
            "{}",
            String::from_utf8_lossy(words[0])
        );
    }
}

#[test]
fn hot_commands_treat_unswept_expired_keys_as_missing() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    shard.apply_memory_limits(crate::MemoryLimits {
        active_expire_keys: 0,
        ..Default::default()
    });

    for key in [b"get".as_slice(), b"delete", b"increment", b"set"] {
        execute(&mut shard, Command::set(owned(key), owned(b"old")));
        assert!(shard.expire_at(key, crate::time::UnixMillis::new(2_000)));
    }
    clock.set(2_000);

    for (key, command, expected) in [
        (
            b"get".as_slice(),
            HotCommand::Get(b"get"),
            b"$-1\r\n".as_slice(),
        ),
        (b"delete", HotCommand::Delete(b"delete"), b":0\r\n"),
        (
            b"increment",
            HotCommand::Increment(b"increment", 1),
            b":1\r\n",
        ),
        (b"set", HotCommand::Set(b"set", b"new", None), b"+OK\r\n"),
    ] {
        let mut output = Vec::new();
        execute_hot_into(&mut shard, command, &mut output).unwrap();
        assert_eq!(output, expected, "{}", String::from_utf8_lossy(key));
    }
}

#[test]
fn command_path_expire_cycle_counts_active_and_lookup_counts_passive() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    execute(&mut shard, Command::set(owned(b"a"), owned(b"1")));
    execute(&mut shard, Command::set(owned(b"b"), owned(b"2")));
    assert_eq!(
        execute(
            &mut shard,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"a"),
                unix_millis: 1_500,
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"b"),
                unix_millis: 1_500,
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(shard.expiration_stats().keys_with_ttl, 2);

    clock.set(1_500);
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"a"))),
        Response::Bulk(None)
    );
    let after_get = shard.expiration_stats();

    assert_eq!(after_get.expired_keys_active, 2);
    assert_eq!(after_get.expired_keys_passive, 0);
    assert_eq!(after_get.keys_with_ttl, 0);

    execute(&mut shard, Command::set(owned(b"c"), owned(b"3")));
    assert_eq!(
        execute(
            &mut shard,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"c"),
                unix_millis: 2_000,
            }
        ),
        Response::Integer(1)
    );
    clock.set(2_000);
    assert!(!shard.delete(b"c"));
    let after_delete = shard.expiration_stats();

    assert_eq!(after_delete.expired_keys_passive, 1);
    assert_eq!(after_delete.expired_keys_active, 2);
    assert_eq!(after_delete.keys_with_ttl, 0);
}

fn command_of(parts: &[&[u8]]) -> Command<'static> {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        frame.extend(format!("${}\r\n", part.len()).bytes());
        frame.extend_from_slice(part);
        frame.extend_from_slice(b"\r\n");
    }

    let (command, _) = parse_command(&frame).unwrap().expect("complete frame");

    command.into_owned()
}

fn logged(
    shard: &mut KvShard<crate::time::TestClock>,
    sink: &mut DatabaseLog,
    parts: &[&[u8]],
) -> Response<'static> {
    let Ok(response) = execute_in_database(shard, command_of(parts), sink, &mut (), 0);

    response.into_owned()
}

/// Replays like startup does: with expiry paused, so expired keys are still stored. It
/// stays paused, so the result shows what was stored, not what the test clock expired.
fn recovered(sink: &DatabaseLog) -> KvShard {
    let mut shard = KvShard::new();

    shard.pause_expiration_for_replay();
    for (database, mutation) in &sink.records {
        crate::persistence::apply_mutation(&mut shard, *database, mutation).unwrap();
    }
    shard
}

fn logging_shard(
    active_expire_keys: usize,
) -> (crate::time::TestClock, KvShard<crate::time::TestClock>) {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    shard.apply_memory_limits(crate::MemoryLimits {
        active_expire_keys,
        ..Default::default()
    });
    shard.log_removals(true);
    (clock, shard)
}

#[test]
fn write_to_an_expired_key_logs_its_delete_before_the_write() {
    let (clock, mut shard) = logging_shard(0);
    let mut log = DatabaseLog::default();

    logged(&mut shard, &mut log, &[b"RPUSH", b"k", b"a"]);
    logged(&mut shard, &mut log, &[b"PEXPIREAT", b"k", b"2000"]);
    clock.set(2_000);
    assert_eq!(
        logged(&mut shard, &mut log, &[b"SADD", b"k", b"m"]),
        Response::Integer(1)
    );

    let deleted = log.records.iter().position(|(_, mutation)| {
        matches!(mutation, OwnedMutation::Delete { keys } if keys == &[b"k".to_vec()])
    });
    assert_eq!(deleted, Some(log.records.len() - 2));

    let mut restored = recovered(&log);
    assert_eq!(
        unordered(execute(&mut restored, Command::SetMembers(owned(b"k")))),
        Response::Set(vec![owned(b"m")])
    );
}

#[test]
fn keys_removed_by_active_or_read_expiry_are_logged_before_reuse() {
    for active in [true, false] {
        let (clock, mut shard) = logging_shard(if active { 20 } else { 0 });
        let mut log = DatabaseLog::default();

        logged(&mut shard, &mut log, &[b"RPUSH", b"q", b"a"]);
        logged(&mut shard, &mut log, &[b"PEXPIREAT", b"q", b"2000"]);
        clock.set(2_000);
        if active {
            assert_eq!(shard.expire_cycle(usize::MAX), 1);
        } else {
            assert_eq!(
                logged(&mut shard, &mut log, &[b"LLEN", b"q"]),
                Response::Integer(0)
            );
        }
        logged(&mut shard, &mut log, &[b"RPUSH", b"q", b"b"]);
        logged(&mut shard, &mut log, &[b"EXPIRE", b"q", b"60"]);

        let mut restored = recovered(&log);
        assert_eq!(
            execute(&mut restored, command_of(&[b"LLEN", b"q"])),
            Response::Integer(1),
            "active expiry: {active}"
        );
    }
}

#[test]
fn a_rolled_back_transaction_requeues_the_expired_deletes_it_buffered() {
    let (clock, mut shard) = logging_shard(0);
    let mut log = DatabaseLog::default();

    logged(&mut shard, &mut log, &[b"RPUSH", b"k", b"a"]);
    logged(&mut shard, &mut log, &[b"RPUSH", b"other", b"a"]);
    logged(&mut shard, &mut log, &[b"PEXPIREAT", b"k", b"2000"]);
    logged(&mut shard, &mut log, &[b"PEXPIREAT", b"other", b"2000"]);
    clock.set(2_000);
    // `other` is reaped by a read before the transaction; both deletes go into the batch.
    logged(&mut shard, &mut log, &[b"LLEN", b"other"]);

    let refused = execute_transaction(
        &mut shard,
        &mut FailingSink,
        &mut (),
        1,
        vec![command_of(&[b"SADD", b"k", b"m"])],
        0,
    );
    assert!(refused.is_err());

    logged(&mut shard, &mut log, &[b"SADD", b"k", b"z"]);
    logged(&mut shard, &mut log, &[b"SADD", b"other", b"z"]);

    let mut restored = recovered(&log);
    for key in [b"k".as_slice(), b"other"] {
        assert_eq!(
            unordered(execute(&mut restored, Command::SetMembers(owned(key)))),
            Response::Set(vec![owned(b"z")])
        );
    }
}

#[test]
fn a_committed_transaction_carries_the_expired_delete_before_its_write() {
    let (clock, mut shard) = logging_shard(0);
    let mut log = DatabaseLog::default();

    logged(&mut shard, &mut log, &[b"RPUSH", b"t", b"a"]);
    logged(&mut shard, &mut log, &[b"PEXPIREAT", b"t", b"2000"]);
    clock.set(2_000);
    let Ok(reply) = execute_transaction(
        &mut shard,
        &mut log,
        &mut (),
        1,
        vec![command_of(&[b"SADD", b"t", b"m"])],
        0,
    );
    assert_eq!(reply, Response::Exec(vec![Response::Integer(1)]));
    assert!(!shard.has_unlogged_removals());

    let mut restored = recovered(&log);
    assert_eq!(
        unordered(execute(&mut restored, Command::SetMembers(owned(b"t")))),
        Response::Set(vec![owned(b"m")])
    );
}

#[test]
fn a_replica_hides_keys_past_its_own_clock_but_keeps_them_for_the_primary() {
    let clock = crate::time::TestClock::new(1_000);
    let mut replica = KvShard::with_clock(clock.clone());
    let mut empty = KvShard::with_clock(clock.clone());

    replica.follow_primary(true);
    let writes: &[&[&[u8]]] = &[
        &[b"SET", b"s", b"value"],
        &[b"RPUSH", b"l", b"a", b"b"],
        &[b"SADD", b"set", b"a", b"b"],
        &[b"HSET", b"h", b"f", b"v"],
        &[b"ZADD", b"z", b"1", b"m"],
        &[b"XADD", b"x", b"1-1", b"f", b"v"],
        &[b"XGROUP", b"CREATE", b"x", b"g", b"0"],
        &[b"PFADD", b"p", b"a"],
        &[b"JSON.SET", b"j", b"$", b"{\"a\":1}"],
        &[b"BF.ADD", b"b", b"a"],
    ];

    for parts in writes {
        let reply = execute(&mut replica, command_of(parts)).into_owned();

        assert!(!matches!(reply, Response::Error(_)), "{parts:?}: {reply:?}");
    }

    let keys: [&[u8]; 9] = [b"s", b"l", b"set", b"h", b"z", b"x", b"p", b"j", b"b"];

    for key in keys {
        assert!(replica.expire_at(key, crate::time::UnixMillis::new(2_000)));
    }
    clock.set(7_000);

    let reads: &[&[&[u8]]] = &[
        &[b"GET", b"s"],
        &[b"STRLEN", b"s"],
        &[b"GETRANGE", b"s", b"0", b"-1"],
        &[b"MGET", b"s", b"l"],
        &[b"GETBIT", b"s", b"1"],
        &[b"BITCOUNT", b"s"],
        &[
            b"EXISTS", b"s", b"l", b"set", b"h", b"z", b"x", b"p", b"j", b"b",
        ],
        &[b"LRANGE", b"l", b"0", b"-1"],
        &[b"LLEN", b"l"],
        &[b"LINDEX", b"l", b"0"],
        &[b"LPOS", b"l", b"a"],
        &[b"SMEMBERS", b"set"],
        &[b"SCARD", b"set"],
        &[b"SISMEMBER", b"set", b"a"],
        &[b"SRANDMEMBER", b"set"],
        &[b"SINTER", b"set"],
        &[b"SUNION", b"set"],
        &[b"HGETALL", b"h"],
        &[b"HGET", b"h", b"f"],
        &[b"HLEN", b"h"],
        &[b"HEXISTS", b"h", b"f"],
        &[b"HSTRLEN", b"h", b"f"],
        &[b"ZRANGE", b"z", b"0", b"-1"],
        &[b"ZSCORE", b"z", b"m"],
        &[b"ZCARD", b"z"],
        &[b"ZRANK", b"z", b"m"],
        &[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf"],
        &[b"XLEN", b"x"],
        &[b"XRANGE", b"x", b"-", b"+"],
        &[b"XREVRANGE", b"x", b"+", b"-"],
        &[b"XREAD", b"COUNT", b"1", b"STREAMS", b"x", b"0"],
        &[b"XINFO", b"STREAM", b"x"],
        &[b"XINFO", b"GROUPS", b"x"],
        &[b"XPENDING", b"x", b"g"],
        &[b"PFCOUNT", b"p"],
        &[b"JSON.GET", b"j"],
        &[b"BF.EXISTS", b"b", b"a"],
        &[b"SCAN", b"0", b"COUNT", b"100"],
        &[b"KEYS", b"*"],
        &[b"RANDOMKEY"],
        &[b"DUMP", b"s"],
        &[b"OBJECT", b"ENCODING", b"l"],
    ];

    for parts in reads {
        let seen = execute(&mut replica, command_of(parts)).into_owned();
        let absent = execute(&mut empty, command_of(parts)).into_owned();

        assert_eq!(
            seen,
            absent,
            "{:?}",
            String::from_utf8_lossy(&parts.concat())
        );
    }

    for key in keys {
        for parts in [
            [b"TYPE".as_slice(), key],
            [b"TTL", key],
            [b"PTTL", key],
            [b"EXPIRETIME", key],
        ] {
            let seen = execute(&mut replica, command_of(&parts)).into_owned();
            let absent = execute(&mut empty, command_of(&parts)).into_owned();

            assert_eq!(
                seen,
                absent,
                "{:?}",
                String::from_utf8_lossy(&parts.concat())
            );
        }
    }

    // Nothing was removed, by reads or by active expiry: DBSIZE counts them, as on a
    // Redis replica, and a later deadline from the primary still applies.
    assert_eq!(replica.expire_cycle(usize::MAX), 0);
    assert_eq!(execute(&mut replica, Command::DbSize), Response::Integer(9));
    assert_eq!(
        replica.snapshot_entries().len(),
        9,
        "a restart keeps them too"
    );
    let replay = replica.pause_expiration_for_replay();

    assert!(replica.expire_at(b"s", crate::time::UnixMillis::new(60_000)));
    assert!(replica.delete(b"l"));
    replica.resume_expiration_after_replay(replay);
    assert_eq!(
        execute(&mut replica, command_of(&[b"GET", b"s"])),
        Response::Bulk(Some(Cow::Borrowed(b"value")))
    );
    assert_eq!(execute(&mut replica, Command::DbSize), Response::Integer(8));
    assert_eq!(replica.expiration_stats().expired_keys, 0);

    // Promoted, its own clock removes them again.
    replica.follow_primary(false);
    assert_eq!(replica.expire_cycle(usize::MAX), 7);
    assert_eq!(execute(&mut replica, Command::DbSize), Response::Integer(1));
}

#[test]
fn a_hash_whose_fields_all_expired_is_missing_to_every_command() {
    let (clock, mut shard) = logging_shard(16);
    let mut log = DatabaseLog::default();

    for key in [&b"h"[..], b"kept"] {
        logged(
            &mut shard,
            &mut log,
            &[b"HSET", key, b"f", b"v", b"g", b"w"],
        );
        logged(
            &mut shard,
            &mut log,
            &[b"HPEXPIREAT", key, b"2000", b"FIELDS", b"2", b"f", b"g"],
        );
    }
    // One field of `kept` outlives the others.
    logged(
        &mut shard,
        &mut log,
        &[b"HPEXPIREAT", b"kept", b"9000", b"FIELDS", b"1", b"g"],
    );
    clock.set(2_000);

    // Reads see a missing key, before any sweep or hash command removed it.
    assert_eq!(
        execute(&mut shard, command_of(&[b"TYPE", b"h"])),
        Response::Simple(b"none")
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"EXISTS", b"h", b"kept"])),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"GET", b"h"])),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, command_of(&[b"TYPE", b"kept"])),
        Response::Simple(b"hash")
    );
    let (_, keys) = shard.scan(0, 100, None, None);
    assert_eq!(keys, vec![&b"kept"[..]]);

    // A write of another type finds no hash, and the delete is logged before it.
    assert_eq!(
        logged(&mut shard, &mut log, &[b"LPUSH", b"h", b"x"]),
        Response::Integer(1)
    );
    let deleted = log.records.iter().position(|(_, mutation)| {
        matches!(mutation, OwnedMutation::Delete { keys } if keys == &[b"h".to_vec()])
    });
    assert_eq!(deleted, Some(log.records.len() - 2));

    let mut restored = recovered(&log);
    assert_eq!(
        execute(&mut restored, command_of(&[b"LLEN", b"h"])),
        Response::Integer(1)
    );
}

#[test]
fn the_field_sweep_drains_a_backlog_of_expired_hashes_in_a_few_ticks() {
    let (clock, mut shard) = logging_shard(16);
    let mut log = DatabaseLog::default();

    for index in 0..500 {
        let key = format!("h{index}").into_bytes();

        logged(&mut shard, &mut log, &[b"HSET", &key, b"f", b"v"]);
        logged(
            &mut shard,
            &mut log,
            &[b"HPEXPIREAT", &key, b"2000", b"FIELDS", b"1", b"f"],
        );
    }
    clock.set(2_000);

    // More than `active_expire_keys` hashes per tick while its time budget lasts.
    let first = shard.expire_hash_fields_due();
    let mut ticks = 1;

    assert!(first > 16, "one tick removed only {first} hashes");
    while !shard.is_empty() {
        assert!(shard.expire_hash_fields_due() > 0);
        ticks += 1;
    }
    assert!(ticks < 500 / 16, "{ticks} ticks");
    assert!(
        shard.has_unlogged_removals(),
        "the removals are logged as deletes"
    );
}

/// Runs `parts` in database `db` of a logging shard.
fn logged_in(
    shard: &mut KvShard<crate::time::TestClock>,
    sink: &mut DatabaseLog,
    db: u8,
    parts: &[&[u8]],
) -> Response<'static> {
    let Ok(response) = execute_in_database(shard, command_of(parts), sink, &mut (), db);

    response.into_owned()
}

/// What `key` in database `db` holds, as its type and its content.
fn describe<C: Clock>(shard: &mut KvShard<C>, db: u8, key: &[u8]) -> String {
    let mut read = |parts: &[&[u8]]| {
        let Ok(response) =
            execute_in_database(shard, command_of(parts), &mut NoopMutationSink, &mut (), db);

        format!("{:?}", unordered(response))
    };
    let kind = read(&[b"TYPE", key]);
    let content = if kind.contains("string") {
        read(&[b"GET", key])
    } else if kind.contains("list") {
        read(&[b"LRANGE", key, b"0", b"-1"])
    } else if kind.contains("zset") {
        read(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"])
    } else if kind.contains("set") {
        read(&[b"SMEMBERS", key])
    } else if kind.contains("hash") {
        read(&[b"HGETALL", key])
    } else if kind.contains("none") {
        String::new()
    } else {
        read(&[b"DUMP", key])
    };

    format!("{kind} {content}")
}

/// Replays `log` with expiry paused, as startup does, then reads with the clock running.
fn replayed_now(log: &DatabaseLog) -> KvShard {
    let mut shard = KvShard::new();
    let previous = shard.pause_expiration_for_replay();

    for (database, mutation) in &log.records {
        crate::persistence::apply_mutation(&mut shard, *database, mutation)
            .unwrap_or_else(|error| panic!("replay failed at {mutation:?}: {error}"));
    }
    shard.resume_expiration_after_replay(previous);
    shard
}

#[test]
fn writes_onto_a_hash_whose_fields_all_expired_replay_to_the_same_data() {
    type Step = (u8, &'static [&'static [u8]]);
    type Keys = &'static [(u8, &'static [u8])];
    // Each case: dead hashes to plant as (database, key), the steps, and the keys to compare.
    let cases: Vec<(Keys, Vec<Step>, Keys)> = vec![
        (
            &[(1, b"k")],
            vec![(0, &[b"SET", b"k", b"s"]), (0, &[b"MOVE", b"k", b"1"])],
            &[(0, b"k"), (1, b"k")],
        ),
        (
            &[(0, b"d")],
            vec![(0, &[b"SET", b"k", b"s"]), (0, &[b"COPY", b"k", b"d"])],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![(0, &[b"SET", b"k", b"s"]), (0, &[b"RENAME", b"k", b"d"])],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"SADD", b"k", b"m"]),
                (0, &[b"SMOVE", b"k", b"d", b"m"]),
            ],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"RPUSH", b"k", b"a"]),
                (0, &[b"LMOVE", b"k", b"d", b"LEFT", b"LEFT"]),
            ],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"SADD", b"k", b"m"]),
                (0, &[b"SDIFFSTORE", b"d", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"SADD", b"k", b"m"]),
                (0, &[b"SUNIONSTORE", b"d", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"ZADD", b"k", b"1", b"m"]),
                (0, &[b"ZUNIONSTORE", b"d", b"1", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"ZADD", b"k", b"1", b"m"]),
                (0, &[b"ZINTERSTORE", b"d", b"1", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"SADD", b"k", b"m"]),
                (0, &[b"SINTERSTORE", b"d", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"SET", b"k", b"s"]),
                (0, &[b"BITOP", b"OR", b"d", b"k"]),
            ],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![(0, &[b"PFADD", b"k", b"a"]), (0, &[b"PFMERGE", b"d", b"k"])],
            &[(0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![
                (0, &[b"RPUSH", b"k", b"a"]),
                (0, &[b"RPOPLPUSH", b"k", b"d"]),
            ],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"d")],
            vec![(0, &[b"SET", b"k", b"s"]), (0, &[b"RENAMENX", b"k", b"d"])],
            &[(0, b"k"), (0, b"d")],
        ),
        (
            &[(0, b"k")],
            vec![(0, &[b"LPUSH", b"k", b"x"])],
            &[(0, b"k")],
        ),
        (&[(0, b"k")], vec![(0, &[b"INCR", b"k"])], &[(0, b"k")]),
        (
            &[(0, b"k")],
            vec![(0, &[b"HSET", b"k", b"g", b"w"])],
            &[(0, b"k")],
        ),
    ];

    for (dead, steps, compare) in cases {
        let (clock, mut shard) = logging_shard(16);
        let mut log = DatabaseLog::default();

        for (db, key) in dead {
            logged_in(&mut shard, &mut log, *db, &[b"HSET", key, b"f", b"v"]);
            logged_in(
                &mut shard,
                &mut log,
                *db,
                &[b"HPEXPIREAT", key, b"2000", b"FIELDS", b"1", b"f"],
            );
        }
        clock.set(2_000);
        for (db, parts) in &steps {
            logged_in(&mut shard, &mut log, *db, parts);
        }
        // Removals queued by the last command are logged before the next one.
        logged_in(&mut shard, &mut log, 0, &[b"PING"]);

        let mut restored = replayed_now(&log);

        for (db, key) in compare {
            assert_eq!(
                describe(&mut restored, *db, key),
                describe(&mut shard, *db, key),
                "{steps:?}: db {db} key {}",
                String::from_utf8_lossy(key)
            );
        }
    }
}

#[test]
fn move_in_a_transaction_onto_an_expired_target_replays() {
    // Scripts may not run MOVE, so only a transaction reaches another database.
    for dead_hash in [true, false] {
        let (clock, mut shard) = logging_shard(16);
        let mut log = DatabaseLog::default();

        // The target in database 1: a hash whose fields expire, or a key with a TTL.
        if dead_hash {
            logged_in(&mut shard, &mut log, 1, &[b"HSET", b"k", b"f", b"v"]);
            logged_in(
                &mut shard,
                &mut log,
                1,
                &[b"HPEXPIREAT", b"k", b"2000", b"FIELDS", b"1", b"f"],
            );
        } else {
            logged_in(&mut shard, &mut log, 1, &[b"SET", b"k", b"old"]);
            logged_in(&mut shard, &mut log, 1, &[b"PEXPIREAT", b"k", b"2000"]);
        }
        logged_in(&mut shard, &mut log, 0, &[b"SET", b"k", b"s"]);
        clock.set(2_000);

        let Ok(reply) = execute_transaction(
            &mut shard,
            &mut log,
            &mut (),
            1,
            vec![
                command_of(&[b"MOVE", b"k", b"1"]),
                command_of(&[b"SET", b"{k}after", b"v"]),
            ],
            0,
        );

        assert_eq!(
            reply,
            Response::Exec(vec![Response::Integer(1), Response::Simple(b"OK")])
        );
        logged_in(&mut shard, &mut log, 0, &[b"PING"]);

        let mut restored = replayed_now(&log);

        for (db, key) in [(0, &b"k"[..]), (1, b"k"), (0, b"{k}after")] {
            assert_eq!(
                describe(&mut restored, db, key),
                describe(&mut shard, db, key),
                "hash {dead_hash}, db {db}"
            );
        }
    }
}

#[test]
fn every_write_onto_an_expired_key_replays_to_the_same_data() {
    type Case = (&'static [&'static [u8]], Vec<&'static [&'static [u8]]>);
    // Each case: keys planted expired, then the writes. The source keys the writes read
    // are created by the writes before them.
    let cases: Vec<Case> = vec![
        (&[b"d"], vec![&[b"SET", b"d", b"v"]]),
        (&[b"d"], vec![&[b"SETNX", b"d", b"v"]]),
        (
            &[b"d"],
            vec![&[b"SET", b"d", b"v", b"PXAT", b"99999999999999"]],
        ),
        (&[b"d"], vec![&[b"GETSET", b"d", b"v"]]),
        (&[b"d"], vec![&[b"APPEND", b"d", b"v"]]),
        (&[b"d"], vec![&[b"SETRANGE", b"d", b"2", b"v"]]),
        (&[b"d"], vec![&[b"SETBIT", b"d", b"7", b"1"]]),
        (
            &[b"d"],
            vec![&[b"BITFIELD", b"d", b"SET", b"u8", b"0", b"7"]],
        ),
        (&[b"d"], vec![&[b"INCRBY", b"d", b"3"]]),
        (&[b"d"], vec![&[b"INCRBYFLOAT", b"d", b"1.5"]]),
        (&[b"d"], vec![&[b"DECR", b"d"]]),
        (&[b"d", b"e"], vec![&[b"MSET", b"d", b"v", b"e", b"w"]]),
        (&[b"d", b"e"], vec![&[b"MSETNX", b"d", b"v", b"e", b"w"]]),
        (&[b"d"], vec![&[b"RPUSH", b"d", b"a"]]),
        (&[b"d"], vec![&[b"RPUSHX", b"d", b"a"]]),
        (&[b"d"], vec![&[b"SADD", b"d", b"m"]]),
        (&[b"d"], vec![&[b"ZADD", b"d", b"1", b"m"]]),
        (&[b"d"], vec![&[b"ZINCRBY", b"d", b"1", b"m"]]),
        (&[b"d"], vec![&[b"HSETNX", b"d", b"f", b"v"]]),
        (&[b"d"], vec![&[b"HINCRBY", b"d", b"f", b"2"]]),
        (&[b"d"], vec![&[b"HINCRBYFLOAT", b"d", b"f", b"2.5"]]),
        (&[b"d"], vec![&[b"PFADD", b"d", b"a"]]),
        (&[b"d"], vec![&[b"XADD", b"d", b"1-1", b"f", b"v"]]),
        (&[b"d"], vec![&[b"JSON.SET", b"d", b"$", b"[1]"]]),
        (&[b"d"], vec![&[b"BF.ADD", b"d", b"a"]]),
        (&[b"d"], vec![&[b"GEOADD", b"d", b"1", b"1", b"m"]]),
        (
            &[b"d"],
            vec![&[b"SET", b"k", b"v"], &[b"COPY", b"k", b"d", b"REPLACE"]],
        ),
        (
            &[b"d"],
            vec![&[b"RPUSH", b"k", b"a"], &[b"COPY", b"k", b"d"]],
        ),
        (
            &[b"d"],
            vec![&[b"RPUSH", b"k", b"a"], &[b"RENAME", b"k", b"d"]],
        ),
        (
            &[b"d"],
            vec![
                &[b"RPUSH", b"k", b"a"],
                &[b"BLMOVE", b"k", b"d", b"LEFT", b"RIGHT", b"0"],
            ],
        ),
        (
            &[b"d"],
            vec![&[b"SADD", b"k", b"m"], &[b"SMOVE", b"k", b"d", b"m"]],
        ),
        (
            &[b"d"],
            vec![
                &[b"ZADD", b"k", b"1", b"m"],
                &[b"ZDIFFSTORE", b"d", b"1", b"k"],
            ],
        ),
        (
            &[b"d"],
            vec![
                &[b"GEOADD", b"k", b"1", b"1", b"m"],
                &[
                    b"GEOSEARCHSTORE",
                    b"d",
                    b"k",
                    b"FROMLONLAT",
                    b"1",
                    b"1",
                    b"BYRADIUS",
                    b"10",
                    b"km",
                ],
            ],
        ),
        (
            &[b"d"],
            vec![
                &[b"GEOADD", b"k", b"1", b"1", b"m"],
                &[b"GEORADIUS", b"k", b"1", b"1", b"10", b"km", b"STORE", b"d"],
            ],
        ),
        (
            &[b"d"],
            vec![
                &[b"GEOADD", b"k", b"1", b"1", b"m"],
                &[
                    b"GEORADIUSBYMEMBER",
                    b"k",
                    b"m",
                    b"10",
                    b"km",
                    b"STOREDIST",
                    b"d",
                ],
            ],
        ),
    ];

    for dead_hash in [true, false] {
        for (dead, steps) in &cases {
            let (clock, mut shard) = logging_shard(0);
            let mut log = DatabaseLog::default();

            for key in *dead {
                if dead_hash {
                    logged(&mut shard, &mut log, &[b"HSET", key, b"f", b"v"]);
                    logged(
                        &mut shard,
                        &mut log,
                        &[b"HPEXPIREAT", key, b"2000", b"FIELDS", b"1", b"f"],
                    );
                } else {
                    logged(&mut shard, &mut log, &[b"SET", key, b"old"]);
                    logged(&mut shard, &mut log, &[b"PEXPIREAT", key, b"2000"]);
                }
            }
            clock.set(2_000);
            for parts in steps {
                logged(&mut shard, &mut log, parts);
            }
            logged(&mut shard, &mut log, &[b"PING"]);

            let mut restored = replayed_now(&log);

            for key in dead.iter().chain([&&b"k"[..]]) {
                assert_eq!(
                    describe(&mut restored, 0, key),
                    describe(&mut shard, 0, key),
                    "hash {dead_hash}, {}: key {}",
                    String::from_utf8_lossy(&steps.concat().join(&b' ')),
                    String::from_utf8_lossy(key)
                );
            }
        }
    }
}

#[test]
fn a_replica_keeps_a_hash_whose_fields_all_expired_hidden_after_the_field_sweep() {
    let clock = crate::time::TestClock::new(1_000);
    let mut replica = KvShard::with_clock(clock.clone());

    replica.follow_primary(true);
    for parts in [
        &[&b"HSET"[..], b"h", b"f", b"v"][..],
        &[b"HPEXPIREAT", b"h", b"2000", b"FIELDS", b"1", b"f"],
    ] {
        execute(&mut replica, command_of(parts));
    }
    clock.set(2_000);

    let read = |replica: &mut KvShard<crate::time::TestClock>, parts: &[&[u8]]| {
        execute(replica, command_of(parts)).into_owned()
    };

    // The sweep runs on every maintenance tick; the hash stays stored and hidden.
    for _ in 0..3 {
        replica.expire_hash_fields_due();
        assert_eq!(read(&mut replica, &[b"EXISTS", b"h"]), Response::Integer(0));
        assert_eq!(read(&mut replica, &[b"PTTL", b"h"]), Response::Integer(-2));
        assert_eq!(
            read(&mut replica, &[b"TYPE", b"h"]),
            Response::Simple(b"none")
        );
        assert_eq!(read(&mut replica, &[b"RANDOMKEY"]), Response::Bulk(None));
        assert_eq!(read(&mut replica, &[b"DBSIZE"]), Response::Integer(1));
        assert!(
            !format!("{:?}", read(&mut replica, &[b"SCAN", b"0"])).contains("[104]"),
            "SCAN skips the dead hash"
        );
    }

    // The primary's delete removes it, applied with expiry paused like every replicated write.
    let paused = replica.pause_expiration_for_replay();
    assert!(replica.delete(b"h"));
    replica.resume_expiration_after_replay(paused);
    assert_eq!(read(&mut replica, &[b"DBSIZE"]), Response::Integer(0));
}

#[test]
fn relative_expirations_past_the_signed_clock_are_rejected() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"value"),
                SetOptions {
                    expiry: SetExpiry::RelativeMs(i64::MAX),
                    ..SetOptions::default()
                }
            )
        ),
        Response::Error(b"ERR invalid expire time in 'set' command")
    );
    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"key"))),
        Response::Integer(-2)
    );

    execute(&mut shard, Command::set(owned(b"key"), owned(b"value")));

    for milliseconds in [i64::MAX, 9_223_372_036_854_775_000] {
        assert_eq!(
            execute(
                &mut shard,
                Command::Expire {
                    condition: crate::protocol::ExpireCondition::Always,
                    key: owned(b"key"),
                    milliseconds,
                }
            ),
            Response::Error(b"ERR invalid expire time")
        );
    }

    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"key"))),
        Response::Integer(-1)
    );
}

#[test]
fn the_latest_absolute_deadline_reports_without_overflow() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"value"),
                SetOptions {
                    expiry: SetExpiry::AbsoluteMs(i64::MAX),
                    ..SetOptions::default()
                }
            )
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::ExpireTime {
                key: owned(b"key"),
                millis: true
            }
        ),
        Response::Integer(i64::MAX)
    );

    match execute(&mut shard, Command::Ttl(owned(b"key"))) {
        Response::Integer(seconds) => assert!(seconds > 0),
        other => panic!("unexpected {other:?}"),
    }
}
