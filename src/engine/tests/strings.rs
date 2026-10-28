use super::*;

#[test]
fn take_and_getex_do_not_touch_the_plain_read_and_increment_paths() {
    // TAKE and GETEX
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut sink = DatabaseLog::default();

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Take {
                key: owned(b"seats"),
                count: 1,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(shard.value_kind(b"seats"), None);
    execute_with_sink(
        &mut shard,
        Command::set(owned(b"seats"), owned(b"5")),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Take {
                key: owned(b"seats"),
                count: 2,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Integer(3)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Take {
                key: owned(b"seats"),
                count: 4,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"seats")), &mut sink).unwrap(),
        Response::Bulk(Some(Cow::Owned(b"3".to_vec())))
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Increment(owned(b"seats"), 1),
            &mut sink
        )
        .unwrap(),
        Response::Integer(4)
    );
    execute_with_sink(
        &mut shard,
        Command::ListPush(owned(b"list"), vec![owned(b"a")]),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::Take {
                key: owned(b"list"),
                count: 1,
            },
            &mut sink,
        )
        .unwrap(),
        wrong_type()
    );

    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::GetEx {
                key: owned(b"missing"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Bulk(None)
    );
    assert_eq!(shard.value_kind(b"missing"), None);
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::GetEx {
                key: owned(b"seats"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        Response::Bulk(Some(Cow::Owned(b"4".to_vec())))
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"seats")), &mut sink).unwrap(),
        Response::Integer(5_000)
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"seats")), &mut sink).unwrap(),
        Response::Bulk(Some(Cow::Owned(b"4".to_vec())))
    );
    clock.set(2_000);
    assert_eq!(
        execute_with_sink(&mut shard, Command::PTtl(owned(b"seats")), &mut sink).unwrap(),
        Response::Integer(4_000)
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::GetEx {
                key: owned(b"list"),
                ttl_ms: 5_000,
            },
            &mut sink,
        )
        .unwrap(),
        wrong_type()
    );

    execute_with_sink(
        &mut shard,
        Command::set(owned(b"keep"), owned(b"session")),
        &mut sink,
    )
    .unwrap();
    execute_with_sink(
        &mut shard,
        Command::GetEx {
            key: owned(b"keep"),
            ttl_ms: 4_000_000_000_000,
        },
        &mut sink,
    )
    .unwrap();

    let mut failed = FailingSink;
    let before = shard.pttl(b"seats");

    assert!(
        execute_with_sink(
            &mut shard,
            Command::Take {
                key: owned(b"seats"),
                count: 1,
            },
            &mut failed,
        )
        .is_err()
    );
    assert_eq!(
        execute_with_sink(&mut shard, Command::Get(owned(b"seats")), &mut sink).unwrap(),
        Response::Bulk(Some(Cow::Owned(b"4".to_vec())))
    );
    assert!(
        execute_with_sink(
            &mut shard,
            Command::GetEx {
                key: owned(b"seats"),
                ttl_ms: 9_000,
            },
            &mut failed,
        )
        .is_err()
    );
    assert_eq!(shard.pttl(b"seats"), before);

    let restored = replayed(&sink);

    assert_eq!(
        restored
            .value_view(b"keep")
            .unwrap()
            .map(|view| view.to_vec()),
        Some(b"session".to_vec())
    );
    assert!(restored.pttl(b"keep") > 0);
}

#[test]
fn set_expiry_options_record_then_apply_deadlines() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();
    let response = execute_with_sink(
        &mut shard,
        Command::Set(
            owned(b"key"),
            owned(b"value"),
            SetOptions {
                expiry: SetExpiry::RelativeMs(60_000),
                condition: SetCondition::Always,
                get: false,
            },
        ),
        &mut sink,
    )
    .unwrap();

    assert_eq!(response, Response::Simple(b"OK"));
    // The value and its deadline are one record, so replay never sees one without the other.
    assert_eq!(sink.mutations, vec![RecordedMutation::Other]);
    let ttl = match execute(&mut shard, Command::PTtl(owned(b"key"))) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    };

    assert!(ttl > 0 && ttl <= 60_000);
}

#[test]
fn set_keepttl_preserves_and_plain_set_clears_deadlines() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::Set(
            owned(b"key"),
            owned(b"one"),
            SetOptions {
                expiry: SetExpiry::RelativeMs(60_000),
                ..SetOptions::default()
            },
        ),
    );
    execute(
        &mut shard,
        Command::Set(
            owned(b"key"),
            owned(b"two"),
            SetOptions {
                expiry: SetExpiry::KeepTtl,
                ..SetOptions::default()
            },
        ),
    );
    let kept = match execute(&mut shard, Command::PTtl(owned(b"key"))) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    };

    assert!(kept > 0);
    execute(&mut shard, Command::set(owned(b"key"), owned(b"three")));
    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"key"))),
        Response::Integer(-1)
    );
}

#[test]
fn set_nx_xx_and_invalid_or_past_expiry_match_redis() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"first"),
                SetOptions {
                    condition: SetCondition::NotExists,
                    ..SetOptions::default()
                }
            )
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"second"),
                SetOptions {
                    condition: SetCondition::NotExists,
                    ..SetOptions::default()
                }
            )
        ),
        Response::Bulk(None)
    );
    assert_eq!(shard.get(b"key"), Some(&b"first"[..]));
    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"missing"),
                owned(b"nope"),
                SetOptions {
                    condition: SetCondition::Exists,
                    ..SetOptions::default()
                }
            )
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"value"),
                SetOptions {
                    expiry: SetExpiry::RelativeMs(0),
                    ..SetOptions::default()
                }
            )
        ),
        Response::Error(b"ERR invalid expire time in 'set' command")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Set(
                owned(b"key"),
                owned(b"gone"),
                SetOptions {
                    expiry: SetExpiry::AbsoluteMs(1),
                    ..SetOptions::default()
                }
            )
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"key"), None);
}

#[test]
fn set_get_returns_the_old_string_and_respects_conditions() {
    let mut shard = KvShard::new();

    assert_eq!(
        reply(&mut shard, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\na\r\n"),
        Reply::Simple(b"OK")
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nb\r\n$3\r\nGET\r\n"
        ),
        Reply::Bulk(b"a".to_vec())
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
        Reply::Bulk(b"b".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$3\r\nSET\r\n$7\r\nmissing\r\n$1\r\nx\r\n$3\r\nGET\r\n"
        ),
        Reply::Nil
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n"),
        Reply::Bulk(b"x".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nc\r\n$2\r\nNX\r\n$3\r\nGET\r\n"
        ),
        Reply::Bulk(b"b".to_vec())
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
        Reply::Bulk(b"b".to_vec())
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*5\r\n$3\r\nSET\r\n$6\r\nabsent\r\n$1\r\ny\r\n$2\r\nXX\r\n$3\r\nGET\r\n"
        ),
        Reply::Nil
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$3\r\nGET\r\n$6\r\nabsent\r\n"),
        Reply::Nil
    );
    reply(
        &mut shard,
        b"*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\nz\r\n",
    );
    assert_eq!(
        reply(
            &mut shard,
            b"*4\r\n$3\r\nSET\r\n$4\r\nlist\r\n$1\r\ns\r\n$3\r\nGET\r\n"
        ),
        Reply::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    assert_eq!(
        reply(&mut shard, b"*2\r\n$4\r\nLLEN\r\n$4\r\nlist\r\n"),
        Reply::Integer(1)
    );
}

#[test]
fn string_family_commands_match_redis_semantics() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::MSet(vec![(owned(b"a"), owned(b"1")), (owned(b"b"), owned(b"2"))]),
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::MGet(vec![owned(b"a"), owned(b"missing"), owned(b"b")])
        ),
        Response::Array(vec![
            Some(Cow::Borrowed(b"1")),
            None,
            Some(Cow::Borrowed(b"2")),
        ])
    );

    execute(
        &mut shard,
        Command::ListPush(owned(b"list"), vec![owned(b"item")]),
    );
    assert_eq!(
        execute(&mut shard, Command::MGet(vec![owned(b"list")])),
        Response::Array(vec![None])
    );
    assert_eq!(
        execute(&mut shard, Command::StrLen(owned(b"list"))),
        wrong_type()
    );
    assert_eq!(
        execute(&mut shard, Command::GetSet(owned(b"list"), owned(b"x"))),
        wrong_type()
    );
    assert_eq!(
        execute(&mut shard, Command::Append(owned(b"list"), owned(b"x"))),
        wrong_type()
    );

    assert_eq!(
        execute(&mut shard, Command::GetSet(owned(b"a"), owned(b"3"))),
        Response::Bulk(Some(Cow::Owned(b"1".to_vec())))
    );
    assert_eq!(shard.get(b"a"), Some(&b"3"[..]));
    assert_eq!(
        execute(&mut shard, Command::Append(owned(b"a"), owned(b"0"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::StrLen(owned(b"a"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::StrLen(owned(b"missing"))),
        Response::Integer(0)
    );
    assert_eq!(
        execute(&mut shard, Command::Append(owned(b"created"), owned(b"hi"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::Increment(owned(b"by"), 5)),
        Response::Integer(5)
    );
    assert_eq!(
        execute(&mut shard, Command::Increment(owned(b"by"), -2)),
        Response::Integer(3)
    );
}

#[test]
fn getset_clears_ttl_append_and_incr_preserve_it() {
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::Set(
            owned(b"ttl"),
            owned(b"ab"),
            SetOptions {
                expiry: SetExpiry::RelativeMs(60_000),
                ..SetOptions::default()
            },
        ),
    );
    execute(&mut shard, Command::Append(owned(b"ttl"), owned(b"c")));
    let after_append = match execute(&mut shard, Command::PTtl(owned(b"ttl"))) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    };

    assert!(after_append > 0);
    assert_eq!(shard.get(b"ttl"), Some(&b"abc"[..]));

    execute(&mut shard, Command::set(owned(b"grow"), owned(b"value")));

    for _ in 0..100 {
        execute(&mut shard, Command::Append(owned(b"grow"), owned(b"z")));
    }

    let grown = shard.get(b"grow").expect("appended string");

    assert_eq!(grown.len(), 105);
    assert!(grown.starts_with(b"value"));
    assert!(grown.ends_with(b"z"));
    assert_eq!(
        execute(&mut shard, Command::StrLen(owned(b"grow"))),
        Response::Integer(105)
    );

    execute(&mut shard, Command::set(owned(b"counter"), owned(b"10")));
    execute(
        &mut shard,
        Command::Expire {
            condition: crate::protocol::ExpireCondition::Always,
            key: owned(b"counter"),
            milliseconds: 60_000,
        },
    );
    assert_eq!(
        execute(&mut shard, Command::Increment(owned(b"counter"), 1)),
        Response::Integer(11)
    );
    let after_incr = match execute(&mut shard, Command::PTtl(owned(b"counter"))) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    };

    assert!(after_incr > 0);

    execute(&mut shard, Command::GetSet(owned(b"ttl"), owned(b"new")));
    assert_eq!(
        execute(&mut shard, Command::Ttl(owned(b"ttl"))),
        Response::Integer(-1)
    );
}

#[test]
fn mset_and_getset_record_before_applying() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"a", b"old");
    let mut sink = RecordingSink::default();

    execute_with_sink(
        &mut shard,
        Command::MSet(vec![(owned(b"a"), owned(b"1")), (owned(b"b"), owned(b"2"))]),
        &mut sink,
    )
    .unwrap();
    assert_eq!(
        sink.mutations,
        vec![
            RecordedMutation::SetBytes(b"a".to_vec(), b"1".to_vec()),
            RecordedMutation::SetBytes(b"b".to_vec(), b"2".to_vec()),
        ]
    );
    assert_eq!(shard.get(b"a"), Some(&b"1"[..]));
    assert_eq!(shard.get(b"b"), Some(&b"2"[..]));

    let mut shard = KvShard::new();

    shard.set_bytes(b"a", b"old");
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::GetSet(owned(b"a"), owned(b"new")),
            &mut FailingSink
        ),
        Err("persistence unavailable")
    );
    assert_eq!(shard.get(b"a"), Some(&b"old"[..]));
}

#[test]
fn mget_encodes_null_bulk_entries() {
    let mut output = Vec::new();

    write_response(
        &mut output,
        Response::Array(vec![Some(Cow::Borrowed(b"a")), None]),
    )
    .unwrap();
    assert_eq!(output, b"*2\r\n$1\r\na\r\n$-1\r\n");
}

#[test]
fn keyspace_commands_cover_exists_type_scan_rename_and_dbsize() {
    let mut shard = KvShard::new();

    execute(&mut shard, Command::set(owned(b"a"), owned(b"1")));
    execute(
        &mut shard,
        Command::ListPush(owned(b"list"), vec![owned(b"item")]),
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Exists(vec![owned(b"a"), owned(b"missing")].into())
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"a"))),
        Response::Simple(b"string")
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"list"))),
        Response::Simple(b"list")
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"missing"))),
        Response::Simple(b"none")
    );
    assert_eq!(execute(&mut shard, Command::DbSize), Response::Integer(2));
    assert_eq!(
        execute(
            &mut shard,
            Command::Rename {
                source: owned(b"missing"),
                destination: owned(b"x"),
            }
        ),
        Response::Error(b"ERR no such key")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Rename {
                source: owned(b"a"),
                destination: owned(b"a"),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::Rename {
                source: owned(b"a"),
                destination: owned(b"renamed"),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"a"), None);
    assert_eq!(shard.get(b"renamed"), Some(&b"1"[..]));

    let first = execute(
        &mut shard,
        Command::Scan {
            cursor: 0,
            pattern: Some(owned(b"*")),
            count: 1,
            type_name: None,
            slot: None,
        },
    );
    let (cursor, first_count) = match first {
        Response::Scan { cursor, keys } => (cursor, keys.len()),
        other => panic!("expected scan {other:?}"),
    };

    // COUNT is a hint: a small keyspace is read as one block.
    assert!(first_count >= 1);
    let mut total = first_count;
    let mut cursor = cursor;

    while cursor != 0 {
        let page = execute(
            &mut shard,
            Command::Scan {
                cursor,
                pattern: Some(owned(b"*")),
                count: 1,
                type_name: None,
                slot: None,
            },
        );
        let Response::Scan { cursor: next, keys } = page else {
            panic!("expected scan {page:?}");
        };

        total += keys.len();
        cursor = next;
    }

    assert_eq!(total, 2);

    let mut output = Vec::new();

    write_response(
        &mut output,
        Response::Scan {
            cursor: 0,
            keys: vec![Cow::Borrowed(b"a")],
        },
    )
    .unwrap();
    assert_eq!(output, b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\na\r\n");
}

fn fill_status(response: &Response<'_>) -> Option<Vec<u8>> {
    match response {
        Response::Array(items) => items[0].as_ref().map(|item| item.to_vec()),
        other => panic!("unexpected {other:?}"),
    }
}

fn fill_get(shard: &mut KvShard) -> Option<Vec<u8>> {
    fill_status(&execute(
        shard,
        Command::FillGet {
            key: owned(b"k"),
            fill_ms: 5_000,
            wait_ms: None,
        },
    ))
}

fn fill_set(shard: &mut KvShard, token: u64) -> Response<'static> {
    let response = execute(
        shard,
        Command::FillSet {
            key: owned(b"k"),
            value: owned(b"v"),
            ttl_ms: 60_000,
            token,
            swr_ms: None,
        },
    );

    match response {
        Response::Integer(value) => Response::Integer(value),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn fillget_hands_one_client_the_token_and_fillset_is_fenced_by_it() {
    let mut shard = KvShard::new();

    assert_eq!(fill_get(&mut shard), Some(b"FILL".to_vec()));
    assert_eq!(fill_get(&mut shard), Some(b"BUSY".to_vec()));
    assert_eq!(fill_set(&mut shard, 99), Response::Integer(0));
    assert_eq!(fill_set(&mut shard, 1), Response::Integer(1));
    assert_eq!(fill_set(&mut shard, 1), Response::Integer(0));
    assert_eq!(fill_get(&mut shard), Some(b"HIT".to_vec()));
}

#[test]
fn a_full_fill_table_keeps_the_soft_expiry_of_a_value_that_is_still_stored() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let get = |shard: &mut KvShard<crate::time::TestClock>, key: &[u8], fill_ms: u64| {
        let response = execute(
            shard,
            Command::FillGet {
                key: owned(key),
                fill_ms,
                wait_ms: None,
            },
        );

        String::from_utf8(fill_status(&response).unwrap()).unwrap()
    };

    assert_eq!(get(&mut shard, b"k", 5_000), "FILL");
    assert_eq!(
        execute(
            &mut shard,
            Command::FillSet {
                key: owned(b"k"),
                value: owned(b"v"),
                ttl_ms: 100,
                token: 1,
                swr_ms: Some(10_000),
            },
        ),
        Response::Integer(1)
    );

    // Fill the table with short fill tokens, then let them lapse and the value turn stale.
    for index in 1..65_536u32 {
        assert_eq!(get(&mut shard, &index.to_be_bytes(), 10), "FILL");
    }

    clock.set(2_000);
    // A new key needs room: the purge drops the lapsed tokens but keeps the stale value's mark.
    assert_eq!(get(&mut shard, b"new", 1_000_000), "FILL");
    assert_eq!(get(&mut shard, b"k", 5_000), "STALE");

    // Once the value's own deadline has passed, its mark is the only one the purge can drop.
    clock.set(12_000);
    for index in 1..65_535u32 {
        assert_eq!(
            get(&mut shard, &(index + 100_000).to_be_bytes(), 1_000_000),
            "FILL"
        );
    }

    assert_eq!(get(&mut shard, b"newer", 5_000), "FILL");
    assert!(matches!(
        execute(
            &mut shard,
            Command::FillGet {
                key: owned(b"newest"),
                fill_ms: 5_000,
                wait_ms: None,
            },
        ),
        Response::Error(_)
    ));
}

fn fill_get_waiting(shard: &mut KvShard) -> String {
    let response = execute(
        shard,
        Command::FillGet {
            key: owned(b"k"),
            fill_ms: 5_000,
            wait_ms: Some(200),
        },
    );

    match response {
        Response::BlockRetry { timeout_ms } => format!("RETRY {timeout_ms:?}"),
        other => String::from_utf8(fill_status(&other).unwrap()).unwrap(),
    }
}

#[test]
fn fillget_wait_asks_the_connection_to_retry_only_while_a_fill_is_pending() {
    let mut shard = KvShard::new();

    // Nobody is filling yet, so the first waiter becomes the filler immediately.
    assert_eq!(fill_get_waiting(&mut shard), "FILL");
    assert_eq!(fill_get_waiting(&mut shard), "RETRY Some(200)");
    assert_eq!(fill_set(&mut shard, 1), Response::Integer(1));
    assert_eq!(fill_get_waiting(&mut shard), "HIT");
}

pub(super) fn words(parts: &[&str]) -> Vec<u8> {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        frame.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
    }

    frame
}

#[test]
fn copy_carries_integer_strings_and_deadlines() {
    let mut shard = KvShard::new();

    reply(&mut shard, &words(&["SET", "n", "12345"]));
    reply(&mut shard, &words(&["SET", "s", "hello"]));
    reply(&mut shard, &words(&["PEXPIRE", "s", "100000"]));

    assert_eq!(
        reply(&mut shard, &words(&["COPY", "n", "n2"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["GET", "n2"])),
        Reply::Bulk(b"12345".to_vec())
    );
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "n", "n2"])),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "n", "n2", "REPLACE"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "s", "s2"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["GET", "s2"])),
        Reply::Bulk(b"hello".to_vec())
    );
    assert!(matches!(
        reply(&mut shard, &words(&["PTTL", "s2"])),
        Reply::Integer(ttl) if ttl > 0 && ttl <= 100_000
    ));
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "nope", "x"])),
        Reply::Integer(0)
    );
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "s", "s"])),
        Reply::Error(b"ERR source and destination objects are the same")
    );

    let long = "x".repeat(1000);

    reply(&mut shard, &words(&["SET", "big", &long]));
    assert_eq!(
        reply(&mut shard, &words(&["COPY", "big", "big2"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["GET", "big2"])),
        Reply::Bulk(long.into_bytes())
    );
}

#[test]
fn expiretime_and_object_encoding_share_one_lookup_paths() {
    let mut shard = KvShard::new();

    assert_eq!(
        reply(&mut shard, &words(&["EXPIRETIME", "none"])),
        Reply::Integer(-2)
    );
    assert_eq!(
        reply(&mut shard, &words(&["OBJECT", "ENCODING", "none"])),
        Reply::Nil
    );

    reply(&mut shard, &words(&["SET", "plain", "abc"]));
    assert_eq!(
        reply(&mut shard, &words(&["EXPIRETIME", "plain"])),
        Reply::Integer(-1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["PEXPIREAT", "plain", "4102444800000"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["EXPIRETIME", "plain"])),
        Reply::Integer(4_102_444_800)
    );
    assert_eq!(
        reply(&mut shard, &words(&["PEXPIRETIME", "plain"])),
        Reply::Integer(4_102_444_800_000)
    );

    for (value, encoding) in [
        ("abc", "embstr"),
        ("12345", "int"),
        ("-7", "int"),
        ("007", "embstr"),
        ("+5", "embstr"),
        ("9223372036854775808", "embstr"),
    ] {
        reply(&mut shard, &words(&["SET", "e", value]));
        assert_eq!(
            reply(&mut shard, &words(&["OBJECT", "ENCODING", "e"])),
            Reply::Bulk(encoding.as_bytes().to_vec()),
            "{value}"
        );
    }

    reply(&mut shard, &words(&["SET", "e", &"y".repeat(45)]));
    assert_eq!(
        reply(&mut shard, &words(&["OBJECT", "ENCODING", "e"])),
        Reply::Bulk(b"raw".to_vec())
    );
    reply(&mut shard, &words(&["RPUSH", "l", "a"]));
    assert_eq!(
        reply(&mut shard, &words(&["OBJECT", "ENCODING", "l"])),
        Reply::Bulk(b"quicklist".to_vec())
    );
}

#[test]
fn single_op_bitfield_replies_as_an_array_of_one() {
    let mut shard = KvShard::new();
    let frame = words(&["BITFIELD", "bf", "INCRBY", "u8", "0", "5"]);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    assert_eq!(
        execute(&mut shard, command.into_owned()),
        Response::OneInteger(Some(5))
    );

    let frame = words(&[
        "BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "300",
    ]);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    assert_eq!(
        execute(&mut shard, command.into_owned()),
        Response::OneInteger(None)
    );

    let frame = words(&["BITFIELD", "bf", "GET", "u8", "0", "GET", "u4", "4"]);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    assert_eq!(
        execute(&mut shard, command.into_owned()),
        Response::Exec(vec![Response::Integer(5), Response::Integer(5)])
    );

    let mut wire = Vec::new();

    write_response(&mut wire, Response::OneInteger(Some(7))).unwrap();
    write_response(&mut wire, Response::OneInteger(None)).unwrap();
    assert_eq!(wire, b"*1\r\n:7\r\n*1\r\n$-1\r\n");
}

#[test]
fn setrange_and_bitfield_in_place_keep_value_ttl_and_grow_when_needed() {
    let mut shard = KvShard::new();

    reply(&mut shard, &words(&["SET", "k", "abcdefgh"]));
    reply(&mut shard, &words(&["PEXPIRE", "k", "100000"]));
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "k", "2", "XY"])),
        Reply::Integer(8)
    );
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "k", "6", "ZZ"])),
        Reply::Integer(8)
    );
    assert_eq!(
        reply(&mut shard, &words(&["GET", "k"])),
        Reply::Bulk(b"abXYefZZ".to_vec())
    );
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "k", "7", "QQ"])),
        Reply::Integer(9)
    );
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "k", "11", "!"])),
        Reply::Integer(12)
    );
    assert_eq!(
        reply(&mut shard, &words(&["GET", "k"])),
        Reply::Bulk(b"abXYefZQQ\0\0!".to_vec())
    );
    assert!(matches!(
        reply(&mut shard, &words(&["PTTL", "k"])),
        Reply::Integer(ttl) if ttl > 0 && ttl <= 100_000
    ));
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "k", "0", ""])),
        Reply::Integer(12)
    );
    assert_eq!(
        reply(&mut shard, &words(&["SETRANGE", "nope", "2", "ab"])),
        Reply::Integer(4)
    );

    reply(&mut shard, &words(&["SET", "b", "\u{0}\u{0}\u{0}\u{0}"]));
    let frame = words(&[
        "BITFIELD", "b", "SET", "u8", "8", "200", "INCRBY", "u8", "8", "100",
    ]);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    assert_eq!(
        execute(&mut shard, command.into_owned()),
        Response::Exec(vec![Response::Integer(0), Response::Integer(44)])
    );
    assert_eq!(
        reply(&mut shard, &words(&["GETRANGE", "b", "1", "1"])),
        Reply::Bulk(vec![44])
    );

    let frame = words(&["BITFIELD", "b", "SET", "u8", "40", "7"]);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    execute(&mut shard, command.into_owned());
    assert_eq!(
        reply(&mut shard, &words(&["STRLEN", "b"])),
        Reply::Integer(6)
    );
}

#[test]
fn flushing_a_database_drops_its_fill_markers() {
    let mut shard = KvShard::new();

    assert_eq!(fill_get(&mut shard), Some(b"FILL".to_vec()));
    assert_eq!(fill_get(&mut shard), Some(b"BUSY".to_vec()));
    shard.flush_database(0);
    assert_eq!(fill_get(&mut shard), Some(b"FILL".to_vec()));
    shard.flush_all();
    assert_eq!(fill_get(&mut shard), Some(b"FILL".to_vec()));
}

#[test]
fn append_returns_in_place_length_with_and_without_a_log() {
    let big = vec![b'b'; 5_000];
    let cases: [(&[u8], &[u8]); 4] = [
        (b"12345", b"6"),
        (b"-42", b"x"),
        (b"short", b"er"),
        (&big, b"tail"),
    ];

    for (initial, suffix) in cases {
        let expected = [initial, suffix].concat();
        let mut memory = KvShard::new();

        execute(&mut memory, Command::set(owned(b"k"), owned(initial)));
        assert_eq!(
            execute(&mut memory, Command::Append(owned(b"k"), owned(suffix))),
            Response::Integer(expected.len() as i64)
        );
        assert_eq!(memory.get(b"k"), Some(expected.as_slice()));

        let mut logged = KvShard::new();
        let mut sink = DatabaseLog::default();

        execute_with_sink(
            &mut logged,
            Command::set(owned(b"k"), owned(initial)),
            &mut sink,
        )
        .unwrap();
        assert_eq!(
            execute_with_sink(
                &mut logged,
                Command::Append(owned(b"k"), owned(suffix)),
                &mut sink
            )
            .unwrap(),
            Response::Integer(expected.len() as i64)
        );
        assert_eq!(logged.get(b"k"), Some(expected.as_slice()));
        assert_eq!(replayed(&sink).get(b"k"), Some(expected.as_slice()));
    }
}

/// MSET leaves the store as one SET per pair in order would: across the prefetch windows
/// (16 keys) and their edges, with a key repeated inside and across windows, keys that
/// had a TTL, a key holding another type, values moving between the log, the heap and
/// integers, and new keys growing the index under a window. The log replays to the same.
#[test]
fn mset_matches_one_set_per_pair_across_windows_repeats_ttls_and_types() {
    fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&[u8]]) -> Response<'static> {
        execute_with_sink(shard, command_of(parts), sink)
            .unwrap()
            .into_owned()
    }

    let long = vec![b'L'; 3_000];

    for pairs in [1usize, 15, 16, 17, 33, 40, 200] {
        let mut batched = KvShard::new();
        let mut single = KvShard::new();
        let mut sink = DatabaseLog::default();
        let mut single_sink = DatabaseLog::default();
        let distinct = pairs.div_ceil(2).max(1) + 2;
        let key = |index: usize| format!("k{}", index % distinct).into_bytes();

        for (shard, log) in [(&mut batched, &mut sink), (&mut single, &mut single_sink)] {
            run(shard, log, &[b"SET", b"k1", b"timed", b"EX", b"100"]);
            run(shard, log, &[b"SET", b"k2", &long, b"PX", b"90000"]);
            run(shard, log, &[b"RPUSH", b"k3", b"a", b"b"]);
            run(shard, log, &[b"SET", b"k4", b"12345"]);
            run(shard, log, &[b"SET", b"k5", &long]);
        }

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..pairs)
            .map(|index| {
                let value = match index % 4 {
                    0 => format!("v{index}").into_bytes(),
                    1 => index.to_string().into_bytes(),
                    2 => vec![b'0' + (index % 10) as u8; 1_000 + index],
                    _ => Vec::new(),
                };

                (key(index), value)
            })
            .collect();
        let mut mset: Vec<&[u8]> = vec![b"MSET"];

        for (key, value) in &entries {
            mset.extend([key.as_slice(), value.as_slice()]);
        }

        assert_eq!(run(&mut batched, &mut sink, &mset), Response::Simple(b"OK"));

        for (key, value) in &entries {
            run(&mut single, &mut single_sink, &[b"SET", key, value]);
        }

        let mut replayed = replayed(&sink);

        for index in 0..distinct + 3 {
            let key = key(index);

            for probe in [&b"GET"[..], b"TTL", b"TYPE"] {
                let expected = run(&mut single, &mut DatabaseLog::default(), &[probe, &key]);

                assert_eq!(
                    run(&mut batched, &mut DatabaseLog::default(), &[probe, &key]),
                    expected,
                    "{pairs} pairs: {} {}",
                    String::from_utf8_lossy(probe),
                    String::from_utf8_lossy(&key)
                );

                if probe == b"GET" {
                    assert_eq!(
                        execute(&mut replayed, command_of(&[probe, &key])).into_owned(),
                        expected
                    );
                }
            }
        }

        let (a, b) = (batched.memory_usage(), single.memory_usage());

        assert_eq!(
            (a.entries, a.key_bytes, a.value_bytes),
            (b.entries, b.key_bytes, b.value_bytes),
            "{pairs} pairs"
        );
    }
}

#[test]
fn scan_with_a_slot_walks_the_keyspace_and_returns_only_that_slots_keys() {
    let mut shard = KvShard::new();
    let slot = crate::routing::hash_slot(b"{tag}a");

    for key in [
        &b"{tag}a"[..],
        b"{tag}b",
        b"{tag}c",
        b"other:1",
        b"other:2",
        b"other:3",
    ] {
        shard.set_bytes(key, b"v");
    }

    let mut cursor = 0;
    let mut found = Vec::new();

    loop {
        let page = execute(
            &mut shard,
            command_of(&[
                b"SCAN",
                cursor.to_string().as_bytes(),
                b"COUNT",
                b"1",
                b"SLOT",
                slot.to_string().as_bytes(),
            ]),
        );
        let Response::Scan { cursor: next, keys } = page else {
            panic!("expected scan {page:?}");
        };

        found.extend(keys.into_iter().map(|key| key.into_owned()));
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    found.sort();
    assert_eq!(
        found,
        [b"{tag}a".to_vec(), b"{tag}b".to_vec(), b"{tag}c".to_vec()]
    );
}
