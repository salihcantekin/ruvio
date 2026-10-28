use super::strings::words;
use super::*;

#[test]
fn json_commands_preserve_ttl_and_type_isolation() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let key = owned(b"doc");
    let path = owned(b"$");
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonSet {
                key: key.clone(),
                path: path.clone(),
                value: owned(br#"{"a":[1,2]}"#),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, Command::Type(key.clone())),
        Response::Simple(b"json")
    );
    assert_eq!(execute(&mut shard, Command::Get(key.clone())), wrong_type());
    assert_eq!(
        execute(
            &mut shard,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: key.clone(),
                unix_millis: 2_000
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonSet {
                key: key.clone(),
                path: owned(b"$.a[0]"),
                value: owned(b"3"),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, Command::PTtl(key.clone())),
        Response::Integer(1_000)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonGet {
                key: key.clone(),
                path: owned(b"$.a")
            }
        ),
        Response::Bulk(Some(owned(b"[3,2]")))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonDel {
                key: key.clone(),
                path: owned(b"$.a[0]")
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::PTtl(key.clone())),
        Response::Integer(1_000)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonGet {
                key: key.clone(),
                path: path.clone()
            }
        ),
        Response::Bulk(Some(owned(br#"{"a":[2]}"#)))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonDel {
                key: key.clone(),
                path
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::Type(key)),
        Response::Simple(b"none")
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"missing"))),
        Response::Simple(b"none")
    );
}

#[test]
fn json_merge_preserves_ttl_and_applies_one_atomic_document() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock);
    let merge = |key: &[u8], path: &[u8], value: &[u8]| Command::JsonMerge {
        key: owned(key),
        path: owned(path),
        value: owned(value),
    };
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$", br#"{"a":null,"b":1}"#)),
        Response::Simple(b"OK")
    );
    assert_eq!(
        shard.json_view(b"doc").unwrap(),
        Some(br#"{"b":1}"#.as_slice())
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"doc"),
                unix_millis: 2_000,
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$.b", br#"{"x":2}"#)),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$.added", b"null")),
        Response::Simple(b"OK")
    );
    assert_eq!(
        shard.json_view(b"doc").unwrap(),
        Some(br#"{"b":{"x":2},"added":null}"#.as_slice())
    );
    assert_eq!(
        execute(&mut shard, Command::PTtl(owned(b"doc"))),
        Response::Integer(1_000)
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$.missing.child", b"1")),
        Response::Bulk(None)
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$", br#"{"added":null}"#)),
        Response::Simple(b"OK")
    );
    assert_eq!(
        shard.json_view(b"doc").unwrap(),
        Some(br#"{"b":{"x":2}}"#.as_slice())
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$", b"oops")),
        Response::Error(b"ERR invalid JSON value")
    );
    assert_eq!(
        execute(&mut shard, merge(b"doc", b"$.bad]", b"1")),
        Response::Error(b"ERR invalid JSON path")
    );
    assert_eq!(
        shard.json_view(b"doc").unwrap(),
        Some(br#"{"b":{"x":2}}"#.as_slice())
    );
    assert_eq!(
        execute(&mut shard, merge(b"missing", b"$.a", b"1")),
        Response::Error(b"ERR new JSON documents must be created at root")
    );
    assert_eq!(
        execute(&mut shard, Command::set(owned(b"string"), owned(b"value"))),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, merge(b"string", b"$", b"1")),
        wrong_type()
    );
}

#[test]
fn large_json_failed_wal_record_keeps_document_unchanged() {
    let mut shard = KvShard::new();
    let original = serde_json::to_vec(&serde_json::json!({
        "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "nested": {"value": 1}
    }))
    .unwrap();
    shard.put_json(b"doc", original).unwrap();
    let before = shard.snapshot_entries();
    for command in [
        Command::JsonSet {
            key: owned(b"doc"),
            path: owned(b"$.nested.value"),
            value: owned(b"2"),
        },
        Command::JsonMerge {
            key: owned(b"doc"),
            path: owned(b"$.nested"),
            value: owned(br#"{"value":null}"#),
        },
        Command::JsonDel {
            key: owned(b"doc"),
            path: owned(b"$.nested.value"),
        },
    ] {
        assert_eq!(
            execute_with_sink(&mut shard, command, &mut FailingSink),
            Err("persistence unavailable")
        );
        assert_eq!(shard.snapshot_entries(), before);
    }
}

#[test]
fn large_json_uses_deltas_and_survives_wal_and_snapshot_recovery() {
    use crate::persistence::{decode_record, encode_record};
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let initial = serde_json::to_vec(&serde_json::json!({
        "bulk": "x".repeat(crate::json::TREE_THRESHOLD),
        "nested": { "old": 1, "keep": true },
        "items": [1, 2, 3]
    }))
    .unwrap();
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonSet {
                key: owned(b"doc"),
                path: owned(b"$"),
                value: owned(&initial)
            }
        ),
        Response::Simple(b"OK")
    );
    assert!(shard.json_tree(b"doc").is_some());
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::PExpireAt {
                condition: crate::protocol::ExpireCondition::Always,
                key: owned(b"doc"),
                unix_millis: 4_000_000_000_000
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonSet {
                key: owned(b"doc"),
                path: owned(b"$.nested.old"),
                value: owned(b"5")
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonMerge {
                key: owned(b"doc"),
                path: owned(b"$.nested"),
                value: owned(br#"{"old":null,"added":7}"#)
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonDel {
                key: owned(b"doc"),
                path: owned(b"$.items[1]")
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::set(owned(b"plain"), owned(b"value"))
        ),
        Response::Simple(b"OK")
    );
    assert!(matches!(sink.records[0].1, OwnedMutation::JsonPut { .. }));
    assert!(matches!(
        sink.records[2].1,
        OwnedMutation::JsonSetPath { .. }
    ));
    assert!(matches!(
        sink.records[3].1,
        OwnedMutation::JsonMergePath { .. }
    ));
    assert!(matches!(
        sink.records[4].1,
        OwnedMutation::JsonDelPath { .. }
    ));
    assert!(matches!(sink.records[5].1, OwnedMutation::SetBytes { .. }));
    let delta_size = encode_record(1, 3, 0, &sink.records[2].1).unwrap().len();
    assert!(
        delta_size < 128,
        "delta WAL must not serialize the large document"
    );
    let expected = shard.snapshot_entries();
    let mut recovered = KvShard::new();
    for (index, (db, mutation)) in sink.records.iter().enumerate() {
        let record = encode_record(1, index as u64 + 1, *db, mutation).unwrap();
        let decoded =
            decode_record(&record, 1, crate::protocol::ProtocolLimits::default()).unwrap();
        crate::persistence::apply_mutation(&mut recovered, decoded.database, &decoded.mutation)
            .unwrap();
    }
    assert_eq!(recovered.snapshot_entries(), expected);
    let mut restored = KvShard::new();
    for entry in expected {
        restored.restore_snapshot_entry(entry).unwrap();
    }
    assert!(restored.json_tree(b"doc").is_some());
    assert_eq!(restored.snapshot_entries(), recovered.snapshot_entries());
    assert_eq!(
        in_db(
            &mut restored,
            &mut DatabaseLog::default(),
            0,
            Command::JsonGet {
                key: owned(b"doc"),
                path: owned(b"$.nested")
            }
        ),
        Response::Bulk(Some(owned(br#"{"keep":true,"added":7}"#)))
    );
    assert_eq!(
        restored.value_view(b"plain").unwrap().map(|v| v.to_vec()),
        Some(b"value".to_vec())
    );
    assert!(restored.pttl(b"doc") > 0);
    assert_eq!(
        restored.value_view(b"plain").unwrap().map(|v| v.to_vec()),
        Some(b"value".to_vec())
    );
}

#[test]
fn large_json_root_reply_streams_correct_bulk_after_path_edits() {
    let mut shard = KvShard::new();
    let original = serde_json::to_vec(&serde_json::json!({
        "padding": "x".repeat(crate::json::TREE_THRESHOLD),
        "nested": {"value": "before"}
    }))
    .unwrap();
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonSet {
                key: owned(b"doc"),
                path: owned(b"$"),
                value: owned(&original),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonSet {
                key: owned(b"doc"),
                path: owned(b"$.nested.value"),
                value: owned("\"after \\\"quote\\\" \\\\ slash \\n newline 雪\"".as_bytes()),
            }
        ),
        Response::Simple(b"OK")
    );
    let response = execute(
        &mut shard,
        Command::JsonGet {
            key: owned(b"doc"),
            path: owned(b"$"),
        },
    );
    assert!(matches!(response, Response::JsonTree(_)));
    let mut encoded = Vec::new();
    write_response(&mut encoded, response.clone()).unwrap();
    let split = encoded.iter().position(|byte| *byte == b'\r').unwrap();
    let advertised: usize = std::str::from_utf8(&encoded[1..split])
        .unwrap()
        .parse()
        .unwrap();
    let body = &encoded[split + 2..encoded.len() - 2];
    assert_eq!(advertised, body.len());
    assert_eq!(response.into_owned(), Response::Bulk(Some(owned(body))));
    let document: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(
        document["nested"]["value"],
        "after \"quote\" \\ slash \n newline 雪"
    );
}

#[test]
fn json_merges_do_not_change_other_key_types_or_commands() {
    let mut shard = KvShard::new();
    assert_eq!(
        execute(&mut shard, Command::set(owned(b"plain"), owned(b"value"))),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"hash"),
                pairs: vec![(owned(b"field"), owned(b"kept"))],
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonSet {
                key: owned(b"doc"),
                path: owned(b"$"),
                value: owned(br#"{"counter":0,"unused":null}"#),
            }
        ),
        Response::Simple(b"OK")
    );
    for i in 1..=128 {
        assert_eq!(
            execute(
                &mut shard,
                Command::JsonMerge {
                    key: owned(b"doc"),
                    path: owned(b"$"),
                    value: owned(format!(r#"{{"counter":{i},"unused":null}}"#).as_bytes()),
                }
            ),
            Response::Simple(b"OK")
        );
        assert_eq!(
            execute(&mut shard, Command::Increment(owned(b"counter"), 1)),
            Response::Integer(i)
        );
        assert_eq!(
            execute(&mut shard, Command::Get(owned(b"plain"))),
            Response::Bulk(Some(owned(b"value")))
        );
        assert_eq!(
            execute(
                &mut shard,
                Command::HashGet {
                    key: owned(b"hash"),
                    field: owned(b"field"),
                }
            ),
            Response::Bulk(Some(owned(b"kept")))
        );
    }
    assert_eq!(
        execute(
            &mut shard,
            Command::Rename {
                source: owned(b"doc"),
                destination: owned(b"renamed"),
            }
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"renamed"))),
        Response::Simple(b"json")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::JsonGet {
                key: owned(b"renamed"),
                path: owned(b"$")
            }
        ),
        Response::Bulk(Some(owned(br#"{"counter":128}"#)))
    );
    assert_eq!(
        execute(&mut shard, Command::Del(vec![owned(b"renamed")].into())),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"counter"))),
        Response::Bulk(Some(owned(b"128")))
    );
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"plain"))),
        Response::Bulk(Some(owned(b"value")))
    );
}

#[test]
fn compact_json_path_writes_log_small_records_and_replay_to_the_same_document() {
    use crate::persistence::{decode_record, encode_record};
    for bulk in [10_000usize, crate::json::TREE_THRESHOLD] {
        let mut shard = KvShard::new();
        let mut sink = DatabaseLog::default();
        let initial = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(bulk),
            "meta": {"value": "a", "tags": [1, 2]},
            "n": 1
        }))
        .unwrap();
        let commands: Vec<Command<'_>> = vec![
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$"),
                value: owned(&initial),
            },
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$.n"),
                value: owned(b"2"),
            },
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$.meta.extra"),
                value: owned(br#"{"a":1}"#),
            },
            Command::JsonMerge {
                key: owned(b"d"),
                path: owned(b"$.meta"),
                value: owned(br#"{"value":null,"x":[1]}"#),
            },
            Command::JsonDel {
                key: owned(b"d"),
                path: owned(b"$.meta.tags[0]"),
            },
            Command::JsonDel {
                key: owned(b"d"),
                path: owned(b"$.meta.extra"),
            },
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$.meta.x[0]"),
                value: owned(b"9"),
            },
        ];
        for command in commands {
            in_db(&mut shard, &mut sink, 0, command);
        }
        for (_, record) in &sink.records[1..] {
            assert!(matches!(
                record,
                OwnedMutation::JsonSetPath { .. }
                    | OwnedMutation::JsonMergePath { .. }
                    | OwnedMutation::JsonDelPath { .. }
            ));
        }
        let mut recovered = KvShard::new();
        for (index, (db, mutation)) in sink.records.iter().enumerate() {
            let record = encode_record(1, index as u64 + 1, *db, mutation).unwrap();
            assert!(index == 0 || record.len() < 160);
            let decoded =
                decode_record(&record, 1, crate::protocol::ProtocolLimits::default()).unwrap();
            crate::persistence::apply_mutation(&mut recovered, decoded.database, &decoded.mutation)
                .unwrap();
        }
        assert_eq!(recovered.snapshot_entries(), shard.snapshot_entries());
        let reply = in_db(
            &mut recovered,
            &mut DatabaseLog::default(),
            0,
            Command::JsonGet {
                key: owned(b"d"),
                path: owned(b"$.meta"),
            },
        );
        assert_eq!(
            reply,
            Response::Bulk(Some(owned(br#"{"tags":[2],"x":[9]}"#)))
        );
    }
}

fn json_op(
    shard: &mut KvShard,
    sink: &mut DatabaseLog,
    op: crate::protocol::JsonOpKind,
    path: &[u8],
    args: &[&[u8]],
) -> Response<'static> {
    let response = in_db(
        shard,
        sink,
        0,
        Command::JsonOp {
            op,
            key: owned(b"d"),
            path: owned(path),
            args: args.iter().map(|arg| owned(arg)).collect(),
        },
    );
    match response {
        Response::Simple(text) => Response::Simple(text),
        Response::Bulk(value) => Response::Bulk(value.map(|bytes| Cow::Owned(bytes.into_owned()))),
        Response::Integer(value) => Response::Integer(value),
        Response::Array(items) => Response::Array(
            items
                .into_iter()
                .map(|item| item.map(|bytes| Cow::Owned(bytes.into_owned())))
                .collect(),
        ),
        Response::Error(text) => Response::Error(text),
        _ => panic!("unexpected JSON op response"),
    }
}

// Covers JSON.TYPE, JSON.STRLEN, JSON.ARRLEN, JSON.OBJKEYS, JSON.NUMINCRBY, JSON.TOGGLE,
// JSON.ARRAPPEND and JSON.CLEAR on compact and indexed documents.
#[test]
fn json_ops_read_and_write_through_ordinary_path_records() {
    use crate::protocol::JsonOpKind as Op;
    for bulk in [100usize, crate::json::TREE_THRESHOLD] {
        let mut shard = KvShard::new();
        let mut sink = DatabaseLog::default();
        let initial = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(bulk), "n": 5, "f": 1.5, "name": "héllo", "ok": false,
            "list": [1, 2], "meta": {"b": 1, "a": 2}
        }))
        .unwrap();
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$"),
                value: owned(&initial),
            },
        );
        let bulk_of = |text: &'static [u8]| Response::Bulk(Some(Cow::Borrowed(text)));
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Type, b"$.n", &[]),
            bulk_of(b"integer")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Type, b"$.f", &[]),
            bulk_of(b"number")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Type, b"$", &[]),
            bulk_of(b"object")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Type, b"$.zz", &[]),
            Response::Bulk(None)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::StrLen, b"$.name", &[]),
            Response::Integer(6)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::ArrLen, b"$.list", &[]),
            Response::Integer(2)
        );
        assert!(matches!(
            json_op(&mut shard, &mut sink, Op::ArrLen, b"$.n", &[]),
            Response::Error(_)
        ));
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::ObjKeys, b"$.meta", &[]),
            Response::Array(vec![
                Some(Cow::Owned(b"b".to_vec())),
                Some(Cow::Owned(b"a".to_vec()))
            ])
        );
        let before = sink.records.len();
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::NumIncrBy, b"$.n", &[b"3"]),
            bulk_of(b"8")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::NumIncrBy, b"$.f", &[b"0.25"]),
            bulk_of(b"1.75")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Toggle, b"$.ok", &[]),
            Response::Integer(1)
        );
        assert_eq!(
            json_op(
                &mut shard,
                &mut sink,
                Op::ArrAppend,
                b"$.list",
                &[b"3", br#"{"z":1}"#]
            ),
            Response::Integer(4)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Clear, b"$.meta", &[]),
            Response::Integer(1)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::Clear, b"$.meta", &[]),
            Response::Integer(0)
        );
        assert!(matches!(
            json_op(&mut shard, &mut sink, Op::Toggle, b"$.n", &[]),
            Response::Error(_)
        ));
        for (_, record) in &sink.records[before..] {
            assert!(matches!(record, OwnedMutation::JsonSetPath { .. }));
        }
        assert_eq!(
            in_db(
                &mut shard,
                &mut sink,
                0,
                Command::JsonGet {
                    key: owned(b"d"),
                    path: owned(b"$.list")
                }
            ),
            Response::Bulk(Some(owned(br#"[1,2,3,{"z":1}]"#)))
        );
        let mut recovered = KvShard::new();
        for (index, (db, mutation)) in sink.records.iter().enumerate() {
            let record =
                crate::persistence::encode_record(1, index as u64 + 1, *db, mutation).unwrap();
            let decoded = crate::persistence::decode_record(
                &record,
                1,
                crate::protocol::ProtocolLimits::default(),
            )
            .unwrap();
            crate::persistence::apply_mutation(&mut recovered, decoded.database, &decoded.mutation)
                .unwrap();
        }
        assert_eq!(recovered.snapshot_entries(), shard.snapshot_entries());
    }
}

#[test]
fn json_documents_survive_copy_and_dump_restore_in_member_order() {
    for bulk in [10usize, crate::json::TREE_THRESHOLD] {
        let mut shard = KvShard::new();
        let document = serde_json::to_string(&serde_json::json!({
            "z": 1,
            "a": {"y": [1, 2, 3], "b": true},
            "blob": "x".repeat(bulk),
        }))
        .unwrap();

        reply(&mut shard, &words(&["JSON.SET", "doc", "$", &document]));
        reply(&mut shard, &words(&["PEXPIRE", "doc", "100000"]));

        assert_eq!(
            reply(&mut shard, &words(&["COPY", "doc", "copy"])),
            Reply::Integer(1)
        );
        assert_eq!(
            reply(&mut shard, &words(&["JSON.GET", "copy", "$"])),
            Reply::Bulk(document.clone().into_bytes())
        );
        assert!(matches!(
            reply(&mut shard, &words(&["PTTL", "copy"])),
            Reply::Integer(ttl) if ttl > 0
        ));

        let Reply::Bulk(payload) = reply(&mut shard, &words(&["DUMP", "doc"])) else {
            panic!("DUMP of JSON must return a payload");
        };
        let mut restore = b"*4\r\n$7\r\nRESTORE\r\n$4\r\nback\r\n$1\r\n0\r\n".to_vec();

        restore.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
        restore.extend_from_slice(&payload);
        restore.extend_from_slice(b"\r\n");
        assert_eq!(reply(&mut shard, &restore), Reply::Simple(b"OK"));
        assert_eq!(
            reply(&mut shard, &words(&["JSON.GET", "back", "$"])),
            Reply::Bulk(document.into_bytes())
        );
        assert_eq!(
            reply(&mut shard, &words(&["TYPE", "back"])),
            Reply::Simple(b"json")
        );
        // The copy is independent of the source.
        reply(&mut shard, &words(&["JSON.SET", "copy", "$.z", "2"]));
        assert_eq!(
            reply(&mut shard, &words(&["JSON.GET", "doc", "$.z"])),
            Reply::Bulk(b"1".to_vec())
        );
    }
}

#[test]
fn json_mget_reads_one_path_from_many_keys() {
    let mut shard = KvShard::new();

    reply(&mut shard, &words(&["JSON.SET", "a", "$", r#"{"n":1}"#]));
    reply(&mut shard, &words(&["JSON.SET", "b", "$", r#"{"n":2}"#]));
    reply(&mut shard, &words(&["SET", "s", "plain"]));

    assert_eq!(
        reply(
            &mut shard,
            &words(&["JSON.MGET", "a", "missing", "s", "b", "$.n"])
        ),
        Reply::Array(vec![Some(b"1".to_vec()), None, None, Some(b"2".to_vec())])
    );
    assert_eq!(
        reply(&mut shard, &words(&["JSON.MGET", "a", "$..["])),
        Reply::Error(b"ERR invalid JSON path")
    );
}

#[test]
fn arrappend_logs_one_small_record_per_item_and_replays_to_the_same_document() {
    use crate::protocol::JsonOpKind as Op;
    // 40 ints stay compact; the bulk string makes the second document an indexed one.
    for bulk in [0usize, crate::json::TREE_THRESHOLD] {
        let mut shard = KvShard::new();
        let mut sink = DatabaseLog::default();
        let initial = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(bulk),
            "l": (0..40).collect::<Vec<_>>(),
            "e": [],
            "s": "text",
        }))
        .unwrap();
        in_db(
            &mut shard,
            &mut sink,
            0,
            Command::JsonSet {
                key: owned(b"d"),
                path: owned(b"$"),
                value: Cow::Owned(initial),
            },
        );
        let before = sink.records.len();
        assert_eq!(
            json_op(
                &mut shard,
                &mut sink,
                Op::ArrAppend,
                b"$.l",
                &[b"41", br#"{"a":"]"}"#]
            ),
            Response::Integer(42)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::ArrAppend, b"$.e", &[b"[1,2]"]),
            Response::Integer(1)
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::ArrAppend, b"$.s", &[b"1"]),
            Response::Error(b"WRONGTYPE JSON value is not an array")
        );
        assert_eq!(
            json_op(&mut shard, &mut sink, Op::ArrAppend, b"$.nope", &[b"1"]),
            Response::Bulk(None)
        );
        assert_eq!(
            sink.records.len() - before,
            3,
            "one record per appended item"
        );
        for (_, record) in &sink.records[before..] {
            let OwnedMutation::JsonSetPath { path, value, .. } = record else {
                panic!("expected a path record, got {record:?}");
            };
            assert!(
                path.len() + value.len() < 64,
                "record carries the item, not the array"
            );
        }
        let read = |shard: &mut KvShard, path: &[u8]| {
            let Response::Bulk(Some(bytes)) = in_db(
                shard,
                &mut DatabaseLog::default(),
                0,
                Command::JsonGet {
                    key: owned(b"d"),
                    path: owned(path),
                },
            ) else {
                panic!("path missing");
            };
            String::from_utf8(bytes.into_owned()).unwrap()
        };
        let mut copy = replayed(&sink);
        for path in [&b"$.l"[..], b"$.e", b"$.s"] {
            assert_eq!(read(&mut copy, path), read(&mut shard, path));
        }
        assert!(read(&mut shard, b"$.l").starts_with("[0,1,2,"));
        assert!(read(&mut shard, b"$.l").ends_with(r#"39,41,{"a":"]"}]"#));
        assert_eq!(read(&mut shard, b"$.e"), "[[1,2]]");
    }
}

#[test]
fn large_compact_documents_stay_compact_through_small_path_edits() {
    use crate::protocol::JsonOpKind as Op;
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    // Many tiny numbers: far over the tree size, but a tree would cost more than 2x the bytes.
    let initial = serde_json::to_vec(&serde_json::json!({
        "l": (0..20_000).collect::<Vec<_>>(),
        "n": 1,
    }))
    .unwrap();
    assert!(initial.len() >= crate::json::TREE_THRESHOLD);
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::JsonSet {
            key: owned(b"d"),
            path: owned(b"$"),
            value: Cow::Owned(initial),
        },
    );
    assert!(shard.json_tree(b"d").is_none());
    assert_eq!(
        json_op(&mut shard, &mut sink, Op::ArrAppend, b"$.l", &[b"7"]),
        Response::Integer(20_001)
    );
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::JsonSet {
            key: owned(b"d"),
            path: owned(b"$.n"),
            value: owned(b"2"),
        },
    );
    assert!(shard.json_tree(b"d").is_none());
    let copy = replayed(&sink);
    assert_eq!(
        shard.json_view(b"d").unwrap(),
        copy.json_view(b"d").unwrap()
    );
    let document = String::from_utf8(shard.json_view(b"d").unwrap().unwrap().to_vec()).unwrap();
    assert!(document.contains("19999,7]") && document.ends_with(r#""n":2}"#));
}

fn array_of(reply: Reply) -> Vec<String> {
    match reply {
        Reply::Array(items) => items
            .into_iter()
            .map(|item| String::from_utf8(item.unwrap()).unwrap())
            .collect(),
        other => panic!("expected an array, got {other:?}"),
    }
}

#[test]
fn json_field_index_finds_keys_and_follows_every_kind_of_change() {
    let mut shard = KvShard::new();

    reply(
        &mut shard,
        &words(&["JSON.SET", "user:1", "$", r#"{"city":"ankara","age":30}"#]),
    );
    reply(
        &mut shard,
        &words(&["JSON.SET", "user:2", "$", r#"{"city":"izmir","age":30}"#]),
    );
    reply(
        &mut shard,
        &words(&["JSON.SET", "other:1", "$", r#"{"city":"ankara"}"#]),
    );

    assert_eq!(
        reply(
            &mut shard,
            &words(&[
                "JSON.INDEX",
                "CREATE",
                "city",
                "PREFIX",
                "user:",
                "PATH",
                "$.city"
            ])
        ),
        Reply::Simple(b"OK")
    );
    assert_eq!(
        reply(
            &mut shard,
            &words(&[
                "JSON.INDEX",
                "CREATE",
                "city",
                "PREFIX",
                "user:",
                "PATH",
                "$.city"
            ])
        ),
        Reply::Error(b"ERR index already exists")
    );
    // Existing documents are read in by the build scan, a few slots per tick.
    assert_eq!(
        reply(
            &mut shard,
            &words(&["JSON.FIND", "city", "VALUE", "ankara"])
        ),
        Reply::Error(b"ERR index is building")
    );
    while shard.json_index_build_step(2) {}

    let find = |shard: &mut KvShard, value: &str| {
        array_of(reply(shard, &words(&["JSON.FIND", "city", "VALUE", value])))
    };

    assert_eq!(find(&mut shard, "ankara"), ["user:1"]);
    assert_eq!(find(&mut shard, "izmir"), ["user:2"]);

    // Change, overwrite with another type, delete, expire, rename and flush.
    reply(
        &mut shard,
        &words(&["JSON.SET", "user:1", "$.city", "\"izmir\""]),
    );
    assert_eq!(find(&mut shard, "izmir"), ["user:1", "user:2"]);
    assert!(find(&mut shard, "ankara").is_empty());
    assert_eq!(
        array_of(reply(
            &mut shard,
            &words(&[
                "JSON.FIND",
                "city",
                "VALUE",
                "izmir",
                "COUNT",
                "1",
                "AFTER",
                "user:1"
            ])
        )),
        ["user:2"]
    );

    reply(&mut shard, &words(&["SET", "user:2", "text"]));
    assert_eq!(find(&mut shard, "izmir"), ["user:1"]);

    reply(&mut shard, &words(&["RENAME", "user:1", "user:9"]));
    assert_eq!(find(&mut shard, "izmir"), ["user:9"]);
    reply(&mut shard, &words(&["RENAME", "user:9", "other:9"]));
    assert!(find(&mut shard, "izmir").is_empty());
    reply(&mut shard, &words(&["RENAME", "other:9", "user:3"]));
    assert_eq!(find(&mut shard, "izmir"), ["user:3"]);

    reply(&mut shard, &words(&["PEXPIRE", "user:3", "1"]));
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(find(&mut shard, "izmir").is_empty());

    reply(
        &mut shard,
        &words(&["JSON.SET", "user:4", "$", r#"{"city":"izmir"}"#]),
    );
    assert_eq!(find(&mut shard, "izmir"), ["user:4"]);
    assert_eq!(
        reply(&mut shard, &words(&["DEL", "user:4"])),
        Reply::Integer(1)
    );
    assert!(find(&mut shard, "izmir").is_empty());

    let list = array_of(reply(&mut shard, &words(&["JSON.INDEX", "LIST"])));
    assert_eq!(list.len(), 1);
    assert!(list[0].starts_with("city prefix=user: path=$.city type=string state=ready keys="));

    assert_eq!(
        reply(&mut shard, &words(&["JSON.INDEX", "DROP", "city"])),
        Reply::Integer(1)
    );
    assert_eq!(
        reply(&mut shard, &words(&["JSON.FIND", "city", "VALUE", "x"])),
        Reply::Error(b"ERR no such index")
    );
}

#[test]
fn json_field_index_types_numbers_and_survives_replay_of_its_definition() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&str]) -> String {
        let frame = words(parts);
        let (command, _) = parse_command(&frame).unwrap().unwrap();

        match in_db(shard, sink, 0, command.into_owned()) {
            Response::Array(keys) => format!("array:{}", keys.len()),
            Response::Error(message) => String::from_utf8_lossy(message).into_owned(),
            other => format!("{other:?}"),
        }
    }

    run(
        &mut shard,
        &mut sink,
        &[
            "JSON.INDEX",
            "CREATE",
            "age",
            "PREFIX",
            "",
            "PATH",
            "$.age",
            "TYPE",
            "number",
        ],
    );
    run(
        &mut shard,
        &mut sink,
        &["JSON.SET", "a", "$", r#"{"age":30}"#],
    );
    run(
        &mut shard,
        &mut sink,
        &["JSON.SET", "b", "$", r#"{"age":30.0}"#],
    );
    run(
        &mut shard,
        &mut sink,
        &["JSON.SET", "c", "$", r#"{"age":"30"}"#],
    );

    while shard.json_index_build_step(8) {}
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.FIND", "age", "VALUE", "30"]),
        "array:2",
        "30 and 30.0 are one number, the string is not indexed"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["JSON.FIND", "age", "VALUE", "abc"]),
        "ERR VALUE does not match the index type"
    );

    // A restart replays the definition key and the documents; the registry is rebuilt.
    let mut restarted = replayed(&sink);

    // Replay alone (as on a replica) registers the index; loading afterwards is idempotent.
    assert_eq!(restarted.json_index_list().len(), 1);
    restarted.json_index_load_definitions();
    assert_eq!(restarted.json_index_list().len(), 1);
    while restarted.json_index_build_step(8) {}

    assert_eq!(
        run(
            &mut restarted,
            &mut DatabaseLog::default(),
            &["JSON.FIND", "age", "VALUE", "30"],
        ),
        "array:2"
    );
}

#[test]
fn json_del_on_a_full_store_keeps_a_large_compact_document_compact() {
    let mut shard = KvShard::new();
    // Many tiny numbers keep the document compact; without them the rest would be a tree.
    let initial = serde_json::to_vec(&serde_json::json!({
        "l": (0..20_000).collect::<Vec<_>>(),
        "s": "x".repeat(2 * crate::json::TREE_THRESHOLD),
    }))
    .unwrap();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    execute(
        &mut shard,
        Command::JsonSet {
            key: owned(b"d"),
            path: owned(b"$"),
            value: Cow::Owned(initial),
        },
    );
    assert!(shard.json_tree(b"d").is_none());
    let charged = shard.memory_usage().charged_bytes();
    shard.set_memory_limit(Some(charged - 1));

    assert_eq!(
        execute(
            &mut shard,
            Command::JsonDel {
                key: owned(b"d"),
                path: owned(b"$.l"),
            }
        ),
        Response::Integer(1)
    );
    assert!(shard.json_tree(b"d").is_none());
    assert!(shard.memory_usage().charged_bytes() < charged);
    let document = shard.json_view(b"d").unwrap().unwrap().to_vec();
    assert!(document.starts_with(br#"{"s":"xx"#));
}
