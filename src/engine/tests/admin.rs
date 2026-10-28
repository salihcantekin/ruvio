use super::*;

#[test]
fn ruvio_answers_with_the_house_line() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(&mut shard, Command::Ruvio(None)),
        Response::Simple(b"PONG is taken")
    );
    assert_eq!(
        execute(&mut shard, Command::Ruvio(Some(owned(b"owner")))),
        Response::Simple(b"the core that owns the slot does the work")
    );
    assert_eq!(
        execute(&mut shard, Command::Ruvio(Some(owned(b"nope")))),
        Response::Error(b"ERR RUVIO expects zero arguments or OWNER")
    );
}

#[test]
fn wait_without_replicas_returns_zero_and_leaves_the_string() {
    let mut shard = KvShard::new();

    execute(&mut shard, Command::set(owned(b"s"), owned(b"value")));
    assert_eq!(
        execute(
            &mut shard,
            Command::Wait {
                replicas: 1,
                timeout_ms: 5
            }
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"s"))),
        Response::Bulk(Some(owned(b"value")))
    );
}

#[test]
fn a_string_rejects_the_other_types_commands() {
    let mut shard = KvShard::new();

    execute(&mut shard, Command::set(owned(b"s"), owned(b"value")));
    let commands: &[&[&[u8]]] = &[
        &[b"LPUSH", b"s", b"a"],
        &[b"RPUSH", b"s", b"a"],
        &[b"SADD", b"s", b"a"],
        &[b"HSET", b"s", b"f", b"v"],
        &[b"ZADD", b"s", b"1", b"a"],
        &[b"XADD", b"s", b"*", b"f", b"v"],
        &[b"LEASE", b"s", b"1000"],
        &[b"SEMAPHORE", b"s", b"2", b"1000"],
        &[b"LIMIT", b"s", b"1", b"1000"],
        &[b"ONCE", b"s", b"1000"],
    ];

    for words in commands {
        let command = crate::protocol::parse_arguments(
            words.iter().copied().map(|word| owned(word)).collect(),
        )
        .unwrap_or_else(|error| panic!("{}: {error}", String::from_utf8_lossy(words[0])));

        assert_eq!(
            execute(&mut shard, command),
            wrong_type(),
            "{}",
            String::from_utf8_lossy(words[0])
        );
    }

    assert_eq!(
        execute(&mut shard, Command::Get(owned(b"s"))),
        Response::Bulk(Some(owned(b"value")))
    );
}

#[test]
fn every_current_mutating_command_crosses_the_boundary() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();
    let commands = [
        Command::ListPush(owned(b"list"), vec![owned(b"item")].into()),
        Command::ListPop {
            key: owned(b"list"),
            count: None,
        },
        Command::SetAdd(owned(b"set"), vec![owned(b"member")]),
        Command::SetRemove(owned(b"set"), vec![owned(b"member")]),
        Command::SortedSetAdd {
            key: owned(b"scores"),
            pairs: vec![(1.5, owned(b"member"))].into(),
            options: SortedSetAddOptions::default(),
        },
        Command::BloomReserve(owned(b"seen"), 0.01, 100, 2),
        Command::BloomAdd(owned(b"seen"), owned(b"value")),
        Command::Del(vec![owned(b"scores"), owned(b"seen")].into()),
        Command::set(owned(b"ttl"), owned(b"value")),
        Command::Expire {
            condition: crate::protocol::ExpireCondition::Always,
            key: owned(b"ttl"),
            milliseconds: 5_000,
        },
        Command::Persist(owned(b"ttl")),
        Command::Del(vec![owned(b"ttl")].into()),
        Command::MSet(vec![(owned(b"m"), owned(b"1"))]),
        Command::GetSet(owned(b"m"), owned(b"2")),
        Command::Append(owned(b"m"), owned(b"x")),
        Command::Rename {
            source: owned(b"m"),
            destination: owned(b"n"),
        },
        Command::Del(vec![owned(b"n")].into()),
        Command::ListPushBack(owned(b"tail"), vec![owned(b"a"), owned(b"b")].into()),
        Command::ListTrim(owned(b"tail"), 1, 1),
        Command::ListPopBack {
            key: owned(b"tail"),
            count: None,
        },
        Command::SetAdd(owned(b"from"), vec![owned(b"moved")]),
        Command::SetAlgebra {
            keys: vec![owned(b"from")],
            op: SetOp::Union,
            destination: Some(owned(b"copy")),
        },
        Command::SetMove {
            source: owned(b"copy"),
            destination: owned(b"other"),
            member: owned(b"moved"),
        },
        Command::Del(vec![owned(b"from"), owned(b"other")].into()),
        Command::HashSet {
            key: owned(b"hash"),
            pairs: vec![(owned(b"f"), owned(b"v"))],
        },
        Command::HashDelete {
            key: owned(b"hash"),
            fields: vec![owned(b"f")],
        },
        Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: vec![(1.0, owned(b"a"))].into(),
            options: SortedSetAddOptions::default(),
        },
        Command::SortedSetIncr {
            key: owned(b"z"),
            delta: 1.0,
            member: owned(b"a"),
        },
        Command::SortedSetRemove {
            key: owned(b"z"),
            members: vec![owned(b"a")],
        },
    ];

    for command in commands {
        execute_with_sink(&mut shard, command, &mut sink).unwrap();
    }

    assert_eq!(sink.mutations.len(), 29);
    assert_eq!(
        sink.mutations
            .iter()
            .filter(|mutation| matches!(
                mutation,
                RecordedMutation::Other | RecordedMutation::Delete(_)
            ))
            .count(),
        26
    );
    assert!(shard.is_empty());
}

#[test]
fn restore_refuses_payloads_that_replay_could_not_apply() {
    let bad = [
        OwnedMutation::SortedSetAdd {
            key: Vec::new(),
            pairs: vec![(f64::NAN, b"m".to_vec())],
        },
        OwnedMutation::SortedSetAdd {
            key: Vec::new(),
            pairs: Vec::new(),
        },
        OwnedMutation::ListPushBack {
            key: Vec::new(),
            values: Vec::new(),
        },
        OwnedMutation::SetStore {
            key: Vec::new(),
            members: Vec::new(),
        },
        OwnedMutation::HashSet {
            key: Vec::new(),
            pairs: Vec::new(),
        },
        OwnedMutation::JsonPut {
            key: Vec::new(),
            value: b"{not json".to_vec(),
        },
    ];

    for value in bad {
        let payload = crate::dump::encode(&value).expect("payload encodes");
        let mut shard = KvShard::new();
        let mut log = DatabaseLog::default();
        let response = in_db(
            &mut shard,
            &mut log,
            0,
            Command::Restore {
                key: owned(b"k"),
                ttl: 0,
                payload: Cow::Owned(payload),
                replace: true,
                absolute: false,
            },
        )
        .into_owned();

        assert!(
            matches!(response, Response::Error(_) | Response::OwnedError(_)),
            "{value:?} -> {response:?}"
        );
        assert!(log.records.is_empty(), "{value:?} reached the WAL");
        assert!(!shard.contains_key(b"k"), "{value:?}");
    }

    // A good payload is restored and replays.
    let payload = crate::dump::encode(&OwnedMutation::SortedSetAdd {
        key: Vec::new(),
        pairs: vec![(1.5, b"m".to_vec())],
    })
    .unwrap();
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    assert_eq!(
        in_db(
            &mut shard,
            &mut log,
            0,
            Command::Restore {
                key: owned(b"k"),
                ttl: 0,
                payload: Cow::Owned(payload),
                replace: false,
                absolute: false,
            },
        )
        .into_owned(),
        Response::Simple(b"OK")
    );
    assert!(replayed(&log).contains_key(b"k"));
}
