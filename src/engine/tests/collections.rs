use super::*;

#[test]
fn set_family_cardinality_enumeration_and_same_slot_ops() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::SetAdd(owned(b"a"), vec![owned(b"x"), owned(b"y"), owned(b"z")])
        ),
        Response::Integer(3)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetAdd(owned(b"b"), vec![owned(b"y"), owned(b"w")])
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::SetCard(owned(b"a"))),
        Response::Integer(3)
    );
    assert_eq!(
        execute(&mut shard, Command::SetCard(owned(b"missing"))),
        Response::Integer(0)
    );
    assert_eq!(
        unordered(execute(&mut shard, Command::SetMembers(owned(b"a")))),
        Response::Set(vec![
            Cow::Borrowed(b"x"),
            Cow::Borrowed(b"y"),
            Cow::Borrowed(b"z"),
        ])
    );
    assert_eq!(
        unordered(execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"a"), owned(b"b")],
                op: SetOp::Inter,
                destination: None,
            }
        )),
        Response::Set(vec![Cow::Borrowed(b"y")])
    );
    assert_eq!(
        unordered(execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"a"), owned(b"b")],
                op: SetOp::Union,
                destination: None,
            }
        )),
        Response::Set(vec![
            Cow::Borrowed(b"w"),
            Cow::Borrowed(b"x"),
            Cow::Borrowed(b"y"),
            Cow::Borrowed(b"z"),
        ])
    );
    assert_eq!(
        unordered(execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"a"), owned(b"b")],
                op: SetOp::Diff,
                destination: None,
            }
        )),
        Response::Set(vec![Cow::Borrowed(b"x"), Cow::Borrowed(b"z")])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"a"), owned(b"b")],
                op: SetOp::Inter,
                destination: Some(owned(b"out")),
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        unordered(execute(&mut shard, Command::SetMembers(owned(b"out")))),
        Response::Set(vec![Cow::Borrowed(b"y")])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetMove {
                source: owned(b"a"),
                destination: owned(b"b"),
                member: owned(b"x"),
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::SetContains(owned(b"a"), owned(b"x"))),
        Response::Integer(0)
    );
    assert_eq!(
        execute(&mut shard, Command::SetContains(owned(b"b"), owned(b"x"))),
        Response::Integer(1)
    );
    execute(&mut shard, Command::set(owned(b"string"), owned(b"v")));
    assert_eq!(
        execute(&mut shard, Command::SetCard(owned(b"string"))),
        wrong_type()
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"string"), owned(b"a")],
                op: SetOp::Union,
                destination: None,
            }
        ),
        wrong_type()
    );
}

#[test]
fn set_union_replies_are_sorted_like_smembers() {
    let mut shard = KvShard::new();

    assert_eq!(
        shard.set_add(b"a", &[b"zz".as_slice(), b"aa", b"mm", b"bb"]),
        Ok(4)
    );
    assert_eq!(shard.set_add(b"b", &[b"cc".as_slice(), b"aa"]), Ok(2));

    let response = execute(
        &mut shard,
        Command::SetAlgebra {
            keys: vec![owned(b"a"), owned(b"b")],
            op: SetOp::Union,
            destination: None,
        },
    );

    assert_eq!(
        response,
        owned_byte_set(
            [b"aa", b"bb", b"cc", b"mm", b"zz"]
                .iter()
                .map(|member| member.to_vec())
                .collect()
        )
    );
}

#[test]
fn set_algebra_borrows_flat_and_indexed_sets_without_changing_results() {
    let mut shard = KvShard::new();
    let indexed: Vec<_> = (0..80).map(|i| format!("m{i:03}").into_bytes()).collect();
    assert_eq!(shard.set_add(b"indexed", &indexed), Ok(80));
    assert_eq!(
        shard.set_add(b"flat", &[b"m000".as_slice(), b"m079", b"other"]),
        Ok(3)
    );

    for (op, keys, expected) in [
        (
            SetOp::Inter,
            vec![b"indexed".as_slice(), b"flat"],
            vec![b"m000".as_slice(), b"m079"],
        ),
        (
            SetOp::Diff,
            vec![b"flat".as_slice(), b"indexed"],
            vec![b"other".as_slice()],
        ),
        (
            SetOp::Inter,
            vec![b"indexed".as_slice(), b"missing"],
            vec![],
        ),
        (
            SetOp::Diff,
            vec![b"flat".as_slice(), b"missing"],
            vec![b"m000".as_slice(), b"m079", b"other"],
        ),
    ] {
        let response = execute(
            &mut shard,
            Command::SetAlgebra {
                keys: keys.iter().map(|key| owned(key)).collect(),
                op,
                destination: None,
            },
        );
        assert_eq!(
            response,
            owned_byte_set(expected.into_iter().map(<[u8]>::to_vec).collect())
        );
    }

    assert_eq!(
        execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"flat"), owned(b"indexed")],
                op: SetOp::Inter,
                destination: Some(owned(b"indexed")),
            }
        ),
        Response::Integer(2)
    );
    assert_eq!(
        shard.set_members(b"indexed").map(|members| members
            .into_iter()
            .map(|m| m.into_owned())
            .collect::<Vec<_>>()),
        Ok(vec![b"m000".to_vec(), b"m079".to_vec()])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SetAlgebra {
                keys: vec![owned(b"flat"), owned(b"missing")],
                op: SetOp::Inter,
                destination: Some(owned(b"flat")),
            }
        ),
        Response::Integer(0)
    );
    assert_eq!(shard.set_card(b"flat"), Ok(0));
}

#[test]
fn hash_family_fields_compact_representation_and_wrong_type() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"user"),
                pairs: vec![
                    (owned(b"name"), owned(b"ada")),
                    (owned(b"city"), owned(b"london"))
                ],
            }
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashSet {
                key: owned(b"user"),
                pairs: vec![(owned(b"name"), owned(b"grace"))],
            }
        ),
        Response::Integer(0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"user"),
                field: owned(b"name"),
            }
        ),
        Response::Bulk(Some(Cow::Borrowed(b"grace")))
    );
    assert_eq!(
        execute(&mut shard, Command::HashLen(owned(b"user"))),
        Response::Integer(2)
    );
    assert_eq!(
        unordered(execute(&mut shard, Command::HashGetAll(owned(b"user")))),
        Response::Map(vec![
            (
                Cow::Borrowed(b"city"),
                Response::Bulk(Some(Cow::Borrowed(b"london"))),
            ),
            (
                Cow::Borrowed(b"name"),
                Response::Bulk(Some(Cow::Borrowed(b"grace"))),
            ),
        ])
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::HashMGet {
                key: owned(b"user"),
                fields: vec![owned(b"city"), owned(b"missing")],
            }
        )),
        Response::Array(vec![Some(Cow::Borrowed(b"london")), None])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashExists {
                key: owned(b"user"),
                field: owned(b"city"),
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        unordered(execute(&mut shard, Command::HashKeys(owned(b"user")))),
        Response::Array(vec![
            Some(Cow::Borrowed(b"city")),
            Some(Cow::Borrowed(b"name")),
        ])
    );
    assert_eq!(
        unordered(execute(&mut shard, Command::HashValues(owned(b"user")))),
        Response::Array(vec![
            Some(Cow::Borrowed(b"grace")),
            Some(Cow::Borrowed(b"london")),
        ])
    );
    assert_eq!(
        execute(&mut shard, Command::Type(owned(b"user"))),
        Response::Simple(b"hash")
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::HashDelete {
                key: owned(b"user"),
                fields: vec![owned(b"city")],
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::HashLen(owned(b"missing"))),
        Response::Integer(0)
    );
    execute(&mut shard, Command::set(owned(b"string"), owned(b"v")));
    assert_eq!(
        execute(
            &mut shard,
            Command::HashGet {
                key: owned(b"string"),
                field: owned(b"name"),
            }
        ),
        wrong_type()
    );
    let mut sink = RecordingSink::default();

    execute_with_sink(
        &mut shard,
        Command::HashSet {
            key: owned(b"string"),
            pairs: vec![(owned(b"a"), owned(b"b"))],
        },
        &mut sink,
    )
    .unwrap();
    assert!(sink.mutations.is_empty());
}

#[test]
fn sadd_of_members_already_present_logs_nothing() {
    let mut shard = KvShard::new();
    let mut sink = RecordingSink::default();

    execute_with_sink(
        &mut shard,
        Command::SetAdd(owned(b"s"), vec![owned(b"a"), owned(b"b")]),
        &mut sink,
    )
    .unwrap();
    assert_eq!(sink.mutations.len(), 1);

    // Nothing changes, so nothing reaches the log; Redis propagates nothing either.
    let reply = execute_with_sink(
        &mut shard,
        Command::SetAdd(owned(b"s"), vec![owned(b"a")]),
        &mut sink,
    )
    .unwrap();

    assert_eq!(reply, Response::Integer(0));
    assert_eq!(sink.mutations.len(), 1);

    // One new member among known ones is a change and is logged.
    let reply = execute_with_sink(
        &mut shard,
        Command::SetAdd(owned(b"s"), vec![owned(b"a"), owned(b"c")]),
        &mut sink,
    )
    .unwrap();

    assert_eq!(reply, Response::Integer(1));
    assert_eq!(sink.mutations.len(), 2);
}

#[test]
fn sorted_set_family_pairs_scores_rank_and_increments() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetAdd {
                key: owned(b"z"),
                pairs: vec![(2.0, owned(b"b")), (1.0, owned(b"a")), (3.0, owned(b"c"))].into(),
                options: SortedSetAddOptions::default(),
            }
        ),
        Response::Integer(3)
    );
    assert_eq!(
        execute(&mut shard, Command::SortedSetCard(owned(b"z"))),
        Response::Integer(3)
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRange {
                key: owned(b"z"),
                start: 0,
                stop: -1,
                with_scores: true,
                reverse: false,
            }
        )),
        Response::Array(vec![
            Some(Cow::Borrowed(b"a")),
            Some(Cow::Owned(b"1".to_vec())),
            Some(Cow::Borrowed(b"b")),
            Some(Cow::Owned(b"2".to_vec())),
            Some(Cow::Borrowed(b"c")),
            Some(Cow::Owned(b"3".to_vec())),
        ])
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRange {
                key: owned(b"z"),
                start: 0,
                stop: 1,
                with_scores: false,
                reverse: true,
            }
        )),
        Response::Array(vec![Some(Cow::Borrowed(b"c")), Some(Cow::Borrowed(b"b"))])
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRangeByScore {
                key: owned(b"z"),
                min: crate::protocol::ScoreBound::Exclusive(1.0),
                max: crate::protocol::ScoreBound::Inclusive(2.0),
                with_scores: false,
                reverse: false,
                limit: None,
                via_zrange: false,
            }
        )),
        Response::Array(vec![Some(Cow::Borrowed(b"b"))])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetRank {
                key: owned(b"z"),
                member: owned(b"b"),
                reverse: false,
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetRank {
                key: owned(b"z"),
                member: owned(b"b"),
                reverse: true,
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetScore {
                key: owned(b"z"),
                member: owned(b"a"),
            }
        ),
        Response::Double(1.0)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetIncr {
                key: owned(b"z"),
                delta: 0.5,
                member: owned(b"a"),
            }
        ),
        Response::Double(1.5)
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::SortedSetRemove {
                key: owned(b"z"),
                members: vec![owned(b"c"), owned(b"missing")],
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        execute(&mut shard, Command::SortedSetCard(owned(b"z"))),
        Response::Integer(2)
    );
    execute(&mut shard, Command::set(owned(b"string"), owned(b"v")));
    assert_eq!(
        execute(&mut shard, Command::SortedSetCard(owned(b"string"))),
        wrong_type()
    );
    let mut sink = RecordingSink::default();

    execute_with_sink(
        &mut shard,
        Command::SortedSetAdd {
            key: owned(b"string"),
            pairs: vec![(1.0, owned(b"x"))].into(),
            options: SortedSetAddOptions::default(),
        },
        &mut sink,
    )
    .unwrap();
    assert!(sink.mutations.is_empty());
}

#[test]
fn sorted_set_lex_range_orders_members_and_limits() {
    // ZRANGEBYLEX and ZREVRANGEBYLEX share this member order.
    let mut shard = KvShard::new();

    execute(
        &mut shard,
        Command::SortedSetAdd {
            key: owned(b"names"),
            pairs: vec![(1.0, owned(b"c")), (5.0, owned(b"a")), (1.0, owned(b"b"))].into(),
            options: SortedSetAddOptions::default(),
        },
    );

    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRangeByLex {
                key: owned(b"names"),
                min: crate::protocol::LexBound::NegInf,
                max: crate::protocol::LexBound::PosInf,
                with_scores: false,
                reverse: false,
                limit: None,
                invocation: crate::protocol::LexInvocation::ZRange,
            }
        )),
        Response::Array(vec![
            Some(Cow::Borrowed(b"a")),
            Some(Cow::Borrowed(b"b")),
            Some(Cow::Borrowed(b"c")),
        ])
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRangeByLex {
                key: owned(b"names"),
                min: crate::protocol::LexBound::Exclusive(owned(b"a")),
                max: crate::protocol::LexBound::Inclusive(owned(b"c")),
                with_scores: true,
                reverse: false,
                limit: Some(crate::protocol::RangeLimit {
                    offset: 1,
                    count: Some(1),
                }),
                invocation: crate::protocol::LexInvocation::ZRangeByLex,
            }
        )),
        Response::Array(vec![
            Some(Cow::Borrowed(b"c")),
            Some(Cow::Owned(b"1".to_vec())),
        ])
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::SortedSetRangeByScore {
                key: owned(b"names"),
                min: crate::protocol::ScoreBound::Inclusive(1.0),
                max: crate::protocol::ScoreBound::Inclusive(5.0),
                with_scores: false,
                reverse: true,
                limit: Some(crate::protocol::RangeLimit {
                    offset: 0,
                    count: Some(1),
                }),
                via_zrange: true,
            }
        )),
        Response::Array(vec![Some(Cow::Borrowed(b"a"))])
    );
}

#[test]
fn list_family_commands_and_waiters_match_redis_order() {
    let mut shard = KvShard::new();

    assert_eq!(
        execute(
            &mut shard,
            Command::ListPushBack(owned(b"list"), vec![owned(b"a"), owned(b"b")].into())
        ),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::ListLen(owned(b"list"))),
        Response::Integer(2)
    );
    assert_eq!(
        execute(&mut shard, Command::ListIndex(owned(b"list"), -1)),
        Response::Bulk(Some(Cow::Borrowed(b"b")))
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::ListPopBack {
                key: owned(b"list"),
                count: None,
            }
        ),
        Response::Bulk(Some(Cow::Owned(b"b".to_vec())))
    );
    execute(&mut shard, Command::ListTrim(owned(b"list"), 0, 0));
    assert_eq!(
        execute(&mut shard, Command::ListLen(owned(b"list"))),
        Response::Integer(1)
    );
    assert_eq!(
        execute_owned(execute(
            &mut shard,
            Command::ListBlock {
                keys: vec![owned(b"list")],
                timeout_ms: Some(10),
                back: false,
            }
        )),
        Response::Array(vec![
            Some(Cow::Owned(b"list".to_vec())),
            Some(Cow::Owned(b"a".to_vec())),
        ])
    );
    assert_eq!(
        execute(
            &mut shard,
            Command::ListBlock {
                keys: vec![owned(b"empty")],
                timeout_ms: Some(10),
                back: false,
            }
        ),
        Response::BlockList {
            keys: vec![Cow::Owned(tagged(0, b"empty"))],
            timeout_ms: Some(10),
            back: false,
            sorted: None,
        }
    );

    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let (reply, receiver) = std::sync::mpsc::sync_channel(1);

    waiters
        .register(vec![tagged(0, b"empty")], None, reply, b"*-1\r\n".to_vec())
        .unwrap();
    assert_eq!(
        execute_with_waiters(
            &mut shard,
            Command::ListPushBack(owned(b"empty"), vec![owned(b"x"), owned(b"y")].into()),
            &mut NoopMutationSink,
            &mut waiters
        )
        .unwrap(),
        Response::Integer(2)
    );
    assert_eq!(
        receiver.try_recv().unwrap(),
        block_hit_response(b"empty", b"x")
    );
    assert_eq!(shard.list_range(b"empty", 0, -1).unwrap(), vec![&b"y"[..]]);
}

#[test]
fn scored_replies_write_the_same_bytes_as_the_collected_array() {
    let members: [&[u8]; 8] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
    let scores = [
        1.0,
        -2.0,
        1.5,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e-300,
        1.7976931348623157e308,
        -0.0,
    ];
    let entries: Vec<(&[u8], f64)> = members.iter().copied().zip(scores).collect();
    let mut streamed = Vec::new();
    let mut collected = Vec::new();

    write_response(&mut streamed, Response::Scored(entries.clone(), true)).unwrap();

    let mut values = Vec::new();

    for (member, score) in entries {
        values.push(Some(Cow::Borrowed(member)));
        values.push(Some(Cow::Owned(format_score(score))));
    }

    write_response(&mut collected, Response::Array(values)).unwrap();
    assert_eq!(streamed, collected);
}

fn parked_on(
    key: &[u8],
) -> (
    crate::list_waiters::ListWaiters<crate::list_waiters::EncodedReplySlot>,
    crate::list_waiters::EncodedReplySlot,
) {
    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let slot = crate::list_waiters::EncodedReplySlot::default();

    waiters
        .register(vec![tagged(0, key)], None, slot.clone(), Vec::new())
        .unwrap();
    (waiters, slot)
}

#[test]
fn a_blocked_client_gets_data_only_after_the_write_is_recorded() {
    // A push whose remaining elements cannot be recorded hands nothing out.
    let mut shard = KvShard::new();
    let (mut waiters, slot) = parked_on(b"q");

    assert!(
        execute_with_waiters(
            &mut shard,
            command_of(&[b"RPUSH", b"q", b"a", b"b"]),
            &mut FailingSink,
            &mut waiters
        )
        .is_err()
    );
    assert_eq!(slot.take(), None);
    assert_eq!(waiters.len(), 1, "the client is still parked");

    // The same inside MULTI, whose batch the log refuses.
    assert!(
        execute_transaction(
            &mut shard,
            &mut FailingSink,
            &mut waiters,
            1,
            vec![command_of(&[b"RPUSH", b"q", b"a", b"b"])],
            0,
        )
        .is_err()
    );
    assert_eq!(slot.take(), None);
    assert_eq!(waiters.len(), 1);

    // LMOVE onto the waited key: the move record fails.
    let (mut moved, moved_slot) = parked_on(b"dst");
    let Ok(_) = execute_with_waiters(
        &mut shard,
        command_of(&[b"RPUSH", b"src", b"v"]),
        &mut NoopMutationSink,
        &mut moved,
    );
    assert!(
        execute_with_waiters(
            &mut shard,
            command_of(&[b"LMOVE", b"src", b"dst", b"LEFT", b"LEFT"]),
            &mut FailingSink,
            &mut moved
        )
        .is_err()
    );
    assert_eq!(moved_slot.take(), None);
    assert_eq!(moved.len(), 1);

    // Recorded, the data goes out.
    let mut log = RecordingSink::default();
    let Ok(reply) = execute_transaction(
        &mut shard,
        &mut log,
        &mut waiters,
        1,
        vec![command_of(&[b"RPUSH", b"q", b"a"])],
        0,
    );
    // The length as if the push came before the hand-off, as in Redis.
    assert_eq!(reply, Response::Exec(vec![Response::Integer(1)]));
    assert_eq!(
        slot.take().as_deref(),
        Some(b"*2\r\n$1\r\nq\r\n$1\r\na\r\n".as_slice())
    );
    assert!(waiters.is_empty());
}

#[test]
fn a_push_handed_to_a_blocked_client_replies_the_length_before_the_hand_off() {
    // Redis counts every element as pushed before blocked clients take theirs.
    type Case = (&'static [&'static [u8]], i64, &'static [u8]);
    let cases: [Case; 4] = [
        (&[b"RPUSH", b"q", b"a"], 1, b""),
        (&[b"LPUSH", b"q", b"a"], 1, b""),
        // One element goes to the client, the other stays in the list.
        (&[b"RPUSH", b"q", b"a", b"b"], 2, b"b"),
        (&[b"RPUSHCAP", b"q", b"5", b"a", b"b", b"c"], 3, b"bc"),
    ];

    for (command, length, kept) in cases {
        let mut shard = KvShard::new();
        let (mut waiters, slot) = parked_on(b"q");
        let Ok(reply) = execute_with_waiters(
            &mut shard,
            command_of(command),
            &mut NoopMutationSink,
            &mut waiters,
        );

        assert_eq!(reply, Response::Integer(length), "{command:?}");
        assert_eq!(
            slot.take().as_deref(),
            Some(b"*2\r\n$1\r\nq\r\n$1\r\na\r\n".as_slice())
        );
        let expected: Vec<&[u8]> = kept.chunks(1).collect();

        assert_eq!(
            shard.list_range(b"q", 0, -1).unwrap_or_default(),
            expected,
            "{command:?}"
        );
    }

    // Over its cap, RPUSHCAP still replies the cap.
    let mut shard = KvShard::new();
    let (mut waiters, slot) = parked_on(b"q");
    let Ok(reply) = execute_with_waiters(
        &mut shard,
        command_of(&[b"RPUSHCAP", b"q", b"1", b"a", b"b", b"c"]),
        &mut NoopMutationSink,
        &mut waiters,
    );

    assert_eq!(reply, Response::Integer(1));
    assert!(slot.take().is_some());
}
