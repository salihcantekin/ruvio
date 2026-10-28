use super::*;

#[test]
fn sorted_pops_and_range_removals_reuse_the_remove_record() {
    // ZPOPMIN ZPOPMAX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX BZPOPMIN BZPOPMAX
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let run = |shard: &mut KvShard, sink: &mut DatabaseLog, command| {
        execute_with_sink(shard, command, sink)
            .unwrap()
            .into_owned()
    };

    run(
        &mut shard,
        &mut sink,
        Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
                .into_iter()
                .zip([b"a", b"b", b"c", b"d", b"e", b"f"])
                .map(|(score, member)| (score, owned(member)))
                .collect(),
            options: crate::protocol::SortedSetAddOptions::default(),
        },
    );

    let records = sink.records.len();

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetPop {
                key: owned(b"z"),
                count: Some(0),
                max: false,
            }
        ),
        Response::Popped(None, PoppedEntries::default())
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetRemoveRange {
                key: owned(b"z"),
                range: SortedRemoveRange::Score(ScoreBound::Inclusive(9.0), ScoreBound::PosInf),
            }
        ),
        Response::Integer(0)
    );
    assert_eq!(
        sink.records.len(),
        records,
        "an empty removal writes no record"
    );

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetPop {
                key: owned(b"z"),
                count: None,
                max: false,
            }
        ),
        popped(None, &[(b"a", 1.0)])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetPop {
                key: owned(b"z"),
                count: Some(2),
                max: true,
            }
        ),
        popped(None, &[(b"f", 6.0), (b"e", 5.0)])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetRemoveRange {
                key: owned(b"z"),
                range: SortedRemoveRange::Rank(-1, -1),
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetRemoveRange {
                key: owned(b"z"),
                range: SortedRemoveRange::Lex(
                    LexBound::Inclusive(owned(b"b")),
                    LexBound::Exclusive(owned(b"c")),
                ),
            }
        ),
        Response::Integer(1)
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            Command::SortedSetBlockPop {
                keys: vec![owned(b"none"), owned(b"z")],
                timeout_ms: Some(10),
                max: false,
            }
        ),
        popped(Some(b"z"), &[(b"c", 3.0)])
    );
    assert_eq!(
        execute_with_sink(
            &mut shard,
            Command::SortedSetBlockPop {
                keys: vec![owned(b"z")],
                timeout_ms: Some(10),
                max: true,
            },
            &mut sink,
        )
        .unwrap(),
        Response::BlockList {
            keys: vec![Cow::Owned(tagged(0, b"z"))],
            timeout_ms: Some(10),
            back: false,
            sorted: Some(true),
        }
    );
    assert!(
        sink.records[records..]
            .iter()
            .all(|(_, mutation)| matches!(mutation, OwnedMutation::SortedSetRemove { .. }))
    );
    assert_eq!(shard.value_kind(b"z"), None);
    assert_eq!(replayed(&sink).value_kind(b"z"), None);
}

#[test]
fn a_sorted_set_write_serves_a_parked_bzpopmin() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut waiters =
        crate::list_waiters::ListWaiters::<crate::list_waiters::EncodedReplySlot>::new(4);
    let list_slot = crate::list_waiters::EncodedReplySlot::default();
    let sorted_slot = crate::list_waiters::EncodedReplySlot::default();

    waiters
        .register(vec![tagged(0, b"z")], None, list_slot.clone(), Vec::new())
        .unwrap();
    waiters
        .register_sorted(
            vec![tagged(0, b"z")],
            false,
            None,
            sorted_slot.clone(),
            Vec::new(),
        )
        .unwrap();

    let added = execute_with_waiters(
        &mut shard,
        Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: vec![(2.0, owned(b"b")), (1.0, owned(b"a"))].into(),
            options: crate::protocol::SortedSetAddOptions::default(),
        },
        &mut sink,
        &mut waiters,
    )
    .unwrap()
    .into_owned();

    assert_eq!(added, Response::Integer(2));
    assert_eq!(
        sorted_slot.take().as_deref(),
        Some(b"*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\n1\r\n".as_slice())
    );
    assert!(
        list_slot.take().is_none(),
        "a list waiter is not served by ZADD"
    );
    assert_eq!(waiters.len(), 1);
    assert_eq!(shard.sorted_set_card(b"z").unwrap(), 1);
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::SortedSetRemove { members, .. })) if members == &vec![b"a".to_vec()]
    ));
}

fn popped(key: Option<&[u8]>, entries: &[(&[u8], f64)]) -> Response<'static> {
    let mut popped = PoppedEntries::default();

    for (member, score) in entries {
        popped.push(member, *score);
    }

    Response::Popped(key.map(owned), popped)
}

fn written(response: Response<'_>, version: RespVersion) -> Vec<u8> {
    let mut out = Vec::new();

    write_response_in(&mut out, response, version).unwrap();
    out
}

fn pop_command(key: &[u8], count: Option<usize>, max: bool) -> Command<'static> {
    Command::SortedSetPop {
        key: owned(key),
        count,
        max,
    }
}

/// A pop reply is the flat member/score array Redis sends, in RESP2 and RESP3 alike, and
/// the record lists the members in pop order, as `ZREM` of them would.
#[test]
fn sorted_pops_write_member_score_pairs_and_record_members_in_pop_order() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    execute_with_sink(
        &mut shard,
        Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: vec![
                (1.5, owned(b"low")),
                (f64::INFINITY, owned(b"top")),
                (-2.0, owned(b"neg")),
                (3.0, owned(b"mid")),
            ]
            .into(),
            options: crate::protocol::SortedSetAddOptions::default(),
        },
        &mut sink,
    )
    .unwrap();

    let reply = execute_with_sink(&mut shard, pop_command(b"z", Some(2), false), &mut sink)
        .unwrap()
        .into_owned();
    let expected = b"*4\r\n$3\r\nneg\r\n$2\r\n-2\r\n$3\r\nlow\r\n$3\r\n1.5\r\n";

    assert_eq!(written(reply.clone(), RespVersion::V2), expected);
    assert_eq!(written(reply, RespVersion::V3), expected);
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::SortedSetRemove { key, members }))
            if key == b"z" && members == &vec![b"neg".to_vec(), b"low".to_vec()]
    ));

    let reply = execute_with_sink(&mut shard, pop_command(b"z", Some(5), true), &mut sink)
        .unwrap()
        .into_owned();

    assert_eq!(
        written(reply, RespVersion::V2),
        b"*4\r\n$3\r\ntop\r\n$3\r\ninf\r\n$3\r\nmid\r\n$1\r\n3\r\n"
    );
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::SortedSetRemove { members, .. }))
            if members == &vec![b"top".to_vec(), b"mid".to_vec()]
    ));
    assert_eq!(shard.value_kind(b"z"), None);
    assert_eq!(replayed(&sink).value_kind(b"z"), None);

    let records = sink.records.len();

    assert_eq!(
        written(
            execute_with_sink(&mut shard, pop_command(b"z", None, false), &mut sink).unwrap(),
            RespVersion::V2
        ),
        b"*0\r\n"
    );
    execute_with_sink(
        &mut shard,
        Command::Set(owned(b"s"), owned(b"v"), Default::default()),
        &mut sink,
    )
    .unwrap();
    assert!(matches!(
        execute_with_sink(&mut shard, pop_command(b"s", None, false), &mut sink).unwrap(),
        Response::Error(message) if message.starts_with(b"WRONGTYPE")
    ));
    assert_eq!(sink.records.len(), records + 1, "only the SET is recorded");
}

/// Pops from both ends of a large (indexed) set, then a small one, against a sorted model:
/// replies, what is left, and the replayed log agree.
#[test]
fn sorted_pops_from_both_ends_follow_a_model_and_replay() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut model: Vec<(f64, Vec<u8>)> = (0..400u32)
        .map(|index| {
            (
                f64::from(index % 37),
                format!("member-{index:04}").into_bytes(),
            )
        })
        .collect();

    execute_with_sink(
        &mut shard,
        Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: model
                .iter()
                .map(|(score, member)| (*score, owned(member)))
                .collect(),
            options: crate::protocol::SortedSetAddOptions::default(),
        },
        &mut sink,
    )
    .unwrap();
    model.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut round = 0usize;

    while !model.is_empty() {
        let max = round % 3 == 1;
        let count = 1 + round % 4;
        let take = count.min(model.len());
        let expected: Vec<(f64, Vec<u8>)> = if max {
            model.drain(model.len() - take..).rev().collect()
        } else {
            model.drain(..take).collect()
        };
        let reply = execute_with_sink(&mut shard, pop_command(b"z", Some(count), max), &mut sink)
            .unwrap()
            .into_owned();
        let Response::Popped(None, entries) = reply else {
            panic!("pop reply: {reply:?}");
        };

        assert_eq!(
            entries
                .iter()
                .map(|(member, score)| (score, member.to_vec()))
                .collect::<Vec<_>>(),
            expected,
            "round {round}"
        );
        assert_eq!(shard.sorted_set_card(b"z").unwrap(), model.len());
        round += 1;
    }

    assert_eq!(shard.value_kind(b"z"), None);
    assert_eq!(replayed(&sink).value_kind(b"z"), None);
}

fn run_logged(shard: &mut KvShard, sink: &mut DatabaseLog, frame: &[u8]) -> Response<'static> {
    let (command, _) = parse_command(frame).unwrap().expect("complete frame");

    execute_with_sink(shard, command.into_owned(), sink)
        .unwrap()
        .into_owned()
}

fn zadd_frame(key: &[u8], extra: &[u8], score: &[u8], member: &[u8]) -> Vec<u8> {
    let mut parts: Vec<&[u8]> = vec![b"ZADD", key];

    if !extra.is_empty() {
        parts.push(extra);
    }

    parts.extend([score, member]);
    frame_of(&parts)
}

fn frame_of(parts: &[&[u8]]) -> Vec<u8> {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        frame.extend(format!("${}\r\n", part.len()).bytes());
        frame.extend_from_slice(part);
        frame.extend_from_slice(b"\r\n");
    }

    frame
}

fn charged(shard: &KvShard) -> usize {
    let usage = shard.memory_usage();

    usage.key_bytes + usage.value_bytes + usage.collection_bytes
}

#[test]
fn sorted_set_full_arena_refuses_cleanly_and_replays() {
    let _limit = crate::arena::limit_chunks_for_test(1);
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let member = |index: usize| {
        let mut member = format!("m{index:05}").into_bytes();

        member.resize(200, b'x');
        member
    };
    let full = Response::Error(b"ERR sorted set cannot hold more members");
    let mut stored = 0;

    for index in 0..1_000 {
        let reply = run_logged(
            &mut shard,
            &mut sink,
            &zadd_frame(b"z", b"", b"1", &member(index)),
        );

        if reply == full {
            break;
        }

        assert_eq!(reply, Response::Integer(1));
        stored += 1;
    }

    assert!(stored > 128 && stored < 1_000, "stored {stored}");
    assert_eq!(shard.sorted_set_card(b"z"), Ok(stored));

    let records = sink.records.len();
    let before = charged(&shard);

    // ZADD with several members, ZADD INCR and ZINCRBY of new members are refused before
    // anything is logged or inserted.
    let mut multi = vec![b"ZADD".as_slice(), b"z"];
    let (a, b) = (member(900), member(901));

    multi.extend([b"5".as_slice(), &a, b"6", &b]);
    assert_eq!(run_logged(&mut shard, &mut sink, &frame_of(&multi)), full);
    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &zadd_frame(b"z", b"INCR", b"1", &member(902))
        ),
        full
    );
    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &frame_of(&[b"ZINCRBY", b"z", b"1", &member(903)])
        ),
        full
    );
    assert_eq!(sink.records.len(), records, "a refused write is not logged");
    assert_eq!(shard.sorted_set_card(b"z"), Ok(stored));
    assert_eq!(charged(&shard), before);
    assert_eq!(charged(&shard), shard.key_memory_usage(b"z").unwrap());

    // Rescoring a member needs no new block.
    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &frame_of(&[b"ZINCRBY", b"z", b"2", &member(0)])
        ),
        Response::Double(3.0)
    );

    // Freeing members makes room again.
    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &frame_of(&[b"ZREM", b"z", &member(1), &member(2)])
        ),
        Response::Integer(2)
    );
    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &zadd_frame(b"z", b"", b"7", &member(904))
        ),
        Response::Integer(1)
    );
    assert_eq!(charged(&shard), shard.key_memory_usage(b"z").unwrap());

    let replayed = replayed(&sink);

    assert_eq!(replayed.sorted_set_card(b"z"), shard.sorted_set_card(b"z"));
    assert_eq!(replayed.sorted_set_score(b"z", &member(0)), Ok(Some(3.0)));
}

#[test]
fn sorted_store_into_full_destination_is_refused_before_logging() {
    let _limit = crate::arena::limit_chunks_for_test(1);
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut stored = 0usize;

    // Two sources that fit one chunk each but not together.
    for (key, base) in [(b"a".as_slice(), 0usize), (b"b".as_slice(), 1_000)] {
        for index in 0..200 {
            let mut member = format!("m{:05}", base + index).into_bytes();

            member.resize(200, b'x');
            let reply = run_logged(&mut shard, &mut sink, &zadd_frame(key, b"", b"1", &member));

            if reply == Response::Integer(1) {
                stored += 1;
            }
        }
    }

    assert_eq!(stored, 400);

    let records = sink.records.len();

    assert_eq!(
        run_logged(
            &mut shard,
            &mut sink,
            &frame_of(&[b"ZUNIONSTORE", b"dest", b"2", b"a", b"b"])
        ),
        Response::Error(b"ERR sorted set cannot hold more members")
    );
    assert_eq!(sink.records.len(), records);
    assert_eq!(shard.sorted_set_card(b"dest"), Ok(0));
}

#[test]
fn sorted_set_add_pairs_keeps_partial_insert_charged_on_full() {
    let _limit = crate::arena::limit_chunks_for_test(1);
    let mut shard = KvShard::new();
    let pairs: Vec<(f64, Vec<u8>)> = (0..1_000)
        .map(|index| {
            let mut member = format!("m{index:05}").into_bytes();

            member.resize(200, b'x');
            (index as f64, member)
        })
        .collect();

    assert_eq!(
        shard.sorted_set_add_pairs(b"z", &pairs),
        Err(SortedSetError::Full)
    );

    let card = shard.sorted_set_card(b"z").unwrap();

    assert!(card > 128 && card < 1_000, "card {card}");
    assert_eq!(charged(&shard), shard.key_memory_usage(b"z").unwrap());
}

/// One scan page of key `k`: the next cursor and the members returned.
type ScanPage = dyn Fn(&KvShard, u64) -> (u64, Vec<Vec<u8>>);

/// Walks `key` with COUNT 100 while a rolling window adds and removes members between
/// pages, and every 20 pages grows the collection far past its size and empties that
/// growth again, so the table is rebuilt under the cursor many times per walk.
fn scan_under_churn(
    kind: &str,
    scan: &ScanPage,
) -> (
    usize,
    std::collections::HashSet<Vec<u8>>,
    std::ops::Range<u32>,
) {
    let mut shard = KvShard::new();
    // One command adds or removes many members: each still goes in or out one at a time.
    let add_all = |shard: &mut KvShard, members: &[String]| {
        let mut parts: Vec<&[u8]> = match kind {
            "zset" => vec![b"ZADD", b"k"],
            "hash" => vec![b"HSET", b"k"],
            _ => vec![b"SADD", b"k"],
        };

        for member in members {
            match kind {
                "zset" => parts.extend([b"1".as_slice(), member.as_bytes()]),
                "hash" => parts.extend([member.as_bytes(), b"v"]),
                _ => parts.push(member.as_bytes()),
            }
        }
        let reply = execute(shard, command_of(&parts)).into_owned();

        assert!(!matches!(reply, Response::Error(_)), "{reply:?}");
    };
    let remove_all = |shard: &mut KvShard, members: &[String]| {
        let verb: &[u8] = match kind {
            "zset" => b"ZREM",
            "hash" => b"HDEL",
            _ => b"SREM",
        };
        let mut parts: Vec<&[u8]> = vec![verb, b"k"];

        parts.extend(members.iter().map(String::as_bytes));
        execute(shard, command_of(&parts));
    };
    let add = |shard: &mut KvShard, member: &str| add_all(shard, &[member.to_owned()]);
    let remove = |shard: &mut KvShard, member: &str| remove_all(shard, &[member.to_owned()]);
    let transient: Vec<String> = (0..20_000).map(|index| format!("x{index}")).collect();
    let window = 5_000u32;
    let (mut low, mut high) = (0u32, window);

    add_all(
        &mut shard,
        &(low..high)
            .map(|index| format!("m{index}"))
            .collect::<Vec<_>>(),
    );

    let initial_high = high;
    let mut seen = std::collections::HashSet::new();
    let mut cursor = 0;
    let mut pages = 0;

    loop {
        let (next, members) = scan(&shard, cursor);

        seen.extend(members);
        pages += 1;
        assert!(pages < 1_000, "{kind}: the scan did not end");
        if next == 0 {
            break;
        }
        cursor = next;

        for _ in 0..60 {
            add(&mut shard, &format!("m{high}"));
            remove(&mut shard, &format!("m{low}"));
            high += 1;
            low += 1;
        }
        if pages % 20 == 5 {
            add_all(&mut shard, &transient);
        }
        if pages % 20 == 15 {
            remove_all(&mut shard, &transient);
        }
    }

    (pages, seen, low..initial_high)
}

#[test]
fn hashed_collection_scans_end_and_miss_nothing_while_the_table_changes_under_them() {
    let zset = |shard: &KvShard, cursor| {
        let (next, page) = shard.sorted_set_scan(b"k", cursor, 100, None).unwrap();

        (
            next,
            page.iter().map(|(member, _)| member.to_vec()).collect(),
        )
    };
    let hash = |shard: &KvShard, cursor| {
        let (next, page) = shard.hash_scan(b"k", cursor, 100, None).unwrap();

        (next, page.iter().map(|(field, _)| field.to_vec()).collect())
    };
    let set = |shard: &KvShard, cursor| {
        let (next, page) = shard.set_scan(b"k", cursor, 100, None).unwrap();

        (next, page.iter().map(|member| member.to_vec()).collect())
    };
    let scans: [(&str, &ScanPage); 3] = [("zset", &zset), ("hash", &hash), ("set", &set)];

    for (kind, scan) in scans {
        let (pages, seen, kept) = scan_under_churn(kind, scan);

        // Every member present for the whole walk was returned.
        for index in kept {
            assert!(
                seen.contains(format!("m{index}").as_bytes()),
                "{kind}: m{index} was skipped"
            );
        }
        assert!(pages > 10, "{kind}: the walk was paged ({pages} pages)");
    }
}

#[test]
fn copy_refuses_a_sorted_set_this_shard_cannot_hold_before_logging_it() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut args: Vec<Vec<u8>> = vec![b"ZADD".to_vec(), b"src".to_vec()];

    for index in 0..2_000u32 {
        args.push(index.to_string().into_bytes());
        args.push(format!("{index:0>100}").into_bytes());
    }

    let parts: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    assert_eq!(
        execute_with_sink(&mut shard, command_of(&parts), &mut sink).unwrap(),
        Response::Integer(2_000)
    );

    // A fresh arena on this shard now holds far fewer members than the source has.
    let small = crate::arena::limit_chunks_for_test(2);
    let reply = execute_with_sink(
        &mut shard,
        command_of(&[b"COPY", b"src", b"dst"]),
        &mut sink,
    )
    .unwrap()
    .into_owned();
    // Replay builds the source again, so it runs without the limit.
    drop(small);

    assert_eq!(
        reply,
        Response::Error(b"ERR sorted set cannot hold more members")
    );
    assert_eq!(shard.value_kind(b"dst"), None);
    assert_eq!(replayed(&sink).value_kind(b"dst"), None, "the log agrees");
}

/// A deterministic stream of pseudo-random numbers, so a failure replays the same way.
fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state >> 33
}

fn zadd_pairs(shard: &mut KvShard, sink: &mut DatabaseLog, key: &[u8], pairs: &[(f64, Vec<u8>)]) {
    execute_with_sink(
        shard,
        Command::SortedSetAdd {
            key: owned(key),
            pairs: pairs
                .iter()
                .map(|(score, member)| (*score, owned(member)))
                .collect(),
            options: crate::protocol::SortedSetAddOptions::default(),
        },
        sink,
    )
    .unwrap();
}

fn pop_logged(
    shard: &mut KvShard,
    sink: &mut DatabaseLog,
    key: &[u8],
    count: usize,
    max: bool,
) -> Vec<(f64, Vec<u8>)> {
    match execute_with_sink(shard, pop_command(key, Some(count), max), sink)
        .unwrap()
        .into_owned()
    {
        Response::Popped(None, entries) => entries
            .iter()
            .map(|(member, score)| (score, member.to_vec()))
            .collect(),
        other => panic!("pop reply: {other:?}"),
    }
}

/// `model` sorted as the set orders it.
fn sort_model(model: &mut [(f64, Vec<u8>)]) {
    model.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
}

/// ZRANGE, ZSCORE and ZRANK of `key` answer as `model` (kept sorted) says, and the
/// running memory charge matches a fresh count of the entry.
fn assert_sorted_set_matches(shard: &mut KvShard, key: &[u8], model: &[(f64, Vec<u8>)]) {
    let frame = frame_of(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"]);
    let (range, _) = parse_command(&frame).unwrap().expect("complete frame");

    match execute(shard, range.into_owned()) {
        Response::Scored(entries, true) => assert!(
            entries
                .iter()
                .map(|(member, score)| (*score, *member))
                .eq(model
                    .iter()
                    .map(|(score, member)| (*score, member.as_slice()))),
            "ZRANGE differs from the model"
        ),
        Response::Array(entries) => assert!(entries.is_empty() && model.is_empty()),
        other => panic!("ZRANGE reply: {other:?}"),
    }

    let step = (model.len() / 40).max(1);

    for (rank, (score, member)) in model.iter().enumerate().step_by(step) {
        assert_eq!(
            reply(shard, &frame_of(&[b"ZSCORE", key, member])),
            Reply::Bulk(format_score(*score)),
            "score of {:?}",
            String::from_utf8_lossy(member)
        );
        assert_eq!(
            reply(shard, &frame_of(&[b"ZRANK", key, member])),
            Reply::Integer(rank as i64)
        );
    }

    assert_eq!(charged(shard), shard.key_memory_usage(key).unwrap_or(0));
}

/// A set spanning many arena chunks popped from the low end down to nothing: every pop
/// returns the right entries, what is left reads back right, the memory goes back as
/// the set shrinks, and the replayed log ends empty too.
#[test]
fn popping_a_multi_chunk_sorted_set_to_empty_returns_its_memory_and_replays() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut state = 7u64;
    let mut model: Vec<(f64, Vec<u8>)> = (0..24_000u32)
        .map(|index| {
            (
                (lcg(&mut state) % 5_000) as f64,
                format!("member-{index:06}-pad").into_bytes(),
            )
        })
        .collect();

    for batch in model.chunks(1_000) {
        zadd_pairs(&mut shard, &mut sink, b"z", batch);
    }

    sort_model(&mut model);
    assert_sorted_set_matches(&mut shard, b"z", &model);

    let full = charged(&shard);
    let mut round = 0usize;

    while !model.is_empty() {
        let count = 1 + round % 53;
        let take = count.min(model.len());
        let expected: Vec<_> = model.drain(..take).collect();

        assert_eq!(
            pop_logged(&mut shard, &mut sink, b"z", count, false),
            expected
        );

        if round.is_multiple_of(150) {
            assert_sorted_set_matches(&mut shard, b"z", &model);
        }

        if model.len() == 6_000 {
            // A quarter of the members left: far less than the full set's charge.
            assert!(charged(&shard) * 2 < full, "{} of {full}", charged(&shard));
        }

        round += 1;
    }

    assert_eq!(shard.value_kind(b"z"), None);
    assert_eq!(charged(&shard), 0);
    assert_eq!(replayed(&sink).value_kind(b"z"), None);
}

/// ZADD, ZPOPMIN and ZPOPMAX interleaved on a large set against a sorted model; reads,
/// the memory charge and the replayed log agree with it throughout.
#[test]
fn interleaved_adds_and_pops_on_a_large_sorted_set_follow_a_model_and_replay() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let mut state = 11u64;
    let mut next = 0u32;
    let fresh = |state: &mut u64, next: &mut u32| {
        *next += 1;
        (
            (lcg(state) % 100_000) as f64 / 8.0,
            format!("m{:06}-{}", *next, lcg(state) % 1_000).into_bytes(),
        )
    };
    let mut model: Vec<(f64, Vec<u8>)> =
        (0..15_000).map(|_| fresh(&mut state, &mut next)).collect();

    zadd_pairs(&mut shard, &mut sink, b"z", &model);
    sort_model(&mut model);

    for round in 0..3_000usize {
        let added: Vec<_> = (0..1 + round % 6)
            .map(|_| fresh(&mut state, &mut next))
            .collect();

        zadd_pairs(&mut shard, &mut sink, b"z", &added);
        model.extend(added);

        // An update of a member already there moves it.
        if round.is_multiple_of(10) {
            let at = lcg(&mut state) as usize % model.len();
            let member = model[at].1.clone();
            let score = (lcg(&mut state) % 100_000) as f64 / 8.0;

            zadd_pairs(&mut shard, &mut sink, b"z", &[(score, member)]);
            model[at].0 = score;
        }

        sort_model(&mut model);

        let max = round % 3 == 2;
        let count = 1 + round % 7;
        let take = count.min(model.len());
        let expected: Vec<_> = if max {
            model.drain(model.len() - take..).rev().collect()
        } else {
            model.drain(..take).collect()
        };

        assert_eq!(
            pop_logged(&mut shard, &mut sink, b"z", count, max),
            expected,
            "round {round}"
        );

        if round.is_multiple_of(300) {
            assert_sorted_set_matches(&mut shard, b"z", &model);
        }
    }

    assert_sorted_set_matches(&mut shard, b"z", &model);
    assert_sorted_set_matches(&mut replayed(&sink), b"z", &model);
}

/// A long run of adds at the high end and pops at the low end, the set's size held
/// steady, keeps its memory charge flat: emptied chunks go back as the pops pass them.
/// Random scores, which leave no chunk in pop order, stay flat as well.
#[test]
fn a_steady_add_and_pop_loop_keeps_the_sorted_set_charge_flat() {
    for random_scores in [false, true] {
        let mut shard = KvShard::new();
        let mut sink = DatabaseLog::default();
        let mut state = 3u64;
        let mut next = 0u32;
        let entry = |state: &mut u64, next: &mut u32| {
            *next += 1;

            let score = if random_scores {
                (lcg(state) % 1_000_000) as f64
            } else {
                f64::from(*next)
            };

            (score, format!("steady-member-{:07}", *next).into_bytes())
        };
        let start: Vec<_> = (0..20_000).map(|_| entry(&mut state, &mut next)).collect();

        zadd_pairs(&mut shard, &mut sink, b"z", &start);

        let mut settled = 0;

        for round in 0..4_000usize {
            let added: Vec<_> = (0..16).map(|_| entry(&mut state, &mut next)).collect();

            zadd_pairs(&mut shard, &mut sink, b"z", &added);
            assert_eq!(pop_logged(&mut shard, &mut sink, b"z", 16, false).len(), 16);

            if round == 400 {
                settled = charged(&shard);
            } else if round > 400 {
                // In pop order, room for a chunk and a lookup/tree step; in random order,
                // for the dead half a compaction waits for. Never for growth.
                let room = if random_scores { settled } else { settled / 8 };

                assert!(
                    charged(&shard) <= settled + room,
                    "random {random_scores}, round {round}: {} > {settled}",
                    charged(&shard)
                );
            }
        }

        assert_eq!(shard.sorted_set_card(b"z").unwrap(), 20_000);
        assert_eq!(charged(&shard), shard.key_memory_usage(b"z").unwrap());
    }
}
