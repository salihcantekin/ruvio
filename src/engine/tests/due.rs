use super::strings::words;
use super::*;

fn render(response: Response<'_>) -> String {
    match response {
        Response::Integer(value) => value.to_string(),
        Response::Error(message) => String::from_utf8_lossy(message).into_owned(),
        Response::PoppedByScore(entries, _) | Response::Popped(None, entries) => entries
            .iter()
            .map(|(member, score)| format!("{}@{score}", String::from_utf8_lossy(member)))
            .collect::<Vec<_>>()
            .join(","),
        other => format!("{other:?}"),
    }
}

fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&str]) -> String {
    let frame = words(parts);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    render(in_db(shard, sink, 0, command.into_owned()))
}

fn schedule(shard: &mut KvShard, sink: &mut DatabaseLog) {
    for (score, member) in [("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")] {
        run(shard, sink, &["ZADD", "z", score, member]);
    }
}

fn list_items(shard: &KvShard, key: &[u8]) -> Vec<String> {
    (0..shard.list_len(key).unwrap())
        .map(|index| {
            let item = shard.list_index(key, index as isize).unwrap().unwrap();

            String::from_utf8_lossy(item).into_owned()
        })
        .collect()
}

#[test]
fn zpopbyscore_takes_the_lowest_due_members() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);

    assert_eq!(
        run(&mut shard, &mut sink, &["ZPOPBYSCORE", "z", "3"]),
        "a@1"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZPOPBYSCORE", "z", "3", "COUNT", "10"]
        ),
        "b@2,c@3"
    );
    assert_eq!(run(&mut shard, &mut sink, &["ZPOPBYSCORE", "z", "3"]), "");
    assert_eq!(shard.sorted_set_card(b"z").unwrap(), 2);
}

#[test]
fn zpopbyscore_honours_min_and_exclusive_bounds() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZPOPBYSCORE", "z", "(4", "MIN", "(1", "COUNT", "9"]
        ),
        "b@2,c@3"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZPOPBYSCORE", "z", "+inf", "COUNT", "9"]
        ),
        "a@1,d@4,e@5"
    );
    // The set emptied, so the key is gone.
    assert!(!shard.contains_key(b"z"));
}

#[test]
fn zpopbyscore_stops_at_count_and_skips_missing_keys() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["ZPOPBYSCORE", "none", "+inf"]),
        ""
    );
    assert!(sink.records.is_empty());

    schedule(&mut shard, &mut sink);

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZPOPBYSCORE", "z", "+inf", "COUNT", "2"]
        ),
        "a@1,b@2"
    );
    assert_eq!(shard.sorted_set_card(b"z").unwrap(), 3);
}

#[test]
fn zpopbyscore_replays_as_the_same_removal() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);
    run(
        &mut shard,
        &mut sink,
        &["ZPOPBYSCORE", "z", "3", "COUNT", "2"],
    );

    let replica = replayed(&sink);

    assert_eq!(replica.sorted_set_card(b"z").unwrap(), 3);
    assert_eq!(
        sink.records.last().map(|(_, mutation)| mutation),
        Some(&OwnedMutation::SortedSetRemove {
            key: b"z".to_vec(),
            members: vec![b"a".to_vec(), b"b".to_vec()],
        })
    );
}

#[test]
fn zpopbyscore_rejects_bad_arguments() {
    let mut shard = KvShard::new();
    let frames: &[&[&str]] = &[
        &["ZPOPBYSCORE", "z"],
        &["ZPOPBYSCORE", "z", "abc"],
        &["ZPOPBYSCORE", "z", "1", "COUNT", "0"],
        &["ZPOPBYSCORE", "z", "1", "COUNT", "10001"],
        &["ZPOPBYSCORE", "z", "1", "COUNT"],
        &["ZPOPBYSCORE", "z", "1", "LIMIT", "1"],
        &["ZMOVEDUE", "z", "l"],
        &["ZMOVEDUE", "z", "l", "1", "COUNT"],
        &["ZMOVEDUE", "z", "l", "1", "LIMIT", "2"],
    ];

    for parts in frames {
        let frame = words(parts);

        assert!(
            !matches!(parse_command(&frame), Ok(Some(_))),
            "{parts:?} should be refused"
        );
    }

    let _ = &mut shard;
}

#[test]
fn zpopbyscore_and_zmovedue_refuse_the_wrong_type() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    run(&mut shard, &mut sink, &["SET", "s", "1"]);
    run(&mut shard, &mut sink, &["RPUSH", "l", "x"]);
    schedule(&mut shard, &mut sink);

    let frames: &[&[&str]] = &[
        &["ZPOPBYSCORE", "s", "1"],
        &["ZPOPBYSCORE", "l", "1"],
        &["ZMOVEDUE", "s", "l", "1"],
        &["ZMOVEDUE", "l", "l", "1"],
        &["ZMOVEDUE", "z", "s", "9"],
        &["ZMOVEDUE", "z", "z", "9"],
    ];

    for parts in frames {
        assert!(
            run(&mut shard, &mut sink, parts).starts_with("WRONGTYPE"),
            "{parts:?}"
        );
    }

    assert_eq!(shard.sorted_set_card(b"z").unwrap(), 5);
}

#[test]
fn zmovedue_moves_due_members_to_the_list_tail() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);
    run(&mut shard, &mut sink, &["RPUSH", "ready", "first"]);

    assert_eq!(
        run(&mut shard, &mut sink, &["ZMOVEDUE", "z", "ready", "3"]),
        "1"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZMOVEDUE", "z", "ready", "3", "COUNT", "5"]
        ),
        "2"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["ZMOVEDUE", "z", "ready", "3"]),
        "0"
    );
    assert_eq!(list_items(&shard, b"ready"), ["first", "a", "b", "c"]);
    assert_eq!(shard.sorted_set_card(b"z").unwrap(), 2);
}

#[test]
fn zmovedue_creates_the_list_and_drops_an_emptied_set() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);

    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["ZMOVEDUE", "z", "ready", "+inf", "COUNT", "100"]
        ),
        "5"
    );
    assert!(!shard.contains_key(b"z"));
    assert_eq!(list_items(&shard, b"ready"), ["a", "b", "c", "d", "e"]);
}

#[test]
fn zmovedue_records_one_transaction_that_replays() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    schedule(&mut shard, &mut sink);

    let before = sink.records.len();

    run(
        &mut shard,
        &mut sink,
        &["ZMOVEDUE", "z", "ready", "2", "COUNT", "5"],
    );

    assert_eq!(sink.records.len(), before + 1);
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::Transaction { .. }))
    ));

    let replica = replayed(&sink);

    assert_eq!(list_items(&replica, b"ready"), ["a", "b"]);
    assert_eq!(replica.sorted_set_card(b"z").unwrap(), 3);

    // Nothing due records nothing.
    let records = sink.records.len();

    run(&mut shard, &mut sink, &["ZMOVEDUE", "z", "ready", "0"]);
    assert_eq!(sink.records.len(), records);
}
