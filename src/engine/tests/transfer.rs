use super::*;

fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&[u8]]) -> Response<'static> {
    execute_with_sink(shard, command_of(parts), sink)
        .unwrap()
        .into_owned()
}

fn get(shard: &mut KvShard, sink: &mut DatabaseLog, key: &[u8]) -> Option<Vec<u8>> {
    match run(shard, sink, &[b"GET", key]) {
        Response::Bulk(value) => value.map(Cow::into_owned),
        other => panic!("unexpected GET reply {other:?}"),
    }
}

#[test]
fn transfer_moves_an_amount_and_writes_one_record() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    run(&mut shard, &mut sink, &[b"SET", b"a", b"10"]);

    let records = sink.records.len();

    assert_eq!(
        run(&mut shard, &mut sink, &[b"TRANSFER", b"a", b"b", b"4"]),
        integers(&[6, 4])
    );
    assert_eq!(sink.records.len(), records + 1);
    assert!(matches!(
        sink.records.last(),
        Some((_, OwnedMutation::Transaction { mutations })) if mutations.len() == 2
    ));
    assert_eq!(
        run(&mut shard, &mut sink, &[b"TRANSFER", b"a", b"b", b"6"]),
        integers(&[0, 10])
    );
    assert_eq!(get(&mut shard, &mut sink, b"a"), Some(b"0".to_vec()));
    assert_eq!(get(&mut shard, &mut sink, b"b"), Some(b"10".to_vec()));

    let replay = replayed(&sink);

    assert_eq!(replay.get(b"a"), shard.get(b"a"));
    assert_eq!(replay.get(b"b"), shard.get(b"b"));
}

#[test]
fn transfer_refusals_write_nothing() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    // Missing source.
    assert_eq!(
        run(&mut shard, &mut sink, &[b"TRANSFER", b"a", b"b", b"1"]),
        Response::Bulk(None)
    );
    assert_eq!(shard.value_kind(b"b"), None);

    run(&mut shard, &mut sink, &[b"SET", b"a", b"5"]);
    run(&mut shard, &mut sink, &[b"SET", b"b", b"7"]);

    let records = sink.records.len();

    // Short source.
    assert_eq!(
        run(&mut shard, &mut sink, &[b"TRANSFER", b"a", b"b", b"6"]),
        Response::Bulk(None)
    );
    // Destination past MAX.
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"TRANSFER", b"a", b"b", b"3", b"MAX", b"9"]
        ),
        Response::Bulk(None)
    );
    // Destination overflow.
    run(
        &mut shard,
        &mut sink,
        &[b"SET", b"b", b"9223372036854775807"],
    );

    let records_after_set = sink.records.len();

    assert_eq!(
        run(&mut shard, &mut sink, &[b"TRANSFER", b"a", b"b", b"1"]),
        Response::Bulk(None)
    );
    assert_eq!(sink.records.len(), records_after_set);
    assert!(records_after_set > records);
    assert_eq!(get(&mut shard, &mut sink, b"a"), Some(b"5".to_vec()));

    // Exactly at MAX is allowed.
    run(&mut shard, &mut sink, &[b"SET", b"b", b"7"]);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"TRANSFER", b"a", b"b", b"2", b"MAX", b"9"]
        ),
        integers(&[3, 9])
    );
}

#[test]
fn transfer_checks_types_and_arguments() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    run(&mut shard, &mut sink, &[b"SET", b"a", b"5"]);
    run(&mut shard, &mut sink, &[b"SET", b"text", b"abc"]);
    run(&mut shard, &mut sink, &[b"RPUSH", b"list", b"x"]);

    for (parts, expected) in [
        (
            &[b"TRANSFER".as_slice(), b"text", b"b", b"1"][..],
            &b"ERR value is not an integer or out of range"[..],
        ),
        (
            &[b"TRANSFER", b"a", b"text", b"1"],
            b"ERR value is not an integer or out of range",
        ),
        (
            &[b"TRANSFER", b"list", b"b", b"1"],
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            &[b"TRANSFER", b"a", b"list", b"1"],
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    ] {
        match run(&mut shard, &mut sink, parts) {
            Response::Error(message) => assert_eq!(message, expected),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    for parts in [
        &[b"TRANSFER".as_slice(), b"a", b"a", b"1"][..],
        &[b"TRANSFER", b"a", b"b", b"0"],
        &[b"TRANSFER", b"a", b"b", b"-1"],
        &[b"TRANSFER", b"a", b"b", b"x"],
        &[b"TRANSFER", b"a", b"b", b"1", b"MAX"],
        &[b"TRANSFER", b"a", b"b", b"1", b"CAP", b"5"],
        &[b"TRANSFER", b"a", b"b"],
        &[b"HTRANSFER", b"h", b"f", b"f", b"1"],
        &[b"HTRANSFER", b"h", b"f", b"g", b"0"],
        &[b"HTRANSFER", b"h", b"f", b"g"],
    ] {
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n", part.len()).bytes());
            frame.extend_from_slice(part);
            frame.extend_from_slice(b"\r\n");
        }

        assert!(
            parse_command(&frame).is_err(),
            "{} must be a syntax error",
            String::from_utf8_lossy(&parts.join(&b' '))
        );
    }
}

#[test]
fn htransfer_moves_between_fields_and_keeps_field_ttls() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    run(&mut shard, &mut sink, &[b"HSET", b"h", b"f", b"10"]);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"f", b"g", b"4"]
        ),
        integers(&[6, 4])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"f", b"g", b"7"]
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"f", b"g", b"6", b"MAX", b"9"]
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"nokey", b"f", b"g", b"1"]
        ),
        Response::Bulk(None)
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"missing", b"g", b"1"]
        ),
        Response::Bulk(None)
    );

    run(&mut shard, &mut sink, &[b"HSET", b"h", b"word", b"abc"]);
    assert!(matches!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"word", b"g", b"1"]
        ),
        Response::Error(_)
    ));
    assert!(matches!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"f", b"word", b"1"]
        ),
        Response::Error(_)
    ));

    run(
        &mut shard,
        &mut sink,
        &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"2", b"f", b"g"],
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"h", b"f", b"g", b"1"]
        ),
        integers(&[5, 5])
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTTL", b"h", b"FIELDS", b"2", b"f", b"g"]
        ),
        integers(&[100, 100])
    );

    run(&mut shard, &mut sink, &[b"SET", b"plain", b"1"]);
    assert!(matches!(
        run(
            &mut shard,
            &mut sink,
            &[b"HTRANSFER", b"plain", b"f", b"g", b"1"]
        ),
        Response::Error(_)
    ));

    let replay = replayed(&sink);

    assert_eq!(replay.hash_get(b"h", b"f"), shard.hash_get(b"h", b"f"));
    assert_eq!(replay.hash_get(b"h", b"g"), shard.hash_get(b"h", b"g"));
}
