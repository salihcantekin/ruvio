use super::*;

#[test]
fn read_responses_borrow_bytes_from_shard_storage() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"key", b"value");
    let stored_pointer = shard.get(b"key").unwrap().as_ptr();

    let response = execute(&mut shard, Command::Get(Cow::Borrowed(b"key")));
    let Response::Bulk(Some(Cow::Borrowed(value))) = response else {
        panic!("expected a borrowed bulk response");
    };

    assert_eq!(value, b"value");
    assert_eq!(value.as_ptr(), stored_pointer);
}

#[test]
fn command_engine_uses_static_dispatch_with_an_alternate_index() {
    let index = crate::BTreeIndex::with_capacity(4);
    let mut shard = KvShard::with_index_and_clock(index, crate::SystemClock::default());

    assert_eq!(
        execute(
            &mut shard,
            Command::set(Cow::Borrowed(b"key"), Cow::Borrowed(b"value"))
        ),
        Response::Simple(b"OK")
    );
    assert_eq!(
        execute(&mut shard, Command::Get(Cow::Borrowed(b"key"))),
        Response::Bulk(Some(Cow::Borrowed(b"value")))
    );
}

#[test]
fn sink_failure_prevents_standard_and_hot_mutations() {
    let mut shard = KvShard::new();
    let mut sink = FailingSink;

    let error = execute_with_sink(
        &mut shard,
        Command::set(owned(b"key"), owned(b"value")),
        &mut sink,
    )
    .unwrap_err();

    assert_eq!(error, "persistence unavailable");
    assert_eq!(shard.get(b"key"), None);

    let mut output = Vec::new();
    let error = execute_hot_into_with_sink(
        &mut shard,
        HotCommand::Set(b"hot", b"value", None),
        &mut output,
        &mut sink,
        b"$-1\r\n",
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ExecuteError::Mutation("persistence unavailable")
    ));
    assert_eq!(shard.get(b"hot"), None);
    assert!(output.is_empty());
}

#[test]
fn hot_delete_and_set_px_match_store_replies() {
    let mut shard = KvShard::new();
    let mut output = Vec::new();

    execute_hot_into(
        &mut shard,
        HotCommand::Set(b"session", b"value", Some(30_000)),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, b"+OK\r\n");
    assert_eq!(shard.get(b"session"), Some(b"value".as_slice()));

    let ttl = shard.pttl(b"session");

    assert!(ttl > 0 && ttl <= 30_000, "{ttl}");

    output.clear();
    execute_hot_into(&mut shard, HotCommand::Delete(b"session"), &mut output).unwrap();
    assert_eq!(output, b":1\r\n");
    assert_eq!(shard.get(b"session"), None);
    assert_eq!(shard.expires_at(b"session"), None);

    output.clear();
    execute_hot_into(&mut shard, HotCommand::Delete(b"missing"), &mut output).unwrap();
    assert_eq!(output, b":0\r\n");

    output.clear();
    execute_hot_into(
        &mut shard,
        HotCommand::Set(b"session", b"value", Some(0)),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, b"-ERR invalid expire time in 'set' command\r\n");
    assert_eq!(shard.get(b"session"), None);
}

#[test]
fn validation_errors_do_not_cross_the_mutation_boundary() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"not-a-list", b"value");
    let mut sink = RecordingSink::default();

    let response = execute_with_sink(
        &mut shard,
        Command::ListPush(owned(b"not-a-list"), vec![owned(b"item")].into()),
        &mut sink,
    )
    .unwrap();

    assert_eq!(response, wrong_type());
    assert!(sink.mutations.is_empty());
    assert_eq!(shard.get(b"not-a-list"), Some(&b"value"[..]));
}
