use super::*;

#[test]
fn flushdb_clears_only_the_selected_database_and_replays() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"zero")),
    );
    in_db(
        &mut shard,
        &mut sink,
        1,
        Command::set(owned(b"k"), owned(b"one")),
    );
    in_db(
        &mut shard,
        &mut sink,
        1,
        Command::set(owned(b"other"), owned(b"x")),
    );

    assert_eq!(
        in_db(&mut shard, &mut sink, 1, Command::FlushDb),
        Response::Simple(b"OK")
    );
    assert_eq!(
        in_db(&mut shard, &mut sink, 1, Command::DbSize),
        Response::Integer(0)
    );
    assert_eq!(db_get(&mut shard, 0, b"k"), Some(b"zero".to_vec()));
    let mut recovered = replayed(&sink);

    assert_eq!(db_get(&mut recovered, 0, b"k"), Some(b"zero".to_vec()));
    assert_eq!(db_get(&mut recovered, 1, b"k"), None);

    assert_eq!(
        in_db(&mut shard, &mut sink, 0, Command::FlushAll),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.memory_usage().entries, 0);
    assert_eq!(replayed(&sink).memory_usage().entries, 0);
    assert_eq!(
        sink.records.last(),
        Some(&(0, OwnedMutation::FlushAll)),
        "FLUSHALL is one record"
    );
}

#[test]
fn swapdb_exchanges_every_key_and_replays() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"zero")),
    );
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"only0"), owned(b"a")),
    );
    in_db(
        &mut shard,
        &mut sink,
        2,
        Command::set(owned(b"k"), owned(b"two")),
    );
    in_db(
        &mut shard,
        &mut sink,
        2,
        Command::Expire {
            condition: crate::protocol::ExpireCondition::Always,
            key: owned(b"k"),
            milliseconds: 60_000,
        },
    );

    assert_eq!(
        in_db(&mut shard, &mut sink, 5, Command::SwapDb(0, 2)),
        Response::Simple(b"OK")
    );

    for target in [&mut shard, &mut replayed(&sink)] {
        assert_eq!(db_get(target, 0, b"k"), Some(b"two".to_vec()));
        assert_eq!(db_get(target, 2, b"k"), Some(b"zero".to_vec()));
        assert_eq!(db_get(target, 2, b"only0"), Some(b"a".to_vec()));
        assert_eq!(db_get(target, 0, b"only0"), None);
        assert!(matches!(
            in_db(target, &mut DatabaseLog::default(), 0, Command::Ttl(owned(b"k"))),
            Response::Integer(ttl) if ttl > 0
        ));
    }
}

#[test]
fn move_transfers_a_key_only_when_the_target_is_free() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();
    let move_to = |db| Command::Move {
        key: owned(b"k"),
        db,
    };

    assert_eq!(
        in_db(&mut shard, &mut sink, 0, move_to(1)),
        Response::Integer(0)
    );
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"v")),
    );
    assert_eq!(
        in_db(&mut shard, &mut sink, 0, move_to(1)),
        Response::Integer(1)
    );
    assert_eq!(db_get(&mut shard, 0, b"k"), None);
    assert_eq!(db_get(&mut shard, 1, b"k"), Some(b"v".to_vec()));
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"new")),
    );
    assert_eq!(
        in_db(&mut shard, &mut sink, 0, move_to(1)),
        Response::Integer(0)
    );
    assert_eq!(
        in_db(&mut shard, &mut sink, 1, move_to(1)),
        Response::Error(b"ERR source and destination objects are the same")
    );
    let mut recovered = replayed(&sink);

    assert_eq!(db_get(&mut recovered, 1, b"k"), Some(b"v".to_vec()));
    assert_eq!(db_get(&mut recovered, 0, b"k"), Some(b"new".to_vec()));
}

#[test]
fn transactions_run_in_the_selected_database() {
    let mut shard = KvShard::new();
    let Ok(response) = execute_transaction(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        1,
        vec![
            Command::set(owned(b"a"), owned(b"1")),
            Command::Get(owned(b"a")),
            Command::DbSize,
        ],
        3,
    );

    assert_eq!(
        response,
        Response::Exec(vec![
            Response::Simple(b"OK"),
            Response::Bulk(Some(Cow::Owned(b"1".to_vec()))),
            Response::Integer(1),
        ])
    );
    assert_eq!(db_get(&mut shard, 0, b"a"), None);
    assert_eq!(db_get(&mut shard, 3, b"a"), Some(b"1".to_vec()));
}

#[test]
fn blocking_and_stream_replies_name_the_user_key() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        4,
        Command::ListPush(owned(b"queue"), vec![owned(b"job")].into()),
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            4,
            Command::ListBlock {
                keys: vec![owned(b"queue")],
                timeout_ms: Some(0),
                back: false,
            }
        ),
        Response::Array(vec![
            Some(Cow::Owned(b"queue".to_vec())),
            Some(Cow::Owned(b"job".to_vec())),
        ])
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut sink,
            4,
            Command::ListBlock {
                keys: vec![owned(b"queue")],
                timeout_ms: Some(0),
                back: false,
            }
        ),
        Response::BlockList {
            keys: vec![Cow::Owned(tagged(4, b"queue"))],
            timeout_ms: Some(0),
            back: false,
            sorted: None,
        }
    );
}

#[test]
fn keyspace_info_lists_each_non_empty_database() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"a"), owned(b"1")),
    );
    in_db(
        &mut shard,
        &mut sink,
        3,
        Command::set(owned(b"a"), owned(b"1")),
    );
    in_db(
        &mut shard,
        &mut sink,
        3,
        Command::set(owned(b"b"), owned(b"1")),
    );

    assert_eq!(
        keyspace_info(&shard),
        "db0:keys=1,expires=0,avg_ttl=0\r\ndb3:keys=2,expires=0,avg_ttl=0\r\n"
    );
}

#[test]
fn database_commands_inside_multi_are_one_record_and_replay() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"zero")),
    );
    in_db(
        &mut shard,
        &mut sink,
        1,
        Command::set(owned(b"k"), owned(b"one")),
    );
    in_db(
        &mut shard,
        &mut sink,
        2,
        Command::set(owned(b"gone"), owned(b"x")),
    );
    let Ok(response) = execute_transaction(
        &mut shard,
        &mut sink,
        &mut (),
        9,
        vec![
            Command::SwapDb(0, 1),
            Command::FlushDb,
            Command::set(owned(b"after"), owned(b"v")),
        ],
        2,
    );

    assert_eq!(
        response,
        Response::Exec(vec![
            Response::Simple(b"OK"),
            Response::Simple(b"OK"),
            Response::Simple(b"OK"),
        ])
    );
    assert!(matches!(
        sink.records.last(),
        Some((2, OwnedMutation::Transaction { mutations })) if mutations.len() == 3
    ));

    for target in [&mut shard, &mut replayed(&sink)] {
        assert_eq!(db_get(target, 0, b"k"), Some(b"one".to_vec()));
        assert_eq!(db_get(target, 1, b"k"), Some(b"zero".to_vec()));
        assert_eq!(db_get(target, 2, b"gone"), None);
        assert_eq!(db_get(target, 2, b"after"), Some(b"v".to_vec()));
    }
}

#[test]
fn refused_transaction_puts_flushed_and_swapped_databases_back() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"zero")),
    );
    in_db(
        &mut shard,
        &mut sink,
        3,
        Command::set(owned(b"k"), owned(b"three")),
    );
    let before = shard.memory_usage();
    let refused = execute_transaction(
        &mut shard,
        &mut FailingSink,
        &mut (),
        9,
        vec![
            Command::set(owned(b"k"), owned(b"changed")),
            Command::SwapDb(0, 3),
            Command::FlushAll,
            Command::set(owned(b"k"), owned(b"late")),
        ],
        0,
    );

    assert_eq!(refused, Err("persistence unavailable"));
    assert_eq!(db_get(&mut shard, 0, b"k"), Some(b"zero".to_vec()));
    assert_eq!(db_get(&mut shard, 3, b"k"), Some(b"three".to_vec()));
    assert_eq!(shard.memory_usage().entries, before.entries);
    assert_eq!(shard.memory_usage().key_bytes, before.key_bytes);
    assert_eq!(shard.memory_usage().value_bytes, before.value_bytes);
}

#[test]
fn watches_belong_to_one_database() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    shard.watch(7, 0, &[b"k"]);
    in_db(
        &mut shard,
        &mut sink,
        1,
        Command::set(owned(b"k"), owned(b"v")),
    );
    assert!(!shard.watches_dirty(7));
    in_db(
        &mut shard,
        &mut sink,
        0,
        Command::set(owned(b"k"), owned(b"v")),
    );
    assert!(shard.watches_dirty(7));

    shard.watch(8, 4, &[b"other"]);
    in_db(&mut shard, &mut sink, 0, Command::SwapDb(4, 5));
    assert!(shard.watches_dirty(8));
}

#[test]
fn blocked_waiter_wakes_only_for_pushes_in_its_database() {
    let mut shard = KvShard::new();
    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let (reply, receiver) = std::sync::mpsc::sync_channel(1);

    waiters
        .register(vec![tagged(3, b"queue")], None, reply, b"*-1\r\n".to_vec())
        .unwrap();
    let Ok(_) = execute_in_database(
        &mut shard,
        Command::ListPush(owned(b"queue"), vec![owned(b"zero")].into()),
        &mut NoopMutationSink,
        &mut waiters,
        0,
    );

    assert!(receiver.try_recv().is_err());
    let Ok(_) = execute_in_database(
        &mut shard,
        Command::ListPush(owned(b"queue"), vec![owned(b"three")].into()),
        &mut NoopMutationSink,
        &mut waiters,
        3,
    );

    assert_eq!(
        receiver.try_recv().unwrap(),
        block_hit_response(b"queue", b"three")
    );
    assert_eq!(
        in_db(
            &mut shard,
            &mut DatabaseLog::default(),
            0,
            Command::ListLen(owned(b"queue"))
        ),
        Response::Integer(1)
    );
}

#[test]
fn records_canonical_increment_result_before_applying_it() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"counter", b"41");
    let mut sink = RecordingSink::default();

    let response = execute_with_sink(
        &mut shard,
        Command::Increment(owned(b"counter"), 1),
        &mut sink,
    )
    .unwrap();

    assert_eq!(response, Response::Integer(42));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"42"[..]));
    assert_eq!(
        sink.mutations,
        vec![RecordedMutation::SetInteger(b"counter".to_vec(), 42)]
    );
}
