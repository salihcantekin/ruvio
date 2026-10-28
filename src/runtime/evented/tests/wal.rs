use super::*;

#[test]
fn evented_wal_group_policy_checks_execution_path_and_session() {
    use crate::protocol::HotCommand;

    let mut session = TxSession::new(1);
    let hot = [
        HotCommand::Ping(None),
        HotCommand::Get(b"k"),
        HotCommand::Set(b"k", b"v", None),
        HotCommand::Set(b"k", b"v", Some(1000)),
        HotCommand::Delete(b"k"),
        HotCommand::Increment(b"k", 1),
    ];
    let native = Command::Limit {
        key: b"k".as_slice().into(),
        max: 10,
        window_ms: 1000,
        mode: crate::protocol::LimitMode::Fixed,
    };
    for command in &hot {
        assert!(wal_group_eligible(WalGroupCommand::Hot(command), &session));
    }
    assert!(wal_group_eligible(
        WalGroupCommand::Parsed(&native),
        &session
    ));
    for command in [
        Command::Get(b"k".as_slice().into()),
        Command::ListPush(b"k".as_slice().into(), vec![b"v".as_slice().into()].into()),
        Command::MSet(vec![(b"k".as_slice().into(), b"v".as_slice().into())]),
        Command::SetAdd(b"k".as_slice().into(), vec![b"v".as_slice().into()]),
    ] {
        assert!(wal_group_eligible(
            WalGroupCommand::Parsed(&command),
            &session
        ));
    }

    for command in [
        Command::ListBlock {
            keys: vec![b"k".as_slice().into()],
            timeout_ms: Some(1000),
            back: false,
        },
        Command::SortedSetBlockPop {
            keys: vec![b"k".as_slice().into()],
            timeout_ms: Some(1000),
            max: false,
        },
        Command::FlushAll,
        Command::Wait {
            replicas: 1,
            timeout_ms: 1000,
        },
        Command::Multi,
        Command::Exec,
        Command::Save,
        Command::ScriptFlush,
        Command::Subscribe(vec![b"k".as_slice().into()]),
    ] {
        assert!(!wal_group_eligible(
            WalGroupCommand::Parsed(&command),
            &session
        ));
    }

    session.in_pubsub = true;
    for command in &hot {
        assert!(!wal_group_eligible(WalGroupCommand::Hot(command), &session));
    }
    assert!(!wal_group_eligible(
        WalGroupCommand::Parsed(&native),
        &session
    ));
    session.in_pubsub = false;
    session.prepare(Command::Multi);
    for command in &hot {
        assert!(!wal_group_eligible(WalGroupCommand::Hot(command), &session));
    }
    assert!(!wal_group_eligible(
        WalGroupCommand::Parsed(&native),
        &session
    ));
}

#[test]
fn evented_scripts_and_transactions_join_the_wal_group() {
    let script = || Command::Eval {
        script: b"return 1".as_slice().into(),
        keys: Vec::new(),
        args: Vec::new(),
    };
    let mut session = TxSession::new(1);

    for command in [script(), Command::Multi] {
        assert!(transaction_joins_group(&command, &session));
    }
    for command in [
        Command::Exec,
        Command::FlushAll,
        Command::ScriptFlush,
        Command::Wait {
            replicas: 1,
            timeout_ms: 1000,
        },
    ] {
        assert!(!transaction_joins_group(&command, &session));
    }

    session.prepare(Command::Multi);
    // Queuing writes nothing.
    for command in [Command::Get(b"k".as_slice().into()), script()] {
        assert!(transaction_joins_group(&command, &session));
    }
    for command in [
        Command::Save,
        Command::Wait {
            replicas: 1,
            timeout_ms: 1000,
        },
    ] {
        assert!(!transaction_joins_group(&command, &session));
    }
    session.prepare(Command::Get(b"k".as_slice().into()));
    assert!(transaction_joins_group(&Command::Exec, &session));
    // A transaction that flushes or swaps databases stays a boundary.
    session.prepare(Command::FlushDb);
    assert!(!transaction_joins_group(&Command::Exec, &session));

    let mut subscribed = TxSession::new(1);

    subscribed.in_pubsub = true;
    assert!(!transaction_joins_group(&script(), &subscribed));
}

#[test]
fn evented_scripts_and_transactions_share_sync_before_reply() {
    scripts_and_transactions_share_sync_before_reply(false);
}

#[test]
fn evented_worker_scripts_and_transactions_share_sync_before_reply() {
    scripts_and_transactions_share_sync_before_reply(true);
}

/// A pipeline of writing scripts and transactions is answered after one shared sync, and
/// everything it was answered for survives a restart.
fn scripts_and_transactions_share_sync_before_reply(worker: bool) {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        sync_worker_active: worker,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    let set = b"return ruvio.call('SET', KEYS[1], ARGV[1])".as_slice();
    let payload = resp(&[
        &[b"EVAL", set, b"1", b"a", b"1"],
        &[b"SET", b"plain", b"p"],
        &[b"EVAL", set, b"1", b"b", b"2"],
        &[b"MULTI"],
        &[b"SET", b"c", b"3"],
        &[b"INCR", b"n"],
        &[b"EXEC"],
        &[
            b"EVAL",
            b"return ruvio.call('RPUSH', KEYS[1], 'x')",
            b"1",
            b"l",
        ],
    ]);
    assert_eq!(
        request(address, &payload),
        b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n:1\r\n:1\r\n"
    );
    let metrics = request(address, b"*2\r\n$4\r\nINFO\r\n$10\r\nprometheus\r\n");
    let metrics = String::from_utf8(metrics).unwrap();
    assert!(
        metrics
            .lines()
            .any(|line| line.starts_with("ruvio_wal_syncs_total{") && line.ends_with(" 1")),
        "{metrics}"
    );
    let info = String::from_utf8(request(address, b"*1\r\n$4\r\nINFO\r\n")).unwrap();
    let sequence = |name: &str| {
        info.lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(
        sequence("wal_last_sequence:"),
        sequence("wal_durable_sequence:")
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| config.persistence = persistence);
    let check = resp(&[
        &[b"MGET", b"a", b"plain", b"b", b"c", b"n"],
        &[b"LRANGE", b"l", b"0", b"-1"],
    ]);
    assert_eq!(
        request(address, &check),
        b"*5\r\n$1\r\n1\r\n$1\r\np\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n*1\r\n$1\r\nx\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Under `always` a script or transaction in the group whose sync fails is refused like a
/// plain write: its record is cut, and a restart does not revive it.
#[test]
fn a_grouped_script_or_transaction_whose_sync_fails_is_refused_and_cut() {
    grouped_script_or_transaction_whose_sync_fails_is_refused_and_cut(false);
}

#[test]
fn a_worker_grouped_script_or_transaction_whose_sync_fails_is_refused_and_cut() {
    grouped_script_or_transaction_whose_sync_fails_is_refused_and_cut(true);
}

fn grouped_script_or_transaction_whose_sync_fails_is_refused_and_cut(worker: bool) {
    let set = b"return ruvio.call('SET', KEYS[1], ARGV[1])".as_slice();
    let batches = [
        resp(&[
            &[b"SET", b"other", b"new"],
            &[b"EVAL", set, b"1", b"k", b"new"],
        ]),
        resp(&[
            &[b"SET", b"other", b"new"],
            &[b"MULTI"],
            &[b"SET", b"k", b"new"],
            &[b"EXEC"],
        ]),
    ];

    for batch in batches {
        let directory = tempfile::tempdir().unwrap();
        let persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            sync_worker_active: worker,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

        assert_eq!(
            request(
                address,
                &resp(&[&[b"MSET", b"k", b"old", b"other", b"old"]])
            ),
            b"+OK\r\n"
        );
        crate::persistence::FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        let refused = request(address, &batch);
        crate::persistence::FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());
        // The client whose sync failed loses its connection before any reply.
        assert!(
            refused.is_empty(),
            "{:?}",
            String::from_utf8_lossy(&refused)
        );
        // Every later write is refused until restart.
        assert_eq!(
            request(address, &resp(&[&[b"SET", b"later", b"v"]])),
            b"-MISCONF persistence error\r\n"
        );
        stop.request();
        assert!(
            join_within(server, PATIENCE).is_err(),
            "a shard whose WAL failed exited cleanly"
        );

        let (address, stop, server) = serve_with(|config| config.persistence = persistence);
        let restarted = request(address, &resp(&[&[b"MGET", b"k", b"other", b"later"]]));

        if worker {
            // A batch the sync worker failed is left in the log as it is (see
            // docs/RECOVERY.md): a restart replays what reached the disk, as for plain
            // writes, and never half of the script or transaction.
            assert!(
                restarted == b"*3\r\n$3\r\nold\r\n$3\r\nold\r\n$-1\r\n"
                    || restarted == b"*3\r\n$3\r\nnew\r\n$3\r\nnew\r\n$-1\r\n",
                "{:?}",
                String::from_utf8_lossy(&restarted)
            );
        } else {
            assert_eq!(restarted, b"*3\r\n$3\r\nold\r\n$3\r\nold\r\n$-1\r\n");
        }
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// A blocking pop served by a script whose group sync fails: the element came with the
/// script's write, which is refused, so the pop is refused too.
#[test]
fn a_pop_served_by_a_grouped_script_whose_sync_fails_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    let mut blocked = StdTcpStream::connect(address).unwrap();

    // Longer than the pop's own timeout: a pop never served answers nil, not a read error.
    blocked
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    blocked
        .write_all(&resp(&[&[b"BLPOP", b"served", b"5"]]))
        .unwrap();
    wait_for_parked(address, 1);
    super::super::connection::DELAY_SERVED_POPS
        .lock()
        .unwrap()
        .push(directory.path().to_path_buf());
    crate::persistence::FAILING_NEXT_SYNC_SHARDS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), 0));
    let pushed = request(
        address,
        &resp(&[
            &[b"SET", b"other", b"v"],
            &[
                b"EVAL",
                b"return ruvio.call('RPUSH', KEYS[1], 'a', 'b')",
                b"1",
                b"served",
            ],
        ]),
    );
    let mut popped = [0u8; 64];
    let popped = blocked
        .read(&mut popped)
        .map(|length| popped[..length].to_vec());

    super::super::connection::DELAY_SERVED_POPS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());
    crate::persistence::FAILING_NEXT_SYNC_SHARDS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    // The pusher is cut before any reply.
    assert!(pushed.is_empty(), "{:?}", String::from_utf8_lossy(&pushed));
    // The pop was answered before its read timeout (a pop never served would answer nil
    // after its own): refused by closing its connection, or with an error, never with the
    // element.
    let popped = popped.expect("the pop got no answer before its read timeout");
    assert!(
        popped.is_empty() || popped[0] == b'-',
        "pop got {:?}",
        String::from_utf8_lossy(&popped)
    );
    // The script ran and served the pop: its writes stay in memory until restart, the
    // element it handed over gone with them.
    assert_eq!(
        request(
            address,
            &resp(&[&[b"GET", b"other"], &[b"LRANGE", b"served", b"0", b"-1"]])
        ),
        b"$1\r\nv\r\n*1\r\n$1\r\nb\r\n"
    );
    stop.request();
    let _ = join_within(server, PATIENCE);
}

/// A WAL write (not sync) failure meets a script or `EXEC` inside a group: its record is
/// refused while it can still roll back, so it is undone in memory and the client gets the
/// error a refused plain write gets, on a connection that stays open. The log resumes
/// afterwards and a restart finds nothing of it.
#[test]
fn a_grouped_script_or_transaction_whose_write_fails_rolls_back() {
    let set = b"ruvio.call('SET', KEYS[1], ARGV[1]); return ruvio.call('INCR', KEYS[2])";
    // Every key in one slot, as a script needs.
    let batches = [
        (
            resp(&[
                &[b"GET", b"{t}k"],
                &[b"EVAL", set.as_slice(), b"2", b"{t}k", b"{t}n", b"new"],
                &[b"MGET", b"{t}k", b"{t}n"],
            ]),
            b"$3\r\nold\r\n-MISCONF persistence error\r\n*2\r\n$3\r\nold\r\n$-1\r\n".as_slice(),
        ),
        (
            resp(&[
                &[b"GET", b"{t}k"],
                &[b"MULTI"],
                &[b"SET", b"{t}k", b"new"],
                &[b"INCR", b"{t}n"],
                &[b"EXEC"],
                &[b"MGET", b"{t}k", b"{t}n"],
            ]),
            b"$3\r\nold\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n-MISCONF persistence error\r\n\
              *2\r\n$3\r\nold\r\n$-1\r\n"
                .as_slice(),
        ),
        // A script queued in MULTI is undone with the rest of the batch.
        (
            resp(&[
                &[b"GET", b"{t}k"],
                &[b"MULTI"],
                &[b"EVAL", set.as_slice(), b"2", b"{t}k", b"{t}n", b"new"],
                &[b"EXEC"],
                &[b"MGET", b"{t}k", b"{t}n"],
            ]),
            b"$3\r\nold\r\n+OK\r\n+QUEUED\r\n-MISCONF persistence error\r\n\
              *2\r\n$3\r\nold\r\n$-1\r\n"
                .as_slice(),
        ),
    ];

    for (batch, expected) in batches {
        let directory = tempfile::tempdir().unwrap();
        let persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

        assert_eq!(
            request(address, &resp(&[&[b"SET", b"{t}k", b"old"]])),
            b"+OK\r\n"
        );
        crate::persistence::FAILING_WRITE_SHARDS
            .lock()
            .unwrap()
            .push((directory.path().to_path_buf(), 0, 1));
        let refused = request(address, &batch);
        crate::persistence::FAILING_WRITE_SHARDS
            .lock()
            .unwrap()
            .retain(|(dir, _, _)| dir != directory.path());
        assert_eq!(
            String::from_utf8_lossy(&refused),
            String::from_utf8_lossy(expected)
        );
        // The log resumed: a later write is accepted and kept.
        assert_eq!(
            request(address, &resp(&[&[b"SET", b"{t}later", b"v"]])),
            b"+OK\r\n"
        );
        stop.request();
        join_within(server, PATIENCE).unwrap();

        let (address, stop, server) = serve_with(|config| config.persistence = persistence);
        assert_eq!(
            request(address, &resp(&[&[b"MGET", b"{t}k", b"{t}n", b"{t}later"]])),
            b"*3\r\n$3\r\nold\r\n$-1\r\n$1\r\nv\r\n"
        );
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

fn wal_syncs(address: SocketAddr) -> u64 {
    let metrics = request(address, b"*2\r\n$4\r\nINFO\r\n$10\r\nprometheus\r\n");
    let metrics = String::from_utf8(metrics).unwrap();

    metrics
        .lines()
        .find_map(|line| {
            line.starts_with("ruvio_wal_syncs_total{")
                .then(|| line.rsplit(' ').next().unwrap().parse().unwrap())
        })
        .unwrap_or_else(|| panic!("{metrics}"))
}

/// An `EXEC` that `WATCH` aborts inside a group answers nil and logs nothing.
#[test]
fn a_grouped_exec_aborted_by_watch_answers_nil_and_logs_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client.write_all(&resp(&[&[b"WATCH", b"k"]])).unwrap();
    let mut reply = [0u8; 5];
    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n");
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"k", b"theirs"]])),
        b"+OK\r\n"
    );
    let sequence = info_field(address, "wal_last_sequence").unwrap();

    client
        .write_all(&resp(&[
            &[b"GET", b"k"],
            &[b"MULTI"],
            &[b"SET", b"k", b"mine"],
            &[b"INCR", b"n"],
            &[b"EXEC"],
            &[b"MGET", b"k", b"n"],
        ]))
        .unwrap();
    let expected =
        b"$6\r\ntheirs\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*-1\r\n*2\r\n$6\r\ntheirs\r\n$-1\r\n";
    let mut replies = vec![0u8; expected.len()];

    client.read_exact(&mut replies).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&replies),
        String::from_utf8_lossy(expected)
    );
    assert_eq!(info_field(address, "wal_last_sequence"), Some(sequence));
    drop(client);
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| config.persistence = persistence);
    assert_eq!(
        request(address, &resp(&[&[b"MGET", b"k", b"n"]])),
        b"*2\r\n$6\r\ntheirs\r\n$-1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A transaction that flushes stays a group boundary: the writes before it are synced on
/// their own, the transaction syncs itself, and everything survives a restart. The same
/// pipeline without the flush shares one sync.
#[test]
fn a_transaction_that_flushes_stays_a_wal_group_boundary() {
    for (flush, syncs) in [(false, 1), (true, 3)] {
        let directory = tempfile::tempdir().unwrap();
        let persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
        let queued: &[&[u8]] = if flush {
            &[b"FLUSHDB"]
        } else {
            &[b"DEL", b"a"]
        };
        let before = wal_syncs(address);

        assert_eq!(
            request(
                address,
                &resp(&[
                    &[b"SET", b"a", b"1"],
                    &[b"MULTI"],
                    queued,
                    &[b"SET", b"b", b"2"],
                    &[b"EXEC"],
                    &[b"SET", b"c", b"3"],
                ])
            ),
            [
                b"+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n".as_slice(),
                if flush { b"+OK\r\n" } else { b":1\r\n" },
                b"+OK\r\n+OK\r\n",
            ]
            .concat(),
        );
        // A flushing transaction cannot share: the group before it commits, it syncs on
        // its own, and the write after it starts a new group.
        assert_eq!(wal_syncs(address) - before, syncs, "flush: {flush}");
        stop.request();
        join_within(server, PATIENCE).unwrap();

        let (address, stop, server) = serve_with(|config| config.persistence = persistence);
        assert_eq!(
            request(address, &resp(&[&[b"MGET", b"a", b"b", b"c"]])),
            b"*3\r\n$-1\r\n$1\r\n2\r\n$1\r\n3\r\n"
        );
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

#[test]
fn evented_native_pipeline_shares_sync_before_reply() {
    native_pipeline_shares_sync_before_reply(false);
}

#[test]
fn evented_worker_native_pipeline_shares_sync_before_reply() {
    native_pipeline_shares_sync_before_reply(true);
}

fn native_pipeline_shares_sync_before_reply(worker: bool) {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        sync_worker_active: worker,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    let payload =
        b"*6\r\n$4\r\nIDEM\r\n$1\r\ni\r\n$5\r\nBEGIN\r\n$1\r\nf\r\n$1\r\no\r\n$5\r\n60000\r\n\
            *4\r\n$5\r\nLIMIT\r\n$1\r\nl\r\n$2\r\n10\r\n$5\r\n60000\r\n\
            *6\r\n$4\r\nIDEM\r\n$1\r\ni\r\n$8\r\nCOMPLETE\r\n$1\r\nf\r\n$1\r\no\r\n$1\r\nr\r\n";
    assert_eq!(
        request(address, payload),
        b"*1\r\n$8\r\nacquired\r\n:9\r\n:1\r\n"
    );
    let metrics = request(address, b"*2\r\n$4\r\nINFO\r\n$10\r\nprometheus\r\n");
    let metrics = String::from_utf8(metrics).unwrap();
    assert!(
        metrics
            .lines()
            .any(|line| line.starts_with("ruvio_wal_syncs_total{") && line.ends_with(" 1")),
        "{metrics}"
    );
    let info = String::from_utf8(request(address, b"*1\r\n$4\r\nINFO\r\n")).unwrap();
    let sequence = |name: &str| {
        info.lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(
        sequence("wal_last_sequence:"),
        sequence("wal_durable_sequence:")
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence;
    });
    assert_eq!(
        request(
            address,
            b"*6\r\n$4\r\nIDEM\r\n$1\r\ni\r\n$5\r\nBEGIN\r\n$1\r\nf\r\n$1\r\no\r\n$5\r\n60000\r\n"
        ),
        b"*2\r\n$9\r\ncompleted\r\n$1\r\nr\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn evented_worker_rejects_unsupported_config_through_library_entry_point() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut config = AppConfig::default();
    config.persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        sync_worker_active: true,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    // One shard drops hot_keys, so the clash needs two.
    config.shards = 2;
    config.hot_keys.push("cache:*".into());
    let error = serve(listener, config, stopped()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("hot_keys"));
}

#[test]
fn evented_worker_hot_reads_and_fallback_transaction_are_durable() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        sync_worker_active: true,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    assert_eq!(
        request(
            address,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n\
                  *2\r\n$3\r\nGET\r\n$1\r\nk\r\n\
                  *1\r\n$5\r\nMULTI\r\n\
                  *2\r\n$4\r\nINCR\r\n$1\r\nn\r\n\
                  *1\r\n$4\r\nEXEC\r\n"
        ),
        b"+OK\r\n$1\r\nv\r\n+OK\r\n+QUEUED\r\n*1\r\n:1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    let (address, stop, server) = serve_with(|config| config.persistence = persistence);
    assert_eq!(
        request(
            address,
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nn\r\n"
        ),
        b"$1\r\nv\r\n$1\r\n1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_save_publishes_a_durable_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(60),
        ..PersistenceConfig::default()
    };

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });

    assert_eq!(
        request(
            address,
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*1\r\n$4\r\nSAVE\r\n"
        ),
        b"+OK\r\n+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    assert!(directory.path().join("CURRENT").exists());
    assert!(directory.path().join("CLOCK").exists());

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence;
    });

    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
        b"$5\r\nvalue\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_shutdown_drains_pending_clients_and_everysec_wal() {
    shutdown_with_pending_clients(false);
}

#[test]
fn evented_shutdown_drains_pending_clients_and_worker_wal() {
    shutdown_with_pending_clients(true);
}

fn shutdown_with_pending_clients(worker: bool) {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: if worker {
            crate::DurabilityProfile::Always
        } else {
            crate::DurabilityProfile::Everysec
        },
        sync_worker_active: worker,
        sync_interval: Duration::from_secs(60),
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
        config.shutdown_grace = Duration::from_millis(300);
    });
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"confirmed", b"yes"]])),
        b"+OK\r\n"
    );

    let mut subscriber = StdTcpStream::connect(address).unwrap();
    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    subscriber
        .write_all(&resp(&[&[b"SUBSCRIBE", b"news"]]))
        .unwrap();
    let mut subscribed = vec![0; crate::pubsub::encode_subscribe(b"news", 1).len()];
    subscriber.read_exact(&mut subscribed).unwrap();
    assert_eq!(subscribed, crate::pubsub::encode_subscribe(b"news", 1));

    let mut waiter = StdTcpStream::connect(address).unwrap();
    waiter
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    waiter
        .write_all(&resp(&[&[b"BLPOP", b"empty", b"0"]]))
        .unwrap();
    let mut byte = [0];
    assert!(matches!(
        waiter.read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ));
    waiter.set_read_timeout(Some(PATIENCE)).unwrap();

    let mut partial = StdTcpStream::connect(address).unwrap();
    partial.set_read_timeout(Some(PATIENCE)).unwrap();
    partial
        .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\npartial\r\n$5\r\nin")
        .unwrap();

    let mut pending = StdTcpStream::connect(address).unwrap();
    pending.set_read_timeout(Some(PATIENCE)).unwrap();
    pending
        .write_all(&resp(&[&[b"SET", b"pending", b"value"]]))
        .unwrap();
    // The earlier requests' connections may not be reaped yet, and under `always` the sync
    // worker may still be syncing the pending SET while this INFO is rendered: wait for
    // the five clients and for it to be durable.
    let ready_by = Instant::now() + PATIENCE;
    let info = loop {
        let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
        let field = |name: &str| {
            info.lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        let last = field("wal_last_sequence:");

        if last >= 2
            && info.contains("connected_clients:5\r\n")
            && (!worker || field("wal_durable_sequence:") == last)
        {
            break info;
        }
        assert!(
            Instant::now() < ready_by,
            "pending write not accepted: {info}"
        );
        thread::sleep(Duration::from_millis(5));
    };
    if !worker {
        assert!(!info.contains("wal_unsynced_bytes:0\r\n"), "{info}");
    }

    let began = Instant::now();
    stop.request();
    join_within(server, PATIENCE).unwrap();
    // The grace is 300 ms; a shutdown that waits for its clients instead never ends.
    assert!(
        began.elapsed() < Duration::from_secs(10),
        "shutdown exceeded drain deadline"
    );
    let mut blocked = Vec::new();
    waiter.read_to_end(&mut blocked).unwrap();
    assert_eq!(blocked, b"*-1\r\n");
    for mut client in [subscriber, partial] {
        let mut output = Vec::new();
        client.read_to_end(&mut output).unwrap();
        assert!(output.is_empty(), "unexpected shutdown reply: {output:?}");
    }
    let mut output = Vec::new();
    pending.read_to_end(&mut output).unwrap();
    assert!(output.is_empty() || output == b"+OK\r\n", "{output:?}");

    let (address, stop, server) = serve_with(|config| config.persistence = persistence);
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"confirmed"]])),
        b"$3\r\nyes\r\n"
    );
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"pending"]])),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"partial"]])),
        b"$-1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_worker_keeps_answering_reads_after_a_wal_append_failure() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        sync_worker_active: true,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"confirmed", b"yes"]])),
        b"+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = AppConfig {
        address: address.to_string(),
        shutdown_grace: Duration::from_millis(100),
        ..AppConfig::default()
    };
    config.persistence = persistence;
    let stop = ShutdownToken::default();
    let shutdown = stop.clone();
    let server = thread::spawn(move || {
        crate::persistence::arm_io_fault(crate::persistence::CrashSite::Append);
        serve(listener, config, shutdown)
    });
    let ready = (0..100).any(|_| {
        if let Ok(mut client) = StdTcpStream::connect(address) {
            client
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            if client.write_all(&resp(&[&[b"PING"]])).is_ok() {
                let mut reply = [0; 7];
                if client.read_exact(&mut reply).is_ok() && reply == *b"+PONG\r\n" {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
        false
    });
    assert!(ready, "fault-injection server failed to start");
    // The write hits the injected failure and is never acknowledged.
    assert!(request(address, &resp(&[&[b"SET", b"failed", b"no"]])).is_empty());
    // After the failure a batch that writes nothing is still answered.
    assert_eq!(request(address, &resp(&[&[b"PING"]])), b"+PONG\r\n");
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"confirmed"]])),
        b"$3\r\nyes\r\n"
    );
    // The injected fault was transient, so logging resumes and the next write is accepted.
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"later", b"yes"]])),
        b"+OK\r\n"
    );
    stop.request();
    let _ = join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_shutdown_after_wal_append_failure_keeps_memory_and_log_consistent() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"confirmed", b"yes"]])),
        b"+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = AppConfig {
        address: address.to_string(),
        shutdown_grace: Duration::from_millis(100),
        ..AppConfig::default()
    };
    config.persistence = persistence.clone();
    let stop = ShutdownToken::default();
    let shutdown = stop.clone();
    let server = thread::spawn(move || {
        crate::persistence::arm_io_fault(crate::persistence::CrashSite::Append);
        serve(listener, config, shutdown)
    });
    let ready = (0..100).any(|_| {
        if let Ok(mut client) = StdTcpStream::connect(address) {
            client
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            if client.write_all(&resp(&[&[b"PING"]])).is_ok() {
                let mut reply = [0; 7];
                if client.read_exact(&mut reply).is_ok() && reply == *b"+PONG\r\n" {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
        false
    });
    assert!(ready, "fault-injection server failed to start");
    // The write is never acknowledged...
    assert!(request(address, &resp(&[&[b"SET", b"failed", b"no"]])).is_empty());
    // ...but what memory holds after it is what the log holds after a restart.
    let in_memory = request(address, &resp(&[&[b"GET", b"failed"]]));
    stop.request();
    // The fault was transient: shutdown leaves the failed state and writes its checkpoint.
    join_within(server, PATIENCE).unwrap().unwrap();

    let (address, stop, server) = serve_with(|config| config.persistence = persistence);
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"confirmed"]])),
        b"$3\r\nyes\r\n"
    );
    assert_eq!(request(address, &resp(&[&[b"GET", b"failed"]])), in_memory);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Under `always` the refused record rolls the transaction back. Under `everysec` it is staged
/// with its group, so the failed group write leaves it applied and kept for the log's
/// recovery, like a plain grouped write (see docs/RECOVERY.md). Either way the client gets no
/// acknowledgement, and memory and the log agree after a restart.
#[test]
fn evented_transaction_refused_by_the_wal_leaves_memory_and_log_alike() {
    transaction_refused_by_the_wal_leaves_memory_and_log_alike(crate::DurabilityProfile::Always);
    transaction_refused_by_the_wal_leaves_memory_and_log_alike(crate::DurabilityProfile::Everysec);
}

fn transaction_refused_by_the_wal_leaves_memory_and_log_alike(profile: crate::DurabilityProfile) {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });
    let mut setup = resp(&[&[b"SET", b"{t}a", b"old"]]);

    setup.extend(resp(&[&[b"RPUSH", b"{t}list", b"x", b"y"]]));
    assert_eq!(request(address, &setup), b"+OK\r\n:2\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = AppConfig {
        address: address.to_string(),
        shutdown_grace: Duration::from_millis(100),
        ..AppConfig::default()
    };
    config.persistence = persistence.clone();
    let stop = ShutdownToken::default();
    let shutdown = stop.clone();
    let server = thread::spawn(move || {
        crate::persistence::arm_io_fault(crate::persistence::CrashSite::Append);
        serve(listener, config, shutdown)
    });
    let ready = (0..100).any(|_| {
        if let Ok(mut client) = StdTcpStream::connect(address) {
            client
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            if client.write_all(&resp(&[&[b"PING"]])).is_ok() {
                let mut reply = [0; 7];
                if client.read_exact(&mut reply).is_ok() && reply == *b"+PONG\r\n" {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
        false
    });
    assert!(ready, "fault-injection server failed to start");

    // Reads first, then writes to the same keys: the batch hits the injected failure.
    let mut batch = resp(&[&[b"MULTI"]]);

    batch.extend(resp(&[&[b"GET", b"{t}a"]]));
    batch.extend(resp(&[&[b"LLEN", b"{t}list"]]));
    batch.extend(resp(&[&[b"SET", b"{t}a", b"new"]]));
    batch.extend(resp(&[&[b"RPUSH", b"{t}list", b"z"]]));
    batch.extend(resp(&[&[b"SET", b"{t}created", b"v"]]));
    batch.extend(resp(&[&[b"EXEC"]]));
    let replied = request(address, &batch);

    assert!(
        !replied.windows(6).any(|window| window == b"*5\r\n$3"),
        "the refused batch was acknowledged: {}",
        String::from_utf8_lossy(&replied)
    );
    let mut check = resp(&[&[b"GET", b"{t}a"]]);

    check.extend(resp(&[&[b"LRANGE", b"{t}list", b"0", b"-1"]]));
    check.extend(resp(&[&[b"EXISTS", b"{t}created"]]));
    let expected: &[u8] = if profile == crate::DurabilityProfile::Always {
        b"$3\r\nold\r\n*2\r\n$1\r\nx\r\n$1\r\ny\r\n:0\r\n"
    } else {
        b"$3\r\nnew\r\n*3\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n:1\r\n"
    };

    assert_eq!(
        String::from_utf8_lossy(&request(address, &check)),
        String::from_utf8_lossy(expected)
    );
    stop.request();
    join_within(server, PATIENCE).unwrap().unwrap();

    let (address, stop, server) = serve_with(|config| config.persistence = persistence);

    assert_eq!(request(address, &check), expected);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_wal_backlog_checkpoint_and_memory_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        sync_interval: Duration::from_secs(60),
        snapshot_interval: Duration::from_secs(60),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
        config.memory.max_shard_bytes = 128 * 1024;
        config.memory.reserved_headroom_bytes = 0;
    });

    let value = vec![b'v'; 4096];
    let mut writes = Vec::new();
    for index in 0..48 {
        let key = format!("wal:{index}");
        writes.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n",
                key.len(),
                value.len()
            )
            .as_bytes(),
        );
        writes.extend_from_slice(&value);
        writes.extend_from_slice(b"\r\n");
    }
    let replies = request(address, &writes);
    let accepted = replies
        .windows(5)
        .filter(|part| *part == b"+OK\r\n")
        .count();
    let rejected = replies.windows(5).filter(|part| *part == b"-OOM ").count();
    assert!(
        accepted > 0 && rejected > 0,
        "unexpected replies: {replies:?}"
    );
    assert_eq!(accepted + rejected, 48);
    let info = String::from_utf8(request(address, b"*1\r\n$4\r\nINFO\r\n")).unwrap();
    let unsynced = info
        .lines()
        .find_map(|line| line.strip_prefix("wal_unsynced_bytes:"))
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(unsynced > 0, "expected WAL backlog: {info}");

    assert_eq!(request(address, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    let info = String::from_utf8(request(address, b"*1\r\n$4\r\nINFO\r\n")).unwrap();
    assert!(info.contains("wal_unsynced_bytes:0\r\n"), "{info}");
    for index in 0..48 {
        let key = format!("wal:{index}");
        let deletion = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{key}\r\n", key.len());
        let response = request(address, deletion.as_bytes());
        assert!(response == b":1\r\n" || response == b":0\r\n");
    }
    assert_eq!(request(address, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    let ready = Arc::new(std::sync::Barrier::new(2));
    let writer_ready = ready.clone();
    let writer = thread::spawn(move || {
        writer_ready.wait();
        for index in 0..32 {
            let value = index.to_string();
            let write = format!(
                "*3\r\n$3\r\nSET\r\n$7\r\ncurrent\r\n${}\r\n{value}\r\n",
                value.len()
            );
            assert_eq!(request(address, write.as_bytes()), b"+OK\r\n");
        }
    });
    ready.wait();
    for _ in 0..8 {
        assert_eq!(request(address, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
        assert_eq!(request(address, b"*1\r\n$4\r\nPING\r\n"), b"+PONG\r\n");
    }
    join_within(writer, PATIENCE).unwrap();
    let mut recovered = b"*3\r\n$3\r\nSET\r\n$9\r\nrecovered\r\n$4096\r\n".to_vec();
    recovered.extend_from_slice(&value);
    recovered.extend_from_slice(b"\r\n");
    assert_eq!(request(address, &recovered), b"+OK\r\n");
    assert_eq!(request(address, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence;
        config.memory.max_shard_bytes = 128 * 1024;
        config.memory.reserved_headroom_bytes = 0;
    });
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$9\r\nrecovered\r\n"),
        [b"$4096\r\n".as_slice(), &value, b"\r\n"].concat()
    );
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$7\r\ncurrent\r\n"),
        b"$2\r\n31\r\n"
    );
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$5\r\nwal:0\r\n"),
        b"$-1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_bgsave_aborted_by_the_journal_bound_still_completes() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Snapshot,
            data_dir: directory.path().to_path_buf(),
            snapshot_max_bytes: 256 * 1024 * 1024,
            // A tiny journal: any write during the walk ends the snapshot.
            snapshot_journal_bytes: 1024,
            // No scheduled checkpoint would pick up an aborted save.
            snapshot_interval: Duration::from_secs(3600),
            ..PersistenceConfig::default()
        };
    });
    let value = vec![b'v'; 100];

    for chunk in 0..10 {
        let keys: Vec<String> = (0..5_000).map(|n| format!("seed-{chunk}-{n}")).collect();
        let commands: Vec<[&[u8]; 3]> = keys
            .iter()
            .map(|key| [&b"SET"[..], key.as_bytes(), &value])
            .collect();
        let commands: Vec<&[&[u8]]> = commands.iter().map(|command| &command[..]).collect();

        request(address, &resp(&commands));
    }

    let writing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let writer = {
        let writing = Arc::clone(&writing);

        thread::spawn(move || {
            let mut round = 0u64;

            while writing.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("busy-{}", round % 64);

                request(address, &resp(&[&[b"SET", key.as_bytes(), b"x"]]));
                round += 1;
            }
        })
    };

    assert_eq!(
        request(address, b"*1\r\n$6\r\nBGSAVE\r\n"),
        b"+Background saving started\r\n"
    );
    let completed = wait_for(Duration::from_secs(20), || {
        directory.path().join("CURRENT").exists()
            && info_field(address, "rdb_bgsave_in_progress") == Some(0)
    });
    let aborts = info_field(address, "snapshot_aborts_total");

    writing.store(false, std::sync::atomic::Ordering::Relaxed);
    join_within(writer, PATIENCE).unwrap();
    stop.request();
    join_within(server, PATIENCE).unwrap();
    assert!(aborts.is_some_and(|aborts| aborts > 0), "{aborts:?}");
    assert!(completed, "BGSAVE never completed after {aborts:?} aborts");
}

#[test]
fn evented_write_larger_than_a_wal_segment_gets_a_clear_error_and_no_misconf() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Everysec,
            data_dir: directory.path().to_path_buf(),
            wal_segment_bytes: 64 * 1024,
            ..PersistenceConfig::default()
        };
    });
    let large = vec![b'x'; 70_000];
    let half = vec![b'x'; 40_000];
    let error = "-ERR write batch exceeds wal_segment_bytes";
    let mut batch = resp(&[
        // The hot path, a parsed command, a transaction and a script.
        &[b"SET", b"{w}big", &large],
        &[b"RPUSH", b"{w}list", &large],
        &[b"MULTI"],
        &[b"SET", b"{w}a", &half],
        &[b"SET", b"{w}b", &half],
        &[b"EXEC"],
        &[
            b"EVAL",
            b"return ruvio.call('SET', KEYS[1], ARGV[1])",
            b"1",
            b"{w}script",
            &large,
        ],
        // The refusal is not sticky: the log takes the next write.
        &[b"SET", b"{w}small", b"v"],
        &[
            b"EXISTS",
            b"{w}big",
            b"{w}list",
            b"{w}a",
            b"{w}b",
            b"{w}script",
        ],
    ]);

    batch.extend(resp(&[&[b"GET", b"{w}small"]]));
    let text = String::from_utf8(request(address, &batch)).unwrap();
    let replies: Vec<&str> = text.split_terminator("\r\n").collect();

    assert_eq!(
        replies,
        [
            error, error, "+OK", "+QUEUED", "+QUEUED", error, error, "+OK", ":0", "$1", "v"
        ],
        "{text}"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Under `everysec` a pipeline of plain writes, scripts and transactions is one WAL write,
/// made before any of its replies leaves, so a process kill right after the replies keeps
/// every acknowledged write.
#[test]
fn an_everysec_pipeline_of_scripts_and_transactions_is_one_wal_write() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    let set = b"return ruvio.call('SET', KEYS[1], ARGV[1])".as_slice();
    let payload = resp(&[
        &[b"SET", b"plain", b"p"],
        &[b"EVAL", set, b"1", b"a", b"1"],
        &[b"MULTI"],
        &[b"SET", b"c", b"3"],
        &[b"INCR", b"n"],
        &[b"EXEC"],
        &[b"EVAL", set, b"1", b"b", b"2"],
        &[
            b"EVAL",
            b"return ruvio.call('RPUSH', KEYS[1], 'x')",
            b"1",
            b"l",
        ],
    ]);

    crate::persistence::COUNTED_WRITE_DIRS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), 0));
    let replies = request(address, &payload);
    let writes = {
        let mut counted = crate::persistence::COUNTED_WRITE_DIRS.lock().unwrap();
        let position = counted
            .iter()
            .position(|(dir, _)| dir == directory.path())
            .unwrap();

        counted.remove(position).1
    };
    assert_eq!(
        String::from_utf8_lossy(&replies),
        "+OK\r\n+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n:1\r\n+OK\r\n:1\r\n"
    );
    assert_eq!(writes, 1);

    // A kill: the copy holds what reached the kernel, synced or not.
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: killed.path().to_path_buf(),
            ..persistence
        }
    });
    let check = resp(&[
        &[b"MGET", b"plain", b"a", b"b", b"c", b"n"],
        &[b"LRANGE", b"l", b"0", b"-1"],
    ]);
    assert_eq!(
        String::from_utf8_lossy(&request(address, &check)),
        "*5\r\n$1\r\np\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n*1\r\n$1\r\nx\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Under `everysec` single-command clients served in one scheduler turn share one WAL
/// write, made before any of their replies leaves: a process kill right after the replies
/// keeps every acknowledged write.
#[test]
fn everysec_clients_served_in_one_turn_share_a_wal_write() {
    const CLIENTS: usize = 16;
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    let mut fewest = usize::MAX;
    let mut acknowledged = 0;

    crate::persistence::COUNTED_WRITE_DIRS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), 0));
    // Each round sends every client's SET before reading any reply; the shard may still
    // see them across several turns, so a few rounds are tried.
    for round in 0..5 {
        let mut clients: Vec<_> = (0..CLIENTS)
            .map(|_| {
                let client = StdTcpStream::connect(address).unwrap();

                client.set_read_timeout(Some(PATIENCE)).unwrap();
                client
            })
            .collect();
        let before = written(directory.path());

        for (index, client) in clients.iter_mut().enumerate() {
            let key = format!("k{round}-{index}");

            client
                .write_all(&resp(&[&[b"SET", key.as_bytes(), b"v"]]))
                .unwrap();
        }
        for client in &mut clients {
            let mut reply = [0u8; 5];

            client.read_exact(&mut reply).unwrap();
            assert_eq!(&reply, b"+OK\r\n");
            acknowledged += 1;
        }
        fewest = fewest.min(usize::try_from(written(directory.path()) - before).unwrap());
        if fewest < CLIENTS {
            break;
        }
    }
    crate::persistence::COUNTED_WRITE_DIRS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    assert!(fewest < CLIENTS, "every client wrote on its own");

    // A kill: the copy holds what reached the kernel, synced or not.
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: killed.path().to_path_buf(),
            ..persistence
        }
    });
    let reply = request(address, &resp(&[&[b"DBSIZE"]]));
    let size: usize = String::from_utf8_lossy(&reply)
        .trim()
        .trim_start_matches(':')
        .parse()
        .unwrap();

    assert_eq!(size, acknowledged);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

fn written(directory: &std::path::Path) -> u64 {
    crate::persistence::COUNTED_WRITE_DIRS
        .lock()
        .unwrap()
        .iter()
        .find(|(dir, _)| dir == directory)
        .unwrap()
        .1
}

/// Under `everysec` a failed group write meets a script or `EXEC` the way it meets a plain
/// grouped write: the client is cut off without a reply, the changes stay applied in memory,
/// and the kept records are written once the log resumes with the next write.
#[test]
fn an_everysec_grouped_script_or_transaction_whose_write_fails_acts_like_a_plain_write() {
    let set = b"return ruvio.call('SET', KEYS[1], ARGV[1])".as_slice();
    let batches = [
        resp(&[&[b"SET", b"other", b"new"], &[b"SET", b"k", b"new"]]),
        resp(&[
            &[b"SET", b"other", b"new"],
            &[b"EVAL", set, b"1", b"k", b"new"],
        ]),
        resp(&[
            &[b"SET", b"other", b"new"],
            &[b"MULTI"],
            &[b"SET", b"k", b"new"],
            &[b"EXEC"],
        ]),
    ];

    for batch in batches {
        let directory = tempfile::tempdir().unwrap();
        let persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Everysec,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());

        assert_eq!(
            request(
                address,
                &resp(&[&[b"MSET", b"k", b"old", b"other", b"old"]])
            ),
            b"+OK\r\n"
        );
        crate::persistence::FAILING_WRITE_SHARDS
            .lock()
            .unwrap()
            .push((directory.path().to_path_buf(), 0, 1));
        let refused = request(address, &batch);
        crate::persistence::FAILING_WRITE_SHARDS
            .lock()
            .unwrap()
            .retain(|(dir, _, _)| dir != directory.path());
        // The group's one write failed: no reply at all, not even the plain write's.
        assert!(
            refused.is_empty(),
            "{:?}",
            String::from_utf8_lossy(&refused)
        );
        // The changes stay applied in memory.
        assert_eq!(
            request(address, &resp(&[&[b"MGET", b"k", b"other"]])),
            b"*2\r\n$3\r\nnew\r\n$3\r\nnew\r\n"
        );
        // The log resumes with the next write, which writes the kept records first.
        assert_eq!(
            request(address, &resp(&[&[b"SET", b"later", b"v"]])),
            b"+OK\r\n"
        );
        let killed = tempfile::tempdir().unwrap();

        copy_dir(directory.path(), killed.path());
        stop.request();
        join_within(server, PATIENCE).unwrap();

        let (address, stop, server) = serve_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: killed.path().to_path_buf(),
                ..persistence
            }
        });
        assert_eq!(
            request(address, &resp(&[&[b"MGET", b"k", b"other", b"later"]])),
            b"*3\r\n$3\r\nnew\r\n$3\r\nnew\r\n$1\r\nv\r\n"
        );
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// With one shard `hot_keys` is dropped: the keys live in the shard, so a restart finds
/// them and the checkpoint still matches the one shard.
#[test]
fn one_shard_with_hot_keys_restarts_with_its_keys() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let start = |persistence: PersistenceConfig| {
        serve_with(move |config| {
            config.persistence = persistence;
            config.hot_keys = vec![b"lock:*".to_vec()];
        })
    };
    let (address, stop, server) = start(persistence.clone());

    assert_eq!(
        request(
            address,
            b"*3\r\n$3\r\nSET\r\n$6\r\nlock:a\r\n$1\r\nv\r\n*1\r\n$4\r\nSAVE\r\n"
        ),
        b"+OK\r\n+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = start(persistence);

    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$6\r\nlock:a\r\n"),
        b"$1\r\nv\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}
