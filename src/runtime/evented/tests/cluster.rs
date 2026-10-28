use super::*;

#[test]
fn evented_two_shards_forward_and_reject_cross_slot_keys() {
    use crate::routing::hash_slot;

    let (left_addr, right_addr, stop, left_server, right_server) = serve_two_shards();
    let slots = String::from_utf8(request(
        left_addr,
        b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
    ))
    .unwrap();

    assert!(slots.contains(":0\r\n:8191\r\n"), "{slots}");
    assert!(slots.contains(":8192\r\n:16383\r\n"), "{slots}");

    let nodes = String::from_utf8(request(
        left_addr,
        b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n",
    ))
    .unwrap();

    assert!(nodes.contains("myself,master"), "{nodes}");
    assert!(
        nodes.contains(&format!("127.0.0.1:{}", left_addr.port())),
        "{nodes}"
    );
    assert!(
        nodes.contains(&format!("127.0.0.1:{}", right_addr.port())),
        "{nodes}"
    );

    let info = String::from_utf8(request(left_addr, b"*1\r\n$4\r\nINFO\r\n")).unwrap();

    assert!(info.contains("ruvio_backend:evented"), "{info}");
    assert!(info.contains("ruvio_shard_id:0"), "{info}");
    assert!(info.contains("ruvio_shard_count:2"), "{info}");

    let slot = hash_slot(b"left");
    let owner = if slot < 8192 { left_addr } else { right_addr };

    let other = if slot < 8192 { right_addr } else { left_addr };

    let set = request(
        owner,
        b"*3\r\n$3\r\nSET\r\n$12\r\n{left}:value\r\n$6\r\nstored\r\n",
    );

    assert_eq!(set, b"+OK\r\n");
    assert_eq!(
        request(owner, b"*2\r\n$3\r\nGET\r\n$12\r\n{left}:value\r\n"),
        b"$6\r\nstored\r\n"
    );
    // The other shard forwards the key to its owner instead of answering MOVED.
    assert_eq!(
        request(other, b"*2\r\n$3\r\nGET\r\n$12\r\n{left}:value\r\n"),
        b"$6\r\nstored\r\n"
    );

    let cross = String::from_utf8(request(
        left_addr,
        b"*3\r\n$3\r\nDEL\r\n$8\r\n{left}:a\r\n$9\r\n{right}:b\r\n",
    ))
    .unwrap();

    assert!(cross.starts_with("-CROSSSLOT"), "{cross}");

    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_cluster_discovery_reports_shards_and_the_advertised_port() {
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.advertised_port = Some(17000));
    let info = String::from_utf8(request(
        right_addr,
        &resp(&[
            &[b"CLUSTER", b"INFO"],
            &[b"CLUSTER", b"MYID"],
            &[b"CLUSTER", b"KEYSLOT", b"{user1000}.following"],
            &[b"READONLY"],
            &[b"READWRITE"],
        ]),
    ))
    .unwrap();

    assert!(info.contains("cluster_state:ok\r\n"), "{info}");
    assert!(info.contains("cluster_known_nodes:2\r\n"), "{info}");
    assert!(info.contains("cluster_my_epoch:2\r\n"), "{info}");
    // A node's id is the SHA-1 of the address it is announced on: shard 1 is on 17001.
    let id: String = {
        use sha1::{Digest, Sha1};

        Sha1::digest(b"127.0.0.1:17001")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    };

    assert!(info.contains(&format!("$40\r\n{id}\r\n")), "{info}");
    assert!(info.ends_with(":3443\r\n+OK\r\n+OK\r\n"), "{info}");

    let shards = String::from_utf8(request(left_addr, &resp(&[&[b"CLUSTER", b"SHARDS"]]))).unwrap();

    assert!(
        shards.starts_with("*2\r\n*4\r\n$5\r\nslots\r\n*2\r\n:0\r\n:8191\r\n"),
        "{shards}"
    );
    assert!(shards.contains("$4\r\nport\r\n:17000\r\n"), "{shards}");
    assert!(shards.contains("$4\r\nport\r\n:17001\r\n"), "{shards}");

    let slots = String::from_utf8(request(left_addr, &resp(&[&[b"CLUSTER", b"SLOTS"]]))).unwrap();

    assert!(slots.contains("127.0.0.1\r\n:17001\r\n"), "{slots}");

    // Slot 11058 belongs to the other shard; the command is forwarded there, not redirected.
    assert_eq!(
        request(left_addr, b"*2\r\n$3\r\nGET\r\n$7\r\nsomekey\r\n"),
        b"$-1\r\n"
    );

    let server = String::from_utf8(request(left_addr, &resp(&[&[b"INFO"]]))).unwrap();

    assert!(
        server.contains("# Cluster\r\ncluster_enabled:1\r\n"),
        "{server}"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_two_shards_save_recovers_both_owners() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(60),
        ..PersistenceConfig::default()
    };

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    assert_eq!(
        request(
            left_addr,
            b"*3\r\n$3\r\nSET\r\n$9\r\n{right}:k\r\n$4\r\nwest\r\n"
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        request(
            right_addr,
            b"*3\r\n$3\r\nSET\r\n$8\r\n{left}:k\r\n$4\r\neast\r\n"
        ),
        b"+OK\r\n"
    );
    // SAVE on one shard asks every shard and answers once the round is published.
    assert_eq!(request(left_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    assert!(directory.path().join("CURRENT").exists());
    assert_eq!(request(right_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    assert!(directory.path().join("CLOCK").exists());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence;
        });

    assert_eq!(
        request(left_addr, b"*2\r\n$3\r\nGET\r\n$9\r\n{right}:k\r\n"),
        b"$4\r\nwest\r\n"
    );
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$8\r\n{left}:k\r\n"),
        b"$4\r\neast\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_rejects_wal_only_shard_count_changes_before_serving() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    for shard in 0..2 {
        let (mut sink, _, _) =
            WalSink::open(persistence.clone(), shard, ProtocolLimits::default()).unwrap();
        sink.begin_group();
        sink.record(crate::mutation::Mutation::SetBytes {
            key: b"key",
            value: b"value",
        })
        .unwrap();
        sink.end_group().unwrap();
        sink.flush().unwrap();
    }
    assert!(!directory.path().join("CURRENT").exists());

    let mut config = AppConfig {
        persistence,
        address: "127.0.0.1:0".into(),
        ..AppConfig::default()
    };
    let error = run(&config, stopped()).unwrap_err();
    assert!(error.to_string().contains("WAL shard layout"), "{error}");
    assert!(!directory.path().join("SHARD_LAYOUT").exists());

    validate_shard_layout(&config.persistence, 2, 2, config.protocol).unwrap();
    config.shards = 3;
    let error = run(&config, stopped()).unwrap_err();
    assert!(error.to_string().contains("shard layout"), "{error}");
}

#[test]
fn evented_rejects_legacy_checkpoint_only_shard_count_change() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (left, right, stop, left_server, right_server) = serve_two_shards_with(|config| {
        config.persistence = persistence.clone();
    });
    assert_eq!(
        request(left, &resp(&[&[b"SET", b"{right}:k", b"v"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right, &resp(&[&[b"SET", b"{left}:k", b"v"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    assert_eq!(request(right, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
    assert!(directory.path().join("CURRENT").exists());
    assert!(!directory.path().join("SHARD_LAYOUT").exists());
    let config = AppConfig {
        persistence,
        address: "127.0.0.1:0".into(),
        ..AppConfig::default()
    };
    let error = run(&config, stopped()).unwrap_err();
    assert!(
        error.to_string().contains("checkpoint shard count"),
        "{error}"
    );
}

#[test]
fn evented_rejects_empty_durable_layout_change_and_legacy_single_shard_wal() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let config = AppConfig {
        persistence: persistence.clone(),
        ..AppConfig::default()
    };
    validate_shard_layout(&persistence, 2, 2, config.protocol).unwrap();
    assert!(validate_shard_layout(&persistence, 1, 1, config.protocol).is_err());

    let old = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: old.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (address, stop, server) = serve_with(|config| config.persistence = persistence.clone());
    assert_eq!(
        request(address, &resp(&[&[b"SET", b"key", b"value"]])),
        b"+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    assert!(validate_shard_layout(&persistence, 2, 2, config.protocol).is_err());
}

#[test]
fn evented_hot_keys_are_visible_on_both_shards_without_moved() {
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    assert_eq!(
        request(
            left_addr,
            b"*3\r\n$3\r\nSET\r\n$11\r\nlock:shared\r\n$4\r\nheld\r\n"
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$11\r\nlock:shared\r\n"),
        b"$4\r\nheld\r\n"
    );
    assert_eq!(
        request(
            right_addr,
            b"*3\r\n$3\r\nSET\r\n$11\r\nlock:shared\r\n$7\r\nupdated\r\n"
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        request(left_addr, b"*2\r\n$3\r\nGET\r\n$11\r\nlock:shared\r\n"),
        b"$7\r\nupdated\r\n"
    );
    assert_eq!(
        request(left_addr, b"*2\r\n$3\r\nDEL\r\n$11\r\nlock:shared\r\n"),
        b":1\r\n"
    );
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$11\r\nlock:shared\r\n"),
        b"$-1\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_keys_set_nx_has_one_winner_across_shards() {
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    let left = thread::spawn(move || {
        request(
            left_addr,
            b"*4\r\n$3\r\nSET\r\n$11\r\nlock:global\r\n$4\r\nleft\r\n$2\r\nNX\r\n",
        )
    });

    let right = thread::spawn(move || {
        request(
            right_addr,
            b"*4\r\n$3\r\nSET\r\n$11\r\nlock:global\r\n$5\r\nright\r\n$2\r\nNX\r\n",
        )
    });

    let replies = [
        join_within(left, PATIENCE).unwrap(),
        join_within(right, PATIENCE).unwrap(),
    ];
    let wins = replies
        .iter()
        .filter(|reply| reply.as_slice() == b"+OK\r\n")
        .count();
    let misses = replies
        .iter()
        .filter(|reply| reply.as_slice() == b"$-1\r\n")
        .count();

    assert_eq!(wins, 1, "{replies:?}");
    assert_eq!(misses, 1, "{replies:?}");
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_keys_reject_unsupported_and_leave_shard_untouched() {
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    let rejected = request(
        left_addr,
        b"*3\r\n$5\r\nLPUSH\r\n$8\r\nlock:foo\r\n$1\r\nx\r\n",
    );

    assert_eq!(rejected, b"-ERR hot key does not support lpush\r\n");
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n"),
        b"$-1\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn a_hot_key_inside_multi_is_refused_and_aborts_exec() {
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    assert_eq!(
        request(
            left_addr,
            b"*3\r\n$3\r\nSET\r\n$8\r\nlock:foo\r\n$3\r\nold\r\n\
              *1\r\n$5\r\nMULTI\r\n\
              *3\r\n$3\r\nSET\r\n$8\r\nlock:foo\r\n$3\r\nnew\r\n\
              *1\r\n$7\r\nDISCARD\r\n\
              *1\r\n$5\r\nMULTI\r\n\
              *2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n\
              *2\r\n$3\r\nDEL\r\n$8\r\nlock:foo\r\n\
              *1\r\n$4\r\nEXEC\r\n\
              *1\r\n$5\r\nMULTI\r\n\
              *2\r\n$4\r\nINCR\r\n$8\r\nlock:foo\r\n\
              *1\r\n$4\r\nEXEC\r\n\
              *1\r\n$5\r\nMULTI\r\n\
              *1\r\n$4\r\nPING\r\n\
              *1\r\n$4\r\nEXEC\r\n"
        ),
        b"+OK\r\n\
          +OK\r\n-ERR hot keys cannot be used inside MULTI\r\n+OK\r\n\
          +OK\r\n-ERR hot keys cannot be used inside MULTI\r\n\
          -ERR hot keys cannot be used inside MULTI\r\n\
          -EXECABORT Transaction discarded because of previous errors.\r\n\
          +OK\r\n-ERR hot keys cannot be used inside MULTI\r\n\
          -EXECABORT Transaction discarded because of previous errors.\r\n\
          +OK\r\n+QUEUED\r\n*1\r\n+PONG\r\n"
    );
    // Nothing queued with a hot key ran: the value is the one set before the MULTIs.
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n"),
        b"$3\r\nold\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_keys_save_publishes_after_hot_partition() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(60),
        ..PersistenceConfig::default()
    };

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    assert_eq!(
        request(
            left_addr,
            b"*3\r\n$3\r\nSET\r\n$8\r\nlock:foo\r\n$4\r\nheld\r\n"
        ),
        b"+OK\r\n"
    );
    // Published with every shard's file and the hot partition before SAVE answers.
    assert_eq!(request(left_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    assert!(directory.path().join("CURRENT").exists());
    assert_eq!(request(right_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence;
            config.hot_keys = vec![b"lock:*".to_vec()];
        });

    assert_eq!(
        request(left_addr, b"*2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n"),
        b"$4\r\nheld\r\n"
    );
    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n"),
        b"$4\r\nheld\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_keys_off_still_shards_prefixed_keys() {
    use crate::routing::hash_slot;

    let (left_addr, right_addr, stop, left_server, right_server) = serve_two_shards();
    let slot = hash_slot(b"lock:foo");
    let owner = if slot < 8192 { left_addr } else { right_addr };

    let other = if slot < 8192 { right_addr } else { left_addr };

    assert_eq!(
        request(
            owner,
            b"*3\r\n$3\r\nSET\r\n$8\r\nlock:foo\r\n$4\r\nheld\r\n"
        ),
        b"+OK\r\n"
    );
    // Without `hot_keys` the prefix is an ordinary key: owned by one shard, forwarded by the other.
    assert_eq!(
        request(other, b"*2\r\n$3\r\nGET\r\n$8\r\nlock:foo\r\n"),
        b"$4\r\nheld\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_shards_stay_responsive_while_a_peer_blocks() {
    use std::io::ErrorKind;

    let (left, right, stop, left_server, right_server) = serve_two_shards();
    let blocked_key = key_on_shard(0, "blocked");
    let left_key = key_on_shard(0, "left");
    let right_key = key_on_shard(1, "right");
    let right_counter = key_on_shard(1, "counter");
    let mut waiting = StdTcpStream::connect(left).unwrap();

    waiting
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    waiting.set_write_timeout(Some(PATIENCE)).unwrap();
    waiting
        // Without a timeout: a shard blocked by it would not answer the requests below
        // before their deadline.
        .write_all(&resp(&[&[b"BLPOP", blocked_key.as_bytes(), b"0"]]))
        .unwrap();
    let mut probe = [0u8; 1];
    let still_blocked = waiting.read(&mut probe).unwrap_err();

    assert!(
        matches!(
            still_blocked.kind(),
            ErrorKind::TimedOut | ErrorKind::WouldBlock
        ),
        "{still_blocked}"
    );

    let peer = resp(&[
        &[b"SET", right_key.as_bytes(), b"v"],
        &[b"GET", right_key.as_bytes()],
        &[b"INCR", right_counter.as_bytes()],
    ]);
    let local = resp(&[
        &[b"SET", left_key.as_bytes(), b"v"],
        &[b"GET", left_key.as_bytes()],
        &[b"PING"],
    ]);

    assert_eq!(
        request_within(right, &peer, PATIENCE),
        b"+OK\r\n$1\r\nv\r\n:1\r\n"
    );
    assert_eq!(
        request_within(left, &local, PATIENCE),
        b"+OK\r\n$1\r\nv\r\n+PONG\r\n"
    );
    assert_eq!(
        request(left, &resp(&[&[b"LPUSH", blocked_key.as_bytes(), b"x"]])),
        b":1\r\n"
    );
    waiting.set_read_timeout(Some(PATIENCE)).unwrap();
    let expected = format!(
        "*2\r\n${}\r\n{}\r\n$1\r\nx\r\n",
        blocked_key.len(),
        blocked_key
    );
    let mut woke = vec![0u8; expected.len()];

    waiting.read_exact(&mut woke).unwrap();
    assert_eq!(woke, expected.as_bytes());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_script_quota_keeps_both_shards_responsive() {
    let (left, right, stop, left_server, right_server) = serve_two_shards_with(|config| {
        config.scripts.max_instructions = 100_000;
    });
    let script = resp(&[&[b"EVAL", b"while true do end", b"0"]]);
    let left_request = thread::spawn(move || request_within(left, &script, PATIENCE));
    assert_eq!(
        request_within(right, &resp(&[&[b"PING"]]), PATIENCE),
        b"+PONG\r\n"
    );
    assert_eq!(
        join_within(left_request, PATIENCE).unwrap(),
        b"-ERR Lua instruction quota exceeded\r\n"
    );
    assert_eq!(
        request_within(left, &resp(&[&[b"PING"]]), PATIENCE),
        b"+PONG\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_shards_keep_keys_scripts_and_subscribers_local() {
    let (left, right, stop, left_server, right_server) = serve_two_shards();
    let left_key = key_on_shard(0, "owned");
    let right_key = key_on_shard(1, "other");

    assert_eq!(
        request(left, &resp(&[&[b"SET", left_key.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right, &resp(&[&[b"SET", right_key.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left, &resp(&[&[b"DBSIZE"]])), b":1\r\n");
    assert_eq!(request(right, &resp(&[&[b"DBSIZE"]])), b":1\r\n");

    let left_info = String::from_utf8(request(left, &resp(&[&[b"INFO"]]))).unwrap();
    let right_info = String::from_utf8(request(right, &resp(&[&[b"INFO"]]))).unwrap();

    assert!(left_info.contains("db0:keys=1,expires=0"), "{left_info}");
    assert!(right_info.contains("db0:keys=1,expires=0"), "{right_info}");
    assert!(!left_info.contains("db0:keys=2"), "{left_info}");

    let scan = request(left, &resp(&[&[b"SCAN", b"0"]]));

    assert!(
        scan.windows(left_key.len())
            .any(|window| window == left_key.as_bytes())
    );
    assert!(
        !scan
            .windows(right_key.len())
            .any(|window| window == right_key.as_bytes())
    );
    assert_eq!(
        request(left, &resp(&[&[b"GET", right_key.as_bytes()]])),
        b"$1\r\nv\r\n"
    );
    assert!(
        request(
            left,
            &resp(&[&[
                b"MSET",
                left_key.as_bytes(),
                b"no",
                right_key.as_bytes(),
                b"no",
            ]])
        )
        .starts_with(b"-CROSSSLOT")
    );
    assert!(
        request(
            left,
            &resp(&[&[b"RENAME", left_key.as_bytes(), right_key.as_bytes()]])
        )
        .starts_with(b"-CROSSSLOT")
    );
    assert_eq!(
        request(left, &resp(&[&[b"GET", left_key.as_bytes()]])),
        b"$1\r\nv\r\n"
    );

    let loaded = request(left, &resp(&[&[b"SCRIPT", b"LOAD", b"return 7"]]));

    assert!(loaded.starts_with(b"$40\r\n"), "{loaded:?}");
    let sha = loaded[5..45].to_vec();

    assert!(
        request(right, &resp(&[&[b"EVALSHA", sha.as_slice(), b"0"]])).starts_with(b"-NOSCRIPT"),
    );
    assert_eq!(
        request(left, &resp(&[&[b"EVALSHA", sha.as_slice(), b"0"]])),
        b":7\r\n"
    );

    let mut subscriber = StdTcpStream::connect(right).unwrap();

    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    subscriber
        .write_all(&resp(&[&[b"SUBSCRIBE", b"news"]]))
        .unwrap();
    let expected = crate::pubsub::encode_subscribe(b"news", 1);
    let mut subscribed = vec![0u8; expected.len()];

    subscriber.read_exact(&mut subscribed).unwrap();
    assert_eq!(subscribed, expected);
    // Classic PUBLISH reaches a subscriber on any shard (Redis Cluster broadcasts it too);
    // the count answers for the publisher's own shard, as a cluster node does.
    let expected = crate::pubsub::encode_message(b"news", b"hello");
    let mut message = vec![0u8; expected.len()];

    assert_eq!(
        request(left, &resp(&[&[b"PUBLISH", b"news", b"hello"]])),
        b":0\r\n"
    );
    subscriber.read_exact(&mut message).unwrap();
    assert_eq!(message, expected);
    assert_eq!(
        request(right, &resp(&[&[b"PUBLISH", b"news", b"hello"]])),
        b":1\r\n"
    );
    subscriber.read_exact(&mut message).unwrap();
    assert_eq!(message, expected);
    let info = String::from_utf8(request(right, &resp(&[&[b"INFO"]]))).unwrap();
    let queued_peak = info
        .lines()
        .find_map(|line| line.strip_prefix("pubsub_queue_peak_bytes:"))
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(queued_peak >= expected.len(), "{info}");
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn a_publish_the_gate_refuses_reaches_no_other_shard() {
    use std::io::ErrorKind;

    let (left, right, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.security.requirepass = "secret".into());
    let mut subscriber = StdTcpStream::connect(right).unwrap();

    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    subscriber
        .write_all(&resp(&[&[b"AUTH", b"secret"], &[b"SUBSCRIBE", b"news"]]))
        .unwrap();
    let expected = [
        b"+OK\r\n".to_vec(),
        crate::pubsub::encode_subscribe(b"news", 1),
    ]
    .concat();
    let mut subscribed = vec![0u8; expected.len()];

    subscriber.read_exact(&mut subscribed).unwrap();
    assert_eq!(subscribed, expected);

    // Unauthenticated: refused on the publishing shard, and nothing crosses to the other.
    let refused = request(left, &resp(&[&[b"PUBLISH", b"news", b"leak"]]));

    assert!(refused.starts_with(b"-NOAUTH"), "{refused:?}");
    subscriber
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut quiet = [0u8; 1];
    let missed = subscriber.read(&mut quiet).unwrap_err();

    assert!(
        matches!(missed.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock),
        "{missed}"
    );

    // Inside MULTI a PUBLISH is queued and sent only at EXEC: DISCARD sends nothing.
    let queued = String::from_utf8(request(
        left,
        &resp(&[
            &[b"AUTH", b"secret"],
            &[b"MULTI"],
            &[b"PUBLISH", b"news", b"queued"],
            &[b"DISCARD"],
        ]),
    ))
    .unwrap();

    assert_eq!(queued, "+OK\r\n+OK\r\n+QUEUED\r\n+OK\r\n");
    let missed = subscriber.read(&mut quiet).unwrap_err();

    assert!(matches!(
        missed.kind(),
        ErrorKind::TimedOut | ErrorKind::WouldBlock
    ));

    // EXEC sends it, to the subscriber on the other shard too; the reply counts this shard's.
    assert_eq!(
        request(
            left,
            &resp(&[
                &[b"AUTH", b"secret"],
                &[b"MULTI"],
                &[b"PUBLISH", b"news", b"committed"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n:0\r\n"
    );
    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    let expected = crate::pubsub::encode_message(b"news", b"committed");
    let mut message = vec![0u8; expected.len()];

    subscriber.read_exact(&mut message).unwrap();
    assert_eq!(message, expected);

    // Authenticated and outside MULTI: delivered.
    assert_eq!(
        request(
            left,
            &resp(&[&[b"AUTH", b"secret"], &[b"PUBLISH", b"news", b"hello"]])
        ),
        b"+OK\r\n:0\r\n"
    );
    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    let expected = crate::pubsub::encode_message(b"news", b"hello");
    let mut message = vec![0u8; expected.len()];

    subscriber.read_exact(&mut message).unwrap();
    assert_eq!(message, expected);
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_shards_flush_locally_and_share_one_acl_directory() {
    let (left, right, stop, left_server, right_server) = serve_two_shards();
    let left_key = key_on_shard(0, "flush-left");
    let right_key = key_on_shard(1, "flush-right");

    assert_eq!(
        request(left, &resp(&[&[b"SET", left_key.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right, &resp(&[&[b"SET", right_key.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    let mixed = request(
        left,
        &resp(&[
            &[b"GET", left_key.as_bytes()],
            &[b"GET", right_key.as_bytes()],
        ]),
    );

    assert_eq!(mixed, b"$1\r\nv\r\n$1\r\nv\r\n", "{mixed:?}");
    assert_eq!(
        request(left, &resp(&[&[b"MOVE", left_key.as_bytes(), b"1"]])),
        b"-ERR MOVE is not allowed in cluster mode\r\n"
    );
    assert_eq!(
        request(left, &resp(&[&[b"GET", left_key.as_bytes()]])),
        b"$1\r\nv\r\n"
    );

    let loaded = request(left, &resp(&[&[b"SCRIPT", b"LOAD", b"return 7"]]));
    let sha = loaded[5..45].to_vec();

    assert_eq!(
        request(right, &resp(&[&[b"SCRIPT", b"LOAD", b"return 7"]])),
        loaded.as_slice()
    );
    assert_eq!(request(left, &resp(&[&[b"SCRIPT", b"FLUSH"]])), b"+OK\r\n");
    assert!(request(left, &resp(&[&[b"EVALSHA", sha.as_slice(), b"0"]])).starts_with(b"-NOSCRIPT"));
    assert_eq!(
        request(right, &resp(&[&[b"EVALSHA", sha.as_slice(), b"0"]])),
        b":7\r\n"
    );
    assert_eq!(
        request(
            left,
            &resp(&[&[
                b"ACL", b"SETUSER", b"app", b"on", b">pw", b"~*", b"+get", b"+auth", b"+acl",
            ]])
        ),
        b"+OK\r\n"
    );
    assert_eq!(
            request(
                right,
                &resp(&[
                    &[b"AUTH", b"app", b"pw"],
                    &[b"ACL", b"WHOAMI"],
                    &[b"GET", right_key.as_bytes()],
                    &[b"SET", right_key.as_bytes(), b"no"],
                ])
            ),
            b"+OK\r\n$3\r\napp\r\n$1\r\nv\r\n-NOPERM this user has no permissions to run the 'set' command or its subcommand\r\n"
        );
    assert_eq!(request(left, &resp(&[&[b"FLUSHALL"]])), b"+OK\r\n");
    assert_eq!(
        request(left, &resp(&[&[b"GET", left_key.as_bytes()]])),
        b"$-1\r\n"
    );
    assert_eq!(
        request(right, &resp(&[&[b"GET", right_key.as_bytes()]])),
        b"$1\r\nv\r\n"
    );
    assert_eq!(request(right, &resp(&[&[b"FLUSHDB"]])), b"+OK\r\n");
    assert_eq!(
        request(right, &resp(&[&[b"GET", right_key.as_bytes()]])),
        b"$-1\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_shards_run_same_slot_work_and_route_sharded_channels() {
    use std::io::ErrorKind;

    let (left, right, stop, left_server, right_server) = serve_two_shards();
    let tag = tag_on_shard(0);
    let alpha = format!("{{{tag}}}a");
    let beta = format!("{{{tag}}}b");
    let set_a = format!("{{{tag}}}sa");
    let set_b = format!("{{{tag}}}sb");
    let stream_a = format!("{{{tag}}}s1");
    let stream_b = format!("{{{tag}}}s2");
    let other = key_on_shard(1, "remote");
    let watched = key_on_shard(0, "watched");
    let stable = key_on_shard(1, "stable");
    let channel = key_on_shard(1, "schan");

    assert_eq!(
        request(
            left,
            &resp(&[
                &[b"SET", alpha.as_bytes(), b"va"],
                &[b"SET", beta.as_bytes(), b"vb"],
                &[b"MGET", alpha.as_bytes(), beta.as_bytes()],
                &[b"EXISTS", alpha.as_bytes(), beta.as_bytes()],
                &[b"SADD", set_a.as_bytes(), b"m"],
                &[b"SADD", set_b.as_bytes(), b"m"],
                &[b"SMOVE", set_a.as_bytes(), set_b.as_bytes(), b"m"],
                &[b"SISMEMBER", set_b.as_bytes(), b"m"],
            ])
        ),
        b"+OK\r\n+OK\r\n*2\r\n$2\r\nva\r\n$2\r\nvb\r\n:2\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
    );
    assert!(
        request(
            left,
            &resp(&[&[b"EXISTS", alpha.as_bytes(), other.as_bytes()]])
        )
        .starts_with(b"-CROSSSLOT")
    );
    assert!(
        request(
            left,
            &resp(&[&[b"SMOVE", set_a.as_bytes(), other.as_bytes(), b"m"]])
        )
        .starts_with(b"-CROSSSLOT")
    );
    assert_eq!(
        request(right, &resp(&[&[b"GET", alpha.as_bytes()]])),
        b"$2\r\nva\r\n"
    );
    assert_eq!(
        request(
            left,
            &resp(&[
                &[b"MULTI"],
                &[b"MGET", alpha.as_bytes(), beta.as_bytes()],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n*1\r\n*2\r\n$2\r\nva\r\n$2\r\nvb\r\n"
    );
    assert_eq!(
        request(
            left,
            &resp(&[&[b"XADD", stream_a.as_bytes(), b"*", b"f", b"1"]])
        )
        .first()
        .copied(),
        Some(b'$')
    );
    assert_eq!(
        request(
            left,
            &resp(&[&[b"XADD", stream_b.as_bytes(), b"*", b"f", b"1"]])
        )
        .first()
        .copied(),
        Some(b'$')
    );
    let same_slot_read = request(
        left,
        &resp(&[&[
            b"XREAD",
            b"STREAMS",
            stream_a.as_bytes(),
            stream_b.as_bytes(),
            b"0",
            b"0",
        ]]),
    );

    assert!(
        same_slot_read
            .windows(stream_a.len())
            .any(|window| window == stream_a.as_bytes()),
        "{same_slot_read:?}"
    );
    assert!(
        same_slot_read
            .windows(stream_b.len())
            .any(|window| window == stream_b.as_bytes()),
        "{same_slot_read:?}"
    );
    assert!(
        request(
            left,
            &resp(&[&[
                b"XREAD",
                b"STREAMS",
                stream_a.as_bytes(),
                other.as_bytes(),
                b"0",
                b"0",
            ]])
        )
        .starts_with(b"-CROSSSLOT")
    );
    assert_eq!(
        request(
            left,
            &resp(&[&[b"FUNCTION", b"LOAD", b"echo", b"return 7"]])
        ),
        b"$4\r\necho\r\n"
    );
    assert_eq!(
        request(right, &resp(&[&[b"FCALL", b"echo", b"0"]])),
        b"-ERR function not found\r\n"
    );
    assert_eq!(
        request(left, &resp(&[&[b"FCALL", b"echo", b"0"]])),
        b":7\r\n"
    );

    assert_eq!(
        request(left, &resp(&[&[b"SET", watched.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    let mut watcher = StdTcpStream::connect(left).unwrap();

    watcher.set_read_timeout(Some(PATIENCE)).unwrap();
    watcher
        .write_all(&resp(&[&[b"WATCH", watched.as_bytes()]]))
        .unwrap();
    let mut watched_ok = [0u8; 5];

    watcher.read_exact(&mut watched_ok).unwrap();
    assert_eq!(&watched_ok, b"+OK\r\n");
    assert_eq!(
        request(left, &resp(&[&[b"SET", watched.as_bytes(), b"z"]])),
        b"+OK\r\n"
    );
    watcher
        .write_all(&resp(&[
            &[b"MULTI"],
            &[b"GET", watched.as_bytes()],
            &[b"EXEC"],
        ]))
        .unwrap();
    let aborted_reply = b"+OK\r\n+QUEUED\r\n*-1\r\n";
    let mut aborted = vec![0u8; aborted_reply.len()];

    watcher.read_exact(&mut aborted).unwrap();
    assert_eq!(aborted, aborted_reply);

    assert_eq!(
        request(right, &resp(&[&[b"SET", stable.as_bytes(), b"v"]])),
        b"+OK\r\n"
    );
    let mut peer = StdTcpStream::connect(right).unwrap();

    peer.set_read_timeout(Some(PATIENCE)).unwrap();
    peer.write_all(&resp(&[&[b"WATCH", stable.as_bytes()]]))
        .unwrap();
    peer.read_exact(&mut watched_ok).unwrap();
    assert_eq!(&watched_ok, b"+OK\r\n");
    assert_eq!(
        request(left, &resp(&[&[b"SET", watched.as_bytes(), b"again"]])),
        b"+OK\r\n"
    );
    peer.write_all(&resp(&[
        &[b"MULTI"],
        &[b"GET", stable.as_bytes()],
        &[b"EXEC"],
    ]))
    .unwrap();
    let committed_reply = b"+OK\r\n+QUEUED\r\n*1\r\n$1\r\nv\r\n";
    let mut committed = vec![0u8; committed_reply.len()];

    peer.read_exact(&mut committed).unwrap();
    assert_eq!(committed, committed_reply);

    assert!(request(left, &resp(&[&[b"SSUBSCRIBE", channel.as_bytes()]])).starts_with(b"-MOVED"));
    let mut subscriber = StdTcpStream::connect(right).unwrap();

    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    subscriber
        .write_all(&resp(&[&[b"SSUBSCRIBE", channel.as_bytes()]]))
        .unwrap();
    let expected = crate::pubsub::encode_ssubscribe(channel.as_bytes(), 1);
    let mut subscribed = vec![0u8; expected.len()];

    subscriber.read_exact(&mut subscribed).unwrap();
    assert_eq!(subscribed, expected);
    assert!(
        request(left, &resp(&[&[b"SPUBLISH", channel.as_bytes(), b"hello"]]))
            .starts_with(b"-MOVED")
    );
    subscriber
        .set_read_timeout(Some(Duration::from_millis(150)))
        .unwrap();
    let mut quiet = [0u8; 1];
    let missed = subscriber.read(&mut quiet).unwrap_err();

    assert!(
        matches!(missed.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock),
        "{missed}"
    );
    assert_eq!(
        request(
            right,
            &resp(&[&[b"SPUBLISH", channel.as_bytes(), b"hello"]])
        ),
        b":1\r\n"
    );
    subscriber.set_read_timeout(Some(PATIENCE)).unwrap();
    let expected = crate::pubsub::encode_smessage(channel.as_bytes(), b"hello");
    let mut message = vec![0u8; expected.len()];

    subscriber.read_exact(&mut message).unwrap();
    assert_eq!(message, expected);
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_bgsave_on_one_shard_publishes_a_round_for_every_shard() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    assert_eq!(
        request(
            right_addr,
            b"*3\r\n$3\r\nSET\r\n$8\r\n{left}:k\r\n$4\r\neast\r\n"
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        request(left_addr, b"*1\r\n$6\r\nBGSAVE\r\n"),
        b"+Background saving started\r\n"
    );
    assert!(wait_for(Duration::from_secs(10), || {
        directory.path().join("CURRENT").exists()
            && info_field(left_addr, "rdb_bgsave_in_progress") == Some(0)
            && info_field(right_addr, "rdb_bgsave_in_progress") == Some(0)
    }));
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (_, right_addr, stop, left_server, right_server) = serve_two_shards_with(|config| {
        config.persistence = persistence;
    });

    assert_eq!(
        request(right_addr, b"*2\r\n$3\r\nGET\r\n$8\r\n{left}:k\r\n"),
        b"$4\r\neast\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// `SAVE` on one shard while another shard's background checkpoint, begun before that
/// shard acknowledged a write, is running or already submitted: the published round must
/// hold the write, which without a WAL exists nowhere else.
#[test]
fn evented_save_on_one_shard_keeps_a_write_another_shard_made_during_its_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    // One batch on the right shard: its checkpoint begins between the two writes.
    assert_eq!(
        request(
            right_addr,
            &resp(&[
                &[b"SET", b"{left}:k", b"old"],
                &[b"BGSAVE"],
                &[b"SET", b"{left}:k", b"new"],
            ])
        ),
        b"+OK\r\n+Background saving started\r\n+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");

    // What SAVE published, before shutdown writes final checkpoints of its own.
    let saved = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), saved.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (_, right_addr, stop, left_server, right_server) = serve_two_shards_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: saved.path().to_path_buf(),
            ..persistence
        };
    });

    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", b"{left}:k"]])),
        b"$3\r\nnew\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_save_returns_while_save_and_bgsave_requests_keep_arriving() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 256 * 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });
    let tag = tag_on_shard(0);

    // Enough data on the left shard that its walk spans many slices: the right shard's file
    // is in the open round long before the left one's, while requests keep arriving.
    for batch in 0..20 {
        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|n| format!("{{{tag}}}:{batch}:{n}").into_bytes())
            .collect();
        let value = vec![b'v'; 256];
        let mut command: Vec<&[u8]> = vec![b"MSET"];

        for key in &keys {
            command.push(key);
            command.push(&value);
        }
        assert_eq!(request(left_addr, &resp(&[&command])), b"+OK\r\n");
    }

    let spamming = Arc::new(AtomicBool::new(true));
    let spammers: Vec<_> = [
        (left_addr, &b"BGSAVE"[..]),
        (right_addr, &b"BGSAVE"[..]),
        (left_addr, &b"SAVE"[..]),
        (right_addr, &b"SAVE"[..]),
    ]
    .into_iter()
    .map(|(address, command)| {
        let spamming = Arc::clone(&spamming);

        thread::spawn(move || {
            while spamming.load(Ordering::Acquire) {
                request_within(address, &resp(&[&[command]]), Duration::from_secs(150));
                thread::sleep(Duration::from_millis(20));
            }
        })
    })
    .collect();
    let started = Instant::now();

    for round in 0..2u32 {
        let value = round.to_string();

        assert_eq!(
            request(
                right_addr,
                &resp(&[&[b"SET", b"{left}:k", value.as_bytes()]])
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            request_within(left_addr, &resp(&[&[b"SAVE"]]), Duration::from_secs(150)),
            b"+OK\r\n",
            "SAVE {round} did not succeed"
        );
    }
    // Each save waits for at most the round under way and one more.
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "saves took {:?}",
        started.elapsed()
    );

    // What the last SAVE published, before shutdown writes final checkpoints of its own.
    let saved = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), saved.path());
    spamming.store(false, Ordering::Release);
    for spammer in spammers {
        join_within(spammer, PATIENCE).unwrap();
    }
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (_, right_addr, stop, left_server, right_server) = serve_two_shards_with(|config| {
        config.persistence = PersistenceConfig {
            data_dir: saved.path().to_path_buf(),
            ..persistence
        };
    });

    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", b"{left}:k"]])),
        b"$1\r\n1\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_partition_covers_a_shard_file_that_replaced_its_earlier_one() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"lock:*".to_vec()];
        });
    let set = |value: &[u8]| request(left_addr, &resp(&[&[b"SET", b"lock:foo", value]]));

    set(b"before");
    assert_eq!(request(left_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    // A hot-key write logged in the left shard's WAL, then a new file of that shard that
    // covers it (the replacement within one open round is covered by the coordinator test).
    set(b"after");
    assert_eq!(request(left_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    assert_eq!(request(right_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");

    // The published round must hold the write: the left shard's log no longer replays it.
    let loaded = CheckpointManager::new(persistence.clone(), ProtocolLimits::default())
        .load()
        .unwrap()
        .unwrap();
    let hot = &loaded.shards[crate::runtime::cluster::hot_partition_id(2) as usize];
    let value = hot
        .entries
        .iter()
        .find(|entry| entry.key == b"lock:foo")
        .map(|entry| entry.value.clone());
    assert_eq!(
        value,
        Some(crate::store::SnapshotValue::Bytes(b"after".to_vec()))
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_save_on_one_shard_publishes_the_round_while_the_others_are_idle() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    // Writes reach one shard only; the other one never has a reason to checkpoint.
    let owner = if crate::routing::hash_slot(b"{busy}") < 8192 {
        left_addr
    } else {
        right_addr
    };
    for n in 0..20 {
        let key = format!("{{busy}}:{n}");
        assert_eq!(
            request(owner, &resp(&[&[b"SET", key.as_bytes(), b"v"]])),
            b"+OK\r\n"
        );
    }
    assert_eq!(request(left_addr, b"*1\r\n$4\r\nSAVE\r\n"), b"+OK\r\n");
    assert!(
        directory.path().join("CURRENT").exists(),
        "SAVE answered before its round was published"
    );
    assert_eq!(info_field(left_addr, "rdb_bgsave_in_progress"), Some(0));
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_idle_shard_joins_a_round_a_busy_shard_opened() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_millis(200),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });
    let (busy, idle) = if crate::routing::hash_slot(b"{busy}") < 8192 {
        (left_addr, right_addr)
    } else {
        (right_addr, left_addr)
    };

    // Uneven writes: only one shard has mutations, so only it becomes due.
    for n in 0..20 {
        let key = format!("{{busy}}:{n}");
        assert_eq!(
            request(busy, &resp(&[&[b"SET", key.as_bytes(), b"v"]])),
            b"+OK\r\n"
        );
    }
    let published = wait_for(Duration::from_secs(10), || {
        directory.path().join("CURRENT").exists()
            && info_field(idle, "rdb_bgsave_in_progress") == Some(0)
    });
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
    assert!(published, "the periodic round never published");
}

#[test]
fn evented_two_shards_save_fails_at_once_when_a_checkpoint_cannot_be_written() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        // Smaller than one shard's data: its checkpoint file fails to write.
        snapshot_max_bytes: 4096,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });
    let value = vec![b'v'; 16 * 1024];

    for shard in 0..2 {
        let key = key_on_shard(shard, "full");
        let owner = if shard == 0 { left_addr } else { right_addr };

        assert_eq!(
            request(owner, &resp(&[&[b"SET", key.as_bytes(), &value]])),
            b"+OK\r\n"
        );
    }
    // Answered with the single-shard error before the client's read timeout, instead of
    // after the round's timeout.
    let started = Instant::now();

    assert_eq!(
        request(left_addr, &resp(&[&[b"SAVE"]])),
        b"-MISCONF persistence error\r\n"
    );
    // The round's own timeout is two minutes.
    assert!(started.elapsed() < Duration::from_secs(20));
    // A later SAVE fails the same way: its own request is attempted again.
    assert_eq!(
        request(right_addr, &resp(&[&[b"SAVE"]])),
        b"-MISCONF persistence error\r\n"
    );
    // Small enough again for the final checkpoints at shutdown.
    for shard in 0..2 {
        let key = key_on_shard(shard, "full");
        let owner = if shard == 0 { left_addr } else { right_addr };

        assert_eq!(
            request(owner, &resp(&[&[b"DEL", key.as_bytes()]])),
            b":1\r\n"
        );
    }
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_two_shards_back_to_back_saves_do_not_wait_out_the_retry_throttle() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, _right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    // Each SAVE is a new request: the other shard starts its file at once instead of after
    // the one-second throttle that spaces out retries of a failed one. A SAVE that waited it
    // out takes at least about a second however fast the disk is; the fastest of six tells
    // that floor apart from a slow disk, which only makes some SAVEs slower.
    let fastest = (0..6)
        .map(|_| {
            let started = Instant::now();

            assert_eq!(
                request_connected(left_addr, &resp(&[&[b"SAVE"]])),
                b"+OK\r\n"
            );
            started.elapsed()
        })
        .min()
        .unwrap();

    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
    assert!(
        fastest < Duration::from_millis(800),
        "the fastest of six idle SAVEs took {fastest:?}"
    );
}

#[test]
fn evented_exec_fails_when_queued_keys_span_shards() {
    let (left_addr, _right_addr, stop, left_server, right_server) = serve_two_shards();
    let local = key_on_shard(0, "tx");
    let remote = key_on_shard(1, "tx");
    let text = String::from_utf8(request(
        left_addr,
        &resp(&[
            &[b"MULTI"],
            &[b"SET", local.as_bytes(), b"1"],
            &[b"SET", remote.as_bytes(), b"1"],
            &[b"EXEC"],
            &[b"EXISTS", local.as_bytes()],
        ]),
    ))
    .unwrap();
    let replies: Vec<&str> = text.split_terminator("\r\n").collect();

    // Both SETs queue; EXEC refuses the transaction because its keys hash to different
    // slots, and nothing ran.
    assert_eq!(replies[..3], ["+OK", "+QUEUED", "+QUEUED"], "{text}");
    assert_eq!(
        replies[3..],
        [
            "-CROSSSLOT Keys in request do not hash to the same slot",
            ":0"
        ],
        "{text}"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

#[test]
fn evented_two_shards_save_during_an_open_round_does_not_wait_out_the_throttle() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Snapshot,
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, _right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
        });

    // Warm up: the first round of an idle pair publishes.
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let timed_save = || {
        let started = Instant::now();

        assert_eq!(
            request_connected(left_addr, &resp(&[&[b"SAVE"]])),
            b"+OK\r\n"
        );
        started.elapsed()
    };
    // One round on this machine's disk: the fastest of a few SAVEs with no round open.
    let round = (0..3).map(|_| timed_save()).min().unwrap();

    // Each SAVE arrives while the round its BGSAVE opened is still waiting for the other
    // shard. This shard's file is in that round, so it holds back the SAVE's checkpoint;
    // once the round publishes, that checkpoint starts at once, so the SAVE takes about two
    // rounds. One that waited out the one-second throttle takes at least a second plus its
    // own round. The fastest of six tells the two apart; a slow disk only makes some slower.
    let fastest = (0..6)
        .map(|_| {
            let replies = request(left_addr, &resp(&[&[b"BGSAVE"]]));

            assert!(replies.starts_with(b"+"), "{replies:?}");
            timed_save()
        })
        .min()
        .unwrap();

    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
    assert!(
        fastest < Duration::from_secs(1) + round,
        "the fastest of six SAVEs after a BGSAVE took {fastest:?} (one round takes {round:?})"
    );
}

/// Under `always`, a shard whose WAL sync failed takes no new checkpoint of memory (it may
/// hold refused writes). The other shard's rounds must still publish and compact its log:
/// the failed shard hands each round a copy of its last published file.
#[test]
fn rounds_still_publish_after_one_shard_wal_sync_failed() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_millis(50),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.persistence = persistence.clone());
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");
    let set = |address, key: &str, value: &str| {
        request(
            address,
            &resp(&[&[b"SET", key.as_bytes(), value.as_bytes()]]),
        )
    };

    assert_eq!(set(left_addr, &left, "a"), b"+OK\r\n");
    assert_eq!(set(right_addr, &right, "old"), b"+OK\r\n");
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");

    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), 1));
    let refused = set(right_addr, &right, "refused");
    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    assert!(!refused.starts_with(b"+"), "{refused:?}");

    let failed_at = published_generation(&persistence);
    let deadline = Instant::now() + PATIENCE;
    let mut last = String::new();

    for n in 0.. {
        last = format!("b{n}");
        assert_eq!(set(left_addr, &left, &last), b"+OK\r\n");
        if published_generation(&persistence) > failed_at + 2 || Instant::now() > deadline {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        published_generation(&persistence) > failed_at + 2,
        "no round published after shard 1's WAL failed (generation {failed_at})"
    );
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    // The failed shard ends with an error: its log failed.
    let _ = join_within(right_server, PATIENCE);
    assert_no_orphan_round(&persistence);

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: killed.path().to_path_buf(),
                ..persistence.clone()
            };
        });

    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", left.as_bytes()]])),
        format!("${}\r\n{last}\r\n", last.len()).into_bytes()
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", right.as_bytes()]])),
        b"$3\r\nold\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// Fails every WAL sync of `shard` in `directory` while `run` runs.
fn with_failing_shard<T>(directory: &std::path::Path, shard: u32, run: impl FnOnce() -> T) -> T {
    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .push((directory.to_path_buf(), shard));
    let result = run();

    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory);
    result
}

fn published_generation(persistence: &PersistenceConfig) -> u64 {
    let manager = CheckpointManager::new(persistence.clone(), ProtocolLimits::default());

    // A round published while this reads can remove the files it was reading.
    for _ in 0..20 {
        if let Ok(loaded) = manager.load() {
            return loaded.map_or(0, |checkpoint| checkpoint.generation);
        }
        thread::sleep(Duration::from_millis(10));
    }
    manager
        .load()
        .unwrap()
        .map_or(0, |checkpoint| checkpoint.generation)
}

/// No round directory is left newer than the published one.
fn assert_no_orphan_round(persistence: &PersistenceConfig) {
    let published = published_generation(persistence);
    let orphans: Vec<u64> = std::fs::read_dir(persistence.data_dir.join("checkpoints"))
        .unwrap()
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
        .filter(|generation| *generation > published)
        .collect();

    assert!(
        orphans.is_empty(),
        "rounds {orphans:?} after published {published}"
    );
}

/// The copy of the last published file a shard with a failed log hands each round can be
/// as large as the shard: it is made off the shard's thread, which keeps answering.
#[test]
fn a_shard_whose_log_failed_keeps_answering_while_its_file_is_copied() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_millis(50),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.persistence = persistence.clone());
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");

    assert_eq!(
        request(left_addr, &resp(&[&[b"SET", left.as_bytes(), b"a"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    crate::persistence::CARRY_DELAYS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), Duration::from_secs(5)));
    let refused = with_failing_shard(directory.path(), 1, || {
        request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"new"]]))
    });
    assert!(!refused.starts_with(b"+"), "{refused:?}");

    let started = Instant::now();
    let mut slowest = Duration::ZERO;

    while started.elapsed() < Duration::from_secs(4) {
        request(left_addr, &resp(&[&[b"SET", left.as_bytes(), b"b"]]));
        let asked = Instant::now();
        let reply = request_within(
            right_addr,
            &resp(&[&[b"GET", right.as_bytes()]]),
            Duration::from_secs(10),
        );

        slowest = slowest.max(asked.elapsed());
        assert_eq!(reply, b"$3\r\nnew\r\n");
        thread::sleep(Duration::from_millis(50));
    }
    crate::persistence::CARRY_DELAYS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    let _ = join_within(right_server, PATIENCE);

    // A shard that waited for the copy stalls for most of its 5 s.
    assert!(
        slowest < Duration::from_millis(2500),
        "the shard stalled {slowest:?} while its file was copied"
    );
}

/// A hot-key write the failed shard refused stays in the shared hot store: no round may
/// publish a hot partition holding it.
#[test]
fn a_hot_key_write_refused_by_a_failed_log_is_not_published() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_millis(50),
        ..PersistenceConfig::default()
    };
    let configure = |config: &mut AppConfig| {
        config.persistence = persistence.clone();
        config.hot_keys = vec![b"hot*".to_vec()];
    };
    let (left_addr, right_addr, stop, left_server, right_server) = serve_two_shards_with(configure);
    let left = key_on_shard(0, "left");

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", b"hot", b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let refused = with_failing_shard(directory.path(), 1, || {
        request(right_addr, &resp(&[&[b"SET", b"hot", b"refused"]]))
    });
    assert!(!refused.starts_with(b"+"), "{refused:?}");

    // Rounds the healthy shard starts; the failed shard joins them once a second.
    for _ in 0..40 {
        request(left_addr, &resp(&[&[b"SET", left.as_bytes(), b"b"]]));
        thread::sleep(Duration::from_millis(50));
    }
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    let _ = join_within(left_server, PATIENCE);
    let _ = join_within(right_server, PATIENCE);

    let (left_addr, _right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            configure(config);
            config.persistence.data_dir = killed.path().to_path_buf();
        });

    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", b"hot"]])),
        b"$3\r\nold\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// With no round ever published, a shard whose log failed still lets rounds publish: its
/// log was never compacted, so it hands an empty file and replays the whole log.
#[test]
fn rounds_publish_after_a_shard_log_failed_before_any_round() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.persistence = persistence.clone());
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"kept"]])),
        b"+OK\r\n"
    );
    let refused = with_failing_shard(directory.path(), 1, || {
        request(
            right_addr,
            &resp(&[&[b"SET", right.as_bytes(), b"refused"]]),
        )
    });
    assert!(!refused.starts_with(b"+"), "{refused:?}");
    assert_eq!(published_generation(&persistence), 0);

    let deadline = Instant::now() + PATIENCE;

    while published_generation(&persistence) == 0 && Instant::now() < deadline {
        request(left_addr, &resp(&[&[b"SET", left.as_bytes(), b"b"]]));
        request(left_addr, &resp(&[&[b"BGSAVE"]]));
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        published_generation(&persistence) > 0,
        "no round published after shard 1's log failed"
    );
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    let _ = join_within(right_server, PATIENCE);
    assert_no_orphan_round(&persistence);

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: killed.path().to_path_buf(),
                ..persistence.clone()
            };
        });

    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", left.as_bytes()]])),
        b"$1\r\nb\r\n"
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", right.as_bytes()]])),
        b"$4\r\nkept\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// Fails the next `count` WAL writes of `shard` in `directory`; the log resumes by itself
/// once they are used up.
fn fail_shard_writes(directory: &std::path::Path, shard: u32, count: u32) {
    crate::persistence::FAILING_WRITE_SHARDS
        .lock()
        .unwrap()
        .push((directory.to_path_buf(), shard, count));
}

fn stop_failing_writes(directory: &std::path::Path) {
    crate::persistence::FAILING_WRITE_SHARDS
        .lock()
        .unwrap()
        .retain(|(dir, _, _)| dir != directory);
}

/// Waits until a round past `generation` is published, poking the healthy shard.
fn wait_for_round_after(
    persistence: &PersistenceConfig,
    generation: u64,
    address: SocketAddr,
    key: &str,
) {
    let deadline = Instant::now() + PATIENCE;

    while published_generation(persistence) <= generation && Instant::now() < deadline {
        request(address, &resp(&[&[b"SET", key.as_bytes(), b"poke"]]));
        request(address, &resp(&[&[b"BGSAVE"]]));
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        published_generation(persistence) > generation,
        "no round published after {generation}"
    );
}

/// A WAL write failure that resumes by itself while the shard's carried copy is still being
/// written: a `BGSAVE` landing meanwhile must not start a newer file that the late copy then
/// replaces, or the published round would hold the older file while the log is compacted
/// past it.
#[test]
fn a_bgsave_during_a_slow_carry_does_not_lose_writes_made_after_the_log_resumed() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.persistence = persistence.clone());
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let saved = published_generation(&persistence);

    crate::persistence::CARRY_DELAYS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), Duration::from_secs(2)));
    fail_shard_writes(directory.path(), 1, 1);
    let refused = request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"lost"]]));
    assert!(!refused.starts_with(b"+"), "{refused:?}");

    // The left shard opens a round; the right one, whose log failed, starts its copy.
    assert_eq!(request(left_addr, &resp(&[&[b"BGSAVE"]]))[0], b'+');
    let carrying = request(right_addr, &resp(&[&[b"BGSAVE"]]));
    assert!(!carrying.starts_with(b"+"), "{carrying:?}");

    // The log resumes and takes a write past the copied file's snapshot point.
    let deadline = Instant::now() + PATIENCE;
    let mut accepted = false;

    while !accepted && Instant::now() < deadline {
        accepted = request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"new"]])) == b"+OK\r\n";
        thread::sleep(Duration::from_millis(50));
    }
    assert!(accepted, "the right shard's log did not resume");
    // The copy is still being written: this save waits for it instead of racing it.
    request(right_addr, &resp(&[&[b"BGSAVE"]]));

    wait_for_round_after(&persistence, saved, left_addr, &left);
    let published = published_generation(&persistence);
    // One more round, taken after the copy: the right shard's log is compacted then.
    wait_for_round_after(&persistence, published, left_addr, &left);
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    crate::persistence::CARRY_DELAYS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    stop_failing_writes(directory.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    let _ = join_within(right_server, PATIENCE);

    let (_left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: killed.path().to_path_buf(),
                ..persistence.clone()
            };
        });

    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", right.as_bytes()]])),
        b"$3\r\nnew\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// A hot-key write on a shard whose log is failed is refused before it reaches the shared
/// hot store, and rounds with a hot partition publish again once the log resumes.
#[test]
fn a_hot_key_write_on_a_failed_log_is_refused_before_it_is_applied() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"hot*".to_vec()];
        });
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", b"hot", b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let saved = published_generation(&persistence);

    // The first failed write fails the log; the second fails its first attempt to resume.
    fail_shard_writes(directory.path(), 1, 2);
    let refused = request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"x"]]));
    assert!(!refused.starts_with(b"+"), "{refused:?}");
    let refused = request(right_addr, &resp(&[&[b"SET", b"hot", b"refused"]]));
    assert!(!refused.starts_with(b"+"), "{refused:?}");
    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", b"hot"]])),
        b"$3\r\nold\r\n"
    );

    // The log resumes (recovery is tried at most twice a second) and rounds publish.
    let deadline = Instant::now() + PATIENCE;
    let mut accepted = false;

    while !accepted && Instant::now() < deadline {
        accepted = request(right_addr, &resp(&[&[b"SET", b"hot", b"new"]])) == b"+OK\r\n";
        thread::sleep(Duration::from_millis(100));
    }
    assert!(accepted, "the right shard's log did not resume");
    wait_for_round_after(&persistence, saved, left_addr, &left);
    stop_failing_writes(directory.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// A shard whose log sync failed for writes that were not hot-key writes does not keep the
/// hot partition, and with it every round, out until restart.
#[test]
fn rounds_with_a_hot_partition_publish_after_a_sync_failure_of_plain_writes() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let configure = |config: &mut AppConfig| {
        config.persistence = persistence.clone();
        config.hot_keys = vec![b"hot*".to_vec()];
    };
    let (left_addr, right_addr, stop, left_server, right_server) = serve_two_shards_with(configure);
    let left = key_on_shard(0, "left");
    let right = key_on_shard(1, "right");

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", b"hot", b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", right.as_bytes(), b"old"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let saved = published_generation(&persistence);
    let refused = with_failing_shard(directory.path(), 1, || {
        request(
            right_addr,
            &resp(&[&[b"SET", right.as_bytes(), b"refused"]]),
        )
    });
    assert!(!refused.starts_with(b"+"), "{refused:?}");
    assert_eq!(
        request(left_addr, &resp(&[&[b"SET", b"hot", b"new"]])),
        b"+OK\r\n"
    );

    wait_for_round_after(&persistence, saved, left_addr, &left);
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    let _ = join_within(right_server, PATIENCE);

    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            configure(config);
            config.persistence.data_dir = killed.path().to_path_buf();
        });

    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", b"hot"]])),
        b"$3\r\nnew\r\n"
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", right.as_bytes()]])),
        b"$3\r\nold\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// Waits until no shard is writing or owes a checkpoint.
fn wait_for_idle_checkpoints(addresses: &[SocketAddr]) {
    let deadline = Instant::now() + PATIENCE;
    let idle = b"rdb_bgsave_in_progress:0";

    for &address in addresses {
        while !request(address, &resp(&[&[b"INFO"]]))
            .windows(idle.len())
            .any(|window| window == idle)
        {
            assert!(Instant::now() < deadline, "a checkpoint did not finish");
            thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Fails the next round commit of `directory` and waits until it was tried. The left
/// shard's file is held back meanwhile, so the right shard submits first (and notes its
/// submission) and the left shard's submission is the commit that fails.
/// Starts a BGSAVE, retrying while an earlier one or a carried copy is still running (a
/// loaded test machine can still be finishing the previous round).
fn start_bgsave(address: SocketAddr) {
    let deadline = Instant::now() + PATIENCE;

    loop {
        let reply = request(address, &resp(&[&[b"BGSAVE"]]));
        if reply[0] == b'+' {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "BGSAVE did not start: {}",
            String::from_utf8_lossy(&reply)
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Round commits in a data directory keep failing while this lives (see [`fail_commits`]).
struct FailingCommits(std::path::PathBuf);

impl Drop for FailingCommits {
    fn drop(&mut self) {
        crate::persistence::FAILING_COMMITS
            .lock()
            .unwrap()
            .retain(|(dir, _)| *dir != self.0);
        crate::persistence::SHARD_FILE_DELAYS
            .lock()
            .unwrap()
            .retain(|(dir, _, _)| *dir != self.0);
    }
}

/// Fails every round commit from `start` on, and returns once one was tried. The commits
/// keep failing until the guard drops: a round retried after the failed one cannot publish
/// while the test still checks that nothing did.
fn fail_commits(directory: &std::path::Path, start: impl FnOnce()) -> FailingCommits {
    let guard = FailingCommits(directory.to_path_buf());

    crate::persistence::SHARD_FILE_DELAYS.lock().unwrap().push((
        directory.to_path_buf(),
        0,
        Duration::from_millis(500),
    ));
    crate::persistence::FAILING_COMMITS
        .lock()
        .unwrap()
        .push((directory.to_path_buf(), u32::MAX));
    start();
    wait_until("a round commit to be tried", || {
        crate::persistence::FAILING_COMMITS
            .lock()
            .unwrap()
            .iter()
            .any(|(dir, left)| dir == directory && *left < u32::MAX)
    });
    guard
}

/// A round whose commit failed never publishes, but the shard's file submitted to it was
/// noted. Once a later round holding the shard's carried copy (older than that file)
/// publishes, the shard's next submission must compact its log only to what the copy
/// covers: recovery loads the copy and replays the rest of the log. Small WAL segments
/// put the writes between the two files in sealed segments a wrong compaction removes.
#[test]
fn checkpoint_failed_round_commit_does_not_compact_the_log_past_a_later_carried_copy() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        wal_segment_bytes: 4096,
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| config.persistence = persistence.clone());
    let left = key_on_shard(0, "left");
    let old = key_on_shard(1, "old");
    let kept = key_on_shard(1, "kept");
    let after = key_on_shard(1, "after");
    let filler = vec![b'f'; 3000];

    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", old.as_bytes(), b"a"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(left_addr, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    let saved = published_generation(&persistence);

    // Written after the published file, then sealed in by later segments.
    assert_eq!(
        request(right_addr, &resp(&[&[b"SET", kept.as_bytes(), b"b"]])),
        b"+OK\r\n"
    );
    for index in 0..4 {
        let key = key_on_shard(1, &format!("filler{index}"));

        assert_eq!(
            request(right_addr, &resp(&[&[b"SET", key.as_bytes(), &filler]])),
            b"+OK\r\n"
        );
    }
    let failing = fail_commits(directory.path(), || {
        start_bgsave(right_addr);
    });
    assert_eq!(published_generation(&persistence), saved);
    drop(failing);

    // The right shard's log fails, and stays failed until a round publishes: it carries
    // the file published before the failed round.
    fail_shard_writes(directory.path(), 1, u32::MAX);
    let refused = request(right_addr, &resp(&[&[b"SET", after.as_bytes(), b"x"]]));
    assert!(!refused.starts_with(b"+"), "{refused:?}");
    wait_for_round_after(&persistence, saved, left_addr, &left);
    // A BGSAVE the wait sent after the round it saw may still be running: let it finish, or
    // it can publish after `published` is read or take the failing commit meant below.
    wait_for_idle_checkpoints(&[left_addr, right_addr]);
    let published = published_generation(&persistence);

    // The log resumes; the right shard's next file goes into a round that fails again.
    // Commits fail from before the log resumes: the right shard still owes the snapshot
    // requests the wait's BGSAVEs raised (its carried copies answer none), and starts a
    // round for them on its own within a second of resuming, which must not publish.
    let failing = fail_commits(directory.path(), || {
        stop_failing_writes(directory.path());
        let deadline = Instant::now() + PATIENCE;
        let mut accepted = false;

        while !accepted && Instant::now() < deadline {
            accepted =
                request(right_addr, &resp(&[&[b"SET", after.as_bytes(), b"y"]])) == b"+OK\r\n";
            thread::sleep(Duration::from_millis(50));
        }
        assert!(accepted, "the right shard's log did not resume");
        start_bgsave(right_addr);
    });
    assert_eq!(published_generation(&persistence), published);

    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    drop(failing);
    stop_failing_writes(directory.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    let _ = join_within(right_server, PATIENCE);

    let (_left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = PersistenceConfig {
                data_dir: killed.path().to_path_buf(),
                ..persistence.clone()
            };
        });

    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", kept.as_bytes()]])),
        b"$1\r\nb\r\n"
    );
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", after.as_bytes()]])),
        b"$1\r\ny\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// A hot-key SET keeps the deadline of every expiry form through the log: EXAT and PXAT
/// set one, KEEPTTL leaves the old one in place.
#[test]
fn hot_key_set_keeps_absolute_and_kept_deadlines_through_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let start = |persistence: PersistenceConfig| {
        serve_two_shards_with(move |config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"lock:*".to_vec()];
        })
    };
    let soon = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 400)
        .to_string();
    let frame = |args: &[&[u8]]| {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();

        for arg in args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        out
    };
    let mut writes = Vec::new();

    writes.extend(frame(&[b"SET", b"lock:at", b"v", b"PXAT", soon.as_bytes()]));
    writes.extend(frame(&[b"SET", b"lock:kept", b"v", b"PX", b"400"]));
    writes.extend(frame(&[b"SET", b"lock:kept", b"w", b"KEEPTTL"]));
    writes.extend(frame(&[b"SET", b"lock:past", b"v", b"EXAT", b"1"]));
    writes.extend(frame(&[b"GET", b"lock:kept"]));
    writes.extend(frame(&[b"GET", b"lock:past"]));

    let (address, _, stop, left, right) = start(persistence.clone());

    assert_eq!(
        request(address, &writes),
        b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n$1\r\nw\r\n$-1\r\n"
    );
    stop.request();
    join_within(left, PATIENCE).unwrap();
    join_within(right, PATIENCE).unwrap();
    thread::sleep(Duration::from_millis(500));

    let (address, _, stop, left, right) = start(persistence);
    let mut reads = frame(&[b"GET", b"lock:at"]);

    reads.extend(frame(&[b"GET", b"lock:kept"]));
    assert_eq!(request(address, &reads), b"$-1\r\n$-1\r\n");
    stop.request();
    join_within(left, PATIENCE).unwrap();
    join_within(right, PATIENCE).unwrap();
}

/// A hot-key write whose WAL record is refused never reaches the shared store: the writer
/// gets an error and the other shard still reads the old value (issue #52). Refused here:
/// a record too large for a segment, and every write while the log is failed.
#[test]
fn a_refused_hot_key_write_leaves_the_old_value_for_every_shard() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 64 * 1024,
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (left_addr, right_addr, stop, left_server, right_server) =
        serve_two_shards_with(|config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"lock:*".to_vec()];
        });
    let old = b"$3\r\nold\r\n";
    let large = vec![b'x'; 128 * 1024];

    assert_eq!(
        request(left_addr, &resp(&[&[b"SET", b"lock:a", b"old"]])),
        b"+OK\r\n"
    );
    let oversized = request(left_addr, &resp(&[&[b"SET", b"lock:a", &large]]));

    assert!(oversized.starts_with(b"-"), "{oversized:?}");
    assert_eq!(request(right_addr, &resp(&[&[b"GET", b"lock:a"]])), old);

    // The log fails on another key's write and cannot resume while writes keep failing.
    fail_shard_writes(directory.path(), 0, u32::MAX);
    let failed = request(left_addr, &resp(&[&[b"SET", b"lock:b", b"x"]]));

    assert!(failed.starts_with(b"-MISCONF"), "{failed:?}");
    for refused in [
        &[&b"SET"[..], b"lock:a", b"new"][..],
        &[b"DEL", b"lock:a"],
        &[b"FLUSHALL"],
    ] {
        let reply = request(left_addr, &resp(&[refused]));

        assert!(reply.starts_with(b"-MISCONF"), "{refused:?}: {reply:?}");
        assert_eq!(
            request(right_addr, &resp(&[&[b"GET", b"lock:a"]])),
            old,
            "{refused:?}"
        );
    }
    stop_failing_writes(directory.path());

    // The log resumes (after its retry pause) and takes the next write.
    let deadline = Instant::now() + PATIENCE;

    while request(left_addr, &resp(&[&[b"SET", b"lock:a", b"new"]])) != b"+OK\r\n" {
        assert!(Instant::now() < deadline, "the log did not resume");
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", b"lock:a"]])),
        b"$3\r\nnew\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// A hot-key write whose record was taken but whose log write then failed is in the shared
/// store, as a grouped shard write is in its shard: it stays readable, its record is kept,
/// and the log writes it first when it resumes, so it survives a crash after that.
#[test]
fn a_hot_key_write_whose_log_write_failed_is_written_when_the_log_resumes() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let start = |persistence: PersistenceConfig| {
        serve_two_shards_with(move |config| {
            config.persistence = persistence.clone();
            config.hot_keys = vec![b"lock:*".to_vec()];
        })
    };
    let (left_addr, right_addr, stop, left_server, right_server) = start(persistence.clone());

    fail_shard_writes(directory.path(), 0, 1);
    let failed = request(left_addr, &resp(&[&[b"SET", b"lock:a", b"kept"]]));

    stop_failing_writes(directory.path());
    assert!(failed.starts_with(b"-MISCONF"), "{failed:?}");
    assert_eq!(
        request(right_addr, &resp(&[&[b"GET", b"lock:a"]])),
        b"$4\r\nkept\r\n"
    );
    let deadline = Instant::now() + PATIENCE;

    while request(left_addr, &resp(&[&[b"SET", b"lock:poke", b"x"]])) != b"+OK\r\n" {
        assert!(Instant::now() < deadline, "the log did not resume");
        thread::sleep(Duration::from_millis(50));
    }
    // A copy of the data directory now, as a crash would leave it: no checkpoint since.
    let killed = tempfile::tempdir().unwrap();

    copy_dir(directory.path(), killed.path());
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();

    let (left_addr, _, stop, left_server, right_server) = start(PersistenceConfig {
        data_dir: killed.path().to_path_buf(),
        ..persistence
    });

    assert_eq!(
        request(left_addr, &resp(&[&[b"GET", b"lock:a"]])),
        b"$4\r\nkept\r\n"
    );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// One `SCAN` reply on a connection that stays open: a forwarded command answers only a
/// client that is still connected.
fn scan_page(address: SocketAddr, args: &[&[u8]]) -> (String, Vec<String>) {
    use std::io::{BufRead, BufReader, Read};

    let mut stream = StdTcpStream::connect(address).unwrap();

    stream.set_read_timeout(Some(PATIENCE)).unwrap();
    stream.write_all(&resp(&[args])).unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let bulk = |reader: &mut BufReader<StdTcpStream>| {
        let mut header = String::new();

        reader.read_line(&mut header).unwrap();
        let length: usize = header.trim_end()[1..].parse().unwrap();
        let mut body = vec![0; length + 2];

        reader.read_exact(&mut body).unwrap();
        String::from_utf8(body[..length].to_vec()).unwrap()
    };

    reader.read_line(&mut line).unwrap();
    assert_eq!(line, "*2\r\n");
    let cursor = bulk(&mut reader);

    line.clear();
    reader.read_line(&mut line).unwrap();
    let count: usize = line.trim_end()[1..].parse().unwrap();
    let keys = (0..count).map(|_| bulk(&mut reader)).collect();

    (cursor, keys)
}

#[test]
fn evented_scan_with_a_slot_runs_on_the_owner_from_either_shard() {
    use crate::routing::hash_slot;

    let (left_addr, right_addr, stop, left_server, right_server) = serve_two_shards();
    let slot = hash_slot(b"tag").to_string();

    for key in ["{tag}a", "{tag}b", "other"] {
        let set = resp(&[&[b"SET", key.as_bytes(), b"v"]]);

        assert_eq!(request(left_addr, &set), b"+OK\r\n");
    }

    // One of the two shards owns the slot; the other forwards the command to it.
    for address in [left_addr, right_addr] {
        let mut cursor = String::from("0");
        let mut keys = Vec::new();

        loop {
            let (next, page) = scan_page(
                address,
                &[
                    b"SCAN",
                    cursor.as_bytes(),
                    b"COUNT",
                    b"1000",
                    b"SLOT",
                    slot.as_bytes(),
                ],
            );

            keys.extend(page);
            cursor = next;
            if cursor == "0" {
                break;
            }
        }
        keys.sort();
        assert_eq!(keys, ["{tag}a", "{tag}b"], "from {address}");
    }

    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}
