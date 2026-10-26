use super::*;
use crate::persistence::ShardCheckpoint;

#[test]
fn evented_skewed_replica_promotion_keeps_expired_deadlines_expired() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    // Held until the promoted replica has stopped: it listens on the same address.
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let floor = 4_000_000_000_000;
    let persistence = |directory: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let primary_persistence = persistence(primary_dir.path());
    let replica_persistence = persistence(replica_dir.path());
    let primary_clock =
        CheckpointManager::new(primary_persistence.clone(), ProtocolLimits::default());
    let replica_clock =
        CheckpointManager::new(replica_persistence.clone(), ProtocolLimits::default());
    primary_clock.persist_clock(UnixMillis::new(floor)).unwrap();
    replica_clock
        .persist_clock(UnixMillis::new(floor + 500))
        .unwrap();

    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = primary_persistence;
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = replica_persistence.clone();
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });
    // One batch reads the clock once: the GET sees the value however slow the runner is,
    // where a later request could come after the 250 ms.
    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[b"SET", b"near", b"value", b"PX", b"250"],
                &[b"GET", b"near"]
            ])
        ),
        b"+OK\r\n$5\r\nvalue\r\n"
    );
    // The value and its deadline travel as one record.
    wait_for_info(replica_address, "wal_last_sequence", |sequence| {
        sequence >= 1
    });
    assert_eq!(
        request(replica_address, &resp(&[&[b"GET", b"near"]])),
        b"$-1\r\n"
    );
    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();

    replica_clock.persist_clock(UnixMillis::new(floor)).unwrap();
    assert_eq!(
        replica_clock.load_clock().unwrap(),
        Some(UnixMillis::new(floor + 500))
    );
    let (promoted_address, promoted_stop, promoted) = serve_with(|config| {
        config.persistence = replica_persistence;
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    assert_eq!(
        request(promoted_address, &resp(&[&[b"GET", b"near"]])),
        b"$-1\r\n"
    );
    promoted_stop.request();
    join_within(promoted, PATIENCE).unwrap();
}

#[test]
fn evented_replica_installs_a_compacted_checkpoint() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let primary_persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: primary_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = primary_persistence;
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });

    assert_eq!(
            request(
                primary_address,
                b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nkept\r\n*3\r\n$3\r\nSET\r\n$5\r\nother\r\n$4\r\nsnap\r\n*1\r\n$4\r\nSAVE\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nnext\r\n"
            ),
            b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n"
        );

    let snapshot = std::fs::read(
        primary_dir
            .path()
            .join("checkpoints/00000000000000000001/shard-0000.snap"),
    )
    .unwrap();
    let checkpoint_sequence = u64::from_le_bytes(snapshot[32..40].try_into().unwrap());
    let shard_dir = primary_dir.path().join("wal").join("shard-0000");

    for entry in std::fs::read_dir(&shard_dir).unwrap() {
        let path = entry.unwrap().path();
        let first_sequence = path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap();

        if first_sequence <= checkpoint_sequence {
            std::fs::remove_file(path).unwrap();
        }
    }

    let replica_persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: replica_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = replica_persistence;
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });

    wait_until(
        "the replica to install the checkpoint and the following SET",
        || {
            let key = request(replica_address, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
            let other = request(replica_address, b"*2\r\n$3\r\nGET\r\n$5\r\nother\r\n");

            other == b"$4\r\nsnap\r\n" && key == b"$4\r\nnext\r\n"
        },
    );

    assert_eq!(
        crate::replication::upstream_history(replica_dir.path(), 0),
        crate::replication::load_or_create_history(primary_dir.path()).unwrap()
    );

    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();
}

#[test]
fn replica_replays_json_root_then_path_and_ttl_in_one_transaction() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let persistence = |path: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });
    let document = serde_json::to_vec(&serde_json::json!({
        "padding": "x".repeat(crate::json::TREE_THRESHOLD), "field": 0
    }))
    .unwrap();
    let expiry = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 100_000)
        .to_string();
    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[b"MULTI"],
                &[b"JSON.SET", b"{doc}", b"$", &document],
                &[b"JSON.SET", b"{doc}", b"$.field", b"1"],
                &[b"PEXPIREAT", b"{doc}", expiry.as_bytes()],
                &[b"PERSIST", b"{doc}"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*4\r\n+OK\r\n+OK\r\n:1\r\n:1\r\n"
    );
    wait_until("the replica to apply the transaction", || {
        request(
            replica_address,
            &resp(&[&[b"JSON.GET", b"{doc}", b"$.field"]]),
        ) == b"$1\r\n1\r\n"
    });
    assert!(
        CheckpointManager::new(persistence(replica_dir.path()), Default::default())
            .load()
            .unwrap()
            .is_none(),
        "valid transaction must not request a snapshot"
    );
    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();
}

#[test]
fn incomplete_replica_blocks_reads_and_requires_force_on_every_writer_restart() {
    let directory = tempfile::tempdir().unwrap();
    // The writer restarts take ports of their own: the replica's freed one may be reused.
    let ports = crate::test_ports::reserve();
    let mut config = AppConfig::default();
    config.persistence.profile = crate::DurabilityProfile::Everysec;
    config.persistence.data_dir = directory.path().to_path_buf();
    config.address = "127.0.0.1:0".into();
    replica_guard::write(directory.path(), 0, CatchupMarker::Preparing).unwrap();

    let (address, stop, server) = serve_with(|replica| {
        replica.persistence = config.persistence.clone();
        replica.replication.role = crate::config::ReplicationRole::Replica;
        replica.replication.primary = Some("127.0.0.1:1".parse().unwrap());
    });
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"key"]])),
        b"-LOADING replica catching up\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    config.address = ports.address_at(0).to_string();
    let address = ports.address_at(0);

    let error = run_with_force(&config, stopped(), false).unwrap_err();
    assert!(error.to_string().contains("--force-promote-incomplete"));
    let stop = ShutdownToken::default();
    let server_stop = stop.clone();
    let writer = config.clone();
    let thread = thread::spawn(move || run_with_force(&writer, server_stop, true).unwrap());
    wait_until_listening(address, &thread);
    assert!(!request(address, &resp(&[&[b"GET", b"key"]])).is_empty());
    stop.request();
    join_within(thread, PATIENCE).unwrap();
    assert_eq!(
        replica_guard::read(directory.path(), 0).unwrap(),
        Some(CatchupMarker::Preparing)
    );
    assert!(run_with_force(&config, stopped(), false).is_err());

    let multi = tempfile::tempdir().unwrap();
    config.persistence.data_dir = multi.path().to_path_buf();
    // Refused before it binds; the ports are reserved should it bind after all.
    config.address = ports.address_at(2).to_string();
    config.shards = 2;
    replica_guard::write(multi.path(), 1, CatchupMarker::Preparing).unwrap();
    assert!(
        run_with_force(&config, stopped(), false)
            .unwrap_err()
            .to_string()
            .contains("--force-promote-incomplete")
    );
}

#[test]
fn restart_only_clears_installed_marker_when_checkpoint_and_wal_prove_catchup() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let checkpoint = ShardCheckpoint {
        shard_id: 0,
        last_sequence: 0,
        entries: Vec::new(),
    };
    CheckpointManager::new(persistence.clone(), Default::default())
        .publish(1, &[checkpoint])
        .unwrap();
    let marker = CatchupMarker::Installed {
        generation: 1,
        snapshot: 0,
        through: 1,
    };
    replica_guard::write(directory.path(), 0, marker).unwrap();
    let start = || {
        serve_with(|config| {
            config.persistence = persistence.clone();
            config.replication.role = crate::config::ReplicationRole::Replica;
            config.replication.primary = Some("127.0.0.1:1".parse().unwrap());
        })
    };
    let (address, stop, server) = start();
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"key"]])),
        b"-LOADING replica catching up\r\n"
    );
    assert_eq!(
        replica_guard::read(directory.path(), 0).unwrap(),
        Some(marker)
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let complete = CatchupMarker::Installed {
        generation: 1,
        snapshot: 0,
        through: 0,
    };
    replica_guard::write(directory.path(), 0, complete).unwrap();
    let (address, stop, server) = start();
    assert_eq!(request(address, &resp(&[&[b"GET", b"key"]])), b"$-1\r\n");
    assert_eq!(replica_guard::read(directory.path(), 0).unwrap(), None);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn failed_wal_replay_keeps_installed_catchup_marker() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    CheckpointManager::new(persistence.clone(), Default::default())
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            }],
        )
        .unwrap();
    let (mut wal, _, _) = WalSink::open(persistence.clone(), 0, ProtocolLimits::default()).unwrap();
    wal.record(crate::mutation::Mutation::JsonSetPath {
        key: b"missing",
        path: b"$.field",
        value: b"1",
    })
    .unwrap();
    wal.flush().unwrap();
    drop(wal);
    let marker = CatchupMarker::Installed {
        generation: 1,
        snapshot: 0,
        through: 1,
    };
    replica_guard::write(directory.path(), 0, marker).unwrap();
    let mut config = AppConfig::default();
    config.persistence = persistence;
    config.address = "127.0.0.1:0".into();
    config.replication.role = crate::config::ReplicationRole::Replica;
    config.replication.primary = Some("127.0.0.1:1".parse().unwrap());
    let error = run_with_force(&config, stopped(), false).unwrap_err();
    assert!(error.to_string().contains("JSON path set replay failed"));
    assert_eq!(
        replica_guard::read(directory.path(), 0).unwrap(),
        Some(marker)
    );
}

#[test]
fn preparing_marker_recovers_from_equal_sequence_snapshot_without_new_writes() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication_address = ports.address_at(0);
    let persistence = |path: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let primary_config = persistence(primary_dir.path());
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = primary_config.clone();
        config.replication.role = crate::config::ReplicationRole::Primary;
        config.replication.listen = Some(replication_address);
    });
    assert_eq!(
        request(
            primary_address,
            &resp(&[&[b"SET", b"key", b"value"], &[b"SAVE"]])
        ),
        b"+OK\r\n+OK\r\n"
    );
    let saved = CheckpointManager::new(primary_config, Default::default())
        .load()
        .unwrap()
        .unwrap();
    let replica_config = persistence(replica_dir.path());
    CheckpointManager::new(replica_config.clone(), Default::default())
        .publish(1, &saved.shards)
        .unwrap();
    replica_guard::write(replica_dir.path(), 0, CatchupMarker::Preparing).unwrap();
    let (address, stop, replica) = serve_with(|config| {
        config.persistence = replica_config.clone();
        config.replication.role = crate::config::ReplicationRole::Replica;
        config.replication.primary = Some(replication_address);
    });
    wait_until("the replica to recover", || {
        request(address, &resp(&[&[b"GET", b"key"]])) == b"$5\r\nvalue\r\n"
    });
    assert_eq!(replica_guard::read(replica_dir.path(), 0).unwrap(), None);
    stop.request();
    primary_stop.request();
    join_within(replica, PATIENCE).unwrap();
    join_within(primary, PATIENCE).unwrap();
}

#[test]
fn two_shard_catchup_publishes_complete_checkpoint_before_unblocking() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    // Each shard's replication listener takes `replication_address + shard`.
    let ports = crate::test_ports::reserve();
    let replication_address = ports.address_at(0);
    let persistence = |path: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let (primary_left, primary_right, primary_stop, primary0, primary1) =
        serve_two_shards_with(|config| {
            config.persistence = persistence(primary_dir.path());
            config.replication.role = crate::config::ReplicationRole::Primary;
            config.replication.listen = Some(replication_address);
        });
    let key0 = key_on_shard(0, "replica-a");
    let key1 = key_on_shard(1, "replica-b");
    assert_eq!(
        request(primary_left, &resp(&[&[b"SET", key0.as_bytes(), b"a"]])),
        b"+OK\r\n"
    );
    assert_eq!(
        request(primary_right, &resp(&[&[b"SET", key1.as_bytes(), b"b"]])),
        b"+OK\r\n"
    );
    assert_eq!(request(primary_left, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    assert_eq!(request(primary_right, &resp(&[&[b"SAVE"]])), b"+OK\r\n");
    for shard in 0..2 {
        replica_guard::write(replica_dir.path(), shard, CatchupMarker::Preparing).unwrap();
    }
    let (replica_left, replica_right, replica_stop, replica0, replica1) =
        serve_two_shards_with(|config| {
            config.persistence = persistence(replica_dir.path());
            config.replication.role = crate::config::ReplicationRole::Replica;
            config.replication.primary = Some(replication_address);
        });
    wait_until("both replica shards to catch up", || {
        request(replica_left, &resp(&[&[b"GET", key0.as_bytes()]])) == b"$1\r\na\r\n"
            && request(replica_right, &resp(&[&[b"GET", key1.as_bytes()]])) == b"$1\r\nb\r\n"
    });
    assert_eq!(replica_guard::read(replica_dir.path(), 0).unwrap(), None);
    assert_eq!(replica_guard::read(replica_dir.path(), 1).unwrap(), None);
    // Each shard recorded the primary's history once its own snapshot was installed.
    let primary_history = crate::replication::load_or_create_history(primary_dir.path()).unwrap();
    for shard in 0..2 {
        assert_eq!(
            crate::replication::upstream_history(replica_dir.path(), shard),
            primary_history
        );
    }
    assert_eq!(
        CheckpointManager::new(persistence(replica_dir.path()), Default::default())
            .load()
            .unwrap()
            .unwrap()
            .shards
            .len(),
        2
    );
    replica_stop.request();
    primary_stop.request();
    for join in [replica0, replica1, primary0, primary1] {
        join_within(join, PATIENCE).unwrap();
    }
}

#[test]
fn replica_keeps_json_past_its_own_clock_for_the_delayed_delta_and_persist() {
    resync_expired_json(false, false);
}

#[test]
fn replica_resyncs_compacted_json_history_and_mixed_transaction() {
    resync_expired_json(true, false);
}

#[test]
fn replica_catches_up_from_older_snapshot_while_primary_keeps_writing() {
    resync_expired_json(false, true);
}

fn resync_expired_json(compacted: bool, busy: bool) {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let persistence = |path: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        sync_interval: Duration::from_millis(1),
        snapshot_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let primary_config = persistence(primary_dir.path());
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = primary_config.clone();
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    let original = serde_json::to_vec(&serde_json::json!({
        "padding": "x".repeat(crate::json::TREE_THRESHOLD), "count": 0
    }))
    .unwrap();
    assert_eq!(
        request(
            primary_address,
            &resp(&[&[b"JSON.SET", b"{doc}", b"$", &original],])
        ),
        b"+OK\r\n"
    );
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 2500;
    let expiry_bytes = expiry.to_string();
    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[b"PEXPIREAT", b"{doc}", expiry_bytes.as_bytes()],
                &[b"SAVE"],
            ])
        ),
        b":1\r\n+OK\r\n"
    );

    let replica_config = persistence(replica_dir.path());
    let primary_checkpoints = CheckpointManager::new(primary_config.clone(), Default::default());
    let checkpoint = primary_checkpoints.load().unwrap().unwrap();
    let replica_checkpoints = CheckpointManager::new(replica_config.clone(), Default::default());

    let first_delta = if busy {
        resp(&[&[b"JSON.SET", b"{doc}", b"$.count", b"1"]])
    } else {
        resp(&[&[b"JSON.SET", b"{doc}", b"$.count", b"1"], &[b"SAVE"]])
    };
    assert_eq!(
        request(primary_address, &first_delta),
        if busy {
            &b"+OK\r\n"[..]
        } else {
            &b"+OK\r\n+OK\r\n"[..]
        }
    );
    let intermediate = primary_checkpoints.load().unwrap().unwrap().shards[0].last_sequence;
    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[b"MULTI"],
                &[b"JSON.SET", b"{doc}", b"$.count", b"2"],
                &[b"SET", b"{doc}other", b"written"],
                &[b"EXEC"],
                &[b"PERSIST", b"{doc}"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n:1\r\n"
    );

    while (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
        <= expiry
    {
        thread::sleep(Duration::from_millis(2));
    }
    let replica_snapshot: Vec<_> = checkpoint
        .shards
        .iter()
        .map(|shard| ShardCheckpoint {
            shard_id: shard.shard_id,
            last_sequence: shard.last_sequence,
            entries: if compacted {
                Vec::new()
            } else {
                shard.entries.clone()
            },
        })
        .collect();
    replica_checkpoints.publish(1, &replica_snapshot).unwrap();
    // The replica directory stands in for a copy of the primary's: it continues its history.
    crate::replication::record_upstream_history(
        replica_dir.path(),
        0,
        &crate::replication::Origin {
            history: crate::replication::load_or_create_history(primary_dir.path()).unwrap(),
            epoch: 0,
        },
    )
    .unwrap();
    let writes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer = busy.then(|| {
        let writes = Arc::clone(&writes);
        thread::spawn(move || {
            for index in 1..=500u64 {
                let value = index.to_string();
                assert_eq!(
                    request(
                        primary_address,
                        &resp(&[&[b"SET", b"noise", value.as_bytes()]])
                    ),
                    b"+OK\r\n"
                );
                writes.store(index, Ordering::Release);
                thread::sleep(Duration::from_millis(2));
            }
        })
    });
    if busy {
        wait_until("the primary's writer to start", || {
            writes.load(Ordering::Acquire) >= 20
        });
    }
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = replica_config.clone();
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });
    wait_until("the snapshot resync", || {
        request(
            replica_address,
            &resp(&[&[b"JSON.GET", b"{doc}", b"$.count"]]),
        ) == b"$1\r\n2\r\n"
            && request(replica_address, &resp(&[&[b"GET", b"{doc}other"]])) == b"$7\r\nwritten\r\n"
    });
    // Past its deadline by the replica's clock, the document is still held for the
    // primary's deltas and PERSIST; only a replica without it needs a newer snapshot.
    if compacted {
        assert!(
            replica_checkpoints.load().unwrap().unwrap().shards[0].last_sequence
                >= intermediate + u64::from(busy),
            "replica must install a snapshot containing the first field delta"
        );
    }
    if let Some(writer) = writer {
        join_within(writer, PATIENCE).unwrap();
        wait_until("the replica to catch up with the ongoing writes", || {
            request(replica_address, &resp(&[&[b"GET", b"noise"]])) == b"$3\r\n500\r\n"
        });
    }
    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();

    let (restarted_address, restarted_stop, restarted) = serve_with(|config| {
        config.persistence = replica_config;
    });
    assert_eq!(
        request(
            restarted_address,
            &resp(&[&[b"JSON.GET", b"{doc}", b"$.count"]])
        ),
        b"$1\r\n2\r\n"
    );
    assert_eq!(
        request(restarted_address, &resp(&[&[b"GET", b"{doc}other"]])),
        b"$7\r\nwritten\r\n"
    );
    restarted_stop.request();
    join_within(restarted, PATIENCE).unwrap();
}

#[test]
fn an_idle_replica_applies_a_burst_without_waiting_for_its_timers() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let persistence = |directory: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    let (_replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });

    let records = 20_000u64;
    let mut burst = Vec::new();
    for index in 0..records {
        burst.extend_from_slice(&resp(&[&[b"SET", format!("k{index}").as_bytes(), b"v"]]));
    }
    let replies = request_within(primary_address, &burst, Duration::from_secs(20));
    assert_eq!(replies.len(), records as usize * b"+OK\r\n".len());

    // Nothing talks to the replica: frames must be applied as they arrive, not one queue
    // at a time on the shard's maintenance timers. The log is read from disk so that no
    // command to the replica drains its queue on the way.
    let limits = ProtocolLimits::default();
    let started = Instant::now();

    wait_until("the idle replica to apply the burst", || {
        matches!(
            crate::persistence::read_frames(replica_dir.path(), 0, records - 1, records, limits),
            Ok(crate::persistence::FrameRead::Frames(frames)) if frames.len() == 1
        )
    });
    eprintln!(
        "idle replica applied {records} records in {:?}",
        started.elapsed()
    );

    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();
}

#[test]
fn replica_with_a_pending_full_resync_starts_over_a_log_that_does_not_replay() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    {
        // A frame that failed to apply is already in the replica's log; replay fails on it.
        let (mut sink, _, _) =
            crate::persistence::WalSink::open(persistence.clone(), 0, ProtocolLimits::default())
                .unwrap();
        sink.record(crate::mutation::Mutation::SetBytes {
            key: b"key",
            value: b"v",
        })
        .unwrap();
        sink.record(crate::mutation::Mutation::SortedSetAdd {
            key: b"key",
            pairs: &[(1.0, std::borrow::Cow::Borrowed(b"member"))],
        })
        .unwrap();
        sink.flush().unwrap();
    }
    let mut config = AppConfig {
        persistence: persistence.clone(),
        address: "127.0.0.1:0".into(),
        ..AppConfig::default()
    };

    // Without a pending resync the log is genuinely unusable.
    assert!(run_with_force(&config, stopped(), false).is_err());

    replica_guard::write(directory.path(), 0, CatchupMarker::Preparing).unwrap();
    let (address, stop, server) = serve_with(|replica| {
        replica.persistence = persistence.clone();
        replica.replication.role = crate::config::ReplicationRole::Replica;
        replica.replication.primary = Some("127.0.0.1:1".parse().unwrap());
    });
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"key"]])),
        b"-LOADING replica catching up\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
    assert_eq!(
        replica_guard::read(directory.path(), 0).unwrap(),
        Some(CatchupMarker::Preparing)
    );
    // Promoting it anyway still refuses the log it cannot replay. On a port of its own:
    // the replica's freed one may be taken by now.
    let ports = crate::test_ports::reserve();

    config.address = ports.address_at(0).to_string();
    assert!(run_with_force(&config, stopped(), true).is_err());
}

#[test]
fn replica_whose_log_sync_failed_reports_the_link_down() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let persistence = |directory: &std::path::Path, profile| PersistenceConfig {
        profile,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path(), crate::DurabilityProfile::Everysec);
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            primary: None,
            primary_name: None,
            ..Default::default()
        };
    });
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path(), crate::DurabilityProfile::Always);
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });
    let status = |address| {
        String::from_utf8(request(address, &resp(&[&[b"INFO"]])))
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("master_link_status:").map(str::to_owned))
    };

    request(primary_address, &resp(&[&[b"SET", b"first", b"1"]]));
    wait_for_info(replica_address, "wal_last_sequence", |sequence| {
        sequence >= 1
    });
    assert_eq!(status(replica_address).as_deref(), Some("up"));

    // From now on the replica's log syncs fail: it refuses every later frame.
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .push(replica_dir.path().to_path_buf());
    request(primary_address, &resp(&[&[b"SET", b"second", b"2"]]));
    let reported = wait_for(PATIENCE, || {
        status(replica_address).as_deref() == Some("down")
    });
    crate::persistence::FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .retain(|dir| dir != replica_dir.path());
    primary_stop.request();
    replica_stop.request();
    join_within(primary, PATIENCE).unwrap();
    join_within(replica, PATIENCE).unwrap();
    assert!(
        reported,
        "a replica that cannot log frames must not report its link up"
    );
}

/// A primary restart breaks the link while frames of the old link still wait in the
/// replica's queue. The new link resumes after the last applied sequence, so it sends those
/// frames again; the replica must take them as already logged and keep following. A real
/// gap must turn into a snapshot request, never into a link that stays down.
#[test]
fn replica_follows_a_new_link_that_resends_queued_frames_and_resyncs_on_a_gap() {
    let source_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let limits = ProtocolLimits::default();
    let frames: Vec<Vec<u8>> = {
        let config = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: source_dir.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let (mut sink, _, _) = crate::persistence::WalSink::open(config, 0, limits).unwrap();
        for index in 0..12 {
            sink.record(crate::mutation::Mutation::SetBytes {
                key: format!("k{index}").as_bytes(),
                value: b"v",
            })
            .unwrap();
        }
        sink.flush().unwrap();
        match crate::persistence::read_frames(source_dir.path(), 0, 0, 12, limits).unwrap() {
            crate::persistence::FrameRead::Frames(frames) => frames,
            _ => panic!("source log has no frames"),
        }
    };
    assert_eq!(frames.len(), 12);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replication = listener.local_addr().unwrap();
    let history = crate::replication::Origin {
        history: [5; crate::replication::HISTORY_LEN],
        epoch: 0,
    };
    let ok = |next| {
        let mut reply = crate::replication::encode_reply_with_history(
            crate::replication::HandshakeStatus::Ok,
            next,
            &history,
        );

        reply.extend_from_slice(&crate::replication::encode_address(None));
        reply
    };
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: replica_dir.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            listen: None,
            primary: Some(replication),
            primary_name: None,
            ..Default::default()
        };
    });
    let applied = || info_field(replica_address, "wal_last_sequence").unwrap_or(0);

    // The first link ships frames 1..=5, then the primary dies.
    let mut first = accept_bounded(&listener);
    let handshake = crate::replication::read_current_handshake(&mut first);
    assert_eq!(handshake.after_sequence, 0);
    first.write_all(&ok(1)).unwrap();
    for frame in &frames[..5] {
        first.write_all(frame).unwrap();
    }
    wait_until("the replica to apply frames 1..=5", || applied() >= 5);
    drop(first);

    // The restarted primary resends from 1, as a new link does for frames the shard had not
    // drained when it reconnected, then continues with new frames.
    let mut second = accept_bounded(&listener);
    let handshake = crate::replication::read_current_handshake(&mut second);
    assert_eq!(handshake.version, crate::replication::VERSION_RESUME);
    second.write_all(&ok(handshake.after_sequence + 1)).unwrap();
    for frame in &frames[..10] {
        second.write_all(frame).unwrap();
    }
    let followed = wait_for(PATIENCE, || applied() >= 10);
    let value = request(replica_address, &resp(&[&[b"GET", b"k9"]]));

    // Frame 12 without 11: the replica cannot continue this stream and asks for a snapshot
    // that covers 11 on a new link.
    second.write_all(&frames[11]).unwrap();
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + PATIENCE;
    let resync = loop {
        match listener.accept() {
            Ok((mut third, _)) => {
                third.set_nonblocking(false).unwrap();
                third
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                break Some(crate::replication::read_current_handshake(&mut third));
            }
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Err(_) => break None,
        }
    };

    let reached = applied();
    replica_stop.request();
    drop(second);
    join_within(replica, PATIENCE).unwrap();

    assert!(followed, "replica stopped at sequence {reached}");
    assert_eq!(value, b"$1\r\nv\r\n");
    let resync = resync.expect("replica never asked again after a gap");
    assert_eq!(resync.version, crate::replication::VERSION_RESYNC);
    assert_eq!(resync.after_sequence, 10);
}

/// Shard 1 of a replication port 65535 would wrap to port 0, which binds a random port or
/// dials nothing; the start is refused instead.
#[test]
fn a_replication_port_past_65535_is_refused_instead_of_wrapping() {
    let base: SocketAddr = "127.0.0.1:65535".parse().unwrap();

    assert_eq!(super::super::replica::shard_port(base, 0).unwrap(), base);
    assert_eq!(
        super::super::replica::shard_port(base, 1)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        super::super::replica::shard_port("127.0.0.1:20000".parse().unwrap(), 3)
            .unwrap()
            .port(),
        20_003
    );
}

#[test]
fn a_durable_connection_waits_for_its_replica_and_is_refused_without_one() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let replication = ports.address_at(0);
    let persistence = |directory: &std::path::Path| PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Primary,
            listen: Some(replication),
            ..Default::default()
        };
    });
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = crate::config::ReplicationConfig {
            role: crate::config::ReplicationRole::Replica,
            primary: Some(replication),
            ..Default::default()
        };
    });

    wait_for_info(primary_address, "replication_replicas", |count| count == 1);
    let started = Instant::now();

    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[
                    b"CLIENT",
                    b"DURABILITY",
                    b"REPLICAS",
                    b"1",
                    b"TIMEOUT",
                    b"10000"
                ],
                &[b"SET", b"kept", b"1"],
            ]),
        ),
        b"+OK\r\n+OK\r\n"
    );
    eprintln!("durable SET answered in {:?}", started.elapsed());
    // The replica holds it on disk, and both sides know it was committed.
    let committed = wait_for_info(primary_address, "replication_committed_sequence", |seq| {
        seq > 0
    });

    wait_for_info(replica_address, "replication_committed_sequence", |seq| {
        seq >= committed
    });
    assert!(
        info_field(replica_address, "wal_durable_sequence").unwrap() >= committed,
        "the replica acknowledged a record it had not synced"
    );

    let mut client = connect_bounded(primary_address);
    let mut line = |payload: &[u8]| {
        client.write_all(payload).unwrap();
        let mut reply = Vec::new();
        let mut byte = [0u8; 1];

        while !reply.ends_with(b"\r\n") {
            client.read_exact(&mut byte).unwrap();
            reply.push(byte[0]);
        }
        reply
    };

    assert_eq!(
        line(&resp(&[&[
            b"CLIENT",
            b"DURABILITY",
            b"REPLICAS",
            b"1",
            b"TIMEOUT",
            b"100"
        ]])),
        b"+OK\r\n"
    );
    // Without a replica the write is applied and its reply refused.
    replica_stop.request();
    join_within(replica, PATIENCE).unwrap();
    wait_for_info(primary_address, "replication_replicas", |count| count == 0);

    assert_eq!(
        line(&resp(&[&[b"SET", b"lost", b"1"]])),
        b"-NOREPLICAS Not enough good replicas to write.\r\n"
    );
    // No replica holds that write yet: a read on this connection is held back too.
    assert_eq!(
        line(&resp(&[&[b"EXISTS", b"lost"]])),
        b"-NOREPLICAS Not enough good replicas to write.\r\n"
    );

    primary_stop.request();
    join_within(primary, PATIENCE).unwrap();
}

#[test]
fn client_durability_needs_replication() {
    let (address, stop, server) = serve_on_ephemeral();

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"CLIENT", b"DURABILITY", b"REPLICAS", b"1"],
                &[b"CLIENT", b"DURABILITY", b"REPLICAS", b"0"],
            ]),
        ),
        b"-ERR CLIENT DURABILITY needs replication on this server\r\n+OK\r\n"
    );

    stop.request();
    join_within(server, PATIENCE).unwrap();
}
