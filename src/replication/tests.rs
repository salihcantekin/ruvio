use super::*;
use crate::persistence::read_bounded_history;
use std::io::{Read, Write};

#[test]
fn a_deadline_or_persist_for_a_key_the_replica_lacks_is_a_divergence() {
    let mut shard = KvShard::new();
    let now = shard.now_unix_millis().get();
    let past = OwnedMutation::ExpireAt {
        key: b"gone".to_vec(),
        unix_millis: now.saturating_sub(1),
    };
    let future = OwnedMutation::ExpireAt {
        key: b"gone".to_vec(),
        unix_millis: now + 60_000,
    };

    assert!(needs_snapshot(&mut shard, &past));
    assert!(needs_snapshot(&mut shard, &future));
    assert!(needs_snapshot(
        &mut shard,
        &OwnedMutation::Persist {
            key: b"gone".to_vec()
        }
    ));
}

#[test]
fn a_rename_or_move_of_a_key_the_replica_lacks_is_a_divergence() {
    let mut shard = KvShard::new();
    let rename = OwnedMutation::Rename {
        source: b"held".to_vec(),
        destination: b"renamed".to_vec(),
    };
    let moved = OwnedMutation::MoveKey {
        key: b"held".to_vec(),
        destination: 1,
    };

    assert!(needs_snapshot(&mut shard, &rename));
    assert!(needs_snapshot(&mut shard, &moved));

    shard.set_bytes(b"held", b"v");
    assert!(!needs_snapshot(&mut shard, &rename));
    assert!(!needs_snapshot(&mut shard, &moved));

    shard.enter(1);
    shard.set_bytes(b"held", b"other");
    shard.enter(0);
    assert!(needs_snapshot(&mut shard, &moved));
    assert_eq!(shard.active_database(), 0);
}

#[test]
fn a_transaction_setting_an_already_expired_value_then_its_deadline_is_not_a_divergence() {
    let mut shard = KvShard::new();
    let now = shard.now_unix_millis().get();
    // The primary's clock is behind this replica's: its deadline is already past here. The
    // replica keeps the key until the primary deletes it, so the next deadline has a key.
    let mutations = [
        OwnedMutation::SetBytesExpiring {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at: now.saturating_sub(5),
        },
        OwnedMutation::ExpireAt {
            key: b"k".to_vec(),
            unix_millis: now + 60_000,
        },
    ];

    assert!(!transaction_needs_snapshot(&mut shard, 0, &mutations));
}

#[test]
fn durable_acks_count_only_replicas_that_synced_the_target() {
    let acks = DurableAcks::new();

    acks.reserve(0);
    acks.reserve(1);
    assert_eq!(acks.reached(0), 2);
    assert_eq!(acks.reached(1), 0);
    acks.observe(0, 4);
    assert_eq!(acks.reached(4), 1);
    assert_eq!(acks.reached(5), 0);
    acks.observe(1, 4);
    assert_eq!(acks.reached(4), 2);
    acks.clear(0);
    assert_eq!(acks.reached(4), 1);
}

#[test]
fn connected_replica_cursors_publish_the_oldest_and_the_count() {
    let floor = Arc::new(AtomicU64::new(u64::MAX));
    let count = Arc::new(AtomicU64::new(0));
    let progress = Arc::new(ReplicaProgress::new(
        Arc::clone(&floor),
        Arc::clone(&count),
        Arc::new(DurableAcks::new()),
    ));
    let first = progress.hold(4);

    assert_eq!(floor.load(Ordering::Acquire), 4);
    assert_eq!(count.load(Ordering::Acquire), 1);

    let second = progress.hold(9);

    assert_eq!(floor.load(Ordering::Acquire), 4);
    assert_eq!(count.load(Ordering::Acquire), 2);
    first.advance(12);
    assert_eq!(floor.load(Ordering::Acquire), 9);
    drop(second);
    assert_eq!(floor.load(Ordering::Acquire), 12);
    assert_eq!(count.load(Ordering::Acquire), 1);
    drop(first);
    assert_eq!(floor.load(Ordering::Acquire), u64::MAX);
    assert_eq!(count.load(Ordering::Acquire), 0);
}

#[test]
fn encode_handshake_returns_an_error_when_the_password_does_not_fit() {
    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: vec![b'a'; 65_536],
        epoch: 0,
        announce: None,
    };

    assert!(encode_handshake(&handshake).is_err());
}

#[test]
fn handshake_round_trips_and_rejects_a_bad_checksum() {
    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
        epoch: 0,
        announce: None,
    };

    let bytes = encode_handshake(&handshake).unwrap();

    assert_eq!(&bytes[..8], b"RUVREP01");
    assert_eq!(decode_handshake(&bytes), Ok(handshake));
    let mut broken = bytes.clone();

    *broken.last_mut().unwrap() ^= 0xff;
    assert!(decode_handshake(&broken).is_err());
    let reply = encode_reply(HandshakeStatus::Gap, 0);

    assert_eq!(decode_reply(&reply), Ok((HandshakeStatus::Gap, 0)));
    assert_eq!(reply.len(), 46);
    let snapshot = encode_reply(HandshakeStatus::Snapshot, 41);

    assert_eq!(decode_reply(&snapshot), Ok((HandshakeStatus::Snapshot, 41)));
    assert_eq!(snapshot.len(), 46);
}

#[test]
fn decode_handshake_rejects_a_short_buffer_bad_magic_and_length_mismatch() {
    assert!(decode_handshake(&[]).is_err());
    assert!(decode_handshake(&[0; 55]).is_err());

    let empty = Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    };

    let empty_bytes = encode_handshake(&empty).unwrap();

    assert_eq!(empty_bytes.len(), 60);
    assert_eq!(decode_handshake(&empty_bytes), Ok(empty));

    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
        epoch: 0,
        announce: None,
    };

    let mut bad_magic = encode_handshake(&handshake).unwrap();

    bad_magic[0] ^= 0xff;
    let checksum_at = bad_magic.len() - 4;
    let checksum = crc32c::crc32c(&bad_magic[..checksum_at]);

    bad_magic[checksum_at..].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_handshake(&bad_magic).is_err());

    let mut bad_length = encode_handshake(&handshake).unwrap();

    bad_length[22] = 5;
    let checksum_at = bad_length.len() - 4;
    let checksum = crc32c::crc32c(&bad_length[..checksum_at]);

    bad_length[checksum_at..].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_handshake(&bad_length).is_err());

    let mut long = encode_handshake(&handshake).unwrap();

    long.push(0);
    assert!(decode_handshake(&long).is_err());
}

#[test]
fn decode_reply_rejects_a_short_buffer_bad_magic_bad_checksum_and_unknown_status() {
    assert!(decode_reply(&[]).is_err());
    assert!(decode_reply(&[0; 21]).is_err());

    let mut bad_magic = encode_reply(HandshakeStatus::Ok, 9);

    bad_magic[0] ^= 0xff;
    let checksum = crc32c::crc32c(&bad_magic[..18]);

    bad_magic[18..22].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_reply(&bad_magic).is_err());

    let mut bad_checksum = encode_reply(HandshakeStatus::Ok, 9);

    *bad_checksum.last_mut().unwrap() ^= 0xff;
    assert!(decode_reply(&bad_checksum).is_err());

    let mut unknown = encode_reply(HandshakeStatus::Local, 3);

    unknown[8] = 9;
    unknown[9] = 0;
    let checksum = crc32c::crc32c(&unknown[..18]);

    unknown[18..22].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_reply(&unknown).is_err());

    let mut long = encode_reply(HandshakeStatus::Ok, 9);

    long.push(0);
    assert!(decode_reply(&long).is_err());
    assert_eq!(
        decode_reply(&encode_reply(HandshakeStatus::Ahead, 41)),
        Ok((HandshakeStatus::Ahead, 41))
    );
}

#[test]
fn handshake_status_codes_and_limits_match_the_contract() {
    assert_eq!(HANDSHAKE_MAGIC, b"RUVREP01");
    assert_eq!(
        READONLY_LINE,
        b"READONLY You can't write against a read only replica."
    );
    assert_eq!(QUEUE_FRAMES, 1024);
    assert_eq!(QUEUE_BYTES, 16 * 1024 * 1024);
    assert_eq!(RETRY_INTERVAL, Duration::from_secs(1));
    assert_eq!(SHIPPER_POLL, Duration::from_millis(10));
    assert_eq!(
        (VERSION_RESUME, VERSION_RESYNC, VERSION_RESYNC_INCOMPLETE),
        (15, 16, 17)
    );

    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 2,
        after_sequence: 40,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: b"secret".to_vec(),
        epoch: 3,
        announce: None,
    };

    let bytes = encode_handshake(&handshake).unwrap();

    assert_eq!(&bytes[8..10], &15u16.to_le_bytes());
    assert_eq!(&bytes[10..14], &2u32.to_le_bytes());
    assert_eq!(&bytes[14..22], &40u64.to_le_bytes());
    assert_eq!(&bytes[22..24], &6u16.to_le_bytes());
    assert_eq!(&bytes[24..30], b"secret");
    assert_eq!(&bytes[30..46], &[0; HISTORY_LEN]);
    assert_eq!(&bytes[46..50], &1u32.to_le_bytes());
    assert_eq!(&bytes[50..58], &3u64.to_le_bytes());
    // No client address: port 0 and an empty host.
    assert_eq!(&bytes[58..62], &[0; 4]);
    assert_eq!(
        u32::from_le_bytes(bytes[62..66].try_into().unwrap()),
        crc32c::crc32c(&bytes[..62])
    );

    let reply = encode_reply_with_history(
        HandshakeStatus::Gap,
        0,
        &Origin {
            history: [7; HISTORY_LEN],
            epoch: 4,
        },
    );

    assert_eq!(&reply[8..10], &2u16.to_le_bytes());
    assert_eq!(&reply[10..18], &0u64.to_le_bytes());
    assert_eq!(&reply[18..34], &[7; HISTORY_LEN]);
    assert_eq!(&reply[34..42], &4u64.to_le_bytes());
    assert_eq!(
        u32::from_le_bytes(reply[42..46].try_into().unwrap()),
        crc32c::crc32c(&reply[..42])
    );

    let statuses = [
        (HandshakeStatus::Ok, 1),
        (HandshakeStatus::Gap, 2),
        (HandshakeStatus::Auth, 3),
        (HandshakeStatus::Shard, 4),
        (HandshakeStatus::Version, 5),
        (HandshakeStatus::Ahead, 6),
        (HandshakeStatus::Local, 7),
        (HandshakeStatus::Snapshot, 8),
        (HandshakeStatus::History, 9),
        (HandshakeStatus::Stale, 10),
    ];

    for (status, code) in statuses {
        let reply = encode_reply(status, 11);

        assert_eq!(u16::from_le_bytes(reply[8..10].try_into().unwrap()), code);
        assert_eq!(decode_reply(&reply), Ok((status, 11)));
    }
}

#[test]
fn mixed_expired_delta_and_compacted_history_never_enter_the_replica_wal() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, ShardCheckpoint},
    };

    let limits = ProtocolLimits::default();
    let source_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
    primary
        .record(Mutation::Transaction {
            mutations: &[
                OwnedMutation::JsonSetPath {
                    key: b"doc".to_vec(),
                    path: b"$.count".to_vec(),
                    value: b"2".to_vec(),
                },
                OwnedMutation::SetBytes {
                    key: b"other".to_vec(),
                    value: b"written".to_vec(),
                },
            ],
        })
        .unwrap();
    primary
        .record(Mutation::JsonSetPath {
            key: b"doc",
            path: b"$.count",
            value: b"3",
        })
        .unwrap();
    let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 2, limits).unwrap() else {
        panic!("primary WAL frames unavailable");
    };
    assert!(matches!(
        read_bounded_history(source_dir.path(), 0, 0, 2, limits, 1, u64::MAX),
        Ok(FrameRead::Local),
    ));
    assert!(matches!(
        read_bounded_history(source_dir.path(), 0, 0, 2, limits, 2, 1),
        Ok(FrameRead::Local),
    ));

    let mut original = KvShard::new();
    original
        .put_json(b"doc", br#"{"count":1}"#.as_slice())
        .unwrap();
    let mut entries = original.snapshot_entries();
    entries[0].expires_at = Some(crate::UnixMillis::new(1));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: entries.clone(),
            }],
        )
        .unwrap();
    assert!(manager.load_bounded_history(1).is_err());
    let mut replica = KvShard::new();
    let paused = replica.pause_expiration_for_replay();
    for entry in entries {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    replica.resume_expiration_after_replay(paused);
    assert_eq!(replica.expire_cycle(usize::MAX), 1);

    let (mut wal, _, _) = WalSink::open_after(replica_config.clone(), 0, limits, 0).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits,
        ),
        Err(ApplyFailure::NeedsSnapshot),
    );
    assert_eq!(wal.last_sequence(), 0);
    assert_eq!(replica.value_kind(b"other"), None);

    manager
        .publish(
            2,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 1,
                entries: vec![],
            }],
        )
        .unwrap();
    wal.install_base(1).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[1],
            limits,
        ),
        Err(ApplyFailure::NeedsSnapshot),
    );
    assert_eq!(wal.last_sequence(), 1);
    drop(wal);
    let (_, mutations, _) = WalSink::open_after(replica_config, 0, limits, 1).unwrap();
    assert!(mutations.is_empty());
}

#[test]
fn transaction_preflight_uses_sequential_key_kinds_before_append() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::DurabilityProfile,
    };

    let run = |mutations: Vec<OwnedMutation>, valid: bool| {
        let source_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let config = |path: &std::path::Path| PersistenceConfig {
            profile: DurabilityProfile::Everysec,
            data_dir: path.to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = ProtocolLimits::default();
        let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
        primary
            .record(Mutation::Transaction {
                mutations: &mutations,
            })
            .unwrap();
        let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 1, limits).unwrap()
        else {
            panic!("transaction frame unavailable");
        };
        let replica_config = config(replica_dir.path());
        let manager = CheckpointManager::new(replica_config.clone(), limits);
        let (mut wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();
        let mut shard = KvShard::new();
        let result = accept_replica_frame(
            &mut shard,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits,
        );
        if valid {
            let (db, mutation, _) = result.unwrap();
            crate::persistence::apply_mutation(&mut shard, db, &mutation).unwrap();
            assert_eq!(wal.last_sequence(), 1);
        } else {
            assert_eq!(result, Err(ApplyFailure::NeedsSnapshot));
            assert_eq!(wal.last_sequence(), 0);
            drop(wal);
            let (_, replayed, _) = WalSink::open(replica_config, 0, limits).unwrap();
            assert!(replayed.is_empty());
        }
    };

    let large = format!(
        r#"{{"field":0,"padding":"{}"}}"#,
        "x".repeat(crate::json::TREE_THRESHOLD)
    );
    let put = || OwnedMutation::JsonPut {
        key: b"doc".to_vec(),
        value: large.as_bytes().to_vec(),
    };
    let path = |key: &[u8]| OwnedMutation::JsonSetPath {
        key: key.to_vec(),
        path: b"$.field".to_vec(),
        value: b"1".to_vec(),
    };
    let bytes = |key: &[u8]| OwnedMutation::SetBytes {
        key: key.to_vec(),
        value: b"value".to_vec(),
    };
    run(
        vec![
            put(),
            path(b"doc"),
            OwnedMutation::Persist {
                key: b"doc".to_vec(),
            },
        ],
        true,
    );
    run(vec![put(), put(), path(b"doc")], true);
    run(
        vec![
            bytes(b"other"),
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
            OwnedMutation::ExpireAt {
                key: b"other".to_vec(),
                unix_millis: u64::MAX / 2,
            },
        ],
        true,
    );
    run(
        vec![
            put(),
            OwnedMutation::Rename {
                source: b"doc".to_vec(),
                destination: b"renamed".to_vec(),
            },
            path(b"renamed"),
        ],
        true,
    );
    run(
        vec![
            put(),
            OwnedMutation::Delete {
                keys: vec![b"doc".to_vec()],
            },
            path(b"doc"),
        ],
        false,
    );
    run(vec![put(), bytes(b"doc"), path(b"doc")], false);
    run(
        vec![put(), OwnedMutation::FlushDatabase, path(b"doc")],
        false,
    );
    run(
        vec![
            put(),
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            path(b"doc"),
        ],
        false,
    );
    run(
        vec![
            put(),
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
            path(b"doc"),
        ],
        true,
    );
    run(
        vec![
            OwnedMutation::SetStore {
                key: b"other".to_vec(),
                members: vec![],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
    run(
        vec![
            OwnedMutation::SetStore {
                key: b"other".to_vec(),
                members: vec![b"member".to_vec()],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        true,
    );
    run(
        vec![
            bytes(b"other"),
            OwnedMutation::Delete {
                keys: vec![b"other".to_vec()],
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
    run(
        vec![
            OwnedMutation::LimitPut {
                key: b"other".to_vec(),
                used: 1,
                expires_at: None,
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        true,
    );
    run(
        vec![
            OwnedMutation::SemaphoreAcquire {
                key: b"other".to_vec(),
                token: 1,
                expires_at: u64::MAX / 2,
            },
            OwnedMutation::SemaphoreRelease {
                key: b"other".to_vec(),
                token: 1,
            },
            OwnedMutation::Persist {
                key: b"other".to_vec(),
            },
        ],
        false,
    );
}

#[test]
fn expired_replica_snapshot_json_delta_requests_snapshot_without_appending() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, ShardCheckpoint},
    };

    let limits = ProtocolLimits::default();
    let source_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut primary, _, _) = WalSink::open(config(source_dir.path()), 0, limits).unwrap();
    primary
        .record(Mutation::JsonSetPath {
            key: b"doc",
            path: b"$.count",
            value: b"2",
        })
        .unwrap();
    primary.record(Mutation::Persist { key: b"doc" }).unwrap();
    let FrameRead::Frames(frames) = read_frames(source_dir.path(), 0, 0, 2, limits).unwrap() else {
        panic!("primary WAL frames unavailable");
    };

    let mut original = KvShard::new();
    original.put_json(b"doc", br#"{"count":1}"#).unwrap();
    let mut entries = original.snapshot_entries();
    entries[0].expires_at = Some(crate::UnixMillis::new(1));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: entries.clone(),
            }],
        )
        .unwrap();
    let mut replica = KvShard::new();
    let paused = replica.pause_expiration_for_replay();
    for entry in entries {
        replica.restore_snapshot_entry(entry).unwrap();
    }
    replica.resume_expiration_after_replay(paused);
    assert_eq!(replica.expire_cycle(usize::MAX), 1);

    let (mut wal, _, _) = WalSink::open_after(replica_config.clone(), 0, limits, 0).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[0],
            limits
        ),
        Err(ApplyFailure::NeedsSnapshot)
    );
    assert_eq!(wal.last_sequence(), 0);
    manager
        .publish(
            2,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 1,
                entries: vec![],
            }],
        )
        .unwrap();
    wal.install_base(1).unwrap();
    assert_eq!(
        accept_replica_frame(
            &mut replica,
            &mut wal,
            &manager,
            &replica_config,
            &frames[1],
            limits
        ),
        Err(ApplyFailure::NeedsSnapshot)
    );
    assert_eq!(wal.last_sequence(), 1);
    drop(wal);
    let (_, mutations, _) = WalSink::open_after(replica_config, 0, limits, 1).unwrap();
    assert!(mutations.is_empty());
}

// Arms a WAL I/O fault, which exists only outside Miri.
#[cfg(not(miri))]
#[test]
fn apply_frame_appends_then_applies_and_stops_on_a_bad_frame() {
    use std::sync::atomic::Ordering;

    use crate::{
        KvShard,
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, FrameRead, PersistenceConfig, WalSink, read_frames},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let primary_config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: primary_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut primary, _, _) = WalSink::open(primary_config, 0, limits).unwrap();

    primary
        .record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
    primary
        .record(Mutation::SetBytes {
            key: b"k",
            value: b"w",
        })
        .unwrap();

    let FrameRead::Frames(frames) = read_frames(primary_dir.path(), 0, 0, 1, limits).unwrap()
    else {
        panic!("primary did not return the first frame");
    };

    let frame = frames[0].clone();
    let FrameRead::Frames(next_frames) = read_frames(primary_dir.path(), 0, 1, 2, limits).unwrap()
    else {
        panic!("primary did not return the next frame");
    };

    let next = next_frames[0].clone();
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: replica_dir.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut replica, _, _) = WalSink::open(replica_config, 0, limits).unwrap();
    let mut shard = KvShard::new();

    assert_eq!(
        apply_frame(&mut shard, &mut replica, &frame, limits),
        Ok(())
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
    assert_eq!(replica.published().load(Ordering::Acquire), 1);
    assert_eq!(
        apply_frame(&mut shard, &mut replica, &frame, limits),
        Err(ApplyFailure::Sequence)
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));

    let mut broken = frame.clone();

    *broken.last_mut().unwrap() ^= 0xff;
    assert_eq!(
        apply_frame(&mut shard, &mut replica, &broken, limits),
        Err(ApplyFailure::BadFrame)
    );
    assert_eq!(replica.published().load(Ordering::Acquire), 1);
    crate::persistence::arm_io_fault(crate::persistence::CrashSite::Append);

    let persisted = apply_frame(&mut shard, &mut replica, &next, limits);

    assert!(
        matches!(persisted, Err(ApplyFailure::Persist(ref message)) if message.contains("no space")),
        "{persisted:?}"
    );
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
}

#[test]
fn frame_reader_accepts_a_length_above_the_default_within_configured_max() {
    let configured = crate::protocol::DEFAULT_MAX_COMMAND_BYTES + 64;
    let length = crate::protocol::DEFAULT_MAX_COMMAND_BYTES + 1;
    let mut reader = FrameReader {
        pending: (length as u32).to_le_bytes().to_vec(),
        max_frame_bytes: configured,
    };

    let taken = reader
        .take_frame()
        .expect("length within configured max_command_bytes is a legal frame");

    assert!(taken.is_none());
    reader.pending = 31u32.to_le_bytes().to_vec();
    assert!(reader.take_frame().is_err());
}

#[test]
fn gap_handshake_leaves_the_live_generation() {
    use std::sync::atomic::Ordering;

    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let value = vec![b'v'; 2500];

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: &value,
    })
    .unwrap();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: &value,
    })
    .unwrap();
    sink.flush().unwrap();

    let published = sink.durable();

    assert_eq!(published.load(Ordering::Acquire), 2);
    drop(sink);

    let shard_dir = directory.path().join("wal").join("shard-0000");
    let mut segments = std::fs::read_dir(&shard_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();

    segments.sort();
    assert_eq!(segments.len(), 2, "{segments:?}");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published,
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: MAX_REPLICAS,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 1,
            lineage: Lineage::root([0; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();

    let mut live = connect_replication(address);
    let live_handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 1,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    })
    .unwrap();

    live.write_all(&live_handshake).unwrap();
    let reply = read_reply_bytes(&mut live);

    assert_eq!(decode_reply(&reply).unwrap().0, HandshakeStatus::Ok);
    let _frame = read_replication_frame(&mut live);

    std::fs::remove_file(&segments[0]).unwrap();

    let mut gap = connect_replication(address);
    let gap_handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    })
    .unwrap();

    gap.write_all(&gap_handshake).unwrap();
    let gap_reply = read_reply_bytes(&mut gap);

    assert_eq!(decode_reply(&gap_reply).unwrap().0, HandshakeStatus::Gap);
    drop(gap);
    thread::sleep(Duration::from_millis(200));
    live.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    let mut byte = [0; 1];
    let still_live = live.read(&mut byte);

    assert!(
        matches!(
            still_live,
            Err(ref error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock
        ),
        "gap handshake closed the live replica stream: {still_live:?}"
    );
    stop_primary(primary);
}

#[test]
fn two_replicas_receive_the_same_appended_frame() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.durable();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published,
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: MAX_REPLICAS,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 1,
            lineage: Lineage::root([0; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();
    let mut first = open_ok_replica(address, 0);
    let mut second = open_ok_replica(address, 0);

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.flush().unwrap();
    let first_frame = read_replication_frame(&mut first);
    let second_frame = read_replication_frame(&mut second);

    assert_eq!(first_frame, second_frame);
    stop_primary(primary);
}

#[test]
fn closing_one_replica_leaves_the_other_streaming() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.durable();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published,
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: MAX_REPLICAS,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 1,
            lineage: Lineage::root([0; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();
    let gone = open_ok_replica(address, 0);
    let mut staying = open_ok_replica(address, 0);

    drop(gone);
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.flush().unwrap();
    let frame = read_replication_frame(&mut staying);

    assert!(frame.len() > 4);
    stop_primary(primary);
}

#[test]
fn replica_limit_closes_the_extra_connection() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };

    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published: sink.durable(),
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: 1,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 1,
            lineage: Lineage::root([0; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();
    let _live = open_ok_replica(address, 0);
    let mut extra = connect_replication(address);
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    })
    .unwrap();
    let wrote = extra.write_all(&handshake);

    if wrote.is_ok() {
        let mut byte = [0; 1];
        let read = extra.read(&mut byte);

        assert!(
            matches!(read, Ok(0) | Err(_)),
            "extra replica stayed open: {read:?}"
        );
    }

    stop_primary(primary);
}

/// A replica that hangs up gives its slot back even while the primary has nothing to ship:
/// with a limit of one, the replica that dials again is served.
#[test]
fn a_replica_that_hangs_up_on_an_idle_primary_frees_its_slot() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published: sink.durable(),
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: 1,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 1,
            lineage: Lineage::root([0; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    })
    .unwrap();

    // Each replica is served, then hangs up; the next one waits at most for that
    // connection to notice.
    for _ in 0..4 {
        let deadline = std::time::Instant::now() + PATIENCE;
        let served = loop {
            let mut stream = connect_replication(address);
            let answered = stream
                .write_all(&handshake)
                .and_then(|()| try_read_reply_bytes(&mut stream));

            if let Ok(reply) = answered {
                break decode_reply(&reply).ok().map(|reply| reply.0);
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            thread::sleep(Duration::from_millis(20));
        };

        assert_eq!(served, Some(HandshakeStatus::Ok));
    }

    stop_primary(primary);
}

#[test]
fn primary_refuses_a_replica_from_another_history_or_shard_layout() {
    use crate::{
        persistence::{DurabilityProfile, PersistenceConfig, WalSink},
        protocol::ProtocolLimits,
    };

    let limits = ProtocolLimits::default();
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    let (sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    let primary = spawn_primary_on(
        listener,
        Shipment {
            listen: address,
            shard_id: 0,
            published: sink.durable(),
            data_dir: directory.path().to_path_buf(),
            requirepass: Vec::new(),
            limits,
            max_replicas: 4,
            backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
            replica_count: Arc::new(AtomicU64::new(0)),
            acks: Arc::new(DurableAcks::new()),
            fanout: Arc::new(PublishFanout::new()),
            snapshot_max_bytes: 1024 * 1024 * 1024,
            shard_count: 2,
            lineage: Lineage::root([7; HISTORY_LEN]),
            newer_epoch: Arc::new(AtomicU64::new(0)),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            announce: None,
            tls: None,
            durable_replies: Default::default(),
        },
    )
    .unwrap();
    let ask = |version: u16, after_sequence: u64, history: [u8; HISTORY_LEN], shard_count: u32| {
        let mut stream = connect_replication(address);
        let handshake = encode_handshake(&Handshake {
            version,
            shard_id: 0,
            after_sequence,
            history,
            shard_count,
            password: Vec::new(),
            epoch: 0,
            announce: None,
        })
        .unwrap();

        stream.write_all(&handshake).unwrap();
        decode_reply(&read_reply_bytes(&mut stream)).unwrap().0
    };

    // Resuming from its own log under a different history is refused; a forced snapshot
    // and a replica with no log yet are not.
    assert_eq!(
        ask(VERSION_RESUME, 5, [9; HISTORY_LEN], 2),
        HandshakeStatus::History
    );
    // Same history: the ordinary checks follow (this log is shorter than the replica's).
    assert_eq!(
        ask(VERSION_RESUME, 5, [7; HISTORY_LEN], 2),
        HandshakeStatus::Ahead
    );
    assert_eq!(
        ask(VERSION_RESUME, 0, [9; HISTORY_LEN], 2),
        HandshakeStatus::Ok
    );
    assert_eq!(
        ask(VERSION_RESUME, 0, [7; HISTORY_LEN], 3),
        HandshakeStatus::Shard
    );

    stop_primary(primary);
}

fn open_ok_replica(address: SocketAddr, after_sequence: u64) -> TcpStream {
    let mut stream = connect_replication(address);
    let handshake = encode_handshake(&Handshake {
        version: VERSION_RESUME,
        shard_id: 0,
        after_sequence,
        history: [0; HISTORY_LEN],
        shard_count: 1,
        password: Vec::new(),
        epoch: 0,
        announce: None,
    })
    .unwrap();

    stream.write_all(&handshake).unwrap();
    let reply = read_reply_bytes(&mut stream);

    assert_eq!(decode_reply(&reply).unwrap().0, HandshakeStatus::Ok);
    stream
}

/// How long a test waits for what a passing run sees within milliseconds: a bug fails the
/// test instead of hanging the run.
const PATIENCE: Duration = Duration::from_secs(30);

/// Joins `handle`, failing the test when the thread has not ended within [`PATIENCE`].
#[track_caller]
fn join_within<T>(handle: JoinHandle<T>) -> thread::Result<T> {
    let deadline = std::time::Instant::now() + PATIENCE;

    while !handle.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "a thread did not end within {PATIENCE:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }

    handle.join()
}

/// Stops a primary started with `spawn_primary_on`, failing the test when its threads have
/// not ended within [`PATIENCE`] (dropping it joins them).
#[track_caller]
fn stop_primary(primary: ReplicationThread) {
    join_within(thread::spawn(move || drop(primary))).unwrap();
}

fn connect_replication(address: SocketAddr) -> TcpStream {
    let deadline = std::time::Instant::now() + PATIENCE;

    loop {
        if let Ok(stream) = TcpStream::connect(address) {
            stream.set_nodelay(true).unwrap();
            stream.set_read_timeout(Some(PATIENCE)).unwrap();
            stream.set_write_timeout(Some(PATIENCE)).unwrap();

            return stream;
        }

        if std::time::Instant::now() >= deadline {
            panic!("timed out connecting to {address}");
        }

        thread::sleep(Duration::from_millis(10));
    }
}

fn read_socket(stream: &mut TcpStream, length: usize) -> Vec<u8> {
    let mut buffer = vec![0; length];

    stream.read_exact(&mut buffer).unwrap();
    buffer
}

/// A primary's reply, after reading the client address that follows every reply but a
/// version refusal.
fn try_read_reply_bytes(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut reply = vec![0; REPLY_LEN];

    stream.read_exact(&mut reply)?;
    if read_u16(&reply, 8) != HandshakeStatus::Version as u16 {
        let mut address = vec![0; 4];

        stream.read_exact(&mut address)?;
        address.resize(ADDRESS_FRAME_MIN + usize::from(read_u16(&address, 2)), 0);
        stream.read_exact(&mut address[4..])?;
        decode_address(&address)
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))?;
    }

    Ok(reply)
}

fn read_reply_bytes(stream: &mut TcpStream) -> Vec<u8> {
    try_read_reply_bytes(stream).unwrap()
}

fn read_replication_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut length_bytes = [0; 4];

    stream.read_exact(&mut length_bytes).unwrap();
    let length = u32::from_le_bytes(length_bytes) as usize;
    let mut frame = vec![0; length];

    frame[..4].copy_from_slice(&length_bytes);
    stream.read_exact(&mut frame[4..]).unwrap();
    frame
}

#[test]
fn copied_publish_round_trips_and_rejects_a_wal_frame() {
    let mut frame = encode_copied_publish(false, b"news", b"hello");

    assert!(frame.len() >= 32);
    assert_eq!(
        decode_copied_publish(&frame),
        Some((false, b"news".to_vec(), b"hello".to_vec()))
    );
    frame[4] = b'X';
    assert_eq!(decode_copied_publish(&frame), None);
}

#[test]
fn control_frames_round_trip_and_are_not_publish_or_wal_frames() {
    let mut frame = encode_control(41, true);

    assert_eq!(frame.len(), 32);
    assert_eq!(decode_control(&frame), Some((41, true)));
    assert_eq!(decode_control(&encode_control(7, false)), Some((7, false)));
    assert_eq!(decode_copied_publish(&frame), None);
    assert_eq!(
        decode_control(&encode_copied_publish(false, b"news", b"hello")),
        None
    );
    frame[20] = 0;
    assert_eq!(decode_control(&frame), None, "the checksum covers the flag");
}

fn idle_shipment(data_dir: &Path, history: HistoryId) -> Shipment {
    Shipment {
        listen: "127.0.0.1:0".parse().unwrap(),
        shard_id: 0,
        published: Arc::new(crate::persistence::DurableSequence::new(0)),
        data_dir: data_dir.to_path_buf(),
        requirepass: Vec::new(),
        limits: ProtocolLimits::default(),
        max_replicas: 4,
        backlog_floor: Arc::new(AtomicU64::new(u64::MAX)),
        replica_count: Arc::new(AtomicU64::new(0)),
        acks: Arc::new(DurableAcks::new()),
        fanout: Arc::new(PublishFanout::new()),
        snapshot_max_bytes: 1024 * 1024,
        shard_count: 1,
        lineage: Lineage::root(history),
        newer_epoch: Arc::new(AtomicU64::new(0)),
        snapshot_requests: Arc::new(AtomicU64::new(0)),
        announce: None,
        tls: None,
        durable_replies: Default::default(),
    }
}

#[test]
fn upstream_history_is_kept_per_shard_and_ignores_the_shared_legacy_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();

    // A copy of a primary's directory continues that primary's history.
    std::fs::write(path.join("HISTORY"), [3; HISTORY_LEN]).unwrap();
    assert_eq!(upstream_history(path, 0), [3; HISTORY_LEN]);

    record_upstream_history(
        path,
        0,
        &Origin {
            history: [1; HISTORY_LEN],
            epoch: 0,
        },
    )
    .unwrap();
    record_upstream_history(
        path,
        1,
        &Origin {
            history: [2; HISTORY_LEN],
            epoch: 0,
        },
    )
    .unwrap();
    assert_eq!(upstream_history(path, 0), [1; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 1), [2; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 2), [3; HISTORY_LEN]);
    assert!(!path.join("UPSTREAM_0000.tmp").exists());
    assert!(!path.join("UPSTREAM_0001.tmp").exists());

    // The shared file of older builds may have been written by another shard: a shard
    // without its own record claims no history, so the primary sends it a full copy.
    std::fs::write(path.join("UPSTREAM"), [4; HISTORY_LEN]).unwrap();
    assert_eq!(upstream_history(path, 2), [0; HISTORY_LEN]);
    assert_eq!(upstream_history(path, 1), [2; HISTORY_LEN]);
}

fn link_to(
    primary: SocketAddr,
    data_dir: &Path,
) -> (ReplicaLink, std::sync::mpsc::Receiver<ReplicaDelivery>) {
    let config = PersistenceConfig {
        profile: crate::persistence::DurabilityProfile::Everysec,
        data_dir: data_dir.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (wal, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let (inbound, outbound) = std::sync::mpsc::sync_channel(QUEUE_FRAMES);

    (
        ReplicaLink {
            primary,
            primary_name: None,
            shard_id: 0,
            wal: Arc::new(Mutex::new(wal)),
            inbound,
            password: Vec::new(),
            limits: ProtocolLimits::default(),
            snapshot_max_bytes: 1024 * 1024,
            resync_sequence: Arc::new(AtomicU64::new(0)),
            data_dir: data_dir.to_path_buf(),
            shard_count: 1,
            status: Arc::new(LinkStatus::default()),
            wake_shard: Arc::new(tokio::sync::Notify::new()),
            announce: None,
            peers: Arc::new(DurableAcks::new()),
            tls: None,
            yields: false,
        },
        outbound,
    )
}

/// Accepts one connection within 10 s and gives the socket 10 s read and write timeouts.
fn accept_within_deadline(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return stream;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "no connection reached the fake primary within 10 s"
                );
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("fake primary accept failed: {error}"),
        }
    }
}

/// One fake primary connection: reads the handshake, answers `reply`, then hangs up.
fn answer_once(reply: Vec<u8>) -> (SocketAddr, JoinHandle<Handshake>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let mut stream = accept_within_deadline(&listener);
        let Ok(Incoming::Handshake(handshake)) = read_handshake(&mut stream) else {
            panic!("replica sent no current handshake");
        };

        let mut reply = reply;

        if read_u16(&reply, 8) != HandshakeStatus::Version as u16 {
            reply.extend_from_slice(&encode_address(None));
        }
        stream.write_all(&reply).unwrap();
        handshake
    });

    (address, server)
}

#[test]
fn snapshot_reply_leaves_the_upstream_record_until_the_snapshot_is_installed() {
    let directory = tempfile::tempdir().unwrap();

    record_upstream_history(
        directory.path(),
        0,
        &Origin {
            history: [1; HISTORY_LEN],
            epoch: 0,
        },
    )
    .unwrap();

    // The primary answers with a snapshot of another history, then the link breaks before
    // the snapshot arrives (as a crash during the download would).
    let (address, server) = answer_once(encode_reply_with_history(
        HandshakeStatus::Snapshot,
        10,
        &Origin {
            history: [9; HISTORY_LEN],
            epoch: 0,
        },
    ));
    let (link, _deliveries) = link_to(address, directory.path());
    let mut stream = connect_replication(address);
    let (retry, _) = read_primary(
        &mut stream,
        &link,
        &mut VecDeque::new(),
        &AtomicBool::new(false),
    );

    assert!(retry);
    assert_eq!(join_within(server).unwrap().version, VERSION_RESUME);
    assert_eq!(upstream_history(directory.path(), 0), [1; HISTORY_LEN]);

    // A resume the primary accepts continues its history and is recorded at once.
    let (address, server) = answer_once(encode_reply_with_history(
        HandshakeStatus::Ok,
        1,
        &Origin {
            history: [9; HISTORY_LEN],
            epoch: 0,
        },
    ));
    let (link, _deliveries) = link_to(address, directory.path());
    let mut stream = connect_replication(address);

    // The fake primary hangs up after the reply, which ends the stream.
    read_primary(
        &mut stream,
        &link,
        &mut VecDeque::new(),
        &AtomicBool::new(false),
    );
    join_within(server).unwrap();
    assert_eq!(upstream_history(directory.path(), 0), [9; HISTORY_LEN]);
}

#[test]
fn primary_refuses_older_and_newer_handshake_versions_at_once() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(idle_shipment(directory.path(), [7; HISTORY_LEN]));
    let accept_stop = Arc::clone(&stop);
    let primary = thread::spawn(move || accept_replicas(listener, shipment, accept_stop));

    // A build before the history id: its handshake ends after the password, and it reads a
    // reply without the history id.
    for legacy in [1u16, 3, 5] {
        let mut bytes = HANDSHAKE_MAGIC.to_vec();

        put_u16(&mut bytes, legacy);
        put_u32(&mut bytes, 0);
        put_u64(&mut bytes, 7);
        put_u16(&mut bytes, 0);
        append_checksum(&mut bytes);
        let mut stream = connect_replication(address);
        let started = std::time::Instant::now();

        stream.write_all(&bytes).unwrap();
        let reply = read_socket(&mut stream, LEGACY_REPLY_LEN);

        // The primary's handshake read gives up after 5 s.
        assert!(
            started.elapsed() < Duration::from_millis(2500),
            "refusal waited for a timeout"
        );
        assert_eq!(&reply[..8], HANDSHAKE_MAGIC);
        assert_eq!(read_u16(&reply, 8), HandshakeStatus::Version as u16);
        assert!(checksum_matches(&reply));
    }

    // A build with a history id but no epoch: its handshake ends with the history id and
    // shard count, and it reads a reply without the epoch.
    for history_only in [6u16, 8] {
        let mut bytes = HANDSHAKE_MAGIC.to_vec();

        put_u16(&mut bytes, history_only);
        put_u32(&mut bytes, 0);
        put_u64(&mut bytes, 7);
        put_u16(&mut bytes, 0);
        bytes.extend_from_slice(&[0; HISTORY_LEN]);
        put_u32(&mut bytes, 1);
        append_checksum(&mut bytes);
        let mut stream = connect_replication(address);
        let started = std::time::Instant::now();

        stream.write_all(&bytes).unwrap();
        let reply = read_socket(&mut stream, HISTORY_ONLY_REPLY_LEN);

        assert!(
            started.elapsed() < Duration::from_millis(2500),
            "refusal waited for a timeout"
        );
        assert_eq!(read_u16(&reply, 8), HandshakeStatus::Version as u16);
        assert!(checksum_matches(&reply));
    }

    // A build with an epoch but no client address: its handshake ends with the epoch, and it
    // reads this build's reply without the address after it.
    for epoch_only in [9u16, 11] {
        let mut bytes = HANDSHAKE_MAGIC.to_vec();

        put_u16(&mut bytes, epoch_only);
        put_u32(&mut bytes, 0);
        put_u64(&mut bytes, 7);
        put_u16(&mut bytes, 0);
        bytes.extend_from_slice(&[0; HISTORY_LEN]);
        put_u32(&mut bytes, 1);
        put_u64(&mut bytes, 0);
        append_checksum(&mut bytes);
        let mut stream = connect_replication(address);
        let started = std::time::Instant::now();

        stream.write_all(&bytes).unwrap();
        let reply = read_socket(&mut stream, REPLY_LEN);

        assert!(
            started.elapsed() < Duration::from_millis(2500),
            "refusal waited for a timeout"
        );
        assert_eq!(decode_reply(&reply).unwrap().0, HandshakeStatus::Version);
    }

    // A build whose stream had no control frames speaks this handshake: read in full, then
    // refused in this build's reply format.
    for no_control in [12u16, 14] {
        let mut stream = connect_replication(address);

        stream
            .write_all(
                &encode_handshake(&Handshake {
                    version: no_control,
                    shard_id: 0,
                    after_sequence: 0,
                    password: Vec::new(),
                    history: [0; HISTORY_LEN],
                    shard_count: 1,
                    epoch: 0,
                    announce: None,
                })
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            decode_reply(&read_reply_bytes(&mut stream)).unwrap().0,
            HandshakeStatus::Version
        );
    }

    // A newer build gets a refusal in this build's reply format.
    let mut stream = connect_replication(address);

    stream
        .write_all(
            &encode_handshake(&Handshake {
                version: VERSION_RESYNC_INCOMPLETE + 1,
                shard_id: 0,
                after_sequence: 0,
                password: Vec::new(),
                history: [0; HISTORY_LEN],
                shard_count: 1,
                epoch: 0,
                announce: None,
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        decode_reply(&read_reply_bytes(&mut stream)).unwrap().0,
        HandshakeStatus::Version
    );

    stop.store(true, Ordering::Release);
    join_within(primary).unwrap();
}

#[test]
fn replica_accept_loop_survives_accept_errors() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(idle_shipment(directory.path(), [7; HISTORY_LEN]));
    let accept_stop = Arc::clone(&stop);
    let primary = thread::spawn(move || {
        let mut failures = 3;

        accept_replicas_from(
            move || {
                if failures > 0 {
                    failures -= 1;

                    // What `accept` returns once the process is out of file descriptors.
                    return Err(io::Error::from_raw_os_error(24));
                }

                listener.accept()
            },
            shipment,
            accept_stop,
        )
    });
    let mut stream = connect_replication(address);

    stream
        .write_all(
            &encode_handshake(&Handshake {
                version: VERSION_RESUME,
                shard_id: 0,
                after_sequence: 0,
                password: Vec::new(),
                history: [0; HISTORY_LEN],
                shard_count: 1,
                epoch: 0,
                announce: None,
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        decode_reply(&read_reply_bytes(&mut stream)).unwrap().0,
        HandshakeStatus::Ok
    );
    assert!(!primary.is_finished());

    stop.store(true, Ordering::Release);
    drop(stream);
    join_within(primary).unwrap();
}

#[test]
fn a_replica_clock_ahead_of_the_primary_neither_loses_an_extended_key_nor_logs_its_own_deletes() {
    use crate::{
        engine::{Response, execute_with_sink},
        persistence::{DurabilityProfile, apply_mutation},
        protocol::parse_command,
        time::{Clock, SystemClock, TestClock, UnixMillis},
    };

    fn command(text: &str) -> crate::protocol::Command<'static> {
        let parts: Vec<&str> = text.split(' ').collect();
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n{part}\r\n", part.len()).bytes());
        }

        parse_command(&frame)
            .unwrap()
            .expect("complete")
            .0
            .into_owned()
    }

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let start = SystemClock::default().now_unix_millis().get();
    let primary_clock = TestClock::new(start);
    let mut primary = KvShard::with_clock(primary_clock.clone());
    let (mut primary_wal, _, _) = WalSink::open(config(primary_dir.path()), 0, limits).unwrap();

    primary.log_removals(true);
    // The replica's clock runs 5 s ahead of the primary's.
    let mut replica = KvShard::with_clock(SystemClock::with_floor(UnixMillis::new(start + 5_000)));
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    let (mut replica_wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();

    replica.follow_primary(true);
    let primary_last = std::cell::Cell::new(0);
    let mut run = |text: &str| -> Response<'static> {
        let reply = execute_with_sink(&mut primary, command(text), &mut primary_wal)
            .unwrap()
            .into_owned();

        primary_last.set(primary_wal.last_sequence());
        reply
    };
    let mut replicate = |replica: &mut KvShard| {
        let after = replica_wal.last_sequence();
        let FrameRead::Frames(frames) =
            read_frames(primary_dir.path(), 0, after, primary_last.get(), limits).unwrap()
        else {
            panic!("primary frames unavailable");
        };

        for frame in frames {
            let paused = replica.pause_expiration_for_replay();
            let (database, mutation, _) = accept_replica_frame(
                replica,
                &mut replica_wal,
                &manager,
                &replica_config,
                &frame,
                limits,
            )
            .expect("no divergence, no resync");

            apply_mutation(replica, database, &mutation).unwrap();
            replica.resume_expiration_after_replay(paused);
        }
        replica_wal.last_sequence()
    };
    let read = |replica: &mut KvShard, text: &str| {
        crate::engine::execute(replica, command(text)).into_owned()
    };

    run("SET k v PX 2000");
    run("SET g v PX 2000");
    replicate(&mut replica);

    // Past the deadline by the replica's clock only: hidden, not removed.
    assert_eq!(read(&mut replica, "GET k"), Response::Bulk(None));
    assert_eq!(read(&mut replica, "TTL k"), Response::Integer(-2));
    assert_eq!(read(&mut replica, "DBSIZE"), Response::Integer(2));

    // The primary extends k just before its own deadline.
    primary_clock.set(start + 1_900);
    assert_eq!(run("PEXPIRE k 60000"), Response::Integer(1));
    replicate(&mut replica);
    assert_eq!(
        read(&mut replica, "GET k"),
        Response::Bulk(Some(std::borrow::Cow::Borrowed(b"v")))
    );

    // g expires on the primary; its delete reaches the replica with the next write.
    primary_clock.set(start + 2_500);
    assert_eq!(run("GET g"), Response::Bulk(None));
    run("SET other x");
    let replicated = replicate(&mut replica);

    assert_eq!(read(&mut replica, "EXISTS g"), Response::Integer(0));
    assert_eq!(read(&mut replica, "DBSIZE"), Response::Integer(2));
    // The replica's log is exactly the primary's: it recorded no delete of its own.
    assert_eq!(replicated, primary_last.get());
    assert_eq!(replica.expiration_stats().expired_keys, 0);
}

#[test]
fn a_replica_checkpoint_holds_its_snapshot_point_while_frames_keep_arriving() {
    use crate::{
        engine::execute_with_sink,
        persistence::{DurabilityProfile, apply_mutation},
        protocol::parse_command,
        store::{SnapshotEntry, SnapshotStep},
    };

    fn command(text: &str) -> crate::protocol::Command<'static> {
        let parts: Vec<&str> = text.split(' ').collect();
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            frame.extend(format!("${}\r\n{part}\r\n", part.len()).bytes());
        }

        parse_command(&frame)
            .unwrap()
            .expect("complete")
            .0
            .into_owned()
    }

    fn sorted(mut entries: Vec<SnapshotEntry>) -> Vec<SnapshotEntry> {
        entries.sort_by(|left, right| {
            left.database
                .cmp(&right.database)
                .then_with(|| left.key.cmp(&right.key))
        });
        entries
    }

    let limits = ProtocolLimits::default();
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    };
    let mut primary = KvShard::new();
    let (mut primary_wal, _, _) = WalSink::open(config(primary_dir.path()), 0, limits).unwrap();
    let mut replica = KvShard::new();
    let replica_config = config(replica_dir.path());
    let manager = CheckpointManager::new(replica_config.clone(), limits);
    let (mut replica_wal, _, _) = WalSink::open(replica_config.clone(), 0, limits).unwrap();

    replica.follow_primary(true);
    let mut run = |text: &str| {
        execute_with_sink(&mut primary, command(text), &mut primary_wal).unwrap();
        primary_wal.last_sequence()
    };
    // As the replica's shard loop does: log each frame, then apply it.
    let mut replicate = |replica: &mut KvShard, through: u64| {
        let after = replica_wal.last_sequence();
        let FrameRead::Frames(frames) =
            read_frames(primary_dir.path(), 0, after, through, limits).unwrap()
        else {
            panic!("primary frames unavailable");
        };

        for frame in frames {
            let paused = replica.pause_expiration_for_replay();
            let (database, mutation, _) = accept_replica_frame(
                replica,
                &mut replica_wal,
                &manager,
                &replica_config,
                &frame,
                limits,
            )
            .expect("no divergence, no resync");

            apply_mutation(replica, database, &mutation).unwrap();
            replica.resume_expiration_after_replay(paused);
        }
        replica_wal.last_sequence()
    };

    let keys = 128;
    for index in 0..keys {
        run(&format!("RPUSH list:{index} a b"));
        run(&format!("XADD stream:{index} {}-1 f v", index + 1));
    }
    run("SET gone v");
    run("SET renamed v");
    run("SET moved v");
    let covered = replicate(&mut replica, run("HSET hash f v"));

    // A background checkpoint begins and streams part of the store into its file, as its
    // slices do.
    let generation = 1;

    manager.begin_round(generation).unwrap();
    let mut writer = manager.shard_writer(generation, 0, 1, covered).unwrap();
    replica.begin_snapshot(64 * 1024 * 1024).unwrap();
    let at_snapshot_point = sorted(replica.snapshot_entries());
    let mut buffer = Vec::new();
    let walked = std::cell::Cell::new(0u64);
    let mut emit = |entry: crate::store::WalkedEntry<'_>| {
        entry.encode_into(&mut buffer).unwrap();
        walked.set(walked.get() + 1);
    };

    while walked.get() == 0 {
        assert_eq!(replica.snapshot_step(1, &mut emit), SnapshotStep::More);
    }

    // Frames for keys the walk did and did not pass arrive before it ends.
    for index in 0..keys {
        run(&format!("RPUSH list:{index} c"));
        run(&format!("XADD stream:{index} {}-2 f w", index + 1));
    }
    run("DEL gone");
    run("RENAME renamed fresh");
    run("MOVE moved 3");
    let last = replicate(&mut replica, run("HSET hash g w"));

    while replica.snapshot_step(keys, &mut emit) != SnapshotStep::Done {}
    for entry in replica.finish_snapshot_journal() {
        crate::persistence::encode_entry(&mut buffer, &entry).unwrap();
        walked.set(walked.get() + 1);
    }
    writer.write_chunk(&buffer, walked.get()).unwrap();
    manager
        .commit_round(generation, &[writer.finish().unwrap()])
        .unwrap();

    let published = manager.load().unwrap().expect("published checkpoint");
    let [shard] = <[_; 1]>::try_from(published.shards)
        .ok()
        .expect("one shard");

    assert_eq!(shard.last_sequence, covered);
    let written = sorted(shard.entries);

    let differing: Vec<String> = written
        .iter()
        .filter(|entry| !at_snapshot_point.contains(entry))
        .chain(
            at_snapshot_point
                .iter()
                .filter(|entry| !written.contains(entry)),
        )
        .map(|entry| String::from_utf8_lossy(&entry.key).into_owned())
        .collect();

    assert!(
        differing.is_empty(),
        "the checkpoint does not hold these keys as they were at its snapshot point: {differing:?}"
    );

    // A restart loads the file and replays the log after the point it covers.
    let mut restarted = KvShard::new();

    restarted.follow_primary(true);
    for entry in written {
        restarted.restore_snapshot_entry(entry).unwrap();
    }
    let FrameRead::Frames(frames) =
        read_frames(replica_dir.path(), 0, covered, last, limits).unwrap()
    else {
        panic!("replica frames unavailable");
    };
    for frame in frames {
        let record = decode_record(&frame, 0, limits).unwrap();

        apply_mutation(&mut restarted, record.database, &record.mutation).unwrap();
    }
    assert_eq!(
        sorted(restarted.snapshot_entries()),
        sorted(replica.snapshot_entries())
    );
}

#[test]
fn a_promoted_shard_forks_from_the_history_it_followed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let followed = Origin {
        history: [4; HISTORY_LEN],
        epoch: 2,
    };

    record_upstream_history(path, 0, &followed).unwrap();
    let lineage = resolve_lineage(path, 0, 120).unwrap();

    assert_eq!(lineage.origin.epoch, 3);
    assert_ne!(lineage.origin.history, followed.history);
    assert_eq!(lineage.parent, followed.history);
    assert_eq!(lineage.fork_sequence, 120);
    // Settled once: a restart after more writes keeps the fork where it was.
    assert_eq!(resolve_lineage(path, 0, 500).unwrap(), lineage);

    // Siblings that stopped at or before the fork continue; one past it holds writes this
    // primary never saw, and an unrelated history continues nothing.
    assert!(lineage.continues(&followed.history, 1));
    assert!(lineage.continues(&followed.history, 120));
    assert!(!lineage.continues(&followed.history, 121));
    assert!(lineage.continues(&lineage.origin.history, 900));
    assert!(!lineage.continues(&[9; HISTORY_LEN], 1));

    // Following a primary again gives the lineage up; the next promotion forks anew.
    let newer = Origin {
        history: [5; HISTORY_LEN],
        epoch: 7,
    };

    record_upstream_history(path, 0, &newer).unwrap();
    assert!(!path.join(lineage_file_name(0)).exists());
    let again = resolve_lineage(path, 0, 300).unwrap();

    assert_eq!(
        (again.origin.epoch, again.parent, again.fork_sequence),
        (8, newer.history, 300)
    );
}

#[test]
fn a_shard_that_never_followed_continues_the_directory_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let history = load_or_create_history(path).unwrap();
    let lineage = resolve_lineage(path, 1, 40).unwrap();

    assert_eq!(lineage, Lineage::root(history));
    assert!(!lineage.continues(&[0; HISTORY_LEN], 1));
    // Upstream records of older builds carry no epoch.
    std::fs::write(path.join(upstream_file_name(2)), [3; HISTORY_LEN]).unwrap();
    assert_eq!(
        upstream_origin(path, 2),
        Origin {
            history: [3; HISTORY_LEN],
            epoch: 0
        }
    );
}

#[test]
fn a_primary_refuses_a_replica_that_followed_a_newer_epoch() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();

    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut shipment = idle_shipment(directory.path(), [7; HISTORY_LEN]);

    shipment.lineage.origin.epoch = 2;
    let newer_epoch = Arc::clone(&shipment.newer_epoch);
    let shipment = Arc::new(shipment);
    let accept_stop = Arc::clone(&stop);
    let primary = thread::spawn(move || accept_replicas(listener, shipment, accept_stop));
    let handshake = |epoch| Handshake {
        version: VERSION_RESYNC,
        shard_id: 0,
        after_sequence: 0,
        password: Vec::new(),
        history: [7; HISTORY_LEN],
        shard_count: 1,
        epoch,
        announce: None,
    };

    // A replica that followed the primary which replaced this one.
    let mut stream = connect_replication(address);

    stream
        .write_all(&encode_handshake(&handshake(3)).unwrap())
        .unwrap();
    assert_eq!(
        decode_reply(&read_reply_bytes(&mut stream)).unwrap().0,
        HandshakeStatus::Stale
    );
    // The shard reads it on its next maintenance tick and stops taking writes.
    assert_eq!(newer_epoch.load(Ordering::Acquire), 3);

    stop.store(true, Ordering::Release);
    join_within(primary).unwrap();
}

#[test]
fn a_replica_does_not_follow_a_primary_below_the_epoch_it_followed() {
    let directory = tempfile::tempdir().unwrap();
    let followed = Origin {
        history: [1; HISTORY_LEN],
        epoch: 5,
    };

    record_upstream_history(directory.path(), 0, &followed).unwrap();

    // A replaced primary (epoch 3) accepts the resume; the replica still does not follow it.
    let (address, server) = answer_once(encode_reply_with_history(
        HandshakeStatus::Ok,
        1,
        &Origin {
            history: [9; HISTORY_LEN],
            epoch: 3,
        },
    ));
    let (link, _deliveries) = link_to(address, directory.path());
    let mut stream = connect_replication(address);
    let (retry, _) = read_primary(
        &mut stream,
        &link,
        &mut VecDeque::new(),
        &AtomicBool::new(false),
    );
    let sent = join_within(server).unwrap();

    assert!(retry);
    assert_eq!((sent.history, sent.epoch), (followed.history, 5));
    assert_eq!(upstream_origin(directory.path(), 0), followed);
}

#[test]
fn fresh_counts_connected_replicas_that_acknowledged_within_the_lag() {
    let acks = DurableAcks::new();

    assert_eq!(acks.fresh(Duration::from_secs(10)), 0);
    acks.reserve(0);
    acks.reserve(1);
    // A repeated acknowledgement of the same sequence still counts as a sign of life.
    acks.observe(1, 0);
    assert_eq!(acks.fresh(Duration::from_secs(10)), 2);
    assert_eq!(acks.fresh(Duration::ZERO), 0);
    acks.clear(0);
    assert_eq!(acks.fresh(Duration::from_secs(10)), 1);
    assert_eq!(acks.reached(1), 0);
}

#[test]
fn handshake_and_address_frame_carry_the_client_address() {
    let endpoint = Endpoint {
        host: "replica-1.internal".into(),
        port: 7001,
    };
    let handshake = Handshake {
        version: VERSION_RESUME,
        shard_id: 1,
        after_sequence: 9,
        password: b"secret".to_vec(),
        history: [3; HISTORY_LEN],
        shard_count: 2,
        epoch: 4,
        announce: Some(endpoint.clone()),
    };
    let bytes = encode_handshake(&handshake).unwrap();

    assert_eq!(bytes.len(), 60 + 6 + endpoint.host.len());
    assert_eq!(decode_handshake(&bytes), Ok(handshake));

    let frame = encode_address(Some(&endpoint));

    assert_eq!(decode_address(&frame), Ok(Some(endpoint)));
    assert_eq!(decode_address(&encode_address(None)), Ok(None));
    let mut corrupt = encode_address(None);

    corrupt[0] ^= 1;
    assert!(decode_address(&corrupt).is_err());
}

#[test]
fn a_loopback_announcement_from_another_machine_is_listed_at_the_peer_address() {
    let announced = |host: &str| {
        Some(Endpoint {
            host: host.into(),
            port: 7001,
        })
    };
    let remote = Some("10.1.2.3".parse().unwrap());
    let local = Some("127.0.0.1".parse().unwrap());

    assert_eq!(
        reachable_endpoint(announced("127.0.0.1"), remote),
        announced("10.1.2.3")
    );
    assert_eq!(
        reachable_endpoint(announced("localhost"), remote),
        announced("10.1.2.3")
    );
    assert_eq!(
        reachable_endpoint(announced("127.0.0.1"), local),
        announced("127.0.0.1")
    );
    assert_eq!(
        reachable_endpoint(announced("db.example"), remote),
        announced("db.example")
    );
    assert_eq!(reachable_endpoint(None, remote), None);
}

#[test]
fn only_announced_replicas_that_acknowledged_are_listed() {
    let acks = DurableAcks::new();
    let endpoint = |port| Endpoint {
        host: "10.0.0.2".into(),
        port,
    };

    acks.reserve(0);
    acks.announce(0, Some(endpoint(7000)));
    acks.reserve(1);
    acks.announce(1, None);
    acks.reserve(2);
    acks.announce(2, Some(endpoint(7002)));
    // Still copying: no acknowledgement yet.
    assert!(acks.replicas().is_empty());

    acks.observe(0, 5);
    acks.observe(1, 5);
    let listed = acks.replicas();

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].endpoint, endpoint(7000));
    assert_eq!(listed[0].sequence, 5);

    acks.clear(0);
    assert!(acks.replicas().is_empty());

    assert_eq!(acks.primary(), None);
    acks.set_primary(Some(endpoint(6379)));
    assert_eq!(acks.primary(), Some(endpoint(6379)));
}

#[test]
fn dial_address_adds_the_shard_id_to_the_port_and_refuses_an_overflow() {
    let directory = tempfile::tempdir().unwrap();
    let primary: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let (mut link, _outbound) = link_to(primary, directory.path());

    link.primary_name = Some(("127.0.0.1".to_owned(), 7000));
    link.shard_id = 3;
    assert_eq!(dial_addresses(&link).unwrap()[0].port(), 7003);

    link.primary_name = Some(("127.0.0.1".to_owned(), 65_535));
    assert!(dial_addresses(&link).is_err());

    link.primary_name = Some(("127.0.0.1".to_owned(), 7000));
    link.shard_id = 70_000;
    assert!(dial_addresses(&link).is_err());
}

#[test]
fn a_primary_named_by_host_is_dialed_at_every_address_the_name_resolves_to() {
    let directory = tempfile::tempdir().unwrap();
    let (mut link, _deliveries) = link_to("127.0.0.1:1".parse().unwrap(), directory.path());

    link.primary_name = Some(("localhost".into(), 7000));
    let addresses = dial_addresses(&link).unwrap();

    // `localhost` usually resolves to ::1 first; a primary listening on 127.0.0.1 only must
    // still be reached, so the IPv4 address is among those tried.
    assert!(
        addresses
            .iter()
            .any(|address| address.ip().is_loopback() && address.port() == 7000),
        "{addresses:?}"
    );
    link.primary_name = None;
    assert_eq!(dial_addresses(&link).unwrap(), vec![link.primary]);
}

#[test]
fn a_replica_still_copying_sends_no_acknowledgement() {
    let directory = tempfile::tempdir().unwrap();
    let primary = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut replica_side = TcpStream::connect(primary.local_addr().unwrap()).unwrap();
    let (mut primary_side, _) = primary.accept().unwrap();
    let (link, _deliveries) = link_to(primary.local_addr().unwrap(), directory.path());
    let mut acks = AckSend {
        status: Arc::clone(&link.status),
        last: 0,
        sent_at: std::time::Instant::now() - ACK_HEARTBEAT,
        offset: 0,
        bytes: [0; ACK_LEN],
        queued: false,
    };

    link.status.set_copying(true);
    acks.poll(&mut replica_side, &link.wal);
    primary_side
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let mut byte = [0; 1];

    assert!(primary_side.read(&mut byte).is_err(), "an ack was sent");

    link.status.set_copying(false);
    acks.poll(&mut replica_side, &link.wal);
    assert_eq!(read_socket(&mut primary_side, ACK_LEN).len(), ACK_LEN);
}

#[test]
fn only_host_names_and_addresses_are_announced() {
    for host in [
        "10.0.0.1",
        "db-1.example.com",
        "fe80::1%en0",
        "[::1]",
        "replica_2",
    ] {
        assert!(announceable_host(host), "{host}");
    }

    for host in [
        "",
        "a b",
        "1.2.3.4\nx",
        "1.2.3.4\r\nrole:master",
        "h,ip=1",
        &"a".repeat(256),
    ] {
        assert!(!announceable_host(host), "{host:?}");
        assert_eq!(read_endpoint(7000, host.as_bytes()), Ok(None));
    }
}

/// Accepts at most `limit` bytes per write and counts the writes.
struct CountingWire {
    written: Vec<u8>,
    writes: usize,
    limit: usize,
}

impl Read for CountingWire {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl Write for CountingWire {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let accepted = bytes.len().min(self.limit);

        self.written.extend_from_slice(&bytes[..accepted]);
        self.writes += 1;
        Ok(accepted)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Wire for CountingWire {
    fn set_read_timeout(&self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    fn set_write_timeout(&self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    fn set_nodelay(&self, _: bool) -> io::Result<()> {
        Ok(())
    }

    fn set_nonblocking(&self, _: bool) -> io::Result<()> {
        Ok(())
    }

    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        Err(io::ErrorKind::NotConnected.into())
    }
}

#[test]
fn shipped_records_are_gathered_into_few_writes_in_order() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::DurabilityProfile,
    };

    let limits = ProtocolLimits::default();
    let dir = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: dir.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut wal, _, _) = WalSink::open(config, 0, limits).unwrap();
    let value = vec![b'v'; 1000];

    for index in 0..600u32 {
        wal.record(Mutation::SetBytes {
            key: &index.to_le_bytes(),
            value: &value,
        })
        .unwrap();
    }

    let FrameRead::Frames(frames) = read_frames(dir.path(), 0, 0, 600, limits).unwrap() else {
        panic!("WAL frames unavailable");
    };
    let expected = frames.concat();
    let floor = Arc::new(AtomicU64::new(u64::MAX));
    let progress = Arc::new(ReplicaProgress::new(
        Arc::clone(&floor),
        Arc::new(AtomicU64::new(0)),
        Arc::new(DurableAcks::new()),
    ));
    let hold = progress.hold(0);
    let stop = AtomicBool::new(false);
    let mut batch = Vec::new();

    for limit in [usize::MAX, 4096] {
        let mut wire = CountingWire {
            written: Vec::new(),
            writes: 0,
            limit,
        };
        let last = write_frames(&mut wire, &frames, &mut batch, &hold, &stop).unwrap();

        assert_eq!(frames.len(), 600);
        assert_eq!(last, Some(Some(600)));
        assert_eq!(floor.load(Ordering::Acquire), 600);
        assert!(wire.written == expected, "records out of order or lost");

        let batches = expected.len().div_ceil(SHIPPER_WRITE_BYTES);

        if limit == usize::MAX {
            assert!(wire.writes <= batches + 1, "{} writes", wire.writes);
        } else {
            assert!(wire.writes <= expected.len().div_ceil(limit) + batches + 1);
        }
    }

    stop.store(true, Ordering::Release);

    let mut wire = CountingWire {
        written: Vec::new(),
        writes: 0,
        limit: usize::MAX,
    };

    assert_eq!(
        write_frames(&mut wire, &frames, &mut batch, &hold, &stop).unwrap(),
        None
    );
    assert!(wire.written.is_empty());
}

#[test]
fn a_record_larger_than_a_shipper_write_is_written_on_its_own() {
    use crate::{
        mutation::{Mutation, MutationSink},
        persistence::DurabilityProfile,
    };

    let limits = ProtocolLimits::default();
    let dir = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: dir.path().to_path_buf(),
        ..PersistenceConfig::default()
    };
    let (mut wal, _, _) = WalSink::open(config, 0, limits).unwrap();
    let small = vec![b's'; 100];
    let large = vec![b'l'; SHIPPER_WRITE_BYTES];

    for (key, value) in [(b"a", &small), (b"b", &large), (b"c", &small)] {
        wal.record(Mutation::SetBytes { key, value }).unwrap();
    }

    let FrameRead::Frames(frames) = read_frames(dir.path(), 0, 0, 3, limits).unwrap() else {
        panic!("WAL frames unavailable");
    };
    let progress = Arc::new(ReplicaProgress::new(
        Arc::new(AtomicU64::new(u64::MAX)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(DurableAcks::new()),
    ));
    let hold = progress.hold(0);
    let mut wire = CountingWire {
        written: Vec::new(),
        writes: 0,
        limit: usize::MAX,
    };
    let mut batch = Vec::new();
    let last = write_frames(
        &mut wire,
        &frames,
        &mut batch,
        &hold,
        &AtomicBool::new(false),
    );

    assert_eq!(last.unwrap(), Some(Some(3)));
    assert!(
        wire.written == frames.concat(),
        "records out of order or lost"
    );
    assert_eq!(wire.writes, 3);
    assert!(batch.capacity() < SHIPPER_WRITE_BYTES);
}
