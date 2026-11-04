use tempfile::tempdir;

use super::*;

/// Value kinds 1-16 exist at snapshot minor 1. A reader that meets a later kind refuses
/// the file, so a new kind also needs a snapshot minor bump and a row here.
const LAST_SNAPSHOT_KIND: u8 = 16;

#[test]
fn every_value_kind_the_decoder_knows_is_counted() {
    let limits = ProtocolLimits::default();

    for kind in 0..=u8::MAX {
        let known = !matches!(
            decode_value(kind, &[], limits),
            Err(PersistenceError::Unsupported(_))
        );

        assert_eq!(
            known,
            (1..=LAST_SNAPSHOT_KIND).contains(&kind),
            "snapshot value kind {kind}: a new kind needs LAST_SNAPSHOT_KIND and a \
                 snapshot minor bump"
        );
    }
}

fn config(path: &Path) -> PersistenceConfig {
    PersistenceConfig {
        data_dir: path.to_path_buf(),
        snapshot_max_bytes: 1024 * 1024,
        ..PersistenceConfig::default()
    }
}

fn snapshot_config(path: &Path) -> PersistenceConfig {
    config(path)
}

#[test]
fn load_resync_bytes_returns_a_newer_checkpoint_and_refuses_an_older_one() {
    let directory = tempdir().unwrap();
    let config = snapshot_config(directory.path());
    let limits = ProtocolLimits::default();
    let manager = CheckpointManager::new(config, limits);

    manager
        .publish(
            1,
            &[
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 4,
                    entries: vec![SnapshotEntry {
                        database: 0,
                        key: b"k".to_vec(),
                        expires_at: None,
                        value: SnapshotValue::Bytes(b"kept".to_vec()),
                    }],
                },
                ShardCheckpoint {
                    shard_id: 1,
                    last_sequence: 4,
                    entries: Vec::new(),
                },
            ],
        )
        .unwrap();

    let newer = load_resync_bytes(directory.path(), limits, 1024 * 1024, 0, 1, 1, 4)
        .unwrap()
        .expect("checkpoint covers sequence 1");

    assert_eq!(newer.last_sequence, 4);
    assert!(newer.hot.is_some());
    assert!(newer.shard.starts_with(b"RUVSNP01"));
    assert!(
        load_resync_bytes(directory.path(), limits, 1024 * 1024, 0, 1, 5, 4)
            .unwrap()
            .is_none()
    );
}

#[test]
fn atomically_publishes_and_loads_a_checkpoint() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let checkpoint = ShardCheckpoint {
        shard_id: 0,
        last_sequence: 7,
        entries: vec![SnapshotEntry {
            database: 0,
            key: b"key".to_vec(),
            expires_at: Some(UnixMillis::new(42)),
            value: SnapshotValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
        }],
    };

    manager.publish(1, &[checkpoint]).unwrap();

    let loaded = manager.load().unwrap().unwrap();

    assert_eq!(loaded.generation, 1);
    assert_eq!(loaded.shards[0].last_sequence, 7);
    assert_eq!(loaded.shards[0].entries.len(), 1);
}

#[test]
fn atomic_write_replaces_a_leftover_temporary_file() {
    let directory = tempdir().unwrap();

    std::fs::write(directory.path().join(".CURRENT.tmp"), vec![b'x'; 4096]).unwrap();
    atomic_write_file(directory.path(), "CURRENT", b"fresh").unwrap();

    assert_eq!(
        std::fs::read(directory.path().join("CURRENT")).unwrap(),
        b"fresh"
    );
    assert!(!directory.path().join(".CURRENT.tmp").exists());
}

#[test]
fn plan_streams_only_the_requested_shard() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let entry = |key: &[u8]| SnapshotEntry {
        database: 0,
        key: key.to_vec(),
        expires_at: None,
        value: SnapshotValue::Bytes(b"v".to_vec()),
    };

    manager
        .publish(
            3,
            &[
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 5,
                    entries: vec![entry(b"a"), entry(b"b")],
                },
                ShardCheckpoint {
                    shard_id: 1,
                    last_sequence: 9,
                    entries: vec![entry(b"c")],
                },
            ],
        )
        .unwrap();

    let plan = manager.plan().unwrap().unwrap();
    let mut keys = Vec::new();

    assert_eq!(plan.generation, 3);
    assert_eq!(plan.shard_count(), 2);
    assert_eq!(plan.last_sequence(1), Some(9));
    assert_eq!(plan.last_sequence(7), None);
    plan.stream_shard(0, |entry| {
        keys.push(entry.key);
        Ok(())
    })
    .unwrap();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    assert!(plan.stream_shard(7, |_| Ok(())).is_err());
    assert!(
        CheckpointManager::new(config(tempdir().unwrap().path()), ProtocolLimits::default())
            .plan()
            .unwrap()
            .is_none()
    );
}

#[test]
fn streamed_recovery_reads_entries_across_buffer_boundaries_and_rejects_corruption() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path());

    config.snapshot_max_bytes = 64 * 1024 * 1024;
    let manager = CheckpointManager::new(config, ProtocolLimits::default());
    let large = vec![b'L'; 3 * SNAPSHOT_READ_BUFFER + 17];
    let mut entries: Vec<SnapshotEntry> = (0..20_000u32)
        .map(|n| SnapshotEntry {
            database: (n % 3) as u8,
            key: format!("key-{n}").into_bytes(),
            expires_at: (n % 7 == 0).then(|| UnixMillis::new(4_000_000_000_000)),
            value: SnapshotValue::Bytes(vec![b'v'; (n % 200) as usize]),
        })
        .collect();

    entries.insert(
        7_000,
        SnapshotEntry {
            database: 0,
            key: b"large".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(large.clone()),
        },
    );
    manager
        .publish(
            2,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 11,
                entries: entries.clone(),
            }],
        )
        .unwrap();

    let plan = manager.plan().unwrap().unwrap();
    let mut streamed = Vec::new();

    plan.stream_shard(0, |entry| {
        streamed.push(entry);
        Ok(())
    })
    .unwrap();
    assert_eq!(streamed.len(), entries.len());
    for (streamed, written) in streamed.iter().zip(&entries) {
        assert_eq!(streamed.key, written.key);
        assert_eq!(streamed.database, written.database);
        assert_eq!(streamed.expires_at, written.expires_at);
        assert!(matches!(
            (&streamed.value, &written.value),
            (SnapshotValue::Bytes(left), SnapshotValue::Bytes(right)) if left == right
        ));
    }

    // Limits lowered after the checkpoint was written do not make it unreadable.
    let tight = ProtocolLimits {
        max_value_bytes: 64,
        max_command_bytes: 1024,
        max_connection_buffer_bytes: 1024,
        ..ProtocolLimits::default()
    };
    let tight_config = PersistenceConfig {
        data_dir: directory.path().to_path_buf(),
        snapshot_max_bytes: 64 * 1024 * 1024,
        ..PersistenceConfig::default()
    };
    let tight_plan = CheckpointManager::new(tight_config, tight)
        .plan()
        .unwrap()
        .unwrap();
    let mut count = 0;

    tight_plan
        .stream_shard(0, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, entries.len());

    let file = directory
        .path()
        .join("checkpoints/00000000000000000002/shard-0000.snap");
    let mut bytes = fs::read(&file).unwrap();
    let middle = bytes.len() / 2;

    bytes[middle] ^= 1;
    fs::write(&file, &bytes).unwrap();
    let error = plan.stream_shard(0, |_| Ok(())).unwrap_err();

    assert!(error.to_string().contains("digest mismatch"), "{error}");
}

#[test]
fn structured_json_checkpoint_round_trips_and_restores_tree() {
    let mut shard = crate::store::KvShard::new();
    let value = serde_json::to_vec(&serde_json::json!({
        "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "nested": {"n": 1}
    }))
    .unwrap();
    shard.put_json(b"document", value).unwrap();
    let deadline = crate::time::UnixMillis::new(4_000_000_000_000);
    shard.expire_at(b"document", deadline);
    shard
        .apply_json_delta(b"document", b"$.nested.n", Some(b"2"), false)
        .unwrap();
    let checkpoint = ShardCheckpoint {
        shard_id: 0,
        last_sequence: 3,
        entries: shard.snapshot_entries(),
    };
    let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
    let (decoded, _) = decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
    let mut restored = crate::store::KvShard::new();
    for entry in decoded.entries {
        restored.restore_snapshot_entry(entry).unwrap();
    }
    assert!(restored.json_tree(b"document").is_some());
    assert_eq!(
        restored
            .json_tree(b"document")
            .unwrap()
            .get(b"$.nested.n")
            .unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(restored.expires_at(b"document"), Some(deadline));
    assert_eq!(restored.snapshot_entries(), shard.snapshot_entries());
}

#[test]
fn json_snapshot_round_trips_with_its_type_and_ttl() {
    let value = SnapshotValue::Json(br#"{"nested":[true,null,42]}"#.to_vec());
    let (kind, payload) = encode_value(&value).unwrap();
    assert_eq!(kind, 15);
    assert_eq!(
        decode_value(kind, &payload, ProtocolLimits::default()).unwrap(),
        value
    );
    let limits = ProtocolLimits {
        max_value_bytes: 8,
        ..ProtocolLimits::default()
    };
    assert!(decode_value(kind, &payload, limits).is_err());

    let entry = SnapshotEntry {
        database: 3,
        key: b"document".to_vec(),
        expires_at: Some(UnixMillis::new(4_000_000_000_000)),
        value,
    };
    let mut bytes = Vec::new();
    encode_entry(&mut bytes, &entry).unwrap();
    let (decoded, consumed) = decode_entry(&bytes, ProtocolLimits::default()).unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, entry);
}

#[test]
fn snapshot_entries_keep_their_database() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let entry = |database, value: &[u8]| SnapshotEntry {
        database,
        key: b"k".to_vec(),
        expires_at: None,
        value: SnapshotValue::Bytes(value.to_vec()),
    };

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 2,
                entries: vec![entry(0, b"zero"), entry(255, b"last")],
            }],
        )
        .unwrap();

    let loaded = manager.load().unwrap().unwrap();

    assert_eq!(
        loaded.shards[0].entries,
        vec![entry(0, b"zero"), entry(255, b"last")]
    );
}

#[test]
fn rejects_a_key_twice_in_one_database() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let entry = SnapshotEntry {
        database: 4,
        key: b"k".to_vec(),
        expires_at: None,
        value: SnapshotValue::Bytes(b"v".to_vec()),
    };

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 2,
                entries: vec![entry.clone(), entry],
            }],
        )
        .unwrap();

    assert!(matches!(
        manager.load(),
        Err(PersistenceError::Corruption(message)) if message.contains("duplicate key")
    ));
}

#[test]
fn keeps_previous_current_when_new_generation_is_incomplete() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            }],
        )
        .unwrap();
    fs::create_dir_all(directory.path().join("checkpoints/00000000000000000002")).unwrap();

    assert_eq!(manager.load().unwrap().unwrap().generation, 1);
}

#[test]
fn rejects_snapshot_corruption_before_returning_entries() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            }],
        )
        .unwrap();
    let path = directory
        .path()
        .join("checkpoints/00000000000000000001/shard-0000.snap");
    let mut bytes = fs::read(&path).unwrap();

    bytes[20] ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(matches!(
        manager.load(),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn keeps_the_previous_generation_until_a_newer_replacement_is_durable() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let empty = || ShardCheckpoint {
        shard_id: 0,
        last_sequence: 0,
        entries: Vec::new(),
    };

    manager.publish(1, &[empty()]).unwrap();
    manager.publish(2, &[empty()]).unwrap();
    assert!(
        directory
            .path()
            .join("checkpoints/00000000000000000001")
            .exists()
    );
    manager.publish(3, &[empty()]).unwrap();
    assert!(
        !directory
            .path()
            .join("checkpoints/00000000000000000001")
            .exists()
    );
    assert!(
        directory
            .path()
            .join("checkpoints/00000000000000000002")
            .exists()
    );
    assert_eq!(manager.load().unwrap().unwrap().generation, 3);
}

#[test]
fn truncated_current_pointer_is_rejected() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            }],
        )
        .unwrap();
    fs::write(directory.path().join("CURRENT"), b"short").unwrap();
    assert!(matches!(
        manager.load(),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn missing_manifest_fails_load() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager
        .publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            }],
        )
        .unwrap();
    fs::remove_file(
        directory
            .path()
            .join("checkpoints/00000000000000000001/MANIFEST"),
    )
    .unwrap();
    assert!(manager.load().is_err());
}

#[test]
fn rejects_snapshots_over_the_configured_size_limit() {
    let directory = tempdir().unwrap();
    let mut persistence = config(directory.path());

    persistence.snapshot_max_bytes = 4096;
    let manager = CheckpointManager::new(persistence, ProtocolLimits::default());
    let entries = vec![SnapshotEntry {
        database: 0,
        key: b"key".to_vec(),
        expires_at: None,
        value: SnapshotValue::Bytes(vec![0; 8192]),
    }];

    assert!(matches!(
        manager.publish(
            1,
            &[ShardCheckpoint {
                shard_id: 0,
                last_sequence: 1,
                entries,
            }],
        ),
        Err(PersistenceError::Failed(_))
    ));
    assert!(manager.load().unwrap().is_none());
}

#[test]
fn persists_a_monotonic_clock_floor() {
    let directory = tempdir().unwrap();
    let mut persistence = config(directory.path());

    persistence.profile = DurabilityProfile::Snapshot;
    let manager = CheckpointManager::new(persistence, ProtocolLimits::default());

    manager.persist_clock(UnixMillis::new(1_000)).unwrap();
    manager.persist_clock(UnixMillis::new(500)).unwrap();
    assert_eq!(manager.load_clock().unwrap(), Some(UnixMillis::new(1_000)));
    assert_eq!(
        manager
            .recovery_clock_floor(UnixMillis::new(2_000))
            .unwrap(),
        UnixMillis::new(2_000)
    );
    assert_eq!(
        manager.recovery_clock_floor(UnixMillis::new(100)).unwrap(),
        UnixMillis::new(1_000)
    );
}

#[test]
fn rejects_corrupt_clock_files() {
    let directory = tempdir().unwrap();
    let mut persistence = config(directory.path());

    persistence.profile = DurabilityProfile::Snapshot;
    let manager = CheckpointManager::new(persistence, ProtocolLimits::default());

    manager.persist_clock(UnixMillis::new(1)).unwrap();
    fs::write(directory.path().join("CLOCK"), b"not-a-clock-file!!!!").unwrap();
    assert!(matches!(
        manager.load_clock(),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn rejects_snapshot_filenames_outside_the_checkpoint_directory() {
    let manifest = encode_manifest(
        1,
        &[ManifestItem {
            shard_id: 0,
            last_sequence: 0,
            byte_length: 0,
            digest: [0; 32],
            file_name: "../etc/passwd".into(),
        }],
    )
    .unwrap();

    match decode_manifest(&manifest, 1) {
        Err(PersistenceError::Corruption(message)) => {
            assert!(message.contains("unsafe"), "{message}");
        }

        Err(error) => panic!("expected unsafe filename corruption, got {error}"),
        Ok(_) => panic!("expected unsafe filename corruption"),
    }
}

#[test]
fn stream_snapshot_keeps_the_assigned_id() {
    let mut shard = crate::store::KvShard::new();

    shard
        .stream_append(
            b"events",
            5,
            1,
            &[(b"field".as_slice(), b"value".as_slice())],
        )
        .unwrap();
    assert!(shard.stream_delete(b"events", 5, 1).unwrap());
    let checkpoint = ShardCheckpoint {
        shard_id: 0,
        last_sequence: 1,
        entries: shard.snapshot_entries(),
    };

    let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
    let (decoded, _) = decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
    let mut restored = crate::store::KvShard::new();

    restored
        .restore_snapshot_entry(decoded.entries.into_iter().next().unwrap())
        .unwrap();
    assert_eq!(restored.stream_len(b"events").unwrap(), 0);
    assert_eq!(restored.stream_next_id(b"events", 5).unwrap(), (5, 2));

    let entry = shard.snapshot_entries().into_iter().next().unwrap();
    let (kind, payload) = encode_value(&entry.value).unwrap();
    let legacy = decode_value(
        kind,
        &payload[..payload.len() - 4],
        ProtocolLimits::default(),
    )
    .unwrap();
    let crate::store::SnapshotValue::Stream {
        groups, entries, ..
    } = legacy
    else {
        panic!("expected a stream");
    };

    assert!(groups.is_empty());
    assert!(entries.is_empty());
}

#[test]
fn limit_snapshot_stores_the_count_and_reads_the_older_payload() {
    let (kind, payload) = encode_value(&SnapshotValue::Limit { used: 3 }).unwrap();

    assert_eq!((kind, payload.len()), (12, 8));
    assert_eq!(
        decode_value(kind, &payload, ProtocolLimits::default()).unwrap(),
        SnapshotValue::Limit { used: 3 }
    );

    let mut legacy = Vec::new();

    put_u64(&mut legacy, 3);
    put_u64(&mut legacy, 4_000_000_000_000);

    let value = decode_value(10, &legacy, ProtocolLimits::default()).unwrap();

    assert_eq!(value, SnapshotValue::Limit { used: 3 });

    let mut restored = crate::store::KvShard::new();

    restored
        .restore_snapshot_entry(crate::store::SnapshotEntry {
            database: 0,
            key: b"login".to_vec(),
            expires_at: Some(UnixMillis::new(4_000_000_000_000)),
            value,
        })
        .unwrap();
    assert_eq!(
        restored.value_kind(b"login"),
        Some(crate::store::ValueKind::Limit)
    );
    assert_eq!(
        restored.expires_at(b"login"),
        Some(UnixMillis::new(4_000_000_000_000))
    );
}

#[test]
fn a_hash_with_field_ttls_keeps_them_through_the_snapshot() {
    let mut shard = crate::store::KvShard::new();

    shard
        .hash_set(b"h", &[(b"a".as_slice(), b"1".as_slice()), (b"b", b"2")])
        .unwrap();
    shard
        .hash_expire_fields(b"h", &[b"a"], 4_000_000_000_000)
        .unwrap();
    shard
        .hash_set(b"plain", &[(b"f".as_slice(), b"v".as_slice())])
        .unwrap();

    let mut restored = crate::store::KvShard::new();

    for entry in shard.snapshot_entries() {
        let (kind, payload) = encode_value(&entry.value).unwrap();

        assert_eq!(kind, if entry.key == b"h" { 13 } else { 6 });

        let mut round = entry;

        round.value = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();
        restored.restore_snapshot_entry(round).unwrap();
    }

    assert_eq!(
        restored.field_deadline(b"h", b"a"),
        Ok(Some(4_000_000_000_000))
    );
    assert_eq!(restored.field_deadline(b"h", b"b"), Ok(None));
    assert_eq!(restored.hash_get(b"plain", b"f"), Ok(Some(&b"v"[..])));
    assert!(restored.has_field_ttls());
}

#[test]
fn a_semaphore_keeps_its_live_holders_through_the_snapshot() {
    let value = SnapshotValue::Semaphore {
        next_token: 7,
        holders: vec![(5, 4_000_000_000_000), (6, 1)],
    };
    let (kind, payload) = encode_value(&value).unwrap();

    assert_eq!((kind, payload.len()), (14, 8 + 4 + 32));

    let decoded = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();

    assert_eq!(decoded, value);

    let mut restored = crate::store::KvShard::new();

    restored
        .restore_snapshot_entry(crate::store::SnapshotEntry {
            database: 0,
            key: b"jobs".to_vec(),
            expires_at: Some(UnixMillis::new(4_000_000_000_000)),
            value: decoded,
        })
        .unwrap();
    assert!(restored.semaphore_holds(b"jobs", 5));
    assert!(
        !restored.semaphore_holds(b"jobs", 6),
        "an expired holder is dropped"
    );
}

#[test]
fn delayed_stream_entry_survives_the_snapshot() {
    let mut shard = crate::store::KvShard::new();

    shard
        .stream_append(
            b"events",
            5,
            1,
            &[(b"field".as_slice(), b"value".as_slice())],
        )
        .unwrap();
    shard.stream_hold_until(b"events", 5, 1, 9_000).unwrap();

    let entry = shard.snapshot_entries().into_iter().next().unwrap();
    let (kind, payload) = encode_value(&entry.value).unwrap();
    let decoded = decode_value(kind, &payload, ProtocolLimits::default()).unwrap();
    let mut round = entry;

    round.value = decoded;

    let mut restored = crate::store::KvShard::new();

    restored.restore_snapshot_entry(round).unwrap();

    let again = restored.snapshot_entries().into_iter().next().unwrap();
    let crate::store::SnapshotValue::Stream { delayed, .. } = again.value else {
        panic!("expected a stream");
    };

    assert_eq!(delayed, vec![(5, 1, 9_000)]);
}

#[test]
fn stream_group_cursor_survives_the_snapshot() {
    let mut shard = crate::store::KvShard::new();

    shard
        .stream_append(
            b"events",
            5,
            1,
            &[(b"field".as_slice(), b"value".as_slice())],
        )
        .unwrap();
    shard
        .stream_create_group(b"events", b"g", 0, 0, false)
        .unwrap();
    assert_eq!(
        shard.stream_group_new(b"events", b"g", 10).unwrap().len(),
        1
    );
    shard
        .stream_group_claim(b"events", b"g", b"c1", 5, 1, 1_000)
        .unwrap();

    let checkpoint = ShardCheckpoint {
        shard_id: 0,
        last_sequence: 1,
        entries: shard.snapshot_entries(),
    };

    let bytes = encode_snapshot(1, 1, &checkpoint).unwrap();
    let (decoded, _) = decode_snapshot(&bytes, 1, 0, ProtocolLimits::default(), u64::MAX).unwrap();
    let mut restored = crate::store::KvShard::new();

    restored
        .restore_snapshot_entry(decoded.entries.into_iter().next().unwrap())
        .unwrap();
    assert!(
        restored
            .stream_group_new(b"events", b"g", 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        restored
            .stream_group_pending(b"events", b"g", 5, 1)
            .unwrap()
    );
    assert!(restored.stream_group_ack(b"events", b"g", 5, 1).unwrap());
    assert!(
        !restored
            .stream_group_pending(b"events", b"g", 5, 1)
            .unwrap()
    );
}

#[test]
fn version_one_meta_and_snapshot_fixtures_remain_stable() {
    assert_eq!(encode_current(1), load_hex_fixture("current.hex"));
    assert_eq!(decode_current(&load_hex_fixture("current.hex")).unwrap(), 1);
    assert_eq!(
        encode_clock(1_700_000_000_000),
        load_hex_fixture("clock.hex")
    );
    assert_eq!(
        decode_clock(&load_hex_fixture("clock.hex")).unwrap(),
        1_700_000_000_000
    );

    let empty = empty_snapshot();

    assert_eq!(
        encode_snapshot_at(1, 1, &empty, 1_700_000_000_000).unwrap(),
        load_hex_fixture("snapshot-empty.hex")
    );
    let (decoded_empty, created) = decode_snapshot(
        &load_hex_fixture("snapshot-empty.hex"),
        1,
        0,
        ProtocolLimits::default(),
        1024 * 1024,
    )
    .unwrap();

    assert_eq!(created, 1_700_000_000_000);
    assert!(decoded_empty.entries.is_empty());

    let populated = populated_snapshot();
    let encoded = encode_snapshot_at(1, 1, &populated, 1_700_000_000_000).unwrap();

    assert_eq!(encoded, load_hex_fixture("snapshot-all-types.hex"));
    let (decoded, _) =
        decode_snapshot(&encoded, 1, 0, ProtocolLimits::default(), 1024 * 1024).unwrap();

    assert_eq!(decoded.entries.len(), populated.entries.len());

    let manifest = encode_manifest(
        1,
        &[ManifestItem {
            shard_id: 0,
            last_sequence: populated.last_sequence,
            byte_length: encoded.len() as u64,
            digest: *blake3::hash(&encoded).as_bytes(),
            file_name: "shard-0000.snap".into(),
        }],
    )
    .unwrap();

    assert_eq!(manifest, load_hex_fixture("manifest.hex"));
    assert_eq!(decode_manifest(&manifest, 1).unwrap().len(), 1);

    let mut shard = crate::KvShard::new();

    for entry in decoded.entries {
        shard.restore_snapshot_entry(entry).unwrap();
    }

    assert_eq!(shard.get_bytes(b"bytes").unwrap(), Some(b"v".as_slice()));
    assert_eq!(shard.list_len(b"list").unwrap(), 2);
    assert_eq!(shard.set_card(b"set").unwrap(), 1);
    assert_eq!(
        shard.hash_get(b"hash", b"f").unwrap(),
        Some(b"v".as_slice())
    );
    assert_eq!(shard.sorted_set_score(b"zset", b"m").unwrap(), Some(1.5));
    assert!(shard.bloom_contains(b"bloom", b"m").is_ok());
    assert!(shard.get_bytes(b"ttl").unwrap().is_none());
}

#[test]
fn frozen_v1_directory_opens_as_a_prior_release() {
    let directory = tempfile::tempdir().unwrap();
    let generation = directory
        .path()
        .join("checkpoints")
        .join(format!("{:020}", 1u64));

    std::fs::create_dir_all(&generation).unwrap();
    std::fs::write(
        directory.path().join("CURRENT"),
        load_hex_fixture("current.hex"),
    )
    .unwrap();
    std::fs::write(
        directory.path().join("CLOCK"),
        load_hex_fixture("clock.hex"),
    )
    .unwrap();
    std::fs::write(
        generation.join("MANIFEST"),
        load_hex_fixture("manifest.hex"),
    )
    .unwrap();
    std::fs::write(
        generation.join("shard-0000.snap"),
        load_hex_fixture("snapshot-all-types.hex"),
    )
    .unwrap();

    let checkpoints = CheckpointManager::new(
        PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        },
        ProtocolLimits::default(),
    );
    let checkpoint = checkpoints.load().unwrap().unwrap();
    let mut shard =
        crate::KvShard::with_clock(crate::SystemClock::with_floor(checkpoint.created_at));

    for entry in checkpoint.shards.into_iter().next().unwrap().entries {
        shard.restore_snapshot_entry(entry).unwrap();
    }

    assert_eq!(shard.get_bytes(b"bytes").unwrap(), Some(b"v".as_slice()));
    assert_eq!(shard.list_len(b"list").unwrap(), 2);
    assert_eq!(shard.set_card(b"set").unwrap(), 1);
    assert_eq!(
        shard.hash_get(b"hash", b"f").unwrap(),
        Some(b"v".as_slice())
    );
    assert_eq!(shard.sorted_set_score(b"zset", b"m").unwrap(), Some(1.5));
    assert!(shard.bloom_contains(b"bloom", b"m").is_ok());
    assert!(shard.get_bytes(b"ttl").unwrap().is_none());
}

#[test]
fn writes_v1_snapshot_fixtures_when_requested() {
    if std::env::var_os("RUVIO_WRITE_FIXTURES").is_none() {
        return;
    }

    write_hex_fixture("current.hex", &encode_current(1));
    write_hex_fixture("clock.hex", &encode_clock(1_700_000_000_000));
    let empty = empty_snapshot();

    write_hex_fixture(
        "snapshot-empty.hex",
        &encode_snapshot_at(1, 1, &empty, 1_700_000_000_000).unwrap(),
    );
    let populated = populated_snapshot();
    let snapshot = encode_snapshot_at(1, 1, &populated, 1_700_000_000_000).unwrap();

    write_hex_fixture("snapshot-all-types.hex", &snapshot);
    write_hex_fixture(
        "manifest.hex",
        &encode_manifest(
            1,
            &[ManifestItem {
                shard_id: 0,
                last_sequence: populated.last_sequence,
                byte_length: snapshot.len() as u64,
                digest: *blake3::hash(&snapshot).as_bytes(),
                file_name: "shard-0000.snap".into(),
            }],
        )
        .unwrap(),
    );
}

fn empty_snapshot() -> ShardCheckpoint {
    ShardCheckpoint {
        shard_id: 0,
        last_sequence: 0,
        entries: Vec::new(),
    }
}

#[test]
fn a_scaled_bloom_filter_round_trips_and_a_legacy_one_still_loads() {
    let mut shard = crate::KvShard::new();

    for index in 0..2_000 {
        shard
            .bloom_add(b"layered", format!("item-{index}").as_bytes())
            .unwrap();
    }

    let before = shard.bloom_info(b"layered").unwrap().unwrap();

    assert!(before.filters > 1);

    let mut restored = crate::KvShard::new();

    for entry in shard.snapshot_entries() {
        let mut bytes = Vec::new();

        encode_entry(&mut bytes, &entry).unwrap();

        let (decoded, _) = decode_entry(&bytes, ProtocolLimits::default()).unwrap();

        assert_eq!(decoded, entry);
        restored.restore_snapshot_entry(decoded).unwrap();
    }

    assert_eq!(restored.bloom_info(b"layered").unwrap().unwrap(), before);

    for index in (0..2_000).step_by(37) {
        assert!(
            restored
                .bloom_contains(b"layered", format!("item-{index}").as_bytes())
                .unwrap()
        );
    }

    // A filter written before layers existed: capacity and item count are rebuilt from the bits.
    let mut old = crate::KvShard::new();
    let mut words = vec![0u64; 16];

    for index in 0..40u64 {
        let (first, second) = (
            index.wrapping_mul(0x9e37_79b9_7f4a_7c15u64),
            0x1234_5679u64 | 1,
        );
        let mut position = first;

        for _ in 0..7 {
            let bit = (position % 1024) as usize;

            words[bit / 64] |= 1 << (bit % 64);
            position = position.wrapping_add(second);
        }
    }

    old.restore_snapshot_entry(SnapshotEntry {
        database: 0,
        key: b"old".to_vec(),
        expires_at: None,
        value: SnapshotValue::BloomFilter {
            words,
            hash_count: 7,
        },
    })
    .unwrap();

    let info = old.bloom_info(b"old").unwrap().unwrap();

    assert_eq!(info.filters, 1);
    assert!(info.capacity > 0 && info.items > 0, "{info:?}");
    assert_eq!(info.expansion, Some(2), "an old filter now scales");
}

fn populated_snapshot() -> ShardCheckpoint {
    ShardCheckpoint {
        shard_id: 0,
        last_sequence: 9,
        entries: vec![
            SnapshotEntry {
                database: 0,
                key: b"bytes".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"v".to_vec()),
            },
            SnapshotEntry {
                database: 0,
                key: b"list".to_vec(),
                expires_at: None,
                value: SnapshotValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
            },
            SnapshotEntry {
                database: 0,
                key: b"set".to_vec(),
                expires_at: None,
                value: SnapshotValue::Set(vec![b"a".to_vec()]),
            },
            SnapshotEntry {
                database: 0,
                key: b"hash".to_vec(),
                expires_at: None,
                value: SnapshotValue::Hash(vec![(b"f".to_vec(), b"v".to_vec())]),
            },
            SnapshotEntry {
                database: 0,
                key: b"zset".to_vec(),
                expires_at: None,
                value: SnapshotValue::SortedSet(vec![(1.5, b"m".to_vec())]),
            },
            SnapshotEntry {
                database: 0,
                key: b"bloom".to_vec(),
                expires_at: None,
                value: SnapshotValue::BloomFilter {
                    words: vec![1],
                    hash_count: 2,
                },
            },
            SnapshotEntry {
                database: 0,
                key: b"ttl".to_vec(),
                expires_at: Some(UnixMillis::new(1)),
                value: SnapshotValue::Bytes(b"expired".to_vec()),
            },
        ],
    }
}

fn fixture_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persistence/v1")
}

fn load_hex_fixture(name: &str) -> Vec<u8> {
    let text = std::fs::read_to_string(fixture_dir().join(name))
        .unwrap_or_else(|error| panic!("missing v1 fixture {name}: {error}"));
    let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();

    compact
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn write_hex_fixture(name: &str, bytes: &[u8]) {
    let directory = fixture_dir();

    std::fs::create_dir_all(&directory).unwrap();
    let mut text = String::with_capacity(bytes.len() * 2 + 1);

    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }

    text.push('\n');
    std::fs::write(directory.join(name), text).unwrap();
}
fn sample_entries() -> Vec<SnapshotEntry> {
    vec![
        SnapshotEntry {
            database: 0,
            key: b"a".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(b"one".to_vec()),
        },
        SnapshotEntry {
            database: 2,
            key: b"set".to_vec(),
            expires_at: Some(UnixMillis::new(99)),
            value: SnapshotValue::Set(vec![b"x".to_vec(), b"y".to_vec()]),
        },
    ]
}

fn streamed_file(
    directory: &Path,
    entries: &[SnapshotEntry],
    maximum: u64,
) -> Result<ShardFile, PersistenceError> {
    std::fs::create_dir_all(directory)?;
    let mut writer = SnapshotWriter::create_at(directory, 1, 0, 1, 7, maximum, 1_000)?;

    for chunk in entries.chunks(1) {
        let mut bytes = Vec::new();

        for entry in chunk {
            encode_entry(&mut bytes, entry)?;
        }

        writer.write_chunk(&bytes, chunk.len() as u64)?;
    }

    writer.finish()
}

#[test]
fn streamed_snapshot_loads_through_the_manager() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager.begin_round(1).unwrap();
    let mut writer = manager.shard_writer(1, 0, 1, 7).unwrap();
    let entries = sample_entries();
    let mut bytes = Vec::new();

    for entry in &entries {
        encode_entry(&mut bytes, entry).unwrap();
    }

    writer.write_chunk(&bytes, entries.len() as u64).unwrap();
    let file = writer.finish().unwrap();

    manager.commit_round(1, &[file]).unwrap();
    let loaded = manager.load().unwrap().expect("checkpoint");

    assert_eq!(loaded.generation, 1);
    assert_eq!(loaded.shards[0].last_sequence, 7);
    assert_eq!(loaded.shards[0].entries, entries);
}

#[test]
fn streamed_snapshot_matches_the_fixture_and_rejects_damage() {
    let directory = tempdir().unwrap();
    let file = streamed_file(directory.path(), &sample_entries(), 1 << 20).unwrap();
    let bytes = std::fs::read(directory.path().join(&file.file_name)).unwrap();

    if std::env::var_os("RUVIO_WRITE_FIXTURES").is_some() {
        write_hex_fixture("snapshot-streamed.hex", &bytes);
    }

    assert_eq!(bytes, load_hex_fixture("snapshot-streamed.hex"));
    assert_eq!(file.byte_length, bytes.len() as u64);
    assert_eq!(&file.digest, blake3::hash(&bytes).as_bytes());
    assert_eq!(u16_at(&bytes, 10).unwrap(), 1);
    assert_eq!(u64_at(&bytes, 40).unwrap(), u64::MAX);

    let limits = ProtocolLimits::default();
    let (decoded, created_at) = decode_snapshot(&bytes, 1, 0, limits, 1 << 20).unwrap();

    assert_eq!(decoded.entries, sample_entries());
    assert_eq!(created_at, 1_000);

    // A header count other than the sentinel on a minor 1 file is damage.
    let mut counted = bytes.clone();

    counted[40..48].copy_from_slice(&2u64.to_le_bytes());
    let checksum = crc32c::crc32c(&counted[..60]);

    counted[60..64].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_snapshot(&counted, 1, 0, limits, 1 << 20).is_err());

    // Footer count that disagrees with the entries present.
    let mut short = bytes.clone();
    let footer = short.len() - SNAPSHOT_FOOTER_LEN;

    short[footer + 8..footer + 16].copy_from_slice(&3u64.to_le_bytes());
    assert!(decode_snapshot(&short, 1, 0, limits, 1 << 20).is_err());

    // A minor from the future is refused.
    let mut future = bytes;

    future[10..12].copy_from_slice(&2u16.to_le_bytes());
    assert!(decode_snapshot(&future, 1, 0, limits, 1 << 20).is_err());
}

#[test]
fn streamed_writer_enforces_the_size_limit_and_abort_removes_the_file() {
    let directory = tempdir().unwrap();
    let error = streamed_file(directory.path(), &sample_entries(), 120).unwrap_err();

    assert!(error.to_string().contains("size limit"));

    let mut writer =
        SnapshotWriter::create_at(directory.path(), 1, 0, 1, 7, 1 << 20, 1_000).unwrap();
    let temporary = directory.path().join(".shard-0000.snap.tmp");

    writer.write_chunk(&[], 0).unwrap();
    assert!(temporary.exists());
    writer.abort();
    assert!(!temporary.exists());
}

#[test]
fn a_large_snapshot_is_synced_in_steps_while_it_is_written() {
    // A file synced only at the end leaves the whole shard to one flush, and a WAL sync on
    // the same disk waits behind it. Each step's bytes must be on disk before the next.
    let directory = tempdir().unwrap();
    let value = vec![7u8; 64 << 10];
    let entry = |index: usize| SnapshotEntry {
        database: 0,
        key: format!("key:{index}").into_bytes(),
        expires_at: None,
        value: SnapshotValue::Bytes(value.clone()),
    };
    let count = (3 * SNAPSHOT_SYNC_STEP_BYTES as usize) / value.len() + 8;
    let mut writer =
        SnapshotWriter::create_at(directory.path(), 1, 0, 1, 7, u64::MAX, 1_000).unwrap();

    for index in 0..count {
        let mut bytes = Vec::new();

        encode_entry(&mut bytes, &entry(index)).unwrap();
        writer.write_chunk(&bytes, 1).unwrap();
        assert!(
            writer.unsynced() < SNAPSHOT_SYNC_STEP_BYTES,
            "{} bytes written since the last sync",
            writer.unsynced()
        );
    }

    let file = writer.finish().unwrap();
    let bytes = std::fs::read(directory.path().join(&file.file_name)).unwrap();
    let limits = ProtocolLimits::default();
    let (decoded, _) = decode_snapshot(&bytes, 1, 0, limits, u64::MAX).unwrap();

    assert!(step_syncs(directory.path()) >= 3);
    assert_eq!(decoded.entries.len(), count);
    assert_eq!(decoded.entries[count - 1], entry(count - 1));
}

#[test]
fn concurrent_clock_writes_never_fail_and_keep_the_highest_floor() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path());

    config.profile = DurabilityProfile::Snapshot;
    let manager = std::sync::Arc::new(CheckpointManager::new(config, ProtocolLimits::default()));
    let writers: Vec<_> = (0..8u64)
        .map(|writer| {
            let manager = std::sync::Arc::clone(&manager);

            std::thread::spawn(move || {
                for round in 0..50u64 {
                    manager
                        .persist_clock(UnixMillis::new(1_000 + round * 8 + writer))
                        .unwrap();
                }
            })
        })
        .collect();

    for writer in writers {
        writer.join().unwrap();
    }
    assert_eq!(
        manager.load_clock().unwrap(),
        Some(UnixMillis::new(1_000 + 49 * 8 + 7))
    );
}

#[test]
fn startup_sweep_removes_leftover_temporary_files_only() {
    let directory = tempdir().unwrap();
    let generation = directory.path().join("checkpoints/00000000000000000003");

    std::fs::create_dir_all(&generation).unwrap();
    for path in [
        directory.path().join(".CLOCK.tmp"),
        generation.join(".shard-0000.snap.tmp"),
        generation.join("shard-0000.snap"),
        directory.path().join("CURRENT"),
    ] {
        std::fs::write(path, b"x").unwrap();
    }

    let mut config = config(directory.path());

    config.profile = DurabilityProfile::Snapshot;
    let _lock = super::super::DataDirectoryLock::acquire(&config).unwrap();

    assert!(!directory.path().join(".CLOCK.tmp").exists());
    assert!(!generation.join(".shard-0000.snap.tmp").exists());
    assert!(generation.join("shard-0000.snap").exists());
    assert!(directory.path().join("CURRENT").exists());
}

#[test]
fn a_round_that_failed_after_current_never_reuses_its_generation() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path());

    config.profile = DurabilityProfile::Snapshot;
    let manager = CheckpointManager::new(config.clone(), ProtocolLimits::default());
    let shard = |last_sequence| ShardCheckpoint {
        shard_id: 0,
        last_sequence,
        entries: vec![SnapshotEntry {
            database: 0,
            key: b"k".to_vec(),
            expires_at: None,
            value: SnapshotValue::Bytes(b"v".to_vec()),
        }],
    };

    manager.publish(1, &[shard(1)]).unwrap();
    let generation = manager.allocate_generation(1).unwrap();

    assert_eq!(generation, 2);
    manager.begin_round(generation).unwrap();
    let file = manager.write_shard_file(generation, 1, &shard(2)).unwrap();

    // CURRENT is renamed, then the directory sync fails: the caller sees an error.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Manifest);
    assert!(manager.commit_round(generation, &[file]).is_err());

    // A caller whose published counter still says 1 must not clear what CURRENT names.
    assert!(manager.begin_round(2).is_err());
    assert_eq!(manager.allocate_generation(1).unwrap(), 3);
    assert_eq!(manager.plan().unwrap().unwrap().generation, 2);
    manager
        .plan()
        .unwrap()
        .unwrap()
        .stream_shard(0, |_| Ok(()))
        .unwrap();
    // After a restart too.
    assert_eq!(
        CheckpointManager::new(config, ProtocolLimits::default())
            .allocate_generation(0)
            .unwrap(),
        3
    );
}

#[test]
fn begin_round_discards_a_stale_directory_of_the_same_generation() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager.begin_round(5).unwrap();
    let stale = directory
        .path()
        .join("checkpoints/00000000000000000005/leftover");

    std::fs::write(&stale, b"x").unwrap();
    manager.begin_round(5).unwrap();
    assert!(!stale.exists());
}

#[test]
fn an_abandoned_round_directory_is_removed_but_a_published_one_is_kept() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());
    let generation = manager.allocate_generation(0).unwrap();

    manager.begin_round(generation).unwrap();
    let file = manager
        .write_shard_file(
            generation,
            1,
            &ShardCheckpoint {
                shard_id: 0,
                last_sequence: 0,
                entries: Vec::new(),
            },
        )
        .unwrap();
    manager.commit_round(generation, &[file]).unwrap();

    let abandoned = manager.allocate_generation(generation).unwrap();
    manager.begin_round(abandoned).unwrap();
    manager.discard_round(abandoned).unwrap();
    manager.discard_round(generation).unwrap();

    assert!(!manager.generation_directory(abandoned).exists());
    assert!(manager.generation_directory(generation).exists());
    assert!(manager.load().unwrap().is_some());
}

/// A copy that fails (here, the source does not match its digest) leaves no partial
/// file in the new round.
#[test]
fn a_failed_carry_leaves_no_temporary_file() {
    let directory = tempdir().unwrap();
    let manager = CheckpointManager::new(config(directory.path()), ProtocolLimits::default());

    manager.begin_round(1).unwrap();
    let file = manager
        .write_shard_file(
            1,
            1,
            &ShardCheckpoint {
                shard_id: 0,
                last_sequence: 7,
                entries: Vec::new(),
            },
        )
        .unwrap();
    manager.commit_round(1, &[file]).unwrap();
    let source = manager.generation_directory(1).join("shard-0000.snap");
    let mut bytes = std::fs::read(&source).unwrap();
    let digest = bytes.len() - 10;

    bytes[digest] ^= 0xff;
    std::fs::write(&source, &bytes).unwrap();
    manager.begin_round(2).unwrap();

    assert!(manager.carry_shard_file(1, 2, 0).is_err());
    let left: Vec<_> = std::fs::read_dir(manager.generation_directory(2))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(left.is_empty(), "{left:?}");
}
