use super::*;

#[test]
fn copy_on_write_snapshot_preserves_pre_mutation_values() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"keep", b"original");
    shard.set_bytes(b"mutate", b"before");
    shard.begin_snapshot(1024).unwrap();
    shard.capture_snapshot_key(b"mutate");
    shard.set_bytes(b"mutate", b"after");
    shard.capture_snapshot_key(b"created");
    shard.set_bytes(b"created", b"new");
    shard.capture_snapshot_key(b"keep");
    shard.delete(b"keep");
    let snapshot = shard.finish_snapshot();

    assert_eq!(
        snapshot,
        vec![
            SnapshotEntry {
                database: 0,
                key: b"keep".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"original".to_vec()),
            },
            SnapshotEntry {
                database: 0,
                key: b"mutate".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"before".to_vec()),
            },
        ]
    );
    assert_eq!(shard.get(b"mutate"), Some(&b"after"[..]));
    assert_eq!(shard.get(b"created"), Some(&b"new"[..]));
    assert!(shard.get(b"keep").is_none());
    assert!(
        shard
            .snapshot_entries()
            .iter()
            .any(|entry| entry.key == b"created")
    );
}

#[test]
fn snapshot_captured_keys_only_shadow_their_own_database() {
    let mut shard = KvShard::new();
    shard.set_bytes(b"shared", b"db0-before");
    shard.set_bytes(b"uncaptured", b"db0");
    shard.enter(1);
    shard.set_bytes(b"shared", b"db1-before");
    shard.begin_snapshot(1024).unwrap();
    shard.enter(0);
    shard.capture_snapshot_key(b"shared");
    shard.set_bytes(b"shared", b"db0-after");
    shard.capture_snapshot_key(b"created");
    shard.set_bytes(b"created", b"new");

    let snapshot = shard.finish_snapshot();
    assert_eq!(
        snapshot
            .into_iter()
            .map(|entry| (entry.database, entry.key, entry.value))
            .collect::<Vec<_>>(),
        vec![
            (
                0,
                b"shared".to_vec(),
                SnapshotValue::Bytes(b"db0-before".to_vec())
            ),
            (
                0,
                b"uncaptured".to_vec(),
                SnapshotValue::Bytes(b"db0".to_vec())
            ),
            (
                1,
                b"shared".to_vec(),
                SnapshotValue::Bytes(b"db1-before".to_vec())
            ),
        ]
    );
}

#[test]
fn snapshot_journal_beyond_the_configured_bound_aborts_the_snapshot() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"key", &[7; 64]);
    shard.begin_snapshot(16).unwrap();
    shard.capture_snapshot_key(b"key");
    assert!(!shard.journal_is_open());
    assert_eq!(shard.snapshot_aborts(), 1);
    assert_eq!(shard.get(b"key"), Some(&[7; 64][..]));
}

#[test]
fn a_capture_past_the_journal_bound_aborts_before_copying_the_value() {
    let mut shard = KvShard::new();
    let members: Vec<Vec<u8>> = (0..4_096u32)
        .map(|index| format!("member-{index:08}").into_bytes())
        .collect();

    shard.set_add(b"big", &members).unwrap();
    shard.begin_snapshot(1024).unwrap();
    super::snapshot::LIVE_SNAPSHOT_COPIES.with(|copies| copies.set(0));
    shard.capture_snapshot_key(b"big");

    assert!(!shard.journal_is_open());
    assert_eq!(shard.snapshot_aborts(), 1);
    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_COPIES.with(std::cell::Cell::get),
        0,
        "the value was copied only to be thrown away"
    );
}

#[test]
fn clock_jumps_and_restart_cannot_resurrect_an_expired_snapshot_entry() {
    let wall = crate::time::TestWallClock::new(1_000, 1_000);
    let mut shard = KvShard::with_clock(wall.clone());
    shard.set_bytes(b"soon", b"value");
    assert!(shard.expire_at(b"soon", UnixMillis::new(1_500)));
    let snapshot = shard.snapshot_entries();
    assert_eq!(shard.pttl(b"soon"), 500);

    wall.set(1_500);
    assert_eq!(shard.pttl(b"soon"), -2);
    wall.set(500);
    assert_eq!(shard.get(b"soon"), None);

    let mut restarted = KvShard::with_clock(crate::time::TestWallClock::new(500, 1_500));
    for entry in snapshot {
        restarted.restore_snapshot_entry(entry).unwrap();
    }
    assert_eq!(restarted.get(b"soon"), None);
    assert_eq!(restarted.pttl(b"soon"), -2);
}

#[test]
fn incremental_snapshot_matches_the_point_in_time_state_under_concurrent_writes() {
    let mut shard = KvShard::new();
    let mut expected = std::collections::BTreeMap::new();

    for index in 0..3_000u32 {
        let key = format!("key:{index}").into_bytes();
        let value = format!("v{index}").into_bytes();

        shard.set_bytes(&key, &value);
        expected.insert(key, value);
    }

    shard.begin_snapshot(1 << 24).unwrap();

    let mut written = Vec::new();
    let mut round = 0u32;

    loop {
        let step = shard.snapshot_step(100, &mut |entry| written.push(entry.to_entry()));

        // Writes between slices: overwrite, delete and create (enough to grow the table).
        for offset in 0..40u32 {
            let key = format!("key:{}", (round * 97 + offset * 31) % 3_000).into_bytes();

            shard.capture_snapshot_key(&key);

            if offset % 3 == 0 {
                shard.delete(&key);
            } else {
                shard.set_bytes(&key, b"changed");
            }

            let fresh = format!("fresh:{round}:{offset}").into_bytes();

            shard.capture_snapshot_key(&fresh);
            shard.set_bytes(&fresh, b"new");
        }

        round += 1;
        // Log reclaim may run between slices: values move, key records stay put.
        shard.reclaim_logs(std::time::Duration::from_millis(1));

        match step {
            SnapshotStep::Done => break,
            SnapshotStep::More => {}
            SnapshotStep::Waiting => {
                shard.migrate_indexes(std::time::Duration::from_millis(5));
            }
        }
    }

    written.extend(shard.finish_snapshot_journal());

    let mut actual = std::collections::BTreeMap::new();

    for entry in written {
        let SnapshotValue::Bytes(value) = entry.value else {
            panic!("unexpected value type");
        };

        assert!(
            actual.insert(entry.key, value).is_none(),
            "key written twice"
        );
    }

    assert_eq!(actual, expected);
}

#[test]
fn the_snapshot_walk_encodes_large_values_without_copying_them() {
    let mut shard = KvShard::new();
    let pairs: Vec<(f64, Vec<u8>)> = (0..5_000u32)
        .map(|index| (f64::from(index), format!("member-{index:08}").into_bytes()))
        .collect();
    let members: Vec<Vec<u8>> = pairs.iter().map(|(_, member)| member.clone()).collect();

    shard.sorted_set_add_pairs(b"zset", &pairs).unwrap();
    shard.set_add(b"set", &members).unwrap();
    shard.set_bytes(b"string", &[b's'; 100_000]);
    shard.begin_snapshot(1 << 24).unwrap();
    super::snapshot::LIVE_SNAPSHOT_COPIES.with(|copies| copies.set(0));
    super::snapshot::LIVE_SNAPSHOT_ENCODES.with(|encodes| encodes.set(0));

    let mut frames = 0;

    while !matches!(
        shard.snapshot_step(100, &mut |entry| {
            let mut frame = Vec::new();

            entry.encode_into(&mut frame).unwrap();
            // Sized once: no spare room from growing the buffer while encoding.
            assert_eq!(frame.capacity(), frame.len());
            frames += 1;
        }),
        SnapshotStep::Done
    ) {}

    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_COPIES.with(std::cell::Cell::get),
        0
    );
    assert_eq!(
        super::snapshot::LIVE_SNAPSHOT_ENCODES.with(std::cell::Cell::get),
        3
    );
    assert_eq!(frames, 3);
}

#[test]
fn a_lease_from_an_older_snapshot_gets_its_deadline_or_is_dropped() {
    // Snapshots written before leases had their own deadline store a lease without one.
    let mut shard = KvShard::with_clock(TestClock::new(2_000));
    let lease = |key: &[u8], expires_at| SnapshotEntry {
        database: 0,
        key: key.to_vec(),
        expires_at: None,
        value: SnapshotValue::Lease {
            token: 9,
            expires_at,
        },
    };

    shard.restore_snapshot_entry(lease(b"held", 5_000)).unwrap();
    shard
        .restore_snapshot_entry(lease(b"lapsed", 1_500))
        .unwrap();

    assert_eq!(shard.pttl(b"held"), 3_000);
    assert_eq!(shard.pttl(b"lapsed"), -2);
}
