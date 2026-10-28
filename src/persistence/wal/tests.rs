use std::{borrow::Cow, fs, io::Write, time::Duration};

use crate::persistence::codec::encode_record;

use tempfile::tempdir;

use super::*;

fn config(path: &Path, profile: DurabilityProfile) -> PersistenceConfig {
    PersistenceConfig {
        profile,
        data_dir: path.to_path_buf(),
        wal_segment_bytes: 4096,
        sync_interval: std::time::Duration::from_millis(1),
        ..PersistenceConfig::default()
    }
}

/// Many appends with one sync at the end. `Always` would fsync every record.
fn bulk_config(path: &Path) -> PersistenceConfig {
    PersistenceConfig {
        sync_interval: std::time::Duration::from_secs(3600),
        ..config(path, DurabilityProfile::Everysec)
    }
}

#[test]
fn sync_totals_count_inline_syncs() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let before = sink.sync_totals().0;

    for index in 0..3u8 {
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: &[index; 16],
        })
        .unwrap();
        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
    }

    let (count, nanos) = sink.sync_totals();

    assert_eq!(count - before, 3);
    assert!(nanos > 0);
}

/// A checkpoint whose snapshot point is past `always` records not synced yet (a batch
/// between its group's end and its sync) syncs them first, and is refused when that
/// sync fails: the batch's clients are refused and its records cut.
#[test]
fn a_checkpoint_past_unsynced_always_records_syncs_them_or_is_refused() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
    let write = |sink: &mut WalSink, key: &[u8]| {
        sink.begin_group();
        sink.record(Mutation::SetBytes { key, value: b"v" })
            .unwrap();
        sink.end_group().unwrap();
    };

    write(&mut sink, b"synced");
    sink.sync_for_checkpoint(sink.last_sequence()).unwrap();
    assert_eq!(sink.durable.load(Ordering::Acquire), sink.last_sequence());

    write(&mut sink, b"refused");
    assert!(sink.checkpoint_may_cover(sink.last_sequence()).is_ok());
    FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .push(directory.path().to_path_buf());
    let checkpoint = sink.sync_for_checkpoint(sink.last_sequence());
    let batch = sink.sync_batch(1);
    FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());

    assert!(checkpoint.is_err());
    assert!(batch.is_err());
    assert!(sink.checkpoint_may_cover(sink.last_sequence()).is_err());
    drop(sink);
    let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    assert_eq!(mutations.len(), 1);
}

/// `SAVE` and rotation flush the log on the shard thread, possibly between a batch's
/// group end and its sync. When that flush's sync fails, the batch's clients are refused
/// by their own sync, so its records are cut and a restart does not revive them.
#[test]
fn a_failed_flush_cuts_always_records_whose_batch_is_refused() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();

    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"acknowledged",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    sink.sync_pending().unwrap();
    let start = sink.last_sequence();

    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"refused",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .push(directory.path().to_path_buf());
    let flushed = sink.flush();
    let batch = sink.sync_batch(start);
    FAILING_SYNC_DIRS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());

    assert!(flushed.is_err());
    assert!(batch.is_err());
    drop(sink);
    let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    assert_eq!(mutations.len(), 1);
}

/// A sync on another descriptor of the segment (the preallocation fill) failed: the
/// failure is picked up before this batch's own sync runs, and the batch's clients are
/// refused, so its records are cut as when its own sync fails.
#[test]
fn a_failed_fill_sync_cuts_always_records_whose_batch_is_refused() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
    let write = |sink: &mut WalSink, key: &[u8]| {
        let start = sink.last_sequence();

        sink.begin_group();
        sink.record(Mutation::SetBytes { key, value: b"v" })
            .unwrap();
        sink.end_group().unwrap();
        start
    };

    let start = write(&mut sink, b"acknowledged");
    sink.sync_batch(start).unwrap();
    let start = write(&mut sink, b"refused");
    sink.sync_error.report("injected fill sync failure".into());

    assert!(sink.sync_batch(start).is_err());
    drop(sink);
    let (_sink, mutations, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    assert_eq!(mutations.len(), 1);
}

#[test]
fn steady_writes_through_the_sync_worker_keep_unsynced_bytes_bounded() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    // One segment for the whole run: a rotation would reset the count.
    config.wal_segment_bytes = 64 << 20;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Each barrier is prepared after the next record is written, so the check never
    // finds the whole log durable.
    for index in 0..200u8 {
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: &[index; 1000],
        })
        .unwrap();
        sink.end_group().unwrap();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        block_on_bounded(&runtime, barrier.wait()).unwrap();
    }

    assert!(
        sink.dirty_bytes() < 3 * 1100,
        "unsynced bytes grew to {}",
        sink.dirty_bytes()
    );
}

#[test]
fn sync_worker_barriers_survive_rotation_and_recovery() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for index in 0..30 {
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: &[index; 512],
        })
        .unwrap();
        sink.end_group().unwrap();
        let sequence = sink.last_sequence();
        let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
        block_on_bounded(&runtime, barrier.wait()).unwrap();
        assert!(sink.durable.load(Ordering::Acquire) >= sequence);
    }
    sink.flush().unwrap();
    drop(sink);
    let (_, records, report) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    assert_eq!(records.len(), 30);
    assert_eq!(report.last_sequence, 30);
}

#[test]
fn shutdown_flush_waits_for_a_stalled_sync_worker() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let release = sink.sync_worker.as_ref().unwrap().pause_handle();
    release(true);
    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    let _barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    let wait_started = std::time::Instant::now();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(150));
            release(false);
        });
        sink.flush().unwrap();
    });
    assert!(wait_started.elapsed() >= Duration::from_millis(150));
    assert_eq!(sink.durable.load(Ordering::Acquire), sink.last_sequence());
}

#[test]
fn observed_read_barrier_waits_for_another_clients_unsynced_write() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    sink.sync_worker.as_ref().unwrap().pause(true);
    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    let writer = sink.prepare_sync_barrier().unwrap().unwrap();
    // A GET emits no new record, but must wait for the same observed sequence.
    let reader = sink.prepare_sync_barrier().unwrap().unwrap();
    assert_eq!(sink.durable.load(Ordering::Acquire), 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    block_on_bounded(&runtime, async {
        let release = async {
            tokio::task::yield_now().await;
            sink.sync_worker.as_ref().unwrap().pause(false);
        };
        let (a, b, _) = tokio::join!(writer.wait(), reader.wait(), release);
        a.unwrap();
        b.unwrap();
    });
    assert_eq!(sink.durable.load(Ordering::Acquire), 1);
}

#[test]
fn worker_failure_is_visible_before_next_append_and_rejects_mutations() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    sink.sync_worker.as_ref().unwrap().fail_sync_for_test();
    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(block_on_bounded(&runtime, barrier.wait()).is_err());
    assert!(sink.failure().is_some());
    assert_eq!(sink.durable.load(Ordering::Acquire), 0);
    let sequence = sink.last_sequence();
    assert!(
        sink.record(Mutation::SetBytes {
            key: b"next",
            value: b"v"
        })
        .is_err()
    );
    assert_eq!(sink.last_sequence(), sequence);
    assert!(sink.flush().is_err());
}

#[test]
fn group_bytes_reset_per_group_while_worker_owns_dirty_bytes() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    config.wal_segment_bytes = 1 << 20;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let value = vec![b'v'; 40 * 1024];
    for _ in 0..2 {
        sink.begin_group();
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: &value,
        })
        .unwrap();
        assert!(sink.group_bytes() >= 40 * 1024);
        assert!(sink.group_bytes() < 64 * 1024);
        sink.end_group().unwrap();
        assert_eq!(sink.group_bytes(), 0);
        drop(sink.prepare_sync_barrier().unwrap().unwrap());
    }
    // The worker may already have synced the first group before the second barrier (then only
    // the second group is still dirty), never the second: its barrier was prepared just now.
    assert!(sink.dirty_bytes() >= 40 * 1024);
}

#[test]
fn base_replacement_resets_worker_submission_cursor() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    sink.begin_group();
    for _ in 0..3 {
        sink.record(Mutation::SetBytes {
            key: b"k",
            value: b"v",
        })
        .unwrap();
    }
    sink.end_group().unwrap();
    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    block_on_bounded(&runtime, barrier.wait()).unwrap();
    sink.install_base(0).unwrap();
    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"new",
        value: b"v",
    })
    .unwrap();
    sink.end_group().unwrap();
    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    block_on_bounded(&runtime, barrier.wait()).unwrap();
    assert_eq!(sink.durable.load(Ordering::Acquire), 1);
}

#[test]
fn idempotency_torn_completion_recovers_pending_and_success_recovers_result() {
    use crate::engine::{Response, execute_in_database};
    use crate::protocol::Command;
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let clock = crate::time::TestClock::new(4_000_000_000_000);
    let mut shard = crate::KvShard::with_clock(clock);
    let begin = Command::IdempotencyBegin {
        key: Cow::Borrowed(b"request"),
        fingerprint: Cow::Borrowed(b"fp"),
        owner: Cow::Borrowed(b"owner"),
        ttl_ms: 60_000,
    };
    let complete = Command::IdempotencyComplete {
        key: Cow::Borrowed(b"request"),
        fingerprint: Cow::Borrowed(b"fp"),
        owner: Cow::Borrowed(b"owner"),
        result: Cow::Borrowed(b"\0\xffsaved"),
    };
    execute_in_database(&mut shard, begin.clone(), &mut sink, &mut (), 2).unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(execute_in_database(&mut shard, complete.clone(), &mut sink, &mut (), 2).is_err());
    drop(sink);
    let (mut sink, records, report) = WalSink::open(config.clone(), 0, limits).unwrap();
    assert_eq!(records.len(), 1);
    assert!(report.torn_tail_bytes > 0);
    let mut restored = crate::KvShard::new();
    for (database, mutation) in records {
        crate::persistence::apply_mutation(&mut restored, database, &mutation).unwrap();
    }
    assert_eq!(
        execute_in_database(&mut restored, begin.clone(), &mut sink, &mut (), 2).unwrap(),
        Response::Array(vec![Some(Cow::Borrowed(b"pending"))])
    );
    assert_eq!(
        restored.expires_at(b"request"),
        Some(crate::UnixMillis::new(4_000_000_060_000))
    );
    assert_eq!(
        execute_in_database(&mut restored, complete, &mut sink, &mut (), 2).unwrap(),
        Response::Integer(1)
    );
    drop(sink);
    let (_sink, records, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(records.len(), 2);
    let mut restored = crate::KvShard::new();
    for (database, mutation) in records {
        assert_eq!(database, 2);
        crate::persistence::apply_mutation(&mut restored, database, &mutation).unwrap();
    }
    assert_eq!(
        execute_in_database(
            &mut restored,
            begin,
            &mut crate::mutation::NoopMutationSink,
            &mut (),
            2
        )
        .unwrap(),
        Response::Array(vec![
            Some(Cow::Borrowed(b"completed")),
            Some(Cow::Borrowed(b"\0\xffsaved"))
        ])
    );
    assert_eq!(
        restored.expires_at(b"request"),
        Some(crate::UnixMillis::new(4_000_000_060_000))
    );
}

fn config_at(config: &PersistenceConfig, data_dir: &Path) -> PersistenceConfig {
    PersistenceConfig {
        data_dir: data_dir.to_path_buf(),
        ..config.clone()
    }
}

fn keys_of(mutations: &RecoveredMutations) -> Vec<Vec<u8>> {
    mutations
        .iter()
        .map(|(_, mutation)| match mutation {
            OwnedMutation::SetBytes { key, .. } => key.clone(),
            other => panic!("unexpected mutation {other:?}"),
        })
        .collect()
}

#[test]
fn full_disk_during_grouped_write_keeps_wal_sequences_contiguous() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"kept",
        value: b"1",
    })
    .unwrap();
    sink.begin_group();
    for key in [&b"grouped-a"[..], b"grouped-b"] {
        sink.record(Mutation::SetBytes { key, value: b"2" })
            .unwrap();
    }
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    assert!(sink.failure().is_some());

    // The disk accepts writes again: the batch that failed to reach it is written first.
    sink.record(Mutation::SetBytes {
        key: b"later",
        value: b"3",
    })
    .unwrap();
    assert!(sink.failure().is_none());
    sink.flush().unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(report.last_sequence, 4);
    assert_eq!(
        keys_of(&mutations),
        [&b"kept"[..], b"grouped-a", b"grouped-b", b"later"]
    );
}

#[test]
fn full_disk_during_first_grouped_write_reuses_the_empty_segment() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"grouped",
        value: b"1",
    })
    .unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    // While failed, a later group that wrote nothing ends cleanly and must not append the
    // kept batch after the torn bytes.
    sink.begin_group();
    sink.end_group().unwrap();
    assert!(sink.failure().is_some());
    sink.record(Mutation::SetBytes {
        key: b"later",
        value: b"2",
    })
    .unwrap();
    sink.flush().unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(report.torn_tail_bytes, 0);
    assert_eq!(keys_of(&mutations), [&b"grouped"[..], b"later"]);
}

#[test]
fn replica_frame_append_recovers_after_a_full_disk() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let frame = |sequence, key: &[u8]| {
        encode_record(
            0,
            sequence,
            0,
            &OwnedMutation::SetBytes {
                key: key.to_vec(),
                value: b"v".to_vec(),
            },
        )
        .unwrap()
    };

    sink.append_frame(&frame(1, b"one")).unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.append_frame(&frame(2, b"two")).is_err());
    sink.append_frame(&frame(2, b"two")).unwrap();
    assert!(sink.failure().is_none());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
}

#[test]
fn failed_segment_creation_leaves_no_file_that_blocks_a_retry() {
    let directory = tempdir().unwrap();
    let shard_dir = shard_wal_directory(directory.path(), 0);

    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Rotate);
    assert!(create_segment(&shard_dir, 0, 7).is_err());
    let (_, _, first, length) = create_segment(&shard_dir, 0, 7).unwrap();

    assert_eq!((first, length), (7, WAL_HEADER_LEN as u64));

    // A header-only leftover the cleanup could not remove is reused, not an EEXIST forever.
    let path = segment_path(&shard_dir, 9);

    fs::write(&path, &encode_segment_header(0, 9, 9)[..10]).unwrap();
    let (_, _, first, _) = create_segment(&shard_dir, 0, 9).unwrap();

    assert_eq!(first, 9);
    assert_eq!(fs::metadata(&path).unwrap().len(), WAL_HEADER_LEN as u64);
}

#[test]
fn rotation_failure_enters_the_failed_state_and_recovers() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let value = vec![b'x'; 2_000];

    sink.record(Mutation::SetBytes {
        key: b"first",
        value: &value,
    })
    .unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Rotate);
    assert!(
        sink.record(Mutation::SetBytes {
            key: b"second",
            value: &value,
        })
        .is_err()
    );
    assert!(sink.failure().is_some());
    sink.record(Mutation::SetBytes {
        key: b"second",
        value: &value,
    })
    .unwrap();
    assert!(sink.failure().is_none());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"first"[..], b"second"]);
}

#[test]
fn everysec_flush_reports_a_background_sync_that_fails_late() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

    // Past the 1 ms interval, so this append starts a background sync.
    std::thread::sleep(std::time::Duration::from_millis(5));
    // The background sync started by this append fails a little later.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();

    assert!(sink.flush().is_err());
    assert!(sink.dirty_bytes() > 0);
    assert_eq!(sink.durable.load(Ordering::Acquire), 0);
}

#[test]
fn open_after_a_checkpoint_past_the_log_end_starts_a_new_segment() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for key in [&b"one"[..], b"two"] {
        sink.record(Mutation::SetBytes { key, value: b"v" })
            .unwrap();
    }
    drop(sink);

    // The checkpoint covers sequence 5; records 3..=5 never reached this log.
    let (mut sink, mutations, _) = WalSink::open_after(config.clone(), 0, limits, 5).unwrap();

    assert!(mutations.is_empty());
    assert_eq!(sink.last_sequence(), 5);
    sink.record(Mutation::SetBytes {
        key: b"six",
        value: b"v",
    })
    .unwrap();
    drop(sink);

    let (sink, mutations, _) = WalSink::open_after(config, 0, limits, 5).unwrap();

    assert_eq!(keys_of(&mutations), [&b"six"[..]]);
    assert_eq!(sink.last_sequence(), 6);
}

#[test]
fn always_sync_failure_does_not_revive_the_refused_record() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"one",
        value: b"v",
    })
    .unwrap();
    // The record reaches the file but its sync fails: the write is refused (not applied).
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(
        sink.record(Mutation::SetBytes {
            key: b"refused",
            value: b"v",
        })
        .is_err()
    );
    // A restart before any recovery must not replay it either.
    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let copy = directory.path().join("restart-copy");

    fs::create_dir_all(shard_wal_directory(&copy, 0)).unwrap();
    fs::copy(&path, segment_path(&shard_wal_directory(&copy, 0), 1)).unwrap();
    let (_copy_sink, mutations, _) = WalSink::open(config_at(&config, &copy), 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..]]);

    // A failed sync is not retried in place: writes stay refused until a restart.
    sink.last_recovery = None;
    assert!(
        sink.record(Mutation::SetBytes {
            key: b"three",
            value: b"v",
        })
        .is_err()
    );
    drop(sink);

    let (mut sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    sink.record(Mutation::SetBytes {
        key: b"three",
        value: b"v",
    })
    .unwrap();
}

/// Runs `future` on `runtime`, failing the test after 10 s instead of hanging.
fn block_on_bounded<F: std::future::Future>(
    runtime: &tokio::runtime::Runtime,
    future: F,
) -> F::Output {
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(10), future)
            .await
            .expect("test wait exceeded 10 s")
    })
}

/// `WalSink::wait_background_sync` with a 10 s deadline, for tests.
fn wait_background_sync_bounded(sink: &WalSink) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    while sink.sync_running.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "background sync still running after 10 s"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

fn set(sink: &mut WalSink, key: &[u8], value: &[u8]) -> Result<(), PersistenceError> {
    sink.record(Mutation::SetBytes { key, value })
}

fn segment_sizes(directory: &Path) -> Vec<u64> {
    discover_segments(&shard_wal_directory(directory, 0))
        .unwrap()
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .collect()
}

#[test]
fn background_sync_failure_keeps_writes_refused_until_restart() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    // The sync this append started must be done, or the next append would not start one.
    wait_background_sync_bounded(&sink);
    std::thread::sleep(std::time::Duration::from_millis(5));
    // The background sync this append starts fails: the pages it covered may be gone
    // while a later sync reports success.
    let hold = super::super::crash::arm_held_sync_fault();
    set(&mut sink, b"two", b"v").unwrap();
    assert!(sink.sync_running.load(Ordering::Acquire));
    hold.release();
    assert!(sink.flush().is_err());
    sink.shrink_to_written();
    let sizes = segment_sizes(directory.path());

    for _ in 0..2 {
        sink.last_recovery = None;
        assert!(set(&mut sink, b"three", b"v").is_err());
        assert!(sink.flush_for_shutdown().is_err());
        assert!(
            sink.prepare_sync_barrier()
                .is_ok_and(|barrier| barrier.is_none())
        );
    }
    assert!(sink.failure().is_some());
    // Nothing written before the failure was cut, moved or rewritten.
    assert_eq!(segment_sizes(directory.path()), sizes);
    assert!(sink.durable.load(Ordering::Acquire) < 2);
    drop(sink);

    // Startup decides from what is on disk.
    let (mut sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
    set(&mut sink, b"three", b"v").unwrap();
}

#[test]
fn write_failure_while_a_background_sync_fails_is_not_recovered() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    // The sync this append started must be done, or the next append would not start one.
    wait_background_sync_bounded(&sink);
    std::thread::sleep(std::time::Duration::from_millis(5));
    // This append starts a background sync that fails once the test releases it.
    let hold = super::super::crash::arm_held_sync_fault();
    set(&mut sink, b"two", b"v").unwrap();
    assert!(sink.sync_running.load(Ordering::Acquire));
    // Meanwhile a write fails (a full disk): an ordinary, recoverable failure.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(set(&mut sink, b"three", b"v").is_err());
    assert!(!sink.sync_failed);
    // The background sync fails while recovery runs; recovery's own sync would report clean
    // pages the failed sync may have lost.
    hold.release();
    sink.last_recovery = None;
    assert!(set(&mut sink, b"four", b"v").is_err());
    assert!(sink.sync_failed);
    sink.last_recovery = None;
    assert!(set(&mut sink, b"four", b"v").is_err());
    assert!(sink.durable.load(Ordering::Acquire) < 2);
}

#[test]
fn failed_prefill_sync_is_a_failed_sync_of_the_segment() {
    let directory = tempdir().unwrap();
    let config = PersistenceConfig {
        wal_segment_bytes: 1 << 20,
        ..config(directory.path(), DurabilityProfile::Always)
    };
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    let probe = File::create(directory.path().join("probe")).unwrap();
    if !sink.prealloc || !preallocate(&probe, 0, 4096) {
        // `fallocate` is unsupported here: no fill, no sync to fail.
        return;
    }
    // The first append preallocates the segment; that fill's sync fails.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::PrefillSync);
    assert!(set(&mut sink, b"one", b"v").is_err());
    assert!(sink.sync_failed);
    sink.last_recovery = None;
    assert!(set(&mut sink, b"two", b"v").is_err());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert!(mutations.is_empty());
}

/// Segments written with a larger `wal_segment_bytes` still recover after the setting is
/// lowered (issue #32: they were reported as corrupt, which also broke `ruvio-ctl` on a data
/// directory written with segments above its 256 MiB default), and new records go on.
#[test]
fn segments_longer_than_a_lowered_setting_still_recover() {
    let directory = tempdir().unwrap();
    let large = PersistenceConfig {
        wal_segment_bytes: 1 << 20,
        ..config(directory.path(), DurabilityProfile::Always)
    };
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(large, 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    set(&mut sink, b"two", b"v").unwrap();
    // Fill past the smaller size so the segment is longer even without preallocation.
    for index in 0..64u32 {
        set(&mut sink, format!("pad{index}").as_bytes(), &[7; 96]).unwrap();
    }
    drop(sink);

    let small = config(directory.path(), DurabilityProfile::Always);
    let shard_wal = shard_wal_directory(directory.path(), 0);
    let longest = fs::read_dir(&shard_wal)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .max()
        .unwrap();

    assert!(longest > small.wal_segment_bytes, "{longest}");
    let (mut sink, mutations, _) = WalSink::open(small.clone(), 0, limits).unwrap();

    assert_eq!(mutations.len(), 66);
    set(&mut sink, b"three", b"v").unwrap();
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(small, 0, limits).unwrap();
    assert_eq!(mutations.len(), 67);
}

#[test]
fn always_group_refused_by_a_failed_sync_is_not_replayed() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    sink.begin_group();
    set(&mut sink, b"ga", b"v").unwrap();
    set(&mut sink, b"gb", b"v").unwrap();
    sink.end_group().unwrap();
    // The batch's sync fails: its clients get an error.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(sink.sync_pending().is_err());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
}

#[test]
fn probe_second_batch_acked_after_cut() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let published = sink.published();

    set(&mut sink, b"one", b"v").unwrap();
    // Two batches end before either syncs; the first sync covers both and fails.
    for key in [&b"a"[..], b"b"] {
        sink.begin_group();
        set(&mut sink, key, b"v").unwrap();
        sink.end_group().unwrap();
    }
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(sink.sync_pending().is_err());
    // The second batch's records were cut too: its clients must not get OK.
    assert!(sink.sync_pending().is_err());
    assert_eq!(published.load(Ordering::Acquire), 1);
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
}

#[test]
fn probe_group_synced_after_inline_failure() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let published = sink.published();

    set(&mut sink, b"one", b"v").unwrap();
    sink.begin_group();
    set(&mut sink, b"batch", b"v").unwrap();
    sink.end_group().unwrap();
    // Another client's single write fails its sync before the batch syncs. The kernel
    // reported the error once; a later sync of the same file comes back clean.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(set(&mut sink, b"single", b"v").is_err());
    assert!(sink.sync_pending().is_err());
    assert_eq!(published.load(Ordering::Acquire), 1);
    assert!(sink.durable.load(Ordering::Acquire) < 2);
    drop(sink);

    // Neither the refused write nor the batch whose clients got an error comes back.
    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
}

#[test]
fn records_rewritten_by_recovery_are_published_after_their_sync() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();

    set(&mut sink, b"one", b"v").unwrap();
    sink.begin_group();
    set(&mut sink, b"a", b"v").unwrap();
    // The batch's write fails (a full disk): its record stays staged for recovery.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    sink.last_recovery = None;
    sink.begin_group();
    set(&mut sink, b"b", b"v").unwrap();
    // Recovery wrote the staged record again, but no sync covers it yet.
    assert_eq!(published.load(Ordering::Acquire), 1);
    sink.end_group().unwrap();
    sink.sync_pending().unwrap();
    assert_eq!(published.load(Ordering::Acquire), 3);
}

#[test]
fn no_background_sync_vouches_for_records_after_a_failed_sync() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

    sink.begin_group();
    set(&mut sink, b"a", b"v").unwrap();
    // A sync of the batch's bytes fails; the kernel may drop their pages.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(sink.flush().is_err());
    assert!(sink.sync_failed);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let _ = sink.end_group();
    wait_background_sync_bounded(&sink);
    // A later background sync would come back clean for those pages.
    assert_eq!(sink.durable.load(Ordering::Acquire), 0);
}

#[test]
fn abandoned_always_group_is_synced_and_published() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();

    sink.begin_group();
    let start = sink.last_sequence();
    set(&mut sink, b"a", b"v").unwrap();
    // The client went away before the commit: no one else may sync this shard soon.
    sink.abandon_group(start).unwrap();
    assert_eq!(published.load(Ordering::Acquire), 1);
    assert_eq!(sink.durable.load(Ordering::Acquire), 1);
    assert_eq!(sink.dirty_bytes(), 0);
}

#[test]
fn always_group_reaches_replicas_only_once_durable() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();

    set(&mut sink, b"one", b"v").unwrap();
    assert_eq!(published.load(Ordering::Acquire), 1);
    sink.begin_group();
    set(&mut sink, b"ga", b"v").unwrap();
    sink.end_group().unwrap();
    // Not synced yet: a shipped record could be cut below, and a restarted primary
    // would reuse its sequence for another write.
    assert_eq!(published.load(Ordering::Acquire), 1);
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(sink.sync_pending().is_err());
    assert_eq!(published.load(Ordering::Acquire), 1);
}

#[test]
fn always_group_is_published_after_its_sync() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let published = sink.published();

    sink.begin_group();
    set(&mut sink, b"ga", b"v").unwrap();
    set(&mut sink, b"gb", b"v").unwrap();
    sink.end_group().unwrap();
    sink.sync_pending().unwrap();
    assert_eq!(published.load(Ordering::Acquire), 2);
    // A group that wrote nothing has nothing to wait for.
    sink.begin_group();
    sink.end_group().unwrap();
    sink.sync_pending().unwrap();
    assert_eq!(published.load(Ordering::Acquire), 2);
}

#[test]
fn sync_worker_failure_is_not_recovered_in_place() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);
    config.sync_worker_active = true;
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    sink.begin_group();
    set(&mut sink, b"one", b"v").unwrap();
    sink.end_group().unwrap();
    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    block_on_bounded(&runtime, barrier.wait()).unwrap();
    sink.sync_worker.as_ref().unwrap().fail_sync_for_test();
    sink.begin_group();
    set(&mut sink, b"two", b"v").unwrap();
    sink.end_group().unwrap();
    let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
    assert!(block_on_bounded(&runtime, barrier.wait()).is_err());
    sink.last_recovery = None;
    assert!(set(&mut sink, b"three", b"v").is_err());
    assert!(sink.sync_failed);
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..], b"two"]);
}

#[test]
fn recovery_sync_failure_is_not_retried_in_place() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    sink.begin_group();
    set(&mut sink, b"two", b"v").unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    // Recovery syncs "one" before continuing; that sync fails.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Sync);
    assert!(set(&mut sink, b"three", b"v").is_err());
    assert!(sink.sync_failed);
    // A retry would sync again and could see a clean result over lost pages.
    sink.last_recovery = None;
    assert!(set(&mut sink, b"three", b"v").is_err());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
}

#[test]
fn always_group_write_failure_and_failed_recovery_write_keep_synced_records() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"k1", b"v").unwrap();
    set(&mut sink, b"k2", b"v").unwrap();
    sink.begin_group();
    set(&mut sink, b"ga", b"v").unwrap();
    set(&mut sink, b"gb", b"v").unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    // The next record triggers recovery, whose own write of the kept batch fails too.
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(set(&mut sink, b"k3", b"v").is_err());
    assert!(!sink.sync_failed);

    // A restart now still has the synced records.
    let copy = directory.path().join("restart-copy");
    let source = shard_wal_directory(directory.path(), 0);
    fs::create_dir_all(shard_wal_directory(&copy, 0)).unwrap();
    for path in discover_segments(&source).unwrap() {
        fs::copy(
            &path,
            shard_wal_directory(&copy, 0).join(path.file_name().unwrap()),
        )
        .unwrap();
    }
    let (_copy_sink, mutations, _) = WalSink::open(config_at(&config, &copy), 0, limits).unwrap();
    // The torn half of the batch may hold whole records; those replay too.
    assert_eq!(keys_of(&mutations)[..2], [&b"k1"[..], b"k2"]);

    // Once the disk accepts writes, the batch is written before the next record.
    sink.last_recovery = None;
    set(&mut sink, b"k3", b"v").unwrap();
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(
        keys_of(&mutations),
        [&b"k1"[..], b"k2", b"ga", b"gb", b"k3"]
    );
}

/// Two connections close their groups in one scheduler turn: the first to resume writes
/// both records with one `write`, and the second finds its record already written.
#[test]
fn everysec_tick_hold_writes_the_turns_records_once() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let mut tickets = Vec::new();

    for key in [&b"a"[..], b"b"] {
        sink.begin_group();
        set(&mut sink, key, b"v").unwrap();
        assert!(sink.reply_waits_for_publish());
        sink.hold_for_tick();
        sink.end_group().unwrap();
        tickets.push(sink.last_sequence());
    }
    // An expiry's record, written outside any group, waits for the turn too.
    set(&mut sink, b"c", b"v").unwrap();
    assert_eq!(sink.write_pos, WAL_HEADER_LEN as u64);

    sink.publish_through(tickets[0]).unwrap();
    let written = sink.write_pos;

    assert!(written > WAL_HEADER_LEN as u64);
    assert!(sink.staged.is_empty());
    sink.publish_through(tickets[1]).unwrap();
    assert_eq!(sink.write_pos, written);
    assert_eq!(sink.group, 0);
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"a"[..], b"b", b"c"]);
}

/// The turn's shared `write` fails: every connection whose record it carried gets the
/// error (no reply), and the records stay staged for recovery, as with each
/// connection's own failed `write` before.
#[test]
fn everysec_tick_hold_failure_cuts_every_waiter() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let mut tickets = Vec::new();

    for key in [&b"a"[..], b"b"] {
        sink.begin_group();
        set(&mut sink, key, b"v").unwrap();
        sink.hold_for_tick();
        sink.end_group().unwrap();
        tickets.push(sink.last_sequence());
    }
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.publish_through(tickets[0]).is_err());
    assert!(sink.publish_through(tickets[1]).is_err());
    // A later reply, after the failure, does not wait and is not refused.
    assert!(!sink.reply_waits_for_publish());
    assert_eq!(sink.group, 0);

    sink.last_recovery = None;
    set(&mut sink, b"c", b"v").unwrap();
    assert!(sink.failure().is_none());
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"a"[..], b"b", b"c"]);
}

#[test]
fn staged_records_of_mixed_sizes_recover_after_a_failed_write() {
    let directory = tempdir().unwrap();
    let config = PersistenceConfig {
        wal_segment_bytes: 1 << 20,
        ..config(directory.path(), DurabilityProfile::Always)
    };
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"k1", b"v").unwrap();
    set(&mut sink, b"k2", &[b'x'; 300]).unwrap();
    sink.begin_group();
    set(&mut sink, b"ga", &[b'y'; 1_777]).unwrap();
    set(&mut sink, b"gb", b"v").unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    assert!(sink.end_group().is_err());
    for attempt in 0..3 {
        sink.last_recovery = None;
        if attempt < 2 {
            super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
            assert!(set(&mut sink, b"k3", &[b'z'; 33]).is_err());
        } else {
            set(&mut sink, b"k3", &[b'z'; 33]).unwrap();
        }
    }
    assert!(sink.failure().is_none());
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(
        keys_of(&mutations),
        [&b"k1"[..], b"k2", b"ga", b"gb", b"k3"]
    );
    assert_eq!(report.last_sequence, 5);
}

#[test]
fn write_failure_after_synced_but_unsettled_bytes_keeps_them() {
    // Background `everysec` sync finished but not yet settled, and sync-worker barriers
    // whose bytes are still counted dirty: recovery must cut nothing that was written.
    for worker in [false, true] {
        let directory = tempdir().unwrap();
        let mut config = config(
            directory.path(),
            if worker {
                DurabilityProfile::Always
            } else {
                DurabilityProfile::Everysec
            },
        );
        config.sync_worker_active = worker;
        config.wal_segment_bytes = 1 << 20;
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        for key in [&b"k1"[..], b"k2"] {
            sink.begin_group();
            set(&mut sink, key, &[b'v'; 700]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            sink.end_group().unwrap();
            if worker {
                let barrier = sink.prepare_sync_barrier().unwrap().unwrap();
                block_on_bounded(&runtime, barrier.wait()).unwrap();
            } else {
                wait_background_sync_bounded(&sink);
            }
        }
        assert!(sink.dirty_bytes() > 0, "worker {worker}");
        sink.begin_group();
        set(&mut sink, b"ga", &[b'g'; 123]).unwrap();
        super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
        assert!(sink.end_group().is_err());
        set(&mut sink, b"k3", b"v").unwrap();
        assert!(sink.failure().is_none(), "worker {worker}");
        sink.flush().unwrap();
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(
            keys_of(&mutations),
            [&b"k1"[..], b"k2", b"ga", b"k3"],
            "worker {worker}"
        );
    }
}

#[test]
fn records_written_under_a_larger_command_limit_still_replay_and_ship() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);

    config.wal_segment_bytes = 1 << 20;
    let roomy = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, roomy).unwrap();
    let value = vec![b'v'; 64 * 1024];

    sink.record(Mutation::SetBytes {
        key: b"big",
        value: &value,
    })
    .unwrap();
    drop(sink);

    // The operator lowers the limits afterwards (or a replica runs with smaller ones).
    let tight = ProtocolLimits {
        max_command_bytes: 1024,
        max_value_bytes: 512,
        max_connection_buffer_bytes: 1024,
        ..roomy
    };
    let (_sink, mutations, _) = WalSink::open(config.clone(), 0, tight).unwrap();

    assert_eq!(keys_of(&mutations), [&b"big"[..]]);
    let FrameRead::Frames(frames) = read_frames(directory.path(), 0, 0, 1, tight).unwrap() else {
        panic!("frames expected");
    };
    let replica = tempdir().unwrap();
    let (mut replica_sink, _, _) =
        WalSink::open(config_at(&config, replica.path()), 0, tight).unwrap();

    replica_sink.append_frame(&frames[0]).unwrap();
}

#[test]
fn full_disk_during_append_keeps_the_acked_prefix_and_resumes() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"kept",
        value: b"1",
    })
    .unwrap();
    super::super::crash::arm_io_fault(super::super::crash::CrashSite::Append);
    let denied = sink.record(Mutation::SetBytes {
        key: b"lost",
        value: b"2",
    });

    assert!(
        matches!(denied, Err(PersistenceError::Failed(ref message)) if message.contains("space")),
        "{denied:?}"
    );
    assert!(sink.failure().is_some());

    // The disk accepts writes again: the torn tail is dropped and logging continues.
    sink.record(Mutation::SetBytes {
        key: b"later",
        value: b"3",
    })
    .unwrap();
    assert!(sink.failure().is_none());
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 2);
    assert_eq!(report.torn_tail_bytes, 0);
    assert!(matches!(
        &mutations[0].1,
        OwnedMutation::SetBytes { key, value } if key == b"kept" && value == b"1"
    ));
    assert!(matches!(
        &mutations[1].1,
        OwnedMutation::SetBytes { key, value } if key == b"later" && value == b"3"
    ));
}

#[test]
fn readonly_wal_directory_blocks_rotation_and_keeps_acked_records() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Always);

    config.wal_segment_bytes = 4096;
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    let value = vec![b'x'; 2_000];

    sink.record(Mutation::SetBytes {
        key: b"kept",
        value: &value,
    })
    .unwrap();

    let shard_dir = shard_wal_directory(directory.path(), 0);
    let original = fs::metadata(&shard_dir).unwrap().permissions();
    let mut permissions = original.clone();

    permissions.set_readonly(true);
    fs::set_permissions(&shard_dir, permissions).unwrap();

    let probe = shard_dir.join("permission-probe");

    if fs::File::create(&probe).is_ok() {
        // A privileged user (for example root in a container) ignores directory modes.
        fs::remove_file(&probe).unwrap();
        fs::set_permissions(&shard_dir, original).unwrap();
        return;
    }

    let denied = sink.record(Mutation::SetBytes {
        key: b"lost",
        value: &value,
    });

    fs::set_permissions(&shard_dir, original).unwrap();
    assert!(
        matches!(denied, Err(PersistenceError::Io(ref error)) if error.kind() == std::io::ErrorKind::PermissionDenied),
        "{denied:?}"
    );
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 1);
    assert!(matches!(
        &mutations[0].1,
        OwnedMutation::SetBytes { key, .. } if key == b"kept"
    ));
}

#[test]
fn appends_recovers_and_rotates_canonical_mutations() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();

    assert!(mutations.is_empty());
    assert_eq!(report.last_sequence, 0);

    for index in 0..100 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index; 64],
        })
        .unwrap();
    }

    sink.flush().unwrap();

    sink.flush().unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 100);
    assert_eq!(report.last_sequence, 100);
    assert!(
        fs::read_dir(shard_wal_directory(directory.path(), 0))
            .unwrap()
            .count()
            > 1
    );
}

#[test]
fn a_segment_from_an_older_minor_is_not_appended_to() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let delete = |sink: &mut WalSink, key: &'static [u8]| {
        sink.record(Mutation::Delete {
            keys: &[Cow::Borrowed(key)],
        })
        .unwrap();
        sink.flush().unwrap();
    };
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    delete(&mut sink, b"a");
    delete(&mut sink, b"b");
    drop(sink);

    let wal_dir = shard_wal_directory(directory.path(), 0);
    let old = segment_path(&wal_dir, 1);
    let mut bytes = fs::read(&old).unwrap();
    let header = super::super::codec::encode_segment_header_at(
        1,
        FORMAT_MINOR - 1,
        0,
        1,
        1,
        u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
        0,
    );
    bytes[..WAL_HEADER_LEN].copy_from_slice(&header);
    fs::write(&old, &bytes).unwrap();

    let (mut sink, mutations, _) = WalSink::open(config.clone(), 0, limits).unwrap();
    assert_eq!(mutations.len(), 2);
    delete(&mut sink, b"c");
    drop(sink);

    let minor = |path: &Path| {
        let bytes = fs::read(path).unwrap();
        u16::from_le_bytes(bytes[10..12].try_into().unwrap())
    };
    assert_eq!(minor(&old), FORMAT_MINOR - 1);
    assert_eq!(minor(&segment_path(&wal_dir, 3)), FORMAT_MINOR);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(mutations.len(), 3);
    assert_eq!(report.last_sequence, 3);
}

#[test]
fn preallocated_tail_is_not_a_torn_record_and_a_torn_record_in_it_is_dropped() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for key in [&b"a"[..], b"b", b"c"] {
        sink.record(Mutation::Delete {
            keys: &[Cow::Borrowed(key)],
        })
        .unwrap();
    }
    sink.flush().unwrap();

    // Crash image taken while the sink is alive: on Linux the file still has its zero tail.
    let image = tempdir().unwrap();
    let live = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let copy_dir = shard_wal_directory(image.path(), 0);
    fs::create_dir_all(&copy_dir).unwrap();
    let copy = copy_dir.join(live.file_name().unwrap());
    fs::copy(&live, &copy).unwrap();
    let exact = sink.write_pos;
    drop(sink);

    let (sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();
    assert_eq!(mutations.len(), 3);
    assert_eq!(report.torn_tail_bytes, 0);
    drop(sink);
    assert_eq!(fs::metadata(&live).unwrap().len(), exact);

    let mut image_config = config.clone();
    image_config.data_dir = image.path().to_path_buf();
    let (sink, mutations, report) = WalSink::open(image_config.clone(), 0, limits).unwrap();
    assert_eq!(mutations.len(), 3);
    assert_eq!(report.torn_tail_bytes, 0);
    drop(sink);
    assert_eq!(fs::metadata(&copy).unwrap().len(), exact);

    // A record cut in the middle, with zeros after it as in a preallocated file.
    let mut bytes = fs::read(&copy).unwrap();
    let cut = bytes.len() - 3;
    bytes[cut] ^= 0xff;
    bytes.extend_from_slice(&[0; 512]);
    fs::write(&copy, &bytes).unwrap();
    let (_sink, mutations, report) = WalSink::open(image_config, 0, limits).unwrap();
    assert_eq!(mutations.len(), 2);
    assert!(report.torn_tail_bytes > 0);
}

#[test]
fn truncates_only_a_torn_final_record() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::Delete {
        keys: &[Cow::Borrowed(b"key")],
    })
    .unwrap();
    drop(sink);

    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let original = fs::metadata(&path).unwrap().len();

    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"\x20\x00")
        .unwrap();

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 1);
    assert_eq!(report.torn_tail_bytes, 2);
    assert_eq!(fs::metadata(path).unwrap().len(), original);
}

#[test]
fn discards_a_truncated_header_on_the_final_segment() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"kept",
        value: b"yes",
    })
    .unwrap();
    drop(sink);

    let abandoned = segment_path(&shard_wal_directory(directory.path(), 0), 2);

    fs::write(&abandoned, b"RUVWAL").unwrap();

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 1);
    assert_eq!(report.torn_tail_bytes, 6);
    assert!(!abandoned.exists());
}

/// A power cut between writing a new segment's header and its sync can leave the file with
/// its length but zeros for content. No record was ever acknowledged in it (`create_segment`
/// syncs the header before any record is appended), so recovery drops it like a short header.
#[test]
fn discards_a_never_synced_zero_header_on_the_final_segment() {
    for length in [WAL_HEADER_LEN, WAL_HEADER_LEN + 64] {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), DurabilityProfile::Always);
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"kept",
            value: b"yes",
        })
        .unwrap();
        drop(sink);

        let abandoned = segment_path(&shard_wal_directory(directory.path(), 0), 2);

        fs::write(&abandoned, vec![0u8; length]).unwrap();

        let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(report.torn_tail_bytes, length as u64);
        assert!(!abandoned.exists());

        // The next record takes the abandoned segment's sequence and survives a restart.
        sink.record(Mutation::SetBytes {
            key: b"next",
            value: b"yes",
        })
        .unwrap();
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

        assert_eq!(mutations.len(), 2);
    }
}

#[test]
fn a_zero_header_with_data_after_it_or_before_the_last_segment_is_corruption() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"kept",
        value: b"yes",
    })
    .unwrap();
    drop(sink);

    let shard_dir = shard_wal_directory(directory.path(), 0);
    let zeroed = segment_path(&shard_dir, 2);
    let mut bytes = vec![0u8; WAL_HEADER_LEN + 16];

    bytes[WAL_HEADER_LEN + 3] = 7;
    fs::write(&zeroed, &bytes).unwrap();

    assert!(matches!(
        WalSink::open(config.clone(), 0, limits),
        Err(PersistenceError::Corruption(_))
    ));

    // A zeroed header followed by a later segment is not a torn creation either.
    fs::write(&zeroed, vec![0u8; WAL_HEADER_LEN]).unwrap();
    fs::write(segment_path(&shard_dir, 3), encode_segment_header(0, 3, 3)).unwrap();

    assert!(matches!(
        WalSink::open(config, 0, limits),
        Err(PersistenceError::Corruption(_))
    ));
}

/// Inside an `always` group, `record_now` writes the records staged ahead of it first and
/// then its own, so the log keeps sequence order and a failed write reaches the script or
/// transaction that can still roll back. Its sync still waits for the group's commit.
/// Other profiles stage it like `record`.
#[test]
fn record_now_writes_the_staged_records_before_its_own() {
    let limits = ProtocolLimits::default();
    let batch = [OwnedMutation::SetBytes {
        key: b"scripted".to_vec(),
        value: b"2".to_vec(),
    }];

    for profile in [DurabilityProfile::Always, DurabilityProfile::Everysec] {
        let directory = tempdir().unwrap();
        let config = config(directory.path(), profile);
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let on_disk = || {
            inspect_shard_wal(directory.path(), 0, limits, config.wal_segment_bytes, 0)
                .unwrap()
                .0
        };

        sink.begin_group();
        set(&mut sink, b"staged", b"1").unwrap();
        assert!(on_disk().is_empty(), "{profile:?}");
        sink.record_now(Mutation::Transaction { mutations: &batch })
            .unwrap();

        if profile == DurabilityProfile::Always {
            assert!(sink.staged.is_empty());
            assert_eq!(
                on_disk(),
                [
                    (
                        0,
                        OwnedMutation::SetBytes {
                            key: b"staged".to_vec(),
                            value: b"1".to_vec(),
                        }
                    ),
                    // A batch of one write is stored as that write.
                    (0, batch[0].clone()),
                ]
            );
            // Written, not yet synced: the group's commit syncs it.
            assert!(sink.durable_sequence() < sink.last_sequence());
        } else {
            assert!(on_disk().is_empty());
        }

        sink.end_group().unwrap();
        sink.sync_pending().unwrap();
        assert_eq!(on_disk().len(), 2, "{profile:?}");
    }
}

#[test]
fn inspect_does_not_truncate_a_torn_final_record() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::Delete {
        keys: &[Cow::Borrowed(b"key")],
    })
    .unwrap();
    drop(sink);

    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);

    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"\x20\x00")
        .unwrap();
    let torn_length = fs::metadata(&path).unwrap().len();

    let (mutations, report) =
        inspect_shard_wal(directory.path(), 0, limits, config.wal_segment_bytes, 0).unwrap();

    assert_eq!(mutations.len(), 1);
    assert_eq!(report.torn_tail_bytes, 2);
    assert_eq!(fs::metadata(path).unwrap().len(), torn_length);
}

#[test]
fn zeroed_record_length_before_data_in_the_last_segment_is_a_torn_tail() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    let cut_at = sink.write_pos;
    set(&mut sink, b"two", b"v").unwrap();
    drop(sink);

    // The last record's length is gone and the rest of it is garbage: a torn tail.
    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let mut bytes = fs::read(&path).unwrap();
    bytes[cut_at as usize..cut_at as usize + 4].fill(0);
    bytes[cut_at as usize + 10] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    let (_, inspected) = inspect_shard_wal(directory.path(), 0, limits, 4096, 0).unwrap();
    assert_eq!(inspected.last_sequence, 1);
    assert_eq!(fs::metadata(&path).unwrap().len(), bytes.len() as u64);

    let (mut sink, mutations, report) = WalSink::open(config.clone(), 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"one"[..]]);
    assert_eq!(report.torn_tail_bytes, bytes.len() as u64 - cut_at);
    assert_eq!(fs::metadata(&path).unwrap().len(), cut_at);
    set(&mut sink, b"four", b"v").unwrap();
    drop(sink);
    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
    assert_eq!(keys_of(&mutations), [&b"one"[..], b"four"]);
}

#[test]
fn zeroed_record_length_before_intact_records_refuses_to_start() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    let cut_at = sink.write_pos;
    set(&mut sink, b"two", b"v").unwrap();
    let three_at = sink.write_pos;
    set(&mut sink, b"three", b"v").unwrap();
    drop(sink);

    // Mid-segment damage: synced records follow the zeroed length.
    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let mut bytes = fs::read(&path).unwrap();
    bytes[cut_at as usize..cut_at as usize + 4].fill(0);
    fs::write(&path, &bytes).unwrap();

    let Err(PersistenceError::Corruption(message)) = WalSink::open(config.clone(), 0, limits)
    else {
        panic!("startup must refuse a segment with intact records after the damage");
    };
    assert!(message.contains(&format!("offset {cut_at}")), "{message}");
    assert!(
        message.contains(&format!("follows at offset {three_at}")),
        "{message}"
    );
    // Nothing was cut.
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn zeroed_record_length_before_data_in_an_earlier_segment_is_corruption() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    set(&mut sink, b"one", b"v").unwrap();
    let cut_at = sink.write_pos;
    set(&mut sink, b"two", b"v").unwrap();
    sink.rotate().unwrap();
    set(&mut sink, b"three", b"v").unwrap();
    drop(sink);

    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let mut bytes = fs::read(&path).unwrap();
    bytes[cut_at as usize..cut_at as usize + 4].fill(0);
    fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        WalSink::open(config, 0, limits),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn rejects_mid_log_checksum_corruption() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for key in [b"a".as_slice(), b"b".as_slice()] {
        sink.record(Mutation::SetBytes {
            key,
            value: b"value",
        })
        .unwrap();
    }

    drop(sink);

    let path = segment_path(&shard_wal_directory(directory.path(), 0), 1);
    let mut bytes = fs::read(&path).unwrap();

    bytes[WAL_HEADER_LEN + RECORD_MIN_LEN] ^= 1;
    fs::write(path, bytes).unwrap();

    assert!(matches!(
        WalSink::open(config, 0, limits),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn replays_every_supported_value_type() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"bytes",
        value: b"value",
    })
    .unwrap();
    sink.record(Mutation::SetInteger {
        key: b"integer",
        value: 42,
    })
    .unwrap();
    sink.record(Mutation::ListPushFront {
        key: b"list",
        values: &[Cow::Borrowed(b"a"), Cow::Borrowed(b"b")],
    })
    .unwrap();
    sink.record(Mutation::SetAdd {
        key: b"set",
        members: &[Cow::Borrowed(b"a"), Cow::Borrowed(b"b")],
    })
    .unwrap();
    sink.record(Mutation::HashSet {
        key: b"hash",
        pairs: &[(Cow::Borrowed(b"field"), Cow::Borrowed(b"value"))],
    })
    .unwrap();
    sink.record(Mutation::SortedSetAdd {
        key: b"sorted",
        pairs: &[(1.0, Cow::Borrowed(b"member"))],
    })
    .unwrap();
    sink.record(Mutation::BloomReserve {
        key: b"bloom",
        error_rate: 0.01,
        capacity: 100,
        expansion: 2,
    })
    .unwrap();
    sink.record(Mutation::BloomAdd {
        key: b"bloom",
        value: b"member",
    })
    .unwrap();
    sink.record(Mutation::Transaction {
        mutations: &[
            OwnedMutation::SetBytes {
                key: b"tx-a".to_vec(),
                value: b"1".to_vec(),
            },
            OwnedMutation::SetInteger {
                key: b"tx-i".to_vec(),
                value: 2,
            },
        ],
    })
    .unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
    let mut shard = crate::KvShard::new();

    for (database, mutation) in &mutations {
        super::super::apply_mutation(&mut shard, *database, mutation).unwrap();
    }

    assert_eq!(report.records_replayed, 9);
    assert_eq!(
        shard.get_bytes(b"bytes").unwrap(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        shard.copied_value(b"integer").as_deref(),
        Some(b"42".as_slice())
    );
    assert_eq!(shard.list_range(b"list", 0, -1).unwrap(), [b"b", b"a"]);
    assert!(shard.set_contains(b"set", b"a").unwrap());
    assert_eq!(
        shard.hash_get(b"hash", b"field").unwrap(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        shard.sorted_set_range(b"sorted", 0, -1).unwrap(),
        [b"member"]
    );
    assert!(shard.bloom_contains(b"bloom", b"member").unwrap());
    assert_eq!(
        shard.copied_value(b"tx-a").as_deref(),
        Some(b"1".as_slice())
    );
    assert_eq!(
        shard.copied_value(b"tx-i").as_deref(),
        Some(b"2".as_slice())
    );
}

#[test]
fn missing_earlier_wal_segment_is_rejected() {
    let directory = tempdir().unwrap();
    let mut config = bulk_config(directory.path());

    config.wal_segment_bytes = 4096;
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for index in 0..200 {
        let value = vec![index as u8; 64];

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &value,
        })
        .unwrap();
    }

    sink.flush().unwrap();

    drop(sink);
    let wal_dir = shard_wal_directory(directory.path(), 0);
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    segments.sort();
    assert!(segments.len() >= 2);
    fs::remove_file(&segments[0]).unwrap();
    assert!(matches!(
        WalSink::open(config, 0, limits),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn open_after_replays_only_sequences_after_a_checkpoint() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for index in 0..10 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index; 8],
        })
        .unwrap();
    }

    drop(sink);

    let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, 4).unwrap();

    assert_eq!(mutations.len(), 6);
    assert_eq!(report.records_replayed, 6);
    assert_eq!(report.last_sequence, 10);
}

#[test]
fn install_base_drops_old_records_and_keeps_the_checkpoint_sequence() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"old",
    })
    .unwrap();
    sink.install_base(4).unwrap();
    assert_eq!(sink.last_sequence(), 4);
    drop(sink);

    let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, 4).unwrap();

    assert!(mutations.is_empty());
    assert_eq!(report.last_sequence, 4);
}

#[test]
fn compact_keeps_the_change_stream_window_and_nothing_more() {
    let wal_size = |config: &PersistenceConfig| -> (u64, usize) {
        let directory = shard_wal_directory(&config.data_dir, 0);
        let sizes: Vec<u64> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .collect();

        (sizes.iter().sum(), sizes.len())
    };
    let run = |retain: u64| {
        let directory = tempdir().unwrap();
        let mut config = bulk_config(directory.path());
        config.cdc_retain_bytes = retain;
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, ProtocolLimits::default()).unwrap();

        for index in 0..200 {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index as u8; 64],
            })
            .unwrap();
        }

        sink.flush().unwrap();
        let before = wal_size(&config);
        sink.compact(200).unwrap();

        (before, wal_size(&config), config.wal_segment_bytes)
    };

    let (before, off, _) = run(0);
    let (_, everything, _) = run(u64::MAX);
    let (_, windowed, segment_bytes) = run(2048);

    assert!(before.1 > 2, "test needs several segments");
    assert!(off.1 < before.1, "no window: covered segments go");
    assert!(
        everything.1 >= before.1,
        "a huge window keeps every segment"
    );
    assert!(windowed.1 > off.1, "a window holds back some segments");
    assert!(windowed.1 < before.1, "but not all of them");
    assert!(windowed.0 <= 2048 + segment_bytes);
}

#[test]
fn compact_deletes_segments_covered_by_a_checkpoint() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for index in 0..200 {
        let value = vec![index as u8; 64];

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &value,
        })
        .unwrap();
    }

    sink.flush().unwrap();

    let covered = 50;

    sink.compact(covered).unwrap();
    drop(sink);

    let wal_dir = shard_wal_directory(directory.path(), 0);
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    segments.sort();
    assert!(!segments.is_empty());
    let first = segment_first_sequence(&segments[0]).unwrap();

    assert!(first > 1, "covered prefix segments should be deleted");

    let (_sink, mutations, report) =
        WalSink::open_after(config.clone(), 0, limits, covered).unwrap();

    assert_eq!(report.last_sequence, 200);
    assert_eq!(mutations.len(), 150);
    assert!(matches!(
        WalSink::open(config, 0, limits),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn compact_keeps_segments_a_replica_has_not_received() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

    for index in 0..200 {
        let value = vec![index as u8; 64];

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &value,
        })
        .unwrap();
    }

    sink.flush().unwrap();

    let wal_dir = shard_wal_directory(directory.path(), 0);
    let before = fs::read_dir(&wal_dir).unwrap().count();

    sink.backlog_floor().store(0, Ordering::Release);
    sink.compact(sink.last_sequence()).unwrap();

    assert_eq!(fs::read_dir(&wal_dir).unwrap().count(), before);

    sink.backlog_floor().store(u64::MAX, Ordering::Release);
    sink.compact(50).unwrap();

    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    segments.sort();
    let first = segment_first_sequence(&segments[0]).unwrap();

    assert!(first > 1, "prefix is deleted once no replica holds it");
}

#[test]
fn compact_drops_a_replica_hold_that_exceeds_the_budget() {
    let directory = tempdir().unwrap();
    let mut config = bulk_config(directory.path());
    config.replica_retain_max_bytes = 1024;
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

    for index in 0..400 {
        let value = vec![index as u8; 64];

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &value,
        })
        .unwrap();
    }

    sink.flush().unwrap();

    let wal_dir = shard_wal_directory(directory.path(), 0);
    let before = fs::read_dir(&wal_dir).unwrap().count();
    let dropped = REPLICA_HOLDS_DROPPED.load(Ordering::Relaxed);

    sink.backlog_floor().store(0, Ordering::Release);
    sink.compact(sink.last_sequence()).unwrap();

    assert!(fs::read_dir(&wal_dir).unwrap().count() < before);
    assert!(REPLICA_HOLDS_DROPPED.load(Ordering::Relaxed) > dropped);
}

#[test]
fn read_frames_skips_segments_before_the_requested_sequence() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

    for index in 0..200 {
        let value = vec![index as u8; 64];

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &value,
        })
        .unwrap();
    }

    sink.flush().unwrap();

    let wal_dir = shard_wal_directory(directory.path(), 0);
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    segments.sort();
    assert!(
        segments.len() > 1,
        "the log should span more than one segment"
    );

    let second = segment_first_sequence(&segments[1]).unwrap();

    fs::write(&segments[0], b"not a wal segment").unwrap();

    let read = read_frames(
        directory.path(),
        0,
        second.saturating_sub(1),
        sink.last_sequence(),
        limits,
    )
    .unwrap();

    assert!(matches!(read, FrameRead::Frames(frames) if !frames.is_empty()));
}

#[test]
fn read_frames_chunk_advances_across_segments_without_collecting_the_whole_backlog() {
    let directory = tempdir().unwrap();
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(bulk_config(directory.path()), 0, limits).unwrap();
    for index in 0..200 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index as u8; 64],
        })
        .unwrap();
    }
    sink.flush().unwrap();

    let mut cursor = 0;
    let mut chunks = 0;
    while cursor < sink.last_sequence() {
        let FrameRead::Frames(frames) = read_frames_chunk(
            directory.path(),
            0,
            cursor,
            sink.last_sequence(),
            limits,
            256,
        )
        .unwrap() else {
            panic!("missing WAL history");
        };
        let bytes: usize = frames.iter().map(Vec::len).sum();
        assert!(bytes - frames.last().unwrap().len() < 256);
        assert!(!frames.is_empty());
        for frame in frames {
            let record = decode_record(&frame, 0, limits).unwrap();
            assert_eq!(record.sequence, cursor + 1);
            cursor = record.sequence;
        }
        chunks += 1;
    }
    assert!(chunks > 1);
    assert_eq!(cursor, 200);
}

#[test]
fn a_resumed_reader_continues_where_it_stopped_instead_of_rescanning() {
    let directory = tempdir().unwrap();
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(bulk_config(directory.path()), 0, limits).unwrap();
    let write = |sink: &mut WalSink, count: u8| {
        for index in 0..count {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 64],
            })
            .unwrap();
        }
        sink.flush().unwrap();
    };
    write(&mut sink, 20);

    let mut reader = FrameCursor::default();
    let mut last = 0;
    let follow = |reader: &mut FrameCursor, last: &mut u64, through: u64| {
        while *last < through {
            let FrameRead::Frames(frames) =
                read_frames_resuming(reader, directory.path(), 0, *last, through, limits, 256)
                    .unwrap()
            else {
                panic!("missing WAL history after {last}");
            };
            assert!(!frames.is_empty());
            for frame in frames {
                let record = decode_record(&frame, 0, limits).unwrap();
                assert_eq!(record.sequence, *last + 1);
                *last = record.sequence;
            }
        }
    };
    follow(&mut reader, &mut last, 10);

    // Damage a record the reader already returned. A rescan from the segment start
    // trips over it; a reader that continues from its offset never looks at it again.
    let segment = discover_segments(&shard_wal_directory(directory.path(), 0)).unwrap()[0].clone();
    let mut bytes = fs::read(&segment).unwrap();
    bytes[WAL_HEADER_LEN + 12] ^= 0xff;
    fs::write(&segment, &bytes).unwrap();
    assert!(matches!(
        read_frames_chunk(directory.path(), 0, last, 20, limits, 256).unwrap(),
        FrameRead::Local
    ));
    follow(&mut reader, &mut last, 20);
    assert_eq!(last, 20);

    // Records appended after the reader reached the end of the live data are picked up
    // from the same offset.
    write(&mut sink, 5);
    follow(&mut reader, &mut last, 25);
    assert_eq!(last, 25);

    // A request that does not continue the cursor scans; the damaged record is in the
    // scanned range, so the reader reports the log unusable rather than skipping it.
    assert!(matches!(
        read_frames_resuming(&mut reader, directory.path(), 0, 2, 25, limits, 256).unwrap(),
        FrameRead::Local
    ));
}

#[test]
fn a_resumed_reader_follows_rotation_and_rescans_when_its_segment_is_gone() {
    let directory = tempdir().unwrap();
    let limits = ProtocolLimits::default();
    let mut config = bulk_config(directory.path());
    config.wal_segment_bytes = 4096;
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    for index in 0..200u8 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index; 64],
        })
        .unwrap();
    }
    sink.flush().unwrap();
    let wal = shard_wal_directory(directory.path(), 0);
    assert!(discover_segments(&wal).unwrap().len() > 3);

    let mut reader = FrameCursor::default();
    let mut resumed = Vec::new();
    let mut last = 0;
    while last < 100 {
        let FrameRead::Frames(frames) =
            read_frames_resuming(&mut reader, directory.path(), 0, last, 100, limits, 300).unwrap()
        else {
            panic!("missing WAL history");
        };
        for frame in frames {
            last = decode_record(&frame, 0, limits).unwrap().sequence;
            resumed.push(frame);
        }
    }
    let FrameRead::Frames(whole) = read_frames(directory.path(), 0, 0, 100, limits).unwrap() else {
        panic!("missing WAL history");
    };
    assert_eq!(resumed, whole);

    // The segment under the cursor is compacted away: the next chunk rescans and
    // answers exactly as a reader without a cursor would.
    let (held, _) = reader.position.clone().unwrap();
    let held_first = segment_first_sequence(&held).unwrap();
    for path in discover_segments(&wal).unwrap() {
        if segment_first_sequence(&path).unwrap() <= held_first {
            fs::remove_file(path).unwrap();
        }
    }
    let resumed = read_frames_resuming(&mut reader, directory.path(), 0, last, 200, limits, 300);
    let scanned = read_frames_chunk(directory.path(), 0, last, 200, limits, 300);
    assert_eq!(format!("{resumed:?}"), format!("{scanned:?}"));
    assert!(matches!(resumed, Ok(FrameRead::Gap)));
}

// Only Unix keeps the segment open between chunks.
#[cfg(unix)]
#[test]
fn a_resumed_reader_keeps_the_live_segment_open_and_drops_a_replaced_one() {
    let directory = tempdir().unwrap();
    let limits = ProtocolLimits::default();
    let config = config(directory.path(), DurabilityProfile::Always);
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let write = |sink: &mut WalSink, count: u8| {
        for index in 0..count {
            sink.record(Mutation::SetBytes {
                key: b"key",
                value: &[index; 16],
            })
            .unwrap();
        }
        sink.flush().unwrap();
    };
    write(&mut sink, 5);

    let mut reader = FrameCursor::default();
    let FrameRead::Frames(first) =
        read_frames_resuming(&mut reader, directory.path(), 0, 0, 5, limits, usize::MAX).unwrap()
    else {
        panic!("missing WAL history");
    };
    assert_eq!(first.len(), 5);
    assert!(reader.open.is_some());

    // Records appended to the live segment after the first chunk come from the open file.
    write(&mut sink, 3);
    let FrameRead::Frames(next) =
        read_frames_resuming(&mut reader, directory.path(), 0, 5, 8, limits, usize::MAX).unwrap()
    else {
        panic!("missing WAL history");
    };
    let FrameRead::Frames(scanned) = read_frames(directory.path(), 0, 5, 8, limits).unwrap() else {
        panic!("missing WAL history");
    };
    assert_eq!(next, scanned);
    assert!(reader.open.is_some());

    // The same path now names another file: the reader rescans instead of reading on.
    let (held, _) = reader.position.clone().unwrap();
    let copy = held.with_extension("copy");
    fs::copy(&held, &copy).unwrap();
    fs::remove_file(&held).unwrap();
    fs::rename(&copy, &held).unwrap();
    write(&mut sink, 1);
    let resumed = read_frames_resuming(&mut reader, directory.path(), 0, 8, 9, limits, usize::MAX);
    let scanned = read_frames(directory.path(), 0, 8, 9, limits);
    assert_eq!(format!("{resumed:?}"), format!("{scanned:?}"));
}

#[test]
fn compact_of_the_full_log_leaves_recovery_from_the_covered_sequence() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    for index in 0..50 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index; 8],
        })
        .unwrap();
    }

    sink.flush().unwrap();

    let covered = sink.last_sequence();

    sink.compact(covered).unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open_after(config, 0, limits, covered).unwrap();

    assert!(mutations.is_empty());
    assert_eq!(report.last_sequence, covered);
    assert_eq!(report.records_replayed, 0);
}

#[test]
fn published_sequence_reads_the_exact_wal_frame() {
    let directory = tempdir().unwrap();
    let primary_config = config(directory.path(), DurabilityProfile::Everysec);
    let limits = ProtocolLimits::default();
    let shard_id = 0;
    let (mut sink, _, _) = WalSink::open(primary_config, shard_id, limits).unwrap();
    let owned = OwnedMutation::SetBytes {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };

    sink.append(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();

    assert_eq!(sink.published().load(Ordering::Acquire), 1);

    let frame = encode_record(shard_id, 1, 0, &owned).unwrap();
    let read = read_frames(directory.path(), shard_id, 0, 1, limits).unwrap();

    assert_eq!(read, FrameRead::Frames(vec![frame.clone()]));
    assert_eq!(
        read_frames(directory.path(), shard_id, 1, 1, limits).unwrap(),
        FrameRead::Frames(Vec::new())
    );
    assert_eq!(
        read_frames(directory.path(), shard_id, 2, 1, limits).unwrap(),
        FrameRead::Ahead
    );

    let segment = segment_path(&shard_wal_directory(directory.path(), shard_id), 1);

    drop(sink);
    fs::remove_file(segment).unwrap();
    assert_eq!(
        read_frames(directory.path(), shard_id, 0, 1, limits).unwrap(),
        FrameRead::Gap
    );

    let replica = tempdir().unwrap();
    let replica_config = config(replica.path(), DurabilityProfile::Everysec);
    let (mut replica_sink, _, _) = WalSink::open(replica_config, shard_id, limits).unwrap();

    replica_sink.append_frame(&frame).unwrap();
    assert_eq!(
        read_frames(replica.path(), shard_id, 0, 1, limits).unwrap(),
        FrameRead::Frames(vec![frame.clone()])
    );

    let published = replica_sink.published().load(Ordering::Acquire);
    let skipped = encode_record(shard_id, published.saturating_add(2), 0, &owned).unwrap();
    let rejected = replica_sink.append_frame(&skipped);

    assert!(rejected.is_err(), "{rejected:?}");
    assert_eq!(replica_sink.published().load(Ordering::Acquire), published);
}

#[test]
fn everysec_keeps_an_unsynced_record_off_the_durable_sequence() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Everysec);

    config.sync_interval = std::time::Duration::from_secs(3600);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();
    let owned = OwnedMutation::SetBytes {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };

    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();

    assert_eq!(sink.published().load(Ordering::Acquire), 1);
    assert_eq!(sink.durable().load(Ordering::Acquire), 0);
    assert_eq!(
        read_frames(directory.path(), 0, 0, 0, limits).unwrap(),
        FrameRead::Frames(Vec::new())
    );

    sink.flush().unwrap();
    assert_eq!(sink.durable().load(Ordering::Acquire), 1);

    let frame = encode_record(0, 1, 0, &owned).unwrap();

    assert_eq!(
        read_frames(directory.path(), 0, 0, 1, limits).unwrap(),
        FrameRead::Frames(vec![frame])
    );
}

#[test]
fn always_group_commit_syncs_every_record_in_the_batch() {
    let directory = tempdir().unwrap();
    let config = config(directory.path(), DurabilityProfile::Always);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.begin_group();

    for index in 0..8 {
        sink.record(Mutation::SetBytes {
            key: b"key",
            value: &[index],
        })
        .unwrap();
    }

    assert_eq!(sink.durable().load(Ordering::Acquire), 0);
    assert_eq!(sink.published().load(Ordering::Acquire), 0);
    sink.end_group().unwrap();
    assert_eq!(sink.published().load(Ordering::Acquire), 0);
    assert_eq!(sink.durable().load(Ordering::Acquire), 0);
    sink.sync_pending().unwrap();
    assert_eq!(sink.durable().load(Ordering::Acquire), 8);
    assert_eq!(sink.published().load(Ordering::Acquire), 8);
    drop(sink);

    let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();

    assert_eq!(mutations.len(), 8);
}

#[test]
fn everysec_group_is_readable_before_the_background_sync() {
    let directory = tempdir().unwrap();
    let mut config = config(directory.path(), DurabilityProfile::Everysec);

    config.sync_interval = std::time::Duration::from_secs(3600);
    let limits = ProtocolLimits::default();
    let (mut sink, _, _) = WalSink::open(config, 0, limits).unwrap();

    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"k",
        value: b"v",
    })
    .unwrap();
    assert_eq!(sink.published().load(Ordering::Acquire), 0);
    sink.end_group().unwrap();
    assert_eq!(sink.published().load(Ordering::Acquire), 1);
    assert_eq!(sink.durable().load(Ordering::Acquire), 0);
    assert!(matches!(
        read_frames(directory.path(), 0, 0, 1, limits).unwrap(),
        FrameRead::Frames(frames) if frames.len() == 1
    ));
}

/// Records staged inside a group are encoded onto the end of the batch buffer. A record that
/// needs a new segment, overflows the batch, or is refused must leave the batch exactly as it
/// was: recovery returns every accepted record in order and nothing of the refused one.
#[test]
fn staged_records_survive_rotation_batch_overflow_and_a_refused_record() {
    let check = |segment_bytes: u64, value_len: usize, count: u8| {
        let directory = tempdir().unwrap();
        let config = PersistenceConfig {
            wal_segment_bytes: segment_bytes,
            ..bulk_config(directory.path())
        };
        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let mut expected = Vec::new();

        sink.begin_group();
        for index in 0..count {
            let key = [b'k', index];
            let value = vec![index; value_len];

            sink.record(Mutation::SetBytes {
                key: &key,
                value: &value,
            })
            .unwrap();
            expected.push(
                Mutation::SetBytes {
                    key: &key,
                    value: &value,
                }
                .to_owned(),
            );

            if index == count / 2 {
                let refused = vec![0u8; segment_bytes as usize];
                let error = sink
                    .record(Mutation::SetBytes {
                        key: b"refused",
                        value: &refused,
                    })
                    .unwrap_err();

                assert!(error.to_string().contains("segment size"), "{error}");
            }
        }
        sink.end_group().unwrap();
        sink.flush().unwrap();
        drop(sink);

        let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
        let recovered: Vec<_> = mutations
            .into_iter()
            .map(|(_, mutation)| mutation)
            .collect();

        assert_eq!(recovered, expected);
        assert_eq!(report.last_sequence, u64::from(count));
        fs::read_dir(shard_wal_directory(directory.path(), 0))
            .unwrap()
            .count()
    };

    // Small segments: the group rotates several times while records are staged.
    assert!(check(4096, 64, 100) > 1);
    // One large segment: the batch passes `WAL_GROUP_BYTES` several times.
    assert_eq!(check(4 * 1024 * 1024, 4000, 60), 1);
}

/// A record over the command limit, refused in the middle of a staged batch, leaves the batch
/// as it was.
#[test]
fn a_record_over_the_command_limit_leaves_the_staged_batch_intact() {
    let directory = tempdir().unwrap();
    let config = bulk_config(directory.path());
    let limits = ProtocolLimits {
        max_command_bytes: 256,
        ..ProtocolLimits::default()
    };
    let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

    sink.begin_group();
    sink.record(Mutation::SetBytes {
        key: b"a",
        value: b"1",
    })
    .unwrap();
    let error = sink
        .record(Mutation::SetBytes {
            key: b"big",
            value: &[7; 512],
        })
        .unwrap_err();

    assert!(error.to_string().contains("command limit"), "{error}");
    sink.record(Mutation::SetBytes {
        key: b"b",
        value: b"2",
    })
    .unwrap();
    sink.end_group().unwrap();
    sink.flush().unwrap();
    drop(sink);

    let (_sink, mutations, report) = WalSink::open(config, 0, limits).unwrap();
    let keys: Vec<_> = mutations
        .into_iter()
        .map(|(_, mutation)| match mutation {
            OwnedMutation::SetBytes { key, .. } => key,
            other => panic!("unexpected {other:?}"),
        })
        .collect();

    assert_eq!(keys, [b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(report.last_sequence, 2);
}
