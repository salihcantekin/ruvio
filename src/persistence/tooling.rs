use std::{
    collections::BTreeSet,
    fs,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use crate::{KvShard, SystemClock, protocol::ProtocolLimits, time::UnixMillis};

use super::{
    DataDirectoryLock, DurabilityProfile, PersistenceConfig, PersistenceError, apply_mutation,
    signature::{BackupSeal, verify_backup_signature, write_backup_signature},
    snapshot::CheckpointManager,
    wal::{RecoveryReport, discover_wal_shards, inspect_shard_wal},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointSummary {
    pub generation: u64,
    pub created_at_unix_millis: u64,
    pub shard_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardIntegrity {
    pub shard_id: u32,
    pub snapshot_sequence: u64,
    pub snapshot_entries: usize,
    pub wal: RecoveryReport,
    pub recovered_keys: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntegrityReport {
    pub data_dir: PathBuf,
    pub clock_unix_millis: Option<u64>,
    pub checkpoint: Option<CheckpointSummary>,
    pub shards: Vec<ShardIntegrity>,
}

pub fn check_data_dir(data_dir: impl AsRef<Path>) -> Result<IntegrityReport, PersistenceError> {
    let data_dir = data_dir.as_ref().to_path_buf();
    let limits = ProtocolLimits::default();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: data_dir.clone(),
        ..PersistenceConfig::default()
    };

    let checkpoints = CheckpointManager::new(config, limits);
    let checkpoint = checkpoints.load()?;
    let clock = checkpoints.load_clock()?;
    let mut shard_ids = BTreeSet::new();

    if let Some(checkpoint) = &checkpoint {
        shard_ids.extend(checkpoint.shards.iter().map(|shard| shard.shard_id));
    }

    shard_ids.extend(discover_wal_shards(&data_dir)?);

    if shard_ids.is_empty() {
        shard_ids.insert(0);
    }

    let mut shards = Vec::new();

    for shard_id in shard_ids {
        let snapshot = checkpoint.as_ref().and_then(|checkpoint| {
            checkpoint
                .shards
                .iter()
                .find(|shard| shard.shard_id == shard_id)
        });

        let snapshot_sequence = snapshot.map_or(0, |shard| shard.last_sequence);
        let snapshot_entries = snapshot.map_or(0, |shard| shard.entries.len());
        let (mutations, wal) = inspect_shard_wal(
            &data_dir,
            shard_id,
            limits,
            PersistenceConfig::default().wal_segment_bytes,
            snapshot_sequence,
        )?;
        let mut store =
            KvShard::with_clock(SystemClock::with_floor(clock.unwrap_or(UnixMillis::new(0))));
        let replay_clock = store.pause_expiration_for_replay();

        if let Some(snapshot) = snapshot {
            for entry in &snapshot.entries {
                store
                    .restore_snapshot_entry(entry.clone())
                    .map_err(|error| PersistenceError::Corruption(error.to_string()))?;
            }
        }

        for (database, mutation) in &mutations {
            apply_mutation(&mut store, *database, mutation)?;
        }
        store.resume_expiration_after_replay(replay_clock);
        store.expire_cycle(usize::MAX);

        shards.push(ShardIntegrity {
            shard_id,
            snapshot_sequence,
            snapshot_entries,
            wal,
            recovered_keys: store.len(),
        });
    }

    Ok(IntegrityReport {
        data_dir,
        clock_unix_millis: clock.map(UnixMillis::get),
        checkpoint: checkpoint.map(|checkpoint| CheckpointSummary {
            generation: checkpoint.generation,
            created_at_unix_millis: checkpoint.created_at.get(),
            shard_count: checkpoint.shards.len(),
        }),
        shards,
    })
}

pub fn backup_data_dir(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<IntegrityReport, PersistenceError> {
    backup_data_dir_with(source, destination, &BackupSeal::default())
}

pub fn backup_data_dir_with(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    seal: &BackupSeal,
) -> Result<IntegrityReport, PersistenceError> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    let _lock = lock_directory(source)?;

    if destination.exists() && destination.read_dir()?.next().is_some() {
        return Err(PersistenceError::Failed(format!(
            "backup destination {} is not empty",
            destination.display()
        )));
    }

    copy_persistence_tree(source, destination)?;
    write_backup_signature(destination, seal)?;
    // The report below must not be printed for files that only sit in the page cache.
    sync_tree(destination)?;

    if let Some(parent) = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        sync_directory(parent)?;
    }

    verify_backup_signature(destination, &seal.key)?;
    check_data_dir(destination)
}

pub fn restore_data_dir(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    force: bool,
) -> Result<IntegrityReport, PersistenceError> {
    restore_data_dir_with(source, destination, force, &[])
}

pub fn restore_data_dir_with(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    force: bool,
    key: &[u8],
) -> Result<IntegrityReport, PersistenceError> {
    let source = source.as_ref();
    let destination = destination.as_ref();

    verify_backup_signature(source, key)?;
    let report = check_data_dir(source)?;

    fs::create_dir_all(destination)?;
    let _lock = lock_directory(destination)?;

    if !force && persistence_tree_exists(destination)? {
        return Err(PersistenceError::Failed(format!(
            "restore destination {} already contains persistence files; pass --force to replace them",
            destination.display()
        )));
    }

    // Stage a full copy first; CURRENT is written last, so an interrupted restore never
    // leaves CURRENT naming files that are not there.
    let staging = destination.join(RESTORE_STAGING);

    remove_if_present(&staging)?;
    copy_persistence_tree(source, &staging)?;
    let current = staging.join("CURRENT");
    let current_bytes = match fs::read(&current) {
        Ok(bytes) => {
            fs::remove_file(&current)?;
            Some(bytes)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    sync_tree(&staging)?;
    // From here until the backup is in place the directory is incomplete; the marker keeps
    // a server from starting on it after a crash.
    fs::File::create(destination.join(RESTORE_MARKER))?.sync_all()?;
    sync_directory(destination)?;
    // The destination's layout file describes the data being replaced, never the backup.
    remove_persistence_tree(destination)?;
    remove_if_present(&destination.join("SHARD_LAYOUT"))?;
    forget_write_history(destination)?;
    sync_directory(destination)?;
    for entry in fs::read_dir(&staging)? {
        let entry = entry?;

        fs::rename(entry.path(), destination.join(entry.file_name()))?;
    }
    sync_directory(destination)?;
    if let Some(bytes) = current_bytes {
        let temporary = destination.join(".CURRENT.tmp");
        let mut file = fs::File::create(&temporary)?;

        io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, destination.join("CURRENT"))?;
        sync_directory(destination)?;
    }
    fs::remove_dir(&staging)?;
    sync_directory(destination)?;
    fs::remove_file(destination.join(RESTORE_MARKER))?;
    sync_directory(destination)?;
    let restored = check_data_dir(destination)?;

    if restored.shards.len() != report.shards.len() {
        return Err(PersistenceError::Corruption(
            "restored shard set does not match the backup".into(),
        ));
    }

    Ok(restored)
}

/// Where a restore assembles its copy inside the destination (same file system, so the final
/// moves are renames). A leftover from an interrupted restore is discarded.
const RESTORE_STAGING: &str = ".restore-staging";

/// Present from the moment a restore starts removing the old data until the backup is fully
/// in place. A server refuses to start on a directory that holds it.
pub(crate) const RESTORE_MARKER: &str = "RESTORE_IN_PROGRESS";

fn remove_if_present(path: &Path) -> io::Result<()> {
    let removed = if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };

    match removed {
        Err(error) if error.kind() != ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Windows refuses to flush a handle without write access (`ERROR_ACCESS_DENIED`), so the
/// file is opened for writing there; Unix syncs through any descriptor, which also works for
/// a file the user made read-only.
fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    let file = fs::OpenOptions::new().write(true).open(path)?;
    #[cfg(not(windows))]
    let file = fs::File::open(path)?;

    file.sync_all()
}

/// fsyncs every file and directory under `root`, `root` included.
fn sync_tree(root: &Path) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;

        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            sync_file(&entry.path())?;
        }
    }
    sync_directory(root)
}

fn lock_directory(path: &Path) -> Result<Option<DataDirectoryLock>, PersistenceError> {
    DataDirectoryLock::acquire(&PersistenceConfig {
        profile: DurabilityProfile::Always,
        data_dir: path.to_path_buf(),
        ..PersistenceConfig::default()
    })
}

fn persistence_tree_exists(path: &Path) -> io::Result<bool> {
    Ok(path.join("CURRENT").exists()
        || path.join("CLOCK").exists()
        || path.join("wal").exists()
        || path.join("checkpoints").exists()
        || !catchup_markers(path)?.is_empty())
}

fn catchup_markers(directory: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut markers = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(shard) = name.strip_prefix("REPLICA_CATCHUP_")
            && let Ok(shard) = shard.parse::<u32>()
            && name == format!("REPLICA_CATCHUP_{shard:04}")
        {
            markers.push(entry.path());
        }
    }
    Ok(markers)
}

fn remove_persistence_tree(path: &Path) -> io::Result<()> {
    for marker in catchup_markers(path)? {
        fs::remove_file(marker)?;
    }
    for name in ["CURRENT", "CLOCK", "wal", "checkpoints"] {
        let child = path.join(name);

        if !child.exists() {
            continue;
        }

        if child.is_dir() {
            fs::remove_dir_all(child)?;
        } else {
            fs::remove_file(child)?;
        }
    }

    Ok(())
}

/// A restored directory starts a new write history. Its sequence numbers go back to the
/// backup's while replicas may already hold later writes under the same numbers; keeping the
/// old `HISTORY` would let such a replica resume (and merge two logs) once the restored
/// primary passed its position. Without the file the server creates a new id, and replicas
/// take a full copy. Upstream records and lineages go too: the restored log is not the one
/// they described.
fn forget_write_history(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name == "HISTORY"
            || name.starts_with("UPSTREAM")
            || name.starts_with("LINEAGE")
            || (name.starts_with("HISTORY.") && name.ends_with(".tmp"))
        {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn copy_persistence_tree(source: &Path, destination: &Path) -> Result<(), PersistenceError> {
    fs::create_dir_all(destination)?;

    for name in ["CURRENT", "CLOCK", "SHARD_LAYOUT"] {
        copy_if_present(&source.join(name), &destination.join(name))?;
    }

    copy_dir_if_present(
        &source.join("checkpoints"),
        &destination.join("checkpoints"),
    )?;
    copy_dir_if_present(&source.join("wal"), &destination.join("wal"))?;
    for marker in catchup_markers(source)? {
        fs::copy(&marker, destination.join(marker.file_name().unwrap()))?;
    }
    Ok(())
}

fn copy_if_present(source: &Path, destination: &Path) -> Result<(), PersistenceError> {
    match fs::copy(source, destination) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn copy_dir_if_present(source: &Path, destination: &Path) -> Result<(), PersistenceError> {
    let entries = match fs::read_dir(source) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    fs::create_dir_all(destination)?;

    for entry in entries {
        let entry = entry?;
        let from = entry.path();
        let to = destination.join(entry.file_name());

        if entry.file_type()?.is_dir() {
            copy_dir_if_present(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::{Mutation, MutationSink};

    use crate::persistence::{WalSink, snapshot::ShardCheckpoint};

    use crate::store::{SnapshotEntry, SnapshotValue};

    #[test]
    fn checks_backups_and_restores_a_wal_directory() {
        let source = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            wal_segment_bytes: 64 * 1024,
            ..PersistenceConfig::default()
        };

        let limits = ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: b"value",
        })
        .unwrap();
        drop(sink);
        CheckpointManager::new(config, limits)
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: vec![SnapshotEntry {
                        database: 0,
                        key: b"key".to_vec(),
                        expires_at: None,
                        value: SnapshotValue::Bytes(b"value".to_vec()),
                    }],
                }],
            )
            .unwrap();

        let checked = check_data_dir(source.path()).unwrap();

        assert_eq!(checked.checkpoint.unwrap().generation, 1);
        assert_eq!(checked.shards[0].recovered_keys, 1);

        let marker = "REPLICA_CATCHUP_0000";
        fs::write(source.path().join(marker), b"preparing\n").unwrap();
        let backup = tempfile::tempdir().unwrap();

        backup_data_dir(source.path(), backup.path()).unwrap();
        assert_eq!(
            fs::read(backup.path().join(marker)).unwrap(),
            b"preparing\n"
        );
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = restore_data_dir(backup.path(), restored_dir.path(), false).unwrap();

        assert_eq!(restored.shards[0].recovered_keys, 1);
        assert_eq!(restored.shards[0].snapshot_sequence, 1);
        assert_eq!(
            fs::read(restored_dir.path().join(marker)).unwrap(),
            b"preparing\n"
        );

        fs::remove_file(source.path().join(marker)).unwrap();
        let complete_backup = tempfile::tempdir().unwrap();
        backup_data_dir(source.path(), complete_backup.path()).unwrap();
        assert!(restore_data_dir(complete_backup.path(), restored_dir.path(), false).is_err());
        restore_data_dir(complete_backup.path(), restored_dir.path(), true).unwrap();
        assert!(!restored_dir.path().join(marker).exists());

        let marker_only = tempfile::tempdir().unwrap();
        fs::write(marker_only.path().join(marker), b"preparing\n").unwrap();
        assert!(restore_data_dir(complete_backup.path(), marker_only.path(), false).is_err());
    }

    /// A one-shard data directory with a checkpoint, a WAL record and its layout file.
    fn one_shard_source() -> tempfile::TempDir {
        let source = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = ProtocolLimits::default();

        super::super::validate_shard_layout(&config, 1, 1, limits).unwrap();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: b"value",
        })
        .unwrap();
        drop(sink);
        CheckpointManager::new(config, limits)
            .publish(
                1,
                &[ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: Vec::new(),
                }],
            )
            .unwrap();
        source
    }

    #[test]
    fn restore_brings_the_backup_shard_layout_and_leaves_no_staging() {
        let source = one_shard_source();
        let backup = tempfile::tempdir().unwrap();

        backup_data_dir(source.path(), backup.path()).unwrap();

        // The destination was last served with 8 shards and holds nothing else.
        let destination = tempfile::tempdir().unwrap();

        fs::write(
            destination.path().join("SHARD_LAYOUT"),
            "ruvio-shards-v1 8\n",
        )
        .unwrap();
        // A restore interrupted earlier left its staging area behind.
        fs::create_dir_all(destination.path().join(".restore-staging/wal")).unwrap();
        restore_data_dir(backup.path(), destination.path(), false).unwrap();

        assert_eq!(
            fs::read_to_string(destination.path().join("SHARD_LAYOUT")).unwrap(),
            "ruvio-shards-v1 1\n"
        );
        assert!(!destination.path().join(".restore-staging").exists());
        assert_eq!(
            fs::read(destination.path().join("CURRENT")).unwrap(),
            fs::read(source.path().join("CURRENT")).unwrap()
        );
    }

    #[test]
    fn a_restore_interrupted_after_removing_the_old_data_blocks_startup() {
        let destination = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: destination.path().to_path_buf(),
            ..PersistenceConfig::default()
        };

        // A crash after the old tree was removed and before the backup was moved in: an
        // empty directory would otherwise start cleanly with no keys.
        fs::create_dir_all(destination.path().join(".restore-staging/wal")).unwrap();
        fs::write(destination.path().join(RESTORE_MARKER), b"").unwrap();
        let checked = super::super::validate_shard_layout(&config, 1, 1, ProtocolLimits::default());
        let message = format!("{checked:?}");

        assert!(checked.is_err() && message.contains("restore"), "{message}");

        // Running the restore again finishes it and clears the marker.
        let source = one_shard_source();
        let backup = tempfile::tempdir().unwrap();
        backup_data_dir(source.path(), backup.path()).unwrap();
        restore_data_dir(backup.path(), destination.path(), true).unwrap();
        assert!(!destination.path().join(RESTORE_MARKER).exists());
        super::super::validate_shard_layout(&config, 1, 1, ProtocolLimits::default()).unwrap();
    }

    #[test]
    fn startup_layout_check_does_not_trust_a_stale_layout_file() {
        let source = one_shard_source();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            ..PersistenceConfig::default()
        };

        // Data for one shard under a layout file that claims eight.
        fs::write(source.path().join("SHARD_LAYOUT"), "ruvio-shards-v1 8\n").unwrap();
        let checked = super::super::validate_shard_layout(&config, 8, 8, ProtocolLimits::default());

        assert!(checked.is_err(), "{checked:?}");
    }

    #[test]
    fn restore_starts_a_new_write_history() {
        let source = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            wal_segment_bytes: 64 * 1024,
            ..PersistenceConfig::default()
        };
        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: b"value",
        })
        .unwrap();
        drop(sink);
        let backup = tempfile::tempdir().unwrap();

        backup_data_dir(source.path(), backup.path()).unwrap();

        // The server that keeps running in this directory had id `old` and replicas of it.
        let target = tempfile::tempdir().unwrap();
        let old = crate::replication::load_or_create_history(target.path()).unwrap();

        crate::replication::record_upstream_history(
            target.path(),
            0,
            &crate::replication::Origin {
                history: [5; 16],
                epoch: 0,
            },
        )
        .unwrap();
        fs::write(target.path().join("UPSTREAM"), [6; 16]).unwrap();

        for force in [false, true] {
            if force {
                // Restoring over existing data needs --force and also drops the old id.
                crate::replication::load_or_create_history(target.path()).unwrap();
            }

            restore_data_dir(backup.path(), target.path(), force).unwrap();

            for name in ["HISTORY", "UPSTREAM", "UPSTREAM_0000"] {
                assert!(
                    !target.path().join(name).exists(),
                    "{name} survived restore"
                );
            }

            assert_ne!(
                crate::replication::load_or_create_history(target.path()).unwrap(),
                old
            );
        }
    }

    #[test]
    fn inspect_rejects_mid_log_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            wal_segment_bytes: 64 * 1024,
            ..PersistenceConfig::default()
        };

        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

        for key in [b"a".as_slice(), b"b".as_slice()] {
            sink.record(Mutation::SetBytes {
                key,
                value: b"value",
            })
            .unwrap();
        }

        drop(sink);

        let wal = directory
            .path()
            .join("wal/shard-0000")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut bytes = fs::read(&wal).unwrap();
        let corrupt_at = bytes.len() / 3;

        bytes[corrupt_at] ^= 0xff;
        fs::write(&wal, bytes).unwrap();

        assert!(matches!(
            check_data_dir(directory.path()),
            Err(PersistenceError::Corruption(_))
        ));
    }

    #[test]
    fn backup_fails_while_the_data_directory_is_locked() {
        let source = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            ..PersistenceConfig::default()
        };

        let _lock = DataDirectoryLock::acquire(&config).unwrap().unwrap();
        let destination = tempfile::tempdir().unwrap();

        assert!(matches!(
            backup_data_dir(source.path(), destination.path()),
            Err(PersistenceError::Failed(_))
        ));
    }

    #[test]
    fn a_signed_backup_rejects_a_planted_byte_without_the_key() {
        let source = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: source.path().to_path_buf(),
            wal_segment_bytes: 64 * 1024,
            ..PersistenceConfig::default()
        };

        let (mut sink, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

        sink.record(Mutation::SetBytes {
            key: b"key",
            value: b"value",
        })
        .unwrap();
        drop(sink);

        let backup = tempfile::tempdir().unwrap();
        let seal = crate::persistence::BackupSeal {
            signer: "ada".into(),
            key: b"backup-secret".to_vec(),
        };

        backup_data_dir_with(source.path(), backup.path(), &seal).unwrap();
        assert!(backup.path().join("SIGNATURE").is_file());

        let restored = tempfile::tempdir().unwrap();

        assert!(matches!(
            restore_data_dir(backup.path(), restored.path(), false),
            Err(PersistenceError::Failed(_))
        ));
        assert_eq!(
            restore_data_dir_with(backup.path(), restored.path(), false, b"backup-secret")
                .unwrap()
                .shards[0]
                .recovered_keys,
            1
        );

        let wal = backup
            .path()
            .join("wal/shard-0000")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut bytes = fs::read(&wal).unwrap();
        let at = bytes.len() / 2;

        bytes[at] ^= 0xff;
        fs::write(&wal, bytes).unwrap();

        let planted = tempfile::tempdir().unwrap();

        assert!(matches!(
            restore_data_dir_with(backup.path(), planted.path(), false, b"backup-secret"),
            Err(PersistenceError::Corruption(_))
        ));
    }
}
