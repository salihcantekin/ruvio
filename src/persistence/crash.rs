#[cfg(test)]
use std::{
    cell::{Cell, RefCell},
    process,
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use std::{
    fs::File,
    io::{self, Write},
};

/// Where a crash harness may kill the process.
///
/// Release builds keep the same call sites, and the checks compile to the
/// normal write or sync. The armed path exists so tests can abort without
/// running destructors, which would flush a clean shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum CrashSite {
    Append = 1,
    Sync = 2,
    Rotate = 3,
    Snapshot = 4,
    Rename = 5,
    /// Every shard file of a checkpoint round is in place; the manifest is not.
    Manifest = 6,
    /// The sync that ends a WAL preallocation fill (fault injection only).
    #[allow(dead_code)]
    PrefillSync = 7,
}

#[cfg(test)]
static SITE: AtomicU8 = AtomicU8::new(0);
#[cfg(test)]
static REMAINING: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
thread_local! {
    static IO_FAULT: Cell<u8> = const { Cell::new(0) };
    static SYNC_HOLD: RefCell<Option<Gate>> = const { RefCell::new(None) };
}

/// Open once the test lets a held sync report its failure.
#[cfg(test)]
pub(crate) type Gate = Arc<(Mutex<bool>, Condvar)>;

/// Keeps the next background WAL sync with an injected fault from reporting its failure until
/// [`SyncHold::release`] or drop, so a test acts while that sync is still running instead of
/// guessing how long a thread takes. Dropping it on a failed assertion lets the sync finish.
#[cfg(test)]
pub(crate) struct SyncHold(Gate);

#[cfg(test)]
impl SyncHold {
    pub(crate) fn release(&self) {
        let (open, wake) = &*self.0;

        *open.lock().unwrap_or_else(PoisonError::into_inner) = true;
        wake.notify_all();
    }
}

#[cfg(test)]
impl Drop for SyncHold {
    fn drop(&mut self) {
        self.release();
    }
}

/// Fails the next sync on this thread like [`arm_io_fault`]; a background sync waits for the
/// returned hold before it reports the failure.
#[cfg(test)]
pub(crate) fn arm_held_sync_fault() -> SyncHold {
    let gate = Gate::default();

    arm_io_fault(CrashSite::Sync);
    SYNC_HOLD.with(|hold| *hold.borrow_mut() = Some(Arc::clone(&gate)));
    SyncHold(gate)
}

/// The hold armed with the sync fault just taken on this thread, if any.
#[cfg(test)]
pub(crate) fn take_sync_hold() -> Option<Gate> {
    SYNC_HOLD.with(|hold| hold.borrow_mut().take())
}

/// Waits until the hold is released; after 30 s the sync goes on, so a test that never
/// releases it fails on its own assertions instead of hanging.
#[cfg(test)]
pub(crate) fn wait_released(gate: &Gate) {
    let (open, wake) = &**gate;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut released = open.lock().unwrap_or_else(PoisonError::into_inner);

    while !*released {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };

        released = wake
            .wait_timeout(released, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
}

#[cfg(test)]
pub(crate) fn arm(site: CrashSite, kill_on_hit: u64) {
    REMAINING.store(kill_on_hit.max(1), Ordering::SeqCst);
    SITE.store(site as u8, Ordering::SeqCst);
}

/// Fail the next write at `site` with `ENOSPC` after a short write.
///
/// The fault lives on the calling thread, so parallel tests do not steal it.
/// Release builds do not contain the check.
#[cfg(test)]
pub(crate) fn arm_io_fault(site: CrashSite) {
    IO_FAULT.with(|fault| fault.set(site as u8));
}

#[cfg(test)]
pub(crate) fn take_io_fault(site: CrashSite) -> Option<io::Error> {
    IO_FAULT.with(|fault| {
        if fault.get() != site as u8 {
            return None;
        }

        fault.set(0);
        Some(io::Error::new(
            io::ErrorKind::StorageFull,
            "no space left on device",
        ))
    })
}

#[cfg(test)]
fn armed_hit(site: CrashSite) -> bool {
    if SITE.load(Ordering::Relaxed) != site as u8 {
        return false;
    }

    REMAINING.fetch_sub(1, Ordering::SeqCst) == 1
}

pub(crate) fn hit(site: CrashSite) {
    #[cfg(test)]
    if armed_hit(site) {
        process::abort();
    }

    #[cfg(not(test))]
    let _ = site;
}

pub(crate) fn hit_write(site: CrashSite, file: &mut File, bytes: &[u8]) -> io::Result<()> {
    #[cfg(test)]
    if armed_hit(site) {
        let partial = bytes.len() / 2;

        if partial > 0 {
            file.write_all(&bytes[..partial])?;
        }

        process::abort();
    }

    #[cfg(test)]
    if let Some(error) = take_io_fault(site) {
        let partial = bytes.len() / 2;

        if partial > 0 {
            file.write_all(&bytes[..partial])?;
        }

        return Err(error);
    }

    #[cfg(not(test))]
    let _ = site;

    file.write_all(bytes)
}

/// [`hit_write`] for a write at `offset` (the WAL writer's positional writes).
pub(crate) fn hit_write_at(
    site: CrashSite,
    file: &File,
    bytes: &[u8],
    offset: u64,
) -> io::Result<()> {
    #[cfg(test)]
    if armed_hit(site) {
        let partial = bytes.len() / 2;

        if partial > 0 {
            super::wal::write_all_at(file, &bytes[..partial], offset)?;
        }

        process::abort();
    }

    #[cfg(test)]
    if let Some(error) = take_io_fault(site) {
        let partial = bytes.len() / 2;

        if partial > 0 {
            super::wal::write_all_at(file, &bytes[..partial], offset)?;
        }

        return Err(error);
    }

    #[cfg(not(test))]
    let _ = site;

    super::wal::write_all_at(file, bytes, offset)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File, OpenOptions},
        io::Write,
        path::Path,
        process::{Command, Stdio},
        time::{Duration, Instant},
    };

    use tempfile::tempdir;

    use crate::{
        OwnedMutation,
        mutation::{Mutation, MutationSink},
        persistence::{
            CheckpointManager, DurabilityProfile, PersistenceConfig, ShardCheckpoint, WalSink,
        },
        protocol::ProtocolLimits,
        store::{SnapshotEntry, SnapshotValue},
    };

    use super::{CrashSite, arm};

    const TEST_NAME: &str = "persistence::crash::tests::killed_process_recovers_at_crash_sites";

    #[test]
    fn killed_process_recovers_at_crash_sites() {
        if std::env::var_os("RUVIO_CRASH_CHILD").is_some() {
            crash_child();
            std::process::exit(0);
        }

        for (site, after) in [
            ("append", 3),
            ("sync", 2),
            ("rotate", 1),
            ("snapshot", 2),
            ("rename", 1),
        ] {
            trial(site, after, false);
            if matches!(site, "append" | "sync" | "rotate") {
                trial(site, after, true);
            }
        }
    }

    fn trial(site: &str, after: u64, worker: bool) {
        let directory = tempdir().expect("tempdir");
        // The child's stderr goes to a file outside its data directory, so a full pipe cannot
        // stall it and the test reads it after the child is gone.
        let logs = tempdir().expect("tempdir");
        let log_path = logs.path().join("stderr");
        // Not while a parallel test probes ports: see `test_ports::spawn_lock`.
        let spawning = crate::test_ports::spawn_lock();
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .arg(TEST_NAME)
            .arg("--exact")
            .env("RUVIO_CRASH_CHILD", "1")
            .env("RUVIO_CRASH_SITE", site)
            .env("RUVIO_CRASH_AFTER", after.to_string())
            .env("RUVIO_CRASH_DIR", directory.path())
            .env("RUVIO_CRASH_WORKER", if worker { "1" } else { "0" })
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(File::create(&log_path).expect("child log"))
            .spawn()
            .expect("spawn crash child");
        drop(spawning);
        // Eight small records take milliseconds; a child that hangs fails the trial.
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("crash child status") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "crash child for {site} after {after} did not exit within 60 s: {}",
                    fs::read_to_string(&log_path).unwrap_or_default()
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let stderr = fs::read_to_string(&log_path).unwrap_or_default();

        assert!(
            directory.path().join("started").exists(),
            "crash child did not start for {site} after {after}: {stderr}"
        );

        // `abort` ends a Windows process with STATUS_STACK_BUFFER_OVERRUN.
        const WINDOWS_ABORT: i32 = 0xC000_0409_u32 as i32;
        if let Some(code) = status
            .code()
            .filter(|code| !(cfg!(windows) && *code == WINDOWS_ABORT))
        {
            assert_eq!(code, 0, "{site} after {after} exited {code}: {stderr}");
        }
        if worker {
            assert!(!status.success(), "worker crash site was not reached");
        }

        if matches!(site, "snapshot" | "rename") {
            assert_snapshot(directory.path());
        } else {
            assert_wal(directory.path());
        }
    }

    fn crash_child() {
        let directory = std::env::var("RUVIO_CRASH_DIR").expect("crash directory");
        let site = std::env::var("RUVIO_CRASH_SITE").expect("crash site");
        let after: u64 = std::env::var("RUVIO_CRASH_AFTER")
            .expect("crash hit")
            .parse()
            .expect("hit count");
        let directory = Path::new(&directory);

        fs::write(directory.join("started"), &site).expect("started marker");
        match site.as_str() {
            "append" => wal_child(directory, CrashSite::Append, after),
            "sync" => wal_child(directory, CrashSite::Sync, after),
            "rotate" => wal_child(directory, CrashSite::Rotate, after),
            "snapshot" => snapshot_child(directory, CrashSite::Snapshot, after),
            "rename" => snapshot_child(directory, CrashSite::Rename, after),
            other => panic!("unknown crash site {other}"),
        }
    }

    fn wal_config(directory: &Path) -> PersistenceConfig {
        PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: directory.to_path_buf(),
            wal_segment_bytes: 4096,
            sync_interval: std::time::Duration::from_secs(60),
            ..PersistenceConfig::default()
        }
    }

    fn wal_child(directory: &Path, site: CrashSite, after: u64) {
        let limits = ProtocolLimits::default();
        let mut config = wal_config(directory);
        config.sync_worker_active = std::env::var("RUVIO_CRASH_WORKER").as_deref() == Ok("1");
        let worker = config.sync_worker_active;
        let (mut sink, _, _) = WalSink::open(config, 0, limits).expect("open");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        arm(site, after);
        let value = vec![b'x'; 1_800];
        let ack_path = directory.join("acked.txt");

        for index in 0..8 {
            if worker {
                sink.begin_group();
            }
            if let Err(error) = sink.record(Mutation::SetBytes {
                key: format!("k{index:02}").as_bytes(),
                value: &value,
            }) {
                eprintln!("record {index} failed: {error}");
                std::process::exit(2);
            }
            if worker {
                sink.end_group().expect("group publish");
                let barrier = sink
                    .prepare_sync_barrier()
                    .expect("submit sync")
                    .expect("worker");
                runtime
                    .block_on(async {
                        tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                            .await
                            .expect("durability barrier timed out")
                    })
                    .expect("durability barrier");
            }

            let mut ack = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&ack_path)
                .expect("ack");

            writeln!(ack, "{index}").expect("ack line");
            ack.sync_all().expect("ack sync");
        }
    }

    fn snapshot_child(directory: &Path, site: CrashSite, after: u64) {
        let config = PersistenceConfig {
            profile: DurabilityProfile::Snapshot,
            data_dir: directory.to_path_buf(),
            ..PersistenceConfig::default()
        };

        let manager = CheckpointManager::new(config, ProtocolLimits::default());

        manager.publish(1, &[shard(1)]).expect("first publish");
        arm(site, after);

        if let Err(error) = manager.publish(2, &[shard(2)]) {
            eprintln!("second publish failed: {error}");
            std::process::exit(2);
        }
    }

    fn shard(sequence: u64) -> ShardCheckpoint {
        ShardCheckpoint {
            shard_id: 0,
            last_sequence: sequence,
            entries: vec![SnapshotEntry {
                database: 0,
                key: b"kept".to_vec(),
                expires_at: None,
                value: SnapshotValue::Bytes(b"yes".to_vec()),
            }],
        }
    }

    fn assert_wal(directory: &Path) {
        let (_sink, mutations, _) =
            WalSink::open(wal_config(directory), 0, ProtocolLimits::default())
                .expect("WAL recovery");
        let acked = fs::read_to_string(directory.join("acked.txt")).unwrap_or_default();
        let acked: Vec<usize> = acked
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| line.parse().expect("ack index"))
            .collect();
        let keys: Vec<Vec<u8>> = mutations
            .iter()
            .map(|(_, mutation)| match mutation {
                OwnedMutation::SetBytes { key, .. } => key.clone(),
                other => panic!("unexpected mutation {other:?}"),
            })
            .collect();

        for index in acked {
            let key = format!("k{index:02}").into_bytes();

            assert!(
                keys.iter().any(|found| found == &key),
                "acknowledged key k{index:02} missing after crash"
            );
        }

        for (position, key) in keys.iter().enumerate() {
            assert_eq!(key, &format!("k{position:02}").into_bytes());
        }
    }

    fn assert_snapshot(directory: &Path) {
        let config = PersistenceConfig {
            profile: DurabilityProfile::Snapshot,
            data_dir: directory.to_path_buf(),
            ..PersistenceConfig::default()
        };

        let loaded = CheckpointManager::new(config, ProtocolLimits::default())
            .load()
            .expect("checkpoint recovery")
            .expect("published generation");

        assert!(loaded.generation == 1 || loaded.generation == 2);
        assert_eq!(loaded.shards[0].entries[0].key, b"kept");
        assert_eq!(
            loaded.shards[0].entries[0].value,
            SnapshotValue::Bytes(b"yes".to_vec())
        );
    }
}
