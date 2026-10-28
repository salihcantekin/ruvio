use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{
    OwnedMutation,
    mutation::{Mutation, MutationSink},
    protocol::ProtocolLimits,
};

use super::{
    DurabilityProfile, DurableSequence, PersistenceConfig, PersistenceError,
    codec::{
        FORMAT_MINOR, RECORD_MIN_LEN, WAL_HEADER_LEN, decode_record, decode_segment_header,
        encode_borrowed_record_appended, encode_borrowed_record_into, encode_segment_header,
        record_header_plausible, verify_record_frame,
    },
    crash, snapshot, sync_worker,
};

mod append;
mod prefill;
mod reader;
mod recovery;
mod segment;
mod sink;
mod sync;

use prefill::*;
use recovery::*;
use segment::*;

#[cfg(test)]
pub(crate) use reader::read_bounded_history;
pub use reader::read_frames;
pub(crate) use reader::{read_frames_chunk, read_frames_resuming};
pub(crate) use segment::{discover_wal_shards, inspect_shard_wal};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub records_replayed: u64,
    pub last_sequence: u64,
    pub torn_tail_bytes: u64,
}

/// Replayed WAL records with the database each one belongs to.
pub type RecoveredMutations = Vec<(u8, OwnedMutation)>;

/// Bytes staged for one client batch before a single `write`.
const WAL_GROUP_BYTES: usize = 64 * 1024;

/// The first error a background sync or fill reported, for the shard thread to pick up. The
/// flag lets every append check it with one load instead of locking the mutex.
#[derive(Default)]
struct SyncError {
    set: AtomicBool,
    message: Mutex<Option<String>>,
}

impl SyncError {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.message
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Records `message`, replacing an earlier one.
    fn report(&self, message: String) {
        let mut slot = self.lock();

        *slot = Some(message);
        self.set.store(true, Ordering::Release);
    }

    /// Records `message` unless an error is already waiting.
    fn report_first(&self, message: String) {
        let mut slot = self.lock();

        slot.get_or_insert(message);
        self.set.store(true, Ordering::Release);
    }

    fn is_set(&self) -> bool {
        self.set.load(Ordering::Acquire)
    }

    fn take(&self) -> Option<String> {
        if !self.is_set() {
            return None;
        }
        let mut slot = self.lock();

        self.set.store(false, Ordering::Release);
        slot.take()
    }
}
/// How far ahead of the write cursor a segment is extended with `fallocate`.
const WAL_PREALLOC_CHUNK: u64 = 4 * 1024 * 1024;
/// A new chunk is requested when less than this much written-zero space is left ahead of the cursor.
const WAL_PREALLOC_AHEAD: u64 = 3 * 1024 * 1024;

/// Data directories whose WAL syncs fail (fault injection for tests that run a server).
#[cfg(test)]
pub(crate) static FAILING_SYNC_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Like `FAILING_SYNC_DIRS`, for one shard's WAL of a data directory.
#[cfg(test)]
pub(crate) static FAILING_SYNC_SHARDS: Mutex<Vec<(PathBuf, u32)>> = Mutex::new(Vec::new());

/// Like `FAILING_SYNC_SHARDS`, but only the next sync of that shard's WAL fails.
#[cfg(test)]
pub(crate) static FAILING_NEXT_SYNC_SHARDS: Mutex<Vec<(PathBuf, u32)>> = Mutex::new(Vec::new());

/// One shard's WAL of a data directory whose next writes fail, as many as the count says
/// (fault injection for tests that run a server; the log resumes by itself afterwards).
#[cfg(test)]
pub(crate) static FAILING_WRITE_SHARDS: Mutex<Vec<(PathBuf, u32, u32)>> = Mutex::new(Vec::new());

/// Data directories whose WAL file writes are counted (for tests that run a server).
#[cfg(test)]
pub(crate) static COUNTED_WRITE_DIRS: Mutex<Vec<(PathBuf, u64)>> = Mutex::new(Vec::new());

#[cfg(test)]
fn count_write(data_dir: &Path) {
    let mut counted = COUNTED_WRITE_DIRS
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if let Some((_, writes)) = counted.iter_mut().find(|(dir, _)| dir == data_dir) {
        *writes += 1;
    }
}

#[cfg(test)]
fn injected_write_fault(data_dir: &Path, shard_id: u32) -> Option<io::Error> {
    let mut failing = FAILING_WRITE_SHARDS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let (_, _, left) = failing
        .iter_mut()
        .find(|(dir, shard, left)| dir == data_dir && *shard == shard_id && *left > 0)?;

    *left -= 1;
    Some(io::Error::new(
        io::ErrorKind::StorageFull,
        "injected write failure",
    ))
}

#[cfg(test)]
fn injected_sync_fault(data_dir: &Path, shard_id: u32) -> Option<io::Error> {
    super::crash::take_io_fault(super::crash::CrashSite::Sync)
        .or_else(|| {
            FAILING_SYNC_DIRS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .any(|dir| dir == data_dir)
                .then(|| io::Error::other("injected sync failure"))
        })
        .or_else(|| {
            FAILING_SYNC_SHARDS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .any(|(dir, shard)| dir == data_dir && *shard == shard_id)
                .then(|| io::Error::other("injected sync failure"))
        })
        .or_else(|| {
            let mut once = FAILING_NEXT_SYNC_SHARDS
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let index = once
                .iter()
                .position(|(dir, shard)| dir == data_dir && *shard == shard_id)?;

            once.remove(index);
            Some(io::Error::other("injected sync failure"))
        })
}

/// How many times a connected replica's WAL hold was dropped for exceeding the budget.
pub(crate) static REPLICA_HOLDS_DROPPED: AtomicU64 = AtomicU64::new(0);

pub struct WalSink {
    profile: DurabilityProfile,
    config: PersistenceConfig,
    limits: ProtocolLimits,
    shard_id: u32,
    next_sequence: u64,
    segment_id: u64,
    segment_first_sequence: u64,
    file: Option<File>,
    file_length: u64,
    /// Bytes physically written; `file` is not in append mode so a preallocated tail does not move the cursor.
    write_pos: u64,
    /// End of what the last successful shard-thread `fsync` of the open segment covered.
    /// Not advanced by the sync worker, so only used where the worker is off.
    synced_pos: u64,
    /// Size the segment file has been extended to by `fallocate` (equals `write_pos` when not preallocating).
    prealloc_end: u64,
    prealloc: bool,
    prefill: Prefill,
    dirty_bytes: u64,
    /// The last sync barrier handed to the worker: its sequence and `dirty_bytes` then.
    /// Once that sequence is durable those bytes are synced, so steady writes, where a
    /// new record always follows before the check, do not grow `dirty_bytes` forever.
    barrier_mark: (u64, u64),
    last_sync: Instant,
    failed: Option<String>,
    /// An `fsync` failed. The kernel may have dropped the dirty pages it covered and report a
    /// later sync as clean, so nothing written before it can be trusted as durable again:
    /// writes stay refused until a restart, whose recovery reads what actually reached disk.
    sync_failed: bool,
    /// The sequence of the last record of a hot-key write, which the shard applied to the
    /// shared hot store before the record synced (see `sync_hot_records`).
    hot_through: u64,
    last_recovery: Option<Instant>,
    published: Arc<AtomicU64>,
    durable: Arc<DurableSequence>,
    backlog_floor: Arc<AtomicU64>,
    replica_count: Arc<AtomicU64>,
    database: u8,
    staged: Vec<u8>,
    /// Sequence of the first record in `staged`; meaningful only while it is non-empty.
    staged_first_sequence: u64,
    record: Vec<u8>,
    group: u32,
    group_bytes: u64,
    /// One level of `group` is held for the shard's current scheduler turn (`hold_for_tick`).
    tick_hold: bool,
    sync_running: Arc<AtomicBool>,
    sync_error: Arc<SyncError>,
    /// Bytes covered by the running background `everysec` sync; still counted in `dirty_bytes`.
    sync_in_flight: u64,
    sync_worker: Option<super::sync_worker::SyncWorker>,
    sync_worker_file: Option<Arc<File>>,
    /// `fsync`s issued on the shard thread (rotation, shutdown, fallback, no worker) and their time.
    inline_syncs: u64,
    inline_sync_nanos: u64,
}

struct ClearSync(Arc<AtomicBool>);

impl Drop for ClearSync {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Drop for WalSink {
    fn drop(&mut self) {
        self.wait_background_sync();
        self.shrink_to_written();
    }
}

impl MutationSink for WalSink {
    type Error = PersistenceError;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.append(mutation)
    }

    fn record_now(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.append_as::<true>(mutation)
    }

    fn select_database(&mut self, database: u8) {
        self.database = database;
    }

    fn persists(&self) -> bool {
        self.profile.uses_wal()
    }

    fn written_sequence(&self) -> Option<u64> {
        self.profile.uses_wal().then(|| self.last_sequence())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameRead {
    Frames(Vec<Vec<u8>>),
    Gap,
    Ahead,
    Local,
}

/// Where a reader that follows the log stopped: the segment, the byte offset just past
/// the last record it returned, and the sequence that record's successor carries. A
/// chunk that starts there seeks straight to it instead of decoding the segment again
/// from its header, so following a long segment costs its length once, not per chunk.
///
/// The segment stays open between chunks, so a reader that keeps up with the live segment
/// lists the directory and opens a file only when the log rolls over to a new segment.
#[derive(Debug, Default)]
pub(crate) struct FrameCursor {
    position: Option<(PathBuf, u64)>,
    next_sequence: u64,
    /// The segment `position` names, still open.
    open: Option<OpenSegment>,
}

#[derive(Debug)]
struct OpenSegment {
    file: io::BufReader<File>,
    /// The file's length when last looked at; the live segment can grow past it.
    length: u64,
    /// Which file this is, so a compacted or replaced segment is not read on.
    identity: FileIdentity,
}

#[cfg(unix)]
type FileIdentity = (u64, u64);
#[cfg(not(unix))]
type FileIdentity = ();

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;

    (metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
fn file_identity(_: &fs::Metadata) -> FileIdentity {}

struct Segment {
    path: PathBuf,
    segment_id: u64,
    first_sequence: u64,
    /// The sequence the next record appended to this segment must carry.
    next_sequence: u64,
    /// The format minor its header declares.
    minor: u16,
}

#[cfg(test)]
mod tests;
