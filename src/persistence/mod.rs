mod codec;
mod crash;
mod signature;
mod snapshot;
mod sync_worker;
pub(crate) use sync_worker::SyncBarrier;
mod tooling;
mod wal;
#[cfg(test)]
pub(crate) use snapshot::{CARRY_DELAYS, FAILING_COMMITS, SHARD_FILE_DELAYS};
#[cfg(test)]
pub(crate) use wal::{
    COUNTED_WRITE_DIRS, FAILING_NEXT_SYNC_SHARDS, FAILING_SYNC_DIRS, FAILING_SYNC_SHARDS,
    FAILING_WRITE_SHARDS,
};

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use serde::Deserialize;

use crate::{KvShard, OwnedMutation, time::UnixMillis};

// Only the evented tests arm faults, and Miri does not build them.
#[cfg(all(test, not(miri)))]
pub(crate) use crash::{CrashSite, arm_io_fault};
pub(crate) use snapshot::{
    CheckpointManager, ShardCheckpoint, ShardFile, SnapshotWriter, decode_entry_frame,
    encode_entry, encode_entry_with,
};

// The shipper consumes these in a later snapshot-resync task.
#[allow(unused_imports)]
pub(crate) use snapshot::{ResyncBytes, decode_snapshot_blob, load_resync_bytes};

pub use signature::{BackupSeal, verify_backup_signature};

pub use tooling::{
    CheckpointSummary, IntegrityReport, ShardIntegrity, backup_data_dir, backup_data_dir_with,
    check_data_dir, restore_data_dir, restore_data_dir_with,
};

pub(crate) use wal::REPLICA_HOLDS_DROPPED;
pub(crate) use wal::{FrameCursor, read_frames_resuming};
pub use wal::{FrameRead, RecoveredMutations, RecoveryReport, WalSink, read_frames};

#[cfg(test)]
pub(crate) use codec::encode_record;
pub(crate) use codec::{WAL_RECORD_MAX_BYTES, decode_record, record_sequence};
pub(crate) use codec::{decode_dump_record, encode_dump_record};
pub(crate) use wal::discover_wal_shards;
#[cfg(test)]
pub(crate) use wal::read_bounded_history;

/// Keep a durable owner count even before the first checkpoint or WAL segment.
pub(crate) fn validate_shard_layout(
    config: &PersistenceConfig,
    shard_count: usize,
    checkpoint_partitions: usize,
    limits: crate::protocol::ProtocolLimits,
) -> Result<(), PersistenceError> {
    if config.profile == DurabilityProfile::Memory {
        return Ok(());
    }

    if config.data_dir.join(tooling::RESTORE_MARKER).exists() {
        return Err(PersistenceError::Failed(format!(
            "a restore into {} did not finish (found {}); run `ruvio-ctl restore` again \
             before starting the server",
            config.data_dir.display(),
            tooling::RESTORE_MARKER
        )));
    }

    let path = config.data_dir.join("SHARD_LAYOUT");
    let expected = format!("ruvio-shards-v1 {shard_count}\n");
    let recorded = match fs::read_to_string(&path) {
        Ok(stored) if stored == expected => true,
        Ok(stored) => {
            return Err(PersistenceError::Failed(format!(
                "shard layout does not match configured shard count ({shard_count}): {stored:?}; offline migration is required"
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };

    // The data itself is checked even when the layout file agrees: a restore or a copy can
    // bring data of another shard count under it. The manifest is enough, not the entries.
    if let Some(plan) = CheckpointManager::new(config.clone(), limits).plan()?
        && plan.shard_count() != checkpoint_partitions
    {
        return Err(PersistenceError::Failed(
            "checkpoint shard count does not match configured shard count".into(),
        ));
    }

    let shards = discover_wal_shards(&config.data_dir)?;
    if !shards.is_empty()
        && (shards.len() != shard_count
            || shards
                .iter()
                .enumerate()
                .any(|(id, shard)| *shard as usize != id))
    {
        return Err(PersistenceError::Failed(format!(
            "WAL shard layout {shards:?} does not match configured shard count ({shard_count}); offline migration is required"
        )));
    }

    if recorded {
        return Ok(());
    }

    // Written beside the final name and renamed, so a crash leaves either no file or a whole
    // one; a partial `SHARD_LAYOUT` would make every later start refuse the directory.
    let temporary = config.data_dir.join(".SHARD_LAYOUT.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    use io::Write;
    file.write_all(expected.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &path)?;
    #[cfg(unix)]
    File::open(&config.data_dir)?.sync_all()?;
    Ok(())
}

pub struct DataDirectoryLock {
    _file: File,
}

impl DataDirectoryLock {
    pub fn acquire(config: &PersistenceConfig) -> Result<Option<Self>, PersistenceError> {
        if config.profile == DurabilityProfile::Memory {
            return Ok(None);
        }

        fs::create_dir_all(&config.data_dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.data_dir.join("LOCK"))?;

        // `flock` on Unix and `LockFileEx` on Windows, the same locks older builds took.
        file.try_lock().map_err(|error| {
            PersistenceError::Failed(format!(
                "data directory {} is already locked or unavailable: {error}",
                config.data_dir.display()
            ))
        })?;
        // Only now is no other process writing here: temp files are leftovers of a crash.
        snapshot::sweep_temporary_files(&config.data_dir);

        Ok(Some(Self { _file: file }))
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityProfile {
    #[default]
    Memory,
    Snapshot,
    Everysec,
    Always,
}

impl DurabilityProfile {
    pub const fn uses_wal(self) -> bool {
        matches!(self, Self::Everysec | Self::Always)
    }
}

impl fmt::Display for DurabilityProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Memory => "memory",
            Self::Snapshot => "snapshot",
            Self::Everysec => "everysec",
            Self::Always => "always",
        })
    }
}

impl FromStr for DurabilityProfile {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "memory" => Ok(Self::Memory),
            "snapshot" => Ok(Self::Snapshot),
            "everysec" => Ok(Self::Everysec),
            "always" => Ok(Self::Always),
            _ => Err("durability must be memory, snapshot, everysec, or always"),
        }
    }
}

/// Whether `always` fsyncs run on the sync worker thread instead of the shard thread.
/// `Auto` turns it on only where it is measured to pay off (see `AppConfig::resolve_sync_worker`).
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncWorkerMode {
    #[default]
    Auto,
    On,
    Off,
}

impl fmt::Display for SyncWorkerMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        })
    }
}

/// Whether a WAL segment is extended ahead of the cursor and zero-filled on a helper thread
/// (Linux only). `Auto` does it for `always`, where a sync of unwritten extents was measured
/// at 2.0 ms against 0.8 ms; `everysec` syncs once a second and the zero-fill only doubled
/// the bytes a throughput-capped disk saw (SET 2.1M/s against 4.1M/s without it), so `Auto`
/// skips it there.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WalPrefill {
    #[default]
    Auto,
    On,
    Off,
}

impl WalPrefill {
    pub fn enabled_for(self, profile: DurabilityProfile) -> bool {
        match self {
            Self::Auto => profile == DurabilityProfile::Always,
            Self::On => true,
            Self::Off => false,
        }
    }
}

impl fmt::Display for WalPrefill {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        })
    }
}

impl FromStr for WalPrefill {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "on" | "true" | "yes" | "1" => Ok(Self::On),
            "off" | "false" | "no" | "0" => Ok(Self::Off),
            _ => Err("wal_prefill must be auto, on, or off"),
        }
    }
}

impl FromStr for SyncWorkerMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "on" | "true" | "yes" | "1" => Ok(Self::On),
            "off" | "false" | "no" | "0" => Ok(Self::Off),
            _ => Err("sync_worker must be auto, on, or off"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistenceConfig {
    pub profile: DurabilityProfile,
    pub data_dir: PathBuf,
    pub wal_segment_bytes: u64,
    pub sync_interval: Duration,
    pub sync_worker: SyncWorkerMode,
    /// Resolved from `sync_worker` by `AppConfig::resolve_sync_worker`; library users set it directly.
    pub sync_worker_active: bool,
    pub snapshot_max_bytes: u64,
    pub snapshot_journal_bytes: usize,
    pub snapshot_interval: Duration,
    pub snapshot_min_mutations: u64,
    /// WAL bytes kept for change-stream subscribers; `0` keeps none beyond recovery needs.
    pub cdc_retain_bytes: u64,
    /// Most WAL a connected replica may hold back from compaction; past it the hold is
    /// dropped and the replica resyncs from a snapshot. `0` means no limit.
    pub replica_retain_max_bytes: u64,
    /// See [`WalPrefill`]. Env `RUVIO_WAL_PREFILL`; not a file key.
    pub wal_prefill: WalPrefill,
}

impl PersistenceConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        // `on` requires `always`: the worker only moves always-mode fsyncs off the shard thread.
        if self.sync_worker == SyncWorkerMode::On && self.profile != DurabilityProfile::Always {
            return Err("sync_worker = on requires always durability");
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err("data directory cannot be empty");
        }

        if self.wal_segment_bytes < 4096 {
            return Err("WAL segment size must be at least 4096 bytes");
        }

        if self.sync_interval.is_zero() {
            return Err("WAL sync interval must be greater than zero");
        }

        if self.snapshot_max_bytes < 4096 {
            return Err("snapshot size limit must be at least 4096 bytes");
        }

        if self.snapshot_journal_bytes == 0 {
            return Err("snapshot journal limit must be greater than zero");
        }

        Ok(())
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            profile: DurabilityProfile::Memory,
            data_dir: PathBuf::from("./data"),
            wal_segment_bytes: 256 * 1024 * 1024,
            sync_interval: Duration::from_secs(1),
            sync_worker: SyncWorkerMode::Auto,
            sync_worker_active: false,
            snapshot_max_bytes: 16 * 1024 * 1024 * 1024,
            snapshot_journal_bytes: 64 * 1024 * 1024,
            snapshot_interval: Duration::from_secs(60),
            snapshot_min_mutations: 0,
            cdc_retain_bytes: 0,
            replica_retain_max_bytes: 1 << 30,
            wal_prefill: WalPrefill::Auto,
        }
    }
}

#[derive(Debug)]
pub enum PersistenceError {
    Io(io::Error),
    Corruption(String),
    Unsupported(String),
    Failed(String),
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "persistence I/O error: {error}"),
            Self::Corruption(message) => write!(formatter, "persistence corruption: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "unsupported persistence format: {message}")
            }

            Self::Failed(message) => write!(formatter, "persistence is unavailable: {message}"),
        }
    }
}

impl std::error::Error for PersistenceError {}

/// A replayed mutation found its key holding another type than the log says it had.
fn type_conflict<E>(kind: &'static str) -> impl FnOnce(E) -> PersistenceError {
    move |_| PersistenceError::Corruption(format!("{kind} type conflict"))
}

impl From<io::Error> for PersistenceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Replays `mutation` in `database`. The shard's active database is left unchanged.
pub fn apply_mutation(
    shard: &mut KvShard,
    database: u8,
    mutation: &OwnedMutation,
) -> Result<(), PersistenceError> {
    let previous = shard.active_database();

    shard.enter(database);
    if shard.journal_is_open() {
        capture_for_snapshot(shard, database, mutation);
    }
    let applied = apply_in_active(shard, database, mutation);

    shard.enter(previous);
    if database == 0 && applied.is_ok() {
        shard.json_index_replayed(mutation);
    }
    applied
}

/// Settles, for a running background snapshot, every key `mutation` is about to change, as
/// the command path does before it writes. Without it a replica's snapshot would walk values
/// written after its snapshot point, and its WAL would replay them a second time.
fn capture_for_snapshot(shard: &mut KvShard, database: u8, mutation: &OwnedMutation) {
    match mutation {
        OwnedMutation::FlushDatabase => shard.capture_snapshot_databases(&[database]),
        OwnedMutation::FlushAll => {
            let databases = shard.database_ids();

            shard.capture_snapshot_databases(&databases);
        }
        OwnedMutation::SwapDatabases { first, second } => {
            shard.capture_snapshot_databases(&[*first, *second]);
        }
        OwnedMutation::MoveKey { key, destination } => {
            shard.capture_snapshot_key(key);
            shard.capture_snapshot_key_in(*destination, key);
        }
        OwnedMutation::Transaction { mutations } => {
            for mutation in mutations {
                capture_for_snapshot(shard, database, mutation);
            }
        }
        mutation => mutation.for_each_write_key(&mut |key| shard.capture_snapshot_key(key)),
    }
}

fn replay_json(
    shard: &mut KvShard,
    apply: impl FnOnce(&mut KvShard) -> Result<(), PersistenceError>,
) -> Result<(), PersistenceError> {
    let previous = shard.pause_expiration_for_replay();
    let result = apply(shard);
    shard.resume_expiration_after_replay(previous);
    result
}

fn apply_in_active(
    shard: &mut KvShard,
    database: u8,
    mutation: &OwnedMutation,
) -> Result<(), PersistenceError> {
    match mutation {
        OwnedMutation::SetBytes { key, value } => shard.set_bytes(key, value),
        OwnedMutation::SetBytesExpiring {
            key,
            value,
            expires_at,
        } => shard.set_bytes_expiring(key, value, crate::time::UnixMillis::new(*expires_at)),
        OwnedMutation::JsonPut { key, value } => replay_json(shard, |shard| {
            shard
                .put_json(key, value.as_slice())
                .map_err(|_| PersistenceError::Corruption("JSON replay failed".into()))
        })?,
        OwnedMutation::JsonSetPath { key, path, value } => replay_json(shard, |shard| {
            shard
                .apply_json_delta(key, path, Some(value), false)
                .map_err(|_| PersistenceError::Corruption("JSON path set replay failed".into()))
        })?,
        OwnedMutation::JsonMergePath { key, path, value } => replay_json(shard, |shard| {
            shard
                .apply_json_delta(key, path, Some(value), true)
                .map_err(|_| PersistenceError::Corruption("JSON path merge replay failed".into()))
        })?,
        OwnedMutation::JsonDelPath { key, path } => replay_json(shard, |shard| {
            shard
                .apply_json_delta(key, path, None, false)
                .map_err(|_| PersistenceError::Corruption("JSON path delete replay failed".into()))
        })?,
        OwnedMutation::IdempotencyPut {
            key,
            value,
            expires_at,
        } => {
            shard.set_bytes(key, value);
            shard.expire_at(key, crate::UnixMillis::new(*expires_at));
        }
        OwnedMutation::SetInteger { key, value } => shard
            .set_integer(key, *value)
            .map_err(type_conflict("integer"))?,
        OwnedMutation::Delete { keys } => {
            for key in keys {
                shard.delete(key);
            }
        }

        OwnedMutation::ListPushFront { key, values } => {
            shard
                .list_push_front(key, values)
                .map_err(type_conflict("list"))?;
        }

        OwnedMutation::ListPopFront { key } => {
            shard.list_pop_front(key).map_err(type_conflict("list"))?;
        }

        OwnedMutation::ListPushBack { key, values } => {
            shard
                .list_push_back(key, values)
                .map_err(type_conflict("list"))?;
        }

        OwnedMutation::ListPopBack { key } => {
            shard.list_pop_back(key).map_err(type_conflict("list"))?;
        }

        OwnedMutation::ListTrim { key, start, stop } => {
            let start = isize::try_from(*start)
                .map_err(|_| PersistenceError::Corruption("list trim start overflow".into()))?;
            let stop = isize::try_from(*stop)
                .map_err(|_| PersistenceError::Corruption("list trim stop overflow".into()))?;

            shard
                .list_trim(key, start, stop)
                .map_err(type_conflict("list"))?;
        }

        OwnedMutation::SetRange { key, offset, bytes } => {
            let offset = usize::try_from(*offset)
                .map_err(|_| PersistenceError::Corruption("SETRANGE offset overflow".into()))?;

            shard
                .set_range(key, offset, bytes)
                .map_err(type_conflict("string"))?;
        }

        OwnedMutation::ListSet { key, index, value } => {
            shard
                .list_set(key, *index, value)
                .map_err(type_conflict("list"))?
                .map_err(|_| PersistenceError::Corruption("LSET out of range".into()))?;
        }

        OwnedMutation::ListInsert {
            key,
            before,
            pivot,
            value,
        } => {
            shard
                .list_insert(key, *before, pivot, value)
                .map_err(type_conflict("list"))?;
        }

        OwnedMutation::ListRemove { key, count, value } => {
            shard
                .list_remove(key, *count, value)
                .map_err(type_conflict("list"))?;
        }

        OwnedMutation::SetAdd { key, members } => {
            shard.set_add(key, members).map_err(type_conflict("set"))?;
        }

        OwnedMutation::SetRemove { key, members } => {
            shard
                .set_remove(key, members)
                .map_err(type_conflict("set"))?;
        }

        OwnedMutation::SetStore { key, members } => {
            shard.set_store(key, members);
        }

        OwnedMutation::SetMove {
            source,
            destination,
            member,
        } => {
            if !shard
                .set_move(source, destination, member)
                .map_err(type_conflict("set"))?
            {
                return Err(PersistenceError::Corruption(
                    "set move source member missing during replay".into(),
                ));
            }
        }

        OwnedMutation::HashSet { key, pairs } => {
            shard.hash_set(key, pairs).map_err(type_conflict("hash"))?;
        }

        OwnedMutation::HashDelete { key, fields } => {
            shard
                .hash_delete(key, fields)
                .map_err(type_conflict("hash"))?;
        }

        OwnedMutation::HashFieldExpire {
            key,
            fields,
            unix_millis,
        } => {
            shard
                .hash_expire_fields(key, fields, *unix_millis)
                .map_err(type_conflict("hash"))?;
        }

        OwnedMutation::SemaphoreAcquire {
            key,
            token,
            expires_at,
        } => {
            shard
                .semaphore_put(key, *token, *expires_at)
                .map_err(|_| PersistenceError::Corruption("semaphore replay failed".into()))?;
        }

        OwnedMutation::SemaphoreRelease { key, token } => {
            if shard.semaphore_holds(key, *token) {
                shard
                    .semaphore_release(key, *token)
                    .map_err(|_| PersistenceError::Corruption("semaphore replay failed".into()))?;
            }
        }

        OwnedMutation::HashFieldPersist { key, fields } => {
            shard
                .hash_persist_fields(key, fields)
                .map_err(type_conflict("hash"))?;
        }

        OwnedMutation::SortedSetAdd { key, pairs } => {
            // Writes are checked for room before they are logged and replay packs the
            // arena at least as tightly, so a refusal is not expected. If it happens anyway
            // the state differs from the one that wrote the record (and a replica from its
            // primary): fail loudly rather than drop the write.
            if !shard.sorted_set_make_room(key, false, pairs.iter().map(|(_, m)| m.as_slice())) {
                return Err(PersistenceError::Corruption(format!(
                    "sorted-set replay has no room for a logged write to {}",
                    String::from_utf8_lossy(key)
                )));
            }
            shard
                .sorted_set_add_pairs(key, pairs)
                .map_err(|_| PersistenceError::Corruption("sorted-set replay failed".into()))?;
        }

        OwnedMutation::SortedSetRemove { key, members } => {
            shard
                .sorted_set_remove(key, members)
                .map_err(type_conflict("sorted-set"))?;
        }

        OwnedMutation::BloomReserve {
            key,
            error_rate,
            capacity,
            expansion,
        } => {
            let capacity = usize::try_from(*capacity)
                .map_err(|_| PersistenceError::Corruption("Bloom capacity overflow".into()))?;

            shard
                .bloom_reserve(key, *error_rate, capacity, *expansion)
                .map_err(|_| PersistenceError::Corruption("Bloom reserve replay failed".into()))?;
        }

        OwnedMutation::BloomAdd { key, value } => {
            shard
                .bloom_add(key, value)
                .map_err(|_| PersistenceError::Corruption("Bloom replay failed".into()))?;
        }

        OwnedMutation::BloomAddMany { key, values } => {
            shard
                .bloom_add_each(key, values, |_| {})
                .map_err(|_| PersistenceError::Corruption("Bloom replay failed".into()))?;
        }

        OwnedMutation::StreamAppend {
            key,
            millis,
            sequence,
            fields,
        } => {
            shard
                .stream_append(key, *millis, *sequence, fields)
                .map_err(|_| PersistenceError::Corruption("stream append replay failed".into()))?;
        }

        OwnedMutation::StreamDelete {
            key,
            millis,
            sequence,
        } => {
            shard
                .stream_delete(key, *millis, *sequence)
                .map_err(|_| PersistenceError::Corruption("stream delete replay failed".into()))?;
        }

        OwnedMutation::StreamTrim { key, max_len } => {
            let max_len = usize::try_from(*max_len)
                .map_err(|_| PersistenceError::Corruption("stream trim length overflow".into()))?;

            shard
                .stream_trim(key, max_len)
                .map_err(|_| PersistenceError::Corruption("stream trim replay failed".into()))?;
        }

        OwnedMutation::StreamTrimMinId {
            key,
            millis,
            sequence,
        } => {
            shard
                .stream_trim_min_id(key, *millis, *sequence)
                .map_err(|_| PersistenceError::Corruption("stream minid replay failed".into()))?;
        }

        OwnedMutation::StreamSetId {
            key,
            millis,
            sequence,
        } => {
            shard
                .stream_set_id(key, *millis, *sequence)
                .map_err(|_| PersistenceError::Corruption("stream set-id replay failed".into()))?;
        }

        OwnedMutation::StreamGroupCreate {
            key,
            group,
            millis,
            sequence,
            mkstream,
        } => {
            shard
                .stream_create_group(key, group, *millis, *sequence, *mkstream)
                .map_err(|_| {
                    PersistenceError::Corruption("stream group create replay failed".into())
                })?;
        }

        OwnedMutation::StreamClaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
        } => {
            shard
                .stream_group_claim(key, group, consumer, *millis, *sequence, *delivered_at)
                .map_err(|_| PersistenceError::Corruption("stream claim replay failed".into()))?;
        }

        OwnedMutation::StreamReclaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
            deliveries,
        } => {
            shard
                .stream_group_reclaim(
                    key,
                    group,
                    consumer,
                    crate::stream::ReclaimUpdate {
                        millis: *millis,
                        sequence: *sequence,
                        delivered_at: *delivered_at,
                        deliveries: *deliveries,
                    },
                )
                .map_err(|_| PersistenceError::Corruption("stream reclaim replay failed".into()))?;
        }

        OwnedMutation::StreamForceClaim {
            key,
            group,
            consumer,
            millis,
            sequence,
            delivered_at,
            deliveries,
        } => {
            shard
                .stream_group_force_claim(
                    key,
                    group,
                    consumer,
                    crate::stream::ReclaimUpdate {
                        millis: *millis,
                        sequence: *sequence,
                        delivered_at: *delivered_at,
                        deliveries: *deliveries,
                    },
                )
                .map_err(|_| {
                    PersistenceError::Corruption("stream force claim replay failed".into())
                })?;
        }

        OwnedMutation::StreamAck {
            key,
            group,
            millis,
            sequence,
        } => {
            shard
                .stream_group_ack(key, group, *millis, *sequence)
                .map_err(|_| PersistenceError::Corruption("stream ack replay failed".into()))?;
        }

        OwnedMutation::StreamGroupDestroy { key, group } => {
            shard.stream_destroy_group(key, group).map_err(|_| {
                PersistenceError::Corruption("stream group destroy replay failed".into())
            })?;
        }

        OwnedMutation::StreamGroupSetId {
            key,
            group,
            millis,
            sequence,
        } => {
            shard
                .stream_set_group_id(key, group, *millis, *sequence)
                .map_err(|_| {
                    PersistenceError::Corruption("stream group setid replay failed".into())
                })?;
        }

        OwnedMutation::StreamGroupDelConsumer {
            key,
            group,
            consumer,
        } => {
            shard
                .stream_delete_consumer(key, group, consumer)
                .map_err(|_| {
                    PersistenceError::Corruption("stream group delconsumer replay failed".into())
                })?;
        }

        OwnedMutation::StreamGroupCreateConsumer {
            key,
            group,
            consumer,
        } => {
            shard
                .stream_create_consumer(key, group, consumer)
                .map_err(|_| {
                    PersistenceError::Corruption("stream group createconsumer replay failed".into())
                })?;
        }

        OwnedMutation::StreamHold {
            key,
            millis,
            sequence,
            ready_at,
        } => {
            shard
                .stream_hold_until(key, *millis, *sequence, *ready_at)
                .map_err(|_| PersistenceError::Corruption("stream hold replay failed".into()))?;
        }

        OwnedMutation::LeasePut {
            key,
            token,
            expires_at,
        } => {
            shard
                .lease_put(key, *token, *expires_at, "LEASE")
                .map_err(|_| PersistenceError::Corruption("lease replay failed".into()))?;
        }

        OwnedMutation::SetVersioned {
            key,
            value,
            version,
        } => {
            shard
                .put_versioned(key, value, *version)
                .map_err(|_| PersistenceError::Corruption("versioned replay failed".into()))?;
        }

        OwnedMutation::LimitPut {
            key,
            used,
            expires_at,
        } => {
            let created = shard
                .put_limit(key, *used)
                .map_err(|_| PersistenceError::Corruption("limit replay failed".into()))?;

            // A legacy record repeats the window end on every increment. Only the record that
            // opened the window set the deadline then, so only that one sets it here.
            if created && let Some(expires_at) = expires_at {
                shard.expire_at(key, crate::time::UnixMillis::new(*expires_at));
            }
        }

        OwnedMutation::OncePut { key, expires_at } => {
            shard
                .put_once(key, *expires_at)
                .map_err(|_| PersistenceError::Corruption("once replay failed".into()))?;
        }

        OwnedMutation::StreamGroupAdvance {
            key,
            group,
            millis,
            sequence,
        } => {
            shard
                .stream_group_advance(key, group, *millis, *sequence)
                .map_err(|_| {
                    PersistenceError::Corruption("stream group advance replay failed".into())
                })?;
        }

        OwnedMutation::ExpireAt { key, unix_millis } => {
            shard.expire_at(key, UnixMillis::new(*unix_millis));
        }

        OwnedMutation::Persist { key } => {
            shard.persist(key);
        }

        OwnedMutation::Rename {
            source,
            destination,
        } => {
            if !shard.rename(source, destination) {
                return Err(PersistenceError::Corruption(
                    "rename source missing during replay".into(),
                ));
            }
        }

        OwnedMutation::FlushDatabase => shard.flush_database(database),
        OwnedMutation::FlushAll => shard.flush_all(),
        OwnedMutation::SlotCopy { key, entry } => {
            shard.forget_key(database, key);
            if let Some(entry) = entry {
                let mut entry = snapshot::decode_entry_frame(entry)?;

                entry.database = database;
                shard
                    .restore_snapshot_entry(entry)
                    .map_err(|error| PersistenceError::Corruption(error.into()))?;
            }
        }
        OwnedMutation::SlotsMoved { .. }
        | OwnedMutation::SlotsIncoming { .. }
        | OwnedMutation::SlotsGiven { .. } => {}
        OwnedMutation::SwapDatabases { first, second } => shard.swap_databases(*first, *second),
        OwnedMutation::MoveKey { key, destination } => {
            if !shard.move_key(key, *destination) {
                return Err(PersistenceError::Corruption(
                    "move source missing or target occupied during replay".into(),
                ));
            }
        }

        OwnedMutation::Transaction { mutations } => {
            for mutation in mutations {
                apply_in_active(shard, database, mutation)?;
            }
        }
    }

    Ok(())
}

/// Replaces `path` with `bytes` so a crash leaves the old file or the new one, never a torn
/// mix: write a temporary file next to it, sync it, rename it over `path`, sync the directory.
/// The temporary name is unique per call, so concurrent writers (one per shard) do not share
/// it. `private` creates the file readable by its owner only (Unix).
pub(crate) fn write_file_atomically(
    path: &std::path::Path,
    bytes: &[u8],
    private: bool,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("path has no file name"))?
        .to_string_lossy()
        .into_owned();
    let temporary = directory.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut options = std::fs::OpenOptions::new();

    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;

    let written = (|| {
        let mut file = options.open(&temporary)?;

        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(&directory)?.sync_all()?;
        Ok(())
    })();

    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }

    written
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shard_layout_file_is_written_whole_and_a_leftover_temporary_is_harmless() {
        let directory = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = crate::protocol::ProtocolLimits::default();

        // A crash in an earlier start left a partial temporary behind.
        fs::write(directory.path().join(".SHARD_LAYOUT.tmp"), b"ruvio-sh").unwrap();
        validate_shard_layout(&config, 2, 2, limits).unwrap();

        assert_eq!(
            fs::read_to_string(directory.path().join("SHARD_LAYOUT")).unwrap(),
            "ruvio-shards-v1 2\n"
        );
        assert!(!directory.path().join(".SHARD_LAYOUT.tmp").exists());
        validate_shard_layout(&config, 2, 2, limits).unwrap();
        assert!(validate_shard_layout(&config, 3, 3, limits).is_err());
    }

    #[test]
    fn data_directory_lock_excludes_a_second_owner() {
        let directory = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };

        let first = DataDirectoryLock::acquire(&config).unwrap().unwrap();

        assert!(DataDirectoryLock::acquire(&config).is_err());
        drop(first);

        // The lock belongs to the open file, not to this descriptor. A parallel test that
        // spawns a process (the crash tests start this binary again) forks a child holding a
        // copy of every descriptor until its `exec` closes them, the lock file's included, so
        // for that moment the released lock is still held. The child lets go on its own,
        // so the test waits for that instead of expecting the lock back at once.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);

        loop {
            match DataDirectoryLock::acquire(&config) {
                Ok(lock) => {
                    assert!(lock.is_some());
                    break;
                }
                Err(error) => assert!(
                    std::time::Instant::now() < deadline,
                    "the lock was not released: {error}"
                ),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// A replica applies the primary's frames with `apply_mutation` while its own background
    /// checkpoint may be open. Each frame must save the key's old value in the checkpoint
    /// journal first, as the command path does; otherwise the checkpoint takes the newer value
    /// while its sequence still precedes the frame, and startup replays the frame again.
    #[test]
    fn a_replicated_write_during_a_checkpoint_leaves_the_snapshot_point_value() {
        let mut shard = crate::store::KvShard::new();

        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::ListPushBack {
                key: b"list".to_vec(),
                values: vec![b"before".to_vec()],
            },
        )
        .unwrap();
        shard.begin_snapshot(1 << 20).unwrap();
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::ListPushBack {
                key: b"list".to_vec(),
                values: vec![b"after".to_vec()],
            },
        )
        .unwrap();
        let snapshot = shard.finish_snapshot();

        assert_eq!(
            snapshot
                .iter()
                .find(|entry| entry.key == b"list")
                .map(|entry| &entry.value),
            Some(&crate::store::SnapshotValue::List(vec![b"before".to_vec()])),
            "the checkpoint holds a write made after its snapshot point"
        );
    }

    #[test]
    fn replay_json_put_preserves_the_document_and_existing_ttl() {
        let mut shard = crate::store::KvShard::new();
        let key = b"document".to_vec();
        let first = OwnedMutation::JsonPut {
            key: key.clone(),
            value: br#"{"version":1}"#.to_vec(),
        };
        let updated = OwnedMutation::JsonPut {
            key: key.clone(),
            value: br#"{"version":2}"#.to_vec(),
        };
        apply_mutation(&mut shard, 0, &first).unwrap();
        let deadline = UnixMillis::new(4_000_000_000_000);
        shard.expire_at(&key, deadline);
        apply_mutation(&mut shard, 0, &updated).unwrap();

        assert_eq!(
            shard.json_view(&key).unwrap(),
            Some(br#"{"version":2}"#.as_slice())
        );
        assert_eq!(shard.expires_at(&key), Some(deadline));
    }

    #[test]
    fn replay_json_delta_after_expired_checkpoint_does_not_fail_recovery() {
        let mut checkpoint = crate::store::KvShard::new();
        let json = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(crate::json::TREE_THRESHOLD), "nested": {"old": 1}
        }))
        .unwrap();
        checkpoint.put_json(b"document", json).unwrap();
        let mut entries = checkpoint.snapshot_entries();
        entries[0].expires_at = Some(UnixMillis::new(1));
        let mut recovered = crate::store::KvShard::new();
        let replay_clock = recovered.pause_expiration_for_replay();
        for entry in entries {
            recovered.restore_snapshot_entry(entry).unwrap();
        }
        recovered.resume_expiration_after_replay(replay_clock);
        apply_mutation(
            &mut recovered,
            0,
            &OwnedMutation::Transaction {
                mutations: vec![OwnedMutation::JsonSetPath {
                    key: b"document".to_vec(),
                    path: b"$.nested.old".to_vec(),
                    value: b"2".to_vec(),
                }],
            },
        )
        .unwrap();
        assert_eq!(recovered.pttl(b"document"), -2);
        assert_eq!(recovered.expire_cycle(usize::MAX), 1);
        assert_eq!(recovered.len(), 0);
    }

    /// A checkpoint holds a hash whose only field expired (by the wall clock) before the
    /// restart, and the log after it renames that key: replay sees the key as it was when
    /// the rename ran, like a key with its own TTL, and expiry takes the field afterwards.
    #[test]
    fn replay_after_a_checkpoint_with_an_expired_hash_field_keeps_the_key_until_replayed() {
        let entry = crate::store::SnapshotEntry {
            database: 0,
            key: b"source".to_vec(),
            expires_at: None,
            value: crate::store::SnapshotValue::HashTtl(vec![(
                b"field".to_vec(),
                b"value".to_vec(),
                1,
            )]),
        };
        let mut recovered = crate::store::KvShard::new();
        let replay_clock = recovered.pause_expiration_for_replay();

        recovered.restore_snapshot_entry(entry).unwrap();
        apply_mutation(
            &mut recovered,
            0,
            &OwnedMutation::Rename {
                source: b"source".to_vec(),
                destination: b"destination".to_vec(),
            },
        )
        .unwrap();
        recovered.resume_expiration_after_replay(replay_clock);
        assert_eq!(recovered.hash_get(b"destination", b"field"), Ok(None));
    }

    #[test]
    fn replay_json_path_deltas_apply_in_order_and_preserve_ttl() {
        let mut shard = crate::store::KvShard::new();
        let key = b"document".to_vec();
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::JsonPut {
                key: key.clone(),
                value: br#"{"nested":{"a":1},"keep":true}"#.to_vec(),
            },
        )
        .unwrap();
        let deadline = UnixMillis::new(4_000_000_000_000);
        shard.expire_at(&key, deadline);
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::JsonSetPath {
                key: key.clone(),
                path: b"$.nested.a".to_vec(),
                value: b"2".to_vec(),
            },
        )
        .unwrap();
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::JsonMergePath {
                key: key.clone(),
                path: b"$.nested".to_vec(),
                value: br#"{"b":3}"#.to_vec(),
            },
        )
        .unwrap();
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::JsonDelPath {
                key: key.clone(),
                path: b"$.nested.a".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(
            shard.json_view(&key).unwrap(),
            Some(br#"{"nested":{"b":3},"keep":true}"#.as_slice())
        );
        assert_eq!(shard.expires_at(&key), Some(deadline));
    }

    #[test]
    fn replay_invalid_json_path_delta_is_corruption() {
        let mut shard = crate::store::KvShard::new();
        assert!(matches!(
            apply_mutation(
                &mut shard,
                0,
                &OwnedMutation::JsonSetPath {
                    key: b"doc".to_vec(),
                    path: b"not-a-path".to_vec(),
                    value: b"1".to_vec(),
                },
            ),
            Err(PersistenceError::Corruption(_))
        ));
    }

    /// Replayed records (a replica's frames) settle what they change for a running snapshot,
    /// as commands do: a transaction's keys, a moved key in both databases, and a
    /// database-wide record ends the snapshot.
    #[test]
    fn replayed_records_keep_a_running_snapshot_at_its_point() {
        let list = |key: &[u8], values: &[&[u8]]| OwnedMutation::ListPushBack {
            key: key.to_vec(),
            values: values.iter().map(|value| value.to_vec()).collect(),
        };
        let mut shard = crate::store::KvShard::new();

        apply_mutation(&mut shard, 0, &list(b"queue", &[b"a"])).unwrap();
        apply_mutation(&mut shard, 0, &list(b"moved", &[b"m"])).unwrap();
        apply_mutation(&mut shard, 1, &list(b"other", &[b"o"])).unwrap();
        let before = shard.snapshot_entries();

        shard.begin_snapshot(1024 * 1024).unwrap();
        apply_mutation(
            &mut shard,
            0,
            &OwnedMutation::Transaction {
                mutations: vec![
                    list(b"queue", &[b"b"]),
                    OwnedMutation::MoveKey {
                        key: b"moved".to_vec(),
                        destination: 1,
                    },
                    list(b"created", &[b"c"]),
                ],
            },
        )
        .unwrap();
        apply_mutation(&mut shard, 1, &list(b"moved", &[b"n"])).unwrap();
        assert!(shard.journal_is_open());
        assert_eq!(shard.finish_snapshot(), before);

        for mutation in [
            OwnedMutation::FlushDatabase,
            OwnedMutation::FlushAll,
            OwnedMutation::SwapDatabases {
                first: 0,
                second: 1,
            },
        ] {
            shard.begin_snapshot(1024 * 1024).unwrap();
            apply_mutation(&mut shard, 1, &mutation).unwrap();
            assert!(!shard.journal_is_open(), "{mutation:?} kept the snapshot");
        }
    }

    #[test]
    fn legacy_limit_records_set_the_window_once() {
        let mut shard = crate::store::KvShard::new();
        let at = 4_000_000_000_000;
        let put = |used| OwnedMutation::LimitPut {
            key: b"login".to_vec(),
            used,
            expires_at: Some(at),
        };

        apply_mutation(&mut shard, 0, &put(1)).unwrap();
        assert_eq!(shard.expires_at(b"login"), Some(UnixMillis::new(at)));

        assert!(shard.persist(b"login"));
        apply_mutation(&mut shard, 0, &put(2)).unwrap();
        assert_eq!(shard.expires_at(b"login"), None);
        assert_eq!(
            shard.value_kind(b"login"),
            Some(crate::store::ValueKind::Limit)
        );
    }
}
