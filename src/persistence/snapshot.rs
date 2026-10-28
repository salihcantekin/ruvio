use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    protocol::ProtocolLimits,
    store::{SnapshotEntry, SnapshotValue},
    time::UnixMillis,
};

use super::wal::{FrameRead, read_frames_chunk};

use super::{DurabilityProfile, PersistenceConfig, PersistenceError, crash, decode_record};

mod entry;
mod manager;
mod manifest;
mod reader;
mod writer;

use entry::*;
use manifest::*;
use reader::*;
use writer::*;

pub(crate) use entry::{decode_entry_frame, encode_entry, encode_entry_with};
pub(crate) use manifest::sweep_temporary_files;
pub(crate) use reader::{ResyncBytes, decode_snapshot_blob, load_resync_bytes};
pub(crate) use writer::SnapshotWriter;

/// Data directories whose carried checkpoint copies take this long (a large shard file).
#[cfg(test)]
pub(crate) static CARRY_DELAYS: std::sync::Mutex<Vec<(PathBuf, std::time::Duration)>> =
    std::sync::Mutex::new(Vec::new());

/// Data directories whose next round commits fail before `CURRENT` changes, as many as the
/// count says (the round is not published).
#[cfg(test)]
pub(crate) static FAILING_COMMITS: std::sync::Mutex<Vec<(PathBuf, u32)>> =
    std::sync::Mutex::new(Vec::new());

/// Shards of a data directory whose new (not carried) checkpoint files take this much
/// longer to finish, to order the submissions of a round.
#[cfg(test)]
pub(crate) static SHARD_FILE_DELAYS: std::sync::Mutex<Vec<(PathBuf, u32, std::time::Duration)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn delay_shard_file(path: &Path, shard_id: u32) {
    let delay = SHARD_FILE_DELAYS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(dir, shard, _)| path.starts_with(dir) && *shard == shard_id)
        .map(|(_, _, delay)| *delay);

    if let Some(delay) = delay {
        std::thread::sleep(delay);
    }
}

const SNAPSHOT_MAGIC: &[u8; 8] = b"RUVSNP01";
const SNAPSHOT_FOOTER_MAGIC: &[u8; 8] = b"RUVEND01";
const SNAPSHOT_HEADER_LEN: usize = 64;
/// Minor version 1 streams entries before their count is known: the header holds
/// `COUNT_IN_FOOTER` and the footer holds the real count.
const SNAPSHOT_MINOR_STREAMED: u16 = 1;
const COUNT_IN_FOOTER: u64 = u64::MAX;
const SNAPSHOT_FOOTER_LEN: usize = 60;
const MANIFEST_MAGIC: &[u8; 8] = b"RUVMAN01";
const CURRENT_MAGIC: &[u8; 8] = b"RUVCUR01";
const CLOCK_MAGIC: &[u8; 8] = b"RUVCLK01";

pub(crate) struct ShardCheckpoint {
    pub shard_id: u32,
    pub last_sequence: u64,
    pub entries: Vec<SnapshotEntry>,
}

pub(crate) struct Checkpoint {
    pub generation: u64,
    pub created_at: UnixMillis,
    pub shards: Vec<ShardCheckpoint>,
}

/// A published checkpoint whose entries have not been read yet.
pub(crate) struct CheckpointPlan {
    pub generation: u64,
    pub created_at: UnixMillis,
    directory: PathBuf,
    items: Vec<ManifestItem>,
    limits: ProtocolLimits,
    maximum_bytes: u64,
}

impl CheckpointPlan {
    pub(crate) fn shard_count(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn last_sequence(&self, shard_id: u32) -> Option<u64> {
        self.items
            .iter()
            .find(|item| item.shard_id == shard_id)
            .map(|item| item.last_sequence)
    }

    /// Verifies one shard's file and passes its entries to `sink` one at a time.
    pub(crate) fn stream_shard(
        &self,
        shard_id: u32,
        sink: impl FnMut(SnapshotEntry) -> Result<(), PersistenceError>,
    ) -> Result<(), PersistenceError> {
        let item = self
            .items
            .iter()
            .find(|item| item.shard_id == shard_id)
            .ok_or_else(|| PersistenceError::Corruption("snapshot shard missing".into()))?;
        let path = self.directory.join(&item.file_name);
        // Streamed: recovery memory follows the store being filled, not the file size too.
        let (last_sequence, _) = stream_snapshot_file(
            &path,
            item.byte_length,
            &item.digest,
            self.generation,
            item.shard_id,
            self.limits,
            self.maximum_bytes,
            sink,
        )?;

        if last_sequence != item.last_sequence {
            return Err(PersistenceError::Corruption(
                "manifest snapshot sequence mismatch".into(),
            ));
        }

        Ok(())
    }
}

pub(crate) struct CheckpointManager {
    config: PersistenceConfig,
    limits: ProtocolLimits,
    /// Highest generation any round used, published or not; `None` until first read from
    /// disk. A generation is never handed out twice.
    last_generation: std::sync::Mutex<Option<u64>>,
}

/// One finished shard file of a checkpoint round.
#[derive(Clone, Debug)]
pub(crate) struct ShardFile {
    pub shard_id: u32,
    pub last_sequence: u64,
    pub byte_length: u64,
    pub digest: [u8; 32],
    pub file_name: String,
}

struct ManifestItem {
    shard_id: u32,
    last_sequence: u64,
    byte_length: u64,
    digest: [u8; 32],
    file_name: String,
}

pub(super) fn sync_directory(directory: &Path) -> Result<(), PersistenceError> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn put_byte_list(output: &mut Vec<u8>, values: &[Vec<u8>]) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(values.len())
            .map_err(|_| PersistenceError::Failed("snapshot collection too large".into()))?,
    );

    for value in values {
        put_bytes(output, value)?;
    }

    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), PersistenceError> {
    put_u32(
        output,
        u32::try_from(value.len())
            .map_err(|_| PersistenceError::Failed("snapshot byte string too large".into()))?,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn u16_at(bytes: &[u8], offset: usize) -> Result<u16, PersistenceError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u16::from_le_bytes(value.try_into().unwrap()))
}

fn u32_at(bytes: &[u8], offset: usize) -> Result<u32, PersistenceError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn u64_at(bytes: &[u8], offset: usize) -> Result<u64, PersistenceError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| PersistenceError::Corruption("truncated integer".into()))?;

    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PersistenceError> {
        if length > self.remaining.len() {
            return Err(PersistenceError::Corruption(
                "truncated persistence payload".into(),
            ));
        }

        let (value, remaining) = self.remaining.split_at(length);

        self.remaining = remaining;
        Ok(value)
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take_rest(&mut self) -> Vec<u8> {
        let bytes = self.remaining.to_vec();

        self.remaining = &[];
        bytes
    }

    fn u8(&mut self) -> Result<u8, PersistenceError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, PersistenceError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, PersistenceError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, PersistenceError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>, PersistenceError> {
        let length = self.u32()? as usize;

        if length > maximum {
            return Err(PersistenceError::Corruption(
                "persistence byte string exceeds configured limit".into(),
            ));
        }

        Ok(self.take(length)?.to_vec())
    }

    fn byte_list(&mut self, maximum_bytes: usize) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let count = self.u32()? as usize;

        if count > maximum_bytes / 4 {
            return Err(PersistenceError::Corruption(
                "snapshot collection count is excessive".into(),
            ));
        }

        let mut values = Vec::with_capacity(count.min(1024));

        for _ in 0..count {
            values.push(self.bytes(maximum_bytes)?);
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests;
