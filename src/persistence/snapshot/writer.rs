//! Writing one shard's snapshot file: the streaming writer and the in-memory encoder.

use super::*;

/// Writes one shard snapshot in the streamed layout (format minor 1). The file is
/// built under a temporary name and renamed into place by `finish`.
pub(crate) struct SnapshotWriter {
    directory: PathBuf,
    file_name: String,
    temporary: PathBuf,
    file: File,
    hasher: blake3::Hasher,
    written: u64,
    entries: u64,
    last_sequence: u64,
    shard_id: u32,
    maximum_bytes: u64,
}

impl SnapshotWriter {
    pub(crate) fn create(
        directory: &Path,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
        maximum_bytes: u64,
    ) -> Result<Self, PersistenceError> {
        Self::create_at(
            directory,
            generation,
            shard_id,
            shard_count,
            last_sequence,
            maximum_bytes,
            unix_millis(),
        )
    }

    pub(super) fn create_at(
        directory: &Path,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
        maximum_bytes: u64,
        created_at: u64,
    ) -> Result<Self, PersistenceError> {
        let file_name = format!("shard-{shard_id:04}.snap");
        let temporary = directory.join(format!(".{file_name}.tmp"));
        let file = crate::persistence::private_file_options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        let mut writer = Self {
            directory: directory.to_path_buf(),
            file_name,
            temporary,
            file,
            hasher: blake3::Hasher::new(),
            written: 0,
            entries: 0,
            last_sequence,
            shard_id,
            maximum_bytes,
        };
        let mut header = Vec::with_capacity(SNAPSHOT_HEADER_LEN);

        header.extend_from_slice(SNAPSHOT_MAGIC);
        put_u16(&mut header, 1);
        put_u16(&mut header, SNAPSHOT_MINOR_STREAMED);
        put_u32(&mut header, SNAPSHOT_HEADER_LEN as u32);
        put_u64(&mut header, generation);
        put_u32(&mut header, shard_id);
        put_u32(&mut header, shard_count);
        put_u64(&mut header, last_sequence);
        put_u64(&mut header, COUNT_IN_FOOTER);
        put_u64(&mut header, created_at);
        put_u32(&mut header, 0);
        let checksum = crc32c::crc32c(&header);

        put_u32(&mut header, checksum);
        debug_assert_eq!(header.len(), SNAPSHOT_HEADER_LEN);
        writer.write_raw(&header)?;
        Ok(writer)
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<(), PersistenceError> {
        let total = self.written.saturating_add(bytes.len() as u64);

        if total.saturating_add(SNAPSHOT_FOOTER_LEN as u64) > self.maximum_bytes {
            return Err(PersistenceError::Failed(
                "snapshot exceeds configured size limit".into(),
            ));
        }

        super::crash::hit_write(super::crash::CrashSite::Snapshot, &mut self.file, bytes)?;
        self.hasher.update(bytes);
        self.written = total;
        Ok(())
    }

    /// Appends `bytes`, which hold `entries` complete encoded entries.
    pub(crate) fn write_chunk(
        &mut self,
        bytes: &[u8],
        entries: u64,
    ) -> Result<(), PersistenceError> {
        self.write_raw(bytes)?;
        self.entries += entries;
        Ok(())
    }

    /// Writes the footer, syncs, and renames the file into place.
    pub(crate) fn finish(mut self) -> Result<ShardFile, PersistenceError> {
        #[cfg(test)]
        delay_shard_file(&self.directory, self.shard_id);
        let digest = *self.hasher.finalize().as_bytes();
        let mut footer = Vec::with_capacity(SNAPSHOT_FOOTER_LEN);

        footer.extend_from_slice(SNAPSHOT_FOOTER_MAGIC);
        put_u64(&mut footer, self.entries);
        put_u64(&mut footer, self.last_sequence);
        footer.extend_from_slice(&digest);
        let checksum = crc32c::crc32c(&footer);

        put_u32(&mut footer, checksum);
        self.file.write_all(&footer)?;
        self.hasher.update(&footer);
        self.written += footer.len() as u64;
        self.file.sync_all()?;
        super::crash::hit(super::crash::CrashSite::Rename);
        fs::rename(&self.temporary, self.directory.join(&self.file_name))?;
        sync_directory(&self.directory)?;
        Ok(ShardFile {
            shard_id: self.shard_id,
            last_sequence: self.last_sequence,
            byte_length: self.written,
            digest: *self.hasher.finalize().as_bytes(),
            file_name: self.file_name.clone(),
        })
    }

    /// Drops the partial file.
    pub(crate) fn abort(self) {
        let _ = fs::remove_file(&self.temporary);
    }
}

pub(super) fn encode_snapshot(
    generation: u64,
    shard_count: u32,
    checkpoint: &ShardCheckpoint,
) -> Result<Vec<u8>, PersistenceError> {
    encode_snapshot_at(generation, shard_count, checkpoint, unix_millis())
}

pub(super) fn encode_snapshot_at(
    generation: u64,
    shard_count: u32,
    checkpoint: &ShardCheckpoint,
    created_at: u64,
) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    put_u16(&mut bytes, 1);
    put_u16(&mut bytes, 0);
    put_u32(&mut bytes, SNAPSHOT_HEADER_LEN as u32);
    put_u64(&mut bytes, generation);
    put_u32(&mut bytes, checkpoint.shard_id);
    put_u32(&mut bytes, shard_count);
    put_u64(&mut bytes, checkpoint.last_sequence);
    put_u64(&mut bytes, checkpoint.entries.len() as u64);
    put_u64(&mut bytes, created_at);
    put_u32(&mut bytes, 0);
    let header_checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, header_checksum);
    debug_assert_eq!(bytes.len(), SNAPSHOT_HEADER_LEN);

    for entry in &checkpoint.entries {
        encode_entry(&mut bytes, entry)?;
    }

    let digest = *blake3::hash(&bytes).as_bytes();

    bytes.extend_from_slice(SNAPSHOT_FOOTER_MAGIC);
    put_u64(&mut bytes, checkpoint.entries.len() as u64);
    put_u64(&mut bytes, checkpoint.last_sequence);
    bytes.extend_from_slice(&digest);
    let footer_start = bytes.len() - 56;
    let footer_checksum = crc32c::crc32c(&bytes[footer_start..]);

    put_u32(&mut bytes, footer_checksum);
    Ok(bytes)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
