//! The manifest, CURRENT and clock files of a generation, and the atomic writes and cleanup around them.

use super::*;

pub(super) fn encode_manifest(
    generation: u64,
    items: &[ManifestItem],
) -> Result<Vec<u8>, PersistenceError> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(MANIFEST_MAGIC);
    put_u16(&mut bytes, 1);
    put_u16(&mut bytes, 0);
    put_u64(&mut bytes, generation);
    put_u32(
        &mut bytes,
        u32::try_from(items.len())
            .map_err(|_| PersistenceError::Failed("too many checkpoint shards".into()))?,
    );

    for item in items {
        put_u32(&mut bytes, item.shard_id);
        put_u64(&mut bytes, item.last_sequence);
        put_u64(&mut bytes, item.byte_length);
        bytes.extend_from_slice(&item.digest);
        put_u16(
            &mut bytes,
            u16::try_from(item.file_name.len())
                .map_err(|_| PersistenceError::Failed("snapshot filename too long".into()))?,
        );
        bytes.extend_from_slice(item.file_name.as_bytes());
    }

    let digest = *blake3::hash(&bytes).as_bytes();

    bytes.extend_from_slice(&digest);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    Ok(bytes)
}

pub(super) fn decode_manifest(
    bytes: &[u8],
    generation: u64,
) -> Result<Vec<ManifestItem>, PersistenceError> {
    if bytes.len() < 60 || &bytes[..8] != MANIFEST_MAGIC {
        return Err(PersistenceError::Corruption("invalid manifest".into()));
    }

    if u16_at(bytes, 8)? != 1 || u16_at(bytes, 10)? != 0 {
        return Err(PersistenceError::Unsupported(
            "manifest format version".into(),
        ));
    }

    if u64_at(bytes, 12)? != generation {
        return Err(PersistenceError::Corruption(
            "manifest generation mismatch".into(),
        ));
    }

    let digest_offset = bytes.len() - 36;

    if blake3::hash(&bytes[..digest_offset]).as_bytes() != &bytes[digest_offset..digest_offset + 32]
        || crc32c::crc32c(&bytes[..bytes.len() - 4]) != u32_at(bytes, bytes.len() - 4)?
    {
        return Err(PersistenceError::Corruption(
            "manifest digest mismatch".into(),
        ));
    }

    let count = u32_at(bytes, 20)? as usize;
    let mut decoder = Decoder::new(&bytes[24..digest_offset]);
    let mut items = Vec::with_capacity(count);

    for expected_shard in 0..count {
        let shard_id = decoder.u32()?;

        if shard_id as usize != expected_shard {
            return Err(PersistenceError::Corruption(
                "manifest shard set is not ordered".into(),
            ));
        }

        let last_sequence = decoder.u64()?;
        let byte_length = decoder.u64()?;
        let digest = decoder.take(32)?.try_into().unwrap();
        let name_length = decoder.u16()? as usize;
        let file_name = std::str::from_utf8(decoder.take(name_length)?)
            .map_err(|_| PersistenceError::Corruption("invalid snapshot filename".into()))?
            .to_owned();

        if file_name != format!("shard-{shard_id:04}.snap") {
            return Err(PersistenceError::Corruption(
                "unsafe or unexpected snapshot filename".into(),
            ));
        }

        items.push(ManifestItem {
            shard_id,
            last_sequence,
            byte_length,
            digest,
            file_name,
        });
    }

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing manifest bytes".into(),
        ));
    }

    Ok(items)
}

pub(super) fn encode_current(generation: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);

    bytes.extend_from_slice(CURRENT_MAGIC);
    put_u64(&mut bytes, generation);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    bytes
}

pub(super) fn decode_current(bytes: &[u8]) -> Result<u64, PersistenceError> {
    if bytes.len() != 20
        || &bytes[..8] != CURRENT_MAGIC
        || crc32c::crc32c(&bytes[..16]) != u32_at(bytes, 16)?
    {
        return Err(PersistenceError::Corruption(
            "invalid CURRENT pointer".into(),
        ));
    }

    u64_at(bytes, 8)
}

pub(super) fn encode_clock(unix_millis: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);

    bytes.extend_from_slice(CLOCK_MAGIC);
    put_u64(&mut bytes, unix_millis);
    let checksum = crc32c::crc32c(&bytes);

    put_u32(&mut bytes, checksum);
    bytes
}

pub(super) fn decode_clock(bytes: &[u8]) -> Result<u64, PersistenceError> {
    if bytes.len() != 20
        || &bytes[..8] != CLOCK_MAGIC
        || crc32c::crc32c(&bytes[..16]) != u32_at(bytes, 16)?
    {
        return Err(PersistenceError::Corruption("invalid CLOCK file".into()));
    }

    u64_at(bytes, 8)
}

pub(super) fn retain_previous_generations(directory: &Path, generation: u64) {
    let keep_from = generation.saturating_sub(1);
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        if let Ok(existing) = name.parse::<u64>()
            && existing < keep_from
        {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

/// Removes `.<name>.tmp` files an interrupted write left in the data directory and in every
/// checkpoint generation. Call only while holding the data directory lock.
pub(crate) fn sweep_temporary_files(data_dir: &Path) {
    let mut directories = vec![data_dir.to_path_buf()];

    if let Ok(entries) = fs::read_dir(data_dir.join("checkpoints")) {
        directories.extend(
            entries
                .flatten()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .map(|entry| entry.path()),
        );
    }

    for directory in directories {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let is_temporary = name
                .to_str()
                .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"));

            if is_temporary && entry.file_type().is_ok_and(|kind| kind.is_file()) {
                match fs::remove_file(entry.path()) {
                    Ok(()) => {
                        tracing::info!(path = %entry.path().display(), "removed leftover temporary file")
                    }
                    Err(error) => {
                        tracing::warn!(path = %entry.path().display(), %error, "cannot remove leftover temporary file")
                    }
                }
            }
        }
    }
}

/// Writes a temp file, syncs it, then renames it over the final name. A failed or interrupted
/// write leaves the previous file of that name intact, so the old checkpoint stays valid.
pub(super) fn atomic_write_file(
    directory: &Path,
    name: &str,
    bytes: &[u8],
) -> Result<(), PersistenceError> {
    write_through_temporary(
        &directory.join(format!(".{name}.tmp")),
        &directory.join(name),
        bytes,
    )?;
    sync_directory(directory)?;
    Ok(())
}

/// The temporary name of a carried copy (see `CheckpointManager::carry_shard_file`): not
/// the one a checkpoint of the same shard writes, so neither can replace or remove the
/// other's partial file. Startup cleanup removes it like any `.<name>.tmp`.
pub(super) fn carry_temporary_name(file_name: &str) -> String {
    format!(".{file_name}.carry.tmp")
}

/// [`atomic_write_file`] without the directory sync.
pub(super) fn write_through_temporary(
    temporary_path: &Path,
    final_path: &Path,
    bytes: &[u8],
) -> Result<(), PersistenceError> {
    // A leftover temp file from an interrupted write is overwritten, never fatal.
    let mut file = crate::persistence::private_file_options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(temporary_path)?;

    super::crash::hit_write(super::crash::CrashSite::Snapshot, &mut file, bytes)?;
    file.sync_all()?;
    drop(file);
    super::crash::hit(super::crash::CrashSite::Rename);
    fs::rename(temporary_path, final_path)?;
    Ok(())
}
