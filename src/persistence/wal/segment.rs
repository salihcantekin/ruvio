//! WAL segment files: naming, creation, discovery, rotation, and compaction with replica and CDC retention.

use super::*;

impl WalSink {
    /// Drops the segments a checkpoint through `covered_sequence` made redundant. The files
    /// are closed and removed on the writer thread, in order with the writes; a failure there
    /// only keeps them longer and is logged.
    pub fn compact(&mut self, covered_sequence: u64) -> Result<(), PersistenceError> {
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        let floor = self.backlog_floor.load(Ordering::Acquire);
        let wanted = covered_sequence;
        let mut covered_sequence = covered_sequence.min(floor);

        if !self.profile.uses_wal() || wanted == 0 {
            return Ok(());
        }

        if floor < wanted && self.replica_hold_exceeds_budget(floor, wanted)? {
            REPLICA_HOLDS_DROPPED.fetch_add(1, Ordering::Relaxed);
            covered_sequence = wanted;
        }

        if covered_sequence == 0 {
            return Ok(());
        }

        // Close the open segment when the checkpoint covers all of it, or when it holds covered
        // records and is large: the next checkpoint can then delete it whole.
        if self.segment_first_sequence <= covered_sequence
            && (self.last_sequence() <= covered_sequence
                || self.file_length
                    >= self.config.wal_segment_bytes / WAL_CHECKPOINT_ROTATE_DIVISOR)
        {
            self.rotate()?;
        }

        let retention = Retention {
            directory: shard_wal_directory(&self.config.data_dir, self.shard_id),
            history_bytes: self.config.wal_segment_bytes / WAL_CHECKPOINT_ROTATE_DIVISOR,
            cdc_retain_bytes: self.config.cdc_retain_bytes,
        };

        self.writer
            .task(Box::new(move |physical, _| {
                if let Err(error) = retention.remove_covered(physical, covered_sequence) {
                    tracing::warn!(%error, covered_sequence, "WAL compaction failed");
                }
            }))
            .map_err(PersistenceError::from)
    }

    /// True when the segments kept only because a connected replica has not received them
    /// (sequences above `floor` up to `wanted`) are larger than `replica_retain_max_bytes`.
    fn replica_hold_exceeds_budget(
        &self,
        floor: u64,
        wanted: u64,
    ) -> Result<bool, PersistenceError> {
        let budget = self.config.replica_retain_max_bytes;

        if budget == 0 {
            return Ok(false);
        }

        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let segments = discover_segments(&directory)?;
        let mut held = 0u64;

        for (index, path) in segments.iter().enumerate() {
            let next_first = match segments.get(index + 1) {
                Some(next) => segment_first_sequence(next)?,
                None => self.segment_first_sequence,
            };
            let last_sequence = next_first.saturating_sub(1);

            if last_sequence > floor && last_sequence <= wanted {
                held = held.saturating_add(fs::metadata(path)?.len());
            }
        }

        Ok(held > budget)
    }

    /// Closes the open segment and starts the next one at `next_sequence`. The writer
    /// thread syncs, shrinks and closes the old file and creates the new one in order with
    /// the writes; the shard only moves its own cursors. A rotation that fails there fails
    /// the log, and recovery opens a fresh segment.
    pub(super) fn rotate(&mut self) -> Result<(), PersistenceError> {
        let inline = self.profile == DurabilityProfile::Always && self.sync_worker.is_none();

        if inline {
            // Replies wait for the shard's own sync here: the old segment is synced now, as
            // before, and a failed sync cuts its unacknowledged records.
            self.flush()?;
        } else {
            if let Some(error) = &self.failed {
                return Err(PersistenceError::Failed(error.clone()));
            }
            self.observe_sync_error()?;
            self.publish_staged()?;
        }

        #[cfg(test)]
        let faults = (
            self.dirty_bytes != 0
                && injected_sync_fault(&self.config.data_dir, self.shard_id).is_some(),
            super::crash::take_io_fault(super::crash::CrashSite::Rotate).is_some(),
        );
        let first_sequence = self.next_sequence;
        let last_sequence = self.last_sequence();

        if let Err(error) = self.writer.task(Box::new(move |physical, shared| {
            physical.rotate(
                shared,
                first_sequence,
                last_sequence,
                #[cfg(test)]
                faults,
            );
        })) {
            return self.fail(error);
        }

        self.segment_id = first_sequence;
        self.segment_first_sequence = first_sequence;
        self.file_length = WAL_HEADER_LEN as u64;
        self.write_pos = WAL_HEADER_LEN as u64;
        self.synced_pos = WAL_HEADER_LEN as u64;
        // The writer syncs the old segment before it opens the new one.
        self.dirty_bytes = 0;
        self.barrier_mark = (0, 0);
        self.sync_in_flight = 0;

        if inline {
            self.writer.drain();
            self.observe_write_error()?;
            self.observe_sync_error()?;
        }
        self.mark_published();
        Ok(())
    }

    /// Drop the preallocated, never-written tail so closed segments have their exact size.
    pub(super) fn shrink_to_written(&mut self) {
        self.writer.drain();
        self.writer.physical().shrink_to_written();
    }
}

/// Which closed segments compaction keeps.
struct Retention {
    directory: PathBuf,
    /// Newest log kept as resume history (see `history_retained_from`).
    history_bytes: u64,
    cdc_retain_bytes: u64,
}

impl Retention {
    /// Removes the closed segments whose records are all at or before `covered_sequence`,
    /// except the history and change-stream windows. Runs on the writer thread, after every
    /// rotation handed over before it.
    fn remove_covered(
        &self,
        physical: &Physical,
        covered_sequence: u64,
    ) -> Result<(), PersistenceError> {
        if physical.file.is_none() {
            return Ok(());
        }

        let current = segment_path(&self.directory, physical.segment_first_sequence);
        let segments = discover_segments(&self.directory)?;
        let retained_from = self
            .cdc_retained_from(&segments)?
            .min(self.history_retained_from(&segments, &current, physical.write_pos)?);

        for (index, path) in segments.iter().enumerate() {
            if path == &current || index >= retained_from {
                continue;
            }

            let next_first = match segments.get(index + 1) {
                Some(next) => segment_first_sequence(next)?,
                None => physical.segment_first_sequence,
            };

            let last_sequence = next_first.saturating_sub(1);

            if last_sequence <= covered_sequence {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    /// Index of the oldest segment of the newest log kept as resume history: segments are
    /// kept from the newest back until they hold an eighth of `wal_segment_bytes`. Before a
    /// checkpoint closed large open segments, the open segment kept up to `wal_segment_bytes`
    /// of history by accident; this keeps a bounded part of it on purpose.
    fn history_retained_from(
        &self,
        segments: &[PathBuf],
        current: &Path,
        written: u64,
    ) -> Result<usize, PersistenceError> {
        let mut kept = 0u64;

        for (index, path) in segments.iter().enumerate().rev() {
            // The open segment's file is preallocated ahead of what was written.
            let length = if path == current {
                written
            } else {
                fs::metadata(path)?.len()
            };
            kept = kept.saturating_add(length);

            if kept >= self.history_bytes {
                return Ok(index);
            }
        }

        Ok(0)
    }

    /// Index of the oldest segment the change-stream window keeps; `usize::MAX` (nothing
    /// held back) when `cdc_retain_bytes` is `0`.
    fn cdc_retained_from(&self, segments: &[PathBuf]) -> Result<usize, PersistenceError> {
        let budget = self.cdc_retain_bytes;

        if budget == 0 {
            return Ok(usize::MAX);
        }

        let mut kept = 0u64;
        let mut from = segments.len();

        for (index, path) in segments.iter().enumerate().rev() {
            kept = kept.saturating_add(fs::metadata(path)?.len());

            if kept > budget {
                break;
            }

            from = index;
        }

        Ok(from)
    }
}

pub(super) fn shard_wal_directory(data_dir: &Path, shard_id: u32) -> PathBuf {
    data_dir.join("wal").join(format!("shard-{shard_id:04}"))
}

pub(super) fn segment_path(directory: &Path, first_sequence: u64) -> PathBuf {
    directory.join(format!("{first_sequence:020}.wal"))
}

pub(super) fn create_segment(
    directory: &Path,
    shard_id: u32,
    first_sequence: u64,
) -> Result<(File, u64, u64, u64), PersistenceError> {
    crate::persistence::create_private_directory(directory)?;
    let segment_id = first_sequence;
    let path = segment_path(directory, first_sequence);
    let mut file = match crate::persistence::private_file_options()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // A leftover of an earlier attempt that failed before any record was written (its
            // cleanup failed too) holds no data: reuse it. Anything longer is a real segment.
            let file = OpenOptions::new().read(true).write(true).open(&path)?;

            if file.metadata()?.len() > WAL_HEADER_LEN as u64 {
                return Err(error.into());
            }
            file.set_len(0)?;
            file
        }
        Err(error) => return Err(error.into()),
    };
    let header = encode_segment_header(shard_id, segment_id, first_sequence);
    let initialized = (|| -> Result<(), PersistenceError> {
        super::crash::hit_write(super::crash::CrashSite::Rotate, &mut file, &header)?;
        file.sync_data()?;
        // The new directory entry must be durable too, or a power cut can lose the whole segment.
        super::snapshot::sync_directory(directory)
    })();

    if let Err(error) = initialized {
        // Do not leave a partial segment behind to block the next attempt with EEXIST.
        drop(file);
        let _ = fs::remove_file(&path);
        return Err(error);
    }
    Ok((file, segment_id, first_sequence, WAL_HEADER_LEN as u64))
}

pub(super) fn discover_segments(directory: &Path) -> Result<Vec<PathBuf>, PersistenceError> {
    let mut segments = Vec::new();

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("wal") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            return Err(PersistenceError::Corruption(
                "non-UTF-8 WAL segment name".into(),
            ));
        };

        stem.parse::<u64>().map_err(|_| {
            PersistenceError::Corruption(format!("invalid WAL segment name: {stem}"))
        })?;

        segments.push(path);
    }

    segments.sort();
    Ok(segments)
}

pub(super) fn segment_first_sequence(path: &Path) -> Result<u64, PersistenceError> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| PersistenceError::Corruption("invalid WAL filename".into()))
}

pub(super) fn first_usable_segment_index(
    paths: &[PathBuf],
    after_sequence: u64,
) -> Result<usize, PersistenceError> {
    if paths.is_empty() {
        return Ok(0);
    }

    let target = after_sequence.saturating_add(1).max(1);

    for (index, path) in paths.iter().enumerate().rev() {
        if segment_first_sequence(path)? <= target {
            return Ok(index);
        }
    }

    Ok(0)
}

pub(crate) fn inspect_shard_wal(
    data_dir: &Path,
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_segment_bytes: u64,
    after_sequence: u64,
) -> Result<(RecoveredMutations, RecoveryReport), PersistenceError> {
    let directory = shard_wal_directory(data_dir, shard_id);

    if !directory.exists() {
        return Ok((
            Vec::new(),
            RecoveryReport {
                last_sequence: after_sequence,
                ..RecoveryReport::default()
            },
        ));
    }

    let segments = discover_segments(&directory)?;
    let (mutations, report, _) = recover_segments(
        &segments,
        shard_id,
        limits,
        maximum_segment_bytes,
        after_sequence,
        true,
    )?;

    Ok((mutations, report))
}

pub(crate) fn discover_wal_shards(data_dir: &Path) -> Result<Vec<u32>, PersistenceError> {
    let directory = data_dir.join("wal");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut shards = Vec::new();

    for entry in entries {
        let entry = entry?;

        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let Some(id) = name.strip_prefix("shard-") else {
            continue;
        };

        let Ok(shard_id) = id.parse::<u32>() else {
            return Err(PersistenceError::Corruption(format!(
                "invalid WAL shard directory: {name}"
            )));
        };

        shards.push(shard_id);
    }

    shards.sort_unstable();
    shards.dedup();
    Ok(shards)
}
