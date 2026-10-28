//! Appending records to the open segment, and the failed state with in-place recovery after a write error.

use super::*;

impl WalSink {
    pub(super) fn append(&mut self, mutation: Mutation<'_>) -> Result<(), PersistenceError> {
        self.append_as::<false>(mutation)
    }

    /// `THROUGH`: inside an `always` group the record is written now (after what is staged)
    /// instead of staged, so a failed write reaches the caller while it can still roll back.
    /// Its sync still waits for the group's commit. Other profiles stage it as before.
    pub(super) fn append_as<const THROUGH: bool>(
        &mut self,
        mutation: Mutation<'_>,
    ) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Ok(());
        }

        self.recover_if_failed()?;
        self.observe_sync_error()?;

        let through = THROUGH && self.group > 0 && self.profile == DurabilityProfile::Always;
        let record_length;
        let written;

        if self.group > 0 && !through {
            // Staged: encode straight onto the end of the batch, not into `record` and then
            // copied over.
            let start = self.staged.len();
            let encoded = encode_borrowed_record_appended(
                &mut self.staged,
                self.shard_id,
                self.next_sequence,
                self.database,
                &mutation,
            );
            let length = (self.staged.len() - start) as u64;

            if let Err(error) = encoded.and_then(|()| self.check_record_length(length)) {
                self.staged.truncate(start);
                return Err(error);
            }
            record_length = length;

            let rotate = self.needs_rotation(length);

            if !rotate && (start == 0 || self.staged.len() <= WAL_GROUP_BYTES) {
                if start == 0 {
                    self.staged_first_sequence = self.next_sequence;
                }
                written = Ok(());
            } else {
                // Rare: the record must not join this batch (a new segment, or the batch is
                // full). Move it out and take the general path.
                self.record.clear();
                self.record.extend_from_slice(&self.staged[start..]);
                self.staged.truncate(start);

                if rotate {
                    self.rotate()?;
                }
                let record = std::mem::take(&mut self.record);

                written = self.write_record(&record);
                self.record = record;
            }
        } else {
            encode_borrowed_record_into(
                &mut self.record,
                self.shard_id,
                self.next_sequence,
                self.database,
                &mutation,
            )?;
            record_length = self.record.len() as u64;
            self.check_record_length(record_length)?;

            if self.needs_rotation(record_length) {
                self.rotate()?;
            }

            let record = std::mem::take(&mut self.record);

            written = if through {
                self.publish_staged_io()
                    .and_then(|()| self.write_file(&record))
            } else {
                self.write_record(&record)
            };
            self.record = record;
        }

        self.record.clear();

        if let Err(error) = written {
            return self.fail(error);
        }

        self.file_length += record_length;
        self.dirty_bytes += record_length;
        self.group_bytes += record_length;

        let synced = match self.sync_open_segment() {
            Ok(synced) => synced,
            Err(error) => {
                self.forget_refused(record_length);
                return Err(error);
            }
        };

        self.note_appended(synced)?;

        if self.group == 0 && self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    pub fn append_frame(&mut self, frame: &[u8]) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Err(PersistenceError::Failed(
                "WAL frame append requires a WAL-backed profile".into(),
            ));
        }

        self.recover_if_failed()?;
        self.observe_sync_error()?;

        let record = decode_record(frame, self.shard_id, self.limits)?;

        if record.sequence != self.next_sequence {
            return Err(PersistenceError::Failed(
                "WAL frame sequence is not the next sequence".into(),
            ));
        }

        let record_length = frame.len() as u64;

        if WAL_HEADER_LEN as u64 + record_length > self.config.wal_segment_bytes {
            return Err(PersistenceError::Failed(
                "encoded WAL record exceeds configured segment size".into(),
            ));
        }

        if self.file_length > WAL_HEADER_LEN as u64
            && self.file_length.saturating_add(record_length) > self.config.wal_segment_bytes
        {
            self.rotate()?;
        }

        if let Err(error) = self.write_record(frame) {
            return self.fail(error);
        }

        self.file_length += record_length;
        self.dirty_bytes += record_length;
        self.group_bytes += record_length;

        let synced = match self.sync_open_segment() {
            Ok(synced) => synced,
            Err(error) => {
                self.forget_refused(record_length);
                return Err(error);
            }
        };

        self.note_appended(synced)?;

        if self.group == 0 && self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    fn check_record_length(&self, record_length: u64) -> Result<(), PersistenceError> {
        if record_length > self.limits.max_command_bytes as u64 {
            return Err(PersistenceError::Failed(
                "encoded WAL record exceeds configured command limit".into(),
            ));
        }

        if WAL_HEADER_LEN as u64 + record_length > self.config.wal_segment_bytes {
            // The runtime matches this prefix to answer the client with a clear error.
            return Err(PersistenceError::Failed(format!(
                "encoded WAL record exceeds configured segment size ({record_length} bytes, \
                 wal_segment_bytes = {})",
                self.config.wal_segment_bytes
            )));
        }

        Ok(())
    }

    fn needs_rotation(&self, record_length: u64) -> bool {
        self.file_length > WAL_HEADER_LEN as u64
            && self.file_length.saturating_add(record_length) > self.config.wal_segment_bytes
    }

    fn write_record(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.group > 0 {
            if !self.staged.is_empty() && self.staged.len() + bytes.len() > WAL_GROUP_BYTES {
                self.publish_staged_io()?;
            }

            if self.staged.is_empty() {
                // Every caller writes the record that will get `next_sequence`.
                self.staged_first_sequence = self.next_sequence;
            }
            self.staged.extend_from_slice(bytes);

            return Ok(());
        }

        self.write_file(bytes)
    }

    fn write_file(&mut self, bytes: &[u8]) -> io::Result<()> {
        let Some(file) = &mut self.file else {
            return Err(io::Error::other("WAL-backed profile has no open segment"));
        };
        let end = self.write_pos + bytes.len() as u64;

        // Preallocated *and zero-filled* space: a sync of blocks that already hold data needs no extent
        // conversion or size change, which made `fdatasync` ~2.4x faster (2.0 -> 0.8 ms per sync) on ext4.
        if self.prealloc {
            let cap = self.config.wal_segment_bytes.max(end);

            self.prefill.reserve(
                file,
                &mut self.prealloc_end,
                &mut self.prealloc,
                end,
                cap,
                self.write_pos,
            );
        }

        #[cfg(test)]
        if let Some(error) = injected_write_fault(&self.config.data_dir, self.shard_id) {
            self.prealloc_end = self.write_pos;
            return Err(error);
        }
        #[cfg(test)]
        count_write(&self.config.data_dir);
        let written = super::crash::hit_write(super::crash::CrashSite::Append, file, bytes);

        if written.is_ok() {
            self.write_pos = end;
        } else {
            // Keep whatever a failed write left behind (recovery classifies it); only a clean tail is trimmed.
            self.prealloc_end = self.write_pos;
        }

        written
    }

    pub(super) fn publish_staged(&mut self) -> Result<(), PersistenceError> {
        if let Err(error) = self.publish_staged_io() {
            return self.fail(error);
        }

        Ok(())
    }

    fn publish_staged_io(&mut self) -> io::Result<()> {
        if self.staged.is_empty() {
            return Ok(());
        }

        // After a failed write the file cursor may sit past torn bytes; only recovery writes again.
        if let Some(message) = &self.failed {
            return Err(io::Error::other(message.clone()));
        }

        let staged = std::mem::take(&mut self.staged);
        let written = self.write_file(&staged);

        self.staged = staged;
        // The staged records already own their sequences (and are applied in memory): a failed
        // write keeps them so recovery writes them before anything newer.
        if written.is_ok() {
            self.staged.clear();
        }
        written
    }

    /// A single `always` record's sync failed. Every unsynced record before it belongs to a
    /// batch that has not synced yet and now gets an error too, so all are cut. With the
    /// worker the shard thread does not know what it synced: only this record goes.
    fn forget_refused(&mut self, record_length: u64) {
        let position = if self.sync_worker.is_none() {
            self.synced_pos
        } else {
            self.write_pos.saturating_sub(record_length)
        };

        self.forget_unsynced_since(position.min(self.write_pos));
    }

    /// The records from `position` on reached the file but their `always` sync failed, so
    /// their clients get an error: move the end back to `position` and cut them, so a
    /// restart does not revive writes that were refused. Never called with a `position`
    /// before the end of a successful sync.
    pub(super) fn forget_unsynced_since(&mut self, position: u64) {
        let length = self.write_pos.saturating_sub(position);

        self.write_pos = position;
        self.file_length = self.file_length.saturating_sub(length);
        self.dirty_bytes = self.dirty_bytes.saturating_sub(length);
        self.group_bytes = self.group_bytes.saturating_sub(length);
        // If the file cannot be shortened, overwrite the records with zeros instead: replay
        // then sees an unwritten preallocated tail.
        self.prefill.finish(&mut self.prealloc_end);
        if let Some(file) = &self.file {
            if file.set_len(self.write_pos).is_ok() {
                self.prealloc_end = self.write_pos;
            } else {
                let zeros = vec![0u8; usize::try_from(length).unwrap_or(0)];
                let _ = write_all_at(file, &zeros, self.write_pos);
            }
        }
    }

    /// After a write or sync error the log refuses appends. Once the disk accepts a clean
    /// start again (space freed, transient error gone), drop the torn tail and continue in a
    /// fresh segment, so writes resume without a restart. Tried at most twice a second.
    pub(super) fn recover_if_failed(&mut self) -> Result<(), PersistenceError> {
        let Some(message) = self.failed.clone() else {
            return Ok(());
        };

        // A sync still running, or one that failed after the write error, may have taken
        // the error for pages recovery's own sync would then report as clean: settle them
        // first and treat any error they left as a failed sync.
        self.wait_background_sync();
        self.prefill.finish(&mut self.prealloc_end);
        if !self.sync_failed
            && let Err(error) = self.observe_sync_error()
        {
            return Err(error);
        }

        // Recovery would sync and continue after bytes a failed sync may have lost.
        if self.sync_failed {
            return Err(PersistenceError::Failed(message));
        }

        if self
            .last_recovery
            .is_some_and(|at| at.elapsed() < std::time::Duration::from_millis(500))
        {
            return Err(PersistenceError::Failed(message));
        }

        self.last_recovery = Some(Instant::now());

        let mut sync_lost = None;
        let recovered = (|| -> Result<(), PersistenceError> {
            self.sync_worker_file = None;

            if let Some(file) = &mut self.file {
                // Drop torn bytes and put the cursor back: a failed write may have moved it.
                // Only bytes past `write_pos` go; they never reached a successful write.
                file.set_len(self.write_pos)?;
                file.seek(SeekFrom::Start(self.write_pos))?;
                #[cfg(test)]
                let synced = match super::crash::take_io_fault(super::crash::CrashSite::Sync) {
                    Some(error) => Err(error),
                    None => file.sync_data(),
                };
                #[cfg(not(test))]
                let synced = file.sync_data();
                if let Err(error) = synced {
                    sync_lost = Some(error.to_string());
                    return Err(error.into());
                }
            }

            self.prealloc_end = self.write_pos;
            self.prefill.reset();

            // Records staged before the failure keep their sequences and are written first.
            let resume_at = if self.staged.is_empty() {
                self.next_sequence
            } else {
                self.staged_first_sequence
            };

            if self.write_pos > WAL_HEADER_LEN as u64 {
                let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
                let (file, segment_id, first_sequence, file_length) =
                    create_segment(&directory, self.shard_id, resume_at)?;

                self.file = Some(file);
                self.segment_id = segment_id;
                self.segment_first_sequence = first_sequence;
                self.write_pos = file_length;
                self.synced_pos = file_length;
                self.prealloc_end = file_length;
            } else if self.segment_first_sequence != resume_at {
                return Err(PersistenceError::Failed(
                    "WAL recovery found an empty segment at the wrong sequence".into(),
                ));
            }

            // Everything written before the failure was synced above.
            self.file_length = self.write_pos;
            self.dirty_bytes = 0;

            if !self.staged.is_empty() {
                let staged = std::mem::take(&mut self.staged);
                let written = self.write_file(&staged);

                self.staged = staged;
                written?;
                self.file_length += self.staged.len() as u64;
                self.dirty_bytes = self.staged.len() as u64;
                self.staged.clear();
            }

            Ok(())
        })();

        match recovered {
            Ok(()) => {
                self.observe_sync_error()?;
                self.failed = None;
                self.synced_pos = self.file_length - self.dirty_bytes;
                self.sync_in_flight = 0;
                self.group_bytes = 0;
                // Rewritten `always` records reach replicas once a sync covers them.
                if self.dirty_bytes == 0
                    || self.profile != DurabilityProfile::Always
                    || self.sync_worker.is_some()
                {
                    self.mark_published();
                }
                tracing::warn!(previous = %message, "WAL writes resumed after an earlier failure");

                Ok(())
            }

            Err(_) => match sync_lost {
                Some(error) => self.fail_sync(error),
                None => Err(PersistenceError::Failed(message)),
            },
        }
    }

    /// Enter the sticky failed state after an `fsync` error; see `sync_failed`.
    pub(super) fn fail_sync<T>(&mut self, error: String) -> Result<T, PersistenceError> {
        if !self.sync_failed {
            tracing::error!(
                shard = self.shard_id,
                %error,
                "WAL fsync failed; writes are refused until restart"
            );
        }
        self.sync_failed = true;
        self.failed = Some(error.clone());
        // Also when another descriptor's sync (the preallocation fill) failed and this is
        // only where the error is picked up: the unsynced records' clients are refused.
        self.cut_unsynced_always_records();
        Err(PersistenceError::Failed(error))
    }

    pub(super) fn fail<T>(&mut self, error: io::Error) -> Result<T, PersistenceError> {
        let message = error.to_string();

        self.failed = Some(message.clone());
        Err(PersistenceError::Failed(message))
    }
}
