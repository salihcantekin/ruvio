//! Appending records to the open segment, and the failed state with in-place recovery after a write error.

use super::*;

impl WalSink {
    pub(super) fn append(&mut self, mutation: Mutation<'_>) -> Result<(), PersistenceError> {
        self.append_as::<false>(mutation)
    }

    /// `THROUGH`: inside an `always` group the record is handed to the writer now (after what
    /// is staged) instead of staged. Its sync still waits for the group's commit, which fails
    /// if the writer could not write it. Other profiles stage it as before.
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
                    .and_then(|()| self.write_file_now(&record, self.next_sequence))
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

        // Every caller writes the record that will get `next_sequence`.
        self.write_file(bytes, self.next_sequence)
    }

    /// Hands `bytes`, the records up to `last_sequence`, to the writer thread. A write that
    /// fails there keeps them for recovery, which writes them before anything newer, as for
    /// staged records.
    fn write_file(&mut self, bytes: &[u8], last_sequence: u64) -> io::Result<()> {
        self.writer.write(bytes, last_sequence)?;
        self.write_pos += bytes.len() as u64;

        Ok(())
    }

    /// `write_file`, then waits for the writer: a script or transaction inside an `always`
    /// group learns of a failed write while it can still roll back, and its record is not
    /// kept for recovery. Only these wait for the disk on the shard's thread; their replies
    /// wait for the group's sync anyway.
    fn write_file_now(&mut self, bytes: &[u8], last_sequence: u64) -> io::Result<()> {
        self.write_file(bytes, last_sequence)?;
        self.writer.drain();

        let Some(message) = self.writer.failure_message() else {
            return Ok(());
        };

        // The writer stopped before this record or on it: either way it holds it last.
        self.writer.physical().forget_last(bytes.len() as u64);
        self.write_pos -= bytes.len() as u64;
        Err(io::Error::other(message))
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
        // Staged records end at the last sequence handed out.
        let written = self.write_file(&staged, self.last_sequence());

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
        self.writer.drain();
        self.writer.physical().forget_last(length);
    }

    /// After a write or sync error the log refuses appends. Once the disk accepts a clean
    /// start again (space freed, transient error gone), drop the torn tail and continue in a
    /// fresh segment, so writes resume without a restart. Tried at most twice a second.
    pub(super) fn recover_if_failed(&mut self) -> Result<(), PersistenceError> {
        // A write the writer thread could not make fails the log here first.
        if self.failed.is_none() {
            self.failed = self.writer.failure_message();
        }
        let Some(message) = self.failed.clone() else {
            return Ok(());
        };

        // A sync still running, or one that failed after the write error, may have taken
        // the error for pages recovery's own sync would then report as clean: settle them
        // first and treat any error they left as a failed sync. The writer sets aside
        // whatever it still holds.
        self.wait_background_sync();
        self.writer.drain();
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

        let source = self.writer.source();
        let mut guard = source.lock_physical();
        let physical = &mut *guard;
        // Records the writer could not write, then records still staged, keep their
        // sequences and are written before anything newer.
        let mut pending = std::mem::take(&mut physical.unwritten);
        let resume_at = if !pending.is_empty() {
            physical.unwritten_first
        } else if !self.staged.is_empty() {
            self.staged_first_sequence
        } else {
            self.next_sequence
        };

        pending.extend_from_slice(&self.staged);
        self.staged.clear();

        let mut sync_lost = None;
        let recovered = (|| -> Result<(), PersistenceError> {
            physical.prefill.finish(&mut physical.prealloc_end);
            if let Some(file) = &physical.file {
                // Drop torn bytes: only bytes past `write_pos` go; they never reached a
                // successful write.
                file.set_len(physical.write_pos)?;
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

            physical.prealloc_end = physical.write_pos;
            physical.prefill.reset();

            if physical.write_pos > WAL_HEADER_LEN as u64 || physical.file.is_none() {
                let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
                let (file, _, first_sequence, file_length) =
                    create_segment(&directory, self.shard_id, resume_at)?;

                physical.file = Some(Arc::new(file));
                physical.segment_first_sequence = first_sequence;
                physical.write_pos = file_length;
                physical.prealloc_end = file_length;
            } else if physical.segment_first_sequence != resume_at {
                return Err(PersistenceError::Failed(
                    "WAL recovery found an empty segment at the wrong sequence".into(),
                ));
            }

            // Everything written before the failure was synced above.
            self.segment_id = physical.segment_first_sequence;
            self.segment_first_sequence = physical.segment_first_sequence;
            self.write_pos = physical.write_pos;
            self.file_length = physical.write_pos;
            self.dirty_bytes = 0;
            self.barrier_mark = (0, 0);

            if !pending.is_empty() {
                physical.write_at_end(&pending)?;
                self.write_pos = physical.write_pos;
                self.file_length = physical.write_pos;
                self.dirty_bytes = pending.len() as u64;
            }
            physical.written = self.last_sequence();
            physical.failed = None;

            Ok(())
        })();

        if recovered.is_err() {
            // Kept for the next attempt.
            physical.unwritten_first = resume_at;
            physical.unwritten = pending;
            physical.failed.get_or_insert_with(|| message.clone());
        }
        drop(guard);

        match recovered {
            Ok(()) => {
                source.note_rewritten(self.last_sequence());
                if let Some(worker) = &self.sync_worker {
                    worker.clear_not_written();
                }
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
        self.sync_error.poison();
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
