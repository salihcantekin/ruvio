//! Flushing, record groups, publishing, and the inline and background fsync paths of the WAL sink.

use super::*;

impl WalSink {
    pub fn flush(&mut self) -> Result<(), PersistenceError> {
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        // Wait before looking for errors: a background sync may fail after the check.
        self.wait_background_sync();
        self.observe_sync_error()?;
        self.settle_background_sync();
        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
            if self.durable.load(Ordering::Acquire) >= self.last_sequence() {
                self.dirty_bytes = 0;
            }
        }
        self.publish_staged()?;

        if self.dirty_bytes != 0 {
            self.fsync_always_or_cut()?;
            self.durable.store(self.last_sequence(), Ordering::Release);
        }
        self.mark_published();

        Ok(())
    }

    /// Shutdown: try to leave the failed state at once (no retry spacing), then flush.
    pub fn flush_for_shutdown(&mut self) -> Result<(), PersistenceError> {
        if self.failed.is_some() && self.profile.uses_wal() {
            self.last_recovery = None;
            self.recover_if_failed()?;
        }

        self.flush()
    }

    pub fn sync_if_due(&mut self) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Everysec {
            return Ok(());
        }

        self.poke_background_sync()
    }

    /// Stage later records so one client batch shares a write and, for `always`, one sync.
    pub fn begin_group(&mut self) {
        if self.profile.uses_wal() {
            if self.group == 0 {
                self.group_bytes = 0;
            }
            self.group = self.group.saturating_add(1);
        }
    }

    /// Ends the group a hot-key write opened (`begin_group`) once the shared hot store holds
    /// it: the record staged under the store's lock is written now, outside that lock, and
    /// under `always` synced, unless an enclosing group (`start` as in `sync_batch`) does
    /// it later. As for any staged record, a failed write keeps it for recovery.
    pub fn end_hot_write(&mut self, start: u64) -> Result<(), PersistenceError> {
        let outermost = self.group == 1;

        self.end_group()?;
        if outermost && self.profile == DurabilityProfile::Always {
            return self.sync_batch(start);
        }

        Ok(())
    }

    /// Bytes appended since the outermost open group began; zero outside a group.
    pub fn group_bytes(&self) -> u64 {
        if self.group == 0 { 0 } else { self.group_bytes }
    }

    /// Write staged records. `always` sync happens in `sync_pending` so several clients can share it.
    pub fn end_group(&mut self) -> Result<(), PersistenceError> {
        if self.group == 0 {
            return Ok(());
        }

        self.group -= 1;

        if self.group > 0 {
            return Ok(());
        }

        // A batch that appended nothing while the log is failed (reads, refused writes) has
        // nothing to publish; records kept from the failed batch wait for recovery.
        if self.failed.is_some() && self.group_bytes == 0 {
            return Ok(());
        }

        self.observe_sync_error()?;
        self.publish_staged()?;
        // An `always` batch reaches replicas from `sync_pending`, once it is on disk: a
        // batch whose sync fails is cut, and a restarted primary reuses its sequences.
        if self.profile != DurabilityProfile::Always || self.sync_worker.is_some() {
            self.mark_published();
        }

        if self.profile == DurabilityProfile::Everysec {
            self.poke_background_sync()?;
        }

        Ok(())
    }

    /// Writes the records staged behind another task's open group before a reply that may
    /// acknowledge one of them leaves. The group counter is shared by the shard's tasks, so a
    /// write made outside a group, or in a group nested in another task's, is only staged;
    /// an `everysec` reply promises its record reached the kernel. `always` replies wait for
    /// `sync_pending` or a worker barrier, which publish it themselves. Records staged
    /// before a failure wait for recovery, as in `end_group`: a write staged behind another
    /// task's group when that group's write failed is answered while it is still in memory,
    /// and a process kill before recovery loses it. Refusing the reply instead would cut off
    /// every client of the shard, readers too, until recovery. A reply that waited for the
    /// turn's shared write (`publish_through`) is the exception: when that write fails,
    /// every waiter of the turn, readers included, gets the error instead of a reply.
    pub fn publish_before_reply(&mut self) -> Result<(), PersistenceError> {
        debug_assert!(self.profile != DurabilityProfile::Always);
        if self.staged.is_empty() || self.failed.is_some() {
            return Ok(());
        }

        self.publish_staged()?;
        self.mark_published();

        Ok(())
    }

    /// An `everysec` reply that waits for records staged in memory (its own or another
    /// task's) may wait for the end of the scheduler turn instead of writing them now, so
    /// every connection served in the turn shares one `write`. True when there is such a
    /// record and the log can still write it.
    pub fn reply_waits_for_publish(&self) -> bool {
        self.profile == DurabilityProfile::Everysec
            && !self.staged.is_empty()
            && self.failed.is_none()
    }

    /// Keeps the log staging until the turn's first waiter calls `publish_through`: one
    /// level of the shared group counter belongs to the turn, so closing a connection's
    /// group does not write. Only called when a reply waits for the staged records, so
    /// a waiter always releases it.
    pub fn hold_for_tick(&mut self) {
        if !self.tick_hold && self.profile == DurabilityProfile::Everysec {
            self.begin_group();
            self.tick_hold = true;
        }
    }

    /// Ends the turn's hold, writing what is staged (unless another task's group is still
    /// open). Also called from the shard's maintenance tick, so a hold whose waiter went
    /// away does not keep records in memory.
    pub fn release_tick_hold(&mut self) -> Result<(), PersistenceError> {
        if !std::mem::take(&mut self.tick_hold) {
            return Ok(());
        }

        self.end_group()
    }

    /// Writes the records up to `through` (the last sequence when a reply began waiting)
    /// before that reply leaves. A waiter whose records another waiter's `write` already
    /// published makes no system call; one whose records are still staged after a failed
    /// write gets the error and no reply, as it would have from its own `write`.
    pub fn publish_through(&mut self, through: u64) -> Result<(), PersistenceError> {
        self.release_tick_hold()?;
        if self.staged.is_empty() || self.staged_first_sequence > through {
            return Ok(());
        }
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }

        self.publish_staged()?;
        self.mark_published();

        Ok(())
    }

    /// Sync dirty `always` bytes before their replies are sent.
    pub fn sync_pending(&mut self) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }
        // A failed log may have cut this batch with another one, or lost its pages in a
        // sync another client saw fail: no batch is acknowledged until recovery. A caller
        // whose batch wrote nothing does not call this while the log is failed.
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }
        if self.dirty_bytes == 0 {
            self.mark_published();
            return Ok(());
        }

        self.observe_sync_error()?;
        self.publish_staged()?;
        self.fsync_always_or_cut()?;
        self.durable.store(self.last_sequence(), Ordering::Release);
        self.mark_published();
        Ok(())
    }

    /// Sync the open segment. Under `always` the unsynced records are not acknowledged yet,
    /// and once a sync fails the log refuses every batch until restart, so every client of
    /// those records gets the error, whichever caller's sync failed (its own batch, a `SAVE`
    /// or a rotation in between): cut them so a restart does not revive them. With the
    /// worker the shard thread does not know what it synced.
    fn fsync_always_or_cut(&mut self) -> Result<(), PersistenceError> {
        let synced = self.fsync_file();

        if synced.is_err() {
            self.cut_unsynced_always_records();
        }
        synced
    }

    /// See `fsync_always_or_cut`: the log failed, so no unsynced `always` record will be
    /// acknowledged.
    pub(super) fn cut_unsynced_always_records(&mut self) {
        if self.profile == DurabilityProfile::Always
            && self.sync_worker.is_none()
            && self.synced_pos < self.write_pos
        {
            self.forget_unsynced_since(self.synced_pos);
        }
    }

    /// `sync_pending` for a batch that began when the last sequence was `start`. A batch
    /// that wrote nothing has nothing to acknowledge, so a failed log does not fail it.
    pub fn sync_batch(&mut self, start: u64) -> Result<(), PersistenceError> {
        if self.failed.is_some() && self.last_sequence() == start {
            return Ok(());
        }

        self.sync_pending()
    }

    /// Closes a batch whose client went away before its commit (`start` as in `sync_batch`).
    /// Its records are still synced and published now, so they do not wait for another
    /// client's sync on this shard.
    pub fn abandon_group(&mut self, start: u64) -> Result<(), PersistenceError> {
        self.end_group()?;
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }
        if self.sync_worker.is_some() {
            // The worker syncs it; nobody waits for the result.
            return self.prepare_sync_barrier().map(drop);
        }

        self.sync_batch(start)
    }

    /// Capture the observed-write sequence before releasing the persistence lock.
    pub(crate) fn prepare_sync_barrier(
        &mut self,
    ) -> Result<Option<super::sync_worker::SyncBarrier>, PersistenceError> {
        if self.sync_worker.is_none() {
            return Ok(None);
        }
        if let Some(error) = &self.failed {
            return Err(PersistenceError::Failed(error.clone()));
        }
        self.observe_sync_error()?;
        self.publish_staged()?;
        self.mark_published();
        #[cfg(test)]
        if FAILING_SYNC_DIRS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&self.config.data_dir)
            && let Some(worker) = &self.sync_worker
        {
            // The worker's syncs fail from here on, like the shard thread's would.
            worker.fail_sync_for_test();
        }
        let sequence = self.last_sequence();
        let durable = self.durable.load(Ordering::Acquire);
        if durable >= sequence {
            self.dirty_bytes = 0;
        } else if durable >= self.barrier_mark.0 {
            self.dirty_bytes = self.dirty_bytes.saturating_sub(self.barrier_mark.1);
        }
        self.barrier_mark = (sequence, self.dirty_bytes);
        if self.sync_worker_file.is_none() {
            let file = self.file.as_ref().ok_or_else(|| {
                PersistenceError::Failed("WAL-backed profile has no open segment".into())
            })?;
            let cloned = match file.try_clone() {
                Ok(cloned) => cloned,
                Err(error) => return self.fail(error),
            };
            self.sync_worker_file = Some(Arc::new(cloned));
        }
        self.sync_worker
            .as_ref()
            .expect("enabled sync worker")
            .submit(
                Arc::clone(self.sync_worker_file.as_ref().expect("sync file")),
                sequence,
            )
            .map(Some)
    }

    /// Every sync so far, the worker's and the shard thread's: `(count, nanoseconds)`.
    pub(crate) fn sync_totals(&self) -> (u64, u64) {
        let (count, nanos) = self.sync_worker_stats().unwrap_or((0, 0));

        (
            count.saturating_add(self.inline_syncs),
            nanos.saturating_add(self.inline_sync_nanos),
        )
    }

    pub(crate) fn sync_worker_stats(&self) -> Option<(u64, u64)> {
        self.sync_worker.as_ref().map(|worker| worker.stats())
    }

    pub(super) fn sync_open_segment(&mut self) -> Result<bool, PersistenceError> {
        if self.profile != DurabilityProfile::Always || self.group > 0 {
            return Ok(false);
        }

        self.fsync_file()?;
        Ok(true)
    }

    fn fsync_file(&mut self) -> Result<(), PersistenceError> {
        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
        }
        super::crash::hit(super::crash::CrashSite::Sync);
        // The kernel reports a failed sync once: a later one may come back clean for pages
        // it dropped.
        if self.sync_failed {
            return Err(PersistenceError::Failed(
                self.failed
                    .clone()
                    .unwrap_or_else(|| "WAL fsync failed earlier".into()),
            ));
        }

        let started = Instant::now();
        let synced = {
            let Some(file) = &mut self.file else {
                return Err(PersistenceError::Failed(
                    "WAL-backed profile has no open segment".into(),
                ));
            };

            #[cfg(test)]
            if let Some(error) = injected_sync_fault(&self.config.data_dir, self.shard_id) {
                return self.fail_sync(error.to_string());
            }

            // Data-only sync: on the preallocated segment the size rarely changes, so no
            // metadata commit is forced per sync.
            file.sync_data()
        };

        self.inline_syncs = self.inline_syncs.saturating_add(1);
        self.inline_sync_nanos = self
            .inline_sync_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);

        if let Err(error) = synced {
            return self.fail_sync(error.to_string());
        }

        // A sync on another descriptor of this file (a background or prefill sync) may have
        // taken the error for pages this one now reports as clean.
        self.observe_sync_error()?;
        self.synced_pos = self.write_pos;
        self.dirty_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    pub(super) fn note_appended(&mut self, synced: bool) -> Result<(), PersistenceError> {
        let sequence = self.next_sequence;

        if self.group == 0 {
            self.published.store(sequence, Ordering::Release);
        }

        if synced {
            self.durable.store(sequence, Ordering::Release);
        }

        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("WAL sequence exhausted".into()))?;
        Ok(())
    }

    pub(super) fn mark_published(&mut self) {
        let sequence = self.last_sequence();

        if sequence > 0 {
            self.published.store(sequence, Ordering::Release);
        }
    }

    pub(super) fn observe_sync_error(&mut self) -> Result<(), PersistenceError> {
        if let Some(worker) = &self.sync_worker
            && let Err(error) = worker.check()
        {
            let message = match error {
                PersistenceError::Failed(message) => message,
                other => other.to_string(),
            };
            return self.fail_sync(message);
        }
        if let Some(message) = self.sync_error.take() {
            return self.fail_sync(message);
        }

        Ok(())
    }

    pub(super) fn wait_background_sync(&self) {
        while self.sync_running.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Retire the bytes a finished background sync covered. A failed one leaves them dirty and
    /// its error for `observe_sync_error`.
    fn settle_background_sync(&mut self) {
        if self.sync_in_flight == 0 || self.sync_running.load(Ordering::Acquire) {
            return;
        }

        if self.sync_error.is_set() {
            return;
        }

        self.dirty_bytes = self.dirty_bytes.saturating_sub(self.sync_in_flight);
        self.sync_in_flight = 0;
    }

    pub(super) fn poke_background_sync(&mut self) -> Result<(), PersistenceError> {
        self.observe_sync_error()?;
        // After a failed sync a clean one proves nothing: `durable` stays where it was.
        if self.sync_failed {
            return Ok(());
        }
        self.settle_background_sync();

        if self.profile != DurabilityProfile::Everysec {
            return Ok(());
        }

        self.publish_staged()?;
        self.mark_published();

        if self.dirty_bytes == 0 || self.last_sync.elapsed() < self.config.sync_interval {
            return Ok(());
        }

        if self.sync_running.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let Some(file) = &self.file else {
            self.sync_running.store(false, Ordering::Release);

            return Ok(());
        };

        let cloned = match file.try_clone() {
            Ok(file) => file,
            Err(error) => {
                self.sync_running.store(false, Ordering::Release);

                return self.fail(error);
            }
        };

        #[cfg(test)]
        let injected = super::crash::take_io_fault(super::crash::CrashSite::Sync)
            .map(|error| (error, super::crash::take_sync_hold()));
        let sequence = self.last_sequence();
        let running = Arc::clone(&self.sync_running);
        let durable = Arc::clone(&self.durable);
        let sync_error = Arc::clone(&self.sync_error);

        self.sync_in_flight = self.dirty_bytes;
        self.last_sync = Instant::now();

        if let Err(error) = std::thread::Builder::new()
            .name("ruvio-wal-sync".into())
            .spawn(move || {
                let _clear = ClearSync(running);
                #[cfg(test)]
                let synced = match injected {
                    Some((error, hold)) => {
                        // Fail after the caller moved on, like a slow device; a test that
                        // holds the sync decides when.
                        if let Some(hold) = hold {
                            super::crash::wait_released(&hold);
                        }
                        Err(error)
                    }
                    None => cloned.sync_data(),
                };
                #[cfg(not(test))]
                let synced = cloned.sync_data();

                if let Err(error) = synced {
                    sync_error.report(error.to_string());

                    return;
                }

                durable.fetch_max(sequence, Ordering::Release);
            })
        {
            self.sync_running.store(false, Ordering::Release);
            self.sync_in_flight = 0;

            return self.fail(io::Error::other(error));
        }

        Ok(())
    }
}
