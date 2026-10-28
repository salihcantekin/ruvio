//! Opening a shard's WAL sink, its accessors, and the hot-record and checkpoint durability checks.

use super::*;

impl WalSink {
    pub fn open(
        config: PersistenceConfig,
        shard_id: u32,
        limits: ProtocolLimits,
    ) -> Result<(Self, RecoveredMutations, RecoveryReport), PersistenceError> {
        Self::open_after(config, shard_id, limits, 0)
    }

    pub fn open_after(
        config: PersistenceConfig,
        shard_id: u32,
        limits: ProtocolLimits,
        after_sequence: u64,
    ) -> Result<(Self, RecoveredMutations, RecoveryReport), PersistenceError> {
        config.validate().map_err(|message| {
            PersistenceError::Failed(format!("invalid persistence configuration: {message}"))
        })?;

        if !config.profile.uses_wal() {
            let published = Arc::new(AtomicU64::new(0));
            let durable = Arc::new(DurableSequence::new(0));
            let backlog_floor = Arc::new(AtomicU64::new(u64::MAX));
            let replica_count = Arc::new(AtomicU64::new(0));

            return Ok((
                Self {
                    profile: config.profile,
                    config,
                    limits,
                    shard_id,
                    next_sequence: 1,
                    segment_id: 0,
                    segment_first_sequence: 1,
                    file: None,
                    file_length: 0,
                    write_pos: 0,
                    synced_pos: 0,
                    prealloc_end: 0,
                    prealloc: false,
                    prefill: Prefill::default(),
                    dirty_bytes: 0,
                    barrier_mark: (0, 0),
                    last_sync: Instant::now(),
                    failed: None,
                    sync_failed: false,
                    hot_through: 0,
                    last_recovery: None,
                    published,
                    durable,
                    backlog_floor,
                    replica_count,
                    database: 0,
                    staged: Vec::new(),
                    staged_first_sequence: 0,
                    record: Vec::new(),
                    group: 0,
                    group_bytes: 0,
                    tick_hold: false,
                    sync_running: Arc::new(AtomicBool::new(false)),
                    sync_error: Arc::default(),
                    sync_in_flight: 0,
                    sync_worker: None,
                    sync_worker_file: None,
                    inline_syncs: 0,
                    inline_sync_nanos: 0,
                },
                Vec::new(),
                RecoveryReport::default(),
            ));
        }

        let directory = shard_wal_directory(&config.data_dir, shard_id);

        fs::create_dir_all(&directory)?;
        let segments = discover_segments(&directory)?;
        let (mutations, report, last_segment) = recover_segments(
            &segments,
            shard_id,
            limits,
            config.wal_segment_bytes,
            after_sequence,
            false,
        )?;
        // A checkpoint can cover records that never reached the log (it was published while
        // the log was failing): continue after the checkpoint, never inside what it covers.
        let mut report = report;

        report.last_sequence = report.last_sequence.max(after_sequence);
        let next_sequence = report
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Corruption("WAL sequence overflow".into()))?;

        // A segment written under an older minor is not appended to: its header would let an
        // older server open records it cannot read. The next record starts a new segment.
        let (file, segment_id, segment_first_sequence, file_length) = if let Some(segment) =
            last_segment.filter(|segment| {
                segment.next_sequence == next_sequence && segment.minor == FORMAT_MINOR
            }) {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&segment.path)?;
            let file_length = file.seek(SeekFrom::End(0))?;
            (
                file,
                segment.segment_id,
                segment.first_sequence,
                file_length,
            )
        } else {
            create_segment(&directory, shard_id, next_sequence)?
        };

        let published = Arc::new(AtomicU64::new(report.last_sequence));
        let durable = Arc::new(DurableSequence::new(report.last_sequence));
        let backlog_floor = Arc::new(AtomicU64::new(u64::MAX));
        let replica_count = Arc::new(AtomicU64::new(0));
        let sync_error = Arc::new(SyncError::default());
        let prefill_enabled =
            cfg!(target_os = "linux") && config.wal_prefill.enabled_for(config.profile);
        let sync_worker = if config.sync_worker_active {
            Some(super::sync_worker::SyncWorker::new(
                Arc::clone(&durable),
                shard_id,
            )?)
        } else {
            None
        };

        Ok((
            Self {
                profile: config.profile,
                config,
                limits,
                shard_id,
                next_sequence,
                segment_id,
                segment_first_sequence,
                file: Some(file),
                file_length,
                write_pos: file_length,
                synced_pos: file_length,
                prealloc_end: file_length,
                prealloc: prefill_enabled,
                prefill: Prefill::reporting_to(Arc::clone(&sync_error)),
                dirty_bytes: 0,
                barrier_mark: (0, 0),
                last_sync: Instant::now(),
                failed: None,
                sync_failed: false,
                hot_through: 0,
                last_recovery: None,
                published,
                durable,
                backlog_floor,
                replica_count,
                database: 0,
                staged: Vec::with_capacity(WAL_GROUP_BYTES),
                staged_first_sequence: 0,
                record: Vec::new(),
                group: 0,
                group_bytes: 0,
                tick_hold: false,
                sync_running: Arc::new(AtomicBool::new(false)),
                sync_error,
                sync_in_flight: 0,
                sync_worker,
                sync_worker_file: None,
                inline_syncs: 0,
                inline_sync_nanos: 0,
            },
            mutations,
            report,
        ))
    }

    /// The configured segment size: no record larger than this fits in the log.
    pub(crate) fn segment_bytes(&self) -> u64 {
        self.config.wal_segment_bytes
    }

    pub fn published(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.published)
    }

    /// Last sequence synced to disk. Replication ships through this cursor.
    pub fn durable(&self) -> Arc<DurableSequence> {
        Arc::clone(&self.durable)
    }

    /// Lowest sequence a connected replica still needs. `u64::MAX` holds nothing.
    pub fn backlog_floor(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.backlog_floor)
    }

    /// Connected replicas whose socket this primary is writing.
    pub fn replica_count(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.replica_count)
    }

    pub fn profile(&self) -> DurabilityProfile {
        self.profile
    }

    /// Last sequence synced to disk (see `durable`).
    pub fn durable_sequence(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    pub fn last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    /// Whether the log on disk still starts at the first sequence: nothing was compacted
    /// away and no base was installed, so replaying it from sequence 0 rebuilds the shard.
    pub fn holds_every_record(&self) -> Result<bool, PersistenceError> {
        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);
        let first = discover_segments(&directory)?
            .first()
            .and_then(|path| path.file_stem()?.to_str()?.parse::<u64>().ok());

        Ok(first.is_none_or(|first| first <= 1))
    }

    pub fn install_base(&mut self, sequence: u64) -> Result<(), PersistenceError> {
        if !self.profile.uses_wal() {
            return Ok(());
        }

        let next = sequence
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("WAL sequence overflow".into()))?;
        let directory = shard_wal_directory(&self.config.data_dir, self.shard_id);

        if let Some(worker) = &self.sync_worker {
            worker.drain()?;
            worker.reset(sequence)?;
            self.sync_worker_file = None;
        }
        self.file = None;
        fs::create_dir_all(&directory)?;

        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("wal") {
                fs::remove_file(path)?;
            }
        }

        let (file, segment_id, first_sequence, file_length) =
            create_segment(&directory, self.shard_id, next)?;

        self.file = Some(file);
        self.segment_id = segment_id;
        self.segment_first_sequence = first_sequence;
        self.file_length = file_length;
        self.write_pos = file_length;
        self.synced_pos = file_length;
        self.prealloc_end = file_length;
        self.prefill.reset();
        self.next_sequence = next;
        self.dirty_bytes = 0;
        self.staged.clear();
        self.wait_background_sync();
        self.sync_in_flight = 0;
        self.published.store(sequence, Ordering::Release);
        self.durable.store(sequence, Ordering::Release);
        Ok(())
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn snapshot_journal_bytes(&self) -> usize {
        self.config.snapshot_journal_bytes
    }

    pub fn snapshot_interval(&self) -> std::time::Duration {
        self.config.snapshot_interval
    }

    pub fn snapshot_min_mutations(&self) -> u64 {
        self.config.snapshot_min_mutations
    }

    pub fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    pub fn log_fields(&self) -> (u64, u64) {
        (self.last_sequence(), self.dirty_bytes())
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }

    /// A sync failed: the log refuses writes until restart (see `sync_failed`).
    pub fn sync_failed(&self) -> bool {
        self.sync_failed
    }

    /// Before a hot-key write: a failed log tries to resume, as its next record would, and
    /// refuses the write while it cannot, even one that would record nothing.
    pub fn ready_for_record(&mut self) -> Result<(), PersistenceError> {
        if self.failed.is_none() {
            return Ok(());
        }

        self.recover_if_failed()
    }

    /// The records just made hold a hot-key write now in the shared hot store.
    pub fn note_hot_record(&mut self) {
        self.hot_through = self.last_sequence();
    }

    /// For a cluster round's hot partition, taken from the shared hot store: under
    /// `always`, syncs this log's hot-key records, so every hot write the partition holds
    /// is acknowledged or will be. Fails while that is not so; the flag says whether a
    /// later attempt may succeed: not after a failed sync past which `hot_through` lies
    /// (the store may hold a hot write the log cannot vouch for), which lasts until restart. A log that failed for other records alone does not keep
    /// the partition out.
    pub fn sync_hot_records(&mut self) -> Result<(), (PersistenceError, bool)> {
        if self.profile != DurabilityProfile::Always {
            return Ok(());
        }

        let shard_id = self.shard_id;
        let refused = move |message: &str| {
            PersistenceError::Failed(format!(
                "hot partition not taken: shard {shard_id}'s log refused a hot-key write the \
                 hot store may still hold ({message})"
            ))
        };

        // A base installed since (a replica's resync) may have moved the sequence back.
        let hot_through = self.hot_through.min(self.last_sequence());

        if hot_through <= self.durable.load(Ordering::Acquire) {
            return Ok(());
        }

        // Records of a write failure are written again once the log resumes.
        if self.failed.is_some() && !self.sync_failed {
            let _ = self.recover_if_failed();
        }

        self.sync_for_checkpoint(hot_through).map_err(|error| {
            if self.sync_failed {
                (refused(&error.to_string()), false)
            } else {
                (error, true)
            }
        })
    }

    pub fn failure(&self) -> Option<&str> {
        self.failed.as_deref().or_else(|| {
            self.sync_worker
                .as_ref()
                .filter(|worker| worker.unavailable())
                .map(|_| "WAL sync worker unavailable")
        })
    }

    /// Whether a checkpoint of memory as it stood at `sequence` may be published. With
    /// `always`, records are applied in memory before their batch syncs; when that sync
    /// fails their clients are refused and the records are cut from the log, but memory
    /// keeps them. A checkpoint holding them would revive refused writes after a restart, so
    /// a failed log publishes none past its last good sync. Acknowledged writes are all at
    /// or before that sync and stay in the log.
    pub fn checkpoint_may_cover(&self, sequence: u64) -> Result<(), PersistenceError> {
        if self.profile == DurabilityProfile::Always
            && sequence > self.durable.load(Ordering::Acquire)
            && let Some(error) = self.failure()
        {
            return Err(PersistenceError::Failed(error.to_owned()));
        }

        Ok(())
    }

    /// Before a checkpoint of memory as it stood at `sequence` is published: under `always`,
    /// records up to there that are not synced yet (a batch between its group's end and its
    /// sync, or one the sync worker holds) are synced now. If that sync fails their clients
    /// are refused, so the checkpoint is too (see `checkpoint_may_cover`).
    pub fn sync_for_checkpoint(&mut self, sequence: u64) -> Result<(), PersistenceError> {
        if self.profile != DurabilityProfile::Always
            || sequence <= self.durable.load(Ordering::Acquire)
        {
            return Ok(());
        }

        self.checkpoint_may_cover(sequence)?;
        if self.sync_worker.is_some() {
            self.flush()?;
        } else {
            self.sync_pending()?;
        }

        if sequence > self.durable.load(Ordering::Acquire) {
            return Err(PersistenceError::Failed(
                "checkpoint covers WAL records that are not synced".into(),
            ));
        }

        Ok(())
    }
}
