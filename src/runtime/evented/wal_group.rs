use super::*;

pub(super) struct TimeSample<'a> {
    pub(super) state: &'a ShardState,
}

impl Drop for TimeSample<'_> {
    fn drop(&mut self) {
        self.state.store.borrow().clear_time_sample();
    }
}

pub(super) enum WalGroupCommand<'a, 'input> {
    Hot(&'a crate::protocol::HotCommand<'input>),
    Parsed(&'a Command<'input>),
}

/// A blocking command run on the connection's own loop may join the group: when it finds its
/// data it is a plain synchronous command, and when it has to wait the loop commits the
/// group before parking it. Forwarded batches park in place, so they keep it a boundary.
pub(super) fn blocking_joins_group(command: &Command<'_>, session: &TxSession) -> bool {
    !session.is_multi()
        && !session.in_pubsub
        // `FILLGET WAIT` retries instead of parking, so it stays a group boundary.
        && crate::protocol::command_may_block(command)
        && !matches!(command, Command::FillGet { .. })
}

/// Scripts and `MULTI` transactions on the connection's own loop may join the group under
/// `always` and `everysec`: they run synchronously with no await before their reply and
/// write their changes as one `Transaction` record, so they share the group's write (and,
/// for `always`, its sync) like a plain write. A record the WAL refuses while staging still
/// rolls them back. Under `always` the record is written through at once, so a failed write
/// rolls them back too, and a failed group sync refuses them like any grouped write. Under
/// `everysec` it is staged and written with the group (one `write` per pipeline), so a failed
/// group write cuts their client off like any grouped write's, their changes staying applied
/// and their record kept for the log's recovery. `MULTI` and the commands it queues write
/// nothing. A transaction that
/// flushes or swaps databases stays a boundary. Only a connection that never waits on another
/// shard asks: an open group must not stay open across such a wait. Forwarded batches keep
/// scripts and `EXEC` boundaries.
///
/// Nothing awaits while such a group is open: the script and `EXEC` arms of `dispatch_command`
/// run the whole script or batch while holding the store `RefCell` borrow and the persistence
/// lock, which clippy's `await_holding_refcell_ref` and `await_holding_lock` (denied in CI)
/// keep free of awaits; only blocking commands can park. Adding an await to either arm means
/// taking scripts and `EXEC` out of the group first.
pub(super) fn transaction_joins_group(command: &Command<'_>, session: &TxSession) -> bool {
    let script = matches!(
        command,
        Command::Eval { .. } | Command::EvalSha { .. } | Command::FCall { .. }
    );

    if session.in_pubsub {
        return false;
    }
    if !session.is_multi() {
        return script || matches!(command, Command::Multi);
    }

    match command {
        Command::Exec => !session.queued().iter().any(|queued| {
            matches!(
                queued,
                Command::FlushDb | Command::FlushAll | Command::SwapDb(..)
            )
        }),
        command => script || eligible_outside_multi(WalGroupCommand::Parsed(command)),
    }
}

// Only reviewed synchronous execution paths may keep the durability group open.
// A newly added hot variant must be classified; other parsed commands default
// to a boundary, including blocking/session commands (scripts and transactions:
// `transaction_joins_group`).
#[inline]
pub(super) fn wal_group_eligible(command: WalGroupCommand<'_, '_>, session: &TxSession) -> bool {
    if session.is_multi() || session.in_pubsub {
        return false;
    }

    eligible_outside_multi(command)
}

#[inline]
fn eligible_outside_multi(command: WalGroupCommand<'_, '_>) -> bool {
    match command {
        WalGroupCommand::Hot(command) => match command {
            crate::protocol::HotCommand::Ping(_)
            | crate::protocol::HotCommand::Get(_)
            | crate::protocol::HotCommand::Set(..)
            | crate::protocol::HotCommand::SetEx(..)
            | crate::protocol::HotCommand::Delete(_)
            | crate::protocol::HotCommand::Increment(..) => true,
        },
        // Plain data commands execute synchronously with no await before their
        // reply, so they can share one sync barrier. Blocking reads, scripts,
        // flush/swap, transactions and admin commands stay boundaries.
        WalGroupCommand::Parsed(command) => matches!(
            command,
            Command::Limit { .. }
                | Command::IdempotencyBegin { .. }
                | Command::IdempotencyComplete { .. }
                | Command::IdempotencyAbort { .. }
                | Command::Get { .. }
                | Command::Set { .. }
                | Command::GetEx { .. }
                | Command::Increment { .. }
                | Command::IncrementCapped { .. }
                | Command::MGet { .. }
                | Command::MSet { .. }
                | Command::GetSet { .. }
                | Command::Append { .. }
                | Command::IncrementFloat(..)
                | Command::SetRange { .. }
                | Command::BitField { .. }
                | Command::Copy { .. }
                | Command::ListSet { .. }
                // `BLMOVE` (`block: Some`) retries with an await; plain `LMOVE` does not.
                | Command::ListMove { block: None, .. }
                | Command::StrLen { .. }
                | Command::Exists { .. }
                | Command::Type { .. }
                | Command::Del { .. }
                | Command::CreateTtl { .. }
                | Command::Expire { .. }
                | Command::PExpireAt { .. }
                | Command::Ttl { .. }
                | Command::PTtl { .. }
                | Command::Persist { .. }
                | Command::ListPush { .. }
                | Command::ListPushBack { .. }
                | Command::ListPop { .. }
                | Command::ListPopBack { .. }
                | Command::ListLen { .. }
                | Command::ListIndex { .. }
                | Command::ListTrim { .. }
                | Command::ListRange { .. }
                | Command::SetAdd { .. }
                | Command::SetRemove { .. }
                | Command::SetContains { .. }
                | Command::SetCard { .. }
                | Command::SetMembers { .. }
                | Command::SetPop { .. }
                | Command::SetRandom { .. }
                | Command::SetAlgebra { .. }
                | Command::SetMove { .. }
                | Command::HashSet { .. }
                | Command::HashMSet { .. }
                | Command::HashDelete { .. }
                | Command::HashGet { .. }
                | Command::HashMGet { .. }
                | Command::HashLen { .. }
                | Command::HashGetAll { .. }
                | Command::HashExists { .. }
                | Command::HashKeys { .. }
                | Command::HashValues { .. }
                | Command::HashIncrement { .. }
                | Command::HashSetNx { .. }
                | Command::HashStrLen { .. }
                | Command::HashExpire { .. }
                | Command::HashTtl { .. }
                | Command::HashPersist { .. }
                | Command::SortedSetAdd { .. }
                | Command::SortedSetRange { .. }
                | Command::SortedSetRangeByScore { .. }
                | Command::SortedSetRangeByLex { .. }
                | Command::SortedSetRemove { .. }
                | Command::SortedSetIncr { .. }
                | Command::SortedSetCard { .. }
                | Command::SortedSetScore { .. }
                | Command::SortedSetRank { .. }
                | Command::SortedSetPop { .. }
                | Command::SortedSetPopByScore { .. }
                | Command::SortedSetMoveDue { .. }
                | Command::SortedSetRemoveRange { .. }
                | Command::GeoAdd { .. }
                | Command::GeoDist { .. }
                | Command::GeoPos { .. }
                | Command::GeoHash { .. }
                | Command::BloomReserve { .. }
                | Command::BloomAdd { .. }
                | Command::BloomContains { .. }
                | Command::BloomAddMany { .. }
                | Command::BloomContainsMany { .. }
                | Command::BloomInsert { .. }
                | Command::BloomInfo { .. }
                | Command::BloomCard { .. }
                | Command::SetIfEq { .. }
                | Command::SetIfCmp { .. }
                | Command::DelIfEq { .. }
                | Command::ListPushCapped { .. }
                | Command::JsonGet { .. }
                | Command::JsonMGet { .. }
                | Command::JsonSet { .. }
                | Command::JsonSetIf { .. }
                | Command::JsonMerge { .. }
                | Command::JsonDel { .. }
                | Command::JsonOp { .. }
                | Command::StreamAdd { .. }
                | Command::StreamLen { .. }
                | Command::StreamRange { .. }
                | Command::StreamDelete { .. }
                | Command::StreamTrim { .. }
                | Command::StreamTrimMinId { .. }
        ),
    }
}

/// Syncs this connection's `always` work since `start` and, once it is on disk, moves
/// `start` to the end of the log for its next commit. A failed sync keeps `start`: a forwarded
/// batch turns a failed commit at a group boundary into one command's error, and its replies
/// before that command (records the failure cut) wait for the batch's last commit, which must
/// fail too instead of finding nothing new to sync.
fn sync_since(
    persistence: &mut crate::persistence::WalSink,
    start: &mut u64,
) -> Result<(), crate::PersistenceError> {
    persistence.sync_batch(*start)?;
    *start = persistence.last_sequence();
    Ok(())
}

pub(super) struct OpenWalGroup<'a> {
    pub(super) state: &'a ShardState,
    pub(super) open: bool,
    pub(super) uses_wal: bool,
    pub(super) always: bool,
    /// Sequence of the last record when this connection's work since its previous commit
    /// began (taken before it ran any command, grouped or not): unchanged at commit means
    /// it wrote nothing. Only kept for `always`.
    start_sequence: u64,
    /// A sync worker barrier of this group failed: every later commit fails too (see
    /// `sync_since` for why).
    worker_failed: bool,
    /// A served pop moved `start_sequence` past the records others wrote while it waited
    /// (`skip_records_written_while_parked`). If nothing is written after it, its reply
    /// depends on no unsynced record, so the next commit syncs nothing: syncing the others'
    /// records could only fail it for their failure, after its element was taken.
    skipped_while_parked: bool,
    /// Waiting for the end of the scheduler turn with the turn's hold possibly still taken
    /// (see `commit_deferred`); dropped here, the hold is released.
    waiting_for_tick: bool,
    /// Last record sequence when the open group began: the records past it at commit are
    /// this connection's batch.
    group_start_sequence: u64,
}

/// A batch with more records than this already shares its own `write` among them, so its
/// reply does not wait for the end of the turn: the extra turn would cost more than the
/// `write` it saves.
const TICK_SHARE_MAX_RECORDS: u64 = 4;

impl<'a> OpenWalGroup<'a> {
    pub(super) fn new(state: &'a ShardState) -> Self {
        let profile = state.published_config.persistence.profile;
        let always = profile == crate::DurabilityProfile::Always;
        // A write outside a group may still be staged behind another connection's group, so
        // the mark is taken before any command runs, not when one opens a group.
        let start_sequence = if always {
            state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_sequence()
        } else {
            0
        };

        Self {
            state,
            open: false,
            uses_wal: profile.uses_wal(),
            always,
            start_sequence,
            worker_failed: false,
            skipped_while_parked: false,
            waiting_for_tick: false,
            group_start_sequence: 0,
        }
    }

    pub(super) fn begin(&mut self) {
        if self.open || !self.uses_wal {
            return;
        }

        {
            let mut persistence = self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            persistence.begin_group();
            self.group_start_sequence = persistence.last_sequence();
        }
        self.open = true;
    }

    /// A command parked (outside the group, after its commit) and another client's write
    /// served it; `served_by` is that write's last WAL record, if it wrote one. The records
    /// written while it waited are not this connection's, so its next commit starts after
    /// them (a failed sync of theirs is not its failure), unless the serving write's record
    /// is not durable yet: the element came with it, so the reply waits for its sync and
    /// fails with it.
    pub(super) fn skip_records_written_while_parked(&mut self, served_by: Option<u64>) {
        if !self.always || self.open {
            return;
        }

        let persistence = self
            .state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if served_by.is_some_and(|sequence| persistence.durable_sequence() < sequence) {
            return;
        }
        self.start_sequence = persistence.last_sequence();
        self.skipped_while_parked = true;
    }

    pub(super) async fn commit(&mut self) -> io::Result<()> {
        match self.commit_deferred().await? {
            Some(wait) => {
                let waited = wait.wait(self.state).await;

                // A timed-out wait counts as a failure too. The batch's records may still
                // reach the disk, but a timeout says nothing about whether they did, and
                // the earlier replies of a forwarded batch are sent after its last commit:
                // failing them (an error for a write that may persist) is safe, while an OK
                // for one the failing sync then cuts would not be.
                self.worker_failed |= waited.is_err();
                waited
            }
            None => Ok(()),
        }
    }

    /// Publishes the group and, for `always`, either syncs here (no sync worker) or hands back the
    /// worker barrier to wait on later. Lets a caller keep executing while the sync worker works,
    /// then send replies once `DurabilityWait::wait` returns.
    pub(super) async fn commit_deferred(&mut self) -> io::Result<Option<DurabilityWait>> {
        let mut wait_through = None;

        if self.open || (self.uses_wal && !self.always) {
            let mut persistence = self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // Under `everysec` with other clients on the shard, the records wait for the end
            // of the scheduler turn: the connections served in it stage theirs too and the
            // first to resume writes them all at once (`publish_through`). A lone client
            // writes now, as the wait would only add a turn, and so does a pipelined batch
            // (`TICK_SHARE_MAX_RECORDS`).
            let waits = !self.always
                && self.state.active_connections.get() > 1
                && (!self.open
                    || persistence.last_sequence() - self.group_start_sequence
                        <= TICK_SHARE_MAX_RECORDS)
                && persistence.reply_waits_for_publish();

            if waits {
                persistence.hold_for_tick();
            }
            if self.open {
                persistence
                    .end_group()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                self.open = false;
            }
            // Another task's open group may still hold this connection's records, grouped
            // or not, in memory: they reach the file before any of its replies does.
            if waits {
                wait_through = Some(persistence.last_sequence());
            } else if !self.always {
                persistence
                    .publish_before_reply()
                    .map_err(|error| io::Error::other(error.to_string()))?;
            }
        }

        if let Some(through) = wait_through {
            self.waiting_for_tick = true;
            tokio::task::yield_now().await;
            self.waiting_for_tick = false;
            self.state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .publish_through(through)
                .map_err(|error| io::Error::other(error.to_string()))?;
        }

        if !self.always {
            return Ok(None);
        }

        if std::mem::take(&mut self.skipped_while_parked)
            && self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_sequence()
                == self.start_sequence
        {
            return Ok(None);
        }

        tokio::task::yield_now().await;
        if self.state.published_config.persistence.sync_worker_active {
            // See `commit`: after a failed or timed-out barrier, every later commit of this
            // batch fails, so none of its replies is an OK for a write that may be lost.
            if self.worker_failed {
                return Err(io::Error::other("an earlier WAL sync of this batch failed"));
            }
            let barrier = {
                let mut persistence = self
                    .state
                    .persistence
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());

                let start =
                    std::mem::replace(&mut self.start_sequence, persistence.last_sequence());

                // After a persistence failure (disk full, I/O error) mutations are refused with
                // MISCONF; a batch that wrote nothing must still be answered, not dropped.
                if persistence.failure().is_some() && persistence.last_sequence() == start {
                    return Ok(None);
                }

                persistence
                    .prepare_sync_barrier()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .ok_or_else(|| io::Error::other("configured WAL sync worker is unavailable"))?
            };

            return Ok(Some(DurabilityWait {
                barrier,
                write_timeout: self.state.live.write_timeout.get(),
            }));
        }
        let (totals, sync_result) = {
            let mut persistence = self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let result = sync_since(&mut persistence, &mut self.start_sequence)
                .map_err(|error| io::Error::other(error.to_string()));

            (persistence.sync_totals(), result)
        };
        {
            let mut metrics = self.state.command_metrics.borrow_mut();

            metrics.wal_sync_count = totals.0;
            metrics.wal_sync_duration_nanos = u128::from(totals.1);
        }
        sync_result?;

        Ok(None)
    }
}

/// A group already handed to the sync worker; `wait` returns once it is on disk.
pub(super) struct DurabilityWait {
    barrier: crate::persistence::SyncBarrier,
    write_timeout: std::time::Duration,
}

impl DurabilityWait {
    pub(super) async fn wait(self, state: &ShardState) -> io::Result<()> {
        timeout(self.write_timeout, self.barrier.wait())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "WAL durability wait timed out"))?
            .map_err(|e| io::Error::other(e.to_string()))?;
        let (count, nanos) = {
            let persistence = state.persistence.lock().unwrap_or_else(|e| e.into_inner());

            if persistence.sync_worker_stats().is_none() {
                return Err(io::Error::other("WAL sync worker is unavailable"));
            }

            persistence.sync_totals()
        };
        let mut metrics = state.command_metrics.borrow_mut();
        metrics.wal_sync_count = count;
        metrics.wal_sync_duration_nanos = u128::from(nanos);
        Ok(())
    }
}

impl Drop for OpenWalGroup<'_> {
    fn drop(&mut self) {
        if self.waiting_for_tick {
            let _ = self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .release_tick_hold();
        }
        if self.open {
            let _ = self
                .state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .abandon_group(self.start_sequence);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::sync_since;
    use crate::mutation::{Mutation, MutationSink};
    use crate::persistence::{FAILING_SYNC_DIRS, PersistenceConfig, WalSink};

    fn set(sink: &mut WalSink, key: &[u8]) {
        sink.record(Mutation::SetBytes { key, value: b"v" })
            .unwrap();
    }

    /// A forwarded batch: a write in the group, then a command at a group boundary whose
    /// commit fails (and cuts the write), then the batch's last commit, which answers the
    /// write. That commit must fail as well.
    #[test]
    fn a_failed_commit_inside_a_batch_fails_the_batch_commit_too() {
        let directory = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = crate::protocol::ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();
        let mut start = sink.last_sequence();

        sink.begin_group();
        set(&mut sink, b"grouped");
        sink.end_group().unwrap();
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        let boundary = sync_since(&mut sink, &mut start);
        let batch = sync_since(&mut sink, &mut start);
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());
        drop(sink);

        assert!(boundary.is_err());
        assert!(
            batch.is_err(),
            "the batch was acknowledged for a write that is gone"
        );
        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert!(mutations.is_empty());
    }

    #[test]
    fn probe_nongroup_write_staged_under_other_group_acked_after_cut() {
        let directory = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            profile: crate::DurabilityProfile::Always,
            data_dir: directory.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let limits = crate::protocol::ProtocolLimits::default();
        let (mut sink, _, _) = WalSink::open(config.clone(), 0, limits).unwrap();

        set(&mut sink, b"one");
        // Connection X opens a batch and awaits forwarded replies before committing.
        sink.begin_group();
        let mut x = sink.last_sequence();
        set(&mut sink, b"x");
        // Connection Y runs a write outside any batch; X's open group keeps it unsynced.
        let mut y = sink.last_sequence();
        set(&mut sink, b"y");
        // X's commit: its sync fails and cuts every unsynced record, Y's too.
        sink.end_group().unwrap();
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        assert!(sync_since(&mut sink, &mut x).is_err());
        let y_ok = sync_since(&mut sink, &mut y).is_ok();
        FAILING_SYNC_DIRS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());
        drop(sink);

        let (_sink, mutations, _) = WalSink::open(config, 0, limits).unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(!y_ok, "Y was acknowledged for a write that is gone");
    }
}
