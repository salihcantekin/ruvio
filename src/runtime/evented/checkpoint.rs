use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::JoinHandle;

use super::*;
use crate::persistence::{ShardFile, SnapshotWriter, encode_entry};
use crate::store::SnapshotStep;

const CHUNK_BYTES: usize = 256 * 1024;
const FIRST_SLICE_MICROS: u64 = 500;
/// The longest a checkpoint slice grows after repeated aborts while the journal can still
/// grow. Each slice keeps a deadline, so a snapshot that overflows its journal does not
/// stall the shard for a whole walk; at this length the walk takes most of the shard's time
/// and outpaces the writes that fill the journal. Only a walk that overflows the journal at
/// its memory cap gets longer slices (see `after_abort`).
const MAX_SLICE_MICROS: u64 = 16_000;
/// The longest an overload slice (a walk that overflowed the journal at its memory cap)
/// may hold the shard: 16 times the normal longest slice. A quarter second walks on the
/// order of a million keys, and is about the worst pause a single large command
/// already causes; past it the attempt is given up instead of stalling clients for a
/// whole walk of a big shard.
const MAX_OVERLOAD_SLICE_MICROS: u64 = 256_000;
/// How long a given-up checkpoint waits before the next automatic attempt.
const OVERLOAD_RETRY: Duration = Duration::from_secs(5);
/// Each checkpoint given up under overload raises the journal's memory cap one level: a
/// quarter, then half, then all of the shard's data, never past the memory left under
/// the shard's limit (see `journal_cap`).
const TOP_JOURNAL_LEVEL: u32 = 2;
/// Past the top journal level the walk falls back to long blocking slices, from this
/// length and doubling after each abort: memory stays bounded and the stall is the price.
/// A slice that outlasts the rest of the walk lets no write in, so one attempt finishes.
const FALLBACK_LEVEL: u32 = TOP_JOURNAL_LEVEL + 1;
const FALLBACK_SLICE_MICROS: u64 = 2_000_000;
const SLICE_KEYS: usize = 64;

pub(super) fn checkpoint_due(state: &ShardState) -> bool {
    if state.replica_incomplete.get() {
        return false;
    }
    let (profile, interval, minimum) = {
        let wal = persistence_state(state);

        (
            wal.profile(),
            wal.snapshot_interval(),
            wal.snapshot_min_mutations(),
        )
    };

    if profile == crate::DurabilityProfile::Memory {
        return false;
    }

    if interval.is_zero() || state.last_checkpoint_at.get().elapsed() < interval {
        return false;
    }

    if state.mutations_since_checkpoint.get() == 0 {
        return false;
    }

    minimum == 0 || state.mutations_since_checkpoint.get() >= minimum
}

enum IoMessage {
    Chunk(Vec<u8>, u64),
    Finish,
}

/// One shard's running incremental checkpoint: the walk happens in bounded slices on the
/// shard thread, the file is written by a dedicated I/O thread.
pub(super) struct CheckpointJob {
    generation: u64,
    covered: u64,
    requested: u64,
    sender: SyncSender<IoMessage>,
    result: Receiver<Result<ShardFile, crate::PersistenceError>>,
    /// The I/O thread, joined when the job is dropped unfinished so a restarted job never
    /// shares its temporary file or round directory with it.
    writer: Option<JoinHandle<()>>,
    /// Tells the I/O thread to drop the chunks still queued instead of writing them.
    cancelled: Arc<AtomicBool>,
    buffer: Vec<u8>,
    buffered: u64,
    outbox: VecDeque<IoMessage>,
    journal_bound: usize,
    /// `journal_bound` is at its memory cap: another abort cannot grow it.
    journal_capped: bool,
    walk_done: bool,
    finishing: bool,
}

enum Progress {
    Running,
    /// A database-wide rewrite or the journal bound ended the snapshot; try again.
    Aborted,
    Finished(Result<ShardFile, crate::PersistenceError>),
}

#[allow(clippy::type_complexity)]
fn spawn_writer(
    writer: SnapshotWriter,
    cancelled: Arc<AtomicBool>,
) -> std::io::Result<(
    SyncSender<IoMessage>,
    Receiver<Result<ShardFile, crate::PersistenceError>>,
    JoinHandle<()>,
)> {
    let (sender, messages) = mpsc::sync_channel::<IoMessage>(4);
    let (done, result) = mpsc::channel();

    let handle = std::thread::Builder::new()
        .name("ruvio-snapshot-io".into())
        .spawn(move || {
            let mut writer = writer;

            for message in messages {
                if cancelled.load(Ordering::Acquire) {
                    break;
                }

                match message {
                    IoMessage::Chunk(bytes, entries) => {
                        if let Err(error) = writer.write_chunk(&bytes, entries) {
                            writer.abort();
                            let _ = done.send(Err(error));
                            return;
                        }
                    }
                    IoMessage::Finish => {
                        let _ = done.send(writer.finish());
                        return;
                    }
                }
            }

            // The shard dropped the job: leave no partial file behind.
            writer.abort();
        })?;

    Ok((sender, result, handle))
}

impl CheckpointJob {
    /// Moves queued messages into the I/O channel. `blocking` waits for room.
    fn flush(&mut self, blocking: bool) -> Result<(), crate::PersistenceError> {
        while let Some(message) = self.outbox.pop_front() {
            if blocking {
                if self.sender.send(message).is_err() {
                    return Err(self.writer_failure());
                }
            } else {
                match self.sender.try_send(message) {
                    Ok(()) => {}
                    Err(TrySendError::Full(message)) => {
                        self.outbox.push_front(message);
                        return Ok(());
                    }
                    Err(TrySendError::Disconnected(_)) => return Err(self.writer_failure()),
                }
            }
        }

        Ok(())
    }

    fn writer_failure(&self) -> crate::PersistenceError {
        match self.result.try_recv() {
            Ok(Err(error)) => error,
            _ => crate::PersistenceError::Failed("snapshot writer stopped".into()),
        }
    }

    fn queue_chunk(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let bytes = std::mem::replace(&mut self.buffer, Vec::with_capacity(CHUNK_BYTES));

        self.outbox
            .push_back(IoMessage::Chunk(bytes, std::mem::take(&mut self.buffered)));
    }
}

fn persistence_state(state: &ShardState) -> std::sync::MutexGuard<'_, crate::persistence::WalSink> {
    state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

/// Starts a checkpoint for this shard: snapshot point, round directory, writer thread.
///
/// A carried copy still being written is waited for first: it writes this shard's file of
/// the open round, and once it is submitted any newer file replaces it, never the reverse
/// (callers that must not wait check `carry_running` first).
fn start_job(state: &ShardState, requested: u64) -> Result<CheckpointJob, crate::PersistenceError> {
    collect_carry(state, true);
    let usage = state.store.borrow().memory_usage();
    let headroom = state
        .published_config
        .shard_memory_limits()
        .shard_admission_budget()
        .map(|limit| limit.saturating_sub(usage.charged_bytes()));
    let cap = journal_cap(
        persistence_state(state).snapshot_journal_bytes(),
        usage,
        state.checkpoint_ladder.get().level,
        headroom,
    );
    let journal_bytes = journal_bound(
        persistence_state(state).snapshot_journal_bytes(),
        state.checkpoint_ladder.get().shift,
        cap,
    );
    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);

    // Memory may hold `always` records whose sync failed and whose clients were refused.
    let coverable = {
        let wal = persistence_state(state);

        wal.checkpoint_may_cover(wal.last_sequence())
    };

    if let Err(error) = coverable {
        carry_published_file(state);
        return Err(error);
    }

    state
        .store
        .borrow_mut()
        .begin_snapshot(journal_bytes)
        .map_err(|message| crate::PersistenceError::Failed(message.into()))?;

    let covered = persistence_state(state).last_sequence();
    let started = (|| {
        let (manager, generation, shard_count) = if let Some(coordinator) = &state.cluster {
            let generation = coordinator.begin_round(shard_id)?;

            (
                coordinator.manager(),
                generation,
                coordinator.shard_count() as u32,
            )
        } else {
            // Past every round ever started: one that failed after CURRENT may be published.
            let generation = state
                .checkpoints
                .allocate_generation(state.checkpoint_generation.get())?;

            state.checkpoints.begin_round(generation)?;
            (&state.checkpoints, generation, 1)
        };
        let writer = manager.shard_writer(generation, shard_id, shard_count, covered)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let (sender, result, handle) = spawn_writer(writer, Arc::clone(&cancelled))?;

        Ok::<_, crate::PersistenceError>((generation, sender, result, handle, cancelled))
    })();

    match started {
        Ok((generation, sender, result, handle, cancelled)) => Ok(CheckpointJob {
            generation,
            covered,
            requested,
            sender,
            result,
            writer: Some(handle),
            cancelled,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            buffered: 0,
            outbox: VecDeque::new(),
            journal_bound: journal_bytes,
            journal_capped: journal_bytes >= cap,
            walk_done: false,
            finishing: false,
        }),
        Err(error) => {
            state.store.borrow_mut().abort_snapshot();
            Err(error)
        }
    }
}

/// Runs the job until `deadline` (never, when `None`). `blocking` waits on the writer.
fn advance(
    state: &ShardState,
    job: &mut CheckpointJob,
    deadline: Option<Instant>,
    blocking: bool,
) -> Progress {
    if job.finishing {
        if let Err(error) = job.flush(blocking) {
            return Progress::Finished(Err(error));
        }

        if !job.outbox.is_empty() {
            return Progress::Running;
        }

        let result = if blocking {
            job.result.recv().unwrap_or_else(|_| {
                Err(crate::PersistenceError::Failed(
                    "snapshot writer stopped".into(),
                ))
            })
        } else {
            match job.result.try_recv() {
                Ok(result) => result,
                Err(mpsc::TryRecvError::Empty) => return Progress::Running,
                Err(mpsc::TryRecvError::Disconnected) => Err(crate::PersistenceError::Failed(
                    "snapshot writer stopped".into(),
                )),
            }
        };

        return Progress::Finished(result);
    }

    if let Err(error) = job.flush(blocking) {
        return Progress::Finished(Err(error));
    }

    let mut store = state.store.borrow_mut();

    while !job.walk_done {
        if !job.outbox.is_empty() {
            // The writer is behind: end the slice instead of holding the shard.
            return Progress::Running;
        }

        if !store.journal_is_open() {
            return Progress::Aborted;
        }

        let mut encoded = Ok(());
        let step = store.snapshot_step(SLICE_KEYS, &mut |entry| {
            if encoded.is_ok() {
                encoded = entry.encode_into(&mut job.buffer);
                job.buffered += 1;
            }
        });

        if let Err(error) = encoded {
            return Progress::Finished(Err(error));
        }

        if job.buffer.len() >= CHUNK_BYTES {
            job.queue_chunk();

            if let Err(error) = job.flush(blocking) {
                return Progress::Finished(Err(error));
            }
        }

        match step {
            SnapshotStep::Done => job.walk_done = true,
            SnapshotStep::Waiting => {
                store.migrate_indexes(Duration::from_micros(200));
                return Progress::Running;
            }
            SnapshotStep::More => {}
        }

        if !job.walk_done && deadline.is_some_and(|limit| Instant::now() >= limit) {
            // A journal near its bound earns a longer slice next time round.
            return Progress::Running;
        }
    }

    // Keys that changed or vanished before the walk reached them come from the journal.
    let mut encoded = Ok(());

    for entry in store.finish_snapshot_journal() {
        if encoded.is_ok() {
            encoded = encode_entry(&mut job.buffer, &entry);
            job.buffered += 1;
        }
    }

    drop(store);

    if let Err(error) = encoded {
        return Progress::Finished(Err(error));
    }

    job.queue_chunk();
    job.outbox.push_back(IoMessage::Finish);
    job.finishing = true;
    Progress::Running
}

/// Records a finished file with the round (or single-shard generation) and compacts the WAL.
fn complete_job(
    state: &ShardState,
    job: &CheckpointJob,
    file: ShardFile,
    covers: u64,
) -> Result<(), crate::PersistenceError> {
    let covered = job.covered;

    // The batches the file covers may not be synced yet (a sync in flight on the worker, or
    // a batch whose group ended but whose sync has not run): sync them now, and refuse the
    // file if that fails, since their clients are then refused.
    persistence_state(state).sync_for_checkpoint(covered)?;

    if let Some(coordinator) = &state.cluster {
        coordinator.persist_clock(state.store.borrow().now_unix_millis())?;
        coordinator.submit_file_covering(
            job.generation,
            file,
            state.store.borrow().now_unix_millis(),
            covers,
        )?;

        record_submission(state, coordinator, job.generation, covered);
        coordinator.persist_clock(state.store.borrow().now_unix_millis())?;
    } else {
        // The clock goes to disk before the round, so a crash between the two cannot leave
        // a checkpoint whose restart floor is below a token granted before it.
        state
            .checkpoints
            .persist_clock(state.store.borrow().now_unix_millis())?;
        state.checkpoints.commit_round(job.generation, &[file])?;
        state.checkpoint_generation.set(job.generation);
        compact_after_checkpoint(state, covered);
        state
            .checkpoints
            .persist_clock(state.store.borrow().now_unix_millis())?;
    }

    state.last_checkpoint_at.set(Instant::now());
    state.checkpoint_failure_warned.replace(None);
    state.snapshot_request_seen.set(job.requested);
    state
        .checkpoint_ladder
        .set(state.checkpoint_ladder.get().after_job());
    state.bgsave_pending.set(false);
    Ok(())
}

/// Notes a file this shard submitted to cluster round `generation`, and compacts the WAL
/// once a round holding one of its files is published.
fn record_submission(
    state: &ShardState,
    coordinator: &crate::runtime::cluster::ClusterCheckpointCoordinator,
    generation: u64,
    covered: u64,
) {
    state.last_submitted_sequence.set(covered);
    state
        .submitted_coverage
        .borrow_mut()
        .push_back((generation, covered));
    let published = coordinator.published_generation();

    if published > state.compacted_generation.get() {
        // Only what the published round covers may leave the WAL: a newer file of this
        // shard is not recoverable until every shard has submitted that round.
        // Only the file of the published round itself counts: an older entry is either
        // covered by it or belongs to a round that never published (its commit failed), and
        // compacting to that one would drop writes the published round lacks.
        let mut safe = None;
        {
            let mut submitted = state.submitted_coverage.borrow_mut();

            while let Some(&(generation, covered)) = submitted.front()
                && generation <= published
            {
                submitted.pop_front();
                safe = (generation == published).then_some(covered);
            }
        }

        if let Some(safe) = safe {
            compact_after_checkpoint(state, safe);
        }

        state.compacted_generation.set(published);
    }
}

/// A copy of the last published file being made on a helper thread.
pub(super) struct Carry(std::thread::JoinHandle<CarryResult>);

type CarryResult = Result<Option<(u64, u64)>, crate::PersistenceError>;

/// A shard whose log failed takes no new file; it gives an open round (or the final one)
/// a copy of its last published file instead (see `carry_published_file` on the
/// coordinator), so the other shards still publish rounds and compact their logs. The
/// copy can be as large as the shard, so it is made on a helper thread; only the final
/// round's copy at shutdown is made here.
fn carry_published_file(state: &ShardState) {
    let Some(coordinator) = &state.cluster else {
        return;
    };
    let final_round = coordinator.final_round();

    // One copy at a time; at shutdown the one still running is waited for first, since
    // both would write the same file.
    if collect_carry(state, final_round) {
        return;
    }

    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);
    let now = state.store.borrow().now_unix_millis();
    let log_holds_everything = persistence_state(state)
        .holds_every_record()
        .unwrap_or(false);

    if final_round {
        let result = coordinator.carry_published_file(shard_id, now, log_holds_everything);

        carry_finished(state, result);
        return;
    }

    let coordinator = Arc::clone(coordinator);
    let spawned = std::thread::Builder::new()
        .name("ruvio-checkpoint-carry".into())
        .spawn(move || coordinator.carry_published_file(shard_id, now, log_holds_everything));

    match spawned {
        Ok(handle) => *state.carry.borrow_mut() = Some(Carry(handle)),
        Err(error) => carry_finished(state, Err(error.into())),
    }
}

/// Collects a finished copy (waiting for it when `block`). True while one still runs.
pub(super) fn collect_carry(state: &ShardState, block: bool) -> bool {
    let Some(carry) = state.carry.borrow_mut().take() else {
        return false;
    };

    if !block && !carry.0.is_finished() {
        *state.carry.borrow_mut() = Some(carry);
        return true;
    }

    let result = carry.0.join().unwrap_or_else(|_| {
        Err(crate::PersistenceError::Failed(
            "checkpoint copy thread panicked".into(),
        ))
    });

    carry_finished(state, result);
    false
}

/// True while a carried copy is being written (it is collected when it finished).
fn carry_running(state: &ShardState) -> bool {
    collect_carry(state, false)
}

/// A submitted copy is recorded like any other file of this shard (`record_submission`):
/// once its round publishes, that round is what recovery loads, so the log may only be
/// compacted to what the copy covers, never to a newer file of a round that failed to
/// publish. Only a failure is reported, at most once every few minutes while it repeats.
fn carry_finished(state: &ShardState, result: CarryResult) {
    let error = match result {
        Ok(Some((generation, covered))) => {
            if let Some(coordinator) = &state.cluster {
                record_submission(state, coordinator, generation, covered);
            }
            return;
        }
        Ok(None) => return,
        Err(error) => error,
    };

    if let Some(repeats_not_logged) = repeats_to_log(&state.carry_failure_warned, &error) {
        warn!(
            %error,
            repeats_not_logged,
            "could not carry the last published checkpoint into the open round"
        );
    }
}

/// `Some(repeats left out since the last warning)` when `error` is to be logged: a new
/// error, or the same one again after `CHECKPOINT_WARNING_INTERVAL`.
fn repeats_to_log(
    warned: &RefCell<Option<(String, Instant, u64)>>,
    error: &crate::PersistenceError,
) -> Option<u64> {
    let message = error.to_string();
    let mut warned = warned.borrow_mut();

    match warned.as_mut() {
        Some((last, at, repeats))
            if *last == message && at.elapsed() < CHECKPOINT_WARNING_INTERVAL =>
        {
            *repeats += 1;
            None
        }
        Some((last, at, repeats)) if *last == message => {
            *at = Instant::now();
            Some(std::mem::take(repeats))
        }
        _ => {
            *warned = Some((message, Instant::now(), 0));
            Some(0)
        }
    }
}

/// Drops an aborted job. A single shard's round directory is removed (the retry takes a
/// new generation); a cluster round stays open and the shard rejoins it.
fn abandon_job(state: &ShardState, job: CheckpointJob) {
    let generation = job.generation;

    stop_writer(job);
    if state.cluster.is_none()
        && let Err(error) = state.checkpoints.discard_round(generation)
    {
        warn!(%error, generation, "could not remove an aborted checkpoint directory");
    }
}

/// Ends an unfinished job's I/O thread and waits for it, so its partial file is removed
/// before anything else may write or remove the same path. The thread skips the chunks
/// still queued, so this waits only for the chunk it is writing. Never called on a job that
/// is finishing: its thread may already be syncing and renaming the whole file, which can
/// take seconds, so such a job is run to completion instead (see `cover_request`).
fn stop_writer(job: CheckpointJob) {
    let CheckpointJob {
        sender,
        writer,
        cancelled,
        ..
    } = job;

    cancelled.store(true, Ordering::Release);
    drop(sender);
    if let Some(writer) = writer
        && writer.join().is_err()
    {
        warn!("the snapshot I/O thread of an abandoned checkpoint panicked");
    }
}

/// The checkpoint is already published: a log that cannot be trimmed now (a failed WAL, a
/// rotation error) is only kept longer, the checkpoint did not fail.
fn compact_after_checkpoint(state: &ShardState, covered: u64) {
    if let Err(error) = persistence_state(state).compact(covered) {
        warn!(%error, covered, "WAL compaction after a checkpoint failed");
    }
}

/// Ends a checkpoint attempt for snapshot request `requested` that failed. With several
/// shards a `SAVE` waiting on that request fails at once (see [`save_failed`]).
fn fail_job(state: &ShardState, error: &crate::PersistenceError, requested: u64) {
    state.store.borrow_mut().abort_snapshot();
    if let Some(coordinator) = &state.cluster {
        coordinator.fail_requests(requested);
    }
    // A failure is not overload: the next attempt does not start in long blocking slices.
    state
        .checkpoint_ladder
        .set(state.checkpoint_ladder.get().after_job());
    // A real error ends a requested save; only an abort by the journal bound retries it.
    state.bgsave_pending.set(false);
    state
        .mutations_since_checkpoint
        .set(state.mutations_since_checkpoint.get().max(1));

    // A snapshot over the size limit will not shrink by trying again right away.
    let interval = persistence_state(state).snapshot_interval();
    let postpone = if error.to_string().contains("size limit") {
        interval.saturating_mul(9)
    } else {
        Duration::ZERO
    };

    state.last_checkpoint_at.set(Instant::now() + postpone);
    // The same failure again (a log that failed for good fails every attempt) is warned
    // about at most once every few minutes, with the number of repeats left out.
    let Some(suppressed) = repeats_to_log(&state.checkpoint_failure_warned, error) else {
        return;
    };
    let (wal_last_sequence, wal_unsynced_bytes) = persistence_state(state).log_fields();

    warn!(
        %error,
        wal_last_sequence,
        wal_unsynced_bytes,
        repeats_not_logged = suppressed,
        "scheduled or replica-requested checkpoint failed"
    );
}

/// How often a checkpoint failure that keeps repeating is warned about again.
const CHECKPOINT_WARNING_INTERVAL: Duration = Duration::from_secs(300);

/// Starts a checkpoint when one is due or requested. Slices run from `checkpoint_slice`.
pub(super) fn checkpoint_start_if_due(state: &ShardState) {
    if state.checkpoint_job.borrow().is_some() || collect_carry(state, false) {
        return;
    }

    // A round that waits only for its hot partition takes it once it can, with no new
    // shard file (one atomic load unless a round waits).
    if let Some(coordinator) = &state.cluster
        && let Err(error) = coordinator.retry_hot_partition(state.store.borrow().now_unix_millis())
    {
        tracing::debug!(%error, "hot partition retry failed");
    }

    let requested = state.snapshot_requests.load(Ordering::Acquire);
    let catching_up = state.replica_incomplete.get();
    let unanswered = !catching_up && requested > state.snapshot_request_seen.get();
    let saving = !catching_up
        && state
            .cluster
            .as_ref()
            .is_some_and(|coordinator| coordinator.save_waiting());
    let start = requested_start(
        unanswered,
        saving,
        state.last_requested_checkpoint_at.get().elapsed(),
    );
    let forced = start != RequestedStart::No;

    // A round another shard opened needs this shard's file even when it has written
    // nothing, or it never publishes and no shard's WAL is compacted. An idle shard joins
    // at most once a second, unless a `SAVE` waits for the round.
    let round_waiting = !catching_up
        && (saving || state.last_checkpoint_at.get().elapsed() >= Duration::from_secs(1))
        && matches!(
            (&state.cluster, &state.route),
            (Some(coordinator), Some(route))
                if coordinator.round_open() && !coordinator.joined_open_round(route.shard_id as u32)
        );

    if !(forced || state.bgsave_pending.get() || round_waiting || checkpoint_due(state)) {
        return;
    }

    // After an attempt was given up under overload, wait before retrying; only a `SAVE`
    // whose client waits does not.
    let urgent = start == RequestedStart::Now || (saving && round_waiting);

    if !urgent && Instant::now() < state.checkpoint_retry_at.get() {
        return;
    }

    if persistence_state(state).profile() == crate::DurabilityProfile::Memory {
        return;
    }

    // This shard's part of the open round is written; any further checkpoint waits for the
    // round to publish rather than replace it. Replacing would empty the slot again, so
    // requests that keep arriving could hold the round open for good; a request made after
    // the file was taken is answered by the next round instead.
    if joined_open_round(state) {
        return;
    }

    // Recorded only for an attempt that starts: a request held back above (by the open
    // round) does not wait out the throttle once it can start.
    if unanswered {
        state.last_requested_checkpoint_at.set(Instant::now());
    }
    state.mutations_since_checkpoint.set(0);

    match start_job(state, requested) {
        Ok(job) => *state.checkpoint_job.borrow_mut() = Some(job),
        Err(error) => fail_job(state, &error, requested),
    }
}

/// Whether an unanswered snapshot request starts this shard's checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestedStart {
    No,
    /// After the overload back-off, like any due checkpoint.
    AfterBackoff,
    /// At once: a `SAVE` client waits on it.
    Now,
}

/// While a `SAVE` client waits (no round has answered its request and no checkpoint taken
/// for it failed), a request starts at once, past the overload back-off. Another request
/// (`BGSAVE`, a replica's) starts at most once a second and keeps the back-off, and so does
/// a retry once the `SAVE` failed, so a checkpoint that keeps failing or gets given up is
/// not restarted on every tick.
fn requested_start(unanswered: bool, saving: bool, since_attempt: Duration) -> RequestedStart {
    match (unanswered, saving) {
        (false, _) => RequestedStart::No,
        (true, true) => RequestedStart::Now,
        (true, false) if since_attempt >= Duration::from_secs(1) => RequestedStart::AfterBackoff,
        (true, false) => RequestedStart::No,
    }
}

/// Drops the running checkpoint before a replica's full resync replaces the store it walks.
/// A job whose walk is done is dropped too: completing it would publish a file of the old
/// data, with an older snapshot point, over the installed copy.
pub(super) fn abandon_checkpoint_for_install(state: &ShardState) {
    let Some(job) = state.checkpoint_job.borrow_mut().take() else {
        return;
    };

    state.store.borrow_mut().cancel_snapshot();
    state
        .mutations_since_checkpoint
        .set(state.mutations_since_checkpoint.get().max(1));
    abandon_job(state, job);
}

pub(super) fn checkpoint_running(state: &ShardState) -> bool {
    state.checkpoint_job.borrow().is_some()
}

/// One bounded slice of the running checkpoint.
pub(super) fn checkpoint_slice(state: &ShardState) {
    let Some(mut job) = state.checkpoint_job.borrow_mut().take() else {
        return;
    };

    let ladder = state.checkpoint_ladder.get();
    let micros = ladder.micros;
    let mut slice = Duration::from_micros(micros);

    if job.journal_bound > 0
        && state
            .store
            .borrow()
            .snapshot_journal_used()
            .saturating_mul(4)
            >= job.journal_bound.saturating_mul(3)
    {
        slice = slice.saturating_mul(2);
    }

    // An overload slice (see `after_abort`) also waits for the writer while walking instead
    // of yielding to clients when it is behind; it never outlasts its ceiling, and the
    // finished file is collected without waiting. A fallback slice has no ceiling.
    let level = ladder.level;
    let overload = micros > MAX_SLICE_MICROS;
    let slice = if overload && level < FALLBACK_LEVEL {
        slice.min(Duration::from_micros(MAX_OVERLOAD_SLICE_MICROS))
    } else {
        slice
    };
    let deadline = Instant::now().checked_add(slice);
    let blocking = overload && !job.finishing;

    match advance(state, &mut job, deadline, blocking) {
        Progress::Running => *state.checkpoint_job.borrow_mut() = Some(job),
        Progress::Aborted => {
            // The next attempt walks with a longer, still bounded, slice; once slices are
            // at their longest, it gets a larger journal instead, and once the journal is at
            // its memory cap, longer slices again.
            let next = next_attempt(micros, ladder.shift, job.journal_capped, level);
            state
                .mutations_since_checkpoint
                .set(state.mutations_since_checkpoint.get().max(1));
            state.last_checkpoint_at.set(Instant::now());
            abandon_job(state, job);
            match next {
                Some((micros, shift)) => {
                    state.checkpoint_ladder.set(SliceLadder {
                        micros,
                        shift,
                        level,
                    });
                    checkpoint_start_if_due(state);
                }
                None => give_up_overloaded_checkpoint(state),
            }
        }
        Progress::Finished(result) => {
            match result.and_then(|file| complete_job(state, &job, file, job.requested)) {
                Ok(()) => {}
                Err(error) => fail_job(state, &error, job.requested),
            }
        }
    }
}

/// True when the WAL holds every write made so far durably, so a running job with an earlier
/// snapshot point plus the log still recovers them. Syncs the log to make sure; `SAVE` and
/// shutdown only, never per request.
fn log_replays_later_writes(state: &ShardState) -> bool {
    let mut wal = persistence_state(state);

    wal.profile().uses_wal() && wal.failure().is_none() && wal.flush().is_ok()
}

/// Makes a running job count for snapshot request `requested`. It is kept when the log
/// replays what was written since it began; otherwise it is replaced by a job started now.
/// A job whose walk is done is completed first rather than abandoned: its I/O thread may
/// be syncing the whole file, and stopping it would hold the shard for that sync anyway.
fn cover_request(
    state: &ShardState,
    mut job: CheckpointJob,
    requested: u64,
) -> Result<CheckpointJob, crate::PersistenceError> {
    if log_replays_later_writes(state) {
        job.requested = requested;

        return Ok(job);
    }

    if job.finishing {
        // The file is all but written: submit it (it is still a valid checkpoint, just an
        // older one), then take the one this request needs.
        let earlier = job.requested;

        if let Err(error) = run_to_completion(state, job, earlier) {
            warn!(%error, "an earlier checkpoint failed while a newer one was requested");
        }
    } else {
        state.store.borrow_mut().cancel_snapshot();
        abandon_job(state, job);
    }
    state.mutations_since_checkpoint.set(0);
    start_job(state, requested)
}

/// Drives `job` to its end on the shard thread and records it as answering `covers`.
fn run_to_completion(
    state: &ShardState,
    mut job: CheckpointJob,
    covers: u64,
) -> Result<(), crate::PersistenceError> {
    loop {
        match advance(state, &mut job, None, true) {
            Progress::Running => {}
            Progress::Aborted => {
                let requested = job.requested;

                abandon_job(state, job);
                job = start_job(state, requested)?;
            }
            Progress::Finished(result) => {
                let covers = covers.max(job.requested);

                return match result.and_then(|file| complete_job(state, &job, file, covers)) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        fail_job(state, &error, job.requested);
                        Err(error)
                    }
                };
            }
        }
    }
}

/// Where the next checkpoint attempt stands: its slice length, how often its journal bound
/// doubles, and its overload level (see [`after_abort`] and [`after_give_up`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SliceLadder {
    micros: u64,
    shift: u32,
    level: u32,
}

impl SliceLadder {
    pub(super) const START: Self = Self {
        micros: FIRST_SLICE_MICROS,
        shift: 0,
        level: 0,
    };

    /// After a checkpoint completed or failed: only aborts by the journal climb the ladder.
    fn after_job(self) -> Self {
        Self::START
    }
}

/// The next attempt walks with a longer slice, up to [`MAX_SLICE_MICROS`].
fn slice_after_abort(micros: u64) -> u64 {
    micros.saturating_mul(2).min(MAX_SLICE_MICROS)
}

/// The journal grows after aborts to at most this share of the shard's data, so a
/// checkpoint under heavy writes adds a bounded fraction to RSS rather than up to a second
/// copy of every value. Each overload level halves it (see [`OVERLOAD_LEVELS`]).
const JOURNAL_DATA_DIVISOR: usize = 4;

/// The most the journal may grow to: the configured bound or a share of the shard's key,
/// value and collection bytes, whichever is larger. The share is a quarter, doubled per
/// overload `level` up to all of the data; past the first level it also stays within
/// `headroom`, the memory left under the shard's limit when one is set.
fn journal_cap(
    configured: usize,
    usage: crate::memory::MemoryUsage,
    level: u32,
    headroom: Option<usize>,
) -> usize {
    let data = usage
        .key_bytes
        .saturating_add(usage.value_bytes)
        .saturating_add(usage.collection_bytes);
    let share = data / (JOURNAL_DATA_DIVISOR >> level.min(TOP_JOURNAL_LEVEL));
    let share = if level == 0 {
        share
    } else {
        share.min(headroom.unwrap_or(usize::MAX))
    };

    configured.max(share)
}

/// The configured bound doubled `shift` times, never past `cap`.
fn journal_bound(configured: usize, shift: u32, cap: usize) -> usize {
    configured
        .checked_shl(shift)
        .filter(|bound| bound >> shift == configured)
        .unwrap_or(usize::MAX)
        .min(cap)
        .max(configured)
}

/// The next attempt's slice length and journal shift after an abort, or `None` to give the
/// checkpoint up for now. Slices grow first, up to [`MAX_SLICE_MICROS`]; then the journal
/// doubles, up to its cap. A walk that overflows even the capped journal (writes touch more
/// than a quarter of the data during one walk) gets longer slices that wait for the writer,
/// up to [`MAX_OVERLOAD_SLICE_MICROS`]: in a slice that outlasts the rest of the walk no
/// write lands, so a shard whose walk fits finishes. Past that the attempt is given up.
fn after_abort(micros: u64, shift: u32, journal_capped: bool) -> Option<(u64, u32)> {
    if micros < MAX_SLICE_MICROS {
        Some((slice_after_abort(micros), shift))
    } else if !journal_capped {
        Some((micros, shift.saturating_add(1)))
    } else if micros < MAX_OVERLOAD_SLICE_MICROS {
        Some((
            micros.saturating_mul(2).min(MAX_OVERLOAD_SLICE_MICROS),
            shift,
        ))
    } else {
        None
    }
}

/// [`after_abort`] at overload `level`. In the fallback the attempt is never given up:
/// its slices double instead, past every ceiling, until one outlasts the walk.
fn next_attempt(micros: u64, shift: u32, journal_capped: bool, level: u32) -> Option<(u64, u32)> {
    if level >= FALLBACK_LEVEL {
        return Some((micros.saturating_mul(2), shift));
    }

    after_abort(micros, shift, journal_capped)
}

/// The next attempt after a give-up at overload `level`: `(slice micros, journal shift,
/// level)`. It starts where the last attempt stopped, with the longest overload slices and
/// the journal at its cap, and the cap one level higher, so attempts do not climb the whole
/// ladder again. Past the top journal level come the fallback's long slices.
fn after_give_up(level: u32) -> (u64, u32, u32) {
    let level = level.saturating_add(1).min(FALLBACK_LEVEL);
    let micros = if level >= FALLBACK_LEVEL {
        FALLBACK_SLICE_MICROS
    } else {
        MAX_OVERLOAD_SLICE_MICROS
    };

    (micros, u32::MAX, level)
}

/// Writes outpace even the longest slices with the capped journal: drop this attempt
/// (its round directory is already discarded), count and log it, and retry after
/// [`OVERLOAD_RETRY`] with a larger journal instead of stalling the shard.
fn give_up_overloaded_checkpoint(state: &ShardState) {
    let skips = state.checkpoint_overload_skips.get().saturating_add(1);
    let (micros, shift, level) = after_give_up(state.checkpoint_ladder.get().level);

    state.checkpoint_overload_skips.set(skips);
    state.checkpoint_ladder.set(SliceLadder {
        micros,
        shift,
        level,
    });
    state
        .checkpoint_retry_at
        .set(Instant::now() + OVERLOAD_RETRY);
    if level >= FALLBACK_LEVEL {
        tracing::error!(
            skips,
            retry_in_secs = OVERLOAD_RETRY.as_secs(),
            slice_ms = micros / 1000,
            "checkpoint given up at the largest journal memory allows: the retry walks in \
             long blocking slices, stalling this shard's clients until it completes"
        );
    } else {
        warn!(
            skips,
            journal_level = level,
            retry_in_secs = OVERLOAD_RETRY.as_secs(),
            "checkpoint given up: writes outpace the walk even with the longest slices and \
             the capped journal; the WAL keeps every write meanwhile and the retry gets a \
             larger journal"
        );
    }
}

/// Writes this shard's final checkpoint to completion before returning: shutdown. With
/// several shards the round then completes only with every shard's final file.
pub(super) fn publish_checkpoint(state: &ShardState) -> Result<(), crate::PersistenceError> {
    // A copy still being written would submit after the final file and empty its slot.
    collect_carry(state, true);
    if let Some(coordinator) = &state.cluster {
        coordinator.require_final_round();
    }

    publish_checkpoint_round(state, u64::MAX)
}

/// `SAVE`. A single shard writes its checkpoint to completion. With several shards it
/// raises a snapshot request, which every shard answers with a file taken after it, and
/// returns that request: the save is durable once a round answering it is published (see
/// [`save_published`]). This shard starts its file at once when it can; a round already
/// holding files taken before the request publishes as usual, and the next one answers it.
/// No log is synced here: an older file plus the log does not count for a request.
/// The returned guard marks the client as waiting until it is dropped.
pub(super) fn save_every_shard(
    state: &ShardState,
) -> Result<(u64, Option<crate::runtime::cluster::SaveWait>), crate::PersistenceError> {
    if state.replica_incomplete.get() {
        return Err(crate::PersistenceError::Failed(
            "replica catch-up incomplete".into(),
        ));
    }

    let Some(coordinator) = &state.cluster else {
        return publish_checkpoint_round(state, 0).map(|()| (0, None));
    };

    if persistence_state(state).profile() == crate::DurabilityProfile::Memory {
        return Ok((0, None));
    }

    let requested = request_every_shard(state);
    let waiting = coordinator.begin_save(requested);

    // A carried copy still being written holds the start back; the request stays raised
    // and the shard starts it once the copy is in (see `checkpoint_start_if_due`).
    if state.checkpoint_job.borrow().is_none() && !joined_open_round(state) && !carry_running(state)
    {
        state.last_requested_checkpoint_at.set(Instant::now());
        state.mutations_since_checkpoint.set(0);
        match start_job(state, requested) {
            Ok(job) => *state.checkpoint_job.borrow_mut() = Some(job),
            // The request stays raised: the shard retries it like any requested checkpoint.
            Err(error) => fail_job(state, &error, requested),
        }
    }

    Ok((requested, Some(waiting)))
}

/// True once a published round answers snapshot request `requested` (from
/// [`save_every_shard`]); always true with a single shard, whose checkpoint is published
/// when written.
pub(super) fn save_published(state: &ShardState, requested: u64) -> bool {
    state
        .cluster
        .as_ref()
        .is_none_or(|coordinator| coordinator.published_requests() >= requested)
}

/// True once a checkpoint taken for snapshot request `requested` (or a later one) failed
/// before a round answering it was published: the `SAVE` that made the request fails like a
/// single shard's, instead of waiting for the retry. Always false with a single shard.
pub(super) fn save_failed(state: &ShardState, requested: u64) -> bool {
    state.cluster.as_ref().is_some_and(|coordinator| {
        coordinator.failed_requests() >= requested && coordinator.published_requests() < requested
    })
}

/// True while the open cluster round already holds this shard's file.
fn joined_open_round(state: &ShardState) -> bool {
    matches!(
        (&state.cluster, &state.route),
        (Some(coordinator), Some(route)) if coordinator.joined_open_round(route.shard_id as u32)
    )
}

/// Writes this shard's checkpoint to completion (nothing without persistence): a single
/// shard's `SAVE`, or a shard's final checkpoint at shutdown. The file holds every write
/// made before the call; `covers` is the snapshot request it answers (`u64::MAX`: final).
fn publish_checkpoint_round(
    state: &ShardState,
    covers: u64,
) -> Result<(), crate::PersistenceError> {
    if state.replica_incomplete.get() {
        return Err(crate::PersistenceError::Failed(
            "replica catch-up incomplete".into(),
        ));
    }
    if persistence_state(state).profile() == crate::DurabilityProfile::Memory {
        return Ok(());
    }

    let requested = state.snapshot_requests.load(Ordering::Acquire);
    let running = state.checkpoint_job.borrow_mut().take();

    // A running job holds an earlier snapshot point: it is finished only when the log
    // replays the writes made since (see `cover_request`).
    let job = match running {
        Some(job) => cover_request(state, job, requested)?,
        None => start_job(state, requested)?,
    };
    state.mutations_since_checkpoint.set(0);
    run_to_completion(state, job, covers)
}

/// Why a background save could not start.
pub(super) enum BackgroundSave {
    Started,
    AlreadyRunning,
}

/// `BGSAVE`: starts a checkpoint that the shard advances in slices between requests, so
/// the caller is answered at once instead of waiting for the whole walk.
pub(super) fn start_background_checkpoint(
    state: &ShardState,
) -> Result<BackgroundSave, crate::PersistenceError> {
    if state.replica_incomplete.get() {
        return Err(crate::PersistenceError::Failed(
            "replica catch-up incomplete".into(),
        ));
    }

    if persistence_state(state).profile() == crate::DurabilityProfile::Memory {
        return Ok(BackgroundSave::Started);
    }

    // A carried copy being written is this shard's part of the round under way.
    if state.checkpoint_job.borrow().is_some() || joined_open_round(state) || carry_running(state) {
        return Ok(BackgroundSave::AlreadyRunning);
    }

    // A round already open is the background save under way: this shard joins it without
    // a new request, which would only queue another round after it.
    let round_open = state
        .cluster
        .as_ref()
        .is_some_and(|coordinator| coordinator.round_open());
    let requested = if round_open {
        state.snapshot_requests.load(Ordering::Acquire)
    } else {
        request_every_shard(state)
    };
    let job = start_job(state, requested)?;

    state.mutations_since_checkpoint.set(0);
    *state.checkpoint_job.borrow_mut() = Some(job);
    // Stays set until a checkpoint completes: an abort by the journal bound starts it again.
    state.bgsave_pending.set(true);

    Ok(BackgroundSave::Started)
}

/// With several shards a round is published only once every shard submitted its file, so a
/// requested save asks every shard to checkpoint. Returns the request number this shard covers.
fn request_every_shard(state: &ShardState) -> u64 {
    if state.cluster.is_some() {
        state.snapshot_requests.fetch_add(1, Ordering::AcqRel) + 1
    } else {
        state.snapshot_requests.load(Ordering::Acquire)
    }
}

/// `rdb_bgsave_in_progress`: this shard is writing or owes a requested checkpoint, or the
/// cluster round it belongs to is not published yet.
pub(super) fn background_save_in_progress(state: &ShardState) -> bool {
    state.bgsave_pending.get()
        || checkpoint_running(state)
        || state
            .cluster
            .as_ref()
            .is_some_and(|coordinator| coordinator.round_open())
}

#[cfg(test)]
mod slice_tests {
    use super::*;

    #[test]
    fn aborts_at_the_longest_slice_grow_the_journal_until_the_walk_can_finish() {
        const CONFIGURED: usize = 64 * 1024;
        let usage = crate::memory::MemoryUsage {
            key_bytes: 8 << 20,
            value_bytes: 56 << 20,
            ..Default::default()
        };
        let cap = journal_cap(CONFIGURED, usage, 0, None);
        let mut micros = FIRST_SLICE_MICROS;
        let mut shift = 0;
        let bound = |shift| journal_bound(CONFIGURED, shift, cap);
        let next = |micros, shift| after_abort(micros, shift, bound(shift) >= cap).unwrap();

        // 500 µs doubles to 16 ms in five aborts.
        for _ in 0..5 {
            (micros, shift) = next(micros, shift);
        }
        // Slices grew first; the journal stays as configured until they are at their longest.
        assert_eq!((micros, shift), (MAX_SLICE_MICROS, 0));
        assert_eq!(bound(shift), CONFIGURED);
        for expected in 1..=3 {
            (micros, shift) = next(micros, shift);
            assert_eq!((micros, shift), (MAX_SLICE_MICROS, expected));
            assert_eq!(bound(shift), CONFIGURED << expected);
        }
        // The journal stops at a quarter of the data (16 MiB of 64 MiB) after 8 doublings.
        while bound(shift) < cap {
            (micros, shift) = next(micros, shift);
        }
        assert_eq!(cap, 16 << 20);
        assert_eq!((micros, shift, bound(shift)), (MAX_SLICE_MICROS, 8, cap));
        // A capped journal that still overflows gets longer slices, up to their ceiling.
        (micros, shift) = next(micros, shift);
        assert_eq!((micros, shift), (2 * MAX_SLICE_MICROS, 8));
        while micros < MAX_OVERLOAD_SLICE_MICROS {
            (micros, shift) = next(micros, shift);
        }
        assert_eq!((micros, shift), (MAX_OVERLOAD_SLICE_MICROS, 8));
        // Then the attempt is given up rather than stalling the shard any longer.
        assert_eq!(after_abort(micros, shift, true), None);
    }

    #[test]
    fn sustained_overload_ends_in_a_finished_checkpoint_with_bounded_memory() {
        const CONFIGURED: usize = 64 * 1024;
        let usage = crate::memory::MemoryUsage {
            key_bytes: 8 << 20,
            value_bytes: 56 << 20,
            ..Default::default()
        };
        let data = 64usize << 20;
        // A walk that takes 3 s without writes in between.
        let walk_micros = 3_000_000;

        // A walk finishes once its journal holds every capture its writes make (here up to
        // more than the whole data: small values carry per-copy overhead), or once one
        // slice outlasts the walk. Memory left under the shard's limit bounds the journal.
        for headroom in [None, Some(data / 3)] {
            let top = headroom.map_or(data, |headroom| data.min(headroom));

            for needed in [data / 3, data, 3 * data] {
                let (mut micros, mut shift, mut level) = (FIRST_SLICE_MICROS, 0, 0);
                let mut give_ups = 0;
                let mut attempts = 0;

                loop {
                    attempts += 1;
                    assert!(
                        attempts < 100,
                        "no attempt finished for a walk needing {needed}"
                    );
                    let cap = journal_cap(CONFIGURED, usage, level, headroom);
                    let bound = journal_bound(CONFIGURED, shift, cap);

                    assert!(bound <= top, "journal {bound} past {top}");
                    if level < FALLBACK_LEVEL {
                        assert!(micros <= MAX_OVERLOAD_SLICE_MICROS);
                    }
                    if bound >= needed || micros >= walk_micros {
                        break;
                    }
                    match next_attempt(micros, shift, bound >= cap, level) {
                        Some(next) => (micros, shift) = next,
                        None => {
                            give_ups += 1;
                            (micros, shift, level) = after_give_up(level);
                        }
                    }
                }
                // Retries resume at the longest slices: one attempt per raised level.
                assert!(give_ups <= FALLBACK_LEVEL, "{give_ups} give-ups");
                // Only a walk the largest allowed journal cannot hold falls back.
                assert_eq!(
                    level == FALLBACK_LEVEL,
                    needed > top,
                    "{needed} {headroom:?}"
                );
            }
        }
    }

    #[test]
    fn a_failed_checkpoint_does_not_leave_the_next_one_in_the_fallback() {
        let mut ladder = SliceLadder::START;

        for _ in 0..FALLBACK_LEVEL {
            let (micros, shift, level) = after_give_up(ladder.level);
            ladder = SliceLadder {
                micros,
                shift,
                level,
            };
        }
        assert_eq!(ladder.level, FALLBACK_LEVEL);
        // A failure other than an abort (a snapshot over its size limit) ends the attempt.
        assert_eq!(ladder.after_job(), SliceLadder::START);
    }

    #[test]
    fn journal_bound_never_exceeds_its_cap_or_drops_below_the_configured_size() {
        let small = crate::memory::MemoryUsage {
            value_bytes: 1024,
            ..Default::default()
        };

        // Little data: the configured bound is the cap and never grows.
        assert_eq!(journal_cap(64 * 1024, small, 0, None), 64 * 1024);
        for shift in [0, 1, 16, 63, 64, u32::MAX] {
            assert_eq!(journal_bound(64 * 1024, shift, 64 * 1024), 64 * 1024);
            assert!(journal_bound(64 * 1024, shift, usize::MAX) >= 64 * 1024);
        }
        assert_eq!(journal_bound(3, 70, usize::MAX), usize::MAX);
    }

    #[test]
    fn repeated_aborts_never_grow_a_slice_past_its_bound() {
        let mut micros = FIRST_SLICE_MICROS;

        for _ in 0..64 {
            micros = slice_after_abort(micros);
            assert!(micros <= MAX_SLICE_MICROS);
        }
        assert_eq!(micros, MAX_SLICE_MICROS);
    }

    #[test]
    fn only_a_save_request_skips_the_throttle_and_the_overload_back_off() {
        let just_now = Duration::from_millis(10);
        let a_while_ago = Duration::from_secs(2);

        assert_eq!(
            requested_start(false, false, a_while_ago),
            RequestedStart::No
        );
        assert_eq!(requested_start(true, true, just_now), RequestedStart::Now);
        // A `BGSAVE` or replica request, or a retry of a failed `SAVE`, waits out a second
        // since the last attempt and then the back-off.
        assert_eq!(requested_start(true, false, just_now), RequestedStart::No);
        assert_eq!(
            requested_start(true, false, a_while_ago),
            RequestedStart::AfterBackoff
        );
    }
}
