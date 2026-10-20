//! `RUVIO MOVESLOTS`: moves a slot range between the shards of this process (ADR 0014).
//!
//! The source shard copies the range to the target in bounded steps while it keeps serving
//! it, then sends the keys written meanwhile again until few are left. The hand-over is one
//! turn of the source's thread: it sends the last changes and a flip message, then waits
//! (bounded) while the target applies them, records the move and points the slot table at
//! itself. Nothing on the source runs in between, so no write can land behind the copy.
//! Commands that were routed to the source before the flip and run there later are checked
//! where they run (see [`SlotMover::moved`]). The source then drops its copy of the range in
//! bounded steps, without logging it: recovery routes by the `SLOTS` file.

use super::*;
use crate::protocol::SlotRangeMove;
use crate::routing::hash_slot;
use crate::slot_moves::SlotMoves;
use std::sync::Condvar;
use std::sync::atomic::AtomicBool;

/// Keys per copy message, and the bytes after which a message is sent sooner: with
/// `IN_FLIGHT` that bounds what a move holds in transit at about a megabyte.
const COPY_BATCH_KEYS: u32 = 1024;
const COPY_BATCH_BYTES: usize = 256 * 1024;
/// Index positions one walk step visits.
const WALK_STEP: usize = 1024;
/// Copy messages the target may have queued before the source waits.
const IN_FLIGHT: u64 = 4;
/// Catch-up rounds before the hand-over goes ahead whatever is still changing.
const CATCH_UP_ROUNDS: usize = 32;
/// Changed keys few enough to send in the hand-over turn itself.
const HAND_OVER_KEYS: usize = 512;
/// The longest the source waits for the target in the hand-over; past it the move is
/// cancelled.
const HAND_OVER_LIMIT: Duration = Duration::from_secs(1);
/// Keys one clean-up step removes.
const CLEAN_UP_STEP: usize = 1024;

pub(super) const UNBLOCKED_ERROR: &[u8] =
    b"UNBLOCKED the key's slot moved to another shard while the client was blocked";
const UNBLOCKED_REPLY: &[u8] =
    b"-UNBLOCKED the key's slot moved to another shard while the client was blocked\r\n";
pub(super) const TRYAGAIN_REPLY: &[u8] =
    b"-TRYAGAIN the key's slot moved to another shard; send the command again\r\n";

/// Moves of this process: the one running, the last one, and the durable history.
#[derive(Default)]
pub(super) struct SlotMover {
    inner: Mutex<MoverState>,
    /// Set by the first hand-over: from then on a request routed before a hand-over may
    /// reach a shard that no longer owns its keys, so shards check forwarded requests and
    /// commands that waited. Never set in a process that moved nothing.
    moved: AtomicBool,
}

#[derive(Default)]
struct MoverState {
    /// The `SLOTS` history, and the directory it is saved in (`None` without a log: a
    /// multi-node node then keeps it in memory only). `None` without durability and map.
    durable: Option<(SlotMoves, Option<std::path::PathBuf>)>,
    current: Option<Arc<MoveSession>>,
    last: Option<Report>,
}

impl SlotMover {
    /// Keeps the history loaded at start, so moves are recorded in `data_dir`.
    pub(super) fn set_durable(&self, history: SlotMoves, data_dir: Option<std::path::PathBuf>) {
        self.lock().durable = Some((history, data_dir));
    }

    /// Runs `step` on the durable history and its directory; `None` when there is none.
    pub(super) fn with_history<T>(
        &self,
        step: impl FnOnce(&mut SlotMoves, Option<&std::path::Path>) -> T,
    ) -> Option<T> {
        let mut state = self.lock();

        state
            .durable
            .as_mut()
            .map(|(history, data_dir)| step(history, data_dir.as_deref()))
    }

    /// Runs `step` unless a move between this process's shards is running, holding the
    /// mover's lock so none starts meanwhile. Locks are taken mover first, then the moves
    /// between nodes (as `RUVIO MOVESLOTS` does), never the other way round.
    pub(super) fn unless_moving<T>(&self, step: impl FnOnce() -> T) -> Option<T> {
        let state = self.lock();

        state.current.is_none().then(step)
    }

    /// From now on shards check forwarded requests and commands that waited.
    pub(super) fn mark_moved(&self) {
        self.moved.store(true, Ordering::Release);
    }

    /// Whether any hand-over happened in this process.
    #[inline]
    pub(super) fn moved(&self) -> bool {
        self.moved.load(Ordering::Acquire)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MoverState> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// One move, shared by its source and target shards.
pub(super) struct MoveSession {
    first: u16,
    last: u16,
    from: usize,
    to: usize,
    started: Instant,
    /// Copy messages sent and applied (the opening message counts as one).
    sent: AtomicU64,
    applied: AtomicU64,
    keys: AtomicU64,
    rounds: AtomicU64,
    /// The source's first log sequence after the hand-over (0 without a log).
    until: AtomicU64,
    /// The target's last log sequence at the hand-over: where a change stream continues.
    position: AtomicU64,
    progress: tokio::sync::Notify,
    outcome: Mutex<Outcome>,
    handed_over: Condvar,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Copying,
    Failed(String),
    /// The target owns the range; the source is dropping its copy.
    HandedOver {
        freeze: Duration,
    },
}

/// What `RUVIO MOVESLOTS STATUS` reports about a finished move.
#[derive(Clone, Debug)]
struct Report {
    first: u16,
    last: u16,
    from: usize,
    to: usize,
    state: &'static str,
    keys: u64,
    rounds: u64,
    freeze_ms: Option<f64>,
    elapsed_ms: f64,
    error: Option<String>,
}

impl MoveSession {
    fn outcome(&self) -> Outcome {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Records a failure unless the move already ended; true when this call ended it.
    fn fail(&self, reason: impl Into<String>) -> bool {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if *outcome != Outcome::Copying {
            return false;
        }

        *outcome = Outcome::Failed(reason.into());
        drop(outcome);
        self.handed_over.notify_all();
        self.progress.notify_one();
        true
    }

    fn report(&self, state: &'static str, error: Option<String>) -> Report {
        let freeze_ms = match self.outcome() {
            Outcome::HandedOver { freeze } => Some(freeze.as_secs_f64() * 1000.0),
            _ => None,
        };

        Report {
            first: self.first,
            last: self.last,
            from: self.from,
            to: self.to,
            state,
            keys: self.keys.load(Ordering::Acquire),
            rounds: self.rounds.load(Ordering::Acquire),
            freeze_ms,
            elapsed_ms: self.started.elapsed().as_secs_f64() * 1000.0,
            error,
        }
    }
}

/// Keys as the source holds them, in one buffer: per key its length and bytes, the number
/// of frames, then one checkpoint entry frame per database that holds it (written straight
/// from the live value). No frames removes the key. The target logs the frames as they are.
#[derive(Default)]
pub(super) struct CopyBatch {
    bytes: Vec<u8>,
    keys: u32,
}

/// A key of a [`CopyBatch`] and its checkpoint entry frames.
type KeyFrames<'a> = (&'a [u8], Vec<&'a [u8]>);

impl CopyBatch {
    /// A batch as another node sent it; [`CopyBatch::entries`] checks it.
    pub(super) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes, keys: 0 }
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn keys(&self) -> u32 {
        self.keys
    }

    pub(super) fn push(
        &mut self,
        store: &KvShard,
        key: &[u8],
    ) -> Result<(), crate::PersistenceError> {
        let start = self.bytes.len();

        self.bytes
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.bytes.extend_from_slice(key);
        let count_at = self.bytes.len();

        self.bytes.extend_from_slice(&0u32.to_le_bytes());
        match store.encode_key_frames(key, &mut self.bytes) {
            Ok(frames) => {
                self.bytes[count_at..count_at + 4].copy_from_slice(&frames.to_le_bytes());
                self.keys += 1;
                Ok(())
            }
            Err(error) => {
                self.bytes.truncate(start);
                Err(error)
            }
        }
    }

    pub(super) fn full(&self) -> bool {
        self.keys >= COPY_BATCH_KEYS || self.bytes.len() >= COPY_BATCH_BYTES
    }

    /// Each key with its frames, in order; `None` for a buffer that does not parse.
    fn entries(&self) -> Option<Vec<KeyFrames<'_>>> {
        let mut at = 0;
        let mut out = Vec::with_capacity(self.keys as usize);
        let u32_at = |bytes: &[u8], at: usize| {
            bytes
                .get(at..at + 4)
                .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
        };

        while at < self.bytes.len() {
            let length = u32_at(&self.bytes, at)?;
            let key = self.bytes.get(at + 4..at + 4 + length)?;

            at += 4 + length;
            let count = u32_at(&self.bytes, at)?;

            at += 4;
            let mut frames = Vec::with_capacity(count);

            for _ in 0..count {
                let frame_length = u32_at(&self.bytes, at)?;

                frames.push(self.bytes.get(at..at + frame_length)?);
                at += frame_length;
            }
            out.push((key, frames));
        }

        Some(out)
    }
}

pub(super) enum MoveMessage {
    /// The target records that a copy begins.
    Open(Arc<MoveSession>),
    Copy(Arc<MoveSession>, CopyBatch),
    /// The last copy is in; the target takes the range.
    Flip(Arc<MoveSession>),
    /// The move failed: the target drops what it copied.
    Cancel(Arc<MoveSession>),
    /// Starts a move on its source shard.
    Start(Arc<MoveSession>),
    /// Starts copying a range to another node on the shard that holds it.
    Export(Arc<super::migrate::Export>),
    /// Drops a cancelled range arriving from another node on its target shard.
    CancelImport(Arc<super::migrate::Import>),
    /// A newer cluster map was installed: settle this shard's ranges with it.
    Settle,
    /// Drops the keys of a range this shard no longer holds, in bounded steps.
    Forget {
        first: u16,
        last: u16,
    },
}

/// `RUVIO MOVESLOTS ...` on any shard of a multi-shard process.
pub(super) fn command(state: &Rc<ShardState>, target: Option<SlotRangeMove>) -> Response<'static> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return match target {
            Some(_) => Response::Error(b"ERR RUVIO MOVESLOTS needs more than one shard"),
            None => status(&SlotMover::default()),
        };
    };
    let fabric = &forwarding.fabric;

    let Some(target) = target else {
        return status(&fabric.mover);
    };

    match start(state, forwarding, target) {
        Ok(()) => Response::Simple(b"OK"),
        Err(error) => Response::Error(error),
    }
}

fn start(
    state: &Rc<ShardState>,
    forwarding: &Forwarding,
    target: SlotRangeMove,
) -> Result<(), &'static [u8]> {
    let fabric = &forwarding.fabric;
    let to = usize::from(target.shard);

    if to >= fabric.shard_count() {
        return Err(b"ERR RUVIO MOVESLOTS no such shard");
    }
    if state.role.get() != crate::config::ReplicationRole::Off {
        return Err(b"ERR RUVIO MOVESLOTS is not supported while replication is on");
    }
    // Without a log a restart would load the last checkpoint with the formula's owners.
    if state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .profile()
        == crate::DurabilityProfile::Snapshot
    {
        return Err(b"ERR RUVIO MOVESLOTS needs durability memory, everysec or always");
    }

    let from = fabric.slot_owner(target.first);

    if (target.first..=target.last).any(|slot| fabric.slot_owner(slot) != from) {
        return Err(b"ERR RUVIO MOVESLOTS range has more than one owner; move each part");
    }
    if from >= fabric.shard_count() {
        return Err(b"ERR RUVIO MOVESLOTS range belongs to another node of the cluster");
    }
    if from == to {
        return Err(b"ERR RUVIO MOVESLOTS range is already on that shard");
    }

    let session = Arc::new(MoveSession {
        first: target.first,
        last: target.last,
        from,
        to,
        started: Instant::now(),
        sent: AtomicU64::new(0),
        applied: AtomicU64::new(0),
        keys: AtomicU64::new(0),
        rounds: AtomicU64::new(0),
        until: AtomicU64::new(0),
        position: AtomicU64::new(0),
        progress: tokio::sync::Notify::new(),
        outcome: Mutex::new(Outcome::Copying),
        handed_over: Condvar::new(),
    });

    {
        let mut mover = fabric.mover.lock();

        if mover.current.is_some() || fabric.migrations.busy() {
            return Err(b"ERR RUVIO MOVESLOTS another move is in progress");
        }
        mover.current = Some(Arc::clone(&session));
    }

    if from == forwarding.me {
        tokio::task::spawn_local(run_source(Rc::clone(state), session));
    } else {
        fabric.send(
            forwarding.me,
            from,
            Message::Move(MoveMessage::Start(session)),
        );
    }

    Ok(())
}

fn status(mover: &SlotMover) -> Response<'static> {
    // The mover lock is let go before a session's own lock is taken: the hand-over takes
    // them the other way round.
    let (current, last) = {
        let state = mover.lock();

        (state.current.clone(), state.last.clone())
    };
    let report = match (current, last) {
        (Some(session), _) => Some(match session.outcome() {
            Outcome::Copying => session.report("copying", None),
            Outcome::HandedOver { .. } => session.report("cleaning", None),
            Outcome::Failed(reason) => session.report("cancelling", Some(reason)),
        }),
        (None, Some(last)) => Some(last),
        (None, None) => None,
    };
    let Some(report) = report else {
        return Response::Map(vec![(
            std::borrow::Cow::Borrowed(b"state".as_slice()),
            Response::Bulk(Some(std::borrow::Cow::Borrowed(b"none"))),
        )]);
    };
    // Every value is a string, as `CONFIG GET` answers, so a client reads one map type.
    let mut pairs = vec![
        ("state", report.state.to_string()),
        ("first", report.first.to_string()),
        ("last", report.last.to_string()),
        ("from", report.from.to_string()),
        ("to", report.to.to_string()),
        ("keys", report.keys.to_string()),
        ("rounds", report.rounds.to_string()),
        ("elapsed_ms", format!("{:.3}", report.elapsed_ms)),
    ];

    if let Some(freeze) = report.freeze_ms {
        pairs.push(("freeze_ms", format!("{freeze:.3}")));
    }
    if let Some(error) = report.error {
        pairs.push(("error", error));
    }

    Response::Map(
        pairs
            .into_iter()
            .map(|(name, value)| {
                (
                    std::borrow::Cow::Borrowed(name.as_bytes()),
                    Response::Bulk(Some(std::borrow::Cow::Owned(value.into_bytes()))),
                )
            })
            .collect(),
    )
}

/// A move message arriving on this shard's fabric loop.
pub(super) fn on_message(state: &Rc<ShardState>, message: MoveMessage) {
    match message {
        MoveMessage::Start(session) => {
            tokio::task::spawn_local(run_source(Rc::clone(state), session));
        }
        MoveMessage::Open(session) => {
            if let Err(reason) = record_begin(state, &session) {
                session.fail(reason);
            }
            applied(&session);
        }
        MoveMessage::Copy(session, keys) => {
            if session.outcome() == Outcome::Copying {
                apply_copies(state, &session, keys);
            }
            applied(&session);
        }
        MoveMessage::Flip(session) => take_over(state, &session),
        MoveMessage::Cancel(session) => {
            tokio::task::spawn_local(forget_range(Rc::clone(state), session, "cancelled"));
        }
        MoveMessage::Export(export) => super::migrate::start_export(state, export),
        MoveMessage::CancelImport(import) => super::migrate::cancel_import(state, import),
        MoveMessage::Settle => super::migrate::settle(state),
        MoveMessage::Forget { first, last } => {
            let state = Rc::clone(state);

            tokio::task::spawn_local(async move { drop_slots(&state, first, last).await });
        }
    }
}

/// Saves `history` when it lives in a data directory.
pub(super) fn save(history: &SlotMoves, data_dir: Option<&std::path::Path>) -> std::io::Result<()> {
    match data_dir {
        Some(data_dir) => history.save(data_dir),
        None => Ok(()),
    }
}

fn applied(session: &MoveSession) {
    session.applied.fetch_add(1, Ordering::AcqRel);
    session.progress.notify_one();
}

/// Target, before the first copy: records the move as begun at its next log sequence, so a
/// restart before the hand-over skips the copies.
fn record_begin(state: &ShardState, session: &MoveSession) -> Result<(), String> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return Ok(());
    };
    let since = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .last_sequence()
        + 1;
    let mut mover = forwarding.fabric.mover.lock();
    let Some((history, data_dir)) = mover.durable.as_mut() else {
        return Ok(());
    };

    // Changed in memory only once the file holds it, so a failed write leaves no trace.
    let mut next = history.clone();

    next.begin(session.first, session.last, session.to as u16, since)
        .map_err(|error| format!("slot table refused the move: {error}"))?;
    save(&next, data_dir.as_deref())
        .map_err(|error| format!("could not write the slot table: {error}"))?;
    *history = next;
    Ok(())
}

/// Target: logs and applies each key as the source holds it. A key absent from a database
/// there is removed here too.
fn apply_copies(state: &ShardState, session: &MoveSession, batch: CopyBatch) {
    match apply_batch(state, &batch) {
        Ok(keys) => {
            session.keys.fetch_add(keys, Ordering::AcqRel);
        }
        Err(reason) => {
            session.fail(reason);
        }
    }
}

/// Logs and applies a batch of copies; the number of keys, or why the move must stop.
pub(super) fn apply_batch(state: &ShardState, batch: &CopyBatch) -> Result<u64, String> {
    use crate::mutation::MutationSink;

    let Some(keys) = batch.entries() else {
        return Err("target received a damaged copy".into());
    };
    let count = keys.len() as u64;
    let mut store = state.store.borrow_mut();
    let databases = store.database_ids();
    let active = store.active_database();
    let mut persistence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let logging = persistence.persists();

    // One record group for the whole batch, as one client batch shares one write.
    if logging {
        persistence.begin_group();
    }
    let applied = (|| {
        for (key, frames) in keys {
            let mut entries = Vec::with_capacity(frames.len());

            for frame in &frames {
                let entry = crate::persistence::decode_entry_frame(frame)
                    .map_err(|error| format!("target received a damaged copy: {error}"))?;

                entries.push(entry);
            }
            if logging {
                log_copy(&mut persistence, &store, &databases, key, &frames, &entries)
                    .map_err(|error| format!("target could not log the copy: {error}"))?;
            }
            for database in &databases {
                store.forget_key(*database, key);
            }
            for entry in entries {
                store
                    .restore_snapshot_entry(entry)
                    .map_err(|error| format!("target could not take a key: {error}"))?;
            }
        }
        Ok::<(), String>(())
    })();
    let applied = if logging {
        persistence.select_database(active);
        let ended = persistence
            .end_group()
            .map_err(|error| format!("target could not log the copy: {error}"));

        applied.and(ended)
    } else {
        applied
    };

    drop(persistence);
    applied?;
    if store.memory_admission_would_fail(0) {
        return Err("the range does not fit under the target shard's memory limit".into());
    }

    Ok(count)
}

/// One `SlotCopy` record per database that holds `key` on either side, carrying the
/// source's frame as it came.
fn log_copy(
    persistence: &mut crate::persistence::WalSink,
    store: &KvShard,
    databases: &[u8],
    key: &[u8],
    frames: &[&[u8]],
    entries: &[crate::store::SnapshotEntry],
) -> Result<(), crate::PersistenceError> {
    use crate::mutation::{Mutation, MutationSink};

    for (frame, entry) in frames.iter().zip(entries) {
        persistence.select_database(entry.database);
        persistence.record(Mutation::SlotCopy {
            key,
            entry: Some(frame),
        })?;
    }
    // Held here, gone there: replay removes it as the apply does.
    for database in databases {
        if entries.iter().all(|entry| entry.database != *database)
            && store.holds_key_in(*database, key)
        {
            persistence.select_database(*database);
            persistence.record(Mutation::SlotCopy { key, entry: None })?;
        }
    }
    Ok(())
}

/// Target, in the hand-over: takes the range unless the source gave up meanwhile.
fn take_over(state: &Rc<ShardState>, session: &Arc<MoveSession>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    // The copy on disk, then the move recorded: the one point a restart sees it happen. The
    // sync runs before the source's lock is taken, so the source's time limit still holds
    // while it runs.
    let synced = {
        let mut persistence = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        persistence
            .flush()
            .map(|()| persistence.last_sequence())
            .map_err(|error| format!("target could not sync the copy: {error}"))
    };
    let mut outcome = session
        .outcome
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if *outcome != Outcome::Copying {
        // The source timed out or the copy failed: the source sends the cancel.
        return;
    }

    let position = match synced {
        Ok(position) => position,
        Err(reason) => {
            *outcome = Outcome::Failed(reason);
            drop(outcome);
            session.handed_over.notify_all();
            return;
        }
    };
    {
        let mut mover = forwarding.fabric.mover.lock();

        if let Some((history, data_dir)) = mover.durable.as_mut() {
            // As in `record_begin`: memory follows the file, never leads it.
            let mut next = history.clone();
            let saved = next
                .complete(session.until.load(Ordering::Acquire))
                .map_err(str::to_string)
                .and_then(|()| save(&next, data_dir.as_deref()).map_err(|error| error.to_string()));

            if saved.is_ok() {
                *history = next;
            } else if let Err(error) = save(history, data_dir.as_deref()) {
                // The failed write may have replaced the file before it failed; without the
                // old table back, a restart would give the range to the target.
                error!(%error, "could not restore the slot table after a failed move");
            }
            if let Err(error) = saved {
                drop(mover);
                *outcome = Outcome::Failed(format!("could not record the move: {error}"));
                drop(outcome);
                session.handed_over.notify_all();
                return;
            }
        }
    }
    session.position.store(position, Ordering::Release);

    let table = forwarding.fabric.slot_table();

    for slot in session.first..=session.last {
        table.assign(slot, session.to);
    }
    forwarding.fabric.mover.mark_moved();
    *outcome = Outcome::HandedOver {
        freeze: Duration::ZERO,
    };
    drop(outcome);
    session.handed_over.notify_all();
}

/// Source: copies the range, hands it over, then drops its copy.
async fn run_source(state: Rc<ShardState>, session: Arc<MoveSession>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let fabric = Arc::clone(&forwarding.fabric);

    state
        .store
        .borrow_mut()
        .watch_slots(session.first, session.last);

    let result = copy_and_hand_over(&state, &fabric, &session).await;

    state.store.borrow_mut().unwatch_slots();

    match result {
        Ok(()) => {
            log_moved(&state, &session);
            release_range_clients(&state, &session);
            forget_range(state, session, "done").await;
        }
        Err(reason) => {
            session.fail(reason);
            fabric.send(
                session.from,
                session.to,
                Message::Move(MoveMessage::Cancel(session)),
            );
        }
    }
}

async fn copy_and_hand_over(
    state: &Rc<ShardState>,
    fabric: &Arc<Fabric>,
    session: &Arc<MoveSession>,
) -> Result<(), String> {
    send(fabric, session, MoveMessage::Open(Arc::clone(session)));
    settle(session, 0).await?;

    // Every key of the range, database by database, in bounded steps, encoded straight
    // from the live values into the batch.
    let databases = state.store.borrow().database_ids();
    let mut batch = CopyBatch::default();

    for database in databases {
        let mut cursor = 0;

        loop {
            {
                let store = state.store.borrow();
                let mut failed = None;

                cursor = store.walk_slots(
                    database,
                    session.first,
                    session.last,
                    cursor,
                    WALK_STEP,
                    &mut |key| {
                        if failed.is_none()
                            && let Err(error) = batch.push(&store, key)
                        {
                            failed = Some(error);
                        }
                    },
                );
                if let Some(error) = failed {
                    return Err(format!("source could not copy a key: {error}"));
                }
            }

            if batch.full() {
                send_copies(fabric, session, std::mem::take(&mut batch)).await?;
            }
            if cursor == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    send_copies(fabric, session, batch).await?;

    // Keys written since they were copied, until few are left.
    for _ in 0..CATCH_UP_ROUNDS {
        settle(session, 0).await?;

        let changes = state.store.borrow_mut().take_slot_changes();

        if changes.rewritten {
            return Err("a flush or SWAPDB ran during the move".into());
        }
        if changes.keys.len() <= HAND_OVER_KEYS {
            return hand_over(state, fabric, session, changes.keys);
        }

        session.rounds.fetch_add(1, Ordering::AcqRel);
        let mut batch = CopyBatch::default();

        for key in &changes.keys {
            batch
                .push(&state.store.borrow(), key)
                .map_err(|error| format!("source could not copy a key: {error}"))?;
            if batch.full() {
                send_copies(fabric, session, std::mem::take(&mut batch)).await?;
            }
        }
        send_copies(fabric, session, batch).await?;
    }

    settle(session, 0).await?;
    let changes = state.store.borrow_mut().take_slot_changes();

    if changes.rewritten {
        return Err("a flush or SWAPDB ran during the move".into());
    }
    hand_over(state, fabric, session, changes.keys)
}

/// One synchronous turn: the last changes, the flip, and a bounded wait for the target.
/// No other task of this shard runs until it returns.
fn hand_over(
    state: &ShardState,
    fabric: &Arc<Fabric>,
    session: &Arc<MoveSession>,
    keys: Vec<Box<[u8]>>,
) -> Result<(), String> {
    let started = Instant::now();

    if !keys.is_empty() {
        let mut batch = CopyBatch::default();
        let store = state.store.borrow();

        for key in &keys {
            batch
                .push(&store, key)
                .map_err(|error| format!("source could not copy a key: {error}"))?;
        }
        drop(store);
        send(
            fabric,
            session,
            MoveMessage::Copy(Arc::clone(session), batch),
        );
    }
    // Nothing runs on this shard until the wait below ends, so its next record is the
    // first after the hand-over. Its log is synced first: a restart that lost a tail below
    // `until` would reuse sequences the slot table already gave away.
    let until = {
        let mut persistence = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        persistence
            .flush()
            .map_err(|error| format!("source could not sync its log: {error}"))?;
        persistence.last_sequence() + 1
    };

    session.until.store(until, Ordering::Release);
    fabric.send(
        session.from,
        session.to,
        Message::Move(MoveMessage::Flip(Arc::clone(session))),
    );

    let outcome = session
        .outcome
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let (mut outcome, _) = session
        .handed_over
        .wait_timeout_while(outcome, HAND_OVER_LIMIT, |outcome| {
            *outcome == Outcome::Copying
        })
        .unwrap_or_else(|error| error.into_inner());

    match &mut *outcome {
        Outcome::HandedOver { freeze } => {
            *freeze = started.elapsed();
            Ok(())
        }
        Outcome::Failed(reason) => Err(reason.clone()),
        Outcome::Copying => {
            // Under the lock, so the target can no longer take the range.
            *outcome = Outcome::Failed("the target did not take the range in time".into());
            Err("the target did not take the range in time".into())
        }
    }
}

fn send(fabric: &Fabric, session: &Arc<MoveSession>, message: MoveMessage) {
    session.sent.fetch_add(1, Ordering::AcqRel);
    fabric.send(session.from, session.to, Message::Move(message));
}

async fn send_copies(
    fabric: &Fabric,
    session: &Arc<MoveSession>,
    batch: CopyBatch,
) -> Result<(), String> {
    if batch.keys == 0 {
        return Ok(());
    }

    settle(session, IN_FLIGHT).await?;
    send(
        fabric,
        session,
        MoveMessage::Copy(Arc::clone(session), batch),
    );
    Ok(())
}

/// Waits until at most `behind` copy messages are still queued at the target.
async fn settle(session: &MoveSession, behind: u64) -> Result<(), String> {
    loop {
        let notified = session.progress.notified();

        if let Outcome::Failed(reason) = session.outcome() {
            return Err(reason);
        }
        if session.applied.load(Ordering::Acquire) + behind >= session.sent.load(Ordering::Acquire)
        {
            return Ok(());
        }
        notified.await;
    }
}

/// Source, after the hand-over: one `SlotsMoved` record, so a change stream that follows this
/// shard learns where the range continues. A failure only loses that event.
fn log_moved(state: &ShardState, session: &MoveSession) {
    use crate::mutation::{Mutation, MutationSink};

    let mut persistence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if !persistence.persists() {
        return;
    }

    persistence.begin_group();
    let logged = persistence
        .record(Mutation::SlotsMoved {
            first: session.first,
            last: session.last,
            to: session.to as u16,
            position: session.position.load(Ordering::Acquire),
        })
        .and(persistence.end_group())
        .and_then(|()| persistence.flush());

    if let Err(error) = logged {
        warn!(%error, "the slot move was not logged for change streams");
    }
}

/// Source, after the hand-over: clients waiting on keys of the range here would wait for
/// writes that now go to the target.
fn release_range_clients(state: &ShardState, session: &MoveSession) {
    release_clients(state, session.first, session.last);
}

/// Releases clients blocked on, watching or tracking keys of `first..=last`.
pub(super) fn release_clients(state: &ShardState, first: u16, last: u16) {
    let contains = |key: &[u8]| (first..=last).contains(&hash_slot(key));

    for waiter in state.waiters.borrow_mut().take_where(contains) {
        waiter.fill(UNBLOCKED_REPLY.to_vec());
    }

    // A WATCH on a moved key fails its EXEC, and a retried blocking command wakes to find
    // its key gone from this shard.
    state.store.borrow_mut().dirty_watches_in_slots(first, last);

    let tracked = state.tracker.borrow().tracked_where(contains);

    if !tracked.is_empty() {
        let invalidations = state.tracker.borrow_mut().invalidate(&tracked);

        super::hot::push_invalidations(state, invalidations);
    }
}

/// Removes this shard's keys of the range in bounded steps, then ends the move: the source
/// after a hand-over, the target after a cancel.
async fn forget_range(state: Rc<ShardState>, session: Arc<MoveSession>, ended: &'static str) {
    drop_slots(&state, session.first, session.last).await;
    finish(&state, &session, ended);
}

/// Removes this shard's keys of `first..=last` in bounded steps, without logging it.
pub(super) async fn drop_slots(state: &ShardState, first: u16, last: u16) {
    let databases = state.store.borrow().database_ids();

    for database in databases {
        let mut cursor = 0;

        loop {
            let mut keys: Vec<Box<[u8]>> = Vec::new();

            cursor = state.store.borrow().walk_slots(
                database,
                first,
                last,
                cursor,
                CLEAN_UP_STEP,
                &mut |key| keys.push(key.into()),
            );

            {
                let mut store = state.store.borrow_mut();

                for key in &keys {
                    store.forget_key(database, key);
                }
            }

            if cursor == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
}

fn finish(state: &ShardState, session: &Arc<MoveSession>, ended: &'static str) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let error = match session.outcome() {
        Outcome::Failed(reason) => Some(reason),
        _ => None,
    };
    // Built before the mover lock is taken (see `status`).
    let report = session.report(ended, error);
    let mut mover = forwarding.fabric.mover.lock();

    mover.last = Some(report);
    if mover
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, session))
    {
        mover.current = None;
    }
}

/// With [`SlotMover::moved`] set: whether `command`, about to run on this shard, belongs to
/// a slot another shard took over since it was routed.
pub(super) fn runs_elsewhere(
    state: &ShardState,
    session: &TxSession,
    command: &Command<'_>,
) -> bool {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return false;
    };
    let slot = if matches!(command, Command::Exec) {
        match transaction_slot(session.queued()) {
            Ok(Some(slot)) => Some(slot),
            Ok(None) => session.watch_slot(),
            // Keys of several slots on this shard: any of them moved away is enough.
            Err(_) => {
                return session.queued().iter().any(|queued| {
                    crate::routing::command_keys(queued)
                        .into_iter()
                        .any(|key| forwarding.fabric.owner_of(key) != forwarding.me)
                });
            }
        }
    } else if session.is_multi() {
        // Queued, not run: EXEC decides.
        None
    } else {
        // As routing decides it (a hot key never leaves). A command whose keys are now on
        // two shards does not run here either: routed again, it is refused as a split.
        return !matches!(
            super::forward::command_owner(state, forwarding, command).0,
            super::forward::Owner::Local
        );
    };

    slot.is_some_and(|slot| forwarding.fabric.slot_owner(slot) != forwarding.me)
}

/// The shard every keyed frame of a forwarded request now belongs to, when that is not
/// this one: the request goes there whole. `None` runs it here.
pub(super) fn request_owner(state: &ShardState, frames: &[u8], count: u32) -> Option<usize> {
    let forwarding = state.forwarding.as_ref()?;
    let limits = state.published_config.protocol;
    let mut consumed = 0;
    let mut owner = None;

    for _ in 0..count {
        let (command, length) = parse_command_with_limits(&frames[consumed..], limits).ok()??;

        consumed += length;
        let Some(slot) = command_slot(&command).ok().flatten() else {
            continue;
        };
        let found = forwarding.fabric.slot_owner(slot);

        match owner {
            None => owner = Some(found),
            Some(previous) if previous != found => return None,
            Some(_) => {}
        }
    }

    // A range on another node or frozen for a move runs here and answers its redirect.
    owner.filter(|owner| *owner != forwarding.me && *owner < forwarding.fabric.shard_count())
}
