//! `RUVIO MIGRATE`: moves a slot range to another node of a multi-node cluster while both
//! keep serving it. The coordinators drive each move:
//!
//! 1. `IMPORT id first last shard` on the target shard: it records that the range begins to
//!    arrive (a `SlotsIncoming` record and a `SLOTS` entry), so a restart before the end
//!    skips the copies.
//! 2. `EXPORT id first last host port [AUTH ...]` on the source node: the shard that holds
//!    the range connects to the target shard's client port and copies the range with
//!    `COPY id batch` in bounded steps, then the keys written meanwhile until few are left.
//! 3. The source freezes the range (its commands get `TRYAGAIN`), sends the last changes and
//!    `PREPARE id`: the target syncs its log, marks the copy prepared and answers the
//!    sequence it reached. A prepared copy survives a restart until the map decides.
//! 4. The coordinators commit the move and push a map that gives the range to the target.
//!    The target then serves it; the source records that it gave the range away (a
//!    `SlotsGiven` record), answers `MOVED` for it and drops its copy in bounded steps.
//!
//! `CANCEL id` on either side ends a move before the map is pushed: the source serves the
//! range again, the target drops the copies. Nothing here runs while no move is in progress,
//! and the routing path is unchanged: a frozen slot is a slot table entry past the shards
//! (see [`crate::routing::FROZEN_BASE`]), answered on the existing other-node branch.

use super::*;
use crate::slot_moves::IncomingStep;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use super::moveslots::{CopyBatch, apply_batch, drop_slots, release_clients};

/// Copy messages the source keeps in flight before it waits for a reply.
const IN_FLIGHT: usize = 4;
/// Index positions one walk step visits.
const WALK_STEP: usize = 1024;
/// Catch-up rounds before the source freezes the range whatever is still changing.
const CATCH_UP_ROUNDS: usize = 32;
/// Changed keys few enough to send while the range is frozen.
const FREEZE_KEYS: usize = 512;
/// The longest the source waits for one reply from the target.
const REPLY_LIMIT: Duration = Duration::from_secs(10);
/// The longest the frozen hand-over may take before the source gives up and serves the
/// range again (the coordinators have not committed anything yet).
const FREEZE_LIMIT: Duration = Duration::from_secs(1);

/// The moves this node takes part in: at most one import and one export at a time.
#[derive(Default)]
pub(super) struct Migrations {
    inner: Mutex<State>,
}

#[derive(Default)]
struct State {
    import: Option<Arc<Import>>,
    export: Option<Arc<Export>>,
    /// What the last finished import or export reported.
    last: Vec<(String, String)>,
    /// Where a prepared move is kept across a restart (`None` without a log).
    dir: Option<std::path::PathBuf>,
}

/// A prepared import, kept until the map or a cancel decides it.
const IMPORT_FILE: &str = "MIGRATE_IN";
/// A prepared export: after a restart its range stays frozen until the map or a cancel
/// decides it, so the source never takes writes the target may already own.
const EXPORT_FILE: &str = "MIGRATE_OUT";

impl Migrations {
    /// Whether a range is arriving or leaving.
    pub(super) fn busy(&self) -> bool {
        let state = self.lock();

        state.import.is_some() || state.export.is_some()
    }

    fn save(&self, name: &str, fields: &[String]) -> Result<(), String> {
        let Some(dir) = self.lock().dir.clone() else {
            return Ok(());
        };
        let mut text = fields.join("\n");

        text.push('\n');
        crate::replication::write_durably(&dir.join(name), text.as_bytes())
            .map_err(|error| format!("could not record the prepared move: {error}"))
    }

    fn clear(&self, name: &str) {
        let Some(dir) = self.lock().dir.clone() else {
            return;
        };

        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(%error, file = name, "could not remove a finished move's file"),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// A range arriving on one shard of this node.
pub(super) struct Import {
    id: Vec<u8>,
    first: u16,
    last: u16,
    shard: usize,
    keys: AtomicU64,
    started: Instant,
    /// The target's last sequence once prepared; 0 before.
    prepared: AtomicU64,
    /// Set by a cancel before anything else: later copies and a late prepare are refused.
    cancelled: AtomicBool,
}

/// A range leaving one shard of this node.
pub(super) struct Export {
    id: Vec<u8>,
    first: u16,
    last: u16,
    shard: usize,
    host: String,
    port: u16,
    auth: Vec<Vec<u8>>,
    keys: AtomicU64,
    rounds: AtomicU64,
    started: Instant,
    /// Set once the range is frozen: the copy rate no longer applies.
    frozen: AtomicBool,
    outcome: Mutex<ExportState>,
    cancel: tokio::sync::Notify,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExportState {
    Copying,
    /// The range is frozen and the target holds the whole copy at `position`: the move
    /// waits for the map. `freeze` is how long the range was frozen before the target
    /// answered.
    Prepared {
        position: u64,
        freeze: Duration,
    },
    Failed(String),
    Cancelled,
}

impl Export {
    fn state(&self) -> ExportState {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Moves on from `Copying` only; true when this call changed the state.
    fn end(&self, next: ExportState) -> bool {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if *outcome != ExportState::Copying {
            return false;
        }
        *outcome = next;
        true
    }
}

/// `RUVIO MIGRATE ...`.
pub(super) fn command(state: &Rc<ShardState>, args: &[&[u8]]) -> Response<'static> {
    let result = match args.first().map(|word| word.to_ascii_uppercase()) {
        Some(sub) if sub == b"STATUS" && args.len() == 1 => return status(state),
        Some(sub) if sub == b"IMPORT" && args.len() == 5 => import(state, args),
        Some(sub) if sub == b"COPY" && args.len() == 3 => copy(state, args[1], args[2]),
        Some(sub) if sub == b"PREPARE" && args.len() == 2 => prepare(state, args[1]),
        Some(sub) if sub == b"EXPORT" && (args.len() == 6 || args.len() >= 8) => {
            export(state, args)
        }
        Some(sub) if sub == b"CANCEL" && args.len() == 2 => cancel(state, args[1]),
        _ => Err(
            "ERR RUVIO MIGRATE expects IMPORT id first last shard, COPY id batch, PREPARE id, \
             EXPORT id first last host port [AUTH ...], CANCEL id or STATUS"
                .into(),
        ),
    };

    match result {
        Ok(response) => response,
        Err(message) => Response::OwnedError(message.into_bytes()),
    }
}

type Reply = Result<Response<'static>, String>;

fn forwarding(state: &ShardState) -> Result<&Forwarding, String> {
    state
        .forwarding
        .as_ref()
        .filter(|forwarding| forwarding.fabric.topology().is_some())
        .ok_or_else(|| "ERR RUVIO MIGRATE needs a node started with cluster_topology".into())
}

fn number<T: std::str::FromStr>(bytes: &[u8], what: &str) -> Result<T, String> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| format!("ERR RUVIO MIGRATE {what} is not a valid number"))
}

/// Move ids are kept in a text file: 1 to 64 printable ASCII characters, no spaces.
fn move_id(id: &[u8]) -> Result<Vec<u8>, String> {
    if id.is_empty() || id.len() > 64 || !id.iter().all(u8::is_ascii_graphic) {
        return Err("ERR RUVIO MIGRATE id must be 1 to 64 printable characters".into());
    }
    Ok(id.to_vec())
}

fn slot_range(first: &[u8], last: &[u8]) -> Result<(u16, u16), String> {
    let first: u16 = number(first, "first slot")?;
    let last: u16 = number(last, "last slot")?;

    if first > last || usize::from(last) >= crate::routing::HASH_SLOT_COUNT {
        return Err("ERR RUVIO MIGRATE slot range is not valid".into());
    }
    Ok((first, last))
}

fn writable(state: &ShardState) -> Result<(), String> {
    if state.role.get() == crate::config::ReplicationRole::Replica {
        return Err("ERR RUVIO MIGRATE runs on a primary".into());
    }
    if state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .profile()
        == crate::DurabilityProfile::Snapshot
    {
        return Err("ERR RUVIO MIGRATE needs durability memory, everysec or always".into());
    }
    Ok(())
}

/// Appends one record in its own group and syncs the log; the record's sequence.
fn log_step(state: &ShardState, mutation: crate::mutation::Mutation<'_>) -> Result<u64, String> {
    use crate::mutation::MutationSink;

    let mut persistence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    if !persistence.persists() {
        return Ok(persistence.last_sequence() + 1);
    }
    persistence.begin_group();
    persistence
        .record(mutation)
        .and(persistence.end_group())
        .and_then(|()| persistence.flush())
        .map_err(|error| format!("ERR RUVIO MIGRATE could not log the move: {error}"))?;
    Ok(persistence.last_sequence())
}

/// `IMPORT id first last shard`, on the target shard.
fn import(state: &Rc<ShardState>, args: &[&[u8]]) -> Reply {
    let forwarding = forwarding(state)?;
    let (first, last) = slot_range(args[2], args[3])?;
    let shard: usize = number(args[4], "shard")?;
    let id = move_id(args[1])?;

    if shard != forwarding.me {
        return Err(format!(
            "ERR RUVIO MIGRATE IMPORT for shard {shard} reached shard {}",
            forwarding.me
        ));
    }
    writable(state)?;

    let migrations = &forwarding.fabric.migrations;
    let running = forwarding
        .fabric
        .mover
        .unless_moving(|| migrations.lock().import.clone())
        .ok_or("ERR RUVIO MIGRATE a move between shards is in progress")?;

    if let Some(import) = running {
        if import.id == args[1] && (import.first, import.last) == (first, last) {
            return Ok(Response::Simple(b"OK"));
        }
        // The coordinators run one move at a time: another id is a move they ended while
        // this node could not hear it.
        cancel(state, &import.id)?;
        return Err(
            "ERR RUVIO MIGRATE another range is arriving on this node; it is being cancelled"
                .into(),
        );
    }
    // The history is checked before anything is logged, then recorded at the sequence of
    // the begin record: copies follow it.
    forwarding
        .fabric
        .mover
        .with_history(|history, _| {
            let mut next = history.clone();

            next.begin_gain(first, last, shard as u16, 1).map(|_| ())
        })
        .unwrap_or(Ok(()))
        .map_err(|error| format!("ERR RUVIO MIGRATE {error}"))?;
    let since = log_step(
        state,
        crate::mutation::Mutation::SlotsIncoming {
            first,
            last,
            step: IncomingStep::Begin as u8,
        },
    )?;
    record_incoming(state, first, last, IncomingStep::Begin, since)?;

    migrations.lock().import = Some(Arc::new(Import {
        id,
        first,
        last,
        shard,
        keys: AtomicU64::new(0),
        started: Instant::now(),
        prepared: AtomicU64::new(0),
        cancelled: AtomicBool::new(false),
    }));
    info!(
        first,
        last, shard, "a slot range begins to arrive from another node"
    );
    Ok(Response::Simple(b"OK"))
}

/// Records a step of an arriving range in the `SLOTS` history: on the target as it takes
/// it, on its replicas as the record arrives.
fn record_incoming(
    state: &ShardState,
    first: u16,
    last: u16,
    step: IncomingStep,
    sequence: u64,
) -> Result<(), String> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return Ok(());
    };
    let shard = forwarding.me as u16;

    forwarding
        .fabric
        .mover
        .with_history(|history, data_dir| {
            let mut next = history.clone();

            match step {
                IncomingStep::Begin => next.begin_gain(first, last, shard, sequence)?,
                IncomingStep::Prepared => {
                    // A replica that missed the begin (a crash before it saved it) still
                    // keeps the range's records from the start of its log.
                    if next.unfinished_gain().is_none_or(|gain| {
                        (gain.first, gain.last, gain.to) != (first, last, shard)
                    }) {
                        warn!(first, last, "the start of an arriving range was not recorded here; keeping the range's whole log");
                        next.begin_gain(first, last, shard, 1)?;
                    }
                    next.prepare(sequence)?
                }
                IncomingStep::Cancelled => next.cancel(),
            }
            super::moveslots::save(&next, data_dir)
                .map_err(|_| "could not write the slot table")?;
            *history = next;
            Ok::<(), &'static str>(())
        })
        .unwrap_or(Ok(()))
        .map_err(|error| format!("ERR RUVIO MIGRATE {error}"))
}

fn current_import(state: &ShardState, id: &[u8]) -> Result<Arc<Import>, String> {
    let forwarding = forwarding(state)?;
    let migrations = forwarding.fabric.migrations.lock();

    migrations
        .import
        .as_ref()
        .filter(|import| import.id == id && import.shard == forwarding.me)
        .cloned()
        .ok_or_else(|| "ERR RUVIO MIGRATE no such move arriving on this shard".into())
}

/// `COPY id batch`, on the target shard.
fn copy(state: &Rc<ShardState>, id: &[u8], batch: &[u8]) -> Reply {
    let import = current_import(state, id)?;

    if import.cancelled.load(Ordering::Acquire) {
        return Err("ERR RUVIO MIGRATE the move was cancelled".into());
    }
    if import.prepared.load(Ordering::Acquire) != 0 {
        return Err("ERR RUVIO MIGRATE the move is already prepared".into());
    }
    let keys = apply_batch(state, &CopyBatch::from_bytes(batch.to_vec()))
        .map_err(|reason| format!("ERR RUVIO MIGRATE {reason}"))?;

    import.keys.fetch_add(keys, Ordering::AcqRel);
    Ok(Response::Integer(keys as i64))
}

/// `PREPARE id`, on the target shard: the copy is complete and durable here.
fn prepare(state: &Rc<ShardState>, id: &[u8]) -> Reply {
    let import = current_import(state, id)?;

    if import.cancelled.load(Ordering::Acquire) {
        return Err("ERR RUVIO MIGRATE the move was cancelled".into());
    }
    let done = import.prepared.load(Ordering::Acquire);

    if done != 0 {
        return Ok(Response::Integer(done as i64));
    }
    let position = log_step(
        state,
        crate::mutation::Mutation::SlotsIncoming {
            first: import.first,
            last: import.last,
            step: IncomingStep::Prepared as u8,
        },
    )?;

    record_incoming(
        state,
        import.first,
        import.last,
        IncomingStep::Prepared,
        position,
    )?;
    forwarding(state)?
        .fabric
        .migrations
        .save(
            IMPORT_FILE,
            &[
                String::from_utf8_lossy(&import.id).into_owned(),
                import.first.to_string(),
                import.last.to_string(),
                import.shard.to_string(),
                position.to_string(),
            ],
        )
        .map_err(|error| format!("ERR RUVIO MIGRATE {error}"))?;
    import.prepared.store(position, Ordering::Release);
    info!(
        first = import.first,
        last = import.last,
        keys = import.keys.load(Ordering::Acquire),
        "a slot range from another node is prepared; waiting for the cluster map"
    );
    Ok(Response::Integer(position as i64))
}

/// `EXPORT id first last host port [AUTH ...]`, on any shard of the source node.
fn export(state: &Rc<ShardState>, args: &[&[u8]]) -> Reply {
    let forwarding = forwarding(state)?;
    let (first, last) = slot_range(args[2], args[3])?;
    let host = std::str::from_utf8(args[4])
        .map_err(|_| "ERR RUVIO MIGRATE host is not UTF-8".to_string())?
        .to_owned();
    let port: u16 = number(args[5], "port")?;
    let auth = match args.get(6) {
        Some(word) if word.eq_ignore_ascii_case(b"AUTH") => {
            args[7..].iter().map(|word| word.to_vec()).collect()
        }
        Some(_) => return Err("ERR RUVIO MIGRATE EXPORT expects AUTH after the port".into()),
        None => Vec::new(),
    };
    let fabric = &forwarding.fabric;
    let shard = fabric.slot_owner(first);

    if (first..=last).any(|slot| fabric.slot_owner(slot) != shard) {
        return Err("ERR RUVIO MIGRATE range has more than one owner; move each part".into());
    }
    if shard >= fabric.shard_count() {
        return Err("ERR RUVIO MIGRATE range is not on this node".into());
    }
    writable(state)?;

    let export = Arc::new(Export {
        id: move_id(args[1])?,
        first,
        last,
        shard,
        host,
        port,
        auth,
        keys: AtomicU64::new(0),
        rounds: AtomicU64::new(0),
        started: Instant::now(),
        frozen: AtomicBool::new(false),
        outcome: Mutex::new(ExportState::Copying),
        cancel: tokio::sync::Notify::new(),
    });
    let running = fabric
        .mover
        .unless_moving(|| {
            let mut migrations = fabric.migrations.lock();

            match &migrations.export {
                Some(running) => Some(Arc::clone(running)),
                None => {
                    migrations.export = Some(Arc::clone(&export));
                    None
                }
            }
        })
        .ok_or("ERR RUVIO MIGRATE a move between shards is in progress")?;

    if let Some(running) = running {
        if running.id == args[1] && (running.first, running.last) == (first, last) {
            return Ok(Response::Simple(b"OK"));
        }
        // A move the coordinators ended while this node could not hear it. They send the
        // current map before a new move, so a range still frozen here was not given away.
        cancel(state, &running.id)?;
        return Err(
            "ERR RUVIO MIGRATE another range is leaving this node; it is being cancelled".into(),
        );
    }

    if shard == forwarding.me {
        tokio::task::spawn_local(run_export(Rc::clone(state), export));
    } else {
        fabric.send(
            forwarding.me,
            shard,
            Message::Move(super::moveslots::MoveMessage::Export(export)),
        );
    }
    Ok(Response::Simple(b"OK"))
}

/// A message from another shard of this node.
pub(super) fn start_export(state: &Rc<ShardState>, export: Arc<Export>) {
    tokio::task::spawn_local(run_export(Rc::clone(state), export));
}

/// `CANCEL id`: ends the import or export with this id before the map gives the range away.
fn cancel(state: &Rc<ShardState>, id: &[u8]) -> Reply {
    let forwarding = forwarding(state)?;
    let fabric = &forwarding.fabric;
    let (import, export) = {
        let migrations = fabric.migrations.lock();

        (
            migrations.import.clone().filter(|import| import.id == id),
            migrations.export.clone().filter(|export| export.id == id),
        )
    };

    if let Some(export) = export {
        if !export.end(ExportState::Cancelled)
            && matches!(export.state(), ExportState::Prepared { .. })
        {
            // Prepared: the coordinators decide. Cancelling now gives the range back here.
            *export
                .outcome
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = ExportState::Cancelled;
            thaw(fabric, &export);
            finish_export(fabric, &export, "cancelled");
        }
        export.cancel.notify_one();
    }
    // A primary promoted during a move followed its copies from the log, but never took
    // the import itself: the coordinators cancel it by an id it does not know.
    let import = import.or_else(|| orphan_import(state, id));

    if let Some(import) = import {
        if import.shard != forwarding.me {
            fabric.send(
                forwarding.me,
                import.shard,
                Message::Move(super::moveslots::MoveMessage::CancelImport(import)),
            );
        } else {
            cancel_import(state, import);
        }
    }
    Ok(Response::Simple(b"OK"))
}

/// The unfinished gain of a primary that holds no import, as an import with `id`.
fn orphan_import(state: &ShardState, id: &[u8]) -> Option<Arc<Import>> {
    let forwarding = state.forwarding.as_ref()?;

    if state.role.get() == crate::config::ReplicationRole::Replica
        || forwarding.fabric.migrations.lock().import.is_some()
    {
        return None;
    }
    let gain = forwarding
        .fabric
        .mover
        .with_history(|history, _| history.unfinished_gain())
        .flatten()?;

    Some(Arc::new(Import {
        id: id.to_vec(),
        first: gain.first,
        last: gain.last,
        shard: usize::from(gain.to),
        keys: AtomicU64::new(0),
        started: Instant::now(),
        prepared: AtomicU64::new(gain.prepared),
        cancelled: AtomicBool::new(false),
    }))
}

/// Target shard: drops an import that will not complete.
pub(super) fn cancel_import(state: &Rc<ShardState>, import: Arc<Import>) {
    // Runs on the import's shard, as copies and the prepare do: none applies after this.
    if import.cancelled.swap(true, Ordering::AcqRel) {
        return;
    }
    let logged = log_step(
        state,
        crate::mutation::Mutation::SlotsIncoming {
            first: import.first,
            last: import.last,
            step: IncomingStep::Cancelled as u8,
        },
    )
    .and_then(|sequence| {
        record_incoming(
            state,
            import.first,
            import.last,
            IncomingStep::Cancelled,
            sequence,
        )
    });

    if let Err(error) = logged {
        warn!(%error, "a cancelled range from another node was not recorded; a restart drops it unless prepared");
    }
    let state = Rc::clone(state);

    tokio::task::spawn_local(async move {
        drop_slots(&state, import.first, import.last).await;
        if let Some(forwarding) = state.forwarding.as_ref() {
            let mut migrations = forwarding.fabric.migrations.lock();

            if migrations
                .import
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &import))
            {
                migrations.import = None;
                migrations.last = report_import(&import, "cancelled");
                drop(migrations);
                forwarding.fabric.migrations.clear(IMPORT_FILE);
            }
        }
        info!(
            first = import.first,
            last = import.last,
            "dropped a cancelled range from another node"
        );
    });
}

fn status(state: &ShardState) -> Response<'static> {
    let pairs = match state.forwarding.as_ref() {
        None => vec![("state".to_string(), "none".to_string())],
        Some(forwarding) => {
            let migrations = forwarding.fabric.migrations.lock();
            let mut pairs = Vec::new();

            if let Some(import) = &migrations.import {
                let prepared = import.prepared.load(Ordering::Acquire);

                pairs.extend(report_import(
                    import,
                    if prepared == 0 { "copying" } else { "prepared" },
                ));
                if prepared != 0 {
                    pairs.push((
                        "replicas_behind".into(),
                        replicas_behind(&forwarding.fabric, import.shard, prepared).to_string(),
                    ));
                }
            }
            if let Some(export) = &migrations.export {
                pairs.extend(report_export(export));
            }
            if pairs.is_empty() {
                pairs = migrations.last.clone();
            }
            if pairs.is_empty() {
                pairs.push(("state".into(), "none".into()));
            }
            pairs
        }
    };

    Response::Map(
        pairs
            .into_iter()
            .map(|(name, value)| {
                (
                    std::borrow::Cow::Owned(name.into_bytes()),
                    Response::Bulk(Some(std::borrow::Cow::Owned(value.into_bytes()))),
                )
            })
            .collect(),
    )
}

/// Replicas of `shard` that have not reported `sequence` durable yet.
fn replicas_behind(fabric: &Fabric, shard: usize, sequence: u64) -> usize {
    fabric
        .durability_of(1 << shard)
        .iter()
        .flat_map(|durability| durability.acks.replicas())
        .filter(|replica| replica.sequence < sequence)
        .count()
}

fn report_import(import: &Import, state: &str) -> Vec<(String, String)> {
    vec![
        (
            "id".into(),
            String::from_utf8_lossy(&import.id).into_owned(),
        ),
        ("role".into(), "target".into()),
        ("state".into(), state.into()),
        ("first".into(), import.first.to_string()),
        ("last".into(), import.last.to_string()),
        ("shard".into(), import.shard.to_string()),
        (
            "keys".into(),
            import.keys.load(Ordering::Acquire).to_string(),
        ),
        (
            "position".into(),
            import.prepared.load(Ordering::Acquire).to_string(),
        ),
        (
            "elapsed_ms".into(),
            format!("{:.3}", import.started.elapsed().as_secs_f64() * 1000.0),
        ),
    ]
}

fn report_export(export: &Export) -> Vec<(String, String)> {
    let (name, extra) = match export.state() {
        ExportState::Copying => ("copying", None),
        ExportState::Prepared { position, freeze } => (
            "prepared",
            Some(vec![
                ("position".to_string(), position.to_string()),
                (
                    "freeze_ms".to_string(),
                    format!("{:.3}", freeze.as_secs_f64() * 1000.0),
                ),
            ]),
        ),
        ExportState::Failed(reason) => ("failed", Some(vec![("error".to_string(), reason)])),
        ExportState::Cancelled => ("cancelled", None),
    };
    let mut pairs = vec![
        (
            "id".into(),
            String::from_utf8_lossy(&export.id).into_owned(),
        ),
        ("role".into(), "source".into()),
        ("state".into(), name.into()),
        ("first".into(), export.first.to_string()),
        ("last".into(), export.last.to_string()),
        ("shard".into(), export.shard.to_string()),
        ("target".into(), format!("{}:{}", export.host, export.port)),
        (
            "keys".into(),
            export.keys.load(Ordering::Acquire).to_string(),
        ),
        (
            "rounds".into(),
            export.rounds.load(Ordering::Acquire).to_string(),
        ),
        (
            "elapsed_ms".into(),
            format!("{:.3}", export.started.elapsed().as_secs_f64() * 1000.0),
        ),
    ];

    pairs.extend(extra.into_iter().flatten());
    pairs
}

/// The source's connection to the target shard: RESP commands out, one-line replies in.
struct Link {
    reader: BufReader<tokio::net::TcpStream>,
    /// Replies still to read, in order.
    pending: usize,
    /// `migration_max_bytes_per_sec` (0: no limit), and the bytes sent since `since`.
    rate: u64,
    sent: u64,
    since: Instant,
}

impl Link {
    async fn open(export: &Export, rate: u64) -> Result<Self, String> {
        let stream = tokio::time::timeout(
            REPLY_LIMIT,
            tokio::net::TcpStream::connect((export.host.as_str(), export.port)),
        )
        .await
        .map_err(|_| "the target did not accept a connection in time".to_string())?
        .map_err(|error| format!("cannot reach the target: {error}"))?;

        stream
            .set_nodelay(true)
            .map_err(|error| format!("cannot reach the target: {error}"))?;
        let mut link = Self {
            reader: BufReader::new(stream),
            pending: 0,
            rate,
            sent: 0,
            since: Instant::now(),
        };

        if !export.auth.is_empty() {
            let mut args: Vec<&[u8]> = vec![b"AUTH"];

            args.extend(export.auth.iter().map(Vec::as_slice));
            link.send(&args).await?;
            link.reply().await?;
        }
        Ok(link)
    }

    async fn send(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let mut frame =
            Vec::with_capacity(16 + args.iter().map(|arg| arg.len() + 16).sum::<usize>());

        let _ = write!(frame, "*{}\r\n", args.len());
        for arg in args {
            let _ = write!(frame, "${}\r\n", arg.len());
            frame.extend_from_slice(arg);
            frame.extend_from_slice(b"\r\n");
        }
        self.reader
            .get_mut()
            .write_all(&frame)
            .await
            .map_err(|error| format!("lost the target: {error}"))?;
        self.pending += 1;
        Ok(())
    }

    /// The next reply: an integer, or `OK`; an error reply ends the move.
    async fn reply(&mut self) -> Result<i64, String> {
        let mut line = Vec::new();
        let read = tokio::time::timeout(REPLY_LIMIT, self.reader.read_until(b'\n', &mut line))
            .await
            .map_err(|_| "the target did not answer in time".to_string())?
            .map_err(|error| format!("lost the target: {error}"))?;

        if read == 0 {
            return Err("the target closed the connection".into());
        }
        self.pending -= 1;
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end();

        match text.as_bytes().first() {
            Some(b'+') => Ok(0),
            Some(b':') => text[1..]
                .parse()
                .map_err(|_| format!("the target answered {text:?}")),
            Some(b'-') => Err(format!("the target refused: {}", &text[1..])),
            Some(b'$') => {
                // Not sent by these commands; read it whole so the link stays in step.
                let length: i64 = text[1..].parse().unwrap_or(-1);

                if length >= 0 {
                    let mut rest = vec![0; length as usize + 2];

                    self.reader
                        .read_exact(&mut rest)
                        .await
                        .map_err(|error| format!("lost the target: {error}"))?;
                }
                Ok(0)
            }
            _ => Err(format!("the target answered {text:?}")),
        }
    }

    /// Sends a batch, keeping at most [`IN_FLIGHT`] unanswered.
    async fn copy(&mut self, export: &Export, batch: CopyBatch) -> Result<(), String> {
        if batch.keys() == 0 {
            return Ok(());
        }
        while self.pending >= IN_FLIGHT {
            self.reply().await?;
        }
        if self.rate != 0 {
            // Waits until the bytes sent so far fit the rate. Never while frozen: the
            // hand-over sends few keys and must not hold the range longer.
            self.sent += batch.as_bytes().len() as u64;
            let due = Duration::from_secs_f64(self.sent as f64 / self.rate as f64);

            if let Some(wait) = due.checked_sub(self.since.elapsed())
                && !export.frozen.load(Ordering::Acquire)
            {
                tokio::time::sleep(wait).await;
            }
        }
        self.send(&[b"RUVIO", b"MIGRATE", b"COPY", &export.id, batch.as_bytes()])
            .await?;
        export
            .keys
            .fetch_add(u64::from(batch.keys()), Ordering::AcqRel);
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), String> {
        while self.pending > 0 {
            self.reply().await?;
        }
        Ok(())
    }
}

/// Source shard: copies the range to the target, freezes it and has the target prepare.
async fn run_export(state: Rc<ShardState>, export: Arc<Export>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let fabric = Arc::clone(&forwarding.fabric);

    state
        .store
        .borrow_mut()
        .watch_slots(export.first, export.last);

    let result = tokio::select! {
        result = copy_and_prepare(&state, &fabric, &export) => result,
        () = export.cancel.notified() => Err("cancelled".into()),
    };

    state.store.borrow_mut().unwatch_slots();

    match result {
        Ok(()) => info!(
            first = export.first,
            last = export.last,
            keys = export.keys.load(Ordering::Acquire),
            "a slot range is prepared on the target node; waiting for the cluster map"
        ),
        Err(reason) => {
            let cancelled = export.state() == ExportState::Cancelled;

            if !cancelled {
                warn!(first = export.first, last = export.last, %reason, "moving a slot range to another node failed");
                export.end(ExportState::Failed(reason));
            }
            thaw(&fabric, &export);
            finish_export(
                &fabric,
                &export,
                if cancelled { "cancelled" } else { "failed" },
            );
        }
    }
}

async fn copy_and_prepare(
    state: &Rc<ShardState>,
    fabric: &Arc<Fabric>,
    export: &Arc<Export>,
) -> Result<(), String> {
    let mut link = Link::open(export, state.published_config.migration_max_bytes_per_sec).await?;
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
                    export.first,
                    export.last,
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
                link.copy(export, std::mem::take(&mut batch)).await?;
            }
            if cursor == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    link.copy(export, batch).await?;

    // Keys written since they were copied, until few are left.
    let mut changes = Vec::new();

    for _ in 0..=CATCH_UP_ROUNDS {
        link.drain().await?;
        let taken = state.store.borrow_mut().take_slot_changes();

        if taken.rewritten {
            return Err("a flush or SWAPDB ran during the move".into());
        }
        changes = taken.keys;
        if changes.len() <= FREEZE_KEYS {
            break;
        }
        export.rounds.fetch_add(1, Ordering::AcqRel);
        let mut batch = CopyBatch::default();

        for key in std::mem::take(&mut changes) {
            batch
                .push(&state.store.borrow(), &key)
                .map_err(|error| format!("source could not copy a key: {error}"))?;
            if batch.full() {
                link.copy(export, std::mem::take(&mut batch)).await?;
            }
        }
        link.copy(export, batch).await?;
    }

    // Frozen: commands for the range get `TRYAGAIN` from here on, so the keys read below
    // are the last state of the range on this node.
    let frozen_at = Instant::now();

    freeze(fabric, export);
    let tail = state.store.borrow_mut().take_slot_changes();

    if tail.rewritten {
        return Err("a flush or SWAPDB ran during the move".into());
    }
    changes.extend(tail.keys);
    changes.sort_unstable();
    changes.dedup();
    let mut batch = CopyBatch::default();

    for key in &changes {
        batch
            .push(&state.store.borrow(), key)
            .map_err(|error| format!("source could not copy a key: {error}"))?;
        if batch.full() {
            link.copy(export, std::mem::take(&mut batch)).await?;
        }
    }
    link.copy(export, batch).await?;
    link.send(&[b"RUVIO", b"MIGRATE", b"PREPARE", &export.id])
        .await?;

    let position = loop {
        let remaining = FREEZE_LIMIT.saturating_sub(frozen_at.elapsed());
        let reply = tokio::time::timeout(remaining, link.reply())
            .await
            .map_err(|_| "the target did not prepare the range in time".to_string())??;

        if link.pending == 0 {
            break reply as u64;
        }
    };

    // Kept before anyone may commit the move: a restart keeps the range frozen, and the
    // slot history exists so a restart after the map gave the range away records the loss.
    fabric
        .mover
        .with_history(|history, data_dir| super::moveslots::save(history, data_dir))
        .unwrap_or(Ok(()))
        .map_err(|error| format!("could not record the slot history: {error}"))?;
    fabric.migrations.save(
        EXPORT_FILE,
        &[
            String::from_utf8_lossy(&export.id).into_owned(),
            export.first.to_string(),
            export.last.to_string(),
            export.shard.to_string(),
            position.to_string(),
            export.host.clone(),
            export.port.to_string(),
        ],
    )?;
    if !export.end(ExportState::Prepared {
        position,
        freeze: frozen_at.elapsed(),
    }) {
        return Err("cancelled".into());
    }
    Ok(())
}

/// Points the range at this shard's frozen entry: its commands get `TRYAGAIN`, and every
/// shard of this node checks forwarded and waiting commands again.
fn freeze(fabric: &Fabric, export: &Export) {
    let table = fabric.slot_table();
    let frozen = crate::routing::FROZEN_BASE + export.shard;

    export.frozen.store(true, Ordering::Release);
    fabric.mover.mark_moved();
    for slot in export.first..=export.last {
        table.assign(slot, frozen);
    }
}

/// Serves a frozen range on its shard again, unless the map already gave it away.
fn thaw(fabric: &Fabric, export: &Export) {
    let table = fabric.slot_table();
    let frozen = crate::routing::FROZEN_BASE + export.shard;

    for slot in export.first..=export.last {
        if table.owner(slot) == frozen {
            table.assign(slot, export.shard);
        }
    }
}

fn finish_export(fabric: &Fabric, export: &Arc<Export>, ended: &str) {
    let mut migrations = fabric.migrations.lock();

    if migrations
        .export
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, export))
    {
        let mut report = report_export(export);

        if let Some(state) = report.iter_mut().find(|(name, _)| name == "state") {
            state.1 = ended.into();
        }
        migrations.export = None;
        migrations.last = report;
        drop(migrations);
        fabric.migrations.clear(EXPORT_FILE);
    }
}

/// Sends every shard of this node the newest map so each settles its ranges with it.
pub(super) fn map_installed(state: &ShardState) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };

    for shard in 0..forwarding.fabric.shard_count() {
        if shard == forwarding.me {
            continue;
        }
        forwarding.fabric.send(
            forwarding.me,
            shard,
            Message::Move(super::moveslots::MoveMessage::Settle),
        );
    }
}

/// Whether the map `placement` (this node's table by a newer map) may be installed: every
/// slot it gives this node is held here, or arriving and prepared on the shard the map
/// names.
pub(super) fn map_fits(fabric: &Fabric, placement: &[u16]) -> Result<(), String> {
    let count = fabric.shard_count() as u16;

    fabric
        .mover
        .with_history(|history, _| {
            let pending = history.pending_gain();

            for (slot, &owner) in placement.iter().enumerate() {
                let slot = slot as u16;

                if owner >= count || history.owner(slot) < count {
                    continue;
                }
                let arriving = pending.is_some_and(|gain| {
                    (gain.first..=gain.last).contains(&slot) && gain.to == owner
                });

                if !arriving {
                    return Err(format!(
                        "ERR RUVIO TOPOLOGY gives this node slot {slot}, whose data is not here"
                    ));
                }
            }
            Ok(())
        })
        .unwrap_or(Ok(()))
}

/// This shard after a newer map: takes a prepared range the map gives it, and gives up a
/// range the map gave another group (logged on a primary, so a change stream follows it).
pub(super) fn settle(state: &Rc<ShardState>) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let fabric = &forwarding.fabric;
    let Some(topology) = fabric.topology().map(|shared| shared.get()) else {
        return;
    };

    super::replica::raise_fence(state);

    let me = forwarding.me as u16;
    let count = fabric.shard_count() as u16;
    let placement = topology.placement(usize::from(count));
    let next_sequence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .last_sequence()
        + 1;
    let (gained, lost) = fabric
        .mover
        .with_history(|history, data_dir| {
            let mut next = history.clone();
            let mut gained = None;

            if let Some(gain) = next.pending_gain().filter(|gain| {
                gain.to == me
                    && (gain.first..=gain.last).all(|slot| placement[usize::from(slot)] == me)
            }) && next.complete(next_sequence).is_ok()
            {
                gained = Some((gain.first, gain.last));
            }
            // Ranges this shard holds that the map gave to another group.
            let mut lost: Vec<(u16, u16)> = Vec::new();

            for slot in 0..crate::routing::HASH_SLOT_COUNT as u16 {
                if next.owner(slot) == me && placement[usize::from(slot)] >= count {
                    match lost.last_mut() {
                        Some(range) if range.1 + 1 == slot => range.1 = slot,
                        _ => lost.push((slot, slot)),
                    }
                }
            }
            // A replica records a loss when the primary's record arrives, after every
            // write of the range.
            if state.role.get() == crate::config::ReplicationRole::Replica {
                lost.clear();
            }
            for &(first, last) in &lost {
                let _ = next.lose(first, last, me, next_sequence);
            }
            if gained.is_none() && lost.is_empty() {
                return (None, Vec::new());
            }
            if let Err(error) = super::moveslots::save(&next, data_dir) {
                warn!(%error, "could not record a range that moved between nodes");
                return (None, Vec::new());
            }
            *history = next;
            (gained, lost)
        })
        .unwrap_or((None, Vec::new()));

    let table = fabric.slot_table();

    if let Some((first, last)) = gained {
        for slot in first..=last {
            table.assign(slot, usize::from(me));
        }
        let mut migrations = fabric.migrations.lock();

        if let Some(import) = migrations
            .import
            .take_if(|import| (import.first, import.last) == (first, last))
        {
            migrations.last = report_import(&import, "done");
        }
        drop(migrations);
        fabric.migrations.clear(IMPORT_FILE);
        info!(
            first,
            last, "serving a slot range that arrived from another node"
        );
    }

    for (first, last) in lost {
        give_away(state, fabric, &topology, first, last);
    }
}

/// Source shard: the map gave `first..=last` to another group. Logs it, releases clients
/// of the range and drops the keys in bounded steps.
fn give_away(
    state: &Rc<ShardState>,
    fabric: &Arc<Fabric>,
    topology: &crate::topology::Topology,
    first: u16,
    last: u16,
) {
    let export = {
        let migrations = fabric.migrations.lock();

        migrations
            .export
            .clone()
            .filter(|export| (export.first, export.last) == (first, last))
    };
    let position = match export.as_ref().map(|export| export.state()) {
        Some(ExportState::Prepared { position, .. }) => position,
        _ => 0,
    };
    let owner = fabric.slot_table().owner(first);
    let address = topology
        .remote(first, owner, fabric.shard_count())
        .map_or_else(String::new, |(host, port)| format!("{host}:{port}"));

    if let Err(error) = log_step(
        state,
        crate::mutation::Mutation::SlotsGiven {
            first,
            last,
            address: address.as_bytes(),
            position,
        },
    ) {
        warn!(%error, "the slot range given to another node was not logged for change streams");
    }
    fabric.mover.mark_moved();
    release_clients(state, first, last);
    if let Some(export) = &export {
        finish_export(fabric, export, "done");
    }
    info!(first, last, %address, "gave a slot range to another node");

    let state = Rc::clone(state);

    tokio::task::spawn_local(async move {
        drop_slots(&state, first, last).await;
    });
}

/// A replica: a record of a range moving between nodes arrived from the primary at
/// `sequence`. Records the same step here.
pub(super) fn replicated(state: &ShardState, mutation: &crate::OwnedMutation, sequence: u64) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    // The keys go in bounded steps on this shard's own loop.
    let forget = |first, last| {
        forwarding.fabric.send(
            forwarding.me,
            forwarding.me,
            Message::Move(super::moveslots::MoveMessage::Forget { first, last }),
        );
    };

    match *mutation {
        crate::OwnedMutation::SlotsIncoming { first, last, step } => {
            let Some(step) = IncomingStep::from_u8(step) else {
                return;
            };

            if let Err(error) = record_incoming(state, first, last, step, sequence) {
                warn!(%error, "a replica could not record a range arriving from another node");
            }
            if step == IncomingStep::Cancelled {
                forget(first, last);
            }
        }
        crate::OwnedMutation::SlotsGiven { first, last, .. } => {
            let me = forwarding.me as u16;
            let recorded = forwarding
                .fabric
                .mover
                .with_history(|history, data_dir| {
                    let mut next = history.clone();

                    next.lose(first, last, me, sequence)?;
                    super::moveslots::save(&next, data_dir)
                        .map_err(|_| "could not write the slot table")?;
                    *history = next;
                    Ok::<(), &'static str>(())
                })
                .unwrap_or(Ok(()));

            if let Err(error) = recorded {
                warn!(%error, "a replica could not record a range given to another node");
            }
            forwarding.fabric.mover.mark_moved();
            release_clients(state, first, last);
            forget(first, last);
        }
        _ => {}
    }
}

/// At start, before any shard serves: takes back the prepared moves a restart interrupted.
/// A prepared export the map has not given away stays frozen; a prepared import the map
/// has not completed waits for it again. Either can still be cancelled by its id.
pub(super) fn restore(fabric: &Fabric, dir: Option<std::path::PathBuf>, primary: bool) {
    let Some(dir) = dir else {
        return;
    };
    // A node restarted as a replica follows its primary; the coordinators cancel the move
    // or the map completes it there.
    if !primary {
        for name in [IMPORT_FILE, EXPORT_FILE] {
            let _ = std::fs::remove_file(dir.join(name));
        }
        fabric.migrations.lock().dir = Some(dir);
        return;
    }
    let read = |name: &str| -> Option<Vec<String>> {
        let text = std::fs::read_to_string(dir.join(name)).ok()?;

        Some(text.lines().map(str::to_owned).collect())
    };
    let imported = read(IMPORT_FILE);
    let exported = read(EXPORT_FILE);

    fabric.migrations.lock().dir = Some(dir.clone());

    if let Some(fields) = imported {
        let restored = (|| {
            let [id, first, last, shard, position] = fields.as_slice() else {
                return None;
            };
            let (first, last, shard, position) = (
                first.parse::<u16>().ok()?,
                last.parse::<u16>().ok()?,
                shard.parse::<usize>().ok()?,
                position.parse::<u64>().ok()?,
            );
            let waiting = fabric
                .mover
                .with_history(|history, _| history.pending_gain())
                .flatten()
                .is_some_and(|gain| {
                    (gain.first, gain.last, usize::from(gain.to)) == (first, last, shard)
                });

            waiting.then(|| Import {
                id: id.clone().into_bytes(),
                first,
                last,
                shard,
                keys: AtomicU64::new(0),
                started: Instant::now(),
                prepared: AtomicU64::new(position),
                cancelled: AtomicBool::new(false),
            })
        })();

        match restored {
            Some(import) => {
                info!(
                    first = import.first,
                    last = import.last,
                    "a prepared slot range from another node waits for the cluster map again"
                );
                fabric.migrations.lock().import = Some(Arc::new(import));
            }
            None => fabric.migrations.clear(IMPORT_FILE),
        }
    }

    if let Some(fields) = exported {
        let table = fabric.slot_table();
        let restored = (|| {
            let [id, first, last, shard, position, host, port] = fields.as_slice() else {
                return None;
            };
            let (first, last, shard, position, port) = (
                first.parse::<u16>().ok()?,
                last.parse::<u16>().ok()?,
                shard.parse::<usize>().ok()?,
                position.parse::<u64>().ok()?,
                port.parse::<u16>().ok()?,
            );
            // Given away already (the map on disk is newer): nothing to wait for.
            let held = shard < fabric.shard_count()
                && last >= first
                && (first..=last).all(|slot| table.owner(slot) == shard);

            held.then(|| Export {
                id: id.clone().into_bytes(),
                first,
                last,
                shard,
                host: host.clone(),
                port,
                auth: Vec::new(),
                keys: AtomicU64::new(0),
                rounds: AtomicU64::new(0),
                started: Instant::now(),
                frozen: AtomicBool::new(true),
                outcome: Mutex::new(ExportState::Prepared {
                    position,
                    freeze: Duration::ZERO,
                }),
                cancel: tokio::sync::Notify::new(),
            })
        })();

        match restored {
            Some(export) => {
                freeze(fabric, &export);
                warn!(
                    first = export.first,
                    last = export.last,
                    "a slot range prepared for another node stays frozen until the cluster \
                     map or RUVIO MIGRATE CANCEL decides it"
                );
                fabric.migrations.lock().export = Some(Arc::new(export));
            }
            None => fabric.migrations.clear(EXPORT_FILE),
        }
    }
}
