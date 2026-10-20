//! Tokio current-thread shard owner: parse, apply, and write on one executor.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    future::Future,
    io::{self, Write},
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use tracing::{error, info, warn};

use crate::{
    AppConfig, ConnectionTimeouts, KvShard, RequestLimits, SecurityConfig, ShutdownToken,
    SystemClock, TlsAcceptor, UnixMillis,
    admission::{CommandRateLimiter, ConnectionLimiter, ConnectionPermit},
    engine::{
        Response, evict_debt, execute_hot_into_with_sink, execute_in_database, write_response,
        write_response_in,
    },
    list_waiters::{DEFAULT_MAX_LIST_WAITERS, EncodedReplySlot, ListWaiters, ParkRequest},
    mutation::{ExecuteError, NoopMutationSink},
    output::BoundedOutput,
    persistence::{
        CheckpointManager, DataDirectoryLock, RecoveryReport, WalSink, apply_mutation,
        validate_shard_layout,
    },
    protocol::{
        Command, HotCommand, HotParse, ProtocolError, ProtocolLimits, SetExpiry, SetOptions,
        command_mutates_store, command_rejection, hot_command_mutates_store,
        parse_command_with_limits, parse_hot_command_with_limits,
    },
    routing::{command_slot, common_slot, hot_command_slot},
    transaction::{PrepareResult, TxSession, transaction_slot},
};

use super::{
    cluster::{
        CROSSSLOT_REPLY, ClusterCheckpointCoordinator, ClusterRoute, info_section,
        redirect_if_remote, write_cluster_info, write_cluster_myid, write_cluster_nodes,
        write_cluster_shards, write_cluster_slots, write_key_slot, write_read_mode,
    },
    replica_guard::{self, CatchupMarker},
    session::{SessionAction, SessionCtx, gate},
};

mod cdc;
mod checkpoint;
mod clients;
mod connection;
mod durable;
mod forward;
mod hot;
mod info;
mod input;
mod live;
mod maintenance;
mod migrate;
mod moveslots;
mod net;
mod pubsub;
mod replica;
#[cfg(all(test, not(miri)))]
mod tests;
mod topology;
mod wal_group;

use checkpoint::*;
use clients::ClientEntry;
use connection::*;
use forward::*;
use hot::*;
use info::*;
use input::*;
use maintenance::*;
use pubsub::*;
use replica::*;
use wal_group::*;

// One single-threaded executor owns each shard, so this state uses RefCell rather than Mutex.
// Only the WAL sink (`persistence`) is locked, because it is shared. Never share a ShardState across threads.
struct ShardState {
    store: RefCell<KvShard>,
    command_metrics: RefCell<CommandMetrics>,
    latency: RefCell<crate::latency::LatencyTable>,
    key_stats: RefCell<crate::keystats::KeyStats>,
    persistence: Arc<Mutex<WalSink>>,
    replica_frames: RefCell<Option<mpsc::Receiver<crate::replication::ReplicaDelivery>>>,
    /// The thread filling `replica_frames`, unparked once a drain has made room.
    replica_reader: RefCell<Option<std::thread::Thread>>,
    /// Signalled by that thread for every delivery, and by a drain that ran out of budget.
    replica_wake: Arc<tokio::sync::Notify>,
    replica_resync_sequence: Arc<AtomicU64>,
    /// The shard's replication role now: the configured one until `REPLICAOF` changes it.
    role: Cell<crate::config::ReplicationRole>,
    /// The primary this replica shard dials (its own port), for `ROLE`.
    followed: RefCell<Option<(String, u16)>>,
    /// The thread that ships this shard's log to replicas, or follows a primary.
    replication: RefCell<Option<crate::replication::ReplicationThread>>,
    /// Whether this shard takes writes; read for every write.
    write_gate: Cell<crate::replication::WriteGate>,
    /// Epoch of the history this shard writes as a primary (0 otherwise).
    epoch: Cell<u64>,
    /// Highest epoch a refused replica followed, set by the replication thread; checked on
    /// the maintenance tick, which fences the shard once it passes `epoch`.
    newer_epoch: Arc<AtomicU64>,
    replica_link: Arc<crate::replication::LinkStatus>,
    /// When a replica last warned that its log refuses the primary's frames.
    replica_persist_warned: Cell<Option<Instant>>,
    replica_catchup_through: Cell<u64>,
    replica_incomplete: Cell<bool>,
    replica_caught_up: Cell<bool>,
    catchup_wait_started: Cell<Instant>,
    snapshot_requests: Arc<AtomicU64>,
    snapshot_request_seen: Cell<u64>,
    last_requested_checkpoint_at: Cell<Instant>,
    recovery: RecoveryReport,
    security: SecurityConfig,
    checkpoints: CheckpointManager,
    checkpoint_generation: Cell<u64>,
    last_checkpoint_at: Cell<Instant>,
    mutations_since_checkpoint: Cell<u64>,
    checkpoint_job: RefCell<Option<CheckpointJob>>,
    /// Slice length, journal growth and overload level of the next checkpoint attempt; back
    /// to the start once a checkpoint completes or fails.
    checkpoint_ladder: Cell<checkpoint::SliceLadder>,
    /// Checkpoint attempts given up because even the longest slice with the capped journal
    /// could not outpace the writes (`snapshot_overload_skips_total`).
    checkpoint_overload_skips: Cell<u64>,
    /// No automatic checkpoint restarts before this, after such a give-up.
    checkpoint_retry_at: Cell<Instant>,
    /// The last checkpoint failure warned about, when, and how many repeats of it went
    /// unlogged since: a shard whose log failed for good fails every attempt.
    checkpoint_failure_warned: RefCell<Option<(String, Instant, u64)>>,
    /// A copy of this shard's last published checkpoint file being made for the open
    /// round on a helper thread (see `checkpoint::carry_published_file`).
    carry: RefCell<Option<checkpoint::Carry>>,
    /// Like `checkpoint_failure_warned`, for a copy that failed.
    carry_failure_warned: RefCell<Option<(String, Instant, u64)>>,
    /// A `BGSAVE` was accepted and no checkpoint has completed since: restarted when aborted.
    bgsave_pending: Cell<bool>,
    waiters: RefCell<ListWaiters<EncodedReplySlot>>,
    pubsub: RefCell<crate::pubsub::PubSubHub>,
    tracker: RefCell<crate::tracking::KeyTracker>,
    subscribers: RefCell<HashMap<u64, Outbound>>,
    clients: RefCell<HashMap<u64, ClientEntry>>,
    slowlog: RefCell<crate::slowlog::SlowLog>,
    /// Settings fixed at start: the restart-only ones are read here. The ones `CONFIG SET`
    /// changes are read from `live`.
    published_config: AppConfig,
    live: live::LiveConfig,
    /// Admits this shard's own listener connections (per-shard mode).
    limiter: ConnectionLimiter,
    next_conn_id: Cell<u64>,
    scripts: RefCell<crate::script::ScriptCache>,
    active_connections: Cell<u64>,
    retry_blockers: Cell<usize>,
    peak_input_buffer_bytes: Cell<usize>,
    peak_output_buffer_bytes: Cell<usize>,
    peak_pubsub_queue_frames: Rc<Cell<usize>>,
    peak_pubsub_queue_bytes: Rc<Cell<usize>>,
    tls: Option<TlsAcceptor>,
    route: Option<ClusterRoute>,
    forwarding: Option<Forwarding>,
    /// `port_mode = "single"`: one port, connections spread over the shards, keyless commands
    /// fanned to every shard and `SCAN` walked across all of them. Off in `per-shard` mode,
    /// where every shard has its own port and cluster clients do that themselves.
    single_port: bool,
    cluster: Option<Arc<ClusterCheckpointCoordinator>>,
    last_submitted_sequence: Cell<u64>,
    compacted_generation: Cell<u64>,
    /// `(generation, covered sequence)` of this shard's files whose round is not yet published.
    submitted_coverage: RefCell<std::collections::VecDeque<(u64, u64)>>,
    hot_store: Option<crate::hot_store::SharedMemoryStore>,
    /// Copies of `PUBLISH` for replicas; fed only while the shard is a primary.
    publish_fanout: Arc<crate::replication::PublishFanout>,
    durable_acks: Arc<crate::replication::DurableAcks>,
    /// Committed sequence and waiting durable replies, shipped to replicas.
    durable_replies: Arc<crate::replication::DurableReplies>,
    /// The primary lease from the coordinators (`primary-lease`): shared by every shard of
    /// the process, read on the maintenance tick only.
    primary_lease: Arc<crate::primary_lease::LeaseTable>,
}

impl ShardState {
    /// Whether transactions must keep their keys in one hash slot: only when other shards
    /// or cluster nodes own some of the slots.
    fn enforces_slots(&self) -> bool {
        self.forwarding.is_some() || self.route.is_some()
    }

    /// Every shard's replicas or followed primary, for the cluster answers. Empty without
    /// forwarding, where there are no cluster answers.
    fn shard_peers(&self) -> Vec<crate::runtime::cluster::ShardPeers> {
        self.forwarding
            .as_ref()
            .map(|forwarding| forwarding.fabric.shard_peers())
            .unwrap_or_default()
    }

    /// The route as cluster clients see it: absent in single-port mode.
    fn cluster_view(&self) -> Option<&ClusterRoute> {
        if self.single_port {
            None
        } else {
            self.route.as_ref()
        }
    }
}

#[derive(Clone, Copy, Default)]
struct CommandMetrics {
    commands: u64,
    gets: u64,
    sets: u64,
    increments: u64,
    other_commands: u64,
    get_duration_nanos: u128,
    set_duration_nanos: u128,
    increment_duration_nanos: u128,
    other_duration_nanos: u128,
    wal_sync_count: u64,
    wal_sync_duration_nanos: u128,
    timing_tick: u32,
}

/// Command durations are measured on one command in this many and scaled up, so the
/// processing-time counters stay estimates without two clock reads per command.
const COMMAND_TIMING_SAMPLE: u32 = 16;

#[derive(Clone, Copy)]
enum CommandGroup {
    Get,
    Set,
    Increment,
    Other,
}

struct CommandTiming {
    state: Rc<ShardState>,
    group: CommandGroup,
    started: Option<Instant>,
    /// Command-table index for the latency histogram; set only on sampled commands.
    histogram: Option<usize>,
    /// This command is the one in `HOT_KEY_SAMPLE` whose key `RUVIO HOTKEYS` counts.
    hot_sample: bool,
    /// Dropped uncounted: another timing counts this command (see `discard`).
    discarded: bool,
}

impl CommandTiming {
    fn new(
        state: Rc<ShardState>,
        group: CommandGroup,
        name: impl FnOnce() -> &'static str,
    ) -> Self {
        let sampled = {
            let mut metrics = state.command_metrics.borrow_mut();
            let tick = metrics.timing_tick;
            metrics.timing_tick = tick.wrapping_add(1);
            (
                tick.is_multiple_of(COMMAND_TIMING_SAMPLE),
                tick.is_multiple_of(crate::keystats::HOT_KEY_SAMPLE),
            )
        };
        let (sampled, hot_tick) = sampled;
        let hot_sample = hot_tick && state.live.key_stats.get();

        let histogram = (sampled && state.live.latency_histograms.get())
            .then(|| {
                crate::protocol::command_spec(name().as_bytes()).map(crate::protocol::command_index)
            })
            .flatten();

        Self {
            state,
            group,
            started: sampled.then(Instant::now),
            histogram,
            hot_sample,
            discarded: false,
        }
    }
}

/// Counts a command this connection hands to other shards (or runs on all of them): the
/// owner shards do not count what they are sent.
fn forwarded_timing(state: &Rc<ShardState>, command: &Command<'_>) -> CommandTiming {
    CommandTiming::new(state.clone(), command_group(command), || {
        crate::acl::command_acl_name(command)
    })
}

impl CommandTiming {
    /// Leaves the count to a later timing of the same command.
    fn discard(mut self) {
        self.discarded = true;
    }

    /// Feed the sampled-key tables: one access in `HOT_KEY_SAMPLE` is counted as hot, and
    /// every string write of at least `BIG_KEY_BYTES` is remembered as big.
    fn observe_key(&self, key: impl Fn() -> Option<Vec<u8>>, write_bytes: Option<usize>) {
        if !self.state.live.key_stats.get() {
            return;
        }

        if let Some(bytes) = write_bytes.filter(|&bytes| bytes >= crate::keystats::BIG_KEY_BYTES)
            && let Some(key) = key()
        {
            self.state.key_stats.borrow_mut().big(&key, bytes);
        }

        if self.hot_sample
            && let Some(key) = key()
        {
            self.state.key_stats.borrow_mut().hit(&key);
        }
    }
}

impl Drop for CommandTiming {
    fn drop(&mut self) {
        if self.discarded {
            return;
        }
        let measured = self.started.map(|started| started.elapsed().as_nanos());

        if let (Some(index), Some(nanos)) = (self.histogram, measured) {
            self.state
                .latency
                .borrow_mut()
                .record(index, u64::try_from(nanos).unwrap_or(u64::MAX));
        }

        let elapsed = measured.map_or(0, |nanos| nanos * u128::from(COMMAND_TIMING_SAMPLE));
        let mut metrics = self.state.command_metrics.borrow_mut();
        metrics.commands = metrics.commands.saturating_add(1);
        match self.group {
            CommandGroup::Get => {
                metrics.gets = metrics.gets.saturating_add(1);
                metrics.get_duration_nanos = metrics.get_duration_nanos.saturating_add(elapsed);
            }
            CommandGroup::Set => {
                metrics.sets = metrics.sets.saturating_add(1);
                metrics.set_duration_nanos = metrics.set_duration_nanos.saturating_add(elapsed);
            }
            CommandGroup::Increment => {
                metrics.increments = metrics.increments.saturating_add(1);
                metrics.increment_duration_nanos =
                    metrics.increment_duration_nanos.saturating_add(elapsed);
            }
            CommandGroup::Other => {
                metrics.other_commands = metrics.other_commands.saturating_add(1);
                metrics.other_duration_nanos = metrics.other_duration_nanos.saturating_add(elapsed);
            }
        }
    }
}

fn command_group(command: &Command<'_>) -> CommandGroup {
    match command {
        Command::Get(_) => CommandGroup::Get,
        Command::Set(..) => CommandGroup::Set,
        Command::Increment(..) | Command::IncrementCapped { .. } => CommandGroup::Increment,
        _ => CommandGroup::Other,
    }
}

fn hot_command_name(command: &crate::protocol::HotCommand<'_>) -> &'static str {
    match command {
        crate::protocol::HotCommand::Ping(_) => "ping",
        crate::protocol::HotCommand::Get(_) => "get",
        crate::protocol::HotCommand::Set(..) => "set",
        crate::protocol::HotCommand::SetEx(..) => "setex",
        crate::protocol::HotCommand::Delete(_) => "del",
        crate::protocol::HotCommand::Increment(..) => "incr",
    }
}

fn hot_command_key<'a>(command: &crate::protocol::HotCommand<'a>) -> Option<&'a [u8]> {
    use crate::protocol::HotCommand;

    match command {
        HotCommand::Get(key)
        | HotCommand::Set(key, ..)
        | HotCommand::SetEx(key, ..)
        | HotCommand::Delete(key)
        | HotCommand::Increment(key, _) => Some(key),
        HotCommand::Ping(_) => None,
    }
}

fn hot_command_write_bytes(command: &crate::protocol::HotCommand<'_>) -> Option<usize> {
    match command {
        crate::protocol::HotCommand::Set(_, value, _)
        | crate::protocol::HotCommand::SetEx(_, value, _) => Some(value.len()),
        _ => None,
    }
}

fn command_write_bytes(command: &Command<'_>) -> Option<usize> {
    match command {
        Command::Set(_, value, _) => Some(value.len()),
        _ => None,
    }
}

fn hot_command_group(command: &crate::protocol::HotCommand<'_>) -> CommandGroup {
    match command {
        crate::protocol::HotCommand::Get(_) => CommandGroup::Get,
        crate::protocol::HotCommand::Set(..) | crate::protocol::HotCommand::SetEx(..) => {
            CommandGroup::Set
        }
        crate::protocol::HotCommand::Increment(..) => CommandGroup::Increment,
        _ => CommandGroup::Other,
    }
}

struct ConnectionGuard {
    state: Rc<ShardState>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let active = &self.state.active_connections;

        active.set(active.get().saturating_sub(1));
    }
}

struct WatchGuard {
    state: Rc<ShardState>,
    conn: u64,
    outbound: Outbound,
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        self.state.store.borrow_mut().unwatch(self.conn);
        self.state.pubsub.borrow_mut().disconnect(self.conn);
        self.state.tracker.borrow_mut().forget(self.conn);
        self.state.subscribers.borrow_mut().remove(&self.conn);
        self.state.clients.borrow_mut().remove(&self.conn);
        self.outbound.close();
        refresh_notify_listeners(&self.state);
    }
}

// Socket I/O stays on the shard threads: dedicated I/O threads measured slower on a Linux
// 4-core VM (0.57-0.87x), and io_uring is not used.
pub fn run(config: &AppConfig, shutdown: ShutdownToken) -> io::Result<()> {
    run_with_force(config, shutdown, false)
}

pub fn run_with_force(config: &AppConfig, shutdown: ShutdownToken, force: bool) -> io::Result<()> {
    let config = &one_shard_without_hot_keys(config.clone());

    crate::time::Stamp::calibrate();

    if force && config.replication.role == crate::config::ReplicationRole::Replica {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "force promotion requires writer role",
        ));
    }
    let _data_lock = DataDirectoryLock::acquire(&config.persistence)
        .map_err(|error| io::Error::other(error.to_string()))?;
    if config.replication.role == crate::config::ReplicationRole::Primary {
        // Created once here, so every shard's shipper announces the same history.
        crate::replication::load_or_create_history(&config.persistence.data_dir)?;
    }
    validate_shard_layout(
        &config.persistence,
        config.shards,
        config.checkpoint_partition_count(),
        config.protocol,
    )
    .map_err(|error| io::Error::other(error.to_string()))?;

    // A one-shard node of a multi-node cluster still routes (to answer `MOVED`) and
    // answers the cluster commands, so it runs as a cluster of one shard.
    if config.shards <= 1 && config.cluster_topology.is_none() {
        let bind: SocketAddr = config
            .address
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let std_listener = bind_listener(bind)?;

        std_listener.set_nonblocking(true)?;

        return serve_unlocked(
            Some(std_listener),
            config.clone(),
            shutdown,
            None,
            None,
            None,
            crate::hot_store::shared_memory_store(&config.hot_keys),
            force,
        );
    }

    run_cluster(config, shutdown, force)
}

fn run_cluster(config: &AppConfig, shutdown: ShutdownToken, force: bool) -> io::Result<()> {
    let base: SocketAddr = config
        .address
        .parse()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    let advertised_port = config.advertised_port.unwrap_or(base.port());

    let listeners = if config.port_mode == crate::config::PortMode::Single {
        1
    } else {
        config.shards
    };

    if listeners > 1
        && usize::from(base.port().max(advertised_port)) + listeners - 1 + 10_000
            > usize::from(u16::MAX)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shard data or cluster bus port range exceeds 65535",
        ));
    }

    let hot_store = crate::hot_store::shared_memory_store(&config.hot_keys);
    if let Some(hot) = &hot_store {
        hot.keep_expired_keys(config.replication.role == crate::config::ReplicationRole::Replica);
    }
    let cluster = if config.persistence.profile == crate::DurabilityProfile::Memory {
        None
    } else {
        let manager = CheckpointManager::new(config.persistence.clone(), config.protocol);
        let generation = manager
            .plan()
            .map_err(|error| io::Error::other(error.to_string()))?
            .map_or(0, |plan| plan.generation);
        Some(Arc::new(ClusterCheckpointCoordinator::new(
            manager,
            config.checkpoint_partition_count(),
            generation,
            hot_store.clone(),
        )))
    };
    if config.replication.role != crate::config::ReplicationRole::Replica && !force {
        for shard in 0..config.shards {
            if replica_guard::read(&config.persistence.data_dir, shard as u32)?.is_some() {
                return Err(io::Error::other(
                    "incomplete replica: promotion requires --force-promote-incomplete",
                ));
            }
        }
    }
    let mut joins = Vec::with_capacity(config.shards);
    let single = config.port_mode == crate::config::PortMode::Single;

    if single {
        warn!(
            "port_mode = \"single\" is deprecated and will be removed: every shard of a \
             per-shard server also forwards misrouted keys, so one port serves every client"
        );
    }
    // Both modes forward a command to the shard that owns its key; `single` also spreads
    // connections over the shards from one port, `per-shard` lets cluster clients connect
    // to the owner directly and never sends `MOVED`.
    let fabric = Some(Fabric::new(config.shards));

    // Before any shard serves: every route reads the table the moves left, and new moves
    // are recorded next to the log.
    if let Some(fabric) = &fabric {
        // The cluster map places this group's slots on its shards first; moves between
        // the shards apply on top of it.
        let mut pushed = false;

        if let Some(path) = &config.cluster_topology {
            let (topology, saved) = crate::topology::Topology::load_newest(
                path,
                &config.persistence.data_dir,
                &config.cluster_group,
                config.shards,
            )
            .map_err(io::Error::other)?;

            pushed = saved;
            fabric.set_topology(topology);
        }
        let placement: Option<std::sync::Arc<[u16]>> = fabric
            .topology()
            .map(|shared| shared.get().placement(config.shards).into());
        let moves = load_slot_moves(config, config.shards, placement.clone(), Some(pushed))?;
        let mapped = placement.is_some();

        if let Some(moves) = &moves {
            moves.apply(&fabric.slot_table(), |slot| {
                placement
                    .as_ref()
                    .map_or(0, |placement| usize::from(placement[usize::from(slot)]))
            });
        }
        // A node of a multi-node cluster keeps its history even without a log: a range
        // arriving from another node is tracked here until the map gives it the range.
        if config.persistence.profile.uses_wal() || placement.is_some() {
            fabric.mover.set_durable(
                moves.unwrap_or_else(|| {
                    let moves = crate::slot_moves::SlotMoves::new(config.shards);

                    match placement {
                        Some(base) => moves.with_base(base),
                        None => moves,
                    }
                }),
                config
                    .persistence
                    .profile
                    .uses_wal()
                    .then(|| config.persistence.data_dir.clone()),
            );
        }
        if mapped {
            migrate::restore(
                fabric,
                config
                    .persistence
                    .profile
                    .uses_wal()
                    .then(|| config.persistence.data_dir.clone()),
                config.replication.role != crate::config::ReplicationRole::Replica,
            );
        }
    }

    // One acceptor hands each std TcpStream to a shard. SO_REUSEPORT does not balance
    // connections on macOS and needs socket2.
    if single && let Some(fabric) = &fabric {
        let acceptor = bind_listener(base)?;
        let fabric = fabric.clone();
        let limiter = ConnectionLimiter::new(config.connections);
        fabric
            .config
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .register_acceptor(limiter.clone());
        let stop = shutdown.clone();
        let exit_guard = ShutdownOnExit(shutdown.clone());

        joins.push(
            thread::Builder::new()
                .name("ruvio-acceptor".into())
                .spawn(move || {
                    let _exit_guard = exit_guard;

                    run_acceptor(acceptor, fabric, limiter, stop)
                })
                .map_err(io::Error::other)?,
        );
    }

    for shard_id in 0..config.shards {
        let listener = if single {
            None
        } else {
            let bind = SocketAddr::new(base.ip(), base.port() + shard_id as u16);
            let listener = bind_listener(bind)?;

            listener.set_nonblocking(true)?;
            Some(listener)
        };
        let shard_fabric = fabric.clone();
        let route = ClusterRoute {
            shard_id,
            shard_count: config.shards,
            advertised_host: config.advertised_host.clone(),
            advertised_port,
            slots: fabric.as_ref().map_or_else(
                || crate::routing::SlotTable::from_formula(config.shards),
                |fabric| fabric.slot_table(),
            ),
            topology: fabric
                .as_ref()
                .and_then(|fabric| fabric.topology().cloned()),
        };

        let cfg = config.clone();
        let stop = shutdown.clone();
        let exit_guard = ShutdownOnExit(shutdown.clone());
        let shard_cluster = cluster.clone();
        let shard_hot = hot_store.clone();

        joins.push(
            thread::Builder::new()
                .name(format!("ruvio-evented-{shard_id}"))
                .spawn(move || {
                    // A shard that returns or unwinds takes the whole process down with it.
                    let _exit_guard = exit_guard;

                    serve_unlocked(
                        listener,
                        cfg,
                        stop,
                        Some(route),
                        shard_fabric,
                        shard_cluster,
                        shard_hot,
                        force,
                    )
                })
                .map_err(io::Error::other)?,
        );
    }

    let mut first_error = None;

    for join in joins {
        match join.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => first_error = first_error.or(Some(error)),
            Err(_) => {
                first_error =
                    first_error.or(Some(io::Error::other("evented shard thread panicked")));
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

/// Blocking commands parked on each shard of the servers tests run (list waiters and retried
/// commands such as `BLMOVE`), by configured address and shard id, with when the shard last
/// counted them (every maintenance tick): a test polls it to know that a command has parked,
/// or that a client which hung up has given up its waiter, instead of sleeping.
#[cfg(test)]
static PARKED_WAITERS: Mutex<Vec<(String, usize, usize, Instant)>> = Mutex::new(Vec::new());

/// Records `parked` for this shard, or forgets the shard (`None`) as it stops.
#[cfg(test)]
fn note_parked_waiters(state: &ShardState, parked: Option<usize>) {
    let address = &state.published_config.address;
    let shard = state.route.as_ref().map_or(0, |route| route.shard_id);
    let mut all = PARKED_WAITERS
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    all.retain(|(other, id, _, _)| other != address || *id != shard);
    if let Some(parked) = parked {
        all.push((address.clone(), shard, parked, Instant::now()));
    }
}

/// Blocking commands parked on every shard of the running test server configured with
/// `address`, once every shard has counted them after `since`; `None` until then.
#[cfg(test)]
fn parked_waiters(address: &str, since: Instant) -> Option<usize> {
    let all = PARKED_WAITERS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let shards: Vec<_> = all
        .iter()
        .filter(|(other, _, _, _)| other == address)
        .collect();

    if shards.is_empty() || shards.iter().any(|(_, _, _, counted)| *counted <= since) {
        return None;
    }

    Some(shards.iter().map(|(_, _, parked, _)| parked).sum())
}

/// Requests shutdown for every shard when its owning thread ends for any reason.
struct ShutdownOnExit(ShutdownToken);

impl Drop for ShutdownOnExit {
    fn drop(&mut self) {
        self.0.request();
    }
}

fn one_shard_without_hot_keys(mut config: AppConfig) -> AppConfig {
    if config.drop_hot_keys_on_one_shard() {
        tracing::warn!("hot_keys is ignored with one shard: every key is already on it");
    }
    config
}

pub fn serve(
    std_listener: std::net::TcpListener,
    config: AppConfig,
    shutdown: ShutdownToken,
) -> io::Result<()> {
    let config = one_shard_without_hot_keys(config);
    let _data_lock = DataDirectoryLock::acquire(&config.persistence)
        .map_err(|error| io::Error::other(error.to_string()))?;

    serve_unlocked(
        Some(std_listener),
        config.clone(),
        shutdown,
        None,
        None,
        None,
        crate::hot_store::shared_memory_store(&config.hot_keys),
        false,
    )
}

/// The slot moves saved in the data directory, checked against the cluster map
/// (`placement`, this node's table by the map). Replicas follow ranges that move between
/// nodes through the log and the map, but not moves between a node's own shards, so those
/// are refused while replication is on. `pushed`: whether the map came from the
/// coordinators, when the check's decision is to be saved; `None` reads what an earlier
/// check saved.
fn load_slot_moves(
    config: &AppConfig,
    shard_count: usize,
    placement: Option<std::sync::Arc<[u16]>>,
    pushed: Option<bool>,
) -> io::Result<Option<crate::slot_moves::SlotMoves>> {
    if config.persistence.profile == crate::DurabilityProfile::Memory {
        return Ok(None);
    }

    let data_dir = &config.persistence.data_dir;
    let Some(moves) = crate::slot_moves::SlotMoves::load(data_dir, shard_count)? else {
        return Ok(None);
    };
    let local_moves = moves.has_local_moves();

    if local_moves && config.replication.role != crate::config::ReplicationRole::Off {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "slot ranges were moved between shards (SLOTS file); replication is not supported \
             with moved slots",
        ));
    }
    let Some(placement) = placement else {
        crate::slot_moves::SlotMoves::check_base(data_dir, shard_count, None)?;
        return Ok(Some(moves));
    };
    // Moves are recorded against the table the node had at its first move: the saved one,
    // or the formula for moves made before the node had a map.
    let moves = match crate::slot_moves::SlotMoves::load_base(data_dir, shard_count)? {
        Some(base) => moves.with_base(base),
        None => {
            crate::slot_moves::SlotMoves::check_base(data_dir, shard_count, Some(&placement))?;
            moves.with_base(placement.clone())
        }
    };
    let Some(pushed) = pushed else {
        return Ok(Some(moves));
    };
    let (moves, changed) = moves
        .reconcile(&placement, pushed)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    if changed {
        moves.save(data_dir)?;
    }
    Ok(Some(moves))
}

#[allow(clippy::too_many_arguments)]
fn serve_unlocked(
    std_listener: Option<std::net::TcpListener>,
    config: AppConfig,
    shutdown: ShutdownToken,
    route: Option<ClusterRoute>,
    fabric: Option<Arc<Fabric>>,
    cluster: Option<Arc<ClusterCheckpointCoordinator>>,
    hot_store: Option<crate::hot_store::SharedMemoryStore>,
    force: bool,
) -> io::Result<()> {
    crate::runtime::affinity::pin_shard_thread(
        config.cpu_set.as_deref(),
        route.as_ref().map_or(0, |route| route.shard_id),
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let local = tokio::task::LocalSet::new();

    rt.block_on(local.run_until(serve_async(
        std_listener,
        config,
        shutdown,
        route,
        fabric,
        cluster,
        hot_store,
        force,
    )))
}

async fn next_client(
    listener: Option<&TcpListener>,
    forwarding: Option<&Forwarding>,
) -> io::Result<(TcpStream, SocketAddr, Option<ConnectionPermit>)> {
    match (listener, forwarding) {
        (Some(listener), _) => {
            let (stream, peer) = listener.accept().await?;

            Ok((stream, peer, None))
        }
        (None, Some(forwarding)) => {
            let (stream, peer, permit) = forwarding.next_client().await?;

            Ok((stream, peer, Some(permit)))
        }
        (None, None) => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_async(
    std_listener: Option<std::net::TcpListener>,
    config: AppConfig,
    shutdown: ShutdownToken,
    route: Option<ClusterRoute>,
    fabric: Option<Arc<Fabric>>,
    cluster: Option<Arc<ClusterCheckpointCoordinator>>,
    hot_store: Option<crate::hot_store::SharedMemoryStore>,
    force: bool,
) -> io::Result<()> {
    let shard_id = route.as_ref().map_or(0, |route| route.shard_id as u32);
    let marker = replica_guard::read(&config.persistence.data_dir, shard_id)?;
    if let Some(marker) = marker {
        warn!(
            shard_id,
            ?marker,
            force,
            "incomplete replica data directory; recovery required"
        );
    }
    if marker.is_some()
        && config.replication.role != crate::config::ReplicationRole::Replica
        && !force
    {
        return Err(io::Error::other(
            "incomplete replica: promotion requires --force-promote-incomplete",
        ));
    }
    if config.persistence.sync_worker_active && !config.sync_worker_supported() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sync worker does not yet support hot_keys",
        ));
    }
    let checkpoints = CheckpointManager::new(config.persistence.clone(), config.protocol);
    let plan = checkpoints
        .plan()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let checkpoint_generation = plan.as_ref().map_or(0, |value| value.generation);
    let created_at = checkpoints
        .recovery_clock_floor(
            plan.as_ref()
                .map_or(UnixMillis::new(0), |value| value.created_at),
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
    let expected_shards = route.as_ref().map_or(1, |route| route.shard_count);
    let expected_partitions = config.checkpoint_partition_count();
    let hot_id = crate::runtime::cluster::hot_partition_id(config.shards);
    let mut snapshot_sequence = 0;

    if let Some(plan) = &plan {
        if plan.shard_count() != expected_partitions {
            return Err(io::Error::other(
                "checkpoint shard count does not match configured shard count",
            ));
        }

        snapshot_sequence = plan.last_sequence(shard_id).unwrap_or(0);
    }

    let slot_moves = match &route {
        Some(route) => load_slot_moves(
            &config,
            route.shard_count,
            route
                .topology
                .as_ref()
                .map(|shared| shared.get().placement(route.shard_count).into()),
            // Checked and saved before the shards started.
            None,
        )?,
        None => None,
    };
    let mut slot_filter = slot_moves
        .as_ref()
        .and_then(|moves| moves.replay_filter(shard_id as usize, snapshot_sequence));
    // A prepared range from another node is kept on its shard until the map decides.
    let arriving = slot_moves
        .as_ref()
        .and_then(crate::slot_moves::SlotMoves::pending_gain)
        .filter(|gain| usize::from(gain.to) == shard_id as usize)
        .map(|gain| gain.first..=gain.last);

    let (mut sink, mutations, report) = WalSink::open_after(
        config.persistence.clone(),
        shard_id,
        config.protocol,
        snapshot_sequence,
    )
    .map_err(|error| io::Error::other(error.to_string()))?;

    sink.compact(snapshot_sequence)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut incomplete = marker.is_some();
    let proven_complete = config.replication.role == crate::config::ReplicationRole::Replica
        && matches!(marker, Some(CatchupMarker::Installed { generation, snapshot, through })
            if generation == checkpoint_generation
                && snapshot == snapshot_sequence
                && sink.last_sequence() >= through);
    let mut store = KvShard::with_clock(SystemClock::with_floor(created_at));
    let replay_clock = store.pause_expiration_for_replay();

    // A replica removes a key only when the primary's delete arrives; restarted with
    // another role, the node expires keys by its own clock again.
    store.follow_primary(config.replication.role == crate::config::ReplicationRole::Replica);

    store.apply_memory_limits(config.shard_memory_limits());

    // With a cluster map, the lowest slot of another group a replay filter skipped: such
    // keys refuse the start, as they do on a shard without a filter (below).
    let other_group = |slot: u16| {
        route.as_ref().is_some_and(|route| {
            route.topology.is_some() && route.slots.owner(slot) >= route.shard_count
        })
    };
    let mut skipped_foreign: Option<u16> = None;
    let mut note_foreign = |key: &[u8]| {
        let slot = crate::routing::hash_slot(key);

        if other_group(slot)
            && !slot_moves
                .as_ref()
                .is_some_and(|moves| moves.crossed_nodes(slot))
        {
            skipped_foreign = Some(skipped_foreign.map_or(slot, |known| known.min(slot)));
        }
    };

    // Entries go from the decoder straight into the store, so recovery holds one
    // shard's file and its store, not a second copy of the data.
    if let Some(plan) = &plan {
        plan.stream_shard(shard_id, |entry| {
            if slot_filter
                .as_mut()
                .is_some_and(|filter| !filter.keeps_entry(&entry.key))
            {
                note_foreign(&entry.key);
                return Ok(());
            }

            store
                .restore_snapshot_entry(entry)
                .map_err(|error| crate::persistence::PersistenceError::Corruption(error.into()))
        })
        .map_err(|error| io::Error::other(error.to_string()))?;

        if let Some(hot) = &hot_store
            && plan.last_sequence(hot_id).is_some()
        {
            let mut entries = Vec::new();

            plan.stream_shard(hot_id, |entry| {
                entries.push(entry);
                Ok(())
            })
            .map_err(|error| io::Error::other(error.to_string()))?;
            hot.restore(entries);
        }

        // The file buffer is gone; hand its pages back instead of keeping them as RSS.
        crate::memory::release_free_pages();
    }

    // A replica waiting for a full resync (for example after a frame it could not apply,
    // which is already in its log) serves no data and replaces it all from the primary's
    // copy: a record that does not replay must not keep it from starting.
    let resync_pending = config.replication.role == crate::config::ReplicationRole::Replica
        && marker == Some(CatchupMarker::Preparing);
    let replayed = mutations.len();
    // Records are contiguous: the first replayed one follows the checkpoint.
    let first_replayed = report.last_sequence + 1 - report.records_replayed;

    // Each record is dropped once applied and the list with the loop, so after replay the
    // shard holds its store only, not a second copy of everything since the checkpoint.
    for (index, (database, mutation)) in mutations.into_iter().enumerate() {
        if database == 0
            && hot_store.as_ref().is_some_and(|store| {
                crate::hot_store::apply_owned(store, &config.hot_keys, &mutation)
            })
        {
            continue;
        }

        if let Some(filter) = slot_filter.as_mut()
            && !filter.keeps_record(first_replayed + index as u64, &mutation, &mut |slots| {
                store.drop_slots(slots);
            })
        {
            mutation.for_each_write_key(&mut |key| note_foreign(key));
            continue;
        }

        if let Err(error) = apply_mutation(&mut store, database, &mutation) {
            if !resync_pending {
                return Err(io::Error::other(error.to_string()));
            }
            warn!(
                shard_id,
                %error,
                skipped = replayed - index,
                "replica log does not replay; leaving the rest to the pending full resync"
            );
            break;
        }
    }
    if replayed != 0 {
        crate::memory::release_free_pages();
    }
    if proven_complete {
        sink.flush()
            .map_err(|error| io::Error::other(error.to_string()))?;
        replica_guard::clear(&config.persistence.data_dir, shard_id)?;
        incomplete = false;
    }
    if let Some(stale) = slot_filter.and_then(crate::slot_moves::ReplayFilter::stale_slots) {
        let dropped = store.drop_slots(&stale);

        info!(
            shard_id,
            dropped, "dropped keys of slots another shard owns"
        );
    }
    // A node of a multi-node cluster must not hold keys the map serves elsewhere: they
    // would be unreachable here and missing there. One walk, only with a map.
    if let Some(slot) = skipped_foreign {
        return Err(io::Error::other(format!(
            "shard {shard_id} recorded keys in slots another group serves under \
             cluster_topology (first: slot {slot}); start with a map that gives this group \
             those slots, or with an empty data directory"
        )));
    }
    if let Some(route) = route.as_ref().filter(|route| route.topology.is_some()) {
        let foreign: Vec<bool> = (0..crate::routing::HASH_SLOT_COUNT as u16)
            .map(|slot| {
                let owner = route.slots.owner(slot);

                // A frozen range (a prepared move to another node) is still held here.
                owner != shard_id as usize
                    && owner != crate::routing::FROZEN_BASE + shard_id as usize
                    && !arriving.as_ref().is_some_and(|range| range.contains(&slot))
            })
            .collect();

        if let (count @ 1.., Some(slot)) = store.count_in_slots(&foreign) {
            return Err(io::Error::other(format!(
                "shard {shard_id} holds {count} keys in slots it does not serve under \
                 cluster_topology (first: slot {slot}; expired keys count too); start with a \
                 map that gives this group those slots, or with an empty data directory"
            )));
        }
    }
    store.resume_expiration_after_replay(replay_clock);
    store.json_index_load_definitions();
    // Replay keeps expired keys, so each one expiry removes is logged as a delete before
    // a later write could reuse its name. A replica's log is the primary's.
    store.log_removals(
        config.persistence.profile.uses_wal()
            && config.replication.role != crate::config::ReplicationRole::Replica,
    );
    if !incomplete || force {
        // In bounded batches, each logged as one group, so a mass expiry at startup costs
        // a sync per batch rather than one per key, and the queue never holds every key.
        loop {
            let reaped = store.expire_cycle(STARTUP_EXPIRE_BATCH);

            log_expired_removals(&mut store, &mut sink);
            if reaped < STARTUP_EXPIRE_BATCH || store.has_unlogged_removals() {
                break;
            }
        }
    }

    let tls = TlsAcceptor::from_security(&config.security)?;
    let snapshot_requests = cluster.as_ref().map_or_else(
        || Arc::new(AtomicU64::new(0)),
        |cluster| cluster.snapshot_requests(),
    );
    let state = Rc::new(ShardState {
        store: RefCell::new(store),
        command_metrics: RefCell::new(CommandMetrics::default()),
        latency: RefCell::new(crate::latency::LatencyTable::default()),
        key_stats: RefCell::new(crate::keystats::KeyStats::default()),
        persistence: Arc::new(Mutex::new(sink)),
        replica_frames: RefCell::new(None),
        replica_reader: RefCell::new(None),
        replica_wake: Arc::new(tokio::sync::Notify::new()),
        replica_link: Arc::new(crate::replication::LinkStatus::default()),
        replica_persist_warned: Cell::new(None),
        role: Cell::new(config.replication.role),
        followed: RefCell::new(None),
        replication: RefCell::new(None),
        write_gate: Cell::new(match config.replication.role {
            crate::config::ReplicationRole::Replica => crate::replication::WriteGate::Replica,
            // Closed until a majority of the coordinators grant the lease.
            crate::config::ReplicationRole::Primary if config.replication.primary_lease => {
                crate::replication::WriteGate::NoLease
            }
            _ => crate::replication::WriteGate::Open,
        }),
        epoch: Cell::new(0),
        newer_epoch: Arc::new(AtomicU64::new(0)),
        replica_resync_sequence: Arc::new(AtomicU64::new(if incomplete && !force {
            u64::MAX
        } else {
            0
        })),
        replica_catchup_through: Cell::new(0),
        replica_incomplete: Cell::new(incomplete && !force),
        replica_caught_up: Cell::new(false),
        catchup_wait_started: Cell::new(Instant::now()),
        snapshot_requests,
        snapshot_request_seen: Cell::new(0),
        last_requested_checkpoint_at: Cell::new(Instant::now() - Duration::from_secs(1)),
        recovery: report,
        security: config.security.clone(),
        checkpoints,
        checkpoint_generation: Cell::new(checkpoint_generation),
        last_checkpoint_at: Cell::new(Instant::now()),
        mutations_since_checkpoint: Cell::new(0),
        checkpoint_job: RefCell::new(None),
        checkpoint_ladder: Cell::new(checkpoint::SliceLadder::START),
        checkpoint_overload_skips: Cell::new(0),
        checkpoint_retry_at: Cell::new(Instant::now()),
        checkpoint_failure_warned: RefCell::new(None),
        carry: RefCell::new(None),
        carry_failure_warned: RefCell::new(None),
        bgsave_pending: Cell::new(false),
        waiters: RefCell::new(ListWaiters::new(DEFAULT_MAX_LIST_WAITERS)),
        pubsub: RefCell::new(crate::pubsub::PubSubHub::new()),
        tracker: RefCell::new(crate::tracking::KeyTracker::default()),
        subscribers: RefCell::new(HashMap::new()),
        clients: RefCell::new(HashMap::new()),
        slowlog: RefCell::new(crate::slowlog::SlowLog::default()),
        published_config: config.clone(),
        live: live::LiveConfig::new(&config),
        limiter: ConnectionLimiter::new(config.connections),
        next_conn_id: Cell::new(1),
        scripts: RefCell::new(crate::script::ScriptCache::with_payload_limits(
            config.protocol,
        )),
        active_connections: Cell::new(0),
        retry_blockers: Cell::new(0),
        peak_input_buffer_bytes: Cell::new(0),
        peak_output_buffer_bytes: Cell::new(0),
        peak_pubsub_queue_frames: Rc::new(Cell::new(0)),
        peak_pubsub_queue_bytes: Rc::new(Cell::new(0)),
        tls,
        single_port: std_listener.is_none() && fabric.is_some(),
        primary_lease: fabric
            .as_ref()
            .map_or_else(Arc::default, |fabric| Arc::clone(&fabric.lease)),
        forwarding: fabric.map(|fabric| Forwarding::new(fabric, shard_id as usize)),
        route,
        cluster,
        last_submitted_sequence: Cell::new(0),
        compacted_generation: Cell::new(0),
        submitted_coverage: RefCell::new(std::collections::VecDeque::new()),
        hot_store,
        publish_fanout: Arc::new(crate::replication::PublishFanout::new()),
        durable_acks: Arc::new(crate::replication::DurableAcks::new()),
        durable_replies: Arc::default(),
    });

    // A replica that starts with an unfinished copy acknowledges nothing until it has one.
    state
        .replica_link
        .set_copying(state.replica_incomplete.get());

    if let Some(coordinator) = &state.cluster {
        coordinator.register_wal(shard_id, &state.persistence);
    }

    let listener = match std_listener {
        Some(std_listener) => {
            std_listener.set_nonblocking(true)?;
            Some(TcpListener::from_std(std_listener)?)
        }
        None => None,
    };
    let address = match &listener {
        Some(listener) => listener.local_addr()?,
        None => config
            .address
            .parse::<SocketAddr>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
    };

    if let Some(forwarding) = state.forwarding.as_ref() {
        forwarding.fabric.register_durability(
            forwarding.me,
            ShardDurability {
                wal: Arc::clone(&state.persistence),
                acks: Arc::clone(&state.durable_acks),
                replies: Arc::clone(&state.durable_replies),
            },
        );
        tokio::task::spawn_local(remote_loop(Rc::clone(&state), shutdown.clone()));
    }

    info!(
        backend = "evented",
        shard_id,
        shard_count = expected_shards,
        %address,
        durability = %state.persistence.lock().unwrap_or_else(|error| error.into_inner()).profile(),
        recovered_records = state.recovery.records_replayed,
        "server listening"
    );
    let limits = config.protocol;
    let shutdown_grace = config.shutdown_grace;

    *state.replication.borrow_mut() = attach_replication(&state, shard_id)?;
    // A map installed just before a stop may have given ranges away before the loss was
    // recorded: record it now, before this shard serves.
    if state
        .route
        .as_ref()
        .is_some_and(|route| route.topology.is_some())
    {
        migrate::settle(&state);
    }
    let mut maintenance = tokio::time::interval(Duration::from_millis(100));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    maintenance.tick().await;
    let mut expiry_maintenance = tokio::time::interval(EXPIRE_MAINTENANCE_INTERVAL);
    expiry_maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    expiry_maintenance.tick().await;

    let mut checkpoint_pace = tokio::time::interval(Duration::from_millis(2));
    checkpoint_pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut accept_warnings = crate::admission::WarnLimiter::new(Duration::from_secs(1));

    loop {
        drain_replicated(&state);

        if shutdown.is_requested() {
            break;
        }

        tokio::select! {
            accepted = next_client(listener.as_ref(), state.forwarding.as_ref()) => {
                let (stream, peer, handed) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        // Descriptor exhaustion and aborted handshakes must not take the
                        // server down; back off and keep serving the clients already here.
                        if !matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) {
                            if accept_warnings.allow(Instant::now()) {
                                warn!(
                                    %error,
                                    suppressed = accept_warnings.take_suppressed(),
                                    "accept failed"
                                );
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }

                        continue;
                    }
                };
                // Connection settings are read per accept: `CONFIG SET` changes them for new
                // connections only.
                let (timeouts, request_limits) =
                    state.live.with(|live| (live.timeouts, live.requests));
                let permit = match handed {
                    Some(permit) => permit,
                    None => {
                        let Some(permit) = state.limiter.try_acquire(peer.ip()) else {
                            let mut stream = stream;
                            let _ = timeout(
                                timeouts.write,
                                stream.write_all(b"-ERR max number of clients reached\r\n"),
                            )
                            .await;
                            continue;
                        };
                        permit
                    }
                };

                let state = Rc::clone(&state);
                let client_shutdown = shutdown.clone();

                tokio::task::spawn_local(async move {
                    if let Err(error) = handle_client(
                        stream,
                        state,
                        limits,
                        timeouts,
                        request_limits,
                        client_shutdown,
                        permit,
                        peer.ip(),
                    )
                    .await
                    {
                        if error.kind() == io::ErrorKind::ConnectionAborted {
                            tracing::debug!(%peer, %error, "connection closed");
                        } else {
                            warn!(%peer, %error, "connection failed");
                        }
                    }
                });
            },
            _ = expiry_maintenance.tick() => {
                if state.replica_incomplete.get() {
                    continue;
                }
                let maintenance_keys =
                    state.live.with(|live| live.memory.active_expire_maintenance_keys);
                let mut store = state.store.borrow_mut();
                let reaped = expire_maintenance(
                    &mut store,
                    maintenance_keys,
                    EXPIRE_MAINTENANCE_TIME,
                    EXPIRE_MAINTENANCE_MAX_KEYS,
                );
                // A slice that ended on its time limit with work left comes back as soon as
                // requests have had their turn, so a mass expiry clears in seconds.
                let backlog = reaped >= maintenance_keys
                    && store.soonest_deadline_due();
                // A write admitted before eviction made room for it left a debt: evict the
                // rest in slices, so no single request stalls the shard.
                if store.has_unlogged_removals() {
                    let mut persistence = state
                        .persistence
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());

                    log_expired_removals(&mut store, &mut persistence);
                }
                let owed = store.eviction_debt() && pay_eviction_debt(&state, &mut store);
                let migrated = store.migrate_indexes(EXPIRE_MAINTENANCE_TIME);
                let compacted = store.step_collection_work(EXPIRE_MAINTENANCE_TIME);
                let reclaimed = !migrated || store.reclaim_logs(EXPIRE_MAINTENANCE_TIME);
                drop(store);
                flush_key_notices(&state);

                if backlog || owed || !migrated || !compacted || !reclaimed {
                    // A resize, a compaction or a log drain round is unfinished: come back
                    // as soon as the other ready work has had its turn.
                    expiry_maintenance.reset_immediately();
                }
            },
            _ = state.replica_wake.notified(), if state.replica_frames.borrow().is_some() => {
                // The drain at the top of the loop applies what arrived. Yield first so a
                // replica under a steady stream still serves its clients between budgets.
                tokio::task::yield_now().await;
            },
            _ = checkpoint_pace.tick(), if checkpoint_running(&state) => {
                checkpoint_slice(&state);
            },
            _ = maintenance.tick() => {
                fence_if_replaced(&state);
                gate_on_replicas(&state);
                let mut store = state.store.borrow_mut();

                if !state.replica_incomplete.get() {
                    store.expire_hash_fields_due();
                }
                store.collect_freed_pages();
                store.set_aux_memory(
                    0,
                    0,
                    state
                        .persistence
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .dirty_bytes() as usize,
                );
                drop(store);
                flush_key_notices(&state);
                expire_waiters(&state);
                #[cfg(test)]
                note_parked_waiters(
                    &state,
                    Some(state.waiters.borrow().len() + state.retry_blockers.get()),
                );

                {
                    let mut wal = state
                        .persistence
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());

                    // A turn's hold is released by its first waiter; one whose waiters
                    // all went away is released here.
                    if let Err(error) = wal.release_tick_hold().and_then(|()| wal.sync_if_due()) {
                        let (wal_last_sequence, wal_unsynced_bytes) = wal.log_fields();

                        warn!(
                            %error,
                            wal_last_sequence,
                            wal_unsynced_bytes,
                            "periodic WAL sync failed"
                        );
                    }
                }

                if state.replica_caught_up.get()
                    && let Err(error) = finish_replica_catchup(&state) {
                        warn!(%error, "replica catch-up remains incomplete");
                    }

                checkpoint_start_if_due(&state);
            }
        }
    }

    #[cfg(test)]
    note_parked_waiters(&state, None);
    let deadline = Instant::now() + shutdown_grace;

    while state.active_connections.get() != 0 && Instant::now() < deadline {
        // Every connection waits on its outbound queue between requests; closing it wakes
        // an idle one, which then sees the shutdown. A connection running a command
        // finishes it first. Nothing in the request loop polls for shutdown.
        for outbound in state.subscribers.borrow().values() {
            outbound.close();
        }
        // Blocked clients sleep until something wakes them: give each its timeout reply.
        let parked = state.waiters.borrow_mut().drain_all();

        for (slot, timeout) in parked {
            slot.fill(timeout);
        }
        state.store.borrow_mut().wake_all_watchers();

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let flushed = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .flush_for_shutdown();

    if let Err(error) = &flushed {
        warn!(%error, "final WAL flush failed; writing a final checkpoint instead");
    }

    // Even with a failed log the checkpoint can still save the data held in memory. It is
    // tried once more: the cause may have passed (space freed, a transient I/O error).
    // Not under `always` with a failed log: it refuses every checkpoint until restart (the
    // first attempt already handed the final round a copy of the last published file).
    let log_refuses = flushed.is_err()
        && state.published_config.persistence.profile == crate::DurabilityProfile::Always;
    let saved = publish_checkpoint(&state).or_else(|error| {
        if log_refuses {
            return Err(error);
        }
        warn!(%error, "final checkpoint failed; trying once more");
        publish_checkpoint(&state)
    });

    if let Err(error) = saved {
        // Without a log that replays them, writes since the last published checkpoint are
        // lost: say so and end with an error rather than as a clean save. Under `always`
        // every acknowledged write was synced to the log before its reply, so a log that
        // failed since holds them all; only refused writes are left out.
        let profile = state.published_config.persistence.profile;
        let log_keeps_acknowledged =
            profile == crate::DurabilityProfile::Always || (flushed.is_ok() && profile.uses_wal());

        if !log_keeps_acknowledged {
            error!(
                %error,
                "final checkpoint failed; writes since the last checkpoint were not saved"
            );

            return Err(io::Error::other(format!(
                "final checkpoint failed: {error}"
            )));
        }

        if let Err(wal_error) = &flushed {
            // No write is lost, but the disk refused this shard's log: end with an error
            // so a supervisor sees the fault instead of a clean save.
            error!(
                %error,
                %wal_error,
                "WAL failed; final checkpoint not written (the WAL keeps every acknowledged write)"
            );

            return Err(io::Error::other(format!(
                "WAL failed before shutdown: {wal_error}"
            )));
        }

        // Only the checkpoint failed: the log is healthy and synced, so the next start
        // replays it and nothing is lost.
        warn!(%error, "final checkpoint failed; the WAL keeps every acknowledged write");
    }

    drop(state.replication.borrow_mut().take());
    Ok(())
}
