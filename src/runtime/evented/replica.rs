use super::*;

use std::{borrow::Cow, sync::atomic::AtomicBool};

pub(super) fn attach_replication(
    state: &ShardState,
    shard_id: u32,
) -> io::Result<Option<crate::replication::ReplicationThread>> {
    match state.role.get() {
        crate::config::ReplicationRole::Off => {
            // A standalone shard that never followed a primary has nothing to fork.
            if state
                .published_config
                .persistence
                .data_dir
                .join(crate::replication::upstream_file_name(shard_id))
                .exists()
            {
                writer_lineage(state, shard_id)?;
            }

            Ok(None)
        }
        crate::config::ReplicationRole::Primary => {
            let lineage = writer_lineage(state, shard_id)?;

            state.epoch.set(lineage.origin.epoch);
            raise_fence(state);
            spawn_shipper(state, shard_id, lineage)
        }

        crate::config::ReplicationRole::Replica => {
            let Some(primary) = state.published_config.replication.primary.or_else(|| {
                state
                    .published_config
                    .replication
                    .primary_name
                    .as_ref()
                    .map(|(_, port)| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), *port))
            }) else {
                return Ok(None);
            };

            spawn_reader(
                state,
                shard_id,
                primary,
                state.published_config.replication.primary_name.clone(),
            )
            .map(Some)
        }
    }
}

/// Starts shipping this primary shard's log to replicas on `replication.listen`; none when
/// no listen address is configured.
fn spawn_shipper(
    state: &ShardState,
    shard_id: u32,
    lineage: crate::replication::Lineage,
) -> io::Result<Option<crate::replication::ReplicationThread>> {
    let Some(listen) = state.published_config.replication.listen else {
        return Ok(None);
    };
    let (published, backlog_floor, replica_count) = {
        let wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        (wal.durable(), wal.backlog_floor(), wal.replica_count())
    };

    crate::replication::spawn_primary(crate::replication::Shipment {
        listen: shard_port(listen, shard_id)?,
        shard_id,
        published,
        data_dir: state.published_config.persistence.data_dir.clone(),
        requirepass: state
            .published_config
            .security
            .replication_secret()
            .to_vec(),
        limits: state.published_config.protocol,
        max_replicas: crate::replication::MAX_REPLICAS,
        backlog_floor,
        replica_count,
        acks: Arc::clone(&state.durable_acks),
        fanout: Arc::clone(&state.publish_fanout),
        snapshot_max_bytes: state.published_config.persistence.snapshot_max_bytes,
        shard_count: state.published_config.shards as u32,
        lineage,
        newer_epoch: Arc::clone(&state.newer_epoch),
        snapshot_requests: Arc::clone(&state.snapshot_requests),
        announce: client_endpoint(&state.published_config, shard_id),
        tls: replication_acceptor(state)?,
        durable_replies: Arc::clone(&state.durable_replies),
    })
    .map(Some)
}

/// Where clients reach shard `shard_id` of this node: `advertised-host`, and
/// `advertised-port` (else the bind port) plus the shard index when every shard has its own
/// port, as `CLUSTER SLOTS` announces it.
pub(super) fn client_endpoint(
    config: &crate::config::AppConfig,
    shard_id: u32,
) -> Option<crate::replication::Endpoint> {
    let bind: SocketAddr = config.address.parse().ok()?;
    let base = config.advertised_port.unwrap_or(bind.port());
    let port = if config.shards > 1 && config.port_mode != crate::config::PortMode::Single {
        base.checked_add(u16::try_from(shard_id).ok()?)?
    } else {
        base
    };

    crate::replication::announceable_host(&config.advertised_host).then(|| {
        crate::replication::Endpoint {
            host: config.advertised_host.clone(),
            port,
        }
    })
}

fn replication_acceptor(state: &ShardState) -> io::Result<Option<Arc<crate::tls::TlsAcceptor>>> {
    let config = &state.published_config;

    if !config.replication.tls {
        return Ok(None);
    }

    Ok(crate::tls::TlsAcceptor::from_security(&config.security)?.map(Arc::new))
}

fn replication_client(state: &ShardState) -> io::Result<Option<Arc<crate::tls::ReplicaTlsClient>>> {
    let replication = &state.published_config.replication;

    if !replication.tls {
        return Ok(None);
    }

    let pair = (!replication.tls_client_cert.as_os_str().is_empty()).then_some((
        replication.tls_client_cert.as_path(),
        replication.tls_client_key.as_path(),
    ));

    crate::tls::ReplicaTlsClient::load(&replication.tls_ca, pair)
        .map(|client| Some(Arc::new(client)))
}

/// Starts following `primary` (the base address; this shard dials its own port).
fn spawn_reader(
    state: &ShardState,
    shard_id: u32,
    primary: SocketAddr,
    primary_name: Option<(String, u16)>,
) -> io::Result<crate::replication::ReplicationThread> {
    let (inbound, outbound) = mpsc::sync_channel(crate::replication::QUEUE_FRAMES);
    let dialed = shard_port(primary, shard_id)?;

    // Until the new primary accepts this shard, it names no primary in the cluster answers.
    state.durable_acks.set_primary(None);
    *state.followed.borrow_mut() = Some(match &primary_name {
        Some((host, _)) => (host.clone(), dialed.port()),
        None => (dialed.ip().to_string(), dialed.port()),
    });
    let thread = crate::replication::spawn_replica_reader(crate::replication::ReplicaLink {
        primary: dialed,
        primary_name,
        shard_id,
        wal: Arc::clone(&state.persistence),
        inbound,
        password: state
            .published_config
            .security
            .replication_secret()
            .to_vec(),
        limits: state.published_config.protocol,
        snapshot_max_bytes: state.published_config.persistence.snapshot_max_bytes,
        resync_sequence: Arc::clone(&state.replica_resync_sequence),
        data_dir: state.published_config.persistence.data_dir.clone(),
        shard_count: state.published_config.shards as u32,
        status: Arc::clone(&state.replica_link),
        wake_shard: Arc::clone(&state.replica_wake),
        announce: client_endpoint(&state.published_config, shard_id)
            .filter(|_| state.published_config.replication.announced),
        peers: Arc::clone(&state.durable_acks),
        tls: replication_client(state)?,
    });

    *state.replica_frames.borrow_mut() = Some(outbound);
    *state.replica_reader.borrow_mut() = thread.reader();
    Ok(thread)
}

/// `REPLICAOF host port` or `REPLICAOF NO ONE [FORCE]` for the whole process: checked here,
/// applied on this shard, then on every other shard on its own thread; `OK` once all have.
pub(super) async fn replicaof(
    state: &ShardState,
    primary: Option<(&[u8], &[u8])>,
    force: bool,
) -> Response<'static> {
    if !state.published_config.persistence.profile.uses_wal() {
        return Response::Error(b"ERR replication requires a WAL-backed durability profile");
    }

    let target = match primary {
        None => None,
        Some((host, port)) => {
            let text = format!(
                "{}:{}",
                String::from_utf8_lossy(host),
                String::from_utf8_lossy(port)
            );

            match crate::config::parse_replication_primary(&text) {
                Ok(parsed) => {
                    let base = parsed.socket.unwrap_or_else(|| {
                        let port = parsed.name.as_ref().map_or(0, |(_, port)| *port);

                        SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), port)
                    });

                    Some((base, parsed.name))
                }
                Err(error) => return Response::OwnedError(format!("ERR {error}").into_bytes()),
            }
        }
    };

    if target.is_none() && !force {
        let data_dir = &state.published_config.persistence.data_dir;

        for shard in 0..state.published_config.shards as u32 {
            if !matches!(replica_guard::read(data_dir, shard), Ok(None)) {
                return Response::Error(PROMOTE_INCOMPLETE);
            }
        }
    }

    let change = RoleChange {
        target,
        force,
        failed: AtomicBool::new(false),
    };

    change.apply(state);
    let change = if let Some(forwarding) = state.forwarding.as_ref() {
        let change = Arc::new(change);

        forwarding
            .apply_everywhere(Box::new(SharedRoleChange(Arc::clone(&change))))
            .await;
        change
    } else {
        Arc::new(change)
    };

    if change.failed.load(Ordering::Acquire) {
        Response::Error(b"ERR REPLICAOF did not complete on every shard; see the server log")
    } else {
        Response::Simple(b"OK")
    }
}

/// One `REPLICAOF` for every shard.
struct RoleChange {
    target: Option<(SocketAddr, Option<(String, u16)>)>,
    force: bool,
    failed: AtomicBool,
}

impl RoleChange {
    fn apply(&self, state: &ShardState) {
        let shard_id = state
            .route
            .as_ref()
            .map_or(0, |route| route.shard_id as u32);
        let result = match &self.target {
            None => promote(state, shard_id, self.force),
            Some((primary, name)) => demote(state, shard_id, *primary, name.clone()),
        };

        if let Err(error) = result {
            warn!(shard_id, %error, "REPLICAOF failed on this shard");
            self.failed.store(true, Ordering::Release);
        }
    }
}

struct SharedRoleChange(Arc<RoleChange>);

impl super::forward::ShardChange for SharedRoleChange {
    fn apply(&self, state: &ShardState) {
        self.0.apply(state);
    }
}

/// `ROLE`, as Redis answers it, for this shard: a primary (or a server that does not
/// replicate) answers `master`, its log sequence and no replica list; a replica answers
/// `slave`, the primary it dials, the link state and the sequence it has synced.
pub(super) fn role_response(state: &ShardState) -> Response<'static> {
    let (last, durable) = {
        let wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        (wal.last_sequence(), wal.durable().load(Ordering::Acquire))
    };
    let bulk = |bytes: Vec<u8>| Response::Bulk(Some(Cow::Owned(bytes)));

    if state.role.get() != crate::config::ReplicationRole::Replica {
        // Redis's shape: host, port and acknowledged offset of each replica, as strings.
        let replicas = state
            .durable_acks
            .replicas()
            .into_iter()
            .map(|replica| {
                Response::Exec(vec![
                    bulk(replica.endpoint.host.into_bytes()),
                    bulk(replica.endpoint.port.to_string().into_bytes()),
                    bulk(replica.sequence.to_string().into_bytes()),
                ])
            })
            .collect();

        return Response::Exec(vec![
            bulk(b"master".to_vec()),
            Response::Integer(last as i64),
            Response::Exec(replicas),
        ]);
    }

    let (host, port) = state
        .followed
        .borrow()
        .clone()
        .unwrap_or_else(|| (String::new(), 0));
    let link = if state.replica_link.is_up() {
        &b"connected"[..]
    } else {
        &b"connect"[..]
    };

    Response::Exec(vec![
        bulk(b"slave".to_vec()),
        bulk(host.into_bytes()),
        Response::Integer(i64::from(port)),
        bulk(link.to_vec()),
        Response::Integer(durable as i64),
    ])
}

/// Why `REPLICAOF NO ONE` refuses a shard.
pub(super) const PROMOTE_INCOMPLETE: &[u8] = b"ERR this replica has not finished copying its \
    primary; REPLICAOF NO ONE FORCE serves it anyway and may lose writes";

/// `REPLICAOF NO ONE` on this shard: stop following, apply what already arrived, fork a
/// history of its own (see `writer_lineage`) and take writes; ship to replicas when
/// `replication.listen` is set. A shard that is already a writer is left as it is.
pub(super) fn promote(state: &ShardState, shard_id: u32, force: bool) -> io::Result<()> {
    if state.role.get() != crate::config::ReplicationRole::Replica {
        return Ok(());
    }

    // Dropping the reader joins its thread; frames it queued are applied before the fork.
    drop(state.replication.borrow_mut().take());
    *state.replica_reader.borrow_mut() = None;
    while drain_replicated_budget(state, REPLICA_DRAIN_BUDGET) > 0 {}
    *state.replica_frames.borrow_mut() = None;
    // A full copy queued and dropped here never runs, so whether this shard still copies is
    // what its incomplete flag says, not what the reader last announced.
    state
        .replica_link
        .set_copying(state.replica_incomplete.get());
    state.replica_link.mark_down();

    if state.replica_incomplete.get() {
        if !force {
            return Err(io::Error::other("incomplete replica"));
        }
        warn!(
            shard_id,
            "promoting an incomplete replica: writes it never received are lost"
        );
        state.replica_incomplete.set(false);
        state.replica_link.set_copying(false);
    }

    // A primary lists its own replicas, not the one it used to follow.
    state.durable_acks.set_primary(None);
    let lineage = writer_lineage(state, shard_id)?;
    let uses_wal = state.published_config.persistence.profile.uses_wal();
    {
        let mut store = state.store.borrow_mut();

        store.follow_primary(false);
        store.log_removals(uses_wal);
    }
    if let Some(hot) = &state.hot_store {
        hot.keep_expired_keys(false);
    }

    state.epoch.set(lineage.origin.epoch);
    raise_fence(state);
    let shipper = spawn_shipper(state, shard_id, lineage)?;
    state.role.set(if shipper.is_some() {
        crate::config::ReplicationRole::Primary
    } else {
        crate::config::ReplicationRole::Off
    });
    *state.replication.borrow_mut() = shipper;
    state.write_gate.set(crate::replication::WriteGate::Open);
    warn!(
        shard_id,
        epoch = lineage.origin.epoch,
        fork = lineage.fork_sequence,
        "promoted to primary"
    );
    // A range the map gave to another group while this shard followed is recorded (and
    // logged for this shard's own replicas) now that it writes.
    if let Some(forwarding) = state.forwarding.as_ref()
        && forwarding.fabric.topology().is_some()
    {
        forwarding.fabric.send(
            forwarding.me,
            forwarding.me,
            Message::Move(super::moveslots::MoveMessage::Settle),
        );
    }
    Ok(())
}

/// `REPLICAOF host port` on this shard: stop taking writes and shipping, give up the
/// history it wrote and follow `primary`. The shard resumes from its own log when the new
/// primary's history continues its own (it was promoted from this shard's replica and this
/// shard wrote nothing past the fork); otherwise it takes a full copy.
pub(super) fn demote(
    state: &ShardState,
    shard_id: u32,
    primary: SocketAddr,
    primary_name: Option<(String, u16)>,
) -> io::Result<()> {
    state.write_gate.set(crate::replication::WriteGate::Replica);
    drop(state.replication.borrow_mut().take());
    *state.replica_reader.borrow_mut() = None;
    *state.replica_frames.borrow_mut() = None;
    // A full copy queued and dropped here never runs, so whether this shard still copies is
    // what its incomplete flag says, not what the reader last announced.
    state
        .replica_link
        .set_copying(state.replica_incomplete.get());

    let data_dir = &state.published_config.persistence.data_dir;

    // The history this shard wrote becomes the one it claims to follow: a primary forked
    // from it lets the log resume. Recording it removes the shard's own lineage.
    if state.role.get() != crate::config::ReplicationRole::Replica {
        let own = writer_lineage(state, shard_id)?.origin;

        crate::replication::record_upstream_history(data_dir, shard_id, &own)?;
    }

    {
        let mut store = state.store.borrow_mut();

        store.follow_primary(true);
        store.log_removals(false);
    }
    if let Some(hot) = &state.hot_store {
        hot.keep_expired_keys(true);
    }

    state.role.set(crate::config::ReplicationRole::Replica);
    state.epoch.set(0);
    *state.replication.borrow_mut() = Some(spawn_reader(state, shard_id, primary, primary_name)?);
    warn!(shard_id, %primary, "following a new primary");
    Ok(())
}

/// Shard `shard_id`'s replication address: `base`'s port plus the shard id. A port past
/// 65535 is an error; wrapping around would bind or dial a port nobody configured.
/// Stops a primary shard taking writes once a replica has shown it a newer epoch: another
/// primary replaced this one. It stays fenced until the process restarts or changes role.
/// Raises the generation of new `LEASE` and `SEMAPHORE` tokens to this shard's epoch or the
/// cluster map's version, whichever is higher. A coordinator's map version grows with every
/// failover and every committed move, so tokens keep growing when a range changes groups.
pub(super) fn raise_fence(state: &ShardState) {
    let map = state
        .forwarding
        .as_ref()
        .and_then(|forwarding| forwarding.fabric.topology())
        .map_or(0, |shared| shared.get().version);

    state
        .store
        .borrow_mut()
        .set_fence_generation(state.epoch.get().max(map));
}

pub(super) fn fence_if_replaced(state: &ShardState) {
    if state.write_gate.get() == crate::replication::WriteGate::Open
        && state.newer_epoch.load(Ordering::Acquire) > state.epoch.get()
    {
        warn!(
            epoch = state.epoch.get(),
            newer_epoch = state.newer_epoch.load(Ordering::Acquire),
            "a newer primary replaced this one; refusing writes"
        );
        state.write_gate.set(crate::replication::WriteGate::Fenced);
    }
}

/// `min-replicas-to-write`: a primary shard refuses writes while fewer replicas than that
/// acknowledged within `min-replicas-max-lag`, and takes them again once enough do. Checked
/// on the maintenance tick, so the write path reads only the gate.
pub(super) fn gate_on_replicas(state: &ShardState) {
    use crate::replication::WriteGate;

    let gate = state.write_gate.get();

    if !matches!(gate, WriteGate::Open | WriteGate::NoReplicas)
        || state.role.get() != crate::config::ReplicationRole::Primary
    {
        return;
    }

    let (wanted, max_lag) = state.live.with(|live| {
        (
            live.replication.min_replicas_to_write,
            live.replication.min_replicas_max_lag,
        )
    });
    let next = if wanted == 0 || state.durable_acks.fresh(max_lag) >= wanted {
        WriteGate::Open
    } else {
        WriteGate::NoReplicas
    };

    if next != gate {
        warn!(
            wanted,
            max_lag_secs = max_lag.as_secs(),
            "{}",
            if next == WriteGate::Open {
                "enough replicas acknowledge again; taking writes"
            } else {
                "too few replicas acknowledge; refusing writes (min-replicas-to-write)"
            }
        );
        state.write_gate.set(next);
    }
}

/// The lineage this shard writes as a writer, settled before it accepts its first write: a
/// shard promoted from replica forks here, at its last logged sequence.
fn writer_lineage(state: &ShardState, shard_id: u32) -> io::Result<crate::replication::Lineage> {
    let data_dir = &state.published_config.persistence.data_dir;
    let last_sequence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .last_sequence();

    crate::replication::resolve_lineage(data_dir, shard_id, last_sequence)
}

pub(super) fn shard_port(base: SocketAddr, shard_id: u32) -> io::Result<SocketAddr> {
    u16::try_from(shard_id)
        .ok()
        .and_then(|offset| base.port().checked_add(offset))
        .map(|port| SocketAddr::new(base.ip(), port))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "replication port {} plus shard {shard_id} exceeds 65535",
                    base.port()
                ),
            )
        })
}

pub(super) fn install_replica_snapshot(
    state: &ShardState,
    shard: &[u8],
    hot: Option<&[u8]>,
    catchup_through: u64,
    history: &crate::replication::Origin,
) -> bool {
    let limits = state.published_config.protocol;
    let maximum = state.published_config.persistence.snapshot_max_bytes;
    let Ok(shard_checkpoint) = crate::persistence::decode_snapshot_blob(shard, limits, maximum)
    else {
        return false;
    };

    let hot_checkpoint = match hot {
        Some(bytes) => {
            let Ok(checkpoint) = crate::persistence::decode_snapshot_blob(bytes, limits, maximum)
            else {
                return false;
            };

            Some(checkpoint)
        }

        None => None,
    };

    let mut fresh = KvShard::with_clock(SystemClock::with_floor(
        state.store.borrow().now_unix_millis(),
    ));

    fresh.apply_memory_limits(state.live.with(AppConfig::shard_memory_limits));
    fresh.follow_primary(state.store.borrow().follows_primary());
    let replay_clock = fresh.pause_expiration_for_replay();

    let memory_entries = match crate::persistence::decode_snapshot_blob(shard, limits, maximum) {
        Ok(checkpoint) => checkpoint.entries,
        Err(_) => return false,
    };

    for entry in memory_entries {
        if fresh.restore_snapshot_entry(entry).is_err() {
            return false;
        }
    }
    fresh.resume_expiration_after_replay(replay_clock);

    let sequence = shard_checkpoint.last_sequence;
    let requested = state.replica_resync_sequence.load(Ordering::Acquire);
    if catchup_through < sequence
        || (requested != 0 && requested != u64::MAX && sequence < requested)
    {
        return false;
    }
    abandon_checkpoint_for_install(state);
    let mut published = vec![shard_checkpoint];

    if let Some(hot_checkpoint) = hot_checkpoint {
        published.push(hot_checkpoint);
    }

    // A round allocates its generation; with several shards the coordinator's round does.
    let mut generation = match &state.cluster {
        None => match state
            .checkpoints
            .allocate_generation(state.checkpoint_generation.get())
        {
            Ok(generation) => generation,
            Err(_) => return false,
        },
        Some(coordinator) => coordinator.published_generation().saturating_add(1),
    };
    let data_dir = &state.published_config.persistence.data_dir;
    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);
    state.replica_incomplete.set(true);
    state.replica_link.set_copying(true);
    state.replica_caught_up.set(false);
    state.catchup_wait_started.set(Instant::now());
    if replica_guard::write(data_dir, shard_id, CatchupMarker::Preparing).is_err() {
        return false;
    }

    // From here the marker forces a full resync after any crash, so the log may claim the
    // primary's history before the new base replaces it.
    if crate::replication::record_upstream_history(data_dir, shard_id, history).is_err() {
        return false;
    }

    if state.cluster.is_none() && state.checkpoints.publish(generation, &published).is_err() {
        return false;
    }

    if state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .install_base(sequence)
        .is_err()
    {
        return false;
    }
    // The primary's hot keys are in place before this shard's file can complete a round:
    // the coordinator writes the round's hot partition from the shared hot store.
    if let Some(store) = &state.hot_store
        && let Some(checkpoint) = published.get(1)
    {
        store.restore(checkpoint.entries.clone());
    }
    if let Some(coordinator) = &state.cluster {
        // A carried copy writes this shard's file of the round: it goes in first.
        super::checkpoint::collect_carry(state, true);
        let now = state.store.borrow().now_unix_millis();
        let shard_checkpoint = published.swap_remove(0);

        match coordinator.submit(shard_checkpoint, now) {
            Ok(round) => generation = round,
            Err(error) => {
                warn!(%error, "replica could not add its installed copy to a checkpoint round");
                return false;
            }
        }
    }

    if replica_guard::write(
        data_dir,
        shard_id,
        CatchupMarker::Installed {
            generation,
            snapshot: sequence,
            through: catchup_through,
        },
    )
    .is_err()
    {
        return false;
    }
    state.checkpoint_generation.set(generation);
    state.replica_catchup_through.set(catchup_through);
    *state.store.borrow_mut() = fresh;

    if requested != 0 {
        state.replica_resync_sequence.store(0, Ordering::Release);
    }
    true
}

pub(super) fn finish_replica_catchup(state: &ShardState) -> io::Result<()> {
    if !state.replica_caught_up.get() || state.replica_resync_sequence.load(Ordering::Acquire) != 0
    {
        return Ok(());
    }
    let shard_id = state
        .route
        .as_ref()
        .map_or(0, |route| route.shard_id as u32);
    let data_dir = &state.published_config.persistence.data_dir;
    let target = state.replica_catchup_through.get();
    let marker = replica_guard::read(data_dir, shard_id)?;
    let Some(CatchupMarker::Installed {
        generation,
        through,
        ..
    }) = marker
    else {
        return Err(io::Error::other(
            "replica catch-up marker is missing or uninstalled",
        ));
    };
    if generation != state.checkpoint_generation.get() || through != target {
        return Err(io::Error::other("replica catch-up checkpoint mismatch"));
    }
    if state
        .cluster
        .as_ref()
        .is_some_and(|coordinator| coordinator.published_generation() != generation)
    {
        return Ok(());
    }
    {
        let mut wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if wal.last_sequence() < target {
            return Ok(());
        }
        wal.flush()
            .map_err(|error| io::Error::other(error.to_string()))?;
    }
    replica_guard::clear(data_dir, shard_id)?;
    state.replica_catchup_through.set(0);
    state.replica_incomplete.set(false);
    state.replica_link.set_copying(false);
    state.replica_caught_up.set(false);
    Ok(())
}

pub(super) struct ReplicaReplayClock<'a> {
    pub(super) store: &'a RefCell<KvShard>,
    pub(super) previous: Option<u64>,
}

impl Drop for ReplicaReplayClock<'_> {
    fn drop(&mut self) {
        self.store
            .borrow()
            .resume_expiration_after_replay(self.previous);
    }
}

/// Every generic command calls this; on a primary there is no inbound stream, so the
/// check stays inline and the draining loop stays out of the dispatcher's code.
#[inline]
pub(super) fn drain_replicated(state: &ShardState) {
    if state.replica_frames.borrow().is_some() {
        drain_replicated_frames(state);
    }
}

/// Frames applied per drain. The reader thread refills the queue while it drains, so
/// without a bound a fast primary would keep the shard away from its own clients.
const REPLICA_DRAIN_BUDGET: usize = 256;

#[inline(never)]
fn drain_replicated_frames(state: &ShardState) {
    let taken = drain_replicated_budget(state, REPLICA_DRAIN_BUDGET);

    // A durable reply waits on the primary: make this batch durable now, not on the next
    // `everysec` tick, so the acknowledgement goes out at once.
    if state.replica_link.ack_each()
        && let Err(error) = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .flush()
    {
        warn!(%error, "replica log sync for a durable reply failed");
    }

    if taken == 0 {
        return;
    }

    if taken >= REPLICA_DRAIN_BUDGET {
        // More may be waiting: come back once other ready work has had its turn.
        state.replica_wake.notify_one();
    }

    if let Some(reader) = state.replica_reader.borrow().as_ref() {
        reader.unpark();
    }
}

/// Takes at most `budget` deliveries and returns how many it took.
fn drain_replicated_budget(state: &ShardState, budget: usize) -> usize {
    let mut taken = 0;

    while taken < budget {
        let delivery = {
            let mut slot = state.replica_frames.borrow_mut();
            let Some(inbound) = slot.as_mut() else {
                return taken;
            };

            match inbound.try_recv() {
                Ok(delivery) => delivery,
                Err(mpsc::TryRecvError::Empty) => return taken,
                Err(mpsc::TryRecvError::Disconnected) => {
                    *slot = None;

                    return taken;
                }
            }
        };
        taken += 1;

        let frame = match delivery {
            crate::replication::ReplicaDelivery::Snapshot {
                shard,
                hot,
                catchup_through,
                history,
            } => {
                if !install_replica_snapshot(
                    state,
                    &shard,
                    hot.as_deref(),
                    catchup_through,
                    &history,
                ) {
                    if state.replica_incomplete.get() {
                        state
                            .replica_resync_sequence
                            .store(u64::MAX, Ordering::Release);
                    }
                    if state.replica_resync_sequence.load(Ordering::Acquire) != 0 {
                        continue;
                    }
                    // Dropping the queue would end replication until a restart: ask for a
                    // newer copy instead, which skips the frames queued behind this one.
                    warn!("replica could not install the primary's snapshot; requesting another");
                    request_replica_resync(state);
                    continue;
                }

                continue;
            }

            crate::replication::ReplicaDelivery::CaughtUp(sequence) => {
                let target = state.replica_catchup_through.get();
                let applied = state
                    .persistence
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .last_sequence();
                if state.replica_incomplete.get()
                    && state.replica_resync_sequence.load(Ordering::Acquire) == 0
                    && sequence >= target
                    && applied >= target
                {
                    state.replica_caught_up.set(true);
                    if let Err(error) = finish_replica_catchup(state) {
                        warn!(%error, "replica catch-up remains incomplete");
                    }
                }
                continue;
            }

            crate::replication::ReplicaDelivery::Frame(frame) => {
                if state.replica_resync_sequence.load(Ordering::Acquire) != 0 {
                    continue;
                }
                frame
            }
        };

        if let Some((sharded, channel, message)) = crate::replication::decode_copied_publish(&frame)
        {
            if sharded {
                publish_sharded(state, &channel, &message);
            } else {
                publish_classic(state, &channel, &message);
            }

            continue;
        }

        // Frames apply as the primary wrote them: a key past this replica's clock is
        // still there for the primary's next deadline, persist or delete.
        let _replay_clock = ReplicaReplayClock {
            store: &state.store,
            previous: state.store.borrow().pause_expiration_for_replay(),
        };
        let limits = state.published_config.protocol;
        let applied = {
            let mut wal = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            crate::replication::accept_replica_frame(
                &mut state.store.borrow_mut(),
                &mut wal,
                &state.checkpoints,
                &state.published_config.persistence,
                &frame,
                limits,
            )
        };

        match applied {
            Ok((_, mutation, _))
                if matches!(
                    mutation,
                    crate::OwnedMutation::SlotsIncoming { .. }
                        | crate::OwnedMutation::SlotsGiven { .. }
                ) =>
            {
                let sequence = state
                    .persistence
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .last_sequence();

                super::migrate::replicated(state, &mutation, sequence);
            }
            Ok((database, mutation, skip_expired)) => {
                let track_plan = crate::tracking::TrackPlan::replicated(database, &mutation);
                let hot = database == 0
                    && state.hot_store.as_ref().is_some_and(|store| {
                        crate::hot_store::apply_owned(
                            store,
                            &state.published_config.hot_keys,
                            &mutation,
                        )
                    });

                if !hot
                    && !skip_expired
                    && let Err(error) =
                        apply_mutation(&mut state.store.borrow_mut(), database, &mutation)
                {
                    // The frame is already in this replica's log but not in memory: the
                    // two no longer agree with the primary. Restarting the link would
                    // resume after the frame and keep the difference, so take a full copy
                    // instead, and mark the shard so a crash before it lands resyncs too.
                    warn!(%error, "replica WAL frame could not be applied; requesting a full resync");
                    let shard_id = state
                        .route
                        .as_ref()
                        .map_or(0, |route| route.shard_id as u32);
                    let _ = replica_guard::write(
                        &state.published_config.persistence.data_dir,
                        shard_id,
                        CatchupMarker::Preparing,
                    );
                    state.replica_incomplete.set(true);
                    state.replica_link.set_copying(true);
                    state.replica_caught_up.set(false);
                    state
                        .replica_resync_sequence
                        .store(u64::MAX, Ordering::Release);
                    continue;
                }

                if !state.waiters.borrow().is_empty() {
                    let mut store = state.store.borrow_mut();
                    let previous = store.active_database();

                    store.enter(database);
                    crate::engine::wake_replicated_readers(
                        &mut *state.waiters.borrow_mut(),
                        database,
                        &mutation,
                        &mut |key, millis, sequence| {
                            store
                                .stream_contains(key, millis, sequence)
                                .unwrap_or(false)
                        },
                    );
                    store.enter(previous);
                }
                bump_mutations(state);
                flush_key_notices(state);
                observe_tracking(state, 0, false, track_plan, true);
            }

            Err(crate::replication::ApplyFailure::Persist(error)) => {
                // The frame is not applied and the next one requests a snapshot. After a
                // failed fsync this repeats until a restart while reads serve older data: say so, at most
                // every ten seconds (INFO also reports the link down).
                if state
                    .replica_persist_warned
                    .get()
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
                {
                    state.replica_persist_warned.set(Some(Instant::now()));
                    warn!(
                        %error,
                        "replica cannot log the primary's frames; it serves older data until \
                         the log recovers (after a failed fsync: until restart)"
                    );
                }
            }
            // A frame an earlier link queued, sent again by the link that replaced it.
            Err(crate::replication::ApplyFailure::Applied) => {}
            Err(crate::replication::ApplyFailure::NeedsSnapshot) => {
                let sequence = request_replica_resync(state);
                warn!(
                    sequence,
                    "replica lacks the key a frame edits (JSON path, deadline or PERSIST); \
                     requesting a newer primary snapshot"
                );
            }
            Err(failure) => {
                // A gap (a frame refused before it, such as after a failed log write), a bad
                // frame or another shard's frame: this log cannot continue from the stream.
                // Resuming would hit the same frame again, and dropping the queue would end
                // replication for good, so take a copy that covers the next sequence.
                let sequence = request_replica_resync(state);
                warn!(
                    ?failure,
                    sequence, "replica frame refused; requesting a newer primary snapshot"
                );
            }
        }
    }

    taken
}

/// Asks the reader for a primary snapshot that covers the next sequence; the frames queued
/// until it arrives are skipped. Returns that sequence.
fn request_replica_resync(state: &ShardState) -> u64 {
    let sequence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .last_sequence()
        .saturating_add(1);
    // The reader may have asked for a full copy (u64::MAX) meanwhile; keep any request
    // already pending instead of narrowing it.
    match state.replica_resync_sequence.compare_exchange(
        0,
        sequence,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => sequence,
        Err(pending) => pending,
    }
}

pub(super) async fn wait_for_durable_replicas(
    state: &ShardState,
    forwarded_shards: u64,
    replicas: u64,
    timeout_ms: u64,
    shutdown: &ShutdownToken,
) -> u64 {
    if state.role.get() != crate::config::ReplicationRole::Primary {
        return 0;
    }

    let targets: Vec<(Arc<crate::replication::DurableAcks>, u64)> =
        durable_shards(state, forwarded_shards)
            .into_iter()
            .map(|shard| {
                let target = shard
                    .wal
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .last_sequence();

                (shard.acks, target)
            })
            .collect();

    wait_for_targets(state, &targets, replicas, timeout_ms, shutdown).await
}

/// The shards whose replicas a connection's writes wait for.
///
/// In single-port mode a client's writes may sit on any shard: wait for each shard's
/// replicas to reach that shard's last sequence, and count the replicas all of them reached.
/// In per-shard mode a cluster client sends WAIT to the shard it wrote on, as with Redis
/// Cluster, so that shard's replicas count, plus those of every shard this connection had
/// commands forwarded to (a client that is not cluster-aware writes through one port).
pub(super) fn durable_shards(state: &ShardState, forwarded_shards: u64) -> Vec<ShardDurability> {
    let own = || ShardDurability {
        wal: Arc::clone(&state.persistence),
        acks: Arc::clone(&state.durable_acks),
        replies: Arc::clone(&state.durable_replies),
    };

    match state.forwarding.as_ref() {
        Some(forwarding) if state.single_port => forwarding.fabric.durability(),
        Some(forwarding) if forwarded_shards != 0 => {
            let mut shards = forwarding
                .fabric
                .durability_of(forwarded_shards & !(1 << forwarding.me));

            shards.push(own());
            shards
        }
        _ => vec![own()],
    }
}

/// Waits until `replicas` replicas reached every target, or the timeout; returns how many
/// replicas reached all of them.
pub(super) async fn wait_for_targets(
    state: &ShardState,
    targets: &[(Arc<crate::replication::DurableAcks>, u64)],
    replicas: u64,
    timeout_ms: u64,
    shutdown: &ShutdownToken,
) -> u64 {
    let reached = || {
        targets
            .iter()
            .map(|(acks, target)| acks.reached(*target))
            .min()
            .unwrap_or(0)
    };
    let deadline = (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms));

    loop {
        if shutdown.is_requested() {
            return reached();
        }

        let reached_now = reached();

        if reached_now >= replicas {
            return reached_now;
        }

        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            return reached();
        }

        tokio::select! {
            _ = state.durable_acks.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}
