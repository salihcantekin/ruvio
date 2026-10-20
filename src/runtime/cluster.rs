//! Shared CLUSTER SLOTS / NODES / MOVED encoding and cluster checkpoints.

use std::{
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::{
    persistence::{CheckpointManager, PersistenceError, ShardCheckpoint, ShardFile},
    replication::{Endpoint, ReplicaPeer},
    routing::SlotTable,
    time::UnixMillis,
};

pub(crate) const CROSSSLOT_REPLY: &[u8] =
    b"-CROSSSLOT Keys in request do not hash to the same slot\r\n";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClusterRoute {
    pub shard_id: usize,
    pub shard_count: usize,
    pub advertised_host: String,
    /// Port announced for shard 0. Shard `i` is announced on this port plus `i`.
    pub advertised_port: u16,
    /// Which shard owns each slot, shared by every shard of the process.
    pub slots: SlotTable,
    /// The other groups of a multi-node cluster; `None` on a node that owns every slot.
    pub topology: Option<crate::topology::SharedTopology>,
}

/// The other nodes serving one shard's slots, as this node knows them: a primary's
/// replicas that announced an address, or on a replica the primary it follows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardPeers {
    /// Set on a replica that has been accepted by a primary that announced itself.
    pub primary: Option<Endpoint>,
    pub replicas: Vec<ReplicaPeer>,
}

/// One node of a shard in the cluster answers.
struct Node {
    host: String,
    port: u16,
    id: String,
    primary: bool,
    /// Sequence the node acknowledged; 0 for the primary, as before replicas were listed.
    offset: u64,
}

/// The nodes that serve `shard_id`'s slots, primary first.
fn shard_nodes(route: &ClusterRoute, peers: &[ShardPeers], shard_id: usize) -> Vec<Node> {
    let host = route.advertised_host.clone();
    let port = route.advertised_port.wrapping_add(shard_id as u16);
    let own = |primary| Node {
        id: node_id(&host, port),
        host: host.clone(),
        port,
        primary,
        offset: 0,
    };
    let peers = peers.get(shard_id);

    if let Some(primary) = peers.and_then(|peers| peers.primary.as_ref()) {
        return vec![
            Node {
                id: node_id(&primary.host, primary.port),
                host: primary.host.clone(),
                port: primary.port,
                primary: true,
                offset: 0,
            },
            own(false),
        ];
    }

    let mut nodes = vec![own(true)];

    for replica in peers.map_or(&[][..], |peers| &peers.replicas) {
        nodes.push(Node {
            id: node_id(&replica.endpoint.host, replica.endpoint.port),
            host: replica.endpoint.host.clone(),
            port: replica.endpoint.port,
            primary: false,
            offset: replica.sequence,
        });
    }

    nodes
}

/// Who serves a slot range: a shard of this node, or shard `shard` of another group's
/// primary (multi-node cluster).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Holder {
    Local(usize),
    Remote { group: usize, shard: usize },
}

/// Consecutive slots of one holder as `(first, last, index into the holders)`, and the
/// holders: this node's shards first (index = shard id), then the other groups' shards.
/// Without a topology this is [`SlotTable::ranges`] and the local shards.
fn holder_ranges(route: &ClusterRoute) -> (Vec<(u16, u16, usize)>, Vec<Holder>) {
    let mut holders: Vec<Holder> = (0..route.shard_count).map(Holder::Local).collect();
    let Some(topology) = route.topology.as_ref().map(|shared| shared.get()) else {
        return (route.slots.ranges(), holders);
    };
    let mut ranges: Vec<(u16, u16, usize)> = Vec::new();
    let mut known: std::collections::HashMap<Holder, usize> = holders
        .iter()
        .enumerate()
        .map(|(index, holder)| (*holder, index))
        .collect();

    for slot in 0..crate::routing::HASH_SLOT_COUNT as u16 {
        let owner = route.slots.owner(slot);
        let holder = if owner < route.shard_count {
            Holder::Local(owner)
        } else {
            let group = owner - route.shard_count;

            Holder::Remote {
                group,
                shard: topology
                    .groups
                    .get(group)
                    .and_then(|known| known.shard_of(slot))
                    .unwrap_or(0),
            }
        };
        let index = *known.entry(holder).or_insert_with(|| {
            holders.push(holder);
            holders.len() - 1
        });

        match ranges.last_mut() {
            Some((_, last, previous)) if *previous == index => *last = slot,
            _ => ranges.push((slot, slot, index)),
        }
    }

    (ranges, holders)
}

/// The nodes that serve a holder's slots, primary first. Another group is listed by its
/// primary only: this node does not know that group's replicas.
fn holder_nodes(route: &ClusterRoute, peers: &[ShardPeers], holder: Holder) -> Vec<Node> {
    match holder {
        Holder::Local(shard_id) => shard_nodes(route, peers, shard_id),
        Holder::Remote { group, shard } => {
            let Some(group) = route
                .topology
                .as_ref()
                .and_then(|shared| shared.get().groups.get(group).cloned())
            else {
                return Vec::new();
            };
            let port = group.port.wrapping_add(shard as u16);

            vec![Node {
                id: node_id(&group.host, port),
                host: group.host.clone(),
                port,
                primary: true,
                offset: 0,
            }]
        }
    }
}

pub(crate) fn write_cluster_slots(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
    peers: &[ShardPeers],
) -> io::Result<()> {
    let Some(route) = route else {
        output.write_all(b"-ERR cluster mode is disabled\r\n")?;

        return Ok(());
    };

    let (ranges, holders) = holder_ranges(route);
    let nodes: Vec<Vec<Node>> = holders
        .iter()
        .map(|holder| holder_nodes(route, peers, *holder))
        .collect();

    write!(output, "*{}\r\n", ranges.len())?;

    for (start, end, shard_id) in ranges {
        let nodes = &nodes[shard_id];

        write!(output, "*{}\r\n:{start}\r\n:{end}\r\n", 2 + nodes.len())?;

        for node in nodes {
            write!(output, "*3\r\n")?;
            write_bulk(output, node.host.as_bytes())?;
            write!(output, ":{}\r\n", node.port)?;
            write_bulk(output, node.id.as_bytes())?;
        }
    }

    Ok(())
}

pub(crate) fn write_cluster_nodes(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
    peers: &[ShardPeers],
) -> io::Result<()> {
    let Some(route) = route else {
        output.write_all(b"-ERR cluster mode is disabled\r\n")?;

        return Ok(());
    };

    let mut lines = Vec::new();
    let (ranges, holders) = holder_ranges(route);
    let myself = my_id(route);

    for (shard_id, holder) in holders.iter().enumerate() {
        let slots = shard_ranges(&ranges, shard_id)
            .map(|(start, end)| {
                if start == end {
                    format!(" {start}")
                } else {
                    format!(" {start}-{end}")
                }
            })
            .collect::<String>();
        let nodes = holder_nodes(route, peers, *holder);
        let Some(primary_id) = nodes.first().map(|node| node.id.clone()) else {
            continue;
        };

        for node in nodes {
            let bus_port = u32::from(node.port) + 10_000;
            let role = if node.primary { "master" } else { "slave" };
            let flags = if node.id == myself {
                format!("myself,{role}")
            } else {
                role.to_owned()
            };
            let (master, slots) = if node.primary {
                ("-", slots.as_str())
            } else {
                (primary_id.as_str(), "")
            };

            writeln!(
                lines,
                "{} {}:{}@{bus_port} {flags} {master} 0 0 {} connected{slots}",
                node.id,
                node.host,
                node.port,
                shard_id + 1
            )?;
        }
    }

    crate::engine::write_response(output, crate::engine::Response::Bulk(Some(lines.into())))
}

pub(crate) fn write_cluster_shards(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
    peers: &[ShardPeers],
) -> io::Result<()> {
    let Some(route) = route else {
        output.write_all(b"-ERR cluster mode is disabled\r\n")?;

        return Ok(());
    };

    let (ranges, holders) = holder_ranges(route);

    write!(output, "*{}\r\n", holders.len())?;

    for (shard_id, holder) in holders.iter().enumerate() {
        let owned: Vec<(u16, u16)> = shard_ranges(&ranges, shard_id).collect();
        let nodes = holder_nodes(route, peers, *holder);

        write!(output, "*4\r\n$5\r\nslots\r\n*{}\r\n", owned.len() * 2)?;
        for (start, end) in owned {
            write!(output, ":{start}\r\n:{end}\r\n")?;
        }
        write!(output, "$5\r\nnodes\r\n*{}\r\n", nodes.len())?;

        for node in nodes {
            output.write_all(b"*14\r\n")?;
            write_bulk(output, b"id")?;
            write_bulk(output, node.id.as_bytes())?;
            write_bulk(output, b"port")?;
            write!(output, ":{}\r\n", node.port)?;
            write_bulk(output, b"ip")?;
            write_bulk(output, node.host.as_bytes())?;
            write_bulk(output, b"endpoint")?;
            write_bulk(output, node.host.as_bytes())?;
            write_bulk(output, b"role")?;
            write_bulk(
                output,
                if node.primary {
                    &b"master"[..]
                } else {
                    &b"replica"[..]
                },
            )?;
            write_bulk(output, b"replication-offset")?;
            write!(output, ":{}\r\n", node.offset)?;
            write_bulk(output, b"health")?;
            write_bulk(output, b"online")?;
        }
    }

    Ok(())
}

pub(crate) fn write_cluster_info(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
    peers: &[ShardPeers],
) -> io::Result<()> {
    let Some(route) = route else {
        output.write_all(b"-ERR cluster mode is disabled\r\n")?;

        return Ok(());
    };

    let (_, holders) = holder_ranges(route);
    let size = holders.len();
    let known: usize = holders
        .iter()
        .map(|holder| holder_nodes(route, peers, *holder).len())
        .sum();
    let epoch = route
        .topology
        .as_ref()
        .map_or(size as u64, |shared| shared.get().version);
    let info = format!(
        "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{known}\r\ncluster_size:{size}\r\ncluster_current_epoch:{epoch}\r\ncluster_my_epoch:{}\r\n",
        route.shard_id + 1
    );

    write_bulk(output, info.as_bytes())
}

pub(crate) fn write_cluster_myid(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
) -> io::Result<()> {
    let Some(route) = route else {
        output.write_all(b"-ERR cluster mode is disabled\r\n")?;

        return Ok(());
    };

    write_bulk(output, my_id(route).as_bytes())
}

/// `READONLY` and `READWRITE` have nothing to switch: every shard is a primary.
pub(crate) fn write_read_mode(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
) -> io::Result<()> {
    if route.is_none() {
        return output.write_all(b"-ERR cluster mode is disabled\r\n");
    }

    output.write_all(b"+OK\r\n")
}

pub(crate) fn write_key_slot(output: &mut impl Write, key: &[u8]) -> io::Result<()> {
    write!(output, ":{}\r\n", crate::routing::hash_slot(key))
}

/// `INFO` section that cluster-aware clients read to decide whether to discover slots.
pub(crate) fn info_section(route: Option<&ClusterRoute>) -> &'static str {
    if route.is_some() {
        "# Cluster\r\ncluster_enabled:1\r\n"
    } else {
        "# Cluster\r\ncluster_enabled:0\r\n"
    }
}

/// The slot ranges `shard_id` owns, from [`SlotTable::ranges`].
fn shard_ranges(
    ranges: &[(u16, u16, usize)],
    shard_id: usize,
) -> impl Iterator<Item = (u16, u16)> + '_ {
    ranges
        .iter()
        .filter(move |(_, _, owner)| *owner == shard_id)
        .map(|(start, end, _)| (*start, *end))
}

/// A node's id: the SHA-1 of the address clients reach it on, so it differs between the
/// shards of one process and between processes, and survives a restart on the same address.
fn node_id(host: &str, port: u16) -> String {
    use sha1::{Digest, Sha1};

    let digest = Sha1::digest(format!("{host}:{port}").as_bytes());

    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The id of the shard answering, at its announced address.
fn my_id(route: &ClusterRoute) -> String {
    node_id(
        &route.advertised_host,
        route.advertised_port.wrapping_add(route.shard_id as u16),
    )
}

fn write_bulk(output: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write!(output, "${}\r\n", bytes.len())?;
    output.write_all(bytes)?;
    output.write_all(b"\r\n")
}

pub(crate) fn redirect_if_remote(
    output: &mut impl Write,
    route: Option<&ClusterRoute>,
    slot: u16,
) -> io::Result<bool> {
    let Some(route) = route else {
        return Ok(false);
    };

    let owner = route.slots.owner(slot);

    if owner == route.shard_id {
        return Ok(false);
    }

    if owner >= route.shard_count
        && let Some(topology) = &route.topology
    {
        return write_moved(output, &topology.get(), slot, owner, route.shard_count).map(|()| true);
    }

    let port = route.advertised_port + owner as u16;

    write!(output, "-MOVED {slot} {}:{port}\r\n", route.advertised_host)?;
    Ok(true)
}

/// `-MOVED slot host:port` for a slot another group of the cluster owns (`owner` is the
/// slot table entry, past the local shards).
pub(crate) fn write_moved(
    output: &mut impl Write,
    topology: &crate::topology::Topology,
    slot: u16,
    owner: usize,
    local_shards: usize,
) -> io::Result<()> {
    match topology.remote(slot, owner, local_shards) {
        Some((host, port)) => write!(output, "-MOVED {slot} {host}:{port}\r\n"),
        None => output.write_all(b"-CLUSTERDOWN Hash slot not served\r\n"),
    }
}

struct ClusterCheckpointState {
    generation: u64,
    /// The round whose directory is open and whose shard files are being collected.
    open: Option<u64>,
    shards: Vec<Option<ShardFile>>,
    /// The snapshot requests made before each submitted file's snapshot point (`u64::MAX`
    /// for a shard's final checkpoint at shutdown and for installed copies).
    covers: Vec<u64>,
    /// Shutdown began: only final checkpoints may complete a round.
    final_round: bool,
    /// Shards whose carried copy (see `carry_published_file`) is being written into the
    /// open round: it writes the shard's file path, so no other file of that shard may
    /// start meanwhile.
    carrying: Vec<bool>,
    /// The last time the hot partition was tried and found not yet safe to take (see
    /// `retry_hot_partition`).
    hot_tried_at: Option<std::time::Instant>,
    /// A log holds a hot write it refused, which the shared hot store may still hold: no
    /// hot partition is taken until restart (see `WalSink::sync_hot_records`).
    hot_blocked: bool,
}

/// Clears a shard's carrying mark when its copy ends (see `carry_published_file`).
struct CarryingMark<'a> {
    coordinator: &'a ClusterCheckpointCoordinator,
    index: usize,
}

impl Drop for CarryingMark<'_> {
    fn drop(&mut self) {
        self.coordinator
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .carrying[self.index] = false;
    }
}

/// A `SAVE` client waiting for its round; dropping it, on any reply or when the client
/// is gone, ends the wait.
pub(crate) struct SaveWait {
    coordinator: Arc<ClusterCheckpointCoordinator>,
}

impl Drop for SaveWait {
    fn drop(&mut self) {
        self.coordinator.save_clients.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct ClusterCheckpointCoordinator {
    manager: CheckpointManager,
    published_generation: AtomicU64,
    /// Every snapshot request up to this one was made before every file of a published
    /// round was taken: that round holds each write acknowledged before such a request.
    published_requests: AtomicU64,
    /// The latest snapshot request a shard's checkpoint failed to answer (a write, sync or
    /// commit error): a `SAVE` waiting on it fails instead of waiting for a retry.
    failed_requests: AtomicU64,
    /// The latest snapshot request a `SAVE` raised: its client waits, so every shard starts
    /// its file at once, past the throttles that space out other requests.
    save_requests: AtomicU64,
    /// `SAVE` clients still waiting for their round (see [`SaveWait`]). A request whose
    /// client gave up (its timeout, shutdown, a hang-up) no longer skips the throttles.
    save_clients: AtomicUsize,
    snapshot_requests: Arc<AtomicU64>,
    state: Mutex<ClusterCheckpointState>,
    /// The shared hot store, when `hot_keys` adds a hot partition to every round. Only the
    /// coordinator writes that partition, under its lock (see `submit_file`).
    hot_store: Option<crate::hot_store::SharedMemoryStore>,
    /// Each shard's log (see [`Self::register_wal`]): the hot partition is taken only when
    /// every hot-key write it holds is synced in its shard's log.
    /// Weak: a shard's log is dropped (its preallocated tail trimmed, its syncs waited
    /// for) when the shard stops, which a strong reference here would prevent.
    wals: Mutex<Vec<Option<std::sync::Weak<Mutex<crate::persistence::WalSink>>>>>,
    /// The open round holds every regular shard file and waits for its hot partition (see
    /// `retry_hot_partition`): read by every shard on every tick, so kept outside the lock.
    hot_waiting: AtomicBool,
}

pub(crate) fn hot_partition_id(shard_count: usize) -> u32 {
    shard_count as u32
}

impl ClusterCheckpointCoordinator {
    pub(crate) fn new(
        manager: CheckpointManager,
        shard_count: usize,
        generation: u64,
        hot_store: Option<crate::hot_store::SharedMemoryStore>,
    ) -> Self {
        Self {
            hot_store,
            manager,
            published_generation: AtomicU64::new(generation),
            published_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            save_requests: AtomicU64::new(0),
            save_clients: AtomicUsize::new(0),
            snapshot_requests: Arc::new(AtomicU64::new(0)),
            wals: Mutex::new((0..shard_count).map(|_| None).collect()),
            hot_waiting: AtomicBool::new(false),
            state: Mutex::new(ClusterCheckpointState {
                generation,
                open: None,
                shards: (0..shard_count).map(|_| None).collect(),
                covers: vec![0; shard_count],
                final_round: false,
                carrying: vec![false; shard_count],
                hot_tried_at: None,
                hot_blocked: false,
            }),
        }
    }

    /// Gives the coordinator shard `shard_id`'s log, for the hot partition (see
    /// [`Self::hot_writes_durable`]).
    pub(crate) fn register_wal(
        &self,
        shard_id: u32,
        wal: &Arc<Mutex<crate::persistence::WalSink>>,
    ) {
        if let Some(slot) = self
            .wals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(shard_id as usize)
        {
            *slot = Some(Arc::downgrade(wal));
        }
    }

    /// Called after the hot store was read for a partition. A hot-key write is applied to
    /// the shared store before its record is synced in its shard's log, so under `always`
    /// a write the partition holds may still be refused. Every shard holds its log's lock
    /// from applying a hot write to logging it, so each log already has the record of every
    /// write that was read, and syncs it here (see `WalSink::sync_hot_records`); only a log
    /// with a hot write it refused keeps the partition out.
    ///
    /// The logs are kept alive in `alive`, which the caller drops after the coordinator's
    /// lock: a shard that stopped meanwhile drops its log there, not under that lock.
    ///
    /// The error says whether a later attempt may succeed: not while a log holds a refused
    /// hot write, which lasts until restart.
    fn hot_writes_durable(
        &self,
        alive: &mut Vec<Arc<Mutex<crate::persistence::WalSink>>>,
    ) -> Result<(), (PersistenceError, bool)> {
        alive.extend(
            self.wals
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .flatten()
                .filter_map(std::sync::Weak::upgrade),
        );

        for wal in alive.iter() {
            wal.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .sync_hot_records()?;
        }

        Ok(())
    }

    pub(crate) fn manager(&self) -> &CheckpointManager {
        &self.manager
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .shards
            .len()
    }

    /// Returns the open round, opening a new generation directory for the first caller.
    pub(crate) fn begin_round(&self, shard_id: u32) -> Result<u64, PersistenceError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.carrying.get(shard_id as usize) == Some(&true) {
            return Err(PersistenceError::Failed(
                "this shard's carried checkpoint copy is still being written".into(),
            ));
        }

        Self::open_round(&self.manager, &mut state, shard_id)
    }

    fn open_round(
        manager: &CheckpointManager,
        state: &mut ClusterCheckpointState,
        shard_id: u32,
    ) -> Result<u64, PersistenceError> {
        if let Some(generation) = state.open {
            // A shard that already submitted to the open round replaces its file: the new
            // one is written over the old path, so the round forgets the old file and its
            // digest and cannot commit until the replacement is submitted.
            if let Some(slot) = state.shards.get_mut(shard_id as usize) {
                *slot = None;
            }

            return Ok(generation);
        }

        // Past every round ever started: one that failed after CURRENT may be published.
        let generation = manager.allocate_generation(state.generation)?;

        manager.begin_round(generation)?;
        state.open = Some(generation);
        state.hot_tried_at = None;
        Ok(generation)
    }

    /// For a shard whose log failed (see `CheckpointManager::carry_shard_file`): puts a copy
    /// of its file of the last published round into the open round, or into a new one at
    /// shutdown, so the other shards' rounds still publish. Returns the round and the
    /// copy's snapshot point, or `None` when no round needs it. The copy answers no
    /// snapshot request; at shutdown it counts as final, since that shard's log keeps every
    /// write it acknowledged after the copy's snapshot point.
    ///
    /// With no round ever published, `log_holds_everything` (the shard's log was never
    /// compacted nor based) lets it hand an empty file at sequence 0 instead: replaying its
    /// whole log over it rebuilds the shard.
    pub(crate) fn carry_published_file(
        &self,
        shard_id: u32,
        now: UnixMillis,
        log_holds_everything: bool,
    ) -> Result<Option<(u64, u64)>, PersistenceError> {
        let index = shard_id as usize;
        let (from, generation, final_round) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if state.open.is_none() && !state.final_round {
                return Ok(None);
            }

            // Read under the lock rounds publish under: the newest published file is the
            // one copied, never one a round published meanwhile superseded.
            let from = self.published_generation();

            if from == 0 && !log_holds_everything {
                return Err(PersistenceError::Failed(
                    "no published checkpoint to carry into the round".into(),
                ));
            }

            // The round already holds a newer file of this shard (taken before its log
            // failed, or once it resumed): a copy of an older one never replaces it. At
            // shutdown only a final file counts.
            if state.open.is_some()
                && state.shards.get(index).is_some_and(Option::is_some)
                && (!state.final_round || state.covers[index] == u64::MAX)
            {
                return Ok(None);
            }

            if state.carrying.get(index) != Some(&false) {
                return Err(PersistenceError::Failed(
                    "invalid carried checkpoint shard".into(),
                ));
            }

            let generation = Self::open_round(&self.manager, &mut state, shard_id)?;

            state.carrying[index] = true;
            (from, generation, state.final_round)
        };
        // Clears the shard's carrying mark however the copy ends, a panic included.
        let _carrying = CarryingMark {
            coordinator: self,
            index,
        };

        let file = if from == 0 {
            self.manager
                .empty_shard_file(generation, shard_id, self.shard_count() as u32)
        } else {
            self.manager.carry_shard_file(from, generation, shard_id)
        };
        let covers = if final_round { u64::MAX } else { 0 };
        let covered = file.and_then(|file| {
            let covered = file.last_sequence;

            self.submit_file_inner(generation, file, now, covers, true)
                .map(|()| covered)
        })?;

        Ok(Some((generation, covered)))
    }

    /// True while the open round already holds this shard's file and waits for others.
    pub(crate) fn joined_open_round(&self, shard_id: u32) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state.open.is_some()
            && state
                .shards
                .get(shard_id as usize)
                .is_some_and(Option::is_some)
    }

    /// Writes an in-memory checkpoint (small partitions) as a shard file of the open round,
    /// and returns that round's generation.
    pub(crate) fn submit(
        &self,
        checkpoint: ShardCheckpoint,
        now: UnixMillis,
    ) -> Result<u64, PersistenceError> {
        if self.hot_store.is_some() && checkpoint.shard_id as usize + 1 == self.shard_count() {
            return Err(PersistenceError::Failed(
                "the hot partition is written by the checkpoint coordinator".into(),
            ));
        }

        let generation = self.begin_round(checkpoint.shard_id)?;
        let file =
            self.manager
                .write_shard_file(generation, self.shard_count() as u32, &checkpoint)?;

        self.submit_file(generation, file, now)?;
        Ok(generation)
    }

    /// Records a finished shard file; the last one of the round commits it. With a hot
    /// store, the submission that completes the set of regular shard files (a checkpoint or
    /// a replica's installed copy alike) writes the hot partition then, so it is taken
    /// after every shard file's coverage point: a hot-key write logged by any shard is
    /// either in that shard's file range (and so in the hot partition) or replayed from its
    /// WAL. A shard that replaces its file reopens its slot, so the hot partition is taken
    /// again after the replacement. The file is written under the lock: hot partitions are
    /// small, and a single writer keeps the file the digest was taken from.
    pub(crate) fn submit_file(
        &self,
        generation: u64,
        file: ShardFile,
        now: UnixMillis,
    ) -> Result<(), PersistenceError> {
        self.submit_file_covering(generation, file, now, u64::MAX)
    }

    /// True once shutdown began (see [`Self::require_final_round`]).
    pub(crate) fn final_round(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .final_round
    }

    /// From now on a round completes only with every shard's final checkpoint: a file a
    /// shard wrote before it stopped taking writes may miss some of them.
    pub(crate) fn require_final_round(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .final_round = true;
    }

    /// [`Self::submit_file`] for a file whose snapshot point came after snapshot request
    /// `covers` (`u64::MAX`: a final checkpoint). A round publishes with whatever files it
    /// holds, so requests that keep arriving never hold it open; it answers the requests
    /// made before its oldest file (see [`Self::published_requests`]). Only after shutdown
    /// began is a file dropped: one that is not final, which its shard's final checkpoint
    /// replaces; the hot partition is then taken again after it.
    pub(crate) fn submit_file_covering(
        &self,
        generation: u64,
        file: ShardFile,
        now: UnixMillis,
        covers: u64,
    ) -> Result<(), PersistenceError> {
        self.submit_file_inner(generation, file, now, covers, false)
    }

    /// `carried`: the file is a carried copy whose claim (see `carry_published_file`) this
    /// submission ends; any other file of a shard whose copy is being written is refused.
    fn submit_file_inner(
        &self,
        generation: u64,
        file: ShardFile,
        now: UnixMillis,
        covers: u64,
        carried: bool,
    ) -> Result<(), PersistenceError> {
        // Declared before the state guard, so dropped after it (see `hot_writes_durable`).
        let mut alive = Vec::new();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = file.shard_id as usize;

        if index >= state.shards.len()
            || state.open != Some(generation)
            || state.carrying[index] != carried
        {
            return Err(PersistenceError::Failed(
                "invalid checkpoint shard submission".into(),
            ));
        }

        state.carrying[index] = false;
        state.shards[index] = Some(file);
        state.covers[index] = covers;

        let regular = state.shards.len() - usize::from(self.hot_store.is_some());
        let mut stale = false;

        for shard in 0..regular {
            if state.final_round && state.shards[shard].is_some() && state.covers[shard] < u64::MAX
            {
                state.shards[shard] = None;
                stale = true;
            }
        }

        if stale {
            if self.hot_store.is_some()
                && let Some(hot) = state.shards.last_mut()
            {
                *hot = None;
            }

            return Ok(());
        }

        if index + usize::from(self.hot_store.is_some()) < state.shards.len()
            && let Err(error) = self.take_hot_partition(&mut state, generation, now, &mut alive)
        {
            // The shard's file stays in the round, which waits for the hot partition (see
            // `retry_hot_partition`); only at shutdown, when no retry follows, is it an
            // error.
            if state.final_round {
                return Err(error);
            }

            tracing::warn!(%error, generation, "checkpoint round waits for its hot partition");
            return Ok(());
        }

        self.commit_if_complete(&mut state, generation)
    }

    /// With a hot store, once every regular shard file is in, writes the round's hot
    /// partition. A partition that is not safe to take yet (see `hot_writes_durable`) leaves
    /// the regular files in place: shards retry it (see `retry_hot_partition`) without
    /// writing new files.
    fn take_hot_partition(
        &self,
        state: &mut ClusterCheckpointState,
        generation: u64,
        now: UnixMillis,
        alive: &mut Vec<Arc<Mutex<crate::persistence::WalSink>>>,
    ) -> Result<(), PersistenceError> {
        if let Some(hot) = &self.hot_store
            && let Some((hot_slot, regular)) = state.shards.split_last()
            && hot_slot.is_none()
            && regular.iter().all(Option::is_some)
        {
            use crate::hot_store::HotStringStore;

            if state.hot_blocked {
                // No round publishes until restart: every SAVE fails now, not on timeout.
                self.fail_requests(u64::MAX);
                return Err(PersistenceError::Failed(
                    "hot partition not taken: a shard's log refused a hot-key write the hot \
                     store may still hold"
                        .into(),
                ));
            }

            let hot_id = regular.len() as u32;
            let covered = regular
                .iter()
                .flatten()
                .map(|file| file.last_sequence)
                .max()
                .unwrap_or(0);
            let hot_checkpoint = hot.checkpoint(now, hot_id, covered);

            if let Err((error, retryable)) = self.hot_writes_durable(alive) {
                state.hot_tried_at = Some(std::time::Instant::now());
                state.hot_blocked |= !retryable;
                if state.hot_blocked {
                    self.fail_requests(u64::MAX);
                }
                self.hot_waiting
                    .store(!state.hot_blocked, Ordering::Release);
                return Err(error);
            }
            self.hot_waiting.store(false, Ordering::Release);
            let hot_file = self.manager.write_shard_file(
                generation,
                state.shards.len() as u32,
                &hot_checkpoint,
            )?;

            state.shards[hot_id as usize] = Some(hot_file);
            state.covers[hot_id as usize] = u64::MAX;
        }

        Ok(())
    }

    /// Takes the open round's hot partition when only it is missing and the last attempt
    /// (which found a log with an unsynced or refused hot write) was at least a second ago,
    /// and publishes the round when that completes it. Called by shards that already
    /// joined the round, so a round waiting for its hot partition needs no new shard file.
    pub(crate) fn retry_hot_partition(&self, now: UnixMillis) -> Result<(), PersistenceError> {
        if !self.hot_waiting.load(Ordering::Acquire) {
            return Ok(());
        }

        let mut alive = Vec::new();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(generation) = state.open else {
            return Ok(());
        };

        if state.final_round
            || state.hot_blocked
            || state
                .hot_tried_at
                .is_none_or(|at| at.elapsed() < std::time::Duration::from_secs(1))
        {
            return Ok(());
        }

        self.take_hot_partition(&mut state, generation, now, &mut alive)?;
        self.commit_if_complete(&mut state, generation)
    }

    fn commit_if_complete(
        &self,
        state: &mut ClusterCheckpointState,
        generation: u64,
    ) -> Result<(), PersistenceError> {
        if state.shards.iter().any(Option::is_none) {
            return Ok(());
        }

        let answered = state.covers.iter().copied().min().unwrap_or(0);
        let complete: Vec<_> = state
            .shards
            .iter_mut()
            .map(|shard| shard.take().unwrap())
            .collect();

        // This round is closed either way: a failure after CURRENT was renamed may already
        // have published it, so its generation is never reused.
        state.open = None;
        self.manager.commit_round(generation, &complete)?;
        state.generation = generation;
        self.published_requests
            .fetch_max(answered, Ordering::AcqRel);
        self.published_generation
            .store(generation, Ordering::Release);
        Ok(())
    }

    /// The latest snapshot request a published round answers: every file of that round
    /// was taken after it, so `SAVE` with this request number or lower is durable.
    pub(crate) fn published_requests(&self) -> u64 {
        self.published_requests.load(Ordering::Acquire)
    }

    /// Records that a checkpoint taken for snapshot request `requested` failed.
    pub(crate) fn fail_requests(&self, requested: u64) {
        self.failed_requests.fetch_max(requested, Ordering::AcqRel);
    }

    /// Records that a `SAVE` raised snapshot request `requested`. Its client counts as
    /// waiting until the returned guard is dropped, however the `SAVE` ends.
    pub(crate) fn begin_save(self: &Arc<Self>, requested: u64) -> SaveWait {
        self.save_requests.fetch_max(requested, Ordering::AcqRel);
        self.save_clients.fetch_add(1, Ordering::AcqRel);

        SaveWait {
            coordinator: Arc::clone(self),
        }
    }

    /// True while a `SAVE` client waits: no published round answers its request yet and
    /// no checkpoint taken for it failed.
    pub(crate) fn save_waiting(&self) -> bool {
        let saved = self.save_requests.load(Ordering::Acquire);

        self.save_clients.load(Ordering::Acquire) > 0
            && saved > self.published_requests()
            && saved > self.failed_requests()
    }

    /// The latest snapshot request a failed checkpoint was taken for (see
    /// [`Self::fail_requests`]).
    pub(crate) fn failed_requests(&self) -> u64 {
        self.failed_requests.load(Ordering::Acquire)
    }

    pub(crate) fn published_generation(&self) -> u64 {
        self.published_generation.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot_requests(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.snapshot_requests)
    }

    pub(crate) fn persist_clock(&self, now: UnixMillis) -> Result<(), PersistenceError> {
        self.manager.persist_clock(now)
    }

    /// True while a round collects shard files and is not yet published.
    pub(crate) fn round_open(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .open
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_shard_route(shard_id: usize) -> ClusterRoute {
        ClusterRoute {
            shard_id,
            shard_count: 2,
            advertised_host: "127.0.0.1".into(),
            advertised_port: 6379,
            slots: SlotTable::from_formula(2),
            topology: None,
        }
    }

    #[test]
    fn disabled_cluster_commands_are_errors() {
        let mut slots = Vec::new();

        write_cluster_slots(&mut slots, None, &[]).unwrap();
        assert_eq!(slots, b"-ERR cluster mode is disabled\r\n");
        let mut nodes = Vec::new();

        write_cluster_nodes(&mut nodes, None, &[]).unwrap();
        assert_eq!(nodes, b"-ERR cluster mode is disabled\r\n");

        for write in [write_cluster_shards, write_cluster_info] {
            let mut encoded = Vec::new();

            write(&mut encoded, None, &[]).unwrap();
            assert_eq!(encoded, b"-ERR cluster mode is disabled\r\n");
        }

        for write in [write_cluster_myid, write_read_mode] {
            let mut encoded = Vec::new();

            write(&mut encoded, None).unwrap();
            assert_eq!(encoded, b"-ERR cluster mode is disabled\r\n");
        }

        let mut slot = Vec::new();

        write_key_slot(&mut slot, b"somekey").unwrap();
        assert_eq!(slot, b":11058\r\n");
        assert_eq!(info_section(None), "# Cluster\r\ncluster_enabled:0\r\n");
    }

    #[test]
    fn shards_list_each_owner_with_its_slot_range() {
        let mut encoded = Vec::new();

        write_cluster_shards(&mut encoded, Some(&two_shard_route(1)), &[]).unwrap();
        let text = String::from_utf8(encoded).unwrap();

        assert!(
            text.starts_with(
                "*2\r\n*4\r\n$5\r\nslots\r\n*2\r\n:0\r\n:8191\r\n$5\r\nnodes\r\n*1\r\n*14\r\n"
            ),
            "{text}"
        );
        assert!(text.contains(":8192\r\n:16383\r\n"), "{text}");
        assert!(
            text.contains("$4\r\nport\r\n:6380\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n"),
            "{text}"
        );
        assert!(text.contains("$6\r\nhealth\r\n$6\r\nonline\r\n"), "{text}");
    }

    #[test]
    fn slots_cover_the_full_hash_space_on_consecutive_ports() {
        let mut encoded = Vec::new();

        write_cluster_slots(&mut encoded, Some(&two_shard_route(0)), &[]).unwrap();
        let text = String::from_utf8(encoded).unwrap();

        assert!(text.starts_with("*2\r\n"), "{text}");
        assert!(text.contains(":0\r\n:8191\r\n"), "{text}");
        assert!(text.contains(":8192\r\n:16383\r\n"), "{text}");
        assert!(text.contains("127.0.0.1\r\n:6379\r\n"), "{text}");
        assert!(text.contains("127.0.0.1\r\n:6380\r\n"), "{text}");
    }

    #[test]
    fn nodes_marks_the_receiving_shard_as_myself() {
        let mut encoded = Vec::new();

        write_cluster_nodes(&mut encoded, Some(&two_shard_route(0)), &[]).unwrap();
        let text = String::from_utf8(encoded).unwrap();

        assert!(
            text.contains("127.0.0.1:6379@16379 myself,master"),
            "{text}"
        );
        assert!(text.contains("127.0.0.1:6380@16380 master"), "{text}");
        assert!(text.contains(" connected 0-8191"), "{text}");
        assert!(text.contains(" connected 8192-16383"), "{text}");
    }

    #[test]
    fn remote_slots_encode_moved_and_local_slots_do_not() {
        let route = two_shard_route(0);
        let mut local = Vec::new();

        assert!(!redirect_if_remote(&mut local, Some(&route), 0).unwrap());
        assert!(local.is_empty());

        let mut remote = Vec::new();

        assert!(redirect_if_remote(&mut remote, Some(&route), 8192).unwrap());
        assert_eq!(remote, b"-MOVED 8192 127.0.0.1:6380\r\n");

        let mut disabled = Vec::new();

        assert!(!redirect_if_remote(&mut disabled, None, 8192).unwrap());
        assert!(disabled.is_empty());
    }

    fn snapshot_config(path: &std::path::Path) -> crate::PersistenceConfig {
        crate::PersistenceConfig {
            profile: crate::DurabilityProfile::Snapshot,
            data_dir: path.to_path_buf(),
            snapshot_max_bytes: 1024 * 1024,
            ..crate::PersistenceConfig::default()
        }
    }

    #[test]
    fn publishes_only_after_every_shard_submits() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = ClusterCheckpointCoordinator::new(manager, 2, 0, None);

        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert!(!directory.path().join("CURRENT").exists());
        assert_eq!(coordinator.published_generation(), 0);
        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 1,
                    last_sequence: 2,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert!(directory.path().join("CURRENT").exists());
        assert_eq!(coordinator.published_generation(), 1);
        let loaded = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        )
        .load()
        .unwrap()
        .unwrap();

        assert_eq!(loaded.shards.len(), 2);
    }

    #[test]
    fn a_shard_replaces_its_file_in_a_round_it_already_joined() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = ClusterCheckpointCoordinator::new(manager, 2, 0, None);

        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        // Shard 0 takes a newer checkpoint (SAVE, shutdown) before the round closes: it
        // replaces its file, and the round waits for the replacement.
        assert_eq!(coordinator.begin_round(0).unwrap(), 1);
        assert_eq!(coordinator.begin_round(1).unwrap(), 1);
        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 1,
                    last_sequence: 2,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert_eq!(coordinator.published_generation(), 0);
        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 5,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert_eq!(coordinator.published_generation(), 1);

        // The published round loads, with shard 0's newer file.
        let loaded = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        )
        .load()
        .unwrap()
        .unwrap();
        assert_eq!(loaded.shards[0].last_sequence, 5);
        assert_eq!(coordinator.begin_round(0).unwrap(), 2);
    }

    fn covering_file(
        coordinator: &ClusterCheckpointCoordinator,
        shard_id: u32,
        last_sequence: u64,
        covers: u64,
    ) {
        let generation = coordinator.begin_round(shard_id).unwrap();
        let file = coordinator
            .manager()
            .write_shard_file(
                generation,
                2,
                &ShardCheckpoint {
                    shard_id,
                    last_sequence,
                    entries: Vec::new(),
                },
            )
            .unwrap();

        coordinator
            .submit_file_covering(generation, file, UnixMillis::new(1), covers)
            .unwrap();
    }

    #[test]
    fn a_round_publishes_while_requests_keep_arriving_and_answers_those_before_its_files() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = ClusterCheckpointCoordinator::new(manager, 2, 0, None);
        let requests = coordinator.snapshot_requests();

        // A request arrives after every file is taken: each round still publishes, and
        // answers only the requests made before its oldest file.
        for round in 1..=5u64 {
            let before = requests.load(Ordering::Acquire);

            covering_file(&coordinator, 1, round, before);
            requests.fetch_add(1, Ordering::AcqRel);
            covering_file(&coordinator, 0, round, before + 1);
            requests.fetch_add(1, Ordering::AcqRel);
            assert_eq!(coordinator.published_generation(), round);
            assert_eq!(coordinator.published_requests(), before);
        }

        // Two files taken after request 10 answer it.
        covering_file(&coordinator, 0, 6, 10);
        covering_file(&coordinator, 1, 6, 10);
        assert_eq!(coordinator.published_requests(), 10);
        // An installed copy answers whatever the other files answer.
        covering_file(&coordinator, 0, 7, u64::MAX);
        covering_file(&coordinator, 1, 7, 12);
        assert_eq!(coordinator.published_requests(), 12);
    }

    #[test]
    fn after_shutdown_begins_only_final_files_complete_a_round() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = ClusterCheckpointCoordinator::new(manager, 2, 0, None);

        covering_file(&coordinator, 1, 1, 0);
        coordinator.require_final_round();
        covering_file(&coordinator, 0, 2, u64::MAX);
        assert_eq!(coordinator.published_generation(), 0);
        covering_file(&coordinator, 1, 3, u64::MAX);
        assert_eq!(coordinator.published_generation(), 1);
    }

    fn hot_store_with(value: &[u8]) -> crate::hot_store::SharedMemoryStore {
        let patterns = vec![b"lock:*".to_vec()];
        let store = crate::hot_store::shared_memory_store(&patterns).unwrap();

        crate::hot_store::apply_owned(
            &store,
            &patterns,
            &crate::OwnedMutation::SetBytes {
                key: b"lock:a".to_vec(),
                value: value.to_vec(),
            },
        );
        store
    }

    fn hot_value(directory: &std::path::Path) -> crate::store::SnapshotValue {
        let loaded = CheckpointManager::new(
            snapshot_config(directory),
            crate::protocol::ProtocolLimits::default(),
        )
        .load()
        .unwrap()
        .unwrap();

        loaded.shards.last().unwrap().entries[0].value.clone()
    }

    #[test]
    fn hot_partition_is_taken_by_the_shard_that_completes_the_regular_files() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let store = hot_store_with(b"old");
        let coordinator = ClusterCheckpointCoordinator::new(manager, 3, 0, Some(store.clone()));
        let set = |value: &[u8]| {
            crate::hot_store::apply_owned(
                &store,
                &[b"lock:*".to_vec()],
                &crate::OwnedMutation::SetBytes {
                    key: b"lock:a".to_vec(),
                    value: value.to_vec(),
                },
            );
        };
        let file = |shard_id, last_sequence| {
            let generation = coordinator.begin_round(shard_id).unwrap();
            let file = coordinator
                .manager()
                .write_shard_file(
                    generation,
                    3,
                    &ShardCheckpoint {
                        shard_id,
                        last_sequence,
                        entries: Vec::new(),
                    },
                )
                .unwrap();
            (generation, file)
        };
        let now = UnixMillis::new(1);

        let (generation, first) = file(0, 1);
        coordinator.submit_file(generation, first, now).unwrap();
        // Shard 0 replaces its file before the round closes, after a hot-key write.
        set(b"new");
        let (generation, replaced) = file(0, 2);
        let (_, second) = file(1, 1);
        coordinator.submit_file(generation, second, now).unwrap();
        assert!(coordinator.round_open(), "shard 0's slot is open again");
        coordinator.submit_file(generation, replaced, now).unwrap();
        assert_eq!(coordinator.published_generation(), 1);
        assert_eq!(
            hot_value(directory.path()),
            crate::store::SnapshotValue::Bytes(b"new".to_vec())
        );
    }

    #[test]
    fn installed_copies_that_complete_a_round_get_its_hot_partition() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator =
            ClusterCheckpointCoordinator::new(manager, 3, 0, Some(hot_store_with(b"primary")));

        // A replica's full-resync install submits whole shard checkpoints, never the hot one.
        for shard_id in 0..2 {
            coordinator
                .submit(
                    ShardCheckpoint {
                        shard_id,
                        last_sequence: 7,
                        entries: Vec::new(),
                    },
                    UnixMillis::new(1),
                )
                .unwrap();
        }
        assert!(!coordinator.round_open());
        assert_eq!(coordinator.published_generation(), 1);
        assert_eq!(
            hot_value(directory.path()),
            crate::store::SnapshotValue::Bytes(b"primary".to_vec())
        );
        // Only the coordinator writes the hot partition.
        assert!(
            coordinator
                .submit(
                    ShardCheckpoint {
                        shard_id: 2,
                        last_sequence: 7,
                        entries: Vec::new(),
                    },
                    UnixMillis::new(1),
                )
                .is_err()
        );
    }

    #[test]
    fn rejects_out_of_range_shard_submission() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = ClusterCheckpointCoordinator::new(manager, 2, 0, None);

        assert!(
            coordinator
                .submit(
                    ShardCheckpoint {
                        shard_id: 2,
                        last_sequence: 1,
                        entries: Vec::new(),
                    },
                    UnixMillis::new(1)
                )
                .is_err()
        );
    }

    #[test]
    fn current_waits_for_hot_partition_when_hot_keys_enabled() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let shards = 2;
        let partitions = crate::config::AppConfig {
            shards,
            hot_keys: vec![b"lock:*".to_vec()],
            ..crate::config::AppConfig::default()
        }
        .checkpoint_partition_count();
        let coordinator = ClusterCheckpointCoordinator::new(manager, partitions, 0, None);

        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 0,
                    last_sequence: 1,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: 1,
                    last_sequence: 2,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert!(
            !directory.path().join("CURRENT").exists(),
            "CURRENT must wait for the hot partition"
        );
        coordinator
            .submit(
                ShardCheckpoint {
                    shard_id: hot_partition_id(shards),
                    last_sequence: 3,
                    entries: Vec::new(),
                },
                UnixMillis::new(1),
            )
            .unwrap();
        assert!(directory.path().join("CURRENT").exists());
        assert_eq!(coordinator.published_generation(), 1);
        let loaded = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        )
        .load()
        .unwrap()
        .unwrap();

        assert_eq!(loaded.shards.len(), 3);
    }

    #[test]
    fn a_save_whose_client_stopped_waiting_no_longer_skips_the_throttles() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CheckpointManager::new(
            snapshot_config(directory.path()),
            crate::protocol::ProtocolLimits::default(),
        );
        let coordinator = Arc::new(ClusterCheckpointCoordinator::new(manager, 2, 0, None));

        assert!(!coordinator.save_waiting());
        let first = coordinator.begin_save(1);
        let second = coordinator.begin_save(2);

        assert!(coordinator.save_waiting());
        // One client timed out or hung up; the other still waits.
        drop(second);
        assert!(coordinator.save_waiting());
        // Nothing published or failed, but no client waits any more.
        drop(first);
        assert!(!coordinator.save_waiting());
        // A failed checkpoint ends the wait even while the client is still there.
        let third = coordinator.begin_save(3);

        coordinator.fail_requests(3);
        assert!(!coordinator.save_waiting());
        drop(third);
    }

    /// The answers before the slot table, built from the formula's one range per shard.
    fn formula_answers(route: &ClusterRoute) -> (String, String, String) {
        use crate::routing::slot_range_for_shard;

        let host = &route.advertised_host;
        let mut slots = format!("*{}\r\n", route.shard_count);
        let mut nodes = String::new();
        let mut shards = format!("*{}\r\n", route.shard_count);

        for shard_id in 0..route.shard_count {
            let (start, end) = slot_range_for_shard(shard_id, route.shard_count);
            let port = route.advertised_port + shard_id as u16;
            let id = node_id(host, port);
            let flags = if shard_id == route.shard_id {
                "myself,master"
            } else {
                "master"
            };

            slots += &format!(
                "*3\r\n:{start}\r\n:{end}\r\n*3\r\n${}\r\n{host}\r\n:{port}\r\n${}\r\n{id}\r\n",
                host.len(),
                id.len()
            );
            nodes += &format!(
                "{id} {host}:{port}@{} {flags} - 0 0 {} connected {start}-{end}\n",
                u32::from(port) + 10_000,
                shard_id + 1
            );
            shards += &format!(
                "*4\r\n$5\r\nslots\r\n*2\r\n:{start}\r\n:{end}\r\n$5\r\nnodes\r\n*1\r\n*14\r\n\
                 $2\r\nid\r\n${}\r\n{id}\r\n$4\r\nport\r\n:{port}\r\n$2\r\nip\r\n${}\r\n{host}\r\n\
                 $8\r\nendpoint\r\n${}\r\n{host}\r\n$4\r\nrole\r\n$6\r\nmaster\r\n\
                 $18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n$6\r\nonline\r\n",
                id.len(),
                host.len(),
                host.len()
            );
        }

        (slots, format!("${}\r\n{nodes}\r\n", nodes.len()), shards)
    }

    #[test]
    fn cluster_answers_from_the_slot_table_match_the_formula() {
        for count in [1, 2, 3, 7, 16] {
            let route = ClusterRoute {
                shard_id: count / 2,
                shard_count: count,
                advertised_host: "10.0.0.7".into(),
                advertised_port: 7000,
                slots: SlotTable::from_formula(count),
                topology: None,
            };
            let (slots, nodes, shards) = formula_answers(&route);
            let mut written = Vec::new();

            write_cluster_slots(&mut written, Some(&route), &[]).unwrap();
            assert_eq!(String::from_utf8(written).unwrap(), slots, "{count} shards");
            let mut written = Vec::new();

            write_cluster_nodes(&mut written, Some(&route), &[]).unwrap();
            assert_eq!(String::from_utf8(written).unwrap(), nodes, "{count} shards");
            let mut written = Vec::new();

            write_cluster_shards(&mut written, Some(&route), &[]).unwrap();
            assert_eq!(
                String::from_utf8(written).unwrap(),
                shards,
                "{count} shards"
            );
        }
    }

    #[test]
    fn a_slot_given_to_another_shard_moves_its_redirect_and_splits_the_ranges() {
        let route = two_shard_route(0);

        // Slot 100 belongs to shard 0 by the formula; give it to shard 1.
        assert!(!redirect_if_remote(&mut Vec::new(), Some(&route), 100).unwrap());
        route.slots.assign(100, 1);
        let mut moved = Vec::new();

        assert!(redirect_if_remote(&mut moved, Some(&route), 100).unwrap());
        assert_eq!(moved, b"-MOVED 100 127.0.0.1:6380\r\n");

        let mut slots = Vec::new();

        write_cluster_slots(&mut slots, Some(&route), &[]).unwrap();
        let slots = String::from_utf8(slots).unwrap();

        // Shard 0 now owns 0-99 and 101-8191, shard 1 owns 100 and 8192-16383.
        assert!(slots.starts_with("*4\r\n*3\r\n:0\r\n:99\r\n"), "{slots}");
        assert!(slots.contains(":100\r\n:100\r\n"), "{slots}");
        assert!(slots.contains(":101\r\n:8191\r\n"), "{slots}");

        let mut nodes = Vec::new();

        write_cluster_nodes(&mut nodes, Some(&route), &[]).unwrap();
        let nodes = String::from_utf8(nodes).unwrap();

        assert!(nodes.contains("connected 0-99 101-8191\n"), "{nodes}");
        assert!(nodes.contains("connected 100 8192-16383\n"), "{nodes}");

        let mut shards = Vec::new();

        write_cluster_shards(&mut shards, Some(&route), &[]).unwrap();
        let shards = String::from_utf8(shards).unwrap();

        assert!(
            shards.contains("$5\r\nslots\r\n*4\r\n:0\r\n:99\r\n:101\r\n:8191\r\n"),
            "{shards}"
        );
        assert!(
            shards.contains("$5\r\nslots\r\n*4\r\n:100\r\n:100\r\n:8192\r\n:16383\r\n"),
            "{shards}"
        );
    }
}
