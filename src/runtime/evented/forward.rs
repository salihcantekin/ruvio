//! Single-port mode: connection hand-off and cross-shard command forwarding.
//!
//! One acceptor thread spreads connections over the shards. A command whose key belongs to
//! another shard is copied (raw RESP frame) into a per-pair mailbox, executed there with the
//! normal parser and executor, and its reply bytes come back the same way. Data is still
//! touched by one thread only.

use super::*;

use crate::{engine::RespVersion, routing::SlotTable, routing::hash_slot};
use std::sync::atomic::AtomicUsize;

/// Data directories whose next commit before a forwarded command fails, once each.
#[cfg(test)]
pub(super) static FAILING_FORWARDED_COMMITS: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn forced_commit_failure(state: &ShardState) -> bool {
    let mut dirs = FAILING_FORWARDED_COMMITS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let data_dir = &state.published_config.persistence.data_dir;

    match dirs.iter().position(|dir| dir == data_dir) {
        Some(index) => {
            dirs.remove(index);
            true
        }
        None => false,
    }
}

/// Remote frames executed per turn before yielding to local clients.
const REMOTE_MESSAGES_PER_TURN: usize = 64;
/// Pending foreign commands one connection may hold before it stops reading.
pub(super) const MAX_PENDING_FORWARDS: usize = 1024;
/// Marks connection ids that stand for a client of another shard.
const REMOTE_ID_BIT: u64 = 1 << 63;

/// Id under which the owning shard keeps WATCH and tracking state of `conn` (home shard `src`).
pub(super) fn remote_id(src: usize, conn: u64) -> u64 {
    REMOTE_ID_BIT | ((src as u64) << 48) | (conn & 0xFFFF_FFFF_FFFF)
}

/// Home shard and connection of a [`remote_id`], or `None` for a local connection id.
pub(super) fn split_remote_id(id: u64) -> Option<(usize, u64)> {
    (id & REMOTE_ID_BIT != 0).then_some((((id >> 48) & 0x7FFF) as usize, id & 0xFFFF_FFFF_FFFF))
}

pub(super) struct Handoff {
    pub(super) stream: std::net::TcpStream,
    pub(super) peer: SocketAddr,
    pub(super) permit: ConnectionPermit,
}

pub(super) struct Request {
    pub(super) src: usize,
    pub(super) conn: u64,
    pub(super) token: u64,
    pub(super) database: u8,
    pub(super) resp: RespVersion,
    pub(super) tracking: bool,
    /// Run in its own task: the command may park (blocking pops, scripts).
    pub(super) spawn: bool,
    pub(super) count: u32,
    pub(super) frames: Vec<u8>,
    /// The caller's identity: the owner shard runs the frames with these rights, so a
    /// script's `ruvio.call` and the queued commands of an EXEC obey the caller's ACL.
    pub(super) username: Arc<[u8]>,
    pub(super) rules: Arc<crate::acl::AclRules>,
}

pub(super) struct Reply {
    pub(super) conn: u64,
    pub(super) token: u64,
    pub(super) lens: Vec<u32>,
    pub(super) data: Vec<u8>,
    /// Commands that only handed their elements to parked clients and logged no record:
    /// their answers stand when the batch's sync fails (see `fail_reply`). Only used on the
    /// shard that ran them; usually empty.
    pub(super) unlogged: Vec<u32>,
}

pub(super) enum Message {
    Request(Request),
    Reply(Reply),
    /// Client-side cache invalidation for connection `conn` of the receiving shard.
    Invalidate {
        conn: u64,
        keys: Vec<Vec<u8>>,
    },
    /// The client `(src, conn)` is gone: drop its WATCH and tracking state.
    Forget {
        src: usize,
        conn: u64,
    },
    /// The client `(src, conn)` left while its forwarded blocking command waits here.
    Cancel {
        src: usize,
        conn: u64,
    },
    /// The client `(src, conn)` ran UNWATCH or DISCARD.
    Unwatch {
        src: usize,
        conn: u64,
    },
    /// The client `(src, conn)` turned CLIENT TRACKING off.
    Untrack {
        src: usize,
        conn: u64,
    },
    /// A classic `PUBLISH` another shard received: deliver it to this shard's subscribers.
    /// Nothing is sent back; the publisher's reply counts its own shard's receivers, as a
    /// Redis Cluster node answers for the node that took the command. One copy is shared by
    /// every destination; the receiver returns its reservation (see `Fabric::reserve_publish`).
    Publish(Arc<PublishDelivery>),
    /// A change another shard made that every shard applies on its own thread (see
    /// [`Forwarding::apply_everywhere`]).
    Broadcast(Arc<Broadcast>),
    /// A step of `RUVIO MOVESLOTS`.
    Move(super::moveslots::MoveMessage),
}

/// Something every shard applies on its own thread, such as a `CONFIG SET`.
pub(super) trait ShardChange: Send + Sync {
    fn apply(&self, state: &ShardState);
}

/// One [`ShardChange`] handed to the other shards, shared by all of them.
pub(super) struct Broadcast {
    change: Box<dyn ShardChange>,
    /// Shards that have not applied it yet.
    pending: AtomicUsize,
    done: tokio::sync::Notify,
}

impl Broadcast {
    fn apply_on(&self, state: &ShardState) {
        self.change.apply(state);

        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.done.notify_one();
        }
    }
}

impl Forwarding {
    /// Sends `change` to every other shard and waits until each has applied it. The caller
    /// applies it on its own shard. Nothing is sent with one shard.
    pub(super) async fn apply_everywhere(&self, change: Box<dyn ShardChange>) {
        let others = self.fabric.count - 1;

        if others == 0 {
            return;
        }

        let broadcast = Arc::new(Broadcast {
            change,
            pending: AtomicUsize::new(others),
            done: tokio::sync::Notify::new(),
        });

        for dst in (0..self.fabric.count).filter(|&dst| dst != self.me) {
            self.fabric
                .send(self.me, dst, Message::Broadcast(Arc::clone(&broadcast)));
        }

        // `notify_one` keeps a permit when it runs before this waits.
        while broadcast.pending.load(Ordering::Acquire) > 0 {
            broadcast.done.notified().await;
        }
    }
}

/// One classic `PUBLISH` handed to the other shards.
pub(super) struct PublishDelivery {
    pub(super) channel: Vec<u8>,
    pub(super) message: Vec<u8>,
}

impl PublishDelivery {
    /// What one queued delivery is charged against a shard's publish budget.
    fn cost(&self) -> usize {
        self.channel.len() + self.message.len()
    }
}

struct Port {
    notify: tokio::sync::Notify,
    /// Bit `src` is set while mailbox `src -> this shard` holds messages.
    mask: AtomicU64,
    /// Publish deliveries queued for this shard, from every shard, and their bytes.
    publish_queued: AtomicUsize,
    publish_bytes: AtomicUsize,
    accept: Mutex<VecDeque<Handoff>>,
    accept_notify: tokio::sync::Notify,
}

/// Shared by every shard thread and the acceptor.
pub(crate) struct Fabric {
    count: usize,
    ports: Vec<Port>,
    /// `boxes[src * count + dst]`: only `src` pushes and only `dst` drains.
    boxes: Vec<Mutex<Vec<Message>>>,
    /// Each shard's log and replica acknowledgements, so `WAIT` covers every shard.
    durability: Mutex<Vec<Option<ShardDurability>>>,
    /// Which shard owns each hash slot (see [`SlotTable`]).
    slots: SlotTable,
    /// The newest `CONFIG SET` configuration and its version (see `live`).
    pub(super) config: Mutex<super::live::ConfigAuthority>,
    /// Slot moves between these shards.
    pub(super) mover: super::moveslots::SlotMover,
    /// Ranges moving to or from another node.
    pub(super) migrations: super::migrate::Migrations,
    /// The cluster map when this node is one of several; set before any shard serves.
    topology: std::sync::OnceLock<crate::topology::SharedTopology>,
}

#[derive(Clone)]
pub(super) struct ShardDurability {
    pub(super) wal: Arc<Mutex<crate::persistence::WalSink>>,
    pub(super) acks: Arc<crate::replication::DurableAcks>,
    pub(super) replies: Arc<crate::replication::DurableReplies>,
}

impl Fabric {
    pub(crate) fn new(count: usize) -> Arc<Self> {
        Arc::new(Self {
            count,
            ports: (0..count)
                .map(|_| Port {
                    notify: tokio::sync::Notify::new(),
                    mask: AtomicU64::new(0),
                    publish_queued: AtomicUsize::new(0),
                    publish_bytes: AtomicUsize::new(0),
                    accept: Mutex::new(VecDeque::new()),
                    accept_notify: tokio::sync::Notify::new(),
                })
                .collect(),
            boxes: (0..count * count).map(|_| Mutex::new(Vec::new())).collect(),
            durability: Mutex::new(vec![None; count]),
            slots: SlotTable::from_formula(count),
            config: Mutex::new(super::live::ConfigAuthority::default()),
            mover: super::moveslots::SlotMover::default(),
            migrations: super::migrate::Migrations::default(),
            topology: std::sync::OnceLock::new(),
        })
    }

    /// Makes this node one of a multi-node cluster: the slots of other groups point past
    /// the local shards (see [`crate::topology::Topology::apply`]).
    pub(crate) fn set_topology(&self, topology: crate::topology::Topology) {
        match self.topology.get() {
            // A newer map keeps this node's slots (and the moves between its shards); only
            // the other groups' entries are rewritten.
            Some(shared) => {
                for (slot, owner) in topology.placement(self.count).into_iter().enumerate() {
                    if usize::from(owner) >= self.count {
                        self.slots.assign(slot as u16, usize::from(owner));
                    }
                }
                shared.replace(topology);
            }
            None => {
                topology.apply(&self.slots, self.count);
                let _ = self
                    .topology
                    .set(crate::topology::SharedTopology::new(topology));
            }
        }
    }

    pub(crate) fn topology(&self) -> Option<&crate::topology::SharedTopology> {
        self.topology.get()
    }

    pub(super) fn register_durability(&self, shard: usize, durability: ShardDurability) {
        self.durability
            .lock()
            .unwrap_or_else(|error| error.into_inner())[shard] = Some(durability);
    }

    /// Every shard registered so far.
    pub(super) fn durability(&self) -> Vec<ShardDurability> {
        self.durability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Each shard's replicas, or the primary it follows, by shard index; a shard not yet
    /// registered has none.
    pub(super) fn shard_peers(&self) -> Vec<crate::runtime::cluster::ShardPeers> {
        let durability = self
            .durability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();

        durability
            .iter()
            .map(|durability| {
                durability
                    .as_ref()
                    .map(|durability| crate::runtime::cluster::ShardPeers {
                        primary: durability.acks.primary(),
                        replicas: durability.acks.replicas(),
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    /// The registered shards whose bit is set in `mask`.
    pub(super) fn durability_of(&self, mask: u64) -> Vec<ShardDurability> {
        self.durability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .enumerate()
            .filter(|(shard, _)| mask & (1 << shard) != 0)
            .filter_map(|(_, durability)| durability.clone())
            .collect()
    }

    pub(super) fn shard_count(&self) -> usize {
        self.count
    }

    pub(super) fn owner_of(&self, key: &[u8]) -> usize {
        self.slots.owner(hash_slot(key))
    }

    /// The shard that owns `slot`.
    #[inline]
    pub(super) fn slot_owner(&self, slot: u16) -> usize {
        self.slots.owner(slot)
    }

    /// Which shard owns each slot; shared with the cluster route of every shard.
    pub(crate) fn slot_table(&self) -> SlotTable {
        self.slots.clone()
    }

    pub(super) fn hand_off(&self, shard: usize, item: Handoff) {
        self.ports[shard]
            .accept
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push_back(item);
        self.ports[shard].accept_notify.notify_one();
    }

    pub(super) fn send(&self, src: usize, dst: usize, message: Message) {
        {
            let mut mailbox = self.boxes[src * self.count + dst]
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            mailbox.push(message);
        }

        // Only the 0 -> non-zero transition wakes the target; a busy shard is already draining.
        let previous = self.ports[dst].mask.fetch_or(1 << src, Ordering::AcqRel);

        if previous == 0 {
            self.ports[dst].notify.notify_one();
        }
    }

    /// Charges one publish delivery of `cost` bytes to shard `dst`'s budget. False, with nothing
    /// charged, when the shard already holds `PUBLISH_QUEUE_MESSAGES` deliveries or
    /// `PUBLISH_QUEUE_BYTES` bytes: a shard stuck in a long command then drops new publishes
    /// instead of letting large messages pile up in memory.
    fn reserve_publish(&self, dst: usize, cost: usize) -> bool {
        let port = &self.ports[dst];

        if port.publish_queued.fetch_add(1, Ordering::AcqRel) >= PUBLISH_QUEUE_MESSAGES {
            port.publish_queued.fetch_sub(1, Ordering::AcqRel);

            return false;
        }

        if port.publish_bytes.fetch_add(cost, Ordering::AcqRel) + cost > PUBLISH_QUEUE_BYTES {
            port.publish_bytes.fetch_sub(cost, Ordering::AcqRel);
            port.publish_queued.fetch_sub(1, Ordering::AcqRel);

            return false;
        }

        true
    }

    /// Returns what `reserve_publish` charged, once shard `me` has delivered the publish.
    pub(super) fn release_publish(&self, me: usize, delivery: &PublishDelivery) {
        let port = &self.ports[me];

        port.publish_bytes
            .fetch_sub(delivery.cost(), Ordering::AcqRel);
        port.publish_queued.fetch_sub(1, Ordering::AcqRel);
    }

    fn drain(&self, me: usize, into: &mut Vec<Message>) {
        let mut mask = self.ports[me].mask.swap(0, Ordering::AcqRel);

        while mask != 0 {
            let src = mask.trailing_zeros() as usize;

            mask &= mask - 1;
            let mut mailbox = self.boxes[src * self.count + me]
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            into.append(&mut mailbox);
        }
    }
}

/// How long an idle acceptor waits for a connection before it looks at shutdown again.
const ACCEPT_IDLE_WAIT: Duration = Duration::from_millis(50);

/// Acceptor thread body: spreads connections round-robin over the shards.
pub(crate) fn run_acceptor(
    listener: std::net::TcpListener,
    fabric: Arc<Fabric>,
    limiter: ConnectionLimiter,
    shutdown: ShutdownToken,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let mut next = 0usize;
    let mut accept_warnings = crate::admission::WarnLimiter::new(Duration::from_secs(1));

    while !shutdown.is_requested() {
        match listener.accept() {
            Ok((stream, peer)) => {
                let Some(permit) = limiter.try_acquire(peer.ip()) else {
                    let mut stream = stream;

                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
                    let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                    continue;
                };

                fabric.hand_off(
                    next,
                    Handoff {
                        stream,
                        peer,
                        permit,
                    },
                );
                next = (next + 1) % fabric.count;
            }

            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::runtime::wait_for_connection(&listener, ACCEPT_IDLE_WAIT);
            }

            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                if accept_warnings.allow(std::time::Instant::now()) {
                    warn!(
                        %error,
                        suppressed = accept_warnings.take_suppressed(),
                        "accept failed"
                    );
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    Ok(())
}

/// Per-connection place where replies from other shards land.
pub(super) struct ConnFwd {
    ready: RefCell<Vec<Reply>>,
    notify: tokio::sync::Notify,
    /// Other shards may hold WATCH or tracking state for this connection.
    pub(super) remote_state: Cell<bool>,
}

/// Shard-local view of the fabric.
pub(super) struct Forwarding {
    pub(super) fabric: Arc<Fabric>,
    pub(super) me: usize,
    conns: RefCell<HashMap<u64, Rc<ConnFwd>>>,
    /// Blocking commands forwarded here and still running, by their client `(src, conn)`.
    blocked: RefCell<HashMap<(usize, u64), Rc<ForwardCancel>>>,
}

/// Tells a forwarded blocking command that its client left the home shard, so it gives up
/// its waiter as a local one does when its socket closes.
#[derive(Default)]
pub(super) struct ForwardCancel {
    cancelled: Cell<bool>,
    notify: tokio::sync::Notify,
}

impl ForwardCancel {
    fn cancel(&self) {
        self.cancelled.set(true);
        self.notify.notify_waiters();
    }

    pub(super) async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();

            if self.cancelled.get() {
                return;
            }
            notified.await;
        }
    }
}

impl Forwarding {
    pub(super) fn new(fabric: Arc<Fabric>, me: usize) -> Self {
        Self {
            fabric,
            me,
            conns: RefCell::new(HashMap::new()),
            blocked: RefCell::new(HashMap::new()),
        }
    }

    /// Waits for the next connection handed to this shard.
    pub(super) async fn next_client(
        &self,
    ) -> io::Result<(TcpStream, SocketAddr, ConnectionPermit)> {
        let port = &self.fabric.ports[self.me];

        loop {
            let item = port
                .accept
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pop_front();

            if let Some(item) = item {
                item.stream.set_nonblocking(true)?;

                return Ok((TcpStream::from_std(item.stream)?, item.peer, item.permit));
            }

            port.accept_notify.notified().await;
        }
    }

    pub(super) fn register(&self, conn: u64) -> Rc<ConnFwd> {
        let entry = Rc::new(ConnFwd {
            ready: RefCell::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            remote_state: Cell::new(false),
        });

        self.conns.borrow_mut().insert(conn, Rc::clone(&entry));
        entry
    }

    pub(super) fn unregister(&self, conn: u64) {
        let entry = self.conns.borrow_mut().remove(&conn);

        if entry.is_some_and(|entry| entry.remote_state.get()) {
            for dst in (0..self.fabric.count).filter(|dst| *dst != self.me) {
                self.fabric
                    .send(self.me, dst, Message::Forget { src: self.me, conn });
            }
        }
    }

    fn deliver(&self, reply: Reply) {
        if let Some(entry) = self.conns.borrow().get(&reply.conn) {
            entry.ready.borrow_mut().push(reply);
            entry.notify.notify_one();
        }
    }
}

struct Lane {
    frames: Vec<u8>,
    count: u32,
}

struct Mark {
    dst: usize,
    offset: usize,
}

/// Foreign commands of the current pipeline slice, in issue order.
pub(super) struct Forwarder {
    lanes: Vec<Lane>,
    marks: Vec<Mark>,
    next_token: u64,
    /// Bit `dst` is set once a command that may write went to shard `dst` (see
    /// `TxSession::forwarded_shards`). Reads do not count: a WAIT waits for writes.
    wrote: u64,
}

impl Forwarder {
    pub(super) fn new(shard_count: usize) -> Self {
        Self {
            lanes: (0..shard_count)
                .map(|_| Lane {
                    frames: Vec::new(),
                    count: 0,
                })
                .collect(),
            marks: Vec::new(),
            next_token: 1,
            wrote: 0,
        }
    }

    /// The shards this connection has sent commands that may write to so far.
    pub(super) fn wrote(&self) -> u64 {
        self.wrote
    }

    pub(super) fn is_empty(&self) -> bool {
        self.marks.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.marks.len()
    }

    /// Queues `frame` for `dst`; its reply belongs at `offset` in the output. `writes` when the
    /// command may change data there.
    pub(super) fn push(&mut self, dst: usize, frame: &[u8], offset: usize, writes: bool) {
        let lane = &mut self.lanes[dst];

        if writes {
            self.wrote |= 1 << dst;
        }

        lane.frames.extend_from_slice(frame);
        lane.count += 1;
        self.marks.push(Mark { dst, offset });
    }

    /// Sends the queued frames and waits for every reply. Returns the lane order and replies.
    async fn exchange(
        &mut self,
        forwarding: &Forwarding,
        entry: &ConnFwd,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
    ) -> io::Result<(Vec<(usize, u64)>, Vec<Reply>)> {
        let mut tokens: Vec<(usize, u64)> = Vec::new();

        for (dst, lane) in self.lanes.iter_mut().enumerate() {
            if lane.count == 0 {
                continue;
            }

            let token = self.next_token;

            self.next_token += 1;
            tokens.push((dst, token));
            forwarding.fabric.send(
                forwarding.me,
                dst,
                Message::Request(Request {
                    src: forwarding.me,
                    conn,
                    token,
                    database: session.database,
                    resp: session.resp,
                    tracking: session.tracking,
                    spawn: false,
                    count: lane.count,
                    frames: std::mem::take(&mut lane.frames),
                    username: Arc::clone(&auth.username),
                    rules: Arc::clone(&auth.rules),
                }),
            );
            lane.count = 0;
        }

        let mut replies: Vec<Option<Reply>> = tokens.iter().map(|_| None).collect();
        let mut missing = tokens.len();

        while missing > 0 {
            let arrived: Vec<Reply> = entry.ready.borrow_mut().drain(..).collect();

            for reply in arrived {
                if let Some(index) = tokens.iter().position(|(_, token)| *token == reply.token) {
                    replies[index] = Some(reply);
                    missing -= 1;
                }
            }

            if missing > 0 {
                entry.notify.notified().await;
            }
        }

        let replies = replies
            .into_iter()
            .map(|reply| reply.expect("reply arrived"))
            .collect();

        Ok((tokens, replies))
    }

    /// Sends the queued frames, waits for every reply and splices them into `output`.
    pub(super) async fn resolve(
        &mut self,
        forwarding: &Forwarding,
        entry: &ConnFwd,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
        output: &mut BoundedOutput,
    ) -> io::Result<()> {
        if self.marks.is_empty() {
            return Ok(());
        }

        let (tokens, replies) = self
            .exchange(forwarding, entry, conn, session, auth)
            .await?;
        let old = output.to_vec();
        let mut cursors = vec![(0usize, 0usize); tokens.len()];
        let mut written = 0usize;

        output.clear();

        for mark in self.marks.drain(..) {
            let lane = tokens
                .iter()
                .position(|(dst, _)| *dst == mark.dst)
                .expect("lane was sent");
            let reply = &replies[lane];
            let (command, data_at) = &mut cursors[lane];
            let length =
                *reply.lens.get(*command).ok_or_else(|| {
                    io::Error::other("forwarded reply is shorter than its request")
                })? as usize;

            output.write_all(&old[written..mark.offset])?;
            written = mark.offset;
            output.write_all(&reply.data[*data_at..*data_at + length])?;
            *data_at += length;
            *command += 1;
        }

        output.write_all(&old[written..])?;
        Ok(())
    }

    /// Sends `frame` to every other shard and returns their replies in shard order.
    pub(super) async fn broadcast(
        &mut self,
        forwarding: &Forwarding,
        entry: &ConnFwd,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
        frame: &[u8],
    ) -> io::Result<Vec<Vec<u8>>> {
        for dst in 0..self.lanes.len() {
            if dst != forwarding.me {
                self.push(dst, frame, 0, false);
            }
        }

        self.marks.clear();
        let (_, replies) = self
            .exchange(forwarding, entry, conn, session, auth)
            .await?;

        Ok(replies.into_iter().map(|reply| reply.data).collect())
    }
}

impl Forwarder {
    /// Sends `frames` (`count` commands) to `dst` on their own and waits for the reply.
    /// Callers settle the queued pipeline first so command order holds.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn call(
        &mut self,
        forwarding: &Forwarding,
        entry: &ConnFwd,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
        dst: usize,
        frames: Vec<u8>,
        count: u32,
        spawn: bool,
    ) -> Reply {
        let token = self.send_request(forwarding, conn, session, auth, dst, frames, count, spawn);

        wait_reply(entry, token).await
    }

    /// `call` for a command that may block on its owner shard: if the client hangs up while
    /// it waits, the owner is told to cancel it, so nothing is popped for nobody there.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn call_blocking(
        &mut self,
        forwarding: &Forwarding,
        entry: &ConnFwd,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
        dst: usize,
        frame: Vec<u8>,
        peer: &TcpStream,
    ) -> io::Result<Reply> {
        let token = self.send_request(forwarding, conn, session, auth, dst, frame, 1, true);

        tokio::select! {
            biased;
            reply = wait_reply(entry, token) => Ok(reply),
            () = super::connection::peer_closed(Some(super::connection::Peer::Socket(peer))) => {
                forwarding.fabric.send(
                    forwarding.me,
                    dst,
                    Message::Cancel { src: forwarding.me, conn },
                );

                Err(super::connection::closed_while_blocked())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn send_request(
        &mut self,
        forwarding: &Forwarding,
        conn: u64,
        session: &TxSession,
        auth: &crate::acl::ConnectionAuth,
        dst: usize,
        frames: Vec<u8>,
        count: u32,
        spawn: bool,
    ) -> u64 {
        let token = self.next_token;

        self.next_token += 1;
        // EXEC, blocking pops and the other commands sent alone: counted as writes.
        self.wrote |= 1 << dst;
        forwarding.fabric.send(
            forwarding.me,
            dst,
            Message::Request(Request {
                src: forwarding.me,
                conn,
                token,
                database: session.database,
                resp: session.resp,
                tracking: session.tracking,
                spawn,
                count,
                frames,
                username: Arc::clone(&auth.username),
                rules: Arc::clone(&auth.rules),
            }),
        );

        token
    }
}

async fn wait_reply(entry: &ConnFwd, token: u64) -> Reply {
    loop {
        let found = {
            let mut ready = entry.ready.borrow_mut();

            ready
                .iter()
                .position(|reply| reply.token == token)
                .map(|index| ready.swap_remove(index))
        };

        if let Some(reply) = found {
            return reply;
        }

        entry.notify.notified().await;
    }
}

fn remote_permit() -> Option<ConnectionPermit> {
    let limiter = ConnectionLimiter::new(crate::config::ConnectionLimits {
        max_global: usize::MAX,
        max_per_ip: usize::MAX,
        max_per_user: usize::MAX,
        max_commands_per_second: 0,
    });

    limiter.try_acquire(std::net::Ipv4Addr::LOCALHOST.into())
}

/// Executes forwarded frames on this (the owning) shard.
pub(super) async fn remote_loop(state: Rc<ShardState>, shutdown: ShutdownToken) {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return;
    };
    let Some(mut permit) = remote_permit() else {
        return;
    };
    let mut batch = Vec::new();

    loop {
        forwarding.fabric.ports[forwarding.me]
            .notify
            .notified()
            .await;
        batch.clear();
        forwarding.fabric.drain(forwarding.me, &mut batch);
        let mut handled = 0usize;
        // Replies wait for one commit that covers every request of this batch.
        let mut wal_group = OpenWalGroup::new(&state);
        let mut replies: Vec<(usize, Reply)> = Vec::new();

        for message in batch.drain(..) {
            match message {
                Message::Reply(reply) => forwarding.deliver(reply),
                Message::Invalidate { conn, keys } => {
                    push_invalidations(&state, vec![(conn, keys)])
                }
                Message::Forget { src, conn } => {
                    let id = remote_id(src, conn);

                    state.store.borrow_mut().unwatch(id);
                    state.tracker.borrow_mut().forget(id);
                }

                Message::Untrack { src, conn } => {
                    state.tracker.borrow_mut().forget(remote_id(src, conn));
                }

                Message::Publish(delivery) => {
                    super::pubsub::publish_classic(&state, &delivery.channel, &delivery.message);
                    forwarding.fabric.release_publish(forwarding.me, &delivery);
                }

                Message::Broadcast(broadcast) => broadcast.apply_on(&state),

                Message::Move(message) => super::moveslots::on_message(&state, message),

                Message::Cancel { src, conn } => {
                    if let Some(cancel) = forwarding.blocked.borrow().get(&(src, conn)) {
                        cancel.cancel();
                    }
                }

                Message::Unwatch { src, conn } => {
                    state.store.borrow_mut().unwatch(remote_id(src, conn));
                }

                // Every key of it now belongs to another shard (see `SlotMover::moved`).
                Message::Request(request)
                    if forwarding.fabric.mover.moved()
                        && super::moveslots::request_owner(
                            &state,
                            &request.frames,
                            request.count,
                        )
                        .is_some() =>
                {
                    let owner =
                        super::moveslots::request_owner(&state, &request.frames, request.count)
                            .unwrap_or(forwarding.me);

                    forwarding
                        .fabric
                        .send(forwarding.me, owner, Message::Request(request));
                }

                Message::Request(request) if request.spawn => {
                    let state = Rc::clone(&state);
                    let shutdown = shutdown.clone();
                    let client = (request.src, request.conn);
                    // Registered before the task first runs: a Cancel later in this batch
                    // must find it.
                    let cancel = Rc::new(ForwardCancel::default());

                    forwarding
                        .blocked
                        .borrow_mut()
                        .insert(client, Rc::clone(&cancel));
                    tokio::task::spawn_local(async move {
                        let Some(mut permit) = remote_permit() else {
                            return;
                        };
                        let src = request.src;
                        let peer = Some(super::connection::Peer::Forwarded(&cancel));
                        let reply =
                            execute_request(&state, &mut permit, &shutdown, &request, peer).await;

                        if let Some(forwarding) = state.forwarding.as_ref() {
                            let mut blocked = forwarding.blocked.borrow_mut();

                            if blocked.get(&client).is_some_and(|c| Rc::ptr_eq(c, &cancel)) {
                                blocked.remove(&client);
                            }
                        }

                        if let Some(forwarding) = state.forwarding.as_ref() {
                            forwarding
                                .fabric
                                .send(forwarding.me, src, Message::Reply(reply));
                        }
                    });
                }

                Message::Request(request) => {
                    let src = request.src;
                    let reply = execute_request_in(
                        &state,
                        &mut permit,
                        &shutdown,
                        &request,
                        &mut wal_group,
                        None,
                    )
                    .await;

                    replies.push((src, reply));
                }
            }

            handled += 1;

            if handled.is_multiple_of(REMOTE_MESSAGES_PER_TURN) {
                // Settle what ran so far before giving other tasks the turn.
                settle_replies(&state, &mut wal_group, std::mem::take(&mut replies)).await;

                tokio::task::yield_now().await;
            }
        }

        settle_replies(&state, &mut wal_group, replies).await;
    }
}

fn send_replies(state: &ShardState, replies: Vec<(usize, Reply)>) {
    if let Some(forwarding) = state.forwarding.as_ref() {
        for (src, reply) in replies {
            forwarding
                .fabric
                .send(forwarding.me, src, Message::Reply(reply));
        }
    }
}

/// Commits the batch's WAL group, then sends its replies. With the sync worker the loop does not
/// wait for the disk: replies go out from a task once the worker's barrier passes, so the next batch
/// keeps executing (and joins the next worker sync) instead of queueing behind this one's fsync.
async fn settle_replies(
    state: &Rc<ShardState>,
    wal_group: &mut OpenWalGroup<'_>,
    replies: Vec<(usize, Reply)>,
) {
    if replies.is_empty() {
        let _ = wal_group.commit().await;

        return;
    }

    match wal_group.commit_deferred().await {
        Ok(Some(wait)) => {
            let state = Rc::clone(state);

            tokio::task::spawn_local(async move {
                let mut replies = replies;

                if wait.wait(&state).await.is_err() {
                    replies.iter_mut().for_each(|(_, reply)| fail_reply(reply));
                }

                send_replies(&state, replies);
            });
        }
        Ok(None) => send_replies(state, replies),
        Err(_) => {
            // The batch's records did not reach the log: no client may see its OK.
            let mut replies = replies;

            replies.iter_mut().for_each(|(_, reply)| fail_reply(reply));
            send_replies(state, replies);
        }
    }
}

/// Runs a future and notes in `suspended` whether it ever waited (returned `Pending`).
struct NoteSuspended<'f, 'c, F> {
    future: std::pin::Pin<&'f mut F>,
    suspended: &'c std::cell::Cell<bool>,
}

impl<F: std::future::Future> std::future::Future for NoteSuspended<'_, '_, F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let polled = self.future.as_mut().poll(context);

        if polled.is_pending() {
            self.suspended.set(true);
        }
        polled
    }
}

/// Replace every command's answer in `reply` with a persistence error, except a command
/// that logged nothing and only handed elements to parked clients: those clients already
/// have them (and answer OK, see `skip_records_written_while_parked`), so an error would
/// make its client push them again.
fn fail_reply(reply: &mut Reply) {
    const ERROR: &[u8] = b"-MISCONF persistence error\r\n";

    let data = std::mem::take(&mut reply.data);
    let mut start = 0usize;

    for (index, length) in reply.lens.iter_mut().enumerate() {
        let end = start + *length as usize;

        if reply.unlogged.contains(&(index as u32)) {
            reply.data.extend_from_slice(&data[start..end]);
        } else {
            reply.data.extend_from_slice(ERROR);
            *length = ERROR.len() as u32;
        }
        start = end;
    }
}

async fn execute_request(
    state: &Rc<ShardState>,
    permit: &mut ConnectionPermit,
    shutdown: &ShutdownToken,
    request: &Request,
    peer: Option<super::connection::Peer<'_>>,
) -> Reply {
    let mut wal_group = OpenWalGroup::new(state);
    let mut reply =
        execute_request_in(state, permit, shutdown, request, &mut wal_group, peer).await;

    if wal_group.commit().await.is_err() {
        fail_reply(&mut reply);
    }

    reply
}

/// Runs `request` inside the caller's WAL group and leaves that group open: the caller commits
/// once for every request it ran, so forwarded writes share one `always` sync instead of each
/// waiting for its own (a batch executed one request at a time collapsed grouping to ~1.3 requests
/// per fsync with two shards, against ~20 with one).
async fn execute_request_in(
    state: &Rc<ShardState>,
    permit: &mut ConnectionPermit,
    shutdown: &ShutdownToken,
    request: &Request,
    wal_group: &mut OpenWalGroup<'_>,
    peer: Option<super::connection::Peer<'_>>,
) -> Reply {
    let limits = state.published_config.protocol;
    let id = remote_id(request.src, request.conn);
    let mut session = TxSession::new(id);

    session.resp = request.resp;
    session.database = request.database;
    session.tracking = request.tracking;
    // The home shard already ran the NOAUTH, rate and ACL checks; the ACL is checked again
    // here (and inside scripts) with the caller's own rules.
    let mut auth = crate::acl::ConnectionAuth {
        authenticated: true,
        username: Arc::clone(&request.username),
        rules: Arc::clone(&request.rules),
    };
    let mut rate = CommandRateLimiter::new(0);
    let mut output = BoundedOutput::with_capacity(0, state.live.max_output_buffer_bytes.get());
    let mut lens = Vec::with_capacity(request.count as usize);
    let mut unlogged = Vec::new();
    let suspended = std::cell::Cell::new(false);
    let mut consumed = 0usize;

    state.store.borrow().sample_time();
    let _time_sample = TimeSample { state };

    // A forwarded transaction (MULTI, its queued commands, EXEC) whose part failed: the rest
    // does not run. Without its MULTI, its commands would run one by one outside the
    // transaction and EXEC would find no MULTI; without a queued command, EXEC would run the
    // rest. Every later command gets the failure's error instead.
    let mut transaction_failed: Option<&'static [u8]> = None;
    // Where the open forwarded transaction's MULTI is in `lens`: an EXEC that was a pure
    // delivery keeps its MULTI's and its `+QUEUED` answers too.
    let mut transaction_start: Option<u32> = None;

    while lens.len() < request.count as usize {
        let before = output.len();
        let failed = match parse_command_with_limits(&request.frames[consumed..], limits) {
            Ok(Some((_, length))) if transaction_failed.is_some() => {
                consumed += length;
                transaction_failed
            }

            Ok(Some((command, length))) => {
                consumed += length;

                let in_transaction = session.is_multi() || matches!(command, Command::Multi);

                if matches!(command, Command::Multi) && !session.is_multi() {
                    transaction_start = Some(lens.len() as u32);
                }
                let synced = if wal_group_eligible(WalGroupCommand::Parsed(&command), &session) {
                    wal_group.begin();
                    true
                } else {
                    let committed = wal_group.commit().await.is_ok();

                    #[cfg(test)]
                    let committed = committed && !forced_commit_failure(state);

                    committed
                };

                // Only with a client parked here can the command be a pure delivery.
                suspended.set(false);
                let deliveries_before = {
                    let waiters = state.waiters.borrow();

                    (!waiters.is_empty()).then(|| waiters.unlogged_deliveries())
                };
                // Routed here before another shard took the slot over: the client sends it
                // again and reaches the new owner.
                let misrouted = state
                    .forwarding
                    .as_ref()
                    .is_some_and(|forwarding| forwarding.fabric.mover.moved())
                    && super::moveslots::runs_elsewhere(state, &session, &command);
                let failed = if misrouted {
                    if matches!(command, Command::Exec) {
                        state.store.borrow_mut().unwatch(id);
                        session.finish_exec();
                        session.clear_watches();
                    }

                    Some(super::moveslots::TRYAGAIN_REPLY)
                } else if synced {
                    let dispatch = std::pin::pin!(dispatch_command(
                        state,
                        &mut output,
                        &mut session,
                        &mut auth,
                        permit,
                        &mut rate,
                        command,
                        &request.frames[consumed - length..consumed],
                        id,
                        shutdown,
                        peer,
                        None,
                    ));
                    let dispatched = NoteSuspended {
                        future: dispatch,
                        suspended: &suspended,
                    }
                    .await;

                    dispatched
                        .is_err()
                        .then_some(&b"-ERR forwarded command failed\r\n"[..])
                } else {
                    // The batch's records did not reach the log: a persistence error, as the
                    // home shard gives when its own commit fails.
                    Some(&b"-MISCONF persistence error\r\n"[..])
                };

                if let Some(served_by) = session.take_served_after_park() {
                    wal_group.skip_records_written_while_parked(served_by);
                }
                // A command that waited (a parked or retried blocking command) may see other
                // clients' deliveries in the shard-wide count: only one that ran straight
                // through counts as a delivery of its own.
                if failed.is_none()
                    && !suspended.get()
                    && deliveries_before
                        .is_some_and(|before| state.waiters.borrow().unlogged_deliveries() > before)
                {
                    let index = lens.len() as u32;

                    match transaction_start.take() {
                        Some(start) if !session.is_multi() => unlogged.extend(start..=index),
                        _ => unlogged.push(index),
                    }
                }
                if !session.is_multi() {
                    transaction_start = None;
                }
                if in_transaction && failed.is_some() {
                    transaction_failed = failed;
                }
                failed
            }

            Ok(None) | Err(_) => Some(&b"-ERR forwarded command failed\r\n"[..]),
        };

        if let Some(error) = failed {
            output.truncate_to(before);
            let _ = output.write_all(error);
        }

        lens.push((output.len() - before) as u32);
    }

    Reply {
        conn: request.conn,
        token: request.token,
        lens,
        data: output.to_vec(),
        unlogged,
    }
}

/// The rate, NOAUTH and ACL checks `gate` runs, for a command this shard does not dispatch
/// itself: it is forwarded to its owner shard or answered here over all shards (SCAN). The
/// owner shard re-checks the ACL with the caller's rules but knows nothing of this connection's
/// login state or rate budget. `rate` is `None` when the command is still counted by a local
/// `gate` afterwards.
pub(super) fn forward_refusal(
    auth: &crate::acl::ConnectionAuth,
    rate: Option<&mut CommandRateLimiter>,
    command: &Command<'_>,
) -> Option<Response<'static>> {
    if rate.is_some_and(|rate| !rate.allow()) {
        return Some(Response::Error(crate::admission::RATE_LIMIT_ERROR));
    }

    if !auth.authenticated {
        return Some(Response::Error(b"NOAUTH Authentication required."));
    }

    auth.rules
        .authorize(command)
        .err()
        .map(crate::acl::noperm_response)
}

pub(super) enum Owner {
    Local,
    Remote(usize),
    /// Another node of a multi-node cluster owns this slot: the answer is `MOVED`.
    Node(u16),
    /// Keys live on different shards.
    Split,
}

pub(super) const SWAPDB_SHARDS_REPLY: &[u8] =
    b"-ERR SWAPDB is not supported with more than one shard\r\n";
pub(super) const FAN_IN_MULTI_REPLY: &[u8] =
    b"-ERR this command runs on every shard and cannot be queued in MULTI with more than one shard\r\n";
/// Keys of one command on different shards: the Redis Cluster answer, which every client
/// recognises (a client that routes itself refuses the same command before sending it).
pub(super) const SPLIT_REPLY: &[u8] = CROSSSLOT_REPLY;
/// Which shard runs `command`, and whether every key of the command is in one hash slot
/// (the `CROSSSLOT` rule the lean path relies on). The usual case, keys in one slot, costs
/// one `command_slot` and no allocation; only a cross-slot command walks the keys to tell a
/// split over shards from a cross-slot command on this shard. A hot key lives in the
/// process-wide hot store, so it never decides the owner: a command on hot keys alone runs
/// here, in order with the hot path.
pub(super) fn command_owner(
    state: &ShardState,
    forwarding: &Forwarding,
    command: &Command<'_>,
) -> (Owner, bool) {
    let hot = state.hot_store.is_some() && !state.published_config.hot_keys.is_empty();

    // A sharded channel has a slot but is not a key: a subscription cannot be forwarded, so
    // the connection's shard answers (with `MOVED` when another shard owns the channel).
    let sharded_channel = matches!(
        command,
        Command::ShardPublish { .. } | Command::ShardSubscribe(_) | Command::ShardUnsubscribe(_)
    );

    if sharded_channel {
        return (Owner::Local, true);
    }

    if !hot {
        match crate::routing::command_slot(command) {
            Ok(None) => return (Owner::Local, true),
            Ok(Some(slot)) => {
                let found = forwarding.fabric.slot_owner(slot);

                return (
                    if found == forwarding.me {
                        Owner::Local
                    } else if found < forwarding.fabric.count {
                        Owner::Remote(found)
                    } else {
                        Owner::Node(slot)
                    },
                    true,
                );
            }
            Err(_) => {}
        }
    }

    let one_slot = crate::routing::command_slot(command).is_ok();
    let mut owner = None;

    for key in crate::routing::command_keys(command) {
        if hot && crate::hot_store::matches_hot_key(&state.published_config.hot_keys, key) {
            continue;
        }
        let slot = hash_slot(key);
        let found = forwarding.fabric.slot_owner(slot);

        if found >= forwarding.fabric.count {
            return (Owner::Node(slot), one_slot);
        }

        match owner {
            None => owner = Some(found),
            Some(previous) if previous != found => return (Owner::Split, one_slot),
            Some(_) => {}
        }
    }

    (
        match owner {
            Some(found) if found != forwarding.me => Owner::Remote(found),
            _ => Owner::Local,
        },
        one_slot,
    )
}

/// Shard that owns the hot command's key, when it is not this one.
#[inline]
pub(super) fn hot_remote_owner(
    forwarding: &Forwarding,
    command: &crate::protocol::HotCommand<'_>,
) -> Option<(usize, u16)> {
    let slot = hot_command_slot(command)?;
    let owner = forwarding.fabric.slot_owner(slot);

    (owner != forwarding.me).then_some((owner, slot))
}

/// Answers `MOVED` when another node of the cluster owns `slot` (`owner` is the slot
/// table entry); `false` when the owner is a shard of this process.
pub(super) fn redirect_to_node(
    forwarding: &Forwarding,
    output: &mut impl std::io::Write,
    slot: u16,
    owner: usize,
) -> io::Result<bool> {
    let count = forwarding.fabric.count;

    if owner < count {
        return Ok(false);
    }
    match forwarding.fabric.topology() {
        Some(topology) => {
            crate::runtime::cluster::write_moved(output, &topology.get(), slot, owner, count)?
        }
        None => output.write_all(b"-CLUSTERDOWN Hash slot not served\r\n")?,
    }

    Ok(true)
}

/// Sends queued foreign commands and splices their replies into `output`.
pub(super) async fn settle(
    state: &ShardState,
    forwarder: &mut Forwarder,
    entry: Option<&Rc<ConnFwd>>,
    conn: u64,
    session: &TxSession,
    auth: &crate::acl::ConnectionAuth,
    output: &mut BoundedOutput,
) -> io::Result<()> {
    if forwarder.is_empty() {
        return Ok(());
    }

    let (Some(forwarding), Some(entry)) = (state.forwarding.as_ref(), entry) else {
        return Ok(());
    };

    forwarder
        .resolve(forwarding, entry, conn, session, auth, output)
        .await
}

pub(super) struct FwdGuard {
    pub(super) state: Rc<ShardState>,
    pub(super) conn: u64,
}

impl Drop for FwdGuard {
    fn drop(&mut self) {
        if let Some(forwarding) = &self.state.forwarding {
            forwarding.unregister(self.conn);
        }
    }
}

/// Commands that run on every shard and whose replies are merged.
#[derive(Clone, Copy)]
pub(super) enum Fan {
    Sum,
    Flush,
    /// `KEYS`: the arrays of every shard, joined.
    Concat,
    /// `RANDOMKEY`: one of the shards' answers that is not nil.
    AnyKey,
    /// `JSON.FIND`: the shards' ascending key pages merged, cut to `COUNT`.
    SortedKeys(usize),
}

/// `fan_kind` as this shard applies it: in single-port mode every keyless command in the
/// list runs on every shard and waits for their answers; in per-shard mode cluster clients
/// send `DBSIZE`, `KEYS`, `FLUSHALL` and the others to each node themselves. `PUBLISH` is
/// never fanned this way: the connection loop hands it to the other shards as a
/// [`Message::Publish`] without waiting (see `publish_everywhere`).
pub(super) fn fan_for(state: &ShardState, command: &Command<'_>) -> Option<Fan> {
    fan_kind(command).filter(|_| state.single_port && !matches!(command, Command::Publish { .. }))
}

/// Publish deliveries a shard may hold, from all other shards together, before new ones are
/// dropped: pub/sub is best effort (Redis drops a slow subscriber too), and a shard stuck in a
/// long command must not let its queue grow with every publish in the process.
const PUBLISH_QUEUE_MESSAGES: usize = 16_384;

/// The same budget in bytes (channel plus message, per destination), so a few huge messages are
/// bounded too.
const PUBLISH_QUEUE_BYTES: usize = 64 << 20;

/// Publish deliveries dropped by a full budget since the process started (`INFO`
/// `pubsub_cluster_dropped`).
static PUBLISH_DROPPED: AtomicU64 = AtomicU64::new(0);

pub(super) fn publish_dropped() -> u64 {
    PUBLISH_DROPPED.load(Ordering::Relaxed)
}

/// Sends a classic `PUBLISH` the caller has already run (and that passed its auth, ACL and
/// rate checks there) to every other shard, without waiting for them: a subscriber may sit on
/// any shard. The caller's reply counts its own shard's receivers.
pub(super) fn publish_everywhere(forwarding: &Forwarding, channel: &[u8], message: &[u8]) {
    let fabric = &forwarding.fabric;
    let cost = channel.len() + message.len();
    let mut delivery: Option<Arc<PublishDelivery>> = None;

    for dst in (0..fabric.shard_count()).filter(|dst| *dst != forwarding.me) {
        if !fabric.reserve_publish(dst, cost) {
            PUBLISH_DROPPED.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        // One copy for every destination, made only once one of them has room.
        let shared = delivery.get_or_insert_with(|| {
            Arc::new(PublishDelivery {
                channel: channel.to_vec(),
                message: message.to_vec(),
            })
        });

        fabric.send(forwarding.me, dst, Message::Publish(Arc::clone(shared)));
    }
}

pub(super) fn fan_kind(command: &Command<'_>) -> Option<Fan> {
    match command {
        Command::DbSize | Command::Publish { .. } => Some(Fan::Sum),
        Command::FlushDb | Command::FlushAll => Some(Fan::Flush),
        Command::JsonIndex {
            op: crate::protocol::JsonIndexKind::Create | crate::protocol::JsonIndexKind::Drop,
            ..
        } => Some(Fan::Flush),
        Command::JsonIndex {
            op: crate::protocol::JsonIndexKind::List,
            ..
        } => Some(Fan::Concat),
        Command::JsonIndex {
            op: crate::protocol::JsonIndexKind::Find,
            args,
        } => Some(Fan::SortedKeys(find_count(args))),
        Command::Keys(_) => Some(Fan::Concat),
        Command::RandomKey => Some(Fan::AnyKey),
        _ => None,
    }
}

/// The `COUNT` of a `JSON.FIND`, clamped like the command itself (default 100, at most 1000).
fn find_count(args: &[crate::protocol::Bytes<'_>]) -> usize {
    args.windows(2)
        .find(|pair| pair[0].eq_ignore_ascii_case(b"COUNT"))
        .and_then(|pair| std::str::from_utf8(&pair[1]).ok()?.parse().ok())
        .map_or(100, |count: usize| count.clamp(1, 1000))
}

/// The bulk strings of a RESP array reply, or `None` when it is not a well-formed array of bulks.
fn bulk_array(reply: &[u8]) -> Option<Vec<&[u8]>> {
    let (count, mut rest) = array_reply(reply)?;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        let header_end = rest.windows(2).position(|pair| pair == b"\r\n")?;
        let length: usize = std::str::from_utf8(rest.strip_prefix(b"$")?.get(..header_end - 1)?)
            .ok()?
            .parse()
            .ok()?;
        let start = header_end + 2;

        items.push(rest.get(start..start + length)?);
        rest = rest.get(start + length + 2..)?;
    }

    Some(items)
}

fn integer_reply(reply: &[u8]) -> Option<i64> {
    std::str::from_utf8(reply.strip_prefix(b":")?.strip_suffix(b"\r\n")?)
        .ok()?
        .parse()
        .ok()
}

/// Splits a RESP array reply into its element count and the bytes after the header.
fn array_reply(reply: &[u8]) -> Option<(usize, &[u8])> {
    let header_end = reply.windows(2).position(|pair| pair == b"\r\n")?;
    let count = std::str::from_utf8(reply.strip_prefix(b"*")?.get(..header_end - 1)?)
        .ok()?
        .parse()
        .ok()?;

    Some((count, &reply[header_end + 2..]))
}

/// Merges the local reply with the other shards' replies.
pub(super) fn merge_fan(
    fan: Fan,
    local: &[u8],
    remote: &[Vec<u8>],
    output: &mut BoundedOutput,
) -> io::Result<()> {
    let replies = || std::iter::once(local).chain(remote.iter().map(Vec::as_slice));

    match fan {
        Fan::Sum => {
            let mut total = 0i64;

            for reply in replies() {
                match integer_reply(reply) {
                    Some(value) => total += value,
                    None => return output.write_all(reply),
                }
            }

            write!(output, ":{total}\r\n")
        }

        Fan::Flush => output.write_all(
            replies()
                .find(|reply| *reply != b"+OK\r\n")
                .unwrap_or(local),
        ),

        Fan::Concat => {
            let mut total = 0usize;
            let mut bodies = Vec::new();

            for reply in replies() {
                let Some((count, body)) = array_reply(reply) else {
                    return output.write_all(reply);
                };

                total += count;
                bodies.push(body);
            }

            write!(output, "*{total}\r\n")?;

            for body in bodies {
                output.write_all(body)?;
            }

            Ok(())
        }

        Fan::SortedKeys(count) => {
            let mut keys = Vec::new();

            for reply in replies() {
                match bulk_array(reply) {
                    Some(items) => keys.extend(items),
                    None => return output.write_all(reply),
                }
            }

            keys.sort_unstable();
            keys.dedup();
            keys.truncate(count);
            write!(output, "*{}\r\n", keys.len())?;

            for key in keys {
                write!(output, "${}\r\n", key.len())?;
                output.write_all(key)?;
                output.write_all(b"\r\n")?;
            }

            Ok(())
        }

        Fan::AnyKey => {
            let found: Vec<&[u8]> = replies()
                .filter(|reply| reply.starts_with(b"$") && *reply != b"$-1\r\n")
                .collect();

            match found.len() {
                0 => output.write_all(local),
                len => {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |elapsed| elapsed.subsec_nanos() as usize);

                    output.write_all(found[nanos % len])
                }
            }
        }
    }
}

/// Raw frames of the commands a connection queued after MULTI, replayed on the owning shard at EXEC.
#[derive(Default)]
pub(super) struct MultiLog {
    frames: Vec<u8>,
    count: u32,
}

impl MultiLog {
    pub(super) fn clear(&mut self) {
        self.frames.clear();
        self.count = 0;

        // A large transaction's buffer goes with it instead of staying with the connection.
        if self.frames.capacity() > MULTI_LOG_KEPT_BYTES {
            self.frames = Vec::new();
        }
    }

    /// Keeps `frame` when the home shard answered it with `+QUEUED`.
    pub(super) fn record(&mut self, frame: &[u8], output: &[u8]) {
        if output.ends_with(b"+QUEUED\r\n") {
            self.frames.extend_from_slice(frame);
            self.count += 1;
        }
    }
}

const MULTI_FRAME: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
const MULTI_LOG_KEPT_BYTES: usize = 64 * 1024;
const SCAN_SHARD_SHIFT: u32 = 56;

fn unwatch_remote(forwarding: &Forwarding, session: &TxSession, conn: u64) {
    let Some(slot) = session.watch_slot() else {
        return;
    };
    let dst = forwarding.fabric.slot_owner(slot);

    if dst != forwarding.me {
        forwarding.fabric.send(
            forwarding.me,
            dst,
            Message::Unwatch {
                src: forwarding.me,
                conn,
            },
        );
    }
}

/// Handles a command that cannot ride the pipelined lanes: blocking commands, scripts,
/// MULTI/EXEC/WATCH, CLIENT TRACKING and SCAN. Returns `false` when the home shard should run
/// the command itself. Queued pipeline replies are settled before this is called.
#[allow(clippy::too_many_arguments)]
pub(super) async fn forward_special(
    state: &Rc<ShardState>,
    forwarder: &mut Forwarder,
    entry: &Rc<ConnFwd>,
    conn: u64,
    session: &mut TxSession,
    auth: &crate::acl::ConnectionAuth,
    rate: &mut CommandRateLimiter,
    output: &mut BoundedOutput,
    command: &Command<'_>,
    frame: &[u8],
    owner: &Owner,
    log: &mut MultiLog,
    peer: &TcpStream,
) -> io::Result<bool> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return Ok(false);
    };
    let shards = forwarding.fabric.shard_count();

    if session.is_multi() {
        match command {
            Command::Exec => {
                // An aborted transaction runs nowhere: the home shard answers EXECABORT.
                if session.is_aborted() {
                    unwatch_remote(forwarding, session, conn);
                    log.clear();

                    return Ok(false);
                }

                let slot = match crate::transaction::transaction_slot(session.queued()) {
                    Ok(Some(slot)) => Some(slot),
                    Ok(None) => session.watch_slot(),
                    Err(_) => None,
                };
                let Some(slot) = slot else {
                    return Ok(false);
                };
                let dst = forwarding.fabric.slot_owner(slot);

                if session.watch_slot().is_some_and(|watched| watched != slot) {
                    unwatch_remote(forwarding, session, conn);
                    session.finish_exec();
                    session.clear_watches();
                    log.clear();
                    output.write_all(CROSSSLOT_REPLY)?;

                    return Ok(true);
                }

                // The watched keys moved to another shard since WATCH: they count as changed.
                if session.watch_shard().is_some_and(|watches| watches != dst) {
                    unwatch_remote(forwarding, session, conn);
                    state.store.borrow_mut().unwatch(conn);
                    session.finish_exec();
                    session.clear_watches();
                    log.clear();
                    output.write_all(session.resp.null_aggregate())?;

                    return Ok(true);
                }

                if dst == forwarding.me {
                    log.clear();

                    return Ok(false);
                }

                // The owner shard runs the transaction; `gate` never sees this EXEC here.
                if let Some(refusal) = forward_refusal(auth, Some(rate), command) {
                    write_response(output, refusal)?;

                    return Ok(true);
                }

                let mut frames =
                    Vec::with_capacity(MULTI_FRAME.len() + log.frames.len() + frame.len());

                frames.extend_from_slice(MULTI_FRAME);
                frames.extend_from_slice(&log.frames);
                frames.extend_from_slice(frame);
                let count = log.count + 2;
                let reply = forwarder
                    .call(
                        forwarding, entry, conn, session, auth, dst, frames, count, false,
                    )
                    .await;
                let last = reply.lens.last().copied().unwrap_or(0) as usize;

                output.write_all(&reply.data[reply.data.len().saturating_sub(last)..])?;
                session.finish_exec();
                session.clear_watches();
                log.clear();

                return Ok(true);
            }

            Command::Discard => {
                unwatch_remote(forwarding, session, conn);
                log.clear();

                return Ok(false);
            }

            _ => return Ok(false),
        }
    }

    match command {
        Command::Watch(keys) => {
            let Some(first) = keys.first() else {
                return Ok(false);
            };
            let slot = hash_slot(first);
            let crossed = matches!(owner, Owner::Split)
                || session.watch_slot().is_some_and(|watched| {
                    forwarding.fabric.slot_owner(watched) != forwarding.fabric.slot_owner(slot)
                });

            if crossed {
                output.write_all(CROSSSLOT_REPLY)?;

                return Ok(true);
            }

            let Owner::Remote(dst) = owner else {
                return Ok(false);
            };
            let reply = forwarder
                .call(
                    forwarding,
                    entry,
                    conn,
                    session,
                    auth,
                    *dst,
                    frame.to_vec(),
                    1,
                    false,
                )
                .await;

            if reply.data == b"+OK\r\n" {
                session.accept_watch(slot, keys.len(), *dst);
                entry.remote_state.set(true);
            }

            output.write_all(&reply.data)?;

            Ok(true)
        }

        Command::Unwatch => {
            unwatch_remote(forwarding, session, conn);

            Ok(false)
        }

        Command::ClientTracking(on) => {
            if *on {
                entry.remote_state.set(true);
            } else {
                for dst in (0..shards).filter(|dst| *dst != forwarding.me) {
                    forwarding.fabric.send(
                        forwarding.me,
                        dst,
                        Message::Untrack {
                            src: forwarding.me,
                            conn,
                        },
                    );
                }
            }

            Ok(false)
        }

        Command::Scan {
            cursor,
            pattern,
            count,
            type_name,
            slot: None,
        } if state.single_port => {
            scan_all(
                state,
                forwarder,
                entry,
                conn,
                session,
                auth,
                output,
                *cursor,
                pattern.as_deref(),
                *count,
                type_name.as_deref(),
            )
            .await?;

            Ok(true)
        }

        _ => match owner {
            Owner::Local => Ok(false),
            Owner::Node(slot) => {
                let owner = forwarding.fabric.slot_owner(*slot);

                redirect_to_node(forwarding, output, *slot, owner)
            }
            Owner::Split => {
                output.write_all(SPLIT_REPLY)?;

                Ok(true)
            }

            Owner::Remote(dst) => {
                let reply = forwarder
                    .call_blocking(
                        forwarding,
                        entry,
                        conn,
                        session,
                        auth,
                        *dst,
                        frame.to_vec(),
                        peer,
                    )
                    .await?;

                output.write_all(&reply.data)?;

                Ok(true)
            }
        },
    }
}

fn scan_frame(
    local: u64,
    pattern: Option<&[u8]>,
    count: usize,
    type_name: Option<&[u8]>,
) -> Vec<u8> {
    let local = local.to_string();
    let count = count.to_string();
    let mut args: Vec<&[u8]> = vec![b"SCAN", local.as_bytes()];

    if let Some(pattern) = pattern {
        args.extend([b"MATCH".as_slice(), pattern]);
    }

    args.extend([b"COUNT".as_slice(), count.as_bytes()]);

    if let Some(type_name) = type_name {
        args.extend([b"TYPE".as_slice(), type_name]);
    }

    let mut frame = format!("*{}\r\n", args.len()).into_bytes();

    for arg in args {
        frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        frame.extend_from_slice(arg);
        frame.extend_from_slice(b"\r\n");
    }

    frame
}

/// Splits `*2\r\n$n\r\n<cursor>\r\n<keys array>` into the cursor and the keys array.
fn split_scan_reply(reply: &[u8]) -> Option<(u64, &[u8])> {
    let rest = reply.strip_prefix(b"*2\r\n$")?;
    let line = rest.windows(2).position(|pair| pair == b"\r\n")?;
    let length: usize = std::str::from_utf8(&rest[..line]).ok()?.parse().ok()?;
    let start = line + 2;
    let cursor = std::str::from_utf8(rest.get(start..start + length)?)
        .ok()?
        .parse()
        .ok()?;

    Some((cursor, rest.get(start + length + 2..)?))
}

/// `SCAN` over all shards: the high cursor bits name the shard being walked, the low bits are
/// that shard's own cursor. A shard that reports 0 hands over to the next one.
#[allow(clippy::too_many_arguments)]
async fn scan_all(
    state: &Rc<ShardState>,
    forwarder: &mut Forwarder,
    entry: &Rc<ConnFwd>,
    conn: u64,
    session: &TxSession,
    auth: &crate::acl::ConnectionAuth,
    output: &mut BoundedOutput,
    cursor: u64,
    pattern: Option<&[u8]>,
    count: usize,
    type_name: Option<&[u8]>,
) -> io::Result<()> {
    let Some(forwarding) = state.forwarding.as_ref() else {
        return Ok(());
    };
    let shards = forwarding.fabric.shard_count();
    let shard = (cursor >> SCAN_SHARD_SHIFT) as usize;
    let local = cursor & ((1 << SCAN_SHARD_SHIFT) - 1);

    if shard >= shards {
        return output.write_all(b"-ERR invalid cursor\r\n");
    }

    let reply = if shard == forwarding.me {
        let command = Command::Scan {
            cursor: local,
            pattern: pattern.map(std::borrow::Cow::Borrowed),
            count,
            type_name: type_name.map(std::borrow::Cow::Borrowed),
            slot: None,
        };
        let mut local_out = BoundedOutput::with_capacity(0, usize::MAX);

        {
            let mut store = state.store.borrow_mut();
            let mut waiters = state.waiters.borrow_mut();
            let mut sink = NoopMutationSink;

            match execute_in_database(
                &mut store,
                command,
                &mut sink,
                &mut *waiters,
                session.database,
            ) {
                Ok(response) => write_response_in(&mut local_out, response, session.resp)?,
                Err(_) => local_out.write_all(b"-ERR scan failed\r\n")?,
            }
        }

        local_out.to_vec()
    } else {
        forwarder
            .call(
                forwarding,
                entry,
                conn,
                session,
                auth,
                shard,
                scan_frame(local, pattern, count, type_name),
                1,
                false,
            )
            .await
            .data
    };

    let Some((next, keys)) = split_scan_reply(&reply) else {
        return output.write_all(&reply);
    };
    let next = match (next, shard + 1 < shards) {
        (0, true) => ((shard + 1) as u64) << SCAN_SHARD_SHIFT,
        (0, false) => 0,
        (next, _) => ((shard as u64) << SCAN_SHARD_SHIFT) | next,
    };
    let next = next.to_string();

    write!(output, "*2\r\n${}\r\n{next}\r\n", next.len())?;
    output.write_all(keys)
}

#[cfg(test)]
mod tests {
    #[test]
    fn keys_and_slots_are_routed_by_the_slot_table_not_the_formula() {
        let fabric = Fabric::new(2);
        let slot = crate::routing::hash_slot(b"user:1");
        let formula = crate::routing::shard_for_slot(slot, 2);

        assert_eq!(fabric.owner_of(b"user:1"), formula);
        fabric.slots.assign(slot, 1 - formula);
        assert_eq!(fabric.owner_of(b"user:1"), 1 - formula);
        assert_eq!(fabric.slot_owner(slot), 1 - formula);
        // The route each shard gets shares the same table.
        assert_eq!(fabric.slot_table().owner(slot), 1 - formula);
    }

    #[test]
    fn publish_budget_bounds_messages_and_bytes_per_shard_and_is_returned_on_delivery() {
        let fabric = Fabric::new(2);
        let small = PublishDelivery {
            channel: b"c".to_vec(),
            message: b"m".to_vec(),
        };

        for _ in 0..PUBLISH_QUEUE_MESSAGES {
            assert!(fabric.reserve_publish(1, small.cost()));
        }
        assert!(!fabric.reserve_publish(1, small.cost()));
        // The other shard has its own budget.
        assert!(fabric.reserve_publish(0, small.cost()));

        fabric.release_publish(1, &small);
        assert!(fabric.reserve_publish(1, small.cost()));

        for _ in 0..PUBLISH_QUEUE_MESSAGES {
            fabric.release_publish(1, &small);
        }

        // Bytes: one message larger than the whole budget never fits; up to the budget does.
        assert!(!fabric.reserve_publish(1, PUBLISH_QUEUE_BYTES + 1));
        assert!(fabric.reserve_publish(1, PUBLISH_QUEUE_BYTES));
        assert!(!fabric.reserve_publish(1, 1));
        assert_eq!(fabric.ports[1].publish_queued.load(Ordering::Acquire), 1);
    }

    use super::*;

    /// Keyless commands that run on every shard in single-port mode (`Publish` is delivered
    /// to every shard without waiting, in both modes). Each keyless command the parser
    /// builds is in this list or in `KEYLESS_ON_ONE_SHARD`, so a new one fails here until
    /// someone decides whether one shard's answer is the right one.
    const KEYLESS_FANNED: &[&str] = &[
        "DbSize",
        "FlushAll",
        "FlushDb",
        "JsonIndex",
        "Keys",
        "Publish",
        "RandomKey",
    ];
    /// Answered by the connection's shard: connection state, process-wide state every shard
    /// shares (ACL, config, scripts, checkpoints, cluster view), or routed another way
    /// (`SCAN` by its cursor, `SWAPDB` refused).
    const KEYLESS_ON_ONE_SHARD: &[&str] = &[
        "Acl",
        "Auth",
        "Bgsave",
        "Changes",
        "ChangesSubscribe",
        "ClientGetName",
        "ClientId",
        "ClientInfo",
        "ClientKill",
        "ClientList",
        "ClientSetName",
        "ClientTracking",
        "ClientDurability",
        "ClusterInfo",
        "ClusterKeySlot",
        "ClusterMyId",
        "ClusterNodes",
        "ClusterShards",
        "ClusterSlots",
        "CommandMeta",
        "ConfigGet",
        "ConfigRewrite",
        "ConfigSet",
        "ReplicaOf",
        "MoveSlots",
        "Topology",
        "Role",
        "Discard",
        "Echo",
        "Eval",
        "EvalSha",
        "Exec",
        "FCall",
        "FunctionDelete",
        "FunctionList",
        "FunctionLoad",
        "Hello",
        "Info",
        "LatencyLatest",
        "Multi",
        "PSubscribe",
        "PUnsubscribe",
        "Ping",
        "ReadOnly",
        "ReadWrite",
        "Ruvio",
        "Save",
        "Scan",
        "ScriptExists",
        "ScriptFlush",
        "ScriptKill",
        "ScriptLoad",
        "Select",
        "ShardPublish",
        "ShardSubscribe",
        "ShardUnsubscribe",
        "Slowlog",
        "StreamGroupRead",
        "Subscribe",
        "SwapDb",
        "Unsubscribe",
        "Unwatch",
        "Wait",
    ];

    #[test]
    fn every_keyless_command_is_decided_for_fan_out() {
        let mut undecided = Vec::new();
        let mut wrong = Vec::new();

        for (variant, command) in crate::protocol::consistency::keyless_forms() {
            let fanned = KEYLESS_FANNED.contains(&variant.as_str());

            if !fanned && !KEYLESS_ON_ONE_SHARD.contains(&variant.as_str()) {
                undecided.push(variant);
            } else if fan_kind(&command).is_some() != fanned {
                wrong.push(variant);
            }
        }

        assert!(
            undecided.is_empty(),
            "keyless commands with no fan-out decision: {undecided:?}"
        );
        assert!(
            wrong.is_empty(),
            "fan_kind disagrees with the lists: {wrong:?}"
        );
    }

    #[test]
    fn failed_wal_commit_turns_every_forwarded_answer_into_an_error() {
        let mut reply = Reply {
            conn: 1,
            token: 2,
            lens: vec![5, 4],
            data: b"+OK\r\n:1\r\n".to_vec(),
            unlogged: Vec::new(),
        };

        fail_reply(&mut reply);

        let error = b"-MISCONF persistence error\r\n";
        assert_eq!(reply.lens, [error.len() as u32; 2]);
        assert_eq!(reply.data, [&error[..], error].concat());
    }

    #[test]
    fn a_failed_wal_commit_keeps_the_answer_of_a_push_that_only_served_parked_clients() {
        let mut reply = Reply {
            conn: 1,
            token: 2,
            lens: vec![5, 4, 4],
            data: b"+OK\r\n:0\r\n:1\r\n".to_vec(),
            unlogged: vec![1],
        };

        fail_reply(&mut reply);

        let error = b"-MISCONF persistence error\r\n";
        assert_eq!(reply.lens, [error.len() as u32, 4, error.len() as u32]);
        assert_eq!(reply.data, [&error[..], b":0\r\n", error].concat());
    }
}
