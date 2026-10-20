//! Watching data nodes and acting on them: every coordinator pings every member; the Raft
//! leader decides failovers with a majority of coordinators and keeps every member pointed at
//! the agreed primary.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::broadcast;
use tracing::{info, warn};

use super::{
    config::{CoordinatorConfig, Group, Member},
    raft::{
        Commit, Leadership, NodeId, Primary, Raft, Raised, Recorded, Request, RpcRequest,
        RpcResponse, StateMachine, raise,
    },
    resp::{NodeClient, Value},
};

/// What one coordinator last saw of a member.
#[derive(Clone, Debug, Default)]
pub struct Sighting {
    /// When it last answered a ping.
    pub last_ok: Option<Instant>,
    /// What `ROLE` said then: `master`, or `slave` and the replication address it follows.
    pub role: Option<Role>,
    /// Its `wal_durable_sequence` summed over shards: how much of the history it holds.
    pub durable: u64,
    /// Each shard's `wal_durable_sequence`.
    pub shard_durable: Vec<u64>,
    /// Each shard's `replication_committed_sequence`: the highest record its primary released
    /// a `CLIENT DURABILITY` reply for, as a replica was last told or a primary holds it.
    pub shard_committed: Vec<u64>,
    /// It answered `LOADING`: still copying its primary, never a candidate.
    pub loading: bool,
    /// The cluster map version it serves (`RUVIO TOPOLOGY VERSION`), read only when the
    /// coordinators manage a multi-node cluster.
    pub topology: Option<u64>,
    /// How many shards it runs, and how many of them answered `ROLE` and `INFO replication`.
    pub shards: u16,
    pub shards_answering: u16,
    /// The replicas it lists in `INFO replication` (`slaveN:ip=…,port=…`): where clients reach
    /// each one.
    pub replicas: Vec<super::config::Endpoint>,
    /// Its process identity (`run_id` in `INFO server`), when it reports one.
    pub run_id: Option<String>,
}

impl Sighting {
    /// Every shard answered: a look at a member with a shard down does not count as an answer.
    pub fn complete(&self) -> bool {
        self.shards_answering == self.shards
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    /// Following the primary whose replication address (`host:port`) is this.
    Replica(Option<String>),
}

/// Every member's latest sighting, by group and member address, this coordinator's lease
/// grants, and what it did as the leader.
#[derive(Clone, Default)]
pub struct Sightings(
    Arc<Mutex<HashMap<(String, String), Sighting>>>,
    Grants,
    Arc<Stats>,
);

impl Sightings {
    pub fn grants(&self) -> &Grants {
        &self.1
    }

    pub fn stats(&self) -> &Stats {
        &self.2
    }

    /// How many members of `group` answered on every shard within `fresh`.
    pub fn answering(&self, group: &Group, fresh: Duration) -> usize {
        self.seen(group)
            .iter()
            .filter(|sighting| {
                sighting.complete() && sighting.last_ok.is_some_and(|at| at.elapsed() <= fresh)
            })
            .count()
    }

    /// The sighting of `member` (its client address) of `group`.
    pub fn get(&self, group: &str, member: &str) -> Sighting {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(group.to_owned(), member.to_owned()))
            .cloned()
            .unwrap_or_default()
    }

    /// Every member's sighting, in the order `group` lists them.
    pub fn seen(&self, group: &Group) -> Vec<Sighting> {
        group
            .members
            .iter()
            .map(|member| self.get(&group.name, &member.address.to_string()))
            .collect()
    }

    /// Takes in a look at a member. One where a shard did not answer keeps the last complete
    /// sighting and only updates how many shards answered: the member is not heard from, so it
    /// is down once `down_after` passes without a complete answer, and is no candidate meanwhile.
    fn set(&self, group: &str, member: &str, sighting: Sighting) {
        let mut sightings = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let key = (group.to_owned(), member.to_owned());

        if sighting.complete() {
            let mut sighting = sighting;

            // One failed `INFO server` does not forget who the member is.
            if sighting.run_id.is_none() {
                sighting.run_id = sightings.get(&key).and_then(|known| known.run_id.clone());
            }
            sightings.insert(key, sighting);
        } else {
            let known = sightings.entry(key).or_default();

            known.shards = sighting.shards;
            known.shards_answering = sighting.shards_answering;
        }
    }

    /// Milliseconds since `member` of `group` last answered, as [`RpcRequest::Observe`]
    /// reports it.
    pub fn age(&self, group: &str, member: &str) -> Option<u64> {
        self.get(group, member)
            .last_ok
            .map(|at| at.elapsed().as_millis() as u64)
    }
}

/// What this coordinator granted for the primary lease (`primary_lease_ms`), and the promises
/// it made to the leader about it.
///
/// When the leader asks whether a primary's lease ended ([`RpcRequest::Leases`]) and this
/// coordinator answers yes, it promises not to grant that member again until the leader
/// confirms, after any failover it was deciding, that the record still names it
/// ([`confirm_locally`]). The leader decides each failover under the group's
/// [`Grants::decision`] lock, held until the failover is recorded, and a confirmation for the
/// group waits for it and reads the record after a majority confirmed the leader. So a
/// majority that reported the lease ended cannot renew the old primary once the failover is
/// recorded, and any majority that granted it shares a coordinator with that one. Groups do
/// not wait for each other.
#[derive(Clone)]
pub struct Grants(Arc<GrantBook>);

struct GrantBook {
    /// A coordinator that just started may have granted before it stopped: it reports every
    /// lease as granted at its start.
    started: Instant,
    members: Mutex<HashMap<(String, String), Granting>>,
    /// One per group, held by the leader while it decides whether to fail that group over.
    decisions: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// One member's grants from this coordinator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Granting {
    /// Taken just before the last `RUVIO LEASE GRANT` was sent, so after the node's BEGIN
    /// instant the lease counts from.
    pub last: Option<Instant>,
    /// The epoch the leader confirmed for this member since this coordinator last reported
    /// its lease ended; it grants only at that epoch. `None` at start.
    pub cleared: Option<u64>,
    /// How many times this coordinator reported the lease ended: a confirmation asked
    /// before the latest report does not count.
    pub reports: u64,
}

impl Default for Grants {
    fn default() -> Self {
        Self(Arc::new(GrantBook {
            started: Instant::now(),
            members: Mutex::default(),
            decisions: Mutex::default(),
        }))
    }
}

/// How long ago a coordinator that started at `started` may last have granted a lease.
pub fn grant_age(last: Option<Instant>, started: Instant, now: Instant) -> Duration {
    now.saturating_duration_since(last.map_or(started, |last| last.max(started)))
}

impl Granting {
    /// Answers the leader: whether the lease ended, its grant age past `safe`. A yes is a
    /// promise: no grant until the leader confirms the member again.
    pub fn report(&mut self, started: Instant, now: Instant, safe: Duration) -> bool {
        let ended = grant_age(self.last, started, now) > safe;

        if ended {
            self.cleared = None;
            self.reports += 1;
        }
        ended
    }

    /// The leader confirmed the record names this member at `epoch`, in answer to a request
    /// sent when [`Granting::reports`] was `asked_at`.
    pub fn confirm(&mut self, asked_at: u64, epoch: u64) {
        if self.reports == asked_at {
            self.cleared = Some(epoch);
        }
    }

    /// Records a grant at `epoch` about to be sent, when the member is cleared for it.
    pub fn record(&mut self, epoch: u64, now: Instant) -> bool {
        let cleared = self.cleared == Some(epoch);

        if cleared {
            self.last = Some(now);
        }
        cleared
    }
}

impl Grants {
    fn with<R>(&self, group: &str, member: &str, act: impl FnOnce(&mut Granting) -> R) -> R {
        let mut members = self
            .0
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        act(members
            .entry((group.to_owned(), member.to_owned()))
            .or_default())
    }

    /// [`Granting::report`] for `member` (its client address) of `group`.
    pub fn report(&self, group: &str, member: &str, safe: Duration) -> bool {
        let started = self.0.started;

        self.with(group, member, |granting| {
            granting.report(started, Instant::now(), safe)
        })
    }

    /// Held by the leader while it decides whether to fail `group` over, until the failover
    /// is recorded; [`confirm_locally`] for that group waits for it.
    pub async fn decision(&self, group: &str) -> DecisionGuard {
        let lock = {
            let mut decisions = self
                .0
                .decisions
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            Arc::clone(decisions.entry(group.to_owned()).or_default())
        };

        lock.lock_owned().await
    }
}

impl Grants {
    /// When this coordinator last granted `member` of `group` a lease, if it did since it started.
    pub fn last(&self, group: &str, member: &str) -> Option<Instant> {
        self.with(group, member, |granting| granting.last)
    }
}

/// Where a group stands, as the leader last tended it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupState {
    /// The primary answers, or a majority of coordinators still hear it.
    Ok,
    /// A majority lost the primary, but its lease has not ended for a majority yet.
    LeaseRunning,
    /// The primary is down and no replica holds every committed record.
    Waiting,
    /// The primary is down and no replica can take over.
    NoCandidate,
}

impl GroupState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::LeaseRunning => "lease_running",
            Self::Waiting => "waiting",
            Self::NoCandidate => "no_candidate",
        }
    }
}

/// What this coordinator did as the leader, for `INFO`.
#[derive(Default)]
pub struct Stats {
    failovers: std::sync::atomic::AtomicU64,
    last_failover_unix_ms: std::sync::atomic::AtomicU64,
    /// Per group: its state, since when, and when a warning about it was last logged.
    groups: Mutex<HashMap<String, (GroupState, Instant, Instant)>>,
}

/// How often at most a group that stays without a writer is warned about.
const WARN_EVERY: Duration = Duration::from_secs(10);

impl Stats {
    /// Records that `group` is in `state`; whether a warning about it is due: when it enters
    /// the state, then every [`WARN_EVERY`] while it stays.
    pub fn enter(&self, group: &str, state: GroupState) -> bool {
        let now = Instant::now();
        let mut groups = self
            .groups
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        match groups.get_mut(group) {
            Some((known, _, warned)) if *known == state => {
                let due = now.duration_since(*warned) >= WARN_EVERY;

                if due {
                    *warned = now;
                }
                due
            }
            _ => {
                groups.insert(group.to_owned(), (state, now, now));
                true
            }
        }
    }

    /// `group`'s state and how long it has been in it; `None` until this coordinator led.
    pub fn state(&self, group: &str) -> Option<(GroupState, Duration)> {
        self.groups
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(group)
            .map(|&(state, since, _)| (state, since.elapsed()))
    }

    fn failed_over(&self) {
        use std::sync::atomic::Ordering;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as u64);

        self.failovers.fetch_add(1, Ordering::Relaxed);
        self.last_failover_unix_ms.store(now, Ordering::Relaxed);
    }

    /// Failovers this coordinator did as the leader, and when the last one was (Unix ms, 0
    /// for none).
    pub fn failovers(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;

        (
            self.failovers.load(Ordering::Relaxed),
            self.last_failover_unix_ms.load(Ordering::Relaxed),
        )
    }
}

/// A group's [`Grants::decision`] lock.
pub type DecisionGuard = tokio::sync::OwnedMutexGuard<()>;

/// How long [`confirm_locally`] may take, waiting for a decision included: less than a caller
/// waits ([`CALL_TIMEOUT`]), so the answer can still reach it, and nothing keeps running for a
/// caller that gave up.
const CONFIRM_BUDGET: Duration = Duration::from_millis(750);

/// [`RpcRequest::Confirm`] on the leader: after any failover decision in progress for the
/// group, and once a majority confirmed this coordinator still leads (so its record is
/// current), the group's primary. `None` when it cannot confirm within [`CONFIRM_BUDGET`].
pub async fn confirm_locally(
    raft: &Raft,
    machine: &StateMachine,
    grants: &Grants,
    group: &str,
) -> Option<Primary> {
    // Nothing recorded, nothing to confirm; also keeps names no group has out of the locks.
    if !machine.primaries().contains_key(group) {
        return None;
    }
    let confirmed = async {
        let _decided = grants.decision(group).await;

        raft.ensure_linearizable().await.ok()?;
        machine.primaries().get(group).cloned()
    };

    tokio::time::timeout(CONFIRM_BUDGET, confirmed)
        .await
        .ok()
        .flatten()
}

/// The coordinators that vote, with their addresses: the Raft membership once there is one,
/// `peers` from the file before that (a coordinator that has not started or joined yet).
pub fn coordinators(config: &CoordinatorConfig, raft: &Raft) -> Vec<(NodeId, String)> {
    let metrics = raft.metrics();
    let metrics = metrics.borrow();
    let membership = metrics.membership_config.membership();
    let voters: Vec<(NodeId, String)> = membership
        .voter_ids()
        .filter_map(|id| membership.get_node(&id).map(|node| (id, node.addr.clone())))
        .collect();

    if voters.is_empty() {
        config
            .peers
            .iter()
            .map(|(id, address)| (*id, address.to_string()))
            .collect()
    } else {
        voters
    }
}

/// Where coordinator `id` listens: from the Raft membership (voters and learners), else from
/// `peers`.
pub fn address_of(config: &CoordinatorConfig, raft: &Raft, id: NodeId) -> Option<String> {
    let known = raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .get_node(&id)
        .map(|node| node.addr.clone());

    known.or_else(|| {
        config
            .peers
            .iter()
            .find(|(peer, _)| *peer == id)
            .map(|(_, address)| address.to_string())
    })
}

/// How many of `voters` coordinators make a majority.
pub fn quorum(voters: usize) -> usize {
    voters / 2 + 1
}

/// The group's primary as the current leader confirms it ([`confirm_locally`]).
async fn confirm(
    config: &CoordinatorConfig,
    raft: &Raft,
    machine: &StateMachine,
    grants: &Grants,
    group: &str,
) -> Option<Primary> {
    let leader = raft.metrics().borrow().current_leader?;

    if leader == config.id {
        return confirm_locally(raft, machine, grants, group).await;
    }
    let address = address_of(config, raft, leader)?;

    match super::raft::call(
        &address,
        config.secret.as_deref().unwrap_or(""),
        RpcRequest::Confirm {
            group: group.to_owned(),
        },
        CALL_TIMEOUT,
    )
    .await
    {
        Ok(RpcResponse::Confirmed(primary)) => primary,
        _ => None,
    }
}

/// Renews the lease of the primary this coordinator's record names in `group`: confirms it
/// with the leader first when needed, then `RUVIO LEASE BEGIN` and `RUVIO LEASE GRANT` on
/// one connection. The grant is recorded before it is sent: once sent it may arrive.
async fn renew(
    config: &CoordinatorConfig,
    raft: &Raft,
    machine: &StateMachine,
    grants: &Grants,
    group: &Group,
    primary: Primary,
) {
    let Some((lease, _)) = config.primary_lease else {
        return;
    };
    let name = group.name.as_str();
    let member = primary.member.as_str();
    let asked_at = grants.with(name, member, |granting| {
        (granting.cleared != Some(primary.epoch)).then_some(granting.reports)
    });

    if let Some(asked_at) = asked_at {
        if confirm(config, raft, machine, grants, name).await.as_ref() != Some(&primary) {
            return;
        }
        grants.with(name, member, |granting| {
            granting.confirm(asked_at, primary.epoch)
        });
    }
    let Ok(target) = super::config::Endpoint::parse(member) else {
        return;
    };
    let Ok(mut client) = NodeClient::connect(&target, config.auth.as_deref(), CALL_TIMEOUT).await
    else {
        return;
    };
    let Ok(Value::Integer(token)) = client
        .call(&[b"RUVIO", b"LEASE", b"BEGIN"], CALL_TIMEOUT)
        .await
    else {
        return;
    };
    let words = [
        token.to_string(),
        config.id.to_string(),
        coordinators(config, raft).len().to_string(),
        primary.epoch.to_string(),
        lease.as_millis().to_string(),
    ];

    if !grants.with(name, member, |granting| {
        granting.record(primary.epoch, Instant::now())
    }) {
        return;
    }
    if let Ok(Value::Error(error)) = client
        .call(
            &[
                b"RUVIO",
                b"LEASE",
                b"GRANT",
                words[0].as_bytes(),
                words[1].as_bytes(),
                words[2].as_bytes(),
                words[3].as_bytes(),
                words[4].as_bytes(),
            ],
            CALL_TIMEOUT,
        )
        .await
    {
        tracing::debug!(group = name, member, %error, "the primary refused a lease grant");
    }
}

/// With `primary_lease_ms`, renews every group's primary lease every `ping_interval`, each
/// group on its own task: a slow or silent member elsewhere never delays a renewal, and a
/// renewal still running is not started twice. Each coordinator grants on its own: the lease
/// holds only while a majority of them do.
pub async fn renew_leases(
    config: Arc<CoordinatorConfig>,
    raft: Raft,
    machine: StateMachine,
    sightings: Sightings,
) {
    if config.primary_lease.is_none() {
        return;
    }
    let grants = sightings.grants().clone();
    let running: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut tick = tokio::time::interval(config.ping_interval);

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let primaries = machine.primaries();

        for group in &config.groups {
            let Some(primary) = primaries.get(&group.name).cloned() else {
                continue;
            };
            // A primary with a shard this coordinator cannot reach is not renewed: it would
            // keep writing on the others while the coordinators fail it over.
            if !sightings.get(&group.name, &primary.member).complete() {
                continue;
            }
            {
                let mut running = running.lock().unwrap_or_else(|error| error.into_inner());

                if running.contains(&group.name) {
                    continue;
                }
                running.push(group.name.clone());
            }
            let (config, raft, machine, grants, running, group) = (
                Arc::clone(&config),
                raft.clone(),
                machine.clone(),
                grants.clone(),
                Arc::clone(&running),
                group.clone(),
            );

            tokio::spawn(async move {
                renew(&config, &raft, &machine, &grants, &group, primary).await;
                running
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .retain(|name| *name != group.name);
            });
        }
    }
}

/// A failover announcement for `+switch-master` subscribers:
/// `group old-ip old-port new-ip new-port`.
pub type Switches = broadcast::Sender<String>;

const CALL_TIMEOUT: Duration = Duration::from_millis(1000);

/// Pings one member and reads its role and synced sequence.
async fn look(member: &Member, auth: Option<&str>, cluster: bool) -> Option<Sighting> {
    let mut client = NodeClient::connect(&member.address, auth, CALL_TIMEOUT)
        .await
        .ok()?;
    let role = client.call(&[b"ROLE"], CALL_TIMEOUT).await.ok()?;
    let mut sighting = Sighting {
        last_ok: Some(Instant::now()),
        ..Sighting::default()
    };

    match role {
        Value::Error(error) if error.starts_with("LOADING") || error.starts_with("MASTERDOWN") => {
            sighting.loading = true;
            sighting.role = Some(Role::Replica(None));
            return Some(sighting);
        }
        Value::Array(Some(items)) => {
            sighting.role = match items.first().and_then(Value::text).as_deref() {
                Some("master") => Some(Role::Master),
                Some("slave") => Some(Role::Replica(
                    match (items.get(1).and_then(Value::text), items.get(2)) {
                        (Some(host), Some(Value::Integer(port))) => Some(
                            super::config::Endpoint {
                                host,
                                port: u16::try_from(*port).unwrap_or(0),
                            }
                            .to_string(),
                        ),
                        _ => None,
                    },
                )),
                _ => None,
            };
        }
        _ => return None,
    }

    if cluster {
        sighting.topology = match client
            .call(&[b"RUVIO", b"TOPOLOGY", b"VERSION"], CALL_TIMEOUT)
            .await
            .ok()?
        {
            Value::Integer(version) => u64::try_from(version).ok(),
            // Refused (an ACL without `ruvio|topology`, a release without the command): pushed
            // anyway, so the refusal shows in the coordinator's log.
            _ => Some(0),
        };
    }

    sighting.run_id = client
        .call(&[b"INFO", b"server"], CALL_TIMEOUT)
        .await
        .ok()
        .and_then(|info| info.text())
        .and_then(|text| run_id_in(&text));

    // Every shard's synced sequence: shard i answers on the address's port plus i.
    let shards = match client
        .call(&[b"CONFIG", b"GET", b"shards"], CALL_TIMEOUT)
        .await
        .ok()?
    {
        Value::Array(Some(pair)) => pair
            .get(1)
            .and_then(Value::text)
            .and_then(|count| count.parse::<u16>().ok())
            .unwrap_or(1),
        _ => 1,
    };

    sighting.shards = shards;
    for shard in 0..shards {
        // Shard 0 answered `ROLE` above; every other shard must answer it too, so a member
        // with one shard down is not heard from (see `Sightings::set`).
        let info = if shard == 0 {
            client
                .call(&[b"INFO", b"replication"], CALL_TIMEOUT)
                .await
                .ok()
        } else {
            other_shard(&member.address.plus(shard), auth, &mut sighting).await
        };
        sighting.shards_answering += u16::from(info.is_some());
        let text = info.and_then(|info| info.text()).unwrap_or_default();

        if shard == 0 {
            sighting.replicas = listed_replicas(&text);
        }
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(0)
        };
        let durable = field("wal_durable_sequence:");

        sighting.durable += durable;
        sighting.shard_durable.push(durable);
        sighting
            .shard_committed
            .push(field("replication_committed_sequence:"));
    }

    Some(sighting)
}

/// The replicas an `INFO replication` text lists, by the address clients reach them at.
fn listed_replicas(text: &str) -> Vec<super::config::Endpoint> {
    text.lines()
        .filter(|line| line.starts_with("slave"))
        .filter_map(|line| {
            let (_, fields) = line.split_once(':')?;
            let field = |name: &str| {
                fields
                    .split(',')
                    .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
            };

            Some(super::config::Endpoint {
                host: field("ip")?.to_owned(),
                port: field("port")?.parse().ok()?,
            })
        })
        .collect()
}

/// Shard `address` of a member: its `INFO replication` when it answers `ROLE` and that, or
/// `None`. A shard still loading marks the member loading.
async fn other_shard(
    address: &super::config::Endpoint,
    auth: Option<&str>,
    sighting: &mut Sighting,
) -> Option<Value> {
    let mut client = NodeClient::connect(address, auth, CALL_TIMEOUT)
        .await
        .ok()?;

    match client.call(&[b"ROLE"], CALL_TIMEOUT).await.ok()? {
        Value::Error(error) if error.starts_with("LOADING") || error.starts_with("MASTERDOWN") => {
            sighting.loading = true;
        }
        Value::Array(Some(_)) => {}
        _ => return None,
    }
    client
        .call(&[b"INFO", b"replication"], CALL_TIMEOUT)
        .await
        .ok()
}

/// How long a member may take to answer a command: `down_after`, at least 1 s. `REPLICAOF`
/// answers before any copy starts, so a member slower than that is stuck, and waiting longer
/// only delays the group.
fn command_timeout(config: &CoordinatorConfig) -> Duration {
    config.down_after.max(CALL_TIMEOUT)
}

/// Sends one command to a member; `Ok` when it answers `OK` within `timeout`.
async fn command(
    member: &Member,
    auth: Option<&str>,
    args: &[&[u8]],
    timeout: Duration,
) -> Result<(), String> {
    let mut client = NodeClient::connect(&member.address, auth, CALL_TIMEOUT)
        .await
        .map_err(|error| error.to_string())?;

    match client
        .call(args, timeout)
        .await
        .map_err(|error| error.to_string())?
    {
        Value::Simple(_) => Ok(()),
        Value::Error(error) => Err(error),
        other => Err(format!("unexpected reply {other:?}")),
    }
}

/// Every group's committed sequences as this coordinator learned them, per shard: a failover
/// keeps these records. Learned from the members on every tick and taken in from the Raft
/// record, where the leader writes them when they rise, so a new or restarted leader starts
/// from what the previous one knew.
#[derive(Clone, Default)]
pub struct Commits(Arc<Mutex<HashMap<String, Commit>>>);

fn at(values: &[u64], shard: usize) -> u64 {
    values.get(shard).copied().unwrap_or(0)
}

impl Commit {
    /// Takes in what the primary and the replicas following it report now.
    fn learn(&mut self, group: &Group, primary: &Primary, seen: &[Sighting], fresh: Duration) {
        // Only under the primary the values belong to: after a failover the Raft record
        // brings the values the promotion kept (see `Commit::after_promotion`).
        if self.epoch != primary.epoch {
            return;
        }
        let Some(index) = primary.index(group) else {
            return;
        };
        let head = &seen[index];
        // A primary that answers bounds what can be committed: a replica's higher value is
        // left from an earlier primary.
        let alive = head.last_ok.is_some_and(|at| at.elapsed() <= fresh);
        let following = Some(Role::Replica(Some(
            group.members[index].replication.to_string(),
        )));

        for (member, sighting) in seen.iter().enumerate() {
            let own = member == index && sighting.role == Some(Role::Master);

            if !own && sighting.role != following {
                continue;
            }
            for (shard, &reported) in sighting.shard_committed.iter().enumerate() {
                let value = if own {
                    reported.min(at(&sighting.shard_durable, shard))
                } else if reported <= at(&self.stale, shard) {
                    continue;
                } else if alive {
                    reported.min(at(&head.shard_durable, shard))
                } else {
                    reported
                };

                if value > 0 {
                    raise(&mut self.committed, shard, value);
                }
            }
        }
    }
}

impl Commits {
    /// Updates what is known committed in `group` from the Raft record and this tick's
    /// sightings, and returns it.
    pub fn learn(
        &self,
        group: &Group,
        primary: &Primary,
        seen: &[Sighting],
        fresh: Duration,
        recorded: &Recorded,
    ) -> Vec<u64> {
        let mut commits = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let commit = commits.entry(group.name.clone()).or_default();

        match recorded.get(&group.name) {
            Some(record) => commit.merge(record),
            // Nothing recorded for the group (state written before commits were recorded):
            // the values carry over to the current primary, as a promotion without a decision.
            None if commit.epoch < primary.epoch => commit.epoch = primary.epoch,
            None => {}
        }
        commit.learn(group, primary, seen, fresh);
        commit.committed.clone()
    }

    /// Per group, the shards whose committed sequence rose above the Raft record: what the
    /// leader records next. Groups that never use `CLIENT DURABILITY` have none.
    pub fn raised(&self, recorded: &Recorded) -> BTreeMap<String, Raised> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(|(group, commit)| {
                let committed = match recorded.get(group) {
                    Some(record) => commit.raised_over(record),
                    None => commit.raised_over(&Commit {
                        epoch: commit.epoch,
                        ..Commit::default()
                    }),
                };

                (!committed.is_empty()).then(|| {
                    let raised = Raised {
                        epoch: commit.epoch,
                        committed,
                    };

                    (group.clone(), raised)
                })
            })
            .collect()
    }

    /// What holds in `group` once a member holding `durable` per shard is primary at `epoch`.
    pub fn after_promotion(&self, group: &str, durable: &[u64], epoch: u64) -> Commit {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(group)
            .cloned()
            .unwrap_or_default()
            .after_promotion(durable, epoch)
    }

    /// What is known committed in `group`, per shard; empty when nothing is.
    pub fn known(&self, group: &str) -> Vec<u64> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(group)
            .map(|commit| commit.committed.clone())
            .unwrap_or_default()
    }
}

/// Whether a member holds every committed record: in each shard, at least the committed
/// sequence. Nothing committed (no `CLIENT DURABILITY` in use) asks for nothing.
fn holds(sighting: &Sighting, committed: &[u64]) -> bool {
    committed
        .iter()
        .enumerate()
        .all(|(shard, &sequence)| at(&sighting.shard_durable, shard) >= sequence)
}

/// The member to promote: answering on every shard, a replica with a complete copy that holds
/// every committed record, holding the most of the history; ties go to the lowest index.
pub fn choose_candidate(
    group: &Group,
    primary: usize,
    sightings: &[Sighting],
    fresh: Duration,
    committed: &[u64],
) -> Option<usize> {
    (0..group.members.len())
        .filter(|&member| member != primary)
        .filter(|&member| {
            let sighting = &sightings[member];

            !sighting.loading
                && sighting.complete()
                && sighting.last_ok.is_some_and(|at| at.elapsed() <= fresh)
                && matches!(sighting.role, Some(Role::Replica(_)))
                && holds(sighting, committed)
        })
        .max_by(|&left, &right| {
            sightings[left]
                .durable
                .cmp(&sightings[right].durable)
                .then(right.cmp(&left))
        })
}

/// What a failover does.
#[derive(Debug, PartialEq, Eq)]
pub enum Choice {
    /// Promote this replica: it holds every committed record.
    Promote(usize),
    /// Promote this replica although it lacks committed records (`unclean_failover`, or
    /// `COORDINATOR FAILOVER group FORCE`).
    Unclean(usize),
    /// Replicas answer, but none holds every committed record: wait for one.
    Wait,
    /// No replica can take over.
    Nobody,
}

pub fn choose(
    group: &Group,
    primary: usize,
    sightings: &[Sighting],
    fresh: Duration,
    committed: &[u64],
    unclean: bool,
) -> Choice {
    if let Some(member) = choose_candidate(group, primary, sightings, fresh, committed) {
        return Choice::Promote(member);
    }
    match choose_candidate(group, primary, sightings, fresh, &[]) {
        Some(member) if unclean => Choice::Unclean(member),
        Some(_) => Choice::Wait,
        None => Choice::Nobody,
    }
}

/// The committed records `sighting` lacks, per shard, for the log.
fn missing(sighting: &Sighting, committed: &[u64]) -> String {
    committed
        .iter()
        .enumerate()
        .filter(|&(shard, &sequence)| at(&sighting.shard_durable, shard) < sequence)
        .map(|(shard, &sequence)| {
            format!(
                "shard {shard}: holds {} of {sequence}",
                at(&sighting.shard_durable, shard)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Pings every member of every group, every `ping_interval`.
pub async fn watch(config: Arc<CoordinatorConfig>, machine: StateMachine, sightings: Sightings) {
    let mut tick = tokio::time::interval(config.ping_interval);

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let mut looks = Vec::new();
        let cluster = config.groups.iter().any(|group| group.shards.is_some());

        for group in &config.groups {
            for member in machine.group(group).members {
                let auth = config.auth.clone();
                let name = group.name.clone();

                looks.push(tokio::spawn(async move {
                    let sighting = look(&member, auth.as_deref(), cluster).await;

                    (name, member.address.to_string(), sighting)
                }));
            }
        }
        for look in looks {
            if let Ok((name, address, Some(sighting))) = look.await {
                sightings.set(&name, &address, sighting);
            }
        }
    }
}

/// What the coordinators say about a primary the leader no longer hears from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Fewer than a majority lost it.
    Up,
    /// A majority lost it, but fewer than a majority saw its lease end.
    LeaseRunning,
    /// Down, and its lease (if any coordinator grants one) ended.
    Down,
}

/// Counts the coordinators' answers: how many no longer hear the primary, how many report
/// its lease ended, and whether any of them grants leases.
pub fn verdict(heard_down: usize, lease_ended: usize, lease_on: bool, quorum: usize) -> Verdict {
    if heard_down < quorum {
        Verdict::Up
    } else if lease_on && lease_ended < quorum {
        Verdict::LeaseRunning
    } else {
        Verdict::Down
    }
}

/// Asks every coordinator (this one included, the others at once) how long ago it heard from
/// `primary` and whether that member's lease ended, each by its own lease settings. A
/// coordinator that reports the lease ended promises not to renew it until the leader
/// confirms the member again (see [`Grants`]). One that does not answer the lease question (it
/// is down, or of an older version) counts as still granting; its `Observe` answer still
/// counts for hearing. Also whether any coordinator grants leases.
async fn objectively_down(
    config: &CoordinatorConfig,
    raft: &Raft,
    sightings: &Sightings,
    group: &Group,
    primary: &str,
) -> (Verdict, bool) {
    let down_after = config.down_after.as_millis() as u64;
    let is_down = |age: Option<u64>| age.is_none_or(|age| age > down_after);
    let mut heard_down = usize::from(is_down(sightings.age(&group.name, primary)));
    let mut lease_on = config.primary_lease.is_some();
    let mut ended = usize::from(match config.primary_lease {
        Some((_, safe)) => sightings.grants().report(&group.name, primary, safe),
        None => true,
    });
    let mut calls = tokio::task::JoinSet::new();
    let voters = coordinators(config, raft);

    for (id, address) in &voters {
        if *id == config.id {
            continue;
        }
        let (address, secret, name, member) = (
            address.clone(),
            config.secret.clone().unwrap_or_default(),
            group.name.clone(),
            primary.to_owned(),
        );

        calls.spawn(async move {
            let leases = RpcRequest::Leases {
                group: name.clone(),
                member: member.clone(),
            };

            match super::raft::call(&address, &secret, leases, CALL_TIMEOUT).await {
                Ok(RpcResponse::Leases {
                    heard,
                    ended,
                    lease_on,
                }) => (Some(heard), ended, lease_on),
                _ => match super::raft::call(
                    &address,
                    &secret,
                    RpcRequest::Observe {
                        group: name,
                        member,
                    },
                    CALL_TIMEOUT,
                )
                .await
                {
                    Ok(RpcResponse::Observe(heard)) => (Some(heard), false, false),
                    _ => (None, false, false),
                },
            }
        });
    }
    while let Some(answer) = calls.join_next().await {
        let Ok((heard, peer_ended, peer_lease_on)) = answer else {
            continue;
        };

        heard_down += usize::from(heard.is_some_and(is_down));
        ended += usize::from(peer_ended);
        lease_on |= peer_lease_on;
    }

    (
        verdict(heard_down, ended, lease_on, quorum(voters.len())),
        lease_on,
    )
}

/// Asked before each command to a data node: whether this coordinator may still act on them.
pub trait Fence: Sync {
    fn holds(&self) -> impl Future<Output = bool> + Send;
}

/// The leader's [`Fence`]: a majority of coordinators confirmed it within the last window
/// ([`Leadership::confirm`], cached for that window), and it applied its own term. A leader
/// that lost its majority in the middle of a tick stops before its next command, so it never
/// points members at a primary a newer leader replaced.
pub struct Leading<'a> {
    pub leadership: &'a Leadership,
    pub raft: &'a Raft,
}

impl Fence for Leading<'_> {
    async fn holds(&self) -> bool {
        self.leadership.confirm(self.raft).await && caught_up(self.raft)
    }
}

/// What the leader's loop shares with the task tending each group.
#[derive(Clone)]
struct Leader {
    config: Arc<CoordinatorConfig>,
    raft: Raft,
    machine: StateMachine,
    sightings: Sightings,
    commits: Commits,
    switches: Switches,
    leadership: Arc<Leadership>,
    discovery: Arc<Discovery>,
}

/// Replica discovery's own bookkeeping: the groups a discovery runs for, and replicas whose
/// replication address could not be read, with when, so they are tried again only later.
#[derive(Default)]
struct Discovery {
    running: Mutex<Vec<String>>,
    failed: Mutex<HashMap<String, Instant>>,
}

/// How long a replica whose replication address could not be read is left alone.
const DISCOVERY_RETRY: Duration = Duration::from_secs(30);

/// The leader's loop: start each group from its configured primary, fail over a primary a
/// majority sees down, and point every answering member at the agreed primary. Each group is
/// tended on its own task, so a member that accepts connections but does not answer delays
/// only its own group; a group still being tended from an earlier tick is skipped.
pub async fn lead(
    config: Arc<CoordinatorConfig>,
    raft: Raft,
    machine: StateMachine,
    sightings: Sightings,
    commits: Commits,
    switches: Switches,
    moves: super::migrate::Context,
) {
    let leader = Leader {
        config: Arc::clone(&config),
        raft: raft.clone(),
        machine: machine.clone(),
        sightings: sightings.clone(),
        commits: commits.clone(),
        switches,
        leadership: Arc::clone(&moves.leadership),
        discovery: Arc::default(),
    };
    let tending: Arc<Mutex<Vec<usize>>> = Arc::default();
    let mut tick = tokio::time::interval(config.ping_interval);
    let mut recorded_at: Option<Instant> = None;

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        // A leader cut off from the other coordinators keeps `current_leader()` (openraft
        // does not step it down), so a majority confirms it before anything is done on the
        // data nodes. A new leader also acts only once it has applied what earlier leaders
        // recorded: until then its primaries and committed sequences may be those of an old
        // snapshot. Until both hold it only learns, like a follower. Both are asked again
        // before each command to a data node (see [`Leading`]).
        if !moves.leadership.confirm(&raft).await || !caught_up(&raft) {
            // Every coordinator learns what was committed, so a new leader also knows what
            // rose after the last record.
            let primaries = machine.primaries();
            let recorded = machine.commits();

            for group in &config.groups {
                if let Some(primary) = primaries.get(&group.name) {
                    let group = &machine.group(group);
                    let seen = sightings.seen(group);

                    commits.learn(group, primary, &seen, config.down_after, &recorded);
                }
            }
            continue;
        }

        for index in 0..config.groups.len() {
            {
                let mut tending = tending.lock().unwrap_or_else(|error| error.into_inner());

                if tending.contains(&index) {
                    continue;
                }
                tending.push(index);
            }
            let (leader, tending) = (leader.clone(), Arc::clone(&tending));

            tokio::spawn(async move {
                leader.tend(&leader.config.groups[index]).await;
                tending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .retain(|known| *known != index);
            });
        }

        let recorded = machine.commits();

        if recorded_at.is_none_or(|at| at.elapsed() >= RECORD_EVERY)
            && record_commits(&raft, &commits, &recorded).await
        {
            recorded_at = Some(Instant::now());
        }
        push_cluster_map(&config, &machine, &sightings).await;
        // A move the previous leader began continues here.
        super::migrate::resume(&moves);
    }
}

impl Leader {
    /// One tick of one group: record its first primary, fail it over when a majority sees the
    /// primary down, or point its members at the agreed primary.
    async fn tend(&self, configured: &Group) {
        let Self {
            config,
            raft,
            machine,
            sightings,
            commits,
            switches,
            leadership,
            ..
        } = self;
        let fence = Leading { leadership, raft };
        let group = &machine.group(configured);
        let seen = sightings.seen(group);
        let Some(primary) = machine.primaries().get(&group.name).cloned() else {
            // First start: record the member that already is the primary. Nothing is
            // changed on the data nodes until exactly one of them answers as one.
            let masters: Vec<usize> = (0..seen.len())
                .filter(|&member| seen[member].role == Some(Role::Master))
                .collect();

            match masters.as_slice() {
                [only] => {
                    let member = group.members[*only].address.to_string();

                    propose(raft, group, member, 1, None).await;
                }
                [] => {}
                _ => {
                    warn!(group = %group.name, "several members answer as primary; waiting for one")
                }
            }
            return;
        };
        let Some(at) = primary.index(group) else {
            warn!(group = %group.name, primary = %primary.member, "the recorded primary is not among the group's members; leaving the group as it is");
            return;
        };
        let committed = commits.learn(
            group,
            &primary,
            &seen,
            config.down_after,
            &machine.commits(),
        );
        let primary_age = seen[at].last_ok.map(|at| at.elapsed());

        // Held until the failover is recorded, so no coordinator is confirmed this primary
        // between reporting its lease ended and the record naming another member.
        if primary_age.is_none_or(|age| age > config.down_after) {
            let decided = sightings.grants().decision(&group.name).await;

            match objectively_down(config, raft, sightings, group, &primary.member)
                .await
                .0
            {
                Verdict::Down => {
                    let failing = Failing {
                        group,
                        primary,
                        at,
                        seen: &seen,
                        committed: &committed,
                        commits,
                        decided,
                        stats: sightings.stats(),
                    };
                    let _ = failover(config, raft, machine, switches, failing, false, &fence).await;
                    return;
                }
                Verdict::LeaseRunning => {
                    if sightings
                        .stats()
                        .enter(&group.name, GroupState::LeaseRunning)
                    {
                        info!(group = %group.name, "primary is down; waiting for its lease to end (repeated at most every 10 s)");
                    }
                }
                Verdict::Up => {
                    sightings.stats().enter(&group.name, GroupState::Ok);
                }
            }
        } else {
            sightings.stats().enter(&group.name, GroupState::Ok);
        }

        reconcile(config, group, &primary, &seen, &fence).await;
        self.list_again(configured).await;
        self.start_discovery(configured, group, &seen[at]);
    }
}

impl Leader {
    /// Records the group's list again when the file names a member that was forgotten, which
    /// takes it back (see [`super::raft::apply_members`]). Rare: only after the file changed.
    async fn list_again(&self, configured: &Group) {
        let forgotten = self.machine.forgotten(&configured.name);

        if !configured
            .members
            .iter()
            .any(|member| forgotten.contains(&member.address.to_string()))
        {
            return;
        }
        let (group, previous) = self.machine.group_and_record(configured);
        let request = Request::SetMembers {
            group: configured.name.clone(),
            previous,
            members: group.members,
        };

        match tokio::time::timeout(DISCOVERY_WRITE, self.raft.client_write(request)).await {
            Ok(Ok(reply)) if reply.data => {
                info!(group = %configured.name, "a forgotten member is listed in the file again; it is a member again")
            }
            _ => {}
        }
    }

    /// Starts [`Leader::discover`] on its own task when the answering primary lists replicas
    /// that `group` does not have, have not been forgotten and did not fail lately; one at a
    /// time per group, so it never delays the group's tending.
    fn start_discovery(&self, configured: &Group, group: &Group, primary: &Sighting) {
        if primary.role != Some(Role::Master)
            || primary
                .last_ok
                .is_none_or(|at| at.elapsed() > self.config.down_after)
        {
            return;
        }
        let forgotten = self.machine.forgotten(&group.name);
        let mut unknown: Vec<super::config::Endpoint> = Vec::new();

        {
            let mut failed = self
                .discovery
                .failed
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            failed.retain(|_, at| at.elapsed() < DISCOVERY_RETRY);
            for replica in &primary.replicas {
                let name = replica.to_string();

                if !group
                    .members
                    .iter()
                    .any(|member| member.address == *replica)
                    && !forgotten.contains(&name)
                    && !failed.contains_key(&name)
                    && !unknown.contains(replica)
                {
                    unknown.push(replica.clone());
                }
            }
        }
        if unknown.is_empty() {
            return;
        }
        {
            let mut running = self
                .discovery
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            if running.contains(&group.name) {
                return;
            }
            running.push(group.name.clone());
        }
        let (leader, configured) = (self.clone(), configured.clone());

        tokio::spawn(async move {
            leader.discover(&configured, unknown).await;
            leader
                .discovery
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|name| *name != configured.name);
        });
    }

    /// Records the `unknown` replicas the primary lists, so they are watched, followed and can
    /// be promoted. Each is listed by the address clients reach it at (as the primary lists
    /// it); its replication address comes from its own `CONFIG GET replication-listen`, with
    /// the listed host when it listens on every interface. A replica the group has under
    /// another name (a host name for the same address) is not added again, and neither is
    /// one whose address does not resolve now. The list is written only if the recorded one
    /// did not change meanwhile; otherwise the next tick tries again.
    async fn discover(&self, configured: &Group, unknown: Vec<super::config::Endpoint>) {
        let auth = self.config.auth.as_deref();
        let mut known = Vec::new();
        let mut identities = Vec::new();
        let (group, _) = self.machine.group_and_record(configured);

        for member in &group.members {
            known.extend(resolve(&member.address).await);
            identities.extend(
                self.sightings
                    .get(&group.name, &member.address.to_string())
                    .run_id,
            );
        }
        let mut found = Vec::new();

        for replica in unknown {
            let addresses = resolve(&replica).await;

            if addresses.is_empty() || addresses.iter().any(|address| known.contains(address)) {
                continue;
            }
            // The same process under a name that does not resolve to a known address (a
            // service address and a pod address, for example).
            let identity = run_id(&replica, auth).await;

            if identity
                .as_ref()
                .is_some_and(|identity| identities.contains(identity))
            {
                continue;
            }
            let Some(replication) =
                replication_address(&replica, self.config.auth.as_deref()).await
            else {
                self.discovery
                    .failed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(replica.to_string(), Instant::now());
                continue;
            };

            known.extend(addresses);
            identities.extend(identity);
            found.push(Member {
                address: replica,
                replication,
            });
        }
        if found.is_empty() {
            return;
        }
        // Read again just before writing: the write applies only over this exact list.
        let (group, previous) = self.machine.group_and_record(configured);
        let mut members = group.members;

        for member in found {
            if members.iter().all(|known| known.address != member.address) {
                info!(group = %configured.name, member = %member.address, replication = %member.replication, "found a new replica of the group");
                members.push(member);
            }
        }
        let request = Request::SetMembers {
            group: configured.name.clone(),
            previous,
            members,
        };

        match tokio::time::timeout(DISCOVERY_WRITE, self.raft.client_write(request)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                warn!(group = %configured.name, %error, "could not record the group's new replicas")
            }
            Err(_) => {
                warn!(group = %configured.name, "recording the group's new replicas timed out")
            }
        }
    }
}

/// How long recording discovered replicas may take before it is tried again on a later tick.
const DISCOVERY_WRITE: Duration = Duration::from_secs(5);

/// The node's `run_id` from `INFO server`: one per process, so two addresses of one node
/// give the same. `None` when it does not answer or is too old to report one.
async fn run_id(address: &super::config::Endpoint, auth: Option<&str>) -> Option<String> {
    let mut client = NodeClient::connect(address, auth, CALL_TIMEOUT)
        .await
        .ok()?;
    let text = client
        .call(&[b"INFO", b"server"], CALL_TIMEOUT)
        .await
        .ok()?
        .text()?;

    run_id_in(&text)
}

/// The `run_id` line of an `INFO server` text.
fn run_id_in(text: &str) -> Option<String> {
    text.lines()
        .find_map(|line| line.strip_prefix("run_id:"))
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
}

/// The socket addresses `endpoint` names now; none when it does not resolve.
async fn resolve(endpoint: &super::config::Endpoint) -> Vec<std::net::SocketAddr> {
    match tokio::time::timeout(
        CALL_TIMEOUT,
        tokio::net::lookup_host((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    {
        Ok(Ok(addresses)) => addresses.collect(),
        _ => Vec::new(),
    }
}

/// Where the replica at `address` takes replication connections: its `replication-listen`,
/// with `address`'s host when it listens on every interface. `None` when it does not answer
/// or has no replication listener.
async fn replication_address(
    address: &super::config::Endpoint,
    auth: Option<&str>,
) -> Option<super::config::Endpoint> {
    let mut client = NodeClient::connect(address, auth, CALL_TIMEOUT)
        .await
        .ok()?;
    let reply = client
        .call(&[b"CONFIG", b"GET", b"replication-listen"], CALL_TIMEOUT)
        .await
        .ok()?;
    let Value::Array(Some(pair)) = reply else {
        return None;
    };
    let mut listen = super::config::Endpoint::parse(&pair.get(1)?.text()?).ok()?;
    let anywhere = listen
        .host
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_unspecified());

    if anywhere {
        listen.host.clone_from(&address.host);
    }
    Some(listen)
}

/// Sends the cluster map to every answering member that serves an older one, so `MOVED`
/// names each group's agreed primary.
async fn push_cluster_map(
    config: &CoordinatorConfig,
    machine: &StateMachine,
    sightings: &Sightings,
) {
    // Members a push is still running for, so a slow one never holds up the failover loop or
    // gets a second push meanwhile; and when each member's failure was last logged.
    static PUSHING: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static WARNED: Mutex<Vec<(String, Instant)>> = Mutex::new(Vec::new());
    const WARN_EVERY: Duration = Duration::from_secs(60);

    let Some((version, map)) = config.cluster_map(&machine.primaries(), &machine.moves().moved)
    else {
        return;
    };
    let map: Arc<[u8]> = map.into_bytes().into();

    for group in &config.groups {
        for member in machine.group(group).members {
            let sighting = sightings.get(&group.name, &member.address.to_string());

            if sighting
                .last_ok
                .is_none_or(|at| at.elapsed() > config.down_after)
                || sighting.topology.is_none_or(|served| served >= version)
            {
                continue;
            }
            let key = member.address.to_string();
            {
                let mut pushing = PUSHING.lock().unwrap_or_else(|error| error.into_inner());

                if pushing.contains(&key) {
                    continue;
                }
                pushing.push(key.clone());
            }
            let (member, auth, map, name) = (
                member.clone(),
                config.auth.clone(),
                Arc::clone(&map),
                group.name.clone(),
            );

            let timeout = command_timeout(config);

            tokio::spawn(async move {
                let result = command(
                    &member,
                    auth.as_deref(),
                    &[b"RUVIO", b"TOPOLOGY", b"SET", &map],
                    timeout,
                )
                .await;

                match result {
                    Ok(()) => {
                        info!(group = %name, member = %member.address, version, "sent the cluster map")
                    }
                    Err(error) => {
                        let mut warned = WARNED.lock().unwrap_or_else(|error| error.into_inner());
                        let last = warned.iter_mut().find(|(known, _)| *known == key);

                        if last
                            .as_ref()
                            .is_none_or(|(_, at)| at.elapsed() > WARN_EVERY)
                        {
                            warn!(group = %name, member = %member.address, %error, "could not send the cluster map (repeated at most once a minute)");
                            match last {
                                Some((_, at)) => *at = Instant::now(),
                                None => warned.push((key.clone(), Instant::now())),
                            }
                        }
                    }
                }
                PUSHING
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .retain(|known| *known != key);
            });
        }
    }
}

/// How often at most the leader records committed sequences that rose.
const RECORD_EVERY: Duration = Duration::from_secs(1);

/// Whether this coordinator has applied its log up to its own term: as leader, everything
/// earlier leaders committed.
pub fn caught_up(raft: &Raft) -> bool {
    let metrics = raft.metrics();
    let metrics = metrics.borrow();

    applied_own_term(metrics.last_applied, metrics.current_term)
}

fn applied_own_term(last_applied: Option<openraft::LogId<NodeId>>, term: u64) -> bool {
    last_applied.is_some_and(|applied| applied.leader_id.term == term)
}

/// Writes the shards whose committed sequence rose since the last record through Raft, all
/// groups in one entry. Whether it wrote; a write that fails or takes too long is tried again
/// later.
async fn record_commits(raft: &Raft, commits: &Commits, recorded: &Recorded) -> bool {
    let raised = commits.raised(recorded);

    if raised.is_empty() {
        return false;
    }
    match tokio::time::timeout(CALL_TIMEOUT, raft.client_write(Request::Commit(raised))).await {
        Ok(Ok(_)) => true,
        Ok(Err(error)) => {
            warn!(%error, "could not record the committed sequences");
            false
        }
        Err(_) => {
            warn!("recording the committed sequences timed out");
            false
        }
    }
}

async fn propose(
    raft: &Raft,
    group: &Group,
    member: String,
    epoch: u64,
    commit: Option<Commit>,
) -> bool {
    let request = Request::SetPrimary {
        group: group.name.clone(),
        member,
        epoch,
        commit,
    };

    match raft.client_write(request).await {
        Ok(reply) => reply.data,
        Err(error) => {
            warn!(group = %group.name, %error, "could not record the primary");
            false
        }
    }
}

/// A group whose primary is down, as the leader sees it, and its decision lock
/// ([`Grants::decision`]), released once the promotion is recorded.
struct Failing<'a> {
    group: &'a Group,
    primary: Primary,
    /// The primary's index in `group`.
    at: usize,
    seen: &'a [Sighting],
    committed: &'a [u64],
    commits: &'a Commits,
    decided: DecisionGuard,
    stats: &'a Stats,
}

/// Promotes a replica of a group whose primary is down: one that holds every committed record,
/// or with `force` or `unclean_failover` the best one anyway. The promoted member's address.
/// Each command to a data node waits for `fence`; the promotion stands in the record either
/// way, and the leader after this one points the members at it.
async fn failover(
    config: &CoordinatorConfig,
    raft: &Raft,
    machine: &StateMachine,
    switches: &Switches,
    failing: Failing<'_>,
    force: bool,
    fence: &impl Fence,
) -> Result<String, String> {
    let Failing {
        group,
        primary,
        at,
        seen,
        committed,
        commits,
        decided,
        stats,
    } = failing;
    let choice = choose(
        group,
        at,
        seen,
        config.down_after,
        committed,
        force || config.unclean_failover,
    );
    let candidate = match choice {
        Choice::Promote(candidate) => candidate,
        Choice::Unclean(candidate) => {
            warn!(
                group = %group.name,
                to = %group.members[candidate].address,
                missing = %missing(&seen[candidate], committed),
                "no replica holds every record a durable reply was released for; promoting the best one, and the records it lacks are lost"
            );
            candidate
        }
        Choice::Wait => {
            if stats.enter(&group.name, GroupState::Waiting) {
                warn!(
                    group = %group.name,
                    "primary is down and no replica holds every record a durable reply was released for; waiting (unclean_failover or COORDINATOR FAILOVER group FORCE promotes one anyway; repeated at most every 10 s)"
                );
            }
            return Err("ERR no replica holds every committed record; waiting for one".into());
        }
        Choice::Nobody => {
            if stats.enter(&group.name, GroupState::NoCandidate) {
                warn!(group = %group.name, "primary is down and no replica can take over (repeated at most every 10 s)");
            }
            return Err("ERR no replica can take over".into());
        }
    };

    warn!(
        group = %group.name,
        from = %group.members[at].address,
        to = %group.members[candidate].address,
        force,
        "primary is down; failing over"
    );
    // Decided once, here, from the promoted member as the leader sees it now, and recorded with
    // the promotion: every coordinator takes the same values from the record.
    let kept = commits.after_promotion(
        &group.name,
        &seen[candidate].shard_durable,
        primary.epoch + 1,
    );

    let promoted = &group.members[candidate];
    let new = &promoted.address;

    if !propose(raft, group, new.to_string(), primary.epoch + 1, Some(kept)).await {
        return Err("ERR the failover was not recorded; try again".into());
    }
    stats.failed_over();
    stats.enter(&group.name, GroupState::Ok);
    // Applied here once recorded: a confirmation waiting for the decision reads the new
    // primary. The calls to the data nodes below need not hold it up.
    drop(decided);

    let timeout = command_timeout(config);

    // The newest map first: a range the group gave away is then answered with `MOVED`
    // before the promoted member takes writes.
    if let Some((_, map)) = config.cluster_map(&machine.primaries(), &machine.moves().moved) {
        if !fence.holds().await {
            warn!(group = %group.name, primary = %new, "recorded the failover but no longer leads; the next leader promotes the member");
            return Ok(new.to_string());
        }
        if let Err(error) = command(
            promoted,
            config.auth.as_deref(),
            &[b"RUVIO", b"TOPOLOGY", b"SET", map.as_bytes()],
            timeout,
        )
        .await
        {
            warn!(group = %group.name, %error, "the chosen replica refused the cluster map before its promotion");
        }
    }
    if !fence.holds().await {
        warn!(group = %group.name, primary = %new, "recorded the failover but no longer leads; the next leader promotes the member");
        return Ok(new.to_string());
    }
    if let Err(error) = command(
        promoted,
        config.auth.as_deref(),
        &[b"REPLICAOF", b"NO", b"ONE"],
        timeout,
    )
    .await
    {
        // The record stands; the next ticks keep reconciling, and a later failover can pick
        // another replica if this one stays down.
        warn!(group = %group.name, %error, "the chosen replica refused promotion");
    }

    let old = &group.members[at].address;
    let _ = switches.send(format!(
        "{} {} {} {} {}",
        group.name, old.host, old.port, new.host, new.port
    ));
    info!(group = %group.name, primary = %new, "failover done");
    reconcile(
        config,
        group,
        &Primary {
            member: new.to_string(),
            epoch: primary.epoch + 1,
        },
        seen,
        fence,
    )
    .await;
    Ok(new.to_string())
}

/// What `COORDINATOR FAILOVER group FORCE` needs from the coordinator.
pub struct Force<'a> {
    pub config: &'a CoordinatorConfig,
    pub raft: &'a Raft,
    pub machine: &'a StateMachine,
    pub sightings: &'a Sightings,
    pub commits: &'a Commits,
    pub switches: &'a Switches,
    pub leadership: &'a Leadership,
}

/// `COORDINATOR FAILOVER group FORCE` on the leader: promotes the best answering replica of a
/// group whose primary is down now, even one that lacks committed records. The promoted
/// member's address.
pub async fn force_failover(force: Force<'_>, configured: &Group) -> Result<String, String> {
    if !caught_up(force.raft) {
        return Err("ERR the leader is still applying the log; try again".into());
    }
    let group = &force.machine.group(configured);
    let Some(primary) = force.machine.primaries().get(&group.name).cloned() else {
        return Err(format!(
            "ERR group {} has no agreed primary yet",
            group.name
        ));
    };
    let Some(at) = primary.index(group) else {
        return Err(format!(
            "ERR the primary of {} is not among its members",
            group.name
        ));
    };
    let seen = force.sightings.seen(group);

    if seen[at]
        .last_ok
        .is_some_and(|at| at.elapsed() <= force.config.down_after)
    {
        return Err(format!(
            "ERR the primary of {} answers; FORCE only overrides a failover that waits",
            group.name
        ));
    }
    // FORCE overrides the wait for committed records, never the lease: promoting while the
    // old primary may still hold it would let both take writes. Asked even when this
    // coordinator grants no lease, since the others may.
    let decided = force.sightings.grants().decision(&group.name).await;

    match objectively_down(
        force.config,
        force.raft,
        force.sightings,
        group,
        &primary.member,
    )
    .await
    {
        (Verdict::LeaseRunning, _) => {
            return Err(format!(
                "ERR the primary of {} may still hold its lease; try again once a majority of coordinators saw it end",
                group.name
            ));
        }
        (Verdict::Up, true) => {
            return Err(format!(
                "ERR a majority of coordinators still hear the primary of {}, so its lease may be renewed",
                group.name
            ));
        }
        _ => {}
    }
    let committed = force.commits.learn(
        group,
        &primary,
        &seen,
        force.config.down_after,
        &force.machine.commits(),
    );
    let failing = Failing {
        group,
        primary,
        at,
        seen: &seen,
        committed: &committed,
        commits: force.commits,
        decided,
        stats: force.sightings.stats(),
    };
    let fence = Leading {
        leadership: force.leadership,
        raft: force.raft,
    };

    failover(
        force.config,
        force.raft,
        force.machine,
        force.switches,
        failing,
        true,
        &fence,
    )
    .await
}

/// Makes every answering member agree with the record: the primary takes writes, the others
/// follow it. Stops at the first command `fence` does not let through: the record it acts on
/// may be stale by then.
async fn reconcile(
    config: &CoordinatorConfig,
    group: &Group,
    primary: &Primary,
    seen: &[Sighting],
    fence: &impl Fence,
) {
    let Some(at) = primary.index(group) else {
        return;
    };
    let target = &group.members[at];
    let timeout = command_timeout(config);

    for (index, member) in group.members.iter().enumerate() {
        let sighting = &seen[index];

        if sighting.loading
            || sighting
                .last_ok
                .is_none_or(|at| at.elapsed() > config.down_after)
        {
            continue;
        }

        // The primary listed again under another entry (the same address or replication
        // address): never pointed at itself.
        if index != at
            && (member.address == target.address || member.replication == target.replication)
        {
            continue;
        }
        let agrees = if index == at {
            sighting.role == Some(Role::Master)
        } else {
            sighting.role == Some(Role::Replica(Some(target.replication.to_string())))
        };

        if agrees {
            continue;
        }
        if !fence.holds().await {
            warn!(group = %group.name, "no longer confirmed as the leader; leaving the group's members as they are");
            return;
        }
        // The primary itself under another address (same process identity): left alone.
        if index != at && sighting.run_id.is_some() && sighting.run_id == seen[at].run_id {
            continue;
        }
        let result = if index == at {
            command(
                member,
                config.auth.as_deref(),
                &[b"REPLICAOF", b"NO", b"ONE"],
                timeout,
            )
            .await
        } else {
            let host = target.replication.host.clone();
            let port = target.replication.port.to_string();

            command(
                member,
                config.auth.as_deref(),
                &[b"REPLICAOF", host.as_bytes(), port.as_bytes()],
                timeout,
            )
            .await
        };

        match result {
            Ok(()) => {
                info!(group = %group.name, member = %member.address, "pointed at the agreed primary")
            }
            Err(error) => {
                warn!(group = %group.name, member = %member.address, %error, "could not point at the agreed primary")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(count: usize) -> Group {
        Group {
            name: "main".into(),
            members: (0..count)
                .map(|index| Member {
                    address: super::super::config::Endpoint::parse(&format!(
                        "127.0.0.1:{}",
                        6379 + index
                    ))
                    .unwrap(),
                    replication: super::super::config::Endpoint::parse(&format!(
                        "127.0.0.1:{}",
                        26379 + index
                    ))
                    .unwrap(),
                })
                .collect(),
            shards: None,
            slots: Vec::new(),
        }
    }

    /// Member `index`'s client address in [`group`].
    fn address(index: usize) -> String {
        format!("127.0.0.1:{}", 6379 + index)
    }

    fn replica(durable: u64) -> Sighting {
        sharded(&[durable])
    }

    /// An answering replica holding `durable` in each shard.
    fn sharded(durable: &[u64]) -> Sighting {
        Sighting {
            last_ok: Some(Instant::now()),
            role: Some(Role::Replica(None)),
            durable: durable.iter().sum(),
            shard_durable: durable.to_vec(),
            shard_committed: vec![0; durable.len()],
            loading: false,
            topology: None,
            replicas: Vec::new(),
            run_id: None,
            shards: durable.len() as u16,
            shards_answering: durable.len() as u16,
        }
    }

    /// A member as the leader saw it: its role, durable and committed sequences per shard.
    fn member(role: Role, durable: &[u64], committed: &[u64]) -> Sighting {
        Sighting {
            role: Some(role),
            shard_committed: committed.to_vec(),
            ..sharded(durable)
        }
    }

    fn following(group: &Group, member: usize) -> Role {
        Role::Replica(Some(group.members[member].replication.to_string()))
    }

    #[test]
    fn a_member_with_a_shard_down_is_not_heard_from_and_is_no_candidate() {
        let sightings = Sightings::default();
        let earlier = Instant::now() - Duration::from_secs(5);
        let whole = Sighting {
            last_ok: Some(earlier),
            shards: 2,
            shards_answering: 2,
            ..sharded(&[10, 10])
        };

        sightings.set("main", &address(1), whole);
        // Shard 1 stops answering: the last complete sighting stays, with the count.
        sightings.set(
            "main",
            &address(1),
            Sighting {
                shards: 2,
                shards_answering: 1,
                ..sharded(&[20, 0])
            },
        );
        let kept = sightings.get("main", &address(1));

        assert_eq!(kept.last_ok, Some(earlier));
        assert_eq!(kept.shard_durable, [10, 10]);
        assert!(!kept.complete());
        // Never seen whole: not heard from at all.
        sightings.set(
            "main",
            &address(2),
            Sighting {
                shards: 2,
                shards_answering: 1,
                ..sharded(&[20, 0])
            },
        );
        assert_eq!(sightings.get("main", &address(2)).last_ok, None);

        // A fresh replica with a shard down is passed over for a complete one.
        let group = group(3);
        let fresh = Duration::from_secs(1);
        let partial = Sighting {
            shards: 2,
            shards_answering: 1,
            ..sharded(&[50, 50])
        };
        let complete = Sighting {
            shards: 2,
            shards_answering: 2,
            ..sharded(&[10, 10])
        };

        assert_eq!(
            choose_candidate(
                &group,
                0,
                &[replica(0), partial.clone(), complete],
                fresh,
                &[]
            ),
            Some(2)
        );
        assert_eq!(
            choose_candidate(
                &group,
                0,
                &[replica(0), partial.clone(), partial],
                fresh,
                &[]
            ),
            None
        );
    }

    #[test]
    fn a_group_state_is_warned_about_when_entered_then_at_most_every_ten_seconds() {
        let stats = Stats::default();

        assert_eq!(stats.state("main"), None);
        assert!(stats.enter("main", GroupState::Waiting));
        assert!(!stats.enter("main", GroupState::Waiting));
        assert_eq!(stats.state("main").unwrap().0, GroupState::Waiting);
        // Another group, or another state, is warned about at once.
        assert!(stats.enter("other", GroupState::Waiting));
        assert!(stats.enter("main", GroupState::NoCandidate));
        // Ten seconds on, the same state is due again.
        stats.groups.lock().unwrap().get_mut("main").unwrap().2 -= WARN_EVERY;
        assert!(stats.enter("main", GroupState::NoCandidate));
        assert!(!stats.enter("main", GroupState::NoCandidate));

        assert_eq!(stats.failovers(), (0, 0));
        stats.failed_over();
        assert_eq!(stats.failovers().0, 1);
        assert!(stats.failovers().1 > 0);
    }

    #[test]
    fn the_replicas_a_primary_lists_are_read_from_its_info() {
        let text = "# Replication\r\nrole:master\r\nconnected_slaves:3\r\n\
                    slave0:ip=10.0.0.2,port=6379,state=online,offset=10,lag=0\r\n\
                    slave1:ip=ruvio-3,port=7000,state=online,offset=10,lag=1\r\n\
                    slave_read_only:1\r\n";
        let listed: Vec<String> = listed_replicas(text)
            .iter()
            .map(ToString::to_string)
            .collect();

        assert_eq!(listed, ["10.0.0.2:6379", "ruvio-3:7000"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failover_decision_holds_up_only_its_own_group() {
        let grants = Grants::default();
        let deciding = grants.decision("a").await;

        // Another group is not held up.
        drop(grants.decision("b").await);
        // The same group waits until the decision is released.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), grants.decision("a"))
                .await
                .is_err()
        );
        drop(deciding);
        drop(grants.decision("a").await);
    }

    #[test]
    fn a_coordinator_grants_only_after_the_leader_confirms_the_primary() {
        let start = Instant::now();
        let ms = Duration::from_millis;
        let safe = ms(2400);
        let mut granting = Granting::default();

        // Just started: nothing confirmed, so no grant at any epoch.
        assert!(!granting.record(1, start));
        granting.confirm(0, 1);
        assert!(!granting.record(2, start + ms(10)));
        assert!(granting.record(1, start + ms(10)));
        assert_eq!(granting.last, Some(start + ms(10)));

        // Grants within the safe interval: the lease has not ended, nothing is promised.
        assert!(!granting.report(start, start + ms(2410), safe));
        assert!(granting.record(1, start + ms(2410)));

        // A gap past it: the report promises no grant until the leader confirms again, and
        // that promise holds however much later the renewal comes.
        assert!(granting.report(start, start + ms(4811), safe));
        assert_eq!(granting.cleared, None);
        assert!(!granting.record(1, start + ms(4900)));
        assert!(!granting.record(1, start + ms(60_000)));
        assert_eq!(granting.last, Some(start + ms(2410)));
    }

    #[test]
    fn a_confirmation_asked_before_a_report_does_not_clear_the_member() {
        let start = Instant::now();
        let safe = Duration::from_millis(2400);
        let mut granting = Granting::default();
        let asked_at = granting.reports;

        // The leader asks for the lease while this coordinator waits for a confirmation that
        // read the record before the failover.
        assert!(granting.report(start, start + safe * 2, safe));
        granting.confirm(asked_at, 1);
        assert!(!granting.record(1, start + safe * 2));

        // A confirmation asked after the report counts.
        granting.confirm(granting.reports, 1);
        assert!(granting.record(1, start + safe * 2));
        // The record moved on: the old epoch is never granted again.
        granting.confirm(granting.reports, 2);
        assert!(!granting.record(1, start + safe * 3));
        assert!(granting.record(2, start + safe * 3));
    }

    #[test]
    fn grant_ages_never_count_from_before_the_coordinator_started() {
        let start = Instant::now();
        let ms = Duration::from_millis;

        assert_eq!(grant_age(None, start, start + ms(500)), ms(500));
        assert_eq!(
            grant_age(Some(start + ms(100)), start, start + ms(500)),
            ms(400)
        );
        assert_eq!(
            grant_age(Some(start - ms(9000)), start, start + ms(500)),
            ms(500)
        );
        assert_eq!(
            grant_age(Some(start + ms(900)), start, start + ms(500)),
            ms(0)
        );
    }

    #[test]
    fn a_failover_waits_for_a_majority_to_hear_the_primary_down_and_see_its_lease_end() {
        assert_eq!(verdict(1, 3, true, 2), Verdict::Up);
        assert_eq!(verdict(2, 1, true, 2), Verdict::LeaseRunning);
        assert_eq!(verdict(2, 2, true, 2), Verdict::Down);
        // No coordinator grants leases: hearing alone decides.
        assert_eq!(verdict(2, 0, false, 2), Verdict::Down);
        assert_eq!(verdict(3, 2, true, 3), Verdict::LeaseRunning);
    }

    #[test]
    fn the_candidate_is_the_answering_complete_replica_with_the_most_history() {
        let group = group(4);
        let fresh = Duration::from_secs(5);
        let mut seen = vec![Sighting::default(), replica(10), replica(30), replica(30)];

        // Equal history: the lower index wins.
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[]), Some(2));
        seen[2].loading = true;
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[]), Some(3));
        seen[3].last_ok = None;
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[]), Some(1));
        seen[1].role = Some(Role::Master);
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[]), None);
    }

    #[test]
    fn only_a_replica_holding_every_committed_record_is_promoted() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let seen = vec![Sighting::default(), replica(10), replica(30)];
        let choice =
            |committed: &[u64], unclean| choose(&group, 0, &seen, fresh, committed, unclean);

        // Nothing committed, or zeros: exactly the choice without the rule.
        assert_eq!(choice(&[], false), Choice::Promote(2));
        assert_eq!(choice(&[0], false), Choice::Promote(2));
        assert_eq!(choice(&[30], false), Choice::Promote(2));
        // Neither replica holds record 31: wait.
        assert_eq!(choice(&[31], false), Choice::Wait);
        // `unclean_failover` and `FORCE` promote the best one anyway.
        assert_eq!(choice(&[31], true), Choice::Unclean(2));
        // A qualifying replica is promoted cleanly even when unclean promotion is allowed.
        assert_eq!(choice(&[30], true), Choice::Promote(2));
        assert_eq!(missing(&seen[2], &[31]), "shard 0: holds 30 of 31");

        let nobody = vec![Sighting::default(); 3];
        assert_eq!(
            choose(&group, 0, &nobody, fresh, &[5], true),
            Choice::Nobody
        );
    }

    #[test]
    fn committed_records_are_compared_shard_by_shard() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        // Member 1 holds more in total but lacks shard 1's committed records.
        let seen = vec![Sighting::default(), sharded(&[100, 5]), sharded(&[20, 9])];

        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[0, 0]), Some(1));
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[10, 8]), Some(2));
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[30, 8]), None);
        // A shard the replica did not report holds nothing.
        assert_eq!(choose_candidate(&group, 0, &seen, fresh, &[0, 0, 1]), None);
    }

    #[test]
    fn the_leader_keeps_the_highest_committed_sequence_it_saw() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let commits = Commits::default();
        let primary = Primary {
            member: address(0),
            epoch: 1,
        };
        let mut seen = vec![
            member(Role::Master, &[50, 40], &[20, 0]),
            member(following(&group, 0), &[45, 40], &[20, 30]),
            // Follows another address: its value is not this primary's.
            member(following(&group, 2), &[0, 0], &[99, 99]),
        ];

        assert_eq!(
            commits.learn(&group, &primary, &seen, fresh, &Recorded::new()),
            vec![20, 30]
        );
        // A replica's value above what the answering primary holds is left from an earlier
        // primary: it counts only up to the primary's sequence.
        seen[1].shard_committed = vec![70, 30];
        assert_eq!(
            commits.learn(&group, &primary, &seen, fresh, &Recorded::new()),
            vec![50, 30]
        );
        // The primary is down: replicas' values count as told, and the highest one stays.
        seen[0].last_ok = None;
        seen[1].shard_committed = vec![55, 10];
        assert_eq!(
            commits.learn(&group, &primary, &seen, fresh, &Recorded::new()),
            vec![55, 30]
        );
        assert_eq!(
            choose(&group, 0, &seen, fresh, &[55, 30], false),
            Choice::Wait
        );

        // Groups that never use durability know nothing committed.
        let quiet = vec![
            member(Role::Master, &[50], &[0]),
            member(following(&group, 0), &[45], &[0]),
            Sighting::default(),
        ];
        let other = Group {
            name: "quiet".into(),
            ..group.clone()
        };
        assert!(
            commits
                .learn(&other, &primary, &quiet, fresh, &Recorded::new())
                .is_empty()
        );
    }

    /// The replicated state as the state machine applies it.
    #[derive(Default)]
    struct Agreed {
        primaries: super::super::raft::Primaries,
        recorded: Recorded,
    }

    impl Agreed {
        fn apply(&mut self, request: Request) {
            use super::super::raft::{apply_commit, apply_promotion, apply_request};

            if apply_request(&mut self.primaries, &request) {
                apply_promotion(&mut self.recorded, &request);
            } else {
                apply_commit(&mut self.recorded, &self.primaries, &request);
            }
        }

        fn promote(&mut self, member: usize, epoch: u64, commit: Option<Commit>) -> Primary {
            self.apply(Request::SetPrimary {
                group: "main".into(),
                member: address(member),
                epoch,
                commit,
            });
            self.primaries["main"].clone()
        }

        /// What the leader writes at the end of a tick.
        fn record(&mut self, commits: &Commits) {
            let raised = commits.raised(&self.recorded);

            if !raised.is_empty() {
                self.apply(Request::Commit(raised));
            }
        }
    }

    #[test]
    fn a_clean_failover_keeps_the_committed_records_and_an_unclean_one_drops_them() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let commits = Commits::default();
        let mut agreed = Agreed::default();
        let first = agreed.promote(0, 1, None);
        let mut seen = vec![
            member(Role::Master, &[50], &[40]),
            member(following(&group, 0), &[45], &[40]),
            member(following(&group, 0), &[30], &[40]),
        ];

        assert_eq!(
            commits.learn(&group, &first, &seen, fresh, &agreed.recorded),
            vec![40]
        );
        agreed.record(&commits);

        // Member 1 held every committed record and was promoted: they stay committed, so
        // member 2 must catch up before it can take over.
        let kept = commits.after_promotion("main", &[45], 2);
        assert_eq!(
            (kept.committed.as_slice(), kept.stale.len()),
            (&[40][..], 0)
        );
        let second = agreed.promote(1, 2, Some(kept));
        seen[0] = Sighting::default();
        seen[1] = member(Role::Master, &[45], &[0]);
        seen[2] = member(following(&group, 1), &[30], &[40]);
        assert_eq!(
            commits.learn(&group, &second, &seen, fresh, &agreed.recorded),
            vec![40]
        );
        assert_eq!(choose(&group, 1, &seen, fresh, &[40], false), Choice::Wait);

        // Forced onto member 2, which lacked them: those records are lost and its own
        // sequences reuse their numbers. Member 1 keeps reporting 40 (a replica keeps the
        // highest value it was told), which must not block the next failover.
        let kept = commits.after_promotion("main", &[30], 3);
        let third = agreed.promote(2, 3, Some(kept));
        seen[1] = member(following(&group, 2), &[30], &[40]);
        seen[2] = member(Role::Master, &[32], &[0]);
        assert_eq!(
            commits.learn(&group, &third, &seen, fresh, &agreed.recorded),
            vec![0]
        );
        seen[2].last_ok = None;
        assert_eq!(
            commits.learn(&group, &third, &seen, fresh, &agreed.recorded),
            vec![0]
        );
        // A durable write of the new primary above the old value counts again.
        seen[1].shard_committed = vec![41];
        assert_eq!(
            commits.learn(&group, &third, &seen, fresh, &agreed.recorded),
            vec![41]
        );
        // A primary record of an older epoch read late changes nothing.
        seen[1].shard_committed = vec![99];
        assert_eq!(
            commits.learn(&group, &second, &seen, fresh, &agreed.recorded),
            vec![41]
        );
    }

    #[test]
    fn a_restarted_coordinator_takes_the_promotion_from_the_record_not_from_sightings() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let leader = Commits::default();
        let mut agreed = Agreed::default();
        let first = agreed.promote(0, 1, None);
        let seen = vec![
            member(Role::Master, &[50], &[40]),
            member(following(&group, 0), &[45], &[40]),
            member(following(&group, 0), &[30], &[0]),
        ];

        leader.learn(&group, &first, &seen, fresh, &agreed.recorded);
        agreed.record(&leader);
        // A clean promotion of member 1, then every coordinator restarts and has not seen
        // member 1 yet: its values stay, they are not taken for an unclean promotion's.
        let clean = agreed.promote(1, 2, Some(leader.after_promotion("main", &[45], 2)));
        let unseen = vec![Sighting::default(), Sighting::default(), seen[2].clone()];
        let restarted = Commits::default();

        assert_eq!(
            restarted.learn(&group, &clean, &unseen, fresh, &agreed.recorded),
            vec![40]
        );
        assert_eq!(
            choose(&group, 1, &unseen, fresh, &[40], false),
            Choice::Wait
        );
        assert!(restarted.raised(&agreed.recorded).is_empty());

        // An unclean one: a restarted coordinator ignores member 0's old value 40 as stale.
        let mut agreed = Agreed::default();
        agreed.promote(0, 1, None);
        agreed.record(&leader);
        let unclean = agreed.promote(2, 2, Some(leader.after_promotion("main", &[30], 2)));
        let reporting = vec![
            member(following(&group, 2), &[45], &[40]),
            Sighting::default(),
            Sighting::default(),
        ];
        let restarted = Commits::default();

        assert_eq!(
            restarted.learn(&group, &unclean, &reporting, fresh, &agreed.recorded),
            vec![0]
        );
        assert_eq!(agreed.recorded["main"].stale, [40]);
    }

    #[test]
    fn a_new_leader_starts_from_the_recorded_commits_and_records_only_what_rose() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let old_leader = Commits::default();
        let mut agreed = Agreed::default();
        let primary = agreed.promote(0, 1, None);
        let mut seen = vec![
            member(Role::Master, &[50, 60], &[40, 0]),
            member(following(&group, 0), &[45, 60], &[40, 0]),
            member(following(&group, 0), &[30, 60], &[0, 0]),
        ];

        assert_eq!(
            old_leader.learn(&group, &primary, &seen, fresh, &agreed.recorded),
            vec![40]
        );
        // Only the shard that rose is sent.
        assert_eq!(
            old_leader.raised(&agreed.recorded)["main"],
            Raised {
                epoch: 1,
                committed: vec![40]
            }
        );
        agreed.record(&old_leader);
        assert!(old_leader.raised(&agreed.recorded).is_empty());
        old_leader.learn(&group, &primary, &seen, fresh, &agreed.recorded);
        assert!(old_leader.raised(&agreed.recorded).is_empty());

        // The old leader and the primary are gone, and so is member 1; member 2 never held
        // record 40 and reports nothing committed. A coordinator that knows only what members
        // report would promote it; one that starts from the record waits.
        seen[0] = Sighting::default();
        seen[1] = Sighting::default();
        let new_leader = Commits::default();

        assert!(
            new_leader
                .learn(&group, &primary, &seen, fresh, &Recorded::new())
                .is_empty()
        );
        let new_leader = Commits::default();
        assert_eq!(
            new_leader.learn(&group, &primary, &seen, fresh, &agreed.recorded),
            vec![40]
        );
        assert_eq!(
            choose(&group, 0, &seen, fresh, &[40, 0], false),
            Choice::Wait
        );
        assert!(new_leader.raised(&agreed.recorded).is_empty());

        // Groups that never use durability write nothing.
        let quiet = Commits::default();
        let idle = vec![
            member(Role::Master, &[50], &[0]),
            member(following(&group, 0), &[45], &[0]),
            Sighting::default(),
        ];
        quiet.learn(&group, &primary, &idle, fresh, &Recorded::new());
        assert!(quiet.raised(&Recorded::new()).is_empty());
    }

    #[test]
    fn a_leader_acts_only_once_it_applied_its_own_term() {
        let applied = |term, index| {
            Some(openraft::LogId::new(
                openraft::CommittedLeaderId::new(term, 1),
                index,
            ))
        };

        assert!(!applied_own_term(None, 1));
        // Elected for term 3, still applying what term 2 wrote (a snapshot and the log after).
        assert!(!applied_own_term(applied(2, 900), 3));
        // Its own blank entry is applied: so is everything before it.
        assert!(applied_own_term(applied(3, 901), 3));
    }

    /// Lets through as many commands as it holds.
    struct Allow(std::sync::atomic::AtomicUsize);

    impl Fence for Allow {
        async fn holds(&self) -> bool {
            use std::sync::atomic::Ordering;

            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        }
    }

    /// Members listening on loopback that count the commands they get and answer `+OK`, or
    /// never answer when `silent`.
    async fn listening(count: usize, silent: bool) -> (Group, Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let commands = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut group = group(count);

        for member in &mut group.members {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let commands = Arc::clone(&commands);

            member.address =
                super::super::config::Endpoint::parse(&format!("127.0.0.1:{port}")).unwrap();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let commands = Arc::clone(&commands);

                    tokio::spawn(async move {
                        let mut buffer = [0; 512];

                        while matches!(stream.read(&mut buffer).await, Ok(read) if read > 0) {
                            commands.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            if !silent && stream.write_all(b"+OK\r\n").await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
        }
        (group, commands)
    }

    fn config(down_after: Duration) -> CoordinatorConfig {
        CoordinatorConfig {
            id: 1,
            listen: "127.0.0.1:0".parse().unwrap(),
            data_dir: std::path::PathBuf::new(),
            peers: Vec::new(),
            ping_interval: Duration::from_millis(100),
            down_after,
            unclean_failover: false,
            primary_lease: None,
            auth: None,
            secret: None,
            tls: Default::default(),
            groups: Vec::new(),
            join: false,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_leader_that_stops_leading_mid_tick_sends_no_further_commands() {
        let (group, commands) = listening(3, false).await;
        let primary = &Primary {
            member: group.members[0].address.to_string(),
            epoch: 2,
        };
        // Every member disagrees with the record: three commands to send.
        let seen = vec![
            member(Role::Replica(None), &[10], &[0]),
            member(Role::Master, &[10], &[0]),
            member(Role::Master, &[10], &[0]),
        ];
        let config = config(Duration::from_millis(1000));
        let sent = || commands.load(std::sync::atomic::Ordering::SeqCst);

        reconcile(&config, &group, primary, &seen, &Allow(0.into())).await;
        assert_eq!(sent(), 0);
        // Confirmed for the first command, then lost.
        reconcile(&config, &group, primary, &seen, &Allow(1.into())).await;
        assert_eq!(sent(), 1);
        reconcile(&config, &group, primary, &seen, &Allow(3.into())).await;
        assert_eq!(sent(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_primary_listed_twice_is_never_pointed_at_itself() {
        let (mut group, commands) = listening(3, false).await;

        // Entry 2 is the primary under another name.
        group.members[2] = group.members[0].clone();
        let primary = &Primary {
            member: group.members[0].address.to_string(),
            epoch: 2,
        };
        let seen = vec![
            member(Role::Master, &[10], &[0]),
            member(following(&group, 0), &[10], &[0]),
            member(Role::Master, &[10], &[0]),
        ];

        reconcile(
            &config(Duration::from_millis(1000)),
            &group,
            primary,
            &seen,
            &Allow(3.into()),
        )
        .await;
        assert_eq!(commands.load(std::sync::atomic::Ordering::SeqCst), 0);

        // Under an address that is not the primary's, but the same process: left alone too.
        let (group, commands) = listening(3, false).await;
        let primary = &Primary {
            member: group.members[0].address.to_string(),
            epoch: 2,
        };
        let same = Sighting {
            run_id: Some("a".repeat(40)),
            ..member(Role::Master, &[10], &[0])
        };
        let seen = vec![
            same.clone(),
            member(following(&group, 0), &[10], &[0]),
            same,
        ];

        reconcile(
            &config(Duration::from_millis(1000)),
            &group,
            primary,
            &seen,
            &Allow(3.into()),
        )
        .await;
        assert_eq!(commands.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_member_that_never_answers_holds_a_command_for_down_after_not_longer() {
        let (group, commands) = listening(2, true).await;
        let primary = &Primary {
            member: group.members[0].address.to_string(),
            epoch: 2,
        };
        let seen = vec![
            member(Role::Master, &[10], &[0]),
            member(Role::Master, &[10], &[0]),
        ];
        let started = Instant::now();

        reconcile(
            &config(Duration::from_millis(1000)),
            &group,
            primary,
            &seen,
            &Allow(1.into()),
        )
        .await;
        assert_eq!(commands.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
    }
}
