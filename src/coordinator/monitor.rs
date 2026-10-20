//! Watching data nodes and acting on them: every coordinator pings every member; the Raft
//! leader decides failovers with a majority of coordinators and keeps every member pointed at
//! the agreed primary.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::broadcast;
use tracing::{info, warn};

use super::{
    config::{CoordinatorConfig, Group, Member},
    raft::{Primary, Raft, Request, RpcRequest, RpcResponse, StateMachine},
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    /// Following the primary whose replication address (`host:port`) is this.
    Replica(Option<String>),
}

/// Every member's latest sighting, by group and member index, and this coordinator's lease
/// grants.
#[derive(Clone, Default)]
pub struct Sightings(Arc<Mutex<HashMap<(String, usize), Sighting>>>, Grants);

impl Sightings {
    pub fn grants(&self) -> &Grants {
        &self.1
    }

    pub fn get(&self, group: &str, member: usize) -> Sighting {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(group.to_owned(), member))
            .cloned()
            .unwrap_or_default()
    }

    fn set(&self, group: &str, member: usize, sighting: Sighting) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert((group.to_owned(), member), sighting);
    }

    /// Milliseconds since each member of `group` last answered, as [`RpcRequest::Observe`]
    /// reports it.
    pub fn ages(&self, group: &Group) -> Vec<Option<u64>> {
        (0..group.members.len())
            .map(|member| {
                self.get(&group.name, member)
                    .last_ok
                    .map(|at| at.elapsed().as_millis() as u64)
            })
            .collect()
    }
}

/// What this coordinator granted for the primary lease (`primary_lease_ms`), and the promises
/// it made to the leader about it.
///
/// When the leader asks whether a primary's lease ended ([`RpcRequest::Leases`]) and this
/// coordinator answers yes, it promises not to grant that member again until the leader
/// confirms, after any failover it was deciding, that the record still names it
/// ([`confirm_locally`]). The leader decides each failover under [`Grants::decision`], and a
/// confirmation waits for it and reads the record after a majority confirmed the leader. So
/// a majority that reported the lease ended cannot renew the old primary once the failover is
/// recorded, and any majority that granted it shares a coordinator with that one.
#[derive(Clone)]
pub struct Grants(Arc<GrantBook>);

struct GrantBook {
    /// A coordinator that just started may have granted before it stopped: it reports every
    /// lease as granted at its start.
    started: Instant,
    members: Mutex<HashMap<(String, usize), Granting>>,
    /// Held by the leader while it decides whether to fail a group over.
    decision: tokio::sync::Mutex<()>,
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
            decision: tokio::sync::Mutex::new(()),
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
    fn with<R>(&self, group: &str, member: usize, act: impl FnOnce(&mut Granting) -> R) -> R {
        let mut members = self
            .0
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        act(members.entry((group.to_owned(), member)).or_default())
    }

    /// [`Granting::report`] for `member` of `group`.
    pub fn report(&self, group: &str, member: usize, safe: Duration) -> bool {
        let started = self.0.started;

        self.with(group, member, |granting| {
            granting.report(started, Instant::now(), safe)
        })
    }

    /// Held by the leader while it decides a failover; [`confirm_locally`] waits for it.
    pub async fn decision(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.decision.lock().await
    }
}

/// [`RpcRequest::Confirm`] on the leader: after any failover decision in progress, and once a
/// majority confirmed this coordinator still leads (so its record is current), the group's
/// primary. `None` when it cannot confirm.
pub async fn confirm_locally(
    raft: &Raft,
    machine: &StateMachine,
    grants: &Grants,
    group: &str,
) -> Option<Primary> {
    let _decided = grants.decision().await;

    match tokio::time::timeout(CALL_TIMEOUT, raft.ensure_linearizable()).await {
        Ok(Ok(_)) => machine.primaries().get(group).copied(),
        _ => None,
    }
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
    let (_, address) = config.peers.iter().find(|(id, _)| *id == leader)?;

    match super::raft::call(
        &address.to_string(),
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
    let member = primary.member;
    let asked_at = grants.with(name, member, |granting| {
        (granting.cleared != Some(primary.epoch)).then_some(granting.reports)
    });

    if let Some(asked_at) = asked_at {
        if confirm(config, raft, machine, grants, name).await != Some(primary) {
            return;
        }
        grants.with(name, member, |granting| {
            granting.confirm(asked_at, primary.epoch)
        });
    }
    let Some(target) = group.members.get(member) else {
        return;
    };
    let Ok(mut client) =
        NodeClient::connect(&target.address, config.auth.as_deref(), CALL_TIMEOUT).await
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
        config.peers.len().to_string(),
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
            let Some(primary) = primaries.get(&group.name).copied() else {
                continue;
            };
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

    for shard in 0..shards {
        let address = member.address.plus(shard);
        let info = if shard == 0 {
            client
                .call(&[b"INFO", b"replication"], CALL_TIMEOUT)
                .await
                .ok()
        } else {
            match NodeClient::connect(&address, auth, CALL_TIMEOUT).await {
                Ok(mut other) => other
                    .call(&[b"INFO", b"replication"], CALL_TIMEOUT)
                    .await
                    .ok(),
                Err(_) => None,
            }
        };
        let text = info.and_then(|info| info.text()).unwrap_or_default();
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

/// Sends one command to a member; `Ok` when it answers `OK`.
async fn command(member: &Member, auth: Option<&str>, args: &[&[u8]]) -> Result<(), String> {
    let mut client = NodeClient::connect(&member.address, auth, CALL_TIMEOUT)
        .await
        .map_err(|error| error.to_string())?;

    match client
        .call(args, Duration::from_secs(10))
        .await
        .map_err(|error| error.to_string())?
    {
        Value::Simple(_) => Ok(()),
        Value::Error(error) => Err(error),
        other => Err(format!("unexpected reply {other:?}")),
    }
}

/// Every group's committed sequences as this coordinator learned them, per shard: a failover
/// keeps these records. Held in memory; a restarted coordinator learns them again from the
/// members.
#[derive(Clone, Default)]
pub struct Commits(Arc<Mutex<HashMap<String, Commit>>>);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Commit {
    /// The primary's epoch the values belong to.
    epoch: u64,
    /// The highest committed sequence seen, per shard.
    committed: Vec<u64>,
    /// Per shard, what replicas may still report from a primary whose history an unclean
    /// failover dropped (a replica keeps the highest value it was ever told); replica reports
    /// up to it are ignored.
    stale: Vec<u64>,
}

fn at(values: &[u64], shard: usize) -> u64 {
    values.get(shard).copied().unwrap_or(0)
}

fn raise(values: &mut Vec<u64>, shard: usize, value: u64) {
    if values.len() <= shard {
        values.resize(shard + 1, 0);
    }
    values[shard] = values[shard].max(value);
}

impl Commit {
    /// Takes in what the primary and the replicas following it report now.
    fn learn(&mut self, group: &Group, primary: Primary, seen: &[Sighting], fresh: Duration) {
        if self.epoch != primary.epoch {
            // A failover. The promoted member holds what was committed before, so it stays
            // committed, unless it was promoted without it: those records are gone and the
            // new primary hands out their sequences again, so the old values no longer apply.
            let promoted = &seen[primary.member];

            for shard in 0..self.committed.len() {
                let committed = self.committed[shard];

                if at(&promoted.shard_durable, shard) < committed {
                    raise(&mut self.stale, shard, committed);
                    self.committed[shard] = 0;
                }
            }
            self.epoch = primary.epoch;
        }

        let head = &seen[primary.member];
        // A primary that answers bounds what can be committed: a replica's higher value is
        // left from an earlier primary.
        let alive = head.last_ok.is_some_and(|at| at.elapsed() <= fresh);
        let following = Some(Role::Replica(Some(
            group.members[primary.member].replication.to_string(),
        )));

        for (member, sighting) in seen.iter().enumerate() {
            let own = member == primary.member && sighting.role == Some(Role::Master);

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
    /// Updates what is known committed in `group` from this tick's sightings and returns it.
    pub fn learn(
        &self,
        group: &Group,
        primary: Primary,
        seen: &[Sighting],
        fresh: Duration,
    ) -> Vec<u64> {
        let mut commits = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let commit = commits.entry(group.name.clone()).or_default();

        commit.learn(group, primary, seen, fresh);
        commit.committed.clone()
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

/// The member to promote: answering, a replica with a complete copy that holds every
/// committed record, holding the most of the history; ties go to the lowest index.
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
pub async fn watch(config: Arc<CoordinatorConfig>, sightings: Sightings) {
    let mut tick = tokio::time::interval(config.ping_interval);

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let mut looks = Vec::new();
        let cluster = config.groups.iter().any(|group| group.shards.is_some());

        for group in &config.groups {
            for (index, member) in group.members.iter().enumerate() {
                let auth = config.auth.clone();
                let member = member.clone();
                let name = group.name.clone();

                looks.push(tokio::spawn(async move {
                    (name, index, look(&member, auth.as_deref(), cluster).await)
                }));
            }
        }
        for look in looks {
            if let Ok((name, index, Some(sighting))) = look.await {
                sightings.set(&name, index, sighting);
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
    sightings: &Sightings,
    group: &Group,
    primary: usize,
) -> (Verdict, bool) {
    let down_after = config.down_after.as_millis() as u64;
    let is_down = |ages: &[Option<u64>]| {
        ages.get(primary)
            .copied()
            .flatten()
            .is_none_or(|age| age > down_after)
    };
    let mut heard_down = usize::from(is_down(&sightings.ages(group)));
    let mut lease_on = config.primary_lease.is_some();
    let mut ended = usize::from(match config.primary_lease {
        Some((_, safe)) => sightings.grants().report(&group.name, primary, safe),
        None => true,
    });
    let mut calls = tokio::task::JoinSet::new();

    for (id, address) in &config.peers {
        if *id == config.id {
            continue;
        }
        let (address, secret, name) = (
            address.to_string(),
            config.secret.clone().unwrap_or_default(),
            group.name.clone(),
        );

        calls.spawn(async move {
            let leases = RpcRequest::Leases {
                group: name.clone(),
                member: primary,
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
                    RpcRequest::Observe { group: name },
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

        heard_down += usize::from(heard.is_some_and(|ages| is_down(&ages)));
        ended += usize::from(peer_ended);
        lease_on |= peer_lease_on;
    }

    (
        verdict(heard_down, ended, lease_on, config.quorum()),
        lease_on,
    )
}

/// The leader's loop: start each group from its configured primary, fail over a primary a
/// majority sees down, and point every answering member at the agreed primary.
pub async fn lead(
    config: Arc<CoordinatorConfig>,
    raft: Raft,
    machine: StateMachine,
    sightings: Sightings,
    commits: Commits,
    switches: Switches,
    moves: super::migrate::Context,
) {
    let mut tick = tokio::time::interval(config.ping_interval);

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        // A leader cut off from the other coordinators keeps `current_leader()` (openraft
        // does not step it down), so a majority confirms it before anything is done on the
        // data nodes; until then it only learns, like a follower.
        if !moves.leadership.confirm(&raft).await {
            // Every coordinator learns what was committed, so a new leader knows it.
            let primaries = machine.primaries();

            for group in &config.groups {
                if let Some(primary) = primaries.get(&group.name) {
                    let seen: Vec<Sighting> = (0..group.members.len())
                        .map(|member| sightings.get(&group.name, member))
                        .collect();

                    commits.learn(group, *primary, &seen, config.down_after);
                }
            }
            continue;
        }

        for group in &config.groups {
            let seen: Vec<Sighting> = (0..group.members.len())
                .map(|member| sightings.get(&group.name, member))
                .collect();
            let Some(primary) = machine.primaries().get(&group.name).copied() else {
                // First start: record the member that already is the primary. Nothing is
                // changed on the data nodes until exactly one of them answers as one.
                let masters: Vec<usize> = (0..seen.len())
                    .filter(|&member| seen[member].role == Some(Role::Master))
                    .collect();

                match masters.as_slice() {
                    [only] => {
                        propose(&raft, group, *only, 1).await;
                    }
                    [] => {}
                    _ => {
                        warn!(group = %group.name, "several members answer as primary; waiting for one")
                    }
                }
                continue;
            };
            let committed = commits.learn(group, primary, &seen, config.down_after);
            let primary_age = seen[primary.member].last_ok.map(|at| at.elapsed());

            // Held through the failover, so no coordinator is confirmed this primary between
            // reporting its lease ended and the record naming another member.
            let decided = match primary_age.is_none_or(|age| age > config.down_after) {
                true => Some(sightings.grants().decision().await),
                false => None,
            };

            if decided.is_some()
                && objectively_down(&config, &sightings, group, primary.member)
                    .await
                    .0
                    == Verdict::Down
            {
                let failing = Failing {
                    group,
                    primary,
                    seen: &seen,
                    committed: &committed,
                };
                let _ = failover(&config, &raft, &machine, &switches, failing, false).await;
                continue;
            }
            drop(decided);

            reconcile(&config, group, primary, &seen).await;
        }

        push_cluster_map(&config, &machine, &sightings).await;
        // A move the previous leader began continues here.
        super::migrate::resume(&moves);
    }
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
        for (index, member) in group.members.iter().enumerate() {
            let sighting = sightings.get(&group.name, index);

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

            tokio::spawn(async move {
                let result = command(
                    &member,
                    auth.as_deref(),
                    &[b"RUVIO", b"TOPOLOGY", b"SET", &map],
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

async fn propose(raft: &Raft, group: &Group, member: usize, epoch: u64) -> bool {
    let request = Request::SetPrimary {
        group: group.name.clone(),
        member,
        epoch,
    };

    match raft.client_write(request).await {
        Ok(reply) => reply.data,
        Err(error) => {
            warn!(group = %group.name, %error, "could not record the primary");
            false
        }
    }
}

/// A group whose primary is down, as the leader sees it.
struct Failing<'a> {
    group: &'a Group,
    primary: Primary,
    seen: &'a [Sighting],
    committed: &'a [u64],
}

/// Promotes a replica of a group whose primary is down: one that holds every committed record,
/// or with `force` or `unclean_failover` the best one anyway. The promoted member's address.
async fn failover(
    config: &CoordinatorConfig,
    raft: &Raft,
    machine: &StateMachine,
    switches: &Switches,
    failing: Failing<'_>,
    force: bool,
) -> Result<String, String> {
    let Failing {
        group,
        primary,
        seen,
        committed,
    } = failing;
    let choice = choose(
        group,
        primary.member,
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
            warn!(
                group = %group.name,
                "primary is down and no replica holds every record a durable reply was released for; waiting (unclean_failover or COORDINATOR FAILOVER group FORCE promotes one anyway)"
            );
            return Err("ERR no replica holds every committed record; waiting for one".into());
        }
        Choice::Nobody => {
            warn!(group = %group.name, "primary is down and no replica can take over");
            return Err("ERR no replica can take over".into());
        }
    };

    warn!(
        group = %group.name,
        from = %group.members[primary.member].address,
        to = %group.members[candidate].address,
        force,
        "primary is down; failing over"
    );
    if !propose(raft, group, candidate, primary.epoch + 1).await {
        return Err("ERR the failover was not recorded; try again".into());
    }

    let promoted = &group.members[candidate];

    // The newest map first: a range the group gave away is then answered with `MOVED`
    // before the promoted member takes writes.
    if let Some((_, map)) = config.cluster_map(&machine.primaries(), &machine.moves().moved)
        && let Err(error) = command(
            promoted,
            config.auth.as_deref(),
            &[b"RUVIO", b"TOPOLOGY", b"SET", map.as_bytes()],
        )
        .await
    {
        warn!(group = %group.name, %error, "the chosen replica refused the cluster map before its promotion");
    }
    if let Err(error) = command(
        promoted,
        config.auth.as_deref(),
        &[b"REPLICAOF", b"NO", b"ONE"],
    )
    .await
    {
        // The record stands; the next ticks keep reconciling, and a later failover can pick
        // another replica if this one stays down.
        warn!(group = %group.name, %error, "the chosen replica refused promotion");
    }

    let old = &group.members[primary.member].address;
    let new = &promoted.address;
    let _ = switches.send(format!(
        "{} {} {} {} {}",
        group.name, old.host, old.port, new.host, new.port
    ));
    info!(group = %group.name, primary = %new, "failover done");
    reconcile(
        config,
        group,
        Primary {
            member: candidate,
            epoch: primary.epoch + 1,
        },
        seen,
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
}

/// `COORDINATOR FAILOVER group FORCE` on the leader: promotes the best answering replica of a
/// group whose primary is down now, even one that lacks committed records. The promoted
/// member's address.
pub async fn force_failover(force: Force<'_>, group: &Group) -> Result<String, String> {
    let Some(primary) = force.machine.primaries().get(&group.name).copied() else {
        return Err(format!(
            "ERR group {} has no agreed primary yet",
            group.name
        ));
    };
    let seen: Vec<Sighting> = (0..group.members.len())
        .map(|member| force.sightings.get(&group.name, member))
        .collect();

    if seen[primary.member]
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
    let _decided = force.sightings.grants().decision().await;

    match objectively_down(force.config, force.sightings, group, primary.member).await {
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
    let committed = force
        .commits
        .learn(group, primary, &seen, force.config.down_after);
    let failing = Failing {
        group,
        primary,
        seen: &seen,
        committed: &committed,
    };

    failover(
        force.config,
        force.raft,
        force.machine,
        force.switches,
        failing,
        true,
    )
    .await
}

/// Makes every answering member agree with the record: the primary takes writes, the others
/// follow it.
async fn reconcile(config: &CoordinatorConfig, group: &Group, primary: Primary, seen: &[Sighting]) {
    let target = &group.members[primary.member];

    for (index, member) in group.members.iter().enumerate() {
        let sighting = &seen[index];

        if sighting.loading
            || sighting
                .last_ok
                .is_none_or(|at| at.elapsed() > config.down_after)
        {
            continue;
        }

        let result = if index == primary.member {
            if sighting.role == Some(Role::Master) {
                continue;
            }
            command(
                member,
                config.auth.as_deref(),
                &[b"REPLICAOF", b"NO", b"ONE"],
            )
            .await
        } else {
            if sighting.role == Some(Role::Replica(Some(target.replication.to_string()))) {
                continue;
            }
            let host = target.replication.host.clone();
            let port = target.replication.port.to_string();

            command(
                member,
                config.auth.as_deref(),
                &[b"REPLICAOF", host.as_bytes(), port.as_bytes()],
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
            member: 0,
            epoch: 1,
        };
        let mut seen = vec![
            member(Role::Master, &[50, 40], &[20, 0]),
            member(following(&group, 0), &[45, 40], &[20, 30]),
            // Follows another address: its value is not this primary's.
            member(following(&group, 2), &[0, 0], &[99, 99]),
        ];

        assert_eq!(commits.learn(&group, primary, &seen, fresh), vec![20, 30]);
        // A replica's value above what the answering primary holds is left from an earlier
        // primary: it counts only up to the primary's sequence.
        seen[1].shard_committed = vec![70, 30];
        assert_eq!(commits.learn(&group, primary, &seen, fresh), vec![50, 30]);
        // The primary is down: replicas' values count as told, and the highest one stays.
        seen[0].last_ok = None;
        seen[1].shard_committed = vec![55, 10];
        assert_eq!(commits.learn(&group, primary, &seen, fresh), vec![55, 30]);
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
        assert!(commits.learn(&other, primary, &quiet, fresh).is_empty());
    }

    #[test]
    fn a_clean_failover_keeps_the_committed_records_and_an_unclean_one_drops_them() {
        let group = group(3);
        let fresh = Duration::from_secs(5);
        let commits = Commits::default();
        let first = Primary {
            member: 0,
            epoch: 1,
        };
        let mut seen = vec![
            member(Role::Master, &[50], &[40]),
            member(following(&group, 0), &[45], &[40]),
            member(following(&group, 0), &[30], &[40]),
        ];

        assert_eq!(commits.learn(&group, first, &seen, fresh), vec![40]);

        // Member 1 held every committed record and was promoted: they stay committed, so
        // member 2 must catch up before it can take over.
        let second = Primary {
            member: 1,
            epoch: 2,
        };
        seen[0] = Sighting::default();
        seen[1] = member(Role::Master, &[45], &[0]);
        seen[2] = member(following(&group, 1), &[30], &[40]);
        assert_eq!(commits.learn(&group, second, &seen, fresh), vec![40]);
        assert_eq!(choose(&group, 1, &seen, fresh, &[40], false), Choice::Wait);

        // Forced onto member 2, which lacked them: those records are lost and its own
        // sequences reuse their numbers. Member 1 keeps reporting 40 (a replica keeps the
        // highest value it was told), which must not block the next failover.
        let third = Primary {
            member: 2,
            epoch: 3,
        };
        seen[1] = member(following(&group, 2), &[30], &[40]);
        seen[2] = member(Role::Master, &[32], &[0]);
        assert_eq!(commits.learn(&group, third, &seen, fresh), vec![0]);
        seen[2].last_ok = None;
        assert_eq!(commits.learn(&group, third, &seen, fresh), vec![0]);
        // A durable write of the new primary above the old value counts again.
        seen[1].shard_committed = vec![41];
        assert_eq!(commits.learn(&group, third, &seen, fresh), vec![41]);
    }
}
