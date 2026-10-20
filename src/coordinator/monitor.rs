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
        Commit, NodeId, Primary, Raft, Raised, Recorded, Request, RpcRequest, RpcResponse,
        StateMachine, raise,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    /// Following the primary whose replication address (`host:port`) is this.
    Replica(Option<String>),
}

/// Every member's latest sighting, by group and member index.
#[derive(Clone, Default)]
pub struct Sightings(Arc<Mutex<HashMap<(String, usize), Sighting>>>);

impl Sightings {
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
    fn learn(&mut self, group: &Group, primary: Primary, seen: &[Sighting], fresh: Duration) {
        // Only under the primary the values belong to: after a failover the Raft record
        // brings the values the promotion kept (see `Commit::after_promotion`).
        if self.epoch != primary.epoch {
            return;
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
    /// Updates what is known committed in `group` from the Raft record and this tick's
    /// sightings, and returns it.
    pub fn learn(
        &self,
        group: &Group,
        primary: Primary,
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

/// Asks every other coordinator how long ago it heard from `primary`; true when a majority
/// (this one included) has not heard from it within `down_after`.
async fn objectively_down(
    config: &CoordinatorConfig,
    sightings: &Sightings,
    group: &Group,
    primary: usize,
) -> bool {
    let down_after = config.down_after.as_millis() as u64;
    let local = sightings.ages(group)[primary];
    let mut down = usize::from(local.is_none_or(|age| age > down_after));

    for (id, address) in &config.peers {
        if *id == config.id {
            continue;
        }
        let request = RpcRequest::Observe {
            group: group.name.clone(),
        };

        if let Ok(RpcResponse::Observe(ages)) = super::raft::call(
            &address.to_string(),
            config.secret.as_deref().unwrap_or(""),
            request,
            CALL_TIMEOUT,
        )
        .await
            && ages
                .get(primary)
                .copied()
                .flatten()
                .is_none_or(|age| age > down_after)
        {
            down += 1;
        }
    }

    down >= config.quorum()
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
    let mut recorded_at: Option<Instant> = None;

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        // A new leader acts only once it has applied what earlier leaders recorded: until
        // then its primaries and committed sequences may be those of an old snapshot.
        if raft.current_leader().await != Some(config.id) || !caught_up(&raft) {
            // Every coordinator learns what was committed, so a new leader also knows what
            // rose after the last record.
            let primaries = machine.primaries();
            let recorded = machine.commits();

            for group in &config.groups {
                if let Some(primary) = primaries.get(&group.name) {
                    let seen: Vec<Sighting> = (0..group.members.len())
                        .map(|member| sightings.get(&group.name, member))
                        .collect();

                    commits.learn(group, *primary, &seen, config.down_after, &recorded);
                }
            }
            continue;
        }

        let recorded = machine.commits();

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
                        propose(&raft, group, *only, 1, None).await;
                    }
                    [] => {}
                    _ => {
                        warn!(group = %group.name, "several members answer as primary; waiting for one")
                    }
                }
                continue;
            };
            let committed = commits.learn(group, primary, &seen, config.down_after, &recorded);
            let primary_age = seen[primary.member].last_ok.map(|at| at.elapsed());

            if primary_age.is_none_or(|age| age > config.down_after)
                && objectively_down(&config, &sightings, group, primary.member).await
            {
                let failing = Failing {
                    group,
                    primary,
                    seen: &seen,
                    committed: &committed,
                    commits: &commits,
                };
                let _ = failover(&config, &raft, &machine, &switches, failing, false).await;
                continue;
            }

            reconcile(&config, group, primary, &seen).await;
        }

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
    member: usize,
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

/// A group whose primary is down, as the leader sees it.
struct Failing<'a> {
    group: &'a Group,
    primary: Primary,
    seen: &'a [Sighting],
    committed: &'a [u64],
    commits: &'a Commits,
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
        commits,
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
    // Decided once, here, from the promoted member as the leader sees it now, and recorded with
    // the promotion: every coordinator takes the same values from the record.
    let kept = commits.after_promotion(
        &group.name,
        &seen[candidate].shard_durable,
        primary.epoch + 1,
    );

    if !propose(raft, group, candidate, primary.epoch + 1, Some(kept)).await {
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
    if !caught_up(force.raft) {
        return Err("ERR the leader is still applying the log; try again".into());
    }
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
    let committed = force.commits.learn(
        group,
        primary,
        &seen,
        force.config.down_after,
        &force.machine.commits(),
    );
    let failing = Failing {
        group,
        primary,
        seen: &seen,
        committed: &committed,
        commits: force.commits,
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

        assert_eq!(
            commits.learn(&group, primary, &seen, fresh, &Recorded::new()),
            vec![20, 30]
        );
        // A replica's value above what the answering primary holds is left from an earlier
        // primary: it counts only up to the primary's sequence.
        seen[1].shard_committed = vec![70, 30];
        assert_eq!(
            commits.learn(&group, primary, &seen, fresh, &Recorded::new()),
            vec![50, 30]
        );
        // The primary is down: replicas' values count as told, and the highest one stays.
        seen[0].last_ok = None;
        seen[1].shard_committed = vec![55, 10];
        assert_eq!(
            commits.learn(&group, primary, &seen, fresh, &Recorded::new()),
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
                .learn(&other, primary, &quiet, fresh, &Recorded::new())
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
                member,
                epoch,
                commit,
            });
            self.primaries["main"]
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
            commits.learn(&group, first, &seen, fresh, &agreed.recorded),
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
            commits.learn(&group, second, &seen, fresh, &agreed.recorded),
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
            commits.learn(&group, third, &seen, fresh, &agreed.recorded),
            vec![0]
        );
        seen[2].last_ok = None;
        assert_eq!(
            commits.learn(&group, third, &seen, fresh, &agreed.recorded),
            vec![0]
        );
        // A durable write of the new primary above the old value counts again.
        seen[1].shard_committed = vec![41];
        assert_eq!(
            commits.learn(&group, third, &seen, fresh, &agreed.recorded),
            vec![41]
        );
        // A primary record of an older epoch read late changes nothing.
        seen[1].shard_committed = vec![99];
        assert_eq!(
            commits.learn(&group, second, &seen, fresh, &agreed.recorded),
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

        leader.learn(&group, first, &seen, fresh, &agreed.recorded);
        agreed.record(&leader);
        // A clean promotion of member 1, then every coordinator restarts and has not seen
        // member 1 yet: its values stay, they are not taken for an unclean promotion's.
        let clean = agreed.promote(1, 2, Some(leader.after_promotion("main", &[45], 2)));
        let unseen = vec![Sighting::default(), Sighting::default(), seen[2].clone()];
        let restarted = Commits::default();

        assert_eq!(
            restarted.learn(&group, clean, &unseen, fresh, &agreed.recorded),
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
            restarted.learn(&group, unclean, &reporting, fresh, &agreed.recorded),
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
            old_leader.learn(&group, primary, &seen, fresh, &agreed.recorded),
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
        old_leader.learn(&group, primary, &seen, fresh, &agreed.recorded);
        assert!(old_leader.raised(&agreed.recorded).is_empty());

        // The old leader and the primary are gone, and so is member 1; member 2 never held
        // record 40 and reports nothing committed. A coordinator that knows only what members
        // report would promote it; one that starts from the record waits.
        seen[0] = Sighting::default();
        seen[1] = Sighting::default();
        let new_leader = Commits::default();

        assert!(
            new_leader
                .learn(&group, primary, &seen, fresh, &Recorded::new())
                .is_empty()
        );
        let new_leader = Commits::default();
        assert_eq!(
            new_leader.learn(&group, primary, &seen, fresh, &agreed.recorded),
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
        quiet.learn(&group, primary, &idle, fresh, &Recorded::new());
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
}
