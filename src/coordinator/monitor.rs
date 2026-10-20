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
        let durable = info
            .and_then(|info| info.text())
            .and_then(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("wal_durable_sequence:"))
                    .and_then(|value| value.trim().parse::<u64>().ok())
            })
            .unwrap_or(0);

        sighting.durable += durable;
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

/// The member to promote: answering, a replica with a complete copy, holding the most of the
/// history; ties go to the lowest index.
pub fn choose_candidate(
    group: &Group,
    primary: usize,
    sightings: &[Sighting],
    fresh: Duration,
) -> Option<usize> {
    (0..group.members.len())
        .filter(|&member| member != primary)
        .filter(|&member| {
            let sighting = &sightings[member];

            !sighting.loading
                && sighting.last_ok.is_some_and(|at| at.elapsed() <= fresh)
                && matches!(sighting.role, Some(Role::Replica(_)))
        })
        .max_by(|&left, &right| {
            sightings[left]
                .durable
                .cmp(&sightings[right].durable)
                .then(right.cmp(&left))
        })
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
    switches: Switches,
    moves: super::migrate::Context,
) {
    let mut tick = tokio::time::interval(config.ping_interval);

    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if raft.current_leader().await != Some(config.id) {
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
            let primary_age = seen[primary.member].last_ok.map(|at| at.elapsed());

            if primary_age.is_none_or(|age| age > config.down_after)
                && objectively_down(&config, &sightings, group, primary.member).await
            {
                failover(&config, &raft, &machine, group, primary, &seen, &switches).await;
                continue;
            }

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

async fn failover(
    config: &CoordinatorConfig,
    raft: &Raft,
    machine: &StateMachine,
    group: &Group,
    primary: Primary,
    seen: &[Sighting],
    switches: &Switches,
) {
    let Some(candidate) = choose_candidate(group, primary.member, seen, config.down_after) else {
        warn!(group = %group.name, "primary is down and no replica can take over");
        return;
    };

    warn!(
        group = %group.name,
        from = %group.members[primary.member].address,
        to = %group.members[candidate].address,
        "primary is down for a majority of coordinators; failing over"
    );
    if !propose(raft, group, candidate, primary.epoch + 1).await {
        return;
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
        Sighting {
            last_ok: Some(Instant::now()),
            role: Some(Role::Replica(None)),
            durable,
            loading: false,
            topology: None,
        }
    }

    #[test]
    fn the_candidate_is_the_answering_complete_replica_with_the_most_history() {
        let group = group(4);
        let fresh = Duration::from_secs(5);
        let mut seen = vec![Sighting::default(), replica(10), replica(30), replica(30)];

        // Equal history: the lower index wins.
        assert_eq!(choose_candidate(&group, 0, &seen, fresh), Some(2));
        seen[2].loading = true;
        assert_eq!(choose_candidate(&group, 0, &seen, fresh), Some(3));
        seen[3].last_ok = None;
        assert_eq!(choose_candidate(&group, 0, &seen, fresh), Some(1));
        seen[1].role = Some(Role::Master);
        assert_eq!(choose_candidate(&group, 0, &seen, fresh), None);
    }
}
