//! Moving a slot range from one group to another. The leader drives each move with
//! `RUVIO MIGRATE` on the two primaries: the target shard expects the range, the source
//! copies it there and freezes it, and once the source reports the copy prepared the
//! coordinators commit the move. The next cluster map gives the range to the target; it
//! goes to the target's primary first, then the source's, then everyone else.
//!
//! The move is in the Raft state from its start, so a new leader takes it over: every step
//! can be sent again. A failover of either group before the commit cancels the move and
//! starts it again with the new primaries. After the commit nothing undoes it.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::{info, warn};

use super::{
    config::{CoordinatorConfig, Endpoint},
    raft::{Migration, Raft, Request, StateMachine},
    resp::{NodeClient, Value},
};

/// How often the leader asks the source how the copy is going.
const POLL: Duration = Duration::from_millis(50);
/// The longest one call to a data node may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// Starts of one move (the first and the ones a failover forces).
const ATTEMPTS: usize = 3;
/// Polls the leader waits for the target's replicas to hold a prepared copy (10 s).
const REPLICA_WAIT_POLLS: usize = 200;

/// The leader's side of the moves: which one it drives, a cancel asked for, and how the
/// last one ended.
#[derive(Default)]
pub struct Mover {
    inner: Mutex<MoverState>,
}

#[derive(Default)]
struct MoverState {
    driving: Option<String>,
    cancel: Option<String>,
    last: Vec<(String, String)>,
}

impl Mover {
    fn lock(&self) -> std::sync::MutexGuard<'_, MoverState> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// What a move needs from the coordinator.
#[derive(Clone)]
pub struct Context {
    pub config: Arc<CoordinatorConfig>,
    pub raft: Raft,
    pub machine: StateMachine,
    pub mover: Arc<Mover>,
}

impl Context {
    async fn leading(&self) -> bool {
        self.raft.current_leader().await == Some(self.config.id)
    }

    async fn not_leader(&self) -> String {
        match self.raft.current_leader().await {
            Some(leader) => format!("ERR not the leader; send it to coordinator {leader}"),
            None => "ERR no leader yet; try again".into(),
        }
    }
}

/// `COORDINATOR MOVE first last group shard`: starts moving `first..=last` to shard `shard`
/// of `group`; the move's id.
pub async fn start(
    context: &Context,
    first: u16,
    last: u16,
    to: &str,
    shard: usize,
) -> Result<String, String> {
    if !context.leading().await {
        return Err(context.not_leader().await);
    }
    let config = &context.config;
    let target = config
        .group(to)
        .ok_or_else(|| format!("ERR no group named {to}"))?;
    let shards = target
        .shards
        .ok_or("ERR moves need a multi-node cluster (groups with shards)")?;

    if first > last || usize::from(last) >= crate::routing::HASH_SLOT_COUNT {
        return Err("ERR slot range is not valid".into());
    }
    if shard >= shards {
        return Err(format!("ERR group {to} has {shards} shards"));
    }
    let moves = context.machine.moves();

    if moves.current.is_some() {
        return Err("ERR another move is in progress".into());
    }
    let owners = config.owners(&moves.moved);
    let owner = owners[usize::from(first)];

    if owners[usize::from(first)..=usize::from(last)]
        .iter()
        .any(|known| *known != owner)
    {
        return Err("ERR slot range is on more than one shard; move each part".into());
    }
    let source = &config.groups[owner.0];

    if source.name == target.name {
        return Err("ERR slot range is already in that group".into());
    }
    let primaries = context.machine.primaries();
    let (Some(from_primary), Some(to_primary)) =
        (primaries.get(&source.name), primaries.get(&target.name))
    else {
        return Err("ERR both groups need an agreed primary first".into());
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let migration = Migration {
        id: format!("m{millis}-{first}-{last}"),
        first,
        last,
        from: source.name.clone(),
        to: target.name.clone(),
        shard,
        from_epoch: from_primary.epoch,
        to_epoch: to_primary.epoch,
    };

    if !propose(context, Request::BeginMove(migration.clone())).await? {
        return Err("ERR another move is in progress".into());
    }
    info!(id = %migration.id, first, last, from = %migration.from, to = %migration.to, shard, "moving a slot range between groups");
    resume(context);
    Ok(migration.id)
}

/// `COORDINATOR CANCEL`: ends the running move unless it is committed already.
pub async fn cancel(context: &Context) -> Result<(), String> {
    if !context.leading().await {
        return Err(context.not_leader().await);
    }
    let Some(current) = context.machine.moves().current else {
        return Err("ERR no move in progress".into());
    };

    context.mover.lock().cancel = Some(current.id);
    Ok(())
}

/// `COORDINATOR STATUS`: the move in progress (or the last one this leader drove) and how
/// many moves the map holds, as field-value pairs.
pub fn status(context: &Context) -> Value {
    let moves = context.machine.moves();
    let mut pairs: Vec<(String, String)> = match &moves.current {
        Some(current) => vec![
            ("state".into(), "moving".into()),
            ("id".into(), current.id.clone()),
            ("first".into(), current.first.to_string()),
            ("last".into(), current.last.to_string()),
            ("from".into(), current.from.clone()),
            ("to".into(), current.to.clone()),
            ("shard".into(), current.shard.to_string()),
        ],
        None => {
            let last = context.mover.lock().last.clone();

            if last.is_empty() {
                vec![("state".into(), "none".into())]
            } else {
                last
            }
        }
    };

    pairs.push(("moved_ranges".into(), moves.moved.len().to_string()));
    Value::array(
        pairs
            .into_iter()
            .flat_map(|(name, value)| [Value::bulk(name), Value::bulk(value)])
            .collect(),
    )
}

/// Drives the move in the Raft state unless this coordinator already does: after `start`,
/// and on every leader tick so a new leader takes over a move its predecessor began.
pub fn resume(context: &Context) {
    let Some(current) = context.machine.moves().current else {
        return;
    };
    {
        let mut mover = context.mover.lock();

        if mover.driving.as_deref() == Some(current.id.as_str()) {
            return;
        }
        mover.driving = Some(current.id.clone());
    }
    let context = context.clone();

    tokio::spawn(async move {
        drive(&context, current).await;
        context.mover.lock().driving = None;
    });
}

enum Ended {
    Committed,
    /// Ended before the commit; a failover asks for another start.
    Aborted {
        reason: String,
        again: bool,
    },
    /// This coordinator is no longer the leader, or the move left the Raft state.
    Handed,
}

async fn drive(context: &Context, mut migration: Migration) {
    let base = migration.id.clone();

    for attempt in 1..=ATTEMPTS {
        let outcome = attempt_move(context, &migration).await;

        match outcome {
            Ended::Committed => {
                info!(id = %migration.id, "slot range moved; sending the new map");
                send_map(context, &[&migration.to, &migration.from]).await;
                report(context, &migration, "done", None);
                return;
            }
            Ended::Handed => return,
            Ended::Aborted { reason, again } => {
                warn!(id = %migration.id, %reason, "the slot range move ended before its commit");
                // Ended in the Raft state first: once that holds, no leader can commit the
                // move, so cancelling it on the nodes cannot undo a committed move.
                if propose(
                    context,
                    Request::EndMove {
                        id: migration.id.clone(),
                    },
                )
                .await
                    != Ok(true)
                {
                    return;
                }
                cancel_on_nodes(context, &migration).await;
                if !again || attempt == ATTEMPTS {
                    report(context, &migration, "failed", Some(reason));
                    return;
                }
                // Started again with the groups' new primaries.
                let primaries = context.machine.primaries();
                let (Some(from), Some(to)) =
                    (primaries.get(&migration.from), primaries.get(&migration.to))
                else {
                    report(context, &migration, "failed", Some(reason));
                    return;
                };

                migration.id = format!("{base}-{}", attempt + 1);
                migration.from_epoch = from.epoch;
                migration.to_epoch = to.epoch;
                match propose(context, Request::BeginMove(migration.clone())).await {
                    Ok(true) => {
                        context.mover.lock().driving = Some(migration.id.clone());
                    }
                    _ => return,
                }
            }
        }
    }
}

async fn attempt_move(context: &Context, migration: &Migration) -> Ended {
    let config = &context.config;
    let id = migration.id.as_bytes();
    let (first, last) = (migration.first.to_string(), migration.last.to_string());
    let shard = migration.shard.to_string();

    if let Some(ended) = interrupted(context, migration).await {
        return ended;
    }
    let (Some(source), Some(target)) = (
        primary_address(context, &migration.from, 0),
        primary_address(context, &migration.to, migration.shard),
    ) else {
        return Ended::Handed;
    };
    let auth: Vec<&[u8]> = config
        .auth
        .as_deref()
        .map(|auth| auth.split_whitespace().map(str::as_bytes).collect())
        .unwrap_or_default();

    // The current map first: a range a node still holds frozen from a committed move is
    // given away before anything else happens there.
    send_map(context, &[&migration.to, &migration.from]).await;
    let import: [&[u8]; 7] = [
        b"RUVIO",
        b"MIGRATE",
        b"IMPORT",
        id,
        first.as_bytes(),
        last.as_bytes(),
        shard.as_bytes(),
    ];
    let mut tries = 0;

    // A move cancelled just before may still be dropping its copy on the target.
    while let Err(error) = expect_ok(call(config, &target, &import).await) {
        tries += 1;
        if !error.contains("another range is arriving") || tries >= 200 {
            return Ended::Aborted {
                reason: format!("the target refused the range: {error}"),
                again: false,
            };
        }
        tokio::time::sleep(POLL).await;
        if let Some(ended) = interrupted(context, migration).await {
            return ended;
        }
    }
    let port = target.port.to_string();
    let mut export: Vec<&[u8]> = vec![
        b"RUVIO",
        b"MIGRATE",
        b"EXPORT",
        id,
        first.as_bytes(),
        last.as_bytes(),
        target.host.as_bytes(),
        port.as_bytes(),
    ];

    if !auth.is_empty() {
        export.push(b"AUTH");
        export.extend(&auth);
    }
    let mut tries = 0;

    // A move the source missed the end of is cancelled there first.
    while let Err(error) = expect_ok(call(config, &source, &export).await) {
        tries += 1;
        if !error.contains("another range is leaving") || tries >= 200 {
            return Ended::Aborted {
                reason: format!("the source refused the range: {error}"),
                again: false,
            };
        }
        tokio::time::sleep(POLL).await;
        if let Some(ended) = interrupted(context, migration).await {
            return ended;
        }
    }

    // The source reports `prepared` once the target holds the whole range and the source
    // has kept the frozen range on disk; only then may the move commit.
    loop {
        tokio::time::sleep(POLL).await;
        if let Some(ended) = interrupted(context, migration).await {
            return ended;
        }
        let Ok(reply) = call(config, &source, &[b"RUVIO", b"MIGRATE", b"STATUS"]).await else {
            // The source may be restarting; a failover ends the move above.
            continue;
        };
        let pairs = pairs(&reply);
        let field = |name: &str| {
            pairs
                .iter()
                .find(|(known, _)| known == name)
                .map(|(_, value)| value.as_str())
        };

        if field("id") != Some(migration.id.as_str()) {
            return Ended::Aborted {
                reason: "the source no longer knows the move".into(),
                again: false,
            };
        }
        match field("state") {
            Some("prepared") => break,
            Some("copying") => {}
            Some(state) => {
                return Ended::Aborted {
                    reason: format!(
                        "the source reports {state}: {}",
                        field("error").unwrap_or("no reason given")
                    ),
                    again: false,
                };
            }
            None => {}
        }
    }

    // The target's replicas must hold the copy too: a failover of the target right after
    // the commit would otherwise promote a replica without the range.
    let mut waited = 0;

    loop {
        if let Some(ended) = interrupted(context, migration).await {
            return ended;
        }
        let behind = call(config, &target, &[b"RUVIO", b"MIGRATE", b"STATUS"])
            .await
            .ok()
            .map(|reply| pairs(&reply))
            .and_then(|pairs| {
                let field = |name: &str| {
                    pairs
                        .iter()
                        .find(|(known, _)| known == name)
                        .map(|(_, value)| value.clone())
                };

                (field("id").as_deref() == Some(migration.id.as_str())
                    && field("state").as_deref() == Some("prepared"))
                .then(|| field("replicas_behind"))
                .flatten()
            });

        if behind.as_deref() == Some("0") {
            break;
        }
        waited += 1;
        if waited >= REPLICA_WAIT_POLLS {
            return Ended::Aborted {
                reason: format!(
                    "the target's replicas did not get the copy in time ({} behind)",
                    behind.as_deref().unwrap_or("unknown")
                ),
                again: true,
            };
        }
        tokio::time::sleep(POLL).await;
    }
    match propose(
        context,
        Request::CommitMove {
            id: migration.id.clone(),
        },
    )
    .await
    {
        Ok(true) => Ended::Committed,
        _ => Ended::Handed,
    }
}

/// Whether the move must stop before its next step: leadership lost, the move left the
/// Raft state, a cancel was asked for, or either group failed over.
async fn interrupted(context: &Context, migration: &Migration) -> Option<Ended> {
    if !context.leading().await
        || context
            .machine
            .moves()
            .current
            .is_none_or(|current| current.id != migration.id)
    {
        return Some(Ended::Handed);
    }
    if context
        .mover
        .lock()
        .cancel
        .take_if(|id| *id == migration.id)
        .is_some()
    {
        return Some(Ended::Aborted {
            reason: "cancelled".into(),
            again: false,
        });
    }
    let primaries = context.machine.primaries();
    let failed_over = primaries
        .get(&migration.from)
        .is_none_or(|primary| primary.epoch != migration.from_epoch)
        || primaries
            .get(&migration.to)
            .is_none_or(|primary| primary.epoch != migration.to_epoch);

    failed_over.then(|| Ended::Aborted {
        reason: "a primary failed over".into(),
        again: true,
    })
}

/// The agreed primary's client address of `group`, `shard` ports up.
fn primary_address(context: &Context, group: &str, shard: usize) -> Option<Endpoint> {
    let primary = context.machine.primaries().get(group)?.member;
    let group = context.config.group(group)?;

    Some(group.members.get(primary)?.address.plus(shard as u16))
}

/// Ends the move on both primaries (and on the members that were primaries when it began,
/// in case they still answer). A node that does not know the id answers `OK` anyway.
async fn cancel_on_nodes(context: &Context, migration: &Migration) {
    let id = migration.id.as_bytes();
    let mut targets: Vec<Endpoint> = Vec::new();

    for (group, shard) in [(&migration.from, 0), (&migration.to, migration.shard)] {
        if let Some(group) = context.config.group(group) {
            targets.extend(
                group
                    .members
                    .iter()
                    .map(|member| member.address.plus(shard as u16)),
            );
        }
    }
    for target in targets {
        if !context.leading().await {
            return;
        }
        if let Err(error) = call(
            &context.config,
            &target,
            &[b"RUVIO", b"MIGRATE", b"CANCEL", id],
        )
        .await
        {
            tracing::debug!(%target, %error, "could not cancel a move on a member");
        }
    }
}

/// Sends the current map to the primaries of `groups`, in that order; the leader's loop
/// brings every other member up to date.
async fn send_map(context: &Context, groups: &[&String]) {
    let Some((version, map)) = context
        .config
        .cluster_map(&context.machine.primaries(), &context.machine.moves().moved)
    else {
        return;
    };

    for group in groups {
        let Some(primary) = primary_address(context, group, 0) else {
            continue;
        };

        match expect_ok(
            call(
                &context.config,
                &primary,
                &[b"RUVIO", b"TOPOLOGY", b"SET", map.as_bytes()],
            )
            .await,
        ) {
            Ok(()) => info!(%group, version, "sent the cluster map with the moved range"),
            Err(error) => {
                warn!(%group, %error, "could not send the cluster map; the leader's loop sends it again")
            }
        }
    }
}

fn report(context: &Context, migration: &Migration, state: &str, error: Option<String>) {
    let mut pairs = vec![
        ("state".to_string(), state.to_string()),
        ("id".into(), migration.id.clone()),
        ("first".into(), migration.first.to_string()),
        ("last".into(), migration.last.to_string()),
        ("from".into(), migration.from.clone()),
        ("to".into(), migration.to.clone()),
        ("shard".into(), migration.shard.to_string()),
    ];

    if let Some(error) = error {
        pairs.push(("error".into(), error));
    }
    context.mover.lock().last = pairs;
}

async fn propose(context: &Context, request: Request) -> Result<bool, String> {
    context
        .raft
        .client_write(request)
        .await
        .map(|reply| reply.data)
        .map_err(|error| {
            warn!(%error, "could not record a slot range move");
            format!("ERR could not record the move: {error}")
        })
}

async fn call(
    config: &CoordinatorConfig,
    address: &Endpoint,
    args: &[&[u8]],
) -> Result<Value, String> {
    let mut client = NodeClient::connect(address, config.auth.as_deref(), CALL_TIMEOUT)
        .await
        .map_err(|error| format!("{address}: {error}"))?;

    client
        .call(args, CALL_TIMEOUT)
        .await
        .map_err(|error| format!("{address}: {error}"))
}

fn expect_ok(reply: Result<Value, String>) -> Result<(), String> {
    match reply? {
        Value::Simple(_) => Ok(()),
        Value::Error(error) => Err(error),
        other => Err(format!("unexpected reply {other:?}")),
    }
}

/// A flat field-value array as pairs of text.
fn pairs(reply: &Value) -> Vec<(String, String)> {
    let Value::Array(Some(items)) = reply else {
        return Vec::new();
    };

    items
        .chunks_exact(2)
        .filter_map(|pair| Some((pair[0].text()?, pair[1].text()?)))
        .collect()
}
