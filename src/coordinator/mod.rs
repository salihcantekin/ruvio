//! `ruvio coordinator`: a separate process that watches replication groups and fails them over
//! automatically. Three or five of them agree through Raft on which member of each group is
//! primary; the leader runs the failover with `REPLICAOF`, and every coordinator answers the
//! part of the Redis Sentinel API clients use to find the primary. Data nodes need nothing
//! beyond what they already serve (`ROLE`, `INFO`, `CONFIG GET`, `REPLICAOF`).

pub mod config;
mod migrate;
mod monitor;
mod raft;
mod resp;

use std::{collections::BTreeMap, io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

pub use config::CoordinatorConfig;
use monitor::{Commits, Sightings, Switches};
use raft::{Network, Raft, RpcRequest, RpcResponse, StateMachine};
use resp::Value;

/// Runs the coordinator until `shutdown` is requested.
pub fn run(config: CoordinatorConfig, shutdown: crate::shutdown::ShutdownToken) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("ruvio-coordinator")
        .enable_all()
        .build()?;

    runtime.block_on(serve(Arc::new(config), shutdown))
}

async fn serve(
    config: Arc<CoordinatorConfig>,
    shutdown: crate::shutdown::ShutdownToken,
) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen).await?;
    let raft_config = Arc::new(
        openraft::Config {
            cluster_name: "ruvio-coordinator".into(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            // The log is rewritten and synced whole on every append, and grows by up to one
            // committed sequence record a second: keep it short with frequent small snapshots
            // (the state is a few kilobytes).
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100),
            max_in_snapshot_log_to_keep: 20,
            ..openraft::Config::default()
        }
        .validate()
        .map_err(io::Error::other)?,
    );
    let log = raft::LogStore::open(&config.data_dir)?;
    let machine = StateMachine::open(&config.data_dir)?;
    let network = Network {
        secret: config.secret.clone().unwrap_or_default().into(),
    };
    let raft = Raft::new(config.id, raft_config, network, log, machine.clone())
        .await
        .map_err(io::Error::other)?;

    // Every coordinator proposes the same membership on its first start; once one is
    // committed the others' attempts are refused, which is expected.
    if !raft.is_initialized().await.map_err(io::Error::other)? {
        let members: BTreeMap<u64, openraft::BasicNode> = config
            .peers
            .iter()
            .map(|(id, address)| (*id, openraft::BasicNode::new(address.to_string())))
            .collect();

        if let Err(error) = raft.initialize(members).await {
            info!(%error, "membership already set by another coordinator");
        }
    }

    let sightings = Sightings::default();
    let commits = Commits::default();
    let (switches, _) = tokio::sync::broadcast::channel(64);

    info!(id = config.id, listen = %config.listen, groups = config.groups.len(), "coordinator listening");
    let watcher = tokio::spawn(monitor::watch(Arc::clone(&config), sightings.clone()));
    let moves = migrate::Context {
        config: Arc::clone(&config),
        raft: raft.clone(),
        machine: machine.clone(),
        mover: Arc::default(),
        leadership: Arc::new(raft::Leadership::new(config.id, config.ping_interval)),
    };
    let leader = tokio::spawn(monitor::lead(
        Arc::clone(&config),
        raft.clone(),
        machine.clone(),
        sightings.clone(),
        commits.clone(),
        switches.clone(),
        moves.clone(),
    ));
    let api = Api {
        config: Arc::clone(&config),
        raft: raft.clone(),
        machine,
        sightings,
        commits,
        switches,
        moves,
    };

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                let api = api.clone();

                tokio::spawn(async move {
                    if let Err(error) = api.connection(stream).await
                        && error.kind() != io::ErrorKind::UnexpectedEof
                    {
                        tracing::debug!(%error, "coordinator connection closed");
                    }
                });
            }
            () = wait_for(&shutdown) => break,
        }
    }

    watcher.abort();
    leader.abort();
    let _ = raft.shutdown().await;
    Ok(())
}

async fn wait_for(shutdown: &crate::shutdown::ShutdownToken) {
    while !shutdown.is_requested() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Clone)]
struct Api {
    config: Arc<CoordinatorConfig>,
    raft: Raft,
    machine: StateMachine,
    sightings: Sightings,
    commits: Commits,
    switches: Switches,
    moves: migrate::Context,
}

impl Api {
    async fn connection(&self, mut stream: TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        let mut first = [0; 1];

        if stream.peek(&mut first).await? == 0 {
            return Ok(());
        }
        if first[0] == raft::RPC_MAGIC {
            stream.readable().await?;
            let mut magic = [0; 1];

            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut magic).await?;
            let envelope: raft::Envelope = raft::read_frame(&mut stream).await?;
            let secret = self.config.secret.as_deref().unwrap_or("");

            if !crate::security::constant_time_eq(envelope.secret.as_bytes(), secret.as_bytes()) {
                warn!("a coordinator call with a wrong secret was refused");
                return Ok(());
            }
            let response = self.rpc(envelope.request).await;

            return raft::write_frame(&mut stream, &response).await;
        }

        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut authenticated = self.config.secret.is_none();

        while let Some(args) = resp::read_command(&mut reader).await? {
            let words: Vec<String> = args
                .iter()
                .map(|arg| String::from_utf8_lossy(arg).to_ascii_lowercase())
                .collect();

            if words.first().map(String::as_str) == Some("auth") {
                // `AUTH secret` or `AUTH user secret`: only the secret is checked.
                let given = args.last().map(Vec::as_slice).unwrap_or_default();
                let secret = self.config.secret.as_deref().unwrap_or("");
                let reply = if args.len() >= 2
                    && crate::security::constant_time_eq(given, secret.as_bytes())
                {
                    authenticated = true;
                    Value::Simple("OK".into())
                } else {
                    Value::Error("WRONGPASS invalid secret".into())
                };
                let mut out = Vec::new();

                reply.encode(&mut out);
                write.write_all(&out).await?;
                continue;
            }
            if !authenticated {
                write
                    .write_all(b"-NOAUTH Authentication required.\r\n")
                    .await?;
                continue;
            }

            if matches!(
                words.first().map(String::as_str),
                Some("subscribe" | "psubscribe")
            ) {
                return self.subscribe(&words, &mut write).await;
            }
            let mut out = Vec::new();
            let reply = if words.first().map(String::as_str) == Some("coordinator") {
                self.coordinator(&words).await
            } else {
                self.command(&words, &args)
            };

            reply.encode(&mut out);
            write.write_all(&out).await?;
        }

        Ok(())
    }

    async fn rpc(&self, request: RpcRequest) -> RpcResponse {
        match request {
            RpcRequest::Append(rpc) => RpcResponse::Append(self.raft.append_entries(rpc).await),
            RpcRequest::Vote(rpc) => RpcResponse::Vote(self.raft.vote(rpc).await),
            RpcRequest::Snapshot(rpc) => {
                RpcResponse::Snapshot(self.raft.install_snapshot(rpc).await)
            }
            RpcRequest::Observe { group } => RpcResponse::Observe(
                self.config
                    .group(&group)
                    .map(|group| self.sightings.ages(group))
                    .unwrap_or_default(),
            ),
        }
    }

    /// `COORDINATOR MOVE first last group shard`, `COORDINATOR STATUS`, `COORDINATOR CANCEL`:
    /// moving slot ranges between groups. `COORDINATOR FAILOVER group FORCE`: promoting a
    /// replica that lacks committed records.
    async fn coordinator(&self, words: &[String]) -> Value {
        let word = |index: usize| words.get(index).map(String::as_str);

        match (word(1), words.len()) {
            (Some("failover"), 4) if word(3) == Some("force") => {
                if !self.moves.leading().await {
                    return Value::Error(self.moves.not_leader().await);
                }
                let Some(group) = self
                    .config
                    .groups
                    .iter()
                    .find(|group| group.name.eq_ignore_ascii_case(&words[2]))
                else {
                    return Value::Error(format!("ERR no group named {}", words[2]));
                };
                let force = monitor::Force {
                    config: &self.config,
                    raft: &self.raft,
                    machine: &self.machine,
                    sightings: &self.sightings,
                    commits: &self.commits,
                    switches: &self.switches,
                };

                match monitor::force_failover(force, group).await {
                    Ok(address) => Value::bulk(address),
                    Err(error) => Value::Error(error),
                }
            }
            (Some("move"), 6) => {
                let (Ok(first), Ok(last), Ok(shard)) = (
                    words[2].parse::<u16>(),
                    words[3].parse::<u16>(),
                    words[5].parse::<usize>(),
                ) else {
                    return Value::Error("ERR slots and shard must be numbers".into());
                };
                // Group names keep their case; `words` are lowercased.
                let group = self
                    .config
                    .groups
                    .iter()
                    .find(|group| group.name.eq_ignore_ascii_case(&words[4]))
                    .map_or(words[4].clone(), |group| group.name.clone());

                match migrate::start(&self.moves, first, last, &group, shard).await {
                    Ok(id) => Value::bulk(id),
                    Err(error) => Value::Error(error),
                }
            }
            (Some("status"), 2) => migrate::status(&self.moves),
            (Some("cancel"), 2) => match migrate::cancel(&self.moves).await {
                Ok(()) => Value::Simple("OK".into()),
                Err(error) => Value::Error(error),
            },
            _ => Value::Error(
                "ERR COORDINATOR expects MOVE first last group shard, STATUS, CANCEL or FAILOVER group FORCE".into(),
            ),
        }
    }

    /// The agreed primary of `group` and the group itself.
    fn primary(&self, group: &str) -> Option<(&config::Group, usize)> {
        let group = self.config.group(group)?;
        let primary = self.machine.primaries().get(&group.name)?.member;

        Some((group, primary))
    }

    fn command(&self, words: &[String], args: &[Vec<u8>]) -> Value {
        let word = |index: usize| words.get(index).map(String::as_str);
        let name = || {
            args.get(2)
                .map(|name| String::from_utf8_lossy(name).into_owned())
                .unwrap_or_default()
        };

        match (word(0), word(1)) {
            (Some("ping"), _) => Value::Simple("PONG".into()),
            (Some("info"), _) => {
                let leader = self.raft.metrics().borrow().current_leader;
                let mut text = format!(
                    "# Coordinator\r\ncoordinator_id:{}\r\nleader_id:{}\r\nis_leader:{}\r\n",
                    self.config.id,
                    leader.map_or(-1, |id| id as i64),
                    u8::from(leader == Some(self.config.id)),
                );
                let primaries = self.machine.primaries();
                let recorded = self.machine.commits();

                for group in &self.config.groups {
                    match primaries.get(&group.name) {
                        Some(primary) => {
                            text += &format!(
                                "group_{}:primary={},epoch={}",
                                group.name, group.members[primary.member].address, primary.epoch
                            )
                        }
                        None => text += &format!("group_{}:primary=,epoch=0", group.name),
                    }
                    // Only for groups whose clients use `CLIENT DURABILITY`: per shard, the
                    // values in the Raft record, then the highest record a failover must keep
                    // as this coordinator knows it.
                    let recorded = recorded
                        .get(&group.name)
                        .map(|commit| commit.committed.as_slice())
                        .unwrap_or_default();

                    if recorded.iter().any(|&sequence| sequence > 0) {
                        let shards: Vec<String> = recorded.iter().map(u64::to_string).collect();

                        text += &format!(",recorded={}", shards.join("/"));
                    }
                    let committed = self.commits.known(&group.name);

                    if committed.iter().any(|&sequence| sequence > 0) {
                        let shards: Vec<String> = committed.iter().map(u64::to_string).collect();

                        text += &format!(",committed={}", shards.join("/"));
                    }
                    text += "\r\n";
                }
                Value::bulk(text)
            }
            (Some("role"), _) => Value::array(vec![
                Value::bulk("sentinel"),
                Value::array(
                    self.config
                        .groups
                        .iter()
                        .map(|group| Value::bulk(&group.name))
                        .collect(),
                ),
            ]),
            (Some("sentinel"), Some("get-master-addr-by-name")) => match self.primary(&name()) {
                Some((group, primary)) => {
                    let address = &group.members[primary].address;

                    Value::array(vec![
                        Value::bulk(&address.host),
                        Value::bulk(address.port.to_string()),
                    ])
                }
                None => Value::Array(None),
            },
            (Some("sentinel"), Some("masters")) => Value::array(
                self.config
                    .groups
                    .iter()
                    .filter_map(|group| self.primary(&group.name))
                    .map(|(group, primary)| self.describe(group, primary, true))
                    .collect(),
            ),
            (Some("sentinel"), Some("master")) => match self.primary(&name()) {
                Some((group, primary)) => self.describe(group, primary, true),
                None => Value::Error("ERR No such master with that name".into()),
            },
            (Some("sentinel"), Some("replicas" | "slaves")) => match self.primary(&name()) {
                Some((group, primary)) => Value::array(
                    (0..group.members.len())
                        .filter(|&member| member != primary)
                        .map(|member| self.describe(group, member, false))
                        .collect(),
                ),
                None => Value::Error("ERR No such master with that name".into()),
            },
            (Some("sentinel"), Some("sentinels")) => Value::array(
                self.config
                    .peers
                    .iter()
                    .filter(|(id, _)| *id != self.config.id)
                    .map(|(id, address)| {
                        Value::array(vec![
                            Value::bulk("name"),
                            Value::bulk(id.to_string()),
                            Value::bulk("ip"),
                            Value::bulk(&address.host),
                            Value::bulk("port"),
                            Value::bulk(address.port.to_string()),
                        ])
                    })
                    .collect(),
            ),
            _ => Value::Error(format!(
                "ERR unknown command for a Ruvio coordinator: {}",
                words.join(" ")
            )),
        }
    }

    /// One member as `SENTINEL masters` / `replicas` list it: a flat field-value array.
    fn describe(&self, group: &config::Group, member: usize, primary: bool) -> Value {
        let address = &group.members[member].address;
        let sighting = self.sightings.get(&group.name, member);
        let down = sighting
            .last_ok
            .is_none_or(|at| at.elapsed() > self.config.down_after);
        let flags = match (primary, down) {
            (true, false) => "master",
            (true, true) => "master,s_down",
            (false, false) => "slave",
            (false, true) => "slave,s_down",
        };
        let mut fields = vec![
            (
                "name",
                if primary {
                    group.name.clone()
                } else {
                    address.to_string()
                },
            ),
            ("ip", address.host.clone()),
            ("port", address.port.to_string()),
            ("flags", flags.to_owned()),
            (
                "last-ok-ping-reply",
                sighting
                    .last_ok
                    .map_or(-1, |at| at.elapsed().as_millis() as i64)
                    .to_string(),
            ),
        ];

        if primary {
            fields.push(("num-slaves", (group.members.len() - 1).to_string()));
            fields.push(("quorum", self.config.quorum().to_string()));
            // Read by Sentinel clients (redis-py's `check_master_state`).
            fields.push((
                "num-other-sentinels",
                (self.config.peers.len() - 1).to_string(),
            ));
            fields.push((
                "down-after-milliseconds",
                self.config.down_after.as_millis().to_string(),
            ));
            fields.push((
                "config-epoch",
                self.machine
                    .primaries()
                    .get(&group.name)
                    .map_or(0, |primary| primary.epoch)
                    .to_string(),
            ));
        } else {
            fields.push(("slave-repl-offset", sighting.durable.to_string()));
        }

        Value::array(
            fields
                .into_iter()
                .flat_map(|(name, value)| [Value::bulk(name), Value::bulk(value)])
                .collect(),
        )
    }

    /// `SUBSCRIBE +switch-master` (any channel list): confirms each channel, then pushes a
    /// message on every failover until the client hangs up.
    async fn subscribe(
        &self,
        words: &[String],
        write: &mut tokio::net::tcp::OwnedWriteHalf,
    ) -> io::Result<()> {
        let mut receiver = self.switches.subscribe();
        let kind = words[0].clone();

        for (count, channel) in words[1..].iter().enumerate() {
            let mut out = Vec::new();

            Value::array(vec![
                Value::bulk(&kind),
                Value::bulk(channel),
                Value::Integer(count as i64 + 1),
            ])
            .encode(&mut out);
            write.write_all(&out).await?;
        }

        loop {
            match receiver.recv().await {
                Ok(message) => {
                    let mut out = Vec::new();

                    Value::array(vec![
                        Value::bulk("message"),
                        Value::bulk("+switch-master"),
                        Value::bulk(message),
                    ])
                    .encode(&mut out);
                    write.write_all(&out).await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => return Ok(()),
            }
        }
    }
}
