//! `ruvio coordinator` against real data nodes: three coordinators fail a replication group
//! over when its primary dies, bring the old primary back as a replica, keep their agreement
//! across a restart, and do nothing without a majority. Network partitions and loss under
//! load are in `scripts/coordinator_faults.py`.

#![cfg(feature = "coordinator")]

mod common;

use std::{
    net::SocketAddr,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use std::io::{BufRead, BufReader, Write};

use common::test_ports;

const PATIENCE: Duration = Duration::from_secs(30);
const SECRET: &str = "coordinator-test-secret";

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Process {
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Group {
    resp: Vec<SocketAddr>,
    replication: Vec<SocketAddr>,
    coordinators: Vec<SocketAddr>,
    /// A free address for a coordinator that joins later.
    spare: SocketAddr,
    /// How many of the data nodes the coordinators' file lists (the first ones).
    listed: usize,
    /// Shards per data node; shard i of a node listens on its ports plus i.
    shards: u16,
    /// Data nodes serve only TLS on their RESP ports when set.
    tls: Option<TestTls>,
    _ports: (test_ports::PortBlock, test_ports::PortBlock),
}

impl Group {
    fn new() -> Self {
        Self::with_shards(1)
    }

    fn with_shards(shards: u16) -> Self {
        let data = test_ports::reserve();
        let replication = test_ports::reserve();

        Self {
            resp: (0..3)
                .map(|index| data.address_at(index * shards))
                .collect(),
            replication: (0..3)
                .map(|index| replication.address_at(index * shards))
                .collect(),
            coordinators: (0..3)
                .map(|index| data.address_at(3 * shards + index))
                .collect(),
            spare: data.address_at(3 * shards + 3),
            listed: 3,
            shards,
            tls: None,
            _ports: (data, replication),
        }
    }

    /// A group whose data nodes and coordinators all use certificates from one test CA.
    fn with_tls(dir: &Path) -> Self {
        Self {
            tls: Some(TestTls::new(dir)),
            ..Self::new()
        }
    }

    /// One command to data node `index`, over TLS when the group has it.
    fn call_node(&self, index: usize, args: &[&str]) -> Option<String> {
        let address = self.resp[index].to_string();

        match &self.tls {
            Some(tls) => tls.calls(&address, &[args])?.pop(),
            None => call(&address, args),
        }
    }

    fn node(&self, dir: &Path, index: usize, follow: Option<usize>) -> Process {
        self.node_with(dir, index, follow, &[])
    }

    fn node_with(
        &self,
        dir: &Path,
        index: usize,
        follow: Option<usize>,
        env: &[(&str, &str)],
    ) -> Process {
        let data_dir = dir.join(format!("node{index}"));
        let config = dir.join(format!("node{index}.toml"));

        if !config.exists() {
            std::fs::copy(
                concat!(env!("CARGO_MANIFEST_DIR"), "/ruvio.example.toml"),
                &config,
            )
            .unwrap();
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));

        command
            .arg("--config")
            .arg(&config)
            .args(["--address", &self.resp[index].to_string()])
            .args(["--durability", "always", "--data-dir"])
            .arg(&data_dir)
            .env(
                "RUVIO_REPLICATION_LISTEN",
                self.replication[index].to_string(),
            )
            .env("RUVIO_SHARDS", self.shards.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match follow {
            Some(primary) => command.env("RUVIO_REPLICATION_ROLE", "replica").env(
                "RUVIO_REPLICATION_PRIMARY",
                self.replication[primary].to_string(),
            ),
            None => command.env("RUVIO_REPLICATION_ROLE", "primary"),
        };
        if let Some(tls) = &self.tls {
            command
                .env("RUVIO_TLS_CERT", &tls.cert)
                .env("RUVIO_TLS_KEY", &tls.key);
        }
        command.envs(env.iter().copied());
        let process = Process(command.spawn().unwrap());

        wait_until("a data node to answer", || {
            self.call_node(index, &["PING"]).as_deref() == Some("PONG")
        });
        process
    }

    fn coordinator(&self, dir: &Path, index: usize) -> Process {
        self.coordinator_with(dir, index, "", &self.resp, None)
    }

    /// A coordinator whose log goes to `log` when given.
    fn coordinator_logging(&self, dir: &Path, index: usize, log: Option<&Path>) -> Process {
        self.coordinator_with(dir, index, "", &self.resp, log)
    }

    /// A coordinator with `extra` settings that dials the data nodes at `addresses`, its log
    /// going to `log` when given. `down_after_ms` is 1000 unless `extra` sets it.
    fn coordinator_with(
        &self,
        dir: &Path,
        index: usize,
        extra: &str,
        addresses: &[SocketAddr],
        log: Option<&Path>,
    ) -> Process {
        let output = || match log {
            Some(path) => Stdio::from(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .unwrap(),
            ),
            None => Stdio::null(),
        };
        let config = dir.join(format!("coordinator{index}.toml"));
        let down_after = if extra.contains("down_after_ms") {
            ""
        } else {
            "down_after_ms = 1000\n"
        };
        let peers = self
            .coordinators
            .iter()
            .enumerate()
            .map(|(id, address)| format!("\"{}@{address}\"", id + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let members = (0..self.listed)
            .map(|node| {
                format!(
                    "  {{ address = \"{}\", replication = \"{}\" }}",
                    addresses[node], self.replication[node]
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");

        std::fs::write(
            &config,
            format!(
                "id = {}\nlisten = \"{}\"\ndata_dir = \"{}\"\npeers = [{peers}]\nsecret = \"{SECRET}\"\n\
                 ping_interval_ms = 200\n{down_after}{extra}\n\n[[groups]]\nname = \"main\"\n\
                 members = [\n{members}\n]\n",
                index + 1,
                self.coordinators[index],
                dir.join(format!("coordinator{index}")).display(),
            ),
        )
        .unwrap();

        Process(
            Command::new(env!("CARGO_BIN_EXE_ruvio"))
                .arg("coordinator")
                .arg("--config")
                .arg(&config)
                .stdout(output())
                .stderr(output())
                .spawn()
                .unwrap(),
        )
    }

    /// `args` on whichever coordinator takes it as the leader.
    fn on_leader(&self, args: &[&str]) -> Option<String> {
        self.coordinators.iter().find_map(|coordinator| {
            call_with_secret(&coordinator.to_string(), args).filter(|reply| {
                !reply.starts_with("ERR not the leader")
                    && !reply.starts_with("ERR no leader")
                    && !reply.starts_with("ERR this leader")
            })
        })
    }

    /// `INFO` of the coordinator that answers as the leader.
    fn leader_info(&self) -> Option<String> {
        self.coordinators.iter().find_map(|coordinator| {
            call_with_secret(&coordinator.to_string(), &["INFO"])
                .filter(|info| info.contains("is_leader:1\r\n"))
        })
    }

    /// The primary coordinator `index` names, as an index into the data nodes.
    fn primary_by(&self, index: usize) -> Option<usize> {
        self.primary_among(index, &self.resp)
    }

    /// The primary coordinator `index` names, as an index into `addresses`: the data nodes'
    /// addresses as the coordinators were given them.
    fn primary_among(&self, index: usize, addresses: &[SocketAddr]) -> Option<usize> {
        let address = self.coordinators[index].to_string();
        let question = ["SENTINEL", "get-master-addr-by-name", "main"];
        let reply = match &self.tls {
            Some(tls) => {
                let replies = tls.calls(&address, &[&["AUTH", SECRET], &question])?;

                (replies[0] == "OK").then_some(())?;
                replies[1].clone()
            }
            None => call_with_secret(&address, &question)?,
        };
        let port: u16 = reply.split_whitespace().nth(1)?.parse().ok()?;

        addresses.iter().position(|address| address.port() == port)
    }
}

/// One command on a fresh connection, its reply as text: simple strings, errors and bulk
/// strings as they are, an array as its items joined by spaces; `None` when nothing answers.
fn call(address: &str, args: &[&str]) -> Option<String> {
    let mut stream =
        std::net::TcpStream::connect_timeout(&address.parse().ok()?, Duration::from_millis(300))
            .ok()?;
    let mut frame = format!("*{}\r\n", args.len());

    for arg in args {
        frame.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
    }
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream.write_all(frame.as_bytes()).ok()?;
    read_reply(&mut BufReader::new(stream))
}

/// `AUTH` with the coordinators' secret, then `args`, on one connection.
fn call_with_secret(address: &str, args: &[&str]) -> Option<String> {
    let mut stream =
        std::net::TcpStream::connect_timeout(&address.parse().ok()?, Duration::from_millis(300))
            .ok()?;
    let frame = |args: &[&str]| {
        let mut frame = format!("*{}\r\n", args.len());

        for arg in args {
            frame.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        frame
    };

    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .write_all((frame(&["AUTH", SECRET]) + &frame(args)).as_bytes())
        .ok()?;
    let mut reader = BufReader::new(stream);

    (read_reply(&mut reader)? == "OK").then_some(())?;
    read_reply(&mut reader)
}

fn read_reply(reader: &mut impl BufRead) -> Option<String> {
    let mut line = String::new();

    reader.read_line(&mut line).ok()?;
    let line = line.trim_end();
    let rest = line.get(1..)?;

    match line.as_bytes().first()? {
        b'+' | b'-' | b':' => Some(rest.to_owned()),
        b'$' if rest.starts_with('-') => Some("(nil)".into()),
        b'$' => {
            let mut body = vec![0; rest.parse::<usize>().ok()? + 2];

            reader.read_exact(&mut body).ok()?;
            body.truncate(body.len() - 2);
            String::from_utf8(body).ok()
        }
        b'*' if rest.starts_with('-') => Some("(nil)".into()),
        b'*' => (0..rest.parse::<usize>().ok()?)
            .map(|_| read_reply(reader))
            .collect::<Option<Vec<_>>>()
            .map(|items| items.join(" ")),
        _ => None,
    }
}

fn ping(address: &str) -> bool {
    call(address, &["PING"]).as_deref() == Some("PONG")
}

#[track_caller]
fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;

    while Instant::now() < deadline {
        if done() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("gave up after {PATIENCE:?} waiting for {what}");
}

#[test]
fn coordinators_fail_over_a_dead_primary_and_bring_it_back_as_a_replica() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];
    let primary = group.resp[0].to_string();

    assert_eq!(
        call(&primary, &["SET", "before", "1"]).as_deref(),
        Some("OK")
    );
    wait_until("the replicas to copy the primary", || {
        (1..3).all(|node| {
            call(&group.resp[node].to_string(), &["GET", "before"]).as_deref() == Some("1")
        })
    });
    let mut coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });
    // The Sentinel API needs the secret.
    let coordinator = group.coordinators[0].to_string();

    assert!(
        call(&coordinator, &["SENTINEL", "masters"])
            .unwrap()
            .starts_with("NOAUTH")
    );
    assert!(
        call(&coordinator, &["AUTH", "wrong"])
            .unwrap()
            .starts_with("WRONGPASS")
    );

    // A lease the old primary granted: the new one's tokens are larger, from a newer generation.
    let lease = |node: usize| -> u64 {
        call(&group.resp[node].to_string(), &["LEASE", "fence", "100"])
            .unwrap()
            .parse()
            .unwrap()
    };
    let old_token = lease(0);

    // The primary dies: a replica takes over and the other follows it.
    nodes[0].kill();
    let killed = Instant::now();

    wait_until("a failover", || {
        group.primary_by(0).is_some_and(|node| node != 0)
    });
    let promoted = group.primary_by(0).unwrap();
    let other = 3 - promoted;

    wait_until("the new primary to take writes", || {
        call(&group.resp[promoted].to_string(), &["SET", "after", "1"]).as_deref() == Some("OK")
    });
    assert!(
        killed.elapsed() < Duration::from_secs(15),
        "failover took {:?}",
        killed.elapsed()
    );
    let new_token = lease(promoted);

    assert!(
        new_token >> 48 > old_token >> 48,
        "token {new_token} after the failover is not from a newer generation than {old_token}"
    );
    wait_until("the other replica to follow the new primary", || {
        call(&group.resp[other].to_string(), &["GET", "after"]).as_deref() == Some("1")
    });
    assert_eq!(group.primary_by(1), Some(promoted));
    assert_eq!(group.primary_by(2), Some(promoted));
    // The leader counts the failover; the old primary no longer answers.
    let info = group.leader_info().unwrap();

    assert!(info.contains("failovers_total:1\r\n"), "{info}");
    assert!(!info.contains("last_failover_unix_ms:0\r\n"), "{info}");
    assert!(info.contains(",members_answering=2/3,state=ok,"), "{info}");

    // The old primary returns as a writer and is made a replica of the new one.
    nodes[0] = group.node(dir.path(), 0, None);
    wait_until("the old primary to follow the new one", || {
        call(&primary, &["GET", "after"]).as_deref() == Some("1")
    });
    assert!(
        call(&primary, &["SET", "late", "1"])
            .unwrap()
            .starts_with("READONLY")
    );

    // The coordinators keep their agreement across a restart.
    coordinators.clear();
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until(
        "the restarted coordinators to report the new primary",
        || (0..3).all(|index| group.primary_by(index) == Some(promoted)),
    );
}

#[test]
fn a_leader_cut_off_from_the_other_coordinators_does_not_act() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];
    let mut coordinators: Vec<Option<Process>> = (0..3)
        .map(|index| Some(group.coordinator(dir.path(), index)))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });
    let is_leader = |index: usize| {
        call_with_secret(&group.coordinators[index].to_string(), &["INFO"])
            .is_some_and(|info| info.contains("is_leader:1"))
    };
    let mut leader = None;

    wait_until("a leader", || {
        leader = (0..3).find(|&index| is_leader(index));
        leader.is_some()
    });
    let leader = leader.unwrap();

    // The leader loses the other coordinators but still reaches every data node. openraft
    // keeps it leading; only a confirmation by a majority stops it.
    for (index, coordinator) in coordinators.iter_mut().enumerate() {
        if index != leader {
            coordinator.take().unwrap().kill();
        }
    }
    // Longer than a confirmation is trusted (one ping interval).
    std::thread::sleep(Duration::from_secs(1));
    assert!(is_leader(leader), "openraft stepped the lone leader down");

    // As if the majority had promoted member 1 meanwhile: the stale leader's record still names
    // member 0, and acting on it would point member 1 back at member 0.
    let role = |node: usize| call(&group.resp[node].to_string(), &["ROLE"]).unwrap_or_default();

    assert_eq!(
        call(&group.resp[1].to_string(), &["REPLICAOF", "NO", "ONE"]).as_deref(),
        Some("OK")
    );
    assert!(role(1).starts_with("master"));
    nodes[0].kill();
    // Several times down_after_ms: the lone leader neither fails over nor reconciles.
    let watched = Instant::now();

    while watched.elapsed() < Duration::from_secs(4) {
        assert_eq!(group.primary_by(leader), Some(0));
        assert!(
            role(1).starts_with("master"),
            "the cut-off leader pointed member 1 at its old record: {}",
            role(1)
        );
        assert!(
            call(&group.resp[2].to_string(), &["SET", "x", "1"])
                .unwrap()
                .starts_with("READONLY")
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // The majority is back: the coordinators act again, and member 1 is made a replica.
    for (index, coordinator) in coordinators.iter_mut().enumerate() {
        if index != leader {
            *coordinator = Some(group.coordinator(dir.path(), index));
        }
    }
    wait_until("the coordinators to act again with a majority", || {
        role(1).starts_with("slave")
    });
}

/// Several commands on one connection, each reply as `call` gives it.
fn calls(address: &str, commands: &[&[&str]]) -> Option<Vec<String>> {
    let mut stream =
        std::net::TcpStream::connect_timeout(&address.parse().ok()?, Duration::from_millis(300))
            .ok()?;
    let mut frame = String::new();

    for args in commands {
        frame.push_str(&format!("*{}\r\n", args.len()));
        for arg in *args {
            frame.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
    }
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .ok()?;
    stream.write_all(frame.as_bytes()).ok()?;
    let mut reader = BufReader::new(stream);

    commands.iter().map(|_| read_reply(&mut reader)).collect()
}

fn info_field(address: &str, name: &str) -> Option<u64> {
    call(address, &["INFO", "replication"])?
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))?
        .trim()
        .parse()
        .ok()
}

#[test]
fn a_replica_missing_a_committed_record_is_promoted_only_when_forced() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];
    let primary = group.resp[0].to_string();

    assert_eq!(
        call(&primary, &["SET", "before", "1"]).as_deref(),
        Some("OK")
    );
    wait_until("the replicas to copy the primary", || {
        (1..3).all(|node| {
            call(&group.resp[node].to_string(), &["GET", "before"]).as_deref() == Some("1")
        })
    });
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });

    // Member 2 is down while a durable write is answered: only member 1 holds it.
    nodes[2].kill();
    assert_eq!(
        calls(
            &primary,
            &[
                &["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "10000"],
                &["SET", "kept", "1"],
            ],
        ),
        Some(vec!["OK".to_owned(), "OK".to_owned()])
    );
    let mut committed = 0;

    wait_until("the primary to record the committed sequence", || {
        committed = info_field(&primary, "replication_committed_sequence").unwrap_or(0);
        committed > 0
    });
    let learned = format!(",committed={committed}\r\n");

    wait_until("every coordinator to learn the committed sequence", || {
        group.coordinators.iter().all(|coordinator| {
            call_with_secret(&coordinator.to_string(), &["INFO"])
                .is_some_and(|info| info.contains(&learned))
        })
    });

    // The primary and member 1 die; member 2 comes back without the record.
    nodes[0].kill();
    nodes[1].kill();
    nodes[2] = group.node(dir.path(), 2, Some(0));
    let returned = group.resp[2].to_string();

    assert!(info_field(&returned, "wal_durable_sequence").unwrap() < committed);
    wait_until("the coordinators to see member 2 answer", || {
        group.coordinators.iter().all(|coordinator| {
            call_with_secret(&coordinator.to_string(), &["SENTINEL", "replicas", "main"])
                .is_some_and(|replicas| {
                    // One entry per replica, each starting with its name.
                    replicas.split("name ").any(|replica| {
                        replica.starts_with(&format!("{returned} "))
                            && replica.contains(" flags slave ")
                    })
                })
        })
    });
    // Several times down_after_ms: the group waits rather than lose the record.
    std::thread::sleep(Duration::from_secs(4));
    for index in 0..3 {
        assert_eq!(group.primary_by(index), Some(0));
    }
    let info = group.leader_info().unwrap();

    assert!(info.contains("failovers_total:0\r\n"), "{info}");
    assert!(
        info.contains(",members_answering=1/3,state=waiting,"),
        "{info}"
    );
    assert!(
        call(&returned, &["SET", "x", "1"])
            .unwrap()
            .starts_with("READONLY")
    );

    // The operator accepts the loss on the leader; the others refuse.
    let mut promoted = None;

    wait_until("a coordinator to take the forced failover", || {
        promoted = group.coordinators.iter().find_map(|coordinator| {
            call_with_secret(
                &coordinator.to_string(),
                &["COORDINATOR", "FAILOVER", "main", "FORCE"],
            )
            .filter(|reply| {
                !reply.starts_with("ERR not the leader")
                    && !reply.starts_with("ERR no leader")
                    && !reply.starts_with("ERR the leader is still applying")
            })
        });
        promoted.is_some()
    });
    assert_eq!(promoted.as_deref(), Some(returned.as_str()));
    wait_until("every coordinator to name member 2", || {
        (0..3).all(|index| group.primary_by(index) == Some(2))
    });
    wait_until("member 2 to take writes", || {
        call(&returned, &["SET", "after", "1"]).as_deref() == Some("OK")
    });
    assert_eq!(call(&returned, &["GET", "kept"]).as_deref(), Some("(nil)"));
    let info = group.leader_info().unwrap();

    assert!(info.contains("failovers_total:1\r\n"), "{info}");
    assert!(info.contains(",state=ok,"), "{info}");
}

/// The committed sequences are in Raft: after the primary and the only replica holding a
/// committed record die and every coordinator restarts, the new leader still knows the record
/// and does not promote the replica that lacks it.
#[test]
fn restarted_coordinators_keep_the_committed_records_a_failover_must_hold() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];
    let primary = group.resp[0].to_string();

    assert_eq!(
        call(&primary, &["SET", "before", "1"]).as_deref(),
        Some("OK")
    );
    wait_until("the replicas to copy the primary", || {
        (1..3).all(|node| {
            call(&group.resp[node].to_string(), &["GET", "before"]).as_deref() == Some("1")
        })
    });
    let mut coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });

    // Member 2 is down while a durable write is answered: only member 1 holds it.
    nodes[2].kill();
    assert_eq!(
        calls(
            &primary,
            &[
                &["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "10000"],
                &["SET", "kept", "1"],
            ],
        ),
        Some(vec!["OK".to_owned(), "OK".to_owned()])
    );
    let mut committed = 0;

    wait_until("the primary to record the committed sequence", || {
        committed = info_field(&primary, "replication_committed_sequence").unwrap_or(0);
        committed > 0
    });
    // The leader writes the value through Raft: wait until every coordinator applied it, so
    // it is in the log of each.
    let recorded = format!(",recorded={committed},");

    wait_until(
        "every coordinator to apply the recorded committed sequence",
        || {
            group.coordinators.iter().all(|coordinator| {
                call_with_secret(&coordinator.to_string(), &["INFO"])
                    .is_some_and(|info| info.contains(&recorded))
            })
        },
    );

    // The primary and member 1 die, every coordinator restarts, and member 2 comes back
    // without the record. No member that answers knows it was committed.
    nodes[0].kill();
    nodes[1].kill();
    coordinators.clear();
    nodes[2] = group.node(dir.path(), 2, Some(0));
    let returned = group.resp[2].to_string();

    assert!(info_field(&returned, "wal_durable_sequence").unwrap() < committed);
    assert_eq!(
        info_field(&returned, "replication_committed_sequence"),
        Some(0)
    );
    let logs: Vec<_> = (0..3)
        .map(|index| dir.path().join(format!("coordinator{index}.log")))
        .collect();
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator_logging(dir.path(), index, Some(&logs[index])))
        .collect();
    let learned = format!(",committed={committed}\r\n");
    // Checked on every poll below, so a wrong promotion fails at once.
    let not_promoted = || {
        if let Some(primary) = group.primary_by(0) {
            assert_eq!(
                primary, 0,
                "a replica missing the committed record was promoted"
            );
        }
    };

    wait_until(
        "the restarted coordinators to know the committed sequence and see member 2",
        || {
            not_promoted();
            group.coordinators.iter().all(|coordinator| {
                let coordinator = coordinator.to_string();

                call_with_secret(&coordinator, &["INFO"])
                    .is_some_and(|info| info.contains(&learned) && !info.contains("leader_id:-1"))
                    && call_with_secret(&coordinator, &["SENTINEL", "replicas", "main"])
                        .is_some_and(|replicas| {
                            replicas.split("name ").any(|replica| {
                                replica.starts_with(&format!("{returned} "))
                                    && replica.contains(" flags slave ")
                            })
                        })
            })
        },
    );
    // The new leader decided: it waits rather than lose the record.
    wait_until(
        "the leader to wait for a replica holding the record",
        || {
            not_promoted();
            logs.iter().any(|log| {
                std::fs::read_to_string(log).is_ok_and(|text| {
                    text.contains(
                        "no replica holds every record a durable reply was released for; waiting",
                    )
                })
            })
        },
    );
    for index in 0..3 {
        assert_eq!(group.primary_by(index), Some(0));
    }
    assert!(
        call(&returned, &["SET", "x", "1"])
            .unwrap()
            .starts_with("READONLY")
    );

    // Only the operator's FORCE promotes it.
    let mut promoted = None;

    wait_until("a coordinator to take the forced failover", || {
        promoted = group.coordinators.iter().find_map(|coordinator| {
            call_with_secret(
                &coordinator.to_string(),
                &["COORDINATOR", "FAILOVER", "main", "FORCE"],
            )
            .filter(|reply| {
                !reply.starts_with("ERR not the leader")
                    && !reply.starts_with("ERR no leader")
                    && !reply.starts_with("ERR the leader is still applying")
            })
        });
        promoted.is_some()
    });
    assert_eq!(promoted.as_deref(), Some(returned.as_str()));
    wait_until("every coordinator to name member 2", || {
        (0..3).all(|index| group.primary_by(index) == Some(2))
    });
}

/// Two groups of a multi-node cluster under one coordinator: the coordinator sends every node
/// the map, and after a failover in one group the other group's nodes redirect its keys to
/// the new primary.
#[test]
fn a_failover_in_one_group_moves_the_redirects_of_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_ports::reserve();
    let replication = test_ports::reserve();
    let resp: Vec<SocketAddr> = (0..4).map(|index| data.address_at(index)).collect();
    let repl: Vec<SocketAddr> = (0..4).map(|index| replication.address_at(index)).collect();
    let coordinator = data.address_at(4);
    let topology = dir.path().join("topology.toml");
    let groups = [("a", "0-8191", 0), ("b", "8192-16383", 2)];

    std::fs::write(
        &topology,
        groups.iter().fold("version = 1\n".to_owned(), |text, (name, slots, node)| {
            format!(
                "{text}[[groups]]\nname = \"{name}\"\naddress = \"{}\"\nshards = 1\nslots = [\"{slots}\"]\n",
                resp[*node]
            )
        }),
    )
    .unwrap();

    let node = |index: usize| {
        let (group, follow) = match index {
            0 => ("a", None),
            1 => ("a", Some(0)),
            2 => ("b", None),
            _ => ("b", Some(2)),
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));

        command
            .args(["--address", &resp[index].to_string()])
            .args(["--durability", "always", "--data-dir"])
            .arg(dir.path().join(format!("node{index}")))
            .env("RUVIO_REPLICATION_LISTEN", repl[index].to_string())
            .env("RUVIO_CLUSTER_TOPOLOGY", &topology)
            .env("RUVIO_CLUSTER_GROUP", group)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match follow {
            Some(primary) => command
                .env("RUVIO_REPLICATION_ROLE", "replica")
                .env("RUVIO_REPLICATION_PRIMARY", repl[primary].to_string()),
            None => command.env("RUVIO_REPLICATION_ROLE", "primary"),
        };
        let process = Process(command.spawn().unwrap());

        wait_until("a data node to answer", || ping(&resp[index].to_string()));
        process
    };
    let mut nodes: Vec<Process> = (0..4).map(node).collect();
    let members = |first: usize| {
        (first..first + 2)
            .map(|index| {
                format!(
                    "{{ address = \"{}\", replication = \"{}\" }}",
                    resp[index], repl[index]
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let config = dir.path().join("coordinator.toml");

    std::fs::write(
        &config,
        format!(
            "id = 1\nlisten = \"{coordinator}\"\ndata_dir = \"{}\"\npeers = [\"1@{coordinator}\"]\n\
             secret = \"{SECRET}\"\nping_interval_ms = 200\ndown_after_ms = 1000\n\n\
             [[groups]]\nname = \"a\"\nshards = 1\nslots = [\"0-8191\"]\nmembers = [{}]\n\n\
             [[groups]]\nname = \"b\"\nshards = 1\nslots = [\"8192-16383\"]\nmembers = [{}]\n",
            dir.path().join("coordinator").display(),
            members(0),
            members(2),
        ),
    )
    .unwrap();
    let _coordinator = Process(
        Command::new(env!("CARGO_BIN_EXE_ruvio"))
            .arg("coordinator")
            .arg("--config")
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let version = |index: usize| call(&resp[index].to_string(), &["RUVIO", "TOPOLOGY", "VERSION"]);

    // Each group's first primary is recorded at epoch 1: version 1 + 1 + 1.
    wait_until("every node to get the coordinator's map", || {
        (0..4).all(|index| version(index).as_deref() == Some("3"))
    });

    // Slot 12182 belongs to group b; node a0 redirects it to b's primary.
    let key = "foo";
    let moved = |port: u16| format!("MOVED 12182 127.0.0.1:{port}");

    assert_eq!(
        call(&resp[0].to_string(), &["GET", key]).unwrap(),
        moved(resp[2].port())
    );

    nodes[2].kill();
    wait_until(
        "node a0 to redirect group b's keys to its new primary",
        || call(&resp[0].to_string(), &["GET", key]).as_deref() == Some(&moved(resp[3].port())),
    );
    assert_eq!(version(0).as_deref(), Some("4"));
    wait_until("the new primary of group b to take the key", || {
        call(&resp[3].to_string(), &["SET", key, "1"]).as_deref() == Some("OK")
    });
}

/// Group `a` (nodes 0 and 1) owns slots 0-8191 (or up to the split asked for), group `b`
/// (nodes 2 and 3) the rest; one shard each, node 1 and 3 replicas; one coordinator.
struct TwoGroups {
    resp: Vec<SocketAddr>,
    coordinator: String,
    processes: Vec<Option<Process>>,
    _stand_ins: Vec<StandIn>,
    _ports: (test_ports::PortBlock, test_ports::PortBlock),
}

impl TwoGroups {
    /// `env`: extra settings of every data node.
    fn start(dir: &Path, env: &[(&str, &str)]) -> Self {
        Self::start_with(dir, env, |_, _| None)
    }

    /// Like [`TwoGroups::start`], but node `index` is whatever `stand_in(index, address)`
    /// starts when it returns `Some`.
    fn start_with(
        dir: &Path,
        env: &[(&str, &str)],
        stand_in: impl Fn(usize, SocketAddr) -> Option<StandIn>,
    ) -> Self {
        Self::start_split(dir, env, 8192, stand_in)
    }

    /// Like [`TwoGroups::start_with`], with group `a` owning slots `0..split`.
    fn start_split(
        dir: &Path,
        env: &[(&str, &str)],
        split: u16,
        stand_in: impl Fn(usize, SocketAddr) -> Option<StandIn>,
    ) -> Self {
        let (a_slots, b_slots) = (format!("0-{}", split - 1), format!("{split}-16383"));
        let data = test_ports::reserve();
        let replication = test_ports::reserve();
        let resp: Vec<SocketAddr> = (0..4).map(|index| data.address_at(index)).collect();
        let repl: Vec<SocketAddr> = (0..4).map(|index| replication.address_at(index)).collect();
        let coordinator = data.address_at(4);
        let topology = dir.join("topology.toml");
        let mut stand_ins = Vec::new();

        std::fs::write(
            &topology,
            [("a", &a_slots, 0), ("b", &b_slots, 2)].iter().fold(
                "version = 1\n".to_owned(),
                |text, (name, slots, node)| {
                    format!(
                        "{text}[[groups]]\nname = \"{name}\"\naddress = \"{}\"\nshards = 1\nslots = [\"{slots}\"]\n",
                        resp[*node]
                    )
                },
            ),
        )
        .unwrap();
        let mut processes: Vec<Option<Process>> = (0..4)
            .map(|index| {
                if let Some(stand_in) = stand_in(index, resp[index]) {
                    stand_ins.push(stand_in);
                    return None;
                }
                let group = if index < 2 { "a" } else { "b" };
                let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));

                command
                    .args(["--address", &resp[index].to_string()])
                    .args(["--durability", "always", "--data-dir"])
                    .arg(dir.join(format!("node{index}")))
                    .env("RUVIO_REPLICATION_LISTEN", repl[index].to_string())
                    .env("RUVIO_CLUSTER_TOPOLOGY", &topology)
                    .env("RUVIO_CLUSTER_GROUP", group)
                    .envs(env.iter().copied())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                if index % 2 == 1 {
                    command
                        .env("RUVIO_REPLICATION_ROLE", "replica")
                        .env("RUVIO_REPLICATION_PRIMARY", repl[index - 1].to_string());
                } else {
                    command.env("RUVIO_REPLICATION_ROLE", "primary");
                }
                let process = Process(command.spawn().unwrap());

                wait_until("a data node to answer", || ping(&resp[index].to_string()));
                Some(process)
            })
            .collect();
        let members = |first: usize| {
            (first..first + 2)
                .map(|index| {
                    format!(
                        "{{ address = \"{}\", replication = \"{}\" }}",
                        resp[index], repl[index]
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        let config = dir.join("coordinator.toml");

        std::fs::write(
            &config,
            format!(
                "id = 1\nlisten = \"{coordinator}\"\ndata_dir = \"{}\"\npeers = [\"1@{coordinator}\"]\n\
                 secret = \"{SECRET}\"\nping_interval_ms = 200\ndown_after_ms = 1000\n\n\
                 [[groups]]\nname = \"a\"\nshards = 1\nslots = [\"{a_slots}\"]\nmembers = [{}]\n\n\
                 [[groups]]\nname = \"b\"\nshards = 1\nslots = [\"{b_slots}\"]\nmembers = [{}]\n",
                dir.join("coordinator").display(),
                members(0),
                members(2),
            ),
        )
        .unwrap();
        processes.push(Some(Process(
            Command::new(env!("CARGO_BIN_EXE_ruvio"))
                .arg("coordinator")
                .arg("--config")
                .arg(&config)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )));

        let groups = Self {
            resp,
            coordinator: coordinator.to_string(),
            processes,
            _stand_ins: stand_ins,
            _ports: (data, replication),
        };

        wait_until("every node to get the coordinator's map", || {
            (0..4).all(|index| groups.version(index).as_deref() == Some("3"))
        });
        groups
    }

    fn node(&self, index: usize) -> String {
        self.resp[index].to_string()
    }

    fn version(&self, index: usize) -> Option<String> {
        call(&self.node(index), &["RUVIO", "TOPOLOGY", "VERSION"])
    }
}

/// A key whose slot is in `range`.
fn key_in(range: std::ops::Range<u16>, label: &str) -> String {
    (0..)
        .map(|n| format!("{label}:{n}"))
        .find(|key| range.contains(&ruvio::routing::hash_slot(key.as_bytes())))
        .unwrap()
}

#[test]
fn a_slot_range_moves_between_groups_while_a_client_keeps_writing() {
    let dir = tempfile::tempdir().unwrap();
    let groups = TwoGroups::start(dir.path(), &[]);
    let keys: Vec<String> = (0..)
        .map(|n| format!("moving:{n}"))
        .filter(|key| (100..200).contains(&ruvio::routing::hash_slot(key.as_bytes())))
        .take(300)
        .collect();

    for key in &keys {
        assert_eq!(
            call(&groups.node(0), &["SET", key, key]).as_deref(),
            Some("OK")
        );
    }

    // A lease in the range moves with it; the new owner's next token is from a newer generation.
    let lock = key_in(100..200, "lock");
    let old_token: u64 = call(&groups.node(0), &["LEASE", &lock, "600000"])
        .unwrap()
        .parse()
        .unwrap();

    // A client writes one key of the range throughout, following MOVED and TRYAGAIN as a
    // cluster client does; every write it saw acknowledged must be on the new owner.
    let busy = key_in(100..200, "busy");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (busy, stop) = (busy.clone(), std::sync::Arc::clone(&stop));
        let mut address = groups.node(0);
        let b = groups.node(2);

        std::thread::spawn(move || {
            let (mut acked, mut retried) = (0u64, 0u64);
            // From the first `TRYAGAIN` of a write to its `OK`: how long a caller waits
            // while the range is frozen.
            let mut frozen: Option<std::time::Instant> = None;
            let mut longest = Duration::ZERO;

            for value in 1u64.. {
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                loop {
                    match call(&address, &["SET", &busy, &value.to_string()]).as_deref() {
                        Some("OK") => {
                            acked = value;
                            if let Some(since) = frozen.take() {
                                longest = longest.max(since.elapsed());
                            }
                            break;
                        }
                        Some(reply) if reply.starts_with("MOVED") => address = b.clone(),
                        Some(reply) if reply.starts_with("TRYAGAIN") => {
                            retried += 1;
                            frozen.get_or_insert_with(std::time::Instant::now);
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        other => panic!("unexpected reply {other:?}"),
                    }
                }
            }
            eprintln!(
                "move freeze seen by a writer: {:.1} ms over {retried} TRYAGAIN replies",
                longest.as_secs_f64() * 1000.0
            );
            (acked, retried, address)
        })
    };

    std::thread::sleep(Duration::from_millis(200));
    let id = call_with_secret(
        &groups.coordinator,
        &["COORDINATOR", "MOVE", "100", "199", "b", "0"],
    )
    .unwrap();

    assert!(id.starts_with('m'), "{id}");
    assert!(
        call_with_secret(
            &groups.coordinator,
            &["COORDINATOR", "MOVE", "300", "399", "b", "0"]
        )
        .unwrap()
        .contains("another move")
    );
    wait_until("the move to finish", || {
        call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"])
            .is_some_and(|status| status.starts_with("state done"))
    });
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::Release);
    let (acked, _retried, address) = writer.join().unwrap();

    assert_eq!(address, groups.node(2), "the writer followed the range");
    assert_eq!(
        call(&groups.node(2), &["GET", &busy]),
        Some(acked.to_string())
    );

    // Every node has the new map; the range is served by b and redirected by a.
    wait_until("every node to get the map with the move", || {
        (0..4).all(|index| groups.version(index).as_deref() == Some("4"))
    });
    let b_port = groups.resp[2].port();
    let token = old_token.to_string();

    assert_eq!(
        call(&groups.node(2), &["RELEASE", &lock, &token]).as_deref(),
        Some("1")
    );
    let new_token: u64 = call(&groups.node(2), &["LEASE", &lock, "600000"])
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        new_token >> 48 > old_token >> 48,
        "token {new_token} after the move is not from a newer generation than {old_token}"
    );

    for key in &keys {
        let slot = ruvio::routing::hash_slot(key.as_bytes());

        assert_eq!(
            call(&groups.node(2), &["GET", key]).as_deref(),
            Some(key.as_str())
        );
        assert_eq!(
            call(&groups.node(0), &["GET", key]),
            Some(format!("MOVED {slot} 127.0.0.1:{b_port}"))
        );
    }
    // The replicas follow: b's copies the range, a's drops it.
    wait_until("b's replica to hold the range", || {
        keys.iter()
            .all(|key| call(&groups.node(3), &["GET", key]).as_deref() == Some(key.as_str()))
    });
    wait_until("group a to drop its copy", || {
        [0, 1]
            .iter()
            .all(|&node| call(&groups.node(node), &["DBSIZE"]).as_deref() == Some("0"))
    });
    assert!(
        call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"])
            .unwrap()
            .contains("moved_ranges 1")
    );
}

#[test]
fn a_rebalance_evens_out_the_slots_in_moves_while_a_client_keeps_writing() {
    let dir = tempfile::tempdir().unwrap();
    // Group a owns 512 slots more than its share: two moves of 256 even it out.
    let groups = TwoGroups::start_split(dir.path(), &[], 8704, |_, _| None);
    let keys: Vec<String> = (0..)
        .map(|n| format!("spread:{n}"))
        .filter(|key| (8192..8704).contains(&ruvio::routing::hash_slot(key.as_bytes())))
        .take(200)
        .collect();

    for key in &keys {
        assert_eq!(
            call(&groups.node(0), &["SET", key, key]).as_deref(),
            Some("OK")
        );
    }
    let coordinator = |args: &[&str]| call_with_secret(&groups.coordinator, args).unwrap();

    assert_eq!(
        coordinator(&["COORDINATOR", "REBALANCE", "DRYRUN"]),
        "8448-8703 a/0 -> b/0 8192-8447 a/0 -> b/0"
    );
    // A dry run moves nothing.
    assert!(coordinator(&["COORDINATOR", "STATUS"]).starts_with("state none moved_ranges 0"));

    // A client writes one key of the moving slots throughout, following MOVED and TRYAGAIN.
    let busy = key_in(8192..8704, "busy");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (busy, stop) = (busy.clone(), std::sync::Arc::clone(&stop));
        let mut address = groups.node(0);
        let b = groups.node(2);

        std::thread::spawn(move || {
            let mut acked = 0;

            for value in 1u64.. {
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                loop {
                    match call(&address, &["SET", &busy, &value.to_string()]).as_deref() {
                        Some("OK") => {
                            acked = value;
                            break;
                        }
                        Some(reply) if reply.starts_with("MOVED") => address = b.clone(),
                        Some(reply) if reply.starts_with("TRYAGAIN") => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        other => panic!("unexpected reply {other:?}"),
                    }
                }
            }
            acked
        })
    };

    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(coordinator(&["COORDINATOR", "REBALANCE"]), "2");
    assert!(
        coordinator(&["COORDINATOR", "REBALANCE"]).contains("in progress"),
        "a second rebalance waits for the first"
    );
    wait_until("the rebalance to finish", || {
        call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"]).is_some_and(|status| {
            status.contains("rebalance done rebalance_moves 2 rebalance_left 0")
        })
    });
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::Release);
    let acked = writer.join().unwrap();

    assert!(acked > 0);
    assert_eq!(
        call(&groups.node(2), &["GET", &busy]),
        Some(acked.to_string())
    );
    assert!(coordinator(&["COORDINATOR", "STATUS"]).contains("moved_ranges 2"));
    // Balanced now: nothing left to plan, and a rebalance has no move to make.
    assert_eq!(coordinator(&["COORDINATOR", "REBALANCE", "DRYRUN"]), "");
    assert_eq!(coordinator(&["COORDINATOR", "REBALANCE"]), "0");

    wait_until("every node to get the map with both moves", || {
        (0..4).all(|index| groups.version(index).as_deref() == Some("5"))
    });
    let b_port = groups.resp[2].port();

    for key in &keys {
        let slot = ruvio::routing::hash_slot(key.as_bytes());

        assert_eq!(
            call(&groups.node(2), &["GET", key]).as_deref(),
            Some(key.as_str())
        );
        assert_eq!(
            call(&groups.node(0), &["GET", key]),
            Some(format!("MOVED {slot} 127.0.0.1:{b_port}"))
        );
    }
    // b's replica took both ranges, although its map may have come only after the second
    // move began.
    wait_until("b's replica to hold both ranges", || {
        keys.iter()
            .all(|key| call(&groups.node(3), &["GET", key]).as_deref() == Some(key.as_str()))
    });
}

#[test]
fn a_cancel_ends_a_rebalance_with_its_move() {
    let dir = tempfile::tempdir().unwrap();
    // Slow copies (a few KiB per second) keep the first move copying when it is cancelled.
    let groups = TwoGroups::start_split(
        dir.path(),
        &[("RUVIO_MIGRATION_MAX_BYTES_PER_SEC", "4096")],
        8704,
        |_, _| None,
    );
    let keys: Vec<String> = (0..)
        .map(|n| format!("spread:{n}"))
        .filter(|key| (8448..8704).contains(&ruvio::routing::hash_slot(key.as_bytes())))
        .take(200)
        .collect();

    for key in &keys {
        assert_eq!(
            call(&groups.node(0), &["SET", key, key]).as_deref(),
            Some("OK")
        );
    }
    let coordinator = |args: &[&str]| call_with_secret(&groups.coordinator, args).unwrap();

    assert_eq!(coordinator(&["COORDINATOR", "REBALANCE"]), "2");
    wait_until("the source to copy", || {
        call(&groups.node(0), &["RUVIO", "MIGRATE", "STATUS"])
            .is_some_and(|status| status.contains("state copying"))
    });
    assert_eq!(coordinator(&["COORDINATOR", "CANCEL"]), "OK");
    wait_until("the rebalance to end", || {
        call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"])
            .is_some_and(|status| status.contains("rebalance cancelled"))
    });
    let status = coordinator(&["COORDINATOR", "STATUS"]);

    assert!(
        status.contains("moved_ranges 0 rebalance cancelled rebalance_moves 0"),
        "{status}"
    );
    for key in &keys {
        assert_eq!(
            call(&groups.node(0), &["GET", key]).as_deref(),
            Some(key.as_str())
        );
    }
    // Nothing runs any more; the plan is still there for the next rebalance.
    assert!(coordinator(&["COORDINATOR", "CANCEL"]).contains("no move in progress"));
    assert_eq!(
        coordinator(&["COORDINATOR", "REBALANCE", "DRYRUN"]),
        "8448-8703 a/0 -> b/0 8192-8447 a/0 -> b/0"
    );
}

#[test]
fn a_failover_of_the_source_during_a_move_starts_it_again_with_the_new_primary() {
    let dir = tempfile::tempdir().unwrap();
    // Slow copies (a few KiB per second) keep the first attempt copying when its source dies.
    let mut groups = TwoGroups::start(dir.path(), &[("RUVIO_MIGRATION_MAX_BYTES_PER_SEC", "4096")]);
    let keys: Vec<String> = (0..)
        .map(|n| format!("moving:{n}"))
        .filter(|key| (100..200).contains(&ruvio::routing::hash_slot(key.as_bytes())))
        .take(200)
        .collect();

    for key in &keys {
        assert_eq!(
            call(&groups.node(0), &["SET", key, key]).as_deref(),
            Some("OK")
        );
    }
    wait_until("a's replica to copy the keys", || {
        keys.iter()
            .all(|key| call(&groups.node(1), &["GET", key]).as_deref() == Some(key.as_str()))
    });
    let id = call_with_secret(
        &groups.coordinator,
        &["COORDINATOR", "MOVE", "100", "199", "b", "0"],
    )
    .unwrap();

    assert!(id.starts_with('m'), "{id}");
    wait_until("the source to copy", || {
        call(&groups.node(0), &["RUVIO", "MIGRATE", "STATUS"])
            .is_some_and(|status| status.contains("state copying"))
    });
    groups.processes[0].as_mut().unwrap().kill();

    wait_until("the move to finish with a's new primary", || {
        call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"])
            .is_some_and(|status| status.starts_with("state done"))
    });
    let status = call_with_secret(&groups.coordinator, &["COORDINATOR", "STATUS"]).unwrap();

    assert!(status.contains(&format!("id {id}-2")), "{status}");
    for key in &keys {
        assert_eq!(
            call(&groups.node(2), &["GET", key]).as_deref(),
            Some(key.as_str())
        );
    }
    wait_until("a's new primary to drop its copy", || {
        call(&groups.node(1), &["DBSIZE"]).as_deref() == Some("0")
    });
}

/// A TCP proxy in front of one data node for the coordinators only: cutting it leaves the
/// node reachable by clients and by its replicas, as a partition between it and the
/// coordinators would.
struct Proxy {
    cut: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    accepter: Option<std::thread::JoinHandle<()>>,
}

impl Proxy {
    fn start(listen: SocketAddr, target: SocketAddr) -> Self {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };

        let listener = std::net::TcpListener::bind(listen).unwrap();
        let cut = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let open: Arc<Mutex<Vec<std::net::TcpStream>>> = Arc::default();

        listener.set_nonblocking(true).unwrap();
        let accepter = {
            let (cut, stop) = (Arc::clone(&cut), Arc::clone(&stop));

            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if cut.load(Ordering::Acquire) {
                        for stream in open.lock().unwrap().drain(..) {
                            let _ = stream.shutdown(std::net::Shutdown::Both);
                        }
                    }
                    let Ok((client, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    };
                    if cut.load(Ordering::Acquire) {
                        continue;
                    }
                    let Ok(server) =
                        std::net::TcpStream::connect_timeout(&target, Duration::from_secs(1))
                    else {
                        continue;
                    };
                    let _ = client.set_nonblocking(false);
                    let mut open = open.lock().unwrap();

                    for (from, to) in [(&client, &server), (&server, &client)] {
                        let (Ok(mut from), Ok(mut to)) = (from.try_clone(), to.try_clone()) else {
                            continue;
                        };

                        std::thread::spawn(move || {
                            let _ = std::io::copy(&mut from, &mut to);
                            let _ = to.shutdown(std::net::Shutdown::Both);
                        });
                    }
                    open.push(client);
                    open.push(server);
                }
                for stream in open.lock().unwrap().drain(..) {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
            })
        };

        Self {
            cut,
            stop,
            accepter: Some(accepter),
        }
    }

    /// Drops every proxied connection and refuses new ones.
    fn cut(&self) {
        self.cut.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(accepter) = self.accepter.take() {
            let _ = accepter.join();
        }
    }
}

/// What a primary cut off from the coordinators did while they failed over.
struct CutResult {
    /// The last write the old primary took, and its first refusal, after the cut.
    last_taken: Option<Instant>,
    first_refused: Option<Instant>,
    /// When a coordinator was first seen naming another primary.
    promoted: Instant,
    cut: Instant,
}

fn info_text(address: &str, name: &str) -> Option<String> {
    call(address, &["INFO", "replication"])?
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
        .map(|value| value.trim().to_owned())
}

/// Three nodes and three coordinators that reach node 0 only through a proxy. Once node 0
/// takes writes the proxy is cut; a client keeps writing to node 0 directly while the
/// coordinators fail over. With `lease_ms`, every node runs `primary-lease` and the
/// coordinators grant it.
fn primary_cut_from_the_coordinators(lease_ms: Option<u64>) -> CutResult {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let ports = test_ports::reserve();
    let proxy = Proxy::start(ports.address_at(0), group.resp[0]);
    let env = [(
        "RUVIO_PRIMARY_LEASE",
        if lease_ms.is_some() { "yes" } else { "no" },
    )];
    let _nodes = [
        group.node_with(dir.path(), 0, None, &env),
        group.node_with(dir.path(), 1, Some(0), &env),
        group.node_with(dir.path(), 2, Some(0), &env),
    ];
    let extra = lease_ms.map_or_else(String::new, |ms| {
        format!("primary_lease_ms = {ms}\nprimary_lease_drift_percent = 5")
    });
    let mut addresses = group.resp.clone();

    addresses[0] = ports.address_at(0);
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator_with(dir.path(), index, &extra, &addresses, None))
        .collect();
    let primary = group.resp[0].to_string();
    // The coordinators name node 0 by the proxy's address.
    let named = |index: usize| {
        let reply = call_with_secret(
            &group.coordinators[index].to_string(),
            &["SENTINEL", "get-master-addr-by-name", "main"],
        )?;
        let port: u16 = reply.split_whitespace().nth(1)?.parse().ok()?;

        addresses.iter().position(|address| address.port() == port)
    };

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| named(index) == Some(0))
    });
    // With the lease, the primary takes writes once a majority granted it.
    wait_until("the primary to take writes", || {
        call(&primary, &["SET", "before", "1"]).as_deref() == Some("OK")
    });
    let lease_state = info_text(&primary, "primary_lease");

    assert_eq!(
        lease_state.as_deref(),
        Some(if lease_ms.is_some() { "on" } else { "off" })
    );

    proxy.cut();
    let cut = Instant::now();
    let mut result = CutResult {
        last_taken: None,
        first_refused: None,
        promoted: cut,
        cut,
    };
    let deadline = cut + PATIENCE;
    let mut writes = 0u64;

    loop {
        assert!(Instant::now() < deadline, "no failover after the cut");
        // A reply that does not arrive (a slow machine) says nothing either way.
        if let Some(reply) = call(&primary, &["SET", "during", &writes.to_string()]) {
            let at = Instant::now();

            if reply == "OK" {
                result.last_taken = Some(at);
            } else {
                assert!(reply.starts_with("NOREPLICAS Primary lease"), "{reply}");
                result.first_refused.get_or_insert(at);
            }
        }
        writes += 1;
        if (0..3).any(|index| named(index).is_some_and(|node| node != 0)) {
            result.promoted = Instant::now();
            break;
        }
    }
    // After the failover the old primary still answers as it did.
    let mut reply = None;

    wait_until("the old primary to answer a write", || {
        reply = call(&primary, &["SET", "after", "1"]);
        reply.is_some()
    });
    let reply = reply.unwrap_or_default();

    if lease_ms.is_some() {
        assert!(reply.starts_with("NOREPLICAS Primary lease"), "{reply}");
        // The promoted replica takes writes once the coordinators grant it the lease.
        let promoted = (0..3)
            .find_map(|index| named(index).filter(|&node| node != 0))
            .unwrap();

        wait_until("the new primary to take writes", || {
            call(&group.resp[promoted].to_string(), &["SET", "new", "1"]).as_deref() == Some("OK")
        });
        eprintln!(
            "the new primary took writes {:?} after the cut",
            cut.elapsed()
        );
    } else {
        assert_eq!(
            reply, "OK",
            "without the lease the old primary keeps writing"
        );
    }
    eprintln!(
        "lease {lease_ms:?}: last write taken {:?}, first refused {:?}, promotion seen {:?} after the cut",
        result.last_taken.map(|at| at - cut),
        result.first_refused.map(|at| at - cut),
        result.promoted - cut
    );
    result
}

#[test]
fn with_the_lease_a_primary_cut_from_the_coordinators_stops_writing_before_the_failover() {
    // ping 200 ms, down_after 1000 ms: a 700 ms lease with 5% drift counts as ended 974 ms
    // after a grant (700 * 105 / 95, rounded up, plus 200).
    let result = primary_cut_from_the_coordinators(Some(700));

    // Every write the old primary took came before any coordinator named another primary,
    // and it refused writes once one did.
    assert!(
        result
            .last_taken
            .is_none_or(|taken| taken < result.promoted),
        "a write was taken {:?} after the cut, promotion seen {:?}",
        result.last_taken.map(|at| at - result.cut),
        result.promoted - result.cut
    );
    if let Some(refused) = result.first_refused {
        assert!(
            result.last_taken.is_none_or(|taken| taken < refused),
            "a write was taken after the first refusal"
        );
    }
}

#[test]
fn without_the_lease_a_primary_cut_from_the_coordinators_keeps_writing() {
    let result = primary_cut_from_the_coordinators(None);

    assert!(result.first_refused.is_none());
    assert!(result.last_taken.is_some());
}

/// With the lease, a member that accepts connections but never answers (a stalled host)
/// must not hold up the renewal of the primary's lease: the primary keeps taking writes.
#[test]
fn a_stalled_member_does_not_hold_up_the_primary_lease() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
    let env = [("RUVIO_PRIMARY_LEASE", "yes")];
    let _nodes = [
        group.node_with(dir.path(), 0, None, &env),
        group.node_with(dir.path(), 1, Some(0), &env),
    ];
    // Member 2's address: connections are queued by the kernel and never read.
    let _stalled = std::net::TcpListener::bind(group.resp[2]).unwrap();
    // A look at the stalled member takes a full second (the coordinators' call timeout), so
    // pings of the group go out about once a second. Renewals come every 200 ms on their own:
    // with a 1100 ms lease the gate closes 800 ms after a renewal, so a renewal that waited for
    // the pings would close it for about 200 ms each round, while renewals on time leave 600 ms
    // to spare for a loaded machine.
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| {
            group.coordinator_with(
                dir.path(),
                index,
                "primary_lease_ms = 1100\nprimary_lease_drift_percent = 5\ndown_after_ms = 1500",
                &group.resp,
                None,
            )
        })
        .collect();
    let primary = group.resp[0].to_string();

    wait_until("the primary to take writes", || {
        call(&primary, &["SET", "before", "1"]).as_deref() == Some("OK")
    });
    let until = Instant::now() + Duration::from_secs(4);
    let mut writes = 0u64;

    while Instant::now() < until {
        if let Some(reply) = call(&primary, &["SET", "during", &writes.to_string()]) {
            assert_eq!(reply, "OK", "write {writes} refused");
        }
        writes += 1;
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(writes > 20, "only {writes} writes in four seconds");
}

/// A data node that answers what the coordinators ask when they look at it (`ROLE` as a
/// replica of nobody in the group, one shard, map version 3) and never answers anything else,
/// such as `REPLICAOF`: a member stuck after it was seen.
struct StandIn {
    address: SocketAddr,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl StandIn {
    fn start(address: SocketAddr) -> Self {
        let listener = std::net::TcpListener::bind(address).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = std::sync::Arc::clone(&stop);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopped.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let stopped = std::sync::Arc::clone(&stopped);

                std::thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                    let mut writer = stream.try_clone().unwrap();
                    let mut reader = BufReader::new(stream);

                    while !stopped.load(std::sync::atomic::Ordering::SeqCst) {
                        let Some(words) = read_command(&mut reader) else {
                            continue;
                        };
                        let words: Vec<String> =
                            words.iter().map(|word| word.to_ascii_uppercase()).collect();
                        let reply: &[u8] = match words.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                            ["PING"] => b"+PONG\r\n",
                            ["ROLE"] => {
                                b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:1\r\n$9\r\nconnected\r\n:0\r\n"
                            }
                            ["CONFIG", "GET", "SHARDS"] => b"*2\r\n$6\r\nshards\r\n$1\r\n1\r\n",
                            ["INFO", ..] => b"$0\r\n\r\n",
                            ["RUVIO", "TOPOLOGY", "VERSION"] => b":3\r\n",
                            // Stuck: read on, answer nothing.
                            _ => continue,
                        };
                        if writer.write_all(reply).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self { address, stop }
    }
}

impl Drop for StandIn {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        // Wakes the accept loop so it sees the flag and drops the listener.
        let _ = std::net::TcpStream::connect_timeout(&self.address, Duration::from_millis(300));
    }
}

/// One RESP command (an array of bulk strings); `None` on a timeout or anything else.
fn read_command(reader: &mut impl BufRead) -> Option<Vec<String>> {
    let mut line = String::new();

    reader.read_line(&mut line).ok()?;
    let count: usize = line.trim_end().strip_prefix('*')?.parse().ok()?;
    let mut words = Vec::with_capacity(count);

    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).ok()?;
        let length: usize = line.trim_end().strip_prefix('$')?.parse().ok()?;
        let mut word = vec![0; length + 2];

        reader.read_exact(&mut word).ok()?;
        word.truncate(length);
        words.push(String::from_utf8_lossy(&word).into_owned());
    }
    Some(words)
}

/// Each group is tended on its own task: a member of group `a` that answers the coordinator's
/// looks but never its `REPLICAOF` holds up only group `a`, and group `b` still fails over
/// within about `down_after`. Before, one such member held every group for ten seconds a tick.
#[test]
fn a_stuck_member_of_one_group_does_not_delay_a_failover_in_another() {
    let dir = tempfile::tempdir().unwrap();
    // Node 1, group a's replica, is stuck.
    let mut groups = TwoGroups::start_with(dir.path(), &[], |index, address| {
        (index == 1).then(|| StandIn::start(address))
    });

    // A few ticks in which the leader tries to point the stuck member at group a's primary.
    std::thread::sleep(Duration::from_millis(600));
    let killed = Instant::now();

    groups.processes[2].as_mut().unwrap().kill();
    wait_until("group b's replica to take over", || {
        call(&groups.node(3), &["ROLE"]).is_some_and(|role| role.starts_with("master"))
    });
    let took = killed.elapsed();

    // down_after is 1 s; a tick, the coordinators' verdict and the promotion fit in the rest.
    assert!(
        took < Duration::from_secs(4),
        "group b failed over {took:?} after its primary died"
    );
}

/// A primary one of whose shards the coordinators cannot reach is not heard from: they fail it
/// over as if it were down, and do not promote a replica with a shard they cannot reach.
#[test]
fn a_primary_with_one_shard_cut_off_is_failed_over() {
    primary_with_one_shard_cut_off(false);
}

/// With the lease, the coordinators stop renewing such a primary, so its lease ends and the
/// failover goes ahead.
#[test]
fn with_the_lease_a_primary_with_one_shard_cut_off_is_failed_over() {
    primary_with_one_shard_cut_off(true);
}

fn primary_with_one_shard_cut_off(lease: bool) {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::with_shards(2);
    let ports = test_ports::reserve();
    let shard =
        |address: SocketAddr, offset: u16| SocketAddr::new(address.ip(), address.port() + offset);
    // The coordinators reach node 0's two shards through these.
    let proxies = [
        Proxy::start(ports.address_at(0), group.resp[0]),
        Proxy::start(ports.address_at(1), shard(group.resp[0], 1)),
    ];
    let env = [("RUVIO_PRIMARY_LEASE", if lease { "yes" } else { "no" })];
    let extra = if lease {
        "primary_lease_ms = 700\nprimary_lease_drift_percent = 5"
    } else {
        ""
    };
    let _nodes = [
        group.node_with(dir.path(), 0, None, &env),
        group.node_with(dir.path(), 1, Some(0), &env),
        group.node_with(dir.path(), 2, Some(0), &env),
    ];
    let mut addresses = group.resp.clone();

    addresses[0] = ports.address_at(0);
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator_with(dir.path(), index, extra, &addresses, None))
        .collect();

    wait_until("the coordinators to agree on node 0", || {
        group.primary_among(0, &addresses) == Some(0)
    });
    // Shard 0 still answers through its proxy; shard 1 no longer does.
    proxies[1].cut();
    wait_until("a replica to be promoted", || {
        matches!(group.primary_among(0, &addresses), Some(1 | 2))
    });
    let promoted = group.primary_among(0, &addresses).unwrap();

    wait_until("the promoted replica to take writes on both shards", || {
        (0..2).all(|offset| {
            call(&shard(group.resp[promoted], offset).to_string(), &["ROLE"])
                .is_some_and(|role| role.starts_with("master"))
        })
    });
}

/// A coordinator joins running ones with `COORDINATOR ADD` and one leaves with `COORDINATOR
/// REMOVE`; failovers need a majority of the coordinators as they are now, not as `peers`
/// listed them at the first start.
#[test]
fn coordinators_are_added_and_removed_while_they_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut group = Group::new();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];
    let mut coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });

    // A fourth coordinator (id 4) starts with `join = true` and is added.
    group.coordinators.push(group.spare);
    coordinators.push(group.coordinator_with(dir.path(), 3, "join = true", &group.resp, None));
    let spare = group.spare.to_string();

    wait_until("the leader to add coordinator 4", || {
        group
            .on_leader(&["COORDINATOR", "ADD", "4", &spare])
            .as_deref()
            == Some("OK")
    });
    wait_until(
        "every coordinator to count four and the new one to know the primary",
        || {
            group.primary_by(3) == Some(0)
                && group.coordinators.iter().all(|coordinator| {
                    call_with_secret(&coordinator.to_string(), &["INFO"]).is_some_and(|info| {
                        info.lines()
                            .find_map(|line| line.strip_prefix("coordinators:"))
                            .is_some_and(|list| list.split(',').count() == 4)
                    })
                })
        },
    );

    // Two of four are no majority: with coordinators 2 and 3 down, a dead primary stays.
    coordinators[1].kill();
    coordinators[2].kill();
    nodes[0].kill();
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(group.primary_by(0), Some(0));
    assert_eq!(group.primary_by(3), Some(0));

    // Three of four are: coordinator 2 returns and the group fails over.
    coordinators[1] = group.coordinator(dir.path(), 1);
    wait_until("a failover with three of four coordinators", || {
        group.primary_by(0).is_some_and(|node| node != 0)
    });
    let first = group.primary_by(0).unwrap();

    wait_until("every live coordinator to name the new primary", || {
        [0, 1, 3]
            .iter()
            .all(|&index| group.primary_by(index) == Some(first))
    });

    // Coordinator 3, still down, is removed: three vote, so two of them are a majority.
    wait_until("the leader to remove coordinator 3", || {
        group.on_leader(&["COORDINATOR", "REMOVE", "3"]).as_deref() == Some("OK")
    });
    coordinators[0].kill();
    nodes[first].kill();
    wait_until("a failover with two of three coordinators", || {
        group
            .primary_by(3)
            .is_some_and(|node| node != 0 && node != first)
    });
}

/// A replica the coordinators' file does not list is found through its primary's `INFO
/// replication`, watched, and promoted when it is the only one left; a member the file lists
/// cannot be forgotten.
#[test]
fn a_replica_started_later_is_found_and_can_be_promoted() {
    let dir = tempfile::tempdir().unwrap();
    let mut group = Group::new();

    group.listed = 2;
    let mut nodes = vec![
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
    ];
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| group.coordinator(dir.path(), index))
        .collect();

    wait_until("the coordinators to agree on the primary", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });
    // Node 2 is started following node 0; nobody tells the coordinators.
    nodes.push(group.node(dir.path(), 2, Some(0)));
    let third = group.resp[2].to_string();

    wait_until("every coordinator to list node 2 as a replica", || {
        group.coordinators.iter().all(|coordinator| {
            call_with_secret(&coordinator.to_string(), &["SENTINEL", "replicas", "main"])
                .is_some_and(|replicas| replicas.contains(&format!("name {third} ")))
        })
    });
    assert_eq!(
        call(&group.resp[0].to_string(), &["SET", "found", "1"]).as_deref(),
        Some("OK")
    );
    wait_until("node 2 to copy the write", || {
        call(&third, &["GET", "found"]).as_deref() == Some("1")
    });

    // Every node reports a process identity, the coordinators' check for one node under two
    // addresses.
    let identity = |node: usize| {
        call(&group.resp[node].to_string(), &["INFO", "server"])
            .and_then(|info| {
                info.lines()
                    .find_map(|line| line.strip_prefix("run_id:").map(str::to_owned))
            })
            .unwrap()
    };

    assert_eq!(identity(2).len(), 40);
    assert_ne!(identity(1), identity(2));

    // Only node 2 is left: it takes over.
    nodes[0].kill();
    nodes[1].kill();
    wait_until("node 2 to be promoted", || {
        (0..3).all(|index| group.primary_by(index) == Some(2))
    });
    wait_until("node 2 to take writes", || {
        call(&third, &["SET", "after", "1"]).as_deref() == Some("OK")
    });

    let first = group.resp[0].to_string();
    let refused = group
        .on_leader(&["COORDINATOR", "FORGET", "main", &first])
        .unwrap();

    assert!(refused.contains("listed in the file"), "{refused}");
    let refused = group
        .on_leader(&["COORDINATOR", "FORGET", "main", &third])
        .unwrap();

    assert!(refused.contains("is the primary"), "{refused}");
}

/// A test CA and one certificate it signed for 127.0.0.1, used by every process of a group.
struct TestTls {
    ca: std::path::PathBuf,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    client: std::sync::Arc<rustls::ClientConfig>,
}

impl TestTls {
    fn new(dir: &Path) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(vec!["Ruvio test CA".into()]).unwrap();

        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["127.0.0.1".into()])
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        let tls = |name: &str| dir.join(name);

        std::fs::write(tls("ca.crt"), ca_cert.pem()).unwrap();
        std::fs::write(tls("node.crt"), cert.pem()).unwrap();
        std::fs::write(tls("node.key"), key.serialize_pem()).unwrap();
        let mut roots = rustls::RootCertStore::empty();

        roots.add(ca_cert.der().clone()).unwrap();
        Self {
            ca: tls("ca.crt"),
            cert: tls("node.crt"),
            key: tls("node.key"),
            client: std::sync::Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ),
        }
    }

    /// The coordinator settings: TLS on its port, to the other coordinators and to the nodes.
    fn coordinator_settings(&self) -> String {
        format!(
            "tls_cert = \"{}\"\ntls_key = \"{}\"\ntls_ca = \"{}\"\nnode_tls = true",
            self.cert.display(),
            self.key.display(),
            self.ca.display()
        )
    }

    /// Several commands on one TLS connection, each reply as `call` gives it.
    fn calls(&self, address: &str, commands: &[&[&str]]) -> Option<Vec<String>> {
        let stream = std::net::TcpStream::connect_timeout(
            &address.parse().ok()?,
            Duration::from_millis(300),
        )
        .ok()?;

        stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .ok()?;
        let name = rustls::pki_types::ServerName::try_from("127.0.0.1").ok()?;
        let connection =
            rustls::ClientConnection::new(std::sync::Arc::clone(&self.client), name).ok()?;
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let mut frame = String::new();

        for args in commands {
            frame.push_str(&format!("*{}\r\n", args.len()));
            for arg in *args {
                frame.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
            }
        }
        stream.write_all(frame.as_bytes()).ok()?;
        stream.flush().ok()?;
        let mut reader = BufReader::new(stream);

        commands.iter().map(|_| read_reply(&mut reader)).collect()
    }
}

/// Coordinators that speak TLS among themselves, to clients and to data nodes that accept
/// nothing else still agree on the primary and fail it over.
#[test]
fn coordinators_fail_over_over_tls() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::with_tls(dir.path());
    let tls = group.tls.as_ref().unwrap();
    let mut nodes = [
        group.node(dir.path(), 0, None),
        group.node(dir.path(), 1, Some(0)),
        group.node(dir.path(), 2, Some(0)),
    ];

    // The nodes take no clear text: a coordinator reaches them only through TLS.
    assert_eq!(call(&group.resp[0].to_string(), &["PING"]), None);
    assert_eq!(
        group.call_node(0, &["SET", "before", "1"]).as_deref(),
        Some("OK")
    );
    let _coordinators: Vec<Process> = (0..3)
        .map(|index| {
            group.coordinator_with(
                dir.path(),
                index,
                &tls.coordinator_settings(),
                &group.resp,
                None,
            )
        })
        .collect();

    wait_until("the coordinators to agree on the primary over TLS", || {
        (0..3).all(|index| group.primary_by(index) == Some(0))
    });
    // Neither the Sentinel API nor the Raft calls answer in clear text.
    assert_eq!(call(&group.coordinators[0].to_string(), &["PING"]), None);

    nodes[0].kill();
    let killed = Instant::now();

    wait_until("a failover over TLS", || {
        group.primary_by(0).is_some_and(|node| node != 0)
    });
    let promoted = group.primary_by(0).unwrap();

    wait_until("the new primary to take writes", || {
        group.call_node(promoted, &["SET", "after", "1"]).as_deref() == Some("OK")
    });
    assert!(
        killed.elapsed() < Duration::from_secs(15),
        "failover took {:?}",
        killed.elapsed()
    );
    wait_until("the other replica to follow the new primary", || {
        group.call_node(3 - promoted, &["GET", "after"]).as_deref() == Some("1")
    });
}
