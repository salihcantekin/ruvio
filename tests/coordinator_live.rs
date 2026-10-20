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
    _ports: (test_ports::PortBlock, test_ports::PortBlock),
}

impl Group {
    fn new() -> Self {
        let data = test_ports::reserve();
        let replication = test_ports::reserve();

        Self {
            resp: (0..3).map(|index| data.address_at(index)).collect(),
            replication: (0..3).map(|index| replication.address_at(index)).collect(),
            coordinators: (3..6).map(|index| data.address_at(index)).collect(),
            _ports: (data, replication),
        }
    }

    fn node(&self, dir: &Path, index: usize, follow: Option<usize>) -> Process {
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
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match follow {
            Some(primary) => command.env("RUVIO_REPLICATION_ROLE", "replica").env(
                "RUVIO_REPLICATION_PRIMARY",
                self.replication[primary].to_string(),
            ),
            None => command.env("RUVIO_REPLICATION_ROLE", "primary"),
        };
        let process = Process(command.spawn().unwrap());

        wait_until("a data node to answer", || {
            ping(&self.resp[index].to_string())
        });
        process
    }

    fn coordinator(&self, dir: &Path, index: usize) -> Process {
        let config = dir.join(format!("coordinator{index}.toml"));
        let peers = self
            .coordinators
            .iter()
            .enumerate()
            .map(|(id, address)| format!("\"{}@{address}\"", id + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let members = (0..3)
            .map(|node| {
                format!(
                    "  {{ address = \"{}\", replication = \"{}\" }}",
                    self.resp[node], self.replication[node]
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");

        std::fs::write(
            &config,
            format!(
                "id = {}\nlisten = \"{}\"\ndata_dir = \"{}\"\npeers = [{peers}]\nsecret = \"{SECRET}\"\n\
                 ping_interval_ms = 200\ndown_after_ms = 1000\n\n[[groups]]\nname = \"main\"\n\
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
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    /// The primary coordinator `index` names, as an index into the data nodes.
    fn primary_by(&self, index: usize) -> Option<usize> {
        let reply = call_with_secret(
            &self.coordinators[index].to_string(),
            &["SENTINEL", "get-master-addr-by-name", "main"],
        )?;
        let port: u16 = reply.split_whitespace().nth(1)?.parse().ok()?;

        self.resp.iter().position(|address| address.port() == port)
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
fn one_coordinator_without_a_majority_does_not_fail_over() {
    let dir = tempfile::tempdir().unwrap();
    let group = Group::new();
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
    coordinators[1].kill();
    coordinators[2].kill();
    nodes[0].kill();
    // Several times down_after_ms: a lone coordinator must not act.
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(group.primary_by(0), Some(0));
    for node in 1..3 {
        assert!(
            call(&group.resp[node].to_string(), &["SET", "x", "1"])
                .unwrap()
                .starts_with("READONLY")
        );
    }
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

/// Group `a` (nodes 0 and 1) owns slots 0-8191, group `b` (nodes 2 and 3) the rest; one
/// shard each, node 1 and 3 replicas; one coordinator.
struct TwoGroups {
    resp: Vec<SocketAddr>,
    coordinator: String,
    processes: Vec<Process>,
    _ports: (test_ports::PortBlock, test_ports::PortBlock),
}

impl TwoGroups {
    /// `env`: extra settings of every data node.
    fn start(dir: &Path, env: &[(&str, &str)]) -> Self {
        let data = test_ports::reserve();
        let replication = test_ports::reserve();
        let resp: Vec<SocketAddr> = (0..4).map(|index| data.address_at(index)).collect();
        let repl: Vec<SocketAddr> = (0..4).map(|index| replication.address_at(index)).collect();
        let coordinator = data.address_at(4);
        let topology = dir.join("topology.toml");

        std::fs::write(
            &topology,
            [("a", "0-8191", 0), ("b", "8192-16383", 2)].iter().fold(
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
        let mut processes: Vec<Process> = (0..4)
            .map(|index| {
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
                process
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
                 [[groups]]\nname = \"a\"\nshards = 1\nslots = [\"0-8191\"]\nmembers = [{}]\n\n\
                 [[groups]]\nname = \"b\"\nshards = 1\nslots = [\"8192-16383\"]\nmembers = [{}]\n",
                dir.join("coordinator").display(),
                members(0),
                members(2),
            ),
        )
        .unwrap();
        processes.push(Process(
            Command::new(env!("CARGO_BIN_EXE_ruvio"))
                .arg("coordinator")
                .arg("--config")
                .arg(&config)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        ));

        let groups = Self {
            resp,
            coordinator: coordinator.to_string(),
            processes,
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
                            retried += 1;
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        other => panic!("unexpected reply {other:?}"),
                    }
                }
            }
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
    groups.processes[0].kill();

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
