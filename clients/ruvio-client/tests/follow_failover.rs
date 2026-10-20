//! A standalone client with several seeds finds the primary again after a failover, against
//! scripted nodes that answer `ROLE`, refuse writes with `READONLY` when they are replicas, and
//! (for the coordinator case) name a primary with `SENTINEL get-master-addr-by-name`.
//! `tests/failover_live.rs` does the same against real servers.

use std::io::{BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ruvio_client::__bench::parse;
use ruvio_client::{Client, ClientOptions, Error, RespValue};

#[derive(Clone)]
enum Mode {
    Master,
    /// A replica; it reports this primary in `ROLE`.
    Replica(Option<u16>),
    /// Says `master` but refuses writes anyway.
    Liar,
    /// A replica that is promoted right after it answers its first `ROLE`.
    Promoted,
    /// A coordinator that names this primary.
    Names(u16),
}

struct Node {
    port: u16,
    mode: Arc<Mutex<Mode>>,
    commands: Arc<Mutex<Vec<Vec<String>>>>,
    streams: Arc<Mutex<Vec<TcpStream>>>,
    stop: Arc<AtomicBool>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.drop_connections();
    }
}

impl Node {
    fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let node = Self {
            port: listener.local_addr().unwrap().port(),
            mode: Arc::new(Mutex::new(mode)),
            commands: Arc::default(),
            streams: Arc::default(),
            stop: Arc::default(),
        };
        let (mode, commands, streams, stop) = (
            Arc::clone(&node.mode),
            Arc::clone(&node.commands),
            Arc::clone(&node.streams),
            Arc::clone(&node.stop),
        );

        listener.set_nonblocking(true).unwrap();
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        socket.set_nonblocking(false).unwrap();
                        streams.lock().unwrap().push(socket.try_clone().unwrap());
                        let (mode, commands) = (Arc::clone(&mode), Arc::clone(&commands));

                        thread::spawn(move || serve(socket, &mode, &commands));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(2)),
                }
            }
        });

        node
    }

    /// An address nothing listens on.
    fn dead() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn become_mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn drop_connections(&self) {
        for stream in self.streams.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn seen(&self, name: &str) -> usize {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command[0].eq_ignore_ascii_case(name))
            .count()
    }

    fn total(&self) -> usize {
        self.commands.lock().unwrap().len()
    }
}

fn serve(socket: TcpStream, mode: &Mutex<Mode>, commands: &Mutex<Vec<Vec<String>>>) {
    let mut writer = socket.try_clone().unwrap();
    let mut reader = BufReader::new(socket);

    while let Ok(RespValue::Array(items)) = parse(&mut reader) {
        let command: Vec<String> = items
            .into_iter()
            .map(|item| {
                String::from_utf8_lossy(&item.into_bytes().unwrap_or_default()).into_owned()
            })
            .collect();
        let reply = {
            let mut mode = mode.lock().unwrap();
            let reply = answer(&command, &mode);

            if matches!(*mode, Mode::Promoted) && command[0].eq_ignore_ascii_case("ROLE") {
                *mode = Mode::Master;
            }

            reply
        };

        commands.lock().unwrap().push(command);

        if writer.write_all(&reply).is_err() {
            return;
        }
    }
}

fn answer(command: &[String], mode: &Mode) -> Vec<u8> {
    let name = command[0].to_ascii_uppercase();
    let writes = matches!(name.as_str(), "SET" | "INCR" | "DEL");

    match (name.as_str(), mode) {
        ("ROLE", Mode::Master | Mode::Liar) => b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n".to_vec(),
        ("ROLE", Mode::Promoted) => {
            b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:0\r\n$9\r\nconnected\r\n:0\r\n".to_vec()
        }
        ("ROLE", Mode::Replica(primary)) => format!(
            "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:0\r\n",
            primary.unwrap_or(0)
        )
        .into_bytes(),
        ("SENTINEL", Mode::Names(port)) => {
            let port = port.to_string();

            format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n", port.len()).into_bytes()
        }
        (_, Mode::Replica(_) | Mode::Liar | Mode::Promoted) if writes => {
            b"-READONLY You can't write against a read only replica.\r\n".to_vec()
        }
        ("PING", _) => b"+PONG\r\n".to_vec(),
        ("GET", _) => b"$-1\r\n".to_vec(),
        ("INCR", _) => b":1\r\n".to_vec(),
        ("EXEC", _) => b"*0\r\n".to_vec(),
        _ => b"+OK\r\n".to_vec(),
    }
}

fn options(ports: &[u16]) -> ClientOptions {
    ClientOptions {
        seeds: ports
            .iter()
            .map(|port| ("127.0.0.1".to_owned(), *port))
            .collect(),
        discover_cluster: false,
        connect_timeout: Duration::from_secs(1),
        command_timeout: Some(Duration::from_secs(5)),
        reconnect_base_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        ..ClientOptions::default()
    }
}

fn readonly(result: Result<impl std::fmt::Debug, Error>) {
    match result {
        Err(Error::Server(message)) => assert!(message.starts_with("READONLY"), "{message}"),
        other => panic!("expected READONLY, got {other:?}"),
    }
}

#[test]
fn a_readonly_write_follows_the_new_primary_and_is_sent_once_more() {
    let old = Node::start(Mode::Master);
    let new = Node::start(Mode::Replica(None));
    let mut client = Client::connect_with(options(&[old.port, new.port])).unwrap();

    client.set("k", "1").unwrap();
    assert_eq!((old.seen("SET"), new.seen("SET")), (1, 0));

    old.become_mode(Mode::Replica(Some(new.port)));
    new.become_mode(Mode::Master);

    client.set("k", "2").unwrap();

    // The refused write was sent once to the old node, then once to the new primary.
    assert_eq!((old.seen("SET"), new.seen("SET")), (2, 1));

    client.set("k", "3").unwrap();
    assert_eq!((old.seen("SET"), new.seen("SET")), (2, 2));
}

#[test]
fn a_healthy_connection_sends_nothing_extra() {
    let node = Node::start(Mode::Master);
    let other = Node::start(Mode::Replica(None));
    let mut client = Client::connect_with(options(&[node.port, other.port])).unwrap();

    for _ in 0..50 {
        client.set("k", "v").unwrap();
    }

    assert_eq!(node.total(), 50);
    assert_eq!(other.total(), 0);
}

#[test]
fn a_replica_names_its_primary_when_that_is_no_seed() {
    let replica = Node::start(Mode::Master);
    let hidden = Node::start(Mode::Master);
    let dead = Node::dead();
    let mut client = Client::connect_with(options(&[replica.port, dead])).unwrap();

    replica.become_mode(Mode::Replica(Some(hidden.port)));

    client.set("k", "v").unwrap();

    assert_eq!((replica.seen("SET"), hidden.seen("SET")), (1, 1));
}

#[test]
fn follow_primary_off_returns_readonly_and_sends_no_role() {
    let node = Node::start(Mode::Master);
    let other = Node::start(Mode::Master);
    let mut client = Client::connect_with(ClientOptions {
        follow_primary: false,
        ..options(&[node.port, other.port])
    })
    .unwrap();

    node.become_mode(Mode::Replica(Some(other.port)));

    readonly(client.set("k", "v"));
    assert_eq!((node.seen("ROLE"), other.total()), (0, 0));
}

#[test]
fn a_reconnect_takes_the_node_that_says_master_not_the_next_seed() {
    let first = Node::start(Mode::Master);
    let second = Node::start(Mode::Master);
    let third = Node::start(Mode::Master);
    let mut client = Client::connect_with(options(&[first.port, second.port, third.port])).unwrap();

    client.ping().unwrap();

    // The second seed became a replica while the connection was down.
    second.become_mode(Mode::Replica(Some(third.port)));
    first.become_mode(Mode::Replica(Some(third.port)));
    first.drop_connections();

    assert!(matches!(client.ping(), Err(Error::Io(_))));

    client.set("k", "v").unwrap();

    assert_eq!(
        (first.seen("SET"), second.seen("SET"), third.seen("SET")),
        (0, 0, 1)
    );
}

#[test]
fn a_reconnect_without_any_primary_still_connects_to_the_first_seed_that_answers() {
    let first = Node::start(Mode::Master);
    let second = Node::start(Mode::Master);
    let mut client = Client::connect_with(options(&[first.port, second.port])).unwrap();

    client.ping().unwrap();
    first.become_mode(Mode::Replica(None));
    second.become_mode(Mode::Replica(None));
    first.drop_connections();

    assert!(matches!(client.ping(), Err(Error::Io(_))));
    assert_eq!(client.ping().unwrap(), "PONG");
}

#[test]
fn without_any_primary_the_readonly_error_comes_back_within_the_backoff() {
    let first = Node::start(Mode::Master);
    let second = Node::start(Mode::Master);
    let mut client = Client::connect_with(ClientOptions {
        max_reconnect_attempts: 3,
        ..options(&[first.port, second.port])
    })
    .unwrap();

    first.become_mode(Mode::Replica(None));
    second.become_mode(Mode::Replica(None));

    let started = Instant::now();

    readonly(client.set("k", "v"));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!((first.seen("SET"), second.seen("SET")), (1, 0));
}

#[test]
fn a_write_refused_twice_is_not_sent_a_third_time() {
    let old = Node::start(Mode::Master);
    let liar = Node::start(Mode::Master);
    let mut client = Client::connect_with(options(&[old.port, liar.port])).unwrap();

    old.become_mode(Mode::Replica(None));
    liar.become_mode(Mode::Liar);

    readonly(client.set("k", "v"));
    assert_eq!((old.seen("SET"), liar.seen("SET")), (1, 1));
}

#[test]
fn the_refusing_replica_is_taken_once_it_is_promoted() {
    let replica = Node::start(Mode::Promoted);
    let dead = Node::dead();
    let mut client = Client::connect_with(options(&[replica.port, dead])).unwrap();

    // The first round finds it a replica and no other primary; the next round finds it
    // promoted and sends the write there again.
    client.set("k", "v").unwrap();

    assert_eq!(replica.seen("SET"), 2);
    assert_eq!(replica.seen("ROLE"), 2);
}

#[test]
fn a_coordinator_naming_the_refusing_replica_is_followed_once_it_is_promoted() {
    let coordinator = Node::start(Mode::Names(0));
    let node = Node::start(Mode::Master);

    coordinator.become_mode(Mode::Names(node.port));

    let mut client = Client::connect_with(ClientOptions {
        sentinel_master: Some("main".to_owned()),
        ..options(&[coordinator.port])
    })
    .unwrap();

    node.become_mode(Mode::Promoted);

    client.set("k", "v").unwrap();
    assert_eq!(node.seen("SET"), 2);
}

#[test]
fn a_refusing_node_that_still_says_master_is_not_taken_again() {
    let liar = Node::start(Mode::Liar);
    let other = Node::start(Mode::Replica(None));
    let mut client = Client::connect_with(ClientOptions {
        max_reconnect_attempts: 3,
        ..options(&[liar.port, other.port])
    })
    .unwrap();

    readonly(client.set("k", "v"));
    assert_eq!((liar.seen("SET"), liar.seen("ROLE")), (1, 3));
}

#[test]
fn a_write_refused_inside_a_transaction_follows_but_is_not_sent_again() {
    let old = Node::start(Mode::Master);
    let new = Node::start(Mode::Replica(None));
    let mut client = Client::connect_with(options(&[old.port, new.port])).unwrap();

    client.execute(&["MULTI"]).unwrap();
    old.become_mode(Mode::Replica(Some(new.port)));
    new.become_mode(Mode::Master);

    readonly(client.execute(&["SET", "k", "v"]));
    assert_eq!(new.seen("SET"), 0);

    // The client moved: the next command is not inside the lost transaction.
    client.set("k", "v").unwrap();
    assert_eq!(new.seen("SET"), 1);
}

#[test]
fn a_coordinator_name_finds_the_primary_at_connect_and_after_a_failover() {
    let coordinator = Node::start(Mode::Names(0));
    let one = Node::start(Mode::Master);
    let two = Node::start(Mode::Replica(None));

    coordinator.become_mode(Mode::Names(one.port));

    let mut client = Client::connect_with(ClientOptions {
        sentinel_master: Some("main".to_owned()),
        ..options(&[coordinator.port])
    })
    .unwrap();

    client.set("k", "1").unwrap();
    assert_eq!((one.seen("SET"), two.seen("SET")), (1, 0));
    assert_eq!(coordinator.seen("SENTINEL"), 1);

    one.become_mode(Mode::Replica(None));
    two.become_mode(Mode::Master);
    coordinator.become_mode(Mode::Names(two.port));

    client.set("k", "2").unwrap();
    assert_eq!((one.seen("SET"), two.seen("SET")), (2, 1));

    // After a dropped socket the coordinator is asked again.
    one.become_mode(Mode::Master);
    two.become_mode(Mode::Replica(None));
    coordinator.become_mode(Mode::Names(one.port));
    two.drop_connections();

    assert!(matches!(client.ping(), Err(Error::Io(_))));
    client.set("k", "3").unwrap();
    assert_eq!((one.seen("SET"), two.seen("SET")), (3, 1));
}

#[test]
fn a_coordinator_that_names_no_primary_fails_the_connect() {
    let coordinator = Node::start(Mode::Master);
    let error = Client::connect_with(ClientOptions {
        sentinel_master: Some("main".to_owned()),
        ..options(&[coordinator.port])
    })
    .err()
    .unwrap();

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
}

#[test]
fn a_hung_seed_costs_one_bounded_wait() {
    // Accepts and never answers.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_port = silent.local_addr().unwrap().port();
    let live = Node::start(Mode::Master);
    let other = Node::start(Mode::Replica(None));
    let mut client = Client::connect_with(ClientOptions {
        connect_timeout: Duration::from_millis(300),
        ..options(&[live.port, silent_port, other.port])
    })
    .unwrap();

    live.become_mode(Mode::Replica(Some(other.port)));
    other.become_mode(Mode::Master);

    let started = Instant::now();

    client.set("k", "v").unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    drop(silent);
}
