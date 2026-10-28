//! Single-server client behaviour against an in-process fake: connecting, reconnecting,
//! Pub/Sub bookkeeping, pipelines and reply-shape errors. No Ruvio server needed.

use std::io::{BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ruvio_client::__bench::parse;
use ruvio_client::{
    Aggregate, Client, ClientOptions, ConnectionNotice, Error, GeoFrom, GeoSearch, GeoUnit,
    LimitOutcome, ListSide, RespValue, SortedSetCombine,
};

enum Reply {
    Send(String),
    Close,
}

/// `(connection index, command)` in arrival order.
type Log = Arc<Mutex<Vec<(usize, Vec<String>)>>>;

struct Server {
    port: u16,
    log: Log,
    connections: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    accept: Mutex<Option<JoinHandle<()>>>,
}

impl Server {
    /// `handler(connection, n-th command on that connection, command)`.
    fn start(handler: impl Fn(usize, usize, &[String]) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(handler);

        let accept = {
            let log = Arc::clone(&log);
            let connections = Arc::clone(&connections);
            let stop = Arc::clone(&stop);

            thread::spawn(move || {
                listener.set_nonblocking(true).unwrap();

                loop {
                    match listener.accept() {
                        Ok(_) if stop.load(Ordering::SeqCst) => return,
                        Ok((socket, _)) => {
                            socket.set_nonblocking(false).unwrap();

                            let index = connections.fetch_add(1, Ordering::SeqCst);
                            let log = Arc::clone(&log);
                            let handler = Arc::clone(&handler);

                            thread::spawn(move || {
                                let mut writer = socket.try_clone().unwrap();
                                let mut reader = BufReader::new(socket);
                                let mut count = 0;

                                loop {
                                    let Ok(RespValue::Array(items)) = parse(&mut reader) else {
                                        return;
                                    };
                                    let command: Vec<String> = items
                                        .into_iter()
                                        .map(|i| {
                                            String::from_utf8_lossy(
                                                &i.into_bytes().unwrap_or_default(),
                                            )
                                            .into_owned()
                                        })
                                        .collect();

                                    log.lock().unwrap().push((index, command.clone()));

                                    let reply = handler(index, count, &command);

                                    count += 1;

                                    match reply {
                                        Reply::Send(text) => {
                                            if writer.write_all(text.as_bytes()).is_err() {
                                                return;
                                            }
                                        }
                                        Reply::Close => {
                                            let _ = writer.shutdown(Shutdown::Both);

                                            return;
                                        }
                                    }
                                }
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if stop.load(Ordering::SeqCst) {
                                return;
                            }

                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => return,
                    }
                }
            })
        };

        Self {
            port,
            log,
            connections,
            stop,
            accept: Mutex::new(Some(accept)),
        }
    }

    /// Answers like a plain standalone server.
    fn standalone() -> Self {
        Self::start(|_, _, command| Reply::Send(standard(command)))
    }

    fn options(&self) -> ClientOptions {
        ClientOptions {
            port: self.port,
            reconnect_base_delay: Duration::ZERO,
            reconnect_max_delay: Duration::ZERO,
            max_reconnect_attempts: 3,
            connect_timeout: Duration::from_secs(1),
            ..ClientOptions::default()
        }
    }

    fn client(&self) -> Client {
        let mut client = Client::connect_with(self.options()).unwrap();

        // No test may hang on a reply that never comes.
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();

        client
    }

    fn commands(&self, connection: usize) -> Vec<Vec<String>> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(index, _)| *index == connection)
            .map(|(_, command)| command.clone())
            .collect()
    }

    /// The connections accepted so far, for checks that no new connection was opened: it
    /// gives the accept loop, which polls every few milliseconds, time to see a stray one.
    fn connection_count(&self) -> usize {
        thread::sleep(Duration::from_millis(40));
        self.connections.load(Ordering::SeqCst)
    }

    /// Waits until at least `expected` connections were accepted, or 10 s passed, and returns
    /// the count.
    fn wait_for_connections(&self, expected: usize) -> usize {
        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            let count = self.connections.load(Ordering::SeqCst);

            if count >= expected || Instant::now() >= deadline {
                return count;
            }

            thread::sleep(Duration::from_millis(2));
        }
    }

    /// Stops accepting and closes the listener: the port refuses connections when this returns.
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);

        if let Some(accept) = self.accept.lock().unwrap().take() {
            accept.join().unwrap();
        }
    }
}

fn standard(command: &[String]) -> String {
    let name = command[0].to_ascii_uppercase();

    match name.as_str() {
        "CLUSTER" => "-ERR This instance has cluster support disabled\r\n".to_owned(),
        "PING" => "+PONG\r\n".to_owned(),
        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE"
        | "SUNSUBSCRIBE" => {
            let channels = &command[1..];
            let kind = name.to_ascii_lowercase();

            if channels.is_empty() {
                return format!("*3\r\n${}\r\n{kind}\r\n$-1\r\n:0\r\n", kind.len());
            }

            channels
                .iter()
                .enumerate()
                .map(|(index, channel)| {
                    format!(
                        "*3\r\n${}\r\n{kind}\r\n${}\r\n{channel}\r\n:{}\r\n",
                        kind.len(),
                        channel.len(),
                        index + 1
                    )
                })
                .collect()
        }
        _ => "+OK\r\n".to_owned(),
    }
}

fn words(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

/// A client talking to a server that always answers `reply`, plus the last request.
fn probe<T>(reply: &str, call: impl FnOnce(&mut Client) -> T) -> (T, Vec<String>) {
    let reply = reply.to_owned();
    let server = Server::start(move |_, _, command| {
        if command[0].eq_ignore_ascii_case("CLUSTER") {
            Reply::Send("-ERR cluster support disabled\r\n".to_owned())
        } else {
            Reply::Send(reply.clone())
        }
    });
    let mut client = Client::connect_with(ClientOptions {
        discover_cluster: false,
        ..server.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let result = call(&mut client);
    let last = server
        .log
        .lock()
        .unwrap()
        .last()
        .map(|(_, c)| c.clone())
        .unwrap_or_default();

    (result, last)
}

fn protocol<T>(result: Result<T, Error>) -> String {
    match result {
        Err(Error::Protocol(message)) => message,
        Err(other) => panic!("expected a protocol error, got {other:?}"),
        Ok(_) => panic!("expected a protocol error, got Ok"),
    }
}

// ---------------------------------------------------------------------------------------------
// Connecting
// ---------------------------------------------------------------------------------------------

#[test]
fn connect_helpers_reach_the_same_server() {
    let server = Server::standalone();
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let mut by_ip = Client::connect_ip(ip, server.port, 0).unwrap();
    assert_eq!(by_ip.ping().unwrap(), "PONG");
    assert!(!by_ip.is_cluster());
    assert_eq!(by_ip.shard_count(), 1);

    let by_addr = Client::connect_addr(SocketAddr::new(ip, server.port), 4).unwrap();
    assert_eq!(by_addr.database(), 4);

    let by_database = Client::connect_database("127.0.0.1", server.port, 5).unwrap();
    assert_eq!(by_database.database(), 5);

    let selected: Vec<_> = server
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, c)| c[0] == "SELECT")
        .map(|(_, c)| c[1].clone())
        .collect();

    assert!(selected.contains(&"4".to_owned()) && selected.contains(&"5".to_owned()));
}

#[test]
fn connect_validates_the_host_and_reports_refusals() {
    let result = Client::connect_with(ClientOptions {
        host: String::new(),
        ..ClientOptions::default()
    });
    assert_eq!(protocol(result), "host is required");

    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();

    drop(closed);
    assert!(matches!(
        Client::connect_with(ClientOptions {
            port,
            connect_timeout: Duration::from_millis(200),
            ..ClientOptions::default()
        }),
        Err(Error::Io(_))
    ));
}

#[test]
fn select_and_into_database_update_the_remembered_database() {
    let server = Server::standalone();
    let mut client = server.client();

    client.select(7).unwrap();
    assert_eq!(client.database(), 7);

    let client = client.into_database(2).unwrap();
    assert_eq!(client.database(), 2);
    assert_eq!(server.commands(0).last().unwrap(), &words(&["SELECT", "2"]));
}

// ---------------------------------------------------------------------------------------------
// Reconnecting
// ---------------------------------------------------------------------------------------------

#[test]
fn with_reconnect_off_a_dropped_socket_stays_closed_until_reconnect() {
    // Connection 0 dies on its third command (after CLUSTER SLOTS and one PING).
    let server = Server::start(|connection, count, command| {
        if connection == 0 && count == 2 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = Client::connect_with(ClientOptions {
        reconnect: false,
        ..server.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    client.ping().unwrap();
    assert!(matches!(client.ping(), Err(Error::Io(_))));

    for _ in 0..2 {
        assert!(matches!(
            client.ping(),
            Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::BrokenPipe
        ));
    }
    assert_eq!(server.connection_count(), 1);

    client.reconnect().unwrap();
    assert_eq!(server.wait_for_connections(2), 2);
    assert_eq!(client.ping().unwrap(), "PONG");

    // Reconnecting a healthy client does nothing.
    client.reconnect().unwrap();
    assert_eq!(server.connection_count(), 2);
}

#[test]
fn a_dropped_socket_is_reopened_with_its_database_and_hooks_fire() {
    let server = Server::start(|connection, count, command| {
        // CLUSTER SLOTS, SELECT, then the PING that finds the drop.
        if connection == 0 && count == 2 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = Client::connect_with(ClientOptions {
        database: 3,
        ..server.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let events: Arc<Mutex<Vec<(&'static str, ConnectionNotice)>>> =
        Arc::new(Mutex::new(Vec::new()));

    {
        let events = Arc::clone(&events);

        client.on_connection_lost(move |notice| events.lock().unwrap().push(("lost", notice)));
    }
    {
        let events = Arc::clone(&events);

        client.on_connection_restored(move |notice| {
            events.lock().unwrap().push(("restored", notice))
        });
    }

    assert!(client.ping().is_err());
    assert_eq!(client.ping().unwrap(), "PONG");

    // The new socket selects database 3 again before the command runs.
    assert_eq!(
        server.commands(1),
        [words(&["SELECT", "3"]), words(&["PING"])]
    );

    let events = events.lock().unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "lost");
    assert_eq!(events[1].0, "restored");
    assert_eq!(events[0].1.port, server.port);
    assert!(!events[0].1.subscriber);
}

#[test]
fn reconnect_attempts_follow_the_custom_delay_function() {
    let server = Server::start(|connection, count, command| {
        if connection == 0 && count == 1 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = server.client();
    let attempts = Arc::new(Mutex::new(Vec::new()));

    {
        let attempts = Arc::clone(&attempts);

        client.set_reconnect_delay(move |attempt| {
            attempts.lock().unwrap().push(attempt);

            (attempt < 3).then_some(Duration::ZERO)
        });
    }

    assert!(client.ping().is_err());
    server.stop();

    // Nothing listens any more: three tries, then the connect error.
    assert!(matches!(client.ping(), Err(Error::Io(_))));
    assert_eq!(*attempts.lock().unwrap(), [1, 2, 3]);
}

#[test]
fn the_built_in_delay_gives_up_after_the_configured_attempts() {
    let server = Server::start(|connection, count, command| {
        if connection == 0 && count == 1 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = Client::connect_with(ClientOptions {
        max_reconnect_attempts: 2,
        reconnect_base_delay: Duration::from_millis(1),
        reconnect_max_delay: Duration::from_millis(2),
        ..server.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    assert!(client.ping().is_err());
    server.stop();

    let started = std::time::Instant::now();

    assert!(matches!(client.ping(), Err(Error::Io(_))));
    assert!(started.elapsed() < Duration::from_secs(5));
    // reconnect() goes through the same path and fails the same way.
    assert!(matches!(client.reconnect(), Err(Error::Io(_))));
}

#[test]
fn with_reconnect_off_reopening_fails_at_once_when_the_server_is_gone() {
    let server = Server::start(|connection, count, command| {
        if connection == 0 && count == 1 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = Client::connect_with(ClientOptions {
        reconnect: false,
        ..server.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    assert!(client.ping().is_err());
    server.stop();
    assert!(matches!(client.reconnect(), Err(Error::Io(_))));
}

// ---------------------------------------------------------------------------------------------
// Pub/Sub bookkeeping
// ---------------------------------------------------------------------------------------------

#[test]
fn a_subscriber_resubscribes_to_what_is_left_after_a_reconnect() {
    // Connection 0 is the command socket. Connection 1 is the subscriber: it drops after the
    // nine commands below. Connection 2 is the reopened subscriber.
    let server = Server::start(|connection, count, command| {
        if connection == 1 && count == 9 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let client = server.client();
    let mut subscriber = client.subscriber().unwrap();

    subscriber.subscribe(&["a", "b", "a"]).unwrap();
    subscriber.psubscribe(&["p*", "q*"]).unwrap();
    subscriber.ssubscribe(&["s", "t"]).unwrap();
    subscriber.unsubscribe(&["a"]).unwrap();
    subscriber.punsubscribe(&["q*"]).unwrap();
    subscriber.sunsubscribe(&["t"]).unwrap();
    // Unknown names and non-subscription commands are not remembered.
    subscriber.unsubscribe(&["never-subscribed"]).unwrap();
    subscriber.execute(&["PING"]).unwrap();
    subscriber.ping().unwrap();

    // The tenth command meets the closed socket.
    assert!(subscriber.ping().is_err());

    // The next use reopens the socket and replays what is still subscribed.
    subscriber.ping().unwrap();

    let replay: Vec<Vec<String>> = server.commands(2);

    assert_eq!(
        replay,
        [
            words(&["SUBSCRIBE", "b"]),
            words(&["PSUBSCRIBE", "p*"]),
            words(&["SSUBSCRIBE", "s"]),
            words(&["PING"]),
        ]
    );
}

#[test]
fn unsubscribing_from_everything_clears_the_remembered_names() {
    let server = Server::start(|connection, count, command| {
        if connection == 1 && count == 5 {
            Reply::Close
        } else {
            Reply::Send(standard(command))
        }
    });
    let client = server.client();
    let mut subscriber = client.subscriber().unwrap();

    subscriber.subscribe(&["a", "b"]).unwrap();
    subscriber.psubscribe(&["p*"]).unwrap();
    subscriber.ssubscribe(&["s"]).unwrap();
    // No names: drop every subscription of that kind.
    subscriber.unsubscribe(&[]).unwrap();
    subscriber.punsubscribe(&[]).unwrap();

    assert!(subscriber.ping().is_err());
    subscriber.ping().unwrap();
    assert_eq!(
        server.commands(2),
        [words(&["SSUBSCRIBE", "s"]), words(&["PING"])]
    );

    subscriber.sunsubscribe(&[]).unwrap();
}

#[test]
fn a_subscriber_that_fails_to_resubscribe_reports_the_server_error() {
    let server = Server::start(|connection, count, command| {
        if connection == 1 && count == 1 {
            return Reply::Close;
        }
        if connection == 2 && command[0] == "SUBSCRIBE" {
            return Reply::Send("-ERR no subscriptions now\r\n".to_owned());
        }

        Reply::Send(standard(command))
    });
    let client = server.client();
    let mut subscriber = client.subscriber().unwrap();

    subscriber.subscribe(&["a"]).unwrap();
    assert!(subscriber.ping().is_err());
    assert!(matches!(subscriber.ping(), Err(Error::Server(m)) if m == "ERR no subscriptions now"));
}

#[test]
fn subscribing_from_the_command_client_opens_a_second_socket_with_the_read_timeout() {
    let server = Server::standalone();
    let mut client = server.client();

    client
        .set_read_timeout(Some(Duration::from_millis(40)))
        .unwrap();
    assert_eq!(
        client
            .execute(&["SUBSCRIBE", "news"])
            .unwrap()
            .as_array()
            .map(<[_]>::len),
        Some(3)
    );
    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.commands(1), [words(&["SUBSCRIBE", "news"])]);

    // The subscriber socket inherited the timeout: waiting for a delivery times out and survives.
    assert!(matches!(client.read_message(), Err(Error::Io(_))));
    assert!(matches!(client.read_message(), Err(Error::Io(_))));
    // The command socket is unaffected by the subscriber's timeout state.
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(client.ping().unwrap(), "PONG");
}

#[test]
fn a_server_error_in_a_subscribe_confirmation_is_returned() {
    let server = Server::start(|_, _, command| {
        if command[0] == "SUBSCRIBE" {
            Reply::Send("-ERR subscribe refused\r\n".to_owned())
        } else {
            Reply::Send(standard(command))
        }
    });
    let mut client = server.client();

    assert!(
        matches!(client.subscribe(&["a"]), Err(Error::Server(m)) if m == "ERR subscribe refused")
    );
}

// ---------------------------------------------------------------------------------------------
// Pipelines and reply shapes
// ---------------------------------------------------------------------------------------------

#[test]
fn execute_many_pipelines_on_a_single_server() {
    let server = Server::standalone();
    let mut client = server.client();

    let replies = client
        .execute_many(&[&["PING"], &["SET", "k", "v"]])
        .unwrap();

    assert_eq!(
        replies,
        [
            RespValue::Simple("PONG".into()),
            RespValue::Simple("OK".into())
        ]
    );
    assert!(matches!(client.execute_many(&[]), Err(Error::Protocol(_))));
    assert!(matches!(
        client.execute_many(&[&[]]),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn ping_reads_every_reply_shape() {
    let (reply, _) = probe("*2\r\n$4\r\npong\r\n$5\r\nhello\r\n", |c| {
        c.ping_message("hello")
    });
    assert_eq!(reply.unwrap(), "hello");
    let (reply, _) = probe("*1\r\n$4\r\nPONG\r\n", |c| c.ping());
    assert_eq!(reply.unwrap(), "PONG");
    let (reply, request) = probe("$5\r\nhello\r\n", |c| c.ping_message("hello"));
    assert_eq!(reply.unwrap(), "hello");
    assert_eq!(request, words(&["PING", "hello"]));
    // An array that is not a pong push falls back to the generic conversion (an array has no text).
    let (reply, _) = probe("*1\r\n$3\r\nfoo\r\n", |c| c.ping());
    assert!(protocol(reply).contains("null reply"));
    let (reply, _) = probe("*0\r\n", |c| c.ping());
    assert!(protocol(reply).contains("null reply"));
    let (reply, _) = probe("$-1\r\n", |c| c.ping());
    assert!(protocol(reply).contains("null reply"));
    let (reply, _) = probe("*2\r\n$4\r\npong\r\n$-1\r\n", |c| c.ping());
    assert_eq!(reply.unwrap(), "");
}

#[test]
fn byte_and_string_list_replies_are_checked() {
    let (reply, _) = probe("+short\r\n", |c| c.dump("k"));
    assert_eq!(reply.unwrap(), Some(b"short".to_vec()));
    let (reply, _) = probe(":1\r\n", |c| c.dump("k"));
    assert!(protocol(reply).starts_with("DUMP returned"));

    let (reply, _) = probe("*-1\r\n", |c| c.keys("*"));
    assert!(reply.unwrap().is_empty());
    let (reply, _) = probe("*1\r\n$-1\r\n", |c| c.keys("*"));
    assert!(protocol(reply).contains("returned a null bulk"));
    let (reply, _) = probe(":1\r\n", |c| c.keys("*"));
    assert!(protocol(reply).starts_with("KEYS returned"));

    let (reply, _) = probe("*-1\r\n", |c| c.mget(&["a"]));
    assert!(reply.unwrap().is_empty());
    let (reply, _) = probe("*2\r\n$1\r\nx\r\n$-1\r\n", |c| c.mget(&["a", "b"]));
    assert_eq!(reply.unwrap(), [Some("x".to_owned()), None]);
    let (reply, _) = probe(":1\r\n", |c| c.mget(&["a"]));
    assert!(protocol(reply).starts_with("MGET returned"));
}

#[test]
fn pop_many_and_scan_replies_are_checked() {
    let (reply, request) = probe("*2\r\n$1\r\nk\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n", |c| {
        c.lmpop(&["k"], ListSide::Left, 2)
    });
    assert_eq!(
        reply.unwrap(),
        Some(("k".to_owned(), vec!["a".to_owned(), "b".to_owned()]))
    );
    assert_eq!(request, words(&["LMPOP", "1", "k", "LEFT", "COUNT", "2"]));
    let (reply, _) = probe("*-1\r\n", |c| c.lmpop(&["k"], ListSide::Right, 1));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("*2\r\n$1\r\nk\r\n:5\r\n", |c| {
        c.lmpop(&["k"], ListSide::Left, 1)
    });
    assert!(protocol(reply).starts_with("LMPOP returned"));
    let (reply, _) = probe("*3\r\n:1\r\n:2\r\n:3\r\n", |c| {
        c.lmpop(&["k"], ListSide::Left, 1)
    });
    assert!(protocol(reply).starts_with("LMPOP returned"));

    let (reply, _) = probe("*3\r\n$1\r\n0\r\n*0\r\n*0\r\n", |c| {
        c.sscan("s", 0, None, None)
    });
    assert!(protocol(reply).contains("returned 3 parts"));
    let (reply, _) = probe("*2\r\n$1\r\n5\r\n*-1\r\n", |c| c.sscan("s", 0, None, None));
    assert_eq!(reply.unwrap().cursor, 5);
    let (reply, _) = probe("+x\r\n", |c| c.sscan("s", 0, None, None));
    assert!(protocol(reply).starts_with("SSCAN returned"));
    let (reply, _) = probe("*2\r\n$1\r\n0\r\n:1\r\n", |c| c.sscan("s", 0, None, None));
    assert!(protocol(reply).starts_with("SSCAN returned"));
}

#[test]
fn sorted_set_combine_options_are_sent() {
    let combine = SortedSetCombine {
        weights: &[1.0, 2.5],
        aggregate: Some(Aggregate::Min),
    };
    let (reply, request) = probe("*2\r\n$1\r\na\r\n$1\r\nb\r\n", |c| {
        c.zunion(&["x", "y"], combine)
    });
    assert_eq!(reply.unwrap(), ["a", "b"]);
    assert_eq!(
        request,
        words(&[
            "ZUNION",
            "2",
            "x",
            "y",
            "WEIGHTS",
            "1.0",
            "2.5",
            "AGGREGATE",
            "MIN"
        ])
    );

    for (aggregate, word) in [(Aggregate::Sum, "SUM"), (Aggregate::Max, "MAX")] {
        let (_, request) = probe("*0\r\n", |c| {
            c.zunion_with_scores(
                &["x"],
                SortedSetCombine {
                    weights: &[],
                    aggregate: Some(aggregate),
                },
            )
        });
        assert_eq!(
            request,
            words(&["ZUNION", "1", "x", "AGGREGATE", word, "WITHSCORES"])
        );
    }
    let (reply, _) = probe("*-1\r\n", |c| c.zunion(&["x"], SortedSetCombine::default()));
    assert!(reply.unwrap().is_empty());
}

#[test]
fn geo_search_from_coordinates_with_a_hash() {
    let mut search = GeoSearch::radius(GeoFrom::LonLat(1.5, 2.5), 3.0, GeoUnit::Meters);
    search.with_hash = true;
    let (reply, request) = probe("*1\r\n*2\r\n$1\r\na\r\n:7\r\n", |c| {
        c.geo_search_with("g", &search)
    });

    assert_eq!(reply.unwrap()[0].hash, Some(7));
    assert_eq!(
        request,
        words(&[
            "GEOSEARCH",
            "g",
            "FROMLONLAT",
            "1.5",
            "2.5",
            "BYRADIUS",
            "3.0",
            "m",
            "ASC",
            "WITHHASH"
        ])
    );
}

#[test]
fn hash_field_expiry_conditions_are_spelled_out() {
    use ruvio_client::FieldExpireCondition as Condition;

    for (condition, word) in [
        (Condition::IfNoTtl, Some("NX")),
        (Condition::IfTtl, Some("XX")),
        (Condition::IfGreater, Some("GT")),
        (Condition::IfLess, Some("LT")),
        (Condition::Always, None),
    ] {
        let (reply, request) = probe("*1\r\n:1\r\n", |c| {
            c.hexpire_with("h", Duration::from_secs(1), condition, &["f"])
        });
        reply.unwrap();

        let mut expected = words(&["HEXPIRE", "h", "1"]);
        expected.extend(word.map(str::to_owned));
        expected.extend(words(&["FIELDS", "1", "f"]));
        assert_eq!(request, expected);
    }
}

#[test]
fn limit_modes_send_their_keywords_and_read_their_replies() {
    let (reply, request) = probe(":4\r\n", |c| c.limit_sliding("k", 5, 1_000));
    assert_eq!(reply.unwrap(), Some(4));
    assert_eq!(request, words(&["LIMIT", "k", "5", "1000", "SLIDING"]));

    let (reply, _) = probe("$-1\r\n", |c| c.limit_sliding("k", 5, 1_000));
    assert_eq!(reply.unwrap(), None);

    let (reply, request) = probe(":3\r\n", |c| c.limit_tokens("k", 10, 2_000, 2));
    assert_eq!(reply.unwrap(), Some(3));
    assert_eq!(
        request,
        words(&["LIMIT", "k", "10", "2000", "TOKENS", "COST", "2"])
    );

    let (reply, request) = probe("*2\r\n:2\r\n:0\r\n", |c| {
        c.limit_sliding_with_retry("k", 5, 1_000)
    });
    assert_eq!(
        reply.unwrap(),
        LimitOutcome {
            allowed: true,
            remaining: 2,
            retry_after_ms: 0
        }
    );
    assert_eq!(
        request,
        words(&["LIMIT", "k", "5", "1000", "SLIDING", "WITHRETRY"])
    );

    let (reply, request) = probe("*2\r\n:-1\r\n:250\r\n", |c| {
        c.limit_tokens_with_retry("k", 10, 2_000, 1)
    });
    assert_eq!(
        reply.unwrap(),
        LimitOutcome {
            allowed: false,
            remaining: 0,
            retry_after_ms: 250
        }
    );
    assert_eq!(
        request,
        words(&[
            "LIMIT",
            "k",
            "10",
            "2000",
            "TOKENS",
            "COST",
            "1",
            "WITHRETRY"
        ])
    );

    let (reply, _) = probe("*1\r\n:1\r\n", |c| {
        c.limit_sliding_with_retry("k", 5, 1_000)
    });
    assert!(protocol(reply).contains("LIMIT"));
    let (reply, _) = probe("+OK\r\n", |c| c.limit_tokens("k", 5, 1_000, 1));
    assert!(protocol(reply).contains("LIMIT"));
}
