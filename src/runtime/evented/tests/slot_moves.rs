use super::*;
use crate::slot_moves::SlotMoves;

fn serve_durable_pair(
    data_dir: &std::path::Path,
    configure: impl FnOnce(&mut AppConfig),
) -> (
    SocketAddr,
    SocketAddr,
    ShutdownToken,
    thread::JoinHandle<io::Result<()>>,
) {
    let ports = crate::test_ports::reserve();
    let (left, right) = (ports.address_at(0), ports.address_at(1));
    let mut config = AppConfig {
        address: left.to_string(),
        shards: 2,
        advertised_host: "127.0.0.1".into(),
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = crate::DurabilityProfile::Always;
    config.persistence.data_dir = data_dir.to_path_buf();
    configure(&mut config);
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || {
        let _ports = ports;

        run(&config, shutdown)
    });

    (left, right, stop, server)
}

fn stop_pair(shutdown: ShutdownToken, server: thread::JoinHandle<io::Result<()>>) {
    shutdown.request();
    join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap();
}

#[test]
fn a_moved_range_is_served_by_its_new_shard_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let moved = key_on_shard(0, "moved");
    let stays = key_on_shard(0, "stays");
    let slot = crate::routing::hash_slot(moved.as_bytes());

    assert_ne!(slot, crate::routing::hash_slot(stays.as_bytes()));

    let (left, _, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    for key in [&moved, &stays] {
        let set = resp(&[&[b"SET", key.as_bytes(), b"before"]]);

        assert_eq!(request_connected(left, &set), b"+OK\r\n");
    }
    stop_pair(shutdown, server);

    // As if the slot had moved to shard 1 before shard 0 wrote anything: shard 0's
    // records for it are not replayed, and shard 1 holds none.
    let mut moves = SlotMoves::new(2);

    moves.begin(slot, slot, 1, 1).unwrap();
    moves.complete(1).unwrap();
    moves.save(dir.path()).unwrap();

    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    for address in [left, right] {
        let get = resp(&[&[b"GET", moved.as_bytes()]]);

        assert_eq!(request_connected(address, &get), b"$-1\r\n");
    }
    let get = resp(&[&[b"GET", stays.as_bytes()]]);

    assert_eq!(request_connected(left, &get), b"$6\r\n");

    // Written through shard 0, the key goes to shard 1 and survives another restart there.
    let set = resp(&[&[b"SET", moved.as_bytes(), b"after"]]);

    assert_eq!(request_connected(left, &set), b"+OK\r\n");
    stop_pair(shutdown, server);

    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(right, &server);
    let get = resp(&[&[b"GET", moved.as_bytes()]]);

    assert_eq!(request_connected(right, &get), b"$5\r\n");
    let slots = request(left, &resp(&[&[b"CLUSTER", b"SHARDS"]]));
    let text = String::from_utf8_lossy(&slots);

    assert!(text.contains(&format!(":{slot}\r\n")), "{text}");
    stop_pair(shutdown, server);
}

#[test]
fn a_moved_table_refuses_replication() {
    let dir = tempfile::tempdir().unwrap();
    let mut moves = SlotMoves::new(2);

    moves.begin(0, 10, 1, 1).unwrap();
    moves.complete(1).unwrap();
    moves.save(dir.path()).unwrap();

    let (_, _, _, server) = serve_durable_pair(dir.path(), |config| {
        config.replication.role = crate::config::ReplicationRole::Primary;
    });
    let error = join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap_err();

    assert!(
        error.to_string().contains("replication is not supported"),
        "{error}"
    );
}

#[test]
fn a_damaged_table_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();

    std::fs::write(
        dir.path().join(crate::slot_moves::FILE_NAME),
        b"not a table",
    )
    .unwrap();

    let (_, _, _, server) = serve_durable_pair(dir.path(), |_| {});
    let error = join_within(server, Duration::from_secs(10))
        .unwrap()
        .unwrap_err();

    assert!(error.to_string().contains("damaged"), "{error}");
}

/// A reply, as a small client sees it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Value>),
}

pub(super) struct Client(pub(super) std::io::BufReader<StdTcpStream>);

impl Client {
    pub(super) fn connect(address: SocketAddr) -> Self {
        let stream = StdTcpStream::connect(address).unwrap();

        stream.set_read_timeout(Some(PATIENCE)).unwrap();
        Self(std::io::BufReader::new(stream))
    }

    pub(super) fn call(&mut self, parts: &[&[u8]]) -> Value {
        self.0.get_mut().write_all(&resp(&[parts])).unwrap();
        self.read()
    }

    pub(super) fn read(&mut self) -> Value {
        use std::io::BufRead;

        let mut line = String::new();

        self.0.read_line(&mut line).unwrap();
        let (kind, rest) = line.trim_end().split_at(1);

        match kind {
            "+" => Value::Simple(rest.into()),
            "-" => Value::Error(rest.into()),
            ":" => Value::Integer(rest.parse().unwrap()),
            "$" if rest == "-1" => Value::Bulk(None),
            "$" => {
                let mut bytes = vec![0; rest.parse::<usize>().unwrap() + 2];

                self.0.read_exact(&mut bytes).unwrap();
                bytes.truncate(bytes.len() - 2);
                Value::Bulk(Some(bytes))
            }
            "*" if rest == "-1" => Value::Array(Vec::new()),
            "*" => Value::Array(
                (0..rest.parse::<usize>().unwrap())
                    .map(|_| self.read())
                    .collect(),
            ),
            "%" => Value::Array(
                (0..rest.parse::<usize>().unwrap() * 2)
                    .map(|_| self.read())
                    .collect(),
            ),
            other => panic!("unexpected reply {other}{rest}"),
        }
    }
}

pub(super) fn text(value: &Value) -> String {
    match value {
        Value::Simple(text) | Value::Error(text) => text.clone(),
        Value::Bulk(Some(bytes)) => String::from_utf8_lossy(bytes).into(),
        Value::Integer(value) => value.to_string(),
        other => format!("{other:?}"),
    }
}

/// `RUVIO MOVESLOTS STATUS` as name/value pairs.
fn move_status(client: &mut Client) -> HashMap<String, String> {
    let Value::Array(items) = client.call(&[b"RUVIO", b"MOVESLOTS", b"STATUS"]) else {
        panic!("status is not a map");
    };

    items
        .chunks(2)
        .map(|pair| (text(&pair[0]), text(&pair[1])))
        .collect()
}

fn wait_for_move(client: &mut Client) -> HashMap<String, String> {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        let status = move_status(client);

        if !["copying", "cleaning", "cancelling"].contains(&status["state"].as_str()) {
            return status;
        }
        assert!(Instant::now() < deadline, "move did not end: {status:?}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn serve_memory_pair() -> (
    SocketAddr,
    SocketAddr,
    ShutdownToken,
    thread::JoinHandle<io::Result<()>>,
) {
    let dir = std::env::temp_dir();

    serve_durable_pair(&dir, |config| {
        config.persistence.profile = crate::DurabilityProfile::Memory;
    })
}

/// A key of `label` whose slot is in `first..=last`.
fn key_in_slots(label: &str, first: u16, last: u16) -> String {
    (0..)
        .map(|n| format!("{label}:{n}"))
        .find(|key| (first..=last).contains(&crate::routing::hash_slot(key.as_bytes())))
        .unwrap()
}

#[test]
fn a_range_moves_with_every_value_type_while_a_writer_runs() {
    let (left, right, shutdown, server) = serve_memory_pair();

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);

    let mut client = Client::connect(left);
    let (first, last) = (0u16, 2047u16);
    let mut strings = Vec::new();

    for n in 0..2000 {
        let key = key_in_slots(&format!("s{n}"), first, last);

        assert_eq!(
            client.call(&[b"SET", key.as_bytes(), key.as_bytes()]),
            Value::Simple("OK".into())
        );
        strings.push(key);
    }
    let list = key_in_slots("list", first, last);
    let hash = key_in_slots("hash", first, last);
    let stream = key_in_slots("stream", first, last);
    let expiring = key_in_slots("ttl", first, last);
    let outside = key_in_slots("outside", 3000, 8191);

    client.call(&[b"RPUSH", list.as_bytes(), b"a", b"b", b"c"]);
    client.call(&[b"HSET", hash.as_bytes(), b"f", b"v"]);
    client.call(&[b"XADD", stream.as_bytes(), b"1-1", b"f", b"v"]);
    client.call(&[b"SET", expiring.as_bytes(), b"v", b"EX", b"1000"]);
    client.call(&[b"SET", outside.as_bytes(), b"stays"]);

    // A writer increments counters in the range through both ports while the range moves.
    let counters: Vec<String> = (0..8)
        .map(|n| key_in_slots(&format!("c{n}"), first, last))
        .collect();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let counters = counters.clone();
        let stop = Arc::clone(&stop);

        thread::spawn(move || {
            let mut clients = [Client::connect(left), Client::connect(right)];
            let mut acked = vec![0i64; counters.len()];
            let mut round = 0usize;

            while !stop.load(Ordering::Acquire) {
                let index = round % counters.len();

                match clients[round % 2].call(&[b"INCR", counters[index].as_bytes()]) {
                    Value::Integer(_) => acked[index] += 1,
                    Value::Error(error) if error.starts_with("TRYAGAIN") => {}
                    other => panic!("INCR answered {other:?}"),
                }
                round += 1;
            }
            acked
        })
    };

    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        client.call(&[b"RUVIO", b"MOVESLOTS", b"0-2047", b"1"]),
        Value::Simple("OK".into())
    );
    let status = wait_for_move(&mut client);

    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Release);
    let acked = writer.join().unwrap();

    assert_eq!(status["state"], "done", "{status:?}");
    assert!(status["keys"].parse::<u64>().unwrap() >= 2000, "{status:?}");

    for port in [left, right] {
        let mut client = Client::connect(port);

        for key in strings.iter().step_by(97) {
            assert_eq!(
                client.call(&[b"GET", key.as_bytes()]),
                Value::Bulk(Some(key.as_bytes().to_vec()))
            );
        }
        assert_eq!(client.call(&[b"LLEN", list.as_bytes()]), Value::Integer(3));
        assert_eq!(
            client.call(&[b"HGET", hash.as_bytes(), b"f"]),
            Value::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            client.call(&[b"XLEN", stream.as_bytes()]),
            Value::Integer(1)
        );
        let Value::Integer(ttl) = client.call(&[b"TTL", expiring.as_bytes()]) else {
            panic!("TTL");
        };
        assert!((990..=1000).contains(&ttl), "{ttl}");
        assert_eq!(
            client.call(&[b"GET", outside.as_bytes()]),
            Value::Bulk(Some(b"stays".to_vec()))
        );
        for (key, acked) in counters.iter().zip(&acked) {
            assert_eq!(
                client.call(&[b"GET", key.as_bytes()]),
                Value::Bulk(Some(acked.to_string().into_bytes())),
                "{key}"
            );
        }
    }

    // The new owner holds the range; the old one dropped its copy.
    let mut on_right = Client::connect(right);
    let Value::Array(slots) = on_right.call(&[b"CLUSTER", b"SLOTS"]) else {
        panic!("CLUSTER SLOTS");
    };
    assert!(
        slots.iter().any(|range| matches!(range, Value::Array(parts)
            if parts[0] == Value::Integer(0) && parts[1] == Value::Integer(2047)
                && matches!(&parts[2], Value::Array(node) if node[1] == Value::Integer(i64::from(right.port()))))),
        "{slots:?}"
    );
    let Value::Integer(left_keys) = Client::connect(left).call(&[b"DBSIZE"]) else {
        panic!("DBSIZE");
    };
    let Value::Integer(total) = on_right.call(&[b"DBSIZE"]) else {
        panic!("DBSIZE");
    };
    assert_eq!(total, 2000 + 4 + counters.len() as i64);
    assert_eq!(left_keys, 1, "only the key outside the range stays");

    stop_pair(shutdown, server);
}

#[test]
fn moveslots_refuses_bad_requests_and_reports_none() {
    let (left, right, shutdown, server) = serve_memory_pair();

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let mut client = Client::connect(left);

    assert_eq!(move_status(&mut client)["state"], "none");
    for (args, expected) in [
        (
            &[&b"RUVIO"[..], b"MOVESLOTS", b"0-10", b"0"][..],
            "already on that shard",
        ),
        (
            &[&b"RUVIO"[..], b"MOVESLOTS", b"0-10", b"7"][..],
            "no such shard",
        ),
        (
            &[&b"RUVIO"[..], b"MOVESLOTS", b"8000-8300", b"1"][..],
            "more than one owner",
        ),
        (
            &[&b"RUVIO"[..], b"MOVESLOTS", b"10-5", b"1"][..],
            "ends before it starts",
        ),
        (
            &[&b"RUVIO"[..], b"MOVESLOTS", b"16384", b"1"][..],
            "between 0 and 16383",
        ),
        (&[&b"RUVIO"[..], b"MOVESLOTS"][..], "expects"),
    ] {
        let Value::Error(error) = client.call(args) else {
            panic!("{args:?} was accepted");
        };
        assert!(error.contains(expected), "{error}");
    }

    stop_pair(shutdown, server);
}

#[test]
fn a_hand_over_releases_blocked_clients_and_fails_watches_on_the_range() {
    let (left, right, shutdown, server) = serve_memory_pair();

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let queue = key_in_slots("queue", 0, 99);
    let watched = key_in_slots("watched", 0, 99);
    let mut client = Client::connect(left);

    client.call(&[b"SET", watched.as_bytes(), b"1"]);

    let mut blocked = Client::connect(left);

    blocked
        .0
        .get_mut()
        .write_all(&resp(&[&[b"BLPOP", queue.as_bytes(), b"0"]]))
        .unwrap();
    let mut watcher = Client::connect(left);

    assert_eq!(
        watcher.call(&[b"WATCH", watched.as_bytes()]),
        Value::Simple("OK".into())
    );
    assert_eq!(watcher.call(&[b"MULTI"]), Value::Simple("OK".into()));
    assert_eq!(
        watcher.call(&[b"SET", watched.as_bytes(), b"2"]),
        Value::Simple("QUEUED".into())
    );
    wait_for_parked(left, 1);

    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-99", b"1"]);
    assert_eq!(wait_for_move(&mut client)["state"], "done");

    let Value::Error(error) = blocked.read() else {
        panic!("BLPOP was not released");
    };
    assert!(error.starts_with("UNBLOCKED"), "{error}");
    // The watched key moved: EXEC runs nothing, as when a watched key changes.
    assert_eq!(watcher.call(&[b"EXEC"]), Value::Array(Vec::new()));
    assert!(matches!(watcher.call(&[b"PING"]), Value::Simple(_)));
    assert_eq!(
        client.call(&[b"GET", watched.as_bytes()]),
        Value::Bulk(Some(b"1".to_vec()))
    );
    // Blocking on the new owner works as before.
    let mut pusher = Client::connect(right);

    pusher.call(&[b"RPUSH", queue.as_bytes(), b"x"]);
    assert_eq!(
        Client::connect(left).call(&[b"LPOP", queue.as_bytes()]),
        Value::Bulk(Some(b"x".to_vec()))
    );

    stop_pair(shutdown, server);
}

#[test]
fn a_range_that_does_not_fit_on_the_target_is_cancelled_and_stays() {
    let dir = std::env::temp_dir();
    let (left, right, shutdown, server) = serve_durable_pair(&dir, |config| {
        config.persistence.profile = crate::DurabilityProfile::Memory;
        config.memory.max_shard_bytes = 4 << 20;
        config.memory.reserved_headroom_bytes = 0;
    });

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let mut client = Client::connect(left);
    let value = vec![b'v'; 1024];
    let mut moving = Vec::new();

    for n in 0..2400 {
        let key = key_in_slots(&format!("m{n}"), 0, 4095);

        assert_eq!(
            client.call(&[b"SET", key.as_bytes(), &value]),
            Value::Simple("OK".into())
        );
        moving.push(key);
    }
    for n in 0..2400 {
        let key = key_in_slots(&format!("t{n}"), 8192, 16383);

        assert_eq!(
            client.call(&[b"SET", key.as_bytes(), &value]),
            Value::Simple("OK".into())
        );
    }

    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-4095", b"1"]);
    let status = wait_for_move(&mut client);

    assert_eq!(status["state"], "cancelled", "{status:?}");
    assert!(status["error"].contains("memory limit"), "{status:?}");
    for key in moving.iter().step_by(101) {
        assert_eq!(
            client.call(&[b"GET", key.as_bytes()]),
            Value::Bulk(Some(value.clone()))
        );
    }
    assert_eq!(
        Client::connect(right).call(&[b"DBSIZE"]),
        Value::Integer(2400)
    );
    let Value::Array(slots) = client.call(&[b"CLUSTER", b"SLOTS"]) else {
        panic!("CLUSTER SLOTS");
    };
    assert_eq!(slots.len(), 2, "{slots:?}");

    stop_pair(shutdown, server);
}

/// What a writer expects each key of the range to hold, from the writes the server acked.
#[derive(Default)]
struct Model {
    strings: HashMap<String, Option<String>>,
    lists: HashMap<String, VecDeque<String>>,
    hashes: HashMap<String, HashMap<String, String>>,
}

#[test]
fn every_kind_of_write_during_a_move_lands_on_the_new_owner() {
    let (left, right, shutdown, server) = serve_memory_pair();

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let mut client = Client::connect(left);

    for n in 0..3000 {
        let key = key_in_slots(&format!("bulk{n}"), 0, 4095);

        client.call(&[b"SET", key.as_bytes(), b"x"]);
    }

    let strings: Vec<String> = (0..16)
        .map(|n| key_in_slots(&format!("str{n}"), 0, 4095))
        .collect();
    let lists: Vec<String> = (0..8)
        .map(|n| key_in_slots(&format!("list{n}"), 0, 4095))
        .collect();
    let hashes: Vec<String> = (0..8)
        .map(|n| key_in_slots(&format!("hash{n}"), 0, 4095))
        .collect();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (strings, lists, hashes) = (strings.clone(), lists.clone(), hashes.clone());
        let stop = Arc::clone(&stop);

        thread::spawn(move || {
            let mut clients = [Client::connect(left), Client::connect(right)];
            let mut model = Model::default();
            let mut step = 0usize;

            while !stop.load(Ordering::Acquire) || step < 2000 {
                let client = &mut clients[step % 2];
                let value = step.to_string();
                let string = &strings[step % strings.len()];
                let list = &lists[step % lists.len()];
                let hash = &hashes[step % hashes.len()];
                let field = format!("f{}", step % 5);
                let reply = match step % 7 {
                    0 => client.call(&[b"SET", string.as_bytes(), value.as_bytes()]),
                    1 => client.call(&[b"APPEND", string.as_bytes(), b"+"]),
                    2 => client.call(&[b"DEL", string.as_bytes()]),
                    3 => client.call(&[b"RPUSH", list.as_bytes(), value.as_bytes()]),
                    4 => client.call(&[b"LPOP", list.as_bytes()]),
                    5 => {
                        client.call(&[b"HSET", hash.as_bytes(), field.as_bytes(), value.as_bytes()])
                    }
                    _ => client.call(&[b"HDEL", hash.as_bytes(), field.as_bytes()]),
                };

                step += 1;
                if matches!(&reply, Value::Error(error) if error.starts_with("TRYAGAIN")) {
                    continue;
                }
                assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
                match (step - 1) % 7 {
                    0 => {
                        model.strings.insert(string.clone(), Some(value));
                    }
                    1 => {
                        let entry = model.strings.entry(string.clone()).or_insert(None);

                        *entry = Some(format!("{}+", entry.clone().unwrap_or_default()));
                    }
                    2 => {
                        model.strings.insert(string.clone(), None);
                    }
                    3 => model
                        .lists
                        .entry(list.clone())
                        .or_default()
                        .push_back(value),
                    4 => {
                        model.lists.entry(list.clone()).or_default().pop_front();
                    }
                    5 => {
                        model
                            .hashes
                            .entry(hash.clone())
                            .or_default()
                            .insert(field, value);
                    }
                    _ => {
                        model.hashes.entry(hash.clone()).or_default().remove(&field);
                    }
                }
            }
            model
        })
    };

    thread::sleep(Duration::from_millis(30));
    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-4095", b"1"]);
    let status = wait_for_move(&mut client);

    stop.store(true, Ordering::Release);
    let model = writer.join().unwrap();

    assert_eq!(status["state"], "done", "{status:?}");

    let mut check = Client::connect(right);

    for (key, value) in &model.strings {
        assert_eq!(
            check.call(&[b"GET", key.as_bytes()]),
            Value::Bulk(value.as_ref().map(|value| value.as_bytes().to_vec())),
            "{key}"
        );
    }
    for (key, values) in &model.lists {
        let expected: Vec<Value> = values
            .iter()
            .map(|value| Value::Bulk(Some(value.as_bytes().to_vec())))
            .collect();

        assert_eq!(
            check.call(&[b"LRANGE", key.as_bytes(), b"0", b"-1"]),
            Value::Array(expected),
            "{key}"
        );
    }
    for (key, fields) in &model.hashes {
        assert_eq!(
            check.call(&[b"HLEN", key.as_bytes()]),
            Value::Integer(fields.len() as i64),
            "{key}"
        );
        for (field, value) in fields {
            assert_eq!(
                check.call(&[b"HGET", key.as_bytes(), field.as_bytes()]),
                Value::Bulk(Some(value.as_bytes().to_vec())),
                "{key} {field}"
            );
        }
    }

    stop_pair(shutdown, server);
}

#[test]
fn a_durable_move_survives_a_restart_on_its_new_owner() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let mut client = Client::connect(left);
    let mut moved = Vec::new();

    for n in 0..300 {
        let key = key_in_slots(&format!("d{n}"), 0, 2047);

        client.call(&[b"SET", key.as_bytes(), key.as_bytes()]);
        moved.push(key);
    }
    let stream = key_in_slots("stream", 0, 2047);
    let bloom = key_in_slots("bloom", 0, 2047);
    let expiring = key_in_slots("ttl", 0, 2047);
    let outside = key_in_slots("outside", 3000, 8191);

    client.call(&[b"XADD", stream.as_bytes(), b"1-1", b"f", b"v"]);
    client.call(&[b"BF.ADD", bloom.as_bytes(), b"alpha"]);
    client.call(&[b"SET", expiring.as_bytes(), b"v", b"EX", b"1000"]);
    client.call(&[b"SET", outside.as_bytes(), b"stays"]);

    assert_eq!(
        client.call(&[b"RUVIO", b"MOVESLOTS", b"0-2047", b"1"]),
        Value::Simple("OK".into())
    );
    assert_eq!(wait_for_move(&mut client)["state"], "done");
    // Written after the hand-over: only the new owner's log has it.
    client.call(&[b"SET", moved[0].as_bytes(), b"after"]);
    stop_pair(shutdown, server);

    let saved = SlotMoves::load(dir.path(), 2).unwrap().unwrap();

    assert_eq!(saved.owners()[0], 1);

    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    for port in [left, right] {
        let mut client = Client::connect(port);

        assert_eq!(
            client.call(&[b"GET", moved[0].as_bytes()]),
            Value::Bulk(Some(b"after".to_vec()))
        );
        for key in &moved[1..] {
            assert_eq!(
                client.call(&[b"GET", key.as_bytes()]),
                Value::Bulk(Some(key.as_bytes().to_vec()))
            );
        }
        assert_eq!(
            client.call(&[b"XLEN", stream.as_bytes()]),
            Value::Integer(1)
        );
        assert_eq!(
            client.call(&[b"BF.EXISTS", bloom.as_bytes(), b"alpha"]),
            Value::Integer(1)
        );
        assert!(
            matches!(client.call(&[b"TTL", expiring.as_bytes()]), Value::Integer(ttl) if ttl > 900)
        );
    }
    // Each key once: the old owner dropped its copy, the new one holds every key.
    assert_eq!(Client::connect(left).call(&[b"DBSIZE"]), Value::Integer(1));
    assert_eq!(
        Client::connect(right).call(&[b"DBSIZE"]),
        Value::Integer(303)
    );

    stop_pair(shutdown, server);
}

#[test]
fn the_old_owner_change_stream_reports_where_a_moved_range_continues() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right, shutdown, server) = serve_durable_pair(dir.path(), |_| {});

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let mut client = Client::connect(left);
    let key = key_in_slots("fed", 0, 99);

    client.call(&[b"SET", key.as_bytes(), b"1"]);
    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-99", b"1"]);
    assert_eq!(wait_for_move(&mut client)["state"], "done");

    let mut feed = Client::connect(left);

    feed.0
        .get_mut()
        .write_all(&resp(&[&[
            b"CHANGES",
            b"SUBSCRIBE",
            b"FROM",
            b"0:0:0",
            b"MATCH",
            b"nothing:*",
        ]]))
        .unwrap();
    // The MATCH drops the SET; the move passes every filter.
    let event = loop {
        match feed.read() {
            Value::Array(items) if text(&items[0]) == "change" => break items,
            _ => {}
        }
    };

    assert_eq!(text(&event[3]), "moved");
    assert_eq!(text(&event[4]), "0-99");
    // `generation:shard:sequence`; a server that never replicated is generation 0.
    assert!(text(&event[5]).starts_with("0:1:"), "{event:?}");

    stop_pair(shutdown, server);
}

#[test]
fn keys_split_by_a_move_are_refused_together_and_still_work_apart() {
    let (left, right, shutdown, server) = serve_memory_pair();

    wait_until_listening(left, &server);
    wait_until_listening(right, &server);
    let moved = key_in_slots("split-a", 0, 99);
    let stays = key_in_slots("split-b", 100, 8191);
    let mut client = Client::connect(left);

    client.call(&[b"SET", moved.as_bytes(), b"1"]);
    client.call(&[b"SET", stays.as_bytes(), b"1"]);
    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-99", b"1"]);
    assert_eq!(wait_for_move(&mut client)["state"], "done");

    // One shard each now: a command over both is refused, never a write on the old owner.
    let Value::Error(error) =
        client.call(&[b"MSET", moved.as_bytes(), b"2", stays.as_bytes(), b"2"])
    else {
        panic!("MSET across the move was accepted");
    };
    assert!(error.starts_with("CROSSSLOT"), "{error}");
    for (key, port) in [(&moved, right), (&stays, left)] {
        assert_eq!(
            Client::connect(port).call(&[b"GET", key.as_bytes()]),
            Value::Bulk(Some(b"1".to_vec()))
        );
    }
    // Moving status reads while a move runs do not block it.
    client.call(&[b"RUVIO", b"MOVESLOTS", b"0-99", b"0"]);
    for _ in 0..50 {
        move_status(&mut client);
    }
    assert_eq!(wait_for_move(&mut client)["state"], "done");

    stop_pair(shutdown, server);
}
