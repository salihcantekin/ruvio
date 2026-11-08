use super::*;

/// The single-port server binds its address itself: the reserved block moves into the
/// server's thread, so no other test takes the port before the server has stopped.
fn serve_single_port(shards: usize) -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let mut config = AppConfig {
        address: address.to_string(),
        shards,
        port_mode: crate::config::PortMode::Single,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = crate::DurabilityProfile::Memory;
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || {
        let _ports = ports;

        run(&config, shutdown).unwrap()
    });

    wait_until_listening(address, &server);
    (address, stop, server)
}

fn serve_single_port_always(
    shards: usize,
    data_dir: &std::path::Path,
) -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    serve_single_port_durable(shards, data_dir, crate::DurabilityProfile::Always, |_| {})
}

fn serve_single_port_durable(
    shards: usize,
    data_dir: &std::path::Path,
    profile: crate::DurabilityProfile,
    configure: impl FnOnce(&mut AppConfig),
) -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let mut config = AppConfig {
        address: address.to_string(),
        shards,
        port_mode: crate::config::PortMode::Single,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = profile;
    config.persistence.data_dir = data_dir.to_path_buf();
    configure(&mut config);
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || {
        let _ports = ports;
        // The shard whose log failed ends with an error.
        let _ = run(&config, shutdown);
    });

    wait_until_listening(address, &server);
    (address, stop, server)
}

fn read_exactly(client: &mut StdTcpStream, expected: &[u8]) {
    let mut got = vec![0u8; expected.len()];

    client
        .read_exact(&mut got)
        .unwrap_or_else(|error| panic!("{error}: wanted {:?}", String::from_utf8_lossy(expected)));
    assert_eq!(
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(expected)
    );
}

/// Whether a server on `address` answers `PING`.
fn pings(address: SocketAddr) -> bool {
    let Ok(mut client) = StdTcpStream::connect(address) else {
        return false;
    };
    let mut reply = [0u8; 7];

    let _ = client.set_read_timeout(Some(PATIENCE));

    client.write_all(&resp(&[&[b"PING"]])).is_ok()
        && client.read_exact(&mut reply).is_ok()
        && &reply == b"+PONG\r\n"
}

/// Connects to a replication listener of the server running on `server`, which may come up
/// a moment after its RESP port.
#[track_caller]
fn connect_replication<T>(port: u16, server: &thread::JoinHandle<T>) -> StdTcpStream {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut replica = None;

    wait_until(&format!("the replication listener on {address}"), || {
        assert!(!server.is_finished(), "the server ended early");
        replica = StdTcpStream::connect(address).ok();
        replica.is_some()
    });
    let replica = replica.unwrap();

    replica.set_read_timeout(Some(PATIENCE)).unwrap();
    replica.set_write_timeout(Some(PATIENCE)).unwrap();
    replica
}

fn next_port(address: SocketAddr) -> SocketAddr {
    SocketAddr::new(address.ip(), address.port() + 1)
}

fn connect(address: SocketAddr) -> StdTcpStream {
    let client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    client
}

#[test]
fn pipelined_commands_reach_every_shard_and_replies_keep_order() {
    let (address, stop, server) = serve_single_port(4);
    let mut writer = connect(address);
    let mut payload = Vec::new();
    let mut expected = Vec::new();

    for index in 0..300 {
        let key = format!("key:{index}");
        let value = format!("value:{index}");

        payload.extend(resp(&[&[b"SET", key.as_bytes(), value.as_bytes()]]));
        expected.extend_from_slice(b"+OK\r\n");
        payload.extend(resp(&[&[b"GET", key.as_bytes()]]));
        expected.extend_from_slice(format!("${}\r\n{value}\r\n", value.len()).as_bytes());
        payload.extend(resp(&[&[b"PING"]]));
        expected.extend_from_slice(b"+PONG\r\n");
    }

    writer.write_all(&payload).unwrap();
    read_exactly(&mut writer, &expected);

    // Other connections land on other shards and must still see every key.
    for _ in 0..4 {
        let mut reader = connect(address);
        let mut payload = Vec::new();
        let mut expected = Vec::new();

        for index in (0..300).step_by(7) {
            let key = format!("key:{index}");
            let value = format!("value:{index}");

            payload.extend(resp(&[&[b"GET", key.as_bytes()]]));
            expected.extend_from_slice(format!("${}\r\n{value}\r\n", value.len()).as_bytes());
        }

        reader.write_all(&payload).unwrap();
        read_exactly(&mut reader, &expected);
    }

    // One command at a time, plus a command that is not forwardable-simple (barrier).
    let mut single = connect(address);

    for index in 0..40 {
        let key = format!("solo:{index}");

        single
            .write_all(&resp(&[&[b"INCR", key.as_bytes()]]))
            .unwrap();
        read_exactly(&mut single, b":1\r\n");
    }

    single
        .write_all(&resp(&[
            &[b"SET", b"{a}x", b"1"],
            &[b"PING"],
            &[b"GET", b"{a}x"],
        ]))
        .unwrap();
    read_exactly(&mut single, b"+OK\r\n+PONG\r\n$1\r\n1\r\n");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn keys_on_different_shards_are_rejected_until_cross_shard_support_lands() {
    let (address, stop, server) = serve_single_port(2);
    let mut client = connect(address);
    let low = (0..).map(|n| format!("lo{n}")).find(|key| {
        crate::routing::shard_for_slot(crate::routing::hash_slot(key.as_bytes()), 2) == 0
    });
    let high = (0..).map(|n| format!("hi{n}")).find(|key| {
        crate::routing::shard_for_slot(crate::routing::hash_slot(key.as_bytes()), 2) == 1
    });

    client
        .write_all(&resp(&[&[
            b"MGET",
            low.unwrap().as_bytes(),
            high.unwrap().as_bytes(),
        ]]))
        .unwrap();
    let mut reply = [0u8; 14];

    client.read_exact(&mut reply).unwrap();
    assert!(reply.starts_with(b"-CROSSSLOT"), "{reply:?}");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn keyspace_commands_cover_all_shards_and_publish_reaches_subscribers() {
    let (address, stop, server) = serve_single_port(4);
    let mut client = connect(address);
    let mut payload = Vec::new();

    for index in 0..100 {
        let key = format!("fan:{index}");

        payload.extend(resp(&[&[b"SET", key.as_bytes(), b"v"]]));
    }

    client.write_all(&payload).unwrap();
    read_exactly(&mut client, &b"+OK\r\n".repeat(100));
    client.write_all(&resp(&[&[b"DBSIZE"]])).unwrap();
    read_exactly(&mut client, b":100\r\n");

    // A subscriber on some shard hears a publish issued from another connection.
    let mut subscriber = connect(address);

    subscriber
        .write_all(&resp(&[&[b"SUBSCRIBE", b"news"]]))
        .unwrap();
    read_exactly(
        &mut subscriber,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    );

    // The count is the publisher's own shard's receivers (as a Redis Cluster node answers),
    // so it is 1 only when the publisher landed on the subscriber's shard; the message
    // reaches the subscriber either way.
    for _ in 0..4 {
        let mut publisher = connect(address);

        publisher
            .write_all(&resp(&[&[b"PUBLISH", b"news", b"hi"]]))
            .unwrap();
        let count = read_line(&mut publisher);

        assert!(count == ":0" || count == ":1", "{count}");
        read_exactly(
            &mut subscriber,
            &crate::pubsub::encode_message(b"news", b"hi"),
        );
    }

    client.write_all(&resp(&[&[b"FLUSHALL"]])).unwrap();
    read_exactly(&mut client, b"+OK\r\n");
    client.write_all(&resp(&[&[b"DBSIZE"]])).unwrap();
    read_exactly(&mut client, b":0\r\n");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

fn two_shard_keys(label: &str) -> [String; 2] {
    [key_on_shard(0, label), key_on_shard(1, label)]
}

fn bulk_reply(value: &str) -> Vec<u8> {
    format!("${}\r\n{value}\r\n", value.len()).into_bytes()
}

#[test]
fn blocking_pop_waits_on_the_owning_shard_from_any_home_shard() {
    let (address, stop, server) = serve_single_port(2);

    for key in two_shard_keys("block") {
        // Two consecutive connections land on different shards, so one is always remote.
        for _ in 0..2 {
            let mut blocked = connect(address);

            blocked
                .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"5"]]))
                .unwrap();
            wait_for_parked(address, 1);

            let mut pusher = connect(address);

            pusher
                .write_all(&resp(&[&[b"RPUSH", key.as_bytes(), b"val"]]))
                .unwrap();
            // The engine hands the element straight to the parked client.
            assert!(read_line(&mut pusher).starts_with(':'));

            let mut expected = b"*2\r\n".to_vec();

            expected.extend(bulk_reply(&key));
            expected.extend(bulk_reply("val"));
            read_exactly(&mut blocked, &expected);

            // The owner shard stayed responsive and the key is consumed.
            blocked
                .write_all(&resp(&[&[b"LLEN", key.as_bytes()]]))
                .unwrap();
            read_exactly(&mut blocked, b":0\r\n");
        }

        let mut scripted = connect(address);

        scripted
            .write_all(&resp(&[&[
                b"EVAL",
                b"return ruvio.call('SET', KEYS[1], 'scripted')",
                b"1",
                key.as_bytes(),
            ]]))
            .unwrap();
        read_exactly(&mut scripted, b"+OK\r\n");
        scripted
            .write_all(&resp(&[&[b"GET", key.as_bytes()]]))
            .unwrap();
        read_exactly(&mut scripted, &bulk_reply("scripted"));
        scripted
            .write_all(&resp(&[&[b"DEL", key.as_bytes()]]))
            .unwrap();
        read_exactly(&mut scripted, b":1\r\n");

        let mut timed_out = connect(address);

        timed_out
            .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"0.2"], &[b"PING"]]))
            .unwrap();
        read_exactly(&mut timed_out, b"*-1\r\n+PONG\r\n");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn multi_exec_and_watch_run_on_the_owning_shard() {
    let (address, stop, server) = serve_single_port(2);

    for key in two_shard_keys("tx") {
        for _ in 0..2 {
            let mut client = connect(address);
            let key = key.as_bytes();

            client
                .write_all(&resp(&[
                    &[b"MULTI"],
                    &[b"SET", key, b"1"],
                    &[b"INCR", key],
                    &[b"EXEC"],
                    &[b"GET", key],
                ]))
                .unwrap();
            read_exactly(
                &mut client,
                b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n:2\r\n$1\r\n2\r\n",
            );

            // WATCH a key, change it from another connection: EXEC aborts.
            client.write_all(&resp(&[&[b"WATCH", key]])).unwrap();
            read_exactly(&mut client, b"+OK\r\n");
            let mut other = connect(address);

            other.write_all(&resp(&[&[b"SET", key, b"9"]])).unwrap();
            read_exactly(&mut other, b"+OK\r\n");
            client
                .write_all(&resp(&[&[b"MULTI"], &[b"SET", key, b"x"], &[b"EXEC"]]))
                .unwrap();
            read_exactly(&mut client, b"+OK\r\n+QUEUED\r\n*-1\r\n");

            // An unchanged WATCH lets EXEC through.
            client.write_all(&resp(&[&[b"WATCH", key]])).unwrap();
            read_exactly(&mut client, b"+OK\r\n");
            client
                .write_all(&resp(&[&[b"MULTI"], &[b"SET", key, b"y"], &[b"EXEC"]]))
                .unwrap();
            read_exactly(&mut client, b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");

            // DISCARD leaves nothing queued and UNWATCH forgets the watch.
            client
                .write_all(&resp(&[
                    &[b"WATCH", key],
                    &[b"UNWATCH"],
                    &[b"MULTI"],
                    &[b"SET", key, b"z"],
                    &[b"DISCARD"],
                    &[b"GET", key],
                ]))
                .unwrap();
            read_exactly(
                &mut client,
                b"+OK\r\n+OK\r\n+OK\r\n+QUEUED\r\n+OK\r\n$1\r\ny\r\n",
            );
        }
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn client_tracking_invalidations_reach_the_home_shard() {
    let (address, stop, server) = serve_single_port(2);

    for round in 0..4 {
        for key in two_shard_keys(&format!("track{round}")) {
            let mut tracker = connect(address);
            let key = key.as_bytes();

            tracker.write_all(&resp(&[&[b"HELLO", b"3"]])).unwrap();
            let mut hello = [0u8; 1];

            tracker.read_exact(&mut hello).unwrap();
            thread::sleep(Duration::from_millis(100));
            let mut drain = [0u8; 4096];
            let _ = tracker.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = tracker.read(&mut drain);
            let _ = tracker.set_read_timeout(Some(Duration::from_secs(5)));

            tracker
                .write_all(&resp(&[&[b"CLIENT", b"TRACKING", b"on"], &[b"GET", key]]))
                .unwrap();
            read_exactly(&mut tracker, b"+OK\r\n_\r\n");

            let mut writer = connect(address);

            writer.write_all(&resp(&[&[b"SET", key, b"1"]])).unwrap();
            read_exactly(&mut writer, b"+OK\r\n");

            let mut expected = b">2\r\n$10\r\ninvalidate\r\n*1\r\n".to_vec();

            expected.extend(bulk_reply(std::str::from_utf8(key).unwrap()));
            read_exactly(&mut tracker, &expected);
        }
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn an_expired_key_is_invalidated_for_a_tracking_client() {
    let (address, stop, server) = serve_single_port(1);
    let mut tracker = connect(address);
    let mut writer = connect(address);

    tracker.write_all(&resp(&[&[b"HELLO", b"3"]])).unwrap();
    let mut hello = [0u8; 1];

    tracker.read_exact(&mut hello).unwrap();
    thread::sleep(Duration::from_millis(50));
    let mut drain = [0u8; 4096];
    let _ = tracker.set_read_timeout(Some(Duration::from_millis(100)));
    let _ = tracker.read(&mut drain);
    let _ = tracker.set_read_timeout(Some(Duration::from_secs(5)));

    // One batch reads the clock once, so the GET sees the key however slow the machine is.
    // The key is not tracked yet when this client writes it: no invalidation for the SET.
    tracker
        .write_all(&resp(&[
            &[b"CLIENT", b"TRACKING", b"on"],
            &[b"SET", b"short", b"v", b"PX", b"1000"],
            &[b"GET", b"short"],
        ]))
        .unwrap();
    read_exactly(&mut tracker, b"+OK\r\n+OK\r\n$1\r\nv\r\n");

    // The writer's read finds the key past its deadline and removes it.
    thread::sleep(Duration::from_millis(1100));
    writer.write_all(&resp(&[&[b"GET", b"short"]])).unwrap();
    read_exactly(&mut writer, b"$-1\r\n");

    read_exactly(
        &mut tracker,
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nshort\r\n",
    );

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn a_command_sent_to_another_shard_is_counted() {
    let (address, stop, server) = serve_single_port(2);
    let [local, remote] = two_shard_keys("count");
    let mut client = connect(address);

    client
        .write_all(&resp(&[
            &[b"HSET", local.as_bytes(), b"f", b"v"],
            &[b"HSET", remote.as_bytes(), b"f", b"v"],
            &[b"INFO"],
        ]))
        .unwrap();
    read_exactly(&mut client, b":1\r\n:1\r\n");
    let mut info = Vec::new();
    let mut chunk = [0u8; 8192];

    while !String::from_utf8_lossy(&info).contains("other_commands:") {
        let read = client.read(&mut chunk).unwrap();

        assert!(read > 0);
        info.extend_from_slice(&chunk[..read]);
    }
    while !String::from_utf8_lossy(&info).contains("other_commands:2\r\n")
        && !String::from_utf8_lossy(&info)
            .split("other_commands:")
            .nth(1)
            .is_some_and(|rest| rest.contains("\r\n"))
    {
        let read = client.read(&mut chunk).unwrap();

        info.extend_from_slice(&chunk[..read]);
    }
    let text = String::from_utf8_lossy(&info);

    assert!(text.contains("other_commands:2\r\n"), "{text}");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

fn read_line(client: &mut StdTcpStream) -> String {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        client.read_exact(&mut byte).unwrap();
        line.push(byte[0]);

        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);

            return String::from_utf8(line).unwrap();
        }
    }
}

#[test]
fn scan_walks_every_shard_exactly_once() {
    let (address, stop, server) = serve_single_port(4);
    let mut client = connect(address);
    let mut payload = Vec::new();

    for index in 0..60 {
        let key = format!("scan:{index}");

        payload.extend(resp(&[&[b"SET", key.as_bytes(), b"v"]]));
    }

    client.write_all(&payload).unwrap();
    read_exactly(&mut client, &b"+OK\r\n".repeat(60));

    let mut seen = std::collections::BTreeSet::new();
    let mut cursor = "0".to_owned();
    let mut pages = 0;

    loop {
        client
            .write_all(&resp(&[&[b"SCAN", cursor.as_bytes(), b"COUNT", b"7"]]))
            .unwrap();
        assert_eq!(read_line(&mut client), "*2");
        read_line(&mut client);
        cursor = read_line(&mut client);
        let keys: usize = read_line(&mut client)[1..].parse().unwrap();

        for _ in 0..keys {
            read_line(&mut client);
            assert!(seen.insert(read_line(&mut client)), "key returned twice");
        }

        pages += 1;
        assert!(pages < 200, "SCAN did not terminate");

        if cursor == "0" {
            break;
        }
    }

    assert_eq!(seen.len(), 60);

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn restricted_user_is_refused_on_every_shard_before_forwarding() {
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let mut config = AppConfig {
        address: address.to_string(),
        shards: 4,
        port_mode: crate::config::PortMode::Single,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = crate::DurabilityProfile::Memory;
    config.security.requirepass = "secret".into();
    config.security.users = vec![
        crate::acl::AclUser::parse("bob", "on >pw ~app:* +get +set +lpush +auth +ping +dbsize")
            .unwrap(),
    ];
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || run(&config, shutdown).unwrap());

    wait_until_listening(address, &server);
    let mut payload = b"*3\r\n$4\r\nAUTH\r\n$3\r\nbob\r\n$2\r\npw\r\n".to_vec();
    let keys = 32;

    for index in 0..keys {
        let key = format!("secret:{index}");

        payload.extend_from_slice(
            format!(
                "*3\r\n$5\r\nLPUSH\r\n${}\r\n{key}\r\n$1\r\nx\r\n",
                key.len()
            )
            .as_bytes(),
        );
    }

    let mut client = connect(address);

    client.write_all(&payload).unwrap();
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];

    while String::from_utf8_lossy(&seen).matches("NOPERM").count() < keys {
        let read = client.read(&mut chunk).unwrap();

        assert!(read > 0, "connection closed early");
        seen.extend_from_slice(&chunk[..read]);
    }

    let text = String::from_utf8_lossy(&seen);

    assert!(text.starts_with("+OK\r\n"), "{text}");
    assert_eq!(text.matches("NOPERM").count(), keys, "{text}");
    assert!(
        !text.contains(":1\r\n"),
        "no forbidden write succeeded: {text}"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

fn serve_secured(
    shards: usize,
    users: &[(&str, &str)],
    max_commands_per_second: u64,
) -> (SocketAddr, ShutdownToken, thread::JoinHandle<()>) {
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let mut config = AppConfig {
        address: address.to_string(),
        shards,
        port_mode: crate::config::PortMode::Single,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence.profile = crate::DurabilityProfile::Memory;
    config.security.requirepass = "secret".into();
    config.security.users = users
        .iter()
        .map(|(name, rules)| crate::acl::AclUser::parse(name, rules).unwrap())
        .collect();
    config.connections.max_commands_per_second = max_commands_per_second;
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || {
        let _ports = ports;

        run(&config, shutdown).unwrap()
    });

    wait_until_listening(address, &server);
    (address, stop, server)
}

/// Reads until `count` complete top-level replies of the simple kinds the tests use arrived.
fn read_replies(client: &mut StdTcpStream, count: usize) -> Vec<String> {
    let mut replies = Vec::new();

    while replies.len() < count {
        let line = read_line(client);
        let reply = match line.as_bytes().first() {
            Some(b'$') if line != "$-1" => {
                let body = read_line(client);

                format!("{line}\r\n{body}")
            }
            _ => line,
        };

        replies.push(reply);
    }

    replies
}

#[test]
fn unauthenticated_scan_is_refused_for_every_shard_cursor() {
    let (address, stop, server) = serve_secured(2, &[], 0);
    let keys = two_shard_keys("hidden");
    let mut admin = connect(address);

    admin
        .write_all(&resp(&[
            &[b"AUTH", b"secret"],
            &[b"SET", keys[0].as_bytes(), b"v"],
            &[b"SET", keys[1].as_bytes(), b"v"],
        ]))
        .unwrap();
    read_exactly(&mut admin, b"+OK\r\n+OK\r\n+OK\r\n");

    // Both connections, whichever shard they land on, and both shard cursors.
    for _ in 0..2 {
        let mut anonymous = connect(address);
        let second_shard = (1u64 << 56).to_string();

        anonymous
            .write_all(&resp(&[
                &[b"SCAN", b"0", b"COUNT", b"100"],
                &[b"SCAN", second_shard.as_bytes(), b"COUNT", b"100"],
            ]))
            .unwrap();
        assert_eq!(
            read_replies(&mut anonymous, 2),
            vec!["-NOAUTH Authentication required."; 2]
        );
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn scan_obeys_the_command_acl_on_every_shard() {
    let (address, stop, server) = serve_secured(2, &[("ann", "on >pw ~* +@all -scan")], 0);

    for _ in 0..2 {
        let mut client = connect(address);

        client
            .write_all(&resp(&[
                &[b"AUTH", b"ann", b"pw"],
                &[b"SCAN", b"0"],
                &[b"SCAN", (1u64 << 56).to_string().as_bytes()],
            ]))
            .unwrap();
        let replies = read_replies(&mut client, 3);

        assert_eq!(replies[0], "+OK");
        assert!(replies[1].starts_with("-NOPERM"), "{replies:?}");
        assert!(replies[2].starts_with("-NOPERM"), "{replies:?}");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn forwarded_commands_count_against_the_rate_limit() {
    let (address, stop, server) = serve_secured(2, &[], 3);
    let keys = two_shard_keys("rated");
    let mut client = connect(address);
    let mut frames = vec![vec![b"AUTH".as_slice(), b"secret".as_slice()]];

    // STRLEN is not on the hot path, so the remote one is forwarded as a parsed command.
    for _ in 0..3 {
        frames.push(vec![b"STRLEN", keys[0].as_bytes()]);
        frames.push(vec![b"STRLEN", keys[1].as_bytes()]);
    }

    frames.push(vec![b"SCAN", b"0"]);
    let borrowed: Vec<&[&[u8]]> = frames.iter().map(Vec::as_slice).collect();

    client.write_all(&resp(&borrowed)).unwrap();
    let replies = read_replies(&mut client, frames.len());
    let limited = replies
        .iter()
        .filter(|reply| reply.contains("per second exceeded"))
        .count();

    // Three tokens: AUTH and the first two STRLENs pass; the other four STRLENs (local and
    // forwarded alike) and the SCAN are refused.
    assert_eq!(limited, 5, "{replies:?}");
    assert_eq!(replies[1..3], [":0", ":0"], "{replies:?}");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn scripts_forwarded_to_another_shard_obey_the_callers_acl() {
    let (address, stop, server) = serve_secured(2, &[("eve", "on >pw ~* +@all -del")], 0);
    let keys = two_shard_keys("victim");
    let mut admin = connect(address);

    admin
        .write_all(&resp(&[
            &[b"AUTH", b"secret"],
            &[b"SET", keys[0].as_bytes(), b"v"],
            &[b"SET", keys[1].as_bytes(), b"v"],
        ]))
        .unwrap();
    read_exactly(&mut admin, b"+OK\r\n+OK\r\n+OK\r\n");

    // Two connections cover both home shards, so each key is remote for one of them.
    for _ in 0..2 {
        let mut eve = connect(address);

        eve.write_all(&resp(&[
            &[b"AUTH", b"eve", b"pw"],
            &[
                b"EVAL",
                b"return ruvio.call('DEL', KEYS[1])",
                b"1",
                keys[0].as_bytes(),
            ],
            &[
                b"EVAL",
                b"return ruvio.call('DEL', KEYS[1])",
                b"1",
                keys[1].as_bytes(),
            ],
        ]))
        .unwrap();
        let replies = read_replies(&mut eve, 3);

        assert_eq!(replies[0], "+OK");
        assert!(replies[1].starts_with("-NOPERM"), "{replies:?}");
        assert!(replies[2].starts_with("-NOPERM"), "{replies:?}");
    }

    admin
        .write_all(&resp(&[
            &[b"EXISTS", keys[0].as_bytes()],
            &[b"EXISTS", keys[1].as_bytes()],
        ]))
        .unwrap();
    read_exactly(&mut admin, b":1\r\n:1\r\n");

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn exec_forwarded_to_another_shard_works_for_a_restricted_user() {
    let (address, stop, server) = serve_secured(2, &[("eve", "on >pw ~* +@all -del")], 0);
    let keys = two_shard_keys("queued");

    for _ in 0..2 {
        let mut eve = connect(address);

        eve.write_all(&resp(&[
            &[b"AUTH", b"eve", b"pw"],
            &[b"MULTI"],
            &[b"SET", keys[0].as_bytes(), b"1"],
            &[b"EXEC"],
            &[b"MULTI"],
            &[b"SET", keys[1].as_bytes(), b"1"],
            &[b"EXEC"],
        ]))
        .unwrap();
        read_exactly(
            &mut eve,
            b"+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n",
        );
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn aborted_transaction_is_not_executed_on_the_owning_shard() {
    let (address, stop, server) = serve_single_port(2);
    let keys = two_shard_keys("aborted");

    // Two connections cover both home shards, so each key is remote for one of them.
    for _ in 0..2 {
        let mut client = connect(address);

        for key in &keys {
            client
                .write_all(&resp(&[
                    &[b"MULTI"],
                    &[b"SET", key.as_bytes(), b"1"],
                    // A wrong arity is refused while queueing and aborts the transaction.
                    &[b"GET"],
                    &[b"EXEC"],
                    &[b"EXISTS", key.as_bytes()],
                ]))
                .unwrap();
            let replies = read_replies(&mut client, 5);

            assert_eq!(replies[..2], ["+OK", "+QUEUED"], "{replies:?}");
            assert!(replies[2].starts_with("-ERR"), "{replies:?}");
            assert!(replies[3].starts_with("-EXECABORT"), "{replies:?}");
            assert_eq!(replies[4], ":0", "{replies:?}");
        }
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn transaction_aborts_after_a_key_refused_on_either_shard() {
    let (address, stop, server) = serve_secured(2, &[("bob", "on >pw ~app:* +@all")], 0);

    // Two connections cover both home shards, so one refusal is checked before forwarding.
    for _ in 0..2 {
        let mut client = connect(address);

        for key in two_shard_keys("secret") {
            client
                .write_all(&resp(&[
                    &[b"AUTH", b"bob", b"pw"],
                    &[b"MULTI"],
                    &[b"SET", b"app:{tx}", b"1"],
                    &[b"SET", key.as_bytes(), b"1"],
                    &[b"EXEC"],
                    &[b"EXISTS", b"app:{tx}"],
                ]))
                .unwrap();
            let replies = read_replies(&mut client, 6);

            assert_eq!(replies[..3], ["+OK", "+OK", "+QUEUED"], "{replies:?}");
            assert!(replies[3].starts_with("-NOPERM"), "{replies:?}");
            assert!(replies[4].starts_with("-EXECABORT"), "{replies:?}");
            assert_eq!(replies[5], ":0", "{replies:?}");
        }
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn swapdb_and_queued_fan_commands_are_refused_with_several_shards() {
    let (address, stop, server) = serve_single_port(2);
    let keys = two_shard_keys("fan");

    for _ in 0..2 {
        let mut client = connect(address);

        client
            .write_all(&resp(&[
                &[b"SET", keys[0].as_bytes(), b"1"],
                &[b"SET", keys[1].as_bytes(), b"1"],
                &[b"SWAPDB", b"0", b"1"],
                &[b"MULTI"],
                &[b"FLUSHALL"],
                &[b"EXEC"],
                &[b"MULTI"],
                &[b"SWAPDB", b"0", b"1"],
                &[b"EXEC"],
                &[b"EXISTS", keys[0].as_bytes()],
                &[b"EXISTS", keys[1].as_bytes()],
            ]))
            .unwrap();
        let replies = read_replies(&mut client, 11);

        assert_eq!(replies[..2], ["+OK", "+OK"], "{replies:?}");
        assert!(
            replies[2].contains("SWAPDB is not supported"),
            "{replies:?}"
        );
        assert_eq!(replies[3], "+OK");
        assert!(replies[4].contains("cannot be queued"), "{replies:?}");
        assert!(replies[5].starts_with("-EXECABORT"), "{replies:?}");
        assert_eq!(replies[6], "+OK");
        assert!(
            replies[7].contains("SWAPDB is not supported"),
            "{replies:?}"
        );
        assert!(replies[8].starts_with("-EXECABORT"), "{replies:?}");
        // Nothing was flushed on either shard.
        assert_eq!(replies[9..], [":1", ":1"], "{replies:?}");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn wait_counts_replicas_that_reached_every_shard() {
    let directory = tempfile::tempdir().unwrap();
    // The data port, then one replication port per shard (`replication_port + shard`).
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let replication_port = ports.port_at(2);
    let mut config = AppConfig {
        address: address.to_string(),
        shards: 2,
        port_mode: crate::config::PortMode::Single,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    config.replication.role = crate::config::ReplicationRole::Primary;
    config.replication.listen = Some(format!("127.0.0.1:{replication_port}").parse().unwrap());
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || run(&config, shutdown).unwrap());

    wait_until_listening(address, &server);
    // A replica of shard 0 only, which acknowledges everything that shard will ever write.
    // The replication listener may come up a moment after the RESP port.
    let mut replica = connect_replication(replication_port, &server);
    let handshake = crate::replication::encode_handshake(&crate::replication::Handshake {
        version: crate::replication::VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        password: Vec::new(),
        history: [0; crate::replication::HISTORY_LEN],
        shard_count: 2,
        epoch: 0,
        announce: None,
    })
    .unwrap();

    replica.write_all(&handshake).unwrap();
    replica
        .write_all(&crate::replication::encode_ack(1_000_000))
        .unwrap();
    let keys = two_shard_keys("waited");
    let mut writer = connect(address);

    writer
        .write_all(&resp(&[&[b"SET", keys[1].as_bytes(), b"v"]]))
        .unwrap();
    read_exactly(&mut writer, b"+OK\r\n");

    // Whichever shard a connection lives on, shard 1's write has no replica.
    for _ in 0..2 {
        let mut client = connect(address);

        client
            .write_all(&resp(&[&[b"WAIT", b"1", b"300"]]))
            .unwrap();
        read_exactly(&mut client, b":0\r\n");
    }

    drop(replica);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A blocking pop forwarded to the shard that owns its key does not hold back the replies
/// queued ahead of it on this connection.
#[test]
fn replies_ahead_of_a_forwarded_blocking_pop_are_sent_before_it_waits() {
    let (address, stop, server) = serve_single_port(2);

    // One of the two keys lives on a shard other than the connection's.
    for shard in 0..2 {
        let key = key_on_shard(shard, "never");
        let mut client = connect(address);

        client
            .write_all(&resp(&[
                &[b"SET", b"ahead", b"1"],
                &[b"BLPOP", key.as_bytes(), b"0"],
            ]))
            .unwrap();
        read_exactly(&mut client, b"+OK\r\n");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// `FILLGET ... WAIT` forwarded to the shard that owns its key does not hold back the
/// replies queued ahead of it while it waits for the filler.
#[test]
fn replies_ahead_of_a_forwarded_fillget_wait_are_sent_before_it_waits() {
    let (address, stop, server) = serve_single_port(2);

    for shard in 0..2 {
        let key = key_on_shard(shard, "fill");
        let mut filler = connect(address);

        filler
            .write_all(&resp(&[&[b"FILLGET", key.as_bytes(), b"5000"]]))
            .unwrap();
        read_exactly(&mut filler, b"*2\r\n$4\r\nFILL\r\n");

        let mut client = connect(address);

        client
            .write_all(&resp(&[
                &[b"SET", b"ahead", b"1"],
                &[b"FILLGET", key.as_bytes(), b"5000", b"WAIT", b"4000"],
            ]))
            .unwrap();
        read_exactly(&mut client, b"+OK\r\n");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A blocking pop parked on a shard whose WAL sync then fails, woken by a push that hands it
/// the element: the pop writes no record, so the records other clients wrote meanwhile (and
/// the failure that cut them) are not its own. It gets the element; a MISCONF there would
/// tell the client the pop did not happen although the element is gone.
#[test]
fn a_pop_woken_after_a_wal_sync_failure_gets_its_element() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_single_port_always(2, directory.path());
    let keys = [key_on_shard(1, "woken"), key_on_shard(1, "woken-too")];
    let other = key_on_shard(1, "other");
    // Consecutive connections land on different shards: one pop is forwarded, one is local.
    let mut blocked: Vec<StdTcpStream> = keys
        .iter()
        .map(|key| {
            let mut client = connect(address);

            client
                .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"5"]]))
                .unwrap();
            client
        })
        .collect();

    wait_for_parked(address, 2);
    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .push((directory.path().to_path_buf(), 1));
    let mut writer = connect(address);

    writer
        .write_all(&resp(&[&[b"SET", other.as_bytes(), b"refused"]]))
        .unwrap();
    // Refused with an error, or (on the owning shard) by closing the connection.
    let mut refused = [0u8; 64];
    let length = writer.read(&mut refused).unwrap_or(0);

    crate::persistence::FAILING_SYNC_SHARDS
        .lock()
        .unwrap()
        .retain(|(dir, _)| dir != directory.path());
    assert!(
        length == 0 || refused[0] == b'-',
        "{:?}",
        &refused[..length]
    );

    for (client, key) in blocked.iter_mut().zip(&keys) {
        let mut pusher = connect(address);

        pusher
            .write_all(&resp(&[&[b"RPUSH", key.as_bytes(), b"val"]]))
            .unwrap();
        assert_eq!(read_line(&mut pusher), ":1");

        let mut expected = b"*2\r\n".to_vec();

        expected.extend(bulk_reply(key));
        expected.extend(bulk_reply("val"));
        read_exactly(client, &expected);
    }

    stop.request();
    let _ = join_within(server, PATIENCE);
}

/// A blocking pop served by a push whose own WAL sync then fails: the element came with that
/// push (`RPUSH k a b` hands `a` over and logs `b`), and the push is refused. The pop must be
/// refused too, not answered with `a` from a write the client was told did not happen.
#[test]
fn a_pop_served_by_a_push_whose_sync_fails_is_refused() {
    // Consecutive connections land on different shards: the first pop is local or forwarded.
    for served in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let (address, stop, server) = serve_single_port_always(2, directory.path());
        let keys = [key_on_shard(1, "served"), key_on_shard(1, "served-too")];
        let mut blocked: Vec<StdTcpStream> = keys
            .iter()
            .map(|key| {
                let mut client = connect(address);

                client
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                client
                    .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"1"]]))
                    .unwrap();
                client
            })
            .collect();

        wait_for_parked(address, 2);
        // Only the push's own sync fails; the log then refuses every later batch. The pop
        // resumes after it, so its own commit finds the log already failed.
        super::super::connection::DELAY_SERVED_POPS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        crate::persistence::FAILING_NEXT_SYNC_SHARDS
            .lock()
            .unwrap()
            .push((directory.path().to_path_buf(), 1));
        let mut pusher = connect(address);

        pusher
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        pusher
            .write_all(&resp(&[&[b"RPUSH", keys[served].as_bytes(), b"a", b"b"]]))
            .unwrap();
        let mut pushed = [0u8; 64];
        let pushed_length = pusher.read(&mut pushed).unwrap_or(0);
        let mut popped = [0u8; 64];
        let popped_length = blocked[served].read(&mut popped).unwrap_or(0);

        super::super::connection::DELAY_SERVED_POPS
            .lock()
            .unwrap()
            .retain(|dir| dir != directory.path());
        crate::persistence::FAILING_NEXT_SYNC_SHARDS
            .lock()
            .unwrap()
            .retain(|(dir, _)| dir != directory.path());
        // Refused with an error, or (on the owning shard) by closing the connection.
        assert!(
            pushed_length == 0 || pushed[0] == b'-',
            "{:?}",
            String::from_utf8_lossy(&pushed[..pushed_length])
        );
        assert!(
            popped_length == 0 || popped[0] == b'-',
            "pop {served} got {:?}",
            String::from_utf8_lossy(&popped[..popped_length])
        );
        stop.request();
        let _ = join_within(server, PATIENCE);
    }
}

/// The commit before a forwarded transaction's MULTI fails (another client's records in the
/// owner's batch did not reach the log). The transaction must fail as a whole: its queued
/// commands may not run one by one without the MULTI, and EXEC answers the persistence error.
#[test]
fn a_forwarded_transaction_whose_multi_commit_failed_runs_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_single_port_always(2, directory.path());
    let mut client = connect(address);
    let mut replies = Vec::new();

    for key in two_shard_keys("tx") {
        // Only the forwarded transaction meets the injected failure.
        super::super::forward::FAILING_FORWARDED_COMMITS
            .lock()
            .unwrap()
            .push(directory.path().to_path_buf());
        client
            .write_all(&resp(&[
                &[b"MULTI"],
                &[b"SET", key.as_bytes(), b"v"],
                &[b"GET", key.as_bytes()],
                &[b"EXEC"],
            ]))
            .unwrap();
        let lines: Vec<String> = (0..3).map(|_| read_line(&mut client)).collect();
        let exec = read_line(&mut client);

        if exec == "*2" {
            // `+OK`, then the value as a bulk string.
            let rest: Vec<String> = (0..3).map(|_| read_line(&mut client)).collect();

            assert_eq!(rest, ["+OK", "$1", "v"]);
        }
        assert_eq!(lines, ["+OK", "+QUEUED", "+QUEUED"]);
        replies.push((key, exec));
    }
    super::super::forward::FAILING_FORWARDED_COMMITS
        .lock()
        .unwrap()
        .retain(|dir| dir != directory.path());

    let failed: Vec<_> = replies
        .iter()
        .filter(|(_, exec)| exec.starts_with('-'))
        .collect();

    assert_eq!(failed.len(), 1, "{replies:?}");
    let (key, exec) = failed[0];

    assert_eq!(exec, "-MISCONF persistence error");
    client
        .write_all(&resp(&[&[b"EXISTS", key.as_bytes()]]))
        .unwrap();
    assert_eq!(read_line(&mut client), ":0");
    stop.request();
    let _ = join_within(server, PATIENCE);
}

/// A forwarded push that only hands its element to a parked client logs nothing; another
/// command of the same forwarded batch then fails its sync. The pop already has the element
/// and answers it, so the push must answer too: a MISCONF would make its client push the
/// element again, and it would be popped twice.
#[test]
fn a_forwarded_push_that_only_served_a_pop_keeps_its_reply_when_the_batch_sync_fails() {
    let mut forwarded_runs = 0;

    for shard in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let (address, stop, server) = serve_single_port_always(2, directory.path());
        let key = key_on_shard(shard, "handed");
        let other = key_on_shard(shard, "refused");
        let mut popper = connect(address);
        let mut pusher = connect(address);

        popper
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        pusher
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        popper
            .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"5"]]))
            .unwrap();
        wait_for_parked(address, 1);
        crate::persistence::FAILING_SYNC_SHARDS
            .lock()
            .unwrap()
            .push((directory.path().to_path_buf(), shard as u32));
        // One write, so both commands travel in one forwarded batch.
        pusher
            .write_all(
                &[
                    resp(&[&[b"SET", other.as_bytes(), b"x"]]),
                    resp(&[&[b"LPUSH", key.as_bytes(), b"v"]]),
                ]
                .concat(),
            )
            .unwrap();
        let mut replies = Vec::new();
        let mut buffer = [0u8; 256];

        while !replies.ends_with(b"\r\n") || replies.iter().filter(|&&b| b == b'\n').count() < 2 {
            match pusher.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => replies.extend_from_slice(&buffer[..read]),
            }
        }
        let mut popped = [0u8; 64];
        let popped_length = popper.read(&mut popped).unwrap_or(0);

        crate::persistence::FAILING_SYNC_SHARDS
            .lock()
            .unwrap()
            .retain(|(dir, _)| dir != directory.path());
        stop.request();
        let _ = join_within(server, PATIENCE);

        // On the owning shard the pusher's connection is closed instead; nothing to check.
        if replies.is_empty() {
            continue;
        }

        forwarded_runs += 1;
        let replies = String::from_utf8_lossy(&replies).into_owned();
        let mut lines = replies.split("\r\n");

        assert_eq!(
            lines.next(),
            Some("-MISCONF persistence error"),
            "{replies:?}"
        );
        assert!(
            lines.next().is_some_and(|push| push.starts_with(':')),
            "{replies:?}"
        );

        let mut expected = b"*2\r\n".to_vec();

        expected.extend(bulk_reply(&key));
        expected.extend(bulk_reply("v"));
        assert_eq!(
            String::from_utf8_lossy(&popped[..popped_length]),
            String::from_utf8_lossy(&expected)
        );
    }

    assert_eq!(forwarded_runs, 1, "one of the pushes must be forwarded");
}

/// Two pops wait on one list. A push that logs nothing serves the first; a second push
/// serves the second and logs its leftover element, and that sync fails. Neither the second
/// pop nor its push may answer as if it happened: the second pop waited through the first
/// push's delivery, which is not one of its own.
#[test]
fn a_pop_that_waited_through_another_delivery_is_refused_with_its_failed_push() {
    for shard in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let (address, stop, server) = serve_single_port_always(2, directory.path());
        let key = key_on_shard(shard, "queue");
        // Consecutive connections land on different shards: one pop is local, one forwarded.
        // Each pop is parked before the next is sent: the first in line is known.
        let mut pops: Vec<StdTcpStream> = (0..2)
            .map(|index| {
                let mut client = connect(address);

                client
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                client
                    .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"5"]]))
                    .unwrap();
                wait_for_parked(address, index + 1);
                client
            })
            .collect();
        let mut first_pusher = connect(address);

        first_pusher
            .write_all(&resp(&[&[b"LPUSH", key.as_bytes(), b"a"]]))
            .unwrap();
        assert!(read_line(&mut first_pusher).starts_with(':'));

        let mut expected = b"*2\r\n".to_vec();

        expected.extend(bulk_reply(&key));
        expected.extend(bulk_reply("a"));
        read_exactly(&mut pops[0], &expected);

        crate::persistence::FAILING_SYNC_SHARDS
            .lock()
            .unwrap()
            .push((directory.path().to_path_buf(), shard as u32));
        let mut second_pusher = connect(address);

        second_pusher
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        // `b` goes to the second pop, `c` is logged.
        second_pusher
            .write_all(&resp(&[&[b"RPUSH", key.as_bytes(), b"b", b"c"]]))
            .unwrap();
        let mut pushed = [0u8; 64];
        let pushed_length = second_pusher.read(&mut pushed).unwrap_or(0);
        let mut popped = [0u8; 64];
        let popped_length = pops[1].read(&mut popped).unwrap_or(0);

        crate::persistence::FAILING_SYNC_SHARDS
            .lock()
            .unwrap()
            .retain(|(dir, _)| dir != directory.path());
        stop.request();
        let _ = join_within(server, PATIENCE);

        assert!(
            pushed_length == 0 || pushed[0] == b'-',
            "shard {shard}: push got {:?}",
            String::from_utf8_lossy(&pushed[..pushed_length])
        );
        assert!(
            popped_length == 0 || popped[0] == b'-',
            "shard {shard}: second pop got {:?}",
            String::from_utf8_lossy(&popped[..popped_length])
        );
    }
}

/// Under `everysec` a write is answered once its record reached the kernel, so a process kill
/// keeps it. One connection holds a local write in its open WAL group while it waits for
/// another shard to run its script; a write another client makes meanwhile on the first
/// shard must still reach the log file before its `+OK`, not wait in memory behind that group.
#[test]
fn an_everysec_write_reaches_the_log_before_its_reply_while_a_batch_waits_on_another_shard() {
    const SLOW: &[u8] = b"local i = 0 while i < tonumber(ARGV[1]) do i = i + 1 end return 1";

    // The holder's home shard depends on the order connections were handed out: try both.
    for home in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let (address, stop, server) = serve_single_port_durable(
            2,
            directory.path(),
            crate::DurabilityProfile::Everysec,
            |config| {
                config.scripts.max_instructions = 1 << 31;
                config.scripts.max_time_ms = 60_000;
            },
        );
        let held = key_on_shard(home, "held");
        let slow = key_on_shard(1 - home, "slow");
        let acked = key_on_shard(home, "acked");
        let value = format!("acked-value-{home}");
        let mut holder = connect(address);
        let _other_home = connect(address);
        // Two connections later: the holder's home shard again.
        let mut writer = connect(address);

        for client in [&holder, &writer] {
            client
                .set_read_timeout(Some(Duration::from_secs(60)))
                .unwrap();
        }
        holder
            .write_all(&resp(&[
                &[b"SET", held.as_bytes(), b"v"],
                &[b"EVAL", SLOW, b"1", slow.as_bytes(), b"3000000"],
            ]))
            .unwrap();
        thread::sleep(Duration::from_millis(100));
        writer
            .write_all(&resp(&[&[b"SET", acked.as_bytes(), value.as_bytes()]]))
            .unwrap();
        read_exactly(&mut writer, b"+OK\r\n");

        let logged = std::fs::read_dir(directory.path().join(format!("wal/shard-{home:04}")))
            .unwrap()
            .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
            .any(|bytes| bytes.windows(value.len()).any(|w| w == value.as_bytes()));

        read_exactly(&mut holder, b"+OK\r\n:1\r\n");
        stop.request();
        let _ = join_within(server, PATIENCE);
        assert!(
            logged,
            "an acknowledged write of shard {home} was not in its WAL file"
        );
    }
}

/// A batch's group stays open while its connection waits on its own client, and every record
/// the shard writes meanwhile is only staged. Here a client fills its socket with replies it
/// does not read, then opens a group with a write and sends a frame that cannot be parsed:
/// the connection waits to send the error. Another client's `everysec` write made meanwhile
/// must still reach the log file before its `+OK`.
#[test]
fn an_everysec_write_reaches_the_log_before_its_reply_while_a_stalled_client_holds_a_group() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_single_port_durable(
        1,
        directory.path(),
        crate::DurabilityProfile::Everysec,
        |_| {},
    );
    let big = vec![b'b'; 1 << 20];
    let mut holder = connect(address);
    let mut writer = connect(address);

    for client in [&holder, &writer] {
        client
            .set_read_timeout(Some(Duration::from_secs(60)))
            .unwrap();
    }
    holder.write_all(&resp(&[&[b"SET", b"big", &big]])).unwrap();
    read_exactly(&mut holder, b"+OK\r\n");

    // About 24 MiB of replies: more than the socket takes, under half the output limit.
    let mut pipeline = resp(&vec![[b"GET".as_slice(), b"big"].as_slice(); 24]);

    pipeline.extend_from_slice(&resp(&[&[b"SET", b"held", b"v"]]));
    pipeline.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$99999999999\r\n");
    holder.write_all(&pipeline).unwrap();
    thread::sleep(Duration::from_millis(300));

    writer
        .write_all(&resp(&[&[b"SET", b"acked", b"acked-value"]]))
        .unwrap();
    read_exactly(&mut writer, b"+OK\r\n");
    let logged = std::fs::read_dir(directory.path().join("wal/shard-0000"))
        .unwrap()
        .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
        .any(|bytes| bytes.windows(11).any(|w| w == b"acked-value"));

    drop(holder);
    stop.request();
    let _ = join_within(server, PATIENCE);
    assert!(logged, "an acknowledged write was not in its WAL file");
}

/// A client that hangs up while its blocking pop waits on another shard gives up the
/// waiter there too: a later push is not popped for nobody.
#[test]
fn a_client_that_hangs_up_during_a_forwarded_blocking_pop_gives_up_its_waiter() {
    let (address, stop, server) = serve_single_port(2);

    // Consecutive connections land on different shards, so one of the two pops is forwarded.
    for shard in 0..2 {
        let key = key_on_shard(shard, "left");
        let mut blocked = connect(address);

        blocked
            .write_all(&resp(&[&[b"BLPOP", key.as_bytes(), b"0"]]))
            .unwrap();
        wait_for_parked(address, 1);
        drop(blocked);
        wait_for_parked(address, 0);

        let mut client = connect(address);

        client
            .write_all(&resp(&[
                &[b"RPUSH", key.as_bytes(), b"x"],
                &[b"LLEN", key.as_bytes()],
                &[b"DEL", key.as_bytes()],
            ]))
            .unwrap();
        read_exactly(&mut client, b":1\r\n:1\r\n:1\r\n");
    }

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_hot_keys_serve_database_zero_only() {
    let directory = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_single_port_durable(
        2,
        directory.path(),
        crate::DurabilityProfile::Memory,
        |config| config.hot_keys = vec![b"lock:*".to_vec()],
    );

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"SET", b"lock:a", b"zero"],
                &[b"SELECT", b"1"],
                &[b"GET", b"lock:a"],
                &[b"LPUSH", b"lock:list", b"x"],
                &[b"SELECT", b"0"],
                &[b"GET", b"lock:a"],
                &[b"FLUSHALL"],
                &[b"GET", b"lock:a"],
            ])
        ),
        b"+OK\r\n+OK\r\n$-1\r\n:1\r\n+OK\r\n$4\r\nzero\r\n+OK\r\n$-1\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Per-shard mode: a client that is not cluster-aware writes a key of shard 1 through shard 0's
/// port. Its WAIT must count shard 1's replicas too, not only those of the shard it is on.
#[test]
fn per_shard_wait_counts_the_shards_a_connection_forwarded_to() {
    let directory = tempfile::tempdir().unwrap();
    // Two consecutive data ports, then a replication port per shard. The reserved range sits
    // low enough for the cluster bus ports (data + 10000), which are only advertised.
    let ports = crate::test_ports::reserve();
    let address = ports.address_at(0);
    let replication_port = ports.port_at(2);
    let mut config = AppConfig {
        address: address.to_string(),
        shards: 2,
        port_mode: crate::config::PortMode::PerShard,
        shutdown_grace: Duration::from_secs(1),
        ..AppConfig::default()
    };

    config.persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        sync_interval: Duration::from_millis(1),
        ..PersistenceConfig::default()
    };
    config.replication.role = crate::config::ReplicationRole::Primary;
    config.replication.listen = Some(format!("127.0.0.1:{replication_port}").parse().unwrap());
    let shutdown = ShutdownToken::default();
    let stop = shutdown.clone();
    let server = thread::spawn(move || run(&config, shutdown));

    wait_until("both data ports to answer PING", || {
        assert!(!server.is_finished(), "the per-shard server ended early");
        pings(address) && pings(next_port(address))
    });
    // A replica of shard 0 only, which acknowledges everything that shard will ever write.
    let mut replica = connect_replication(replication_port, &server);
    let handshake = crate::replication::encode_handshake(&crate::replication::Handshake {
        version: crate::replication::VERSION_RESUME,
        shard_id: 0,
        after_sequence: 0,
        password: Vec::new(),
        history: [0; crate::replication::HISTORY_LEN],
        shard_count: 2,
        epoch: 0,
        announce: None,
    })
    .unwrap();

    replica.write_all(&handshake).unwrap();
    replica
        .write_all(&crate::replication::encode_ack(1_000_000))
        .unwrap();

    // A connection on shard 0 that wrote only there: its replica counts.
    let mut local = connect(address);

    local
        .write_all(&resp(&[&[
            b"SET",
            key_on_shard(0, "wait-local").as_bytes(),
            b"v",
        ]]))
        .unwrap();
    read_exactly(&mut local, b"+OK\r\n");
    local.write_all(&resp(&[&[b"WAIT", b"1", b"300"]])).unwrap();
    read_exactly(&mut local, b":1\r\n");

    // A read forwarded to shard 1 does not make WAIT wait for shard 1.
    let mut reader = connect(address);

    reader
        .write_all(&resp(&[&[b"GET", key_on_shard(1, "wait-read").as_bytes()]]))
        .unwrap();
    read_exactly(&mut reader, b"$-1\r\n");
    reader
        .write_all(&resp(&[&[b"WAIT", b"1", b"300"]]))
        .unwrap();
    read_exactly(&mut reader, b":1\r\n");

    // The same port, a write forwarded to shard 1, which has no replica.
    let mut forwarded = connect(address);

    forwarded
        .write_all(&resp(&[&[
            b"SET",
            key_on_shard(1, "wait-forwarded").as_bytes(),
            b"v",
        ]]))
        .unwrap();
    read_exactly(&mut forwarded, b"+OK\r\n");
    forwarded
        .write_all(&resp(&[&[b"WAIT", b"1", b"300"]]))
        .unwrap();
    read_exactly(&mut forwarded, b":0\r\n");

    drop(replica);
    stop.request();
    join_within(server, PATIENCE).unwrap().unwrap();
}

/// In single-port mode the acceptor admits connections for every shard: `CONFIG SET
/// max-connections` reaches its limiter too, and every shard reports the new value.
#[test]
fn single_port_config_set_reaches_the_acceptor_and_every_shard() {
    let (address, stop, server) = serve_single_port(2);
    let mut first = connect_bounded(address);

    first
        .write_all(&resp(&[&[
            b"CONFIG",
            b"SET",
            b"max-connections",
            b"1",
            b"slowlog-max-len",
            b"9",
        ]]))
        .unwrap();
    let mut reply = [0u8; 5];
    first.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n");

    let mut refused = connect_bounded(address);
    let mut answer = Vec::new();
    refused.read_to_end(&mut answer).unwrap();
    assert_eq!(answer, b"-ERR max number of clients reached\r\n");

    first
        .write_all(&resp(&[&[b"CONFIG", b"SET", b"max-connections", b"100"]]))
        .unwrap();
    first.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n");
    drop(first);

    // Connections are spread over both shards; each answers with the new value.
    for _ in 0..6 {
        assert_eq!(
            request(address, &resp(&[&[b"CONFIG", b"GET", b"slowlog-max-len"]])),
            b"*2\r\n$15\r\nslowlog-max-len\r\n$1\r\n9\r\n"
        );
    }
    stop.request();
    join_within(server, PATIENCE).unwrap();
}
