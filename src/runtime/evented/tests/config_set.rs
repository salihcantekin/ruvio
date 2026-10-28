use super::*;

fn config_get(address: SocketAddr, name: &[u8]) -> Vec<u8> {
    request(address, &resp(&[&[b"CONFIG", b"GET", name]]))
}

fn pair(name: &str, value: &str) -> Vec<u8> {
    format!(
        "*2\r\n${}\r\n{name}\r\n${}\r\n{value}\r\n",
        name.len(),
        value.len()
    )
    .into_bytes()
}

/// A live setting changes at once, shows in `CONFIG GET` and is used by the next command.
#[test]
fn config_set_changes_a_live_setting_and_config_get_shows_it() {
    let (address, stop, server) = serve_with(|config| config.slowlog_threshold_micros = -1);

    assert_eq!(
        request(
            address,
            &resp(&[&[
                b"CONFIG",
                b"SET",
                b"slowlog-log-slower-than",
                b"0",
                b"SLOWLOG-MAX-LEN",
                b"5"
            ]])
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        config_get(address, b"slowlog-log-slower-than"),
        pair("slowlog-log-slower-than", "0")
    );
    assert_eq!(
        config_get(address, b"slowlog-max-len"),
        pair("slowlog-max-len", "5")
    );

    // Every command is now slow enough for the log, which keeps five. (`GET`, `SET` and
    // `PING` take the hot path, which `SLOWLOG` does not time.)
    for _ in 0..8 {
        request(address, &resp(&[&[b"HSET", b"h", b"f", b"v"]]));
    }
    assert_eq!(request(address, &resp(&[&[b"SLOWLOG", b"LEN"]])), b":5\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A restart-only, unknown or invalid name fails the whole call and changes nothing.
#[test]
fn config_set_refuses_restart_only_unknown_and_invalid_values_as_a_whole() {
    let (address, stop, server) = serve_with(|config| config.slowlog_max_len = 7);

    assert_eq!(
        request(
            address,
            &resp(&[&[b"CONFIG", b"SET", b"slowlog-max-len", b"9", b"shards", b"4"]])
        ),
        b"-ERR CONFIG SET failed (possibly related to argument 'shards') - restart-only setting; change it in the config file and restart\r\n"
    );
    assert_eq!(
        request(
            address,
            &resp(&[&[b"CONFIG", b"SET", b"no-such-setting", b"1"]])
        ),
        b"-ERR Unknown option or number of arguments for CONFIG SET - 'no-such-setting'\r\n"
    );
    assert_eq!(
        request(
            address,
            &resp(&[&[b"CONFIG", b"SET", b"slowlog-max-len", b"9", b"maxmemory-samples", b"0"]])
        ),
        b"-ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len', 'maxmemory-samples') - maxmemory-samples must be between 1 and 64\r\n"
    );
    assert_eq!(
        request(
            address,
            &resp(&[&[
                b"CONFIG",
                b"SET",
                b"slowlog-max-len",
                b"9",
                b"slowlog-max-len",
                b"8"
            ]])
        ),
        b"-ERR Duplicate parameter - 'slowlog-max-len'\r\n"
    );
    assert_eq!(
        request(address, &resp(&[&[b"CONFIG", b"SET", b"key-stats", b"maybe"]])),
        b"-ERR CONFIG SET failed (possibly related to argument 'key-stats') - the value must be a boolean\r\n"
    );
    assert_eq!(
        config_get(address, b"slowlog-max-len"),
        pair("slowlog-max-len", "7")
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// `CONFIG SET` and `CONFIG REWRITE` are refused inside `MULTI`, as other commands that
/// cannot be queued are.
#[test]
fn config_set_is_refused_inside_a_transaction() {
    let (address, stop, server) = serve_with(|_| {});

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"MULTI"],
                &[b"CONFIG", b"SET", b"slowlog-max-len", b"1"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n-ERR command not allowed inside MULTI\r\n-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// `requirepass` is the default user's password: new connections must log in, the one
/// that set it stays logged in, and an empty value turns the password off again.
#[test]
fn config_set_requirepass_applies_to_new_connections() {
    let (address, stop, server) = serve_with(|_| {});
    let mut admin = connect_bounded(address);

    admin
        .write_all(&resp(&[&[b"CONFIG", b"SET", b"requirepass", b"s3cret"]]))
        .unwrap();
    let mut reply = [0u8; 5];
    admin.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n");

    assert_eq!(
        request(address, &resp(&[&[b"PING"]])),
        b"-NOAUTH Authentication required.\r\n"
    );
    assert_eq!(
        request(address, &resp(&[&[b"AUTH", b"s3cret"], &[b"PING"]])),
        b"+OK\r\n+PONG\r\n"
    );
    assert_eq!(
        request(
            address,
            &resp(&[&[b"AUTH", b"s3cret"], &[b"CONFIG", b"GET", b"requirepass"]])
        ),
        [&b"+OK\r\n"[..], &pair("requirepass", "***")].concat()
    );

    admin
        .write_all(&resp(&[
            &[b"CONFIG", b"SET", b"requirepass", b""],
            &[b"PING"],
        ]))
        .unwrap();
    let mut reply = [0u8; 12];
    admin.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n+PONG\r\n");
    assert_eq!(request(address, &resp(&[&[b"PING"]])), b"+PONG\r\n");
    drop(admin);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A lower connection limit refuses the next connection; the open ones stay.
#[test]
fn config_set_max_connections_applies_to_the_next_accept() {
    let (address, stop, server) = serve_with(|_| {});
    let mut first = connect_bounded(address);

    first
        .write_all(&resp(&[&[b"CONFIG", b"SET", b"max-connections", b"1"]]))
        .unwrap();
    let mut reply = [0u8; 5];
    first.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n");

    // The refused client is told so and closed before it sends anything.
    let mut refused = connect_bounded(address);
    let mut answer = Vec::new();
    refused.read_to_end(&mut answer).unwrap();
    assert_eq!(answer, b"-ERR max number of clients reached\r\n");
    first
        .write_all(&resp(&[
            &[b"CONFIG", b"SET", b"max-connections", b"100"],
            &[b"PING"],
        ]))
        .unwrap();
    let mut reply = [0u8; 12];
    first.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+OK\r\n+PONG\r\n");
    assert_eq!(request(address, &resp(&[&[b"PING"]])), b"+PONG\r\n");
    drop(first);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A change sent to one shard is applied by every shard before `OK`: the other shard's
/// `CONFIG GET` and its store's memory limit show it at once.
#[test]
fn config_set_on_one_shard_reaches_every_shard_before_ok() {
    let (left, right, stop, left_server, right_server) = serve_two_shards();

    assert_eq!(
        request(
            left,
            &resp(&[&[
                b"CONFIG",
                b"SET",
                b"maxmemory-policy",
                b"allkeys-lru",
                b"key-stats",
                b"no"
            ]])
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        config_get(right, b"maxmemory-policy"),
        pair("maxmemory-policy", "allkeys-lru")
    );
    assert_eq!(config_get(right, b"key-stats"), pair("key-stats", "no"));

    // The second change, sent to the other shard, wins on both.
    assert_eq!(
        request(
            right,
            &resp(&[&[b"CONFIG", b"SET", b"maxmemory-policy", b"noeviction"]])
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        config_get(left, b"maxmemory-policy"),
        pair("maxmemory-policy", "noeviction")
    );
    assert_eq!(config_get(left, b"key-stats"), pair("key-stats", "no"));
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// `CONFIG REWRITE` writes the live values into the file the server started from and keeps
/// what the file says about everything else; with no file it is refused.
#[test]
fn config_rewrite_writes_live_settings_to_the_config_file() {
    let (address, stop, server) = serve_with(|_| {});

    assert_eq!(
        request(address, &resp(&[&[b"CONFIG", b"REWRITE"]])),
        b"-ERR The server is running without a config file\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ruvio.toml");
    std::fs::write(&path, "shards = 1\nslowlog_max_len = 3\n").unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.config_file = Some(path.clone());
        config.slowlog_max_len = 3;
    });

    assert_eq!(
        request(
            address,
            &resp(&[
                &[
                    b"CONFIG",
                    b"SET",
                    b"slowlog-max-len",
                    b"11",
                    b"maxmemory",
                    b"100000000"
                ],
                &[b"CONFIG", b"REWRITE"],
            ])
        ),
        b"+OK\r\n+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let written = std::fs::read_to_string(&path).unwrap();
    let mut reloaded = AppConfig::default();

    reloaded.apply_toml(&written).unwrap();
    assert_eq!(reloaded.slowlog_max_len, 11, "{written}");
    assert_eq!(reloaded.memory.max_process_bytes, 100_000_000, "{written}");
    assert_eq!(reloaded.shards, 1, "{written}");
    // The test server listens on an ephemeral port: a restart-only value the process runs
    // with but the file never named is not written.
    assert!(!written.contains("address"), "{written}");
}
