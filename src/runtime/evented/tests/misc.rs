use super::*;

#[test]
fn evented_select_isolates_databases_and_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..PersistenceConfig::default()
    };

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });

    assert_eq!(
            request(
                address,
                &resp(&[
                    &[b"SET", b"k", b"zero"],
                    &[b"SELECT", b"1"],
                    &[b"SET", b"k", b"one"],
                    &[b"GET", b"k"],
                    &[b"INCR", b"hits"],
                    &[b"DBSIZE"],
                    &[b"SELECT", b"0"],
                    &[b"GET", b"k"],
                    &[b"GET", b"hits"],
                    &[b"DBSIZE"],
                    &[b"SELECT", b"16"],
                ])
            ),
            b"+OK\r\n+OK\r\n+OK\r\n$3\r\none\r\n:1\r\n:2\r\n+OK\r\n$4\r\nzero\r\n$-1\r\n:1\r\n-ERR DB index is out of range\r\n"
        );
    stop.request();
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence.clone();
    });

    assert_eq!(
        request(
            address,
            &resp(&[&[b"SELECT", b"1"], &[b"GET", b"k"], &[b"GET", b"hits"]])
        ),
        b"+OK\r\n$3\r\none\r\n$1\r\n1\r\n"
    );
    let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();

    assert!(info.contains("db0:keys=1,expires=0"), "{info}");
    assert!(info.contains("db1:keys=2,expires=0"), "{info}");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_database_commands_work_over_the_wire() {
    let (address, stop, server) = serve_on_ephemeral();

    assert_eq!(
            request(
                address,
                &resp(&[
                    &[b"SELECT", b"2"],
                    &[b"RPUSH", b"queue", b"job"],
                    &[b"BLPOP", b"queue", b"0"],
                    &[b"SET", b"k", b"two"],
                    &[b"MOVE", b"k", b"3"],
                    &[b"SELECT", b"3"],
                    &[b"GET", b"k"],
                    &[b"SWAPDB", b"3", b"4"],
                    &[b"GET", b"k"],
                    &[b"SELECT", b"4"],
                    &[b"GET", b"k"],
                    &[b"FLUSHDB"],
                    &[b"DBSIZE"],
                ])
            ),
            b"+OK\r\n:1\r\n*2\r\n$5\r\nqueue\r\n$3\r\njob\r\n+OK\r\n:1\r\n+OK\r\n$3\r\ntwo\r\n+OK\r\n$-1\r\n+OK\r\n$3\r\ntwo\r\n+OK\r\n:0\r\n"
        );
    assert_eq!(
            request(
                address,
                &resp(&[
                    &[b"SET", b"a", b"1"],
                    &[b"SELECT", b"5"],
                    &[b"SET", b"a", b"5"],
                    &[b"MULTI"],
                    &[b"GET", b"a"],
                    &[b"FLUSHDB"],
                    &[b"EXEC"],
                    &[b"EVAL", b"return ruvio.call('GET', KEYS[1])", b"1", b"a"],
                    &[b"FLUSHALL"],
                    &[b"SELECT", b"0"],
                    &[b"DBSIZE"],
                ])
            ),
            b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n$1\r\n5\r\n+OK\r\n$-1\r\n+OK\r\n+OK\r\n:0\r\n"
        );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_cluster_mode_keeps_database_zero_only() {
    let (left, _right, stop, left_server, right_server) = serve_two_shards();

    assert_eq!(
            request(
                left,
                &resp(&[
                    &[b"SELECT", b"0"],
                    &[b"SELECT", b"1"],
                    &[b"SWAPDB", b"0", b"1"],
                ])
            ),
            b"+OK\r\n-ERR SELECT is not allowed in cluster mode\r\n-ERR SWAPDB is not supported with more than one shard\r\n"
        );
    stop.request();
    join_within(left_server, PATIENCE).unwrap();
    join_within(right_server, PATIENCE).unwrap();
}

/// `CONFIG GET` answers the running settings inside `MULTI` too, not the defaults.
#[test]
fn config_get_in_a_transaction_answers_the_running_settings() {
    let (address, stop, server) = serve_with(|config| config.slowlog_max_len = 7);

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"MULTI"],
                &[b"CONFIG", b"GET", b"slowlog-max-len"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n*1\r\n*2\r\n$15\r\nslowlog-max-len\r\n$1\r\n7\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// `EVAL`, `EVALSHA` and `FCALL` queue in `MULTI` and run in their place at `EXEC`, seeing
/// the writes queued before them; `SELECT` stays refused.
#[test]
fn scripts_queue_in_a_transaction_and_run_in_order() {
    let (address, stop, server) = serve_with(|_| {});
    let sha = String::from_utf8(request(
        address,
        &resp(&[&[b"SCRIPT", b"LOAD", b"return ruvio.call('INCR', KEYS[1])"]]),
    ))
    .unwrap();
    let sha = sha.trim_end().rsplit('\n').next().unwrap().to_owned();

    assert_eq!(
        request(
            address,
            &resp(&[&[b"FUNCTION", b"LOAD", b"seven", b"return 7"]])
        ),
        b"$5\r\nseven\r\n"
    );
    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"MULTI"],
                &[b"SET", b"n", b"1"],
                &[b"EVAL", b"return ruvio.call('INCR', KEYS[1])", b"1", b"n"],
                &[b"EVALSHA", sha.as_bytes(), b"1", b"n"],
                &[b"FCALL", b"seven", b"0"],
                &[b"GET", b"n"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*5\r\n+OK\r\n:2\r\n:3\r\n:7\r\n$1\r\n3\r\n"
    );

    // A script error is that command's reply; the others still run, as in Redis.
    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"MULTI"],
                &[b"EVALSHA", b"0000000000000000000000000000000000000000", b"0"],
                &[b"INCR", b"n"],
                &[b"EXEC"],
            ])
        ),
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n-NOSCRIPT No matching script. Please use EVAL.\r\n:4\r\n"
    );

    assert_eq!(
        request(
            address,
            &resp(&[&[b"MULTI"], &[b"PUBLISH", b"news", b"m"], &[b"EXEC"]])
        ),
        b"+OK\r\n+QUEUED\r\n*1\r\n:0\r\n"
    );

    let refused = request(
        address,
        &resp(&[&[b"MULTI"], &[b"SELECT", b"1"], &[b"EXEC"]]),
    );

    assert!(refused.ends_with(b"-EXECABORT Transaction discarded because of previous errors.\r\n"));
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// `INFO` queued in `MULTI` answers with the server's report, as it does outside one.
#[test]
fn info_in_a_transaction_reports_the_server() {
    let (address, stop, server) = serve_with(|_| {});
    let reply = request(address, &resp(&[&[b"MULTI"], &[b"INFO"], &[b"EXEC"]]));
    let text = String::from_utf8_lossy(&reply);

    assert!(
        text.contains("# Keyspace") || text.contains("# Memory"),
        "{text}"
    );
    assert!(!text.contains("ruvio_backend:portable"), "{text}");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// Every field `INFO` writes has a row in the field table of `docs/OBSERVABILITY.md`, and
/// every row names a field `INFO` writes (replica-only rows and `<…>` pattern rows aside).
#[test]
fn info_fields_and_the_observability_table_agree() {
    let (address, stop, server) = serve_with(|config| config.latency_histograms = true);

    request(address, &resp(&[&[b"SET", b"k", b"v"]]));
    let info = String::from_utf8(request(address, &resp(&[&[b"INFO"]]))).unwrap();
    let keyspace = String::from_utf8(request(address, &resp(&[&[b"INFO", b"KeySpace"]]))).unwrap();
    let unknown = request(address, &resp(&[&[b"INFO", b"nosuchsection"]]));
    stop.request();
    join_within(server, PATIENCE).unwrap();

    assert!(keyspace.contains("# Keyspace\r\n") && keyspace.contains("db0:keys=1"));
    assert!(!keyspace.contains("# Server"), "{keyspace}");
    assert_eq!(unknown, b"$0\r\n\r\n");

    let pattern = |name: &str| -> String {
        let digits = |rest: &str| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit());

        if let Some(rest) = name.strip_prefix("db")
            && digits(rest)
        {
            "db<N>".into()
        } else if name.starts_with("latency_percentiles_usec_") {
            "latency_percentiles_usec_<command>".into()
        } else if let Some(rest) = name
            .strip_prefix("slab_class_")
            .and_then(|r| r.strip_suffix('b'))
            && digits(rest)
        {
            "slab_class_<N>b".into()
        } else {
            name.into()
        }
    };
    let emitted: std::collections::BTreeSet<String> = info
        .lines()
        .skip(1)
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once(':').map(|(name, _)| pattern(name)))
        .collect();
    let doc = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/OBSERVABILITY.md"
    ))
    .unwrap();
    let mut documented = std::collections::BTreeSet::new();
    let mut replica_only = std::collections::BTreeSet::new();

    for line in doc.lines().filter(|line| line.starts_with("| ")) {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();

        if let [_, _, field, meaning, ..] = cells.as_slice()
            && let Some(name) = field.strip_prefix('`').and_then(|f| f.strip_suffix('`'))
        {
            documented.insert(name.to_owned());
            if meaning.starts_with("Replica only") || name.contains('<') {
                replica_only.insert(name.to_owned());
            }
        }
    }

    let undocumented: Vec<_> = emitted.difference(&documented).collect();
    let missing: Vec<_> = documented
        .iter()
        .filter(|name| !emitted.contains(*name) && !replica_only.contains(*name))
        .collect();

    assert!(
        undocumented.is_empty(),
        "INFO fields with no row: {undocumented:?}"
    );
    assert!(missing.is_empty(), "rows INFO does not write: {missing:?}");
}

/// `HMSET` writes like multi-field `HSET` and replies `OK`, as Redis does (CodeIgniter's
/// and other PHP cache handlers send it through phpredis `hMSet`).
#[test]
fn hmset_writes_like_hset_and_replies_ok() {
    let (address, stop, server) = serve_with(|_| {});

    assert_eq!(
        request(
            address,
            &resp(&[
                &[b"HMSET", b"h", b"a", b"1", b"b", b"2"],
                &[b"HMSET", b"h", b"a", b"3"],
                &[b"HGET", b"h", b"a"],
                &[b"HLEN", b"h"],
                &[b"HMSET", b"h", b"a"],
                &[b"SET", b"s", b"v"],
                &[b"HMSET", b"s", b"a", b"1"],
            ])
        ),
        b"+OK\r\n+OK\r\n$1\r\n3\r\n:2\r\n-ERR wrong number of arguments for 'hmset' command\r\n+OK\r\n\
          -WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}
