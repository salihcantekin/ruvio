//! A value and its deadline must reach the log as one record: a log torn between two records
//! would otherwise bring back a value with no TTL (`SET lock tok NX PX 30000` as a permanent lock).
use super::*;

fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn run(shard: &mut KvShard, log: &mut DatabaseLog, parts: &[&[u8]]) {
    let bytes = frame(parts);
    let (command, _) = parse_command(&bytes).unwrap().unwrap();
    let response = in_db(shard, log, 0, command).into_owned();

    assert!(!matches!(response, Response::Error(_)), "{response:?}");
}

/// Replays the log cut after every record from `from` on and checks that `key`, whenever it
/// exists, has a deadline.
fn assert_no_torn_tail_drops_the_ttl(log: &DatabaseLog, from: usize, key: &[u8]) {
    for end in from..=log.records.len() {
        let prefix = DatabaseLog {
            database: 0,
            records: log.records[..end].to_vec(),
        };
        let shard = replayed(&prefix);

        assert!(
            !shard.contains_key(key) || shard.expires_at(key).is_some(),
            "log cut after record {end} of {} left {key:?} without its TTL: {:?}",
            log.records.len(),
            &log.records[from..],
        );
    }
}

fn check(setup: &[&[&[u8]]], command: &[&[u8]], key: &[u8]) {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    for parts in setup {
        run(&mut shard, &mut log, parts);
    }
    let from = log.records.len();

    run(&mut shard, &mut log, command);
    assert!(log.records.len() > from, "{command:?} logged nothing");
    assert_no_torn_tail_drops_the_ttl(&log, from, key);
}

#[test]
fn set_with_expiry_logs_value_and_ttl_in_one_record() {
    check(
        &[],
        &[b"SET", b"lock", b"tok", b"NX", b"PX", b"30000"],
        b"lock",
    );
    check(&[], &[b"SET", b"lock", b"tok", b"EX", b"30"], b"lock");
}

#[test]
fn hot_set_with_expiry_logs_value_and_ttl_in_one_record() {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();
    let mut output = Vec::new();

    execute_hot_into_with_sink(
        &mut shard,
        crate::protocol::HotCommand::Set(b"lock", b"tok", Some(30_000)),
        &mut output,
        &mut log,
        b"$-1\r\n",
    )
    .unwrap();
    assert_eq!(output, b"+OK\r\n");
    assert_no_torn_tail_drops_the_ttl(&log, 0, b"lock");
}

#[test]
fn rewrites_that_keep_a_ttl_log_it_with_the_value() {
    let with_ttl: &[&[u8]] = &[b"SET", b"k", b"1", b"PX", b"60000"];

    check(&[with_ttl], &[b"INCRBYFLOAT", b"k", b"1.5"], b"k");
    check(&[with_ttl], &[b"SETBIT", b"k", b"40", b"1"], b"k");
    check(
        &[&[b"PFADD", b"h", b"a"], &[b"PEXPIRE", b"h", b"60000"]],
        &[b"PFADD", b"h", b"b"],
        b"h",
    );
}

#[test]
fn limit_and_fillset_log_their_window_with_the_value() {
    check(&[], &[b"LIMIT", b"rate", b"5", b"60000"], b"rate");
    check(
        &[&[b"FILLGET", b"f", b"5000"]],
        &[b"FILLSET", b"f", b"v", b"60000", b"1"],
        b"f",
    );
}

#[test]
fn commands_logging_their_own_transaction_still_run_inside_multi() {
    let directory = tempfile::tempdir().unwrap();
    let config = crate::PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.path().to_path_buf(),
        ..crate::PersistenceConfig::default()
    };
    let limits = crate::protocol::ProtocolLimits::default();
    let (mut wal, _, _) = crate::persistence::WalSink::open(config.clone(), 0, limits).unwrap();
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    run(
        &mut shard,
        &mut log,
        &[b"SET", b"{t}src", b"v", b"PX", b"60000"],
    );
    let commands = vec![
        Command::Limit {
            key: owned(b"{t}rate"),
            max: 5,
            window_ms: 60_000,
            mode: crate::protocol::LimitMode::Fixed,
        },
        Command::Copy {
            source: owned(b"{t}src"),
            destination: owned(b"{t}dst"),
            replace: false,
        },
    ];
    let reply = execute_transaction(&mut shard, &mut wal, &mut (), 1, commands, 0).unwrap();

    assert!(
        matches!(reply, Response::Exec(ref replies) if replies.len() == 2),
        "{reply:?}"
    );
    drop(wal);

    let (_, recovered, _) = crate::persistence::WalSink::open(config, 0, limits).unwrap();
    let mut replay = KvShard::new();

    for (database, mutation) in &recovered {
        crate::persistence::apply_mutation(&mut replay, *database, mutation).unwrap();
    }
    assert!(replay.expires_at(b"{t}rate").is_some());
}

#[test]
fn ttl_on_create_logs_the_write_and_its_deadline_in_one_record() {
    for command in [
        &[b"INCR".as_slice(), b"k", b"EX", b"60"][..],
        &[b"INCRBY", b"k", b"5", b"PX", b"60000", b"MAX", b"9"],
        &[b"DECRBY", b"k", b"5", b"EX", b"60"],
        &[b"INCRBYFLOAT", b"k", b"1.5", b"EX", b"60"],
        &[b"HINCRBY", b"k", b"f", b"2", b"EX", b"60"],
        &[b"ZADD", b"k", b"EX", b"60", b"1", b"a", b"2", b"b"],
        &[b"ZINCRBY", b"k", b"2", b"a", b"PX", b"60000"],
        &[b"GEOADD", b"k", b"EX", b"60", b"13.36", b"38.11", b"p"],
        &[b"XADD", b"k", b"EX", b"60", b"*", b"f", b"v"],
        &[b"SETBIT", b"k", b"7", b"1", b"EX", b"60"],
        &[b"JSON.SET", b"k", b"$", b"{\"a\":1}", b"NX", b"EX", b"60"],
        &[b"BF.INSERT", b"k", b"EX", b"60", b"ITEMS", b"a", b"b"],
    ] {
        check(&[], command, b"k");
    }
}

#[test]
fn ttl_on_create_leaves_an_existing_key_alone() {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    run(&mut shard, &mut log, &[b"SET", b"k", b"1"]);
    run(&mut shard, &mut log, &[b"INCR", b"k", b"EX", b"60"]);
    assert_eq!(shard.expires_at(b"k"), None);
}
