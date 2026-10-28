use super::*;

fn run<C: Clock>(shard: &mut KvShard<C>, parts: &[&[u8]]) -> Response<'static> {
    execute_owned(execute(shard, command_of(parts)))
}

#[test]
fn ttl_and_stream_id_changes_dirty_watches() {
    let mut shard = KvShard::new();

    run(&mut shard, &[b"SET", b"k", b"v"]);
    run(&mut shard, &[b"XADD", b"s", b"1-1", b"f", b"v"]);
    shard.watch(7, 0, &[b"k".as_slice(), b"s".as_slice()]);

    for command in [
        &[b"EXPIRE".as_slice(), b"k", b"100"][..],
        &[b"PERSIST", b"k"],
        &[b"GETEX", b"k", b"100000"],
        &[b"XSETID", b"s", b"5-0"],
    ] {
        run(&mut shard, command);
        assert!(
            shard.take_watch_change(7),
            "{} must dirty a watch",
            String::from_utf8_lossy(command[0])
        );
    }

    // A refused change leaves the watch clean.
    run(&mut shard, &[b"XSETID", b"s", b"1-0"]);
    assert!(!shard.take_watch_change(7));
}

#[test]
fn a_group_read_that_delivers_nothing_leaves_watches_clean() {
    let mut shard = KvShard::new();

    run(&mut shard, &[b"XADD", b"s", b"1-1", b"f", b"v"]);
    run(&mut shard, &[b"XGROUP", b"CREATE", b"s", b"g", b"$"]);
    shard.watch(7, 0, &[b"s".as_slice()]);

    let read: &[&[u8]] = &[b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"];

    run(&mut shard, read);
    assert!(!shard.take_watch_change(7));

    run(&mut shard, &[b"XADD", b"s", b"2-1", b"f", b"v"]);
    assert!(shard.take_watch_change(7));
    run(&mut shard, read);
    assert!(shard.take_watch_change(7), "a delivered entry is a change");
}

#[test]
fn geosearchstore_refused_for_memory_keeps_the_destination() {
    let mut shard = KvShard::new();
    let mut add = vec![b"GEOADD".to_vec(), b"src".to_vec()];

    for index in 0..64 {
        add.push(b"13.36".to_vec());
        add.push(b"38.11".to_vec());
        add.push(format!("member-with-a-long-name-{index:04}").into_bytes());
    }
    let parts: Vec<&[u8]> = add.iter().map(Vec::as_slice).collect();
    run(&mut shard, &parts);
    run(&mut shard, &[b"ZADD", b"dst", b"1", b"a"]);
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 16));

    let response = run(
        &mut shard,
        &[
            b"GEOSEARCHSTORE",
            b"dst",
            b"src",
            b"FROMLONLAT",
            b"13.36",
            b"38.11",
            b"BYRADIUS",
            b"10",
            b"km",
        ],
    );

    assert_eq!(
        response,
        Response::Error(b"OOM command not allowed when used memory > 'maxmemory'.")
    );
    assert_eq!(
        run(&mut shard, &[b"ZSCORE", b"dst", b"a"]),
        Response::Double(1.0)
    );
}

#[test]
fn sorted_set_algebra_without_destination_is_a_read() {
    for parts in [
        &[b"ZUNION".as_slice(), b"2", b"a", b"b"][..],
        &[b"ZINTER", b"2", b"a", b"b"],
        &[b"ZDIFF", b"2", b"a", b"b"],
    ] {
        assert!(!crate::protocol::command_mutates_store(&command_of(parts)));
    }
    assert!(crate::protocol::command_mutates_store(&command_of(&[
        b"ZUNIONSTORE",
        b"d",
        b"2",
        b"a",
        b"b"
    ])));
}

#[test]
fn a_read_keeps_a_collection_under_lru_eviction() {
    for read in [
        &[b"TOUCH".as_slice(), b"hot"][..],
        &[b"HGET", b"hot", b"f"],
        &[b"HGETALL", b"hot"],
        &[b"HEXISTS", b"hot", b"f"],
        &[b"TYPE", b"hot"],
    ] {
        let clock = crate::time::TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock.clone());

        shard.set_eviction_policy(crate::EvictionPolicy::AllKeysLru);
        run(&mut shard, &[b"HSET", b"hot", b"f", b"v"]);
        run(&mut shard, &[b"SET", b"cold", &[7; 128]]);
        clock.set(10_000);
        run(&mut shard, read);
        shard.set_memory_limit(Some(shard.memory_usage().charged_bytes()));
        run(&mut shard, &[b"SET", b"new", b"3"]);

        assert_eq!(
            shard.get(b"cold"),
            None,
            "{}",
            String::from_utf8_lossy(read[0])
        );
        assert!(
            shard.contains_key(b"hot"),
            "{}",
            String::from_utf8_lossy(read[0])
        );
    }
}

/// One key of every type, so each write below finds what it edits.
const SETUP: &[&[&str]] = &[
    &["SET", "s", "10"],
    &["SET", "t", "abc"],
    &["RPUSH", "l", "a", "b", "c"],
    &["HSET", "h", "f", "1", "g", "2"],
    &["ZADD", "z", "1", "a", "2", "b", "3", "c"],
    &["SADD", "e", "a", "b", "c"],
    &["XADD", "x", "1-1", "f", "v"],
    &["XADD", "x", "2-1", "f", "w"],
    &["XGROUP", "CREATE", "x", "g", "0"],
    &[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "COUNT",
        "1",
        "STREAMS",
        "x",
        ">",
    ],
    &[
        "GEOADD", "geo", "13.36", "38.11", "m1", "15.08", "37.50", "m2",
    ],
    &["BF.ADD", "b", "a"],
    &["PFADD", "p", "a"],
    &[
        "JSON.SET",
        "j",
        "$",
        r#"{"a":[1,2,3],"n":1,"s":"x","t":true}"#,
    ],
    &["SET", "v", "1", "PX", "60000"],
];

/// Keys a write may change; `d` is the usual destination.
const KEYS: &[&str] = &[
    "s", "t", "l", "h", "z", "e", "x", "geo", "b", "p", "j", "v", "d", "k",
];

/// Each write command, after optional extra steps; the last frame is the one under test.
/// `$LAST` is replaced with the previous step's bulk reply.
const CASES: &[(&str, &[&[&str]])] = &[
    ("set", &[&["SET", "s", "x"]]),
    ("incr", &[&["INCR", "s"]]),
    ("decr", &[&["DECR", "s"]]),
    ("incrby", &[&["INCRBY", "s", "5"]]),
    ("decrby", &[&["DECRBY", "s", "5"]]),
    ("getset", &[&["GETSET", "s", "x"]]),
    ("append", &[&["APPEND", "t", "d"]]),
    ("setex", &[&["SETEX", "s", "100", "x"]]),
    ("psetex", &[&["PSETEX", "s", "100000", "x"]]),
    ("setnx", &[&["SETNX", "d", "x"]]),
    ("getdel", &[&["GETDEL", "s"]]),
    ("msetnx", &[&["MSETNX", "d", "x", "k", "y"]]),
    ("incrbyfloat", &[&["INCRBYFLOAT", "s", "1.5"]]),
    ("setrange", &[&["SETRANGE", "t", "1", "zz"]]),
    ("zunionstore", &[&["ZUNIONSTORE", "d", "1", "z"]]),
    ("zinterstore", &[&["ZINTERSTORE", "d", "1", "z"]]),
    ("zdiffstore", &[&["ZDIFFSTORE", "d", "1", "z"]]),
    ("hincrbyfloat", &[&["HINCRBYFLOAT", "h", "f", "1.5"]]),
    ("linsert", &[&["LINSERT", "l", "BEFORE", "b", "x"]]),
    ("lset", &[&["LSET", "l", "0", "x"]]),
    ("lrem", &[&["LREM", "l", "0", "a"]]),
    ("lmove", &[&["LMOVE", "l", "d", "LEFT", "RIGHT"]]),
    ("blmove", &[&["BLMOVE", "l", "d", "LEFT", "RIGHT", "0"]]),
    ("rpoplpush", &[&["RPOPLPUSH", "l", "l"]]),
    ("lpushx", &[&["LPUSHX", "l", "x"]]),
    ("rpushx", &[&["RPUSHX", "l", "x"]]),
    ("lmpop", &[&["LMPOP", "1", "l", "LEFT"]]),
    ("blmpop", &[&["BLMPOP", "0", "1", "l", "LEFT"]]),
    ("setbit", &[&["SETBIT", "t", "1", "0"]]),
    ("bitfield", &[&["BITFIELD", "t", "SET", "u8", "0", "255"]]),
    ("bitop", &[&["BITOP", "NOT", "d", "t"]]),
    ("pfadd", &[&["PFADD", "p", "b", "c"]]),
    ("pfmerge", &[&["PFMERGE", "d", "p"]]),
    (
        "fillset",
        &[
            &["FILLGET", "k", "5000"],
            &["FILLSET", "k", "x", "60000", "1"],
        ],
    ),
    ("renamenx", &[&["RENAMENX", "s", "d"]]),
    ("copy", &[&["COPY", "l", "d"]]),
    (
        "restore",
        &[&["DUMP", "l"], &["RESTORE", "d", "0", "$LAST"]],
    ),
    ("json.del", &[&["JSON.DEL", "j", "$.n"]]),
    ("json.forget", &[&["JSON.FORGET", "j", "$.n"]]),
    ("json.set", &[&["JSON.SET", "j", "$.n", "2"]]),
    ("json.merge", &[&["JSON.MERGE", "j", "$", r#"{"n":5}"#]]),
    ("json.numincrby", &[&["JSON.NUMINCRBY", "j", "$.n", "2"]]),
    ("json.toggle", &[&["JSON.TOGGLE", "j", "$.t"]]),
    ("json.arrappend", &[&["JSON.ARRAPPEND", "j", "$.a", "4"]]),
    ("json.clear", &[&["JSON.CLEAR", "j", "$.a"]]),
    (
        "json.strappend",
        &[&["JSON.STRAPPEND", "j", "$.s", r#""y""#]],
    ),
    ("json.nummultby", &[&["JSON.NUMMULTBY", "j", "$.n", "3"]]),
    (
        "json.arrinsert",
        &[&["JSON.ARRINSERT", "j", "$.a", "0", "9"]],
    ),
    ("json.arrpop", &[&["JSON.ARRPOP", "j", "$.a"]]),
    ("json.arrtrim", &[&["JSON.ARRTRIM", "j", "$.a", "0", "0"]]),
    ("jmerge", &[&["JMERGE", "j", "$", r#"{"n":6}"#]]),
    ("lease", &[&["LEASE", "k", "5000"]]),
    ("semaphore", &[&["SEMAPHORE", "k", "2", "5000"]]),
    (
        "limit",
        &[&["LIMIT", "k", "5", "60000"], &["LIMIT", "k", "5", "60000"]],
    ),
    (
        "limit",
        &[
            &["LIMIT", "k", "5", "60000", "SLIDING"],
            &["LIMIT", "k", "5", "60000", "SLIDING", "WITHRETRY"],
        ],
    ),
    (
        "limit",
        &[
            &["LIMIT", "k", "5", "60000", "TOKENS", "COST", "2"],
            &["LIMIT", "k", "5", "60000", "TOKENS", "WITHRETRY"],
        ],
    ),
    ("idem", &[&["IDEM", "k", "BEGIN", "fp", "owner", "5000"]]),
    ("once", &[&["ONCE", "k", "5000"]]),
    ("take", &[&["TAKE", "s", "3"]]),
    ("transfer", &[&["TRANSFER", "s", "d", "4"]]),
    (
        "transfer",
        &[&["SET", "d", "7"], &["TRANSFER", "s", "d", "4"]],
    ),
    ("htransfer", &[&["HTRANSFER", "h", "f", "g", "1"]]),
    (
        "htransfer",
        &[
            &["HEXPIRE", "h", "100", "FIELDS", "1", "f"],
            &["HTRANSFER", "h", "f", "g", "1"],
        ],
    ),
    ("getex", &[&["GETEX", "s", "100000"]]),
    (
        "release",
        &[&["LEASE", "k", "5000"], &["RELEASE", "k", "$LAST"]],
    ),
    (
        "renew",
        &[
            &["SEMAPHORE", "sem", "2", "5000"],
            &["RENEW", "sem", "$LAST", "9000"],
            &["LEASE", "k", "5000"],
            &["RENEW", "k", "$LAST", "9000"],
        ],
    ),
    ("setv", &[&["SETV", "k", "x"]]),
    ("mset", &[&["MSET", "s", "x", "d", "y"]]),
    ("unlink", &[&["UNLINK", "s", "l"]]),
    ("rename", &[&["RENAME", "s", "d"]]),
    ("flushdb", &[&["FLUSHDB"]]),
    ("flushall", &[&["FLUSHALL"]]),
    ("swapdb", &[&["SWAPDB", "0", "1"]]),
    ("move", &[&["MOVE", "s", "1"]]),
    ("del", &[&["DEL", "s", "h"]]),
    ("expire", &[&["EXPIRE", "s", "100"]]),
    ("pexpire", &[&["PEXPIRE", "s", "100000"]]),
    ("expireat", &[&["EXPIREAT", "s", "4102444800"]]),
    ("pexpireat", &[&["PEXPIREAT", "s", "4102444800000"]]),
    ("persist", &[&["PERSIST", "v"]]),
    ("lpushcap", &[&["LPUSHCAP", "l", "3", "x"]]),
    ("rpushcap", &[&["RPUSHCAP", "l", "3", "x"]]),
    ("delex", &[&["DELEX", "t", "IFEQ", "abc"]]),
    ("lpush", &[&["LPUSH", "l", "x"]]),
    ("lpop", &[&["LPOP", "l"]]),
    ("rpush", &[&["RPUSH", "l", "x"]]),
    ("rpop", &[&["RPOP", "l"]]),
    ("ltrim", &[&["LTRIM", "l", "0", "0"]]),
    ("blpop", &[&["BLPOP", "l", "0"]]),
    ("brpop", &[&["BRPOP", "l", "0"]]),
    ("sadd", &[&["SADD", "e", "x"]]),
    ("srem", &[&["SREM", "e", "a"]]),
    ("sinterstore", &[&["SINTERSTORE", "d", "e"]]),
    ("sunionstore", &[&["SUNIONSTORE", "d", "e"]]),
    ("sdiffstore", &[&["SDIFFSTORE", "d", "e"]]),
    ("spop", &[&["SPOP", "e"]]),
    ("smove", &[&["SMOVE", "e", "d", "a"]]),
    ("hincrby", &[&["HINCRBY", "h", "f", "2"]]),
    ("hsetnx", &[&["HSETNX", "h", "n", "1"]]),
    ("hset", &[&["HSET", "h", "f", "x"]]),
    ("hmset", &[&["HMSET", "h", "f", "x", "g", "y"]]),
    ("hdel", &[&["HDEL", "h", "f"]]),
    ("hexpire", &[&["HEXPIRE", "h", "100", "FIELDS", "1", "f"]]),
    (
        "hpexpire",
        &[&["HPEXPIRE", "h", "100000", "FIELDS", "1", "f"]],
    ),
    (
        "hexpireat",
        &[&["HEXPIREAT", "h", "4102444800", "FIELDS", "1", "f"]],
    ),
    (
        "hpexpireat",
        &[&["HPEXPIREAT", "h", "4102444800000", "FIELDS", "1", "f"]],
    ),
    (
        "hpersist",
        &[
            &["HEXPIRE", "h", "100", "FIELDS", "1", "f"],
            &["HPERSIST", "h", "FIELDS", "1", "f"],
        ],
    ),
    ("zadd", &[&["ZADD", "z", "5", "x"]]),
    ("zrem", &[&["ZREM", "z", "a"]]),
    ("zincrby", &[&["ZINCRBY", "z", "2", "a"]]),
    ("zpopmin", &[&["ZPOPMIN", "z"]]),
    ("zpopmax", &[&["ZPOPMAX", "z"]]),
    ("zpopbyscore", &[&["ZPOPBYSCORE", "z", "10", "COUNT", "2"]]),
    ("zmovedue", &[&["ZMOVEDUE", "z", "l", "10", "COUNT", "2"]]),
    ("bzpopmin", &[&["BZPOPMIN", "z", "0"]]),
    ("bzpopmax", &[&["BZPOPMAX", "z", "0"]]),
    ("zremrangebyrank", &[&["ZREMRANGEBYRANK", "z", "0", "0"]]),
    ("zremrangebyscore", &[&["ZREMRANGEBYSCORE", "z", "1", "1"]]),
    ("zremrangebylex", &[&["ZREMRANGEBYLEX", "z", "-", "+"]]),
    ("geoadd", &[&["GEOADD", "geo", "1", "2", "m3"]]),
    (
        "geosearchstore",
        &[&[
            "GEOSEARCHSTORE",
            "d",
            "geo",
            "FROMMEMBER",
            "m1",
            "BYRADIUS",
            "500",
            "km",
        ]],
    ),
    (
        "georadius",
        &[&["GEORADIUS", "geo", "15", "37", "500", "km", "STORE", "d"]],
    ),
    (
        "georadiusbymember",
        &[&["GEORADIUSBYMEMBER", "geo", "m1", "500", "km", "STORE", "d"]],
    ),
    ("bf.reserve", &[&["BF.RESERVE", "k", "0.01", "100"]]),
    ("bf.add", &[&["BF.ADD", "b", "x"]]),
    ("bf.madd", &[&["BF.MADD", "b", "x", "y"]]),
    ("bf.insert", &[&["BF.INSERT", "b", "ITEMS", "x"]]),
    ("xadd", &[&["XADD", "x", "*", "f", "v"]]),
    ("xdel", &[&["XDEL", "x", "1-1"]]),
    ("xtrim", &[&["XTRIM", "x", "MAXLEN", "1"]]),
    ("xsetid", &[&["XSETID", "x", "9-0"]]),
    ("xgroup", &[&["XGROUP", "CREATE", "x", "g2", "0"]]),
    ("xgroup", &[&["XGROUP", "SETID", "x", "g", "0"]]),
    ("xgroup", &[&["XGROUP", "DESTROY", "x", "g"]]),
    ("xgroup", &[&["XGROUP", "CREATECONSUMER", "x", "g", "c9"]]),
    ("xgroup", &[&["XGROUP", "DELCONSUMER", "x", "g", "c"]]),
    ("xadd", &[&["XADD", "x", "MAXLEN", "1", "*", "f", "v"]]),
    ("bitfield", &[&["BITFIELD", "t", "INCRBY", "u8", "0", "1"]]),
    ("set", &[&["SET", "s", "x", "GET"]]),
    ("set", &[&["SET", "s", "x", "KEEPTTL"]]),
    ("expire", &[&["EXPIRE", "s", "0"]]),
    (
        "xreadgroup",
        &[&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "x", ">"]],
    ),
    ("xack", &[&["XACK", "x", "g", "1-1"]]),
    ("xclaim", &[&["XCLAIM", "x", "g", "c2", "0", "1-1"]]),
    ("xautoclaim", &[&["XAUTOCLAIM", "x", "g", "c2", "0", "0-0"]]),
];

/// Write rows without a case here, and why.
const NO_CASE: &[(&str, &str)] = &[
    (
        "fillget",
        "takes an in-memory fill marker; no stored key changes",
    ),
    ("json.index", "writes only its index definition key"),
    (
        "eval",
        "needs a connection; its writes are the commands above",
    ),
    (
        "evalsha",
        "needs a connection; its writes are the commands above",
    ),
    (
        "fcall",
        "needs a connection; its writes are the commands above",
    ),
];

fn frame_words(words: &[&[u8]]) -> Command<'static> {
    command_of(words)
}

/// Any change to a key's value or deadline marks every watch on it, whatever the command.
/// A write row with no case fails the test, so a new command cannot skip this.
#[test]
fn every_write_command_dirties_the_watches_on_what_it_changes() {
    type Shard = KvShard<crate::time::TestClock>;

    // The stored entry itself: `DUMP` does not cover every type.
    fn state(
        shard: &mut Shard,
        key: &str,
    ) -> (Option<crate::store::Entry>, Option<crate::UnixMillis>) {
        (
            shard.clone_entry(key.as_bytes()),
            shard.deadline(0, key.as_bytes()),
        )
    }

    let covered: std::collections::BTreeSet<&str> = CASES
        .iter()
        .map(|(name, _)| *name)
        .chain(NO_CASE.iter().map(|(name, _)| *name))
        .collect();
    let uncovered: Vec<&str> = crate::protocol::all_commands()
        .iter()
        .filter(|spec| spec.flags & crate::protocol::flags::WRITE != 0)
        .map(|spec| spec.name)
        .filter(|name| !covered.contains(name))
        .collect();
    let mut failures = Vec::new();

    for (name, steps) in CASES {
        let mut shard: Shard = KvShard::with_clock(crate::time::TestClock::new(1_000_000));

        for words in SETUP {
            let words: Vec<&[u8]> = words.iter().map(|word| word.as_bytes()).collect();
            let reply = execute_owned(execute(&mut shard, frame_words(&words)));
            assert!(
                !matches!(reply, Response::Error(_) | Response::OwnedError(_)),
                "{words:?}"
            );
        }

        let mut last: Vec<u8> = Vec::new();
        let (tested, extra) = steps.split_last().unwrap();
        let run_step = |shard: &mut Shard, words: &[&str], last: &mut Vec<u8>| {
            let owned_words: Vec<Vec<u8>> = words
                .iter()
                .map(|word| {
                    if *word == "$LAST" {
                        last.clone()
                    } else {
                        word.as_bytes().to_vec()
                    }
                })
                .collect();
            let words: Vec<&[u8]> = owned_words.iter().map(Vec::as_slice).collect();
            let reply = execute_owned(execute(shard, frame_words(&words)));

            match &reply {
                Response::Bulk(Some(bytes)) => *last = bytes.to_vec(),
                Response::Integer(value) => *last = value.to_string().into_bytes(),
                _ => {}
            }
            reply
        };

        for words in extra {
            run_step(&mut shard, words, &mut last);
        }

        let before: Vec<_> = KEYS.iter().map(|key| state(&mut shard, key)).collect();

        for (conn, key) in KEYS.iter().enumerate() {
            shard.watch(conn as u64, 0, &[key.as_bytes()]);
        }

        let reply = run_step(&mut shard, tested, &mut last);
        shard.enter(0);

        let changed: Vec<usize> = (0..KEYS.len())
            .filter(|&conn| state(&mut shard, KEYS[conn]) != before[conn])
            .collect();

        if changed.is_empty() {
            failures.push(format!("{name}: changed no key, reply {reply:?}"));
        }
        for conn in changed {
            if !shard.watches_dirty(conn as u64) {
                failures.push(format!("{tested:?}: {:?} changed, watch clean", KEYS[conn]));
            }
        }
    }

    assert!(
        uncovered.is_empty(),
        "write commands without a case: {uncovered:?}"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn a_collection_emptied_by_a_removal_is_announced_deleted() {
    for (setup, removal) in [
        (
            &[b"HSET".as_slice(), b"c", b"f", b"v"][..],
            &[b"HDEL".as_slice(), b"c", b"f"][..],
        ),
        (&[b"RPUSH", b"c", b"a"], &[b"LPOP", b"c"]),
        (&[b"RPUSH", b"c", b"a"], &[b"LTRIM", b"c", b"1", b"0"]),
        (&[b"SADD", b"c", b"a"], &[b"SREM", b"c", b"a"]),
        (&[b"ZADD", b"c", b"1", b"a"], &[b"ZREM", b"c", b"a"]),
        (&[b"ZADD", b"c", b"1", b"a"], &[b"ZPOPMIN", b"c"]),
        (&[b"SADD", b"c", b"a"], &[b"SINTERSTORE", b"c", b"missing"]),
    ] {
        let mut shard = KvShard::new();

        run(&mut shard, setup);
        shard.set_notify_mask(crate::notify::parse_mask("Eg").unwrap());
        shard.set_notify_listeners(1);
        run(&mut shard, removal);

        let notices = shard.take_notices();
        assert!(!shard.contains_key(b"c"));
        assert!(
            notices
                .iter()
                .any(|(_, key, event)| &**key == b"c" && *event == crate::notify::KeyEvent::Del),
            "{}: {notices:?}",
            String::from_utf8_lossy(removal[0])
        );
    }
}

#[test]
fn a_string_written_over_an_indexed_document_leaves_the_index() {
    let mut shard = KvShard::new();

    run(
        &mut shard,
        &[
            b"JSON.INDEX",
            b"CREATE",
            b"city",
            b"PREFIX",
            b"user:",
            b"PATH",
            b"$.city",
        ],
    );
    while shard.json_index_build_step(16) {}
    run(
        &mut shard,
        &[b"JSON.SET", b"user:1", b"$", br#"{"city":"ankara"}"#],
    );
    let entries = |shard: &KvShard| shard.json_index_list()[0].2;
    assert_eq!(entries(&shard), 1);

    run(&mut shard, &[b"SET", b"user:1", b"plain"]);
    assert_eq!(entries(&shard), 0);
}

/// Under `noeviction` with memory over the limit, a write is refused exactly when the table
/// flags it `denyoom`: the flag is the one place that decision is declared.
#[test]
fn oom_refusal_matches_the_denyoom_flag() {
    let mut report = Vec::new();

    for (name, steps) in CASES {
        let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000_000));

        for words in SETUP {
            let words: Vec<&[u8]> = words.iter().map(|word| word.as_bytes()).collect();
            execute(&mut shard, command_of(&words));
        }
        let mut last = Vec::new();
        let (tested, extra) = steps.split_last().unwrap();
        let mut run_step = |shard: &mut KvShard<crate::time::TestClock>, words: &[&str]| {
            let owned_words: Vec<Vec<u8>> = words
                .iter()
                .map(|word| {
                    if *word == "$LAST" {
                        last.clone()
                    } else {
                        word.as_bytes().to_vec()
                    }
                })
                .collect();
            let words: Vec<&[u8]> = owned_words.iter().map(Vec::as_slice).collect();
            let reply = execute_owned(execute(shard, command_of(&words)));

            match &reply {
                Response::Bulk(Some(bytes)) => last = bytes.to_vec(),
                Response::Integer(value) => last = value.to_string().into_bytes(),
                _ => {}
            }
            reply
        };
        for words in extra {
            run_step(&mut shard, words);
        }

        let used = shard.memory_usage().charged_bytes();
        shard.set_memory_limit(Some(used.saturating_sub(1)));
        let refused = run_step(&mut shard, tested) == Response::Error(crate::engine::OOM_ERROR);
        let spec = crate::protocol::command_spec(name.as_bytes()).unwrap();
        // XGROUP is flagged per subcommand in Redis: only CREATE and CREATECONSUMER grow.
        // An EXPIRE to a past time is a delete, which a full store lets through.
        let denyoom = if *name == "expire" && tested[2] == "0" {
            false
        } else if *name == "xgroup" {
            ["CREATE", "CREATECONSUMER"].contains(&tested[1].to_ascii_uppercase().as_str())
        } else {
            spec.flags & crate::protocol::flags::DENYOOM != 0
        };

        if refused != denyoom {
            report.push(format!("{tested:?}: refused={refused} denyoom={denyoom}"));
        }
    }

    assert!(report.is_empty(), "\n{}", report.join("\n"));
}

/// A `MULTI` whose batch the log refuses leaves JSON indexes as they were: a created index
/// is gone again and a dropped one is back, with its definition key, state and postings.
#[test]
fn a_refused_transaction_rolls_back_json_indexes() {
    let mut shard = KvShard::new();
    let indexes = |shard: &KvShard| -> Vec<String> {
        shard
            .json_index_list()
            .into_iter()
            .map(|(definition, ..)| definition.name)
            .collect()
    };

    run(
        &mut shard,
        &[
            b"JSON.INDEX",
            b"CREATE",
            b"kept",
            b"PREFIX",
            b"u:",
            b"PATH",
            b"$.a",
        ],
    );
    run(&mut shard, &[b"JSON.SET", b"u:1", b"$", br#"{"a":"five"}"#]);
    while shard.json_index_build_step(64) {}
    let stored_keys = shard.len();
    let before = shard.json_index_list();

    assert_eq!(before.len(), 1);
    assert_eq!(before[0].1, crate::json::index::IndexState::Ready);
    assert_eq!(before[0].2, 1);

    let refused = execute_transaction(
        &mut shard,
        &mut FailingSink,
        &mut (),
        1,
        vec![
            command_of(&[
                b"JSON.INDEX",
                b"CREATE",
                b"fresh",
                b"PREFIX",
                b"v:",
                b"PATH",
                b"$.b",
            ]),
            command_of(&[b"JSON.INDEX", b"DROP", b"kept"]),
        ],
        0,
    );

    assert!(refused.is_err());
    assert_eq!(indexes(&shard), vec!["kept".to_owned()]);
    assert_eq!(shard.len(), stored_keys);
    // Back as it was, ready and holding its document, not rebuilding from scratch.
    assert_eq!(shard.json_index_list(), before);
}

/// A replica applying a `MULTI` that creates one JSON index and drops another ends with the
/// primary's index registry, not only its definition keys.
#[test]
fn a_replayed_transaction_keeps_the_json_index_registry() {
    let mut primary = KvShard::new();
    let mut log = DatabaseLog::default();

    let Ok(_) = execute_with_waiters(
        &mut primary,
        command_of(&[
            b"JSON.INDEX",
            b"CREATE",
            b"old",
            b"PREFIX",
            b"u:",
            b"PATH",
            b"$.a",
        ]),
        &mut log,
        &mut (),
    );
    let Ok(_) = execute_transaction(
        &mut primary,
        &mut log,
        &mut (),
        1,
        vec![
            command_of(&[
                b"JSON.INDEX",
                b"CREATE",
                b"new",
                b"PREFIX",
                b"v:",
                b"PATH",
                b"$.b",
            ]),
            command_of(&[b"JSON.INDEX", b"DROP", b"old"]),
        ],
        0,
    );

    let mut replica = KvShard::new();

    for (database, record) in &log.records {
        crate::persistence::apply_mutation(&mut replica, *database, record).unwrap();
    }

    let names = |shard: &KvShard| -> Vec<String> {
        shard
            .json_index_list()
            .into_iter()
            .map(|(definition, ..)| definition.name)
            .collect()
    };

    assert_eq!(names(&primary), vec!["new".to_owned()]);
    assert_eq!(names(&replica), names(&primary));
}

/// A small JSON.MERGE into a large compact document leaves it compact on the primary, as the
/// replayed path merge does on a replica; before, only the primary built a tree.
#[test]
fn a_small_json_merge_stores_the_same_form_on_primary_and_replica() {
    let compact = serde_json::to_vec(&serde_json::json!({
        "l": (0..20_000).collect::<Vec<_>>(),
        "n": 1,
    }))
    .unwrap();
    // Worth a tree, but reached by a small edit, so it is held compact.
    let worthy = serde_json::to_vec(&serde_json::json!({
        "s": "x".repeat(40_000),
        "n": 1,
    }))
    .unwrap();
    assert!(crate::json::TreeDocument::from_bytes(&worthy).is_some());
    let setup = || {
        let mut shard = KvShard::new();
        shard.put_json(b"d", compact.clone()).unwrap();
        shard.put_json_edit(b"d", worthy.clone(), true).unwrap();
        assert!(shard.json_tree(b"d").is_none());
        shard
    };
    let mut primary = setup();
    let mut log = DatabaseLog::default();
    let Ok(Response::Simple(b"OK")) = execute_with_waiters(
        &mut primary,
        command_of(&[b"JSON.MERGE", b"d", b"$.n", b"2"]),
        &mut log,
        &mut (),
    ) else {
        panic!("JSON.MERGE failed");
    };

    let mut replica = setup();
    for (database, record) in &log.records {
        crate::persistence::apply_mutation(&mut replica, *database, record).unwrap();
    }

    assert_eq!(
        primary.json_tree(b"d").is_some(),
        replica.json_tree(b"d").is_some()
    );
    assert_eq!(
        primary.memory_usage().value_bytes,
        replica.memory_usage().value_bytes
    );
    assert_eq!(
        primary.json_view(b"d").unwrap(),
        replica.json_view(b"d").unwrap()
    );
}

/// Keys of database `db` that JSON index `name` finds for `value`.
fn found(shard: &mut KvShard, db: u8, name: &str, value: &[u8]) -> Vec<Vec<u8>> {
    let previous = shard.active_database();

    shard.enter(db);
    let keys = shard
        .json_index_find(name, value, None, 10)
        .unwrap_or_default();
    shard.enter(previous);
    keys
}

fn indexed_shard() -> (KvShard, DatabaseLog) {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    for (db, command) in [
        (
            0,
            &[
                &b"JSON.INDEX"[..],
                b"CREATE",
                b"c",
                b"PREFIX",
                b"u:",
                b"PATH",
                b"$.a",
            ][..],
        ),
        (0, &[b"JSON.SET", b"u:0", b"$", br#"{"a":"x"}"#]),
        (1, &[b"JSON.SET", b"u:1", b"$", br#"{"a":"x"}"#]),
    ] {
        in_db(&mut shard, &mut log, db, command_of(command));
    }
    while shard.json_index_build_step(16) {}
    assert_eq!(found(&mut shard, 1, "c", b"x"), [b"u:1".to_vec()]);
    (shard, log)
}

/// FLUSHDB and FLUSHALL inside `MULTI` drop JSON index state as the plain commands and
/// their replay do; before, the registry and postings outlived the flushed documents.
#[test]
fn a_flush_in_multi_drops_json_index_state_like_a_plain_flush() {
    for (db, flush) in [(1, &b"FLUSHDB"[..]), (0, b"FLUSHDB"), (0, b"FLUSHALL")] {
        let (mut primary, mut log) = indexed_shard();

        let Ok(_) = execute_transaction(
            &mut primary,
            &mut log,
            &mut (),
            1,
            vec![command_of(&[flush])],
            db,
        );
        let replica = replayed(&log);
        let names = |shard: &KvShard| shard.json_index_list().len();

        assert_eq!(names(&primary), names(&replica), "{db} {flush:?}");
        assert!(
            found(&mut primary, 1, "c", b"x").len() <= usize::from(db == 0 && flush == b"FLUSHDB")
        );
    }
}

/// A refused `MULTI` that flushed puts the JSON index registry and postings back.
#[test]
fn a_refused_flush_in_multi_restores_json_index_state() {
    for (db, flush) in [(1, &b"FLUSHDB"[..]), (0, b"FLUSHDB"), (0, b"FLUSHALL")] {
        let (mut shard, _) = indexed_shard();

        let refused = execute_transaction(
            &mut shard,
            &mut FailingSink,
            &mut (),
            1,
            vec![command_of(&[flush])],
            db,
        );

        assert!(refused.is_err());
        assert_eq!(shard.json_index_list().len(), 1, "{db} {flush:?}");
        assert_eq!(found(&mut shard, 0, "c", b"x"), [b"u:0".to_vec()]);
        assert_eq!(found(&mut shard, 1, "c", b"x"), [b"u:1".to_vec()]);
    }
}

/// A write that needs two records to replay (a kept field TTL, an XADD DELAY hold) logs
/// them as one, so a torn WAL tail or a replica never applies only the first.
#[test]
fn a_command_with_two_effects_logs_one_record() {
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();

    for command in [
        &[&b"HSET"[..], b"h", b"n", b"1", b"x", b"1.5"][..],
        &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"2", b"n", b"x"],
    ] {
        in_db(&mut shard, &mut log, 0, command_of(command));
    }

    for command in [
        &[&b"HINCRBY"[..], b"h", b"n", b"2"][..],
        &[b"HINCRBYFLOAT", b"h", b"x", b"1"],
        &[b"XADD", b"s", b"DELAY", b"5000", b"*", b"f", b"v"],
    ] {
        let before = log.records.len();

        in_db(&mut shard, &mut log, 0, command_of(command));
        assert_eq!(log.records.len(), before + 1, "{command:?}");
    }

    let mut replica = replayed(&log);

    assert_eq!(
        replica.field_deadline(b"h", b"n"),
        shard.field_deadline(b"h", b"n")
    );
    assert_eq!(
        replica.field_deadline(b"h", b"x"),
        shard.field_deadline(b"h", b"x")
    );
    assert!(shard.field_deadline(b"h", b"n").unwrap().is_some());
    assert_eq!(
        execute_owned(execute(&mut replica, command_of(&[b"XLEN", b"s"]))),
        execute_owned(execute(&mut shard, command_of(&[b"XLEN", b"s"])))
    );
}

/// Every write command, replayed from its records into a fresh shard (recovery and a
/// replica take this path), leaves the same keys, values, deadlines and charged memory as
/// on the primary. A write row with no case fails the watch test above, so this one walks
/// every write command too.
#[test]
fn every_write_command_replays_to_the_primary_state() {
    fn words_of<'w>(words: &[&str], last: &'w [u8], owned: &'w mut Vec<Vec<u8>>) -> Vec<&'w [u8]> {
        *owned = words
            .iter()
            .map(|word| {
                if *word == "$LAST" {
                    last.to_vec()
                } else {
                    word.as_bytes().to_vec()
                }
            })
            .collect();
        owned.iter().map(Vec::as_slice).collect()
    }

    fn state(
        shard: &mut KvShard,
    ) -> Vec<(
        u8,
        &'static str,
        Option<crate::store::Entry>,
        Option<crate::UnixMillis>,
    )> {
        let mut found = Vec::new();

        for database in 0..16u8 {
            shard.enter(database);
            for key in KEYS {
                let entry = shard.clone_entry(key.as_bytes());
                let deadline = shard.deadline(database, key.as_bytes());

                if entry.is_some() || deadline.is_some() {
                    found.push((database, *key, entry, deadline));
                }
            }
        }
        shard.enter(0);
        found
    }

    fn charged(shard: &KvShard) -> (usize, usize, usize, usize) {
        let usage = shard.memory_usage();

        (
            usage.entries,
            usage.key_bytes,
            usage.value_bytes,
            usage.collection_bytes,
        )
    }

    let mut failures = Vec::new();

    for (name, steps) in CASES {
        let mut primary = KvShard::new();
        let mut log = DatabaseLog::default();
        let mut last: Vec<u8> = Vec::new();

        for words in SETUP.iter().chain(steps.iter()) {
            let mut owned = Vec::new();
            let words = words_of(words, &last, &mut owned);
            let Ok(reply) =
                execute_in_database(&mut primary, command_of(&words), &mut log, &mut (), 0);

            match reply.into_owned() {
                Response::Bulk(Some(bytes)) => last = bytes.to_vec(),
                Response::Integer(value) => last = value.to_string().into_bytes(),
                _ => {}
            }
        }

        let mut replica = KvShard::new();

        for (database, record) in &log.records {
            if let Err(error) = crate::persistence::apply_mutation(&mut replica, *database, record)
            {
                failures.push(format!("{name}: replay refused {record:?}: {error}"));
            }
        }

        let (on_primary, on_replica) = (state(&mut primary), state(&mut replica));

        if on_primary != on_replica {
            let differ: Vec<_> = on_primary
                .iter()
                .zip(&on_replica)
                .filter(|(a, b)| a != b)
                .map(|(a, b)| format!("{a:?}\n    replica {b:?}"))
                .collect();
            failures.push(format!(
                "{name}: keys differ ({} vs {}): {}",
                on_primary.len(),
                on_replica.len(),
                differ.first().cloned().unwrap_or_default()
            ));
        } else if charged(&primary) != charged(&replica) {
            failures.push(format!(
                "{name}: charged memory {:?} vs replica {:?}",
                charged(&primary),
                charged(&replica)
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
