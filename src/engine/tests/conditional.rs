use super::strings::words;
use super::*;

fn run(shard: &mut KvShard, sink: &mut DatabaseLog, parts: &[&str]) -> String {
    let frame = words(parts);
    let (command, _) = parse_command(&frame).unwrap().unwrap();

    match in_db(shard, sink, 0, command.into_owned()) {
        Response::Integer(value) => format!("int:{value}"),
        Response::Simple(text) => String::from_utf8_lossy(text).into_owned(),
        Response::Bulk(Some(bytes)) => format!("bulk:{}", String::from_utf8_lossy(&bytes)),
        Response::Bulk(None) => "nil".to_owned(),
        Response::Error(message) => String::from_utf8_lossy(message).into_owned(),
        other => {
            let owned = execute_owned(other);
            format!("{owned:?}")
        }
    }
}

fn list(shard: &mut KvShard, sink: &mut DatabaseLog, key: &str) -> Vec<String> {
    let reply = in_db(
        shard,
        sink,
        0,
        parse_command(&words(&["LRANGE", key, "0", "-1"]))
            .unwrap()
            .unwrap()
            .0
            .into_owned(),
    );

    match execute_owned(reply) {
        Response::Array(items) => items
            .into_iter()
            .map(|item| String::from_utf8_lossy(&item.unwrap()).into_owned())
            .collect(),
        other => panic!("unexpected reply {other:?}"),
    }
}

#[test]
fn set_ifgt_and_iflt_keep_the_larger_or_smaller_number() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "10", "IFGT"]),
        "OK",
        "a missing key is written"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "9", "IFGT"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "10", "IFGT"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "11", "ifgt"]),
        "OK"
    );
    assert_eq!(run(&mut shard, &mut sink, &["GET", "w"]), "bulk:11");

    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "12", "IFLT"]),
        "nil"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "w", "11", "IFLT"]),
        "nil"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["SET", "w", "-3", "IFLT", "EX", "100"]
        ),
        "OK"
    );
    assert_eq!(run(&mut shard, &mut sink, &["GET", "w"]), "bulk:-3");
    assert_eq!(run(&mut shard, &mut sink, &["TTL", "w"]), "int:100");

    // Counters (integer encoding) compare too, and the bounds hold.
    run(&mut shard, &mut sink, &["SET", "n", "41"]);
    run(&mut shard, &mut sink, &["INCR", "n"]);
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "n", "42", "IFGT"]),
        "nil"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["SET", "n", "9223372036854775807", "IFGT"]
        ),
        "OK"
    );
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["SET", "n", "-9223372036854775808", "IFLT"]
        ),
        "OK"
    );

    // A stored string that is not an integer writes nothing.
    run(&mut shard, &mut sink, &["SET", "s", "abc"]);
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "s", "1", "IFGT"]),
        "ERR value is not an integer or out of range"
    );
    assert_eq!(run(&mut shard, &mut sink, &["GET", "s"]), "bulk:abc");

    run(&mut shard, &mut sink, &["RPUSH", "l", "a"]);
    assert!(run(&mut shard, &mut sink, &["SET", "l", "1", "IFGT"]).starts_with("WRONGTYPE"));

    for bad in [
        &["SET", "k", "v", "IFGT"][..],
        &["SET", "k", "1.5", "IFLT"][..],
        &["SET", "k", "9223372036854775808", "IFGT"][..],
        &["SET", "k", "1", "IFGT", "NX"][..],
        &["SET", "k", "1", "IFLT", "XX"][..],
        &["SET", "k", "1", "IFGT", "GET"][..],
    ] {
        assert!(parse_command(&words(bad)).is_err(), "{bad:?}");
    }

    let mut recovered = replayed(&sink);
    assert_eq!(
        run(&mut recovered, &mut DatabaseLog::default(), &["GET", "w"]),
        "bulk:-3"
    );
}

#[test]
fn set_ifeq_writes_only_when_the_string_matches() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "k", "new", "IFEQ", "old"]),
        "nil"
    );
    assert_eq!(shard.value_kind(b"k"), None, "a missing key is not created");

    run(&mut shard, &mut sink, &["SET", "k", "old"]);
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "k", "new", "IFEQ", "nope"]),
        "nil"
    );
    assert_eq!(run(&mut shard, &mut sink, &["GET", "k"]), "bulk:old");
    assert_eq!(
        run(&mut shard, &mut sink, &["SET", "k", "new", "ifeq", "old"]),
        "OK"
    );
    assert_eq!(run(&mut shard, &mut sink, &["GET", "k"]), "bulk:new");

    // Counters compare as decimal text; the write keeps the expiry options.
    run(&mut shard, &mut sink, &["SET", "n", "41"]);
    run(&mut shard, &mut sink, &["INCR", "n"]);
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["SET", "n", "x", "IFEQ", "42", "EX", "100"]
        ),
        "OK"
    );
    assert_eq!(run(&mut shard, &mut sink, &["TTL", "n"]), "int:100");

    run(&mut shard, &mut sink, &["RPUSH", "l", "a"]);
    assert!(run(&mut shard, &mut sink, &["SET", "l", "v", "IFEQ", "a"]).starts_with("WRONGTYPE"));

    for bad in [
        &["SET", "k", "v", "IFEQ"][..],
        &["SET", "k", "v", "IFEQ", "x", "NX"][..],
        &["SET", "k", "v", "IFEQ", "x", "GET"][..],
    ] {
        assert!(parse_command(&words(bad)).is_err(), "{bad:?}");
    }

    let mut recovered = replayed(&sink);
    assert_eq!(
        run(&mut recovered, &mut DatabaseLog::default(), &["GET", "k"]),
        "bulk:new"
    );
}

#[test]
fn delex_deletes_only_a_matching_string() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["DELEX", "k", "IFEQ", "t"]),
        "int:0"
    );
    run(&mut shard, &mut sink, &["SET", "k", "token"]);
    assert_eq!(
        run(&mut shard, &mut sink, &["DELEX", "k", "IFEQ", "other"]),
        "int:0"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["DELEX", "k", "IFEQ", "token"]),
        "int:1"
    );
    assert_eq!(shard.value_kind(b"k"), None);

    run(&mut shard, &mut sink, &["RPUSH", "l", "a"]);
    assert!(run(&mut shard, &mut sink, &["DELEX", "l", "IFEQ", "a"]).starts_with("WRONGTYPE"));
    assert!(parse_command(&words(&["DELEX", "k", "t"])).is_err());

    let recovered = replayed(&sink);
    assert_eq!(recovered.value_kind(b"k"), None);
}

#[test]
fn capped_push_keeps_the_newest_items_and_replays() {
    let mut shard = KvShard::new();
    let mut sink = DatabaseLog::default();

    assert_eq!(
        run(&mut shard, &mut sink, &["RPUSHCAP", "chat", "3", "a", "b"]),
        "int:2"
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["RPUSHCAP", "chat", "3", "c", "d"]),
        "int:3"
    );
    assert_eq!(list(&mut shard, &mut sink, "chat"), ["b", "c", "d"]);

    // LPUSHCAP drops from the tail; LPUSH puts the last argument first.
    assert_eq!(
        run(&mut shard, &mut sink, &["LPUSHCAP", "chat", "3", "x"]),
        "int:3"
    );
    assert_eq!(list(&mut shard, &mut sink, "chat"), ["x", "b", "c"]);

    // More values than the cap: only the newest `cap` stay.
    assert_eq!(
        run(
            &mut shard,
            &mut sink,
            &["RPUSHCAP", "big", "2", "1", "2", "3", "4"]
        ),
        "int:2"
    );
    assert_eq!(list(&mut shard, &mut sink, "big"), ["3", "4"]);

    // A list already over the cap shrinks with one trim record.
    run(
        &mut shard,
        &mut sink,
        &["RPUSH", "long", "1", "2", "3", "4", "5", "6"],
    );
    let before = sink.records.len();
    assert_eq!(
        run(&mut shard, &mut sink, &["RPUSHCAP", "long", "2", "7"]),
        "int:2"
    );
    assert_eq!(list(&mut shard, &mut sink, "long"), ["6", "7"]);
    assert_eq!(sink.records.len() - before, 2, "one push and one trim");
    run(
        &mut shard,
        &mut sink,
        &["LPUSH", "long2", "1", "2", "3", "4", "5"],
    );
    assert_eq!(
        run(&mut shard, &mut sink, &["LPUSHCAP", "long2", "2", "9"]),
        "int:2"
    );
    assert_eq!(list(&mut shard, &mut sink, "long2"), ["9", "5"]);

    run(&mut shard, &mut sink, &["SET", "s", "v"]);
    assert!(run(&mut shard, &mut sink, &["RPUSHCAP", "s", "3", "a"]).starts_with("WRONGTYPE"));
    for bad in [
        &["RPUSHCAP", "k", "0", "a"][..],
        &["RPUSHCAP", "k", "x", "a"][..],
        &["RPUSHCAP", "k", "3"][..],
    ] {
        assert!(parse_command(&words(bad)).is_err(), "{bad:?}");
    }

    let mut recovered = replayed(&sink);
    let mut scratch = DatabaseLog::default();
    for key in ["chat", "big", "long", "long2"] {
        assert_eq!(
            list(&mut recovered, &mut scratch, key),
            list(&mut shard, &mut sink, key),
            "{key}"
        );
    }
}
