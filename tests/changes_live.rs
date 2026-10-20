//! `CHANGES SUBSCRIBE` on a real server: event order and shape, resume after a crash without loss or
//! repeats, filters, and the refusals.

mod common;

use common::{Client, Server};

const ENV: &[(&str, &str)] = &[
    ("RUVIO_DURABILITY", "always"),
    ("RUVIO_CDC_RETAIN_BYTES", "67108864"),
    ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
];

/// Reads `want` change events (skipping heartbeats), returning each as `[change,cursor,db,type,key,value]`.
fn changes(subscriber: &mut Client, want: usize) -> Vec<Vec<String>> {
    let mut seen = Vec::new();

    while seen.len() < want {
        let message = subscriber.read();
        let fields: Vec<String> = message
            .trim_matches(|c| c == '[' || c == ']')
            .split(',')
            .map(str::to_owned)
            .collect();

        if fields[0] == "change" {
            seen.push(fields);
        } else {
            assert_eq!(fields[0], "cursor", "unexpected push {message}");
        }
    }

    seen
}

#[test]
fn events_arrive_in_write_order_with_their_types() {
    let server = Server::start_with(ENV);
    let mut subscriber = Client::connect(&server.address);
    let mut writer = Client::connect(&server.address);

    assert_eq!(
        subscriber.call(&["CHANGES", "SUBSCRIBE", "FROM", "NOW"]),
        "0:0:0"
    );

    writer.call(&["SET", "a", "1"]);
    writer.call(&["RPUSH", "l", "x"]);
    writer.call(&["SADD", "s", "m"]);
    writer.call(&["HSET", "h", "f", "v"]);
    writer.call(&["ZADD", "z", "1", "m"]);
    writer.call(&["EXPIRE", "a", "100"]);
    writer.call(&["RENAME", "a", "b"]);
    writer.call(&["DEL", "b"]);
    writer.call(&["JSON.SET", "j", "$", "{\"x\":1}"]);
    writer.call(&["FLUSHALL"]);

    let events = changes(&mut subscriber, 11);
    let types: Vec<&str> = events.iter().map(|e| e[3].as_str()).collect();

    assert_eq!(
        types,
        [
            "put", "list", "set", "hash", "zset", "expire", "rename", "rename", "del", "json",
            "flush"
        ]
    );
    // A RENAME is one record with two keys: one event per key, written together under one cursor.
    assert_eq!((events[6][4].as_str(), events[7][4].as_str()), ("a", "b"));
    assert_eq!(events[6][1], events[7][1]);
    assert_eq!(events[0][4], "a");
    assert_eq!(events[0][5], "(nil)", "values are only sent with VALUES");

    let cursors: Vec<u64> = events
        .iter()
        .map(|e| e[1].rsplit(':').next().unwrap().parse().unwrap())
        .collect();

    assert!(
        cursors.windows(2).all(|w| w[0] <= w[1]) && cursors[0] < cursors[10],
        "cursors must increase: {cursors:?}"
    );
}

#[test]
fn resuming_from_a_cursor_after_a_crash_loses_and_repeats_nothing() {
    let mut server = Server::start_with(ENV);
    let mut subscriber = Client::connect(&server.address);
    let mut writer = Client::connect(&server.address);

    subscriber.call(&["CHANGES", "SUBSCRIBE", "FROM", "NOW"]);

    for i in 0..120 {
        assert_eq!(
            writer.call(&["SET", &format!("k{i}"), &format!("v{i}")]),
            "OK"
        );
    }

    let first = changes(&mut subscriber, 50);
    let cursor = first.last().unwrap()[1].clone();

    assert_eq!(
        first.iter().map(|e| e[4].clone()).collect::<Vec<_>>(),
        (0..50).map(|i| format!("k{i}")).collect::<Vec<_>>()
    );

    drop((subscriber, writer));
    server.restart();

    let mut writer = Client::connect(&server.address);

    for i in 120..140 {
        assert_eq!(
            writer.call(&["SET", &format!("k{i}"), &format!("v{i}")]),
            "OK"
        );
    }

    let mut resumed = Client::connect(&server.address);

    assert_eq!(
        resumed.call(&["CHANGES", "SUBSCRIBE", "FROM", &cursor, "VALUES"]),
        cursor
    );

    let rest = changes(&mut resumed, 90);

    assert_eq!(
        rest.iter().map(|e| e[4].clone()).collect::<Vec<_>>(),
        (50..140).map(|i| format!("k{i}")).collect::<Vec<_>>(),
        "an event was lost or repeated"
    );
    assert_eq!(rest[0][5], "v50", "VALUES must carry the written value");
}

#[test]
fn match_and_types_filters_select_events() {
    let server = Server::start_with(ENV);
    let mut writer = Client::connect(&server.address);

    writer.call(&["SET", "user:1", "a"]);
    writer.call(&["SET", "order:1", "b"]);
    writer.call(&["RPUSH", "user:list", "x"]);
    writer.call(&["SET", "user:2", "c"]);
    writer.call(&["DEL", "user:2"]);

    let mut by_key = Client::connect(&server.address);

    by_key.call(&["CHANGES", "SUBSCRIBE", "FROM", "0:0", "MATCH", "user:*"]);

    let keys: Vec<String> = changes(&mut by_key, 4)
        .iter()
        .map(|e| e[4].clone())
        .collect();

    assert_eq!(keys, ["user:1", "user:list", "user:2", "user:2"]);

    let mut by_type = Client::connect(&server.address);

    by_type.call(&[
        "CHANGES",
        "SUBSCRIBE",
        "FROM",
        "0:0",
        "TYPES",
        "put,del",
        "MATCH",
        "user:*",
    ]);

    let kinds: Vec<(String, String)> = changes(&mut by_type, 3)
        .iter()
        .map(|e| (e[3].clone(), e[4].clone()))
        .collect();

    assert_eq!(
        kinds,
        [
            ("put".to_owned(), "user:1".to_owned()),
            ("put".to_owned(), "user:2".to_owned()),
            ("del".to_owned(), "user:2".to_owned()),
        ]
    );
}

#[test]
fn bad_cursors_and_arguments_are_refused() {
    let server = Server::start_with(ENV);
    let mut client = Client::connect(&server.address);

    client.call(&["SET", "a", "1"]);

    for (args, why) in [
        (
            vec!["CHANGES", "SUBSCRIBE", "FROM", "0:999999"],
            "ahead of this server",
        ),
        (
            vec!["CHANGES", "SUBSCRIBE", "FROM", "7:1"],
            "different shard",
        ),
    ] {
        let mut probe = Client::connect(&server.address);
        let reply = probe.call(&args);

        assert!(
            reply.starts_with("ERR:") && reply.contains(why),
            "{args:?} -> {reply}"
        );
    }

    for args in [
        vec!["CHANGES", "SUBSCRIBE", "FROM", "garbage"],
        vec!["CHANGES", "SUBSCRIBE", "TYPES", "nonsense"],
        vec!["CHANGES", "SUBSCRIBE", "BOGUS"],
    ] {
        let mut probe = Client::connect(&server.address);
        let reply = probe.call(&args);

        assert!(reply.starts_with("ERR:"), "{args:?} -> {reply}");
    }
}
