//! `CHANGES SUBSCRIBE` cursors across a failover: a consumer past the fork is told the records
//! it saw were taken back, and a `COMMITTED` consumer never sees them.

use super::slot_moves::{Client, Value, text};
use super::*;

use std::io::BufRead;

fn persistence(directory: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    }
}

fn replication(
    role: crate::config::ReplicationRole,
    listen: Option<SocketAddr>,
    primary: Option<SocketAddr>,
) -> crate::config::ReplicationConfig {
    crate::config::ReplicationConfig {
        role,
        listen,
        primary,
        ..Default::default()
    }
}

fn command(address: SocketAddr, args: &[&[u8]]) -> Vec<u8> {
    request(address, &resp(&[args]))
}

/// `generation:shard:sequence` as numbers.
fn cursor(value: &Value) -> (u64, u32, u64) {
    let text = text(value);
    let parts: Vec<&str> = text.split(':').collect();

    assert_eq!(parts.len(), 3, "{text}");
    (
        parts[0].parse().unwrap(),
        parts[1].parse().unwrap(),
        parts[2].parse().unwrap(),
    )
}

/// The next push that is not a cursor heartbeat.
fn next_event(feed: &mut Client) -> Vec<Value> {
    loop {
        match feed.read() {
            Value::Array(items) if text(&items[0]) != "cursor" => return items,
            Value::Array(_) => {}
            other => panic!("not a push: {other:?}"),
        }
    }
}

/// Every push until the server closes the connection.
fn read_until_closed(feed: &mut Client) -> Vec<Vec<Value>> {
    let mut pushes = Vec::new();

    while matches!(feed.0.fill_buf(), Ok(buffered) if !buffered.is_empty()) {
        match feed.read() {
            Value::Array(items) => pushes.push(items),
            other => panic!("not a push: {other:?}"),
        }
    }
    pushes
}

#[test]
fn a_change_cursor_past_the_fork_is_rewound_and_a_committed_one_never_passes_it() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let (old, new) = (ports.address_at(0), ports.address_at(1));

    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = replication(Primary, Some(old), None);
    });
    let (_, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = replication(Replica, Some(new), Some(old));
    });

    wait_for_info(primary_address, "replication_replicas", |count| count == 1);
    // Committed: the replica holds it on disk.
    assert_eq!(
        request(
            primary_address,
            &resp(&[
                &[
                    b"CLIENT",
                    b"DURABILITY",
                    b"REPLICAS",
                    b"1",
                    b"TIMEOUT",
                    b"10000"
                ],
                &[b"SET", b"kept", b"1"],
            ]),
        ),
        b"+OK\r\n+OK\r\n"
    );
    let committed = wait_for_info(primary_address, "replication_committed_sequence", |seq| {
        seq > 0
    });

    // The replica goes; the next write is on the primary alone.
    replica_stop.request();
    join_within(replica, PATIENCE).unwrap();
    assert_eq!(
        command(primary_address, &[b"SET", b"lost", b"1"]),
        b"+OK\r\n"
    );

    // A consumer of every record sees the write the failover takes back.
    let mut everything = Client::connect(primary_address);

    assert_eq!(
        everything.call(&[b"CHANGES", b"SUBSCRIBE", b"FROM", b"0:0:0"]),
        Value::Bulk(Some(b"0:0:0".to_vec()))
    );
    let (mut kept_at, mut lost_at) = (None, None);

    while lost_at.is_none() {
        let event = next_event(&mut everything);

        assert_eq!(text(&event[0]), "change", "{event:?}");
        match text(&event[4]).as_str() {
            "kept" => kept_at = Some(event[1].clone()),
            "lost" => lost_at = Some(event[1].clone()),
            _ => {}
        }
    }
    let (kept_at, lost_at) = (kept_at.unwrap(), lost_at.unwrap());

    assert_eq!(cursor(&kept_at).0, 0);
    assert!(cursor(&kept_at).2 <= committed);
    assert!(cursor(&lost_at).2 > committed);

    // A committed consumer gets the committed write and stops there: both records were
    // durable when it subscribed, so without the gate one read would have carried both.
    let mut committed_feed = Client::connect(primary_address);

    assert_eq!(
        committed_feed.call(&[b"CHANGES", b"SUBSCRIBE", b"FROM", b"0:0:0", b"COMMITTED"]),
        Value::Bulk(Some(b"0:0:0".to_vec()))
    );
    let mut committed_last = loop {
        let event = next_event(&mut committed_feed);

        assert_ne!(text(&event[4]), "lost", "{event:?}");
        if text(&event[4]) == "kept" {
            break event[1].clone();
        }
    };

    primary_stop.request();
    join_within(primary, PATIENCE).unwrap();
    for push in read_until_closed(&mut committed_feed) {
        assert!(
            push.iter().all(|item| text(item) != "lost"),
            "a committed stream passed the commit: {push:?}"
        );
        if text(&push[0]) == "cursor" {
            assert!(cursor(&push[1]).2 <= committed, "{push:?}");
            committed_last = push[1].clone();
        }
    }

    // The replica comes back and is promoted: it forks at the last record it holds.
    let (promoted_address, promoted_stop, promoted) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = replication(Replica, Some(new), Some(old));
    });

    wait_until("the replica to be promoted", || {
        command(promoted_address, &[b"REPLICAOF", b"NO", b"ONE"]) == b"+OK\r\n"
    });
    assert_eq!(info_field(promoted_address, "replication_epoch"), Some(1));

    // The consumer past the fork is told once, then streams from the fork.
    let mut rewound = Client::connect(promoted_address);
    let lost_cursor = text(&lost_at);
    let reply = rewound.call(&[b"CHANGES", b"SUBSCRIBE", b"FROM", lost_cursor.as_bytes()]);
    let (generation, shard, fork) = cursor(&reply);

    assert_eq!((generation, shard), (1, 0));
    assert!(
        fork >= cursor(&kept_at).2 && fork < cursor(&lost_at).2,
        "{reply:?}"
    );
    assert_eq!(
        next_event(&mut rewound),
        vec![Value::Bulk(Some(b"rewound".to_vec())), reply.clone()]
    );
    assert_eq!(
        command(promoted_address, &[b"SET", b"after", b"1"]),
        b"+OK\r\n"
    );
    let event = next_event(&mut rewound);

    assert_eq!(text(&event[0]), "change", "{event:?}");
    assert_eq!(text(&event[4]), "after", "{event:?}");
    let (generation, _, sequence) = cursor(&event[1]);

    assert_eq!(generation, 1);
    assert!(sequence > fork);

    // The committed consumer resumes with no rewind: it saw nothing past the fork.
    let mut resumed = Client::connect(promoted_address);
    let last = text(&committed_last);
    let reply = resumed.call(&[b"CHANGES", b"SUBSCRIBE", b"FROM", last.as_bytes()]);

    assert_eq!(cursor(&reply), (1, 0, cursor(&committed_last).2));
    let event = next_event(&mut resumed);

    assert_eq!(text(&event[0]), "change", "{event:?}");
    assert_eq!(text(&event[4]), "after", "{event:?}");

    // Cursors this shard cannot place are refused.
    assert_eq!(
        Client::connect(promoted_address).call(&[b"CHANGES", b"SUBSCRIBE", b"FROM", b"2:0:1"]),
        Value::Error("ERR change cursor generation is newer than this server".into())
    );

    promoted_stop.request();
    join_within(promoted, PATIENCE).unwrap();
}

#[test]
fn a_committed_change_stream_needs_replication() {
    let dir = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.persistence = persistence(dir.path());
    });

    assert_eq!(
        command(address, &[b"CHANGES", b"SUBSCRIBE", b"COMMITTED"]),
        b"-ERR CHANGES SUBSCRIBE COMMITTED needs replication on this server\r\n"
    );

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A primary told to follow another one ends its change streams before its log takes the new
/// primary's records: the subscriber resumes there.
#[test]
fn a_demoted_primary_ends_its_change_streams() {
    use crate::config::ReplicationRole::Primary;

    let demoted_dir = tempfile::tempdir().unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let (demoted_listen, other_listen) = (ports.address_at(0), ports.address_at(1));

    let (demoted_address, demoted_stop, demoted) = serve_with(|config| {
        config.persistence = persistence(demoted_dir.path());
        config.replication = replication(Primary, Some(demoted_listen), None);
    });
    let (_, other_stop, other) = serve_with(|config| {
        config.persistence = persistence(other_dir.path());
        config.replication = replication(Primary, Some(other_listen), None);
    });

    let mut feed = Client::connect(demoted_address);

    assert!(matches!(
        feed.call(&[b"CHANGES", b"SUBSCRIBE"]),
        Value::Bulk(Some(_))
    ));
    assert_eq!(
        command(demoted_address, &[b"SET", b"before", b"1"]),
        b"+OK\r\n"
    );
    let event = next_event(&mut feed);

    assert_eq!(text(&event[0]), "change", "{event:?}");
    assert_eq!(text(&event[4]), "before", "{event:?}");

    let port = other_listen.port().to_string();

    assert_eq!(
        command(
            demoted_address,
            &[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]
        ),
        b"+OK\r\n"
    );
    let pushes = read_until_closed(&mut feed);
    let last = pushes.last().expect("the stream ended without a notice");

    assert_eq!(
        last,
        &vec![
            Value::Bulk(Some(b"resync".to_vec())),
            Value::Bulk(Some(
                b"the shard follows another primary; resume on it".to_vec()
            )),
        ]
    );
    assert!(
        pushes[..pushes.len() - 1]
            .iter()
            .all(|push| text(&push[0]) == "cursor"),
        "{pushes:?}"
    );

    demoted_stop.request();
    join_within(demoted, PATIENCE).unwrap();
    other_stop.request();
    join_within(other, PATIENCE).unwrap();
}
