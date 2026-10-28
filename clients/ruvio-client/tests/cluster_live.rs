//! Runs against a sharded server when `RUVIO_CLUSTER_ADDR` is set, for example
//! `RUVIO_CLUSTER_ADDR=127.0.0.1:16400 cargo test -p ruvio-client --test cluster_live`.

use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};

use ruvio_client::{Client, Error, RespValue, hash_slot};

// The tests flush and count keys in the one database a cluster has, so they take turns.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn connect() -> Option<Client> {
    let address: SocketAddr = std::env::var("RUVIO_CLUSTER_ADDR").ok()?.parse().ok()?;
    let mut client = Client::connect(address.ip().to_string(), address.port()).unwrap();

    client
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();

    Some(client)
}

#[test]
fn routes_keys_and_splits_mget() {
    let _turn = turn();
    let Some(mut db) = connect() else { return };

    assert!(db.is_cluster());
    assert!(db.shard_count() >= 2);
    assert_eq!(hash_slot("somekey"), 11_058);
    db.flushdb().unwrap();
    db.set("{user1000}.following", "left").unwrap();
    db.set("somekey", "right").unwrap();
    assert_eq!(
        db.mget(&["{user1000}.following", "somekey"]).unwrap(),
        vec![Some("left".to_owned()), Some("right".to_owned())]
    );
    assert_eq!(db.cluster_keyslot("somekey").unwrap(), 11_058);
    assert!(db.cluster_info().unwrap().contains("cluster_state:ok"));
    assert_eq!(db.dbsize().unwrap(), 2);
    db.readonly().unwrap();
    db.readwrite().unwrap();
}

const CROSSSLOT: &str = "CROSSSLOT Keys in request do not hash to the same slot";

#[test]
fn refuses_rename_and_other_commands_across_slots_before_sending() {
    let _turn = turn();
    let Some(mut db) = connect() else {
        return;
    };

    let source = "portal-rename-src";
    let destination = "key1-rename-dst";

    assert_ne!(hash_slot(source), hash_slot(destination));
    db.del(&[source, destination]).unwrap();
    db.set(source, "from-screen").unwrap();

    for command in [
        ["RENAME", source, destination].as_slice(),
        &["RENAMENX", source, destination],
        &["COPY", source, destination],
        &["SUNIONSTORE", destination, source],
    ] {
        match db.execute(command) {
            Err(Error::Server(message)) => assert_eq!(message, CROSSSLOT, "{command:?}"),
            other => panic!("{command:?}: expected CROSSSLOT, got {other:?}"),
        }
    }

    // Nothing was sent: the source is where it was and the destination was never written.
    assert_eq!(
        db.get(source).unwrap().as_deref(),
        Some(b"from-screen".as_slice())
    );
    assert_eq!(db.get(destination).unwrap(), None);

    // In one slot the server renames it.
    db.execute(&["RENAME", source, "{portal-rename-src}x"])
        .unwrap();
    assert_eq!(
        db.get("{portal-rename-src}x").unwrap().as_deref(),
        Some(b"from-screen".as_slice())
    );
    db.del(&["{portal-rename-src}x"]).unwrap();
}

#[test]
fn a_pipeline_splits_cross_slot_commands_and_refuses_the_rest() {
    let _turn = turn();
    let Some(mut db) = connect() else {
        return;
    };

    let (left, right) = ("{user1000}.following", "somekey");

    assert_ne!(hash_slot(left), hash_slot(right));
    db.del(&[left, right]).unwrap();

    let replies = db
        .execute_many(&[
            &["MSET", left, "l", right, "r"],
            &["MGET", right, "missing-pipeline-key", left],
            &["COPY", left, right],
            &["EXISTS", left, right, left],
            &["DEL", left, right],
        ])
        .unwrap();

    assert_eq!(
        replies,
        vec![
            RespValue::Simple("OK".to_owned()),
            RespValue::Array(vec![
                RespValue::Bulk(b"r".to_vec()),
                RespValue::Null,
                RespValue::Bulk(b"l".to_vec()),
            ]),
            RespValue::Error(CROSSSLOT.to_owned()),
            RespValue::Integer(3),
            RespValue::Integer(2),
        ]
    );
}

#[test]
fn reports_slot_moves_and_refuses_a_range_already_in_place() {
    let _turn = turn();
    let Some(mut db) = connect() else { return };
    let status = db.move_slots_status().unwrap();

    assert!(status.iter().any(|(name, _)| name == "state"), "{status:?}");
    // Slot 0 is on shard 0: nothing to move, and the server says why.
    match db.move_slots(0, 10, 0) {
        Err(Error::Server(message)) => {
            assert!(message.contains("already on that shard"), "{message}")
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}
