//! Runs against the replicated pair `scripts/run_with_servers.sh` starts (a 2-shard primary on
//! `RUVIO_REPLICATED_ADDR`, its 2-shard replica on `RUVIO_REPLICA_ADDR`); skipped without them.

use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use ruvio_client::{Client, ClientOptions, Durability, ReadPolicy, hash_slot};

// Each test counts the GETs one replica shard has served, so they take turns.
static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn addresses() -> Option<(SocketAddr, SocketAddr)> {
    let primary = std::env::var("RUVIO_REPLICATED_ADDR").ok()?.parse().ok()?;
    let replica = std::env::var("RUVIO_REPLICA_ADDR").ok()?.parse().ok()?;

    Some((primary, replica))
}

fn client(primary: SocketAddr, policy: ReadPolicy) -> Client {
    Client::connect_with(ClientOptions {
        host: primary.ip().to_string(),
        port: primary.port(),
        read_policy: policy,
        ..ClientOptions::default()
    })
    .unwrap()
}

/// One shard of the replica, without cluster routing.
fn shard(replica: SocketAddr, key: &str) -> Client {
    let port = replica.port() + u16::from(hash_slot(key) >= 8192);

    Client::connect_with(ClientOptions {
        host: replica.ip().to_string(),
        port,
        discover_cluster: false,
        ..ClientOptions::default()
    })
    .unwrap()
}

fn get_commands(shard: &mut Client) -> u64 {
    let info = shard.info(None).unwrap();

    info.lines()
        .find_map(|line| line.strip_prefix("get_commands:"))
        .and_then(|value| value.trim().parse().ok())
        .expect("INFO reports get_commands")
}

fn wait_for_copy(shard: &mut Client, key: &str, value: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while shard.get_string(key).unwrap().as_deref() != Some(value) {
        assert!(Instant::now() < deadline, "the replica did not copy {key}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn prefer_replica_reads_from_the_replica_and_writes_to_the_primary() {
    let _turn = turn();
    let Some((primary, replica)) = addresses() else {
        return;
    };
    let key = "rust:replica-reads:prefer";
    let mut db = client(primary, ReadPolicy::PreferReplica);
    let mut on_replica = shard(replica, key);

    db.set(key, "v1").unwrap();
    wait_for_copy(&mut on_replica, key, "v1");
    let before = get_commands(&mut on_replica);

    assert_eq!(db.get_string(key).unwrap().as_deref(), Some("v1"));
    assert_eq!(get_commands(&mut on_replica), before + 1);
}

#[test]
fn replica_policy_reads_from_the_replica() {
    let _turn = turn();
    let Some((primary, replica)) = addresses() else {
        return;
    };
    let key = "rust:replica-reads:only";
    let mut db = client(primary, ReadPolicy::Replica);
    let mut on_replica = shard(replica, key);

    db.set(key, "v2").unwrap();
    wait_for_copy(&mut on_replica, key, "v2");
    let before = get_commands(&mut on_replica);

    assert_eq!(db.get_string(key).unwrap().as_deref(), Some("v2"));
    assert_eq!(get_commands(&mut on_replica), before + 1);
}

#[test]
fn primary_policy_keeps_reads_on_the_primary() {
    let _turn = turn();
    let Some((primary, replica)) = addresses() else {
        return;
    };
    let key = "rust:replica-reads:primary";
    let mut db = client(primary, ReadPolicy::Primary);
    let mut on_replica = shard(replica, key);

    db.set(key, "v3").unwrap();
    wait_for_copy(&mut on_replica, key, "v3");
    let before = get_commands(&mut on_replica);

    assert_eq!(db.get_string(key).unwrap().as_deref(), Some("v3"));
    assert_eq!(get_commands(&mut on_replica), before);
}

#[test]
fn durability_option_writes_and_reads_back_on_the_replicated_primary() {
    let Some((primary, _)) = addresses() else {
        return;
    };
    let mut db = Client::connect_with(ClientOptions {
        host: primary.ip().to_string(),
        port: primary.port(),
        durability: Some(Durability {
            replicas: 1,
            timeout_ms: Some(5000),
        }),
        ..ClientOptions::default()
    })
    .unwrap();

    db.set("durability-live", "v").unwrap();
    assert_eq!(
        db.get("durability-live").unwrap().as_deref(),
        Some(&b"v"[..])
    );
    db.del(&["durability-live"]).unwrap();
}
