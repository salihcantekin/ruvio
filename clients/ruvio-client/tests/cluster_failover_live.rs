//! A cluster client leaves a demoted primary after its `READONLY` reply, on real servers.
//! `scripts/run_with_servers.sh` starts two 2-shard nodes that can each be primary or replica
//! (`RUVIO_CLUSTER_FAILOVER_ADDRS`, shard 0 of each; their replication addresses in
//! `RUVIO_CLUSTER_FAILOVER_REPLICATION`; both primary first). No coordinator runs: the test swaps
//! the roles itself with `REPLICAOF`, as a coordinator does. Skipped without the variables.

use std::time::{Duration, Instant};

use ruvio_client::{Client, ClientOptions, Error, RespValue};

const PATIENCE: Duration = Duration::from_secs(10);

fn split(name: &str) -> Option<[(String, u16); 2]> {
    let value = std::env::var(name).ok()?;
    let mut parts = value.split(',').map(|part| {
        let (host, port) = part.rsplit_once(':')?;

        Some((host.to_owned(), port.parse().ok()?))
    });

    Some([parts.next()??, parts.next()??])
}

fn direct(address: &(String, u16)) -> Client {
    Client::connect_with(ClientOptions {
        host: address.0.clone(),
        port: address.1,
        discover_cluster: false,
        follow_primary: false,
        command_timeout: Some(Duration::from_secs(5)),
        ..ClientOptions::default()
    })
    .unwrap()
}

fn is_master(address: &(String, u16)) -> bool {
    let reply = direct(address).role().unwrap();

    matches!(
        reply.as_array().and_then(|items| items.first()),
        Some(RespValue::Bulk(kind)) if kind == b"master"
    )
}

#[track_caller]
fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;

    while !done() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_cluster_client_reloads_its_slots_after_readonly_from_a_demoted_primary() {
    let (Some(clients), Some(replication)) = (
        split("RUVIO_CLUSTER_FAILOVER_ADDRS"),
        split("RUVIO_CLUSTER_FAILOVER_REPLICATION"),
    ) else {
        return;
    };
    // Whichever node is the primary now, so the test can run again on the same servers.
    let (old, new) = if is_master(&clients[0]) {
        (0, 1)
    } else {
        (1, 0)
    };
    let mut client = Client::connect_with(ClientOptions {
        host: clients[old].0.clone(),
        port: clients[old].1,
        discover_cluster: true,
        follow_primary: false,
        command_timeout: Some(Duration::from_secs(5)),
        ..ClientOptions::default()
    })
    .unwrap();
    // One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
    let tag = std::process::id();
    let keys = [
        format!("cluster-failover:rust:{tag}:{{a}}"),
        format!("cluster-failover:rust:{tag}:{{b}}"),
    ];
    let mut on_old = direct(&clients[old]);
    let mut on_new = direct(&clients[new]);

    for key in &keys {
        client.set(key, "before").unwrap();
    }
    // A replica that still copies refuses to be promoted without FORCE.
    wait_until("the replica to hold the first writes", || {
        keys.iter()
            .all(|key| on_new.get_string(key).unwrap().as_deref() == Some("before"))
    });

    on_new.replicaof_no_one(false).unwrap();
    on_old
        .replicaof(&replication[new].0, replication[new].1)
        .unwrap();
    wait_until("the old primary to become a replica", || {
        !is_master(&clients[old])
    });

    // The demoted node names its new primary once that primary has accepted it.
    let started = Instant::now();

    loop {
        match client.set(&keys[0], "after") {
            Ok(set) => {
                assert!(set);
                break;
            }
            Err(Error::Server(message)) if message.starts_with("READONLY") => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "still refused after {:?}: {message}",
                    started.elapsed()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(other) => panic!("{other:?}"),
        }
    }
    assert_eq!(
        on_new.get_string(&keys[0]).unwrap().as_deref(),
        Some("after")
    );
    // The other shard's slots moved too: no write is refused once the map is reloaded.
    for round in 0..10 {
        for key in &keys {
            client.set(key, &format!("v{round}")).unwrap();
        }
    }
    for key in &keys {
        assert_eq!(on_new.get_string(key).unwrap().as_deref(), Some("v9"));
    }
}
