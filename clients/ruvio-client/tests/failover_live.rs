//! A client with two seeds follows a failover on real servers. `scripts/run_with_servers.sh`
//! starts a pair that can each be primary or replica (`RUVIO_FAILOVER_ADDRS`, their replication
//! addresses in `RUVIO_FAILOVER_REPLICATION`); no coordinator runs, the test swaps the roles
//! itself with `REPLICAOF`, as a coordinator does. Skipped without the variables.

use std::time::{Duration, Instant};

use ruvio_client::{Client, ClientOptions, Error, RespValue};

const PATIENCE: Duration = Duration::from_secs(10);

struct Pair {
    clients: [(String, u16); 2],
    replication: [(String, u16); 2],
}

fn pair() -> Option<Pair> {
    let split = |name: &str| -> Option<[(String, u16); 2]> {
        let value = std::env::var(name).ok()?;
        let mut parts = value.split(',').map(|part| {
            let (host, port) = part.rsplit_once(':')?;

            Some((host.to_owned(), port.parse().ok()?))
        });

        Some([parts.next()??, parts.next()??])
    };

    Some(Pair {
        clients: split("RUVIO_FAILOVER_ADDRS")?,
        replication: split("RUVIO_FAILOVER_REPLICATION")?,
    })
}

fn direct(address: &(String, u16)) -> Client {
    Client::connect_with(ClientOptions {
        host: address.0.clone(),
        port: address.1,
        discover_cluster: false,
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
fn a_client_follows_the_primary_after_the_roles_are_swapped() {
    let Some(pair) = pair() else {
        return;
    };
    // Whichever node is the primary now, so the test can run again on the same servers.
    let (old, new) = if is_master(&pair.clients[0]) {
        (0, 1)
    } else {
        (1, 0)
    };
    let seeds = vec![pair.clients[old].clone(), pair.clients[new].clone()];
    let options = ClientOptions {
        seeds: seeds.clone(),
        discover_cluster: false,
        command_timeout: Some(Duration::from_secs(5)),
        ..ClientOptions::default()
    };
    let key = format!("failover:{}", std::process::id());
    let mut client = Client::connect_with(options.clone()).unwrap();
    let mut on_old = direct(&pair.clients[old]);
    let mut on_new = direct(&pair.clients[new]);

    client.set(&key, "before").unwrap();
    // A replica that still copies refuses to be promoted without FORCE.
    wait_until("the replica to hold the first write", || {
        on_new.get_string(&key).unwrap().as_deref() == Some("before")
    });

    on_new.replicaof_no_one(false).unwrap();
    on_old
        .replicaof(&pair.replication[new].0, pair.replication[new].1)
        .unwrap();

    let started = Instant::now();

    client.set(&key, "after").unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(on_new.get_string(&key).unwrap().as_deref(), Some("after"));

    // Without the option the same situation is the READONLY error.
    let mut stuck = Client::connect_with(ClientOptions {
        follow_primary: false,
        ..options.clone()
    })
    .unwrap();

    match stuck.set(&key, "never") {
        Err(Error::Server(message)) => assert!(message.starts_with("READONLY"), "{message}"),
        other => panic!("expected READONLY, got {other:?}"),
    }

    // A dropped socket: the reconnect starts at the old primary (now a replica) but takes the
    // node that answers `master`.
    let id = client.client_id().unwrap();

    on_new
        .execute(&["CLIENT", "KILL", "ID", &id.to_string()])
        .unwrap();

    wait_until("the client to see its socket closed", || {
        client.ping().is_err()
    });
    // Right after the reconnect, before any write: the client's own socket is on the primary.
    let role = client.role().unwrap();

    assert!(
        matches!(
            role.as_array().and_then(|items| items.first()),
            Some(RespValue::Bulk(kind)) if kind == b"master"
        ),
        "{role:?}"
    );
    client.set(&key, "again").unwrap();
    assert_eq!(on_new.get_string(&key).unwrap().as_deref(), Some("again"));
}
