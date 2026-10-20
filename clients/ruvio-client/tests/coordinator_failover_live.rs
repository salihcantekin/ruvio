//! A client follows a failover that real coordinators carry out. `scripts/run_with_servers.sh`
//! starts a group of two data nodes (`RUVIO_COORDINATOR_GROUP_ADDRS`, their pids in
//! `RUVIO_COORDINATOR_GROUP_PIDS`) watched by three `ruvio coordinator` processes
//! (`RUVIO_COORDINATOR_ADDRS`, the group named `RUVIO_COORDINATOR_MASTER`). The test kills the
//! primary with SIGKILL, so the group serves one run of the script. Skipped without the variables.

use std::time::{Duration, Instant};

use ruvio_client::{Client, ClientOptions, RespValue};

/// The coordinators notice the kill after `down_after_ms` (1 s) and promote within a few ticks.
const PATIENCE: Duration = Duration::from_secs(15);
/// Each client's first write after the kill comes within this, the seeds client included: the
/// replica that refused is taken once it is promoted, not after the rounds run out (about 7 s).
const FOLLOW_BOUND: Duration = Duration::from_secs(5);

struct Group {
    coordinators: Vec<(String, u16)>,
    master: String,
    members: Vec<(String, u16)>,
    pids: Vec<u32>,
}

fn group() -> Option<Group> {
    let addresses = |name: &str| -> Option<Vec<(String, u16)>> {
        std::env::var(name)
            .ok()?
            .split(',')
            .map(|part| {
                let (host, port) = part.rsplit_once(':')?;

                Some((host.to_owned(), port.parse().ok()?))
            })
            .collect()
    };

    Some(Group {
        coordinators: addresses("RUVIO_COORDINATOR_ADDRS")?,
        master: std::env::var("RUVIO_COORDINATOR_MASTER").ok()?,
        members: addresses("RUVIO_COORDINATOR_GROUP_ADDRS")?,
        pids: std::env::var("RUVIO_COORDINATOR_GROUP_PIDS")
            .ok()?
            .split(',')
            .map(|pid| pid.parse().ok())
            .collect::<Option<_>>()?,
    })
}

fn options(seeds: Vec<(String, u16)>) -> ClientOptions {
    ClientOptions {
        seeds,
        discover_cluster: false,
        connect_timeout: Duration::from_secs(1),
        command_timeout: Some(Duration::from_secs(5)),
        ..ClientOptions::default()
    }
}

fn direct(address: &(String, u16)) -> Client {
    Client::connect_with(options(vec![address.clone()])).unwrap()
}

fn is_master(client: &mut Client) -> bool {
    let reply = client.role().unwrap();

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

/// Writes `value` to `key` through `client` until a write succeeds; the time from `started`.
fn write_until_it_succeeds(
    client: &mut Client,
    key: &str,
    value: &str,
    started: Instant,
) -> Duration {
    loop {
        match client.set(key, value) {
            Ok(_) => return started.elapsed(),
            Err(error) => assert!(
                started.elapsed() < PATIENCE,
                "no write succeeded within {PATIENCE:?}: {error:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn clients_reach_the_new_primary_after_the_coordinators_fail_over() {
    let Some(group) = group() else {
        return;
    };
    let mut members: Vec<Client> = group.members.iter().map(direct).collect();
    let old = members
        .iter_mut()
        .position(is_master)
        .expect("no member of the coordinated group is a primary");
    let new = 1 - old;
    assert!(
        !is_master(&mut members[new]),
        "the coordinated group has two primaries"
    );
    // One client asks the coordinators for the primary, the other has the data nodes as seeds.
    let mut clients = [
        Client::connect_with(ClientOptions {
            sentinel_master: Some(group.master.clone()),
            ..options(group.coordinators.clone())
        })
        .unwrap(),
        Client::connect_with(options(group.members.clone())).unwrap(),
    ];
    let keys = [
        format!("coordinated:{}:sentinel", std::process::id()),
        format!("coordinated:{}:seeds", std::process::id()),
    ];

    for (client, key) in clients.iter_mut().zip(&keys) {
        client.set(key, "before").unwrap();
    }
    // Only a replica that holds every acknowledged write is promoted.
    wait_until("the replica to hold the first writes", || {
        keys.iter()
            .all(|key| members[new].get_string(key).unwrap().as_deref() == Some("before"))
    });

    let status = std::process::Command::new("kill")
        .args(["-9", &group.pids[old].to_string()])
        .status()
        .unwrap();

    assert!(status.success(), "kill -9 failed: {status}");
    // Both clients write at once, so neither waits for the other.
    let started = Instant::now();
    let took: Vec<Duration> = std::thread::scope(|scope| {
        let writers: Vec<_> = clients
            .iter_mut()
            .zip(&keys)
            .map(|(client, key)| {
                scope.spawn(move || write_until_it_succeeds(client, key, "after", started))
            })
            .collect();

        writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect()
    });

    eprintln!(
        "first write after the kill: sentinel_master {:?}, seeds {:?}",
        took[0], took[1]
    );
    for took in &took {
        assert!(*took < FOLLOW_BOUND, "a first write took {took:?}");
    }
    assert!(is_master(&mut members[new]));
    for key in &keys {
        assert_eq!(
            members[new].get_string(key).unwrap().as_deref(),
            Some("after")
        );
    }
}
