//! A slot move killed at every step: the server is killed while a range moves between its two
//! shards (copying, catching up, handing over, cleaning up, done) and restarted. Each key must
//! then exist exactly once, on the shard the slot table names, with every acknowledged write.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Client, Server};
use ruvio::routing::hash_slot;

const ENV: &[(&str, &str)] = &[
    ("RUVIO_DURABILITY", "always"),
    ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
    ("RUVIO_SHARDS", "2"),
];
const KEYS: usize = 6000;
const COUNTERS: usize = 8;

/// Keys of slots 0-4095 (shard 0 by the formula), the range that moves.
fn moving(label: &str, count: usize) -> Vec<String> {
    (0..)
        .map(|n| format!("{label}:{n}"))
        .filter(|key| hash_slot(key.as_bytes()) < 4096)
        .take(count)
        .collect()
}

fn load(client: &mut Client, keys: &[String]) {
    for chunk in keys.chunks(500) {
        for key in chunk {
            client.send(&["SET", key, key]);
        }
        for _ in chunk {
            assert_eq!(client.read(), "OK");
        }
    }
}

fn status(client: &mut Client) -> String {
    client.call(&["RUVIO", "MOVESLOTS", "STATUS"])
}

/// The owner of slot 0 as `CLUSTER SLOTS` reports it: the port of its first node.
fn owner_port(client: &mut Client) -> String {
    let slots = client.call(&["CLUSTER", "SLOTS"]);
    let first = slots.split("],[").next().unwrap().to_owned();

    assert!(first.starts_with("[[0,"), "{slots}");
    first
        .split(',')
        .nth(3)
        .unwrap()
        .trim_matches(|c| c == '[' || c == ']')
        .to_owned()
}

fn check(server: &Server, keys: &[String], outside: &str, counters: &[String], acked: &[i64]) {
    let right = server.ports().address_at(1).to_string();
    let mut left = Client::connect(&server.address);
    let mut other = Client::connect(&right);

    for key in keys.iter().step_by(37) {
        assert_eq!(left.call(&["GET", key]), *key);
        assert_eq!(other.call(&["GET", key]), *key);
    }
    for (key, acked) in counters.iter().zip(acked) {
        let value: i64 = left.call(&["GET", key]).parse().unwrap_or(0);

        // `always` syncs before replying: every acknowledged increment is there, plus at most
        // the one in flight when the process died.
        assert!(
            (*acked..=acked + 1).contains(&value),
            "{key}: {value} after {acked} acknowledged"
        );
    }
    assert_eq!(left.call(&["GET", outside]), "stays");

    let owner = owner_port(&mut left);
    let on_left: usize = left.call(&["DBSIZE"]).parse().unwrap();
    let on_right: usize = other.call(&["DBSIZE"]).parse().unwrap();
    // A counter the writer never reached does not exist.
    let counted = counters
        .iter()
        .filter(|key| left.call(&["EXISTS", key]) == "1")
        .count();
    let moving_keys = keys.len() + counted;

    // Each key exactly once, on the shard that owns its slot.
    if owner == server.ports().port_at(0).to_string() {
        assert_eq!((on_left, on_right), (moving_keys + 1, 0));
    } else {
        assert_eq!(owner, server.ports().port_at(1).to_string());
        assert_eq!((on_left, on_right), (1, moving_keys));
    }
}

#[test]
fn a_move_killed_at_any_step_leaves_each_key_once_on_its_owner() {
    let keys = moving("k", KEYS);
    let counters = moving("c", COUNTERS);
    // On shard 0 too, outside the range: it stays.
    let outside = (0..)
        .map(|n| format!("outside:{n}"))
        .find(|key| (4096..8192).contains(&hash_slot(key.as_bytes())))
        .unwrap();
    let mut phases = Vec::new();

    for delay_ms in [0u64, 3, 10, 25, 60, 150, 1000] {
        let mut server = Server::start_with(ENV);
        let mut client = Client::connect(&server.address);

        load(&mut client, &keys);
        client.call(&["SET", &outside, "stays"]);

        // Increments through both ports while the range moves.
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(Mutex::new(vec![0i64; COUNTERS]));
        let writer = {
            let (stop, acked, counters) = (Arc::clone(&stop), Arc::clone(&acked), counters.clone());
            let addresses = [
                server.address.clone(),
                server.ports().address_at(1).to_string(),
            ];

            std::thread::spawn(move || {
                let mut clients = addresses.map(|address| Client::connect(&address));
                let mut round = 0;

                while !stop.load(Ordering::Acquire) {
                    let index = round % counters.len();
                    let client = &mut clients[round % 2];

                    client.send(&["INCR", &counters[index]]);
                    // The kill closes the socket: what was not answered was not acknowledged.
                    let reply =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client.read()));

                    match reply {
                        Ok(reply) if reply.parse::<i64>().is_ok() => {
                            acked.lock().unwrap()[index] += 1
                        }
                        Ok(reply) if reply.starts_with("ERR:TRYAGAIN") => {}
                        _ => return,
                    }
                    round += 1;
                }
            })
        };

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(client.call(&["RUVIO", "MOVESLOTS", "0-4095", "1"]), "OK");
        let started = Instant::now();

        while started.elapsed() < Duration::from_millis(delay_ms) {
            std::thread::sleep(Duration::from_millis(1));
        }
        phases.push(status(&mut client));
        stop.store(true, Ordering::Release);
        server.stop();
        let _ = writer.join();
        let acked = acked.lock().unwrap().clone();

        server.restart();
        check(&server, &keys, &outside, &counters, &acked);

        // The directory moves again after the crash, whatever step it stopped at.
        let mut client = Client::connect(&server.address);
        let owner = owner_port(&mut client);
        let target = if owner == server.ports().port_at(0).to_string() {
            "1"
        } else {
            "0"
        };

        assert_eq!(client.call(&["RUVIO", "MOVESLOTS", "0-4095", target]), "OK");
        let deadline = Instant::now() + Duration::from_secs(20);

        while !status(&mut client).contains("done") {
            assert!(Instant::now() < deadline, "{}", status(&mut client));
            std::thread::sleep(Duration::from_millis(5));
        }
        server.restart();
        check(&server, &keys, &outside, &counters, &acked);
    }

    // The delays reached the copy and the end of a move, not only one of them.
    assert!(
        phases.iter().any(|phase| phase.contains("copying")),
        "{phases:?}"
    );
    assert!(
        phases
            .iter()
            .any(|phase| phase.contains("done") || phase.contains("cleaning")),
        "{phases:?}"
    );
}
