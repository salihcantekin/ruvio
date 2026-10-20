//! Two nodes of one multi-node cluster, each with two shards: every node serves the slots of
//! its own group, answers `MOVED` with the owning shard's address for the others, and
//! describes the whole map in `CLUSTER SLOTS`.

mod common;

use common::{Client, Server};
use ruvio::routing::hash_slot;

fn key_in(range: std::ops::Range<u16>, label: &str) -> String {
    (0..)
        .map(|n| format!("{label}:{n}"))
        .find(|key| range.contains(&hash_slot(key.as_bytes())))
        .unwrap()
}

fn write_topology(path: &std::path::Path, a_port: u16, b_port: u16) {
    std::fs::write(
        path,
        format!(
            "version = 7\n\
             [[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [\"0-8191\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"8192-16383\"]\n"
        ),
    )
    .unwrap();
}

#[test]
fn a_node_holding_keys_the_map_gives_away_refuses_to_start() {
    let mut a = Server::start_with(&[("RUVIO_SHARDS", "2"), ("RUVIO_DURABILITY", "always")]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let elsewhere = key_in(8192..16384, "elsewhere");

    // Written while the node owned every slot.
    assert_eq!(
        Client::connect(&a.ports().address_at(1).to_string()).call(&["SET", &elsewhere, "1"]),
        "OK"
    );
    write_topology(&topology, a.ports().port_at(0), a.ports().port_at(2));
    a.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
    a.set_env("RUVIO_CLUSTER_GROUP", "a");

    let log = a.restart_refused();

    assert!(
        log.contains("does not serve under cluster_topology"),
        "{log}"
    );
}

#[test]
fn each_node_serves_its_slots_and_redirects_the_rest() {
    let mut a = Server::start_with(&[("RUVIO_SHARDS", "2")]);
    let mut b = Server::start_with(&[("RUVIO_SHARDS", "2")]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let (a_port, b_port) = (a.ports().port_at(0), b.ports().port_at(0));

    write_topology(&topology, a_port, b_port);

    for (server, group) in [(&mut a, "a"), (&mut b, "b")] {
        server.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
        server.set_env("RUVIO_CLUSTER_GROUP", group);
        server.restart();
    }

    let local = key_in(0..8192, "here");
    // Slot 12288 and above is shard 1 of group b.
    let remote = key_in(12288..16384, "there");
    let remote_slot = hash_slot(remote.as_bytes());
    let moved = format!("ERR:MOVED {remote_slot} 127.0.0.1:{}", b_port + 1);
    let mut on_a = Client::connect(&a.address);
    let mut on_b1 = Client::connect(&b.ports().address_at(1).to_string());

    // Local keys run on either shard of node a; remote ones are redirected, on the lean
    // path (GET, SET) and the generic one (APPEND) alike.
    assert_eq!(on_a.call(&["SET", &local, "1"]), "OK");
    assert_eq!(on_a.call(&["GET", &local]), "1");
    assert_eq!(on_a.call(&["SET", &remote, "1"]), moved);
    assert_eq!(on_a.call(&["GET", &remote]), moved);
    assert_eq!(on_a.call(&["APPEND", &remote, "x"]), moved);

    // The owner takes them.
    assert_eq!(on_b1.call(&["SET", &remote, "2"]), "OK");
    assert_eq!(on_b1.call(&["GET", &remote]), "2");
    let b_local = format!("ERR:MOVED {} 127.0.0.1:", hash_slot(local.as_bytes()));
    assert!(on_b1.call(&["GET", &local]).starts_with(&b_local));

    // A transaction touching another node's key is aborted.
    assert_eq!(on_a.call(&["MULTI"]), "OK");
    assert_eq!(on_a.call(&["SET", &remote, "3"]), moved);
    assert!(on_a.call(&["EXEC"]).starts_with("ERR:EXECABORT"));
    assert_eq!(on_b1.call(&["GET", &remote]), "2");

    // Every node describes the whole map: its two shards, then the other group's.
    let slots = on_a.call(&["CLUSTER", "SLOTS"]);
    for (first, last, port) in [
        (0, 4095, a_port),
        (4096, 8191, a_port + 1),
        (8192, 12287, b_port),
        (12288, 16383, b_port + 1),
    ] {
        let range = format!("[{first},{last},[127.0.0.1,{port},");

        assert!(slots.contains(&range), "{range} missing in {slots}");
    }
    assert_eq!(slots, on_b1.call(&["CLUSTER", "SLOTS"]));

    let info = on_a.call(&["CLUSTER", "INFO"]);
    assert!(info.contains("cluster_current_epoch:7"), "{info}");
    assert!(info.contains("cluster_size:4"), "{info}");
    assert_eq!(
        on_a.call(&["CONFIG", "GET", "cluster-group"]),
        "[cluster-group,a]"
    );

    // A newer map may move another group's primary; it may not give this group a slot
    // whose data is not here.
    let map = |version: u64, a_slots: &str, b_slots: &str, b_port: u16| {
        format!(
            "version = {version}\n[[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [\"{a_slots}\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"{b_slots}\"]\n"
        )
    };

    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "7");
    assert!(
        on_a.call(&[
            "RUVIO",
            "TOPOLOGY",
            "SET",
            &map(6, "0-8191", "8192-16383", 9000)
        ])
        .contains("older")
    );
    assert!(
        on_a.call(&[
            "RUVIO",
            "TOPOLOGY",
            "SET",
            &map(8, "0-8192", "8193-16383", 9000)
        ])
        .contains("whose data is not here")
    );
    assert_eq!(
        on_a.call(&[
            "RUVIO",
            "TOPOLOGY",
            "SET",
            &map(8, "0-8191", "8192-16383", 9000)
        ]),
        "OK"
    );
    assert_eq!(
        on_a.call(&["GET", &remote]),
        format!("ERR:MOVED {remote_slot} 127.0.0.1:9001")
    );
    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "8");

    // The newer map survives a restart (kept in the data directory).
    a.restart();
    let mut on_a = Client::connect(&a.address);

    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "VERSION"]), "8");
}

#[test]
fn slots_move_between_a_nodes_own_shards_and_survive_a_restart() {
    let mut a = Server::start_with(&[
        ("RUVIO_SHARDS", "2"),
        ("RUVIO_DURABILITY", "always"),
        ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let b_port = a.ports().port_at(4);

    // Group a owns 0-4095: 0-2047 on shard 0 and 2048-4095 on shard 1, where the shard
    // formula would put all of them on shard 0.
    std::fs::write(
        &topology,
        format!(
            "version = 1\n\
             [[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{}\"\nshards = 2\nslots = [\"0-4095\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"4096-16383\"]\n",
            a.ports().port_at(0)
        ),
    )
    .unwrap();
    a.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
    a.set_env("RUVIO_CLUSTER_GROUP", "a");
    a.restart();

    // Keys on both shards; then 0-1023 moves from shard 0 to shard 1.
    let keys: Vec<String> = (0..)
        .map(|n| format!("moving:{n}"))
        .filter(|key| {
            let slot = hash_slot(key.as_bytes());

            slot < 1024 || (2048..4096).contains(&slot)
        })
        .take(300)
        .collect();
    let mut client = Client::connect(&a.address);

    for key in &keys {
        assert_eq!(client.call(&["SET", key, key]), "OK");
    }
    // Another group's range cannot move.
    assert!(
        client
            .call(&["RUVIO", "MOVESLOTS", "9000-9100", "1"])
            .contains("another node")
    );
    assert_eq!(client.call(&["RUVIO", "MOVESLOTS", "0-1023", "1"]), "OK");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);

    while !client
        .call(&["RUVIO", "MOVESLOTS", "STATUS"])
        .contains("done")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the move did not finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let (shard0, shard1) = (a.ports().port_at(0), a.ports().port_at(1));
    let check = |client: &mut Client| {
        let slots = client.call(&["CLUSTER", "SLOTS"]);

        assert!(
            slots.contains(&format!("[0,1023,[127.0.0.1,{shard1},")),
            "{slots}"
        );
        assert!(
            slots.contains(&format!("[1024,2047,[127.0.0.1,{shard0},")),
            "{slots}"
        );
        assert!(
            slots.contains(&format!("[2048,4095,[127.0.0.1,{shard1},")),
            "{slots}"
        );
        assert!(slots.contains(&format!(",[127.0.0.1,{b_port},")), "{slots}");
        for key in &keys {
            assert_eq!(client.call(&["GET", key]), *key);
        }
    };

    check(&mut client);

    // The move is kept against the map's placement across a restart.
    a.restart();
    let mut client = Client::connect(&a.address);

    check(&mut client);
}

#[test]
fn moves_made_without_a_map_refuse_a_start_with_one() {
    let mut a = Server::start_with(&[
        ("RUVIO_SHARDS", "2"),
        ("RUVIO_DURABILITY", "always"),
        ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let mut client = Client::connect(&a.address);

    assert_eq!(client.call(&["SET", &key_in(0..100, "early"), "1"]), "OK");
    assert_eq!(client.call(&["RUVIO", "MOVESLOTS", "0-99", "1"]), "OK");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);

    while !client
        .call(&["RUVIO", "MOVESLOTS", "STATUS"])
        .contains("done")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the move did not finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    write_topology(&topology, a.ports().port_at(0), a.ports().port_at(4));
    a.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
    a.set_env("RUVIO_CLUSTER_GROUP", "a");

    let log = a.restart_refused();

    assert!(log.contains("with another slot layout"), "{log}");
}

fn wait_for(client: &mut Client, args: &[&str], expected: &str) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);

    loop {
        let reply = client.call(args);

        if reply.contains(expected) {
            return reply;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{args:?} never answered {expected}: {reply}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

#[test]
fn a_slot_range_moves_to_another_node_and_stays_there_after_restarts() {
    let env = [
        ("RUVIO_SHARDS", "2"),
        ("RUVIO_DURABILITY", "always"),
        ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
    ];
    let mut a = Server::start_with(&env);
    let mut b = Server::start_with(&env);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let (a_port, b_port) = (a.ports().port_at(0), b.ports().port_at(0));
    let map = |version: u64, a_slots: &str, b_slots: &str| {
        format!(
            "version = {version}\n[[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [{a_slots}]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [{b_slots}]\n"
        )
    };

    std::fs::write(
        &topology,
        map(
            1,
            "\"0-4095@0\", \"4096-8191@1\"",
            "\"8192-12287@0\", \"12288-16383@1\"",
        ),
    )
    .unwrap();
    for (server, group) in [(&mut a, "a"), (&mut b, "b")] {
        server.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
        server.set_env("RUVIO_CLUSTER_GROUP", group);
        server.restart();
    }

    let moving: Vec<String> = (0..)
        .map(|n| format!("moving:{n}"))
        .filter(|key| (100..200).contains(&hash_slot(key.as_bytes())))
        .take(200)
        .collect();
    let staying = key_in(300..400, "staying");
    let mut on_a = Client::connect(&a.address);
    let mut on_b = Client::connect(&b.address);

    for key in &moving {
        assert_eq!(on_a.call(&["SET", key, key]), "OK");
    }
    assert_eq!(on_a.call(&["SET", &staying, "1"]), "OK");

    // The target expects the range on its shard 0, then the source copies it there.
    let port = b_port.to_string();

    assert_eq!(
        on_b.call(&["RUVIO", "MIGRATE", "IMPORT", "m1", "100", "199", "0"]),
        "OK"
    );
    assert!(
        on_b.call(&["RUVIO", "MIGRATE", "IMPORT", "m2", "300", "399", "0"])
            .contains("another range")
    );
    assert_eq!(
        on_a.call(&[
            "RUVIO",
            "MIGRATE",
            "EXPORT",
            "m1",
            "100",
            "199",
            "127.0.0.1",
            &port
        ]),
        "OK"
    );
    wait_for(&mut on_a, &["RUVIO", "MIGRATE", "STATUS"], "state,prepared");
    wait_for(&mut on_b, &["RUVIO", "MIGRATE", "STATUS"], "state,prepared");

    // Frozen until the map decides: the source asks clients to retry, the target does
    // not serve the range yet.
    assert!(
        on_a.call(&["SET", &moving[0], "late"])
            .starts_with("ERR:TRYAGAIN")
    );
    assert!(on_b.call(&["GET", &moving[0]]).starts_with("ERR:MOVED"));
    assert_eq!(on_a.call(&["GET", &staying]), "1");

    let next = map(
        2,
        "\"0-99@0\", \"200-4095@0\", \"4096-8191@1\"",
        "\"100-199@0\", \"8192-12287@0\", \"12288-16383@1\"",
    );

    assert_eq!(on_b.call(&["RUVIO", "TOPOLOGY", "SET", &next]), "OK");
    assert_eq!(on_a.call(&["RUVIO", "TOPOLOGY", "SET", &next]), "OK");
    wait_for(&mut on_a, &["RUVIO", "MIGRATE", "STATUS"], "state,done");
    wait_for(&mut on_b, &["RUVIO", "MIGRATE", "STATUS"], "state,done");

    let check = |a: &Server, b: &Server| {
        let mut on_a = Client::connect(&a.address);
        let mut on_b = Client::connect(&b.address);

        for key in &moving {
            assert_eq!(on_b.call(&["GET", key]), *key);
            assert_eq!(
                on_a.call(&["GET", key]),
                format!("ERR:MOVED {} 127.0.0.1:{b_port}", hash_slot(key.as_bytes()))
            );
        }
        assert_eq!(on_a.call(&["GET", &staying]), "1");
        assert_eq!(on_a.call(&["DBSIZE"]), "1");
        let slots = on_a.call(&["CLUSTER", "SLOTS"]);

        assert!(
            slots.contains(&format!("[100,199,[127.0.0.1,{b_port},")),
            "{slots}"
        );
    };

    check(&a, &b);
    a.restart();
    b.restart();
    check(&a, &b);
}

#[test]
fn a_prepared_move_survives_restarts_on_both_sides_until_it_is_cancelled() {
    let env = [
        ("RUVIO_SHARDS", "2"),
        ("RUVIO_DURABILITY", "always"),
        ("RUVIO_SNAPSHOT_INTERVAL_MS", "0"),
    ];
    let mut a = Server::start_with(&env);
    let mut b = Server::start_with(&env);
    let dir = tempfile::tempdir().unwrap();
    let topology = dir.path().join("topology.toml");
    let (a_port, b_port) = (a.ports().port_at(0), b.ports().port_at(0));

    std::fs::write(
        &topology,
        format!(
            "version = 1\n[[groups]]\nname = \"a\"\naddress = \"127.0.0.1:{a_port}\"\nshards = 2\nslots = [\"0-4095@0\", \"4096-8191@1\"]\n\
             [[groups]]\nname = \"b\"\naddress = \"127.0.0.1:{b_port}\"\nshards = 2\nslots = [\"8192-12287@0\", \"12288-16383@1\"]\n"
        ),
    )
    .unwrap();
    for (server, group) in [(&mut a, "a"), (&mut b, "b")] {
        server.set_env("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap());
        server.set_env("RUVIO_CLUSTER_GROUP", group);
        server.restart();
    }

    let key = key_in(100..200, "kept");
    let mut on_a = Client::connect(&a.address);
    let mut on_b = Client::connect(&b.address);
    let port = b_port.to_string();

    assert_eq!(on_a.call(&["SET", &key, "1"]), "OK");
    assert_eq!(
        on_b.call(&["RUVIO", "MIGRATE", "IMPORT", "m1", "100", "199", "0"]),
        "OK"
    );
    assert_eq!(
        on_a.call(&[
            "RUVIO",
            "MIGRATE",
            "EXPORT",
            "m1",
            "100",
            "199",
            "127.0.0.1",
            &port
        ]),
        "OK"
    );
    wait_for(&mut on_a, &["RUVIO", "MIGRATE", "STATUS"], "state,prepared");

    // The source comes back frozen: the target may own the range by now.
    a.restart();
    b.restart();
    let mut on_a = Client::connect(&a.address);
    let mut on_b = Client::connect(&b.address);

    assert!(on_a.call(&["SET", &key, "2"]).starts_with("ERR:TRYAGAIN"));
    assert!(
        on_a.call(&["RUVIO", "MIGRATE", "STATUS"])
            .contains("state,prepared")
    );
    assert!(
        on_b.call(&["RUVIO", "MIGRATE", "STATUS"])
            .contains("state,prepared")
    );

    // Cancelled on both sides: the source serves the range again, the target drops it.
    assert_eq!(on_a.call(&["RUVIO", "MIGRATE", "CANCEL", "m1"]), "OK");
    assert_eq!(on_b.call(&["RUVIO", "MIGRATE", "CANCEL", "m1"]), "OK");
    wait_for(
        &mut on_b,
        &["RUVIO", "MIGRATE", "STATUS"],
        "state,cancelled",
    );
    assert_eq!(on_a.call(&["GET", &key]), "1");
    assert_eq!(on_a.call(&["SET", &key, "2"]), "OK");
    assert!(on_b.call(&["GET", &key]).starts_with("ERR:MOVED"));
    assert_eq!(on_b.call(&["DBSIZE"]), "0");

    // Nothing comes back after another restart.
    a.restart();
    b.restart();
    let mut on_a = Client::connect(&a.address);
    let mut on_b = Client::connect(&b.address);

    assert_eq!(on_a.call(&["GET", &key]), "2");
    assert_eq!(on_b.call(&["DBSIZE"]), "0");
    assert_eq!(on_b.call(&["RUVIO", "MIGRATE", "STATUS"]), "[state,none]");
}
