//! Replicas in `CLUSTER SLOTS / NODES`, `ROLE` and `INFO` once they announce where clients
//! reach them.

use super::*;

fn command(address: SocketAddr, args: &[&[u8]]) -> String {
    String::from_utf8(request(address, &resp(&[args]))).unwrap()
}

fn configure(
    config: &mut AppConfig,
    directory: &std::path::Path,
    role: crate::config::ReplicationRole,
    listen: Option<SocketAddr>,
    primary: Option<SocketAddr>,
) {
    config.persistence = PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    };
    config.replication = crate::config::ReplicationConfig {
        role,
        listen,
        primary,
        ..Default::default()
    };
}

/// `*3` node entry of `CLUSTER SLOTS` for a node on loopback, without its id.
fn slot_node(port: u16) -> String {
    format!("*3\r\n$9\r\n127.0.0.1\r\n:{port}\r\n$40\r\n")
}

#[test]
fn replicas_are_listed_after_their_primary_and_name_it_in_their_own_answers() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let listen = ports.address_at(0);
    let (p0, p1, primary_stop, primary_a, primary_b) = serve_two_shards_with(|config| {
        configure(config, primary_dir.path(), Primary, Some(listen), None);
    });
    let (r0, r1, replica_stop, replica_a, replica_b) = serve_two_shards_with(|config| {
        configure(config, replica_dir.path(), Replica, None, Some(listen));
    });

    assert_eq!(command(p0, &[b"SET", b"{a}k", b"v"]), "+OK\r\n");
    wait_until("the replica to copy the key", || {
        command(r0, &[b"GET", b"{a}k"]) == "$1\r\nv\r\n"
    });
    wait_until("both replica shards to be listed", || {
        let slots = command(p0, &[b"CLUSTER", b"SLOTS"]);

        slots.contains(&slot_node(r0.port())) && slots.contains(&slot_node(r1.port()))
    });

    // Each range: the primary shard, then the replica shard that follows it.
    for answering in [p0, p1, r0, r1] {
        let slots = command(answering, &[b"CLUSTER", b"SLOTS"]);
        let first = slots.find(&slot_node(p0.port())).unwrap();
        let second = slots.find(&slot_node(r0.port())).unwrap();

        assert!(slots.starts_with("*2\r\n*4\r\n:0\r\n:8191\r\n"), "{slots}");
        assert!(first < second, "{slots}");
        assert!(slots.contains("*4\r\n:8192\r\n:16383\r\n"), "{slots}");
        assert!(
            slots.find(&slot_node(p1.port())).unwrap() < slots.find(&slot_node(r1.port())).unwrap(),
            "{slots}"
        );
    }

    let nodes = command(r0, &[b"CLUSTER", b"NODES"]);
    let primary_id = command(p0, &[b"CLUSTER", b"MYID"]);
    let primary_id = primary_id.lines().nth(1).unwrap();

    assert!(
        nodes.contains(&format!(
            "127.0.0.1:{}@{} myself,slave {primary_id} ",
            r0.port(),
            u32::from(r0.port()) + 10_000
        )),
        "{nodes}"
    );
    assert_ne!(
        command(r0, &[b"CLUSTER", b"MYID"]),
        command(p0, &[b"CLUSTER", b"MYID"])
    );
    assert!(
        command(p1, &[b"CLUSTER", b"SHARDS"]).contains("$4\r\nrole\r\n$7\r\nreplica\r\n"),
        "replica entry in CLUSTER SHARDS"
    );
    assert!(
        command(p1, &[b"CLUSTER", b"INFO"]).contains("cluster_known_nodes:4\r\n"),
        "known nodes"
    );

    let info = command(p1, &[b"INFO", b"replication"]);

    assert!(
        info.contains(&format!(
            "slave0:ip=127.0.0.1,port={},state=online,offset=",
            r1.port()
        )),
        "{info}"
    );
    assert!(
        command(p0, &[b"ROLE"]).contains(&format!("$9\r\n127.0.0.1\r\n$5\r\n{}\r\n", r0.port())),
        "ROLE lists the replica"
    );
    assert!(command(r0, &[b"READONLY"]).starts_with("+OK"));
    assert!(command(r0, &[b"SET", b"{a}k", b"w"]).starts_with("-READONLY"));

    replica_stop.request();
    join_within(replica_a, PATIENCE).unwrap();
    join_within(replica_b, PATIENCE).unwrap();
    wait_until("the stopped replica to leave the topology", || {
        !command(p0, &[b"CLUSTER", b"SLOTS"]).contains(&slot_node(r0.port()))
    });

    primary_stop.request();
    join_within(primary_a, PATIENCE).unwrap();
    join_within(primary_b, PATIENCE).unwrap();
}

#[test]
fn a_replica_that_is_not_announced_stays_out_of_the_topology() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let listen = ports.address_at(0);
    let (p0, _, primary_stop, primary_a, primary_b) = serve_two_shards_with(|config| {
        configure(config, primary_dir.path(), Primary, Some(listen), None);
    });
    let (r0, _, replica_stop, replica_a, replica_b) = serve_two_shards_with(|config| {
        configure(config, replica_dir.path(), Replica, None, Some(listen));
        config.replication.announced = false;
    });

    assert_eq!(command(p0, &[b"SET", b"{a}k", b"v"]), "+OK\r\n");
    wait_until("the replica to copy the key", || {
        command(r0, &[b"GET", b"{a}k"]) == "$1\r\nv\r\n"
    });
    wait_for_info(p0, "replication_replicas", |count| count == 1);

    assert!(!command(p0, &[b"CLUSTER", b"SLOTS"]).contains(&slot_node(r0.port())));
    assert!(!command(p0, &[b"INFO", b"replication"]).contains("slave0:"));
    // The replica still names its primary: that address came from the primary.
    assert!(command(r0, &[b"CLUSTER", b"SLOTS"]).contains(&slot_node(p0.port())));

    replica_stop.request();
    join_within(replica_a, PATIENCE).unwrap();
    join_within(replica_b, PATIENCE).unwrap();
    primary_stop.request();
    join_within(primary_a, PATIENCE).unwrap();
    join_within(primary_b, PATIENCE).unwrap();
}
