//! Runs against the multi-node cluster `scripts/run_with_servers.sh` starts: node a
//! (`RUVIO_MULTINODE_A_ADDR`, slots 0-8191), node b (`RUVIO_MULTINODE_B_ADDR`, slots
//! 8192-16383), and a node with an older map (`RUVIO_MULTINODE_STALE_ADDR`) that gives every
//! slot to node a. Skipped without them.

use std::net::SocketAddr;

use ruvio_client::{Client, ClientOptions, hash_slot};

fn address(name: &str) -> Option<SocketAddr> {
    std::env::var(name).ok()?.parse().ok()
}

/// A connection to one node, without cluster routing.
fn node(address: SocketAddr) -> Client {
    Client::connect_with(ClientOptions {
        host: address.ip().to_string(),
        port: address.port(),
        discover_cluster: false,
        ..ClientOptions::default()
    })
    .unwrap()
}

#[test]
fn follows_moved_from_a_node_with_an_older_map() {
    let (Some(a), Some(b), Some(stale)) = (
        address("RUVIO_MULTINODE_A_ADDR"),
        address("RUVIO_MULTINODE_B_ADDR"),
        address("RUVIO_MULTINODE_STALE_ADDR"),
    ) else {
        return;
    };
    // Node b's slot (12182) and node a's (5061).
    let (on_b, on_a) = ("{foo}rust:multinode", "{bar}rust:multinode");

    assert_eq!(hash_slot(on_b), 12_182);
    assert_eq!(hash_slot(on_a), 5_061);

    // Seeded with the stale node, the client first sends node b's key to node a, which
    // answers MOVED.
    let mut db = Client::connect(stale.ip().to_string(), stale.port()).unwrap();

    db.set(on_b, "b").unwrap();
    db.set(on_a, "a").unwrap();
    assert_eq!(db.get_string(on_b).unwrap().as_deref(), Some("b"));
    assert_eq!(db.get_string(on_a).unwrap().as_deref(), Some("a"));

    // Each key is on the node that owns it.
    assert_eq!(node(b).get_string(on_b).unwrap().as_deref(), Some("b"));
    assert_eq!(node(a).get_string(on_a).unwrap().as_deref(), Some("a"));
}
