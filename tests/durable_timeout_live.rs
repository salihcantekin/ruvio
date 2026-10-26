//! `CLIENT DURABILITY` after a timeout: replies keep waiting for the replicas, so a connection
//! never shows a write no replica holds.

mod common;

use common::{Client, Server};

const REFUSED: &str = "ERR:NOREPLICAS Not enough good replicas to write.";

#[test]
fn replies_after_a_timeout_keep_waiting_until_a_replica_holds_the_write() {
    // A primary with no replica at all: every wait times out.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = listener.local_addr().unwrap().to_string();

    drop(listener);

    let server = Server::start_with(&[
        ("RUVIO_REPLICATION_ROLE", "primary"),
        ("RUVIO_REPLICATION_LISTEN", &listen),
        ("RUVIO_DURABILITY", "everysec"),
    ]);
    let mut writer = Client::connect(&server.address);
    let mut reader = Client::connect(&server.address);
    let mut plain = Client::connect(&server.address);

    for client in [&mut writer, &mut reader] {
        assert_eq!(
            client.call(&["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "200"]),
            "OK"
        );
    }

    assert_eq!(writer.call(&["SET", "k", "1"]), REFUSED);

    // The write stays applied and a connection without durability sees it.
    assert_eq!(plain.call(&["GET", "k"]), "1");

    // The durable reader never gets it, however often it asks.
    for _ in 0..3 {
        assert_eq!(reader.call(&["GET", "k"]), REFUSED);
    }

    // So does the writer's next command, with no write in between.
    assert_eq!(writer.call(&["GET", "k"]), REFUSED);
}
