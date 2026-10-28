//! `ruvio-ctl ping`, the container health check: success against a live server, failure once
//! nothing listens.

mod common;

use std::process::Command;

use common::Server;

fn ping(address: &str) -> bool {
    Command::new(env!("CARGO_BIN_EXE_ruvio-ctl"))
        .args(["ping", "--address", address])
        .status()
        .unwrap()
        .success()
}

#[test]
fn ping_succeeds_against_a_server_and_fails_without_one() {
    let server = Server::start();
    let address = server.address.clone();

    assert!(ping(&address));
    drop(server);
    assert!(!ping(&address));
}
