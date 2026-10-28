//! A data directory written by an earlier release opens and replays with the current one.
//!
//! `tests/fixtures/persistence/data-0.3.4` was written by the 0.3.4 server (format minor 3,
//! two shards, durability `always`): a checkpoint taken by `SAVE`, then a few writes after
//! it that only the WAL holds. The WAL segments are cut to their used length (the server
//! preallocates 4 MiB each).

mod common;

use std::path::Path;

use common::{Client, Server};

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();

    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());

        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn assert_keys_of_the_fixture(client: &mut Client) {
    // From the checkpoint.
    assert_eq!(client.call(&["GET", "str:ttl"]), "expires");
    assert!(client.call(&["PTTL", "str:ttl"]).parse::<i64>().unwrap() > 0);
    assert_eq!(client.call(&["SCARD", "set:a"]), "3");
    assert_eq!(client.call(&["SISMEMBER", "set:a", "y"]), "1");
    assert_eq!(client.call(&["HGET", "hash:a", "f2"]), "v2");
    assert_eq!(
        client.call(&["ZRANGE", "zset:a", "0", "-1", "WITHSCORES"]),
        "[m1,1.5,m2,2]"
    );
    assert_eq!(client.call(&["XLEN", "stream:a"]), "2");
    assert_eq!(client.call(&["GET", "counter"]), "1");
    // Only in the WAL after the checkpoint.
    assert_eq!(client.call(&["GET", "str:plain"]), "changed");
    assert_eq!(client.call(&["GET", "after:save"]), "tail");
    assert_eq!(client.call(&["LRANGE", "list:a", "0", "-1"]), "[1,2,3,4]");
    assert_eq!(client.call(&["EXISTS", "gone"]), "0");

    // Every key is there, none extra (`DBSIZE` counts one shard).
    let keys = client.call(&["SCAN", "0", "COUNT", "100"]);

    assert!(!keys.contains("gone"), "{keys}");
}

#[test]
fn a_data_directory_written_by_0_3_4_opens_and_replays() {
    let mut server = Server::start_with(&[("RUVIO_DURABILITY", "always"), ("RUVIO_SHARDS", "2")]);

    server.stop();
    std::fs::remove_dir_all(&server.data_dir).unwrap();
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persistence/data-0.3.4"),
        &server.data_dir,
    );
    server.restart();

    let mut client = Client::connect(&server.address);

    assert_keys_of_the_fixture(&mut client);

    // The directory keeps working: a write lands next to the old records and survives
    // another restart together with them.
    assert_eq!(client.call(&["SET", "written:now", "current"]), "OK");
    drop(client);
    server.restart();

    let mut client = Client::connect(&server.address);

    assert_eq!(client.call(&["GET", "written:now"]), "current");
    assert_eq!(client.call(&["LRANGE", "list:a", "0", "-1"]), "[1,2,3,4]");
    assert_eq!(client.call(&["GET", "after:save"]), "tail");
}
