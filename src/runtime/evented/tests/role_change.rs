//! `REPLICAOF`, `ROLE` and fencing on running servers.

use super::*;

fn persistence(directory: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig {
        profile: crate::DurabilityProfile::Always,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    }
}

fn replication(
    role: crate::config::ReplicationRole,
    listen: Option<SocketAddr>,
    primary: Option<SocketAddr>,
) -> crate::config::ReplicationConfig {
    crate::config::ReplicationConfig {
        role,
        listen,
        primary,
        ..Default::default()
    }
}

fn command(address: SocketAddr, args: &[&[u8]]) -> Vec<u8> {
    request(address, &resp(&[args]))
}

#[track_caller]
fn wait_for_reply(address: SocketAddr, args: &[&[u8]], expected: &[u8]) {
    wait_until(
        &format!(
            "{:?} to answer {:?}",
            args,
            String::from_utf8_lossy(expected)
        ),
        || command(address, args) == expected,
    );
}

/// A replica promoted with `REPLICAOF NO ONE` takes writes at a new epoch, its sibling follows
/// it with `REPLICAOF host port`, and the old primary, back with a write past the fork, is
/// demoted and loses that write.
#[test]
fn replicaof_promotes_repoints_and_demotes_without_a_restart() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let promoted_dir = tempfile::tempdir().unwrap();
    let sibling_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let (old, new) = (ports.address_at(0), ports.address_at(1));

    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = replication(Primary, Some(old), None);
    });
    let (promoted_address, promoted_stop, promoted) = serve_with(|config| {
        config.persistence = persistence(promoted_dir.path());
        config.replication = replication(Replica, Some(new), Some(old));
    });
    let (sibling_address, sibling_stop, sibling) = serve_with(|config| {
        config.persistence = persistence(sibling_dir.path());
        config.replication = replication(Replica, None, Some(old));
    });

    assert_eq!(command(primary_address, &[b"SET", b"k", b"v"]), b"+OK\r\n");
    wait_for_reply(promoted_address, &[b"GET", b"k"], b"$1\r\nv\r\n");
    wait_for_reply(sibling_address, &[b"GET", b"k"], b"$1\r\nv\r\n");
    // Both replicas announce their client address and are listed once they acknowledge.
    let listed = |port: u16| {
        format!(
            "*3\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n",
            port.to_string().len()
        )
    };
    let deadline = std::time::Instant::now() + PATIENCE;
    let role = loop {
        let role = String::from_utf8(command(primary_address, &[b"ROLE"])).unwrap();

        if role.contains("*2\r\n*3") || std::time::Instant::now() >= deadline {
            break role;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };

    assert!(
        role.starts_with("*3\r\n$6\r\nmaster\r\n:1\r\n*2\r\n"),
        "{role}"
    );
    assert!(role.contains(&listed(promoted_address.port())), "{role}");
    assert!(role.contains(&listed(sibling_address.port())), "{role}");
    assert!(command(promoted_address, &[b"ROLE"]).starts_with(b"*5\r\n$5\r\nslave\r\n"));
    assert!(command(promoted_address, &[b"SET", b"x", b"1"]).starts_with(b"-READONLY"));

    primary_stop.request();
    join_within(primary, PATIENCE).unwrap();

    assert_eq!(
        command(promoted_address, &[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n"
    );
    assert_eq!(command(promoted_address, &[b"SET", b"x", b"1"]), b"+OK\r\n");
    assert_eq!(info_field(promoted_address, "replication_epoch"), Some(1));
    assert!(command(promoted_address, &[b"ROLE"]).starts_with(b"*3\r\n$6\r\nmaster\r\n"));

    let port = new.port().to_string();

    assert_eq!(
        command(
            sibling_address,
            &[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]
        ),
        b"+OK\r\n"
    );
    wait_for_reply(sibling_address, &[b"GET", b"x"], b"$1\r\n1\r\n");
    assert_eq!(command(sibling_address, &[b"GET", b"k"]), b"$1\r\nv\r\n");

    // The old primary comes back as a writer and takes a write the new one never saw.
    let (back_address, back_stop, back) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = replication(Primary, Some(old), None);
    });

    assert_eq!(command(back_address, &[b"SET", b"late", b"1"]), b"+OK\r\n");
    assert_eq!(
        command(back_address, &[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(command(back_address, &[b"SET", b"y", b"1"]).starts_with(b"-READONLY"));
    // Past the fork, it takes a full copy: the late write is gone, the new primary's is in.
    wait_for_reply(back_address, &[b"GET", b"x"], b"$1\r\n1\r\n");
    assert_eq!(command(back_address, &[b"EXISTS", b"late"]), b":0\r\n");

    for (stop, server) in [
        (back_stop, back),
        (sibling_stop, sibling),
        (promoted_stop, promoted),
    ] {
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// An old primary that a replica of its successor connects to stops taking writes.
#[test]
fn a_replica_of_the_new_primary_fences_the_old_one() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let (old, new) = (ports.address_at(0), ports.address_at(1));

    let (primary_address, primary_stop, primary) = serve_with(|config| {
        config.persistence = persistence(primary_dir.path());
        config.replication = replication(Primary, Some(old), None);
    });
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = replication(Replica, Some(new), Some(old));
    });

    assert_eq!(command(primary_address, &[b"SET", b"k", b"v"]), b"+OK\r\n");
    wait_for_reply(replica_address, &[b"GET", b"k"], b"$1\r\nv\r\n");
    // The replica is promoted (epoch 1) and then follows the old primary again: the old
    // primary sees the newer epoch, refuses the replica and stops taking writes.
    assert_eq!(
        command(replica_address, &[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n"
    );
    let port = old.port().to_string();

    assert_eq!(
        command(
            replica_address,
            &[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]
        ),
        b"+OK\r\n"
    );
    wait_for_info(primary_address, "replication_fenced", |fenced| fenced == 1);
    assert!(
        command(primary_address, &[b"SET", b"k", b"w"])
            .starts_with(b"-READONLY This primary was replaced")
    );
    assert_eq!(command(primary_address, &[b"GET", b"k"]), b"$1\r\nv\r\n");

    for (stop, server) in [(replica_stop, replica), (primary_stop, primary)] {
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// The first primary, restarted with its primary configuration after it followed the promoted
/// replica, forks a newer epoch than the promoted one. Pointed back with `REPLICAOF ... YIELD`
/// (what the coordinators send) it follows the promoted primary instead of fencing it.
#[test]
fn a_restarted_primary_yields_to_the_primary_it_is_pointed_at() {
    use crate::config::ReplicationRole::{Primary, Replica};

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let (old, new) = (ports.address_at(0), ports.address_at(1));
    let port = new.port().to_string();
    let as_configured = |config: &mut AppConfig| {
        config.persistence = persistence(primary_dir.path());
        config.replication = replication(Primary, Some(old), None);
    };

    let (primary_address, primary_stop, primary) = serve_with(as_configured);
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication = replication(Replica, Some(new), Some(old));
    });

    assert_eq!(command(primary_address, &[b"SET", b"k", b"v"]), b"+OK\r\n");
    wait_for_reply(replica_address, &[b"GET", b"k"], b"$1\r\nv\r\n");
    primary_stop.request();
    join_within(primary, PATIENCE).unwrap();
    assert_eq!(
        command(replica_address, &[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n"
    );
    assert_eq!(info_field(replica_address, "replication_epoch"), Some(1));

    // First restart: the old primary writes its own root history and follows the new one.
    let (back_address, back_stop, back) = serve_with(as_configured);
    assert_eq!(
        command(
            back_address,
            &[b"REPLICAOF", b"127.0.0.1", port.as_bytes(), b"YIELD"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(command(replica_address, &[b"SET", b"a", b"1"]), b"+OK\r\n");
    wait_for_reply(back_address, &[b"GET", b"a"], b"$1\r\n1\r\n");
    back_stop.request();
    join_within(back, PATIENCE).unwrap();

    // Second restart: its last role was replica, so it forks one epoch above the new primary
    // and takes a write the new primary never sees.
    let (back_address, back_stop, back) = serve_with(as_configured);
    assert_eq!(info_field(back_address, "replication_epoch"), Some(2));
    assert_eq!(command(back_address, &[b"SET", b"late", b"1"]), b"+OK\r\n");
    assert_eq!(
        command(
            back_address,
            &[b"REPLICAOF", b"127.0.0.1", port.as_bytes(), b"YIELD"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(command(replica_address, &[b"SET", b"b", b"1"]), b"+OK\r\n");
    wait_for_reply(back_address, &[b"GET", b"b"], b"$1\r\n1\r\n");
    assert_eq!(command(back_address, &[b"EXISTS", b"late"]), b":0\r\n");
    assert_eq!(info_field(replica_address, "replication_fenced"), Some(0));
    assert_eq!(command(replica_address, &[b"SET", b"c", b"1"]), b"+OK\r\n");

    for (stop, server) in [(back_stop, back), (replica_stop, replica)] {
        stop.request();
        join_within(server, PATIENCE).unwrap();
    }
}

/// A replica that has not finished copying its primary answers `LOADING` to every command,
/// `REPLICAOF` included: it is promoted with a restart and `--force-promote-incomplete`.
/// (`REPLICAOF NO ONE` sent to a complete shard of a process with an incomplete one needs
/// `FORCE`; its check reads every shard's marker.)
#[test]
fn an_incomplete_replica_answers_loading_to_replicaof() {
    let replica_dir = tempfile::tempdir().unwrap();
    let ports = crate::test_ports::reserve();
    let nobody = ports.address_at(0);

    crate::runtime::replica_guard::write(
        replica_dir.path(),
        0,
        crate::runtime::replica_guard::CatchupMarker::Preparing,
    )
    .unwrap();
    let (replica_address, replica_stop, replica) = serve_with(|config| {
        config.persistence = persistence(replica_dir.path());
        config.replication =
            replication(crate::config::ReplicationRole::Replica, None, Some(nobody));
    });

    assert!(
        command(replica_address, &[b"REPLICAOF", b"NO", b"ONE", b"FORCE"]).starts_with(b"-LOADING")
    );
    assert!(command(replica_address, &[b"SET", b"k", b"v"]).starts_with(b"-LOADING"));

    replica_stop.request();
    join_within(replica, PATIENCE).unwrap();
}
