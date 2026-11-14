use super::*;

fn persistence(directory: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig {
        profile: crate::DurabilityProfile::Everysec,
        data_dir: directory.to_path_buf(),
        ..PersistenceConfig::default()
    }
}

#[test]
fn shutdown_stops_a_saving_server_and_keeps_its_data() {
    let directory = tempfile::tempdir().unwrap();
    let start =
        |persistence: PersistenceConfig| serve_with(move |config| config.persistence = persistence);
    let (address, _stop, server) = start(persistence(directory.path()));

    assert_eq!(
        request(
            address,
            &resp(&[&[b"SET", b"kept", b"value"], &[b"SHUTDOWN"]])
        ),
        b"+OK\r\n+OK\r\n"
    );
    // The command alone stops the server; nothing requests the token from outside.
    join_within(server, PATIENCE).unwrap();

    let (address, stop, server) = start(persistence(directory.path()));

    assert_eq!(
        request(address, &resp(&[&[b"GET", b"kept"]])),
        b"$5\r\nvalue\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn shutdown_refuses_to_drop_data_without_nosave_under_memory() {
    let (address, stop, server) = serve_on_ephemeral();
    let refused = request(
        address,
        &resp(&[
            &[b"SET", b"k", b"v"],
            &[b"SHUTDOWN"],
            &[b"SHUTDOWN", b"SAVE"],
            &[b"GET", b"k"],
        ]),
    );
    let text = String::from_utf8_lossy(&refused);

    assert!(
        text.starts_with("+OK\r\n-ERR durability is memory"),
        "{text}"
    );
    assert!(
        text.contains("-ERR SHUTDOWN SAVE needs a durability profile that saves"),
        "{text}"
    );
    assert!(text.ends_with("$1\r\nv\r\n"), "{text}");

    assert_eq!(
        request(address, &resp(&[&[b"SHUTDOWN", b"nosave"]])),
        b"+OK\r\n"
    );
    join_within(server, PATIENCE).unwrap();
    drop(stop);
}

#[test]
fn shutdown_nosave_is_refused_when_the_server_saves() {
    let directory = tempfile::tempdir().unwrap();
    let persistence = persistence(directory.path());
    let (address, stop, server) = serve_with(move |config| config.persistence = persistence);
    let reply = request(address, &resp(&[&[b"SHUTDOWN", b"NOSAVE"]]));
    let text = String::from_utf8_lossy(&reply);

    assert!(
        text.starts_with("-ERR SHUTDOWN NOSAVE is for durability memory"),
        "{text}"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}
