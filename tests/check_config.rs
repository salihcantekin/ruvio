//! `ruvio --check-config` and `ruvio coordinator --check`: they check settings the way a
//! start does, then exit without opening a port or writing a file.

use std::{
    path::Path,
    process::{Command, Output},
};

fn ruvio(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));

    command.args(args).current_dir(dir).env_clear();
    // A file the check is not pointed at must not be found beside the binary.
    command.env("RUVIO_CONFIG", dir.join("absent.toml"));
    for (key, value) in env {
        command.env(key, value);
    }

    command.output().expect("run ruvio")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn check_config_accepts_good_settings_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let output = ruvio(
        dir.path(),
        &["--check-config"],
        &[
            ("RUVIO_SHARDS", "3"),
            ("RUVIO_DURABILITY", "everysec"),
            ("RUVIO_DATA_DIR", dir.path().join("data").to_str().unwrap()),
        ],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(
        stdout(&output).starts_with("config ok: 3 shard(s) on 127.0.0.1:6379, durability everysec"),
        "{}",
        stdout(&output)
    );
    // Only the data directory, created to prove it is writable, and left empty.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();

    assert_eq!(entries, ["data"]);
    assert_eq!(
        std::fs::read_dir(dir.path().join("data")).unwrap().count(),
        0
    );
}

#[test]
fn check_config_refuses_bad_settings_with_the_reason() {
    let dir = tempfile::tempdir().unwrap();
    let output = ruvio(
        dir.path(),
        &["--check-config"],
        &[("RUVIO_DURABILITY", "weekly")],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("durability must be"),
        "{}",
        stderr(&output)
    );

    let file = dir.path().join("ruvio.toml");

    std::fs::write(&file, "shards = 2\nnot_a_setting = 1\n").unwrap();
    let output = ruvio(
        dir.path(),
        &["--check-config", "--config", file.to_str().unwrap()],
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("not_a_setting"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn check_config_needs_an_explicit_file_to_exist() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.toml");
    let output = ruvio(
        dir.path(),
        &["--check-config", "--config", missing.to_str().unwrap()],
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(!missing.exists());
}

/// The map `coordinator --check` prints is one every data node of the cluster accepts, and
/// a node whose shard count differs from its group's is refused.
#[test]
fn coordinator_check_prints_the_map_the_nodes_accept() {
    let dir = tempfile::tempdir().unwrap();
    let coordinator = dir.path().join("coordinator.toml");

    std::fs::write(
        &coordinator,
        r#"id = 1
listen = "127.0.0.1:26400"
data_dir = "/tmp/unused"
peers = ["1@127.0.0.1:26400"]

[[groups]]
name = "a"
shards = 2
slots = ["0-8191"]
members = [
  { address = "10.0.0.1:6379", replication = "10.0.0.1:26379" },
  { address = "10.0.0.3:6379", replication = "10.0.0.3:26379" },
]

[[groups]]
name = "b"
shards = 2
slots = ["8192-16383"]
members = [
  { address = "10.0.0.2:6379", replication = "10.0.0.2:26379" },
  { address = "10.0.0.4:6379", replication = "10.0.0.4:26379" },
]
"#,
    )
    .unwrap();
    let output = ruvio(
        dir.path(),
        &[
            "coordinator",
            "--config",
            coordinator.to_str().unwrap(),
            "--check",
        ],
        &[],
    );

    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    let (first, map) = text.split_once('\n').unwrap();

    assert_eq!(first, "coordinator config ok: id 1, 1 peer(s), 2 group(s)");
    assert!(map.starts_with("version = 1\n"), "{map}");
    assert!(map.contains("address = \"10.0.0.2:6379\""), "{map}");

    let topology = dir.path().join("topology.toml");

    std::fs::write(&topology, map).unwrap();
    let node = |shards: &str| {
        ruvio(
            dir.path(),
            &["--check-config"],
            &[
                ("RUVIO_SHARDS", shards),
                ("RUVIO_DURABILITY", "everysec"),
                ("RUVIO_DATA_DIR", dir.path().join("data").to_str().unwrap()),
                ("RUVIO_CLUSTER_TOPOLOGY", topology.to_str().unwrap()),
                ("RUVIO_CLUSTER_GROUP", "b"),
            ],
        )
    };
    let output = node("2");

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(
        stdout(&output).contains("cluster group b"),
        "{}",
        stdout(&output)
    );
    assert_eq!(node("3").status.code(), Some(1));
}

#[test]
fn coordinator_check_refuses_a_public_listener_without_a_secret() {
    let dir = tempfile::tempdir().unwrap();
    let coordinator = dir.path().join("coordinator.toml");

    std::fs::write(
        &coordinator,
        r#"id = 1
listen = "0.0.0.0:26400"
data_dir = "/tmp/unused"
peers = ["1@10.0.0.1:26400"]

[[groups]]
name = "main"
members = [
  { address = "10.0.0.1:6379", replication = "10.0.0.1:26379" },
  { address = "10.0.0.2:6379", replication = "10.0.0.2:26379" },
]
"#,
    )
    .unwrap();
    let output = ruvio(
        dir.path(),
        &[
            "coordinator",
            "--config",
            coordinator.to_str().unwrap(),
            "--check",
        ],
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("without a secret"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn print_config_schema_lists_every_setting_as_json() {
    let dir = tempfile::tempdir().unwrap();
    let output = ruvio(dir.path(), &["--print-config-schema"], &[]);

    assert!(output.status.success());
    let text = stdout(&output);

    assert!(text.starts_with("{\n  \"settings\": ["), "{text}");
    assert!(text.contains("\"env\": \"RUVIO_DURABILITY\""));
    assert!(text.contains("\"file\": \"persistence.profile\""));
}
