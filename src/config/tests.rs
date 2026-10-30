/// Every key the config file accepts, as `key` or `section.key`. Read from serde's
/// unknown-field error, so the list follows the structs without a hand-kept copy.
fn file_keys() -> Vec<String> {
    fn accepted(document: &str) -> Option<Vec<String>> {
        let error = toml::from_str::<super::FileConfig>(document)
            .err()?
            .to_string();
        let (_, expected) = error
            .split_once("unknown field")?
            .1
            .split_once("expected")?;

        Some(
            expected
                .split('`')
                .skip(1)
                .step_by(2)
                .map(str::to_owned)
                .collect(),
        )
    }

    let mut keys = Vec::new();

    for key in accepted("not_a_setting = 1").expect("top-level keys") {
        match accepted(&format!("[{key}]\nnot_a_setting = 1")) {
            Some(nested) => keys.extend(nested.into_iter().map(|inner| format!("{key}.{inner}"))),
            None => keys.push(key),
        }
    }

    keys
}

/// Environment variable names read by `apply_environment`.
fn environment_names() -> Vec<String> {
    // Line endings as committed, even in a checkout that turned them into CRLF.
    let source = include_str!("../config.rs").replace("\r\n", "\n");
    let start = source
        .find("pub fn apply_environment")
        .expect("apply_environment");
    let body = &source[start..];
    let body = &body[..body
        .find("\n        Ok(())\n    }")
        .expect("end of apply_environment")];
    let mut names: Vec<String> = body
        .split('"')
        .filter(|token| {
            token.starts_with("RUVIO_")
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
        .map(str::to_owned)
        .collect();

    names.sort();
    names.dedup();
    names
}

/// A setting the file accepts but `docs/CONFIGURATION.md` never names is a setting
/// nobody can find. A key counts as documented as a `key =` line (commented or not) in
/// the sample file, or written in backticks as `key` or `section.key`.
#[test]
fn every_file_setting_is_documented() {
    let docs = include_str!("../../docs/CONFIGURATION.md");
    let keys = file_keys();
    assert!(keys.len() > 50, "found only {} keys", keys.len());

    let missing: Vec<&String> = keys
        .iter()
        .filter(|path| {
            let key = path.rsplit('.').next().unwrap_or(path);

            !(docs.contains(&format!("`{path}`"))
                || docs.contains(&format!("`{key}`"))
                || docs.contains(&format!("[[{path}]]"))
                || docs.lines().any(|line| {
                    let line = line.trim_start().trim_start_matches('#').trim_start();
                    line.strip_prefix(key)
                        .is_some_and(|rest| rest.trim_start().starts_with('='))
                }))
        })
        .collect();

    assert!(
        missing.is_empty(),
        "settings missing from docs/CONFIGURATION.md: {missing:?}"
    );
}

/// Every `RUVIO_*` variable `apply_environment` reads has a row in the environment table
/// of `docs/CONFIGURATION.md`.
#[test]
fn every_environment_variable_is_documented() {
    let docs = include_str!("../../docs/CONFIGURATION.md");
    let names = environment_names();
    assert!(names.len() > 50, "found only {} names", names.len());

    let missing: Vec<&String> = names
        .iter()
        .filter(|name| !docs.contains(&format!("| `{name}` |")))
        .collect();

    assert!(
        missing.is_empty(),
        "variables missing from docs/CONFIGURATION.md: {missing:?}"
    );
}

#[test]
fn a_script_time_limit_too_large_to_add_to_the_clock_is_refused() {
    let limits = super::ScriptLimits {
        max_time_ms: usize::MAX,
        ..super::ScriptLimits::default()
    };

    assert!(limits.validate().is_err());
    assert!(super::ScriptLimits::default().validate().is_ok());
}

#[test]
fn default_config_file_holds_the_code_defaults() {
    let mut parsed = super::AppConfig::default();

    parsed.apply_toml(super::DEFAULT_CONFIG).unwrap();
    assert_eq!(parsed, super::AppConfig::default());
}

#[test]
#[cfg_attr(miri, ignore = "writes the config file, which fsyncs")]
fn write_config_refuses_a_file_that_does_not_parse_and_writes_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ruvio.toml");
    let loaded = super::AppConfig::default();
    let changed = super::AppConfig {
        shards: 3,
        ..super::AppConfig::default()
    };
    let broken = "address = \"127.0.0.1:7000\"\n[persistence\nprofile = \"always\"\n";

    std::fs::write(&path, broken).unwrap();
    assert!(
        changed
            .write_overrides(&path, &loaded)
            .unwrap_err()
            .contains("does not parse")
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), broken);

    // A missing file is created; a valid one keeps its other settings.
    std::fs::remove_file(&path).unwrap();
    changed.write_overrides(&path, &loaded).unwrap();
    std::fs::write(&path, "address = \"127.0.0.1:7000\"\n").unwrap();
    changed.write_overrides(&path, &loaded).unwrap();
    let mut restored = super::AppConfig::default();

    restored
        .apply_toml(&std::fs::read_to_string(&path).unwrap())
        .unwrap();
    assert_eq!(restored.address, "127.0.0.1:7000");
    assert_eq!(restored.shards, 3);
    let names: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();

    assert_eq!(names, ["ruvio.toml"]);
}

#[test]
#[cfg_attr(miri, ignore = "writes the config file, which fsyncs")]
fn sync_worker_modes_resolve_and_round_trip() {
    use crate::persistence::SyncWorkerMode;
    let mut config = super::AppConfig::default();
    assert_eq!(config.persistence.sync_worker, SyncWorkerMode::Auto);
    config
        .apply_toml("[persistence]\nprofile = \"always\"\n")
        .unwrap();
    config.resolve_sync_worker();
    assert_eq!(
        config.persistence.sync_worker_active,
        cfg!(target_os = "linux")
    );
    // Legacy key keeps working and maps onto the mode.
    config
        .apply_toml("[persistence]\nexperimental_sync_worker = true\n")
        .unwrap();
    assert_eq!(config.persistence.sync_worker, SyncWorkerMode::On);
    config.resolve_sync_worker();
    config.validate().unwrap();
    assert!(config.persistence.sync_worker_active);
    config
        .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "off".into()))
        .unwrap();
    config.resolve_sync_worker();
    assert!(!config.persistence.sync_worker_active);
    assert!(
        config
            .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "maybe".into()))
            .is_err()
    );
    config
        .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "on".into()))
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ruvio.toml");
    let loaded = super::AppConfig::default();
    std::fs::write(&path, "[persistence]\nprofile = \"memory\"\n").unwrap();
    config.write_overrides(&path, &loaded).unwrap();
    let mut restored = super::AppConfig::default();
    restored
        .apply_toml(&std::fs::read_to_string(&path).unwrap())
        .unwrap();
    assert_eq!(restored.persistence.sync_worker, SyncWorkerMode::On);
    // `on` outside the supported envelope is an error; `auto` just stays off.
    config.persistence.profile = crate::DurabilityProfile::Memory;
    assert!(config.validate().unwrap_err().contains("always durability"));
    config.persistence.profile = crate::DurabilityProfile::Always;
    config.hot_keys.push("cache:*".into());
    assert!(config.validate().unwrap_err().contains("hot_keys"));
    config.persistence.sync_worker = SyncWorkerMode::Auto;
    config.resolve_sync_worker();
    assert!(!config.persistence.sync_worker_active);
    config.validate().unwrap();
    // `auto` also stays off with more than one shard (not yet measured).
    config.hot_keys.clear();
    config.shards = 2;
    config.resolve_sync_worker();
    assert!(!config.persistence.sync_worker_active);
}
use super::*;

#[test]
#[cfg_attr(miri, ignore = "writes the config file, which fsyncs")]
fn environment_override_is_written_into_the_toml_and_users_stay() {
    let path = env::temp_dir().join(format!(
        "ruvio-config-{}-{}.toml",
        std::process::id(),
        "users"
    ));

    fs::write(
            &path,
            "shards = 1\naddress = \"127.0.0.1:6379\"\n\n[[security.users]]\nname = \"app\"\nrules = \"on >secret ~app:* +ping\"\n",
        )
        .unwrap();

    let mut loaded = AppConfig::default();

    loaded
        .apply_toml(&fs::read_to_string(&path).unwrap())
        .unwrap();

    let mut effective = loaded.clone();

    effective.shards = 2;
    effective.address = "0.0.0.0:6380".into();
    effective.write_overrides(&path, &loaded).unwrap();

    let text = fs::read_to_string(&path).unwrap();

    assert!(text.contains("shards = 2"), "{text}");
    assert!(text.contains("0.0.0.0:6380"), "{text}");
    assert!(text.contains("name = \"app\""), "{text}");
    assert!(text.contains("does not change the live server"), "{text}");

    let mut again = AppConfig::default();

    again.apply_toml(&text).unwrap();
    assert_eq!(again.shards, 2);
    assert_eq!(again.address, "0.0.0.0:6380");
    assert_eq!(again.security.users.len(), 1);

    let stable = fs::read_to_string(&path).unwrap();

    again.write_overrides(&path, &again).unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), stable);
    let _ = fs::remove_file(&path);
}

#[test]
fn rejects_disabled_timeouts() {
    let timeouts = ConnectionTimeouts {
        read: Duration::ZERO,
        ..ConnectionTimeouts::default()
    };

    assert_eq!(
        timeouts.validate(),
        Err("read timeout must be greater than zero")
    );
}

#[test]
fn clamps_nested_connection_limits_to_the_global_cap() {
    let mut limits = ConnectionLimits {
        max_global: 10,
        max_per_ip: 11,
        max_per_user: 40,
        ..ConnectionLimits::default()
    };

    let notes = limits.clamp_to_global();

    assert_eq!(limits.max_per_ip, 10);
    assert_eq!(limits.max_per_user, 10);
    assert_eq!(notes.len(), 2);
    assert!(limits.validate().is_ok());
    assert!(
        ConnectionLimits {
            max_global: 0,
            ..ConnectionLimits::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn rejects_disabled_request_limits() {
    assert_eq!(
        RequestLimits {
            max_queued_requests: 0,
            ..RequestLimits::default()
        }
        .validate(),
        Err("request queue limit must be greater than zero")
    );
}

#[test]
fn advertised_port_comes_from_the_environment_and_rejects_zero() {
    let mut config = AppConfig::default();

    config
        .apply_environment(|name| (name == "RUVIO_ADVERTISE_PORT").then(|| "17000".into()))
        .unwrap();
    assert_eq!(config.advertised_port, Some(17000));
    assert!(
        config
            .introspect()
            .contains(&("advertised-port".into(), "17000".into()))
    );
    assert!(
        AppConfig::default()
            .apply_environment(|name| (name == "RUVIO_ADVERTISE_PORT").then(|| "0".into()))
            .is_err()
    );

    let mut file = AppConfig::default();

    file.apply_toml("advertised_port = 7100\n").unwrap();
    assert_eq!(file.advertised_port, Some(7100));
}

#[test]
fn merges_file_environment_and_cli_in_precedence_order() {
    let mut config = AppConfig::default();

    config
        .apply_toml(
            r#"
                address = "127.0.0.1:7000"

                [requests]
                max_queued_requests = 10
                "#,
        )
        .unwrap();
    config
        .apply_environment(|name| match name {
            "RUVIO_ADDR" => Some("127.0.0.1:7001".into()),
            "RUVIO_MAX_QUEUED_REQUESTS" => Some("11".into()),
            _ => None,
        })
        .unwrap();
    config.apply_overrides(ConfigOverrides {
        address: Some("127.0.0.1:7002".into()),
        ..ConfigOverrides::default()
    });

    assert_eq!(config.address, "127.0.0.1:7002");
    assert_eq!(config.requests.max_queued_requests, 11);
    config.validate().unwrap();
}

#[test]
fn rejects_unknown_file_fields_and_overlapping_metrics() {
    let mut config = AppConfig::default();

    assert!(config.apply_toml("unknown = true").is_err());
    assert!(config.apply_toml("backend = \"evented\"\n").is_err());

    config.metrics_address = Some("127.0.0.1:6379".into());
    assert_eq!(
        config.validate(),
        Err("metrics port range overlaps the RESP listener range".into())
    );
    config.metrics_address = Some("127.0.0.1:9121".into());
    config.shards = 2;
    config.validate().unwrap();
}

#[test]
fn consecutive_shard_ports_are_accepted() {
    let config = AppConfig {
        shards: 2,
        advertised_host: "127.0.0.1".into(),
        ..AppConfig::default()
    };

    config.validate().unwrap();
}

#[test]
fn config_get_redacts_requirepass_and_matches_globs() {
    let mut config = AppConfig::default();

    config.security.requirepass = "super-secret-password".into();
    config.protocol.max_arguments = 32;

    let requirepass = config.config_get(b"requirepass");

    assert_eq!(requirepass, vec![("requirepass".into(), "***".into())]);
    let dump = config
        .config_get(b"*")
        .into_iter()
        .flat_map(|(name, value)| [name, value])
        .collect::<Vec<_>>()
        .join("\n");

    assert!(!dump.contains("super-secret-password"));
    assert!(dump.contains("requirepass\n***"));

    let matched: Vec<_> = config
        .config_get(b"max-arg*")
        .into_iter()
        .map(|(name, _)| name)
        .collect();

    assert_eq!(matched, ["max-arguments"]);
    assert!(config.config_get(b"unknown-setting").is_empty());
    assert_eq!(
        config.config_get(b"SAVE"),
        vec![("save".into(), String::new())]
    );
    assert_eq!(
        config.config_get(b"maxmemory-policy"),
        vec![("maxmemory-policy".into(), "noeviction".into())]
    );
    assert_eq!(
        config.config_get(b"notify-keyspace-events"),
        vec![("notify-keyspace-events".into(), String::new())]
    );
}

#[test]
fn notify_keyspace_events_parse_from_file_and_environment() {
    let mut config = AppConfig::default();

    assert_eq!(config.memory.notify_mask, 0);
    config
        .apply_toml("notify_keyspace_events = \"K$\"\n")
        .unwrap();
    assert_eq!(config.notify_keyspace_events, "K$");
    assert_ne!(config.memory.notify_mask, 0);
    config
        .apply_environment(|name| {
            (name == "RUVIO_NOTIFY_KEYSPACE_EVENTS").then(|| "Eg".to_string())
        })
        .unwrap();
    assert_eq!(config.notify_keyspace_events, "Eg");
    assert!(
        config
            .apply_toml("notify_keyspace_events = \"nope\"\n")
            .is_err()
    );
    assert!(
        config
            .apply_toml("notify_keyspace_events = \"A\"\n")
            .is_ok()
    );
    assert_eq!(config.memory.notify_mask, 0);
}

#[test]
fn parses_connection_quotas_from_file_and_environment() {
    let mut config = AppConfig::default();

    config
        .apply_toml(
            r#"
                [connections]
                max_per_user = 3
                max_commands_per_second = 40
                "#,
        )
        .unwrap();
    assert_eq!(config.connections.max_per_user, 3);
    assert_eq!(config.connections.max_commands_per_second, 40);
    config
        .apply_environment(|name| match name {
            "RUVIO_MAX_CONNECTIONS_PER_USER" => Some("2".into()),
            "RUVIO_MAX_COMMANDS_PER_SECOND" => Some("10".into()),
            _ => None,
        })
        .unwrap();
    assert_eq!(config.connections.max_per_user, 2);
    assert_eq!(config.connections.max_commands_per_second, 10);
    config.connections.max_per_user = 20_000;
    assert_eq!(config.connections.clamp_to_global().len(), 1);
    assert_eq!(
        config.connections.max_per_user,
        config.connections.max_global
    );
}

#[test]
fn parses_acl_users_from_file_and_rejects_unknown_rules() {
    let mut config = AppConfig::default();

    config
        .apply_toml(
            r#"
                [security]
                requirepass = "secret"
                [[security.users]]
                name = "app"
                rules = "on >app-secret ~app:* +get +ping +auth"
                "#,
        )
        .unwrap();
    assert_eq!(config.security.users.len(), 1);
    assert!(config.security.users[0].rules.allows_command("get"));
    assert!(!config.security.users[0].rules.allows_command("set"));
    assert_eq!(
        config.config_get(b"acl-users"),
        vec![("acl-users".into(), "default app".into())]
    );
    assert!(
        AppConfig::default()
            .apply_toml(
                r#"
                    [[security.users]]
                    name = "bad"
                    rules = "on +@not-a-category"
                    "#,
            )
            .is_err()
    );
}

#[test]
fn accepts_paired_tls_paths() {
    let mut config = AppConfig::default();

    config
        .apply_toml(
            r#"
                [security]
                tls_cert = "/tmp/cert.pem"
                tls_key = "/tmp/key.pem"
                "#,
        )
        .unwrap();
    assert!(!config.security.tls_cert.as_os_str().is_empty());
    config
        .apply_environment(|name| match name {
            "RUVIO_TLS_CERT" => Some("/env/cert.pem".into()),
            "RUVIO_TLS_KEY" => Some("/env/key.pem".into()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        config.security.tls_cert,
        std::path::PathBuf::from("/env/cert.pem")
    );
    assert_eq!(
        config.config_get(b"tls-cert"),
        vec![("tls-cert".into(), "/env/cert.pem".into())]
    );
    config.validate().unwrap();

    config.security.tls_key = std::path::PathBuf::new();
    assert_eq!(
        config.security.validate(),
        Err("tls-cert and tls-key must be set together")
    );
}

#[test]
fn parses_maxmemory_policy_from_file_and_environment() {
    let mut config = AppConfig::default();

    config
        .apply_toml("[memory]\nmaxmemory_policy = \"allkeys-lru\"\n")
        .unwrap();
    assert_eq!(config.memory.eviction_policy, EvictionPolicy::AllKeysLru);
    config
        .apply_environment(|name| (name == "RUVIO_MAXMEMORY_POLICY").then(|| "volatile-ttl".into()))
        .unwrap();
    assert_eq!(config.memory.eviction_policy, EvictionPolicy::VolatileTtl);
    assert!(
        AppConfig::default()
            .apply_toml("[memory]\nmaxmemory_policy = \"random\"\n")
            .is_err()
    );
}

#[test]
#[cfg_attr(miri, ignore = "writes the config file, which fsyncs")]
fn parses_memory_tuning_from_file_and_environment() {
    let mut config = AppConfig::default();

    config
            .apply_toml(
                "[memory]\nmaxmemory_samples = 4\nactive_expire_keys = 32\nactive_expire_maintenance_keys = 64\nhash_max_compact_entries = 8\nhash_max_compact_value_bytes = 128\n",
            )
            .unwrap();
    assert_eq!(config.memory.maxmemory_samples, 4);
    assert_eq!(config.memory.active_expire_keys, 32);
    assert_eq!(config.memory.active_expire_maintenance_keys, 64);
    assert_eq!(config.memory.hash_max_compact_entries, 8);
    assert_eq!(config.memory.hash_max_compact_value_bytes, 128);
    config
        .apply_environment(|name| match name {
            "RUVIO_MAXMEMORY_SAMPLES" => Some("8".into()),
            "RUVIO_ACTIVE_EXPIRE_KEYS" => Some("64".into()),
            "RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS" => Some("32".into()),
            _ => None,
        })
        .unwrap();
    assert_eq!(config.memory.maxmemory_samples, 8);
    assert_eq!(config.memory.active_expire_keys, 64);
    assert_eq!(config.memory.active_expire_maintenance_keys, 32);
    assert_eq!(
        config.config_get(b"active-expire-*"),
        vec![
            ("active-expire-keys".into(), "64".into()),
            ("active-expire-maintenance-keys".into(), "32".into()),
        ]
    );
    assert_eq!(
        config.config_get(b"maxmemory-samples"),
        vec![("maxmemory-samples".into(), "8".into())]
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ruvio.toml");
    std::fs::write(&path, "[memory]\nactive_expire_keys = 32\n").unwrap();
    let mut loaded = AppConfig::default();
    loaded
        .apply_toml(&std::fs::read_to_string(&path).unwrap())
        .unwrap();
    config.write_overrides(&path, &loaded).unwrap();
    let mut restored = AppConfig::default();
    restored
        .apply_toml(&std::fs::read_to_string(&path).unwrap())
        .unwrap();
    assert_eq!(restored.memory.active_expire_keys, 64);
    assert_eq!(restored.memory.active_expire_maintenance_keys, 32);
    config.memory.maxmemory_samples = 0;
    assert_eq!(
        config.memory.validate(),
        Err("maxmemory-samples must be between 1 and 64")
    );
    config.memory.maxmemory_samples = 8;
    for invalid in [0, 1_025] {
        config.memory.active_expire_maintenance_keys = invalid;
        assert_eq!(
            config.memory.validate(),
            Err("active-expire-maintenance-keys must be between 1 and 1024")
        );
    }
}

#[test]
fn parses_database_count_from_file_and_environment() {
    let mut config = AppConfig::default();

    assert_eq!(config.protocol.databases, 16);
    config.apply_toml("[protocol]\ndatabases = 4\n").unwrap();
    assert_eq!(config.protocol.databases, 4);
    config
        .apply_environment(|name| (name == "RUVIO_DATABASES").then(|| "32".into()))
        .unwrap();
    assert_eq!(config.protocol.databases, 32);
    assert_eq!(
        config.config_get(b"databases"),
        vec![("databases".into(), "32".into())]
    );
    config.protocol.databases = 0;
    assert!(config.protocol.validate().is_err());
    config.protocol.databases = 256;
    assert!(config.protocol.validate().is_ok());
    config.protocol.databases = 257;
    assert!(config.protocol.validate().is_err());
}

#[test]
fn accepts_hot_keys_from_file_and_environment() {
    let mut config = AppConfig::default();

    assert!(config.hot_keys.is_empty());
    config
        .apply_toml(r#"hot_keys = ["lock:*", "session:*"]"#)
        .unwrap();
    assert_eq!(
        config.hot_keys,
        vec![b"lock:*".to_vec(), b"session:*".to_vec()]
    );
    config
        .apply_environment(|name| (name == "RUVIO_HOT_KEYS").then(|| "cache:?,other:*".into()))
        .unwrap();
    assert_eq!(
        config.hot_keys,
        vec![b"cache:?".to_vec(), b"other:*".to_vec()]
    );
}

#[test]
fn rejects_empty_hot_key_patterns() {
    let config = AppConfig {
        hot_keys: vec![Vec::new()],
        ..AppConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err("hot_keys entries must be non-empty globs".into())
    );
}

#[test]
fn process_maxmemory_split_keeps_the_same_usable_total_for_any_shard_count() {
    let budget_total = |shards: usize| {
        let mut config = AppConfig {
            shards,
            ..AppConfig::default()
        };

        config.memory.max_process_bytes = 64 * 1024 * 1024;
        config.validate().unwrap();
        config
            .shard_memory_limits()
            .shard_admission_budget()
            .unwrap()
            * shards
    };
    let single = budget_total(1);

    assert_eq!(single, 48 * 1024 * 1024);
    for shards in [2, 4, 8] {
        assert_eq!(budget_total(shards), single, "{shards} shards");
    }
}

#[test]
fn maxmemory_that_leaves_no_shard_budget_is_rejected_at_startup() {
    let mut config = AppConfig {
        shards: 4,
        ..AppConfig::default()
    };

    config.memory.max_process_bytes = 16 * 1024 * 1024;
    let error = config.validate().unwrap_err();

    assert!(error.contains("per shard"), "{error}");

    config.memory.reserved_headroom_bytes = 0;
    config.validate().unwrap();
}

#[test]
fn checkpoint_partitions_include_hot_store_when_hot_keys_set() {
    let off = AppConfig {
        shards: 2,
        ..AppConfig::default()
    };

    assert_eq!(off.checkpoint_partition_count(), 2);
    let on = AppConfig {
        shards: 2,
        hot_keys: vec![b"lock:*".to_vec()],
        ..AppConfig::default()
    };

    assert_eq!(on.checkpoint_partition_count(), 3);
}

#[test]
fn replication_config_parses_role_and_rejects_a_non_wal_profile() {
    let mut config = AppConfig::default();

    config
        .apply_toml(
            r#"
            replication.role = "primary"
            replication.listen = "127.0.0.1:16379"
            persistence.profile = "everysec"
            persistence.data_dir = "target/replication-config"
            "#,
        )
        .unwrap();
    assert_eq!(config.replication.role, ReplicationRole::Primary);
    assert_eq!(config.replication.listen.unwrap().port(), 16379);
    config.replication.role = ReplicationRole::Primary;
    config.persistence.profile = DurabilityProfile::Memory;
    assert_eq!(
        config.validate(),
        Err("replication requires a WAL-backed durability profile".into())
    );
    config.persistence.profile = DurabilityProfile::Everysec;
    config
        .apply_environment(|name| match name {
            "RUVIO_REPLICATION_ROLE" => Some("replica".into()),
            "RUVIO_REPLICATION_PRIMARY" => Some("127.0.0.1:16379".into()),
            _ => None,
        })
        .unwrap();
    assert_eq!(config.replication.role, ReplicationRole::Replica);
    assert_eq!(config.replication.primary.unwrap().port(), 16379);
    config.hot_keys.clear();
    config.replication.role = ReplicationRole::Primary;
    config.replication.listen = None;
    assert_eq!(
        config.validate(),
        Err("replication primary requires replication.listen".into())
    );
    config.replication.role = ReplicationRole::Replica;
    config.replication.primary = None;
    assert_eq!(
        config.validate(),
        Err("replication replica requires replication.primary".into())
    );
    config.replication.role = ReplicationRole::Primary;
    config.replication.listen = Some("127.0.0.1:6379".parse().unwrap());
    assert_eq!(
        config.validate(),
        Err("replication port range overlaps the RESP listener range".into())
    );
    config.replication.listen = Some("127.0.0.1:16379".parse().unwrap());
    config.metrics_address = None;
    assert_eq!(
        config.validate(),
        Err("replication port range overlaps the cluster bus port range".into())
    );
    config.replication.listen = Some("127.0.0.1:26379".parse().unwrap());
    config.metrics_address = Some("127.0.0.1:26379".into());
    assert_eq!(
        config.validate(),
        Err("replication port range overlaps the metrics listener range".into())
    );
    assert!(
        config
            .apply_toml(
                r#"
            replication.nope = true
            "#,
            )
            .is_err()
    );
    assert_eq!(
        config.apply_toml("replication.listen = \"localhost:16379\"\n"),
        Err("replication.listen must be a numeric socket address".into())
    );
    assert_eq!(
        config.apply_environment(|name| {
            (name == "RUVIO_REPLICATION_PRIMARY").then(|| "not-a-socket".into())
        }),
        Err("replication.primary must be host:port".into())
    );
}

#[test]
fn validate_rejects_requirepass_longer_than_65535() {
    let mut config = AppConfig::default();

    config.security.requirepass = "x".repeat(65_536);
    assert_eq!(
        config.validate(),
        Err("requirepass must be at most 65535 bytes".into())
    );
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn refuses_a_public_listener_without_authentication() {
    let mut config = AppConfig::default();

    config.address = "0.0.0.0:6379".into();
    config.security.protected_mode = false;
    assert!(config.validate().unwrap_err().contains("protected mode"));

    config.security.requirepass = "secret".into();
    assert!(config.validate().is_ok());

    config.security.requirepass.clear();
    config.security.allow_unauthenticated_public = true;
    assert!(config.validate().is_ok());
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn public_metrics_and_replication_require_a_secret() {
    let mut config = AppConfig::default();

    config.metrics_address = Some("0.0.0.0:9121".into());
    assert!(config.validate().unwrap_err().contains("metrics"));
    config.security.metrics_token = "scrape".into();
    assert!(config.validate().is_ok());

    config.persistence.profile = crate::DurabilityProfile::Everysec;
    config.replication.role = ReplicationRole::Primary;
    config.replication.listen = Some("0.0.0.0:26379".parse().unwrap());
    assert!(config.validate().unwrap_err().contains("replication"));
    config.security.replication_password = "repl".into();
    assert!(config.validate().is_ok());
    assert_eq!(
        config.config_get(b"replication-password"),
        vec![("replication-password".into(), "***".into())]
    );
    assert_eq!(
        config.config_get(b"metrics-token"),
        vec![("metrics-token".into(), "***".into())]
    );
}

#[cfg(unix)]
#[test]
#[cfg_attr(miri, ignore = "writes the config file, which fsyncs")]
fn written_overrides_are_readable_by_the_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ruvio.toml");
    let loaded = AppConfig::default();
    let mut config = AppConfig::default();

    config.security.requirepass = "secret".into();
    config.write_overrides(&path, &loaded).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

/// Every setting the shipped template names answers `CONFIG GET` under one name. A
/// key added to the template fails here until it has a row and a `CONFIG GET` entry.
#[test]
fn every_template_setting_answers_config_get() {
    const NAMES: &[(&str, &str)] = &[
        ("address", "address"),
        ("advertised_host", "advertised-host"),
        (
            "connections.max_commands_per_second",
            "max-commands-per-second",
        ),
        ("connections.max_global", "max-connections"),
        ("connections.max_per_ip", "max-connections-per-ip"),
        ("connections.max_per_user", "max-connections-per-user"),
        ("hot_keys", "hot-keys"),
        ("memory.active_expire_keys", "active-expire-keys"),
        (
            "memory.active_expire_maintenance_keys",
            "active-expire-maintenance-keys",
        ),
        (
            "memory.hash_max_compact_entries",
            "hash-max-compact-entries",
        ),
        (
            "memory.hash_max_compact_value_bytes",
            "hash-max-compact-value-bytes",
        ),
        (
            "memory.list_max_compact_entries",
            "list-max-compact-entries",
        ),
        (
            "memory.list_max_compact_value_bytes",
            "list-max-compact-value-bytes",
        ),
        ("memory.max_process_bytes", "maxmemory"),
        ("memory.max_shard_bytes", "max-shard-bytes"),
        ("memory.maxmemory_policy", "maxmemory-policy"),
        ("memory.maxmemory_samples", "maxmemory-samples"),
        ("memory.reserved_headroom_bytes", "memory-headroom-bytes"),
        ("memory.set_max_intset_entries", "set-max-intset-entries"),
        ("notify_keyspace_events", "notify-keyspace-events"),
        ("persistence.data_dir", "data-dir"),
        ("persistence.profile", "durability"),
        ("persistence.snapshot_interval_ms", "snapshot-interval-ms"),
        (
            "persistence.snapshot_journal_bytes",
            "snapshot-journal-bytes",
        ),
        ("persistence.snapshot_max_bytes", "snapshot-max-bytes"),
        (
            "persistence.snapshot_min_mutations",
            "snapshot-min-mutations",
        ),
        ("persistence.sync_interval_ms", "wal-sync-interval-ms"),
        ("persistence.wal_segment_bytes", "wal-segment-bytes"),
        ("protocol.databases", "databases"),
        ("protocol.max_arguments", "max-arguments"),
        ("protocol.max_command_bytes", "max-command-bytes"),
        (
            "protocol.max_connection_buffer_bytes",
            "max-connection-buffer-bytes",
        ),
        ("protocol.max_key_bytes", "max-key-bytes"),
        ("protocol.max_pipeline_commands", "max-pipeline-commands"),
        ("protocol.max_value_bytes", "max-value-bytes"),
        ("replication.announced", "replica-announced"),
        ("replication.min_replicas_max_lag", "min-replicas-max-lag"),
        ("replication.min_replicas_to_write", "min-replicas-to-write"),
        ("replication.primary_lease", "primary-lease"),
        (
            "replication.primary_lease_coordinators",
            "primary-lease-coordinators",
        ),
        ("replication.role", "replication-role"),
        (
            "requests.max_output_buffer_bytes",
            "max-output-buffer-bytes",
        ),
        ("requests.max_queued_requests", "max-queued-requests"),
        ("scripts.max_bytes", "script-max-bytes"),
        ("scripts.max_cached", "script-max-cached"),
        ("scripts.max_calls", "script-max-calls"),
        ("scripts.max_instructions", "script-max-instructions"),
        (
            "security.allow_unauthenticated_public",
            "allow-unauthenticated-public",
        ),
        ("security.metrics_token", "metrics-token"),
        ("security.protected_mode", "protected-mode"),
        ("security.replication_password", "replication-password"),
        ("security.requirepass", "requirepass"),
        ("security.tls_cert", "tls-cert"),
        ("security.tls_key", "tls-key"),
        ("shards", "shards"),
        ("shutdown_grace_ms", "shutdown-grace-ms"),
        ("timeouts.idle_ms", "idle-timeout-ms"),
        ("timeouts.read_ms", "read-timeout-ms"),
        ("timeouts.write_ms", "write-timeout-ms"),
    ];

    fn keys(table: &toml::Table, prefix: &str, out: &mut Vec<String>) {
        for (name, value) in table {
            match value {
                toml::Value::Table(inner) => keys(inner, &format!("{prefix}{name}."), out),
                _ => out.push(format!("{prefix}{name}")),
            }
        }
    }

    let template: toml::Table = toml::from_str(DEFAULT_CONFIG).unwrap();
    let mut found = Vec::new();

    keys(&template, "", &mut found);
    let answered: Vec<String> = AppConfig::default()
        .introspect()
        .into_iter()
        .map(|(name, _)| name)
        .collect();

    for key in &found {
        let Some((_, name)) = NAMES.iter().find(|(path, _)| path == key) else {
            panic!("template setting {key} has no CONFIG GET name in this test");
        };

        assert!(
            answered.iter().any(|answered| answered == name),
            "CONFIG GET does not answer {name} (template {key})"
        );
    }
}

/// Every `CONFIG GET` name is decided: live (`CONFIG SET` changes it) or restart-only. A
/// setting added to `introspect` fails here until it is in exactly one list.
#[test]
fn every_config_get_name_is_live_or_restart_only() {
    let names: Vec<String> = AppConfig::default()
        .introspect()
        .into_iter()
        .map(|(name, _)| name)
        .collect();

    for name in &names {
        let live = super::LIVE_SETTINGS.contains(&name.as_str());
        let restart = super::RESTART_ONLY_SETTINGS.contains(&name.as_str());

        assert!(
            live != restart,
            "{name}: live {live}, restart-only {restart}"
        );
    }

    for name in super::LIVE_SETTINGS
        .iter()
        .chain(super::RESTART_ONLY_SETTINGS)
    {
        assert!(
            names.iter().any(|known| known == name),
            "{name} is not a CONFIG GET name"
        );
    }
}

/// Each live setting takes the text `CONFIG GET` shows for it: setting every one to its own
/// value changes nothing.
#[test]
fn every_live_setting_accepts_its_config_get_value() {
    let config = AppConfig::default();

    for (name, value) in config.introspect() {
        if !super::LIVE_SETTINGS.contains(&name.as_str()) || name == "requirepass" {
            continue;
        }

        let changed = config
            .with_live_changes(&[(name.as_bytes(), value.as_bytes())])
            .unwrap_or_else(|error| panic!("{name} = {value:?}: {error}"));

        assert_eq!(changed, config, "{name} = {value:?}");
    }
}

#[test]
fn live_changes_apply_together_or_not_at_all() {
    let config = AppConfig::default();
    let changed = config
        .with_live_changes(&[
            (b"MAXMEMORY", b"200000000"),
            (b"maxmemory-policy", b"allkeys-lfu"),
            (b"notify-keyspace-events", b"Ex"),
            (b"idle-timeout-ms", b"1500"),
            (b"requirepass", b"pw"),
        ])
        .unwrap();

    assert_eq!(changed.memory.max_process_bytes, 200_000_000);
    assert_eq!(changed.memory.eviction_policy, EvictionPolicy::AllKeysLfu);
    assert_ne!(changed.memory.notify_mask, 0);
    assert_eq!(changed.timeouts.idle, Duration::from_millis(1500));
    assert_eq!(changed.security.requirepass, "pw");

    for (pairs, error) in [
        (
            &[(&b"maxmemory"[..], &b"1"[..]), (b"port-mode", b"single")][..],
            "restart-only setting",
        ),
        (&[(b"maxmemory", b"lots")], "must be an unsigned integer"),
        (
            &[(b"maxmemory-policy", b"random")],
            "unknown maxmemory-policy",
        ),
        (
            &[(b"notify-keyspace-events", b"?")],
            "notify-keyspace-events",
        ),
        (&[(b"nope", b"1")], "Unknown option"),
    ] {
        let message = config.with_live_changes(pairs).unwrap_err();

        assert!(message.contains(error), "{message}");
    }
}

/// Both tables of `docs/CONFIGURATION.md` agree with `LIVE_SETTINGS`: the `CONFIG SET` column
/// of the environment table names each live setting once and says `no` for every other
/// row, and the table under *Changing settings at runtime* lists each live setting once.
#[test]
fn the_configuration_tables_show_every_live_setting() {
    let docs = include_str!("../../docs/CONFIGURATION.md");
    let mut in_environment = Vec::new();
    let mut in_runtime = Vec::new();

    let runtime = docs
        .split("## Changing settings at runtime")
        .nth(1)
        .and_then(|rest| rest.split("\n## ").next())
        .unwrap();

    for row in docs.lines().filter(|line| line.starts_with("| `RUVIO_")) {
        match row.split('|').nth(2).unwrap().trim() {
            "no" => {}
            cell => in_environment.push(cell.trim_matches('`')),
        }
    }

    // Rows of the runtime table, after its header row.
    for row in runtime
        .lines()
        .filter(|line| line.starts_with("| `"))
        .skip(1)
    {
        in_runtime.push(row.split('|').nth(1).unwrap().trim().trim_matches('`'));
    }

    for shown in [&in_environment, &in_runtime] {
        let mut sorted = shown.clone();
        let mut live = super::LIVE_SETTINGS.to_vec();

        sorted.sort_unstable();
        live.sort_unstable();
        assert_eq!(sorted, live);
    }
}

#[test]
fn replication_leaves_the_host_only_for_a_non_loopback_peer() {
    let mut replication = ReplicationConfig::default();

    assert!(!replication.leaves_the_host());

    replication.role = ReplicationRole::Primary;
    replication.listen = Some("127.0.0.1:7000".parse().unwrap());
    assert!(!replication.leaves_the_host());
    replication.listen = Some("10.0.0.5:7000".parse().unwrap());
    assert!(replication.leaves_the_host());

    replication.role = ReplicationRole::Replica;
    replication.primary = Some("127.0.0.1:7000".parse().unwrap());
    assert!(!replication.leaves_the_host());
    replication.primary = None;
    replication.primary_name = Some(("primary.internal".into(), 7000));
    assert!(replication.leaves_the_host());
}

#[test]
fn replication_tls_needs_its_material_and_stops_the_cleartext_warning() {
    let mut config = AppConfig::default();

    config
        .apply_toml("[persistence]\nprofile = \"always\"\n")
        .unwrap();

    config.replication.role = ReplicationRole::Replica;
    config.replication.primary_name = Some(("primary.internal".into(), 7000));
    assert!(config.replication.leaves_the_host());

    config.replication.tls = true;
    assert!(!config.replication.leaves_the_host());
    assert!(config.validate().unwrap_err().contains("tls_ca"));

    config.replication.tls_ca = "ca.pem".into();
    config.replication.tls_client_cert = "client.pem".into();
    assert!(config.validate().unwrap_err().contains("go together"));

    config.replication.tls_client_cert = Default::default();
    config.validate().unwrap();

    config.replication.role = ReplicationRole::Primary;
    config.replication.listen = Some("127.0.0.1:7000".parse().unwrap());
    assert!(config.validate().unwrap_err().contains("tls_cert"));
}

#[test]
fn the_primary_lease_needs_the_coordinator_ids() {
    let mut config = super::AppConfig::default();

    config.replication.primary_lease = true;
    assert!(config.validate().is_err(), "a lease without coordinators");

    config
        .apply_environment(|name| {
            (name == "RUVIO_PRIMARY_LEASE_COORDINATORS").then(|| "1, 2,3".into())
        })
        .unwrap();

    assert_eq!(config.replication.primary_lease_coordinators, [1, 2, 3]);
    config.validate().unwrap();

    let mut twice = config.clone();

    twice.replication.primary_lease_coordinators = vec![1, 1];
    assert!(twice.validate().is_err(), "an id listed twice");

    // The lease cannot be turned on live while the list is empty.
    let off = super::AppConfig::default();

    assert!(
        off.with_live_changes(&[(b"primary-lease", b"yes")])
            .is_err()
    );
}

/// The value a schema test sets for a setting: one that differs from the default, as it
/// is written in the file (TOML) and in the environment.
fn schema_sample(setting: &super::schema::Setting) -> (String, String) {
    use super::schema::Kind;

    let text = match setting.file {
        "address" => "127.0.0.1:7100",
        "advertised_host" => "10.0.0.9",
        "replication.listen" => "127.0.0.1:27100",
        "replication.primary" => "10.0.0.9:27100",
        "metrics_address" => "127.0.0.1:9100",
        "cpu_set" => "0-1",
        "notify_keyspace_events" => "KEA",
        "cluster_group" => "a",
        _ => "/tmp/ruvio-schema-sample",
    };

    match setting.kind {
        Kind::Integer => ("7".into(), "7".into()),
        Kind::Boolean => {
            let default = !matches!(
                setting.file,
                "security.protected_mode"
                    | "replication.announced"
                    | "latency_histograms"
                    | "key_stats"
            );
            (default.to_string(), default.to_string())
        }
        Kind::Text => (format!("{text:?}"), text.to_owned()),
        Kind::List if setting.file == "hot_keys" => ("[\"a*\", \"b*\"]".into(), "a*,b*".into()),
        Kind::List => ("[1, 2]".into(), "1,2".into()),
        // The first value of every list is the default.
        Kind::Enum(values) => (format!("{:?}", values[1]), values[1].to_owned()),
    }
}

/// Every variable `apply_environment` reads has one row in the schema, and no row names
/// a variable it does not read.
#[test]
fn the_schema_lists_every_environment_variable() {
    let mut listed: Vec<String> = super::schema::SETTINGS
        .iter()
        .filter(|setting| !setting.env.is_empty())
        .map(|setting| setting.env.to_owned())
        .collect();

    listed.sort();
    assert_eq!(listed, environment_names());
}

/// Every key the file accepts has a row in the schema, and every row's key is accepted.
#[test]
fn the_schema_lists_every_file_setting() {
    let mut listed: Vec<String> = super::schema::SETTINGS
        .iter()
        .filter(|setting| !setting.file.is_empty())
        .map(|setting| setting.file.to_owned())
        .collect();

    listed.sort();
    listed.dedup();
    let mut keys = file_keys();

    keys.sort();
    assert_eq!(listed, keys);
}

#[test]
fn schema_config_set_names_change_at_runtime() {
    for setting in super::schema::SETTINGS {
        if !setting.config_set.is_empty() {
            assert!(
                LIVE_SETTINGS.contains(&setting.config_set),
                "{} names {}, which CONFIG SET does not change",
                setting.env,
                setting.config_set
            );
        }
    }
}

/// A row's variable and file key set the same field: the same value through either one
/// gives the same configuration, and it differs from the default.
#[test]
fn schema_variable_and_file_key_set_the_same_setting() {
    for setting in super::schema::SETTINGS {
        // Older names for a setting that another variable also sets.
        if setting.env.is_empty() || setting.file.is_empty() || setting.env == "RUVIO_CORE" {
            continue;
        }
        let (toml_value, env_value) = schema_sample(setting);
        let document = match setting.file.split_once('.') {
            Some((section, key)) => format!("[{section}]\n{key} = {toml_value}\n"),
            None => format!("{} = {toml_value}\n", setting.file),
        };
        let mut from_file = AppConfig::default();

        from_file
            .apply_toml(&document)
            .unwrap_or_else(|error| panic!("{}: {error}", setting.file));
        let mut from_env = AppConfig::default();

        from_env
            .apply_environment(|name| (name == setting.env).then(|| env_value.clone()))
            .unwrap_or_else(|error| panic!("{}: {error}", setting.env));
        assert!(
            from_file != AppConfig::default(),
            "{} = {toml_value} left the default unchanged",
            setting.file
        );
        assert!(
            from_file == from_env,
            "{} and {} set different fields",
            setting.file,
            setting.env
        );
    }
}

/// The site's copy of the schema matches the table. Regenerate it with
/// `cargo run --bin ruvio -- --print-config-schema > site-next/src/data/config-schema.json`.
#[test]
fn the_site_schema_file_is_current() {
    let committed =
        include_str!("../../site-next/src/data/config-schema.json").replace("\r\n", "\n");

    assert!(
        committed == super::schema::schema_json(),
        "site-next/src/data/config-schema.json is stale: run \
         `cargo run --bin ruvio -- --print-config-schema > site-next/src/data/config-schema.json`"
    );
}
