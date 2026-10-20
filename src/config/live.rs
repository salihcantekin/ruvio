//! `CONFIG SET` and `CONFIG REWRITE`: which settings change while the server runs, how a
//! change is checked, and how the live values reach the config file.

use std::{path::Path, time::Duration};

use super::{AppConfig, parse_bool};

/// Settings `CONFIG SET` changes while the server runs, by their `CONFIG GET` name. Each
/// shard applies a change on its own thread; connection settings apply to new connections.
pub const LIVE_SETTINGS: &[&str] = &[
    // Memory
    "maxmemory",
    "max-shard-bytes",
    "memory-headroom-bytes",
    "maxmemory-policy",
    "maxmemory-samples",
    "active-expire-keys",
    "active-expire-maintenance-keys",
    "hash-max-compact-entries",
    "hash-max-compact-value-bytes",
    "set-max-intset-entries",
    "list-max-compact-entries",
    "list-max-compact-value-bytes",
    "notify-keyspace-events",
    // Observability
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "key-stats",
    "latency-histograms",
    // Connections
    "idle-timeout-ms",
    "read-timeout-ms",
    "write-timeout-ms",
    "max-connections",
    "max-connections-per-ip",
    "max-connections-per-user",
    "max-commands-per-second",
    "max-output-buffer-bytes",
    // Scripts
    "script-max-bytes",
    "script-max-cached",
    "script-max-calls",
    "script-max-instructions",
    "script-max-time-ms",
    // Security
    "requirepass",
    // Replication
    "min-replicas-to-write",
    "min-replicas-max-lag",
];

/// `CONFIG GET` names that only change with a restart, or only report a value. `CONFIG SET`
/// refuses them by name. Every `CONFIG GET` name is in exactly one of the two lists.
pub const RESTART_ONLY_SETTINGS: &[&str] = &[
    "acl-file",
    "acl-users",
    "address",
    "advertised-host",
    "advertised-port",
    "allow-unauthenticated-public",
    "appendonly",
    "cdc-retain-bytes",
    "cluster-group",
    "cluster-topology",
    "cpu-set",
    "data-dir",
    "databases",
    "dir",
    "durability",
    "hot-keys",
    "max-arguments",
    "max-command-bytes",
    "max-connection-buffer-bytes",
    "max-key-bytes",
    "max-pipeline-commands",
    "max-queued-requests",
    "max-value-bytes",
    "metrics-address",
    "metrics-token",
    "migration-max-bytes-per-sec",
    "port-mode",
    "protected-mode",
    "replica-announced",
    "replica-retain-max-bytes",
    "replication-listen",
    "replication-password",
    "replication-primary",
    "replication-role",
    "save",
    "shards",
    "shutdown-grace-ms",
    "snapshot-interval-ms",
    "snapshot-journal-bytes",
    "snapshot-max-bytes",
    "snapshot-min-mutations",
    "sync-worker",
    "sync-worker-active",
    "tls-cert",
    "tls-client-auth",
    "tls-client-ca",
    "tls-key",
    "wal-prefill",
    "wal-segment-bytes",
    "wal-sync-interval-ms",
];

impl AppConfig {
    /// This configuration with the `CONFIG SET` pairs applied, checked with the rules the
    /// file and the environment follow. An unknown, repeated or restart-only name, or a
    /// value that does not parse or validate, fails the whole call with the error text to
    /// send (without the `ERR ` prefix), and nothing changes.
    pub fn with_live_changes(&self, pairs: &[(&[u8], &[u8])]) -> Result<Self, String> {
        let mut changed = self.clone();
        let mut names: Vec<String> = Vec::with_capacity(pairs.len());

        for (name, value) in pairs {
            let name = String::from_utf8_lossy(name).to_ascii_lowercase();

            if names.contains(&name) {
                return Err(format!("Duplicate parameter - '{name}'"));
            }

            if RESTART_ONLY_SETTINGS.contains(&name.as_str()) {
                return Err(failed(
                    &name,
                    "restart-only setting; change it in the config file and restart",
                ));
            }

            if !LIVE_SETTINGS.contains(&name.as_str()) {
                return Err(format!(
                    "Unknown option or number of arguments for CONFIG SET - '{name}'"
                ));
            }

            let value = std::str::from_utf8(value)
                .map_err(|_| failed(&name, "the value is not valid UTF-8"))?;

            changed
                .set_live(&name, value)
                .map_err(|reason| failed(&name, &reason))?;
            names.push(name);
        }

        changed
            .validate()
            .map_err(|reason| failed(&names.join("', '"), &reason))?;
        Ok(changed)
    }

    /// Sets one name of [`LIVE_SETTINGS`] from its text form, the one `CONFIG GET` shows.
    fn set_live(&mut self, name: &str, value: &str) -> Result<(), String> {
        let memory = &mut self.memory;

        match name {
            "maxmemory" => memory.max_process_bytes = unsigned(value)?,
            "max-shard-bytes" => memory.max_shard_bytes = unsigned(value)?,
            "memory-headroom-bytes" => memory.reserved_headroom_bytes = unsigned(value)?,
            "maxmemory-policy" => memory.eviction_policy = value.parse()?,
            "maxmemory-samples" => memory.maxmemory_samples = unsigned(value)?,
            "active-expire-keys" => memory.active_expire_keys = unsigned(value)?,
            "active-expire-maintenance-keys" => {
                memory.active_expire_maintenance_keys = unsigned(value)?;
            }
            "hash-max-compact-entries" => memory.hash_max_compact_entries = unsigned(value)?,
            "hash-max-compact-value-bytes" => {
                memory.hash_max_compact_value_bytes = unsigned(value)?;
            }
            "set-max-intset-entries" => memory.set_max_intset_entries = unsigned(value)?,
            "list-max-compact-entries" => memory.list_max_compact_entries = unsigned(value)?,
            "list-max-compact-value-bytes" => {
                memory.list_max_compact_value_bytes = unsigned(value)?;
            }
            "notify-keyspace-events" => self.set_notify_keyspace_events(value.to_owned())?,
            "slowlog-log-slower-than" => {
                self.slowlog_threshold_micros = value
                    .parse()
                    .map_err(|_| "the value must be an integer".to_owned())?;
            }
            "slowlog-max-len" => self.slowlog_max_len = unsigned(value)?,
            "key-stats" => self.key_stats = parse_bool(value, "the value")?,
            "latency-histograms" => self.latency_histograms = parse_bool(value, "the value")?,
            "idle-timeout-ms" => self.timeouts.idle = Duration::from_millis(unsigned(value)?),
            "read-timeout-ms" => self.timeouts.read = Duration::from_millis(unsigned(value)?),
            "write-timeout-ms" => self.timeouts.write = Duration::from_millis(unsigned(value)?),
            "max-connections" => self.connections.max_global = unsigned(value)?,
            "max-connections-per-ip" => self.connections.max_per_ip = unsigned(value)?,
            "max-connections-per-user" => self.connections.max_per_user = unsigned(value)?,
            "max-commands-per-second" => {
                self.connections.max_commands_per_second = unsigned(value)?;
            }
            "max-output-buffer-bytes" => self.requests.max_output_buffer_bytes = unsigned(value)?,
            "script-max-bytes" => self.scripts.max_bytes = unsigned(value)?,
            "script-max-cached" => self.scripts.max_cached = unsigned(value)?,
            "script-max-calls" => self.scripts.max_calls = unsigned(value)?,
            "script-max-instructions" => self.scripts.max_instructions = unsigned(value)?,
            "script-max-time-ms" => self.scripts.max_time_ms = unsigned(value)?,
            "requirepass" => self.security.requirepass = value.to_owned(),
            "min-replicas-to-write" => self.replication.min_replicas_to_write = unsigned(value)?,
            "min-replicas-max-lag" => {
                self.replication.min_replicas_max_lag = Duration::from_secs(unsigned(value)?);
            }
            _ => return Err("not a live setting".into()),
        }

        Ok(())
    }

    /// Copies every [`LIVE_SETTINGS`] value of `live` into this configuration.
    fn copy_live_settings(&mut self, live: &Self) {
        self.memory = live.memory;
        self.notify_keyspace_events = live.notify_keyspace_events.clone();
        self.slowlog_threshold_micros = live.slowlog_threshold_micros;
        self.slowlog_max_len = live.slowlog_max_len;
        self.key_stats = live.key_stats;
        self.latency_histograms = live.latency_histograms;
        self.timeouts = live.timeouts;
        self.connections = live.connections;
        self.requests.max_output_buffer_bytes = live.requests.max_output_buffer_bytes;
        self.scripts = live.scripts;
        self.security.requirepass = live.security.requirepass.clone();
        self.replication.min_replicas_to_write = live.replication.min_replicas_to_write;
        self.replication.min_replicas_max_lag = live.replication.min_replicas_max_lag;
    }

    /// `CONFIG REWRITE`: writes the live settings of `self` into the config file the server
    /// started from (`config_file`), through the same path as `--write-config`, which keeps
    /// the file's other keys. Restart-only settings keep what the file says, even when the
    /// process runs with an environment or command-line value for them.
    pub fn rewrite_live_settings(&self) -> Result<(), String> {
        let Some(path) = self.config_file.as_deref() else {
            return Err("The server is running without a config file".into());
        };
        let loaded = loaded_from(path)?;
        let mut target = loaded.clone();

        target.copy_live_settings(self);
        target.write_overrides(path, &loaded)
    }
}

/// The configuration the file at `path` holds over the defaults, without the environment.
fn loaded_from(path: &Path) -> Result<AppConfig, String> {
    let mut loaded = AppConfig::default();

    match std::fs::read_to_string(path) {
        Ok(contents) => loaded.apply_toml(&contents)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("failed to read config {}: {error}", path.display())),
    }

    loaded.config_file = Some(path.to_path_buf());
    Ok(loaded)
}

fn unsigned<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| "the value must be an unsigned integer".to_owned())
}

fn failed(name: &str, reason: &str) -> String {
    format!("CONFIG SET failed (possibly related to argument '{name}') - {reason}")
}
