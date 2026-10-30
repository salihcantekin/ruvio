//! A machine-readable list of every setting: its environment variable, file key, flag,
//! `CONFIG SET` name, type and the value the default file holds. `ruvio
//! --print-config-schema` prints it as JSON; the setup wizard on the site reads that file.
//! Tests keep the table in step with `apply_environment`, the file keys and the flags.

use super::DEFAULT_CONFIG;

/// The values a setting takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Integer,
    Boolean,
    Text,
    /// A TOML array; a comma-separated list in the environment.
    List,
    Enum(&'static [&'static str]),
}

/// One setting. An empty string means the setting has no such name.
#[derive(Clone, Copy, Debug)]
pub struct Setting {
    pub env: &'static str,
    pub file: &'static str,
    pub flag: &'static str,
    pub config_set: &'static str,
    pub kind: Kind,
}

const PORT_MODES: &[&str] = &["per-shard", "single"];
const PROFILES: &[&str] = &["memory", "snapshot", "everysec", "always"];
const POLICIES: &[&str] = &[
    "noeviction",
    "allkeys-lru",
    "allkeys-lfu",
    "volatile-lru",
    "volatile-ttl",
];
const ROLES: &[&str] = &["off", "primary", "replica"];
const AUTO_ON_OFF: &[&str] = &["auto", "on", "off"];

use Kind::{Boolean, Enum, Integer, List, Text};

const fn row(
    env: &'static str,
    file: &'static str,
    flag: &'static str,
    config_set: &'static str,
    kind: Kind,
) -> Setting {
    Setting {
        env,
        file,
        flag,
        config_set,
        kind,
    }
}

/// Every setting, in the order of the environment table in `docs/CONFIGURATION.md`,
/// then the settings only the file holds.
pub const SETTINGS: &[Setting] = &[
    row("RUVIO_ADDR", "address", "--address", "", Text),
    row("RUVIO_SHARDS", "shards", "--shards", "", Integer),
    row("RUVIO_PORT_MODE", "port_mode", "", "", Enum(PORT_MODES)),
    row(
        "RUVIO_ADVERTISE_HOST",
        "advertised_host",
        "--advertised-host",
        "",
        Text,
    ),
    row(
        "RUVIO_ADVERTISE_PORT",
        "advertised_port",
        "--advertised-port",
        "",
        Integer,
    ),
    row("RUVIO_CLUSTER_TOPOLOGY", "cluster_topology", "", "", Text),
    row("RUVIO_CLUSTER_GROUP", "cluster_group", "", "", Text),
    row(
        "RUVIO_MIGRATION_MAX_BYTES_PER_SEC",
        "migration_max_bytes_per_sec",
        "",
        "",
        Integer,
    ),
    row("RUVIO_CPUSET", "cpu_set", "--cpu-set", "", Text),
    row("RUVIO_CORE", "cpu_set", "", "", Text),
    row(
        "RUVIO_SHUTDOWN_GRACE_MS",
        "shutdown_grace_ms",
        "--shutdown-grace-ms",
        "",
        Integer,
    ),
    row(
        "RUVIO_SLOWLOG_THRESHOLD_MICROS",
        "slowlog_threshold_micros",
        "",
        "slowlog-log-slower-than",
        Integer,
    ),
    row(
        "RUVIO_SLOWLOG_MAX_LEN",
        "slowlog_max_len",
        "",
        "slowlog-max-len",
        Integer,
    ),
    row(
        "RUVIO_LATENCY_HISTOGRAMS",
        "latency_histograms",
        "",
        "latency-histograms",
        Boolean,
    ),
    row("RUVIO_KEY_STATS", "key_stats", "", "key-stats", Boolean),
    row(
        "RUVIO_MAX_ARGUMENTS",
        "protocol.max_arguments",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_KEY_BYTES",
        "protocol.max_key_bytes",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_VALUE_BYTES",
        "protocol.max_value_bytes",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_COMMAND_BYTES",
        "protocol.max_command_bytes",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_PIPELINE_COMMANDS",
        "protocol.max_pipeline_commands",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_CONNECTION_BUFFER_BYTES",
        "protocol.max_connection_buffer_bytes",
        "",
        "",
        Integer,
    ),
    row("RUVIO_DATABASES", "protocol.databases", "", "", Integer),
    row(
        "RUVIO_IDLE_TIMEOUT_MS",
        "timeouts.idle_ms",
        "--idle-timeout-ms",
        "idle-timeout-ms",
        Integer,
    ),
    row(
        "RUVIO_READ_TIMEOUT_MS",
        "timeouts.read_ms",
        "--read-timeout-ms",
        "read-timeout-ms",
        Integer,
    ),
    row(
        "RUVIO_WRITE_TIMEOUT_MS",
        "timeouts.write_ms",
        "--write-timeout-ms",
        "write-timeout-ms",
        Integer,
    ),
    row(
        "RUVIO_MAX_CONNECTIONS",
        "connections.max_global",
        "--max-connections",
        "max-connections",
        Integer,
    ),
    row(
        "RUVIO_MAX_CONNECTIONS_PER_IP",
        "connections.max_per_ip",
        "--max-connections-per-ip",
        "max-connections-per-ip",
        Integer,
    ),
    row(
        "RUVIO_MAX_CONNECTIONS_PER_USER",
        "connections.max_per_user",
        "--max-connections-per-user",
        "max-connections-per-user",
        Integer,
    ),
    row(
        "RUVIO_MAX_COMMANDS_PER_SECOND",
        "connections.max_commands_per_second",
        "--max-commands-per-second",
        "max-commands-per-second",
        Integer,
    ),
    row(
        "RUVIO_MAX_QUEUED_REQUESTS",
        "requests.max_queued_requests",
        "--max-queued-requests",
        "",
        Integer,
    ),
    row(
        "RUVIO_MAX_OUTPUT_BUFFER_BYTES",
        "requests.max_output_buffer_bytes",
        "--max-output-buffer-bytes",
        "max-output-buffer-bytes",
        Integer,
    ),
    row(
        "RUVIO_DURABILITY",
        "persistence.profile",
        "--durability",
        "",
        Enum(PROFILES),
    ),
    row(
        "RUVIO_DATA_DIR",
        "persistence.data_dir",
        "--data-dir",
        "",
        Text,
    ),
    row(
        "RUVIO_WAL_SEGMENT_BYTES",
        "persistence.wal_segment_bytes",
        "--wal-segment-bytes",
        "",
        Integer,
    ),
    row(
        "RUVIO_WAL_SYNC_INTERVAL_MS",
        "persistence.sync_interval_ms",
        "--wal-sync-interval-ms",
        "",
        Integer,
    ),
    row(
        "RUVIO_EXPERIMENTAL_SYNC_WORKER",
        "persistence.experimental_sync_worker",
        "",
        "",
        Boolean,
    ),
    row(
        "RUVIO_SYNC_WORKER",
        "persistence.sync_worker",
        "",
        "",
        Enum(AUTO_ON_OFF),
    ),
    row("RUVIO_WAL_PREFILL", "", "", "", Enum(AUTO_ON_OFF)),
    row(
        "RUVIO_SNAPSHOT_MAX_BYTES",
        "persistence.snapshot_max_bytes",
        "--snapshot-max-bytes",
        "",
        Integer,
    ),
    row(
        "RUVIO_SNAPSHOT_JOURNAL_BYTES",
        "persistence.snapshot_journal_bytes",
        "--snapshot-journal-bytes",
        "",
        Integer,
    ),
    row(
        "RUVIO_SNAPSHOT_INTERVAL_MS",
        "persistence.snapshot_interval_ms",
        "--snapshot-interval-ms",
        "",
        Integer,
    ),
    row(
        "RUVIO_SNAPSHOT_MIN_MUTATIONS",
        "persistence.snapshot_min_mutations",
        "--snapshot-min-mutations",
        "",
        Integer,
    ),
    row(
        "RUVIO_CDC_RETAIN_BYTES",
        "persistence.cdc_retain_bytes",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_REPLICA_RETAIN_MAX_BYTES",
        "persistence.replica_retain_max_bytes",
        "",
        "",
        Integer,
    ),
    row(
        "RUVIO_REQUIREPASS",
        "security.requirepass",
        "--requirepass",
        "requirepass",
        Text,
    ),
    row(
        "RUVIO_TLS_CERT",
        "security.tls_cert",
        "--tls-cert",
        "",
        Text,
    ),
    row("RUVIO_TLS_KEY", "security.tls_key", "--tls-key", "", Text),
    row(
        "RUVIO_TLS_CLIENT_CA",
        "security.tls_client_ca",
        "--tls-client-ca",
        "",
        Text,
    ),
    row(
        "RUVIO_TLS_CLIENT_AUTH",
        "security.tls_client_auth",
        "--tls-client-auth",
        "",
        Boolean,
    ),
    row("RUVIO_ACL_FILE", "security.acl_file", "", "", Text),
    row(
        "RUVIO_PROTECTED_MODE",
        "security.protected_mode",
        "--protected-mode",
        "",
        Boolean,
    ),
    row(
        "RUVIO_ALLOW_UNAUTHENTICATED",
        "security.allow_unauthenticated_public",
        "",
        "",
        Boolean,
    ),
    row(
        "RUVIO_METRICS_TOKEN",
        "security.metrics_token",
        "",
        "",
        Text,
    ),
    row(
        "RUVIO_REPLICATION_PASSWORD",
        "security.replication_password",
        "",
        "",
        Text,
    ),
    row(
        "RUVIO_MAXMEMORY",
        "memory.max_process_bytes",
        "--maxmemory",
        "maxmemory",
        Integer,
    ),
    row(
        "RUVIO_MAX_SHARD_BYTES",
        "memory.max_shard_bytes",
        "--max-shard-bytes",
        "max-shard-bytes",
        Integer,
    ),
    row(
        "RUVIO_MEMORY_HEADROOM_BYTES",
        "memory.reserved_headroom_bytes",
        "--memory-headroom-bytes",
        "memory-headroom-bytes",
        Integer,
    ),
    row(
        "RUVIO_MAXMEMORY_POLICY",
        "memory.maxmemory_policy",
        "--maxmemory-policy",
        "maxmemory-policy",
        Enum(POLICIES),
    ),
    row(
        "RUVIO_MAXMEMORY_SAMPLES",
        "memory.maxmemory_samples",
        "--maxmemory-samples",
        "maxmemory-samples",
        Integer,
    ),
    row(
        "RUVIO_ACTIVE_EXPIRE_KEYS",
        "memory.active_expire_keys",
        "--active-expire-keys",
        "active-expire-keys",
        Integer,
    ),
    row(
        "RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS",
        "memory.active_expire_maintenance_keys",
        "--active-expire-maintenance-keys",
        "active-expire-maintenance-keys",
        Integer,
    ),
    row(
        "RUVIO_HASH_MAX_COMPACT_ENTRIES",
        "memory.hash_max_compact_entries",
        "--hash-max-compact-entries",
        "hash-max-compact-entries",
        Integer,
    ),
    row(
        "RUVIO_HASH_MAX_COMPACT_VALUE_BYTES",
        "memory.hash_max_compact_value_bytes",
        "--hash-max-compact-value-bytes",
        "hash-max-compact-value-bytes",
        Integer,
    ),
    row(
        "RUVIO_SET_MAX_INTSET_ENTRIES",
        "memory.set_max_intset_entries",
        "--set-max-intset-entries",
        "set-max-intset-entries",
        Integer,
    ),
    row(
        "RUVIO_LIST_MAX_COMPACT_ENTRIES",
        "memory.list_max_compact_entries",
        "--list-max-compact-entries",
        "list-max-compact-entries",
        Integer,
    ),
    row(
        "RUVIO_LIST_MAX_COMPACT_VALUE_BYTES",
        "memory.list_max_compact_value_bytes",
        "--list-max-compact-value-bytes",
        "list-max-compact-value-bytes",
        Integer,
    ),
    row(
        "RUVIO_NOTIFY_KEYSPACE_EVENTS",
        "notify_keyspace_events",
        "",
        "notify-keyspace-events",
        Text,
    ),
    row(
        "RUVIO_METRICS_ADDR",
        "metrics_address",
        "--metrics-address",
        "",
        Text,
    ),
    row(
        "RUVIO_SCRIPT_MAX_BYTES",
        "scripts.max_bytes",
        "",
        "script-max-bytes",
        Integer,
    ),
    row(
        "RUVIO_SCRIPT_MAX_CACHED",
        "scripts.max_cached",
        "",
        "script-max-cached",
        Integer,
    ),
    row(
        "RUVIO_SCRIPT_MAX_CALLS",
        "scripts.max_calls",
        "",
        "script-max-calls",
        Integer,
    ),
    row(
        "RUVIO_SCRIPT_MAX_INSTRUCTIONS",
        "scripts.max_instructions",
        "",
        "script-max-instructions",
        Integer,
    ),
    row(
        "RUVIO_SCRIPT_MAX_TIME_MS",
        "scripts.max_time_ms",
        "",
        "script-max-time-ms",
        Integer,
    ),
    row("RUVIO_HOT_KEYS", "hot_keys", "", "", List),
    row(
        "RUVIO_REPLICATION_ROLE",
        "replication.role",
        "",
        "",
        Enum(ROLES),
    ),
    row(
        "RUVIO_REPLICATION_LISTEN",
        "replication.listen",
        "",
        "",
        Text,
    ),
    row(
        "RUVIO_REPLICATION_PRIMARY",
        "replication.primary",
        "",
        "",
        Text,
    ),
    row(
        "RUVIO_MIN_REPLICAS_TO_WRITE",
        "replication.min_replicas_to_write",
        "",
        "min-replicas-to-write",
        Integer,
    ),
    row(
        "RUVIO_MIN_REPLICAS_MAX_LAG",
        "replication.min_replicas_max_lag",
        "",
        "min-replicas-max-lag",
        Integer,
    ),
    row(
        "RUVIO_PRIMARY_LEASE",
        "replication.primary_lease",
        "",
        "primary-lease",
        Boolean,
    ),
    row(
        "RUVIO_PRIMARY_LEASE_COORDINATORS",
        "replication.primary_lease_coordinators",
        "",
        "",
        List,
    ),
    row(
        "RUVIO_REPLICA_ANNOUNCED",
        "replication.announced",
        "",
        "",
        Boolean,
    ),
    // Only in the file.
    row("", "replication.tls", "", "", Boolean),
    row("", "replication.tls_ca", "", "", Text),
    row("", "replication.tls_client_cert", "", "", Text),
    row("", "replication.tls_client_key", "", "", Text),
    row("", "security.users", "", "", List),
];

impl Kind {
    const fn name(self) -> &'static str {
        match self {
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Text => "string",
            Self::List => "list",
            Self::Enum(_) => "enum",
        }
    }
}

/// The schema as pretty JSON: `{"settings": [...]}`, one object per setting, `null` for a
/// name the setting does not have. `default` is the value in the file a first start writes
/// (`ruvio.example.toml`), `null` when that file leaves the setting unset.
pub fn schema_json() -> String {
    let defaults: toml::Table = toml::from_str(DEFAULT_CONFIG).expect("the default config parses");
    let name = |value: &str| (!value.is_empty()).then(|| value.to_owned());
    let settings: Vec<serde_json::Value> = SETTINGS
        .iter()
        .map(|setting| {
            let values: &[&str] = match setting.kind {
                Kind::Enum(values) => values,
                _ => &[],
            };

            serde_json::json!({
                "env": name(setting.env),
                "file": name(setting.file),
                "flag": name(setting.flag),
                "config_set": name(setting.config_set),
                "type": setting.kind.name(),
                "values": values,
                "default": default_value(&defaults, setting.file),
            })
        })
        .collect();
    let mut text = serde_json::to_string_pretty(&serde_json::json!({ "settings": settings }))
        .expect("the schema serializes");

    text.push('\n');
    text
}

fn default_value(defaults: &toml::Table, file: &str) -> serde_json::Value {
    let mut table = defaults;
    let mut parts = file.split('.').peekable();

    while let Some(part) = parts.next() {
        match table.get(part) {
            Some(toml::Value::Table(inner)) if parts.peek().is_some() => table = inner,
            Some(value) if parts.peek().is_none() => {
                return serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
            }
            _ => break,
        }
    }

    serde_json::Value::Null
}
