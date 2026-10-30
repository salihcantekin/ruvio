use std::{io, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about = "Ruvio key-value server")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    shards: Option<usize>,
    #[arg(long)]
    advertised_host: Option<String>,
    #[arg(long)]
    advertised_port: Option<u16>,
    #[arg(long)]
    cpu_set: Option<String>,
    #[arg(long)]
    max_connections: Option<usize>,
    #[arg(long)]
    max_connections_per_ip: Option<usize>,
    #[arg(long)]
    max_connections_per_user: Option<usize>,
    #[arg(long)]
    max_commands_per_second: Option<u64>,
    #[arg(long)]
    max_queued_requests: Option<usize>,
    #[arg(long)]
    max_output_buffer_bytes: Option<usize>,
    #[arg(long)]
    idle_timeout_ms: Option<u64>,
    #[arg(long)]
    read_timeout_ms: Option<u64>,
    #[arg(long)]
    write_timeout_ms: Option<u64>,
    #[arg(long)]
    shutdown_grace_ms: Option<u64>,
    #[arg(long)]
    durability: Option<String>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Serve a recoverable but incomplete replica as a writer, accepting potential data loss.
    #[arg(long)]
    force_promote_incomplete: bool,
    /// Write the effective settings (CLI flags and environment included) back into the config file.
    #[arg(long)]
    write_config: bool,
    #[arg(long)]
    wal_segment_bytes: Option<u64>,
    #[arg(long)]
    wal_sync_interval_ms: Option<u64>,
    #[arg(long)]
    snapshot_max_bytes: Option<u64>,
    #[arg(long)]
    snapshot_journal_bytes: Option<usize>,
    #[arg(long)]
    snapshot_interval_ms: Option<u64>,
    #[arg(long)]
    snapshot_min_mutations: Option<u64>,
    #[arg(long)]
    requirepass: Option<String>,
    #[arg(long, action = clap::ArgAction::Set)]
    protected_mode: Option<bool>,
    #[arg(long)]
    maxmemory: Option<usize>,
    #[arg(long)]
    max_shard_bytes: Option<usize>,
    #[arg(long)]
    memory_headroom_bytes: Option<usize>,
    #[arg(long)]
    maxmemory_policy: Option<String>,
    #[arg(long)]
    maxmemory_samples: Option<usize>,
    #[arg(long)]
    active_expire_keys: Option<usize>,
    #[arg(long)]
    active_expire_maintenance_keys: Option<usize>,
    #[arg(long)]
    hash_max_compact_entries: Option<usize>,
    #[arg(long)]
    hash_max_compact_value_bytes: Option<usize>,
    #[arg(long)]
    set_max_intset_entries: Option<usize>,
    #[arg(long)]
    list_max_compact_entries: Option<usize>,
    #[arg(long)]
    list_max_compact_value_bytes: Option<usize>,
    #[arg(long)]
    metrics_address: Option<String>,
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    #[arg(long)]
    tls_key: Option<PathBuf>,
    /// PEM issuer for optional TLS client certificates.
    #[arg(long)]
    tls_client_ca: Option<PathBuf>,
    /// Reject a TLS handshake that does not present a certificate from `--tls-client-ca`.
    #[arg(long, default_value_t = false)]
    tls_client_auth: bool,
    /// Print Ruvio's license and the third-party notices embedded in this binary.
    #[arg(long, default_value_t = false)]
    licenses: bool,
    /// Check the settings (file, environment, flags and the cluster_topology map) and exit
    /// without opening a listener or writing a file.
    #[arg(long, default_value_t = false)]
    check_config: bool,
    /// Print every setting (variable, file key, flag, type, default) as JSON and exit.
    #[arg(long, default_value_t = false)]
    print_config_schema: bool,
    #[command(subcommand)]
    mode: Option<Mode>,
}

#[derive(Debug, Subcommand)]
enum Mode {
    /// Run a coordinator: watch replication groups and fail them over automatically. It holds
    /// no data; run three or five of them.
    #[cfg(feature = "coordinator")]
    Coordinator {
        /// The coordinator's TOML file (see docs/COORDINATOR.md).
        #[arg(long)]
        config: PathBuf,
        /// Check the file and exit; for a cluster, print the map the data nodes start with.
        #[arg(long, default_value_t = false)]
        check: bool,
    },
}

#[cfg(feature = "mimalloc-allocator")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// mimalloc asks the kernel for transparent huge pages on its arenas. A key-value server
/// holding many small values then pays a 2 MiB page wherever a few bytes landed: on a small
/// data set RSS was 2-3x the live bytes. Redis turns THP off
/// for itself the same way (`disable-thp yes`). `MIMALLOC_ALLOW_THP=1` keeps it on.
///
/// mimalloc reads its options and reserves its first arena before `main`, so setting
/// `mi_option_allow_thp` here would come too late; `PR_SET_THP_DISABLE` applies to every
/// later page fault of the process, including in arenas already reserved.
#[cfg(target_os = "linux")]
fn tune_allocator() {
    unsafe extern "C" {
        fn prctl(option: std::ffi::c_int, ...) -> std::ffi::c_int;
    }

    const PR_SET_THP_DISABLE: std::ffi::c_int = 41;

    if std::env::var_os("MIMALLOC_ALLOW_THP").is_some_and(|value| value != "0") {
        return;
    }

    // SAFETY: prctl with PR_SET_THP_DISABLE only sets a flag on this process; a kernel
    // without it returns an error, which changes nothing.
    unsafe {
        prctl(
            PR_SET_THP_DISABLE,
            1 as std::ffi::c_ulong,
            0 as std::ffi::c_ulong,
            0 as std::ffi::c_ulong,
            0 as std::ffi::c_ulong,
        )
    };
}

#[cfg(not(target_os = "linux"))]
fn tune_allocator() {}

fn main() -> io::Result<()> {
    tune_allocator();
    let log_pump = ruvio::init_process_logging();
    let result = run();

    drop(log_pump);
    result
}

fn run() -> io::Result<()> {
    let cli = Cli::parse();

    if cli.licenses {
        print!("{}", ruvio::THIRD_PARTY_NOTICES);

        return Ok(());
    }

    if cli.print_config_schema {
        print!("{}", ruvio::config::schema::schema_json());

        return Ok(());
    }

    #[cfg(feature = "coordinator")]
    if let Some(Mode::Coordinator { config, check }) = &cli.mode {
        return run_coordinator(config, *check);
    }

    let config_path = ruvio::config::resolve_config_path(cli.config.as_deref());
    let check = cli.check_config;
    // A check reads the defaults in place of a missing file it was not pointed at, and
    // creates nothing.
    let file_missing = check && cli.config.is_none() && !config_path.exists();

    if !check {
        ruvio::config::ensure_config_file(&config_path).map_err(invalid_config)?;
    }

    let mut config = ruvio::AppConfig {
        config_file: Some(config_path.clone()),
        ..ruvio::AppConfig::default()
    };
    let file_text = if file_missing {
        String::new()
    } else {
        std::fs::read_to_string(&config_path).map_err(|error| {
            invalid_config(format!(
                "failed to read config {}: {error}",
                config_path.display()
            ))
        })?
    };

    config.apply_toml(&file_text).map_err(invalid_config)?;

    let loaded = config.clone();

    config
        .apply_environment(|name| std::env::var(name).ok())
        .map_err(invalid_config)?;
    let force_promote_incomplete = cli.force_promote_incomplete;
    let write_config = cli.write_config;
    apply_cli(&mut config, cli).map_err(invalid_config)?;

    for note in config.connections.clamp_to_global() {
        tracing::warn!("{note}");
    }

    config.resolve_sync_worker();
    config.validate().map_err(invalid_config)?;
    operational_checks(&config).map_err(invalid_config)?;
    if check {
        return check_report(&config, (!file_missing).then_some(config_path.as_path()));
    }
    if write_config {
        config
            .write_overrides(&config_path, &loaded)
            .map_err(invalid_config)?;
    }

    tracing::info!(path = %config_path.display(), "configuration file");
    let shutdown = ruvio::ShutdownToken::default();
    let signal_token = shutdown.clone();

    // SIGINT and SIGTERM drain and stop the server.
    ctrlc::set_handler(move || signal_token.request())
        .map_err(|error| io::Error::other(format!("failed to install signal handler: {error}")))?;
    #[cfg(unix)]
    ignore_hangup();

    ruvio::runtime::evented::run_with_force(&config, shutdown, force_promote_incomplete)
}

/// `--check-config`: the checks a start runs before it opens a listener, plus the
/// `cluster_topology` map, then one line naming what the settings start.
fn check_report(config: &ruvio::AppConfig, file: Option<&std::path::Path>) -> io::Result<()> {
    if let Some(path) = &config.cluster_topology {
        ruvio::topology::Topology::load(path, &config.cluster_group, config.shards)
            .map_err(invalid_config)?;
    }
    let mut line = format!(
        "config ok: {} shard(s) on {}, durability {}",
        config.shards,
        config.address,
        format!("{:?}", config.persistence.profile).to_lowercase()
    );

    if config.replication.role != ruvio::config::ReplicationRole::Off {
        line.push_str(&format!(
            ", replication {}",
            format!("{:?}", config.replication.role).to_lowercase()
        ));
    }
    if config.cluster_topology.is_some() {
        line.push_str(&format!(", cluster group {}", config.cluster_group));
    }
    match file {
        Some(path) => line.push_str(&format!(" (file {})", path.display())),
        None => line.push_str(" (no file: built-in defaults)"),
    }
    println!("{line}");

    Ok(())
}

#[cfg(feature = "coordinator")]
fn run_coordinator(path: &std::path::Path, check: bool) -> io::Result<()> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| invalid_config(format!("failed to read {}: {error}", path.display())))?;
    let config = ruvio::coordinator::CoordinatorConfig::parse(&text).map_err(invalid_config)?;

    if check {
        println!(
            "coordinator config ok: id {}, {} peer(s), {} group(s)",
            config.id,
            config.peers.len(),
            config.groups.len()
        );
        // The map every data node of a cluster starts with (`cluster_topology`).
        if let Some(map) = config.initial_cluster_map() {
            print!("{map}");
        }

        return Ok(());
    }
    let shutdown = ruvio::ShutdownToken::default();
    let signal_token = shutdown.clone();

    ctrlc::set_handler(move || signal_token.request())
        .map_err(|error| io::Error::other(format!("failed to install signal handler: {error}")))?;
    #[cfg(unix)]
    ignore_hangup();

    ruvio::coordinator::run(config, shutdown)
}

/// `ctrlc`'s `termination` feature also routes SIGHUP to the shutdown handler, so a closed
/// terminal or a reload-style `kill -HUP` would stop the server. Ruvio has no reload, so the
/// signal is ignored.
#[cfg(unix)]
#[allow(unsafe_code)]
fn ignore_hangup() {
    // SAFETY: `SIG_IGN` installs no handler code; this only changes the signal's disposition.
    unsafe {
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
    }
}

fn apply_cli(config: &mut ruvio::AppConfig, cli: Cli) -> Result<(), String> {
    config.apply_overrides(ruvio::ConfigOverrides {
        address: cli.address,
        shards: cli.shards,
        advertised_host: cli.advertised_host,
        advertised_port: cli.advertised_port,
        cpu_set: cli.cpu_set,
        shutdown_grace: cli.shutdown_grace_ms.map(Duration::from_millis),
    });

    if let Some(value) = cli.max_connections {
        config.connections.max_global = value;
    }

    if let Some(value) = cli.max_connections_per_ip {
        config.connections.max_per_ip = value;
    }

    if let Some(value) = cli.max_connections_per_user {
        config.connections.max_per_user = value;
    }

    if let Some(value) = cli.max_commands_per_second {
        config.connections.max_commands_per_second = value;
    }

    if let Some(value) = cli.max_queued_requests {
        config.requests.max_queued_requests = value;
    }

    if let Some(value) = cli.max_output_buffer_bytes {
        config.requests.max_output_buffer_bytes = value;
    }

    if let Some(value) = cli.idle_timeout_ms {
        config.timeouts.idle = Duration::from_millis(value);
    }

    if let Some(value) = cli.read_timeout_ms {
        config.timeouts.read = Duration::from_millis(value);
    }

    if let Some(value) = cli.write_timeout_ms {
        config.timeouts.write = Duration::from_millis(value);
    }

    if let Some(value) = cli.durability {
        config.persistence.profile = value.parse().map_err(str::to_owned)?;
    }

    if let Some(value) = cli.data_dir {
        config.persistence.data_dir = value;
    }

    if let Some(value) = cli.wal_segment_bytes {
        config.persistence.wal_segment_bytes = value;
    }

    if let Some(value) = cli.wal_sync_interval_ms {
        config.persistence.sync_interval = Duration::from_millis(value);
    }

    if let Some(value) = cli.snapshot_max_bytes {
        config.persistence.snapshot_max_bytes = value;
    }

    if let Some(value) = cli.snapshot_journal_bytes {
        config.persistence.snapshot_journal_bytes = value;
    }

    if let Some(value) = cli.snapshot_interval_ms {
        config.persistence.snapshot_interval = Duration::from_millis(value);
    }

    if let Some(value) = cli.snapshot_min_mutations {
        config.persistence.snapshot_min_mutations = value;
    }

    if let Some(value) = cli.requirepass {
        config.security.requirepass = value;
    }

    if let Some(value) = cli.protected_mode {
        config.security.protected_mode = value;
    }

    if let Some(value) = cli.maxmemory {
        config.memory.max_process_bytes = value;
    }

    if let Some(value) = cli.max_shard_bytes {
        config.memory.max_shard_bytes = value;
    }

    if let Some(value) = cli.memory_headroom_bytes {
        config.memory.reserved_headroom_bytes = value;
    }

    if let Some(value) = cli.maxmemory_policy {
        config.memory.eviction_policy = value.parse()?;
    }

    if let Some(value) = cli.maxmemory_samples {
        config.memory.maxmemory_samples = value;
    }

    if let Some(value) = cli.active_expire_keys {
        config.memory.active_expire_keys = value;
    }

    if let Some(value) = cli.active_expire_maintenance_keys {
        config.memory.active_expire_maintenance_keys = value;
    }

    if let Some(value) = cli.hash_max_compact_entries {
        config.memory.hash_max_compact_entries = value;
    }

    if let Some(value) = cli.set_max_intset_entries {
        config.memory.set_max_intset_entries = value;
    }

    if let Some(value) = cli.list_max_compact_entries {
        config.memory.list_max_compact_entries = value;
    }

    if let Some(value) = cli.list_max_compact_value_bytes {
        config.memory.list_max_compact_value_bytes = value;
    }

    if let Some(value) = cli.hash_max_compact_value_bytes {
        config.memory.hash_max_compact_value_bytes = value;
    }

    if let Some(value) = cli.metrics_address {
        config.metrics_address = if value.is_empty() { None } else { Some(value) };
    }

    if let Some(value) = cli.tls_cert {
        config.security.tls_cert = value;
    }

    if let Some(value) = cli.tls_key {
        config.security.tls_key = value;
    }

    if let Some(value) = cli.tls_client_ca {
        config.security.tls_client_ca = value;
    }

    if cli.tls_client_auth {
        config.security.tls_client_auth = true;
    }

    Ok(())
}

fn operational_checks(config: &ruvio::AppConfig) -> Result<(), String> {
    if config.metrics_address.is_some() {
        tracing::warn!(
            "metrics_address is set but no HTTP metrics endpoint is served yet; read metrics with INFO prometheus"
        );
    }

    if !ruvio::bind_is_loopback(&config.address) {
        if config.security.authentication_enabled() {
            tracing::warn!(
                address = %config.address,
                "binding a non-loopback address with authentication enabled"
            );
        } else if config.security.protected_mode {
            tracing::warn!(
                address = %config.address,
                requirepass = config.security.redacted_requirepass(),
                "public bind without AUTH; protected mode rejects non-loopback clients"
            );
        } else {
            tracing::warn!(
                address = %config.address,
                "unauthenticated public listener allowed by RUVIO_ALLOW_UNAUTHENTICATED"
            );
        }
    }

    if config.security.tls_enabled() {
        ruvio::TlsAcceptor::from_security(&config.security).map_err(|error| error.to_string())?;

        if !config.security.authentication_enabled() && !ruvio::bind_is_loopback(&config.address) {
            tracing::warn!("tls is enabled without requirepass on a non-loopback bind");
        }

        if config.replication.leaves_the_host() {
            tracing::warn!(
                "tls covers client connections only: replication traffic, including the replication password, is not encrypted; set replication.tls or keep it on a private network or a tunnel"
            );
        }
    }

    if config.persistence.profile != ruvio::DurabilityProfile::Memory {
        std::fs::create_dir_all(&config.persistence.data_dir).map_err(|error| {
            format!(
                "data directory {} is not writable: {error}",
                config.persistence.data_dir.display()
            )
        })?;

        let probe = config.persistence.data_dir.join(".ruvio-write-probe");

        std::fs::write(&probe, b"ok").map_err(|error| {
            format!(
                "data directory {} is not writable: {error}",
                config.persistence.data_dir.display()
            )
        })?;

        let _ = std::fs::remove_file(&probe);
    }

    #[cfg(target_os = "linux")]
    if let Some(limit) = linux_open_file_limit() {
        let needed = config
            .connections
            .max_global
            .saturating_add(config.shards + 16);

        if limit < needed as u64 {
            tracing::warn!(
                max_open_files = limit,
                needed,
                "file descriptor limit is lower than the configured connection budget"
            );
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_open_file_limit() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/limits").ok()?;

    for line in text.lines() {
        if line.starts_with("Max open files") {
            let mut parts = line.split_whitespace();
            let _ = parts.next();
            let _ = parts.next();
            let _ = parts.next();

            return parts.next()?.parse().ok();
        }
    }

    None
}

fn invalid_config(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    /// A flag missing from `docs/CONFIGURATION.md` is a flag nobody finds.
    #[test]
    fn every_command_line_flag_is_documented() {
        let docs = include_str!("../../docs/CONFIGURATION.md");
        let command = super::Cli::command();
        let missing: Vec<String> = command
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .map(|long| format!("--{long}"))
            .filter(|flag| !docs.contains(&format!("`{flag}`")))
            .collect();

        assert!(command.get_arguments().count() > 40);
        assert!(
            missing.is_empty(),
            "flags missing from docs/CONFIGURATION.md: {missing:?}"
        );
    }

    /// The schema's flags are this binary's flags, and every flag that sets a setting is in
    /// the schema.
    #[test]
    fn the_config_schema_names_every_setting_flag() {
        let flags: Vec<String> = super::Cli::command()
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .map(|long| format!("--{long}"))
            .collect();
        let listed: Vec<&str> = ruvio::config::schema::SETTINGS
            .iter()
            .map(|setting| setting.flag)
            .filter(|flag| !flag.is_empty())
            .collect();
        // Flags that run a mode or act on the file instead of setting a value.
        let actions = [
            "--config",
            "--force-promote-incomplete",
            "--write-config",
            "--licenses",
            "--check-config",
            "--print-config-schema",
        ];

        for flag in &listed {
            assert!(
                flags.iter().any(|known| known == flag),
                "{flag} is not a flag"
            );
        }
        for flag in &flags {
            assert!(
                actions.contains(&flag.as_str()) || listed.contains(&flag.as_str()),
                "{flag} is missing from the config schema"
            );
        }
    }
}
