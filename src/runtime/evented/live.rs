//! `CONFIG SET` and `CONFIG REWRITE` on the evented runtime.
//!
//! The receiving connection checks the change once (`AppConfig::with_live_changes`), applies
//! it to its own shard and sends the new configuration to every other shard as one shared
//! `ConfigUpdate` over the fabric (`Forwarding::apply_everywhere`). Each shard applies it on its own thread, so no lock is
//! taken on the command path; `OK` goes out once every shard has. Updates carry the whole
//! live configuration and a version taken under the fabric's lock: a shard that sees an
//! older version after a newer one keeps the newer one.

use super::*;

use super::forward::ShardChange;

/// The settings `CONFIG SET` changes, as this shard runs them.
pub(super) struct LiveConfig {
    /// Read for every command: one load each, as the plain fields they replaced.
    pub(super) slowlog_threshold_micros: Cell<i64>,
    pub(super) key_stats: Cell<bool>,
    pub(super) latency_histograms: Cell<bool>,
    /// Read for every forwarded request and every `always` durability wait.
    pub(super) max_output_buffer_bytes: Cell<usize>,
    pub(super) write_timeout: Cell<Duration>,
    /// The whole running configuration: what `CONFIG GET` shows, `CONFIG REWRITE` writes
    /// and new connections start with.
    current: RefCell<Arc<AppConfig>>,
    version: Cell<u64>,
}

impl LiveConfig {
    pub(super) fn new(config: &AppConfig) -> Self {
        Self {
            slowlog_threshold_micros: Cell::new(config.slowlog_threshold_micros),
            key_stats: Cell::new(config.key_stats),
            latency_histograms: Cell::new(config.latency_histograms),
            max_output_buffer_bytes: Cell::new(config.requests.max_output_buffer_bytes),
            write_timeout: Cell::new(config.timeouts.write),
            current: RefCell::new(Arc::new(config.clone())),
            version: Cell::new(0),
        }
    }

    pub(super) fn current(&self) -> Arc<AppConfig> {
        Arc::clone(&self.current.borrow())
    }

    /// A value read from the running configuration, for paths that run once per
    /// connection, script or slow command.
    pub(super) fn with<R>(&self, read: impl FnOnce(&AppConfig) -> R) -> R {
        read(&self.current.borrow())
    }
}

/// The process-wide side of `CONFIG SET`, kept by the fabric: the newest configuration and
/// its version. Taken only by `CONFIG SET`.
#[derive(Default)]
pub(super) struct ConfigAuthority {
    version: u64,
    current: Option<Arc<AppConfig>>,
    /// The single-port acceptor's limiter, which is not on any shard.
    acceptor: Option<ConnectionLimiter>,
}

impl ConfigAuthority {
    pub(super) fn register_acceptor(&mut self, limiter: ConnectionLimiter) {
        self.acceptor = Some(limiter);
    }
}

/// One `CONFIG SET` for the other shards.
struct ConfigUpdate {
    version: u64,
    config: Arc<AppConfig>,
}

impl ShardChange for ConfigUpdate {
    fn apply(&self, state: &ShardState) {
        apply(state, &self.config, self.version);
    }
}

/// Runs `config` on this shard, unless the shard already runs a newer version.
fn apply(state: &ShardState, config: &Arc<AppConfig>, version: u64) {
    let live = &state.live;

    if version <= live.version.get() {
        return;
    }

    live.version.set(version);
    live.slowlog_threshold_micros
        .set(config.slowlog_threshold_micros);
    live.key_stats.set(config.key_stats);
    live.latency_histograms.set(config.latency_histograms);
    live.max_output_buffer_bytes
        .set(config.requests.max_output_buffer_bytes);
    live.write_timeout.set(config.timeouts.write);
    state
        .store
        .borrow_mut()
        .apply_memory_limits(config.shard_memory_limits());
    state.slowlog.borrow_mut().trim(config.slowlog_max_len);
    state.limiter.set_limits(config.connections);
    *live.current.borrow_mut() = Arc::clone(config);
}

/// `CONFIG SET name value [name value ...]`.
pub(super) async fn config_set(state: &ShardState, pairs: &[(&[u8], &[u8])]) -> Response<'static> {
    let update = {
        let mut authority = state.forwarding.as_ref().map(|forwarding| {
            forwarding
                .fabric
                .config
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        });
        let base = authority
            .as_ref()
            .and_then(|authority| authority.current.clone())
            .unwrap_or_else(|| state.live.current());
        let changed = match base.with_live_changes(pairs) {
            Ok(changed) => Arc::new(changed),
            Err(error) => return Response::OwnedError(format!("ERR {error}").into_bytes()),
        };

        if changed.security.requirepass != base.security.requirepass
            && let Err(error) = set_default_password(state, &changed.security.requirepass)
        {
            return Response::OwnedError(format!("ERR {error}").into_bytes());
        }

        let version = match authority.as_deref_mut() {
            Some(authority) => {
                authority.version += 1;
                authority.current = Some(Arc::clone(&changed));
                if let Some(acceptor) = &authority.acceptor {
                    acceptor.set_limits(changed.connections);
                }
                authority.version
            }
            None => state.live.version.get() + 1,
        };

        apply(state, &changed, version);
        ConfigUpdate {
            version,
            config: changed,
        }
    };

    if let Some(forwarding) = state.forwarding.as_ref() {
        forwarding.apply_everywhere(Box::new(update)).await;
    }

    Response::Simple(b"OK")
}

/// `requirepass` is the default user's password, as in Redis: setting it replaces the
/// user's passwords in the process-wide ACL directory, and an empty value makes the user
/// `nopass`. Connections already logged in stay logged in.
fn set_default_password(state: &ShardState, password: &str) -> Result<(), String> {
    let rule = format!(">{password}");
    let rules: [&[u8]; 2] = if password.is_empty() {
        [b"resetpass", b"nopass"]
    } else {
        [b"resetpass", rule.as_bytes()]
    };

    state.security.directory().set_user(b"default", &rules)
}

/// `CONFIG REWRITE`: writes this shard's live settings (every shard runs the same ones)
/// into the config file the server started from.
pub(super) fn config_rewrite(state: &ShardState) -> Response<'static> {
    match state.live.current().rewrite_live_settings() {
        Ok(()) => Response::Simple(b"OK"),
        Err(error) => Response::OwnedError(format!("ERR {error}").into_bytes()),
    }
}
