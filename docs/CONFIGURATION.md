# Configuration

Ruvio resolves settings in this order:

1. built-in defaults,
2. a TOML file passed with `--config`,
3. `RUVIO_*` environment variables,
4. explicit command-line options.

All merged settings and cross-field relationships are validated before a
listener is opened. Unknown TOML fields are rejected. Which settings also
change while the server runs, and under which `CONFIG SET` name, is listed in
[Changing settings at runtime](#changing-settings-at-runtime) and in the
`CONFIG SET` column of the [environment table](#environment-variables).

```toml
address = "127.0.0.1:6379"
shards = 1
port_mode = "per-shard"  # shards > 1: "per-shard" (default) = port + i per shard and cluster discovery; "single" (deprecated) = one port only. Both forward a key's command to its owning shard
advertised_host = "127.0.0.1"  # host in CLUSTER SLOTS/SHARDS/NODES
# advertised_port = 6379       # shard 0's announced port; omitted = bind port
hot_keys = []          # Redis globs; empty keeps every key on its shard
shutdown_grace_ms = 30000

# key_stats = true            # RUVIO HOTKEYS (1/1024 sampled, 128 entries) and RUVIO BIGKEYS (SET values >= 1 MiB, last 64)
# slowlog_threshold_micros = 10000  # SLOWLOG: commands slower than this (negative = off, 0 = all); only the generic dispatcher is timed
# slowlog_max_len = 128       # SLOWLOG entries kept per shard
# latency_histograms = true   # per-command latency histograms (INFO latencystats, INFO prometheus); false = off

[protocol]
max_arguments = 1048576
max_key_bytes = 1048576
max_value_bytes = 16777216
max_command_bytes = 67108864
max_pipeline_commands = 1024
max_connection_buffer_bytes = 67108864  # byte limits in [protocol] are capped at 2147483648 (2 GiB)
databases = 16                # SELECT 0..databases-1; 1 to 256

[timeouts]
idle_ms = 300000
read_ms = 30000
write_ms = 30000

[connections]
max_global = 10000
max_per_ip = 256              # lowered to max_global at startup when it is higher, with a warning
max_per_user = 0              # 0 disables the per-ACL-user connection cap; a higher value is lowered to max_global
max_commands_per_second = 0   # 0 disables the per-connection command rate

[requests]
max_queued_requests = 1024
max_output_buffer_bytes = 67108864   # unsent reply bytes per connection; a pipeline sends replies as it goes

[persistence]
profile = "memory" # memory, snapshot, everysec, or always
data_dir = "./data"
wal_segment_bytes = 268435456
sync_interval_ms = 1000
snapshot_max_bytes = 17179869184
snapshot_journal_bytes = 67108864
snapshot_interval_ms = 60000
snapshot_min_mutations = 0
cdc_retain_bytes = 0
replica_retain_max_bytes = 1073741824

[replication]
role = "off"                  # off, primary, or replica
listen = "127.0.0.1:26379"    # primary base address; shard i uses port + i
primary = "127.0.0.1:26379"   # replica dials this base address; a hostname is allowed
min_replicas_to_write = 0     # a primary refuses writes with fewer good replicas; 0 = off
min_replicas_max_lag = 10     # seconds since a replica's last acknowledgement
announced = true              # a replica tells its primary where clients reach it
tls = false                   # encrypt the replication link; see Replication TLS

[security]
requirepass = ""
protected_mode = true
tls_cert = ""                 # PEM certificate chain; empty disables TLS
tls_key = ""                  # PEM private key; must be set with tls_cert

[[security.users]]
name = "app"
rules = "on >app-secret ~app:* +@read +ping +auth +info"

[memory]
max_shard_bytes = 0          # 0 disables the shard admission budget
max_process_bytes = 0        # 0 disables the process budget
reserved_headroom_bytes = 16777216
maxmemory_policy = "noeviction"
maxmemory_samples = 16       # eviction candidates examined per decision
active_expire_keys = 16      # expired keys reaped before each command
active_expire_maintenance_keys = 16 # keys per expiry-timer batch
hash_max_compact_entries = 64
hash_max_compact_value_bytes = 64
set_max_intset_entries = 512      # integer-only sets up to this size stay a sorted array
list_max_compact_entries = 128    # lists up to this size stay in one packed buffer
list_max_compact_value_bytes = 64

[scripts]
max_bytes = 16384
max_cached = 1024
max_calls = 1024
max_instructions = 1000000   # each shard's Lua state is also capped at 128 MiB of memory
max_time_ms = 1000           # wall-clock limit per script, at most 3600000; see below
```

Use `ruvio --help` for CLI overrides; every environment variable is listed in
[Environment variables](#environment-variables). When `--config` is omitted, the process loads `RUVIO_CONFIG`, then
`ruvio.toml` beside the binary, then `/etc/ruvio/ruvio.toml`, then `./ruvio.toml`.
A missing file is created from the built-in defaults. Environment variables and
CLI flags still win. The file is left as it is unless you pass `--write-config`,
which writes the effective values back into it at startup, so a process started
with `--shards 3` never changes what the next process reads. The file is not
watched: an edit applies only on the next start.
Shard count, bind address, and durability cannot change under a live process;
the settings listed under *Changing settings at runtime* can.

## Changing settings at runtime

`CONFIG SET name value [name value ...]` changes the settings in the table
below while the server runs. Names are the ones `CONFIG GET` shows
(`maxmemory`, `slowlog-log-slower-than`, `max-connections`, `script-max-time-ms`,
`requirepass`, and so on); values follow the same rules as the file and the
environment, so byte counts are plain numbers and booleans are `yes`/`no`.

```text
CONFIG SET maxmemory 1073741824 maxmemory-policy allkeys-lru
CONFIG GET maxmemory*
CONFIG REWRITE
```

These are all the settings that change while the server runs; every other
setting takes a restart.

| `CONFIG SET` name | File setting | Takes effect |
| --- | --- | --- |
| `maxmemory` | `memory.max_process_bytes` | at once |
| `max-shard-bytes` | `memory.max_shard_bytes` | at once |
| `memory-headroom-bytes` | `memory.reserved_headroom_bytes` | at once |
| `maxmemory-policy` | `memory.maxmemory_policy` | at once |
| `maxmemory-samples` | `memory.maxmemory_samples` | next eviction |
| `active-expire-keys` | `memory.active_expire_keys` | next expiry cycle |
| `active-expire-maintenance-keys` | `memory.active_expire_maintenance_keys` | next expiry cycle |
| `hash-max-compact-entries` | `memory.hash_max_compact_entries` | when the collection next grows |
| `hash-max-compact-value-bytes` | `memory.hash_max_compact_value_bytes` | when the collection next grows |
| `set-max-intset-entries` | `memory.set_max_intset_entries` | when the collection next grows |
| `list-max-compact-entries` | `memory.list_max_compact_entries` | when the collection next grows |
| `list-max-compact-value-bytes` | `memory.list_max_compact_value_bytes` | when the collection next grows |
| `notify-keyspace-events` | `notify_keyspace_events` | next write |
| `slowlog-log-slower-than` | `slowlog_threshold_micros` | next command |
| `slowlog-max-len` | `slowlog_max_len` | at once |
| `key-stats` | `key_stats` | next command |
| `latency-histograms` | `latency_histograms` | next command |
| `idle-timeout-ms` | `timeouts.idle_ms` | new connections |
| `read-timeout-ms` | `timeouts.read_ms` | new connections |
| `write-timeout-ms` | `timeouts.write_ms` | new connections |
| `max-connections` | `connections.max_global` | next accept |
| `max-connections-per-ip` | `connections.max_per_ip` | next accept |
| `max-connections-per-user` | `connections.max_per_user` | next login |
| `max-commands-per-second` | `connections.max_commands_per_second` | new connections |
| `max-output-buffer-bytes` | `requests.max_output_buffer_bytes` | new connections |
| `script-max-bytes` | `scripts.max_bytes` | next script |
| `script-max-cached` | `scripts.max_cached` | next script |
| `script-max-calls` | `scripts.max_calls` | next script |
| `script-max-instructions` | `scripts.max_instructions` | next script |
| `script-max-time-ms` | `scripts.max_time_ms` | next script |
| `requirepass` | `security.requirepass` | next login |
| `min-replicas-to-write` | `replication.min_replicas_to_write` | next replica check (100 ms) |
| `min-replicas-max-lag` | `replication.min_replicas_max_lag` | next replica check (100 ms) |

- One call changes all its pairs or none. An unknown name, a name given twice,
  a restart-only name, or a value that does not parse or validate fails the
  whole call and nothing changes. A restart-only name answers
  `restart-only setting; change it in the config file and restart`.
- The connection that receives the call checks it, applies it to its own shard
  and sends it to every other shard; `OK` comes back once every shard runs the
  new values. `CONFIG GET` then shows them on every shard.
- Memory settings (`maxmemory`, `max-shard-bytes`, `memory-headroom-bytes`,
  `maxmemory-policy`, `maxmemory-samples`, the active-expire and compact
  thresholds, `notify-keyspace-events`) apply at once. A lower limit is reached
  by eviction (or refused writes under `noeviction`) as new writes arrive.
  Changing `maxmemory-policy` resets the access bits of every key on each shard
  once, so a policy change on a large shard takes a moment. New compact
  thresholds apply to collections as they next grow; existing ones keep their
  encoding.
- Slowlog, `key-stats` and `latency-histograms` apply to the next command.
  A shorter `slowlog-max-len` drops the oldest entries.
- Connection settings (timeouts, `max-connections*`,
  `max-commands-per-second`, `max-output-buffer-bytes`)
  apply to connections accepted afterwards; open connections keep theirs, as
  they keep their ACL rules.
- Script limits apply to the next `EVAL`, `EVALSHA` or `FCALL`.
- `requirepass` sets the default user's password in the live ACL directory,
  as `ACL SETUSER default resetpass >password` would; an empty value makes the
  default user `nopass`. Clients already logged in stay logged in. When
  `replication_password` is empty the replication port keeps the
  `requirepass` it started with until a restart, so rotate a leaked password
  there with a restart (or set `replication_password`).
- A change is not written to the WAL and not replicated: each node, primary or
  replica, is configured on its own. It is lost on restart unless written to
  the file.
- `CONFIG SET` and `CONFIG REWRITE` are refused inside `MULTI` and scripts and
  need the `config` command in the caller's ACL rules (`@admin`,
  `@dangerous`), as in Redis: a user granted `+config` may run all three.
  `-config|set` or `-config|rewrite` denies only that subcommand; a user that
  should only read settings gets `+config|get` or `+config -config|set
  -config|rewrite`.

`CONFIG REWRITE` writes the live settings into the config file the server
started from, through the same code as `--write-config`: other keys stay,
comments are dropped, and the file stays owner-only. Restart-only settings
keep what the file says even when the process runs with an environment or
command-line value for them. A live value that came from the environment is
written, so a secret such as `requirepass` from `RUVIO_REQUIREPASS` lands in
the file. `CONFIG SET` never writes the file by itself.

Persistence timing (`snapshot_interval_ms`, `snapshot_min_mutations`,
`wal_sync_interval_ms`, `replica_retain_max_bytes`) is restart-only for now.

## Environment variables

Each variable overrides one file setting, and a command-line flag, where there is one, overrides the variable. Booleans accept `1`/`0`, `true`/`false`, `yes`/`no` and `on`/`off`; durations are milliseconds.

| Variable | `CONFIG SET` | Flag | File setting | Notes |
| --- | --- | --- | --- | --- |
| `RUVIO_ADDR` | no | `--address` | `address` | |
| `RUVIO_SHARDS` | no | `--shards` | `shards` | |
| `RUVIO_PORT_MODE` | no | | `port_mode` | |
| `RUVIO_ADVERTISE_HOST` | no | `--advertised-host` | `advertised_host` | |
| `RUVIO_ADVERTISE_PORT` | no | `--advertised-port` | `advertised_port` | |
| `RUVIO_CLUSTER_TOPOLOGY` | no | | `cluster_topology` | Path of the cluster map; empty = none |
| `RUVIO_CLUSTER_GROUP` | no | | `cluster_group` | This node's group in the map |
| `RUVIO_MIGRATION_MAX_BYTES_PER_SEC` | no | | `migration_max_bytes_per_sec` | Copy rate of a range moving to another node; 0 = no limit |
| `RUVIO_CPUSET` | no | `--cpu-set` | `cpu_set` | |
| `RUVIO_CORE` | no | | `cpu_set` | Older name; read only when `RUVIO_CPUSET` is unset |
| `RUVIO_SHUTDOWN_GRACE_MS` | no | `--shutdown-grace-ms` | `shutdown_grace_ms` | |
| `RUVIO_SLOWLOG_THRESHOLD_MICROS` | `slowlog-log-slower-than` | | `slowlog_threshold_micros` | |
| `RUVIO_SLOWLOG_MAX_LEN` | `slowlog-max-len` | | `slowlog_max_len` | |
| `RUVIO_LATENCY_HISTOGRAMS` | `latency-histograms` | | `latency_histograms` | |
| `RUVIO_KEY_STATS` | `key-stats` | | `key_stats` | |
| `RUVIO_MAX_ARGUMENTS` | no | | `protocol.max_arguments` | |
| `RUVIO_MAX_KEY_BYTES` | no | | `protocol.max_key_bytes` | |
| `RUVIO_MAX_VALUE_BYTES` | no | | `protocol.max_value_bytes` | |
| `RUVIO_MAX_COMMAND_BYTES` | no | | `protocol.max_command_bytes` | |
| `RUVIO_MAX_PIPELINE_COMMANDS` | no | | `protocol.max_pipeline_commands` | |
| `RUVIO_MAX_CONNECTION_BUFFER_BYTES` | no | | `protocol.max_connection_buffer_bytes` | |
| `RUVIO_DATABASES` | no | | `protocol.databases` | |
| `RUVIO_IDLE_TIMEOUT_MS` | `idle-timeout-ms` | `--idle-timeout-ms` | `timeouts.idle_ms` | |
| `RUVIO_READ_TIMEOUT_MS` | `read-timeout-ms` | `--read-timeout-ms` | `timeouts.read_ms` | |
| `RUVIO_WRITE_TIMEOUT_MS` | `write-timeout-ms` | `--write-timeout-ms` | `timeouts.write_ms` | |
| `RUVIO_MAX_CONNECTIONS` | `max-connections` | `--max-connections` | `connections.max_global` | |
| `RUVIO_MAX_CONNECTIONS_PER_IP` | `max-connections-per-ip` | `--max-connections-per-ip` | `connections.max_per_ip` | |
| `RUVIO_MAX_CONNECTIONS_PER_USER` | `max-connections-per-user` | `--max-connections-per-user` | `connections.max_per_user` | |
| `RUVIO_MAX_COMMANDS_PER_SECOND` | `max-commands-per-second` | `--max-commands-per-second` | `connections.max_commands_per_second` | |
| `RUVIO_MAX_QUEUED_REQUESTS` | no | `--max-queued-requests` | `requests.max_queued_requests` | |
| `RUVIO_MAX_OUTPUT_BUFFER_BYTES` | `max-output-buffer-bytes` | `--max-output-buffer-bytes` | `requests.max_output_buffer_bytes` | |
| `RUVIO_DURABILITY` | no | `--durability` | `persistence.profile` | `memory`, `snapshot`, `everysec` or `always` |
| `RUVIO_DATA_DIR` | no | `--data-dir` | `persistence.data_dir` | |
| `RUVIO_WAL_SEGMENT_BYTES` | no | `--wal-segment-bytes` | `persistence.wal_segment_bytes` | |
| `RUVIO_WAL_SYNC_INTERVAL_MS` | no | `--wal-sync-interval-ms` | `persistence.sync_interval_ms` | |
| `RUVIO_EXPERIMENTAL_SYNC_WORKER` | no | | `persistence.sync_worker` | Older boolean form (`true` = `on`); `RUVIO_SYNC_WORKER` wins |
| `RUVIO_SYNC_WORKER` | no | | `persistence.sync_worker` | |
| `RUVIO_WAL_PREFILL` | no | | | `auto` (default), `on` or `off`. Linux only: zero-fill WAL segments ahead of the cursor on a helper thread. `auto` does it for `always` only; under `everysec` the zero-fill doubled the bytes a throughput-capped disk saw and halved SET throughput. `CONFIG GET wal-prefill` shows the mode |
| `RUVIO_SNAPSHOT_MAX_BYTES` | no | `--snapshot-max-bytes` | `persistence.snapshot_max_bytes` | |
| `RUVIO_SNAPSHOT_JOURNAL_BYTES` | no | `--snapshot-journal-bytes` | `persistence.snapshot_journal_bytes` | |
| `RUVIO_SNAPSHOT_INTERVAL_MS` | no | `--snapshot-interval-ms` | `persistence.snapshot_interval_ms` | |
| `RUVIO_SNAPSHOT_MIN_MUTATIONS` | no | `--snapshot-min-mutations` | `persistence.snapshot_min_mutations` | |
| `RUVIO_CDC_RETAIN_BYTES` | no | | `persistence.cdc_retain_bytes` | |
| `RUVIO_REPLICA_RETAIN_MAX_BYTES` | no | | `persistence.replica_retain_max_bytes` | |
| `RUVIO_REQUIREPASS` | `requirepass` | `--requirepass` | `security.requirepass` | |
| `RUVIO_TLS_CERT` | no | `--tls-cert` | `security.tls_cert` | |
| `RUVIO_TLS_KEY` | no | `--tls-key` | `security.tls_key` | |
| `RUVIO_TLS_CLIENT_CA` | no | `--tls-client-ca` | `security.tls_client_ca` | |
| `RUVIO_TLS_CLIENT_AUTH` | no | `--tls-client-auth` | `security.tls_client_auth` | |
| `RUVIO_ACL_FILE` | no | | `security.acl_file` | |
| `RUVIO_PROTECTED_MODE` | no | `--protected-mode` | `security.protected_mode` | |
| `RUVIO_ALLOW_UNAUTHENTICATED` | no | | `security.allow_unauthenticated_public` | |
| `RUVIO_METRICS_TOKEN` | no | | `security.metrics_token` | |
| `RUVIO_REPLICATION_PASSWORD` | no | | `security.replication_password` | |
| `RUVIO_MAXMEMORY` | `maxmemory` | `--maxmemory` | `memory.max_process_bytes` | |
| `RUVIO_MAX_SHARD_BYTES` | `max-shard-bytes` | `--max-shard-bytes` | `memory.max_shard_bytes` | |
| `RUVIO_MEMORY_HEADROOM_BYTES` | `memory-headroom-bytes` | `--memory-headroom-bytes` | `memory.reserved_headroom_bytes` | |
| `RUVIO_MAXMEMORY_POLICY` | `maxmemory-policy` | `--maxmemory-policy` | `memory.maxmemory_policy` | |
| `RUVIO_MAXMEMORY_SAMPLES` | `maxmemory-samples` | `--maxmemory-samples` | `memory.maxmemory_samples` | |
| `RUVIO_ACTIVE_EXPIRE_KEYS` | `active-expire-keys` | `--active-expire-keys` | `memory.active_expire_keys` | |
| `RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS` | `active-expire-maintenance-keys` | `--active-expire-maintenance-keys` | `memory.active_expire_maintenance_keys` | |
| `RUVIO_HASH_MAX_COMPACT_ENTRIES` | `hash-max-compact-entries` | `--hash-max-compact-entries` | `memory.hash_max_compact_entries` | |
| `RUVIO_HASH_MAX_COMPACT_VALUE_BYTES` | `hash-max-compact-value-bytes` | `--hash-max-compact-value-bytes` | `memory.hash_max_compact_value_bytes` | |
| `RUVIO_SET_MAX_INTSET_ENTRIES` | `set-max-intset-entries` | `--set-max-intset-entries` | `memory.set_max_intset_entries` | |
| `RUVIO_LIST_MAX_COMPACT_ENTRIES` | `list-max-compact-entries` | `--list-max-compact-entries` | `memory.list_max_compact_entries` | |
| `RUVIO_LIST_MAX_COMPACT_VALUE_BYTES` | `list-max-compact-value-bytes` | `--list-max-compact-value-bytes` | `memory.list_max_compact_value_bytes` | |
| `RUVIO_NOTIFY_KEYSPACE_EVENTS` | `notify-keyspace-events` | | `notify_keyspace_events` | |
| `RUVIO_METRICS_ADDR` | no | `--metrics-address` | `metrics_address` | |
| `RUVIO_SCRIPT_MAX_BYTES` | `script-max-bytes` | | `scripts.max_bytes` | |
| `RUVIO_SCRIPT_MAX_CACHED` | `script-max-cached` | | `scripts.max_cached` | |
| `RUVIO_SCRIPT_MAX_CALLS` | `script-max-calls` | | `scripts.max_calls` | |
| `RUVIO_SCRIPT_MAX_INSTRUCTIONS` | `script-max-instructions` | | `scripts.max_instructions` | |
| `RUVIO_SCRIPT_MAX_TIME_MS` | `script-max-time-ms` | | `scripts.max_time_ms` | |
| `RUVIO_HOT_KEYS` | no | | `hot_keys` | Comma-separated globs |
| `RUVIO_REPLICATION_ROLE` | no | | `replication.role` | |
| `RUVIO_REPLICATION_LISTEN` | no | | `replication.listen` | |
| `RUVIO_REPLICATION_PRIMARY` | no | | `replication.primary` | |
| `RUVIO_MIN_REPLICAS_TO_WRITE` | `min-replicas-to-write` | | `replication.min_replicas_to_write` | |
| `RUVIO_MIN_REPLICAS_MAX_LAG` | `min-replicas-max-lag` | | `replication.min_replicas_max_lag` | Seconds, as in Redis |
| `RUVIO_REPLICA_ANNOUNCED` | no | | `replication.announced` | `CONFIG GET replica-announced` |

Flags with no variable:

| Flag | What it does |
| --- | --- |
| `--config` | Config file to read; see the lookup order above |
| `--force-promote-incomplete` | Serve a recoverable but incomplete replica as a writer, accepting possible data loss |
| `--write-config` | Write the effective settings back into the config file at startup |
| `--licenses` | Print Ruvio's license and the third-party notices, then exit |

The process is evented. There is no runtime switch in the file, on the command line, or in the environment. `INFO` still reports `ruvio_backend:evented`.

Default features are `mimalloc-allocator`. `--no-default-features` keeps the same runtime and uses the system allocator. One shard or consecutive ports, and TLS, are available on Linux, macOS, and Windows.
Invalid limits, zero timeouts, and malformed CPU sets fail startup.

`max_per_user` / `RUVIO_MAX_CONNECTIONS_PER_USER` counts authenticated ACL
identities and may be `0`. `max_commands_per_second` /
`RUVIO_MAX_COMMANDS_PER_SECOND` is per connection and may be `0`. A per-user
cap cannot exceed `max_global`.

WAL-backed profiles exclusively lock `<data_dir>/LOCK` before opening a
listener. `everysec` acknowledges after the bytes are written to the kernel and
runs `fdatasync` in the background every `sync_interval_ms` (default 1000).
That interval is the loss window: `60000` is about one minute, `500` is twice
a second. There is no separate `everyminute` profile; the contract stays
`everysec` and the number is the user's. Stalls can make the window longer.
`always` ignores `sync_interval_ms` and `fdatasync`s a client batch before the
replies are sent. A persistence write or sync failure puts that shard into a
visible error state and further mutations return `MISCONF`. After a failed
write (for example a full disk) the shard resumes by itself once the disk
accepts writes again. After a failed `fdatasync` it does not: the kernel may
have dropped the unsynced pages and report a later sync as clean, so mutations
stay refused (reads keep working) until a restart, whose recovery replays only
what actually reached the disk.

One WAL record must fit in a segment: a write, a `MULTI`/`EXEC` batch, or a
script's writes whose record is larger than `wal_segment_bytes` is refused with
`ERR write batch exceeds wal_segment_bytes`. Nothing of it is applied or logged,
the shard stays healthy (this is not a `MISCONF` state), and the refusal is
logged at most once a minute. Raise `wal_segment_bytes` or split the write.

`wal_segment_bytes` can be lowered between restarts: older, longer segments are still
read (with a warning) and new records go to segments of the new size. `ruvio-ctl` reads
segments of any length.

### Sequence-barrier sync worker

`persistence.sync_worker = "auto" | "on" | "off"` (env `RUVIO_SYNC_WORKER`) moves
the grouped `fdatasync` of `always` durability off the shard thread.
The default `auto` turns it on only for Linux, `always` durability, one shard and no
`hot_keys`; everywhere else it stays off. `on` forces it
(other profiles and `hot_keys` fail validation) and `off`
returns to the shard-thread sync. The old `experimental_sync_worker` /
`RUVIO_EXPERIMENTAL_SYNC_WORKER` keys still work and map to `on` / `off`.
`CONFIG GET sync-worker` shows the mode and `CONFIG GET sync-worker-active`
the resolved value.

Each shard has one long-lived sync worker, one active sync and one coalescing
pending slot. The shard retains WAL encoding, append and mutation ownership;
the worker uses a segment-scoped cloned file handle without taking the shard's
persistence mutex during `sync_data`. Grouped response batches capture a
sequence and asynchronously wait for the durable cursor before flushing.
Read-only batches and outbound notification frames also wait for already
observed writes. A conservative 64 KiB unsynchronized-byte threshold drains the
group between commands; one command's WAL records may exceed that threshold,
bounded by existing protocol/segment limits. Existing connection and output
limits still bound pending replies. No per-command thread/task/timer or queue
entry is introduced.

This is **sync offload, not the complete persistence-worker architecture**.
Append I/O, fallback mutations, segment rotation and checkpoint operations
remain synchronous. Rotation/drain and shutdown can still block on the OS's
disk call; a client durability wait uses the write timeout but does not cancel
an in-flight fsync or replay a write. Worker errors are latched, logged and fail
barriers without emitting buffered success responses; after a persistence
failure (disk full, I/O error) batches that wrote nothing, such as reads and
`PING`, are still answered and writes return `MISCONF`. Worker sync metrics
count worker calls, not inline fallback/rotation syncs.

Measured on Linux (4-core VM, Premium SSD): `always` SET throughput about 2x
after the WAL began filling its preallocated chunks with zeros on a helper
thread. More
shards do not speed up `always`: disk flush latency, not shard CPU, is the limit.

`SAVE` and `BGSAVE` publish a durable checkpoint on the shard worker, persist
the last-seen clock floor to `<data_dir>/CLOCK`, and then delete WAL segments
covered by that checkpoint. `SAVE` replies once the checkpoint is published.
`BGSAVE` does not fork; it starts the same in-process checkpoint and replies
`Background saving started` at once, before the checkpoint is written. The
shard writes it in slices between requests, and `INFO` reports
`rdb_bgsave_in_progress:1` until it is published; a failure after the reply
is only logged. With several shards `SAVE` asks every shard for a checkpoint
taken after the request and replies once a round holding all of them is
published; if any shard's checkpoint or the round fails, it replies
`MISCONF persistence error` at once, as with one shard. Set
`snapshot_interval_ms` to schedule checkpoints after that interval when at
least `snapshot_min_mutations` mutating commands have run (`0` means any
mutation). `snapshot_interval_ms = 0` disables the
scheduler. `SAVE` still works. Recovery uses
`max(CLOCK, checkpoint.created_at)` as a monotonic clock floor so TTLs do not
appear expired if the wall clock moves backwards.

A background checkpoint walks the keyspace in short slices between requests and
keeps the old copy of each key written meanwhile in a journal bounded by
`snapshot_journal_bytes`. When writes overflow it, the attempt restarts with
longer slices, then with a larger journal (at most a quarter of the shard's
data), then with slices of up to 256 ms that wait for the snapshot writer. If
even those cannot outpace the writes, the attempt is given up, logged, and
retried after 5 seconds; `INFO` counts these in `snapshot_overload_skips_total`
(`snapshot_aborts_total` counts every restart). The WAL keeps every write
meanwhile. Each retry starts with the longest slices and raises the journal's
cap to half, then all of the shard's data; with `max_shard_bytes` set, these
caps also stay within the memory left under that limit. If even the largest
journal overflows, the next retry falls back to long blocking slices (2 s,
doubling after each abort) and logs an error: memory stays bounded, and this
shard's clients stall during those slices until the walk completes. On a large
shard one stall can be long (a slice may reach about twice a full walk), but it
ends. The first journal level (a quarter of the data) is not limited by the
memory left under `max_shard_bytes`. A completed or failed checkpoint resets
the ladder. `SAVE` walks in one go and is not affected.

### Change stream

`CHANGES SUBSCRIBE [FROM <shard>:<seq>|NOW] [MATCH glob] [TYPES a,b] [VALUES]` turns this
shard's WAL into a push stream (RESP3 `>`; RESP2 clients get plain arrays). The reply is the
cursor the stream starts after. Each event is
`["change", cursor, db, type, key|nil, value|nil]`; `type` is one of `put`, `json`, `del`,
`expire`, `rename`, `list`, `set`, `hash`, `zset`, `stream`, `flush`, `other`. `value` is filled
only with `VALUES`, and only for string and JSON writes. A record with several keys produces one
event per key, all with the same cursor, written together. A client stores the cursor of the last
event it processed and passes it as `FROM` after a disconnect or restart: no event is lost or
repeated. Filtered-out records still move the cursor (`["cursor", c]` about once a second).
Only durable records are streamed. A shard is its own sequence, so a cluster client subscribes on
each shard it needs; the cursor names its shard and a wrong one is refused.

`cdc_retain_bytes` (environment `RUVIO_CDC_RETAIN_BYTES`, default `0`) is how much WAL a
checkpoint keeps for subscribers, rounded to whole segments, whether or not anyone is
connected. `0` keeps nothing extra and changes nothing. A cursor older than the window gets
`["resync", reason]` and the connection closes: reload fully, then `FROM NOW`. A subscriber too
slow to drain its queue is disconnected and resumes from its cursor. A stream needs a durability
profile with a WAL, and is refused on a replica. Expiry that happens lazily on read is not in the
WAL, so it produces no event; the `expire` event carries the deadline change, not the removal.

`replica_retain_max_bytes` (environment `RUVIO_REPLICA_RETAIN_MAX_BYTES`, default 1 GiB, `0` = no
limit) caps the WAL a connected replica that has not yet received it may keep on the primary. Past
the cap the primary drops that hold and compacts as usual; the replica finds its cursor gone and
takes a fresh snapshot (the fail-closed catch-up path), it is never promoted with a gap.
`INFO` shows `replica_retain_dropped`, how many times a hold was dropped.

`ruvio-ctl check` and `inspect` validate the same files without taking `LOCK`
or truncating a torn WAL tail. `ruvio-ctl backup` exclusively locks the source
and copies `CURRENT`, `CLOCK`, `checkpoints/`, `wal/`, and any per-shard
`REPLICA_CATCHUP_####` markers. `ruvio-ctl restore` refuses a destination that
already has those files or markers unless `--force` is set. Integrity checks
do not certify replica catch-up completeness. See [RECOVERY.md](RECOVERY.md).

`INFO prometheus` returns Prometheus text for the receiving shard. There is no
HTTP metrics endpoint yet: `metrics_address` / `--metrics-address` is accepted but nothing listens on it
(the server warns at startup if it is set).

`hot_keys` / `RUVIO_HOT_KEYS` is a list of Redis-style `*` / `?` globs
(`hot_keys = ["lock:*", "session:*"]` or `RUVIO_HOT_KEYS=lock:*,session:*`).
The default is empty: the feature is off and the shard GET path takes no extra
lock. A matching key is served from a process-global string store before hash
slot / `MOVED`, so two shards in one process see the same `lock:foo`. With
one shard every key is already on it, so `hot_keys` is ignored there (with a
warning). Allowed commands are `GET`, `SET` (including `NX` / `XX` / `EX` /
`PX` / `EXAT` / `PXAT` / `KEEPTTL`), and `DEL`.
Any other command on a matching key returns
`-ERR hot key does not support <cmd>` and does not write the shard. Inside
`MULTI` a command on a matching key returns
`-ERR hot keys cannot be used inside MULTI` and `EXEC` then aborts with
`EXECABORT`: the hot store cannot join the transaction. Empty glob
entries fail startup. Enable the prefix on empty data; existing shard keys are
not migrated. Checkpoints wait for a virtual hot partition (`shards + 1`)
before publishing `CURRENT`. `SAVE` on a primary with `hot_keys` writes the hot
partition into the same checkpoint. A replica behind a deleted WAL prefix
receives that checkpoint.

A hot `SET`, `DEL` or a `FLUSHDB` / `FLUSHALL` that removes hot keys keeps the
usual order: checked, logged, then applied. Its record is taken in the shard's
log before the shared store changes, so a refused record (a failed log, or a
record larger than `wal_segment_bytes`) leaves the key as it was for every
client. As for a batch of shard writes, the record is written to the file (and
synced under `always`) just after the store's lock is released. If that write
fails (a disk error), the writer gets `MISCONF`, the value stays readable, and
the log keeps the record and writes it first when it resumes.

`replication.role` / `RUVIO_REPLICATION_ROLE` is `off` (default), `primary`, or
`replica`. `replication.listen` / `RUVIO_REPLICATION_LISTEN` is the primary's
base address. `replication.primary` / `RUVIO_REPLICATION_PRIMARY` is the
address a replica dials. `replication.listen` must be a numeric socket address.
`replication.primary` may be a numeric address or `host:port`, resolved again on
each dial.
Shard `i` uses that base port plus `i`. Role `off` opens no replication port
and dials nobody. A primary binds `replication.listen` (loopback in the sample
above). A replica dials `replication.primary` and does not listen. A primary keeps
each replica on its own stream, up to 16.

`replication.min_replicas_to_write` / `min-replicas-to-write` (default `0`, off)
and `replication.min_replicas_max_lag` / `min-replicas-max-lag` (seconds, default
`10`) work as in Redis. A primary shard refuses writes with `NOREPLICAS Not enough
good replicas to write.` while fewer connected replicas than the first value
acknowledged within the second; reads go on. A replica acknowledges each synced
sequence and repeats its last acknowledgement every second while idle. The shard
checks this every 100 ms, so a lost replica is noticed up to that much after the
lag runs out, and writes resume on the next check after enough replicas
acknowledge. It bounds how long a primary cut off from its replicas keeps taking
writes another primary will not have; it is not a synchronous write. A replica
that installs a full copy acknowledges nothing until it has caught up, so with a
single replica and `min-replicas-to-write 1` the primary refuses writes while that
replica resyncs, as in Redis. Both change
with `CONFIG SET`. Only role `primary` applies them.

**Replicas in the topology.** A replica sends its primary the address clients
reach it on: `advertised_host`, and `advertised_port` (else the bind port) plus
the shard index in `per-shard` mode. A replica that announces a loopback host
but connects from another machine is listed at the address it connected from,
so the default `advertised_host` works on a flat network; set `advertised_host`
behind NAT, a proxy or in containers. A host is announced only when it is a
host name or an IP address of at most 255 characters (letters, digits and
`. - _ : % [ ]`); anything else is not announced. A replica sends no
acknowledgement while it installs a full copy and catches up, so it is not
listed, and not counted by `WAIT` or `min-replicas-to-write`, until its copy is
complete. Once it acknowledges, the primary lists it after its own shard in
`CLUSTER SLOTS`, `CLUSTER SHARDS` and `CLUSTER NODES`, in `ROLE`, and as
`slave0:ip=…,port=…,state=online,offset=…,lag=…` lines in `INFO replication`;
a replica that disconnects, or sends no acknowledgement for 10 seconds (it
repeats one every second), leaves the list. The same loopback rule applies to the
primary's own address as its replicas see it. A replica answers the cluster
commands with the primary it follows first and itself second, so a client that
knows only a replica's address still sends writes to the primary.
`replication.announced = false` (`RUVIO_REPLICA_ANNOUNCED=no`,
`CONFIG GET replica-announced`, Redis's `replica-announced`) keeps a replica out
of these answers; it still replicates. Node ids are the SHA-1 of the announced
`host:port`, so they differ between processes and stay the same across restarts
on the same address. Clients choose whether to read from replicas; see
[CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md#reads-from-replicas).
Replication TLS (`replication.tls`, default `false`, restart-only, TOML only)
wraps each replication connection in TLS. A primary presents the certificate in
`tls_cert`/`tls_key` (required; the same files and reload as client TLS), so
the certificate must cover the name or address replicas dial. A replica verifies
it against `replication.tls_ca` (PEM issuer, required on a replica) and, when
the primary asks for one (`tls_client_ca`/`tls_client_auth` apply to replicas
too), presents `replication.tls_client_cert` and `replication.tls_client_key`
(both or neither). Both ends of a link must agree: a TLS replica cannot follow a
cleartext primary or the reverse. Off, nothing is loaded and the stream is
exactly as before.

```toml
[replication]
tls = true
tls_ca = "/etc/ruvio/ca.pem"                 # replica: issuer of the primary's certificate
tls_client_cert = "/etc/ruvio/replica.pem"   # replica, optional
tls_client_key = "/etc/ruvio/replica.key"
```

`INFO` reports `replication_role`, `wal_last_sequence`, and
`wal_durable_sequence` on every shard. A primary also reports
`replication_replicas`, `replication_replica_sequence`, and
`replication_send_lag`. A replica reports `master_link_status` (`up` or
`down`), `master_last_io_seconds_ago` (`-1` before the first frame),
`slave_repl_offset` and `master_sync_in_progress`. When a checkpoint must be installed, the replica
persists a `REPLICA_CATCHUP_####` marker under `data_dir` before replacing its
local snapshot/WAL base. Until it has replayed and flushed WAL through the
primary's catch-up watermark, client reads return `LOADING`; after 30 seconds
without completion they return `MASTERDOWN`. The marker survives restart and
backup/restore. A replica with an incomplete marker requests a fresh snapshot
when the primary becomes available; do not delete the marker by hand. No
additional TOML setting is needed for this protection.

**Clocks.** Every node expires keys by its own clock, and the primary does not
replicate expiry deletes. Keep primary and replicas in sync with NTP. A replica
whose clock runs ahead can drop a key early; if the primary then extends that
key's TTL, the replica no longer has it and requests a full copy of the shard.
A TTL change whose new deadline has already passed is applied without a resync.

Changing `replication.role` from `replica` to `off` or `primary` (or `REPLICAOF NO ONE`) is a
**manual promotion**; for automatic failover run `ruvio coordinator` ([COORDINATOR.md](COORDINATOR.md)). If any shard still has a catch-up
marker, normal writer startup refuses it. `ruvio --config ./ruvio.toml
--force-promote-incomplete` allows a recoverable but possibly incomplete
replica to start as a writer **only for that invocation**; it is a CLI flag,
not a TOML or environment setting, and is never written back to the config.
It cannot be used while `role = "replica"` and does not bypass corrupt
checkpoint/WAL validation or recover missing writes. The marker stays on disk,
so every subsequent writer restart requires another explicit force decision.
Fence the old primary to prevent split brain. Follow the per-shard sequence
checks and procedure in [RECOVERY.md](RECOVERY.md#promote-a-replica-by-hand).

A later handshake does not close an earlier replica. At the limit the primary closes the new connection.
Primary and replicas must run the same Ruvio version: a primary refuses a replica of another
version and both sides log a warning (see [RECOVERY.md](RECOVERY.md#write-history)).
Startup refuses replication unless the process can copy a WAL:

- `memory` and `snapshot` return `replication requires a WAL-backed durability profile`.
- Role `primary` without `replication.listen` returns `replication primary requires replication.listen`.
- Role `replica` without `replication.primary` returns `replication replica requires replication.primary`.
- A non-numeric `replication.listen` returns `replication.listen must be a numeric socket address`. `replication.primary` accepts a numeric address or a resolvable `host:port`; an invalid address is rejected.
- Role must be `off`, `primary`, or `replica`, or startup returns `replication role must be off, primary, or replica`.
- The replication port range must not overlap RESP (`replication port range overlaps the RESP listener range`) or metrics (`replication port range overlaps the metrics listener range`).

Script limits are `RUVIO_SCRIPT_MAX_BYTES`, `RUVIO_SCRIPT_MAX_CACHED`,
`RUVIO_SCRIPT_MAX_CALLS`, `RUVIO_SCRIPT_MAX_INSTRUCTIONS`, and
`RUVIO_SCRIPT_MAX_TIME_MS`. Zero is rejected, and `max_time_ms` above
3,600,000 returns `script time limit must be at most 3600000 ms`. The time
limit is checked every 1,000 Lua instructions, more often once the shard's Lua
heap passes 1 MiB (down to every 10 at 100 MiB), before each library call that
is charged by the size of its data, and before each `ruvio.call`. A single
operation is not interrupted, so a script can run past the limit by the time
of the last few operations on its largest values.

The default bind address is loopback. A non-loopback bind without
`requirepass` keeps `protected_mode` on by default and rejects non-loopback
clients. Turning protected mode off on a public bind without a password
refuses to start. Container images set `RUVIO_PROTECTED_MODE=false` because
published ports are not loopback inside the namespace, so they need
`RUVIO_REQUIREPASS` or an ACL password. `RUVIO_ALLOW_UNAUTHENTICATED=true`
is the explicit exception for a network that is already private.
`RUVIO_REPLICATION_PASSWORD`
is the replication handshake secret (Garnet's cluster password). When it is
empty the handshake uses `requirepass`. A public replication listen needs one
of those secrets. See [THREAT_MODEL.md](THREAT_MODEL.md). `AUTH` uses
constant-time comparison and never logs the secret. `requirepass`,
`metrics-token`, and `replication-password` are redacted in `CONFIG GET`.
`CONFIG GET *` lists the effective settings for the process; `CONFIG GET max-*`
uses the same glob rules as `SCAN`. Redis compatibility keys `save` (empty)
and `appendonly` (`no`) remain present. `CONFIG SET` changes the live
settings; see *Changing settings at runtime*.

TLS is supported on portable and evented. Set `tls_cert` / `tls_key`, `RUVIO_TLS_CERT` /
`RUVIO_TLS_KEY`, or `--tls-cert` / `--tls-key` to PEM files. Both paths must
be set together. The accept path completes the rustls handshake before
reading RESP and reloads material when either file's mtime changes.
`io-uring` plus TLS fails startup; terminate TLS in front of that adapter.
When TLS is enabled, plaintext clients are closed at handshake. Redis clients
must enable TLS (`rediss://`, `--tls`, or the library equivalent) and trust
the configured certificate. `tls_client_ca` / `RUVIO_TLS_CLIENT_CA` /
`--tls-client-ca` is a PEM issuer. A presented client certificate must chain
to it. `tls_client_auth` / `RUVIO_TLS_CLIENT_AUTH` / `--tls-client-auth`
rejects a handshake that does not present one. Neither setting is consulted
on a plaintext connection.

`tls_cert` covers client connections only. The replication port (WAL and snapshot
stream, and the replication password in its handshake) is encrypted only when
`replication.tls = true` (see below); otherwise keep it on a private network, a
VPN or a tunnel. With TLS on and a replication listener or primary on another
machine and `replication.tls` off, the server logs a warning at startup.

ACL users are configured in `[[security.users]]` with Redis-style rule tokens:
`on`/`off`, `>password`, `nopass`, `~pattern`, `+@category`, `-@category`,
`+command`, and `-command`. The implicit `default` user is `on nopass ~* +@all`
until `requirepass` or a `default` user entry sets a password. `AUTH password`
targets `default`; `AUTH username password` selects a named user. The operator
UI sign-in form is this same `AUTH`. An empty user and password send nothing
and the session stays `default`. Command and
key checks run on the connection after AUTH using a copied rule set; they do
not take a shard lock. `ACL SETUSER` uses the same tokens and leaves an
open connection on the rules copied at its last `AUTH`. `security.acl_file`
or `RUVIO_ACL_FILE` is the ACL file. When that file already exists at
startup it is the user list. `ACL SAVE` writes the live directory there,
as password hashes (`#<hex>`; a plain `>password` line in a file you write
yourself is hashed when it is loaded). `ACL LOAD` reads it back and replaces the directory.
`GET` and `SET` do not open the file. `ACL LIST` still redacts passwords.
An empty path refuses `ACL SAVE` and `ACL LOAD`. Restart without a file
reads `[[security.users]]` again.
`ACL LIST` redacts passwords. `INCR`/`DECR`/`INCRBY`/`DECRBY` share the `incr`
ACL name.

Shard memory admission defaults to `noeviction`. `index_memory_bytes` and the
admission charge include retained key and TTL deadline-index capacities; TTL
keys can therefore reach a configured budget sooner than keys without TTL.
This modeled budget is not an exact process RSS cap (allocator, runtime, and
persistence allocations also contribute). When `max_shard_bytes` is set,
writes that would exceed the budget minus `reserved_headroom_bytes` return
Redis-style `OOM` unless `maxmemory-policy` / `RUVIO_MAXMEMORY_POLICY` /
`--maxmemory-policy` is `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, or
`volatile-ttl`. Commands that only remove or move data (`DEL`, `HDEL`,
`LPOP`, ...) are not refused; see [MEMORY_ACCOUNTING.md](MEMORY_ACCOUNTING.md#limits),
including why memory can stay at the limit until a running checkpoint ends.
Eviction is shard-local. Each decision compares up to
`maxmemory_samples` eligible live keys (default 16, range 1–64); the engine deletes
at most 16 keys per command. Sampling rotates over index slots and permitted
logical databases instead of repeatedly starting at the first keys. The production
hash index resumes at bucket positions. Expired and active-command protected keys
are skipped. Volatile policies sample only keys with a TTL from the deadline
lookup and return `OOM` immediately when the shard has no TTL keys. Under
allkeys policies, finding eligible candidates can still require a full pass when
most keys are expired or protected: `maxmemory_samples` is not a scanned-key or
latency limit. This preserves admission when an eligible key exists outside the
initial sample. Under memory pressure with an eviction policy enabled, admission
first tries one additional, bounded 16-entry expiry sweep before sampling
victims, even if the ordinary command sweep has already run. This can recover
space without eviction but does not guarantee a bound on the subsequent scan.
`active_expire_keys` (default 16, range 1–1024) bounds each ordinary command
sweep (and the separate hash-field maintenance pass). The independent
`active_expire_maintenance_keys` (default 16, range 1–1024) sets the batch size
for background key-expiry maintenance; changing it does not enlarge the command
sweep. Deadline lookup prioritizes the earliest expiry in each database, even
if many later deadlines were registered first; databases rotate when a sweep
fills its budget. Key-expiry maintenance runs approximately every 25 ms,
separately from the 100 ms general maintenance tick. It repeats full batches
while due keys remain, stopping after 750 µs of elapsed cleanup or 1,024 keys
per expiry tick. The time limit is checked between batches, not during a key
removal, so larger maintenance batches can lengthen a single event-loop stall;
it is not a strict shard-latency limit. If more keys expire than the combined
command, timer and pressure sweeps can remove, physical memory can remain
occupied until later turns; reads still treat expired keys as absent. Increasing
the command budget raises synchronous work on every command; increasing only
the maintenance batch may change idle drain at the cost of longer
non-preemptible maintenance turns. In the measured 50k SET/s WAL burst,
batches 32 and 64 did not consistently outperform the default 16; tune only
against representative backlog and shard-latency measurements.
A write larger than the shard budget is rejected without evicting. Connection,
output, persistence, and snapshot-journal charges are included in `charged_bytes`.

Hashes stay in a compact field list until `hash_max_compact_entries` or a
field/value longer than `hash_max_compact_value_bytes`; past that, field and
value share one arena block. A set of canonical integers stays a sorted integer
array up to `set_max_intset_entries` (0 turns it off); other sets of more than
128 members keep their bytes in an arena. A list stays in one packed buffer up
to `list_max_compact_entries` items of at most `list_max_compact_value_bytes`
each, then becomes a chain of 8 KiB packed nodes. Sorted sets have one
encoding. The
key index, string buffers, list rings, and set, hash, and sorted-set tables
shrink after deletes or a much smaller overwrite once they are under a quarter
full. The hot-key map uses the same rule. Environment overrides are
`RUVIO_MAXMEMORY_SAMPLES`, `RUVIO_ACTIVE_EXPIRE_KEYS`,
`RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS`, `RUVIO_HASH_MAX_COMPACT_ENTRIES`,
`RUVIO_HASH_MAX_COMPACT_VALUE_BYTES`, `RUVIO_SET_MAX_INTSET_ENTRIES`,
`RUVIO_LIST_MAX_COMPACT_ENTRIES`, and `RUVIO_LIST_MAX_COMPACT_VALUE_BYTES`.
CLI flags use the same names with dashes.

`notify_keyspace_events` (environment `RUVIO_NOTIFY_KEYSPACE_EVENTS`) is
empty by default. `K` publishes `__keyspace@<db>__:<key>` and `E` publishes
`__keyevent@<db>__:<event>`, where `<db>` is the key's logical database.
Classes are `g` (delete), `$` (`SET`), `x` (expired), `e` (evicted), and `A`
for those four. Without `K` or `E` the feature stays off. A client subscribes
with `SSUBSCRIBE` on the exact channel. Delivery is asynchronous `smessage`
and is not written to the WAL.

To receive both expiration and eviction notices, enable keyevent delivery
(`E`) and both classes (`x` and `e`) in the TOML file:

```toml
notify_keyspace_events = "Exe"
```

The setting is read at startup; restart Ruvio after changing it. Subscribe to
the exact channel for the key's database, for example
`SSUBSCRIBE __keyevent@0__:expired` or
`SSUBSCRIBE __keyevent@0__:evicted`. `PSUBSCRIBE` patterns do not receive
keyspace notices. On these keyevent channels the message payload is the key
name, not its value. Notifications are live, best-effort messages: they are
not replayed to later subscribers and are not durable.

The C# client provides convenience methods that form these channel names from
the selected database:

```csharp
await db.SubscribeExpiredAsync(message =>
    Console.WriteLine($"Expired key: {message.Payload}"));
await db.SubscribeEvictedAsync(message =>
    Console.WriteLine($"Evicted key: {message.Payload}"));
```

Eviction notices occur only when memory pressure actually triggers an eviction.
`noeviction` is the default; configure a shard memory budget and an eviction
policy if eviction is desired. In a multi-shard deployment, notifications are
local to the shard that owns the key. A subscription on one shard does not
collect events from every shard. The Operator Pub/Sub screen has matching
expired/evicted listeners; choose the database and shard to monitor there.

`shards` (environment `RUVIO_SHARDS`, flag `--shards`) starts that many
shard owners in one process. With the default `port_mode = "per-shard"` shard
`i` listens on the bind port plus `i` and owns slots `i * 16384 / shards` up to
the next shard's first slot. A command for a key another shard owns is forwarded
to that shard and answered on the connection it came in on; the server never
sends `MOVED` (sharded Pub/Sub channels are the exception: `SSUBSCRIBE` on the
wrong shard is redirected, as a subscription cannot be forwarded). Cluster-aware
clients find the other shards from `CLUSTER SLOTS`, `CLUSTER SHARDS`, or
`CLUSTER NODES`, connect to each owner and pay no hop; any other client works on
any one port, with the hop. `port_mode = "single"` (deprecated: it logs a
warning at startup and will be removed; any one per-shard port already serves
every key) opens one port only, spreads connections over the shards, runs
keyless commands (`DBSIZE`, `KEYS`, `FLUSHALL`, `SCAN`) across every shard and
lets `WAIT` cover every shard; in `per-shard` those stay per shard, as in Redis
Cluster, and cluster clients fan them out themselves; a client that is not cluster-aware and
talks to one port sees one shard's keys there, and its `FLUSHALL` clears that shard only.
Its `WAIT` in `per-shard` covers the port's shard plus every shard its commands were
forwarded to.
Classic `PUBLISH` reaches subscribers on every shard in both modes.
`advertised_host` (`RUVIO_ADVERTISE_HOST`, `--advertised-host`, default
`127.0.0.1`) is the host the cluster replies name. `advertised_port` (`RUVIO_ADVERTISE_PORT`, `--advertised-port`) is the
port they name for shard 0; shard `i` is that port plus `i`. Leave it unset
when clients reach the bind ports directly. Set both when a container or load
balancer publishes the shards elsewhere, for example `-p 7000-7003:6379-6382`
with `RUVIO_ADVERTISE_HOST=<host clients use>` and `RUVIO_ADVERTISE_PORT=7000`.

`cluster_topology` (`RUVIO_CLUSTER_TOPOLOGY`) makes this node one of a multi-node
cluster: the path of a map that gives every hash slot to one group, where a group
is a primary and its replicas. `cluster_group` (`RUVIO_CLUSTER_GROUP`) names this
node's group in it. Both are read at startup only; without a map the node owns
every slot, as before.

```toml
version = 1                    # increases with every change; CLUSTER INFO cluster_current_epoch

[[groups]]
name = "a"
address = "10.0.0.1:7000"      # client address of the group primary's shard 0
shards = 2                     # must equal `shards` on that group's nodes
slots = ["0-8191"]

[[groups]]
name = "b"
address = "10.0.0.2:7000"
shards = 4                     # groups may differ in shard count
slots = ["8192-16383"]
```

Every slot must belong to exactly one group, and the node's own group must list its
`shards`. A group's slots, in order, are split into equal runs over its shards (shard
`i` of group `b` above listens on port 7000 + `i`); a group that owns every slot
places them exactly as a node without a map does. A command whose key belongs to
another group is answered `-MOVED slot host:port` with the address of the shard
that holds it (inside `MULTI` the transaction is then aborted), so cluster-aware
clients reach the owner directly; local keys are forwarded between this node's
shards as before. `CLUSTER SLOTS`, `CLUSTER SHARDS` and `CLUSTER NODES` describe
the whole map, other groups by their primary only. Keyless commands (`DBSIZE`,
`KEYS`, `SCAN`, `FLUSHALL`), `WAIT` and classic `PUBLISH`/`SUBSCRIBE` stay on the
node that receives them; use `SPUBLISH`/`SSUBSCRIBE` for channels the whole
cluster shares. Start a group with its slots empty, or as the only group of a
node that already holds data; `ruvio coordinator` moves ranges between groups
later (`COORDINATOR MOVE`, see `docs/COORDINATOR.md`), and a node refuses a map
that gives its group a slot whose data is not there. `migration_max_bytes_per_sec`
(`RUVIO_MIGRATION_MAX_BYTES_PER_SEC`, default 0: no cap) caps how fast a node copies
a range it sends to another group. A node that holds keys in slots the map serves elsewhere (or on another of its
own shards) refuses to start and names the first such slot; the check walks the
keys once at startup, only when a map is set. With `ruvio coordinator` managing the
groups, the coordinators send each node a newer map after every failover (`RUVIO
TOPOLOGY SET`, see `docs/COORDINATOR.md`); the node keeps it in `TOPOLOGY` in its data
directory and starts from the newer of that file and `cluster_topology`. A one-shard
node with a map answers the `CLUSTER` commands too. A map needs `port_mode = "per-shard"` and cannot be combined with `hot_keys`.
`RUVIO MOVESLOTS` still moves this group's slots between the node's own shards, starting
from the map's placement; another group's range cannot be moved. The layout the moves
started from is kept next to them (`SLOTS_BASE`), and a node whose moves were made with
another layout (no map, or a map with other slots for this group) refuses to start.
`cpu_set` (`RUVIO_CPUSET`, `--cpu-set`) pins shard threads on Linux. It takes a
CPU list such as `2`, `0-3` or `0,2,4-5`; shard `i` runs on the `i`th listed
core and the list wraps when there are more shards than cores. Unset means no
pinning. A bad list or a core the process may not use only logs a warning. Other
platforms log that pinning is unsupported.
The shard count is read at start; changing it needs a restart on an empty data
directory.

`[protocol] databases` (environment `RUVIO_DATABASES`) is the number of
logical databases, 16 by default and 1 to 256. `SELECT` accepts `0` to
`databases - 1`. With `shards > 1` only database 0 is allowed. `CONFIG GET
databases` reports the value. See `docs/LOGICAL_DATABASES.md`.
