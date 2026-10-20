# Observability

Each shard owner keeps its own counters. Updates are ordinary local integer operations; the hot path does not use atomic counters or a global metrics lock.

Query a shard with:

```bash
redis-cli -p 6379 INFO
```

`INFO section` returns one block; the name compares without case or spaces
(`INFO commandcounters`), `all`, `everything` and `default` return every block,
and an unknown name returns an empty reply. `INFO prometheus` returns the
Prometheus text below. Every field `INFO` writes is in this table, and a test
fails when one is added without a row:

| Section | Field | Meaning |
| --- | --- | --- |
| Server | `ruvio_version` | Server version |
| Server | `ruvio_backend` | Runtime backend (`evented`) |
| Server | `ruvio_health` | `ok` while the shard answers |
| Server | `ruvio_readiness` | `ready` once recovery finished |
| Server | `ruvio_durability` | Durability profile |
| Server | `ruvio_persistence_state` | `ok`, or `error` after a WAL or sync failure |
| Server | `recovered_wal_records` | WAL records replayed at startup |
| Server | `wal_last_sequence` | Last WAL sequence written |
| Server | `wal_unsynced_bytes` | WAL bytes written but not yet synced |
| Server | `ruvio_shard_id` | This shard |
| Server | `ruvio_shard_count` | Shards in the process |
| Stats | `connected_clients` | Open client connections on this shard |
| Stats | `total_connections_received` | Accepted connections, process-wide |
| Stats | `rejected_connections` | Connections refused by the connection limits, process-wide |
| Stats | `total_net_input_bytes` | Bytes read from this shard's client connections |
| Stats | `total_net_output_bytes` | Bytes written to this shard's client connections |
| Buffers | `input_buffer_peak_bytes` | Largest connection input buffer seen |
| Buffers | `output_buffer_peak_bytes` | Largest connection output buffer seen |
| Buffers | `pubsub_queue_peak_frames` | Longest subscriber queue seen, in frames |
| Buffers | `pubsub_queue_peak_bytes` | Longest subscriber queue seen, in bytes |
| Buffers | `pubsub_cluster_dropped` | Classic `PUBLISH` deliveries to another shard dropped because that shard's publish queue was full (16,384 deliveries or 64 MiB), since the process started |
| Memory | `charged_memory_bytes` | Memory charged against `maxmemory` |
| Keyspace | `keys` | Keys in every database |
| Keyspace | `expired_keys` | Keys removed at their deadline |
| Keyspace | `expired_keys_passive` | Of those, found on lookup or write |
| Keyspace | `expired_keys_active` | Of those, found by the bounded expire cycle |
| Keyspace | `evicted_keys` | Keys removed by the eviction policy |
| Keyspace | `expires` | Keys that carry a deadline, overdue ones included |
| Keyspace | `db<N>` | `keys=…,expires=…,avg_ttl=…` per non-empty database, as in Redis |
| Command counters | `total_commands_processed` | Commands run |
| Command counters | `get_commands` | `GET` calls |
| Command counters | `set_commands` | `SET` calls |
| Command counters | `increment_commands` | `INCR`, `DECR`, `INCRBY`, `DECRBY` calls |
| Command counters | `other_commands` | Every other command |
| Memory breakdown | `key_memory_bytes` | Charged key bytes |
| Memory breakdown | `value_memory_bytes` | Charged value bytes |
| Memory breakdown | `collection_memory_bytes` | Charged collection bytes |
| Memory breakdown | `index_memory_bytes` | Primary index bytes |
| Memory breakdown | `pooled_value_bytes` | Bytes kept in the value pool for reuse |
| Memory breakdown | `persistence_memory_bytes` | WAL and snapshot buffers |
| Memory breakdown | `lazyfree_pending_bytes` | Bytes waiting for the lazy-free thread |
| Snapshot | `snapshot_in_progress` | 1 while a snapshot walk is open |
| Snapshot | `snapshot_journal_bytes` | Bytes the open snapshot keeps for keys written during it |
| Snapshot | `snapshot_aborts_total` | Snapshots given up |
| Snapshot | `snapshot_overload_skips_total` | Checkpoints skipped because the shard was busy |
| Snapshot | `rdb_bgsave_in_progress` | 1 while a `BGSAVE` runs |
| Latencystats | `latency_percentiles_usec_<command>` | p50, p99, p99.9 and samples; only with `latency_histograms` on |
| Logs | `log_pages` | Value-log pages in use |
| Logs | `log_extents` | Value-log extents held |
| Logs | `log_free_bytes` | Free bytes inside held pages |
| Logs | `log_pages_released_total` | Pages given back |
| Logs | `log_relocated_bytes_total` | Bytes moved to compact pages |
| Slabs | `slab_pages` | Slab pages in use |
| Slabs | `slab_records` | Records stored in slabs |
| Slabs | `slab_class_<N>b` | Pages and records of one slot size, for classes in use |
| Replication | `replication_role` | `off`, `primary` or `replica` |
| Replication | `wal_durable_sequence` | Last WAL sequence synced to disk |
| Replication | `replication_replicas` | Connected replica sockets on a primary, otherwise 0 |
| Replication | `replication_replica_sequence` | Oldest sequence written to a connected replica socket, otherwise 0 |
| Replication | `replication_send_lag` | `wal_durable_sequence` minus that oldest send; 0 means the bytes left the primary |
| Replication | `replica_retain_dropped` | WAL holds for slow replicas dropped |
| Replication | `replication_epoch` | Epoch of the history this primary writes; grows with each promotion (0 on a replica and on a server that never replicated) |
| Replication | `slave<n>` | Primary only, one line per replica that announced a client address, finished copying and acknowledged within the last 10 seconds: `ip=…,port=…,state=online,offset=<acknowledged sequence>,lag=<seconds since its last acknowledgement>`; the replica's client port for this shard. `replication_replicas` also counts replicas that are still copying or announce nothing |
| Replication | `replication_fenced` | 1 when a replica showed this primary a newer epoch: another primary replaced it and writes are refused |
| Replication | `replication_committed_sequence` | Highest sequence a `CLIENT DURABILITY` reply was released for: on a primary its own, on a replica what its primary last sent. A failover should promote only a replica whose synced sequence reaches it |
| Replication | `master_link_status` | Replica only: `up` or `down` |
| Replication | `master_last_io_seconds_ago` | Replica only: seconds since the last frame, `-1` before the first |
| Replication | `slave_repl_offset` | Replica only: last applied WAL sequence |
| Replication | `master_sync_in_progress` | Replica only: 1 while a full resync is requested |
| Cluster | `cluster_enabled` | 1 in cluster mode |

The replica's own `wal_durable_sequence` is the copy you can promote.

In multi-shard mode, query every consecutive shard port. Counters are intentionally not aggregated in the server because aggregation would introduce cross-core synchronization. Monitoring systems should scrape and sum them externally.

Prometheus text is available without adding hot-path atomics or a global lock:

```bash
redis-cli INFO prometheus
```

There is no HTTP scrape endpoint yet; `INFO prometheus` over the RESP port is the
only way to read the metrics. A scrape is a control-plane read on the owning
shard; it does not update command counters.

Expiration and eviction counters live on the owning shard. Linux updates them
as ordinary local integers; portable scrapes read the same shard fields. A
scrape does not run the active expire cycle or evict keys.

Prometheus text includes:

- `ruvio_net_input_bytes_total` and `ruvio_net_output_bytes_total`, per shard
- `ruvio_connections_total`, `ruvio_rejected_connections_total` (refused by the
  connection limits), `ruvio_rate_limited_commands_total` (refused by the
  per-connection token bucket) and `ruvio_protocol_errors_total` (frames the parser
  could not read, which close the connection). These four are process-wide and
  reported on shard 0 only.
- `ruvio_expired_keys_total{reason="passive|active"}`
- `ruvio_keys_with_ttl`
- `ruvio_evicted_keys_total`
- `ruvio_eviction_scans_total` / `ruvio_eviction_slots_inspected_total`
  (victim-search work, including empty hash buckets and searches with no victim)
- `ruvio_allocation_events_total` / `ruvio_allocation_bytes_total`
- pool retention remains `ruvio_pooled_value_bytes`

Charged memory is a deterministic storage-budget model, not process RSS. See
[MEMORY_ACCOUNTING.md](MEMORY_ACCOUNTING.md) for category definitions and
limitations.