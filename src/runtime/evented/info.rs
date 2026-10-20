use super::*;

pub(super) fn write_evented_info(
    output: &mut BoundedOutput,
    state: &ShardState,
    section: Option<&[u8]>,
) -> io::Result<()> {
    write_response(
        output,
        Response::Bulk(Some(evented_info(state, section)?.into())),
    )
}

/// The `INFO` text, for a plain call and for one queued in `MULTI`.
pub(super) fn evented_info(state: &ShardState, section: Option<&[u8]>) -> io::Result<Vec<u8>> {
    if section.is_some_and(|value| value.eq_ignore_ascii_case(b"prometheus")) {
        return Ok(crate::metrics::encode_prometheus(&evented_snapshot(state)));
    }

    let store = state.store.borrow();
    let persistence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let memory = store.memory_usage();
    let persistence_state = if persistence.failure().is_some() {
        "error"
    } else {
        "ok"
    };

    let shard_id = state.route.as_ref().map_or(0, |route| route.shard_id);
    let shard_count = state.route.as_ref().map_or(1, |route| route.shard_count);
    let mut info = Vec::with_capacity(512);
    let counters = *state.command_metrics.borrow();
    let (net_input, net_output) = super::net::totals();
    let admission = crate::admission::admission_totals();

    write!(
        info,
        "# Server\r\nruvio_version:{}\r\nruvio_backend:evented\r\nruvio_health:ok\r\nruvio_readiness:ready\r\nruvio_durability:{}\r\nruvio_persistence_state:{}\r\nrecovered_wal_records:{}\r\nwal_last_sequence:{}\r\nwal_unsynced_bytes:{}\r\nruvio_shard_id:{shard_id}\r\nruvio_shard_count:{shard_count}\r\n\
         # Stats\r\nconnected_clients:{}\r\ntotal_connections_received:{}\r\nrejected_connections:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\n# Buffers\r\ninput_buffer_peak_bytes:{}\r\noutput_buffer_peak_bytes:{}\r\npubsub_queue_peak_frames:{}\r\npubsub_queue_peak_bytes:{}\r\npubsub_cluster_dropped:{}\r\n# Memory\r\ncharged_memory_bytes:{}\r\n# Keyspace\r\nkeys:{}\r\nexpired_keys:{}\r\nexpired_keys_passive:{}\r\nexpired_keys_active:{}\r\nevicted_keys:{}\r\nexpires:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        persistence.profile(),
        persistence_state,
        state.recovery.records_replayed,
        persistence.last_sequence(),
        persistence.dirty_bytes(),
        state.active_connections.get(),
        admission.accepted,
        admission.rejected,
        net_input,
        net_output,
        state.peak_input_buffer_bytes.get(),
        state.peak_output_buffer_bytes.get(),
        state.peak_pubsub_queue_frames.get(),
        state.peak_pubsub_queue_bytes.get(),
        super::forward::publish_dropped(),
        memory.charged_bytes(),
        store.len(),
        store.expiration_stats().expired_keys,
        store.expiration_stats().expired_keys_passive,
        store.expiration_stats().expired_keys_active,
        store.memory_stats().evicted_keys,
        store.expiration_stats().keys_with_ttl,
    )?;
    info.extend_from_slice(crate::engine::keyspace_info(&store).as_bytes());

    write!(
        info,
        "# Command counters\r\ntotal_commands_processed:{}\r\nget_commands:{}\r\nset_commands:{}\r\nincrement_commands:{}\r\nother_commands:{}\r\n\
         # Memory breakdown\r\nkey_memory_bytes:{}\r\nvalue_memory_bytes:{}\r\ncollection_memory_bytes:{}\r\nindex_memory_bytes:{}\r\npooled_value_bytes:{}\r\npersistence_memory_bytes:{}\r\nlazyfree_pending_bytes:{}\r\n\
         # Snapshot\r\nsnapshot_in_progress:{}\r\nsnapshot_journal_bytes:{}\r\nsnapshot_aborts_total:{}\r\nsnapshot_overload_skips_total:{}\r\nrdb_bgsave_in_progress:{}\r\n",
        counters.commands,
        counters.gets,
        counters.sets,
        counters.increments,
        counters.other_commands,
        memory.key_bytes,
        memory.value_bytes,
        memory.collection_bytes,
        memory.index_bytes,
        memory.pooled_bytes,
        memory.persistence_bytes,
        crate::lazy_free::pending_bytes(),
        u8::from(store.journal_is_open()),
        memory.snapshot_journal_bytes,
        store.snapshot_aborts(),
        state.checkpoint_overload_skips.get(),
        u8::from(super::checkpoint::background_save_in_progress(state)),
    )?;

    if state.live.latency_histograms.get() {
        info.extend_from_slice(b"# Latencystats\r\n");

        for latency in command_latency(state) {
            write!(
                info,
                "latency_percentiles_usec_{}:p50={:.3},p99={:.3},p99.9={:.3},samples={}\r\n",
                latency.command,
                latency.p50_nanos as f64 / 1000.0,
                latency.p99_nanos as f64 / 1000.0,
                latency.p999_nanos as f64 / 1000.0,
                latency.samples,
            )?;
        }
    }

    let logs = store.log_stats();

    write!(
        info,
        "# Logs\r\nlog_pages:{}\r\nlog_extents:{}\r\nlog_free_bytes:{}\r\nlog_pages_released_total:{}\r\nlog_relocated_bytes_total:{}\r\n",
        logs.pages, logs.extents, logs.free_bytes, logs.pages_released, logs.relocated_bytes,
    )?;

    let slabs = store.slab_stats();
    let slab_pages: u64 = slabs.iter().map(|class| class.pages).sum();
    let slab_live: u64 = slabs.iter().map(|class| class.live).sum();

    write!(
        info,
        "# Slabs\r\nslab_pages:{slab_pages}\r\nslab_records:{slab_live}\r\n"
    )?;

    for class in slabs.iter().filter(|class| class.pages > 0) {
        write!(
            info,
            "slab_class_{}b:pages={},records={}\r\n",
            class.slot_bytes, class.pages, class.live
        )?;
    }

    let durable_sequence = persistence.durable().load(Ordering::Acquire);
    let replica_count = persistence.replica_count().load(Ordering::Acquire);
    let oldest_sent = persistence.backlog_floor().load(Ordering::Acquire);
    let role = match state.role.get() {
        crate::config::ReplicationRole::Off => "off",
        crate::config::ReplicationRole::Primary => "primary",
        crate::config::ReplicationRole::Replica => "replica",
    };

    let replica_sequence = if replica_count == 0 { 0 } else { oldest_sent };
    let dropped = crate::persistence::REPLICA_HOLDS_DROPPED.load(Ordering::Relaxed);

    let send_lag = if replica_count == 0 {
        0
    } else {
        durable_sequence.saturating_sub(oldest_sent)
    };

    write!(
        info,
        "# Replication\r\nreplication_role:{role}\r\nwal_durable_sequence:{durable_sequence}\r\nreplication_replicas:{replica_count}\r\nreplication_replica_sequence:{replica_sequence}\r\nreplication_send_lag:{send_lag}\r\nreplica_retain_dropped:{dropped}\r\nreplication_epoch:{epoch}\r\nreplication_fenced:{fenced}\r\nreplication_committed_sequence:{committed}\r\n",
        epoch = state.epoch.get(),
        committed = if state.role.get() == crate::config::ReplicationRole::Replica {
            state.replica_link.committed()
        } else {
            state.durable_replies.committed()
        },
        fenced = u8::from(state.write_gate.get() == crate::replication::WriteGate::Fenced),
    )?;
    // Redis's lines for each replica that announced where clients reach it; replicas that
    // announced nothing, or are still copying, are counted above but not listed.
    for (index, replica) in state.durable_acks.replicas().iter().enumerate() {
        write!(
            info,
            "slave{index}:ip={},port={},state=online,offset={},lag={}\r\n",
            replica.endpoint.host,
            replica.endpoint.port,
            replica.sequence,
            replica.lag.as_secs()
        )?;
    }
    if state.role.get() == crate::config::ReplicationRole::Replica {
        let link = &state.replica_link;
        // A replica whose log refuses frames is not following its primary.
        let status = if link.is_up() && persistence.failure().is_none() {
            "up"
        } else {
            "down"
        };
        let since = link
            .seconds_since_io()
            .map_or_else(|| "-1".to_string(), |seconds| seconds.to_string());
        let applied = persistence.last_sequence();
        let resync = state.replica_resync_sequence.load(Ordering::Acquire) != 0;

        write!(
            info,
            "master_link_status:{status}\r\nmaster_last_io_seconds_ago:{since}\r\nslave_repl_offset:{applied}\r\nmaster_sync_in_progress:{}\r\n",
            u8::from(resync)
        )?;
    }
    info.extend_from_slice(info_section(state.cluster_view()).as_bytes());
    Ok(match section {
        Some(name) => only_section(&info, name),
        None => info,
    })
}

/// The `# Name` blocks of `info` that `INFO name` asks for. Names compare without case or
/// spaces (`commandcounters`); `all`, `everything` and `default` keep every block, and an
/// unknown name gives an empty reply, as in Redis.
fn only_section(info: &[u8], name: &[u8]) -> Vec<u8> {
    let wanted: Vec<u8> = name.to_ascii_lowercase();

    if matches!(wanted.as_slice(), b"all" | b"everything" | b"default") {
        return info.to_vec();
    }

    let mut out = Vec::new();
    let mut keep = false;

    for line in info.split_inclusive(|byte| *byte == b'\n') {
        if let Some(header) = line.strip_prefix(b"# ") {
            let header: Vec<u8> = header
                .iter()
                .filter(|byte| !byte.is_ascii_whitespace())
                .map(u8::to_ascii_lowercase)
                .collect();

            keep = header == wanted;
        }
        if keep {
            out.extend_from_slice(line);
        }
    }
    out
}

fn command_latency(state: &ShardState) -> Vec<crate::metrics::CommandLatency> {
    let commands = crate::protocol::all_commands();

    state
        .latency
        .borrow()
        .iter()
        .map(|(index, histogram)| crate::metrics::CommandLatency {
            command: commands[index].name,
            samples: histogram.count(),
            sum_nanos: histogram.sum_nanos(),
            buckets: histogram.cumulative(),
            p50_nanos: histogram.quantile(0.5),
            p99_nanos: histogram.quantile(0.99),
            p999_nanos: histogram.quantile(0.999),
        })
        .collect()
}

pub(super) fn evented_snapshot(state: &ShardState) -> crate::metrics::MetricsSnapshot {
    let store = state.store.borrow();
    let command_metrics = *state.command_metrics.borrow();
    let persistence = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let memory = store.memory_usage();

    let admission = crate::admission::admission_totals();
    let (net_input, net_output) = super::net::totals();

    crate::metrics::MetricsSnapshot {
        protocol_errors: admission.protocol_errors,
        backend: "evented",
        durability: persistence.profile().to_string(),
        persistence_state: if persistence.failure().is_some() {
            "error"
        } else {
            "ok"
        },
        recovered_wal_records: state.recovery.records_replayed,
        wal_last_sequence: persistence.last_sequence(),
        wal_unsynced_bytes: persistence.dirty_bytes(),
        shard_id: state
            .route
            .as_ref()
            .map_or(0, |route| route.shard_id as u64),
        shard_count: state
            .route
            .as_ref()
            .map_or(1, |route| route.shard_count as u64),
        allocator: if cfg!(feature = "mimalloc-allocator") {
            "mimalloc"
        } else {
            "system"
        },
        connections: admission.accepted,
        active_connections: state.active_connections.get(),
        rejected_connections: admission.rejected,
        rate_limited_commands: admission.rate_limited,
        commands: command_metrics.commands,
        gets: command_metrics.gets,
        sets: command_metrics.sets,
        increments: command_metrics.increments,
        other_commands: command_metrics.other_commands,
        get_command_duration_nanos: command_metrics.get_duration_nanos,
        set_command_duration_nanos: command_metrics.set_duration_nanos,
        increment_command_duration_nanos: command_metrics.increment_duration_nanos,
        other_command_duration_nanos: command_metrics.other_duration_nanos,
        wal_sync_count: command_metrics.wal_sync_count,
        wal_sync_duration_nanos: command_metrics.wal_sync_duration_nanos,
        input_bytes: net_input,
        output_bytes: net_output,
        charged_memory_bytes: memory.charged_bytes() as u64,
        key_memory_bytes: memory.key_bytes as u64,
        value_memory_bytes: memory.value_bytes as u64,
        collection_memory_bytes: memory.collection_bytes as u64,
        index_memory_bytes: memory.index_bytes as u64,
        pooled_value_bytes: memory.pooled_bytes as u64,
        process_resident_memory_bytes: crate::metrics::process_resident_memory_bytes(),
        keys: store.len() as u64,
        expired_keys_passive: store.expiration_stats().expired_keys_passive,
        expired_keys_active: store.expiration_stats().expired_keys_active,
        keys_with_ttl: store.expiration_stats().keys_with_ttl,
        evicted_keys: store.memory_stats().evicted_keys,
        eviction_scans: store.memory_stats().eviction_scans,
        eviction_slots_inspected: store.memory_stats().eviction_slots_inspected,
        allocation_events: store.memory_stats().allocation_events,
        allocation_bytes: store.memory_stats().allocation_bytes,
        command_latency: command_latency(state),
        up: true,
        ready: true,
    }
}
