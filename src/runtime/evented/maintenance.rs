use super::*;

pub(super) const READ_CHUNK_BYTES: usize = 8 * 1024;
/// Unparsed input at least this large gets its incomplete frame's size computed once.
pub(super) const LARGE_FRAME_HINT_BYTES: usize = 512;
pub(super) const EXPIRE_MAINTENANCE_TIME: Duration = Duration::from_micros(750);
pub(super) const EXPIRE_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(25);
pub(super) const EXPIRE_MAINTENANCE_MAX_KEYS: usize = 16_384;
/// Keys startup expires and logs per group.
pub(super) const STARTUP_EXPIRE_BATCH: usize = 65_536;

pub(super) fn expire_maintenance<C: crate::time::Clock>(
    store: &mut KvShard<C>,
    batch: usize,
    max_time: Duration,
    max_keys: usize,
) -> usize {
    store.json_index_build_step(256);
    if batch == 0 || max_keys == 0 {
        return 0;
    }
    let started = Instant::now();
    let mut reaped = store.expire_cycle(batch.min(max_keys));
    while reaped >= batch && reaped < max_keys && started.elapsed() < max_time {
        let removed = store.expire_cycle(batch.min(max_keys - reaped));
        reaped += removed;
        if removed < batch {
            break;
        }
    }
    reaped
}

/// Logs the deletes of keys active expiry removed, as one group: with `always` the batch
/// shares one write and one sync instead of a sync per key. A refused record stays queued,
/// and the next write records it before its own.
pub(super) fn log_expired_removals<C: crate::time::Clock>(
    store: &mut KvShard<C>,
    persistence: &mut WalSink,
) {
    use crate::mutation::MutationSink;

    let active = store.active_database();

    persistence.begin_group();
    let start = persistence.last_sequence();
    let logged = store
        .drain_removals(None, false, |database, key| {
            let keys = [std::borrow::Cow::Borrowed(key)];

            persistence.select_database(database);
            persistence.record(crate::mutation::Mutation::Delete { keys: &keys })
        })
        .and(persistence.end_group())
        // Nobody waits for these records: under `always` the sync worker syncs them (a
        // client reply after them waits for that sync), so a slow disk does not hold the
        // shard here.
        .and_then(|()| persistence.sync_batch_soon(start));

    persistence.select_database(active);
    if let Err(error) = logged {
        warn!(%error, "expired keys could not be logged as deleted; retrying before the next write");
    }
}

/// One slice of eviction debt, recorded like a write's own evictions. Returns true
/// while some is still owed.
pub(super) fn pay_eviction_debt(state: &ShardState, store: &mut KvShard) -> bool {
    let owed = if state.published_config.persistence.profile.uses_wal() {
        let mut persistence = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        evict_debt_logged(store, &mut persistence, EVICTION_DEBT_SLICE)
    } else {
        evict_debt(store, &mut NoopMutationSink, EVICTION_DEBT_SLICE)
            .map_err(|never| match never {})
    };

    owed.unwrap_or_else(|error| {
        warn!(
            %error,
            "eviction could not log its deletes (a failed or full WAL); this debt is dropped \
             and a later write that needs memory evicts again"
        );
        store.clear_eviction_debt();
        false
    })
}

/// Pays eviction debt for up to `max_time`, logging the deletes as one group with one
/// sync: synced one by one, an `always` log lets a slice evict only a few keys.
pub(super) fn evict_debt_logged<C: crate::time::Clock>(
    store: &mut KvShard<C>,
    persistence: &mut WalSink,
    max_time: Duration,
) -> Result<bool, crate::persistence::PersistenceError> {
    persistence.begin_group();
    let start = persistence.last_sequence();
    let owed = evict_debt(store, persistence, max_time);

    persistence
        .end_group()
        .and_then(|()| persistence.sync_batch_soon(start))
        .and(owed)
}

/// Time one maintenance turn spends paying eviction debt.
const EVICTION_DEBT_SLICE: Duration = Duration::from_millis(2);

pub(super) fn expire_waiters(state: &ShardState) {
    if state.waiters.borrow().is_empty() {
        return;
    }

    for (slot, timeout) in state.waiters.borrow_mut().expire(Instant::now()) {
        slot.fill(timeout);
    }
}
