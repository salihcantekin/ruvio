use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{BTreeSet, VecDeque},
    mem::size_of,
    sync::Arc,
};

use ahash::{AHashMap, AHashSet};

use crate::{
    EvictionPolicy, MemoryLimits,
    index::{
        AHashIndex, HEAP_SLOT, IndexRef, OBJECT_SLOT, RECORD_INTEGER, RECORD_LOG, RECORD_SLOT,
        SLAB_SLOT, SlabTouch, StorageIndex, TAG_SET_SLAB, TAG_ZSET_SLAB, reclaim_capacity,
    },
    memory::{MemoryUsage, alloc_size},
    time::{Clock, SystemClock, UnixMillis},
};

mod access;
mod arena_table;
mod bloom;
mod evict;
mod expire;
mod fence;
mod fill;
mod hash;
mod intset;
pub(crate) mod json_indexing;
mod limit;
mod list;
mod pool;
mod set;
mod slab_ops;
mod slot_watch;
mod smallbuf;
mod snapshot;
mod stream;
mod string;
#[cfg(test)]
mod tests;
mod value;
pub(crate) use fill::FillState;
pub(crate) use limit::{LIMIT_SLIDING_MAX, LIMIT_TOKENS_MAX_CAPACITY, LIMIT_TOKENS_MAX_REFILL_MS};
pub(crate) use string::{ReadMark, SMALL_JSON_EDIT};
mod zset;

use access::*;
pub use bloom::*;
use expire::*;
pub use hash::*;
pub use list::*;
use pool::*;
pub use set::*;
pub(crate) use snapshot::*;
pub use value::*;
pub use zset::*;

/// Encoding thresholds for lists and sets, taken from `MemoryLimits`.
#[derive(Clone, Copy, Debug)]
pub(super) struct CollectionLimits {
    pub(super) set_max_intset_entries: usize,
    pub(super) list_max_entries: usize,
    pub(super) list_max_item_bytes: usize,
}

impl Default for CollectionLimits {
    fn default() -> Self {
        let limits = MemoryLimits::default();

        Self {
            set_max_intset_entries: limits.set_max_intset_entries,
            list_max_entries: limits.list_max_compact_entries,
            list_max_item_bytes: limits.list_max_compact_value_bytes,
        }
    }
}

pub const EVICTION_TURN_BUDGET: usize = 16;

/// Deadlines averaged for the `avg_ttl` field of `INFO keyspace`.
const KEYSPACE_TTL_SAMPLES: usize = 64;

/// A well-mixed value for picking a random bucket; successive calls differ, and so do
/// the sequences of two processes.
pub(super) fn sample_seed() -> u64 {
    static PROCESS_KEY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

    let key = *PROCESS_KEY.get_or_init(|| {
        use std::hash::BuildHasher;

        std::collections::hash_map::RandomState::new().hash_one(0x5eed_u64)
    });
    let mut x = SET_SAMPLE
        .fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        ^ key;

    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

static SET_SAMPLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A uniform value in `0..n`; `n` must be positive.
pub(super) fn random_below(n: usize) -> usize {
    ((u128::from(sample_seed()) * n as u128) >> 64) as usize
}

/// `k` distinct positions of `0..n` (`k` at most `n`), each drawn independently and
/// uniformly, in random order. Expected cost `O(k)`: a dense draw shuffles the
/// positions, a sparse one rejects repeats.
pub(super) fn distinct_positions(n: usize, k: usize) -> Vec<usize> {
    let k = k.min(n);

    if k.saturating_mul(2) >= n {
        let mut all: Vec<usize> = (0..n).collect();

        for slot in 0..k {
            all.swap(slot, slot + random_below(n - slot));
        }

        all.truncate(k);
        return all;
    }

    // Under half of the positions are taken, so each draw is new with odds above 1/2.
    let mut seen = AHashSet::with_capacity(k);
    let mut out = Vec::with_capacity(k);

    while out.len() < k {
        let at = random_below(n);

        if seen.insert(at) {
            out.push(at);
        }
    }

    out
}

/// A position cursor for a collection paged by position. A bucket cursor (bucket index
/// in the high half) comes from the same key before it changed encoding, so the walk
/// starts over instead of ending early.
/// Slots one hashed-collection SCAN page may inspect: a home is read along its probe
/// group, so a page looks at more slots than it returns, but never without bound.
pub(super) fn scan_budget(count: usize) -> usize {
    count.saturating_mul(32).max(512)
}

/// Marks a cursor of an arena collection, so a cursor from another encoding of the same
/// key (a [`scan_by_hash`] one, or a stale plain number) starts the walk over instead
/// of being read as a home and skipping members.
const HOME_CURSOR: u64 = 1 << 62;

/// The home position a hashed-collection scan resumes from.
pub(super) fn home_cursor_in(cursor: u64) -> u64 {
    if cursor & (HOME_CURSOR | HASH_ORDER_CURSOR) == HOME_CURSOR {
        cursor & !HOME_CURSOR
    } else {
        0
    }
}

/// Marks a cursor of [`scan_by_hash`]; its other bits are a position in hash order.
const HASH_ORDER_CURSOR: u64 = 1 << 63;

/// One page of a collection whose table gives no stable slot order (the boxed spill
/// tables): items in order of `hash >> 2` under the table's own hasher, which no
/// insert or removal changes, from where `cursor` left off. An item present for the
/// whole walk is returned at least once and the walk ends, however the table changes
/// between calls. Each page still reads every item, so a full walk costs
/// `O(len² / count)`; these encodings are rare spills, not the common case.
pub(super) fn scan_by_hash<T>(
    items: impl Iterator<Item = T>,
    hash_of: impl Fn(&T) -> u64,
    cursor: u64,
    count: usize,
) -> (u64, Vec<T>) {
    let start = if cursor & HASH_ORDER_CURSOR == 0 {
        0
    } else {
        cursor & !HASH_ORDER_CURSOR
    };
    let count = count.max(1);
    let mut ahead: Vec<(u64, T)> = items
        .map(|item| (hash_of(&item) >> 2, item))
        .filter(|(order, _)| *order >= start)
        .collect();

    if ahead.len() <= count {
        return (0, ahead.into_iter().map(|(_, item)| item).collect());
    }

    ahead.select_nth_unstable_by_key(count - 1, |(order, _)| *order);
    let last = ahead[..count]
        .iter()
        .map(|(order, _)| *order)
        .max()
        .expect("count is at least one");
    // Items sharing the last order value but left out are returned next time, with
    // the ones already sent; only more than `count` equal 62-bit values could stall.
    let tied = ahead[count..].iter().any(|(order, _)| *order == last);
    let next = if tied { last } else { last + 1 };

    ahead.truncate(count);
    (
        next | HASH_ORDER_CURSOR,
        ahead.into_iter().map(|(_, item)| item).collect(),
    )
}

/// The cursor a hashed-collection scan hands back; 0 when the walk is over.
pub(super) fn home_cursor_out(next: u64) -> u64 {
    if next == 0 { 0 } else { next | HOME_CURSOR }
}

/// SCAN examines at most this many slots per requested key (Redis: 10 buckets).
const SCAN_SLOT_FACTOR: usize = 10;

const VALUE_POOL_CLASSES: usize = 48;
const MAX_POOLED_VALUE_BYTES: usize = 64 * 1024;
const MAX_BUFFERS_PER_CLASS: usize = 64;
pub(crate) const LOG_VALUE_MAX: usize = 64;

/// Returned when a command targets a key containing an incompatible data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamWriteError {
    WrongType,
    Id,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupError {
    WrongType,
    MissingKey,
    Busy,
    NoGroup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamSetIdError {
    Missing,
    WrongType,
    TooSmall,
}

impl From<WrongType> for StreamWriteError {
    fn from(_: WrongType) -> Self {
        Self::WrongType
    }
}

impl From<WrongType> for StreamGroupError {
    fn from(_: WrongType) -> Self {
        Self::WrongType
    }
}

/// Why a sorted-set insert was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertError {
    InvalidScore,
    /// The member arena has no offsets left.
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidBloomConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementError {
    WrongType,
    NotIntegerOrOverflow,
}

/// Shard-local expiration counters. Linux scrapes read these as ordinary
/// integers; the hot path does not take atomics or a metrics lock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpirationStats {
    pub expired_keys: u64,
    pub expired_keys_passive: u64,
    pub expired_keys_active: u64,
    pub keys_with_ttl: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryStats {
    pub evicted_keys: u64,
    /// Victim searches started under memory pressure, including searches that found no victim.
    pub eviction_scans: u64,
    /// Index slots, including empty hash buckets, inspected by victim searches.
    pub eviction_slots_inspected: u64,
    pub allocation_events: u64,
    pub allocation_bytes: u64,
}

#[derive(Clone, Copy)]
enum ExpireCause {
    Passive,
    Active,
}

/// A single-owner in-memory key-value shard.
///
/// The type deliberately contains no synchronization. A thread-per-core runtime
/// should create one shard per executor and route each key to exactly one owner.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct KvShard<C = SystemClock, I = AHashIndex> {
    entries: I,
    byte_pool: BytePool,
    value_log: ValueLog,
    /// Databases taken out by `take_database` and not yet released or restored. They
    /// still own spans in `value_log`, so it is only reset when this is zero.
    detached_databases: usize,
    /// Set when a flush handed whole databases to the lazy-free thread. Pages freed there
    /// return to this thread's heap, which gives them back to the OS only when it collects.
    collect_after_lazy_free: bool,
    memory_usage: MemoryUsage,
    clock: C,
    snapshot: Option<SnapshotSession>,
    snapshot_aborts: u64,
    memory_limit: Option<usize>,
    databases: Vec<Option<I>>,
    active: u8,
    parked_entries: usize,
    parked_index_bytes: usize,
    deadline_index_bytes: usize,
    /// Large sets and hashes with an unfinished resize or arena compaction, by
    /// `(database, key)`. Writes to the collection drive that work; maintenance finishes
    /// it for one that went quiet, so its memory is still returned.
    collection_work: AHashSet<(u8, Box<[u8]>)>,
    watches: AHashMap<u64, Vec<(u8, Arc<[u8]>)>>,
    watchers_by_key: Option<AHashMap<u8, AHashMap<Arc<[u8]>, AHashSet<u64>>>>,
    /// Keys of a slot range written while it is copied to another shard.
    slot_watch: Option<Box<slot_watch::SlotWatch>>,
    /// `watchers_by_key` or `slot_watch` is set: one test on every write.
    observing: bool,
    dirty_watches: AHashSet<u64>,
    /// Tasks to wake when a watch of theirs turns dirty: clients blocked in a command
    /// that is retried when one of its keys changes.
    watch_wakers: AHashMap<u64, std::task::Waker>,
    expiration: ExpirationStats,
    eviction_policy: EvictionPolicy,
    eviction_sample_size: usize,
    active_expire_keys: usize,
    hash_max_compact_entries: usize,
    hash_max_compact_value_bytes: usize,
    collection_limits: CollectionLimits,
    access_rng: u32,
    /// Created by the first victim search; stays absent without an eviction policy.
    eviction_pool: RefCell<Option<Box<EvictionPool>>>,
    deadlines: Vec<DeadlineDb>,
    soonest_deadline: Option<UnixMillis>,
    expire_cursor: usize,
    /// `None` until a hash field gets a TTL. Each timed hash sits here once, under its
    /// earliest field deadline, so the maintenance tick finds due fields without a scan.
    field_sweep: Option<Box<FieldSweep>>,
    /// `None` until the first `JSON.INDEX CREATE`.
    json_indexes: Option<Box<crate::json::index::JsonIndexes>>,
    /// `None` until the first `FILLGET`.
    fills: Option<Box<fill::FillTable>>,
    cached_now: Cell<Option<u64>>,
    eviction_cursor: Cell<usize>,
    /// Set when a write was admitted before eviction made room for it, naming the key it
    /// wrote (database, key); maintenance keeps evicting other keys until the shard is
    /// back under its limit.
    eviction_debt: Option<(u8, Box<[u8]>)>,
    removals: expire::RemovalLog,
    /// Victim searches and index slots they inspected.
    eviction_scan: Cell<(u64, u64)>,
    memory_stats: MemoryStats,
    notify: crate::notify::NotifyState,
    /// `None` until `CHANGES START`. Mutations do not record while this is absent.
    change_log: Option<std::collections::VecDeque<ChangeNote>>,
    change_next: u64,
    /// Generation in the high bits of new `LEASE` and `SEMAPHORE` tokens.
    fence_generation: u64,
    /// Highest token of `fence_generation` this shard granted or applied.
    fence_last: u64,
    /// Highest token of a later generation applied before this shard learned it (a log
    /// replayed before the epoch was read); taken over when that generation is set.
    fence_ahead: u64,
    /// Lowest clock reading, in milliseconds, new tokens are taken from: one past the checkpoint
    /// a restart loaded, whatever the generation.
    fence_clock_floor: u64,
    /// Log drain round in progress; `None` while every log is dense enough.
    reclaim: Option<Box<ReclaimWalk>>,
}

/// Position of a log drain round: databases in order, and the slot cursor in one.
#[derive(Debug)]
struct ReclaimWalk {
    order: Vec<u8>,
    position: usize,
    cursor: u64,
}

impl Default for KvShard<SystemClock, AHashIndex> {
    fn default() -> Self {
        Self {
            entries: AHashIndex::default(),
            byte_pool: BytePool::default(),
            value_log: ValueLog::default(),
            detached_databases: 0,
            collect_after_lazy_free: false,
            memory_usage: MemoryUsage::default(),
            clock: SystemClock::default(),
            snapshot: None,
            snapshot_aborts: 0,
            memory_limit: None,
            databases: Vec::new(),
            active: 0,
            parked_entries: 0,
            parked_index_bytes: 0,
            deadline_index_bytes: 0,
            collection_work: AHashSet::new(),
            watches: AHashMap::new(),
            watchers_by_key: None,
            slot_watch: None,
            observing: false,
            dirty_watches: AHashSet::new(),
            watch_wakers: AHashMap::new(),
            expiration: ExpirationStats::default(),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_sample_size: MemoryLimits::default().maxmemory_samples,
            active_expire_keys: MemoryLimits::default().active_expire_keys,
            hash_max_compact_entries: MemoryLimits::default().hash_max_compact_entries,
            hash_max_compact_value_bytes: MemoryLimits::default().hash_max_compact_value_bytes,
            collection_limits: CollectionLimits::default(),
            access_rng: ACCESS_RNG_SEED,
            eviction_pool: RefCell::default(),
            deadlines: Vec::new(),
            soonest_deadline: None,
            expire_cursor: 0,
            field_sweep: None,
            json_indexes: None,
            fills: None,
            cached_now: Cell::new(None),
            eviction_cursor: Cell::new(0),
            eviction_debt: None,
            removals: expire::RemovalLog::default(),
            eviction_scan: Cell::new((0, 0)),
            memory_stats: MemoryStats::default(),
            notify: crate::notify::NotifyState::default(),
            change_log: None,
            change_next: 0,
            fence_generation: 0,
            fence_last: 0,
            fence_ahead: 0,
            fence_clock_floor: 0,
            reclaim: None,
        }
    }
}

impl KvShard<SystemClock, AHashIndex> {
    /// Create an empty shard with no initial allocation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty shard sized for an expected number of entries.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut shard = Self {
            entries: AHashIndex::with_capacity(capacity),
            byte_pool: BytePool::default(),
            value_log: ValueLog::default(),
            detached_databases: 0,
            collect_after_lazy_free: false,
            memory_usage: MemoryUsage::default(),
            clock: SystemClock::default(),
            snapshot: None,
            snapshot_aborts: 0,
            memory_limit: None,
            databases: Vec::new(),
            active: 0,
            parked_entries: 0,
            parked_index_bytes: 0,
            deadline_index_bytes: 0,
            collection_work: AHashSet::new(),
            watches: AHashMap::new(),
            watchers_by_key: None,
            slot_watch: None,
            observing: false,
            dirty_watches: AHashSet::new(),
            watch_wakers: AHashMap::new(),
            expiration: ExpirationStats::default(),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_sample_size: MemoryLimits::default().maxmemory_samples,
            active_expire_keys: MemoryLimits::default().active_expire_keys,
            hash_max_compact_entries: MemoryLimits::default().hash_max_compact_entries,
            hash_max_compact_value_bytes: MemoryLimits::default().hash_max_compact_value_bytes,
            collection_limits: CollectionLimits::default(),
            access_rng: ACCESS_RNG_SEED,
            eviction_pool: RefCell::default(),
            deadlines: Vec::new(),
            soonest_deadline: None,
            expire_cursor: 0,
            field_sweep: None,
            json_indexes: None,
            fills: None,
            cached_now: Cell::new(None),
            eviction_cursor: Cell::new(0),
            eviction_debt: None,
            removals: expire::RemovalLog::default(),
            eviction_scan: Cell::new((0, 0)),
            memory_stats: MemoryStats::default(),
            notify: crate::notify::NotifyState::default(),
            change_log: None,
            change_next: 0,
            fence_generation: 0,
            fence_last: 0,
            fence_ahead: 0,
            fence_clock_floor: 0,
            reclaim: None,
        };

        shard.refresh_global_memory_usage();
        shard
    }
}

impl<C: Clock> KvShard<C, AHashIndex> {
    pub fn with_clock(clock: C) -> Self {
        Self::with_capacity_and_clock(0, clock)
    }

    pub fn with_capacity_and_clock(capacity: usize, clock: C) -> Self {
        let mut shard = Self {
            entries: AHashIndex::with_capacity(capacity),
            byte_pool: BytePool::default(),
            value_log: ValueLog::default(),
            detached_databases: 0,
            collect_after_lazy_free: false,
            memory_usage: MemoryUsage::default(),
            clock,
            snapshot: None,
            snapshot_aborts: 0,
            memory_limit: None,
            databases: Vec::new(),
            active: 0,
            parked_entries: 0,
            parked_index_bytes: 0,
            deadline_index_bytes: 0,
            collection_work: AHashSet::new(),
            watches: AHashMap::new(),
            watchers_by_key: None,
            slot_watch: None,
            observing: false,
            dirty_watches: AHashSet::new(),
            watch_wakers: AHashMap::new(),
            expiration: ExpirationStats::default(),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_sample_size: MemoryLimits::default().maxmemory_samples,
            active_expire_keys: MemoryLimits::default().active_expire_keys,
            hash_max_compact_entries: MemoryLimits::default().hash_max_compact_entries,
            hash_max_compact_value_bytes: MemoryLimits::default().hash_max_compact_value_bytes,
            collection_limits: CollectionLimits::default(),
            access_rng: ACCESS_RNG_SEED,
            eviction_pool: RefCell::default(),
            deadlines: Vec::new(),
            soonest_deadline: None,
            expire_cursor: 0,
            field_sweep: None,
            json_indexes: None,
            fills: None,
            cached_now: Cell::new(None),
            eviction_cursor: Cell::new(0),
            eviction_debt: None,
            removals: expire::RemovalLog::default(),
            eviction_scan: Cell::new((0, 0)),
            memory_stats: MemoryStats::default(),
            notify: crate::notify::NotifyState::default(),
            change_log: None,
            change_next: 0,
            fence_generation: 0,
            fence_last: 0,
            fence_ahead: 0,
            fence_clock_floor: 0,
            reclaim: None,
        };

        shard.refresh_global_memory_usage();
        shard
    }
}

fn index_key_bytes<'a, I: StorageIndex>(
    entries: &'a I,
    databases: &'a [Option<I>],
    active: u8,
    db: u8,
    address: u32,
) -> Option<&'a [u8]> {
    if db == active {
        return entries.key_bytes_at(address);
    }

    databases.get(db as usize)?.as_ref()?.key_bytes_at(address)
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub fn with_index_and_clock(index: I, clock: C) -> Self {
        let mut shard = Self {
            entries: index,
            byte_pool: BytePool::default(),
            value_log: ValueLog::default(),
            detached_databases: 0,
            collect_after_lazy_free: false,
            memory_usage: MemoryUsage::default(),
            clock,
            snapshot: None,
            snapshot_aborts: 0,
            memory_limit: None,
            databases: Vec::new(),
            active: 0,
            parked_entries: 0,
            parked_index_bytes: 0,
            deadline_index_bytes: 0,
            collection_work: AHashSet::new(),
            watches: AHashMap::new(),
            watchers_by_key: None,
            slot_watch: None,
            observing: false,
            dirty_watches: AHashSet::new(),
            watch_wakers: AHashMap::new(),
            expiration: ExpirationStats::default(),
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_sample_size: MemoryLimits::default().maxmemory_samples,
            active_expire_keys: MemoryLimits::default().active_expire_keys,
            hash_max_compact_entries: MemoryLimits::default().hash_max_compact_entries,
            hash_max_compact_value_bytes: MemoryLimits::default().hash_max_compact_value_bytes,
            collection_limits: CollectionLimits::default(),
            access_rng: ACCESS_RNG_SEED,
            eviction_pool: RefCell::default(),
            deadlines: Vec::new(),
            soonest_deadline: None,
            expire_cursor: 0,
            field_sweep: None,
            json_indexes: None,
            fills: None,
            cached_now: Cell::new(None),
            eviction_cursor: Cell::new(0),
            eviction_debt: None,
            removals: expire::RemovalLog::default(),
            eviction_scan: Cell::new((0, 0)),
            memory_stats: MemoryStats::default(),
            notify: crate::notify::NotifyState::default(),
            change_log: None,
            change_next: 0,
            fence_generation: 0,
            fence_last: 0,
            fence_ahead: 0,
            fence_clock_floor: 0,
            reclaim: None,
        };

        shard.refresh_global_memory_usage();
        shard
    }

    /// Bytes charged to the memory budget for `key`: key, value and collection storage.
    pub fn key_memory_usage(&self, key: &[u8]) -> Option<usize> {
        self.live(key)?;

        let charge = self.entry_charge(key);

        Some(charge.key_bytes + charge.value_bytes + charge.collection_bytes)
    }

    fn entry_charge(&self, key: &[u8]) -> EntryCharge {
        let Some((stored_key, hit)) = self.entries.get_key_value(key) else {
            return EntryCharge::default();
        };

        charge_of(stored_key, hit)
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Edits the live object stored under `key` with one index lookup (plus one when keys
    /// carry deadlines, to drop an expired one), charging the change from the entry in hand.
    /// `edit` says whether it changed the value; an unchanged value is not charged or
    /// announced. `None` for a missing or expired key or a value that is not an object.
    fn edit_object<R>(
        &mut self,
        key: &[u8],
        edit: impl FnOnce(&mut Entry) -> (R, bool),
    ) -> Option<R> {
        if self.remove_expired(key) {
            return None;
        }

        let entry = self.entries.object_mut(key)?;
        let before = EntryCharge::for_entry(key, entry);
        let (result, changed) = edit(entry);

        if changed {
            let after = EntryCharge::for_entry(key, entry);
            self.apply_entry_charge(before, after, key);
        }

        Some(result)
    }

    fn reconcile_entry_charge(&mut self, before: EntryCharge, key: &[u8]) {
        let after = self.entry_charge(key);
        self.apply_entry_charge(before, after, key);
    }

    fn apply_entry_charge(&mut self, before: EntryCharge, after: EntryCharge, key: &[u8]) {
        if arena_table::background_work_started() {
            self.note_collection_work(key);
        }

        let charged_before = self.memory_usage.charged_bytes();

        self.memory_usage.key_bytes = self
            .memory_usage
            .key_bytes
            .saturating_sub(before.key_bytes)
            .saturating_add(after.key_bytes);
        self.memory_usage.value_bytes = self
            .memory_usage
            .value_bytes
            .saturating_sub(before.value_bytes)
            .saturating_add(after.value_bytes);
        self.memory_usage.collection_bytes = self
            .memory_usage
            .collection_bytes
            .saturating_sub(before.collection_bytes)
            .saturating_add(after.collection_bytes);
        self.note_mutation(key);
        if after.key_bytes != 0 || after.value_bytes != 0 || after.collection_bytes != 0 {
            self.note_write(key);
        }
        self.refresh_global_memory_usage();
        let charged_after = self.memory_usage.charged_bytes();

        if charged_after > charged_before {
            self.memory_stats.allocation_events =
                self.memory_stats.allocation_events.saturating_add(1);
            self.memory_stats.allocation_bytes = self
                .memory_stats
                .allocation_bytes
                .saturating_add((charged_after - charged_before) as u64);
        }
    }

    /// A change that moves no stored bytes (a deadline, a counter in place): marks the key
    /// changed exactly as a charged write does in `apply_entry_charge`.
    #[inline]
    pub(super) fn note_changed(&mut self, key: &[u8]) {
        self.note_mutation(key);
        self.note_write(key);
    }

    /// Inlined so a store with no watched key pays one test, not a call.
    #[inline]
    fn note_mutation(&mut self, key: &[u8]) {
        if self.observing {
            self.note_observed_mutation(key);
        }
    }

    #[inline(never)]
    fn note_observed_mutation(&mut self, key: &[u8]) {
        if let Some(watch) = self.slot_watch.as_mut() {
            watch.note(key);
        }
        self.note_watched_mutation(key);
    }

    fn refresh_observing(&mut self) {
        self.observing = self.watchers_by_key.is_some() || self.slot_watch.is_some();
    }

    fn note_watched_mutation(&mut self, key: &[u8]) {
        if let Some(connections) = self
            .watchers_by_key
            .as_ref()
            .and_then(|databases| databases.get(&self.active))
            .and_then(|keys| keys.get(key))
        {
            self.dirty_watches.extend(connections.iter().copied());
            if !self.watch_wakers.is_empty() {
                for conn in connections {
                    if let Some(waker) = self.watch_wakers.remove(conn) {
                        waker.wake();
                    }
                }
            }
        }
    }

    /// Marks every watch on a key of `databases` dirty, as FLUSHDB and SWAPDB do in Redis.
    fn note_database_rewrite(&mut self, databases: &[u8]) {
        if let Some(watch) = self.slot_watch.as_mut() {
            watch.note_rewrite();
        }
        for (conn, keys) in &self.watches {
            if keys.iter().any(|(db, _)| databases.contains(db)) {
                self.dirty_watches.insert(*conn);
                if let Some(waker) = self.watch_wakers.remove(conn) {
                    waker.wake();
                }
            }
        }
    }

    pub(crate) fn watch(&mut self, conn: u64, database: u8, keys: &[impl AsRef<[u8]>]) {
        let watched = self.watches.entry(conn).or_default();

        for key in keys {
            let key = key.as_ref();
            if watched
                .iter()
                .any(|(db, existing)| *db == database && existing.as_ref() == key)
            {
                continue;
            }

            let keys = self
                .watchers_by_key
                .get_or_insert_with(AHashMap::new)
                .entry(database)
                .or_default();
            let owned = if let Some((existing, _)) = keys.get_key_value(key) {
                let owned = Arc::clone(existing);
                keys.get_mut(key).unwrap().insert(conn);
                owned
            } else {
                let owned: Arc<[u8]> = key.into();
                keys.insert(Arc::clone(&owned), AHashSet::from_iter([conn]));
                owned
            };
            watched.push((database, owned));
        }
        self.refresh_observing();
    }

    pub(crate) fn unwatch(&mut self, conn: u64) {
        if let Some(watched) = self.watches.remove(&conn)
            && let Some(databases) = self.watchers_by_key.as_mut()
        {
            for (db, key) in watched {
                if let Some(keys) = databases.get_mut(&db) {
                    if let Some(connections) = keys.get_mut(key.as_ref()) {
                        connections.remove(&conn);
                        if connections.is_empty() {
                            keys.remove(key.as_ref());
                        }
                    }
                    if keys.is_empty() {
                        databases.remove(&db);
                    }
                }
            }
            if databases.is_empty() {
                self.watchers_by_key = None;
                self.refresh_observing();
            }
        }
        self.dirty_watches.remove(&conn);
        self.watch_wakers.remove(&conn);
    }

    pub(crate) fn watches_dirty(&self, conn: u64) -> bool {
        self.dirty_watches.contains(&conn)
    }

    /// Whether a watched key changed since the last call; the watch stays in place.
    pub(crate) fn take_watch_change(&mut self, conn: u64) -> bool {
        self.dirty_watches.remove(&conn)
    }

    /// Wakes `waker` the next time one of `conn`'s watched keys changes.
    pub(crate) fn wake_on_watch(&mut self, conn: u64, waker: &std::task::Waker) {
        if self.dirty_watches.contains(&conn) {
            waker.wake_by_ref();
        } else {
            self.watch_wakers.insert(conn, waker.clone());
        }
    }

    /// Wakes every task waiting on a watch, as shutdown does.
    pub(crate) fn wake_all_watchers(&mut self) {
        for (_, waker) in self.watch_wakers.drain() {
            waker.wake();
        }
    }

    // A new value type must say here whether a copy shares log spans: no wildcard arm.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(crate) fn clone_entry(&self, key: &[u8]) -> Option<Entry> {
        Some(match self.entries.lookup(key)? {
            IndexRef::Log { offset, len } => Entry::fresh(Value::Bytes(BytesValue::from_heap(
                self.value_log.slice(offset, plain_len(len)).to_vec(),
            ))),
            IndexRef::Integer(value) => {
                Entry::fresh(Value::Bytes(BytesValue::from_heap(integer_bytes(value))))
            }

            IndexRef::Heap(bytes) => {
                Entry::fresh(Value::Bytes(BytesValue::from_heap(bytes.to_vec())))
            }

            IndexRef::SetSlab(bytes) => Entry::from_slab_set(bytes),
            IndexRef::SortedSlab(bytes) => Entry::from_slab_sorted(bytes),
            IndexRef::CollSlab(bytes) => Entry::from_slab_coll(bytes),

            IndexRef::Object(entry) => match &entry.value {
                Value::Bytes(value) => Entry::fresh(Value::Bytes(BytesValue::from_heap(
                    value.view(&self.value_log).to_vec(),
                ))),
                // A copy of a log-backed body would share its span, which the live value
                // frees: the saved copy owns its bytes.
                Value::Versioned(value) => {
                    Entry::fresh(Value::Versioned(Box::new(VersionedValue {
                        version: value.version,
                        body: BytesValue::from_heap(value.body.view(&self.value_log).to_vec()),
                    })))
                }
                other @ (Value::Json(_)
                | Value::JsonTree(_)
                | Value::List(_)
                | Value::Set(_)
                | Value::Hash(_)
                | Value::SortedSet(_)
                | Value::BloomFilter(_)
                | Value::Stream(_)
                | Value::Lease(_)
                | Value::Limit(_)
                | Value::Once
                | Value::TimedHash(_)
                | Value::Semaphore(_)) => {
                    debug_assert!(
                        other.string_body().is_none(),
                        "a string body is copied above"
                    );
                    Entry {
                        value: other.clone(),
                    }
                }
            },
        })
    }

    /// Puts back `key` as a rolled-back script or `MULTI` saved it. The key may never have
    /// been written (its command was refused), so a running snapshot may not have captured
    /// it; capture it before the reinsert moves it past the snapshot's walk. Removing the
    /// key took a timed hash out of the field sweep, so it is filed again.
    pub(crate) fn restore_entry(&mut self, key: &[u8], entry: Option<Entry>) {
        self.capture_snapshot_key(key);
        let before = self.entry_charge(key);

        if let Some(previous) = self.remove_entry(key) {
            self.free_entry(key, previous);
        }

        if let Some(mut entry) = entry {
            let timed = entry.value.soonest_field_deadline().is_some();

            // `clone_entry` saves every string on the heap; a short one goes back to the
            // log, where it lived and is charged as it was.
            if let Some(body) = entry.value.string_body_mut()
                && let ByteBody::Heap(data) = &body.body
                && data.len() <= LOG_VALUE_MAX
            {
                *body = self.alloc_bytes(data);
            }

            self.insert_entry(key, entry);

            if timed {
                self.refile_arrived_hash(key);
            }
        }

        self.reconcile_entry_charge(before, key);
    }

    fn refresh_global_memory_usage(&mut self) {
        self.memory_usage.entries = self.entries.len() + self.parked_entries;
        self.memory_usage.index_bytes = self
            .entries
            .charged_bytes()
            .saturating_add(self.parked_index_bytes)
            .saturating_add(self.deadline_index_bytes);
        self.memory_usage.pooled_bytes = self.byte_pool.cached_bytes;
    }

    fn update_deadline_index_bytes(&mut self, db: usize, before: usize) {
        self.deadline_index_bytes =
            self.deadline_index_bytes - before + self.deadlines[db].charged_bytes();
    }

    /// Database whose map every key method uses.
    pub(crate) fn active_database(&self) -> u8 {
        self.active
    }

    /// Makes `database` the map every key method uses. A map is created on first use.
    /// Parked maps do not change, so their totals are cached until they are entered again.
    #[inline]
    pub(crate) fn enter(&mut self, database: u8) {
        if database != self.active {
            self.switch_database(database);
        }
    }

    #[inline(never)]
    fn switch_database(&mut self, database: u8) {
        let next = self
            .slot(database)
            .take()
            .unwrap_or_else(|| I::with_capacity(0));

        self.parked_entries = self.parked_entries.saturating_sub(next.len());
        self.parked_index_bytes = self.parked_index_bytes.saturating_sub(next.charged_bytes());
        let previous = std::mem::replace(&mut self.entries, next);

        self.parked_entries += previous.len();
        self.parked_index_bytes += previous.charged_bytes();
        *self.slot(self.active) = Some(previous);
        self.active = database;
    }

    fn slot(&mut self, database: u8) -> &mut Option<I> {
        let index = usize::from(database);

        if self.databases.len() <= index {
            self.databases.resize_with(index + 1, || None);
        }

        &mut self.databases[index]
    }

    /// Every materialised database map, the active one first.
    fn database_maps(&self) -> impl Iterator<Item = (u8, &I)> {
        std::iter::once((self.active, &self.entries)).chain(
            self.databases
                .iter()
                .enumerate()
                .filter_map(|(db, map)| map.as_ref().map(|map| (db as u8, map))),
        )
    }

    /// Indexes of every materialised database, ascending.
    pub(crate) fn database_ids(&self) -> Vec<u8> {
        let mut ids: Vec<u8> = self.database_maps().map(|(db, _)| db).collect();

        ids.sort_unstable();
        ids
    }

    /// Keys in the active database, expired ones included, like Redis `DBSIZE`.
    pub(crate) fn database_len(&self) -> usize {
        self.entries.len()
    }

    /// Exchanges two databases. Only the map slots move.
    pub(crate) fn swap_databases(&mut self, left: u8, right: u8) {
        if left == right {
            return;
        }

        self.json_index_databases_swapped(left, right);

        let active = self.active;
        let parked = if active == left || active == right {
            let other = if active == left { right } else { left };

            let map = self.slot(other).take();

            *self.slot(other) = Some(std::mem::replace(
                &mut self.entries,
                map.unwrap_or_else(|| I::with_capacity(0)),
            ));
            true
        } else {
            let left_map = self.slot(left).take();
            let right_map = self.slot(right).take();

            *self.slot(left) = right_map;
            *self.slot(right) = left_map;
            false
        };

        if parked {
            self.recount_parked();
        }

        if !self.collection_work.is_empty() {
            self.collection_work = std::mem::take(&mut self.collection_work)
                .into_iter()
                .map(|(database, key)| {
                    let database = if database == left {
                        right
                    } else if database == right {
                        left
                    } else {
                        database
                    };

                    (database, key)
                })
                .collect();
        }

        if self.expiration.keys_with_ttl != 0 {
            Self::swap_deadline_dbs(&mut self.deadlines, left, right);
        }

        if let Some(sweep) = self.field_sweep.take() {
            let swapped = sweep
                .into_iter()
                .map(|(at, database, key)| {
                    let database = if database == left {
                        right
                    } else if database == right {
                        left
                    } else {
                        database
                    };

                    (at, database, key)
                })
                .collect();

            self.field_sweep = Some(Box::new(swapped));
        }

        self.note_database_rewrite(&[left, right]);
        self.refresh_global_memory_usage();
    }

    fn recount_parked(&mut self) {
        self.parked_entries = self.databases.iter().flatten().map(StorageIndex::len).sum();
        self.parked_index_bytes = self
            .databases
            .iter()
            .flatten()
            .map(StorageIndex::charged_bytes)
            .sum();
    }

    /// Detaches every entry of `database` and returns it with its charges, so a failed
    /// transaction can put it back. `release_database` frees it for good.
    pub(crate) fn take_database(&mut self, database: u8) -> DetachedDatabase<I> {
        self.detach_database(database, false)
    }

    /// `take_database`. With `uncharged`, a database that shares the shard with others
    /// skips the per-entry charge walk and carries no charge; the caller then frees it and
    /// never restores it. The last database of a shard is charged the shard totals.
    fn detach_database(&mut self, database: u8, uncharged: bool) -> DetachedDatabase<I> {
        let previous = self.active;

        self.enter(database);
        let mut charge = EntryCharge::default();

        if self.parked_entries == 0 {
            // Every entry of the shard is in this map, so its charge is the running total.
            charge = EntryCharge {
                key_bytes: self.memory_usage.key_bytes,
                value_bytes: self.memory_usage.value_bytes,
                collection_bytes: self.memory_usage.collection_bytes,
            };
        } else if !uncharged {
            for (key, hit) in self.entries.entries() {
                let entry_charge = charge_of(key, hit);

                charge.key_bytes += entry_charge.key_bytes;
                charge.value_bytes += entry_charge.value_bytes;
                charge.collection_bytes += entry_charge.collection_bytes;
            }
        }

        let deadline_index = Box::new(self.take_deadline_db(database));
        let deadlines = deadline_index.len as u64;

        let map = std::mem::replace(&mut self.entries, I::with_capacity(0));

        self.memory_usage.key_bytes = self.memory_usage.key_bytes.saturating_sub(charge.key_bytes);
        self.memory_usage.value_bytes = self
            .memory_usage
            .value_bytes
            .saturating_sub(charge.value_bytes);
        self.memory_usage.collection_bytes = self
            .memory_usage
            .collection_bytes
            .saturating_sub(charge.collection_bytes);
        self.expiration.keys_with_ttl = self.expiration.keys_with_ttl.saturating_sub(deadlines);
        self.note_database_rewrite(&[database]);
        self.enter(previous);
        self.refresh_global_memory_usage();
        self.detached_databases += 1;

        DetachedDatabase {
            database,
            map,
            charge,
            deadlines,
            deadline_index,
        }
    }

    /// Puts a detached database back in place of whatever that slot holds now.
    pub(crate) fn restore_database(&mut self, detached: DetachedDatabase<I>) {
        let stale = self.take_database(detached.database);

        self.release_database(stale);
        self.detached_databases = self.detached_databases.saturating_sub(1);
        let previous = self.active;

        self.enter(detached.database);
        self.entries = detached.map;
        self.install_deadline_db(detached.database, *detached.deadline_index);
        self.memory_usage.key_bytes += detached.charge.key_bytes;
        self.memory_usage.value_bytes += detached.charge.value_bytes;
        self.memory_usage.collection_bytes += detached.charge.collection_bytes;
        self.expiration.keys_with_ttl += detached.deadlines;
        self.enter(previous);
        self.refresh_global_memory_usage();
    }

    /// Frees a detached database. Byte buffers go back to the pool.
    pub(crate) fn release_database(&mut self, detached: DetachedDatabase<I>) {
        self.release_databases(vec![detached]);
    }

    /// Frees detached databases together. When nothing else in the shard holds a value
    /// log span, the index and the log are handed to the lazy-free thread whole and the
    /// shard keeps an empty log, so the shard thread frees nothing per entry.
    fn release_databases(&mut self, detached: Vec<DetachedDatabase<I>>) {
        let count = detached.len();
        let everything_gone =
            self.detached_databases == count && self.entries.len() + self.parked_entries == 0;

        if everything_gone {
            let mut bytes = 0usize;
            let mut parts = Vec::with_capacity(count);

            for DetachedDatabase {
                map,
                charge,
                deadline_index,
                ..
            } in detached
            {
                bytes += charge.key_bytes + charge.value_bytes + charge.collection_bytes;
                bytes += deadline_index.charged_bytes();
                parts.push((map, deadline_index));
            }

            // Nothing references the log or the pool any more; drop their pages so an
            // emptied shard returns to its idle footprint. One job carries everything, so
            // a database detached without a charge still leaves the shard thread.
            let log = std::mem::take(&mut self.value_log);
            let pool = std::mem::take(&mut self.byte_pool);

            crate::lazy_free::defer_flush((parts, log, pool), bytes);
            self.detached_databases = 0;
            self.collect_after_lazy_free = true;
            self.refresh_global_memory_usage();
            crate::memory::release_free_pages();

            return;
        }

        for DetachedDatabase { map, .. } in detached {
            for entry in map.into_values() {
                self.release_entry(entry);
            }

            self.detached_databases = self.detached_databases.saturating_sub(1);
        }

        self.refresh_global_memory_usage();
    }

    /// Called on the maintenance tick: once the lazy-free thread has dropped what a flush
    /// handed it, gives the freed pages back to the OS.
    pub fn collect_freed_pages(&mut self) {
        if self.collect_after_lazy_free && crate::lazy_free::pending_bytes() == 0 {
            self.collect_after_lazy_free = false;
            crate::memory::release_free_pages();
        }
    }

    /// Empties one database.
    pub(crate) fn flush_database(&mut self, database: u8) {
        self.json_index_database_flushed(database);
        self.fill_database_flushed(database);

        if database == 0 {
            // The definitions live in database 0.
            self.json_indexes = None;
        }

        let detached = self.take_database(database);

        self.release_database(detached);

        if let Some(sweep) = self.field_sweep.as_mut() {
            sweep.retain(|(_, filed, _)| *filed != database);

            if sweep.is_empty() {
                self.field_sweep = None;
            }
        }
    }

    /// Lifts out what `flush_database` drops beside the entries (JSON index postings or
    /// registry, fill markers, field TTL sweep), for a flush inside `MULTI` that may be
    /// refused. Database 0 takes the whole JSON registry, so a `FLUSHALL` detaches it first.
    pub(crate) fn detach_side_state(&mut self, database: u8) -> DetachedSide {
        let marks = self.fill_detach_database(database);
        let sweep = match self.field_sweep.as_mut() {
            Some(sweep) => {
                let taken: Vec<_> = sweep
                    .extract_if(.., |(_, filed, _)| *filed == database)
                    .collect();

                if sweep.is_empty() {
                    self.field_sweep = None;
                }

                taken
            }
            None => Vec::new(),
        };
        let json = if database == 0 {
            DetachedJson::Registry(self.json_indexes.take())
        } else {
            DetachedJson::Postings(
                self.json_indexes
                    .as_mut()
                    .map(|indexes| indexes.detach_database(database)),
            )
        };

        DetachedSide {
            database,
            marks,
            sweep,
            json,
        }
    }

    /// Puts back what `detach_side_state` lifted, in place of what the database has now.
    pub(crate) fn restore_side_state(&mut self, side: DetachedSide) {
        let database = side.database;

        self.fill_restore_database(database, side.marks);
        if let Some(sweep) = self.field_sweep.as_mut() {
            sweep.retain(|(_, filed, _)| *filed != database);
        }
        if !side.sweep.is_empty() {
            self.field_sweep
                .get_or_insert_with(Box::default)
                .extend(side.sweep);
        }
        if self
            .field_sweep
            .as_ref()
            .is_some_and(|sweep| sweep.is_empty())
        {
            self.field_sweep = None;
        }
        match side.json {
            DetachedJson::Registry(registry) => self.json_indexes = registry,
            DetachedJson::Postings(Some(postings)) => {
                if let Some(indexes) = self.json_indexes.as_mut() {
                    indexes.restore_database(database, postings);
                }
            }
            DetachedJson::Postings(None) => {}
        }
    }

    /// Empties every database.
    pub(crate) fn flush_all(&mut self) {
        self.json_index_all_flushed();
        self.fill_all_flushed();
        let detached: Vec<_> = self
            .database_ids()
            .into_iter()
            .map(|database| self.detach_database(database, true))
            .collect();

        self.release_databases(detached);

        if let Some(sweep) = self.field_sweep.take() {
            drop(sweep);
        }
    }

    /// Removes every key, in every database, whose hash slot is set in `slots` (indexed by
    /// slot), without logging it. Recovery uses it for keys of slots another shard owns.
    pub(crate) fn drop_slots(&mut self, slots: &[bool]) -> usize {
        let active = self.active;
        let mut dropped = 0;

        for database in self.database_ids() {
            self.enter(database);
            let keys: Vec<Box<[u8]>> = self
                .entries
                .entries()
                .filter(|(key, _)| slots[usize::from(crate::routing::hash_slot(key))])
                .map(|(key, _)| key.into())
                .collect();

            // An expired key is removed as expired; either way it is gone.
            for key in &keys {
                self.delete_internal(key, true, crate::notify::KeyEvent::Del);
            }
            dropped += keys.len();
        }

        self.enter(active);
        dropped
    }

    /// How many keys, in every database, live in a slot marked in `slots`, and the lowest
    /// such slot. Reads only; walks every key once.
    pub(crate) fn count_in_slots(&mut self, slots: &[bool]) -> (usize, Option<u16>) {
        let active = self.active;
        let mut count = 0;
        let mut lowest: Option<u16> = None;

        for database in self.database_ids() {
            self.enter(database);
            for (key, _) in self.entries.entries() {
                let slot = crate::routing::hash_slot(key);

                if slots[usize::from(slot)] {
                    count += 1;
                    lowest = Some(lowest.map_or(slot, |known| known.min(slot)));
                }
            }
        }

        self.enter(active);
        (count, lowest)
    }

    /// Moves `key` from the active database to `target`. False when the key is missing
    /// or `target` already has it.
    pub(crate) fn move_key(&mut self, key: &[u8], target: u8) -> bool {
        let source = self.active;

        if !self.can_move_key(key, target) {
            return false;
        }

        let before = self.entry_charge(key);
        let deadline = self.deadline(source, key);
        let access = self.entries.access_get(key);
        let timed_hashes = self.field_sweep.is_some();
        let entry = self
            .remove_entry(key)
            .expect("checked key disappeared from single-owner shard");

        self.reconcile_entry_charge(before, key);
        self.enter(target);
        let before = self.entry_charge(key);

        self.insert_entry(key, entry);

        if let Some(deadline) = deadline {
            self.set_deadline(target, key, deadline);
        }

        if access != 0
            && let Some(cell) = self.entries.access_mut(key)
        {
            *cell = access;
        }

        if timed_hashes {
            self.refile_arrived_hash(key);
        }

        self.reconcile_entry_charge(before, key);
        self.enter(source);
        true
    }

    /// Remembers `key` of the active database: a write to it just started a resize or a
    /// compaction, or a collection with such work under way arrived there, and
    /// maintenance must be able to finish it.
    #[cold]
    fn note_collection_work(&mut self, key: &[u8]) {
        arena_table::clear_background_work_started();
        self.collection_work.insert((self.active, Box::from(key)));
    }

    /// Moves unfinished resize and compaction work of tracked large sets and hashes
    /// for up to `max_time`. Returns true when none is left.
    pub fn step_collection_work(&mut self, max_time: std::time::Duration) -> bool {
        const CHUNK: usize = 256;

        if self.collection_work.is_empty() {
            return true;
        }

        let deadline = std::time::Instant::now() + max_time;

        while let Some(tracked) = self.collection_work.iter().next().cloned() {
            let (db, ref key) = tracked;
            let map = if db == self.active {
                Some(&mut self.entries)
            } else {
                self.databases
                    .get_mut(usize::from(db))
                    .and_then(Option::as_mut)
            };
            // Look first: `object_mut` would promote a small slab record, and only an
            // object holds an arena table.
            let map = map.filter(|map| {
                matches!(map.lookup(key), Some(IndexRef::Object(entry))
                    if arena_table::has_background_work(&entry.value))
            });
            let Some(entry) = map.and_then(|map| map.object_mut(key)) else {
                self.collection_work.remove(&tracked);
                continue;
            };
            let before = EntryCharge::for_entry(key, entry);
            let finished = loop {
                if arena_table::background_step(&mut entry.value, CHUNK)
                    || std::time::Instant::now() >= deadline
                {
                    break !arena_table::has_background_work(&entry.value);
                }
            };
            let after = EntryCharge::for_entry(key, entry);

            // Moving members changes no content, so watches and snapshots are not told;
            // only the charge follows the released memory.
            self.memory_usage.collection_bytes = self
                .memory_usage
                .collection_bytes
                .saturating_sub(before.collection_bytes)
                .saturating_add(after.collection_bytes);
            self.refresh_global_memory_usage();

            if !finished {
                return false;
            }

            self.collection_work.remove(&tracked);
        }

        true
    }

    /// Spends up to `max_time` finishing unfinished index and deadline-table resizes in
    /// every database, and starts the shrinks delete bursts left owed. Returns true
    /// when no index resize is left, which log reclaim and snapshots wait for.
    pub fn migrate_indexes(&mut self, max_time: std::time::Duration) -> bool {
        const CHUNK: usize = 256;

        let started = std::time::Instant::now();
        let out_of_time = || started.elapsed() >= max_time;
        let mut done = true;

        while !self.entries.migrate_step(CHUNK) {
            if out_of_time() {
                done = false;
                break;
            }
        }

        // Each map's owed shrink waits only for that map's own resize.
        if done && self.entries.shrink_if_sparse() {
            done = false;
        }

        // A parked map's charge is cached until it is entered, so every change to it
        // is carried into that cache.
        for slot in 0..self.databases.len() {
            let Some(map) = self.databases[slot].as_mut() else {
                continue;
            };
            let before = map.charged_bytes();
            let mut finished = true;

            while !map.migrate_step(CHUNK) {
                if out_of_time() {
                    finished = false;
                    break;
                }
            }

            if finished && map.shrink_if_sparse() {
                finished = false;
            }

            let after = map.charged_bytes();

            self.parked_index_bytes = self
                .parked_index_bytes
                .saturating_sub(before)
                .saturating_add(after);
            done &= finished;
        }

        for db in 0..self.deadlines.len() {
            let deadlines = &mut self.deadlines[db];
            let before = deadlines.charged_bytes();
            let mut finished = true;

            while !deadlines.migrate_step(CHUNK) {
                if out_of_time() {
                    finished = false;
                    break;
                }
            }

            if finished && deadlines.shrink_if_sparse() {
                finished = false;
            }

            self.update_deadline_index_bytes(db, before);
            done &= finished;
        }

        self.refresh_global_memory_usage();
        done
    }

    /// Spends up to `max_time` draining sparse log pages: flags the
    /// sparsest pages of the value log and of each key log, then walks every database
    /// and moves the spans still on them. A page is released when its last live byte
    /// leaves, so a walk cut short only delays the release. Returns true when no
    /// round is in progress.
    pub fn reclaim_logs(&mut self, max_time: std::time::Duration) -> bool {
        const CHUNK: usize = 256;

        if self.reclaim.is_none() && !self.start_reclaim() {
            return true;
        }

        let started = std::time::Instant::now();

        loop {
            let walk = self.reclaim.as_mut().expect("reclaim round");

            let Some(&db) = walk.order.get(walk.position) else {
                self.finish_reclaim();
                return true;
            };

            let KvShard {
                entries,
                databases,
                value_log,
                deadlines,
                active,
                snapshot,
                detached_databases,
                parked_index_bytes,
                ..
            } = self;
            let parked = db != *active;
            let map = if db == *active {
                Some(entries)
            } else {
                databases.get_mut(usize::from(db)).and_then(Option::as_mut)
            };

            let Some(map) = map else {
                walk.position += 1;
                walk.cursor = 0;
                continue;
            };

            if map.is_growing() {
                // Slots move during growth; `migrate_indexes` finishes it first.
                return false;
            }

            // Detached databases still own value spans the walk cannot see, and a
            // snapshot remembers walked keys by key-log address.
            let values = *detached_databases == 0;
            let keys = snapshot.is_none();
            let mut deadlines = deadlines.get_mut(usize::from(db));
            // A moved record may take a new recency-cell page: a parked map's charge is a
            // running total, adjusted here.
            let charged = if parked { map.charged_bytes() } else { 0 };
            let next = map.relocate_step(
                walk.cursor,
                CHUNK,
                keys,
                &mut |offset, len| {
                    if values {
                        value_log.relocate_value(offset, len)
                    } else {
                        None
                    }
                },
                &mut |key, old, new| {
                    if let Some(deadlines) = deadlines.as_mut() {
                        deadlines.move_address(key, old, new);
                    }
                },
            );

            if parked {
                *parked_index_bytes = parked_index_bytes
                    .saturating_sub(charged)
                    .saturating_add(map.charged_bytes());
            }

            if next == 0 {
                walk.position += 1;
            }

            walk.cursor = next;

            if started.elapsed() >= max_time {
                return false;
            }
        }
    }

    fn start_reclaim(&mut self) -> bool {
        let mut flagged = 0;

        if self.detached_databases == 0 {
            flagged += self.value_log.plan_drain();
        }

        if self.snapshot.is_none() {
            flagged += self.entries.plan_key_drain();

            for map in self.databases.iter_mut().flatten() {
                flagged += map.plan_key_drain();
            }
        }

        if flagged == 0 {
            return false;
        }

        self.reclaim = Some(Box::new(ReclaimWalk {
            order: self.database_ids(),
            position: 0,
            cursor: 0,
        }));
        true
    }

    fn finish_reclaim(&mut self) {
        self.reclaim = None;
        self.value_log.end_drain();
        self.entries.end_key_drain();

        for map in self.databases.iter_mut().flatten() {
            map.end_key_drain();
        }

        crate::memory::release_free_pages();
    }

    /// Slab pages and records per size class, summed over every database.
    pub fn slab_stats(&self) -> Vec<crate::index::SlabClassStats> {
        let mut total: Vec<crate::index::SlabClassStats> = Vec::new();

        for (_, map) in self.database_maps() {
            for (index, class) in map.slab_stats().into_iter().enumerate() {
                match total.get_mut(index) {
                    Some(sum) => {
                        sum.pages += class.pages;
                        sum.live += class.live;
                    }
                    None => total.push(class),
                }
            }
        }

        total
    }

    /// Page counters of the value log and every key log.
    pub fn log_stats(&self) -> LogStats {
        let mut stats = self.value_log.stats();

        for (_, map) in self.database_maps() {
            stats.add(map.log_stats());
        }

        stats
    }

    pub fn memory_usage(&self) -> MemoryUsage {
        self.memory_usage
    }

    pub fn set_memory_limit(&mut self, limit: Option<usize>) {
        self.memory_limit = limit;
    }

    pub fn set_eviction_policy(&mut self, policy: EvictionPolicy) {
        if policy != self.eviction_policy {
            // LRU and LFU read the same bits differently, and pooled scores belong to one policy.
            self.entries.reset_access();

            // Parked maps are charged by a running total: their cells leave it here.
            for map in self.databases.iter_mut().flatten() {
                let before = map.charged_bytes();

                map.reset_access();
                self.parked_index_bytes = self
                    .parked_index_bytes
                    .saturating_sub(before)
                    .saturating_add(map.charged_bytes());
            }

            self.refresh_global_memory_usage();

            *self.eviction_pool.get_mut() = None;
        }

        self.eviction_policy = policy;
    }

    pub fn apply_memory_limits(&mut self, limits: MemoryLimits) {
        self.set_memory_limit(limits.shard_admission_budget());
        self.set_eviction_policy(limits.eviction_policy);
        self.eviction_sample_size = limits.maxmemory_samples;
        self.active_expire_keys = limits.active_expire_keys;
        self.hash_max_compact_entries = limits.hash_max_compact_entries;
        self.hash_max_compact_value_bytes = limits.hash_max_compact_value_bytes;
        self.collection_limits = CollectionLimits {
            set_max_intset_entries: limits.set_max_intset_entries,
            list_max_entries: limits.list_max_compact_entries,
            list_max_item_bytes: limits.list_max_compact_value_bytes,
        };
        self.set_notify_mask(limits.notify_mask);
    }

    pub(crate) fn set_notify_mask(&mut self, mask: u32) {
        self.notify.set_mask(mask);
    }

    pub(crate) fn set_notify_listeners(&mut self, listeners: u32) {
        self.notify.set_listeners(listeners);
    }

    /// Whether expiry and eviction deletes are kept for client-side tracking.
    pub(crate) fn set_tracking_active(&mut self, tracking: bool) {
        self.notify.set_tracking(tracking);
    }

    #[cfg(test)]
    pub(crate) fn notify_mask(&self) -> u32 {
        self.notify.mask()
    }

    #[cfg(test)]
    pub(crate) fn take_notices(&mut self) -> Vec<(u8, Box<[u8]>, crate::notify::KeyEvent)> {
        self.notify.take()
    }

    /// Nothing for `drain_notices` to hand out.
    #[inline]
    pub(crate) fn notices_idle(&self) -> bool {
        self.notify.is_idle()
    }

    /// One borrow for the connection loop: the notices to publish and the keys expiry or
    /// eviction removed for tracking. An idle notify state allocates nothing.
    #[allow(clippy::type_complexity)]
    pub(crate) fn drain_notices(
        &mut self,
    ) -> (
        u32,
        Vec<(u8, Box<[u8]>, crate::notify::KeyEvent)>,
        Vec<(u8, Box<[u8]>)>,
    ) {
        if self.notify.is_idle() {
            return (0, Vec::new(), Vec::new());
        }

        (
            self.notify.mask(),
            self.notify.take(),
            self.notify.take_dropped(),
        )
    }

    fn note_key(&mut self, key: &[u8], event: crate::notify::KeyEvent) {
        self.notify.record(self.active, key, event);
    }

    fn compact_limits(&self) -> CompactLimits {
        CompactLimits {
            max_entries: self.hash_max_compact_entries,
            max_value_bytes: self.hash_max_compact_value_bytes,
        }
    }

    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    pub fn memory_stats(&self) -> MemoryStats {
        let (eviction_scans, eviction_slots_inspected) = self.eviction_scan.get();

        MemoryStats {
            eviction_scans,
            eviction_slots_inspected,
            ..self.memory_stats
        }
    }

    pub fn touch(&mut self, key: &[u8]) {
        // `record_access` finds the key's cell in one lookup and skips a missing key.
        if !self.eviction_policy.tracks_recency() || self.key_is_expired(key) {
            return;
        }

        self.record_access(key);
    }

    /// Stamps the read that [`Self::value_view_read`] described, once its view is dropped.
    pub(crate) fn mark_read(&mut self, key: &[u8], mark: ReadMark) {
        match mark {
            ReadMark::Missing => {}
            ReadMark::At(address) => self.record_access_at(address),
            ReadMark::Key => self.touch(key),
        }
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    fn live(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        self.live_hit(key)
    }

    /// The collection at `key` as a read view. `object` maps the value of an object entry,
    /// `slab` maps any other index record (the slab kind of this collection, else `None`);
    /// a record that neither accepts is a different type.
    pub(super) fn live_view<'a, T>(
        &'a self,
        key: &[u8],
        object: impl FnOnce(&'a Value) -> Option<T>,
        slab: impl FnOnce(IndexRef<'a>) -> Option<T>,
    ) -> Result<Option<T>, WrongType> {
        match self.live_hit(key) {
            None => Ok(None),
            Some(IndexRef::Object(entry)) => object(&entry.value).map(Some).ok_or(WrongType),
            Some(record) => slab(record).map(Some).ok_or(WrongType),
        }
    }

    fn live_object(&self, key: &[u8]) -> Result<Option<&Entry>, WrongType> {
        match self.live_hit(key) {
            None => Ok(None),
            Some(IndexRef::Object(entry)) => Ok(Some(entry)),
            Some(_) => Err(WrongType),
        }
    }

    /// `object_value` for a caller that just ran `remove_expired`: on a replica that
    /// keeps expired keys, the key is still stored and must read as missing.
    fn unexpired_object_value(&self, key: &[u8]) -> Result<Option<&Value>, WrongType> {
        if self.removals.keeps_expired() && self.key_is_expired(key) {
            return Ok(None);
        }

        self.object_value(key)
    }

    fn object_value(&self, key: &[u8]) -> Result<Option<&Value>, WrongType> {
        match self.entries.lookup(key) {
            None => Ok(None),
            Some(IndexRef::Object(entry)) => Ok(Some(&entry.value)),
            Some(_) => Err(WrongType),
        }
    }

    /// `lookup` that hides an expired key, using the index's TTL bit to skip the deadline table.
    /// Also hides a hash whose every field is past its deadline.
    fn live_hit(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        let hit = self.live_unchecked(key)?;

        if let IndexRef::Object(entry) = &hit
            && matches!(entry.value, Value::TimedHash(_))
            && self.timed_hash_is_dead(&entry.value)
        {
            return None;
        }

        Some(hit)
    }

    /// [`Self::live_hit`] without the check for a hash whose fields are all due: for a
    /// caller that treats a hash hit as the wrong type and checks it there.
    #[inline]
    pub(super) fn live_unchecked(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        let (hit, may_expire) = self.entries.lookup_ttl(key)?;

        if may_expire && self.deadline_passed(self.active, key) {
            None
        } else {
            Some(hit)
        }
    }

    /// Past its key deadline, or a hash whose fields are all past theirs. The second check
    /// runs only once some hash in this shard has a field TTL.
    fn key_is_expired(&self, key: &[u8]) -> bool {
        let timed_hashes = self.field_sweep.is_some();

        if self.expiration.keys_with_ttl == 0 && !timed_hashes {
            return false;
        }

        let Some((hit, may_expire)) = self.entries.lookup_ttl(key) else {
            return false;
        };

        (may_expire && self.expiration.keys_with_ttl != 0 && self.deadline_passed(self.active, key))
            || (timed_hashes && self.hit_is_dead_hash(hit))
    }

    /// A stored hash whose every field is past its deadline.
    #[cold]
    fn hit_is_dead_hash(&self, hit: IndexRef<'_>) -> bool {
        matches!(hit, IndexRef::Object(entry) if self.timed_hash_is_dead(&entry.value))
    }

    #[cold]
    pub(super) fn timed_hash_is_dead(&self, value: &Value) -> bool {
        value.hash_fields_all_due(self.now_unix_millis().get())
    }

    fn forget_deadline(&mut self, had_deadline: bool) {
        if had_deadline {
            self.expiration.keys_with_ttl = self.expiration.keys_with_ttl.saturating_sub(1);
        }
    }

    fn remember_deadline(&mut self, has_deadline: bool) {
        if has_deadline {
            self.expiration.keys_with_ttl = self.expiration.keys_with_ttl.saturating_add(1);
        }
    }

    fn remove_entry(&mut self, key: &[u8]) -> Option<Entry> {
        // The deadline record finds its key through the key-log address, and the
        // remove frees that record (its page may go back to the allocator), so the
        // deadline goes first. A clear TTL hint means there is none.
        if self.expiration.keys_with_ttl != 0
            && matches!(self.entries.lookup_ttl(key), Some((_, true)))
        {
            self.clear_deadline(self.active, key);
        }

        let entry = self.entries.remove(key)?;

        if self.json_indexes.is_some() {
            self.json_index_refresh(key);
        }

        if self.field_sweep.is_some()
            && let Some(at) = entry.value.soonest_field_deadline()
        {
            self.refile_hash(key, Some(at), None);
        }

        Some(entry)
    }

    fn insert_entry(&mut self, key: impl AsRef<[u8]>, entry: Entry) -> Option<Entry> {
        let key = key.as_ref();

        // A collection arriving with a resize or compaction under way (RENAME, MOVE,
        // a restored or cloned value) is tracked under its new name.
        if arena_table::has_background_work(&entry.value) {
            self.note_collection_work(key);
        }

        if self.entries.lookup(key).is_some() {
            self.clear_deadline(self.active, key);

            let replaced = self.entries.insert(key, entry);

            if self.json_indexes.is_some() {
                self.json_index_refresh(key);
            }

            return match replaced {
                Some(Entry {
                    value: Value::Bytes(_),
                }) => replaced,
                Some(old) => {
                    // A replaced collection is freed off the shard thread when it is large.
                    let charge = EntryCharge::for_entry(key, &old);

                    self.discard_value(old.value, charge);
                    None
                }
                None => None,
            };
        }

        let replaced = self.entries.insert(key, entry);

        if self.json_indexes.is_some() {
            self.json_index_refresh(key);
        }

        replaced
    }

    /// Removes a collection whose last element just went. The caller took `entry_charge`
    /// before the edit and reconciles after it, as for any edit; this is the one place such
    /// a key leaves and is announced deleted.
    pub(super) fn remove_emptied(&mut self, key: &[u8]) {
        if self.remove_entry(key).is_some() {
            self.note_key(key, crate::notify::KeyEvent::Del);
        }
    }

    /// Frees a replaced or removed entry: strings back to the log or pool, a large
    /// collection or document off the shard thread.
    pub(super) fn free_entry(&mut self, key: &[u8], entry: Entry) {
        let charge = EntryCharge::for_entry(key, &entry);

        self.discard_value(entry.value, charge);
    }

    /// Frees a value taken out of the index. Byte buffers go back to the log or the pool;
    /// any other value charged at least `lazy_free::MIN_DEFERRED_BYTES` is dropped on the
    /// lazy-free thread.
    fn discard_value(&mut self, value: Value, charge: EntryCharge) {
        match value.into_string_body() {
            Ok(body) => body.release(&mut self.value_log, &mut self.byte_pool),
            Err(value) => crate::lazy_free::defer_drop(
                value,
                charge.value_bytes.saturating_add(charge.collection_bytes),
            ),
        }
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub fn rename(&mut self, source: &[u8], destination: &[u8]) -> bool {
        self.remove_expired(source);
        self.remove_expired(destination);

        if source == destination {
            return self.entries.lookup(source).is_some();
        }

        let source_before = self.entry_charge(source);
        let destination_before = self.entry_charge(destination);
        let deadline = self.deadline(self.active, source);
        let access = self.entries.access_get(source);
        let timed_hashes = self.field_sweep.is_some();
        let Some(entry) = self.remove_entry(source) else {
            return false;
        };

        if let Some(replaced) = self.remove_entry(destination) {
            self.discard_value(replaced.value, destination_before);
        }

        self.insert_entry(destination, entry);

        if let Some(deadline) = deadline {
            self.set_deadline(self.active, destination, deadline);
        }

        if access != 0
            && let Some(cell) = self.entries.access_mut(destination)
        {
            *cell = access;
        }

        if timed_hashes {
            self.refile_arrived_hash(destination);
        }

        self.reconcile_entry_charge(source_before, source);
        self.reconcile_entry_charge(destination_before, destination);
        true
    }

    /// Keys, keys with a TTL, and sampled mean remaining TTL in milliseconds per non-empty
    /// database. Constant work per database: keys that expired but are not reaped yet are
    /// counted, as in Redis.
    pub(crate) fn keyspace(&self) -> Vec<(u8, usize, usize, u64)> {
        let now = self.clock.now_unix_millis().get();
        let mut rows: Vec<(u8, usize, usize, u64)> = self
            .database_maps()
            .filter(|(_, map)| map.len() > 0)
            .map(|(db, map)| {
                let (expires, avg_ttl) = self.deadlines.get(usize::from(db)).map_or((0, 0), |d| {
                    (d.len, d.sampled_mean_ttl(now, KEYSPACE_TTL_SAMPLES))
                });

                (db, map.len(), expires, avg_ttl)
            })
            .collect();

        rows.sort_unstable_by_key(|row| row.0);
        rows
    }

    pub(crate) fn scan(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
        kind: Option<ValueKind>,
    ) -> (u64, Vec<&[u8]>) {
        // COUNT bounds the work, not the reply: slots that are empty, expired or
        // filtered out still count, so one call is O(COUNT) whatever the keyspace size.
        let count = count.max(1);
        let mut keys = Vec::new();
        let next = self.entries.scan_slots(
            cursor,
            count,
            count.saturating_mul(SCAN_SLOT_FACTOR),
            &mut |key, entry, _| {
                if self.key_is_expired(key)
                    || kind.is_some_and(|kind| hit_kind(entry).reported() != kind.reported())
                    || pattern.is_some_and(|pattern| !crate::protocol::glob_match(pattern, key))
                {
                    return;
                }

                keys.push(key);
            },
        );

        (next, keys)
    }

    /// Remove a key and return its owned value.
    pub fn remove(&mut self, key: &[u8]) -> Option<Box<[u8]>> {
        if self.remove_expired(key) {
            return None;
        }

        let before = self.entry_charge(key);
        let removed = match self.remove_entry(key) {
            Some(Entry {
                value: Value::Bytes(value),
            }) => Some(value.take_boxed(&mut self.value_log)),
            Some(entry) => {
                self.free_entry(key, entry);
                None
            }
            None => return None,
        };

        self.note_key(key, crate::notify::KeyEvent::Del);

        self.reconcile_entry_charge(before, key);
        removed
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.delete_internal(key, true, crate::notify::KeyEvent::Del)
    }

    // A new value type must say here whether it is freed off the shard thread: no wildcard arm.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn delete_internal(
        &mut self,
        key: &[u8],
        retain_in_pool: bool,
        event: crate::notify::KeyEvent,
    ) -> bool {
        if self.remove_expired(key) {
            return false;
        }

        let before = self.entry_charge(key);
        let Some(entry) = self.remove_entry(key) else {
            return false;
        };

        match entry.value {
            Value::Bytes(value) => {
                if retain_in_pool {
                    value.release(&mut self.value_log, &mut self.byte_pool);
                } else {
                    // An evicted heap buffer goes back to the allocator, not the pool;
                    // a log span is freed either way.
                    value.discard(&mut self.value_log);
                }
            }
            // Large collections and documents are freed off the shard thread.
            value @ (Value::Json(_)
            | Value::JsonTree(_)
            | Value::List(_)
            | Value::Set(_)
            | Value::Hash(_)
            | Value::SortedSet(_)
            | Value::BloomFilter(_)
            | Value::Stream(_)
            | Value::Lease(_)
            | Value::Versioned(_)
            | Value::Limit(_)
            | Value::Once
            | Value::TimedHash(_)
            | Value::Semaphore(_)) => self.discard_value(value, before),
        }

        self.reconcile_entry_charge(before, key);
        self.note_key(key, event);
        true
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Physical entries in the active database, including keys whose deadline
    /// has passed but which active expiry has not reaped yet. This matches
    /// Redis `DBSIZE` and stays O(1).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether the active database has no physical entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn pooled_value_bytes(&self) -> usize {
        self.byte_pool.cached_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortedSetError {
    WrongType,
    InvalidScore,
    InvalidIncrement,
    /// The sorted set cannot hold more members.
    Full,
}

impl From<InsertError> for SortedSetError {
    fn from(error: InsertError) -> Self {
        match error {
            InsertError::InvalidScore => Self::InvalidScore,
            InsertError::Full => Self::Full,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomError {
    WrongType,
    KeyExists,
    InvalidConfiguration,
}

fn stream_group_error(error: crate::stream::GroupError) -> StreamGroupError {
    match error {
        crate::stream::GroupError::Busy => StreamGroupError::Busy,
        crate::stream::GroupError::NoGroup => StreamGroupError::NoGroup,
    }
}

fn parse_counter(bytes: &[u8]) -> Result<i64, IncrementError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(IncrementError::NotIntegerOrOverflow)
}

/// What the value found under `key` is charged, without a further lookup.
#[inline]
fn charge_of(key: &[u8], hit: IndexRef<'_>) -> EntryCharge {
    match hit {
        IndexRef::Object(entry) => EntryCharge::for_entry(key, entry),
        IndexRef::Log { len, .. } => EntryCharge::logged(key, usize::from(len)),
        IndexRef::Integer(_) => EntryCharge::integer(key),
        IndexRef::Heap(bytes) => EntryCharge::heap(key, bytes.capacity()),
        IndexRef::SetSlab(bytes) => EntryCharge::slab(key, set_used_len(bytes)),
        IndexRef::SortedSlab(bytes) => EntryCharge::slab(key, sorted_used_len(bytes)),
        IndexRef::CollSlab(bytes) => EntryCharge::slab(key, 3 + usize::from(bytes[2])),
    }
}

fn bytes_body_of(hit: IndexRef<'_>) -> Option<ByteBody> {
    match hit {
        IndexRef::Log { offset, len } => Some(ByteBody::Log { offset, len }),
        IndexRef::Heap(_) => Some(ByteBody::Heap(Box::default())),

        IndexRef::Integer(value) => Some(ByteBody::Integer(value)),
        IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_) => None,
        IndexRef::Object(entry) => match &entry.value {
            Value::Bytes(value) => Some(value.body.clone()),
            _ => None,
        },
    }
}

fn hit_kind(hit: IndexRef<'_>) -> ValueKind {
    match hit {
        IndexRef::Log { .. } | IndexRef::Integer(_) | IndexRef::Heap(_) => ValueKind::Bytes,

        IndexRef::SetSlab(_) => ValueKind::Set,
        IndexRef::SortedSlab(_) => ValueKind::SortedSet,
        IndexRef::CollSlab(bytes) if bytes[0] == crate::index::COLL_LIST => ValueKind::List,
        IndexRef::CollSlab(_) => ValueKind::Hash,
        IndexRef::Object(entry) => entry_kind(&entry.value),
    }
}

fn entry_kind(value: &Value) -> ValueKind {
    match value {
        Value::Bytes(_) => ValueKind::Bytes,
        Value::Json(_) | Value::JsonTree(_) => ValueKind::Json,
        Value::List(_) => ValueKind::List,
        Value::Set(_) => ValueKind::Set,
        Value::Hash(_) | Value::TimedHash(_) => ValueKind::Hash,
        Value::SortedSet(_) => ValueKind::SortedSet,
        Value::BloomFilter(_) => ValueKind::BloomFilter,
        Value::Stream(_) => ValueKind::Stream,
        Value::Lease(_) => ValueKind::Lease,
        Value::Limit(_) => ValueKind::Limit,
        Value::Semaphore(_) => ValueKind::Semaphore,
        Value::Once => ValueKind::Once,
        Value::Versioned(_) => ValueKind::Versioned,
    }
}

pub(crate) fn integer_bytes(value: i64) -> Vec<u8> {
    let (start, digits) = integer_digits(value);

    digits[start..].to_vec()
}

fn integer_digits(value: i64) -> (usize, [u8; 20]) {
    let mut digits = [0u8; 20];
    let negative = value < 0;
    let mut magnitude = value.unsigned_abs();
    let mut position = digits.len();

    loop {
        position -= 1;
        digits[position] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;

        if magnitude == 0 {
            break;
        }
    }

    if negative {
        position -= 1;
        digits[position] = b'-';
    }

    (position, digits)
}
