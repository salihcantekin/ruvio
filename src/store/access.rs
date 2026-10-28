//! Access metadata for LRU/LFU eviction: a wrap-aware 24-bit seconds clock, Redis-style
//! logarithmic frequency counters with minute decay, and a fixed eviction pool.

use super::*;

/// Seconds resolution, 24 bits: wraps every ~194 days, so age is always a masked difference.
const LRU_MASK: u32 = 0x00ff_ffff;
const LFU_MINUTE_MASK: u32 = 0xffff;
const LFU_INIT: u8 = 5;
const LFU_LOG_FACTOR: u32 = 10;
const LFU_DECAY_MINUTES: u32 = 1;
const POOL_SIZE: usize = 16;

pub(super) const ACCESS_RNG_SEED: u32 = 0x9e37_79b9;

/// Low 24 bits: LRU seconds stamp, or the LFU minute stamp. High 8 bits: LFU counter;
/// LRU stores 1 there so a recorded value is never 0 (0 means "never read").
/// `set_eviction_policy` resets the index metadata on any policy change, so one layout
/// is live. The value lives beside the key record in the index (`access_mut`).
#[derive(Clone, Copy, Debug)]
pub(super) struct AccessMeta(u32);

impl AccessMeta {
    fn from_raw(raw: u32) -> Option<Self> {
        (raw != 0).then_some(Self(raw))
    }

    fn new(stamp: u32, counter: u8) -> Self {
        Self((stamp & LRU_MASK) | (u32::from(counter) << 24))
    }

    fn stamp(self) -> u32 {
        self.0 & LRU_MASK
    }

    fn counter(self) -> u8 {
        (self.0 >> 24) as u8
    }
}

/// A candidate remembered across eviction calls by position, not by key copy.
#[derive(Clone, Copy, Debug, Default)]
struct PoolSlot {
    db: u8,
    slot: u32,
    score: u64,
}

/// Candidates with the score they had when pooled; `slots[..len]` are live.
#[derive(Debug, Default)]
pub(super) struct EvictionPool {
    len: usize,
    /// Lowest stored score, kept while the pool is full.
    floor: u64,
    slots: [PoolSlot; POOL_SIZE],
}

impl EvictionPool {
    fn offer(&mut self, candidate: PoolSlot) {
        // A full pool turns away a candidate no better than its weakest entry without
        // a scan. A pooled copy then keeps its stored score: the leader is rescored
        // before it is taken.
        if self.len == POOL_SIZE && candidate.score <= self.floor {
            return;
        }

        let live = &mut self.slots[..self.len];

        if let Some(existing) = live
            .iter_mut()
            .find(|slot| slot.db == candidate.db && slot.slot == candidate.slot)
        {
            existing.score = candidate.score;
        } else if self.len < POOL_SIZE {
            self.slots[self.len] = candidate;
            self.len += 1;
        } else if let Some(weakest) = live.iter_mut().min_by_key(|slot| slot.score) {
            *weakest = candidate;
        }

        if self.len == POOL_SIZE {
            self.floor = self.slots.iter().map(|slot| slot.score).min().unwrap_or(0);
        }
    }

    /// Removes the candidate with the highest stored score, the first of equal ones.
    fn pop_best(&mut self) -> Option<PoolSlot> {
        let live = &self.slots[..self.len];
        let mut best = 0;

        for (index, slot) in live.iter().enumerate().skip(1) {
            if slot.score > live[best].score {
                best = index;
            }
        }

        let last = self.len.checked_sub(1)?;

        self.slots.swap(best, last);
        self.len = last;
        Some(self.slots[last])
    }

    fn retain_database(&mut self, db: u8) {
        let mut kept = 0;

        for index in 0..self.len {
            if self.slots[index].db == db {
                self.slots[kept] = self.slots[index];
                kept += 1;
            }
        }
        self.len = kept;
    }

    fn best_score(&self) -> Option<u64> {
        self.slots[..self.len].iter().map(|slot| slot.score).max()
    }
}

fn seconds(now_ms: u64) -> u32 {
    ((now_ms / 1000) as u32) & LRU_MASK
}

fn minutes(now_ms: u64) -> u32 {
    ((now_ms / 60_000) as u32) & LFU_MINUTE_MASK
}

/// Idle time in seconds (LRU) or minutes (LFU), correct across clock wrap.
fn idle(now: u32, stamp: u32, mask: u32) -> u32 {
    now.wrapping_sub(stamp) & mask
}

fn decayed(counter: u8, idle_minutes: u32) -> u8 {
    let periods = idle_minutes / LFU_DECAY_MINUTES;

    counter.saturating_sub(periods.min(u32::from(u8::MAX)) as u8)
}

fn next_random(state: &mut u32) -> u32 {
    let mut x = *state;

    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    x
}

/// Redis `LFULogIncr`: the higher the counter, the less likely an increment.
fn lfu_increment(rng: &mut u32, counter: u8) -> u8 {
    if counter == u8::MAX {
        return counter;
    }

    let base = u32::from(counter.saturating_sub(LFU_INIT));
    let threshold = u32::MAX / (base * LFU_LOG_FACTOR + 1);

    if next_random(rng) <= threshold {
        counter + 1
    } else {
        counter
    }
}

/// Writes one access into `cell`: LRU stores the stamp, LFU also bumps the decayed counter.
fn stamp_cell(cell: &mut u32, stamp: u32, lfu: bool, rng: &mut u32) {
    let counter = match AccessMeta::from_raw(*cell) {
        None => {
            if lfu {
                LFU_INIT
            } else {
                1
            }
        }
        Some(previous) if lfu => {
            let aged = decayed(
                previous.counter(),
                idle(stamp, previous.stamp(), LFU_MINUTE_MASK),
            );

            lfu_increment(rng, aged)
        }
        Some(_) => 1,
    };

    *cell = AccessMeta::new(stamp, counter).0;
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Stamps the access metadata of a key just created or overwritten, as Redis does on
    /// every write: a key that is only written then ages from its last write instead of
    /// looking never used (the first to evict). Only recency-tracking policies keep the
    /// metadata, so the default policy pays one predictable branch. Log replay is skipped:
    /// its time sample is not the write's time.
    #[inline]
    pub(super) fn note_write(&mut self, key: &[u8]) {
        if self.eviction_policy.tracks_recency() && self.cached_now.get() != Some(0) {
            self.record_access(key);
        }
    }

    /// Runs `edit` on `key` without it counting as an access: expiry-driven edits (due
    /// hash fields) are not uses of the key, as in Redis.
    pub(super) fn without_access<R>(&mut self, key: &[u8], edit: impl FnOnce(&mut Self) -> R) -> R {
        if !self.eviction_policy.tracks_recency() {
            return edit(self);
        }

        let access = self.entries.access_get(key);
        let result = edit(self);

        // Only a changed cell is written back: a first write allocates the cells.
        if self.entries.access_get(key) != access
            && let Some(cell) = self.entries.access_mut(key)
        {
            *cell = access;
        }
        result
    }

    pub(super) fn record_access(&mut self, key: &[u8]) {
        let (stamp, lfu) = self.access_stamp();
        let KvShard {
            entries,
            access_rng,
            ..
        } = self;
        // One lookup: the cell is read and written in place.
        if let Some(cell) = entries.access_mut(key) {
            stamp_cell(cell, stamp, lfu, access_rng);
        }
    }

    /// [`Self::record_access`] for the record a read just found at `address`.
    pub(super) fn record_access_at(&mut self, address: u32) {
        let (stamp, lfu) = self.access_stamp();
        let KvShard {
            entries,
            access_rng,
            ..
        } = self;

        if let Some(cell) = entries.access_at(address) {
            stamp_cell(cell, stamp, lfu, access_rng);
        }
    }

    fn access_stamp(&self) -> (u32, bool) {
        let now_ms = self.now_unix_millis().get();
        let lfu = matches!(self.eviction_policy, EvictionPolicy::AllKeysLfu);

        (
            if lfu {
                minutes(now_ms)
            } else {
                seconds(now_ms)
            },
            lfu,
        )
    }

    /// Higher score evicts first. LRU: idle seconds. LFU: lower decayed counter first,
    /// then longer idle. A key without metadata was never read or written under a
    /// recency policy, so it goes first.
    pub(super) fn eviction_score(&self, db: u8, key: &[u8], deadline: Option<UnixMillis>) -> u64 {
        let access = self
            .database_maps()
            .find(|&(id, _)| id == db)
            .map_or(0, |(_, map)| map.access_get(key));

        self.access_score(access, deadline)
    }

    /// [`Self::eviction_score`] from the key's raw eviction metadata.
    pub(super) fn access_score(&self, access: u32, deadline: Option<UnixMillis>) -> u64 {
        let now_ms = self.now_unix_millis().get();
        let access = AccessMeta::from_raw(access);

        match self.eviction_policy {
            EvictionPolicy::VolatileTtl => u64::MAX - deadline.map_or(u64::MAX, UnixMillis::get),
            EvictionPolicy::AllKeysLfu => {
                let now = minutes(now_ms);
                let (counter, idle_minutes) = match access {
                    Some(meta) => {
                        let idle_minutes = idle(now, meta.stamp(), LFU_MINUTE_MASK);

                        (decayed(meta.counter(), idle_minutes), idle_minutes)
                    }
                    None => (0, LFU_MINUTE_MASK),
                };

                (u64::from(u8::MAX - counter) << 32) | u64::from(idle_minutes)
            }
            _ => match access {
                Some(meta) => u64::from(idle(seconds(now_ms), meta.stamp(), LRU_MASK)),
                None => u64::from(u32::MAX),
            },
        }
    }

    /// Drops pooled candidates of databases other than the active one, before a pick
    /// that may not evict there (a transaction buffer logs every delete against its own
    /// database). Left pooled, better-scored ones would turn the active database's
    /// candidates away.
    pub(super) fn keep_pool_database(&self) {
        if let Some(pool) = self.eviction_pool.borrow_mut().as_mut() {
            pool.retain_database(self.active);
        }
    }

    pub(super) fn offer_eviction_candidate(&self, db: u8, slot: usize, score: u64) {
        self.eviction_pool
            .borrow_mut()
            .get_or_insert_default()
            .offer(PoolSlot {
                db,
                slot: slot as u32,
                score,
            });
    }

    /// Pick the best pooled candidate. Scores are as of pooling, so only the leader is
    /// revalidated: a slot that no longer holds a live key is dropped, and a key read
    /// since it was pooled (now scoring lower) goes back to compete with the rest. An
    /// excluded key is dropped: kept, keys a write protects could fill the pool and turn
    /// every other candidate away. Without `other_databases` a candidate of another
    /// database is dropped too (see [`Self::keep_pool_database`]).
    pub(super) fn take_eviction_victim<'a>(
        &'a self,
        exclude: &[&[u8]],
        other_databases: bool,
        resolve: impl Fn(u8, usize) -> Option<(&'a [u8], Option<UnixMillis>)>,
    ) -> Option<DatabaseKey> {
        let mut pool_slot = self.eviction_pool.borrow_mut();
        let pool = pool_slot.get_or_insert_default();
        let mut victim = None;

        // Each turn removes a candidate or lowers its stored score to the current one,
        // which a second turn then accepts, so this ends within twice the pool size.
        while let Some(candidate) = pool.pop_best() {
            if !other_databases && candidate.db != self.active {
                continue;
            }

            let Some((key, deadline)) = resolve(candidate.db, candidate.slot as usize) else {
                continue;
            };

            if self.deadline_passed(candidate.db, key)
                || (candidate.db == self.active && exclude.contains(&key))
            {
                continue;
            }

            let score = self.eviction_score(candidate.db, key, deadline);

            if score < candidate.score && pool.best_score().is_some_and(|top| top > score) {
                pool.offer(PoolSlot { score, ..candidate });
                continue;
            }

            victim = Some((candidate.db, key.to_vec().into_boxed_slice()));
            break;
        }

        victim
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Seconds since the key was last read; `None` when the current policy keeps no LRU stamp.
    pub fn idle_seconds(&self, key: &[u8]) -> Option<u64> {
        if !self.eviction_policy.tracks_recency()
            || matches!(self.eviction_policy, EvictionPolicy::AllKeysLfu)
        {
            return None;
        }

        let now = seconds(self.now_unix_millis().get());

        Some(
            AccessMeta::from_raw(self.entries.access_get(key))
                .map_or(0, |meta| u64::from(idle(now, meta.stamp(), LRU_MASK))),
        )
    }

    /// The decayed LFU counter; `None` unless the policy is `allkeys-lfu`.
    pub fn access_frequency(&self, key: &[u8]) -> Option<u8> {
        if !matches!(self.eviction_policy, EvictionPolicy::AllKeysLfu) {
            return None;
        }

        let now = minutes(self.now_unix_millis().get());

        Some(
            AccessMeta::from_raw(self.entries.access_get(key)).map_or(LFU_INIT, |meta| {
                decayed(meta.counter(), idle(now, meta.stamp(), LFU_MINUTE_MASK))
            }),
        )
    }
}
