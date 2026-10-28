use super::*;

/// One deadline record. Heap entries are record IDs, not key copies. Keys that are not in the
/// key log (rare) live in `DeadlineDb::owned_keys`, marked by `NO_ADDRESS`.
pub(super) struct DeadlineRecord {
    /// Key-log address. `NO_ADDRESS` means `owned_keys` holds the only copy.
    pub(super) address: u32,
    /// Low 32 bits of the key hash (`DeadlineDb::hash_key` widens them again).
    pub(super) hash: u32,
    /// Heap position plus one; the non-zero niche keeps `Option<DeadlineRecord>` at 32 bytes.
    slot: std::num::NonZeroU32,
    pub(super) at: UnixMillis,
    /// The heap's ordering key: `at` when the record was last placed, never later than `at`.
    /// Moving a deadline later (every `SETEX` overwrite) only changes `at`; the record is
    /// re-placed when it reaches the top of the heap (`first_due`), not on each write.
    pub(super) heap_at: UnixMillis,
}

impl DeadlineRecord {
    pub(super) fn position(&self) -> u32 {
        self.slot.get() - 1
    }

    fn set_position(&mut self, position: u32) {
        self.slot = std::num::NonZeroU32::new(position + 1).expect("heap position fits");
    }

    pub(super) fn is_owned(&self) -> bool {
        self.address == NO_ADDRESS
    }
}

const _: () = assert!(size_of::<Option<DeadlineRecord>>() == 32);

/// Keys that expiry (or eviction inside a buffered transaction) removed but no delete
/// was recorded for yet. WAL replay keeps expired keys, so a later write to such a key
/// must be preceded by its delete in the log; the engine drains this before recording.
#[derive(Debug, Default)]
pub(super) struct RemovalLog {
    /// Off in memory mode and on replicas, whose log is the primary's.
    on: bool,
    /// A replica: its own clock never removes a key. Reads hide one past its deadline,
    /// and it stays stored until the primary's delete arrives, so a later deadline from
    /// the primary still finds it.
    keep_expired: bool,
    pending: Vec<(u8, Box<[u8]>)>,
    /// Drained into a transaction buffer that is not durable yet: committed, they are
    /// dropped; rolled back, they go back to `pending`.
    held: Vec<(u8, Box<[u8]>)>,
}

impl RemovalLog {
    #[inline]
    pub(super) fn keeps_expired(&self) -> bool {
        self.keep_expired
    }
}

const RECORD_CHUNK: usize = 2048;

/// Deadline records in fixed chunks: growing never copies and never leaves a freed, larger
/// block behind, so resident memory tracks the number of live records.
#[derive(Default)]
pub(super) struct Records {
    chunks: Vec<Vec<Option<DeadlineRecord>>>,
    len: usize,
}

impl Records {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Every chunk is allocated for `RECORD_CHUNK` records, so this is O(1): the charge
    /// is read on every deadline change.
    pub(super) fn capacity(&self) -> usize {
        self.chunks.len() * RECORD_CHUNK
    }

    pub(super) fn push(&mut self, record: Option<DeadlineRecord>) {
        if self
            .chunks
            .last()
            .is_none_or(|chunk| chunk.len() == RECORD_CHUNK)
        {
            self.chunks.push(Vec::with_capacity(RECORD_CHUNK));
        }

        self.chunks.last_mut().expect("chunk").push(record);
        self.len += 1;
    }
}

impl std::ops::Index<usize> for Records {
    type Output = Option<DeadlineRecord>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.chunks[index / RECORD_CHUNK][index % RECORD_CHUNK]
    }
}

impl std::ops::IndexMut<usize> for Records {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.chunks[index / RECORD_CHUNK][index % RECORD_CHUNK]
    }
}

pub(super) const NO_ADDRESS: u32 = u32::MAX;

/// Timed hashes by earliest field deadline: `(unix_millis, database, key)`.
pub(super) type FieldSweep = std::collections::BTreeSet<(u64, u8, Box<[u8]>)>;

pub(super) struct DeadlineDb {
    pub(super) records: Records,
    pub(super) free: Vec<u32>,
    pub(super) lookup: crate::slots::Slots<u32>,
    pub(super) hasher: ahash::RandomState,
    pub(super) heap: Vec<u32>,
    pub(super) len: usize,
    pub(super) owned_keys: AHashMap<u32, Box<[u8]>>,
    pub(super) owned_bytes: usize,
}

impl std::fmt::Debug for DeadlineDb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeadlineDb")
            .field("len", &self.len)
            .finish()
    }
}

impl Default for DeadlineDb {
    fn default() -> Self {
        Self {
            records: Records::default(),
            free: Vec::new(),
            lookup: crate::slots::Slots::with_capacity(0),
            hasher: ahash::RandomState::new(),
            heap: Vec::new(),
            len: 0,
            owned_keys: AHashMap::new(),
            owned_bytes: 0,
        }
    }
}

impl DeadlineDb {
    /// Mean milliseconds left over the first heap entries: a bounded estimate for `INFO`.
    pub(super) fn sampled_mean_ttl(&self, now: u64, samples: usize) -> u64 {
        let mut total = 0u64;
        let mut seen = 0u64;

        for id in self.heap.iter().take(samples) {
            if let Some(record) = &self.records[*id as usize] {
                total = total.saturating_add(record.at.get().saturating_sub(now));
                seen += 1;
            }
        }

        total.checked_div(seen).unwrap_or(0)
    }

    pub(super) fn charged_bytes(&self) -> usize {
        self.records
            .capacity()
            .saturating_mul(size_of::<Option<DeadlineRecord>>())
            .saturating_add(self.free.capacity().saturating_mul(size_of::<u32>()))
            .saturating_add(self.heap.capacity().saturating_mul(size_of::<u32>()))
            .saturating_add(
                self.lookup
                    .all_allocated_buckets()
                    .saturating_mul(size_of::<u32>() + 1),
            )
            .saturating_add(self.owned_bytes)
    }

    /// 32 significant bits, repeated in both halves so the table's bucket bits (low) and tag
    /// bits (high) are both well spread.
    pub(super) fn hash_key(&self, key: &[u8]) -> u64 {
        let narrow = self.hasher.hash_one(key) as u32;

        u64::from(narrow) << 32 | u64::from(narrow)
    }

    pub(super) fn owned_key(&self, id: u32) -> Option<&[u8]> {
        self.owned_keys.get(&id).map(|key| &**key)
    }

    pub(super) fn same_key<'a>(
        &self,
        id: u32,
        record: &DeadlineRecord,
        key: &[u8],
        bytes_at: &impl Fn(u32) -> Option<&'a [u8]>,
    ) -> bool {
        if record.is_owned() {
            return self.owned_key(id) == Some(key);
        }

        bytes_at(record.address).is_some_and(|stored| stored == key)
    }

    pub(super) fn id_of<'a>(
        &self,
        key: &[u8],
        bytes_at: impl Fn(u32) -> Option<&'a [u8]>,
    ) -> Option<u32> {
        let hash = self.hash_key(key);
        let records = &self.records;

        self.lookup
            .find(hash, |id| {
                records[*id as usize]
                    .as_ref()
                    .is_some_and(|record| self.same_key(*id, record, key, &bytes_at))
            })
            .copied()
    }

    pub(super) fn get<'a>(
        &self,
        key: &[u8],
        bytes_at: impl Fn(u32) -> Option<&'a [u8]>,
    ) -> Option<UnixMillis> {
        let id = self.id_of(key, bytes_at)?;

        Some(self.records[id as usize].as_ref()?.at)
    }

    pub(super) fn set<'a>(
        &mut self,
        key: &[u8],
        address: Option<u32>,
        at: UnixMillis,
        bytes_at: impl Fn(u32) -> Option<&'a [u8]>,
    ) -> Option<UnixMillis> {
        if let Some(id) = self.id_of(key, bytes_at) {
            let record = self.records[id as usize].as_mut().expect("live deadline");
            let previous = record.at;
            let same_address =
                address.is_none_or(|address| record.address == address && !record.is_owned());

            if previous == at && same_address {
                return Some(previous);
            }

            record.at = at;
            let position = record.position() as usize;
            let earlier = at < record.heap_at;

            if earlier {
                record.heap_at = at;
            }

            if let Some(address) = address {
                record.address = address;
                if let Some(owned) = self.owned_keys.remove(&id) {
                    self.owned_bytes -= owned.len();
                }
            }

            // A later deadline keeps its place; `heap_at` stays a lower bound.
            if earlier {
                self.sift_up(position);
            }

            return Some(previous);
        }

        let hash = self.hash_key(key);
        let id = self.allocate(hash, address, key, at);

        self.push_heap(id);
        self.insert_lookup(hash, id);
        None
    }

    /// Repoints the record of `key` that names key-log address `old` to `new`, after
    /// log reclaim moved the key record. Compares addresses, never reads key bytes.
    pub(super) fn move_address(&mut self, key: &[u8], old: u32, new: u32) {
        let hash = self.hash_key(key);
        let records = &self.records;
        let found = self
            .lookup
            .find(hash, |id| {
                records[*id as usize]
                    .as_ref()
                    .is_some_and(|record| !record.is_owned() && record.address == old)
            })
            .copied();

        if let Some(id) = found {
            self.records[id as usize]
                .as_mut()
                .expect("live deadline")
                .address = new;
        }
    }

    pub(super) fn note_address(&mut self, id: u32, address: u32) {
        let record = self.records[id as usize].as_mut().expect("live deadline");

        record.address = address;
        if let Some(owned) = self.owned_keys.remove(&id) {
            self.owned_bytes -= owned.len();
        }
    }

    pub(super) fn remove<'a>(
        &mut self,
        key: &[u8],
        bytes_at: impl Fn(u32) -> Option<&'a [u8]>,
    ) -> bool {
        let Some(id) = self.id_of(key, bytes_at) else {
            return false;
        };

        self.remove_id(id);
        true
    }

    pub(super) fn allocate(
        &mut self,
        hash: u64,
        address: Option<u32>,
        key: &[u8],
        at: UnixMillis,
    ) -> u32 {
        let record = DeadlineRecord {
            address: address.unwrap_or(NO_ADDRESS),
            hash: hash as u32,
            slot: std::num::NonZeroU32::new(self.heap.len() as u32 + 1).expect("heap position"),
            at,
            heap_at: at,
        };

        let id = if let Some(id) = self.free.pop() {
            self.records[id as usize] = Some(record);
            id
        } else {
            let id = self.records.len() as u32;

            self.records.push(Some(record));
            id
        };

        if address.is_none() {
            self.owned_keys.insert(id, Box::from(key));
            self.owned_bytes += key.len();
        }
        self.len += 1;
        id
    }

    /// The lookup hash of a stored record id, from the bits kept in its record.
    fn stored_hash(records: &Records, id: u32) -> u64 {
        let narrow = records[id as usize].as_ref().expect("live deadline").hash;

        u64::from(narrow) << 32 | u64::from(narrow)
    }

    pub(super) fn insert_lookup(&mut self, hash: u64, id: u32) {
        let DeadlineDb {
            records, lookup, ..
        } = self;

        lookup.insert_unique(hash, id, |stored| Self::stored_hash(records, *stored));
    }

    /// Moves up to `budget` lookup slots of an unfinished resize. Returns true when none
    /// is left.
    pub(super) fn migrate_step(&mut self, budget: usize) -> bool {
        let DeadlineDb {
            records, lookup, ..
        } = self;

        lookup.migrate(budget, &|stored| Self::stored_hash(records, *stored))
    }

    /// Starts shrinking a lookup table that removals left mostly empty. Eviction
    /// sampling walks its buckets, so a sparse table also slows every volatile pick.
    pub(super) fn shrink_if_sparse(&mut self) -> bool {
        if self.lookup.is_migrating() {
            return false;
        }

        let Some(target) =
            crate::index::reclaim_capacity(self.lookup.len(), self.lookup.full_capacity())
        else {
            return false;
        };
        let DeadlineDb {
            records, lookup, ..
        } = self;

        lookup.begin_shrink(target, |stored| Self::stored_hash(records, *stored));
        true
    }

    pub(super) fn heap_less(&self, left: usize, right: usize) -> bool {
        let left_id = self.heap[left];
        let right_id = self.heap[right];
        let left_at = self.records[left_id as usize]
            .as_ref()
            .expect("live deadline")
            .heap_at;
        let right_at = self.records[right_id as usize]
            .as_ref()
            .expect("live deadline")
            .heap_at;
        (left_at, left_id) < (right_at, right_id)
    }

    pub(super) fn heap_swap(&mut self, left: usize, right: usize) {
        self.heap.swap(left, right);
        self.records[self.heap[left] as usize]
            .as_mut()
            .expect("live deadline")
            .set_position(left as u32);
        self.records[self.heap[right] as usize]
            .as_mut()
            .expect("live deadline")
            .set_position(right as u32);
    }

    pub(super) fn sift_up(&mut self, mut position: usize) -> usize {
        while position > 0 {
            let parent = (position - 1) / 2;
            if !self.heap_less(position, parent) {
                break;
            }
            self.heap_swap(position, parent);
            position = parent;
        }
        position
    }

    pub(super) fn sift_down(&mut self, mut position: usize) {
        while let Some(left) = position.checked_mul(2).and_then(|n| n.checked_add(1)) {
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.heap.len() && self.heap_less(right, left) {
                right
            } else {
                left
            };
            if !self.heap_less(child, position) {
                break;
            }
            self.heap_swap(position, child);
            position = child;
        }
    }

    pub(super) fn fix_heap(&mut self, position: usize) {
        let position = self.sift_up(position);
        self.sift_down(position);
    }

    pub(super) fn push_heap(&mut self, id: u32) {
        self.heap.push(id);
        self.sift_up(self.heap.len() - 1);
    }

    pub(super) fn remove_heap(&mut self, position: usize) {
        let last = self.heap.len() - 1;
        if position != last {
            self.heap_swap(position, last);
        }
        self.heap.pop();
        if position < self.heap.len() {
            self.fix_heap(position);
        }
    }

    pub(super) fn remove_id(&mut self, id: u32) {
        let narrow = self.records[id as usize]
            .as_ref()
            .expect("live deadline")
            .hash;
        let hash = u64::from(narrow) << 32 | u64::from(narrow);

        self.lookup.remove(hash, |stored| *stored == id);

        let position = self.records[id as usize]
            .as_ref()
            .expect("live deadline")
            .position() as usize;
        self.remove_heap(position);
        self.records[id as usize].take().expect("live deadline");
        if let Some(owned) = self.owned_keys.remove(&id) {
            self.owned_bytes -= owned.len();
        }
        self.free.push(id);
        self.len -= 1;
        if self.len == 0 {
            *self = Self::default();
            return;
        }

        if let Some(target) = crate::index::reclaim_capacity(self.len, self.heap.capacity()) {
            self.heap.shrink_to(target);
        }

        // Eviction sampling walks lookup buckets, so do not keep a mostly empty table.
        // Removals also drive a resize already under way, so a mass delete finishes it.
        if self.lookup.is_migrating() {
            self.migrate_step(crate::slots::MIGRATE_PER_ELEMENT);
        } else {
            self.shrink_if_sparse();
        }
    }

    /// A lower bound on every deadline (exact once `first_due` has re-placed moved ones).
    pub(super) fn min_at(&self) -> Option<UnixMillis> {
        let id = *self.heap.first()?;
        Some(
            self.records[id as usize]
                .as_ref()
                .expect("live deadline")
                .heap_at,
        )
    }

    /// The key whose deadline passed first, re-placing records whose deadline moved later.
    #[cfg(test)]
    pub(super) fn first_due(
        &mut self,
        now: UnixMillis,
        bytes_at: impl Fn(u32) -> Box<[u8]>,
    ) -> Option<Box<[u8]>> {
        let id = self.first_due_id(now)?;
        let record = self.records[id as usize].as_ref().expect("live deadline");

        Some(if record.is_owned() {
            self.owned_keys[&id].clone()
        } else {
            bytes_at(record.address)
        })
    }

    /// The record id of the deadline that passed first, re-placing records whose deadline
    /// moved later.
    pub(super) fn first_due_id(&mut self, now: UnixMillis) -> Option<u32> {
        loop {
            let id = *self.heap.first()?;
            let record = self.records[id as usize].as_mut().expect("live deadline");

            if record.heap_at > now {
                return None;
            }

            if record.at <= now {
                return Some(id);
            }

            record.heap_at = record.at;
            self.sift_down(0);
        }
    }

    /// The key bytes of record `id`: its own copy, or the key at its index address.
    pub(super) fn key_of<'a>(
        &'a self,
        id: u32,
        bytes_at: impl FnOnce(u32) -> Option<&'a [u8]>,
    ) -> Option<&'a [u8]> {
        let record = self.records[id as usize].as_ref()?;

        if record.is_owned() {
            self.owned_keys.get(&id).map(|key| &**key)
        } else {
            bytes_at(record.address)
        }
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub(crate) fn deadline(&self, db: u8, key: &[u8]) -> Option<UnixMillis> {
        if self.expiration.keys_with_ttl == 0 {
            return None;
        }

        let KvShard {
            entries,
            databases,
            deadlines,
            active,
            ..
        } = self;

        let active = *active;

        deadlines.get(db as usize)?.get(key, |address| {
            index_key_bytes(entries, databases, active, db, address)
        })
    }

    pub(super) fn deadline_passed(&self, db: u8, key: &[u8]) -> bool {
        if self.expiration.keys_with_ttl == 0 {
            return false;
        }

        let now = self.now_unix_millis();

        // `soonest_deadline` is a lower bound on every stored deadline, or `None` when unknown.

        if self.soonest_deadline.is_some_and(|soonest| soonest > now) {
            return false;
        }

        self.deadline(db, key)
            .is_some_and(|deadline| deadline <= now)
    }

    /// Keeps the index's per-key "may have a deadline" bit in step with `DeadlineDb`.
    pub(super) fn mark_ttl_hint(&mut self, db: u8, key: &[u8], on: bool) {
        let index = if db == self.active {
            Some(&mut self.entries)
        } else {
            self.databases.get_mut(db as usize).and_then(Option::as_mut)
        };

        if let Some(index) = index {
            if on {
                index.mark_ttl(key);
            } else {
                index.clear_ttl(key);
            }
        }
    }

    /// Call after a key's record was removed and inserted again without going through
    /// `insert_entry`'s deadline clearing (collection slab promotion keeps the TTL).
    /// `deadline` is `deadline_id(key)` taken before the record was removed: the new
    /// record sits at another key-log address, and the deadline must follow it.
    pub(super) fn restore_deadline(&mut self, key: &[u8], deadline: Option<u32>) {
        if let Some(id) = deadline {
            self.point_deadline(key, id);
            self.entries.mark_ttl(key);
        }
    }

    pub(super) fn deadline_db_mut(&mut self, db: u8) -> &mut DeadlineDb {
        let index = db as usize;

        if self.deadlines.len() <= index {
            self.deadlines.resize_with(index + 1, DeadlineDb::default);
        }

        &mut self.deadlines[index]
    }

    pub(super) fn set_deadline(&mut self, db: u8, key: &[u8], expires_at: UnixMillis) {
        let before = self.deadline_db_mut(db).charged_bytes();
        let empty = self.expiration.keys_with_ttl == 0;
        let KvShard {
            entries,
            databases,
            deadlines,
            active,
            ..
        } = self;

        let active = *active;

        let address = if db == active {
            entries.key_address(key)
        } else {
            databases
                .get(db as usize)
                .and_then(|slot| slot.as_ref()?.key_address(key))
        };

        let previous = deadlines[db as usize].set(key, address, expires_at, |stored| {
            index_key_bytes(entries, databases, active, db, stored)
        });

        if previous.is_none() {
            self.remember_deadline(true);
        }

        self.note_deadline_bound(empty, expires_at);
        self.mark_ttl_hint(db, key, true);
        self.update_deadline_index_bytes(db as usize, before);
        self.refresh_global_memory_usage();
    }

    pub(super) fn note_deadline_bound(&mut self, was_empty: bool, expires_at: UnixMillis) {
        if was_empty {
            self.soonest_deadline = Some(expires_at);

            return;
        }

        if let Some(soonest) = self.soonest_deadline
            && expires_at < soonest
        {
            self.soonest_deadline = Some(expires_at);
        }
    }

    pub(super) fn clear_deadline(&mut self, db: u8, key: &[u8]) -> bool {
        if self.expiration.keys_with_ttl == 0 {
            return false;
        }

        if self.deadlines.len() <= db as usize {
            return false;
        }

        let KvShard {
            entries,
            databases,
            deadlines,
            active,
            ..
        } = self;

        let active = *active;
        let before = deadlines[db as usize].charged_bytes();

        if !deadlines[db as usize].remove(key, |address| {
            index_key_bytes(entries, databases, active, db, address)
        }) {
            return false;
        }

        self.mark_ttl_hint(db, key, false);
        self.update_deadline_index_bytes(db as usize, before);
        self.forget_deadline(true);

        if self.expiration.keys_with_ttl == 0 {
            self.soonest_deadline = None;
        }
        self.refresh_global_memory_usage();

        true
    }

    /// Removes deadline record `id` of `db` without touching any key.
    fn drop_stale_deadline(&mut self, db: u8, id: u32, key: Option<&[u8]>) {
        let index = db as usize;
        let before = self.deadlines[index].charged_bytes();

        self.deadlines[index].remove_id(id);

        if let Some(key) = key {
            self.mark_ttl_hint(db, key, false);
        }

        self.update_deadline_index_bytes(index, before);
        self.forget_deadline(true);
        self.refresh_global_memory_usage();
    }

    /// Moves the deadline index of `db` out whole. Its key addresses point into that
    /// database's map, so it stays valid as long as it travels with the map.
    pub(super) fn take_deadline_db(&mut self, db: u8) -> DeadlineDb {
        self.soonest_deadline = None;
        let index = db as usize;

        if self.deadlines.len() <= index {
            return DeadlineDb::default();
        }

        let before = self.deadlines[index].charged_bytes();
        let taken = std::mem::take(&mut self.deadlines[index]);

        self.update_deadline_index_bytes(index, before);
        self.refresh_global_memory_usage();
        taken
    }

    /// Puts back an index taken by `take_deadline_db` together with its map.
    pub(super) fn install_deadline_db(&mut self, db: u8, deadlines: DeadlineDb) {
        self.soonest_deadline = None;
        let index = db as usize;

        if self.deadlines.len() <= index {
            self.deadlines.resize_with(index + 1, DeadlineDb::default);
        }

        let before = self.deadlines[index].charged_bytes();
        self.deadlines[index] = deadlines;
        self.update_deadline_index_bytes(index, before);
        self.refresh_global_memory_usage();
    }

    pub(super) fn swap_deadline_dbs(deadlines: &mut Vec<DeadlineDb>, left: u8, right: u8) {
        let left = left as usize;
        let right = right as usize;
        let need = left.max(right) + 1;

        if deadlines.len() < need {
            deadlines.resize_with(need, DeadlineDb::default);
        }

        deadlines.swap(left, right);
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub(super) fn note_expired(&mut self, cause: ExpireCause) {
        self.expiration.expired_keys = self.expiration.expired_keys.saturating_add(1);
        match cause {
            ExpireCause::Passive => {
                self.expiration.expired_keys_passive =
                    self.expiration.expired_keys_passive.saturating_add(1);
            }

            ExpireCause::Active => {
                self.expiration.expired_keys_active =
                    self.expiration.expired_keys_active.saturating_add(1);
            }
        }
    }

    pub fn expiration_stats(&self) -> ExpirationStats {
        self.expiration
    }

    pub(super) fn remove_expired(&mut self, key: &[u8]) -> bool {
        self.remove_expired_with(key, ExpireCause::Passive)
    }

    pub(super) fn remove_expired_with(&mut self, key: &[u8], cause: ExpireCause) -> bool {
        if !self.key_is_expired(key) {
            return false;
        }

        if self.removals.keep_expired {
            return true;
        }

        let before = self.entry_charge(key);
        let entry = self
            .remove_entry(key)
            .expect("expired entry disappeared from single-owner shard");

        self.discard_value(entry.value, before);
        self.note_expired(cause);
        if self.removals.on {
            self.removals.pending.push((self.active, key.into()));
        }
        self.reconcile_entry_charge(before, key);
        self.note_key(key, crate::notify::KeyEvent::Expired);
        true
    }

    /// Replica mode: the primary's deletes alone remove keys. Expiry by this shard's
    /// clock hides keys from reads but never removes them, and a memory limit never
    /// evicts or refuses (Redis `replica-ignore-maxmemory`). Off again on a promoted node.
    pub fn follow_primary(&mut self, on: bool) {
        self.removals.keep_expired = on;
    }

    pub fn follows_primary(&self) -> bool {
        self.removals.keep_expired
    }

    /// Queue a delete record for every key expiry removes from now on (see [`RemovalLog`]).
    pub fn log_removals(&mut self, on: bool) {
        self.removals.on = on;
        if !on {
            self.removals.pending = Vec::new();
        }
    }

    #[inline]
    pub fn has_unlogged_removals(&self) -> bool {
        !self.removals.pending.is_empty()
    }

    /// Whether a key about to be written may be stored but already past its deadline.
    /// Free while no deadline is due, so the write path pays nothing then.
    #[inline]
    pub fn write_may_find_expired(&self) -> bool {
        self.removals.on && (self.soonest_deadline_due() || self.field_deadline_due())
    }

    /// Some hash has a field past its deadline, so it may have no live field left.
    fn field_deadline_due(&self) -> bool {
        self.field_sweep.as_ref().is_some_and(|sweep| {
            sweep
                .first()
                .is_some_and(|(at, _, _)| *at <= self.now_unix_millis().get())
        })
    }

    /// Removes `key` if it is stored but expired, queueing its delete record.
    pub fn reap_expired(&mut self, key: &[u8]) -> bool {
        self.remove_expired(key)
    }

    /// [`Self::reap_expired`] in database `database`.
    pub fn reap_expired_in(&mut self, database: u8, key: &[u8]) -> bool {
        let previous = self.active;

        self.enter(database);
        let reaped = self.remove_expired(key);

        self.enter(previous);
        reaped
    }

    /// Hands each unlogged removal to `record`, in database `only` when given (the rest
    /// stay queued). A removal `record` refuses stays queued, as does every one after it.
    /// With `hold`, recorded removals wait for [`Self::settle_held_removals`].
    pub fn drain_removals<E>(
        &mut self,
        only: Option<u8>,
        hold: bool,
        mut record: impl FnMut(u8, &[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut pending = std::mem::take(&mut self.removals.pending);
        let mut result = Ok(());

        pending.retain_mut(|(database, key)| {
            if result.is_err() || only.is_some_and(|only| only != *database) {
                return true;
            }

            match record(*database, key) {
                Ok(()) => {
                    if hold {
                        self.removals.held.push((*database, std::mem::take(key)));
                    }
                    false
                }

                Err(error) => {
                    result = Err(error);
                    true
                }
            }
        });

        // A mass expiry leaves a large buffer behind; do not keep it.
        if pending.is_empty() && pending.capacity() > 1024 {
            pending = Vec::new();
        }
        self.removals.pending = pending;
        result
    }

    /// A removal recorded into a transaction buffer, outside the expiry queue: an
    /// eviction made inside `MULTI` or a script.
    pub fn hold_removal(&mut self, database: u8, key: &[u8]) {
        if self.removals.on {
            self.removals.held.push((database, key.into()));
        }
    }

    /// The transaction holding drained removals ended. Committed, its buffer logged them;
    /// rolled back, nothing did, so they are queued again.
    pub fn settle_held_removals(&mut self, committed: bool) {
        if self.removals.held.is_empty() {
            return;
        }

        if committed {
            self.removals.held.clear();
        } else {
            let held = std::mem::take(&mut self.removals.held);
            self.removals.pending.extend(held);
        }
    }

    /// Set an absolute expiration deadline for an existing live key.
    pub fn expire_at(&mut self, key: &[u8], expires_at: UnixMillis) -> bool {
        if self.live_deadline(key).is_none() {
            // Missing, or past its deadline and still stored: reap it.
            self.remove_expired(key);

            return false;
        }

        self.set_deadline(self.active, key, expires_at);
        self.note_changed(key);
        true
    }

    /// Remove an expiration deadline from an existing live key.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        if self.remove_expired(key) {
            return false;
        }

        if self.entries.lookup(key).is_none() {
            return false;
        }

        let cleared = self.clear_deadline(self.active, key);
        if cleared {
            self.note_changed(key);
        }
        cleared
    }

    pub fn now_unix_millis(&self) -> UnixMillis {
        if let Some(cached) = self.cached_now.get() {
            return UnixMillis::new(cached);
        }

        self.clock.now_unix_millis()
    }

    /// Read the clock once for a batch of commands on this connection.
    /// Replay applies historical mutations before current-time expiry is evaluated.
    /// The previous sample is restored after each replay or after startup recovery.
    pub(crate) fn pause_expiration_for_replay(&self) -> Option<u64> {
        self.cached_now.replace(Some(0))
    }

    pub(crate) fn resume_expiration_after_replay(&self, previous: Option<u64>) {
        self.cached_now.set(previous);
    }

    pub fn sample_time(&self) {
        let now = self.clock.now_unix_millis();

        self.cached_now.set(Some(now.get()));
    }

    pub(crate) fn clear_time_sample(&self) {
        self.cached_now.set(None);
    }

    /// `None` when the key is missing or expired, else its deadline (if any): what
    /// `contains_key` + `expires_at` answer, with one index lookup instead of three.
    pub(crate) fn live_deadline(&self, key: &[u8]) -> Option<Option<UnixMillis>> {
        let (_, may_expire) = self.entries.lookup_ttl(key)?;

        if !may_expire {
            return Some(None);
        }

        match self.deadline(self.active, key) {
            Some(at) if at <= self.now_unix_millis() => None,
            deadline => Some(deadline),
        }
    }

    pub fn expires_at(&self, key: &[u8]) -> Option<UnixMillis> {
        self.live(key)?;

        self.deadline(self.active, key)
    }

    pub fn pttl(&self, key: &[u8]) -> i64 {
        if self.entries.lookup(key).is_none() || self.key_is_expired(key) {
            return -2;
        }

        let Some(deadline) = self.deadline(self.active, key) else {
            return -1;
        };

        let now = self.now_unix_millis();

        if deadline.get() <= now.get() {
            return -2;
        }

        i64::try_from(deadline.get() - now.get()).unwrap_or(i64::MAX)
    }

    pub fn ttl_seconds(&self, key: &[u8]) -> i64 {
        match self.pttl(key) {
            remaining if remaining < 0 => remaining,
            millis => millis.saturating_add(500) / 1_000,
        }
    }

    /// Whether some deadline may already have passed.
    pub fn soonest_deadline_due(&self) -> bool {
        self.expiration.keys_with_ttl != 0
            && self
                .soonest_deadline
                .is_none_or(|soonest| soonest <= self.now_unix_millis())
    }

    #[inline]
    pub fn expire_due(&mut self) -> usize {
        // Checked here as well, so a store with no TTLs skips the call.
        if self.expiration.keys_with_ttl == 0 {
            return 0;
        }

        self.expire_cycle(self.active_expire_keys)
    }

    /// Reaps up to `budget` expired keys across every database.
    pub fn expire_cycle(&mut self, budget: usize) -> usize {
        if budget == 0 || self.expiration.keys_with_ttl == 0 || self.removals.keep_expired {
            return 0;
        }

        let now = self.now_unix_millis();

        if self.soonest_deadline.is_some_and(|soonest| soonest > now) {
            return 0;
        }

        let mut reaped = 0usize;
        let databases = self.deadlines.len();
        let start = self.expire_cursor % databases;
        let previous = self.active;
        // One buffer for the whole cycle: the key is copied out of the index because
        // removing it needs the shard mutably.
        let mut key = Vec::new();

        for offset in 0..databases {
            let db = (start + offset) % databases;
            while reaped < budget {
                let Some(id) = self.deadlines[db].first_due_id(now) else {
                    break;
                };
                let found = {
                    let KvShard {
                        entries,
                        databases,
                        deadlines,
                        active,
                        ..
                    } = self;
                    let active = *active;
                    let bytes = deadlines[db].key_of(id, |address| {
                        index_key_bytes(entries, databases, active, db as u8, address)
                    });

                    key.clear();
                    bytes
                        .inspect(|bytes| key.extend_from_slice(bytes))
                        .is_some()
                };
                self.enter(db as u8);

                if found && self.remove_expired_with(&key, ExpireCause::Active) {
                    reaped += 1;
                } else {
                    // A due deadline whose key is gone or no longer expires: drop the
                    // record, or the same one comes back until the budget is spent.
                    self.drop_stale_deadline(db as u8, id, found.then_some(key.as_slice()));
                }
            }
            if reaped == budget {
                self.expire_cursor = (db + 1) % databases;
                break;
            }
        }

        self.enter(previous);
        self.soonest_deadline = self.deadlines.iter().filter_map(DeadlineDb::min_at).min();
        reaped
    }
}
