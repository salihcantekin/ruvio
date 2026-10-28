use super::*;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SnapshotEntry {
    pub database: u8,
    pub key: Vec<u8>,
    pub expires_at: Option<UnixMillis>,
    pub value: SnapshotValue,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SnapshotValue {
    Bytes(Vec<u8>),
    Json(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    /// A hash with at least one field TTL: `(field, value, unix_millis)`, `0` for no TTL.
    HashTtl(Vec<(Vec<u8>, Vec<u8>, u64)>),
    Semaphore {
        next_token: u64,
        holders: Vec<(u64, u64)>,
    },
    SortedSet(Vec<(f64, Vec<u8>)>),
    /// A filter from before layers: one fixed bit array. Read only; see `BloomLayers`.
    BloomFilter {
        words: Vec<u64>,
        hash_count: u8,
    },
    BloomLayers {
        layers: Vec<BloomLayer>,
        error_rate: f64,
        expansion: u32,
    },
    Stream {
        started: bool,
        last_millis: u64,
        last_sequence: u64,
        entries: Vec<crate::stream::StreamEntry>,
        groups: Vec<crate::stream::ConsumerGroup>,
        delayed: Vec<(u64, u64, u64)>,
    },
    Lease {
        token: u64,
        expires_at: u64,
    },
    Limit {
        used: u64,
    },
    Once,
    Versioned {
        version: u64,
        bytes: Vec<u8>,
    },
}

/// A key and the logical database it lives in.
pub(crate) type DatabaseKey = (u8, Box<[u8]>);

/// What the journal remembers about a key the snapshot has not settled by walking.
#[derive(Debug)]
pub(super) enum Captured {
    /// The key did not exist at the snapshot point.
    Absent,
    /// The walk already wrote the key's snapshot-time value.
    Written,
    /// The key's value at the snapshot point, copied before the first change.
    Entry(SnapshotEntry),
}

/// Which key-log addresses the walk has written, one bit per 4 bytes of log. Addresses
/// of large blocks (top bit set) are rare and kept in a set.
#[derive(Debug, Default)]
pub(super) struct WrittenAddresses {
    bits: Vec<u64>,
    large: AHashSet<u32>,
}

impl WrittenAddresses {
    const GRANULE_SHIFT: u32 = 2;

    fn with_log_len(len: u32) -> Self {
        let granules = (len >> Self::GRANULE_SHIFT) as usize + 1;

        Self {
            bits: vec![0; granules.div_ceil(64)],
            large: AHashSet::new(),
        }
    }

    pub(super) fn bytes(&self) -> usize {
        self.bits.len() * size_of::<u64>()
    }

    fn contains(&self, address: u32) -> bool {
        let granule = (address >> Self::GRANULE_SHIFT) as usize;

        match self.bits.get(granule / 64) {
            Some(word) if address < LOG_LARGE => word & (1 << (granule % 64)) != 0,
            _ => self.large.contains(&address),
        }
    }

    fn insert(&mut self, address: u32) {
        let granule = (address >> Self::GRANULE_SHIFT) as usize;

        match self.bits.get_mut(granule / 64) {
            Some(word) if address < LOG_LARGE => *word |= 1 << (granule % 64),
            _ => {
                self.large.insert(address);
            }
        }
    }
}

/// Walk state of one database in the snapshot plan.
#[derive(Debug)]
pub(super) struct DatabaseWalk {
    /// Key-log length when the snapshot began; keys at or above it are newer.
    /// `None` for an index without a key log, which is walked in one step.
    pub(super) log_len: Option<u32>,
    pub(super) written: WrittenAddresses,
}

#[derive(Debug)]
pub(super) struct SnapshotSession {
    /// Per database, keys the walk must not write from the live table.
    pub(super) captured: Vec<AHashMap<Box<[u8]>, Captured>>,
    pub(super) journal_bytes: usize,
    pub(super) maximum_bytes: usize,
    /// Databases that existed at the snapshot point, ascending, and the walk position.
    pub(super) plan: Vec<u8>,
    pub(super) position: usize,
    pub(super) cursor: u64,
    pub(super) walks: AHashMap<u8, DatabaseWalk>,
}

impl SnapshotSession {
    fn done(&self) -> bool {
        self.position >= self.plan.len()
    }

    fn captured_in(&self, database: u8) -> Option<&AHashMap<Box<[u8]>, Captured>> {
        self.captured.get(usize::from(database))
    }

    /// True when the walk has already written, or will never write, `address`.
    fn walked(&self, database: u8, address: Option<u32>) -> bool {
        let Some(index) = self.plan.iter().position(|planned| *planned == database) else {
            return true;
        };

        match index.cmp(&self.position) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => {
                let walk = &self.walks[&database];

                address.is_some_and(|address| walk.written.contains(address))
            }
        }
    }
}

/// Result of one `snapshot_step`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotStep {
    /// More keys remain.
    More,
    /// An index is moving slots into a larger table; try again after maintenance.
    Waiting,
    /// Every key of the plan has been walked.
    Done,
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// An expired key is left out of a snapshot, except on a replica: it keeps the key
    /// until the primary deletes it, and a restart must too.
    pub(super) fn snapshot_skips(&self, db: u8, key: &[u8]) -> bool {
        !self.removals.keeps_expired() && self.deadline_passed(db, key)
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot_entries(&self) -> Vec<SnapshotEntry> {
        let mut entries = Vec::new();

        for (db, map) in self.database_maps() {
            for (key, entry) in map.entries() {
                if self.snapshot_skips(db, key) {
                    continue;
                }

                entries.push(live_snapshot_entry(
                    db,
                    key,
                    entry,
                    self.deadline(db, key),
                    &self.value_log,
                ));
            }
        }

        sort_snapshot_entries(&mut entries);
        entries
    }

    pub(super) fn map_of(&self, database: u8) -> Option<&I> {
        if database == self.active {
            Some(&self.entries)
        } else {
            self.databases.get(usize::from(database))?.as_ref()
        }
    }

    /// Starts a snapshot at the current state. The walk then runs through `snapshot_step`
    /// while writes keep going; `maximum_bytes` bounds the copies the journal may hold.
    pub(crate) fn begin_snapshot(&mut self, maximum_bytes: usize) -> Result<(), &'static str> {
        if self.snapshot.is_some() {
            return Err("snapshot already in progress");
        }

        let plan = self.database_ids();
        let mut walks = AHashMap::new();
        let mut bitmap_bytes = 0usize;

        for &database in &plan {
            let log_len = self.map_of(database).and_then(StorageIndex::key_log_len);
            let written =
                log_len.map_or_else(WrittenAddresses::default, WrittenAddresses::with_log_len);

            bitmap_bytes += written.bytes();
            walks.insert(database, DatabaseWalk { log_len, written });
        }

        self.snapshot = Some(SnapshotSession {
            captured: Vec::new(),
            journal_bytes: 0,
            maximum_bytes,
            plan,
            position: 0,
            cursor: 0,
            walks,
        });
        self.memory_usage.snapshot_journal_bytes = bitmap_bytes;
        Ok(())
    }

    pub(crate) fn journal_is_open(&self) -> bool {
        self.snapshot.is_some()
    }

    /// Drops the running snapshot and its journal. The caller starts a new one later.
    pub(crate) fn abort_snapshot(&mut self) {
        if self.snapshot.is_some() {
            self.snapshot_aborts += 1;
        }

        self.cancel_snapshot();
    }

    /// Ends the running snapshot without counting it as aborted: `SAVE` or shutdown
    /// replaces it with one taken now.
    pub(crate) fn cancel_snapshot(&mut self) {
        self.snapshot = None;
        self.memory_usage.snapshot_journal_bytes = 0;
    }

    /// Snapshots aborted so far: a database-wide rewrite, or a journal past its bound.
    pub(crate) fn snapshot_aborts(&self) -> u64 {
        self.snapshot_aborts
    }

    /// Bytes the running snapshot holds in journal copies.
    pub(crate) fn snapshot_journal_used(&self) -> usize {
        self.snapshot
            .as_ref()
            .map_or(0, |session| session.journal_bytes)
    }

    /// Settles `key` of the active database before a write changes it, so the snapshot
    /// still sees the value it had when the snapshot began. A walk that already passed the
    /// key needs only a marker; one that has not gets a copy. A journal past its bound
    /// aborts the snapshot instead of failing the write.
    pub(crate) fn capture_snapshot_key(&mut self, key: &[u8]) {
        let database = self.active;
        let Some(session) = self.snapshot.as_ref() else {
            return;
        };

        if !session.plan.contains(&database)
            || session
                .captured_in(database)
                .is_some_and(|keys| keys.contains_key(key))
        {
            return;
        }

        let found = self
            .entries
            .get_key_value(key)
            .filter(|(stored_key, _)| !self.snapshot_skips(self.active, stored_key));
        let address = self.entries.key_address(key);
        let walk = &session.walks[&database];
        // An address past the snapshot-time log end belongs to a key created after the
        // snapshot point, even if no earlier capture saw it.
        let newer =
            matches!((walk.log_len, address), (Some(end), Some(at)) if at >= end && at < LOG_LARGE);

        // The stored charge is known without touching the value. A value that would push
        // the journal past its bound aborts here, before it is copied; the exact size of a
        // copy that is made is checked below.
        if let Some((stored_key, entry)) = found
            && !newer
            && !session.walked(database, address)
        {
            let charge = super::charge_of(stored_key, entry);
            let estimate = 32 + key.len() + charge.value_bytes + charge.collection_bytes;

            if session.journal_bytes.saturating_add(estimate) > session.maximum_bytes {
                self.abort_snapshot();
                return;
            }
        }

        let captured = match found {
            None => Captured::Absent,
            Some(_) if newer => Captured::Absent,
            Some(_) if session.walked(database, address) => Captured::Written,
            Some((stored_key, entry)) => Captured::Entry(live_snapshot_entry(
                database,
                stored_key,
                entry,
                self.deadline(database, stored_key),
                &self.value_log,
            )),
        };

        let charge = 32
            + key.len()
            + match &captured {
                Captured::Entry(entry) => snapshot_payload_bytes(entry),
                _ => 0,
            };
        let session = self
            .snapshot
            .as_mut()
            .expect("snapshot presence was checked");

        if session.journal_bytes.saturating_add(charge) > session.maximum_bytes {
            self.abort_snapshot();
            return;
        }

        let index = usize::from(database);

        if session.captured.len() <= index {
            session.captured.resize_with(index + 1, AHashMap::new);
        }

        session.journal_bytes += charge;
        session.captured[index].insert(key.into(), captured);
        self.memory_usage.snapshot_journal_bytes = session.journal_bytes
            + session
                .walks
                .values()
                .map(|walk| walk.written.bytes())
                .sum::<usize>();
    }

    /// Captures `key` of `database` for the running snapshot.
    pub(crate) fn capture_snapshot_key_in(&mut self, database: u8, key: &[u8]) {
        if self.snapshot.is_none() {
            return;
        }

        let previous = self.active;

        self.enter(database);
        self.capture_snapshot_key(key);
        self.enter(previous);
    }

    /// A database-wide rewrite (`FLUSHDB`, `SWAPDB`, a transaction that touches whole
    /// databases) is rare, and copying every key would exceed any journal bound, so it
    /// ends the running snapshot. The caller starts a new one.
    pub(crate) fn capture_snapshot_databases(&mut self, _databases: &[u8]) {
        self.abort_snapshot();
    }

    /// True when `move_key(key, target)` would move the key.
    pub(crate) fn can_move_key(&mut self, key: &[u8], target: u8) -> bool {
        let source = self.active;

        self.remove_expired(key);

        if !self.contains_key(key) {
            return false;
        }

        self.enter(target);
        self.remove_expired(key);
        let free = !self.contains_key(key);

        self.enter(source);
        free
    }

    /// Walks up to `max_keys` more keys of the snapshot plan, handing each snapshot-time
    /// entry to `emit`. Keys the journal already settled are skipped.
    pub(crate) fn snapshot_step(
        &mut self,
        max_keys: usize,
        emit: &mut dyn FnMut(WalkedEntry<'_>),
    ) -> SnapshotStep {
        let Some(mut session) = self.snapshot.take() else {
            return SnapshotStep::Done;
        };

        let mut budget = max_keys.max(1);
        let mut waiting = false;

        while budget > 0 && !session.done() {
            let database = session.plan[session.position];
            let Some(map) = self.map_of(database) else {
                // The database vanished under the walk; nothing at the snapshot point
                // can be recovered from it.
                session.position += 1;
                session.cursor = 0;
                continue;
            };

            if map.is_growing() {
                waiting = true;
                break;
            }

            let has_log = session.walks[&database].log_len.is_some();
            let bound = session.walks[&database].log_len;
            let (slots, keys) = if has_log {
                (budget.saturating_mul(10), budget)
            } else {
                (usize::MAX, usize::MAX)
            };
            let mut walked: Vec<u32> = Vec::new();
            let mut visited = 0usize;
            let next = {
                let journal = session.captured_in(database);
                let written = &session.walks[&database].written;

                map.scan_slots(session.cursor, keys, slots, &mut |key, hit, address| {
                    visited += 1;

                    if journal.is_some_and(|keys| keys.contains_key(key)) {
                        return;
                    }

                    if let Some(address) = address {
                        let newer = bound.is_some_and(|end| address >= end && address < LOG_LARGE);

                        if newer || written.contains(address) {
                            return;
                        }
                    }

                    // An expired key is settled without being written.
                    if !self.snapshot_skips(database, key) {
                        emit(WalkedEntry {
                            database,
                            key,
                            expires_at: self.deadline(database, key),
                            hit,
                            log: &self.value_log,
                        });
                    }

                    walked.extend(address);
                })
            };

            budget = budget.saturating_sub(visited.max(1));

            let walk = session.walks.get_mut(&database).expect("planned database");

            for address in walked {
                walk.written.insert(address);
            }

            session.cursor = next;

            if next == 0 {
                session.position += 1;
            }
        }

        let step = if session.done() {
            SnapshotStep::Done
        } else if waiting {
            SnapshotStep::Waiting
        } else {
            SnapshotStep::More
        };

        self.snapshot = Some(session);
        step
    }

    /// Ends a finished walk and returns the journal copies that were never written: keys
    /// that changed or disappeared before the walk reached them.
    pub(crate) fn finish_snapshot_journal(&mut self) -> Vec<SnapshotEntry> {
        let session = self
            .snapshot
            .take()
            .expect("finish_snapshot_journal requires an active snapshot");

        debug_assert!(session.done(), "snapshot walk is not finished");
        self.memory_usage.snapshot_journal_bytes = 0;

        session
            .captured
            .into_iter()
            .flatten()
            .filter_map(|(_, captured)| match captured {
                Captured::Entry(entry) => Some(entry),
                _ => None,
            })
            .collect()
    }

    /// Runs the whole snapshot at once and returns it sorted. This is the stop-the-world
    /// form used at shutdown and by tools.
    #[cfg(test)]
    pub(crate) fn finish_snapshot(&mut self) -> Vec<SnapshotEntry> {
        let mut entries = Vec::new();

        loop {
            match self.snapshot_step(usize::MAX / 20, &mut |entry| entries.push(entry.to_entry())) {
                SnapshotStep::Done => break,
                SnapshotStep::More => {}
                SnapshotStep::Waiting => {
                    for map in std::iter::once(&mut self.entries)
                        .chain(self.databases.iter_mut().flatten())
                    {
                        map.migrate_step(usize::MAX);
                    }
                }
            }
        }

        entries.extend(self.finish_snapshot_journal());
        sort_snapshot_entries(&mut entries);
        entries
    }

    pub(crate) fn restore_snapshot_entry(
        &mut self,
        snapshot: SnapshotEntry,
    ) -> Result<(), &'static str> {
        let previous = self.active;

        self.enter(snapshot.database);
        let restored = self.restore_active_entry(snapshot);

        self.enter(previous);
        restored
    }

    pub(super) fn restore_active_entry(
        &mut self,
        snapshot: SnapshotEntry,
    ) -> Result<(), &'static str> {
        if snapshot
            .expires_at
            .is_some_and(|deadline| deadline <= self.now_unix_millis())
        {
            return Ok(());
        }

        if self.entries.lookup(&snapshot.key).is_some() {
            return Err("duplicate snapshot key");
        }

        let limits = self.compact_limits();
        let value = match snapshot.value {
            SnapshotValue::Bytes(value) => Value::Bytes(self.alloc_bytes(&value)),
            SnapshotValue::Json(value) => match crate::json::TreeDocument::from_bytes(&value) {
                Some(tree) => Value::JsonTree(Box::new(tree)),
                None => Value::Json(Box::new(value)),
            },
            SnapshotValue::List(values) => Value::List(Box::new(ListValue::from_values(
                values,
                self.collection_limits,
            ))),

            SnapshotValue::Set(values) => Value::Set(Box::new(SetValue::from_members(
                values,
                self.collection_limits.set_max_intset_entries,
            ))),
            SnapshotValue::Hash(pairs) => {
                Value::Hash(Box::new(HashValue::from_pairs(pairs, limits)))
            }

            // Expiry reads the store's time, which replay pauses: the log after the
            // checkpoint may still touch a field that has expired by the wall clock since.
            SnapshotValue::HashTtl(fields) => {
                let now = self.now_unix_millis().get();
                let mut hash = Value::Hash(Box::default());

                for (field, value, at) in fields {
                    if at != 0 && at <= now {
                        continue;
                    }

                    hash.hash_insert(&field, &value, limits);

                    if at != 0 {
                        hash.set_field_deadline(&field, at);
                    }
                }

                if hash.as_hash().is_none_or(HashValue::is_empty) {
                    return Ok(());
                }

                hash
            }

            SnapshotValue::SortedSet(values) => {
                let mut set = SortedSet::default();

                for (score, member) in values {
                    set.insert(score, &member)
                        .map_err(|_| "invalid sorted-set score in snapshot")?;
                }

                Value::SortedSet(Box::new(set))
            }

            SnapshotValue::BloomFilter { words, hash_count } => Value::BloomFilter(Box::new(
                BloomFilter::from_legacy(words, hash_count)
                    .map_err(|_| "invalid Bloom filter in snapshot")?,
            )),
            SnapshotValue::BloomLayers {
                layers,
                error_rate,
                expansion,
            } => Value::BloomFilter(Box::new(
                BloomFilter::from_layers(layers, error_rate, expansion)
                    .map_err(|_| "invalid Bloom filter in snapshot")?,
            )),

            // A snapshot from before leases had their own deadline: a lease that has run out is
            // dropped, and a live one gets its deadline below.
            SnapshotValue::Lease { token, expires_at } => {
                self.note_fence_token(token);

                if expires_at <= self.now_unix_millis().get() {
                    return Ok(());
                }

                Value::Lease(Box::new(LeaseValue { token, expires_at }))
            }

            SnapshotValue::Limit { used } => Value::Limit(Box::new(LimitValue { used })),
            SnapshotValue::Semaphore {
                next_token,
                mut holders,
            } => {
                let now = self.now_unix_millis().get();

                self.note_fence_token(next_token.saturating_sub(1));
                holders.retain(|(_, expires_at)| *expires_at > now);

                if holders.is_empty() {
                    return Ok(());
                }

                Value::Semaphore(Box::new(SemaphoreValue::new(next_token, holders)))
            }

            SnapshotValue::Once => Value::Once,

            SnapshotValue::Versioned { version, bytes } => {
                Value::Versioned(Box::new(VersionedValue {
                    version,
                    body: self.alloc_bytes(&bytes),
                }))
            }

            SnapshotValue::Stream {
                started,
                last_millis,
                last_sequence,
                entries,
                groups,
                delayed,
            } => Value::Stream(Box::new(
                crate::stream::StreamValue::restore(
                    started,
                    last_millis,
                    last_sequence,
                    entries,
                    groups,
                    delayed,
                )
                .map_err(|_| "invalid stream entry in snapshot")?,
            )),
        };

        let key = snapshot.key;
        let before = self.entry_charge(&key);

        let expires_at = match &value {
            Value::Lease(lease) if snapshot.expires_at.is_none() => {
                Some(UnixMillis::new(lease.expires_at))
            }
            _ => snapshot.expires_at,
        };

        let timed = value.soonest_field_deadline().is_some();

        self.insert_entry(key.clone(), Entry::fresh(value));

        if let Some(expires_at) = expires_at {
            self.set_deadline(self.active, &key, expires_at);
        }

        if timed {
            self.refile_arrived_hash(&key);
        }

        self.reconcile_entry_charge(before, &key);
        Ok(())
    }
}

/// Database by database, key order inside each.
pub(super) fn sort_snapshot_entries(entries: &mut [SnapshotEntry]) {
    entries.sort_by(|left, right| {
        left.database
            .cmp(&right.database)
            .then_with(|| left.key.cmp(&right.key))
    });
}

// Live values copied into snapshot entries on this thread, for tests that check a copy
// was not made.
#[cfg(test)]
thread_local! {
    pub(super) static LIVE_SNAPSHOT_COPIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Live values encoded straight into a snapshot frame on this thread.
    pub(super) static LIVE_SNAPSHOT_ENCODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn live_snapshot_entry(
    database: u8,
    key: &[u8],
    hit: IndexRef<'_>,
    expires_at: Option<UnixMillis>,
    log: &ValueLog,
) -> SnapshotEntry {
    #[cfg(test)]
    LIVE_SNAPSHOT_COPIES.with(|copies| copies.set(copies.get() + 1));
    let value = match hit {
        IndexRef::Log { offset, len } => {
            SnapshotValue::Bytes(log.slice(offset, plain_len(len)).to_vec())
        }

        IndexRef::Integer(value) => SnapshotValue::Bytes(integer_bytes(value)),
        IndexRef::Heap(bytes) => SnapshotValue::Bytes(bytes.to_vec()),
        // Members and fields go out in storage order: loading rebuilds the collection by
        // inserting each one, so sorting them here would only cost time on the shard.
        IndexRef::SetSlab(bytes) => SnapshotValue::Set(
            SetMembers {
                bytes,
                at: 2,
                left: flat_count(bytes),
            }
            .map(<[u8]>::to_vec)
            .collect(),
        ),

        IndexRef::CollSlab(bytes) => {
            let entry = Entry::from_slab_coll(bytes);

            return live_snapshot_entry(database, key, IndexRef::Object(&entry), expires_at, log);
        }

        IndexRef::SortedSlab(bytes) => SnapshotValue::SortedSet(
            SortedEntries::new(bytes)
                .map(|(score, member)| (score.0, member.to_vec()))
                .collect(),
        ),
        IndexRef::Object(entry) => match &entry.value {
            Value::Bytes(value) => SnapshotValue::Bytes(value.view(log).to_vec()),
            Value::Json(value) => SnapshotValue::Json(value.to_vec()),
            Value::JsonTree(value) => SnapshotValue::Json(value.to_vec()),
            Value::Lease(lease) => SnapshotValue::Lease {
                token: lease.token,
                expires_at: lease.expires_at,
            },
            Value::Limit(limit) => SnapshotValue::Limit { used: limit.used },
            Value::Semaphore(semaphore) => SnapshotValue::Semaphore {
                next_token: semaphore.next_token(),
                holders: semaphore.holders().collect(),
            },
            Value::Once => SnapshotValue::Once,
            Value::Versioned(value) => SnapshotValue::Versioned {
                version: value.version,
                bytes: value.body.view(log).to_vec(),
            },
            Value::List(values) => {
                SnapshotValue::List(values.iter().map(|value| value.to_vec()).collect())
            }

            Value::Set(values) => {
                SnapshotValue::Set(values.iter().map(|value| value.to_vec()).collect())
            }

            Value::Hash(hash) => {
                let mut pairs = Vec::with_capacity(hash.len());

                hash.visit_pairs(|(field, value)| pairs.push((field.to_vec(), value.to_vec())));
                SnapshotValue::Hash(pairs)
            }
            Value::TimedHash(timed) => SnapshotValue::HashTtl(timed.pairs()),
            Value::SortedSet(values) => SnapshotValue::SortedSet(
                values
                    .range_entries(0, -1, false)
                    .into_iter()
                    .map(|(member, score)| (score, member.to_vec()))
                    .collect(),
            ),

            Value::BloomFilter(filter) => SnapshotValue::BloomLayers {
                layers: filter.layers().to_vec(),
                error_rate: filter.error_rate(),
                expansion: filter.expansion(),
            },
            Value::Stream(stream) => {
                let (started, last_millis, last_sequence) = stream.high_water();

                SnapshotValue::Stream {
                    started,
                    last_millis,
                    last_sequence,
                    entries: stream.entries().to_vec(),
                    groups: stream.groups().to_vec(),
                    delayed: stream.holds().to_vec(),
                }
            }
        },
    };

    SnapshotEntry {
        database,
        key: key.to_vec(),
        expires_at,
        value,
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Appends `key`'s snapshot frame from every database that holds it (and has not
    /// expired), encoded straight from the live values as a checkpoint writes them. Returns
    /// how many frames it wrote; on error `output` keeps only what came before.
    pub(crate) fn encode_key_frames(
        &self,
        key: &[u8],
        output: &mut Vec<u8>,
    ) -> Result<u32, crate::PersistenceError> {
        let start = output.len();
        let mut frames = 0;

        for (database, map) in self.database_maps() {
            let Some(hit) = map.lookup(key) else {
                continue;
            };

            if self.snapshot_skips(database, key) {
                continue;
            }

            let walked = WalkedEntry {
                database,
                key,
                expires_at: self.deadline(database, key),
                hit,
                log: &self.value_log,
            };

            if let Err(error) = walked.encode_into(output) {
                output.truncate(start);
                return Err(error);
            }
            frames += 1;
        }

        Ok(frames)
    }
}

/// A key the snapshot walk reached, still borrowed from the shard: the checkpoint encodes
/// it straight from the live value instead of copying it into a [`SnapshotEntry`] first.
pub(crate) struct WalkedEntry<'a> {
    database: u8,
    key: &'a [u8],
    expires_at: Option<UnixMillis>,
    hit: IndexRef<'a>,
    log: &'a ValueLog,
}

impl WalkedEntry<'_> {
    /// An owned copy of the entry, as the journal keeps one.
    pub(crate) fn to_entry(&self) -> SnapshotEntry {
        live_snapshot_entry(self.database, self.key, self.hit, self.expires_at, self.log)
    }

    /// Appends the entry's snapshot frame, the same bytes `encode_entry(&self.to_entry())`
    /// writes. Large values are written once, into space reserved for their exact size.
    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), crate::PersistenceError> {
        let (size, kind, items) = match self.hit {
            // A sorted set knows its size: skip the sizing walk over its members.
            IndexRef::Object(Entry {
                value: Value::SortedSet(set),
            }) => {
                let card = set.card();

                (
                    card.saturating_mul(12)
                        .saturating_add(set.payload_bytes)
                        .saturating_add(4),
                    4,
                    card,
                )
            }

            _ => {
                let mut size = PayloadSize(0);
                let Some((kind, items)) = live_payload(&mut size, self.hit, self.log, 0)? else {
                    return crate::persistence::encode_entry(output, &self.to_entry());
                };

                (size.0, kind, items)
            }
        };

        crate::persistence::encode_entry_with(
            output,
            self.database,
            self.key,
            self.expires_at,
            size,
            |payload| {
                #[cfg(test)]
                LIVE_SNAPSHOT_ENCODES.with(|encodes| encodes.set(encodes.get() + 1));
                match live_payload(payload, self.hit, self.log, items)? {
                    Some((_, written)) if written == items => Ok(kind),
                    _ => Err(crate::PersistenceError::Failed(
                        "snapshot value changed while it was encoded".into(),
                    )),
                }
            },
        )
    }
}

/// Where [`live_payload`] writes: the frame itself, or a byte count to size it first.
trait PayloadOut {
    fn put(&mut self, bytes: &[u8]);

    fn put_u32(&mut self, value: u32) {
        self.put(&value.to_le_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.put(&value.to_le_bytes());
    }

    fn put_bytes(&mut self, value: &[u8]) -> Result<(), crate::PersistenceError> {
        let length = u32::try_from(value.len()).map_err(|_| {
            crate::PersistenceError::Failed("snapshot byte string too large".into())
        })?;

        self.put_u32(length);
        self.put(value);
        Ok(())
    }
}

impl PayloadOut for Vec<u8> {
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

struct PayloadSize(usize);

impl PayloadOut for PayloadSize {
    fn put(&mut self, bytes: &[u8]) {
        self.0 = self.0.saturating_add(bytes.len());
    }
}

fn item_count(items: usize, what: &str) -> Result<u32, crate::PersistenceError> {
    u32::try_from(items).map_err(|_| crate::PersistenceError::Failed(format!("too many {what}")))
}

/// Counts the items [`live_payload`] writes and keeps the first error.
struct Items<'o, O> {
    out: &'o mut O,
    count: usize,
    failed: Result<(), crate::PersistenceError>,
}

impl<O: PayloadOut> Items<'_, O> {
    fn bytes(&mut self, value: &[u8]) {
        if self.failed.is_ok() {
            self.failed = self.out.put_bytes(value);
        }
    }

    fn item(&mut self, value: &[u8]) {
        self.count += 1;
        self.bytes(value);
    }

    fn scored(&mut self, score: f64, value: &[u8]) {
        self.out.put_u64(score.to_bits());
        self.item(value);
    }
}

/// Writes the snapshot payload of strings and large collections straight from the live
/// value, in the order [`live_snapshot_entry`] copies them. `count` is the item count a
/// sizing pass returned (anything on that pass). Returns the value type and item count, or
/// `None` for the rarer types, which go through a [`SnapshotEntry`] copy.
fn live_payload<O: PayloadOut>(
    out: &mut O,
    hit: IndexRef<'_>,
    log: &ValueLog,
    count: usize,
) -> Result<Option<(u8, usize)>, crate::PersistenceError> {
    let string = |out: &mut O, view: ByteView<'_>| match view {
        ByteView::Borrowed(bytes) => out.put(bytes),
        ByteView::Integer(value) => out.put(&integer_bytes(value)),
    };
    let mut items = Items {
        out,
        count: 0,
        failed: Ok(()),
    };
    let kind = match hit {
        IndexRef::Log { offset, len } => {
            items.out.put(log.slice(offset, plain_len(len)));
            1
        }

        IndexRef::Integer(value) => {
            string(items.out, ByteView::Integer(value));
            1
        }

        IndexRef::Heap(bytes) => {
            items.out.put(bytes);
            1
        }

        IndexRef::SetSlab(bytes) => {
            items
                .out
                .put_u32(item_count(count, "snapshot collection items")?);
            SetMembers {
                bytes,
                at: 2,
                left: flat_count(bytes),
            }
            .for_each(|value| items.item(value));
            3
        }

        IndexRef::SortedSlab(bytes) => {
            items.out.put_u32(item_count(count, "sorted-set members")?);
            SortedEntries::new(bytes).for_each(|(score, value)| items.scored(score.0, value));
            4
        }

        IndexRef::Object(entry) => match &entry.value {
            Value::Bytes(value) => {
                string(items.out, value.view(log));
                1
            }

            Value::Json(value) => {
                items.out.put(value);
                15
            }

            Value::List(values) => {
                items
                    .out
                    .put_u32(item_count(count, "snapshot collection items")?);
                values.iter().for_each(|value| items.item(value));
                2
            }

            Value::Set(values) => {
                items
                    .out
                    .put_u32(item_count(count, "snapshot collection items")?);
                values.iter().for_each(|value| items.item(&value));
                3
            }

            Value::Hash(hash) => {
                items.out.put_u32(item_count(count, "hash fields")?);
                hash.visit_pairs(|(field, value)| {
                    items.item(field);
                    items.bytes(value);
                });
                6
            }

            Value::SortedSet(values) => {
                items.out.put_u32(item_count(count, "sorted-set members")?);
                values.for_each_entry(|value, score| items.scored(score, value));
                4
            }

            Value::Versioned(value) => {
                items.out.put_u64(value.version);
                string(items.out, value.body.view(log));
                9
            }

            _ => return Ok(None),
        },

        IndexRef::CollSlab(_) => return Ok(None),
    };

    items.failed?;
    Ok(Some((kind, items.count)))
}

pub(super) fn snapshot_payload_bytes(entry: &SnapshotEntry) -> usize {
    match &entry.value {
        SnapshotValue::Bytes(value) | SnapshotValue::Json(value) => value.len(),
        SnapshotValue::List(values) | SnapshotValue::Set(values) => {
            values.iter().map(Vec::len).sum()
        }

        SnapshotValue::Hash(pairs) => pairs
            .iter()
            .map(|(field, value)| field.len().saturating_add(value.len()))
            .sum(),
        SnapshotValue::HashTtl(fields) => fields
            .iter()
            .map(|(field, value, _)| field.len().saturating_add(value.len()).saturating_add(8))
            .sum(),
        SnapshotValue::SortedSet(values) => values
            .iter()
            .map(|(_, member)| member.len().saturating_add(8))
            .sum(),
        SnapshotValue::BloomFilter { words, .. } => words.len().saturating_mul(size_of::<u64>()),
        SnapshotValue::BloomLayers { layers, .. } => layers
            .iter()
            .map(|layer| layer.words.len().saturating_mul(size_of::<u64>()))
            .sum(),
        SnapshotValue::Lease { .. } => 16,
        SnapshotValue::Limit { .. } => 8,
        SnapshotValue::Semaphore { holders, .. } => {
            holders.len().saturating_mul(16).saturating_add(8)
        }

        SnapshotValue::Once => 0,
        SnapshotValue::Versioned { bytes, .. } => bytes.len().saturating_add(8),
        SnapshotValue::Stream {
            entries, groups, ..
        } => {
            let entry_bytes = entries.iter().fold(0usize, |total, entry| {
                total.saturating_add(entry.fields.iter().fold(0, |fields, (field, value)| {
                    fields
                        .saturating_add(field.len())
                        .saturating_add(value.len())
                }))
            });

            let group_bytes = groups.iter().fold(0usize, |total, group| {
                total.saturating_add(group.name.len()).saturating_add(
                    group.pending.iter().fold(0, |pending, delivery| {
                        pending.saturating_add(delivery.consumer.len())
                    }),
                )
            });

            entry_bytes.saturating_add(group_bytes)
        }
    }
}
