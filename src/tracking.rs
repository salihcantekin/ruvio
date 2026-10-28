//! Shard-local client cache tracking. The hot parser does not consult this map.
//! A connection records `GET` and `MGET` keys only while `CLIENT TRACKING` is on.
//! A completed write of one of those keys pushes one RESP3 `invalidate` and drops it.
//! Keys are [`crate::database::tagged`] with their database, so equal user keys in two
//! databases are tracked apart.

use std::{collections::VecDeque, sync::Arc};

use ahash::{AHashMap, AHashSet};

use crate::protocol::Command;

/// Most keys one shard tracks. Past it the oldest tracked key is invalidated and dropped.
pub const MAX_TRACKED_KEYS: usize = 1_000_000;

/// Both maps share one copy of each key name. `Arc`, not `Rc`, so a flushed table can be
/// freed on the lazy-free thread.
#[derive(Debug)]
pub struct KeyTracker {
    by_conn: AHashMap<u64, AHashSet<Arc<[u8]>>>,
    by_key: AHashMap<Arc<[u8]>, Readers>,
    /// Tracked keys, oldest first. An entry is live only while it is the very `Arc` that
    /// `by_key` holds; invalidated keys stay behind until they reach the front.
    order: VecDeque<Arc<[u8]>>,
    max_keys: usize,
}

/// Connections tracking one key. Most keys have a single reader, which is kept inline with no
/// allocation; a few readers sit in a small vector (removal scans at most
/// [`SMALL_READERS`]); a hot key with many readers moves to a set, so a connection leaving
/// costs O(1) instead of a scan of every other reader.
#[derive(Debug)]
enum Readers {
    One(u64),
    Few(Vec<u64>),
    Many(Box<AHashSet<u64>>),
}

const SMALL_READERS: usize = 32;

impl Readers {
    fn add(&mut self, conn: u64) {
        match self {
            Readers::One(only) => {
                let mut readers = Vec::with_capacity(4);

                readers.push(*only);
                readers.push(conn);
                *self = Readers::Few(readers);
            }
            Readers::Few(readers) if readers.len() < SMALL_READERS => readers.push(conn),
            Readers::Few(readers) => {
                let mut set: AHashSet<u64> = readers.iter().copied().collect();

                set.insert(conn);
                *self = Readers::Many(Box::new(set));
            }
            Readers::Many(set) => {
                set.insert(conn);
            }
        }
    }

    /// Removes `conn`; true when no reader is left.
    fn remove(&mut self, conn: u64) -> bool {
        match self {
            Readers::One(only) => *only == conn,
            Readers::Few(readers) => {
                if let Some(at) = readers.iter().position(|id| *id == conn) {
                    readers.swap_remove(at);

                    if let [only] = readers.as_slice() {
                        *self = Readers::One(*only);
                    }
                }

                false
            }
            Readers::Many(set) => {
                set.remove(&conn);
                set.is_empty()
            }
        }
    }

    fn into_vec(self) -> Vec<u64> {
        match self {
            Readers::One(only) => vec![only],
            Readers::Few(readers) => readers,
            Readers::Many(set) => set.into_iter().collect(),
        }
    }
}

impl Default for KeyTracker {
    fn default() -> Self {
        Self::with_max_keys(MAX_TRACKED_KEYS)
    }
}

#[derive(Debug, Default)]
pub struct TrackPlan {
    pub reads: Vec<Vec<u8>>,
    pub writes: Vec<Vec<u8>>,
    /// FLUSHDB, FLUSHALL, and SWAPDB invalidate every tracked key.
    pub flush: bool,
}

impl TrackPlan {
    pub fn invalidates(&self) -> bool {
        self.flush || !self.writes.is_empty()
    }

    /// Writes of a `MULTI` batch run in `database`. Reads are not tracked inside `EXEC`.
    pub fn batch(commands: &[Command<'_>], database: u8) -> Self {
        let mut batch = TrackPlan {
            reads: Vec::new(),
            writes: Vec::new(),
            flush: false,
        };

        for command in commands {
            let plan = plan(command, database);

            batch.writes.extend(plan.writes);
            batch.flush |= plan.flush;
        }

        batch
    }

    /// Writes of a replicated record applied in `database`.
    pub fn replicated(database: u8, mutation: &crate::OwnedMutation) -> Self {
        let mut plan = TrackPlan {
            reads: Vec::new(),
            writes: Vec::new(),
            flush: false,
        };

        plan.add_replicated(database, mutation);
        plan
    }

    fn add_replicated(&mut self, database: u8, mutation: &crate::OwnedMutation) {
        match mutation {
            crate::OwnedMutation::Transaction { mutations } => {
                for mutation in mutations {
                    self.add_replicated(database, mutation);
                }
            }

            crate::OwnedMutation::FlushDatabase
            | crate::OwnedMutation::FlushAll
            | crate::OwnedMutation::SwapDatabases { .. } => self.flush = true,
            crate::OwnedMutation::MoveKey { key, destination } => {
                self.writes.push(crate::database::tagged(database, key));
                self.writes.push(crate::database::tagged(*destination, key));
            }

            other => self.writes.extend(
                other
                    .write_keys()
                    .iter()
                    .map(|key| crate::database::tagged(database, key)),
            ),
        }
    }
}

impl KeyTracker {
    pub fn with_max_keys(max_keys: usize) -> Self {
        Self {
            by_conn: AHashMap::new(),
            by_key: AHashMap::new(),
            order: VecDeque::new(),
            max_keys: max_keys.max(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Records `keys` as read by `conn`. Returns the invalidations of the oldest keys this
    /// pushed past the table limit.
    pub fn track(&mut self, conn: u64, keys: &[Vec<u8>]) -> Vec<(u64, Vec<Vec<u8>>)> {
        for key in keys {
            let seen = self.by_conn.entry(conn).or_default();

            if seen.contains(key.as_slice()) {
                continue;
            }

            let shared = match self.by_key.get_key_value(key.as_slice()) {
                Some((shared, _)) => shared.clone(),
                None => {
                    let shared: Arc<[u8]> = Arc::from(key.as_slice());

                    self.order.push_back(shared.clone());
                    shared
                }
            };

            seen.insert(shared.clone());
            match self.by_key.entry(shared) {
                std::collections::hash_map::Entry::Occupied(mut readers) => {
                    readers.get_mut().add(conn);
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(Readers::One(conn));
                }
            }
        }

        let mut evicted = Vec::new();

        while self.by_key.len() > self.max_keys {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };

            if self.is_live(&oldest) {
                evicted.extend(self.invalidate(&[oldest.to_vec()]));
            }
        }

        // Stale entries are dropped once they outnumber the live ones, so `order` stays
        // within twice the table.
        if self.order.len() > self.by_key.len().saturating_mul(2).saturating_add(1_024) {
            let by_key = &self.by_key;

            self.order.retain(|key| {
                by_key
                    .get_key_value(&**key)
                    .is_some_and(|(live, _)| Arc::ptr_eq(live, key))
            });
        }

        evicted
    }

    fn is_live(&self, key: &Arc<[u8]>) -> bool {
        self.by_key
            .get_key_value(&**key)
            .is_some_and(|(live, _)| Arc::ptr_eq(live, key))
    }

    pub fn forget(&mut self, conn: u64) {
        let Some(keys) = self.by_conn.remove(&conn) else {
            return;
        };

        for key in keys {
            let Some(conns) = self.by_key.get_mut(&*key) else {
                continue;
            };

            if conns.remove(conn) {
                self.by_key.remove(&*key);
            }
        }
    }

    /// Tracked keys (tagged with their database) that `select` picks.
    pub fn tracked_where(&self, select: impl Fn(&[u8]) -> bool) -> Vec<Vec<u8>> {
        self.by_key
            .keys()
            .filter(|key| select(crate::database::untagged(key).1))
            .map(|key| key.to_vec())
            .collect()
    }

    /// Drops the tracked keys and returns one key list per connection.
    pub fn invalidate(&mut self, keys: &[Vec<u8>]) -> Vec<(u64, Vec<Vec<u8>>)> {
        let mut grouped: AHashMap<u64, Vec<Vec<u8>>> = AHashMap::new();

        for key in keys {
            let Some(conns) = self.by_key.remove(key.as_slice()) else {
                continue;
            };

            for conn in conns.into_vec() {
                let empty = if let Some(seen) = self.by_conn.get_mut(&conn) {
                    seen.remove(key.as_slice());
                    seen.is_empty()
                } else {
                    false
                };

                if empty {
                    self.by_conn.remove(&conn);
                }

                grouped.entry(conn).or_default().push(key.clone());
            }
        }

        grouped.into_iter().collect()
    }

    /// Invalidations for `plan`. A flush answers every tracking connection with one
    /// invalidation that names no key, which tells the client to drop its whole cache, and
    /// frees the table off the shard thread.
    pub fn invalidate_plan(&mut self, plan: &TrackPlan) -> Vec<(u64, Vec<Vec<u8>>)> {
        if plan.flush {
            let conns: Vec<(u64, Vec<Vec<u8>>)> = self
                .by_conn
                .keys()
                .map(|conn| (*conn, Vec::new()))
                .collect();
            let bytes = self.by_key.len().saturating_mul(96);
            let table = (
                std::mem::take(&mut self.by_conn),
                std::mem::take(&mut self.by_key),
                std::mem::take(&mut self.order),
            );

            crate::lazy_free::defer_drop(table, bytes);
            return conns;
        }

        self.invalidate(&plan.writes)
    }
}

/// Tracking effect of `command` run in `database`.
pub fn plan(command: &Command<'_>, database: u8) -> TrackPlan {
    let mut keys = crate::routing::command_keys(command)
        .into_iter()
        .map(|key| crate::database::tagged(database, key))
        .collect::<Vec<_>>();

    if let Command::Move { key, db } = command
        && let Ok(target) = u8::try_from(*db)
    {
        keys.push(crate::database::tagged(target, key));
    }

    if crate::protocol::command_mutates_store(command) {
        return TrackPlan {
            reads: Vec::new(),
            writes: keys,
            flush: matches!(
                command,
                Command::FlushDb | Command::FlushAll | Command::SwapDb(_, _)
            ),
        };
    }

    if matches!(command, Command::Get(_) | Command::MGet(_)) {
        return TrackPlan {
            reads: keys,
            writes: Vec::new(),
            flush: false,
        };
    }

    TrackPlan {
        reads: Vec::new(),
        writes: Vec::new(),
        flush: false,
    }
}

/// The RESP3 `invalidate` push. An empty `keys` is a flush and encodes as null.
pub fn encode_invalidate(keys: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b">2\r\n");
    write_bulk(&mut out, b"invalidate");

    if keys.is_empty() {
        // A flush: a null list tells the client to drop every cached key.
        out.extend_from_slice(b"_\r\n");

        return out;
    }

    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());

    for key in keys {
        write_bulk(&mut out, crate::database::untagged(key).1);
    }

    out
}

pub fn response_applied(response: &crate::engine::Response<'_>) -> bool {
    !matches!(
        response,
        crate::engine::Response::Error(_)
            | crate::engine::Response::OwnedError(_)
            | crate::engine::Response::NullArray
            | crate::engine::Response::BlockList { .. }
            | crate::engine::Response::BlockStream { .. }
            | crate::engine::Response::BlockGroup { .. }
            | crate::engine::Response::BlockRetry { .. }
    )
}

fn write_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::{KeyTracker, Readers, SMALL_READERS, encode_invalidate};

    #[test]
    fn invalidate_push_names_the_key_once_and_forgets_it() {
        let mut tracker = KeyTracker::default();

        tracker.track(1, &[b"a".to_vec(), b"b".to_vec()]);
        tracker.track(2, &[b"a".to_vec()]);

        let mut found = tracker.invalidate(&[b"a".to_vec()]);

        found.sort_by_key(|(conn, _)| *conn);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].1, vec![b"a".to_vec()]);
        assert!(tracker.invalidate(&[b"a".to_vec()]).is_empty());
        assert!(!tracker.is_empty());
        tracker.forget(1);
        assert!(tracker.is_empty());

        let frame = encode_invalidate(&[crate::database::tagged(0, b"a")]);

        assert!(frame.starts_with(b">2\r\n"));
        assert!(frame.windows(10).any(|window| window == b"invalidate"));
    }

    #[test]
    fn flush_plans_invalidate_every_tracked_key() {
        let mut tracker = KeyTracker::default();

        tracker.track(1, &[b"a".to_vec(), b"b".to_vec()]);
        let plan = super::plan(&crate::protocol::Command::FlushDb, 0);

        assert!(plan.invalidates());
        let found = tracker.invalidate_plan(&plan);

        // One invalidation per connection naming no key: a null list, as Redis sends.
        assert_eq!(found, vec![(1, Vec::new())]);
        assert!(tracker.is_empty());
        assert_eq!(
            encode_invalidate(&found[0].1),
            b">2\r\n$10\r\ninvalidate\r\n_\r\n".to_vec()
        );
    }

    #[test]
    fn a_full_table_invalidates_its_oldest_keys() {
        let mut tracker = KeyTracker::with_max_keys(3);

        assert!(tracker.track(1, &[b"a".to_vec(), b"b".to_vec()]).is_empty());
        assert!(tracker.track(2, &[b"a".to_vec(), b"c".to_vec()]).is_empty());

        let mut evicted = tracker.track(2, &[b"d".to_vec()]);

        evicted.sort();
        assert_eq!(
            evicted,
            vec![(1, vec![b"a".to_vec()]), (2, vec![b"a".to_vec()])]
        );
        assert_eq!(tracker.by_key.len(), 3);

        // A key invalidated by a write and read again is the newest, not the oldest.
        assert_eq!(tracker.invalidate(&[b"b".to_vec()]).len(), 1);
        assert!(tracker.track(1, &[b"b".to_vec()]).is_empty());
        assert_eq!(
            tracker.track(1, &[b"e".to_vec()]),
            vec![(2, vec![b"c".to_vec()])]
        );

        // Churn on one key leaves the order queue bounded.
        for _ in 0..10_000 {
            tracker.invalidate(&[b"e".to_vec()]);
            tracker.track(1, &[b"e".to_vec()]);
        }

        assert!(tracker.order.len() <= 2 * tracker.by_key.len() + 1_025);
    }

    #[test]
    fn readers_grow_through_every_form_and_shrink_back_as_connections_leave() {
        let key = vec![b"hot".to_vec()];
        let mut tracker = KeyTracker::default();
        let count = SMALL_READERS as u64 + 5;

        for conn in 0..count {
            tracker.track(conn, &key);
        }

        assert!(matches!(
            tracker.by_key.get(b"hot".as_slice()),
            Some(Readers::Many(_))
        ));

        // Leave in an order that is neither first-in nor last-in.
        for conn in (0..count).step_by(2).chain((1..count).step_by(2)) {
            tracker.forget(conn);
        }

        assert!(tracker.is_empty());
        assert!(tracker.by_conn.is_empty());

        tracker.track(1, &key);
        tracker.track(2, &key);
        tracker.forget(1);
        assert!(matches!(
            tracker.by_key.get(b"hot".as_slice()),
            Some(Readers::One(2))
        ));

        let mut invalidated = tracker.invalidate(&key);

        invalidated.sort();
        assert_eq!(invalidated, vec![(2, key)]);
        assert!(tracker.is_empty());
    }

    #[test]
    fn invalidating_a_hot_key_names_every_reader_once() {
        let key = vec![b"hot".to_vec()];
        let mut tracker = KeyTracker::default();

        for conn in 0..100 {
            tracker.track(conn, &key);
        }

        let mut readers: Vec<u64> = tracker
            .invalidate(&key)
            .into_iter()
            .map(|(conn, _)| conn)
            .collect();

        readers.sort_unstable();
        assert_eq!(readers, (0..100).collect::<Vec<_>>());
    }

    fn resident_bytes() -> usize {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
        let pages: usize = statm.split_whitespace().nth(1).unwrap().parse().unwrap();

        pages * 4096
    }

    /// Prints the table's memory per tracked key; run by hand with `--ignored --nocapture`.
    #[test]
    #[ignore]
    fn measure_memory_per_tracked_key() {
        let connections: u64 = std::env::var("TRACK_CONNS")
            .ok()
            .map_or(1, |v| v.parse().unwrap());

        for label in ["connections per key"] {
            let keys: Vec<Vec<u8>> = (0..1_000_000u32)
                .map(|n| format!("user:{n:09}").into_bytes())
                .collect();
            let before = resident_bytes();
            let mut tracker = KeyTracker::with_max_keys(2_000_000);

            for conn in 0..connections {
                tracker.track(conn, &keys);
            }

            let after = resident_bytes();

            println!(
                "{connections} {label}: {} bytes per tracked key (incl. key bytes)",
                (after - before) / keys.len()
            );
            drop(tracker);
        }
    }

    #[test]
    fn plans_track_equal_keys_of_two_databases_apart() {
        let get = crate::protocol::Command::Get(std::borrow::Cow::Borrowed(b"k"));
        let set = crate::protocol::Command::set(
            std::borrow::Cow::Borrowed(b"k"),
            std::borrow::Cow::Borrowed(b"v"),
        );
        let mut tracker = KeyTracker::default();

        tracker.track(1, &super::plan(&get, 3).reads);
        assert!(tracker.invalidate_plan(&super::plan(&set, 0)).is_empty());
        assert_eq!(tracker.invalidate_plan(&super::plan(&set, 3)).len(), 1);
    }

    #[test]
    fn invalidate_push_names_the_user_key_of_a_selected_database() {
        assert_eq!(
            encode_invalidate(&[crate::database::tagged(3, b"user")]),
            b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nuser\r\n".to_vec()
        );
    }
}
