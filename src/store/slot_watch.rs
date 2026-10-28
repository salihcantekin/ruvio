//! What a slot move needs from the source shard's store: the keys of the range written
//! while it is copied, each key's value as a snapshot entry, and removing the range once
//! it has moved.

use super::*;
use crate::routing::hash_slot;

/// Keys of the slots `first..=last` that changed since the last [`SlotWatch::take`].
#[derive(Debug)]
pub(crate) struct SlotWatch {
    first: u16,
    last: u16,
    changed: AHashSet<Box<[u8]>>,
    /// A flush or `SWAPDB` changed keys without naming them.
    rewritten: bool,
}

impl SlotWatch {
    pub(super) fn note(&mut self, key: &[u8]) {
        let slot = hash_slot(key);

        if (self.first..=self.last).contains(&slot) && !self.changed.contains(key) {
            self.changed.insert(key.into());
        }
    }

    pub(super) fn note_rewrite(&mut self) {
        self.rewritten = true;
    }
}

/// Changes to a watched range since the last look.
#[derive(Debug, Default)]
pub(crate) struct SlotChanges {
    pub keys: Vec<Box<[u8]>>,
    pub rewritten: bool,
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Starts noting every key of `first..=last` that a write, delete or expiry changes.
    pub(crate) fn watch_slots(&mut self, first: u16, last: u16) {
        self.slot_watch = Some(Box::new(SlotWatch {
            first,
            last,
            changed: AHashSet::new(),
            rewritten: false,
        }));
        self.refresh_observing();
    }

    /// The keys noted since the last call, in no order.
    pub(crate) fn take_slot_changes(&mut self) -> SlotChanges {
        match self.slot_watch.as_mut() {
            Some(watch) => SlotChanges {
                keys: watch.changed.drain().collect(),
                rewritten: std::mem::take(&mut watch.rewritten),
            },
            None => SlotChanges::default(),
        }
    }

    pub(crate) fn unwatch_slots(&mut self) {
        self.slot_watch = None;
        self.refresh_observing();
    }

    /// Marks dirty every watch on a key of `first..=last` and wakes the commands retried on
    /// one: the keys now live on another shard.
    pub(crate) fn dirty_watches_in_slots(&mut self, first: u16, last: u16) {
        let Some(databases) = self.watchers_by_key.as_ref() else {
            return;
        };
        let conns: Vec<u64> = databases
            .values()
            .flat_map(|keys| keys.iter())
            .filter(|(key, _)| (first..=last).contains(&hash_slot(key)))
            .flat_map(|(_, conns)| conns.iter().copied())
            .collect();

        for conn in conns {
            self.dirty_watches.insert(conn);
            if let Some(waker) = self.watch_wakers.remove(&conn) {
                waker.wake();
            }
        }
    }

    /// Up to `budget` positions of `database`'s index from `cursor`, handing every key of a
    /// slot in `first..=last` to `visit`. Returns the next cursor, 0 at the end. Keys present
    /// for the whole walk are visited at least once, whatever is written meanwhile.
    pub(crate) fn walk_slots(
        &self,
        database: u8,
        first: u16,
        last: u16,
        cursor: u64,
        budget: usize,
        visit: &mut dyn FnMut(&[u8]),
    ) -> u64 {
        let Some(map) = self.map_of(database) else {
            return 0;
        };

        map.scan_slots(
            cursor,
            budget,
            budget.saturating_mul(SCAN_SLOT_FACTOR),
            &mut |key, _, _| {
                if (first..=last).contains(&hash_slot(key)) {
                    visit(key);
                }
            },
        )
    }

    /// Whether `database` holds `key`, expired or not.
    pub(crate) fn holds_key_in(&self, database: u8, key: &[u8]) -> bool {
        self.map_of(database)
            .is_some_and(|map| map.lookup(key).is_some())
    }

    /// Removes `key` from `database` without logging it, notifying anyone or counting it as
    /// expired: the key now lives on another shard.
    pub(crate) fn forget_key(&mut self, database: u8, key: &[u8]) -> bool {
        let active = self.active;

        self.enter(database);
        let before = self.entry_charge(key);
        let forgotten = match self.remove_entry(key) {
            Some(entry) => {
                self.free_entry(key, entry);
                self.reconcile_entry_charge(before, key);
                true
            }
            None => false,
        };

        self.enter(active);
        forgotten
    }
}
