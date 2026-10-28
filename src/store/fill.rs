//! Fill markers for `FILLGET` / `FILLSET`. Memory only: nothing is allocated until
//! the first `FILLGET`, and markers are never written to the WAL or a snapshot.

use super::*;

/// Most markers kept per shard; when full, lapsed fill tokens and the soft expiries of
/// values that are gone are purged first.
const MAX_MARKS: usize = 65_536;

#[derive(Clone, Copy, Debug)]
struct Mark {
    /// Token handed to the one client allowed to fill, and when it stops being valid.
    fill: Option<(u64, u64)>,
    /// For a value written with `SWR`: the end of its fresh period, and its deadline (the end
    /// of the stale window), after which the mark says nothing.
    soft: Option<(u64, u64)>,
}

#[derive(Default, Debug)]
pub(crate) struct FillTable {
    marks: AHashMap<(u8, Box<[u8]>), Mark>,
    next_token: u64,
}

/// Fill markers of one database, held by a `MULTI` flush until the batch is durable.
pub(crate) struct DetachedMarks(Vec<(MarkId, Mark)>);

type MarkId = (u8, Box<[u8]>);

pub(crate) enum FillState {
    /// The key holds a fresh value (or a stale one with nothing to say about it).
    Fresh,
    /// The value is past its soft expiry but still stored.
    Stale,
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    fn fill_table(&mut self) -> &mut FillTable {
        self.fills.get_or_insert_with(Box::default)
    }

    /// Soft-expiry state of an existing value.
    pub(crate) fn fill_state(&self, key: &[u8]) -> FillState {
        let now = self.now_unix_millis().get();
        let soft = self
            .fills
            .as_ref()
            .and_then(|table| table.marks.get(&(self.active, key.into())))
            .and_then(|mark| mark.soft);

        match soft {
            Some((soft, _)) if now >= soft => FillState::Stale,
            _ => FillState::Fresh,
        }
    }

    /// Hand out a fill token unless a live one is out. `Err(())` when the table is full.
    pub(crate) fn fill_acquire(&mut self, key: &[u8], fill_ms: u64) -> Result<Option<u64>, ()> {
        let now = self.now_unix_millis().get();
        let active = self.active;
        let table = self.fill_table();
        let id = (active, Box::<[u8]>::from(key));

        if let Some(mark) = table.marks.get(&id)
            && mark.fill.is_some_and(|(_, expires_at)| now < expires_at)
        {
            return Ok(None);
        }

        if table.marks.len() >= MAX_MARKS && !table.marks.contains_key(&id) {
            // A stale value's mark is what makes FILLGET answer STALE and hand out a refresh
            // token; dropping it while the value lives would serve the value as fresh.
            table.marks.retain(|_, mark| {
                mark.fill.is_some_and(|(_, expires_at)| now < expires_at)
                    || mark.soft.is_some_and(|(_, deadline)| now < deadline)
            });

            if table.marks.len() >= MAX_MARKS {
                return Err(());
            }
        }

        table.next_token += 1;
        let token = table.next_token;
        let mark = table.marks.entry(id).or_insert(Mark {
            fill: None,
            soft: None,
        });

        mark.fill = Some((token, now.saturating_add(fill_ms)));
        Ok(Some(token))
    }

    /// True when `token` is the live fill token of `key`.
    pub(crate) fn fill_token_valid(&self, key: &[u8], token: u64) -> bool {
        let now = self.now_unix_millis().get();

        self.fills
            .as_ref()
            .and_then(|table| table.marks.get(&(self.active, key.into())))
            .and_then(|mark| mark.fill)
            .is_some_and(|(live, expires_at)| live == token && now < expires_at)
    }

    /// Milliseconds until the pending fill of `key` lapses, when one is out.
    pub(crate) fn fill_lapses_in(&self, database: u8, key: &[u8]) -> Option<u64> {
        let now = self.now_unix_millis().get();

        self.fills
            .as_ref()
            .and_then(|table| table.marks.get(&(database, key.into())))
            .and_then(|mark| mark.fill)
            .map(|(_, expires_at)| expires_at.saturating_sub(now))
    }

    /// Consume the marker after a successful `FILLSET`; remember the soft expiry and the
    /// value's deadline, if any.
    pub(crate) fn fill_complete(&mut self, key: &[u8], soft: Option<(u64, u64)>) {
        let active = self.active;
        let table = self.fill_table();
        let id = (active, Box::<[u8]>::from(key));

        if soft.is_none() {
            table.marks.remove(&id);
        } else {
            table.marks.insert(id, Mark { fill: None, soft });
        }
    }

    /// Drops the markers of an emptied database: a later `FILLGET` must become the filler.
    pub(crate) fn fill_database_flushed(&mut self, database: u8) {
        if let Some(table) = self.fills.as_mut() {
            table.marks.retain(|(owner, _), _| *owner != database);
        }
    }

    /// Lifts out the markers of a database emptied inside `MULTI`, so a refused batch can
    /// put them back.
    pub(crate) fn fill_detach_database(&mut self, database: u8) -> DetachedMarks {
        let Some(table) = self.fills.as_mut() else {
            return DetachedMarks(Vec::new());
        };

        DetachedMarks(
            table
                .marks
                .extract_if(|(owner, _), _| *owner == database)
                .collect(),
        )
    }

    /// Puts back what `fill_detach_database` lifted, in place of the database's markers now.
    pub(crate) fn fill_restore_database(&mut self, database: u8, detached: DetachedMarks) {
        self.fill_database_flushed(database);
        if !detached.0.is_empty() {
            self.fill_table().marks.extend(detached.0);
        }
    }

    /// Drops every marker after `FLUSHALL`.
    pub(crate) fn fill_all_flushed(&mut self) {
        self.fills = None;
    }

    /// Forget the soft expiry of a key that is gone.
    pub(crate) fn fill_forget_soft(&mut self, key: &[u8]) {
        let active = self.active;

        if let Some(table) = self.fills.as_mut()
            && let Some(mark) = table.marks.get_mut(&(active, key.into()))
        {
            mark.soft = None;
        }
    }
}
