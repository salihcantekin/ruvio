//! Slot ranges moved between the shards of one process, kept in the `SLOTS` file.
//!
//! Without the file every shard owns the slots the formula gives it ([`shard_for_slot`]), or
//! with a multi-node cluster map the slots the map places on it ([`SlotMoves::with_base`]):
//! moves are recorded against that starting table.
//! Each move appends one entry: the range, the shard it left and the shard it went to, the
//! target's log sequence where its copy began (`since`) and the source's first sequence
//! after the hand-over (`until`). An entry whose `until` is 0 was never completed: a move
//! in progress, or one a crash or a cancel ended.
//!
//! Recovery replays each shard's checkpoint and log as they were written and keeps, for
//! every slot, only what the shard held while it owned the slot ([`ReplayFilter`]): records
//! from a move that never completed are skipped, and keys of a range that left the shard
//! are dropped. The file is replaced whole (temporary file, fsync, rename), so a restart
//! sees the table before a hand-over or after it, never a mix.

use std::io;
use std::path::Path;

use crate::mutation::OwnedMutation;
use crate::routing::{HASH_SLOT_COUNT, SlotTable, hash_slot, shard_for_slot};

/// Name of the file in the data directory.
pub(crate) const FILE_NAME: &str = "SLOTS";
/// The starting table the moves were recorded against when it is not the formula (a
/// cluster map's placement): one little-endian `u16` per slot, the local shard or
/// [`NOT_LOCAL`]. Absent means the formula.
pub(crate) const BASE_FILE_NAME: &str = "SLOTS_BASE";
const NOT_LOCAL: u16 = u16::MAX;

const MAGIC: &[u8; 8] = b"RVSLOTS1";
const HEADER_LEN: usize = MAGIC.len() + 4 + 4;
const MOVE_LEN: usize = 2 * 4 + 8 * 2;
const CHECKSUM_LEN: usize = 4;

/// One move of the slots `first..=last` from shard `from` to shard `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotMove {
    pub first: u16,
    pub last: u16,
    pub from: u16,
    pub to: u16,
    /// The target's first log sequence of the copy.
    pub since: u64,
    /// The source's first log sequence after the hand-over; 0 while not completed.
    pub until: u64,
}

impl SlotMove {
    pub(crate) fn completed(&self) -> bool {
        self.until != 0
    }

    fn slots(&self) -> std::ops::RangeInclusive<u16> {
        self.first..=self.last
    }
}

/// Every move this data directory has seen, oldest first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotMoves {
    shard_count: u16,
    moves: Vec<SlotMove>,
    /// The owner of every slot before any move, when it is not the formula: a cluster map's
    /// placement, with other nodes' slots past the local shards. Not saved; the map is.
    base: Option<std::sync::Arc<[u16]>>,
}

impl SlotMoves {
    pub(crate) fn new(shard_count: usize) -> Self {
        Self {
            shard_count: shard_count as u16,
            moves: Vec::new(),
            base: None,
        }
    }

    /// Records moves against `base` (an owner per slot) instead of the formula.
    pub(crate) fn with_base(mut self, base: std::sync::Arc<[u16]>) -> Self {
        debug_assert_eq!(base.len(), HASH_SLOT_COUNT);
        self.base = Some(base);
        self
    }

    /// The owner of `slot` before any move.
    fn base_owner(&self, slot: u16) -> u16 {
        match &self.base {
            Some(base) => base[usize::from(slot)],
            None => shard_for_slot(slot, usize::from(self.shard_count)) as u16,
        }
    }

    /// The moves saved in `data_dir`, or `None` when nothing has moved. A file that does
    /// not decode, or was written for another shard count, is an error: starting from the
    /// formula instead would serve moved keys from shards that no longer hold them.
    pub(crate) fn load(data_dir: &Path, shard_count: usize) -> io::Result<Option<Self>> {
        let bytes = match std::fs::read(data_dir.join(FILE_NAME)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let moves = Self::decode(&bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{FILE_NAME} file is damaged"),
            )
        })?;

        if usize::from(moves.shard_count) != shard_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{FILE_NAME} file was written for {} shards, but {shard_count} are configured",
                    moves.shard_count
                ),
            ));
        }

        Ok(Some(moves))
    }

    /// Replaces the file in `data_dir` with these moves, durably.
    pub(crate) fn save(&self, data_dir: &Path) -> io::Result<()> {
        // The starting table first: a crash before `SLOTS` is replaced leaves moves that
        // were recorded against the same table (a restart refuses any other).
        match &self.base {
            Some(base) => crate::replication::write_durably(
                &data_dir.join(BASE_FILE_NAME),
                &encode_base(base, self.shard_count),
            )?,
            None => match std::fs::remove_file(data_dir.join(BASE_FILE_NAME)) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            },
        }
        crate::replication::write_durably(&data_dir.join(FILE_NAME), &self.encode())
    }

    /// Refuses moves saved against another starting table than `base` (`None`: the
    /// formula): their replay would keep the wrong slots on each shard.
    pub(crate) fn check_base(
        data_dir: &Path,
        shard_count: usize,
        base: Option<&[u16]>,
    ) -> io::Result<()> {
        let saved = match std::fs::read(data_dir.join(BASE_FILE_NAME)) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let formula: Vec<u16>;
        let current = match base {
            Some(base) => base,
            None => {
                formula = (0..HASH_SLOT_COUNT)
                    .map(|slot| shard_for_slot(slot as u16, shard_count) as u16)
                    .collect();
                &formula
            }
        };
        let expected = encode_base(current, shard_count as u16);
        let matches = match &saved {
            Some(saved) => *saved == expected,
            // Saved against the formula.
            None => (0..HASH_SLOT_COUNT)
                .all(|slot| current[slot] == shard_for_slot(slot as u16, shard_count) as u16),
        };

        if matches {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "slots were moved between shards ({FILE_NAME} file) with another slot layout \
                 ({}), or {BASE_FILE_NAME} is damaged; start with the cluster_topology this \
                 node had then",
                if saved.is_some() {
                    "an earlier cluster_topology"
                } else {
                    "no cluster_topology"
                }
            ),
        ))
    }

    /// The owner of every slot after the completed moves.
    pub(crate) fn owners(&self) -> Vec<u16> {
        let mut owners: Vec<u16> = (0..HASH_SLOT_COUNT)
            .map(|slot| self.base_owner(slot as u16))
            .collect();

        for entry in self.moves.iter().filter(|entry| entry.completed()) {
            for slot in entry.slots() {
                owners[usize::from(slot)] = entry.to;
            }
        }

        owners
    }

    /// Records a move that has begun. It must start from the current owner of every slot
    /// of the range, and no other move may be in progress.
    pub(crate) fn begin(
        &mut self,
        first: u16,
        last: u16,
        to: u16,
        since: u64,
    ) -> Result<(), &'static str> {
        if first > last || usize::from(last) >= HASH_SLOT_COUNT {
            return Err("invalid slot range");
        }
        if to >= self.shard_count {
            return Err("no such shard");
        }
        if since == 0 {
            return Err("log sequences start at 1");
        }

        let owners = self.owners();
        let from = owners[usize::from(first)];

        if owners[usize::from(first)..=usize::from(last)]
            .iter()
            .any(|owner| *owner != from)
        {
            return Err("slot range has more than one owner");
        }
        if from == to {
            return Err("slot range is already on that shard");
        }

        self.moves.push(SlotMove {
            first,
            last,
            from,
            to,
            since,
            until: 0,
        });
        Ok(())
    }

    /// Completes the newest move at the source's sequence `until`. Until this is saved,
    /// the move did not happen.
    pub(crate) fn complete(&mut self, until: u64) -> Result<(), &'static str> {
        let Some(entry) = self.moves.last_mut().filter(|entry| !entry.completed()) else {
            return Err("no move in progress");
        };

        if until == 0 {
            return Err("log sequences start at 1");
        }

        entry.until = until;
        Ok(())
    }

    /// Gives every slot of `table` its owner after the completed moves.
    pub(crate) fn apply(&self, table: &SlotTable) {
        for (slot, owner) in self.owners().into_iter().enumerate() {
            table.assign(slot as u16, usize::from(owner));
        }
    }

    /// What recovery of `shard` keeps, or `None` when no move concerns it (it then owns
    /// what the starting table gives it, for its whole log).
    pub(crate) fn replay_filter(
        &self,
        shard: usize,
        checkpoint_sequence: u64,
    ) -> Option<ReplayFilter> {
        let shard = shard as u16;

        if !self
            .moves
            .iter()
            .any(|entry| entry.from == shard || entry.to == shard)
        {
            return None;
        }

        let mut owned: Vec<bool> = (0..HASH_SLOT_COUNT)
            .map(|slot| self.base_owner(slot as u16) == shard)
            .collect();
        // A shard's own sequences increase with time, so its gains and losses are in
        // order; the sort only makes that explicit.
        let mut events: Vec<Event> = Vec::new();

        for entry in self.moves.iter().filter(|entry| entry.completed()) {
            if entry.to == shard {
                events.push(Event {
                    sequence: entry.since,
                    first: entry.first,
                    last: entry.last,
                    owned: true,
                });
            }
            if entry.from == shard {
                events.push(Event {
                    sequence: entry.until,
                    first: entry.first,
                    last: entry.last,
                    owned: false,
                });
            }
        }
        events.sort_by_key(|event| event.sequence);

        // Ownership when the checkpoint was taken: its entries for other slots are copies
        // of a move that never completed, or keys the source had not dropped yet.
        let pending = events.partition_point(|event| event.sequence <= checkpoint_sequence);

        for event in &events[..pending] {
            owned[usize::from(event.first)..=usize::from(event.last)].fill(event.owned);
        }
        events.drain(..pending);

        Some(ReplayFilter {
            owned,
            held: vec![false; HASH_SLOT_COUNT],
            events,
            next: 0,
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + self.moves.len() * MOVE_LEN + CHECKSUM_LEN);

        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&u32::from(self.shard_count).to_le_bytes());
        bytes.extend_from_slice(&(self.moves.len() as u32).to_le_bytes());
        for entry in &self.moves {
            for value in [entry.first, entry.last, entry.from, entry.to] {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            bytes.extend_from_slice(&entry.since.to_le_bytes());
            bytes.extend_from_slice(&entry.until.to_le_bytes());
        }
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let body_len = bytes.len().checked_sub(CHECKSUM_LEN)?;
        let (body, checksum) = bytes.split_at(body_len);

        if body.len() < HEADER_LEN
            || &body[..MAGIC.len()] != MAGIC
            || crc32c::crc32c(body).to_le_bytes() != checksum
        {
            return None;
        }

        let u16_at = |at: usize| u16::from_le_bytes([body[at], body[at + 1]]);
        let u32_at = |at: usize| u32::from_le_bytes(body[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let shard_count = u32_at(MAGIC.len());
        let count = u32_at(MAGIC.len() + 4) as usize;

        if shard_count == 0
            || shard_count > u32::from(u16::MAX)
            || body.len() != HEADER_LEN + count.checked_mul(MOVE_LEN)?
        {
            return None;
        }

        let moves = (0..count)
            .map(|index| {
                let at = HEADER_LEN + index * MOVE_LEN;

                SlotMove {
                    first: u16_at(at),
                    last: u16_at(at + 2),
                    from: u16_at(at + 4),
                    to: u16_at(at + 6),
                    since: u64_at(at + 8),
                    until: u64_at(at + 16),
                }
            })
            .collect::<Vec<_>>();
        let valid = moves.iter().all(|entry| {
            entry.first <= entry.last
                && usize::from(entry.last) < HASH_SLOT_COUNT
                && u32::from(entry.from) < shard_count
                && u32::from(entry.to) < shard_count
                && entry.from != entry.to
        });

        valid.then_some(Self {
            shard_count: shard_count as u16,
            moves,
            base: None,
        })
    }
}

/// A shard gaining (`owned`) or losing a range at one of its own log sequences.
#[derive(Clone, Copy, Debug)]
struct Event {
    sequence: u64,
    first: u16,
    last: u16,
    owned: bool,
}

/// Decides, during one shard's recovery, which checkpoint entries and log records belong to
/// it, and which keys it must drop afterwards.
///
/// A record is kept when the shard owned the slot of its keys at the record's sequence:
/// a shard writes only keys it owns, so every key of a record shares that answer, and
/// records it holds for slots it did not own are copies of a move that never completed.
/// Keys of a range the shard later gave away are kept while it replays (a later record of
/// a slot it still owns may read them, as `RENAME` does) and dropped when it gains the
/// range again or at the end ([`ReplayFilter::stale_slots`]).
#[derive(Debug)]
pub(crate) struct ReplayFilter {
    owned: Vec<bool>,
    /// Slots of which this replay has put keys into the store.
    held: Vec<bool>,
    events: Vec<Event>,
    next: usize,
}

impl ReplayFilter {
    /// Whether a checkpoint entry for `key` is kept.
    pub(crate) fn keeps_entry(&mut self, key: &[u8]) -> bool {
        let slot = usize::from(hash_slot(key));

        self.held[slot] |= self.owned[slot];
        self.owned[slot]
    }

    /// Whether the record at `sequence` is applied. Before it, any range the shard gains
    /// at or before `sequence` is handed to `drop_slots` when the store may still hold
    /// keys of an earlier stay; the caller drops those keys first.
    pub(crate) fn keeps_record(
        &mut self,
        sequence: u64,
        mutation: &OwnedMutation,
        drop_slots: &mut impl FnMut(&[bool]),
    ) -> bool {
        self.advance(sequence, drop_slots);

        let mut kept = None;
        let (owned, held) = (&self.owned, &mut self.held);

        mutation.for_each_write_key(&mut |key| {
            let slot = usize::from(hash_slot(key));
            let keep = *kept.get_or_insert(owned[slot]);

            held[slot] |= keep;
        });

        // Database-wide records name no key and apply to whatever the shard holds.
        kept.unwrap_or(true)
    }

    /// Applies the remaining gains and losses and returns the slots whose keys the store
    /// must drop: held by this replay and not owned at the end. `None` when there are none,
    /// so recovery walks the store only after a move a checkpoint does not yet cover.
    pub(crate) fn stale_slots(mut self) -> Option<Vec<bool>> {
        self.advance(u64::MAX, &mut |_| {});

        let stale: Vec<bool> = self
            .held
            .iter()
            .zip(&self.owned)
            .map(|(held, owned)| *held && !owned)
            .collect();

        stale.contains(&true).then_some(stale)
    }

    fn advance(&mut self, sequence: u64, drop_slots: &mut impl FnMut(&[bool])) {
        while let Some(event) = self.events.get(self.next).copied()
            && event.sequence <= sequence
        {
            let range = usize::from(event.first)..=usize::from(event.last);

            if event.owned && self.held[range.clone()].contains(&true) {
                let mut stale = vec![false; HASH_SLOT_COUNT];

                stale[range.clone()].copy_from_slice(&self.held[range.clone()]);
                drop_slots(&stale);
                self.held[range.clone()].fill(false);
            }
            self.owned[range].fill(event.owned);
            self.next += 1;
        }
    }
}

/// A starting table as [`BASE_FILE_NAME`] holds it: other nodes' slots as [`NOT_LOCAL`].
fn encode_base(base: &[u16], shard_count: u16) -> Vec<u8> {
    base.iter()
        .flat_map(|&owner| {
            if owner < shard_count {
                owner
            } else {
                NOT_LOCAL
            }
            .to_le_bytes()
        })
        .collect()
}

#[cfg(test)]
mod tests;
