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
//! A range can also come from another node of a multi-node cluster or leave for one (an
//! online migration): such an entry names [`REMOTE`] as its source or its target. A gain
//! is begun before the copy arrives, marked prepared once the copy is durable here
//! (`prepared`, this shard's last sequence then), and completed when the cluster map
//! gives the range to this node. A loss is recorded completed, at the source shard's first
//! sequence after the hand-over.
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
/// The source of a range gained from another node, or the target of one given to another
/// node.
pub(crate) const REMOTE: u16 = u16::MAX;
/// Why a source gave up a move whose target did not prepare the frozen range in time. The
/// coordinators start such a move again; a busy disk on the target may be quicker next time.
pub(crate) const PREPARE_LATE: &str = "the target did not prepare the range in time";

/// Version 1 entries have no `prepared` field; version 2 adds it (and remote entries).
const MAGIC_V1: &[u8; 8] = b"RVSLOTS1";
const MAGIC: &[u8; 8] = b"RVSLOTS2";
const HEADER_LEN: usize = MAGIC.len() + 4 + 4;
const MOVE_LEN_V1: usize = 2 * 4 + 8 * 2;
const MOVE_LEN: usize = MOVE_LEN_V1 + 8;
const CHECKSUM_LEN: usize = 4;

/// A step of a range arriving from another node, as a `SlotsIncoming` record carries it,
/// so a replica of the target records the same steps at the same sequences.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum IncomingStep {
    /// The copy begins at this record's sequence.
    Begin = 0,
    /// Every copy up to this record's sequence is durable.
    Prepared = 1,
    /// The move was cancelled: the copies are dropped.
    Cancelled = 2,
    /// The cluster map gave the range to this node at this record's sequence.
    Completed = 3,
}

impl IncomingStep {
    pub(crate) fn from_u8(step: u8) -> Option<Self> {
        match step {
            0 => Some(Self::Begin),
            1 => Some(Self::Prepared),
            2 => Some(Self::Cancelled),
            3 => Some(Self::Completed),
            _ => None,
        }
    }
}

/// One move of the slots `first..=last` from shard `from` to shard `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotMove {
    pub first: u16,
    pub last: u16,
    pub from: u16,
    pub to: u16,
    /// The target's first log sequence of the copy.
    pub since: u64,
    /// The source's first log sequence after the hand-over; 0 while not completed. For a
    /// range gained from another node, the target's sequence when the map gave it the range.
    pub until: u64,
    /// For a range gained from another node: the target's last sequence once the whole copy
    /// was durable, 0 before that or after a cancel. A prepared gain keeps its copies through
    /// a restart until the cluster map decides the move.
    pub prepared: u64,
}

impl SlotMove {
    pub(crate) fn completed(&self) -> bool {
        self.until != 0
    }

    /// Prepared and not yet completed: the copy is kept, the range is not served.
    pub(crate) fn pending(&self) -> bool {
        self.prepared != 0 && self.until == 0
    }

    /// A range gained from another node.
    pub(crate) fn gained(&self) -> bool {
        self.from == REMOTE
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

    /// The starting table saved next to the moves ([`BASE_FILE_NAME`]), other nodes' slots
    /// as a value at or above the shard count; `None` when the moves were recorded against
    /// the formula.
    pub(crate) fn load_base(
        data_dir: &Path,
        shard_count: usize,
    ) -> io::Result<Option<std::sync::Arc<[u16]>>> {
        let bytes = match std::fs::read(data_dir.join(BASE_FILE_NAME)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };

        if bytes.len() != HASH_SLOT_COUNT * 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{BASE_FILE_NAME} file is damaged"),
            ));
        }
        let base: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect();

        if base
            .iter()
            .any(|&owner| owner != NOT_LOCAL && usize::from(owner) >= shard_count)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{BASE_FILE_NAME} file was written for more shards than are configured"),
            ));
        }
        Ok(Some(base.into()))
    }

    /// Whether any move went between two shards of this node (not to or from another node).
    pub(crate) fn has_local_moves(&self) -> bool {
        self.moves
            .iter()
            .any(|entry| entry.from != REMOTE && entry.to != REMOTE)
    }

    /// Checks the moves against the cluster map's table for this node (`placement`: a local
    /// shard, or at or above the shard count for another group) and returns them with the
    /// map's decision applied, and whether that changed them:
    ///
    /// - a slot the map gives this node must be held here, or be part of a prepared gain,
    ///   which the map then completes;
    /// - a slot held here that the map gives another group is accepted only from a map the
    ///   coordinators sent (`pushed`): the node gave the range away and stopped before it
    ///   recorded that; its shard records the loss once it has replayed its log.
    pub(crate) fn reconcile(
        mut self,
        placement: &[u16],
        pushed: bool,
    ) -> Result<(Self, bool), String> {
        let mut changed = false;

        if let Some(gain) = self.pending_gain() {
            let given =
                (gain.first..=gain.last).all(|slot| placement[usize::from(slot)] == gain.to);

            if given {
                self.complete(gain.prepared)
                    .map_err(|error| format!("{FILE_NAME}: {error}"))?;
                changed = true;
            }
        }

        for slot in 0..HASH_SLOT_COUNT as u16 {
            let held = self.owner(slot) < self.shard_count;
            let mapped = placement[usize::from(slot)] < self.shard_count;

            if mapped && !held {
                return Err(format!(
                    "the cluster map gives this node slot {slot}, but its data is not here \
                     ({FILE_NAME} file); start with the cluster_topology this node had"
                ));
            }
            if held && !mapped && !pushed {
                return Err(format!(
                    "cluster_topology gives slot {slot} to another group, but this node holds \
                     its data ({FILE_NAME} file); move ranges between groups with the \
                     coordinators"
                ));
            }
        }

        Ok((self, changed))
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

        let from = self.single_owner(first, last)?;

        if from >= self.shard_count {
            return Err("slot range belongs to another node");
        }
        if from == to {
            return Err("slot range is already on that shard");
        }
        self.no_pending_gain()?;

        self.moves.push(SlotMove {
            first,
            last,
            from,
            to,
            since,
            until: 0,
            prepared: 0,
        });
        Ok(())
    }

    /// Records that a range owned by another node begins to arrive on shard `to`, whose
    /// copy starts at its sequence `since`.
    pub(crate) fn begin_gain(
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
        if (first..=last).any(|slot| self.owner(slot) < self.shard_count) {
            return Err("slot range is already on this node");
        }
        self.no_pending_gain()?;

        self.moves.push(SlotMove {
            first,
            last,
            from: REMOTE,
            to,
            since,
            until: 0,
            prepared: 0,
        });
        Ok(())
    }

    /// Marks the newest gain prepared: every copy up to this shard's sequence `prepared` is
    /// durable here.
    pub(crate) fn prepare(&mut self, prepared: u64) -> Result<(), &'static str> {
        let Some(entry) = self
            .newest_gain()
            .map(|index| &mut self.moves[index])
            .filter(|entry| !entry.completed())
        else {
            return Err("no gain in progress");
        };

        if prepared == 0 {
            return Err("log sequences start at 1");
        }

        entry.prepared = prepared;
        Ok(())
    }

    /// Ends the newest gain when it is `first..=last` arriving on shard `to` and not ended
    /// yet: completed at `until`, or cancelled when `until` is `None`. The gain it ended, or
    /// `None` when the newest gain is another one, or already ended: an end recorded once
    /// already, or that of a gain whose records never reached here.
    pub(crate) fn end_gain(
        &mut self,
        first: u16,
        last: u16,
        to: u16,
        until: Option<u64>,
    ) -> Result<Option<SlotMove>, &'static str> {
        let Some(entry) = self
            .newest_gain()
            .map(|index| &mut self.moves[index])
            .filter(|entry| (entry.first, entry.last, entry.to) == (first, last, to))
        else {
            return Ok(None);
        };

        match until {
            _ if entry.completed() => Ok(None),
            None => {
                entry.prepared = 0;
                Ok(Some(*entry))
            }
            Some(_) if entry.prepared == 0 => Err("the range from another node is not prepared"),
            Some(until) => {
                entry.until = until;
                Ok(Some(*entry))
            }
        }
    }

    /// Records that shard `from` gave `first..=last` to another node at its sequence `until`
    /// (its first after the hand-over).
    pub(crate) fn lose(
        &mut self,
        first: u16,
        last: u16,
        from: u16,
        until: u64,
    ) -> Result<(), &'static str> {
        if first > last || usize::from(last) >= HASH_SLOT_COUNT {
            return Err("invalid slot range");
        }
        if until == 0 {
            return Err("log sequences start at 1");
        }
        if self.single_owner(first, last)? != from {
            return Err("slot range is not on that shard");
        }

        self.moves.push(SlotMove {
            first,
            last,
            from,
            to: REMOTE,
            since: 0,
            until,
            prepared: 0,
        });
        Ok(())
    }

    /// Whether `slot` ever moved between this node and another one: a log may then hold
    /// its keys although another group serves it now.
    pub(crate) fn crossed_nodes(&self, slot: u16) -> bool {
        self.moves.iter().any(|entry| {
            (entry.from == REMOTE || entry.to == REMOTE) && entry.slots().contains(&slot)
        })
    }

    /// The newest gain when the map has not completed it: arriving, prepared or cancelled.
    pub(crate) fn unfinished_gain(&self) -> Option<SlotMove> {
        self.newest_gain()
            .map(|index| self.moves[index])
            .filter(|entry| !entry.completed())
    }

    /// The prepared gain the cluster map has not decided yet.
    pub(crate) fn pending_gain(&self) -> Option<SlotMove> {
        self.newest_gain()
            .map(|index| self.moves[index])
            .filter(SlotMove::pending)
    }

    /// Where the newest range gained from another node is: losses recorded after it, by
    /// other shards of the node, do not hide it.
    fn newest_gain(&self) -> Option<usize> {
        self.moves.iter().rposition(SlotMove::gained)
    }

    /// The owner every slot of `first..=last` has, or an error when they differ.
    fn single_owner(&self, first: u16, last: u16) -> Result<u16, &'static str> {
        let owners = self.owners();
        let from = owners[usize::from(first)];

        if owners[usize::from(first)..=usize::from(last)]
            .iter()
            .any(|owner| *owner != from)
        {
            return Err("slot range has more than one owner");
        }
        Ok(from)
    }

    /// The owner of `slot` after the completed moves; at or above the shard count when
    /// another node owns it.
    pub(crate) fn owner(&self, slot: u16) -> u16 {
        self.moves
            .iter()
            .rev()
            .find(|entry| entry.completed() && (entry.first..=entry.last).contains(&slot))
            .map_or_else(|| self.base_owner(slot), |entry| entry.to)
    }

    /// A prepared gain keeps its copies until the map decides; no other move starts first.
    fn no_pending_gain(&self) -> Result<(), &'static str> {
        match self.pending_gain() {
            Some(_) => Err("a range from another node waits for the cluster map"),
            None => Ok(()),
        }
    }

    /// Completes the newest move at the source's sequence `until` (for a gain, this shard's
    /// sequence when the map gave it the range). Until this is saved, the move did not happen.
    pub(crate) fn complete(&mut self, until: u64) -> Result<(), &'static str> {
        let Some(entry) = self
            .moves
            .iter()
            .rposition(|entry| !entry.completed())
            .map(|index| &mut self.moves[index])
        else {
            return Err("no move in progress");
        };

        if until == 0 {
            return Err("log sequences start at 1");
        }
        if entry.gained() && entry.prepared == 0 {
            return Err("the range from another node is not prepared");
        }

        entry.until = until;
        Ok(())
    }

    /// Gives every slot of `table` its owner after the completed moves. A slot of another
    /// node gets `remote(slot)`, its entry for that node (the cluster map's).
    pub(crate) fn apply(&self, table: &SlotTable, remote: impl Fn(u16) -> usize) {
        for (slot, owner) in self.owners().into_iter().enumerate() {
            let slot = slot as u16;

            table.assign(
                slot,
                if owner < self.shard_count {
                    usize::from(owner)
                } else {
                    remote(slot)
                },
            );
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

        // A prepared gain keeps its copies until the cluster map decides the move.
        for entry in self
            .moves
            .iter()
            .filter(|entry| entry.completed() || entry.pending())
        {
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
            bytes.extend_from_slice(&entry.prepared.to_le_bytes());
        }
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let body_len = bytes.len().checked_sub(CHECKSUM_LEN)?;
        let (body, checksum) = bytes.split_at(body_len);

        if body.len() < HEADER_LEN || crc32c::crc32c(body).to_le_bytes() != checksum {
            return None;
        }
        let move_len = match &body[..MAGIC.len()] {
            magic if magic == MAGIC => MOVE_LEN,
            magic if magic == MAGIC_V1 => MOVE_LEN_V1,
            _ => return None,
        };

        let u16_at = |at: usize| u16::from_le_bytes([body[at], body[at + 1]]);
        let u32_at = |at: usize| u32::from_le_bytes(body[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let shard_count = u32_at(MAGIC.len());
        let count = u32_at(MAGIC.len() + 4) as usize;

        if shard_count == 0
            || shard_count > u32::from(u16::MAX)
            || body.len() != HEADER_LEN + count.checked_mul(move_len)?
        {
            return None;
        }

        let moves = (0..count)
            .map(|index| {
                let at = HEADER_LEN + index * move_len;

                SlotMove {
                    first: u16_at(at),
                    last: u16_at(at + 2),
                    from: u16_at(at + 4),
                    to: u16_at(at + 6),
                    since: u64_at(at + 8),
                    until: u64_at(at + 16),
                    prepared: if move_len == MOVE_LEN {
                        u64_at(at + 24)
                    } else {
                        0
                    },
                }
            })
            .collect::<Vec<_>>();
        let shard_or_remote = |shard: u16| shard == REMOTE || u32::from(shard) < shard_count;
        let valid = moves.iter().all(|entry| {
            entry.first <= entry.last
                && usize::from(entry.last) < HASH_SLOT_COUNT
                && shard_or_remote(entry.from)
                && shard_or_remote(entry.to)
                && entry.from != entry.to
                // A loss is recorded completed.
                && (entry.to != REMOTE || entry.completed())
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
