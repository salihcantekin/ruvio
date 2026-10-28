use crate::id_map::IdMap;
use blocks::Blocks;
use std::cmp::Ordering;
use std::ops::Bound;

mod blocks;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub millis: u64,
    pub sequence: u64,
    pub fields: StreamFields,
}

/// Bytes an entry's fields keep inside the entry before they move to the heap.
const INLINE_FIELDS: usize = 22;

/// An entry's field-value pairs packed into one buffer: each field and each value as a
/// LEB128 length followed by its bytes. Small entries (`f v`, a short reading) fit in
/// the entry itself; larger ones take one allocation instead of one per pair, field and
/// value.
#[derive(Clone, PartialEq, Eq)]
pub struct StreamFields(Packed);

#[derive(Clone, PartialEq, Eq)]
enum Packed {
    Inline(u8, [u8; INLINE_FIELDS]),
    Heap(Box<[u8]>),
}

impl StreamFields {
    pub fn from_pairs<'p>(pairs: impl Iterator<Item = (&'p [u8], &'p [u8])> + Clone) -> Self {
        let size = pairs.clone().fold(0usize, |size, (field, value)| {
            size.saturating_add(varint_len(field.len()) + field.len())
                .saturating_add(varint_len(value.len()) + value.len())
        });
        let write = |buffer: &mut [u8]| {
            let mut at = 0;

            for (field, value) in pairs {
                for bytes in [field, value] {
                    at += put_varint(&mut buffer[at..], bytes.len());
                    buffer[at..at + bytes.len()].copy_from_slice(bytes);
                    at += bytes.len();
                }
            }
        };

        if size <= INLINE_FIELDS {
            let mut inline = [0; INLINE_FIELDS];

            write(&mut inline[..size]);
            Self(Packed::Inline(size as u8, inline))
        } else {
            let mut heap = vec![0; size].into_boxed_slice();

            write(&mut heap);
            Self(Packed::Heap(heap))
        }
    }

    /// Fields already in the packed layout, as a block keeps them.
    pub(crate) fn from_packed(bytes: &[u8]) -> Self {
        Self::build(bytes.len(), |buffer| buffer.copy_from_slice(bytes))
    }

    /// Field names and values, each a run of lengths and bytes in the packed layout,
    /// merged into pairs.
    pub(crate) fn interleave(names: &[u8], values: &[u8]) -> Self {
        Self::build(names.len() + values.len(), |buffer| {
            let (mut names, mut values, mut at) = (names, values, 0);

            while !names.is_empty() {
                for run in [&mut names, &mut values] {
                    let (length, used) = read_varint(run);
                    let (part, rest) = run.split_at(used + length);

                    buffer[at..at + part.len()].copy_from_slice(part);
                    at += part.len();
                    *run = rest;
                }
            }
        })
    }

    fn build(size: usize, write: impl FnOnce(&mut [u8])) -> Self {
        if size <= INLINE_FIELDS {
            let mut inline = [0; INLINE_FIELDS];

            write(&mut inline[..size]);
            Self(Packed::Inline(size as u8, inline))
        } else {
            let mut heap = vec![0; size].into_boxed_slice();

            write(&mut heap);
            Self(Packed::Heap(heap))
        }
    }

    fn bytes(&self) -> &[u8] {
        match &self.0 {
            Packed::Inline(len, bytes) => &bytes[..usize::from(*len)],
            Packed::Heap(bytes) => bytes,
        }
    }

    pub fn iter(&self) -> FieldPairs<'_> {
        FieldPairs(self.bytes())
    }

    /// Number of field-value pairs.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes().is_empty()
    }

    /// Field and value bytes, without the length prefixes.
    pub fn payload(&self) -> usize {
        self.iter()
            .fold(0, |total, (field, value)| total + field.len() + value.len())
    }

    /// Heap bytes the entry owns beyond its own size.
    pub fn heap_bytes(&self) -> usize {
        match &self.0 {
            Packed::Inline(..) => 0,
            Packed::Heap(bytes) => bytes.len(),
        }
    }
}

impl std::fmt::Debug for StreamFields {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> IntoIterator for &'a StreamFields {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = FieldPairs<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The pairs of a [`StreamFields`], in insertion order.
#[derive(Clone)]
pub struct FieldPairs<'a>(&'a [u8]);

impl<'a> FieldPairs<'a> {
    fn take(&mut self) -> &'a [u8] {
        let (len, used) = read_varint(self.0);
        let (bytes, rest) = self.0[used..].split_at(len);

        self.0 = rest;
        bytes
    }
}

impl<'a> Iterator for FieldPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }

        let field = self.take();
        let value = self.take();

        Some((field, value))
    }
}

fn varint_len(mut value: usize) -> usize {
    let mut len = 1;

    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }

    len
}

fn put_varint(buffer: &mut [u8], mut value: usize) -> usize {
    let mut at = 0;

    while value >= 0x80 {
        buffer[at] = (value as u8) | 0x80;
        value >>= 7;
        at += 1;
    }

    buffer[at] = value as u8;
    at + 1
}

fn read_varint(buffer: &[u8]) -> (usize, usize) {
    let mut value = 0usize;

    for (index, byte) in buffer.iter().enumerate() {
        value |= usize::from(byte & 0x7f) << (7 * index);

        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }

    unreachable!("stream fields are written by `StreamFields::from_pairs`")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDelivery {
    pub millis: u64,
    pub sequence: u64,
    pub consumer: Vec<u8>,
    pub delivered_at: u64,
    pub deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroup {
    pub name: Vec<u8>,
    pub last_millis: u64,
    pub last_sequence: u64,
    pub pending: Vec<PendingDelivery>,
    /// Names registered before they have a pending entry.
    pub idle: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReclaimUpdate {
    pub(crate) millis: u64,
    pub(crate) sequence: u64,
    pub(crate) delivered_at: u64,
    pub(crate) deliveries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupError {
    Busy,
    NoGroup,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingSummary {
    pub(crate) count: usize,
    pub(crate) smallest: Option<(u64, u64)>,
    pub(crate) greatest: Option<(u64, u64)>,
    pub(crate) consumers: Vec<(Vec<u8>, i64)>,
}

pub(crate) struct PendingFilter<'a> {
    pub(crate) start: StreamBound,
    pub(crate) end: StreamBound,
    pub(crate) min_idle: Option<u64>,
    pub(crate) consumer: Option<&'a [u8]>,
    pub(crate) count: usize,
    pub(crate) now: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingRow {
    pub(crate) millis: u64,
    pub(crate) sequence: u64,
    pub(crate) consumer: Vec<u8>,
    pub(crate) idle: u64,
    pub(crate) deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamOverview {
    pub(crate) length: usize,
    pub(crate) last_millis: Option<u64>,
    pub(crate) last_sequence: u64,
    pub(crate) groups: usize,
    pub(crate) first: Option<StreamEntry>,
    pub(crate) last: Option<StreamEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupOverview {
    pub(crate) name: Vec<u8>,
    pub(crate) consumers: usize,
    pub(crate) pending: usize,
    pub(crate) last_millis: u64,
    pub(crate) last_sequence: u64,
    pub(crate) lag: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AutoclaimStep {
    Claim {
        millis: u64,
        sequence: u64,
        deliveries: u64,
    },
    /// The pending id is no longer in the stream.
    Drop { millis: u64, sequence: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoclaimPlan {
    pub(crate) steps: Vec<AutoclaimStep>,
    /// `0-0` when the pending list was finished. Otherwise the last id visited.
    pub(crate) next: (u64, u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsumerOverview {
    pub(crate) name: Vec<u8>,
    pub(crate) pending: usize,
    pub(crate) seen_at: u64,
    pub(crate) idle: u64,
}

/// A stream: entries in id order, consumer groups and delayed holds.
///
/// Entries sit in a ring buffer, so trimming from the front (`MAXLEN`, `MINID`) moves
/// nothing. Each group keeps its pending entries list in an id map (see `IdMap`), and every
/// consumer the ids it owns, so acknowledging, claiming and paging the list are logarithmic
/// in its size.
#[derive(Clone, Debug)]
pub struct StreamValue {
    started: bool,
    last_millis: u64,
    last_sequence: u64,
    entries: Blocks,
    /// Sorted by name.
    groups: Vec<Group>,
    /// `(millis, sequence, ready_at)`. Empty until an `XADD DELAY` writes one. `Vec::new` does not allocate.
    delayed: Vec<(u64, u64, u64)>,
    /// Group names and consumer names (each once). Updated on each mutation so memory
    /// admission does not walk the stream.
    payload_bytes: usize,
    /// Pending deliveries across all groups, charged at `PENDING_ROW_BYTES` each.
    pending_rows: usize,
}

impl PartialEq for StreamValue {
    fn eq(&self, other: &Self) -> bool {
        self.started == other.started
            && self.last_millis == other.last_millis
            && self.last_sequence == other.last_sequence
            && self.groups == other.groups
            && self.delayed == other.delayed
            && self.entries.len() == other.entries.len()
            && self.entries.to_vec() == other.entries.to_vec()
    }
}

impl Eq for StreamValue {}

/// Heap bytes one pending delivery takes: its row in the group's id map (id and delivery)
/// plus its id in the consumer's map, with run slack: 60 to 80 bytes per row whether ids
/// were delivered in order or scattered and however many were acknowledged
/// (`a_pending_row_costs_no_more_than_it_is_charged`), charged with some margin.
pub(crate) const PENDING_ROW_BYTES: usize = 88;

type Id = crate::id_map::Id;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Group {
    name: Vec<u8>,
    last_millis: u64,
    last_sequence: u64,
    pending: IdMap<Delivery>,
    /// Slots addressed by the consumer number stored in each delivery; a deleted consumer
    /// leaves `None` until a new one reuses the slot.
    consumers: Vec<Option<Consumer>>,
    /// Slot numbers of the live consumers, sorted by consumer name.
    by_name: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Delivery {
    consumer: u32,
    delivered_at: u64,
    deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Consumer {
    name: Vec<u8>,
    /// Latest delivery or claim time this consumer got, kept after its entries are
    /// acknowledged. `None` for a consumer only ever created (its WAL record and the
    /// snapshot carry no time).
    seen_at: Option<u64>,
    /// Ids of this consumer's pending deliveries.
    owned: IdMap<()>,
}

impl Group {
    fn new(name: &[u8], last_millis: u64, last_sequence: u64) -> Self {
        Self {
            name: name.to_vec(),
            last_millis,
            last_sequence,
            pending: IdMap::new(),
            consumers: Vec::new(),
            by_name: Vec::new(),
        }
    }

    fn consumer(&self, slot: u32) -> &Consumer {
        self.consumers[slot as usize]
            .as_ref()
            .expect("a delivery names a live consumer")
    }

    fn consumer_mut(&mut self, slot: u32) -> &mut Consumer {
        self.consumers[slot as usize]
            .as_mut()
            .expect("a delivery names a live consumer")
    }

    fn name_position(&self, name: &[u8]) -> Result<usize, usize> {
        self.by_name
            .binary_search_by(|slot| self.consumer(*slot).name.as_slice().cmp(name))
    }

    fn find_consumer(&self, name: &[u8]) -> Option<u32> {
        self.name_position(name)
            .ok()
            .map(|position| self.by_name[position])
    }

    /// The consumer's slot, created when missing; the bool says whether it is new.
    fn consumer_slot(&mut self, name: &[u8]) -> (u32, bool) {
        match self.name_position(name) {
            Ok(position) => (self.by_name[position], false),
            Err(position) => {
                let consumer = Consumer {
                    name: name.to_vec(),
                    seen_at: None,
                    owned: IdMap::new(),
                };
                let slot = match self.consumers.iter().position(Option::is_none) {
                    Some(free) => {
                        self.consumers[free] = Some(consumer);
                        free
                    }
                    None => {
                        self.consumers.push(Some(consumer));
                        self.consumers.len() - 1
                    }
                };
                let slot = u32::try_from(slot).expect("fewer than 2^32 consumers");

                self.by_name.insert(position, slot);
                (slot, true)
            }
        }
    }

    /// Inserts or replaces the delivery of `id`, moving it to `consumer`, who counts as seen
    /// at `delivered_at`. Returns the bytes of a consumer name this added and whether the
    /// row is new.
    fn put(
        &mut self,
        id: Id,
        consumer: &[u8],
        delivered_at: u64,
        deliveries: u64,
    ) -> (usize, bool) {
        let (slot, created) = self.consumer_slot(consumer);
        let delivery = Delivery {
            consumer: slot,
            delivered_at,
            deliveries,
        };

        let previous = self.pending.insert(id, delivery);

        if let Some(previous) = previous
            && previous.consumer != slot
        {
            self.consumer_mut(previous.consumer).owned.remove(&id);
        }

        let owner = self.consumer_mut(slot);

        owner.owned.insert(id, ());
        owner.seen_at = Some(
            owner
                .seen_at
                .map_or(delivered_at, |seen| seen.max(delivered_at)),
        );

        (if created { consumer.len() } else { 0 }, previous.is_none())
    }

    fn row(&self, id: Id, delivery: &Delivery) -> PendingDelivery {
        PendingDelivery {
            millis: id.0,
            sequence: id.1,
            consumer: self.consumer(delivery.consumer).name.clone(),
            delivered_at: delivery.delivered_at,
            deliveries: delivery.deliveries,
        }
    }

    fn live_consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.by_name.iter().map(|slot| self.consumer(*slot))
    }

    /// The interchange form snapshots and DUMP write: rows in id order, and the consumers
    /// without a pending entry by name.
    fn export(&self) -> ConsumerGroup {
        ConsumerGroup {
            name: self.name.clone(),
            last_millis: self.last_millis,
            last_sequence: self.last_sequence,
            pending: self
                .pending
                .iter()
                .map(|(id, delivery)| self.row(*id, delivery))
                .collect(),
            idle: self
                .live_consumers()
                .filter(|consumer| consumer.owned.is_empty())
                .map(|consumer| consumer.name.clone())
                .collect(),
        }
    }

    fn import(group: ConsumerGroup) -> Self {
        let mut imported = Self::new(&group.name, group.last_millis, group.last_sequence);

        for name in &group.idle {
            imported.consumer_slot(name);
        }
        for delivery in group.pending {
            imported.put(
                (delivery.millis, delivery.sequence),
                &delivery.consumer,
                delivery.delivered_at,
                delivery.deliveries,
            );
        }

        imported
    }

    fn payload(&self) -> usize {
        self.live_consumers()
            .fold(self.name.len(), |total, consumer| {
                total.saturating_add(consumer.name.len())
            })
    }
}

impl StreamValue {
    pub(crate) fn new() -> Self {
        Self {
            started: false,
            last_millis: 0,
            last_sequence: 0,
            entries: Blocks::new(),
            groups: Vec::new(),
            delayed: Vec::new(),
            payload_bytes: 0,
            pending_rows: 0,
        }
    }

    /// Bytes the stream holds beyond its payload: the packed entries and pending deliveries.
    pub(crate) fn collection_bytes(&self) -> usize {
        self.entries
            .bytes()
            .saturating_add(self.pending_rows.saturating_mul(PENDING_ROW_BYTES))
    }

    /// Books what `Group::put` reports: a new consumer name and a new row.
    fn account(&mut self, (name_bytes, new_row): (usize, bool)) {
        self.payload_bytes = self.payload_bytes.saturating_add(name_bytes);
        self.pending_rows += usize::from(new_row);
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entries(&self) -> Entries<'_> {
        Entries(&self.entries)
    }

    pub(crate) fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub(crate) fn append(
        &mut self,
        millis: u64,
        sequence: u64,
        fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)],
    ) -> Result<(), &'static [u8]> {
        if fields.is_empty() {
            return Err(b"ERR stream entry needs at least one field");
        }

        if self.started
            && compare_id(millis, sequence, self.last_millis, self.last_sequence)
                != Ordering::Greater
        {
            return Err(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
            );
        }

        self.started = true;
        self.last_millis = millis;
        self.last_sequence = sequence;
        self.entries.push(millis, sequence, fields);

        Ok(())
    }

    pub(crate) fn accepts(&self, millis: u64, sequence: u64) -> bool {
        !self.started
            || compare_id(millis, sequence, self.last_millis, self.last_sequence)
                == Ordering::Greater
    }

    pub(crate) fn last_id_is_too_small(&self, millis: u64, sequence: u64) -> bool {
        self.started
            && compare_id(millis, sequence, self.last_millis, self.last_sequence) == Ordering::Less
    }

    /// Moves the last generated id forward. An equal id is kept. A smaller id is refused.
    pub(crate) fn set_last_id(&mut self, millis: u64, sequence: u64) -> Result<(), &'static [u8]> {
        if self.last_id_is_too_small(millis, sequence) {
            return Err(
                b"ERR The ID specified in XSETID is smaller than the target stream top item",
            );
        }

        self.started = true;
        self.last_millis = millis;
        self.last_sequence = sequence;

        Ok(())
    }

    pub(crate) fn entry(&self, millis: u64, sequence: u64) -> Option<StreamEntry> {
        self.entries.get(millis, sequence)
    }

    pub(crate) fn contains(&self, millis: u64, sequence: u64) -> bool {
        self.entries.contains(millis, sequence)
    }

    /// Sequence for `XADD <millis>-*`; `None` when no id at `millis` can follow the top id.
    pub(crate) fn next_seq_at(&self, millis: u64) -> Option<u64> {
        if !self.started || millis > self.last_millis {
            return Some(0);
        }

        if millis < self.last_millis {
            return None;
        }

        self.last_sequence.checked_add(1)
    }

    pub(crate) fn next_auto_id(&self, now_millis: u64) -> (u64, u64) {
        if !self.started {
            return (now_millis, 0);
        }

        if now_millis > self.last_millis {
            return (now_millis, 0);
        }

        match self.last_sequence.checked_add(1) {
            Some(sequence) => (self.last_millis, sequence),
            // The sequence is used up: continue in the next millisecond, as Redis does.
            None => (self.last_millis.saturating_add(1), 0),
        }
    }

    pub(crate) fn delete(&mut self, millis: u64, sequence: u64) -> bool {
        if !self.entries.remove(millis, sequence) {
            return false;
        }

        self.drop_holds_for_missing_entries();
        true
    }

    /// Drops entries whose id is strictly below `millis-sequence`.
    pub(crate) fn trim_before(&mut self, millis: u64, sequence: u64) -> usize {
        let removed = self.entries.drop_before(millis, sequence);

        self.drop_holds_for_missing_entries();
        removed
    }

    pub(crate) fn trim(&mut self, max_len: usize) {
        if self.entries.len() > max_len {
            self.drop_front(self.entries.len() - max_len);
        }
    }

    /// Removes the `count` oldest entries.
    fn drop_front(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        self.entries.drop_front(count);
        self.drop_holds_for_missing_entries();
    }

    fn drop_holds_for_missing_entries(&mut self) {
        if self.delayed.is_empty() {
            return;
        }

        let kept: Vec<_> = self
            .delayed
            .iter()
            .copied()
            .filter(|(millis, sequence, _)| self.entries.contains(*millis, *sequence))
            .collect();

        self.delayed = kept;
    }

    pub(crate) fn read_after(&self, cursor: StreamCursor, limit: usize) -> Vec<StreamEntry> {
        match cursor {
            StreamCursor::End => Vec::new(),
            StreamCursor::After(millis, sequence) => self.entries.after(millis, sequence, limit),
        }
    }

    /// At most `count` entries between the bounds, from the start (or from the end when
    /// `reverse`); only the entries returned are visited.
    pub(crate) fn range_page(
        &self,
        start: StreamBound,
        end: StreamBound,
        count: usize,
        reverse: bool,
    ) -> Vec<StreamEntry> {
        let start = match start {
            StreamBound::Min => None,
            StreamBound::Max => return Vec::new(),
            StreamBound::Id(millis, sequence) => Some((millis, sequence)),
        };
        let end = match end {
            StreamBound::Max => None,
            StreamBound::Min => return Vec::new(),
            StreamBound::Id(millis, sequence) => Some((millis, sequence)),
        };

        self.entries.range(start, end, count, reverse)
    }

    fn group(&self, name: &[u8]) -> Result<&Group, GroupError> {
        self.groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
            .map(|index| &self.groups[index])
            .map_err(|_| GroupError::NoGroup)
    }

    fn group_mut(&mut self, name: &[u8]) -> Result<&mut Group, GroupError> {
        match self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        {
            Ok(index) => Ok(&mut self.groups[index]),
            Err(_) => Err(GroupError::NoGroup),
        }
    }

    pub(crate) fn has_group(&self, name: &[u8]) -> bool {
        self.group(name).is_ok()
    }

    /// Every group in the interchange form, for snapshots and DUMP.
    pub(crate) fn groups(&self) -> Vec<ConsumerGroup> {
        self.groups.iter().map(Group::export).collect()
    }

    pub(crate) fn overview(&self) -> StreamOverview {
        StreamOverview {
            length: self.entries.len(),
            last_millis: self.started.then_some(self.last_millis),
            last_sequence: self.last_sequence,
            groups: self.groups.len(),
            first: self.entries.first(),
            last: self.entries.last(),
        }
    }

    pub(crate) fn group_overviews(&self) -> Vec<GroupOverview> {
        self.groups
            .iter()
            .map(|group| GroupOverview {
                name: group.name.clone(),
                consumers: group.by_name.len(),
                pending: group.pending.len(),
                last_millis: group.last_millis,
                last_sequence: group.last_sequence,
                lag: self.entries_after(group.last_millis, group.last_sequence),
            })
            .collect()
    }

    pub(crate) fn consumer_overviews(
        &self,
        group: &[u8],
        now: u64,
    ) -> Result<Vec<ConsumerOverview>, GroupError> {
        let group = self.group(group)?;

        Ok(group
            .live_consumers()
            .map(|consumer| {
                // A consumer never handed an entry counts as seen now.
                let seen_at = consumer.seen_at.unwrap_or(now);

                ConsumerOverview {
                    name: consumer.name.clone(),
                    pending: consumer.owned.len(),
                    seen_at,
                    idle: now.saturating_sub(seen_at),
                }
            })
            .collect())
    }

    pub(crate) fn autoclaim_plan(
        &self,
        group: &[u8],
        start: (u64, u64),
        min_idle: u64,
        count: usize,
        now: u64,
    ) -> Result<AutoclaimPlan, GroupError> {
        let group = self.group(group)?;
        let mut pending = group
            .pending
            .range((Bound::Excluded(start), Bound::Unbounded))
            .peekable();
        let mut steps = Vec::new();
        let mut claimed = 0usize;

        // Like Redis, look at no more than ten pending entries per one asked for.
        let budget = count.saturating_mul(10);
        let mut scanned = 0usize;

        while let Some((&(millis, sequence), delivery)) = pending.next() {
            scanned += 1;

            if !self.contains(millis, sequence) {
                steps.push(AutoclaimStep::Drop { millis, sequence });
            } else if now.saturating_sub(delivery.delivered_at) >= min_idle {
                steps.push(AutoclaimStep::Claim {
                    millis,
                    sequence,
                    deliveries: delivery.deliveries.saturating_add(1),
                });
                claimed += 1;
            }

            if claimed == count || scanned >= budget {
                let next = if pending.peek().is_none() {
                    (0, 0)
                } else {
                    (millis, sequence)
                };

                return Ok(AutoclaimPlan { steps, next });
            }
        }

        Ok(AutoclaimPlan {
            steps,
            next: (0, 0),
        })
    }

    fn entries_after(&self, millis: u64, sequence: u64) -> usize {
        self.entries.count_after(millis, sequence)
    }

    pub(crate) fn group_cursor(&self, name: &[u8]) -> Result<(u64, u64), GroupError> {
        let group = self.group(name)?;

        Ok((group.last_millis, group.last_sequence))
    }

    pub(crate) fn create_group(
        &mut self,
        name: &[u8],
        last_millis: u64,
        last_sequence: u64,
    ) -> Result<(), GroupError> {
        let Err(index) = self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        else {
            return Err(GroupError::Busy);
        };

        self.groups
            .insert(index, Group::new(name, last_millis, last_sequence));
        self.payload_bytes = self.payload_bytes.saturating_add(name.len());
        Ok(())
    }

    pub(crate) fn destroy_group(&mut self, name: &[u8]) -> bool {
        let Ok(index) = self
            .groups
            .binary_search_by(|group| group.name.as_slice().cmp(name))
        else {
            return false;
        };

        let removed = self.groups.remove(index);

        self.payload_bytes = self.payload_bytes.saturating_sub(removed.payload());
        self.pending_rows -= removed.pending.len();
        true
    }

    pub(crate) fn set_group_id(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        group.last_millis = millis;
        group.last_sequence = sequence;

        Ok(())
    }

    /// Registers a consumer with no pending entry. Returns whether the name is new.
    pub(crate) fn create_consumer(
        &mut self,
        name: &[u8],
        consumer: &[u8],
    ) -> Result<bool, GroupError> {
        let (_, created) = self.group_mut(name)?.consumer_slot(consumer);

        if created {
            self.payload_bytes = self.payload_bytes.saturating_add(consumer.len());
        }
        Ok(created)
    }

    /// Removes a consumer and its pending deliveries; returns how many deliveries went.
    pub(crate) fn delete_consumer(
        &mut self,
        name: &[u8],
        consumer: &[u8],
    ) -> Result<usize, GroupError> {
        let group = self.group_mut(name)?;
        let Ok(position) = group.name_position(consumer) else {
            return Ok(0);
        };
        let slot = group.by_name.remove(position);
        let removed = group.consumers[slot as usize]
            .take()
            .expect("a listed consumer is live");

        for (id, ()) in &removed.owned {
            group.pending.remove(id);
        }

        self.payload_bytes = self.payload_bytes.saturating_sub(removed.name.len());
        self.pending_rows -= removed.owned.len();
        Ok(removed.owned.len())
    }

    pub(crate) fn advance_group(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        if compare_id(millis, sequence, group.last_millis, group.last_sequence) == Ordering::Greater
        {
            group.last_millis = millis;
            group.last_sequence = sequence;
        }

        Ok(())
    }

    pub(crate) fn hold_until(&mut self, millis: u64, sequence: u64, ready_at: u64) {
        if let Some((held_ms, held_seq, _)) = self.delayed.last() {
            match compare_id(millis, sequence, *held_ms, *held_seq) {
                Ordering::Greater => {
                    self.delayed.push((millis, sequence, ready_at));

                    return;
                }

                Ordering::Equal => return,
                Ordering::Less => {}
            }
        }

        let index = self
            .delayed
            .binary_search_by(|(held_ms, held_seq, _)| {
                compare_id(*held_ms, *held_seq, millis, sequence)
            })
            .unwrap_or_else(|index| index);

        if self
            .delayed
            .get(index)
            .is_some_and(|(held_ms, held_seq, _)| *held_ms == millis && *held_seq == sequence)
        {
            return;
        }

        self.delayed.insert(index, (millis, sequence, ready_at));
    }

    /// Entries held back from groups by a delayed add.
    pub(crate) fn held_count(&self) -> usize {
        self.delayed.len()
    }

    pub(crate) fn new_for_group(
        &mut self,
        name: &[u8],
        limit: usize,
        now: u64,
    ) -> Result<Vec<StreamEntry>, GroupError> {
        let group = self.group(name)?;
        let start = (group.last_millis, group.last_sequence);
        let cursor = StreamCursor::After(start.0, start.1);

        if self.delayed.is_empty() {
            return Ok(self.read_after(cursor, limit));
        }

        self.delayed.retain(|(_, _, ready_at)| *ready_at > now);

        // Held ids are few; read ahead by the limit plus the holds, so at most that many
        // entries are decoded.
        let mut ready = Vec::new();
        let mut after = start;

        while ready.len() < limit {
            let want = (limit - ready.len()).saturating_add(self.delayed.len());
            let chunk = self.entries.after(after.0, after.1, want);
            let exhausted = chunk.len() < want;

            if let Some(last) = chunk.last() {
                after = (last.millis, last.sequence);
            }

            ready.extend(chunk.into_iter().filter(|entry| {
                !self.delayed.iter().any(|(millis, sequence, _)| {
                    *millis == entry.millis && *sequence == entry.sequence
                })
            }));

            if exhausted {
                break;
            }
        }

        ready.truncate(limit);
        Ok(ready)
    }

    pub(crate) fn claim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
    ) -> Result<(), GroupError> {
        let group = self.group_mut(name)?;

        if group.pending.contains_key(&(millis, sequence)) {
            return Ok(());
        }

        if compare_id(millis, sequence, group.last_millis, group.last_sequence) != Ordering::Greater
        {
            return Ok(());
        }

        group.last_millis = millis;
        group.last_sequence = sequence;
        let added = group.put((millis, sequence), consumer, delivered_at, 1);

        self.account(added);
        Ok(())
    }

    pub(crate) fn reclaim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        update: ReclaimUpdate,
    ) -> Result<bool, GroupError> {
        let group = self.group_mut(name)?;
        let id = (update.millis, update.sequence);

        if !group.pending.contains_key(&id) {
            return Ok(false);
        }

        let added = group.put(id, consumer, update.delivered_at, update.deliveries);

        self.account(added);
        Ok(true)
    }

    /// Inserts a pending row without moving the group cursor. An existing row is replaced.
    pub(crate) fn upsert_pending(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        update: ReclaimUpdate,
    ) -> Result<(), GroupError> {
        let added = self.group_mut(name)?.put(
            (update.millis, update.sequence),
            consumer,
            update.delivered_at,
            update.deliveries,
        );

        self.account(added);
        Ok(())
    }

    /// Removes a pending delivery. Its consumer stays in the group.
    pub(crate) fn ack(
        &mut self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, GroupError> {
        let group = self.group_mut(name)?;
        let Some(delivery) = group.pending.remove(&(millis, sequence)) else {
            return Ok(false);
        };

        group
            .consumer_mut(delivery.consumer)
            .owned
            .remove(&(millis, sequence));
        self.pending_rows -= 1;
        Ok(true)
    }

    /// The pending delivery of one id, if the group has it.
    pub(crate) fn pending_delivery(
        &self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<Option<PendingDelivery>, GroupError> {
        let group = self.group(name)?;

        Ok(group
            .pending
            .get(&(millis, sequence))
            .map(|delivery| group.row((millis, sequence), delivery)))
    }

    pub(crate) fn is_pending(
        &self,
        name: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Result<bool, GroupError> {
        Ok(self.group(name)?.pending.contains_key(&(millis, sequence)))
    }

    pub(crate) fn pending_summary(&self, name: &[u8]) -> Result<PendingSummary, GroupError> {
        let group = self.group(name)?;

        Ok(PendingSummary {
            count: group.pending.len(),
            smallest: group.pending.first_key(),
            greatest: group.pending.last_key(),
            consumers: group
                .live_consumers()
                .filter(|consumer| !consumer.owned.is_empty())
                .map(|consumer| (consumer.name.clone(), consumer.owned.len() as i64))
                .collect(),
        })
    }

    pub(crate) fn pending_rows(
        &self,
        name: &[u8],
        filter: PendingFilter<'_>,
    ) -> Result<Vec<PendingRow>, GroupError> {
        let group = self.group(name)?;

        if filter.count == 0 {
            return Ok(Vec::new());
        }

        let start = match filter.start {
            StreamBound::Min => Bound::Unbounded,
            StreamBound::Max => return Ok(Vec::new()),
            StreamBound::Id(millis, sequence) => Bound::Included((millis, sequence)),
        };
        let end = match filter.end {
            StreamBound::Max => Bound::Unbounded,
            StreamBound::Min => return Ok(Vec::new()),
            StreamBound::Id(millis, sequence) => Bound::Included((millis, sequence)),
        };

        if let (Bound::Included(low), Bound::Included(high)) = (start, end)
            && low > high
        {
            return Ok(Vec::new());
        }

        let row = |id: Id, delivery: &Delivery| {
            let idle = filter.now.saturating_sub(delivery.delivered_at);

            (!filter.min_idle.is_some_and(|minimum| idle < minimum)).then(|| PendingRow {
                millis: id.0,
                sequence: id.1,
                consumer: group.consumer(delivery.consumer).name.clone(),
                idle,
                deliveries: delivery.deliveries,
            })
        };

        let rows = match filter.consumer {
            Some(consumer) => {
                let Some(slot) = group.find_consumer(consumer) else {
                    return Ok(Vec::new());
                };

                group
                    .consumer(slot)
                    .owned
                    .range((start, end))
                    .filter_map(|(id, ())| row(*id, &group.pending[id]))
                    .take(filter.count)
                    .collect()
            }
            None => group
                .pending
                .range((start, end))
                .filter_map(|(id, delivery)| row(*id, delivery))
                .take(filter.count)
                .collect(),
        };

        Ok(rows)
    }

    pub(crate) fn high_water(&self) -> (bool, u64, u64) {
        (self.started, self.last_millis, self.last_sequence)
    }

    pub(crate) fn holds(&self) -> &[(u64, u64, u64)] {
        &self.delayed
    }

    pub(crate) fn restore(
        started: bool,
        last_millis: u64,
        last_sequence: u64,
        entries: Vec<StreamEntry>,
        groups: Vec<ConsumerGroup>,
        delayed: Vec<(u64, u64, u64)>,
    ) -> Result<Self, &'static str> {
        if !started && !entries.is_empty() {
            return Err("stream snapshot has entries before it started");
        }

        let mut previous = None;

        for entry in &entries {
            if let Some((millis, sequence)) = previous
                && compare_id(entry.millis, entry.sequence, millis, sequence) != Ordering::Greater
            {
                return Err("stream snapshot entries are not ordered");
            }

            if started
                && compare_id(entry.millis, entry.sequence, last_millis, last_sequence)
                    == Ordering::Greater
            {
                return Err("stream snapshot entry exceeds the last id");
            }

            previous = Some((entry.millis, entry.sequence));
        }

        let mut blocks = Blocks::new();

        for entry in entries {
            let pairs: Vec<_> = entry.fields.iter().collect();

            blocks.push(entry.millis, entry.sequence, &pairs);
        }

        let mut imported: Vec<Group> = groups.into_iter().map(Group::import).collect();

        imported.sort_by(|left, right| left.name.cmp(&right.name));
        if imported.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err("duplicate stream group");
        }

        let payload_bytes = imported
            .iter()
            .fold(0usize, |total, group| total.saturating_add(group.payload()));

        let pending_rows = imported.iter().map(|group| group.pending.len()).sum();

        Ok(Self {
            started,
            last_millis,
            last_sequence,
            entries: blocks,
            groups: imported,
            delayed,
            payload_bytes,
            pending_rows,
        })
    }
}

/// Read access to a stream's entries in id order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Entries<'a>(&'a Blocks);

impl<'a> Entries<'a> {
    pub(crate) fn to_vec(self) -> Vec<StreamEntry> {
        self.0.to_vec()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamBound {
    Min,
    Max,
    Id(u64, u64),
}

/// Last id stored by `XGROUP CREATE`. `End` is `$` and is resolved to the current top.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupStart {
    Id(u64, u64),
    End,
}

/// Exclusive cursor for non-blocking `XREAD`. `End` is `$` and yields nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamCursor {
    After(u64, u64),
    End,
}

pub(crate) fn parse_id(bytes: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (millis, sequence) = text.split_once('-')?;

    Some((millis.parse().ok()?, sequence.parse().ok()?))
}

/// `millis-sequence`, written digit by digit: XADD replies with one per call and `format!`
/// cost more than the append itself.
pub(crate) fn format_id(millis: u64, sequence: u64) -> Vec<u8> {
    let (mut left, mut right) = ([0; 20], [0; 20]);
    let millis = crate::output::decimal(millis, &mut left);
    let sequence = crate::output::decimal(sequence, &mut right);
    let mut id = Vec::with_capacity(millis.len() + 1 + sequence.len());

    id.extend_from_slice(millis);
    id.push(b'-');
    id.extend_from_slice(sequence);
    id
}

fn compare_id(left_ms: u64, left_seq: u64, right_ms: u64, right_seq: u64) -> Ordering {
    left_ms.cmp(&right_ms).then(left_seq.cmp(&right_seq))
}

#[cfg(test)]
mod tests;
