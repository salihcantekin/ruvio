use super::arena_table::ArenaTable;
use super::smallbuf::{PackBuf, SmallBuf};
use super::*;
use crate::hashbrown::HashTable;

#[derive(Clone, Copy, Debug)]
pub(super) struct CompactLimits {
    pub(super) max_entries: usize,
    pub(super) max_value_bytes: usize,
}

pub(super) type HashFieldValue<'a> = (&'a [u8], &'a [u8]);

/// A field and its value, copied out of the store.
pub(crate) type OwnedPair = (Vec<u8>, Vec<u8>);

/// What a [`HashStream`] reply contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashStreamKind {
    Pairs,
    Fields,
    Values,
}

/// A hash being read: an object, or a small hash's packed bytes inside an index slab.
#[derive(Clone, Copy)]
pub(crate) enum HashRead<'a> {
    Value(&'a HashValue),
    Packed { data: &'a [u8], count: usize },
}

impl<'a> HashRead<'a> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Value(hash) => hash.len(),
            Self::Packed { count, .. } => count,
        }
    }

    pub(crate) fn get(self, field: &[u8]) -> Option<&'a [u8]> {
        match self {
            Self::Value(hash) => hash.get(field),
            Self::Packed { data, .. } => packed_pairs(data)
                .find(|(name, _)| *name == field)
                .map(|(_, value)| value),
        }
    }

    pub(crate) fn contains(self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    #[cfg(test)]
    pub(crate) fn is_compact(self) -> bool {
        match self {
            Self::Value(hash) => hash.is_compact(),
            Self::Packed { .. } => true,
        }
    }

    pub(crate) fn visit_pairs(self, mut visit: impl FnMut(HashFieldValue<'a>)) {
        match self {
            Self::Value(hash) => hash.visit_pairs(visit),
            Self::Packed { data, .. } => packed_pairs(data).for_each(&mut visit),
        }
    }

    pub(crate) fn iter_sorted(self) -> Vec<HashFieldValue<'a>> {
        let mut pairs = Vec::with_capacity(self.len());

        self.visit_pairs(|pair| pairs.push(pair));
        pairs.sort_unstable_by(|left, right| left.0.cmp(right.0));
        pairs
    }
}

/// Fields and values of one hash, borrowed from the store. Order is unspecified.
#[derive(Clone, Copy)]
pub struct HashStream<'a> {
    hash: Option<HashRead<'a>>,
}

impl<'a> HashStream<'a> {
    pub fn len(&self) -> usize {
        self.hash.map_or(0, HashRead::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn visit<E>(&self, mut visit: impl FnMut(&[u8], &[u8]) -> Result<(), E>) -> Result<(), E> {
        let Some(hash) = self.hash else {
            return Ok(());
        };
        let mut result = Ok(());

        hash.visit_pairs(|(field, value)| {
            if result.is_ok() {
                result = visit(field, value);
            }
        });
        result
    }
}

impl std::fmt::Debug for HashStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashStream({} fields)", self.len())
    }
}

impl PartialEq for HashStream<'_> {
    fn eq(&self, other: &Self) -> bool {
        let collect = |stream: &Self| {
            let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let _ = stream.visit::<()>(|field, value| {
                all.push((field.to_vec(), value.to_vec()));
                Ok(())
            });
            all.sort();
            all
        };

        collect(self) == collect(other)
    }
}

/// Native hash: a packed field/value buffer until size or value length requires a table.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct HashValue {
    pub(super) inner: HashInner,
    pub(super) payload_bytes: usize,
}

/// `Compact` keeps every pair in one buffer, listpack style: `len field len value`, where
/// `len` is one byte below 255 and `255` followed by a little-endian `u32` otherwise. Small
/// hashes then cost one allocation instead of two per pair.
///
/// `Arena` is the large form: each field and its value share one block in a chunked arena
/// and the table holds 4-byte offsets. `Table` is only the escape hatch for an arena that
/// has run out of offsets.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::type_complexity)]
pub(super) enum HashInner {
    Compact { bytes: SmallBuf, count: u32 },
    Arena(Box<ArenaTable<true>>),
    Table(Box<AHashMap<Box<[u8]>, Box<[u8]>>>),
}

impl Default for HashInner {
    fn default() -> Self {
        Self::Compact {
            bytes: SmallBuf::new(),
            count: 0,
        }
    }
}

pub(super) const PACKED_LONG_LEN: u8 = u8::MAX;

pub(super) fn packed_len_size(len: usize) -> usize {
    if len < PACKED_LONG_LEN as usize { 1 } else { 5 }
}

pub(super) fn put_packed(bytes: &mut impl PackBuf, item: &[u8]) {
    if item.len() < PACKED_LONG_LEN as usize {
        bytes.push(item.len() as u8);
    } else {
        bytes.push(PACKED_LONG_LEN);
        bytes.extend_from_slice(&(item.len() as u32).to_le_bytes());
    }

    bytes.extend_from_slice(item);
}

pub(super) fn encode_packed(item: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(packed_len_size(item.len()) + item.len());

    put_packed(&mut bytes, item);
    bytes
}

/// Reads one packed item at `at`. Returns the item and the offset just past it.
pub(super) fn read_packed(bytes: &[u8], at: usize) -> (&[u8], usize) {
    let (len, start) = if bytes[at] == PACKED_LONG_LEN {
        let mut raw = [0u8; 4];

        raw.copy_from_slice(&bytes[at + 1..at + 5]);
        (u32::from_le_bytes(raw) as usize, at + 5)
    } else {
        (bytes[at] as usize, at + 1)
    };

    (&bytes[start..start + len], start + len)
}

/// Grows a packed buffer by `extra` with bounded slack, so a small collection does not
/// double its capacity on every push the way `Vec` does.
pub(super) fn reserve_packed(bytes: &mut impl PackBuf, extra: usize) {
    if bytes.capacity() - bytes.len() < extra {
        bytes.reserve_exact(extra.max(bytes.len() / 4));
    }
}

/// One compact hash pair: where it starts, where its value starts and where it ends.
pub(super) struct PackedPair {
    pub(super) start: usize,
    pub(super) value_at: usize,
    pub(super) end: usize,
}

pub(super) struct PackedPairs<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) at: usize,
}

impl<'a> Iterator for PackedPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.bytes.len() {
            return None;
        }

        let (field, value_at) = read_packed(self.bytes, self.at);
        let (value, end) = read_packed(self.bytes, value_at);

        self.at = end;
        Some((field, value))
    }
}

pub(super) fn packed_pairs(bytes: &[u8]) -> PackedPairs<'_> {
    PackedPairs { bytes, at: 0 }
}

pub(super) fn find_packed_pair(bytes: &[u8], field: &[u8]) -> Option<PackedPair> {
    let mut at = 0;

    while at < bytes.len() {
        let (name, value_at) = read_packed(bytes, at);
        let (_, end) = read_packed(bytes, value_at);

        if name == field {
            return Some(PackedPair {
                start: at,
                value_at,
                end,
            });
        }

        at = end;
    }

    None
}

pub(super) enum HashWrite {
    Added(usize),
    Replaced { old_len: usize, new_len: usize },
}

pub(super) fn overwrite_boxed(existing: &mut Box<[u8]>, value: &[u8]) -> HashWrite {
    let old_len = existing.len();

    if old_len == value.len() {
        existing.copy_from_slice(value);
    } else {
        *existing = value.into();
    }

    HashWrite::Replaced {
        old_len,
        new_len: value.len(),
    }
}

pub(super) fn overwrite_packed(
    bytes: &mut impl PackBuf,
    pair: &PackedPair,
    value: &[u8],
) -> HashWrite {
    let (old, _) = read_packed(bytes, pair.value_at);
    let old_len = old.len();

    if old_len == value.len() {
        let start = pair.end - old_len;

        bytes[start..pair.end].copy_from_slice(value);
    } else {
        bytes.replace_range(pair.value_at..pair.end, &encode_packed(value));
    }

    HashWrite::Replaced {
        old_len,
        new_len: value.len(),
    }
}

impl HashValue {
    /// The slab record of a small hash: one packed buffer that fits a slot.
    pub(super) fn slab_record(&self) -> Option<crate::index::SlabBuf> {
        let HashInner::Compact { bytes, count } = &self.inner else {
            return None;
        };

        (!bytes.is_empty() && bytes.len() <= crate::index::COLL_DATA)
            .then(|| crate::index::SlabBuf::coll(crate::index::COLL_HASH, *count as usize, bytes))
    }

    /// Slab record length (header included) when this hash fits one.
    pub(super) fn slab_len(&self) -> Option<usize> {
        match &self.inner {
            HashInner::Compact { bytes, .. }
                if !bytes.is_empty() && bytes.len() <= crate::index::COLL_DATA =>
            {
                Some(3 + bytes.len())
            }
            _ => None,
        }
    }

    pub(super) fn from_slab(count: usize, data: &[u8]) -> Self {
        let payload_bytes = packed_pairs(data)
            .map(|(field, value)| field.len() + value.len())
            .sum();

        Self {
            inner: HashInner::Compact {
                bytes: SmallBuf::from_slice(data),
                count: count as u32,
            },
            payload_bytes,
        }
    }

    pub(super) fn from_pairs(pairs: Vec<(Vec<u8>, Vec<u8>)>, limits: CompactLimits) -> Self {
        let mut hash = Self::default();

        for (field, value) in pairs {
            hash.insert(&field, &value, limits);
        }

        hash
    }

    pub(super) fn len(&self) -> usize {
        match &self.inner {
            HashInner::Compact { count, .. } => *count as usize,
            HashInner::Arena(table) => table.len(),
            HashInner::Table(map) => map.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn arena(&self) -> Option<&ArenaTable<true>> {
        match &self.inner {
            HashInner::Arena(table) => Some(table),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn is_compact(&self) -> bool {
        matches!(self.inner, HashInner::Compact { .. })
    }

    /// A stored field other than `except` whose hash under `hasher` is `handle`. One probe of
    /// an arena table whose own hasher this is; a scan of a compact or boxed hash.
    pub(super) fn field_by_handle(
        &self,
        hasher: &ahash::RandomState,
        handle: u64,
        except: Option<&[u8]>,
    ) -> Option<&[u8]> {
        let scanned =
            |field: &[u8]| except != Some(field) && scanned_handle(hasher, field) == handle;

        match &self.inner {
            HashInner::Arena(table) => table.find_key(handle, |field| {
                except != Some(field) && hasher.hash_one(field) == handle
            }),
            HashInner::Compact { bytes, .. } => packed_pairs(bytes)
                .map(|(field, _)| field)
                .find(|field| scanned(field)),
            HashInner::Table(map) => map.keys().map(Box::as_ref).find(|field| scanned(field)),
        }
    }

    pub(super) fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match &self.inner {
            HashInner::Compact { bytes, .. } => packed_pairs(bytes)
                .find(|(name, _)| *name == field)
                .map(|(_, value)| value),
            HashInner::Arena(table) => table.get(field),
            HashInner::Table(map) => map.get(field).map(Box::as_ref),
        }
    }

    pub(super) fn contains(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    pub(super) fn apply_write(&mut self, write: HashWrite) -> bool {
        match write {
            HashWrite::Added(bytes) => {
                self.payload_bytes = self.payload_bytes.saturating_add(bytes);
                true
            }

            HashWrite::Replaced { old_len, new_len } => {
                self.payload_bytes = self
                    .payload_bytes
                    .saturating_sub(old_len)
                    .saturating_add(new_len);
                false
            }
        }
    }

    pub(super) fn insert(&mut self, field: &[u8], value: &[u8], limits: CompactLimits) -> bool {
        let too_large =
            field.len() > limits.max_value_bytes || value.len() > limits.max_value_bytes;

        if let HashInner::Compact { bytes, count } = &mut self.inner {
            if let Some(pair) = find_packed_pair(bytes, field) {
                if too_large {
                    self.promote();
                } else {
                    let write = overwrite_packed(bytes, &pair, value);

                    return self.apply_write(write);
                }
            } else if !too_large && (*count as usize) < limits.max_entries {
                let added = field.len().saturating_add(value.len());

                reserve_packed(
                    bytes,
                    packed_len_size(field.len()) + packed_len_size(value.len()) + added,
                );
                put_packed(bytes, field);
                put_packed(bytes, value);
                *count += 1;

                return self.apply_write(HashWrite::Added(added));
            }
        }

        if matches!(self.inner, HashInner::Compact { .. }) {
            self.promote();
        }

        if let HashInner::Arena(table) = &self.inner
            && !table.can_insert(field.len(), value.len())
        {
            self.spill();
        }

        let write = match &mut self.inner {
            HashInner::Arena(table) => {
                if let Some(old_len) = table.replace_value(field, value) {
                    HashWrite::Replaced {
                        old_len,
                        new_len: value.len(),
                    }
                } else {
                    table
                        .insert_new(field, value)
                        .expect("room checked before the insert");
                    HashWrite::Added(field.len().saturating_add(value.len()))
                }
            }

            HashInner::Table(map) => {
                if let Some(existing) = map.get_mut(field) {
                    overwrite_boxed(existing, value)
                } else {
                    let added = field.len().saturating_add(value.len());

                    map.insert(field.into(), value.into());
                    HashWrite::Added(added)
                }
            }

            HashInner::Compact { .. } => unreachable!("compact hash was promoted"),
        };

        self.apply_write(write)
    }

    pub(super) fn remove(&mut self, field: &[u8], limits: CompactLimits) -> bool {
        let removed_bytes = match &mut self.inner {
            HashInner::Compact { bytes, count } => find_packed_pair(bytes, field).map(|pair| {
                let (value, _) = read_packed(bytes, pair.value_at);
                let removed = field.len().saturating_add(value.len());

                bytes.drain_range(pair.start..pair.end);
                *count -= 1;
                removed
            }),
            HashInner::Arena(table) => table
                .remove(field)
                .map(|value_len| field.len().saturating_add(value_len)),
            HashInner::Table(map) => map
                .remove(field)
                .map(|value| field.len().saturating_add(value.len())),
        };

        let Some(removed_bytes) = removed_bytes else {
            return false;
        };

        self.payload_bytes = self.payload_bytes.saturating_sub(removed_bytes);
        self.maybe_demote(limits);

        if let HashInner::Compact { bytes, .. } = &mut self.inner
            && let Some(target) = reclaim_capacity(bytes.len(), bytes.capacity())
        {
            bytes.shrink_to(target);
        }

        if let HashInner::Table(map) = &mut self.inner
            && let Some(target) = reclaim_capacity(map.len(), map.capacity())
        {
            map.shrink_to(target);
        }

        true
    }

    pub(super) fn promote(&mut self) {
        let HashInner::Compact { bytes, .. } = &self.inner else {
            return;
        };

        let mut table = ArenaTable::<true>::with_capacity(self.len() * 2);

        for (field, value) in packed_pairs(bytes) {
            table
                .insert_new(field, value)
                .expect("a small hash fits the arena");
        }

        self.inner = HashInner::Arena(Box::new(table));
    }

    /// Moves an arena hash that ran out of offsets to the boxed map.
    fn spill(&mut self) {
        let HashInner::Arena(table) = &self.inner else {
            return;
        };

        let map = table
            .iter()
            .map(|(field, value)| (Box::<[u8]>::from(field), Box::<[u8]>::from(value)))
            .collect();

        self.inner = HashInner::Table(Box::new(map));
    }

    pub(super) fn maybe_demote(&mut self, limits: CompactLimits) {
        if matches!(self.inner, HashInner::Compact { .. }) || self.len() > limits.max_entries {
            return;
        }

        let mut oversize = false;
        let mut size = 0;

        self.visit_pairs(|(field, value)| {
            oversize |=
                field.len() > limits.max_value_bytes || value.len() > limits.max_value_bytes;
            size += packed_len_size(field.len())
                + field.len()
                + packed_len_size(value.len())
                + value.len();
        });

        if oversize {
            return;
        }

        let mut bytes = SmallBuf::with_capacity(size);

        self.visit_pairs(|(field, value)| {
            put_packed(&mut bytes, field);
            put_packed(&mut bytes, value);
        });

        self.inner = HashInner::Compact {
            bytes,
            count: self.len() as u32,
        };
    }

    pub(super) fn visit_pairs<'a>(&'a self, mut visit: impl FnMut(HashFieldValue<'a>)) {
        match &self.inner {
            HashInner::Compact { bytes, .. } => packed_pairs(bytes).for_each(&mut visit),
            HashInner::Arena(table) => table.iter().for_each(&mut visit),
            HashInner::Table(map) => {
                for (field, value) in map.iter() {
                    visit((field.as_ref(), value.as_ref()));
                }
            }
        }
    }

    pub(super) fn iter_sorted(&self) -> Vec<HashFieldValue<'_>> {
        let mut pairs = Vec::with_capacity(self.len());
        self.visit_pairs(|pair| pairs.push(pair));
        pairs.sort_unstable_by(|left, right| left.0.cmp(right.0));
        pairs
    }

    pub(super) fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    /// Bytes beyond the payload. A compact hash stores the payload inside its buffer.
    pub(super) fn collection_bytes(&self) -> usize {
        match &self.inner {
            HashInner::Compact { bytes, .. } => {
                bytes.heap_capacity().saturating_sub(self.payload_bytes)
            }
            HashInner::Arena(table) => table.capacity_bytes().saturating_sub(self.payload_bytes),
            HashInner::Table(map) => map.capacity().saturating_mul(
                size_of::<Box<[u8]>>()
                    .saturating_add(size_of::<Box<[u8]>>())
                    .saturating_add(1),
            ),
        }
    }
}

/// Most fields one maintenance pass removes from one hash, so a wide hash whose fields fall
/// due together is trimmed over several ticks instead of stalling one.
pub(super) const FIELDS_PER_SWEEP: usize = 1024;
/// How long one maintenance tick (every 100 ms) may spend on due hash fields once it has
/// visited `active_expire_keys` hashes: 5% of the shard's time at most.
const FIELD_SWEEP_BUDGET: std::time::Duration = std::time::Duration::from_millis(5);
/// The sweep reads the clock once per this many hashes.
const FIELD_SWEEP_CLOCK_EVERY: usize = 16;

/// Field deadlines of one timed hash, in one of three forms:
/// - `Few`: up to `FieldDeadlines::FEW` named pairs in a vector, whatever the encoding;
/// - `Heap`: past that, a min-heap that names each field by its hash, so no name is copied;
/// - `Named`: a name-keyed index, used only for a hash that spilled to the boxed map, which
///   cannot be probed by hash.
#[derive(Debug, Clone)]
pub(super) enum FieldDeadlines {
    Few(Vec<(Box<[u8]>, u64)>),
    Heap(Box<DeadlineHeap>),
    Named(Box<ManyDeadlines>),
}

/// The name-keyed wide form: one map from field to deadline and one set ordered by deadline.
/// Both share the field name, so a name is stored once.
#[derive(Debug, Clone, PartialEq, Default)]
pub(super) struct ManyDeadlines {
    by_field: AHashMap<Arc<[u8]>, u64>,
    by_deadline: BTreeSet<(u64, Arc<[u8]>)>,
    key_bytes: usize,
}

impl ManyDeadlines {
    fn insert(&mut self, field: &[u8], at: u64) -> Option<u64> {
        if let Some((name, existing)) = self.by_field.get_key_value(field) {
            let previous = *existing;

            if previous != at {
                let name = name.clone();

                self.by_deadline.remove(&(previous, name.clone()));
                self.by_deadline.insert((at, name.clone()));
                self.by_field.insert(name, at);
            }

            return Some(previous);
        }

        let name: Arc<[u8]> = Arc::from(field);

        self.key_bytes = self.key_bytes.saturating_add(field.len());
        self.by_deadline.insert((at, name.clone()));
        self.by_field.insert(name, at);
        None
    }

    fn remove(&mut self, field: &[u8]) -> Option<u64> {
        let (name, at) = self.by_field.remove_entry(field)?;

        self.key_bytes = self.key_bytes.saturating_sub(field.len());
        self.by_deadline.remove(&(at, name));

        if let Some(target) = reclaim_capacity(self.by_field.len(), self.by_field.capacity()) {
            self.by_field.shrink_to(target);
        }

        Some(at)
    }

    fn table_bytes(&self) -> usize {
        let slot = size_of::<Arc<[u8]>>()
            .saturating_add(size_of::<u64>())
            .saturating_add(1);
        // B-tree nodes run between half and fully used; charge them at two thirds.
        let ordered = size_of::<(u64, Arc<[u8]>)>().saturating_mul(3) / 2;
        // Each shared name carries the two reference counts of its `Arc`.
        let shared = size_of::<usize>().saturating_mul(2);

        size_of::<Self>()
            .saturating_add(self.key_bytes)
            .saturating_add(self.by_field.capacity().saturating_mul(slot))
            .saturating_add(
                self.by_field
                    .len()
                    .saturating_mul(ordered.saturating_add(shared)),
            )
    }
}

#[cfg(test)]
thread_local! {
    static HANDLE_MASK: std::cell::Cell<u64> = const { std::cell::Cell::new(u64::MAX) };
}

/// Narrows the handles of compact hashes on this thread until the guard drops, so tests can
/// make different fields share one.
#[cfg(test)]
pub(super) fn mask_handles_for_test(mask: u64) -> impl Drop {
    struct Restore(u64);

    impl Drop for Restore {
        fn drop(&mut self) {
            HANDLE_MASK.with(|current| current.set(self.0));
        }
    }

    Restore(HANDLE_MASK.with(|current| current.replace(mask)))
}

/// The handle of `field` in a hash that is not an arena table.
fn scanned_handle(hasher: &ahash::RandomState, field: &[u8]) -> u64 {
    let handle = hasher.hash_one(field);

    #[cfg(test)]
    let handle = handle & HANDLE_MASK.with(std::cell::Cell::get);

    handle
}

/// Children per node of [`DeadlineHeap`]: four halves the depth of a binary heap, and with it
/// the index updates of a sift.
const HEAP_ARITY: usize = 4;

/// Parks the index entry of a heap slot being sifted, so no other slot's position matches it.
const SIFTING: u32 = u32::MAX;

/// The wide form of field deadlines: a 4-ary min-heap of `(deadline, handle)`, where the
/// handle is the field's hash under `hasher`, and `index` from handle to heap position. The
/// index buckets hold only the 4-byte position and read the handle back from the heap, so a
/// timed field costs 16 bytes of heap plus about 5 bytes of index per bucket, and no copy of
/// its name.
///
/// While the hash is an arena table, `hasher` is a clone of the table's own, so a handle finds
/// its field with one probe of that table; a handle survives table growth, rehash and arena
/// compaction because it depends only on the field bytes. A compact hash is scanned instead,
/// which its entry limit bounds. Moving between the two encodings recomputes the handles
/// ([`TimedHash::rehandle`]); that only happens at compact sizes.
///
/// A handle in `index` belongs to exactly one stored field. A field whose hash equals another
/// stored field's keeps its deadline in `collided`, by name, so a 64-bit collision never mixes
/// two fields up.
#[derive(Debug, Clone)]
pub(super) struct DeadlineHeap {
    heap: Vec<(u64, u64)>,
    index: HashTable<u32>,
    hasher: ahash::RandomState,
    /// True when `hasher` is the arena table's, false for a compact hash.
    arena: bool,
    collided: Vec<(Box<[u8]>, u64)>,
}

impl DeadlineHeap {
    /// An empty heap for `hash`; `None` for the boxed map, which cannot be probed by hash.
    fn for_hash(hash: &HashValue) -> Option<Self> {
        let (hasher, arena) = match &hash.inner {
            HashInner::Arena(table) => (table.hasher().clone(), true),
            HashInner::Compact { .. } => (ahash::RandomState::new(), false),
            HashInner::Table(_) => return None,
        };

        Some(Self {
            heap: Vec::new(),
            index: HashTable::new(),
            hasher,
            arena,
            collided: Vec::new(),
        })
    }

    /// True while the handles were computed for the encoding `hash` has now.
    fn matches(&self, hash: &HashValue) -> bool {
        match &hash.inner {
            HashInner::Arena(_) => self.arena,
            HashInner::Compact { .. } => !self.arena,
            HashInner::Table(_) => false,
        }
    }

    fn handle(&self, field: &[u8]) -> u64 {
        if self.arena {
            self.hasher.hash_one(field)
        } else {
            scanned_handle(&self.hasher, field)
        }
    }

    fn position(&self, handle: u64) -> Option<usize> {
        let heap = &self.heap;

        self.index
            .find(handle, |&at| heap[at as usize].1 == handle)
            .map(|&at| at as usize)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty() && self.collided.is_empty()
    }

    fn earliest(&self) -> Option<u64> {
        let collided = self.collided.iter().map(|(_, at)| *at).min();

        match (self.heap.first().map(|(at, _)| *at), collided) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        }
    }

    /// The deadline of `field`, which the caller has checked is stored.
    fn get(&self, field: &[u8]) -> Option<u64> {
        if let Some((_, at)) = self
            .collided
            .iter()
            .find(|(name, _)| name.as_ref() == field)
        {
            return Some(*at);
        }

        self.position(self.handle(field))
            .map(|position| self.heap[position].0)
    }

    /// Stores `at` for `field`, which `hash` holds. Returns the previous deadline.
    fn insert(&mut self, hash: &HashValue, field: &[u8], at: u64) -> Option<u64> {
        if let Some((_, existing)) = self
            .collided
            .iter_mut()
            .find(|(name, _)| name.as_ref() == field)
        {
            return Some(std::mem::replace(existing, at));
        }

        let handle = self.handle(field);

        if let Some(position) = self.position(handle) {
            let previous = self.heap[position].0;

            self.heap[position].0 = at;

            if self.out_of_order(position) {
                self.relocate(handle, position as u32, SIFTING);

                let settled = self.restore(position);

                self.relocate(handle, SIFTING, settled as u32);
            }

            return Some(previous);
        }

        if hash
            .field_by_handle(&self.hasher, handle, Some(field))
            .is_some()
        {
            self.collided.push((field.into(), at));
            return None;
        }

        let position = self.heap.len();
        let heap = &mut self.heap;

        // Grows by a quarter, not double, so a wide heap carries little spare capacity.
        if heap.len() == heap.capacity() {
            heap.reserve_exact((heap.len() / 4).max(4));
        }

        heap.push((at, handle));

        // The new slot gets its index entry once it has settled, so the sift moves no entry
        // of its own.
        let settled = self.sift_up(position) as u32;
        let heap = &self.heap;

        self.index
            .insert_unique(handle, settled, |&at| heap[at as usize].1);
        None
    }

    /// Drops the deadline of `field`, which the hash holds or held until this removal.
    fn remove(&mut self, field: &[u8]) -> Option<u64> {
        if let Some(found) = self
            .collided
            .iter()
            .position(|(name, _)| name.as_ref() == field)
        {
            return Some(self.collided.swap_remove(found).1);
        }

        let position = self.position(self.handle(field))?;
        let (at, _) = self.remove_at(position);

        self.release_spare();
        Some(at)
    }

    /// Gives back heap and index capacity once most of it is unused.
    fn release_spare(&mut self) {
        if let Some(target) = reclaim_capacity(self.heap.len(), self.heap.capacity()) {
            self.heap.shrink_to(target);
        }

        if let Some(target) = reclaim_capacity(self.index.len(), self.index.capacity()) {
            let heap = &self.heap;

            self.index.shrink_to(target, |&at| heap[at as usize].1);
        }
    }

    /// Runs before `field`, which `hash` does not hold, is added: a timed field with the same
    /// handle moves to `collided`, so the handle keeps naming one field.
    fn before_new_field(&mut self, hash: &HashValue, field: &[u8]) {
        let handle = self.handle(field);
        let Some(position) = self.position(handle) else {
            return;
        };
        let Some(owner) = hash.field_by_handle(&self.hasher, handle, Some(field)) else {
            debug_assert!(false, "an indexed handle names a stored field");
            return;
        };
        let owner: Box<[u8]> = owner.into();
        let (at, _) = self.remove_at(position);

        self.collided.push((owner, at));
    }

    /// Points the index entry of the heap slot that was at `from` to `to`.
    fn relocate(&mut self, handle: u64, from: u32, to: u32) {
        if let Some(slot) = self.index.find_mut(handle, |&at| at == from) {
            *slot = to;
        }
    }

    fn remove_at(&mut self, position: usize) -> (u64, u64) {
        let removed = self.heap[position];

        if let Ok(entry) = self
            .index
            .find_entry(removed.1, |&at| at as usize == position)
        {
            entry.remove();
        }

        let last = self.heap.len() - 1;
        let moved = self.heap[last];

        self.heap[position] = moved;
        self.heap.pop();

        if position != last {
            // `last` is past the end now, so no other slot moves to it while this one sifts.
            let settled = self.restore(position);

            self.relocate(moved.1, last as u32, settled as u32);
        }

        removed
    }

    fn out_of_order(&self, position: usize) -> bool {
        let item = self.heap[position];
        let first = position * HEAP_ARITY + 1;
        let end = (first + HEAP_ARITY).min(self.heap.len());

        (position > 0 && item < self.heap[(position - 1) / HEAP_ARITY])
            || (first..end).any(|child| self.heap[child] < item)
    }

    /// Sifts the slot at `position` to its place and returns that place. Every other slot it
    /// passes has its index entry moved; the caller moves the sifted slot's own entry, whose
    /// current value must not be a position on the way.
    fn restore(&mut self, position: usize) -> usize {
        if position > 0 && self.heap[position] < self.heap[(position - 1) / HEAP_ARITY] {
            self.sift_up(position)
        } else {
            self.sift_down(position)
        }
    }

    fn sift_up(&mut self, start: usize) -> usize {
        let item = self.heap[start];
        let mut position = start;

        while position > 0 {
            let parent = (position - 1) / HEAP_ARITY;
            let above = self.heap[parent];

            if above <= item {
                break;
            }

            self.heap[position] = above;
            self.relocate(above.1, parent as u32, position as u32);
            position = parent;
        }

        self.heap[position] = item;
        position
    }

    fn sift_down(&mut self, start: usize) -> usize {
        let item = self.heap[start];
        let len = self.heap.len();
        let mut position = start;

        loop {
            let first = position * HEAP_ARITY + 1;

            if first >= len {
                break;
            }

            let end = (first + HEAP_ARITY).min(len);
            let child = (first + 1..end).fold(first, |best, next| {
                if self.heap[next] < self.heap[best] {
                    next
                } else {
                    best
                }
            });
            let below = self.heap[child];

            if below >= item {
                break;
            }

            self.heap[position] = below;
            self.relocate(below.1, child as u32, position as u32);
            position = child;
        }

        self.heap[position] = item;
        position
    }

    fn table_bytes(&self) -> usize {
        let collided: usize = self.collided.iter().map(|(name, _)| name.len()).sum();

        size_of::<Self>()
            .saturating_add(self.heap.capacity().saturating_mul(size_of::<(u64, u64)>()))
            .saturating_add(self.index.allocation_size())
            .saturating_add(
                self.collided
                    .capacity()
                    .saturating_mul(size_of::<(Box<[u8]>, u64)>()),
            )
            .saturating_add(collided)
    }

    #[cfg(test)]
    pub(super) fn check(&self, hash: &HashValue) {
        for (position, (at, handle)) in self.heap.iter().enumerate() {
            assert_eq!(
                self.position(*handle),
                Some(position),
                "index follows the heap"
            );

            if position > 0 {
                assert!(self.heap[(position - 1) / HEAP_ARITY] <= (*at, *handle));
            }

            assert!(
                hash.field_by_handle(&self.hasher, *handle, None).is_some(),
                "every handle names a stored field"
            );
        }

        assert_eq!(self.index.len(), self.heap.len());
        assert!(self.matches(hash));
    }
}

impl FieldDeadlines {
    pub(super) const FEW: usize = 32;

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Few(pairs) => pairs.is_empty(),
            Self::Heap(heap) => heap.is_empty(),
            Self::Named(many) => many.by_field.is_empty(),
        }
    }

    pub(super) fn earliest(&self) -> Option<u64> {
        match self {
            Self::Few(pairs) => pairs.iter().map(|(_, at)| *at).min(),
            Self::Heap(heap) => heap.earliest(),
            Self::Named(many) => many.by_deadline.first().map(|(at, _)| *at),
        }
    }

    /// How many fields have a deadline.
    fn len(&self) -> usize {
        match self {
            Self::Few(pairs) => pairs.len(),
            Self::Heap(heap) => heap.heap.len() + heap.collided.len(),
            Self::Named(many) => many.by_field.len(),
        }
    }

    /// The latest deadline. A scan of the heap form; called only once the earliest is due.
    fn latest(&self) -> Option<u64> {
        match self {
            Self::Few(pairs) => pairs.iter().map(|(_, at)| *at).max(),
            Self::Heap(heap) => heap
                .heap
                .iter()
                .map(|(at, _)| *at)
                .chain(heap.collided.iter().map(|(_, at)| *at))
                .max(),
            Self::Named(many) => many.by_deadline.last().map(|(at, _)| *at),
        }
    }

    pub(super) fn table_bytes(&self) -> usize {
        match self {
            Self::Few(pairs) => pairs
                .capacity()
                .saturating_mul(size_of::<(Box<[u8]>, u64)>())
                .saturating_add(pairs.iter().map(|(name, _)| name.len()).sum::<usize>()),
            Self::Heap(heap) => heap.table_bytes(),
            Self::Named(many) => many.table_bytes(),
        }
    }
}

/// A hash after its first field TTL: the same fields, plus one deadline per timed field.
/// A hash without field TTLs stays `Value::Hash` and pays nothing for this.
#[derive(Debug, Clone)]
pub struct TimedHash {
    pub(super) hash: HashValue,
    /// Absolute Unix milliseconds, one entry per field that has a TTL.
    pub(super) deadlines: FieldDeadlines,
    pub(super) soonest: u64,
}

impl PartialEq for TimedHash {
    fn eq(&self, other: &Self) -> bool {
        let mut same = self.hash == other.hash;

        if same {
            self.hash.visit_pairs(|(field, _)| {
                same = same && self.deadline(field) == other.deadline(field);
            });
        }

        same
    }
}

impl TimedHash {
    pub(super) fn new(hash: HashValue) -> Self {
        Self {
            hash,
            deadlines: FieldDeadlines::Few(Vec::new()),
            soonest: u64::MAX,
        }
    }

    pub(super) fn extra_bytes(&self) -> usize {
        size_of::<Self>().saturating_add(self.deadlines.table_bytes())
    }

    fn refresh_soonest(&mut self) {
        self.soonest = self.deadlines.earliest().unwrap_or(u64::MAX);
    }

    /// True when every field has a deadline at or before `now`: the hash reads as missing.
    /// Counting is exact because every deadline names a stored field: a field leaves the hash
    /// only through `remove` or `purge`, which drop its deadline with it, and a deadline is
    /// set only on a stored field.
    pub(super) fn all_fields_due(&self, now: u64) -> bool {
        let due = self.soonest <= now
            && self.deadlines.len() == self.hash.len()
            && self.deadlines.latest().is_some_and(|at| at <= now);

        debug_assert!(
            !due || self.deadlines_name_stored_fields(),
            "a deadline names a field the hash does not hold"
        );
        due
    }

    /// Every deadline belongs to a distinct stored field. A full check, for debug builds.
    #[cfg(any(test, debug_assertions))]
    fn deadlines_name_stored_fields(&self) -> bool {
        match &self.deadlines {
            FieldDeadlines::Few(pairs) => {
                pairs.iter().all(|(name, _)| self.hash.contains(name))
                    && pairs
                        .iter()
                        .map(|(name, _)| name)
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        == pairs.len()
            }

            FieldDeadlines::Heap(heap) => {
                heap.collided
                    .iter()
                    .all(|(name, _)| self.hash.contains(name))
                    && heap.heap.iter().all(|(_, handle)| {
                        self.hash
                            .field_by_handle(&heap.hasher, *handle, None)
                            .is_some()
                    })
            }

            FieldDeadlines::Named(many) => {
                many.by_field.keys().all(|name| self.hash.contains(name))
            }
        }
    }

    #[cfg(not(any(test, debug_assertions)))]
    fn deadlines_name_stored_fields(&self) -> bool {
        true
    }

    /// Absolute Unix milliseconds when `field` expires, or `None` without a TTL.
    pub(super) fn deadline(&self, field: &[u8]) -> Option<u64> {
        match &self.deadlines {
            FieldDeadlines::Few(pairs) => pairs
                .iter()
                .find(|(name, _)| name.as_ref() == field)
                .map(|(_, at)| *at),
            // A handle only names a stored field, so an absent one must not be looked up.
            FieldDeadlines::Heap(heap) => {
                self.hash.contains(field).then(|| heap.get(field)).flatten()
            }
            FieldDeadlines::Named(many) => many.by_field.get(field).copied(),
        }
    }

    /// Sets `at` on `field`, which the hash holds.
    pub(super) fn set_deadline(&mut self, field: &[u8], at: u64) {
        if let FieldDeadlines::Few(pairs) = &mut self.deadlines
            && pairs.len() == FieldDeadlines::FEW
            && !pairs.iter().any(|(name, _)| name.as_ref() == field)
        {
            self.widen();
        }

        match &mut self.deadlines {
            FieldDeadlines::Few(pairs) => {
                if let Some((_, existing)) =
                    pairs.iter_mut().find(|(name, _)| name.as_ref() == field)
                {
                    *existing = at;
                } else {
                    // One slot at a time: most timed hashes carry one or two deadlines.
                    pairs.reserve_exact(1);
                    pairs.push((field.into(), at));
                }
            }

            FieldDeadlines::Heap(heap) => {
                heap.insert(&self.hash, field, at);
            }

            FieldDeadlines::Named(many) => {
                many.insert(field, at);
            }
        }

        self.refresh_soonest();
    }

    /// Moves a full vector of deadlines to the wide form that fits the encoding.
    fn widen(&mut self) {
        let FieldDeadlines::Few(pairs) = &mut self.deadlines else {
            return;
        };
        let pairs = std::mem::take(pairs);

        self.deadlines = match DeadlineHeap::for_hash(&self.hash) {
            Some(mut heap) => {
                for (field, at) in &pairs {
                    heap.insert(&self.hash, field, *at);
                }

                FieldDeadlines::Heap(Box::new(heap))
            }

            None => {
                let mut many = Box::<ManyDeadlines>::default();

                for (field, at) in &pairs {
                    many.insert(field, *at);
                }

                FieldDeadlines::Named(many)
            }
        };
    }

    /// Drops the deadline of `field` without checking that the hash holds it. Returns it.
    fn forget(&mut self, field: &[u8]) -> Option<u64> {
        let previous = match &mut self.deadlines {
            FieldDeadlines::Few(pairs) => {
                let index = pairs.iter().position(|(name, _)| name.as_ref() == field)?;
                let at = pairs.swap_remove(index).1;

                if let Some(target) = reclaim_capacity(pairs.len(), pairs.capacity()) {
                    pairs.shrink_to(target);
                }

                at
            }

            FieldDeadlines::Heap(heap) => heap.remove(field)?,
            FieldDeadlines::Named(many) => many.remove(field)?,
        };

        self.refresh_soonest();
        Some(previous)
    }

    /// Drops the TTL on `field`. True when it had one.
    pub(super) fn clear_deadline(&mut self, field: &[u8]) -> bool {
        if matches!(self.deadlines, FieldDeadlines::Heap(_)) && !self.hash.contains(field) {
            return false;
        }

        self.forget(field).is_some()
    }

    /// Writes a field and keeps its TTL. True when the field is new.
    pub(super) fn insert(&mut self, field: &[u8], value: &[u8], limits: CompactLimits) -> bool {
        if let FieldDeadlines::Heap(heap) = &mut self.deadlines
            && !self.hash.contains(field)
        {
            heap.before_new_field(&self.hash, field);
        }

        let added = self.hash.insert(field, value, limits);

        self.after_reshape();
        added
    }

    /// Removes a field and its TTL. True when the field existed.
    pub(super) fn remove(&mut self, field: &[u8], limits: CompactLimits) -> bool {
        if !self.hash.contains(field) {
            return false;
        }

        // While the field is still stored, its handle still names it.
        self.forget(field);
        self.hash.remove(field, limits);
        self.after_reshape();
        true
    }

    /// Recomputes the handles after the hash changed encoding under the heap.
    fn after_reshape(&mut self) {
        if let FieldDeadlines::Heap(heap) = &self.deadlines
            && !heap.matches(&self.hash)
        {
            self.rehandle();
        }
    }

    /// Rebuilds the wide form for the current encoding. Bounded by the compact entry limit
    /// for promote and demote; a spill to the boxed map is a full copy already.
    fn rehandle(&mut self) {
        let FieldDeadlines::Heap(mut old) =
            std::mem::replace(&mut self.deadlines, FieldDeadlines::Few(Vec::new()))
        else {
            return;
        };
        let mut timed = std::mem::take(&mut old.collided);

        self.hash.visit_pairs(|(field, _)| {
            if let Some(position) = old.position(old.handle(field)) {
                timed.push((field.into(), old.heap[position].0));
            }
        });

        let wide = timed.len() > FieldDeadlines::FEW;

        self.deadlines = FieldDeadlines::Few(timed);

        if wide {
            self.widen();
        }

        self.refresh_soonest();
    }

    /// Removes up to `max` fields whose deadline is at or before `now`, earliest first in the
    /// wide forms. Returns how many.
    pub(super) fn purge(&mut self, now: u64, max: usize, limits: CompactLimits) -> usize {
        let mut name = Vec::new();
        let mut removed = 0;

        while removed < max && self.soonest <= now {
            name.clear();

            match &mut self.deadlines {
                FieldDeadlines::Few(pairs) => {
                    let Some((field, _)) = pairs.iter().find(|(_, at)| *at <= now) else {
                        break;
                    };

                    name.extend_from_slice(field);
                }

                FieldDeadlines::Named(many) => match many.by_deadline.first() {
                    Some((at, field)) if *at <= now => name.extend_from_slice(field),
                    _ => break,
                },

                FieldDeadlines::Heap(heap) => {
                    if let Some((field, _)) = heap.collided.iter().find(|(_, at)| *at <= now) {
                        name.extend_from_slice(field);
                    } else {
                        let Some(&(at, handle)) = heap.heap.first() else {
                            break;
                        };

                        if at > now {
                            break;
                        }

                        let found = self.hash.field_by_handle(&heap.hasher, handle, None);

                        debug_assert!(found.is_some(), "an indexed handle names a stored field");

                        if let Some(field) = found {
                            name.extend_from_slice(field);
                        }

                        // The top slot is this field's: drop it by position, without a lookup.
                        heap.remove_at(0);
                        heap.release_spare();

                        if !name.is_empty() && self.hash.remove(&name, limits) {
                            removed += 1;
                        }

                        self.refresh_soonest();
                        self.after_reshape();
                        continue;
                    }
                }
            }

            if self.remove(&name, limits) {
                removed += 1;
            } else {
                // Unreachable while every deadline names a stored field; dropping it still
                // ends the loop.
                debug_assert!(false, "a due deadline names a field the hash does not hold");
                self.forget(&name);
            }
        }

        removed
    }

    pub(super) fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        self.hash
            .iter_sorted()
            .into_iter()
            .map(|(field, value)| {
                (
                    field.to_vec(),
                    value.to_vec(),
                    self.deadline(field).unwrap_or(0),
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn check(&self) {
        if let FieldDeadlines::Heap(heap) = &self.deadlines {
            heap.check(&self.hash);
        }

        assert_eq!(self.soonest, self.deadlines.earliest().unwrap_or(u64::MAX));
        assert!(self.deadlines_name_stored_fields());
        assert!(self.deadlines.len() <= self.hash.len());
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub fn hash_set<F: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        pairs: &[(F, V)],
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);
        let limits = self.compact_limits();
        let (done, new_fields) = self.hash_set_in_slab(key, pairs, limits);
        let pairs = &pairs[done..];

        if pairs.is_empty() {
            return Ok(new_fields);
        }

        // One lookup: the charge before comes from the entry found (nothing for a new one).
        let mut created = false;
        let Some(entry) = self.entries.get_or_insert_with(key, || {
            created = true;
            Entry::fresh(Value::Hash(Box::default()))
        }) else {
            return Err(WrongType);
        };

        let before = if created {
            EntryCharge::default()
        } else {
            EntryCharge::for_entry(key, entry)
        };

        let (added, refile) = match &mut entry.value {
            Value::Hash(hash) => (
                pairs
                    .iter()
                    .filter(|(field, value)| hash.insert(field.as_ref(), value.as_ref(), limits))
                    .count(),
                None,
            ),
            value @ Value::TimedHash(_) => {
                let soonest = value.soonest_field_deadline();
                let added = pairs
                    .iter()
                    .filter(|(field, written)| {
                        value.hash_insert(field.as_ref(), written.as_ref(), limits)
                    })
                    .count();

                (added, Some((soonest, value.soonest_field_deadline())))
            }

            _ => return Err(WrongType),
        };

        let after = EntryCharge::for_entry(key, entry);

        if let Some((soonest, after)) = refile {
            self.refile_hash(key, soonest, after);
        }

        self.apply_entry_charge(before, after, key);
        self.entries.demote_collection(key);
        Ok(added + new_fields)
    }

    /// Deletes leading fields inside the key's slab record. Returns how many fields were handled
    /// and how many of them existed; the rest need the general path.
    fn hash_del_in_slab<T: AsRef<[u8]>>(&mut self, key: &[u8], fields: &[T]) -> (usize, usize) {
        let mut done = 0;
        let mut removed = 0;
        let mut first_len = 0usize;
        let mut last_len = 0usize;
        let mut changed = false;

        for field in fields {
            let mut used_before = 0usize;
            let mut used_after = 0usize;
            let mut hit = false;
            let touch = self
                .entries
                .touch_slab(key, crate::index::TAG_COLL_SLAB, |slot| {
                    used_before = usize::from(slot[2]);

                    let result = super::slab_ops::hash_del(slot, field.as_ref());

                    used_after = usize::from(slot[2]);
                    hit = result == Some(true);
                    result
                });

            if !matches!(touch, SlabTouch::Done(Some(_))) {
                break;
            }

            if hit {
                if !changed {
                    first_len = 3 + used_before;
                    changed = true;
                }

                last_len = 3 + used_after;
                removed += 1;
            }

            done += 1;
        }

        if changed {
            let charge = |len: usize| EntryCharge::slab(key, len);

            self.apply_entry_charge(charge(first_len), charge(last_len), key);
        }

        (done, removed)
    }

    /// Applies leading pairs inside the key's slab record. Returns how many pairs were applied
    /// and how many of them added a field; the rest need the general path.
    fn hash_set_in_slab<F: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        pairs: &[(F, V)],
        limits: CompactLimits,
    ) -> (usize, usize) {
        let mut done = 0;
        let mut added = 0;
        let mut first_len = 0usize;
        let mut last_len = 0usize;

        for (field, value) in pairs {
            let mut grew = false;
            let mut used_before = 0usize;
            let mut used_after = 0usize;
            let mut touch = SlabTouch::Absent;

            // A record that outgrows its slot moves up a size class and is retried once.
            for attempt in 0..2 {
                touch = self
                    .entries
                    .touch_slab(key, crate::index::TAG_COLL_SLAB, |slot| {
                        used_before = usize::from(slot[2]);

                        let result = super::slab_ops::hash_set(
                            slot,
                            field.as_ref(),
                            value.as_ref(),
                            limits.max_entries,
                            limits.max_value_bytes,
                        );

                        used_after = usize::from(slot[2]);
                        grew = result == Some(true);
                        result
                    });

                let needed = 3 + used_before + field.as_ref().len() + value.as_ref().len() + 10;

                if attempt == 0
                    && matches!(touch, SlabTouch::Done(None))
                    && self.entries.enlarge_slab(key, needed)
                {
                    continue;
                }

                break;
            }

            if !matches!(touch, SlabTouch::Done(Some(_))) {
                break;
            }

            if done == 0 {
                first_len = 3 + used_before;
            }

            last_len = 3 + used_after;
            added += usize::from(grew);
            done += 1;
        }

        if done > 0 {
            let charge = |len: usize| EntryCharge::slab(key, len);

            self.apply_entry_charge(charge(first_len), charge(last_len), key);
        }

        (done, added)
    }

    /// Writes one field and keeps its TTL, like `HINCRBY`.
    pub(crate) fn hash_set_keep_ttl(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<(), WrongType> {
        if self.field_deadline(key, field)?.is_none() {
            return self.hash_set(key, &[(field, value)]).map(|_| ());
        }

        self.edit_hash(key, |hash, limits| {
            hash.hash_insert_keep_deadline(field, value, limits);
        })
        .map(|_| ())
    }

    pub fn hash_strlen(&self, key: &[u8], field: &[u8]) -> Result<usize, WrongType> {
        Ok(self.hash_get(key, field)?.map_or(0, <[u8]>::len))
    }

    pub fn hash_get(&self, key: &[u8], field: &[u8]) -> Result<Option<&[u8]>, WrongType> {
        Ok(self.live_hash(key)?.and_then(|hash| hash.get(field)))
    }

    pub fn hash_delete<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        fields: &[T],
    ) -> Result<usize, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        let limits = self.compact_limits();
        let (done, removed_early) = self.hash_del_in_slab(key, fields);
        let fields = &fields[done..];

        if fields.is_empty() {
            return Ok(removed_early);
        }

        let before = self.entry_charge(key);
        let Some(entry) = self.entries.object_mut(key) else {
            return Ok(removed_early);
        };

        let (removed, remove_entry) = match &mut entry.value {
            Value::Hash(hash) => {
                let removed = fields
                    .iter()
                    .filter(|field| hash.remove(field.as_ref(), limits))
                    .count();

                (removed, hash.is_empty())
            }

            value @ Value::TimedHash(_) => {
                let soonest = value.soonest_field_deadline();
                let removed = fields
                    .iter()
                    .filter(|field| value.hash_remove(field.as_ref(), limits))
                    .count();
                let after = value.soonest_field_deadline();
                let empty = value.as_hash().is_none_or(HashValue::is_empty);

                self.refile_hash(key, soonest, after);
                (removed, empty)
            }

            _ => return Err(WrongType),
        };

        if remove_entry {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(before, key);
        self.entries.demote_collection(key);
        Ok(removed + removed_early)
    }

    pub fn hash_len(&self, key: &[u8]) -> Result<usize, WrongType> {
        Ok(self.live_hash(key)?.map_or(0, HashRead::len))
    }

    pub fn hash_exists(&self, key: &[u8], field: &[u8]) -> Result<bool, WrongType> {
        Ok(self
            .live_hash(key)?
            .is_some_and(|hash| hash.contains(field)))
    }

    /// A borrowed, unordered view of a hash for streaming a reply.
    pub fn hash_stream(&self, key: &[u8]) -> Result<HashStream<'_>, WrongType> {
        Ok(HashStream {
            hash: self.live_hash(key)?,
        })
    }

    pub fn hash_get_all(&self, key: &[u8]) -> Result<Vec<HashFieldValue<'_>>, WrongType> {
        Ok(self
            .live_hash(key)?
            .map_or_else(Vec::new, HashRead::iter_sorted))
    }

    pub fn hash_mget<T: AsRef<[u8]>>(
        &self,
        key: &[u8],
        fields: &[T],
    ) -> Result<Vec<Option<&[u8]>>, WrongType> {
        let Some(hash) = self.live_hash(key)? else {
            return Ok(fields.iter().map(|_| None).collect());
        };

        Ok(fields
            .iter()
            .map(|field| hash.get(field.as_ref()))
            .collect())
    }

    pub fn hash_scan(
        &self,
        key: &[u8],
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> Result<(u64, Vec<HashFieldValue<'_>>), WrongType> {
        let Some(hash) = self.live_hash(key)? else {
            return Ok((0, Vec::new()));
        };

        let matches = |field: &[u8]| {
            pattern.is_none_or(|pattern| crate::protocol::glob_match(pattern, field))
        };

        if let HashRead::Value(value) = hash
            && let Some(table) = value.arena()
        {
            let mut page = Vec::new();
            let next = table.scan_buckets(cursor, count.max(1), |field, value| {
                if matches(field) {
                    page.push((field, value));
                }
            });

            return Ok((next, page));
        }

        // A compact hash comes back whole with cursor 0, as in Redis, so a position
        // cursor never skips a field when an earlier one is removed between calls.
        if !matches!(hash, HashRead::Value(value) if matches!(value.inner, HashInner::Table(_))) {
            let mut page = Vec::with_capacity(hash.len());

            hash.visit_pairs(|(field, value)| {
                if matches(field) {
                    page.push((field, value));
                }
            });

            return Ok((0, page));
        }

        // A hash that spilled to the boxed table is paged in hash order.
        let HashRead::Value(value) = hash else {
            unreachable!("only a stored table hash reaches here");
        };
        let HashInner::Table(map) = &value.inner else {
            unreachable!("only a stored table hash reaches here");
        };
        let hasher = map.hasher();

        Ok(scan_by_hash(
            map.iter()
                .map(|(field, value)| (field.as_ref(), value.as_ref()))
                .filter(|(field, _)| matches(field)),
            |(field, _)| hasher.hash_one(field),
            cursor,
            count,
        ))
    }

    /// `HRANDFIELD`: a positive `count` is distinct fields, a negative one allows repeats.
    /// Arena hashes are sampled by bucket, so the cost follows `count`, not the hash size.
    pub(crate) fn hash_random_pairs(
        &self,
        key: &[u8],
        count: Option<i64>,
    ) -> Result<Vec<OwnedPair>, WrongType> {
        let Some(hash) = self.live_hash(key)? else {
            return Ok(Vec::new());
        };
        let take = match count {
            None => 1,
            Some(0) => return Ok(Vec::new()),
            Some(count) => usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX),
        };
        let repeats = count.is_none() || count.is_some_and(|count| count < 0);
        let mut picked = Vec::new();

        if let HashRead::Value(value) = hash
            && let Some(table) = value.arena()
        {
            let mut bytes = 0usize;

            table.sample(take, !repeats, |field, value| {
                if !repeats {
                    picked.push((field.to_vec(), value.to_vec()));
                    return;
                }

                let cost = field.len().saturating_add(value.len());

                if bytes.saturating_add(cost) > MAX_RANDOM_REPLY_BYTES {
                    return;
                }

                bytes += cost;
                picked.push((field.to_vec(), value.to_vec()));
            });

            return Ok(picked);
        }

        // Compact hashes are small; a spilled table has no bucket access and is walked once.
        let mut pairs = Vec::with_capacity(hash.len());

        hash.visit_pairs(|pair| pairs.push(pair));

        if pairs.is_empty() {
            return Ok(picked);
        }

        let positions = if repeats {
            (0..take).map(|_| random_below(pairs.len())).collect()
        } else {
            distinct_positions(pairs.len(), take)
        };

        let mut bytes = 0usize;

        for at in positions {
            let (field, value) = pairs[at];
            let cost = field.len().saturating_add(value.len());

            if repeats && bytes.saturating_add(cost) > MAX_RANDOM_REPLY_BYTES {
                break;
            }

            bytes = bytes.saturating_add(cost);
            picked.push((field.to_vec(), value.to_vec()));
        }

        Ok(picked)
    }

    pub fn hash_keys(&self, key: &[u8]) -> Result<Vec<&[u8]>, WrongType> {
        Ok(self.live_hash(key)?.map_or_else(Vec::new, |hash| {
            hash.iter_sorted()
                .into_iter()
                .map(|(field, _)| field)
                .collect()
        }))
    }

    pub fn hash_values(&self, key: &[u8]) -> Result<Vec<&[u8]>, WrongType> {
        Ok(self.live_hash(key)?.map_or_else(Vec::new, |hash| {
            hash.iter_sorted()
                .into_iter()
                .map(|(_, value)| value)
                .collect()
        }))
    }

    #[cfg(test)]
    pub(crate) fn hash_is_compact(&self, key: &[u8]) -> Result<Option<bool>, WrongType> {
        Ok(self.live_hash(key)?.map(HashRead::is_compact))
    }

    /// TTL of one field in Unix milliseconds. `Ok(None)` without a TTL or without the field.
    pub(crate) fn field_deadline(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Result<Option<u64>, WrongType> {
        Ok(self
            .live_hash_value(key)?
            .and_then(|value| value.field_deadline(field)))
    }

    /// For each field: `None` when the hash or field is missing, else the field's TTL.
    pub(crate) fn field_deadlines<T: AsRef<[u8]>>(
        &self,
        key: &[u8],
        fields: &[T],
    ) -> Result<Vec<Option<Option<u64>>>, WrongType> {
        let hash = self.live_hash(key)?;
        let value = self.live_hash_value(key)?;

        Ok(fields
            .iter()
            .map(|field| {
                let field = field.as_ref();

                hash.filter(|hash| hash.contains(field))
                    .map(|_| value.and_then(|value| value.field_deadline(field)))
            })
            .collect())
    }

    /// Sets `at` as the TTL of each existing field. Missing fields are skipped.
    pub(crate) fn hash_expire_fields<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        fields: &[T],
        at: u64,
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);

        Ok(self
            .edit_hash(key, |hash, _| {
                fields
                    .iter()
                    .filter(|field| {
                        let field = field.as_ref();
                        let present = hash.as_hash().is_some_and(|plain| plain.contains(field));

                        if present {
                            hash.set_field_deadline(field, at);
                        }

                        present
                    })
                    .count()
            })?
            .unwrap_or(0))
    }

    /// Drops the TTL of each field. Returns how many had one.
    pub(crate) fn hash_persist_fields<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        fields: &[T],
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);

        Ok(self
            .edit_hash(key, |hash, _| {
                fields
                    .iter()
                    .filter(|field| hash.clear_field_deadline(field.as_ref()))
                    .count()
            })?
            .unwrap_or(0))
    }

    /// True once any hash in this shard has a field TTL.
    pub(crate) fn has_field_ttls(&self) -> bool {
        self.field_sweep.is_some()
    }

    /// Removes due fields of `key` before a hash command reads or writes it. A shard that
    /// never set a field TTL returns after one null check.
    #[inline]
    pub(crate) fn expire_hash_fields(&mut self, key: &[u8]) {
        if self.field_sweep.is_some() {
            self.purge_hash_fields(key);
        }
    }

    #[cold]
    pub(super) fn purge_hash_fields(&mut self, key: &[u8]) {
        let now = self.now_unix_millis().get();
        let due = matches!(
            self.entries.lookup(key),
            Some(IndexRef::Object(entry))
                if entry.value.soonest_field_deadline().is_some_and(|at| at <= now)
        );

        if due {
            // A hash with no live field left is a missing key: removed like an expired one,
            // with its delete logged.
            if self.remove_expired(key) {
                return;
            }

            // A command reads every field it finds, so every due field goes now.
            let _ = self.without_access(key, |shard| {
                shard.edit_hash(key, |hash, limits| {
                    hash.purge_due_fields(now, usize::MAX, limits)
                })
            });
        }
    }

    /// Removes due hash fields on the maintenance tick: at least `active_expire_keys` hashes,
    /// then more while the tick's time budget lasts, so a backlog of due hashes drains in a
    /// few ticks; at most `FIELDS_PER_SWEEP` fields of each. A hash left with no live field
    /// is removed like an expired key, its delete logged. Returns how many hashes were
    /// trimmed or removed.
    pub fn expire_hash_fields_due(&mut self) -> usize {
        if self.field_sweep.is_none() {
            return 0;
        }

        let now = self.now_unix_millis().get();
        let previous = self.active;
        let mut trimmed = 0;
        let minimum = self.active_expire_keys.max(1);
        let started = std::time::Instant::now();
        let mut visited = 0usize;
        let mut unfinished = Vec::new();

        loop {
            if visited >= minimum
                && visited.is_multiple_of(FIELD_SWEEP_CLOCK_EVERY)
                && started.elapsed() >= FIELD_SWEEP_BUDGET
            {
                break;
            }

            visited += 1;

            let Some(sweep) = self.field_sweep.as_mut() else {
                break;
            };

            if sweep.first().is_none_or(|(at, _, _)| *at > now) {
                break;
            }

            let (at, database, key) = sweep.pop_first().expect("first was checked");

            self.enter(database);

            let current = match self.entries.lookup(&key) {
                Some(IndexRef::Object(entry)) => entry.value.soonest_field_deadline(),
                _ => None,
            };

            if current == Some(at) && self.remove_expired_with(&key, ExpireCause::Active) {
                // A replica keeps a dead hash until the primary's delete arrives. It stays
                // filed: reads find a hash dead only while some hash is in the sweep.
                if self.removals.keeps_expired() {
                    unfinished.push((at, database, key));
                } else {
                    trimmed += 1;
                }
                continue;
            }

            if current == Some(at) {
                let _ = self.without_access(&key, |shard| {
                    shard.edit_hash(&key, |hash, limits| {
                        hash.purge_due_fields(now, FIELDS_PER_SWEEP, limits)
                    })
                });

                // A capped pass can leave fields due at the same deadline; `edit_hash` does not
                // refile an unchanged soonest deadline, so file the key again here.
                let left = match self.entries.lookup(&key) {
                    Some(IndexRef::Object(entry)) => entry.value.soonest_field_deadline(),
                    _ => None,
                };

                // Filed again after the loop, so this tick does not come back to it.
                if left == Some(at) {
                    unfinished.push((at, database, key));
                }

                trimmed += 1;
            }
        }

        if !unfinished.is_empty() {
            self.field_sweep
                .get_or_insert_with(Box::default)
                .extend(unfinished);
        }

        self.enter(previous);

        if self
            .field_sweep
            .as_ref()
            .is_some_and(|sweep| sweep.is_empty())
        {
            self.field_sweep = None;
        }

        trimmed
    }

    /// Edits the hash at `key`, keeps the field sweep and the memory charge in step, and drops
    /// the key once the last field is gone.
    pub(super) fn edit_hash<R>(
        &mut self,
        key: &[u8],
        edit: impl FnOnce(&mut Value, CompactLimits) -> R,
    ) -> Result<Option<R>, WrongType> {
        match self.value_kind(key) {
            None => return Ok(None),
            Some(ValueKind::Hash) => {}
            Some(_) => return Err(WrongType),
        }

        let limits = self.compact_limits();
        let charge = self.entry_charge(key);
        let Some(entry) = self.entries.object_mut(key) else {
            return Err(WrongType);
        };

        let hash = &mut entry.value;
        let soonest = hash.soonest_field_deadline();
        let result = edit(hash, limits);
        let after = hash.soonest_field_deadline();
        let empty = hash.as_hash().is_none_or(HashValue::is_empty);

        self.refile_hash(key, soonest, after);

        if empty {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(charge, key);
        self.entries.demote_collection(key);
        Ok(Some(result))
    }

    /// Moves `key` in the field sweep from `before` to `after`, both earliest field deadlines.
    pub(super) fn refile_hash(&mut self, key: &[u8], before: Option<u64>, after: Option<u64>) {
        if before == after {
            return;
        }

        let database = self.active;

        if let Some(at) = before
            && let Some(sweep) = self.field_sweep.as_mut()
        {
            sweep.remove(&(at, database, Box::from(key)));
        }

        match after {
            Some(at) => {
                self.field_sweep.get_or_insert_with(Box::default).insert((
                    at,
                    database,
                    Box::from(key),
                ));
            }

            None if self
                .field_sweep
                .as_ref()
                .is_some_and(|sweep| sweep.is_empty()) =>
            {
                self.field_sweep = None;
            }

            None => {}
        }
    }

    /// Files a timed hash that arrived at `key` by rename, move, or restore.
    pub(super) fn refile_arrived_hash(&mut self, key: &[u8]) {
        let soonest = match self.entries.lookup(key) {
            Some(IndexRef::Object(entry)) => entry.value.soonest_field_deadline(),
            _ => None,
        };

        self.refile_hash(key, None, soonest);
    }

    /// True when any field of the hash carries its own TTL.
    pub fn hash_has_field_ttl(&self, key: &[u8]) -> bool {
        matches!(self.live_hash_value(key), Ok(Some(Value::TimedHash(_))))
    }

    pub(super) fn live_hash(&self, key: &[u8]) -> Result<Option<HashRead<'_>>, WrongType> {
        self.live_view(
            key,
            |value| match value {
                Value::Hash(_) | Value::TimedHash(_) => value.as_hash().map(HashRead::Value),
                _ => None,
            },
            |record| match record {
                IndexRef::CollSlab(slot) if slot[0] == crate::index::COLL_HASH => {
                    let (_, count, data) = crate::index::coll_parts(slot);

                    Some(HashRead::Packed { data, count })
                }
                _ => None,
            },
        )
    }

    /// The `Hash` or `TimedHash` value at `key`.
    pub(super) fn live_hash_value(&self, key: &[u8]) -> Result<Option<&Value>, WrongType> {
        match self.live_hit(key) {
            None => Ok(None),
            Some(IndexRef::Object(entry)) => match &entry.value {
                value @ (Value::Hash(_) | Value::TimedHash(_)) => Ok(Some(value)),
                _ => Err(WrongType),
            },
            // A slab hash has no field TTLs, so there is nothing to report for it.
            Some(IndexRef::CollSlab(slot)) if slot[0] == crate::index::COLL_HASH => Ok(None),
            Some(_) => Err(WrongType),
        }
    }
}
