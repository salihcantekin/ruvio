use std::{collections::BTreeMap, fmt::Debug, mem::size_of};

use ahash::RandomState;

use crate::chunked::Chunked;
use crate::slots::{MIGRATE_PER_ELEMENT, Slots};
use crate::store::{Entry, LogStats, Packed, PackedBytes, ValueLog};

/// Compile-time storage-index boundary for a shard.
pub trait StorageIndex: Debug + Send + 'static {
    fn with_capacity(capacity: usize) -> Self
    where
        Self: Sized;

    fn lookup(&self, key: &[u8]) -> Option<IndexRef<'_>>;
    fn object_mut(&mut self, key: &[u8]) -> Option<&mut Entry>;
    fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], IndexRef<'_>)>;
    /// What a lookup of `key` computes first (its hash), for [`Self::lookup_hinted`].
    fn lookup_hint(&self, _key: &[u8]) -> u64 {
        0
    }
    /// [`Self::lookup_hint`] that also has the CPU load where that lookup starts, so a
    /// command touching several keys overlaps their cache misses. Only a hint.
    fn prefetch(&self, key: &[u8]) -> u64 {
        self.lookup_hint(key)
    }
    /// [`Self::lookup`] given `hint`, which must be `lookup_hint(key)` of this index. A
    /// hint stays valid while the index changes: it depends only on the key.
    fn lookup_hinted(&self, key: &[u8], _hint: u64) -> Option<IndexRef<'_>> {
        self.lookup(key)
    }
    fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry>;
    fn remove(&mut self, key: &[u8]) -> Option<Entry>;

    /// Insert `create()` when the key is missing. `None` means the key exists
    /// and is not a collection.
    fn get_or_insert_with(
        &mut self,
        key: &[u8],
        create: impl FnOnce() -> Entry,
    ) -> Option<&mut Entry>;

    fn len(&self) -> usize;
    fn entries(&self) -> impl Iterator<Item = (&[u8], IndexRef<'_>)>;
    /// Slots for resumable eviction traversal. Empty hash buckets count as slots.
    fn eviction_slots(&self) -> usize {
        self.len()
    }

    fn eviction_keys(&self, start: usize, slots: usize) -> impl Iterator<Item = Option<&[u8]>> {
        self.entries()
            .skip(start)
            .take(slots)
            .map(|(key, _)| Some(key))
    }

    /// Eviction metadata (see [`Self::access_get`]) of the key in each slot that
    /// [`Self::eviction_keys`] walks, `None` for an empty slot or a key in `exclude`.
    fn eviction_access(
        &self,
        start: usize,
        slots: usize,
        exclude: &[&[u8]],
    ) -> impl Iterator<Item = Option<u32>> {
        self.eviction_keys(start, slots).map(move |key| {
            key.filter(|key| !exclude.contains(key))
                .map(|key| self.access_get(key))
        })
    }

    /// Resumable keyspace traversal for SCAN. Visits at most `keys` live keys and
    /// examines at most `slots` slots, then returns the cursor to resume from, or 0
    /// when the traversal is complete. A key present for the whole traversal is
    /// visited at least once; keys may repeat when the index was rebuilt in between.
    ///
    /// The default walks `entries()` by position: no ordering guarantee under writes.
    fn scan_slots<'a>(
        &'a self,
        cursor: u64,
        keys: usize,
        _slots: usize,
        visit: &mut dyn FnMut(&'a [u8], IndexRef<'a>, Option<u32>),
    ) -> u64 {
        let start = usize::try_from(cursor).unwrap_or(usize::MAX);
        let mut taken = 0usize;

        for (key, entry) in self.entries().skip(start).take(keys) {
            visit(key, entry, None);
            taken += 1;
        }

        if start.saturating_add(taken) >= self.len() {
            0
        } else {
            (start + taken) as u64
        }
    }

    fn into_values(self) -> impl Iterator<Item = Entry>
    where
        Self: Sized;

    fn charged_bytes(&self) -> usize;

    /// True when the index's own key log is close to its address limit.
    fn log_near_limit(&self) -> bool {
        false
    }

    /// Moves up to `budget` slots of an unfinished table migration. Returns true when
    /// none is left. Indexes that grow in one step have nothing to do.
    fn migrate_step(&mut self, _budget: usize) -> bool {
        true
    }

    /// Starts shrinking a table that deletes left mostly empty, for a delete burst that
    /// ended while an earlier resize was still running. It changes `charged_bytes`, as
    /// `migrate_step` does; the caller accounts for both. Returns true when a shrink
    /// started.
    fn shrink_if_sparse(&mut self) -> bool {
        false
    }

    /// Called before an admitted write under a memory limit. An index whose next
    /// insert would grow only because removals left tombstones may reuse those
    /// slots in place. Returns whether it did.
    fn reclaim_before_growth(&mut self) -> bool {
        false
    }

    /// Flags sparse key-log pages for draining. Returns the number flagged.
    fn plan_key_drain(&mut self) -> usize {
        0
    }

    /// Ends a key-log drain round; pages still flagged take allocations again.
    fn end_key_drain(&mut self) {}

    /// Walks up to `slots` table slots from `cursor` (a `scan_slots` cursor). For each
    /// log-backed string, `value(offset, len)` may move its span and return the new
    /// offset. With `keys`, records on draining key-log pages move too; `moved` sees
    /// `(key, old address, new address)` for each moved record whose TTL hint is set.
    /// Slots and hashes never change. Returns the next cursor, 0 when done.
    fn relocate_step(
        &mut self,
        _cursor: u64,
        _slots: usize,
        _keys: bool,
        _value: &mut dyn FnMut(u32, u16) -> Option<u32>,
        _moved: &mut dyn FnMut(&[u8], u32, u32),
    ) -> u64 {
        0
    }

    /// Page counters of the key log, when the index keeps one.
    fn log_stats(&self) -> LogStats {
        LogStats::default()
    }

    /// Slab pages and live records per size class, smallest class first.
    fn slab_stats(&self) -> Vec<SlabClassStats> {
        Vec::new()
    }

    fn touch_slab(
        &mut self,
        _key: &[u8],
        _kind: u16,
        _edit: impl FnOnce(&mut [u8]) -> Option<bool>,
    ) -> SlabTouch {
        SlabTouch::Absent
    }

    fn create_slab(&mut self, _key: &[u8], _kind: u16) -> bool {
        false
    }

    fn enlarge_slab(&mut self, _key: &[u8], _min_len: usize) -> bool {
        false
    }

    fn take_slab(&mut self, _key: &[u8]) -> Option<Vec<u8>> {
        None
    }

    fn heap_mut(&mut self, _key: &[u8]) -> Option<&mut Vec<u8>> {
        None
    }

    /// Turns a small list, hash, set or sorted set held in a slab into an ordinary object.
    /// Cheap no-op for every other key. `object_mut` and `get_or_insert_with` do this
    /// themselves, for lists and hashes only.
    fn promote_collection(&mut self, _key: &[u8]) {}

    /// Moves a small collection object back into a slab record. No-op when it does not fit.
    fn demote_collection(&mut self, _key: &[u8]) {}

    fn retarget_log(&mut self, _key: &[u8], _offset: u32, _len: u16) -> bool {
        false
    }

    /// Writes `value` when `key` is already an inline integer. The key record stays put.
    fn write_integer(&mut self, _key: &[u8], _value: i64) -> bool {
        false
    }

    /// Length of the index's key log, when it has one. Addresses from `key_address`
    /// and `scan_slots` are below it for keys that exist now.
    fn key_log_len(&self) -> Option<u32> {
        None
    }

    /// True while the index is still moving slots into a resized table. Slot
    /// positions are unsettled until it is false.
    fn is_growing(&self) -> bool {
        false
    }

    /// `lookup` plus the TTL hint of the same record: false means the key has no
    /// deadline, true means it might. Indexes without a hint always say true.
    fn lookup_ttl(&self, key: &[u8]) -> Option<(IndexRef<'_>, bool)> {
        self.lookup(key).map(|hit| (hit, true))
    }

    /// [`Self::lookup_ttl`] plus the record's address for [`Self::access_at`], when the
    /// index keeps a stable key log; `None` address otherwise.
    fn lookup_ttl_at(&self, key: &[u8]) -> Option<(IndexRef<'_>, bool, Option<u32>)> {
        self.lookup_ttl(key).map(|(hit, hint)| (hit, hint, None))
    }

    /// Records that `key` has a deadline.
    fn mark_ttl(&mut self, _key: &[u8]) {}

    /// Records that `key` has no deadline.
    fn clear_ttl(&mut self, _key: &[u8]) {}

    /// Address of `key` in this index, when the index keeps a stable key log.
    fn key_address(&self, _key: &[u8]) -> Option<u32> {
        None
    }

    fn key_bytes_at(&self, _address: u32) -> Option<&[u8]> {
        None
    }

    /// Eviction metadata of `key`; 0 means none recorded. Indexes without metadata
    /// storage always say 0.
    fn access_get(&self, _key: &[u8]) -> u32 {
        0
    }

    /// Writable eviction metadata of `key`, `None` when the key is absent or the
    /// index keeps none. Storage is allocated by the first call, so a shard that never
    /// tracks recency pays nothing.
    fn access_mut(&mut self, _key: &[u8]) -> Option<&mut u32> {
        None
    }

    /// [`Self::access_mut`] for the record at `address` from [`Self::lookup_ttl_at`], with
    /// no second lookup. The address holds until the index is next written.
    fn access_at(&mut self, _address: u32) -> Option<&mut u32> {
        None
    }

    /// Drops all eviction metadata and its storage.
    fn reset_access(&mut self) {}

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One slab size class: slot size in bytes, 4 KiB pages held, records stored.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlabClassStats {
    pub slot_bytes: usize,
    pub pages: u64,
    pub live: u64,
}

pub enum SlabTouch {
    Absent,
    Wrong,
    Done(Option<bool>),
}

/// Strings and counters are inline. Collections are a heap `Entry`.
#[derive(Clone, Copy, Debug)]
pub enum IndexRef<'a> {
    Log {
        offset: u32,
        len: u16,
    },
    Integer(i64),
    /// A heap string's buffer; its capacity is what it is charged.
    Heap(&'a Vec<u8>),
    SetSlab(&'a [u8]),
    SortedSlab(&'a [u8]),
    /// A small list or hash; see [`TAG_COLL_SLAB`].
    CollSlab(&'a [u8]),
    Object(&'a Entry),
}

pub(crate) const TAG_SET_SLAB: u16 = 5;
pub(crate) const TAG_ZSET_SLAB: u16 = 6;
/// Small lists and hashes: slot bytes `[subkind, item count, used length, packed bytes...]`.
pub(crate) const TAG_COLL_SLAB: u16 = 7;
/// Largest slab record of a set or sorted set.
pub(crate) const SLAB_SLOT: usize = 80;
/// Largest slab record of a list or hash.
pub(crate) const COLL_SLAB_MAX: usize = 256;
pub(crate) const COLL_LIST: u8 = 1;
pub(crate) const COLL_HASH: u8 = 2;
/// Packed bytes a collection slab record can hold.
pub(crate) const COLL_DATA: usize = COLL_SLAB_MAX - 3;

/// A slab record on the stack, ready for `SlabPages::alloc`.
pub(crate) struct SlabBuf {
    bytes: [u8; COLL_SLAB_MAX],
    len: usize,
}

impl SlabBuf {
    pub(crate) fn raw(src: &[u8]) -> Self {
        let mut bytes = [0u8; COLL_SLAB_MAX];

        bytes[..src.len()].copy_from_slice(src);
        Self {
            bytes,
            len: src.len(),
        }
    }

    pub(crate) fn coll(subkind: u8, count: usize, data: &[u8]) -> Self {
        let mut bytes = [0u8; COLL_SLAB_MAX];

        bytes[0] = subkind;
        bytes[1] = count as u8;
        bytes[2] = data.len() as u8;
        bytes[3..3 + data.len()].copy_from_slice(data);
        Self {
            bytes,
            len: 3 + data.len(),
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// The packed bytes and item count of a collection slab slot.
pub(crate) fn coll_parts(slot: &[u8]) -> (u8, usize, &[u8]) {
    let used = usize::from(slot[2]);

    (slot[0], usize::from(slot[1]), &slot[3..3 + used])
}

const TAG_LOG: u16 = 0;
const TAG_INTEGER: u16 = 1;
const TAG_HEAP: u16 = 2;
const TAG_OBJECT: u16 = 3;
// Record header, two little-endian u16 after the key length: `[kind:4 | ttl hint:1 | payload length:11]`.
//
// Kinds: 0 log string, 1 integer, 2 heap string, 3 object, 4 free (was an inline-value kind
// that was never written), 5 set slab, 6 sorted-set slab, 7 list/hash slab (sub-kind in the
// slot's first byte), 8..=15 free. A new stored type takes the next free kind, adds an
// `IndexRef` arm, and picks up the generic promote/demote ladder through `Entry::slab_body`.
const KIND_BITS: u16 = 4;
const KIND_SHIFT: u16 = 16 - KIND_BITS;
/// Set while the key may have a deadline record. Clear means it certainly has none.
/// Lets GET skip the deadline-table lookup for keys without one.
const TTL_HINT: u16 = 1 << (KIND_SHIFT - 1);
const LEN_MASK: u16 = TTL_HINT - 1;
/// No record carries more than 8 inline payload bytes.
const _: () = assert!(LEN_MASK >= 8);
const HEADER_LEN: u32 = 4;
/// Key-log bytes of a record besides its key, by what the record holds: a value-log
/// span (offset and length), an integer, or the id of a heap string, object or slab slot.
pub(crate) const RECORD_LOG: usize = HEADER_LEN as usize + 6;
pub(crate) const RECORD_INTEGER: usize = HEADER_LEN as usize + 8;
pub(crate) const RECORD_SLOT: usize = HEADER_LEN as usize + 4;
/// The table slot that holds a heap string's buffer, and one that holds an object.
pub(crate) const HEAP_SLOT: usize = size_of::<Option<Vec<u8>>>();
pub(crate) const OBJECT_SLOT: usize = size_of::<Option<Entry>>();

#[derive(Clone, Copy, Debug)]
pub(crate) struct Addr(pub(crate) u32);

/// Size probe for `tests/layout.rs`; the slot type itself stays private.
#[doc(hidden)]
pub const INDEX_SLOT_SIZE: usize = size_of::<Addr>();

/// 4 KiB pages: the memory charge grows by at most one page at a time.
const ACCESS_PAGE_BITS: usize = 10;
const ACCESS_PAGE: usize = 1 << ACCESS_PAGE_BITS;

/// Sparse paged `u32` cells: growth never copies or leaves slack, and a page nobody
/// wrote to is never allocated. Reads outside allocated pages are 0.
#[derive(Debug, Default)]
struct AccessCells {
    pages: Vec<Option<Box<[u32; ACCESS_PAGE]>>>,
    /// Bytes of the allocated pages and the page list, for the memory charge.
    bytes: usize,
}

impl AccessCells {
    fn get(&self, cell: usize) -> u32 {
        self.pages
            .get(cell >> ACCESS_PAGE_BITS)
            .and_then(Option::as_ref)
            .map_or(0, |page| page[cell & (ACCESS_PAGE - 1)])
    }

    /// Clears a cell without allocating.
    fn clear(&mut self, cell: usize) {
        if let Some(Some(page)) = self.pages.get_mut(cell >> ACCESS_PAGE_BITS) {
            page[cell & (ACCESS_PAGE - 1)] = 0;
        }
    }

    fn cell_mut(&mut self, cell: usize) -> &mut u32 {
        let index = cell >> ACCESS_PAGE_BITS;

        if self.pages.len() <= index {
            let listed = self.pages.capacity();

            self.pages.resize_with(index + 1, || None);
            self.bytes +=
                (self.pages.capacity() - listed) * size_of::<Option<Box<[u32; ACCESS_PAGE]>>>();
        }

        let page = self.pages[index].get_or_insert_with(|| {
            self.bytes += ACCESS_PAGE * size_of::<u32>();
            vec![0u32; ACCESS_PAGE].try_into().expect("page length")
        });

        &mut page[cell & (ACCESS_PAGE - 1)]
    }

    fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

#[derive(Debug)]
pub struct AHashIndex {
    keys: ValueLog,
    table: Slots,
    heaps: Chunked<Option<Vec<u8>>>,
    objects: Chunked<Option<Entry>>,
    slabs: SlabPages,
    free_heaps: Vec<u32>,
    free_objects: Vec<u32>,
    hasher: RandomState,
    /// Eviction metadata, one `u32` per 8 bytes of key log (`addr >> 3`: a record is at
    /// least 8 bytes, so starts never share a cell). Empty until recency is tracked.
    access: AccessCells,
}

const SCAN_SLOT_BITS: u32 = 40;
const SCAN_SLOT_MASK: u64 = (1 << SCAN_SLOT_BITS) - 1;
const SCAN_EPOCH_MASK: u32 = (1 << (64 - SCAN_SLOT_BITS)) - 1;

/// Slot sizes of the slab classes. The smallest hold the records of one-element lists, hashes,
/// sets and sorted sets; a record moves up a class when it outgrows its slot.
const CLASS_LEN: [usize; 15] = [
    16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256,
];
const CLASS_BITS: u32 = 4;
const CLASS_SHIFT: u32 = 32 - CLASS_BITS;
const CLASS_PAGE: usize = 4096;

const NO_SLOT: u16 = u16::MAX;

/// One class page and its free-slot list. Free slots link through their first two bytes.
#[derive(Debug)]
struct SlotPage {
    bytes: Option<Box<[u8]>>,
    live: u16,
    head: u16,
    /// The page sits on `open` or is `current`. Kept across release so `open` never
    /// holds it twice.
    listed: bool,
}

/// Fixed-size slots on 4 KiB pages. A page whose slots are all free goes back to the
/// allocator (except the last one in use) and its index is reused.
#[derive(Debug)]
struct ClassPool {
    slot: u32,
    per_page: u32,
    pages: Vec<SlotPage>,
    /// Page opened last, below every page of `open`: allocation takes from it while
    /// `open` is empty, so a pool that only grows keeps `open` unallocated.
    current: u32,
    /// Pages that may have a free slot; empty heads are dropped lazily.
    open: Vec<u32>,
    free_pages: Vec<u32>,
}

const NO_PAGE: u32 = u32::MAX;

impl ClassPool {
    fn new(slot: usize) -> Self {
        Self {
            slot: slot as u32,
            per_page: (CLASS_PAGE / slot) as u32,
            pages: Vec::new(),
            current: NO_PAGE,
            open: Vec::new(),
            free_pages: Vec::new(),
        }
    }

    fn place(&self, id: u32) -> (usize, usize) {
        let page = (id / self.per_page) as usize;
        let offset = (id % self.per_page) as usize * self.slot as usize;

        (page, offset)
    }

    fn link(&mut self, page: usize, at: u16, next: u16) {
        let offset = at as usize * self.slot as usize;
        let bytes = self.pages[page].bytes.as_mut().expect("live slab page");

        bytes[offset..offset + 2].copy_from_slice(&next.to_le_bytes());
    }

    fn next_of(&self, page: usize, at: u16) -> u16 {
        let offset = at as usize * self.slot as usize;
        let bytes = self.pages[page].bytes.as_ref().expect("live slab page");

        u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
    }

    fn open_page(&mut self) -> usize {
        let fresh = vec![0u8; (self.slot * self.per_page) as usize].into_boxed_slice();
        let page = if let Some(page) = self.free_pages.pop() {
            self.pages[page as usize].bytes = Some(fresh);
            page as usize
        } else {
            self.pages.push(SlotPage {
                bytes: Some(fresh),
                live: 0,
                head: NO_SLOT,
                listed: false,
            });
            self.pages.len() - 1
        };

        for at in 0..self.per_page as u16 {
            let next = if u32::from(at) + 1 == self.per_page {
                NO_SLOT
            } else {
                at + 1
            };

            self.link(page, at, next);
        }

        self.pages[page].head = 0;
        // `open` is empty here (see `take_page`), so no page is listed but this one.
        self.pages[page].listed = true;
        self.current = page as u32;

        page
    }

    /// A page with a free slot: the top of `open`, else `current`, else a new page.
    #[inline]
    fn take_page(&mut self) -> usize {
        if self.open.is_empty() {
            let current = self.current as usize;

            if let Some(entry) = self.pages.get(current)
                && entry.head != NO_SLOT
            {
                return current;
            }
        }

        self.take_page_slow()
    }

    #[cold]
    fn take_page_slow(&mut self) -> usize {
        loop {
            match self.open.last() {
                Some(&page) if self.pages[page as usize].head != NO_SLOT => break page as usize,
                Some(&page) => {
                    self.pages[page as usize].listed = false;
                    self.open.pop();
                }
                None => {
                    if let Some(entry) = self.pages.get_mut(self.current as usize) {
                        if entry.head != NO_SLOT {
                            break self.current as usize;
                        }

                        entry.listed = false;
                        self.current = NO_PAGE;
                    }

                    break self.open_page();
                }
            }
        }
    }

    fn alloc(&mut self, src: &[u8]) -> u32 {
        let page = self.take_page();

        let at = self.pages[page].head;

        self.pages[page].head = self.next_of(page, at);
        self.pages[page].live += 1;

        let id = page as u32 * self.per_page + u32::from(at);
        let slot = self.slot_mut(id);

        slot[..src.len()].copy_from_slice(src);
        slot[src.len()..].fill(0);
        id
    }

    fn slot(&self, id: u32) -> &[u8] {
        let (page, offset) = self.place(id);

        &self.pages[page].bytes.as_ref().expect("live slab page")
            [offset..offset + self.slot as usize]
    }

    fn slot_mut(&mut self, id: u32) -> &mut [u8] {
        let (page, offset) = self.place(id);
        let len = self.slot as usize;

        &mut self.pages[page].bytes.as_mut().expect("live slab page")[offset..offset + len]
    }

    fn free(&mut self, id: u32) {
        let page = (id / self.per_page) as usize;
        let at = (id % self.per_page) as u16;
        let in_use = self.pages_in_use();
        let entry = &mut self.pages[page];

        entry.live -= 1;

        if entry.live == 0 && in_use > 1 {
            entry.bytes = None;
            entry.head = NO_SLOT;
            self.free_pages.push(page as u32);

            return;
        }

        let head = entry.head;

        entry.head = at;

        if !entry.listed {
            entry.listed = true;
            self.open.push(page as u32);
        }

        self.link(page, at, head);
    }

    fn pages_in_use(&self) -> usize {
        self.pages.len() - self.free_pages.len()
    }

    fn live_slots(&self) -> u64 {
        self.pages
            .iter()
            .filter(|page| page.bytes.is_some())
            .map(|page| u64::from(page.live))
            .sum()
    }
}

#[derive(Debug)]
struct SlabPages {
    // Boxed so the shard handle stays small (tests/layout.rs).
    classes: Box<[ClassPool; CLASS_LEN.len()]>,
}

impl Default for SlabPages {
    fn default() -> Self {
        Self {
            classes: Box::new(std::array::from_fn(|class| {
                ClassPool::new(CLASS_LEN[class])
            })),
        }
    }
}

impl SlabPages {
    fn class_for(len: usize) -> Option<usize> {
        CLASS_LEN.iter().position(|slot| len <= *slot)
    }

    fn pack(class: usize, id: u32) -> u32 {
        ((class as u32) << CLASS_SHIFT) | id
    }

    fn class_of(id: u32) -> usize {
        (id >> CLASS_SHIFT) as usize
    }

    fn local(id: u32) -> u32 {
        id & ((1 << CLASS_SHIFT) - 1)
    }

    fn alloc(&mut self, src: &[u8]) -> u32 {
        let class = Self::class_for(src.len()).expect("slab record fits a slot");

        self.alloc_class(class, src)
    }

    fn alloc_class(&mut self, class: usize, src: &[u8]) -> u32 {
        let id = self.classes[class].alloc(src);

        Self::pack(class, id)
    }

    fn slot(&self, id: u32) -> &[u8] {
        self.classes[Self::class_of(id)].slot(Self::local(id))
    }

    fn slot_mut(&mut self, id: u32) -> &mut [u8] {
        let class = Self::class_of(id);
        let local = Self::local(id);

        self.classes[class].slot_mut(local)
    }

    fn free(&mut self, id: u32) {
        self.classes[Self::class_of(id)].free(Self::local(id));
    }

    fn enlarge(&mut self, id: u32, min_len: usize) -> Option<u32> {
        let next = Self::class_for(min_len)?;
        let class = Self::class_of(id);

        if next <= class {
            return None;
        }

        let old = self.slot(id).to_vec();
        let stored = self.alloc_class(next, &old);

        self.free(id);
        Some(stored)
    }
}

impl AHashIndex {
    #[cfg(test)]
    pub(crate) fn slab_pages_in_use(&self) -> usize {
        self.slabs.classes.iter().map(ClassPool::pages_in_use).sum()
    }
}

impl Default for AHashIndex {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl AHashIndex {
    fn decode(keys: &ValueLog, addr: Addr) -> (u16, u16, u16) {
        let raw = keys.slice(addr.0, HEADER_LEN as u16);
        let key_len = u16::from_le_bytes([raw[0], raw[1]]);
        let meta = u16::from_le_bytes([raw[2], raw[3]]);

        (key_len, meta >> KIND_SHIFT, meta & LEN_MASK)
    }

    fn ttl_hint(keys: &ValueLog, addr: Addr) -> bool {
        let raw = keys.slice(addr.0, HEADER_LEN as u16);

        u16::from_le_bytes([raw[2], raw[3]]) & TTL_HINT != 0
    }

    fn set_ttl_hint(keys: &mut ValueLog, addr: Addr, on: bool) {
        let raw = keys.slice(addr.0, HEADER_LEN as u16);
        let mut meta = u16::from_le_bytes([raw[2], raw[3]]);

        if on {
            meta |= TTL_HINT;
        } else {
            meta &= !TTL_HINT;
        }

        keys.write_at(addr.0 + 2, &meta.to_le_bytes());
    }

    fn payload_at(addr: Addr, key_len: u16) -> u32 {
        addr.0 + HEADER_LEN + u32::from(key_len)
    }

    fn occupied(key_len: u16, payload_len: u16) -> u32 {
        HEADER_LEN + u32::from(key_len) + u32::from(payload_len)
    }

    fn write_header(keys: &mut ValueLog, addr: Addr, key_len: u16, kind: u16, payload_len: u16) {
        let meta = (kind << KIND_SHIFT) | payload_len;
        let mut header = [0u8; 4];

        header[0..2].copy_from_slice(&key_len.to_le_bytes());
        header[2..4].copy_from_slice(&meta.to_le_bytes());
        keys.write_at(addr.0, &header);
    }

    fn alloc_record(&mut self, key: &[u8], kind: u16, payload: &[u8]) -> Addr {
        let addr = Addr(self.keys.alloc_span(4 + key.len() + payload.len()));

        self.access.clear((addr.0 >> 3) as usize);

        Self::write_header(
            &mut self.keys,
            addr,
            key.len() as u16,
            kind,
            payload.len() as u16,
        );
        self.keys.write_at(addr.0 + HEADER_LEN, key);
        self.keys
            .write_at(Self::payload_at(addr, key.len() as u16), payload);
        addr
    }

    /// The eviction cells, once recency is tracked: the pages actually allocated, so a
    /// first read on a large keyspace adds one page, not the whole span. Capped at the
    /// cells of the live records plus a page: pages under key-log space that deletes
    /// emptied are not freed, and charging them would leave a charge eviction cannot
    /// lower. A read may allocate a page; the shard charges it at its next write.
    #[inline]
    fn access_charge(&self) -> usize {
        // No cells without recency tracking: one compare on the write path.
        if self.access.bytes == 0 {
            return 0;
        }

        let live = self.keys.live_bytes() as usize / 8 * size_of::<u32>();

        self.access.bytes.min(live + ACCESS_PAGE * size_of::<u32>())
    }

    fn access_cell(&self, addr: Addr) -> u32 {
        self.access.get((addr.0 >> 3) as usize)
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    fn hash_record(keys: &ValueLog, hasher: &RandomState, addr: &Addr) -> u64 {
        let (key_len, _, _) = Self::decode(keys, *addr);

        hasher.hash_one(keys.slice(addr.0 + HEADER_LEN, key_len))
    }

    fn same_key(keys: &ValueLog, addr: &Addr, key: &[u8]) -> bool {
        let (key_len, _, _) = Self::decode(keys, *addr);

        keys.slice(addr.0 + HEADER_LEN, key_len) == key
    }

    fn read_u32(keys: &ValueLog, offset: u32) -> u32 {
        let raw = keys.slice(offset, 4);

        u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]])
    }

    fn view<'a>(
        addr: Addr,
        keys: &'a ValueLog,
        heaps: &'a Chunked<Option<Vec<u8>>>,
        objects: &'a Chunked<Option<Entry>>,
        slabs: &'a SlabPages,
    ) -> IndexRef<'a> {
        let (key_len, kind, _) = Self::decode(keys, addr);
        let payload = Self::payload_at(addr, key_len);

        match kind {
            TAG_LOG => {
                let raw = keys.slice(payload, 6);
                let offset = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                let len = u16::from_le_bytes([raw[4], raw[5]]);

                IndexRef::Log { offset, len }
            }

            TAG_INTEGER => {
                let raw = keys.slice(payload, 8);
                let mut bytes = [0u8; 8];

                bytes.copy_from_slice(raw);
                IndexRef::Integer(i64::from_le_bytes(bytes))
            }

            TAG_HEAP => IndexRef::Heap(
                heaps[Self::read_u32(keys, payload) as usize]
                    .as_ref()
                    .expect("heap value"),
            ),
            TAG_OBJECT => IndexRef::Object(
                objects[Self::read_u32(keys, payload) as usize]
                    .as_ref()
                    .expect("collection"),
            ),
            TAG_SET_SLAB => IndexRef::SetSlab(slabs.slot(Self::read_u32(keys, payload))),
            TAG_ZSET_SLAB => IndexRef::SortedSlab(slabs.slot(Self::read_u32(keys, payload))),
            TAG_COLL_SLAB => IndexRef::CollSlab(slabs.slot(Self::read_u32(keys, payload))),
            _ => unreachable!("record kind"),
        }
    }

    fn alloc_slot<T>(slots: &mut Chunked<Option<T>>, free: &mut Vec<u32>, value: T) -> u32 {
        if let Some(index) = free.pop() {
            slots[index as usize] = Some(value);

            return index;
        }

        let index = slots.len() as u32;

        slots.push(Some(value));
        index
    }

    fn take_heap(&mut self, index: u32) -> Vec<u8> {
        self.free_heaps.push(index);
        self.heaps[index as usize].take().expect("heap value")
    }

    fn find_addr(&self, key: &[u8]) -> Option<Addr> {
        let hash = self.hash_key(key);

        self.table
            .find(hash, |record| Self::same_key(&self.keys, record, key))
            .copied()
    }

    /// Rewrites the record's kind and 4-byte payload id in place, keeping the TTL hint.
    fn retag(&mut self, addr: Addr, key_len: u16, kind: u16, id: u32) {
        let hint = Self::ttl_hint(&self.keys, addr);

        Self::write_header(&mut self.keys, addr, key_len, kind, 4);

        if hint {
            Self::set_ttl_hint(&mut self.keys, addr, true);
        }

        self.keys
            .write_at(Self::payload_at(addr, key_len), &id.to_le_bytes());
    }

    fn promote_at(&mut self, addr: Addr) {
        let (key_len, kind, _) = Self::decode(&self.keys, addr);

        if !matches!(kind, TAG_COLL_SLAB | TAG_SET_SLAB | TAG_ZSET_SLAB) {
            return;
        }

        let id = Self::read_u32(&self.keys, Self::payload_at(addr, key_len));
        let slot = self.slabs.slot(id);
        let entry = match kind {
            TAG_COLL_SLAB => Entry::from_slab_coll(slot),
            TAG_SET_SLAB => Entry::from_slab_set(slot),
            _ => Entry::from_slab_sorted(slot),
        };

        self.slabs.free(id);

        let index = Self::alloc_slot(&mut self.objects, &mut self.free_objects, entry);

        self.retag(addr, key_len, TAG_OBJECT, index);
    }

    fn take_object(&mut self, index: u32) -> Entry {
        self.free_objects.push(index);
        self.objects[index as usize].take().expect("collection")
    }

    fn unpack(&mut self, addr: Addr) -> Entry {
        let (key_len, kind, _) = Self::decode(&self.keys, addr);
        let payload = Self::payload_at(addr, key_len);

        match kind {
            TAG_LOG => {
                let raw = self.keys.slice(payload, 6);
                let offset = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                let len = u16::from_le_bytes([raw[4], raw[5]]);

                Entry::from_packed_bytes(PackedBytes::Log { offset, len })
            }

            TAG_INTEGER => {
                let raw = self.keys.slice(payload, 8);
                let mut bytes = [0u8; 8];

                bytes.copy_from_slice(raw);
                Entry::from_packed_bytes(PackedBytes::Integer(i64::from_le_bytes(bytes)))
            }

            TAG_HEAP => Entry::from_packed_bytes(PackedBytes::Heap(
                self.take_heap(Self::read_u32(&self.keys, payload)),
            )),
            TAG_OBJECT => self.take_object(Self::read_u32(&self.keys, payload)),
            TAG_SET_SLAB => {
                let id = Self::read_u32(&self.keys, payload);
                let entry = Entry::from_slab_set(self.slabs.slot(id));

                self.slabs.free(id);
                entry
            }

            TAG_ZSET_SLAB => {
                let id = Self::read_u32(&self.keys, payload);
                let entry = Entry::from_slab_sorted(self.slabs.slot(id));

                self.slabs.free(id);
                entry
            }

            TAG_COLL_SLAB => {
                let id = Self::read_u32(&self.keys, payload);
                let entry = Entry::from_slab_coll(self.slabs.slot(id));

                self.slabs.free(id);
                entry
            }

            _ => unreachable!("record kind"),
        }
    }

    fn write_ttl_hint(&mut self, key: &[u8], on: bool) {
        let hash = self.hash_key(key);
        let found = self
            .table
            .find(hash, |record| Self::same_key(&self.keys, record, key))
            .copied();

        if let Some(addr) = found {
            Self::set_ttl_hint(&mut self.keys, addr, on);
        }
    }

    fn store_entry(&mut self, key: &[u8], entry: Entry) -> Addr {
        let mut inline = [0u8; 8];
        let (kind, len) = match entry.into_packed() {
            Packed::Bytes(PackedBytes::Log { offset, len }) => {
                inline[0..4].copy_from_slice(&offset.to_le_bytes());
                inline[4..6].copy_from_slice(&len.to_le_bytes());
                (TAG_LOG, 6)
            }

            Packed::Bytes(PackedBytes::Integer(value)) => {
                inline = value.to_le_bytes();
                (TAG_INTEGER, 8)
            }

            Packed::Bytes(PackedBytes::Heap(data)) => {
                let index = Self::alloc_slot(&mut self.heaps, &mut self.free_heaps, data);

                inline[0..4].copy_from_slice(&index.to_le_bytes());
                (TAG_HEAP, 4)
            }

            Packed::Object(entry) => {
                if let Some((kind, bytes)) = entry.slab_body() {
                    let id = self.slabs.alloc(bytes.as_slice());

                    inline[0..4].copy_from_slice(&id.to_le_bytes());
                    (kind, 4)
                } else {
                    let index = Self::alloc_slot(&mut self.objects, &mut self.free_objects, entry);

                    inline[0..4].copy_from_slice(&index.to_le_bytes());
                    (TAG_OBJECT, 4)
                }
            }
        };

        self.alloc_record(key, kind, &inline[..len])
    }
}

impl StorageIndex for AHashIndex {
    fn log_near_limit(&self) -> bool {
        self.keys.near_limit()
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: ValueLog::default(),
            table: Slots::with_capacity(capacity),
            heaps: Chunked::new(),
            objects: Chunked::new(),
            slabs: SlabPages::default(),
            free_heaps: Vec::new(),
            free_objects: Vec::new(),
            // Unit tests that depend on slot placement (eviction sampling) need
            // the same layout on every run.
            #[cfg(test)]
            hasher: RandomState::with_seeds(42, 2, 3, 4),
            #[cfg(not(test))]
            hasher: RandomState::new(),
            access: AccessCells::default(),
        }
    }

    #[inline]
    fn lookup(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        self.lookup_hinted(key, self.hash_key(key))
    }

    #[inline]
    fn lookup_hint(&self, key: &[u8]) -> u64 {
        self.hash_key(key)
    }

    #[inline]
    fn prefetch(&self, key: &[u8]) -> u64 {
        let hash = self.hash_key(key);

        self.table.prefetch(hash);
        hash
    }

    #[inline]
    fn lookup_hinted(&self, key: &[u8], hash: u64) -> Option<IndexRef<'_>> {
        let AHashIndex {
            keys,
            table,
            heaps,
            objects,
            slabs,
            ..
        } = self;

        table
            .find(hash, |record| Self::same_key(keys, record, key))
            .map(|addr| Self::view(*addr, keys, heaps, objects, slabs))
    }

    fn lookup_ttl(&self, key: &[u8]) -> Option<(IndexRef<'_>, bool)> {
        let hash = self.hash_key(key);
        let AHashIndex {
            keys,
            table,
            heaps,
            objects,
            slabs,
            ..
        } = self;

        table
            .find(hash, |record| Self::same_key(keys, record, key))
            .map(|addr| {
                (
                    Self::view(*addr, keys, heaps, objects, slabs),
                    Self::ttl_hint(keys, *addr),
                )
            })
    }

    fn lookup_ttl_at(&self, key: &[u8]) -> Option<(IndexRef<'_>, bool, Option<u32>)> {
        let hash = self.hash_key(key);
        let AHashIndex {
            keys,
            table,
            heaps,
            objects,
            slabs,
            ..
        } = self;

        table
            .find(hash, |record| Self::same_key(keys, record, key))
            .map(|addr| {
                (
                    Self::view(*addr, keys, heaps, objects, slabs),
                    Self::ttl_hint(keys, *addr),
                    Some(addr.0),
                )
            })
    }

    fn access_get(&self, key: &[u8]) -> u32 {
        if self.access.is_empty() {
            return 0;
        }

        let hash = self.hash_key(key);

        self.table
            .find(hash, |record| Self::same_key(&self.keys, record, key))
            .map_or(0, |addr| self.access_cell(*addr))
    }

    fn access_mut(&mut self, key: &[u8]) -> Option<&mut u32> {
        let hash = self.hash_key(key);
        let addr = self
            .table
            .find(hash, |record| Self::same_key(&self.keys, record, key))
            .copied()?;
        Some(self.access.cell_mut((addr.0 >> 3) as usize))
    }

    fn access_at(&mut self, address: u32) -> Option<&mut u32> {
        Some(self.access.cell_mut((address >> 3) as usize))
    }

    fn reset_access(&mut self) {
        self.access = AccessCells::default();
    }

    /// A cleared bit on a key with a deadline would silently skip expiry, so every
    /// deadline write sets it first.
    fn mark_ttl(&mut self, key: &[u8]) {
        self.write_ttl_hint(key, true);
    }

    /// Only `clear_deadline` clears the bit, after the deadline record is gone; a stale 1
    /// is harmless, a stale 0 would skip expiry.
    fn clear_ttl(&mut self, key: &[u8]) {
        self.write_ttl_hint(key, false);
    }

    fn object_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let hash = self.hash_key(key);
        let found = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
        }?;

        if Self::decode(&self.keys, found).1 == TAG_COLL_SLAB {
            self.promote_at(found);
        }

        let (key_len, kind, _) = Self::decode(&self.keys, found);

        if kind != TAG_OBJECT {
            return None;
        }

        let index = Self::read_u32(&self.keys, Self::payload_at(found, key_len));

        self.objects[index as usize].as_mut()
    }

    fn promote_collection(&mut self, key: &[u8]) {
        if let Some(addr) = self.find_addr(key) {
            self.promote_at(addr);
        }
    }

    fn demote_collection(&mut self, key: &[u8]) {
        let Some(addr) = self.find_addr(key) else {
            return;
        };
        let (key_len, kind, _) = Self::decode(&self.keys, addr);

        if kind != TAG_OBJECT {
            return;
        }

        let index = Self::read_u32(&self.keys, Self::payload_at(addr, key_len));
        let Some((tag, record)) = self.objects[index as usize]
            .as_ref()
            .and_then(Entry::slab_body)
        else {
            return;
        };

        let id = self.slabs.alloc(record.as_slice());

        drop(self.take_object(index));
        self.retag(addr, key_len, tag, id);
    }

    fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], IndexRef<'_>)> {
        let hash = self.hash_key(key);
        let AHashIndex {
            keys,
            table,
            heaps,
            objects,
            slabs,
            ..
        } = self;

        table
            .find(hash, |record| Self::same_key(keys, record, key))
            .map(|addr| {
                let (key_len, _, _) = Self::decode(keys, *addr);

                (
                    keys.slice(addr.0 + HEADER_LEN, key_len),
                    Self::view(*addr, keys, heaps, objects, slabs),
                )
            })
    }

    fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        let hash = self.hash_key(key);
        let existing = self
            .table
            .find(hash, |record| Self::same_key(&self.keys, record, key))
            .copied();

        if let Some(addr) = existing {
            let (key_len, kind, _) = Self::decode(&self.keys, addr);
            let payload = Self::payload_at(addr, key_len);

            if kind == TAG_INTEGER
                && let Packed::Bytes(PackedBytes::Integer(value)) = entry.clone().into_packed()
            {
                let raw = self.keys.slice(payload, 8);
                let mut bytes = [0u8; 8];

                bytes.copy_from_slice(raw);
                self.keys.write_at(payload, &value.to_le_bytes());

                return Some(Entry::from_packed_bytes(PackedBytes::Integer(
                    i64::from_le_bytes(bytes),
                )));
            }

            let entry = if kind == TAG_LOG || kind == TAG_HEAP {
                match entry.into_packed() {
                    Packed::Bytes(PackedBytes::Log { offset, len }) if kind == TAG_LOG => {
                        let raw = self.keys.slice(payload, 6);
                        let old_offset = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                        let old_len = u16::from_le_bytes([raw[4], raw[5]]);
                        let mut bytes = [0u8; 6];

                        bytes[0..4].copy_from_slice(&offset.to_le_bytes());
                        bytes[4..6].copy_from_slice(&len.to_le_bytes());
                        self.keys.write_at(payload, &bytes);

                        return Some(Entry::from_packed_bytes(PackedBytes::Log {
                            offset: old_offset,
                            len: old_len,
                        }));
                    }

                    Packed::Bytes(PackedBytes::Heap(data)) if kind == TAG_HEAP => {
                        let index = Self::read_u32(&self.keys, payload);
                        let previous = self.heaps[index as usize]
                            .replace(data)
                            .expect("heap value");

                        return Some(Entry::from_packed_bytes(PackedBytes::Heap(previous)));
                    }

                    Packed::Bytes(body) => Entry::from_packed_bytes(body),
                    Packed::Object(entry) => entry,
                }
            } else {
                entry
            };

            let previous = self.unpack(addr);
            let (key_len, _, payload_len) = Self::decode(&self.keys, addr);
            let hint = Self::ttl_hint(&self.keys, addr);
            let access = self.access_cell(addr);
            let stored = self.store_entry(key, entry);

            Self::set_ttl_hint(&mut self.keys, stored, hint);

            if access != 0 {
                // The old record is still live here, so `stored` is a different cell.
                *self.access.cell_mut((stored.0 >> 3) as usize) = access;
            }

            self.table.replace(
                hash,
                |candidate| Self::same_key(&self.keys, candidate, key),
                stored,
            );

            self.keys
                .free_span(addr.0, Self::occupied(key_len, payload_len));

            return Some(previous);
        }

        let stored = self.store_entry(key, entry);
        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        table.insert_unique(hash, stored, |addr| Self::hash_record(keys, hasher, addr));

        None
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let hash = self.hash_key(key);
        let record = self
            .table
            .remove(hash, |record| Self::same_key(&self.keys, record, key))?;

        let (key_len, _, payload_len) = Self::decode(&self.keys, record);
        let entry = self.unpack(record);

        self.keys
            .free_span(record.0, Self::occupied(key_len, payload_len));

        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        if table.is_migrating() {
            // Deletes drive an unfinished resize too, so a mass delete finishes it.
            table.migrate(MIGRATE_PER_ELEMENT, &|record| {
                Self::hash_record(keys, hasher, record)
            });
        } else if let Some(target) = reclaim_capacity(table.len(), table.full_capacity()) {
            // Tombstones lower `capacity()`, so measure against the allocation itself.
            table.begin_shrink(target, |record| Self::hash_record(keys, hasher, record));
        }

        Some(entry)
    }

    fn get_or_insert_with(
        &mut self,
        key: &[u8],
        create: impl FnOnce() -> Entry,
    ) -> Option<&mut Entry> {
        let hash = self.hash_key(key);
        let found = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .map(|addr| (addr, Self::decode(keys, addr).1))
        };

        let found = found.map(|(addr, tag)| {
            if tag == TAG_COLL_SLAB {
                self.promote_at(addr);
            }

            let (key_len, tag, _) = Self::decode(&self.keys, addr);

            (tag == TAG_OBJECT).then(|| Self::read_u32(&self.keys, Self::payload_at(addr, key_len)))
        });

        match found {
            Some(Some(index)) => self.objects[index as usize].as_mut(),
            Some(None) => None,
            None => {
                self.insert(key, create());
                self.object_mut(key)
            }
        }
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    fn eviction_slots(&self) -> usize {
        if self.table.is_empty() {
            0
        } else {
            self.table.num_buckets()
        }
    }

    fn eviction_keys(&self, start: usize, slots: usize) -> impl Iterator<Item = Option<&[u8]>> {
        (start..start.saturating_add(slots)).map(|slot| {
            self.table.get_bucket(slot).map(|addr| {
                let (key_len, _, _) = Self::decode(&self.keys, *addr);
                self.keys.slice(addr.0 + HEADER_LEN, key_len)
            })
        })
    }

    fn eviction_access(
        &self,
        start: usize,
        slots: usize,
        exclude: &[&[u8]],
    ) -> impl Iterator<Item = Option<u32>> {
        // The cell sits by key-log address: no key read or hash lookup per slot. A few
        // excluded keys are matched by address, found once; more by key.
        const BY_ADDRESS: usize = 4;
        let mut excluded = [None; BY_ADDRESS];
        let by_address = exclude.len() <= BY_ADDRESS;

        if by_address {
            for (slot, key) in excluded.iter_mut().zip(exclude) {
                *slot = self.find_addr(key).map(|addr| addr.0);
            }
        }

        (start..start.saturating_add(slots)).map(move |slot| {
            let addr = *self.table.get_bucket(slot)?;
            let skip = if by_address {
                excluded.contains(&Some(addr.0))
            } else {
                let (key_len, _, _) = Self::decode(&self.keys, addr);
                exclude.contains(&self.keys.slice(addr.0 + HEADER_LEN, key_len))
            };

            if skip {
                None
            } else if self.access.is_empty() {
                Some(0)
            } else {
                Some(self.access_cell(addr))
            }
        })
    }

    fn scan_slots<'a>(
        &'a self,
        cursor: u64,
        keys: usize,
        slots: usize,
        visit: &mut dyn FnMut(&'a [u8], IndexRef<'a>, Option<u32>),
    ) -> u64 {
        // A reverse-binary cursor over home buckets (see `Slots::scan`): it survives
        // growth, migration, shrinking and tombstone reclaim without restarting.
        let AHashIndex {
            keys: log,
            table,
            heaps,
            objects,
            slabs,
            hasher,
            ..
        } = self;

        table.scan(
            cursor,
            keys,
            slots,
            &|addr| Self::hash_record(log, hasher, addr),
            &|addr| log.prefetch(addr.0),
            &mut |addr| {
                let (key_len, _, _) = Self::decode(log, *addr);

                visit(
                    log.slice(addr.0 + HEADER_LEN, key_len),
                    Self::view(*addr, log, heaps, objects, slabs),
                    Some(addr.0),
                );
            },
        )
    }

    fn entries(&self) -> impl Iterator<Item = (&[u8], IndexRef<'_>)> {
        let AHashIndex {
            keys,
            table,
            heaps,
            objects,
            slabs,
            ..
        } = self;

        table.iter().map(|addr| {
            let (key_len, _, _) = Self::decode(keys, *addr);

            (
                keys.slice(addr.0 + HEADER_LEN, key_len),
                Self::view(*addr, keys, heaps, objects, slabs),
            )
        })
    }

    fn into_values(self) -> impl Iterator<Item = Entry> {
        let AHashIndex {
            keys,
            table,
            mut heaps,
            mut objects,
            slabs,
            ..
        } = self;

        table.into_iter().map(move |addr| {
            let (key_len, kind, _) = Self::decode(&keys, addr);
            let payload = Self::payload_at(addr, key_len);

            match kind {
                TAG_LOG => {
                    let raw = keys.slice(payload, 6);
                    let offset = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                    let len = u16::from_le_bytes([raw[4], raw[5]]);

                    Entry::from_packed_bytes(PackedBytes::Log { offset, len })
                }

                TAG_INTEGER => {
                    let raw = keys.slice(payload, 8);
                    let mut bytes = [0u8; 8];

                    bytes.copy_from_slice(raw);
                    Entry::from_packed_bytes(PackedBytes::Integer(i64::from_le_bytes(bytes)))
                }

                TAG_HEAP => Entry::from_packed_bytes(PackedBytes::Heap(
                    heaps[Self::read_u32(&keys, payload) as usize]
                        .take()
                        .expect("heap value"),
                )),
                TAG_OBJECT => objects[Self::read_u32(&keys, payload) as usize]
                    .take()
                    .expect("collection"),
                TAG_SET_SLAB => Entry::from_slab_set(slabs.slot(Self::read_u32(&keys, payload))),
                TAG_ZSET_SLAB => {
                    Entry::from_slab_sorted(slabs.slot(Self::read_u32(&keys, payload)))
                }
                TAG_COLL_SLAB => Entry::from_slab_coll(slabs.slot(Self::read_u32(&keys, payload))),

                _ => unreachable!("record kind"),
            }
        })
    }

    fn touch_slab(
        &mut self,
        key: &[u8],
        kind: u16,
        edit: impl FnOnce(&mut [u8]) -> Option<bool>,
    ) -> SlabTouch {
        let hash = self.hash_key(key);
        let found = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .map(|addr| {
                    let (key_len, found, _) = Self::decode(keys, addr);

                    (found, Self::read_u32(keys, Self::payload_at(addr, key_len)))
                })
        };

        let Some((found, id)) = found else {
            return SlabTouch::Absent;
        };

        if found != kind {
            return SlabTouch::Wrong;
        }

        SlabTouch::Done(edit(self.slabs.slot_mut(id)))
    }

    fn create_slab(&mut self, key: &[u8], kind: u16) -> bool {
        if self.lookup(key).is_some() {
            return false;
        }

        let id = self.slabs.alloc_class(0, &[0, 0]);
        let stored = self.alloc_record(key, kind, &id.to_le_bytes());
        let hash = self.hash_key(key);
        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        table.insert_unique(hash, stored, |addr| Self::hash_record(keys, hasher, addr));
        true
    }

    fn enlarge_slab(&mut self, key: &[u8], min_len: usize) -> bool {
        if min_len > COLL_SLAB_MAX {
            return false;
        }

        let hash = self.hash_key(key);
        let found = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .map(|addr| {
                    let (key_len, kind, _) = Self::decode(keys, addr);

                    (
                        kind,
                        addr,
                        key_len,
                        Self::read_u32(keys, Self::payload_at(addr, key_len)),
                    )
                })
        };

        let Some((kind, addr, key_len, id)) = found else {
            return false;
        };

        if kind != TAG_SET_SLAB && kind != TAG_ZSET_SLAB && kind != TAG_COLL_SLAB {
            return false;
        }

        if kind != TAG_COLL_SLAB && min_len > SLAB_SLOT {
            return false;
        }

        let Some(stored) = self.slabs.enlarge(id, min_len) else {
            return false;
        };

        self.keys
            .write_at(Self::payload_at(addr, key_len), &stored.to_le_bytes());
        true
    }

    fn take_slab(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = self.hash_key(key);
        let addr = self
            .table
            .find(hash, |addr| Self::same_key(&self.keys, addr, key))
            .copied()?;
        let (key_len, kind, payload_len) = Self::decode(&self.keys, addr);

        if kind != TAG_SET_SLAB && kind != TAG_ZSET_SLAB {
            return None;
        }

        let id = Self::read_u32(&self.keys, Self::payload_at(addr, key_len));
        let bytes = self.slabs.slot(id).to_vec();

        self.slabs.free(id);

        self.table
            .remove(hash, |candidate| Self::same_key(&self.keys, candidate, key));

        self.keys
            .free_span(addr.0, Self::occupied(key_len, payload_len));
        Some(bytes)
    }

    fn heap_mut(&mut self, key: &[u8]) -> Option<&mut Vec<u8>> {
        let hash = self.hash_key(key);
        let index = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .filter(|addr| Self::decode(keys, *addr).1 == TAG_HEAP)
                .map(|addr| {
                    let (key_len, _, _) = Self::decode(keys, addr);

                    Self::read_u32(keys, Self::payload_at(addr, key_len))
                })
        }?;

        self.heaps[index as usize].as_mut()
    }

    fn retarget_log(&mut self, key: &[u8], offset: u32, len: u16) -> bool {
        let hash = self.hash_key(key);
        let found = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .map(|addr| {
                    let (key_len, kind, _) = Self::decode(keys, addr);

                    (kind, key_len, addr)
                })
        };

        let Some((kind, key_len, addr)) = found else {
            return false;
        };

        if kind != TAG_LOG {
            return false;
        }

        let mut raw = [0u8; 6];

        raw[..4].copy_from_slice(&offset.to_le_bytes());
        raw[4..].copy_from_slice(&len.to_le_bytes());
        self.keys.write_at(Self::payload_at(addr, key_len), &raw);
        true
    }

    fn write_integer(&mut self, key: &[u8], value: i64) -> bool {
        let hash = self.hash_key(key);
        let payload = {
            let AHashIndex { keys, table, .. } = self;

            table
                .find(hash, |addr| Self::same_key(keys, addr, key))
                .copied()
                .filter(|addr| Self::decode(keys, *addr).1 == TAG_INTEGER)
                .map(|addr| {
                    let (key_len, _, _) = Self::decode(keys, addr);

                    Self::payload_at(addr, key_len)
                })
        };

        let Some(payload) = payload else {
            return false;
        };

        self.keys.write_at(payload, &value.to_le_bytes());
        true
    }

    fn key_address(&self, key: &[u8]) -> Option<u32> {
        let hash = self.hash_key(key);

        self.table
            .find(hash, |addr| Self::same_key(&self.keys, addr, key))
            .map(|addr| addr.0)
    }

    fn key_bytes_at(&self, address: u32) -> Option<&[u8]> {
        let addr = Addr(address);
        let (key_len, _, _) = Self::decode(&self.keys, addr);

        Some(self.keys.slice(addr.0 + HEADER_LEN, key_len))
    }

    fn charged_bytes(&self) -> usize {
        // Tombstones occupy the allocation too; charging by live capacity would
        // understate memory until a resize suddenly doubled the charge.
        // Both tables while a resize runs: the draining one is still allocated.
        // A bucket is the slot and its control byte.
        let table = self
            .table
            .all_allocated_buckets()
            .saturating_mul(size_of::<Addr>() + 1);

        table.saturating_add(self.access_charge())
    }

    fn key_log_len(&self) -> Option<u32> {
        Some(self.keys.end())
    }

    fn is_growing(&self) -> bool {
        self.table.is_migrating()
    }

    fn migrate_step(&mut self, budget: usize) -> bool {
        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        table.migrate(budget, &|record| Self::hash_record(keys, hasher, record))
    }

    fn shrink_if_sparse(&mut self) -> bool {
        if self.table.is_migrating() {
            return false;
        }

        let Some(target) = reclaim_capacity(self.table.len(), self.table.full_capacity()) else {
            return false;
        };
        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        table.begin_shrink(target, |record| Self::hash_record(keys, hasher, record));
        true
    }

    fn reclaim_before_growth(&mut self) -> bool {
        let full = self.table.full_capacity();

        // Keep at least 1/8 of the capacity free after rehashing, so steady churn
        // costs a bounded number of moved slots per insert.
        if self.table.is_migrating()
            || self.table.growth_left() > 0
            || self.table.active_len() > full - full / 8
        {
            return false;
        }

        let AHashIndex {
            keys,
            table,
            hasher,
            ..
        } = self;

        table.reclaim_tombstones(|record| Self::hash_record(keys, hasher, record));
        true
    }

    fn plan_key_drain(&mut self) -> usize {
        self.keys.plan_drain()
    }

    fn end_key_drain(&mut self) {
        self.keys.end_drain();
    }

    fn relocate_step(
        &mut self,
        cursor: u64,
        slots: usize,
        keys: bool,
        value: &mut dyn FnMut(u32, u16) -> Option<u32>,
        moved: &mut dyn FnMut(&[u8], u32, u32),
    ) -> u64 {
        // A migration moves elements between the two tables; the caller waits for it.
        if self.table.is_migrating() {
            return cursor;
        }

        let epoch = u64::from(self.table.epoch() & SCAN_EPOCH_MASK);
        let mut slot = if cursor == 0 || cursor >> SCAN_SLOT_BITS != epoch {
            0
        } else {
            (cursor & SCAN_SLOT_MASK) as usize
        };
        let buckets = if self.table.is_empty() {
            0
        } else {
            self.table.num_buckets()
        };
        let end = slot.saturating_add(slots).min(buckets);
        let keys = keys && self.keys.plan_active();

        while slot < end {
            if let Some(&addr) = self.table.get_bucket(slot) {
                let (key_len, kind, payload_len) = Self::decode(&self.keys, addr);

                if kind == TAG_LOG {
                    let payload = Self::payload_at(addr, key_len);
                    let raw = self.keys.slice(payload, 6);
                    let offset = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                    let len = u16::from_le_bytes([raw[4], raw[5]]);

                    if let Some(next) = value(offset, len) {
                        self.keys.write_at(payload, &next.to_le_bytes());
                    }
                }

                if keys && self.keys.is_draining(addr.0) {
                    let next = Addr(
                        self.keys
                            .relocate(addr.0, Self::occupied(key_len, payload_len)),
                    );

                    *self.table.get_bucket_mut(slot).expect("occupied slot") = next;

                    if !self.access.is_empty() {
                        let cell = self.access_cell(addr);

                        self.access.clear((addr.0 >> 3) as usize);

                        if cell != 0 {
                            *self.access.cell_mut((next.0 >> 3) as usize) = cell;
                        } else {
                            self.access.clear((next.0 >> 3) as usize);
                        }
                    }

                    if Self::ttl_hint(&self.keys, next) {
                        moved(
                            self.keys.slice(next.0 + HEADER_LEN, key_len),
                            addr.0,
                            next.0,
                        );
                    }
                }
            }

            slot += 1;
        }

        if slot >= buckets {
            0
        } else {
            epoch << SCAN_SLOT_BITS | slot as u64
        }
    }

    fn log_stats(&self) -> LogStats {
        self.keys.stats()
    }

    fn slab_stats(&self) -> Vec<SlabClassStats> {
        self.slabs
            .classes
            .iter()
            .map(|class| SlabClassStats {
                slot_bytes: class.slot as usize,
                pages: class.pages_in_use() as u64,
                live: class.live_slots(),
            })
            .collect()
    }
}

pub(crate) fn reclaim_capacity(len: usize, capacity: usize) -> Option<usize> {
    if len == 0 && capacity > 0 {
        Some(0)
    } else if capacity > 32 && len.saturating_mul(4) < capacity {
        Some(len.saturating_mul(2).max(16))
    } else {
        None
    }
}

#[derive(Debug, Default)]
pub struct BTreeIndex {
    entries: BTreeMap<Box<[u8]>, Entry>,
    access: BTreeMap<Box<[u8]>, u32>,
}

impl StorageIndex for BTreeIndex {
    fn with_capacity(_capacity: usize) -> Self {
        Self::default()
    }

    fn lookup(&self, key: &[u8]) -> Option<IndexRef<'_>> {
        self.entries.get(key).map(IndexRef::Object)
    }

    fn object_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.entries.get_mut(key)
    }

    fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], IndexRef<'_>)> {
        self.entries
            .get_key_value(key)
            .map(|(stored_key, entry)| (stored_key.as_ref(), IndexRef::Object(entry)))
    }

    fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        self.entries.insert(key.into(), entry)
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        if !self.access.is_empty() {
            self.access.remove(key);
        }

        self.entries.remove(key)
    }

    fn access_get(&self, key: &[u8]) -> u32 {
        self.access.get(key).copied().unwrap_or(0)
    }

    fn access_mut(&mut self, key: &[u8]) -> Option<&mut u32> {
        if !self.entries.contains_key(key) {
            return None;
        }

        Some(self.access.entry(key.into()).or_insert(0))
    }

    fn reset_access(&mut self) {
        self.access.clear();
    }

    fn get_or_insert_with(
        &mut self,
        key: &[u8],
        create: impl FnOnce() -> Entry,
    ) -> Option<&mut Entry> {
        Some(self.entries.entry(key.into()).or_insert_with(create))
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn entries(&self) -> impl Iterator<Item = (&[u8], IndexRef<'_>)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.as_ref(), IndexRef::Object(entry)))
    }

    fn into_values(self) -> impl Iterator<Item = Entry> {
        self.entries.into_values()
    }

    fn charged_bytes(&self) -> usize {
        self.entries.len().saturating_mul(
            size_of::<Box<[u8]>>()
                .saturating_add(size_of::<Entry>())
                .saturating_add(3 * size_of::<usize>()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hint taken before the index grows (and while it migrates) still finds the key:
    /// it is the key's hash under the index's fixed hasher. Prefetching an empty index,
    /// or a key that is absent, is harmless.
    #[test]
    fn lookup_hints_survive_growth_and_prefetch_is_only_a_hint() {
        let mut index = AHashIndex::with_capacity(0);
        let entry = |n: i64| Entry::from_packed_bytes(PackedBytes::Integer(n));

        assert_eq!(index.prefetch(b"absent"), index.lookup_hint(b"absent"));
        assert!(
            index
                .lookup_hinted(b"absent", index.prefetch(b"absent"))
                .is_none()
        );

        let hints: Vec<(u32, u64)> = (0..5_000u32)
            .map(|key| (key, index.prefetch(&key.to_le_bytes())))
            .collect();

        for (key, hint) in &hints {
            index.insert(&key.to_le_bytes(), entry(i64::from(*key)));

            let first = &0u32.to_le_bytes();

            assert!(matches!(
                index.lookup_hinted(first, hints[0].1),
                Some(IndexRef::Integer(0))
            ));
            assert_eq!(*hint, index.lookup_hint(&key.to_le_bytes()));
        }

        for (key, hint) in hints {
            assert!(matches!(
                index.lookup_hinted(&key.to_le_bytes(), hint),
                Some(IndexRef::Integer(value)) if value == i64::from(key)
            ));
        }

        let btree = BTreeIndex::with_capacity(0);

        assert!(
            btree
                .lookup_hinted(b"absent", btree.prefetch(b"absent"))
                .is_none()
        );
    }

    #[test]
    fn access_metadata_follows_the_key_and_is_free_until_used() {
        let mut index = AHashIndex::with_capacity(0);
        let entry = |n: i64| Entry::from_packed_bytes(PackedBytes::Integer(n));

        for key in 0..64u32 {
            index.insert(&key.to_le_bytes(), entry(1));
        }
        assert!(index.access.is_empty());
        assert_eq!(index.access_get(&7u32.to_le_bytes()), 0);

        *index.access_mut(&7u32.to_le_bytes()).unwrap() = 42;
        assert!(index.access_mut(b"absent").is_none());
        // Replacing the record moves it; the metadata moves with it.
        index.insert(
            &7u32.to_le_bytes(),
            Entry::from_packed_bytes(PackedBytes::Heap(vec![1; 40])),
        );
        assert_eq!(index.access_get(&7u32.to_le_bytes()), 42);
        assert_eq!(index.access_get(&8u32.to_le_bytes()), 0);

        index.remove(&7u32.to_le_bytes());
        index.insert(&7u32.to_le_bytes(), entry(2));
        assert_eq!(index.access_get(&7u32.to_le_bytes()), 0);

        *index.access_mut(&9u32.to_le_bytes()).unwrap() = 5;
        index.reset_access();
        assert_eq!(index.access_get(&9u32.to_le_bytes()), 0);
        assert!(index.access.is_empty());
    }

    #[test]
    fn tombstone_reclaim_keeps_the_allocation_and_charge() {
        let mut index = AHashIndex::with_capacity(1_024);
        let entry = || Entry::from_packed_bytes(PackedBytes::Integer(1));

        for key in 0..1_024u32 {
            index.insert(&key.to_le_bytes(), entry());
        }
        let charged = index.charged_bytes();
        let buckets = index.table.allocated_buckets();
        assert!(!index.reclaim_before_growth());

        let mut next = 1_024u32;
        let mut reclaimed = false;
        while !reclaimed {
            assert!(index.remove(&(next - 1_024).to_le_bytes()).is_some());
            index.insert(&next.to_le_bytes(), entry());
            next += 1;
            assert_eq!(index.table.allocated_buckets(), buckets);
            reclaimed = index.reclaim_before_growth();
        }

        assert!(index.table.growth_left() > 0);
        assert_eq!(index.charged_bytes(), charged);
        for key in next - 1_024..next {
            assert!(index.lookup(&key.to_le_bytes()).is_some());
        }
    }

    #[test]
    fn address_slot_is_four_bytes() {
        assert_eq!(size_of::<Addr>(), 4);
    }
}
