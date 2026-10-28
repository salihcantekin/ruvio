use super::*;

impl Value {
    /// The string body of a value whose bytes may be a span in the shard value log. Every
    /// variant is listed: one that holds a `BytesValue` must answer here, because a plain
    /// clone of it would share the span and a plain drop would leak it.
    pub(super) fn string_body(&self) -> Option<&BytesValue> {
        match self {
            Value::Bytes(body) => Some(body),
            Value::Versioned(value) => Some(&value.body),
            Value::Json(_)
            | Value::JsonTree(_)
            | Value::List(_)
            | Value::Set(_)
            | Value::Hash(_)
            | Value::SortedSet(_)
            | Value::BloomFilter(_)
            | Value::Stream(_)
            | Value::Lease(_)
            | Value::Limit(_)
            | Value::Once
            | Value::TimedHash(_)
            | Value::Semaphore(_) => None,
        }
    }

    pub(super) fn string_body_mut(&mut self) -> Option<&mut BytesValue> {
        match self {
            Value::Bytes(body) => Some(body),
            Value::Versioned(value) => Some(&mut value.body),
            Value::Json(_)
            | Value::JsonTree(_)
            | Value::List(_)
            | Value::Set(_)
            | Value::Hash(_)
            | Value::SortedSet(_)
            | Value::BloomFilter(_)
            | Value::Stream(_)
            | Value::Lease(_)
            | Value::Limit(_)
            | Value::Once
            | Value::TimedHash(_)
            | Value::Semaphore(_) => None,
        }
    }

    /// The string body to release into the value log, or the value itself when it holds none.
    pub(super) fn into_string_body(self) -> Result<BytesValue, Value> {
        match self {
            Value::Bytes(body) => Ok(body),
            Value::Versioned(value) => Ok(value.body),
            value @ (Value::Json(_)
            | Value::JsonTree(_)
            | Value::List(_)
            | Value::Set(_)
            | Value::Hash(_)
            | Value::SortedSet(_)
            | Value::BloomFilter(_)
            | Value::Stream(_)
            | Value::Lease(_)
            | Value::Limit(_)
            | Value::Once
            | Value::TimedHash(_)
            | Value::Semaphore(_)) => Err(value),
        }
    }
    /// The fields of a plain or timed hash.
    pub(super) fn as_hash(&self) -> Option<&HashValue> {
        match self {
            Self::Hash(hash) => Some(hash),
            Self::TimedHash(timed) => Some(&timed.hash),
            _ => None,
        }
    }

    /// Absolute Unix milliseconds when `field` expires, or `None` without a TTL.
    pub(super) fn field_deadline(&self, field: &[u8]) -> Option<u64> {
        match self {
            Self::TimedHash(timed) => timed.deadline(field),
            _ => None,
        }
    }

    /// A timed hash whose every field is due at `now`: it counts as a missing key.
    pub(super) fn hash_fields_all_due(&self, now: u64) -> bool {
        match self {
            Self::TimedHash(timed) => timed.all_fields_due(now),
            _ => false,
        }
    }

    /// Earliest field deadline of a timed hash.
    pub(super) fn soonest_field_deadline(&self) -> Option<u64> {
        match self {
            Self::TimedHash(timed) => Some(timed.soonest),
            _ => None,
        }
    }

    /// Sets a TTL on an existing hash field. The caller checks that the field exists.
    pub(super) fn set_field_deadline(&mut self, field: &[u8], at: u64) {
        if let Self::Hash(hash) = self {
            let hash = std::mem::take(hash.as_mut());

            *self = Self::TimedHash(Box::new(TimedHash::new(hash)));
        }

        if let Self::TimedHash(timed) = self {
            timed.set_deadline(field, at);
        }
    }

    /// Drops the TTL on `field`. True when it had one. The last TTL turns the value back into
    /// a plain `Hash`, so it stops paying for the feature.
    pub(super) fn clear_field_deadline(&mut self, field: &[u8]) -> bool {
        let Self::TimedHash(timed) = self else {
            return false;
        };

        let cleared = timed.clear_deadline(field);

        self.settle_timed_hash();
        cleared
    }

    /// Turns a timed hash without deadlines back into a plain `Hash`.
    fn settle_timed_hash(&mut self) {
        if let Self::TimedHash(timed) = self
            && timed.deadlines.is_empty()
        {
            let hash = std::mem::take(&mut timed.hash);

            *self = Self::Hash(Box::new(hash));
        }
    }

    /// `HSET` on a hash value: the field is replaced and its TTL is dropped.
    pub(super) fn hash_insert(
        &mut self,
        field: &[u8],
        value: &[u8],
        limits: CompactLimits,
    ) -> bool {
        self.clear_field_deadline(field);
        self.hash_insert_keep_deadline(field, value, limits)
    }

    /// Writes a field and keeps its TTL, like `HINCRBY`.
    pub(super) fn hash_insert_keep_deadline(
        &mut self,
        field: &[u8],
        value: &[u8],
        limits: CompactLimits,
    ) -> bool {
        match self {
            Self::Hash(hash) => hash.insert(field, value, limits),
            Self::TimedHash(timed) => timed.insert(field, value, limits),
            _ => false,
        }
    }

    /// `HDEL` on a hash value: the field and its TTL go together.
    pub(super) fn hash_remove(&mut self, field: &[u8], limits: CompactLimits) -> bool {
        let removed = match self {
            Self::Hash(hash) => return hash.remove(field, limits),
            Self::TimedHash(timed) => timed.remove(field, limits),
            _ => return false,
        };

        self.settle_timed_hash();
        removed
    }

    /// Removes up to `max` fields whose deadline is at or before `now`. Returns how many.
    pub(super) fn purge_due_fields(
        &mut self,
        now: u64,
        max: usize,
        limits: CompactLimits,
    ) -> usize {
        let removed = match self {
            Self::TimedHash(timed) if timed.soonest <= now => timed.purge(now, max, limits),
            _ => return 0,
        };

        self.settle_timed_hash();
        removed
    }
}

/// The logical data stored for one key.
///
/// Strings stay inline. Every larger collection is boxed so a string entry is
/// not sized like a sorted set. The box is ordinary Rust ownership: the entry
/// roots the collection, with no side table of object ids.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bytes(BytesValue),
    Json(Box<Vec<u8>>),
    JsonTree(Box<crate::json::TreeDocument>),
    List(Box<ListValue>),
    Set(Box<SetValue>),
    Hash(Box<HashValue>),
    SortedSet(Box<SortedSet>),
    BloomFilter(Box<BloomFilter>),
    Stream(Box<crate::stream::StreamValue>),
    /// Present only after `LEASE`. The token lives here. The deadline index holds the expiry.
    Lease(Box<LeaseValue>),
    /// Present only after `SETV`. A plain `SET` does not create this arm.
    Versioned(Box<VersionedValue>),
    /// Present only after `LIMIT`. The deadline index is the window.
    Limit(Box<LimitValue>),
    /// Present only after `ONCE`. The deadline index drops the marker.
    Once,
    /// A hash with at least one field TTL. `HPERSIST` of the last one returns it to `Hash`.
    TimedHash(Box<TimedHash>),
    /// Present only after `SEMAPHORE`. The deadline index holds the latest holder deadline, so
    /// the key goes away once every holder has expired.
    Semaphore(Box<SemaphoreValue>),
}

const _: () = assert!(size_of::<Value>() <= 24);

/// One counting semaphore. A single live holder sits in this object. A second holder is the
/// first extra allocation: an exact slice of `(token, expires_at)`, 16 bytes each, with no
/// spare capacity. `Value` keeps a thin pointer to this object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemaphoreValue {
    pub(super) state: SemaphoreState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SemaphoreState {
    One {
        next_token: u64,
        token: u64,
        expires_at: u64,
    },
    Many {
        next_token: u64,
        holders: Box<[(u64, u64)]>,
    },
}

pub(crate) struct HolderIter<'a> {
    pub(super) one: Option<(u64, u64)>,
    pub(super) many: std::slice::Iter<'a, (u64, u64)>,
}

impl Iterator for HolderIter<'_> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(holder) = self.one.take() {
            return Some(holder);
        }

        self.many.next().copied()
    }
}

pub(super) fn no_holders() -> std::slice::Iter<'static, (u64, u64)> {
    static EMPTY: [(u64, u64); 0] = [];

    EMPTY.iter()
}

impl SemaphoreValue {
    pub(super) fn new(next_token: u64, holders: impl IntoIterator<Item = (u64, u64)>) -> Self {
        let mut holders = holders.into_iter();
        let Some((token, expires_at)) = holders.next() else {
            return Self {
                state: SemaphoreState::Many {
                    next_token,
                    holders: Box::from([]),
                },
            };
        };

        let Some(second) = holders.next() else {
            return Self {
                state: SemaphoreState::One {
                    next_token,
                    token,
                    expires_at,
                },
            };
        };

        let mut all = vec![(token, expires_at), second];

        all.extend(holders);
        Self {
            state: SemaphoreState::Many {
                next_token,
                holders: all.into(),
            },
        }
    }

    pub(crate) fn next_token(&self) -> u64 {
        match self.state {
            SemaphoreState::One { next_token, .. } | SemaphoreState::Many { next_token, .. } => {
                next_token
            }
        }
    }

    pub(crate) fn holders(&self) -> HolderIter<'_> {
        match &self.state {
            SemaphoreState::One {
                token, expires_at, ..
            } => HolderIter {
                one: Some((*token, *expires_at)),
                many: no_holders(),
            },
            SemaphoreState::Many { holders, .. } => HolderIter {
                one: None,
                many: holders.iter(),
            },
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.state {
            SemaphoreState::One { .. } => 1,
            SemaphoreState::Many { holders, .. } => holders.len(),
        }
    }

    pub(super) fn latest(&self) -> Option<u64> {
        self.holders().map(|(_, expires_at)| expires_at).max()
    }

    /// Drops expired holders and `drop`, then adds `added` when given. One survivor stays
    /// inside this object. Two or more become an exact slice.
    pub(super) fn rewrite(&mut self, now: u64, drop: Option<u64>, added: Option<(u64, u64)>) {
        let mut next_token = self.next_token();

        if let Some((token, _)) = added {
            next_token = next_token.max(token.saturating_add(1));
        }

        let mut only = None;
        let mut more = false;

        for (token, expires_at) in self.holders() {
            if expires_at <= now || Some(token) == drop {
                continue;
            }

            if only.is_some() {
                more = true;

                break;
            }

            only = Some((token, expires_at));
        }

        if let Some(holder) = added {
            if only.is_some() {
                more = true;
            } else {
                only = Some(holder);
            }
        }

        if !more {
            self.state = match only {
                Some((token, expires_at)) => SemaphoreState::One {
                    next_token,
                    token,
                    expires_at,
                },
                None => SemaphoreState::Many {
                    next_token,
                    holders: Box::from([]),
                },
            };

            return;
        }

        let holders: Box<[_]> = self
            .holders()
            .filter(|(token, expires_at)| *expires_at > now && Some(*token) != drop)
            .chain(added)
            .collect();

        self.state = SemaphoreState::Many {
            next_token,
            holders,
        };
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseValue {
    pub(crate) token: u64,
    pub(crate) expires_at: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VersionedValue {
    pub(crate) version: u64,
    pub(crate) body: BytesValue,
}

/// The window end is the key's deadline in the ring, not a field here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitValue {
    pub(crate) used: u64,
}

pub(crate) enum LimitDecision {
    Allow {
        used: u64,
        /// `Some` when this call moves the key's deadline: it opens a window, rolls the
        /// sliding window, or advances the token bucket.
        window_end: Option<u64>,
        remaining: u64,
        /// This call creates the key.
        fresh: bool,
    },
    Reject,
    /// A refusal from `SLIDING` or `TOKENS`, with the wait until the same call would pass.
    RejectRetry {
        retry_after_ms: u64,
    },
}

/// Why a `LIMIT` call could not run on the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LimitError {
    WrongType,
    /// The key holds a limit created by another mode.
    Mode,
    /// The clock and the arguments do not fit the packed state.
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangeNote {
    pub(crate) sequence: u64,
    pub(crate) text: String,
}

pub(crate) enum SetVersionError {
    WrongType,
    Mismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueKind {
    Bytes,
    Json,
    List,
    Set,
    Hash,
    SortedSet,
    BloomFilter,
    Stream,
    Lease,
    Versioned,
    Limit,
    Once,
    Semaphore,
}

impl ValueKind {
    /// Every kind, so a test can walk them all; a new kind fails to compile in
    /// `type_name` until it has a name.
    pub(crate) const ALL: [ValueKind; 13] = [
        ValueKind::Bytes,
        ValueKind::Json,
        ValueKind::List,
        ValueKind::Set,
        ValueKind::Hash,
        ValueKind::SortedSet,
        ValueKind::BloomFilter,
        ValueKind::Stream,
        ValueKind::Lease,
        ValueKind::Versioned,
        ValueKind::Limit,
        ValueKind::Once,
        ValueKind::Semaphore,
    ];

    /// The name `TYPE` reports and `SCAN ... TYPE` filters on. A versioned value is a
    /// string to clients.
    pub(crate) const fn type_name(self) -> &'static [u8] {
        match self {
            ValueKind::Bytes | ValueKind::Versioned => b"string",
            ValueKind::Json => b"json",
            ValueKind::List => b"list",
            ValueKind::Set => b"set",
            ValueKind::Hash => b"hash",
            ValueKind::SortedSet => b"zset",
            ValueKind::BloomFilter => b"bloomfilter",
            ValueKind::Stream => b"stream",
            ValueKind::Lease => b"lease",
            ValueKind::Limit => b"limit",
            ValueKind::Semaphore => b"semaphore",
            ValueKind::Once => b"once",
        }
    }

    /// The kind clients see: kinds with one `TYPE` name compare equal.
    pub(crate) const fn reported(self) -> ValueKind {
        match self {
            ValueKind::Versioned => ValueKind::Bytes,
            kind => kind,
        }
    }

    /// The kind `SCAN ... TYPE name` selects, by the name `TYPE` reports.
    pub(crate) fn from_type_name(name: &[u8]) -> Option<ValueKind> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.type_name().eq_ignore_ascii_case(name))
    }
}

pub(super) enum AppendHit {
    Heap,
    Log { offset: u32, len: u16 },
    Integer(i64),
}

/// Where a string's bytes live. Short values are a span in the shard byte log.
/// Longer values stay on the heap so the byte pool can recycle them. The log
/// arm is pointer-sized; the heap arm is boxed so a 64-byte string does not
/// widen every entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ByteBody {
    Log {
        offset: u32,
        len: u16,
    },
    // Boxed on purpose: it keeps the heap arm pointer-sized (see the type's note).
    #[allow(clippy::box_collection)]
    Heap(Box<Vec<u8>>),
    /// Counter stored without its decimal text. Replies format the digits.
    Integer(i64),
}

/// Size probe for `tests/layout.rs`; the type itself stays private.
#[doc(hidden)]
pub const BYTE_BODY_SIZE: usize = size_of::<ByteBody>();

pub(super) const INTEGER_MARK: u16 = 0x8000;

pub(super) fn plain_len(len: u16) -> u16 {
    len & !INTEGER_MARK
}

pub(super) fn is_integer(len: u16) -> bool {
    len & INTEGER_MARK != 0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BytesValue {
    pub(super) body: ByteBody,
}

pub(super) const LOG_PAGE: usize = 4096;
pub(super) const LOG_LARGE: u32 = 1 << 31;

/// Bump-allocated offsets stay below `LOG_LARGE`, so one log addresses at most 2 GiB.
/// Wider offsets (page index, u64) would grow RSS or the index slot.
pub(super) const LOG_LIMIT: u32 = LOG_LARGE;

/// End of a page-local free list.
const NO_SPAN: u16 = u16::MAX;
const NO_PAGE: u32 = u32::MAX;
/// Free spans shorter than this are not listed: they hold no link and stay dead
/// until their page is released or drained.
const MIN_SPAN: u32 = 8;
/// Size classes of free spans: class `c` holds lengths `8(c+1)..8(c+2)-1`, the last
/// class every length from 64 up.
const SPAN_CLASSES: usize = 8;
const BIG_CLASS: usize = SPAN_CLASSES - 1;
const BIG_SPAN: u32 = (MIN_SPAN as usize * SPAN_CLASSES) as u32;
/// A drain round starts only when this many bytes are dead...
const DRAIN_MIN_DEAD: u64 = if cfg!(test) { 16 * 1024 } else { 1 << 20 };
/// ...and at most this many pages are flagged per round, sparsest extents first.
const DRAIN_MAX_PAGES: usize = 1024;
/// Pages per extent, the unit the log takes from and returns to the allocator.
/// Measured on macOS with mimalloc 2.3: freed blocks above 512 KiB leave the
/// physical footprint, smaller ones (4 KiB pages included) stay in its page cache.
const EXTENT_PAGES: usize = if cfg!(test) { 8 } else { 256 };
const EXTENT_BYTES: usize = EXTENT_PAGES * LOG_PAGE;
const MASK_WORDS: usize = EXTENT_PAGES.div_ceil(64);

/// Per-page bookkeeping packed in one `u16`: live bytes (at most `LOG_PAGE`, so 13
/// bits) and two flags. Adding or removing live bytes is a plain add or subtract.
/// The heads of a page's free lists live in `FreeSpace::lists`, allocated with the
/// first free: a log that only grows keeps 2 bytes per page next to its extent.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PageMeta(u16);

const PAGE_LIVE: u16 = (1 << 13) - 1;
/// Opened by the bump and not released since.
const PAGE_OPEN: u16 = 1 << 13;
const PAGE_DRAINING: u16 = 1 << 14;

const _: () = assert!(LOG_PAGE as u16 <= PAGE_LIVE);

impl PageMeta {
    #[inline]
    fn live(self) -> u16 {
        self.0 & PAGE_LIVE
    }

    #[inline]
    fn add_live(&mut self, len: u16) {
        self.0 += len;
    }

    #[inline]
    fn sub_live(&mut self, len: u16) {
        debug_assert!(self.live() >= len, "log page live underflow");
        self.0 -= len;
    }

    #[inline]
    pub(super) fn open(self) -> bool {
        self.0 & PAGE_OPEN != 0
    }

    #[inline]
    fn draining(self) -> bool {
        self.0 & PAGE_DRAINING != 0
    }

    fn set_flag(&mut self, flag: u16, on: bool) {
        if on {
            self.0 |= flag;
        } else {
            self.0 &= !flag;
        }
    }
}

/// Free-list heads of one page. A free span stores its own link: `[next u16][len
/// u16]`, plus `[prev u16]` in the big class so a best-fit take can unlink it in O(1).
#[derive(Debug, Clone, Copy)]
struct PageLists {
    heads: [u16; SPAN_CLASSES],
    /// Bit `c`: the page sits on `small[c]`. Kept across release so the stack never
    /// holds a page twice.
    listed: u8,
}

impl Default for PageLists {
    fn default() -> Self {
        Self {
            heads: [NO_SPAN; SPAN_CLASSES],
            listed: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ExtentMeta {
    /// Bit `i`: page `i` was released and may be opened again.
    free: [u64; MASK_WORDS],
    /// Open pages.
    open: u16,
    /// On `FreeSpace::partial`.
    listed: bool,
    draining: bool,
}

/// One extent: its bytes (absent once handed back), its counters and its pages'
/// counters, in one element so a log that holds one extent owns two heap blocks.
#[derive(Debug)]
pub(super) struct Extent {
    pub(super) bytes: Option<Box<[u8; EXTENT_BYTES]>>,
    meta: ExtentMeta,
    pages: [PageMeta; EXTENT_PAGES],
}

/// Page counters of one log, summed over logs for `INFO`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    pub pages: u64,
    /// Extents held from the allocator.
    pub extents: u64,
    pub free_bytes: u64,
    pub pages_released: u64,
    pub relocated_bytes: u64,
}

impl LogStats {
    pub(crate) fn add(&mut self, other: Self) {
        self.pages += other.pages;
        self.extents += other.extents;
        self.free_bytes += other.free_bytes;
        self.pages_released += other.pages_released;
        self.relocated_bytes += other.relocated_bytes;
    }
}

#[derive(Debug, Default)]
struct FreeSpace {
    /// Free-list heads by page index; grown on demand up to the pages held.
    lists: Vec<PageLists>,
    /// Per small class, pages whose list of that class may be non-empty (lazy).
    small: [Vec<u32>; BIG_CLASS],
    /// Big-class free spans by `(len, offset)`, for best fit.
    big: BTreeSet<(u32, u32)>,
    /// Live extents that may have a released page (lazy).
    partial: Vec<u32>,
    /// Extents handed back to the allocator; their indexes are reused first.
    free_extents: Vec<u32>,
    free_bytes: u64,
    draining: u32,
    released: u64,
    relocated: u64,
}

impl FreeSpace {
    fn lists_mut(&mut self, index: usize) -> &mut PageLists {
        if index >= self.lists.len() {
            self.lists.resize(
                (index / EXTENT_PAGES + 1) * EXTENT_PAGES,
                PageLists::default(),
            );
        }

        &mut self.lists[index]
    }

    #[cfg(test)]
    fn heads(&self, index: usize) -> [u16; SPAN_CLASSES] {
        self.lists
            .get(index)
            .map_or([NO_SPAN; SPAN_CLASSES], |lists| lists.heads)
    }
}

/// Paged byte log with segregated free lists. A page whose live bytes reach zero is
/// handed back to the allocator and its index reused; sparse pages are drained by
/// `relocate` during maintenance so random deletes also return memory.
#[derive(Debug)]
pub(crate) struct ValueLog {
    pub(super) extents: Vec<Extent>,
    /// Next page never opened. Pages from here to the end of the last extent are
    /// untouched memory.
    fresh: u32,
    open_pages: u32,
    bump_page: u32,
    bump_at: u32,
    /// Highest offset ever bumped; addresses of keys that exist now are below it.
    high: u32,
    /// Bump allocation may not create a page reaching past this. `LOG_LIMIT` outside tests.
    pub(super) limit: u32,
    /// While at most this many pages are open, 1% of the limit is still unused, so
    /// `near_limit` answers with one compare.
    calm_pages: u32,
    live_bytes: u64,
    /// Free-space bookkeeping, allocated at the first free: a log that only grows
    /// pays one pointer.
    spare: Option<Box<FreeSpace>>,
    /// Spans longer than a page, one heap block each; allocated at first use.
    large: Option<Box<LargeBlocks>>,
}

#[derive(Debug, Default)]
struct LargeBlocks {
    blocks: Vec<Option<Vec<u8>>>,
    free: Vec<u32>,
}

impl Default for ValueLog {
    fn default() -> Self {
        Self {
            extents: Vec::new(),
            fresh: 0,
            open_pages: 0,
            bump_page: NO_PAGE,
            bump_at: 0,
            high: 0,
            limit: LOG_LIMIT,
            calm_pages: calm_pages(LOG_LIMIT),
            live_bytes: 0,
            spare: None,
            large: None,
        }
    }
}

impl ExtentMeta {
    fn has_free(&self) -> bool {
        self.free.iter().any(|word| *word != 0)
    }

    fn set_free(&mut self, slot: usize) {
        self.free[slot / 64] |= 1 << (slot % 64);
    }

    /// Lowest released page; the caller checked `has_free`.
    fn take_free(&mut self) -> usize {
        let word = self
            .free
            .iter()
            .position(|word| *word != 0)
            .expect("released page");
        let bit = self.free[word].trailing_zeros() as usize;

        self.free[word] &= !(1 << bit);
        word * 64 + bit
    }
}

/// Open pages below which a log with `limit` keeps 1% of it unused (`near_limit`).
const fn calm_pages(limit: u32) -> u32 {
    let pages = limit as usize / LOG_PAGE;
    let reserve = (limit as usize / 100).div_ceil(LOG_PAGE);

    pages.saturating_sub(reserve) as u32
}

fn span_class(len: u32) -> usize {
    ((len / MIN_SPAN) as usize).min(SPAN_CLASSES) - 1
}

fn page_of(offset: u32) -> usize {
    offset as usize / LOG_PAGE
}

fn page_in(extents: &[Extent], index: usize) -> &[u8; LOG_PAGE] {
    let start = index % EXTENT_PAGES * LOG_PAGE;

    extents[index / EXTENT_PAGES]
        .bytes
        .as_deref()
        .expect("live log extent")[start..start + LOG_PAGE]
        .try_into()
        .expect("page length")
}

fn page_in_mut(extents: &mut [Extent], index: usize) -> &mut [u8; LOG_PAGE] {
    let start = index % EXTENT_PAGES * LOG_PAGE;

    (&mut extents[index / EXTENT_PAGES]
        .bytes
        .as_deref_mut()
        .expect("live log extent")[start..start + LOG_PAGE])
        .try_into()
        .expect("page length")
}

fn zeroed_extent() -> Box<[u8; EXTENT_BYTES]> {
    // `vec!` of zeros asks the allocator for zeroed memory: fresh pages stay untouched.
    vec![0u8; EXTENT_BYTES]
        .into_boxed_slice()
        .try_into()
        .expect("extent length")
}

fn read_u16(page: &[u8; LOG_PAGE], at: usize) -> u16 {
    u16::from_le_bytes([page[at], page[at + 1]])
}

fn write_u16(page: &mut [u8; LOG_PAGE], at: usize, value: u16) {
    page[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

impl ValueLog {
    #[inline]
    fn page(&self, index: usize) -> PageMeta {
        self.extents[index / EXTENT_PAGES].pages[index % EXTENT_PAGES]
    }

    #[inline]
    fn page_mut(&mut self, index: usize) -> &mut PageMeta {
        &mut self.extents[index / EXTENT_PAGES].pages[index % EXTENT_PAGES]
    }

    /// The page is open (test helper).
    #[cfg(test)]
    pub(crate) fn page_open(&self, index: usize) -> bool {
        self.page(index).open()
    }

    /// One past the highest bump-allocated offset.
    #[inline]
    pub(crate) fn end(&self) -> u32 {
        self.high
    }

    fn cursor(&self) -> u32 {
        if self.bump_page == NO_PAGE {
            return NO_PAGE;
        }

        self.bump_page * LOG_PAGE as u32 + self.bump_at
    }

    /// True when fewer than 1% of the limit's bytes are left for new pages and no freed
    /// page-sized span can absorb the next write. Admission refuses writes with `OOM`
    /// instead of letting the bump reach the limit.
    #[inline]
    pub(crate) fn near_limit(&self) -> bool {
        self.open_pages > self.calm_pages && self.near_limit_slow()
    }

    #[cold]
    #[inline(never)]
    fn near_limit_slow(&self) -> bool {
        let unused_pages =
            (self.limit as usize / LOG_PAGE).saturating_sub(self.open_pages as usize);
        let bump_left = if self.bump_page == NO_PAGE {
            0
        } else {
            LOG_PAGE - self.bump_at as usize
        };
        let unused = unused_pages * LOG_PAGE + bump_left;

        unused < self.limit as usize / 100 && self.largest_free() < LOG_PAGE as u32
    }

    #[cold]
    fn largest_free(&self) -> u32 {
        self.spare
            .as_ref()
            .and_then(|space| space.big.last())
            .map_or(0, |&(len, _)| len)
    }

    #[cfg(test)]
    pub(crate) fn set_limit_for_test(&mut self, limit: u32) {
        self.limit = limit.min(LOG_LIMIT);
        self.calm_pages = calm_pages(self.limit);
    }

    pub(crate) fn alloc(&mut self, value: &[u8]) -> (u32, u16) {
        let offset = self.alloc_span(value.len());

        self.write_at(offset, value);
        (offset, value.len() as u16)
    }

    pub(crate) fn alloc_span(&mut self, len: usize) -> u32 {
        if len > LOG_PAGE {
            return self.alloc_large(len);
        }

        if len == 0 {
            // Reads of zero bytes never touch a page.
            return 0;
        }

        let need = len as u32;

        if self.spare.is_none() {
            return self.bump(need);
        }

        if need < BIG_SPAN {
            for class in span_class(need.next_multiple_of(MIN_SPAN))..BIG_CLASS {
                if let Some((offset, cap)) = self.pop_small(class) {
                    return self.carve(offset, cap, need);
                }
            }
        }

        if let Some(&(cap, offset)) = self
            .spare
            .as_ref()
            .and_then(|space| space.big.range((need, 0)..).next())
        {
            self.unlink_big(offset, cap);

            return self.carve(offset, cap, need);
        }

        self.bump(need)
    }

    /// Bump a new span. APPEND uses this so a growing string can later extend in place.
    pub(crate) fn alloc_tail(&mut self, len: usize) -> u32 {
        if len > LOG_PAGE {
            return self.alloc_large(len);
        }

        self.bump(len as u32)
    }

    /// Write `suffix` when the span already ends at the cursor and stays on one page.
    pub(crate) fn extend_tail(&mut self, offset: u32, len: usize, suffix: &[u8]) -> bool {
        if offset & LOG_LARGE != 0 || suffix.is_empty() || self.bump_page == NO_PAGE {
            return false;
        }

        let cursor = self.cursor();

        if cursor != offset.saturating_add(len as u32) {
            return false;
        }

        let at = self.bump_at as usize;

        if at == 0 || at + suffix.len() > LOG_PAGE {
            return false;
        }

        self.write_at(cursor, suffix);
        self.bump_at += suffix.len() as u32;
        let page = self.bump_page as usize;

        self.page_mut(page).add_live(suffix.len() as u16);
        self.live_bytes += suffix.len() as u64;
        self.high = self.high.max(cursor + suffix.len() as u32);
        true
    }

    #[inline]
    fn bump(&mut self, need: u32) -> u32 {
        if self.bump_page == NO_PAGE || LOG_PAGE as u32 - self.bump_at < need {
            self.next_bump_page();
        }

        let page = self.bump_page as usize;
        let offset = page as u32 * LOG_PAGE as u32 + self.bump_at;

        self.bump_at += need;
        self.page_mut(page).add_live(need as u16);
        self.live_bytes += u64::from(need);
        self.high = self.high.max(offset + need);
        offset
    }

    #[cold]
    #[inline(never)]
    fn next_bump_page(&mut self) {
        self.retire_bump();
        self.open_bump_page();
    }

    fn retire_bump(&mut self) {
        if self.bump_page == NO_PAGE {
            return;
        }

        let page = self.bump_page;
        let rest = LOG_PAGE as u32 - self.bump_at;

        self.bump_page = NO_PAGE;

        if self.page(page as usize).live() == 0 {
            self.release(page as usize);
        } else if rest >= MIN_SPAN {
            self.push_free(page * LOG_PAGE as u32 + self.bump_at, rest);
        }
    }

    fn open_bump_page(&mut self) {
        let page = match self.reuse_page() {
            Some(page) => page,
            None => self.fresh_page(),
        };

        // Admission answers OOM before the bump gets here, so this is a bug check: an explicit
        // panic beats a wrapped offset.
        if (page + 1) * LOG_PAGE > self.limit as usize {
            panic!("value log exhausted: admission guard bypassed");
        }

        self.page_mut(page).set_flag(PAGE_OPEN, true);
        self.extents[page / EXTENT_PAGES].meta.open += 1;
        self.open_pages += 1;
        self.bump_page = page as u32;
        self.bump_at = 0;
    }

    /// A released page of a live extent, else a whole extent the log gave back.
    fn reuse_page(&mut self) -> Option<usize> {
        let space = self.spare.as_mut()?;

        while let Some(&extent) = space.partial.last() {
            let entry = &mut self.extents[extent as usize];
            let meta = &mut entry.meta;

            if !meta.has_free() || meta.draining || entry.bytes.is_none() {
                meta.listed = false;
                space.partial.pop();
                continue;
            }

            return Some(extent as usize * EXTENT_PAGES + meta.take_free());
        }

        let extent = space.free_extents.pop()? as usize;
        let entry = &mut self.extents[extent];

        entry.bytes = Some(zeroed_extent());
        for slot in 1..EXTENT_PAGES {
            entry.meta.set_free(slot);
        }

        if !entry.meta.listed {
            entry.meta.listed = true;
            space.partial.push(extent as u32);
        }

        Some(extent * EXTENT_PAGES)
    }

    fn fresh_page(&mut self) -> usize {
        let page = self.fresh as usize;

        if page.is_multiple_of(EXTENT_PAGES) {
            debug_assert_eq!(page / EXTENT_PAGES, self.extents.len());
            // Exact growth: one extent's element is not small, and most logs hold one.
            self.extents.reserve_exact(1);
            self.extents.push(Extent {
                bytes: Some(zeroed_extent()),
                meta: ExtentMeta::default(),
                pages: [PageMeta::default(); EXTENT_PAGES],
            });
        }

        self.fresh += 1;
        page
    }

    /// Hand `need` bytes of a free span to the caller and list the rest.
    fn carve(&mut self, offset: u32, cap: u32, need: u32) -> u32 {
        self.page_mut(page_of(offset)).add_live(need as u16);
        self.live_bytes += u64::from(need);

        if cap - need >= MIN_SPAN {
            self.push_free(offset + need, cap - need);
        }

        offset
    }

    fn push_free(&mut self, offset: u32, len: u32) {
        let index = page_of(offset);
        let at = offset as usize % LOG_PAGE;
        let class = span_class(len);
        let space = self.spare.get_or_insert_with(Box::default);
        let lists = space.lists_mut(index);
        let page = page_in_mut(&mut self.extents, index);
        let head = lists.heads[class];

        write_u16(page, at, head);
        write_u16(page, at + 2, len as u16);
        lists.heads[class] = at as u16;

        if class == BIG_CLASS {
            write_u16(page, at + 4, NO_SPAN);

            if head != NO_SPAN {
                write_u16(page, head as usize + 4, at as u16);
            }

            space.big.insert((len, offset));
        } else if lists.listed & (1 << class) == 0 {
            lists.listed |= 1 << class;
            space.small[class].push(index as u32);
        }

        space.free_bytes += u64::from(len);
    }

    fn pop_small(&mut self, class: usize) -> Option<(u32, u32)> {
        let space = self.spare.as_mut()?;

        while let Some(&index) = space.small[class].last() {
            let lists = &mut space.lists[index as usize];
            let at = lists.heads[class];

            if at == NO_SPAN {
                lists.listed &= !(1 << class);
                space.small[class].pop();
                continue;
            }

            let page = page_in(&self.extents, index as usize);
            let next = read_u16(page, at as usize);
            let len = u32::from(read_u16(page, at as usize + 2));

            lists.heads[class] = next;
            space.free_bytes -= u64::from(len);

            return Some((index * LOG_PAGE as u32 + u32::from(at), len));
        }

        None
    }

    fn unlink_big(&mut self, offset: u32, len: u32) {
        let index = page_of(offset);
        let at = offset as usize % LOG_PAGE;
        let page = page_in_mut(&mut self.extents, index);
        let next = read_u16(page, at);
        let prev = read_u16(page, at + 4);
        let space = self.spare.as_mut().expect("listed span");

        if prev == NO_SPAN {
            space.lists[index].heads[BIG_CLASS] = next;
        } else {
            write_u16(page, prev as usize, next);
        }

        if next != NO_SPAN {
            write_u16(page, next as usize + 4, prev);
        }

        space.big.remove(&(len, offset));
        space.free_bytes -= u64::from(len);
    }

    /// Drop every free span of a page from the lists; small-class stacks drop the page
    /// lazily once they find its head empty.
    fn forget_lists(&mut self, index: usize) {
        let Some(space) = self.spare.as_mut() else {
            return;
        };

        if !self.extents[index / EXTENT_PAGES].pages[index % EXTENT_PAGES].open() {
            return;
        }

        let Some(lists) = space.lists.get_mut(index) else {
            return;
        };
        let page = page_in(&self.extents, index);

        for class in 0..SPAN_CLASSES {
            let mut at = lists.heads[class];

            while at != NO_SPAN {
                let len = u32::from(read_u16(page, at as usize + 2));

                if class == BIG_CLASS {
                    space
                        .big
                        .remove(&(len, (index * LOG_PAGE) as u32 + u32::from(at)));
                }

                space.free_bytes -= u64::from(len);
                at = read_u16(page, at as usize);
            }

            lists.heads[class] = NO_SPAN;
        }
    }

    fn release(&mut self, index: usize) {
        self.forget_lists(index);

        let space = self.spare.get_or_insert_with(Box::default);
        let extent = index / EXTENT_PAGES;
        let entry = &mut self.extents[extent];
        let meta = &mut entry.pages[index % EXTENT_PAGES];

        if meta.draining() {
            space.draining -= 1;
        }

        // Live 0, neither open nor draining.
        *meta = PageMeta::default();
        self.open_pages -= 1;
        space.released += 1;

        let ext = &mut entry.meta;

        ext.open -= 1;

        if ext.open == 0 {
            entry.bytes = None;
            ext.free = [0; MASK_WORDS];
            ext.draining = false;
            space.free_extents.push(extent as u32);

            // Untouched pages of a released last extent are given up with it.
            let first = (extent * EXTENT_PAGES) as u32;
            let end = first + EXTENT_PAGES as u32;

            if self.fresh > first && self.fresh < end {
                self.fresh = end;
            }
        } else {
            ext.set_free(index % EXTENT_PAGES);

            if !ext.listed && !ext.draining {
                ext.listed = true;
                space.partial.push(extent as u32);
            }
        }
    }

    pub(super) fn alloc_large(&mut self, len: usize) -> u32 {
        let large = self.large.get_or_insert_with(Box::default);

        if let Some(id) = large.free.pop() {
            large.blocks[id as usize] = Some(vec![0; len]);

            return LOG_LARGE | id;
        }

        let id = large.blocks.len() as u32;

        large.blocks.push(Some(vec![0; len]));
        LOG_LARGE | id
    }

    /// Bytes of a span. The page path is one bounds check on the extent table and one
    /// on the span; large spans and empty reads (offset 0 may name no extent) go cold.
    #[inline(always)]
    pub(super) fn bytes(&self, offset: u32, len: usize) -> &[u8] {
        let start = offset as usize;
        let at = start % EXTENT_BYTES;

        if offset & LOG_LARGE == 0
            && let Some(Extent {
                bytes: Some(bytes), ..
            }) = self.extents.get(start / EXTENT_BYTES)
            && let Some(span) = bytes.get(at..at + len)
        {
            return span;
        }

        self.bytes_cold(offset, len)
    }

    #[cold]
    #[inline(never)]
    fn bytes_cold(&self, offset: u32, len: usize) -> &[u8] {
        if len == 0 {
            return &[];
        }

        assert!(offset & LOG_LARGE != 0, "span outside live log extents");

        &self.large.as_ref().expect("large span").blocks[(offset ^ LOG_LARGE) as usize]
            .as_ref()
            .expect("large span")[..len]
    }

    #[inline]
    pub(super) fn bytes_mut(&mut self, offset: u32, len: usize) -> &mut [u8] {
        let start = offset as usize;
        let at = start % EXTENT_BYTES;

        let extent = start / EXTENT_BYTES;

        if offset & LOG_LARGE == 0
            && at + len <= EXTENT_BYTES
            && matches!(
                self.extents.get(extent),
                Some(Extent { bytes: Some(_), .. })
            )
        {
            let bytes = self.extents[extent]
                .bytes
                .as_deref_mut()
                .expect("checked above");

            return &mut bytes[at..at + len];
        }

        self.bytes_mut_cold(offset, len)
    }

    #[cold]
    #[inline(never)]
    fn bytes_mut_cold(&mut self, offset: u32, len: usize) -> &mut [u8] {
        if len == 0 {
            return &mut [];
        }

        assert!(offset & LOG_LARGE != 0, "span outside live log extents");

        &mut self.large.as_mut().expect("large span").blocks[(offset ^ LOG_LARGE) as usize]
            .as_mut()
            .expect("large span")[..len]
    }

    pub(crate) fn write_at(&mut self, offset: u32, value: &[u8]) {
        self.bytes_mut(offset, value.len()).copy_from_slice(value);
    }

    pub(crate) fn free_span(&mut self, offset: u32, len: u32) {
        if len == 0 {
            return;
        }

        if offset & LOG_LARGE != 0 {
            let id = offset ^ LOG_LARGE;

            // The allocator pools freed blocks; keeping them here only pinned memory.
            let large = self.large.as_mut().expect("large span");

            large.blocks[id as usize] = None;
            large.free.push(id);

            return;
        }

        let index = page_of(offset);
        let meta = self.page_mut(index);

        meta.sub_live(len as u16);

        let meta = *meta;

        self.live_bytes -= u64::from(len);

        if meta.live() == 0 && index as u32 != self.bump_page {
            self.release(index);
        } else if !meta.draining() && len >= MIN_SPAN {
            self.push_free(offset, len);
        }
    }

    pub(crate) fn free(&mut self, offset: u32, len: u16) {
        self.free_span(offset, u32::from(len));
    }

    /// Hints the CPU to load the cache line at `offset`; does nothing for large spans.
    #[inline]
    pub(crate) fn prefetch(&self, offset: u32) {
        let start = offset as usize;

        if offset & LOG_LARGE == 0
            && let Some(Extent {
                bytes: Some(bytes), ..
            }) = self.extents.get(start / EXTENT_BYTES)
            && let Some(byte) = bytes.get(start % EXTENT_BYTES)
        {
            crate::hashbrown::prefetch_read(byte);
        }
    }

    #[inline(always)]
    pub(crate) fn slice(&self, offset: u32, len: u16) -> &[u8] {
        self.bytes(offset, len as usize)
    }

    pub(super) fn read_integer(&self, offset: u32) -> i64 {
        let mut bytes = [0u8; 8];

        bytes.copy_from_slice(self.bytes(offset - 8, 8));
        i64::from_le_bytes(bytes)
    }

    pub(super) fn release_span(&mut self, offset: u32, len: u16) {
        if len == 0 {
            return;
        }

        if is_integer(len) {
            self.free(offset - 8, plain_len(len) + 8);

            return;
        }

        self.free(offset, len);
    }

    /// Pages of extent `extent` that were ever opened.
    fn touched(&self, extent: usize) -> usize {
        (self.fresh as usize)
            .saturating_sub(extent * EXTENT_PAGES)
            .min(EXTENT_PAGES)
    }

    /// Flags the open pages of the sparsest extents for draining when enough of the
    /// log is dead. Flagged pages take no new spans; `relocate` moves their live spans
    /// out, and the last move releases the page and then its extent. Returns the
    /// number of pages flagged.
    pub(crate) fn plan_drain(&mut self) -> usize {
        if self.plan_active() {
            return 0;
        }

        let touched: usize = (0..self.extents.len())
            .filter(|&extent| self.extents[extent].bytes.is_some())
            .map(|extent| self.touched(extent))
            .sum();
        let dead = ((touched * LOG_PAGE) as u64).saturating_sub(self.live_bytes);

        if dead < DRAIN_MIN_DEAD || dead * 3 <= self.live_bytes {
            return 0;
        }

        let bump_extent =
            (self.bump_page != NO_PAGE).then_some(self.bump_page as usize / EXTENT_PAGES);
        let mut candidates: Vec<(usize, u32)> = (0..self.extents.len())
            .filter(|&extent| self.extents[extent].bytes.is_some() && Some(extent) != bump_extent)
            .filter_map(|extent| {
                let pages = &self.extents[extent].pages;
                let live: usize = pages.iter().map(|meta| meta.live() as usize).sum();

                (live * 4 < self.touched(extent) * LOG_PAGE * 3).then_some((live, extent as u32))
            })
            .collect();
        let most = (DRAIN_MAX_PAGES / EXTENT_PAGES).max(1);

        if candidates.len() > most {
            candidates.select_nth_unstable(most);
            candidates.truncate(most);
        }

        let mut flagged = 0;

        for &(_, extent) in &candidates {
            let extent = extent as usize;

            self.extents[extent].meta.draining = true;

            for index in extent * EXTENT_PAGES..(extent + 1) * EXTENT_PAGES {
                if self.page(index).open() {
                    self.forget_lists(index);
                    self.page_mut(index).set_flag(PAGE_DRAINING, true);
                    flagged += 1;
                }
            }
        }

        if flagged > 0 {
            self.spare.get_or_insert_with(Box::default).draining = flagged as u32;
        }

        flagged
    }

    /// Ends a drain round. Pages that still hold spans take allocations again; their
    /// free space was dropped from the lists and comes back when they drain next time.
    pub(crate) fn end_drain(&mut self) {
        if !self.plan_active() {
            return;
        }

        for entry in &mut self.extents {
            for meta in &mut entry.pages {
                meta.set_flag(PAGE_DRAINING, false);
            }
        }

        let space = self.spare.as_mut().expect("drain round");

        space.draining = 0;

        for (extent, entry) in self.extents.iter_mut().enumerate() {
            let meta = &mut entry.meta;

            if meta.draining {
                meta.draining = false;

                if meta.has_free() && !meta.listed {
                    meta.listed = true;
                    space.partial.push(extent as u32);
                }
            }
        }
    }

    /// True while a drain round has flagged pages.
    pub(crate) fn plan_active(&self) -> bool {
        self.spare.as_ref().is_some_and(|space| space.draining > 0)
    }

    pub(crate) fn is_draining(&self, offset: u32) -> bool {
        self.plan_active()
            && offset & LOG_LARGE == 0
            && self
                .extents
                .get(page_of(offset) / EXTENT_PAGES)
                .is_some_and(|entry| entry.pages[page_of(offset) % EXTENT_PAGES].draining())
    }

    /// Moves a span off a draining page and frees the old one. The caller rewrites
    /// the single reference to it.
    pub(crate) fn relocate(&mut self, offset: u32, len: u32) -> u32 {
        let mut copy = [0u8; LOG_PAGE];
        let len_usize = len as usize;

        copy[..len_usize].copy_from_slice(self.bytes(offset, len_usize));

        let moved = self.alloc_span(len_usize);

        debug_assert!(!self.is_draining(moved));
        self.write_at(moved, &copy[..len_usize]);
        self.free_span(offset, len);
        self.spare.get_or_insert_with(Box::default).relocated += u64::from(len);
        moved
    }

    /// Moves a value span named by a `ByteBody::Log` offset and length when it sits on
    /// a draining page. Returns the new offset.
    pub(crate) fn relocate_value(&mut self, offset: u32, len: u16) -> Option<u32> {
        let (start, span) = if is_integer(len) {
            (offset.wrapping_sub(8), u32::from(plain_len(len)) + 8)
        } else {
            (offset, u32::from(len))
        };

        if span == 0 || !self.is_draining(start) {
            return None;
        }

        Some(self.relocate(start, span) + (offset - start))
    }

    pub(crate) fn stats(&self) -> LogStats {
        let space = self.spare.as_deref();

        LogStats {
            pages: u64::from(self.open_pages),
            extents: self
                .extents
                .iter()
                .filter(|extent| extent.bytes.is_some())
                .count() as u64,
            free_bytes: space.map_or(0, |space| space.free_bytes),
            pages_released: space.map_or(0, |space| space.released),
            relocated_bytes: space.map_or(0, |space| space.relocated),
        }
    }

    pub(crate) fn live_bytes(&self) -> u64 {
        self.live_bytes
    }

    #[cfg(test)]
    pub(crate) fn pages_in_use(&self) -> usize {
        self.open_pages as usize
    }

    /// Checks counters and lists against the pages; test only.
    #[cfg(test)]
    pub(crate) fn check_invariants(&self) {
        let empty = FreeSpace::default();
        let space = self.spare.as_deref().unwrap_or(&empty);
        let mut live = 0u64;
        let mut free = 0u64;
        let mut big = 0usize;
        let mut open = 0u32;

        for index in 0..self.extents.len() * EXTENT_PAGES {
            let meta = self.page(index);
            let heads = space.heads(index);

            live += u64::from(meta.live());

            if !meta.open() {
                assert_eq!(meta.live(), 0);
                assert!(!meta.draining());
                assert!(heads.iter().all(|&head| head == NO_SPAN));
                continue;
            }

            open += 1;
            assert!(self.extents[index / EXTENT_PAGES].bytes.is_some());

            let page = page_in(&self.extents, index);

            let mut spans = Vec::new();

            for (class, &head) in heads.iter().enumerate() {
                let mut at = head;
                let mut prev = NO_SPAN;

                while at != NO_SPAN {
                    let len = u32::from(read_u16(page, at as usize + 2));

                    assert_eq!(span_class(len), class);
                    if class == BIG_CLASS {
                        assert_eq!(read_u16(page, at as usize + 4), prev);
                        assert!(
                            space
                                .big
                                .contains(&(len, (index * LOG_PAGE) as u32 + u32::from(at)))
                        );
                        big += 1;
                    } else {
                        assert!(space.lists[index].listed & (1 << class) != 0);
                        assert!(space.small[class].contains(&(index as u32)));
                    }

                    spans.push((at as usize, len as usize));
                    free += u64::from(len);
                    prev = at;
                    at = read_u16(page, at as usize);
                }
            }

            spans.sort_unstable();
            for pair in spans.windows(2) {
                assert!(pair[0].0 + pair[0].1 <= pair[1].0, "free spans overlap");
            }

            let listed: usize = spans.iter().map(|span| span.1).sum();

            assert!(listed + meta.live() as usize <= LOG_PAGE);
        }

        assert_eq!(live, self.live_bytes);
        assert_eq!(open, self.open_pages);

        for entry in &self.extents {
            assert_eq!(
                entry.meta.open as usize,
                entry.pages.iter().filter(|page| page.open()).count()
            );
        }
        assert_eq!(free, space.free_bytes);
        assert_eq!(big, space.big.len());
    }
}

impl ValueLog {
    pub(super) fn rewrite(&mut self, body: &ByteBody, value: &[u8]) -> ByteBody {
        if let ByteBody::Log { offset, len } = *body
            && is_integer(len)
        {
            self.release_span(offset, len);

            return self.store_plain(value);
        }

        if value.len() <= LOG_VALUE_MAX {
            if let ByteBody::Log { offset, len } = *body
                && value.len() <= plain_len(len) as usize
            {
                self.write_at(offset, value);

                if value.len() < plain_len(len) as usize {
                    self.free(
                        offset + value.len() as u32,
                        plain_len(len) - value.len() as u16,
                    );
                }

                return ByteBody::Log {
                    offset,
                    len: value.len() as u16,
                };
            }

            if let ByteBody::Log { offset, len } = *body {
                self.release_span(offset, len);
            }

            return self.store_plain(value);
        }

        if let ByteBody::Log { offset, len } = *body {
            self.release_span(offset, len);
        }

        ByteBody::Heap(Box::new(heap_copy(value)))
    }

    pub(super) fn store_plain(&mut self, value: &[u8]) -> ByteBody {
        if value.len() <= LOG_VALUE_MAX {
            let (offset, len) = self.alloc(value);

            return ByteBody::Log { offset, len };
        }

        ByteBody::Heap(Box::new(heap_copy(value)))
    }
}

impl BytesValue {
    pub(super) fn logged(offset: u32, len: u16) -> Self {
        Self {
            body: ByteBody::Log { offset, len },
        }
    }

    pub(super) fn from_heap(data: Vec<u8>) -> Self {
        Self {
            body: ByteBody::Heap(Box::new(data)),
        }
    }

    pub(super) fn view<'a>(&'a self, log: &'a ValueLog) -> ByteView<'a> {
        match &self.body {
            ByteBody::Log { offset, len } => {
                ByteView::Borrowed(log.slice(*offset, plain_len(*len)))
            }

            ByteBody::Heap(data) => ByteView::Borrowed(data.as_slice()),
            ByteBody::Integer(value) => ByteView::Integer(*value),
        }
    }

    pub(super) fn charged(&self) -> usize {
        match &self.body {
            ByteBody::Log { len, .. } => plain_len(*len) as usize,
            ByteBody::Heap(data) => data.capacity(),
            ByteBody::Integer(_) => 0,
        }
    }

    pub(super) fn into_boxed(self, log: &ValueLog) -> Box<[u8]> {
        match self.body {
            ByteBody::Log { offset, len } => Box::from(log.slice(offset, plain_len(len))),
            ByteBody::Heap(data) => data.into_boxed_slice(),
            ByteBody::Integer(value) => integer_bytes(value).into_boxed_slice(),
        }
    }

    /// [`Self::into_boxed`] that also frees the log span.
    pub(super) fn take_boxed(self, log: &mut ValueLog) -> Box<[u8]> {
        if let ByteBody::Log { offset, len } = self.body {
            let boxed = Box::from(log.slice(offset, plain_len(len)));

            log.release_span(offset, len);
            return boxed;
        }

        self.into_boxed(log)
    }

    /// Frees the value without keeping a heap buffer for reuse.
    pub(super) fn discard(self, log: &mut ValueLog) {
        if let ByteBody::Log { offset, len } = self.body {
            log.release_span(offset, len);
        }
    }

    pub(super) fn release(self, log: &mut ValueLog, pool: &mut BytePool) {
        match self.body {
            ByteBody::Log { offset, len } => log.release_span(offset, len),
            ByteBody::Heap(data) => pool.release(*data),
            ByteBody::Integer(_) => {}
        }
    }
}

/// A string body borrowed from the shard, or a counter that is formatted on read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteView<'a> {
    Borrowed(&'a [u8]),
    Integer(i64),
}

impl<'a> ByteView<'a> {
    pub fn to_vec(self) -> Vec<u8> {
        match self {
            Self::Borrowed(bytes) => bytes.to_vec(),
            Self::Integer(value) => integer_bytes(value),
        }
    }

    pub fn text_len(self) -> usize {
        match self {
            Self::Borrowed(bytes) => bytes.len(),
            Self::Integer(value) => integer_text_len(value),
        }
    }
}

pub(crate) fn format_integer(value: i64, output: &mut [u8; 20]) -> usize {
    let (start, digits) = integer_digits(value);
    let text = &digits[start..];

    output[..text.len()].copy_from_slice(text);
    text.len()
}

pub(super) fn integer_text_len(value: i64) -> usize {
    let (start, digits) = integer_digits(value);

    digits.len() - start
}

/// A value stored for one key.
///
/// Expiration and eviction metadata are not stored here. A string with no TTL
/// under `noeviction` would otherwise carry those fields on every key.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub(super) value: Value,
}

impl Entry {
    pub(super) fn fresh(value: Value) -> Self {
        Self { value }
    }

    pub(crate) fn slab_body(&self) -> Option<(u16, crate::index::SlabBuf)> {
        use crate::index::SlabBuf;

        match &self.value {
            Value::Set(set) => set
                .slab_bytes()
                .map(|bytes| (TAG_SET_SLAB, SlabBuf::raw(bytes))),
            Value::SortedSet(set) => set
                .slab_bytes()
                .map(|bytes| (TAG_ZSET_SLAB, SlabBuf::raw(bytes))),
            Value::List(list) => list
                .slab_record()
                .map(|record| (crate::index::TAG_COLL_SLAB, record)),
            Value::Hash(hash) => hash
                .slab_record()
                .map(|record| (crate::index::TAG_COLL_SLAB, record)),
            _ => None,
        }
    }

    /// A small list or hash read back from its slab record.
    pub(crate) fn from_slab_coll(slot: &[u8]) -> Self {
        let (subkind, count, data) = crate::index::coll_parts(slot);

        if subkind == crate::index::COLL_LIST {
            Self::fresh(Value::List(Box::new(ListValue::from_slab(count, data))))
        } else {
            Self::fresh(Value::Hash(Box::new(HashValue::from_slab(count, data))))
        }
    }

    pub(crate) fn from_slab_set(bytes: &[u8]) -> Self {
        Self::fresh(Value::Set(Box::new(SetValue::from_flat(bytes))))
    }

    pub(crate) fn from_slab_sorted(bytes: &[u8]) -> Self {
        Self::fresh(Value::SortedSet(Box::new(SortedSet::from_flat(bytes))))
    }

    pub(crate) fn into_packed(self) -> Packed {
        match self.value {
            Value::Bytes(value) => Packed::Bytes(match value.body {
                ByteBody::Log { offset, len } => PackedBytes::Log { offset, len },
                ByteBody::Integer(value) => PackedBytes::Integer(value),
                ByteBody::Heap(data) => PackedBytes::Heap(*data),
            }),
            value => Packed::Object(Self { value }),
        }
    }

    pub(crate) fn from_packed_bytes(body: PackedBytes) -> Self {
        let body = match body {
            PackedBytes::Log { offset, len } => ByteBody::Log { offset, len },
            PackedBytes::Integer(value) => ByteBody::Integer(value),
            PackedBytes::Heap(data) => ByteBody::Heap(Box::new(data)),
        };

        Self {
            value: Value::Bytes(BytesValue { body }),
        }
    }
}

pub(crate) enum PackedBytes {
    Log { offset: u32, len: u16 },
    Integer(i64),
    Heap(Vec<u8>),
}

pub(crate) enum Packed {
    Bytes(PackedBytes),
    Object(Entry),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct EntryCharge {
    pub(super) key_bytes: usize,
    pub(super) value_bytes: usize,
    pub(super) collection_bytes: usize,
}

/// A database map taken out of its shard by `take_database`.
#[derive(Debug)]
pub(crate) struct DetachedDatabase<I> {
    pub(super) database: u8,
    pub(super) map: I,
    pub(super) charge: EntryCharge,
    pub(super) deadlines: u64,
    pub(super) deadline_index: Box<DeadlineDb>,
}

/// What a database emptied inside `MULTI` held beside its entries, by `detach_side_state`.
pub(crate) struct DetachedSide {
    pub(super) database: u8,
    pub(super) marks: super::fill::DetachedMarks,
    pub(super) sweep: Vec<(u64, u8, Box<[u8]>)>,
    pub(super) json: DetachedJson,
}

pub(super) enum DetachedJson {
    /// Database 0 holds the definitions: the whole registry goes with it.
    Registry(Option<Box<crate::json::index::JsonIndexes>>),
    Postings(Option<crate::json::index::DetachedPostings>),
}

/// A heap string's buffer, allocated as the allocator's whole block: the capacity is what
/// the string is charged.
pub(super) fn heap_copy(value: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(alloc_size(value.len()));

    buffer.extend_from_slice(value);
    buffer
}

/// The allocator's block for a boxed `T`.
const fn boxed<T>() -> usize {
    alloc_size(size_of::<T>())
}

impl EntryCharge {
    pub(super) fn for_entry(key: &[u8], entry: &Entry) -> Self {
        // A list or hash that fits a slab record is charged the same whether it is currently
        // stored there or briefly promoted to an object, so promoting never moves the total.
        let slab_len = match &entry.value {
            Value::List(list) => list.slab_len(),
            Value::Hash(hash) => hash.slab_len(),
            _ => None,
        };

        if let Some(len) = slab_len {
            return Self::slab(key, len);
        }

        let (value_bytes, collection_bytes) = match &entry.value {
            Value::Bytes(value) => return Self::bytes(key, value),
            Value::Json(value) => (value.capacity(), boxed::<Vec<u8>>()),
            Value::JsonTree(value) => (value.charge(), boxed::<crate::json::TreeDocument>()),
            Value::List(list) => (
                list.payload_bytes,
                list.capacity_bytes().saturating_add(boxed::<ListValue>()),
            ),
            Value::Set(set) => (
                set.payload_bytes,
                set.capacity_bytes().saturating_add(boxed::<SetValue>()),
            ),
            Value::Hash(hash) => (
                hash.payload_bytes(),
                hash.collection_bytes().saturating_add(boxed::<HashValue>()),
            ),
            Value::TimedHash(timed) => (
                timed.hash.payload_bytes(),
                timed
                    .hash
                    .collection_bytes()
                    .saturating_add(timed.extra_bytes()),
            ),
            Value::SortedSet(set) => (set.payload_bytes, set.collection_overhead()),

            Value::BloomFilter(filter) => (filter.payload_bytes(), boxed::<BloomFilter>()),
            Value::Stream(stream) => (stream.payload_bytes(), stream.collection_bytes()),
            Value::Lease(_) => (16, boxed::<LeaseValue>()),
            Value::Limit(_) => (8, boxed::<LimitValue>()),
            Value::Semaphore(semaphore) => (
                semaphore.len().saturating_mul(16),
                boxed::<SemaphoreValue>(),
            ),
            Value::Once => (0, 0),
            Value::Versioned(value) => (
                value.body.charged().saturating_add(8),
                boxed::<VersionedValue>(),
            ),
        };

        // The object's table slot rides on the record, so every object pays it.
        Self {
            key_bytes: key.len() + RECORD_SLOT,
            value_bytes,
            collection_bytes: collection_bytes.saturating_add(OBJECT_SLOT),
        }
    }

    /// A string: its key-log record (header, key and inline payload) and, for a heap
    /// string, its buffer's capacity and the slot that owns it. Buffers are allocated
    /// in allocator classes (see `BytePool::acquire`), so the capacity is the block.
    #[inline]
    pub(super) fn bytes(key: &[u8], value: &BytesValue) -> Self {
        match &value.body {
            ByteBody::Log { len, .. } => Self::logged(key, plain_len(*len) as usize),
            ByteBody::Heap(data) => Self::heap(key, data.capacity()),
            ByteBody::Integer(_) => Self::integer(key),
        }
    }

    #[inline]
    pub(super) fn logged(key: &[u8], len: usize) -> Self {
        Self {
            key_bytes: key.len() + RECORD_LOG,
            value_bytes: len,
            collection_bytes: 0,
        }
    }

    #[inline]
    pub(super) fn integer(key: &[u8]) -> Self {
        Self {
            key_bytes: key.len() + RECORD_INTEGER,
            value_bytes: 0,
            collection_bytes: 0,
        }
    }

    #[inline]
    pub(super) fn heap(key: &[u8], capacity: usize) -> Self {
        Self {
            key_bytes: key.len() + RECORD_SLOT,
            value_bytes: capacity + HEAP_SLOT,
            collection_bytes: 0,
        }
    }

    /// A small collection in a slab slot of the class that fits `len` (the classes follow
    /// the allocator's steps from 16 bytes).
    #[inline]
    pub(super) fn slab(key: &[u8], len: usize) -> Self {
        Self {
            key_bytes: key.len() + RECORD_SLOT,
            value_bytes: alloc_size(len).max(16),
            collection_bytes: 0,
        }
    }
}
