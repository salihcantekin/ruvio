use std::time::{Duration, SystemTime};

/// A sorted-set member and its score.
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
    /// The member.
    pub member: String,
    /// The member's score.
    pub score: f64,
}

impl SortedSetEntry {
    /// A member and its score.
    pub fn new(member: impl Into<String>, score: f64) -> Self {
        Self {
            member: member.into(),
            score,
        }
    }
}

impl<S: Into<String>> From<(S, f64)> for SortedSetEntry {
    fn from((member, score): (S, f64)) -> Self {
        Self::new(member, score)
    }
}

/// When `HEXPIRE` and its siblings may change a field's TTL. A field without a TTL counts
/// as an infinite TTL for `IfGreater` and `IfLess`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldExpireCondition {
    /// Always set the TTL.
    #[default]
    Always,
    /// Only when the field has no TTL (`NX`).
    IfNoTtl,
    /// Only when the field already has a TTL (`XX`).
    IfTtl,
    /// Only when the new deadline is later (`GT`).
    IfGreater,
    /// Only when the new deadline is earlier (`LT`).
    IfLess,
}

/// `ZADD` options. The server refuses `NX` with `XX`, `GT`, or `LT`, and `GT` with `LT`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SortedSetAddOptions {
    /// Only add new members (`NX`).
    pub if_not_exists: bool,
    /// Only update existing members (`XX`).
    pub if_exists: bool,
    /// Only update when the new score is greater (`GT`). A missing member is still added.
    pub greater_than: bool,
    /// Only update when the new score is lower (`LT`). A missing member is still added.
    pub less_than: bool,
    /// Count changed members as well as new ones (`CH`).
    pub changed: bool,
    /// Deadline for the key, sent as `EX` / `PX`; set only when the call creates the key (an
    /// existing key keeps its deadline). A zero `Duration` is refused before sending.
    pub ttl: Option<Duration>,
}

/// `XREAD` and `XREADGROUP` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamReadOptions {
    /// Most entries to return per stream (`COUNT`).
    pub count: Option<u64>,
    /// How long to wait for a new entry (`BLOCK`). `Duration::ZERO` waits until an entry arrives.
    /// The connection sends nothing else while it waits.
    pub block: Option<Duration>,
    /// `XREADGROUP` only: advance the group without adding pending entries (`NOACK`).
    pub no_ack: bool,
}

/// `XCLAIM` options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamClaimOptions {
    /// Set the claimed entries' idle time (`IDLE`). Cannot be combined with [`Self::time`].
    pub idle: Option<Duration>,
    /// Set the claimed entries' last delivery time (`TIME`).
    pub time: Option<SystemTime>,
    /// Set the delivery count (`RETRYCOUNT`).
    pub retry_count: Option<u64>,
    /// Claim ids that are in the stream but not pending (`FORCE`).
    pub force: bool,
    /// Move the group's last delivered id (`LASTID`).
    pub last_id: Option<String>,
}

/// Optional narrowing for the extended `XPENDING` form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamPendingFilter {
    /// Only entries owned by this consumer.
    pub consumer: Option<String>,
    /// Only entries idle for at least this long (`IDLE`).
    pub min_idle: Option<Duration>,
}

/// One row of the extended `XPENDING` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPendingEntry {
    /// Entry id.
    pub id: String,
    /// Consumer that owns the entry.
    pub consumer: String,
    /// Milliseconds since the entry was last delivered.
    pub idle_millis: i64,
    /// How many times the entry was delivered.
    pub delivery_count: i64,
}

/// One `SCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Keys on this page.
    pub keys: Vec<String>,
}

/// One `HSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Field and value pairs on this page.
    pub fields: Vec<(String, String)>,
}

/// One stream entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    /// Entry id.
    pub id: String,
    /// Field and value pairs in the order they were added.
    pub fields: Vec<(String, String)>,
}

/// Entries read from one stream key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadResult {
    /// Stream key.
    pub key: String,
    /// Entries read from the key.
    pub entries: Vec<StreamEntry>,
}

/// A Pub/Sub delivery: `message`, `pmessage`, or `smessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
    /// `message`, `pmessage`, or `smessage`.
    pub kind: String,
    /// The matching pattern for `pmessage`.
    pub pattern: Option<String>,
    /// Channel the message was published to.
    pub channel: String,
    /// Raw payload.
    pub payload: Vec<u8>,
}

/// The answer to `FILLGET`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillOutcome {
    /// The value is stored and fresh.
    Hit(String),
    /// Nothing is stored and this caller must build the value, then call `FILLSET` with the token.
    Fill(String),
    /// Nothing is stored and another caller is building it.
    Busy,
    /// The value is past its soft TTL. `token` is `Some` when this caller must refresh it.
    Stale {
        /// The old value, still served.
        value: String,
        /// The refresh token, or `None` when someone else is refreshing.
        token: Option<String>,
    },
}

/// One `SSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Members on this page.
    pub members: Vec<String>,
}

/// One `ZSCAN` page. A cursor of 0 means the scan is complete.
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetScanPage {
    /// Cursor for the next call.
    pub cursor: u64,
    /// Members and scores on this page.
    pub entries: Vec<SortedSetEntry>,
}

/// `BF.RESERVE` options beyond error rate and capacity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BloomReserveOptions {
    /// Growth factor of each new layer (`EXPANSION`).
    pub expansion: Option<u64>,
    /// One fixed layer; a full filter answers an error (`NONSCALING`).
    pub non_scaling: bool,
}

/// `BF.INSERT` options. The creation options apply only when the key is new.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BloomInsertOptions {
    /// `CAPACITY` for a new filter.
    pub capacity: Option<u64>,
    /// `ERROR` rate for a new filter.
    pub error_rate: Option<f64>,
    /// `EXPANSION` for a new filter.
    pub expansion: Option<u64>,
    /// `NOCREATE`: a missing key is an error instead of a new filter.
    pub no_create: bool,
    /// `NONSCALING` for a new filter.
    pub non_scaling: bool,
    /// Deadline for the key, sent as `EX` / `PX`; set only when the call creates the filter (an
    /// existing key keeps its deadline). A zero `Duration` is refused before sending.
    pub ttl: Option<Duration>,
}

/// The `BF.INFO` reply.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BloomInfo {
    /// Items the first layer was sized for.
    pub capacity: i64,
    /// Bytes: the bit arrays plus the filter header.
    pub size: i64,
    /// Number of layers.
    pub filters: i64,
    /// Items that set at least one new bit.
    pub items: i64,
    /// Growth factor; `None` for a non-scaling filter.
    pub expansion: Option<i64>,
}

/// Value type of a `JSON.INDEX` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonIndexType {
    /// `TYPE string`.
    String,
    /// `TYPE number`.
    Number,
    /// `TYPE bool`.
    Bool,
}

/// When `JSON.SET` may write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JsonSetCondition {
    /// Always write.
    #[default]
    Always,
    /// Only when the path is missing (`NX`).
    IfMissing,
    /// Only when the path exists (`XX`).
    IfExists,
}

/// The `BITOP` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOperation {
    /// `AND`.
    And,
    /// `OR`.
    Or,
    /// `XOR`.
    Xor,
    /// `NOT` (one source key).
    Not,
}

impl BitOperation {
    /// The wire keyword (`AND`, `OR`, `XOR`, `NOT`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Xor => "XOR",
            Self::Not => "NOT",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The unit of a `BITCOUNT` / `BITPOS` range.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BitUnit {
    /// Byte offsets (the default).
    #[default]
    Byte,
    /// Bit offsets (`BIT`).
    Bit,
}

impl BitUnit {
    /// The wire keyword (`BYTE` or `BIT`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Byte => "BYTE",
            Self::Bit => "BIT",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// How `ZUNION`, `ZINTER` and their `STORE` forms combine scores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Aggregate {
    /// Add the scores (the default).
    #[default]
    Sum,
    /// Keep the lowest score.
    Min,
    /// Keep the highest score.
    Max,
}

impl Aggregate {
    /// The wire keyword (`SUM`, `MIN`, `MAX`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// `WEIGHTS` and `AGGREGATE` for `ZUNION` / `ZINTER` / `ZUNIONSTORE` / `ZINTERSTORE`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SortedSetCombine<'a> {
    /// One multiplier per input key; empty sends no `WEIGHTS`.
    pub weights: &'a [f64],
    /// `AGGREGATE`; `None` leaves the server default (`SUM`).
    pub aggregate: Option<Aggregate>,
}

/// What the bounds of [`crate::Client::zrange_query`] mean.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RangeBy {
    /// Ranks (indexes).
    #[default]
    Rank,
    /// Scores (`BYSCORE`): `-inf`, `+inf`, `(` for exclusive.
    Score,
    /// Members (`BYLEX`): `-`, `+`, `[member`, `(member`.
    Lex,
}

/// `ZRANGE` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeOptions {
    /// What the bounds mean.
    pub by: RangeBy,
    /// Highest first (`REV`). Bounds are sent exactly as given.
    pub rev: bool,
    /// `LIMIT offset count` (only with `Score` or `Lex`). A negative count returns the rest.
    pub limit: Option<(i64, i64)>,
}

/// The answer to `IDEM key BEGIN`, returned by
/// [`Client::idem_begin`](crate::Client::idem_begin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdemState {
    /// This caller owns the reservation and must run the work, then `COMPLETE`.
    Acquired,
    /// Another owner holds a live reservation. Do not run the work.
    Pending,
    /// The key holds a different fingerprint.
    Conflict,
    /// The work already finished; the stored result.
    Completed(Vec<u8>),
}

/// `RESTORE` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// Overwrite an existing key (`REPLACE`).
    pub replace: bool,
    /// The TTL is a Unix-millisecond deadline (`ABSTTL`).
    pub absolute_ttl: bool,
}

/// A distance unit for the geo commands.
///
/// # Examples
///
/// ```
/// use ruvio_client::GeoUnit;
///
/// assert_eq!(GeoUnit::Kilometers.as_str(), "km");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters (`m`), the default of `GEODIST`.
    #[default]
    Meters,
    /// Kilometers (`km`).
    Kilometers,
    /// Feet (`ft`).
    Feet,
    /// Miles (`mi`).
    Miles,
}

impl GeoUnit {
    /// The wire keyword: `m`, `km`, `ft`, or `mi`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Meters => "m",
            Self::Kilometers => "km",
            Self::Feet => "ft",
            Self::Miles => "mi",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The result order of the geo search commands.
///
/// # Examples
///
/// ```
/// use ruvio_client::GeoOrder;
///
/// assert_eq!(GeoOrder::Desc.as_str(), "DESC");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GeoOrder {
    /// Nearest first (`ASC`).
    #[default]
    Asc,
    /// Farthest first (`DESC`).
    Desc,
}

impl GeoOrder {
    /// The wire keyword: `ASC` or `DESC`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// Where `LINSERT` places the new element relative to the pivot.
///
/// # Examples
///
/// ```
/// use ruvio_client::InsertPosition;
///
/// assert_eq!(InsertPosition::Before.as_str(), "BEFORE");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertPosition {
    /// In front of the pivot (`BEFORE`).
    Before,
    /// Behind the pivot (`AFTER`).
    After,
}

impl InsertPosition {
    /// The wire keyword: `BEFORE` or `AFTER`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// An end of a list, for `LMOVE`, `BLMOVE`, `LMPOP` and `BLMPOP`.
///
/// # Examples
///
/// ```
/// use ruvio_client::ListSide;
///
/// assert_eq!(ListSide::Left.as_str(), "LEFT");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSide {
    /// The head (`LEFT`).
    Left,
    /// The tail (`RIGHT`).
    Right,
}

impl ListSide {
    /// The wire keyword: `LEFT` or `RIGHT`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// A value kind for the `TYPE` option of [`crate::Client::scan`].
///
/// # Examples
///
/// ```
/// use ruvio_client::KeyType;
///
/// assert_eq!(KeyType::SortedSet.as_str(), "zset");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// `string`.
    String,
    /// `json`.
    Json,
    /// `list`.
    List,
    /// `set`.
    Set,
    /// `hash`.
    Hash,
    /// `zset`.
    SortedSet,
    /// `bloomfilter`.
    BloomFilter,
    /// `stream`.
    Stream,
    /// `lease`.
    Lease,
    /// `limit`.
    Limit,
    /// `semaphore`.
    Semaphore,
    /// `once`.
    Once,
}

impl KeyType {
    /// The wire name, as the server reports it in `TYPE`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Json => "json",
            Self::List => "list",
            Self::Set => "set",
            Self::Hash => "hash",
            Self::SortedSet => "zset",
            Self::BloomFilter => "bloomfilter",
            Self::Stream => "stream",
            Self::Lease => "lease",
            Self::Limit => "limit",
            Self::Semaphore => "semaphore",
            Self::Once => "once",
        }
    }

    /// The wire name as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The `OVERFLOW` mode of `BITFIELD` writes.
///
/// # Examples
///
/// ```
/// use ruvio_client::BitFieldOverflow;
///
/// assert_eq!(BitFieldOverflow::Sat.as_str(), "SAT");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BitFieldOverflow {
    /// Wrap around (`WRAP`, the default).
    #[default]
    Wrap,
    /// Clamp to the limits (`SAT`).
    Sat,
    /// Refuse the write and reply nil (`FAIL`).
    Fail,
}

impl BitFieldOverflow {
    /// The wire keyword: `WRAP`, `SAT`, or `FAIL`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wrap => "WRAP",
            Self::Sat => "SAT",
            Self::Fail => "FAIL",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The integer type of a `BITFIELD` field: `i1` to `i64` (signed) or `u1` to `u63` (unsigned).
///
/// # Examples
///
/// ```
/// use ruvio_client::BitFieldType;
///
/// assert_eq!(BitFieldType::unsigned(8).unwrap().as_string(), "u8");
/// assert!(BitFieldType::unsigned(64).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitFieldType {
    signed: bool,
    bits: u8,
}

impl BitFieldType {
    /// A signed type of `bits` bits (`i<bits>`). `None` unless `bits` is 1 to 64.
    pub const fn signed(bits: u8) -> Option<Self> {
        if bits == 0 || bits > 64 {
            return None;
        }

        Some(Self { signed: true, bits })
    }

    /// An unsigned type of `bits` bits (`u<bits>`). `None` unless `bits` is 1 to 63.
    pub const fn unsigned(bits: u8) -> Option<Self> {
        if bits == 0 || bits > 63 {
            return None;
        }

        Some(Self {
            signed: false,
            bits,
        })
    }

    /// The width in bits.
    pub const fn bits(self) -> u8 {
        self.bits
    }

    /// True for `i<n>`.
    pub const fn is_signed(self) -> bool {
        self.signed
    }

    /// The type written into a stack buffer, such as `i16`.
    pub(crate) fn text(self) -> BitFieldText {
        let mut text = BitFieldText {
            buffer: [0; 3],
            length: 0,
        };

        text.buffer[0] = if self.signed { b'i' } else { b'u' };

        if self.bits >= 10 {
            text.buffer[1] = b'0' + self.bits / 10;
            text.buffer[2] = b'0' + self.bits % 10;
            text.length = 3;
        } else {
            text.buffer[1] = b'0' + self.bits;
            text.length = 2;
        }

        text
    }

    /// The wire text, such as `u8` or `i16`.
    pub fn as_string(self) -> String {
        self.text().as_str().to_owned()
    }
}

/// A `BITFIELD` type name held in a stack buffer.
pub(crate) struct BitFieldText {
    buffer: [u8; 3],
    length: usize,
}

impl BitFieldText {
    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buffer[..self.length]).unwrap_or("u8")
    }
}

/// Where a `BITFIELD` field starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldOffset {
    /// A bit offset (`100`).
    Bit(u64),
    /// The n-th field of the type's width (`#3`).
    Field(u64),
}

/// One `BITFIELD` subcommand.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// let u8_type = BitFieldType::unsigned(8).unwrap();
/// let replies = client.bitfield(
///     "counters",
///     &[
///         BitFieldOp::Overflow(BitFieldOverflow::Sat),
///         BitFieldOp::IncrBy { ty: u8_type, offset: BitFieldOffset::Bit(0), delta: 10 },
///         BitFieldOp::Get { ty: u8_type, offset: BitFieldOffset::Bit(0) },
///     ],
/// )?;
/// # let _ = replies;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldOp {
    /// `GET type offset`.
    Get {
        /// The field type.
        ty: BitFieldType,
        /// The field position.
        offset: BitFieldOffset,
    },
    /// `SET type offset value`.
    Set {
        /// The field type.
        ty: BitFieldType,
        /// The field position.
        offset: BitFieldOffset,
        /// The value to store.
        value: i64,
    },
    /// `INCRBY type offset delta`.
    IncrBy {
        /// The field type.
        ty: BitFieldType,
        /// The field position.
        offset: BitFieldOffset,
        /// The amount to add.
        delta: i64,
    },
    /// `OVERFLOW mode`, which applies to the writes after it.
    Overflow(BitFieldOverflow),
}

/// Where a `GEOSEARCH` starts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoFrom<'a> {
    /// `FROMMEMBER member`: centre on an existing member of the index.
    Member(&'a str),
    /// `FROMLONLAT longitude latitude` in degrees. Longitude is first, -180 to 180; latitude
    /// -85.05112878 to 85.05112878.
    LonLat(f64, f64),
}

/// The area of a `GEOSEARCH`, measured in the request's unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoShape {
    /// `BYRADIUS radius`: a circle.
    Radius(f64),
    /// `BYBOX width height`: an axis-aligned rectangle centred on the origin.
    Box(f64, f64),
}

/// A full `GEOSEARCH` request for [`Client::geo_search_with`](crate::Client::geo_search_with).
/// Build a radius search with [`GeoSearch::radius`] and set the other fields as needed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoSearch<'a> {
    /// Center.
    pub from: GeoFrom<'a>,
    /// Area.
    pub shape: GeoShape,
    /// The distance unit of the radius or box.
    pub unit: GeoUnit,
    /// `Some(GeoOrder::Asc)` sends `ASC`, `Some(GeoOrder::Desc)` sends `DESC`, `None` sends
    /// neither.
    pub order: Option<GeoOrder>,
    /// `COUNT n`.
    pub count: Option<u64>,
    /// Only with `count`: `ANY` stops at the first `count` matches found instead of the nearest
    /// `count`; the order is then not meaningful.
    pub any: bool,
    /// `WITHDIST`.
    pub with_distance: bool,
    /// `WITHHASH`.
    pub with_hash: bool,
    /// `WITHCOORD`.
    pub with_coordinates: bool,
}

impl<'a> GeoSearch<'a> {
    /// A radius search around a point, nearest first, without extra fields.
    pub fn radius(from: GeoFrom<'a>, radius: f64, unit: GeoUnit) -> Self {
        Self {
            from,
            shape: GeoShape::Radius(radius),
            unit,
            order: Some(GeoOrder::Asc),
            count: None,
            any: false,
            with_distance: false,
            with_hash: false,
            with_coordinates: false,
        }
    }
}

/// One `GEOSEARCH` match. Fields are `Some` when the matching `WITH*` flag was set.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoMatch {
    /// Member name.
    pub member: String,
    /// Distance in the request unit.
    pub distance: Option<f64>,
    /// 52-bit geohash score.
    pub hash: Option<i64>,
    /// Longitude and latitude.
    pub coordinates: Option<(f64, f64)>,
}

/// `LPOS` options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListPositionOptions {
    /// `RANK`: which match to start from; negative searches from the tail.
    pub rank: Option<i64>,
    /// `MAXLEN`: compare at most this many elements.
    pub max_len: Option<u64>,
}

/// The answer of [`Client::rate_limit`](crate::Client::rate_limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// True when this call fit in the window.
    pub allowed: bool,
    /// Calls left in the window after this one (0 when refused).
    pub remaining: i64,
}

/// The answer of the `*_with_retry` limit calls ([`Client::limit_sliding_with_retry`](crate::Client::limit_sliding_with_retry),
/// [`Client::limit_tokens_with_retry`](crate::Client::limit_tokens_with_retry)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitOutcome {
    /// True when this call was let through.
    pub allowed: bool,
    /// Allowance left after this call (0 when refused).
    pub remaining: i64,
    /// Milliseconds until the same call would pass (0 when it was let through). Fit for a
    /// `Retry-After` header.
    pub retry_after_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_tables_match_the_server_values() {
        let units = [
            (GeoUnit::Meters, "m"),
            (GeoUnit::Kilometers, "km"),
            (GeoUnit::Feet, "ft"),
            (GeoUnit::Miles, "mi"),
        ];

        for (unit, text) in units {
            assert_eq!(unit.as_str(), text);
            assert_eq!(unit.as_bytes(), text.as_bytes());
        }

        assert_eq!(GeoOrder::Asc.as_bytes(), b"ASC");
        assert_eq!(GeoOrder::Desc.as_bytes(), b"DESC");
        assert_eq!(InsertPosition::Before.as_bytes(), b"BEFORE");
        assert_eq!(InsertPosition::After.as_bytes(), b"AFTER");
        assert_eq!(ListSide::Left.as_bytes(), b"LEFT");
        assert_eq!(ListSide::Right.as_bytes(), b"RIGHT");
        assert_eq!(BitFieldOverflow::Wrap.as_str(), "WRAP");
        assert_eq!(BitFieldOverflow::Sat.as_str(), "SAT");
        assert_eq!(BitFieldOverflow::Fail.as_str(), "FAIL");
        assert_eq!(BitOperation::Not.as_str(), "NOT");
        assert_eq!(BitUnit::Bit.as_str(), "BIT");
        assert_eq!(Aggregate::Max.as_str(), "MAX");
    }

    #[test]
    fn key_types_cover_every_scan_type() {
        let all = [
            (KeyType::String, "string"),
            (KeyType::Json, "json"),
            (KeyType::List, "list"),
            (KeyType::Set, "set"),
            (KeyType::Hash, "hash"),
            (KeyType::SortedSet, "zset"),
            (KeyType::BloomFilter, "bloomfilter"),
            (KeyType::Stream, "stream"),
            (KeyType::Lease, "lease"),
            (KeyType::Limit, "limit"),
            (KeyType::Semaphore, "semaphore"),
            (KeyType::Once, "once"),
        ];

        for (kind, text) in all {
            assert_eq!(kind.as_str(), text);
            assert_eq!(kind.as_bytes(), text.as_bytes());
        }
    }

    #[test]
    fn bitfield_types_render_and_validate() {
        assert_eq!(BitFieldType::signed(1).unwrap().as_string(), "i1");
        assert_eq!(BitFieldType::signed(64).unwrap().as_string(), "i64");
        assert_eq!(BitFieldType::unsigned(8).unwrap().as_string(), "u8");
        assert_eq!(BitFieldType::unsigned(63).unwrap().as_string(), "u63");
        assert!(BitFieldType::signed(0).is_none());
        assert!(BitFieldType::signed(65).is_none());
        assert!(BitFieldType::unsigned(0).is_none());
        assert!(BitFieldType::unsigned(64).is_none());
    }
}
