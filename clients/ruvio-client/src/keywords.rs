//! Typed keywords for stream trimming, `INFO`, `CLIENT LIST TYPE` and `CHANGES SUBSCRIBE`,
//! plus the constant stream ids and range bounds the range commands accept.
//!
//! Every keyword maps to a `&'static str` constant, so building a command never formats or
//! allocates a keyword.

use std::ops::BitOr;

use crate::args::Num;

/// What `XADD` / `XTRIM` trim by.
///
/// # Examples
///
/// ```
/// use ruvio_client::TrimStrategy;
///
/// assert_eq!(TrimStrategy::MinId.as_str(), "MINID");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimStrategy {
    /// `MAXLEN`: keep the newest N entries.
    MaxLen,
    /// `MINID`: drop entries with an id below the threshold.
    MinId,
}

impl TrimStrategy {
    /// The wire keyword: `MAXLEN` or `MINID`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MaxLen => "MAXLEN",
            Self::MinId => "MINID",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The trim marker of `XADD` / `XTRIM`: `=` (exact) or `~` (approximate). Ruvio trims exactly
/// in both modes.
///
/// # Examples
///
/// ```
/// use ruvio_client::TrimMode;
///
/// assert_eq!(TrimMode::Approximate.as_str(), "~");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TrimMode {
    /// `=`.
    #[default]
    Exact,
    /// `~`.
    Approximate,
}

impl TrimMode {
    /// The wire token: `=` or `~`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "=",
            Self::Approximate => "~",
        }
    }

    /// The wire token as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// One trim rule for [`Client::xadd_trim`](crate::Client::xadd_trim) and
/// [`Client::xtrim`](crate::Client::xtrim): strategy (`MAXLEN` / `MINID`), mode (`=` / `~`) and
/// threshold. The server does not accept `LIMIT`.
///
/// # Examples
///
/// ```
/// use ruvio_client::{StreamTrim, TrimMode, TrimStrategy};
///
/// let by_length = StreamTrim::max_len(1000).mode(TrimMode::Approximate);
/// let by_id = StreamTrim::min_id("1700000000000-0");
///
/// assert_eq!(by_length.strategy(), TrimStrategy::MaxLen);
/// assert_eq!(by_id.strategy(), TrimStrategy::MinId);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTrim<'a> {
    strategy: TrimStrategy,
    mode: TrimMode,
    max_len: u64,
    min_id: &'a str,
}

impl<'a> StreamTrim<'a> {
    /// `MAXLEN = max_len`: keep exactly the newest `max_len` entries.
    pub const fn max_len(max_len: u64) -> Self {
        Self {
            strategy: TrimStrategy::MaxLen,
            mode: TrimMode::Exact,
            max_len,
            min_id: "",
        }
    }

    /// `MINID = id`: drop entries with an id below `id` (`ms-seq` or `ms`).
    pub const fn min_id(id: &'a str) -> Self {
        Self {
            strategy: TrimStrategy::MinId,
            mode: TrimMode::Exact,
            max_len: 0,
            min_id: id,
        }
    }

    /// The same rule with another [`TrimMode`].
    pub const fn mode(mut self, mode: TrimMode) -> Self {
        self.mode = mode;

        self
    }

    /// The strategy.
    pub const fn strategy(&self) -> TrimStrategy {
        self.strategy
    }

    /// The `=` / `~` mode.
    pub const fn trim_mode(&self) -> TrimMode {
        self.mode
    }

    /// Calls `f` with the three wire words: strategy, mode and threshold. The threshold number
    /// is formatted on the stack.
    pub(crate) fn with_words<R>(&self, f: impl FnOnce(&str, &str, &str) -> R) -> R {
        match self.strategy {
            TrimStrategy::MaxLen => f(
                self.strategy.as_str(),
                self.mode.as_str(),
                Num::uint(self.max_len).as_str(),
            ),
            TrimStrategy::MinId => f(self.strategy.as_str(), self.mode.as_str(), self.min_id),
        }
    }
}

/// The condition of `EXPIRE`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT`: the deadline changes only
/// when it holds. A key without a deadline counts as an infinite one, so `Gt` never applies to
/// it and `Lt` always does.
///
/// # Examples
///
/// ```
/// use ruvio_client::ExpireCondition;
///
/// assert_eq!(ExpireCondition::Gt.as_str(), "GT");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireCondition {
    /// `NX`: only when the key has no deadline.
    Nx,
    /// `XX`: only when the key already has a deadline.
    Xx,
    /// `GT`: only when the new deadline is later than the current one.
    Gt,
    /// `LT`: only when the new deadline is earlier than the current one.
    Lt,
}

impl ExpireCondition {
    /// The wire keyword: `NX`, `XX`, `GT` or `LT`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nx => "NX",
            Self::Xx => "XX",
            Self::Gt => "GT",
            Self::Lt => "LT",
        }
    }

    /// The wire keyword as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// A section for [`Client::info_section`](crate::Client::info_section). An unknown name makes
/// the server answer with an empty text, which is why this is an enum.
///
/// # Examples
///
/// ```
/// use ruvio_client::InfoSection;
///
/// assert_eq!(InfoSection::MemoryBreakdown.as_str(), "memorybreakdown");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfoSection {
    /// `server`.
    Server,
    /// `stats`.
    Stats,
    /// `buffers`.
    Buffers,
    /// `memory`.
    Memory,
    /// `memorybreakdown` (the `# Memory breakdown` block).
    MemoryBreakdown,
    /// `keyspace`.
    Keyspace,
    /// `snapshot`.
    Snapshot,
    /// `cluster`.
    Cluster,
    /// `replication`.
    Replication,
    /// `commandcounters` (the `# Command counters` block).
    CommandCounters,
    /// `latencystats`.
    LatencyStats,
    /// `logs`.
    Logs,
    /// `slabs`.
    Slabs,
    /// `all`: every block.
    All,
    /// `default`: every block.
    Default,
    /// `prometheus`: the metrics in Prometheus text format instead of `INFO` blocks.
    Prometheus,
}

impl InfoSection {
    /// The wire name (lowercase, no spaces).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Stats => "stats",
            Self::Buffers => "buffers",
            Self::Memory => "memory",
            Self::MemoryBreakdown => "memorybreakdown",
            Self::Keyspace => "keyspace",
            Self::Snapshot => "snapshot",
            Self::Cluster => "cluster",
            Self::Replication => "replication",
            Self::CommandCounters => "commandcounters",
            Self::LatencyStats => "latencystats",
            Self::Logs => "logs",
            Self::Slabs => "slabs",
            Self::All => "all",
            Self::Default => "default",
            Self::Prometheus => "prometheus",
        }
    }

    /// The wire name as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// The `TYPE` filter of `CLIENT LIST`. Ruvio checks the name but lists every connection.
///
/// # Examples
///
/// ```
/// use ruvio_client::ClientType;
///
/// assert_eq!(ClientType::PubSub.as_str(), "pubsub");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    /// `normal`.
    Normal,
    /// `master`.
    Master,
    /// `replica`.
    Replica,
    /// `pubsub`.
    PubSub,
}

impl ClientType {
    /// The wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Master => "master",
            Self::Replica => "replica",
            Self::PubSub => "pubsub",
        }
    }

    /// The wire name as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// One event category of `CHANGES SUBSCRIBE ... TYPES`.
///
/// # Examples
///
/// ```
/// use ruvio_client::ChangeType;
///
/// assert_eq!(ChangeType::Zset.as_str(), "zset");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    /// `put`: string writes.
    Put,
    /// `json`.
    Json,
    /// `del`.
    Del,
    /// `expire`.
    Expire,
    /// `rename`.
    Rename,
    /// `list`.
    List,
    /// `set`.
    Set,
    /// `hash`.
    Hash,
    /// `zset`.
    Zset,
    /// `stream`.
    Stream,
    /// `flush`.
    Flush,
    /// `other`.
    Other,
}

impl ChangeType {
    /// Every category, in wire order.
    pub const ALL: [ChangeType; 12] = [
        Self::Put,
        Self::Json,
        Self::Del,
        Self::Expire,
        Self::Rename,
        Self::List,
        Self::Set,
        Self::Hash,
        Self::Zset,
        Self::Stream,
        Self::Flush,
        Self::Other,
    ];

    /// The wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Json => "json",
            Self::Del => "del",
            Self::Expire => "expire",
            Self::Rename => "rename",
            Self::List => "list",
            Self::Set => "set",
            Self::Hash => "hash",
            Self::Zset => "zset",
            Self::Stream => "stream",
            Self::Flush => "flush",
            Self::Other => "other",
        }
    }

    /// The wire name as constant bytes.
    pub const fn as_bytes(self) -> &'static [u8] {
        self.as_str().as_bytes()
    }

    const fn bit(self) -> u16 {
        1 << self as u16
    }
}

/// A set of [`ChangeType`]s: the `TYPES` list of `CHANGES SUBSCRIBE`. The empty set sends no
/// `TYPES` filter, which accepts every category.
///
/// # Examples
///
/// ```
/// use ruvio_client::{ChangeType, ChangeTypes};
///
/// let types = ChangeTypes::of(ChangeType::Put) | ChangeType::Del;
///
/// assert!(types.contains(ChangeType::Del));
/// assert_eq!(types.to_wire(), "put,del");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeTypes(u16);

impl ChangeTypes {
    /// The empty set (no filter).
    pub const EMPTY: Self = Self(0);

    /// Every category.
    pub const ALL: Self = Self((1 << 12) - 1);

    /// A set holding one category.
    pub const fn of(kind: ChangeType) -> Self {
        Self(kind.bit())
    }

    /// This set plus `kind`.
    pub const fn with(self, kind: ChangeType) -> Self {
        Self(self.0 | kind.bit())
    }

    /// Whether `kind` is in the set.
    pub const fn contains(self, kind: ChangeType) -> bool {
        self.0 & kind.bit() != 0
    }

    /// Whether the set is empty.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The comma-separated `TYPES` argument in wire order (`put,del`); empty for the empty set.
    pub fn to_wire(self) -> String {
        let mut text = String::new();

        for kind in ChangeType::ALL {
            if self.contains(kind) {
                if !text.is_empty() {
                    text.push(',');
                }

                text.push_str(kind.as_str());
            }
        }

        text
    }
}

impl From<ChangeType> for ChangeTypes {
    fn from(kind: ChangeType) -> Self {
        Self::of(kind)
    }
}

impl BitOr<ChangeType> for ChangeTypes {
    type Output = Self;

    fn bitor(self, kind: ChangeType) -> Self {
        self.with(kind)
    }
}

impl BitOr for ChangeTypes {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl BitOr for ChangeType {
    type Output = ChangeTypes;

    fn bitor(self, other: Self) -> ChangeTypes {
        ChangeTypes::of(self).with(other)
    }
}

/// Constant stream ids for `XADD`, `XREADGROUP`, `XGROUP CREATE`, `XRANGE` and friends.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// client.xadd("events", stream_id::NEW, &[("type", "login")])?;
/// let all = client.xrange("events", stream_id::MIN, stream_id::MAX, None)?;
/// # Ok(())
/// # }
/// ```
pub mod stream_id {
    /// `*`: let the server pick the id of a new entry.
    pub const NEW: &str = "*";
    /// `$`: only entries added after now (`XREAD`, `XGROUP CREATE`).
    pub const LAST_DELIVERED: &str = "$";
    /// `>`: entries never delivered to this group (`XREADGROUP`).
    pub const NEW_MESSAGES: &str = ">";
    /// `-`: the smallest possible id (`XRANGE` start).
    pub const MIN: &str = "-";
    /// `+`: the largest possible id (`XRANGE` end).
    pub const MAX: &str = "+";
}

/// Range bounds for the sorted-set range commands (`ZRANGEBYSCORE`, `ZCOUNT`,
/// `ZREMRANGEBYSCORE`, `ZRANGEBYLEX`, `ZLEXCOUNT`, ...). Each returns the exact text those
/// methods accept.
///
/// # Examples
///
/// ```
/// use ruvio_client::bound;
///
/// assert_eq!(bound::NEG_INF, "-inf");
/// assert_eq!(bound::score_exclusive(1.5), "(1.5");
/// assert_eq!(bound::lex_inclusive("a"), "[a");
/// ```
pub mod bound {
    use crate::args::Num;

    /// `-inf`: below every score.
    pub const NEG_INF: &str = "-inf";
    /// `+inf`: above every score.
    pub const POS_INF: &str = "+inf";
    /// `-`: below every member (lex ranges).
    pub const LEX_MIN: &str = "-";
    /// `+`: above every member (lex ranges).
    pub const LEX_MAX: &str = "+";

    /// `(score`: the score itself is excluded.
    pub fn score_exclusive(score: f64) -> String {
        format!("({}", Num::float(score).as_str())
    }

    /// `[member`: the member is included (lex ranges).
    pub fn lex_inclusive(member: &str) -> String {
        format!("[{member}")
    }

    /// `(member`: the member is excluded (lex ranges).
    pub fn lex_exclusive(member: &str) -> String {
        format!("({member}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_words_match_the_string_form() {
        let words = |trim: StreamTrim| trim.with_words(|a, b, c| format!("{a} {b} {c}"));

        assert_eq!(words(StreamTrim::max_len(10)), "MAXLEN = 10");
        assert_eq!(
            words(StreamTrim::max_len(u64::MAX).mode(TrimMode::Approximate)),
            "MAXLEN ~ 18446744073709551615"
        );
        assert_eq!(words(StreamTrim::min_id("5-0")), "MINID = 5-0");
        assert_eq!(
            words(StreamTrim::min_id("5").mode(TrimMode::Approximate)),
            "MINID ~ 5"
        );
    }

    #[test]
    fn keyword_tables() {
        let info = [
            (InfoSection::Server, "server"),
            (InfoSection::Stats, "stats"),
            (InfoSection::Buffers, "buffers"),
            (InfoSection::Memory, "memory"),
            (InfoSection::MemoryBreakdown, "memorybreakdown"),
            (InfoSection::Keyspace, "keyspace"),
            (InfoSection::Snapshot, "snapshot"),
            (InfoSection::Cluster, "cluster"),
            (InfoSection::Replication, "replication"),
            (InfoSection::CommandCounters, "commandcounters"),
            (InfoSection::LatencyStats, "latencystats"),
            (InfoSection::Logs, "logs"),
            (InfoSection::Slabs, "slabs"),
            (InfoSection::All, "all"),
            (InfoSection::Default, "default"),
            (InfoSection::Prometheus, "prometheus"),
        ];

        for (section, name) in info {
            assert_eq!(section.as_str(), name);
            assert_eq!(section.as_bytes(), name.as_bytes());
        }

        assert_eq!(ClientType::Normal.as_str(), "normal");
        assert_eq!(ClientType::Master.as_str(), "master");
        assert_eq!(ClientType::Replica.as_bytes(), b"replica");
        assert_eq!(ClientType::PubSub.as_str(), "pubsub");
        assert_eq!(TrimStrategy::MaxLen.as_bytes(), b"MAXLEN");
        assert_eq!(TrimMode::Exact.as_bytes(), b"=");
    }

    #[test]
    fn change_types_form_a_set() {
        assert!(ChangeTypes::EMPTY.is_empty());
        assert_eq!(ChangeTypes::EMPTY.to_wire(), "");
        assert_eq!(
            ChangeTypes::ALL.to_wire(),
            "put,json,del,expire,rename,list,set,hash,zset,stream,flush,other"
        );
        assert_eq!((ChangeType::Other | ChangeType::Put).to_wire(), "put,other");

        let types = ChangeTypes::from(ChangeType::Hash) | ChangeTypes::of(ChangeType::Json);

        assert!(types.contains(ChangeType::Hash) && !types.contains(ChangeType::Put));
        assert_eq!(types.to_wire(), "json,hash");

        for (index, kind) in ChangeType::ALL.iter().enumerate() {
            assert_eq!(ChangeTypes::of(*kind), ChangeTypes(1 << index));
        }
    }

    #[test]
    fn bounds_and_ids() {
        assert_eq!(bound::score_exclusive(2.0), "(2.0");
        assert_eq!(bound::score_exclusive(-0.5), "(-0.5");
        assert_eq!(bound::lex_inclusive("m"), "[m");
        assert_eq!(bound::lex_exclusive("m"), "(m");
        assert_eq!(
            [
                stream_id::NEW,
                stream_id::LAST_DELIVERED,
                stream_id::NEW_MESSAGES,
                stream_id::MIN,
                stream_id::MAX
            ],
            ["*", "$", ">", "-", "+"]
        );
    }
}
