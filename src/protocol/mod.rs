use std::{borrow::Cow, fmt, io, io::BufRead};

mod command;
mod direct;
mod hot;
mod inline;
mod parse;
mod resp;
mod table;
pub(crate) use table::flags;
pub(crate) use table::{
    CommandSpec, acl_canonical, acl_commands, command_index, commands as all_commands,
    lookup as command_spec,
};
#[cfg(test)]
mod client_spec;
#[cfg(test)]
pub(crate) mod consistency;
#[cfg(test)]
mod tests;
mod validate;

pub(crate) use command::*;
pub use hot::*;
pub(crate) use parse::*;
pub use resp::*;
use validate::*;

/// The byte limits a parsed command must meet, for callers that build commands without a
/// frame (`ruvio.call` in scripts).
pub(crate) fn validate_payloads(
    command: &Command<'_>,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    validate_command_payloads(command, limits)
}

pub const DEFAULT_MAX_ARGUMENTS: usize = 1_048_576;
pub use inline::MAX_INLINE_BYTES;
pub const DEFAULT_MAX_KEY_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_COMMAND_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_PIPELINE_COMMANDS: usize = 1024;
pub const DEFAULT_MAX_CONNECTION_BUFFER_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_IDEMPOTENCY_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_IDEMPOTENCY_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_arguments: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_command_bytes: usize,
    pub max_pipeline_commands: usize,
    pub max_connection_buffer_bytes: usize,
    /// Selectable logical databases, `0..databases`.
    pub databases: usize,
}

/// Largest frame, value and connection buffer the limits may allow. Argument offsets inside
/// a frame are kept as `u32` and WAL frames carry a `u32` length, so 2 GiB leaves headroom.
pub const MAX_FRAME_LIMIT_BYTES: usize = 1 << 31;

impl ProtocolLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.max_command_bytes > MAX_FRAME_LIMIT_BYTES
            || self.max_value_bytes > MAX_FRAME_LIMIT_BYTES
            || self.max_key_bytes > MAX_FRAME_LIMIT_BYTES
            || self.max_connection_buffer_bytes > MAX_FRAME_LIMIT_BYTES
        {
            return Err(
                "max_command_bytes, max_key_bytes, max_value_bytes and max_connection_buffer_bytes must be at most 2147483648",
            );
        }

        if self.max_arguments == 0 {
            return Err("max_arguments must be greater than zero");
        }

        if self.max_key_bytes == 0 {
            return Err("max_key_bytes must be greater than zero");
        }

        if self.max_value_bytes == 0 {
            return Err("max_value_bytes must be greater than zero");
        }

        if self.max_command_bytes == 0 {
            return Err("max_command_bytes must be greater than zero");
        }

        if self.max_pipeline_commands == 0 {
            return Err("max_pipeline_commands must be greater than zero");
        }

        if self.max_connection_buffer_bytes < self.max_command_bytes {
            return Err("max_connection_buffer_bytes must be at least max_command_bytes");
        }

        if !(1..=crate::database::MAX_DATABASES).contains(&self.databases) {
            return Err("databases must be between 1 and 256");
        }

        Ok(self)
    }

    fn max_bulk_bytes(self) -> usize {
        self.max_key_bytes.max(self.max_value_bytes)
    }
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_arguments: DEFAULT_MAX_ARGUMENTS,
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_command_bytes: DEFAULT_MAX_COMMAND_BYTES,
            max_pipeline_commands: DEFAULT_MAX_PIPELINE_COMMANDS,
            max_connection_buffer_bytes: DEFAULT_MAX_CONNECTION_BUFFER_BYTES,
            databases: crate::database::DEFAULT_DATABASES,
        }
    }
}

/// Single-key JSON commands that read a value or compute a replacement from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonOpKind {
    Type,
    StrLen,
    ArrLen,
    ObjKeys,
    NumIncrBy,
    Toggle,
    ArrAppend,
    Clear,
    ObjLen,
    StrAppend,
    NumMultBy,
    ArrInsert,
    ArrPop,
    ArrTrim,
    ArrIndex,
}

impl JsonOpKind {
    pub(crate) fn writes(self) -> bool {
        matches!(
            self,
            Self::NumIncrBy
                | Self::Toggle
                | Self::ArrAppend
                | Self::Clear
                | Self::StrAppend
                | Self::NumMultBy
                | Self::ArrInsert
                | Self::ArrPop
                | Self::ArrTrim
        )
    }

    pub(crate) fn acl_name(self) -> &'static str {
        match self {
            Self::Type => "json.type",
            Self::StrLen => "json.strlen",
            Self::ArrLen => "json.arrlen",
            Self::ObjKeys => "json.objkeys",
            Self::NumIncrBy => "json.numincrby",
            Self::Toggle => "json.toggle",
            Self::ArrAppend => "json.arrappend",
            Self::Clear => "json.clear",
            Self::ObjLen => "json.objlen",
            Self::StrAppend => "json.strappend",
            Self::NumMultBy => "json.nummultby",
            Self::ArrInsert => "json.arrinsert",
            Self::ArrPop => "json.arrpop",
            Self::ArrTrim => "json.arrtrim",
            Self::ArrIndex => "json.arrindex",
        }
    }
}

/// `JSON.INDEX` and `JSON.FIND`: field indexes over JSON documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonIndexKind {
    Create,
    Drop,
    List,
    Find,
}

impl JsonIndexKind {
    pub(crate) fn writes(self) -> bool {
        matches!(self, Self::Create | Self::Drop)
    }

    pub(crate) fn acl_name(self) -> &'static str {
        match self {
            Self::Create | Self::Drop | Self::List => "json.index",
            Self::Find => "json.find",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HotCommand<'a> {
    Ping(Option<&'a [u8]>),
    Get(&'a [u8]),
    /// Plain `SET`, or `SET` with only a `PX` millisecond argument.
    Set(&'a [u8], &'a [u8], Option<i64>),
    /// `SETEX key seconds value` with a valid, positive expiry, in milliseconds. Its own
    /// variant so ACL rules and command statistics still see `setex`.
    SetEx(&'a [u8], &'a [u8], i64),
    /// Single-key `DEL` or `UNLINK`.
    Delete(&'a [u8]),
    Increment(&'a [u8], i64),
}

#[derive(Debug, PartialEq, Eq)]
pub enum HotParse<'a> {
    Complete(HotCommand<'a>, usize),
    Incomplete,
    Fallback,
}

/// How `ZUNION`/`ZINTER` combine the scores of a shared member.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SortedAggregate {
    #[default]
    Sum,
    Min,
    Max,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitOperation {
    And,
    Or,
    Xor,
    Not,
}

/// What `BITFIELD` does when a value does not fit its type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitFieldOverflow {
    Wrap,
    Sat,
    Fail,
}

/// `i1`..`i64` or `u1`..`u63`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitFieldType {
    pub signed: bool,
    pub bits: u8,
}

/// One `BITFIELD` subcommand. `OVERFLOW` is folded into the writes that follow it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitFieldOp {
    Get {
        ty: BitFieldType,
        offset: u64,
    },
    Set {
        ty: BitFieldType,
        offset: u64,
        value: i64,
        overflow: BitFieldOverflow,
    },
    IncrBy {
        ty: BitFieldType,
        offset: u64,
        delta: i64,
        overflow: BitFieldOverflow,
    },
}

impl BitFieldOp {
    pub fn writes(&self) -> bool {
        !matches!(self, Self::Get { .. })
    }
}

/// The subcommands of one `BITFIELD`: almost always a single one, which then needs no
/// allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BitFieldOps {
    One(BitFieldOp),
    Many(Vec<BitFieldOp>),
}

impl BitFieldOps {
    pub fn new() -> Self {
        Self::Many(Vec::new())
    }

    pub fn push(&mut self, op: BitFieldOp) {
        match self {
            Self::Many(ops) if ops.is_empty() => *self = Self::One(op),
            Self::Many(ops) => ops.push(op),
            Self::One(first) => *self = Self::Many(vec![*first, op]),
        }
    }
}

impl Default for BitFieldOps {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for BitFieldOps {
    type Target = [BitFieldOp];

    fn deref(&self) -> &[BitFieldOp] {
        match self {
            Self::One(op) => std::slice::from_ref(op),
            Self::Many(ops) => ops,
        }
    }
}

impl PartialEq<Vec<BitFieldOp>> for BitFieldOps {
    fn eq(&self, other: &Vec<BitFieldOp>) -> bool {
        **self == **other
    }
}

/// The keys of `DEL`, `EXISTS` and `TOUCH`: usually one, which then needs no allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Keys<'a> {
    One(Bytes<'a>),
    Many(Vec<Bytes<'a>>),
}

impl<'a> Keys<'a> {
    pub fn into_vec(self) -> Vec<Bytes<'a>> {
        match self {
            Self::One(key) => vec![key],
            Self::Many(keys) => keys,
        }
    }
}

impl<'a> From<Vec<Bytes<'a>>> for Keys<'a> {
    fn from(mut keys: Vec<Bytes<'a>>) -> Self {
        if keys.len() == 1 {
            Self::One(keys.pop().expect("one key"))
        } else {
            Self::Many(keys)
        }
    }
}

impl<'a> std::ops::Deref for Keys<'a> {
    type Target = [Bytes<'a>];

    fn deref(&self) -> &[Bytes<'a>] {
        match self {
            Self::One(key) => std::slice::from_ref(key),
            Self::Many(keys) => keys,
        }
    }
}

impl<'a> IntoIterator for Keys<'a> {
    type Item = Bytes<'a>;
    type IntoIter = std::vec::IntoIter<Bytes<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_vec().into_iter()
    }
}

pub type Bytes<'a> = Cow<'a, [u8]>;

/// Optional `XCLAIM` fields. `idle` and `time` are mutually exclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimOptions {
    pub idle: Option<u64>,
    pub time: Option<u64>,
    pub retry_count: Option<u64>,
    pub force: bool,
    pub just_id: bool,
    pub last_id: Option<(u64, u64)>,
}

impl ClaimOptions {
    pub(crate) const fn plain() -> Self {
        Self {
            idle: None,
            time: None,
            retry_count: None,
            force: false,
            just_id: false,
            last_id: None,
        }
    }
}

/// `XADD` id. `Auto` is `*`. `AutoSequence` is `<millis>-*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamAddId {
    Auto,
    AutoSequence(u64),
    Explicit(u64, u64),
}

/// Which `COMMAND` reply a [`Command::CommandMeta`] asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandMetaKind {
    /// `COMMAND`: every command, in `COMMAND INFO` form.
    List,
    /// `COMMAND COUNT`
    Count,
    /// `COMMAND INFO [name ...]`
    Info,
    /// `COMMAND DOCS [name ...]`
    Docs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowlogOp {
    /// Newest first; `usize::MAX` means every entry.
    Get(usize),
    Len,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Encoding,
    IdleTime,
    Freq,
}

/// A slot range and the shard of this process it should move to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRangeMove {
    pub first: u16,
    pub last: u16,
    pub shard: u16,
}

/// `RUVIO LEASE GRANT token coordinator voters epoch lease_ms`: coordinator `coordinator` of
/// `voters` grants this primary its lease from the `RUVIO LEASE BEGIN` that answered `token`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseGrant {
    pub token: u64,
    pub coordinator: u64,
    pub voters: u32,
    pub epoch: u64,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command<'a> {
    Ping(Option<Bytes<'a>>),
    /// `COMMAND` and its subcommands, answered from the command table.
    CommandMeta {
        kind: CommandMetaKind,
        names: Vec<Bytes<'a>>,
    },
    /// Block until `replicas` copies have synced `wal_last_sequence`, or the timeout.
    /// `timeout_ms` `0` waits until shutdown.
    Wait {
        replicas: u64,
        timeout_ms: u64,
    },
    /// No argument, or the single word <c>owner</c>.
    Ruvio(Option<Bytes<'a>>),
    /// `None` keeps the current version. `Some(2)` and `Some(3)` switch.
    /// Any other value is `NOPROTO` and does not switch.
    /// `auth` is `HELLO AUTH username password`. A failed auth changes nothing.
    Hello {
        protocol: Option<i64>,
        auth: Option<(Bytes<'a>, Bytes<'a>)>,
        client_name: Option<Bytes<'a>>,
    },
    ClientGetName,
    ClientId,
    /// `CLIENT SETINFO LIB-NAME|LIB-VER value`: accepted and answered `OK`; nothing is kept.
    ClientSetInfo,
    /// `CLIENT LIST [ID id ...]`; empty `ids` lists every connection of the shard.
    ClientList {
        ids: Vec<u64>,
    },
    ClientInfo,
    /// `CLIENT KILL`: by id, by address, or the legacy `CLIENT KILL addr` (replies `OK`).
    ClientKill {
        id: Option<u64>,
        addr: Option<Bytes<'a>>,
        skip_me: bool,
        legacy: bool,
    },
    Slowlog(SlowlogOp),
    LatencyLatest,
    Echo(Bytes<'a>),
    Object {
        kind: ObjectKind,
        key: Bytes<'a>,
    },
    MemoryUsage(Bytes<'a>),
    ClientSetName(Bytes<'a>),
    /// `true` is `ON`. `false` is `OFF`.
    ClientTracking(bool),
    /// `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`: writes on this connection are answered
    /// once `replicas` replicas hold them on disk. `0` turns it off.
    ClientDurability {
        replicas: u64,
        timeout_ms: u64,
    },
    ShardPublish {
        channel: Bytes<'a>,
        message: Bytes<'a>,
    },
    ShardSubscribe(Vec<Bytes<'a>>),
    ShardUnsubscribe(Vec<Bytes<'a>>),
    Publish {
        channel: Bytes<'a>,
        message: Bytes<'a>,
    },
    Subscribe(Vec<Bytes<'a>>),
    Unsubscribe(Vec<Bytes<'a>>),
    PSubscribe(Vec<Bytes<'a>>),
    PUnsubscribe(Vec<Bytes<'a>>),
    ClusterSlots,
    ClusterNodes,
    ClusterInfo,
    ClusterShards,
    ClusterMyId,
    /// `CLUSTER KEYSLOT key`. The argument is hashed, not read, so it does not route.
    ClusterKeySlot(Bytes<'a>),
    ReadOnly,
    ReadWrite,
    ConfigGet(Bytes<'a>),
    /// `CONFIG SET name value [name value ...]`: names and values as sent, checked by the
    /// connection that receives it.
    ConfigSet(Vec<(Bytes<'a>, Bytes<'a>)>),
    ConfigRewrite,
    /// `REPLICAOF host port [YIELD]`, or `REPLICAOF NO ONE [FORCE]` (`primary` is `None`);
    /// `force` is `FORCE` or `YIELD`.
    ReplicaOf {
        primary: Option<(Bytes<'a>, Bytes<'a>)>,
        force: bool,
    },
    Role,
    /// `RUVIO MOVESLOTS first-last shard` moves a slot range to another shard of this
    /// process; `RUVIO MOVESLOTS STATUS` (`None`) reports the current or last move.
    MoveSlots(Option<SlotRangeMove>),
    /// `RUVIO TOPOLOGY SET map` installs a multi-node cluster map; `RUVIO TOPOLOGY VERSION`
    /// (`None`) reports the version this node serves (0 without a map).
    Topology(Option<Bytes<'a>>),
    /// `RUVIO LEASE BEGIN` (`None`) and `RUVIO LEASE GRANT ...`: the primary lease
    /// `ruvio coordinator` renews (see `primary-lease`). Rare, so the grant is boxed.
    PrimaryLease(Option<Box<LeaseGrant>>),
    /// `RUVIO MIGRATE ...`: the steps of moving a slot range to another node, sent by the
    /// coordinators and by the source node; the runtime reads the arguments.
    Migrate(Vec<Bytes<'a>>),
    Info(Option<Bytes<'a>>),
    Save,
    Bgsave,
    Get(Bytes<'a>),
    JsonGet {
        key: Bytes<'a>,
        path: Bytes<'a>,
    },
    JsonMGet {
        keys: Vec<Bytes<'a>>,
        path: Bytes<'a>,
    },
    JsonSet {
        key: Bytes<'a>,
        path: Bytes<'a>,
        value: Bytes<'a>,
    },
    JsonMerge {
        key: Bytes<'a>,
        path: Bytes<'a>,
        value: Bytes<'a>,
    },
    JsonDel {
        key: Bytes<'a>,
        path: Bytes<'a>,
    },
    /// `JSON.TYPE/STRLEN/ARRLEN/OBJKEYS/NUMINCRBY/TOGGLE/ARRAPPEND/CLEAR` on one key.
    JsonIndex {
        op: JsonIndexKind,
        args: Vec<Bytes<'a>>,
    },
    /// `JSON.SET key path value NX|XX`: `must_exist` is `XX`.
    JsonSetIf {
        key: Bytes<'a>,
        path: Bytes<'a>,
        value: Bytes<'a>,
        must_exist: bool,
    },
    JsonOp {
        op: JsonOpKind,
        key: Bytes<'a>,
        path: Bytes<'a>,
        args: Vec<Bytes<'a>>,
    },
    Set(Bytes<'a>, Bytes<'a>, SetOptions),
    Lease {
        key: Bytes<'a>,
        ttl_ms: u64,
    },
    Release {
        key: Bytes<'a>,
        token: u64,
    },
    /// `RENEW key token ttl_ms`: moves a live lease or semaphore holder's deadline to now + TTL.
    Renew {
        key: Bytes<'a>,
        token: u64,
        ttl_ms: u64,
    },
    /// `SEMAPHORE key max ttl_ms`: one of at most `max` holders, each with its own TTL.
    Semaphore {
        key: Bytes<'a>,
        max: u64,
        ttl_ms: u64,
    },
    SetVersioned {
        key: Bytes<'a>,
        value: Bytes<'a>,
        expected: Option<u64>,
    },
    /// `None` arms the feed and returns the cursor. `Some` reads notes after that cursor.
    Changes(Option<u64>),
    /// `CHANGES SUBSCRIBE`: a durable stream read from the WAL (evented runtime only).
    ChangesSubscribe(crate::changes::ChangeSubscription),
    Limit {
        key: Bytes<'a>,
        max: u64,
        window_ms: u64,
        mode: LimitMode,
    },
    Once {
        key: Bytes<'a>,
        ttl_ms: u64,
    },
    IdempotencyBegin {
        key: Bytes<'a>,
        fingerprint: Bytes<'a>,
        owner: Bytes<'a>,
        ttl_ms: u64,
    },
    IdempotencyComplete {
        key: Bytes<'a>,
        fingerprint: Bytes<'a>,
        owner: Bytes<'a>,
        result: Bytes<'a>,
    },
    /// `IDEM key ABORT fingerprint owner`: removes a pending record of this owner.
    IdempotencyAbort {
        key: Bytes<'a>,
        fingerprint: Bytes<'a>,
        owner: Bytes<'a>,
    },
    Take {
        key: Bytes<'a>,
        count: i64,
    },
    /// `TRANSFER source destination amount [MAX cap]`: moves `amount` between two integer
    /// strings in one step. Nil when `source` is missing or short, or `destination` would
    /// pass `max`.
    Transfer {
        source: Bytes<'a>,
        destination: Bytes<'a>,
        amount: i64,
        max: Option<i64>,
    },
    /// `HTRANSFER key from to amount [MAX cap]`: `TRANSFER` between two fields of one hash.
    HashTransfer {
        key: Bytes<'a>,
        from: Bytes<'a>,
        to: Bytes<'a>,
        amount: i64,
        max: Option<i64>,
    },
    /// `SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]`: writes only when the
    /// string at `key` equals `expected`. Nil otherwise.
    SetIfEq {
        key: Bytes<'a>,
        value: Bytes<'a>,
        expected: Bytes<'a>,
        options: SetOptions,
    },
    /// `SET key value IFGT|IFLT [EX|PX|EXAT|PXAT|KEEPTTL]`: writes when the key is missing or
    /// `number` (the parsed `value`) is greater (`greater`) or smaller than the stored integer.
    SetIfCmp {
        key: Bytes<'a>,
        value: Bytes<'a>,
        number: i64,
        greater: bool,
        options: SetOptions,
    },
    /// `DELEX key IFEQ expected`: deletes the string at `key` only when it equals `expected`.
    DelIfEq {
        key: Bytes<'a>,
        expected: Bytes<'a>,
    },
    GetEx {
        key: Bytes<'a>,
        ttl_ms: u64,
    },
    Increment(Bytes<'a>, i64),
    /// `INCRBY key delta MAX limit`: nil, with no write, when the result would pass `limit`.
    IncrementCapped {
        key: Bytes<'a>,
        delta: i64,
        limit: i64,
    },
    MGet(Vec<Bytes<'a>>),
    MSet(Vec<(Bytes<'a>, Bytes<'a>)>),
    GetSet(Bytes<'a>, Bytes<'a>),
    Append(Bytes<'a>, Bytes<'a>),
    /// `SETNX`: `1` when it wrote, `0` when the key existed.
    SetNx(Bytes<'a>, Bytes<'a>),
    /// `GETDEL`: the value, removed in the same step.
    GetDel(Bytes<'a>),
    /// `MSETNX`: writes every pair only when none of the keys exist.
    MSetNx(Vec<(Bytes<'a>, Bytes<'a>)>),
    IncrementFloat(Bytes<'a>, f64),
    /// `GETRANGE`/`SUBSTR`: both ends inclusive, negative counts from the end.
    GetRange {
        key: Bytes<'a>,
        start: i64,
        end: i64,
    },
    SetRange {
        key: Bytes<'a>,
        offset: u64,
        value: Bytes<'a>,
    },
    SetBit {
        key: Bytes<'a>,
        offset: u64,
        value: bool,
    },
    GetBit(Bytes<'a>, u64),
    /// `range` is `(start, end, bit_units)`; both ends may be negative (from the end).
    BitCount {
        key: Bytes<'a>,
        range: Option<(i64, i64, bool)>,
    },
    /// `range` is `(start, end, bit_units)`; `end` is `None` when omitted.
    BitPos {
        key: Bytes<'a>,
        bit: bool,
        range: Option<(i64, Option<i64>, bool)>,
    },
    /// `FILLGET key fill_ms [WAIT wait_ms]`: single-flight cache fill.
    FillGet {
        key: Bytes<'a>,
        fill_ms: u64,
        wait_ms: Option<u64>,
    },
    FillSet {
        key: Bytes<'a>,
        value: Bytes<'a>,
        ttl_ms: u64,
        token: u64,
        swr_ms: Option<u64>,
    },
    PfAdd {
        key: Bytes<'a>,
        elements: Vec<Bytes<'a>>,
    },
    PfCount(Keys<'a>),
    PfMerge {
        destination: Bytes<'a>,
        sources: Vec<Bytes<'a>>,
    },
    /// `BITFIELD key op...`, or `BITFIELD_RO` when `read_only_name` is set.
    BitField {
        key: Bytes<'a>,
        ops: BitFieldOps,
        read_only_name: bool,
    },
    BitOp {
        operation: BitOperation,
        destination: Bytes<'a>,
        keys: Vec<Bytes<'a>>,
    },
    StrLen(Bytes<'a>),
    Exists(Keys<'a>),
    Type(Bytes<'a>),
    Rename {
        source: Bytes<'a>,
        destination: Bytes<'a>,
    },
    /// `KEYS pattern`; refused on databases past `KEYS_LIMIT`.
    Keys(Bytes<'a>),
    RandomKey,
    Touch(Keys<'a>),
    Dump(Bytes<'a>),
    Restore {
        key: Bytes<'a>,
        ttl: i64,
        payload: Bytes<'a>,
        replace: bool,
        absolute: bool,
    },
    RenameNx {
        source: Bytes<'a>,
        destination: Bytes<'a>,
    },
    /// `EXPIRETIME` (seconds) and `PEXPIRETIME` (milliseconds).
    ExpireTime {
        key: Bytes<'a>,
        millis: bool,
    },
    Copy {
        source: Bytes<'a>,
        destination: Bytes<'a>,
        replace: bool,
    },
    Scan {
        cursor: u64,
        pattern: Option<Bytes<'a>>,
        count: usize,
        type_name: Option<Bytes<'a>>,
        /// `SLOT n`: only keys of that hash slot; the command goes to the slot's owner.
        slot: Option<u16>,
    },
    DbSize,
    Del(Keys<'a>),
    ListPush(Bytes<'a>, Vec<Bytes<'a>>),
    ListPushBack(Bytes<'a>, Vec<Bytes<'a>>),
    ListPop {
        key: Bytes<'a>,
        count: Option<usize>,
    },
    ListPopBack {
        key: Bytes<'a>,
        count: Option<usize>,
    },
    ListLen(Bytes<'a>),
    ListIndex(Bytes<'a>, isize),
    ListTrim(Bytes<'a>, isize, isize),
    ListBlock {
        keys: Vec<Bytes<'a>>,
        timeout_ms: Option<u64>,
        back: bool,
    },
    ListRange(Bytes<'a>, isize, isize),
    ListInsert {
        key: Bytes<'a>,
        before: bool,
        pivot: Bytes<'a>,
        value: Bytes<'a>,
    },
    ListSet {
        key: Bytes<'a>,
        index: i64,
        value: Bytes<'a>,
    },
    /// `LREM`: `count` > 0 from the head, < 0 from the tail, 0 removes every match.
    ListRemove {
        key: Bytes<'a>,
        count: i64,
        value: Bytes<'a>,
    },
    /// `LPOS`: `count` `None` returns one index; `Some(0)` returns every match.
    ListPos {
        key: Bytes<'a>,
        element: Bytes<'a>,
        rank: i64,
        count: Option<u64>,
        max_len: u64,
    },
    /// `LPUSHCAP`/`RPUSHCAP key cap value...`: pushes, then drops items from the opposite
    /// end until at most `cap` remain.
    ListPushCapped {
        key: Bytes<'a>,
        cap: usize,
        values: Vec<Bytes<'a>>,
        back: bool,
    },
    /// `LPUSHX`/`RPUSHX`: pushes only when the list exists.
    ListPushX {
        key: Bytes<'a>,
        values: Vec<Bytes<'a>>,
        back: bool,
    },
    /// `LMOVE`, `RPOPLPUSH`, `BLMOVE`. `block` is `Some(timeout)` for `BLMOVE`, `None` inside it
    /// meaning no timeout.
    ListMove {
        source: Bytes<'a>,
        destination: Bytes<'a>,
        from_left: bool,
        to_left: bool,
        block: Option<Option<u64>>,
    },
    /// `LMPOP` and `BLMPOP`.
    ListMPop {
        keys: Vec<Bytes<'a>>,
        from_left: bool,
        count: u64,
        block: Option<Option<u64>>,
    },
    SetAdd(Bytes<'a>, Vec<Bytes<'a>>),
    SetRemove(Bytes<'a>, Vec<Bytes<'a>>),
    SetContains(Bytes<'a>, Bytes<'a>),
    SetCard(Bytes<'a>),
    SetMembers(Bytes<'a>),
    SetPop {
        key: Bytes<'a>,
        count: Option<usize>,
    },
    SetRandom {
        key: Bytes<'a>,
        count: Option<i64>,
    },
    /// `SMISMEMBER`.
    SetMIsMember {
        key: Bytes<'a>,
        members: Vec<Bytes<'a>>,
    },
    /// `SINTERCARD`: `limit` `0` means no limit.
    SetInterCard {
        keys: Vec<Bytes<'a>>,
        limit: u64,
    },
    SetAlgebra {
        keys: Vec<Bytes<'a>>,
        op: SetOp,
        destination: Option<Bytes<'a>>,
    },
    SetMove {
        source: Bytes<'a>,
        destination: Bytes<'a>,
        member: Bytes<'a>,
    },
    HashSet {
        key: Bytes<'a>,
        pairs: Vec<(Bytes<'a>, Bytes<'a>)>,
    },
    /// `HMSET`: `HSET` that replies `OK`.
    HashMSet {
        key: Bytes<'a>,
        pairs: Vec<(Bytes<'a>, Bytes<'a>)>,
    },
    HashDelete {
        key: Bytes<'a>,
        fields: Vec<Bytes<'a>>,
    },
    HashGet {
        key: Bytes<'a>,
        field: Bytes<'a>,
    },
    HashMGet {
        key: Bytes<'a>,
        fields: Vec<Bytes<'a>>,
    },
    HashLen(Bytes<'a>),
    HashGetAll(Bytes<'a>),
    HashExists {
        key: Bytes<'a>,
        field: Bytes<'a>,
    },
    HashKeys(Bytes<'a>),
    HashValues(Bytes<'a>),
    HashIncrement {
        key: Bytes<'a>,
        field: Bytes<'a>,
        delta: i64,
    },
    HashSetNx {
        key: Bytes<'a>,
        field: Bytes<'a>,
        value: Bytes<'a>,
    },
    HashStrLen {
        key: Bytes<'a>,
        field: Bytes<'a>,
    },
    HashScan {
        key: Bytes<'a>,
        cursor: u64,
        pattern: Option<Bytes<'a>>,
        count: usize,
    },
    /// `SSCAN` or `ZSCAN`.
    CollectionScan {
        kind: CollectionScanKind,
        key: Bytes<'a>,
        cursor: u64,
        pattern: Option<Bytes<'a>>,
        count: usize,
    },
    /// `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, or `HPEXPIREAT`.
    HashExpire {
        key: Bytes<'a>,
        time: FieldExpireTime,
        condition: FieldExpireCondition,
        fields: Vec<Bytes<'a>>,
        /// Lowercase command name for the invalid-time error.
        name: &'static str,
    },
    /// `HTTL`, `HPTTL`, `HEXPIRETIME`, or `HPEXPIRETIME`.
    HashTtl {
        key: Bytes<'a>,
        reply: FieldTtlReply,
        fields: Vec<Bytes<'a>>,
    },
    HashPersist {
        key: Bytes<'a>,
        fields: Vec<Bytes<'a>>,
    },
    SortedSetAdd {
        key: Bytes<'a>,
        pairs: Vec<(f64, Bytes<'a>)>,
        options: SortedSetAddOptions,
    },
    SortedSetRange {
        key: Bytes<'a>,
        start: isize,
        stop: isize,
        with_scores: bool,
        reverse: bool,
    },
    SortedSetRangeByScore {
        key: Bytes<'a>,
        min: ScoreBound,
        max: ScoreBound,
        with_scores: bool,
        reverse: bool,
        limit: Option<RangeLimit>,
        /// True when the client sent `ZRANGE ... BYSCORE` rather than `ZRANGEBYSCORE`.
        via_zrange: bool,
    },
    SortedSetRangeByLex {
        key: Bytes<'a>,
        min: LexBound<'a>,
        max: LexBound<'a>,
        with_scores: bool,
        reverse: bool,
        limit: Option<RangeLimit>,
        invocation: LexInvocation,
    },
    SortedSetRemove {
        key: Bytes<'a>,
        members: Vec<Bytes<'a>>,
    },
    SortedSetIncr {
        key: Bytes<'a>,
        delta: f64,
        member: Bytes<'a>,
    },
    SortedSetCard(Bytes<'a>),
    SortedSetCount {
        key: Bytes<'a>,
        min: ScoreBound,
        max: ScoreBound,
    },
    SortedSetLexCount {
        key: Bytes<'a>,
        min: LexBound<'a>,
        max: LexBound<'a>,
    },
    SortedSetMScore {
        key: Bytes<'a>,
        members: Vec<Bytes<'a>>,
    },
    /// `ZUNION`/`ZINTER`/`ZDIFF` and their `STORE` forms; `weights` is empty or one per key.
    SortedSetAlgebra {
        op: SetOp,
        keys: Vec<Bytes<'a>>,
        weights: Vec<f64>,
        aggregate: SortedAggregate,
        with_scores: bool,
        destination: Option<Bytes<'a>>,
    },
    SortedSetRandMember {
        key: Bytes<'a>,
        count: Option<i64>,
        with_scores: bool,
    },
    HashRandField {
        key: Bytes<'a>,
        count: Option<i64>,
        with_values: bool,
    },
    HashIncrementFloat {
        key: Bytes<'a>,
        field: Bytes<'a>,
        delta: f64,
    },
    SortedSetScore {
        key: Bytes<'a>,
        member: Bytes<'a>,
    },
    SortedSetRank {
        key: Bytes<'a>,
        member: Bytes<'a>,
        reverse: bool,
    },
    /// `ZPOPMIN` or `ZPOPMAX`. `count` is `None` when the client sent no count.
    SortedSetPop {
        key: Bytes<'a>,
        count: Option<usize>,
        max: bool,
    },
    /// `ZPOPBYSCORE key max [MIN min] [COUNT n] [WITHSCORES]`: removes and returns up to
    /// `count` members with `min <= score <= max`, lowest score first.
    SortedSetPopByScore {
        key: Bytes<'a>,
        min: ScoreBound,
        max: ScoreBound,
        count: usize,
        with_scores: bool,
    },
    /// `ZMOVEDUE source destination max [COUNT n]`: moves up to `count` members with
    /// `score <= max`, lowest first, from a sorted set to the tail of a list.
    SortedSetMoveDue {
        source: Bytes<'a>,
        destination: Bytes<'a>,
        max: ScoreBound,
        count: usize,
    },
    /// `BZPOPMIN` or `BZPOPMAX`.
    SortedSetBlockPop {
        keys: Vec<Bytes<'a>>,
        timeout_ms: Option<u64>,
        max: bool,
    },
    SortedSetRemoveRange {
        key: Bytes<'a>,
        range: SortedRemoveRange<'a>,
    },
    GeoAdd {
        key: Bytes<'a>,
        members: Vec<(f64, f64, Bytes<'a>)>,
        options: SortedSetAddOptions,
    },
    GeoDist {
        key: Bytes<'a>,
        from: Bytes<'a>,
        to: Bytes<'a>,
        unit: f64,
    },
    GeoPos {
        key: Bytes<'a>,
        members: Vec<Bytes<'a>>,
    },
    GeoHash {
        key: Bytes<'a>,
        members: Vec<Bytes<'a>>,
    },
    GeoSearch {
        key: Bytes<'a>,
        center: GeoCenter<'a>,
        area: GeoArea,
        unit: f64,
        descending: bool,
        ordered: bool,
        count: Option<usize>,
        any: bool,
        with_dist: bool,
        with_hash: bool,
        with_coord: bool,
        store: Option<GeoStore<'a>>,
        query: GeoCommand,
    },
    /// `BF.RESERVE key error capacity [EXPANSION n | NONSCALING]`; the last field is `0` for a
    /// non-scaling filter.
    BloomReserve(Bytes<'a>, f64, usize, u32),
    BloomAdd(Bytes<'a>, Bytes<'a>),
    BloomContains(Bytes<'a>, Bytes<'a>),
    /// `BF.MADD key item...`
    BloomAddMany(Bytes<'a>, Vec<Bytes<'a>>),
    /// `BF.MEXISTS key item...`
    BloomContainsMany(Bytes<'a>, Vec<Bytes<'a>>),
    /// `BF.INSERT key [CAPACITY c] [ERROR e] [EXPANSION x] [NOCREATE] [NONSCALING] ITEMS item...`
    BloomInsert {
        key: Bytes<'a>,
        error_rate: f64,
        capacity: usize,
        expansion: u32,
        no_create: bool,
        items: Vec<Bytes<'a>>,
    },
    /// `BF.INFO key [CAPACITY|SIZE|FILTERS|ITEMS|EXPANSION]`
    BloomInfo(Bytes<'a>, Option<BloomInfoField>),
    /// `BF.CARD key`
    BloomCard(Bytes<'a>),
    StreamAdd {
        key: Bytes<'a>,
        id: StreamAddId,
        max_len: Option<usize>,
        min_id: Option<(u64, u64)>,
        delay_ms: Option<u64>,
        nomkstream: bool,
        fields: Vec<(Bytes<'a>, Bytes<'a>)>,
    },
    StreamLen(Bytes<'a>),
    StreamInfo {
        key: Bytes<'a>,
        group: Option<Bytes<'a>>,
        kind: StreamInfoKind,
    },
    StreamRange {
        key: Bytes<'a>,
        start: crate::stream::StreamBound,
        end: crate::stream::StreamBound,
        count: Option<usize>,
        reverse: bool,
    },
    StreamRead {
        count: Option<usize>,
        /// `None` returns immediately. `Some(None)` is `BLOCK 0`. `Some(ms)` waits that many milliseconds.
        block: Option<Option<u64>>,
        keys: Vec<(Bytes<'a>, crate::stream::StreamCursor)>,
    },
    StreamDelete {
        key: Bytes<'a>,
        ids: Vec<(u64, u64)>,
    },
    StreamTrim {
        key: Bytes<'a>,
        max_len: usize,
    },
    StreamTrimMinId {
        key: Bytes<'a>,
        id: (u64, u64),
    },
    StreamSetId {
        key: Bytes<'a>,
        id: (u64, u64),
    },
    StreamGroupCreate {
        key: Bytes<'a>,
        group: Bytes<'a>,
        start: crate::stream::GroupStart,
        mkstream: bool,
    },
    StreamGroupRead {
        key: Bytes<'a>,
        group: Bytes<'a>,
        consumer: Bytes<'a>,
        count: Option<usize>,
        /// `None` returns immediately. `Some(None)` is `BLOCK 0`. `Some(ms)` waits that many milliseconds.
        block: Option<Option<u64>>,
        noack: bool,
    },
    StreamGroupDestroy {
        key: Bytes<'a>,
        group: Bytes<'a>,
    },
    StreamGroupSetId {
        key: Bytes<'a>,
        group: Bytes<'a>,
        start: crate::stream::GroupStart,
    },
    StreamGroupDelConsumer {
        key: Bytes<'a>,
        group: Bytes<'a>,
        consumer: Bytes<'a>,
    },
    StreamGroupCreateConsumer {
        key: Bytes<'a>,
        group: Bytes<'a>,
        consumer: Bytes<'a>,
    },
    StreamAck {
        key: Bytes<'a>,
        group: Bytes<'a>,
        ids: Vec<(u64, u64)>,
    },
    StreamPending {
        key: Bytes<'a>,
        group: Bytes<'a>,
    },
    StreamPendingRange {
        key: Bytes<'a>,
        group: Bytes<'a>,
        min_idle: Option<u64>,
        start: crate::stream::StreamBound,
        end: crate::stream::StreamBound,
        count: usize,
        consumer: Option<Bytes<'a>>,
    },
    StreamClaimPending {
        key: Bytes<'a>,
        group: Bytes<'a>,
        consumer: Bytes<'a>,
        min_idle: u64,
        ids: Vec<(u64, u64)>,
        options: ClaimOptions,
    },
    StreamAutoclaim {
        key: Bytes<'a>,
        group: Bytes<'a>,
        consumer: Bytes<'a>,
        min_idle: u64,
        start: (u64, u64),
        count: Option<usize>,
        just_id: bool,
    },
    Expire {
        key: Bytes<'a>,
        milliseconds: i64,
        condition: ExpireCondition,
    },
    PExpireAt {
        key: Bytes<'a>,
        unix_millis: i64,
        condition: ExpireCondition,
    },
    /// A write that sets a key deadline only when it creates the key (`EX`/`PX` on `INCRBY`,
    /// `ZADD`, ...). The inner write and the deadline are logged as one record.
    CreateTtl {
        ttl_ms: u64,
        inner: Box<Command<'a>>,
    },
    Ttl(Bytes<'a>),
    PTtl(Bytes<'a>),
    Persist(Bytes<'a>),
    Auth {
        username: Option<Bytes<'a>>,
        password: Bytes<'a>,
    },
    Acl(AclCommand<'a>),
    Multi,
    Exec,
    Discard,
    Watch(Vec<Bytes<'a>>),
    Unwatch,
    Eval {
        script: Bytes<'a>,
        keys: Vec<Bytes<'a>>,
        args: Vec<Bytes<'a>>,
    },
    EvalSha {
        sha: Bytes<'a>,
        keys: Vec<Bytes<'a>>,
        args: Vec<Bytes<'a>>,
    },
    ScriptLoad(Bytes<'a>),
    ScriptExists(Vec<Bytes<'a>>),
    ScriptFlush,
    ScriptKill,
    FunctionLoad {
        name: Bytes<'a>,
        body: Bytes<'a>,
    },
    FunctionList,
    FunctionDelete(Bytes<'a>),
    FCall {
        name: Bytes<'a>,
        keys: Vec<Bytes<'a>>,
        args: Vec<Bytes<'a>>,
    },
    /// Logical database index. The connection stores it; the store is not mutated.
    Select(i64),
    /// Removes every key of the selected database. `ASYNC` and `SYNC` both run inline.
    FlushDb,
    FlushAll,
    SwapDb(i64, i64),
    Move {
        key: Bytes<'a>,
        db: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclCommand<'a> {
    WhoAmI,
    Users,
    List,
    GetUser(Bytes<'a>),
    Cat(Option<Bytes<'a>>),
    SetUser {
        name: Bytes<'a>,
        rules: Vec<Bytes<'a>>,
    },
    Load,
    Save,
}

impl<'a> AclCommand<'a> {
    fn into_owned(self) -> AclCommand<'static> {
        match self {
            Self::WhoAmI => AclCommand::WhoAmI,
            Self::Users => AclCommand::Users,
            Self::List => AclCommand::List,
            Self::GetUser(name) => AclCommand::GetUser(Cow::Owned(name.into_owned())),
            Self::Cat(name) => AclCommand::Cat(name.map(|value| Cow::Owned(value.into_owned()))),
            Self::SetUser { name, rules } => AclCommand::SetUser {
                name: Cow::Owned(name.into_owned()),
                rules: rules
                    .into_iter()
                    .map(|rule| Cow::Owned(rule.into_owned()))
                    .collect(),
            },
            Self::Load => AclCommand::Load,
            Self::Save => AclCommand::Save,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamInfoKind {
    Stream,
    Groups,
    Consumers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Inter,
    Union,
    Diff,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    NegInf,
    PosInf,
}

impl ScoreBound {
    pub(crate) fn accepts_min(self, score: f64) -> bool {
        match self {
            Self::NegInf => true,
            Self::PosInf => score == f64::INFINITY,
            Self::Inclusive(bound) => score >= bound,
            Self::Exclusive(bound) => score > bound,
        }
    }

    pub(crate) fn accepts_max(self, score: f64) -> bool {
        match self {
            Self::PosInf => true,
            Self::NegInf => score == f64::NEG_INFINITY,
            Self::Inclusive(bound) => score <= bound,
            Self::Exclusive(bound) => score < bound,
        }
    }
}

/// How `LIMIT` counts. `Fixed` is the plain `LIMIT key max window_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitMode {
    Fixed,
    Sliding {
        with_retry: bool,
    },
    /// `cost` tokens are taken from a bucket of `max` tokens that refills over `window_ms`.
    Tokens {
        cost: u32,
        with_retry: bool,
    },
}

/// `LIMIT offset count` on a score or lexicographical range. A negative count means the rest of the range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeLimit {
    pub offset: usize,
    pub count: Option<usize>,
}

/// The span `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, or `ZREMRANGEBYLEX` removes.
#[derive(Debug, Clone, PartialEq)]
pub enum SortedRemoveRange<'a> {
    Rank(isize, isize),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound<'a>, LexBound<'a>),
}

/// Which collection an `SSCAN` or `ZSCAN` walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionScanKind {
    Set,
    SortedSet,
}

/// `NX`, `XX`, `GT`, or `LT` on `EXPIRE`, `PEXPIRE`, `EXPIREAT` or `PEXPIREAT`. A key without a
/// deadline counts as infinite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireCondition {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
}

/// `NX`, `XX`, `GT`, or `LT` on a hash field expire. A field without a TTL counts as infinite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldExpireCondition {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
}

/// A hash field deadline as sent. The engine reads the clock for the relative form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldExpireTime {
    InMillis(u64),
    AtMillis(u64),
}

/// Which number `HTTL`, `HPTTL`, `HEXPIRETIME`, or `HPEXPIRETIME` replies with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldTtlReply {
    Seconds,
    Millis,
    AtSeconds,
    AtMillis,
}

/// Inclusive and exclusive member bounds for `BYLEX`. `-` and `+` are the ends of the member order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound<'a> {
    NegInf,
    PosInf,
    Inclusive(Bytes<'a>),
    Exclusive(Bytes<'a>),
}

impl LexBound<'_> {
    pub fn accepts_min(&self, member: &[u8]) -> bool {
        match self {
            Self::NegInf => true,
            Self::PosInf => false,
            Self::Inclusive(bound) => member >= bound.as_ref(),
            Self::Exclusive(bound) => member > bound.as_ref(),
        }
    }

    pub fn accepts_max(&self, member: &[u8]) -> bool {
        match self {
            Self::PosInf => true,
            Self::NegInf => false,
            Self::Inclusive(bound) => member <= bound.as_ref(),
            Self::Exclusive(bound) => member < bound.as_ref(),
        }
    }
}

/// Which command name produced a lexicographical range, so ACL sees `ZRANGE` and `ZRANGEBYLEX` apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexInvocation {
    ZRange,
    ZRangeByLex,
    ZRevRangeByLex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    #[default]
    Always,
    NotExists,
    Exists,
}

/// The single field `BF.INFO key <field>` asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomInfoField {
    Capacity,
    Size,
    Filters,
    Items,
    Expansion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetExpiry {
    #[default]
    None,
    KeepTtl,
    RelativeMs(i64),
    AbsoluteMs(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortedSetCondition {
    #[default]
    Always,
    NotExists,
    Exists,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GeoCenter<'a> {
    Member(Bytes<'a>),
    Coordinates(f64, f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoArea {
    Radius(f64),
    Box { width_m: f64, height_m: f64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeoStore<'a> {
    pub destination: Bytes<'a>,
    pub distance: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoCommand {
    Search,
    SearchStore,
    Radius,
    RadiusByMember,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SortedSetAddOptions {
    pub condition: SortedSetCondition,
    pub greater: bool,
    pub less: bool,
    pub changed: bool,
    pub increment: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SetOptions {
    pub expiry: SetExpiry,
    pub condition: SetCondition,
    pub get: bool,
}

#[derive(Debug)]
pub enum ProtocolError {
    Io(io::Error),
    Invalid(&'static str),
    LimitExceeded(&'static str),
    /// A complete command frame named something this server will not run.
    /// The bytes are consumed. Callers reply and keep the connection.
    Rejected {
        message: &'static str,
        consumed: usize,
    },
    /// A bulk argument over the key/value limit in a frame whose declared size is still
    /// within `max_command_bytes`. The first `parsed` bytes (through that argument's
    /// length line) are consumed; the caller discards `skip` more bytes and then
    /// `args_left` further bulk arguments (see [`bulk_header`]), replies with `message`
    /// and keeps the connection, without buffering the frame.
    Oversized {
        message: &'static str,
        parsed: usize,
        skip: usize,
        args_left: usize,
    },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Invalid(message) => write!(formatter, "invalid RESP frame: {message}"),
            Self::LimitExceeded(message) => write!(formatter, "RESP limit exceeded: {message}"),
            Self::Rejected { message, .. } | Self::Oversized { message, .. } => {
                write!(formatter, "rejected command: {message}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<io::Error> for ProtocolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// `EX seconds` or `PX milliseconds` at `at`, as milliseconds, for a write that sets a
/// deadline when it creates the key. `None` when the argument at `at` is neither keyword.
pub(crate) fn create_ttl_option(
    args: &table::Args<'_, '_>,
    at: usize,
) -> Result<Option<u64>, ProtocolError> {
    let option = args.bytes(at);
    let seconds = option.eq_ignore_ascii_case(b"EX");

    if !seconds && !option.eq_ignore_ascii_case(b"PX") {
        return Ok(None);
    }

    if at + 1 >= args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    const INVALID: ProtocolError = ProtocolError::Invalid("invalid expire time");

    let value = u64::try_from(parse_i64(args.bytes(at + 1))?).map_err(|_| INVALID)?;

    if value == 0 {
        return Err(INVALID);
    }

    if seconds {
        value.checked_mul(1_000).map(Some).ok_or(INVALID)
    } else {
        Ok(Some(value))
    }
}

/// `command`, wrapped so the deadline is set only when it creates the key.
pub(crate) fn with_create_ttl(ttl_ms: Option<u64>, command: Command<'_>) -> Command<'_> {
    match ttl_ms {
        Some(ttl_ms) => Command::CreateTtl {
            ttl_ms,
            inner: Box::new(command),
        },
        None => command,
    }
}
