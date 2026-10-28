use std::{convert::Infallible, io};

use crate::protocol::Bytes;

/// A validated state change in the shard's canonical mutation vocabulary.
///
/// Mutations borrow command input only until they cross the durability
/// boundary. A persistence implementation must encode or otherwise take
/// ownership of everything it needs before returning from `record`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mutation<'a> {
    /// Atomically publishes the packed idempotency record and its original absolute deadline.
    IdempotencyPut {
        key: &'a [u8],
        value: &'a [u8],
        expires_at: u64,
    },
    SetBytes {
        key: &'a [u8],
        value: &'a [u8],
    },
    /// A string and its absolute deadline in one record, so no torn log tail can keep the
    /// value without its TTL.
    SetBytesExpiring {
        key: &'a [u8],
        value: &'a [u8],
        expires_at: u64,
    },
    JsonPut {
        key: &'a [u8],
        value: &'a [u8],
    },
    JsonSetPath {
        key: &'a [u8],
        path: &'a [u8],
        value: &'a [u8],
    },
    JsonMergePath {
        key: &'a [u8],
        path: &'a [u8],
        value: &'a [u8],
    },
    JsonDelPath {
        key: &'a [u8],
        path: &'a [u8],
    },
    SetInteger {
        key: &'a [u8],
        value: i64,
    },
    Delete {
        keys: &'a [Bytes<'a>],
    },
    ListPushFront {
        key: &'a [u8],
        values: &'a [Bytes<'a>],
    },
    ListPopFront {
        key: &'a [u8],
    },
    ListPushBack {
        key: &'a [u8],
        values: &'a [Bytes<'a>],
    },
    ListPopBack {
        key: &'a [u8],
    },
    ListTrim {
        key: &'a [u8],
        start: i64,
        stop: i64,
    },
    /// Overwrites `bytes` at `offset` of a string, zero-padding it first when it is shorter
    /// (`SETRANGE` semantics). Bit, bitfield and HyperLogLog writes log only what changed.
    SetRange {
        key: &'a [u8],
        offset: u64,
        bytes: &'a [u8],
    },
    /// `LSET`: replaces the element at `index` (negative counts from the tail).
    ListSet {
        key: &'a [u8],
        index: i64,
        value: &'a [u8],
    },
    /// `LINSERT`: inserts `value` next to the first `pivot`.
    ListInsert {
        key: &'a [u8],
        before: bool,
        pivot: &'a [u8],
        value: &'a [u8],
    },
    /// `LREM`: removes `count` matches of `value` (negative from the tail, 0 all).
    ListRemove {
        key: &'a [u8],
        count: i64,
        value: &'a [u8],
    },
    SetAdd {
        key: &'a [u8],
        members: &'a [Bytes<'a>],
    },
    SetRemove {
        key: &'a [u8],
        members: &'a [Bytes<'a>],
    },
    SetStore {
        key: &'a [u8],
        members: &'a [Bytes<'a>],
    },
    SetMove {
        source: &'a [u8],
        destination: &'a [u8],
        member: &'a [u8],
    },
    HashSet {
        key: &'a [u8],
        pairs: &'a [(Bytes<'a>, Bytes<'a>)],
    },
    HashDelete {
        key: &'a [u8],
        fields: &'a [Bytes<'a>],
    },
    /// One absolute deadline for each listed field. Only fields that exist and passed the
    /// `NX`/`XX`/`GT`/`LT` check are listed.
    HashFieldExpire {
        key: &'a [u8],
        fields: &'a [Bytes<'a>],
        unix_millis: u64,
    },
    HashFieldPersist {
        key: &'a [u8],
        fields: &'a [Bytes<'a>],
    },
    SemaphoreAcquire {
        key: &'a [u8],
        token: u64,
        expires_at: u64,
    },
    SemaphoreRelease {
        key: &'a [u8],
        token: u64,
    },
    SortedSetAdd {
        key: &'a [u8],
        pairs: &'a [(f64, Bytes<'a>)],
    },
    SortedSetRemove {
        key: &'a [u8],
        members: &'a [Bytes<'a>],
    },
    BloomReserve {
        key: &'a [u8],
        error_rate: f64,
        capacity: usize,
        /// `0` for a non-scaling filter.
        expansion: u32,
    },
    BloomAdd {
        key: &'a [u8],
        value: &'a [u8],
    },
    BloomAddMany {
        key: &'a [u8],
        values: &'a [Bytes<'a>],
    },
    StreamAppend {
        key: &'a [u8],
        millis: u64,
        sequence: u64,
        fields: &'a [(Bytes<'a>, Bytes<'a>)],
    },
    StreamDelete {
        key: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    StreamTrim {
        key: &'a [u8],
        max_len: usize,
    },
    StreamTrimMinId {
        key: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    StreamSetId {
        key: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    StreamGroupCreate {
        key: &'a [u8],
        group: &'a [u8],
        millis: u64,
        sequence: u64,
        mkstream: bool,
    },
    StreamClaim {
        key: &'a [u8],
        group: &'a [u8],
        consumer: &'a [u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
    },
    StreamReclaim {
        key: &'a [u8],
        group: &'a [u8],
        consumer: &'a [u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
        deliveries: u64,
    },
    StreamForceClaim {
        key: &'a [u8],
        group: &'a [u8],
        consumer: &'a [u8],
        millis: u64,
        sequence: u64,
        delivered_at: u64,
        deliveries: u64,
    },
    StreamAck {
        key: &'a [u8],
        group: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    StreamGroupDestroy {
        key: &'a [u8],
        group: &'a [u8],
    },
    StreamGroupSetId {
        key: &'a [u8],
        group: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    StreamGroupDelConsumer {
        key: &'a [u8],
        group: &'a [u8],
        consumer: &'a [u8],
    },
    StreamGroupCreateConsumer {
        key: &'a [u8],
        group: &'a [u8],
        consumer: &'a [u8],
    },
    StreamHold {
        key: &'a [u8],
        millis: u64,
        sequence: u64,
        ready_at: u64,
    },
    StreamGroupAdvance {
        key: &'a [u8],
        group: &'a [u8],
        millis: u64,
        sequence: u64,
    },
    ExpireAt {
        key: &'a [u8],
        unix_millis: u64,
    },
    Persist {
        key: &'a [u8],
    },
    Rename {
        source: &'a [u8],
        destination: &'a [u8],
    },
    /// Empties the record's database.
    FlushDatabase,
    FlushAll,
    SwapDatabases {
        first: u8,
        second: u8,
    },
    /// Moves `key` from the record's database to `destination`.
    MoveKey {
        key: &'a [u8],
        destination: u8,
    },
    /// A key's value copied from another shard by a slot move, as a checkpoint entry frame
    /// (`None`: the key is absent there). Replaces what this shard held under the name.
    SlotCopy {
        key: &'a [u8],
        entry: Option<&'a [u8]>,
    },
    /// Slots `first..=last` moved from this shard to shard `to`, whose log continues them
    /// from `position`. Changes nothing here; a change stream reports it.
    SlotsMoved {
        first: u16,
        last: u16,
        to: u16,
        position: u64,
    },
    /// Slots `first..=last` are arriving from another node: a step of that move
    /// ([`crate::slot_moves::IncomingStep`]). Changes nothing here; a replica records it.
    SlotsIncoming {
        first: u16,
        last: u16,
        step: u8,
    },
    /// Slots `first..=last` moved from this shard to another node, reached at `address`,
    /// whose log continues them from `position`. A change stream reports it.
    SlotsGiven {
        first: u16,
        last: u16,
        address: &'a [u8],
        position: u64,
    },
    LeasePut {
        key: &'a [u8],
        token: u64,
        expires_at: u64,
    },
    SetVersioned {
        key: &'a [u8],
        value: &'a [u8],
        version: u64,
    },
    LimitPut {
        key: &'a [u8],
        used: u64,
    },
    OncePut {
        key: &'a [u8],
        expires_at: u64,
    },
    Transaction {
        mutations: &'a [OwnedMutation],
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedMutation {
    IdempotencyPut {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    SetBytes {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    SetBytesExpiring {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    JsonPut {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    JsonSetPath {
        key: Vec<u8>,
        path: Vec<u8>,
        value: Vec<u8>,
    },
    JsonMergePath {
        key: Vec<u8>,
        path: Vec<u8>,
        value: Vec<u8>,
    },
    JsonDelPath {
        key: Vec<u8>,
        path: Vec<u8>,
    },
    SetInteger {
        key: Vec<u8>,
        value: i64,
    },
    Delete {
        keys: Vec<Vec<u8>>,
    },
    ListPushFront {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    ListPopFront {
        key: Vec<u8>,
    },
    ListPushBack {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    ListPopBack {
        key: Vec<u8>,
    },
    ListTrim {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    SetRange {
        key: Vec<u8>,
        offset: u64,
        bytes: Vec<u8>,
    },
    ListSet {
        key: Vec<u8>,
        index: i64,
        value: Vec<u8>,
    },
    ListInsert {
        key: Vec<u8>,
        before: bool,
        pivot: Vec<u8>,
        value: Vec<u8>,
    },
    ListRemove {
        key: Vec<u8>,
        count: i64,
        value: Vec<u8>,
    },
    SetAdd {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SetRemove {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SetStore {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SetMove {
        source: Vec<u8>,
        destination: Vec<u8>,
        member: Vec<u8>,
    },
    HashSet {
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    HashDelete {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HashFieldExpire {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        unix_millis: u64,
    },
    HashFieldPersist {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    SemaphoreAcquire {
        key: Vec<u8>,
        token: u64,
        expires_at: u64,
    },
    SemaphoreRelease {
        key: Vec<u8>,
        token: u64,
    },
    SortedSetAdd {
        key: Vec<u8>,
        pairs: Vec<(f64, Vec<u8>)>,
    },
    SortedSetRemove {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    BloomReserve {
        key: Vec<u8>,
        error_rate: f64,
        capacity: u64,
        expansion: u32,
    },
    BloomAdd {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    BloomAddMany {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    StreamAppend {
        key: Vec<u8>,
        millis: u64,
        sequence: u64,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    },
    StreamDelete {
        key: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    StreamTrim {
        key: Vec<u8>,
        max_len: u64,
    },
    StreamTrimMinId {
        key: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    StreamSetId {
        key: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    StreamGroupCreate {
        key: Vec<u8>,
        group: Vec<u8>,
        millis: u64,
        sequence: u64,
        mkstream: bool,
    },
    StreamClaim {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
        millis: u64,
        sequence: u64,
        delivered_at: u64,
    },
    StreamReclaim {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
        millis: u64,
        sequence: u64,
        delivered_at: u64,
        deliveries: u64,
    },
    StreamForceClaim {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
        millis: u64,
        sequence: u64,
        delivered_at: u64,
        deliveries: u64,
    },
    StreamAck {
        key: Vec<u8>,
        group: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    StreamGroupDestroy {
        key: Vec<u8>,
        group: Vec<u8>,
    },
    StreamGroupSetId {
        key: Vec<u8>,
        group: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    StreamGroupDelConsumer {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
    },
    StreamGroupCreateConsumer {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
    },
    StreamHold {
        key: Vec<u8>,
        millis: u64,
        sequence: u64,
        ready_at: u64,
    },
    StreamGroupAdvance {
        key: Vec<u8>,
        group: Vec<u8>,
        millis: u64,
        sequence: u64,
    },
    ExpireAt {
        key: Vec<u8>,
        unix_millis: u64,
    },
    Persist {
        key: Vec<u8>,
    },
    Rename {
        source: Vec<u8>,
        destination: Vec<u8>,
    },
    FlushDatabase,
    FlushAll,
    SwapDatabases {
        first: u8,
        second: u8,
    },
    MoveKey {
        key: Vec<u8>,
        destination: u8,
    },
    SlotCopy {
        key: Vec<u8>,
        entry: Option<Vec<u8>>,
    },
    SlotsMoved {
        first: u16,
        last: u16,
        to: u16,
        position: u64,
    },
    SlotsIncoming {
        first: u16,
        last: u16,
        step: u8,
    },
    SlotsGiven {
        first: u16,
        last: u16,
        address: Vec<u8>,
        position: u64,
    },
    LeasePut {
        key: Vec<u8>,
        token: u64,
        expires_at: u64,
    },
    SetVersioned {
        key: Vec<u8>,
        value: Vec<u8>,
        version: u64,
    },
    LimitPut {
        key: Vec<u8>,
        used: u64,
        /// Only in records written before the window end moved to `ExpireAt` (WAL tag 46).
        expires_at: Option<u64>,
    },
    OncePut {
        key: Vec<u8>,
        expires_at: u64,
    },
    Transaction {
        mutations: Vec<OwnedMutation>,
    },
}

impl OwnedMutation {
    pub fn write_keys(&self) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();

        self.for_each_write_key(&mut |key| keys.push(key.to_vec()));
        keys
    }

    /// Calls `visit` with each key this mutation writes in the database it is applied in,
    /// without collecting them. A `MoveKey` also writes its key in the destination
    /// database, and database-wide mutations name no key.
    pub fn for_each_write_key(&self, visit: &mut impl FnMut(&[u8])) {
        match self {
            Self::Delete { keys } => keys.iter().for_each(|key| visit(key)),
            Self::SetMove {
                source,
                destination,
                ..
            }
            | Self::Rename {
                source,
                destination,
            } => {
                visit(source);
                visit(destination);
            }
            Self::Transaction { mutations } => {
                for mutation in mutations {
                    mutation.for_each_write_key(visit);
                }
            }

            Self::FlushDatabase
            | Self::FlushAll
            | Self::SwapDatabases { .. }
            | Self::SlotsMoved { .. }
            | Self::SlotsIncoming { .. }
            | Self::SlotsGiven { .. } => {}

            Self::SetBytes { key, .. }
            | Self::SetBytesExpiring { key, .. }
            | Self::JsonPut { key, .. }
            | Self::JsonSetPath { key, .. }
            | Self::JsonMergePath { key, .. }
            | Self::JsonDelPath { key, .. }
            | Self::IdempotencyPut { key, .. }
            | Self::SetInteger { key, .. }
            | Self::LeasePut { key, .. }
            | Self::SetVersioned { key, .. }
            | Self::LimitPut { key, .. }
            | Self::OncePut { key, .. }
            | Self::ListPushFront { key, .. }
            | Self::ListPopFront { key }
            | Self::ListPushBack { key, .. }
            | Self::ListPopBack { key }
            | Self::ListTrim { key, .. }
            | Self::SetRange { key, .. }
            | Self::ListSet { key, .. }
            | Self::ListInsert { key, .. }
            | Self::ListRemove { key, .. }
            | Self::SetAdd { key, .. }
            | Self::SetRemove { key, .. }
            | Self::SetStore { key, .. }
            | Self::HashSet { key, .. }
            | Self::HashDelete { key, .. }
            | Self::HashFieldExpire { key, .. }
            | Self::HashFieldPersist { key, .. }
            | Self::SemaphoreAcquire { key, .. }
            | Self::SemaphoreRelease { key, .. }
            | Self::SortedSetAdd { key, .. }
            | Self::SortedSetRemove { key, .. }
            | Self::BloomReserve { key, .. }
            | Self::BloomAdd { key, .. }
            | Self::BloomAddMany { key, .. }
            | Self::StreamAppend { key, .. }
            | Self::StreamDelete { key, .. }
            | Self::StreamTrim { key, .. }
            | Self::StreamTrimMinId { key, .. }
            | Self::StreamSetId { key, .. }
            | Self::StreamGroupCreate { key, .. }
            | Self::StreamClaim { key, .. }
            | Self::StreamReclaim { key, .. }
            | Self::StreamForceClaim { key, .. }
            | Self::StreamAck { key, .. }
            | Self::StreamGroupDestroy { key, .. }
            | Self::StreamGroupSetId { key, .. }
            | Self::StreamGroupDelConsumer { key, .. }
            | Self::StreamGroupCreateConsumer { key, .. }
            | Self::StreamHold { key, .. }
            | Self::StreamGroupAdvance { key, .. }
            | Self::ExpireAt { key, .. }
            | Self::Persist { key }
            | Self::MoveKey { key, .. }
            | Self::SlotCopy { key, .. } => visit(key),
        }
    }
}

impl Mutation<'_> {
    pub fn to_owned(self) -> OwnedMutation {
        match self {
            Self::SetBytes { key, value } => OwnedMutation::SetBytes {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            Self::SetBytesExpiring {
                key,
                value,
                expires_at,
            } => OwnedMutation::SetBytesExpiring {
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at,
            },
            Self::JsonPut { key, value } => OwnedMutation::JsonPut {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            Self::JsonSetPath { key, path, value } => OwnedMutation::JsonSetPath {
                key: key.to_vec(),
                path: path.to_vec(),
                value: value.to_vec(),
            },
            Self::JsonMergePath { key, path, value } => OwnedMutation::JsonMergePath {
                key: key.to_vec(),
                path: path.to_vec(),
                value: value.to_vec(),
            },
            Self::JsonDelPath { key, path } => OwnedMutation::JsonDelPath {
                key: key.to_vec(),
                path: path.to_vec(),
            },
            Self::IdempotencyPut {
                key,
                value,
                expires_at,
            } => OwnedMutation::IdempotencyPut {
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at,
            },
            Self::SetInteger { key, value } => OwnedMutation::SetInteger {
                key: key.to_vec(),
                value,
            },
            Self::Delete { keys } => OwnedMutation::Delete {
                keys: keys.iter().map(|key| key.to_vec()).collect(),
            },
            Self::ListPushFront { key, values } => OwnedMutation::ListPushFront {
                key: key.to_vec(),
                values: values.iter().map(|value| value.to_vec()).collect(),
            },
            Self::ListPopFront { key } => OwnedMutation::ListPopFront { key: key.to_vec() },
            Self::ListPushBack { key, values } => OwnedMutation::ListPushBack {
                key: key.to_vec(),
                values: values.iter().map(|value| value.to_vec()).collect(),
            },
            Self::ListPopBack { key } => OwnedMutation::ListPopBack { key: key.to_vec() },
            Self::ListTrim { key, start, stop } => OwnedMutation::ListTrim {
                key: key.to_vec(),
                start,
                stop,
            },
            Self::SetRange { key, offset, bytes } => OwnedMutation::SetRange {
                key: key.to_vec(),
                offset,
                bytes: bytes.to_vec(),
            },
            Self::ListSet { key, index, value } => OwnedMutation::ListSet {
                key: key.to_vec(),
                index,
                value: value.to_vec(),
            },
            Self::ListInsert {
                key,
                before,
                pivot,
                value,
            } => OwnedMutation::ListInsert {
                key: key.to_vec(),
                before,
                pivot: pivot.to_vec(),
                value: value.to_vec(),
            },
            Self::ListRemove { key, count, value } => OwnedMutation::ListRemove {
                key: key.to_vec(),
                count,
                value: value.to_vec(),
            },
            Self::SetAdd { key, members } => OwnedMutation::SetAdd {
                key: key.to_vec(),
                members: members.iter().map(|member| member.to_vec()).collect(),
            },
            Self::SetRemove { key, members } => OwnedMutation::SetRemove {
                key: key.to_vec(),
                members: members.iter().map(|member| member.to_vec()).collect(),
            },
            Self::SetStore { key, members } => OwnedMutation::SetStore {
                key: key.to_vec(),
                members: members.iter().map(|member| member.to_vec()).collect(),
            },
            Self::SetMove {
                source,
                destination,
                member,
            } => OwnedMutation::SetMove {
                source: source.to_vec(),
                destination: destination.to_vec(),
                member: member.to_vec(),
            },
            Self::HashSet { key, pairs } => OwnedMutation::HashSet {
                key: key.to_vec(),
                pairs: pairs
                    .iter()
                    .map(|(field, value)| (field.to_vec(), value.to_vec()))
                    .collect(),
            },
            Self::HashDelete { key, fields } => OwnedMutation::HashDelete {
                key: key.to_vec(),
                fields: fields.iter().map(|field| field.to_vec()).collect(),
            },
            Self::HashFieldExpire {
                key,
                fields,
                unix_millis,
            } => OwnedMutation::HashFieldExpire {
                key: key.to_vec(),
                fields: fields.iter().map(|field| field.to_vec()).collect(),
                unix_millis,
            },
            Self::HashFieldPersist { key, fields } => OwnedMutation::HashFieldPersist {
                key: key.to_vec(),
                fields: fields.iter().map(|field| field.to_vec()).collect(),
            },
            Self::SemaphoreAcquire {
                key,
                token,
                expires_at,
            } => OwnedMutation::SemaphoreAcquire {
                key: key.to_vec(),
                token,
                expires_at,
            },
            Self::SemaphoreRelease { key, token } => OwnedMutation::SemaphoreRelease {
                key: key.to_vec(),
                token,
            },
            Self::SortedSetAdd { key, pairs } => OwnedMutation::SortedSetAdd {
                key: key.to_vec(),
                pairs: pairs
                    .iter()
                    .map(|(score, member)| (*score, member.to_vec()))
                    .collect(),
            },
            Self::SortedSetRemove { key, members } => OwnedMutation::SortedSetRemove {
                key: key.to_vec(),
                members: members.iter().map(|member| member.to_vec()).collect(),
            },
            Self::BloomReserve {
                key,
                error_rate,
                capacity,
                expansion,
            } => OwnedMutation::BloomReserve {
                key: key.to_vec(),
                error_rate,
                capacity: capacity as u64,
                expansion,
            },
            Self::BloomAddMany { key, values } => OwnedMutation::BloomAddMany {
                key: key.to_vec(),
                values: values.iter().map(|value| value.to_vec()).collect(),
            },
            Self::BloomAdd { key, value } => OwnedMutation::BloomAdd {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            Self::StreamAppend {
                key,
                millis,
                sequence,
                fields,
            } => OwnedMutation::StreamAppend {
                key: key.to_vec(),
                millis,
                sequence,
                fields: fields
                    .iter()
                    .map(|(field, value)| (field.to_vec(), value.to_vec()))
                    .collect(),
            },
            Self::StreamDelete {
                key,
                millis,
                sequence,
            } => OwnedMutation::StreamDelete {
                key: key.to_vec(),
                millis,
                sequence,
            },
            Self::StreamTrim { key, max_len } => OwnedMutation::StreamTrim {
                key: key.to_vec(),
                max_len: max_len as u64,
            },
            Self::StreamTrimMinId {
                key,
                millis,
                sequence,
            } => OwnedMutation::StreamTrimMinId {
                key: key.to_vec(),
                millis,
                sequence,
            },
            Self::StreamSetId {
                key,
                millis,
                sequence,
            } => OwnedMutation::StreamSetId {
                key: key.to_vec(),
                millis,
                sequence,
            },
            Self::StreamGroupCreate {
                key,
                group,
                millis,
                sequence,
                mkstream,
            } => OwnedMutation::StreamGroupCreate {
                key: key.to_vec(),
                group: group.to_vec(),
                millis,
                sequence,
                mkstream,
            },
            Self::StreamClaim {
                key,
                group,
                consumer,
                millis,
                sequence,
                delivered_at,
            } => OwnedMutation::StreamClaim {
                key: key.to_vec(),
                group: group.to_vec(),
                consumer: consumer.to_vec(),
                millis,
                sequence,
                delivered_at,
            },
            Self::StreamReclaim {
                key,
                group,
                consumer,
                millis,
                sequence,
                delivered_at,
                deliveries,
            } => OwnedMutation::StreamReclaim {
                key: key.to_vec(),
                group: group.to_vec(),
                consumer: consumer.to_vec(),
                millis,
                sequence,
                delivered_at,
                deliveries,
            },
            Self::StreamForceClaim {
                key,
                group,
                consumer,
                millis,
                sequence,
                delivered_at,
                deliveries,
            } => OwnedMutation::StreamForceClaim {
                key: key.to_vec(),
                group: group.to_vec(),
                consumer: consumer.to_vec(),
                millis,
                sequence,
                delivered_at,
                deliveries,
            },
            Self::StreamAck {
                key,
                group,
                millis,
                sequence,
            } => OwnedMutation::StreamAck {
                key: key.to_vec(),
                group: group.to_vec(),
                millis,
                sequence,
            },
            Self::StreamGroupDestroy { key, group } => OwnedMutation::StreamGroupDestroy {
                key: key.to_vec(),
                group: group.to_vec(),
            },
            Self::StreamGroupSetId {
                key,
                group,
                millis,
                sequence,
            } => OwnedMutation::StreamGroupSetId {
                key: key.to_vec(),
                group: group.to_vec(),
                millis,
                sequence,
            },
            Self::StreamGroupDelConsumer {
                key,
                group,
                consumer,
            } => OwnedMutation::StreamGroupDelConsumer {
                key: key.to_vec(),
                group: group.to_vec(),
                consumer: consumer.to_vec(),
            },
            Self::StreamGroupCreateConsumer {
                key,
                group,
                consumer,
            } => OwnedMutation::StreamGroupCreateConsumer {
                key: key.to_vec(),
                group: group.to_vec(),
                consumer: consumer.to_vec(),
            },
            Self::StreamHold {
                key,
                millis,
                sequence,
                ready_at,
            } => OwnedMutation::StreamHold {
                key: key.to_vec(),
                millis,
                sequence,
                ready_at,
            },
            Self::StreamGroupAdvance {
                key,
                group,
                millis,
                sequence,
            } => OwnedMutation::StreamGroupAdvance {
                key: key.to_vec(),
                group: group.to_vec(),
                millis,
                sequence,
            },
            Self::ExpireAt { key, unix_millis } => OwnedMutation::ExpireAt {
                key: key.to_vec(),
                unix_millis,
            },
            Self::Persist { key } => OwnedMutation::Persist { key: key.to_vec() },
            Self::Rename {
                source,
                destination,
            } => OwnedMutation::Rename {
                source: source.to_vec(),
                destination: destination.to_vec(),
            },
            Self::FlushDatabase => OwnedMutation::FlushDatabase,
            Self::FlushAll => OwnedMutation::FlushAll,
            Self::SwapDatabases { first, second } => OwnedMutation::SwapDatabases { first, second },
            Self::MoveKey { key, destination } => OwnedMutation::MoveKey {
                key: key.to_vec(),
                destination,
            },
            Self::SlotCopy { key, entry } => OwnedMutation::SlotCopy {
                key: key.to_vec(),
                entry: entry.map(<[u8]>::to_vec),
            },
            Self::SlotsMoved {
                first,
                last,
                to,
                position,
            } => OwnedMutation::SlotsMoved {
                first,
                last,
                to,
                position,
            },
            Self::SlotsIncoming { first, last, step } => {
                OwnedMutation::SlotsIncoming { first, last, step }
            }
            Self::SlotsGiven {
                first,
                last,
                address,
                position,
            } => OwnedMutation::SlotsGiven {
                first,
                last,
                address: address.to_vec(),
                position,
            },
            Self::LeasePut {
                key,
                token,
                expires_at,
            } => OwnedMutation::LeasePut {
                key: key.to_vec(),
                token,
                expires_at,
            },
            Self::SetVersioned {
                key,
                value,
                version,
            } => OwnedMutation::SetVersioned {
                key: key.to_vec(),
                value: value.to_vec(),
                version,
            },
            Self::LimitPut { key, used } => OwnedMutation::LimitPut {
                key: key.to_vec(),
                used,
                expires_at: None,
            },
            Self::OncePut { key, expires_at } => OwnedMutation::OncePut {
                key: key.to_vec(),
                expires_at,
            },
            Self::Transaction { mutations } => OwnedMutation::Transaction {
                mutations: mutations.to_vec(),
            },
        }
    }
}

/// The durability boundary between validation and in-memory mutation.
///
/// Implementations may append to a WAL, replicate, or observe mutations. An
/// error guarantees that the corresponding mutation has not yet been applied
/// to the shard.
pub trait MutationSink {
    type Error;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error>;

    /// `record` for a caller that rolls back when the record is refused (a script or `MULTI`
    /// batch): a sink that stages records writes this one at once, so a failed write is
    /// refused here and not after the caller's changes can no longer be undone.
    fn record_now(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.record(mutation)
    }

    /// Database that later `record` calls belong to. Sinks that persist must keep it.
    fn select_database(&mut self, _database: u8) {}

    /// False for sinks whose records all land in one database, such as a transaction buffer.
    fn switches_databases(&self) -> bool {
        true
    }

    /// False when `record` keeps nothing, and so never refuses one. Memory mode can then
    /// edit a string in place, and `MULTI` keeps no undo copies.
    fn persists(&self) -> bool {
        true
    }

    /// Sequence of the last record this sink wrote to the log, for sinks that have one.
    fn written_sequence(&self) -> Option<u64> {
        None
    }
}

/// Mutation sink used when persistence and replication are disabled.
#[derive(Debug, Default)]
pub struct NoopMutationSink;

impl MutationSink for NoopMutationSink {
    type Error = Infallible;

    #[inline]
    fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn persists(&self) -> bool {
        false
    }
}

/// Error returned while executing and encoding a command through a mutation
/// sink.
#[derive(Debug)]
pub enum ExecuteError<E> {
    Mutation(E),
    Io(io::Error),
}

impl<E> From<io::Error> for ExecuteError<E> {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
