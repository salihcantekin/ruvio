use crate::protocol::{Command, HotCommand};

pub const HASH_SLOT_COUNT: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrossSlot;

/// Slot table entries from here on mark a range frozen while it moves to another node:
/// `FROZEN_BASE + shard` is still held by `shard`, but its commands answer `TRYAGAIN`.
/// Entries between the shard count and this value are the other groups of the cluster.
pub const FROZEN_BASE: usize = 0x8000;

pub fn hash_slot(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % HASH_SLOT_COUNT as u16
}

pub fn shard_for_slot(slot: u16, shard_count: usize) -> usize {
    assert!(shard_count > 0, "shard count must be positive");
    usize::from(slot) * shard_count / HASH_SLOT_COUNT
}

/// Which shard owns each hash slot, read on every routed command instead of computing it.
///
/// Built from [`shard_for_slot`], so a data directory keeps the layout it was written with.
/// Entries are atomics so a later change can move one slot in place without a lock; a
/// `Relaxed` load is a plain load. Moved ranges (`SLOTS`) are applied at start. Cloning
/// shares the table.
#[derive(Clone)]
pub struct SlotTable(std::sync::Arc<[std::sync::atomic::AtomicU16]>);

impl SlotTable {
    /// The table the formula gives for `shard_count` shards.
    pub fn from_formula(shard_count: usize) -> Self {
        assert!(
            shard_count > 0 && shard_count <= usize::from(u16::MAX),
            "shard count must fit a slot table entry"
        );

        Self(
            (0..HASH_SLOT_COUNT)
                .map(|slot| {
                    std::sync::atomic::AtomicU16::new(
                        shard_for_slot(slot as u16, shard_count) as u16
                    )
                })
                .collect(),
        )
    }

    /// The shard that owns `slot`.
    #[inline]
    pub fn owner(&self, slot: u16) -> usize {
        usize::from(self.0[usize::from(slot)].load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Gives `slot` to `owner`.
    pub fn assign(&self, slot: u16, owner: usize) {
        self.0[usize::from(slot)].store(owner as u16, std::sync::atomic::Ordering::Relaxed);
    }

    /// Consecutive slots of one owner as `(first, last, owner)`, in slot order.
    pub fn ranges(&self) -> Vec<(u16, u16, usize)> {
        let mut ranges: Vec<(u16, u16, usize)> = Vec::new();

        for slot in 0..HASH_SLOT_COUNT as u16 {
            let owner = self.owner(slot);

            match ranges.last_mut() {
                Some((_, last, previous)) if *previous == owner => *last = slot,
                _ => ranges.push((slot, slot, owner)),
            }
        }

        ranges
    }
}

impl std::fmt::Debug for SlotTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlotTable")
            .field("ranges", &self.ranges().len())
            .finish()
    }
}

/// Two tables are equal when they give every slot the same owner.
impl PartialEq for SlotTable {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
            || (0..HASH_SLOT_COUNT as u16).all(|slot| self.owner(slot) == other.owner(slot))
    }
}

impl Eq for SlotTable {}

pub fn slot_range_for_shard(shard_id: usize, shard_count: usize) -> (u16, u16) {
    assert!(shard_count > 0, "shard count must be positive");
    assert!(shard_id < shard_count, "shard id must be in range");
    let start = (shard_id * HASH_SLOT_COUNT).div_ceil(shard_count);
    let end = ((shard_id + 1) * HASH_SLOT_COUNT).div_ceil(shard_count) - 1;

    (start as u16, end as u16)
}

pub fn command_slot(command: &Command<'_>) -> Result<Option<u16>, CrossSlot> {
    match command {
        // `SCAN … SLOT n` runs where slot `n` lives; a plain `SCAN` is keyless.
        Command::Scan {
            slot: Some(slot), ..
        } => Ok(Some(*slot)),
        Command::CreateTtl { inner, .. } => command_slot(inner),
        Command::ShardUnsubscribe(channels) if channels.is_empty() => Ok(None),
        Command::ShardPublish { channel, .. } => Ok(Some(hash_slot(channel))),
        Command::ShardSubscribe(channels) | Command::ShardUnsubscribe(channels) => {
            common_slot(channels.iter().map(AsRef::as_ref)).map(Some)
        }

        Command::Ping(_)
        | Command::CommandMeta { .. }
        | Command::Wait { .. }
        | Command::Ruvio(_)
        | Command::Changes(_)
        | Command::ChangesSubscribe(_)
        | Command::Hello { .. }
        | Command::ClientGetName
        | Command::ClientSetName(_)
        | Command::ClientTracking(_)
        | Command::ClientDurability { .. }
        | Command::ClusterSlots
        | Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ClusterKeySlot(_)
        | Command::ReadOnly
        | Command::ReadWrite
        | Command::ConfigGet(_)
        | Command::Info(_)
        | Command::Save
        | Command::ConfigSet(_)
        | Command::ConfigRewrite
        | Command::ReplicaOf { .. }
        | Command::MoveSlots(_)
        | Command::Topology(_)
        | Command::PrimaryLease(_)
        | Command::Migrate(_)
        | Command::Role
        | Command::Bgsave
        | Command::Scan { .. }
        | Command::Keys(_)
        | Command::JsonIndex { .. }
        | Command::RandomKey
        | Command::ClientId
        | Command::ClientSetInfo
        | Command::ClientList { .. }
        | Command::ClientInfo
        | Command::ClientKill { .. }
        | Command::Slowlog(_)
        | Command::LatencyLatest
        | Command::Echo(_)
        | Command::DbSize
        | Command::Auth { .. }
        | Command::Acl(_)
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Unwatch
        | Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill
        | Command::FunctionLoad { .. }
        | Command::FunctionList
        | Command::FunctionDelete(_)
        | Command::Select(_)
        | Command::FlushDb
        | Command::FlushAll
        | Command::SwapDb(_, _)
        | Command::Publish { .. }
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::PSubscribe(_)
        | Command::PUnsubscribe(_) => Ok(None),
        Command::Eval { keys, .. }
        | Command::EvalSha { keys, .. }
        | Command::FCall { keys, .. }
            if keys.is_empty() =>
        {
            Ok(None)
        }

        Command::Watch(keys)
        | Command::Eval { keys, .. }
        | Command::EvalSha { keys, .. }
        | Command::FCall { keys, .. } => common_slot(keys.iter().map(AsRef::as_ref)).map(Some),
        Command::StreamRead { keys, .. } => {
            common_slot(keys.iter().map(|(key, _)| key.as_ref())).map(Some)
        }

        Command::Del(keys) | Command::Exists(keys) | Command::Touch(keys) => {
            common_slot(keys.iter().map(AsRef::as_ref)).map(Some)
        }

        Command::ListMove {
            source,
            destination,
            ..
        }
        | Command::SortedSetMoveDue {
            source,
            destination,
            ..
        } => common_slot([source.as_ref(), destination.as_ref()].into_iter()).map(Some),
        Command::MGet(keys)
        | Command::JsonMGet { keys, .. }
        | Command::SetInterCard { keys, .. }
        | Command::ListBlock { keys, .. }
        | Command::ListMPop { keys, .. }
        | Command::SortedSetBlockPop { keys, .. } => {
            common_slot(keys.iter().map(AsRef::as_ref)).map(Some)
        }

        Command::PfAdd { key, .. } => Ok(Some(hash_slot(key))),
        Command::PfCount(keys) => common_slot(keys.iter().map(AsRef::as_ref)).map(Some),
        Command::PfMerge {
            destination,
            sources,
        } => common_slot(
            std::iter::once(destination.as_ref()).chain(sources.iter().map(AsRef::as_ref)),
        )
        .map(Some),
        Command::BitOp {
            destination, keys, ..
        } => {
            common_slot(std::iter::once(destination.as_ref()).chain(keys.iter().map(AsRef::as_ref)))
                .map(Some)
        }
        Command::SetAlgebra {
            keys, destination, ..
        }
        | Command::SortedSetAlgebra {
            keys, destination, ..
        } => common_slot(
            destination
                .iter()
                .map(AsRef::as_ref)
                .chain(keys.iter().map(AsRef::as_ref)),
        )
        .map(Some),
        Command::SetMove {
            source,
            destination,
            ..
        } => common_slot([source.as_ref(), destination.as_ref()].into_iter()).map(Some),
        Command::GeoSearch {
            key,
            store: Some(store),
            ..
        } => common_slot([key.as_ref(), store.destination.as_ref()].into_iter()).map(Some),
        Command::MSet(pairs) | Command::MSetNx(pairs) => {
            common_slot(pairs.iter().map(|(key, _)| key.as_ref())).map(Some)
        }
        Command::Transfer {
            source,
            destination,
            ..
        }
        | Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        }
        | Command::Copy {
            source,
            destination,
            ..
        } => common_slot([source.as_ref(), destination.as_ref()].into_iter()).map(Some),
        Command::Lease { key, .. }
        | Command::Semaphore { key, .. }
        | Command::IncrementCapped { key, .. }
        | Command::Release { key, .. }
        | Command::Renew { key, .. }
        | Command::Limit { key, .. }
        | Command::Once { key, .. }
        | Command::IdempotencyBegin { key, .. }
        | Command::IdempotencyComplete { key, .. }
        | Command::IdempotencyAbort { key, .. }
        | Command::Take { key, .. }
        | Command::SetIfEq { key, .. }
        | Command::SetIfCmp { key, .. }
        | Command::DelIfEq { key, .. }
        | Command::ListPushCapped { key, .. }
        | Command::GetEx { key, .. }
        | Command::SetVersioned { key, .. }
        | Command::Get(key)
        | Command::Set(key, _, _)
        | Command::Increment(key, _)
        | Command::GetSet(key, _)
        | Command::Append(key, _)
        | Command::SetNx(key, _)
        | Command::GetDel(key)
        | Command::IncrementFloat(key, _)
        | Command::GetRange { key, .. }
        | Command::SetRange { key, .. }
        | Command::SetBit { key, .. }
        | Command::BitField { key, .. }
        | Command::FillGet { key, .. }
        | Command::FillSet { key, .. }
        | Command::GetBit(key, _)
        | Command::BitCount { key, .. }
        | Command::BitPos { key, .. }
        | Command::StrLen(key)
        | Command::Type(key)
        | Command::ListPush(key, _)
        | Command::ListInsert { key, .. }
        | Command::ListSet { key, .. }
        | Command::ListRemove { key, .. }
        | Command::ListPos { key, .. }
        | Command::ListPushX { key, .. }
        | Command::ListPushBack(key, _)
        | Command::ListPop { key, .. }
        | Command::ListPopBack { key, .. }
        | Command::HashScan { key, .. }
        | Command::CollectionScan { key, .. }
        | Command::ListLen(key)
        | Command::ListIndex(key, _)
        | Command::ListTrim(key, _, _)
        | Command::ListRange(key, _, _)
        | Command::SetAdd(key, _)
        | Command::SetRemove(key, _)
        | Command::SetContains(key, _)
        | Command::SetCard(key)
        | Command::SetMembers(key)
        | Command::SetPop { key, .. }
        | Command::SetRandom { key, .. }
        | Command::HashSet { key, .. }
        | Command::HashMSet { key, .. }
        | Command::HashSetNx { key, .. }
        | Command::HashIncrement { key, .. }
        | Command::HashTransfer { key, .. }
        | Command::HashStrLen { key, .. }
        | Command::HashDelete { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMGet { key, .. }
        | Command::HashLen(key)
        | Command::HashGetAll(key)
        | Command::HashExists { key, .. }
        | Command::HashKeys(key)
        | Command::HashValues(key)
        | Command::SortedSetAdd { key, .. }
        | Command::SortedSetRange { key, .. }
        | Command::SortedSetRangeByScore { key, .. }
        | Command::SortedSetRangeByLex { key, .. }
        | Command::SortedSetRemove { key, .. }
        | Command::SortedSetIncr { key, .. }
        | Command::SortedSetCard(key)
        | Command::SortedSetCount { key, .. }
        | Command::SortedSetLexCount { key, .. }
        | Command::SortedSetMScore { key, .. }
        | Command::SortedSetRandMember { key, .. }
        | Command::HashRandField { key, .. }
        | Command::HashIncrementFloat { key, .. }
        | Command::SetMIsMember { key, .. }
        | Command::SortedSetScore { key, .. }
        | Command::SortedSetRank { key, .. }
        | Command::SortedSetPop { key, .. }
        | Command::SortedSetPopByScore { key, .. }
        | Command::SortedSetRemoveRange { key, .. }
        | Command::HashExpire { key, .. }
        | Command::HashTtl { key, .. }
        | Command::HashPersist { key, .. }
        | Command::GeoAdd { key, .. }
        | Command::GeoDist { key, .. }
        | Command::GeoPos { key, .. }
        | Command::GeoHash { key, .. }
        | Command::GeoSearch { key, .. }
        | Command::BloomReserve(key, ..)
        | Command::BloomAdd(key, _)
        | Command::BloomContains(key, _)
        | Command::BloomAddMany(key, _)
        | Command::BloomContainsMany(key, _)
        | Command::BloomInsert { key, .. }
        | Command::BloomInfo(key, _)
        | Command::BloomCard(key)
        | Command::JsonGet { key, .. }
        | Command::JsonSet { key, .. }
        | Command::JsonSetIf { key, .. }
        | Command::JsonMerge { key, .. }
        | Command::JsonDel { key, .. }
        | Command::JsonOp { key, .. }
        | Command::StreamAdd { key, .. }
        | Command::StreamLen(key)
        | Command::StreamInfo { key, .. }
        | Command::StreamRange { key, .. }
        | Command::StreamDelete { key, .. }
        | Command::StreamTrim { key, .. }
        | Command::StreamTrimMinId { key, .. }
        | Command::StreamSetId { key, .. }
        | Command::StreamGroupCreate { key, .. }
        | Command::StreamGroupRead { key, .. }
        | Command::StreamGroupDestroy { key, .. }
        | Command::StreamGroupSetId { key, .. }
        | Command::StreamGroupDelConsumer { key, .. }
        | Command::StreamGroupCreateConsumer { key, .. }
        | Command::StreamAck { key, .. }
        | Command::StreamPending { key, .. }
        | Command::StreamPendingRange { key, .. }
        | Command::StreamClaimPending { key, .. }
        | Command::StreamAutoclaim { key, .. }
        | Command::Expire { key, .. }
        | Command::PExpireAt { key, .. }
        | Command::Ttl(key)
        | Command::PTtl(key)
        | Command::Persist(key)
        | Command::ExpireTime { key, .. }
        | Command::Object { key, .. }
        | Command::MemoryUsage(key)
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Move { key, .. } => Ok(Some(hash_slot(key))),
    }
}

pub fn command_keys<'a>(command: &'a Command<'_>) -> Vec<&'a [u8]> {
    match command {
        Command::CreateTtl { inner, .. } => command_keys(inner),
        Command::Ping(_)
        | Command::CommandMeta { .. }
        | Command::Wait { .. }
        | Command::Ruvio(_)
        | Command::Changes(_)
        | Command::ChangesSubscribe(_)
        | Command::Hello { .. }
        | Command::ClientGetName
        | Command::ClientSetName(_)
        | Command::ClientTracking(_)
        | Command::ClientDurability { .. }
        | Command::ShardPublish { .. }
        | Command::ShardSubscribe(_)
        | Command::ShardUnsubscribe(_)
        | Command::Publish { .. }
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::PSubscribe(_)
        | Command::PUnsubscribe(_)
        | Command::ClusterSlots
        | Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ClusterKeySlot(_)
        | Command::ReadOnly
        | Command::ReadWrite
        | Command::ConfigGet(_)
        | Command::Info(_)
        | Command::Save
        | Command::ConfigSet(_)
        | Command::ConfigRewrite
        | Command::ReplicaOf { .. }
        | Command::MoveSlots(_)
        | Command::Topology(_)
        | Command::PrimaryLease(_)
        | Command::Migrate(_)
        | Command::Role
        | Command::Bgsave
        | Command::Scan { .. }
        | Command::Keys(_)
        | Command::JsonIndex { .. }
        | Command::RandomKey
        | Command::ClientId
        | Command::ClientSetInfo
        | Command::ClientList { .. }
        | Command::ClientInfo
        | Command::ClientKill { .. }
        | Command::Slowlog(_)
        | Command::LatencyLatest
        | Command::Echo(_)
        | Command::DbSize
        | Command::Auth { .. }
        | Command::Acl(_)
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Unwatch
        | Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill
        | Command::FunctionLoad { .. }
        | Command::FunctionList
        | Command::FunctionDelete(_)
        | Command::Select(_)
        | Command::FlushDb
        | Command::FlushAll
        | Command::SwapDb(_, _) => Vec::new(),
        Command::Del(keys) | Command::Exists(keys) | Command::Touch(keys) => {
            keys.iter().map(AsRef::as_ref).collect()
        }
        Command::Watch(keys)
        | Command::MGet(keys)
        | Command::JsonMGet { keys, .. }
        | Command::SetInterCard { keys, .. }
        | Command::ListBlock { keys, .. }
        | Command::ListMPop { keys, .. }
        | Command::SortedSetBlockPop { keys, .. }
        | Command::Eval { keys, .. }
        | Command::EvalSha { keys, .. }
        | Command::FCall { keys, .. } => keys.iter().map(AsRef::as_ref).collect(),
        Command::StreamRead { keys, .. } => keys.iter().map(|(key, _)| key.as_ref()).collect(),
        Command::PfAdd { key, .. } => vec![key.as_ref()],
        Command::PfCount(keys) => keys.iter().map(AsRef::as_ref).collect(),
        Command::PfMerge {
            destination,
            sources,
        } => std::iter::once(destination.as_ref())
            .chain(sources.iter().map(AsRef::as_ref))
            .collect(),
        Command::BitOp {
            destination, keys, ..
        } => std::iter::once(destination.as_ref())
            .chain(keys.iter().map(AsRef::as_ref))
            .collect(),
        Command::ListMove {
            source,
            destination,
            ..
        }
        | Command::SortedSetMoveDue {
            source,
            destination,
            ..
        } => vec![source.as_ref(), destination.as_ref()],
        Command::SetAlgebra {
            keys, destination, ..
        }
        | Command::SortedSetAlgebra {
            keys, destination, ..
        } => destination
            .iter()
            .map(AsRef::as_ref)
            .chain(keys.iter().map(AsRef::as_ref))
            .collect(),
        Command::SetMove {
            source,
            destination,
            ..
        }
        | Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        }
        | Command::Copy {
            source,
            destination,
            ..
        } => vec![source.as_ref(), destination.as_ref()],
        Command::Transfer {
            source,
            destination,
            ..
        } => vec![source.as_ref(), destination.as_ref()],
        Command::GeoSearch {
            key,
            store: Some(store),
            ..
        } => vec![key.as_ref(), store.destination.as_ref()],
        Command::MSet(pairs) | Command::MSetNx(pairs) => {
            pairs.iter().map(|(key, _)| key.as_ref()).collect()
        }
        Command::Lease { key, .. }
        | Command::Semaphore { key, .. }
        | Command::IncrementCapped { key, .. }
        | Command::Release { key, .. }
        | Command::Renew { key, .. }
        | Command::Limit { key, .. }
        | Command::Once { key, .. }
        | Command::IdempotencyBegin { key, .. }
        | Command::IdempotencyComplete { key, .. }
        | Command::IdempotencyAbort { key, .. }
        | Command::Take { key, .. }
        | Command::SetIfEq { key, .. }
        | Command::SetIfCmp { key, .. }
        | Command::DelIfEq { key, .. }
        | Command::ListPushCapped { key, .. }
        | Command::GetEx { key, .. }
        | Command::SetVersioned { key, .. }
        | Command::Get(key)
        | Command::Set(key, _, _)
        | Command::Increment(key, _)
        | Command::GetSet(key, _)
        | Command::Append(key, _)
        | Command::SetNx(key, _)
        | Command::GetDel(key)
        | Command::IncrementFloat(key, _)
        | Command::GetRange { key, .. }
        | Command::SetRange { key, .. }
        | Command::SetBit { key, .. }
        | Command::BitField { key, .. }
        | Command::FillGet { key, .. }
        | Command::FillSet { key, .. }
        | Command::GetBit(key, _)
        | Command::BitCount { key, .. }
        | Command::BitPos { key, .. }
        | Command::StrLen(key)
        | Command::Type(key)
        | Command::ListPush(key, _)
        | Command::ListInsert { key, .. }
        | Command::ListSet { key, .. }
        | Command::ListRemove { key, .. }
        | Command::ListPos { key, .. }
        | Command::ListPushX { key, .. }
        | Command::ListPushBack(key, _)
        | Command::ListPop { key, .. }
        | Command::ListPopBack { key, .. }
        | Command::HashScan { key, .. }
        | Command::CollectionScan { key, .. }
        | Command::ListLen(key)
        | Command::ListIndex(key, _)
        | Command::ListTrim(key, _, _)
        | Command::ListRange(key, _, _)
        | Command::SetAdd(key, _)
        | Command::SetRemove(key, _)
        | Command::SetContains(key, _)
        | Command::SetCard(key)
        | Command::SetMembers(key)
        | Command::SetPop { key, .. }
        | Command::SetRandom { key, .. }
        | Command::HashSet { key, .. }
        | Command::HashMSet { key, .. }
        | Command::HashSetNx { key, .. }
        | Command::HashIncrement { key, .. }
        | Command::HashTransfer { key, .. }
        | Command::HashStrLen { key, .. }
        | Command::HashDelete { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMGet { key, .. }
        | Command::HashLen(key)
        | Command::HashGetAll(key)
        | Command::HashExists { key, .. }
        | Command::HashKeys(key)
        | Command::HashValues(key)
        | Command::SortedSetAdd { key, .. }
        | Command::SortedSetRange { key, .. }
        | Command::SortedSetRangeByScore { key, .. }
        | Command::SortedSetRangeByLex { key, .. }
        | Command::SortedSetRemove { key, .. }
        | Command::SortedSetIncr { key, .. }
        | Command::SortedSetCard(key)
        | Command::SortedSetCount { key, .. }
        | Command::SortedSetLexCount { key, .. }
        | Command::SortedSetMScore { key, .. }
        | Command::SortedSetRandMember { key, .. }
        | Command::HashRandField { key, .. }
        | Command::HashIncrementFloat { key, .. }
        | Command::SetMIsMember { key, .. }
        | Command::SortedSetScore { key, .. }
        | Command::SortedSetRank { key, .. }
        | Command::SortedSetPop { key, .. }
        | Command::SortedSetPopByScore { key, .. }
        | Command::SortedSetRemoveRange { key, .. }
        | Command::HashExpire { key, .. }
        | Command::HashTtl { key, .. }
        | Command::HashPersist { key, .. }
        | Command::GeoAdd { key, .. }
        | Command::GeoDist { key, .. }
        | Command::GeoPos { key, .. }
        | Command::GeoHash { key, .. }
        | Command::GeoSearch { key, .. }
        | Command::BloomReserve(key, ..)
        | Command::BloomAdd(key, _)
        | Command::BloomContains(key, _)
        | Command::BloomAddMany(key, _)
        | Command::BloomContainsMany(key, _)
        | Command::BloomInsert { key, .. }
        | Command::BloomInfo(key, _)
        | Command::BloomCard(key)
        | Command::JsonGet { key, .. }
        | Command::JsonSet { key, .. }
        | Command::JsonSetIf { key, .. }
        | Command::JsonMerge { key, .. }
        | Command::JsonDel { key, .. }
        | Command::JsonOp { key, .. }
        | Command::StreamAdd { key, .. }
        | Command::StreamLen(key)
        | Command::StreamInfo { key, .. }
        | Command::StreamRange { key, .. }
        | Command::StreamDelete { key, .. }
        | Command::StreamTrim { key, .. }
        | Command::StreamTrimMinId { key, .. }
        | Command::StreamSetId { key, .. }
        | Command::StreamGroupCreate { key, .. }
        | Command::StreamGroupRead { key, .. }
        | Command::StreamGroupDestroy { key, .. }
        | Command::StreamGroupSetId { key, .. }
        | Command::StreamGroupDelConsumer { key, .. }
        | Command::StreamGroupCreateConsumer { key, .. }
        | Command::StreamAck { key, .. }
        | Command::StreamPending { key, .. }
        | Command::StreamPendingRange { key, .. }
        | Command::StreamClaimPending { key, .. }
        | Command::StreamAutoclaim { key, .. }
        | Command::Expire { key, .. }
        | Command::PExpireAt { key, .. }
        | Command::Ttl(key)
        | Command::PTtl(key)
        | Command::Persist(key)
        | Command::ExpireTime { key, .. }
        | Command::Object { key, .. }
        | Command::MemoryUsage(key)
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Move { key, .. } => vec![key.as_ref()],
    }
}

pub fn hot_command_slot(command: &HotCommand<'_>) -> Option<u16> {
    match command {
        HotCommand::Ping(_) => None,
        HotCommand::Get(key)
        | HotCommand::Set(key, _, _)
        | HotCommand::SetEx(key, _, _)
        | HotCommand::Delete(key)
        | HotCommand::Increment(key, _) => Some(hash_slot(key)),
    }
}

pub(crate) fn common_slot<'a>(mut keys: impl Iterator<Item = &'a [u8]>) -> Result<u16, CrossSlot> {
    let first = keys.next().map(hash_slot).ok_or(CrossSlot)?;

    keys.all(|key| hash_slot(key) == first)
        .then_some(first)
        .ok_or(CrossSlot)
}

fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|byte| *byte == b'{') else {
        return key;
    };

    let tagged = &key[open + 1..];
    let Some(close) = tagged.iter().position(|byte| *byte == b'}') else {
        return key;
    };

    if close == 0 { key } else { &tagged[..close] }
}

/// CRC16-XMODEM tables for slice-by-4: `CRC16_TABLES[k][b]` is the CRC of byte `b` followed by
/// `k` zero bytes, so four bytes fold in with four independent lookups.
const CRC16_TABLES: [[u16; 256]; 4] = {
    let mut tables = [[0u16; 256]; 4];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = (byte as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        tables[0][byte] = crc;
        byte += 1;
    }
    let mut k = 1;
    while k < 4 {
        let mut byte = 0;
        while byte < 256 {
            let prev = tables[k - 1][byte];
            tables[k][byte] = (prev << 8) ^ tables[0][(prev >> 8) as usize];
            byte += 1;
        }
        k += 1;
    }
    tables
};

fn crc16(bytes: &[u8]) -> u16 {
    let [t0, t1, t2, t3] = &CRC16_TABLES;
    let mut crc = 0u16;
    let mut chunks = bytes.chunks_exact(4);

    for chunk in &mut chunks {
        let high = usize::from(chunk[0] ^ (crc >> 8) as u8);
        let low = usize::from(chunk[1] ^ crc as u8);
        crc = t3[high] ^ t2[low] ^ t1[usize::from(chunk[2])] ^ t0[usize::from(chunk[3])];
    }

    for byte in chunks.remainder() {
        crc = (crc << 8) ^ t0[usize::from(*byte ^ (crc >> 8) as u8)];
    }

    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitwise_crc16(bytes: &[u8]) -> u16 {
        let mut crc = 0u16;
        for byte in bytes {
            crc ^= u16::from(*byte) << 8;
            for _ in 0..8 {
                crc = if crc & 0x8000 != 0 {
                    (crc << 1) ^ 0x1021
                } else {
                    crc << 1
                };
            }
        }
        crc
    }

    #[test]
    fn crc16_matches_known_xmodem_values() {
        for (input, expected) in [
            (b"".as_slice(), 0x0000),
            (b"A".as_slice(), 0x58E5),
            (b"123456789".as_slice(), 0x31C3),
            (b"user:1000".as_slice(), bitwise_crc16(b"user:1000")),
            (
                b"\xff\xff\xff\xff\xff".as_slice(),
                bitwise_crc16(b"\xff\xff\xff\xff\xff"),
            ),
        ] {
            assert_eq!(crc16(input), expected, "{input:?}");
        }
        assert_eq!(bitwise_crc16(b"A"), 0x58E5);
        assert_eq!(bitwise_crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn table_crc16_equals_bitwise_for_all_lengths() {
        // Fixed-seed xorshift: the same bytes on every run.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut buffer = [0u8; 64];
        for _ in 0..64 {
            for byte in &mut buffer {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            for len in 0..=buffer.len() {
                let bytes = &buffer[..len];
                assert_eq!(crc16(bytes), bitwise_crc16(bytes), "{bytes:?}");
            }
        }
        for byte in 0..=255u8 {
            assert_eq!(crc16(&[byte]), bitwise_crc16(&[byte]));
            assert_eq!(crc16(&[byte; 7]), bitwise_crc16(&[byte; 7]));
        }
    }

    #[test]
    fn hash_tag_edge_cases_hash_like_redis() {
        for (key, hashed) in [
            (b"{}".as_slice(), b"{}".as_slice()),
            (b"{a".as_slice(), b"{a".as_slice()),
            (b"a}{b}".as_slice(), b"b".as_slice()),
            (b"{a}{b}".as_slice(), b"a".as_slice()),
            (b"{{a}}".as_slice(), b"{a".as_slice()),
            (b"}{".as_slice(), b"}{".as_slice()),
        ] {
            assert_eq!(hash_tag(key), hashed, "{key:?}");
            assert_eq!(
                hash_slot(key),
                bitwise_crc16(hashed) % HASH_SLOT_COUNT as u16,
                "{key:?}"
            );
        }
    }

    #[test]
    fn matches_known_redis_slot_and_hash_tags() {
        assert_eq!(hash_slot(b"123456789"), 12_739);
        assert_eq!(hash_slot(b"user:{42}:name"), hash_slot(b"cart:{42}"));
    }

    /// Every client checks its own slot function against the same file.
    #[test]
    fn shared_client_slot_vectors_match_the_server() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../clients/testdata/slots.json")).unwrap();
        let keys = vectors["keys"].as_array().unwrap();

        assert!(keys.len() > 10);

        for entry in keys {
            let key = entry["key"].as_str().unwrap();

            assert_eq!(
                u64::from(hash_slot(key.as_bytes())),
                entry["slot"].as_u64().unwrap(),
                "{key:?}"
            );
        }
    }

    #[test]
    fn rejects_multi_key_commands_across_slots() {
        let command =
            Command::Del(vec![b"{a}:1".as_slice().into(), b"{b}:2".as_slice().into()].into());

        assert_eq!(command_slot(&command), Err(CrossSlot));

        let command =
            Command::Del(vec![b"{a}:1".as_slice().into(), b"{a}:2".as_slice().into()].into());

        assert_eq!(command_slot(&command), Ok(Some(hash_slot(b"a"))));

        let command = Command::MGet(vec![b"{a}:1".as_slice().into(), b"{b}:2".as_slice().into()]);

        assert_eq!(command_slot(&command), Err(CrossSlot));

        let command = Command::MSet(vec![
            (b"{a}:1".as_slice().into(), b"1".as_slice().into()),
            (b"{a}:2".as_slice().into(), b"2".as_slice().into()),
        ]);

        assert_eq!(command_slot(&command), Ok(Some(hash_slot(b"a"))));

        let command = Command::Rename {
            source: b"{a}:1".as_slice().into(),
            destination: b"{b}:2".as_slice().into(),
        };

        assert_eq!(command_slot(&command), Err(CrossSlot));

        let command = Command::SetAlgebra {
            keys: vec![b"{a}:1".as_slice().into(), b"{b}:2".as_slice().into()],
            op: crate::protocol::SetOp::Inter,
            destination: None,
        };

        assert_eq!(command_slot(&command), Err(CrossSlot));
        let command = Command::SetMove {
            source: b"{a}:1".as_slice().into(),
            destination: b"{b}:2".as_slice().into(),
            member: b"m".as_slice().into(),
        };

        assert_eq!(command_slot(&command), Err(CrossSlot));
    }

    #[test]
    fn scripts_without_keys_run_on_the_connection_shard() {
        let frames: [&[u8]; 3] = [
            b"*3\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n",
            b"*3\r\n$7\r\nEVALSHA\r\n$2\r\nab\r\n$1\r\n0\r\n",
            b"*3\r\n$5\r\nFCALL\r\n$3\r\none\r\n$1\r\n0\r\n",
        ];

        for frame in frames {
            let (command, _) = crate::protocol::parse_command(frame).unwrap().unwrap();

            assert_eq!(command_slot(&command), Ok(None));
        }

        let (command, _) = crate::protocol::parse_command(
            b"*5\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n2\r\n$4\r\n{a}1\r\n$4\r\n{b}2\r\n",
        )
        .unwrap()
        .unwrap();

        assert_eq!(command_slot(&command), Err(CrossSlot));
    }

    #[test]
    fn distributes_all_slots_without_exceeding_shard_count() {
        for count in [1, 2, 3, 7, 16, 127, HASH_SLOT_COUNT] {
            let mut next = 0;
            for shard_id in 0..count {
                let (start, end) = slot_range_for_shard(shard_id, count);
                assert_eq!(usize::from(start), next, "count={count} shard={shard_id}");
                assert!(start <= end);
                for slot in start..=end {
                    assert_eq!(shard_for_slot(slot, count), shard_id);
                }
                next = usize::from(end) + 1;
            }
            assert_eq!(next, HASH_SLOT_COUNT);
        }
    }

    #[test]
    fn hash_tags_use_first_nonempty_brace_pair_only() {
        assert_eq!(hash_slot(b"prefix{tag}suffix"), hash_slot(b"tag"));
        assert_eq!(hash_slot(b"{tag}{other}"), hash_slot(b"tag"));
        for key in [
            b"{}tag".as_slice(),
            b"{open".as_slice(),
            b"{} {tag}".as_slice(),
        ] {
            assert_eq!(hash_slot(key), crc16(key) % HASH_SLOT_COUNT as u16);
        }
    }

    fn parse(words: &[&str]) -> Vec<u8> {
        let mut frame = format!("*{}\r\n", words.len()).into_bytes();

        for word in words {
            frame.extend(format!("${}\r\n{}\r\n", word.len(), word).into_bytes());
        }

        frame
    }

    /// Every key-bearing command family reports exactly its keys, and the slot rule agrees with them:
    /// one shared slot is routed, different slots are CROSSSLOT, no keys run on the connection shard.
    #[test]
    fn command_keys_and_slots_agree_for_every_command_family() {
        let cases: &[(&[&str], &[&str])] = &[
            (&["GET", "k"], &["k"]),
            (&["SET", "k", "v"], &["k"]),
            (&["SETNX", "k", "v"], &["k"]),
            (&["GETSET", "k", "v"], &["k"]),
            (&["GETDEL", "k"], &["k"]),
            (&["APPEND", "k", "v"], &["k"]),
            (&["STRLEN", "k"], &["k"]),
            (&["INCR", "k"], &["k"]),
            (&["INCRBY", "k", "2"], &["k"]),
            (&["DECR", "k"], &["k"]),
            (&["INCRBYFLOAT", "k", "1.5"], &["k"]),
            (&["EXPIRE", "k", "10"], &["k"]),
            (&["PEXPIRE", "k", "10"], &["k"]),
            (&["TTL", "k"], &["k"]),
            (&["PTTL", "k"], &["k"]),
            (&["PERSIST", "k"], &["k"]),
            (&["TYPE", "k"], &["k"]),
            (&["EXISTS", "a", "b"], &["a", "b"]),
            (&["DEL", "a", "b"], &["a", "b"]),
            (&["UNLINK", "a", "b"], &["a", "b"]),
            (&["MGET", "a", "b", "c"], &["a", "b", "c"]),
            (&["MSET", "a", "1", "b", "2"], &["a", "b"]),
            (&["RENAME", "a", "b"], &["a", "b"]),
            (&["RENAMENX", "a", "b"], &["a", "b"]),
            (&["COPY", "a", "b"], &["a", "b"]),
            (&["LPUSH", "l", "x"], &["l"]),
            (&["RPUSH", "l", "x", "y"], &["l"]),
            (&["LPOP", "l"], &["l"]),
            (&["RPOP", "l", "2"], &["l"]),
            (&["LLEN", "l"], &["l"]),
            (&["LRANGE", "l", "0", "-1"], &["l"]),
            (&["LINDEX", "l", "0"], &["l"]),
            (&["LSET", "l", "0", "x"], &["l"]),
            (&["LTRIM", "l", "0", "1"], &["l"]),
            (&["LREM", "l", "0", "x"], &["l"]),
            (&["LINSERT", "l", "BEFORE", "p", "x"], &["l"]),
            (&["LMOVE", "a", "b", "LEFT", "RIGHT"], &["a", "b"]),
            (&["BLPOP", "a", "b", "0"], &["a", "b"]),
            (&["BRPOP", "a", "0"], &["a"]),
            (&["SADD", "s", "m"], &["s"]),
            (&["SREM", "s", "m"], &["s"]),
            (&["SMEMBERS", "s"], &["s"]),
            (&["SISMEMBER", "s", "m"], &["s"]),
            (&["SCARD", "s"], &["s"]),
            (&["SPOP", "s"], &["s"]),
            (&["SRANDMEMBER", "s", "2"], &["s"]),
            (&["SINTER", "a", "b"], &["a", "b"]),
            (&["SUNION", "a", "b", "c"], &["a", "b", "c"]),
            (&["SDIFF", "a", "b"], &["a", "b"]),
            (&["SINTERSTORE", "d", "a", "b"], &["d", "a", "b"]),
            (&["SUNIONSTORE", "d", "a"], &["d", "a"]),
            (&["SDIFFSTORE", "d", "a"], &["d", "a"]),
            (&["SMOVE", "a", "b", "m"], &["a", "b"]),
            (&["HSET", "h", "f", "v"], &["h"]),
            (&["HGET", "h", "f"], &["h"]),
            (&["HGETALL", "h"], &["h"]),
            (&["HDEL", "h", "f"], &["h"]),
            (&["HEXISTS", "h", "f"], &["h"]),
            (&["HINCRBY", "h", "f", "1"], &["h"]),
            (&["HLEN", "h"], &["h"]),
            (&["ZADD", "z", "1", "m"], &["z"]),
            (&["ZREM", "z", "m"], &["z"]),
            (&["ZSCORE", "z", "m"], &["z"]),
            (&["ZRANK", "z", "m"], &["z"]),
            (&["ZRANGE", "z", "0", "-1"], &["z"]),
            (&["ZRANGEBYSCORE", "z", "0", "5"], &["z"]),
            (&["ZCARD", "z"], &["z"]),
            (&["ZINCRBY", "z", "1", "m"], &["z"]),
            (&["ZPOPMIN", "z"], &["z"]),
            (&["ZPOPBYSCORE", "z", "10"], &["z"]),
            (&["ZMOVEDUE", "z", "l", "10"], &["z", "l"]),
            (&["XADD", "x", "*", "f", "v"], &["x"]),
            (&["XLEN", "x"], &["x"]),
            (&["XRANGE", "x", "-", "+"], &["x"]),
            (&["XDEL", "x", "1-1"], &["x"]),
            (&["XREAD", "STREAMS", "a", "b", "0", "0"], &["a", "b"]),
            (&["XGROUP", "CREATE", "x", "g", "$"], &["x"]),
            (
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "x", ">"],
                &["x"],
            ),
            (&["XACK", "x", "g", "1-1"], &["x"]),
            (&["PFADD", "p", "a"], &["p"]),
            (&["PFCOUNT", "a", "b"], &["a", "b"]),
            (&["PFMERGE", "d", "a", "b"], &["d", "a", "b"]),
            (&["SETBIT", "b", "1", "1"], &["b"]),
            (&["GETBIT", "b", "1"], &["b"]),
            (&["BITCOUNT", "b"], &["b"]),
            (&["BITOP", "AND", "d", "a", "b"], &["d", "a", "b"]),
            (&["GEOADD", "g", "13.4", "38.1", "m"], &["g"]),
            (&["GEODIST", "g", "a", "b"], &["g"]),
            (&["JSON.SET", "j", "$", "{}"], &["j"]),
            (&["JSON.GET", "j", "$"], &["j"]),
            (&["BF.ADD", "f", "x"], &["f"]),
            (&["BF.EXISTS", "f", "x"], &["f"]),
            (&["EVAL", "return 1", "2", "a", "b"], &["a", "b"]),
            (&["EVALSHA", "ab", "1", "a"], &["a"]),
            (&["FCALL", "fn", "1", "a"], &["a"]),
            (&["PING"], &[]),
            (&["DBSIZE"], &[]),
        ];

        for (words, keys) in cases {
            let frame = parse(words);
            let (command, _) = crate::protocol::parse_command(&frame)
                .unwrap_or_else(|error| panic!("{words:?} did not parse: {error:?}"))
                .unwrap_or_else(|| panic!("{words:?} is incomplete"));
            let reported: Vec<&[u8]> = command_keys(&command);
            let expected: Vec<&[u8]> = keys.iter().map(|key| key.as_bytes()).collect();

            assert_eq!(reported, expected, "{words:?}");

            let slots: std::collections::BTreeSet<u16> =
                expected.iter().map(|key| hash_slot(key)).collect();

            match (expected.is_empty(), slots.len()) {
                (true, _) => assert_eq!(command_slot(&command), Ok(None), "{words:?}"),
                (false, 1) => assert_eq!(
                    command_slot(&command),
                    Ok(slots.first().copied()),
                    "{words:?}"
                ),
                (false, _) => {
                    // Single-key commands never reach this arm; multi-key commands must refuse to split.
                    assert_eq!(command_slot(&command), Err(CrossSlot), "{words:?}");
                }
            }
        }
    }

    /// Hash tags make multi-key commands routable: the same commands with `{t}` keys share one slot.
    #[test]
    fn multi_key_commands_with_a_shared_tag_route_to_that_slot() {
        let slot = hash_slot(b"t");

        for words in [
            vec!["DEL", "{t}a", "{t}b"],
            vec!["MGET", "{t}a", "{t}b"],
            vec!["SUNIONSTORE", "{t}d", "{t}a"],
            vec!["RENAME", "{t}a", "{t}b"],
            vec!["LMOVE", "{t}a", "{t}b", "LEFT", "RIGHT"],
            vec!["PFMERGE", "{t}d", "{t}a"],
            vec!["BITOP", "OR", "{t}d", "{t}a", "{t}b"],
            vec!["EVAL", "return 1", "2", "{t}a", "{t}b"],
        ] {
            let frame = parse(&words);
            let (command, _) = crate::protocol::parse_command(&frame).unwrap().unwrap();

            assert_eq!(command_slot(&command), Ok(Some(slot)), "{words:?}");
        }
    }

    #[test]
    fn slot_table_matches_the_formula_for_every_shard_count() {
        for count in 1..=512 {
            let table = SlotTable::from_formula(count);
            let ranges = table.ranges();

            for slot in 0..HASH_SLOT_COUNT as u16 {
                assert_eq!(
                    table.owner(slot),
                    shard_for_slot(slot, count),
                    "{count} shards"
                );
            }
            assert_eq!(ranges.len(), count.min(HASH_SLOT_COUNT));
            for (first, last, owner) in ranges {
                assert_eq!((first, last), slot_range_for_shard(owner, count));
            }
        }
    }
}
