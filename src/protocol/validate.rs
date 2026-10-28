use super::*;

pub(super) fn validate_key(key: &[u8], limits: ProtocolLimits) -> Result<(), ProtocolError> {
    if key.len() > limits.max_key_bytes {
        return Err(ProtocolError::LimitExceeded("key is too large"));
    }

    Ok(())
}

pub(super) fn validate_value(value: &[u8], limits: ProtocolLimits) -> Result<(), ProtocolError> {
    if value.len() > limits.max_value_bytes {
        return Err(ProtocolError::LimitExceeded("value is too large"));
    }

    Ok(())
}

pub(super) fn validate_values(
    values: &[Bytes<'_>],
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    for value in values {
        validate_value(value, limits)?;
    }

    Ok(())
}

pub(super) fn validate_keys(
    keys: &[Bytes<'_>],
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    for key in keys {
        validate_key(key, limits)?;
    }

    Ok(())
}

pub(super) fn validate_command_payloads(
    command: &Command<'_>,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    match command {
        Command::CreateTtl { inner, .. } => validate_command_payloads(inner, limits)?,
        Command::Ping(message) | Command::Ruvio(message) | Command::Info(message) => {
            if let Some(message) = message {
                validate_value(message, limits)?;
            }
        }

        Command::CommandMeta { names, .. } => {
            for name in names {
                validate_value(name, limits)?;
            }
        }

        Command::ClusterKeySlot(key) => validate_key(key, limits)?,
        Command::ClusterSlots
        | Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ReadOnly
        | Command::ReadWrite
        | Command::Wait { .. }
        | Command::Save
        | Command::ConfigRewrite
        | Command::ReplicaOf { .. }
        | Command::MoveSlots(_)
        | Command::Topology(_)
        | Command::PrimaryLease(_)
        | Command::Migrate(_)
        | Command::Role
        | Command::Bgsave
        | Command::DbSize
        | Command::RandomKey
        | Command::ClientId
        | Command::ClientSetInfo
        | Command::ClientList { .. }
        | Command::ClientInfo
        | Command::Slowlog(_)
        | Command::LatencyLatest
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Unwatch
        | Command::Select(_)
        | Command::FlushDb
        | Command::FlushAll
        | Command::SwapDb(_, _)
        | Command::Changes(_)
        | Command::ChangesSubscribe(_) => {}
        Command::Watch(keys) => validate_keys(keys, limits)?,
        Command::IdempotencyBegin {
            key,
            fingerprint,
            owner,
            ..
        }
        | Command::IdempotencyComplete {
            key,
            fingerprint,
            owner,
            ..
        }
        | Command::IdempotencyAbort {
            key,
            fingerprint,
            owner,
        } => {
            validate_key(key, limits)?;
            validate_value(fingerprint, limits)?;
            validate_value(owner, limits)?;
            let mut stored_bytes = 11 + fingerprint.len() + owner.len();
            if let Command::IdempotencyComplete { result, .. } = command {
                validate_value(result, limits)?;
                stored_bytes += result.len();
            }
            if stored_bytes > limits.max_value_bytes {
                return Err(ProtocolError::LimitExceeded(
                    "idempotency record is too large",
                ));
            }
        }
        Command::Move { key, .. } => validate_key(key, limits)?,
        Command::ConfigGet(pattern) => validate_value(pattern, limits)?,
        Command::ConfigSet(pairs) => {
            for (name, value) in pairs {
                validate_value(name, limits)?;
                validate_value(value, limits)?;
            }
        }
        Command::JsonGet { key, path } | Command::JsonDel { key, path } => {
            validate_key(key, limits)?;
            validate_value(path, limits)?;
        }
        Command::JsonIndex { args, .. } => {
            for arg in args {
                validate_value(arg, limits)?;
            }
        }
        Command::JsonOp {
            key, path, args, ..
        } => {
            validate_key(key, limits)?;
            validate_value(path, limits)?;
            for arg in args {
                validate_value(arg, limits)?;
            }
        }
        Command::JsonSet { key, path, value }
        | Command::JsonSetIf {
            key, path, value, ..
        }
        | Command::JsonMerge { key, path, value } => {
            validate_key(key, limits)?;
            validate_value(path, limits)?;
            validate_value(value, limits)?;
        }
        Command::Get(key)
        | Command::Lease { key, .. }
        | Command::Semaphore { key, .. }
        | Command::IncrementCapped { key, .. }
        | Command::Release { key, .. }
        | Command::Renew { key, .. }
        | Command::Limit { key, .. }
        | Command::Once { key, .. }
        | Command::Take { key, .. }
        | Command::DelIfEq { key, .. }
        | Command::GetEx { key, .. }
        | Command::ListPop { key, .. }
        | Command::ListPopBack { key, .. }
        | Command::ListLen(key)
        | Command::HashScan { key, .. }
        | Command::CollectionScan { key, .. }
        | Command::BloomReserve(key, ..)
        | Command::BloomInfo(key, _)
        | Command::BloomCard(key) => validate_key(key, limits)?,
        Command::FillSet { key, value, .. } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
        }
        Command::Set(key, value, _)
        | Command::SetVersioned { key, value, .. }
        | Command::GetSet(key, value)
        | Command::Append(key, value)
        | Command::SetNx(key, value)
        | Command::SetContains(key, value)
        | Command::HashGet { key, field: value }
        | Command::HashExists { key, field: value }
        | Command::BloomAdd(key, value)
        | Command::BloomContains(key, value) => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
        }

        Command::StreamLen(key)
        | Command::StreamRange { key, .. }
        | Command::StreamTrim { key, .. }
        | Command::StreamTrimMinId { key, .. }
        | Command::StreamSetId { key, .. }
        | Command::StreamInfo { key, .. } => validate_key(key, limits)?,
        Command::StreamRead { keys, .. } => {
            for (key, _) in keys {
                validate_key(key, limits)?;
            }
        }

        Command::StreamDelete { key, .. } => validate_key(key, limits)?,
        Command::StreamGroupCreate { key, group, .. }
        | Command::StreamGroupDestroy { key, group, .. }
        | Command::StreamGroupSetId { key, group, .. }
        | Command::StreamAck { key, group, .. }
        | Command::StreamPending { key, group, .. } => {
            validate_key(key, limits)?;
            validate_value(group, limits)?;
        }

        Command::StreamPendingRange {
            key,
            group,
            consumer,
            ..
        } => {
            validate_key(key, limits)?;
            validate_value(group, limits)?;

            if let Some(consumer) = consumer {
                validate_value(consumer, limits)?;
            }
        }

        Command::StreamGroupRead {
            key,
            group,
            consumer,
            ..
        }
        | Command::StreamGroupCreateConsumer {
            key,
            group,
            consumer,
            ..
        }
        | Command::StreamGroupDelConsumer {
            key,
            group,
            consumer,
            ..
        }
        | Command::StreamClaimPending {
            key,
            group,
            consumer,
            ..
        }
        | Command::StreamAutoclaim {
            key,
            group,
            consumer,
            ..
        } => {
            validate_key(key, limits)?;
            validate_value(group, limits)?;
            validate_value(consumer, limits)?;
        }

        Command::StreamAdd { key, fields, .. } => {
            validate_key(key, limits)?;

            for (field, value) in fields {
                validate_value(field, limits)?;
                validate_value(value, limits)?;
            }
        }

        Command::Increment(key, _)
        | Command::StrLen(key)
        | Command::Type(key)
        | Command::SetCard(key)
        | Command::SetMembers(key)
        | Command::SetPop { key, count: None }
        | Command::SetRandom { key, count: None }
        | Command::HashLen(key)
        | Command::HashGetAll(key)
        | Command::HashKeys(key)
        | Command::HashValues(key)
        | Command::SortedSetCard(key)
        | Command::SortedSetCount { key, .. }
        | Command::SortedSetLexCount { key, .. }
        | Command::SortedSetRandMember { key, .. }
        | Command::HashRandField { key, .. }
        | Command::Ttl(key)
        | Command::PTtl(key)
        | Command::Persist(key)
        | Command::ExpireTime { key, .. }
        | Command::Object { key, .. }
        | Command::MemoryUsage(key)
        | Command::Dump(key)
        | Command::Expire { key, .. }
        | Command::PExpireAt { key, .. } => validate_key(key, limits)?,
        Command::Rename {
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
        }
        | Command::SetMove {
            source,
            destination,
            ..
        } => {
            validate_key(source, limits)?;
            validate_key(destination, limits)?;

            if let Command::SetMove { member, .. } = command {
                validate_value(member, limits)?;
            }
        }

        Command::SetMIsMember { key, members } | Command::SortedSetMScore { key, members } => {
            validate_key(key, limits)?;

            for member in members {
                validate_value(member, limits)?;
            }
        }
        Command::SetInterCard { keys, .. } => validate_keys(keys, limits)?,
        Command::HashIncrementFloat { key, field, .. } => {
            validate_key(key, limits)?;
            validate_value(field, limits)?;
        }
        Command::SortedSetAlgebra {
            keys, destination, ..
        }
        | Command::SetAlgebra {
            keys, destination, ..
        } => {
            validate_keys(keys, limits)?;

            if let Some(destination) = destination {
                validate_key(destination, limits)?;
            }
        }

        Command::Scan {
            pattern, type_name, ..
        } => {
            if let Some(pattern) = pattern {
                validate_value(pattern, limits)?;
            }

            if let Some(type_name) = type_name {
                validate_value(type_name, limits)?;
            }
        }

        Command::Restore { key, payload, .. } => {
            validate_key(key, limits)?;
            validate_value(payload, limits)?;
        }
        Command::ClientKill { addr, .. } => {
            if let Some(addr) = addr {
                validate_value(addr, limits)?;
            }
        }
        Command::Keys(pattern) | Command::Echo(pattern) => validate_value(pattern, limits)?,
        Command::MGet(keys) => validate_keys(keys, limits)?,
        Command::JsonMGet { keys, path } => {
            validate_keys(keys, limits)?;
            validate_value(path, limits)?;
        }
        Command::Exists(keys) | Command::Touch(keys) => validate_keys(keys, limits)?,
        Command::GetDel(key) | Command::IncrementFloat(key, _) | Command::GetRange { key, .. } => {
            validate_key(key, limits)?
        }
        Command::SetRange { key, value, .. } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
        }
        Command::MSet(pairs) | Command::MSetNx(pairs) => {
            for (key, value) in pairs {
                validate_key(key, limits)?;
                validate_value(value, limits)?;
            }
        }

        Command::HashSet { key, pairs } | Command::HashMSet { key, pairs } => {
            validate_key(key, limits)?;

            for (field, value) in pairs {
                validate_value(field, limits)?;
                validate_value(value, limits)?;
            }
        }

        Command::Auth { username, password } => {
            if let Some(username) = username {
                validate_value(username, limits)?;
            }

            validate_value(password, limits)?;
        }

        Command::Acl(AclCommand::GetUser(name)) => validate_value(name, limits)?,
        Command::Acl(AclCommand::Cat(Some(name))) => validate_value(name, limits)?,
        Command::Acl(AclCommand::SetUser { name, rules }) => {
            validate_value(name, limits)?;

            for rule in rules {
                validate_value(rule, limits)?;
            }
        }

        Command::Acl(_) => {}
        Command::Del(keys) => validate_keys(keys, limits)?,
        Command::SetBit { key, .. }
        | Command::BitField { key, .. }
        | Command::FillGet { key, .. }
        | Command::GetBit(key, _)
        | Command::BitCount { key, .. }
        | Command::BitPos { key, .. } => validate_key(key, limits)?,
        Command::PfAdd { key, elements } => {
            validate_key(key, limits)?;
            validate_values(elements, limits)?;
        }
        Command::PfCount(keys) => validate_keys(keys, limits)?,
        Command::PfMerge {
            destination,
            sources,
        } => {
            validate_key(destination, limits)?;
            validate_keys(sources, limits)?;
        }
        Command::BitOp {
            destination, keys, ..
        } => {
            validate_key(destination, limits)?;
            validate_keys(keys, limits)?;
        }
        Command::ListBlock { keys, .. } | Command::ListMPop { keys, .. } => {
            validate_keys(keys, limits)?
        }
        Command::ListInsert {
            key, pivot, value, ..
        } => {
            validate_key(key, limits)?;
            validate_value(pivot, limits)?;
            validate_value(value, limits)?;
        }
        Command::ListSet { key, value, .. }
        | Command::ListRemove { key, value, .. }
        | Command::ListPos {
            key,
            element: value,
            ..
        } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
        }
        Command::ListMove {
            source,
            destination,
            ..
        } => {
            validate_key(source, limits)?;
            validate_key(destination, limits)?;
        }
        Command::SetIfEq {
            key,
            value,
            expected,
            ..
        } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
            validate_value(expected, limits)?;
        }
        Command::SetIfCmp { key, value, .. } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
        }
        Command::BloomInsert { key, items, .. } => {
            validate_key(key, limits)?;
            validate_values(items, limits)?;
        }
        Command::ListPushX { key, values, .. } | Command::ListPushCapped { key, values, .. } => {
            validate_key(key, limits)?;
            validate_values(values, limits)?;
        }
        Command::ListPush(key, values) | Command::ListPushBack(key, values) => {
            validate_key(key, limits)?;
            validate_values(values, limits)?;
        }
        Command::BloomAddMany(key, values)
        | Command::BloomContainsMany(key, values)
        | Command::SetAdd(key, values)
        | Command::SetRemove(key, values)
        | Command::HashDelete {
            key,
            fields: values,
        }
        | Command::HashMGet {
            key,
            fields: values,
        } => {
            validate_key(key, limits)?;
            validate_values(values, limits)?;
        }

        Command::ListRange(key, _, _)
        | Command::ListIndex(key, _)
        | Command::ListTrim(key, _, _)
        | Command::SortedSetRange { key, .. }
        | Command::SortedSetRangeByScore { key, .. }
        | Command::SortedSetRangeByLex { key, .. } => {
            validate_key(key, limits)?;
        }

        Command::SetPop {
            key,
            count: Some(_),
        }
        | Command::SetRandom {
            key,
            count: Some(_),
        } => validate_key(key, limits)?,
        Command::HashSetNx { key, field, value } => {
            validate_key(key, limits)?;
            validate_value(field, limits)?;
            validate_value(value, limits)?;
        }

        Command::HashIncrement { key, field, .. } | Command::HashStrLen { key, field } => {
            validate_key(key, limits)?;
            validate_value(field, limits)?;
        }

        Command::HashTransfer { key, from, to, .. } => {
            validate_key(key, limits)?;
            validate_value(from, limits)?;
            validate_value(to, limits)?;
        }

        Command::Transfer {
            source,
            destination,
            ..
        } => {
            validate_key(source, limits)?;
            validate_key(destination, limits)?;
        }

        Command::SortedSetAdd { key, pairs, .. } => {
            validate_key(key, limits)?;

            for (_, member) in pairs {
                validate_value(member, limits)?;
            }
        }

        Command::SortedSetRemove { key, members } => {
            validate_key(key, limits)?;
            validate_values(members, limits)?;
        }

        Command::HashExpire { key, fields, .. }
        | Command::HashTtl { key, fields, .. }
        | Command::HashPersist { key, fields } => {
            validate_key(key, limits)?;
            validate_values(fields, limits)?;
        }

        Command::SortedSetPop { key, .. } | Command::SortedSetPopByScore { key, .. } => {
            validate_key(key, limits)?
        }
        Command::SortedSetMoveDue {
            source,
            destination,
            ..
        } => {
            validate_key(source, limits)?;
            validate_key(destination, limits)?;
        }
        Command::SortedSetBlockPop { keys, .. } => validate_keys(keys, limits)?,
        Command::SortedSetRemoveRange { key, range } => {
            validate_key(key, limits)?;

            if let SortedRemoveRange::Lex(min, max) = range {
                for bound in [min, max] {
                    if let LexBound::Inclusive(member) | LexBound::Exclusive(member) = bound {
                        validate_value(member, limits)?;
                    }
                }
            }
        }

        Command::SortedSetIncr { key, member, .. }
        | Command::SortedSetScore { key, member }
        | Command::SortedSetRank { key, member, .. } => {
            validate_key(key, limits)?;
            validate_value(member, limits)?;
        }

        Command::GeoAdd { key, members, .. } => {
            validate_key(key, limits)?;

            for (_, _, member) in members {
                validate_value(member, limits)?;
            }
        }

        Command::GeoDist { key, from, to, .. } => {
            validate_key(key, limits)?;
            validate_value(from, limits)?;
            validate_value(to, limits)?;
        }

        Command::GeoPos { key, members } | Command::GeoHash { key, members } => {
            validate_key(key, limits)?;
            validate_values(members, limits)?;
        }

        Command::GeoSearch {
            key, center, store, ..
        } => {
            validate_key(key, limits)?;

            if let GeoCenter::Member(member) = center {
                validate_value(member, limits)?;
            }

            if let Some(store) = store {
                validate_key(&store.destination, limits)?;
            }
        }

        Command::Eval { script, keys, args } => {
            validate_value(script, limits)?;
            validate_keys(keys, limits)?;
            validate_values(args, limits)?;
        }

        Command::EvalSha { sha, keys, args } => {
            validate_value(sha, limits)?;
            validate_keys(keys, limits)?;
            validate_values(args, limits)?;
        }

        Command::ScriptLoad(script) => validate_value(script, limits)?,
        Command::ScriptExists(shas) => validate_values(shas, limits)?,
        Command::FunctionLoad { name, body } => {
            validate_key(name, limits)?;
            validate_value(body, limits)?;
        }

        Command::FunctionDelete(name) => validate_key(name, limits)?,
        Command::FCall { name, keys, args } => {
            validate_key(name, limits)?;
            validate_keys(keys, limits)?;
            validate_values(args, limits)?;
        }

        Command::ScriptFlush
        | Command::ScriptKill
        | Command::FunctionList
        | Command::ClientGetName
        | Command::ClientTracking(_)
        | Command::ClientDurability { .. } => {}
        Command::Hello {
            client_name, auth, ..
        } => {
            if let Some(name) = client_name {
                validate_value(name, limits)?;
            }

            if let Some((username, password)) = auth {
                validate_value(username, limits)?;
                validate_value(password, limits)?;
            }
        }

        Command::ClientSetName(name) => validate_value(name, limits)?,
        Command::ShardPublish { channel, message } | Command::Publish { channel, message } => {
            validate_key(channel, limits)?;
            validate_value(message, limits)?;
        }

        Command::ShardSubscribe(channels)
        | Command::ShardUnsubscribe(channels)
        | Command::Subscribe(channels)
        | Command::Unsubscribe(channels)
        | Command::PSubscribe(channels)
        | Command::PUnsubscribe(channels) => {
            validate_keys(channels, limits)?;
        }
    }

    Ok(())
}
