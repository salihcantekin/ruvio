use super::*;

/// Redis `INFO keyspace` lines, one per non-empty database.
pub fn keyspace_info<C: Clock, I: StorageIndex>(shard: &KvShard<C, I>) -> String {
    let mut out = String::new();

    for (db, keys, expires, avg_ttl) in shard.keyspace() {
        out.push_str(&format!(
            "db{db}:keys={keys},expires={expires},avg_ttl={avg_ttl}\r\n"
        ));
    }

    out
}

pub(super) fn move_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    target: u8,
) -> Result<Response<'a>, S::Error> {
    // A key stored but expired in either database counts as missing; its delete is logged
    // before the move, which replay applies only onto a free target.
    if sink.persists() && shard.write_may_find_expired() {
        shard.reap_expired_in(shard.active_database(), &key);
        shard.reap_expired_in(target, &key);
    }
    // A transaction buffer records only this database's deletes; the target's are logged
    // ahead of the whole batch (see `execute_transaction_in`).
    record_removals(shard, sink)?;

    if !shard.can_move_key(&key, target) {
        return Ok(Response::Integer(0));
    }

    shard.capture_snapshot_key(&key);
    shard.capture_snapshot_key_in(target, &key);

    sink.record(Mutation::MoveKey {
        key: &key,
        destination: target,
    })?;

    let moved = shard.move_key(&key, target);

    debug_assert!(moved, "checked move failed on a single-owner shard");
    Ok(Response::Integer(1))
}

#[inline]
pub(super) fn resolve_set_expiry<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    expiry: SetExpiry,
    now: UnixMillis,
) -> Result<Option<UnixMillis>, Response<'static>> {
    match expiry {
        SetExpiry::None => Ok(None),
        SetExpiry::KeepTtl => Ok(shard.expires_at(key)),
        SetExpiry::RelativeMs(milliseconds) if milliseconds <= 0 => {
            Err(Response::Error(b"ERR invalid expire time in 'set' command"))
        }

        SetExpiry::RelativeMs(milliseconds) => {
            let millis = u64::try_from(milliseconds)
                .map_err(|_| Response::Error(b"ERR invalid expire time in 'set' command"))?;

            now.checked_add(millis)
                .map(Some)
                .ok_or(Response::Error(b"ERR invalid expire time in 'set' command"))
        }

        SetExpiry::AbsoluteMs(timestamp) => match u64::try_from(timestamp) {
            Ok(millis) => Ok(Some(UnixMillis::new(millis))),
            Err(_) => Err(Response::Error(b"ERR invalid expire time in 'set' command")),
        },
    }
}

pub(super) fn expire_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    timestamp: i64,
    absolute: bool,
    condition: crate::protocol::ExpireCondition,
) -> Result<Response<'a>, S::Error> {
    if !shard.contains_key(&key) {
        return Ok(Response::Integer(0));
    }

    let now = shard.now_unix_millis();
    let delete = if absolute {
        u64::try_from(timestamp)
            .ok()
            .is_none_or(|deadline| deadline <= now.get())
    } else {
        timestamp <= 0
    };

    if condition != crate::protocol::ExpireCondition::Always {
        // A deadline already past counts as the earliest one; a key without one as infinite.
        let target = if delete {
            0
        } else if absolute {
            u64::try_from(timestamp).unwrap_or(0)
        } else {
            now.get()
                .saturating_add(u64::try_from(timestamp).unwrap_or(0))
        };

        if !expire_condition_holds(condition, shard.expires_at(&key), target) {
            return Ok(Response::Integer(0));
        }
    }

    if delete {
        // A deadline already past deletes the key, which a full store still allows.
        before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

        let keys = [key];

        sink.record(Mutation::Delete { keys: &keys })?;

        shard.delete(&keys[0]);

        return Ok(Response::Integer(1));
    }

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], 0)? {
        return Ok(rejected);
    }

    let deadline = if absolute {
        crate::UnixMillis::new(u64::try_from(timestamp).expect("future deadline checked"))
    } else {
        let millis = u64::try_from(timestamp).expect("positive deadline checked");
        let Some(deadline) = now.checked_add(millis) else {
            return Ok(Response::Error(b"ERR invalid expire time"));
        };

        deadline
    };

    sink.record(Mutation::ExpireAt {
        key: &key,
        unix_millis: deadline.get(),
    })?;

    shard.expire_at(&key, deadline);
    Ok(Response::Integer(1))
}

fn expire_condition_holds(
    condition: crate::protocol::ExpireCondition,
    current: Option<crate::UnixMillis>,
    target: u64,
) -> bool {
    use crate::protocol::ExpireCondition;

    match (condition, current) {
        (ExpireCondition::Always, _) => true,
        (ExpireCondition::Nx, current) => current.is_none(),
        (ExpireCondition::Xx, current) => current.is_some(),
        (ExpireCondition::Gt, current) => current.is_some_and(|at| target > at.get()),
        (ExpireCondition::Lt, current) => current.is_none_or(|at| target < at.get()),
    }
}

pub(super) fn persist_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if shard.expires_at(&key).is_none() {
        return Ok(Response::Integer(0));
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    sink.record(Mutation::Persist { key: &key })?;

    Ok(Response::Integer(i64::from(shard.persist(&key))))
}

pub(super) fn rename_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if !shard.contains_key(&source) {
        return Ok(Response::Error(b"ERR no such key"));
    }

    if source == destination {
        return Ok(Response::Simple(b"OK"));
    }

    let extra = if shard.contains_key(&destination) {
        0
    } else {
        destination.len() + ADMISSION_OVERHEAD_BYTES
    };

    before_mutation_allow_oom(shard, sink, [source.as_ref(), destination.as_ref()], extra)?;

    sink.record(Mutation::Rename {
        source: &source,
        destination: &destination,
    })?;

    shard.rename(&source, &destination);
    Ok(Response::Simple(b"OK"))
}

pub(super) fn scan_keys<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    cursor: u64,
    pattern: Option<&[u8]>,
    count: usize,
    type_name: Option<&[u8]>,
    slot: Option<u16>,
) -> Response<'a> {
    let kind = match type_name {
        None => None,
        Some(name) => match ValueKind::from_type_name(name) {
            Some(kind) => Some(kind),
            None => return Response::Error(b"ERR syntax error"),
        },
    };

    let (cursor, keys) = shard.scan(cursor, count, pattern, kind);

    // `SLOT` filters the same walk: `COUNT` still bounds the work, a page may come back
    // shorter or empty, and the cursor ends the walk at 0 as usual.
    Response::Scan {
        cursor,
        keys: keys
            .into_iter()
            .filter(|key| slot.is_none_or(|slot| crate::routing::hash_slot(key) == slot))
            .map(Cow::Borrowed)
            .collect(),
    }
}

pub(super) fn type_name(kind: Option<ValueKind>) -> &'static [u8] {
    kind.map_or(b"none", ValueKind::type_name)
}

/// `SSCAN` and `ZSCAN`: one page, with scores interleaved for sorted sets.
pub(super) fn collection_scan<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    kind: crate::protocol::CollectionScanKind,
    key: &[u8],
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
) -> Response<'a> {
    use crate::protocol::CollectionScanKind;

    match kind {
        CollectionScanKind::Set => match shard.set_scan(key, cursor, count, pattern) {
            Ok((cursor, members)) => Response::Scan {
                cursor,
                keys: members,
            },
            Err(_) => wrong_type(),
        },
        CollectionScanKind::SortedSet => match shard.sorted_set_scan(key, cursor, count, pattern) {
            Ok((cursor, entries)) => {
                let mut keys = Vec::with_capacity(entries.len().saturating_mul(2));

                for (member, score) in entries {
                    keys.push(Cow::Borrowed(member));
                    keys.push(Cow::Owned(super::response::format_score(score)));
                }

                Response::Scan { cursor, keys }
            }
            Err(_) => wrong_type(),
        },
    }
}
