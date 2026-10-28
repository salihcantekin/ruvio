//! The command entry points: `execute*` and the per-command dispatch of `execute_in_database`.

use super::*;

pub fn execute<'a, C: Clock, I: StorageIndex>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
) -> Response<'a> {
    let mut sink = NoopMutationSink;

    match execute_with_sink(shard, command, &mut sink) {
        Ok(response) => response,
        Err(error) => match error {},
    }
}

pub fn execute_with_sink<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
) -> Result<Response<'a>, S::Error> {
    execute_with_waiters(shard, command, sink, &mut ())
}

pub fn execute_with_waiters<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
    waiters: &mut W,
) -> Result<Response<'a>, S::Error> {
    execute_in_database(shard, command, sink, waiters, 0)
}

/// Counts every key a read names as accessed (see `execute_in_database`).
#[inline(never)]
fn touch_read_keys<C: Clock, I: StorageIndex>(shard: &mut KvShard<C, I>, command: &Command<'_>) {
    for key in crate::routing::command_keys(command) {
        shard.touch(key);
    }
}

pub fn execute_in_database<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    command: Command<'a>,
    sink: &mut S,
    waiters: &mut W,
    database: u8,
) -> Result<Response<'a>, S::Error> {
    shard.expire_due();
    shard.enter(database);
    sink.select_database(database);
    // Best effort: a read must not fail on the log; a write retries in `before_mutation`.
    let _ = record_removals(shard, sink);
    // The one place a read counts as an access for LRU/LFU: writes count through the
    // store's change path, so no command lists its own keys for either.
    if shard.eviction_policy().tracks_recency() && !crate::protocol::command_mutates_store(&command)
    {
        touch_read_keys(shard, &command);
    }
    match command {
        Command::Wait { .. } => Ok(Response::Integer(0)),
        Command::CommandMeta { kind, names } => Ok(command_meta::respond(kind, &names)),
        Command::Ping(message) => match message {
            Some(message) => Ok(Response::Bulk(Some(message))),
            None => Ok(Response::Simple(b"PONG")),
        },
        Command::Ruvio(message) => match message {
            None => Ok(Response::Simple(b"PONG is taken")),
            Some(word) if word.as_ref().eq_ignore_ascii_case(b"owner") => Ok(Response::Simple(
                b"the core that owns the slot does the work",
            )),

            Some(_) => Ok(Response::Error(
                b"ERR RUVIO expects zero arguments or OWNER",
            )),
        },
        Command::Auth { .. } => Ok(Response::Error(
            b"ERR AUTH <password> called without any password configured",
        )),
        Command::Acl(_) => Ok(Response::Error(b"ERR ACL command requires a connection")),
        Command::ClusterSlots => Ok(Response::Error(b"ERR cluster mode is disabled")),
        Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ReadOnly
        | Command::ReadWrite => Ok(Response::Error(b"ERR cluster mode is disabled")),
        Command::ClusterKeySlot(key) => Ok(Response::Integer(i64::from(
            crate::routing::hash_slot(key.as_ref()),
        ))),
        Command::ConfigGet(pattern) => Ok(config_get_response(&AppConfig::default(), &pattern)),
        Command::Info(_) => Ok(Response::Bulk(Some(Cow::Borrowed(
            b"# Server\r\nruvio_backend:portable\r\n",
        )))),
        Command::Save | Command::Bgsave => {
            Ok(Response::Error(b"ERR SAVE is handled by the server"))
        }
        Command::ConfigSet(_) | Command::ConfigRewrite => {
            Ok(Response::Error(b"ERR CONFIG SET is handled by the server"))
        }
        Command::ReplicaOf { .. } | Command::Role => Ok(Response::Error(
            b"ERR REPLICAOF and ROLE are handled by the server",
        )),
        Command::MoveSlots(_) => Ok(Response::Error(
            b"ERR RUVIO MOVESLOTS needs more than one shard",
        )),
        Command::Topology(_) => Ok(Response::Error(
            b"ERR RUVIO TOPOLOGY is handled by the server",
        )),
        Command::PrimaryLease(_) => {
            Ok(Response::Error(b"ERR RUVIO LEASE is handled by the server"))
        }
        Command::Migrate(_) => Ok(Response::Error(
            b"ERR RUVIO MIGRATE is handled by the server",
        )),

        Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch(_)
        | Command::Unwatch => Ok(Response::Error(
            b"ERR transaction command requires a connection",
        )),
        Command::Eval { .. }
        | Command::EvalSha { .. }
        | Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill
        | Command::FunctionLoad { .. }
        | Command::FunctionList
        | Command::FunctionDelete(_)
        | Command::FCall { .. } => Ok(Response::Error(b"ERR script command requires a connection")),
        Command::Hello { .. }
        | Command::ClientGetName
        | Command::ClientId
        | Command::ClientSetInfo
        | Command::ClientList { .. }
        | Command::ClientInfo
        | Command::ClientKill { .. }
        | Command::Slowlog(_)
        | Command::LatencyLatest
        | Command::ClientSetName(_)
        | Command::ClientTracking(_)
        | Command::ClientDurability { .. }
        | Command::Select(_) => Ok(Response::Error(b"ERR command is handled by the connection")),

        Command::ShardPublish { .. }
        | Command::ShardSubscribe(_)
        | Command::ShardUnsubscribe(_)
        | Command::Publish { .. }
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::PSubscribe(_)
        | Command::PUnsubscribe(_) => {
            Ok(Response::Error(b"ERR pubsub is handled by the connection"))
        }

        Command::Get(key) => match shard.value_view(&key) {
            Ok(value) => Ok(Response::Bulk(value.map(byte_view_to_cow))),
            Err(_) => Ok(wrong_type()),
        },

        Command::JsonGet { key, path } => {
            if let Some(tree) = shard.json_tree(&key) {
                if path.as_ref() == b"$" {
                    return Ok(match tree.compact_view() {
                        Some(bytes) => Response::Bulk(Some(Cow::Borrowed(bytes))),
                        None => Response::JsonTree(tree),
                    });
                }
                return match tree.get(&path) {
                    Ok(value) => Ok(Response::Bulk(value.map(Cow::Owned))),
                    Err(crate::json::JsonError::MissingRoot) => Ok(Response::Bulk(None)),
                    Err(_) => Ok(Response::Error(b"ERR invalid JSON path")),
                };
            }
            match shard.json_view(&key) {
                Ok(None) if crate::json::validate_path(&path).is_err() => {
                    Ok(Response::Error(b"ERR invalid JSON path"))
                }
                Ok(None) => Ok(Response::Bulk(None)),
                Ok(Some(document)) if path.as_ref() == b"$" => {
                    Ok(Response::Bulk(Some(Cow::Borrowed(document))))
                }
                Ok(Some(document)) => match crate::json::get(document, &path) {
                    Ok(result) => Ok(Response::Bulk(result.map(Cow::Owned))),
                    Err(_) => Ok(Response::Error(b"ERR invalid JSON path")),
                },
                Err(_) => Ok(wrong_type()),
            }
        }
        Command::JsonMGet { keys, path } => {
            let mut values = Vec::with_capacity(keys.len());

            for key in &keys {
                values.push(match json_value(shard, key, &path) {
                    Ok(value) => value.map(Cow::Owned),
                    Err(rejected) if rejected == Response::Error(b"ERR invalid JSON path") => {
                        return Ok(rejected);
                    }
                    Err(_) => None,
                });
            }

            Ok(Response::Array(values))
        }
        Command::JsonIndex { op, args } => json_index_cmd::run(shard, sink, database, op, &args),
        Command::JsonOp {
            op,
            key,
            path,
            args,
        } => json_op(shard, sink, waiters, database, op, key, path, args),

        Command::JsonSetIf {
            key,
            path,
            value,
            must_exist,
        } => {
            let exists = match json_value(shard, &key, &path) {
                Ok(current) => current.is_some(),
                Err(response) => return Ok(response),
            };
            if exists != must_exist {
                return Ok(Response::Bulk(None));
            }
            execute_in_database(
                shard,
                Command::JsonSet { key, path, value },
                sink,
                waiters,
                database,
            )
        }
        Command::JsonSet { key, path, value } => {
            json_set_at(shard, sink, key, path, value, false, OnOom::Refuse)
        }
        Command::JsonMerge { key, path, value } => json_merge(shard, sink, key, path, value),
        Command::JsonDel { key, path } => json_del(shard, sink, key, path),
        Command::Set(key, value, options) => set_key(shard, sink, key, value, options),
        Command::IdempotencyBegin {
            key,
            fingerprint,
            owner,
            ttl_ms,
        } => idempotency::begin(shard, sink, &key, &fingerprint, &owner, ttl_ms),
        Command::IdempotencyComplete {
            key,
            fingerprint,
            owner,
            result,
        } => idempotency::complete(shard, sink, &key, &fingerprint, &owner, &result),
        Command::IdempotencyAbort {
            key,
            fingerprint,
            owner,
        } => idempotency::abort(shard, sink, &key, &fingerprint, &owner),
        Command::Lease { key, ttl_ms } => {
            let planned = match shard.lease_plan(&key, ttl_ms) {
                Ok(planned) => planned,
                Err(_) => return Ok(wrong_type()),
            };

            let Some((token, expires_at)) = planned else {
                return Ok(Response::Bulk(None));
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::LeasePut {
                key: &key,
                token,
                expires_at,
            })?;

            if shard.lease_put(&key, token, expires_at, "LEASE").is_err() {
                debug_assert!(false, "lease put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(token as i64))
        }

        Command::Release { key, token }
            if shard.value_kind(&key) == Some(crate::store::ValueKind::Semaphore) =>
        {
            if !shard.semaphore_holds(&key, token) {
                return Ok(Response::Integer(0));
            }

            // Releasing only frees memory, but a running snapshot still needs the holder.
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SemaphoreRelease { key: &key, token })?;

            if shard.semaphore_release(&key, token).is_err() {
                debug_assert!(false, "semaphore release failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::Semaphore { key, max, ttl_ms } => {
            let (token, expires_at, fresh) = match shard.semaphore_plan(&key, max, ttl_ms) {
                Ok(Some(planned)) => planned,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(_) => return Ok(wrong_type()),
            };

            let extra = if fresh {
                key.len() + ADMISSION_OVERHEAD_BYTES
            } else {
                16
            };

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::SemaphoreAcquire {
                key: &key,
                token,
                expires_at,
            })?;

            if shard.semaphore_put(&key, token, expires_at).is_err() {
                debug_assert!(false, "semaphore put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(i64::try_from(token).unwrap_or(i64::MAX)))
        }

        Command::Release { key, token } => {
            match shard.lease_release_plan(&key, token) {
                Ok(Some(_)) => {}
                Ok(None) => return Ok(Response::Integer(0)),
                Err(_) => return Ok(wrong_type()),
            }

            // A released lease takes its key with it, recorded as a plain delete so a replica and
            // a replayed log drop it too. A running snapshot still keeps the held lease.
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::Delete {
                keys: std::slice::from_ref(&key),
            })?;

            shard.delete(&key);

            Ok(Response::Integer(1))
        }

        Command::Renew { key, token, ttl_ms } => {
            let (kind, expires_at) = match shard.renew_plan(&key, token, ttl_ms) {
                Ok(Some(planned)) => planned,
                Ok(None) => return Ok(Response::Integer(0)),
                Err(_) => return Ok(wrong_type()),
            };

            // A renewal rewrites the holder in place: a running snapshot keeps the held one.
            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            let applied = if kind == crate::store::ValueKind::Semaphore {
                sink.record(Mutation::SemaphoreAcquire {
                    key: &key,
                    token,
                    expires_at,
                })?;

                shard.semaphore_put(&key, token, expires_at)
            } else {
                sink.record(Mutation::LeasePut {
                    key: &key,
                    token,
                    expires_at,
                })?;

                shard.lease_put(&key, token, expires_at, "RENEW")
            };

            if applied.is_err() {
                debug_assert!(false, "renew failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::SetVersioned {
            key,
            value,
            expected,
        } => {
            let version = match shard.plan_versioned(&key, expected) {
                Ok(version) => version,
                Err(crate::store::SetVersionError::Mismatch) => {
                    return Ok(Response::Error(b"ERR version mismatch"));
                }

                Err(crate::store::SetVersionError::WrongType) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetVersioned {
                key: &key,
                value: &value,
                version,
            })?;

            if shard.put_versioned(&key, &value, version).is_err() {
                debug_assert!(false, "versioned put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(version as i64))
        }

        Command::Limit {
            key,
            max,
            window_ms,
            mode,
        } => idempotency::limit(shard, sink, key, max, window_ms, mode),

        Command::Once { key, ttl_ms } => {
            let expires_at = match shard.plan_once(&key, ttl_ms) {
                Ok(Some(expires_at)) => expires_at,
                Ok(None) => return Ok(Response::Integer(0)),
                Err(_) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::OncePut {
                key: &key,
                expires_at,
            })?;

            if shard.put_once(&key, expires_at).is_err() {
                debug_assert!(false, "once put failed after validation");

                return Ok(wrong_type());
            }

            Ok(Response::Integer(1))
        }

        Command::ChangesSubscribe(_) => Ok(Response::Error(
            b"ERR CHANGES SUBSCRIBE needs a client connection on the evented runtime",
        )),

        Command::Changes(after) => {
            if after.is_none() {
                let cursor = shard.changes_start();

                return Ok(Response::Integer(cursor as i64));
            }

            let notes = shard.changes_after(after.unwrap_or(0));
            let rows = notes
                .into_iter()
                .map(|note| {
                    Response::Exec(vec![
                        Response::Integer(note.sequence as i64),
                        Response::Bulk(Some(Cow::Owned(note.text.into_bytes()))),
                    ])
                })
                .collect();

            Ok(Response::Exec(rows))
        }

        Command::Increment(key, delta) => {
            let updated = match shard.preview_increment(&key, delta) {
                Ok(value) => value,
                Err(error) => return Ok(increment_error(error)),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("increment type was validated before recording");
            Ok(Response::Integer(updated))
        }

        Command::IncrementCapped { key, delta, limit } => {
            let updated = match shard.preview_increment(&key, delta) {
                Ok(value) => value,
                Err(error) => return Ok(increment_error(error)),
            };

            if updated > limit {
                return Ok(Response::Bulk(None));
            }

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("increment type was validated before recording");

            Ok(Response::Integer(updated))
        }

        Command::SetIfEq {
            key,
            value,
            expected,
            options,
        } => set_if_eq_key(shard, sink, key, value, &expected, options),
        Command::SetIfCmp {
            key,
            value,
            number,
            greater,
            options,
        } => set_if_cmp_key(shard, sink, key, value, number, greater, options),
        Command::DelIfEq { key, expected } => del_if_eq_key(shard, sink, key, &expected),
        Command::ListPushCapped {
            key,
            cap,
            values,
            back,
        } => push_list_capped(shard, sink, waiters, key, cap, values, back),
        Command::Take { key, count } => {
            let updated = match shard.preview_take(&key, count) {
                Ok(Some(updated)) => updated,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(error) => return Ok(increment_error(error)),
            };

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SetInteger {
                key: &key,
                value: updated,
            })?;

            shard
                .set_integer(&key, updated)
                .expect("take type was validated before recording");

            Ok(Response::Integer(updated))
        }

        Command::Transfer {
            source,
            destination,
            amount,
            max,
        } => transfer(shard, sink, source, destination, amount, max),

        Command::HashTransfer {
            key,
            from,
            to,
            amount,
            max,
        } => {
            shard.expire_hash_fields(&key);

            hash_transfer(shard, sink, key, from, to, amount, max)
        }

        Command::GetEx { key, ttl_ms } => {
            let bytes = match shard.copy_string(&key) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Ok(Response::Bulk(None)),
                Err(_) => return Ok(wrong_type()),
            };

            let expires_at = shard.now_unix_millis().get().saturating_add(ttl_ms);
            let extra = if shard.deadline_present(&key) {
                0
            } else {
                key.len()
            };

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::ExpireAt {
                key: &key,
                unix_millis: expires_at,
            })?;

            shard.keep_for(&key, expires_at);

            Ok(Response::Bulk(Some(Cow::Owned(bytes))))
        }

        Command::MGet(keys) => {
            let mut values = Vec::with_capacity(keys.len());

            for key in keys {
                values.push(match shard.value_view(&key) {
                    Ok(value) => value.map(byte_view_to_cow),
                    Err(_) => None,
                });
            }

            Ok(Response::Array(values))
        }

        Command::MSet(pairs) => {
            let extra = pairs
                .iter()
                .map(|(key, value)| key.len() + value.len() + ADMISSION_OVERHEAD_BYTES)
                .sum();

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                pairs.iter().map(|(key, _)| key.as_ref()),
                extra,
            )? {
                return Ok(rejected);
            }

            for (key, value) in &pairs {
                sink.record(Mutation::SetBytes { key, value })?;
            }

            shard.set_many_bytes(&pairs);

            Ok(Response::Simple(b"OK"))
        }

        Command::GetSet(key, value) => {
            let previous = match shard.get_bytes(&key) {
                Ok(Some(current)) => Some(Cow::Owned(current.to_vec())),
                Ok(None) => None,
                Err(_) => return Ok(wrong_type()),
            };

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key.as_ref()],
                key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
            )? {
                return Ok(rejected);
            }

            sink.record(Mutation::SetBytes {
                key: &key,
                value: &value,
            })?;

            shard.set_bytes(&key, &value);
            Ok(Response::Bulk(previous))
        }

        Command::Append(key, suffix) => append_key(shard, sink, key, suffix),
        Command::SetNx(key, value) => set_nx(shard, sink, key, value),
        Command::GetDel(key) => get_del(shard, sink, key),
        Command::MSetNx(pairs) => mset_nx(shard, sink, pairs),
        Command::IncrementFloat(key, delta) => increment_float(shard, sink, key, delta),
        Command::GetRange { key, start, end } => Ok(get_range(shard, &key, start, end)),
        Command::SetRange { key, offset, value } => set_range(shard, sink, key, offset, value),
        Command::SetBit { key, offset, value } => set_bit(shard, sink, key, offset, value),
        Command::GetBit(key, offset) => Ok(get_bit(shard, &key, offset)),
        Command::BitField { key, ops, .. } => bit_field(shard, sink, key, &ops),
        Command::BitCount { key, range } => Ok(bit_count(shard, &key, range)),
        Command::BitPos { key, bit, range } => Ok(bit_pos(shard, &key, bit, range)),
        Command::FillGet {
            key,
            fill_ms,
            wait_ms,
        } => Ok(fill_get(shard, &key, fill_ms, wait_ms)),
        Command::FillSet {
            key,
            value,
            ttl_ms,
            token,
            swr_ms,
        } => fill_set(shard, sink, key, value, ttl_ms, token, swr_ms),
        Command::PfAdd { key, elements } => pf_add(shard, sink, key, elements),
        Command::PfCount(keys) => Ok(pf_count(shard, &keys)),
        Command::PfMerge {
            destination,
            sources,
        } => pf_merge(shard, sink, destination, sources),
        Command::BitOp {
            operation,
            destination,
            keys,
        } => bit_op(shard, sink, operation, destination, keys),
        Command::StrLen(key) => match shard.value_view(&key) {
            Ok(Some(value)) => Ok(Response::Integer(value.text_len() as i64)),
            Ok(None) => Ok(Response::Integer(0)),
            Err(_) => Ok(wrong_type()),
        },
        Command::Del(keys) => {
            if !keys.iter().any(|key| shard.contains_key(key)) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, keys.iter().map(|key| key.as_ref()), 0)?;

            sink.record(Mutation::Delete { keys: &keys })?;

            Ok(Response::Integer(
                keys.iter().filter(|key| shard.delete(key)).count() as i64,
            ))
        }

        Command::ListPush(key, values) => push_list(shard, sink, waiters, key, values, false),
        Command::ListPushBack(key, values) => push_list(shard, sink, waiters, key, values, true),
        Command::ListPop { key, count } => pop_list(shard, sink, key, count, false),
        Command::ListPopBack { key, count } => pop_list(shard, sink, key, count, true),
        Command::ListLen(key) => shard.list_len(&key).map_or_else(
            |_| Ok(wrong_type()),
            |len| Ok(Response::Integer(len as i64)),
        ),
        Command::ListIndex(key, index) => match shard.list_index(&key, index) {
            Ok(value) => Ok(Response::Bulk(value.map(Cow::Borrowed))),
            Err(_) => Ok(wrong_type()),
        },
        Command::ListTrim(key, start, stop) => trim_list(shard, sink, key, start, stop),
        Command::ListInsert {
            key,
            before,
            pivot,
            value,
        } => list_insert(shard, sink, key, before, pivot, value),
        Command::ListSet { key, index, value } => list_set(shard, sink, key, index, value),
        Command::ListRemove { key, count, value } => list_remove(shard, sink, key, count, value),
        Command::ListPos {
            key,
            element,
            rank,
            count,
            max_len,
        } => Ok(list_pos(shard, &key, &element, rank, count, max_len)),
        Command::ListPushX { key, values, back } => {
            list_push_existing(shard, sink, waiters, key, values, back)
        }
        Command::ListMove {
            source,
            destination,
            from_left,
            to_left,
            block,
        } => list_move(
            shard,
            sink,
            waiters,
            source,
            destination,
            from_left,
            to_left,
            block,
        ),
        Command::ListMPop {
            keys,
            from_left,
            count,
            block,
        } => list_mpop(shard, sink, keys, from_left, count, block),
        Command::ListBlock {
            keys,
            timeout_ms,
            back,
        } => block_pop_list(shard, sink, keys, timeout_ms, back),
        Command::ListRange(key, start, stop) => shard
            .list_range_stream(&key, start, stop)
            .map_or_else(|_| Ok(wrong_type()), |items| Ok(Response::ListSlice(items))),
        Command::SetAdd(key, members) => {
            if !accepts_type(shard, &key, ValueKind::Set) {
                return Ok(wrong_type());
            }

            let extra = members.iter().map(|member| member.len()).sum::<usize>()
                + key.len()
                + ADMISSION_OVERHEAD_BYTES;

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            // Every member is already there: nothing changes, so nothing is logged (Redis
            // propagates nothing either). Under `everysec` on a saturated disk a logged no-op
            // measured 0.72x of Redis; a probe per member costs far less than the record.
            if members
                .iter()
                .all(|member| shard.set_contains(&key, member).unwrap_or(false))
            {
                return Ok(Response::Integer(0));
            }

            sink.record(Mutation::SetAdd {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .set_add(&key, &members)
                    .expect("set type was validated before recording") as i64,
            ))
        }

        Command::SetRemove(key, members) => {
            if !accepts_type(shard, &key, ValueKind::Set) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SetRemove {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .set_remove(&key, &members)
                    .expect("set type was validated before recording") as i64,
            ))
        }

        Command::SetContains(key, member) => shard.set_contains(&key, &member).map_or_else(
            |_| Ok(wrong_type()),
            |exists| Ok(Response::Integer(i64::from(exists))),
        ),
        Command::SetCard(key) => shard.set_card(&key).map_or_else(
            |_| Ok(wrong_type()),
            |count| Ok(Response::Integer(count as i64)),
        ),
        Command::SetMembers(key) => shard.set_member_stream(&key).map_or_else(
            |_| Ok(wrong_type()),
            |members| Ok(Response::Members(members)),
        ),
        Command::SetPop { key, count } => pop_set(shard, sink, key, count),
        Command::SetRandom { key, count } => sample_set::<C, I, S>(shard, key, count),
        Command::SetAlgebra {
            keys,
            op,
            destination,
        } => set_algebra(shard, sink, &keys, op, destination.as_deref()),
        Command::SetMIsMember { key, members } => Ok(set_mismember(shard, &key, &members)),
        Command::SetInterCard { keys, limit } => Ok(set_inter_card(shard, &keys, limit)),
        Command::SortedSetCount { key, min, max } => Ok(sorted_count(shard, &key, min, max)),
        Command::SortedSetLexCount { key, min, max } => {
            Ok(sorted_lex_count(shard, &key, &min, &max))
        }
        Command::SortedSetMScore { key, members } => Ok(sorted_mscore(shard, &key, &members)),
        Command::SortedSetAlgebra {
            op,
            keys,
            weights,
            aggregate,
            with_scores,
            destination,
        } => sorted_algebra(
            shard,
            sink,
            op,
            &keys,
            &weights,
            aggregate,
            with_scores,
            destination.as_ref(),
        ),
        Command::SortedSetRandMember {
            key,
            count,
            with_scores,
        } => Ok(sorted_rand_member(shard, &key, count, with_scores)),
        Command::HashRandField {
            key,
            count,
            with_values,
        } => Ok(hash_rand_field(shard, &key, count, with_values)),
        Command::HashIncrementFloat { key, field, delta } => {
            hash_increment_float(shard, sink, key, field, delta)
        }
        Command::SetMove {
            source,
            destination,
            member,
        } => set_move(shard, sink, &source, &destination, &member),
        Command::HashMSet { key, pairs } => execute_in_database(
            shard,
            Command::HashSet { key, pairs },
            sink,
            waiters,
            database,
        )
        .map(|response| match response {
            Response::Integer(_) => Response::Simple(b"OK"),
            response => response,
        }),
        Command::HashSet { key, pairs } => {
            shard.expire_hash_fields(&key);

            if !accepts_type(shard, &key, ValueKind::Hash) {
                return Ok(wrong_type());
            }

            let extra = pairs
                .iter()
                .map(|(field, value)| field.len().saturating_add(value.len()))
                .sum::<usize>()
                + key.len()
                + ADMISSION_OVERHEAD_BYTES;

            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
                return Ok(rejected);
            }

            sink.record(Mutation::HashSet {
                key: &key,
                pairs: &pairs,
            })?;

            Ok(Response::Integer(
                shard
                    .hash_set(&key, &pairs)
                    .expect("hash type was validated before recording") as i64,
            ))
        }

        Command::HashDelete { key, fields } => {
            shard.expire_hash_fields(&key);

            if !accepts_type(shard, &key, ValueKind::Hash) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::HashDelete {
                key: &key,
                fields: &fields,
            })?;

            Ok(Response::Integer(
                shard
                    .hash_delete(&key, &fields)
                    .expect("hash type was validated before recording") as i64,
            ))
        }

        Command::HashGet { key, field } => {
            shard.expire_hash_fields(&key);

            shard.hash_get(&key, &field).map_or_else(
                |_| Ok(wrong_type()),
                |value| Ok(Response::Bulk(value.map(Cow::Borrowed))),
            )
        }

        Command::HashMGet { key, fields } => {
            shard.expire_hash_fields(&key);

            match shard.hash_mget(&key, &fields) {
                Ok(values) => Ok(Response::Array(
                    values
                        .into_iter()
                        .map(|value| value.map(Cow::Borrowed))
                        .collect(),
                )),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::HashLen(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_len(&key).map_or_else(
                |_| Ok(wrong_type()),
                |len| Ok(Response::Integer(len as i64)),
            )
        }

        Command::HashGetAll(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Pairs)),
            )
        }

        Command::HashExists { key, field } => {
            shard.expire_hash_fields(&key);

            shard.hash_exists(&key, &field).map_or_else(
                |_| Ok(wrong_type()),
                |exists| Ok(Response::Integer(i64::from(exists))),
            )
        }

        Command::HashKeys(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Fields)),
            )
        }

        Command::HashValues(key) => {
            shard.expire_hash_fields(&key);

            shard.hash_stream(&key).map_or_else(
                |_| Ok(wrong_type()),
                |hash| Ok(Response::HashEntries(hash, HashStreamKind::Values)),
            )
        }

        Command::HashIncrement { key, field, delta } => {
            shard.expire_hash_fields(&key);

            hash_increment(shard, sink, key, field, delta)
        }

        Command::HashSetNx { key, field, value } => {
            shard.expire_hash_fields(&key);

            hash_set_nx(shard, sink, key, field, value)
        }

        Command::HashStrLen { key, field } => {
            shard.expire_hash_fields(&key);

            match shard.hash_strlen(&key, &field) {
                Ok(length) => Ok(Response::Integer(i64::try_from(length).unwrap_or(i64::MAX))),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::HashScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            shard.expire_hash_fields(&key);

            match shard.hash_scan(&key, cursor, count, pattern.as_deref()) {
                Ok((cursor, pairs)) => {
                    let mut keys = Vec::with_capacity(pairs.len().saturating_mul(2));

                    for (field, value) in pairs {
                        keys.push(Cow::Borrowed(field));
                        keys.push(Cow::Borrowed(value));
                    }

                    Ok(Response::Scan { cursor, keys })
                }

                Err(_) => Ok(wrong_type()),
            }
        }

        Command::CollectionScan {
            kind,
            key,
            cursor,
            pattern,
            count,
        } => Ok(keyspace::collection_scan(
            shard,
            kind,
            &key,
            cursor,
            count,
            pattern.as_deref(),
        )),

        Command::SortedSetAdd {
            key,
            pairs,
            options,
        } => sorted_set_add(shard, sink, waiters, key, pairs, options),

        Command::SortedSetRange {
            key,
            start,
            stop,
            with_scores,
            reverse,
        } => match shard.sorted_set_range_entries(&key, start, stop, reverse) {
            Ok(entries) => Ok(sorted_set_range_response(entries, with_scores)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRangeByScore {
            key,
            min,
            max,
            with_scores,
            reverse,
            limit,
            via_zrange: _,
        } => match shard.sorted_set_range_by_score(&key, min, max) {
            Ok(entries) => Ok(sorted_set_range_response(
                apply_range_window(entries, reverse, limit),
                with_scores,
            )),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRangeByLex {
            key,
            min,
            max,
            with_scores,
            reverse,
            limit,
            invocation: _,
        } => match shard.sorted_set_range_by_lex(&key, &min, &max) {
            Ok(entries) => Ok(sorted_set_range_response(
                apply_range_window(entries, reverse, limit),
                with_scores,
            )),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRemove { key, members } => {
            if !accepts_type(shard, &key, ValueKind::SortedSet) {
                return Ok(wrong_type());
            }

            if !shard.contains_key(&key) {
                return Ok(Response::Integer(0));
            }

            before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

            sink.record(Mutation::SortedSetRemove {
                key: &key,
                members: &members,
            })?;

            Ok(Response::Integer(
                shard
                    .sorted_set_remove(&key, &members)
                    .expect("sorted-set type was validated before recording")
                    as i64,
            ))
        }

        Command::SortedSetIncr { key, delta, member } => {
            sorted_set_incr(shard, sink, waiters, key, delta, member)
        }

        Command::SortedSetCard(key) => shard.sorted_set_card(&key).map_or_else(
            |_| Ok(wrong_type()),
            |count| Ok(Response::Integer(count as i64)),
        ),
        Command::SortedSetScore { key, member } => match shard.sorted_set_score(&key, &member) {
            Ok(Some(score)) => Ok(Response::Double(score)),
            Ok(None) => Ok(Response::Bulk(None)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetRank {
            key,
            member,
            reverse,
        } => match shard.sorted_set_rank(&key, &member, reverse) {
            Ok(Some(rank)) => Ok(Response::Integer(rank as i64)),
            Ok(None) => Ok(Response::Bulk(None)),
            Err(_) => Ok(wrong_type()),
        },
        Command::SortedSetPop { key, count, max } => pop_sorted(shard, sink, key, count, max),
        Command::SortedSetPopByScore {
            key,
            min,
            max,
            count,
            with_scores,
        } => pop_sorted_by_score(shard, sink, key, min, max, count, with_scores),
        Command::SortedSetMoveDue {
            source,
            destination,
            max,
            count,
        } => move_due_sorted(shard, sink, waiters, source, destination, max, count),
        Command::SortedSetBlockPop {
            keys,
            timeout_ms,
            max,
        } => block_pop_sorted(shard, sink, keys, timeout_ms, max),
        Command::SortedSetRemoveRange { key, range } => {
            remove_sorted_range(shard, sink, key, range)
        }

        Command::HashExpire {
            key,
            time,
            condition,
            fields,
            name,
        } => hash_field_expire(shard, sink, key, time, condition, fields, name),
        Command::HashTtl { key, reply, fields } => {
            shard.expire_hash_fields(&key);
            Ok(field_ttl_reply(shard, &key, reply, &fields))
        }

        Command::HashPersist { key, fields } => persist_hash_fields(shard, sink, key, fields),
        Command::GeoAdd {
            key,
            members,
            options,
        } => geo_add(shard, sink, waiters, key, members, options),
        Command::GeoDist {
            key,
            from,
            to,
            unit,
        } => Ok(geo_dist(shard, key, from, to, unit)),
        Command::GeoPos { key, members } => Ok(geo_pos(shard, key, members)),
        Command::GeoHash { key, members } => Ok(geo_hash(shard, key, members)),
        Command::GeoSearch {
            key,
            center,
            area,
            unit,
            descending,
            ordered,
            count,
            any,
            with_dist,
            with_hash,
            with_coord,
            store,
            query: _,
        } => geo_search(
            shard, sink, key, center, area, unit, descending, ordered, count, any, with_dist,
            with_hash, with_coord, store,
        ),
        Command::StreamAdd {
            key,
            id,
            max_len,
            min_id,
            delay_ms,
            nomkstream,
            fields,
        } => stream_add(
            shard, sink, waiters, database, key, id, max_len, min_id, delay_ms, nomkstream, fields,
        ),

        Command::StreamLen(key) => shard.stream_len(&key).map_or_else(
            |_| Ok(wrong_type()),
            |len| Ok(Response::Integer(len as i64)),
        ),
        Command::StreamInfo { key, group, kind } => {
            Ok(stream_info(shard, &key, group.as_deref(), kind))
        }

        Command::StreamRange {
            key,
            start,
            end,
            count,
            reverse,
        } => match shard.stream_range(&key, start, end, count, reverse) {
            Ok(entries) => Ok(stream_range_response(entries)),

            Err(_) => Ok(wrong_type()),
        },
        Command::StreamRead { count, block, keys } => {
            stream_read::<C, I, S>(shard, database, count, block, keys)
        }

        Command::StreamDelete { key, ids } => {
            let mut deleted = 0i64;

            for (millis, sequence) in ids {
                let present = match shard.stream_contains(&key, millis, sequence) {
                    Ok(present) => present,
                    Err(_) => return Ok(wrong_type()),
                };

                if !present {
                    continue;
                }

                if deleted == 0 {
                    before_mutation_allow_oom(
                        shard,
                        sink,
                        [key.as_ref()],
                        key.len() + ADMISSION_OVERHEAD_BYTES,
                    )?;
                }

                sink.record(Mutation::StreamDelete {
                    key: &key,
                    millis,
                    sequence,
                })?;

                match shard.stream_delete(&key, millis, sequence) {
                    Ok(true) => deleted += 1,
                    Ok(false) => {}
                    Err(_) => return Ok(wrong_type()),
                }
            }

            Ok(Response::Integer(deleted))
        }

        Command::StreamTrim { key, max_len } => {
            match shard.value_kind(&key) {
                None => return Ok(Response::Integer(0)),
                Some(ValueKind::Stream) => {}
                Some(_) => return Ok(wrong_type()),
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamTrim { key: &key, max_len })?;

            match shard.stream_trim(&key, max_len) {
                Ok(removed) => Ok(Response::Integer(removed as i64)),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::StreamTrimMinId { key, id } => {
            let (millis, sequence) = id;

            match shard.value_kind(&key) {
                None => return Ok(Response::Integer(0)),
                Some(ValueKind::Stream) => {}
                Some(_) => return Ok(wrong_type()),
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamTrimMinId {
                key: &key,
                millis,
                sequence,
            })?;

            match shard.stream_trim_min_id(&key, millis, sequence) {
                Ok(removed) => Ok(Response::Integer(removed as i64)),
                Err(_) => Ok(wrong_type()),
            }
        }

        Command::StreamSetId { key, id } => stream_set_id(shard, sink, key, id),

        Command::StreamGroupCreate {
            key,
            group,
            start,
            mkstream,
        } => stream_group_create(shard, sink, key, group, start, mkstream),

        Command::StreamGroupRead {
            key,
            group,
            consumer,
            count,
            block,
            noack,
        } => stream_group_read(
            shard, sink, database, key, group, consumer, count, block, noack,
        ),

        Command::StreamGroupDestroy { key, group } => {
            match shard.stream_group_cursor(&key, group.as_ref()) {
                Err(crate::store::StreamGroupError::NoGroup) => return Ok(Response::Integer(0)),
                Err(error) => return Ok(stream_group_error(error)),
                Ok(_) => {}
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamGroupDestroy {
                key: key.as_ref(),
                group: group.as_ref(),
            })?;

            match shard.stream_destroy_group(&key, group.as_ref()) {
                Ok(true) => {
                    waiters.release_group(database, key.as_ref(), group.as_ref());
                    Ok(Response::Integer(1))
                }

                Ok(false) => Ok(Response::Integer(0)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamGroupSetId { key, group, start } => {
            let (millis, sequence) = match start {
                crate::stream::GroupStart::Id(millis, sequence) => (millis, sequence),
                crate::stream::GroupStart::End => match shard.stream_end(&key) {
                    Ok(end) => end,
                    Err(_) => return Ok(wrong_type()),
                },
            };

            if let Err(error) = shard.stream_group_cursor(&key, group.as_ref()) {
                return Ok(stream_group_error(error));
            }

            before_mutation_allow_oom(
                shard,
                sink,
                [key.as_ref()],
                key.len() + ADMISSION_OVERHEAD_BYTES,
            )?;

            sink.record(Mutation::StreamGroupSetId {
                key: key.as_ref(),
                group: group.as_ref(),
                millis,
                sequence,
            })?;

            match shard.stream_set_group_id(&key, group.as_ref(), millis, sequence) {
                Ok(()) => Ok(Response::Simple(b"OK")),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamGroupCreateConsumer {
            key,
            group,
            consumer,
        } => stream_group_create_consumer(shard, sink, key, group, consumer),

        Command::StreamGroupDelConsumer {
            key,
            group,
            consumer,
        } => stream_group_del_consumer(shard, sink, key, group, consumer),

        Command::StreamAck { key, group, ids } => stream_ack(shard, sink, key, group, ids),

        Command::StreamPending { key, group } => {
            match shard.stream_group_summary(&key, group.as_ref()) {
                Ok(summary) => Ok(pending_summary_response(summary)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamPendingRange {
            key,
            group,
            min_idle,
            start,
            end,
            count,
            consumer,
        } => {
            match shard.stream_pending_range(
                &key,
                group.as_ref(),
                crate::stream::PendingFilter {
                    start,
                    end,
                    min_idle,
                    consumer: consumer.as_deref(),
                    count,
                    now: shard.now_unix_millis().get(),
                },
            ) {
                Ok(rows) => Ok(pending_rows_response(rows)),
                Err(error) => Ok(stream_group_error(error)),
            }
        }

        Command::StreamClaimPending {
            key,
            group,
            consumer,
            min_idle,
            ids,
            options,
        } => stream_claim_pending(shard, sink, key, group, consumer, min_idle, ids, options),

        Command::StreamAutoclaim {
            key,
            group,
            consumer,
            min_idle,
            start,
            count,
            just_id,
        } => stream_autoclaim(
            shard, sink, key, group, consumer, min_idle, start, count, just_id,
        ),

        Command::BloomReserve(key, error_rate, capacity, expansion) => {
            bloom_reserve_key(shard, sink, key, error_rate, capacity, expansion)
        }
        Command::BloomAdd(key, value) => bloom_add_key(shard, sink, key, value),
        Command::BloomContains(key, value) => shard.bloom_contains(&key, &value).map_or_else(
            |error| Ok(bloom_error(error)),
            |exists| Ok(Response::Integer(i64::from(exists))),
        ),
        Command::BloomAddMany(key, values) => bloom_add_many_key(shard, sink, key, values),
        Command::BloomContainsMany(key, values) => {
            let mut answers = Vec::with_capacity(values.len());

            Ok(
                match shard.bloom_contains_each(&key, &values, |exists| {
                    answers.push(Response::Integer(i64::from(exists)));
                }) {
                    Ok(()) => Response::Exec(answers),
                    Err(error) => bloom_error(error),
                },
            )
        }
        Command::BloomInsert {
            key,
            error_rate,
            capacity,
            expansion,
            no_create,
            items,
        } => bloom_insert_key(
            shard, sink, key, error_rate, capacity, expansion, no_create, items,
        ),
        Command::BloomInfo(key, field) => Ok(bloom_info(shard, &key, field)),
        Command::BloomCard(key) => Ok(match shard.bloom_info(&key) {
            Ok(info) => Response::Integer(info.map_or(0, |info| info.items as i64)),
            Err(error) => bloom_error(error),
        }),
        Command::CreateTtl { ttl_ms, inner } => {
            transaction::create_with_ttl(shard, sink, waiters, ttl_ms, *inner, database)
        }
        Command::Expire {
            key,
            milliseconds,
            condition,
        } => expire_key(shard, sink, key, milliseconds, false, condition),
        Command::PExpireAt {
            key,
            unix_millis,
            condition,
        } => expire_key(shard, sink, key, unix_millis, true, condition),
        Command::Ttl(key) => Ok(Response::Integer(shard.ttl_seconds(&key))),
        Command::PTtl(key) => Ok(Response::Integer(shard.pttl(&key))),
        Command::Persist(key) => persist_key(shard, sink, key),
        Command::Exists(keys) => Ok(Response::Integer(
            keys.iter().filter(|key| shard.contains_key(key)).count() as i64,
        )),
        Command::Type(key) => Ok(Response::Simple(type_name(shard.value_kind(&key)))),
        Command::Rename {
            source,
            destination,
        } => rename_key(shard, sink, source, destination),
        Command::Echo(message) => Ok(Response::Bulk(Some(message))),
        Command::Object { kind, key } => Ok(keyspace_ext::object(shard, kind, &key)),
        Command::MemoryUsage(key) => Ok(keyspace_ext::memory_usage(shard, &key)),
        Command::Keys(pattern) => Ok(keyspace_ext::keys_matching(shard, &pattern)),
        Command::RandomKey => Ok(keyspace_ext::random_key(shard)),
        Command::Dump(key) => Ok(keyspace_ext::dump_key(shard, &key)),
        Command::Restore {
            key,
            ttl,
            payload,
            replace,
            absolute,
        } => keyspace_ext::restore_key(shard, sink, key, ttl, payload, replace, absolute),
        Command::Touch(keys) => Ok(keyspace_ext::touch(shard, &keys)),
        Command::RenameNx {
            source,
            destination,
        } => keyspace_ext::rename_nx(shard, sink, source, destination),
        Command::ExpireTime { key, millis } => Ok(keyspace_ext::expire_time(shard, &key, millis)),
        Command::Copy {
            source,
            destination,
            replace,
        } => keyspace_ext::copy_key(shard, sink, source, destination, replace),
        Command::Scan {
            cursor,
            pattern,
            count,
            type_name,
            slot,
        } => Ok(scan_keys(
            shard,
            cursor,
            pattern.as_deref(),
            count,
            type_name.as_deref(),
            slot,
        )),
        Command::DbSize => Ok(Response::Integer(shard.database_len() as i64)),
        Command::FlushDb => {
            shard.capture_snapshot_databases(&[database]);

            sink.record(Mutation::FlushDatabase)?;
            shard.flush_database(database);
            Ok(Response::Simple(b"OK"))
        }

        Command::FlushAll => {
            shard.capture_snapshot_databases(&shard.database_ids());

            sink.record(Mutation::FlushAll)?;
            shard.flush_all();
            Ok(Response::Simple(b"OK"))
        }

        Command::SwapDb(left, right) => match (u8::try_from(left), u8::try_from(right)) {
            (Ok(first), Ok(second)) => {
                shard.capture_snapshot_databases(&[first, second]);

                sink.record(Mutation::SwapDatabases { first, second })?;

                shard.swap_databases(first, second);
                Ok(Response::Simple(b"OK"))
            }

            _ => Ok(Response::Error(b"ERR DB index is out of range")),
        },
        Command::Move { key, db } => match u8::try_from(db) {
            Ok(target) if target == database => Ok(Response::Error(
                b"ERR source and destination objects are the same",
            )),
            Ok(target) => move_key(shard, sink, key, target),
            _ => Ok(Response::Error(b"ERR DB index is out of range")),
        },
    }
}
