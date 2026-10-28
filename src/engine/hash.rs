use super::*;

pub(super) fn hash_set_nx<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    field: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    match shard.hash_exists(&key, &field) {
        Ok(true) => return Ok(Response::Integer(0)),
        Ok(false) => {}
        Err(_) => return Ok(wrong_type()),
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + field.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let pairs = [(field, value)];

    sink.record(Mutation::HashSet {
        key: &key,
        pairs: &pairs,
    })?;

    shard
        .hash_set(&key, &pairs)
        .expect("hash type was validated before recording");
    Ok(Response::Integer(1))
}

pub(super) fn hash_increment<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    field: crate::protocol::Bytes<'a>,
    delta: i64,
) -> Result<Response<'a>, S::Error> {
    let updated = match shard.preview_hash_increment(&key, &field, delta) {
        Ok(value) => value,
        Err(error) => return Ok(increment_error(error)),
    };

    let encoded: Cow<'_, [u8]> = Cow::Owned(crate::store::integer_bytes(updated));

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + field.len() + encoded.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let pairs = [(field, encoded)];
    let kept = if shard.has_field_ttls() {
        shard
            .field_deadline(&key, &pairs[0].0)
            .expect("hash type was validated")
    } else {
        None
    };

    match kept {
        // The kept field TTL goes out with the value in one record: a torn WAL tail
        // or a replica never sees the field without it.
        Some(at) => sink.record(Mutation::Transaction {
            mutations: &[
                Mutation::HashSet {
                    key: &key,
                    pairs: &pairs,
                }
                .to_owned(),
                Mutation::HashFieldExpire {
                    key: &key,
                    fields: std::slice::from_ref(&pairs[0].0),
                    unix_millis: at,
                }
                .to_owned(),
            ],
        })?,
        None => sink.record(Mutation::HashSet {
            key: &key,
            pairs: &pairs,
        })?,
    }

    match kept {
        Some(_) => {
            shard
                .hash_set_keep_ttl(&key, &pairs[0].0, &pairs[0].1)
                .expect("hash type was validated before recording");
        }

        None => {
            shard
                .hash_set(&key, &pairs)
                .expect("hash type was validated before recording");
        }
    }

    Ok(Response::Integer(updated))
}

/// What `HEXPIRE` does to one field, given the field's TTL before this call (`None` when the
/// field is missing). Fields are decided in argument order, so a repeated field sees the
/// earlier decision.
pub(super) fn field_expire_outcome(
    current: Option<Option<u64>>,
    at: u64,
    now: u64,
    condition: crate::protocol::FieldExpireCondition,
) -> i64 {
    use crate::protocol::FieldExpireCondition;

    let Some(current) = current else {
        return -2;
    };

    let allowed = match condition {
        FieldExpireCondition::Always => true,
        FieldExpireCondition::Nx => current.is_none(),
        FieldExpireCondition::Xx => current.is_some(),
        FieldExpireCondition::Gt => current.is_some_and(|current| at > current),
        FieldExpireCondition::Lt => current.is_none_or(|current| at < current),
    };

    match (allowed, at <= now) {
        (false, _) => 0,
        (true, true) => 2,
        (true, false) => 1,
    }
}

/// Field states a multi-field command has already changed, by field name. A single field
/// needs no map.
struct RepeatedFields<'f>(Option<ahash::AHashMap<&'f [u8], Option<Option<u64>>>>);

impl<'f> RepeatedFields<'f> {
    fn new(fields: usize) -> Self {
        Self((fields > 1).then(ahash::AHashMap::new))
    }

    fn state(&self, field: &[u8], stored: Option<Option<u64>>) -> Option<Option<u64>> {
        self.0
            .as_ref()
            .and_then(|changed| changed.get(field).copied())
            .unwrap_or(stored)
    }

    fn set(&mut self, field: &'f [u8], state: Option<Option<u64>>) {
        if let Some(changed) = &mut self.0 {
            changed.insert(field, state);
        }
    }
}

pub(super) fn hash_field_expire<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    time: crate::protocol::FieldExpireTime,
    condition: crate::protocol::FieldExpireCondition,
    fields: Vec<crate::protocol::Bytes<'a>>,
    name: &'static str,
) -> Result<Response<'a>, S::Error> {
    shard.expire_hash_fields(&key);

    let now = shard.now_unix_millis().get();
    let at = match time {
        crate::protocol::FieldExpireTime::AtMillis(at) => at,
        crate::protocol::FieldExpireTime::InMillis(millis) => match now
            .checked_add(millis)
            .filter(|at| *at <= crate::protocol::MAX_FIELD_EXPIRE_MS)
        {
            Some(at) => at,
            None => {
                return Ok(Response::OwnedError(
                    format!("ERR invalid expire time in '{name}' command").into_bytes(),
                ));
            }
        },
    };

    let Ok(states) = shard.field_deadlines(&key, &fields) else {
        return Ok(wrong_type());
    };

    let mut replies = Vec::with_capacity(fields.len());
    let mut deleted = Vec::new();
    let mut timed = Vec::new();
    // A field named twice sees what this command already did to it; one lookup per field
    // keeps a long field list linear.
    let mut overlay = RepeatedFields::new(fields.len());

    for index in 0..fields.len() {
        let state = overlay.state(&fields[index], states[index]);
        let outcome = field_expire_outcome(state, at, now, condition);
        let next = match outcome {
            1 => Some(Some(at)),
            2 => None,
            _ => state,
        };

        overlay.set(&fields[index], next);

        match outcome {
            1 => timed.push(fields[index].clone()),
            2 => deleted.push(fields[index].clone()),
            _ => {}
        }

        replies.push(Response::Integer(outcome));
    }

    if !deleted.is_empty() || !timed.is_empty() {
        if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], 0)? {
            return Ok(rejected);
        }

        if !deleted.is_empty() {
            sink.record(Mutation::HashDelete {
                key: &key,
                fields: &deleted,
            })?;
        }

        if !timed.is_empty() {
            sink.record(Mutation::HashFieldExpire {
                key: &key,
                fields: &timed,
                unix_millis: at,
            })?;
        }

        if !deleted.is_empty() {
            shard
                .hash_delete(&key, &deleted)
                .expect("hash type was validated before recording");
        }

        if !timed.is_empty() {
            shard
                .hash_expire_fields(&key, &timed, at)
                .expect("hash type was validated before recording");
        }
    }

    Ok(Response::Exec(replies))
}

pub(super) fn field_ttl_reply<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    reply: crate::protocol::FieldTtlReply,
    fields: &[crate::protocol::Bytes<'_>],
) -> Response<'static> {
    use crate::protocol::FieldTtlReply;

    let now = shard.now_unix_millis().get();
    let Ok(states) = shard.field_deadlines(key, fields) else {
        return wrong_type();
    };

    Response::Exec(
        states
            .into_iter()
            .map(|state| {
                Response::Integer(match state {
                    None => -2,
                    Some(None) => -1,
                    Some(Some(at)) => {
                        let value = match reply {
                            FieldTtlReply::Seconds => at.saturating_sub(now).div_ceil(1_000),
                            FieldTtlReply::Millis => at.saturating_sub(now),
                            FieldTtlReply::AtSeconds => at.div_ceil(1_000),
                            FieldTtlReply::AtMillis => at,
                        };

                        i64::try_from(value).unwrap_or(i64::MAX)
                    }
                })
            })
            .collect(),
    )
}

pub(super) fn persist_hash_fields<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    fields: Vec<crate::protocol::Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    shard.expire_hash_fields(&key);

    let Ok(states) = shard.field_deadlines(&key, &fields) else {
        return Ok(wrong_type());
    };

    let mut replies = Vec::with_capacity(fields.len());
    let mut cleared = Vec::new();
    let mut overlay = RepeatedFields::new(fields.len());

    for index in 0..fields.len() {
        let outcome = match overlay.state(&fields[index], states[index]) {
            None => -2,
            Some(None) => -1,
            Some(Some(_)) => {
                overlay.set(&fields[index], Some(None));
                cleared.push(fields[index].clone());
                1
            }
        };

        replies.push(Response::Integer(outcome));
    }

    if !cleared.is_empty() {
        before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

        sink.record(Mutation::HashFieldPersist {
            key: &key,
            fields: &cleared,
        })?;

        shard
            .hash_persist_fields(&key, &cleared)
            .expect("hash type was validated before recording");
    }

    Ok(Response::Exec(replies))
}

pub(super) fn hash_transfer<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    from: crate::protocol::Bytes<'a>,
    to: crate::protocol::Bytes<'a>,
    amount: i64,
    max: Option<i64>,
) -> Result<Response<'a>, S::Error> {
    let (left, right) = match shard.preview_hash_transfer(&key, &from, &to, amount, max) {
        Ok(Some(balances)) => balances,
        Ok(None) => return Ok(Response::Bulk(None)),
        Err(error) => return Ok(increment_error(error)),
    };

    let pairs = [
        (from, Cow::<[u8]>::Owned(crate::store::integer_bytes(left))),
        (to, Cow::Owned(crate::store::integer_bytes(right))),
    ];

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len()
            + pairs
                .iter()
                .map(|(field, value)| field.len() + value.len() + ADMISSION_OVERHEAD_BYTES)
                .sum::<usize>(),
    )? {
        return Ok(rejected);
    }

    // Field TTLs stay on both fields, like `HINCRBY`, and go out with the values in one record.
    let kept = if shard.has_field_ttls() {
        [
            shard
                .field_deadline(&key, &pairs[0].0)
                .expect("hash type was validated"),
            shard
                .field_deadline(&key, &pairs[1].0)
                .expect("hash type was validated"),
        ]
    } else {
        [None, None]
    };

    if kept.iter().all(Option::is_none) {
        sink.record(Mutation::HashSet {
            key: &key,
            pairs: &pairs,
        })?;
    } else {
        let mut mutations = vec![
            Mutation::HashSet {
                key: &key,
                pairs: &pairs,
            }
            .to_owned(),
        ];

        for (pair, at) in pairs.iter().zip(kept) {
            if let Some(at) = at {
                mutations.push(
                    Mutation::HashFieldExpire {
                        key: &key,
                        fields: std::slice::from_ref(&pair.0),
                        unix_millis: at,
                    }
                    .to_owned(),
                );
            }
        }

        sink.record(Mutation::Transaction {
            mutations: &mutations,
        })?;
    }

    if kept.iter().all(Option::is_none) {
        shard
            .hash_set(&key, &pairs)
            .expect("hash type was validated before recording");
    } else {
        for (field, value) in &pairs {
            shard
                .hash_set_keep_ttl(&key, field, value)
                .expect("hash type was validated before recording");
        }
    }

    Ok(Response::Exec(vec![
        Response::Integer(left),
        Response::Integer(right),
    ]))
}
