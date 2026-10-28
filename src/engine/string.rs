use super::*;

pub(super) fn set_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
    options: SetOptions,
) -> Result<Response<'a>, S::Error> {
    let previous = if options.get {
        match shard.value_view(&key) {
            Ok(Some(current)) => Some(current.to_vec()),
            Ok(None) => None,
            Err(_) => return Ok(wrong_type()),
        }
    } else {
        None
    };

    match options.condition {
        SetCondition::NotExists if shard.contains_key(&key) => {
            return Ok(if options.get {
                Response::Bulk(previous.map(Cow::Owned))
            } else {
                Response::Bulk(None)
            });
        }

        SetCondition::Exists if !shard.contains_key(&key) => return Ok(Response::Bulk(None)),
        _ => {}
    }

    let now = shard.now_unix_millis();
    let deadline = match resolve_set_expiry(shard, &key, options.expiry, now) {
        Ok(deadline) => deadline,
        Err(response) => return Ok(response),
    };

    if deadline.is_some_and(|expires_at| expires_at <= now) {
        if shard.contains_key(&key) {
            if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], 0)? {
                return Ok(rejected);
            }

            let keys = [key];

            sink.record(Mutation::Delete { keys: &keys })?;

            shard.delete(&keys[0]);
        }

        return Ok(if options.get {
            Response::Bulk(previous.map(Cow::Owned))
        } else {
            Response::Simple(b"OK")
        });
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    match deadline {
        Some(expires_at) => sink.record(Mutation::SetBytesExpiring {
            key: &key,
            value: &value,
            expires_at: expires_at.get(),
        })?,
        None => sink.record(Mutation::SetBytes {
            key: &key,
            value: &value,
        })?,
    }

    match deadline {
        Some(expires_at) => shard.set_bytes_expiring(&key, &value, expires_at),
        None => shard.set_bytes(&key, &value),
    }

    Ok(if options.get {
        Response::Bulk(previous.map(Cow::Owned))
    } else {
        Response::Simple(b"OK")
    })
}

/// Whether the string at `key` equals `expected`: `Ok(None)` for a missing key, an error for
/// another type. A counter is compared as its decimal text on the stack.
fn string_equals<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    expected: &[u8],
) -> Result<Option<bool>, ()> {
    match shard.value_view(key) {
        Ok(Some(ByteView::Borrowed(current))) => Ok(Some(current == expected)),
        Ok(Some(ByteView::Integer(value))) => {
            let mut digits = [0u8; 20];
            let length = crate::store::format_integer(value, &mut digits);

            Ok(Some(&digits[..length] == expected))
        }
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}

/// `SET key value IFEQ expected [expiry]`: the write is the ordinary `SET` once the compare passes.
pub(super) fn set_if_eq_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
    expected: &[u8],
    options: SetOptions,
) -> Result<Response<'a>, S::Error> {
    match string_equals(shard, &key, expected) {
        Ok(Some(true)) => set_key(shard, sink, key, value, options),
        Ok(_) => Ok(Response::Bulk(None)),
        Err(()) => Ok(wrong_type()),
    }
}

/// `SET key value IFGT|IFLT`: writes when the key is missing or `number` is greater/smaller than
/// the stored integer. A stored string that is not an integer is an error and nothing is written.
pub(super) fn set_if_cmp_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
    number: i64,
    greater: bool,
    options: SetOptions,
) -> Result<Response<'a>, S::Error> {
    let current = match shard.value_view(&key) {
        Ok(Some(ByteView::Integer(current))) => Some(current),
        Ok(Some(ByteView::Borrowed(bytes))) => match crate::protocol::redis_integer(bytes) {
            Some(current) => Some(current),
            None => {
                return Ok(Response::Error(
                    b"ERR value is not an integer or out of range",
                ));
            }
        },
        Ok(None) => None,
        Err(_) => return Ok(wrong_type()),
    };

    match current {
        Some(current) if (number > current) != greater || number == current => {
            Ok(Response::Bulk(None))
        }
        _ => set_key(shard, sink, key, value, options),
    }
}

/// `DELEX key IFEQ expected`: 1 when the string matched and was deleted, otherwise 0.
pub(super) fn del_if_eq_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    expected: &[u8],
) -> Result<Response<'a>, S::Error> {
    match string_equals(shard, &key, expected) {
        Ok(Some(true)) => {}
        Ok(_) => return Ok(Response::Integer(0)),
        Err(()) => return Ok(wrong_type()),
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    let keys = [key];

    sink.record(Mutation::Delete { keys: &keys })?;

    Ok(Response::Integer(i64::from(shard.delete(&keys[0]))))
}

pub(super) fn byte_view_to_cow(view: ByteView<'_>) -> Cow<'_, [u8]> {
    match view {
        ByteView::Borrowed(bytes) => Cow::Borrowed(bytes),
        ByteView::Integer(value) => Cow::Owned(crate::store::integer_bytes(value)),
    }
}

pub(super) fn append_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    suffix: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let kind = shard.value_kind(&key);

    if kind.is_some_and(|kind| kind != ValueKind::Bytes) {
        return Ok(wrong_type());
    }

    let extra =
        suffix.len() + if kind.is_none() { key.len() } else { 0 } + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    let exists = if kind.is_none() {
        false
    } else if sink.persists() {
        // The log gets only the appended bytes, whatever the value's size, so it needs the
        // current length before the value changes.
        match shard.value_view(&key) {
            Ok(Some(view)) => {
                sink.record(Mutation::SetRange {
                    key: &key,
                    offset: view.text_len() as u64,
                    bytes: &suffix,
                })?;
                true
            }
            Ok(None) => false,
            Err(_) => return Ok(wrong_type()),
        }
    } else {
        true
    };

    if exists {
        // The in-place append reports the new length, so memory mode reads the value once.
        match shard.append_in_place(&key, &suffix) {
            Ok(Some(length)) => return Ok(Response::Integer(length)),
            Ok(None) => {}
            Err(_) => return Ok(wrong_type()),
        }

        // A value stored as an object cannot grow in place.
        let mut combined = match shard.value_view(&key) {
            Ok(Some(view)) => byte_view_to_cow(view).into_owned(),
            Ok(None) => {
                shard.set_bytes(&key, &suffix);
                return Ok(Response::Integer(suffix.len() as i64));
            }
            Err(_) => return Ok(wrong_type()),
        };
        let deadline = shard.expires_at(&key);

        combined.extend_from_slice(&suffix);
        let length = combined.len() as i64;

        shard.set_bytes(&key, &combined);

        if let Some(expires_at) = deadline {
            shard.expire_at(&key, expires_at);
        }

        return Ok(Response::Integer(length));
    }

    sink.record(Mutation::SetBytes {
        key: &key,
        value: &suffix,
    })?;

    shard.set_bytes(&key, &suffix);
    Ok(Response::Integer(suffix.len() as i64))
}
