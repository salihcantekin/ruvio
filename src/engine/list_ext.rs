//! Redis list commands beyond push/pop/trim.
//!
//! `LINSERT`, `LSET` and `LREM` change only the packed buffer that holds the affected item
//! and log the command itself (`ListInsert`, `ListSet`, `ListRemove`, WAL minor 1), so a
//! write costs the item, not the list.

use super::*;
use crate::mutation::OwnedMutation;

pub(super) fn list_insert<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    before: bool,
    pivot: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let found = match shard.list_iter(&key) {
        Ok(None) => return Ok(Response::Integer(0)),
        Ok(Some(mut items)) => items.any(|item| item == pivot.as_ref()),
        Err(_) => return Ok(wrong_type()),
    };

    if !found {
        return Ok(Response::Integer(-1));
    }

    let extra = value.len() + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    sink.record(Mutation::ListInsert {
        key: &key,
        before,
        pivot: &pivot,
        value: &value,
    })?;

    let length = shard
        .list_insert(&key, before, &pivot, &value)
        .expect("list type was validated");

    Ok(Response::Integer(length))
}

pub(super) fn list_set<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    index: i64,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let length = match shard.list_len(&key) {
        Ok(0) => return Ok(Response::Error(b"ERR no such key")),
        Ok(length) => length as i64,
        Err(_) => return Ok(wrong_type()),
    };
    let position = if index < 0 { length + index } else { index };

    if position < 0 || position >= length {
        return Ok(Response::Error(b"ERR index out of range"));
    }

    let extra = value.len() + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    sink.record(Mutation::ListSet {
        key: &key,
        index: position,
        value: &value,
    })?;

    let _ = shard.list_set(&key, position, &value);
    Ok(Response::Simple(b"OK"))
}

pub(super) fn list_remove<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    count: i64,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let found = match shard.list_iter(&key) {
        Ok(None) => false,
        Ok(Some(mut items)) => items.any(|item| item == value.as_ref()),
        Err(_) => return Ok(wrong_type()),
    };

    if !found {
        return Ok(Response::Integer(0));
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    sink.record(Mutation::ListRemove {
        key: &key,
        count,
        value: &value,
    })?;

    let removed = shard
        .list_remove(&key, count, &value)
        .expect("list type was validated");

    Ok(Response::Integer(removed as i64))
}

pub(super) fn list_pos<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: &[u8],
    element: &[u8],
    rank: i64,
    count: Option<u64>,
    max_len: u64,
) -> Response<'a> {
    let from_tail = rank < 0;
    let items: Box<dyn Iterator<Item = (usize, &[u8])>> = match shard.list_iter(key) {
        Ok(None) => Box::new(std::iter::empty()),
        Ok(Some(items)) if from_tail => {
            let all: Vec<&[u8]> = items.collect();

            Box::new(all.into_iter().enumerate().rev())
        }
        Ok(Some(items)) => Box::new(items.enumerate()),
        Err(_) => return wrong_type(),
    };
    let mut skip = rank.unsigned_abs().saturating_sub(1);
    let want = count.map(|count| {
        if count == 0 {
            usize::MAX
        } else {
            count as usize
        }
    });
    let mut found = Vec::new();
    for (compared, (index, item)) in items.enumerate() {
        if max_len != 0 && compared as u64 >= max_len {
            break;
        }

        if item == element {
            if skip > 0 {
                skip -= 1;
                continue;
            }

            found.push(index as i64);

            if want.is_none_or(|limit| found.len() >= limit) {
                break;
            }
        }
    }

    match want {
        None => found
            .first()
            .map_or(Response::Bulk(None), |index| Response::Integer(*index)),
        Some(_) => Response::Exec(found.into_iter().map(Response::Integer).collect()),
    }
}

pub(super) fn list_push_existing<
    'a,
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    values: Vec<crate::protocol::Bytes<'a>>,
    back: bool,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &key, ValueKind::List) {
        return Ok(wrong_type());
    }

    if !shard.contains_key(&key) {
        return Ok(Response::Integer(0));
    }

    push_list(shard, sink, waiters, key, values.into(), back)
}

/// Moves one element; `None` when the source is empty. Errors surface as a response.
#[allow(clippy::too_many_arguments)]
pub(super) fn list_move<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
    from_left: bool,
    to_left: bool,
    block: Option<Option<u64>>,
) -> Result<Response<'a>, S::Error> {
    // Rotating one list without a log to feed: one type lookup, then the rotation, which
    // returns the moved item.
    if source == destination && !sink.persists() {
        match shard.value_kind(&source) {
            None => {
                return Ok(match block {
                    Some(timeout_ms) => Response::BlockRetry { timeout_ms },
                    None => Response::Bulk(None),
                });
            }
            Some(ValueKind::List) => {}
            Some(_) => return Ok(wrong_type()),
        }

        if let Some(rejected) = before_mutation(
            shard,
            sink,
            [source.as_ref()],
            destination.len() + ADMISSION_OVERHEAD_BYTES,
        )? {
            return Ok(rejected);
        }

        return Ok(match shard.list_rotate(&source, from_left, to_left) {
            Ok(Some(value)) => Response::Bulk(Some(Cow::Owned(value.into_vec()))),
            _ => Response::Bulk(None),
        });
    }

    if !shard.contains_key(&source) {
        return Ok(match block {
            Some(timeout_ms) => Response::BlockRetry { timeout_ms },
            None => Response::Bulk(None),
        });
    }

    if !accepts_type(shard, &source, ValueKind::List)
        || !accepts_type(shard, &destination, ValueKind::List)
    {
        return Ok(wrong_type());
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [source.as_ref(), destination.as_ref()],
        destination.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let value: Vec<u8> = shard
        .list_index(&source, if from_left { 0 } else { -1 })
        .expect("list type was validated")
        .expect("source was checked to exist")
        .to_vec();

    if source == destination {
        if sink.persists() {
            sink.record(Mutation::Transaction {
                mutations: &rotate_operations(&source, &value, from_left, to_left),
            })?;
        }

        let _ = shard.list_rotate(&source, from_left, to_left);

        return Ok(Response::Bulk(Some(Cow::Owned(value))));
    }

    let database = shard.active_database();
    let holding = waiters.has_waiters();

    if holding {
        waiters.hold_deliveries();
    }
    let delivered = source != destination && waiters.deliver(database, &destination, &value);
    let mut operations = vec![if from_left {
        OwnedMutation::ListPopFront {
            key: source.to_vec(),
        }
    } else {
        OwnedMutation::ListPopBack {
            key: source.to_vec(),
        }
    }];

    if !delivered {
        let values = vec![value.clone()];

        operations.push(if to_left {
            OwnedMutation::ListPushFront {
                key: destination.to_vec(),
                values,
            }
        } else {
            OwnedMutation::ListPushBack {
                key: destination.to_vec(),
                values,
            }
        });
    }

    let recorded = sink.record(Mutation::Transaction {
        mutations: &operations,
    });

    // The value reaches a blocked client only once the move is recorded.
    if holding {
        waiters.served_by(sink.written_sequence());
        waiters.release_deliveries(recorded.is_ok());
    }
    recorded?;

    if from_left {
        let _ = shard.list_pop_front(&source);
    } else {
        let _ = shard.list_pop_back(&source);
    }

    if !delivered {
        let pushed = [value.as_slice()];

        let _ = if to_left {
            shard.list_push_front(&destination, &pushed)
        } else {
            shard.list_push_back(&destination, &pushed)
        };
    }

    Ok(Response::Bulk(Some(Cow::Owned(value))))
}

/// The WAL form of a same-list `LMOVE`: a pop and a push, as the two-key case records it.
fn rotate_operations(
    key: &[u8],
    value: &[u8],
    from_left: bool,
    to_left: bool,
) -> [OwnedMutation; 2] {
    let key = key.to_vec();
    let values = vec![value.to_vec()];

    [
        if from_left {
            OwnedMutation::ListPopFront { key: key.clone() }
        } else {
            OwnedMutation::ListPopBack { key: key.clone() }
        },
        if to_left {
            OwnedMutation::ListPushFront { key, values }
        } else {
            OwnedMutation::ListPushBack { key, values }
        },
    ]
}

pub(super) fn list_mpop<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    keys: Vec<crate::protocol::Bytes<'a>>,
    from_left: bool,
    count: u64,
    block: Option<Option<u64>>,
) -> Result<Response<'a>, S::Error> {
    for key in &keys {
        if !accepts_type(shard, key, ValueKind::List) {
            return Ok(wrong_type());
        }
    }

    let Some(key) = keys.iter().find(|key| shard.contains_key(key)) else {
        return Ok(match block {
            Some(timeout_ms) => Response::BlockRetry { timeout_ms },
            None => Response::NullArray,
        });
    };

    let take = (count as usize).min(shard.list_len(key).unwrap_or(0));

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    let operations: Vec<OwnedMutation> = (0..take)
        .map(|_| {
            if from_left {
                OwnedMutation::ListPopFront { key: key.to_vec() }
            } else {
                OwnedMutation::ListPopBack { key: key.to_vec() }
            }
        })
        .collect();

    sink.record(Mutation::Transaction {
        mutations: &operations,
    })?;

    let mut values = Vec::with_capacity(take);

    for _ in 0..take {
        let value = if from_left {
            shard.list_pop_front(key)
        } else {
            shard.list_pop_back(key)
        }
        .expect("list type was validated")
        .expect("take was limited to the list length");

        values.push(Some(Cow::Owned(Vec::from(value))));
    }

    Ok(Response::Exec(vec![
        Response::Bulk(Some(Cow::Owned(key.to_vec()))),
        Response::Array(values),
    ]))
}
