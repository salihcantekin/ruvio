use super::*;

pub(super) fn push_list<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    values: crate::protocol::OneOrMany<crate::protocol::Bytes<'a>>,
    back: bool,
) -> Result<Response<'a>, S::Error> {
    Ok(
        match push_list_length(shard, sink, waiters, &key, values, back)? {
            Ok(pushed) => Response::Integer(pushed.reply() as i64),
            Err(rejected) => rejected,
        },
    )
}

/// A push that went through: the list's length afterwards and how many elements went
/// straight to blocked clients instead.
#[derive(Clone, Copy)]
struct Pushed {
    length: usize,
    delivered: usize,
}

impl Pushed {
    /// The length Redis replies with: every element counts as pushed before blocked
    /// clients take theirs.
    fn reply(self) -> usize {
        self.length + self.delivered
    }
}

/// The push itself, or the reply that refused it.
fn push_list_length<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &crate::protocol::Bytes<'_>,
    values: crate::protocol::OneOrMany<crate::protocol::Bytes<'_>>,
    back: bool,
) -> Result<Result<Pushed, Response<'static>>, S::Error> {
    if !accepts_type(shard, key, ValueKind::List) {
        return Ok(Err(wrong_type()));
    }

    if !waiters.has_waiters() {
        return push_or_deliver(shard, sink, waiters, key, values, back);
    }

    // Blocked clients get their elements only once the rest of the push is recorded.
    let before = sink.written_sequence();

    waiters.hold_deliveries();
    let pushed = push_or_deliver(shard, sink, waiters, key, values, back);

    waiters.served_by(written_since(before, sink.written_sequence()));
    waiters.release_deliveries(matches!(pushed, Ok(Ok(_))));
    pushed
}

fn push_or_deliver<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &crate::protocol::Bytes<'_>,
    values: crate::protocol::OneOrMany<crate::protocol::Bytes<'_>>,
    back: bool,
) -> Result<Result<Pushed, Response<'static>>, S::Error> {
    let mut remaining = values;
    let database = shard.active_database();
    let mut delivered = 0;

    // Without blocked clients every element stays and nothing is copied.
    if waiters.has_waiters() {
        remaining.retain(|value| {
            let taken = waiters.deliver(database, key, value);

            delivered += usize::from(taken);
            !taken
        });
    }

    if remaining.is_empty() {
        return Ok(Ok(Pushed {
            length: shard.list_len(key).unwrap_or(0),
            delivered,
        }));
    }

    let extra = remaining.iter().map(|value| value.len()).sum::<usize>()
        + key.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(Err(rejected));
    }

    let length = if back {
        sink.record(Mutation::ListPushBack {
            key,
            values: &remaining,
        })?;

        shard
            .list_push_back(key, &remaining)
            .expect("list type was validated before recording")
    } else {
        sink.record(Mutation::ListPushFront {
            key,
            values: &remaining,
        })?;

        shard
            .list_push_front(key, &remaining)
            .expect("list type was validated before recording")
    };

    Ok(Ok(Pushed { length, delivered }))
}

/// `LPUSHCAP`/`RPUSHCAP`: the ordinary push, then the oldest items leave from the opposite end
/// until `cap` remain. Typical pushes drop as many items as they add, so the drop is O(pushed);
/// only a list already over `cap` takes one `LTRIM`-style record.
pub(super) fn push_list_capped<
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
    cap: usize,
    values: Vec<crate::protocol::Bytes<'a>>,
    back: bool,
) -> Result<Response<'a>, S::Error> {
    let pushed = values.len();
    let (length, reply) = match push_list_length(shard, sink, waiters, &key, values.into(), back)? {
        Ok(done) => (done.length, done.reply().min(cap)),
        Err(rejected) => return Ok(rejected),
    };

    if length <= cap {
        return Ok(Response::Integer(reply as i64));
    }

    let excess = length - cap;

    if excess > pushed {
        // Already over the cap: one trim record instead of one pop per item.
        let (start, stop) = if back {
            (excess as i64, -1)
        } else {
            (0, cap as i64 - 1)
        };

        sink.record(Mutation::ListTrim {
            key: &key,
            start,
            stop,
        })?;

        shard
            .list_trim(&key, start as isize, stop as isize)
            .expect("list type was validated before recording");

        return Ok(Response::Integer(cap as i64));
    }

    for _ in 0..excess {
        if back {
            sink.record(Mutation::ListPopFront { key: &key })?;
            shard.list_pop_front(&key)
        } else {
            sink.record(Mutation::ListPopBack { key: &key })?;
            shard.list_pop_back(&key)
        }
        .expect("list type was validated before recording");
    }

    Ok(Response::Integer(cap as i64))
}

pub(super) fn pop_list<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    count: Option<usize>,
    back: bool,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &key, ValueKind::List) {
        return Ok(wrong_type());
    }

    let available = shard.list_len(&key).expect("list type was validated");
    let take = count.unwrap_or(1).min(available);

    if take == 0 {
        return Ok(Response::Bulk(None));
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    if take == 1 {
        if back {
            sink.record(Mutation::ListPopBack { key: &key })?;
        } else {
            sink.record(Mutation::ListPopFront { key: &key })?;
        }
    } else {
        // One record drops the whole run, however many elements it holds.
        let take = take as i64;
        let (start, stop) = if back { (0, -(take + 1)) } else { (take, -1) };

        sink.record(Mutation::ListTrim {
            key: &key,
            start,
            stop,
        })?;
    }

    let mut values = Vec::with_capacity(take);

    for _ in 0..take {
        let value = if back {
            shard.list_pop_back(&key)
        } else {
            shard.list_pop_front(&key)
        }
        .expect("list type was validated before recording")
        .expect("pop count was limited to the list length");

        values.push(value);
    }

    if count.is_none() {
        return Ok(Response::Bulk(Some(Cow::Owned(Vec::from(
            values.pop().expect("one popped value"),
        )))));
    }

    Ok(Response::Array(
        values
            .into_iter()
            .map(|value| Some(Cow::Owned(Vec::from(value))))
            .collect(),
    ))
}

pub(super) fn trim_list<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    start: isize,
    stop: isize,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &key, ValueKind::List) {
        return Ok(wrong_type());
    }

    if !shard.contains_key(&key) {
        return Ok(Response::Simple(b"OK"));
    }

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    sink.record(Mutation::ListTrim {
        key: &key,
        start: start as i64,
        stop: stop as i64,
    })?;

    shard
        .list_trim(&key, start, stop)
        .expect("list type was validated before recording");
    Ok(Response::Simple(b"OK"))
}

pub(super) fn block_pop_list<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    keys: Vec<crate::protocol::Bytes<'a>>,
    timeout_ms: Option<u64>,
    back: bool,
) -> Result<Response<'a>, S::Error> {
    for key in &keys {
        if !accepts_type(shard, key, ValueKind::List) {
            return Ok(wrong_type());
        }
    }

    for key in keys.iter() {
        if !shard.contains_key(key) {
            continue;
        }

        before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

        let value = if back {
            sink.record(Mutation::ListPopBack { key })?;

            shard
                .list_pop_back(key)
                .expect("list type was validated before recording")
        } else {
            sink.record(Mutation::ListPopFront { key })?;

            shard
                .list_pop_front(key)
                .expect("list type was validated before recording")
        };

        return Ok(Response::Array(vec![
            Some(Cow::Owned(key.to_vec())),
            Some(Cow::Owned(value.map_or_else(Vec::new, Vec::from))),
        ]));
    }

    let database = shard.active_database();

    Ok(Response::BlockList {
        keys: keys
            .iter()
            .map(|key| Cow::Owned(tagged(database, key)))
            .collect(),
        timeout_ms,
        back,
        sorted: None,
    })
}
