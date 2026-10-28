use super::*;
use crate::protocol::OneOrMany;

pub(super) fn sorted_set_add<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    pairs: OneOrMany<(f64, crate::protocol::Bytes<'a>)>,
    options: crate::protocol::SortedSetAddOptions,
) -> Result<Response<'a>, S::Error> {
    if pairs.iter().any(|(score, _)| score.is_nan()) {
        return Ok(sorted_set_error(SortedSetError::InvalidScore));
    }

    if !accepts_type(shard, &key, ValueKind::SortedSet) {
        return Ok(wrong_type());
    }

    if options.increment {
        return sorted_set_increment_add(
            shard,
            sink,
            waiters,
            key,
            &pairs[0].1,
            pairs[0].0,
            options,
        );
    }

    let mut added = 0i64;
    let mut changed = 0i64;

    let selected = match pairs {
        // One pair: no other pair can have set its member, and it is either kept or not.
        OneOrMany::One((score, member)) => {
            let current = shard
                .sorted_set_score(&key, &member)
                .expect("sorted-set type was validated");

            if current == Some(score) || !score_update_applies(current, score, options) {
                return Ok(Response::Integer(0));
            }

            added = i64::from(current.is_none());
            changed = 1;
            OneOrMany::One((score, member))
        }
        OneOrMany::Many(mut pairs) => {
            // Scores this command already set, by member: one lookup per pair, so a ZADD of
            // many members stays linear.
            let mut overlay: ahash::AHashMap<&[u8], f64> = ahash::AHashMap::new();
            let mut kept = vec![false; pairs.len()];

            for (index, (score, member)) in pairs.iter().enumerate() {
                let current = overlay.get(member.as_ref()).copied().or_else(|| {
                    shard
                        .sorted_set_score(&key, member)
                        .expect("sorted-set type was validated")
                });

                if current == Some(*score) || !score_update_applies(current, *score, options) {
                    continue;
                }

                if current.is_none() {
                    added += 1;
                }

                changed += 1;
                overlay.insert(member.as_ref(), *score);
                kept[index] = true;
            }
            drop(overlay);

            // `retain` visits every pair once, in order.
            let mut kept = kept.into_iter();

            pairs.retain(|_| kept.next().unwrap_or(false));
            OneOrMany::Many(pairs)
        }
    };

    if selected.is_empty() {
        return Ok(Response::Integer(0));
    }

    let extra = selected
        .iter()
        .map(|(_, member)| member.len())
        .sum::<usize>()
        + key.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(&key, false, selected.iter().map(|(_, m)| m.as_ref())) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    sink.record(Mutation::SortedSetAdd {
        key: &key,
        pairs: &selected,
    })?;

    if let Err(error) = shard.sorted_set_add_pairs(&key, &selected) {
        debug_assert!(false, "sorted-set add failed after validation");
        return Ok(sorted_set_error(error));
    }
    wake_sorted_waiters(shard, sink, waiters, &key)?;

    Ok(Response::Integer(if options.changed {
        changed
    } else {
        added
    }))
}

pub(super) fn sorted_set_increment_add<
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
    member: &crate::protocol::Bytes<'a>,
    delta: f64,
    options: crate::protocol::SortedSetAddOptions,
) -> Result<Response<'a>, S::Error> {
    let current = shard
        .sorted_set_score(&key, member)
        .expect("sorted-set type was validated");
    let next = current.unwrap_or(0.0) + delta;

    if next.is_nan() {
        return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
    }

    if !score_update_applies(current, next, options) {
        return Ok(Response::Bulk(None));
    }

    if current == Some(next) {
        return Ok(Response::Double(next));
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + member.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(&key, false, std::iter::once(member.as_ref())) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    let pair = [(next, member.clone())];

    sink.record(Mutation::SortedSetAdd {
        key: &key,
        pairs: &pair,
    })?;

    if let Err(error) = shard.sorted_set_add(&key, next, member) {
        debug_assert!(false, "sorted-set increment failed after validation");
        return Ok(sorted_set_error(error));
    }
    wake_sorted_waiters(shard, sink, waiters, &key)?;

    Ok(Response::Double(next))
}

pub(super) fn score_update_applies(
    current: Option<f64>,
    score: f64,
    options: crate::protocol::SortedSetAddOptions,
) -> bool {
    match (options.condition, current) {
        (crate::protocol::SortedSetCondition::NotExists, Some(_)) => return false,
        (crate::protocol::SortedSetCondition::Exists, None) => return false,
        _ => {}
    }

    let Some(current) = current else {
        return true;
    };

    if options.greater {
        return score > current;
    }

    if options.less {
        return score < current;
    }

    true
}

/// Pops for parked `BZPOPMIN`/`BZPOPMAX` clients after a write added members to `key`.
#[inline]
pub(super) fn wake_sorted_waiters<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &[u8],
) -> Result<(), S::Error> {
    if !waiters.has_waiters() {
        return Ok(());
    }

    serve_sorted_waiters(shard, sink, waiters, key)
}

#[cold]
pub(super) fn serve_sorted_waiters<
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: &[u8],
) -> Result<(), S::Error> {
    let database = shard.active_database();

    while let Some(max) = waiters.sorted_waiter(database, key) {
        let Some((member, score)) = shard
            .sorted_set_range_entries(key, 0, 0, max)
            .ok()
            .and_then(|entries| {
                entries
                    .first()
                    .map(|(member, score)| (member.to_vec(), *score))
            })
        else {
            break;
        };

        before_mutation_allow_oom(shard, sink, [key], 0)?;

        let members = [Cow::Owned(member)];

        sink.record(Mutation::SortedSetRemove {
            key,
            members: &members,
        })?;

        shard
            .sorted_set_remove_popped(key, &members)
            .expect("sorted-set type was validated before recording");
        waiters.served_by(sink.written_sequence());
        waiters.deliver_sorted(database, key, &members[0], score);
    }

    Ok(())
}

/// Records one `SortedSetRemove` for `members`, then removes them. Empty input writes nothing.
pub(super) fn remove_sorted_members<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Result<(), S::Error> {
    if members.is_empty() {
        return Ok(());
    }

    before_mutation_allow_oom(shard, sink, [key], 0)?;

    sink.record(Mutation::SortedSetRemove { key, members })?;

    shard
        .sorted_set_remove(key, members)
        .expect("sorted-set type was validated before recording");
    Ok(())
}

/// What a pop of `count` would take from the low (`max` false) or high end of `key`, with
/// one index lookup. `None` when `key` holds another type.
fn peek_popped<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    count: usize,
    max: bool,
) -> Option<PoppedEntries> {
    let mut popped = PoppedEntries::default();

    shard
        .sorted_set_visit_ends(key, count, max, |member, score| popped.push(member, score))
        .ok()?;

    Some(popped)
}

/// Records one `SortedSetRemove` for the entries `peek_popped` read, then removes them.
/// The record lists the members in pop order, as a remove of those members would.
fn remove_popped<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    popped: &PoppedEntries,
) -> Result<(), S::Error> {
    let mut entries = popped.iter();

    match popped.len() {
        0 => Ok(()),
        1 => {
            let (member, _) = entries.next().expect("one popped entry");

            apply_popped(shard, sink, key, &[Cow::Borrowed(member)])
        }
        _ => {
            let members: Vec<crate::protocol::Bytes<'_>> =
                entries.map(|(member, _)| Cow::Borrowed(member)).collect();

            apply_popped(shard, sink, key, &members)
        }
    }
}

fn apply_popped<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    members: &[crate::protocol::Bytes<'_>],
) -> Result<(), S::Error> {
    before_mutation_allow_oom(shard, sink, [key], 0)?;

    sink.record(Mutation::SortedSetRemove { key, members })?;

    shard
        .sorted_set_remove_popped(key, members)
        .expect("sorted-set type was validated before recording");
    Ok(())
}

pub(super) fn pop_sorted<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    count: Option<usize>,
    max: bool,
) -> Result<Response<'a>, S::Error> {
    let Some(popped) = peek_popped(shard, &key, count.unwrap_or(1), max) else {
        return Ok(wrong_type());
    };

    remove_popped(shard, sink, &key, &popped)?;

    Ok(Response::Popped(None, popped))
}

/// What `ZPOPBYSCORE`/`ZMOVEDUE` would take: up to `count` entries in `min..=max`, lowest
/// score first, read in one walk of the score order. `None` when `key` holds another type.
fn peek_due<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    min: crate::protocol::ScoreBound,
    max: crate::protocol::ScoreBound,
    count: usize,
) -> Option<PoppedEntries> {
    let mut due = PoppedEntries::default();

    shard
        .sorted_set_visit_scores(key, min, max, |member, score| {
            due.push(member, score);
            due.len() < count
        })
        .ok()?;

    Some(due)
}

pub(super) fn pop_sorted_by_score<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    min: crate::protocol::ScoreBound,
    max: crate::protocol::ScoreBound,
    count: usize,
    with_scores: bool,
) -> Result<Response<'a>, S::Error> {
    let Some(due) = peek_due(shard, &key, min, max, count) else {
        return Ok(wrong_type());
    };

    remove_popped(shard, sink, &key, &due)?;

    Ok(Response::PoppedByScore(due, with_scores))
}

/// `ZMOVEDUE`: the due members leave `source` and join the tail of list `destination` as one
/// recorded transaction. Clients blocked on `destination` take their elements first.
pub(super) fn move_due_sorted<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
    max: crate::protocol::ScoreBound,
    count: usize,
) -> Result<Response<'a>, S::Error> {
    if !accepts_type(shard, &destination, ValueKind::List) {
        return Ok(wrong_type());
    }

    let Some(due) = peek_due(
        shard,
        &source,
        crate::protocol::ScoreBound::NegInf,
        max,
        count,
    ) else {
        return Ok(wrong_type());
    };

    if due.is_empty() {
        return Ok(Response::Integer(0));
    }

    let extra = due.iter().map(|(member, _)| member.len()).sum::<usize>()
        + destination.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) =
        before_mutation(shard, sink, [source.as_ref(), destination.as_ref()], extra)?
    {
        return Ok(rejected);
    }

    let moved = due.len();
    let database = shard.active_database();
    let holding = waiters.has_waiters();

    if holding {
        waiters.hold_deliveries();
    }

    let mut removed = Vec::with_capacity(moved);
    let mut remaining = Vec::new();

    for (member, _) in due.iter() {
        removed.push(member.to_vec());

        if !(holding && waiters.deliver(database, &destination, member)) {
            remaining.push(member.to_vec());
        }
    }

    let mut operations = vec![OwnedMutation::SortedSetRemove {
        key: source.to_vec(),
        members: removed,
    }];

    if !remaining.is_empty() {
        operations.push(OwnedMutation::ListPushBack {
            key: destination.to_vec(),
            values: remaining,
        });
    }

    let recorded = sink.record(Mutation::Transaction {
        mutations: &operations,
    });

    // Blocked clients get their elements only once the move is recorded.
    if holding {
        waiters.served_by(sink.written_sequence());
        waiters.release_deliveries(recorded.is_ok());
    }
    recorded?;

    for operation in &operations {
        match operation {
            OwnedMutation::SortedSetRemove { members, .. } => {
                shard
                    .sorted_set_remove_popped(&source, members)
                    .expect("sorted-set type was validated before recording");
            }
            OwnedMutation::ListPushBack { values, .. } => {
                let _ = shard.list_push_back(&destination, values);
            }
            _ => {}
        }
    }

    Ok(Response::Integer(moved as i64))
}

pub(super) fn remove_sorted_range<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    range: crate::protocol::SortedRemoveRange<'a>,
) -> Result<Response<'a>, S::Error> {
    use crate::protocol::SortedRemoveRange;

    let entries = match &range {
        SortedRemoveRange::Rank(start, stop) => {
            shard.sorted_set_range_entries(&key, *start, *stop, false)
        }

        SortedRemoveRange::Score(min, max) => shard.sorted_set_range_by_score(&key, *min, *max),
        SortedRemoveRange::Lex(min, max) => shard.sorted_set_range_by_lex(&key, min, max),
    };

    let Ok(entries) = entries else {
        return Ok(wrong_type());
    };

    let members: Vec<crate::protocol::Bytes<'static>> = entries
        .into_iter()
        .map(|(member, _)| Cow::Owned(member.to_vec()))
        .collect();

    remove_sorted_members(shard, sink, &key, &members)?;

    Ok(Response::Integer(members.len() as i64))
}

pub(super) fn block_pop_sorted<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    mut keys: Vec<crate::protocol::Bytes<'a>>,
    timeout_ms: Option<u64>,
    max: bool,
) -> Result<Response<'a>, S::Error> {
    for key in &keys {
        if !accepts_type(shard, key, ValueKind::SortedSet) {
            return Ok(wrong_type());
        }
    }

    for index in 0..keys.len() {
        let popped = peek_popped(shard, &keys[index], 1, max).unwrap_or_default();

        if popped.is_empty() {
            continue;
        }

        remove_popped(shard, sink, &keys[index], &popped)?;

        return Ok(Response::Popped(Some(keys.swap_remove(index)), popped));
    }

    let database = shard.active_database();

    Ok(Response::BlockList {
        keys: keys
            .iter()
            .map(|key| Cow::Owned(tagged(database, key)))
            .collect(),
        timeout_ms,
        back: false,
        sorted: Some(max),
    })
}

#[inline(always)]
pub(super) fn sorted_set_incr<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    delta: f64,
    member: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if delta.is_nan() {
        return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
    }

    if !accepts_type(shard, &key, ValueKind::SortedSet) {
        return Ok(wrong_type());
    }

    let current = shard
        .sorted_set_score(&key, &member)
        .expect("type was validated");
    let next = current.unwrap_or(0.0) + delta;

    if next.is_nan() {
        return Ok(sorted_set_error(SortedSetError::InvalidIncrement));
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + member.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    if !shard.sorted_set_make_room(&key, false, std::iter::once(member.as_ref())) {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    let pair = [(next, member)];

    sink.record(Mutation::SortedSetAdd {
        key: &key,
        pairs: &pair,
    })?;

    match shard.sorted_set_add(&key, next, &pair[0].1) {
        Ok(_) => {
            wake_sorted_waiters(shard, sink, waiters, &key)?;

            Ok(Response::Double(next))
        }

        Err(error) => {
            debug_assert!(false, "sorted-set increment failed after validation");
            Ok(sorted_set_error(error))
        }
    }
}
