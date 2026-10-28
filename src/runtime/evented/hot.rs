use super::*;

pub(super) fn hot_to_command<'a>(command: &HotCommand<'a>) -> Option<Command<'a>> {
    match command {
        HotCommand::Ping(_) => None,
        HotCommand::Get(key) => Some(Command::Get(std::borrow::Cow::Borrowed(*key))),
        HotCommand::Set(key, value, px_ms) => Some(Command::Set(
            std::borrow::Cow::Borrowed(*key),
            std::borrow::Cow::Borrowed(*value),
            SetOptions {
                expiry: match px_ms {
                    Some(milliseconds) => SetExpiry::RelativeMs(*milliseconds),
                    None => SetExpiry::None,
                },
                ..SetOptions::default()
            },
        )),
        HotCommand::SetEx(key, value, milliseconds) => Some(Command::Set(
            std::borrow::Cow::Borrowed(*key),
            std::borrow::Cow::Borrowed(*value),
            SetOptions {
                expiry: SetExpiry::RelativeMs(*milliseconds),
                ..SetOptions::default()
            },
        )),
        HotCommand::Delete(key) => Some(Command::Del(crate::protocol::Keys::One(
            std::borrow::Cow::Borrowed(*key),
        ))),
        HotCommand::Increment(key, delta) => {
            Some(Command::Increment(std::borrow::Cow::Borrowed(*key), *delta))
        }
    }
}

pub(super) fn observe_tracking(
    state: &ShardState,
    conn: u64,
    record_reads: bool,
    plan: crate::tracking::TrackPlan,
    applied: bool,
) {
    if !applied {
        return;
    }

    let found = {
        let mut tracker = state.tracker.borrow_mut();
        // Keys pushed out of a full table are invalidated like written ones.
        let mut found = if record_reads && !plan.reads.is_empty() {
            tracker.track(conn, &plan.reads)
        } else {
            Vec::new()
        };

        if plan.invalidates() && !tracker.is_empty() {
            found.extend(tracker.invalidate_plan(&plan));
        }

        // The store keeps expiry and eviction deletes only while some key is tracked. A
        // caller still holding the store (a blocking command's first run) sets it after.
        if record_reads
            && !plan.reads.is_empty()
            && let Ok(mut store) = state.store.try_borrow_mut()
        {
            store.set_tracking_active(true);
        }

        found
    };

    push_invalidations(state, found);
}

pub(super) fn push_invalidations(state: &ShardState, found: Vec<(u64, Vec<Vec<u8>>)>) {
    if found.is_empty() {
        return;
    }

    let failed = RefCell::new(Vec::new());

    {
        let subscribers = state.subscribers.borrow();

        for (conn, keys) in found {
            if let Some((home, real)) = split_remote_id(conn) {
                // Tracked on behalf of a client of another shard: push the invalidation home.
                if let Some(forwarding) = state.forwarding.as_ref() {
                    forwarding.fabric.send(
                        forwarding.me,
                        home,
                        Message::Invalidate { conn: real, keys },
                    );
                }

                continue;
            }

            let frame = std::sync::Arc::<[u8]>::from(crate::tracking::encode_invalidate(&keys));
            let ok = subscribers
                .get(&conn)
                .is_some_and(|slot| slot.try_push(frame));

            if !ok {
                failed.borrow_mut().push(conn);
            }
        }
    }

    let failed = failed.into_inner();

    if failed.is_empty() {
        return;
    }

    let subscribers = state.subscribers.borrow();

    for conn in &failed {
        if let Some(slot) = subscribers.get(conn) {
            slot.close();
        }
    }

    drop(subscribers);
    let mut subscribers = state.subscribers.borrow_mut();
    let mut tracker = state.tracker.borrow_mut();

    for conn in failed {
        subscribers.remove(&conn);
        tracker.forget(conn);
    }
}

pub(super) fn hot_track_plan(
    state: &ShardState,
    tracking: bool,
    command: &HotCommand<'_>,
) -> Option<crate::tracking::TrackPlan> {
    match command {
        HotCommand::Get(key) if tracking => Some(crate::tracking::TrackPlan {
            reads: vec![crate::database::tagged(0, key)],
            writes: Vec::new(),
            flush: false,
        }),
        HotCommand::Set(key, _, _)
        | HotCommand::SetEx(key, _, _)
        | HotCommand::Delete(key)
        | HotCommand::Increment(key, _)
            if !state.tracker.borrow().is_empty() =>
        {
            Some(crate::tracking::TrackPlan {
                reads: Vec::new(),
                writes: vec![crate::database::tagged(0, key)],
                flush: false,
            })
        }

        _ => None,
    }
}

pub(super) fn reply_hot_store(
    state: &ShardState,
    command: &Command<'_>,
    output: &mut BoundedOutput,
    version: crate::engine::RespVersion,
    conn: u64,
    tracking: bool,
) -> io::Result<bool> {
    if state.hot_store.is_none() || state.published_config.hot_keys.is_empty() {
        return Ok(false);
    }

    // A write holds this shard's log while it is recorded and applied to the shared store,
    // so a hot partition read meanwhile finds its record here when it syncs the logs (see
    // `ClusterCheckpointCoordinator::hot_writes_durable`). The lock is this shard's own and
    // uncontended but for that sync; a write that turns out to change nothing (`SET NX` on
    // a present key, `DEL` of a missing one) still takes it, since only the store's own
    // lock decides that.
    let handled = crate::hot_store::dispatch(&state.published_config.hot_keys, command);

    if handled == crate::hot_store::HotDispatch::PassThrough {
        return Ok(false);
    }

    let mut wal = (handled == crate::hot_store::HotDispatch::Handle
        && matches!(command, Command::Set(..) | Command::Del(..)))
    .then(|| {
        state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    });

    // A failed log refuses the write, even one that would change nothing, as a shard
    // write is refused.
    if let Some(sink) = wal.as_deref_mut()
        && let Err(error) = sink.ready_for_record()
    {
        drop(wal.take());
        write_response(output, Response::Error(refused_write(&error)))?;

        return Ok(true);
    }

    let now = state.store.borrow().now_unix_millis();
    let hot = state.hot_store.as_deref();
    // The write is recorded before the shared store applies it, under the store's lock: a
    // refused record leaves the key as it was for every shard. The record is only staged
    // there; it is written (and under `always` synced) once that lock is released, as a
    // grouped shard write is.
    let dispatched = match wal.as_deref_mut() {
        Some(sink) => {
            crate::mutation::MutationSink::select_database(sink, 0);
            let before = sink.last_sequence();

            sink.begin_group();
            let mut dispatched =
                crate::hot_store::reply_dispatched(hot, sink, handled, now, command);
            let written = sink.end_hot_write(before);

            if sink.last_sequence() != before {
                sink.note_hot_record();
            }
            // Applied and kept for recovery, but not written: the writer gets the error.
            if let Err(error) = written
                && let Some(Ok((_, wrote))) = dispatched
            {
                if wrote {
                    bump_mutations(state);
                }
                dispatched = Some(Err(error));
            }
            dispatched
        }
        None => crate::hot_store::reply_dispatched(
            hot,
            &mut crate::mutation::NoopMutationSink,
            handled,
            now,
            command,
        )
        .map(|dispatched| dispatched.map_err(|never| match never {})),
    };

    drop(wal.take());

    match dispatched {
        None => Ok(false),
        Some(Ok((response, wrote))) => {
            let applied = crate::tracking::response_applied(&response);

            if wrote {
                // Counts toward the next checkpoint like any other write.
                bump_mutations(state);
            }

            write_response_in(output, response, version)?;

            if applied {
                observe_tracking(
                    state,
                    conn,
                    tracking,
                    crate::tracking::plan(command, 0),
                    true,
                );
            }

            Ok(true)
        }

        Some(Err(error)) => {
            write_response(output, Response::Error(refused_write(&error)))?;
            Ok(true)
        }
    }
}

pub(super) fn flushes_hot_store(command: &Command<'_>, database: u8) -> bool {
    matches!(command, Command::FlushAll) || (matches!(command, Command::FlushDb) && database == 0)
}

/// Hot keys live in database 0. Returns false when the WAL refused the delete.
pub(super) fn flush_hot_store(state: &ShardState) -> bool {
    let Some(hot) = &state.hot_store else {
        return true;
    };

    let mut wal = state
        .persistence
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    // As for a hot write (see `reply_hot_store`): staged before the store changes, so a
    // refused delete removes nothing, and written once the store's lock is released.
    let start = wal.last_sequence();

    wal.begin_group();
    let flushed = crate::hot_store::flush(hot, &mut wal);
    let written = wal.end_hot_write(start);

    if matches!(flushed, Ok(true)) {
        wal.note_hot_record();
    }
    flushed.is_ok() && written.is_ok()
}

pub(super) fn bump_mutations(state: &ShardState) {
    state
        .mutations_since_checkpoint
        .set(state.mutations_since_checkpoint.get().saturating_add(1));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HotParse, parse_command, parse_hot_command};

    fn frame(words: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", words.len()).into_bytes();

        for word in words {
            out.extend(format!("${}\r\n{word}\r\n", word.len()).bytes());
        }
        out
    }

    /// Every frame the hot parser completes means the same command, with the same length, to
    /// the general parser; a frame it refuses the general parser refuses too. The hot parser
    /// reads its subset again for speed, so nothing else ties the two.
    #[test]
    fn hot_parser_agrees_with_the_general_parser() {
        const NAMES: &[&str] = &[
            "PING", "GET", "SET", "SETEX", "DEL", "UNLINK", "INCR", "DECR", "INCRBY", "DECRBY",
            "get", "set", "SeTeX",
        ];
        const WORDS: &[&str] = &[
            "k",
            "v",
            "",
            "0",
            "1",
            "-1",
            "+5",
            "05",
            "-0",
            "1.5",
            "nan",
            "EX",
            "PX",
            "ex",
            "px",
            "KEEPTTL",
            "9223372036854775807",
            "-9223372036854775808",
            "9223372036854775808",
            "9223372036854776",
        ];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as usize
        };
        let mut disagreements = std::collections::BTreeSet::new();
        let mut completed = 0;

        for round in 0..60_000 {
            let mut words = vec![NAMES[next() % NAMES.len()]];

            for _ in 0..round % 5 {
                words.push(WORDS[next() % WORDS.len()]);
            }
            let bytes = frame(&words);
            let general = parse_command(&bytes);

            match parse_hot_command(&bytes) {
                Ok(HotParse::Complete(hot, length)) => {
                    completed += 1;
                    let expected = match &hot {
                        HotCommand::Ping(message) => {
                            Command::Ping(message.map(std::borrow::Cow::Borrowed))
                        }
                        other => hot_to_command(other).unwrap(),
                    };
                    let agrees = matches!(&general, Ok(Some((command, consumed)))
                        if *consumed == length
                            && format!("{command:?}") == format!("{expected:?}"));

                    if !agrees {
                        disagreements
                            .insert(format!("{words:?} hot={expected:?} general={general:?}"));
                    }
                }
                Err(_) if general.is_ok() => {
                    disagreements.insert(format!("{words:?} hot refused, general={general:?}"));
                }
                _ => {}
            }
        }

        assert!(
            completed > 5_000,
            "only {completed} frames took the hot parser"
        );
        assert!(
            disagreements.is_empty(),
            "hot and general parsers disagree:\n{}",
            disagreements
                .into_iter()
                .take(20)
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}
