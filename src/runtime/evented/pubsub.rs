use super::*;

pub(super) fn publish_sharded(state: &ShardState, channel: &[u8], message: &[u8]) -> i64 {
    if state.pubsub.borrow().subscribers(channel).is_empty() {
        if state.role.get() == crate::config::ReplicationRole::Primary {
            let fanout = &state.publish_fanout;
            fanout.send_with(|| crate::replication::encode_copied_publish(true, channel, message));
        }

        return 0;
    }

    let frame = crate::pubsub::encode_smessage(channel, message);
    let failed = RefCell::new(Vec::new());
    let delivered = {
        let subscribers = state.subscribers.borrow();
        let mut hub = state.pubsub.borrow_mut();
        crate::pubsub::deliver(&mut hub, channel, frame, |conn, frame| {
            let ok = if let Some(slot) = subscribers.get(&conn) {
                slot.try_push(frame)
            } else {
                drop(frame);
                false
            };

            if !ok {
                failed.borrow_mut().push(conn);
            }

            ok
        })
    };

    let failed = failed.into_inner();

    if !failed.is_empty() {
        let subscribers = state.subscribers.borrow();

        for conn in &failed {
            if let Some(slot) = subscribers.get(conn) {
                slot.close();
            }
        }

        drop(subscribers);
        let mut subscribers = state.subscribers.borrow_mut();

        for conn in failed {
            subscribers.remove(&conn);
        }
    }

    if state.role.get() == crate::config::ReplicationRole::Primary {
        let fanout = &state.publish_fanout;
        fanout.send_with(|| crate::replication::encode_copied_publish(true, channel, message));
    }

    delivered
}

pub(super) fn publish_classic(state: &ShardState, channel: &[u8], message: &[u8]) -> i64 {
    if state.pubsub.borrow().classic_unheard(channel) {
        if state.role.get() == crate::config::ReplicationRole::Primary {
            let fanout = &state.publish_fanout;
            fanout.send_with(|| crate::replication::encode_copied_publish(false, channel, message));
        }

        return 0;
    }

    let frames = state.pubsub.borrow().classic_frames(channel, message);
    let failed = RefCell::new(Vec::new());
    let delivered = {
        let subscribers = state.subscribers.borrow();
        let mut hub = state.pubsub.borrow_mut();
        crate::pubsub::deliver_frames(&mut hub, frames, |conn, frame| {
            let ok = if let Some(slot) = subscribers.get(&conn) {
                slot.try_push(frame)
            } else {
                drop(frame);
                false
            };

            if !ok {
                failed.borrow_mut().push(conn);
            }

            ok
        })
    };

    let failed = failed.into_inner();

    if !failed.is_empty() {
        let subscribers = state.subscribers.borrow();

        for conn in &failed {
            if let Some(slot) = subscribers.get(conn) {
                slot.close();
            }
        }

        drop(subscribers);
        let mut subscribers = state.subscribers.borrow_mut();

        for conn in failed {
            subscribers.remove(&conn);
        }
    }

    if state.role.get() == crate::config::ReplicationRole::Primary {
        let fanout = &state.publish_fanout;
        fanout.send_with(|| crate::replication::encode_copied_publish(false, channel, message));
    }

    delivered
}

pub(super) fn refresh_notify_listeners(state: &ShardState) {
    let listeners = state.pubsub.borrow().notify_listeners();

    state.store.borrow_mut().set_notify_listeners(listeners);
}

#[inline]
pub(super) fn flush_key_notices(state: &ShardState) {
    if !state.store.borrow().notices_idle() {
        publish_key_notices(state);
    }
}

#[inline(never)]
fn publish_key_notices(state: &ShardState) {
    let (mask, pending, dropped) = state.store.borrow_mut().drain_notices();

    if !dropped.is_empty() {
        invalidate_dropped(state, &dropped);
    }

    if pending.is_empty() {
        return;
    }

    for (database, key, event) in pending {
        for notice in crate::notify::notices(mask, database, &key, event) {
            let _ = publish_sharded(state, &notice.channel, &notice.payload);
        }
    }

    refresh_notify_listeners(state);
}

/// Client-side caches drop keys that expired or were evicted, as they do written ones.
#[cold]
fn invalidate_dropped(state: &ShardState, dropped: &[(u8, Box<[u8]>)]) {
    let keys: Vec<Vec<u8>> = dropped
        .iter()
        .map(|(database, key)| crate::database::tagged(*database, key))
        .collect();
    let found = {
        let mut tracker = state.tracker.borrow_mut();
        let found = tracker.invalidate(&keys);

        state
            .store
            .borrow_mut()
            .set_tracking_active(!tracker.is_empty());
        found
    };

    super::hot::push_invalidations(state, found);
}

pub(super) fn apply_sharded_pubsub(
    state: &ShardState,
    session: &mut TxSession,
    output: &mut BoundedOutput,
    command: &Command<'_>,
) -> io::Result<bool> {
    // Inside MULTI a PUBLISH is queued (and sent at EXEC); SPUBLISH is refused there.
    if session.is_multi()
        && matches!(
            command,
            Command::Publish { .. } | Command::ShardPublish { .. }
        )
    {
        return Ok(false);
    }

    let (channels, subscribe, _) = match command {
        Command::ShardPublish { channel, message } => {
            let delivered = publish_sharded(state, channel.as_ref(), message.as_ref());
            write_response(output, Response::Integer(delivered))?;

            return Ok(true);
        }

        Command::Publish { channel, message } => {
            let delivered = publish_classic(state, channel.as_ref(), message.as_ref());
            write_response(output, Response::Integer(delivered))?;

            return Ok(true);
        }

        Command::ShardSubscribe(channels) => (channels, true, false),
        Command::ShardUnsubscribe(channels) => (channels, false, false),
        Command::Subscribe(channels) => (channels, true, true),
        Command::Unsubscribe(channels) => (channels, false, true),
        Command::PSubscribe(channels) => (channels, true, true),
        Command::PUnsubscribe(channels) => (channels, false, true),
        _ => return Ok(false),
    };

    let pattern = matches!(command, Command::PSubscribe(_) | Command::PUnsubscribe(_));
    let classic = matches!(
        command,
        Command::Subscribe(_)
            | Command::Unsubscribe(_)
            | Command::PSubscribe(_)
            | Command::PUnsubscribe(_)
    );

    let owned: Vec<Vec<u8>> = channels
        .iter()
        .map(|channel| channel.as_ref().to_vec())
        .collect();
    let (wire, active) = {
        let mut hub = state.pubsub.borrow_mut();

        if classic {
            let kind = if pattern {
                crate::pubsub::ClassicSub::Pattern
            } else {
                crate::pubsub::ClassicSub::Channel
            };

            crate::pubsub::classic_subscription_wire(&mut hub, session.id, &owned, kind, subscribe)
        } else {
            crate::pubsub::subscription_wire(&mut hub, session.id, &owned, subscribe)
        }
    };

    session.in_pubsub = active;
    refresh_notify_listeners(state);
    output.write_all(&wire)?;
    Ok(true)
}
