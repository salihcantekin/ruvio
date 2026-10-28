//! `CHANGES SUBSCRIBE`: streams WAL records to one connection through its outbound queue.

use super::*;
use crate::{
    changes::{self, ChangeCursor, ChangeFork, ChangeLineage, ChangeStart, ChangeSubscription},
    persistence::{FrameCursor, FrameRead, decode_record, read_frames_resuming},
};

const READ_BYTES: usize = 64 * 1024;
/// The queue holds `PUBSUB_QUEUE` frames; stop reading well before it fills.
const QUEUE_HIGH_WATER: usize = crate::pubsub::PUBSUB_QUEUE / 2;
const IDLE_POLL: Duration = Duration::from_millis(20);
const HEARTBEAT: Duration = Duration::from_secs(1);
const FOLLOWS_ANOTHER: &str = "the shard follows another primary; resume on it";

/// Validates the request and starts the stream; returns the cursor the stream begins after.
pub(super) fn start(
    state: &Rc<ShardState>,
    outbound: Outbound,
    subscription: ChangeSubscription,
    resp3: bool,
    shutdown: ShutdownToken,
) -> Result<ChangeCursor, &'static [u8]> {
    if state.role.get() == crate::config::ReplicationRole::Replica {
        return Err(b"ERR CHANGES SUBSCRIBE is only available on a primary");
    }

    let (profile, shard, durable) = {
        let wal = state
            .persistence
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        (wal.profile(), wal.shard_id(), wal.durable())
    };

    if !profile.uses_wal() {
        return Err(b"ERR CHANGES SUBSCRIBE needs a durability profile that keeps a WAL");
    }
    // Only a primary with replicas commits records (`CLIENT DURABILITY`); anywhere else a
    // committed stream would never deliver anything.
    let committed = if subscription.committed {
        if state.role.get() != crate::config::ReplicationRole::Primary {
            return Err(b"ERR CHANGES SUBSCRIBE COMMITTED needs replication on this server");
        }
        Some(Arc::clone(&state.durable_replies))
    } else {
        None
    };

    let lineage = lineage(&state.published_config.persistence.data_dir, shard);
    let now = durable.load(Ordering::Acquire);
    let start = match subscription.from {
        None => ChangeStart::Resume(ChangeCursor {
            generation: Some(lineage.generation),
            shard,
            sequence: now,
        }),
        Some(cursor) => lineage.resume(cursor, shard, now)?,
    };
    let (ChangeStart::Resume(cursor) | ChangeStart::Rewound(cursor)) = start;

    tokio::task::spawn_local(stream(
        Rc::clone(state),
        outbound,
        subscription,
        resp3,
        start,
        Feed {
            durable,
            committed,
            epoch: state.epoch.get(),
        },
        shutdown,
    ));

    Ok(cursor)
}

/// What a stream may read up to: the durable log, and for `COMMITTED` the shard's committed
/// sequence as well.
struct Feed {
    durable: Arc<crate::persistence::DurableSequence>,
    committed: Option<Arc<crate::replication::DurableReplies>>,
    /// The shard's epoch when the stream was asked for.
    epoch: u64,
}

impl Feed {
    fn through(&self) -> u64 {
        let durable = self.durable.load(Ordering::Acquire);

        match &self.committed {
            Some(replies) => durable.min(replies.committed()),
            None => durable,
        }
    }
}

/// The history shard `shard`'s log continues, from its lineage file. A shard that never
/// replicated has none: its log is the root history, generation zero.
fn lineage(data_dir: &std::path::Path, shard: u32) -> ChangeLineage {
    let Some(lineage) = crate::replication::read_lineage(data_dir, shard) else {
        return ChangeLineage {
            generation: 0,
            fork: None,
        };
    };

    ChangeLineage {
        generation: lineage.origin.epoch,
        // A promotion forks one epoch above the history it followed.
        fork: (lineage.parent != [0; crate::replication::HISTORY_LEN]).then(|| ChangeFork {
            generation: lineage.origin.epoch.saturating_sub(1),
            sequence: lineage.fork_sequence,
        }),
    }
}

async fn stream(
    state: Rc<ShardState>,
    outbound: Outbound,
    subscription: ChangeSubscription,
    resp3: bool,
    start: ChangeStart,
    feed: Feed,
    shutdown: ShutdownToken,
) {
    let data_dir = state.published_config.persistence.data_dir.clone();
    let limits = state.published_config.protocol;
    let start = match start {
        ChangeStart::Resume(cursor) => cursor,
        ChangeStart::Rewound(fork) => {
            let mut frame = Vec::new();

            changes::encode_rewound(&mut frame, resp3, fork);
            if !outbound.try_push(frame.into()) {
                outbound.close();
                return;
            }
            fork
        }
    };
    let shard = start.shard;
    let generation = start.generation;
    // A move names a shard of this server; another shard's generation is read when one is seen.
    let generation_of = |to: u32| {
        if to == shard {
            generation.unwrap_or(0)
        } else {
            lineage(&data_dir, to).generation
        }
    };
    let mut position = start.sequence;
    let mut reported = position;
    // Where the last chunk stopped in the log, so the next one does not rescan the segment.
    let mut reader = FrameCursor::default();
    let mut last_report = Instant::now();
    // `REPLICAOF host port` cuts this shard's log back to a fork and continues it with
    // another primary's records, and a promotion after that starts a new generation; past
    // either, the cursors this stream labels would be wrong. The subscriber resumes on the
    // primary the shard now follows, where a cursor past the fork is rewound.
    let followed = || {
        state.role.get() == crate::config::ReplicationRole::Replica
            || state.epoch.get() != feed.epoch
    };

    loop {
        if outbound.closed.get() || shutdown.is_requested() {
            return;
        }
        if followed() {
            resync(&outbound, resp3, FOLLOWS_ANOTHER);
            return;
        }

        let through = feed.through();

        if position >= through || outbound.queue.borrow().len() >= QUEUE_HIGH_WATER {
            if position != reported && last_report.elapsed() >= HEARTBEAT {
                let mut frame = Vec::new();

                changes::encode_cursor(
                    &mut frame,
                    resp3,
                    ChangeCursor {
                        generation,
                        shard,
                        sequence: position,
                    },
                );
                reported = position;
                last_report = Instant::now();

                if !outbound.try_push(frame.into()) {
                    outbound.close();
                    return;
                }
            }

            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }

        let directory = data_dir.clone();
        let after = position;
        let mut cursor = std::mem::take(&mut reader);
        let read = tokio::task::spawn_blocking(move || {
            let read = read_frames_resuming(
                &mut cursor,
                &directory,
                shard,
                after,
                through,
                limits,
                READ_BYTES,
            );
            (read, cursor)
        })
        .await
        .map(|(read, cursor)| {
            reader = cursor;
            read
        });

        // The shard may have changed role while the log was read.
        if followed() {
            resync(&outbound, resp3, FOLLOWS_ANOTHER);
            return;
        }

        let frames = match read {
            Ok(Ok(FrameRead::Frames(frames))) => frames,
            Ok(Ok(FrameRead::Gap | FrameRead::Ahead | FrameRead::Local)) => {
                resync(&outbound, resp3, "cursor is no longer retained");
                return;
            }
            _ => {
                resync(&outbound, resp3, "change log is unreadable");
                return;
            }
        };

        if frames.is_empty() {
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }

        let mut batch = Vec::new();

        for frame in &frames {
            let Ok(record) = decode_record(frame, shard, limits) else {
                resync(&outbound, resp3, "change log is unreadable");
                return;
            };
            let cursor = ChangeCursor {
                generation,
                shard,
                sequence: record.sequence,
            };

            changes::describe(&record.mutation, &generation_of, &mut |event| {
                if subscription.accepts(event.kind, event.key) {
                    // A move's value is the cursor to continue from, never optional.
                    let value = (subscription.values || event.kind == changes::ChangeKind::Moved)
                        .then_some(event.value)
                        .flatten();

                    changes::encode_event(
                        &mut batch,
                        resp3,
                        cursor,
                        record.database,
                        event.kind,
                        event.key,
                        value.as_deref(),
                    );
                }
            });
            position = record.sequence;
        }

        if !batch.is_empty() {
            reported = position;
            last_report = Instant::now();

            if !outbound.try_push(batch.into()) {
                outbound.close();
                return;
            }
        }
    }
}

fn resync(outbound: &Outbound, resp3: bool, reason: &str) {
    let mut frame = Vec::new();

    changes::encode_resync(&mut frame, resp3, reason);
    let _ = outbound.try_push(frame.into());
    // The connection writes queued frames before it honours `closed`, so the notice is sent.
    outbound.close();
}
