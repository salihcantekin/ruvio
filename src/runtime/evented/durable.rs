//! `CLIENT DURABILITY`: a connection's replies leave only once enough replicas hold the
//! records written before them on disk.
//!
//! Checked when a connection flushes its replies, and only when it asked for durability: a
//! connection that did not pays nothing. The wait covers every record its shards wrote up to
//! the flush (other clients' too); a flush after which this shard wrote nothing does not wait. On
//! timeout every reply of the batch becomes `-NOREPLICAS`; the writes are applied and stay,
//! as with `WAIT`. The connection counts a record as waited for only once enough replicas
//! hold it, so after a timeout its next replies wait again (and time out again) until they do.

use std::io::{self, Write};
use std::sync::Arc;

use super::ShardState;
use crate::ShutdownToken;
use crate::output::BoundedOutput;
use crate::replication::{DurableAcks, DurableReplies, NOREPLICAS_LINE};
use crate::transaction::TxSession;

pub(super) async fn hold_until_durable(
    state: &ShardState,
    session: &mut TxSession,
    output: &mut BoundedOutput,
    shutdown: &ShutdownToken,
) -> io::Result<()> {
    let Some(durability) = session.durability else {
        return Ok(());
    };

    // A replica refuses writes; nothing it answers depends on its replicas.
    if output.is_empty() || state.role.get() != crate::config::ReplicationRole::Primary {
        return Ok(());
    }

    let shards = super::replica::durable_shards(state, session.forwarded_shards);
    let mut targets: Vec<(Arc<DurableAcks>, u64)> = Vec::with_capacity(shards.len());

    let mut waits_on = Vec::with_capacity(shards.len());
    // Advanced only once the wait succeeds: after a timeout the same records are waited for again.
    let mut own_mark = None;

    for shard in shards {
        let mut wal = shard.wal.lock().unwrap_or_else(|error| error.into_inner());
        let target = wal.last_sequence();

        // This shard wrote nothing since the replies before these left.
        if Arc::ptr_eq(&shard.acks, &state.durable_acks) {
            if target <= session.durable_mark {
                continue;
            }

            own_mark = Some(target);
        }

        // A primary ships only records it synced: sync now instead of on the next
        // `everysec` tick. A failed sync leaves the records unshipped, and the wait times out.
        if shard.acks.reached(target) < durability.replicas {
            let _ = wal.flush();
        }

        targets.push((Arc::clone(&shard.acks), target));
        drop(wal);
        waits_on.push(shard);
    }

    if targets.is_empty() {
        return Ok(());
    }

    let reached = {
        let _waiting = Waiting::new(&waits_on);

        super::replica::wait_for_targets(
            state,
            &targets,
            durability.replicas,
            durability.timeout_ms,
            shutdown,
        )
        .await
    };

    if reached >= durability.replicas {
        if let Some(mark) = own_mark {
            session.durable_mark = mark;
        }

        for (shard, (_, target)) in waits_on.iter().zip(&targets) {
            shard.replies.commit(*target);
        }

        return Ok(());
    }

    let refused = refuse_replies(output);

    output.clear();
    output.write_all(&refused)
}

/// Tells the shards' replicas to sync and acknowledge every batch while it lives.
struct Waiting<'a>(&'a [super::forward::ShardDurability]);

impl<'a> Waiting<'a> {
    fn new(shards: &'a [super::forward::ShardDurability]) -> Self {
        shards.iter().for_each(|shard| shard.replies.begin_wait());

        Self(shards)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0
            .iter()
            .map(|shard| &shard.replies)
            .for_each(|replies: &Arc<DurableReplies>| replies.end_wait());
    }
}

/// `output` with every reply replaced by `-NOREPLICAS`; RESP3 pushes (tracking
/// invalidations) are kept.
fn refuse_replies(output: &[u8]) -> Vec<u8> {
    let mut refused = Vec::new();
    let mut at = 0;

    while at < output.len() {
        let Some(end) = value_end(output, at) else {
            // Not a reply this server wrote; refuse the rest as one.
            push_refusal(&mut refused);
            break;
        };

        if output[at] == b'>' {
            refused.extend_from_slice(&output[at..end]);
        } else {
            push_refusal(&mut refused);
        }

        at = end;
    }

    refused
}

fn push_refusal(refused: &mut Vec<u8>) {
    refused.push(b'-');
    refused.extend_from_slice(NOREPLICAS_LINE);
    refused.extend_from_slice(b"\r\n");
}

/// End of the RESP2 or RESP3 value that starts at `at`.
fn value_end(bytes: &[u8], at: usize) -> Option<usize> {
    let line_end = at
        + bytes
            .get(at..)?
            .windows(2)
            .position(|pair| pair == b"\r\n")?;
    let header = std::str::from_utf8(bytes.get(at + 1..line_end)?).ok();
    let after = line_end + 2;
    let count = || header?.parse::<i64>().ok();

    match bytes.get(at)? {
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Some(after),
        b'$' | b'!' | b'=' => match count()? {
            length if length < 0 => Some(after),
            length => {
                let end = after + usize::try_from(length).ok()? + 2;

                (end <= bytes.len()).then_some(end)
            }
        },
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let count = count()?;

            if count < 0 {
                return Some(after);
            }

            let items = usize::try_from(count).ok()?;
            let items = if matches!(bytes[at], b'%' | b'|') {
                items.checked_mul(2)?
            } else {
                items
            };
            let mut end = after;

            for _ in 0..items {
                end = value_end(bytes, end)?;
            }

            Some(end)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timed_out_batch_refuses_every_reply_and_keeps_pushes() {
        let output = b"+OK\r\n$3\r\nabc\r\n>2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n*2\r\n:1\r\n$-1\r\n%1\r\n+a\r\n:2\r\n";
        let refused = refuse_replies(output);
        let line = b"-NOREPLICAS Not enough good replicas to write.\r\n";
        let mut expected = Vec::new();

        expected.extend_from_slice(line);
        expected.extend_from_slice(line);
        expected.extend_from_slice(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n");
        expected.extend_from_slice(line);
        expected.extend_from_slice(line);

        assert_eq!(refused, expected);
    }

    #[test]
    fn value_end_stops_at_a_cut_reply() {
        assert_eq!(value_end(b"$5\r\nab", 0), None);
        assert_eq!(value_end(b"*2\r\n:1\r\n", 0), None);
        assert_eq!(value_end(b"*-1\r\n", 0), Some(5));
    }
}
