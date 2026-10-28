//! `FILLGET` / `FILLSET`: one client fills a missing (or stale) cache entry, everyone
//! else sees `BUSY` or the stale value.

use super::*;
use crate::store::FillState;

fn reply<'a>(status: &'static [u8], payload: Option<Vec<u8>>, token: Option<u64>) -> Response<'a> {
    let mut items = vec![Some(Cow::Borrowed(status)), payload.map(Cow::Owned)];

    if let Some(token) = token {
        items.push(Some(Cow::Owned(token.to_string().into_bytes())));
    }

    Response::Array(items)
}

pub(super) fn fill_get<'a, C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    key: &[u8],
    fill_ms: u64,
    wait_ms: Option<u64>,
) -> Response<'a> {
    let value = match shard.value_view(key) {
        Ok(value) => value.map(|view| view.to_vec()),
        Err(_) => return wrong_type(),
    };

    let Some(value) = value else {
        shard.fill_forget_soft(key);

        return match shard.fill_acquire(key, fill_ms) {
            Ok(Some(token)) => Response::Array(vec![
                Some(Cow::Borrowed(b"FILL")),
                Some(Cow::Owned(token.to_string().into_bytes())),
            ]),
            Ok(None) => match wait_ms {
                // The connection retries until the filler writes, the marker lapses
                // (this caller then becomes the filler) or the wait ends with BUSY.
                Some(wait) => Response::BlockRetry {
                    timeout_ms: Some(wait),
                },
                None => Response::Array(vec![Some(Cow::Borrowed(b"BUSY")), None]),
            },
            Err(()) => Response::Error(b"ERR too many pending fills"),
        };
    };

    match shard.fill_state(key) {
        FillState::Fresh => reply(b"HIT", Some(value), None),
        FillState::Stale => match shard.fill_acquire(key, fill_ms) {
            Ok(Some(token)) => {
                let mut items = reply(b"STALE", Some(value), None);

                if let Response::Array(list) = &mut items {
                    list.push(Some(Cow::Owned(token.to_string().into_bytes())));
                }

                items
            }
            Ok(None) => Response::Array(vec![
                Some(Cow::Borrowed(b"STALE")),
                Some(Cow::Owned(value)),
                None,
            ]),
            Err(()) => Response::Error(b"ERR too many pending fills"),
        },
    }
}

pub(super) fn fill_set<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
    ttl_ms: u64,
    token: u64,
    swr_ms: Option<u64>,
) -> Result<Response<'a>, S::Error> {
    if matches!(shard.value_kind(&key), Some(kind) if kind != ValueKind::Bytes) {
        return Ok(wrong_type());
    }

    // Token fence (LEASE-style): a filler whose marker lapsed must not overwrite the value
    // a newer filler wrote.
    if !shard.fill_token_valid(&key, token) {
        return Ok(Response::Integer(0));
    }

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    let now = shard.now_unix_millis();
    let swr = swr_ms.unwrap_or(0);
    let Some(expires_at) = now.checked_add(ttl_ms.saturating_add(swr)) else {
        return Ok(Response::Error(
            b"ERR invalid expire time in 'fillset' command",
        ));
    };

    sink.record(Mutation::SetBytesExpiring {
        key: &key,
        value: &value,
        expires_at: expires_at.get(),
    })?;

    shard.set_bytes(&key, &value);
    shard.expire_at(&key, expires_at);
    shard.fill_complete(
        &key,
        swr_ms.map(|_| (now.get().saturating_add(ttl_ms), expires_at.get())),
    );

    Ok(Response::Integer(1))
}
