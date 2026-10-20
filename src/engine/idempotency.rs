use super::*;
use crate::protocol::{LimitMode, MAX_IDEMPOTENCY_RESULT_BYTES, MAX_IDEMPOTENCY_TTL_MS};
use crate::store::LimitError;

// Native state machine rather than a Lua script: the record is replicated and recovered like
// any other mutation, so no script runs during replay (docs/IDEM.md).

const MAGIC: &[u8; 8] = b"RVIDEM01";
const HEADER: usize = 11;
const INVALID: &[u8] = b"ERR invalid idempotency record or missing expiration";
const INPUT: &[u8] = b"ERR invalid idempotency token, retention or result size";

struct Record<'a> {
    fingerprint: &'a [u8],
    owner: &'a [u8],
    result: Option<&'a [u8]>,
}

impl<'a> Record<'a> {
    fn decode(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < HEADER || &bytes[..8] != MAGIC || bytes[8] > 1 {
            return None;
        }
        let fingerprint_len = usize::from(bytes[9]);
        let owner_len = usize::from(bytes[10]);
        let end = HEADER + fingerprint_len + owner_len;
        if !(1..=128).contains(&fingerprint_len)
            || !(1..=128).contains(&owner_len)
            || bytes.len() < end
            || bytes.len() - end > MAX_IDEMPOTENCY_RESULT_BYTES
            || (bytes[8] == 0 && bytes.len() != end)
        {
            return None;
        }
        Some(Self {
            fingerprint: &bytes[HEADER..HEADER + fingerprint_len],
            owner: &bytes[HEADER + fingerprint_len..end],
            result: (bytes[8] == 1).then_some(&bytes[end..]),
        })
    }
}

fn tokens_valid(fingerprint: &[u8], owner: &[u8]) -> bool {
    (1..=128).contains(&fingerprint.len()) && (1..=128).contains(&owner.len())
}

pub(super) fn begin<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    fingerprint: &[u8],
    owner: &[u8],
    ttl_ms: u64,
) -> Result<Response<'a>, S::Error> {
    if !tokens_valid(fingerprint, owner) || ttl_ms == 0 || ttl_ms > MAX_IDEMPOTENCY_TTL_MS {
        return Ok(Response::Error(INPUT));
    }
    if shard.contains_key(key) {
        if shard.expires_at(key).is_none() {
            return Ok(Response::Error(INVALID));
        }
        let bytes = match shard.value_view(key) {
            Ok(Some(ByteView::Borrowed(bytes))) => bytes,
            _ => return Ok(wrong_type()),
        };
        let Some(record) = Record::decode(bytes) else {
            return Ok(Response::Error(INVALID));
        };
        return Ok(if record.fingerprint != fingerprint {
            state(b"conflict")
        } else if let Some(result) = record.result {
            Response::Array(vec![
                Some(Cow::Borrowed(b"completed")),
                Some(Cow::Borrowed(result)),
            ])
        } else {
            state(b"pending")
        });
    }
    let Some(deadline) = shard
        .now_unix_millis()
        .checked_add(ttl_ms)
        .map(crate::UnixMillis::get)
    else {
        return Ok(Response::Error(INPUT));
    };
    publish(
        shard,
        sink,
        key,
        fingerprint,
        owner,
        None,
        deadline,
        state(b"acquired"),
    )
}

pub(super) fn complete<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    fingerprint: &[u8],
    owner: &[u8],
    result: &[u8],
) -> Result<Response<'a>, S::Error> {
    if !tokens_valid(fingerprint, owner) || result.len() > MAX_IDEMPOTENCY_RESULT_BYTES {
        return Ok(Response::Error(INPUT));
    }
    if !shard.contains_key(key) {
        return Ok(Response::Integer(0));
    }
    let Some(deadline) = shard.expires_at(key) else {
        return Ok(Response::Error(INVALID));
    };
    let bytes = match shard.value_view(key) {
        Ok(Some(ByteView::Borrowed(bytes))) => bytes,
        _ => return Ok(wrong_type()),
    };
    let Some(record) = Record::decode(bytes) else {
        return Ok(Response::Error(INVALID));
    };
    if record.fingerprint != fingerprint || record.owner != owner {
        return Ok(Response::Integer(0));
    }
    if let Some(stored) = record.result {
        return Ok(Response::Integer(i64::from(stored == result)));
    }
    publish(
        shard,
        sink,
        key,
        fingerprint,
        owner,
        Some(result),
        deadline.get(),
        Response::Integer(1),
    )
}

#[allow(clippy::too_many_arguments)]
fn publish<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    fingerprint: &[u8],
    owner: &[u8],
    result: Option<&[u8]>,
    expires_at: u64,
    response: Response<'a>,
) -> Result<Response<'a>, S::Error> {
    let length = HEADER + fingerprint.len() + owner.len() + result.map_or(0, <[u8]>::len);
    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key],
        key.len() + length + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&[
        u8::from(result.is_some()),
        fingerprint.len() as u8,
        owner.len() as u8,
    ]);
    bytes.extend_from_slice(fingerprint);
    bytes.extend_from_slice(owner);
    if let Some(result) = result {
        bytes.extend_from_slice(result);
    }
    sink.record(Mutation::IdempotencyPut {
        key,
        value: &bytes,
        expires_at,
    })?;
    shard.set_bytes(key, &bytes);
    shard.expire_at(key, UnixMillis::new(expires_at));
    Ok(response)
}

fn state(value: &'static [u8]) -> Response<'static> {
    Response::Array(vec![Some(Cow::Borrowed(value))])
}

const LIMIT_MODE_ERROR: &[u8] = b"ERR LIMIT key was created in another mode";
const LIMIT_RANGE_ERROR: &[u8] = b"ERR LIMIT values do not fit the stored state";

#[inline(always)]
pub(super) fn limit<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    max: u64,
    window_ms: u64,
    mode: LimitMode,
) -> Result<Response<'a>, S::Error> {
    let (planned, with_retry) = match mode {
        LimitMode::Fixed => (shard.plan_limit(&key, max, window_ms), false),
        LimitMode::Sliding { with_retry } => {
            (shard.plan_limit_sliding(&key, max, window_ms), with_retry)
        }
        LimitMode::Tokens { cost, with_retry } => (
            shard.plan_limit_tokens(&key, max, window_ms, u64::from(cost)),
            with_retry,
        ),
    };
    let planned = match planned {
        Ok(planned) => planned,
        Err(LimitError::WrongType) => return Ok(wrong_type()),
        Err(LimitError::Mode) => return Ok(Response::Error(LIMIT_MODE_ERROR)),
        Err(LimitError::Range) => return Ok(Response::Error(LIMIT_RANGE_ERROR)),
    };

    let crate::store::LimitDecision::Allow {
        used,
        window_end,
        remaining,
        fresh,
    } = planned
    else {
        return Ok(match planned {
            crate::store::LimitDecision::RejectRetry { retry_after_ms } if with_retry => {
                retry_reply(-1, retry_after_ms)
            }
            _ => Response::Bulk(None),
        });
    };

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        if fresh {
            key.len() + ADMISSION_OVERHEAD_BYTES
        } else {
            0
        },
    )? {
        return Ok(rejected);
    }

    match window_end {
        // A new window, or a moved deadline: the state and its end in one record.
        Some(expires_at) => sink.record(Mutation::Transaction {
            mutations: &[
                OwnedMutation::LimitPut {
                    key: key.to_vec(),
                    used,
                    expires_at: None,
                },
                OwnedMutation::ExpireAt {
                    key: key.to_vec(),
                    unix_millis: expires_at,
                },
            ],
        })?,
        None => sink.record(Mutation::LimitPut { key: &key, used })?,
    }

    if shard.put_limit(&key, used).is_err() {
        debug_assert!(false, "limit put failed after validation");

        return Ok(wrong_type());
    }

    if let Some(expires_at) = window_end {
        shard.expire_at(&key, UnixMillis::new(expires_at));
    }

    if with_retry {
        return Ok(retry_reply(i64::try_from(remaining).unwrap_or(i64::MAX), 0));
    }

    Ok(Response::Integer(
        i64::try_from(remaining).unwrap_or(i64::MAX),
    ))
}

/// `[remaining, retry_after_ms]`; `remaining` is -1 for a refused call.
fn retry_reply(remaining: i64, retry_after_ms: u64) -> Response<'static> {
    Response::Exec(vec![
        Response::Integer(remaining),
        Response::Integer(retry_after_ms as i64),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::TestClock;

    #[derive(Default)]
    struct Log {
        records: Vec<OwnedMutation>,
        fail: bool,
    }
    impl MutationSink for Log {
        type Error = &'static str;
        fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
            if self.fail {
                return Err("WAL unavailable");
            }
            self.records.push(mutation.to_owned());
            Ok(())
        }
    }

    fn reserve<'a>(owner: &'a [u8]) -> Command<'a> {
        Command::IdempotencyBegin {
            key: Cow::Borrowed(b"request"),
            fingerprint: Cow::Borrowed(b"fp"),
            owner: Cow::Borrowed(owner),
            ttl_ms: 1_000,
        }
    }
    fn finish<'a>(owner: &'a [u8], result: &'a [u8]) -> Command<'a> {
        Command::IdempotencyComplete {
            key: Cow::Borrowed(b"request"),
            fingerprint: Cow::Borrowed(b"fp"),
            owner: Cow::Borrowed(owner),
            result: Cow::Borrowed(result),
        }
    }

    #[test]
    fn idempotency_states_are_atomic_and_never_renew_retention() {
        let clock = TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock.clone());
        let mut log = Log::default();
        assert_eq!(
            execute_with_sink(&mut shard, reserve(b"owner"), &mut log).unwrap(),
            state(b"acquired")
        );
        clock.set(1_200);
        assert_eq!(
            execute_with_sink(&mut shard, reserve(b"retry"), &mut log).unwrap(),
            state(b"pending")
        );
        assert_eq!(
            begin(&mut shard, &mut log, b"request", b"other", b"retry", 9_000).unwrap(),
            state(b"conflict")
        );
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"wrong", b"bad"), &mut log).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            complete(&mut shard, &mut log, b"request", b"wrong", b"owner", b"bad").unwrap(),
            Response::Integer(0)
        );
        assert_eq!(log.records.len(), 1);
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b"\0\xffresult"), &mut log).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(shard.pttl(b"request"), 800);
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b"\0\xffresult"), &mut log).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b"changed"), &mut log).unwrap(),
            Response::Integer(0)
        );
        let replay = execute_with_sink(&mut shard, reserve(b"retry"), &mut log).unwrap();
        assert!(matches!(replay, Response::Array(ref values)
            if matches!(&values[1], Some(Cow::Borrowed(value)) if *value == b"\0\xffresult")));
        assert_eq!(log.records.len(), 2);
        clock.set(2_000);
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b"late"), &mut log).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            execute_with_sink(&mut shard, reserve(b"new-owner"), &mut log).unwrap(),
            state(b"acquired")
        );
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b"late"), &mut log).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn idempotency_wal_failure_and_oom_preserve_pending_value_and_deadline() {
        let clock = TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock);
        let mut log = Log {
            fail: true,
            ..Log::default()
        };
        assert!(execute_with_sink(&mut shard, reserve(b"owner"), &mut log).is_err());
        assert!(!shard.contains_key(b"request"));
        log.fail = false;
        execute_with_sink(&mut shard, reserve(b"owner"), &mut log).unwrap();
        let before = shard.get(b"request").unwrap().to_vec();
        log.fail = true;
        assert!(execute_with_sink(&mut shard, finish(b"owner", b"result"), &mut log).is_err());
        assert_eq!(shard.get(b"request"), Some(before.as_slice()));
        assert_eq!(shard.pttl(b"request"), 1_000);
        log.fail = false;
        shard.set_memory_limit(Some(1));
        assert!(matches!(
            execute_with_sink(&mut shard, finish(b"owner", b"result"), &mut log).unwrap(),
            Response::Error(_)
        ));
        assert_eq!(shard.get(b"request"), Some(before.as_slice()));
        assert_eq!(log.records.len(), 1);
    }

    #[test]
    fn idempotency_replay_and_snapshot_preserve_database_and_deadline() {
        let now = 4_000_000_000_000;
        let clock = TestClock::new(now);
        let mut shard = KvShard::with_clock(clock.clone());
        let mut log = Log::default();
        execute_in_database(&mut shard, reserve(b"owner"), &mut log, &mut (), 2).unwrap();
        shard.begin_snapshot(1024 * 1024).unwrap();
        execute_in_database(
            &mut shard,
            finish(b"owner", b"stored"),
            &mut log,
            &mut (),
            2,
        )
        .unwrap();
        let snapshot = shard.finish_snapshot();
        let mut restored = KvShard::with_clock(clock);
        for entry in snapshot {
            restored.restore_snapshot_entry(entry).unwrap();
        }
        assert_eq!(
            execute_in_database(
                &mut restored,
                reserve(b"owner"),
                &mut NoopMutationSink,
                &mut (),
                2
            )
            .unwrap(),
            state(b"pending")
        );
        let mut replica = KvShard::new();
        for record in &log.records {
            crate::persistence::apply_mutation(&mut replica, 2, record).unwrap();
        }
        assert_eq!(
            execute_in_database(
                &mut replica,
                reserve(b"other"),
                &mut NoopMutationSink,
                &mut (),
                2
            )
            .unwrap(),
            Response::Array(vec![
                Some(Cow::Borrowed(b"completed")),
                Some(Cow::Borrowed(b"stored"))
            ])
        );
        assert_eq!(
            replica.expires_at(b"request"),
            Some(UnixMillis::new(now + 1_000))
        );
        replica.enter(0);
        assert!(!replica.contains_key(b"request"));
    }

    #[test]
    fn idempotency_empty_and_maximum_binary_results_and_corrupt_state() {
        let mut shard = KvShard::new();
        let mut sink = NoopMutationSink;
        execute_with_sink(&mut shard, reserve(b"owner"), &mut sink).unwrap();
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", b""), &mut sink).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            execute_with_sink(&mut shard, reserve(b"owner"), &mut sink).unwrap(),
            Response::Array(vec![
                Some(Cow::Borrowed(b"completed")),
                Some(Cow::Borrowed(b""))
            ])
        );
        shard.delete(b"request");
        execute_with_sink(&mut shard, reserve(b"owner"), &mut sink).unwrap();
        let result = vec![255; MAX_IDEMPOTENCY_RESULT_BYTES];
        assert_eq!(
            execute_with_sink(&mut shard, finish(b"owner", &result), &mut sink).unwrap(),
            Response::Integer(1)
        );
        let encoded = shard.get(b"request").unwrap();
        assert_eq!(encoded.len(), HEADER + 2 + 5 + result.len());
        shard.persist(b"request");
        assert!(matches!(
            execute_with_sink(&mut shard, reserve(b"owner"), &mut sink).unwrap(),
            Response::Error(_)
        ));
        shard.set_bytes(b"request", b"not a record");
        shard.expire_at(b"request", UnixMillis::new(4_000_000_000_000));
        assert!(matches!(
            execute_with_sink(&mut shard, reserve(b"owner"), &mut sink).unwrap(),
            Response::Error(_)
        ));
    }

    #[test]
    fn idempotency_protocol_limits_include_packed_metadata() {
        fn wire(args: &[&[u8]]) -> Vec<u8> {
            let mut result = format!("*{}\r\n", args.len()).into_bytes();
            for arg in args {
                result.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
                result.extend_from_slice(arg);
                result.extend_from_slice(b"\r\n");
            }
            result
        }
        for args in [
            vec![b"IDEM".as_slice(), b"k", b"BEGIN", b"fp", b"owner", b"0"],
            vec![
                b"IDEM".as_slice(),
                b"k",
                b"BEGIN",
                b"fp",
                b"owner",
                b"604800001",
            ],
            vec![b"IDEM".as_slice(), b"k", b"BEGIN", b"", b"owner", b"1000"],
            vec![b"IDEM".as_slice(), b"k", b"STEAL", b"fp", b"owner", b"1000"],
        ] {
            assert!(crate::protocol::parse_command(&wire(&args)).is_err());
        }
        let args = wire(&[b"IDEM", b"{tag}k", b"COMPLETE", b"fp", b"owner", b"\0\xff"]);
        let (command, _) = crate::protocol::parse_command(&args).unwrap().unwrap();
        assert_eq!(
            crate::routing::command_keys(&command),
            vec![b"{tag}k".as_slice()]
        );
        let limits = crate::protocol::ProtocolLimits {
            max_value_bytes: 19,
            ..Default::default()
        };
        assert!(crate::protocol::parse_command_with_limits(&args, limits).is_err());
    }
}
