use super::*;

pub fn execute_hot_into(
    shard: &mut KvShard<impl Clock, impl StorageIndex>,
    command: HotCommand<'_>,
    output: &mut impl Write,
) -> io::Result<()> {
    let mut sink = NoopMutationSink;

    match execute_hot_into_with_sink(shard, command, output, &mut sink, b"$-1\r\n") {
        Ok(()) => Ok(()),
        Err(ExecuteError::Io(error)) => Err(error),
        Err(ExecuteError::Mutation(error)) => match error {},
    }
}

pub fn execute_hot_into_with_sink<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    command: HotCommand<'_>,
    output: &mut impl Write,
    sink: &mut S,
    null: &'static [u8],
) -> Result<(), ExecuteError<S::Error>> {
    shard.expire_due();
    shard.enter(0);
    sink.select_database(0);
    let command = match command {
        HotCommand::SetEx(key, value, milliseconds) => {
            HotCommand::Set(key, value, Some(milliseconds))
        }
        other => other,
    };

    match command {
        HotCommand::SetEx(..) => unreachable!("SETEX is turned into SET above"),
        HotCommand::Ping(None) => output.write_all(b"+PONG\r\n")?,
        HotCommand::Ping(Some(message)) => write_bulk(output, message)?,
        HotCommand::Get(key) => {
            if shard.eviction_policy().tracks_recency() {
                // Any live key, not only strings: a read-hot hash must not age from its
                // last write. The read is stamped through the record the lookup found.
                let (view, mark) = shard.value_view_read(key);

                write_get(output, view, null)?;
                shard.mark_read(key, mark);
            } else {
                write_get(output, shard.value_view(key), null)?;
            }
        }

        HotCommand::Set(key, value, px_ms) => {
            let expiry = match px_ms {
                Some(milliseconds) => SetExpiry::RelativeMs(milliseconds),
                None => SetExpiry::None,
            };

            let now = shard.now_unix_millis();
            let deadline = match resolve_set_expiry(shard, key, expiry, now) {
                Ok(deadline) => deadline,
                Err(response) => {
                    write_response(output, response)?;

                    return Ok(());
                }
            };

            if deadline.is_some_and(|expires_at| expires_at <= now) {
                if shard.contains_key(key) {
                    if let Some(rejected) =
                        before_mutation(shard, sink, [key], 0).map_err(ExecuteError::Mutation)?
                    {
                        write_response(output, rejected)?;

                        return Ok(());
                    }

                    let keys = [Cow::Borrowed(key)];

                    sink.record(Mutation::Delete { keys: &keys })
                        .map_err(ExecuteError::Mutation)?;
                    shard.delete(key);
                }

                output.write_all(b"+OK\r\n")?;

                return Ok(());
            }

            if let Some(rejected) = before_mutation(
                shard,
                sink,
                [key],
                key.len() + value.len() + ADMISSION_OVERHEAD_BYTES,
            )
            .map_err(ExecuteError::Mutation)?
            {
                write_response(output, rejected)?;

                return Ok(());
            }

            match deadline {
                Some(expires_at) => sink.record(Mutation::SetBytesExpiring {
                    key,
                    value,
                    expires_at: expires_at.get(),
                }),
                None => sink.record(Mutation::SetBytes { key, value }),
            }
            .map_err(ExecuteError::Mutation)?;

            match deadline {
                Some(expires_at) => shard.set_bytes_expiring(key, value, expires_at),
                None => shard.set_bytes(key, value),
            }

            output.write_all(b"+OK\r\n")?;
        }

        HotCommand::Delete(key) => {
            if !shard.contains_key(key) {
                output.write_all(b":0\r\n")?;

                return Ok(());
            }

            before_mutation_allow_oom(shard, sink, [key], 0).map_err(ExecuteError::Mutation)?;

            let keys = [Cow::Borrowed(key)];

            sink.record(Mutation::Delete { keys: &keys })
                .map_err(ExecuteError::Mutation)?;

            let removed = shard.delete(key);

            output.write_all(if removed { b":1\r\n" } else { b":0\r\n" })?;
        }

        HotCommand::Increment(key, delta) => {
            let hint = shard.key_hint(key);

            match shard.preview_increment_hinted(key, delta, hint) {
                Ok(value) => {
                    if let Some(rejected) =
                        before_mutation(shard, sink, [key], key.len() + ADMISSION_OVERHEAD_BYTES)
                            .map_err(ExecuteError::Mutation)?
                    {
                        write_response(output, rejected)?;

                        return Ok(());
                    }

                    sink.record(Mutation::SetInteger { key, value })
                        .map_err(ExecuteError::Mutation)?;
                    shard
                        .set_integer_hinted(key, value, hint)
                        .expect("increment type was validated before recording");
                    output.write_all(b":")?;
                    write_i64(output, value)?;
                    output.write_all(b"\r\n")?;
                }

                Err(error) => write_response(output, increment_error(error))?,
            }
        }
    }

    Ok(())
}

// Always inlined: the default policy's GET must cost what it did before the split.
#[inline(always)]
fn write_get(
    output: &mut impl Write,
    view: Result<Option<ByteView<'_>>, crate::store::WrongType>,
    null: &'static [u8],
) -> io::Result<()> {
    match view {
        Ok(Some(ByteView::Borrowed(value))) => write_bulk(output, value),
        Ok(Some(ByteView::Integer(value))) => {
            let mut digits = [0u8; 20];
            let len = crate::store::format_integer(value, &mut digits);

            write_bulk(output, &digits[..len])
        }

        Ok(None) => output.write_all(null),
        Err(_) => output
            .write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    }
}
