use super::*;

/// `XINFO STREAM`, `XINFO GROUPS`, and `XINFO CONSUMERS`.
pub(in crate::protocol) fn parse_xinfo<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "XINFO expects STREAM, GROUPS, or CONSUMERS",
        ));
    }

    let kind = args.bytes(0);
    // Arguments after the subcommand.
    let rest = args.len() - 1;

    if kind.eq_ignore_ascii_case(b"STREAM") || kind.eq_ignore_ascii_case(b"GROUPS") {
        if rest != 1 {
            return Err(ProtocolError::Invalid("XINFO expects a key"));
        }

        return Ok(Command::StreamInfo {
            key: args.arg(1),
            group: None,
            kind: if kind.eq_ignore_ascii_case(b"STREAM") {
                StreamInfoKind::Stream
            } else {
                StreamInfoKind::Groups
            },
        });
    }

    if kind.eq_ignore_ascii_case(b"CONSUMERS") {
        if rest != 2 {
            return Err(ProtocolError::Invalid(
                "XINFO CONSUMERS expects a key and a group",
            ));
        }

        if args.bytes(2).is_empty() {
            return Err(ProtocolError::Invalid(
                "XINFO CONSUMERS group must not be empty",
            ));
        }

        return Ok(Command::StreamInfo {
            key: args.arg(1),
            group: Some(args.arg(2)),
            kind: StreamInfoKind::Consumers,
        });
    }

    Err(ProtocolError::Invalid(
        "XINFO expects STREAM, GROUPS, or CONSUMERS",
    ))
}

pub(in crate::protocol) fn parse_stream_add_id(bytes: &[u8]) -> Result<StreamAddId, ProtocolError> {
    if bytes == b"*" {
        return Ok(StreamAddId::Auto);
    }

    if let Some(millis) = bytes.strip_suffix(b"-*") {
        if millis.is_empty() || !millis.iter().all(u8::is_ascii_digit) {
            return Err(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"));
        }

        let millis = std::str::from_utf8(millis)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"))?;

        return Ok(StreamAddId::AutoSequence(millis));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes)
        .ok_or(ProtocolError::Invalid("XADD id must be *, ms-*, or ms-seq"))?;

    Ok(StreamAddId::Explicit(millis, sequence))
}

pub(in crate::protocol) fn parse_xadd<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 4 {
        return Err(ProtocolError::Invalid(
            "XADD expects a key, id, and at least one field",
        ));
    }

    let mut max_len = None;
    let mut min_id = None;
    let mut delay_ms = None;
    let mut nomkstream = false;
    let mut ttl = None;
    let mut index = 1;

    while index < args.len() {
        let argument = args.bytes(index);

        if ttl.is_none()
            && let Some(found) = create_ttl_option(args, index)?
        {
            ttl = Some(found);
            index += 2;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"DELAY") {
            if delay_ms.is_some() {
                return Err(ProtocolError::Invalid("XADD DELAY was repeated"));
            }

            index += 1;

            if index >= args.len() {
                return Err(ProtocolError::Invalid("XADD DELAY expects milliseconds"));
            }

            delay_ms = Some(
                parse_u64(args.bytes(index))
                    .map_err(|_| ProtocolError::Invalid("XADD DELAY expects milliseconds"))?,
            );

            index += 1;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"NOMKSTREAM") {
            if nomkstream {
                return Err(ProtocolError::Invalid("XADD NOMKSTREAM was repeated"));
            }

            nomkstream = true;
            index += 1;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"MAXLEN") {
            if max_len.is_some() {
                return Err(ProtocolError::Invalid("XADD MAXLEN was repeated"));
            }

            if min_id.is_some() {
                return Err(ProtocolError::Invalid(
                    "XADD MAXLEN and MINID cannot be used together",
                ));
            }

            index += 1;

            let mut approximate = false;

            if index < args.len() && matches!(args.bytes(index), b"~" | b"=") {
                approximate = args.bytes(index) == b"~";
                index += 1;
            }

            if index >= args.len() {
                return Err(ProtocolError::Invalid("XADD MAXLEN expects a count"));
            }

            max_len = Some(parse_usize(args.bytes(index))?);
            index += 1;
            skip_trim_limit(args, &mut index, approximate)?;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"MINID") {
            if min_id.is_some() {
                return Err(ProtocolError::Invalid("XADD MINID was repeated"));
            }

            if max_len.is_some() {
                return Err(ProtocolError::Invalid(
                    "XADD MAXLEN and MINID cannot be used together",
                ));
            }

            index += 1;
            min_id = Some(parse_min_id(args, &mut index)?);

            continue;
        }

        break;
    }

    if index >= args.len() {
        return Err(ProtocolError::Invalid("XADD expects an id"));
    }

    let id = parse_stream_add_id(args.bytes(index))?;

    index += 1;

    let rest = args.len() - index;

    if rest == 0 || !rest.is_multiple_of(2) {
        return Err(ProtocolError::Invalid("XADD expects field and value pairs"));
    }

    let mut fields = Vec::with_capacity(rest / 2);

    for at in (index..args.len()).step_by(2) {
        fields.push((args.arg(at), args.arg(at + 1)));
    }

    Ok(with_create_ttl(
        ttl,
        Command::StreamAdd {
            key: args.arg(0),
            id,
            max_len,
            min_id,
            delay_ms,
            nomkstream,
            fields,
        },
    ))
}

pub(in crate::protocol) fn parse_xrange<'a>(reverse: bool, args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XRANGE expects a key, start, and end",
        ));
    }

    let first = parse_stream_bound(args.bytes(1))?;
    let second = parse_stream_bound(args.bytes(2))?;
    let (start, end) = if reverse {
        (second, first)
    } else {
        (first, second)
    };

    let count = parse_stream_count(args, 3)?;

    Ok(Command::StreamRange {
        key: args.arg(0),
        start,
        end,
        count,
        reverse,
    })
}

pub(in crate::protocol) fn parse_xread<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let mut count = None;
    let mut block = None;
    let mut saw_streams = false;
    let mut index = 0;

    while index < args.len() {
        let argument = args.bytes(index);

        if argument.eq_ignore_ascii_case(b"BLOCK") {
            index += 1;

            if index >= args.len() {
                return Err(ProtocolError::Invalid("XREAD BLOCK expects milliseconds"));
            }

            block = Some(parse_block_millis(args.bytes(index))?);
            index += 1;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"COUNT") {
            index += 1;

            if index >= args.len() {
                return Err(ProtocolError::Invalid(
                    "XREAD COUNT expects a positive integer",
                ));
            }

            count = Some(parse_positive_count(args.bytes(index))?);
            index += 1;

            continue;
        }

        if argument.eq_ignore_ascii_case(b"STREAMS") {
            index += 1;
            saw_streams = true;

            break;
        }

        return Err(ProtocolError::Invalid("XREAD expects COUNT and STREAMS"));
    }

    let rest = args.len() - index;

    if !saw_streams || rest == 0 || !rest.is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "XREAD expects STREAMS followed by key and id pairs",
        ));
    }

    let half = rest / 2;
    let mut keys = Vec::with_capacity(half);

    for at in index..index + half {
        keys.push((args.arg(at), parse_stream_cursor(args.bytes(at + half))?));
    }

    Ok(Command::StreamRead { count, block, keys })
}

pub(in crate::protocol) fn parse_block_millis(bytes: &[u8]) -> Result<Option<u64>, ProtocolError> {
    let millis = parse_u64(bytes)?;

    if millis == 0 {
        return Ok(None);
    }

    Ok(Some(millis))
}

pub(in crate::protocol) fn parse_xdel<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("XDEL expects a key and an id"));
    }

    let mut ids = Vec::with_capacity(args.len() - 1);

    for index in 1..args.len() {
        let (millis, sequence) = crate::stream::parse_id(args.bytes(index)).ok_or(
            ProtocolError::Invalid("XDEL expects ids of the form ms-seq"),
        )?;

        ids.push((millis, sequence));
    }

    Ok(Command::StreamDelete {
        key: args.arg(0),
        ids,
    })
}

/// An optional `COUNT n` from argument `from` to the end.
pub(in crate::protocol) fn parse_stream_count(
    args: &Args<'_, '_>,
    from: usize,
) -> Result<Option<usize>, ProtocolError> {
    let rest = args.len() - from;

    if rest == 0 {
        return Ok(None);
    }

    if rest == 2 && args.bytes(from).eq_ignore_ascii_case(b"COUNT") {
        return Ok(Some(parse_positive_count(args.bytes(from + 1))?));
    }

    Err(ProtocolError::Invalid("stream range only supports COUNT"))
}

pub(in crate::protocol) fn parse_positive_count(bytes: &[u8]) -> Result<usize, ProtocolError> {
    let count = parse_usize(bytes)?;

    if count == 0 {
        return Err(ProtocolError::Invalid("COUNT must be positive"));
    }

    Ok(count)
}

pub(in crate::protocol) fn parse_stream_cursor(
    bytes: &[u8],
) -> Result<crate::stream::StreamCursor, ProtocolError> {
    if bytes == b"$" {
        return Ok(crate::stream::StreamCursor::End);
    }

    if bytes == b"0" {
        return Ok(crate::stream::StreamCursor::After(0, 0));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes)
        .ok_or(ProtocolError::Invalid("XREAD id must be $, 0, or ms-seq"))?;

    Ok(crate::stream::StreamCursor::After(millis, sequence))
}

/// `[~|=] id` at argument `*index`, which is moved past what was read.
pub(in crate::protocol) fn parse_min_id(
    args: &Args<'_, '_>,
    index: &mut usize,
) -> Result<(u64, u64), ProtocolError> {
    let mut approximate = false;

    if *index < args.len() && matches!(args.bytes(*index), b"~" | b"=") {
        approximate = args.bytes(*index) == b"~";
        *index += 1;
    }

    if *index >= args.len() {
        return Err(ProtocolError::Invalid("MINID expects an id"));
    }

    let id = crate::stream::parse_id(args.bytes(*index))
        .ok_or(ProtocolError::Invalid("MINID id must be ms-seq"))?;

    *index += 1;
    skip_trim_limit(args, index, approximate)?;

    Ok(id)
}

/// An optional `LIMIT count` after a trim threshold, as Redis allows it with `~`. It bounds the
/// work of an approximate trim; Ruvio always trims to the threshold, so the count is read and
/// ignored.
fn skip_trim_limit(
    args: &Args<'_, '_>,
    index: &mut usize,
    approximate: bool,
) -> Result<(), ProtocolError> {
    if *index >= args.len() || !args.bytes(*index).eq_ignore_ascii_case(b"LIMIT") {
        return Ok(());
    }

    if !approximate {
        return Err(ProtocolError::Invalid(
            "syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }

    if *index + 1 >= args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    parse_usize(args.bytes(*index + 1))?;
    *index += 2;
    Ok(())
}

pub(in crate::protocol) fn parse_xsetid<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid("XSETID expects a key and an id"));
    }

    let id = crate::stream::parse_id(args.bytes(1))
        .ok_or(ProtocolError::Invalid("XSETID id must be ms-seq"))?;

    Ok(Command::StreamSetId {
        key: args.arg(0),
        id,
    })
}

pub(in crate::protocol) fn parse_xtrim<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XTRIM expects a key and MAXLEN or MINID",
        ));
    }

    let key = args.arg(0);
    let strategy = args.bytes(1);
    let mut index = 2;

    if strategy.eq_ignore_ascii_case(b"MINID") {
        let id = parse_min_id(args, &mut index)?;

        if index != args.len() {
            return Err(ProtocolError::Invalid("XTRIM MINID expects one id"));
        }

        return Ok(Command::StreamTrimMinId { key, id });
    }

    if !strategy.eq_ignore_ascii_case(b"MAXLEN") {
        return Err(ProtocolError::Invalid(
            "XTRIM only supports MAXLEN and MINID",
        ));
    }

    let mut approximate = false;

    if matches!(args.bytes(index), b"~" | b"=") {
        approximate = args.bytes(index) == b"~";
        index += 1;
    }

    if index >= args.len() {
        return Err(ProtocolError::Invalid("XTRIM expects MAXLEN and a count"));
    }

    let max_len = parse_usize(args.bytes(index))?;

    index += 1;
    skip_trim_limit(args, &mut index, approximate)?;

    if index != args.len() {
        return Err(ProtocolError::Invalid("XTRIM expects MAXLEN and a count"));
    }

    Ok(Command::StreamTrim { key, max_len })
}

pub(in crate::protocol) fn parse_group_start(
    bytes: &[u8],
) -> Result<crate::stream::GroupStart, ProtocolError> {
    if bytes == b"$" {
        return Ok(crate::stream::GroupStart::End);
    }

    if bytes == b"0" {
        return Ok(crate::stream::GroupStart::Id(0, 0));
    }

    let (millis, sequence) = crate::stream::parse_id(bytes)
        .ok_or(ProtocolError::Invalid("XGROUP id must be $, 0, or ms-seq"))?;

    Ok(crate::stream::GroupStart::Id(millis, sequence))
}

pub(in crate::protocol) fn parse_xgroup<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "XGROUP expects CREATE, CREATECONSUMER, DESTROY, SETID, or DELCONSUMER",
        ));
    }

    let subcommand = args.bytes(0);

    if subcommand.eq_ignore_ascii_case(b"DESTROY") {
        return parse_xgroup_destroy(args);
    }

    if subcommand.eq_ignore_ascii_case(b"SETID") {
        return parse_xgroup_setid(args);
    }

    if subcommand.eq_ignore_ascii_case(b"DELCONSUMER") {
        return parse_xgroup_delconsumer(args);
    }

    if subcommand.eq_ignore_ascii_case(b"CREATECONSUMER") {
        return parse_xgroup_createconsumer(args);
    }

    if !subcommand.eq_ignore_ascii_case(b"CREATE") {
        return Err(ProtocolError::Invalid(
            "XGROUP expects CREATE, CREATECONSUMER, DESTROY, SETID, or DELCONSUMER",
        ));
    }

    // Arguments after the subcommand.
    let rest = args.len() - 1;

    if !(3..=4).contains(&rest) {
        return Err(ProtocolError::Invalid(
            "XGROUP CREATE expects a key, group, id, and optional MKSTREAM",
        ));
    }

    if args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    let mkstream = if rest == 4 {
        if args.bytes(4).eq_ignore_ascii_case(b"MKSTREAM") {
            true
        } else {
            return Err(ProtocolError::Invalid(
                "XGROUP CREATE only supports MKSTREAM",
            ));
        }
    } else {
        false
    };

    Ok(Command::StreamGroupCreate {
        key: args.arg(1),
        group: args.arg(2),
        start: parse_group_start(args.bytes(3))?,
        mkstream,
    })
}

pub(in crate::protocol) fn parse_xreadgroup<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 || !args.bytes(0).eq_ignore_ascii_case(b"GROUP") {
        return Err(ProtocolError::Invalid("XREADGROUP expects GROUP"));
    }

    if args.len() - 1 < 4 {
        return Err(ProtocolError::Invalid(
            "XREADGROUP expects a group, consumer, and one stream",
        ));
    }

    if args.bytes(1).is_empty() || args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XREADGROUP group and consumer must not be empty",
        ));
    }

    let mut count = None;
    let mut block = None;
    let mut noack = false;
    let mut index = 3;

    while index < args.len() && !args.bytes(index).eq_ignore_ascii_case(b"STREAMS") {
        let option = args.bytes(index);

        index += 1;

        if option.eq_ignore_ascii_case(b"COUNT") {
            if index >= args.len() {
                return Err(ProtocolError::Invalid("XREADGROUP COUNT expects a number"));
            }

            count = Some(parse_positive_count(args.bytes(index))?);
            index += 1;
        } else if option.eq_ignore_ascii_case(b"BLOCK") {
            if index >= args.len() {
                return Err(ProtocolError::Invalid(
                    "XREADGROUP BLOCK expects milliseconds",
                ));
            }

            block = Some(parse_block_millis(args.bytes(index))?);
            index += 1;
        } else if option.eq_ignore_ascii_case(b"NOACK") {
            noack = true;
        } else {
            return Err(ProtocolError::Invalid(
                "XREADGROUP only supports COUNT, BLOCK, and NOACK before STREAMS",
            ));
        }
    }

    if index >= args.len() || !args.bytes(index).eq_ignore_ascii_case(b"STREAMS") {
        return Err(ProtocolError::Invalid("XREADGROUP expects STREAMS"));
    }

    index += 1;

    if args.len() - index != 2 {
        return Err(ProtocolError::Invalid("XREADGROUP reads one stream"));
    }

    if args.bytes(index + 1) != b">" {
        return Err(ProtocolError::Invalid(
            "XREADGROUP only reads new messages with >",
        ));
    }

    Ok(Command::StreamGroupRead {
        key: args.arg(index),
        group: args.arg(1),
        consumer: args.arg(2),
        count,
        block,
        noack,
    })
}

/// `XGROUP DESTROY key group`, after the subcommand.
pub(in crate::protocol) fn parse_xgroup_destroy<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() - 1 != 2 {
        return Err(ProtocolError::Invalid(
            "XGROUP DESTROY expects a key and a group",
        ));
    }

    if args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    Ok(Command::StreamGroupDestroy {
        key: args.arg(1),
        group: args.arg(2),
    })
}

/// `XGROUP SETID key group id`, after the subcommand.
pub(in crate::protocol) fn parse_xgroup_setid<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() - 1 != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP SETID expects a key, group, and id",
        ));
    }

    if args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group name must not be empty",
        ));
    }

    Ok(Command::StreamGroupSetId {
        key: args.arg(1),
        group: args.arg(2),
        start: parse_group_start(args.bytes(3))?,
    })
}

/// `XGROUP CREATECONSUMER key group consumer`, after the subcommand.
pub(in crate::protocol) fn parse_xgroup_createconsumer<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() - 1 != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP CREATECONSUMER expects a key, group, and consumer",
        ));
    }

    if args.bytes(2).is_empty() || args.bytes(3).is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group and consumer must not be empty",
        ));
    }

    Ok(Command::StreamGroupCreateConsumer {
        key: args.arg(1),
        group: args.arg(2),
        consumer: args.arg(3),
    })
}

/// `XGROUP DELCONSUMER key group consumer`, after the subcommand.
pub(in crate::protocol) fn parse_xgroup_delconsumer<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() - 1 != 3 {
        return Err(ProtocolError::Invalid(
            "XGROUP DELCONSUMER expects a key, group, and consumer",
        ));
    }

    if args.bytes(2).is_empty() || args.bytes(3).is_empty() {
        return Err(ProtocolError::Invalid(
            "XGROUP group and consumer must not be empty",
        ));
    }

    Ok(Command::StreamGroupDelConsumer {
        key: args.arg(1),
        group: args.arg(2),
        consumer: args.arg(3),
    })
}

pub(in crate::protocol) fn parse_xack<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "XACK expects a key, group, and at least one id",
        ));
    }

    if args.bytes(1).is_empty() {
        return Err(ProtocolError::Invalid("XACK group name must not be empty"));
    }

    let mut ids = Vec::with_capacity(args.len() - 2);

    for index in 2..args.len() {
        let (millis, sequence) = crate::stream::parse_id(args.bytes(index)).ok_or(
            ProtocolError::Invalid("XACK expects ids of the form ms-seq"),
        )?;

        ids.push((millis, sequence));
    }

    Ok(Command::StreamAck {
        key: args.arg(0),
        group: args.arg(1),
        ids,
    })
}

pub(in crate::protocol) fn parse_xpending<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("XPENDING expects a key and a group"));
    }

    if args.bytes(1).is_empty() {
        return Err(ProtocolError::Invalid(
            "XPENDING group name must not be empty",
        ));
    }

    let key = args.arg(0);
    let group = args.arg(1);

    if args.len() == 2 {
        return Ok(Command::StreamPending { key, group });
    }

    let mut min_idle = None;
    let mut index = 2;

    if args.bytes(index).eq_ignore_ascii_case(b"IDLE") {
        index += 1;

        if index >= args.len() {
            return Err(ProtocolError::Invalid(
                "XPENDING IDLE expects a millisecond count",
            ));
        }

        min_idle = Some(parse_u64(args.bytes(index))?);
        index += 1;
    }

    let rest = args.len() - index;

    if !(3..=4).contains(&rest) {
        return Err(ProtocolError::Invalid(
            "XPENDING expects a start id, an end id, a count, and an optional consumer",
        ));
    }

    let start = parse_stream_bound(args.bytes(index))?;
    let end = parse_stream_bound(args.bytes(index + 1))?;
    let count = parse_usize(args.bytes(index + 2))?;
    let consumer = if rest == 3 {
        None
    } else {
        if args.bytes(index + 3).is_empty() {
            return Err(ProtocolError::Invalid(
                "XPENDING consumer must not be empty",
            ));
        }

        Some(args.arg(index + 3))
    };

    Ok(Command::StreamPendingRange {
        key,
        group,
        min_idle,
        start,
        end,
        count,
        consumer,
    })
}

pub(in crate::protocol) fn parse_xautoclaim<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 5 {
        return Err(ProtocolError::Invalid(
            "XAUTOCLAIM expects a key, group, consumer, min-idle time, and start id",
        ));
    }

    let min_idle = parse_u64(args.bytes(3))?;
    let start = crate::stream::parse_id(args.bytes(4))
        .ok_or(ProtocolError::Invalid("XAUTOCLAIM start id must be ms-seq"))?;

    if args.bytes(1).is_empty() || args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XAUTOCLAIM group and consumer must not be empty",
        ));
    }

    let mut count = None;
    let mut just_id = false;
    let mut index = 5;

    while index < args.len() {
        let name = args.bytes(index);

        index += 1;

        if name.eq_ignore_ascii_case(b"COUNT") {
            if count.is_some() || index >= args.len() {
                return Err(ProtocolError::Invalid("XAUTOCLAIM COUNT needs one integer"));
            }

            let value = parse_u64(args.bytes(index))?;

            index += 1;

            if value == 0 {
                return Err(ProtocolError::Invalid(
                    "XAUTOCLAIM COUNT must be at least 1",
                ));
            }

            count = Some(value as usize);
            continue;
        }

        if name.eq_ignore_ascii_case(b"JUSTID") {
            if just_id {
                return Err(ProtocolError::Invalid("XAUTOCLAIM JUSTID was repeated"));
            }

            just_id = true;
            continue;
        }

        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::StreamAutoclaim {
        key: args.arg(0),
        group: args.arg(1),
        consumer: args.arg(2),
        min_idle,
        start,
        count,
        just_id,
    })
}

pub(in crate::protocol) fn parse_xclaim<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 5 {
        return Err(ProtocolError::Invalid(
            "XCLAIM expects a key, group, consumer, min-idle time, and at least one id",
        ));
    }

    let min_idle = parse_u64(args.bytes(3))?;

    if args.bytes(1).is_empty() || args.bytes(2).is_empty() {
        return Err(ProtocolError::Invalid(
            "XCLAIM group and consumer must not be empty",
        ));
    }

    let mut ids = Vec::new();
    let mut options = ClaimOptions::plain();
    let mut parsing_options = false;
    let mut index = 4;

    while index < args.len() {
        let token = args.bytes(index);

        index += 1;

        if !parsing_options && !xclaim_option(token) {
            let (millis, sequence) = crate::stream::parse_id(token).ok_or(
                ProtocolError::Invalid("XCLAIM expects ids of the form ms-seq"),
            )?;

            ids.push((millis, sequence));
            continue;
        }

        parsing_options = true;
        let name = token;

        if name.eq_ignore_ascii_case(b"IDLE") {
            if options.idle.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM IDLE was repeated"));
            }

            if options.time.is_some() {
                return Err(ProtocolError::Invalid(
                    "XCLAIM accepts only one of IDLE or TIME",
                ));
            }

            options.idle = Some(parse_xclaim_u64(args, &mut index, b"IDLE")?);
        } else if name.eq_ignore_ascii_case(b"TIME") {
            if options.time.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM TIME was repeated"));
            }

            if options.idle.is_some() {
                return Err(ProtocolError::Invalid(
                    "XCLAIM accepts only one of IDLE or TIME",
                ));
            }

            options.time = Some(parse_xclaim_u64(args, &mut index, b"TIME")?);
        } else if name.eq_ignore_ascii_case(b"RETRYCOUNT") {
            if options.retry_count.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM RETRYCOUNT was repeated"));
            }

            options.retry_count = Some(parse_xclaim_u64(args, &mut index, b"RETRYCOUNT")?);
        } else if name.eq_ignore_ascii_case(b"FORCE") {
            if options.force {
                return Err(ProtocolError::Invalid("XCLAIM FORCE was repeated"));
            }

            options.force = true;
        } else if name.eq_ignore_ascii_case(b"JUSTID") {
            if options.just_id {
                return Err(ProtocolError::Invalid("XCLAIM JUSTID was repeated"));
            }

            options.just_id = true;
        } else if name.eq_ignore_ascii_case(b"LASTID") {
            if options.last_id.is_some() {
                return Err(ProtocolError::Invalid("XCLAIM LASTID was repeated"));
            }

            if index >= args.len() {
                return Err(ProtocolError::Invalid(
                    "XCLAIM LASTID expects an id of the form ms-seq",
                ));
            }

            let parsed = crate::stream::parse_id(args.bytes(index)).ok_or(
                ProtocolError::Invalid("XCLAIM LASTID expects an id of the form ms-seq"),
            )?;

            index += 1;
            options.last_id = Some(parsed);
        } else {
            return Err(ProtocolError::Invalid("XCLAIM option is not supported"));
        }
    }

    if ids.is_empty() {
        return Err(ProtocolError::Invalid(
            "XCLAIM expects a key, group, consumer, min-idle time, and at least one id",
        ));
    }

    Ok(Command::StreamClaimPending {
        key: args.arg(0),
        group: args.arg(1),
        consumer: args.arg(2),
        min_idle,
        ids,
        options,
    })
}

pub(in crate::protocol) fn xclaim_option(token: &[u8]) -> bool {
    token.eq_ignore_ascii_case(b"IDLE")
        || token.eq_ignore_ascii_case(b"TIME")
        || token.eq_ignore_ascii_case(b"RETRYCOUNT")
        || token.eq_ignore_ascii_case(b"FORCE")
        || token.eq_ignore_ascii_case(b"JUSTID")
        || token.eq_ignore_ascii_case(b"LASTID")
}

/// The number after an `XCLAIM` option at argument `*index`, which is moved past it.
pub(in crate::protocol) fn parse_xclaim_u64(
    args: &Args<'_, '_>,
    index: &mut usize,
    name: &[u8],
) -> Result<u64, ProtocolError> {
    if *index >= args.len() {
        return Err(ProtocolError::Invalid(match name {
            b"IDLE" => "XCLAIM IDLE expects a millisecond count",
            b"TIME" => "XCLAIM TIME expects a unix millisecond time",
            _ => "XCLAIM RETRYCOUNT expects a count",
        }));
    }

    let parsed = parse_u64(args.bytes(*index))?;

    *index += 1;
    Ok(parsed)
}

pub(in crate::protocol) fn parse_stream_bound(
    bytes: &[u8],
) -> Result<crate::stream::StreamBound, ProtocolError> {
    if bytes == b"-" {
        return Ok(crate::stream::StreamBound::Min);
    }

    if bytes == b"+" {
        return Ok(crate::stream::StreamBound::Max);
    }

    let (millis, sequence) = crate::stream::parse_id(bytes)
        .ok_or(ProtocolError::Invalid("stream id must be -, +, or ms-seq"))?;

    Ok(crate::stream::StreamBound::Id(millis, sequence))
}
