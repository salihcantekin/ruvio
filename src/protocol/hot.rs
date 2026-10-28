use super::*;

/// Parse the allocation-sensitive command subset directly from a network buffer.
pub fn parse_hot_command(bytes: &[u8]) -> Result<HotParse<'_>, ProtocolError> {
    parse_hot_command_with_limits(bytes, ProtocolLimits::default())
}

pub fn parse_hot_command_with_limits(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<HotParse<'_>, ProtocolError> {
    match parse_hot_frame(bytes, limits) {
        // The general parser words the refusal and consumes the frame, so the connection
        // survives an oversized key or value.
        Err(ProtocolError::LimitExceeded(_)) if bytes.first() == Some(&b'*') => {
            Ok(HotParse::Fallback)
        }

        other => other,
    }
}

fn parse_hot_frame(bytes: &[u8], limits: ProtocolLimits) -> Result<HotParse<'_>, ProtocolError> {
    if bytes.is_empty() {
        return Ok(HotParse::Incomplete);
    }

    if bytes[0] != b'*' {
        // Inline command; the general parser handles it.
        return Ok(HotParse::Fallback);
    }

    let mut position = 1;
    let Some(argument_count) = read_length_line(bytes, &mut position)? else {
        return Ok(HotParse::Incomplete);
    };

    if argument_count == 0 {
        return Err(ProtocolError::Invalid("command array cannot be empty"));
    }

    if argument_count > limits.max_arguments {
        return Err(ProtocolError::LimitExceeded("too many arguments"));
    }

    if argument_count > 5 {
        return Ok(HotParse::Fallback);
    }

    // Hot names are 3 to 6 bytes long and start with one of a few letter pairs: any
    // other `$<len>\r\n` or name head goes to the general parser before the name is read.
    let rest = &bytes[position.min(bytes.len())..];
    let mut arguments = [&[][..]; 5];
    let name = match *rest {
        [b'$', length @ b'3'..=b'6', b'\r', b'\n', first, second, ..] => {
            if !hot_name_head(first, second) {
                return Ok(HotParse::Fallback);
            }

            // The header is already checked: take the name when it, its CRLF and the
            // limits are plainly fine, and let `read_hot_bulk` word anything else.
            let length = (length - b'0') as usize;
            let end = position + 4 + length;

            if rest.get(4 + length..6 + length) == Some(b"\r\n")
                && end + 2 <= limits.max_command_bytes
                && length <= limits.max_bulk_bytes()
            {
                let name = &rest[4..4 + length];

                position = end + 2;
                name
            } else {
                let Some(name) = read_hot_bulk(bytes, &mut position, limits)? else {
                    return Ok(HotParse::Incomplete);
                };

                name
            }
        }

        [b'$', length, b'\r', ..] if !matches!(length, b'3'..=b'6') => {
            return Ok(HotParse::Fallback);
        }

        _ => {
            let Some(name) = read_hot_bulk(bytes, &mut position, limits)? else {
                return Ok(HotParse::Incomplete);
            };

            name
        }
    };

    arguments[0] = name;

    // Anything else is parsed once by the full parser. Reading the payload here
    // would scan those bytes twice.

    let Some(kind) = hot_command_kind(name, argument_count) else {
        return Ok(HotParse::Fallback);
    };

    for argument in &mut arguments[1..argument_count] {
        let Some(bulk) = read_hot_bulk(bytes, &mut position, limits)? else {
            return Ok(HotParse::Incomplete);
        };

        *argument = bulk;
    }

    let command = match kind {
        HotKind::Ping if argument_count == 1 => HotCommand::Ping(None),
        HotKind::Ping => {
            validate_value(arguments[1], limits)?;
            HotCommand::Ping(Some(arguments[1]))
        }

        HotKind::Get => {
            validate_key(arguments[1], limits)?;
            HotCommand::Get(arguments[1])
        }

        HotKind::Set => {
            validate_key(arguments[1], limits)?;
            validate_value(arguments[2], limits)?;

            if argument_count == 3 {
                HotCommand::Set(arguments[1], arguments[2], None)
            } else {
                let ex = arguments[3].eq_ignore_ascii_case(b"EX");
                let px = arguments[3].eq_ignore_ascii_case(b"PX");

                if !ex && !px {
                    return Ok(HotParse::Fallback);
                }

                // A bad expiry takes the full parser, which words the error.
                let Ok(parsed) = parse_i64(arguments[4]) else {
                    return Ok(HotParse::Fallback);
                };
                let milliseconds = if ex {
                    match parsed.checked_mul(1_000) {
                        Some(milliseconds) => milliseconds,
                        None => return Ok(HotParse::Fallback),
                    }
                } else {
                    parsed
                };

                HotCommand::Set(arguments[1], arguments[2], Some(milliseconds))
            }
        }

        HotKind::SetEx => {
            validate_key(arguments[1], limits)?;
            validate_value(arguments[3], limits)?;

            // A bad or non-positive expiry takes the full parser, which words the error.
            match parse_i64(arguments[2])
                .ok()
                .filter(|seconds| *seconds > 0)
                .and_then(|seconds| seconds.checked_mul(1_000))
            {
                Some(milliseconds) => HotCommand::SetEx(arguments[1], arguments[3], milliseconds),
                None => return Ok(HotParse::Fallback),
            }
        }

        HotKind::Delete => {
            validate_key(arguments[1], limits)?;
            HotCommand::Delete(arguments[1])
        }

        HotKind::Increment => {
            validate_key(arguments[1], limits)?;
            HotCommand::Increment(arguments[1], 1)
        }

        HotKind::Decrement => {
            validate_key(arguments[1], limits)?;
            HotCommand::Increment(arguments[1], -1)
        }

        HotKind::IncrementBy => {
            validate_key(arguments[1], limits)?;
            let Ok(delta) = parse_i64(arguments[2]) else {
                return Ok(HotParse::Fallback);
            };

            HotCommand::Increment(arguments[1], delta)
        }

        HotKind::DecrementBy => {
            validate_key(arguments[1], limits)?;
            let Some(delta) = parse_i64(arguments[2]).ok().and_then(i64::checked_neg) else {
                return Ok(HotParse::Fallback);
            };

            HotCommand::Increment(arguments[1], delta)
        }
    };

    Ok(HotParse::Complete(command, position))
}

/// A hot command name with an argument count it takes, found once from the name.
#[derive(Clone, Copy)]
enum HotKind {
    Ping,
    Get,
    Set,
    SetEx,
    Delete,
    Increment,
    Decrement,
    IncrementBy,
    DecrementBy,
}

/// The second letter of every hot name, by its first letter's low five bits (both
/// lowercase): DEL/DECR*, GET, INCR*, PING, SET*, UNLINK.
const HOT_SECOND: [u8; 32] = {
    let mut table = [0; 32];

    table[(b'd' & 31) as usize] = b'e';
    table[(b'g' & 31) as usize] = b'e';
    table[(b'i' & 31) as usize] = b'n';
    table[(b'p' & 31) as usize] = b'i';
    table[(b's' & 31) as usize] = b'e';
    table[(b'u' & 31) as usize] = b'n';
    table
};

/// Whether a name starting with these two bytes can be hot. One table read; a non-letter
/// that aliases a pair only costs the full check, and an empty entry (zero) never matches
/// since `second | 0x20` is never zero.
#[inline(always)]
fn hot_name_head(first: u8, second: u8) -> bool {
    HOT_SECOND[(first & 31) as usize] == second | 0x20
}

fn hot_command_kind(name: &[u8], count: usize) -> Option<HotKind> {
    // Dispatch on length first: a name that cannot be hot costs one compare, not ten.
    let is = |word: &[u8]| name.eq_ignore_ascii_case(word);
    let kind = match name.len() {
        3 if is(b"GET") && count == 2 => HotKind::Get,
        3 if is(b"DEL") && count == 2 => HotKind::Delete,
        3 if is(b"SET") && (count == 3 || count == 5) => HotKind::Set,
        4 if is(b"PING") && (count == 1 || count == 2) => HotKind::Ping,
        4 if is(b"INCR") && count == 2 => HotKind::Increment,
        4 if is(b"DECR") && count == 2 => HotKind::Decrement,
        5 if is(b"SETEX") && count == 4 => HotKind::SetEx,
        6 if is(b"UNLINK") && count == 2 => HotKind::Delete,
        6 if is(b"INCRBY") && count == 3 => HotKind::IncrementBy,
        6 if is(b"DECRBY") && count == 3 => HotKind::DecrementBy,
        _ => return None,
    };

    Some(kind)
}

pub(super) fn read_hot_bulk<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    limits: ProtocolLimits,
) -> Result<Option<&'a [u8]>, ProtocolError> {
    if *position == bytes.len() {
        return Ok(None);
    }

    if bytes[*position] != b'$' {
        return Err(ProtocolError::Invalid("expected a bulk string"));
    }

    *position += 1;

    let Some(length) = read_length_line(bytes, position)? else {
        return Ok(None);
    };

    if length > limits.max_bulk_bytes() {
        return Err(ProtocolError::LimitExceeded("bulk string is too large"));
    }

    let Some(payload_end) = position.checked_add(length) else {
        return Err(ProtocolError::LimitExceeded("length overflow"));
    };

    let Some(frame_end) = payload_end.checked_add(2) else {
        return Err(ProtocolError::LimitExceeded("length overflow"));
    };

    if frame_end > limits.max_command_bytes {
        return Err(ProtocolError::LimitExceeded("command is too large"));
    }

    if frame_end > bytes.len() {
        return Ok(None);
    }

    if bytes[payload_end..frame_end] != *b"\r\n" {
        return Err(ProtocolError::Invalid("bulk string must end with CRLF"));
    }

    let argument = &bytes[*position..payload_end];

    *position = frame_end;
    Ok(Some(argument))
}

pub(super) fn checked_frame_add(
    current: usize,
    additional: usize,
    limits: ProtocolLimits,
) -> Result<usize, ProtocolError> {
    let total = current
        .checked_add(additional)
        .ok_or(ProtocolError::LimitExceeded("length overflow"))?;

    if total > limits.max_command_bytes {
        return Err(ProtocolError::LimitExceeded("command is too large"));
    }

    Ok(total)
}
