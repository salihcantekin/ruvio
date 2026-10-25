use super::*;

pub(in crate::protocol) fn parse_float(bytes: &[u8]) -> Result<f64, ProtocolError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::Invalid("value is not a valid float"))?;
    let value = text
        .parse::<f64>()
        .map_err(|_| ProtocolError::Invalid("value is not a valid float"))?;

    if !value.is_finite() {
        return Err(ProtocolError::Invalid("value is not a valid float"));
    }

    Ok(value)
}

pub(in crate::protocol) fn parse_f64(bytes: &[u8]) -> Result<f64, ProtocolError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::Invalid("score is not valid ASCII"))?;
    let value = text
        .parse::<f64>()
        .map_err(|_| ProtocolError::Invalid("score is not a number"))?;

    if value.is_nan() {
        return Err(ProtocolError::Invalid("score cannot be NaN"));
    }

    Ok(value)
}

pub(in crate::protocol) fn parse_timeout_ms(bytes: &[u8]) -> Result<Option<u64>, ProtocolError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::Invalid("timeout is not a valid float"))?;
    let seconds: f64 = text
        .parse()
        .map_err(|_| ProtocolError::Invalid("timeout is not a valid float"))?;

    // NaN compares false both ways and would become a 0 ms timeout below.
    if !seconds.is_finite() {
        return Err(ProtocolError::Invalid("timeout is not a valid float"));
    }

    if seconds < 0.0 {
        return Err(ProtocolError::Invalid("timeout is negative"));
    }

    if seconds == 0.0 {
        return Ok(None);
    }

    Ok(Some((seconds * 1_000.0).ceil() as u64))
}

/// The most keys, members or fields one `SCAN`, `HSCAN`, `SSCAN` or `ZSCAN` call examines; a larger
/// `COUNT` is lowered to it. One call runs on the shard's thread, so it must stay short.
pub const MAX_SCAN_COUNT: usize = 10_000;

pub(in crate::protocol) fn parse_scan<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("SCAN expects a cursor"));
    }

    let cursor = parse_u64(args.bytes(0))?;
    let mut pattern = None;
    let mut count = 10;
    let mut type_name = None;
    let mut slot = None;
    let mut index = 1;

    while index < args.len() {
        let name = args.bytes(index);

        if index + 1 >= args.len() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        if name.eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(args.arg(index + 1));
        } else if name.eq_ignore_ascii_case(b"COUNT") {
            let parsed = parse_i64(args.bytes(index + 1))?;

            count = usize::try_from(parsed)
                .map_err(|_| ProtocolError::Invalid("value is not an integer"))?
                .min(MAX_SCAN_COUNT);
        } else if name.eq_ignore_ascii_case(b"TYPE") {
            type_name = Some(args.arg(index + 1));
        } else if name.eq_ignore_ascii_case(b"SLOT") {
            slot = Some(
                std::str::from_utf8(args.bytes(index + 1))
                    .ok()
                    .and_then(|value| value.parse::<u16>().ok())
                    .filter(|&value| usize::from(value) < crate::routing::HASH_SLOT_COUNT)
                    .ok_or(ProtocolError::Invalid("slot is out of range (0-16383)"))?,
            );
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 2;
    }

    Ok(Command::Scan {
        cursor,
        pattern,
        count,
        type_name,
        slot,
    })
}

pub(in crate::protocol) fn parse_u64(bytes: &[u8]) -> Result<u64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(ProtocolError::Invalid("invalid cursor"))
}

/// Redis glob: `*`, `?`, `[abc]`, `[^a-z]` and `\x` escapes.
pub(crate) fn glob_match(pattern: &[u8], value: &[u8]) -> bool {
    let mut pattern_index = 0;
    let mut value_index = 0;
    let mut star = None;

    while value_index < value.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star = Some((pattern_index, value_index));
            pattern_index += 1;

            continue;
        }

        if pattern_index < pattern.len() {
            let (matched, next) = glob_token(pattern, pattern_index, value[value_index]);

            if matched {
                pattern_index = next;
                value_index += 1;

                continue;
            }
        }

        let Some((star_pattern, star_value)) = star else {
            return false;
        };

        pattern_index = star_pattern + 1;
        value_index = star_value + 1;
        star = Some((star_pattern, value_index));
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

/// Matches one non-star pattern token against `byte`; returns the verdict and the next index.
fn glob_token(pattern: &[u8], index: usize, byte: u8) -> (bool, usize) {
    match pattern[index] {
        b'?' => (true, index + 1),
        b'\\' if index + 1 < pattern.len() => (pattern[index + 1] == byte, index + 2),
        b'[' => {
            let mut cursor = index + 1;
            let negate = pattern.get(cursor) == Some(&b'^');
            let mut matched = false;

            if negate {
                cursor += 1;
            }

            while cursor < pattern.len() && pattern[cursor] != b']' {
                if pattern[cursor] == b'\\' && cursor + 1 < pattern.len() {
                    matched |= pattern[cursor + 1] == byte;
                    cursor += 2;
                } else if cursor + 2 < pattern.len()
                    && pattern[cursor + 1] == b'-'
                    && pattern[cursor + 2] != b']'
                {
                    let (low, high) = (pattern[cursor], pattern[cursor + 2]);

                    matched |= (low.min(high)..=low.max(high)).contains(&byte);
                    cursor += 3;
                } else {
                    matched |= pattern[cursor] == byte;
                    cursor += 1;
                }
            }

            (matched != negate, (cursor + 1).min(pattern.len()))
        }
        literal => (literal == byte, index + 1),
    }
}

pub(in crate::protocol) fn parse_set_options(
    arguments: &[Bytes<'_>],
) -> Result<SetOptions, ProtocolError> {
    let mut options = SetOptions::default();
    let mut seen_expiry = false;
    let mut seen_condition = false;
    let mut index = 0;

    while index < arguments.len() {
        let name = arguments[index].as_ref();

        if name.eq_ignore_ascii_case(b"NX") || name.eq_ignore_ascii_case(b"XX") {
            if seen_condition {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = if name.eq_ignore_ascii_case(b"NX") {
                SetCondition::NotExists
            } else {
                SetCondition::Exists
            };

            seen_condition = true;
            index += 1;
            continue;
        }

        if name.eq_ignore_ascii_case(b"KEEPTTL") {
            if seen_expiry {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.expiry = SetExpiry::KeepTtl;
            seen_expiry = true;
            index += 1;
            continue;
        }

        if name.eq_ignore_ascii_case(b"EX")
            || name.eq_ignore_ascii_case(b"PX")
            || name.eq_ignore_ascii_case(b"EXAT")
            || name.eq_ignore_ascii_case(b"PXAT")
        {
            if seen_expiry {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let Some(raw) = arguments.get(index + 1) else {
                return Err(ProtocolError::Invalid("syntax error"));
            };

            let value = parse_i64(raw)?;

            options.expiry = if name.eq_ignore_ascii_case(b"EX") {
                SetExpiry::RelativeMs(
                    value
                        .checked_mul(1_000)
                        .ok_or(ProtocolError::Invalid("invalid expire time"))?,
                )
            } else if name.eq_ignore_ascii_case(b"PX") {
                SetExpiry::RelativeMs(value)
            } else if name.eq_ignore_ascii_case(b"EXAT") {
                SetExpiry::AbsoluteMs(
                    value
                        .checked_mul(1_000)
                        .ok_or(ProtocolError::Invalid("invalid expire time"))?,
                )
            } else {
                SetExpiry::AbsoluteMs(value)
            };

            seen_expiry = true;
            index += 2;
            continue;
        }

        if name.eq_ignore_ascii_case(b"GET") {
            if options.get {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.get = true;
            index += 1;
            continue;
        }

        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(options)
}

/// Redis's integer rule (`string2ll`): an optional `-`, then digits with no leading zero
/// (`0` alone is fine). No `+`, no spaces, no `-0`, no overflow. Every integer argument
/// goes through here, so the same text is a number to every command.
pub(crate) fn redis_integer(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, bytes),
    };

    match digits {
        [] => return None,
        [b'0'] => return (!negative).then_some(0),
        [b'0', ..] => return None,
        _ => {}
    }

    digits.iter().try_fold(0i64, |value, &byte| {
        if !byte.is_ascii_digit() {
            return None;
        }

        let digit = i64::from(byte - b'0');
        let value = value.checked_mul(10)?;

        if negative {
            value.checked_sub(digit)
        } else {
            value.checked_add(digit)
        }
    })
}

pub(in crate::protocol) fn parse_i64(bytes: &[u8]) -> Result<i64, ProtocolError> {
    redis_integer(bytes).ok_or(ProtocolError::Invalid("value is not an integer"))
}

pub(in crate::protocol) fn parse_isize(bytes: &[u8]) -> Result<isize, ProtocolError> {
    redis_integer(bytes)
        .and_then(|value| isize::try_from(value).ok())
        .ok_or(ProtocolError::Invalid("index is not an integer"))
}

pub(in crate::protocol) fn parse_usize(bytes: &[u8]) -> Result<usize, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::Invalid("empty length"));
    }

    bytes.iter().try_fold(0usize, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err(ProtocolError::Invalid("length is not an unsigned integer"));
        }

        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(byte - b'0')))
            .ok_or(ProtocolError::LimitExceeded("length overflow"))
    })
}
