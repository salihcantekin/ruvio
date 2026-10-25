use super::*;

/// Largest absolute field deadline Redis accepts: 2^46 - 1 milliseconds.
pub(crate) const MAX_FIELD_EXPIRE_MS: u64 = (1 << 46) - 1;

pub(in crate::protocol) fn hash_expire_name(name: &[u8]) -> &'static str {
    if name.eq_ignore_ascii_case(b"HEXPIRE") {
        "hexpire"
    } else if name.eq_ignore_ascii_case(b"HPEXPIRE") {
        "hpexpire"
    } else if name.eq_ignore_ascii_case(b"HEXPIREAT") {
        "hexpireat"
    } else {
        "hpexpireat"
    }
}

/// Reads `FIELDS numfields field ...` starting at argument `index`. Every argument must be
/// consumed.
pub(in crate::protocol) fn parse_field_block<'a>(
    args: &Args<'a, '_>,
    index: usize,
    wrong_arity: &'static str,
) -> Result<Vec<Bytes<'a>>, ProtocolError> {
    if args.len() < index + 3 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    if !args.bytes(index).eq_ignore_ascii_case(b"FIELDS") {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let count = parse_usize(args.bytes(index + 1))?;

    if count == 0 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    match (args.len() - (index + 2)).cmp(&count) {
        std::cmp::Ordering::Less => Err(ProtocolError::Invalid("wrong number of arguments")),
        std::cmp::Ordering::Greater => Err(ProtocolError::Invalid("unknown argument")),
        std::cmp::Ordering::Equal => Ok(args.rest(index + 2)),
    }
}

pub(in crate::protocol) fn parse_hash_expire<'a>(args: &Args<'a, '_>, name: &[u8]) -> Parsed<'a> {
    let command = hash_expire_name(name);
    let wrong_arity = match command {
        "hexpire" => "wrong number of arguments for 'hexpire' command",
        "hpexpire" => "wrong number of arguments for 'hpexpire' command",
        "hexpireat" => "wrong number of arguments for 'hexpireat' command",
        _ => "wrong number of arguments for 'hpexpireat' command",
    };

    if args.len() < 5 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    let raw = parse_i64(args.bytes(1))?;

    if raw < 0 {
        return Err(ProtocolError::Invalid("invalid expire time, must be >= 0"));
    }

    let millis = if command == "hexpire" || command == "hexpireat" {
        (raw as u64).checked_mul(1_000)
    } else {
        Some(raw as u64)
    };

    let invalid = match command {
        "hexpire" => "invalid expire time in 'hexpire' command",
        "hpexpire" => "invalid expire time in 'hpexpire' command",
        "hexpireat" => "invalid expire time in 'hexpireat' command",
        _ => "invalid expire time in 'hpexpireat' command",
    };

    let millis = millis
        .filter(|millis| *millis <= MAX_FIELD_EXPIRE_MS)
        .ok_or(ProtocolError::Invalid(invalid))?;
    let time = if command.ends_with("at") {
        FieldExpireTime::AtMillis(millis)
    } else {
        FieldExpireTime::InMillis(millis)
    };

    let mut condition = FieldExpireCondition::Always;
    let mut index = 2;

    while index < args.len() && !args.bytes(index).eq_ignore_ascii_case(b"FIELDS") {
        let flag = args.bytes(index);
        let parsed = if flag.eq_ignore_ascii_case(b"NX") {
            FieldExpireCondition::Nx
        } else if flag.eq_ignore_ascii_case(b"XX") {
            FieldExpireCondition::Xx
        } else if flag.eq_ignore_ascii_case(b"GT") {
            FieldExpireCondition::Gt
        } else if flag.eq_ignore_ascii_case(b"LT") {
            FieldExpireCondition::Lt
        } else {
            return Err(ProtocolError::Invalid("unknown argument"));
        };

        if condition != FieldExpireCondition::Always {
            return Err(ProtocolError::Invalid("Multiple condition flags specified"));
        }

        condition = parsed;
        index += 1;
    }

    let fields = parse_field_block(args, index, wrong_arity)?;

    Ok(Command::HashExpire {
        key: args.arg(0),
        time,
        condition,
        fields,
        name: command,
    })
}

pub(in crate::protocol) fn parse_hash_field_read<'a>(
    args: &Args<'a, '_>,
    name: &[u8],
) -> Parsed<'a> {
    let (reply, wrong_arity) = if name.eq_ignore_ascii_case(b"HTTL") {
        (
            Some(FieldTtlReply::Seconds),
            "wrong number of arguments for 'httl' command",
        )
    } else if name.eq_ignore_ascii_case(b"HPTTL") {
        (
            Some(FieldTtlReply::Millis),
            "wrong number of arguments for 'hpttl' command",
        )
    } else if name.eq_ignore_ascii_case(b"HEXPIRETIME") {
        (
            Some(FieldTtlReply::AtSeconds),
            "wrong number of arguments for 'hexpiretime' command",
        )
    } else if name.eq_ignore_ascii_case(b"HPEXPIRETIME") {
        (
            Some(FieldTtlReply::AtMillis),
            "wrong number of arguments for 'hpexpiretime' command",
        )
    } else {
        (None, "wrong number of arguments for 'hpersist' command")
    };

    if args.len() < 4 {
        return Err(ProtocolError::Invalid(wrong_arity));
    }

    let fields = parse_field_block(args, 1, wrong_arity)?;
    let key = args.arg(0);

    Ok(match reply {
        Some(reply) => Command::HashTtl { key, reply, fields },
        None => Command::HashPersist { key, fields },
    })
}

pub(in crate::protocol) fn parse_hash_scan<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let (key, cursor, pattern, count) =
        parse_scan_arguments(args, "HSCAN expects a key and a cursor")?;

    Ok(Command::HashScan {
        key,
        cursor,
        pattern,
        count,
    })
}

pub(in crate::protocol) fn parse_collection_scan<'a>(
    args: &Args<'a, '_>,
    kind: CollectionScanKind,
) -> Parsed<'a> {
    let usage = match kind {
        CollectionScanKind::Set => "SSCAN expects a key and a cursor",
        CollectionScanKind::SortedSet => "ZSCAN expects a key and a cursor",
    };
    let (key, cursor, pattern, count) = parse_scan_arguments(args, usage)?;

    Ok(Command::CollectionScan {
        kind,
        key,
        cursor,
        pattern,
        count,
    })
}

type ScanArguments<'a> = (Bytes<'a>, u64, Option<Bytes<'a>>, usize);

fn parse_scan_arguments<'a>(
    args: &Args<'a, '_>,
    usage: &'static str,
) -> Result<ScanArguments<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(usage));
    }

    let cursor = parse_u64(args.bytes(1))?;
    let mut pattern = None;
    let mut count = 10;
    let mut index = 2;

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
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 2;
    }

    Ok((args.arg(0), cursor, pattern, count))
}
