//! The row parsers of the command table: each turns [`Args`] into a [`Command`].

use super::*;

pub(super) fn parse_get<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Get(args.arg(0))),
        _ => Err(ProtocolError::Invalid("GET expects one argument")),
    }
}

pub(super) fn parse_set<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("SET expects a key and a value"));
    }

    if args.len() > 2 {
        if args.bytes(2).eq_ignore_ascii_case(b"IFEQ") {
            return parse_set_ifeq(args);
        }

        if args.bytes(2).eq_ignore_ascii_case(b"IFGT") {
            return parse_set_ifcmp(args, true);
        }

        if args.bytes(2).eq_ignore_ascii_case(b"IFLT") {
            return parse_set_ifcmp(args, false);
        }

        let options = parse_set_options(&args.rest(2))?;

        return Ok(Command::Set(args.arg(0), args.arg(1), options));
    }

    Ok(Command::Set(
        args.arg(0),
        args.arg(1),
        SetOptions::default(),
    ))
}

/// `SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]`. `NX`, `XX` and `GET` do not combine
/// with `IFEQ`.
pub(super) fn parse_set_ifeq<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 4 {
        return Err(ProtocolError::Invalid(
            "SET IFEQ expects a value to compare",
        ));
    }

    let options = parse_set_options(&args.rest(4))?;

    if options.get || options.condition != SetCondition::Always {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::SetIfEq {
        key: args.arg(0),
        value: args.arg(1),
        expected: args.arg(3),
        options,
    })
}

/// `SET key value IFGT|IFLT [EX|PX|EXAT|PXAT|KEEPTTL]`: `value` must be a signed 64-bit integer.
/// `NX`, `XX` and `GET` do not combine with it.
pub(super) fn parse_set_ifcmp<'a>(
    args: &Args<'a, '_>,
    greater: bool,
) -> Result<Command<'a>, ProtocolError> {
    let number = parse_integer_argument(args.bytes(1))?;
    let options = parse_set_options(&args.rest(3))?;

    if options.get || options.condition != SetCondition::Always {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::SetIfCmp {
        key: args.arg(0),
        value: args.arg(1),
        number,
        greater,
        options,
    })
}

pub(super) fn parse_delex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 || !args.bytes(1).eq_ignore_ascii_case(b"IFEQ") {
        return Err(ProtocolError::Invalid("DELEX expects key IFEQ value"));
    }

    Ok(Command::DelIfEq {
        key: args.arg(0),
        expected: args.arg(2),
    })
}

pub(super) fn parse_push_capped<'a>(
    args: &Args<'a, '_>,
    back: bool,
    name: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(name));
    }

    let cap = parse_usize(args.bytes(1))
        .ok()
        .filter(|cap| *cap > 0)
        .ok_or(ProtocolError::Invalid("cap must be a positive integer"))?;

    Ok(Command::ListPushCapped {
        key: args.arg(0),
        cap,
        values: (2..args.len()).map(|index| args.arg(index)).collect(),
        back,
    })
}

pub(super) fn parse_lpushcap<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_push_capped(
        args,
        false,
        "LPUSHCAP expects a key, a cap and at least one value",
    )
}

pub(super) fn parse_rpushcap<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_push_capped(
        args,
        true,
        "RPUSHCAP expects a key, a cap and at least one value",
    )
}

/// Integer argument with Redis's wording for a bad one.
pub(super) fn parse_integer_argument(bytes: &[u8]) -> Result<i64, ProtocolError> {
    super::parse::redis_integer(bytes).ok_or(ProtocolError::Invalid(
        "value is not an integer or out of range",
    ))
}

pub(super) fn parse_expire_seconds(
    bytes: &[u8],
    unit_ms: i64,
    invalid: &'static str,
) -> Result<SetExpiry, ProtocolError> {
    let value = parse_integer_argument(bytes)?;

    if value <= 0 {
        return Err(ProtocolError::Invalid(invalid));
    }

    value
        .checked_mul(unit_ms)
        .map(SetExpiry::RelativeMs)
        .ok_or(ProtocolError::Invalid(invalid))
}

pub(super) fn parse_setex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setex' command",
        ));
    }

    let options = SetOptions {
        expiry: parse_expire_seconds(
            args.bytes(1),
            1000,
            "invalid expire time in 'setex' command",
        )?,
        ..SetOptions::default()
    };

    Ok(Command::Set(args.arg(0), args.arg(2), options))
}

pub(super) fn parse_psetex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'psetex' command",
        ));
    }

    let options = SetOptions {
        expiry: parse_expire_seconds(args.bytes(1), 1, "invalid expire time in 'psetex' command")?,
        ..SetOptions::default()
    };

    Ok(Command::Set(args.arg(0), args.arg(2), options))
}

pub(super) fn parse_setnx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::SetNx(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setnx' command",
        )),
    }
}

pub(super) fn parse_getdel<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::GetDel(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'getdel' command",
        )),
    }
}

pub(super) fn parse_msetnx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'msetnx' command",
        ));
    }

    Ok(Command::MSetNx(
        (0..args.len() / 2)
            .map(|pair| (args.arg(pair * 2), args.arg(pair * 2 + 1)))
            .collect(),
    ))
}

/// Like Redis, `inf` parses and fails later as an overflow; only NaN or text is "not a float".
pub(super) fn parse_increment_float(bytes: &[u8]) -> Result<f64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|value| !value.is_nan())
        .ok_or(ProtocolError::Invalid("value is not a valid float"))
}

pub(super) fn parse_incrbyfloat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::IncrementFloat(
            args.arg(0),
            parse_increment_float(args.bytes(1))?,
        )),
        4 => match create_ttl_option(args, 2)? {
            Some(ttl) => Ok(with_create_ttl(
                Some(ttl),
                Command::IncrementFloat(args.arg(0), parse_increment_float(args.bytes(1))?),
            )),
            None => Err(ProtocolError::Invalid("syntax error")),
        },
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'incrbyfloat' command",
        )),
    }
}

pub(super) fn parse_getrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        3 => Ok(Command::GetRange {
            key: args.arg(0),
            start: parse_integer_argument(args.bytes(1))?,
            end: parse_integer_argument(args.bytes(2))?,
        }),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'getrange' command",
        )),
    }
}

pub(super) fn parse_setrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'setrange' command",
        ));
    }

    let offset = parse_integer_argument(args.bytes(1))?;

    if offset < 0 {
        return Err(ProtocolError::Invalid("offset is out of range"));
    }

    Ok(Command::SetRange {
        key: args.arg(0),
        offset: offset as u64,
        value: args.arg(2),
    })
}

pub(super) fn arity_error(name: &'static str) -> ProtocolError {
    ProtocolError::Invalid(match name {
        "smismember" => "wrong number of arguments for 'smismember' command",
        "sintercard" => "wrong number of arguments for 'sintercard' command",
        "zcount" => "wrong number of arguments for 'zcount' command",
        "zlexcount" => "wrong number of arguments for 'zlexcount' command",
        "zmscore" => "wrong number of arguments for 'zmscore' command",
        "zrandmember" => "wrong number of arguments for 'zrandmember' command",
        "hrandfield" => "wrong number of arguments for 'hrandfield' command",
        "hincrbyfloat" => "wrong number of arguments for 'hincrbyfloat' command",
        _ => "wrong number of arguments",
    })
}

pub(super) fn parse_smismember<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("smismember"));
    }

    Ok(Command::SetMIsMember {
        key: args.arg(0),
        members: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

pub(super) fn parse_sintercard<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("sintercard"));
    }

    let numkeys = parse_integer_argument(args.bytes(0))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid("numkeys should be greater than 0"));
    }

    if numkeys as usize > args.len() - 1 {
        return Err(ProtocolError::Invalid(
            "Number of keys can't be greater than number of args",
        ));
    }

    let keys: Vec<Bytes<'a>> = (1..=numkeys as usize)
        .map(|index| args.arg(index))
        .collect();
    let mut limit = 0;
    let mut index = numkeys as usize + 1;

    while index < args.len() {
        if args.bytes(index).eq_ignore_ascii_case(b"LIMIT") && index + 1 < args.len() {
            let value = parse_integer_argument(args.bytes(index + 1))
                .map_err(|_| ProtocolError::Invalid("LIMIT can't be negative"))?;

            if value < 0 {
                return Err(ProtocolError::Invalid("LIMIT can't be negative"));
            }

            limit = value as u64;
            index += 2;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::SetInterCard { keys, limit })
}

pub(super) fn parse_zcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("zcount"));
    }

    let bound = |index: usize| {
        parse_score_bound(args.bytes(index))
            .map_err(|_| ProtocolError::Invalid("min or max is not a float"))
    };

    Ok(Command::SortedSetCount {
        key: args.arg(0),
        min: bound(1)?,
        max: bound(2)?,
    })
}

pub(super) fn parse_zlexcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("zlexcount"));
    }

    Ok(Command::SortedSetLexCount {
        key: args.arg(0),
        min: parse_lex_bound(args.arg(1))?,
        max: parse_lex_bound(args.arg(2))?,
    })
}

pub(super) fn parse_zmscore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_error("zmscore"));
    }

    Ok(Command::SortedSetMScore {
        key: args.arg(0),
        members: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

/// `[dest] numkeys key... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]`.
pub(super) fn parse_sorted_algebra<'a>(
    args: &Args<'a, '_>,
    op: SetOp,
    store: bool,
    name: &'static str,
    empty_keys: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    let first = usize::from(store);

    if args.len() < first + 2 {
        return Err(ProtocolError::Invalid(name));
    }

    let numkeys = parse_integer_argument(args.bytes(first))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid(empty_keys));
    }

    let keys_start = first + 1;
    let keys_end = keys_start + numkeys as usize;

    if keys_end > args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let mut weights = Vec::new();
    let mut aggregate = SortedAggregate::Sum;
    let mut with_scores = false;
    let mut index = keys_end;

    while index < args.len() {
        let word = args.bytes(index);

        if op != SetOp::Diff && word.eq_ignore_ascii_case(b"WEIGHTS") {
            if index + numkeys as usize >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            weights = (1..=numkeys as usize)
                .map(|offset| {
                    std::str::from_utf8(args.bytes(index + offset))
                        .ok()
                        .and_then(|text| text.parse::<f64>().ok())
                        .filter(|weight| !weight.is_nan())
                        .ok_or(ProtocolError::Invalid("weight value is not a float"))
                })
                .collect::<Result<_, _>>()?;
            index += 1 + numkeys as usize;
        } else if op != SetOp::Diff
            && word.eq_ignore_ascii_case(b"AGGREGATE")
            && index + 1 < args.len()
        {
            let mode = args.bytes(index + 1);

            aggregate = if mode.eq_ignore_ascii_case(b"SUM") {
                SortedAggregate::Sum
            } else if mode.eq_ignore_ascii_case(b"MIN") {
                SortedAggregate::Min
            } else if mode.eq_ignore_ascii_case(b"MAX") {
                SortedAggregate::Max
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };
            index += 2;
        } else if !store && word.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::SortedSetAlgebra {
        op,
        keys: (keys_start..keys_end)
            .map(|position| args.arg(position))
            .collect(),
        weights,
        aggregate,
        with_scores,
        destination: store.then(|| args.arg(0)),
    })
}

pub(super) fn parse_zunionstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Union,
        true,
        "wrong number of arguments for 'zunionstore' command",
        "at least 1 input key is needed for 'zunionstore' command",
    )
}

pub(super) fn parse_zinterstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Inter,
        true,
        "wrong number of arguments for 'zinterstore' command",
        "at least 1 input key is needed for 'zinterstore' command",
    )
}

pub(super) fn parse_zdiffstore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Diff,
        true,
        "wrong number of arguments for 'zdiffstore' command",
        "at least 1 input key is needed for 'zdiffstore' command",
    )
}

pub(super) fn parse_zunion<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Union,
        false,
        "wrong number of arguments for 'zunion' command",
        "at least 1 input key is needed for 'zunion' command",
    )
}

pub(super) fn parse_zinter<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Inter,
        false,
        "wrong number of arguments for 'zinter' command",
        "at least 1 input key is needed for 'zinter' command",
    )
}

pub(super) fn parse_zdiff<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_sorted_algebra(
        args,
        SetOp::Diff,
        false,
        "wrong number of arguments for 'zdiff' command",
        "at least 1 input key is needed for 'zdiff' command",
    )
}

/// `key [count [WITHSCORES|WITHVALUES]]` shared by `ZRANDMEMBER` and `HRANDFIELD`.
pub(super) fn parse_random_options<'a>(
    args: &Args<'a, '_>,
    flag: &[u8],
    name: &'static str,
) -> Result<(Bytes<'a>, Option<i64>, bool), ProtocolError> {
    match args.len() {
        1 => Ok((args.arg(0), None, false)),
        2 | 3 => {
            let count = check_random_count(parse_integer_argument(args.bytes(1))?)?;
            let with_extra = if args.len() == 3 {
                if !args.bytes(2).eq_ignore_ascii_case(flag) {
                    return Err(ProtocolError::Invalid("syntax error"));
                }

                true
            } else {
                false
            };

            Ok((args.arg(0), Some(count), with_extra))
        }
        _ => Err(arity_error(name)),
    }
}

pub(super) fn parse_zrandmember<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let (key, count, with_scores) = parse_random_options(args, b"WITHSCORES", "zrandmember")?;

    Ok(Command::SortedSetRandMember {
        key,
        count,
        with_scores,
    })
}

pub(super) fn parse_hrandfield<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let (key, count, with_values) = parse_random_options(args, b"WITHVALUES", "hrandfield")?;

    Ok(Command::HashRandField {
        key,
        count,
        with_values,
    })
}

pub(super) fn parse_hincrbyfloat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(arity_error("hincrbyfloat"));
    }

    Ok(Command::HashIncrementFloat {
        key: args.arg(0),
        field: args.arg(1),
        delta: parse_increment_float(args.bytes(2))?,
    })
}

pub(super) fn parse_linsert<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 4 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'linsert' command",
        ));
    }

    let before = if args.bytes(1).eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if args.bytes(1).eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Err(ProtocolError::Invalid("syntax error"));
    };

    Ok(Command::ListInsert {
        key: args.arg(0),
        before,
        pivot: args.arg(2),
        value: args.arg(3),
    })
}

pub(super) fn parse_lset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lset' command",
        ));
    }

    Ok(Command::ListSet {
        key: args.arg(0),
        index: parse_integer_argument(args.bytes(1))?,
        value: args.arg(2),
    })
}

pub(super) fn parse_lrem<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lrem' command",
        ));
    }

    Ok(Command::ListRemove {
        key: args.arg(0),
        count: parse_integer_argument(args.bytes(1))?,
        value: args.arg(2),
    })
}

pub(super) fn parse_lpos<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lpos' command",
        ));
    }

    let mut rank = 1i64;
    let mut count = None;
    let mut max_len = 0u64;
    let mut index = 2;

    while index < args.len() {
        let word = args.bytes(index);

        if index + 1 >= args.len() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        let value = parse_integer_argument(args.bytes(index + 1))?;

        if word.eq_ignore_ascii_case(b"RANK") {
            if value == 0 {
                return Err(ProtocolError::Invalid(
                    "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                ));
            }

            if value == i64::MIN {
                return Err(ProtocolError::Invalid(
                    "value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                ));
            }

            rank = value;
        } else if word.eq_ignore_ascii_case(b"COUNT") {
            if value < 0 {
                return Err(ProtocolError::Invalid("COUNT can't be negative"));
            }

            count = Some(value as u64);
        } else if word.eq_ignore_ascii_case(b"MAXLEN") {
            if value < 0 {
                return Err(ProtocolError::Invalid("MAXLEN can't be negative"));
            }

            max_len = value as u64;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 2;
    }

    Ok(Command::ListPos {
        key: args.arg(0),
        element: args.arg(1),
        rank,
        count,
        max_len,
    })
}

pub(super) fn parse_side(bytes: &[u8]) -> Result<bool, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"LEFT") {
        Ok(true)
    } else if bytes.eq_ignore_ascii_case(b"RIGHT") {
        Ok(false)
    } else {
        Err(ProtocolError::Invalid("syntax error"))
    }
}

pub(super) fn parse_lmove<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 4 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'lmove' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: parse_side(args.bytes(2))?,
        to_left: parse_side(args.bytes(3))?,
        block: None,
    })
}

pub(super) fn parse_blmove<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 5 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'blmove' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: parse_side(args.bytes(2))?,
        to_left: parse_side(args.bytes(3))?,
        block: Some(parse_timeout_ms(args.bytes(4))?),
    })
}

pub(super) fn parse_rpoplpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'rpoplpush' command",
        ));
    }

    Ok(Command::ListMove {
        source: args.arg(0),
        destination: args.arg(1),
        from_left: false,
        to_left: true,
        block: None,
    })
}

pub(super) fn parse_pushx<'a>(
    args: &Args<'a, '_>,
    back: bool,
    name: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(name));
    }

    Ok(Command::ListPushX {
        key: args.arg(0),
        values: (1..args.len()).map(|index| args.arg(index)).collect(),
        back,
    })
}

pub(super) fn parse_lpushx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_pushx(
        args,
        false,
        "wrong number of arguments for 'lpushx' command",
    )
}

pub(super) fn parse_rpushx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_pushx(args, true, "wrong number of arguments for 'rpushx' command")
}

/// `numkeys key... LEFT|RIGHT [COUNT n]` starting at argument `first`.
pub(super) fn parse_mpop<'a>(
    args: &Args<'a, '_>,
    first: usize,
    block: Option<Option<u64>>,
    arity: &'static str,
) -> Result<Command<'a>, ProtocolError> {
    if args.len() < first + 3 {
        return Err(ProtocolError::Invalid(arity));
    }

    let numkeys = parse_integer_argument(args.bytes(first))?;

    if numkeys < 1 {
        return Err(ProtocolError::Invalid("numkeys should be greater than 0"));
    }

    let keys_end = first + 1 + numkeys as usize;

    if keys_end >= args.len() {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let from_left = parse_side(args.bytes(keys_end))?;
    let mut count = 1u64;
    let mut index = keys_end + 1;

    if index < args.len() {
        if index + 2 != args.len() || !args.bytes(index).eq_ignore_ascii_case(b"COUNT") {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        let value = parse_integer_argument(args.bytes(index + 1))?;

        if value < 1 {
            return Err(ProtocolError::Invalid("count should be greater than 0"));
        }

        count = value as u64;
        index += 2;
    }

    debug_assert_eq!(index, args.len());

    Ok(Command::ListMPop {
        keys: (first + 1..keys_end)
            .map(|position| args.arg(position))
            .collect(),
        from_left,
        count,
        block,
    })
}

pub(super) fn parse_lmpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    parse_mpop(
        args,
        0,
        None,
        "wrong number of arguments for 'lmpop' command",
    )
}

pub(super) fn parse_blmpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'blmpop' command",
        ));
    }

    let timeout = parse_timeout_ms(args.bytes(0))?;

    parse_mpop(
        args,
        1,
        Some(timeout),
        "wrong number of arguments for 'blmpop' command",
    )
}

/// `INCR` and `DECR`: a key and an optional `EX`/`PX` deadline for a key they create.
fn counter_step<'a>(args: &Args<'a, '_>, delta: i64) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Increment(args.arg(0), delta)),
        3 => match create_ttl_option(args, 1)? {
            Some(ttl) => Ok(with_create_ttl(
                Some(ttl),
                Command::Increment(args.arg(0), delta),
            )),
            None => Err(ProtocolError::Invalid("syntax error")),
        },
        _ => Err(ProtocolError::Invalid(
            "INCR and DECR expect a key and an optional EX or PX",
        )),
    }
}

/// `INCRBY key n [EX s | PX ms] [MAX limit]` (`DECRBY` takes no `MAX`).
fn counter_by<'a>(
    args: &Args<'a, '_>,
    delta: i64,
    allow_max: bool,
) -> Result<Command<'a>, ProtocolError> {
    const SYNTAX: ProtocolError = ProtocolError::Invalid(
        "INCRBY and DECRBY expect a key, an integer and optional EX, PX or MAX",
    );

    if args.len() < 2 {
        return Err(SYNTAX);
    }

    let mut ttl = None;
    let mut limit = None;
    let mut index = 2;

    while index < args.len() {
        if ttl.is_none()
            && let Some(found) = create_ttl_option(args, index)?
        {
            ttl = Some(found);
        } else if allow_max
            && limit.is_none()
            && index + 1 < args.len()
            && args.bytes(index).eq_ignore_ascii_case(b"MAX")
        {
            limit = Some(parse_i64(args.bytes(index + 1))?);
        } else {
            return Err(SYNTAX);
        }

        index += 2;
    }

    let key = args.arg(0);
    let command = match limit {
        Some(limit) => Command::IncrementCapped { key, delta, limit },
        None => Command::Increment(key, delta),
    };

    Ok(with_create_ttl(ttl, command))
}

pub(super) fn parse_incr<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    counter_step(args, 1)
}

pub(super) fn parse_decr<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    counter_step(args, -1)
}

pub(super) fn parse_incrby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return counter_by(args, 0, true);
    }

    counter_by(args, parse_i64(args.bytes(1))?, true)
}

pub(super) fn parse_decrby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return counter_by(args, 0, false);
    }

    let delta = parse_i64(args.bytes(1))?
        .checked_neg()
        .ok_or(ProtocolError::Invalid(
            "value is not an integer or out of range",
        ))?;

    counter_by(args, delta, false)
}

pub(super) fn parse_mget<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid("MGET expects at least one key"))
    } else {
        Ok(Command::MGet(args.rest(0)))
    }
}

pub(super) fn parse_getset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::GetSet(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid("GETSET expects a key and a value")),
    }
}

pub(super) fn parse_append<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::Append(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid("APPEND expects a key and a value")),
    }
}

/// Highest bit offset `SETBIT` and `GETBIT` accept (Redis: 2^32 - 1; here bounded by the
/// 16 MiB bitmap limit in the engine).
pub(super) const MAX_BIT_OFFSET: u64 = (1 << 32) - 1;

pub(super) fn parse_bit_offset(bytes: &[u8]) -> Result<u64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|&offset| offset <= MAX_BIT_OFFSET)
        .ok_or(ProtocolError::Invalid(
            "bit offset is not an integer or out of range",
        ))
}

pub(super) fn parse_bit_value(bytes: &[u8]) -> Result<bool, ProtocolError> {
    match bytes {
        b"0" => Ok(false),
        b"1" => Ok(true),
        _ => Err(ProtocolError::Invalid(
            "bit is not an integer or out of range",
        )),
    }
}

/// Optional trailing `BYTE` or `BIT` unit; `true` means bits.
pub(super) fn parse_bit_unit(bytes: &[u8]) -> Result<bool, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"BIT") {
        Ok(true)
    } else if bytes.eq_ignore_ascii_case(b"BYTE") {
        Ok(false)
    } else {
        Err(ProtocolError::Invalid("syntax error"))
    }
}

pub(super) fn parse_setbit<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let ttl = match args.len() {
        3 => None,
        5 => Some(create_ttl_option(args, 3)?.ok_or(ProtocolError::Invalid("syntax error"))?),
        _ => {
            return Err(ProtocolError::Invalid(
                "SETBIT expects a key, an offset, a bit and an optional EX or PX",
            ));
        }
    };

    Ok(with_create_ttl(
        ttl,
        Command::SetBit {
            key: args.arg(0),
            offset: parse_bit_offset(args.bytes(1))?,
            value: parse_bit_value(args.bytes(2))?,
        },
    ))
}

/// Largest bit offset a `BITFIELD` field may end at (16 MiB bitmap).
pub(super) const MAX_BITFIELD_BITS: u64 = 16 * 1024 * 1024 * 8;

/// Largest repeat count (a negative count) `SRANDMEMBER`, `HRANDFIELD` and `ZRANDMEMBER`
/// accept. Repeats are drawn into memory before any output limit applies, so an unbounded
/// count could exhaust memory. A positive count is bounded by the collection size.
pub(crate) const MAX_RANDOM_COUNT: u64 = 1 << 20;

/// Refuses a repeat count whose reply could not fit in memory.
pub(crate) fn check_random_count(count: i64) -> Result<i64, ProtocolError> {
    if count < 0 && count.unsigned_abs() > MAX_RANDOM_COUNT {
        return Err(ProtocolError::Invalid("count is out of range"));
    }

    Ok(count)
}

pub(super) fn parse_bitfield_type(bytes: &[u8]) -> Result<BitFieldType, ProtocolError> {
    const INVALID: ProtocolError = ProtocolError::Invalid(
        "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
    );
    let (signed, digits) = match bytes.split_first() {
        Some((b'i', rest)) => (true, rest),
        Some((b'u', rest)) => (false, rest),
        _ => return Err(INVALID),
    };
    let bits: u8 = std::str::from_utf8(digits)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(INVALID)?;
    let limit = if signed { 64 } else { 63 };

    if bits == 0 || bits > limit {
        return Err(INVALID);
    }

    Ok(BitFieldType { signed, bits })
}

/// A bit offset, or `#n` for the n-th field of the type's width. The field must fit the bitmap limit.
pub(super) fn parse_bitfield_offset(bytes: &[u8], ty: BitFieldType) -> Result<u64, ProtocolError> {
    const INVALID: ProtocolError =
        ProtocolError::Invalid("bit offset is not an integer or out of range");
    let (scaled, digits) = match bytes.split_first() {
        Some((b'#', rest)) => (true, rest),
        _ => (false, bytes),
    };
    let number: u64 = std::str::from_utf8(digits)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(INVALID)?;
    let offset = if scaled {
        number.checked_mul(u64::from(ty.bits)).ok_or(INVALID)?
    } else {
        number
    };

    if offset.saturating_add(u64::from(ty.bits)) > MAX_BITFIELD_BITS {
        return Err(INVALID);
    }

    Ok(offset)
}

pub(super) fn parse_bitfield_ops(
    args: &Args<'_, '_>,
    read_only: bool,
) -> Result<BitFieldOps, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'bitfield' command",
        ));
    }

    const SYNTAX: ProtocolError = ProtocolError::Invalid("syntax error");
    let mut ops = BitFieldOps::new();
    let mut overflow = BitFieldOverflow::Wrap;
    let mut index = 1;

    while index < args.len() {
        let word = args.bytes(index);

        if word.eq_ignore_ascii_case(b"GET") && index + 2 < args.len() {
            let ty = parse_bitfield_type(args.bytes(index + 1))?;

            ops.push(BitFieldOp::Get {
                ty,
                offset: parse_bitfield_offset(args.bytes(index + 2), ty)?,
            });
            index += 3;
        } else if (word.eq_ignore_ascii_case(b"SET") || word.eq_ignore_ascii_case(b"INCRBY"))
            && index + 3 < args.len()
        {
            if read_only {
                return Err(ProtocolError::Invalid(
                    "BITFIELD_RO only supports the GET subcommand",
                ));
            }

            let ty = parse_bitfield_type(args.bytes(index + 1))?;
            let offset = parse_bitfield_offset(args.bytes(index + 2), ty)?;
            let number = super::parse::redis_integer(args.bytes(index + 3)).ok_or(
                ProtocolError::Invalid("value is not an integer or out of range"),
            )?;

            ops.push(if word.eq_ignore_ascii_case(b"SET") {
                BitFieldOp::Set {
                    ty,
                    offset,
                    value: number,
                    overflow,
                }
            } else {
                BitFieldOp::IncrBy {
                    ty,
                    offset,
                    delta: number,
                    overflow,
                }
            });
            index += 4;
        } else if word.eq_ignore_ascii_case(b"OVERFLOW") && index + 1 < args.len() {
            let mode = args.bytes(index + 1);

            overflow = if mode.eq_ignore_ascii_case(b"WRAP") {
                BitFieldOverflow::Wrap
            } else if mode.eq_ignore_ascii_case(b"SAT") {
                BitFieldOverflow::Sat
            } else if mode.eq_ignore_ascii_case(b"FAIL") {
                BitFieldOverflow::Fail
            } else {
                return Err(ProtocolError::Invalid("Invalid OVERFLOW type specified"));
            };
            index += 2;
        } else if read_only
            && (word.eq_ignore_ascii_case(b"SET") || word.eq_ignore_ascii_case(b"INCRBY"))
        {
            return Err(ProtocolError::Invalid(
                "BITFIELD_RO only supports the GET subcommand",
            ));
        } else {
            return Err(SYNTAX);
        }
    }

    Ok(ops)
}

pub(super) fn parse_bitfield<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let ops = parse_bitfield_ops(args, false)?;

    Ok(Command::BitField {
        key: args.arg(0),
        ops,
        read_only_name: false,
    })
}

pub(super) fn parse_bitfield_ro<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let ops = parse_bitfield_ops(args, true)?;

    Ok(Command::BitField {
        key: args.arg(0),
        ops,
        read_only_name: true,
    })
}

pub(super) fn parse_getbit<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::GetBit(
            args.arg(0),
            parse_bit_offset(args.bytes(1))?,
        )),
        _ => Err(ProtocolError::Invalid("GETBIT expects a key and an offset")),
    }
}

pub(super) fn parse_bitcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let range = match args.len() {
        0 => return Err(ProtocolError::Invalid("BITCOUNT expects a key")),
        1 => None,
        3 => Some((parse_i64(args.bytes(1))?, parse_i64(args.bytes(2))?, false)),
        4 => Some((
            parse_i64(args.bytes(1))?,
            parse_i64(args.bytes(2))?,
            parse_bit_unit(args.bytes(3))?,
        )),
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitCount {
        key: args.arg(0),
        range,
    })
}

pub(super) fn parse_bitpos<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid("BITPOS expects a key and a bit"));
    }

    let bit = parse_bit_value(args.bytes(1))?;
    let range = match args.len() {
        2 => None,
        3 => Some((parse_i64(args.bytes(2))?, None, false)),
        4 => Some((
            parse_i64(args.bytes(2))?,
            Some(parse_i64(args.bytes(3))?),
            false,
        )),
        5 => Some((
            parse_i64(args.bytes(2))?,
            Some(parse_i64(args.bytes(3))?),
            parse_bit_unit(args.bytes(4))?,
        )),
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitPos {
        key: args.arg(0),
        bit,
        range,
    })
}

pub(super) fn parse_bitop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "BITOP expects an operation, a destination and at least one source",
        ));
    }

    let operation = match args.bytes(0) {
        name if name.eq_ignore_ascii_case(b"AND") => BitOperation::And,
        name if name.eq_ignore_ascii_case(b"OR") => BitOperation::Or,
        name if name.eq_ignore_ascii_case(b"XOR") => BitOperation::Xor,
        name if name.eq_ignore_ascii_case(b"NOT") => BitOperation::Not,
        _ => return Err(ProtocolError::Invalid("syntax error")),
    };

    Ok(Command::BitOp {
        operation,
        destination: args.arg(1),
        keys: (2..args.len()).map(|index| args.arg(index)).collect(),
    })
}

pub(super) fn parse_pfadd<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFADD expects a key"));
    }

    Ok(Command::PfAdd {
        key: args.arg(0),
        elements: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

pub(super) fn parse_pfcount<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFCOUNT expects at least one key"));
    }

    Ok(Command::PfCount(keys_of(args)))
}

pub(super) fn parse_pfmerge<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("PFMERGE expects a destination key"));
    }

    Ok(Command::PfMerge {
        destination: args.arg(0),
        sources: (1..args.len()).map(|index| args.arg(index)).collect(),
    })
}

pub(super) fn parse_positive_u64(bytes: &[u8], what: &'static str) -> Result<u64, ProtocolError> {
    match parse_i64(bytes) {
        Ok(value) if value > 0 => Ok(value as u64),
        _ => Err(ProtocolError::Invalid(what)),
    }
}

pub(super) fn parse_fillget<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let wait_ms = match args.len() {
        2 => None,
        4 if args.bytes(2).eq_ignore_ascii_case(b"WAIT") => Some(parse_positive_u64(
            args.bytes(3),
            "FILLGET wait time must be a positive integer",
        )?),
        _ => {
            return Err(ProtocolError::Invalid(
                "FILLGET expects a key, a fill time in milliseconds and optionally WAIT ms",
            ));
        }
    };

    Ok(Command::FillGet {
        key: args.arg(0),
        fill_ms: parse_positive_u64(
            args.bytes(1),
            "FILLGET fill time must be a positive integer",
        )?,
        wait_ms,
    })
}

pub(super) fn parse_fillset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    let swr_ms = match args.len() {
        4 => None,
        6 if args.bytes(4).eq_ignore_ascii_case(b"SWR") => Some(parse_positive_u64(
            args.bytes(5),
            "FILLSET SWR must be a positive integer",
        )?),
        _ => {
            return Err(ProtocolError::Invalid(
                "FILLSET expects a key, a value, a ttl in milliseconds, a token and optionally SWR ms",
            ));
        }
    };

    Ok(Command::FillSet {
        key: args.arg(0),
        value: args.arg(1),
        ttl_ms: parse_positive_u64(args.bytes(2), "FILLSET ttl must be a positive integer")?,
        token: parse_positive_u64(args.bytes(3), "FILLSET token must be a positive integer")?,
        swr_ms,
    })
}

pub(super) fn parse_strlen<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::StrLen(args.arg(0))),
        _ => Err(ProtocolError::Invalid("STRLEN expects one argument")),
    }
}

pub(super) fn arity_invalid(name: &'static str) -> ProtocolError {
    ProtocolError::Invalid(match name {
        "keys" => "wrong number of arguments for 'keys' command",
        "randomkey" => "wrong number of arguments for 'randomkey' command",
        "touch" => "wrong number of arguments for 'touch' command",
        "renamenx" => "wrong number of arguments for 'renamenx' command",
        "expiretime" => "wrong number of arguments for 'expiretime' command",
        "pexpiretime" => "wrong number of arguments for 'pexpiretime' command",
        _ => "wrong number of arguments for 'copy' command",
    })
}

pub(super) fn parse_keys<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Keys(args.arg(0))),
        _ => Err(arity_invalid("keys")),
    }
}

pub(super) fn parse_randomkey<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        0 => Ok(Command::RandomKey),
        _ => Err(arity_invalid("randomkey")),
    }
}

pub(super) fn parse_touch<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(arity_invalid("touch"))
    } else {
        Ok(Command::Touch(keys_of(args)))
    }
}

pub(super) fn parse_renamenx<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        2 => Ok(Command::RenameNx {
            source: args.arg(0),
            destination: args.arg(1),
        }),
        _ => Err(arity_invalid("renamenx")),
    }
}

pub(super) fn parse_expiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::ExpireTime {
            key: args.arg(0),
            millis: false,
        }),
        _ => Err(arity_invalid("expiretime")),
    }
}

pub(super) fn parse_pexpiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::ExpireTime {
            key: args.arg(0),
            millis: true,
        }),
        _ => Err(arity_invalid("pexpiretime")),
    }
}

/// `COPY source destination [REPLACE]`. `DB` is refused: a copy stays in the selected database.
pub(super) fn parse_copy<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(arity_invalid("copy"));
    }

    let mut replace = false;

    for index in 2..args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if option.eq_ignore_ascii_case(b"DB") {
            return Err(ProtocolError::Invalid("COPY with DB is not supported"));
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::Copy {
        source: args.arg(0),
        destination: args.arg(1),
        replace,
    })
}

pub(super) fn parse_echo<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Echo(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'echo' command",
        )),
    }
}

pub(super) fn parse_object<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'object' command",
        ));
    }

    let kind = match args.bytes(0) {
        name if name.eq_ignore_ascii_case(b"ENCODING") => ObjectKind::Encoding,
        name if name.eq_ignore_ascii_case(b"IDLETIME") => ObjectKind::IdleTime,
        name if name.eq_ignore_ascii_case(b"FREQ") => ObjectKind::Freq,
        _ => {
            return Err(ProtocolError::Invalid(
                "unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.",
            ));
        }
    };

    if args.len() != 2 {
        return Err(ProtocolError::Invalid(match kind {
            ObjectKind::Encoding => "wrong number of arguments for 'object|encoding' command",
            ObjectKind::IdleTime => "wrong number of arguments for 'object|idletime' command",
            ObjectKind::Freq => "wrong number of arguments for 'object|freq' command",
        }));
    }

    Ok(Command::Object {
        kind,
        key: args.arg(1),
    })
}

/// `MEMORY USAGE key [SAMPLES count]`; the sample count is accepted and ignored because the
/// size is always read exactly.
pub(super) fn parse_memory<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    const UNKNOWN: &str =
        "unknown subcommand or wrong number of arguments for 'MEMORY'. Try MEMORY HELP.";

    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'memory' command",
        ));
    }

    if !args.bytes(0).eq_ignore_ascii_case(b"USAGE") || args.len() < 2 {
        return Err(ProtocolError::Invalid(UNKNOWN));
    }

    match args.len() {
        2 => {}
        4 if args.bytes(2).eq_ignore_ascii_case(b"SAMPLES") => {
            parse_integer_argument(args.bytes(3))?;
        }
        _ => return Err(ProtocolError::Invalid("syntax error")),
    }

    Ok(Command::MemoryUsage(args.arg(1)))
}

pub(super) fn parse_slowlog<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    const UNKNOWN: &str =
        "unknown subcommand or wrong number of arguments for 'SLOWLOG'. Try SLOWLOG HELP.";

    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'slowlog' command",
        ));
    }

    let name = args.bytes(0);

    if name.eq_ignore_ascii_case(b"GET") && args.len() <= 2 {
        let count = if args.len() == 2 {
            match parse_integer_argument(args.bytes(1))? {
                -1 => usize::MAX,
                count if count >= 0 => count as usize,
                _ => {
                    return Err(ProtocolError::Invalid(
                        "count should be greater than or equal to -1",
                    ));
                }
            }
        } else {
            10
        };

        return Ok(Command::Slowlog(SlowlogOp::Get(count)));
    }

    if name.eq_ignore_ascii_case(b"LEN") && args.len() == 1 {
        return Ok(Command::Slowlog(SlowlogOp::Len));
    }

    if name.eq_ignore_ascii_case(b"RESET") && args.len() == 1 {
        return Ok(Command::Slowlog(SlowlogOp::Reset));
    }

    Err(ProtocolError::Invalid(UNKNOWN))
}

/// `LATENCY LATEST` only: Ruvio keeps no latency-spike events, so the list is always empty.
pub(super) fn parse_latency<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'latency' command",
        ));
    }

    if args.bytes(0).eq_ignore_ascii_case(b"LATEST") && args.len() == 1 {
        return Ok(Command::LatencyLatest);
    }

    Err(ProtocolError::Invalid(
        "unknown subcommand or wrong number of arguments for 'LATENCY'. Try LATENCY HELP.",
    ))
}

pub(super) fn parse_dump<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    match args.len() {
        1 => Ok(Command::Dump(args.arg(0))),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'dump' command",
        )),
    }
}

/// `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]`. The last two are
/// accepted and ignored: access data is not carried by `DUMP`.
pub(super) fn parse_restore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'restore' command",
        ));
    }

    let ttl = parse_integer_argument(args.bytes(1))?;
    let mut replace = false;
    let mut absolute = false;
    let mut index = 3;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if option.eq_ignore_ascii_case(b"ABSTTL") {
            absolute = true;
        } else if (option.eq_ignore_ascii_case(b"IDLETIME") || option.eq_ignore_ascii_case(b"FREQ"))
            && index + 1 < args.len()
        {
            parse_integer_argument(args.bytes(index + 1))?;
            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    Ok(Command::Restore {
        key: args.arg(0),
        ttl,
        payload: args.arg(2),
        replace,
        absolute,
    })
}

pub(super) fn parse_exists<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid("EXISTS expects at least one key"))
    } else {
        Ok(Command::Exists(keys_of(args)))
    }
}

pub(super) fn parse_hset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "HSET expects a key and field/value pairs",
        ));
    }

    let pairs = (1..args.len())
        .step_by(2)
        .map(|index| (args.arg(index), args.arg(index + 1)))
        .collect();

    Ok(Command::HashSet {
        key: args.arg(0),
        pairs,
    })
}

pub(super) fn parse_hmset<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'hmset' command",
        ));
    }

    match parse_hset(args)? {
        Command::HashSet { key, pairs } => Ok(Command::HashMSet { key, pairs }),
        _ => unreachable!("parse_hset returns HashSet"),
    }
}

pub(super) fn parse_lpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "LPUSH expects a key and at least one value",
        ));
    }

    Ok(Command::ListPush(args.arg(0), args.rest_one_or_many(1)))
}

pub(super) fn parse_rpush<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "RPUSH expects a key and at least one value",
        ));
    }

    Ok(Command::ListPushBack(args.arg(0), args.rest_one_or_many(1)))
}

pub(super) fn parse_zadd<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "ZADD expects a key and score/member pairs",
        ));
    }

    let mut options = SortedSetAddOptions::default();
    let mut ttl = None;
    let mut index = 1;

    while index < args.len() {
        let option = args.bytes(index);

        if ttl.is_none()
            && let Some(found) = create_ttl_option(args, index)?
        {
            ttl = Some(found);
            index += 2;

            continue;
        }

        if option.eq_ignore_ascii_case(b"NX") {
            if options.condition != SortedSetCondition::Always || options.greater || options.less {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::NotExists;
        } else if option.eq_ignore_ascii_case(b"XX") {
            if options.condition != SortedSetCondition::Always {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::Exists;
        } else if option.eq_ignore_ascii_case(b"GT") || option.eq_ignore_ascii_case(b"LT") {
            if options.greater || options.less || options.condition == SortedSetCondition::NotExists
            {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            if option.eq_ignore_ascii_case(b"GT") {
                options.greater = true;
            } else {
                options.less = true;
            }
        } else if option.eq_ignore_ascii_case(b"CH") {
            options.changed = true;
        } else if option.eq_ignore_ascii_case(b"INCR") {
            options.increment = true;
        } else {
            break;
        }

        index += 1;
    }

    let remaining = args.len() - index;

    if remaining < 2 || !remaining.is_multiple_of(2) {
        return Err(ProtocolError::Invalid(
            "ZADD expects a key and score/member pairs",
        ));
    }

    let pairs = if remaining == 2 {
        OneOrMany::One((parse_f64(args.bytes(index))?, args.arg(index + 1)))
    } else {
        let mut pairs = Vec::with_capacity(remaining / 2);

        for position in (index..args.len()).step_by(2) {
            pairs.push((parse_f64(args.bytes(position))?, args.arg(position + 1)));
        }

        OneOrMany::Many(pairs)
    };

    if options.increment && pairs.len() != 1 {
        return Err(ProtocolError::Invalid(
            "INCR option supports a single increment-element pair",
        ));
    }

    Ok(with_create_ttl(
        ttl,
        Command::SortedSetAdd {
            key: args.arg(0),
            pairs,
            options,
        },
    ))
}

pub(super) fn parse_command_meta<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    if args.len() == 0 {
        return Ok(Command::CommandMeta {
            kind: CommandMetaKind::List,
            names: Vec::new(),
        });
    }

    let subcommand = args.bytes(0);

    if subcommand.eq_ignore_ascii_case(b"COUNT") {
        return if args.len() == 1 {
            Ok(Command::CommandMeta {
                kind: CommandMetaKind::Count,
                names: Vec::new(),
            })
        } else {
            Err(ProtocolError::Invalid("COMMAND COUNT expects no arguments"))
        };
    }

    let kind = if subcommand.eq_ignore_ascii_case(b"INFO") {
        CommandMetaKind::Info
    } else if subcommand.eq_ignore_ascii_case(b"DOCS") {
        CommandMetaKind::Docs
    } else {
        return Err(ProtocolError::Invalid(
            "COMMAND supports COUNT, INFO and DOCS",
        ));
    };

    Ok(Command::CommandMeta {
        kind,
        names: args.rest(1),
    })
}

// Rows whose body in `parse/commands.rs` serves several commands: each passes its own name.

pub(super) fn parse_readonly<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_readonly(b"READONLY", args)
}

pub(super) fn parse_readwrite<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_readonly(b"READWRITE", args)
}

pub(super) fn parse_json_get<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_get(b"JSON.GET", args)
}

pub(super) fn parse_json_del<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_get(b"JSON.DEL", args)
}

pub(super) fn parse_json_forget<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_get(b"JSON.FORGET", args)
}

pub(super) fn parse_json_index<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_index(b"JSON.INDEX", args)
}

pub(super) fn parse_json_find<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_index(b"JSON.FIND", args)
}

pub(super) fn parse_json_type<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.TYPE", args)
}

pub(super) fn parse_json_strlen<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.STRLEN", args)
}

pub(super) fn parse_json_arrlen<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRLEN", args)
}

pub(super) fn parse_json_objkeys<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.OBJKEYS", args)
}

pub(super) fn parse_json_numincrby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.NUMINCRBY", args)
}

pub(super) fn parse_json_toggle<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.TOGGLE", args)
}

pub(super) fn parse_json_arrappend<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRAPPEND", args)
}

pub(super) fn parse_json_clear<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.CLEAR", args)
}

pub(super) fn parse_json_objlen<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.OBJLEN", args)
}

pub(super) fn parse_json_strappend<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.STRAPPEND", args)
}

pub(super) fn parse_json_nummultby<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.NUMMULTBY", args)
}

pub(super) fn parse_json_arrinsert<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRINSERT", args)
}

pub(super) fn parse_json_arrpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRPOP", args)
}

pub(super) fn parse_json_arrtrim<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRTRIM", args)
}

pub(super) fn parse_json_arrindex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_json_op(b"JSON.ARRINDEX", args)
}

pub(super) fn parse_flushdb<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_flushdb(b"FLUSHDB", args)
}

pub(super) fn parse_flushall<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_flushdb(b"FLUSHALL", args)
}

pub(super) fn parse_bzpopmin<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bzpopmin(b"BZPOPMIN", args)
}

pub(super) fn parse_bzpopmax<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bzpopmin(b"BZPOPMAX", args)
}

pub(super) fn parse_bf_add<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_add(b"BF.ADD", args)
}

pub(super) fn parse_bf_madd<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_many(b"BF.MADD", args)
}

pub(super) fn parse_bf_mexists<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_many(b"BF.MEXISTS", args)
}

pub(super) fn parse_bf_info<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_info(b"BF.INFO", args)
}

pub(super) fn parse_bf_card<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_info(b"BF.CARD", args)
}

pub(super) fn parse_bf_exists<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_bf_add(b"BF.EXISTS", args)
}

pub(super) fn parse_blpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_blpop(b"BLPOP", args)
}

pub(super) fn parse_brpop<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_blpop(b"BRPOP", args)
}

pub(super) fn parse_eval<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_eval(b"EVAL", args)
}

pub(super) fn parse_evalsha<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_eval(b"EVALSHA", args)
}

pub(super) fn parse_hexpire<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_hexpire(b"HEXPIRE", args)
}

pub(super) fn parse_hpexpire<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_hexpire(b"HPEXPIRE", args)
}

pub(super) fn parse_hexpireat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_hexpire(b"HEXPIREAT", args)
}

pub(super) fn parse_hpexpireat<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_hexpire(b"HPEXPIREAT", args)
}

pub(super) fn parse_httl<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_httl(b"HTTL", args)
}

pub(super) fn parse_hpttl<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_httl(b"HPTTL", args)
}

pub(super) fn parse_hexpiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_httl(b"HEXPIRETIME", args)
}

pub(super) fn parse_hpexpiretime<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_httl(b"HPEXPIRETIME", args)
}

pub(super) fn parse_hpersist<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_httl(b"HPERSIST", args)
}

pub(super) fn parse_zrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zrange(b"ZRANGE", args)
}

pub(super) fn parse_zrevrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zrange(b"ZREVRANGE", args)
}

pub(super) fn parse_zrangebylex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zrangebylex(b"ZRANGEBYLEX", args)
}

pub(super) fn parse_zrevrangebylex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zrangebylex(b"ZREVRANGEBYLEX", args)
}

pub(super) fn parse_zpopmin<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zpopmin(b"ZPOPMIN", args)
}

pub(super) fn parse_zpopbyscore<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zpopbyscore(args)
}

pub(super) fn parse_zmovedue<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zmovedue(args)
}

pub(super) fn parse_zpopmax<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zpopmin(b"ZPOPMAX", args)
}

pub(super) fn parse_zremrangebyrank<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zremrangebyrank(b"ZREMRANGEBYRANK", args)
}

pub(super) fn parse_zremrangebyscore<'a>(
    args: &Args<'a, '_>,
) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zremrangebyrank(b"ZREMRANGEBYSCORE", args)
}

pub(super) fn parse_zremrangebylex<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_zremrangebyrank(b"ZREMRANGEBYLEX", args)
}

pub(super) fn parse_xrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_xrange(b"XRANGE", args)
}

pub(super) fn parse_xrevrange<'a>(args: &Args<'a, '_>) -> Result<Command<'a>, ProtocolError> {
    commands::cmd_xrange(b"XREVRANGE", args)
}
