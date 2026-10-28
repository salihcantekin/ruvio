use super::*;

pub(in crate::protocol) fn parse_sorted_set_range<'a>(
    args: &Args<'a, '_>,
    legacy_reverse: bool,
) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGE expects start, stop, and optional WITHSCORES",
        ));
    }

    let key = args.arg(0);
    let options = parse_range_options(args, 3, legacy_reverse)?;

    if legacy_reverse && (options.by_score || options.by_lex || options.limit.is_some()) {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    if options.limit.is_some() && !options.by_score && !options.by_lex {
        return Err(ProtocolError::Invalid(
            "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }

    // With REV, Redis takes the high bound first for BYSCORE and BYLEX.
    let (min_at, max_at) = if options.reverse && (options.by_score || options.by_lex) {
        (2, 1)
    } else {
        (1, 2)
    };

    if options.by_lex {
        return Ok(Command::SortedSetRangeByLex {
            key,
            min: parse_lex_bound(args.arg(min_at))?,
            max: parse_lex_bound(args.arg(max_at))?,
            with_scores: options.with_scores,
            reverse: options.reverse,
            limit: options.limit,
            invocation: LexInvocation::ZRange,
        });
    }

    if options.by_score {
        return Ok(Command::SortedSetRangeByScore {
            key,
            min: parse_score_bound(args.bytes(min_at))?,
            max: parse_score_bound(args.bytes(max_at))?,
            with_scores: options.with_scores,
            reverse: options.reverse,
            limit: options.limit,
            via_zrange: true,
        });
    }

    Ok(Command::SortedSetRange {
        key,
        start: parse_isize(args.bytes(min_at))?,
        stop: parse_isize(args.bytes(max_at))?,
        with_scores: options.with_scores,
        reverse: options.reverse,
    })
}

pub(in crate::protocol) fn parse_sorted_set_range_by_score<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGEBYSCORE expects min, max, and optional WITHSCORES",
        ));
    }

    let min = parse_score_bound(args.bytes(1))?;
    let max = parse_score_bound(args.bytes(2))?;
    let options = parse_range_options(args, 3, false)?;

    if options.by_score || options.by_lex || options.reverse {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(Command::SortedSetRangeByScore {
        key: args.arg(0),
        min,
        max,
        with_scores: options.with_scores,
        reverse: false,
        limit: options.limit,
        via_zrange: false,
    })
}

pub(in crate::protocol) fn parse_sorted_set_range_by_lex<'a>(
    args: &Args<'a, '_>,
    reverse: bool,
) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "ZRANGEBYLEX expects min, max, and optional LIMIT",
        ));
    }

    let first = parse_lex_bound(args.arg(1))?;
    let second = parse_lex_bound(args.arg(2))?;
    let options = parse_range_options(args, 3, false)?;

    if options.by_score || options.by_lex || options.reverse || options.with_scores {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let (min, max) = if reverse {
        (second, first)
    } else {
        (first, second)
    };

    Ok(Command::SortedSetRangeByLex {
        key: args.arg(0),
        min,
        max,
        with_scores: false,
        reverse,
        limit: options.limit,
        invocation: if reverse {
            LexInvocation::ZRevRangeByLex
        } else {
            LexInvocation::ZRangeByLex
        },
    })
}

pub(in crate::protocol) struct RangeOptions {
    pub(in crate::protocol) by_score: bool,
    pub(in crate::protocol) by_lex: bool,
    pub(in crate::protocol) reverse: bool,
    pub(in crate::protocol) with_scores: bool,
    pub(in crate::protocol) limit: Option<RangeLimit>,
}

/// Range options (`WITHSCORES`, `BYSCORE`, `BYLEX`, `REV`, `LIMIT`) from argument `from` on.
pub(in crate::protocol) fn parse_range_options(
    args: &Args<'_, '_>,
    from: usize,
    legacy_reverse: bool,
) -> Result<RangeOptions, ProtocolError> {
    let mut options = RangeOptions {
        by_score: false,
        by_lex: false,
        reverse: legacy_reverse,
        with_scores: false,
        limit: None,
    };

    let mut index = from;

    while index < args.len() {
        let token = args.bytes(index);

        if token.eq_ignore_ascii_case(b"WITHSCORES") {
            if options.with_scores {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.with_scores = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"BYSCORE") {
            if options.by_score || options.by_lex {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.by_score = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"BYLEX") {
            if options.by_score || options.by_lex {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.by_lex = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"REV") {
            if legacy_reverse || options.reverse {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.reverse = true;
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"LIMIT") {
            if options.limit.is_some() || index + 2 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let offset = parse_limit_int(args.bytes(index + 1))?;
            let count = parse_limit_int(args.bytes(index + 2))?;

            if offset < 0 {
                return Err(ProtocolError::Invalid("LIMIT offset must be >= 0"));
            }

            options.limit = Some(RangeLimit {
                offset: offset as usize,
                count: (count >= 0).then_some(count as usize),
            });

            index += 3;
            continue;
        }

        return Err(ProtocolError::Invalid("syntax error"));
    }

    Ok(options)
}

pub(in crate::protocol) fn parse_limit_int(bytes: &[u8]) -> Result<isize, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(ProtocolError::Invalid("limit is not an integer"))
}

pub(in crate::protocol) fn parse_lex_bound(
    bytes: Bytes<'_>,
) -> Result<LexBound<'_>, ProtocolError> {
    if bytes.as_ref() == b"-" {
        return Ok(LexBound::NegInf);
    }

    if bytes.as_ref() == b"+" {
        return Ok(LexBound::PosInf);
    }

    if let Some(member) = bytes.as_ref().strip_prefix(b"[") {
        return Ok(LexBound::Inclusive(Cow::Owned(member.to_vec())));
    }

    if let Some(member) = bytes.as_ref().strip_prefix(b"(") {
        return Ok(LexBound::Exclusive(Cow::Owned(member.to_vec())));
    }

    Err(ProtocolError::Invalid(
        "min or max not valid string range item",
    ))
}

pub(in crate::protocol) fn parse_score_bound(bytes: &[u8]) -> Result<ScoreBound, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"-inf") {
        return Ok(ScoreBound::NegInf);
    }

    if bytes.eq_ignore_ascii_case(b"+inf") || bytes.eq_ignore_ascii_case(b"inf") {
        return Ok(ScoreBound::PosInf);
    }

    if let Some(rest) = bytes.strip_prefix(b"(") {
        return Ok(ScoreBound::Exclusive(parse_f64(rest)?));
    }

    Ok(ScoreBound::Inclusive(parse_f64(bytes)?))
}

pub(in crate::protocol) fn parse_set_algebra<'a>(
    args: &Args<'a, '_>,
    op: SetOp,
    store: bool,
) -> Parsed<'a> {
    if store {
        if args.len() < 2 {
            return Err(ProtocolError::Invalid(
                "set STORE commands expect a destination and at least one key",
            ));
        }

        return Ok(Command::SetAlgebra {
            keys: args.rest(1),
            op,
            destination: Some(args.arg(0)),
        });
    }

    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "set algebra commands expect at least one key",
        ));
    }

    Ok(Command::SetAlgebra {
        keys: args.rest(0),
        op,
        destination: None,
    })
}

pub(in crate::protocol) fn parse_sorted_set_pop<'a>(args: &Args<'a, '_>, max: bool) -> Parsed<'a> {
    let count =
        match args.len() {
            1 => None,
            2 => {
                let parsed = parse_i64(args.bytes(1))?;

                Some(usize::try_from(parsed).map_err(|_| {
                    ProtocolError::Invalid("value is out of range, must be positive")
                })?)
            }

            0 => {
                return Err(ProtocolError::Invalid(if max {
                    "wrong number of arguments for 'zpopmax' command"
                } else {
                    "wrong number of arguments for 'zpopmin' command"
                }));
            }

            _ => return Err(ProtocolError::Invalid("syntax error")),
        };

    Ok(Command::SortedSetPop {
        key: args.arg(0),
        count,
        max,
    })
}

/// Most members one `ZPOPBYSCORE`/`ZMOVEDUE` call takes, so one call's work stays bounded.
pub(crate) const MAX_DUE_COUNT: usize = 10_000;

fn parse_due_count(bytes: &[u8]) -> Result<usize, ProtocolError> {
    let parsed = parse_i64(bytes)?;

    match usize::try_from(parsed) {
        Ok(count) if (1..=MAX_DUE_COUNT).contains(&count) => Ok(count),
        _ => Err(ProtocolError::Invalid("count is out of range")),
    }
}

/// `ZPOPBYSCORE key max [MIN min] [COUNT n] [WITHSCORES]`.
pub(in crate::protocol) fn parse_sorted_set_pop_by_score<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'zpopbyscore' command",
        ));
    }

    let max = parse_score_bound(args.bytes(1))?;
    let mut min = ScoreBound::NegInf;
    let mut count = 1;
    let mut with_scores = false;
    let mut index = 2;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"MIN") && index + 1 < args.len() {
            min = parse_score_bound(args.bytes(index + 1))?;
            index += 2;
        } else if option.eq_ignore_ascii_case(b"COUNT") && index + 1 < args.len() {
            count = parse_due_count(args.bytes(index + 1))?;
            index += 2;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::SortedSetPopByScore {
        key: args.arg(0),
        min,
        max,
        count,
        with_scores,
    })
}

/// `ZMOVEDUE source destination max [COUNT n]`.
pub(in crate::protocol) fn parse_sorted_set_move_due<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 3 && args.len() != 5 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'zmovedue' command",
        ));
    }

    let max = parse_score_bound(args.bytes(2))?;
    let mut count = 1;

    if args.len() == 5 {
        if !args.bytes(3).eq_ignore_ascii_case(b"COUNT") {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        count = parse_due_count(args.bytes(4))?;
    }

    Ok(Command::SortedSetMoveDue {
        source: args.arg(0),
        destination: args.arg(1),
        max,
        count,
    })
}

pub(in crate::protocol) fn parse_sorted_set_remove_range<'a>(
    args: &Args<'a, '_>,
    name: &[u8],
) -> Parsed<'a> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            if name.eq_ignore_ascii_case(b"ZREMRANGEBYRANK") {
                "wrong number of arguments for 'zremrangebyrank' command"
            } else if name.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE") {
                "wrong number of arguments for 'zremrangebyscore' command"
            } else {
                "wrong number of arguments for 'zremrangebylex' command"
            },
        ));
    }

    let range = if name.eq_ignore_ascii_case(b"ZREMRANGEBYRANK") {
        let not_integer = |_| ProtocolError::Invalid("value is not an integer or out of range");

        SortedRemoveRange::Rank(
            parse_isize(args.bytes(1)).map_err(not_integer)?,
            parse_isize(args.bytes(2)).map_err(not_integer)?,
        )
    } else if name.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE") {
        let not_float = |_| ProtocolError::Invalid("min or max is not a float");

        SortedRemoveRange::Score(
            parse_score_bound(args.bytes(1)).map_err(not_float)?,
            parse_score_bound(args.bytes(2)).map_err(not_float)?,
        )
    } else {
        SortedRemoveRange::Lex(parse_lex_bound(args.arg(1))?, parse_lex_bound(args.arg(2))?)
    };

    Ok(Command::SortedSetRemoveRange {
        key: args.arg(0),
        range,
    })
}
