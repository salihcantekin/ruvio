//! Parsers for common commands. They were the first to read the frame directly instead of
//! an argument `Vec`, and match the vector-based parsers they replaced argument for argument
//! (same errors, same order). Every table row now reads [`Args`] the same way.

use super::table::Args;
use super::*;

type Parsed<'a> = Result<Command<'a>, ProtocolError>;

macro_rules! key_only {
    ($name:ident, $variant:path, $message:literal) => {
        pub(super) fn $name<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
            match args.len() {
                1 => Ok($variant(args.arg(0))),
                _ => Err(ProtocolError::Invalid($message)),
            }
        }
    };
}

key_only!(cmd_type, Command::Type, "TYPE expects one argument");
key_only!(cmd_llen, Command::ListLen, "LLEN expects one argument");
key_only!(cmd_scard, Command::SetCard, "SCARD expects a key");
key_only!(cmd_smembers, Command::SetMembers, "SMEMBERS expects a key");
key_only!(cmd_hlen, Command::HashLen, "HLEN expects one argument");
key_only!(
    cmd_hgetall,
    Command::HashGetAll,
    "HGETALL expects one argument"
);
key_only!(cmd_hkeys, Command::HashKeys, "HKEYS expects one argument");
key_only!(cmd_hvals, Command::HashValues, "HVALS expects one argument");
key_only!(
    cmd_zcard,
    Command::SortedSetCard,
    "ZCARD expects one argument"
);

const TTL_ARITY: &str = "TTL, PTTL, and PERSIST expect one argument";

pub(super) fn cmd_ttl<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::Ttl(args.arg(0))),
        _ => Err(ProtocolError::Invalid(TTL_ARITY)),
    }
}

pub(super) fn cmd_pttl<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::PTtl(args.arg(0))),
        _ => Err(ProtocolError::Invalid(TTL_ARITY)),
    }
}

pub(super) fn cmd_persist<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::Persist(args.arg(0))),
        _ => Err(ProtocolError::Invalid(TTL_ARITY)),
    }
}

pub(super) fn cmd_ping<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Ping(None)),
        1 => Ok(Command::Ping(Some(args.arg(0)))),
        _ => Err(ProtocolError::Invalid("PING expects zero or one argument")),
    }
}

pub(super) fn cmd_dbsize<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::DbSize),
        _ => Err(ProtocolError::Invalid("DBSIZE expects no arguments")),
    }
}

pub(super) fn cmd_select<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::Select(parse_i64(args.bytes(0))?)),
        _ => Err(ProtocolError::Invalid("SELECT expects a database index")),
    }
}

/// `EXPIRE` (seconds from now), `PEXPIRE` (milliseconds), `EXPIREAT` (unix seconds) or
/// `PEXPIREAT` (unix milliseconds).
fn expire<'a>(args: &Args<'a, '_>, seconds: bool, absolute: bool) -> Parsed<'a> {
    let condition = match args.len() {
        2 => ExpireCondition::Always,
        3 => match args.bytes(2) {
            option if option.eq_ignore_ascii_case(b"NX") => ExpireCondition::Nx,
            option if option.eq_ignore_ascii_case(b"XX") => ExpireCondition::Xx,
            option if option.eq_ignore_ascii_case(b"GT") => ExpireCondition::Gt,
            option if option.eq_ignore_ascii_case(b"LT") => ExpireCondition::Lt,
            _ => {
                return Err(ProtocolError::Invalid(
                    "expiration option must be NX, XX, GT or LT",
                ));
            }
        },
        _ => {
            return Err(ProtocolError::Invalid(
                "expiration commands expect a key, an integer and an optional NX, XX, GT or LT",
            ));
        }
    };

    let ttl = parse_i64(args.bytes(1))?;
    let key = args.arg(0);
    let milliseconds = if seconds {
        ttl.checked_mul(1_000)
            .ok_or(ProtocolError::Invalid("invalid expire time"))?
    } else {
        ttl
    };

    Ok(if absolute {
        Command::PExpireAt {
            key,
            unix_millis: milliseconds,
            condition,
        }
    } else {
        Command::Expire {
            key,
            milliseconds,
            condition,
        }
    })
}

pub(super) fn cmd_expire<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    expire(args, true, false)
}

pub(super) fn cmd_pexpire<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    expire(args, false, false)
}

pub(super) fn cmd_expireat<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    expire(args, true, true)
}

pub(super) fn cmd_pexpireat<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    expire(args, false, true)
}

fn list_pop<'a>(args: &Args<'a, '_>, back: bool) -> Parsed<'a> {
    const RANGE: &str = "value is out of range, must be positive";

    let count = match args.len() {
        1 => None,
        2 => {
            let count = usize::try_from(parse_i64(args.bytes(1))?)
                .map_err(|_| ProtocolError::Invalid(RANGE))?;

            if count == 0 {
                return Err(ProtocolError::Invalid(RANGE));
            }

            Some(count)
        }
        _ => {
            return Err(ProtocolError::Invalid(
                "LPOP and RPOP expect a key and an optional count",
            ));
        }
    };
    let key = args.arg(0);

    Ok(if back {
        Command::ListPopBack { key, count }
    } else {
        Command::ListPop { key, count }
    })
}

pub(super) fn cmd_lpop<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    list_pop(args, false)
}

pub(super) fn cmd_rpop<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    list_pop(args, true)
}

pub(super) fn cmd_lrange<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        3 => {
            let stop = parse_isize(args.bytes(2))?;
            let start = parse_isize(args.bytes(1))?;

            Ok(Command::ListRange(args.arg(0), start, stop))
        }
        _ => Err(ProtocolError::Invalid("LRANGE expects three arguments")),
    }
}

pub(super) fn cmd_ltrim<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        3 => {
            let stop = parse_isize(args.bytes(2))?;
            let start = parse_isize(args.bytes(1))?;

            Ok(Command::ListTrim(args.arg(0), start, stop))
        }
        _ => Err(ProtocolError::Invalid("LTRIM expects three arguments")),
    }
}

pub(super) fn cmd_lindex<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::ListIndex(args.arg(0), parse_isize(args.bytes(1))?)),
        _ => Err(ProtocolError::Invalid("LINDEX expects a key and an index")),
    }
}

pub(super) fn cmd_sismember<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::SetContains(args.arg(0), args.arg(1))),
        _ => Err(ProtocolError::Invalid("SISMEMBER expects two arguments")),
    }
}

pub(super) fn cmd_sadd<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "SADD expects a key and at least one member",
        ));
    }

    Ok(Command::SetAdd(args.arg(0), args.rest(1)))
}

pub(super) fn cmd_srem<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "SREM expects a key and at least one member",
        ));
    }

    Ok(Command::SetRemove(args.arg(0), args.rest(1)))
}

pub(super) fn cmd_hget<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::HashGet {
            key: args.arg(0),
            field: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid("HGET expects a key and field")),
    }
}

pub(super) fn cmd_hexists<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::HashExists {
            key: args.arg(0),
            field: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid("HEXISTS expects a key and field")),
    }
}

pub(super) fn cmd_hdel<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "HDEL expects a key and at least one field",
        ));
    }

    Ok(Command::HashDelete {
        key: args.arg(0),
        fields: args.rest(1),
    })
}

pub(super) fn cmd_hmget<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "HMGET expects a key and at least one field",
        ));
    }

    Ok(Command::HashMGet {
        key: args.arg(0),
        fields: args.rest(1),
    })
}

pub(super) fn cmd_hincrby<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let ttl = match args.len() {
        3 => None,
        5 => Some(create_ttl_option(args, 3)?.ok_or(ProtocolError::Invalid("syntax error"))?),
        _ => {
            return Err(ProtocolError::Invalid(
                "HINCRBY expects a key, field, increment and an optional EX or PX",
            ));
        }
    };
    let delta = parse_i64(args.bytes(2))?;

    Ok(with_create_ttl(
        ttl,
        Command::HashIncrement {
            key: args.arg(0),
            field: args.arg(1),
            delta,
        },
    ))
}

pub(super) fn cmd_hsetnx<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        3 => Ok(Command::HashSetNx {
            key: args.arg(0),
            field: args.arg(1),
            value: args.arg(2),
        }),
        _ => Err(ProtocolError::Invalid(
            "HSETNX expects a key, field, and value",
        )),
    }
}

pub(super) fn cmd_zscore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::SortedSetScore {
            key: args.arg(0),
            member: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid("ZSCORE expects a key and member")),
    }
}

fn zrank<'a>(args: &Args<'a, '_>, reverse: bool) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::SortedSetRank {
            key: args.arg(0),
            member: args.arg(1),
            reverse,
        }),
        _ => Err(ProtocolError::Invalid(
            "ZRANK/ZREVRANK expect a key and member",
        )),
    }
}

pub(super) fn cmd_zrank<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    zrank(args, false)
}

pub(super) fn cmd_zrevrank<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    zrank(args, true)
}

pub(super) fn cmd_zrem<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "ZREM expects a key and at least one member",
        ));
    }

    Ok(Command::SortedSetRemove {
        key: args.arg(0),
        members: args.rest(1),
    })
}

pub(super) fn cmd_zincrby<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let ttl = match args.len() {
        3 => None,
        5 => Some(create_ttl_option(args, 3)?.ok_or(ProtocolError::Invalid("syntax error"))?),
        _ => {
            return Err(ProtocolError::Invalid(
                "ZINCRBY expects a key, increment, member and an optional EX or PX",
            ));
        }
    };
    let delta = parse_f64(args.bytes(1))?;

    Ok(with_create_ttl(
        ttl,
        Command::SortedSetIncr {
            key: args.arg(0),
            delta,
            member: args.arg(2),
        },
    ))
}

pub(super) fn cmd_mset<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 || !args.len().is_multiple_of(2) {
        return Err(ProtocolError::Invalid("MSET expects key/value pairs"));
    }

    Ok(Command::MSet(
        (0..args.len() / 2)
            .map(|pair| (args.arg(pair * 2), args.arg(pair * 2 + 1)))
            .collect(),
    ))
}

pub(super) fn cmd_del<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("DEL expects at least one argument"));
    }

    Ok(Command::Del(super::table::keys_of(args)))
}

pub(super) fn cmd_unlink<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "UNLINK expects at least one argument",
        ));
    }

    Ok(Command::Del(super::table::keys_of(args)))
}

pub(super) fn cmd_spop<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::SetPop {
            key: args.arg(0),
            count: None,
        }),
        2 => {
            const RANGE: &str = "value is out of range";

            let count = parse_i64(args.bytes(1))?;

            if count < 0 {
                return Err(ProtocolError::Invalid(RANGE));
            }

            let count = usize::try_from(count).map_err(|_| ProtocolError::Invalid(RANGE))?;

            Ok(Command::SetPop {
                key: args.arg(0),
                count: Some(count),
            })
        }
        _ => Err(ProtocolError::Invalid(
            "SPOP expects a key and an optional count",
        )),
    }
}

pub(super) fn cmd_srandmember<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::SetRandom {
            key: args.arg(0),
            count: None,
        }),
        2 => {
            let count = super::table::check_random_count(parse_i64(args.bytes(1))?)?;

            Ok(Command::SetRandom {
                key: args.arg(0),
                count: Some(count),
            })
        }
        _ => Err(ProtocolError::Invalid(
            "SRANDMEMBER expects a key and an optional count",
        )),
    }
}

pub(super) fn cmd_hstrlen<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::HashStrLen {
            key: args.arg(0),
            field: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid("HSTRLEN expects a key and a field")),
    }
}

key_only!(cmd_xlen, Command::StreamLen, "XLEN expects a key");

pub(super) fn cmd_rename<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::Rename {
            source: args.arg(0),
            destination: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid(
            "RENAME expects a source and a destination",
        )),
    }
}

pub(super) fn cmd_publish<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::Publish {
            channel: args.arg(0),
            message: args.arg(1),
        }),
        _ => Err(ProtocolError::Invalid(
            "PUBLISH expects a channel and a message",
        )),
    }
}
