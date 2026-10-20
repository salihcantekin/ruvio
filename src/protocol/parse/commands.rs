//! Per-command parsers carried over from the former `parse_arguments` chain. The table
//! (`protocol/table.rs`) dispatches to them; each reads its arguments from [`Args`] (straight
//! from the input frame when there is one). A body several commands share takes the
//! command name as well, and the table gives each of those rows a small wrapper.

use super::*;

pub(in crate::protocol) fn cmd_wait<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "WAIT expects a replica count and a timeout",
        ));
    }

    let replicas = parse_i64(args.bytes(0))?;
    let timeout = parse_i64(args.bytes(1))?;

    if replicas < 0 {
        return Err(ProtocolError::Invalid("WAIT replica count is out of range"));
    }

    if timeout < 0 {
        return Err(ProtocolError::Invalid("WAIT timeout is negative"));
    }

    Ok(Command::Wait {
        replicas: replicas as u64,
        timeout_ms: timeout as u64,
    })
}

pub(in crate::protocol) fn cmd_ruvio<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() >= 1 && args.bytes(0).eq_ignore_ascii_case(b"MOVESLOTS") {
        return parse_moveslots(args);
    }
    if args.len() >= 1 && args.bytes(0).eq_ignore_ascii_case(b"TOPOLOGY") {
        let is = |word: &[u8]| args.bytes(1).eq_ignore_ascii_case(word);

        return match args.len() {
            2 if is(b"VERSION") => Ok(Command::Topology(None)),
            3 if is(b"SET") => Ok(Command::Topology(Some(args.arg(2)))),
            _ => Err(ProtocolError::Invalid(
                "RUVIO TOPOLOGY expects VERSION, or SET map",
            )),
        };
    }

    if args.len() >= 1 && args.bytes(0).eq_ignore_ascii_case(b"LEASE") {
        return parse_lease(args);
    }

    if args.len() >= 2 && args.bytes(0).eq_ignore_ascii_case(b"MIGRATE") {
        return Ok(Command::Migrate(
            (1..args.len()).map(|at| args.arg(at)).collect(),
        ));
    }

    match args.len() {
        0 => Ok(Command::Ruvio(None)),
        1 => Ok(Command::Ruvio(Some(args.arg(0)))),
        _ => Err(ProtocolError::Invalid("RUVIO expects zero or one argument")),
    }
}

/// `RUVIO LEASE BEGIN` or `RUVIO LEASE GRANT token coordinator voters epoch lease_ms`.
fn parse_lease<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    const USAGE: &str =
        "RUVIO LEASE expects BEGIN, or GRANT token coordinator voters epoch lease_ms";
    let is = |word: &[u8]| args.len() >= 2 && args.bytes(1).eq_ignore_ascii_case(word);
    let number = |at: usize| {
        std::str::from_utf8(args.bytes(at))
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(ProtocolError::Invalid(USAGE))
    };

    match args.len() {
        2 if is(b"BEGIN") => Ok(Command::PrimaryLease(None)),
        7 if is(b"GRANT") => Ok(Command::PrimaryLease(Some(Box::new(
            crate::protocol::LeaseGrant {
                token: number(2)?,
                coordinator: number(3)?,
                voters: u32::try_from(number(4)?).map_err(|_| ProtocolError::Invalid(USAGE))?,
                epoch: number(5)?,
                lease_ms: number(6)?,
            },
        )))),
        _ => Err(ProtocolError::Invalid(USAGE)),
    }
}

/// `RUVIO MOVESLOTS first-last shard`, `RUVIO MOVESLOTS slot shard` or `RUVIO MOVESLOTS STATUS`.
fn parse_moveslots<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    const USAGE: &str = "RUVIO MOVESLOTS expects first-last shard, or STATUS";
    let slot = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse::<u16>().ok())
            .filter(|slot| usize::from(*slot) < crate::routing::HASH_SLOT_COUNT)
            .ok_or(ProtocolError::Invalid(
                "RUVIO MOVESLOTS slot is not between 0 and 16383",
            ))
    };

    match args.len() {
        2 if args.bytes(1).eq_ignore_ascii_case(b"STATUS") => Ok(Command::MoveSlots(None)),
        3 => {
            let range = args.bytes(1);
            let (first, last) = match range.iter().position(|byte| *byte == b'-') {
                Some(dash) => (slot(&range[..dash])?, slot(&range[dash + 1..])?),
                None => {
                    let only = slot(range)?;

                    (only, only)
                }
            };
            let shard = std::str::from_utf8(args.bytes(2))
                .ok()
                .and_then(|text| text.parse::<u16>().ok())
                .ok_or(ProtocolError::Invalid(
                    "RUVIO MOVESLOTS shard is not a shard index",
                ))?;

            if first > last {
                return Err(ProtocolError::Invalid(
                    "RUVIO MOVESLOTS range ends before it starts",
                ));
            }

            Ok(Command::MoveSlots(Some(crate::protocol::SlotRangeMove {
                first,
                last,
                shard,
            })))
        }
        _ => Err(ProtocolError::Invalid(USAGE)),
    }
}

pub(in crate::protocol) fn cmd_cluster<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let is = |word: &[u8]| args.bytes(0).eq_ignore_ascii_case(word);

    match args.len() {
        1 if is(b"SLOTS") => Ok(Command::ClusterSlots),
        1 if is(b"NODES") => Ok(Command::ClusterNodes),
        1 if is(b"INFO") => Ok(Command::ClusterInfo),
        1 if is(b"SHARDS") => Ok(Command::ClusterShards),
        1 if is(b"MYID") => Ok(Command::ClusterMyId),
        2 if is(b"KEYSLOT") => Ok(Command::ClusterKeySlot(args.arg(1))),
        _ => Err(ProtocolError::Invalid(
            "only CLUSTER SLOTS, NODES, SHARDS, INFO, MYID, and KEYSLOT are supported",
        )),
    }
}

pub(in crate::protocol) fn cmd_readonly<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 0 {
        return Err(ProtocolError::Invalid(
            "READONLY and READWRITE take no arguments",
        ));
    }

    Ok(if name.eq_ignore_ascii_case(b"READONLY") {
        Command::ReadOnly
    } else {
        Command::ReadWrite
    })
}

pub(in crate::protocol) fn cmd_config<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'config' command",
        ));
    }

    let subcommand = args.bytes(0);

    if args.len() == 2 && subcommand.eq_ignore_ascii_case(b"GET") {
        return Ok(Command::ConfigGet(args.arg(1)));
    }

    if subcommand.eq_ignore_ascii_case(b"SET") {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'config|set' command",
            ));
        }

        let pairs = (1..args.len())
            .step_by(2)
            .map(|index| (args.arg(index), args.arg(index + 1)))
            .collect();

        return Ok(Command::ConfigSet(pairs));
    }

    if subcommand.eq_ignore_ascii_case(b"REWRITE") {
        if args.len() != 1 {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'config|rewrite' command",
            ));
        }

        return Ok(Command::ConfigRewrite);
    }

    Err(ProtocolError::Invalid(
        "only CONFIG GET, SET and REWRITE are supported",
    ))
}

/// `REPLICAOF host port` or `REPLICAOF NO ONE`; `REPLICAOF NO ONE FORCE` promotes a replica
/// that has not finished copying its primary.
pub(in crate::protocol) fn cmd_replicaof<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let no_one = args.len() >= 2
        && args.bytes(0).eq_ignore_ascii_case(b"NO")
        && args.bytes(1).eq_ignore_ascii_case(b"ONE");

    match args.len() {
        2 if no_one => Ok(Command::ReplicaOf {
            primary: None,
            force: false,
        }),
        3 if no_one && args.bytes(2).eq_ignore_ascii_case(b"FORCE") => Ok(Command::ReplicaOf {
            primary: None,
            force: true,
        }),
        2 => Ok(Command::ReplicaOf {
            primary: Some((args.arg(0), args.arg(1))),
            force: false,
        }),
        _ => Err(ProtocolError::Invalid(
            "wrong number of arguments for 'replicaof' command",
        )),
    }
}

pub(in crate::protocol) fn cmd_role<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 0 {
        return Err(ProtocolError::Invalid(
            "wrong number of arguments for 'role' command",
        ));
    }

    Ok(Command::Role)
}

pub(in crate::protocol) fn cmd_info<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Info(None)),
        1 => Ok(Command::Info(Some(args.arg(0)))),
        _ => Err(ProtocolError::Invalid("INFO expects zero or one argument")),
    }
}

pub(in crate::protocol) fn cmd_save<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Save),
        _ => Err(ProtocolError::Invalid("SAVE expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_bgsave<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Bgsave),
        _ => Err(ProtocolError::Invalid("BGSAVE expects no arguments")),
    }
}

/// RedisJSON's legacy paths start at `.` or `[` (or are just `.`); store them as `$` paths.
fn json_path(path: Bytes<'_>) -> Bytes<'_> {
    match path.first() {
        Some(b'.') if path.len() == 1 => Cow::Borrowed(b"$"),
        Some(b'.' | b'[') => {
            let mut normalized = Vec::with_capacity(path.len() + 1);
            normalized.push(b'$');
            normalized.extend_from_slice(&path);
            Cow::Owned(normalized)
        }
        _ => path,
    }
}

pub(in crate::protocol) fn cmd_json_op<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    use crate::protocol::JsonOpKind as Op;
    let op = [
        (&b"JSON.TYPE"[..], Op::Type),
        (b"JSON.STRLEN", Op::StrLen),
        (b"JSON.ARRLEN", Op::ArrLen),
        (b"JSON.OBJKEYS", Op::ObjKeys),
        (b"JSON.NUMINCRBY", Op::NumIncrBy),
        (b"JSON.TOGGLE", Op::Toggle),
        (b"JSON.ARRAPPEND", Op::ArrAppend),
        (b"JSON.CLEAR", Op::Clear),
        (b"JSON.OBJLEN", Op::ObjLen),
        (b"JSON.STRAPPEND", Op::StrAppend),
        (b"JSON.NUMMULTBY", Op::NumMultBy),
        (b"JSON.ARRINSERT", Op::ArrInsert),
        (b"JSON.ARRPOP", Op::ArrPop),
        (b"JSON.ARRTRIM", Op::ArrTrim),
        (b"JSON.ARRINDEX", Op::ArrIndex),
    ]
    .into_iter()
    .find(|(candidate, _)| name.eq_ignore_ascii_case(candidate))
    .map(|(_, op)| op)
    .ok_or(ProtocolError::Invalid("unknown JSON command"))?;
    let (minimum, maximum) = match op {
        Op::NumIncrBy | Op::NumMultBy | Op::StrAppend => (3, 3),
        Op::ArrAppend => (3, usize::MAX),
        Op::ArrInsert => (4, usize::MAX),
        Op::ArrTrim => (4, 4),
        Op::ArrIndex => (3, 5),
        Op::ArrPop => (1, 3),
        _ => (1, 2),
    };
    if args.len() < minimum || args.len() > maximum {
        return Err(ProtocolError::Invalid(
            "JSON command has the wrong number of arguments",
        ));
    }
    let path = if args.len() == 1 {
        Cow::Borrowed(&b"$"[..])
    } else {
        json_path(args.arg(1))
    };
    Ok(Command::JsonOp {
        op,
        key: args.arg(0),
        path,
        args: args.rest(2),
    })
}

pub(in crate::protocol) fn cmd_json_index<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    use crate::protocol::JsonIndexKind;

    if name.eq_ignore_ascii_case(b"JSON.FIND") {
        if args.len() < 3 {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'json.find' command",
            ));
        }

        return Ok(Command::JsonIndex {
            op: JsonIndexKind::Find,
            args: args.rest(0),
        });
    }

    if args.len() == 0 {
        return Err(ProtocolError::Invalid("JSON.INDEX expects a subcommand"));
    }
    let sub = args.bytes(0);
    let op = if sub.eq_ignore_ascii_case(b"CREATE") {
        JsonIndexKind::Create
    } else if sub.eq_ignore_ascii_case(b"DROP") {
        JsonIndexKind::Drop
    } else if sub.eq_ignore_ascii_case(b"LIST") {
        JsonIndexKind::List
    } else {
        return Err(ProtocolError::Invalid(
            "JSON.INDEX subcommand must be CREATE, DROP or LIST",
        ));
    };

    Ok(Command::JsonIndex {
        op,
        args: args.rest(1),
    })
}

pub(in crate::protocol) fn cmd_json_get<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 | 2 => {
            let key = args.arg(0);
            let path = json_path(if args.len() == 2 {
                args.arg(1)
            } else {
                Cow::Borrowed(b"$")
            });
            if name.eq_ignore_ascii_case(b"JSON.GET") {
                Ok(Command::JsonGet { key, path })
            } else {
                Ok(Command::JsonDel { key, path })
            }
        }
        _ => Err(ProtocolError::Invalid(
            "JSON.GET/JSON.DEL expects key and optional path",
        )),
    }
}

pub(in crate::protocol) fn cmd_json_mget<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "JSON.MGET expects at least one key and a path",
        ));
    }

    let last = args.len() - 1;

    Ok(Command::JsonMGet {
        keys: (0..last).map(|index| args.arg(index)).collect(),
        path: json_path(args.arg(last)),
    })
}

pub(in crate::protocol) fn cmd_json_set<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    // `JSON.SET key path value [NX|XX] [EX s | PX ms]`: the deadline is for a key it creates.
    let (length, ttl) = match args.len() {
        length @ (5 | 6) => match create_ttl_option(args, length - 2)? {
            Some(ttl) => (length - 2, Some(ttl)),
            None => return Err(ProtocolError::Invalid("syntax error")),
        },
        length => (length, None),
    };

    let command = match length {
        3 => Ok(Command::JsonSet {
            key: args.arg(0),
            path: json_path(args.arg(1)),
            value: args.arg(2),
        }),
        4 => {
            let condition = args.bytes(3);
            let must_exist = if condition.eq_ignore_ascii_case(b"XX") {
                true
            } else if condition.eq_ignore_ascii_case(b"NX") {
                false
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };

            Ok(Command::JsonSetIf {
                key: args.arg(0),
                path: json_path(args.arg(1)),
                value: args.arg(2),
                must_exist,
            })
        }
        _ => Err(ProtocolError::Invalid(
            "JSON.SET expects key, path, JSON value, an optional NX or XX and an optional EX or PX",
        )),
    }?;

    Ok(with_create_ttl(ttl, command))
}

/// `JSON.MERGE` and its `JMERGE` alias.
pub(in crate::protocol) fn cmd_json_merge<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        3 => Ok(Command::JsonMerge {
            key: args.arg(0),
            path: json_path(args.arg(1)),
            value: args.arg(2),
        }),
        _ => Err(ProtocolError::Invalid(
            "JSON.MERGE expects key, path, and JSON patch",
        )),
    }
}

pub(in crate::protocol) fn cmd_lease<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "LEASE expects a key and a TTL in milliseconds",
        ));
    }

    let ttl_ms = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("LEASE TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "LEASE TTL must be a positive integer",
        ));
    }

    Ok(Command::Lease {
        key: args.arg(0),
        ttl_ms,
    })
}

pub(in crate::protocol) fn cmd_semaphore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "SEMAPHORE expects a key, a maximum, and a TTL in milliseconds",
        ));
    }

    let max = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("SEMAPHORE maximum must be a positive integer"))?;
    let ttl_ms = parse_u64(args.bytes(2))
        .map_err(|_| ProtocolError::Invalid("SEMAPHORE TTL must be a positive integer"))?;

    if max == 0 || max > u64::from(u32::MAX) || ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "SEMAPHORE maximum and TTL must be positive integers",
        ));
    }

    Ok(Command::Semaphore {
        key: args.arg(0),
        max,
        ttl_ms,
    })
}

pub(in crate::protocol) fn cmd_limit<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "LIMIT expects a key, a maximum, and a window in milliseconds",
        ));
    }

    let max = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("LIMIT maximum must be a positive integer"))?;
    let window_ms = parse_u64(args.bytes(2))
        .map_err(|_| ProtocolError::Invalid("LIMIT window must be a positive integer"))?;

    if max == 0 || window_ms == 0 {
        return Err(ProtocolError::Invalid(
            "LIMIT maximum and window must be positive integers",
        ));
    }

    let mode = if args.len() == 3 {
        LimitMode::Fixed
    } else {
        parse_limit_mode(args, max, window_ms)?
    };

    Ok(Command::Limit {
        key: args.arg(0),
        max,
        window_ms,
        mode,
    })
}

/// The trailing `SLIDING | TOKENS [COST n] [WITHRETRY]` of `LIMIT`. Only reached with more
/// than three arguments, so the plain form never pays for it.
fn parse_limit_mode(
    args: &Args<'_, '_>,
    max: u64,
    window_ms: u64,
) -> Result<LimitMode, ProtocolError> {
    use crate::store::{LIMIT_SLIDING_MAX, LIMIT_TOKENS_MAX_CAPACITY, LIMIT_TOKENS_MAX_REFILL_MS};

    let tokens = if args.bytes(3).eq_ignore_ascii_case(b"SLIDING") {
        false
    } else if args.bytes(3).eq_ignore_ascii_case(b"TOKENS") {
        true
    } else {
        return Err(ProtocolError::Invalid(
            "LIMIT mode must be SLIDING or TOKENS",
        ));
    };
    let mut cost = None;
    let mut with_retry = false;
    let mut index = 4;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"WITHRETRY") && !with_retry {
            with_retry = true;
            index += 1;
        } else if tokens && option.eq_ignore_ascii_case(b"COST") && cost.is_none() {
            if index + 1 >= args.len() {
                return Err(ProtocolError::Invalid("LIMIT COST expects a token count"));
            }

            cost =
                Some(parse_u64(args.bytes(index + 1)).map_err(|_| {
                    ProtocolError::Invalid("LIMIT COST must be a positive integer")
                })?);
            index += 2;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    if !tokens {
        if max > LIMIT_SLIDING_MAX {
            return Err(ProtocolError::Invalid(
                "LIMIT SLIDING maximum must be at most 2147483647",
            ));
        }

        return Ok(LimitMode::Sliding { with_retry });
    }

    if max > LIMIT_TOKENS_MAX_CAPACITY {
        return Err(ProtocolError::Invalid(
            "LIMIT TOKENS capacity must be at most 1000000",
        ));
    }

    if window_ms > LIMIT_TOKENS_MAX_REFILL_MS {
        return Err(ProtocolError::Invalid(
            "LIMIT TOKENS refill must be at most 4294967295 milliseconds",
        ));
    }

    let cost = cost.unwrap_or(1);

    if cost == 0 || cost > max {
        return Err(ProtocolError::Invalid(
            "LIMIT COST must be between 1 and the capacity",
        ));
    }

    Ok(LimitMode::Tokens {
        cost: cost as u32,
        with_retry,
    })
}

pub(in crate::protocol) fn cmd_idem<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 5 {
        return Err(ProtocolError::Invalid(
            "IDEM expects key BEGIN fingerprint owner ttl_ms or key COMPLETE fingerprint owner result",
        ));
    }
    let action = args.bytes(1);
    let fingerprint = args.bytes(2);
    let owner = args.bytes(3);
    if fingerprint.is_empty() || fingerprint.len() > 128 || owner.is_empty() || owner.len() > 128 {
        return Err(ProtocolError::Invalid(
            "IDEM fingerprint and owner must contain 1-128 bytes",
        ));
    }
    let value = args.bytes(4);
    if action.eq_ignore_ascii_case(b"BEGIN") {
        let ttl_ms = parse_u64(value)?;
        if ttl_ms == 0 || ttl_ms > MAX_IDEMPOTENCY_TTL_MS {
            return Err(ProtocolError::Invalid(
                "IDEM TTL must be 1-604800000 milliseconds",
            ));
        }
        return Ok(Command::IdempotencyBegin {
            key: args.arg(0),
            fingerprint: args.arg(2),
            owner: args.arg(3),
            ttl_ms,
        });
    }
    if action.eq_ignore_ascii_case(b"COMPLETE") {
        if value.len() > MAX_IDEMPOTENCY_RESULT_BYTES {
            return Err(ProtocolError::Invalid("IDEM result exceeds 4 MiB"));
        }
        return Ok(Command::IdempotencyComplete {
            key: args.arg(0),
            fingerprint: args.arg(2),
            owner: args.arg(3),
            result: args.arg(4),
        });
    }
    Err(ProtocolError::Invalid(
        "IDEM action must be BEGIN or COMPLETE",
    ))
}

pub(in crate::protocol) fn cmd_once<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "ONCE expects a key and a TTL in milliseconds",
        ));
    }

    let ttl_ms = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("ONCE TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "ONCE TTL must be a positive integer",
        ));
    }

    Ok(Command::Once {
        key: args.arg(0),
        ttl_ms,
    })
}

pub(in crate::protocol) fn cmd_take<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "TAKE expects a key and a positive count",
        ));
    }

    let count = parse_i64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("TAKE count must be a positive integer"))?;

    if count <= 0 {
        return Err(ProtocolError::Invalid(
            "TAKE count must be a positive integer",
        ));
    }

    Ok(Command::Take {
        key: args.arg(0),
        count,
    })
}

/// Trailing `[MAX cap]` of `TRANSFER` and `HTRANSFER`, from argument `at`.
fn transfer_max(args: &Args<'_, '_>, at: usize) -> Result<Option<i64>, ProtocolError> {
    if args.len() == at {
        return Ok(None);
    }

    if args.len() != at + 2 || !args.bytes(at).eq_ignore_ascii_case(b"MAX") {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    parse_i64(args.bytes(at + 1)).map(Some)
}

fn transfer_amount(args: &Args<'_, '_>, at: usize) -> Result<i64, ProtocolError> {
    match parse_i64(args.bytes(at)) {
        Ok(amount) if amount > 0 => Ok(amount),
        _ => Err(ProtocolError::Invalid(
            "transfer amount must be a positive integer",
        )),
    }
}

pub(in crate::protocol) fn cmd_transfer<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "TRANSFER expects a source, a destination and an amount",
        ));
    }

    if args.bytes(0) == args.bytes(1) {
        return Err(ProtocolError::Invalid(
            "TRANSFER source and destination must differ",
        ));
    }

    Ok(Command::Transfer {
        source: args.arg(0),
        destination: args.arg(1),
        amount: transfer_amount(args, 2)?,
        max: transfer_max(args, 3)?,
    })
}

pub(in crate::protocol) fn cmd_htransfer<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 4 {
        return Err(ProtocolError::Invalid(
            "HTRANSFER expects a key, two fields and an amount",
        ));
    }

    if args.bytes(1) == args.bytes(2) {
        return Err(ProtocolError::Invalid("HTRANSFER fields must differ"));
    }

    Ok(Command::HashTransfer {
        key: args.arg(0),
        from: args.arg(1),
        to: args.arg(2),
        amount: transfer_amount(args, 3)?,
        max: transfer_max(args, 4)?,
    })
}

pub(in crate::protocol) fn cmd_getex<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid(
            "GETEX expects a key and a TTL in milliseconds",
        ));
    }

    let ttl_ms = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("GETEX TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "GETEX TTL must be a positive integer",
        ));
    }

    Ok(Command::GetEx {
        key: args.arg(0),
        ttl_ms,
    })
}

pub(in crate::protocol) fn cmd_release<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 {
        return Err(ProtocolError::Invalid("RELEASE expects a key and a token"));
    }

    let token = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("RELEASE token must be an integer"))?;

    Ok(Command::Release {
        key: args.arg(0),
        token,
    })
}

pub(in crate::protocol) fn cmd_renew<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "RENEW expects a key, a token, and a TTL in milliseconds",
        ));
    }

    let token = parse_u64(args.bytes(1))
        .map_err(|_| ProtocolError::Invalid("RENEW token must be an integer"))?;
    let ttl_ms = parse_u64(args.bytes(2))
        .map_err(|_| ProtocolError::Invalid("RENEW TTL must be a positive integer"))?;

    if ttl_ms == 0 {
        return Err(ProtocolError::Invalid(
            "RENEW TTL must be a positive integer",
        ));
    }

    Ok(Command::Renew {
        key: args.arg(0),
        token,
        ttl_ms,
    })
}

pub(in crate::protocol) fn cmd_setv<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 2 && args.len() != 3 {
        return Err(ProtocolError::Invalid(
            "SETV expects a key, a value, and an optional version",
        ));
    }

    let expected = if args.len() == 2 {
        None
    } else {
        Some(
            parse_u64(args.bytes(2))
                .map_err(|_| ProtocolError::Invalid("SETV version must be an integer"))?,
        )
    };

    Ok(Command::SetVersioned {
        key: args.arg(0),
        value: args.arg(1),
        expected,
    })
}

pub(in crate::protocol) fn cmd_changes<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() > 0 && args.bytes(0).eq_ignore_ascii_case(b"SUBSCRIBE") {
        let options: Vec<&[u8]> = (1..args.len()).map(|index| args.bytes(index)).collect();

        return Ok(Command::ChangesSubscribe(
            crate::changes::ChangeSubscription::parse(&options)?,
        ));
    }

    match args.len() {
        1 if args.bytes(0).eq_ignore_ascii_case(b"START") => Ok(Command::Changes(None)),
        1 => Ok(Command::Changes(Some(parse_u64(args.bytes(0)).map_err(
            |_| ProtocolError::Invalid("CHANGES expects START or a cursor"),
        )?))),
        _ => Err(ProtocolError::Invalid("CHANGES expects START or a cursor")),
    }
}

pub(in crate::protocol) fn cmd_flushdb<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    let all = name.eq_ignore_ascii_case(b"FLUSHALL");
    let mode = |mode: &[u8]| args.bytes(0).eq_ignore_ascii_case(mode);

    match args.len() {
        0 => Ok(if all {
            Command::FlushAll
        } else {
            Command::FlushDb
        }),
        1 if mode(b"ASYNC") || mode(b"SYNC") => Ok(if all {
            Command::FlushAll
        } else {
            Command::FlushDb
        }),

        _ => Err(ProtocolError::Invalid("syntax error")),
    }
}

pub(in crate::protocol) fn cmd_swapdb<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::SwapDb(
            parse_i64(args.bytes(0))?,
            parse_i64(args.bytes(1))?,
        )),
        _ => Err(ProtocolError::Invalid(
            "SWAPDB expects two database indexes",
        )),
    }
}

pub(in crate::protocol) fn cmd_move<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::Move {
            key: args.arg(0),
            db: parse_i64(args.bytes(1))?,
        }),

        _ => Err(ProtocolError::Invalid(
            "MOVE expects a key and a database index",
        )),
    }
}

pub(in crate::protocol) fn cmd_subscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid(
            "SUBSCRIBE expects at least one channel",
        ))
    } else {
        Ok(Command::Subscribe(args.rest(0)))
    }
}

pub(in crate::protocol) fn cmd_unsubscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    Ok(Command::Unsubscribe(args.rest(0)))
}

pub(in crate::protocol) fn cmd_psubscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid(
            "PSUBSCRIBE expects at least one pattern",
        ))
    } else {
        Ok(Command::PSubscribe(args.rest(0)))
    }
}

pub(in crate::protocol) fn cmd_punsubscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    Ok(Command::PUnsubscribe(args.rest(0)))
}

pub(in crate::protocol) fn cmd_spublish<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => Ok(Command::ShardPublish {
            channel: args.arg(0),
            message: args.arg(1),
        }),

        _ => Err(ProtocolError::Invalid(
            "SPUBLISH expects a channel and a message",
        )),
    }
}

pub(in crate::protocol) fn cmd_ssubscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        Err(ProtocolError::Invalid(
            "SSUBSCRIBE expects at least one channel",
        ))
    } else {
        Ok(Command::ShardSubscribe(args.rest(0)))
    }
}

pub(in crate::protocol) fn cmd_sunsubscribe<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    Ok(Command::ShardUnsubscribe(args.rest(0)))
}

pub(in crate::protocol) fn cmd_auth<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        1 => Ok(Command::Auth {
            username: None,
            password: args.arg(0),
        }),
        2 => Ok(Command::Auth {
            username: Some(args.arg(0)),
            password: args.arg(1),
        }),

        _ => Err(ProtocolError::Invalid("AUTH expects a password")),
    }
}

pub(in crate::protocol) fn cmd_blpop<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_list_block(args, name.eq_ignore_ascii_case(b"BRPOP"))
}

pub(in crate::protocol) fn cmd_sinter<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Inter, false)
}

pub(in crate::protocol) fn cmd_sunion<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Union, false)
}

pub(in crate::protocol) fn cmd_sdiff<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Diff, false)
}

pub(in crate::protocol) fn cmd_sinterstore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Inter, true)
}

pub(in crate::protocol) fn cmd_sunionstore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Union, true)
}

pub(in crate::protocol) fn cmd_sdiffstore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_set_algebra(args, SetOp::Diff, true)
}

pub(in crate::protocol) fn cmd_smove<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        3 => Ok(Command::SetMove {
            source: args.arg(0),
            destination: args.arg(1),
            member: args.arg(2),
        }),

        _ => Err(ProtocolError::Invalid(
            "SMOVE expects a source, destination, and member",
        )),
    }
}

pub(in crate::protocol) fn cmd_sscan<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_collection_scan(args, CollectionScanKind::Set)
}

pub(in crate::protocol) fn cmd_zscan<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_collection_scan(args, CollectionScanKind::SortedSet)
}

pub(in crate::protocol) fn cmd_hexpire<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_hash_expire(args, name)
}

pub(in crate::protocol) fn cmd_httl<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_hash_field_read(args, name)
}

pub(in crate::protocol) fn cmd_zrange<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_range(args, name.eq_ignore_ascii_case(b"ZREVRANGE"))
}

pub(in crate::protocol) fn cmd_zrangebylex<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_range_by_lex(args, name.eq_ignore_ascii_case(b"ZREVRANGEBYLEX"))
}

pub(in crate::protocol) fn cmd_zpopmin<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_pop(args, name.eq_ignore_ascii_case(b"ZPOPMAX"))
}

pub(in crate::protocol) fn cmd_zpopbyscore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_pop_by_score(args)
}

pub(in crate::protocol) fn cmd_zmovedue<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_move_due(args)
}

pub(in crate::protocol) fn cmd_bzpopmin<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    let max = name.eq_ignore_ascii_case(b"BZPOPMAX");

    if args.len() < 2 {
        return Err(ProtocolError::Invalid(if max {
            "wrong number of arguments for 'bzpopmax' command"
        } else {
            "wrong number of arguments for 'bzpopmin' command"
        }));
    }

    let last = args.len() - 1;
    let timeout_ms = parse_timeout_ms(args.bytes(last))?;

    Ok(Command::SortedSetBlockPop {
        keys: (0..last).map(|index| args.arg(index)).collect(),
        timeout_ms,
        max,
    })
}

pub(in crate::protocol) fn cmd_zremrangebyrank<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_sorted_set_remove_range(args, name)
}

pub(in crate::protocol) fn cmd_geosearch<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_geo_search(args, false)
}

pub(in crate::protocol) fn cmd_geosearchstore<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_geo_search(args, true)
}

pub(in crate::protocol) fn cmd_georadius<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_geo_radius(args, false)
}

pub(in crate::protocol) fn cmd_georadiusbymember<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    parse_geo_radius(args, true)
}

const BLOOM_DEFAULT_ERROR_RATE: f64 = 0.01;
const BLOOM_DEFAULT_CAPACITY: usize = 100;

/// `EXPANSION n` (at least 1) or `NONSCALING` at argument `index`; `Ok(false)` when it is
/// neither. `NONSCALING` stores `0`, and the two exclude each other.
fn parse_bloom_growth(
    args: &Args<'_, '_>,
    index: usize,
    expansion: &mut Option<u32>,
) -> Result<bool, ProtocolError> {
    let name = args.bytes(index);

    if name.eq_ignore_ascii_case(b"NONSCALING") {
        if expansion.is_some() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        *expansion = Some(0);
        return Ok(true);
    }

    if name.eq_ignore_ascii_case(b"EXPANSION") {
        if index + 1 >= args.len() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        let value = u32::try_from(parse_u64(args.bytes(index + 1))?)
            .ok()
            .filter(|value| *value >= 1)
            .ok_or(ProtocolError::Invalid(
                "EXPANSION must be a positive integer",
            ))?;

        if expansion.is_some() {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        *expansion = Some(value);
        return Ok(true);
    }

    Ok(false)
}

pub(in crate::protocol) fn cmd_bf_reserve<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 3 {
        return Err(ProtocolError::Invalid(
            "BF.RESERVE expects key, error rate, and capacity",
        ));
    }

    let mut expansion = None;
    let mut index = 3;

    while index < args.len() {
        if !parse_bloom_growth(args, index, &mut expansion)? {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += if args.bytes(index).eq_ignore_ascii_case(b"EXPANSION") {
            2
        } else {
            1
        };
    }

    let capacity = parse_usize(args.bytes(2))?;
    let error_rate = parse_f64(args.bytes(1))?;

    Ok(Command::BloomReserve(
        args.arg(0),
        error_rate,
        capacity,
        expansion.unwrap_or(crate::store::BLOOM_DEFAULT_EXPANSION),
    ))
}

pub(in crate::protocol) fn cmd_bf_add<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        2 => {
            let key = args.arg(0);
            let value = args.arg(1);

            if name.eq_ignore_ascii_case(b"BF.ADD") {
                Ok(Command::BloomAdd(key, value))
            } else {
                Ok(Command::BloomContains(key, value))
            }
        }

        _ => Err(ProtocolError::Invalid(
            "Bloom command expects key and value",
        )),
    }
}

/// `BF.MADD` and `BF.MEXISTS`: a key and at least one item.
pub(in crate::protocol) fn cmd_bf_many<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "Bloom command expects a key and at least one item",
        ));
    }

    let key = args.arg(0);
    let items = args.rest(1);

    if name.eq_ignore_ascii_case(b"BF.MADD") {
        Ok(Command::BloomAddMany(key, items))
    } else {
        Ok(Command::BloomContainsMany(key, items))
    }
}

pub(in crate::protocol) fn cmd_bf_insert<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let mut error_rate = BLOOM_DEFAULT_ERROR_RATE;
    let mut capacity = BLOOM_DEFAULT_CAPACITY;
    let mut expansion = None;
    let mut no_create = false;
    let mut index = 1;
    let mut items_at = None;
    let mut ttl = None;

    while index < args.len() {
        let name = args.bytes(index);

        if name.eq_ignore_ascii_case(b"ITEMS") {
            items_at = Some(index + 1);
            break;
        }

        if ttl.is_none()
            && let Some(found) = create_ttl_option(args, index)?
        {
            ttl = Some(found);
            index += 2;

            continue;
        }

        if name.eq_ignore_ascii_case(b"NOCREATE") {
            no_create = true;
            index += 1;
        } else if name.eq_ignore_ascii_case(b"CAPACITY") || name.eq_ignore_ascii_case(b"ERROR") {
            if index + 1 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let value = args.bytes(index + 1);

            if name.eq_ignore_ascii_case(b"CAPACITY") {
                capacity = parse_usize(value)?;
            } else {
                error_rate = parse_f64(value)?;
            }

            index += 2;
        } else if parse_bloom_growth(args, index, &mut expansion)? {
            index += if name.eq_ignore_ascii_case(b"EXPANSION") {
                2
            } else {
                1
            };
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    let Some(items_at) = items_at.filter(|at| *at < args.len()) else {
        return Err(ProtocolError::Invalid(
            "BF.INSERT expects ITEMS and at least one item",
        ));
    };

    Ok(with_create_ttl(
        ttl,
        Command::BloomInsert {
            key: args.arg(0),
            error_rate,
            capacity,
            expansion: expansion.unwrap_or(crate::store::BLOOM_DEFAULT_EXPANSION),
            no_create,
            items: args.rest(items_at),
        },
    ))
}

pub(in crate::protocol) fn cmd_bf_info<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    if name.eq_ignore_ascii_case(b"BF.CARD") {
        if args.len() != 1 {
            return Err(ProtocolError::Invalid("BF.CARD expects a key"));
        }

        return Ok(Command::BloomCard(args.arg(0)));
    }

    let field = match args.len() {
        1 => None,
        2 => {
            let field = args.bytes(1);

            Some(
                [
                    (&b"CAPACITY"[..], BloomInfoField::Capacity),
                    (b"SIZE", BloomInfoField::Size),
                    (b"FILTERS", BloomInfoField::Filters),
                    (b"ITEMS", BloomInfoField::Items),
                    (b"EXPANSION", BloomInfoField::Expansion),
                ]
                .into_iter()
                .find(|(candidate, _)| field.eq_ignore_ascii_case(candidate))
                .map(|(_, field)| field)
                .ok_or(ProtocolError::Invalid("unknown BF.INFO field"))?,
            )
        }
        _ => {
            return Err(ProtocolError::Invalid(
                "BF.INFO expects a key and an optional field",
            ));
        }
    };

    Ok(Command::BloomInfo(args.arg(0), field))
}

pub(in crate::protocol) fn cmd_xrange<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_xrange(name.eq_ignore_ascii_case(b"XREVRANGE"), args)
}

pub(in crate::protocol) fn cmd_multi<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Multi),
        _ => Err(ProtocolError::Invalid("MULTI expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_exec<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Exec),
        _ => Err(ProtocolError::Invalid("EXEC expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_discard<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Discard),
        _ => Err(ProtocolError::Invalid("DISCARD expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_watch<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid("WATCH expects at least one key"));
    }

    Ok(Command::Watch(args.rest(0)))
}

pub(in crate::protocol) fn cmd_unwatch<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    match args.len() {
        0 => Ok(Command::Unwatch),
        _ => Err(ProtocolError::Invalid("UNWATCH expects no arguments")),
    }
}

pub(in crate::protocol) fn cmd_eval<'a>(name: &[u8], args: &Args<'a, '_>) -> Parsed<'a> {
    parse_eval(name.eq_ignore_ascii_case(b"EVALSHA"), args)
}
