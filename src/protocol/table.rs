//! Compile-time command table. One row per command: dispatch, arity, flags, key positions
//! and the parser. Every parser reads [`Args`]: spans straight into the input frame, so a
//! command whose `Command` keeps only single arguments allocates nothing to parse. Parsers
//! live in `table/parsers.rs`, `direct.rs`, and `parse/` (`parse/commands.rs` and the
//! per-type files); a body several rows share takes the command name or a flag, and each
//! row gets a small named wrapper.

use super::parse::commands;
use super::*;

mod parsers;
#[cfg(test)]
pub(crate) use parsers::MAX_RANDOM_COUNT;
pub(crate) use parsers::check_random_count;
use parsers::*;
// Named again because `super::*` also brings in parsers with these names.
use parsers::{parse_eval, parse_xrange};

/// Command flags, as in `COMMAND INFO`.
pub(crate) mod flags {
    pub(crate) const WRITE: u32 = 1 << 0;
    pub(crate) const READONLY: u32 = 1 << 1;
    pub(crate) const FAST: u32 = 1 << 2;
    pub(crate) const DENYOOM: u32 = 1 << 3;
    pub(crate) const BLOCKING: u32 = 1 << 4;
}

/// Arguments after the command name. Either spans into the input frame (no allocation) or
/// an already-built argument vector (scripts, inline commands, `read_command`).
pub(crate) struct Args<'a, 's> {
    source: ArgSource<'a, 's>,
}

enum ArgSource<'a, 's> {
    Frame {
        frame: &'a [u8],
        spans: &'s [(u32, u32)],
    },
    Slice(&'s [Bytes<'a>]),
}

impl<'a, 's> Args<'a, 's> {
    pub(crate) fn from_frame(frame: &'a [u8], spans: &'s [(u32, u32)]) -> Self {
        Self {
            source: ArgSource::Frame { frame, spans },
        }
    }

    pub(crate) fn from_slice(arguments: &'s [Bytes<'a>]) -> Self {
        Self {
            source: ArgSource::Slice(arguments),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.source {
            ArgSource::Frame { spans, .. } => spans.len(),
            ArgSource::Slice(arguments) => arguments.len(),
        }
    }

    /// Argument `index` for inspection or number parsing.
    pub(crate) fn bytes(&self, index: usize) -> &[u8] {
        match &self.source {
            ArgSource::Frame { frame, spans } => {
                let (start, end) = spans[index];

                &frame[start as usize..end as usize]
            }
            ArgSource::Slice(arguments) => &arguments[index],
        }
    }

    /// Argument `index` as a value a `Command` can keep. Frame arguments stay borrowed.
    pub(crate) fn arg(&self, index: usize) -> Bytes<'a> {
        match &self.source {
            ArgSource::Frame { frame, spans } => {
                let (start, end) = spans[index];

                Cow::Borrowed(&frame[start as usize..end as usize])
            }
            ArgSource::Slice(arguments) => arguments[index].clone(),
        }
    }

    /// Arguments `from..` as owned-or-borrowed values; one `Vec` for commands that keep a list.
    pub(crate) fn rest(&self, from: usize) -> Vec<Bytes<'a>> {
        (from..self.len()).map(|index| self.arg(index)).collect()
    }

    /// Arguments `from..` like [`Self::rest`], with a single argument kept inline.
    pub(crate) fn rest_one_or_many(&self, from: usize) -> OneOrMany<Bytes<'a>> {
        if self.len() == from + 1 {
            OneOrMany::One(self.arg(from))
        } else {
            OneOrMany::Many(self.rest(from))
        }
    }
}

pub(crate) type ParseFn = for<'a, 's> fn(&Args<'a, 's>) -> Result<Command<'a>, ProtocolError>;

pub(crate) struct CommandSpec {
    /// Lowercase ASCII, at most `MAX_NAME` bytes.
    pub(crate) name: &'static str,
    /// Redis arity: exact count including the name when positive, minimum when negative.
    pub(crate) arity: i8,
    pub(crate) flags: u32,
    pub(crate) first_key: i8,
    pub(crate) last_key: i8,
    pub(crate) step: i8,
    pub(crate) categories: &'static [&'static str],
    pub(crate) parse: ParseFn,
}

impl CommandSpec {
    /// Flag names as `COMMAND INFO` reports them.
    pub(crate) fn flag_names(&self) -> Vec<&'static str> {
        [
            (WRITE, "write"),
            (READONLY, "readonly"),
            (FAST, "fast"),
            (DENYOOM, "denyoom"),
            (BLOCKING, "blocking"),
        ]
        .into_iter()
        .filter(|(flag, _)| self.flags & flag != 0)
        .map(|(_, name)| name)
        .collect()
    }

    const fn migrated(
        name: &'static str,
        arity: i8,
        flags: u32,
        keys: (i8, i8, i8),
        categories: &'static [&'static str],
        parse: ParseFn,
    ) -> Self {
        Self {
            name,
            arity,
            flags,
            first_key: keys.0,
            last_key: keys.1,
            step: keys.2,
            categories,
            parse,
        }
    }
}

pub(crate) fn commands() -> &'static [CommandSpec] {
    COMMANDS
}

pub(crate) const MAX_NAME: usize = 32;

use flags::{BLOCKING, DENYOOM, FAST, READONLY, WRITE};

pub(crate) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::migrated(
        "get",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "string", "fast"],
        parse_get,
    ),
    CommandSpec::migrated(
        "set",
        -3,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_set,
    ),
    CommandSpec::migrated(
        "incr",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incr,
    ),
    CommandSpec::migrated(
        "decr",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_decr,
    ),
    CommandSpec::migrated(
        "incrby",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incrby,
    ),
    CommandSpec::migrated(
        "decrby",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_decrby,
    ),
    CommandSpec::migrated(
        "mget",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["read", "string", "fast"],
        parse_mget,
    ),
    CommandSpec::migrated(
        "getset",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_getset,
    ),
    CommandSpec::migrated(
        "append",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_append,
    ),
    CommandSpec::migrated(
        "setex",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_setex,
    ),
    CommandSpec::migrated(
        "psetex",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_psetex,
    ),
    CommandSpec::migrated(
        "setnx",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_setnx,
    ),
    CommandSpec::migrated(
        "getdel",
        2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_getdel,
    ),
    CommandSpec::migrated(
        "msetnx",
        -3,
        WRITE | DENYOOM,
        (1, -1, 2),
        &["write", "string", "slow"],
        parse_msetnx,
    ),
    CommandSpec::migrated(
        "incrbyfloat",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_incrbyfloat,
    ),
    CommandSpec::migrated(
        "getrange",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "string", "slow"],
        parse_getrange,
    ),
    CommandSpec::migrated(
        "substr",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "string", "slow"],
        parse_getrange,
    ),
    CommandSpec::migrated(
        "setrange",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_setrange,
    ),
    CommandSpec::migrated(
        "smismember",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        parse_smismember,
    ),
    CommandSpec::migrated(
        "sintercard",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "set", "slow"],
        parse_sintercard,
    ),
    CommandSpec::migrated(
        "zcount",
        4,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zcount,
    ),
    CommandSpec::migrated(
        "zlexcount",
        4,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zlexcount,
    ),
    CommandSpec::migrated(
        "zmscore",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        parse_zmscore,
    ),
    CommandSpec::migrated(
        "zunionstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zunionstore,
    ),
    CommandSpec::migrated(
        "zinterstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zinterstore,
    ),
    CommandSpec::migrated(
        "zdiffstore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zdiffstore,
    ),
    CommandSpec::migrated(
        "zunion",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zunion,
    ),
    CommandSpec::migrated(
        "zinter",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zinter,
    ),
    CommandSpec::migrated(
        "zdiff",
        -3,
        READONLY,
        (2, -1, 1),
        &["read", "sortedset", "slow"],
        parse_zdiff,
    ),
    CommandSpec::migrated(
        "zrandmember",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrandmember,
    ),
    CommandSpec::migrated(
        "hrandfield",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        parse_hrandfield,
    ),
    CommandSpec::migrated(
        "hincrbyfloat",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hincrbyfloat,
    ),
    CommandSpec::migrated(
        "linsert",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_linsert,
    ),
    CommandSpec::migrated(
        "lset",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_lset,
    ),
    CommandSpec::migrated(
        "lrem",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "list", "slow"],
        parse_lrem,
    ),
    CommandSpec::migrated(
        "lpos",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        parse_lpos,
    ),
    CommandSpec::migrated(
        "lmove",
        5,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "list", "slow"],
        parse_lmove,
    ),
    CommandSpec::migrated(
        "blmove",
        6,
        WRITE | DENYOOM | BLOCKING,
        (1, 2, 1),
        &["write", "list", "slow", "blocking"],
        parse_blmove,
    ),
    CommandSpec::migrated(
        "rpoplpush",
        3,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "list", "slow"],
        parse_rpoplpush,
    ),
    CommandSpec::migrated(
        "lpushx",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpushx,
    ),
    CommandSpec::migrated(
        "rpushx",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpushx,
    ),
    CommandSpec::migrated(
        "lmpop",
        -4,
        WRITE,
        (0, 0, 0),
        &["write", "list", "slow"],
        parse_lmpop,
    ),
    CommandSpec::migrated(
        "blmpop",
        -5,
        WRITE | BLOCKING,
        (0, 0, 0),
        &["write", "list", "slow", "blocking"],
        parse_blmpop,
    ),
    CommandSpec::migrated(
        "setbit",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "bitmap", "slow"],
        parse_setbit,
    ),
    CommandSpec::migrated(
        "bitfield",
        -2,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "bitmap", "slow"],
        parse_bitfield,
    ),
    CommandSpec::migrated(
        "bitfield_ro",
        -2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "bitmap", "fast"],
        parse_bitfield_ro,
    ),
    CommandSpec::migrated(
        "getbit",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "bitmap", "fast"],
        parse_getbit,
    ),
    CommandSpec::migrated(
        "bitcount",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "bitmap", "slow"],
        parse_bitcount,
    ),
    CommandSpec::migrated(
        "bitpos",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "bitmap", "slow"],
        parse_bitpos,
    ),
    CommandSpec::migrated(
        "bitop",
        -4,
        WRITE | DENYOOM,
        (2, -1, 1),
        &["write", "bitmap", "slow"],
        parse_bitop,
    ),
    CommandSpec::migrated(
        "pfadd",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hyperloglog", "fast"],
        parse_pfadd,
    ),
    CommandSpec::migrated(
        "pfcount",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "hyperloglog", "slow"],
        parse_pfcount,
    ),
    CommandSpec::migrated(
        "pfmerge",
        -2,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "hyperloglog", "slow"],
        parse_pfmerge,
    ),
    CommandSpec::migrated(
        "fillget",
        3,
        WRITE | FAST | BLOCKING,
        (1, 1, 1),
        &["write", "string", "fast"],
        parse_fillget,
    ),
    CommandSpec::migrated(
        "fillset",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "string", "slow"],
        parse_fillset,
    ),
    CommandSpec::migrated(
        "strlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "string", "fast"],
        parse_strlen,
    ),
    CommandSpec::migrated(
        "exists",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["read", "keyspace", "fast"],
        parse_exists,
    ),
    CommandSpec::migrated(
        "keys",
        2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow", "dangerous"],
        parse_keys,
    ),
    CommandSpec::migrated(
        "randomkey",
        1,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        parse_randomkey,
    ),
    CommandSpec::migrated(
        "touch",
        -2,
        READONLY | FAST,
        (1, -1, 1),
        &["keyspace", "read", "fast"],
        parse_touch,
    ),
    CommandSpec::migrated(
        "renamenx",
        3,
        WRITE | FAST,
        (1, 2, 1),
        &["keyspace", "write", "fast"],
        parse_renamenx,
    ),
    CommandSpec::migrated(
        "expiretime",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        parse_expiretime,
    ),
    CommandSpec::migrated(
        "pexpiretime",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        parse_pexpiretime,
    ),
    CommandSpec::migrated(
        "copy",
        -3,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["keyspace", "write", "slow"],
        parse_copy,
    ),
    CommandSpec::migrated(
        "echo",
        2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        parse_echo,
    ),
    CommandSpec::migrated(
        "object",
        -2,
        READONLY,
        (2, 2, 1),
        &["keyspace", "read", "slow"],
        parse_object,
    ),
    CommandSpec::migrated(
        "memory",
        -2,
        READONLY,
        (2, 2, 1),
        &["read", "slow"],
        parse_memory,
    ),
    CommandSpec::migrated(
        "slowlog",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        parse_slowlog,
    ),
    CommandSpec::migrated(
        "latency",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        parse_latency,
    ),
    CommandSpec::migrated(
        "dump",
        2,
        READONLY,
        (1, 1, 1),
        &["keyspace", "read", "slow"],
        parse_dump,
    ),
    CommandSpec::migrated(
        "restore",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["keyspace", "write", "slow", "dangerous"],
        parse_restore,
    ),
    CommandSpec::migrated(
        "ping",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        direct::cmd_ping,
    ),
    CommandSpec::migrated(
        "wait",
        3,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        commands::cmd_wait,
    ),
    CommandSpec::migrated(
        "ruvio",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        commands::cmd_ruvio,
    ),
    CommandSpec::migrated(
        "cluster",
        -2,
        0,
        (0, 0, 0),
        &["slow"],
        commands::cmd_cluster,
    ),
    CommandSpec::migrated(
        "readonly",
        1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        parse_readonly,
    ),
    CommandSpec::migrated(
        "readwrite",
        1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        parse_readwrite,
    ),
    CommandSpec::migrated(
        "config",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        commands::cmd_config,
    ),
    CommandSpec::migrated(
        "replicaof",
        -3,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        commands::cmd_replicaof,
    ),
    CommandSpec::migrated(
        "role",
        1,
        FAST,
        (0, 0, 0),
        &["admin", "fast", "dangerous"],
        commands::cmd_role,
    ),
    CommandSpec::migrated(
        "info",
        -1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        commands::cmd_info,
    ),
    CommandSpec::migrated(
        "save",
        1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        commands::cmd_save,
    ),
    CommandSpec::migrated(
        "bgsave",
        -1,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        commands::cmd_bgsave,
    ),
    CommandSpec::migrated(
        "json.get",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_get,
    ),
    CommandSpec::migrated(
        "json.mget",
        -3,
        READONLY,
        (1, -2, 1),
        &["read", "slow"],
        commands::cmd_json_mget,
    ),
    CommandSpec::migrated(
        "json.del",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_del,
    ),
    CommandSpec::migrated(
        "json.forget",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_forget,
    ),
    CommandSpec::migrated(
        "json.set",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        commands::cmd_json_set,
    ),
    CommandSpec::migrated(
        "json.merge",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        commands::cmd_json_merge,
    ),
    CommandSpec::migrated(
        "json.index",
        -2,
        WRITE,
        (0, 0, 0),
        &["write", "slow"],
        parse_json_index,
    ),
    CommandSpec::migrated(
        "json.find",
        -4,
        READONLY,
        (0, 0, 0),
        &["read", "slow"],
        parse_json_find,
    ),
    CommandSpec::migrated(
        "json.type",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_type,
    ),
    CommandSpec::migrated(
        "json.strlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_strlen,
    ),
    CommandSpec::migrated(
        "json.arrlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_arrlen,
    ),
    CommandSpec::migrated(
        "json.objkeys",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_objkeys,
    ),
    CommandSpec::migrated(
        "json.numincrby",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_numincrby,
    ),
    CommandSpec::migrated(
        "json.toggle",
        -2,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_toggle,
    ),
    CommandSpec::migrated(
        "json.arrappend",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_arrappend,
    ),
    CommandSpec::migrated(
        "json.clear",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_clear,
    ),
    CommandSpec::migrated(
        "json.objlen",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_objlen,
    ),
    CommandSpec::migrated(
        "json.strappend",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_strappend,
    ),
    CommandSpec::migrated(
        "json.nummultby",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_nummultby,
    ),
    CommandSpec::migrated(
        "json.arrinsert",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_arrinsert,
    ),
    CommandSpec::migrated(
        "json.arrpop",
        -2,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_arrpop,
    ),
    CommandSpec::migrated(
        "json.arrtrim",
        5,
        WRITE,
        (1, 1, 1),
        &["write", "slow"],
        parse_json_arrtrim,
    ),
    CommandSpec::migrated(
        "json.arrindex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_json_arrindex,
    ),
    // JMERGE is an alias of JSON.MERGE and shares its parser.
    CommandSpec::migrated(
        "jmerge",
        4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        commands::cmd_json_merge,
    ),
    CommandSpec::migrated(
        "lease",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_lease,
    ),
    CommandSpec::migrated(
        "semaphore",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_semaphore,
    ),
    CommandSpec::migrated(
        "limit",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_limit,
    ),
    CommandSpec::migrated(
        "idem",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["keyspace", "write"],
        commands::cmd_idem,
    ),
    CommandSpec::migrated(
        "once",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_once,
    ),
    CommandSpec::migrated(
        "take",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        commands::cmd_take,
    ),
    CommandSpec::migrated(
        "transfer",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 2, 1),
        &["string", "write", "fast"],
        commands::cmd_transfer,
    ),
    CommandSpec::migrated(
        "htransfer",
        -5,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["hash", "write", "fast"],
        commands::cmd_htransfer,
    ),
    CommandSpec::migrated(
        "getex",
        -2,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        commands::cmd_getex,
    ),
    CommandSpec::migrated(
        "release",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_release,
    ),
    CommandSpec::migrated(
        "renew",
        4,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_renew,
    ),
    CommandSpec::migrated(
        "setv",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        commands::cmd_setv,
    ),
    CommandSpec::migrated(
        "changes",
        -2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        commands::cmd_changes,
    ),
    CommandSpec::migrated(
        "mset",
        -3,
        WRITE | DENYOOM,
        (1, -1, 2),
        &["write", "string", "slow"],
        direct::cmd_mset,
    ),
    CommandSpec::migrated(
        "type",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_type,
    ),
    CommandSpec::migrated(
        "unlink",
        -2,
        WRITE,
        (1, -1, 1),
        &["keyspace", "write", "slow"],
        direct::cmd_unlink,
    ),
    CommandSpec::migrated(
        "rename",
        3,
        WRITE | FAST,
        (1, 2, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_rename,
    ),
    CommandSpec::migrated(
        "scan",
        -2,
        READONLY,
        (0, 0, 0),
        &["keyspace", "read", "slow"],
        parse_scan,
    ),
    CommandSpec::migrated(
        "dbsize",
        1,
        READONLY | FAST,
        (0, 0, 0),
        &["keyspace", "read", "fast"],
        direct::cmd_dbsize,
    ),
    CommandSpec::migrated(
        "select",
        2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        direct::cmd_select,
    ),
    CommandSpec::migrated(
        "flushdb",
        -1,
        WRITE,
        (0, 0, 0),
        &["keyspace", "write", "slow", "dangerous"],
        parse_flushdb,
    ),
    CommandSpec::migrated(
        "flushall",
        -1,
        WRITE,
        (0, 0, 0),
        &["keyspace", "write", "slow", "dangerous"],
        parse_flushall,
    ),
    CommandSpec::migrated(
        "swapdb",
        3,
        WRITE | FAST,
        (0, 0, 0),
        &["keyspace", "write", "fast", "dangerous"],
        commands::cmd_swapdb,
    ),
    CommandSpec::migrated(
        "move",
        3,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        commands::cmd_move,
    ),
    CommandSpec::migrated(
        "del",
        -2,
        WRITE,
        (1, -1, 1),
        &["keyspace", "write", "slow"],
        direct::cmd_del,
    ),
    CommandSpec::migrated(
        "publish",
        3,
        FAST,
        (0, 0, 0),
        &["pubsub", "fast"],
        direct::cmd_publish,
    ),
    CommandSpec::migrated(
        "subscribe",
        -2,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        commands::cmd_subscribe,
    ),
    CommandSpec::migrated(
        "unsubscribe",
        -1,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        commands::cmd_unsubscribe,
    ),
    CommandSpec::migrated(
        "psubscribe",
        -2,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        commands::cmd_psubscribe,
    ),
    CommandSpec::migrated(
        "punsubscribe",
        -1,
        0,
        (0, 0, 0),
        &["pubsub", "slow"],
        commands::cmd_punsubscribe,
    ),
    CommandSpec::migrated(
        "spublish",
        3,
        FAST,
        (1, 1, 1),
        &["pubsub", "fast"],
        commands::cmd_spublish,
    ),
    CommandSpec::migrated(
        "ssubscribe",
        -2,
        0,
        (1, -1, 1),
        &["pubsub", "slow"],
        commands::cmd_ssubscribe,
    ),
    CommandSpec::migrated(
        "sunsubscribe",
        -1,
        0,
        (1, -1, 1),
        &["pubsub", "slow"],
        commands::cmd_sunsubscribe,
    ),
    CommandSpec::migrated(
        "hello",
        -1,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        parse_hello,
    ),
    CommandSpec::migrated(
        "client",
        -2,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        parse_client,
    ),
    CommandSpec::migrated(
        "auth",
        -2,
        FAST,
        (0, 0, 0),
        &["connection", "fast"],
        commands::cmd_auth,
    ),
    CommandSpec::migrated(
        "acl",
        -2,
        0,
        (0, 0, 0),
        &["admin", "slow", "dangerous"],
        parse_acl_command,
    ),
    CommandSpec::migrated(
        "expire",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_expire,
    ),
    CommandSpec::migrated(
        "pexpire",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_pexpire,
    ),
    CommandSpec::migrated(
        "expireat",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_expireat,
    ),
    CommandSpec::migrated(
        "pexpireat",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_pexpireat,
    ),
    CommandSpec::migrated(
        "ttl",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_ttl,
    ),
    CommandSpec::migrated(
        "pttl",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["keyspace", "read", "fast"],
        direct::cmd_pttl,
    ),
    CommandSpec::migrated(
        "persist",
        2,
        WRITE | FAST,
        (1, 1, 1),
        &["keyspace", "write", "fast"],
        direct::cmd_persist,
    ),
    CommandSpec::migrated(
        "lpushcap",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpushcap,
    ),
    CommandSpec::migrated(
        "rpushcap",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpushcap,
    ),
    CommandSpec::migrated(
        "delex",
        4,
        WRITE | FAST,
        (1, 1, 1),
        &["string", "write", "fast"],
        parse_delex,
    ),
    CommandSpec::migrated(
        "lpush",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_lpush,
    ),
    CommandSpec::migrated(
        "lpop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        direct::cmd_lpop,
    ),
    CommandSpec::migrated(
        "lrange",
        4,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        direct::cmd_lrange,
    ),
    CommandSpec::migrated(
        "rpush",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        parse_rpush,
    ),
    CommandSpec::migrated(
        "rpop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "list", "fast"],
        direct::cmd_rpop,
    ),
    CommandSpec::migrated(
        "llen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "list", "fast"],
        direct::cmd_llen,
    ),
    CommandSpec::migrated(
        "lindex",
        3,
        READONLY,
        (1, 1, 1),
        &["read", "list", "slow"],
        direct::cmd_lindex,
    ),
    CommandSpec::migrated(
        "ltrim",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "list", "slow"],
        direct::cmd_ltrim,
    ),
    CommandSpec::migrated(
        "blpop",
        -3,
        WRITE | BLOCKING,
        (1, -2, 1),
        &["write", "list", "slow", "blocking"],
        parse_blpop,
    ),
    CommandSpec::migrated(
        "brpop",
        -3,
        WRITE | BLOCKING,
        (1, -2, 1),
        &["write", "list", "slow", "blocking"],
        parse_brpop,
    ),
    CommandSpec::migrated(
        "sadd",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_sadd,
    ),
    CommandSpec::migrated(
        "sismember",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        direct::cmd_sismember,
    ),
    CommandSpec::migrated(
        "srem",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_srem,
    ),
    CommandSpec::migrated(
        "scard",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "set", "fast"],
        direct::cmd_scard,
    ),
    CommandSpec::migrated(
        "smembers",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        direct::cmd_smembers,
    ),
    CommandSpec::migrated(
        "sinter",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        commands::cmd_sinter,
    ),
    CommandSpec::migrated(
        "sunion",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        commands::cmd_sunion,
    ),
    CommandSpec::migrated(
        "sdiff",
        -2,
        READONLY,
        (1, -1, 1),
        &["read", "set", "slow"],
        commands::cmd_sdiff,
    ),
    CommandSpec::migrated(
        "sinterstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        commands::cmd_sinterstore,
    ),
    CommandSpec::migrated(
        "sunionstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        commands::cmd_sunionstore,
    ),
    CommandSpec::migrated(
        "sdiffstore",
        -3,
        WRITE | DENYOOM,
        (1, -1, 1),
        &["write", "set", "slow"],
        commands::cmd_sdiffstore,
    ),
    CommandSpec::migrated(
        "spop",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "set", "fast"],
        direct::cmd_spop,
    ),
    CommandSpec::migrated(
        "srandmember",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        direct::cmd_srandmember,
    ),
    CommandSpec::migrated(
        "smove",
        4,
        WRITE | DENYOOM | FAST,
        (1, 2, 1),
        &["write", "set", "fast"],
        commands::cmd_smove,
    ),
    CommandSpec::migrated(
        "hincrby",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hincrby,
    ),
    CommandSpec::migrated(
        "hsetnx",
        4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hsetnx,
    ),
    CommandSpec::migrated(
        "hscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        parse_hash_scan,
    ),
    CommandSpec::migrated(
        "sscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "set", "slow"],
        commands::cmd_sscan,
    ),
    CommandSpec::migrated(
        "zscan",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        commands::cmd_zscan,
    ),
    CommandSpec::migrated(
        "hstrlen",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hstrlen,
    ),
    CommandSpec::migrated(
        "hset",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hset,
    ),
    CommandSpec::migrated(
        "hmset",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hmset,
    ),
    CommandSpec::migrated(
        "hdel",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        direct::cmd_hdel,
    ),
    CommandSpec::migrated(
        "hget",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hget,
    ),
    CommandSpec::migrated(
        "hmget",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hmget,
    ),
    CommandSpec::migrated(
        "hlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hlen,
    ),
    CommandSpec::migrated(
        "hgetall",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hgetall,
    ),
    CommandSpec::migrated(
        "hexists",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        direct::cmd_hexists,
    ),
    CommandSpec::migrated(
        "hkeys",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hkeys,
    ),
    CommandSpec::migrated(
        "hvals",
        2,
        READONLY,
        (1, 1, 1),
        &["read", "hash", "slow"],
        direct::cmd_hvals,
    ),
    CommandSpec::migrated(
        "hexpire",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hexpire,
    ),
    CommandSpec::migrated(
        "hpexpire",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hpexpire,
    ),
    CommandSpec::migrated(
        "hexpireat",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hexpireat,
    ),
    CommandSpec::migrated(
        "hpexpireat",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hpexpireat,
    ),
    CommandSpec::migrated(
        "httl",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        parse_httl,
    ),
    CommandSpec::migrated(
        "hpttl",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        parse_hpttl,
    ),
    CommandSpec::migrated(
        "hexpiretime",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        parse_hexpiretime,
    ),
    CommandSpec::migrated(
        "hpexpiretime",
        -5,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "hash", "fast"],
        parse_hpexpiretime,
    ),
    CommandSpec::migrated(
        "hpersist",
        -5,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "hash", "fast"],
        parse_hpersist,
    ),
    CommandSpec::migrated(
        "zadd",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        parse_zadd,
    ),
    CommandSpec::migrated(
        "zrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrange,
    ),
    CommandSpec::migrated(
        "zrevrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrevrange,
    ),
    CommandSpec::migrated(
        "zrangebyscore",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_sorted_set_range_by_score,
    ),
    CommandSpec::migrated(
        "zrangebylex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrangebylex,
    ),
    CommandSpec::migrated(
        "zrevrangebylex",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "sortedset", "slow"],
        parse_zrevrangebylex,
    ),
    CommandSpec::migrated(
        "zrem",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        direct::cmd_zrem,
    ),
    CommandSpec::migrated(
        "zincrby",
        -4,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        direct::cmd_zincrby,
    ),
    CommandSpec::migrated(
        "zcard",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zcard,
    ),
    CommandSpec::migrated(
        "zscore",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zscore,
    ),
    CommandSpec::migrated(
        "zrank",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zrank,
    ),
    CommandSpec::migrated(
        "zrevrank",
        -3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "sortedset", "fast"],
        direct::cmd_zrevrank,
    ),
    CommandSpec::migrated(
        "zpopmin",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        parse_zpopmin,
    ),
    CommandSpec::migrated(
        "zpopmax",
        -2,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        parse_zpopmax,
    ),
    CommandSpec::migrated(
        "zpopbyscore",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["write", "sortedset", "fast"],
        parse_zpopbyscore,
    ),
    CommandSpec::migrated(
        "zmovedue",
        -4,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "sortedset", "list", "slow"],
        parse_zmovedue,
    ),
    CommandSpec::migrated(
        "bzpopmin",
        -3,
        WRITE | FAST | BLOCKING,
        (1, -2, 1),
        &["write", "sortedset", "fast", "blocking"],
        parse_bzpopmin,
    ),
    CommandSpec::migrated(
        "bzpopmax",
        -3,
        WRITE | FAST | BLOCKING,
        (1, -2, 1),
        &["write", "sortedset", "fast", "blocking"],
        parse_bzpopmax,
    ),
    CommandSpec::migrated(
        "zremrangebyrank",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zremrangebyrank,
    ),
    CommandSpec::migrated(
        "zremrangebyscore",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zremrangebyscore,
    ),
    CommandSpec::migrated(
        "zremrangebylex",
        4,
        WRITE,
        (1, 1, 1),
        &["write", "sortedset", "slow"],
        parse_zremrangebylex,
    ),
    CommandSpec::migrated(
        "geoadd",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "geo", "slow"],
        parse_geo_add,
    ),
    CommandSpec::migrated(
        "geodist",
        -4,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        parse_geo_dist,
    ),
    CommandSpec::migrated(
        "geopos",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        parse_geo_pos,
    ),
    CommandSpec::migrated(
        "geohash",
        -2,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        parse_geo_hash,
    ),
    CommandSpec::migrated(
        "geosearch",
        -7,
        READONLY,
        (1, 1, 1),
        &["read", "geo", "slow"],
        commands::cmd_geosearch,
    ),
    CommandSpec::migrated(
        "geosearchstore",
        -8,
        WRITE | DENYOOM,
        (1, 2, 1),
        &["write", "geo", "slow"],
        commands::cmd_geosearchstore,
    ),
    CommandSpec::migrated(
        "georadius",
        -6,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["read", "write", "geo", "slow"],
        commands::cmd_georadius,
    ),
    CommandSpec::migrated(
        "georadiusbymember",
        -5,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["read", "write", "geo", "slow"],
        commands::cmd_georadiusbymember,
    ),
    CommandSpec::migrated(
        "bf.reserve",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        commands::cmd_bf_reserve,
    ),
    CommandSpec::migrated(
        "bf.add",
        3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["write", "fast"],
        parse_bf_add,
    ),
    CommandSpec::migrated(
        "bf.madd",
        -3,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        parse_bf_madd,
    ),
    CommandSpec::migrated(
        "bf.mexists",
        -3,
        READONLY,
        (1, 1, 1),
        &["read", "slow"],
        parse_bf_mexists,
    ),
    CommandSpec::migrated(
        "bf.insert",
        -4,
        WRITE | DENYOOM,
        (1, 1, 1),
        &["write", "slow"],
        commands::cmd_bf_insert,
    ),
    CommandSpec::migrated(
        "bf.info",
        -2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        parse_bf_info,
    ),
    CommandSpec::migrated(
        "bf.card",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        parse_bf_card,
    ),
    CommandSpec::migrated(
        "bf.exists",
        3,
        READONLY | FAST,
        (1, 1, 1),
        &["read", "fast"],
        parse_bf_exists,
    ),
    CommandSpec::migrated(
        "xadd",
        -5,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xadd,
    ),
    CommandSpec::migrated(
        "xlen",
        2,
        READONLY | FAST,
        (1, 1, 1),
        &["stream", "read", "fast"],
        direct::cmd_xlen,
    ),
    CommandSpec::migrated(
        "xrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        parse_xrange,
    ),
    CommandSpec::migrated(
        "xrevrange",
        -4,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        parse_xrevrange,
    ),
    CommandSpec::migrated(
        "xread",
        -4,
        READONLY | BLOCKING,
        (0, 0, 0),
        &["stream", "read", "slow"],
        parse_xread,
    ),
    CommandSpec::migrated(
        "xdel",
        -3,
        WRITE | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xdel,
    ),
    CommandSpec::migrated(
        "xtrim",
        -4,
        WRITE,
        (1, 1, 1),
        &["stream", "write", "slow"],
        parse_xtrim,
    ),
    CommandSpec::migrated(
        "xsetid",
        -3,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xsetid,
    ),
    CommandSpec::migrated(
        "xinfo",
        -2,
        READONLY,
        (2, 2, 1),
        &["stream", "read", "slow"],
        parse_xinfo,
    ),
    CommandSpec::migrated(
        "xgroup",
        -2,
        WRITE,
        (2, 2, 1),
        &["stream", "write", "slow"],
        parse_xgroup,
    ),
    CommandSpec::migrated(
        "xreadgroup",
        -7,
        WRITE | DENYOOM | BLOCKING,
        (0, 0, 0),
        &["stream", "write", "slow"],
        parse_xreadgroup,
    ),
    CommandSpec::migrated(
        "xack",
        -4,
        WRITE | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xack,
    ),
    CommandSpec::migrated(
        "xpending",
        -3,
        READONLY,
        (1, 1, 1),
        &["stream", "read", "slow"],
        parse_xpending,
    ),
    CommandSpec::migrated(
        "xclaim",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xclaim,
    ),
    CommandSpec::migrated(
        "xautoclaim",
        -6,
        WRITE | DENYOOM | FAST,
        (1, 1, 1),
        &["stream", "write", "fast"],
        parse_xautoclaim,
    ),
    CommandSpec::migrated(
        "multi",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        commands::cmd_multi,
    ),
    CommandSpec::migrated(
        "exec",
        1,
        0,
        (0, 0, 0),
        &["transaction", "slow"],
        commands::cmd_exec,
    ),
    CommandSpec::migrated(
        "discard",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        commands::cmd_discard,
    ),
    CommandSpec::migrated(
        "watch",
        -2,
        FAST,
        (1, -1, 1),
        &["transaction", "fast"],
        commands::cmd_watch,
    ),
    CommandSpec::migrated(
        "unwatch",
        1,
        FAST,
        (0, 0, 0),
        &["transaction", "fast"],
        commands::cmd_unwatch,
    ),
    CommandSpec::migrated(
        "eval",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        parse_eval,
    ),
    CommandSpec::migrated(
        "evalsha",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        parse_evalsha,
    ),
    CommandSpec::migrated(
        "fcall",
        -3,
        WRITE,
        (0, 0, 0),
        &["scripting", "slow", "write"],
        parse_fcall,
    ),
    CommandSpec::migrated(
        "script",
        -2,
        0,
        (0, 0, 0),
        &["scripting", "slow"],
        parse_script,
    ),
    CommandSpec::migrated(
        "function",
        -2,
        0,
        (0, 0, 0),
        &["scripting", "slow"],
        parse_function,
    ),
    CommandSpec::migrated(
        "command",
        -1,
        0,
        (0, 0, 0),
        &["connection", "slow"],
        parse_command_meta,
    ),
];

// A const-built open-addressed table instead of phf: no new dependency, so the deny.toml
// and SBOM surface stay the same. Duplicate names and long probe chains fail the build.
const SLOTS: usize = 1024;
const SLOT_BITS: u32 = SLOTS.trailing_zeros();

/// The first `WORD_BYTES` bytes of a name are hashed and compared as two words.
const WORD_BYTES: usize = 16;
const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH_BITS: u64 = ONES * 0x80;

/// One table slot: the lowercased name head as two little-endian words (zero padded),
/// the name length, and the `COMMANDS` index plus one (zero is empty). Holding the words
/// in the slot settles a lookup with one read and two word compares.
#[derive(Clone, Copy)]
struct Slot {
    words: [u64; 2],
    command: u16,
    length: u8,
}

const EMPTY: Slot = Slot {
    words: [0; 2],
    command: 0,
    length: 0,
};

/// Lowercases every ASCII letter of `word`; every byte must be below 0x80, so no add
/// carries into the next byte.
const fn lower_ascii(word: u64) -> u64 {
    let from_a = word.wrapping_add(ONES * (0x80 - b'A' as u64));
    let past_z = word.wrapping_add(ONES * (0x80 - b'Z' as u64 - 1));
    let upper = from_a & !past_z & HIGH_BITS;

    word | upper >> 2
}

/// Up to eight bytes as a little-endian word, zero padded, without reading past `bytes`.
#[inline(always)]
fn load_word(bytes: &[u8]) -> u64 {
    let length = bytes.len();

    if length == 8 {
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else if length >= 4 {
        // Two overlapping four-byte loads; the shared bytes are the same on both sides.
        let low = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64;
        let high = u32::from_le_bytes(bytes[length - 4..].try_into().unwrap()) as u64;

        low | high << ((length - 4) * 8)
    } else if length > 0 {
        bytes[0] as u64
            | (bytes[length / 2] as u64) << (length / 2 * 8)
            | (bytes[length - 1] as u64) << ((length - 1) * 8)
    } else {
        0
    }
}

const fn hash(words: [u64; 2], length: usize) -> usize {
    let mixed =
        (words[0] ^ words[1].rotate_left(29) ^ length as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);

    (mixed >> (u64::BITS - SLOT_BITS)) as usize
}

/// The lowercase table name's head words, built byte by byte at compile time.
const fn table_words(name: &[u8]) -> [u64; 2] {
    let mut words = [0; 2];
    let mut index = 0;

    while index < name.len() && index < WORD_BYTES {
        words[index / 8] |= (name[index] as u64) << (index % 8 * 8);
        index += 1;
    }

    words
}

const fn same_name(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;

    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }

        index += 1;
    }

    true
}

/// Open-addressed table of `COMMANDS` rows, built at compile time. Panics the build on a
/// duplicate name, an over-long or non-lowercase name, an overfull table, or a probe
/// sequence longer than `MAX_PROBES`.
const MAX_PROBES: usize = 8;

static TABLE: [Slot; SLOTS] = build_table();

const fn build_table() -> [Slot; SLOTS] {
    let mut table = [EMPTY; SLOTS];
    let mut command = 0;

    assert!(
        COMMANDS.len() * 2 <= SLOTS,
        "command table is over half full"
    );

    while command < COMMANDS.len() {
        let name = COMMANDS[command].name.as_bytes();

        assert!(name.len() <= MAX_NAME, "command name is too long");

        let mut check = 0;

        while check < name.len() {
            assert!(
                name[check].is_ascii() && !name[check].is_ascii_uppercase(),
                "command names are lowercase ASCII"
            );
            check += 1;
        }

        let words = table_words(name);
        let mut slot = hash(words, name.len());
        let mut probes = 0;

        while table[slot].command != 0 {
            let other = COMMANDS[(table[slot].command - 1) as usize].name.as_bytes();

            assert!(!same_name(name, other), "duplicate command name");
            slot = (slot + 1) & (SLOTS - 1);
            probes += 1;
            assert!(probes < MAX_PROBES, "command table probe chain is too long");
        }

        table[slot] = Slot {
            words,
            command: command as u16 + 1,
            length: name.len() as u8,
        };
        command += 1;
    }

    table
}

/// Position of `spec` in the command table.
pub(crate) fn command_index(spec: &CommandSpec) -> usize {
    (spec as *const CommandSpec as usize - COMMANDS.as_ptr() as usize)
        / std::mem::size_of::<CommandSpec>()
}

/// The row for `name`, matched ASCII case-insensitively, or `None` for an unknown or
/// not-yet-tabled command.
pub(crate) fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    let length = name.len();

    if length > MAX_NAME {
        return None;
    }

    let head = &name[..length.min(WORD_BYTES)];
    let split = head.len().min(8);
    let words = [load_word(&head[..split]), load_word(&head[split..])];

    // Command names are ASCII: a high byte is unknown, and checking it first keeps
    // `lower_ascii` exact.
    if (words[0] | words[1]) & HIGH_BITS != 0 {
        return None;
    }

    let words = [lower_ascii(words[0]), lower_ascii(words[1])];
    let mut slot = hash(words, length);

    for _ in 0..MAX_PROBES {
        let entry = &TABLE[slot];

        if entry.command == 0 {
            return None;
        }

        if entry.words == words && entry.length as usize == length {
            let spec = &COMMANDS[(entry.command - 1) as usize];

            // Only names past `WORD_BYTES` have a tail left to compare.
            if length <= WORD_BYTES
                || spec.name.as_bytes()[WORD_BYTES..]
                    .iter()
                    .zip(&name[WORD_BYTES..])
                    .all(|(&wanted, &byte)| byte.to_ascii_lowercase() == wanted)
            {
                return Some(spec);
            }
        }

        slot = (slot + 1) & (SLOTS - 1);
    }

    None
}

/// All arguments as keys; one key is kept without a `Vec`.
pub(super) fn keys_of<'a>(args: &Args<'a, '_>) -> Keys<'a> {
    if args.len() == 1 {
        Keys::One(args.arg(0))
    } else {
        Keys::Many(args.rest(0))
    }
}

/// Commands ACL rules treat as one: enabling or disabling the canonical name covers every
/// alias. Each alias is a row of its own in `COMMANDS`.
const ACL_ALIASES: &[(&str, &str)] = &[
    ("decr", "incr"),
    ("incrby", "incr"),
    ("decrby", "incr"),
    ("unlink", "del"),
    ("georadiusbymember", "georadius"),
    ("jmerge", "json.merge"),
    ("json.forget", "json.del"),
    ("pexpire", "expire"),
    ("expireat", "expire"),
    ("pexpireat", "expire"),
    // These parse to the canonical command, so a rule on the alias alone could not be
    // enforced on every path (MULTI, scripts, forwarding).
    ("setex", "set"),
    ("psetex", "set"),
    ("substr", "getrange"),
    ("rpoplpush", "lmove"),
];

/// Canonical ACL command for `name` (any case, alias or not), or `None` for an unknown command.
pub(crate) fn acl_canonical(name: &str) -> Option<&'static str> {
    let spec = lookup(name.as_bytes())?;

    Some(
        ACL_ALIASES
            .iter()
            .find(|(alias, _)| *alias == spec.name)
            .map_or(spec.name, |(_, canonical)| canonical),
    )
}

/// One row per ACL command: the table without alias rows.
pub(crate) fn acl_commands() -> impl Iterator<Item = &'static CommandSpec> {
    COMMANDS
        .iter()
        .filter(|spec| !ACL_ALIASES.iter().any(|(alias, _)| *alias == spec.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_is_found_by_its_name_in_any_case() {
        for spec in COMMANDS {
            assert!(spec.name.len() <= MAX_NAME);
            assert!(std::ptr::eq(lookup(spec.name.as_bytes()).unwrap(), spec));
            assert!(std::ptr::eq(
                lookup(spec.name.to_ascii_uppercase().as_bytes()).unwrap(),
                spec
            ));
        }

        assert!(lookup(b"definitely-not-a-command").is_none());
        assert!(lookup(&[b'x'; MAX_NAME + 1]).is_none());
    }

    /// `lookup` against a plain scan of `COMMANDS` on near-miss names: every case mix,
    /// truncations, extensions, bit 0x20 flipped on a non-letter (which a naive
    /// `| 0x20` lowercase would accept), high bytes, and every length up to past the
    /// limit.
    #[test]
    fn lookup_matches_a_scan_of_the_table() {
        fn scan(name: &[u8]) -> Option<&'static CommandSpec> {
            COMMANDS
                .iter()
                .find(|spec| spec.name.as_bytes().eq_ignore_ascii_case(name))
        }

        let mut names: Vec<Vec<u8>> = Vec::new();

        for spec in COMMANDS {
            let name = spec.name.as_bytes();

            for mask in 0..name.len() {
                let mixed: Vec<u8> = name
                    .iter()
                    .enumerate()
                    .map(|(index, &byte)| {
                        if (index + mask) % 3 == 0 {
                            byte.to_ascii_uppercase()
                        } else {
                            byte
                        }
                    })
                    .collect();
                names.push(mixed);

                let mut flipped = name.to_vec();
                flipped[mask] ^= 0x20;
                names.push(flipped);

                let mut high = name.to_vec();
                high[mask] |= 0x80;
                names.push(high);

                names.push(name[..mask].to_vec());
            }

            names.push([name, b"x"].concat());
            names.push([name, b"\0"].concat());
        }

        for length in 0..=MAX_NAME + 1 {
            names.push(vec![b'a'; length]);
            names.push(vec![b'Z'; length]);
            names.push(vec![0; length]);
            names.push(b"georadiusbymember_ro_x_y_z_padding"[..length.min(34)].to_vec());
        }

        for name in &names {
            assert_eq!(
                lookup(name).map(command_index),
                scan(name).map(command_index),
                "{:?}",
                String::from_utf8_lossy(name)
            );
        }
    }

    #[test]
    fn arity_matches_what_the_parsers_accept() {
        // A positive arity is exact: the name plus that many minus one arguments must
        // parse for commands whose arguments need no particular shape.
        let free_form = [
            "get", "strlen", "type", "ttl", "pttl", "persist", "llen", "scard", "smembers", "hlen",
            "hgetall", "hkeys", "hvals", "zcard", "xlen",
        ];

        for name in free_form {
            let spec = lookup(name.as_bytes()).unwrap();
            let arguments: Vec<Bytes<'_>> =
                (1..spec.arity).map(|_| Cow::Borrowed(&b"k"[..])).collect();
            let parse = spec.parse;

            assert!(parse(&Args::from_slice(&arguments)).is_ok(), "{name}");

            let mut wrong = arguments.clone();

            wrong.push(Cow::Borrowed(&b"extra"[..]));
            assert!(parse(&Args::from_slice(&wrong)).is_err(), "{name}");
        }
    }

    #[test]
    fn acl_aliases_resolve_to_a_canonical_row() {
        for (alias, canonical) in ACL_ALIASES {
            assert!(lookup(alias.as_bytes()).is_some(), "{alias}");
            assert!(lookup(canonical.as_bytes()).is_some(), "{canonical}");
            assert_eq!(acl_canonical(alias), Some(*canonical));
        }

        assert_eq!(acl_canonical("GET"), Some("get"));
        assert_eq!(acl_canonical("nope"), None);
    }
}
