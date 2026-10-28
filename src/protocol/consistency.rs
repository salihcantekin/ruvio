//! The command table is the one place a command's class is declared. Hot paths keep their
//! own `match` lists (`command_mutates_store`, the ACL name) because a table lookup per
//! command costs a name hash; this test keeps those lists in step with the table, so a
//! command added later cannot be classified twice in two different ways.

use super::table::{COMMANDS, flags};
use super::*;

pub(super) const POOL: &[&str] = &[
    "k", "v", "0", "1", "2", "-1", "10", "1.5", "*", "-", "+", "$", ">", "a", "b", "f",
];

/// Subcommands and option words the random search would not find on its own.
pub(super) const WORDS: &[&str] = &[
    "GET",
    "SET",
    "INCRBY",
    "OVERFLOW",
    "WRAP",
    "u8",
    "i8",
    "ENCODING",
    "IDLETIME",
    "FREQ",
    "REFCOUNT",
    "USAGE",
    "CREATE",
    "DESTROY",
    "SETID",
    "CREATECONSUMER",
    "DELCONSUMER",
    "LOAD",
    "EXISTS",
    "FLUSH",
    "KILL",
    "LIST",
    "DELETE",
    "GETNAME",
    "SETNAME",
    "ID",
    "INFO",
    "TRACKING",
    "ON",
    "OFF",
    "RESET",
    "LEN",
    "LATEST",
    "SLOTS",
    "NODES",
    "KEYSLOT",
    "MYID",
    "SHARDS",
    "WHOAMI",
    "USERS",
    "GETUSER",
    "CAT",
    "SETUSER",
    "SAVE",
    "DROP",
    "FIND",
    "STREAM",
    "GROUPS",
    "CONSUMERS",
    "STREAMS",
    "GROUP",
    "COUNT",
    "BLOCK",
    "LEFT",
    "RIGHT",
    "MIN",
    "MAX",
    "FROMLONLAT",
    "BYRADIUS",
    "km",
    "STORE",
    "MAXLEN",
    "MINID",
    "PATTERN",
    "DOCS",
    "GETKEYS",
    "SUBSCRIBE",
    "STATUS",
    "ACQUIRE",
    "RELEASE",
    "BEGIN",
    "COMPLETE",
    "ABORT",
    "$.a",
    "$",
];

/// Whole commands for syntax the random search does not reach.
pub(super) const SAMPLES: &[&[&str]] = &[
    &["LINSERT", "l", "BEFORE", "a", "b"],
    &["LMOVE", "a", "b", "LEFT", "RIGHT"],
    &["BLMOVE", "a", "b", "LEFT", "RIGHT", "0"],
    &["LMPOP", "1", "a", "LEFT"],
    &["BLMPOP", "0", "1", "a", "LEFT"],
    &["BITOP", "AND", "d", "a", "b"],
    &["DELEX", "k", "IFEQ", "v"],
    &["RENEW", "k", "1", "1000"],
    &["XREAD", "STREAMS", "k", "0"],
    &["SET", "k", "5", "IFGT"],
    &["SET", "k", "5", "IFLT", "EX", "10"],
    &["HEXPIRE", "h", "10", "FIELDS", "1", "f"],
    &["HPEXPIRE", "h", "10", "FIELDS", "1", "f"],
    &["HEXPIREAT", "h", "4102444800", "FIELDS", "1", "f"],
    &["HPEXPIREAT", "h", "4102444800000", "FIELDS", "1", "f"],
    &["HTTL", "h", "FIELDS", "1", "f"],
    &["HPTTL", "h", "FIELDS", "1", "f"],
    &["HEXPIRETIME", "h", "FIELDS", "1", "f"],
    &["HPEXPIRETIME", "h", "FIELDS", "1", "f"],
    &["HPERSIST", "h", "FIELDS", "1", "f"],
    &["GEOSEARCH", "g", "FROMMEMBER", "m", "BYBOX", "2", "2", "km"],
    &[
        "GEOSEARCHSTORE",
        "d",
        "g",
        "FROMMEMBER",
        "m",
        "BYRADIUS",
        "2",
        "km",
    ],
    &["GEORADIUS", "g", "1", "2", "5", "km"],
    &["GEORADIUS", "g", "1", "2", "5", "km", "STORE", "d"],
    &["GEORADIUSBYMEMBER", "g", "m", "5", "km"],
    &["GEORADIUSBYMEMBER", "g", "m", "5", "km", "STORE", "d"],
    &["BF.INSERT", "b", "ITEMS", "a"],
    &["XDEL", "x", "1-1"],
    &["XTRIM", "x", "MAXLEN", "10"],
    &["XTRIM", "x", "MINID", "5-0"],
    &["XSETID", "x", "5-0"],
    &["XREAD", "STREAMS", "x", "0"],
    &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "x", ">"],
    &["XACK", "x", "g", "1-1"],
    &["XCLAIM", "x", "g", "c", "0", "1-1"],
    &["XAUTOCLAIM", "x", "g", "c", "0", "0-0"],
];

pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as usize
    }
}

pub(super) fn frame(words: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", words.len()).into_bytes();

    for word in words {
        out.extend(format!("${}\r\n{word}\r\n", word.len()).bytes());
    }
    out
}

/// Up to `limit` distinct parsed forms of `name`, keyed by their debug variant name.
fn parsed_forms(name: &str, rng: &mut Rng, limit: usize) -> Vec<Command<'static>> {
    let mut forms: Vec<Command<'static>> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let samples = SAMPLES
        .iter()
        .filter(|words| words[0].eq_ignore_ascii_case(name))
        .map(|words| words.to_vec());

    for (round, sample) in (0..4_000)
        .map(|round| (round, None))
        .chain(samples.map(|words| (0, Some(words))))
    {
        let mut words = vec![name];

        for index in 0..round % 9 {
            let pool: &[&str] =
                if index == 0 && rng.next().is_multiple_of(2) || rng.next().is_multiple_of(4) {
                    WORDS
                } else {
                    POOL
                };
            words.push(pool[rng.next() % pool.len()]);
        }
        if let Some(sample) = sample {
            words = sample;
        }

        let bytes = frame(&words);
        if let Ok(Some((command, _))) = parse_command(&bytes) {
            let shape = format!("{:?}", command);
            let variant = shape
                .split(|c: char| !c.is_alphanumeric())
                .next()
                .unwrap_or_default()
                .to_owned();
            let class = (
                variant,
                command_mutates_store(&command),
                crate::acl::command_acl_name(&command),
            );

            if seen.insert(format!("{class:?}")) {
                forms.push(command.into_owned());
                if forms.len() >= limit {
                    return forms;
                }
            }
        }
    }

    forms
}

/// One parsed form per `Command` variant that routes by no key, keyed by the variant name.
/// Keyless commands (connection, admin, global) are the ones a script or a cross-shard
/// fan-out must decide on one by one, so the lists that decide are checked against this.
pub(crate) fn keyless_forms() -> Vec<(String, Command<'static>)> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut forms: Vec<(String, Command<'static>)> = Vec::new();

    for spec in COMMANDS {
        for command in parsed_forms(spec.name, &mut rng, 32) {
            if !crate::routing::command_keys(&command).is_empty() {
                continue;
            }
            let variant = variant_name(&command);

            if !forms.iter().any(|(seen, _)| *seen == variant) {
                forms.push((variant, command));
            }
        }
    }

    forms.sort_by(|left, right| left.0.cmp(&right.0));
    forms
}

fn variant_name(command: &Command<'_>) -> String {
    format!("{command:?}")
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

#[test]
fn hand_kept_command_lists_agree_with_the_table() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut write_flag = Vec::new();
    let mut blocking = Vec::new();
    let mut acl_name = Vec::new();
    let mut never_parsed = Vec::new();

    for spec in COMMANDS {
        let forms = parsed_forms(spec.name, &mut rng, 16);

        if forms.is_empty() {
            never_parsed.push(spec.name);
            continue;
        }

        let canonical = acl_canonical(spec.name).unwrap();
        // One row covers subcommands or options that read and others that write
        // (`BITFIELD GET`, `JSON.INDEX LIST`, `GEORADIUS` without `STORE`).
        let mixed = matches!(
            spec.name,
            "bitfield" | "json.index" | "georadius" | "georadiusbymember"
        );
        let writes = spec.flags & flags::WRITE != 0;

        for command in &forms {
            let name = crate::acl::command_acl_name(command);
            let parent = name.split('|').next().unwrap();

            if parent != canonical {
                acl_name.push(format!("{} -> {name}", spec.name));
            }
            if command_may_block(command) && spec.flags & flags::BLOCKING == 0 {
                blocking.push(spec.name);
            }
            if !mixed && command_mutates_store(command) != writes {
                write_flag.push(format!(
                    "{} (table write={writes}) {:?}",
                    spec.name,
                    format!("{command:?}").chars().take(60).collect::<String>()
                ));
            }
        }
    }

    acl_name.sort();
    acl_name.dedup();
    eprintln!("never parsed: {never_parsed:?}");
    eprintln!("acl name mismatches:\n{}", acl_name.join("\n"));
    eprintln!("write flag mismatches:\n{}", write_flag.join("\n"));
    eprintln!("blocking without the table flag: {blocking:?}");
    // A row no generated form parses is a row this test cannot check: give the search the
    // words it needs (`WORDS`) instead of letting the command drop out silently.
    assert!(
        never_parsed.is_empty()
            && acl_name.is_empty()
            && write_flag.is_empty()
            && blocking.is_empty()
    );
}

/// Rows with keys the table cannot place by position: after a `numkeys` argument or a
/// keyword (`STORE`, `STREAMS`), so the table holds only the positional ones, if any. Shard
/// channels sit at key positions but are not keys.
const KEYS_BEYOND_POSITIONS: &[&str] = &[
    "blmpop",
    "eval",
    "evalsha",
    "fcall",
    "georadius",
    "georadiusbymember",
    "lmpop",
    "xread",
    "xreadgroup",
    "zdiffstore",
    "zinterstore",
    "zunionstore",
];
pub(super) const CHANNEL_ROWS: &[&str] = &["spublish", "ssubscribe", "sunsubscribe"];

/// The keys the row's first/last/step place in `words`.
fn table_keys<'w>(first: i8, last: i8, step: i8, words: &[&'w str]) -> Vec<&'w [u8]> {
    let len = words.len() as i64;
    let last = if last < 0 {
        len + i64::from(last)
    } else {
        i64::from(last)
    };
    let mut keys = Vec::new();
    let mut at = i64::from(first);

    while first > 0 && at <= last && at < len {
        keys.push(words[at as usize].as_bytes());
        at += i64::from(step.max(1));
    }
    keys
}

#[test]
fn table_arity_and_key_positions_match_what_the_parser_accepts() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut wrong = std::collections::BTreeMap::new();
    let mut arity_wrong = std::collections::BTreeMap::new();

    for spec in COMMANDS {
        let samples = SAMPLES
            .iter()
            .filter(|words| words[0].eq_ignore_ascii_case(spec.name))
            .map(|words| words.to_vec());
        let random = (0..3_000usize).map(|round| {
            let mut words = vec![spec.name];

            for index in 0..round % 9 {
                let pool: &[&str] =
                    if index == 0 && rng.next().is_multiple_of(2) || rng.next().is_multiple_of(4) {
                        WORDS
                    } else {
                        POOL
                    };
                words.push(pool[rng.next() % pool.len()]);
            }
            words
        });

        for words in random.collect::<Vec<_>>().into_iter().chain(samples) {
            let bytes = frame(&words);
            let Ok(Some((command, _))) = parse_command(&bytes) else {
                continue;
            };
            let mut routed = crate::routing::command_keys(&command);
            let mut placed = table_keys(spec.first_key, spec.last_key, spec.step, &words);
            let agrees = if CHANNEL_ROWS.contains(&spec.name) {
                routed.is_empty()
            } else if KEYS_BEYOND_POSITIONS.contains(&spec.name) {
                placed.iter().all(|key| routed.contains(key))
            } else {
                routed.sort_unstable();
                placed.sort_unstable();
                routed == placed
            };

            let count = words.len() as i64;
            let arity = i64::from(spec.arity);

            if (arity > 0 && count != arity) || (arity < 0 && count < -arity) {
                arity_wrong
                    .entry(spec.name)
                    .or_insert_with(|| format!("{words:?}"));
            }
            if !agrees {
                wrong
                    .entry(spec.name)
                    .or_insert_with(|| format!("{words:?}"));
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "table key positions disagree with command_keys: {wrong:?}"
    );
    assert!(
        arity_wrong.is_empty(),
        "parsed with an argument count the table arity refuses: {arity_wrong:?}"
    );
}
