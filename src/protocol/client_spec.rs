//! The command facts every client needs, written for them to `clients/testdata/`.
//!
//! `commands.json` gives each command's arity, flags and key rule: where its keys are
//! among the arguments. `key_vectors.json` holds commands the parser accepts with the keys
//! the server routes them by. The clients build their routing tables from the first file
//! and test them against the second. Regenerate both after changing a command:
//!
//! ```text
//! RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec
//! ```

use serde_json::{Value, json};

use super::consistency::{CHANNEL_ROWS, POOL, Rng, SAMPLES, WORDS, frame};
use super::table::COMMANDS;
use super::*;

/// Where a command's keys are among its words (word 0 is the command name).
#[derive(Clone, Copy, Debug)]
enum KeyRule {
    /// No keys.
    None,
    /// Words `first..=last` every `step`; a negative `last` counts from the end
    /// (`-1` is the last word).
    Range { first: i8, last: i8, step: i8 },
    /// The word at `count_at` is a number of keys that follow it; `before` names one more
    /// key ahead of the count (a destination), or 0.
    NumKeys { count_at: u8, before: u8 },
    /// After the word `keyword`, the first half of the remaining words.
    StreamsKeyword { keyword: &'static str },
    /// Word `first`, and the word after any of `keywords`.
    KeyAndStore {
        first: u8,
        keywords: &'static [&'static str],
    },
}

/// Rows whose keys a first/last/step range cannot place, or places only for some forms.
const RULES: &[(&str, KeyRule)] = &[
    (
        "eval",
        KeyRule::NumKeys {
            count_at: 2,
            before: 0,
        },
    ),
    (
        "evalsha",
        KeyRule::NumKeys {
            count_at: 2,
            before: 0,
        },
    ),
    (
        "fcall",
        KeyRule::NumKeys {
            count_at: 2,
            before: 0,
        },
    ),
    (
        "blmpop",
        KeyRule::NumKeys {
            count_at: 2,
            before: 0,
        },
    ),
    (
        "lmpop",
        KeyRule::NumKeys {
            count_at: 1,
            before: 0,
        },
    ),
    (
        "sintercard",
        KeyRule::NumKeys {
            count_at: 1,
            before: 0,
        },
    ),
    (
        "zunion",
        KeyRule::NumKeys {
            count_at: 1,
            before: 0,
        },
    ),
    (
        "zinter",
        KeyRule::NumKeys {
            count_at: 1,
            before: 0,
        },
    ),
    (
        "zdiff",
        KeyRule::NumKeys {
            count_at: 1,
            before: 0,
        },
    ),
    (
        "zunionstore",
        KeyRule::NumKeys {
            count_at: 2,
            before: 1,
        },
    ),
    (
        "zinterstore",
        KeyRule::NumKeys {
            count_at: 2,
            before: 1,
        },
    ),
    (
        "zdiffstore",
        KeyRule::NumKeys {
            count_at: 2,
            before: 1,
        },
    ),
    ("xread", KeyRule::StreamsKeyword { keyword: "STREAMS" }),
    ("xreadgroup", KeyRule::StreamsKeyword { keyword: "STREAMS" }),
    (
        "georadius",
        KeyRule::KeyAndStore {
            first: 1,
            keywords: &["STORE", "STOREDIST"],
        },
    ),
    (
        "georadiusbymember",
        KeyRule::KeyAndStore {
            first: 1,
            keywords: &["STORE", "STOREDIST"],
        },
    ),
];

/// Option-carrying forms of the rows above, which the random search rarely builds.
const OPTION_SAMPLES: &[&[&str]] = &[
    &["ZUNION", "2", "a", "b", "WITHSCORES"],
    &[
        "ZUNION",
        "2",
        "a",
        "b",
        "WEIGHTS",
        "1",
        "2",
        "AGGREGATE",
        "MAX",
    ],
    &["ZINTER", "2", "a", "b", "WITHSCORES"],
    &["ZDIFF", "2", "a", "b", "WITHSCORES"],
    &["ZUNIONSTORE", "d", "2", "a", "b", "WEIGHTS", "1", "2"],
    &["ZINTERSTORE", "d", "2", "a", "b", "AGGREGATE", "MIN"],
    &["ZDIFFSTORE", "d", "2", "a", "b"],
    &["SINTERCARD", "2", "a", "b", "LIMIT", "1"],
    &["LMPOP", "2", "a", "b", "RIGHT", "COUNT", "2"],
    &["BLMPOP", "0", "2", "a", "b", "LEFT", "COUNT", "2"],
    &["EVAL", "return 1", "2", "a", "b", "x", "y"],
    &[
        "EVALSHA",
        "0123456789012345678901234567890123456789",
        "1",
        "a",
        "x",
    ],
    &["FCALL", "f", "2", "a", "b", "x"],
    &["XREAD", "STREAMS", "a", "0"],
    &[
        "XREAD", "COUNT", "1", "BLOCK", "0", "streams", "a", "b", "0", "$",
    ],
    &[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "COUNT",
        "1",
        "STREAMS",
        "a",
        ">",
    ],
    &["GEORADIUS", "g", "1", "2", "5", "km", "STOREDIST", "d"],
    &[
        "GEORADIUSBYMEMBER",
        "g",
        "m",
        "5",
        "km",
        "COUNT",
        "3",
        "STORE",
        "d",
    ],
    &["MSET", "a", "1", "b", "2", "c", "3"],
    &["MSETNX", "a", "1", "b", "2"],
    &["BLPOP", "a", "b", "0"],
    &["BZPOPMIN", "a", "b", "1.5"],
    &["ZMOVEDUE", "a", "b", "10"],
    &["JSON.MGET", "a", "b", "$.x"],
    &["BITOP", "OR", "d", "a", "b", "c"],
    &["DEL", "a", "b", "c"],
    &["SPUBLISH", "ch", "hello"],
];

fn rule_of(spec: &CommandSpec) -> KeyRule {
    if let Some((_, rule)) = RULES.iter().find(|(name, _)| *name == spec.name) {
        return *rule;
    }
    if spec.first_key == 0 || CHANNEL_ROWS.contains(&spec.name) {
        return KeyRule::None;
    }
    KeyRule::Range {
        first: spec.first_key,
        last: spec.last_key,
        step: spec.step,
    }
}

fn apply<'w>(rule: KeyRule, words: &[&'w str]) -> Vec<&'w str> {
    let len = words.len();

    match rule {
        KeyRule::None => Vec::new(),
        KeyRule::Range { first, last, step } => {
            let last = if last < 0 {
                len as i64 + i64::from(last)
            } else {
                i64::from(last)
            };
            let mut at = i64::from(first);
            let mut keys = Vec::new();

            while at <= last && at < len as i64 {
                keys.push(words[at as usize]);
                at += i64::from(step);
            }
            keys
        }
        KeyRule::NumKeys { count_at, before } => {
            let at = usize::from(count_at);
            let count = words
                .get(at)
                .and_then(|word| word.parse::<usize>().ok())
                .unwrap_or(0);
            let mut keys = Vec::new();

            if before > 0
                && let Some(word) = words.get(usize::from(before))
            {
                keys.push(*word);
            }
            keys.extend(words.iter().skip(at + 1).take(count));
            keys
        }
        KeyRule::StreamsKeyword { keyword } => {
            let Some(at) = words
                .iter()
                .skip(1)
                .position(|word| word.eq_ignore_ascii_case(keyword))
            else {
                return Vec::new();
            };
            let rest = &words[at + 2..];
            rest[..rest.len() / 2].to_vec()
        }
        KeyRule::KeyAndStore { first, keywords } => {
            let mut keys: Vec<&str> = words.get(usize::from(first)).copied().into_iter().collect();
            let mut at = usize::from(first) + 1;

            while at + 1 < len {
                if keywords
                    .iter()
                    .any(|keyword| words[at].eq_ignore_ascii_case(keyword))
                {
                    keys.push(words[at + 1]);
                    at += 1;
                }
                at += 1;
            }
            keys
        }
    }
}

fn rule_json(rule: KeyRule) -> Value {
    match rule {
        KeyRule::None => json!({ "kind": "none" }),
        KeyRule::Range { first, last, step } => {
            json!({ "kind": "range", "first": first, "last": last, "step": step })
        }
        KeyRule::NumKeys { count_at, before } => {
            json!({ "kind": "numkeys", "count_at": count_at, "before": before })
        }
        KeyRule::StreamsKeyword { keyword } => {
            json!({ "kind": "keyword_half", "keyword": keyword })
        }
        KeyRule::KeyAndStore { first, keywords } => {
            json!({ "kind": "key_and_store", "first": first, "keywords": keywords })
        }
    }
}

/// Words that may be renamed without changing what the command means.
fn is_plain_name(word: &str) -> bool {
    matches!(word, "k" | "v" | "a" | "b" | "f")
}

/// Parsed forms of one row, each with distinct names in place of `k`, `v`, `a`, `b` and
/// `f` so a client that reads the wrong argument cannot match by accident.
fn forms(name: &str, rng: &mut Rng, limit: usize) -> Vec<(Vec<String>, Vec<String>)> {
    let samples = SAMPLES
        .iter()
        .chain(OPTION_SAMPLES)
        .filter(|words| words[0].eq_ignore_ascii_case(name))
        .map(|words| words.to_vec());
    let random = (0..4_000usize).map(|round| {
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
        words
    });
    let mut out: Vec<(Vec<String>, Vec<String>)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut tried = std::collections::HashSet::new();

    let random: Vec<Vec<&str>> = random.collect();
    let sampled: Vec<Vec<&str>> = samples.collect();
    let sample_count = sampled.len();

    for (index, words) in sampled.into_iter().chain(random).enumerate() {
        let is_sample = index < sample_count;

        // The rest could only add forms past the limit; the generator has already drawn them,
        // so the numbers the next row gets stay the same.
        if !is_sample && out.len() >= limit {
            break;
        }
        // A repeated draw parses as it did before: its shape is seen or it does not parse.
        if !is_sample && !tried.insert(words.clone()) {
            continue;
        }

        let renamed: Vec<std::borrow::Cow<str>> = words
            .iter()
            .enumerate()
            .map(|(at, word)| {
                if at > 0 && is_plain_name(word) {
                    format!("{word}{at}").into()
                } else {
                    (*word).into()
                }
            })
            .collect();
        let renamed: Vec<&str> = renamed.iter().map(|word| word.as_ref()).collect();
        // Without plain names to rename, the second candidate is the first again.
        let candidates = if renamed == words {
            vec![renamed]
        } else {
            vec![renamed, words.clone()]
        };

        for refs in candidates {
            let bytes = frame(&refs);
            let Ok(Some((command, _))) = parse_command(&bytes) else {
                continue;
            };
            let mut keys: Vec<String> = crate::routing::command_keys(&command)
                .into_iter()
                .map(|key| String::from_utf8_lossy(key).into_owned())
                .collect();
            if CHANNEL_ROWS.contains(&name) {
                keys.clear();
            }
            keys.sort();
            let shape = format!(
                "{}|{}|{}",
                format!("{command:?}").split('(').next().unwrap_or_default(),
                refs.len(),
                keys.len()
            );

            if is_sample || (seen.insert(shape) && out.len() < limit) {
                out.push((refs.iter().map(|word| (*word).to_owned()).collect(), keys));
            }
            break;
        }
    }
    out
}

struct Generated {
    commands: String,
    vectors: String,
    wrong: Vec<String>,
}

fn generate() -> Generated {
    let mut rng = Rng(0x5851_f42d_4c95_7f2d);
    let mut rows = Vec::new();
    let mut vectors = Vec::new();
    let mut wrong = Vec::new();
    let mut specs: Vec<&CommandSpec> = COMMANDS.iter().collect();
    specs.sort_by_key(|spec| spec.name);

    for spec in specs {
        let rule = rule_of(spec);
        rows.push(json!({
            "name": spec.name.to_ascii_uppercase(),
            "arity": spec.arity,
            "flags": spec.flag_names(),
            "keys": rule_json(rule),
        }));

        for (words, keys) in forms(spec.name, &mut rng, 6) {
            let refs: Vec<&str> = words.iter().map(String::as_str).collect();
            let mut placed: Vec<String> =
                apply(rule, &refs).into_iter().map(str::to_owned).collect();
            placed.sort();

            if placed != keys {
                wrong.push(format!("{refs:?}: rule {placed:?}, server {keys:?}"));
            }
            vectors.push(json!({ "args": words, "keys": keys }));
        }
    }

    let commands = json!({
        "description": "One entry per command: arity and flags as COMMAND INFO reports them, and where the keys are among the arguments (argument 0 is the command name). Generated from the server's command table by `RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec`; do not edit by hand.",
        "key_rules": {
            "none": "no keys",
            "range": "arguments first..=last every step; a negative last counts from the end, -1 being the last argument",
            "numkeys": "the argument at count_at is a key count and that many keys follow it; before, when not 0, is the position of one more key ahead of the count",
            "keyword_half": "after the argument equal to keyword (any case), the first half of the remaining arguments",
            "key_and_store": "the argument at first, and the argument after any of keywords (any case)"
        },
        "commands": rows,
    });
    let vectors = json!({
        "description": "Commands the server parser accepts, with the keys the server routes them by, sorted. Every client checks its key extraction against this file. Generated with commands.json; do not edit by hand.",
        "vectors": vectors,
    });

    Generated {
        commands: serde_json::to_string_pretty(&commands).unwrap() + "\n",
        vectors: serde_json::to_string_pretty(&vectors).unwrap() + "\n",
        wrong,
    }
}

#[test]
fn client_command_spec_matches_the_server() {
    let generated = generate();

    assert!(
        generated.wrong.is_empty(),
        "key rules disagree with routing::command_keys:\n{}",
        generated.wrong.join("\n")
    );

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("clients/testdata");
    let files = [
        ("commands.json", &generated.commands),
        ("key_vectors.json", &generated.vectors),
    ];

    if std::env::var_os("RUVIO_WRITE_CLIENT_SPEC").is_some() {
        for (name, contents) in files {
            std::fs::write(root.join(name), contents).unwrap();
        }
        return;
    }
    for (name, contents) in files {
        // A checkout that turned LF into CRLF (Windows without `.gitattributes`) is not out
        // of date.
        let current = std::fs::read_to_string(root.join(name))
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert!(
            current == *contents,
            "clients/testdata/{name} is out of date; run RUVIO_WRITE_CLIENT_SPEC=1 cargo test --lib client_spec"
        );
    }
}
