//! Parser robustness over the whole command surface listed in `docs/compatibility-matrix.json`:
//! every listed command is recognised by the parser, and no mix of arguments (wrong counts, junk
//! numbers, empty strings, huge values) can panic it or make it consume a wrong byte count.

use ruvio::protocol::parse_command;

fn frame(words: &[&str]) -> Vec<u8> {
    let mut frame = format!("*{}\r\n", words.len()).into_bytes();

    for word in words {
        frame.extend(format!("${}\r\n{}\r\n", word.len(), word).into_bytes());
    }

    frame
}

fn matrix_commands() -> Vec<String> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/compatibility-matrix.json"
    ))
    .unwrap();
    let matrix: serde_json::Value = serde_json::from_str(&text).unwrap();

    matrix["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|command| command["name"].as_str().unwrap().to_owned())
        .collect()
}

/// Tokens that commonly appear as options or sub-words, plus the awkward values: empty, negative,
/// overflowing, non-numeric, glob and range markers.
const POOL: &[&str] = &[
    "k",
    "key",
    "v",
    "value",
    "0",
    "1",
    "2",
    "-1",
    "10",
    "100",
    "1.5",
    "-inf",
    "+inf",
    "nan",
    "",
    "*",
    "-",
    "+",
    "$",
    ">",
    "(",
    "[",
    "99999999999999999999",
    "18446744073709551616",
    "-9223372036854775809",
    "EX",
    "PX",
    "NX",
    "XX",
    "GT",
    "LT",
    "KEEPTTL",
    "GET",
    "COUNT",
    "MATCH",
    "TYPE",
    "LIMIT",
    "WITHSCORES",
    "BYSCORE",
    "BYLEX",
    "REV",
    "STREAMS",
    "GROUP",
    "BLOCK",
    "NOMKSTREAM",
    "MAXLEN",
    "MINID",
    "LEFT",
    "RIGHT",
    "BEFORE",
    "AFTER",
    "AND",
    "OR",
    "XOR",
    "NOT",
    "CREATE",
    "FROM",
    "NOW",
    "WAIT",
    "IFEQ",
    "FIELDS",
    "ID",
    "$",
    "{}",
    "[1,2]",
    "{\"a\":1}",
    "$.a",
    "0:0",
    "1-1",
    "REPLACE",
    "ABSTTL",
    "ASC",
    "DESC",
    "STORE",
    "WITHVALUES",
    "CAP",
    "SWR",
    "MAX",
    "ON",
    "OFF",
    "TRUE",
    "FALSE",
    "LOAD",
    "SAVE",
];

/// Commands whose syntax needs structure the random mix cannot produce.
const STRUCTURED: &[&[&str]] = &[
    &["IDEM", "k", "BEGIN", "fingerprint", "owner", "1000"],
    &["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "5000"],
    &["CHANGES", "SUBSCRIBE", "FROM", "1:0:42", "COMMITTED"],
    &[
        "CHANGES",
        "SUBSCRIBE",
        "FROM",
        "0:42",
        "MATCH",
        "user:*",
        "VALUES",
    ],
    &["HEXPIRE", "h", "10", "FIELDS", "1", "f"],
    &["HEXPIRE", "h", "10", "NX", "FIELDS", "2", "f", "g"],
    &["HPEXPIRE", "h", "10", "FIELDS", "1", "f"],
    &["HEXPIREAT", "h", "4102444800", "FIELDS", "1", "f"],
    &["HPEXPIREAT", "h", "4102444800000", "FIELDS", "1", "f"],
    &["HTTL", "h", "FIELDS", "1", "f"],
    &["HPTTL", "h", "FIELDS", "1", "f"],
    &["HEXPIRETIME", "h", "FIELDS", "1", "f"],
    &["HPEXPIRETIME", "h", "FIELDS", "1", "f"],
    &["HPERSIST", "h", "FIELDS", "1", "f"],
    &["ZREVRANGEBYLEX", "z", "[b", "[a", "LIMIT", "0", "5"],
    &["ZLEXCOUNT", "z", "-", "+"],
    &[
        "GEOSEARCH",
        "g",
        "FROMLONLAT",
        "1",
        "2",
        "BYRADIUS",
        "5",
        "km",
        "ASC",
        "COUNT",
        "3",
        "WITHDIST",
    ],
    &["GEOSEARCH", "g", "FROMMEMBER", "m", "BYBOX", "2", "2", "km"],
    &[
        "GEOSEARCHSTORE",
        "d",
        "g",
        "FROMMEMBER",
        "m",
        "BYRADIUS",
        "5",
        "km",
        "STOREDIST",
    ],
    &["GEORADIUS", "g", "1", "2", "5", "km", "WITHCOORD"],
    &["GEORADIUSBYMEMBER", "g", "m", "5", "km", "STORE", "d"],
    &["BF.RESERVE", "f", "0.01", "1000"],
    &["BF.RESERVE", "f", "0.01", "1000", "NONSCALING"],
    &[
        "BF.INSERT",
        "f",
        "CAPACITY",
        "100",
        "ERROR",
        "0.01",
        "NOCREATE",
        "ITEMS",
        "a",
        "b",
    ],
    &["XREVRANGE", "x", "+", "-", "COUNT", "2"],
    &["XREAD", "COUNT", "2", "STREAMS", "x", "0"],
    &["XREAD", "BLOCK", "10", "STREAMS", "x", "$"],
    &["XTRIM", "x", "MAXLEN", "~", "10"],
    &["XTRIM", "x", "MINID", "5-0"],
    &[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "COUNT",
        "1",
        "STREAMS",
        "x",
        ">",
    ],
    &["XCLAIM", "x", "g", "c", "0", "1-1", "JUSTID"],
    &["SUBSTR", "k", "0", "-1"],
    &["LMOVE", "a", "b", "LEFT", "RIGHT"],
    &["BLMOVE", "a", "b", "RIGHT", "LEFT", "0.1"],
    &["LMPOP", "2", "a", "b", "LEFT", "COUNT", "2"],
    &["BLMPOP", "0.1", "1", "a", "RIGHT"],
    &["OBJECT", "ENCODING", "k"],
    &["OBJECT", "IDLETIME", "k"],
    &["MEMORY", "USAGE", "k", "SAMPLES", "5"],
    &["LATENCY", "LATEST"],
    &["LIMIT", "k", "5", "60000"],
    &["SEMAPHORE", "k", "2", "5000"],
    &["LTRIM", "l", "0", "-1"],
    &["ZRANGE", "z", "0", "-1"],
    &["ZREVRANGE", "z", "0", "-1"],
    &["ZRANGEBYLEX", "z", "[a", "[b"],
    &["ZREMRANGEBYRANK", "z", "0", "1"],
    &["ZREMRANGEBYLEX", "z", "[a", "[b"],
    &["GEOADD", "g", "13.36", "38.11", "m"],
    &["XRANGE", "x", "-", "+"],
    &["XAUTOCLAIM", "x", "g", "c", "0", "0-0"],
    &["SETBIT", "k", "1", "0"],
    &["FILLSET", "k", "v", "60000", "1"],
    &["GETRANGE", "k", "0", "-1"],
    &["SINTERCARD", "1", "a"],
    &["TRANSFER", "a", "b", "4", "MAX", "9"],
    &["HTRANSFER", "h", "f", "g", "1", "MAX", "5"],
    &["RUVIO", "MOVESLOTS", "0-100", "1"],
    &["RUVIO", "MOVESLOTS", "7", "0"],
    &["RUVIO", "MOVESLOTS", "STATUS"],
];

#[test]
fn structured_command_forms_parse_and_cover_the_hard_to_guess_syntax() {
    for words in STRUCTURED {
        let bytes = frame(words);

        match parse_command(&bytes) {
            Ok(Some((_, consumed))) => assert_eq!(consumed, bytes.len(), "{words:?}"),
            other => panic!("{words:?} did not parse: {:?}", other.map(|_| ())),
        }
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as usize
    }
}

#[test]
fn every_matrix_command_is_recognised_and_no_argument_mix_breaks_the_parser() {
    let commands = matrix_commands();
    let mut unrecognised = Vec::new();
    // A form names its command, or its subcommand when the matrix lists one (`RUVIO MOVESLOTS`).
    let structured: std::collections::HashSet<String> = STRUCTURED
        .iter()
        .flat_map(|words| [words[0].to_owned(), words[..words.len().min(2)].join(" ")])
        .collect();
    assert!(
        commands.len() > 250,
        "matrix looks truncated: {}",
        commands.len()
    );

    for name in &commands {
        let mut head: Vec<&str> = name.split(' ').collect();
        let mut recognised = false;

        // One generator per command, so adding a command to the matrix does not change the
        // argument mixes tried for the others.
        let mut rng = Rng(name.bytes().fold(0x9e37_79b9_7f4a_7c15, |hash: u64, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        }));

        // Whole-frame truncations never panic and report "incomplete" or an error, never a short read.
        let full = frame(&head);
        for cut in 0..full.len() {
            let _ = parse_command(&full[..cut]);
        }

        for round in 0..1500 {
            let extra = round % 8;

            head.truncate(name.split(' ').count());

            for _ in 0..extra {
                head.push(POOL[rng.next() % POOL.len()]);
            }

            let bytes = frame(&head);

            match parse_command(&bytes) {
                Ok(Some((_, consumed))) => {
                    assert_eq!(
                        consumed,
                        bytes.len(),
                        "{head:?} consumed a wrong byte count"
                    );
                    recognised = true;
                }
                Ok(None) => panic!("{head:?}: a complete frame was reported incomplete"),
                Err(_) => {}
            }

            // The same frame with the last byte missing must never parse as complete.
            assert!(
                !matches!(parse_command(&bytes[..bytes.len() - 1]), Ok(Some(_))),
                "{head:?}: a truncated frame parsed as complete"
            );
        }

        if !recognised && !structured.contains(name.as_str()) {
            unrecognised.push(name.clone());
        }
    }

    assert!(
        unrecognised.is_empty(),
        "matrix commands the parser never accepted with any argument mix: {unrecognised:?}"
    );
}

#[test]
fn inline_and_malformed_frames_are_rejected_without_panicking() {
    for bytes in [
        b"".as_slice(),
        b"\r\n",
        b"*0\r\n",
        b"*-1\r\n",
        b"*1\r\n",
        b"*1\r\n$3\r\nGE",
        b"*1\r\n$-1\r\n",
        b"*1\r\n$3\r\nGETX\r\n",
        b"*1\r\n:5\r\n",
        b"*2\r\n$3\r\nGET\r\n$1\r\n",
        b"*99999999999\r\n",
        b"*1\r\n$99999999999\r\nx\r\n",
        b"$3\r\nGET\r\n",
        b"PING\r\n",
        b"SET a b\r\n",
        b"  \r\n",
        b"GET \"unterminated\r\n",
        b"GET 'a b' \r\n",
        b"\xff\xfe\r\n",
    ] {
        let _ = parse_command(bytes);
    }

    let (command, consumed) = parse_command(b"PING\r\n").unwrap().unwrap();

    assert_eq!(consumed, 6);
    drop(command);
    assert!(matches!(parse_command(b"*1\r\n$4\r\nPING"), Ok(None)));
    assert!(parse_command(b"*1\r\n:5\r\n").is_err());
}
