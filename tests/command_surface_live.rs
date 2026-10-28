//! Whole-command-surface checks against a real server process. Each test walks every command
//! listed in `docs/compatibility-matrix.json`, so a command added later is covered without
//! touching this file:
//!
//! - numeric arguments at the integer extremes never abort the server, never hang a reply and
//!   never close the connection;
//! - no command runs before AUTH, on any entry path (direct, MULTI, EVAL, and a key owned by
//!   another shard behind a single port);
//! - a user denied a command, or the command's keys, is refused on every one of those paths.
//!
//! Argument vectors are found by a seeded random search over a token pool, kept only when the
//! library parser accepts them, so the server sees well-formed commands that reach the code
//! behind the parser.

mod common;

use std::{
    collections::{BTreeMap, HashSet},
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use common::Server;
use ruvio::protocol::parse_command;

// ---------------------------------------------------------------------------------------------
// A small RESP2 client that reports failures instead of panicking, so a dead connection is
// attributed to the command that killed it.

#[derive(Debug, Clone)]
enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }

    fn error_text(&self) -> Option<&str> {
        match self {
            Reply::Error(text) => Some(text),
            _ => None,
        }
    }

    /// Every error nested anywhere in the reply (an EXEC result carries per-command errors).
    fn errors(&self) -> Vec<String> {
        match self {
            Reply::Error(text) => vec![text.clone()],
            Reply::Array(Some(items)) => items.iter().flat_map(Reply::errors).collect(),
            _ => Vec::new(),
        }
    }

    fn text(&self) -> String {
        match self {
            Reply::Simple(text) => text.clone(),
            Reply::Error(text) => format!("-{text}"),
            Reply::Integer(value) => value.to_string(),
            Reply::Bulk(Some(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            Reply::Bulk(None) | Reply::Array(None) => "(nil)".into(),
            Reply::Array(Some(items)) => {
                format!(
                    "[{}]",
                    items.iter().map(Reply::text).collect::<Vec<_>>().join(",")
                )
            }
        }
    }
}

/// The steps a long test has taken, with their times, and the one it is in. A watchdog
/// prints them once the test runs far past its usual few seconds, so a test that slows down
/// on one platform names the step instead of only being killed at the time limit.
struct Progress {
    steps: std::sync::Mutex<Steps>,
}

/// The current step, when it started, and every finished step with its time.
type Steps = (String, Instant, Vec<(String, Duration)>);

impl Progress {
    fn watched(after: Duration) -> std::sync::Arc<Self> {
        let progress = std::sync::Arc::new(Self {
            steps: std::sync::Mutex::new(("start".to_owned(), Instant::now(), Vec::new())),
        });
        let watched = std::sync::Arc::downgrade(&progress);

        std::thread::spawn(move || {
            std::thread::sleep(after);
            while let Some(progress) = watched.upgrade() {
                progress.report();
                drop(progress);
                std::thread::sleep(Duration::from_secs(15));
            }
        });
        progress
    }

    fn step(&self, name: String) {
        let mut steps = self.steps.lock().unwrap_or_else(|error| error.into_inner());
        let (current, started, done) = &mut *steps;

        done.push((std::mem::replace(current, name), started.elapsed()));
        *started = Instant::now();
    }

    fn report(&self) {
        let steps = self.steps.lock().unwrap_or_else(|error| error.into_inner());
        let (current, started, done) = &*steps;
        let mut slowest = done.clone();

        slowest.sort_by_key(|(_, took)| std::cmp::Reverse(*took));
        slowest.truncate(15);
        eprintln!(
            "still running: {current} for {:?} after {} steps; slowest steps: {slowest:?}",
            started.elapsed(),
            done.len()
        );
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn open(address: &str, timeout: Duration) -> Self {
        let stream = TcpStream::connect(address).expect("connect");

        stream.set_read_timeout(Some(timeout)).unwrap();
        stream.set_write_timeout(Some(timeout)).unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            writer: stream.try_clone().unwrap(),
            reader: BufReader::new(stream),
        }
    }

    fn send_frame(&mut self, frame: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(frame)
    }

    fn read(&mut self) -> std::io::Result<Reply> {
        let mut line = String::new();

        if self.reader.read_line(&mut line)? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }

        let line = line.trim_end_matches(['\r', '\n']);
        let bad = || std::io::Error::new(std::io::ErrorKind::InvalidData, line.to_owned());

        if line.is_empty() {
            return Err(bad());
        }

        let (kind, rest) = line.split_at(1);

        Ok(match kind {
            "+" => Reply::Simple(rest.to_owned()),
            "-" => Reply::Error(rest.to_owned()),
            ":" => Reply::Integer(rest.parse().map_err(|_| bad())?),
            "$" => {
                let len: i64 = rest.parse().map_err(|_| bad())?;

                if len < 0 {
                    Reply::Bulk(None)
                } else {
                    let mut payload = vec![0; len as usize + 2];

                    self.reader.read_exact(&mut payload)?;
                    payload.truncate(len as usize);
                    Reply::Bulk(Some(payload))
                }
            }
            "*" => {
                let count: i64 = rest.parse().map_err(|_| bad())?;

                if count < 0 {
                    Reply::Array(None)
                } else {
                    let mut items = Vec::with_capacity(count.min(1024) as usize);

                    for _ in 0..count {
                        items.push(self.read()?);
                    }
                    Reply::Array(Some(items))
                }
            }
            _ => return Err(bad()),
        })
    }

    fn call(&mut self, words: &[&str]) -> std::io::Result<Reply> {
        self.send_frame(&frame(words))?;
        self.read()
    }

    fn must(&mut self, words: &[&str]) -> Reply {
        self.call(words)
            .unwrap_or_else(|error| panic!("{words:?}: {error}"))
    }
}

fn frame(words: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", words.len()).into_bytes();

    for word in words {
        out.extend(format!("${}\r\n", word.len()).into_bytes());
        out.extend(word.as_bytes());
        out.extend(b"\r\n");
    }

    out
}

fn assert_alive(address: &str, context: &str) {
    let mut probe = Conn::open(address, Duration::from_secs(5));

    match probe.call(&["PING"]) {
        Ok(Reply::Simple(pong)) if pong == "PONG" => {}
        other => panic!("server stopped answering after {context}: {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Command surface and argument generation.

/// Subcommand forms that are not separate rows in the matrix.
const EXTRA_HEADS: &[&str] = &[
    "OBJECT ENCODING",
    "OBJECT FREQ",
    "OBJECT IDLETIME",
    "OBJECT REFCOUNT",
    "MEMORY USAGE",
    "SLOWLOG GET",
    "SLOWLOG LEN",
    "SLOWLOG RESET",
    "LATENCY LATEST",
    "LATENCY HISTORY",
    "LATENCY RESET",
    "CLIENT NO-EVICT",
    "CLIENT TRACKINGINFO",
    "RUVIO OWNER",
    "RUVIO HOTKEYS",
    "RUVIO BIGKEYS",
    "COMMAND INFO",
    "COMMAND COUNT",
    "COMMAND GETKEYS",
    "FUNCTION FLUSH",
    "SCRIPT FLUSH",
];

fn command_heads() -> Vec<String> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/compatibility-matrix.json"
    ))
    .unwrap();
    let matrix: serde_json::Value = serde_json::from_str(&text).unwrap();
    let mut heads: Vec<String> = matrix["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|command| command["name"].as_str().unwrap().to_owned())
        .collect();

    assert!(heads.len() > 250, "matrix looks truncated: {}", heads.len());
    heads.extend(EXTRA_HEADS.iter().map(|head| (*head).to_owned()));
    heads.sort();
    heads.dedup();
    heads
}

/// Keys the fixture creates, one per value kind; `{t}` is replaced by a hash tag.
const KEY_KINDS: &[&str] = &[
    "str", "list", "set", "iset", "zset", "hash", "stream", "json", "bloom", "hll", "geo",
    "missing",
];

/// Non-key tokens: members and fields the fixture holds, options, numbers and JSON paths.
const WORDS: &[&str] = &[
    "a",
    "b",
    "f",
    "g",
    "c",
    "m1",
    "m2",
    "v",
    "0",
    "1",
    "2",
    "3",
    "-1",
    "10",
    "1.5",
    "-inf",
    "+inf",
    "",
    "*",
    "-",
    "+",
    "$",
    ">",
    "(1",
    "[a",
    "0-0",
    "1-1",
    "$.a",
    "$.n",
    "$.b",
    "{}",
    "[1,2]",
    "{\"a\":1}",
    "km",
    "EX",
    "PX",
    "EXAT",
    "PXAT",
    "NX",
    "XX",
    "GT",
    "LT",
    "CH",
    "INCR",
    "KEEPTTL",
    "GET",
    "COUNT",
    "MATCH",
    "TYPE",
    "LIMIT",
    "WITHSCORES",
    "WITHVALUES",
    "BYSCORE",
    "BYLEX",
    "REV",
    "STREAMS",
    "GROUP",
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
    "FROM",
    "IDLE",
    "TIME",
    "RETRYCOUNT",
    "JUSTID",
    "FORCE",
    "IDLETIME",
    "FREQ",
    "SAMPLES",
    "FIELDS",
    "ID",
    "REPLACE",
    "ABSTTL",
    "ASC",
    "DESC",
    "STORE",
    "STOREDIST",
    "WITHDIST",
    "WITHCOORD",
    "WITHHASH",
    "FROMMEMBER",
    "FROMLONLAT",
    "BYRADIUS",
    "BYBOX",
    "ANY",
    "RANK",
    "CAPACITY",
    "ERROR",
    "EXPANSION",
    "ITEMS",
    "NOCREATE",
    "NONSCALING",
    "WEIGHTS",
    "AGGREGATE",
    "SUM",
    "MIN",
    "MAX",
    "BIT",
    "BYTE",
    "OVERFLOW",
    "WRAP",
    "SAT",
    "FAIL",
    "u8",
    "i64",
    "#1",
    "ENTRIESREAD",
    "MKSTREAM",
    "CREATECONSUMER",
    "IDEM",
    "BEGIN",
    "OWNER",
    "ON",
    "OFF",
    "BCAST",
    "PREFIX",
    "OPTIN",
    "NOLOOP",
    "SKIPME",
    "yes",
    "no",
    "WAIT",
    "TTL",
    "CAS",
    "IFEQ",
    "TOKEN",
    "NOW",
    "return 1",
    "LOAD",
];

/// Integer extremes and boundary values substituted for every numeric argument.
const EXTREMES: &[&str] = &[
    "-9223372036854775808",
    "9223372036854775807",
    "-9223372036854775807",
    "-1",
    "0",
    "-4294967296",
    "4294967296",
    "18446744073709551615",
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as usize
    }
}

fn key(tag: &str, kind: &str) -> String {
    format!("{{{tag}}}{kind}")
}

/// Up to `limit` distinct argument vectors (head words first) that the parser accepts.
fn parseable_vectors(head: &str, tag: &str, limit: usize, seed: u64) -> Vec<Vec<String>> {
    let head_words: Vec<String> = head.split(' ').map(str::to_owned).collect();
    let keys: Vec<String> = KEY_KINDS.iter().map(|kind| key(tag, kind)).collect();
    let mut rng = Rng(seed ^ 0x9e37_79b9_7f4a_7c15);
    let mut found: Vec<Vec<String>> = Vec::new();
    let mut seen = HashSet::new();

    for round in 0..2_500 {
        let mut words = head_words.clone();
        let extra = round % 9;

        for position in 0..extra {
            // Keys are likelier in the first argument positions.
            let pick_key = rng.next().is_multiple_of(if position < 2 { 2 } else { 4 });

            if pick_key {
                words.push(keys[rng.next() % keys.len()].clone());
            } else {
                words.push(WORDS[rng.next() % WORDS.len()].to_owned());
            }
        }

        let refs: Vec<&str> = words.iter().map(String::as_str).collect();
        let bytes = frame(&refs);

        if matches!(parse_command(&bytes), Ok(Some(_))) && seen.insert(words.clone()) {
            found.push(words);
            if found.len() == limit {
                break;
            }
        }
    }

    found
}

/// Known forms that carry counts, offsets, timeouts, capacities and numkeys, with ordinary
/// numbers; `@kind` is the fixture key of that kind. Each number is then swapped for every
/// extreme. The random search adds forms nobody listed.
const TEMPLATES: &[&str] = &[
    "SRANDMEMBER @set 2",
    "SRANDMEMBER @iset -2",
    "HRANDFIELD @hash 2 WITHVALUES",
    "HRANDFIELD @hash -2",
    "ZRANDMEMBER @zset 2 WITHSCORES",
    "ZRANDMEMBER @zset -2",
    "SPOP @set 1",
    "LPOP @list 2",
    "RPOP @list 2",
    "ZPOPMIN @zset 1",
    "ZPOPMAX @zset 1",
    "ZPOPBYSCORE @zset 10 MIN 0 COUNT 2 WITHSCORES",
    "ZMOVEDUE @zset @list 10 COUNT 2",
    "LMPOP 1 @list LEFT COUNT 2",
    "SINTERCARD 1 @set LIMIT 2",
    "LPOS @list e1 RANK 1 COUNT 1 MAXLEN 10",
    "LINDEX @list 1",
    "LSET @list 1 x",
    "LRANGE @list 0 10",
    "LTRIM @list 0 100",
    "LREM @list 1 e1",
    "LPUSHCAP @list 10 x",
    "RPUSHCAP @list 10 x",
    "SCAN 0 COUNT 10",
    "SCAN 0 COUNT 10 MATCH * TYPE string",
    "HSCAN @hash 0 COUNT 10",
    "SSCAN @set 0 COUNT 10",
    "SSCAN @iset 0 COUNT 10",
    "ZSCAN @zset 0 COUNT 10",
    "ZRANGE @zset 0 10",
    "ZRANGE @zset 0 10 BYSCORE LIMIT 0 10",
    "ZRANGEBYSCORE @zset 0 10 LIMIT 0 10",
    "ZREVRANGEBYLEX @zset + - LIMIT 0 10",
    "ZREMRANGEBYRANK @zset 0 1",
    "ZRANK @zset a",
    "ZINCRBY @zset 1 a",
    "ZADD @zset 1 x",
    "ZUNION 1 @zset WEIGHTS 2",
    "ZUNIONSTORE @missing 1 @zset WEIGHTS 2",
    "ZINTER 1 @zset",
    "ZDIFF 1 @zset",
    "GETRANGE @str 0 10",
    "SETRANGE @str 1 x",
    "GETBIT @str 1",
    "SETBIT @str 1 1",
    "BITCOUNT @str 0 10",
    "BITCOUNT @str 0 10 BIT",
    "BITPOS @str 1 0 10",
    "BITFIELD @str GET u8 1",
    "BITFIELD @str SET i64 1 1",
    "BITFIELD @str INCRBY u8 1 1",
    "BITFIELD_RO @str GET u8 1",
    "INCRBY @str 1",
    "DECRBY @str 1",
    "INCRBYFLOAT @str 1",
    "HINCRBY @hash g 1",
    "HINCRBYFLOAT @hash g 1",
    "EXPIRE @str 10",
    "PEXPIRE @str 10",
    "EXPIREAT @str 10",
    "PEXPIREAT @str 10",
    "SET @str v EX 10",
    "SET @str v PX 10",
    "SET @str v EXAT 10",
    "SET @str v PXAT 10",
    "GETEX @str EX 10",
    "GETEX @str PXAT 10",
    "SETEX @str 10 v",
    "PSETEX @str 10 v",
    "RESTORE @missing 10 x",
    "HEXPIRE @hash 10 FIELDS 1 f",
    "HPEXPIRE @hash 10 FIELDS 1 f",
    "HEXPIREAT @hash 10 FIELDS 1 f",
    "HPEXPIREAT @hash 10 FIELDS 1 f",
    "HTTL @hash FIELDS 1 f",
    "HPERSIST @hash FIELDS 1 f",
    "XRANGE @stream - + COUNT 1",
    "XREVRANGE @stream + - COUNT 1",
    "XADD @stream MAXLEN 10 * f v",
    "XADD @stream MAXLEN ~ 10 LIMIT 10 * f v",
    "XTRIM @stream MAXLEN 10",
    "XTRIM @stream MAXLEN ~ 10 LIMIT 10",
    "XPENDING @stream g - + 10",
    "XPENDING @stream g IDLE 10 - + 10 c",
    "XAUTOCLAIM @stream g c 10 0 COUNT 10",
    "XCLAIM @stream g c 10 1-1 IDLE 10 RETRYCOUNT 10",
    "XINFO STREAM @stream FULL COUNT 10",
    "XGROUP SETID @stream g 0 ENTRIESREAD 10",
    "XREAD COUNT 10 STREAMS @stream 0",
    "XREADGROUP GROUP g c COUNT 10 STREAMS @stream 0",
    "XREAD COUNT 1 BLOCK 10 STREAMS @stream $",
    "XREADGROUP GROUP g c COUNT 1 BLOCK 10 STREAMS @stream >",
    "GEOSEARCH @geo FROMMEMBER m1 BYRADIUS 100 km COUNT 10",
    "GEOSEARCH @geo FROMLONLAT 15 37 BYBOX 100 100 km COUNT 10 ANY",
    "GEORADIUS @geo 15 37 100 km COUNT 10",
    "GEORADIUSBYMEMBER @geo m1 100 km COUNT 10",
    "GEOADD @geo 10 10 x",
    "OBJECT FREQ @str",
    "MEMORY USAGE @hash SAMPLES 10",
    "SLOWLOG GET 10",
    "BF.RESERVE @missing 0.01 10",
    "BF.RESERVE @missing 0.01 10 EXPANSION 2",
    "BF.INSERT @missing CAPACITY 10 ERROR 0.01 ITEMS a",
    "JSON.ARRPOP @json $.a 1",
    "JSON.ARRINSERT @json $.a 1 4",
    "JSON.ARRINDEX @json $.a 1 0 10",
    "JSON.ARRTRIM @json $.a 0 10",
    "JSON.NUMINCRBY @json $.n 1",
    "JSON.NUMMULTBY @json $.n 2",
    "EVAL return(1) 0",
    "FCALL f 0",
    "SELECT 1",
    "MOVE @str 1",
    "SWAPDB 0 1",
    "COPY @str @missing DB 1",
    "LIMIT @missing 10 1000",
    "TAKE @str 1",
    "TRANSFER @str @missing 1",
    "HTRANSFER @hash f g 1",
    "ONCE @missing 1000",
    "LEASE @missing 1000",
    "SEMAPHORE @missing 10 1000",
    "IDEM @missing BEGIN fp owner 1000",
    "SETV @missing v",
    "FILLGET @missing 1000",
    "CHANGES 0",
    "BLPOP @list 1",
    "BRPOP @missing 1",
    "BLMOVE @list @missing LEFT RIGHT 1",
    "BLMPOP 1 1 @list LEFT COUNT 2",
    "BZPOPMIN @zset 1",
    "BZPOPMAX @missing 1",
    "WAIT 0 1",
    "CLIENT NO-EVICT on",
];

fn template_vector(template: &str, tag: &str) -> Vec<String> {
    template
        .split(' ')
        .map(|word| match word.strip_prefix('@') {
            Some(kind) => key(tag, kind),
            None => word.to_owned(),
        })
        .collect()
}

fn is_number(token: &str) -> bool {
    !token.is_empty()
        && token
            .strip_prefix('-')
            .unwrap_or(token)
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
        && token.bytes().any(|byte| byte.is_ascii_digit())
}

/// Every vector with one numeric argument replaced by each extreme.
fn extreme_variants(vector: &[String], head_len: usize) -> Vec<Vec<String>> {
    let mut variants = Vec::new();

    for position in head_len..vector.len() {
        if !is_number(&vector[position]) {
            continue;
        }

        for extreme in EXTREMES {
            let mut variant = vector.to_vec();

            variant[position] = (*extreme).to_owned();
            variants.push(variant);
        }
    }

    variants
}

fn fixture(tag: &str) -> Vec<Vec<String>> {
    let k = |kind: &str| key(tag, kind);
    let owned =
        |words: &[&str]| -> Vec<String> { words.iter().map(|word| (*word).to_owned()).collect() };
    let mut list = vec!["RPUSH".to_owned(), k("list")];

    list.extend((0..64).map(|index| format!("e{index}")));
    vec![
        owned(&["SET", &k("str"), "12345"]),
        list,
        owned(&["SADD", &k("set"), "a", "b", "c", "m1"]),
        owned(&["SADD", &k("iset"), "1", "2", "3"]),
        owned(&["ZADD", &k("zset"), "1", "a", "2", "b", "3", "c"]),
        owned(&["HSET", &k("hash"), "f", "v", "g", "1", "a", "b"]),
        owned(&["XADD", &k("stream"), "1-1", "f", "v"]),
        owned(&["XADD", &k("stream"), "2-1", "f", "v"]),
        owned(&["XGROUP", "CREATE", &k("stream"), "g", "0"]),
        owned(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "STREAMS",
            &k("stream"),
            ">",
        ]),
        owned(&[
            "JSON.SET",
            &k("json"),
            "$",
            "{\"a\":[1,2,3],\"b\":\"x\",\"n\":1}",
        ]),
        owned(&["BF.RESERVE", &k("bloom"), "0.01", "100"]),
        owned(&["BF.ADD", &k("bloom"), "a"]),
        owned(&["PFADD", &k("hll"), "a", "b"]),
        owned(&[
            "GEOADD",
            &k("geo"),
            "13.36",
            "38.11",
            "m1",
            "15.08",
            "37.50",
            "m2",
        ]),
    ]
}

fn words_frame(words: &[String]) -> Vec<u8> {
    let refs: Vec<&str> = words.iter().map(String::as_str).collect();

    frame(&refs)
}

/// `COMMAND INFO` facts the tests need: flags and the first key position.
struct Info {
    blocking: bool,
    first_key: i64,
}

fn command_info(conn: &mut Conn) -> BTreeMap<String, Info> {
    let Reply::Array(Some(rows)) = conn.must(&["COMMAND"]) else {
        panic!("COMMAND did not answer an array");
    };
    let mut out = BTreeMap::new();

    for row in rows {
        let Reply::Array(Some(fields)) = row else {
            continue;
        };
        let name = fields[0].text().to_uppercase();
        let blocking = matches!(&fields[2], Reply::Array(Some(flags)) if flags.iter().any(|flag| flag.text() == "blocking"));
        let first_key = match fields[3] {
            Reply::Integer(value) => value,
            _ => 0,
        };

        out.insert(
            name,
            Info {
                blocking,
                first_key,
            },
        );
    }

    out
}

fn info_for<'a>(infos: &'a BTreeMap<String, Info>, head: &str) -> Option<&'a Info> {
    infos.get(head.split(' ').next().unwrap())
}

// ---------------------------------------------------------------------------------------------
// Numeric extremes.

/// Commands that change the connection or server in ways a later command in the same batch
/// cannot recover from, or that kill other connections. They are covered by their own tests.
fn excluded_from_extremes(head: &str) -> bool {
    let first = head.split(' ').next().unwrap();

    matches!(
        first,
        "HELLO"
            | "AUTH"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "SSUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "SUNSUBSCRIBE"
    ) || matches!(
        head,
        "ACL SETUSER" | "ACL LOAD" | "ACL SAVE" | "CLIENT KILL" | "CONFIG SET"
    )
}

/// A command may block (a blocking verb, WAIT, or a BLOCK option): run it alone on its own
/// connection with a short wait, so blocking is allowed but crashing is not.
fn may_block(vector: &[String], info: Option<&Info>) -> bool {
    info.is_some_and(|info| info.blocking)
        || vector[0] == "WAIT"
        || vector.iter().any(|word| word == "BLOCK")
}

fn run_batch(address: &str, tag: &str, head: &str, frames: &[Vec<String>]) {
    let mut conn = Conn::open(address, Duration::from_secs(20));
    let mut sent = Vec::new();

    let mut setup = vec![
        vec!["SELECT".to_owned(), "0".to_owned()],
        vec!["FLUSHALL".to_owned()],
    ];
    setup.extend(fixture(tag));
    let mut payload = Vec::new();
    for words in setup.iter().chain(frames.iter()) {
        payload.extend(words_frame(words));
        sent.push(words.clone());
    }
    payload.extend(frame(&["PING"]));
    sent.push(vec!["PING".to_owned()]);

    // The batch is written while its replies are read. Written first, a batch whose replies
    // outgrow the socket buffers (small on Windows) stalls both ways: the server stops reading
    // while its replies wait, and this side stops reading while its write waits.
    let mut writer = conn.writer.try_clone().unwrap();
    let sending = std::thread::spawn(move || writer.write_all(&payload));
    let mut failure = None;

    for (index, words) in sent.iter().enumerate() {
        match conn.read() {
            Ok(reply) => {
                if index == sent.len() - 1 {
                    assert_eq!(reply.text(), "PONG", "{head}: batch did not end in PONG");
                }
            }
            Err(error) => {
                failure = Some(format!("reading the reply to {words:?}: {error}"));
                // Unblocks the writer if the server stopped reading.
                let _ = conn.writer.shutdown(std::net::Shutdown::Both);
                break;
            }
        }
    }

    let sent_all = sending.join().unwrap();

    if let Some(why) =
        failure.or_else(|| sent_all.err().map(|error| format!("send failed: {error}")))
    {
        narrow_down(address, tag, head, frames, &why);
    }
}

fn narrow_down(address: &str, tag: &str, head: &str, frames: &[Vec<String>], why: &str) -> ! {
    if TcpStream::connect(address).is_err() {
        panic!(
            "{head}: the server process died running {} frames such as {:?} ({why})",
            frames.len(),
            &frames[..frames.len().min(4)]
        );
    }

    for words in frames {
        let mut conn = Conn::open(address, Duration::from_secs(10));
        let mut payload = Vec::new();

        for setup in fixture(tag) {
            payload.extend(words_frame(&setup));
        }
        payload.extend(words_frame(words));
        payload.extend(frame(&["PING"]));
        let fixture_len = fixture(tag).len();

        let ok =
            conn.send_frame(&payload).is_ok() && (0..fixture_len + 2).all(|_| conn.read().is_ok());

        if !ok {
            panic!("{head}: {words:?} broke the connection or the server ({why})");
        }
    }

    panic!("{head}: a batch broke the connection but no single frame does ({why})");
}

fn run_blocking(address: &str, tag: &str, head: &str, frames: &[Vec<String>]) {
    for chunk in frames.chunks(64) {
        let mut open = Vec::new();

        for words in chunk {
            let mut conn = Conn::open(address, Duration::from_secs(10));
            let mut payload = Vec::new();

            for setup in fixture(tag) {
                payload.extend(words_frame(&setup));
            }
            conn.send_frame(&payload).unwrap();
            for _ in 0..fixture(tag).len() {
                conn.read()
                    .unwrap_or_else(|error| panic!("{head}: fixture before {words:?}: {error}"));
            }
            // Sent on its own: replies queued ahead of a command that parks are not
            // what this test is about.
            conn.send_frame(&words_frame(words)).unwrap();
            open.push((conn, words));
        }

        // One shared wait: a command that does not block has answered by now.
        std::thread::sleep(Duration::from_millis(300));

        for (mut conn, words) in open {
            // Set on the stream the reads go through. Set on the writer, it relied on a cloned
            // socket handle sharing its receive timeout, which Windows does not promise; if it
            // does not, every command that stays blocked costs a 10 s read here.
            conn.reader
                .get_ref()
                .set_read_timeout(Some(Duration::from_millis(1)))
                .unwrap();
            match conn.read() {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => panic!("{head}: {words:?} closed the connection: {error}"),
            }
        }
        // Every client above hung up while possibly blocked; the server must not care.
        assert_alive(address, head);
    }
}

/// The candidates that run without an error against the fixture: these reach the code
/// behind argument checking, which is where a bad count does its damage.
fn succeeding(address: &str, tag: &str, candidates: Vec<Vec<String>>) -> Vec<Vec<String>> {
    if candidates.is_empty() {
        return candidates;
    }

    let mut conn = Conn::open(address, Duration::from_secs(20));
    let setup = fixture(tag);
    let mut payload = Vec::new();

    payload.extend(frame(&["SELECT", "0"]));
    payload.extend(frame(&["FLUSHALL"]));
    for words in &setup {
        payload.extend(words_frame(words));
    }
    for words in &candidates {
        payload.extend(words_frame(words));
    }
    conn.send_frame(&payload).unwrap();
    for _ in 0..setup.len() + 2 {
        conn.read().expect("fixture reply");
    }

    candidates
        .into_iter()
        .filter(|words| {
            let reply = conn
                .read()
                .unwrap_or_else(|error| panic!("{words:?} with ordinary numbers: {error}"));

            !reply.is_error()
        })
        .collect()
}

/// The head a template belongs to: the longest listed head it starts with.
fn template_head<'a>(template: &str, heads: &'a [String]) -> Option<&'a String> {
    heads
        .iter()
        .filter(|head| template == head.as_str() || template.starts_with(&format!("{head} ")))
        .max_by_key(|head| head.len())
}

#[test]
fn every_command_survives_extreme_numbers_and_keeps_the_connection() {
    let progress = Progress::watched(Duration::from_secs(45));
    let server = Server::start_with(&[("RUVIO_MAXMEMORY", "268435456")]);
    let address = server.address.clone();
    progress.step("COMMAND INFO".to_owned());
    let infos = command_info(&mut Conn::open(&address, Duration::from_secs(10)));
    let tag = "a";
    let heads = command_heads();
    let mut total = 0usize;
    let mut heads_with_numbers = 0usize;

    for template in TEMPLATES {
        assert!(
            template_head(template, &heads).is_some(),
            "template {template} names no listed command"
        );
    }

    for (index, head) in heads.iter().enumerate() {
        if excluded_from_extremes(head) {
            continue;
        }

        let head_len = head.split(' ').count();
        let info = info_for(&infos, head);
        let mut vectors: Vec<Vec<String>> = TEMPLATES
            .iter()
            .filter(|template| template_head(template, &heads) == Some(head))
            .map(|template| template_vector(template, tag))
            .collect();
        let random: Vec<Vec<String>> = parseable_vectors(head, tag, 64, index as u64 + 1)
            .into_iter()
            .filter(|vector| {
                !may_block(vector, info) && vector[head_len..].iter().any(|word| is_number(word))
            })
            .collect();

        progress.step(format!("{head}: ordinary numbers"));
        vectors.extend(succeeding(&address, tag, random).into_iter().take(12));

        let mut batch = Vec::new();
        let mut blocking = Vec::new();

        for vector in &vectors {
            for variant in extreme_variants(vector, head_len) {
                if may_block(&variant, info) {
                    blocking.push(variant);
                } else {
                    batch.push(variant);
                }
            }
        }

        if batch.is_empty() && blocking.is_empty() {
            continue;
        }
        heads_with_numbers += 1;
        total += batch.len() + blocking.len();

        progress.step(format!("{head}: {} extreme frames", batch.len()));
        for chunk in batch.chunks(800) {
            run_batch(&address, tag, head, chunk);
        }
        progress.step(format!("{head}: {} blocking frames", blocking.len()));
        run_blocking(&address, tag, head, &blocking);
    }

    assert_alive(&address, "the whole surface");
    assert!(
        heads_with_numbers > 120 && total > 5_000,
        "too little of the surface was exercised: {heads_with_numbers} commands, {total} frames"
    );
}

// ---------------------------------------------------------------------------------------------
// Authentication and ACL on every entry path.

const PASSWORD: &str = "surface-secret";

/// Two shards behind one port, so a key owned by the other shard takes the forwarding path.
fn secured_server() -> Server {
    Server::start_with(&[
        ("RUVIO_SHARDS", "2"),
        ("RUVIO_PORT_MODE", "single"),
        ("RUVIO_REQUIREPASS", PASSWORD),
    ])
}

/// One hash tag per shard of a two-shard server.
fn tag_per_shard() -> [String; 2] {
    let mut tags: [Option<String>; 2] = [None, None];

    for index in 0..1_000 {
        let tag = format!("t{index}");
        let shard = ruvio::routing::shard_for_slot(
            ruvio::routing::hash_slot(key(&tag, "str").as_bytes()),
            2,
        );

        tags[shard].get_or_insert(tag);
        if tags.iter().all(Option::is_some) {
            break;
        }
    }

    tags.map(|tag| tag.expect("a tag for every shard"))
}

/// A few parseable vectors per command, with keys on each shard.
fn surface_vectors(tags: &[String]) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();

    for (index, head) in command_heads().iter().enumerate() {
        for tag in tags {
            for vector in parseable_vectors(head, tag, 2, index as u64 + 7) {
                out.push((head.clone(), vector));
            }
        }
    }

    out
}

/// Runs ARGV as one command from a script. `ruvio.call` raises the refusal as the script's
/// error, so it reaches the client.
const EVAL_ANY: &str = "return ruvio.call(unpack(ARGV))";

/// `vector` from a script, with every key-like token declared, so the refusal tested is the
/// ACL's and not the undeclared-key check.
fn eval_frame(vector: &[String]) -> Vec<String> {
    let keys: Vec<String> = vector
        .iter()
        .filter(|word| word.starts_with("{t") && word.contains('}'))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut words = vec![
        "EVAL".to_owned(),
        EVAL_ANY.to_owned(),
        keys.len().to_string(),
    ];

    words.extend(keys);
    words.extend(vector.iter().cloned());
    words
}

#[test]
fn no_command_runs_before_auth_on_any_entry_path() {
    let server = secured_server();
    let address = server.address.clone();
    let tags = tag_per_shard();
    let vectors = surface_vectors(&tags);
    let mut conn = Conn::open(&address, Duration::from_secs(5));
    let mut checked = 0;

    for (head, vector) in &vectors {
        let first = vector[0].as_str();
        // The server may close a client that keeps failing before AUTH; start a new one.
        let reply = match conn
            .send_frame(&words_frame(vector))
            .and_then(|()| conn.read())
        {
            Ok(reply) => reply,
            Err(_) => {
                conn = Conn::open(&address, Duration::from_secs(5));
                conn.send_frame(&words_frame(vector)).unwrap();
                conn.read()
                    .unwrap_or_else(|error| panic!("{vector:?} before AUTH: {error}"))
            }
        };

        match first {
            // Login commands answer with their own error for a wrong or missing password.
            "AUTH" | "HELLO" => assert!(reply.is_error(), "{vector:?} before AUTH: {reply:?}"),
            _ => assert!(
                reply
                    .error_text()
                    .is_some_and(|text| text.starts_with("NOAUTH")),
                "{head}: {vector:?} before AUTH answered {reply:?}"
            ),
        }
        checked += 1;
    }

    // MULTI and EVAL are entry paths too: neither may open a way in.
    for words in [
        vec!["MULTI"],
        vec!["EXEC"],
        vec!["EVAL", "return ruvio.call('PING')", "0"],
        vec!["EVAL", "return ruvio.call('FLUSHALL')", "0"],
    ] {
        let mut conn = Conn::open(&address, Duration::from_secs(5));
        let reply = conn.must(&words);

        assert!(
            reply
                .error_text()
                .is_some_and(|text| text.starts_with("NOAUTH")),
            "{words:?} before AUTH answered {reply:?}"
        );
    }

    // Nothing above ran: the keyspace on both shards is still empty.
    let mut admin = Conn::open(&address, Duration::from_secs(5));
    admin.must(&["AUTH", PASSWORD]);
    assert_eq!(
        admin.must(&["DBSIZE"]).text(),
        "0",
        "a command ran before AUTH"
    );
    assert!(checked > 400, "only {checked} pre-auth frames were checked");
}

#[test]
fn oversized_input_before_auth_is_refused_and_the_server_keeps_serving() {
    let server = secured_server();
    let address = server.address.clone();

    // One huge bulk header and payload from a client that has not logged in.
    let mut conn = Conn::open(&address, Duration::from_secs(5));
    let mut payload = b"*2\r\n$4\r\nAUTH\r\n$4000000\r\n".to_vec();
    payload.extend(std::iter::repeat_n(b'x', 1_000_000));
    let _ = conn.send_frame(&payload);
    let mut rest = Vec::new();
    let _ = conn.reader.read_to_end(&mut rest);
    assert!(
        rest.is_empty() || rest.starts_with(b"-"),
        "an unauthenticated oversized frame got a non-error answer: {:?}",
        String::from_utf8_lossy(&rest[..rest.len().min(80)])
    );

    // Many arguments before AUTH.
    let mut conn = Conn::open(&address, Duration::from_secs(5));
    let _ = conn.send_frame(b"*1000000\r\n");
    let mut rest = Vec::new();
    let _ = conn.reader.read_to_end(&mut rest);
    assert!(rest.is_empty() || rest.starts_with(b"-"));

    let mut admin = Conn::open(&address, Duration::from_secs(5));
    assert_eq!(admin.must(&["AUTH", PASSWORD]).text(), "OK");
    assert_eq!(admin.must(&["PING"]).text(), "PONG");
}

/// Commands the plumbing user may run; everything else must be refused.
const PLUMBING: &[&str] = &[
    "AUTH", "HELLO", "MULTI", "EXEC", "DISCARD", "EVAL", "EVALSHA", "PING", "SELECT",
];

fn login(address: &str, user: &str) -> Conn {
    let mut conn = Conn::open(address, Duration::from_secs(5));
    let reply = conn.must(&["AUTH", user, "pw"]);

    assert_eq!(reply.text(), "OK", "login as {user}");
    conn
}

fn is_noperm(reply: &Reply) -> bool {
    reply.errors().iter().any(|text| text.starts_with("NOPERM"))
}

/// Runs `vector` directly, inside MULTI and from a script; every path must refuse with NOPERM.
/// A refused script call may also say the command is not allowed from scripts at all.
fn assert_refused_everywhere(conn: &mut Conn, head: &str, vector: &[String], what: &str) {
    let direct = conn
        .send_frame(&words_frame(vector))
        .and_then(|()| conn.read())
        .unwrap_or_else(|error| panic!("{what} {vector:?}: {error}"));
    assert!(
        is_noperm(&direct),
        "{what}: {head} {vector:?} ran directly: {direct:?}"
    );

    conn.must(&["MULTI"]);
    let queued = conn
        .send_frame(&words_frame(vector))
        .and_then(|()| conn.read())
        .unwrap_or_else(|error| panic!("{what} MULTI {vector:?}: {error}"));
    let exec = conn.must(&["EXEC"]);
    // A command refused while queueing never runs, whatever the reason given.
    assert!(
        queued.is_error() || is_noperm(&exec),
        "{what}: {head} {vector:?} ran inside MULTI: queued {queued:?}, exec {exec:?}"
    );

    let scripted = conn
        .send_frame(&words_frame(&eval_frame(vector)))
        .and_then(|()| conn.read())
        .unwrap_or_else(|error| panic!("{what} EVAL {vector:?}: {error}"));
    let script_refused = scripted
        .errors()
        .iter()
        .any(|text| text.starts_with("NOPERM") || text.contains("not allowed from script"));
    assert!(
        script_refused,
        "{what}: {head} {vector:?} ran from a script: {scripted:?}"
    );
}

#[test]
fn a_user_denied_a_command_is_refused_on_every_entry_path() {
    let server = secured_server();
    let address = server.address.clone();
    let tags = tag_per_shard();
    let mut admin = Conn::open(&address, Duration::from_secs(5));

    admin.must(&["AUTH", PASSWORD]);
    let mut rules = vec!["ACL", "SETUSER", "plumbing", "on", ">pw", "~*", "-@all"];
    let allowed: Vec<String> = PLUMBING
        .iter()
        .map(|name| format!("+{}", name.to_lowercase()))
        .collect();
    rules.extend(allowed.iter().map(String::as_str));
    assert_eq!(admin.must(&rules).text(), "OK");

    let mut conn = login(&address, "plumbing");
    let mut checked = 0;

    for (head, vector) in surface_vectors(&tags) {
        if PLUMBING.contains(&vector[0].as_str()) {
            continue;
        }

        assert_refused_everywhere(&mut conn, &head, &vector, "command-denied user");
        checked += 1;
    }

    assert_eq!(admin.must(&["DBSIZE"]).text(), "0", "a denied write ran");
    assert!(checked > 400, "only {checked} denied frames were checked");
}

#[test]
fn a_key_restricted_user_cannot_reach_other_keys_on_any_entry_path() {
    let server = secured_server();
    let address = server.address.clone();
    let mut admin = Conn::open(&address, Duration::from_secs(5));

    admin.must(&["AUTH", PASSWORD]);
    admin.must(&["ACL", "SETUSER", "keyed", "on", ">pw", "~mine:*", "+@all"]);

    let infos = command_info(&mut admin);
    let tags = tag_per_shard();
    let forbidden: HashSet<String> = tags
        .iter()
        .flat_map(|tag| KEY_KINDS.iter().map(move |kind| key(tag, kind)))
        .collect();

    // Seed the forbidden keys so a bypass would read or change real data.
    for tag in &tags {
        for words in fixture(tag) {
            admin.send_frame(&words_frame(&words)).unwrap();
            admin.read().unwrap();
        }
    }
    let before = admin.must(&["DBSIZE"]).text();

    let mut conn = login(&address, "keyed");
    let mut checked = 0;

    for (head, vector) in surface_vectors(&tags) {
        let Some(info) = info_for(&infos, &head) else {
            continue;
        };
        // Only commands whose first key position holds one of the forbidden keys. Shard
        // channels sit in the key position but are channels, which key patterns do not cover.
        let first = info.first_key;

        if matches!(
            vector[0].as_str(),
            "SPUBLISH" | "SSUBSCRIBE" | "SUNSUBSCRIBE"
        ) {
            continue;
        }

        if first <= 0
            || first as usize >= vector.len()
            || !forbidden.contains(&vector[first as usize])
        {
            continue;
        }

        assert_refused_everywhere(&mut conn, &head, &vector, "key-restricted user");
        checked += 1;
    }

    assert!(
        checked > 150,
        "only {checked} key-restricted frames were checked"
    );
    assert_eq!(
        admin.must(&["DBSIZE"]).text(),
        before,
        "a refused command changed the keyspace"
    );

    // Feeds that report keys of every pattern are refused to a key-restricted user. (KEYS,
    // SCAN and RANDOMKEY take no key arguments and, as in Redis, are not filtered.)
    let mut leaked = Vec::new();

    for words in [
        vec!["CHANGES", "START"],
        vec!["CHANGES", "0"],
        vec!["RUVIO", "HOTKEYS"],
        vec!["RUVIO", "BIGKEYS"],
        vec!["SUBSCRIBE", "__keyspace@0__:{t0}str"],
        vec!["PSUBSCRIBE", "__key*"],
    ] {
        let mut conn = login(&address, "keyed");
        let reply = conn.must(&words);

        if !is_noperm(&reply) {
            leaked.push(format!("{words:?} -> {}", reply.text()));
        }
    }
    assert!(leaked.is_empty(), "key names leaked: {leaked:?}");
}
