//! Typed command wrappers against a scripted one-connection server: no Ruvio server needed.
//!
//! Each test sends commands through the public API and checks two things: the exact request
//! that went on the wire, and how a canned reply (valid, null, or malformed) is decoded.

use std::io::Write;
use std::net::{Shutdown, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod common;

use ruvio_client::__bench::parse;
use ruvio_client::{
    BitFieldOffset, BitFieldOp, BitFieldOverflow, BitFieldType, BitOperation, BloomInsertOptions,
    Client, ClientOptions, ClientType, Error, ExpireCondition, FieldExpireCondition, FillOutcome,
    GeoFrom, GeoOrder, GeoSearch, GeoUnit, IdemState, InfoSection, JsonIndexType, JsonSetCondition,
    RespValue, RestoreOptions, SetMode, SortedSetAddOptions, StreamClaimOptions,
    StreamPendingFilter, StreamReadOptions, StreamTrim, TrimMode,
};

type Requests = Arc<Mutex<Vec<Vec<String>>>>;

/// Starts a server that answers the n-th command with `replies[n]` (the last reply repeats),
/// then returns a connected client and the log of requests.
fn scripted(replies: &[&str]) -> (Client, Requests) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests: Requests = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&requests);
    let replies: Vec<Vec<u8>> = replies.iter().map(|r| r.as_bytes().to_vec()).collect();

    thread::spawn(move || {
        let (socket, _) = common::accept_within_deadline(&listener);
        let mut writer = socket.try_clone().unwrap();
        let mut reader = std::io::BufReader::new(socket);
        let mut index = 0;

        loop {
            let Ok(RespValue::Array(items)) = parse(&mut reader) else {
                return;
            };
            let command = items
                .into_iter()
                .map(|item| {
                    String::from_utf8_lossy(&item.into_bytes().unwrap_or_default()).into_owned()
                })
                .collect();

            log.lock().unwrap().push(command);

            let reply = &replies[index.min(replies.len() - 1)];

            index += 1;

            if writer.write_all(reply).is_err() {
                let _ = writer.shutdown(Shutdown::Both);

                return;
            }
        }
    });

    let mut client = Client::connect_with(ClientOptions {
        port,
        discover_cluster: false,
        ..ClientOptions::default()
    })
    .unwrap();

    // No test may hang on a reply that never comes.
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    (client, requests)
}

/// Runs `call` against a server that always answers `reply`, returns its result and the last request.
fn probe<T>(reply: &str, call: impl FnOnce(&mut Client) -> T) -> (T, Vec<String>) {
    let (mut client, requests) = scripted(&[reply]);
    let result = call(&mut client);
    let last = requests.lock().unwrap().last().cloned().unwrap_or_default();

    (result, last)
}

fn words(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

fn protocol(result: Result<impl std::fmt::Debug, Error>) -> String {
    match result {
        Err(Error::Protocol(message)) => message,
        other => panic!("expected a protocol error, got {other:?}"),
    }
}

fn at(seconds: u64, millis: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_millis(millis)
}

// ---------------------------------------------------------------------------------------------
// Native Ruvio commands
// ---------------------------------------------------------------------------------------------

#[test]
fn native_integer_commands_encode_and_decode() {
    let (reply, request) = probe(":1\r\n", |c| c.once("k", 500));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["ONCE", "k", "500"]));

    let (reply, request) = probe(":1\r\n", |c| {
        c.idempotency_complete("k", "fp", "me", "done")
    });
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(
        request,
        words(&["IDEM", "k", "COMPLETE", "fp", "me", "done"])
    );

    let (reply, request) = probe(":5\r\n", |c| c.changes_start());
    assert_eq!(reply.unwrap(), 5);
    assert_eq!(request, words(&["CHANGES", "START"]));

    let (reply, request) = probe(":3\r\n", |c| c.setv("k", "v", None));
    assert_eq!(reply.unwrap(), 3);
    assert_eq!(request, words(&["SETV", "k", "v"]));
    let (_, request) = probe(":3\r\n", |c| c.setv("k", "v", Some(7)));
    assert_eq!(request, words(&["SETV", "k", "v", "7"]));

    let (reply, request) = probe(":1\r\n", |c| c.release("k", 9));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["RELEASE", "k", "9"]));
}

#[test]
fn integer_commands_reject_other_reply_kinds() {
    let (reply, _) = probe("+OK\r\n", |c| c.once("k", 1));
    assert!(protocol(reply).starts_with("ONCE returned"));
    let (reply, _) = probe("$1\r\n1\r\n", |c| c.setv("k", "v", None));
    assert!(protocol(reply).starts_with("SETV returned"));
}

#[test]
fn idempotency_begin_returns_the_raw_reply() {
    let (reply, request) = probe("*1\r\n$8\r\nacquired\r\n", |c| {
        c.idempotency_begin("k", "fp", "owner", 60_000)
    });

    assert_eq!(
        reply.unwrap(),
        RespValue::Array(vec![RespValue::Bulk(b"acquired".to_vec())])
    );
    assert_eq!(
        request,
        words(&["IDEM", "k", "BEGIN", "fp", "owner", "60000"])
    );
}

#[test]
fn optional_integer_commands_map_null_and_reject_garbage() {
    type Call = fn(&mut Client) -> Result<Option<i64>, Error>;

    let calls: [(&str, Call, &[&str]); 5] = [
        ("TAKE", |c| c.take("k", -3), &["TAKE", "k", "-3"]),
        ("LEASE", |c| c.lease("k", 100), &["LEASE", "k", "100"]),
        (
            "SEMAPHORE",
            |c| c.semaphore("k", 2, 100),
            &["SEMAPHORE", "k", "2", "100"],
        ),
        (
            "INCRBY",
            |c| c.incr_by_max("k", 5, 10),
            &["INCRBY", "k", "5", "MAX", "10"],
        ),
        (
            "OBJECT",
            |c| c.object_idletime("k"),
            &["OBJECT", "IDLETIME", "k"],
        ),
    ];

    for (name, call, expected) in calls {
        let (reply, request) = probe("$-1\r\n", call);
        assert_eq!(reply.unwrap(), None, "{name}");
        assert_eq!(request, words(expected));
        let (reply, _) = probe(":4\r\n", call);
        assert_eq!(reply.unwrap(), Some(4), "{name}");
        let (reply, _) = probe("+nope\r\n", call);
        assert!(protocol(reply).contains(&format!("{name} returned")));
    }

    let (reply, request) = probe(":2\r\n", |c| c.object_freq("k"));
    assert_eq!(reply.unwrap(), Some(2));
    assert_eq!(request, words(&["OBJECT", "FREQ", "k"]));
    let (reply, _) = probe("$-1\r\n", |c| c.memory_usage("k"));
    assert_eq!(reply.unwrap(), None);
}

#[test]
fn transfer_commands_send_the_cap_and_decode_the_pair() {
    let (reply, request) = probe("*2\r\n:6\r\n:4\r\n", |c| c.transfer("a", "b", 4, None));
    assert_eq!(reply.unwrap(), Some((6, 4)));
    assert_eq!(request, words(&["TRANSFER", "a", "b", "4"]));
    let (reply, request) = probe("*2\r\n:6\r\n:4\r\n", |c| c.transfer("a", "b", 4, Some(9)));
    assert_eq!(reply.unwrap(), Some((6, 4)));
    assert_eq!(request, words(&["TRANSFER", "a", "b", "4", "MAX", "9"]));
    let (reply, _) = probe("$-1\r\n", |c| c.transfer("a", "b", 4, None));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("+nope\r\n", |c| c.transfer("a", "b", 4, None));
    assert!(protocol(reply).contains("TRANSFER returned"));
    let (reply, _) = probe("*1\r\n:1\r\n", |c| c.transfer("a", "b", 4, None));
    assert!(protocol(reply).contains("TRANSFER returned"));

    let (reply, request) = probe("*2\r\n:1\r\n:2\r\n", |c| {
        c.htransfer("h", "f", "g", 1, Some(5))
    });
    assert_eq!(reply.unwrap(), Some((1, 2)));
    assert_eq!(
        request,
        words(&["HTRANSFER", "h", "f", "g", "1", "MAX", "5"])
    );
    let (reply, _) = probe("$-1\r\n", |c| c.htransfer("h", "f", "g", 1, None));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe(":1\r\n", |c| c.htransfer("h", "f", "g", 1, None));
    assert!(protocol(reply).contains("HTRANSFER returned"));
}

#[test]
fn getex_returns_the_string_or_none() {
    let (reply, request) = probe("$1\r\nv\r\n", |c| c.getex("k", 100));
    assert_eq!(reply.unwrap().as_deref(), Some("v"));
    assert_eq!(request, words(&["GETEX", "k", "100"]));
    let (reply, _) = probe("$-1\r\n", |c| c.getex("k", 100));
    assert_eq!(reply.unwrap(), None);
}

#[test]
fn changes_decodes_notes_and_rejects_malformed_rows() {
    let (reply, request) = probe("*2\r\n*2\r\n:1\r\n$1\r\na\r\n*1\r\n:2\r\n", |c| {
        c.changes(3)
    });
    assert_eq!(
        reply.unwrap(),
        vec![(1, "a".to_owned()), (2, String::new())]
    );
    assert_eq!(request, words(&["CHANGES", "3"]));

    let (reply, _) = probe("*0\r\n", |c| c.changes(0));
    assert!(reply.unwrap().is_empty());
    let (reply, _) = probe(":1\r\n", |c| c.changes(0));
    assert!(protocol(reply).starts_with("CHANGES returned"));
    let (reply, _) = probe("*1\r\n:1\r\n", |c| c.changes(0));
    assert!(protocol(reply).starts_with("CHANGES returned"));
    let (reply, _) = probe("*1\r\n*1\r\n$1\r\na\r\n", |c| c.changes(0));
    assert!(protocol(reply).starts_with("CHANGES returned"));
}

// ---------------------------------------------------------------------------------------------
// Keys, lists, sets
// ---------------------------------------------------------------------------------------------

#[test]
fn key_commands_choose_seconds_or_milliseconds() {
    let (reply, request) = probe(":1\r\n", |c| c.expire_at("k", at(1_000, 0)));
    assert!(reply.unwrap());
    assert_eq!(request, words(&["EXPIREAT", "k", "1000"]));
    let (_, request) = probe(":1\r\n", |c| c.expire_at("k", at(1_000, 500)));
    assert_eq!(request, words(&["PEXPIREAT", "k", "1000500"]));
    let (reply, _) = probe(":1\r\n", |c| {
        c.expire_at("k", UNIX_EPOCH - Duration::from_secs(1))
    });
    assert_eq!(protocol(reply), "time is before the Unix epoch");

    let (reply, request) = probe(":1\r\n", |c| c.expire("k", Duration::from_secs(5)));
    assert!(reply.unwrap());
    assert_eq!(request, words(&["EXPIRE", "k", "5"]));
    let (_, request) = probe(":0\r\n", |c| c.expire("k", Duration::from_nanos(1)));
    assert_eq!(request, words(&["PEXPIRE", "k", "1"]));
    let (_, request) = probe(":0\r\n", |c| c.expire("k", Duration::from_micros(1_500)));
    assert_eq!(request, words(&["PEXPIRE", "k", "2"]));

    let (reply, request) = probe(":7\r\n", |c| c.pexpire_time("k"));
    assert_eq!(reply.unwrap(), 7);
    assert_eq!(request, words(&["PEXPIRETIME", "k"]));
    let (reply, request) = probe(":1\r\n", |c| c.copy("a", "b", true));
    assert!(reply.unwrap());
    assert_eq!(request, words(&["COPY", "a", "b", "REPLACE"]));
    let (_, request) = probe(":0\r\n", |c| c.copy("a", "b", false));
    assert_eq!(request, words(&["COPY", "a", "b"]));
}

#[test]
fn delete_commands() {
    let (reply, request) = probe(":1\r\n", |c| c.unlink_key("k"));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["UNLINK", "k"]));
    let (reply, request) = probe(":1\r\n", |c| c.del_key("k"));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["DEL", "k"]));
    let (reply, _) = probe(":1\r\n", |c| c.del(&[]));
    assert_eq!(protocol(reply), "DEL needs at least one key");
}

#[test]
fn set_variants_build_the_right_arguments() {
    let (reply, request) = probe("+OK\r\n", |c| c.set("k", "v"));
    assert!(reply.unwrap());
    assert_eq!(request, words(&["SET", "k", "v"]));
    let (reply, _) = probe("$-1\r\n", |c| c.set("k", "v"));
    assert!(!reply.unwrap());

    let (_, request) = probe("+OK\r\n", |c| {
        c.set_with(
            "k",
            "v",
            Some(Duration::from_millis(1500)),
            SetMode::IfNotExists,
        )
    });
    assert_eq!(request, words(&["SET", "k", "v", "PX", "1500", "NX"]));
    let (_, request) = probe("+OK\r\n", |c| c.set_with("k", "v", None, SetMode::IfExists));
    assert_eq!(request, words(&["SET", "k", "v", "XX"]));
    let (_, request) = probe("+OK\r\n", |c| {
        c.set_expires_at("k", "v", at(2, 5), SetMode::Always)
    });
    assert_eq!(request, words(&["SET", "k", "v", "PXAT", "2005"]));
    let (reply, _) = probe("+OK\r\n", |c| {
        c.set_expires_at(
            "k",
            "v",
            UNIX_EPOCH - Duration::from_secs(1),
            SetMode::Always,
        )
    });
    assert!(matches!(reply, Err(Error::Protocol(_))));
    let (_, request) = probe("+OK\r\n", |c| c.set_keep_ttl("k", "v", SetMode::IfExists));
    assert_eq!(request, words(&["SET", "k", "v", "KEEPTTL", "XX"]));
    let (_, request) = probe("+OK\r\n", |c| c.set_ex("k", "v", 9));
    assert_eq!(request, words(&["SET", "k", "v", "EX", "9"]));
    let (_, request) = probe("+OK\r\n", |c| {
        c.set_exat("k", "v", 99, SetMode::IfNotExists)
    });
    assert_eq!(request, words(&["SET", "k", "v", "EXAT", "99", "NX"]));

    let (reply, request) = probe("$3\r\nold\r\n", |c| {
        c.set_and_get("k", "v", None, SetMode::Always)
    });
    assert_eq!(reply.unwrap().as_deref(), Some("old"));
    assert_eq!(request, words(&["SET", "k", "v", "GET"]));
    let (_, request) = probe("$-1\r\n", |c| {
        c.set_and_get("k", "v", Some(Duration::from_secs(2)), SetMode::IfNotExists)
    });
    assert_eq!(
        request,
        words(&["SET", "k", "v", "PX", "2000", "NX", "GET"])
    );
    let (_, request) = probe("$-1\r\n", |c| {
        c.set_and_get("k", "v", None, SetMode::IfExists)
    });
    assert_eq!(request, words(&["SET", "k", "v", "XX", "GET"]));

    let (reply, request) = probe("+OK\r\n", |c| {
        c.set_if_eq("k", "v", "old", Some(Duration::from_millis(5)))
    });
    assert!(reply.unwrap());
    assert_eq!(request, words(&["SET", "k", "v", "IFEQ", "old", "PX", "5"]));
    let (reply, request) = probe("$-1\r\n", |c| c.set_if_eq("k", "v", "old", None));
    assert!(!reply.unwrap());
    assert_eq!(request, words(&["SET", "k", "v", "IFEQ", "old"]));
    let (_, request) = probe("+OK\r\n", |c| c.set_if_eq_keep_ttl("k", "v", "old"));
    assert_eq!(request, words(&["SET", "k", "v", "IFEQ", "old", "KEEPTTL"]));
    let (reply, request) = probe("+OK\r\n", |c| c.set_if_greater("k", -7, None));
    assert!(reply.unwrap());
    assert_eq!(request, words(&["SET", "k", "-7", "IFGT"]));
    let (reply, request) = probe("$-1\r\n", |c| {
        c.set_if_less("k", 9, Some(Duration::from_millis(5)))
    });
    assert!(!reply.unwrap());
    assert_eq!(request, words(&["SET", "k", "9", "IFLT", "PX", "5"]));
}

#[test]
fn get_and_set_bytes_handle_missing_and_odd_replies() {
    let (reply, _) = probe("$-1\r\n", |c| c.get("k"));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("$2\r\nhi\r\n", |c| c.get("k"));
    assert_eq!(reply.unwrap().unwrap(), b"hi");
    let (reply, _) = probe(":1\r\n", |c| c.get("k"));
    assert!(protocol(reply).starts_with("GET returned"));
    let (reply, _) = probe("$-1\r\n", |c| c.get_string("k"));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("$2\r\nhi\r\n", |c| c.get_string("k"));
    assert_eq!(reply.unwrap().as_deref(), Some("hi"));

    let (reply, request) = probe("+OK\r\n", |c| c.set_bytes("k", b"\xff\x00", None));
    reply.unwrap();
    assert_eq!(request[0], "SET");
    let (reply, request) = probe("+OK\r\n", |c| {
        c.set_bytes("k", b"v", Some(Duration::from_secs(1)))
    });
    reply.unwrap();
    assert_eq!(request, words(&["SET", "k", "v", "PX", "1000"]));

    let (reply, request) = probe(":4\r\n", |c| c.incr("n"));
    assert_eq!(reply.unwrap(), 4);
    assert_eq!(request, words(&["INCR", "n"]));
}

#[test]
fn list_and_set_wrappers() {
    let (reply, request) = probe("$-1\r\n", |c| c.rpop("l"));
    assert_eq!(reply.unwrap(), None);
    assert_eq!(request, words(&["RPOP", "l"]));
    let (reply, request) = probe("$1\r\nx\r\n", |c| c.srandmember("s"));
    assert_eq!(reply.unwrap().as_deref(), Some("x"));
    assert_eq!(request, words(&["SRANDMEMBER", "s"]));
    let (reply, request) = probe(":3\r\n", |c| c.sunionstore("d", &["a", "b"]));
    assert_eq!(reply.unwrap(), 3);
    assert_eq!(request, words(&["SUNIONSTORE", "d", "a", "b"]));
    let (reply, request) = probe(":1\r\n", |c| c.sdiffstore("d", &["a"]));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["SDIFFSTORE", "d", "a"]));
    let (_, request) = probe("*0\r\n", |c| c.zrange_by_lex("z", "-", "+"));
    assert_eq!(request, words(&["ZRANGE", "z", "-", "+", "BYLEX"]));
}

#[test]
fn blocking_pops_decode_pairs_and_timeouts() {
    let pair = "*2\r\n$1\r\nk\r\n$1\r\nv\r\n";

    let (reply, request) = probe(pair, |c| c.brpop_key("k", Duration::from_millis(1500)));
    assert_eq!(reply.unwrap(), Some(("k".to_owned(), "v".to_owned())));
    assert_eq!(request, words(&["BRPOP", "k", "2"]));
    let (reply, request) = probe(pair, |c| c.brpop(&["a", "b"], Duration::from_secs(3)));
    assert_eq!(reply.unwrap(), Some(("k".to_owned(), "v".to_owned())));
    assert_eq!(request, words(&["BRPOP", "a", "b", "3"]));
    let (reply, _) = probe("*-1\r\n", |c| c.brpop_key("k", Duration::ZERO));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("*3\r\n:1\r\n:2\r\n:3\r\n", |c| {
        c.brpop_key("k", Duration::ZERO)
    });
    assert!(protocol(reply).starts_with("BRPOP returned"));
    let (reply, _) = probe(":1\r\n", |c| c.blpop_key("k", Duration::ZERO));
    assert!(protocol(reply).starts_with("BLPOP returned"));

    let (reply, request) = probe("*2\r\n$1\r\nm\r\n$1\r\n2\r\n", |c| {
        c.zpopbyscore("z", "-inf", "(5", 3)
    });
    assert_eq!(
        reply.unwrap(),
        vec![ruvio_client::SortedSetEntry::new("m", 2.0)]
    );
    assert_eq!(
        request,
        words(&[
            "ZPOPBYSCORE",
            "z",
            "(5",
            "MIN",
            "-inf",
            "COUNT",
            "3",
            "WITHSCORES"
        ])
    );
    let (reply, request) = probe(":2\r\n", |c| c.zmovedue("{t}z", "{t}l", "+inf", 4));
    assert_eq!(reply.unwrap(), 2);
    assert_eq!(
        request,
        words(&["ZMOVEDUE", "{t}z", "{t}l", "+inf", "COUNT", "4"])
    );

    let triple = "*3\r\n$1\r\nz\r\n$1\r\nm\r\n$3\r\n1.5\r\n";
    let (reply, request) = probe(triple, |c| c.bzpopmin(&["z"], Duration::from_secs(1)));
    let (key, entry) = reply.unwrap().unwrap();
    assert_eq!(
        (key.as_str(), entry.member.as_str(), entry.score),
        ("z", "m", 1.5)
    );
    assert_eq!(request, words(&["BZPOPMIN", "z", "1"]));
    let (reply, _) = probe("$-1\r\n", |c| c.bzpopmax(&["z"], Duration::from_secs(1)));
    assert_eq!(reply.unwrap().map(|(k, _)| k), None);
    let (reply, _) = probe("*3\r\n$1\r\nz\r\n$1\r\nm\r\n$3\r\nabc\r\n", |c| {
        c.bzpopmax(&["z"], Duration::from_secs(1))
    });
    assert!(protocol(reply).contains("is not a score"));
    let (reply, _) = probe("*1\r\n:1\r\n", |c| {
        c.bzpopmin(&["z"], Duration::from_secs(1))
    });
    assert!(protocol(reply).starts_with("BZPOPMIN returned"));
}

// ---------------------------------------------------------------------------------------------
// Hash field expiry, geo, bitmaps
// ---------------------------------------------------------------------------------------------

#[test]
fn hash_field_expiry_variants() {
    let (reply, request) = probe("*2\r\n:1\r\n:1\r\n", |c| {
        c.hexpire_with(
            "h",
            Duration::from_secs(5),
            FieldExpireCondition::IfNoTtl,
            &["a", "b"],
        )
    });
    assert_eq!(reply.unwrap(), [1, 1]);
    assert_eq!(
        request,
        words(&["HEXPIRE", "h", "5", "NX", "FIELDS", "2", "a", "b"])
    );
    let (_, request) = probe("*1\r\n:1\r\n", |c| {
        c.hexpire_with(
            "h",
            Duration::from_millis(1500),
            FieldExpireCondition::IfLess,
            &["a"],
        )
    });
    assert_eq!(
        request,
        words(&["HPEXPIRE", "h", "1500", "LT", "FIELDS", "1", "a"])
    );
    let (_, request) = probe("*1\r\n:1\r\n", |c| c.hexpire_at("h", at(60, 0), &["a"]));
    assert_eq!(
        request,
        words(&["HEXPIREAT", "h", "60", "FIELDS", "1", "a"])
    );
    let (_, request) = probe("*1\r\n:1\r\n", |c| c.hexpire_at("h", at(60, 250), &["a"]));
    assert_eq!(
        request,
        words(&["HPEXPIREAT", "h", "60250", "FIELDS", "1", "a"])
    );
    let (reply, _) = probe("*1\r\n:1\r\n", |c| {
        c.hexpire_at("h", UNIX_EPOCH - Duration::from_secs(1), &["a"])
    });
    assert!(matches!(reply, Err(Error::Protocol(_))));
    // Replies must be an array of integers.
    let (reply, _) = probe(":1\r\n", |c| c.hexpire("h", Duration::from_secs(1), &["a"]));
    assert!(protocol(reply).starts_with("HEXPIRE returned"));
    let (reply, _) = probe("*1\r\n$1\r\n1\r\n", |c| {
        c.hexpire("h", Duration::from_secs(1), &["a"])
    });
    assert!(protocol(reply).starts_with("HEXPIRE returned"));
}

#[test]
fn geo_pos_decodes_points_and_nulls() {
    let reply =
        "*3\r\n*2\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$-1\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n";
    let (result, request) = probe(reply, |c| c.geo_pos("g", &["a", "b", "c"]));

    assert_eq!(
        result.unwrap(),
        vec![Some((13.361389, 38.115556)), None, Some((1.0, 2.0))]
    );
    assert_eq!(request, words(&["GEOPOS", "g", "a", "b", "c"]));

    let (result, _) = probe(":1\r\n", |c| c.geo_pos("g", &["a"]));
    assert!(protocol(result).starts_with("GEOPOS returned"));
    let (result, _) = probe("*1\r\n:1\r\n", |c| c.geo_pos("g", &["a"]));
    assert!(protocol(result).starts_with("GEOPOS returned"));
    let (result, _) = probe("*1\r\n*1\r\n$1\r\n1\r\n", |c| c.geo_pos("g", &["a"]));
    assert!(protocol(result).starts_with("GEOPOS returned"));
    let (result, _) = probe("*1\r\n*2\r\n$1\r\nx\r\n$1\r\n1\r\n", |c| {
        c.geo_pos("g", &["a"])
    });
    assert!(protocol(result).contains("is not a score"));
}

#[test]
fn geo_add_and_search_build_every_option() {
    let (reply, request) = probe(":2\r\n", |c| {
        c.geo_add_with(
            "g",
            SortedSetAddOptions {
                if_not_exists: true,
                if_exists: true,
                changed: true,
                ..Default::default()
            },
            &[(1.5, 2.5, "a"), (3.0, 4.0, "b")],
        )
    });
    assert_eq!(reply.unwrap(), 2);
    assert_eq!(
        request,
        words(&[
            "GEOADD", "g", "NX", "XX", "CH", "1.5", "2.5", "a", "3.0", "4.0", "b"
        ])
    );

    let mut search = GeoSearch::radius(GeoFrom::Member("a"), 10.0, GeoUnit::Kilometers);
    search.count = Some(3);
    search.any = true;
    search.order = Some(GeoOrder::Desc);
    search.with_distance = true;
    search.with_hash = true;
    search.with_coordinates = true;

    let reply =
        "*2\r\n*4\r\n$1\r\na\r\n$3\r\n1.5\r\n:99\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\nb\r\n";
    let (matches, request) = probe(reply, |c| c.geo_search_with("g", &search));
    let matches = matches.unwrap();

    assert_eq!(
        request,
        words(&[
            "GEOSEARCH",
            "g",
            "FROMMEMBER",
            "a",
            "BYRADIUS",
            "10.0",
            "km",
            "DESC",
            "COUNT",
            "3",
            "ANY",
            "WITHDIST",
            "WITHHASH",
            "WITHCOORD"
        ])
    );
    assert_eq!(matches[0].member, "a");
    assert_eq!(matches[0].distance, Some(1.5));
    assert_eq!(matches[0].hash, Some(99));
    assert_eq!(matches[0].coordinates, Some((1.0, 2.0)));
    // A bare member row (no WITH* data) is a plain bulk.
    assert_eq!(matches[1].member, "b");
    assert_eq!(matches[1].distance, None);

    let mut by_box = GeoSearch::radius(GeoFrom::LonLat(1.0, 2.0), 0.0, GeoUnit::Meters);
    by_box.shape = ruvio_client::GeoShape::Box(3.0, 4.0);
    by_box.order = Some(GeoOrder::Asc);
    by_box.count = Some(1);
    let (_, request) = probe("*0\r\n", |c| c.geo_search_with("g", &by_box));
    assert_eq!(
        request,
        words(&[
            "GEOSEARCH",
            "g",
            "FROMLONLAT",
            "1.0",
            "2.0",
            "BYBOX",
            "3.0",
            "4.0",
            "m",
            "ASC",
            "COUNT",
            "1"
        ])
    );
    by_box.order = None;
    by_box.count = None;
    let (_, request) = probe("*0\r\n", |c| c.geo_search_with("g", &by_box));
    assert_eq!(request.len(), 9);

    let (result, _) = probe(":1\r\n", |c| c.geo_search_with("g", &by_box));
    assert!(protocol(result).starts_with("GEOSEARCH returned"));
    // A non-numeric distance is reported, and a coordinate pair of the wrong size is ignored.
    search.with_hash = false;
    let (result, _) = probe("*1\r\n*3\r\n$1\r\na\r\n$1\r\nx\r\n*0\r\n", |c| {
        c.geo_search_with("g", &search)
    });
    assert!(protocol(result).contains("is not a score"));
    let (result, _) = probe("*1\r\n*3\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$1\r\n1\r\n", |c| {
        c.geo_search_with("g", &search)
    });
    assert_eq!(result.unwrap()[0].coordinates, None);
}

#[test]
fn bit_commands() {
    for (operation, word) in [
        (BitOperation::And, "AND"),
        (BitOperation::Or, "OR"),
        (BitOperation::Xor, "XOR"),
        (BitOperation::Not, "NOT"),
    ] {
        let (reply, request) = probe(":4\r\n", |c| c.bitop(operation, "d", &["a", "b"]));
        assert_eq!(reply.unwrap(), 4);
        assert_eq!(request, words(&["BITOP", word, "d", "a", "b"]));
    }

    let (reply, request) = probe("*3\r\n:1\r\n$-1\r\n:3\r\n", |c| {
        c.bitfield(
            "k",
            &[BitFieldOp::Get {
                ty: BitFieldType::unsigned(8).unwrap(),
                offset: BitFieldOffset::Bit(0),
            }],
        )
    });
    assert_eq!(reply.unwrap(), vec![Some(1), None, Some(3)]);
    assert_eq!(request, words(&["BITFIELD", "k", "GET", "u8", "0"]));
    let (reply, request) = probe("*1\r\n:1\r\n", |c| {
        c.bitfield_ro(
            "k",
            &[BitFieldOp::Get {
                ty: BitFieldType::unsigned(8).unwrap(),
                offset: BitFieldOffset::Bit(0),
            }],
        )
    });
    assert_eq!(reply.unwrap(), vec![Some(1)]);
    assert_eq!(request[0], "BITFIELD_RO");
    let (reply, request) = probe("*4\r\n:1\r\n:2\r\n:3\r\n:4\r\n", |c| {
        let i16_type = BitFieldType::signed(16).unwrap();
        let u63_type = BitFieldType::unsigned(63).unwrap();

        c.bitfield(
            "k",
            &[
                BitFieldOp::Overflow(BitFieldOverflow::Sat),
                BitFieldOp::Set {
                    ty: i16_type,
                    offset: BitFieldOffset::Field(2),
                    value: -5,
                },
                BitFieldOp::IncrBy {
                    ty: u63_type,
                    offset: BitFieldOffset::Bit(100),
                    delta: 7,
                },
                BitFieldOp::Get {
                    ty: i16_type,
                    offset: BitFieldOffset::Field(2),
                },
            ],
        )
    });
    assert_eq!(reply.unwrap().len(), 4);
    assert_eq!(
        request,
        words(&[
            "BITFIELD", "k", "OVERFLOW", "SAT", "SET", "i16", "#2", "-5", "INCRBY", "u63", "100",
            "7", "GET", "i16", "#2"
        ])
    );
    let (reply, _) = probe(":1\r\n", |c| c.bitfield("k", &[]));
    assert!(protocol(reply).starts_with("BITFIELD returned"));
    let (reply, _) = probe("*1\r\n$1\r\nx\r\n", |c| c.bitfield("k", &[]));
    assert!(protocol(reply).starts_with("BITFIELD returned"));
}

// ---------------------------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------------------------

#[test]
fn stream_adds_and_info() {
    let (reply, request) = probe("$3\r\n1-0\r\n", |c| {
        c.xadd_delay("s", 500, "*", &[("f", "v")])
    });
    assert_eq!(reply.unwrap(), "1-0");
    assert_eq!(
        request,
        words(&["XADD", "s", "DELAY", "500", "*", "f", "v"])
    );
    let (reply, request) = probe("$-1\r\n", |c| c.xadd_nomkstream("s", "*", &[("f", "v")]));
    assert_eq!(reply.unwrap(), None);
    assert_eq!(request, words(&["XADD", "s", "NOMKSTREAM", "*", "f", "v"]));
    let (reply, _) = probe("$3\r\n1-0\r\n", |c| {
        c.xadd_nomkstream("s", "*", &[("f", "v")])
    });
    assert_eq!(reply.unwrap().as_deref(), Some("1-0"));

    for (call, expected) in [
        (
            (|c: &mut Client| c.xinfo_stream("s")) as fn(&mut Client) -> Result<RespValue, Error>,
            words(&["XINFO", "STREAM", "s"]),
        ),
        (|c| c.xinfo_groups("s"), words(&["XINFO", "GROUPS", "s"])),
        (
            |c| c.xinfo_consumers("s", "g"),
            words(&["XINFO", "CONSUMERS", "s", "g"]),
        ),
        (
            |c| c.xautoclaim("s", "g", "c", 1000, "0-0"),
            words(&["XAUTOCLAIM", "s", "g", "c", "1000", "0-0"]),
        ),
    ] {
        let (reply, request) = probe("*0\r\n", call);
        assert_eq!(reply.unwrap(), RespValue::Array(vec![]));
        assert_eq!(request, expected);
    }

    let (reply, request) = probe("+OK\r\n", |c| c.xsetid("s", "9-9"));
    reply.unwrap();
    assert_eq!(request, words(&["XSETID", "s", "9-9"]));
    let (reply, request) = probe(":2\r\n", |c| c.xtrim_minid("s", "5-0"));
    assert_eq!(reply.unwrap(), 2);
    assert_eq!(request, words(&["XTRIM", "s", "MINID", "5-0"]));
    let (reply, request) = probe(":2\r\n", |c| c.xtrim("s", &StreamTrim::min_id("5-0")));
    assert_eq!(reply.unwrap(), 2);
    assert_eq!(request, words(&["XTRIM", "s", "MINID", "=", "5-0"]));
    let trim = StreamTrim::max_len(7).mode(TrimMode::Approximate);
    let (reply, request) = probe(":0\r\n", |c| c.xtrim("s", &trim));
    assert_eq!(reply.unwrap(), 0);
    assert_eq!(request, words(&["XTRIM", "s", "MAXLEN", "~", "7"]));
    let (reply, request) = probe("$3\r\n1-1\r\n", |c| {
        c.xadd_trim("s", &trim, "*", &[("f", "v")])
    });
    assert_eq!(reply.unwrap(), "1-1");
    assert_eq!(
        request,
        words(&["XADD", "s", "MAXLEN", "~", "7", "*", "f", "v"])
    );
    let (reply, request) = probe("$3\r\n1-1\r\n", |c| {
        c.xadd_trim("s", &StreamTrim::min_id("3-0"), "*", &[("f", "v")])
    });
    reply.unwrap();
    assert_eq!(
        request,
        words(&["XADD", "s", "MINID", "=", "3-0", "*", "f", "v"])
    );
    let (reply, request) = probe("$0\r\n\r\n", |c| {
        c.info_section(InfoSection::CommandCounters)
    });
    reply.unwrap();
    assert_eq!(request, words(&["INFO", "commandcounters"]));
    let (reply, request) = probe("$0\r\n\r\n", |c| c.client_list_type(ClientType::Replica));
    reply.unwrap();
    assert_eq!(request, words(&["CLIENT", "LIST", "TYPE", "replica"]));
    let (reply, request) = probe(":1\r\n", |c| c.xgroup_create_consumer("s", "g", "c"));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["XGROUP", "CREATECONSUMER", "s", "g", "c"]));
}

#[test]
fn xrange_decodes_entries_and_skips_malformed_ones() {
    let reply = "*3\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n:7\r\n*1\r\n$3\r\n2-0\r\n";
    let (entries, request) = probe(reply, |c| c.xrange("s", "-", "+", Some(5)));
    let entries = entries.unwrap();

    assert_eq!(request, words(&["XRANGE", "s", "-", "+", "COUNT", "5"]));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "1-0");
    assert_eq!(entries[0].fields, [("f".to_owned(), "v".to_owned())]);

    let (entries, request) = probe("*-1\r\n", |c| c.xrange("s", "-", "+", None));
    assert!(entries.unwrap().is_empty());
    assert_eq!(request, words(&["XRANGE", "s", "-", "+"]));
}

#[test]
fn xread_and_xreadgroup_options() {
    let reply = "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let options = StreamReadOptions {
        count: Some(2),
        block: Some(Duration::from_millis(250)),
        no_ack: false,
    };
    let (results, request) = probe(reply, |c| c.xread(&options, &[("s", "0"), ("t", "$")]));
    let results = results.unwrap();

    assert_eq!(
        request,
        words(&[
            "XREAD", "COUNT", "2", "BLOCK", "250", "STREAMS", "s", "t", "0", "$"
        ])
    );
    assert_eq!(results[0].key, "s");
    assert_eq!(results[0].entries[0].id, "1-0");

    let (results, _) = probe("*-1\r\n", |c| {
        c.xread(&StreamReadOptions::default(), &[("s", "0")])
    });
    assert!(results.unwrap().is_empty());
    // A malformed stream block is skipped.
    let (results, _) = probe("*1\r\n:1\r\n", |c| {
        c.xread(&StreamReadOptions::default(), &[("s", "0")])
    });
    assert!(results.unwrap().is_empty());

    let noack = StreamReadOptions {
        no_ack: true,
        ..options
    };
    let (result, request) = probe("*0\r\n", |c| c.xread(&noack, &[("s", "0")]));
    assert_eq!(protocol(result), "NOACK applies only to XREADGROUP");
    assert!(request.is_empty(), "nothing is sent for a refused XREAD");

    let (results, request) = probe(reply, |c| c.xreadgroup("g", "c", &noack, &[("s", ">")]));
    assert_eq!(results.unwrap().len(), 1);
    assert_eq!(
        request,
        words(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "2",
            "BLOCK",
            "250",
            "NOACK",
            "STREAMS",
            "s",
            ">"
        ])
    );
    let (_, request) = probe("*0\r\n", |c| {
        c.xreadgroup("g", "c", &StreamReadOptions::default(), &[("s", ">")])
    });
    assert_eq!(
        request,
        words(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"])
    );
}

#[test]
fn xpending_decodes_rows_and_rejects_bad_ones() {
    let reply = "*1\r\n*4\r\n$3\r\n1-0\r\n$1\r\nc\r\n:1500\r\n:2\r\n";
    let filter = StreamPendingFilter {
        consumer: Some("c".to_owned()),
        min_idle: Some(Duration::from_millis(100)),
    };
    let (rows, request) = probe(reply, |c| c.xpending("s", "g", "-", "+", 10, &filter));
    let rows = rows.unwrap();

    assert_eq!(
        request,
        words(&["XPENDING", "s", "g", "IDLE", "100", "-", "+", "10", "c"])
    );
    assert_eq!(
        (
            rows[0].id.as_str(),
            rows[0].consumer.as_str(),
            rows[0].idle_millis,
            rows[0].delivery_count
        ),
        ("1-0", "c", 1500, 2)
    );

    let (rows, request) = probe("*0\r\n", |c| {
        c.xpending("s", "g", "-", "+", 10, &StreamPendingFilter::default())
    });
    assert!(rows.unwrap().is_empty());
    assert_eq!(request, words(&["XPENDING", "s", "g", "-", "+", "10"]));
    let (rows, _) = probe("*1\r\n*1\r\n:1\r\n", |c| {
        c.xpending("s", "g", "-", "+", 10, &StreamPendingFilter::default())
    });
    assert!(protocol(rows).starts_with("XPENDING returned"));
}

#[test]
fn xclaim_rejects_idle_with_time_and_sends_every_option() {
    let both = StreamClaimOptions {
        idle: Some(Duration::from_secs(1)),
        time: Some(at(5, 0)),
        ..Default::default()
    };
    let (result, request) = probe("*0\r\n", |c| {
        c.xclaim("s", "g", "c", Duration::ZERO, &["1-0"], &both)
    });
    assert_eq!(protocol(result), "IDLE and TIME cannot be combined");
    assert!(request.is_empty());

    let all = StreamClaimOptions {
        idle: Some(Duration::from_millis(30)),
        time: None,
        retry_count: Some(4),
        force: true,
        last_id: Some("9-0".to_owned()),
    };
    let (ids, request) = probe("*1\r\n$3\r\n1-0\r\n", |c| {
        c.xclaim_ids(
            "s",
            "g",
            "c",
            Duration::from_millis(10),
            &["1-0", "2-0"],
            &all,
        )
    });
    assert_eq!(ids.unwrap(), ["1-0"]);
    assert_eq!(
        request,
        words(&[
            "XCLAIM",
            "s",
            "g",
            "c",
            "10",
            "1-0",
            "2-0",
            "IDLE",
            "30",
            "RETRYCOUNT",
            "4",
            "FORCE",
            "LASTID",
            "9-0",
            "JUSTID"
        ])
    );

    let timed = StreamClaimOptions {
        time: Some(at(5, 7)),
        ..Default::default()
    };
    let (entries, request) = probe("*0\r\n", |c| {
        c.xclaim("s", "g", "c", Duration::ZERO, &["1-0"], &timed)
    });
    assert!(entries.unwrap().is_empty());
    assert_eq!(
        request,
        words(&["XCLAIM", "s", "g", "c", "0", "1-0", "TIME", "5007"])
    );
    let before_epoch = StreamClaimOptions {
        time: Some(UNIX_EPOCH - Duration::from_secs(1)),
        ..Default::default()
    };
    let (result, _) = probe("*0\r\n", |c| {
        c.xclaim("s", "g", "c", Duration::ZERO, &["1-0"], &before_epoch)
    });
    assert!(matches!(result, Err(Error::Protocol(_))));
}

// ---------------------------------------------------------------------------------------------
// Server, admin and cluster commands
// ---------------------------------------------------------------------------------------------

#[test]
fn simple_admin_commands_send_their_words() {
    type Call = fn(&mut Client) -> Result<(), Error>;

    let calls: [(Call, &[&str]); 11] = [
        (|c| c.save(), &["SAVE"]),
        (|c| c.bgsave(), &["BGSAVE"]),
        (|c| c.flushall(), &["FLUSHALL"]),
        (|c| c.swapdb(1, 2), &["SWAPDB", "1", "2"]),
        (|c| c.watch_key("k"), &["WATCH", "k"]),
        (|c| c.script_kill(), &["SCRIPT", "KILL"]),
        (
            |c| c.acl_setuser("bob", &["on", ">pw"]),
            &["ACL", "SETUSER", "bob", "on", ">pw"],
        ),
        (|c| c.acl_load(), &["ACL", "LOAD"]),
        (|c| c.acl_save(), &["ACL", "SAVE"]),
        (|c| c.client_tracking(true), &["CLIENT", "TRACKING", "ON"]),
        (|c| c.client_tracking(false), &["CLIENT", "TRACKING", "OFF"]),
    ];

    for (call, expected) in calls {
        let (reply, request) = probe("+OK\r\n", call);
        reply.unwrap();
        assert_eq!(request, words(expected));
        let (reply, _) = probe("-ERR denied\r\n", call);
        assert!(matches!(reply, Err(Error::Server(m)) if m == "ERR denied"));
    }
}

#[test]
fn client_durability_sends_replicas_and_optional_timeout() {
    let (reply, request) = probe("+OK\r\n", |c| c.client_durability(2, None));
    reply.unwrap();
    assert_eq!(request, words(&["CLIENT", "DURABILITY", "REPLICAS", "2"]));

    let (reply, request) = probe("+OK\r\n", |c| c.client_durability(1, Some(250)));
    reply.unwrap();
    assert_eq!(
        request,
        words(&["CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"])
    );

    let (reply, _) = probe("-ERR denied\r\n", |c| c.client_durability(1, None));
    assert!(matches!(reply, Err(Error::Server(m)) if m == "ERR denied"));
}

#[test]
fn exec_decodes_results_aborts_and_garbage() {
    let (reply, request) = probe("*2\r\n+OK\r\n:1\r\n", |c| c.exec());
    assert_eq!(
        reply.unwrap(),
        Some(vec![RespValue::Simple("OK".into()), RespValue::Integer(1)])
    );
    assert_eq!(request, words(&["EXEC"]));
    let (reply, _) = probe("*-1\r\n", |c| c.exec());
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe(":1\r\n", |c| c.exec());
    assert!(protocol(reply).starts_with("EXEC returned"));
}

#[test]
fn cluster_and_client_info_commands() {
    let (reply, request) = probe("*0\r\n", |c| c.cluster_shards());
    assert_eq!(reply.unwrap(), RespValue::Array(vec![]));
    assert_eq!(request, words(&["CLUSTER", "SHARDS"]));
    let (reply, request) = probe("$4\r\nabcd\r\n", |c| c.cluster_myid());
    assert_eq!(reply.unwrap(), "abcd");
    assert_eq!(request, words(&["CLUSTER", "MYID"]));
    let (reply, request) = probe("*0\r\n", |c| c.hello(2));
    assert_eq!(reply.unwrap(), RespValue::Array(vec![]));
    assert_eq!(request, words(&["HELLO", "2"]));
    let (reply, request) = probe("$3\r\nid=\r\n", |c| c.client_info());
    assert_eq!(reply.unwrap(), "id=");
    assert_eq!(request, words(&["CLIENT", "INFO"]));
    let (reply, request) = probe(":1\r\n", |c| c.client_kill_id(42));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["CLIENT", "KILL", "ID", "42"]));

    let (reply, request) = probe("*1\r\n:1\r\n", |c| c.slowlog_get(5));
    assert_eq!(reply.unwrap().len(), 1);
    assert_eq!(request, words(&["SLOWLOG", "GET", "5"]));
    let (reply, _) = probe(":1\r\n", |c| c.slowlog_get(5));
    assert!(protocol(reply).starts_with("SLOWLOG returned"));
}

// ---------------------------------------------------------------------------------------------
// Pub/Sub decoding
// ---------------------------------------------------------------------------------------------

#[test]
fn subscription_commands_validate_their_names() {
    let (mut client, requests) = scripted(&["+OK\r\n"]);

    assert!(protocol(client.subscribe(&[])).contains("SUBSCRIBE needs at least one"));
    assert!(protocol(client.psubscribe(&[])).contains("PSUBSCRIBE needs at least one"));
    assert!(protocol(client.ssubscribe(&[])).contains("SSUBSCRIBE needs at least one"));
    assert!(requests.lock().unwrap().is_empty());
}

#[test]
fn next_message_skips_confirmations_and_decodes_every_kind() {
    // A subscriber socket: the server confirms, then pushes three deliveries and a stray array.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        // First connection: the command socket (nothing is sent on it), second: the subscriber.
        let (_command, _) = common::accept_within_deadline(&listener);
        let (socket, _) = common::accept_within_deadline(&listener);
        let mut writer = socket.try_clone().unwrap();
        let mut reader = std::io::BufReader::new(socket);

        parse(&mut reader).unwrap();
        writer
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
            .unwrap();
        writer
            .write_all(
                b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n\
                  *4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n:7\r\n\
                  *2\r\n$7\r\nmessage\r\n$1\r\nx\r\n\
                  *0\r\n\
                  *3\r\n$8\r\nsmessage\r\n$2\r\nsc\r\n$1\r\nz\r\n",
            )
            .unwrap();
        thread::sleep(Duration::from_millis(300));
    });
    let mut client = Client::connect_with(ClientOptions {
        port,
        discover_cluster: false,
        ..ClientOptions::default()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    client.subscribe(&["news"]).unwrap();

    let first = client.next_message().unwrap();
    assert_eq!(
        (
            first.kind.as_str(),
            first.channel.as_str(),
            first.payload.as_slice()
        ),
        ("message", "news", &b"hi"[..])
    );
    assert_eq!(first.pattern, None);

    // The integer payload is rendered as text; the pattern is kept.
    let second = client.next_message().unwrap();
    assert_eq!(second.kind, "pmessage");
    assert_eq!(second.pattern.as_deref(), Some("n*"));
    assert_eq!(second.payload, b"7");

    // The two-element and empty arrays are not deliveries and are skipped.
    let third = client.next_message().unwrap();
    assert_eq!(
        (third.kind.as_str(), third.channel.as_str()),
        ("smessage", "sc")
    );

    // listen() keeps handing messages over until the read fails.
    client
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let mut seen = 0;
    let result = client.listen(|_| seen += 1);

    assert_eq!(seen, 0);
    assert!(matches!(result, Err(Error::Io(_))));
    server.join().unwrap();
}

// ---------------------------------------------------------------------------------------------
// Extras: dump/restore, bloom, json, scripts
// ---------------------------------------------------------------------------------------------

#[test]
fn restore_sends_the_binary_payload_with_its_flags() {
    let (reply, request) = probe("+OK\r\n", |c| {
        c.restore("k", 0, b"RVD1", RestoreOptions::default())
    });
    reply.unwrap();
    assert_eq!(request, words(&["RESTORE", "k", "0", "RVD1"]));
    let (_, request) = probe("+OK\r\n", |c| {
        c.restore(
            "k",
            9,
            b"RVD1",
            RestoreOptions {
                replace: true,
                absolute_ttl: true,
            },
        )
    });
    assert_eq!(
        request,
        words(&["RESTORE", "k", "9", "RVD1", "REPLACE", "ABSTTL"])
    );
    let (_, request) = probe("+OK\r\n", |c| {
        c.restore(
            "k",
            9,
            b"RVD1",
            RestoreOptions {
                replace: true,
                absolute_ttl: false,
            },
        )
    });
    assert_eq!(request.last().unwrap(), "REPLACE");
}

#[test]
fn bloom_insert_and_info() {
    let options = BloomInsertOptions {
        capacity: Some(100),
        error_rate: Some(0.01),
        expansion: Some(2),
        no_create: true,
        non_scaling: true,
        ..BloomInsertOptions::default()
    };
    let (reply, request) = probe("*2\r\n:1\r\n:0\r\n", |c| {
        c.bf_insert("b", &options, &["x", "y"])
    });
    assert_eq!(reply.unwrap(), [true, false]);
    assert_eq!(
        request,
        words(&[
            "BF.INSERT",
            "b",
            "CAPACITY",
            "100",
            "ERROR",
            "0.01",
            "EXPANSION",
            "2",
            "NOCREATE",
            "NONSCALING",
            "ITEMS",
            "x",
            "y"
        ])
    );
    let (_, request) = probe("*1\r\n:1\r\n", |c| {
        c.bf_insert("b", &BloomInsertOptions::default(), &["x"])
    });
    assert_eq!(request, words(&["BF.INSERT", "b", "ITEMS", "x"]));
    let (reply, _) = probe("*1\r\n-ERR full\r\n", |c| {
        c.bf_insert("b", &BloomInsertOptions::default(), &["x"])
    });
    assert!(matches!(reply, Err(Error::Server(m)) if m == "ERR full"));
    let (reply, _) = probe("*1\r\n$1\r\nx\r\n", |c| c.bf_madd("b", &["x"]));
    assert!(protocol(reply).starts_with("BF.MADD returned"));
    let (reply, _) = probe(":1\r\n", |c| c.bf_mexists("b", &["x"]));
    assert!(protocol(reply).starts_with("BF.MEXISTS returned"));

    let info = "*10\r\n$8\r\nCapacity\r\n:100\r\n$4\r\nSize\r\n:200\r\n$17\r\nNumber of filters\r\n:3\r\n\
                $24\r\nNumber of items inserted\r\n:4\r\n$14\r\nExpansion rate\r\n:2\r\n";
    let (reply, request) = probe(info, |c| c.bf_info("b"));
    let reply = reply.unwrap();
    assert_eq!(request, words(&["BF.INFO", "b"]));
    assert_eq!(
        (
            reply.capacity,
            reply.size,
            reply.filters,
            reply.items,
            reply.expansion
        ),
        (100, 200, 3, 4, Some(2))
    );
    let (reply, _) = probe(
        "*4\r\n$5\r\nOther\r\n:1\r\n$14\r\nExpansion rate\r\n$-1\r\n",
        |c| c.bf_info("b"),
    );
    assert_eq!(reply.unwrap().expansion, None);
    let (reply, _) = probe(":1\r\n", |c| c.bf_info("b"));
    assert!(protocol(reply).starts_with("BF.INFO returned"));

    let (reply, request) = probe(":5\r\n", |c| c.bf_info_field("b", "SIZE"));
    assert_eq!(reply.unwrap(), Some(5));
    assert_eq!(request, words(&["BF.INFO", "b", "SIZE"]));
    let (reply, _) = probe("$-1\r\n", |c| c.bf_info_field("b", "EXPANSION"));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("*1\r\n:9\r\n", |c| c.bf_info_field("b", "EXPANSION"));
    assert_eq!(reply.unwrap(), Some(9));
    let (reply, _) = probe("*1\r\n$-1\r\n", |c| c.bf_info_field("b", "EXPANSION"));
    assert_eq!(reply.unwrap(), None);
    let (reply, _) = probe("+x\r\n", |c| c.bf_info_field("b", "SIZE"));
    assert!(protocol(reply).starts_with("BF.INFO returned"));
}

#[test]
fn json_conditions_and_index_creation() {
    for (condition, tail) in [
        (JsonSetCondition::Always, None),
        (JsonSetCondition::IfMissing, Some("NX")),
        (JsonSetCondition::IfExists, Some("XX")),
    ] {
        let (reply, request) = probe("+OK\r\n", |c| c.json_set_with("k", "$", "{}", condition));
        assert!(reply.unwrap());
        let mut expected = words(&["JSON.SET", "k", "$", "{}"]);
        expected.extend(tail.map(str::to_owned));
        assert_eq!(request, expected);
    }
    let (reply, _) = probe("$-1\r\n", |c| {
        c.json_set_with("k", "$", "{}", JsonSetCondition::IfMissing)
    });
    assert!(!reply.unwrap());

    for (kind, word) in [
        (JsonIndexType::String, "string"),
        (JsonIndexType::Number, "number"),
        (JsonIndexType::Bool, "bool"),
    ] {
        let (reply, request) = probe("+OK\r\n", |c| {
            c.json_index_create("i", "u:", "$.a", Some(kind))
        });
        reply.unwrap();
        assert_eq!(
            request,
            words(&[
                "JSON.INDEX",
                "CREATE",
                "i",
                "PREFIX",
                "u:",
                "PATH",
                "$.a",
                "TYPE",
                word
            ])
        );
    }
    let (_, request) = probe("+OK\r\n", |c| c.json_index_create("i", "u:", "$.a", None));
    assert_eq!(
        request,
        words(&["JSON.INDEX", "CREATE", "i", "PREFIX", "u:", "PATH", "$.a"])
    );
}

#[test]
fn idem_begin_decodes_every_state() {
    let ttl = Duration::from_secs(1);

    let (state, request) = probe("*1\r\n$8\r\nacquired\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert_eq!(state.unwrap(), IdemState::Acquired);
    assert_eq!(request, words(&["IDEM", "k", "BEGIN", "fp", "me", "1000"]));
    let (state, _) = probe("*1\r\n$7\r\npending\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert_eq!(state.unwrap(), IdemState::Pending);
    let (state, _) = probe("*1\r\n$8\r\nconflict\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert_eq!(state.unwrap(), IdemState::Conflict);
    let (state, _) = probe("*2\r\n$9\r\ncompleted\r\n$3\r\nres\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert_eq!(state.unwrap(), IdemState::Completed(b"res".to_vec()));
    let (state, _) = probe("*1\r\n$9\r\ncompleted\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert_eq!(state.unwrap(), IdemState::Completed(Vec::new()));
    let (state, _) = probe("*1\r\n$5\r\nweird\r\n", |c| {
        c.idem_begin("k", "fp", "me", ttl)
    });
    assert!(protocol(state).contains("IDEM returned state"));
    let (state, _) = probe("+OK\r\n", |c| c.idem_begin("k", "fp", "me", ttl));
    assert!(protocol(state).starts_with("IDEM returned"));

    let (done, request) = probe(":1\r\n", |c| c.idem_complete("k", "fp", "me", b"\xffbin"));
    assert!(done.unwrap());
    assert_eq!(request.len(), 6);
    let (done, _) = probe(":0\r\n", |c| c.idem_complete("k", "fp", "me", b"x"));
    assert!(!done.unwrap());
    let (done, _) = probe("+OK\r\n", |c| c.idem_complete("k", "fp", "me", b"x"));
    assert!(protocol(done).starts_with("IDEM returned"));
}

#[test]
fn fill_get_decodes_every_outcome() {
    let fill = Duration::from_secs(1);

    let (reply, request) = probe("*2\r\n$3\r\nHIT\r\n$1\r\nv\r\n", |c| c.fill_get("k", fill));
    assert_eq!(reply.unwrap(), FillOutcome::Hit("v".into()));
    assert_eq!(request, words(&["FILLGET", "k", "1000"]));
    let (reply, _) = probe("*2\r\n$4\r\nFILL\r\n$1\r\nt\r\n", |c| c.fill_get("k", fill));
    assert_eq!(reply.unwrap(), FillOutcome::Fill("t".into()));
    let (reply, _) = probe("*2\r\n$4\r\nBUSY\r\n$-1\r\n", |c| c.fill_get("k", fill));
    assert_eq!(reply.unwrap(), FillOutcome::Busy);
    let (reply, _) = probe("*3\r\n$5\r\nSTALE\r\n$3\r\nold\r\n$1\r\nt\r\n", |c| {
        c.fill_get("k", fill)
    });
    assert_eq!(
        reply.unwrap(),
        FillOutcome::Stale {
            value: "old".into(),
            token: Some("t".into())
        }
    );
    let (reply, _) = probe("*3\r\n$5\r\nSTALE\r\n$3\r\nold\r\n$-1\r\n", |c| {
        c.fill_get("k", fill)
    });
    assert_eq!(
        reply.unwrap(),
        FillOutcome::Stale {
            value: "old".into(),
            token: None
        }
    );
    for bad in [
        "*3\r\n$3\r\nHIT\r\n$1\r\nv\r\n$1\r\nw\r\n",
        "*2\r\n$5\r\nOTHER\r\n$1\r\nv\r\n",
        "*1\r\n$3\r\nHIT\r\n",
        ":1\r\n",
    ] {
        let (reply, _) = probe(bad, |c| c.fill_get("k", fill));
        assert!(protocol(reply).starts_with("FILLGET returned"), "{bad}");
    }

    let (reply, request) = probe(":1\r\n", |c| {
        c.fill_set("k", "v", fill, "tok", Some(Duration::from_secs(2)))
    });
    assert!(reply.unwrap());
    assert_eq!(
        request,
        words(&["FILLSET", "k", "v", "1000", "tok", "SWR", "2000"])
    );
    let (reply, request) = probe(":0\r\n", |c| c.fill_set("k", "v", fill, "tok", None));
    assert!(!reply.unwrap());
    assert_eq!(request.len(), 5);
}

// ---------------------------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------------------------

#[test]
fn scan_iterators_walk_pages_and_report_errors_once() {
    let (mut client, requests) = scripted(&[
        "*2\r\n$1\r\n7\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nc\r\n",
    ]);

    {
        let mut iterator = client.scan_iter(Some("x*"), Some(2));

        assert_eq!(iterator.cursor(), 0);
        assert_eq!(iterator.next().unwrap().unwrap(), "a");
        assert_eq!(iterator.cursor(), 7);
        assert_eq!(iterator.next().unwrap().unwrap(), "b");
        assert_eq!(iterator.next().unwrap().unwrap(), "c");
        assert_eq!(iterator.cursor(), 0);
        assert!(iterator.next().is_none());
        assert!(iterator.next().is_none());
    }

    let log = requests.lock().unwrap().clone();

    assert_eq!(log[0], words(&["SCAN", "0", "MATCH", "x*", "COUNT", "2"]));
    assert_eq!(log[1], words(&["SCAN", "7", "MATCH", "x*", "COUNT", "2"]));

    // A malformed page ends the walk after reporting the error once.
    let (mut client, _) = scripted(&["+OK\r\n"]);
    let mut failing = client.scan_iter(None, None);

    assert!(matches!(failing.next(), Some(Err(Error::Protocol(_)))));
    assert!(failing.next().is_none());
}

#[test]
fn hash_set_and_sorted_set_scan_iterators() {
    let (mut client, requests) =
        scripted(&["*2\r\n$1\r\n0\r\n*4\r\n$1\r\nf\r\n$1\r\n1\r\n$1\r\ng\r\n$1\r\n2\r\n"]);
    let fields: Vec<_> = client
        .hscan_iter("h", None, None)
        .map(Result::unwrap)
        .collect();

    assert_eq!(
        fields,
        [
            ("f".to_owned(), "1".to_owned()),
            ("g".to_owned(), "2".to_owned())
        ]
    );
    assert_eq!(requests.lock().unwrap()[0], words(&["HSCAN", "h", "0"]));

    let (mut client, requests) = scripted(&["*2\r\n$1\r\n0\r\n*2\r\n$1\r\nm\r\n$3\r\n1.5\r\n"]);
    let entries: Vec<_> = client
        .zscan_iter("z", Some("m*"), None)
        .map(Result::unwrap)
        .collect();

    assert_eq!((entries[0].member.as_str(), entries[0].score), ("m", 1.5));
    assert_eq!(
        requests.lock().unwrap()[0],
        words(&["ZSCAN", "z", "0", "MATCH", "m*"])
    );

    let (mut client, requests) = scripted(&["*2\r\n$1\r\n0\r\n*1\r\n$1\r\nm\r\n"]);
    let members: Vec<_> = client
        .sscan_iter("s", None, Some(10))
        .map(Result::unwrap)
        .collect();

    assert_eq!(members, ["m"]);
    assert_eq!(
        requests.lock().unwrap()[0],
        words(&["SSCAN", "s", "0", "COUNT", "10"])
    );
}

#[test]
fn scan_pages_reject_bad_cursors_and_shapes() {
    let (reply, _) = probe("*2\r\n$1\r\nx\r\n*0\r\n", |c| c.scan(0, None, None, None));
    assert!(protocol(reply).contains("bad cursor"));
    let (reply, _) = probe("*3\r\n$1\r\n0\r\n*0\r\n*0\r\n", |c| {
        c.scan(0, None, None, None)
    });
    assert!(protocol(reply).starts_with("SCAN returned"));
    let (reply, _) = probe(":1\r\n", |c| c.scan(0, None, None, None));
    assert!(reply.is_err());
    let (reply, _) = probe("*2\r\n$1\r\n0\r\n:1\r\n", |c| c.sscan("s", 0, None, None));
    assert!(reply.is_err());
}

// ---------------------------------------------------------------------------------------------
// Helpers built on commands
// ---------------------------------------------------------------------------------------------

#[test]
fn lock_guard_releases_on_drop_and_on_demand() {
    // try_lock: SET NX PX -> OK, then the guard's drop sends DELEX.
    let (mut client, requests) = scripted(&["+OK\r\n", ":1\r\n"]);

    {
        let mut guard = client
            .try_lock("lock", Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_eq!(guard.key(), "lock");
        assert_eq!(guard.token().len(), 32);
        // The guard derefs to the client, mutably as well.
        assert_eq!(guard.database(), 0);
        guard.select(0).ok();
    }

    let log = requests.lock().unwrap().clone();

    assert_eq!(log[0][..2], words(&["SET", "lock"])[..]);
    assert_eq!(&log[0][3..], &words(&["PX", "5000", "NX"])[..]);
    assert_eq!(log.last().unwrap()[0], "DELEX");

    // Explicit release reports the server's answer and does not send a second DELEX.
    let (mut client, requests) = scripted(&["+OK\r\n", ":0\r\n"]);
    let guard = client
        .try_lock("lock", Duration::from_secs(5))
        .unwrap()
        .unwrap();

    assert!(!guard.release().unwrap());
    assert_eq!(requests.lock().unwrap().len(), 2);

    // Renew uses SET IFEQ.
    let (mut client, requests) = scripted(&["+OK\r\n", "+OK\r\n", ":1\r\n"]);
    let mut guard = client
        .try_lock("lock", Duration::from_secs(5))
        .unwrap()
        .unwrap();

    assert!(guard.renew(Duration::from_secs(9)).unwrap());
    drop(guard);
    assert!(requests.lock().unwrap()[1].contains(&"IFEQ".to_owned()));

    // Held elsewhere: no guard.
    let (mut client, _) = scripted(&["$-1\r\n"]);
    assert!(
        client
            .try_lock("lock", Duration::from_secs(5))
            .unwrap()
            .is_none()
    );
}

#[test]
fn with_lock_runs_the_closure_and_skips_it_when_held() {
    let (mut client, requests) = scripted(&["+OK\r\n", "+OK\r\n", ":1\r\n"]);
    let outcome: Result<Option<i32>, Error> =
        client.with_lock("lock", Duration::from_secs(5), |db| {
            db.set("inside", "1")?;

            Ok(42)
        });

    assert_eq!(outcome.unwrap(), Some(42));
    assert_eq!(requests.lock().unwrap().last().unwrap()[0], "DELEX");

    let (mut client, _) = scripted(&["$-1\r\n"]);
    let outcome: Result<Option<i32>, Error> =
        client.with_lock("lock", Duration::from_secs(5), |_| Ok(1));

    assert_eq!(outcome.unwrap(), None);

    // An error from the closure propagates and the guard still releases on drop.
    let (mut client, requests) = scripted(&["+OK\r\n", "-ERR boom\r\n", ":1\r\n"]);
    let outcome: Result<Option<i32>, Error> =
        client.with_lock("lock", Duration::from_secs(5), |db| {
            db.set("inside", "1")?;

            Ok(1)
        });

    assert!(matches!(outcome, Err(Error::Server(_))));
    assert_eq!(requests.lock().unwrap().last().unwrap()[0], "DELEX");
}

#[test]
fn blocking_lock_polls_until_the_deadline() {
    let (mut client, requests) = scripted(&["$-1\r\n"]);
    let started = std::time::Instant::now();

    assert!(
        client
            .lock("lock", Duration::from_secs(1), Duration::from_millis(30))
            .unwrap()
            .is_none()
    );
    assert!(started.elapsed() >= Duration::from_millis(30));
    assert!(requests.lock().unwrap().len() >= 2);

    let (mut client, _) = scripted(&["$-1\r\n", "$-1\r\n", "+OK\r\n", ":1\r\n"]);
    let guard = client
        .lock("lock", Duration::from_secs(1), Duration::from_secs(5))
        .unwrap();

    assert!(guard.is_some());
}

#[test]
fn lease_and_semaphore_guards() {
    // lease -> token 9, drop -> RELEASE key 9
    let (mut client, requests) = scripted(&[":9\r\n", ":1\r\n"]);

    {
        let mut guard = client
            .lease_guard("lease", Duration::from_millis(1500))
            .unwrap()
            .unwrap();

        assert_eq!(guard.fence(), 9);
        assert_eq!(guard.database(), 0);
        guard.select(0).ok();
    }

    let log = requests.lock().unwrap().clone();

    assert_eq!(log[0], words(&["LEASE", "lease", "1500"]));
    assert_eq!(log[2], words(&["RELEASE", "lease", "9"]));

    let (mut client, _) = scripted(&[":9\r\n", ":0\r\n"]);
    assert!(
        !client
            .lease_guard("lease", Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .release()
            .unwrap()
    );
    let (mut client, _) = scripted(&["$-1\r\n"]);
    assert!(
        client
            .lease_guard("lease", Duration::from_secs(1))
            .unwrap()
            .is_none()
    );

    let (mut client, requests) = scripted(&[":4\r\n", ":1\r\n"]);

    {
        let mut permit = client
            .semaphore_permit("sem", 3, Duration::from_secs(2))
            .unwrap()
            .unwrap();

        assert_eq!(permit.token(), 4);
        assert_eq!(permit.database(), 0);
        permit.select(0).ok();
    }

    assert_eq!(
        requests.lock().unwrap()[0],
        words(&["SEMAPHORE", "sem", "3", "2000"])
    );
    assert_eq!(
        requests.lock().unwrap().last().unwrap(),
        &words(&["RELEASE", "sem", "4"])
    );

    let (mut client, _) = scripted(&[":4\r\n", ":1\r\n"]);
    assert!(
        client
            .semaphore_permit("sem", 3, Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .release()
            .unwrap()
    );
    let (mut client, _) = scripted(&["$-1\r\n"]);
    assert!(
        client
            .semaphore_permit("sem", 3, Duration::from_secs(2))
            .unwrap()
            .is_none()
    );
}

#[test]
fn typed_keywords_match_the_old_string_requests() {
    use ruvio_client::{InsertPosition, KeyType, ListSide};

    let (_, request) = probe(":1\r\n", |c| {
        c.linsert("l", InsertPosition::After, "p", "v")
    });
    assert_eq!(request, words(&["LINSERT", "l", "AFTER", "p", "v"]));

    let (_, request) = probe("$-1\r\n", |c| {
        c.lmove("a", "b", ListSide::Right, ListSide::Left)
    });
    assert_eq!(request, words(&["LMOVE", "a", "b", "RIGHT", "LEFT"]));

    let (_, request) = probe("$-1\r\n", |c| {
        c.blmove(
            "a",
            "b",
            ListSide::Left,
            ListSide::Right,
            std::time::Duration::from_secs(2),
        )
    });
    assert_eq!(request, words(&["BLMOVE", "a", "b", "LEFT", "RIGHT", "2"]));

    let (_, request) = probe("$-1\r\n", |c| {
        c.geo_dist("g", "a", "b", Some(GeoUnit::Miles))
    });
    assert_eq!(request, words(&["GEODIST", "g", "a", "b", "mi"]));

    let (_, request) = probe("$-1\r\n", |c| c.geo_dist("g", "a", "b", None));
    assert_eq!(request, words(&["GEODIST", "g", "a", "b"]));

    let (_, request) = probe("*0\r\n", |c| {
        c.geo_search("g", 1.5, 2.0, 3.0, GeoUnit::Feet)
    });
    assert_eq!(
        request,
        words(&[
            "GEOSEARCH",
            "g",
            "FROMLONLAT",
            "1.5",
            "2.0",
            "BYRADIUS",
            "3.0",
            "ft",
            "ASC"
        ])
    );

    let (_, request) = probe("*2\r\n$1\r\n0\r\n*0\r\n", |c| {
        c.scan(5, Some("a*"), Some(10), Some(KeyType::SortedSet))
    });
    assert_eq!(
        request,
        words(&["SCAN", "5", "MATCH", "a*", "COUNT", "10", "TYPE", "zset"])
    );
}

// ---------------------------------------------------------------------------------------------
// TTL on create and the EXPIRE conditions
// ---------------------------------------------------------------------------------------------

const MINUTE: Duration = Duration::from_secs(60);
const HALF_SECOND: Duration = Duration::from_millis(1500);

#[test]
fn expire_conditions_are_sent_after_the_time() {
    for (condition, word) in [
        (ExpireCondition::Nx, "NX"),
        (ExpireCondition::Xx, "XX"),
        (ExpireCondition::Gt, "GT"),
        (ExpireCondition::Lt, "LT"),
    ] {
        let (reply, request) = probe(":1\r\n", |c| c.expire_if("k", MINUTE, condition));
        assert!(reply.unwrap());
        assert_eq!(request, words(&["EXPIRE", "k", "60", word]));

        let (reply, request) = probe(":0\r\n", |c| c.expire_if("k", HALF_SECOND, condition));
        assert!(!reply.unwrap());
        assert_eq!(request, words(&["PEXPIRE", "k", "1500", word]));

        let (_, request) = probe(":1\r\n", |c| c.pexpire_if("k", MINUTE, condition));
        assert_eq!(request, words(&["PEXPIRE", "k", "60000", word]));

        let (_, request) = probe(":1\r\n", |c| {
            c.expire_at_if("k", at(1_900_000_000, 0), condition)
        });
        assert_eq!(request, words(&["EXPIREAT", "k", "1900000000", word]));

        let (_, request) = probe(":1\r\n", |c| {
            c.expire_at_if("k", at(1_900_000_000, 250), condition)
        });
        assert_eq!(request, words(&["PEXPIREAT", "k", "1900000000250", word]));

        let (_, request) = probe(":1\r\n", |c| {
            c.pexpire_at_if("k", at(1_900_000_000, 0), condition)
        });
        assert_eq!(request, words(&["PEXPIREAT", "k", "1900000000000", word]));
    }

    let (reply, _) = probe(":1\r\n", |c| {
        c.expire_at_if(
            "k",
            UNIX_EPOCH - Duration::from_secs(1),
            ExpireCondition::Nx,
        )
    });
    assert!(protocol(reply).contains("epoch"));
}

#[test]
fn ttl_on_create_counters_and_bits() {
    let (reply, request) = probe(":1\r\n", |c| c.incr_with_ttl("k", MINUTE));
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(request, words(&["INCR", "k", "EX", "60"]));

    let (_, request) = probe(":1\r\n", |c| c.incr_with_ttl("k", HALF_SECOND));
    assert_eq!(request, words(&["INCR", "k", "PX", "1500"]));

    let (_, request) = probe(":1\r\n", |c| {
        c.incr_with_ttl("k", Duration::from_micros(1500))
    });
    assert_eq!(request, words(&["INCR", "k", "PX", "2"]));

    let (_, request) = probe(":1\r\n", |c| c.decr_with_ttl("k", MINUTE));
    assert_eq!(request, words(&["DECR", "k", "EX", "60"]));

    let (_, request) = probe(":1\r\n", |c| c.incr_by_with_ttl("k", -5, MINUTE));
    assert_eq!(request, words(&["INCRBY", "k", "-5", "EX", "60"]));

    let (_, request) = probe(":1\r\n", |c| c.decr_by_with_ttl("k", 5, HALF_SECOND));
    assert_eq!(request, words(&["DECRBY", "k", "5", "PX", "1500"]));

    let (reply, request) = probe(":3\r\n", |c| c.incr_by_max_with_ttl("k", 1, 10, MINUTE));
    assert_eq!(reply.unwrap(), Some(3));
    assert_eq!(
        request,
        words(&["INCRBY", "k", "1", "EX", "60", "MAX", "10"])
    );
    let (reply, _) = probe("$-1\r\n", |c| c.incr_by_max_with_ttl("k", 1, 10, MINUTE));
    assert_eq!(reply.unwrap(), None);

    let (reply, request) = probe("$3\r\n1.5\r\n", |c| {
        c.incr_by_float_with_ttl("k", 1.5, MINUTE)
    });
    assert_eq!(reply.unwrap(), 1.5);
    assert_eq!(request, words(&["INCRBYFLOAT", "k", "1.5", "EX", "60"]));

    let (reply, request) = probe(":2\r\n", |c| c.hincr_by_with_ttl("k", "f", 2, MINUTE));
    assert_eq!(reply.unwrap(), 2);
    assert_eq!(request, words(&["HINCRBY", "k", "f", "2", "EX", "60"]));

    let (reply, request) = probe("$3\r\n2.5\r\n", |c| {
        c.zincr_by_with_ttl("k", "m", 2.5, HALF_SECOND)
    });
    assert_eq!(reply.unwrap(), 2.5);
    assert_eq!(request, words(&["ZINCRBY", "k", "2.5", "m", "PX", "1500"]));

    let (reply, request) = probe(":0\r\n", |c| c.setbit_with_ttl("k", 7, true, MINUTE));
    assert!(!reply.unwrap());
    assert_eq!(request, words(&["SETBIT", "k", "7", "1", "EX", "60"]));
    let (_, request) = probe(":0\r\n", |c| c.setbit_with_ttl("k", 7, false, MINUTE));
    assert_eq!(request, words(&["SETBIT", "k", "7", "0", "EX", "60"]));
}

#[test]
fn ttl_on_create_is_checked_before_sending() {
    let (mut client, requests) = scripted(&[":1\r\n"]);

    assert!(protocol(client.incr_with_ttl("k", Duration::ZERO)).contains("greater than zero"));
    assert!(protocol(client.setbit_with_ttl("k", 1, true, Duration::ZERO)).contains("zero"));
    assert!(protocol(client.incr_with_ttl("k", Duration::MAX)).contains("too large"));
    assert!(
        protocol(client.zadd_with(
            "k",
            SortedSetAddOptions {
                ttl: Some(Duration::ZERO),
                ..SortedSetAddOptions::default()
            },
            &[("a", 1.0)]
        ))
        .contains("zero")
    );
    assert!(requests.lock().unwrap().is_empty());
}

#[test]
fn ttl_on_create_zadd_geoadd_xadd_json_and_bloom() {
    let ttl = Some(MINUTE);
    let options = SortedSetAddOptions {
        ttl,
        if_exists: true,
        greater_than: true,
        changed: true,
        ..SortedSetAddOptions::default()
    };
    let (reply, request) = probe(":1\r\n", |c| {
        c.zadd_with("z", options, &[("a", 1.0), ("b", 2.5)])
    });
    assert_eq!(reply.unwrap(), 1);
    assert_eq!(
        request,
        words(&[
            "ZADD", "z", "EX", "60", "XX", "GT", "CH", "1.0", "a", "2.5", "b"
        ])
    );

    let (reply, request) = probe("$3\r\n3.0\r\n", |c| c.zadd_incr("z", "a", 2.0, options));
    assert_eq!(reply.unwrap(), Some(3.0));
    assert_eq!(
        request,
        words(&["ZADD", "z", "EX", "60", "XX", "GT", "CH", "INCR", "2", "a"])
    );

    let (_, request) = probe(":1\r\n", |c| c.zadd("z", &[("a", 1.0)]));
    assert_eq!(request, words(&["ZADD", "z", "1.0", "a"]));

    let geo = SortedSetAddOptions {
        ttl: Some(HALF_SECOND),
        if_not_exists: true,
        changed: true,
        ..SortedSetAddOptions::default()
    };
    let (_, request) = probe(":1\r\n", |c| {
        c.geo_add_with("g", geo, &[(13.5, 38.25, "p")])
    });
    assert_eq!(
        request,
        words(&[
            "GEOADD", "g", "PX", "1500", "NX", "CH", "13.5", "38.25", "p"
        ])
    );

    let (reply, request) = probe("$3\r\n1-1\r\n", |c| {
        c.xadd_with_ttl("s", MINUTE, "*", &[("f", "v"), ("g", "w")])
    });
    assert_eq!(reply.unwrap(), "1-1");
    assert_eq!(
        request,
        words(&["XADD", "s", "EX", "60", "*", "f", "v", "g", "w"])
    );

    let trim = StreamTrim::max_len(100).mode(TrimMode::Approximate);
    let (_, request) = probe("$3\r\n1-1\r\n", |c| {
        c.xadd_trim_with_ttl("s", HALF_SECOND, &trim, "*", &[("f", "v")])
    });
    assert_eq!(
        request,
        words(&[
            "XADD", "s", "PX", "1500", "MAXLEN", "~", "100", "*", "f", "v"
        ])
    );

    for (condition, tail) in [
        (JsonSetCondition::Always, &[][..]),
        (JsonSetCondition::IfMissing, &["NX"][..]),
        (JsonSetCondition::IfExists, &["XX"][..]),
    ] {
        let (reply, request) = probe("+OK\r\n", |c| {
            c.json_set_with_ttl("j", "$", "{}", condition, MINUTE)
        });
        assert!(reply.unwrap());
        let mut expected = vec!["JSON.SET", "j", "$", "{}"];
        expected.extend(tail);
        expected.extend(["EX", "60"]);
        assert_eq!(request, words(&expected));
    }
    let (reply, _) = probe("$-1\r\n", |c| {
        c.json_set_with_ttl("j", "$", "{}", JsonSetCondition::IfMissing, MINUTE)
    });
    assert!(!reply.unwrap());

    let bloom = BloomInsertOptions {
        capacity: Some(100),
        ttl: Some(MINUTE),
        no_create: true,
        ..BloomInsertOptions::default()
    };
    let (reply, request) = probe("*1\r\n:1\r\n", |c| c.bf_insert("b", &bloom, &["x"]));
    assert_eq!(reply.unwrap(), [true]);
    assert_eq!(
        request,
        words(&[
            "BF.INSERT",
            "b",
            "EX",
            "60",
            "CAPACITY",
            "100",
            "NOCREATE",
            "ITEMS",
            "x"
        ])
    );
}

#[cfg(feature = "serde")]
#[test]
fn ttl_on_create_json_value() {
    let (reply, request) = probe("+OK\r\n", |c| {
        c.json_set_value_with_ttl("j", "$", &[1, 2], JsonSetCondition::Always, HALF_SECOND)
    });
    assert!(reply.unwrap());
    assert_eq!(
        request,
        words(&["JSON.SET", "j", "$", "[1,2]", "PX", "1500"])
    );
}
