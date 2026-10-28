//! Live tests for the commands and helpers added for full matrix coverage. Runs only when
//! `RUVIO_TEST_ADDR` is set (e.g. `RUVIO_TEST_ADDR=127.0.0.1:17101 cargo test`). Each test
//! owns one logical database (6 to 15, and 0 for `JSON.INDEX`, which is database-0 only) and
//! flushes it first.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use ruvio_client::{
    Aggregate, BitOperation, BitUnit, BloomInsertOptions, BloomReserveOptions, CacheOptions,
    Client, Error, ExpireCondition, FillOutcome, GeoFrom, GeoOrder, GeoSearch, GeoShape, GeoUnit,
    IdemOutcome, IdemState, JsonIndexType, JsonSetCondition, ListPositionOptions, ListSide,
    RangeBy, RangeOptions, RespValue, RestoreOptions, SetMode, SortedSetAddOptions,
    SortedSetCombine, SortedSetEntry, StreamTrim,
};

fn connect(database: u32) -> Option<Client> {
    let address: SocketAddr = std::env::var("RUVIO_TEST_ADDR").ok()?.parse().ok()?;

    common::claim_database(database);

    let mut client = Client::connect_addr(address, database).unwrap();

    // 70 s: get_or_create tests wait out a 60 s fill; nothing may wait longer.
    client
        .set_read_timeout(Some(Duration::from_secs(70)))
        .unwrap();
    client.flushdb().unwrap();

    Some(client)
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

#[test]
fn ruvio_extension_strings_and_set_ifeq() {
    let Some(mut db) = connect(8) else { return };

    assert_eq!(
        db.ruvio(None).unwrap(),
        RespValue::Simple("PONG is taken".into())
    );
    assert!(matches!(
        db.ruvio(Some("OWNER")).unwrap(),
        RespValue::Simple(_)
    ));

    db.set("k", "v1").unwrap();
    assert!(!db.set_if_eq("k", "v2", "nope", None).unwrap());
    assert!(
        db.set_if_eq("k", "v2", "v1", Some(Duration::from_secs(60)))
            .unwrap()
    );
    assert!(db.pttl("k").unwrap() > 0);
    assert!(db.set_if_eq_keep_ttl("k", "v3", "v2").unwrap());
    assert!(db.pttl("k").unwrap() > 0);
    assert!(!db.set_if_eq("missing", "x", "y", None).unwrap());

    assert!(db.set_if_greater("wm", 10, None).unwrap());
    assert!(!db.set_if_greater("wm", 10, None).unwrap());
    assert!(
        db.set_if_greater("wm", 11, Some(Duration::from_secs(60)))
            .unwrap()
    );
    assert!(db.pttl("wm").unwrap() > 0);
    assert!(!db.set_if_less("wm", 12, None).unwrap());
    assert!(db.set_if_less("wm", -1, None).unwrap());
    assert_eq!(db.get("wm").unwrap(), Some(b"-1".to_vec()));
    assert!(!db.delex("k", "v2").unwrap());
    assert!(db.delex("k", "v3").unwrap());
    assert_eq!(db.exists_key("k").unwrap(), 0);

    assert!(db.set_ex("ex", "v", 100).unwrap());
    assert!(db.ttl("ex").unwrap() > 90);
    assert!(
        db.set_exat("exat", "v", 4_102_444_800, SetMode::Always)
            .unwrap()
    );
    assert!(db.ttl("exat").unwrap() > 0);
    db.setex("se", 100, "v").unwrap();
    assert!(db.ttl("se").unwrap() > 90);
    db.psetex("pse", Duration::from_millis(5000), "v").unwrap();
    assert!(db.pttl("pse").unwrap() > 1000);
    assert!(db.setnx("nx", "1").unwrap());
    assert!(!db.setnx("nx", "2").unwrap());
    assert_eq!(db.getdel("nx").unwrap().as_deref(), Some("1"));
    assert_eq!(db.getdel("nx").unwrap(), None);
    assert!(db.msetnx(&[("m1", "1"), ("m2", "2")]).unwrap());
    assert!(!db.msetnx(&[("m2", "x"), ("m3", "3")]).unwrap());
    assert_eq!(db.incr_by_float("f", 1.5).unwrap(), 1.5);
    assert_eq!(db.incr_by_float("f", -0.25).unwrap(), 1.25);
    db.set("s", "hello").unwrap();
    assert_eq!(db.getrange("s", 0, 1).unwrap(), "he");
    assert_eq!(db.substr("s", -3, -1).unwrap(), "llo");
    assert_eq!(db.setrange("s", 1, "a").unwrap(), 5);
    assert_eq!(db.get_string("s").unwrap().as_deref(), Some("hallo"));

    // Binary round trip through SET / GET and DUMP / RESTORE.
    let blob = [0_u8, 0xff, b'\r', b'\n', 7];

    db.set_bytes("bin", &blob, None).unwrap();
    assert_eq!(db.get("bin").unwrap().as_deref(), Some(&blob[..]));
    db.set_bytes("bin-ttl", &blob, Some(Duration::from_secs(30)))
        .unwrap();
    assert!(db.pttl("bin-ttl").unwrap() > 0);

    db.sadd("set", &["a", "b"]).unwrap();
    let payload = db.dump("set").unwrap().expect("payload");

    assert!(payload.starts_with(b"RVD"));
    assert_eq!(db.dump("missing").unwrap(), None);
    db.restore("copy", 0, &payload, RestoreOptions::default())
        .unwrap();
    assert_eq!(sorted(db.smembers("copy").unwrap()), ["a", "b"]);
    assert!(
        db.restore("copy", 0, &payload, RestoreOptions::default())
            .is_err()
    );
    db.restore(
        "copy",
        60_000,
        &payload,
        RestoreOptions {
            replace: true,
            absolute_ttl: false,
        },
    )
    .unwrap();
    assert!(db.pttl("copy").unwrap() > 0);
    assert!(matches!(
        db.restore("bad", 0, b"garbage", RestoreOptions::default()),
        Err(Error::Server(_))
    ));
}

#[test]
fn bitmaps_and_hyperloglog() {
    let Some(mut db) = connect(9) else { return };

    assert!(!db.setbit("b", 7, true).unwrap());
    assert!(db.setbit("b", 7, true).unwrap());
    assert!(db.getbit("b", 7).unwrap());
    assert!(!db.getbit("b", 6).unwrap());
    assert_eq!(db.bitcount("b").unwrap(), 1);
    assert_eq!(db.bitcount_range("b", 0, 0, BitUnit::Byte).unwrap(), 1);
    assert_eq!(db.bitcount_range("b", 0, 6, BitUnit::Bit).unwrap(), 0);
    assert_eq!(db.bitpos("b", true).unwrap(), 7);
    assert_eq!(
        db.bitpos_range("b", true, 0, Some(-1), BitUnit::Bit)
            .unwrap(),
        7
    );
    assert_eq!(
        db.bitpos_range("b", false, 0, None, BitUnit::Byte).unwrap(),
        0
    );
    db.setbit("c", 0, true).unwrap();
    assert_eq!(db.bitop(BitOperation::Or, "d", &["b", "c"]).unwrap(), 1);
    assert_eq!(db.bitcount("d").unwrap(), 2);
    assert_eq!(db.bitop(BitOperation::Not, "n", &["b"]).unwrap(), 1);
    assert_eq!(db.bitcount("n").unwrap(), 7);

    assert!(db.pfadd("h1", &["a", "b", "c"]).unwrap());
    assert!(!db.pfadd("h1", &["a"]).unwrap());
    db.pfadd("h2", &["c", "d"]).unwrap();
    assert_eq!(db.pfcount(&["h1"]).unwrap(), 3);
    assert_eq!(db.pfcount(&["h1", "h2"]).unwrap(), 4);
    db.pfmerge("h3", &["h1", "h2"]).unwrap();
    assert_eq!(db.pfcount(&["h3"]).unwrap(), 4);
}

#[test]
fn lists_and_sets_extensions() {
    let Some(mut db) = connect(10) else { return };

    assert_eq!(db.lpushcap("l", 2, &["a", "b", "c"]).unwrap(), 2);
    assert_eq!(db.lrange("l", 0, -1).unwrap(), ["c", "b"]);
    assert_eq!(db.rpushcap("l", 2, &["d"]).unwrap(), 2);
    assert_eq!(db.lrange("l", 0, -1).unwrap(), ["b", "d"]);
    assert_eq!(
        db.blmpop(Duration::from_secs(1), &["empty", "l"], ListSide::Left, 1)
            .unwrap(),
        Some(("l".to_owned(), vec!["b".to_owned()]))
    );
    assert_eq!(
        db.blmpop(Duration::from_secs(1), &["empty"], ListSide::Left, 1)
            .unwrap(),
        None
    );

    db.rpush("p", &["a", "b", "a", "c", "a"]).unwrap();
    assert_eq!(
        db.lpos_with(
            "p",
            "a",
            ListPositionOptions {
                rank: Some(2),
                max_len: None
            }
        )
        .unwrap(),
        Some(2)
    );
    assert_eq!(
        db.lpos_with("p", "z", ListPositionOptions::default())
            .unwrap(),
        None
    );
    assert_eq!(
        db.lpos_count("p", "a", 0, ListPositionOptions::default())
            .unwrap(),
        [0, 2, 4]
    );
    assert_eq!(
        db.lpos_count(
            "p",
            "a",
            2,
            ListPositionOptions {
                rank: None,
                max_len: Some(3)
            }
        )
        .unwrap(),
        [0, 2]
    );

    db.sadd("s1", &["a", "b", "c"]).unwrap();
    db.sadd("s2", &["b", "c"]).unwrap();
    assert_eq!(db.smismember("s1", &["a", "z"]).unwrap(), [true, false]);
    assert_eq!(db.sintercard(&["s1", "s2"], None).unwrap(), 2);
    assert_eq!(db.sintercard(&["s1", "s2"], Some(1)).unwrap(), 1);

    let page = db.sscan("s1", 0, Some("*"), Some(100)).unwrap();

    assert_eq!(page.cursor, 0);
    assert_eq!(sorted(page.members), ["a", "b", "c"]);

    // Iterators walk every page; COUNT 1 forces several round trips.
    let members: Result<Vec<String>, Error> = db.sscan_iter("s1", None, Some(1)).collect();

    assert_eq!(sorted(members.unwrap()), ["a", "b", "c"]);

    for index in 0..25 {
        db.set(&format!("key:{index}"), "v").unwrap();
    }

    let keys: Vec<String> = db
        .scan_iter(Some("key:*"), Some(4))
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(keys.len(), 25);

    db.hset("h", &[("f1", "1"), ("f2", "2"), ("f3", "3")])
        .unwrap();

    let fields: Vec<(String, String)> = db
        .hscan_iter("h", Some("f*"), Some(1))
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(fields.len(), 3);
    assert!(fields.contains(&("f2".to_owned(), "2".to_owned())));
    assert_eq!(db.sscan_iter("missing", None, None).count(), 0);
}

#[test]
fn sorted_set_extensions() {
    let Some(mut db) = connect(11) else { return };

    db.zadd("z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).unwrap();
    db.zadd("lex", &[("a", 0.0), ("b", 0.0), ("c", 0.0), ("d", 0.0)])
        .unwrap();

    assert_eq!(
        db.zrangebylex("lex", "[b", "+", None).unwrap(),
        ["b", "c", "d"]
    );
    assert_eq!(
        db.zrangebylex("lex", "-", "+", Some((1, 2))).unwrap(),
        ["b", "c"]
    );
    assert_eq!(
        db.zrevrangebylex("lex", "+", "(b", None).unwrap(),
        ["d", "c"]
    );
    assert_eq!(
        db.zrevrangebylex("lex", "+", "-", Some((0, 1))).unwrap(),
        ["d"]
    );
    assert_eq!(
        db.zrange_by_score("z", "(1", "+inf", None).unwrap(),
        ["b", "c"]
    );
    assert_eq!(
        db.zrange_by_score("z", "-inf", "+inf", Some((1, 1)))
            .unwrap(),
        ["b"]
    );
    assert_eq!(
        db.zrange_query(
            "z",
            "0",
            "-1",
            RangeOptions {
                rev: true,
                ..RangeOptions::default()
            }
        )
        .unwrap(),
        ["c", "b", "a"]
    );
    assert_eq!(
        db.zrange_query(
            "z",
            "-inf",
            "+inf",
            RangeOptions {
                by: RangeBy::Score,
                rev: false,
                limit: Some((1, -1))
            }
        )
        .unwrap(),
        ["b", "c"]
    );
    assert_eq!(
        db.zrange_query(
            "lex",
            "[b",
            "[c",
            RangeOptions {
                by: RangeBy::Lex,
                ..RangeOptions::default()
            }
        )
        .unwrap(),
        ["b", "c"]
    );
    assert_eq!(
        db.zrange_query_with_scores(
            "z",
            "2",
            "3",
            RangeOptions {
                by: RangeBy::Score,
                ..RangeOptions::default()
            }
        )
        .unwrap(),
        [SortedSetEntry::new("b", 2.0), SortedSetEntry::new("c", 3.0)]
    );

    assert_eq!(db.zcount("z", "1", "(3").unwrap(), 2);
    assert_eq!(db.zlexcount("lex", "-", "+").unwrap(), 4);
    assert_eq!(db.zmscore("z", &["a", "nope"]).unwrap(), [Some(1.0), None]);

    let page = db.zscan("z", 0, None, Some(100)).unwrap();

    assert_eq!(page.cursor, 0);
    assert_eq!(page.entries.len(), 3);

    let walked: Vec<SortedSetEntry> = db
        .zscan_iter("z", None, Some(1))
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(walked.len(), 3);
    assert!(walked.contains(&SortedSetEntry::new("c", 3.0)));

    db.zadd("y", &[("b", 10.0), ("d", 4.0)]).unwrap();
    assert_eq!(
        db.zunion(&["z", "y"], SortedSetCombine::default())
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        db.zunion_with_scores(
            &["z", "y"],
            SortedSetCombine {
                weights: &[1.0, 2.0],
                aggregate: Some(Aggregate::Max)
            }
        )
        .unwrap()
        .last()
        .cloned(),
        Some(SortedSetEntry::new("b", 20.0))
    );
    assert_eq!(
        db.zinter(&["z", "y"], SortedSetCombine::default()).unwrap(),
        ["b"]
    );
    assert_eq!(
        db.zinter_with_scores(&["z", "y"], SortedSetCombine::default())
            .unwrap(),
        [SortedSetEntry::new("b", 12.0)]
    );
    assert_eq!(db.zdiff(&["z", "y"]).unwrap(), ["a", "c"]);
    assert_eq!(
        db.zdiff_with_scores(&["z", "y"]).unwrap()[0],
        SortedSetEntry::new("a", 1.0)
    );
    assert_eq!(
        db.zunionstore(
            "u",
            &["z", "y"],
            SortedSetCombine {
                weights: &[],
                aggregate: Some(Aggregate::Min)
            }
        )
        .unwrap(),
        4
    );
    assert_eq!(db.zscore("u", "b").unwrap(), Some(2.0));
    assert_eq!(
        db.zinterstore("i", &["z", "y"], SortedSetCombine::default())
            .unwrap(),
        1
    );
    assert_eq!(db.zdiffstore("d", &["z", "y"]).unwrap(), 2);

    assert!(db.zrandmember("z").unwrap().is_some());
    assert_eq!(db.zrandmember("missing").unwrap(), None);
    assert_eq!(db.zrandmember_count("z", 2).unwrap().len(), 2);
    assert_eq!(db.zrandmember_count("z", -5).unwrap().len(), 5);

    let picked = db.zrandmember_with_scores("z", 3).unwrap();

    assert_eq!(picked.len(), 3);
    assert!(picked.iter().all(|entry| entry.score >= 1.0));

    db.hset("h", &[("f", "1"), ("g", "2")]).unwrap();
    assert!(db.hrandfield("h").unwrap().is_some());
    assert_eq!(db.hrandfield_count("h", 2).unwrap().len(), 2);

    let pairs = db.hrandfield_with_values("h", 2).unwrap();

    assert_eq!(pairs.len(), 2);
    assert!(pairs.contains(&("g".to_owned(), "2".to_owned())));
    assert_eq!(db.hincr_by_float("h", "f", 0.5).unwrap(), 1.5);

    // GEOADD options and a full GEOSEARCH.
    assert_eq!(
        db.geo_add_with(
            "g",
            SortedSetAddOptions::default(),
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania")
            ]
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.geo_add_with(
            "g",
            SortedSetAddOptions {
                if_exists: true,
                changed: true,
                ..SortedSetAddOptions::default()
            },
            &[(13.5, 38.1, "Palermo"), (1.0, 1.0, "Nowhere")]
        )
        .unwrap(),
        1
    );

    let found = db
        .geo_search_with(
            "g",
            &GeoSearch {
                count: Some(1),
                with_distance: true,
                with_hash: true,
                with_coordinates: true,
                ..GeoSearch::radius(GeoFrom::LonLat(15.0, 37.0), 400.0, GeoUnit::Kilometers)
            },
        )
        .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].member, "Catania");
    assert!(found[0].distance.unwrap() > 0.0);
    assert!(found[0].hash.is_some());
    assert!(found[0].coordinates.is_some());

    let boxed = db
        .geo_search_with(
            "g",
            &GeoSearch {
                shape: GeoShape::Box(500.0, 500.0),
                order: Some(GeoOrder::Desc),
                ..GeoSearch::radius(GeoFrom::Member("Catania"), 0.0, GeoUnit::Kilometers)
            },
        )
        .unwrap();

    assert_eq!(boxed.len(), 2);
    assert!(boxed.iter().all(|place| place.distance.is_none()));
}

#[test]
fn bloom_filters_full_family() {
    let Some(mut db) = connect(12) else { return };

    db.bf_reserve_with(
        "fixed",
        0.01,
        100,
        BloomReserveOptions {
            expansion: None,
            non_scaling: true,
        },
    )
    .unwrap();
    db.bf_reserve_with(
        "grow",
        0.01,
        100,
        BloomReserveOptions {
            expansion: Some(4),
            non_scaling: false,
        },
    )
    .unwrap();
    assert_eq!(
        db.bf_madd("grow", &["a", "b", "a"]).unwrap(),
        [true, true, false]
    );
    assert_eq!(db.bf_mexists("grow", &["a", "zz"]).unwrap(), [true, false]);
    assert_eq!(db.bf_mexists("missing", &["a"]).unwrap(), [false]);
    assert_eq!(db.bf_card("grow").unwrap(), 2);
    assert_eq!(db.bf_card("missing").unwrap(), 0);

    let info = db.bf_info("grow").unwrap();

    assert_eq!(info.capacity, 100);
    assert_eq!(info.items, 2);
    assert_eq!(info.filters, 1);
    assert_eq!(info.expansion, Some(4));
    assert!(info.size > 0);
    assert_eq!(db.bf_info("fixed").unwrap().expansion, None);
    assert_eq!(db.bf_info_field("grow", "Capacity").unwrap(), Some(100));

    assert_eq!(
        db.bf_insert(
            "ins",
            &BloomInsertOptions {
                capacity: Some(50),
                error_rate: Some(0.001),
                ..BloomInsertOptions::default()
            },
            &["x", "y"]
        )
        .unwrap(),
        [true, true]
    );
    assert_eq!(db.bf_info("ins").unwrap().capacity, 50);
    assert!(matches!(
        db.bf_insert(
            "nope",
            &BloomInsertOptions { no_create: true, ..BloomInsertOptions::default() },
            &["x"]
        ),
        Err(Error::Server(message)) if message.contains("not found")
    ));

    // Helper: per item, whether it was (probably) seen before.
    assert_eq!(
        db.seen_before("seen", &["u1", "u2"]).unwrap(),
        [false, false]
    );
    assert_eq!(
        db.seen_before("seen", &["u1", "u3"]).unwrap(),
        [true, false]
    );
    assert_eq!(db.maybe_seen("seen", &["u2", "u9"]).unwrap(), [true, false]);
}

#[test]
fn json_extensions_and_index() {
    let Some(mut db) = connect(0) else { return };

    db.json_set(
        "u:1",
        "$",
        r#"{"city":"izmir","n":2,"a":[1,2,3],"s":"ab","o":{"x":1}}"#,
    )
    .unwrap();
    assert_eq!(db.json_objlen("u:1", "$.o").unwrap(), Some(1));
    assert_eq!(db.json_objlen("u:1", "$.nope").unwrap(), None);
    assert_eq!(db.json_strappend("u:1", "$.s", r#""cd""#).unwrap(), Some(4));
    assert_eq!(
        db.json_nummultby("u:1", "$.n", 2.5).unwrap().as_deref(),
        Some("5.0")
    );
    assert_eq!(
        db.json_arrinsert("u:1", "$.a", 0, &["0", "-1"]).unwrap(),
        Some(5)
    );
    assert_eq!(
        db.json_arrappend_many("u:1", "$.a", &["4", "5"]).unwrap(),
        Some(7)
    );
    assert_eq!(
        db.json_arrpop("u:1", "$.a", None).unwrap().as_deref(),
        Some("5")
    );
    assert_eq!(
        db.json_arrpop("u:1", "$.a", Some(0)).unwrap().as_deref(),
        Some("0")
    );
    assert_eq!(
        db.json_arrindex("u:1", "$.a", "2", None, None).unwrap(),
        Some(2)
    );
    assert_eq!(
        db.json_arrindex("u:1", "$.a", "2", Some(3), None).unwrap(),
        Some(-1)
    );
    assert_eq!(
        db.json_arrindex("u:1", "$.a", "1", Some(0), Some(2))
            .unwrap(),
        Some(1)
    );
    assert_eq!(db.json_arrtrim("u:1", "$.a", 0, 1).unwrap(), Some(2));
    assert_eq!(db.json_forget("u:1", "$.o").unwrap(), 1);
    assert!(db.jmerge("u:1", "$", r#"{"z":1}"#).unwrap());
    assert!(
        !db.json_set_with("u:1", "$.z", "5", JsonSetCondition::IfMissing)
            .unwrap()
    );
    assert!(
        db.json_set_with("u:1", "$.q", "5", JsonSetCondition::IfMissing)
            .unwrap()
    );
    assert!(
        db.json_set_with("u:1", "$.q", "6", JsonSetCondition::IfExists)
            .unwrap()
    );
    assert!(
        !db.json_set_with("u:1", "$.w", "6", JsonSetCondition::IfExists)
            .unwrap()
    );
    assert_eq!(db.json_get("u:1", "$.q").unwrap().as_deref(), Some("6"));

    db.json_set("u:2", "$", r#"{"city":"ankara","n":7}"#)
        .unwrap();
    db.json_set("u:3", "$", r#"{"city":"izmir","n":7}"#)
        .unwrap();
    db.json_index_create("live_by_city", "u:", "$.city", Some(JsonIndexType::String))
        .unwrap();
    db.json_index_create("live_by_n", "u:", "$.n", Some(JsonIndexType::Number))
        .unwrap();
    assert!(
        db.json_index_list()
            .unwrap()
            .iter()
            .any(|line| line.starts_with("live_by_city"))
    );

    // The first build runs in the background; JSON.FIND errors until it finishes.
    let mut found = Vec::new();

    for _ in 0..200 {
        match db.json_find("live_by_city", "izmir", None, None) {
            Ok(keys) => {
                found = keys;
                break;
            }
            Err(Error::Server(message)) if message.contains("building") => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(other) => panic!("{other}"),
        }
    }

    assert_eq!(found, ["u:1", "u:3"]);
    assert_eq!(
        db.json_find("live_by_city", "izmir", Some(1), None)
            .unwrap(),
        ["u:1"]
    );
    assert_eq!(
        db.json_find("live_by_city", "izmir", None, Some("u:1"))
            .unwrap(),
        ["u:3"]
    );

    let mut by_number = Vec::new();

    for _ in 0..200 {
        if let Ok(keys) = db.json_find("live_by_n", "7", None, None) {
            by_number = keys;
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(by_number, ["u:2", "u:3"]);
    assert!(db.json_index_drop("live_by_city").unwrap());
    assert!(db.json_index_drop("live_by_n").unwrap());
    assert!(!db.json_index_drop("live_by_city").unwrap());
    db.flushdb().unwrap();
}

#[test]
fn admin_commands() {
    let Some(mut db) = connect(13) else { return };

    assert_eq!(db.wait(0, Duration::from_millis(10)).unwrap(), 0);
    assert!(db.command_count().unwrap() > 200);
    assert!(
        matches!(db.command_info(&["get"]).unwrap(), RespValue::Array(items) if items.len() == 1)
    );
    assert!(matches!(
        db.command_docs(&["get"]).unwrap(),
        RespValue::Array(_)
    ));
    assert!(matches!(db.command_list().unwrap(), RespValue::Array(items) if items.len() > 200));
    assert!(matches!(db.latency_latest().unwrap(), RespValue::Array(items) if items.is_empty()));
    assert_eq!(db.client_kill_addr("1.2.3.4:5", Some(true)).unwrap(), 0);
    assert_eq!(db.client_kill_addr("1.2.3.4:5", None).unwrap(), 0);
    db.rpush("l", &["a"]).unwrap();
    assert!(db.memory_usage_samples("l", 5).unwrap().unwrap() > 0);
    assert_eq!(db.memory_usage_samples("missing", 5).unwrap(), None);
    // Default user without a password: AUTH is refused, the connection stays usable.
    assert!(db.auth("wrong").is_err());
    assert!(db.auth_user("default", "wrong").is_err());
    assert_eq!(db.ping().unwrap(), "PONG");
}

#[test]
fn helpers_lock_lease_semaphore_limit() {
    let Some(mut db) = connect(14) else { return };
    let address = std::env::var("RUVIO_TEST_ADDR").unwrap();
    let mut other = Client::connect_addr(address.parse().unwrap(), 14).unwrap();

    other
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    {
        let mut guard = db
            .try_lock("lock", Duration::from_secs(10))
            .unwrap()
            .expect("lock");
        let token = guard.token().to_owned();

        assert_eq!(guard.key(), "lock");
        // The guard derefs to the client.
        assert_eq!(
            guard.get_string("lock").unwrap().as_deref(),
            Some(token.as_str())
        );
        assert!(
            other
                .try_lock("lock", Duration::from_secs(10))
                .unwrap()
                .is_none()
        );
        assert!(!other.release_lock("lock", "not-mine").unwrap());
        assert!(guard.renew(Duration::from_secs(20)).unwrap());
        assert!(guard.pttl("lock").unwrap() > 10_000);
    }

    // Dropped: released with DELEX IFEQ.
    assert_eq!(db.exists_key("lock").unwrap(), 0);

    // Release never deletes someone else's lock: the lock expires and another holder takes it.
    {
        let guard = db
            .try_lock("lock2", Duration::from_millis(50))
            .unwrap()
            .expect("lock");

        std::thread::sleep(Duration::from_millis(80));
        assert!(
            other
                .acquire_lock("lock2", "theirs", Duration::from_secs(10))
                .unwrap()
        );
        assert!(!guard.release().unwrap());
    }

    assert_eq!(db.get_string("lock2").unwrap().as_deref(), Some("theirs"));
    assert!(
        !db.renew_lock("lock2", "mine", Duration::from_secs(1))
            .unwrap()
    );
    assert!(
        other
            .renew_lock("lock2", "theirs", Duration::from_secs(1))
            .unwrap()
    );

    // lock() polls until the holder releases. The holder's 2 s lease outlives the 5 ms wait
    // by far, and the 10 s wait outlives the lease.
    other
        .acquire_lock("lock3", "theirs", Duration::from_secs(2))
        .unwrap();
    assert!(
        db.lock("lock3", Duration::from_secs(5), Duration::from_millis(5))
            .unwrap()
            .is_none()
    );
    assert!(
        db.lock("lock3", Duration::from_secs(5), Duration::from_secs(10))
            .unwrap()
            .is_some()
    );

    let ran = db
        .with_lock("lock4", Duration::from_secs(5), |client| {
            client.set("inside", "1")?;

            Ok::<_, Error>(7)
        })
        .unwrap();

    assert_eq!(ran, Some(7));
    assert_eq!(db.exists_key("lock4").unwrap(), 0);

    let first_fence;

    {
        let lease = db
            .lease_guard("lease", Duration::from_secs(10))
            .unwrap()
            .expect("lease");

        first_fence = lease.fence();
        assert!(
            other
                .lease_guard("lease", Duration::from_secs(10))
                .unwrap()
                .is_none()
        );
    }

    let lease = db
        .lease_guard("lease", Duration::from_secs(10))
        .unwrap()
        .expect("lease");

    assert!(lease.fence() > first_fence);
    assert!(lease.release().unwrap());

    {
        let first = other
            .semaphore_permit("sem", 1, Duration::from_secs(10))
            .unwrap()
            .expect("slot");

        assert!(first.token() > 0);
        assert!(
            db.semaphore_permit("sem", 1, Duration::from_secs(10))
                .unwrap()
                .is_none()
        );
    }

    assert!(
        db.semaphore_permit("sem", 1, Duration::from_secs(10))
            .unwrap()
            .is_some()
    );

    let first = db.rate_limit("rl", 2, Duration::from_secs(60)).unwrap();

    assert!(first.allowed);
    assert_eq!(first.remaining, 1);
    assert_eq!(
        db.rate_limit("rl", 2, Duration::from_secs(60))
            .unwrap()
            .remaining,
        0
    );

    let refused = db.rate_limit("rl", 2, Duration::from_secs(60)).unwrap();

    assert!(!refused.allowed);
}

#[test]
fn helpers_cache_idempotency_recent() {
    let Some(mut db) = connect(15) else { return };
    let mut calls = 0;

    let value = db
        .get_or_create("cache", Duration::from_secs(60), || {
            calls += 1;

            Ok::<_, Error>("built".to_owned())
        })
        .unwrap();

    assert_eq!(value, "built");
    assert_eq!(
        db.get_or_create(
            "cache",
            Duration::from_secs(60),
            || -> Result<String, Error> { panic!("must not rebuild") }
        )
        .unwrap(),
        "built"
    );
    assert_eq!(calls, 1);

    // Another caller is filling: BUSY is polled until the value appears.
    let address: SocketAddr = std::env::var("RUVIO_TEST_ADDR").unwrap().parse().unwrap();
    let mut filler = Client::connect_addr(address, 15).unwrap();

    filler
        .set_read_timeout(Some(Duration::from_secs(70)))
        .unwrap();
    let FillOutcome::Fill(token) = filler.fill_get("slow", Duration::from_secs(5)).unwrap() else {
        panic!("expected FILL");
    };
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        filler
            .fill_set("slow", "from-filler", Duration::from_secs(60), &token, None)
            .unwrap();
    });

    let waited = db
        .get_or_create(
            "slow",
            Duration::from_secs(60),
            || -> Result<String, Error> { panic!("the other caller fills") },
        )
        .unwrap();

    writer.join().unwrap();
    assert_eq!(waited, "from-filler");

    // FILLGET ... WAIT: the server holds a BUSY caller.
    let FillOutcome::Fill(_) = db.fill_get("held", Duration::from_secs(5)).unwrap() else {
        panic!("expected FILL");
    };
    assert_eq!(
        db.fill_get_wait("held", Duration::from_secs(5), Duration::from_millis(20))
            .unwrap(),
        FillOutcome::Busy
    );

    // A busy fill that never finishes times out.
    let timed_out = db.get_or_create_with(
        "held",
        CacheOptions {
            max_wait: Duration::from_millis(30),
            ..CacheOptions::new(Duration::from_secs(1))
        },
        || -> Result<String, Error> { panic!("busy") },
    );

    assert!(
        matches!(timed_out, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
    );

    // Stale-while-revalidate.
    db.get_or_create_with(
        "swr",
        CacheOptions {
            stale: Some(Duration::from_secs(60)),
            ..CacheOptions::new(Duration::from_millis(20))
        },
        || Ok::<_, Error>("v1".to_owned()),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(40));
    assert_eq!(
        db.get_or_create_with(
            "swr",
            CacheOptions {
                stale: Some(Duration::from_secs(60)),
                ..CacheOptions::new(Duration::from_secs(60))
            },
            || Ok::<_, Error>("v2".to_owned()),
        )
        .unwrap(),
        "v2"
    );
    assert_eq!(
        db.get_or_create("swr", Duration::from_secs(60), || Ok::<_, Error>(
            "v3".to_owned()
        ))
        .unwrap(),
        "v2"
    );

    // Idempotency.
    assert_eq!(
        db.idempotent("pay:1", "fp", "worker-a", Duration::from_secs(60), || Ok::<
            _,
            Error,
        >(
            b"ok:1".to_vec()
        ))
        .unwrap(),
        IdemOutcome::Executed(b"ok:1".to_vec())
    );
    assert_eq!(
        db.idempotent(
            "pay:1",
            "fp",
            "worker-b",
            Duration::from_secs(60),
            || -> Result<Vec<u8>, Error> { panic!("must not run twice") }
        )
        .unwrap(),
        IdemOutcome::Replayed(b"ok:1".to_vec())
    );
    assert_eq!(
        db.idempotent(
            "pay:1",
            "other",
            "worker-b",
            Duration::from_secs(60),
            || Ok::<_, Error>(Vec::new())
        )
        .unwrap(),
        IdemOutcome::Conflict
    );
    assert_eq!(
        db.idem_begin("pay:2", "fp", "worker-a", Duration::from_secs(60))
            .unwrap(),
        IdemState::Acquired
    );
    assert_eq!(
        db.idem_begin("pay:2", "fp", "worker-b", Duration::from_secs(60))
            .unwrap(),
        IdemState::Pending
    );
    assert_eq!(
        db.idempotent("pay:2", "fp", "worker-b", Duration::from_secs(60), || Ok::<
            _,
            Error,
        >(
            Vec::new()
        ))
        .unwrap(),
        IdemOutcome::Pending
    );
    assert!(
        db.idem_complete("pay:2", "fp", "worker-a", &[0, 1, 255])
            .unwrap()
    );
    assert_eq!(
        db.idem_begin("pay:2", "fp", "worker-c", Duration::from_secs(60))
            .unwrap(),
        IdemState::Completed(vec![0, 1, 255])
    );

    // Abort frees a pending record; a completed one is kept; another owner cannot abort.
    assert_eq!(
        db.idem_begin("pay:3", "fp", "worker-a", Duration::from_secs(60))
            .unwrap(),
        IdemState::Acquired
    );
    assert!(!db.idem_abort("pay:3", "fp", "worker-b").unwrap());
    assert!(!db.idem_abort("pay:3", "other", "worker-a").unwrap());
    assert!(db.idem_abort("pay:3", "fp", "worker-a").unwrap());
    assert_eq!(
        db.idem_begin("pay:3", "fp", "worker-b", Duration::from_secs(60))
            .unwrap(),
        IdemState::Acquired
    );
    assert!(
        db.idem_complete("pay:3", "fp", "worker-b", b"done")
            .unwrap()
    );
    assert!(!db.idem_abort("pay:3", "fp", "worker-b").unwrap());
    assert_eq!(
        db.idem_begin("pay:3", "fp", "worker-c", Duration::from_secs(60))
            .unwrap(),
        IdemState::Completed(b"done".to_vec())
    );

    // The helper with abort_on_error frees the key after a failing work function; the plain
    // helper leaves it pending.
    let failed = db.idempotent_abort_on_error(
        "pay:4",
        "fp",
        "worker-a",
        Duration::from_secs(60),
        || -> Result<Vec<u8>, Error> { Err(Error::Protocol("boom".into())) },
    );
    assert!(failed.is_err());
    assert_eq!(
        db.idempotent("pay:4", "fp", "worker-b", Duration::from_secs(60), || Ok::<
            _,
            Error,
        >(
            b"ok:4".to_vec()
        ))
        .unwrap(),
        IdemOutcome::Executed(b"ok:4".to_vec())
    );
    let failed = db.idempotent(
        "pay:5",
        "fp",
        "worker-a",
        Duration::from_secs(60),
        || -> Result<Vec<u8>, Error> { Err(Error::Protocol("boom".into())) },
    );
    assert!(failed.is_err());
    assert_eq!(
        db.idempotent("pay:5", "fp", "worker-b", Duration::from_secs(60), || Ok::<
            _,
            Error,
        >(
            Vec::new()
        ))
        .unwrap(),
        IdemOutcome::Pending
    );

    // Recent items.
    for item in ["a", "b", "c", "d"] {
        db.push_recent("recent", 3, item).unwrap();
    }

    assert_eq!(db.recent("recent", 10).unwrap(), ["d", "c", "b"]);
    assert_eq!(db.recent("recent", 2).unwrap(), ["d", "c"]);
    assert!(db.recent("recent", 0).unwrap().is_empty());
}

/// A deadline is set when the call creates the key and kept by later calls.
fn assert_deadline(db: &mut Client, key: &str, low_ms: i64, high_ms: i64) {
    let left = db.pttl(key).unwrap();

    assert!(
        (low_ms..=high_ms).contains(&left),
        "{key}: pttl {left} not in {low_ms}..={high_ms}"
    );
}

#[test]
fn ttl_on_create_sets_the_deadline_once() {
    let Some(mut db) = connect(1) else { return };
    let long = Duration::from_secs(1000);
    let short = Duration::from_secs(100);

    // Every command creates its key with the long deadline, then a second call with a short
    // one must leave the long deadline alone.
    assert_eq!(db.incr_with_ttl("incr", long).unwrap(), 1);
    assert_eq!(db.incr_with_ttl("incr", short).unwrap(), 2);
    assert_eq!(db.decr_with_ttl("decr", long).unwrap(), -1);
    assert_eq!(db.decr_with_ttl("decr", short).unwrap(), -2);
    assert_eq!(db.incr_by_with_ttl("incrby", 5, long).unwrap(), 5);
    assert_eq!(db.incr_by_with_ttl("incrby", 5, short).unwrap(), 10);
    assert_eq!(db.decr_by_with_ttl("decrby", 5, long).unwrap(), -5);
    assert_eq!(db.decr_by_with_ttl("decrby", 5, short).unwrap(), -10);
    assert_eq!(db.incr_by_max_with_ttl("cap", 1, 2, long).unwrap(), Some(1));
    assert_eq!(
        db.incr_by_max_with_ttl("cap", 1, 2, short).unwrap(),
        Some(2)
    );
    assert_eq!(db.incr_by_max_with_ttl("cap", 1, 2, short).unwrap(), None);
    assert_eq!(db.incr_by_float_with_ttl("float", 1.5, long).unwrap(), 1.5);
    assert_eq!(db.incr_by_float_with_ttl("float", 1.0, short).unwrap(), 2.5);
    assert_eq!(db.hincr_by_with_ttl("hash", "f", 1, long).unwrap(), 1);
    assert_eq!(db.hincr_by_with_ttl("hash", "f", 1, short).unwrap(), 2);
    assert_eq!(db.zincr_by_with_ttl("zset", "m", 1.0, long).unwrap(), 1.0);
    assert_eq!(db.zincr_by_with_ttl("zset", "m", 1.0, short).unwrap(), 2.0);
    assert!(!db.setbit_with_ttl("bits", 3, true, long).unwrap());
    assert!(db.setbit_with_ttl("bits", 3, true, short).unwrap());

    for key in [
        "incr", "decr", "incrby", "decrby", "cap", "float", "hash", "zset", "bits",
    ] {
        assert_deadline(&mut db, key, 900_000, 1_000_000);
    }

    // A millisecond TTL uses PX.
    db.incr_with_ttl("px", Duration::from_millis(500_500))
        .unwrap();
    assert_deadline(&mut db, "px", 400_000, 500_500);

    // A refused capped increment creates nothing.
    assert_eq!(db.incr_by_max_with_ttl("full", 5, 2, long).unwrap(), None);
    assert_eq!(db.exists(&["full"]).unwrap(), 0);

    // A key that already exists without a deadline gets none.
    db.set("plain", "1").unwrap();
    db.incr_with_ttl("plain", long).unwrap();
    assert_eq!(db.pttl("plain").unwrap(), -1);

    // The server refuses a zero TTL; the client refuses it before sending.
    assert!(matches!(
        db.incr_with_ttl("zero", Duration::ZERO),
        Err(Error::Protocol(_))
    ));
    assert_eq!(db.exists(&["zero"]).unwrap(), 0);
}

#[test]
fn ttl_on_create_for_zadd_geoadd_xadd_json_and_bloom() {
    let Some(mut db) = connect(2) else { return };
    let long = Duration::from_secs(1000);
    let short = Duration::from_secs(100);
    let options = |ttl| SortedSetAddOptions {
        ttl: Some(ttl),
        ..SortedSetAddOptions::default()
    };

    assert_eq!(db.zadd_with("z", options(long), &[("a", 1.0)]).unwrap(), 1);
    assert_eq!(db.zadd_with("z", options(short), &[("b", 2.0)]).unwrap(), 1);
    assert_eq!(
        db.geo_add_with("g", options(long), &[(13.36, 38.11, "p")])
            .unwrap(),
        1
    );
    assert_eq!(
        db.geo_add_with("g", options(short), &[(15.08, 37.5, "q")])
            .unwrap(),
        1
    );

    let id = db.xadd_with_ttl("s", long, "*", &[("f", "v")]).unwrap();
    assert!(id.contains('-'));
    db.xadd_with_ttl("s", short, "*", &[("f", "v")]).unwrap();
    let trim = StreamTrim::max_len(10);
    db.xadd_trim_with_ttl("st", long, &trim, "*", &[("f", "v")])
        .unwrap();
    db.xadd_trim_with_ttl("st", short, &trim, "*", &[("f", "v")])
        .unwrap();

    assert!(
        db.json_set_with_ttl("j", "$", "{\"a\":1}", JsonSetCondition::Always, long)
            .unwrap()
    );
    assert!(
        db.json_set_with_ttl("j", "$.b", "2", JsonSetCondition::Always, short)
            .unwrap()
    );
    assert!(
        !db.json_set_with_ttl("jnx", "$", "{}", JsonSetCondition::IfExists, long)
            .unwrap()
    );
    assert_eq!(db.exists(&["jnx"]).unwrap(), 0);

    let bloom = |ttl| BloomInsertOptions {
        ttl: Some(ttl),
        ..BloomInsertOptions::default()
    };
    assert_eq!(db.bf_insert("bf", &bloom(long), &["x"]).unwrap(), [true]);
    assert_eq!(db.bf_insert("bf", &bloom(short), &["y"]).unwrap(), [true]);

    for key in ["z", "g", "s", "st", "j", "bf"] {
        assert_deadline(&mut db, key, 900_000, 1_000_000);
    }
}

#[test]
fn expire_conditions() {
    let Some(mut db) = connect(3) else { return };
    let minute = Duration::from_secs(60);
    let hour = Duration::from_secs(3600);

    db.set("k", "v").unwrap();

    // No deadline counts as infinite: GT and XX fail, LT and NX apply.
    assert!(!db.expire_if("k", minute, ExpireCondition::Gt).unwrap());
    assert!(!db.expire_if("k", minute, ExpireCondition::Xx).unwrap());
    assert_eq!(db.pttl("k").unwrap(), -1);
    assert!(db.expire_if("k", hour, ExpireCondition::Nx).unwrap());
    assert!(!db.expire_if("k", minute, ExpireCondition::Nx).unwrap());
    assert!(db.expire_if("k", minute, ExpireCondition::Lt).unwrap());
    assert!(!db.expire_if("k", hour, ExpireCondition::Lt).unwrap());
    assert!(db.expire_if("k", hour, ExpireCondition::Gt).unwrap());
    assert!(
        db.pexpire_if("k", Duration::from_secs(7200), ExpireCondition::Xx)
            .unwrap()
    );
    assert!(!db.pexpire_if("k", hour, ExpireCondition::Gt).unwrap());
    assert_deadline(&mut db, "k", 7_100_000, 7_200_000);

    let now = std::time::SystemTime::now();
    let soon = now + Duration::from_secs(60);
    let later = now + Duration::from_secs(10_000) + Duration::from_millis(250);

    assert!(!db.expire_at_if("k", soon, ExpireCondition::Gt).unwrap());
    assert!(db.expire_at_if("k", soon, ExpireCondition::Lt).unwrap());
    assert!(db.pexpire_at_if("k", later, ExpireCondition::Gt).unwrap());
    assert!(!db.pexpire_at_if("k", soon, ExpireCondition::Gt).unwrap());
    assert!(db.expire_at_if("k", later, ExpireCondition::Xx).unwrap());
    assert_deadline(&mut db, "k", 9_000_000, 10_300_000);

    // A missing key replies false for every condition.
    assert!(!db.expire_if("none", minute, ExpireCondition::Nx).unwrap());
}

#[test]
fn limit_sliding_and_tokens_modes() {
    let Some(mut db) = connect(7) else { return };

    assert_eq!(db.limit_sliding("rl:s", 2, 60_000).unwrap(), Some(1));
    assert_eq!(db.limit_sliding("rl:s", 2, 60_000).unwrap(), Some(0));
    let refused = db.limit_sliding_with_retry("rl:s", 2, 60_000).unwrap();
    assert!(!refused.allowed && refused.remaining == 0 && refused.retry_after_ms > 0);
    // A key keeps the mode it was created with.
    assert!(db.limit_tokens("rl:s", 2, 60_000, 1).is_err());

    assert_eq!(db.limit_tokens("rl:t", 5, 60_000, 2).unwrap(), Some(3));
    let taken = db.limit_tokens_with_retry("rl:t", 5, 60_000, 3).unwrap();
    assert!(taken.allowed && taken.remaining == 0 && taken.retry_after_ms == 0);
    assert_eq!(db.limit_tokens("rl:t", 5, 60_000, 1).unwrap(), None);
}

#[test]
fn transfer_and_htransfer_move_amounts_atomically() {
    let Some(mut db) = connect(6) else { return };

    db.set("{w}:a", "10").unwrap();
    assert_eq!(
        db.transfer("{w}:a", "{w}:b", 4, None).unwrap(),
        Some((6, 4))
    );
    assert_eq!(db.transfer("{w}:a", "{w}:b", 7, None).unwrap(), None);
    assert_eq!(db.transfer("{w}:a", "{w}:b", 3, Some(6)).unwrap(), None);
    assert_eq!(
        db.transfer("{w}:a", "{w}:b", 2, Some(6)).unwrap(),
        Some((4, 6))
    );
    assert_eq!(db.transfer("{w}:none", "{w}:b", 1, None).unwrap(), None);
    assert_eq!(db.get("{w}:a").unwrap().as_deref(), Some(&b"4"[..]));
    assert!(db.transfer("{w}:a", "{w}:a", 1, None).is_err());
    assert!(db.transfer("{w}:a", "{w}:b", 0, None).is_err());

    db.hset("h", &[("f", "10")]).unwrap();
    assert_eq!(db.htransfer("h", "f", "g", 4, None).unwrap(), Some((6, 4)));
    assert_eq!(db.htransfer("h", "f", "g", 7, None).unwrap(), None);
    assert_eq!(db.htransfer("h", "f", "g", 2, Some(5)).unwrap(), None);
    assert_eq!(db.hget("h", "g").unwrap().as_deref(), Some("4"));
}
