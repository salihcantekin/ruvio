//! Runs against a live server when `RUVIO_TEST_ADDR` is set, for example
//! `RUVIO_TEST_ADDR=127.0.0.1:16390 cargo test`. Each test uses its own database.

mod common;

use std::net::SocketAddr;

use std::time::{Duration, SystemTime};

use ruvio_client::{
    BitFieldOffset, BitFieldOp, BitFieldOverflow, BitFieldType, Client, FieldExpireCondition,
    FillOutcome, GeoUnit, InsertPosition, ListSide, SetMode, SortedSetAddOptions, SortedSetEntry,
    StreamClaimOptions, StreamPendingFilter, StreamReadOptions,
};

fn connect(database: u32) -> Option<Client> {
    let address: SocketAddr = std::env::var("RUVIO_TEST_ADDR").ok()?.parse().ok()?;

    common::claim_database(database);

    let mut client = Client::connect_addr(address, database).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    client.flushdb().unwrap();

    Some(client)
}

#[test]
fn strings_keys_and_expiry() {
    let Some(mut db) = connect(1) else { return };

    assert_eq!(db.database(), 1);
    assert_eq!(db.ping_message("hi").unwrap(), "hi");
    db.mset(&[("a", "1"), ("b", "2")]).unwrap();
    assert_eq!(
        db.mget(&["a", "missing", "b"]).unwrap(),
        vec![Some("1".to_owned()), None, Some("2".to_owned())]
    );
    assert_eq!(db.getset("a", "10").unwrap().as_deref(), Some("1"));
    assert_eq!(db.append("a", "0").unwrap(), 3);
    assert_eq!(db.strlen("a").unwrap(), 3);
    assert_eq!(db.incr_by("n", 5).unwrap(), 5);
    assert_eq!(db.decr_by("n", 2).unwrap(), 3);
    assert_eq!(db.decr("n").unwrap(), 2);
    assert_eq!(db.exists(&["a", "b", "zzz"]).unwrap(), 2);
    assert_eq!(db.key_type("a").unwrap(), "string");
    db.rename("b", "c").unwrap();
    assert_eq!(db.unlink(&["c"]).unwrap(), 1);
    assert!(!db.set_with("a", "x", None, SetMode::IfNotExists).unwrap());
    assert_eq!(
        db.set_and_get("a", "y", None, SetMode::Always)
            .unwrap()
            .as_deref(),
        Some("100")
    );
    assert!(db.expire("a", Duration::from_millis(60_500)).unwrap());
    assert!(db.pttl("a").unwrap() > 0);
    assert!(db.set_keep_ttl("a", "z", SetMode::Always).unwrap());
    assert!(db.ttl("a").unwrap() > 0);
    assert!(db.persist("a").unwrap());
    assert_eq!(db.ttl("a").unwrap(), -1);

    let later = SystemTime::now() + Duration::from_secs(60);

    assert!(db.set_expires_at("t", "v", later, SetMode::Always).unwrap());
    assert!(db.expire_at("a", later).unwrap());
    assert!(db.pexpire("t", Duration::from_secs(30)).unwrap());
    assert_eq!(db.dbsize().unwrap(), 3);

    let page = db.scan(0, Some("*"), Some(100), None).unwrap();

    assert_eq!(page.cursor, 0);
    assert_eq!(page.keys.len(), 3);
}

#[test]
fn lists_sets_and_hashes() {
    let Some(mut db) = connect(2) else { return };

    assert_eq!(db.rpush("l", &["a", "b", "c", "d"]).unwrap(), 4);
    assert_eq!(db.lpush("l", &["z"]).unwrap(), 5);
    assert_eq!(db.lpop("l").unwrap().as_deref(), Some("z"));
    assert_eq!(db.rpop_count("l", 2).unwrap(), vec!["d", "c"]);
    assert_eq!(db.llen("l").unwrap(), 2);
    assert_eq!(db.lindex("l", -1).unwrap().as_deref(), Some("b"));
    db.ltrim("l", 0, 0).unwrap();
    assert_eq!(db.lrange("l", 0, -1).unwrap(), vec!["a"]);
    assert_eq!(
        db.blpop(&["l"], Duration::from_secs(1)).unwrap(),
        Some(("l".to_owned(), "a".to_owned()))
    );
    assert_eq!(db.lpop_count("l", 3).unwrap(), Vec::<String>::new());

    assert_eq!(db.sadd("s1", &["a", "b", "c"]).unwrap(), 3);
    assert_eq!(db.sadd("s2", &["b", "c", "d"]).unwrap(), 3);
    assert!(db.sismember("s1", "a").unwrap());
    assert_eq!(db.scard("s1").unwrap(), 3);
    assert_eq!(sorted(db.sinter(&["s1", "s2"]).unwrap()), vec!["b", "c"]);
    assert_eq!(db.sunion(&["s1", "s2"]).unwrap().len(), 4);
    assert_eq!(db.sdiff(&["s1", "s2"]).unwrap(), vec!["a"]);
    assert_eq!(db.sinterstore("s3", &["s1", "s2"]).unwrap(), 2);
    assert!(db.smove("s1", "s3", "a").unwrap());
    assert_eq!(db.srem("s3", &["a"]).unwrap(), 1);
    assert_eq!(db.srandmember_count("s3", 5).unwrap().len(), 2);
    assert_eq!(db.spop_count("s3", 5).unwrap().len(), 2);
    assert_eq!(db.spop("s3").unwrap(), None);

    assert_eq!(db.hset("h", &[("f1", "1"), ("f2", "two")]).unwrap(), 2);
    assert_eq!(db.hget("h", "f2").unwrap().as_deref(), Some("two"));
    assert_eq!(db.hincr_by("h", "f1", 4).unwrap(), 5);
    assert!(!db.hsetnx("h", "f1", "9").unwrap());
    db.hmset("hm", &[("a", "1"), ("b", "2")]).unwrap();
    assert_eq!(db.hget("hm", "b").unwrap().as_deref(), Some("2"));
    assert_eq!(db.hstrlen("h", "f2").unwrap(), 3);
    assert_eq!(db.hlen("h").unwrap(), 2);
    assert!(db.hexists("h", "f1").unwrap());
    assert_eq!(sorted(db.hkeys("h").unwrap()), vec!["f1", "f2"]);
    assert_eq!(db.hvals("h").unwrap().len(), 2);
    assert_eq!(db.hgetall("h").unwrap()["f1"], "5");
    assert_eq!(
        db.hmget("h", &["f2", "nope"]).unwrap(),
        vec![Some("two".to_owned()), None]
    );

    let page = db.hscan("h", 0, Some("f*"), None).unwrap();

    assert_eq!(page.cursor, 0);
    assert_eq!(page.fields.len(), 2);
    assert_eq!(db.hdel("h", &["f1", "f2"]).unwrap(), 2);
}

#[test]
fn geo_commands_round_trip_the_sicily_members() {
    let Some(mut db) = connect(9) else { return };

    assert_eq!(
        db.geo_add(
            "Sicily",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania")
            ]
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.geo_hash("Sicily", &["Palermo", "missing"]).unwrap()[0].as_deref(),
        Some("sqc8b49rny0")
    );
    assert!(
        db.geo_dist("Sicily", "Palermo", "Catania", Some(GeoUnit::Meters))
            .unwrap()
            .unwrap()
            > 100_000.0
    );
    assert!(db.geo_pos("Sicily", &["missing"]).unwrap()[0].is_none());
    assert_eq!(
        db.geo_search("Sicily", 15.0, 37.0, 200.0, GeoUnit::Kilometers)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.geo_search_store("near", "Sicily", "Palermo", 100.0, GeoUnit::Kilometers)
            .unwrap(),
        1
    );
    assert_eq!(
        db.geo_radius("Sicily", 15.0, 37.0, 200.0, GeoUnit::Kilometers)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.geo_radius_by_member("Sicily", "Palermo", 100.0, GeoUnit::Kilometers)
            .unwrap(),
        vec!["Palermo".to_owned()]
    );
}

#[test]
fn sorted_sets_and_bloom_filters() {
    let Some(mut db) = connect(3) else { return };

    assert_eq!(db.zadd("z", &[("ada", 100.0), ("bob", 50.0)]).unwrap(), 2);

    let only_greater = SortedSetAddOptions {
        greater_than: true,
        changed: true,
        ..SortedSetAddOptions::default()
    };

    assert_eq!(
        db.zadd_with("z", only_greater, &[("bob", 10.0)]).unwrap(),
        0
    );
    assert_eq!(db.zincr_by("z", "bob", 5.5).unwrap(), 55.5);
    assert_eq!(
        db.zadd_incr("z", "cy", 1.0, SortedSetAddOptions::default())
            .unwrap(),
        Some(1.0)
    );

    let only_existing = SortedSetAddOptions {
        if_exists: true,
        ..SortedSetAddOptions::default()
    };

    assert_eq!(db.zadd_incr("z", "new", 1.0, only_existing).unwrap(), None);
    assert_eq!(db.zrange("z", 0, -1).unwrap(), vec!["cy", "bob", "ada"]);
    assert_eq!(db.zrevrange("z", 0, 0).unwrap(), vec!["ada"]);
    assert_eq!(
        db.zrange_with_scores("z", 0, 0).unwrap(),
        vec![SortedSetEntry::new("cy", 1.0)]
    );
    assert_eq!(
        db.zrevrange_with_scores("z", 0, 0).unwrap(),
        vec![SortedSetEntry::from(("ada", 100.0))]
    );
    assert_eq!(
        db.zrange_by_score_with_scores("z", "(1", "+inf")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(db.zscore("z", "ada").unwrap(), Some(100.0));
    assert_eq!(db.zscore("z", "nope").unwrap(), None);
    assert_eq!(db.zrank("z", "ada").unwrap(), Some(2));
    assert_eq!(db.zrevrank("z", "ada").unwrap(), Some(0));
    assert_eq!(db.zrank("z", "nope").unwrap(), None);
    assert_eq!(db.zrem("z", &["cy"]).unwrap(), 1);
    assert_eq!(db.zcard("z").unwrap(), 2);

    db.bf_reserve("bf", 0.01, 1000).unwrap();
    assert!(db.bf_add("bf", "x").unwrap());
    assert!(db.bf_exists("bf", "x").unwrap());
    assert!(!db.bf_exists("bf", "y").unwrap());
}

#[test]
fn hash_field_ttls_and_sorted_set_pops() {
    let Some(mut db) = connect(10) else { return };

    db.hset("h", &[("a", "1"), ("b", "2"), ("c", "3")]).unwrap();
    assert_eq!(
        db.hexpire("h", Duration::from_secs(100), &["a", "x"])
            .unwrap(),
        vec![1, -2]
    );
    assert_eq!(
        db.hexpire_with(
            "h",
            Duration::from_secs(50),
            FieldExpireCondition::IfGreater,
            &["a"]
        )
        .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hexpire_with(
            "h",
            Duration::from_secs(9),
            FieldExpireCondition::IfNoTtl,
            &["a", "b"]
        )
        .unwrap(),
        vec![0, 1]
    );

    let ttls = db.httl("h", &["a", "b", "c", "x"]).unwrap();

    assert!(ttls[0] > 90 && ttls[0] <= 100, "{ttls:?}");
    assert_eq!(&ttls[2..], &[-1, -2]);
    assert!(db.hpttl("h", &["b"]).unwrap()[0] <= 9_000);

    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800);

    assert_eq!(db.hexpire_at("h", at, &["c"]).unwrap(), vec![1]);
    assert_eq!(db.hexpiretime("h", &["c"]).unwrap(), vec![4_102_444_800]);
    assert_eq!(
        db.hpexpiretime("h", &["c"]).unwrap(),
        vec![4_102_444_800_000]
    );
    assert_eq!(db.hpersist("h", &["c", "c", "x"]).unwrap(), vec![1, -1, -2]);
    assert_eq!(
        db.hpexpire("h", Duration::from_millis(30), &["b"]).unwrap(),
        vec![1]
    );
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(db.hget("h", "b").unwrap(), None);
    assert_eq!(
        db.hexpire_at("h", SystemTime::UNIX_EPOCH + Duration::from_secs(1), &["a"])
            .unwrap(),
        vec![2]
    );
    assert_eq!(db.hgetall("h").unwrap().len(), 1);

    db.zadd("z", &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)])
        .unwrap();
    assert_eq!(
        db.zpopmin("z", 1).unwrap(),
        vec![SortedSetEntry::new("a", 1.0)]
    );
    assert_eq!(
        db.zpopmax("z", 1).unwrap(),
        vec![SortedSetEntry::new("d", 4.0)]
    );
    assert_eq!(
        db.bzpopmin(&["none", "z"], Duration::from_secs(1)).unwrap(),
        Some(("z".to_owned(), SortedSetEntry::new("b", 2.0)))
    );
    assert_eq!(
        db.bzpopmax(&["z"], Duration::from_secs(1)).unwrap(),
        Some(("z".to_owned(), SortedSetEntry::new("c", 3.0)))
    );
    assert_eq!(db.bzpopmin(&["z"], Duration::from_secs(1)).unwrap(), None);

    db.zadd("due", &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 9.0)])
        .unwrap();
    assert_eq!(
        db.zpopbyscore("due", "-inf", "1", 5).unwrap(),
        vec![SortedSetEntry::new("a", 1.0)]
    );
    assert_eq!(db.zmovedue("due", "ready", "3", 5).unwrap(), 2);
    assert_eq!(db.lrange("ready", 0, -1).unwrap(), ["b", "c"]);
    assert!(db.zpopbyscore("due", "-inf", "3", 5).unwrap().is_empty());

    db.zadd("r", &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("x", 0.0)])
        .unwrap();
    assert_eq!(db.zremrangebyrank("r", 0, 0).unwrap(), 1);
    assert_eq!(db.zremrangebyscore("r", "(1", "2").unwrap(), 1);
    assert_eq!(db.zremrangebylex("r", "-", "+").unwrap(), 2);
    assert_eq!(db.zcard("r").unwrap(), 0);
}

#[test]
fn semaphores_and_capped_increments() {
    let Some(mut db) = connect(11) else { return };

    let first = db.semaphore("jobs", 2, 60_000).unwrap().unwrap();
    let second = db.semaphore("jobs", 2, 60_000).unwrap().unwrap();

    assert!(second > first, "tokens only grow");
    assert_eq!(db.semaphore("jobs", 2, 60_000).unwrap(), None);
    assert_eq!(db.key_type("jobs").unwrap(), "semaphore");
    assert_eq!(db.renew("jobs", first as u64, 120_000).unwrap(), 1);
    assert_eq!(db.renew("jobs", 1, 120_000).unwrap(), 0);
    assert_eq!(
        db.semaphore("jobs", 2, 60_000).unwrap(),
        None,
        "renew takes no slot"
    );
    assert_eq!(db.release("jobs", first as u64).unwrap(), 1);
    assert_eq!(db.release("jobs", first as u64).unwrap(), 0);
    assert_eq!(db.renew("jobs", first as u64, 120_000).unwrap(), 0);
    assert!(db.semaphore("jobs", 2, 60_000).unwrap().is_some());
    assert!(db.semaphore("short", 1, 30).unwrap().is_some());
    std::thread::sleep(Duration::from_millis(80));
    assert!(
        db.semaphore("short", 1, 30).unwrap().is_some(),
        "an expired holder frees its slot"
    );

    assert_eq!(db.incr_by_max("seats", 3, 4).unwrap(), Some(3));
    assert_eq!(db.incr_by_max("seats", 2, 4).unwrap(), None);
    assert_eq!(db.get_string("seats").unwrap().as_deref(), Some("3"));
    assert_eq!(db.incr_by_max("seats", 1, 4).unwrap(), Some(4));
}

#[test]
fn streams_and_consumer_groups() {
    let Some(mut db) = connect(4) else { return };

    let first = db.xadd("s", "*", &[("f", "1")]).unwrap();

    db.xadd("s", "*", &[("f", "2")]).unwrap();
    db.xadd_maxlen("s", 10, "*", &[("f", "3")]).unwrap();
    assert_eq!(db.xlen("s").unwrap(), 3);
    assert_eq!(db.xrange("s", "-", "+", None).unwrap()[0].id, first);
    assert_eq!(
        db.xrevrange("s", "+", "-", Some(1)).unwrap()[0].fields[0].1,
        "3"
    );

    let read = db
        .xread(
            &StreamReadOptions {
                count: Some(2),
                ..StreamReadOptions::default()
            },
            &[("s", "0")],
        )
        .unwrap();

    assert_eq!(read[0].key, "s");
    assert_eq!(read[0].entries.len(), 2);
    db.xgroup_create("s", "g", "0", false).unwrap();
    db.xgroup_create("fresh", "g", "$", true).unwrap();

    let delivered = db
        .xreadgroup("g", "c1", &StreamReadOptions::default(), &[("s", ">")])
        .unwrap();

    assert_eq!(delivered[0].entries.len(), 3);

    let pending = db
        .xpending("s", "g", "-", "+", 10, &StreamPendingFilter::default())
        .unwrap();

    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].consumer, "c1");
    assert!(db.xpending_summary("s", "g").unwrap().as_array().is_some());

    let claimed = db
        .xclaim(
            "s",
            "g",
            "c2",
            Duration::ZERO,
            &[&first],
            &StreamClaimOptions::default(),
        )
        .unwrap();

    assert_eq!(claimed[0].id, first);
    assert_eq!(
        db.xclaim_ids(
            "s",
            "g",
            "c2",
            Duration::ZERO,
            &[&first],
            &StreamClaimOptions::default()
        )
        .unwrap(),
        vec![first.clone()]
    );
    assert_eq!(db.xack("s", "g", &[&first]).unwrap(), 1);
    assert_eq!(db.xgroup_delconsumer("s", "g", "c1").unwrap(), 2);
    db.xgroup_setid("s", "g", "0").unwrap();
    assert!(db.xgroup_destroy("s", "g").unwrap());
    assert_eq!(db.xdel("s", &[&first]).unwrap(), 1);
    assert_eq!(db.xtrim_maxlen("s", 1).unwrap(), 1);
}

#[test]
fn databases_transactions_and_scripts() {
    let Some(mut db) = connect(5) else { return };

    db.set("k", "v").unwrap();
    common::claim_database(6);
    assert!(db.move_key("k", 6).unwrap());

    let mut other = Client::connect_database("127.0.0.1", port(), 0)
        .unwrap()
        .into_database(6)
        .unwrap();

    other
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    assert_eq!(other.get_string("k").unwrap().as_deref(), Some("v"));
    other.flushdb().unwrap();

    db.watch(&["w"]).unwrap();
    db.multi().unwrap();
    db.execute(&["SET", "w", "1"]).unwrap();
    db.execute(&["INCR", "w"]).unwrap();

    let replies = db.exec().unwrap().unwrap();

    assert_eq!(replies[1].as_integer(), Some(2));
    db.unwatch().unwrap();
    db.multi().unwrap();
    db.discard().unwrap();

    assert_eq!(
        db.eval("return ARGV[1]", &[], &["hi"])
            .unwrap()
            .as_string()
            .unwrap()
            .as_deref(),
        Some("hi")
    );

    let sha = db.script_load("return ruvio.call('GET', KEYS[1])").unwrap();

    assert_eq!(
        db.script_exists(&[&sha, "0000"]).unwrap(),
        vec![true, false]
    );
    assert_eq!(
        db.evalsha(&sha, &["w"], &[])
            .unwrap()
            .as_string()
            .unwrap()
            .as_deref(),
        Some("2")
    );
    db.function_load("echo_first", "return ARGV[1]").unwrap();
    assert_eq!(
        db.fcall("echo_first", &[], &["x"])
            .unwrap()
            .as_string()
            .unwrap()
            .as_deref(),
        Some("x")
    );
    assert!(db.function_list().unwrap().as_array().is_some());
    assert!(db.function_delete("echo_first").unwrap());
    assert!(!db.function_delete("echo_first").unwrap());
    db.script_flush().unwrap();
}

#[test]
fn server_commands_and_pubsub() {
    let Some(mut db) = connect(7) else { return };

    assert!(db.info(None).unwrap().contains("ruvio"));
    assert!(!db.info(Some("server")).unwrap().is_empty());
    assert!(!db.config_get("databases").unwrap().is_empty());
    db.config_set(&[("slowlog-max-len", "128")]).unwrap();
    assert_eq!(
        db.config_get("slowlog-max-len").unwrap(),
        [("slowlog-max-len".to_owned(), "128".to_owned())]
    );
    assert!(matches!(
        db.config_set(&[("shards", "2")]),
        Err(ruvio_client::Error::Server(_))
    ));
    match db.role().unwrap() {
        ruvio_client::RespValue::Array(items) => {
            assert_eq!(items[0], ruvio_client::RespValue::Bulk(b"master".to_vec()));
        }
        other => panic!("ROLE answered {other:?}"),
    }
    // The live server keeps no WAL, so it cannot replicate: both forms are refused unchanged.
    assert!(matches!(
        db.replicaof("127.0.0.1", 1),
        Err(ruvio_client::Error::Server(_))
    ));
    assert!(matches!(
        db.replicaof_no_one(false),
        Err(ruvio_client::Error::Server(_))
    ));
    assert_eq!(db.acl_whoami().unwrap(), "default");
    assert!(db.acl_users().unwrap().contains(&"default".to_owned()));
    assert!(!db.acl_list().unwrap().is_empty());
    assert!(!db.acl_cat().unwrap().is_empty());
    assert!(!db.acl_getuser("default").unwrap().is_null());
    db.client_setname("rust-live").unwrap();
    assert_eq!(db.client_getname().unwrap().as_deref(), Some("rust-live"));
    assert!(!matches!(
        db.cluster_nodes(),
        Err(ruvio_client::Error::Io(_))
    ));
    assert!(!matches!(
        db.cluster_slots(),
        Err(ruvio_client::Error::Io(_))
    ));

    let mut subscriber = connect(0).unwrap();

    subscriber.subscribe(&["news"]).unwrap();
    assert_eq!(subscriber.ping().unwrap(), "PONG");
    subscriber.psubscribe(&["ne*"]).unwrap();
    subscriber
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(db.publish("news", "hello").unwrap(), 2);

    let exact = subscriber.next_message().unwrap();
    let pattern = subscriber.next_message().unwrap();

    assert_eq!(exact.kind, "message");
    assert_eq!(exact.payload, b"hello");
    assert_eq!(pattern.pattern.as_deref(), Some("ne*"));
    subscriber.unsubscribe(&[]).unwrap();
    subscriber.punsubscribe(&[]).unwrap();
    subscriber.ssubscribe(&["{s}chan"]).unwrap();
    assert_eq!(db.spublish("{s}chan", "sharded").unwrap(), 1);
    assert_eq!(subscriber.next_message().unwrap().kind, "smessage");
    subscriber.sunsubscribe(&[]).unwrap();
    assert_eq!(subscriber.ping().unwrap(), "PONG");
}

fn port() -> u16 {
    std::env::var("RUVIO_TEST_ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .unwrap()
        .port()
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();

    values
}

#[test]
fn compatibility_commands_and_fill() {
    let Some(mut db) = connect(12) else { return };

    db.mset(&[("k1", "1"), ("k2", "2")]).unwrap();
    assert_eq!(db.keys("k*").unwrap().len(), 2);
    assert!(db.random_key().unwrap().is_some());
    assert_eq!(db.touch(&["k1", "nope"]).unwrap(), 1);
    assert!(!db.rename_nx("k1", "k2").unwrap());
    assert!(db.copy("k1", "k3", false).unwrap());
    assert!(!db.copy("k1", "k3", false).unwrap());
    assert_eq!(db.echo("hi").unwrap(), "hi");
    assert_eq!(db.expire_time("k1").unwrap(), -1);
    assert!(db.object_encoding("k1").unwrap().is_some());
    assert!(db.memory_usage("k1").unwrap().unwrap() > 0);
    assert_eq!(db.memory_usage("nope").unwrap(), None);
    assert!(db.client_id().unwrap() > 0);
    assert!(db.client_list().unwrap().contains("id="));
    db.slowlog_reset().unwrap();
    assert_eq!(db.slowlog_len().unwrap(), 0);

    assert_eq!(db.rpush("l", &["a", "b", "c"]).unwrap(), 3);
    assert_eq!(
        db.linsert("l", InsertPosition::Before, "b", "x").unwrap(),
        4
    );
    db.lset("l", 0, "A").unwrap();
    assert_eq!(db.lrem("l", 0, "x").unwrap(), 1);
    assert_eq!(db.lpos("l", "c").unwrap(), Some(2));
    assert_eq!(db.lpushx("nolist", &["a"]).unwrap(), 0);
    assert_eq!(db.rpushx("l", &["d"]).unwrap(), 4);
    assert_eq!(
        db.lmove("l", "l2", ListSide::Left, ListSide::Right)
            .unwrap()
            .as_deref(),
        Some("A")
    );
    assert_eq!(db.rpoplpush("l", "l2").unwrap().as_deref(), Some("d"));
    assert_eq!(
        db.blmove(
            "l",
            "l2",
            ListSide::Left,
            ListSide::Left,
            Duration::from_secs(1)
        )
        .unwrap()
        .as_deref(),
        Some("b")
    );
    let popped = db
        .lmpop(&["none", "l"], ListSide::Left, 5)
        .unwrap()
        .unwrap();
    assert_eq!(popped, ("l".to_owned(), vec!["c".to_owned()]));
    assert_eq!(
        db.blmove(
            "none",
            "l2",
            ListSide::Left,
            ListSide::Left,
            Duration::from_millis(100)
        )
        .unwrap(),
        None
    );

    let u8_type = BitFieldType::unsigned(8).unwrap();

    assert_eq!(
        db.bitfield(
            "bf",
            &[
                BitFieldOp::Set {
                    ty: u8_type,
                    offset: BitFieldOffset::Bit(0),
                    value: 200,
                },
                BitFieldOp::Get {
                    ty: u8_type,
                    offset: BitFieldOffset::Bit(0),
                },
            ],
        )
        .unwrap(),
        vec![Some(0), Some(200)]
    );
    assert_eq!(
        db.bitfield(
            "bf",
            &[
                BitFieldOp::Overflow(BitFieldOverflow::Fail),
                BitFieldOp::IncrBy {
                    ty: u8_type,
                    offset: BitFieldOffset::Bit(0),
                    delta: 100,
                },
            ],
        )
        .unwrap(),
        vec![None]
    );
    assert_eq!(
        db.bitfield_ro(
            "bf",
            &[BitFieldOp::Get {
                ty: BitFieldType::signed(8).unwrap(),
                offset: BitFieldOffset::Bit(0)
            }]
        )
        .unwrap(),
        vec![Some(-56)]
    );

    let token = match db.fill_get("cache", Duration::from_secs(5)).unwrap() {
        FillOutcome::Fill(token) => token,
        other => panic!("expected FILL, got {other:?}"),
    };
    assert_eq!(
        db.fill_get("cache", Duration::from_secs(5)).unwrap(),
        FillOutcome::Busy
    );
    assert!(
        !db.fill_set("cache", "v", Duration::from_secs(10), "999999", None)
            .unwrap()
    );
    assert!(
        db.fill_set("cache", "v", Duration::from_secs(10), &token, None)
            .unwrap()
    );
    assert_eq!(
        db.fill_get("cache", Duration::from_secs(5)).unwrap(),
        FillOutcome::Hit("v".to_owned())
    );
}

#[test]
fn json_documents_round_trip() {
    let Some(mut db) = connect(13) else { return };

    assert!(db.json_set("{j}a", "$", r#"{"n":1}"#).unwrap());
    assert_eq!(
        db.json_mget(&["{j}a", "{j}none"], "$.n").unwrap(),
        vec![Some("1".to_owned()), None]
    );

    assert!(
        db.json_set("doc", "$", r#"{"a":1,"meta":{"v":"x"}}"#)
            .unwrap()
    );
    assert!(!db.json_set("doc", "$.nope.v", "1").unwrap());
    assert!(
        db.json_merge("doc", "$.meta", r#"{"v":null,"w":2}"#)
            .unwrap()
    );
    assert_eq!(
        db.json_get("doc", "$").unwrap().as_deref(),
        Some(r#"{"a":1,"meta":{"w":2}}"#)
    );
    assert_eq!(db.json_get("doc", "$.zz").unwrap(), None);
    assert_eq!(db.json_del("doc", "$.a").unwrap(), 1);
    assert_eq!(db.json_del("doc", "$.a").unwrap(), 0);
    assert_eq!(db.key_type("doc").unwrap(), "json");
}

#[test]
fn json_value_commands_round_trip() {
    let Some(mut db) = connect(14) else { return };

    assert!(
        db.json_set(
            "d",
            "$",
            r#"{"n":5,"s":"hello","l":[1],"o":{"b":1},"t":false}"#
        )
        .unwrap()
    );
    assert_eq!(db.json_type("d", ".n").unwrap().as_deref(), Some("integer"));
    assert_eq!(db.json_strlen("d", "$.s").unwrap(), Some(5));
    assert_eq!(db.json_arrlen("d", "$.zz").unwrap(), None);
    assert_eq!(db.json_objkeys("d", "$.o").unwrap(), vec!["b".to_owned()]);
    assert_eq!(
        db.json_numincrby("d", "$.n", 2.5).unwrap().as_deref(),
        Some("7.5")
    );
    assert_eq!(db.json_toggle("d", "$.t").unwrap(), Some(true));
    assert_eq!(db.json_arrappend("d", "$.l", "2").unwrap(), Some(2));
    assert_eq!(db.json_clear("d", "$.o").unwrap(), 1);
    assert_eq!(
        db.json_get("d", "$").unwrap().as_deref(),
        Some(r#"{"n":7.5,"s":"hello","l":[1,2],"o":{},"t":true}"#)
    );
}

#[cfg(feature = "serde")]
#[test]
fn json_typed_values_round_trip() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct Address {
        city_name: String,
        zip: u32,
    }

    let Some(mut db) = connect(15) else { return };

    assert!(
        db.json_set_value("doc", "$", &serde_json::json!({"l": []}))
            .unwrap()
    );
    assert!(db.json_set_value("doc", "$.city", "izmir").unwrap());
    assert_eq!(
        db.json_get("doc", "$.city").unwrap().as_deref(),
        Some(r#""izmir""#)
    );
    assert_eq!(
        db.json_get_value::<String>("doc", "$.city")
            .unwrap()
            .as_deref(),
        Some("izmir")
    );

    let address = Address {
        city_name: "Izmir".into(),
        zip: 35000,
    };
    assert!(db.json_set_value("doc", "$.addr", &address).unwrap());
    assert_eq!(
        db.json_get("doc", "$.addr").unwrap().as_deref(),
        Some(r#"{"cityName":"Izmir","zip":35000}"#)
    );
    assert_eq!(
        db.json_get_value::<Address>("doc", "$.addr").unwrap(),
        Some(address)
    );
    assert_eq!(db.json_get_value::<Address>("doc", "$.zz").unwrap(), None);

    assert!(
        db.json_merge_value("doc", "$", &serde_json::json!({"city": null, "n": 1}))
            .unwrap()
    );
    assert_eq!(
        db.json_arrappend_value("doc", "$.l", "a\"b").unwrap(),
        Some(1)
    );
    assert_eq!(
        db.json_get_value::<Vec<String>>("doc", "$.l").unwrap(),
        Some(vec!["a\"b".to_owned()])
    );
}

#[test]
fn a_blocking_command_waits_its_own_timeout_on_top_of_the_read_timeout() {
    let Some(mut db) = connect(8) else { return };

    db.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    // BLPOP k 1 on an empty list answers nil after 1 s, well past the 200 ms read timeout:
    // the call gets 1 s + 200 ms and succeeds.
    let started = std::time::Instant::now();
    assert_eq!(
        db.blpop_key("timeout:never", Duration::from_secs(1))
            .unwrap(),
        None
    );
    assert!(started.elapsed() >= Duration::from_millis(900));

    db.set("timeout:probe", "ok").unwrap();
    assert_eq!(
        db.get_string("timeout:probe").unwrap().as_deref(),
        Some("ok")
    );
}

#[test]
fn client_durability_is_refused_without_replication() {
    let Some(mut db) = connect(7) else {
        return;
    };
    let error = db.client_durability(1, Some(5000)).unwrap_err();

    assert!(
        matches!(&error, ruvio_client::Error::Server(m) if m.contains("needs replication")),
        "{error:?}"
    );
}
