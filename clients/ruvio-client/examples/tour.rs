//! Calls nearly every `ruvio-client` method against one server.
//!
//! ```text
//! cargo run --manifest-path clients/ruvio-client/Cargo.toml --example tour -- 127.0.0.1 6379
//! ```
//!
//! Keys use the `{tour}` hash tag. On a standalone server the tour selects database 15 and
//! flushes it first. Cluster mode stays on database 0 and does not flush.

use std::time::{Duration, SystemTime};

use ruvio_client::{
    Client, FieldExpireCondition, GeoUnit, SetMode, SortedSetAddOptions, StreamClaimOptions,
    StreamPendingFilter, StreamReadOptions,
};

fn main() -> Result<(), ruvio_client::Error> {
    let host = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_owned());
    let port = std::env::args()
        .nth(2)
        .and_then(|text| text.parse().ok())
        .unwrap_or(6379);
    let mut db = Client::connect(&host, port)?;

    db.on_connection_lost(|notice| {
        println!(
            "connection lost {}:{} subscriber={}",
            notice.host, notice.port, notice.subscriber
        );
    });

    db.on_connection_restored(|notice| {
        println!(
            "connection restored {}:{} subscriber={}",
            notice.host, notice.port, notice.subscriber
        );
    });

    db.set_reconnect_delay(|attempt| {
        if attempt >= 8 {
            None
        } else {
            Some(Duration::from_millis(100 * 2u64.pow(attempt - 1)))
        }
    });

    println!(
        "connected cluster={} shards={} slot={}",
        db.is_cluster(),
        db.shard_count(),
        ruvio_client::hash_slot("{tour}")
    );
    println!("reconnect armed; the command that sees a drop fails, the next one opens the socket");

    if !db.is_cluster() {
        db.select(15)?;
        db.flushdb()?;
        println!("selected database {}", db.database());
    }

    strings(&mut db)?;
    owned(&mut db)?;
    lists(&mut db)?;
    sets(&mut db)?;
    hashes(&mut db)?;
    sorted_sets(&mut db)?;
    bloom(&mut db)?;
    streams(&mut db)?;
    transactions(&mut db)?;
    server(&mut db)?;
    pubsub(&host, port)?;

    println!("tour finished");

    Ok(())
}

fn strings(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let key = "{tour}:s";

    db.set(key, "hello")?;
    println!("get {:?}", db.get_string(key)?);
    db.set_with(key, "nx", None, SetMode::IfNotExists)?;
    db.set_expires_at(
        key,
        "later",
        SystemTime::now() + Duration::from_secs(120),
        SetMode::Always,
    )?;
    db.set_keep_ttl(key, "kept", SetMode::Always)?;
    println!(
        "set_and_get {:?}",
        db.set_and_get(key, "now", None, SetMode::Always)?
    );
    db.mset(&[("{tour}:a", "1"), ("{tour}:b", "2")])?;
    println!(
        "mget {:?}",
        db.mget(&["{tour}:a", "{tour}:missing", "{tour}:b"])?
    );
    println!("getset {:?}", db.getset(key, "10")?);
    println!("append {}", db.append(key, "0")?);
    println!("strlen {}", db.strlen(key)?);
    println!("incr {}", db.incr("{tour}:n")?);
    println!("incr_by {}", db.incr_by("{tour}:n", 5)?);
    println!("decr {}", db.decr("{tour}:n")?);
    println!("decr_by {}", db.decr_by("{tour}:n", 2)?);
    println!("exists {}", db.exists(&["{tour}:a", "{tour}:b"])?);
    println!("exists_key {}", db.exists_key(key)?);
    println!("type {}", db.key_type(key)?);
    db.rename("{tour}:b", "{tour}:c")?;
    println!("unlink {}", db.unlink(&["{tour}:c"])?);
    println!("unlink_key {}", db.unlink_key("{tour}:gone")?);
    db.expire(key, Duration::from_secs(30))?;
    db.pexpire("{tour}:a", Duration::from_millis(30_000))?;
    db.expire_at("{tour}:a", SystemTime::now() + Duration::from_secs(60))?;
    println!("ttl {} pttl {}", db.ttl(key)?, db.pttl(key)?);
    println!("persist {}", db.persist(key)?);
    let page = db.scan(0, Some("{tour}:*"), Some(20), None)?;

    println!("scan cursor={} keys={}", page.cursor, page.keys.len());
    println!("dbsize {}", db.dbsize()?);
    println!("del {}", db.del(&["{tour}:a"])?);
    println!("del_key {}", db.del_key("{tour}:n")?);

    Ok(())
}

fn owned(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let first = db.lease("{tour}:lock", 30_000)?;

    println!(
        "lease {:?} held {:?} release {}",
        first,
        db.lease("{tour}:lock", 30_000)?,
        db.release("{tour}:lock", first.unwrap_or(0) as u64)?
    );
    let permit = db.semaphore("{tour}:workers", 1, 30_000)?;

    println!(
        "semaphore {:?} full {:?} release {}",
        permit,
        db.semaphore("{tour}:workers", 1, 30_000)?,
        db.release("{tour}:workers", permit.unwrap_or(0) as u64)?
    );
    println!(
        "incr_by_max {:?} over {:?}",
        db.incr_by_max("{tour}:seats", 3, 4)?,
        db.incr_by_max("{tour}:seats", 2, 4)?
    );
    let created = db.setv("{tour}:doc", "12", None)?;
    let next = db.setv("{tour}:doc", "11", Some(created as u64))?;

    println!("setv {created} then {next}");
    println!(
        "setv mismatch {}",
        db.setv("{tour}:doc", "10", Some(created as u64))
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "accepted".to_owned())
    );
    let cursor = db.changes_start()?;

    db.set("{tour}:note", "paid")?;
    let notes = db.changes(cursor as u64)?;

    println!(
        "changes cursor={cursor} notes={} {}",
        notes.len(),
        notes.first().map(|(_, text)| text.as_str()).unwrap_or("")
    );
    println!(
        "limit {:?} {:?}",
        db.limit("{tour}:login", 2, 60_000)?,
        db.limit("{tour}:login", 2, 60_000)?
    );
    println!(
        "once {} {}",
        db.once("{tour}:pay", 60_000)?,
        db.once("{tour}:pay", 60_000)?
    );
    db.set("{tour}:seats", "5")?;
    println!(
        "take {:?} {:?}",
        db.take("{tour}:seats", 2)?,
        db.take("{tour}:seats", 9)?
    );
    println!("getex {:?}", db.getex("{tour}:seats", 60_000)?);

    Ok(())
}

fn lists(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let key = "{tour}:l";

    println!("rpush {}", db.rpush(key, &["a", "b", "c", "d"])?);
    println!("lpush {}", db.lpush(key, &["z"])?);
    println!("lpop {:?}", db.lpop(key)?);
    println!("rpop_count {:?}", db.rpop_count(key, 2)?);
    println!("llen {}", db.llen(key)?);
    println!("lindex {:?}", db.lindex(key, 0)?);
    println!("lrange {:?}", db.lrange(key, 0, -1)?);
    db.ltrim(key, 0, 0)?;
    println!("lpop_count {:?}", db.lpop_count(key, 1)?);
    db.rpush(key, &["only"])?;
    println!("blpop {:?}", db.blpop_key(key, Duration::from_millis(200))?);
    db.rpush(key, &["right"])?;
    println!("brpop {:?}", db.brpop(&[key], Duration::from_millis(200))?);
    println!(
        "brpop empty {:?}",
        db.brpop_key(key, Duration::from_millis(1))?
    );

    Ok(())
}

fn sets(db: &mut Client) -> Result<(), ruvio_client::Error> {
    println!("sadd {}", db.sadd("{tour}:s1", &["a", "b", "c"])?);
    db.sadd("{tour}:s2", &["b", "c", "d"])?;
    println!("sismember {}", db.sismember("{tour}:s1", "a")?);
    println!("scard {}", db.scard("{tour}:s1")?);
    println!("smembers {:?}", db.smembers("{tour}:s1")?);
    println!("sinter {:?}", db.sinter(&["{tour}:s1", "{tour}:s2"])?);
    println!("sunion {:?}", db.sunion(&["{tour}:s1", "{tour}:s2"])?);
    println!("sdiff {:?}", db.sdiff(&["{tour}:s1", "{tour}:s2"])?);
    println!(
        "sinterstore {}",
        db.sinterstore("{tour}:s3", &["{tour}:s1", "{tour}:s2"])?
    );
    println!(
        "sunionstore {}",
        db.sunionstore("{tour}:s4", &["{tour}:s1", "{tour}:s2"])?
    );
    println!(
        "sdiffstore {}",
        db.sdiffstore("{tour}:s5", &["{tour}:s1", "{tour}:s2"])?
    );
    println!("smove {}", db.smove("{tour}:s1", "{tour}:s3", "a")?);
    println!("srem {}", db.srem("{tour}:s3", &["a"])?);
    println!("srandmember {:?}", db.srandmember("{tour}:s2")?);
    println!(
        "srandmember_count {:?}",
        db.srandmember_count("{tour}:s2", 2)?
    );
    println!("spop_count {:?}", db.spop_count("{tour}:s5", 5)?);
    println!("spop {:?}", db.spop("{tour}:s4")?);

    Ok(())
}

fn hashes(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let key = "{tour}:h";

    println!("hset {}", db.hset(key, &[("f1", "1"), ("f2", "two")])?);
    println!("hget {:?}", db.hget(key, "f2")?);
    println!("hincr_by {}", db.hincr_by(key, "f1", 4)?);
    println!("hsetnx {}", db.hsetnx(key, "f1", "9")?);
    println!("hstrlen {}", db.hstrlen(key, "f2")?);
    println!("hlen {}", db.hlen(key)?);
    println!("hexists {}", db.hexists(key, "f1")?);
    println!("hkeys {:?}", db.hkeys(key)?);
    println!("hvals {:?}", db.hvals(key)?);
    println!("hgetall {:?}", db.hgetall(key)?);
    println!("hmget {:?}", db.hmget(key, &["f2", "nope"])?);
    let page = db.hscan(key, 0, Some("f*"), Some(10))?;

    println!("hscan cursor={} fields={}", page.cursor, page.fields.len());
    println!(
        "hexpire {:?}",
        db.hexpire(key, Duration::from_secs(60), &["f1", "nope"])?
    );
    println!(
        "hexpire_with GT {:?}",
        db.hexpire_with(
            key,
            Duration::from_secs(120),
            FieldExpireCondition::IfGreater,
            &["f1"]
        )?
    );
    println!(
        "hpexpire {:?}",
        db.hpexpire(key, Duration::from_millis(90_500), &["f2"])?
    );
    println!("httl {:?}", db.httl(key, &["f1", "f2"])?);
    println!("hpttl {:?}", db.hpttl(key, &["f1"])?);
    println!(
        "hexpire_at {:?}",
        db.hexpire_at(key, SystemTime::now() + Duration::from_secs(300), &["f2"])?
    );
    println!("hexpiretime {:?}", db.hexpiretime(key, &["f2"])?);
    println!("hpexpiretime {:?}", db.hpexpiretime(key, &["f2"])?);
    println!("hpersist {:?}", db.hpersist(key, &["f1", "f2"])?);
    println!("hdel {}", db.hdel(key, &["f1"])?);

    Ok(())
}

fn sorted_sets(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let key = "{tour}:z";

    println!("zadd {}", db.zadd(key, &[("ada", 100.0), ("bob", 50.0)])?);
    println!(
        "geoadd {}",
        db.geo_add(key, &[(13.361389, 38.115556, "Palermo")])?
    );
    println!(
        "geosearch {:?}",
        db.geo_search(key, 13.36, 38.11, 20.0, GeoUnit::Kilometers)?
    );
    let greater = SortedSetAddOptions {
        greater_than: true,
        changed: true,
        ..SortedSetAddOptions::default()
    };

    println!(
        "zadd_with {}",
        db.zadd_with(key, greater, &[("bob", 10.0)])?
    );
    println!("zincr_by {}", db.zincr_by(key, "bob", 5.5)?);
    println!(
        "zadd_incr {:?}",
        db.zadd_incr(key, "cy", 1.0, SortedSetAddOptions::default())?
    );
    println!("zrange {:?}", db.zrange(key, 0, -1)?);
    println!("zrevrange {:?}", db.zrevrange(key, 0, 0)?);
    println!("zrange_with_scores {:?}", db.zrange_with_scores(key, 0, 0)?);
    println!(
        "zrevrange_with_scores {:?}",
        db.zrevrange_with_scores(key, 0, 0)?
    );
    println!(
        "zrange_by_score {:?}",
        db.zrange_by_score_with_scores(key, "(1", "+inf")?
    );
    println!("zscore {:?}", db.zscore(key, "ada")?);
    println!("zrank {:?}", db.zrank(key, "ada")?);
    println!("zrevrank {:?}", db.zrevrank(key, "ada")?);
    println!("zrem {}", db.zrem(key, &["cy"])?);
    println!("zcard {}", db.zcard(key)?);

    let queue = "{tour}:zq";

    db.zadd(
        queue,
        &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)],
    )?;
    println!("zpopmin {:?}", db.zpopmin(queue, 1)?);
    println!("zpopmax {:?}", db.zpopmax(queue, 1)?);
    println!(
        "bzpopmin {:?}",
        db.bzpopmin(&[queue], Duration::from_secs(1))?
    );
    println!(
        "bzpopmax {:?}",
        db.bzpopmax(&[queue], Duration::from_secs(1))?
    );
    db.zadd(queue, &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("x", 0.0)])?;
    println!("zremrangebyrank {}", db.zremrangebyrank(queue, 0, 0)?);
    println!(
        "zremrangebyscore {}",
        db.zremrangebyscore(queue, "(1", "2")?
    );
    println!("zremrangebylex {}", db.zremrangebylex(queue, "-", "+")?);

    Ok(())
}

fn bloom(db: &mut Client) -> Result<(), ruvio_client::Error> {
    db.bf_reserve("{tour}:bf", 0.01, 1000)?;
    println!("bf_add {}", db.bf_add("{tour}:bf", "x")?);
    println!(
        "bf_exists {} {}",
        db.bf_exists("{tour}:bf", "x")?,
        db.bf_exists("{tour}:bf", "y")?
    );

    Ok(())
}

fn streams(db: &mut Client) -> Result<(), ruvio_client::Error> {
    let key = "{tour}:stream";
    let first = db.xadd(key, "*", &[("f", "1")])?;

    db.xadd(key, "*", &[("f", "2")])?;
    println!(
        "xadd_maxlen {}",
        db.xadd_maxlen(key, 10, "*", &[("f", "3")])?
    );
    println!(
        "xadd_delay {}",
        db.xadd_delay(key, 60_000, "*", &[("f", "later")])?
    );
    println!("xlen {}", db.xlen(key)?);
    println!("xrange {}", db.xrange(key, "-", "+", None)?[0].id);
    println!(
        "xrevrange {}",
        db.xrevrange(key, "+", "-", Some(1))?[0].fields[0].1
    );
    let read = db.xread(
        &StreamReadOptions {
            count: Some(2),
            ..StreamReadOptions::default()
        },
        &[(key, "0")],
    )?;

    println!("xread {}", read[0].entries.len());
    db.xgroup_create(key, "g", "0", false)?;
    db.xgroup_create("{tour}:fresh", "g", "$", true)?;
    let delivered = db.xreadgroup("g", "c1", &StreamReadOptions::default(), &[(key, ">")])?;

    println!("xreadgroup {}", delivered[0].entries.len());
    let pending = db.xpending(key, "g", "-", "+", 10, &StreamPendingFilter::default())?;

    println!(
        "xpending {} summary-array {}",
        pending.len(),
        db.xpending_summary(key, "g")?.as_array().is_some()
    );
    let claimed = db.xclaim(
        key,
        "g",
        "c2",
        Duration::ZERO,
        &[&first],
        &StreamClaimOptions::default(),
    )?;

    println!("xclaim {}", claimed[0].id);
    println!(
        "xclaim_ids {:?}",
        db.xclaim_ids(
            key,
            "g",
            "c2",
            Duration::ZERO,
            &[&first],
            &StreamClaimOptions::default()
        )?
    );
    println!("xack {}", db.xack(key, "g", &[&first])?);
    println!(
        "xgroup_delconsumer {}",
        db.xgroup_delconsumer(key, "g", "c1")?
    );
    db.xgroup_setid(key, "g", "0")?;
    println!("xgroup_destroy {}", db.xgroup_destroy(key, "g")?);
    println!("xdel {}", db.xdel(key, &[&first])?);
    println!("xtrim {}", db.xtrim_maxlen(key, 1)?);

    Ok(())
}

fn transactions(db: &mut Client) -> Result<(), ruvio_client::Error> {
    db.watch_key("{tour}:w")?;
    db.watch(&["{tour}:w"])?;
    db.multi()?;
    db.execute(&["SET", "{tour}:w", "1"])?;
    db.execute(&["INCR", "{tour}:w"])?;
    println!(
        "exec {} replies",
        db.exec()?.map(|replies| replies.len()).unwrap_or(0)
    );
    db.unwatch()?;
    db.multi()?;
    db.discard()?;
    println!(
        "eval {:?}",
        db.eval("return ARGV[1]", &[], &["hi"])?.as_string()?
    );
    let sha = db.script_load("return ruvio.call('GET', KEYS[1])")?;

    println!("script_exists {:?}", db.script_exists(&[&sha, "0000"])?);
    println!(
        "evalsha {:?}",
        db.evalsha(&sha, &["{tour}:w"], &[])?.as_string()?
    );
    db.function_load("tour_echo", "return ARGV[1]")?;
    println!(
        "fcall {:?}",
        db.fcall("tour_echo", &[], &["x"])?.as_string()?
    );
    println!(
        "function_list array {}",
        db.function_list()?.as_array().is_some()
    );
    println!("function_delete {}", db.function_delete("tour_echo")?);
    db.script_flush()?;
    match db.script_kill() {
        Ok(()) => println!("script_kill ok"),
        Err(error) => println!("script_kill {error}"),
    }

    if !db.is_cluster() {
        db.set("{tour}:move", "v")?;
        println!("move {}", db.move_key("{tour}:move", 14)?);
        db.swapdb(14, 15)?;
        println!("swapdb back");
        db.swapdb(14, 15)?;
    }

    Ok(())
}

fn server(db: &mut Client) -> Result<(), ruvio_client::Error> {
    println!("ping {}", db.ping()?);
    println!("ping_message {}", db.ping_message("tour")?);
    println!("info has ruvio {}", db.info(None)?.contains("ruvio"));
    println!("info server {}", db.info(Some("server"))?.len());
    println!("config_get {}", db.config_get("databases")?.len());
    db.config_set(&[("slowlog-max-len", "128")])?;
    println!("config_set slowlog-max-len 128");
    println!("role {:?}", db.role()?);
    println!("whoami {}", db.acl_whoami()?);
    println!("acl_users {:?}", db.acl_users()?);
    println!("acl_list {}", db.acl_list()?.len());
    println!("acl_cat {}", db.acl_cat()?.len());
    println!("acl_getuser null {}", db.acl_getuser("default")?.is_null());
    db.acl_setuser("tour-reader", &["on", "~{tour}:*", "+get"])?;
    db.client_setname("ruvio-tour")?;
    println!("client_getname {:?}", db.client_getname()?);
    db.client_tracking(false)?;
    println!("cluster_keyslot {}", db.cluster_keyslot("{tour}")?);

    if db.is_cluster() {
        println!(
            "cluster_nodes {}",
            db.cluster_nodes()?.chars().take(40).collect::<String>()
        );
        println!(
            "cluster_slots array {}",
            db.cluster_slots()?.as_array().is_some()
        );
        println!(
            "cluster_shards array {}",
            db.cluster_shards()?.as_array().is_some()
        );
        println!("cluster_info {}", db.cluster_info()?.len());
        println!("cluster_myid {}", db.cluster_myid()?);
        db.readonly()?;
        db.readwrite()?;
    }

    db.save()?;
    db.bgsave()?;
    println!("hello array {}", db.hello(2)?.as_array().is_some());
    db.execute_many(&[&["PING"], &["PING"]])?;

    Ok(())
}

fn pubsub(host: &str, port: u16) -> Result<(), ruvio_client::Error> {
    let mut commands = Client::connect(host, port)?;
    let mut listener = commands.subscriber()?;

    listener.subscribe(&["{tour}:news"])?;
    listener.psubscribe(&["{tour}:n*"])?;
    listener.set_read_timeout(Some(Duration::from_secs(2)))?;
    println!("publish {}", commands.publish("{tour}:news", "hello-tour")?);

    let exact = listener.next_message()?;
    let pattern = listener.next_message()?;

    println!(
        "message {} {:?}",
        exact.channel,
        String::from_utf8_lossy(&exact.payload)
    );
    println!("pmessage {:?}", pattern.pattern);
    listener.unsubscribe(&["{tour}:news"])?;
    listener.punsubscribe(&["{tour}:n*"])?;
    // Sharded channels live on the shard that owns their slot: subscribe on a socket to that shard.
    let mut sharded = commands.sharded_subscriber("{tour}:shard")?;

    sharded.set_read_timeout(Some(Duration::from_secs(2)))?;
    sharded.ssubscribe(&["{tour}:shard"])?;
    println!("spublish {}", commands.spublish("{tour}:shard", "sharded")?);
    println!("smessage {}", sharded.next_message()?.kind);
    sharded.sunsubscribe(&["{tour}:shard"])?;
    println!("ping after subscribe {}", commands.ping()?);

    Ok(())
}
