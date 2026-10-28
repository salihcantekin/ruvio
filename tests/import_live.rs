//! `ruvio-ctl import` against two real servers: every supported type, TTLs, the skip/replace/dry-run
//! switches, paging past the 1000-item page size and the error paths.

mod common;

use common::{Client, Server};
use ruvio::import::{ImportOptions, run};

fn options(from: &Server, to: &Server) -> ImportOptions {
    ImportOptions {
        from: format!("redis://{}", from.address),
        to: format!("ruvio://{}", to.address),
        batch: 100,
        ..ImportOptions::default()
    }
}

fn seed(source: &mut Client) {
    source.call(&["SET", "str:plain", "hello"]);
    source.call(&["SET", "str:ttl", "expires"]);
    source.call(&["PEXPIRE", "str:ttl", "600000"]);
    source.call(&["RPUSH", "list:a", "1", "2", "3"]);
    source.call(&["SADD", "set:a", "x", "y", "z"]);
    source.call(&["HSET", "hash:a", "f1", "v1", "f2", "v2"]);
    source.call(&["ZADD", "zset:a", "1.5", "m1", "2", "m2"]);
    source.call(&["XADD", "stream:a", "1-1", "k", "v"]);
    source.call(&["XADD", "stream:a", "2-1", "k2", "v2"]);
}

#[test]
fn copies_every_type_with_ttl_and_verifies() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    seed(&mut source);

    let mut progress_calls = 0;
    let report = run(
        &ImportOptions {
            verify: true,
            ..options(&from, &to)
        },
        &mut |_| progress_calls += 1,
    )
    .unwrap();

    assert!(progress_calls >= 1);
    assert_eq!(report.scanned, 7);
    assert_eq!(report.copied, 7);
    assert_eq!(report.verified, 7);
    assert_eq!(report.mismatched, 0);
    assert_eq!(
        report.skipped_existing + report.vanished + report.unsupported,
        0
    );
    assert_eq!(report.by_type.iter().map(|(_, n)| n).sum::<u64>(), 7);
    assert_eq!(target.call(&["GET", "str:plain"]), "hello");
    assert_eq!(target.call(&["LRANGE", "list:a", "0", "-1"]), "[1,2,3]");
    assert_eq!(target.call(&["SCARD", "set:a"]), "3");
    assert_eq!(target.call(&["HGET", "hash:a", "f2"]), "v2");
    assert_eq!(target.call(&["ZSCORE", "zset:a", "m1"]), "1.5");
    assert_eq!(target.call(&["XLEN", "stream:a"]), "2");

    let ttl: i64 = target.call(&["PTTL", "str:ttl"]).parse().unwrap();

    assert!(ttl > 0 && ttl <= 600_000, "TTL was not carried over: {ttl}");
    assert_eq!(target.call(&["PTTL", "str:plain"]), "-1");
}

#[test]
fn existing_keys_are_skipped_unless_replace_and_dry_run_writes_nothing() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    source.call(&["SET", "a", "from-source"]);
    source.call(&["SET", "b", "only-source"]);
    target.call(&["SET", "a", "from-target"]);

    let dry = run(
        &ImportOptions {
            dry_run: true,
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!((dry.scanned, dry.copied), (2, 2));
    assert_eq!(
        target.call(&["EXISTS", "b"]),
        "0",
        "a dry run must not write"
    );

    let skipped = run(&options(&from, &to), &mut |_| {}).unwrap();

    assert_eq!((skipped.copied, skipped.skipped_existing), (1, 1));
    assert_eq!(target.call(&["GET", "a"]), "from-target");
    assert_eq!(target.call(&["GET", "b"]), "only-source");

    let replaced = run(
        &ImportOptions {
            replace: true,
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!((replaced.copied, replaced.skipped_existing), (2, 0));
    assert_eq!(target.call(&["GET", "a"]), "from-source");
}

#[test]
fn replace_swaps_in_the_copy_with_its_ttl_and_leaves_no_staging_key() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    source.call(&["SET", "ttl", "new"]);
    source.call(&["PEXPIRE", "ttl", "600000"]);
    source.call(&["RPUSH", "kind", "x", "y"]);
    source.call(&["SET", "post:{7}:body", "tagged"]);
    source.call(&["SET", "odd{}key", "braces"]);
    target.call(&["SET", "ttl", "old"]);
    target.call(&["SET", "kind", "was-a-string"]);
    target.call(&["SET", "post:{7}:body", "old"]);
    target.call(&["SET", "odd{}key", "old"]);

    let report = run(
        &ImportOptions {
            replace: true,
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!(report.copied, 4);
    assert_eq!(target.call(&["GET", "ttl"]), "new");

    let ttl: i64 = target.call(&["PTTL", "ttl"]).parse().unwrap();

    assert!(ttl > 0 && ttl <= 600_000, "TTL lost by the swap: {ttl}");
    assert_eq!(target.call(&["TYPE", "kind"]), "list");
    assert_eq!(target.call(&["GET", "post:{7}:body"]), "tagged");
    assert_eq!(target.call(&["GET", "odd{}key"]), "braces");
    assert_eq!(
        target.call(&["DBSIZE"]),
        "4",
        "a staging key was left behind"
    );
}

#[test]
fn replace_never_touches_a_real_key_that_looks_like_the_staging_name() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    source.call(&["SET", "a", "new"]);
    source.call(&["SET", "{t}b", "one"]);
    source.call(&["SET", "{t}b:import-tmp", "two"]);
    target.call(&["SET", "a", "old"]);
    target.call(&["SET", "{a}:import-tmp", "real"]);

    let report = run(
        &ImportOptions {
            replace: true,
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!((report.copied, report.skipped_staging), (3, 0));
    assert_eq!(target.call(&["GET", "a"]), "new");
    assert_eq!(target.call(&["GET", "{a}:import-tmp"]), "real");
    assert_eq!(target.call(&["GET", "{t}b"]), "one");
    assert_eq!(target.call(&["GET", "{t}b:import-tmp"]), "two");
    assert_eq!(target.call(&["DBSIZE"]), "4");
}

#[test]
fn pattern_limits_the_keys_and_an_empty_source_is_fine() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    let empty = run(&options(&from, &to), &mut |_| {}).unwrap();

    assert_eq!((empty.scanned, empty.copied), (0, 0));

    source.call(&["SET", "user:1", "a"]);
    source.call(&["SET", "user:2", "b"]);
    source.call(&["SET", "order:1", "c"]);

    let report = run(
        &ImportOptions {
            pattern: Some("user:*".into()),
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!(report.copied, 2);
    assert_eq!(target.call(&["EXISTS", "order:1"]), "0");
    assert_eq!(target.call(&["DBSIZE"]), "2");
}

#[test]
fn collections_larger_than_one_page_are_copied_completely() {
    let (from, to) = (Server::start(), Server::start());
    let mut source = Client::connect(&from.address);
    let mut target = Client::connect(&to.address);

    // 2300 > the 1000-item page used for LRANGE, XRANGE and the *SCAN loops.
    for chunk in (0..2300).collect::<Vec<_>>().chunks(100) {
        let numbers: Vec<String> = chunk.iter().map(|n| n.to_string()).collect();
        let mut push = vec!["RPUSH", "big:list"];
        let mut add = vec!["SADD", "big:set"];
        let mut zadd_owned: Vec<String> = Vec::new();

        push.extend(numbers.iter().map(String::as_str));
        add.extend(numbers.iter().map(String::as_str));
        source.call(&push);
        source.call(&add);

        for n in &numbers {
            zadd_owned.push(n.clone());
            zadd_owned.push(format!("m{n}"));
        }

        let mut zadd = vec!["ZADD", "big:z"];

        zadd.extend(zadd_owned.iter().map(String::as_str));
        source.call(&zadd);

        let mut hset = vec!["HSET", "big:h"];

        hset.extend(zadd_owned.iter().map(String::as_str));
        source.call(&hset);

        for n in &numbers {
            source.call(&[
                "XADD",
                "big:x",
                &format!("{}-1", n.parse::<u32>().unwrap() + 1),
                "f",
                n,
            ]);
        }
    }

    let report = run(
        &ImportOptions {
            verify: true,
            ..options(&from, &to)
        },
        &mut |_| {},
    )
    .unwrap();

    assert_eq!((report.copied, report.mismatched), (5, 0));
    assert_eq!(target.call(&["LLEN", "big:list"]), "2300");
    assert_eq!(target.call(&["LINDEX", "big:list", "2299"]), "2299");
    assert_eq!(target.call(&["SCARD", "big:set"]), "2300");
    assert_eq!(target.call(&["ZCARD", "big:z"]), "2300");
    assert_eq!(target.call(&["HLEN", "big:h"]), "2300");
    assert_eq!(target.call(&["XLEN", "big:x"]), "2300");
}

#[test]
fn bad_endpoints_and_unreachable_servers_are_errors() {
    let live = Server::start();
    // Nothing listens on port 1 (tcpmux, long unused), and no test can take it the way it
    // could take a port picked by binding and closing.
    let dead = "127.0.0.1:1".to_owned();
    let run_with = |from: &str, to: &str| {
        run(
            &ImportOptions {
                from: from.into(),
                to: to.into(),
                batch: 10,
                ..ImportOptions::default()
            },
            &mut |_| {},
        )
    };

    assert!(
        run_with("redis://", &live.address)
            .unwrap_err()
            .contains("no address")
    );
    assert!(
        run_with(&live.address, "host:1/notanumber")
            .unwrap_err()
            .contains("bad database")
    );
    assert!(
        run_with(&dead, &live.address)
            .unwrap_err()
            .contains("connect")
    );
    assert!(
        run_with(&live.address, &dead)
            .unwrap_err()
            .contains("connect")
    );
}
