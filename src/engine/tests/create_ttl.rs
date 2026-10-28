//! `EX`/`PX` on a write that creates the key, and `NX`/`XX`/`GT`/`LT` on `EXPIRE`.
use super::*;

fn run(shard: &mut KvShard<crate::time::TestClock>, parts: &[&[u8]]) -> Response<'static> {
    execute_owned(execute(shard, command_of(parts)))
}

fn pttl(shard: &mut KvShard<crate::time::TestClock>, key: &[u8]) -> i64 {
    match run(shard, &[b"PTTL", key]) {
        Response::Integer(value) => value,
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn a_new_counter_gets_its_deadline_once() {
    let clock = crate::time::TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());

    assert_eq!(
        run(&mut shard, &[b"INCR", b"hits", b"EX", b"60"]),
        Response::Integer(1)
    );
    assert_eq!(pttl(&mut shard, b"hits"), 60_000);

    clock.set(11_000);
    assert_eq!(
        run(&mut shard, &[b"INCRBY", b"hits", b"4", b"EX", b"60"]),
        Response::Integer(5)
    );
    assert_eq!(
        pttl(&mut shard, b"hits"),
        50_000,
        "the deadline is not reset"
    );

    clock.set(71_000);
    assert_eq!(run(&mut shard, &[b"EXISTS", b"hits"]), Response::Integer(0));
    assert_eq!(
        run(&mut shard, &[b"INCR", b"hits", b"PX", b"500"]),
        Response::Integer(1),
        "an expired counter is created again with a new deadline"
    );
    assert_eq!(pttl(&mut shard, b"hits"), 500);
}

#[test]
fn an_existing_key_without_a_deadline_stays_without_one() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    run(&mut shard, &[b"SET", b"n", b"1"]);
    assert_eq!(
        run(&mut shard, &[b"INCR", b"n", b"EX", b"60"]),
        Response::Integer(2)
    );
    assert_eq!(pttl(&mut shard, b"n"), -1);
}

#[test]
fn every_creating_write_sets_the_deadline() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    for (key, command) in [
        (
            b"c".as_slice(),
            &[b"DECR".as_slice(), b"c", b"EX", b"5"][..],
        ),
        (b"d", &[b"DECRBY", b"d", b"3", b"PX", b"5000"]),
        (b"f", &[b"INCRBYFLOAT", b"f", b"1.5", b"EX", b"5"]),
        (b"h", &[b"HINCRBY", b"h", b"x", b"1", b"EX", b"5"]),
        (b"z", &[b"ZADD", b"z", b"EX", b"5", b"NX", b"1", b"a"]),
        (b"zi", &[b"ZINCRBY", b"zi", b"1", b"a", b"PX", b"5000"]),
        (
            b"g",
            &[b"GEOADD", b"g", b"PX", b"5000", b"13.36", b"38.11", b"p"],
        ),
        (b"x", &[b"XADD", b"x", b"EX", b"5", b"1-1", b"f", b"v"]),
        (b"b", &[b"SETBIT", b"b", b"3", b"1", b"EX", b"5"]),
        (b"j", &[b"JSON.SET", b"j", b"$", b"{}", b"EX", b"5"]),
        (b"bf", &[b"BF.INSERT", b"bf", b"EX", b"5", b"ITEMS", b"a"]),
    ] {
        let response = run(&mut shard, command);

        assert!(!matches!(response, Response::Error(_)), "{response:?}");
        assert_eq!(pttl(&mut shard, key), 5_000, "{command:?}");
    }
}

#[test]
fn a_write_that_creates_nothing_sets_no_deadline() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    assert_eq!(
        run(&mut shard, &[b"ZADD", b"z", b"XX", b"EX", b"5", b"1", b"a"]),
        Response::Integer(0)
    );
    assert_eq!(run(&mut shard, &[b"EXISTS", b"z"]), Response::Integer(0));

    // MAX refuses the increment, so no key and no deadline.
    assert_eq!(
        run(
            &mut shard,
            &[b"INCRBY", b"c", b"9", b"EX", b"5", b"MAX", b"3"]
        ),
        Response::Bulk(None)
    );
    assert_eq!(run(&mut shard, &[b"EXISTS", b"c"]), Response::Integer(0));
}

#[test]
fn a_wrong_option_is_a_syntax_error() {
    for parts in [
        &[b"INCR".as_slice(), b"k", b"EX"][..],
        &[b"INCR", b"k", b"EX", b"0"],
        &[b"INCR", b"k", b"EX", b"-1"],
        &[b"INCR", b"k", b"EX", b"x"],
        &[b"INCR", b"k", b"NX", b"5"],
        &[b"INCRBY", b"k", b"1", b"EX", b"5", b"PX", b"5"],
        &[b"DECRBY", b"k", b"1", b"MAX", b"5"],
        &[b"SETBIT", b"k", b"1", b"1", b"EX"],
        &[b"INCR", b"k", b"EX", b"9223372036854775807"],
    ] {
        let tokens: Vec<_> = parts.iter().map(|part| owned(part)).collect();

        assert!(
            crate::protocol::parse_arguments(tokens).is_err(),
            "{parts:?} must not parse"
        );
    }
}

#[test]
fn expire_conditions_compare_against_the_current_deadline() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));

    run(&mut shard, &[b"SET", b"k", b"v"]);

    // No deadline counts as infinite: only NX and LT apply.
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"100", b"XX"]),
        Response::Integer(0)
    );
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"100", b"GT"]),
        Response::Integer(0)
    );
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"100", b"NX"]),
        Response::Integer(1)
    );
    assert_eq!(pttl(&mut shard, b"k"), 100_000);

    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"200", b"NX"]),
        Response::Integer(0)
    );
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"50", b"GT"]),
        Response::Integer(0)
    );
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"200", b"GT"]),
        Response::Integer(1)
    );
    assert_eq!(pttl(&mut shard, b"k"), 200_000);
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"300", b"LT"]),
        Response::Integer(0)
    );
    assert_eq!(
        run(&mut shard, &[b"PEXPIRE", b"k", b"5000", b"LT"]),
        Response::Integer(1)
    );
    assert_eq!(pttl(&mut shard, b"k"), 5_000);
    assert_eq!(
        run(&mut shard, &[b"PEXPIRE", b"k", b"9000", b"XX"]),
        Response::Integer(1)
    );
    assert_eq!(pttl(&mut shard, b"k"), 9_000);

    // A deadline already past deletes the key only when the condition holds.
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"-1", b"GT"]),
        Response::Integer(0)
    );
    assert_eq!(run(&mut shard, &[b"EXISTS", b"k"]), Response::Integer(1));
    assert_eq!(
        run(&mut shard, &[b"EXPIRE", b"k", b"-1", b"LT"]),
        Response::Integer(1)
    );
    assert_eq!(run(&mut shard, &[b"EXISTS", b"k"]), Response::Integer(0));

    run(&mut shard, &[b"SET", b"m", b"v"]);
    // Absolute deadlines in the future of the test clock.
    assert_eq!(
        run(&mut shard, &[b"EXPIREAT", b"m", b"100000", b"NX"]),
        Response::Integer(1)
    );
    assert_eq!(
        run(&mut shard, &[b"PEXPIREAT", b"m", b"200000000", b"GT"]),
        Response::Integer(1)
    );
}

#[test]
fn expire_rejects_unknown_or_repeated_options() {
    for parts in [
        &[b"EXPIRE".as_slice(), b"k", b"10", b"ZZ"][..],
        &[b"EXPIRE", b"k", b"10", b"NX", b"GT"],
    ] {
        let tokens: Vec<_> = parts.iter().map(|part| owned(part)).collect();

        assert!(
            crate::protocol::parse_arguments(tokens).is_err(),
            "{parts:?}"
        );
    }
}

#[test]
fn ttl_on_create_inside_multi_commits_the_write_and_deadline() {
    let mut shard = KvShard::with_clock(crate::time::TestClock::new(1_000));
    let commands = vec![
        command_of(&[b"INCR", b"{t}a", b"EX", b"60"]),
        command_of(&[b"INCR", b"{t}a", b"EX", b"60"]),
    ];
    let reply = execute_transaction(&mut shard, &mut NoopMutationSink, &mut (), 1, commands, 0)
        .expect("noop sink never fails");

    assert_eq!(
        reply,
        Response::Exec(vec![Response::Integer(1), Response::Integer(2)])
    );
    assert_eq!(pttl(&mut shard, b"{t}a"), 60_000);
}
