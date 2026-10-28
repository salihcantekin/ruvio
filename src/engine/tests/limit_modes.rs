//! `LIMIT ... SLIDING` and `LIMIT ... TOKENS`: replies, the log they write, and replay.
use super::*;
use crate::time::TestClock;

fn call<C: Clock>(
    shard: &mut KvShard<C>,
    sink: &mut DatabaseLog,
    parts: &[&str],
) -> Response<'static> {
    let parts: Vec<&[u8]> = parts.iter().map(|part| part.as_bytes()).collect();
    let command = command_of(&parts);
    let Ok(response) = execute_with_sink(shard, command, sink);

    response.into_owned()
}

fn retry(remaining: i64, retry_after_ms: i64) -> Response<'static> {
    Response::Exec(vec![
        Response::Integer(remaining),
        Response::Integer(retry_after_ms),
    ])
}

#[test]
fn sliding_counts_refuses_and_forgets_idle_keys() {
    let clock = TestClock::new(10_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut log = DatabaseLog::default();
    let sliding = ["LIMIT", "k", "3", "1000", "SLIDING"];

    for remaining in [2, 1, 0] {
        assert_eq!(
            call(&mut shard, &mut log, &sliding),
            Response::Integer(remaining)
        );
    }

    assert_eq!(shard.pttl(b"k"), 2_000);
    let records = log.records.len();
    assert_eq!(call(&mut shard, &mut log, &sliding), Response::Bulk(None));
    assert_eq!(log.records.len(), records, "a refusal must write nothing");

    // Still inside the window, the retry hint says when it ends.
    clock.set(10_400);
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "3", "1000", "SLIDING", "WITHRETRY"]
        ),
        retry(-1, 1_000 - 400 + 1)
    );

    clock.set(12_000);
    assert_eq!(shard.pttl(b"k"), -2);
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "3", "1000", "SLIDING", "WITHRETRY"]
        ),
        retry(2, 0)
    );
}

#[test]
fn sliding_lets_a_previous_window_age_out_instead_of_resetting() {
    let clock = TestClock::new(50_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut log = DatabaseLog::default();
    let sliding = ["LIMIT", "k", "4", "1000", "SLIDING"];

    for _ in 0..4 {
        assert!(matches!(
            call(&mut shard, &mut log, &sliding),
            Response::Integer(_)
        ));
    }

    // The next window starts: all four still count in full.
    clock.set(51_000);
    assert_eq!(call(&mut shard, &mut log, &sliding), Response::Bulk(None));
    // Half a window later half of them have aged out.
    clock.set(51_500);
    assert_eq!(call(&mut shard, &mut log, &sliding), Response::Integer(1));
    assert_eq!(shard.pttl(b"k"), 1_500);
}

#[test]
fn sliding_state_replays_to_the_same_answers() {
    // Real clock with windows far longer than the test, so only the counts matter.
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();
    let sliding = ["LIMIT", "k", "6", "3600000", "SLIDING"];

    for remaining in [5, 4, 3] {
        assert_eq!(
            call(&mut shard, &mut log, &sliding),
            Response::Integer(remaining)
        );
    }

    let mut copy = replayed(&log);
    assert!(copy.pttl(b"k") > 7_100_000);

    for remaining in [2, 1, 0] {
        assert_eq!(
            call(&mut copy, &mut DatabaseLog::default(), &sliding),
            Response::Integer(remaining)
        );
    }

    assert_eq!(
        call(&mut copy, &mut DatabaseLog::default(), &sliding),
        Response::Bulk(None)
    );
}

#[test]
fn tokens_spend_a_burst_refill_over_time_and_report_the_wait() {
    let clock = TestClock::new(20_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut log = DatabaseLog::default();
    let tokens = ["LIMIT", "k", "5", "1000", "TOKENS"];

    for remaining in [4, 3, 2, 1, 0] {
        assert_eq!(
            call(&mut shard, &mut log, &tokens),
            Response::Integer(remaining)
        );
    }

    // The key lives until the bucket is full again.
    assert_eq!(shard.pttl(b"k"), 1_000);
    let records = log.records.len();
    assert_eq!(call(&mut shard, &mut log, &tokens), Response::Bulk(None));
    assert_eq!(log.records.len(), records, "a refusal must write nothing");
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "5", "1000", "TOKENS", "WITHRETRY"]
        ),
        retry(-1, 200)
    );

    clock.set(20_200);
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "5", "1000", "TOKENS", "WITHRETRY"]
        ),
        retry(0, 0)
    );

    clock.set(21_200);
    assert_eq!(shard.pttl(b"k"), -2);
}

#[test]
fn tokens_cost_takes_several_tokens_at_once() {
    let clock = TestClock::new(30_000);
    let mut shard = KvShard::with_clock(clock);
    let mut log = DatabaseLog::default();

    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "10", "1000", "TOKENS", "COST", "4"]
        ),
        Response::Integer(6)
    );
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &[
                "LIMIT",
                "k",
                "10",
                "1000",
                "tokens",
                "cost",
                "7",
                "withretry"
            ]
        ),
        retry(-1, 100)
    );
    assert_eq!(
        call(
            &mut shard,
            &mut log,
            &["LIMIT", "k", "10", "1000", "TOKENS", "COST", "6"]
        ),
        Response::Integer(0)
    );
}

#[test]
fn tokens_state_replays_to_the_same_answers() {
    // Five tokens refill over a day, so nothing refills during the test.
    let mut shard = KvShard::new();
    let mut log = DatabaseLog::default();
    let tokens = ["LIMIT", "k", "5", "86400000", "TOKENS"];

    for remaining in [4, 3] {
        assert_eq!(
            call(&mut shard, &mut log, &tokens),
            Response::Integer(remaining)
        );
    }

    let mut copy = replayed(&log);
    // Two of five tokens are out, so the key lives about two fifths of the refill period.
    assert!((34_000_000..=34_560_000).contains(&copy.pttl(b"k")));

    for remaining in [2, 1, 0] {
        assert_eq!(
            call(&mut copy, &mut DatabaseLog::default(), &tokens),
            Response::Integer(remaining)
        );
    }

    assert_eq!(
        call(&mut copy, &mut DatabaseLog::default(), &tokens),
        Response::Bulk(None)
    );
}

#[test]
fn modes_do_not_mix_on_one_key_and_wrong_types_are_refused() {
    let clock = TestClock::new(60_000);
    let mut shard = KvShard::with_clock(clock);
    let mut log = DatabaseLog::default();
    let mode_error = Response::Error(b"ERR LIMIT key was created in another mode");

    call(&mut shard, &mut log, &["LIMIT", "fixed", "3", "1000"]);
    call(
        &mut shard,
        &mut log,
        &["LIMIT", "slide", "3", "1000", "SLIDING"],
    );
    call(
        &mut shard,
        &mut log,
        &["LIMIT", "bucket", "3", "1000", "TOKENS"],
    );

    for (key, others) in [
        ("fixed", [vec!["SLIDING"], vec!["TOKENS"]]),
        ("slide", [vec![], vec!["TOKENS"]]),
        ("bucket", [vec!["SLIDING"], vec![]]),
    ] {
        for extra in others {
            let mut parts = vec!["LIMIT", key, "3", "1000"];
            parts.extend(extra);
            assert_eq!(call(&mut shard, &mut log, &parts), mode_error, "{parts:?}");
        }
    }

    call(&mut shard, &mut log, &["SET", "plain", "v"]);

    for mode in ["SLIDING", "TOKENS"] {
        assert_eq!(
            call(&mut shard, &mut log, &["LIMIT", "plain", "3", "1000", mode]),
            wrong_type()
        );
    }
}

#[test]
fn limit_options_are_validated() {
    for parts in [
        &["LIMIT", "k", "3", "1000", "BOGUS"][..],
        &["LIMIT", "k", "3", "1000", "SLIDING", "COST", "1"],
        &[
            "LIMIT",
            "k",
            "3",
            "1000",
            "SLIDING",
            "WITHRETRY",
            "WITHRETRY",
        ],
        &["LIMIT", "k", "3", "1000", "SLIDING", "2147483648"],
        &["LIMIT", "k", "2147483648", "1000", "SLIDING"],
        &["LIMIT", "k", "1000001", "1000", "TOKENS"],
        &["LIMIT", "k", "5", "4294967296", "TOKENS"],
        &["LIMIT", "k", "5", "1000", "TOKENS", "COST"],
        &["LIMIT", "k", "5", "1000", "TOKENS", "COST", "0"],
        &["LIMIT", "k", "5", "1000", "TOKENS", "COST", "6"],
        &["LIMIT", "k", "5", "1000", "TOKENS", "COST", "x"],
        &[
            "LIMIT", "k", "5", "1000", "TOKENS", "COST", "1", "COST", "1",
        ],
    ] {
        let bytes: Vec<&[u8]> = parts.iter().map(|part| part.as_bytes()).collect();
        let mut frame = format!("*{}\r\n", bytes.len()).into_bytes();

        for part in &bytes {
            frame.extend(format!("${}\r\n", part.len()).bytes());
            frame.extend_from_slice(part);
            frame.extend_from_slice(b"\r\n");
        }

        assert!(parse_command(&frame).is_err(), "{parts:?} should not parse");
    }
}

#[test]
fn a_huge_fixed_maximum_never_reports_a_negative_remaining_count() {
    let mut shard = KvShard::with_clock(TestClock::new(10_000));
    let mut log = DatabaseLog::default();
    let huge = "18446744073709551615";

    assert_eq!(
        call(&mut shard, &mut log, &["LIMIT", "k", huge, "1000"]),
        Response::Integer(i64::MAX)
    );
}
