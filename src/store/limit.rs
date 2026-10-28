//! `LIMIT ... SLIDING` and `LIMIT ... TOKENS`: the state both modes keep in the one `u64` of a
//! limit value, and the arithmetic that decides a call.
//!
//! The top two bits of the word name the mode (`0` is the fixed window and keeps its plain
//! count), so persistence, snapshots and replicas carry the new modes in the records they
//! already have.
//!
//! * Sliding: `current << 31 | previous` request counts. The window start is not stored; it is
//!   the key deadline minus two windows, which is also when the whole state is dead.
//! * Tokens (GCRA): the theoretical arrival time in nanoseconds. The key deadline is that time
//!   rounded up to a millisecond, so a full bucket is a missing key.

use super::*;

pub(crate) const LIMIT_SLIDING_MAX: u64 = (1 << 31) - 1;
pub(crate) const LIMIT_TOKENS_MAX_CAPACITY: u64 = 1_000_000;
pub(crate) const LIMIT_TOKENS_MAX_REFILL_MS: u64 = u32::MAX as u64;

const MODE_SHIFT: u32 = 62;
const PAYLOAD: u64 = (1 << MODE_SHIFT) - 1;
const SLIDING: u64 = 1 << MODE_SHIFT;
const TOKENS: u64 = 2 << MODE_SHIFT;
const COUNT_BITS: u32 = 31;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
const NANOS_PER_MILLI: u64 = 1_000_000;

/// What a mode decided before the shard writes anything.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Step {
    Allow {
        state: u64,
        deadline: Option<u64>,
        remaining: u64,
    },
    Reject {
        retry_after_ms: u64,
    },
}

/// `state` is the stored word and `deadline` the key deadline, both `None` for a missing key.
pub(super) fn sliding(
    state: Option<(u64, Option<u64>)>,
    now: u64,
    max: u64,
    window: u64,
) -> Result<Step, LimitError> {
    let mut start = now;
    let mut deadline = Some(now.saturating_add(window.saturating_mul(2)));
    let (mut current, mut previous) = (0u64, 0u64);

    if let Some((word, key_deadline)) = state {
        if word & !PAYLOAD != SLIDING {
            return Err(LimitError::Mode);
        }

        // A key whose deadline was removed or moved by EXPIRE/PERSIST has lost its window
        // start and begins a new window.
        if let Some(key_deadline) = key_deadline {
            let opened = key_deadline.saturating_sub(window.saturating_mul(2));
            let rolled = now.saturating_sub(opened) / window;

            if rolled <= 1 {
                start = opened;
                deadline = None;
                current = (word >> COUNT_BITS) & COUNT_MASK;
                previous = word & COUNT_MASK;

                if rolled == 1 {
                    previous = current;
                    current = 0;
                    start = opened.saturating_add(window);
                    deadline = Some(start.saturating_add(window.saturating_mul(2)));
                }
            }
        }
    }

    let elapsed = now.saturating_sub(start).min(window);
    let weighted =
        (u128::from(previous) * u128::from(window - elapsed) / u128::from(window)) as u64;
    let estimate = current + weighted;

    if estimate >= max {
        return Ok(Step::Reject {
            retry_after_ms: sliding_retry(current, previous, elapsed, window, max),
        });
    }

    Ok(Step::Allow {
        state: SLIDING | ((current + 1) << COUNT_BITS) | previous,
        deadline,
        remaining: max - estimate - 1,
    })
}

/// Milliseconds until the same call passes, assuming nothing else is counted meanwhile.
fn sliding_retry(current: u64, previous: u64, elapsed: u64, window: u64, max: u64) -> u64 {
    let (window, elapsed) = (i128::from(window), i128::from(elapsed));
    // While the current window has room, only the weighted previous window blocks the call.
    // Once it is full, the call waits for the roll, where the full window becomes the
    // previous one.
    let (room, carry, base) = if current < max {
        (i128::from(max - current), i128::from(previous), -elapsed)
    } else {
        (i128::from(max), i128::from(current), window - elapsed)
    };
    // The longest remaining span of the window at which the carry still leaves `room`.
    let spare = ((room * window - 1) / carry.max(1)).min(window);
    let wait = base + window - spare;

    u64::try_from(wait.max(1)).unwrap_or(u64::MAX)
}

pub(super) fn tokens(
    state: Option<u64>,
    now: u64,
    capacity: u64,
    refill: u64,
    cost: u64,
) -> Result<Step, LimitError> {
    let per_token = refill * NANOS_PER_MILLI / capacity;
    let burst = per_token * capacity;
    let now_ns = now.checked_mul(NANOS_PER_MILLI).ok_or(LimitError::Range)?;
    let mut arrival = now_ns;

    if let Some(word) = state {
        if word & !PAYLOAD != TOKENS {
            return Err(LimitError::Mode);
        }

        arrival = arrival.max(word & PAYLOAD);
    }

    let next = arrival
        .checked_add(per_token * cost)
        .filter(|next| next.saturating_add(burst) <= PAYLOAD)
        .ok_or(LimitError::Range)?;
    let allowed_from = next.saturating_sub(burst);

    if now_ns < allowed_from {
        return Ok(Step::Reject {
            retry_after_ms: (allowed_from - now_ns).div_ceil(NANOS_PER_MILLI),
        });
    }

    Ok(Step::Allow {
        state: TOKENS | next,
        deadline: Some(next.div_ceil(NANOS_PER_MILLI)),
        remaining: (now_ns + burst - next) / per_token,
    })
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// The stored count of a fixed-window limit.
    pub(super) fn limit_state(&self, key: &[u8]) -> Result<u64, LimitError> {
        let used = self.limit_word(key)?;

        if used & !PAYLOAD != 0 {
            return Err(LimitError::Mode);
        }

        Ok(used)
    }

    fn limit_word(&self, key: &[u8]) -> Result<u64, LimitError> {
        match self.object_value(key).ok().flatten() {
            Some(Value::Limit(limit)) => Ok(limit.used),
            _ => Err(LimitError::WrongType),
        }
    }

    fn limit_slot(&mut self, key: &[u8]) -> Result<(Option<u64>, u64), LimitError> {
        self.remove_expired(key);

        match self.value_kind(key) {
            None => Ok((None, self.now_unix_millis().get())),
            Some(ValueKind::Limit) => {
                Ok((Some(self.limit_word(key)?), self.now_unix_millis().get()))
            }
            Some(_) => Err(LimitError::WrongType),
        }
    }

    pub(crate) fn plan_limit_sliding(
        &mut self,
        key: &[u8],
        max: u64,
        window_ms: u64,
    ) -> Result<LimitDecision, LimitError> {
        let (word, now) = self.limit_slot(key)?;
        let state = word.map(|word| {
            let deadline = self.deadline(self.active, key).map(UnixMillis::get);

            (word, deadline)
        });

        Ok(decision(
            sliding(state, now, max, window_ms)?,
            word.is_none(),
        ))
    }

    pub(crate) fn plan_limit_tokens(
        &mut self,
        key: &[u8],
        capacity: u64,
        refill_ms: u64,
        cost: u64,
    ) -> Result<LimitDecision, LimitError> {
        let (word, now) = self.limit_slot(key)?;

        Ok(decision(
            tokens(word, now, capacity, refill_ms, cost)?,
            word.is_none(),
        ))
    }
}

fn decision(step: Step, fresh: bool) -> LimitDecision {
    match step {
        Step::Allow {
            state,
            deadline,
            remaining,
        } => LimitDecision::Allow {
            used: state,
            window_end: deadline,
            remaining,
            fresh,
        },
        Step::Reject { retry_after_ms } => LimitDecision::RejectRetry { retry_after_ms },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs the sliding window against a clock, keeping state and deadline like the shard.
    struct Sim {
        state: Option<(u64, u64)>,
        max: u64,
        window: u64,
    }

    impl Sim {
        fn call(&mut self, now: u64) -> Result<Step, LimitError> {
            if self.state.is_some_and(|(_, deadline)| now >= deadline) {
                self.state = None;
            }

            let step = sliding(
                self.state.map(|(word, deadline)| (word, Some(deadline))),
                now,
                self.max,
                self.window,
            )?;

            Ok(step)
        }

        fn apply(&mut self, step: &Step) -> bool {
            let Step::Allow {
                state, deadline, ..
            } = step
            else {
                return false;
            };
            let deadline = deadline.unwrap_or_else(|| self.state.expect("kept deadline").1);

            self.state = Some((*state, deadline));
            true
        }
    }

    #[test]
    fn sliding_counts_within_one_window_like_a_fixed_window() {
        let mut sim = Sim {
            state: None,
            max: 3,
            window: 100,
        };

        for expected in [2, 1, 0] {
            let step = sim.call(1_000).unwrap();
            assert!(matches!(step, Step::Allow { remaining, .. } if remaining == expected));
            sim.apply(&step);
        }

        assert!(matches!(sim.call(1_050).unwrap(), Step::Reject { .. }));
    }

    #[test]
    fn sliding_does_not_allow_the_double_burst_across_a_boundary() {
        let mut sim = Sim {
            state: None,
            max: 4,
            window: 100,
        };

        // The window opens at the first call, 1_000, and ends at 1_100.
        let step = sim.call(1_000).unwrap();
        sim.apply(&step);

        for _ in 0..3 {
            let step = sim.call(1_090).unwrap();
            assert!(sim.apply(&step));
        }

        // The roll: the whole previous window still counts.
        assert!(matches!(sim.call(1_100).unwrap(), Step::Reject { .. }));
        // Half way through the next window half of it has aged out.
        let step = sim.call(1_150).unwrap();
        assert!(matches!(step, Step::Allow { remaining: 1, .. }));
    }

    #[test]
    fn sliding_state_forgets_everything_after_two_windows() {
        let mut sim = Sim {
            state: None,
            max: 2,
            window: 100,
        };

        for _ in 0..2 {
            let step = sim.call(1_000).unwrap();
            sim.apply(&step);
        }

        let step = sim.call(1_200).unwrap();
        assert!(matches!(step, Step::Allow { remaining: 1, .. }));
    }

    #[test]
    fn sliding_retry_after_is_the_first_millisecond_that_passes() {
        for (max, window) in [(1, 10), (3, 10), (5, 7), (4, 100), (2, 1)] {
            for fill_at in [0, 3, 9, 12] {
                for probe in [0, 1, 4, 9, 11] {
                    let mut sim = Sim {
                        state: None,
                        max,
                        window,
                    };

                    for _ in 0..max {
                        let step = sim.call(1_000 + fill_at).unwrap();
                        sim.apply(&step);
                    }

                    let now = 1_000 + fill_at + probe;
                    let Step::Reject { retry_after_ms } = sim.call(now).unwrap() else {
                        continue;
                    };
                    let late = || Sim {
                        state: sim.state,
                        max,
                        window,
                    };

                    assert!(retry_after_ms >= 1);
                    assert!(
                        matches!(
                            late().call(now + retry_after_ms).unwrap(),
                            Step::Allow { .. }
                        ),
                        "max {max} window {window} fill {fill_at} probe {probe}"
                    );

                    if retry_after_ms > 1 {
                        assert!(
                            matches!(
                                late().call(now + retry_after_ms - 1).unwrap(),
                                Step::Reject { .. }
                            ),
                            "early pass: max {max} window {window} fill {fill_at} probe {probe}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn sliding_state_without_a_deadline_starts_a_new_window() {
        let step = sliding(Some((SLIDING | (2 << COUNT_BITS), None)), 5_000, 2, 100).unwrap();
        assert!(matches!(
            step,
            Step::Allow {
                remaining: 1,
                deadline: Some(5_200),
                ..
            }
        ));
    }

    #[test]
    fn modes_refuse_each_others_state() {
        assert_eq!(sliding(Some((5, Some(9))), 1, 2, 10), Err(LimitError::Mode));
        assert_eq!(
            sliding(Some((TOKENS | 5, Some(9))), 1, 2, 10),
            Err(LimitError::Mode)
        );
        assert_eq!(tokens(Some(5), 1, 2, 10, 1), Err(LimitError::Mode));
        assert_eq!(
            tokens(Some(SLIDING | 5), 1, 2, 10, 1),
            Err(LimitError::Mode)
        );
    }

    fn token_state(step: &Step) -> u64 {
        match step {
            Step::Allow { state, .. } => *state,
            Step::Reject { .. } => panic!("expected the call to pass"),
        }
    }

    #[test]
    fn tokens_spend_the_burst_then_refill_at_the_set_rate() {
        // 5 tokens over 1000 ms: one token every 200 ms.
        let mut state = None;

        for expected in [4, 3, 2, 1, 0] {
            let step = tokens(state, 10_000, 5, 1_000, 1).unwrap();
            assert!(matches!(step, Step::Allow { remaining, .. } if remaining == expected));
            state = Some(token_state(&step));
        }

        let Step::Reject { retry_after_ms } = tokens(state, 10_000, 5, 1_000, 1).unwrap() else {
            panic!("an empty bucket must refuse");
        };
        assert_eq!(retry_after_ms, 200);
        assert!(matches!(
            tokens(state, 10_000 + 199, 5, 1_000, 1).unwrap(),
            Step::Reject { retry_after_ms: 1 }
        ));
        assert!(matches!(
            tokens(state, 10_000 + 200, 5, 1_000, 1).unwrap(),
            Step::Allow { remaining: 0, .. }
        ));
    }

    #[test]
    fn tokens_cost_takes_several_tokens_and_refusal_takes_none() {
        let step = tokens(None, 1_000, 10, 1_000, 4).unwrap();
        assert!(matches!(step, Step::Allow { remaining: 6, .. }));
        let state = Some(token_state(&step));

        assert!(matches!(
            tokens(state, 1_000, 10, 1_000, 7).unwrap(),
            Step::Reject {
                retry_after_ms: 100
            }
        ));
        // The refused call wrote nothing, so the 6 tokens are all still there.
        assert!(matches!(
            tokens(state, 1_000, 10, 1_000, 6).unwrap(),
            Step::Allow { remaining: 0, .. }
        ));
    }

    #[test]
    fn tokens_deadline_is_when_the_bucket_is_full_again() {
        let Step::Allow { deadline, .. } = tokens(None, 1_000, 10, 1_000, 3).unwrap() else {
            panic!("expected the call to pass");
        };
        assert_eq!(deadline, Some(1_300));
    }

    #[test]
    fn tokens_idle_time_never_banks_more_than_the_capacity() {
        let state = Some(token_state(&tokens(None, 1_000, 3, 300, 3).unwrap()));
        let step = tokens(state, 1_000_000, 3, 300, 1).unwrap();
        assert!(matches!(step, Step::Allow { remaining: 2, .. }));
    }

    #[test]
    fn tokens_faster_than_one_per_millisecond_keep_their_rate() {
        // 1000 tokens per 100 ms is ten per millisecond.
        let mut state = None;

        for _ in 0..1_000 {
            let step = tokens(state, 5_000, 1_000, 100, 1).unwrap();
            state = Some(token_state(&step));
        }

        assert!(matches!(
            tokens(state, 5_000, 1_000, 100, 1).unwrap(),
            Step::Reject { .. }
        ));
        assert!(matches!(
            tokens(state, 5_001, 1_000, 100, 10).unwrap(),
            Step::Allow { remaining: 0, .. }
        ));
    }

    #[test]
    fn tokens_reject_a_clock_that_does_not_fit_the_packed_state() {
        assert_eq!(
            tokens(None, u64::MAX / 2, 10, 1_000, 1),
            Err(LimitError::Range)
        );
        assert_eq!(tokens(None, u64::MAX, 10, 1_000, 1), Err(LimitError::Range));
    }
}
