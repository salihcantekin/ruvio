use std::{
    cell::Cell,
    time::{SystemTime, UNIX_EPOCH},
};

/// A persistence-safe absolute Unix timestamp with millisecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixMillis(u64);

impl UnixMillis {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// Latest deadline any command may store; larger values would not fit the
    /// signed 64-bit millisecond clock that `PEXPIRETIME` and `PTTL` report.
    pub const MAX_DEADLINE: u64 = i64::MAX as u64;

    /// `self + milliseconds`, or `None` when the result passes [`Self::MAX_DEADLINE`].
    pub const fn checked_add(self, milliseconds: u64) -> Option<Self> {
        match self.0.checked_add(milliseconds) {
            Some(value) if value <= Self::MAX_DEADLINE => Some(Self(value)),
            _ => None,
        }
    }
}

/// Time source used for expiration decisions.
///
/// The trait is a generic shard parameter, so production hot paths do not use
/// dynamic dispatch and tests can inject deterministic time. Implementations
/// must be non-decreasing for the lifetime of a shard.
pub trait Clock {
    fn now_unix_millis(&self) -> UnixMillis;
}

/// Wall clock used by normal server shards.
#[derive(Debug, Default)]
pub struct SystemClock {
    last_seen: Cell<u64>,
}

impl SystemClock {
    /// Create a clock that never reports a value below a recovered persistence
    /// timestamp.
    pub const fn with_floor(floor: UnixMillis) -> Self {
        Self {
            last_seen: Cell::new(floor.get()),
        }
    }

    /// Last value returned to expiration logic, including the recovery floor.
    pub fn last_seen(&self) -> UnixMillis {
        UnixMillis::new(self.last_seen.get())
    }

    fn observe_wall_time(&self, wall_time: UnixMillis) -> UnixMillis {
        let now = wall_time.get().max(self.last_seen.get());

        self.last_seen.set(now);
        UnixMillis::new(now)
    }
}

impl Clock for SystemClock {
    fn now_unix_millis(&self) -> UnixMillis {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let wall_time = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

        self.observe_wall_time(UnixMillis::new(wall_time))
    }
}

/// Deterministic wall input with the same monotonic clamp as SystemClock.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestWallClock {
    wall: std::rc::Rc<Cell<u64>>,
    observed: std::rc::Rc<SystemClock>,
}

#[cfg(test)]
impl TestWallClock {
    pub(crate) fn new(wall: u64, floor: u64) -> Self {
        Self {
            wall: std::rc::Rc::new(Cell::new(wall)),
            observed: std::rc::Rc::new(SystemClock::with_floor(UnixMillis::new(floor))),
        }
    }

    pub(crate) fn set(&self, wall: u64) {
        self.wall.set(wall);
    }
}

#[cfg(test)]
impl Clock for TestWallClock {
    fn now_unix_millis(&self) -> UnixMillis {
        self.observed
            .observe_wall_time(UnixMillis::new(self.wall.get()))
    }
}

/// Deterministic clock for expiration tests. Sharing a clone advances every shard
/// that holds it.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestClock {
    now: std::rc::Rc<Cell<u64>>,
    reads: std::rc::Rc<Cell<usize>>,
}

#[cfg(test)]
impl TestClock {
    pub(crate) fn new(now: u64) -> Self {
        Self {
            now: std::rc::Rc::new(Cell::new(now)),
            reads: std::rc::Rc::new(Cell::new(0)),
        }
    }

    pub(crate) fn set(&self, now: u64) {
        self.now.set(now);
    }

    pub(crate) fn reads(&self) -> usize {
        self.reads.get()
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now_unix_millis(&self) -> UnixMillis {
        self.reads.set(self.reads.get() + 1);
        UnixMillis::new(self.now.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_millis_checked_add_detects_overflow() {
        assert_eq!(
            UnixMillis::new(10).checked_add(5),
            Some(UnixMillis::new(15))
        );
        assert_eq!(UnixMillis::new(u64::MAX).checked_add(1), None);
    }

    #[test]
    fn system_clock_returns_a_post_epoch_timestamp() {
        assert!(SystemClock::default().now_unix_millis().get() > 0);
    }

    #[test]
    fn system_clock_never_moves_below_its_recovery_floor() {
        let clock = SystemClock::with_floor(UnixMillis::new(u64::MAX));

        assert_eq!(clock.last_seen(), UnixMillis::new(u64::MAX));
        assert_eq!(clock.now_unix_millis(), UnixMillis::new(u64::MAX));
    }

    #[test]
    fn observed_wall_time_clamps_backward_jumps_and_recovered_floors() {
        let clock = SystemClock::with_floor(UnixMillis::new(1_000));
        for (wall, expected) in [(900, 1_000), (1_500, 1_500), (1_100, 1_500), (2_000, 2_000)] {
            assert_eq!(
                clock.observe_wall_time(UnixMillis::new(wall)),
                UnixMillis::new(expected)
            );
        }
        let restarted = SystemClock::with_floor(clock.last_seen());
        assert_eq!(
            restarted.observe_wall_time(UnixMillis::new(500)),
            UnixMillis::new(2_000)
        );
        assert_eq!(
            restarted.observe_wall_time(UnixMillis::new(3_000)),
            UnixMillis::new(3_000)
        );
    }
}

/// A cheap monotonic stamp for timing one command (`SLOWLOG`): the CPU's cycle counter where
/// there is one, so the command path makes no clock call. `Instant::now` goes through the
/// vDSO and showed 7.7% of a shard's time under a pipelined `SADD` load on an Azure VM.
///
/// x86-64 reads the TSC (invariant on every CPU this server targets) and converts with a
/// rate measured once against `Instant` (5 ms, at the first stamp); aarch64 reads the
/// virtual counter, whose rate the hardware reports. Other targets fall back to `Instant`.
/// The result is for diagnostics: a slow-log entry a few percent off is fine, so no
/// re-calibration is done.
#[derive(Clone, Copy, Debug)]
pub struct Stamp(u64);

impl Stamp {
    /// Measures the counter rate now (5 ms on x86-64), so no shard pays it on its first
    /// timed command. Called once at server start; a stamp before it calibrates itself.
    pub fn calibrate() {
        let _ = ticks_per_micro();
    }

    #[inline]
    pub fn now() -> Self {
        Self(ticks())
    }

    /// Microseconds since this stamp.
    #[inline]
    pub fn elapsed_micros(self) -> u64 {
        let elapsed = ticks().saturating_sub(self.0);

        ((elapsed as f64) / ticks_per_micro()) as u64
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
#[allow(unsafe_code)] // measured: the vDSO clock was 7.7% of a shard's time under a pipelined SADD load
fn ticks() -> u64 {
    // SAFETY: `rdtsc` is unprivileged on every x86-64 operating system this server runs on
    // and has no memory effects.
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[cfg(target_arch = "aarch64")]
#[inline]
#[allow(unsafe_code)] // see the x86-64 variant
fn ticks() -> u64 {
    let value: u64;

    // SAFETY: reads the virtual counter, which EL0 may read; no memory effects.
    unsafe { core::arch::asm!("mrs {}, cntvct_el0", out(reg) value, options(nomem, nostack)) };
    value
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn ticks() -> u64 {
    use std::sync::OnceLock;
    static START: OnceLock<std::time::Instant> = OnceLock::new();

    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_nanos() as u64
}

fn ticks_per_micro() -> f64 {
    use std::sync::OnceLock;
    static RATE: OnceLock<f64> = OnceLock::new();

    *RATE.get_or_init(measure_ticks_per_micro)
}

#[cfg(target_arch = "aarch64")]
#[allow(unsafe_code)] // see `ticks`
fn measure_ticks_per_micro() -> f64 {
    let frequency: u64;

    // SAFETY: reads the counter frequency register, which EL0 may read; no memory effects.
    unsafe { core::arch::asm!("mrs {}, cntfrq_el0", out(reg) frequency, options(nomem, nostack)) };
    (frequency.max(1) as f64) / 1_000_000.0
}

/// The rate is two (wall clock, counter) pairs 5 ms apart. A preemption between the two reads
/// of one pair would skew the rate by its length, so each pair is the tightest of a few reads:
/// the wall time halfway between two `Instant`s around one counter read.
#[cfg(target_arch = "x86_64")]
fn measure_ticks_per_micro() -> f64 {
    fn pair() -> (std::time::Instant, u64) {
        let mut best: Option<(std::time::Duration, std::time::Instant, u64)> = None;

        for _ in 0..16 {
            let before = std::time::Instant::now();
            let counter = ticks();
            let width = before.elapsed();

            if best.is_none_or(|(narrowest, _, _)| width < narrowest) {
                best = Some((width, before + width / 2, counter));
            }
        }

        let (_, wall, counter) = best.expect("sixteen reads");

        (wall, counter)
    }

    let (first_wall, first) = pair();

    while first_wall.elapsed() < std::time::Duration::from_millis(5) {
        std::hint::spin_loop();
    }

    let (last_wall, last) = pair();
    let micros = last_wall.duration_since(first_wall).as_nanos() as f64 / 1000.0;

    last.saturating_sub(first).max(1) as f64 / micros
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn measure_ticks_per_micro() -> f64 {
    1000.0
}

#[cfg(test)]
mod stamp_tests {
    use super::Stamp;

    #[test]
    fn a_stamp_measures_a_sleep_within_a_few_percent() {
        // Calibrated first: when this test is the first to stamp, measuring the rate inside
        // `elapsed_micros` would add its 5 ms to the wall time only.
        Stamp::calibrate();
        // Compared with the wall clock over the same interval: a loaded machine may oversleep
        // a lot, but both clocks see the same oversleep. Wall readings on both sides of each
        // stamp read bound the stamped interval, however long the thread waits between them.
        let outer = std::time::Instant::now();
        let stamp = Stamp::now();
        let inner = std::time::Instant::now();

        std::thread::sleep(std::time::Duration::from_millis(20));
        let shortest = inner.elapsed().as_micros() as f64;
        let micros = stamp.elapsed_micros() as f64;
        let longest = outer.elapsed().as_micros() as f64;

        assert!(micros >= 19_000.0, "{micros}");
        assert!(
            micros >= shortest * 0.9 - 500.0 && micros <= longest * 1.1 + 500.0,
            "stamp {micros} us, wall between {shortest} and {longest} us"
        );
    }
}
