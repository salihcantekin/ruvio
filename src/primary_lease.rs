//! The primary lease a data node holds from `ruvio coordinator` (`primary-lease yes`).
//!
//! Each coordinator renews it on every ping, in two steps on one connection:
//! `RUVIO LEASE BEGIN` records the node's own instant and answers a token, then
//! `RUVIO LEASE GRANT token coordinator voters epoch lease_ms` grants the lease from that
//! instant. The lease runs on the node's clock from the BEGIN instant, which comes before the
//! coordinator sent the grant: a grant that arrives late only shortens it, and the coordinator,
//! which counts from when it sent the grant, never believes a lease ended that the node still
//! holds (as long as the two clocks run at nearly the same rate).
//!
//! A primary takes writes while grants from a majority of the coordinators run for longer
//! than [`GATE_MARGIN`]. The shards check this on their 100 ms maintenance tick and close their
//! write gate; the write path reads only the gate. The table is shared by every shard of the
//! process.

use std::{collections::VecDeque, sync::Mutex, time::Duration};

/// BEGIN instants kept for their grant. A coordinator sends its grant right after BEGIN on
/// the same connection, so only a few are ever pending.
const PENDING_BEGINS: usize = 64;

/// The longest lease a grant may ask for.
pub const MAX_LEASE: Duration = Duration::from_secs(60);

/// A primary stops taking writes this long before its lease ends: one maintenance tick, plus
/// as much again and more for a tick that runs late (a long script, a slow log sync).
pub const GATE_MARGIN: Duration = Duration::from_millis(300);

/// `limits` with the script time cut to what a lease with `left` to run holds past
/// [`GATE_MARGIN`] (at least 1 ms), so a script cannot keep writing after the gate closes.
pub fn limit_script_time(
    mut limits: crate::config::ScriptLimits,
    left: Duration,
) -> crate::config::ScriptLimits {
    let room = left.saturating_sub(GATE_MARGIN).as_millis();

    limits.max_time_ms = limits
        .max_time_ms
        .min(usize::try_from(room).unwrap_or(usize::MAX).max(1));
    limits
}

/// The lease clock: on Linux `CLOCK_BOOTTIME`, which keeps counting while the host is
/// suspended (Rust's `Instant` uses `CLOCK_MONOTONIC`, which stops); on macOS
/// `CLOCK_MONOTONIC`, which counts sleep there. Elsewhere `Instant`. The value is time since an
/// arbitrary origin.
pub fn now() -> Duration {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
        #[cfg(target_vendor = "apple")]
        const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;
        let mut now = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        // SAFETY: `now` is a valid timespec for the call to fill; the clock id is a constant
        // this platform defines.
        #[allow(unsafe_code)]
        let status = unsafe { libc::clock_gettime(CLOCK, &mut now) };

        if status == 0 {
            return Duration::new(now.tv_sec as u64, now.tv_nsec as u32);
        }
    }
    static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

    ORIGIN.get_or_init(std::time::Instant::now).elapsed()
}

/// The most coordinators a grant may name. The coordinators refuse a larger `peers` list
/// when they grant leases.
pub const MAX_VOTERS: u32 = 255;

/// One coordinator's running grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Grant {
    coordinator: u64,
    /// How many coordinators its sender counts, as of its latest grant.
    voters: u32,
    epoch: u64,
    until: Duration,
}

/// Why a grant was not taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// No BEGIN with this token is pending (unknown, already used, or pushed out by newer
    /// BEGINs).
    UnknownToken,
    /// The lease would already have ended: the grant took longer than the lease.
    Stale,
    /// A coordinator granted a newer epoch already.
    OldEpoch,
    /// `voters` or `lease_ms` out of range.
    Invalid,
    /// The node does not take writes of its own: it follows a primary.
    NotPrimary,
}

impl Refused {
    pub fn message(self) -> &'static [u8] {
        match self {
            Self::UnknownToken => b"ERR RUVIO LEASE GRANT names no pending BEGIN",
            Self::Stale => b"ERR RUVIO LEASE GRANT arrived after its lease ended",
            Self::OldEpoch => b"ERR RUVIO LEASE GRANT is for an older epoch",
            Self::Invalid => b"ERR RUVIO LEASE GRANT voters or lease_ms is out of range",
            Self::NotPrimary => b"ERR RUVIO LEASE GRANT on a replica",
        }
    }
}

/// The lease state, without the lock; every instant is passed in so tests choose them.
#[derive(Debug, Default)]
pub struct Leases {
    next_token: u64,
    begins: VecDeque<(u64, Duration)>,
    /// Running grants only: one that ended is dropped at the next grant, and never counts.
    grants: Vec<Grant>,
}

impl Leases {
    /// `RUVIO LEASE BEGIN`: records `now` and answers the token its grant names.
    pub fn begin(&mut self, now: Duration) -> u64 {
        self.next_token += 1;
        if self.begins.len() == PENDING_BEGINS {
            self.begins.pop_front();
        }
        self.begins.push_back((self.next_token, now));
        self.next_token
    }

    /// `RUVIO LEASE GRANT`: coordinator `coordinator` of `voters` grants the lease for
    /// `lease` from the BEGIN instant `token` names.
    pub fn grant(
        &mut self,
        token: u64,
        coordinator: u64,
        voters: u32,
        epoch: u64,
        lease: Duration,
        now: Duration,
    ) -> Result<(), Refused> {
        if voters == 0 || voters > MAX_VOTERS || lease.is_zero() || lease > MAX_LEASE {
            return Err(Refused::Invalid);
        }
        let at = self
            .begins
            .iter()
            .position(|&(known, _)| known == token)
            .ok_or(Refused::UnknownToken)?;
        let (_, begun) = self.begins.remove(at).unwrap_or((token, now));
        let until = begun + lease;

        if until <= now {
            return Err(Refused::Stale);
        }
        // A grant that ended can no longer open the gate, so it may not refuse others either:
        // coordinators that start over (a lost `data_dir`, Raft files deleted to go back a
        // release) record the primary at a lower epoch, and their grants are taken once the
        // grants of the newer epoch ran out, at most one lease later.
        self.grants.retain(|grant| grant.until > now);
        let newest = self.grants.iter().map(|grant| grant.epoch).max();

        if newest.is_some_and(|newest| epoch < newest) {
            return Err(Refused::OldEpoch);
        }
        // A newer epoch supersedes the grants of older ones, and the coordinator counts they
        // named with them.
        self.grants.retain(|grant| grant.epoch == epoch);
        match self
            .grants
            .iter_mut()
            .find(|grant| grant.coordinator == coordinator)
        {
            Some(grant) => {
                grant.until = grant.until.max(until);
                grant.voters = voters;
            }
            None => self.grants.push(Grant {
                coordinator,
                voters,
                epoch,
                until,
            }),
        }
        Ok(())
    }

    /// How many grants run at `now`, and how many make a majority (0 when none runs). The
    /// majority is of the largest coordinator count a running grant names, so a grant naming
    /// fewer does not lower the bar while one naming more runs.
    pub fn count(&self, now: Duration) -> (usize, usize) {
        let (running, voters) = self
            .grants
            .iter()
            .filter(|grant| grant.until > now)
            .fold((0, 0), |(running, voters), grant| {
                (running + 1, voters.max(grant.voters as usize))
            });

        (running, if voters == 0 { 0 } else { voters / 2 + 1 })
    }

    /// Whether a majority of the coordinators' grants run past `now` + [`GATE_MARGIN`]: the
    /// write gate is open only then.
    pub fn holds(&self, now: Duration) -> bool {
        self.remaining(now) > GATE_MARGIN
    }

    /// How long the majority keeps holding from `now`: the needed-th latest end among the
    /// running grants. Zero when it does not hold.
    pub fn remaining(&self, now: Duration) -> Duration {
        let (running, needed) = self.count(now);

        if needed == 0 || running < needed {
            return Duration::ZERO;
        }
        let mut ends: Vec<Duration> = self
            .grants
            .iter()
            .filter(|grant| grant.until > now)
            .map(|grant| grant.until)
            .collect();

        ends.sort_unstable_by(|left, right| right.cmp(left));
        ends[needed - 1].saturating_sub(now)
    }

    /// Forgets every grant: the node stopped being a primary.
    pub fn clear(&mut self) {
        self.grants.clear();
        self.begins.clear();
    }
}

/// [`Leases`] shared by the shards of one process.
#[derive(Debug, Default)]
pub struct LeaseTable(Mutex<Leases>);

impl LeaseTable {
    pub fn with<R>(&self, act: impl FnOnce(&mut Leases) -> R) -> R {
        act(&mut self.0.lock().unwrap_or_else(|error| error.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE: Duration = Duration::from_millis(2000);

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    #[test]
    fn a_majority_of_running_grants_holds_the_lease_from_each_begin_instant() {
        let start = ms(10_000);
        let mut leases = Leases::default();

        assert!(!leases.holds(start), "nothing granted yet");
        let first = leases.begin(start);
        // Granted 300 ms after its BEGIN: the lease still counts from the BEGIN.
        leases
            .grant(first, 1, 3, 1, LEASE, start + ms(300))
            .unwrap();
        assert!(
            !leases.holds(start + ms(300)),
            "one of three is no majority"
        );
        let second = leases.begin(start + ms(500));
        leases
            .grant(second, 2, 3, 1, LEASE, start + ms(510))
            .unwrap();
        assert!(leases.holds(start + ms(510)));
        // The majority ends with the earlier of the two grants: BEGIN + lease.
        assert_eq!(leases.remaining(start + ms(1000)), ms(1000));
        assert_eq!(leases.remaining(start + ms(1999)), ms(1));
        assert_eq!(leases.remaining(start + ms(2000)), Duration::ZERO);
        // The gate closes a margin before the end.
        assert!(leases.holds(start + ms(1699)));
        assert!(!leases.holds(start + ms(1700)));

        // A third coordinator keeps the majority with the second.
        let third = leases.begin(start + ms(1500));
        leases
            .grant(third, 3, 3, 1, LEASE, start + ms(1500))
            .unwrap();
        assert_eq!(leases.remaining(start + ms(2400)), ms(100));
        assert_eq!(leases.remaining(start + ms(2500)), Duration::ZERO);
    }

    #[test]
    fn a_grant_needs_its_own_pending_begin_in_time_and_a_current_epoch() {
        let start = ms(10_000);
        let mut leases = Leases::default();

        assert_eq!(
            leases.grant(7, 1, 3, 1, LEASE, start),
            Err(Refused::UnknownToken)
        );
        let token = leases.begin(start);
        // Arrived after the lease it asks for would have ended.
        assert_eq!(
            leases.grant(token, 1, 3, 1, LEASE, start + LEASE),
            Err(Refused::Stale)
        );
        // A token is used once.
        assert_eq!(
            leases.grant(token, 1, 3, 1, LEASE, start),
            Err(Refused::UnknownToken)
        );
        for (voters, lease) in [(0, LEASE), (3, Duration::ZERO), (3, MAX_LEASE + ms(1))] {
            let token = leases.begin(start);

            assert_eq!(
                leases.grant(token, 1, voters, 1, lease, start),
                Err(Refused::Invalid)
            );
        }

        // A newer epoch drops the older grants; an older one is refused afterwards.
        for coordinator in [1, 2] {
            let token = leases.begin(start);
            leases
                .grant(token, coordinator, 3, 4, LEASE, start)
                .unwrap();
        }
        assert!(leases.holds(start));
        let token = leases.begin(start);
        leases.grant(token, 3, 3, 5, LEASE, start).unwrap();
        assert!(!leases.holds(start), "epoch 5 has one grant so far");
        let token = leases.begin(start);
        assert_eq!(
            leases.grant(token, 1, 3, 4, LEASE, start),
            Err(Refused::OldEpoch)
        );

        leases.clear();
        assert!(!leases.holds(start));
        assert_eq!(leases.count(start), (0, 0));
    }

    #[test]
    fn a_grant_naming_fewer_coordinators_does_not_lower_the_majority() {
        let start = ms(10_000);
        let mut leases = Leases::default();
        let token = leases.begin(start);

        leases.grant(token, 1, 3, 1, LEASE, start).unwrap();
        assert!(!leases.holds(start));
        // A stray grant that claims a single coordinator: still two of three needed.
        let token = leases.begin(start);
        leases.grant(token, 9, 1, 1, LEASE, start).unwrap();
        assert_eq!(leases.count(start), (2, 2));
        let token = leases.begin(start);
        leases.grant(token, 2, 5, 1, LEASE, start).unwrap();
        // Five named once: three needed from then on.
        assert_eq!(leases.count(start), (3, 3));
        assert!(leases.holds(start));
    }

    #[test]
    fn grants_that_ended_do_not_refuse_coordinators_that_start_over_at_a_lower_epoch() {
        let start = ms(10_000);
        let mut leases = Leases::default();

        for coordinator in [1, 2] {
            let token = leases.begin(start);
            leases
                .grant(token, coordinator, 3, 7, LEASE, start)
                .unwrap();
        }
        // While the epoch 7 grants run, a lower epoch is refused.
        let token = leases.begin(start + ms(1000));
        assert_eq!(
            leases.grant(token, 1, 3, 1, LEASE, start + ms(1000)),
            Err(Refused::OldEpoch)
        );
        // Once they ended, the re-initialised coordinators' epoch 1 is taken.
        let later = start + LEASE;
        for coordinator in [1, 2] {
            let token = leases.begin(later);
            leases
                .grant(token, coordinator, 3, 1, LEASE, later)
                .unwrap();
        }
        assert!(leases.holds(later));
        assert_eq!(leases.count(later), (2, 2));
    }

    #[test]
    fn the_majority_follows_the_running_grants_of_the_current_epoch() {
        let start = ms(10_000);
        let mut leases = Leases::default();

        for coordinator in [1, 2, 3] {
            let token = leases.begin(start);
            leases
                .grant(token, coordinator, 5, 1, LEASE, start)
                .unwrap();
        }
        assert_eq!(leases.count(start), (3, 3));
        // A new epoch drops the old grants and the count they named.
        for coordinator in [1, 2] {
            let token = leases.begin(start);
            leases
                .grant(token, coordinator, 3, 2, LEASE, start)
                .unwrap();
        }
        assert_eq!(leases.count(start), (2, 2));
        assert!(leases.holds(start));

        // A coordinator still counting five keeps the bar at three while its grant runs; once
        // it stops (the set shrank) and its grant ends, two of three hold again.
        let token = leases.begin(start);
        leases.grant(token, 4, 5, 2, ms(1000), start).unwrap();
        assert_eq!(leases.count(start), (3, 3));
        let later = start + ms(1000);
        assert_eq!(leases.count(later), (2, 2));
        assert!(leases.holds(later));
        // A coordinator's latest grant sets its count.
        let token = leases.begin(later);
        leases.grant(token, 1, 5, 2, LEASE, later).unwrap();
        assert_eq!(leases.count(later), (2, 3));
        let token = leases.begin(later);
        leases.grant(token, 1, 3, 2, LEASE, later).unwrap();
        assert_eq!(leases.count(later), (2, 2));

        // Absurd counts are refused.
        for voters in [MAX_VOTERS + 1, u32::MAX] {
            let token = leases.begin(later);
            assert_eq!(
                leases.grant(token, 1, voters, 2, LEASE, later),
                Err(Refused::Invalid)
            );
        }
        let token = leases.begin(later);
        leases.grant(token, 1, MAX_VOTERS, 2, LEASE, later).unwrap();
        assert_eq!(leases.count(later), (2, MAX_VOTERS as usize / 2 + 1));
    }

    #[test]
    fn a_late_grant_never_extends_a_lease_and_only_the_newest_begins_are_kept() {
        let start = ms(10_000);
        let mut leases = Leases::default();
        let early = leases.begin(start);
        let late = leases.begin(start + ms(1000));

        leases
            .grant(late, 1, 1, 1, LEASE, start + ms(1000))
            .unwrap();
        // The earlier BEGIN's grant arrives last: it does not move the end back.
        leases
            .grant(early, 1, 1, 1, LEASE, start + ms(1001))
            .unwrap();
        assert_eq!(leases.remaining(start + ms(1001)), ms(1999));

        let oldest = leases.begin(start);
        for _ in 0..PENDING_BEGINS {
            leases.begin(start);
        }
        assert_eq!(
            leases.grant(oldest, 1, 1, 1, LEASE, start),
            Err(Refused::UnknownToken)
        );
    }

    #[test]
    fn the_lease_clock_moves_forward() {
        let first = now();
        std::thread::sleep(ms(2));

        assert!(now() > first);
    }

    #[test]
    fn script_time_stops_at_the_gate_margin() {
        let limits = crate::config::ScriptLimits::default();

        assert_eq!(limit_script_time(limits, ms(1000)).max_time_ms, 700);
        assert_eq!(limit_script_time(limits, ms(30_000)).max_time_ms, 1000);
        assert_eq!(limit_script_time(limits, ms(100)).max_time_ms, 1);
    }
}
