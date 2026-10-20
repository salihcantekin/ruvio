//! The primary lease a data node holds from `ruvio coordinator` (`primary-lease yes`).
//!
//! Each coordinator renews it on every ping, in two steps on one connection:
//! `RUVIO LEASE BEGIN` records the node's own instant and answers a token, then
//! `RUVIO LEASE GRANT token coordinator voters epoch lease_ms` grants the lease from that
//! instant. The lease runs on the node's monotonic clock from the BEGIN instant, which comes
//! before the coordinator sent the grant: a grant that arrives late only shortens it, and the
//! coordinator, which counts from when it sent the grant, never believes a lease ended that
//! the node still holds (as long as the two clocks run at nearly the same rate).
//!
//! A primary takes writes while grants from a majority of the coordinators are running. The
//! shards check this on their 100 ms maintenance tick and close their write gate; the write
//! path reads only the gate. The table is shared by every shard of the process.

use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant},
};

/// BEGIN instants kept for their grant. A coordinator sends its grant right after BEGIN on
/// the same connection, so only a few are ever pending.
const PENDING_BEGINS: usize = 64;

/// The longest lease a grant may ask for.
pub const MAX_LEASE: Duration = Duration::from_secs(60);

/// One coordinator's running grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Grant {
    coordinator: u64,
    /// How many coordinators there are, as that coordinator counts them.
    voters: u32,
    epoch: u64,
    until: Instant,
}

/// Why a grant was not taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// No BEGIN with this token is pending (unknown, or already used).
    UnknownToken,
    /// The lease would already have ended: the grant took longer than the lease.
    Stale,
    /// A coordinator granted a newer epoch already.
    OldEpoch,
    /// `voters` or `lease_ms` out of range.
    Invalid,
}

impl Refused {
    pub fn message(self) -> &'static [u8] {
        match self {
            Self::UnknownToken => b"ERR RUVIO LEASE GRANT names no pending BEGIN",
            Self::Stale => b"ERR RUVIO LEASE GRANT arrived after its lease ended",
            Self::OldEpoch => b"ERR RUVIO LEASE GRANT is for an older epoch",
            Self::Invalid => b"ERR RUVIO LEASE GRANT voters or lease_ms is out of range",
        }
    }
}

/// The lease state, without the lock; every instant is passed in so tests choose them.
#[derive(Debug, Default)]
pub struct Leases {
    next_token: u64,
    begins: VecDeque<(u64, Instant)>,
    grants: Vec<Grant>,
}

impl Leases {
    /// `RUVIO LEASE BEGIN`: records `now` and answers the token its grant names.
    pub fn begin(&mut self, now: Instant) -> u64 {
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
        now: Instant,
    ) -> Result<(), Refused> {
        if voters == 0 || lease.is_zero() || lease > MAX_LEASE {
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
        let newest = self.grants.iter().map(|grant| grant.epoch).max();

        if newest.is_some_and(|newest| epoch < newest) {
            return Err(Refused::OldEpoch);
        }
        // A newer epoch supersedes the grants of older ones.
        self.grants.retain(|grant| grant.epoch == epoch);
        match self
            .grants
            .iter_mut()
            .find(|grant| grant.coordinator == coordinator)
        {
            Some(grant) => {
                grant.voters = voters;
                grant.until = grant.until.max(until);
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

    /// How many grants run at `now`, and how many make a majority (0 when none ran yet).
    pub fn count(&self, now: Instant) -> (usize, usize) {
        let running = self.grants.iter().filter(|grant| grant.until > now);
        let voters = running
            .clone()
            .map(|grant| grant.voters as usize)
            .max()
            .unwrap_or(0);

        (
            running.count(),
            if voters == 0 { 0 } else { voters / 2 + 1 },
        )
    }

    /// Whether a majority of the coordinators' grants run at `now`.
    pub fn holds(&self, now: Instant) -> bool {
        let (running, needed) = self.count(now);

        needed > 0 && running >= needed
    }

    /// How long the majority keeps holding from `now`: the needed-th latest end among the
    /// running grants. Zero when it does not hold.
    pub fn remaining(&self, now: Instant) -> Duration {
        let (running, needed) = self.count(now);

        if needed == 0 || running < needed {
            return Duration::ZERO;
        }
        let mut ends: Vec<Instant> = self
            .grants
            .iter()
            .filter(|grant| grant.until > now)
            .map(|grant| grant.until)
            .collect();

        ends.sort_unstable_by(|left, right| right.cmp(left));
        ends[needed - 1].saturating_duration_since(now)
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
        let start = Instant::now();
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
        assert!(leases.holds(start + ms(1999)));
        assert!(!leases.holds(start + ms(2000)));
        assert_eq!(leases.remaining(start + ms(2000)), Duration::ZERO);

        // A third coordinator keeps the majority with the second.
        let third = leases.begin(start + ms(1500));
        leases
            .grant(third, 3, 3, 1, LEASE, start + ms(1500))
            .unwrap();
        assert!(leases.holds(start + ms(2400)));
        assert_eq!(leases.remaining(start + ms(2400)), ms(100));
        assert!(!leases.holds(start + ms(2500)));
    }

    #[test]
    fn a_grant_needs_its_own_pending_begin_in_time_and_a_current_epoch() {
        let start = Instant::now();
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
    fn a_late_grant_never_extends_a_lease_and_only_the_newest_begins_are_kept() {
        let start = Instant::now();
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
}
