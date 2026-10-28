use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::ConnectionLimits;

/// Process-wide admission counters for the metrics on shard 0. Each moves only on a
/// cold path: an accept, a refused accept, a refused command, a frame that ends the
/// connection.
static ACCEPTED: AtomicU64 = AtomicU64::new(0);
static REJECTED: AtomicU64 = AtomicU64::new(0);
static RATE_LIMITED: AtomicU64 = AtomicU64::new(0);
static PROTOCOL_ERRORS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AdmissionTotals {
    pub(crate) accepted: u64,
    pub(crate) rejected: u64,
    pub(crate) rate_limited: u64,
    pub(crate) protocol_errors: u64,
}

pub(crate) fn admission_totals() -> AdmissionTotals {
    AdmissionTotals {
        accepted: ACCEPTED.load(Ordering::Relaxed),
        rejected: REJECTED.load(Ordering::Relaxed),
        rate_limited: RATE_LIMITED.load(Ordering::Relaxed),
        protocol_errors: PROTOCOL_ERRORS.load(Ordering::Relaxed),
    }
}

/// A frame the parser could not read closed the connection.
pub(crate) fn note_protocol_error() {
    PROTOCOL_ERRORS.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Default)]
struct ConnectionCounts {
    /// Kept under the same lock as the counts, so `CONFIG SET` can change them while
    /// connections come and go.
    limits: ConnectionLimits,
    global: usize,
    by_ip: HashMap<IpAddr, usize>,
    by_user: HashMap<Vec<u8>, usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionLimiter {
    counts: Arc<Mutex<ConnectionCounts>>,
}

impl ConnectionLimiter {
    pub(crate) fn new(limits: ConnectionLimits) -> Self {
        Self {
            counts: Arc::new(Mutex::new(ConnectionCounts {
                limits,
                ..ConnectionCounts::default()
            })),
        }
    }

    /// New limits for the next accept and login; connections already open stay.
    pub(crate) fn set_limits(&self, limits: ConnectionLimits) {
        lock_counts(&self.counts).limits = limits;
    }

    pub(crate) fn try_acquire(&self, ip: IpAddr) -> Option<ConnectionPermit> {
        let mut counts = lock_counts(&self.counts);
        let per_ip = counts.by_ip.get(&ip).copied().unwrap_or(0);

        if counts.global >= counts.limits.max_global || per_ip >= counts.limits.max_per_ip {
            REJECTED.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        ACCEPTED.fetch_add(1, Ordering::Relaxed);
        counts.global += 1;
        *counts.by_ip.entry(ip).or_insert(0) += 1;
        Some(ConnectionPermit {
            limiter: self.clone(),
            ip,
            user: None,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionPermit {
    limiter: ConnectionLimiter,
    ip: IpAddr,
    user: Option<Vec<u8>>,
}

impl ConnectionPermit {
    pub(crate) fn bind_user(&mut self, name: &[u8]) -> bool {
        let key = name.to_ascii_lowercase();

        if self.user.as_deref() == Some(key.as_slice()) {
            return true;
        }

        let mut counts = lock_counts(&self.limiter.counts);
        let max = counts.limits.max_per_user;

        if max > 0 {
            let current = counts.by_user.get(&key).copied().unwrap_or(0);

            if current >= max {
                return false;
            }
        }

        if let Some(previous) = self.user.take() {
            decrement_user(&mut counts.by_user, &previous);
        }

        *counts.by_user.entry(key.clone()).or_insert(0) += 1;
        self.user = Some(key);
        true
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let mut counts = lock_counts(&self.limiter.counts);

        counts.global = counts.global.saturating_sub(1);
        let remove_ip = if let Some(per_ip) = counts.by_ip.get_mut(&self.ip) {
            *per_ip = per_ip.saturating_sub(1);
            *per_ip == 0
        } else {
            false
        };

        if remove_ip {
            counts.by_ip.remove(&self.ip);
        }

        if let Some(user) = self.user.take() {
            decrement_user(&mut counts.by_user, &user);
        }
    }
}

fn decrement_user(by_user: &mut HashMap<Vec<u8>, usize>, user: &[u8]) {
    let remove = if let Some(count) = by_user.get_mut(user) {
        *count = count.saturating_sub(1);
        *count == 0
    } else {
        false
    };

    if remove {
        by_user.remove(user);
    }
}

fn lock_counts(counts: &Mutex<ConnectionCounts>) -> MutexGuard<'_, ConnectionCounts> {
    counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Per-connection token bucket. Refill and consume stay on the client task;
/// the shard command path does not take this state.
#[derive(Debug)]
pub(crate) struct CommandRateLimiter {
    per_second: u64,
    tokens: u64,
    last_refill: Instant,
}

impl CommandRateLimiter {
    pub(crate) fn new(per_second: u64) -> Self {
        Self {
            per_second,
            tokens: per_second,
            last_refill: Instant::now(),
        }
    }

    pub(crate) fn allow(&mut self) -> bool {
        if self.per_second == 0 {
            return true;
        }

        self.allow_at(Instant::now())
    }

    pub(crate) fn allow_at(&mut self, now: Instant) -> bool {
        if self.per_second == 0 {
            return true;
        }

        let elapsed = now.saturating_duration_since(self.last_refill);
        let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        let refill = elapsed_ms.saturating_mul(self.per_second) / 1000;

        if refill > 0 {
            self.tokens = self.tokens.saturating_add(refill).min(self.per_second);
            self.last_refill = now;
        }

        if self.tokens == 0 {
            RATE_LIMITED.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        self.tokens -= 1;
        true
    }
}

/// Lets one warning through per `interval` and counts the rest, so a lasting error (an accept
/// loop out of file descriptors retries every few milliseconds) does not flood the log.
#[derive(Debug)]
pub(crate) struct WarnLimiter {
    interval: Duration,
    next: Option<Instant>,
    suppressed: u64,
}

impl WarnLimiter {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: None,
            suppressed: 0,
        }
    }

    /// True when a warning may be logged at `now`; otherwise the warning is counted.
    pub(crate) fn allow(&mut self, now: Instant) -> bool {
        if self.next.is_some_and(|next| now < next) {
            self.suppressed += 1;

            return false;
        }

        self.next = Some(now + self.interval);
        true
    }

    /// Warnings dropped since the last one that was let through.
    pub(crate) fn take_suppressed(&mut self) -> u64 {
        std::mem::take(&mut self.suppressed)
    }
}

pub(crate) const RATE_LIMIT_ERROR: &[u8] = b"ERR max commands per second exceeded";
pub(crate) const USER_CONNECTION_LIMIT_ERROR: &[u8] =
    b"ERR max number of connections for this user reached";

#[cfg(test)]
mod tests {
    #[test]
    fn warn_limiter_lets_one_warning_through_per_interval_and_counts_the_rest() {
        let mut limiter = super::WarnLimiter::new(super::Duration::from_secs(1));
        let start = super::Instant::now();

        assert!(limiter.allow(start));

        for step in 1..100 {
            assert!(!limiter.allow(start + super::Duration::from_millis(step * 5)));
        }

        assert!(limiter.allow(start + super::Duration::from_secs(1)));
        assert_eq!(limiter.take_suppressed(), 99);
        assert_eq!(limiter.take_suppressed(), 0);
    }
    use super::*;
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    #[test]
    fn releases_global_and_per_ip_capacity_with_the_permit() {
        let limiter = ConnectionLimiter::new(ConnectionLimits {
            max_global: 2,
            max_per_ip: 1,
            ..ConnectionLimits::default()
        });

        let first_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let second_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
        let third_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3));

        let permit = limiter.try_acquire(first_ip).unwrap();

        assert!(limiter.try_acquire(first_ip).is_none());
        let other = limiter.try_acquire(second_ip).unwrap();

        assert!(limiter.try_acquire(second_ip).is_none());
        assert!(limiter.try_acquire(third_ip).is_none());

        drop(permit);
        assert!(limiter.try_acquire(first_ip).is_some());
        drop(other);
    }

    #[test]
    fn caps_authenticated_connections_per_user() {
        let limiter = ConnectionLimiter::new(ConnectionLimits {
            max_global: 4,
            max_per_ip: 4,
            max_per_user: 1,
            ..ConnectionLimits::default()
        });

        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mut first = limiter.try_acquire(ip).unwrap();
        let mut second = limiter.try_acquire(ip).unwrap();

        assert!(first.bind_user(b"app"));
        assert!(!second.bind_user(b"APP"));
        assert!(second.bind_user(b"other"));
        drop(first);
        assert!(second.bind_user(b"app"));
    }

    #[test]
    fn command_rate_limiter_bursts_one_second_then_refills() {
        let start = Instant::now();
        let mut limiter = CommandRateLimiter {
            per_second: 2,
            tokens: 2,
            last_refill: start,
        };

        assert!(limiter.allow_at(start));
        assert!(limiter.allow_at(start));
        assert!(!limiter.allow_at(start));
        assert!(limiter.allow_at(start + Duration::from_secs(1)));
        assert!(limiter.allow_at(start + Duration::from_secs(1)));
        assert!(!limiter.allow_at(start + Duration::from_secs(1)));
    }

    #[test]
    fn zero_command_rate_is_unlimited() {
        let mut limiter = CommandRateLimiter::new(0);

        for _ in 0..32 {
            assert!(limiter.allow());
        }
    }
}
