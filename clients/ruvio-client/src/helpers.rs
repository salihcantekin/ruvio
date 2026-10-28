//! Convenience helpers built on Ruvio's native commands: a distributed lock with an RAII
//! guard, fenced leases, semaphore permits, rate limiting, stampede-safe cache-aside,
//! run-once (idempotency), capped "recent items" lists and Bloom "seen before" checks.
//!
//! Every helper is a thin layer over the typed methods; nothing here keeps state on the
//! server that the plain commands would not.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::client::{Client, SetMode};
use crate::error::Error;
use crate::models::{FillOutcome, IdemState, RateLimit};

/// First and largest wait between polls of a busy lock or cache fill.
const POLL_FIRST: Duration = Duration::from_millis(2);
const POLL_MAX: Duration = Duration::from_millis(50);

/// Returns a random 128-bit token as 32 lowercase hex characters, for use as a lock owner,
/// an `IDEM` owner or any "who holds this" marker.
///
/// Every call returns a different value (seeded from the process's random hash keys, the
/// clock, the process id and a counter). It identifies a holder; it is not a secret and not
/// a cryptographic key.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// let owner = new_token();
/// assert_eq!(owner.len(), 32);
/// # Ok(())
/// # }
/// ```
pub fn new_token() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let mut halves = [0_u64; 2];

    for (salt, half) in halves.iter_mut().enumerate() {
        let mut hasher = RandomState::new().build_hasher();

        hasher.write_u64(count);
        hasher.write_u128(nanos);
        hasher.write_u32(std::process::id());
        hasher.write_usize(salt);
        *half = hasher.finish();
    }

    format!("{:016x}{:016x}", halves[0], halves[1])
}

/// A held distributed lock, returned by [`Client::try_lock`] and [`Client::lock`].
///
/// It derefs to the [`Client`], so you can keep running commands through the guard while the
/// lock is held. Dropping the guard releases the lock with `DELEX key IFEQ token`, which
/// never deletes a lock that expired and was taken by someone else. Call
/// [`LockGuard::release`] instead when you want to know whether the lock was still yours. A
/// drop cannot report errors; if the server is unreachable the TTL frees the lock anyway.
pub struct LockGuard<'a> {
    client: &'a mut Client,
    key: String,
    token: String,
    released: bool,
}

impl LockGuard<'_> {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This holder's token (the value stored in the lock key).
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Extends the lock's TTL to `ttl` from now, using `SET key token IFEQ token PX ttl`.
    ///
    /// Returns `true` when the lock was still yours and was extended, `false` when it had already
    /// expired or belongs to another holder (stop work: you no longer hold it). Call this
    /// periodically while a job runs longer than the original TTL.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(mut lock) = client.try_lock("lock:job", Duration::from_secs(10))? {
    ///     // ... long work ...
    ///     if !lock.renew(Duration::from_secs(10))? {
    ///         // lock lost
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn renew(&mut self, ttl: Duration) -> Result<bool, Error> {
        self.client
            .set_if_eq(&self.key, &self.token, &self.token, Some(ttl))
    }

    /// Releases the lock now (`DELEX key IFEQ token`).
    ///
    /// Returns `true` if it was released, `false` if it had already expired (or another holder
    /// took it). The guard is consumed.
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        self.client.delex(&self.key, &self.token)
    }
}

impl Deref for LockGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for LockGuard<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Best effort: the TTL frees the lock anyway if this cannot reach the server.
            let _ = self.client.delex(&self.key, &self.token);
        }
    }
}

/// A `LEASE` grant, returned by [`Client::lease_guard`]. Derefs to the [`Client`]; dropping
/// it sends `RELEASE key token`. Pass [`LeaseGuard::fence`] along with writes to the resource
/// you protect so it can refuse a stale holder that was paused past its TTL.
pub struct LeaseGuard<'a> {
    client: &'a mut Client,
    key: String,
    token: i64,
    released: bool,
}

impl LeaseGuard<'_> {
    /// The fencing token: strictly increasing per key across grants (it keeps counting after
    /// expiry and `RELEASE`). A resource that remembers the highest token it saw can reject
    /// writes that carry a lower one.
    pub fn fence(&self) -> i64 {
        self.token
    }

    /// Extends the lease to now + `ttl_ms` (`RENEW key token ttl_ms`). `true` if this token still
    /// held it; `false` if it had expired or moved to another holder. The fencing token stays the same.
    pub fn renew(&mut self, ttl_ms: u64) -> Result<bool, Error> {
        Ok(self.client.renew(&self.key, self.token as u64, ttl_ms)? > 0)
    }

    /// Releases the lease now (`RELEASE key token`). `true` if it was still held by this token;
    /// `false` if it had expired or moved to another holder.
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        Ok(self.client.release(&self.key, self.token as u64)? > 0)
    }
}

impl Deref for LeaseGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for LeaseGuard<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for LeaseGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.client.release(&self.key, self.token as u64);
        }
    }
}

/// One `SEMAPHORE` slot, returned by [`Client::semaphore_permit`]. Derefs to the [`Client`];
/// dropping it sends `RELEASE key token`. If the process dies, the slot frees itself when its
/// TTL passes.
pub struct SemaphorePermit<'a> {
    client: &'a mut Client,
    key: String,
    token: i64,
    released: bool,
}

impl SemaphorePermit<'_> {
    /// The holder token returned by the server for this slot.
    pub fn token(&self) -> i64 {
        self.token
    }

    /// Frees the slot now. `true` if the slot was still held; `false` if it had already expired.
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        Ok(self.client.release(&self.key, self.token as u64)? > 0)
    }
}

impl Deref for SemaphorePermit<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for SemaphorePermit<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.client.release(&self.key, self.token as u64);
        }
    }
}

/// Settings for [`Client::get_or_create_with`]. Start from [`CacheOptions::new`].
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// let mut options = CacheOptions::new(Duration::from_secs(60));
/// options.stale = Some(Duration::from_secs(30)); // serve old value while refreshing
/// let value = client.get_or_create_with("page:home", options, || {
///     Ok::<_, Error>("<html>...</html>".to_owned())
/// })?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheOptions {
    /// Fresh lifetime of the stored value.
    pub ttl: Duration,
    /// How long the "being filled" marker lives: the longest the factory may take before another
    /// caller takes over. Default from `new`: 10 seconds.
    pub fill_timeout: Duration,
    /// `Some(d)` keeps serving the old value for `d` past `ttl` while exactly one caller
    /// refreshes it (stale-while-revalidate, `FILLSET ... SWR`). `None` (default) disables it.
    pub stale: Option<Duration>,
    /// How long a caller that sees another caller filling keeps polling before it fails with a
    /// timeout [`Error::Io`]. Default from `new`: 10 seconds.
    pub max_wait: Duration,
}

impl CacheOptions {
    /// Options with the given fresh `ttl`, a 10 s fill timeout, no stale window and a 10 s wait.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            fill_timeout: Duration::from_secs(10),
            stale: None,
            max_wait: Duration::from_secs(10),
        }
    }
}

/// What [`Client::idempotent`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdemOutcome {
    /// This call ran the work and published the result (the bytes the work returned).
    Executed(Vec<u8>),
    /// The work had already finished earlier; these are the stored result bytes. The work was
    /// not run again.
    Replayed(Vec<u8>),
    /// Another owner is running the work right now. The work was not run; retry later.
    Pending,
    /// The key holds a different request fingerprint (the same idempotency key was reused for a
    /// different request). The work was not run.
    Conflict,
}

impl Client {
    // ---- distributed lock ------------------------------------------------------------------

    /// Takes a lock for a caller-chosen `token` with `SET key token NX PX ttl`.
    ///
    /// # Arguments
    /// * `key` - lock key. `token` - your unique holder id (see [`new_token`]). `ttl` - lock
    ///   lifetime; the lock frees itself after this.
    ///
    /// # Returns
    /// `true` if you now hold it, `false` if someone else does. Prefer [`Client::try_lock`],
    /// which also releases safely.
    pub fn acquire_lock(&mut self, key: &str, token: &str, ttl: Duration) -> Result<bool, Error> {
        self.set_with(key, token, Some(ttl), SetMode::IfNotExists)
    }

    /// Extends a lock you hold: `SET key token IFEQ token PX ttl`. `false` if `token` no longer
    /// holds it.
    pub fn renew_lock(&mut self, key: &str, token: &str, ttl: Duration) -> Result<bool, Error> {
        self.set_if_eq(key, token, token, Some(ttl))
    }

    /// Releases a lock only if `token` still holds it: `DELEX key IFEQ token`. `true` if released,
    /// `false` if it had expired or belongs to another holder. Never deletes someone else's lock.
    pub fn release_lock(&mut self, key: &str, token: &str) -> Result<bool, Error> {
        self.delex(key, token)
    }

    /// Tries once to take the lock `key` with a fresh random token.
    ///
    /// # Arguments
    /// * `key` - lock key, e.g. `"lock:report"`.
    /// * `ttl` - how long the lock lasts if never released; choose longer than the work, or call
    ///   [`LockGuard::renew`].
    ///
    /// # Returns
    /// `Some(guard)` when acquired, `None` when someone else holds it. The guard releases on drop.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(mut lock) = client.try_lock("lock:report", Duration::from_secs(30))? {
    ///     lock.set("report:status", "running")?; // the guard derefs to the client
    /// } // released here with DELEX IFEQ
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_lock(&mut self, key: &str, ttl: Duration) -> Result<Option<LockGuard<'_>>, Error> {
        let token = new_token();

        if !self.acquire_lock(key, &token, ttl)? {
            return Ok(None);
        }

        Ok(Some(LockGuard {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    /// Like [`Client::try_lock`], but keeps trying for up to `wait`, sleeping 2 ms and doubling to
    /// at most 50 ms between attempts. Returns `None` when the lock stayed held for the whole
    /// wait. This blocks the calling thread.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let guard = client.lock("lock:report", Duration::from_secs(30), Duration::from_secs(5))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn lock(
        &mut self,
        key: &str,
        ttl: Duration,
        wait: Duration,
    ) -> Result<Option<LockGuard<'_>>, Error> {
        let token = new_token();
        let deadline = Instant::now() + wait;
        let mut delay = POLL_FIRST;

        loop {
            if self.acquire_lock(key, &token, ttl)? {
                return Ok(Some(LockGuard {
                    client: self,
                    key: key.to_owned(),
                    token,
                    released: false,
                }));
            }

            let now = Instant::now();

            if now >= deadline {
                return Ok(None);
            }

            std::thread::sleep(delay.min(deadline - now));
            delay = (delay * 2).min(POLL_MAX);
        }
    }

    /// Runs `work` while holding the lock and releases it afterwards.
    ///
    /// # Returns
    /// `Ok(Some(result))` when the lock was taken and `work` succeeded; `Ok(None)` when the lock
    /// was held by someone else (`work` did not run). If `work` fails the lock is still released
    /// (on drop) and the error is returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let ran = client.with_lock("lock:nightly", Duration::from_secs(60), |db| {
    ///     db.incr("runs")?;
    ///     Ok::<_, Error>(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_lock<T, E>(
        &mut self,
        key: &str,
        ttl: Duration,
        work: impl FnOnce(&mut Client) -> Result<T, E>,
    ) -> Result<Option<T>, E>
    where
        E: From<Error>,
    {
        let Some(mut guard) = self.try_lock(key, ttl)? else {
            return Ok(None);
        };

        let result = work(&mut guard)?;

        guard.release()?;

        Ok(Some(result))
    }

    // ---- leases and semaphores -------------------------------------------------------------

    /// Takes `LEASE key ttl` and wraps it in a guard that releases on drop.
    ///
    /// `None` while another holder has the lease. Read the fencing token with
    /// [`LeaseGuard::fence`]. `ttl` is rounded up to whole milliseconds.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(lease) = client.lease_guard("leader", Duration::from_secs(15))? {
    ///     println!("fence token {}", lease.fence());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn lease_guard(
        &mut self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseGuard<'_>>, Error> {
        let Some(token) = self.lease(key, crate::client::duration_millis(ttl) as u64)? else {
            return Ok(None);
        };

        Ok(Some(LeaseGuard {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    /// Takes one of `max` slots of the semaphore `key` for up to `ttl`, released on drop.
    /// `None` when all slots are taken.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(permit) = client.semaphore_permit("pool:render", 4, Duration::from_secs(60))? {
    ///     // at most 4 holders run this at once
    ///     drop(permit);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn semaphore_permit(
        &mut self,
        key: &str,
        max: u64,
        ttl: Duration,
    ) -> Result<Option<SemaphorePermit<'_>>, Error> {
        let Some(token) = self.semaphore(key, max, crate::client::duration_millis(ttl) as u64)?
        else {
            return Ok(None);
        };

        Ok(Some(SemaphorePermit {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    // ---- rate limit ------------------------------------------------------------------------

    /// Fixed-window rate limit built on `LIMIT key max window`.
    ///
    /// The first call opens a window of length `window`; up to `max` calls are allowed in it. A
    /// refused call is not counted.
    ///
    /// # Returns
    /// A [`RateLimit`]: `allowed` and `remaining` (calls left after this one, `0` when refused).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let decision = client.rate_limit("rl:ip:1.2.3.4", 100, Duration::from_secs(60))?;
    /// if !decision.allowed {
    ///     // reject the request
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn rate_limit(
        &mut self,
        key: &str,
        max: u64,
        window: Duration,
    ) -> Result<RateLimit, Error> {
        Ok(
            match self.limit(key, max, crate::client::duration_millis(window) as u64)? {
                Some(remaining) => RateLimit {
                    allowed: true,
                    remaining,
                },
                None => RateLimit {
                    allowed: false,
                    remaining: 0,
                },
            },
        )
    }

    // ---- cache-aside -----------------------------------------------------------------------

    /// Cache-aside with stampede protection for string values.
    ///
    /// Returns the cached string, or lets exactly one caller run `factory` and store the result
    /// for `ttl` while the others wait for it. See [`Client::get_or_create_with`] for settings
    /// and failure behaviour.
    ///
    /// # Errors
    /// The factory's own error `E`, or [`Error`] converted into `E` (including an
    /// [`Error::Io`] timeout when waiting too long).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let page = client.get_or_create("page:home", Duration::from_secs(60), || {
    ///     Ok::<_, Error>("<html>...</html>".to_owned())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_or_create<F, E>(&mut self, key: &str, ttl: Duration, factory: F) -> Result<String, E>
    where
        F: FnOnce() -> Result<String, E>,
        E: From<Error>,
    {
        self.get_or_create_with(key, CacheOptions::new(ttl), factory)
    }

    /// [`Client::get_or_create`] with every setting ([`CacheOptions`]). Built on `FILLGET` /
    /// `FILLSET`:
    ///
    /// * hit: the stored value is returned;
    /// * fill (or stale with a token): `factory` runs and its value is stored with that token;
    /// * stale without a token: the old value is returned, someone else refreshes it;
    /// * busy: polls with a 2-50 ms backoff (the server-side `WAIT` would park the connection)
    ///   and fails with an [`std::io::ErrorKind::TimedOut`] error after `max_wait`.
    ///
    /// If `factory` fails nothing is stored and the marker lapses after `fill_timeout`, so the
    /// next caller retries.
    pub fn get_or_create_with<F, E>(
        &mut self,
        key: &str,
        options: CacheOptions,
        factory: F,
    ) -> Result<String, E>
    where
        F: FnOnce() -> Result<String, E>,
        E: From<Error>,
    {
        let deadline = Instant::now() + options.max_wait;
        let mut delay = POLL_FIRST;

        loop {
            match self.fill_get(key, options.fill_timeout)? {
                FillOutcome::Hit(value) | FillOutcome::Stale { value, token: None } => {
                    return Ok(value);
                }
                FillOutcome::Fill(token)
                | FillOutcome::Stale {
                    token: Some(token), ..
                } => {
                    let value = factory()?;

                    // False means the marker lapsed and another caller took over; this value is
                    // still correct for the caller.
                    self.fill_set(key, &value, options.ttl, &token, options.stale)?;

                    return Ok(value);
                }
                FillOutcome::Busy => {
                    let now = Instant::now();

                    if now >= deadline {
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "another caller is still filling the key",
                        ))
                        .into());
                    }

                    std::thread::sleep(delay.min(deadline - now));
                    delay = (delay * 2).min(POLL_MAX);
                }
            }
        }
    }

    // ---- idempotency -----------------------------------------------------------------------

    /// Runs `work` at most once per `key` and `fingerprint` (within `ttl`) using native `IDEM`.
    ///
    /// # Arguments
    /// * `key` - idempotency key, scoped to tenant and operation.
    /// * `fingerprint` - digest of the request content (1-128 bytes).
    /// * `owner` - unique token for this attempt, e.g. [`new_token`].
    /// * `ttl` - retention of the record, 1 ms to 7 days.
    /// * `work` - produces the result bytes.
    ///
    /// # Returns
    /// An [`IdemOutcome`]: `Executed` (you ran it), `Replayed` (stored result), `Pending`
    /// (another owner is running it), or `Conflict` (different fingerprint). If `work` fails the
    /// reservation stays pending until `ttl` passes. This prevents duplicate work; it is not
    /// exactly-once for external side effects.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let outcome = client.idempotent("order:42", "sha256-of-request", &new_token(), Duration::from_secs(300), || {
    ///     Ok::<_, Error>(b"charged".to_vec())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn idempotent<F, E>(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl: Duration,
        work: F,
    ) -> Result<IdemOutcome, E>
    where
        F: FnOnce() -> Result<Vec<u8>, E>,
        E: From<Error>,
    {
        self.idempotent_run(key, fingerprint, owner, ttl, false, work)
    }

    /// Like [`Client::idempotent`], but when `work` fails the reservation is released with
    /// `IDEM ... ABORT` and the original error is returned, so a retry runs the work again at
    /// once instead of getting `Pending` until `ttl` passes.
    ///
    /// Use it only when a failed `work` did not take effect or is safe to run again. A failed
    /// abort (for example Ruvio unreachable) is ignored: the record then stays pending until
    /// `ttl`, and the error of `work` is still the one returned.
    pub fn idempotent_abort_on_error<F, E>(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl: Duration,
        work: F,
    ) -> Result<IdemOutcome, E>
    where
        F: FnOnce() -> Result<Vec<u8>, E>,
        E: From<Error>,
    {
        self.idempotent_run(key, fingerprint, owner, ttl, true, work)
    }

    fn idempotent_run<F, E>(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl: Duration,
        abort_on_error: bool,
        work: F,
    ) -> Result<IdemOutcome, E>
    where
        F: FnOnce() -> Result<Vec<u8>, E>,
        E: From<Error>,
    {
        match self.idem_begin(key, fingerprint, owner, ttl)? {
            IdemState::Acquired => {
                let result = match work() {
                    Ok(result) => result,
                    Err(error) => {
                        if abort_on_error {
                            let _ = self.idem_abort(key, fingerprint, owner);
                        }

                        return Err(error);
                    }
                };

                self.idem_complete(key, fingerprint, owner, &result)?;

                Ok(IdemOutcome::Executed(result))
            }
            IdemState::Completed(result) => Ok(IdemOutcome::Replayed(result)),
            IdemState::Pending => Ok(IdemOutcome::Pending),
            IdemState::Conflict => Ok(IdemOutcome::Conflict),
        }
    }

    // ---- recent items and Bloom ------------------------------------------------------------

    /// Pushes `item` to the front of a list capped at `cap` items (`LPUSHCAP`) and returns the
    /// new length. Older items fall off the tail.
    pub fn push_recent(&mut self, key: &str, cap: u64, item: &str) -> Result<i64, Error> {
        self.lpushcap(key, cap, &[item])
    }

    /// Reads the newest `count` items pushed with [`Client::push_recent`], newest first.
    /// `count == 0` returns an empty vector without contacting the server.
    pub fn recent(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }

        self.lrange(key, 0, count as i64 - 1)
    }

    /// Records `items` in a Bloom filter (`BF.MADD`) and reports per item whether it was probably
    /// seen before (`true`) or certainly new (`false`). A missing key becomes a default scalable
    /// filter (error rate 0.01). False positives are possible; false negatives are not.
    pub fn seen_before(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        Ok(self
            .bf_madd(key, items)?
            .into_iter()
            .map(|added| !added)
            .collect())
    }

    /// Checks `items` without recording them (`BF.MEXISTS`): `true` means "maybe seen", `false`
    /// means "never recorded".
    pub fn maybe_seen(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        self.bf_mexists(key, items)
    }
}

#[cfg(test)]
mod tests {
    use super::new_token;

    #[test]
    fn tokens_are_32_hex_characters_and_unique() {
        let first = new_token();
        let second = new_token();

        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }
}
