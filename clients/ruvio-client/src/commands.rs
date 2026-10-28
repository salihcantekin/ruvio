// client_coverage: APPEND DEL EVAL EXEC EXPIRE GET HGET HGETALL HSET INCR
// LPUSH LRANGE MGET MSET MULTI PING SADD SET SMEMBERS STRLEN TTL ZADD ZRANGE

use std::collections::HashMap;

use std::time::{Duration, SystemTime};

use crate::client::{Client, duration_millis, duration_secs, join, unexpected, unix_millis};

use crate::error::Error;
use crate::keywords::{ClientType, InfoSection, StreamTrim};
use crate::models::{
    BitFieldOffset, BitFieldOp, BitFieldText, FieldExpireCondition, FillOutcome, GeoOrder, GeoUnit,
    HashScanPage, InsertPosition, KeyType, LimitOutcome, ListSide, PubSubMessage, ScanPage,
    SortedSetAddOptions, SortedSetEntry, StreamClaimOptions, StreamEntry, StreamPendingEntry,
    StreamPendingFilter, StreamReadOptions, StreamReadResult,
};

use crate::args::Num;
use crate::ttl::ttl_words;
use crate::value::RespValue;

macro_rules! args {
    ($($argument:expr),* $(,)?) => {
        vec![$($argument.to_string()),*]
    };
}

impl Client {
    /// Sends `MGET`: reads several string keys in one round trip.
    ///
    /// # Arguments
    /// * `keys` - keys to read. On a sharded server keys in different slots are fetched per
    ///   shard and merged back in the original order (not atomic).
    ///
    /// # Returns
    /// One entry per key, in order: `Some(text)` or `None` when the key is missing **or holds a
    /// non-string type** (no `WRONGTYPE` here).
    ///
    /// # Errors
    /// [`Error::Protocol`] if a value is not UTF-8; [`Error::Io`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let values = client.mget(&["a", "b"])?;
    /// assert_eq!(values.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn mget(&mut self, keys: &[&str]) -> Result<Vec<Option<String>>, Error> {
        self.optional_strings(join("MGET", keys))
    }

    /// Sends `LIMIT key max window_ms` (Ruvio): a fixed-window rate limiter held in one key.
    ///
    /// The first call on a missing key opens a window of `window_ms` and counts as call 1. Up to
    /// `max` calls are allowed until the window ends; after that every call is refused until the
    /// key expires and a new window starts. See [`Client::rate_limit`] for a friendlier wrapper.
    ///
    /// # Arguments
    /// * `key` - limiter key (for example `"rl:user:42"`).
    /// * `max` - calls allowed per window.
    /// * `window_ms` - window length in milliseconds.
    ///
    /// # Returns
    /// `Some(remaining)` - calls still available in this window after this one (`max - 1` on the
    /// first call, `0` on the last allowed one); `None` when this call did not fit.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` if the key holds another type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// match client.limit("rl:user:42", 100, 60_000)? {
    ///     Some(left) => println!("ok, {left} left"),
    ///     None => println!("slow down"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(&mut self, key: &str, max: u64, window_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(max).as_str(),
            Num::uint(window_ms).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("LIMIT", &value))
    }

    /// Sends `LIMIT key max window_ms SLIDING` (Ruvio): a sliding-window limit.
    ///
    /// The previous window counts for the part of it that still overlaps the last
    /// `window_ms`, so a burst at a window edge is not doubled. The key keeps two counters, not
    /// one entry per call. A key keeps the mode it was created with.
    ///
    /// # Arguments
    /// * `key` - limiter key (for example `"rl:user:42"`).
    /// * `max` - calls allowed per window, at most 2147483647.
    /// * `window_ms` - window length in milliseconds.
    ///
    /// # Returns
    /// `Some(remaining)` when this call fit; `None` when it did not (nothing was counted).
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` if the key holds another type, or an error if the key was
    /// created by another `LIMIT` mode.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if client.limit_sliding("rl:user:42", 100, 60_000)?.is_none() {
    ///     println!("slow down");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit_sliding(
        &mut self,
        key: &str,
        max: u64,
        window_ms: u64,
    ) -> Result<Option<i64>, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(max).as_str(),
            Num::uint(window_ms).as_str(),
            "SLIDING",
        ])?;

        limit_remaining(&value)
    }

    /// Sends `LIMIT key capacity refill_ms TOKENS [COST cost]` (Ruvio): a token bucket.
    ///
    /// The bucket holds `capacity` tokens and refills completely over `refill_ms`; a call takes
    /// `cost` tokens. The key keeps one timestamp and disappears when the bucket is full again.
    ///
    /// # Arguments
    /// * `key` - limiter key.
    /// * `capacity` - bucket size, 1 to 1000000.
    /// * `refill_ms` - time to refill an empty bucket, up to 4294967295 ms.
    /// * `cost` - tokens this call takes, 1 to `capacity`.
    ///
    /// # Returns
    /// `Some(remaining)` tokens left when the call fit; `None` when the bucket held too few
    /// (nothing was taken).
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` if the key holds another type, or an error if the key was
    /// created by another `LIMIT` mode.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // 10 tokens per second, this call takes 2.
    /// let left = client.limit_tokens("rl:api:7", 10, 1_000, 2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit_tokens(
        &mut self,
        key: &str,
        capacity: u64,
        refill_ms: u64,
        cost: u64,
    ) -> Result<Option<i64>, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(capacity).as_str(),
            Num::uint(refill_ms).as_str(),
            "TOKENS",
            "COST",
            Num::uint(cost).as_str(),
        ])?;

        limit_remaining(&value)
    }

    /// [`Client::limit_sliding`] with `WITHRETRY`: the answer also says how long to wait.
    ///
    /// # Returns
    /// A [`LimitOutcome`]; `retry_after_ms` is the wait until the same call would pass.
    ///
    /// # Errors
    /// As [`Client::limit_sliding`].
    pub fn limit_sliding_with_retry(
        &mut self,
        key: &str,
        max: u64,
        window_ms: u64,
    ) -> Result<LimitOutcome, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(max).as_str(),
            Num::uint(window_ms).as_str(),
            "SLIDING",
            "WITHRETRY",
        ])?;

        limit_outcome(&value)
    }

    /// [`Client::limit_tokens`] with `WITHRETRY`: the answer also says how long to wait.
    ///
    /// # Returns
    /// A [`LimitOutcome`]; `retry_after_ms` is the wait until the same call would pass.
    ///
    /// # Errors
    /// As [`Client::limit_tokens`].
    pub fn limit_tokens_with_retry(
        &mut self,
        key: &str,
        capacity: u64,
        refill_ms: u64,
        cost: u64,
    ) -> Result<LimitOutcome, Error> {
        let value = self.run([
            "LIMIT",
            key,
            Num::uint(capacity).as_str(),
            Num::uint(refill_ms).as_str(),
            "TOKENS",
            "COST",
            Num::uint(cost).as_str(),
            "WITHRETRY",
        ])?;

        limit_outcome(&value)
    }

    /// Sends `ONCE key ttl_ms` (Ruvio): a "do this only once until the deadline" marker.
    ///
    /// # Arguments
    /// * `key` - marker key.
    /// * `ttl_ms` - how long the marker stays, in milliseconds.
    ///
    /// # Returns
    /// `1` for the first caller (the marker was created); `0` for every later caller until
    /// `ttl_ms` has passed. For request de-duplication with a result, see [`Client::idem_begin`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if client.once("welcome-email:42", 86_400_000)? == 1 {
    ///     // send the email
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn once(&mut self, key: &str, ttl_ms: u64) -> Result<i64, Error> {
        self.integer(["ONCE", key, Num::uint(ttl_ms).as_str()])
    }

    /// Sends `IDEM key BEGIN fingerprint owner ttl_ms`: reserves an operation for de-duplication
    /// and returns the raw reply. [`Client::idem_begin`] decodes it into an
    /// [`IdemState`](crate::IdemState).
    ///
    /// # Arguments
    /// * `key` - operation key, scoped to the tenant and operation.
    /// * `fingerprint` - 1-128 bytes identifying the request content (a digest of its inputs).
    /// * `owner` - 1-128 bytes unique to this attempt (see [`new_token`](crate::new_token)).
    /// * `ttl_ms` - retention, 1 to 604,800,000 ms (7 days).
    ///
    /// # Returns
    /// An array: `["acquired"]` (you own it, run the work), `["pending"]` (same fingerprint, not
    /// completed; do not run again), `["conflict"]` (live record has another fingerprint) or
    /// `["completed", result]` (replay the saved result).
    ///
    /// # Errors
    /// [`Error::Server`] for out-of-range arguments or `WRONGTYPE`.
    pub fn idempotency_begin(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<RespValue, Error> {
        self.run([
            "IDEM",
            key,
            "BEGIN",
            fingerprint,
            owner,
            Num::uint(ttl_ms).as_str(),
        ])
    }

    /// Sends `IDEM key COMPLETE fingerprint owner result`: stores the result of a reserved
    /// operation. See [`Client::idem_complete`] for the typed wrapper.
    ///
    /// # Arguments
    /// * `key`, `fingerprint`, `owner` - as passed to [`Client::idempotency_begin`].
    /// * `result` - text to replay later, at most 4 MiB.
    ///
    /// # Returns
    /// `1` when published (or an identical repeat); `0` when the record is missing/expired, the
    /// owner or fingerprint differs, or a different result is already stored. Completion never
    /// extends retention.
    pub fn idempotency_complete(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        result: &str,
    ) -> Result<i64, Error> {
        self.integer(["IDEM", key, "COMPLETE", fingerprint, owner, result])
    }

    /// Sends `IDEM key ABORT fingerprint owner`: removes a pending record. See
    /// [`Client::idem_abort`] for the typed wrapper.
    ///
    /// # Arguments
    /// * `key`, `fingerprint`, `owner` - as passed to [`Client::idempotency_begin`].
    ///
    /// # Returns
    /// `1` when the pending record of this owner and fingerprint was removed; `0` when the key is
    /// missing, already completed (its result is never removed), or held by another owner or
    /// fingerprint.
    pub fn idempotency_abort(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
    ) -> Result<i64, Error> {
        self.integer(["IDEM", key, "ABORT", fingerprint, owner])
    }

    /// Sends `TAKE key count` (Ruvio): subtracts from an integer only if the key holds enough.
    ///
    /// # Arguments
    /// * `key` - integer counter (for example stock).
    /// * `count` - amount to subtract.
    ///
    /// # Returns
    /// `Some(new_value)`; `None` when the key is missing or holds less than `count` (nothing is
    /// changed).
    ///
    /// # Errors
    /// [`Error::Server`] if the value is not an integer or the key has another type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// match client.take("stock:sku1", 2)? {
    ///     Some(left) => println!("reserved, {left} left"),
    ///     None => println!("not enough stock"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn take(&mut self, key: &str, count: i64) -> Result<Option<i64>, Error> {
        let value = self.run(["TAKE", key, Num::int(count).as_str()])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("TAKE", &value))
    }

    /// Sends `TRANSFER source destination amount [MAX cap]`: moves `amount` from the integer at
    /// `source` to the integer at `destination` in one step, only when `source` holds at least
    /// that much. A missing `destination` starts at 0. Both keys are written as one WAL record.
    ///
    /// # Arguments
    /// * `amount` - a positive integer to move.
    /// * `max` - optional ceiling for `destination` after the move.
    ///
    /// # Returns
    /// `Some((source_after, destination_after))`, or `None` when `source` is missing or short,
    /// or `destination` would overflow or pass `max`; nothing is written then.
    ///
    /// # Errors
    /// [`Error::Server`] if a value is not an integer, a key has another type, `amount` is not
    /// positive, or (on a sharded server) the two keys are in different slots (`CROSSSLOT`);
    /// hash tags such as `{user:42}` put them in one slot.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// match client.transfer("{u:42}:wallet", "{u:42}:escrow", 30, None)? {
    ///     Some((wallet, escrow)) => println!("moved: {wallet} left, {escrow} held"),
    ///     None => println!("not enough balance"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn transfer(
        &mut self,
        source: &str,
        destination: &str,
        amount: i64,
        max: Option<i64>,
    ) -> Result<Option<(i64, i64)>, Error> {
        let mut command = args!["TRANSFER", source, destination, amount];

        if let Some(max) = max {
            command.extend(args!["MAX", max]);
        }

        let value = self.run(command)?;

        balance_pair("TRANSFER", value)
    }

    /// Sends `HTRANSFER key from to amount [MAX cap]`: [`Client::transfer`] between two integer
    /// fields of one hash. A missing `to` field starts at 0 and field TTLs stay.
    ///
    /// # Returns
    /// `Some((from_after, to_after))`, or `None` when the key or the `from` field is missing or
    /// short, or `to` would overflow or pass `max`; nothing is written then.
    ///
    /// # Errors
    /// [`Error::Server`] if a field is not an integer, the key is not a hash, or `amount` is
    /// not positive.
    pub fn htransfer(
        &mut self,
        key: &str,
        from: &str,
        to: &str,
        amount: i64,
        max: Option<i64>,
    ) -> Result<Option<(i64, i64)>, Error> {
        let mut command = args!["HTRANSFER", key, from, to, amount];

        if let Some(max) = max {
            command.extend(args!["MAX", max]);
        }

        let value = self.run(command)?;

        balance_pair("HTRANSFER", value)
    }

    /// Sends `GETEX key PX ttl_ms`: returns the string and sets a new TTL in one step (sliding
    /// expiry). Plain `GET` does not move the deadline.
    ///
    /// # Arguments
    /// * `ttl_ms` - new time to live in milliseconds.
    ///
    /// # Returns
    /// The value, or `None` when the key is missing (nothing is created).
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let session = client.getex("session:abc", 1_800_000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn getex(&mut self, key: &str, ttl_ms: u64) -> Result<Option<String>, Error> {
        self.bulk_or_null(["GETEX", key, Num::uint(ttl_ms).as_str()])
    }

    /// Sends `LEASE key ttl_ms` (Ruvio): takes an exclusive lease with a fencing token.
    ///
    /// # Arguments
    /// * `ttl_ms` - how long the lease lasts, in milliseconds.
    ///
    /// # Returns
    /// `Some(token)`: you hold the lease; the token grows with every grant (also after expiry or
    /// `RELEASE`) so a stale holder can be fenced off. `None` while another holder still has it.
    /// Free it with [`Client::release`]. See [`Client::lease_guard`] for an automatic release.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` if the key holds another type (`GET` of a lease is
    /// `WRONGTYPE` too).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(token) = client.lease("job:nightly", 30_000)? {
    ///     // do the work, passing `token` to downstream writes
    ///     client.release("job:nightly", token as u64)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn lease(&mut self, key: &str, ttl_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run(["LEASE", key, Num::uint(ttl_ms).as_str()])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("LEASE", &value))
    }

    /// Sends `SEMAPHORE key max ttl_ms` (Ruvio): takes one of `max` slots.
    ///
    /// # Arguments
    /// * `max` - most holders allowed at once.
    /// * `ttl_ms` - this holder's lifetime in milliseconds; each holder expires on its own, so a
    ///   crashed holder frees its slot.
    ///
    /// # Returns
    /// `Some(token)` while fewer than `max` holders are live; `None` when full. Pass the token to
    /// [`Client::release`]. The key disappears when the last holder is gone. See
    /// [`Client::semaphore_permit`] for an automatic release.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some(token) = client.semaphore("pool:db", 10, 5_000)? {
    ///     // use the resource
    ///     client.release("pool:db", token as u64)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn semaphore(&mut self, key: &str, max: u64, ttl_ms: u64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "SEMAPHORE",
            key,
            Num::uint(max).as_str(),
            Num::uint(ttl_ms).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("SEMAPHORE", &value))
    }

    /// Sends `INCRBY key delta MAX limit` (Ruvio): a capped counter.
    ///
    /// # Arguments
    /// * `delta` - amount to add (may be negative).
    /// * `limit` - the result may not go above this.
    ///
    /// # Returns
    /// `Some(new_value)`; `None` when the result would pass `limit`, in which case nothing is
    /// written.
    ///
    /// # Errors
    /// [`Error::Server`] if the value is not an integer or `WRONGTYPE`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let seats = client.incr_by_max("seats:taken", 1, 100)?; // None once 100 are taken
    /// # Ok(())
    /// # }
    /// ```
    pub fn incr_by_max(&mut self, key: &str, delta: i64, limit: i64) -> Result<Option<i64>, Error> {
        let value = self.run([
            "INCRBY",
            key,
            Num::int(delta).as_str(),
            "MAX",
            Num::int(limit).as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("INCRBY", &value))
    }

    /// Sends `RELEASE key token`: frees a lease or a semaphore slot.
    ///
    /// # Arguments
    /// * `token` - the value returned by [`Client::lease`] or [`Client::semaphore`].
    ///
    /// # Returns
    /// `1` when the token matched a current holder and was released; `0` for a stale or wrong
    /// token (nothing changes).
    pub fn release(&mut self, key: &str, token: u64) -> Result<i64, Error> {
        self.integer(["RELEASE", key, Num::uint(token).as_str()])
    }

    /// Sends `RENEW key token ttl_ms`: moves the deadline of a lease or semaphore slot you hold
    /// to now + `ttl_ms`, without releasing it.
    ///
    /// # Arguments
    /// * `token` - the value returned by [`Client::lease`] or [`Client::semaphore`].
    /// * `ttl_ms` - the new lifetime in milliseconds (positive); it replaces the remaining time,
    ///   so it can also be shorter.
    ///
    /// # Returns
    /// `1` when the token is a live holder and its deadline moved; `0` when it expired, was
    /// released or belongs to someone else (nothing is created or changed).
    pub fn renew(&mut self, key: &str, token: u64, ttl_ms: u64) -> Result<i64, Error> {
        self.integer([
            "RENEW",
            key,
            Num::uint(token).as_str(),
            Num::uint(ttl_ms).as_str(),
        ])
    }

    /// Sends `SETV key value [version]` (Ruvio): optimistic-locking write of a versioned string.
    ///
    /// # Arguments
    /// * `value` - new value.
    /// * `version` - `None` creates the key (or replaces a plain string) as version 1. `Some(v)`
    ///   writes only if the current version is exactly `v`.
    ///
    /// # Returns
    /// The new version number (current + 1).
    ///
    /// # Errors
    /// [`Error::Server`] `ERR version mismatch` when `version` is not current or the key is
    /// missing; `WRONGTYPE` for a non-string. A plain `SET` replaces a versioned string with a
    /// plain one, and `INCR` refuses a versioned string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let v1 = client.setv("doc", "draft", None)?;
    /// let v2 = client.setv("doc", "final", Some(v1 as u64))?;
    /// assert_eq!(v2, v1 + 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn setv(&mut self, key: &str, value: &str, version: Option<u64>) -> Result<i64, Error> {
        let mut arguments = vec!["SETV".to_owned(), key.to_owned(), value.to_owned()];

        if let Some(version) = version {
            arguments.push(version.to_string());
        }

        self.integer(arguments)
    }

    /// Sends `CHANGES START` (Ruvio): turns on this shard's in-memory change feed (later `SET`,
    /// `SETV`, `LEASE`, `RELEASE` notes).
    ///
    /// # Returns
    /// The current sequence number; pass it to [`Client::changes`] to read what happens next.
    /// Nothing is recorded before this call, and the feed is empty after a server restart. Only
    /// the last 1,024 notes are kept.
    pub fn changes_start(&mut self) -> Result<i64, Error> {
        self.integer(["CHANGES", "START"])
    }

    /// Sends `CHANGES cursor`: reads change notes recorded after `cursor`.
    ///
    /// # Arguments
    /// * `cursor` - the sequence number returned by [`Client::changes_start`] or by the previous
    ///   call (the highest sequence seen).
    ///
    /// # Returns
    /// `(sequence, text)` pairs in order; empty when nothing is new or the feed was never
    /// started. On a sharded server the feed is per shard, so use a client connected to the shard
    /// you armed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut cursor = client.changes_start()? as u64;
    /// for (sequence, note) in client.changes(cursor)? {
    ///     cursor = sequence as u64;
    ///     println!("{note}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn changes(&mut self, cursor: u64) -> Result<Vec<(i64, String)>, Error> {
        let value = self.run(["CHANGES", Num::uint(cursor).as_str()])?;
        let Some(rows) = value.as_array() else {
            return Err(unexpected("CHANGES", &value));
        };
        let mut notes = Vec::new();

        for row in rows {
            let Some(parts) = row.as_array() else {
                return Err(unexpected("CHANGES", row));
            };
            let sequence = parts
                .first()
                .and_then(|part| part.as_integer())
                .ok_or_else(|| unexpected("CHANGES", row))?;
            let text = parts
                .get(1)
                .and_then(|part| part.as_string().ok().flatten())
                .unwrap_or_default();

            notes.push((sequence, text));
        }

        Ok(notes)
    }

    /// Sends `MSET`: sets several string keys, replacing existing values of any type.
    ///
    /// On a sharded server pairs are split per slot and sent separately; this is **not atomic**
    /// across slots.
    ///
    /// # Arguments
    /// * `pairs` - `(key, value)` pairs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.mset(&[("a", "1"), ("b", "2")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn mset(&mut self, pairs: &[(&str, &str)]) -> Result<(), Error> {
        self.ok(with_pairs(args!["MSET"], pairs))
    }

    /// Sends `GETSET`: stores `value` and returns the old string (`None` if there was none).
    /// Clears any TTL. Redis deprecates it in favour of `SET ... GET`; see
    /// [`Client::set_and_get`].
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string.
    pub fn getset(&mut self, key: &str, value: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["GETSET", key, value])
    }

    /// Sends `APPEND`: adds `value` to the end of the string (creating it when missing) and
    /// returns the new length in bytes. Keeps an existing TTL.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.append("log", "line 1\n")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn append(&mut self, key: &str, value: &str) -> Result<i64, Error> {
        self.integer(["APPEND", key, value])
    }

    /// Sends `STRLEN`: length of the string in bytes; `0` when the key is missing.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string.
    pub fn strlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["STRLEN", key])
    }

    /// Sends `DECR`: subtracts 1 from the integer at `key` (missing counts as 0) and returns the
    /// new value. Errors as [`Client::incr`].
    pub fn decr(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["DECR", key])
    }

    /// Sends `INCRBY`: adds `delta` (may be negative) to the integer at `key` (missing counts as
    /// 0) and returns the new value. Keeps an existing TTL.
    ///
    /// # Errors
    /// [`Error::Server`] if the stored value is not a 64-bit integer, the result overflows, or
    /// `WRONGTYPE`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// assert_eq!(client.incr_by("score", 10)?, 10);
    /// # Ok(())
    /// # }
    /// ```
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<i64, Error> {
        self.integer(["INCRBY", key, Num::int(delta).as_str()])
    }

    /// Sends `DECRBY`: subtracts `delta` from the integer at `key` and returns the new value.
    /// Errors as [`Client::incr_by`].
    pub fn decr_by(&mut self, key: &str, delta: i64) -> Result<i64, Error> {
        self.integer(["DECRBY", key, Num::int(delta).as_str()])
    }

    /// Sends `UNLINK` for one key. Returns 1 if removed, 0 if missing. In Ruvio `UNLINK` is
    /// synchronous, the same as `DEL`.
    pub fn unlink_key(&mut self, key: &str) -> Result<i64, Error> {
        self.unlink(&[key])
    }

    /// Sends `UNLINK` for several keys and returns how many existed. Same as [`Client::del`]
    /// (Ruvio frees the memory synchronously). Split per slot on a sharded server, not atomic.
    pub fn unlink(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("UNLINK", keys))
    }

    /// Sends `EXISTS` for one key: `1` if it exists, `0` if not (any type).
    pub fn exists_key(&mut self, key: &str) -> Result<i64, Error> {
        self.exists(&[key])
    }

    /// Sends `EXISTS` and returns how many of the keys exist; a key listed twice counts twice.
    /// Split per slot on a sharded server (counts are summed).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let present = client.exists(&["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn exists(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("EXISTS", keys))
    }

    /// Sends `TYPE`.
    ///
    /// # Returns
    /// `"string"`, `"list"`, `"set"`, `"hash"`, `"zset"` or `"stream"`, or `"none"` for a
    /// missing key. Ruvio also reports `"json"`, `"bloomfilter"`, `"lease"`, `"limit"`,
    /// `"semaphore"` and `"once"` for its own types; geo indexes are `"zset"`.
    pub fn key_type(&mut self, key: &str) -> Result<String, Error> {
        self.text(["TYPE", key])
    }

    /// Sends `RENAME`: moves `key` to `new_key`, overwriting it and keeping the TTL.
    ///
    /// # Errors
    /// [`Error::Server`] `ERR no such key` when `key` is missing. On a sharded server both keys
    /// must share a slot (use a hash tag), otherwise `CROSSSLOT`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.rename("draft:1", "final:1")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rename(&mut self, key: &str, new_key: &str) -> Result<(), Error> {
        self.ok(["RENAME", key, new_key])
    }

    /// Sends `SCAN`: one page of a key walk.
    ///
    /// # Arguments
    /// * `cursor` - `0` to start, then the cursor of the previous page. Cursors are opaque; on a
    ///   sharded server the client folds the shard number into them, so keep using the returned
    ///   value.
    /// * `pattern` - optional glob (`MATCH`) applied to key names after the page is read.
    /// * `count` - page-size hint (`COUNT`, default 10); it is the number of index slots to
    ///   examine, not a result limit, so a page may be empty or larger.
    /// * `key_type` - optional [`KeyType`] filter (`TYPE`): only keys holding that type.
    ///
    /// # Returns
    /// A [`ScanPage`]. The scan is complete when `cursor` is `0`. A key present for the whole
    /// walk is returned at least once; a key may repeat if the table was rebuilt in between.
    /// Prefer [`Client::scan_iter`], which loops for you.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut cursor = 0;
    /// loop {
    ///     let page = client.scan(cursor, Some("user:*"), Some(100), Some(KeyType::Hash))?;
    ///     for key in &page.keys {
    ///         println!("{key}");
    ///     }
    ///     cursor = page.cursor;
    ///     if cursor == 0 {
    ///         break;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan(
        &mut self,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
        key_type: Option<KeyType>,
    ) -> Result<ScanPage, Error> {
        let mut arguments = args!["SCAN", cursor];

        push_scan_options(&mut arguments, pattern, count);

        if let Some(key_type) = key_type {
            arguments.extend(args!["TYPE", key_type.as_str()]);
        }

        let (cursor, keys) = parse_scan("SCAN", self.run(arguments)?)?;

        Ok(ScanPage { cursor, keys })
    }

    /// Sends `DBSIZE`: number of keys in the selected database.
    ///
    /// A routing client asks every shard and sums the answers. A client opened with
    /// `discover_cluster: false` on a `port_mode = "per-shard"` server counts that shard only.
    pub fn dbsize(&mut self) -> Result<i64, Error> {
        self.integer(["DBSIZE"])
    }

    /// Sets an absolute expiry. Sends `EXPIREAT`, or `PEXPIREAT` when `when` is not a whole second.
    ///
    /// # Arguments
    /// * `when` - wall-clock deadline; a time in the past deletes the key.
    ///
    /// # Returns
    /// `true` when set; `false` if the key does not exist.
    ///
    /// # Errors
    /// [`Error::Protocol`] when `when` is before the Unix epoch.
    pub fn expire_at(&mut self, key: &str, when: SystemTime) -> Result<bool, Error> {
        let millis = unix_millis(when)?;

        if millis % 1000 == 0 {
            return self.flag(args!["EXPIREAT", key, millis / 1000]);
        }

        self.flag(args!["PEXPIREAT", key, millis])
    }

    /// Sends `PEXPIRE`: TTL in milliseconds (rounded up from `ttl`). Returns `false` when the key
    /// does not exist.
    pub fn pexpire(&mut self, key: &str, ttl: Duration) -> Result<bool, Error> {
        self.flag(args!["PEXPIRE", key, duration_millis(ttl)])
    }

    /// Sends `TTL`: remaining life in seconds. `-1` if the key has no expiry, `-2` if it does
    /// not exist.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let left = client.ttl("session:abc")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn ttl(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["TTL", key])
    }

    /// Sends `PTTL`: remaining life in milliseconds, `-1` without an expiry, `-2` when missing.
    pub fn pttl(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["PTTL", key])
    }

    /// Sends `PERSIST`: removes the expiry. `true` if a TTL was removed; `false` when the key is
    /// missing or had none.
    pub fn persist(&mut self, key: &str) -> Result<bool, Error> {
        self.flag(["PERSIST", key])
    }

    /// Sends `LPUSH`: pushes values to the head, one after the other (so the last argument ends
    /// up first). Creates the list. Returns the new length.
    ///
    /// # Arguments
    /// * `values` - at least one element.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-list.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.lpush("queue", &["a", "b"])?; // list is now b, a
    /// # Ok(())
    /// # }
    /// ```
    pub fn lpush(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["LPUSH", key], values))
    }

    /// Sends `RPUSH`: appends values to the tail in order. Creates the list. Returns the new
    /// length. Errors as [`Client::lpush`].
    pub fn rpush(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["RPUSH", key], values))
    }

    /// Sends `LPOP`: removes and returns the head element, `None` when the list is missing or
    /// empty. `WRONGTYPE` for a non-list.
    pub fn lpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["LPOP", key])
    }

    /// Sends `LPOP key count`: removes up to `count` elements from the head. An empty vector for
    /// a missing list. `count` must be positive.
    pub fn lpop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["LPOP", key, count])
    }

    /// Sends `RPOP`: removes and returns the tail element, `None` for a missing or empty list.
    pub fn rpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RPOP", key])
    }

    /// Sends `RPOP key count`: removes up to `count` elements from the tail, last element first.
    /// Empty vector for a missing list.
    pub fn rpop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["RPOP", key, count])
    }

    /// Sends `LLEN`: list length, `0` for a missing key. `WRONGTYPE` for a non-list.
    pub fn llen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["LLEN", key])
    }

    /// Sends `LINDEX`: element at `index` (0 is the head, `-1` the tail). `None` when the index is
    /// out of range or the key is missing.
    pub fn lindex(&mut self, key: &str, index: i64) -> Result<Option<String>, Error> {
        self.bulk_or_null(args!["LINDEX", key, index])
    }

    /// Sends `LRANGE`: elements from `start` to `stop`, both **inclusive**; negative indexes
    /// count from the tail (`-1` is the last). Out-of-range bounds are clamped; a missing list
    /// gives an empty vector.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let all = client.lrange("queue", 0, -1)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["LRANGE", key, start, stop])
    }

    /// Sends `LTRIM`: keeps only elements `start..=stop` (negative counts from the tail) and
    /// removes the rest. An empty result deletes the key. Handy for capped logs.
    pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        self.ok(args!["LTRIM", key, start, stop])
    }

    /// Sends `BLPOP` for one key. See [`Client::blpop`].
    pub fn blpop_key(
        &mut self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blpop(&[key], timeout)
    }

    /// Sends `BLPOP`: pops from the head of the first non-empty list, waiting if all are empty.
    ///
    /// # Arguments
    /// * `keys` - lists checked in order (one slot on a sharded server).
    /// * `timeout` - how long to wait, rounded up to whole seconds. `Duration::ZERO` waits
    ///   forever.
    ///
    /// # Returns
    /// `Some((key, value))`, or `None` when the timeout passes.
    ///
    /// The connection is busy while it waits. The command timeout is extended by `timeout` for
    /// this call, and `Duration::ZERO` has no deadline at all.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// if let Some((queue, job)) = client.blpop(&["jobs"], Duration::from_secs(5))? {
    ///     println!("{queue}: {job}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn blpop(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blocking_pop("BLPOP", keys, timeout)
    }

    /// Sends `BRPOP` for one key. See [`Client::brpop`].
    pub fn brpop_key(
        &mut self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.brpop(&[key], timeout)
    }

    /// Sends `BRPOP`: like [`Client::blpop`] but pops from the tail.
    pub fn brpop(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        self.blocking_pop("BRPOP", keys, timeout)
    }

    /// Sends `SADD`: adds members, creating the set. Returns how many were new.
    /// `WRONGTYPE` for a non-set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.sadd("tags", &["rust", "db"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sadd(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SADD", key], members))
    }

    /// Sends `SREM`: removes members; returns how many were present. A set emptied this way is
    /// deleted.
    pub fn srem(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SREM", key], members))
    }

    /// Sends `SISMEMBER`: `true` if `member` is in the set (`false` also for a missing key).
    pub fn sismember(&mut self, key: &str, member: &str) -> Result<bool, Error> {
        self.flag(["SISMEMBER", key, member])
    }

    /// Sends `SCARD`: number of members, `0` for a missing key.
    pub fn scard(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["SCARD", key])
    }

    /// Sends `SMEMBERS`: all members (a missing key gives an empty vector). Ruvio returns them
    /// sorted. Use [`Client::sscan_iter`] for big sets.
    pub fn smembers(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["SMEMBERS", key])
    }

    /// Sends `SINTER`: members present in every set. A missing key counts as an empty set. On a
    /// sharded server all keys must share a slot (`CROSSSLOT` otherwise).
    pub fn sinter(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SINTER", keys))
    }

    /// Sends `SUNION`: members present in any of the sets. Same-slot rule as [`Client::sinter`].
    pub fn sunion(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SUNION", keys))
    }

    /// Sends `SDIFF`: members of the first set that are in none of the others. Same-slot rule as
    /// [`Client::sinter`].
    pub fn sdiff(&mut self, keys: &[&str]) -> Result<Vec<String>, Error> {
        self.strings(join("SDIFF", keys))
    }

    /// Sends `SINTERSTORE`: stores the intersection in `destination` (overwriting it) and
    /// returns its size. An empty result deletes `destination`. All keys in one slot.
    pub fn sinterstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SINTERSTORE", destination], keys))
    }

    /// Sends `SUNIONSTORE`: stores the union in `destination` and returns its size. All keys in
    /// one slot.
    pub fn sunionstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SUNIONSTORE", destination], keys))
    }

    /// Sends `SDIFFSTORE`: stores the difference in `destination` and returns its size. All keys
    /// in one slot.
    pub fn sdiffstore(&mut self, destination: &str, keys: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["SDIFFSTORE", destination], keys))
    }

    /// Sends `SMOVE`: moves `member` from `source` to `destination`. `false` when it was not in
    /// `source` (nothing happens). Both keys in one slot.
    pub fn smove(&mut self, source: &str, destination: &str, member: &str) -> Result<bool, Error> {
        self.flag(["SMOVE", source, destination, member])
    }

    /// Sends `SPOP`: removes and returns one random member; `None` for a missing or empty set.
    pub fn spop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["SPOP", key])
    }

    /// Sends `SPOP key count`: removes and returns up to `count` random members (empty for a
    /// missing set).
    pub fn spop_count(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        self.strings(args!["SPOP", key, count])
    }

    /// Sends `SRANDMEMBER`: one random member without removing it; `None` for a missing set.
    pub fn srandmember(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["SRANDMEMBER", key])
    }

    /// Sends `SRANDMEMBER key count`. A positive `count` returns up to `count` distinct members;
    /// a negative count returns exactly `|count|` members and may repeat them (limited to
    /// 1,048,576).
    pub fn srandmember_count(&mut self, key: &str, count: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["SRANDMEMBER", key, count])
    }

    /// Sends `JSON.GET`: reads part of a JSON document.
    ///
    /// # Arguments
    /// * `key` - JSON key.
    /// * `path` - `"$"` for the whole document, or a simple path such as `$.user.name` or
    ///   `$.items[0]`. Ruvio supports plain `.field` and `[N]` steps only (no filters or
    ///   wildcards).
    ///
    /// # Returns
    /// The value as JSON text, or `None` when the key or path is absent.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.json_set("user:1", "$", r#"{"name":"Ada","age":36}"#)?;
    /// let name = client.json_get("user:1", "$.name")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_get(&mut self, key: &str, path: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.GET", key, path])
    }

    /// Sends `JSON.MGET`: reads one path from several keys.
    ///
    /// # Arguments
    /// * `keys` - JSON keys, all in one slot on a sharded server.
    /// * `path` - path read from each.
    ///
    /// # Returns
    /// One entry per key; `None` for a missing key or a key that is not JSON.
    pub fn json_mget(&mut self, keys: &[&str], path: &str) -> Result<Vec<Option<String>>, Error> {
        let mut arguments = join("JSON.MGET", keys);

        arguments.push(path.to_owned());
        self.optional_strings(arguments)
    }

    /// Sends `JSON.SET`: writes JSON text at a path.
    ///
    /// # Arguments
    /// * `key` - JSON key.
    /// * `path` - `"$"` for the whole document (creates the key); or a path inside an existing
    ///   document. An array path whose index equals the array length appends.
    /// * `json` - valid JSON text for the new value.
    ///
    /// # Returns
    /// `true` when written; `false` when the path cannot be reached.
    ///
    /// # Errors
    /// [`Error::Server`] if `json` is not valid JSON or the key holds another type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.json_set("cfg", "$", r#"{"debug":false}"#)?;
    /// client.json_set("cfg", "$.debug", "true")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_set(&mut self, key: &str, path: &str, json: &str) -> Result<bool, Error> {
        Ok(!matches!(
            self.run(["JSON.SET", key, path, json])?,
            RespValue::Null
        ))
    }

    /// Sends `JSON.MERGE`: merges a patch into the value at `path` following RFC 7396 (objects
    /// merge recursively, `null` deletes a member, other values replace).
    ///
    /// `true` when applied, `false` when the path cannot be reached. `patch` must be valid JSON.
    pub fn json_merge(&mut self, key: &str, path: &str, patch: &str) -> Result<bool, Error> {
        Ok(!matches!(
            self.run(["JSON.MERGE", key, path, patch])?,
            RespValue::Null
        ))
    }

    /// Sends `JSON.DEL`: deletes a member, an array element, or the whole key (path `"$"`).
    /// Returns `1` if something was removed, `0` otherwise.
    pub fn json_del(&mut self, key: &str, path: &str) -> Result<i64, Error> {
        self.integer(["JSON.DEL", key, path])
    }

    /// Sends `JSON.TYPE`: the JSON type at `path` (`object`, `array`, `string`, `integer`,
    /// `number`, `boolean`, `null`), or `None` when the key or path is absent.
    pub fn json_type(&mut self, key: &str, path: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.TYPE", key, path])
    }

    /// Sends `JSON.STRLEN`: length of the JSON string at `path`; `None` when absent.
    pub fn json_strlen(&mut self, key: &str, path: &str) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.STRLEN", key, path])
    }

    /// Sends `JSON.ARRLEN`: number of elements of the array at `path`; `None` when absent.
    pub fn json_arrlen(&mut self, key: &str, path: &str) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.ARRLEN", key, path])
    }

    /// Sends `JSON.OBJKEYS`: member names of the object at `path`, in stored order. Empty when
    /// absent.
    pub fn json_objkeys(&mut self, key: &str, path: &str) -> Result<Vec<String>, Error> {
        self.strings(["JSON.OBJKEYS", key, path])
    }

    /// Sends `JSON.NUMINCRBY`: adds `by` to the number at `path` and returns the new number as
    /// JSON text; `None` when absent.
    pub fn json_numincrby(
        &mut self,
        key: &str,
        path: &str,
        by: f64,
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(["JSON.NUMINCRBY", key, path, Num::json_number(by).as_str()])
    }

    /// Sends `JSON.TOGGLE`: flips the boolean at `path` and returns the new value; `None` when
    /// absent.
    pub fn json_toggle(&mut self, key: &str, path: &str) -> Result<Option<bool>, Error> {
        Ok(self
            .json_integer(["JSON.TOGGLE", key, path])?
            .map(|value| value != 0))
    }

    /// Sends `JSON.ARRAPPEND` with one JSON value and returns the array's new length; `None`
    /// when the key or path is absent. See [`Client::json_arrappend_many`] for several values.
    pub fn json_arrappend(
        &mut self,
        key: &str,
        path: &str,
        json: &str,
    ) -> Result<Option<i64>, Error> {
        self.json_integer(["JSON.ARRAPPEND", key, path, json])
    }

    /// Sends `JSON.CLEAR`: empties arrays and objects and zeroes numbers at `path`. Returns how
    /// many values changed (0 when absent).
    pub fn json_clear(&mut self, key: &str, path: &str) -> Result<i64, Error> {
        Ok(self.json_integer(["JSON.CLEAR", key, path])?.unwrap_or(0))
    }

    fn json_integer<const N: usize>(&mut self, arguments: [&str; N]) -> Result<Option<i64>, Error> {
        let value = self.run(arguments)?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("JSON", &value))
    }

    /// Sends `HSET`: sets fields, creating the hash. Returns how many fields were **new**
    /// (overwritten fields are not counted). Drops a field's TTL.
    ///
    /// # Arguments
    /// * `fields` - `(field, value)` pairs, at least one.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-hash.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.hset("user:1", &[("name", "Ada"), ("city", "London")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn hset(&mut self, key: &str, fields: &[(&str, &str)]) -> Result<i64, Error> {
        self.integer(with_pairs(args!["HSET", key], fields))
    }

    /// Sends `HGET`: the field's value, or `None` when the field or key is missing.
    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["HGET", key, field])
    }

    /// Sends `HDEL`: removes fields and returns how many existed. An emptied hash is deleted.
    pub fn hdel(&mut self, key: &str, fields: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["HDEL", key], fields))
    }

    /// Sends `HLEN`: number of fields, `0` for a missing key.
    pub fn hlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["HLEN", key])
    }

    /// Sends `HGETALL`: every field and value as a map (empty for a missing key). Field order is
    /// not defined. Use [`Client::hscan_iter`] for very large hashes.
    pub fn hgetall(&mut self, key: &str) -> Result<HashMap<String, String>, Error> {
        let pairs = pairs(&self.run(["HGETALL", key])?)?;

        Ok(pairs.into_iter().collect())
    }

    /// Sends `HMGET`: values of several fields, in order; `None` for a missing field (or key).
    pub fn hmget(&mut self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>, Error> {
        self.optional_strings(with(args!["HMGET", key], fields))
    }

    /// Sends `HEXISTS`: `true` if the field exists.
    pub fn hexists(&mut self, key: &str, field: &str) -> Result<bool, Error> {
        self.flag(["HEXISTS", key, field])
    }

    /// Sends `HKEYS`: field names (order not defined).
    pub fn hkeys(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["HKEYS", key])
    }

    /// Sends `HVALS`: values (order not defined).
    pub fn hvals(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.strings(["HVALS", key])
    }

    /// Sends `HINCRBY`: adds `delta` to the integer in `field` (missing counts as 0) and returns
    /// the new value. Keeps the field's TTL. See [`Client::hincr_by_float`] for decimals.
    ///
    /// # Errors
    /// [`Error::Server`] if the field is not an integer or overflows.
    pub fn hincr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<i64, Error> {
        self.integer(args!["HINCRBY", key, field, delta])
    }

    /// Sends `HMSET`: sets fields like [`Client::hset`] but replies `OK` instead of the
    /// count of new fields. Redis marks it deprecated; it is here for code that still uses it.
    ///
    /// # Arguments
    /// * `fields` - `(field, value)` pairs, at least one.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-hash.
    pub fn hmset(&mut self, key: &str, fields: &[(&str, &str)]) -> Result<(), Error> {
        self.ok(with_pairs(args!["HMSET", key], fields))
    }

    /// Sends `HSETNX`: sets `field` only if it does not exist. `false` when it already existed.
    pub fn hsetnx(&mut self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        self.flag(["HSETNX", key, field, value])
    }

    /// Sends `HSTRLEN`: length in bytes of the field's value; `0` if missing.
    pub fn hstrlen(&mut self, key: &str, field: &str) -> Result<i64, Error> {
        self.integer(["HSTRLEN", key, field])
    }

    /// Sets a TTL on hash fields. Sends `HEXPIRE` for whole seconds, otherwise `HPEXPIRE`.
    ///
    /// # Arguments
    /// * `ttl` - time to live from now. Zero deletes the fields at once.
    /// * `fields` - fields to expire (at least one).
    ///
    /// # Returns
    /// One integer per field, in order: `1` TTL set, `0` condition not met, `2` the field was
    /// deleted (zero TTL), `-2` no such field or key.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-hash.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.hset("session:42", &[("token", "abc"), ("user", "ada")])?;
    /// client.hexpire("session:42", Duration::from_secs(900), &["token"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn hexpire(
        &mut self,
        key: &str,
        ttl: Duration,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        self.hexpire_with(key, ttl, FieldExpireCondition::Always, fields)
    }

    /// Like [`Client::hexpire`] with a [`FieldExpireCondition`] (`NX`, `XX`, `GT` or `LT`).
    /// A field without a TTL counts as an infinite TTL for `GT` and `LT`.
    pub fn hexpire_with(
        &mut self,
        key: &str,
        ttl: Duration,
        condition: FieldExpireCondition,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        let arguments = if ttl.subsec_nanos() == 0 {
            args!["HEXPIRE", key, duration_secs(ttl)]
        } else {
            args!["HPEXPIRE", key, duration_millis(ttl)]
        };

        self.integers(field_block(arguments, condition, fields))
    }

    /// Sends `HPEXPIRE`: TTL in milliseconds on hash fields. Returns as [`Client::hexpire`].
    pub fn hpexpire(
        &mut self,
        key: &str,
        ttl: Duration,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPEXPIRE", key, duration_millis(ttl)],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sets an absolute deadline on hash fields. Sends `HEXPIREAT` for a whole second, otherwise
    /// `HPEXPIREAT`. A time in the past deletes the fields (reply `2`). Replies per field as
    /// [`Client::hexpire`].
    ///
    /// # Errors
    /// [`Error::Protocol`] if `when` is before the Unix epoch.
    pub fn hexpire_at(
        &mut self,
        key: &str,
        when: SystemTime,
        fields: &[&str],
    ) -> Result<Vec<i64>, Error> {
        let millis = unix_millis(when)?;
        let arguments = if millis % 1000 == 0 {
            args!["HEXPIREAT", key, millis / 1000]
        } else {
            args!["HPEXPIREAT", key, millis]
        };

        self.integers(field_block(arguments, FieldExpireCondition::Always, fields))
    }

    /// Sends `HTTL`: seconds left per field. `-1` the field has no TTL, `-2` field or key missing.
    pub fn httl(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HTTL", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPTTL`: milliseconds left per field, `-1` without a TTL, `-2` when missing.
    pub fn hpttl(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPTTL", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HEXPIRETIME`: the Unix second each field expires at; `-1`/`-2` as [`Client::httl`].
    pub fn hexpiretime(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HEXPIRETIME", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPEXPIRETIME`: the Unix millisecond each field expires at; `-1`/`-2` as
    /// [`Client::httl`].
    pub fn hpexpiretime(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPEXPIRETIME", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HPERSIST`: removes field TTLs. Per field: `1` TTL removed, `-1` the field had none,
    /// `-2` field or key missing.
    pub fn hpersist(&mut self, key: &str, fields: &[&str]) -> Result<Vec<i64>, Error> {
        self.integers(field_block(
            args!["HPERSIST", key],
            FieldExpireCondition::Always,
            fields,
        ))
    }

    /// Sends `HSCAN`: one page of a hash walk.
    ///
    /// # Arguments
    /// * `cursor` - `0` to start, then the cursor of the previous page.
    /// * `pattern` - optional glob on field names. `count` - page-size hint (default 10).
    ///
    /// # Returns
    /// A [`HashScanPage`]; the scan is finished when `cursor` is `0`. Prefer
    /// [`Client::hscan_iter`], which loops for you.
    pub fn hscan(
        &mut self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> Result<HashScanPage, Error> {
        let mut arguments = args!["HSCAN", key, cursor];

        push_scan_options(&mut arguments, pattern, count);

        let (cursor, flat) = parse_scan("HSCAN", self.run(arguments)?)?;
        let mut fields = Vec::with_capacity(flat.len() / 2);
        let mut items = flat.into_iter();

        while let (Some(field), Some(value)) = (items.next(), items.next()) {
            fields.push((field, value));
        }

        Ok(HashScanPage { cursor, fields })
    }

    /// Sends `ZADD` with `(member, score)` pairs and returns how many members were **new**
    /// (updated scores are not counted). Creates the set.
    ///
    /// # Arguments
    /// * `entries` - `(member, score)` pairs; scores are finite or infinite `f64` (not NaN).
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-zset; a NaN score is rejected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.zadd("board", &[("ada", 100.0), ("bob", 50.0)])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn zadd(&mut self, key: &str, entries: &[(&str, f64)]) -> Result<i64, Error> {
        self.zadd_with(key, SortedSetAddOptions::default(), entries)
    }

    /// Sends `ZADD` with [`SortedSetAddOptions`] (`EX`/`PX` via `ttl`, `NX`, `XX`, `GT`, `LT`, `CH`).
    ///
    /// With `ttl` set the key's deadline is set only when this call creates the key.
    ///
    /// Returns the number of new members, or with `changed` set the number of new **and**
    /// updated members. The server rejects `NX` with `XX`/`GT`/`LT`, and `GT` with `LT`
    /// ([`Error::Server`]).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let options = SortedSetAddOptions { greater_than: true, changed: true, ..Default::default() };
    /// let changed = client.zadd_with("board", options, &[("ada", 120.0)])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn zadd_with(
        &mut self,
        key: &str,
        options: SortedSetAddOptions,
        entries: &[(&str, f64)],
    ) -> Result<i64, Error> {
        let scores: Vec<Num> = entries
            .iter()
            .map(|(_, score)| Num::float(*score))
            .collect();
        let ttl = options.ttl.map(ttl_words).transpose()?;
        let mut arguments: Vec<&str> = Vec::with_capacity(9 + entries.len() * 2);

        arguments.extend(["ZADD", key]);

        if let Some((word, count)) = &ttl {
            arguments.extend([*word, count.as_str()]);
        }

        for (enabled, flag) in [
            (options.if_not_exists, "NX"),
            (options.if_exists, "XX"),
            (options.greater_than, "GT"),
            (options.less_than, "LT"),
            (options.changed, "CH"),
        ] {
            if enabled {
                arguments.push(flag);
            }
        }

        for (score, (member, _)) in scores.iter().zip(entries) {
            arguments.push(score.as_str());
            arguments.push(member);
        }

        self.integer(arguments)
    }

    /// Sends `ZADD ... INCR`: adds `delta` to one member's score (creating it).
    ///
    /// Returns the new score, or `None` when `options` (`NX`/`XX`/`GT`/`LT`) skipped the update.
    pub fn zadd_incr(
        &mut self,
        key: &str,
        member: &str,
        delta: f64,
        options: SortedSetAddOptions,
    ) -> Result<Option<f64>, Error> {
        let mut arguments = zadd_arguments(key, options)?;

        arguments.extend(args!["INCR", delta, member]);
        self.bulk_or_null(arguments)?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `ZINCRBY`: adds `delta` to `member`'s score (creating the member with `delta`) and
    /// returns the new score.
    pub fn zincr_by(&mut self, key: &str, member: &str, delta: f64) -> Result<f64, Error> {
        parse_score(&self.text(args!["ZINCRBY", key, delta, member])?)
    }

    /// Sends `GEOADD`: adds places to a geo index (stored as a sorted set).
    ///
    /// # Arguments
    /// * `key` - geo index.
    /// * `places` - `(longitude, latitude, member)` tuples. **Longitude comes first.**
    ///   Longitude must be within -180 to 180 and latitude within -85.05112878 to 85.05112878
    ///   degrees (WGS84, the Web Mercator limit); anything else is refused.
    ///   An existing member is moved to the new position.
    ///
    /// # Returns
    /// How many members were newly added (moved members are not counted). Use
    /// [`Client::geo_add_with`] for `NX`/`XX`/`CH`.
    ///
    /// # Errors
    /// [`Error::Server`] `ERR invalid longitude,latitude pair ...` for out-of-range coordinates;
    /// `WRONGTYPE` for a non-zset.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let added = client.geo_add("shops", &[(13.361389, 38.115556, "palermo"), (15.087269, 37.502669, "catania")])?;
    /// assert_eq!(added, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn geo_add(&mut self, key: &str, places: &[(f64, f64, &str)]) -> Result<i64, Error> {
        let mut arguments = args!["GEOADD", key];

        for (longitude, latitude, member) in places {
            arguments.push(longitude.to_string());
            arguments.push(latitude.to_string());
            arguments.push((*member).to_owned());
        }

        self.integer(arguments)
    }

    /// Sends `GEODIST`: distance between two members of a geo index.
    ///
    /// # Arguments
    /// * `key` - geo index. `from`, `to` - member names.
    /// * `unit` - a [`GeoUnit`] (meters, kilometers, feet, miles); `None` means meters.
    ///
    /// # Returns
    /// The distance in `unit` (great-circle, sphere radius 6,372,797.56 m); `None` when the key
    /// or either member is missing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let km = client.geo_dist("shops", "palermo", "catania", Some(GeoUnit::Kilometers))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn geo_dist(
        &mut self,
        key: &str,
        from: &str,
        to: &str,
        unit: Option<GeoUnit>,
    ) -> Result<Option<f64>, Error> {
        let mut arguments = ["GEODIST", key, from, to, ""];
        let used = match unit {
            Some(unit) => {
                arguments[4] = unit.as_str();
                5
            }
            None => 4,
        };

        self.bulk_or_null(&arguments[..used])?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `GEOHASH`: the 11-character Geohash string of each member; `None` for a missing
    /// member.
    pub fn geo_hash(&mut self, key: &str, members: &[&str]) -> Result<Vec<Option<String>>, Error> {
        let mut arguments = args!["GEOHASH", key];

        arguments.extend(members.iter().map(|member| (*member).to_owned()));
        self.optional_strings(arguments)
    }

    /// Sends `GEOPOS`: `(longitude, latitude)` per member, `None` for a missing member. The
    /// position is the centre of the stored 52-bit cell, so it can differ from the input by a
    /// fraction of a metre.
    pub fn geo_pos(
        &mut self,
        key: &str,
        members: &[&str],
    ) -> Result<Vec<Option<(f64, f64)>>, Error> {
        let mut arguments = args!["GEOPOS", key];

        arguments.extend(members.iter().map(|member| (*member).to_owned()));

        let reply = self.run(arguments)?;
        let Some(items) = reply.as_array() else {
            return Err(unexpected("GEOPOS", &reply));
        };

        let mut points = Vec::with_capacity(items.len());

        for item in items {
            if item.is_null() {
                points.push(None);

                continue;
            }

            let Some(pair) = item.as_array() else {
                return Err(unexpected("GEOPOS", item));
            };

            if pair.len() != 2 {
                return Err(unexpected("GEOPOS", item));
            }

            let longitude = parse_score(&pair[0].as_string()?.unwrap_or_default())?;
            let latitude = parse_score(&pair[1].as_string()?.unwrap_or_default())?;

            points.push(Some((longitude, latitude)));
        }

        Ok(points)
    }

    /// Sends `GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit ASC`: members within a circle,
    /// nearest first.
    ///
    /// # Arguments
    /// * `longitude`, `latitude` - centre in degrees (longitude -180..180 first, latitude
    ///   -85.05112878..85.05112878).
    /// * `radius` - positive radius in `unit`.
    /// * `unit` - a [`GeoUnit`].
    ///
    /// # Returns
    /// Member names, nearest first; empty when none or the key is missing. For distances,
    /// coordinates, boxes, descending order or a count limit use [`Client::geo_search_with`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let near = client.geo_search("shops", 15.0, 37.0, 200.0, GeoUnit::Kilometers)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn geo_search(
        &mut self,
        key: &str,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Result<Vec<String>, Error> {
        let longitude = Num::float(longitude);
        let latitude = Num::float(latitude);
        let radius = Num::float(radius);

        self.strings([
            "GEOSEARCH",
            key,
            "FROMLONLAT",
            longitude.as_str(),
            latitude.as_str(),
            "BYRADIUS",
            radius.as_str(),
            unit.as_str(),
            GeoOrder::Asc.as_str(),
        ])
    }

    /// Sends `GEOSEARCHSTORE destination source FROMMEMBER member BYRADIUS radius unit`: stores
    /// the members within a circle around another member into a new geo index.
    ///
    /// # Arguments
    /// * `destination` - key to write (replaced); `source` - geo index to search. Same slot on a
    ///   sharded server.
    /// * `member` - centre member in `source`. `radius` - positive radius in `unit`
    ///   ([`GeoUnit`]).
    ///
    /// # Returns
    /// How many members were stored (an empty result deletes `destination`).
    pub fn geo_search_store(
        &mut self,
        destination: &str,
        source: &str,
        member: &str,
        radius: f64,
        unit: GeoUnit,
    ) -> Result<i64, Error> {
        let radius = Num::float(radius);

        self.integer([
            "GEOSEARCHSTORE",
            destination,
            source,
            "FROMMEMBER",
            member,
            "BYRADIUS",
            radius.as_str(),
            unit.as_str(),
        ])
    }

    /// Sends `GEORADIUSBYMEMBER key member radius unit ASC`: members within `radius` (in
    /// [`GeoUnit`]) of an existing member, nearest first. The member itself is included. Error if
    /// `member` is missing.
    pub fn geo_radius_by_member(
        &mut self,
        key: &str,
        member: &str,
        radius: f64,
        unit: GeoUnit,
    ) -> Result<Vec<String>, Error> {
        let radius = Num::float(radius);

        self.strings([
            "GEORADIUSBYMEMBER",
            key,
            member,
            radius.as_str(),
            unit.as_str(),
            GeoOrder::Asc.as_str(),
        ])
    }

    /// Sends `GEORADIUS key lon lat radius unit ASC`: members within a circle around a point,
    /// nearest first. Arguments as [`Client::geo_search`]. Redis deprecates `GEORADIUS` in favour
    /// of `GEOSEARCH`.
    pub fn geo_radius(
        &mut self,
        key: &str,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Result<Vec<String>, Error> {
        let longitude = Num::float(longitude);
        let latitude = Num::float(latitude);
        let radius = Num::float(radius);

        self.strings([
            "GEORADIUS",
            key,
            longitude.as_str(),
            latitude.as_str(),
            radius.as_str(),
            unit.as_str(),
            GeoOrder::Asc.as_str(),
        ])
    }

    /// Sends `ZRANGE` by rank (0 is the lowest score). `stop` is **inclusive**; negative ranks
    /// count from the end (`-1` is the highest). Members are returned without scores; see
    /// [`Client::zrange_with_scores`]. Missing key gives an empty vector.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let top_three = client.zrevrange("board", 0, 2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["ZRANGE", key, start, stop])
    }

    /// Sends `ZREVRANGE` by rank, highest score first. `stop` is inclusive; negative ranks count
    /// from the end.
    pub fn zrevrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        self.strings(args!["ZREVRANGE", key, start, stop])
    }

    /// Sends `ZRANGE ... WITHSCORES`: members in rank order with their scores.
    pub fn zrange_with_scores(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZRANGE", key, start, stop, "WITHSCORES"])?)
    }

    /// Sends `ZREVRANGE ... WITHSCORES`: highest first, with scores.
    pub fn zrevrange_with_scores(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZREVRANGE", key, start, stop, "WITHSCORES"])?)
    }

    /// Sends `ZRANGE min max BYLEX`: members in a lexicographic range. Meaningful when all
    /// members share a score.
    ///
    /// # Arguments
    /// * `min`, `max` - `-` (lowest), `+` (highest), `[member` (inclusive), `(member`
    ///   (exclusive).
    pub fn zrange_by_lex(&mut self, key: &str, min: &str, max: &str) -> Result<Vec<String>, Error> {
        self.strings(["ZRANGE", key, min, max, "BYLEX"])
    }

    /// Sends `ZRANGEBYSCORE ... WITHSCORES`.
    ///
    /// # Arguments
    /// * `min`, `max` - scores as text: `"10"`, `"-inf"`, `"+inf"`; prefix `(` for an exclusive
    ///   bound, e.g. `"(10"`.
    pub fn zrange_by_score_with_scores(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(["ZRANGEBYSCORE", key, min, max, "WITHSCORES"])?)
    }

    /// Sends `ZREM`: removes members; returns how many existed. An emptied set is deleted.
    pub fn zrem(&mut self, key: &str, members: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["ZREM", key], members))
    }

    /// Sends `ZPOPMIN key count`: removes and returns up to `count` lowest-scored members.
    pub fn zpopmin(&mut self, key: &str, count: u64) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZPOPMIN", key, count])?)
    }

    /// Sends `ZPOPMAX key count`: removes and returns up to `count` highest-scored members.
    pub fn zpopmax(&mut self, key: &str, count: u64) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args!["ZPOPMAX", key, count])?)
    }

    /// Sends `ZPOPBYSCORE key max MIN min COUNT count WITHSCORES`: removes and returns up to
    /// `count` members (1 to 10000) whose score lies between `min` and `max`, lowest score
    /// first. With scores set to due times, this takes the jobs that are ready.
    ///
    /// `min`/`max` are score text: `-inf`, `+inf`, `(` prefix for exclusive. Returns an empty
    /// list when nothing is due.
    pub fn zpopbyscore(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        count: u64,
    ) -> Result<Vec<SortedSetEntry>, Error> {
        sorted_set(&self.run(args![
            "ZPOPBYSCORE",
            key,
            max,
            "MIN",
            min,
            "COUNT",
            count,
            "WITHSCORES"
        ])?)
    }

    /// Sends `ZMOVEDUE source destination max COUNT count`: moves up to `count` members
    /// (1 to 10000) whose score is at most `max` from the sorted set `source` to the tail of
    /// the list `destination`, lowest score first, and returns how many moved. Both keys must
    /// hash to the same slot on a sharded server; clients blocked on `destination` are served
    /// first.
    pub fn zmovedue(
        &mut self,
        source: &str,
        destination: &str,
        max: &str,
        count: u64,
    ) -> Result<i64, Error> {
        self.integer(args!["ZMOVEDUE", source, destination, max, "COUNT", count])
    }

    /// Sends `BZPOPMIN`: waits for the lowest-scored member of the first non-empty set.
    ///
    /// # Arguments
    /// * `keys` - sorted sets checked in order (one slot).
    /// * `timeout` - wait time rounded up to whole seconds; `Duration::ZERO` waits forever.
    ///
    /// # Returns
    /// `Some((key, entry))`, or `None` on timeout. The command timeout is extended by
    /// `timeout` for this call.
    pub fn bzpopmin(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        self.blocking_sorted_pop("BZPOPMIN", keys, timeout)
    }

    /// Sends `BZPOPMAX`. Like [`Client::bzpopmin`], highest score first.
    pub fn bzpopmax(
        &mut self,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        self.blocking_sorted_pop("BZPOPMAX", keys, timeout)
    }

    /// Sends `ZREMRANGEBYRANK`: removes members with ranks `start..=stop` (negative counts from
    /// the end). Returns how many were removed.
    pub fn zremrangebyrank(&mut self, key: &str, start: i64, stop: i64) -> Result<i64, Error> {
        self.integer(args!["ZREMRANGEBYRANK", key, start, stop])
    }

    /// Sends `ZREMRANGEBYSCORE`: removes members by score. `min`/`max` are score text: `-inf`,
    /// `+inf`, `(` prefix for exclusive. Returns how many were removed.
    pub fn zremrangebyscore(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZREMRANGEBYSCORE", key, min, max])
    }

    /// Sends `ZREMRANGEBYLEX`: removes members in a lex range (`-`, `+`, `[member`, `(member`).
    /// Returns how many were removed.
    pub fn zremrangebylex(&mut self, key: &str, min: &str, max: &str) -> Result<i64, Error> {
        self.integer(["ZREMRANGEBYLEX", key, min, max])
    }

    /// Sends `ZCARD`: number of members, `0` for a missing key.
    pub fn zcard(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["ZCARD", key])
    }

    /// Sends `ZSCORE`: the member's score, or `None` for a missing member or key.
    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, Error> {
        self.bulk_or_null(["ZSCORE", key, member])?
            .map(|text| parse_score(&text))
            .transpose()
    }

    /// Sends `ZRANK`: 0-based rank from the lowest score; `None` if the member or key is missing.
    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["ZRANK", key, member])
    }

    /// Sends `ZREVRANK`: 0-based rank from the highest score; `None` if missing.
    pub fn zrevrank(&mut self, key: &str, member: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["ZREVRANK", key, member])
    }

    /// Sends `BF.RESERVE`: creates a Bloom filter.
    ///
    /// # Arguments
    /// * `error_rate` - wanted false-positive rate between 0 and 1 (for example `0.01`).
    /// * `capacity` - items the first layer is sized for. By default the filter scales: when full
    ///   it adds a layer twice as large with half the error rate. See
    ///   [`Client::bf_reserve_with`] for `EXPANSION` / `NONSCALING`.
    ///
    /// # Errors
    /// [`Error::Server`] if the key already exists or the values are out of range.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.bf_reserve("seen:emails", 0.01, 100_000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bf_reserve(&mut self, key: &str, error_rate: f64, capacity: u64) -> Result<(), Error> {
        self.ok(args!["BF.RESERVE", key, error_rate, capacity])
    }

    /// Sends `BF.ADD`: adds an item (creating a filter with error 0.01 and capacity 100 if the
    /// key is missing). `true` when the item was new; `false` when it may already have been
    /// added.
    pub fn bf_add(&mut self, key: &str, item: &str) -> Result<bool, Error> {
        self.flag(["BF.ADD", key, item])
    }

    /// Sends `BF.EXISTS`: `true` means "probably added" (false positives possible), `false`
    /// means "definitely never added".
    pub fn bf_exists(&mut self, key: &str, item: &str) -> Result<bool, Error> {
        self.flag(["BF.EXISTS", key, item])
    }

    /// Sends `XADD ... NOMKSTREAM`: appends only to an existing stream.
    ///
    /// # Arguments
    /// * `id` - `*` to auto-generate, `ms-*` to fix the millisecond, or a full `ms-seq`.
    /// * `fields` - field/value pairs (at least one).
    ///
    /// # Returns
    /// The new entry id, or `None` when the stream does not exist (it is not created).
    pub fn xadd_nomkstream(
        &mut self,
        key: &str,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(with_pairs(args!["XADD", key, "NOMKSTREAM", id], fields))
    }

    /// Sends `XADD key DELAY ms ...` (Ruvio): the entry is added but hidden from `XREADGROUP`
    /// until `delay_ms` milliseconds have passed (a scheduled job). Returns the entry id.
    pub fn xadd_delay(
        &mut self,
        key: &str,
        delay_ms: u64,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        self.text(with_pairs(
            args!["XADD", key, "DELAY", delay_ms, id],
            fields,
        ))
    }

    /// Sends `XADD`: appends an entry to a stream (creating it) and returns its id.
    ///
    /// # Arguments
    /// * `id` - `*` lets the server pick `<ms>-<seq>`; `ms-*` fixes the millisecond part; a full
    ///   `ms-seq` must be greater than the last id.
    /// * `fields` - field/value pairs (at least one).
    ///
    /// # Errors
    /// [`Error::Server`] if the id is not greater than the stream's last id; `WRONGTYPE`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let id = client.xadd("events", "*", &[("type", "login"), ("user", "ada")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xadd(&mut self, key: &str, id: &str, fields: &[(&str, &str)]) -> Result<String, Error> {
        self.text(with_pairs(args!["XADD", key, id], fields))
    }

    /// Sends `XADD key MAXLEN n`: appends and then trims so the stream keeps exactly the newest
    /// `max_len` entries (Ruvio trims exactly, also for `~`). Returns the new id.
    pub fn xadd_maxlen(
        &mut self,
        key: &str,
        max_len: u64,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        self.text(with_pairs(
            args!["XADD", key, "MAXLEN", max_len, id],
            fields,
        ))
    }

    /// Sends `XADD key MAXLEN|MINID [=|~] threshold id ...`: appends an entry and trims the stream
    /// with a typed [`StreamTrim`]. Returns the new id.
    ///
    /// # Arguments
    /// * `trim` - strategy, mode and threshold; the server does not accept `LIMIT`.
    /// * `id` - as for [`Client::xadd`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let trim = StreamTrim::max_len(1000).mode(TrimMode::Approximate);
    /// let id = client.xadd_trim("events", &trim, stream_id::NEW, &[("type", "login")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xadd_trim(
        &mut self,
        key: &str,
        trim: &StreamTrim<'_>,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        let arguments = trim.with_words(|strategy, mode, threshold| {
            args!["XADD", key, strategy, mode, threshold, id]
        });

        self.text(with_pairs(arguments, fields))
    }

    /// Sends `XLEN`: number of entries, `0` for a missing key.
    pub fn xlen(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["XLEN", key])
    }

    /// Sends `XINFO STREAM`: raw reply with length, first/last entry and group count. Error if the
    /// stream is missing.
    pub fn xinfo_stream(&mut self, key: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "STREAM", key])
    }

    /// Sends `XINFO GROUPS`: raw reply, one array per consumer group.
    pub fn xinfo_groups(&mut self, key: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "GROUPS", key])
    }

    /// Sends `XINFO CONSUMERS`: raw reply, one array per consumer in `group`.
    pub fn xinfo_consumers(&mut self, key: &str, group: &str) -> Result<RespValue, Error> {
        self.run(["XINFO", "CONSUMERS", key, group])
    }

    /// Sends `XSETID`: sets the stream's last generated id without adding an entry. `id` must be
    /// `ms-seq` and not lower than the current top id.
    pub fn xsetid(&mut self, key: &str, id: &str) -> Result<(), Error> {
        self.ok(["XSETID", key, id])
    }

    /// Sends `XAUTOCLAIM`: claims pending entries idle for at least `min_idle_ms` for `consumer`.
    ///
    /// # Arguments
    /// * `start` - id to scan from, exclusive; `0-0` starts at the beginning. Use the returned
    ///   next-start id for the next call.
    /// * `min_idle_ms` - minimum idle time in milliseconds.
    ///
    /// # Returns
    /// Raw reply `[next_start, claimed_entries, deleted_ids]`. Without `COUNT` up to 100 entries
    /// are claimed.
    pub fn xautoclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start: &str,
    ) -> Result<RespValue, Error> {
        self.run([
            "XAUTOCLAIM",
            key,
            group,
            consumer,
            Num::uint(min_idle_ms).as_str(),
            start,
        ])
    }

    /// Sends `XRANGE`: entries from `start` to `end`, both inclusive, oldest first.
    ///
    /// # Arguments
    /// * `start`, `end` - ids, or `-` / `+` for the ends. `(id` makes a bound exclusive.
    /// * `count` - optional cap on returned entries.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let entries = client.xrange("events", "-", "+", Some(10))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xrange(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<u64>,
    ) -> Result<Vec<StreamEntry>, Error> {
        let mut arguments = args!["XRANGE", key, start, end];

        if let Some(count) = count {
            arguments.extend(args!["COUNT", count]);
        }

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XREVRANGE`: entries newest first, from `end` down to `start`. Note the argument
    /// order: the high id comes first. `count` caps the result.
    pub fn xrevrange(
        &mut self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<u64>,
    ) -> Result<Vec<StreamEntry>, Error> {
        let mut arguments = args!["XREVRANGE", key, end, start];

        if let Some(count) = count {
            arguments.extend(args!["COUNT", count]);
        }

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XDEL`: deletes entries by id; returns how many existed.
    pub fn xdel(&mut self, key: &str, ids: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["XDEL", key], ids))
    }

    /// Sends `XTRIM MINID`: removes entries with ids below `id`; returns how many. Ruvio trims
    /// exactly even though no `~` is sent.
    pub fn xtrim_minid(&mut self, key: &str, id: &str) -> Result<i64, Error> {
        self.integer(args!["XTRIM", key, "MINID", id])
    }

    /// Sends `XTRIM MAXLEN`: trims the stream to the newest `max_len` entries and returns how
    /// many were removed.
    pub fn xtrim_maxlen(&mut self, key: &str, max_len: u64) -> Result<i64, Error> {
        self.integer(args!["XTRIM", key, "MAXLEN", max_len])
    }

    /// Sends `XTRIM MAXLEN|MINID [=|~] threshold` with a typed [`StreamTrim`]; returns how many
    /// entries were removed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let removed = client.xtrim("events", &StreamTrim::min_id("1700000000000-0"))?;
    /// let kept = client.xtrim("events", &StreamTrim::max_len(100).mode(TrimMode::Approximate))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xtrim(&mut self, key: &str, trim: &StreamTrim<'_>) -> Result<i64, Error> {
        trim.with_words(|strategy, mode, threshold| {
            self.integer(["XTRIM", key, strategy, mode, threshold])
        })
    }

    /// Sends `XREAD`: reads entries newer than the given ids.
    ///
    /// # Arguments
    /// * `options` - [`StreamReadOptions`]: `count` caps entries per stream; `block` waits for
    ///   new data (`Duration::ZERO` waits forever). `no_ack` is not allowed here.
    /// * `streams` - `(key, last_seen_id)` pairs; use `$` to mean "only entries added from now".
    ///   All keys in one slot.
    ///
    /// # Returns
    /// One [`StreamReadResult`] per stream with data; empty when `block` timed out or nothing is
    /// new.
    ///
    /// # Errors
    /// [`Error::Protocol`] if `no_ack` is set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let options = StreamReadOptions { count: Some(10), block: Some(Duration::from_secs(5)), ..Default::default() };
    /// let batches = client.xread(&options, &[("events", "$")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xread(
        &mut self,
        options: &StreamReadOptions,
        streams: &[(&str, &str)],
    ) -> Result<Vec<StreamReadResult>, Error> {
        if options.no_ack {
            return Err(Error::Protocol(
                "NOACK applies only to XREADGROUP".to_owned(),
            ));
        }

        let mut arguments = args!["XREAD"];

        push_stream_read_options(&mut arguments, options);
        push_streams(&mut arguments, streams);
        stream_read(&self.run(arguments)?)
    }

    /// Sends `XGROUP CREATECONSUMER`: registers a consumer in a group. `1` if new, `0` if it
    /// already existed.
    pub fn xgroup_create_consumer(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<i64, Error> {
        self.integer(args!["XGROUP", "CREATECONSUMER", key, group, consumer])
    }

    /// Sends `XGROUP CREATE`: creates a consumer group.
    ///
    /// # Arguments
    /// * `id` - where the group starts reading: `0` for the whole stream, `$` for new entries
    ///   only.
    /// * `make_stream` - create an empty stream when the key is missing (`MKSTREAM`).
    ///
    /// # Errors
    /// [`Error::Server`] `BUSYGROUP` if the group exists; error if the stream is missing and
    /// `make_stream` is false.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.xgroup_create("events", "workers", "0", true)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xgroup_create(
        &mut self,
        key: &str,
        group: &str,
        id: &str,
        make_stream: bool,
    ) -> Result<(), Error> {
        let mut arguments = args!["XGROUP", "CREATE", key, group, id];

        if make_stream {
            arguments.push("MKSTREAM".to_owned());
        }

        self.ok(arguments)
    }

    /// Sends `XREADGROUP`: reads entries as a consumer of a group.
    ///
    /// # Arguments
    /// * `group`, `consumer` - group and consumer names (the consumer is created on first use).
    /// * `options` - [`StreamReadOptions`]: `count`, `block` and `no_ack` (advance without adding
    ///   to the pending list).
    /// * `streams` - `(key, id)`; id `>` means "entries never delivered to this group"; an
    ///   explicit id re-reads this consumer's pending entries.
    ///
    /// # Returns
    /// Entries per stream; empty when none or when `block` timed out.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let batches = client.xreadgroup("workers", "w1", &StreamReadOptions::default(), &[("events", ">")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        options: &StreamReadOptions,
        streams: &[(&str, &str)],
    ) -> Result<Vec<StreamReadResult>, Error> {
        let mut arguments = args!["XREADGROUP", "GROUP", group, consumer];

        push_stream_read_options(&mut arguments, options);

        if options.no_ack {
            arguments.push("NOACK".to_owned());
        }

        push_streams(&mut arguments, streams);
        stream_read(&self.run(arguments)?)
    }

    /// Sends `XGROUP DESTROY`: `true` if the group existed.
    pub fn xgroup_destroy(&mut self, key: &str, group: &str) -> Result<bool, Error> {
        self.flag(["XGROUP", "DESTROY", key, group])
    }

    /// Sends `XGROUP SETID`: moves the group's cursor to `id` (`$` for the end).
    pub fn xgroup_setid(&mut self, key: &str, group: &str, id: &str) -> Result<(), Error> {
        self.ok(["XGROUP", "SETID", key, group, id])
    }

    /// Sends `XGROUP DELCONSUMER`: removes a consumer and drops its pending entries; returns how
    /// many pending entries it had.
    pub fn xgroup_delconsumer(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<i64, Error> {
        self.integer(["XGROUP", "DELCONSUMER", key, group, consumer])
    }

    /// Sends `XACK`: marks entries as processed; returns how many were acknowledged.
    pub fn xack(&mut self, key: &str, group: &str, ids: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["XACK", key, group], ids))
    }

    /// Sends the summary form of `XPENDING`: raw reply `[count, smallest_id, largest_id,
    /// [[consumer, count], ...]]`.
    pub fn xpending_summary(&mut self, key: &str, group: &str) -> Result<RespValue, Error> {
        self.run(["XPENDING", key, group])
    }

    /// Sends the extended `XPENDING`: lists pending entries.
    ///
    /// # Arguments
    /// * `start`, `end` - id range, inclusive (`-` and `+` for all).
    /// * `count` - maximum rows.
    /// * `filter` - [`StreamPendingFilter`]: restrict to one consumer and/or a minimum idle time.
    pub fn xpending(
        &mut self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: u64,
        filter: &StreamPendingFilter,
    ) -> Result<Vec<StreamPendingEntry>, Error> {
        let mut arguments = args!["XPENDING", key, group];

        if let Some(idle) = filter.min_idle {
            arguments.extend(args!["IDLE", duration_millis(idle)]);
        }

        arguments.extend(args![start, end, count]);

        if let Some(consumer) = &filter.consumer {
            arguments.push(consumer.clone());
        }

        let reply = self.run(arguments)?;
        let mut rows = Vec::new();

        for row in reply.as_array().unwrap_or_default() {
            let Some([id, owner, idle, deliveries]) = row.as_array() else {
                return Err(unexpected("XPENDING", row));
            };

            rows.push(StreamPendingEntry {
                id: id.as_string()?.unwrap_or_default(),
                consumer: owner.as_string()?.unwrap_or_default(),
                idle_millis: idle.as_integer().unwrap_or_default(),
                delivery_count: deliveries.as_integer().unwrap_or_default(),
            });
        }

        Ok(rows)
    }

    /// Sends `XCLAIM`: moves pending entries idle for at least `min_idle` to `consumer`.
    ///
    /// # Arguments
    /// * `ids` - entry ids to claim.
    /// * `options` - [`StreamClaimOptions`] (`idle`, `time`, `retry_count`, `force`, `last_id`).
    ///   `idle` and `time` cannot both be set.
    ///
    /// # Returns
    /// The claimed entries (those still in the stream).
    ///
    /// # Errors
    /// [`Error::Protocol`] if both `idle` and `time` are set.
    pub fn xclaim(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        ids: &[&str],
        options: &StreamClaimOptions,
    ) -> Result<Vec<StreamEntry>, Error> {
        let arguments = claim_arguments(key, group, consumer, min_idle, ids, options)?;

        stream_entries(&self.run(arguments)?)
    }

    /// Sends `XCLAIM ... JUSTID`: like [`Client::xclaim`] but returns only the claimed ids.
    pub fn xclaim_ids(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        ids: &[&str],
        options: &StreamClaimOptions,
    ) -> Result<Vec<String>, Error> {
        let mut arguments = claim_arguments(key, group, consumer, min_idle, ids, options)?;

        arguments.push("JUSTID".to_owned());
        self.strings(arguments)
    }

    /// Sends `INFO` or `INFO section` and returns the text.
    ///
    /// `section` such as `"memory"`, `"server"`, `"keyspace"`, or `"prometheus"` (Prometheus text
    /// format). `None` returns the default sections. On a sharded server the answering shard
    /// reports its own numbers.
    pub fn info(&mut self, section: Option<&str>) -> Result<String, Error> {
        match section {
            Some(section) => self.text(["INFO", section]),
            None => self.text(["INFO"]),
        }
    }

    /// Sends `INFO section` with a typed [`InfoSection`]; an unknown section name would make the
    /// server answer with an empty text, which the enum rules out.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let memory = client.info_section(InfoSection::MemoryBreakdown)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn info_section(&mut self, section: InfoSection) -> Result<String, Error> {
        self.text(["INFO", section.as_str()])
    }

    /// Sends `CONFIG GET`: returns `(parameter, value)` pairs matching a name or glob. Ruvio
    /// lists its own settings and redacts `requirepass`.
    pub fn config_get(&mut self, parameter: &str) -> Result<Vec<(String, String)>, Error> {
        pairs(&self.run(["CONFIG", "GET", parameter])?)
    }

    /// Sends `CONFIG SET`: changes live settings (names as `CONFIG GET` shows them) on every
    /// shard of the server before it answers. All pairs apply or none: an unknown or
    /// restart-only name, or an invalid value, fails the call and changes nothing. Not
    /// written to the config file (see [`Client::config_rewrite`]) and not replicated.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.config_set(&[("maxmemory", "1073741824"), ("maxmemory-policy", "allkeys-lru")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn config_set(&mut self, pairs: &[(&str, &str)]) -> Result<(), Error> {
        self.ok(with_pairs(args!["CONFIG", "SET"], pairs))
    }

    /// Sends `CONFIG REWRITE`: writes the live settings into the config file the server
    /// started from.
    pub fn config_rewrite(&mut self) -> Result<(), Error> {
        self.ok(["CONFIG", "REWRITE"])
    }

    /// Sends `REPLICAOF host port`: every shard of the server stops taking writes and follows
    /// the primary at `host:port` (shard `i` dials `port + i`). A shard continues from its own
    /// log when the new primary's history allows it, otherwise it takes a full copy. Lasts
    /// until the server restarts.
    pub fn replicaof(&mut self, host: &str, port: u16) -> Result<(), Error> {
        let port = port.to_string();

        self.ok(["REPLICAOF", host, port.as_str()])
    }

    /// Sends `REPLICAOF NO ONE`: promotes a replica in place, one epoch above its old primary.
    /// `force` adds `FORCE`, which promotes even when a shard has not finished copying its
    /// primary and may lose writes.
    pub fn replicaof_no_one(&mut self, force: bool) -> Result<(), Error> {
        if force {
            self.ok(["REPLICAOF", "NO", "ONE", "FORCE"])
        } else {
            self.ok(["REPLICAOF", "NO", "ONE"])
        }
    }

    /// Sends `RUVIO MOVESLOTS first-last shard`: moves the hash slots `first..=last` to shard
    /// `shard` (counted from 0) of the same server, which keeps serving them meanwhile. Returns
    /// once the move has started; [`Client::move_slots_status`] follows it. One move runs at
    /// a time, and every slot of the range must be on one shard.
    pub fn move_slots(&mut self, first: u16, last: u16, shard: u16) -> Result<(), Error> {
        let range = format!("{first}-{last}");
        let shard = shard.to_string();

        self.ok(["RUVIO", "MOVESLOTS", range.as_str(), shard.as_str()])
    }

    /// Sends `RUVIO MOVESLOTS STATUS`: the running or last move as name/value pairs. `state`
    /// is `none`, `copying`, `cleaning`, `cancelling`, `done` or `cancelled`; the others are
    /// `first`, `last`, `from`, `to`, `keys`, `rounds`, `elapsed_ms`, `freeze_ms` and `error`.
    pub fn move_slots_status(&mut self) -> Result<Vec<(String, String)>, Error> {
        pairs(&self.run(["RUVIO", "MOVESLOTS", "STATUS"])?)
    }

    /// Sends `ROLE`: raw reply for the shard this connection is on. A primary answers
    /// `["master", sequence, []]`; a replica `["slave", host, port, "connect"|"connected",
    /// sequence]`.
    pub fn role(&mut self) -> Result<RespValue, Error> {
        self.run(["ROLE"])
    }

    /// Sends `SAVE`: writes a snapshot now and waits.
    pub fn save(&mut self) -> Result<(), Error> {
        self.ok(["SAVE"])
    }

    /// Sends `BGSAVE`: starts a snapshot in the server process (not a fork).
    pub fn bgsave(&mut self) -> Result<(), Error> {
        self.ok(["BGSAVE"])
    }

    /// Sends `FLUSHDB`: deletes every key of the selected database.
    ///
    /// A routing client sends it to every shard. A client opened with `discover_cluster: false`
    /// on a `port_mode = "per-shard"` server clears **only that shard's** keys. `ASYNC` is not
    /// used (the server runs it synchronously). Irreversible.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.flushdb()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flushdb(&mut self) -> Result<(), Error> {
        self.ok(["FLUSHDB"])
    }

    /// Sends `FLUSHALL`: deletes the keys of every database. Same shard caveat as
    /// [`Client::flushdb`]. Irreversible.
    pub fn flushall(&mut self) -> Result<(), Error> {
        self.ok(["FLUSHALL"])
    }

    /// Sends `SWAPDB`: swaps two logical databases for all connections (O(1)). Refused on a
    /// sharded server; indexes must be valid database numbers.
    pub fn swapdb(&mut self, first: u32, second: u32) -> Result<(), Error> {
        self.ok(args!["SWAPDB", first, second])
    }

    /// Sends `MOVE`: moves `key` to another logical database. `false` when the key is missing or
    /// already exists there. Refused on a sharded server.
    pub fn move_key(&mut self, key: &str, database: u32) -> Result<bool, Error> {
        self.flag(args!["MOVE", key, database])
    }

    /// Sends `MULTI`: starts a transaction. Until [`Client::exec`] the server answers `QUEUED`,
    /// so queue commands with [`Client::execute`] rather than the typed methods (which expect
    /// real replies). On a sharded server all keys of the transaction must share a slot.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.multi()?;
    /// client.execute(&["INCR", "{acct}:a"])?;
    /// client.execute(&["INCR", "{acct}:b"])?;
    /// let results = client.exec()?; // Some(vec![..]) or None if a WATCHed key changed
    /// # Ok(())
    /// # }
    /// ```
    pub fn multi(&mut self) -> Result<(), Error> {
        self.ok(["MULTI"])
    }

    /// Sends `EXEC`: runs the queued commands atomically.
    ///
    /// Returns `Some(replies)` (one per queued command) or `None` when a `WATCH`ed key changed
    /// and nothing ran. At most 1,024 commands can be queued.
    pub fn exec(&mut self) -> Result<Option<Vec<RespValue>>, Error> {
        match self.run(["EXEC"])? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) => Ok(Some(items)),
            other => Err(unexpected("EXEC", &other)),
        }
    }

    /// Sends `DISCARD`: drops the queued commands and leaves the transaction.
    pub fn discard(&mut self) -> Result<(), Error> {
        self.ok(["DISCARD"])
    }

    /// Sends `WATCH` for one key. See [`Client::watch`].
    pub fn watch_key(&mut self, key: &str) -> Result<(), Error> {
        self.watch(&[key])
    }

    /// Sends `WATCH`: makes the next `EXEC` fail (return `None`) if any of these keys changes
    /// before it. Call it before `MULTI`; up to 128 keys. Keys must share a slot on a sharded
    /// server.
    pub fn watch(&mut self, keys: &[&str]) -> Result<(), Error> {
        self.ok(join("WATCH", keys))
    }

    /// Sends `UNWATCH`: forgets all watched keys.
    pub fn unwatch(&mut self) -> Result<(), Error> {
        self.ok(["UNWATCH"])
    }

    /// Sends `EVAL`: runs a Lua script.
    ///
    /// # Arguments
    /// * `script` - Lua source (Lua 5.1 sandbox). Read `KEYS` and `ARGV`; call commands with
    ///   `ruvio.call`.
    /// * `keys` - key names the script touches, passed as `KEYS`; they must share a slot. A
    ///   script with no keys runs on the connection's shard.
    /// * `arguments` - extra values, passed as `ARGV`.
    ///
    /// # Returns
    /// The script result as a raw [`RespValue`].
    ///
    /// # Errors
    /// [`Error::Server`] for script errors, undeclared keys or `CROSSSLOT`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let reply = client.eval("return redis.call('GET', KEYS[1])", &["k"], &[])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn eval(
        &mut self,
        script: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("EVAL", script, keys, arguments))
    }

    /// Sends `EVALSHA`: runs a cached script by its SHA1 from [`Client::script_load`]. Arguments
    /// as [`Client::eval`]. Error `NOSCRIPT` if the shard does not have it (the cache is per
    /// shard and lost on restart).
    pub fn evalsha(
        &mut self,
        sha: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("EVALSHA", sha, keys, arguments))
    }

    /// Runs a script by its SHA1 and sends the whole script only when the shard does not have
    /// it: `EVALSHA`, then `EVAL` once on `NOSCRIPT`. The SHA1 is computed here, and `EVAL`
    /// also caches the script on that shard. Arguments as [`Client::eval`].
    ///
    /// The retry is safe because `NOSCRIPT` is answered before any command of the script ran,
    /// so a script never runs twice.
    ///
    /// # Errors
    /// As [`Client::eval`].
    pub fn eval_cached(
        &mut self,
        script: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        let sha = crate::sha1::hex_digest(script.as_bytes());

        match self.evalsha(&sha, keys, arguments) {
            Err(Error::Server(message)) if message.starts_with("NOSCRIPT") => {
                self.eval(script, keys, arguments)
            }
            other => other,
        }
    }

    /// Sends `SCRIPT LOAD`: caches a script and returns its SHA1 for [`Client::evalsha`].
    pub fn script_load(&mut self, script: &str) -> Result<String, Error> {
        self.text(["SCRIPT", "LOAD", script])
    }

    /// Sends `SCRIPT EXISTS`: one flag per SHA1, `true` if cached.
    pub fn script_exists(&mut self, hashes: &[&str]) -> Result<Vec<bool>, Error> {
        let reply = self.run(with(args!["SCRIPT", "EXISTS"], hashes))?;

        Ok(reply
            .as_array()
            .unwrap_or_default()
            .iter()
            .map(|item| item.as_integer().unwrap_or_default() > 0)
            .collect())
    }

    /// Sends `SCRIPT FLUSH`: clears the script cache.
    pub fn script_flush(&mut self) -> Result<(), Error> {
        self.ok(["SCRIPT", "FLUSH"])
    }

    /// Sends `SCRIPT KILL`. In Ruvio this answers `NOTBUSY` ([`Error::Server`]); long scripts are
    /// stopped by the instruction and time limits instead.
    pub fn script_kill(&mut self) -> Result<(), Error> {
        self.ok(["SCRIPT", "KILL"])
    }

    /// Sends `FCALL`: calls a function loaded with [`Client::function_load`]. Arguments as
    /// [`Client::eval`].
    pub fn fcall(
        &mut self,
        name: &str,
        keys: &[&str],
        arguments: &[&str],
    ) -> Result<RespValue, Error> {
        self.run(script_arguments("FCALL", name, keys, arguments))
    }

    /// Sends `FUNCTION LOAD name body`: registers a Lua function under `name`. `body` is plain
    /// Lua (not a Redis library shebang). Held in memory only: lost on restart.
    pub fn function_load(&mut self, name: &str, body: &str) -> Result<(), Error> {
        self.ok(["FUNCTION", "LOAD", name, body])
    }

    /// Sends `FUNCTION LIST`: raw reply listing loaded functions.
    pub fn function_list(&mut self) -> Result<RespValue, Error> {
        self.run(["FUNCTION", "LIST"])
    }

    /// Sends `FUNCTION DELETE`: `true` if a function with that name existed.
    pub fn function_delete(&mut self, name: &str) -> Result<bool, Error> {
        self.flag(["FUNCTION", "DELETE", name])
    }

    /// Sends `ACL WHOAMI`: name of the user this connection is authenticated as.
    pub fn acl_whoami(&mut self) -> Result<String, Error> {
        self.text(["ACL", "WHOAMI"])
    }

    /// Sends `ACL USERS`: names of all configured users.
    pub fn acl_users(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "USERS"])
    }

    /// Sends `ACL LIST`: one rule line per user (passwords redacted).
    pub fn acl_list(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "LIST"])
    }

    /// Sends `ACL GETUSER`: raw description of one user.
    pub fn acl_getuser(&mut self, user: &str) -> Result<RespValue, Error> {
        self.run(["ACL", "GETUSER", user])
    }

    /// Sends `ACL CAT`: names of the command categories.
    pub fn acl_cat(&mut self) -> Result<Vec<String>, Error> {
        self.strings(["ACL", "CAT"])
    }

    /// Sends `ACL SETUSER user rule...`: creates or updates a user.
    ///
    /// # Arguments
    /// * `rules` - ACL rule tokens such as `on`, `>password`, `~prefix:*`, `+@read`, `-@dangerous`.
    pub fn acl_setuser(&mut self, user: &str, rules: &[&str]) -> Result<(), Error> {
        self.ok(with(args!["ACL", "SETUSER", user], rules))
    }

    /// Sends `ACL LOAD`: replaces the live users from `security.acl_file`. Error if no file is
    /// configured.
    pub fn acl_load(&mut self) -> Result<(), Error> {
        self.ok(["ACL", "LOAD"])
    }

    /// Sends `ACL SAVE`: writes the users to `security.acl_file` (stored as hashes). Error if no
    /// file is configured.
    pub fn acl_save(&mut self) -> Result<(), Error> {
        self.ok(["ACL", "SAVE"])
    }

    /// Sends `CLUSTER SLOTS`: raw slot map. A single-shard server answers "cluster mode is
    /// disabled". This is what [`Client::connect`] uses to discover shards.
    pub fn cluster_slots(&mut self) -> Result<RespValue, Error> {
        self.run(["CLUSTER", "SLOTS"])
    }

    /// Sends `CLUSTER NODES`: node list in the Redis text format.
    pub fn cluster_nodes(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "NODES"])
    }

    /// Sends `CLUSTER SHARDS`: raw shard description.
    pub fn cluster_shards(&mut self) -> Result<RespValue, Error> {
        self.run(["CLUSTER", "SHARDS"])
    }

    /// Sends `CLUSTER INFO`: cluster state text.
    pub fn cluster_info(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "INFO"])
    }

    /// Sends `CLUSTER MYID`: the id of the answering node.
    pub fn cluster_myid(&mut self) -> Result<String, Error> {
        self.text(["CLUSTER", "MYID"])
    }

    /// Sends `CLUSTER KEYSLOT`: the hash slot (0-16383) the **server** computes for `key`. The
    /// client can compute the same locally with [`hash_slot`](crate::hash_slot).
    pub fn cluster_keyslot(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["CLUSTER", "KEYSLOT", key])
    }

    /// Sends `READONLY`. Ruvio has no replicas; it replies `OK` in cluster mode and does nothing.
    pub fn readonly(&mut self) -> Result<(), Error> {
        self.ok(["READONLY"])
    }

    /// Sends `READWRITE`. A no-op `OK` in Ruvio (no replicas).
    pub fn readwrite(&mut self) -> Result<(), Error> {
        self.ok(["READWRITE"])
    }

    /// Sends `CLIENT GETNAME`: the name set with [`Client::client_setname`], or `None`.
    pub fn client_getname(&mut self) -> Result<Option<String>, Error> {
        self.bulk_or_null(["CLIENT", "GETNAME"])
    }

    /// Sends `CLIENT SETNAME`: names this connection (shown in `CLIENT LIST`). No spaces.
    pub fn client_setname(&mut self, name: &str) -> Result<(), Error> {
        self.ok(["CLIENT", "SETNAME", name])
    }

    /// Sends `CLIENT TRACKING ON|OFF` for this shard. Invalidation pushes are RESP3 and this
    /// client decodes RESP2 only, so this is of limited use here.
    pub fn client_tracking(&mut self, enabled: bool) -> Result<(), Error> {
        self.ok(["CLIENT", "TRACKING", if enabled { "ON" } else { "OFF" }])
    }

    /// Sends `CLIENT DURABILITY REPLICAS replicas [TIMEOUT ms]` for this shard's connection:
    /// replies are held until `replicas` replicas have every earlier write on disk, and become
    /// `-NOREPLICAS` after `timeout_ms` (server default 5000 when `None`); the writes stay
    /// applied. 0 turns it off. Refused inside `MULTI` and on a server without replication.
    /// Use [`ClientOptions::durability`] to set it on every connection.
    pub fn client_durability(
        &mut self,
        replicas: u32,
        timeout_ms: Option<u32>,
    ) -> Result<(), Error> {
        match timeout_ms {
            Some(timeout) => self.ok(args![
                "CLIENT",
                "DURABILITY",
                "REPLICAS",
                replicas,
                "TIMEOUT",
                timeout
            ]),
            None => self.ok(args!["CLIENT", "DURABILITY", "REPLICAS", replicas]),
        }
    }

    /// Sends `HELLO protocol` (2 or 3). Connecting does not send it, and this client only
    /// decodes RESP2 replies, so do not switch to 3. Returns the server info map as a raw value.
    pub fn hello(&mut self, protocol: u8) -> Result<RespValue, Error> {
        self.run(args!["HELLO", protocol])
    }

    /// Sends `PUBLISH`: delivers `message` to subscribers of `channel` on every shard.
    ///
    /// Returns the number of receivers **on the shard that took the command** (as a Redis
    /// Cluster node does). Not durable. With several shards `PUBLISH` is refused inside `MULTI`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let receivers = client.publish("news", "hello")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&mut self, channel: &str, message: &str) -> Result<i64, Error> {
        self.integer(["PUBLISH", channel, message])
    }

    /// Sends `SPUBLISH`: publishes to a sharded channel (same slot as the channel name; use a
    /// hash tag to group channels). Returns the number of receivers.
    pub fn spublish(&mut self, channel: &str, message: &str) -> Result<i64, Error> {
        self.integer(["SPUBLISH", channel, message])
    }

    /// Sends `SUBSCRIBE` and reads one confirmation per channel.
    ///
    /// Opens a dedicated Pub/Sub socket the first time, so `PING` and `PUBLISH` keep using the
    /// command connection. Read deliveries with [`Client::next_message`] or
    /// [`Client::listen`]. Subscriptions are restored after a reconnect.
    ///
    /// # Arguments
    /// * `channels` - at least one channel name.
    ///
    /// # Returns
    /// The raw confirmation replies, one `["subscribe", channel, count]` per channel.
    ///
    /// # Errors
    /// [`Error::Protocol`] for an empty list.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.subscribe(&["news", "alerts"])?;
    /// let message = client.next_message()?;
    /// println!("{}: {}", message.channel, String::from_utf8_lossy(&message.payload));
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SUBSCRIBE", channels, false)
    }

    /// Sends `UNSUBSCRIBE`. An empty list drops every exact-channel subscription.
    pub fn unsubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("UNSUBSCRIBE", channels, true)
    }

    /// Sends `PSUBSCRIBE`: subscribes to glob patterns (`*` and `?` only, e.g. `news.*`).
    /// Deliveries are `pmessage` and carry the matching pattern. At least one pattern.
    pub fn psubscribe(&mut self, patterns: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("PSUBSCRIBE", patterns, false)
    }

    /// Sends `PUNSUBSCRIBE`. An empty list drops every pattern subscription.
    pub fn punsubscribe(&mut self, patterns: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("PUNSUBSCRIBE", patterns, true)
    }

    /// Sends `SSUBSCRIBE`: subscribes to sharded channels (`smessage`). All channels must share
    /// a slot; on a sharded server use [`Client::sharded_subscriber`] to reach the right shard.
    pub fn ssubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SSUBSCRIBE", channels, false)
    }

    /// Sends `SUNSUBSCRIBE`. An empty list drops every sharded subscription.
    pub fn sunsubscribe(&mut self, channels: &[&str]) -> Result<Vec<RespValue>, Error> {
        self.subscription("SUNSUBSCRIBE", channels, true)
    }

    /// Reads until a `message`, `pmessage` or `smessage` arrives, skipping subscribe
    /// confirmations, and returns it as a [`PubSubMessage`].
    ///
    /// Blocks until a message arrives; with [`Client::set_read_timeout`] a quiet channel returns
    /// [`Error::Io`] (and the client stays usable).
    pub fn next_message(&mut self) -> Result<PubSubMessage, Error> {
        loop {
            let reply = self.read_message()?;

            if let Some(message) = pubsub_message(&reply)? {
                return Ok(message);
            }
        }
    }

    /// Calls `on_message` for each delivery, forever. Returns only on an error (including a read
    /// timeout). Safe on the command client after [`Client::subscribe`].
    pub fn listen<F>(&mut self, mut on_message: F) -> Result<(), Error>
    where
        F: FnMut(PubSubMessage),
    {
        loop {
            on_message(self.next_message()?);
        }
    }

    fn subscription(
        &mut self,
        command: &str,
        names: &[&str],
        allow_empty: bool,
    ) -> Result<Vec<RespValue>, Error> {
        if names.is_empty() && !allow_empty {
            return Err(Error::Protocol(format!(
                "{command} needs at least one channel or pattern"
            )));
        }

        self.run_replies(join(command, names), names.len().max(1))
    }

    pub(crate) fn integers<A: crate::args::Args>(
        &mut self,
        arguments: A,
    ) -> Result<Vec<i64>, Error> {
        self.run_with(arguments, |command, reply| integer_array(command, &reply))
    }

    fn blocking_sorted_pop(
        &mut self,
        command: &str,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, SortedSetEntry)>, Error> {
        let mut arguments = join(command, keys);

        arguments.push(duration_secs(timeout).to_string());

        match self.run(arguments)? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) if items.len() == 3 => Ok(Some((
                items[0].as_string()?.unwrap_or_default(),
                SortedSetEntry::new(
                    items[1].as_string()?.unwrap_or_default(),
                    parse_score(&items[2].as_string()?.unwrap_or_default())?,
                ),
            ))),
            other => Err(unexpected(command, &other)),
        }
    }

    fn blocking_pop(
        &mut self,
        command: &str,
        keys: &[&str],
        timeout: Duration,
    ) -> Result<Option<(String, String)>, Error> {
        let mut arguments = join(command, keys);

        arguments.push(duration_secs(timeout).to_string());

        match self.run(arguments)? {
            RespValue::Null => Ok(None),
            RespValue::Array(items) if items.len() == 2 => Ok(Some((
                items[0].as_string()?.unwrap_or_default(),
                items[1].as_string()?.unwrap_or_default(),
            ))),
            other => Err(unexpected(command, &other)),
        }
    }
}

impl Client {
    /// Sends `KEYS`: all key names matching a glob (`*`, `?`, `[a-z]`). O(keyspace): use
    /// [`Client::scan_iter`] in production. The server refuses to return more than 100,000 keys.
    /// A routing client asks every shard.
    pub fn keys(&mut self, pattern: &str) -> Result<Vec<String>, Error> {
        self.strings(["KEYS", pattern])
    }

    /// Sends `RANDOMKEY`: a random key, `None` when the database is empty.
    pub fn random_key(&mut self) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RANDOMKEY"])
    }

    /// Sends `TOUCH`: returns how many of the keys exist (updates their last-access time).
    /// Split per slot on a sharded server.
    pub fn touch(&mut self, keys: &[&str]) -> Result<i64, Error> {
        self.integer(join("TOUCH", keys))
    }

    /// Sends `RENAMENX`: renames only if `new_key` does not exist; `false` otherwise. Error if
    /// `key` is missing. Same slot on a sharded server.
    pub fn rename_nx(&mut self, key: &str, new_key: &str) -> Result<bool, Error> {
        self.flag(["RENAMENX", key, new_key])
    }

    /// Sends `EXPIRETIME`: absolute expiry as Unix seconds; `-1` without TTL, `-2` missing.
    pub fn expire_time(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["EXPIRETIME", key])
    }

    /// Sends `PEXPIRETIME`: absolute expiry as Unix milliseconds; `-1` without TTL, `-2` missing.
    pub fn pexpire_time(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["PEXPIRETIME", key])
    }

    /// Sends `COPY`: duplicates a key (including its TTL; JSON documents too).
    ///
    /// `false` when the source is missing or the destination exists and `replace` is false.
    /// Same slot on a sharded server.
    pub fn copy(&mut self, source: &str, destination: &str, replace: bool) -> Result<bool, Error> {
        let mut arguments = args!["COPY", source, destination];

        if replace {
            arguments.push("REPLACE".to_owned());
        }

        self.flag(arguments)
    }

    /// Sends `ECHO`: returns `message` unchanged.
    pub fn echo(&mut self, message: &str) -> Result<String, Error> {
        self.text(["ECHO", message])
    }

    /// Sends `OBJECT ENCODING`: internal encoding name; `None` for a missing key.
    pub fn object_encoding(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["OBJECT", "ENCODING", key])
    }

    /// Sends `OBJECT IDLETIME`: seconds since last access; `None` for a missing key.
    pub fn object_idletime(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["OBJECT", "IDLETIME", key])
    }

    /// Sends `OBJECT FREQ`: access-frequency counter; `None` for a missing key.
    pub fn object_freq(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["OBJECT", "FREQ", key])
    }

    /// Sends `MEMORY USAGE`: bytes used by the key; `None` for a missing key.
    pub fn memory_usage(&mut self, key: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["MEMORY", "USAGE", key])
    }

    /// Sends `SLOWLOG LEN`: number of entries in the slow log.
    pub fn slowlog_len(&mut self) -> Result<i64, Error> {
        self.integer(["SLOWLOG", "LEN"])
    }

    /// Sends `SLOWLOG RESET`: clears the slow log.
    pub fn slowlog_reset(&mut self) -> Result<(), Error> {
        self.ok(["SLOWLOG", "RESET"])
    }

    /// Sends `SLOWLOG GET count`: the newest `count` entries, each as the raw reply.
    pub fn slowlog_get(&mut self, count: u64) -> Result<Vec<RespValue>, Error> {
        match self.run(args!["SLOWLOG", "GET", count])? {
            RespValue::Array(items) => Ok(items),
            other => Err(unexpected("SLOWLOG", &other)),
        }
    }

    /// Sends `CLIENT ID`: this connection's id.
    pub fn client_id(&mut self) -> Result<i64, Error> {
        self.integer(["CLIENT", "ID"])
    }

    /// Sends `CLIENT INFO`: one text line describing this connection.
    pub fn client_info(&mut self) -> Result<String, Error> {
        self.text(["CLIENT", "INFO"])
    }

    /// Sends `CLIENT LIST`: one text line per connection on the answering shard.
    pub fn client_list(&mut self) -> Result<String, Error> {
        self.text(["CLIENT", "LIST"])
    }

    /// Sends `CLIENT LIST TYPE type` with a typed [`ClientType`]. Ruvio checks the name but lists
    /// every connection on the answering shard.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let lines = client.client_list_type(ClientType::PubSub)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_list_type(&mut self, kind: ClientType) -> Result<String, Error> {
        self.text(["CLIENT", "LIST", "TYPE", kind.as_str()])
    }

    /// Sends `CLIENT KILL ID`: closes the connection with that id; returns how many were closed.
    pub fn client_kill_id(&mut self, id: i64) -> Result<i64, Error> {
        self.integer(args!["CLIENT", "KILL", "ID", id])
    }

    /// Sends `LINSERT`: inserts `value` next to the first occurrence of `pivot`.
    ///
    /// # Arguments
    /// * `position` - [`InsertPosition::Before`] or [`InsertPosition::After`] the pivot.
    ///
    /// # Returns
    /// The new list length; `-1` when the pivot is not in the list; `0` when the list does not
    /// exist (nothing written). The list is rewritten, so this is O(n).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.linsert("queue", InsertPosition::Before, "b", "x")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn linsert(
        &mut self,
        key: &str,
        position: InsertPosition,
        pivot: &str,
        value: &str,
    ) -> Result<i64, Error> {
        self.integer(["LINSERT", key, position.as_str(), pivot, value])
    }

    /// Sends `LSET`: overwrites the element at `index` (negative counts from the tail).
    ///
    /// # Errors
    /// [`Error::Server`] `ERR no such key` or `ERR index out of range`.
    pub fn lset(&mut self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        self.ok(args!["LSET", key, index, value])
    }

    /// Sends `LREM`: removes occurrences of `value`.
    ///
    /// # Arguments
    /// * `count` - `> 0` removes up to `count` from the head, `< 0` from the tail, `0` removes
    ///   all.
    ///
    /// Returns how many were removed.
    pub fn lrem(&mut self, key: &str, count: i64, value: &str) -> Result<i64, Error> {
        self.integer(args!["LREM", key, count, value])
    }

    /// Sends `LPOS`: index of the first element equal to `value`; `None` if absent. See
    /// [`Client::lpos_with`] for `RANK`/`MAXLEN`.
    pub fn lpos(&mut self, key: &str, value: &str) -> Result<Option<i64>, Error> {
        self.optional_integer(["LPOS", key, value])
    }

    /// Sends `LPUSHX`: pushes to the head only if the list exists. Returns the new length, or `0`
    /// (nothing written) when the list does not exist.
    pub fn lpushx(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["LPUSHX", key], values))
    }

    /// Sends `RPUSHX`: appends only if the list exists; `0` when it does not.
    pub fn rpushx(&mut self, key: &str, values: &[&str]) -> Result<i64, Error> {
        self.integer(with(args!["RPUSHX", key], values))
    }

    /// Sends `LMOVE`: pops one element from `source` and pushes it onto `destination` atomically.
    ///
    /// # Arguments
    /// * `from` - [`ListSide`] to pop from (`Left` is the head, `Right` the tail).
    /// * `to` - [`ListSide`] to push to. Same slot as `source` on a sharded server.
    ///
    /// # Returns
    /// The moved element, or `None` when `source` is empty or missing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let moved = client.lmove("src", "dst", ListSide::Left, ListSide::Right)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn lmove(
        &mut self,
        source: &str,
        destination: &str,
        from: ListSide,
        to: ListSide,
    ) -> Result<Option<String>, Error> {
        self.bulk_or_null(["LMOVE", source, destination, from.as_str(), to.as_str()])
    }

    /// Sends `RPOPLPUSH`: pops the tail of `source` and pushes it on the head of `destination`;
    /// returns the element, `None` if `source` is empty. Same slot on a sharded server.
    pub fn rpoplpush(&mut self, source: &str, destination: &str) -> Result<Option<String>, Error> {
        self.bulk_or_null(["RPOPLPUSH", source, destination])
    }

    /// Sends `BLMOVE`: like [`Client::lmove`] but waits for an element.
    ///
    /// `timeout` rounds up to whole seconds; `Duration::ZERO` waits forever. Returns the element
    /// or `None` on timeout. Ruvio retries every 5 ms instead of parking a waiter, and `BLMOVE`
    /// is refused inside `MULTI` and scripts. The command timeout is extended by `timeout`.
    pub fn blmove(
        &mut self,
        source: &str,
        destination: &str,
        from: ListSide,
        to: ListSide,
        timeout: Duration,
    ) -> Result<Option<String>, Error> {
        let seconds = Num::uint(duration_secs(timeout));

        self.bulk_or_null([
            "BLMOVE",
            source,
            destination,
            from.as_str(),
            to.as_str(),
            seconds.as_str(),
        ])
    }

    /// Sends `LMPOP`: pops up to `count` elements from the first non-empty list.
    ///
    /// # Arguments
    /// * `keys` - lists checked in order (one slot). `side` - [`ListSide`] to pop from.
    /// * `count` - maximum elements, at least 1.
    ///
    /// # Returns
    /// `Some((key, elements))`, or `None` when every list is empty.
    pub fn lmpop(
        &mut self,
        keys: &[&str],
        side: ListSide,
        count: u64,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        let mut arguments = args!["LMPOP", keys.len()];

        arguments.extend(keys.iter().map(|key| (*key).to_owned()));
        arguments.extend(args![side.as_str(), "COUNT", count]);

        pop_many("LMPOP", self.run(arguments)?)
    }

    /// Sends `BITFIELD key operation...`: reads and writes integer fields inside a string.
    ///
    /// # Arguments
    /// * `operations` - [`BitFieldOp`] items run in order: `Get`, `Set`, `IncrBy`, and
    ///   `Overflow(mode)` which sets `WRAP` / `SAT` / `FAIL` for the writes after it. A field has a
    ///   [`BitFieldType`](crate::BitFieldType) (`i1`..`i64`, `u1`..`u63`) and a [`BitFieldOffset`] (a bit index, or the
    ///   n-th field of that width).
    ///
    /// # Returns
    /// One result per `Get`/`Set`/`IncrBy` (`Set` returns the old value); `None` when
    /// `OVERFLOW FAIL` refused the write. `Overflow` itself produces no result. A write extends the
    /// string and keeps its TTL.
    pub fn bitfield(
        &mut self,
        key: &str,
        operations: &[BitFieldOp],
    ) -> Result<Vec<Option<i64>>, Error> {
        self.bitfield_command("BITFIELD", key, operations)
    }

    /// Sends `BITFIELD_RO`: read-only form of [`Client::bitfield`]; only [`BitFieldOp::Get`] is
    /// accepted.
    pub fn bitfield_ro(
        &mut self,
        key: &str,
        operations: &[BitFieldOp],
    ) -> Result<Vec<Option<i64>>, Error> {
        self.bitfield_command("BITFIELD_RO", key, operations)
    }

    fn bitfield_command(
        &mut self,
        name: &str,
        key: &str,
        operations: &[BitFieldOp],
    ) -> Result<Vec<Option<i64>>, Error> {
        // Each operation fills at most three number/type slots; the keywords are static.
        let mut texts: Vec<Num> = Vec::with_capacity(operations.len() * 2);
        let mut types: Vec<BitFieldText> = Vec::with_capacity(operations.len());

        for operation in operations {
            match *operation {
                BitFieldOp::Get { ty, offset } => {
                    types.push(ty.text());
                    texts.push(bitfield_offset(offset));
                }
                BitFieldOp::Set {
                    ty, offset, value, ..
                } => {
                    types.push(ty.text());
                    texts.push(bitfield_offset(offset));
                    texts.push(Num::int(value));
                }
                BitFieldOp::IncrBy {
                    ty, offset, delta, ..
                } => {
                    types.push(ty.text());
                    texts.push(bitfield_offset(offset));
                    texts.push(Num::int(delta));
                }
                BitFieldOp::Overflow(_) => {}
            }
        }

        let mut arguments: Vec<&str> = Vec::with_capacity(2 + operations.len() * 4);
        let (mut next_type, mut next_text) = (0, 0);

        arguments.extend([name, key]);

        for operation in operations {
            match *operation {
                BitFieldOp::Get { .. } => {
                    arguments.extend(["GET", types[next_type].as_str(), texts[next_text].as_str()]);
                    next_type += 1;
                    next_text += 1;
                }
                BitFieldOp::Set { .. } => {
                    arguments.extend([
                        "SET",
                        types[next_type].as_str(),
                        texts[next_text].as_str(),
                        texts[next_text + 1].as_str(),
                    ]);
                    next_type += 1;
                    next_text += 2;
                }
                BitFieldOp::IncrBy { .. } => {
                    arguments.extend([
                        "INCRBY",
                        types[next_type].as_str(),
                        texts[next_text].as_str(),
                        texts[next_text + 1].as_str(),
                    ]);
                    next_type += 1;
                    next_text += 2;
                }
                BitFieldOp::Overflow(mode) => arguments.extend(["OVERFLOW", mode.as_str()]),
            }
        }

        match self.run(arguments)? {
            RespValue::Array(items) => items
                .iter()
                .map(|item| match item {
                    RespValue::Null => Ok(None),
                    RespValue::Integer(value) => Ok(Some(*value)),
                    other => Err(unexpected(name, other)),
                })
                .collect(),
            other => Err(unexpected(name, &other)),
        }
    }

    /// Sends `FILLGET` (Ruvio): cache read with stampede protection. See [`FillOutcome`].
    ///
    /// # Arguments
    /// * `fill` - how long the "someone is filling" marker lives; after that another caller may
    ///   take over.
    ///
    /// # Returns
    /// * `Hit(value)` - fresh value stored.
    /// * `Fill(token)` - nothing stored and **you** must build the value, then call
    ///   [`Client::fill_set`] with the token.
    /// * `Busy` - someone else is building it.
    /// * `Stale { value, token }` - past its soft TTL; `token` is `Some` when you should refresh.
    ///
    /// Markers are kept in memory and lapse after `fill`. See [`Client::get_or_create`] for a
    /// ready-made loop.
    pub fn fill_get(&mut self, key: &str, fill: Duration) -> Result<FillOutcome, Error> {
        parse_fill(self.run(args!["FILLGET", key, duration_millis(fill)])?)
    }

    /// Sends `FILLSET` (Ruvio): stores the value built after a `Fill` result.
    ///
    /// # Arguments
    /// * `value` - the built value. `ttl` - its lifetime.
    /// * `token` - the token from [`Client::fill_get`]; stale tokens are refused.
    /// * `stale` - `Some(d)` keeps the old value readable for `d` after `ttl` (stale-while-
    ///   revalidate).
    ///
    /// # Returns
    /// `true` when written; `false` when the token was no longer valid.
    pub fn fill_set(
        &mut self,
        key: &str,
        value: &str,
        ttl: Duration,
        token: &str,
        stale: Option<Duration>,
    ) -> Result<bool, Error> {
        let mut arguments = args!["FILLSET", key, value, duration_millis(ttl), token];

        if let Some(stale) = stale {
            arguments.extend(args!["SWR", duration_millis(stale)]);
        }

        self.flag(arguments)
    }
}

/// A `BITFIELD` offset: `100` for a bit offset, `#3` for the third field of the type's width.
fn bitfield_offset(offset: BitFieldOffset) -> Num {
    match offset {
        BitFieldOffset::Bit(bit) => Num::uint(bit),
        BitFieldOffset::Field(index) => Num::hash_uint(index),
    }
}

pub(crate) fn pop_many(
    command: &str,
    reply: RespValue,
) -> Result<Option<(String, Vec<String>)>, Error> {
    match reply {
        RespValue::Null => Ok(None),
        RespValue::Array(items) if items.len() == 2 => {
            let key = items[0].as_string()?.unwrap_or_default();
            let elements = match &items[1] {
                RespValue::Array(values) => values
                    .iter()
                    .map(|value| Ok(value.as_string()?.unwrap_or_default()))
                    .collect::<Result<Vec<_>, Error>>()?,
                other => return Err(unexpected(command, other)),
            };

            Ok(Some((key, elements)))
        }
        other => Err(unexpected(command, &other)),
    }
}

pub(crate) fn parse_fill(reply: RespValue) -> Result<FillOutcome, Error> {
    let items = match &reply {
        RespValue::Array(items) if items.len() >= 2 => items,
        other => return Err(unexpected("FILLGET", other)),
    };
    let state = items[0].as_string()?.unwrap_or_default();
    let text = |value: &RespValue| value.as_string();

    match (state.as_str(), items.len()) {
        ("HIT", 2) => Ok(FillOutcome::Hit(text(&items[1])?.unwrap_or_default())),
        ("FILL", 2) => Ok(FillOutcome::Fill(text(&items[1])?.unwrap_or_default())),
        ("BUSY", 2) => Ok(FillOutcome::Busy),
        ("STALE", 3) => Ok(FillOutcome::Stale {
            value: text(&items[1])?.unwrap_or_default(),
            token: text(&items[2])?,
        }),
        _ => Err(unexpected("FILLGET", &reply)),
    }
}

/// Appends an optional `NX`/`XX`/`GT`/`LT`, then `FIELDS numfields field ...`.
fn field_block(
    mut arguments: Vec<String>,
    condition: FieldExpireCondition,
    fields: &[&str],
) -> Vec<String> {
    let flag = match condition {
        FieldExpireCondition::Always => None,
        FieldExpireCondition::IfNoTtl => Some("NX"),
        FieldExpireCondition::IfTtl => Some("XX"),
        FieldExpireCondition::IfGreater => Some("GT"),
        FieldExpireCondition::IfLess => Some("LT"),
    };

    arguments.extend(flag.map(str::to_owned));
    arguments.push("FIELDS".to_owned());
    arguments.push(fields.len().to_string());

    with(arguments, fields)
}

fn integer_array(command: &str, reply: &RespValue) -> Result<Vec<i64>, Error> {
    let Some(items) = reply.as_array() else {
        return Err(unexpected(command, reply));
    };

    items
        .iter()
        .map(|item| item.as_integer().ok_or_else(|| unexpected(command, item)))
        .collect()
}

fn with(mut arguments: Vec<String>, rest: &[&str]) -> Vec<String> {
    arguments.extend(rest.iter().map(|argument| (*argument).to_owned()));

    arguments
}

fn with_pairs(mut arguments: Vec<String>, pairs: &[(&str, &str)]) -> Vec<String> {
    for (left, right) in pairs {
        arguments.push((*left).to_owned());
        arguments.push((*right).to_owned());
    }

    arguments
}

fn push_scan_options(arguments: &mut Vec<String>, pattern: Option<&str>, count: Option<u64>) {
    if let Some(pattern) = pattern {
        arguments.extend(args!["MATCH", pattern]);
    }

    if let Some(count) = count {
        arguments.extend(args!["COUNT", count]);
    }
}

fn push_stream_read_options(arguments: &mut Vec<String>, options: &StreamReadOptions) {
    if let Some(count) = options.count {
        arguments.extend(args!["COUNT", count]);
    }

    if let Some(block) = options.block {
        arguments.extend(args!["BLOCK", duration_millis(block)]);
    }
}

fn push_streams(arguments: &mut Vec<String>, streams: &[(&str, &str)]) {
    arguments.push("STREAMS".to_owned());
    arguments.extend(streams.iter().map(|(key, _)| (*key).to_owned()));
    arguments.extend(streams.iter().map(|(_, id)| (*id).to_owned()));
}

fn zadd_arguments(key: &str, options: SortedSetAddOptions) -> Result<Vec<String>, Error> {
    let ttl = options.ttl.map(ttl_words).transpose()?;
    let mut arguments = args!["ZADD", key];

    if let Some((word, count)) = ttl {
        arguments.push(word.to_owned());
        arguments.push(count.as_str().to_owned());
    }

    let flags = [
        (options.if_not_exists, "NX"),
        (options.if_exists, "XX"),
        (options.greater_than, "GT"),
        (options.less_than, "LT"),
        (options.changed, "CH"),
    ];

    for (enabled, flag) in flags {
        if enabled {
            arguments.push(flag.to_owned());
        }
    }

    Ok(arguments)
}

fn claim_arguments(
    key: &str,
    group: &str,
    consumer: &str,
    min_idle: Duration,
    ids: &[&str],
    options: &StreamClaimOptions,
) -> Result<Vec<String>, Error> {
    if options.idle.is_some() && options.time.is_some() {
        return Err(Error::Protocol(
            "IDLE and TIME cannot be combined".to_owned(),
        ));
    }

    let mut arguments = with(
        args!["XCLAIM", key, group, consumer, duration_millis(min_idle)],
        ids,
    );

    if let Some(idle) = options.idle {
        arguments.extend(args!["IDLE", duration_millis(idle)]);
    }

    if let Some(time) = options.time {
        arguments.extend(args!["TIME", unix_millis(time)?]);
    }

    if let Some(retry_count) = options.retry_count {
        arguments.extend(args!["RETRYCOUNT", retry_count]);
    }

    if options.force {
        arguments.push("FORCE".to_owned());
    }

    if let Some(last_id) = &options.last_id {
        arguments.extend(args!["LASTID", last_id]);
    }

    Ok(arguments)
}

fn script_arguments(command: &str, script: &str, keys: &[&str], arguments: &[&str]) -> Vec<String> {
    let values = with(args![command, script, keys.len()], keys);

    with(values, arguments)
}

fn parse_scan(command: &str, reply: RespValue) -> Result<(u64, Vec<String>), Error> {
    let Some([cursor, items]) = reply.as_array() else {
        return Err(unexpected(command, &reply));
    };

    let cursor = cursor
        .as_string()?
        .unwrap_or_default()
        .parse()
        .map_err(|_| Error::Protocol(format!("{command} returned a bad cursor")))?;
    let mut keys = Vec::new();

    for item in items.as_array().unwrap_or_default() {
        keys.push(item.as_string()?.unwrap_or_default());
    }

    Ok((cursor, keys))
}

pub(crate) fn parse_score(text: &str) -> Result<f64, Error> {
    text.parse()
        .map_err(|_| Error::Protocol(format!("{text:?} is not a score")))
}

fn pairs(reply: &RespValue) -> Result<Vec<(String, String)>, Error> {
    let items = reply.as_array().unwrap_or_default();
    let mut values = Vec::with_capacity(items.len() / 2);

    for pair in items.chunks_exact(2) {
        values.push((
            pair[0].as_string()?.unwrap_or_default(),
            pair[1].as_string()?.unwrap_or_default(),
        ));
    }

    Ok(values)
}

fn sorted_set(reply: &RespValue) -> Result<Vec<SortedSetEntry>, Error> {
    pairs(reply)?
        .into_iter()
        .map(|(member, score)| Ok(SortedSetEntry::new(member, parse_score(&score)?)))
        .collect()
}

fn stream_entries(reply: &RespValue) -> Result<Vec<StreamEntry>, Error> {
    let mut entries = Vec::new();

    for item in reply.as_array().unwrap_or_default() {
        let Some([id, fields]) = item.as_array() else {
            continue;
        };

        entries.push(StreamEntry {
            id: id.as_string()?.unwrap_or_default(),
            fields: pairs(fields)?,
        });
    }

    Ok(entries)
}

fn stream_read(reply: &RespValue) -> Result<Vec<StreamReadResult>, Error> {
    let mut results = Vec::new();

    for item in reply.as_array().unwrap_or_default() {
        let Some([key, entries]) = item.as_array() else {
            continue;
        };

        results.push(StreamReadResult {
            key: key.as_string()?.unwrap_or_default(),
            entries: stream_entries(entries)?,
        });
    }

    Ok(results)
}

fn pubsub_message(reply: &RespValue) -> Result<Option<PubSubMessage>, Error> {
    let items = reply.as_array().unwrap_or_default();
    let kind = match items.first() {
        Some(first) => first.as_string()?.unwrap_or_default(),
        None => return Ok(None),
    };

    let (pattern, channel, payload) = match (kind.as_str(), items) {
        ("message" | "smessage", [_, channel, payload]) => (None, channel, payload),
        ("pmessage", [_, pattern, channel, payload]) => (pattern.as_string()?, channel, payload),

        _ => return Ok(None),
    };

    let payload = match payload {
        RespValue::Bulk(bytes) => bytes.clone(),
        other => other.as_string()?.unwrap_or_default().into_bytes(),
    };

    Ok(Some(PubSubMessage {
        kind,
        pattern,
        channel: channel.as_string()?.unwrap_or_default(),
        payload,
    }))
}

fn limit_remaining(value: &RespValue) -> Result<Option<i64>, Error> {
    if value.is_null() {
        return Ok(None);
    }

    value
        .as_integer()
        .map(Some)
        .ok_or_else(|| unexpected("LIMIT", value))
}

fn limit_outcome(value: &RespValue) -> Result<LimitOutcome, Error> {
    let [remaining, retry_after_ms] = value.as_array().unwrap_or_default() else {
        return Err(unexpected("LIMIT", value));
    };
    let (Some(remaining), Some(retry_after_ms)) =
        (remaining.as_integer(), retry_after_ms.as_integer())
    else {
        return Err(unexpected("LIMIT", value));
    };

    Ok(LimitOutcome {
        allowed: remaining >= 0,
        remaining: remaining.max(0),
        retry_after_ms: retry_after_ms.max(0) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::{field_block, integer_array, parse_fill, pop_many, pubsub_message};
    use crate::models::{FieldExpireCondition, FillOutcome};
    use crate::value::RespValue;

    #[test]
    fn field_block_puts_the_condition_before_fields() {
        assert_eq!(
            field_block(
                vec!["HEXPIRE".into(), "h".into(), "10".into()],
                FieldExpireCondition::IfGreater,
                &["a", "b"],
            ),
            ["HEXPIRE", "h", "10", "GT", "FIELDS", "2", "a", "b"]
        );
        assert_eq!(
            field_block(
                vec!["HTTL".into(), "h".into()],
                FieldExpireCondition::Always,
                &["a"]
            ),
            ["HTTL", "h", "FIELDS", "1", "a"]
        );
        assert_eq!(
            integer_array(
                "HTTL",
                &RespValue::Array(vec![RespValue::Integer(-2), RespValue::Integer(5)])
            )
            .unwrap(),
            vec![-2, 5]
        );
    }

    #[test]
    fn fill_replies_parse_into_outcomes() {
        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let reply = |items: Vec<RespValue>| RespValue::Array(items);

        assert_eq!(
            parse_fill(reply(vec![bulk("HIT"), bulk("v")])).unwrap(),
            FillOutcome::Hit("v".to_owned())
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("FILL"), bulk("t1")])).unwrap(),
            FillOutcome::Fill("t1".to_owned())
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("BUSY"), RespValue::Null])).unwrap(),
            FillOutcome::Busy
        );
        assert_eq!(
            parse_fill(reply(vec![bulk("STALE"), bulk("old"), RespValue::Null])).unwrap(),
            FillOutcome::Stale {
                value: "old".to_owned(),
                token: None
            }
        );
        assert!(parse_fill(RespValue::Integer(1)).is_err());
    }

    #[test]
    fn lmpop_reply_parses_key_and_elements() {
        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let reply = RespValue::Array(vec![
            bulk("l"),
            RespValue::Array(vec![bulk("a"), bulk("b")]),
        ]);

        assert_eq!(
            pop_many("LMPOP", reply).unwrap(),
            Some(("l".to_owned(), vec!["a".to_owned(), "b".to_owned()]))
        );
        assert_eq!(pop_many("LMPOP", RespValue::Null).unwrap(), None);
    }

    #[test]
    fn pubsub_message_reads_payload_from_an_array() {
        let reply = RespValue::Array(vec![
            RespValue::Bulk(b"message".to_vec()),
            RespValue::Bulk(b"news".to_vec()),
            RespValue::Bulk(b"hello-ruvio".to_vec()),
        ]);

        let parsed = pubsub_message(&reply).unwrap().unwrap();

        assert_eq!(parsed.kind, "message");
        assert_eq!(parsed.channel, "news");
        assert_eq!(parsed.payload, b"hello-ruvio");
        assert!(
            pubsub_message(&RespValue::Array(vec![
                RespValue::Bulk(b"subscribe".to_vec()),
                RespValue::Bulk(b"news".to_vec()),
                RespValue::Integer(1),
            ]))
            .unwrap()
            .is_none()
        );
    }
}

/// `[a, b]` from `TRANSFER` and `HTRANSFER`; null (nothing moved) is `None`.
fn balance_pair(command: &str, value: RespValue) -> Result<Option<(i64, i64)>, Error> {
    if value.is_null() {
        return Ok(None);
    }

    match value.as_array() {
        Some([first, second]) => match (first.as_integer(), second.as_integer()) {
            (Some(first), Some(second)) => Ok(Some((first, second))),
            _ => Err(unexpected(command, &value)),
        },
        _ => Err(unexpected(command, &value)),
    }
}
