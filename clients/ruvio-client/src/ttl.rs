//! TTL on create (Ruvio): `EX seconds` / `PX milliseconds` on the commands that can create a
//! key, and the `NX` / `XX` / `GT` / `LT` condition of the `EXPIRE` family.
//!
//! The deadline is set only when the call creates the key; an existing key keeps its deadline.
//! A TTL is sent as `EX` for whole seconds and as `PX` (rounded up to a millisecond) otherwise.
//! A zero TTL, or one that does not fit the server's range, is refused here with
//! [`Error::Protocol`] before anything is sent.

use std::time::{Duration, SystemTime};

use crate::args::Num;
use crate::client::{Client, duration_millis, duration_secs, unexpected, unix_millis};
use crate::commands::parse_score;
use crate::error::Error;
use crate::keywords::ExpireCondition;

/// The `EX` / `PX` word and its number for `ttl`.
pub(crate) fn ttl_words(ttl: Duration) -> Result<(&'static str, Num), Error> {
    if ttl.is_zero() {
        return Err(Error::Protocol("TTL must be greater than zero".to_owned()));
    }

    let millis = duration_millis(ttl);

    if millis > i64::MAX as u128 {
        return Err(Error::Protocol("TTL is too large".to_owned()));
    }

    if ttl.subsec_nanos() == 0 {
        return Ok(("EX", Num::uint(duration_secs(ttl))));
    }

    Ok(("PX", Num::wide(millis)))
}

impl Client {
    /// Sends `EXPIRE` or `PEXPIRE` with one [`ExpireCondition`]: the deadline changes only when
    /// the condition holds. A key without a deadline counts as an infinite one, so `Gt` never
    /// applies to it and `Lt` always does.
    ///
    /// Sends `EXPIRE` for whole seconds and `PEXPIRE` otherwise, as [`Client::expire`].
    ///
    /// # Returns
    /// `true` when the deadline was set; `false` when the key is missing or the condition failed.
    ///
    /// # Errors
    /// [`Error::Server`] for a zero or negative TTL.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // Give the key a deadline only when it has none yet.
    /// let set = client.expire_if("session:1", Duration::from_secs(60), ExpireCondition::Nx)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn expire_if(
        &mut self,
        key: &str,
        ttl: Duration,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        if ttl.subsec_nanos() == 0 {
            let seconds = Num::uint(duration_secs(ttl));

            return self.flag(["EXPIRE", key, seconds.as_str(), condition.as_str()]);
        }

        self.pexpire_if(key, ttl, condition)
    }

    /// Sends `PEXPIRE key ms NX|XX|GT|LT`: as [`Client::expire_if`] with the TTL in milliseconds
    /// (rounded up from `ttl`).
    pub fn pexpire_if(
        &mut self,
        key: &str,
        ttl: Duration,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        let millis = Num::wide(duration_millis(ttl));

        self.flag(["PEXPIRE", key, millis.as_str(), condition.as_str()])
    }

    /// Sends `EXPIREAT` (or `PEXPIREAT` when `when` is not a whole second) with one
    /// [`ExpireCondition`]. Returns `false` when the key is missing or the condition failed.
    ///
    /// # Errors
    /// [`Error::Protocol`] when `when` is before the Unix epoch.
    pub fn expire_at_if(
        &mut self,
        key: &str,
        when: SystemTime,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        let millis = unix_millis(when)?;

        if millis % 1000 == 0 {
            let seconds = Num::wide(millis / 1000);

            return self.flag(["EXPIREAT", key, seconds.as_str(), condition.as_str()]);
        }

        self.pexpire_at_if(key, when, condition)
    }

    /// Sends `PEXPIREAT key unix_ms NX|XX|GT|LT`: as [`Client::expire_at_if`], always in
    /// milliseconds.
    pub fn pexpire_at_if(
        &mut self,
        key: &str,
        when: SystemTime,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        let millis = Num::wide(unix_millis(when)?);

        self.flag(["PEXPIREAT", key, millis.as_str(), condition.as_str()])
    }

    /// Sends `INCR key EX|PX ttl`: adds 1 and, when the call creates the key, sets its deadline.
    /// An existing key keeps the deadline it has. Returns the new value.
    ///
    /// # Errors
    /// [`Error::Protocol`] for a zero or too large `ttl`; [`Error::Server`] as [`Client::incr`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // A per-minute counter: the first hit starts the minute, later hits keep it.
    /// let hits = client.incr_with_ttl("hits:ada", Duration::from_secs(60))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn incr_with_ttl(&mut self, key: &str, ttl: Duration) -> Result<i64, Error> {
        let (word, count) = ttl_words(ttl)?;

        self.integer(["INCR", key, word, count.as_str()])
    }

    /// Sends `DECR key EX|PX ttl`: as [`Client::incr_with_ttl`] but subtracts 1.
    pub fn decr_with_ttl(&mut self, key: &str, ttl: Duration) -> Result<i64, Error> {
        let (word, count) = ttl_words(ttl)?;

        self.integer(["DECR", key, word, count.as_str()])
    }

    /// Sends `INCRBY key delta EX|PX ttl`: as [`Client::incr_with_ttl`] with an amount.
    pub fn incr_by_with_ttl(&mut self, key: &str, delta: i64, ttl: Duration) -> Result<i64, Error> {
        let (word, count) = ttl_words(ttl)?;
        let delta = Num::int(delta);

        self.integer(["INCRBY", key, delta.as_str(), word, count.as_str()])
    }

    /// Sends `DECRBY key delta EX|PX ttl`: as [`Client::incr_by_with_ttl`] but subtracts.
    pub fn decr_by_with_ttl(&mut self, key: &str, delta: i64, ttl: Duration) -> Result<i64, Error> {
        let (word, count) = ttl_words(ttl)?;
        let delta = Num::int(delta);

        self.integer(["DECRBY", key, delta.as_str(), word, count.as_str()])
    }

    /// Sends `INCRBY key delta EX|PX ttl MAX limit`: a capped counter whose deadline is set when
    /// the call creates the key.
    ///
    /// # Returns
    /// `Some(new_value)`; `None` when the result would pass `limit`, in which case nothing is
    /// written (and no key is created).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let taken = client.incr_by_max_with_ttl("seats:taken", 1, 100, Duration::from_secs(3600))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn incr_by_max_with_ttl(
        &mut self,
        key: &str,
        delta: i64,
        limit: i64,
        ttl: Duration,
    ) -> Result<Option<i64>, Error> {
        let (word, count) = ttl_words(ttl)?;
        let (delta, limit) = (Num::int(delta), Num::int(limit));
        let value = self.run([
            "INCRBY",
            key,
            delta.as_str(),
            word,
            count.as_str(),
            "MAX",
            limit.as_str(),
        ])?;

        if value.is_null() {
            return Ok(None);
        }

        value
            .as_integer()
            .map(Some)
            .ok_or_else(|| unexpected("INCRBY", &value))
    }

    /// Sends `INCRBYFLOAT key delta EX|PX ttl`: as [`Client::incr_by_float`], setting the
    /// deadline when the call creates the key.
    pub fn incr_by_float_with_ttl(
        &mut self,
        key: &str,
        delta: f64,
        ttl: Duration,
    ) -> Result<f64, Error> {
        let (word, count) = ttl_words(ttl)?;
        let delta = Num::float(delta);

        parse_score(&self.text(["INCRBYFLOAT", key, delta.as_str(), word, count.as_str()])?)
    }

    /// Sends `HINCRBY key field delta EX|PX ttl`: as [`Client::hincr_by`]; the deadline (of the
    /// whole hash key) is set when the call creates the key.
    pub fn hincr_by_with_ttl(
        &mut self,
        key: &str,
        field: &str,
        delta: i64,
        ttl: Duration,
    ) -> Result<i64, Error> {
        let (word, count) = ttl_words(ttl)?;
        let delta = Num::int(delta);

        self.integer(["HINCRBY", key, field, delta.as_str(), word, count.as_str()])
    }

    /// Sends `ZINCRBY key delta member EX|PX ttl`: as [`Client::zincr_by`], setting the deadline
    /// when the call creates the key.
    pub fn zincr_by_with_ttl(
        &mut self,
        key: &str,
        member: &str,
        delta: f64,
        ttl: Duration,
    ) -> Result<f64, Error> {
        let (word, count) = ttl_words(ttl)?;
        let delta = Num::float(delta);

        parse_score(&self.text(["ZINCRBY", key, delta.as_str(), member, word, count.as_str()])?)
    }

    /// Sends `SETBIT key offset bit EX|PX ttl`: as [`Client::setbit`], setting the deadline when
    /// the call creates the key. Returns the previous bit.
    pub fn setbit_with_ttl(
        &mut self,
        key: &str,
        offset: u64,
        value: bool,
        ttl: Duration,
    ) -> Result<bool, Error> {
        let (word, count) = ttl_words(ttl)?;
        let offset = Num::uint(offset);

        self.flag([
            "SETBIT",
            key,
            offset.as_str(),
            if value { "1" } else { "0" },
            word,
            count.as_str(),
        ])
    }

    /// Sends `XADD key EX|PX ttl id field value ...`: as [`Client::xadd`], setting the stream's
    /// deadline when the call creates it.
    pub fn xadd_with_ttl(
        &mut self,
        key: &str,
        ttl: Duration,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        let (word, count) = ttl_words(ttl)?;
        let mut arguments = Vec::with_capacity(5 + fields.len() * 2);

        arguments.extend(["XADD", key, word, count.as_str(), id]);

        for (field, value) in fields {
            arguments.extend([*field, *value]);
        }

        self.text(arguments)
    }

    /// Sends `XADD key EX|PX ttl MAXLEN|MINID ... id field value ...`: as
    /// [`Client::xadd_trim`] with a deadline set when the call creates the stream.
    pub fn xadd_trim_with_ttl(
        &mut self,
        key: &str,
        ttl: Duration,
        trim: &crate::keywords::StreamTrim<'_>,
        id: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, Error> {
        let (word, count) = ttl_words(ttl)?;
        let mut arguments: Vec<String> = trim.with_words(|strategy, mode, threshold| {
            vec![
                "XADD".to_owned(),
                key.to_owned(),
                word.to_owned(),
                count.as_str().to_owned(),
                strategy.to_owned(),
                mode.to_owned(),
                threshold.to_owned(),
                id.to_owned(),
            ]
        });

        for (field, value) in fields {
            arguments.push((*field).to_owned());
            arguments.push((*value).to_owned());
        }

        self.text(arguments)
    }

    /// Sends `JSON.SET key path json [NX|XX] EX|PX ttl`: as [`Client::json_set_with`], setting
    /// the key's deadline when the call creates it. Returns `false` when the condition stopped
    /// the write.
    pub fn json_set_with_ttl(
        &mut self,
        key: &str,
        path: &str,
        json: &str,
        condition: crate::models::JsonSetCondition,
        ttl: Duration,
    ) -> Result<bool, Error> {
        use crate::models::JsonSetCondition;

        let (word, count) = ttl_words(ttl)?;
        let mut arguments = vec!["JSON.SET", key, path, json];

        match condition {
            JsonSetCondition::Always => {}
            JsonSetCondition::IfMissing => arguments.push("NX"),
            JsonSetCondition::IfExists => arguments.push("XX"),
        }

        arguments.extend([word, count.as_str()]);

        Ok(!self.run(arguments)?.is_null())
    }
}
