//! Typed `JSON.*` helpers (feature `serde`): pass any `Serialize` value instead of JSON text.
//!
//! Serializer settings live on the value's serde attributes (`rename_all`, `skip_serializing_if`, ...).
//! To use a different serializer entirely, serialize yourself and call the raw `json_set`,
//! `json_merge` and `json_arrappend`, which take JSON text.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::client::Client;
use crate::error::Error;

fn to_json<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value)
        .map_err(|error| Error::Protocol(format!("JSON serialize failed: {error}")))
}

impl Client {
    /// Serializes `value` with serde and sets it at `path` (`JSON.SET`). Requires the `serde`
    /// feature.
    ///
    /// Strings are sent as JSON strings with quotes (`&"izmir"` becomes `"izmir"`). Returns
    /// `false` when the path cannot be reached. Serializer options come from the type's serde
    /// attributes (`rename_all`, `skip_serializing_if`, ...).
    ///
    /// # Errors
    /// [`Error::Protocol`] if serialization fails; [`Error::Server`] as for [`Client::json_set`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// use serde::Serialize;
    /// #[derive(Serialize)]
    /// struct User { name: String, age: u8 }
    /// client.json_set_value("user:1", "$", &User { name: "Ada".into(), age: 36 })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_set_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
    ) -> Result<bool, Error> {
        self.json_set(key, path, &to_json(value)?)
    }

    /// Serializes `patch` and sends it as a `JSON.MERGE` (RFC 7396) patch; `true` when applied.
    /// Members set to `null` in the patch are deleted. Requires `serde`.
    pub fn json_merge_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        patch: &T,
    ) -> Result<bool, Error> {
        self.json_merge(key, path, &to_json(patch)?)
    }

    /// Serializes `value` and appends it to the array at `path` (`JSON.ARRAPPEND`). Returns the
    /// new length, or `None` when the key or path is absent. Requires `serde`.
    pub fn json_arrappend_value<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
    ) -> Result<Option<i64>, Error> {
        self.json_arrappend(key, path, &to_json(value)?)
    }

    /// Reads the JSON at `path` (`JSON.GET`) and deserializes it into `T`.
    ///
    /// Returns `None` when the key or path is absent. Requires `serde`.
    ///
    /// # Errors
    /// [`Error::Protocol`] when the stored JSON does not fit `T`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// use serde::Deserialize;
    /// #[derive(Deserialize)]
    /// struct User { name: String, age: u8 }
    /// let user: Option<User> = client.json_get_value("user:1", "$")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_get_value<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
    ) -> Result<Option<T>, Error> {
        match self.json_get(key, path)? {
            None => Ok(None),
            Some(text) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| Error::Protocol(format!("JSON deserialize failed: {error}"))),
        }
    }
}

impl Client {
    /// Pops an array element (`JSON.ARRPOP`, default the last) and deserializes it into `T`.
    /// `None` for an empty array or missing key. Requires `serde`.
    pub fn json_arrpop_value<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
        index: Option<i64>,
    ) -> Result<Option<T>, Error> {
        match self.json_arrpop(key, path, index)? {
            None => Ok(None),
            Some(text) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| Error::Protocol(format!("JSON deserialize failed: {error}"))),
        }
    }

    /// Serializes `value` and sets it only when `condition` holds (`NX` / `XX`, see
    /// [`JsonSetCondition`](crate::JsonSetCondition)). Returns `false` when it was not written.
    /// Requires `serde`.
    pub fn json_set_value_with<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
        condition: crate::models::JsonSetCondition,
    ) -> Result<bool, Error> {
        self.json_set_with(key, path, &to_json(value)?, condition)
    }

    /// Serializes `value` and sends `JSON.SET ... [NX|XX] EX|PX`, as
    /// [`Client::json_set_with_ttl`]: the deadline is set when the call creates the key. Returns
    /// `false` when it was not written. Requires `serde`.
    pub fn json_set_value_with_ttl<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
        condition: crate::models::JsonSetCondition,
        ttl: std::time::Duration,
    ) -> Result<bool, Error> {
        self.json_set_with_ttl(key, path, &to_json(value)?, condition, ttl)
    }

    /// `JSON.FIND` for a typed value.
    ///
    /// Serializes `value` the way the index stores it (a string is sent without quotes, a number
    /// or bool as its JSON text) and returns the matching keys in ascending order. `count` and
    /// `after` page the result as in [`Client::json_find`]. Requires `serde`.
    pub fn json_find_value<T: Serialize + ?Sized>(
        &mut self,
        index: &str,
        value: &T,
        count: Option<u64>,
        after: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let json = serde_json::to_value(value)
            .map_err(|error| Error::Protocol(format!("JSON serialize failed: {error}")))?;
        let text = match json {
            serde_json::Value::String(text) => text,
            other => other.to_string(),
        };

        self.json_find(index, &text, count, after)
    }
}
