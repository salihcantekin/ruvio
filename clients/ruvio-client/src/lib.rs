//! Blocking RESP2 client for [Ruvio](https://ruviodb.com/), a Redis-compatible key-value server.
//!
//! One [`Client`] type covers everything: strings, lists, sets, hashes, sorted sets, streams,
//! JSON, Bloom filters, geo, bitmaps, HyperLogLog, scripting, Pub/Sub and Ruvio's own
//! primitives (`LIMIT`, `ONCE`, `LEASE`, `SEMAPHORE`, `IDEM`, `SETV`, `TAKE` ...). Every
//! method is synchronous, takes `&mut self`, and sends exactly one command (or one
//! pipeline). Nothing here needs an async runtime.
//!
//! # Getting started
//!
//! ```no_run
//! use ruvio_client::{Client, Error};
//!
//! fn main() -> Result<(), Error> {
//!     let mut client = Client::connect("127.0.0.1", 6379)?;
//!
//!     client.set("greeting", "hello")?;
//!     assert_eq!(client.get_string("greeting")?.as_deref(), Some("hello"));
//!     assert_eq!(client.incr("visits")?, 1);
//!     Ok(())
//! }
//! ```
//!
//! Use [`ClientOptions`] with [`Client::connect_with`] for a password, a logical database,
//! timeouts and reconnect behaviour.
//!
//! # Return conventions
//!
//! * A missing key or field is `None` for single values (`get`, `hget`, `lpop`), an empty
//!   `Vec` for collections (`lrange`, `smembers`), `0` for counts (`llen`, `scard`) and
//!   `false` for existence checks (`sismember`). Each method says which one it uses.
//! * Booleans mean "the command did something": `set` returns `false` when `NX`/`XX`
//!   skipped the write, `expire` returns `false` when the key does not exist.
//! * Integers keep the server's meaning, including `-1`/`-2` from `ttl`.
//!
//! # Errors
//!
//! Every fallible method returns [`Error`]. [`Error::Server`] holds a server error reply
//! such as `WRONGTYPE Operation against a key holding the wrong kind of value`; the
//! connection stays usable. [`Error::Io`] covers connect failures, resets and read
//! timeouts. [`Error::Protocol`] means the bytes were not valid RESP2 or an argument was
//! rejected by the client before sending.
//!
//! # Sharded servers
//!
//! By default [`Client::connect`] sends `CLUSTER SLOTS`. A Ruvio server with `shards > 1`
//! answers with its shard map, and the client then opens one socket per shard and sends
//! each command to the shard that owns the key's hash slot (see [`hash_slot`]). A
//! single-shard server answers as a standalone server and no routing happens.
//!
//! Things that differ from a single Redis server:
//!
//! * With `port_mode = "per-shard"` (the default) each shard listens on its own port. A
//!   connection that was opened to one port without cluster discovery
//!   (`discover_cluster: false`) only sees that shard's keys for `dbsize`, `scan`, `keys`
//!   and `flushdb`/`flushall`. The routing client fans `DBSIZE`, `FLUSHDB`, `FLUSHALL`
//!   and `SCAN` out to every shard itself.
//! * The server forwards a key sent to the wrong shard instead of answering `MOVED`, so a
//!   non-routing client still works (with one extra hop).
//! * `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` over keys in several slots are
//!   split per slot and the replies merged. The split is **not atomic**: some slots may be
//!   written while another fails. Other multi-key commands over several slots fail with
//!   `CROSSSLOT` (as [`Error::Server`]); put related keys in one slot with a hash tag such
//!   as `{user:1}:cart`.
//! * A transaction (`MULTI`/`EXEC`) must stay in one slot.
//! * Logical databases (`SELECT`) exist only on a single-shard server; a routed client
//!   rejects a non-zero database.
//!
//! # Connection handling
//!
//! The client does not send `CLIENT SETINFO` or `HELLO`. A dropped socket is reopened
//! before the next command (see [`ClientOptions::reconnect`]), but the command that found
//! the drop still fails and is never re-sent, so a write is never applied twice by the
//! client. Subscribe commands open a second socket automatically.
//!
//! # Features
//!
//! * `serde` (optional): typed JSON helpers such as `Client::json_set_value` and
//!   `Client::json_get_value` that accept and return any serde type.

mod args;
mod blocking;
mod client;
mod cluster;
mod codec;
mod command_keys;
mod commands;
mod error;
mod extra;
mod helpers;
#[cfg(feature = "serde")]
mod json_value;
mod keywords;
mod models;
mod scan;
mod sha1;
#[cfg(test)]
mod test_support;
mod ttl;
mod value;

pub use client::{Client, ClientOptions, ConnectionNotice, Durability, ReadPolicy, SetMode};

pub use cluster::{hash_slot, hash_slot_bytes};

pub use error::Error;
pub use helpers::{CacheOptions, IdemOutcome, LeaseGuard, LockGuard, SemaphorePermit, new_token};
pub use keywords::{
    ChangeType, ChangeTypes, ClientType, ExpireCondition, InfoSection, StreamTrim, TrimMode,
    TrimStrategy, bound, stream_id,
};
pub use models::{
    Aggregate, BitFieldOffset, BitFieldOp, BitFieldOverflow, BitFieldType, BitOperation, BitUnit,
    BloomInfo, BloomInsertOptions, BloomReserveOptions, FieldExpireCondition, FillOutcome, GeoFrom,
    GeoMatch, GeoOrder, GeoSearch, GeoShape, GeoUnit, HashScanPage, IdemState, InsertPosition,
    JsonIndexType, JsonSetCondition, KeyType, LimitOutcome, ListPositionOptions, ListSide,
    PubSubMessage, RangeBy, RangeOptions, RateLimit, RestoreOptions, ScanPage, SetScanPage,
    SortedSetAddOptions, SortedSetCombine, SortedSetEntry, SortedSetScanPage, StreamClaimOptions,
    StreamEntry, StreamPendingEntry, StreamPendingFilter, StreamReadOptions, StreamReadResult,
};
pub use scan::ScanIter;

pub use value::RespValue;

/// Codec entry points for `examples/alloc_bench.rs` and `tests/resp_vectors.rs`. Not a stable API.
#[doc(hidden)]
pub mod __bench {
    pub use crate::codec::{
        append_bytes as encode_bytes, append_strings as encode, read_value as parse,
    };
}
