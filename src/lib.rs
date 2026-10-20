#![deny(unsafe_code)]

//! Shared primitives for the `ruvio` crate.
//!
//! The project intentionally keeps the first source file small so the examples can
//! focus on the architectural ideas: shared-nothing execution, cache-aware memory
//! layout, and zero-copy wire formats.

#[allow(unsafe_code)]
mod hashbrown;

mod acl;
mod admission;
pub(crate) mod arena;
pub mod changes;
mod chunked;
pub mod config;
#[cfg(feature = "coordinator")]
pub mod coordinator;
mod database;
mod dump;
pub mod engine;
mod geo;
mod hot_store;
mod id_map;
pub mod import;
pub mod index;
mod json;
#[doc(hidden)]
pub use json::fuzz_ops as json_fuzz_ops;
mod keystats;
mod latency;
pub(crate) mod lazy_free;
mod list_waiters;
mod log;
mod lua_library;
mod lua_pattern;
pub mod memory;
pub mod metrics;
pub mod mutation;
mod notices;
mod notify;
mod output;
pub mod persistence;
mod primary_lease;
pub mod protocol;
mod pubsub;
mod replication;
pub mod routing;
pub mod runtime;
mod security;
pub mod shutdown;
mod slot_moves;
mod slots;
mod slowlog;
mod store;
mod stream;
#[cfg(test)]
mod test_ports;
pub mod time;
mod tls;
pub mod topology;
mod transaction;

mod script;
mod tracking;

pub use config::{
    AppConfig, ConfigOverrides, ConnectionLimits, ConnectionTimeouts, EvictionPolicy, MemoryLimits,
    RequestLimits, ScriptLimits, SecurityConfig,
};

#[doc(hidden)]
pub use index::INDEX_SLOT_SIZE;
pub use index::{AHashIndex, BTreeIndex, StorageIndex};

pub use memory::{MemoryDelta, MemoryUsage};

pub use mutation::OwnedMutation;
pub use persistence::{DurabilityProfile, PersistenceConfig, PersistenceError};

pub use security::bind_is_loopback;
pub use shutdown::ShutdownToken;
#[doc(hidden)]
pub use store::BYTE_BODY_SIZE;
pub use store::{
    BloomError, BytesValue, Entry, ExpirationStats, IncrementError, KvShard, MemoryStats,
    SortedSetError, Value, WrongType,
};

pub use time::{Clock, SystemClock, UnixMillis};

pub use log::init_process_logging;
pub use notices::THIRD_PARTY_NOTICES;
pub use tls::TlsAcceptor;

/// A conservative cache line size used by the examples and storage layout helpers.
pub const CACHE_LINE_BYTES: usize = 64;

/// A simple wrapper that keeps values aligned to a cache line.
#[repr(align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheAligned<T>(pub T);

impl<T> CacheAligned<T> {
    /// Wrap a value in cache-line-aligned storage.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Consume the wrapper and return the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Round a size up to the next cache line boundary, or return `None` on overflow.
pub const fn align_up_to_cache_line(bytes: usize) -> Option<usize> {
    match bytes.checked_add(CACHE_LINE_BYTES - 1) {
        Some(rounded) => Some(rounded & !(CACHE_LINE_BYTES - 1)),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_aligned_values_start_on_a_cache_line() {
        let value = CacheAligned::new([0u8; CACHE_LINE_BYTES]);

        assert_eq!((&value as *const _ as usize) % CACHE_LINE_BYTES, 0);
    }

    #[test]
    fn rounds_sizes_to_cache_line_boundaries() {
        assert_eq!(align_up_to_cache_line(0), Some(0));
        assert_eq!(align_up_to_cache_line(1), Some(64));
        assert_eq!(align_up_to_cache_line(64), Some(64));
        assert_eq!(align_up_to_cache_line(65), Some(128));
        assert_eq!(align_up_to_cache_line(usize::MAX), None);
    }
}
