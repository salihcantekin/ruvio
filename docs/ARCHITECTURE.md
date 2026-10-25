[Ruvio](../README.md) / [Docs](README.md) / Architecture

# Ruvio Architecture

Ruvio is structured as a shared-nothing in-memory key-value store. The design goal is to keep hot data local to a single execution context and avoid the cache-line bouncing, mutex contention, and scheduler churn that appear in traditional multi-threaded designs.

## Design Philosophy

The store is meant to run as a set of isolated shards, each owned by one executor. Instead of protecting a single global map with locks, each shard owns its own memory and request queue. That keeps writes local, reduces synchronization points, and makes latency more predictable under load.

This is the same core idea used by many high-performance databases and networking systems: if one core owns a shard, it can make most decisions without cross-core coordination. It can improve tail latency and hardware utilization when routing and load distribution are well designed, but it does not remove coordination for cross-shard operations.

## Shared-Nothing Execution

The storage, protocol, and routing layers remain independent of asynchronous runtime and operating-system APIs. The process runs one Tokio `current_thread` evented runtime per shard, with epoll on Linux, kqueue on macOS, and IOCP on Windows. On Linux, owner threads can be pinned to cores. This model can provide three mechanical advantages:

1. The working set stays warm in the L1/L2 caches of that core.
2. Requests do not bounce between worker threads.
3. The system can scale horizontally by adding more shards instead of adding more locking.

The tradeoff is that ownership matters. Data must be routed to the right shard up front, but the gain is that once the request arrives, the shard can operate without locks in its hot path.

Redis-compatible key hashing selects a slot and its static shard owner. Online rebalancing and cross-slot transactions are not implemented. A slow shard can delay other requests on that shard; a fast shard is not guaranteed to retain its unloaded throughput under skew.

Linux, macOS, and Windows run the same evented runtime. The command engine does not depend on epoll, kqueue, or IOCP. See [PORTABILITY](PORTABILITY.md).

Multi-shard mode divides the Redis-compatible 16,384 hash slots across shard owners. Each executor listens on a consecutive port and owns its shard exclusively. A request arriving at a shard that does not own its key is forwarded to the owner (raw frame in, reply bytes back, data still touched by one thread), while multi-key requests spanning slots receive `CROSSSLOT`; `MOVED` is only sent for a sharded Pub/Sub channel, since a subscription cannot be forwarded. Ownership is read from a slot table (`SlotTable` in `src/routing.rs`, one atomic entry per slot, shared by every shard) that is filled from `slot * shards / 16384` at start, so a data directory keeps its layout, and then takes the ranges recorded in the data directory's `SLOTS` file. `RUVIO MOVESLOTS` changes entries while the server runs: the source shard copies a range to the target in bounded steps (keys written meanwhile are sent again), then hands over in one step of its thread, in which the target applies the last changes, syncs its log, records the move in `SLOTS` and points the table at itself. A command routed to the old owner before the hand-over is forwarded again, or answered `TRYAGAIN`, where it runs. Recovery keeps, per slot, only what a shard held while it owned the slot, so a crash at any step leaves each key on exactly one shard. `CLUSTER SLOTS` / `SHARDS` / `NODES` / `INFO` / `MYID` / `KEYSLOT` describe that map, merging consecutive slots of one shard into ranges; each range also lists the replicas of that shard that announced a client address in the replication handshake, and a replica lists its primary first. Official clients take one seed address, route by slot, and can send read-only commands to replicas with a read policy ([CLIENT_BEHAVIOR](CLIENT_BEHAVIOR.md#reads-from-replicas)). There is no cluster bus. Slots move between machines with `COORDINATOR MOVE` and `COORDINATOR REBALANCE` ([COORDINATOR](COORDINATOR.md)). Failover of a primary and its replicas is done from outside the data path by `ruvio coordinator` processes, which agree through Raft and act with `REPLICAOF` ([COORDINATOR](COORDINATOR.md)).

## Network I/O

The process uses Tokio on the shard owner. Linux uses epoll, macOS uses kqueue, and Windows uses IOCP. Parse, apply, and write stay on that owner. The storage core does not take a socket type.

## Cache Alignment and SIMD

The store should keep selected hot metadata cache-aligned when measurement justifies it. Alignment can prevent a field from straddling cache lines and can isolate independently written data, but it does not inherently reduce cache misses. Over-alignment can waste memory and reduce cache density.

SIMD is useful for scanning memory quickly when the store needs to:

- search a block for a key prefix,
- compare a run of bytes against a query byte,
- accelerate probing in dense in-memory structures.

The `examples/02_simd_search.rs` file demonstrates the core pattern: compare multiple bytes at once with SSE2 or NEON and fall back to scalar logic for the tail. Production code should benchmark against established primitives such as `memchr`, which already provide architecture-specific runtime dispatch and tuned implementations.

## Zero-Copy Networking

Zero-copy in this project means zero-copy parsing: a command's arguments are spans into the connection's input buffer (`Args` in `src/protocol/table.rs`), so a command whose arguments are single values is parsed without a heap allocation; `tests/zero_alloc.rs` holds that for the hot commands. It does not claim that the kernel-to-userspace network transfer is copy-free.

That is a mechanical advantage because it removes:

- heap churn,
- extra memcpy work,
- unnecessary cache pollution from transient objects.

`examples/03_zero_copy.rs` shows the same idea for a binary header: network byte order and alignment-1 integer wrappers, so the header can be read at any offset. In both cases lengths are validated before slicing, frames are bounded, and a buffer is not reused while a parsed view still borrows it.

## Current Storage Core

`KvShard` (`src/store/`) is the single-owner shard. It has no locks. Its key index, `AHashIndex` in `src/index.rs`, keeps each key once and stores almost nothing per key besides it:

- **Key log.** Every key is a record in a paged byte log (`ValueLog`): a 4-byte header, the key bytes, and a small payload. The payload is the value itself when it is an integer, an offset and length into the shard's value log for a string of at most 64 bytes, or the id of a heap string, an object, or a slab slot.
- **Slot table.** A trimmed hashbrown `HashTable` (`src/slots.rs`, `src/hashbrown/`) holds a 4-byte key-log address per key, hashed with a randomized AHash seed. It grows incrementally, so one write never rehashes the whole table.
- **Values.** Strings of at most 64 bytes live in the shard's value log; longer strings are heap buffers, recycled through a bounded size-class pool up to 64 KiB. Small sets, sorted sets, lists and hashes are packed into slab pages by size class; larger collections become objects (`Entry`, `src/store/value.rs`). Log pages whose live bytes reach zero are returned to the allocator, and sparse pages are compacted during maintenance.
- **Side tables.** TTL deadlines and eviction recency live outside the key record. A key with no deadline pays nothing for TTL, and the recency cells stay unallocated until an eviction policy needs them.

`tests/layout.rs` pins the sizes this layout depends on. The memory per structure, measured against Redis, is in the [measurement records](evidence/README.md).

Expiration metadata is stored as absolute Unix milliseconds rather than
process-local `Instant` values, making it suitable for snapshots and restart.
`KvShard` is generic over a clock without dynamic dispatch; production uses the
system wall clock and tests inject deterministic time. Restart loads `CLOCK`
and the last checkpoint timestamp and never reports a time below that floor.
The clock is consulted
only for entries that actually have an expiration deadline.

Each shard also maintains O(1) memory-usage snapshots. Key allocations, value
payloads, collection capacity, primary-index capacity, and retained pool
buffers are charged separately. Signed deltas between snapshots describe a
mutation without scanning the keyspace. The charge model is deterministic for
limits and eviction decisions; it is deliberately not presented as process
RSS. See [MEMORY_ACCOUNTING](MEMORY_ACCOUNTING.md).

The key index is a compile-time `StorageIndex` parameter. Production
uses `AHashIndex`; an ordered `BTreeIndex` provides a conformance baseline.
The index trait is deliberately incompatible with trait objects, so alternate
layouts do not add virtual dispatch to shard operations. Storage semantics,
expiration, memory accounting, and persistence remain outside the index. See
[STORAGE_LAYOUT](STORAGE_LAYOUT.md).

The server binary uses mimalloc by default. The `mimalloc-allocator` feature can be disabled to compare against the system allocator. Allocator choices must be retained only when repeated workload-specific measurements justify them.

## Current Command Server

The `ruvio` binary runs the evented runtime only. Each shard owner parses requests from its sockets, applies commands and encodes responses on its Tokio `current_thread` executor; borrowed reads stay on the owner until encoded. Hot and fallback RESP parsers borrow from bounded connection input buffers. The server validates protocol and connection limits before opening listeners, enforces connection timeouts, and bounds global and per-source-IP admission. Shard owners share the admission tracker on accept/disconnect, not for each command.

WAL-backed `primary` and `replica` roles provide asynchronous per-shard WAL tail replication. A replica can install a usable checkpoint after a compacted WAL gap and then catch up from the tail. The writer does not wait for replicas unless a client explicitly uses `WAIT`; `WAIT` does not roll back a write on timeout. A replica serves reads; once it has finished copying, its primary lists it in `CLUSTER SLOTS`, `ROLE` and `INFO replication`, so cluster clients can read from it (asynchronously, so a read may miss recent writes). No cluster bus is provided; automatic failover and slot moves between machines are `ruvio coordinator`, and `RUVIO MOVESLOTS` moves slots between the shards of one process (refused while replication is on). See [RECOVERY](RECOVERY.md) and [COMPATIBILITY](COMPATIBILITY.md).

One structure is shared between shards on purpose: string keys matching a configured `hot_keys` glob live in one process-wide striped map (`src/hot_store/`), so every shard serves them without `MOVED`; only `GET`, `SET` and `DEL` are allowed on them ([CONFIGURATION](CONFIGURATION.md)). Everything else is owned by exactly one shard.

## Production Gaps

This is not a certified production cluster. Before making stronger availability or performance claims it needs:

- measured overload isolation and tail-latency budgets under saturation,
- production-like fault, disk and recovery drills,
- defined membership, migration, election and split-brain behavior,
- hardware and long-duration durability evidence.

Report at least p50, p95, p99, and p99.9 latency together with throughput, CPU utilization, allocation rate, key/value distribution, connection count, compiler version, kernel version, and CPU model. Compare against a scalar or conventional threaded baseline.

## Invariants

The module map is in [DEVELOPING](DEVELOPING.md#server-module-map). Whatever the module, these hold:

- one shard owns its memory, and one executor owns one shard;
- cross-shard work is a message to the owner, never shared state (the `hot_keys` store above is the one exception);
- a command is parsed from borrowed spans and, on the hot paths, allocates nothing (`tests/zero_alloc.rs`);
- a write reaches the WAL in the shard's mutation order before its reply, as the durability profile requires.
