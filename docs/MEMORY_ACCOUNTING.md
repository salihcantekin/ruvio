# Memory Accounting

Ruvio maintains deterministic memory charges inside each shard. The charges
drive admission control, `maxmemory` and eviction. They are not an RSS
estimator: allocator metadata, page fragmentation, runtime buffers, and
platform-specific collection internals can make process RSS different.

## What `maxmemory` means

`maxmemory` (`memory.max_process_bytes`) is a budget for the **data the shards
hold**, not a cap on the process. Ruvio divides it evenly across the shards
(`max_shard_bytes` sets one shard's budget directly). A write that would push a
shard past its share is refused with `OOM` (policy `noeviction`) or makes room
by evicting keys (any other `maxmemory-policy`).

What counts against the budget (`charged_memory_bytes` in `INFO`): keys, values,
collection storage, the key index, the pooled value buffers and, while a
checkpoint runs, the snapshot journal.

What does **not** count: client connection buffers, unsent replies, Pub/Sub
queues, client-tracking tables, the Lua heap, WAL bytes not yet synced
(`wal_unsynced_bytes`), allocator overhead and the process's own baseline. The
process can therefore use more memory than `maxmemory`. Leave room: set
`maxmemory` well below the memory the container or host gives Ruvio (a common
starting point is 60 to 70 percent), then under real load compare the
process memory your platform reports (RSS, or the container's usage) with
`charged_memory_bytes`. The server does not read cgroup limits. Buffers are
bounded separately: unsent replies by `max_output_buffer_bytes` per connection
and the number of connections by `max_global` and `max_per_ip`; see
`docs/CONFIGURATION.md`.

## Categories

- `key_bytes`: bytes in shard-owned key allocations.
- `value_bytes`: byte-value capacity and collection element payload
  allocations. Sorted-set members are charged for both owned indexes because
  the current representation stores two copies.
- `collection_bytes`: capacities and estimated node/index storage owned by
  lists, sets, hashes, and sorted sets. Compact hashes charge vector capacity;
  promoted hashes charge table capacity.
- `index_bytes`: the primary key index capacity, including its inline key and
  entry slots. The default hash index charges every allocated bucket, so
  delete tombstones do not lower the charge. With a memory limit, a full table
  that still has at least 1/8 free capacity rehashes tombstones in place
  instead of doubling.
- `pooled_bytes`: cleared byte buffers retained by the bounded shard-local
  value pool.
- `entries`: physical entries currently retained by the shard.

`charged_bytes` is the saturating sum of all byte categories.

## Mutation Deltas

`KvShard::memory_usage()` is an O(1) snapshot. Subtract two snapshots with
`MemoryUsage::delta_from` to obtain a signed `MemoryDelta` for one command or
batch:

```rust
let before = shard.memory_usage();
shard.set_bytes(b"key", b"value");
let delta = shard.memory_usage().delta_from(before);
```

The shard reconciles the affected entry after every mutation. It does not scan
the whole keyspace. Index capacity and pool retention are refreshed from their
owners after each reconciliation.

Deleting a byte value may transfer bytes from `value_bytes` to `pooled_bytes`
instead of releasing them. Reusing that buffer performs the inverse transfer.
This is intentional: both states consume the shard's memory budget.

## Expiration

Passive expiration makes a key logically absent, but its allocation remains
charged until a mutation or active expiration (see below) physically removes
it. This prevents memory limits from ignoring expired-but-retained
allocations.

## Limits

Configured `max_shard_bytes` minus `reserved_headroom_bytes` is the admission
budget. The snapshot journal is included in `charged_bytes`. The connection and
output fields of the charge exist but are not filled in by the running server
(they stay zero), and WAL dirty bytes are reported, not charged (see *What
`maxmemory` means*). The default policy is `noeviction`: writes that would
exceed the budget return `OOM command not allowed when used memory > 'maxmemory'.`
and do not record a mutation.

Commands that can grow memory are refused; Ruvio refuses a few more than Redis
does (`SMOVE`, which can create or convert its destination set, the `HEXPIRE`
family, `GETEX`, `JSON.CLEAR`/`JSON.ARRPOP`/`JSON.ARRTRIM`, `XCLAIM`,
`XAUTOCLAIM` and `XREADGROUP`). Commands that only remove or move data, such as
`DEL`, `UNLINK`, `GETDEL`, `HDEL`, `SREM`, `ZREM`, `LPOP`/`RPOP`, `LTRIM`,
`XDEL`, `XTRIM`, `RENAME` and `PERSIST`, still run on a full store, directly,
inside `MULTI` and from scripts, so deleting keys frees memory. Commands that
set a TTL stay refused, because the first deadline allocates the expiry index,
except `EXPIRE` with a deadline already past, which deletes the key. While a
checkpoint is running, a deleted or changed key keeps its old value in the
snapshot journal until the checkpoint ends, so `charged_bytes` and process RSS
can stay at or above the limit until then.

## Peak memory: plan for up to twice the data

Three moments can hold a second copy of data, so size the machine (or `maxmemory`) for them:

- **A checkpoint under heavy writes.** The snapshot journal keeps the old value of each key
  written during the walk. It starts small and grows only after a walk aborts, to a quarter of
  the shard's data; a walk that keeps aborting under overload may grow it up to a full copy of
  the data. With `maxmemory` set it also stays within the memory left under the limit. Without
  `maxmemory`, nothing but the data size bounds it.
- **A full sync.** The primary builds, and the replica receives, the whole snapshot as one
  buffer (at most `snapshot_max_bytes`, 16 GiB by default) next to the live store, so each side
  can briefly hold the data twice. Lower `snapshot_max_bytes` to the largest snapshot you
  expect to refuse a runaway one early.
- **`FLUSHALL`.** Every shard hands its data to the free thread at once, whatever is queued, so
  that a flush stays constant time. The memory returns as the free thread works through it.

Set `maxmemory` below about half of the machine's memory when you replicate or checkpoint a
store that takes heavy writes.

`maxmemory-policy` may be `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, or
`volatile-ttl`. Eviction is shard-local. The default examines 16 candidates
(`maxmemory-samples`) per decision. A write that needs room evicts for up to
2 ms. With an all-keys policy (`allkeys-lru`, `allkeys-lfu`), if the time runs out
while the shard is at most a quarter of its limit over, the write is admitted
anyway and the remaining debt is paid off by maintenance between requests; past
that quarter, or with a `volatile-*` policy, the write is refused with `OOM`.
Active expiry reaps `active-expire-keys` (default 16) per command or idle tick. After a
delete or a much smaller overwrite, the key index, set, hash, sorted-set
member map, list ring, compact hash vector, and string buffer shrink once
they are under a quarter full. The hot-key map uses the same rule. Victims are persisted as WAL `Delete` records. Evicted byte values are
discarded rather than returned to the shard pool so `charged_bytes` can fall.
Recency and frequency are not written to snapshots; recovered keys start cold.
A single write larger than the shard budget is rejected without evicting.

`allocation_events` / `allocation_bytes` count mutations that increased
`charged_bytes`. Pool retention is `pooled_bytes`.

`max_process_bytes` is validated against the per-shard budget at startup, and
again by `CONFIG SET maxmemory`. Zero disables a budget. The memory limits and
`maxmemory-policy` change while the server runs (`CONFIG SET maxmemory`,
`max-shard-bytes`, `memory-headroom-bytes`, `maxmemory-policy`,
`maxmemory-samples`); a lower limit is reached by eviction as writes arrive,
and a policy change resets every key's access bits once per shard.

## Active Expiration

A bounded active-expiration cycle removes a small number of already-due keys
before each command (`active_expire_keys`) and on the maintenance tick
(`active_expire_maintenance_keys`). Passive expiration still treats
due keys as absent on lookup. Physical charges remain until the entry is
removed.
