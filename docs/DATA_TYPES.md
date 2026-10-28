# Data Types

Ruvio stores each key as an `Entry` containing shared metadata and a native typed `Value`. Collection operations do not serialize and deserialize an opaque byte payload.

## Supported Commands

| Type | Commands | Internal representation |
| --- | --- | --- |
| Bytes | `GET`, `GETEX`, `SET`, `SETV`, `MGET`, `MSET`, `GETSET`, `SETEX`, `PSETEX`, `SETNX`, `GETDEL`, `MSETNX`, `INCRBYFLOAT`, `GETRANGE`, `SUBSTR`, `SETRANGE`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `TAKE`, `TRANSFER`, `DELEX`, `DEL` | boxed byte slice. `INCRBY key n MAX limit` adds `n` only when the result stays at or below `limit`; a refusal writes nothing. |
| Semaphore | `SEMAPHORE`, `RELEASE` | one holder lives in the object. A second holder allocates an exact slice, 16 bytes each. The deadline index holds the latest holder deadline, so the key goes away once every holder has expired. |
| Cache fill | `FILLGET`, `FILLSET` | no new value kind: the cached value is an ordinary string. A side table, created by the first `FILLGET`, holds one small marker per key in flight (token, expiry, and for a `SWR` value its soft expiry and deadline). It is memory only and is capped at 65,536 markers per shard; when full, lapsed tokens and the marks of values past their deadline make room, and the marks of stored stale-while-revalidate values are kept. |
| List | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM`, `BLPOP`, `BRPOP`, `LINSERT`, `LSET`, `LREM`, `LPOS`, `LPUSHX`, `RPUSHX`, `LMOVE`, `RPOPLPUSH`, `BLMOVE`, `LMPOP`, `BLMPOP`, `LPUSHCAP`, `RPUSHCAP` | `VecDeque` of boxed byte slices |
| Set | `SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SINTERCARD`, `SCARD`, `SMEMBERS`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SSCAN` | hash set of boxed byte slices. Algebra is same-slot; replies are sorted. `SPOP` removes the members it returns. `SRANDMEMBER` does not. |
| Hash | `HSET`, `HMSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`, `HMGET`, `HEXISTS`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HTRANSFER`, `HRANDFIELD`, `HSETNX`, `HSTRLEN`, `HSCAN`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST` | compact field/value pairs up to 64 fields and 64-byte values; larger hashes use a table. Enumeration order is unspecified, as in Redis. `HSCAN` pages through the hash in storage order. The first field TTL moves the hash to a separate timed value that holds one deadline per timed field; `HPERSIST` of the last one moves it back, so a hash without field TTLs keeps the plain layout. |
| Sorted Set | `ZADD`, `ZCOUNT`, `ZLEXCOUNT`, `ZMSCORE`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZRANDMEMBER`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZREM`, `ZINCRBY`, `ZCARD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZPOPMIN`, `ZPOPMAX`, `ZPOPBYSCORE`, `ZMOVEDUE`, `BZPOPMIN`, `BZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZSCAN` | member hash index plus score-ordered tree. Pops and range removals read the members, then record one `ZREM`. |
| Geospatial | `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUSBYMEMBER` | one sorted set; the score is the 52-bit geohash |
| Bloom Filter | `BF.RESERVE`, `BF.ADD`, `BF.EXISTS`, `BF.MADD`, `BF.MEXISTS`, `BF.INSERT`, `BF.INFO`, `BF.CARD` | layers of packed `u64` bitsets with double hashing; each layer remembers its capacity and item count, and the filter appends a larger, tighter layer when the last one is full (unless it was reserved `NONSCALING`) |
| Stream | `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`, `XGROUP CREATE`, `XGROUP CREATECONSUMER`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS`, `XAUTOCLAIM`, `XSETID` | boxed entry list. `XADD` accepts `*`, `ms-*`, and `ms-seq`. `NOMKSTREAM` returns a null bulk and does not create a missing key. It records the assigned id and wakes a blocked `XREAD` without removing the entry. It also delivers that id to one blocked `XREADGROUP`. `NOACK` does not add a pending entry. `MAXLEN` and `MINID` accept `~` or `=` before the limit and trim exactly either way. The approximate trim option `LIMIT` is not accepted. `XACK` clears a pending id. `XPENDING` summarizes the group or lists an inclusive id range. `XCLAIM` moves an idle pending id and accepts `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`, and `LASTID`. `XSETID` moves the last generated id forward without storing an entry. `DELAY` hides the new entry from `XREADGROUP` until that many milliseconds pass, and stores the hold in the WAL and in snapshots. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` | per-connection queue; shard-local `WATCH` dirty set; one WAL batch |
| Scripts | `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`, `SCRIPT KILL`, `FCALL`, `FUNCTION LOAD`, `FUNCTION LIST`, `FUNCTION DELETE` | Lua 5.1 sandbox; declared keys; mutation batch on write. Function bodies are memory-only and are not replayed. |

Command behavior, including `WRONGTYPE`, `SET` over another type, integer
rules, and TTL on `APPEND`/`GETSET`, is in
[COMPATIBILITY.md](COMPATIBILITY.md#data-semantics). This page covers the
in-memory representation. `DEL`/`UNLINK` remove all types. `TYPE` reports
`string` (also for `SETV` values), `json`, `list`, `set`, `hash`, `zset`,
`bloomfilter`, `stream`, `lease`, `limit`, `semaphore`, `once`, or `none`;
`SCAN ... TYPE` accepts the same names.

## TTL on create

A counter, a daily set or a short-lived hash needs its deadline set once, when the key is created, and left alone by later writes. These commands take `EX seconds` or `PX milliseconds` for that:

```text
INCR key [EX s | PX ms]                     (also DECR)
INCRBY key n [EX s | PX ms] [MAX limit]     (also DECRBY without MAX, INCRBYFLOAT)
HINCRBY key field n [EX s | PX ms]
ZADD key [EX s | PX ms] [NX|XX] [GT|LT] [CH] [INCR] score member ...
ZINCRBY key n member [EX s | PX ms]
GEOADD key [EX s | PX ms] [NX|XX] [CH] lon lat member ...
XADD key [EX s | PX ms] [NOMKSTREAM] ... id field value ...
SETBIT key offset bit [EX s | PX ms]
JSON.SET key path value [NX|XX] [EX s | PX ms]
BF.INSERT key [EX s | PX ms] ... ITEMS item ...
```

The deadline is applied only when the call creates the key. A call on an existing key, with or without a deadline, leaves the deadline as it is. A call that creates nothing (`ZADD ... XX` on a missing key, an `INCRBY ... MAX` that is refused) sets no deadline. `EX 0`, a negative value and a deadline that overflows are errors. The write and the deadline are logged as one WAL record, so a crash cannot leave the key without its TTL, and replicas apply both together. Inside `MULTI` the pair joins the batch.

Variadic commands (`SADD`, `LPUSH`, `RPUSH`, `HSET`, `PFADD`, `BF.MADD`) take no such option, because a member may be called `PX`. Send `EXPIRE key seconds NX` after them in the same pipeline or `MULTI`: `NX` sets the deadline only when the key has none.

`EXPIRE`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT` accept one of `NX` (only without a deadline), `XX` (only with one), `GT` (only later than the current one) and `LT` (only earlier). A key without a deadline counts as infinite. A condition that does not hold replies 0 and changes nothing.

## Current Limits

Native `IDEM` uses the existing byte-value representation, not a new field
on every entry: 11 header bytes, the supplied fingerprint/owner, and a result only
after completion. The ordinary deadline index holds its retention. `TYPE` reports
`string`; reserve these keys for the native command, not ordinary string edits.
See [IDEM.md](IDEM.md) for bounds, replay and durability semantics.

- Hash replies for `HGETALL`, `HKEYS`, and `HVALS` follow storage order, which is unspecified as in Redis.
- `HMSET` writes like multi-field `HSET` and replies `OK` (Redis marks it deprecated). `HSETNX` returns 1 and records one field when the field is missing, and returns 0 without a write when it already exists. `HSTRLEN` returns the field length, or 0 when the key or field is missing. `HINCRBY` writes the new decimal and returns it. `HSCAN` returns a cursor and field/value pairs in field order.
- `LPOP` and `RPOP` accept a positive count. The reply is an array, or nil when the list is empty. Each removed element is recorded separately.
- `ZADD` accepts one or more score/member pairs and `NX`, `XX`, `GT`, `LT`, `CH`, and `INCR`. `NX` does not change an existing score. `XX` does not add a missing member. `GT` and `LT` still add a missing member. Without `CH` the reply counts new members; with `CH` it counts score changes. `INCR` updates one member and returns the new score.
- `ZRANGE` accepts `WITHSCORES`, `BYSCORE`, `BYLEX`, `REV`, and `LIMIT`. `ZREVRANGE` stays rank-only with `WITHSCORES`.
- `MULTI`/`EXEC` is shard-local and same-slot. `WATCH` aborts when a watched
  key is mutated. There is no cross-shard transaction and no `MULTI` nesting.
- `EVAL` is shard-local and same-slot. Lua errors and persistence failures
  restore the declared keys. The script cache is not durable.
- Bloom commands follow the RedisBloom command shape, but module-level compatibility has not been certified.
- Expiration metadata uses absolute Unix milliseconds so it can survive
  persistence and restart. Expired entries behave as missing. RESP TTL commands
  and active expiration are implemented. `maxmemory-policy` supports
  `noeviction` (default), `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, and
  `volatile-ttl`. `INFO` and
  Prometheus expose `expired_keys` by cause (`passive` on lookup, `active` on
  the bounded expire cycle) plus `expires` / `ruvio_keys_with_ttl`. Tests inject
  `TestClock` so those counters do not depend on wall time.
- Byte values use reusable `Vec` storage and a bounded shard-local size-class pool. Collection elements still use independent allocations; typed arenas and compact handles remain later optimizations.