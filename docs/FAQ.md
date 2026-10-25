# Frequently asked questions

Short answers to what people ask first. Details are in the linked pages.

## The basics

### What is Ruvio?

A key-value server that speaks the Redis protocol (RESP2). `redis-cli` and
the Redis clients of most languages connect to it, and there are
[first-party clients](CLIENT_BEHAVIOR.md) for Rust, C#, Go, Java, Python and Node.js
(PHP works through phpredis or Predis, see [PHP](PHP.md)).
Data lives in memory and can be made durable with a write-ahead log.

### Is it a drop-in replacement for Redis?

No. It supports many Redis commands and adds its own, but it is not a
complete Redis. The [compatibility notes](COMPATIBILITY.md) list what is
supported and what differs.

### How do I start it and connect?

Run the binary or the Docker image, then connect to `127.0.0.1:6379`:

```
redis-cli PING
```

A server reached from other machines needs a password
(`--requirepass`); a public bind without one is refused. See [setup](SETUP.md).

### Is my data still there after a restart?

It depends on the durability profile. The default, `memory`, keeps nothing on
disk, so a restart starts empty. `snapshot` writes periodic checkpoints;
`everysec` and `always` also keep a write-ahead log, losing about one second
or nothing on a crash. Choose with `--durability` or `persistence.profile`.
See [recovery](RECOVERY.md) and [configuration](CONFIGURATION.md).

### Are there limits on keys and values?

By default a key may be up to 1 MiB and a value up to 16 MiB
(`max_key_bytes`, `max_value_bytes`). Memory is not limited unless you set a
budget (see below).

### What is a logical database?

A numbered key space chosen with `SELECT n`. A server with one shard has 16 by
default. See [logical databases](LOGICAL_DATABASES.md).

## Expiry and eviction

### When does a key expire?

Only when you give it a time to live, for example `SET key value EX 60` or
`EXPIRE key 60`. A key without a TTL never expires. From the deadline on, the
key is gone for every command: reads treat it as absent even before the memory
is released. The server also removes expired keys in the background (about
every 25 ms and a few before each command), so memory is freed shortly after.
`TTL key` shows what is left, `PERSIST key` removes the TTL.

### When is a key evicted?

Only when you configure a memory budget (`max_shard_bytes`) and an eviction
policy other than the default. Eviction removes keys to make room; expiry
removes keys whose time is up. They are different things.

### What happens when memory is full?

With the default policy, `noeviction`, a write that would exceed the budget
fails with `OOM command not allowed when used memory > 'maxmemory'`. Deletes
and other commands that only free memory still work. With `allkeys-lru`,
`allkeys-lfu`, `volatile-lru` or `volatile-ttl`, the server deletes keys to
make room. The `volatile` policies only consider keys that have a TTL. The
default budget is unlimited, so nothing is evicted until you set one. Eviction
is per shard. See [memory accounting](MEMORY_ACCOUNTING.md).

### Can I find out that a key expired or was evicted?

Yes. Enable keyspace notifications (`notify_keyspace_events`) and subscribe to
the `expired` and `evicted` events. See [configuration](CONFIGURATION.md).

## Shards and slots

### What is a slot?

A slot is a bucket number from 0 to 16,383. Every key belongs to exactly one
slot, and the slot is computed from the key name alone:

1. If the key contains a non-empty `{...}` part, only the text inside the
   first braces is used (the hash tag). Otherwise the whole key is used.
2. The CRC16 checksum of those bytes is taken (the same CRC16 as Redis Cluster).
3. The checksum modulo 16,384 is the slot.

The same key gives the same slot on every server, every client and every run.

### What is a shard, and how does a key get to one?

A shard is one worker inside a Ruvio process with its own data and its own
port. The 16,384 slots are split into consecutive ranges, one per shard. With
2 shards, slots 0-8191 belong to the first and 8192-16383 to the second; the
shard of a slot is `slot * shards / 16384`. So a key's place is decided by its
name: key to slot to shard. The server keeps that answer in a table of 16,384
entries, filled from the rule when it starts, and routes by the table.
`RUVIO MOVESLOTS` can move a range of slots to another shard later (see below).

### Who decides this, and where?

The rule is in `src/routing.rs` of the server (`hash_slot`, `shard_for_slot`
and the `SlotTable` filled from it). Every client library has its own copy of
`hash_slot`, checked against the same test vectors. The client asks the server for the slot ranges with
`CLUSTER SLOTS` when it connects and sends each key to its owner. If a key
reaches the wrong shard anyway, the server forwards it to the owner and answers
on the same connection.

### Can I move slots to another shard?

Yes, between the shards of one server: `RUVIO MOVESLOTS 0-1000 2` moves slots 0 to 1000 to
shard 2 (shards count from 0) while both keep serving them, and `RUVIO MOVESLOTS STATUS`
follows the move (`state` is `copying`, then `cleaning`, then `done`). Use it when one shard
gets more traffic than the others. Clients keep working: a command sent to the old shard is
forwarded, `CLUSTER SLOTS` shows the new owner at once, and the clients pick it up on their
next slot map load. During the hand-over the old shard pauses for a moment (well under a
millisecond without a log, around 15 ms with one on a Mac, mostly the log sync). One move runs at a time, every slot of the range must be on one
shard, and it is refused while replication is on. With `everysec` or `always` the move
survives a crash at any step; the moved ranges are kept in the `SLOTS` file of the data
directory.

Between machines, with a multi-node cluster run by `ruvio coordinator`:
`COORDINATOR MOVE 0 1000 b 0` moves slots 0 to 1000 to shard 0 of group `b`, and
`COORDINATOR STATUS` follows it; `COORDINATOR REBALANCE` evens out the slots across the groups
with such moves (`DRYRUN` shows the plan first). Clients keep working there too: during the short hand-over
the old owner answers `TRYAGAIN`, which the Ruvio clients retry, and afterwards `MOVED`. See
`docs/COORDINATOR.md`.

### How do I list the keys of one slot?

`SCAN 0 SLOT <slot> COUNT 1000`, then again with the returned cursor until it is `0`. Send it to any
port: it runs on the shard that owns the slot. There is no per-slot key index (it would cost memory
for every key), so the walk visits the whole shard in steps and a page may be empty before the end.

### How do I find out which shard holds a key?

Ask the server for the slot, then look it up in the slot ranges:

```
CLUSTER KEYSLOT user:1
CLUSTER SLOTS
```

You rarely need this. The clients do it for you.

### Why do `MGET`, `MSET`, `DEL` or `EVAL` return `CROSSSLOT`?

Their keys are in different slots. A command that reads or writes several keys
must see them all in one place to stay correct, so the server refuses it, as
Redis Cluster does, and changes nothing. This holds even when both slots
happen to be on the same shard.

### How do I make several keys work together?

Give them a common hash tag: `{order:7}:items` and `{order:7}:total` share the
slot of `order:7`, so they share a shard. Then `MGET`, `MULTI`/`EXEC`, `EVAL`
and the like work on them. Do not put one tag on all your keys, or one shard
gets all the load.

### Do the clients handle keys in several slots for me?

For `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH`, yes: the client
splits the command per slot and merges the replies. It is not atomic. Other
commands, including `EVAL`, fail with `CROSSSLOT` before anything is sent. See
[client behavior](CLIENT_BEHAVIOR.md).

### What may a script touch?

Only the keys declared with `EVAL`, `EVALSHA` or `FCALL`, and those keys must
be in one slot. A script that declares no keys runs on the shard of the
connection that sent it.

### Examples

Slots below are for a server with 2 shards (first shard 0-8191, second
8192-16383). `CLUSTER KEYSLOT` returns the slot number for any key.

| Key | Part that is hashed | Slot | Shard |
| --- | --- | --- | --- |
| `user:1` | `user:1` | 10778 | second |
| `user:2` | `user:2` | 6777 | first |
| `{user:1}:profile` | `user:1` | 10778 | second |
| `{user:1}:cart` | `user:1` | 10778 | second |
| `{user:2}:profile` | `user:2` | 6777 | first |
| `order:7:items` | whole key | 11097 | second |
| `{order:7}:items` | `order:7` | 6368 | first |
| `{order:7}:total` | `order:7` | 6368 | first |

- `user:1` and `user:2` are on different shards, so `MGET user:1 user:2`
  returns `CROSSSLOT`. `MGET {user:1}:profile {user:1}:cart` works.
- Only the first non-empty `{...}` counts: `a{b}c` hashes `b`, and
  `foo{}{bar}` hashes the whole key because the first braces are empty.
- Without a tag, related keys such as `order:7:items` and `order:7:total` are
  spread over the slots independently. Write them as `{order:7}:items` and
  `{order:7}:total` when a command or script needs both.

```
SET {order:7}:items 3
SET {order:7}:total 120
MGET {order:7}:items {order:7}:total
EVAL "return ruvio.call('GET', KEYS[1])" 1 {order:7}:total
```

## Building blocks instead of scripts

### How do I take a lock, and what does the fencing token do?

`LEASE key ttl_ms` returns a token if you now hold the key and a null reply if someone else does. The token only grows: after expiry, `RELEASE`, `DEL`, a restart, a failover or a slot move the next grant is larger, so a storage system can refuse a write that carries an older token. `RENEW key token ttl_ms` extends a held lease without giving it up. This is a single-key lock held by the shard that owns the key, not a multi-node Redlock; see [Data types](DATA_TYPES.md) and the [use case](https://ruviodb.com/use-cases/#locks).

### Do I still need a Lua script for a rate limit, a counter with a cap or a conditional write?

Usually not. `LIMIT` is a fixed, sliding or token-bucket limiter in one call, `INCRBY ... MAX` is a counter that never passes a capacity, `SET ... IFEQ|IFGT|IFLT` and `DELEX ... IFEQ` are conditional writes, and `TRANSFER` moves an amount between two integer strings. A refused call writes nothing. Scripts still work (`EVAL`, `FCALL`) when a job needs several steps.

### What does `IDEM` give me, and what does it not?

`IDEM key BEGIN fingerprint owner ttl_ms` tells a caller to run (`acquired`), wait (`pending`), stop (`conflict`) or replay the saved result (`completed`), and `COMPLETE` publishes that result. It deduplicates requests on the shard that owns the key. It does not make the side effects of your handler exactly-once: if the handler crashes after charging a card and before `COMPLETE`, the record stays pending until its TTL passes. Details and the wire contract are in [IDEM](IDEM.md).

### How is `CHANGES SUBSCRIBE` different from keyspace notifications?

Keyspace notifications ride Pub/Sub and are lost when nobody listens. `CHANGES SUBSCRIBE` reads the write-ahead log, so it needs a durability profile other than `memory`, gives every change a cursor (`generation:shard:seq`) and resumes from a saved cursor after a disconnect or restart, with `MATCH`, `TYPES` and `VALUES` filters. `CHANGES START` is the lighter in-memory feed that is empty after a restart. See the [change stream settings](CONFIGURATION.md#change-stream).

### How do I run delayed jobs?

`XADD stream DELAY ms * ...` stores the entry now and `XREADGROUP` skips it until the delay passes. For a plain schedule, keep due times as sorted-set scores and take them with `ZPOPBYSCORE` or `ZMOVEDUE` (which moves them to a list in the same call).

### When should I stay on Redis?

When you need Redis modules, full Sentinel behavior, or RDB/AOF file compatibility: Ruvio speaks RESP2 and supports a large part of the command set, but it is not a complete Redis (see the [compatibility notes](COMPATIBILITY.md)). Ruvio's own building blocks are not available on Redis, so code that uses them does not move back without changes.

### Can I try these in a few minutes?

Start a server (`deploy/local.sh up`, or `ruvio` with its defaults) and run `python3 examples/09_building_blocks_tour.py`. With `RUVIO_DURABILITY=everysec` and `RUVIO_DATA_DIR` set, the tour also reads its writes back through `CHANGES SUBSCRIBE`.

## Replicas and failover

### Can another machine hold a copy of the data?

Yes. A replica follows a primary over the network and keeps its own copy, shard by shard
(`replication.role = "replica"`, or `REPLICAOF host port` while it runs). Replication is
asynchronous: the primary does not wait for replicas unless a client asks with `WAIT`. Clients can
send read-only commands to replicas with a read policy. See [RECOVERY.md](RECOVERY.md).

### What happens when the primary goes down?

Without coordinators, nothing on its own: you promote a replica with `REPLICAOF NO ONE` and point the
others at it. With three `ruvio coordinator` processes running, they notice, agree among themselves,
promote the replica with the most data and repoint the rest, usually within a few seconds; when the
old primary comes back it becomes a replica. Clients with Redis Sentinel support ask the coordinators
where the primary is. See [COORDINATOR.md](COORDINATOR.md).

### Do I need the coordinator?

Only for automatic failover. A single server, or a primary with replicas you promote by hand, runs
without it. The coordinator is part of the same `ruvio` binary and image (`ruvio coordinator`).
