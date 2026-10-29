---
title: "A lock is not enough: fencing tokens with LEASE"
description: "A lock with a TTL keeps two workers apart until one of them pauses for longer than the TTL. Here is what goes wrong then, and how the token that LEASE returns lets the protected side say no."
date: 2026-10-29
tags: [Locks, Patterns]
scene: locks
facts:
  - { value: "64-bit", label: "fencing token on every grant, and it only grows" }
  - { value: "3", label: "commands: LEASE, RENEW, RELEASE" }
  - { value: "0", label: "the answer to RENEW or RELEASE with a token that no longer holds the lock" }
---

The usual distributed lock is `SET key me NX PX 30000`. It works well right up to the day a worker stops for 31 seconds. A long garbage-collection pause, a VM that got migrated, a laptop lid closed during a demo. The lock expires, a second worker takes it, and then the first one wakes up and carries on writing as if nothing happened. For a while you have two holders, and the store that the lock was protecting has no idea.

The fix has been known for a long time: give every grant a number that only goes up, send that number along with every write, and let the thing you protect reject a number older than the last one it saw. That number is a *fencing token*. In Ruvio every lock grant returns one.

<!-- facts -->

## The three commands

```redis
LEASE job:nightly-report 30000
# 114770882368960 -> you hold it, this is your token
LEASE job:nightly-report 30000
# (nil)           -> someone else holds it
RENEW job:nightly-report 114770882368960 30000
# 1               -> deadline moved to now + 30 s
RELEASE job:nightly-report 114770882368960
# 1               -> released
```

`LEASE` gives you the lock and a token, or a null when someone else has it. `RENEW` moves the deadline for a long job, but only for the token that holds the lock right now. `RELEASE` frees it, again only for the right token. A stale worker calling `RENEW` or `RELEASE` gets `0` back and changes nothing, so it cannot free a lock that by now belongs to someone else.

The token is an opaque 64-bit integer. Do not read meaning into its digits; the only promise is that the next grant on the same key gets a larger one. That stays true after the lease expires, after a restart and after a failover.

## Where the token earns its keep

The lock on its own still cannot stop the paused worker. What stops it is the other end. Say the job writes a report row. Keep the highest token you have seen next to the data, and make the write conditional on it:

```sql
UPDATE reports
SET body = $1, fence = $2
WHERE id = $3 AND fence < $2;
-- 0 rows updated -> a newer holder already wrote; stop here
```

Now follow the animation at the top again. Worker 1 gets token 41 and stalls. The lease runs out, worker 2 gets 42 and writes with it. Worker 1 wakes up and tries to write with 41. The `WHERE fence < 41` no longer matches, zero rows change, and worker 1 learns that it lost the lock instead of overwriting newer data.

The same trick works with anything that can compare a number: a column in Postgres, an `IFGT` write in Ruvio itself, a version header on an object store, a check in your own service. The important part is that the comparison happens where the data lives, not in the worker that might be asleep.

## In application code

Every client has typed methods for the three commands. In C#:

```csharp
await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

long? token = await db.LeaseAsync("job:nightly-report", 30_000);
// token -> you hold it
// null -> someone else holds it
if (token is long held)
{
    long released = await db.ReleaseAsync("job:nightly-report", held);
    // 1 -> released (0 if the token no longer matches)
}
```

Rust, Go, Java, Python and Node.js look the same; the [use cases page](/use-cases/#locks) has all six.

## When you need N holders, not one

Sometimes the rule is "at most eight exports at a time" rather than "one". `SEMAPHORE` gives each holder its own slot, its own TTL and its own token:

```redis
SEMAPHORE upstream:billing 8 15000
# 114770882371136 -> a slot, this is your token
# (nil)           -> all 8 slots are taken
RELEASE upstream:billing 114770882371136
```

A crashed worker frees its slot when its own TTL ends. The others are not affected, and the key goes away when the last holder leaves.

## Failover, honestly

Replication in Ruvio is asynchronous by default. If a primary hands out a lease and dies before the replica has that write, the new primary has never heard of the grant and can hand the same lock to someone else. Two things close that gap:

1. Take the lock on a connection that sent `CLIENT DURABILITY REPLICAS 1`. The grant is then only answered once a replica holds it on disk, and `ruvio coordinator` fails over only to a replica that holds every such write.
2. Keep the protected side checking tokens. Tokens keep growing across a failover, so the check above still rejects the older holder even in the rare case where two grants overlap.

Keep the nodes' clocks synchronized as well, since a lease is a deadline. None of this makes a lock magically perfect. It moves the guarantee to the place that can actually enforce it, and that is the whole point of fencing.

## Rules of thumb

- Pick a TTL longer than a normal run, and `RENEW` from the job for the long ones.
- Release as soon as you are done. Waiting for the TTL just makes the next worker wait.
- Always pass the token to the write you are protecting. A lock without a fence is a hint, not a guarantee.

The full syntax is in the [command reference](/docs/commands/). If you have a lock story of your own, good or bad, I would like to hear it in [Discussions](https://github.com/salihcantekin/ruvio/discussions).
