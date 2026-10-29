---
title: "Seven small scripts I no longer write"
description: "Moving credit between wallets, selling the last ticket, keeping the newest timestamp. Each of these used to be a short Lua script. In Ruvio each one is a single command that either does the whole job or writes nothing."
date: 2026-10-29
tags: [Commands, Patterns]
scene: balances
icon: zap
commands: [TRANSFER, HTRANSFER, TAKE, INCRBY MAX, INCR EX, SET IFGT, LIMIT, ZPOPBYSCORE, ZMOVEDUE, SETV]
facts:
  - { value: "1", label: "WAL record for both sides of a TRANSFER" }
  - { value: "nil", label: "the answer when a command refuses; nothing was written" }
  - { value: "0", label: "scripts to load, version or keep in sync with the code" }
---

Every project I have worked on that used a Redis-style store ended up with a folder of tiny Lua scripts. They were never complicated. Read a number, compare it with something, write one or two keys, return. But each of them had to be loaded, called by its SHA, kept in step with the application code, and explained to the next person who opened the folder.

When I started Ruvio I went through that folder and asked a plain question for each script: *is this a command that nobody has written yet?* Surprisingly often the answer was yes. This post is the list of the ones that made it into the server, with the exact replies you get back. Everything below was run against the 0.4.0 release.

<!-- facts -->

## 1. Move money only if there is enough

The classic one. Take 30 credits from a wallet and put them in escrow, but only if the wallet holds at least 30. Nobody should ever see the money in both places, or in neither.

```redis
SET {user:42}:wallet 50
TRANSFER {user:42}:wallet {user:42}:escrow 30
# 1) 20  -> wallet after
# 2) 30  -> escrow after
TRANSFER {user:42}:wallet {user:42}:escrow 30
# (nil)  -> only 20 left, nothing changed
```
<!-- clients: balances -->

Both balances change in one WAL record, so a crash cannot leave you with only half of the move on disk.

> [!TIP] Keep both keys in one slot
> `TRANSFER` works on two keys of the same slot. The `{user:42}` hash tag in the key names is what puts them there, in a single server as well as in a cluster.

When the balances are fields of one hash, `HTRANSFER account:7 available held 30 MAX 1000` does the same between two fields and also refuses if the destination would pass the cap.

## 2. Sell the last ticket once

`DECRBY` happily goes below zero, so the usual fix is a script that reads the stock first. `TAKE` subtracts only when the key holds enough:

```redis
SET stock:ticket-42 500
TAKE stock:ticket-42 2
# 498   -> took 2, 498 left
TAKE stock:ticket-42 1000
# (nil) -> not enough, the key is unchanged
```
<!-- clients: inventory -->

The mirror image is a counter with a ceiling, for seats in a room or members in a team:

```redis
INCRBY seats:room-9 1 MAX 120
# 57    -> joined
# (nil) -> room is full, nothing was written
```

<!-- scene: inventory -->

## 3. A daily counter that cleans up after itself

This one is so small it hurts. You want a per-day counter that disappears tomorrow. With plain `INCR` you need a second `EXPIRE` call, and if you only want to set the deadline when the key is created, you are back to a script or a `MULTI` block.

```redis
INCR hits:user-7:2026-10-29 EX 86400
# 1     -> first hit, the day's deadline is set
# 2, 3… -> later hits leave the deadline alone
```

`EX` and `PX` on `INCR` and `INCRBY` only apply when the call creates the counter. Later calls do not push the deadline forward.

## 4. Keep the newest value, whatever order writers arrive in

Event consumers that run in parallel love to write an older watermark after a newer one. `IFGT` and `IFLT` on `SET` compare against the stored integer and only write when the new number wins:

```redis
SET watermark:orders 1700000200 IFGT
# OK    -> written
SET watermark:orders 1700000100 IFGT
# (nil) -> stored number is greater, nothing written
SET price:best 499 IFLT PX 3600000
# OK    -> new lowest price, expires in an hour
```
<!-- clients: watermarks -->

> [!WARNING] Integers only
> A stored value that is not a number is an error and stays as it was. I prefer that to a silent string comparison.

## 5. A rate limit in one round trip

Fixed window, sliding window and token bucket are all `LIMIT`. The reply is the allowance that is left, and a refused call is not counted:

```redis
LIMIT api:key-7 3 60000
# 2
# 1
# 0
# (nil) -> window is full, this call was not counted
LIMIT api:key-7:burst 10 1000 TOKENS COST 2 WITHRETRY
# 1) 8  -> 8 tokens left
# 2) 0  -> no need to wait
```
<!-- clients: rate-limits -->

`WITHRETRY` puts the wait in the reply, so the number for a `Retry-After` header comes from the server rather than from a guess in the client.

<!-- scene: rate-limits -->

## 6. Pop the jobs that are due

A sorted set with the run time as the score is the simplest scheduler there is. The awkward part was always "give me what is due *and remove it*" without two workers picking the same job.

```redis
ZADD {jobs}:due 100 job:9 200 job:10 300 job:11
ZPOPBYSCORE {jobs}:due 150 COUNT 10
# job:9 -> due and removed in the same call
ZMOVEDUE {jobs}:due {jobs}:ready 250 COUNT 100
# 1     -> job:10 moved to the tail of the list {jobs}:ready
```

`ZMOVEDUE` hands due members to a list queue in one recorded step, up to 10,000 per call. Both keys must share a slot; without the `{jobs}` tag the server answers `CROSSSLOT`, as it should. There is a whole post on [delayed jobs](/blog/delayed-jobs-without-a-scheduler/) if you want the stream version too.

## 7. Save only if nobody else saved first

Two people open the same profile, both press save. With `SETV` every write returns a version and a write that names an old version is refused:

```redis
SETV profile:ada "v1"
# 1
SETV profile:ada "v2" 1
# 2
SETV profile:ada "v3" 1
# ERR version mismatch
```
<!-- clients: versions -->

No `WATCH`, no retry loop around `MULTI`.

## What I did not turn into commands

Not every script deserves to become a command. Anything with real business rules, several branches and a dozen keys is still better as code in your application, or as a script if you really need it next to the data. Ruvio runs Lua for exactly that. The commands above made the cut because they are small, they come up in almost every codebase, and they share three properties:

<!-- cards -->
- {check} **All or nothing** Either the whole thing happens or nothing is written, not to the key and not to the log.
- {shield} **One record** A refused call leaves no trace, and an accepted one is a single WAL record that replicas apply the same way.
- {code} **A typed method** Each one is a method in all six clients, so there is no script text to send and no reply to parse by hand.

There is also a speed side. Every one of these is measured against the script it replaces on the [benchmarks page](/benchmarks/#scripted), together with the memory each key takes. I will let the table speak there rather than paste numbers here.

All six first-party clients have a typed method for each command, so in C# the first example is `await db.TransferAsync(...)` and not a string you send and parse. The tabs under each example show the same steps in all six languages.
