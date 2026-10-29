---
title: "When a hot key expires: stopping cache stampedes with FILLGET"
description: "A popular cache entry expires and a thousand requests hit the database at once. FILLGET decides in one round trip who rebuilds the value, who waits, and who gets the stale copy in the meantime."
date: 2026-10-29
tags: [Caching, Patterns]
scene: cache
icon: layers
commands: [FILLGET, FILLSET]
facts:
  - { value: "1", label: "caller rebuilds an expired key; the rest wait or read the stale copy" }
  - { value: "5", label: "possible answers, so the client never has to guess" }
  - { value: "SWR", label: "stale-while-revalidate window, set per write" }
---

Cache-aside looks harmless on a whiteboard. Read the key; on a miss, load from the database and write it back. The trouble starts when the key is popular. It expires, and every request that arrives in the next few hundred milliseconds sees the same miss and runs the same expensive query. The database that the cache was protecting takes the full load at the worst possible moment.

People fix this with a lock around the refill, a random jitter on the TTL, a background refresher, or all three. Each fix adds a round trip or a moving part. I wanted the cache itself to answer the real question: *should this caller load the value, or not?*

<!-- facts -->

## One command, five answers

`FILLGET` reads the key and, if it is missing, makes the caller the filler or tells it that someone else already is:

```redis
FILLGET product:42 5000
# 1) FILL  2) 1    -> missing: you rebuild it, here is your fill token
FILLGET product:42 5000
# 1) BUSY  2) (nil) -> someone else is rebuilding it
FILLSET product:42 "{...}" 60000 1 SWR 30000
# 1                -> stored with fill token 1, fresh for 60 s
FILLGET product:42 5000
# 1) HIT   2) "{...}"
```
<!-- clients: cache -->

The `5000` is how long a filler may take. If it crashes halfway, its claim lapses after five seconds and the next caller becomes the filler. The token makes sure that only the caller who was asked to fill can write the value; a late `FILLSET` with an old token answers `0` and changes nothing.

## Serving stale while one caller refreshes

The `SWR 30000` on `FILLSET` is the part I like most. After the value's 60 seconds are up it is not thrown away at once. For another 30 seconds `FILLGET` hands out the old copy, and exactly one caller also gets a token to refresh it:

```redis
FILLGET product:42 5000
# 1) STALE  2) "{...}"  3) 2     -> stale copy, and you refresh it
FILLGET product:42 5000
# 1) STALE  2) "{...}"  3) (nil) -> stale copy, someone else is refreshing
```

So the page still renders instantly for everyone, and the database sees one query instead of a burst. If the refresh fails, users keep getting the stale copy until the window ends, which is usually what you want when the database is the thing having a bad day.

## Waiting without a retry loop

Without a stale copy, a `BUSY` caller has nothing to show. It can sleep and try again, or let the server do the waiting:

```redis
FILLGET product:42 5000 WAIT 200
# waits up to 200 ms for the filler's FILLSET, then answers HIT or BUSY
```

> [!WARNING] Not on a shared connection
> `WAIT` is meant for connections that belong to one request. On a connection shared by many callers, a held command would make everyone behind it wait too, so the clients do not use it there. It is also refused inside `MULTI` and scripts.

## From application code

The clients wrap the whole dance in one call: the tabs under the first example show it in every language. In C#, `GetOrCreateAsync` runs your factory in exactly one caller and polls with short `FILLGET` calls in the others, so a shared connection is never held.

If you are in ASP.NET Core, the `Ruvio.Extensions.Caching` package gives you an `IDistributedCache` with the same `GetOrCreate` across every instance of your app, plus the stale period and tags you can remove in one call. The details are on the [integrations page](/integrations/).

## The fine print

<!-- cards -->
- {cpu} **Markers in memory** After a restart nobody is filling, which is fine: the next caller simply becomes the filler.
- {timer} **Size fill_ms** A little above your slowest normal load. Too short and two callers may fill; too long and a crashed filler holds everyone up.
- {database} **The value is a write** `FILLSET` is an ordinary write with a TTL, as durable as your durability setting makes it.

The animation at the top shows the whole thing: one request goes to the database, the rest are served from memory. The reference for both commands is in the [command reference](/docs/commands/), and the [use cases page](/use-cases/#cache) has the same example in all six clients.
