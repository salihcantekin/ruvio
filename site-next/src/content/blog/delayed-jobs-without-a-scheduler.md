---
title: "Delayed jobs without a scheduler"
description: "Send the reminder in 30 minutes, retry the webhook in 5. Two ways to do it with nothing but Ruvio: a stream entry that stays hidden until its time, and a sorted set you drain in one call."
date: 2026-10-29
tags: [Queues, Streams]
scene: delayed
facts:
  - { value: "DELAY", label: "one option on XADD; the entry is stored at once and delivered later" }
  - { value: "10,000", label: "due jobs ZMOVEDUE can hand to a list queue in one call" }
  - { value: "0", label: "extra WAL records when you do not use a delay" }
---

Almost every backend I have built needed some version of "do this later". Send a reminder tomorrow morning. Retry a failed webhook in five minutes. Close an unpaid order after an hour. The usual answer is another moving part: a cron table, a scheduler service, or a polling loop that wakes up every second and asks the database what is due.

If you already run Ruvio for queues or caching, you can skip that part. There are two ways to schedule work, and which one fits depends on whether you want consumer groups or the simplest thing that works.

<!-- facts -->

## Option 1: a stream entry that waits

Streams with consumer groups are the solid way to run workers: every entry is delivered to one consumer, unacknowledged entries stay pending, and a crashed worker's entries can be claimed by another one. Ruvio adds one option to `XADD`:

```redis
XGROUP CREATE jobs workers $ MKSTREAM
XADD jobs DELAY 30000 * kind send-reminder user 42
# 1793295045919-0 -> stored now
XREADGROUP GROUP workers w1 COUNT 10 BLOCK 5000 STREAMS jobs >
# (nil) until the 30 s have passed
XACK jobs workers 1793295045919-0
```

The entry is written right away, so it is in the log and survives a restart. `XREADGROUP` simply skips it until the delay has passed; after that it is delivered like any other entry. Pending lists, `XCLAIM` and `XAUTOCLAIM` work as usual, so your retry and dead-letter logic does not change. A plain `XREAD` still sees the entry, which is handy when you want to look at what is scheduled.

The worker side has nothing special in it. In Python:

```python
import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.xgroup_create("jobs", "workers", "$", mkstream=True)
r.xadd("jobs", {"kind": "send-reminder", "user": 42}, delay_ms=30_000)
read = r.xreadgroup("workers", "w1", {"jobs": ">"}, count=10, block=5000)
for _stream, entries in read or []:
    for entry in entries:  # the entry appears here after 30 s
        r.xack("jobs", "workers", entry.id)
```

The hold is a WAL record, and it is only written when a delay is set. Streams that never use `DELAY` pay nothing for it.

## Option 2: a sorted set as a timetable

Sometimes a stream is more than you need. Put each job in a sorted set with its run time as the score, and take what is due:

```redis
ZADD {jobs}:due 1700000300 job:9 1700000360 job:10
ZPOPBYSCORE {jobs}:due 1700000330 COUNT 10
# job:9 -> due and removed in the same call
```

`ZPOPBYSCORE` returns the members whose score is at or below the limit and removes them in the same step. Two workers calling it at the same moment never get the same job, which is the part that used to need a script.

If your workers already read from a list, `ZMOVEDUE` moves the due jobs onto it for you:

```redis
ZMOVEDUE {jobs}:due {jobs}:ready 1700000400 COUNT 100
# 1 -> one job moved to the tail of {jobs}:ready
```

It is one recorded step, up to 10,000 members per call. Run it from a small loop, or from any worker before it blocks on the list. The two keys have to live in the same slot, so give them a common hash tag like `{jobs}`; without one the server answers `CROSSSLOT` instead of guessing.

## Which one to pick

| | Stream with `DELAY` | Sorted set |
|---|---|---|
| Delivery | one consumer per entry, with acks | whoever pops first |
| Crashed worker | entry stays pending, `XAUTOCLAIM` it | gone once popped, add your own retry |
| Payload | fields on the entry | member name, details in another key |
| Change the time later | no, add a new entry | yes, `ZADD` the new score |
| Best for | reminders, retries, anything that must not be lost | timetables, rescheduling, simple queues |

My own rule: if losing a job would be a bug report, use the stream. If jobs get rescheduled a lot, or the "job" is just an id you look up anyway, the sorted set is simpler and easier to inspect.

## Things worth knowing

- Times are milliseconds for `DELAY`, and whatever unit you choose for sorted-set scores. Pick one and stick to it; I use Unix milliseconds everywhere.
- A delayed entry still counts toward the stream's length, so `MAXLEN` trimming can remove it before it is delivered. Size the trim with that in mind.
- Both options are ordinary writes. With durability set to `everysec` or `always` they are in the WAL and replicate like everything else.

If you also want to *see* jobs as they are written, without polling, `CHANGES SUBSCRIBE MATCH jobs*` gives you a resumable feed of every write when the server runs with a WAL. That is a story for another post. The full syntax for everything here is in the [command reference](/docs/commands/) and on the [use cases page](/use-cases/#delayed).
