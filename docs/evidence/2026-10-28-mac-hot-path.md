[Ruvio](../../README.md) / [Docs](../README.md) / [Measurement records](README.md) / Mac after the hot-path changes

# Mac: 60 commands and memory after the hot-path changes

The same Mac comparison as the 2026-10-14 record, run again on the `dev` head after the hot-path pull requests (#281 to #299), next to the `dev` commit just before them, with Redis in every run. Raw data, every round: [mac-2026-10-28.json](../../benchmark/records/mac-2026-10-28.json) (after), [mac-2026-10-28-pre-hotpath.json](../../benchmark/records/mac-2026-10-28-pre-hotpath.json) (before), [mac-2026-10-14.json](../../benchmark/records/mac-2026-10-14.json) (earlier record). Script: [scripts/redis_compare_features.py](../../scripts/redis_compare_features.py).

## Conditions (read before quoting any number)

- Host: Apple silicon, 12 cores, 24 GiB, macOS 26.7.1. Not pinned, not quiet: two Astro dev servers and one idle Ruvio process were running. The earlier record was taken on macOS 26.6.2.
- Ruvio: plain `release` build, `memory` durability, 1 shard. After is `93eb7084` (#299), before is `37bf8a5d`, the `dev` head ahead of #281. Redis 8.10.2, same binary in all runs.
- Load: `redis-benchmark`, 50 clients, pipeline 16, 64 B values, one command per run, request counts scaled by 0.3, three rounds with alternating server order; the table shows the median. Three seconds between rows because of macOS ephemeral port exhaustion.
- Order: the before run and the after run were back to back (about 4 minutes each), each with its own Redis; the earlier column is the 2026-10-14 record.
- Many rows stop at 3.46M to 3.75M requests per second. That is the resolution and client limit of `redis-benchmark` on this host, so a gain on those rows cannot be seen here.
- Not measured: latency (p50, p99), durability on, more than one shard, large data sets, x86 or Linux. A Mac number says nothing about x86.

## Result

Geometric mean over the 60 commands: before against the 2026-10-14 record 1.28×, after against before 1.03×, after against Redis 1.46×. Most of the gain since 14 October was already in the `dev` head before #281; the hot-path changes add about 3.5%, with a few commands clearly ahead (EXISTS, GETBIT, ZINCRBY, APPEND, TTL, MEMORY USAGE, LPUSH) and a few slightly behind that are inside the round-to-round noise of one run (ZSCORE, SRANDMEMBER, PFCOUNT).

Requests per second in thousands. "After / before" and "After / Redis" are ratios.

| Command | 14 Oct | Before | After | After / before | Redis | After / Redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2,143 | 3,103 | 3,103 | 1.00 | 2,432 | 1.28 |
| SET | 2,812 | 3,214 | 3,103 | 0.97 | 2,045 | 1.52 |
| LPOS (100 elements, hit in the middle) | 1,579 | 1,800 | 1,765 | 0.98 | 1,125 | 1.57 |
| LSET (100 elements) | 1,875 | 2,143 | 2,195 | 1.02 | 1,765 | 1.24 |
| LINSERT (pivot missing, scans 100) | 1,286 | 1,385 | 1,406 | 1.02 | 882 | 1.59 |
| LREM (value missing, scans 100) | 1,304 | 1,385 | 1,429 | 1.03 | 909 | 1.57 |
| LMOVE (rotate a 100-element list) | 2,045 | 2,647 | 2,727 | 1.03 | 1,475 | 1.85 |
| BITFIELD INCRBY u8 (always writes) | 1,875 | 2,500 | 2,571 | 1.03 | 1,607 | 1.60 |
| BITFIELD GET u8 | 2,195 | 2,903 | 2,903 | 1.00 | 1,800 | 1.61 |
| SETBIT (1 KiB bitmap, same bit) | 2,571 | 3,462 | 3,462 | 1.00 | 2,093 | 1.65 |
| GETBIT (1 KiB bitmap) | 2,250 | 3,000 | 3,600 | 1.20 | 2,368 | 1.52 |
| BITCOUNT (1 KiB bitmap) | 2,250 | 3,462 | 3,462 | 1.00 | 2,500 | 1.38 |
| PFADD | 2,432 | 3,462 | 3,462 | 1.00 | 2,432 | 1.42 |
| PFCOUNT | 2,432 | 3,462 | 3,214 | 0.93 | 2,647 | 1.21 |
| COPY ... REPLACE (64 B string) | 2,093 | 3,214 | 3,214 | 1.00 | 1,525 | 2.11 |
| DUMP (64 B string) | 1,957 | 2,308 | 2,500 | 1.08 | 1,765 | 1.42 |
| EXPIRETIME | 2,500 | 3,600 | 3,750 | 1.04 | 2,727 | 1.37 |
| TOUCH | 2,308 | 3,750 | 3,750 | 1.00 | 2,812 | 1.33 |
| OBJECT ENCODING | 2,727 | 3,333 | 3,333 | 1.00 | 2,368 | 1.41 |
| MEMORY USAGE | 2,432 | 3,103 | 3,600 | 1.16 | 2,432 | 1.48 |
| GETRANGE | 2,571 | 3,103 | 3,103 | 1.00 | 2,093 | 1.48 |
| SETRANGE | 2,432 | 3,600 | 3,600 | 1.00 | 2,143 | 1.68 |
| SETEX | 2,500 | 3,000 | 3,103 | 1.03 | 1,304 | 2.38 |
| MSET (3 keys) | 2,195 | 2,812 | 2,903 | 1.03 | 1,154 | 2.52 |
| MGET (3 keys) | 1,837 | 2,045 | 2,093 | 1.02 | 1,667 | 1.26 |
| DEL (missing key) | 2,368 | 3,462 | 3,462 | 1.00 | 2,571 | 1.35 |
| EXISTS | 2,500 | 2,647 | 3,600 | 1.36 | 2,727 | 1.32 |
| TYPE | 2,368 | 3,214 | 3,214 | 1.00 | 2,727 | 1.18 |
| TTL | 2,368 | 3,103 | 3,600 | 1.16 | 2,727 | 1.32 |
| PTTL | 2,250 | 3,333 | 3,462 | 1.04 | 2,727 | 1.27 |
| PERSIST (key without TTL) | 2,500 | 3,750 | 3,600 | 0.96 | 2,727 | 1.32 |
| EXPIRE (key with TTL) | 2,308 | 3,103 | 3,214 | 1.04 | 1,875 | 1.71 |
| APPEND (1 byte) | 2,432 | 2,571 | 3,000 | 1.17 | 2,368 | 1.27 |
| INCRBYFLOAT | 1,343 | 1,385 | 1,500 | 1.08 | 1,268 | 1.18 |
| HSET (existing field) | 2,500 | 3,000 | 3,000 | 1.00 | 2,000 | 1.50 |
| HGET | 2,571 | 3,214 | 3,333 | 1.04 | 2,250 | 1.48 |
| HMGET (2 fields) | 2,000 | 2,432 | 2,368 | 0.97 | 1,837 | 1.29 |
| HSTRLEN | 2,500 | 3,214 | 3,600 | 1.12 | 2,308 | 1.56 |
| LPUSH | 2,093 | 2,308 | 2,647 | 1.15 | 2,000 | 1.32 |
| LLEN | 2,308 | 3,600 | 3,600 | 1.00 | 2,647 | 1.36 |
| LRANGE 0 2 | 1,875 | 2,093 | 2,045 | 0.98 | 1,800 | 1.14 |
| SADD (existing member) | 2,368 | 3,462 | 3,462 | 1.00 | 2,500 | 1.38 |
| SISMEMBER | 2,500 | 3,333 | 3,600 | 1.08 | 2,432 | 1.48 |
| SRANDMEMBER | 2,432 | 3,214 | 3,000 | 0.93 | 2,571 | 1.17 |
| ZADD (existing member, same score) | 2,143 | 2,812 | 3,000 | 1.07 | 1,915 | 1.57 |
| ZINCRBY | 1,579 | 1,731 | 2,045 | 1.18 | 1,552 | 1.32 |
| ZSCORE | 2,250 | 3,103 | 2,812 | 0.91 | 2,250 | 1.25 |
| RENAME (same key) | 2,500 | 3,333 | 3,214 | 0.96 | 2,368 | 1.36 |
| PUBLISH (no subscribers) | 2,571 | 3,600 | 3,600 | 1.00 | 2,727 | 1.32 |
| HGET (10-field hash) | 2,308 | 3,103 | 3,214 | 1.04 | 1,915 | 1.68 |
| HSET (10-field hash, existing field) | 1,915 | 2,647 | 2,903 | 1.10 | 1,731 | 1.68 |
| HDEL (10-field hash, missing field) | 1,957 | 3,000 | 3,103 | 1.03 | 2,045 | 1.52 |
| HGETALL (10-field hash) | 763 | 744 | 763 | 1.03 | 763 | 1.00 |
| LINDEX (20-item list) | 2,143 | 2,903 | 3,214 | 1.11 | 2,250 | 1.43 |
| LRANGE 0 9 (20-item list) | 1,184 | 1,233 | 1,233 | 1.00 | 1,216 | 1.01 |
| LSET (20-item list) | 2,195 | 2,903 | 3,000 | 1.03 | 2,000 | 1.50 |
| GEOADD | 1,935 | 2,609 | 2,609 | 1.00 | 1,154 | 2.26 |
| GEOSEARCH (radius 50 km, COUNT 10) | 44 | 46 | 47 | 1.03 | 19 | 2.47 |
| GETEX (set a 100 s deadline) | 2,368 | 2,812 | 3,000 | 1.07 | 1,500 | 2.00 |
| FILLGET (value present) | 1,957 | 2,432 | 2,500 | 1.03 | 2,647 | 0.94 |

## Memory

Resident memory above the idle process, in KiB, after loading the same data into each server. Idle resident memory of Ruvio: 8,896 KiB on 14 Oct, 9,312 KiB before, 9,296 KiB after (about 420 KiB more than on 14 Oct in both new runs; the cause was not looked into). Before and after are the same within a few percent, so the hot-path changes did not change memory.

| Data | 14 Oct | Before | After | Redis |
| --- | ---: | ---: | ---: | ---: |
| 20,000 lists of 20 elements | 6,016 | 6,032 | 6,032 | 6,464 |
| 5,000 HyperLogLogs of 100 elements | 2,288 | 2,288 | 2,272 | 2,720 |
| 20,000 bitmaps of 128 B (SETBIT) | 3,808 | 3,808 | 3,776 | 4,512 |
| 100,000 BITFIELD counters (u16) | 3,648 | 4,016 | 4,000 | 7,376 |
| 100,000 geo points in one key | 6,384 | 7,616 | 6,912 | 7,936 |
| 100,000 strings of 16 B | 5,200 | 5,136 | 5,120 | 8,928 |
| 100,000 strings of 128 B | 18,176 | 18,144 | 18,128 | 21,536 |
| 100,000 strings of 16 B with TTL | 10,176 | 10,480 | 10,448 | 11,504 |
| 100,000 one-element lists | 4,992 | 4,992 | 4,992 | 8,944 |
| 50,000 small hashes (3 fields) | 3,136 | 3,136 | 3,120 | 4,512 |
| 50,000 small sets (4 members) | 2,704 | 2,736 | 2,752 | 4,560 |
| 50,000 small sorted sets (3 members) | 2,688 | 2,784 | 2,832 | 4,496 |
