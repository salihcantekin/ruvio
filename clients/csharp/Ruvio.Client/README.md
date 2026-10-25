<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.Client <sub>NuGet</sub></h1>

<p align="center">RESP2 client for <a href="https://ruviodb.com/">Ruvio</a> with typed methods for every server command, single-connection multiplexing and cluster routing.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.Client?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.Client?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/csharp/">C# guide</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/README.md">All C# projects</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

## Install

```bash
dotnet add package Ruvio.Client
```

```csharp
using Ruvio.Client;

await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();
await db.SetAsync("session:ada", "hello");
string? value = await db.GetStringAsync("session:ada");
```

ASP.NET Core hosts register the client with [`Ruvio.Client.AspNetCore`](../Ruvio.Client.AspNetCore/README.md). Framework integrations are in [`integrations/dotnet`](../../../integrations/dotnet/README.md).

## Binary commands

`IRuvioClient.ExecuteBinaryAsync(IReadOnlyList<ReadOnlyMemory<byte>>, CancellationToken)`
sends raw RESP bulk arguments without UTF-8/base64 conversion; binary replies are in
`RespValue.Bytes`. Keep argument memory unchanged until the operation completes.
The default interface implementation throws `NotSupportedException`, so existing
implementations remain compatible but must opt into binary support.

```csharp
await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[]
{
    "SET"u8.ToArray(), "binary-key"u8.ToArray(), new byte[] { 0, 255, 128 }
}, cancellationToken);
```

This API never replays a command after a transport failure; cancellation after dispatch
does not establish whether the server executed it. Explicit MOVED rejections may redirect.
Standalone mode accepts arbitrary commands. Cluster mode supports GET, SET, DEL, UNLINK,
EXISTS, HGET, HSET, HMGET, HMSET, HDEL, PEXPIRE, PEXPIREAT, PTTL, TTL, PERSIST, IDEM, EVAL,
EVALSHA, PING, and ECHO. Binary keys use raw-byte hash slots. `DEL`, `UNLINK`, and `EXISTS`
with keys in several slots are sent once per slot and the counts summed; `EVAL`/`EVALSHA`
keys must share one slot, or the call fails with `CROSSSLOT` before anything is sent.
Cluster transactions are deliberately not supported by this API; unsupported commands fail
explicitly. Existing string APIs are unchanged.

RESP2 client for [Ruvio](https://ruviodb.com/). Connecting does not send `CLIENT SETINFO` or `HELLO`. The typed methods cover every command in `docs/compatibility-matrix.json` (299/299) and the options that change what Ruvio does; `MatrixCoverageTests` fails when a matrix command has no typed method, and `LiveCommandTests` sends every one of them to a real server. `FLUSHDB ASYNC` (it runs synchronously) and `HELLO AUTH`/`SETNAME` have no typed form. Anything else can go through `ExecuteAsync` with the same argument lists as `redis-cli`.

## Command coverage

`Ruvio.Client.Tests/MatrixCoverageTests.cs` reads `docs/compatibility-matrix.json` and maps each command
to the method that sends it (`SELECT` maps to `RuvioConnection.GetDatabaseAsync`). `LiveCommandTests`
runs every command family against a live server through a recording TCP proxy and fails unless all 291
matrix commands were sent by their typed methods and answered. Set `RUVIO_TEST_ADDR=host:port` to run the
live tests; without it they are skipped. They use databases 12, 13 and 14 (`FLUSHDB`), create and delete
uniquely-prefixed keys in database 0 for `JSON.INDEX`, and end with `FLUSHALL`: use a disposable server.

```sh
RUVIO_TEST_ADDR=127.0.0.1:6379 dotnet test clients/csharp/Ruvio.Client.Tests
```

Ruvio-specific commands and options added in this pass:

| Command / option | Method |
|---|---|
| `SET key value IFEQ expected [PX ms \| KEEPTTL]` | `SetIfEqualAsync(key, value, expected, expiry?)`, `SetIfEqualKeepingTimeToLiveAsync` |
| `SET key value IFGT \| IFLT [PX ms]` | `SetIfGreaterAsync(key, long, expiry?)`, `SetIfLessAsync(key, long, expiry?)`; false when the stored number was not greater / smaller |
| `DELEX key IFEQ value` | `DeleteIfEqualAsync` |
| `LPUSHCAP` / `RPUSHCAP` | `PushListLeftCappedAsync` / `PushListRightCappedAsync(key, cap, values)` |
| `BF.RESERVE ... EXPANSION n \| NONSCALING` | `ReserveBloomFilterAsync(key, error, capacity, expansion, nonScaling)` |
| `BF.MADD`, `BF.MEXISTS`, `BF.INSERT`, `BF.INFO`, `BF.CARD` | `AddBloomFilterManyAsync`, `BloomFilterContainsManyAsync`, `InsertBloomFilterAsync(key, items, BloomInsertOptions)`, `GetBloomFilterInfoAsync`, `GetBloomFilterCardinalityAsync` |
| `JSON.SET ... NX \| XX` | `JsonSetAsync(key, path, json, SetMode)` |
| `JSON.INDEX CREATE/DROP/LIST`, `JSON.FIND` | `JsonCreateIndexAsync(name, prefix, path, JsonIndexType)`, `JsonDropIndexAsync`, `JsonListIndexesAsync`, `JsonFindAsync(index, value, count?, after?)` |
| `JSON.OBJLEN`, `JSON.STRAPPEND`, `JSON.NUMMULTBY`, `JSON.ARRINSERT`, `JSON.ARRPOP`, `JSON.ARRTRIM`, `JSON.ARRINDEX`, `JSON.FORGET`, `JMERGE` | `JsonObjectLengthAsync`, `JsonStringAppendAsync`, `JsonMultiplyAsync`, `JsonArrayInsertAsync`, `JsonArrayPopAsync`, `JsonArrayTrimAsync`, `JsonArrayIndexAsync`, `JsonForgetAsync`, `JMergeAsync` |
| `IDEM key BEGIN` (typed) | `TryBeginIdempotencyAsync` returns `IdempotencyBegin(State, Result)` |
| `RUVIO`, `RUVIO OWNER` | `RuvioAsync`, `GetRuvioOwnerAsync` |

Redis-compatible commands added: `SETEX`/`PSETEX` (`SetExpireAsync`, `SetExpireMillisecondsAsync`), `SETNX`
(`SetIfNotExistsAsync`), `GETDEL` (`GetDeleteAsync`), `MSETNX` (`SetManyIfNotExistsAsync`), `INCRBYFLOAT`,
`GETRANGE`, `SUBSTR`, `SETRANGE`, `SETBIT`, `GETBIT`, `BITCOUNT`/`BITPOS` (with `BitUnit.Byte|Bit`), `BITOP`
(`BitOperation`), `PFADD`/`PFCOUNT`/`PFMERGE` (`HyperLogLog*Async`), `BLMPOP`, `SMISMEMBER`
(`SetContainsManyAsync`), `SINTERCARD`, `SSCAN` (`ScanSetAsync`), `ZSCAN` (`ScanSortedSetAsync`),
`ZRANGEBYLEX`/`ZREVRANGEBYLEX` (`GetSortedSetRangeByLexAsync(..., descending, offset, count)`),
`ZRANGE ... BYSCORE [REV] [LIMIT]` (`GetSortedSetRangeByScoreAsync`), `ZCOUNT`, `ZLEXCOUNT`, `ZMSCORE`,
`ZRANDMEMBER`, `ZUNION`/`ZINTER`/`ZDIFF` and their `STORE` forms (`WEIGHTS`, `AGGREGATE`), `HRANDFIELD`,
`HINCRBYFLOAT`, `SCAN ... TYPE`, `COMMAND`/`COUNT`/`INFO`/`DOCS`, `LATENCY LATEST`, `WAIT`
(`WaitForReplicasAsync`), `AUTH` (`AuthenticateAsync`, changes the user of the whole shared connection;
a reconnect authenticates with `RuvioClientOptions` again) and `CLIENT KILL ADDR`.

Every new command is also a default member of `IRuvioClient` that sends the same words through
`ExecuteAsync(IReadOnlyList<string>, ...)`, so an existing `IRuvioClient` implementation keeps compiling
and works without changes. `RuvioClient` overrides each one with the inline `RespCommand` path: up to
five arguments are stored in the struct, integers are written straight into the pooled frame, and the
reply is read by a pooled state machine. `DELEX`, `SETNX`, `SETEX`, `PSETEX`, `GETDEL`, `INCRBYFLOAT`,
`GETRANGE`, `SETRANGE`, `GETBIT`, `SETBIT`, `BITCOUNT`, `PFADD`, `LPUSHCAP`, `RPUSHCAP`, `SMISMEMBER`,
`ZCOUNT`, `ZMSCORE`, `HINCRBYFLOAT`, `FILLGET` and `FILLSET` were added to `RespVerbs.txt`, so their name
is copied from a generated UTF-8 span. Commands with more than five arguments (for example
`SET key value IFEQ expected PX ms`) build one exact-size `string[]`; arrays are now encoded through a span
instead of the `IReadOnlyList<string>` interface.

In cluster mode `BITOP` (destination and sources), `SINTERCARD` (counted keys) and `JSON.MGET` (all but the
path) route by their real keys; `JSON.INDEX`, `JSON.FIND` and `RUVIO` are keyless.

## Helpers

`RuvioClientHelpers` adds extension methods on `IRuvioClient` for common patterns:

```csharp
// Distributed lock: SET NX PX to take it, SET IFEQ PX to renew, DELEX IFEQ to release.
// Release and renew never touch a lock that expired and was taken by another owner.
await using (RuvioLock? held = await db.TryAcquireLockAsync("lock:invoice", TimeSpan.FromSeconds(10)))
{
    if (held is null) return;                 // someone else has it
    await held.RenewAsync();                  // false when the lock was lost
}                                             // DisposeAsync -> DELEX lock:invoice IFEQ <token>
RuvioLock? waited = await db.AcquireLockAsync("lock:invoice", ttl, wait: TimeSpan.FromSeconds(2));

// Fenced lease (LEASE/RELEASE) and one semaphore slot (SEMAPHORE/RELEASE).
await using RuvioLease? lease = await db.TryAcquireLeaseAsync("lease:job", TimeSpan.FromSeconds(30));
long fencingToken = lease!.Token;
await using RuvioLease? slot = await db.TryAcquireSemaphoreAsync("sem:export", max: 4, TimeSpan.FromSeconds(30));

// Rate limit (LIMIT): (Allowed, Remaining); a refused call is not counted.
RateLimitResult decision = await db.RateLimitAsync("rl:ada", max: 100, TimeSpan.FromMinutes(1));
// Smoother: sliding window, or a token bucket that says how long to wait.
LimitOutcome bucket = await db.LimitTokensWithRetryAsync("api:key-7", 10, 1_000, 2);

// Cache-aside with stampede protection (FILLGET/FILLSET): one builder across all processes.
string page = await db.GetOrCreateAsync("cache:home", TimeSpan.FromMinutes(5), ct => RenderAsync(ct));
Product product = await db.GetOrCreateAsync("cache:p:1", ttl, LoadAsync, AppJson.Default.Product);

// Idempotency (IDEM BEGIN/COMPLETE).
IdempotencyBegin begin = await db.TryBeginIdempotencyAsync("idem:pay:1", fingerprint, owner, TimeSpan.FromMinutes(10));
if (begin.State == IdempotencyState.Acquired) { /* do it */ await db.CompleteIdempotencyAsync("idem:pay:1", fingerprint, owner, "ok"); }

// Lazy, cursor-driven scans.
await foreach (string key in db.ScanKeysAsync("user:*", count: 500)) { }
await foreach (KeyValuePair<string, string> field in db.ScanHashEntriesAsync("h")) { }
await foreach (string member in db.ScanSetMembersAsync("s")) { }
await foreach (SortedSetEntry entry in db.ScanSortedSetEntriesAsync("z")) { }

// Typed keywords instead of strings and bools; each sends a pre-encoded token. The string and bool overloads still work.
double? km = await db.GeoDistanceAsync("cities", "ankara", "izmir", GeoUnit.Kilometers);
IReadOnlyList<string> far = await db.GeoRadiusAsync("cities", 32.8, 39.9, 500, GeoUnit.Kilometers, GeoOrder.Descending);
await db.InsertListAsync("queue", InsertPosition.After, "b", "c");
await db.MoveListItemAsync("queue", "done", ListSide.Right, ListSide.Left);
ScanPage hashes = await db.ScanAsync(0, "user:*", 100, KeyType.Hash);
IReadOnlyList<long?> bits = await db.BitFieldAsync("counters",
    BitFieldStep.Overflow(BitFieldOverflow.Saturate),
    BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 10));

// Stream trim rule for XADD and XTRIM (MAXLEN or MINID, "=" or "~"), INFO sections and CLIENT LIST types.
await db.AddStreamEntryAsync("events", StreamIds.New, StreamTrim.MaxLen(10_000, StreamTrimMode.Approximate), ("type", "login"));
long removed = await db.TrimStreamAsync("events", StreamTrim.MinId("1760000000000-0"));
string keyspace = await db.GetInfoAsync(InfoSection.Keyspace);
string pubsubClients = await db.GetClientListAsync(ClientType.PubSub);
var above = await db.GetSortedSetRangeByScoreAsync("board", ScoreBound.Exclusive(10), ScoreBound.PositiveInfinity);

// "Recent items" capped list (LPUSHCAP) and Bloom "seen before" (BF.MADD / BF.MEXISTS).
await db.PushRecentAsync("recent:ada", "item-42", cap: 50);
IReadOnlyList<string> newest = await db.GetRecentAsync("recent:ada", 10);
IReadOnlyList<bool> seen = await db.SeenBeforeAsync("seen:emails", "a@x", "b@x");   // records them
IReadOnlyList<bool> maybe = await db.MaybeSeenAsync("seen:emails", "c@x");          // read-only
```

`GetOrCreateAsync` polls plain `FILLGET` with a 2–25 ms backoff while another caller builds the value.
It never uses `FILLGET ... WAIT`, because a server-side wait parks the whole shared connection (including
the builder's own `FILLSET` when it shares the client). A waiter that outlasts
`GetOrCreateOptions.WaitTimeout` (5 s) builds and stores the value itself with a plain `SET`; the fill
marker lives `FillTimeout` (10 s). With `StaleWindow`, `FILLSET ... SWR` keeps the old value readable:
callers get it at once while the single caller that receives the refresh token rebuilds. Typed JSON
helpers (`JsonGetValueAsync`, `JsonSetValueAsync`, ...) are described under *JSON documents*.

## Quick benchmark

```sh
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --quick 127.0.0.1:6379 1
```

Prints frame build+encode and reply parse cost (Stopwatch over 200k iterations after a 150k warmup,
bytes from `GC.GetAllocatedBytesForCurrentThread`), then 1 and 16 concurrent callers of GET, SET, DELEX
and LPUSHCAP on one multiplexed connection for the given seconds per case (database 11, flushed).
One run on a shared, noisy 4-core Linux VM (.NET 8.0.31, loopback, a memory-only server; repeated runs
varied by up to 2x, so treat these as orders of magnitude, not a comparison):

| Case | Result |
|---|---|
| build+encode GET / SET / DELEX / LPUSHCAP | 0 B/op; 150–560 ns/op across runs |
| build+encode `SET k v IFEQ e PX ms` (7 args, list path) | 112 B/op (one `string[]` and the TTL text); ~350 ns/op warm |
| parse `:1`, `+OK` | 0 B/op; 300–420 ns/op |
| parse `$5` bulk / `*2` integers | 88 / 96 B/op (the payload and the item list) |
| GET / SET / DELEX / LPUSHCAP, 1 caller | ~9k–18k ops/s |
| GET / SET / DELEX / LPUSHCAP, 16 callers | ~54k–93k ops/s, 137–430 B/op process-wide (includes harness and reply payload) |

## Not done / not verified

- Cluster-mode routing of the new keyed commands is covered by unit tests (`ClusterTests`) only; no live
  multi-shard run was made. In the standalone test server `CLUSTER SLOTS/NODES/SHARDS/INFO/MYID`, `READONLY`
  and `READWRITE` answer "cluster mode is disabled": the live test checks that the server answers, not the
  cluster replies.
- `ACL LOAD`/`ACL SAVE` were only verified to reach the server (no `security.acl_file` configured), `SCRIPT KILL`
  only as `NOTBUSY`, `OBJECT IDLETIME`/`OBJECT FREQ` only as the maxmemory-policy error, and `CLIENT TRACKING`
  only with `OFF` (ON needs RESP3, which this client does not speak).
- `ZRANGE ... BYSCORE REV` sends the high bound first, as Redis does. Older server builds read REV bounds
  low-first; this was fixed in `src/protocol/parse/zset.rs`, and the exact order is pinned by the scripted test.
- A `BF.MADD`/`BF.INSERT` reply with an error element (a full non-scaling filter) throws `RuvioException`
  for the whole call; the other items' results are not returned.
- Benchmarks above are one noisy run, not BenchmarkDotNet; they were not compared with a baseline.

### Serial socket profiling

On a disposable standalone server, phase profiling can be run with:

```sh
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-phases 16479 5
```

This uses serial typed GET/SET, 256-byte values, a one-second warmup and five
measured seconds per operation, and reports client CPU/allocation and the
`ruvio_client_socket_phase_duration_milliseconds` histogram's queue, encode,
write and reply means. SET repeats the same value. Metrics are enabled for the
measurement and perturb timings/allocations; compare throughput separately with
`--socket-load`. Encode timing is collected only when the histogram is enabled.
Queue includes time through encoding, and reply can overlap write: the phases
must not be summed or treated as pure network/server timings.
An optional third argument selects concurrency 1/16/128; for example,
`--socket-phases 16479 5 16`. `wallMicrosecondsPerOperation` is elapsed wall
time divided by completed operations, not concurrent request latency.

On local Apple M4 Pro/.NET 8.0.18 loopback, initial means were about 30 us total,
0.04–0.05 us encode, 0.26–0.34 us queue, 2.7–2.8 us write and 28.5–28.7 us reply.
The dominant interval includes reply waiting, parsing and async scheduling;
these data do not isolate wire latency.

The reader now rechecks its buffered scalar fast path after filling an empty
buffer, avoiding nested asynchronous parsing for a newly received complete
reply. Fragmented/array replies retain their parser and failure classification.
Local `ReplyBenchmarks` changed bulk parsing from 65.27 to 39.40 ns, integer
from 39.90 to 35.11 ns, and array from 92.08 to 94.83 ns; measured allocation
was unchanged. Instrumented serial socket rates remained about 33k ops/s, so no
end-to-end throughput gain from the reader change is established. No batching
delay or connection-wide retry was added. The subsequent coalescing experiment
and its separate concurrent write-count/CPU measurements are described below.
An instrumented concurrency-16 run measured about 4.5 us write, 36–37 us queue
and 49 us reply per phase sample. Write time can also serialize queue progress,
so these overlapping means neither prove nor rule out coalescing benefits.

### Write-count and pipeline comparison

Socket metrics additionally report `ruvio_client_socket_write_calls` and
`ruvio_client_socket_written_bytes` for successfully completed stream writes.
These count application `WriteAsync` completions and payload bytes, not TCP
packets, kernel syscalls or server execution acknowledgements. Failed writes
are not counted, even when the transport may have accepted a partial payload.

```sh
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-phases 16479 5 16
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-phases 16479 5 16 --pipeline
```

The second mode explicitly submits prebuilt groups of 16 commands through
`ExecuteManyAsync`, verifies every reply and reports per-operation CPU, write
count and average bytes per write. It is an explicit pipeline experiment, not
automatic batching of independent calls.

Local instrumented .NET 8.0.18/M4 Pro comparison (256-byte values, one shared
loopback connection, identical SET values, one warmup and five measured seconds):

| Operation | Concurrent calls ops/s | Pipeline ops/s | Writes/op, calls → pipeline | Client CPU us/op, calls → pipeline |
| --- | ---: | ---: | ---: | ---: |
| GET | 173,178 | 485,978 | 1 → 0.0625 | 55.18 → 8.04 |
| SET | 168,511 | 459,929 | 1 → 0.0625 | 58.60 → 8.30 |

GET write payload grew from 66 to 1,056 bytes; SET from 330 to 5,280 bytes.
Keys include a random run ID. This single ordered comparison is diagnostic,
not release performance evidence. Pipeline changes request grouping, completion
and round-trip scheduling as well as write count; it does not isolate syscall
cost or predict automatic coalescing gains. Instrumentation also perturbs CPU
and timing. Serial calls still send immediately; no timer or batch-fill delay
was introduced. Independent calls on one socket always coalesce already queued
frames into a pooled buffer, with at most 16 request groups per write. The
writer stops collecting after reaching 64 KiB; the last complete frame can
exceed that threshold. An initially large frame is sent separately. There is
no timer, batch-fill wait, configuration switch, or automatic transport replay.
Explicit pipelines are unchanged. Pending replies retain FIFO order; canceled
queued requests are skipped and encoding failures remain per-request errors.
Partial/failed stream writes fault the connection. Database-scoped and
reconnected sockets use the same writer.

Two alternating-order passes per variant on the same instrumented local setup
(three-second cases) measured:

| Concurrency | Operation | Before → coalesced ops/s | Writes/op after | CPU us/op before → after |
| ---: | --- | ---: | ---: | ---: |
| 1 | GET | 34,190 → 34,366 | 1 | 124.29 → 125.24 |
| 1 | SET | 34,059 → 33,254 | 1 | 121.06 → 129.35 |
| 16 | GET | 174,952 → 235,687 | 0.242 | 55.86 → 41.96 |
| 16 | SET | 173,031 → 231,472 | 0.221 | 55.44 → 42.53 |
| 128 | GET | 236,816 → 716,073 | 0.085 | 39.31 → 13.17 |
| 128 | SET | 226,779 → 704,713 | 0.073 | 42.25 → 13.64 |

These are means of short instrumented runs, not latency percentiles or release
certification. Serial throughput in these instrumented runs was essentially
unchanged/slightly lower in the SET cases.

The subsequent uninstrumented alternating comparison was interrupted by a .NET channel failure:
SET with four workers failed during warmup on .NET 8.0.18. The completed before
and after passes included a serial SET decline from 35,691 to 20,950 ops/s
(p99 0.037 to 0.080 ms), despite gains at high concurrency. This incomplete
comparison is not evidence of an established regression or a general gain.

A narrower ten-second serial SET comparison used the same binary with the
option off/on/on/off. Its two-run means were 35,965 versus 35,612 ops/s;
p99 was 0.037–0.040 ms off and 0.039 ms on. The large serial decline did not
reproduce in this isolation. A separate ten-second SET/four-worker attempt
failed again with coalescing enabled, this time during measurement, at the
initial pending-channel publication rather than inside the batching helper.
Eight later SET/four-worker cases (two per option/runtime combination on
8.0.18 and 8.0.31) completed without failure. **The channel failure remains unresolved**:
it was observed before coalescing existed, and these successful short cases
do not prove either mode or runtime immune.

Independent socket calls always coalesce. `--socket-load 16479 10 --inline set:4`
measures that path; `--socket-phases 16479 3 16` profiles it. `--pipeline` is
still the explicit pipeline, not a switch for automatic coalescing. It is a
.NET channel bug on ARM64, not a reason to disable this writer.

### Compile-time command encoding

Small typed collection commands (`LPUSH`, `RPUSH`, `SADD`, `SREM`, `HDEL`,
`HMGET`, `ZREM`) keep the key and up to four additional arguments inline.
`HSET` keeps up to two field/value pairs inline. Larger calls retain a copied
argument array; caller arrays are not retained by either factory. Public `params`
arrays are still created at the call site when applicable: this removes the
client's second, flattened array, not all allocations in the operation.

`KeyedCommandBenchmarks` measures construction plus encoding with preallocated
input pairs. On Apple M4 Pro / .NET 8.0.18 (three warmups, five measured
iterations), one-pair HSET changed from 89.49 to 67.19 ns and 56 to 0 allocated
bytes; two pairs from 126.59 to 97.32 ns and 72 to 0 bytes. Three pairs use the
array fallback (162.83 vs 164.50 ns, 88 bytes in both). These measurements exclude
the caller's input array, network and response costs.

The existing source generator reads `RespVerbs.txt` and emits both UTF-8 verb
spans and complete RESP bulk fragments (including length and CRLF). Inline typed
commands copy a generated fragment instead of measuring, formatting and encoding
their command name at runtime. Arguments still use strict UTF-8 encoding and
integer arguments are written directly. Unlisted or differently cased names,
caller-supplied command lists, and binary commands retain their generic encoders;
no runtime reflection or public API change is involved.

To measure the inline encoder without network or server costs:

```sh
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --filter '*InlineCommandBenchmarks*' --job short
```

On Apple M4 Pro / .NET 8.0.18, a local five-iteration comparison reduced encoding
time from 34.28 to 25.55 ns for GET, 45.99 to 37.26 ns for SET, 34.59 to 25.64 ns
for INCR, and 21.79 to 14.92 ns for PING, with no measured managed allocation in
either version. An unlisted inline CUSTOM command increased from 33.95 to
36.47 ns because of the lookup; public arbitrary-command list encoding is
unchanged. These are microbenchmark results, not end-to-end throughput claims.

The list now covers 43 verbs, including TTL, hash, integer and native coordination
commands. With this expanded lookup, GET remained 25.96 ns and SET 38.03 ns;
INCRBY measured 32.44 vs 42.03 ns, HGET 36.15 vs 45.05 ns, and IDEM BEGIN
68.18 vs 81.81 ns against the unchanged encoder in a separate local comparison.
All generated verbs are checked against generic encoding with zero through five
inline arguments, including UTF-8 strings and integer boundaries.

For a bounded socket comparison, use the same benchmark executable with two
client builds that differ only in the encoder:

```sh
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-load 16479 5 --inline
```

Append `get:128`, `set:16` or `limit:1` to select a single case for failure
isolation. Supported concurrency values are 1/2/4/16/128; invalid selectors fail
explicitly. Runtime comparisons should execute the same built DLL with each
runtime host, rather than rebuilding between runs.

This runs GET, SET and integer-argument LIMIT with concurrency 1/2/4/16/128,
one shared standalone connection, 256-byte GET/SET values, one-second warmups
and five-second measurements per case. SET repeats an identical value.
The benchmark uses unique keys and removes them after each successful case;
failures propagate, and case progress is written to stderr. Without `--inline`
it retains the generic list-command path. Use a disposable server.

Local loopback results were much smaller and noisier than encoder-only results.
On .NET 10.0.2, one new/old pass ranged from -2.7% to +4.2% throughput;
this does not establish an end-to-end improvement. On .NET 8.0.18, one generated
pass completed but two subsequent passes failed in
`System.Threading.Channels.AsyncOperation.SignalCompletion` with an unexpected
state exception; two unchanged-encoder passes completed. The cause is unresolved,
and the successful newer-runtime pass does not establish .NET 8 reliability.
Five subsequent .NET 8 passes completed 75 cases and about 31.6 million
measured operations without reproducing the exception. Added inline/list request
ownership stress also passed; neither result resolves the earlier failures.
The harness logs runtime, run ID, operation/concurrency and warmup/measurement
phase to stderr so a future failure can be located precisely.
Four further alternating 8.0.18/8.0.31 passes completed without failure on either
runtime; a current servicing runtime alone has not been shown to fix the issue.
Treat the channel failure above as open
before treating this optimization as release-ready.

Reads start with `Get`. Writes use the mutation: `Set`, `Add`, `Remove`, `Delete`, `Increment`. A range that returns values is `GetSortedSetRangeByRankWithScoresAsync`.

```csharp
using Ruvio.Client;

await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
RuvioClient db = await connection.GetDatabaseAsync(0);
await db.SubscribeAsync("news", msg => Console.WriteLine(msg.Payload));
await db.PublishAsync("news", "hello");
await db.SubscribeExpiredAsync(msg => Console.WriteLine($"Expired: {msg.Payload}"));
await db.SubscribeEvictedAsync(msg => Console.WriteLine($"Evicted: {msg.Payload}"));
await db.AddSortedSetAsync("leaderboard", ("player2", 50), ("player1", 100));
IReadOnlyList<SortedSetEntry> rows = await db.GetSortedSetRangeByRankWithScoresAsync(
    "leaderboard",
    0,
    -1,
    descending: true);
```

`SubscribeExpiredAsync` and `SubscribeEvictedAsync` listen for keyspace events in
the client's fixed logical database. They use exact sharded
channels internally; the event key is the message payload. Enable the matching
server-side `notify_keyspace_events` classes (`x` and/or `e`, plus `E` for
keyevent channels), for example `notify_keyspace_events = "Exe"` in
`ruvio.toml`. In multi-shard deployments, key-event delivery is limited to the
shard receiving the subscription.

## JSON documents

Three ways to write a field; pick whichever fits:

```csharp
// 1. Raw JSON text (what the server speaks)
await client.JsonSetAsync("user:1", "$.addr", "{\"city\":\"Izmir\"}");

// 2. Any value; the client serializes it ("izmir" is sent as "izmir" with quotes)
await client.JsonSetValueAsync("user:1", "$.city", "Izmir");
await client.JsonSetValueAsync("user:1", "$.age", 31);
await client.JsonSetValueAsync("user:1", "$.addr", new Address("Izmir", 35000));

// 3. Your own serializer settings: per call, or once for the client
var camel = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
await client.JsonSetValueAsync("user:1", "$.addr", addr, camel);
await client.JsonSetValueAsync("user:1", "$.addr", addr, AppJson.Default.Address); // source-generated, AOT-safe
// RuvioClientOptions.JsonSerializerOptions = camel;  // default for calls that pass none

Address? a = await client.JsonGetValueAsync<Address>("user:1", "$.addr");
```

`JsonMergeValueAsync` and `JsonArrayAppendValueAsync` work the same way. The overloads that take `JsonSerializerOptions` use reflection (marked for trimming); pass a `JsonTypeInfo<T>` for trimmed or AOT apps.

## Connecting

Native idempotency is exposed as `BeginIdempotencyAsync(key, fingerprint, owner,
ttlMs)` and `CompleteIdempotencyAsync(key, fingerprint, owner, result)`.
Begin returns a RESP array (`acquired`, `pending`, `conflict`, or `completed` plus
result bytes); complete returns a boolean acknowledgment. The completion helper
takes text; `ExecuteBinaryAsync` supports arbitrary binary tokens/results with
`IDEM key BEGIN ...` / `IDEM key COMPLETE ...`. No Lua is loaded.
Only `acquired` authorizes the caller to execute its operation. Neither replay
nor completion renews retention, and no ambiguous write is automatically retried.
See [the native contract](../../../docs/IDEM.md) before using it for
external side effects.

Give the client one address. On connect it sends `CLUSTER SLOTS`. A standalone server replies with an error and the client keeps that one socket. A sharded server returns the slot map; the client then opens one socket per shard and sends each command to the shard that owns the key. A `MOVED` reply points that slot at the node it names and the command is sent there; the whole map is read again only when that node is unknown, or on the command's second and third `MOVED`. A fourth `MOVED` is returned as the error. An `ASK` reply sends `ASKING` and the command together to the node it names and returns that reply; the slot map is not changed, so the next command for the slot goes to its owner again. `MOVED` and `ASK` share one budget of three redirects per command. A `TRYAGAIN` reply sends the command again, routed by the current map, after 5, 10, 20, 40, 80, 160 and 320 ms (635 ms in all); the eighth `TRYAGAIN` is returned as the error. Multi-key reads and writes (`MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TOUCH`) are split by slot and merged, in a pipeline too: the merged reply sits at that command's index. Any other command whose keys span slots (`RENAME`, `RENAMENX`, `COPY`, `SMOVE`, `LMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ...) fails with a `CROSSSLOT` `RuvioException` before anything is sent; use a hash tag such as `{user:1}` to keep related keys in one slot. `FLUSHDB`, `FLUSHALL`, and `DBSIZE` go to every shard.

```csharp
await using var clusterConnection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
RuvioClient cluster = await clusterConnection.GetDatabaseAsync(0);
await cluster.SetAsync("session:ada", "hello");
await cluster.SetAsync("somekey", "on-another-shard");
IReadOnlyList<string?> values = await cluster.GetManyAsync("session:ada", "somekey");
```

`RuvioClient.GetHashSlot(key)` is the same 0–16383 slot Ruvio uses, including `{hash tags}`. Set `RuvioClientOptions.DiscoverCluster` to `false` to stay on the seed socket.

### Seed addresses

`RuvioClientOptions.Seeds` is a list of `host:port` strings (an IPv6 host in brackets, `[::1]:6379`). It is empty by default; when it is not empty it replaces `Host` and `Port`, which keep working unchanged when no seeds are given. An empty host or a bad port is an `ArgumentException` / `ArgumentOutOfRangeException`.

```csharp
await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Seeds = ["10.0.0.5:6379", "10.0.0.6:6379", "10.0.0.7:6379"],
});
```

Connect tries the seeds in order, one attempt each, and keeps the first that answers. A seed that cannot be reached (refused, timed out) is skipped; a refused `AUTH` stops at once; when none answers, the last connect error is thrown. On a standalone server, each reconnect attempt tries every seed once, starting with the one after the address that dropped and ending with it, and `ReconnectDelay` / `ReconnectBaseDelay` run between attempts. The command that hit the drop still fails. A subscriber socket opened later starts at the address the command socket is on, and `ConnectionLost` / `ConnectionRestored` report the actual address.

On a cluster, `CLUSTER SLOTS` comes from the first reachable seed. A slot map reload (after a `MOVED` to an unknown node, or when a node cannot be opened again) asks the nodes with an open socket first; when none answers, it opens every other known node and then every seed, one attempt each, until one returns a slot map. The command goes to the owner the new map names, and a command without a key goes to the first reachable node of the new map. Seeds are never asked while the known nodes answer. A pipeline that hits a node that cannot be opened fails; the next command reloads the map.

### Following a failover

With two or more seeds a standalone client finds the primary again after a failover. On a reconnect it asks the seeds `ROLE` and connects to the one that answers `master` (a replica's reported primary is tried too); if none does it falls back to the first seed that answers. When a command is answered `READONLY` (the node was demoted) it does the same and sends that command once more to the new primary (the node that answered is skipped only while it still says `master`; once it says it is a replica, a later round takes it when it is promoted): `READONLY` is sent before the command runs, so the retry cannot apply it twice. It is not sent again inside a transaction the client opened, or in a pipeline, and a second `READONLY` is returned as the `RuvioException`. The rounds wait with the reconnect backoff (`MaxReconnectAttempts`, `ReconnectBaseDelay`, `ReconnectMaxDelay`); with `Reconnect = false` there is one round. Nothing is sent while the connection is healthy, and a single address behaves as before. A cluster client keeps its `MOVED`/`ASK` rules. `FollowPrimary = false` turns it off.

With `SentinelMaster` set the seeds are coordinator (or Sentinel) addresses: the client asks `SENTINEL get-master-addr-by-name <name>`, checks the answer with `ROLE`, and connects there, at connect time, on reconnect and after `READONLY`. Connect throws a `RuvioException` naming the master when no coordinator names a primary. The same `AUTH` credentials go to the coordinators and the data node. The rules are the same in every client ([docs/CLIENT_BEHAVIOR.md](../../../docs/CLIENT_BEHAVIOR.md#following-a-failover)).

```csharp
var options = new RuvioClientOptions { Seeds = ["10.0.0.5:6379", "10.0.0.6:6379"] };
// Or with coordinators: the seeds are their addresses.
var viaSentinel = new RuvioClientOptions { Seeds = ["10.0.0.9:26379"], SentinelMaster = "main" };
```

`FailoverLiveTests` needs the pair `scripts/run_with_servers.sh` starts (`RUVIO_FAILOVER_ADDRS`, `RUVIO_FAILOVER_REPLICATION`) and returns early without it; `FollowFailoverTests` uses scripted nodes and needs nothing.

### Reads from replicas

`RuvioClientOptions.DurabilityReplicas` (with an optional `DurabilityTimeout`) makes every connection the client opens send `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]` right after `AUTH`, so the server holds that connection's replies until `n` replicas have the writes on disk; a write the replicas do not confirm in time fails with `NOREPLICAS` and stays applied. A server without replication refuses it and the connect fails. The default sends nothing. `SetDurabilityAsync(replicas, timeout)` sets or clears it on one connection.

`RuvioClientOptions.ReadPolicy` (`ReadPolicy.Primary`, `PreferReplica` or `Replica`) chooses where a cluster client sends reads. A standalone connection ignores it. `CLUSTER SLOTS` lists each shard's replicas after its primary; when the slot map is loaded, the client picks one replica per shard at random and keeps it until the next load. Each replica socket sends `READONLY` once, right after it is opened.

```csharp
await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = "10.0.0.5",
    ReadPolicy = ReadPolicy.PreferReplica,
});
```

- `Primary` (the default): every command goes to the slot's primary, and no replica socket is opened.
- `PreferReplica`: a read goes to the shard's replica when one is known and reachable, otherwise to the primary.
- `Replica`: a read goes to the shard's replica; with none known or reachable it throws a `RuvioException` ("no replica ...").

Replication is asynchronous: a read from a replica may miss recent writes, including your own. A command is a read when the server flags it `readonly` and not blocking (`GET`, `MGET`, `EXISTS`, `HGETALL`, `ZRANGE`, ...). Writes, commands without a key, transactions, Pub/Sub, `SCAN` and the commands that go to every shard, and blocking commands go to the primaries as before. The parts of a split `MGET` or `EXISTS` are reads; the parts of `DEL` or `MSET` are writes. A pipeline goes to replicas only when every command in it is a read; otherwise the whole pipeline is routed as before.

A replica that cannot be opened, or that answers a read with `LOADING` (it is still copying its primary), is skipped until the next slot map load. Under `PreferReplica` the command goes to the primary (a `LOADING` read is sent again there); under `Replica` the open error is thrown, or the `LOADING` error is returned as a `RuvioException`. A socket that drops after a command was sent still fails that command; it is not sent again.

### Concurrent commands

Command sockets multiplex concurrent calls: one writer sends complete RESP frames
without waiting for their replies, and one reader matches replies to callers in FIFO
order. Each shard/database socket has its own queues. Pub/Sub still uses dedicated
sockets. Reuse the connection rather than opening one connection per request.
Keep argument lists and binary argument memory unchanged until the call completes.

The writer is demand-driven: an atomic reservation lets the first caller start
writing directly when there are no scheduled writes, bypassing the outgoing
channel. Concurrent producers enqueue behind that owner. The owner drains queued
frames and releases ownership only after all reserved writes have been consumed;
it waits asynchronously if a producer has reserved a slot but not yet published.
There is no idle writer task, spin loop, per-request worker or reader-mode switch.
Pending replies do not prevent direct writing when the writer is idle.

The read side works the same way. A caller whose call cannot be cancelled and that finds
nobody reading reads the socket itself until its own reply is in, completing the requests
queued before it on the way; its reply then needs no hand-off from a reader to the thread
pool. Otherwise a background reader drains the reply queue and stops when it is empty.

`RuvioClientOptions.MaxInFlightRequests` defaults to 256 and must be positive. It
limits admitted command groups per socket, including queued writes and pending
replies; one `ExecuteManyAsync` pipeline is one group. Additional calls asynchronously
wait for capacity. This is not a byte limit: large arguments, replies, and pipelines
still need appropriately sized application limits. Ordinary commands are encoded
by the writer into pooled buffers that are returned after writing, not after the
server replies. Raw frames and cancellable binary/string argument lists are copied
after admission so caller-owned memory can be reused after cancellation.

`RuvioClientOptions.DedicatedReaderThread` (off by default) instead gives each command socket
blocking I/O and a reader thread of its own: the caller that owns the writer writes its
frames synchronously, and the thread reads the replies and completes the callers, who
resume on the thread pool. This skips the runtime's async socket engine and one thread-pool
hand-off per round trip. On a 4-shard local server (Apple M4 Pro) it cut user CPU per
request by about 45% for 16-command pipelines on one socket and by 25-35% for pipelines
split over the shards; Linux was not measured. The cost is one thread per socket (one per
shard in a cluster), and a write that fills the socket buffer blocks its caller until the
server reads. Pub/Sub sockets keep async I/O.

The queues use bounded `Channel<T>` and admission uses `SemaphoreSlim`; this design
is not lock-free. There is no per-request socket write lock, linked
`CancellationTokenSource`, timer, dedicated thread (unless `DedicatedReaderThread` is
on), or `Task.Run`.
Caller continuations run asynchronously rather than inside the reply reader.
The `socket.queue` trace/histogram covers admission, writer queueing and encoding.
`socket.write` covers the socket write. `socket.reply` covers publication to the
reply queue through completion of parsing, including pipelined waiting and the
socket write; it overlaps `socket.write` and is not network-only time.

Cancellation observed by the writer before dispatch skips the command.
After dispatch it cancels the caller's wait, not the shared socket: the reader must
still consume that command's replies. Cancellation does not prove a mutation was
not executed. A transport failure fails pending calls; ambiguous mutations are not
automatically replayed. Do not interleave connection-scoped commands such as
`MULTI`/`EXEC` or blocking commands with unrelated concurrent calls. Send a complete
transaction in one `ExecuteManyAsync` group or use a separate connection.

The library is marked AOT-compatible. `Ruvio.Client.Tour` publishes a native binary (`PublishAot`). Command names such as `GET` and `GEOADD` are UTF-8 spans generated from `RespVerbs.txt`, so a standalone `GET` or `INCR` does not allocate an argument array for the verb.

Client CPU cost, separate from socket time, is in `Ruvio.Client.Benchmarks` (BenchmarkDotNet). It writes a `GET` and a `GEOADD` frame, a pipeline of 32 `GET`s, and reads an integer, a bulk, and an array on a reused reader. It also hashes a plain key and a hash tag.

```bash
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks
```

For a short live socket comparison, start a dedicated loopback server, then run:

```bash
ruvio --address 127.0.0.1:16389 --durability memory --data-dir /tmp/ruvio-client-load
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-load 16389 3
```

This writes a JSON summary for GET (256-byte values), SET (256-byte values), and
LIMIT at 1, 2, 4, 16, and 128 concurrent callers. Each case warms up for one second and
measures for three seconds. It includes throughput, mean/p50/p95/p99, process-wide
allocated bytes per operation, GC counts, CPU time, end-of-case working set and
the most recent GC's heap size. Latency buckets have 1-microsecond resolution,
with a 100-ms overflow bucket; allocation totals include the load harness.
Working set is not peak memory, and heap size is not a live retained-size measurement.
Run the same command in separate client processes before/after a change with the
same server and no other benchmark/build running. This is a closed-loop client
test, not an ASP.NET middleware benchmark or a Redis comparison.

A connection owns the command clients it creates. Each standalone database client
uses a dedicated command connection so commands for separate databases never race
through mutable `SELECT` state. The connection is the only object that should be
disposed:

```csharp
await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
IRuvioClient cache = await connection.GetDatabaseAsync(2);
IRuvioClient sessions = await connection.GetDatabaseAsync(3);
IRuvioClient main = await connection.GetDatabaseAsync(0);

await using var securedConnection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = "10.0.0.5",
    Port = 6379,
    Username = "app",
    Password = "secret",
    Database = 4,
    ConnectTimeout = TimeSpan.FromSeconds(2),
});
IRuvioClient secured = await securedConnection.GetDefaultDatabaseAsync();
```

The initial connection opens the configured `RuvioClientOptions.Database`; other
databases are created on demand and cached by the owning connection. A nonzero
database sends `SELECT` on its own socket. Indexes run from 0 to the server's
`databases` setting minus one (16 by default, at most 256). Cluster mode has only
database 0; requesting another index throws.

Database clients are fixed to the database they were created for. `SELECT` is
not exposed on the typed API and is rejected through raw command and pipeline
methods. Disposing the owning connection invalidates and closes every client and
socket it created.

## Interfaces

`RuvioConnection` is the resource owner and `RuvioClient` is its database-scoped command client:

- `IRuvioConnection` creates/caches clients through `GetDatabaseAsync` and owns endpoint/status information, reconnect, lifecycle events, and disposal.
- `IRuvioClient` exposes the commands and immutable database/endpoint information. Applications pass it to command-oriented services; disposing the connection invalidates all clients.

`Ruvio.Client.AspNetCore` registers one connection owner and its configured
default `IRuvioClient`; both share the same DI lifetime boundary.

`FlushDatabaseAsync`, `FlushAllDatabasesAsync`, `SwapDatabasesAsync`, and `MoveKeyAsync` send `FLUSHDB`, `FLUSHALL`, `SWAPDB`, and `MOVE`. See `docs/LOGICAL_DATABASES.md`.

## When Ruvio is unreachable

The client throws when it cannot reach Ruvio (`SocketException`, `IOException`, `TimeoutException`, or a `RuvioException` with
`IsServerReply == false`). Applications that sit on top of it, such as the .NET integrations (caching, rate limiting,
idempotency, locks, output caching), often want to decide what to do instead of failing the request. The client ships the
pieces for that:

- `RuvioOutageMode` (`Throw`, `FailClosed`, `FailOpen`) and `RuvioOutageOptions`, the `Outage` option of each integration.
  `RuvioOutageOptions.IsOutage(exception, token)` tells an outage (unreachable server, dropped connection, timeout,
  open circuit, `LOADING`, `MASTERDOWN`, `CLUSTERDOWN`) from a server error reply like `WRONGTYPE`, which is never an outage.
- `RuvioCircuitBreaker`: after `FailureThreshold` outage failures in a row it opens and `TryAcquire()` returns false for
  `BreakDuration`; then one trial call goes through and its outcome closes or reopens the circuit. Healthy, it costs a volatile read.

```csharp
var outage = new RuvioOutageOptions(RuvioOutageMode.FailOpen)
{
    CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 5, BreakDuration = TimeSpan.FromSeconds(30) },
};

if (!outage.TryEnter())
    return Fallback();                                   // the circuit is open: Ruvio is not called

try
{
    var value = await client.GetAsync(key, token);
    outage.Succeeded();
    return value;
}
catch (Exception error) when (outage.Failed(error, token) && outage.Mode == RuvioOutageMode.FailOpen)
{
    return Fallback();                                   // Ruvio is away; other errors still propagate
}
```

See [the integrations README](../../../integrations/dotnet/README.md#when-ruvio-is-unreachable) for what each package does.

## Options

| Command | Methods |
|---|---|
| `SET` | `SetAsync` (`PX`, `NX`, `XX`), `SetAsync(key, value, DateTimeOffset)` (`PXAT`), `SetKeepingTimeToLiveAsync` (`KEEPTTL`), `SetAndGetAsync` (`GET`) |
| `LPOP` / `RPOP` | one element or a count |
| `SPOP` / `SRANDMEMBER` | one member or a count; a negative `SRANDMEMBER` count may repeat |
| `HSCAN` / `SCAN` | `MATCH` and `COUNT` |
| `ZADD` | `(member, score)` tuples, `SortedSetEntry`, or one `member, score`; `SortedSetAddFlags`: `NX`, `XX`, `GT`, `LT`, `CH`; `IncrementSortedSetAsync(..., flags)` sends `INCR` |
| `HEXPIRE` family | `ExpireHashFieldsAsync(key, ttl, [condition,] fields)` sends `HEXPIRE` for whole seconds, otherwise `HPEXPIRE`; `FieldExpireCondition`: `NX`, `XX`, `GT`, `LT`. `ExpireHashFieldsAtAsync` sends `HEXPIREAT` or `HPEXPIREAT`. One reply per field |
| `ZPOPMIN` / `ZPOPMAX` | optional count; `PopSortedSetMinBlockingAsync` / `PopSortedSetMaxBlockingAsync` send `BZPOPMIN` / `BZPOPMAX` for one key or a key list |
| `XADD` | `AddStreamEntryAsync(key, id, maxLength, ...)` sends `MAXLEN` |
| `XRANGE` / `XREVRANGE` | optional `COUNT` |
| `XREAD` / `XREADGROUP` | `StreamReadOptions`: `COUNT`, `BLOCK`, `NOACK` (group reads only) |
| `XGROUP CREATE` | `createStream: true` sends `MKSTREAM` |
| `XPENDING` | summary, or the extended form with `IDLE`, a range, a count, and a consumer |
| `XCLAIM` | `StreamClaimOptions`: `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `LASTID`; `ClaimStreamEntryIdsAsync` sends `JUSTID` |
| `INFO` | optional section; `prometheus` returns Prometheus text |
| `FUNCTION LOAD` | `LoadFunctionAsync(name, body)`; the body is plain Lua and is lost on restart |
| `SET ... IFEQ` | `SetIfEqualAsync(key, value, expected, expiry?)` (`PX`), `SetIfEqualKeepingTimeToLiveAsync` (`KEEPTTL`); false when the stored value differed |
| `SCAN` | `ScanAsync(cursor, pattern, count, type)` sends `TYPE` |
| `SSCAN` / `ZSCAN` | `MATCH` and `COUNT` |
| `BITCOUNT` / `BITPOS` | optional range with `BitUnit.Byte` or `BitUnit.Bit` |
| `ZRANGEBYLEX` / `ZREVRANGEBYLEX` / `ZRANGE BYSCORE` | `descending`, `LIMIT offset count` (a negative count sends no `LIMIT`) |
| `ZUNION` / `ZINTER` (+`STORE`) | `WEIGHTS` (one per key) and `SortedSetAggregate` (`SUM`, `MIN`, `MAX`); reads send `WITHSCORES` |
| `ZRANDMEMBER` / `HRANDFIELD` | one, a count, or a count `WITHSCORES` / `WITHVALUES` |
| `SINTERCARD` | `LIMIT` |
| `BF.INSERT` | `BloomInsertOptions`: `CAPACITY`, `ERROR`, `EXPANSION`, `NOCREATE`, `NONSCALING` |
| `JSON.FIND` | `COUNT`, `AFTER` (page with the last key) |
| `JSON.ARRINDEX` / `JSON.ARRPOP` | optional `start stop` / `index` |

A blocking read (`BLPOP`, `XREAD BLOCK`, `XREADGROUP BLOCK`) holds the connection until it returns. Use a separate client for it.

Subscribe opens a **second TCP socket**. `GET` / `SET` / `PUBLISH` stay on the command socket. Register `Message` or `SubscribeAsync(channel, handler)` (StackExchange / Garnet app style), or `await foreach` `ListenAsync`. `ReadPubSubMessageAsync` still works. `IsConnected` is false when every command socket is broken.

A dropped socket is opened again before the next command, including a shard socket in cluster mode. The command that discovers the drop fails and is never sent again, so a write is not applied twice; that includes reads and pipelines. `RuvioException.ClosedBeforeReply` tells you no byte of the reply arrived, so the caller can resend a read. Pub/Sub channels are subscribed again on the new socket. `ConnectionLost` and `ConnectionRestored` report that, and `ConnectionFailed` still fires on the drop. Set `RuvioClientOptions.Reconnect` to false and call `ReconnectAsync` yourself, or set `ReconnectDelay` to own the wait. `ReconnectDelay` returning null stops the retry.

A password is sent as `AUTH` only when `RuvioClientOptions.Password` is set. One client runs commands one at a time. `ExecuteManyAsync` pipelines several commands on that connection. A server error reply leaves the connection usable. A broken read or write closes it.

## TLS

Plain TCP is the default. Set `RuvioClientOptions.Tls` to turn TLS on for every socket the client opens (reconnects, shards, replicas and Pub/Sub included). The certificate chain is checked against the system trust store and must name the host you connect to.

```csharp
var roots = new X509Certificate2Collection();
roots.ImportFromPemFile("ca.pem");

await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = "cache.internal",
    Tls = new RuvioTlsOptions { TrustedCertificates = roots },
});
```

`TrustedCertificates` trusts only your private CA (revocation is not checked then); leave it null for a certificate from a public CA. `ClientCertificates` answers a server that demands mutual TLS. `ServerName` is the name the certificate carries, when you connect by IP address. `InsecureSkipVerify` accepts any certificate and removes the protection against someone in the middle; use it for a throwaway test only. The handshake is part of the connect: a refused certificate throws `AuthenticationException`. With TLS on, `DedicatedReaderThread` has no effect. Measured: the live TLS tests (`TlsLiveTests`); the latency TLS adds: not measured.

## Changes

See [CHANGELOG.md](https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/CHANGELOG.md).
