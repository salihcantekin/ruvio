<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">ruvio-client <sub>Python</sub></h1>

<p align="center">Sync and asyncio client for <a href="https://ruviodb.com/">Ruvio</a>. No runtime dependencies, typed.</p>

<p align="center">
  <a href="https://pypi.org/project/ruvio-client/"><img alt="PyPI" src="https://img.shields.io/pypi/v/ruvio-client?label=PyPI"></a>
  <a href="https://pypi.org/project/ruvio-client/"><img alt="Downloads" src="https://img.shields.io/pypi/dm/ruvio-client?label=downloads"></a>
  <a href="https://pypi.org/project/ruvio-client/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/ruvio-client?label=python"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/python/">Python guide</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Python client for [Ruvio](https://ruviodb.com/). It has no runtime
dependencies and needs Python 3.9 or newer. It ships type information (`py.typed`).

- `ruvio.Ruvio` is the synchronous client. It uses one socket and a reusable `recv_into` buffer.
- `ruvio.asyncio.Ruvio` is the asyncio client. It keeps one multiplexed, pipelined connection
  that all coroutines share.
- There is a typed method for each of the **298** commands in `docs/compatibility-matrix.json`.
  The methods also cover the options that change Ruvio's behaviour: `SET … IFEQ`,
  `INCRBY … MAX`, `FILLGET … WAIT`, `FILLSET … SWR`, `GETEX` in milliseconds,
  `DELEX key IFEQ v`, `LPUSHCAP`/`RPUSHCAP`, `BF.*`, `JSON.*` (including `JSON.INDEX` and
  `JSON.FIND`), `IDEM BEGIN/COMPLETE`, `LEASE`/`RELEASE`, `SEMAPHORE`, `ONCE`, `TAKE`, `TRANSFER`/`HTRANSFER`, `SETV`,
  `CHANGES`, `LIMIT` and `XADD … DELAY`.
- Pipelines, MULTI/EXEC transactions, Pub/Sub on a dedicated connection, and RESP3 through
  `HELLO 3`, including push messages.
- Helpers: lock, lease and semaphore scopes, rate limiter, `get_or_create`, idempotency,
  scan iterators, typed JSON, "recent items", and Bloom "seen before".

Licensed under the MIT license. See `LICENSE`.

## Install

```bash
pip install ruvio-client                # import name: ruvio
pip install "ruvio-client[hiredis]"     # optional: hiredis speeds up reply parsing
pip install ./clients/python            # the repository version
```

## Quick start

```python
import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379, db=0)        # or ruvio.Ruvio.from_url("ruvio://host:6379/0")
r.set("greeting", "hello", px=60_000)            # True
r.get("greeting")                                # 'hello'  (None when missing)
r.set("greeting", "hi", ifeq="hello")            # Ruvio: write only if the value matches
r.set("watermark", 1700000000, ifgt=True)         # Ruvio: write only if greater than the stored number (iflt for smaller)
r.incrby("stock", 5, max=100)                    # Ruvio: None (nothing written) past the limit
r.delex("greeting", "hi")                        # Ruvio: delete only if the value matches
r.raw.get("greeting")                            # bytes, on the same connection
```

```python
import asyncio
from ruvio.asyncio import Ruvio

async def main():
    async with Ruvio("127.0.0.1", 6379) as r:     # connects lazily if you skip `async with`
        await r.set("k", "v")
        values = await asyncio.gather(*(r.get("k") for _ in range(1000)))  # one socket

asyncio.run(main())
```

Replies are converted to Python values. A bulk string becomes `str`, or `bytes` on
`client.raw` and with `decode_responses=False`. Integers become `int` and scores become
`float`. Nil becomes `None`. `OK` becomes `True` for commands whose only answer is OK.
`HGETALL` returns a `dict`, `SMEMBERS` a `set`, and `ZRANGE … withscores=True` a list of
`(member, score)`. Streams use `StreamEntry(id, fields)`, `FILLGET` uses
`FillResult(state, value, token)` and `IDEM BEGIN` uses `IdemResult(state, result)`.

A string that is not valid UTF-8 comes back as a `str` with surrogate escapes. It
round-trips: send it back and the server stores the original bytes. Use `client.raw` when
you want `bytes`.

Server errors raise `ruvio.ResponseError` or a more specific class: `WrongTypeError`,
`NoScriptError`, `NoGroupError`, `BusyError`, `ExecAbortError` or `MovedError`. Each one has
a `.code` such as `"WRONGTYPE"`. Socket problems raise `ruvio.ConnectionError` or
`ruvio.TimeoutError`. The sync client's `socket_timeout` is 30 seconds by default (`None` waits
forever); a blocking command such as `blpop(key, 5)` gets its own wait on top, and `0` (wait forever)
has no limit. See [Timeouts](../README.md#timeouts).

For any other command, `execute_command("CMD", arg, …)` sends it as written and returns the
raw reply.

### Typed keyword constants

Keyword arguments have `str` enums: `GeoUnit` (`m`, `km`, `ft`, `mi`), `GeoOrder`, `InsertPosition`, `ListSide`, `ExpireCondition`, `BitOperation`, `BitUnit`, `Aggregate`, `KeyType` (for `SCAN TYPE`), and for `BITFIELD` `BitFieldOp`, `Overflow`, `signed(bits)` / `unsigned(bits)`. A member is a `str`, so plain strings keep working and send the same bytes; the encoder uses each member's pre-encoded bytes, with no per-call cost.

```python
from ruvio import GeoUnit, GeoOrder, KeyType, ListSide

r.geosearch("cities", member="A", radius=5, unit=GeoUnit.KILOMETERS, sort=GeoOrder.ASC)  # same as unit="km", sort="ASC"
r.scan(0, type=KeyType.HASH)
r.lmove("src", "dst", ListSide.LEFT, ListSide.RIGHT)
```

More typed helpers: `InfoSection` (`r.info(InfoSection.MEMORY_BREAKDOWN)`), `ClientType` (`r.client_list(ClientType.PUBSUB)`), `ChangeType` with `change_types(...)` for `CHANGES SUBSCRIBE ... TYPES`, `StreamId` (`NEW_ENTRY`, `LAST`, `NEW_MESSAGES`, `MIN`, `MAX`), `ScoreBound` / `LexBound` with `score_exclusive(x)`, `lex_inclusive(m)`, `lex_exclusive(m)`, and `StreamTrim` for `XADD` / `XTRIM`:

```python
from ruvio import StreamTrim, TrimMode

r.xadd("events", {"type": "signup"}, trim=StreamTrim.maxlen(10_000, TrimMode.APPROXIMATE))  # MAXLEN ~ 10000
r.xtrim("events", trim=StreamTrim.minid("1700000000000-0"))                                 # MINID = ...
```

### TTL on create

Commands that can create a key take an optional keyword-only `ttl` (seconds as `int` or `float`, or a `timedelta`): `incr`, `decr`, `incrby`, `decrby`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `zadd`, `geoadd`, `xadd`, `json_set`, `json_set_obj` and `bf_insert`. The deadline is set only when the call creates the key; an existing key keeps the deadline it has. Whole seconds are sent as `EX`, a sub-second part as `PX`. Zero, negative or non-numeric values raise before anything is sent.

```python
r.incr("rate:user:7", ttl=60)                      # the 60 s window starts on the first call
r.incrby("quota:day", 1, max=100, ttl=86400)       # counter with a ceiling and a daily window
r.zadd("recent", {"a": 1}, ttl=timedelta(hours=1))
```

`expire`, `pexpire`, `expireat` and `pexpireat` take `condition=` (`ExpireCondition.NX`, `XX`, `GT` or `LT`, or the string). A key without a deadline counts as infinite; a failed condition returns `False`.

```python
r.expire("session", 3600, condition=ExpireCondition.NX)   # only when the key has no deadline yet
```

### Pipelines and transactions

```python
p = r.pipeline()                       # async: await p.execute()
p.set("a", 1).incr("a").get("a")
p.execute()                            # [True, 2, '2']
t = r.pipeline(transaction=True)       # MULTI … EXEC, one write
t.set("x", "1").append("x", "2")
t.execute()                            # [True, 2]
r.transaction(lambda pipe: pipe.set("bal", int(r.get("bal")) - 1), "bal")  # WATCH + retry
```

By default, `execute(raise_on_error=True)` raises the first error. Pass
`raise_on_error=False` to get the exceptions back in their places in the result list.
Ruvio rejects an unknown command while it is being queued and still runs the rest of the
block. The client keeps the rejection at that command's index.

On the async client, `transaction()` opens a dedicated connection, because WATCH applies
to the whole connection.

### Pub/Sub (on its own socket)

```python
ps = r.pubsub(ignore_subscribe_messages=True)    # async: ps = await r.pubsub()
ps.subscribe("news"); ps.psubscribe("ev.*"); ps.ssubscribe("shard-chan")
for msg in ps.listen():                          # async: async for msg in ps.listen()
    print(msg.kind, msg.channel, msg.data, msg.pattern)
```

When the socket drops, the next `get_message` reopens it and sends the subscriptions again.

### RESP3

`r.hello(3)` switches the connection to RESP3. The typed converters accept RESP3 maps,
doubles and nested pairs. Out-of-band pushes, such as `CLIENT TRACKING` invalidations, go
to `on_push=callback` and never reach a command's reply.

## Helpers

| Helper | Built on | Notes |
| --- | --- | --- |
| `r.lock(name, ttl_ms)` (`with` / `async with`) | `SET NX PX`; renew `SET token IFEQ token PX`; release `DELEX name IFEQ token` | Never deletes another holder's lock. `acquire(blocking, timeout)`, `renew()`, `release()`, `owned()` |
| `r.lease_scope(key, ttl_ms)` | `LEASE` / `RELEASE` | Yields the fencing token and releases it on exit |
| `r.semaphore_scope(key, max, ttl_ms)` | `SEMAPHORE` / `RELEASE` | Raises `LockError` when full |
| `r.rate_limit(key, max, window_ms)` | `LIMIT` | `(allowed, remaining)` |
| `r.limit_sliding(key, max, window_ms)`, `r.limit_tokens(key, capacity, refill_ms, cost=1)` | `LIMIT ... SLIDING`, `LIMIT ... TOKENS` | remaining, or `None` |
| `r.limit_sliding_with_retry(...)`, `r.limit_tokens_with_retry(...)` | `LIMIT ... WITHRETRY` | `(allowed, remaining, retry_after_ms)` |
| `r.get_or_create(key, factory, ttl_ms, fill_ms=…, swr_ms=…)` | `FILLGET` / `FILLSET` | Exactly one caller runs `factory` (sync or async). Waiters poll plain `FILLGET` with backoff from 5 ms up to 100 ms instead of `WAIT`, which would park the shared connection |
| `r.idempotent(key, fingerprint, ttl_ms, fn)` | `IDEM BEGIN` / `COMPLETE` | `("executed", result)`, `("completed", stored)`, `("pending", None)` or `("conflict", None)` |
| `r.scan_iter()` / `hscan_iter` / `sscan_iter` / `zscan_iter` | `SCAN` family | Lazy and driven by the cursor; async iterators on the asyncio client |
| `r.json_set_obj` / `json_get_obj` / `json_mget_obj` | `JSON.SET` / `JSON.GET` / `JSON.MGET` | Uses the standard `json` module |
| `r.push_recent(key, value, keep)` | `LPUSHCAP` | Newest first, keeps at most `keep` items |
| `r.seen_before(key, *items)` | `BF.MADD` | Returns one `bool` per item: was it (probably) seen before this call |

## Performance design

The design follows the C# client:

1. **One shared connection.** The asyncio client appends `(future, converter, decode)` to a
   FIFO deque, so concurrent coroutines share one socket.
2. **Write coalescing without timers.** On an idle connection a frame is written at once.
   While replies are still outstanding, frames from the same event-loop iteration are joined
   into one `transport.write`. A `call_soon` flush sends them, with a cap of 64 KiB.
   Profiled at 64 concurrent coroutines, one write carried about 28 commands and one read
   about 44 replies.
3. **Pre-encoded command fragments.** `$3\r\nGET\r\n` is built once per command name. The
   bulk-length headers `$N\r\n` come from a 4096-entry table. Integers 0–65535 come from a
   table of complete bulk fragments, so they need no int→str conversion. `GET`, `SET`,
   `INCR`, `HGET` and `DEL` build their frame with a single `b"".join`.
4. **Reading in place.** The asyncio reader is an `asyncio.BufferedProtocol`, so the event
   loop reads straight into the parser's reusable `bytearray`; there is no StreamReader and
   no `bytes` object per read. The sync client uses `recv_into` on the same buffer.
5. **Parser fast paths.** The parser finds CRLF with `bytes.find` and decodes small values
   from `bytearray` slices, which are faster than `memoryview` slices on CPython. Large
   values go through the `memoryview` to avoid a second copy. A one- or two-digit bulk
   length is read without `find`/`int`. A flat array of bulk strings and integers is read
   in one tight loop. The parser can resume: an array that arrives in many chunks keeps
   the items already parsed and is never parsed again from the start. A scalar reply
   allocates only the value it returns.
6. **Optional `hiredis`.** If `hiredis` can be imported, `parser="auto"` (the default) uses
   it. Otherwise the client uses the pure-Python parser. You can force either with
   `parser="python"` or `parser="hiredis"`.

### Measured

These numbers come from `benchmarks/bench.py` with Python 3.13.16 on a shared, noisy
4-vCPU Linux VM. The server was a local Ruvio over loopback, with 64-byte values. Each
figure is a single run of 2 seconds. They are indicative, not release numbers.

| | ruvio-client | redis-py 8.1.0 (hiredis) |
| --- | ---: | ---: |
| sync serial GET / SET ops/s | 17,500 / 18,850 | 13,150 / 14,700 |
| sync pipeline GET (100 per batch) ops/s | 605,600 | not measured |
| asyncio, 1 coroutine, GET / SET ops/s | 29,379 / 28,447 | 7,431 / 6,968 |
| asyncio, 64 coroutines, GET / SET ops/s | 186,018 / 147,844 (1 connection) | 8,250 / 8,150 (pool of up to 64) |

Per-reply parse cost, including copying the reply into the read buffer:

| reply | pure Python | hiredis |
| --- | ---: | ---: |
| `+OK` | ~870 ns | ~610 ns |
| `:int` | ~900 ns | ~760 ns |
| `$bulk64` | ~1030 ns | ~660 ns |
| `*10` bulk | ~5100 ns | ~1900 ns |

Encoding a GET takes about 425 ns through the generic encoder and about 310 ns through the
typed method. Encoding a 64-byte SET takes about 580 ns through the typed method. Each
GET frame allocates 70 bytes. A parsed `$bulk64` reply keeps only the returned `str`
(about 114 bytes).

```bash
RUVIO_TEST_ADDR=127.0.0.1:6379 python benchmarks/bench.py --seconds 1 --redis-py
```

## Cluster

```python
c = ruvio.Ruvio("127.0.0.1", 17330, cluster=True)              # sync
c = ruvio.asyncio.Ruvio("127.0.0.1", 17330, cluster=True)      # asyncio
```

`cluster=True` is opt-in; the plain `Ruvio(...)` class (and its fast path) is not touched.
With it, the first command (or `connect()`) asks the seed for `CLUSTER SLOTS` and builds a
16384-entry slot -> node table. One socket per node is opened lazily (asyncio: one multiplexed
connection per node, the same pipelined protocol as the standalone client). A server that
answers `-ERR cluster mode is disabled` (or lists a single node) is detected there and used as a
plain standalone server from then on (`client.is_cluster` / `await client.is_cluster()` tells).
The slot is `CRC16-XMODEM(key or {hashtag}) mod 16384` (computed with `binascii.crc_hqx`, checked
against `CLUSTER KEYSLOT`); `ruvio.key_slot(key)` exposes it.

| Command kind | Routing |
|---|---|
| Single key (argument 1: `GET`, `SET`, `HSET`, `XADD`, `BF.*`, `JSON.*`, Ruvio primitives, `SPUBLISH` ...) | the node that owns the key's slot |
| Keyless (`PING`, `INFO`, `CONFIG`, `ACL`, `CLIENT ...`, `CLUSTER ...`, `SLOWLOG`, `JSON.INDEX/FIND` ...) | the seed node |
| `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TOUCH` across slots | one sub-command per slot, sent to all owning nodes before any reply is read, replies re-assembled in the caller's order (MGET positions, summed counts, `OK`) |
| Other multi-key commands (`RENAME`, `COPY`, `SMOVE`, `LMOVE`, `SINTER*`, `SUNION*`, `SDIFF*`, `ZUNION/ZINTER/ZDIFF[STORE]`, `BITOP`, `PFCOUNT/PFMERGE`, `SINTERCARD`, `LMPOP/BLMPOP`, `BLPOP/BRPOP/BZPOP*`, `EVAL/EVALSHA/FCALL`, `XREAD[GROUP]`, `GEOSEARCHSTORE`, `GEORADIUS ... STORE`, `JSON.MGET`, `MSETNX`, `WATCH`) | routed by their real key positions; keys in different slots raise `CrossSlotError` **before anything is sent** |
| `DBSIZE` | summed over all nodes |
| `FLUSHDB`, `FLUSHALL`, `SCRIPT LOAD/FLUSH`, `FUNCTION LOAD/DELETE/FLUSH`, `AUTH`, `SELECT`, `CLIENT SETNAME` | run on every node, one reply returned |
| `KEYS` | every node, results concatenated |
| `SCAN` | walks the nodes one after the other: the cursor you get back is `server_cursor * nodes + node`, `0` only after the last node (treat it as opaque). `scan_iter()` does this for you |
| `PUBLISH`, classic `SUBSCRIBE`/`PSUBSCRIBE` | the owner of slot 0, so publishers and subscribers meet; `ssubscribe` connects to the owner of the channel's slot |
| `MULTI`/`EXEC`/`DISCARD` sent one by one | `RuvioError` (the queued commands may belong to different nodes); use `pipeline(transaction=True)` or `transaction(fn, *watch)` |

Behaviour worth knowing:

- **`-MOVED`**: the command is re-sent to the node the reply names, at most **3 times** per
  command (then the last `MovedError` is raised). A command's first `MOVED` to a node already in
  the table points that one slot at it; a node that is not in the table, or a second `MOVED` for
  the same command, re-reads `CLUSTER SLOTS`. `MOVED` means the command was not executed, so
  following it is safe.
- **No replay after transport failures.** A dropped node socket fails the command that saw it
  (`ConnectionError`), is closed, and is reopened by the next command for that node. Other nodes
  are not affected. A pipeline/split call that touched several nodes still reads every healthy
  node's replies before raising.
- **Pipelines** group commands by node, send each group as one pipelined write (all groups are
  written before any is read; asyncio gathers them) and return replies in the caller's order.
  A cross-slot `MGET`/`MSET`/`DEL`/`EXISTS`/`UNLINK`/`TOUCH` is split per slot exactly like the
  plain command, and its index in the reply list holds the merged reply (`MGET` in key order,
  summed counts, `OK`; an error from any part is that index's error). Any other cross-slot
  multi-key command, or a fan-out command (`DBSIZE`, `KEYS`, `SCAN`...), inside a pipeline raises
  before sending. A command in a pipeline that gets `MOVED` is followed singly after the batch,
  so its order relative to the other commands is not preserved for that retry.
- **Transactions** (`pipeline(transaction=True)`, `transaction()`) need all keys in one slot, else
  `CrossSlotError`. If `WATCH` ran on another node than the transaction's keys the call also
  raises (the watch would not apply). A `MOVED` transaction is re-sent whole to the new owner,
  following at most 3 redirects like a single command.
- **Blocking commands** (asyncio): `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOP*`, `XREAD ... BLOCK`
  and `FILLGET ... WAIT` run on a short-lived dedicated connection to the owning node, so they
  never stall the shared one. The sync client simply blocks on that node's socket.
- **Helpers** (lock, lease/semaphore scopes, rate limit, `get_or_create`, idempotency, push_recent,
  seen_before, JSON helpers, scan iterators) are unchanged: they only issue single-key commands.
  `transaction()` and `dedicated()` return cluster clients.
- Cluster mode has only database 0 (so `db=` other than 0 is a server error on connect).

Cost: the router reads the command name and first key out of the already-encoded frame
(`frame_head` + `crc_hqx`, about 0.65 us per command in CPython 3.13, from `benchmarks/bench.py`), then
indexes two lists. Only `ClusterRuvio` pays it.

Short benchmark (`RUVIO_TEST_ADDR` plain 1-shard server vs `RUVIO_CLUSTER_ADDR` 3-shard server, same machine with 4 cores, 64 spread keys, 3 s per row, one run, noisy; the serial rows are dominated by server wake-up latency when alternating between shards): sync serial GET 26.6k vs 22.1k ops/s, sync pipeline of 64 GET 526k vs 322k ops/s, asyncio 64 concurrent GET 239k vs 129k ops/s and SET 192k vs 125k ops/s (standalone vs cluster). The cluster client does roughly half the standalone throughput in the CPU-bound asyncio case: routing work in Python plus smaller per-connection write batches over three connections. asyncio keeps the warm path free of an extra
coroutine: the node connection's pending-reply future is returned directly, and only a `MOVED`
reply is intercepted.

## Reads from replicas

`read_policy=` on a cluster client (sync and asyncio) sends reads to a replica of the key's shard. It takes
`ReadPolicy.PRIMARY` (default: nothing changes), `ReadPolicy.PREFER_REPLICA` or `ReadPolicy.REPLICA`, or the
strings `"primary"`, `"prefer-replica"` and `"replica"`. A standalone server ignores it.

```python
from ruvio import ClusterRuvio, ReadPolicy

client = ClusterRuvio("127.0.0.1", 7000, read_policy=ReadPolicy.PREFER_REPLICA)
client.set("user:1", "Ada")   # primary
client.get("user:1")          # a replica; may not have the write yet
```

- Replication is asynchronous: a read from a replica may miss a recent write, including your own.
- A read is a command the server flags read-only and not blocking (`GET`, `MGET`, `EXISTS`, `HGETALL`,
  `ZRANGE` ...). Writes, keyless commands, transactions (`MULTI`/`EXEC`/`WATCH`), blocking commands, scripts,
  Pub/Sub, `SCAN` and the commands sent to every node stay on the primaries. A multi-key command split over
  several slots decides per part: `MGET` and `EXISTS` parts read from replicas, `DEL` and `MSET` parts go to primaries.
- A pipeline goes to replicas only when every command in it is a read; a pipeline with a write is routed as before.
- When the slot table loads, the client picks one replica per shard at random and keeps it until the next load.
  Each replica connection sends `READONLY` right after it opens. With the default policy no replica connection
  is opened and routing does one extra comparison.
- A replica that cannot be connected, or answers `LOADING` (still copying its primary), is skipped until the
  next slot table load. `PREFER_REPLICA` then reads from the primary (a `LOADING` read is sent again there);
  `REPLICA` raises (`ConnectionError`, or the `LOADING` `ResponseError`), and a `RuvioError` saying "no replica"
  when none is known. A socket that drops after a command was sent fails the command, as for every node.

## Seed addresses

`seeds=` takes the place of `host`/`port` (both clients, plain and `cluster=True`): `"host:port"` strings
(IPv6 as `"[::1]:6379"`) or `(host, port)` tuples. Without it nothing changes.

```python
from ruvio import Ruvio

client = Ruvio(seeds=["10.0.0.1:6379", ("10.0.0.2", 6379), "[::1]:6379"])
```

- Connecting tries the seeds in order, one connect attempt each, and keeps the first that answers. A seed
  that cannot be reached is skipped; a refused `AUTH` or `SELECT` raises at once. When none answers, the
  last `ConnectionError` is raised.
- After a dropped connection the command that hit the drop still raises. The next reconnect tries every seed
  once, starting with the seed after the one that dropped and ending with it. A `pubsub()` session opened
  later starts at the address the command connection is on.
- With `cluster=True`, `CLUSTER SLOTS` comes from the first reachable seed. A table reload asks the nodes that
  have an open socket first; when none answers it opens every other known node and then every seed, one
  connect attempt each, until one returns a table. A node that cannot be opened triggers that reload and the
  command goes to the owner the new table names (a keyless command to the first reachable node of the new
  table). Pipelines, fan-out commands and transactions do not trigger the reload on their own; they fail as
  before and the next single-key or keyless command reloads. Seeds are never asked while the known nodes answer.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install pytest pytest-asyncio   # optional: hiredis redis
RUVIO_TEST_ADDR=127.0.0.1:6379 .venv/bin/python -m pytest              # live + unit tests
RUVIO_TEST_PARSER=python ...                                            # force the pure-Python parser
RUVIO_TEST_FLUSHALL=1 ...                                               # also run the FLUSHALL test
```

- `tests/test_resp.py` contains the codec unit tests. They need no server: encoder, both
  parsers, byte-by-byte feeding, RESP3 types, buffer growth, resumable arrays.
- `tests/test_live.py` holds one scenario per command family. Each scenario runs on
  **both** the sync and the asyncio client.
- `tests/test_helpers_live.py` covers the helpers, Pub/Sub, RESP3 pushes, multiplexing
  order, cancellation, reconnect, timeouts, large values, `WATCH`, and URLs.
- `tests/test_coverage.py` reads `docs/compatibility-matrix.json`. It fails when a command
  has no typed method on the sync or the async client. It also fails when a typed method is
  never called by a live test.
- `tests/test_cluster_unit.py` needs no Ruvio server: slot vectors and hashtags, key
  extraction through the typed methods, a check that **all 298 matrix commands have a routing
  rule**, topology / `MOVED` / scan-cursor parsing, and the router against scripted fake servers
  (`tests/fakecluster.py`) on both clients: owner-only routing, fail-fast `CROSSSLOT` (no byte
  sent), split reassembly order, `MOVED` patch / refresh / unknown node / at most 3 redirects, no
  replay after a transport failure, pipeline grouping and order, cross-slot split inside a pipeline, transactions on one node, standalone
  fallback.
- `tests/test_cluster_live.py` runs against a 3-shard server with
  `RUVIO_CLUSTER_ADDR=127.0.0.1:17330` (shard i on base+i; start it with
  `shards = 3` and `port_mode = "per-shard"`) on both clients: slot table vs `CLUSTER KEYSLOT`,
  keys spread over all shards, cross-slot `MGET/MSET/DEL/EXISTS/TOUCH/UNLINK` order,
  cross-slot errors leave data untouched, same-slot multi-key commands, script fan-out,
  pipeline order, cross-slot split inside a pipeline, tx single-slot / cross-slot, `WATCH` + retry, `MOVED` recovery from a fully
  stale table (command, pipeline, split, transaction), helpers, scan iterators over all nodes,
  `BLPOP`, pub/sub and sharded pub/sub, losing one node's connection. The standalone fallback
  test uses `RUVIO_TEST_ADDR` (plain server). Cluster mode has only database 0, so the older
  live suites (database 11) are not run on the cluster.
- `tests/test_replica_reads_live.py` needs `RUVIO_REPLICATED_ADDR` (a 2-shard primary) and `RUVIO_REPLICA_ADDR` (its
  2-shard replica, shard i on port + i); `scripts/run_with_servers.sh` starts both. Without them it is skipped.
- `tests/test_multi_node_live.py` needs `RUVIO_MULTINODE_A_ADDR`, `RUVIO_MULTINODE_B_ADDR` and
  `RUVIO_MULTINODE_STALE_ADDR` (a two-node cluster and a node with an older map, started by
  `scripts/run_with_servers.sh`) and checks that the client follows `MOVED`. Without them it is skipped.
- Without `RUVIO_TEST_ADDR` the live tests are skipped. Live tests use database 11 and run
  `FLUSHDB`. `test_keys` also uses database 12. `JSON.INDEX` only works in database 0, so
  that test uses unique keys there and cleans up after itself.
- `ACL SAVE`/`ACL LOAD` are only exercised end to end when the server has
  `security.acl_file`. Without it, the test only checks that the server's error is raised.

## Not done / not verified

- **Cluster scope.** Routing is done and live-tested on a 3-shard server (see Cluster), but:
  `MULTI`/`EXEC` sent one by one are refused (use `pipeline(transaction=True)`); a
  cross-slot `RENAME`/`COPY`/`SMOVE`/`LMOVE`/... is refused rather than emulated by
  copying; `CLUSTER NODES/SHARDS/INFO/MYID` and `CHANGES` go to the seed only; `SCRIPT EXISTS`
  and `SCRIPT KILL` ask the seed only; there is no `ASK` handling (the server does not
  migrate slots); no replica reads (`READONLY` is sent to the seed only); a cluster whose
  advertised host is unreachable from the client is not handled. Not verified: a server
  topology that changes while the client runs (`MOVED` recovery was verified by corrupting the
  client's table, not by moving slots on a server), clusters with more than 3 nodes, AUTH on
  a cluster with a password, a pipeline > 64 KiB per node with a dead peer.
- **Typing.** The command methods are shared by the sync client, the async client and
  pipelines, so their return annotations are `Any`. Each docstring states the shape of the
  value.
- **Threads.** The sync client is not thread-safe and has no connection pool. Use one
  client per thread, or `dedicated()`.
- **Blocking commands on the shared asyncio connection.** `BLPOP`, `XREAD BLOCK`,
  `FILLGET … WAIT` and similar hold the connection for every coroutine. Run them on
  `client.dedicated()` (the cluster client does this for you). Cancelling an awaiting caller is safe: the late reply is dropped.
- **No retries.** No command is sent again after a connection loss. The command that sees
  the drop raises `ConnectionError`, and the next command reconnects and re-applies `AUTH`,
  `SELECT` and `CLIENT SETNAME`.
- **No write backpressure in asyncio.** The client does not handle `pause_writing`.
  Writes are capped at 64 KiB each, but the transport's buffer can grow without limit if
  the server stops reading.
- **Inside a pipeline, `dump()`** follows the client's `decode_responses` setting. Use
  `client.raw.pipeline()` to get `bytes`.
- **With hiredis**, RESP3 big numbers (`(`) are returned as `str`; the pure-Python parser
  returns `int`.

Server behaviours observed while testing this Ruvio build. Each is covered by a test and
documented here because it differs from Redis or from the matrix text:

- `ZRANGE key max min BYSCORE REV` takes the high bound first, as Redis does (older server builds expected min first).
- `XADD … MAXLEN = n` is rejected, while `XTRIM … MAXLEN = n` is accepted. The client never
  sends `=`, because an exact trim is the default.
- `EXPIRE key 0` deletes the key and returns 1, as Redis does (the matrix text was corrected).
- `FLUSHDB` and `FLUSHALL` clear in-flight `FILLGET` fill markers (older builds kept them until `fill_ms`).
- In Lua scripts the API object is `ruvio.call(...)`. `redis.call` fails with
  `Error running script`.
- An unknown command inside `MULTI` is rejected when queued and `EXEC` answers `EXECABORT`
  (`ruvio.ExecAbortError`) and runs nothing, as Redis does. Older server builds ran the rest.
- `AUTH <anything>` succeeds when no `requirepass` is configured.
