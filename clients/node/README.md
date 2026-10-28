<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">ruvio-client <sub>Node.js</sub></h1>

<p align="center">Fast, zero-dependency RESP client for <a href="https://ruviodb.com/">Ruvio</a>. TypeScript source, ESM output with <code>.d.ts</code>.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/ruvio-client"><img alt="npm" src="https://img.shields.io/npm/v/ruvio-client?label=npm"></a>
  <a href="https://www.npmjs.com/package/ruvio-client"><img alt="Downloads" src="https://img.shields.io/npm/dm/ruvio-client?label=downloads"></a>
  <a href="https://www.npmjs.com/package/ruvio-client"><img alt="Node" src="https://img.shields.io/node/v/ruvio-client?label=node"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/node/">Node.js guide</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Node 18+, zero dependencies, MIT licensed.

```sh
npm install ruvio-client
```

```ts
import { createClient } from 'ruvio-client';

const c = createClient('redis://127.0.0.1:6379/0');   // or { host, port, db, username, password, tls, durability }
await c.set('k', 'v', { px: 5000 });
console.log(await c.get('k'));                          // 'v'
console.log(await c.binary.get('k'));                   // Buffer
await c.quit();
```

## Design (speed and memory first)

- **One multiplexed socket per client.** Concurrent calls share it; replies resolve FIFO from a ring-buffer
  pending queue (no `Array.shift`, no per-call closure: the promise executor is a shared function).
- **Write coalescing without timers.** Frames queued in the same tick go out as one `socket.write`
  (`flush: 'microtask'`, default; `'immediate'` also collects callers resumed by the same I/O event).
  Serial calls are sent at once.
- **Encoder:** command names are pre-encoded (`*N\r\n$3\r\nGET\r\n` cached per arity), integers are written as ASCII digits
  directly into a reusable 64 KiB slab, short ASCII strings are copied char by char, longer strings use `utf8Write`.
  Buffers of 64 KiB or more go to the socket without a copy.
- **Parser:** incremental and resumable (a large array arriving in many reads is parsed in linear time),
  works in place on the socket's reusable read buffer (`onread`), copies only an unfinished tail.
  Strings are decoded with `utf8Slice`; `client.binary.*` / `getBuffer` return copies as `Buffer`.
- RESP2 by default; `protocol: 3` sends `HELLO 3` (maps become objects, pushes are emitted as `push` events,
  `CLIENT TRACKING` invalidations work).

## API

Typed methods for every command in `docs/compatibility-matrix.json` (299/299, enforced by `test/coverage.test.ts`),
including the Ruvio options: `set(k, v, { ifEq })`, `set(k, n, { ifGt })` / `{ ifLt }`, `incrbyMax`, `fillget`/`fillset` (`WAIT`, `SWR`), `getex(k, ms)`,
`delex(k, ifEq)`, `lpushcap`/`rpushcap`, `bf*`, `json*` (incl. `jsonIndexCreate`, `jsonFind`), `idemBegin`/`idemComplete`/`idemAbort`,
`lease`/`semaphore`/`release`, `once`, `take`, `transfer`, `htransfer`, `setv`, `changes*`, `limit`. Anything else: `c.call('CMD', ...args)`.

- **Pipeline:** `await c.pipeline().set('a', 1).get('a').exec()` (one write; per-command errors stay in their slot, `execStrict()` rejects).
- **Transaction:** `await c.multi().incr('{k}a').incr('{k}b').exec()`: `MULTI`..`EXEC` in one contiguous write; `null` if a WATCH failed.
  `WATCH` needs its own connection: `c.isolated(async iso => { await iso.watch(k); ... iso.multi()...exec() })`.
- **Pub/Sub:** `const s = c.subscriber(); s.on('message', m => ...)` or `for await (const m of s)`; a dedicated socket,
  re-subscribes after a drop. Supports `psubscribe`, `ssubscribe`, `binary: true` payloads.
- **Blocking commands** (`blpop`, `xread block`, `fillget ... WAIT`, `wait`) park the shared socket: use `c.duplicate()`.
- **Durable replies:** `createClient({ durability: { replicas: 1, timeoutMs: 5000 } })` sends `CLIENT DURABILITY REPLICAS 1 TIMEOUT 5000` on every
  (re)connect, so each reply waits until that many replicas have the data on disk (otherwise `NOREPLICAS`; the write stays applied).
  `c.clientDurability(n, timeoutMs?)` sets it on one connection; `0` turns it off. Needs a server with replication.
- **Errors:** server errors reject with `RuvioError` (`.code` = `ERR`, `WRONGTYPE`...); a lost connection rejects pending calls with `ConnectionError`,
  and the next call reconnects (database and name are re-applied). Nil replies are `null`.

### Helpers

| Helper | Built on |
| --- | --- |
| `await using lock = await c.acquireLock(key, ttlMs)`; `lock.renew()`, `lock.release()`, `c.withLock(...)` | `SET NX PX`, `SET IFEQ`, `DELEX IFEQ` (never deletes another owner's lock) |
| `c.acquireLease(key, ttl)` (`lease.token` is the fencing token) | `LEASE` / `RELEASE` |
| `c.acquireSemaphore(key, max, ttl)` | `SEMAPHORE` / `RELEASE` |
| `c.rateLimit(key, max, windowMs)` -> `{ allowed, remaining }` | `LIMIT` |
| `c.limitSliding(key, max, windowMs)`, `c.limitTokens(key, capacity, refillMs, cost?)` -> remaining or `null` | `LIMIT ... SLIDING`, `LIMIT ... TOKENS` |
| `c.limitSlidingWithRetry(...)`, `c.limitTokensWithRetry(...)` -> `{ allowed, remaining, retryAfterMs }` | `LIMIT ... WITHRETRY` |
| `c.getOrCreate(key, ttlMs, factory, { staleMs })` | `FILLGET`/`FILLSET`; waiters poll plain `FILLGET` with backoff (never `WAIT`) |
| `c.idempotent(key, fingerprint, ttlMs, fn, opts)` | `IDEM BEGIN/COMPLETE`. If `fn` throws the record stays pending until `ttlMs`; with `opts.abortOnError` (default `false`) it sends `IDEM ABORT` first, so a retry runs `fn` again at once, and the original error is still thrown (a failed abort is ignored). Use it only when a failed `fn` did not take effect or is safe to run again |
| `c.idemAbort(key, fingerprint, owner)` | `IDEM ABORT`: `true` when the pending record of that owner and fingerprint was removed; `false` for a missing key, a completed record (its result is never removed) or another owner. Abort only when the work did not take effect or is safe to run again; a retry after an abort runs it again |
| `c.scanIterator()`, `hscanIterator`, `sscanIterator`, `zscanIterator` | cursor-driven async generators |
| `GeoUnit`, `GeoOrder`, `ListSide`, `InsertPosition`, `BitOp`, `BitUnit`, `Aggregate`, `ScanType`, `BitfieldOp` | named keyword constants and `BITFIELD` builders, e.g. `c.geodist('g', 'a', 'b', GeoUnit.Km)`, `c.scan(0, { type: ScanType.Hash })`, `c.bitfield(k, ...BitfieldOp.get('u8', 0))`; plain string literals still work, and the keywords are sent pre-encoded |
| `StreamTrim`, `TrimMode`, `InfoSection`, `ClientType`, `StreamId`, `ScoreBound`, `LexBound` | one typed trim rule for `XADD` and `XTRIM`, `INFO` and `CLIENT LIST TYPE` names, stream id markers and range bound builders, e.g. `c.xtrim('s', StreamTrim.maxLen(1000, TrimMode.Approximate))`, `c.xadd('s', StreamId.New, f, { trim: StreamTrim.minId(id) })`, `c.info(InfoSection.Memory)`, `c.zrangebyscore(k, ScoreBound.NegativeInfinity, ScoreBound.exclusive(100))`, `c.zrangebylex(k, LexBound.inclusive('a'), LexBound.Max)` |
| `c.jsonGetAs<T>()`, `c.jsonSetAs()` | `JSON.GET/SET` + `JSON.parse/stringify` |
| `c.pushRecent(key, cap, ...)`, `c.recent(key, n)` | `LPUSHCAP` |
| `c.seenBefore(key, ...items)`, `c.mightContain` | `BF.MADD` / `BF.MEXISTS` |

`Symbol.asyncDispose` is polyfilled for Node 18/20. The `await using` syntax itself is not available in plain JavaScript on Node 22 (it failed in a `.mjs` file on Node 22.22): use it from TypeScript 5.2+ (the compiler lowers it), or call `lock.release()` / `c.withLock(...)` instead.

### TTL on create

Counters, bitmaps and other write commands can set a deadline in the same call that creates the key. Pass `ttl` in seconds
(a whole number is sent as `EX`, a fraction as `PX`); the deadline is set only when the call creates the key, and an existing
key keeps the deadline it already has. A `ttl` that is not positive and finite rejects with `RangeError` before anything is sent.

```ts
await c.incr('hits:today', { ttl: 86_400 });                 // INCR hits:today EX 86400
await c.incrbyMax('seats', 1, 100, { ttl: 3600 });           // INCRBY seats 1 EX 3600 MAX 100
await c.zadd('board', 10, 'ada', { ttl: 600 });              // ZADD board EX 600 10 ada
await c.jsonSet('user:1', '$', '{"a":1}', 'NX', { ttl: 60 }); // JSON.SET user:1 $ {...} NX EX 60
```

Supported on `incr`, `decr`, `incrby`, `decrby`, `incrbyMax`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `jsonSet`,
`jsonSetAs` (as a trailing `{ ttl }` argument) and through the `ttl` field of the options of `zadd`/`zaddIncr`, `geoadd`,
`xadd` and `bfInsert`. `expire`, `pexpire`, `expireat` and `pexpireat` take an optional last argument, an `ExpireCondition`
(`NX`, `XX`, `GT`, `LT`); a key without a deadline counts as infinite, and a failed condition returns `false`.

## Cluster

```ts
import { createClusterClient } from 'ruvio-client';

const c = createClusterClient('redis://127.0.0.1:17340');   // any shard; or { host, port, ...ClientOptions }
await c.set('user:1', 'a');                                  // routed by CRC16 slot ({hashtag} aware)
console.log(await c.mget('user:1', 'user:2', 'user:3'));     // split per slot, replies in your order
```

`ClusterClient` has the same typed commands, helpers (`acquireLock`, `rateLimit`, `getOrCreate`, ...), `binary`, `pipeline()` and
`multi()` as `RuvioClient`. On the first command (or `await c.connect()`) it asks the seed for `CLUSTER SLOTS`, builds a
16384-entry slot table and keeps one lazy multiplexed connection per node. A seed that answers `cluster mode is disabled` is a
standalone server: the client then encodes straight into that one connection, exactly like `RuvioClient` (`cluster: true` makes that an error instead).

How routing works (no change to the typed commands): each command is encoded into a private scratch encoder, the key is read from the
encoded bytes (no key strings are built), and the frame, a zero-copy view of the encoder slab, goes to the owner's connection. The frame is kept
only for a possible `MOVED` retry.

| Command shape | Routing |
| --- | --- |
| single key (`GET`, `HSET`, `LEASE`, `LIMIT`, `FILLGET`, `IDEM`, `BF.*`, `JSON.*`, `XADD`, ...) | node of the key's slot |
| `XGROUP` / `XINFO` / `OBJECT` / `MEMORY` | key is argument 2 |
| `RENAME`, `COPY`, `LMOVE`, `SMOVE`, `GEOSEARCHSTORE`, `SINTER*`, `SUNION*`, `SDIFF*`, `PFCOUNT/MERGE`, `BITOP`, `JSON.MGET`, `ZUNION*`, `ZINTER*`, `ZDIFF*`, `SINTERCARD`, `LMPOP`, `BLMPOP`, `BLPOP`..., `EVAL*`/`FCALL*` with keys, `XREAD*`, `WATCH`, `MSETNX` | all their real keys must share one slot, else `RuvioError` `CROSSSLOT` is raised **before anything is sent** |
| `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TOUCH` over several slots | one sub-command per slot (the server refuses even two slots of the same shard), run concurrently, replies re-assembled in your order (`DEL`/`EXISTS`/... counts are summed). Not atomic: a failure in one part rejects the call while other parts may have run |
| `DBSIZE`, `KEYS`, `FLUSHDB`, `FLUSHALL` | sent to every node and merged (sum / concatenation / first reply) |
| other keyless commands (`PING`, `INFO`, `CLIENT`, `CONFIG`, `PUBLISH`, `SCRIPT`, `FUNCTION`, `ACL`, ..., `EVAL` with 0 keys) | the seed node |

- **`-MOVED`**: the slot is repointed to the named node and the command is sent there; up to **3** redirects are followed per command, a fourth `MOVED` is returned as the error.
  The whole table is re-read with `CLUSTER SLOTS` only when the named node is unknown or on the second `MOVED` for the same command (one request is shared by all callers).
- **`-ASK`**: `ASKING` and the command are written together to the named node (opened if unknown) and its reply is returned; the slot table is not changed, so the next command for that slot still goes to the owner.
  `ASK` and `MOVED` share the limit of **3** redirects per command.
- **`-TRYAGAIN`**: the command is sent again, routed by the current table, up to **7** times after waits of 5, 10, 20, 40, 80, 160 and 320 ms (635 ms in all); the eighth `TRYAGAIN` is returned as the error.
  Transport failures are never replayed: the affected calls reject with `ConnectionError`, only that node reconnects (lazily), other nodes are untouched.
- **Pipelines** are grouped per node: frames are written to each node's connection in call order (one coalesced write per node), replies come back in call order.
  A cross-slot `MGET`-style command inside a pipeline is split, an unsplittable cross-slot one leaves a `CROSSSLOT` error in its own slot, and `MOVED`, `ASK` and `TRYAGAIN` are followed per command.
  A lost connection rejects `exec()`, as for a single connection.
- **Transactions** (`c.multi()`): all keys must hash to one slot, otherwise `exec()` rejects with `CROSSSLOT` and sends nothing; keyless ones run on the seed. `MOVED`, `ASK` and `TRYAGAIN` inside a
  transaction are not retried (it surfaces as the `EXECABORT`/command error; the next call fixes the table). `WATCH` needs the owner's own connection: `c.isolated(key, async iso => {...})`.
- **Blocking commands** (`BLPOP`, `BRPOP`, `BZPOP*`, `BLMOVE`, `BLMPOP`, `XREAD`/`XREADGROUP ... BLOCK`, `FILLGET ... WAIT`, `WAIT`) run on a pooled dedicated connection to the
  owning node, so they never park the shared one. (Inside a pipeline they use the shared connection.)
- **Pub/Sub**: `c.subscriber()` connects to the seed node (`SUBSCRIBE`, `PSUBSCRIBE`); `await c.subscriberFor(channel)` connects to the node owning the channel's slot
  (use it with `SSUBSCRIBE` / `SPUBLISH`, which route by channel).
- **Scan family**: `c.scanIterator()` walks `SCAN` on **every node, one after the other**; a plain `c.scan(cursor)` reaches only the seed node. `hscanIterator`/`sscanIterator`/`zscanIterator` route by key.
- Introspection: `c.isCluster`, `c.nodes`, `c.nodeFor(key)` (a node with its own `RuvioClient`), `c.slotOf(key)`, `c.refreshSlots()`, `c.redirectCount`, `c.refreshCount`.
- `hashSlot(key)` is exported and is checked against `CLUSTER KEYSLOT` in the live tests. Cluster mode has database 0 only (`SELECT` is not routed).

## Reads from replicas

`ClusterClient` takes `readPolicy: 'primary' | 'prefer-replica' | 'replica'` (type `ReadPolicy`). A standalone server ignores it.

```ts
const c = createClusterClient({ host: '127.0.0.1', port: 17340, readPolicy: 'prefer-replica' });
```

- `'primary'` (default): every command goes to the shard's primary, as before. No replica connection is opened and routing does one extra comparison.
- `'prefer-replica'`: reads go to a replica of the key's shard; when no replica can be used they go to the primary.
- `'replica'`: reads go to a replica and fail when none can be used (`RuvioError`, message `no replica is known for slot N`).
- Replication is asynchronous: a read from a replica may miss a recent write, including your own.
- A read is a command the server flags readonly and not blocking, with a key (`GET`, `MGET`, `HGETALL`, `EXISTS`, ...). Writes, keyless commands, transactions, Pub/Sub, blocking commands and `SCAN`/fan-out commands (`KEYS`, `DBSIZE`) always use the primary. Split multi-key commands apply the rule per slot (`MGET` and `EXISTS` parts read, `DEL` and `MSET` parts write).
- A pipeline goes to replicas only when every command in it is a read; otherwise it is routed as for `'primary'`.
- `CLUSTER SLOTS` lists each shard's replicas after its primary. On every slot table load the client picks one replica per shard at random and keeps it until the next load. A replica connection sends `READONLY` right after it is opened, and again after every reconnect.
- A replica that cannot be opened, or answers `-LOADING` (still copying its primary), is skipped until the next slot table load: its read goes to the primary under `'prefer-replica'`, and fails under `'replica'` (the `LOADING` error is returned). A socket that drops after the command was sent still rejects the command; it is not sent again.

## Seed addresses

`seeds` lists addresses (`host:port`, IPv6 as `[::1]:6379`) in place of `host` and `port`. Without it nothing changes.

```ts
const client = createClient({ seeds: ['10.0.0.1:6379', '10.0.0.2:6379'] });
const cluster = createClusterClient({ seeds: ['10.0.0.1:7000', '10.0.0.2:7000', '10.0.0.3:7000'] });
```

- Connecting tries the seeds in order, one connect attempt each, and keeps the first that answers. A seed that cannot be reached is skipped; a refused `AUTH` stops at once. When none answers, the last connect error is returned.
- After a dropped connection the next connect starts with the seed after the one that dropped and goes round the whole list. The command that hit the drop still rejects, and a `subscriber()` starts at the seed the command connection is on.
- `ClusterClient` reads the slot table from the first reachable seed. A reload asks the open node connections first; when none answers it opens the other known nodes and then the seeds, and a command whose node cannot be opened goes to the owner the new table names. Seeds that cannot be opened do not stay in `c.nodes`.
- `seeds` cannot be combined with `path`; an empty host or an invalid port throws a `TypeError` when the client is created.

## Retry policies of your own

`isTransient` and `mayHaveRun` sort what a call throws, for a retry policy of your own (cockatiel, a loop).
The client already follows `MOVED`/`ASK`, waits out `TRYAGAIN` during a slot move and finds a new primary after
a failover; these judge what still reaches you.

- `isTransient(err)`: trying again later can succeed. True for `ConnectionError` (including a command timeout)
  and for `LOADING`, `MASTERDOWN`, `CLUSTERDOWN`, `TRYAGAIN`, `READONLY` and `NOREPLICAS`; false for `WRONGTYPE`,
  `OOM`, syntax errors, `ReplyError` and an aborted signal.
- `mayHaveRun(err)`: the failed call may have changed data. False only for a server error reply (the server
  refused the command), except `NOREPLICAS`: the write was applied and only the replica confirmation timed out.
  A failed connection or a timeout may have run.

Repeat a write only when it is safe to repeat or did not run:

```ts
import { handleWhen, retry, ExponentialBackoff } from 'cockatiel';
import { isTransient, mayHaveRun } from 'ruvio-client';

// INCR is not safe to repeat: retry it only when it surely did not run.
const policy = retry(handleWhen((e) => isTransient(e) && !mayHaveRun(e)), { maxAttempts: 3, backoff: new ExponentialBackoff() });
const visits = await policy.execute(() => client.incr('visits'));
```

For a read, `isTransient` alone is enough.

## Following a failover

A standalone client with two or more `seeds`, or with `sentinelMaster`, finds the primary again after a failover instead of staying on a node that is now a replica. `followPrimary` (default `true`) turns it off; with one address and no coordinator name nothing changes. A `ClusterClient` does not follow.

```ts
const client = createClient({ seeds: ['10.0.0.1:6379', '10.0.0.2:6379'] });
const viaCoordinator = createClient({ seeds: ['10.0.0.9:26379'], sentinelMaster: 'main' });
```

- A probe is a short-lived socket that sends `AUTH` as configured and one command (`ROLE`, or `SENTINEL get-master-addr-by-name <name>`), waits at most the smaller of `connectTimeoutMs` and `commandTimeoutMs`, and closes. A node that does not answer is skipped; a refused `AUTH` fails the operation. A replica's reported primary is probed too, once, when it is no seed.
- After a dropped connection each reconnect runs one round first (starting at the seed after the one that dropped) and connects to the node that answered `master`; without one it falls back to the seed order. The command that hit the drop still rejects.
- A reply `READONLY` (that word alone, or followed by a space) makes the client search for the primary, avoiding the node that answered while it still says `master` (once it says it is a replica, a later round takes it when it is promoted), in up to 8 rounds with waits of 100 ms doubling to 2 s (one round with `reconnect: false`). The waits are timers, so other work goes on. When a primary is found the socket is replaced and the refused command is sent once more; its reply, an error included, is the caller's. When none is found the `READONLY` error is returned and the connection stays. Commands still waiting for a reply on the old socket get a second to be answered, then reject with a `ConnectionError`.
- Not sent again, though the connection still moves: a command after a `MULTI` you sent with `call` (until `EXEC` or `DISCARD`), a command of a `pipeline()` or `multi()` batch (its `READONLY` error is in its slot), any other error (`LOADING`, `MASTERDOWN`, `NOREPLICAS`), and a command that failed with a dropped socket.
- With `sentinelMaster` the seeds are coordinators. The first connect asks them (one round) and fails with a `ConnectionError` naming the master when none names a primary. The same `username` and `password` go to coordinators and data nodes.
- The `connect` event carries the address of the new socket, and `disconnect` the address that was lost as its second argument.
- While the connection is healthy nothing is sent: the added work is looking at the text of an error reply, and a following client keeps the position of each pending command's frame so a refused one can be sent again.

## Tests and benchmark

```sh
npm test                                                     # codec unit tests + coverage; live tests skip
RUVIO_TEST_ADDR=127.0.0.1:17106 npm test                     # + live tests per command family and the cluster client's standalone fallback
RUVIO_TEST_ADDR=127.0.0.1:17343 RUVIO_CLUSTER_ADDR=127.0.0.1:17340 npm test   # + live cluster tests
RUVIO_TEST_ADDR=127.0.0.1:17106 npm run bench                # codec ns/B per op, live GET/SET ops/s, ioredis comparison
RUVIO_CLUSTER_ADDR=127.0.0.1:17340 RUVIO_TEST_ADDR=127.0.0.1:17343 npm run bench   # + cluster GET/SET next to standalone
```

Live tests use their own logical databases and `FLUSHDB`; one test runs `FLUSHALL` (use a scratch server).
`ioredis` is a dev dependency used only by the benchmark.

Local numbers (4 vCPU Linux VM, Node 22, loopback, 256-byte values, 1.5 s per case, single connection, one Ruvio process;
indicative only, not a release benchmark):

| client | GET c=1 | SET c=1 | GET c=128 | SET c=128 |
| --- | ---: | ---: | ---: | ---: |
| ruvio-client (microtask flush) | 13,787 | 14,996 | 605,419 | 755,983 |
| ruvio-client (setImmediate flush) | 13,912 | 13,897 | 641,112 | 697,912 |
| ioredis 5 | 11,228 | 12,291 | 104,733 | 92,169 |
| ioredis 5 (autoPipelining) | 8,794 | 8,768 | 146,256 | 132,264 |
| ruvio-client `pipeline()` of 128 GETs | 663,990 ops/s | | | |

Serial numbers vary a lot run to run in this VM (earlier runs of the same code gave 25-31k). Codec: encode GET ~66 ns, encode SET with a 256-byte
string ~230 ns (Buffer ~130 ns), parse `+OK` ~20 ns, `:int` ~57 ns, nil ~37 ns, 256-byte bulk to string ~240 ns. The `B/op` column from the
benchmark is a heap-delta estimate, and its small-window readings for parse (100-270 B) are the returned string/Buffer values themselves.

Cluster client (3-shard server `port_mode = "per-shard"` and a standalone server on the same VM, 256-byte values, 1.5 s per case, one client; indicative, noisy run to run):

| client | GET c=1 | SET c=1 | GET c=128 | SET c=128 |
| --- | ---: | ---: | ---: | ---: |
| `RuvioClient`, standalone server | 55,660 | 53,285 | 838,995 | 789,889 |
| `ClusterClient`, standalone server (fallback path) | 60,772 | 52,990 | 875,575 | 802,657 |
| `ClusterClient`, 3 shards | 46,834 | 38,842 | 659,344 | 523,403 |
| `ClusterClient` `pipeline()` of 128 GETs, 3 shards | 505,758 ops/s | | | |

The fallback path costs nothing measurable; routed commands pay for reading the key from the frame and one extra promise-free callback per call,
and the 3-shard server shares this VM's CPUs with the benchmark, so the cluster rows are a lower bound, not a scaling claim.

## Not done / not verified

- **Cluster, not done:** `ASK` redirects are not handled (Ruvio's per-shard layout has no slot migration); a plain `scan()` call is single-node (use `scanIterator()`);
  `MOVED` inside a transaction is not retried; split `MSET`/`DEL`... are not atomic; `SELECT` is not routed (database 0 only); `CLUSTER NODES/SHARDS/INFO/MYID` go to the seed node only.
  Cluster routing is verified only against Ruvio's 3-shard `per-shard` port layout on one machine, not against a Redis Cluster.
- **Not verified live:** TLS and unix-socket connections; `ACL LOAD`/`ACL SAVE` success (server has no acl_file; the call round-trips an error); `WAIT` with real replicas;
  Node 18 and 20 (only Node 22 was available); `HELLO ... AUTH`.
- Older server builds had quirks the client still tolerates: a blocked `XREAD`/`XREADGROUP` woken by `XADD` replied one entry without the outer list, `SUNION`/`SINTER` order was not sorted, and a queue-time error inside MULTI did not give EXECABORT. All three are fixed in the server; EXEC now answers EXECABORT and runs nothing.
- `EXPIRE`-family methods take no NX/XX/GT/LT (the server rejects them).
- Pipelines re-use frames in a private buffer; a pipeline cannot be re-run after `exec()`.
