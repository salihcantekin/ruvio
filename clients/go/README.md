<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio Go client</h1>

<p align="center">Fast, allocation-light client for <a href="https://ruviodb.com/">Ruvio</a>. Go 1.22+, standard library only.</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/salihcantekin/ruvio/clients/go"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/salihcantekin/ruvio/clients/go.svg"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/go/">Go guide</a> ·
  <a href="https://pkg.go.dev/github.com/salihcantekin/ruvio/clients/go">pkg.go.dev</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

```bash
go get github.com/salihcantekin/ruvio/clients/go
```

Import path `github.com/salihcantekin/ruvio/clients/go` (package `ruvio`).

- One `Client` multiplexes all concurrent callers over **one TCP connection**: frames that
  queue up while a write is in flight are coalesced into a single write, and replies are
  matched to callers through a FIFO.
- A typed method for **every command in `docs/compatibility-matrix.json` (299/299)** plus the
  options that change what Ruvio does (`SET ... IFEQ`, `INCRBY ... MAX`, `FILLGET ... WAIT`,
  `FILLSET ... SWR`, `GETEX` ms, `DELEX key IFEQ`, `LPUSHCAP/RPUSHCAP`, `BF.*`, `JSON.*` incl.
  `JSON.INDEX`/`JSON.FIND`, `IDEM BEGIN/COMPLETE`, `LEASE/RELEASE`, `SEMAPHORE`, `ONCE`, `TAKE`, `TRANSFER/HTRANSFER`,
  `SETV`, `CHANGES`, `LIMIT`, `XADD ... DELAY`).
- `context.Context` on every call; pipelines; `MULTI/EXEC` transactions with `WATCH`;
  Pub/Sub; RESP3 (`Options.Protocol = 3`) with push frames for `CLIENT TRACKING`.
- Helpers: distributed lock, fenced lease, semaphore, rate limit, cache-aside
  `GetOrCreate`, idempotency, scan iterators, typed JSON, capped "recent" lists, Bloom
  "seen before".

- **Cluster routing** (`Options.Cluster`): `CLUSTER SLOTS` slot table, one lazily connected
  multiplexed connection per node, `-MOVED`, `-ASK` and `-TRYAGAIN` handling, cross-slot `MGET/MSET/DEL/...`
  split and re-assembled. A standalone server is detected and handled as before. See
  [Cluster](#cluster).

Connecting sends nothing unless configured (`AUTH`, `SELECT`, `CLIENT SETNAME`, `HELLO 3`, `CLIENT DURABILITY`).

`Options.DurabilityReplicas` (with an optional `DurabilityTimeout`) sends `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`
on every connection the client opens, cluster nodes and reconnects included, so a reply is held until `n` replicas have
the writes on disk; a late replica turns the replies into `-NOREPLICAS`, and a server without replication refuses the
connect. `ClientDurability(ctx, replicas, timeout)` sets it on one connection (timeout 0 sends no `TIMEOUT`).

## Install

```sh
go get github.com/salihcantekin/ruvio/clients/go@v0.2.0
```

Releases are git tags named `clients/go/vX.Y.Z`.

## Quick start

```go
import ruvio "github.com/salihcantekin/ruvio/clients/go"

ctx := context.Background()
c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6461", DB: 0})
if err != nil { log.Fatal(err) }
defer c.Close()

_ = c.Set(ctx, "greeting", "hello")
v, ok, err := c.Get(ctx, "greeting")           // ok=false when the key is missing

ok, err = c.SetArgs(ctx, "k", "v", ruvio.SetOptions{NX: true, TTL: 30 * time.Second})
ok, err = c.SetIfEq(ctx, "k", "v2", "v", ruvio.SetOptions{})   // Ruvio IFEQ
ok, err = c.SetIfGreater(ctx, "watermark", 1700000000, ruvio.SetOptions{}) // Ruvio IFGT: keep the highest number
n, ok, err := c.IncrByMax(ctx, "quota", 1, 100)                 // nil (ok=false) past 100

// anything else: the escape hatch (string, []byte, ints, floats, bool)
val, err := c.Do(ctx, "OBJECT", "ENCODING", "k")
```

Conventions:

- Nullable replies return `(value, ok, err)`; nullable elements use `NullString`,
  `NullInt`, `NullFloat`.
- Server errors are `*ruvio.Error` (`errors.As`; `Prefix()` gives `WRONGTYPE`, `NOSCRIPT`...).
  Transport failures wrap the cause and match `errors.Is(err, ruvio.ErrClosed)`.
- Binary safety: Go strings carry arbitrary bytes, so every `string` argument is binary
  safe. `GetBytes`, `SetBytes`, `HGetBytes`, `HSetBytes`, `DoBytes` avoid string copies.
- Commands that block the server-side connection (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`,
  `BZPOPMIN/MAX`, `XREAD`/`XREADGROUP` with `Block`, `WAIT`, `FILLGET ... WAIT`) run on a
  pooled **dedicated** connection so the shared one never stalls.
- `Select`/`Hello` on a `Client` change the shared connection for every caller (the database
  is re-selected on reconnect). Prefer `Options.DB` or a dedicated `c.Conn(ctx)`.
- Ruvio's Lua API is `ruvio.call(...)` (there is no `redis.call`).

### Pipelines, transactions, Pub/Sub

```go
vals, err := c.Pipelined(ctx, func(p *ruvio.Pipeline) {
    p.Set("a", "1"); p.Incr("n"); p.Do("HSET", "h", "f", "v")
})  // one write; vals in order; err = first server error (others stay in vals)

res, err := c.Transaction(ctx, func(p *ruvio.Pipeline) { p.Incr("x"); p.Get("x") })

tx, _ := c.Tx(ctx)            // dedicated connection
defer tx.Close()
tx.Watch(ctx, "balance")
cur, _, _ := tx.Get(ctx, "balance")
_, err = tx.Exec(ctx, func(p *ruvio.Pipeline) { p.Set("balance", next(cur)) })
if errors.Is(err, ruvio.ErrTxAborted) { /* retry */ }

ps, _ := c.PubSub(ctx, 0)     // dedicated connection
ps.Subscribe(ctx, "news"); ps.PSubscribe(ctx, "news.*"); ps.SSubscribe(ctx, "shard-ch")
for m := range ps.Channel() { fmt.Println(m.Kind, m.Channel, m.Payload) }
```

`Tx.Multi` / `Tx.Queue` / `Tx.ExecQueued` / `Tx.Discard` give the explicit step-by-step form.

### TTL on create

A counter or a first write often needs a deadline that is set once, when the key appears, and not moved by later writes. The `...WithTTL` methods and the `TTL` option fields do this in one command. The deadline is set only when the call creates the key; a key that already exists keeps its deadline.

```go
n, err := c.IncrWithTTL(ctx, "rate:ada", time.Minute)             // INCR rate:ada EX 60
_, err = c.IncrByWithTTL(ctx, "hits", 5, 1500*time.Millisecond)   // INCRBY hits 5 PX 1500
_, err = c.ZAddArgs(ctx, "board", ruvio.ZAddOptions{TTL: time.Hour}, ruvio.Z{Member: "ada", Score: 10})
err = c.JSONSetWithTTL(ctx, "user:1", "$", `{"name":"Ada"}`, time.Hour)
```

Whole seconds are sent as `EX`, anything else as `PX` in milliseconds. A zero or negative duration returns `ErrInvalidTTL` and nothing is sent. Available on `Incr`, `Decr`, `IncrBy`, `DecrBy`, `IncrByMax`, `IncrByFloat`, `HIncrBy`, `ZIncrBy`, `SetBit` and `JSONSet`/`JSONSetMode` as `...WithTTL` methods, and on `ZAddOptions`, `GeoAddOptions`, `XAddArgs` and `BFInsertOptions` as a `TTL` field.

`ExpireIf`, `PExpireIf`, `ExpireAtIf` and `PExpireAtIf` take an `ExpireCondition` (`ExpireNx`, `ExpireXx`, `ExpireGt`, `ExpireLt`). A key without a deadline counts as having an infinite one, and a failed condition returns `false`.

### Typed keywords

String-typed constants encode through pre-built RESP fragments (no per-call allocation); plain string literals still compile.

```go
id, _, _ := c.XAdd(ctx, ruvio.XAddArgs{Stream: "events", Trim: ruvio.TrimMaxLen.Of(10000).WithMode(ruvio.TrimApproximate), Values: []string{"k", "v"}})
n, _ := c.XTrimBy(ctx, "events", ruvio.TrimMinID.OfID("1700000000000-0"))
text, _ := c.InfoOf(ctx, ruvio.InfoMemory)
n, _ = c.ZCount(ctx, "board", ruvio.ScoreExclusive(5), ruvio.ScorePosInf)
```

## Helpers

| Helper | Built on | Notes |
| --- | --- | --- |
| `AcquireLock`, `AcquireLockWait`, `Lock.Refresh`, `Lock.Unlock`, `WithLock` | `SET NX PX`, `SET ... IFEQ token PX`, `DELEX key IFEQ token` | Random 128-bit token; refresh and unlock act only while the token is ours, so they never touch another holder's lock. `WithLock` always releases (`defer`). |
| `AcquireLease`, `WithLease` | `LEASE`/`RELEASE` | Fencing token grows with every grant. |
| `AcquirePermit`, `WithPermit` | `SEMAPHORE`/`RELEASE` | One of `max` slots, each with its own TTL. |
| `RateLimit(key, max, window)` | `LIMIT` | Returns `(allowed, remaining)`. |
| `LimitSliding(key, max, window)`, `LimitTokens(key, capacity, refill, cost)` | `LIMIT ... SLIDING`, `LIMIT ... TOKENS` | Return `(remaining, allowed)`. |
| `LimitSlidingWithRetry(...)`, `LimitTokensWithRetry(...)` | `LIMIT ... WITHRETRY` | Return a `LimitOutcome` with `Allowed`, `Remaining` and `RetryAfter`. |
| `GetOrCreate`, `GetOrCreateJSON[T]` | `FILLGET`/`FILLSET` | One factory run across callers. On `BUSY` it polls plain `FILLGET` with 2..50 ms backoff, because `FILLGET ... WAIT` would park the shared connection. `Stale` enables SWR: stale readers return the old value and one refreshes in the background. |
| `Idempotent(key, fingerprint, ttl, fn)` | `IDEM BEGIN/COMPLETE` | Returns the stored result to repeats; `ErrIdemPending` / `ErrIdemConflict`. Not exactly-once for external effects. |
| `ScanIter`, `HScanIter`, `SScanIter`, `ZScanIter` (+ `Scanner.All` → `iter.Seq2` on Go 1.23+) | `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | Lazy, one page per round trip. |
| `JSONSetValue`, `JSONGetValue`, `JSONGetAs[T]` | `JSON.SET`/`JSON.GET` + `encoding/json` | |
| `PushRecent`, `Recent` | `LPUSHCAP`, `LRANGE` | Capped "recent items" list. |
| `SeenBefore`, `MaybeSeen` | `BF.MADD`, `BF.MEXISTS` | One round trip per batch. |
| `KeySlot(key)` | local CRC16 | `{hashtag}` aware; matches `CLUSTER KEYSLOT`. |

## Seed addresses

```go
c, err := ruvio.Connect(ctx, ruvio.Options{
    Seeds: []string{"10.0.0.5:6379", "10.0.0.6:6379", "[::1]:6379"},
})
```

`Seeds` (`host:port`, IPv6 in brackets) replaces `Addr` when it is not empty; with no seeds
`Addr` works as before. Connecting tries the seeds in order, one connect attempt each, and keeps
the first that answers. A seed that cannot be reached is skipped, a refused `AUTH` or `SELECT`
stops at once, and when none answers the last connect error is returned. After the connection
drops, the next command reconnects by trying every seed once, starting with the one after the
address that dropped; the command that hit the drop still fails. Dedicated and Pub/Sub
connections start at the address of the last successful connection. A custom `Options.Dial`
receives the seed being tried. With `Cluster: true`, `CLUSTER SLOTS` comes from the first
reachable seed; when a node cannot be opened again, the slot map is reloaded from the nodes
that still have a connection and then from the other known nodes and the seeds, and the
command goes to the owner the new map names. No cost is added to the per-command path.

### Following a failover

With two or more `Seeds` a standalone client finds the primary again after a failover. After
the connection drops, the reconnect asks the seeds `ROLE` and connects to the one that answers
`master` (a replica's reported primary is tried too); if none does, the usual seed order
applies. When a command is answered `READONLY` (the node was demoted) the client does the same,
avoiding the node that answered, and sends that command once more on the new connection:
`READONLY` is sent before the command runs, so the retry cannot apply it twice. A second
`READONLY` is returned, and nothing is sent again after a transaction opened with `Do("MULTI")`
on the shared connection, or after an I/O error. Up to 8 rounds run with waits of 100 ms
doubling to 2 s (7.1 s at most, bounded by the call's context); when none finds a primary the
`READONLY` error is returned and the connection stays. Pipelines, `Conn`, `Tx` and blocking
commands run on their own connection and return `READONLY` as is. Nothing is sent while the
connection is healthy, a single address behaves as before, and a cluster client never does
this. `Options.DisableFollowPrimary` turns it off.

With `Options.SentinelMaster` set, `Seeds` (or `Addr`) are coordinator addresses: the client
asks `SENTINEL get-master-addr-by-name <name>`, checks the answer with `ROLE`, and connects
there, on the first connect (which fails with an error wrapping `ErrProtocol` when no
coordinator names a primary), on reconnect and after `READONLY`. The same `AUTH` credentials
go to coordinators and data nodes. The rules are the same in every client
([docs/CLIENT_BEHAVIOR.md](../../docs/CLIENT_BEHAVIOR.md#following-a-failover)).

```go
c, err := ruvio.Connect(ctx, ruvio.Options{
    Seeds:          []string{"10.0.0.9:26379"},
    SentinelMaster: "main",
})
```

## Cluster

```go
c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6461", Cluster: true})
// c.IsCluster(), c.Nodes() (slot order), c.RefreshSlots(ctx)
```

`Cluster: true` is auto-detect: on first use the client sends `CLUSTER SLOTS` to the seed
and builds a 16384-entry slot table. If the seed answers `-ERR cluster mode is disabled`
(standalone, or a multi-shard server in single-port mode) the client falls back to the plain
single-connection path for good, with no extra cost per call (one nil check). Every typed
method, `Do`, helper, pipeline and transaction works the same in both modes.

Each node gets its own multiplexed connection, dialed lazily (the same fast path as the
standalone connection, including coalesced writes). A dead node connection fails only the
calls pending on it, is never replayed, and is redialed by the next call; other nodes are
untouched. The shard layout is `shard i = base port + i` (`port_mode = "per-shard"`); seed
with any of the ports.

How commands route (every command of `docs/compatibility-matrix.json` has an entry, checked
by `TestRoutingCoversMatrix`; the key table is in `cluster_keys.go`):

| Command kind | Routing |
|---|---|
| One key at argument 1 (`GET`, `SET`, `HSET`, `ZADD`, `JSON.*`, `BF.*`, `XADD`, `LEASE`, `LIMIT`, `FILLGET`, `SPUBLISH`, ...) | node owning the key's slot (`{hashtag}` aware) |
| Keys elsewhere: `OBJECT/MEMORY/XGROUP/XINFO sub key`, `EVAL/EVALSHA/FCALL numkeys keys`, `ZUNION/ZINTER/ZDIFF/LMPOP/SINTERCARD/BLMPOP numkeys keys`, `ZUNIONSTORE dst numkeys keys`, `XREAD/XREADGROUP ... STREAMS keys ids`, `BITOP op dst keys`, `BLPOP keys timeout`, `JSON.MGET keys path`, `GEORADIUS ... STORE dst`, `GEOSEARCHSTORE dst src`, `LMOVE/RENAME/COPY/SMOVE` | real key lists are extracted; all keys must share one slot, else `ErrCrossSlot` is returned **before anything is sent** |
| `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TOUCH` spanning slots | split into one command per slot (the server refuses several slots in one command, even on one node), one pipelined write per node, nodes concurrent, replies re-assembled in the caller's order (`MGET` array, `MSET` OK, counts summed). Not atomic across slots |
| Keyless (`PING`, `ECHO`, `INFO`, `CONFIG`, `CLIENT`, `ACL`, `CLUSTER *`, `PUBLISH`, `CHANGES`, `JSON.INDEX/FIND`, ...) | the seed node |
| `DBSIZE` (summed), `KEYS` (joined), `FLUSHDB`, `FLUSHALL`, `SCRIPT LOAD/FLUSH`, `FUNCTION LOAD/DELETE/FLUSH` | every node, concurrently |
| `SCAN` | walks the nodes one after the other; the cursor encodes `serverCursor*nodes+nodeIndex`, `"0"` ends the walk. `ScanIter` just works. Treat the cursor as opaque; a topology change in the middle of a walk can skip or repeat keys. `HSCAN/SSCAN/ZSCAN` are per key |
| Blocking commands (`BLPOP`, `BLMOVE`, `XREAD BLOCK`, `FILLGET WAIT`, ...) | a dedicated connection to the owning node |

`-MOVED slot host:port`: the slot is patched in the table at once and the command is sent
again to the named node (the server did not run it, so this is safe). The whole table is
reloaded with `CLUSTER SLOTS` only when the named node is unknown or from the second `MOVED` for
the same command. `-ASK slot host:port`: `ASKING` and the command are sent to the named node in
one write and its reply is returned; the table is not changed, so the next command for the slot
still goes to the table's owner. `-MOVED` and `-ASK` share one budget: a command follows at
most **3** redirects; a fourth is returned to the caller as `*Error`. `-TRYAGAIN` (a slot is
being handed over; nothing ran) sends the command again, routed by the current table, up to 3
times after waits of 5, 10 and 20 ms (a cancelled context ends the wait); the third retry's
`-TRYAGAIN` is returned as `*Error`. The budgets are separate.

Pipelines: commands are grouped by node, each group is one pipelined write, groups run
concurrently, replies come back in the caller's order; `MOVED`, `ASK` and `TRYAGAIN` replies inside a pipeline are
repaired one by one, with the same limits. `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`
and `TOUCH` whose keys span slots are split exactly like the plain command: one command per
slot, each sent in its place in its node's write, and the merged reply (`MGET` in key order,
counts summed, `MSET` OK) sits at the command's index; an error in one part becomes that
index's error. Any other command whose keys span slots fails the whole pipeline with
`ErrCrossSlot` unsent. A pipeline containing `MULTI` is a transaction: all its keys must share
a slot (nothing is split) or the call fails with `ErrCrossSlot` unsent. A pipeline does not fan
out `DBSIZE`/`SCAN` (they go to one node as plain commands).

Transactions and dedicated connections (`Tx`, `Transaction`, `Conn`): the connection is taken
from the node owning the first key it sees (`Watch`, the queued commands, or an explicit
`ConnFor(ctx, key)` / `TxFor(ctx, key)`), and every later key must hash to the same slot,
otherwise `ErrCrossSlot`. A `Multi()` before any key is known is deferred and sent with the first
keyed command. Keyless commands on an unbound connection bind it to the seed node.

Pub/Sub: `Subscribe/PSubscribe` run on the seed node, so `Publish` (keyless, seed node) reaches
them. `SSubscribe` opens one extra connection per node owning a channel's slot (opened on
demand, one `SSUBSCRIBE` per slot); `SPublish` routes by channel. Messages from all nodes
arrive on the one `Channel()`. Losing any of these connections closes the subscription.

### Reads from replicas

`Options.ReadPolicy` (type `ReadPolicy`) chooses where a cluster client sends reads. Replicas
come from `CLUSTER SLOTS`, which lists them after each primary; a standalone connection
ignores the option.

| Value | Reads go to |
|---|---|
| `ReadPrimary` (default) | the slot's primary, as before; no replica connection is opened |
| `ReadPreferReplica` | one replica of the slot's shard, the primary when it has none or the replica is unusable |
| `ReadReplica` | a replica only; with none known or usable the read fails (the error says "no replica") |

Replication is asynchronous, so a read from a replica can miss recent writes, including your
own. A command is a read when the server's command table flags it `readonly` and not blocking
(`GET`, `HGETALL`, `ZRANGE`, ...); the parts of a split `MGET` or `EXISTS` are reads, those of
`DEL` or `MSET` are writes. Writes, keyless commands, transactions, Pub/Sub, `SCAN`, fan-out
and blocking commands always go to the primaries. A pipeline goes to replicas only when every
command in it is a read; otherwise it is routed as with `ReadPrimary`.

When the slot map is loaded, each primary gets one replica picked at random, kept until the
next load; each connection to it sends `READONLY` first. A replica that cannot be connected,
or answers a read with `LOADING`, is skipped until the next load: `ReadPreferReplica` sends the
read to the primary (for `LOADING`, again), `ReadReplica` returns the error. A replica socket
that drops after a command was sent fails that command, which is not sent again. A `LOADING`
reply inside a pipeline is returned as that command's error.

Helpers (lock, lease, semaphore, rate limit, `GetOrCreate`, idempotency, scan iterators, JSON,
capped lists, Bloom) only issue single-key commands and work unchanged.

Measured on a 4-core box shared by client and server (3-shard cluster server vs 1-shard
standalone server, 16 x GOMAXPROCS goroutines, 64 B values, 256 keys, `-benchtime 3s`; run
`go test -bench 'Cluster|Standalone|Fallback' -run '^$'` with both variables set):

| | standalone | cluster |
|---|---|---|
| serial `GET` | 33.2 us/op | 31.1 us/op |
| concurrent `GET` | 3.3 us/op (~300k ops/s) | 5.4 us/op (~185k ops/s) |
| concurrent `SET` | 3.2 us/op (~310k ops/s) | 6.2 us/op (~160k ops/s) |
| `Cluster: true` on a standalone server, concurrent `GET` | 3.0 us/op | |
| `MGET` of 16 keys in 16 slots | | 149 us/op, 99 allocs |

Serial calls cost the same, so routing is free; concurrent throughput is lower on this box
because three server shard threads and the benchmark compete for four cores and each node
connection coalesces fewer frames per write. This is **not** a statement about a multi-machine
deployment (not measured).

Cost: routing a single-key command reads the command name and the key out of the already
encoded frame (about 56 ns, 0 allocations, `BenchmarkRouteFrameSet`); the slot is a table-driven CRC16
(27 ns). Nothing is added to the standalone path.

## Performance design

1. **One multiplexed connection.** Callers encode their frame into a pooled request
   buffer, then (under one short mutex) append it to the connection's output buffer and
   push the request onto a FIFO ring.
2. **Write coalescing without timers.** The first caller that finds no write in flight
   writes inline (serial calls go out immediately, no goroutine hop). Frames that arrive
   while that write runs accumulate and are flushed together by a writer goroutine. No
   batch-fill wait, no timer. Two output buffers are swapped (double buffering).
3. **Pre-encoded tokens.** Every command name and option is a complete RESP bulk fragment
   (`$3\r\nGET\r\n`) built once at init (`tokens.go`).
4. **No int→string allocation.** Integers and floats are written with
   `strconv.AppendInt/AppendFloat` straight into the frame; typed methods never box
   arguments into `[]any`.
5. **Hand-rolled resumable RESP2/RESP3 parser** over one reusable read buffer (no
   `bufio`). Partial aggregates stay on the parser stack across reads, so big replies parse
   in linear time. Integer and `+OK`/`+QUEUED`/`+PONG` replies allocate nothing; a bulk
   allocates exactly its value; the bulks of one array share one arena allocation.
   Typed string results view the owned bytes without a second copy.
6. **Pooled requests, reusable done channels.** Request structs, their buffers and their
   `chan struct{}` completion signals come from a `sync.Pool`; no per-call channel or
   closure is allocated. A canceled context abandons the request with an atomic CAS and the
   reader recycles it when the reply arrives, so the FIFO never desynchronizes.

Measured on this machine (4 vCPU Xeon @ 2.8 GHz VM, loopback, Ruvio 0.3.0 single shard,
`go test -bench . -benchmem -benchtime=1s`, a busy shared host so numbers vary run to run):

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Encode typed `SET k v PX 60000` | 69 | 0 | 0 |
| Encode generic `AppendCommand` | 67 | 0 | 0 |
| Parse integer | 26–34 | 0 | 0 |
| Parse `+OK` | 23–29 | 0 | 0 |
| Parse 64-byte bulk | 89–174 | 64 | 1 |
| Parse array of 10 bulks | 790–2160 | 960 | 2 |

7. **Serial calls read their own reply.** The read side has a single owner token. A caller
   that finds nobody reading takes it and reads its reply directly from the socket (no
   goroutine hand-off, no wake-up of a separate reader); if other requests are still
   pending it passes the token to the background reader. A context deadline is enforced
   with the socket read deadline. Callers whose context can be canceled (no deadline) wait
   on the reader instead. Pub/Sub and RESP3 (push frames) keep the background reader as
   permanent owner.

Live results (`bench_test.go`, `BenchmarkLive*`; 64 goroutines share one connection for the
parallel cases): serial GET ~40 us/op and SET ~39 us/op (1 and 0 allocs: the returned
value), parallel GET ~3.9 us/op and SET ~3.4 us/op (~260-290k ops/s) with ~0.42-0.46 socket
writes per command, 16-command pipeline ~1.5 us/command. A bare Go `net.Conn` write+read
loop on the same server measured 21-52k ops/s (19-47 us) in the same period, so serial
latency is dominated by the server/loopback round trip, not the client. Numbers on this
shared 4-vCPU VM vary 2x between runs.

Run them yourself:

```sh
RUVIO_TEST_ADDR=127.0.0.1:6461 go test -run '^$' -bench . -benchmem -benchtime=1s
```

## Tests

```sh
go vet ./...
go test ./...                                   # codec unit tests + coverage test; live tests skip
RUVIO_TEST_ADDR=127.0.0.1:6461 go test -race ./...   # + live tests (database 11 and 12, FLUSHDB)
RUVIO_CLUSTER_ADDR=127.0.0.1:17310 go test -race ./... # + cluster live tests (3-shard, per-shard ports)
```

Cluster server for the tests:

```sh
D=$(mktemp -d); cp ruvio.example.toml $D/ruvio.toml
sed -i 's/^shards = 1/shards = 3\nport_mode = "per-shard"/' $D/ruvio.toml
ruvio --config $D/ruvio.toml --address 127.0.0.1:17310 --data-dir $D/data &
```

The cluster tests (`cluster_test.go`: slot table, hashtags, key extraction for every key
position kind, `MOVED`, scripted fake nodes; `cluster_live_test.go`: keys spread over all
shards, cross-slot `MGET/MSET/DEL` order (plain and in pipelines), pipeline order, forced stale-table recovery,
transactions, helpers, blocking, `SCAN`, Pub/Sub, scripts, concurrency, standalone fallback
with `Cluster: true`) skip without their server variables.

`follow_test.go` (scripted nodes) and `follow_live_test.go` (the failover pair
`scripts/run_with_servers.sh` starts: `RUVIO_FAILOVER_ADDRS`, `RUVIO_FAILOVER_REPLICATION`)
cover following a failover; the live test skips without the variables.

The live suite exercises every command family against a real server (strings, keys,
bitmaps/HLL, Ruvio extensions, lists, sets, hashes incl. field TTLs, sorted sets, geo,
streams incl. consumer groups and `DELAY`, JSON incl. `JSON.INDEX/FIND` (database 0, own
prefix, cleaned up), Bloom, connection/server, ACL, scripting/functions, Pub/Sub, RESP3
client tracking, pipelines, transactions, every helper). `TestCoverage` reads
`docs/compatibility-matrix.json` and fails if a command has no typed method.

## Not done / not verified

- Cluster, verified against a 3-shard per-shard-port Ruvio server and a scripted fake server:
  topology, routing, split, pipelines, `MOVED`, `ASK`, `TRYAGAIN`, transactions, Pub/Sub, helpers, fallback.
  Reads from replicas (`Options.ReadPolicy`) are verified with scripted fake nodes and a live
  2-shard primary with its replica. **Not done / not verified:** replicas that change address
  between slot map loads while a read is in flight, `-ASK` and `-TRYAGAIN` against a live server (only scripted fake nodes; the server has no live
  slot migration), `RENAME`/`COPY`/`SMOVE`/`LMOVE` across slots (fail with `ErrCrossSlot`; no
  copy-and-delete emulation), split `MSETNX`/`BITOP`/`SUNION`... (all-or-nothing or
  non-decomposable commands fail across slots), automatic topology refresh in the background
  (the table is refreshed on `MOVED` or `RefreshSlots`; a node that is gone is found out by the
  next call to it), multi-database (a cluster server only has database 0), `CLIENT SETNAME`,
  `CLIENT TRACKING` and `HELLO 3` push frames are per connection and `OnPush` is passed to every
  node's shared connection but only exercised on a standalone server, authentication on a
  cluster (`AUTH` is sent on each new node connection; verified only on standalone), TLS to a
  cluster. A cluster that advertises a different host than the seed was reachable at is used as
  advertised. `PUBLISH` reaches only subscribers on the node it is sent to (the seed).
- `ACL LOAD`/`ACL SAVE` success paths were verified with `security.acl_file` set;
  `SAVE`/`BGSAVE` only against the memory profile.
- `OBJECT IDLETIME/FREQ` were verified only to the server's policy error (default
  `noeviction`).
- `HELLO 3` on the shared connection works (the parser speaks RESP3), but typed results
  for RESP3-only shapes are tested only for maps (`HGETALL`, `HELLO`) and pushes.
- No automatic retry/replay after a transport failure (by design); the next call redials.
- No write deadline: if the server stops reading, the writer blocks and queued frames grow.
- Large values are copied once from the request buffer into the connection buffer.
- Built and tested with Go 1.24.7; the `go 1.22` floor is **not verified** on a 1.22 toolchain.

Server deviations found while testing (Ruvio 0.3.0), handled or documented by the client:

- A blocked `XREAD`/`XREADGROUP` that is woken by a later `XADD` replies
  `[[stream, [id, fields]]]` instead of `[[stream, [[id, fields]]]]`; the client accepts both.
- `ZRANGE key max min BYSCORE REV` takes the high bound first, as Redis does (the server fixed this on `dev`; older builds expected `min max`).
- Scripts must use `ruvio.call`; `redis.call` is undefined.
