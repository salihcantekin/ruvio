# Changelog

Notable changes to the Node.js client `ruvio-client` on npm. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `clientDurability(replicas, timeoutMs?)` for `CLIENT DURABILITY`, and a `durability` connection option (`{ replicas, timeoutMs? }`) that sends it on every (re)connect, so replies wait until that many replicas have the data on disk. Nothing is sent when the option is not set.
- `ClusterClient` follows `-ASK slot host:port` (sends `ASKING` and the command to the named node in one write; the slot map is not changed) and retries `-TRYAGAIN` up to 3 times after 5, 10 and 20 ms. `MOVED` and `ASK` share the 3-redirect limit per command; only error replies are checked, so normal replies cost nothing extra.
- **Behavior change:** `StreamEntry.fields` is now an array of `[name, value]` pairs in stored order instead of an object, so a field name written twice is no longer collapsed to its last value and numeric names such as `"2"` and `"1"` keep their order; use `Object.fromEntries(entry.fields)` for the old shape. Applies to `xrange`, `xrevrange`, `xread`, `xreadgroup`, `xclaim` and `xautoclaim`.
- **Behavior change:** `commandTimeoutMs` is now 30000 by default (was 0, no limit); `0` turns it off. A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus the timeout; a wait of 0, or one the client cannot parse, has no deadline; a pipeline takes its largest wait), and a Pub/Sub or `MONITOR` connection never times out while it waits for messages (issue #48 follow-up).
- The command timeout adds no work per command: only frames whose command may block (`BLPOP`, `XREAD`, `WAIT` and the like) are scanned for their wait; one interval timer per connection does the rest, as before (issue #48 follow-up).
- `moveSlots(first, last, shard)` and `moveSlotsStatus()` for `RUVIO MOVESLOTS`, which moves a slot range to another shard of the same server.
- `hmset(key, fields)` sends `HMSET` (multi-field `HSET` that replies `OK`; Redis marks it deprecated) (issue #26).
- `commandTimeoutMs` option: the connection fails (pending calls reject with a `ConnectionError`, a later command reconnects) when replies are outstanding and none arrives for that long. `connectTimeoutMs` now defaults to 5000 ms (was no limit; `0` still means none), as in the other clients (issue #48).
- `readPolicy` option on `ClusterClient` (`'primary'` by default, `'prefer-replica'`, `'replica'`; new `ReadPolicy` type): reads go to one replica per shard, picked when the slot map loads. A pipeline goes to replicas only when every command in it is a read. A replica that cannot be opened or answers `LOADING` is skipped until the next load.
- `replicaOf(host, port)`, `replicaOfNoOne(force)` and `role()` for `REPLICAOF` and `ROLE`.
- `seeds` option (`host:port` strings) on every client: tried in order instead of `host`/`port`. A dropped connection reconnects starting with the next seed; a cluster reloads its slot map from the other known nodes and then the seeds when no open socket answers. `seeds` with `path` throws `TypeError`.
- TTL on create: an optional `ttl` (seconds; whole numbers send `EX`, fractions send `PX`) on `incr`, `decr`, `incrby`, `decrby`, `incrbyMax`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `jsonSet`, `jsonSetAs`, and in `ZAddOptions`, `GeoAddOptions` (new type), `XAddOptions` and `BfInsertOptions`. The deadline is set only when the call creates the key. A `ttl` that is not positive and finite rejects with `RangeError` before anything is sent. New `TtlOptions` type.
- `expire`, `pexpire`, `expireat` and `pexpireat` take an optional trailing `ExpireCondition` (`NX`, `XX`, `GT`, `LT`).
- `zpopbyscore` and `zmovedue` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `transfer(source, destination, amount, max?)` and `htransfer(key, from, to, amount, max?)` for the server's `TRANSFER` and `HTRANSFER`: they return `[a, b]` after the move, or `null` when nothing moved.

- `limitSliding`, `limitTokens`, `limitSlidingWithRetry` and `limitTokensWithRetry` for the `SLIDING` and `TOKENS` modes of `LIMIT` (needs a server that has them).

## 0.2.0 - 2026-10-17

- Named keyword constants next to the string unions (`GeoUnit.Km`, `GeoOrder`, `ListSide`, `InsertPosition`, `BitOp`, `BitUnit`, `Aggregate`, `ScanType`, `ExpireCondition`), sent from pre-encoded buffers. `ScanOptions.type` is typed by `ScanType`; `BitfieldType` is now `` `i${number}` | `u${number}` ``; `BitfieldOp` builds `BITFIELD` sub-operations. String literals keep working.
- `StreamTrim` (`MAXLEN` / `MINID`, exact `=` or approximate `~`) is accepted by `xtrim` and as `XAddOptions.trim`; `InfoSection` types `info()`, `ClientType` types `clientList()`; both send pre-encoded buffers. New helpers `StreamId` (`*`, `$`, `>`, `-`, `+`), `ScoreBound` and `LexBound` return the strings the range methods accept. The client has no `CHANGES SUBSCRIBE` API, so no change-type set was added.

## 0.1.2 - 2026-10-17

- Cluster routing is cheaper: a command whose first argument is its key is routed without scanning the key rules, a cluster batch is routed in one pass into reused scratch and written unchanged when every command goes to one node, and only error replies are checked for `MOVED`. See `docs/evidence/2026-10-16-client-cluster-routing.md`.
- The cluster client now takes each command's key positions from the server's command table instead of its own hand-kept lists. This fixes routing of `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE` / `STOREDIST`, which used to ignore the destination key.
- **Behaviour change:** the cluster client now follows up to 3 `MOVED` redirects per command (was 1) before returning the `MOVED` error. A `MOVED` to a known node only repoints that slot; the whole slot table is re-read only when the named node is unknown or on the second `MOVED` for the same command (it used to be re-read once a second slot had moved, whichever command saw it).

## 0.1.1

- Same code as 0.1.0.

## 0.1.0

- First release.
