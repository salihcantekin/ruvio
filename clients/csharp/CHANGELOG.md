# Changelog

Notable changes to the C# packages `Ruvio.Client` and `Ruvio.Client.AspNetCore`, which share one version. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Tracing and metrics per command: each command sent through the typed methods, `ExecuteAsync` or `ExecuteBinaryAsync` starts a client span named after it on the `Ruvio.Client` activity source (`db.system`, `db.operation.name`, `db.namespace`; error status on failure; the existing socket spans become its children) and records `ruvio_client_command_duration_milliseconds` on the `Ruvio.Client` meter (`db.operation.name`, `outcome`). Keys and values are never recorded. Without a listener a command pays one check and allocates nothing more. Enable with `AddSource("Ruvio.Client")` / `AddMeter("Ruvio.Client")`; no OpenTelemetry package is needed.
- TLS: `RuvioClientOptions.Tls` (a `RuvioTlsOptions`) wraps every socket the client opens (first connect, reconnects, pooled, per-shard, replica and Pub/Sub sockets) in TLS, checking the certificate chain and host name. `TrustedCertificates` trusts a private CA (instead of the system store), `ClientCertificates` gives mutual TLS, `ServerName` covers a certificate named differently from the host, and `InsecureSkipVerify` (off) skips the check. The handshake runs inside the connect, so a refused certificate throws `AuthenticationException`. With TLS on `DedicatedReaderThread` has no effect. `Ruvio.Client.AspNetCore` binds `Tls`, `TlsServerName`, `TlsCaFile`, `TlsClientCertFile`, `TlsClientKeyFile` and `TlsInsecureSkipVerify` from configuration. Plain TCP stays the default; nothing changes without `Tls`.
- `EvaluateCachedAsync(script, keyCount, arguments)` (an extension method on `IRuvioClient`) runs a script by its SHA1 and sends the whole script only when the shard answers `NOSCRIPT` (`EVALSHA`, then `EVAL` once). The SHA1 is computed in the client; nothing changes for `eval` and `evalsha`.
- `RuvioOutageMode`, `RuvioOutageOptions`, `RuvioCircuitBreaker` (with `RuvioCircuitBreakerOptions`, `RuvioCircuitState` and `RuvioCircuitOpenException`): the building blocks the .NET integrations use to let an application choose what happens when Ruvio is unreachable (throw, fail closed, fail open) and to stop calling Ruvio for a while after repeated outage failures. Nothing in the client itself changes; they are used only when an integration's `Outage` option is set.
- Typed helpers for JSON and text values, so callers need no `byte[]`: `GetJsonAsync<T>` / `SetJsonAsync<T>` (`JsonTypeInfo<T>`, trimming and AOT safe), `FillGetJsonAsync<T>` / `FillSetJsonAsync<T>` returning `FillResult<T>`, `FillResult.Text` and `IdempotencyBegin.ResultText` (UTF-8 decoded). All are extension methods on `IRuvioClient`: nothing changes for code that does not call them, and the `byte[]` members stay.
- A cluster client now retries `-TRYAGAIN` up to 7 times, after waits doubling from 5 ms to 320 ms (635 ms in all), instead of 3 times after 5, 10 and 20 ms. A slot range moving between the nodes of a cluster is frozen for a few hundred milliseconds (about 170-240 ms in a local test), so the caller saw `TRYAGAIN` on every such move before. Only `TRYAGAIN` replies wait; other replies are unchanged.
- Fixed: following a failover after a `READONLY` reply skipped the node that refused in every round of the search, so a replica promoted a moment later (the usual case after the primary dies: the reconnect lands on the replica during the election) was used only once all rounds had run out, about 7 s with the default backoff. The refusing node is now skipped only while it still says `master` (a replaced primary does); once it answers `ROLE` as a replica, a later round takes it when it says `master`. Nothing changes while the connection is healthy.
- A standalone client follows a failover. On a reconnect with several seeds it asks them `ROLE` and connects to the one that answers `master` (a replica's reported primary is tried too), falling back to the old seed order; on a `READONLY` reply to a command it does the same and sends that command once more to the new primary, except inside a transaction it opened or in a pipeline. Nothing is sent while the connection is healthy. New `RuvioClientOptions.FollowPrimary` (default `true`; `false` restores the old behavior) and `RuvioClientOptions.SentinelMaster`, which makes the seeds coordinator or Sentinel addresses asked with `SENTINEL get-master-addr-by-name`. Subscriber sockets are not moved.
- `SetDurabilityAsync(replicas, timeout)` sends `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`, and the `RuvioClientOptions.DurabilityReplicas` / `DurabilityTimeout` options send it on every connection the client opens (reconnects, pooled and per-node connections included); nothing is sent by default.
- A cluster client follows `-ASK slot host:port` (sends `ASKING` and the command to the named node, without changing the slot map) and retries `-TRYAGAIN` up to 3 times after 5, 10 and 20 ms. `ASK` shares the 3-redirect budget with `MOVED`; replies that are neither cost nothing extra. Pipelines follow them like `MOVED`.
- Fixed: stream reads (`XRANGE`, `XREVRANGE`, `XREAD`, `XREADGROUP` and `XCLAIM`) preserve repeated field names and their order instead of keeping only the last value for each name. Contributed by @ertugrulakdag (#88).
- **Behavior change:** `RuvioClientOptions.CommandTimeout` is now 30 seconds by default (was no limit); `TimeSpan.Zero` turns it off. A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus the timeout; a wait of 0, or one the client cannot parse, has no deadline; a pipeline takes its largest wait), and a Pub/Sub or `MONITOR` connection never times out while it waits for messages (issue #48 follow-up).
- The command timeout costs no allocation and no timer per command: one timer per connection fails the sent requests whose deadline has passed (their late replies are still read and dropped), and a caller with no cancellation of its own reads its own reply again. Measured on one connection, GET/SET with the default on allocated about 650 B more per call than with it off (a linked `CancellationTokenSource`, a timer, an async wrapper and a closure); now the same as off. With it off, calls also lose a closure (about 170 B) that the earlier code allocated on every call. A stream write that takes no data for the whole timeout now closes the connection (issue #48 follow-up).
- `MoveSlotsAsync(first, last, shard)` and `GetMoveSlotsStatusAsync()` for `RUVIO MOVESLOTS`, which moves a slot range to another shard of the same server.
- `SetHashManyAsync(key, fields)` sends `HMSET` (multi-field `HSET` that replies `OK`; Redis marks it deprecated) (issue #26).
- `RuvioClientOptions.CommandTimeout`: a command throws `TimeoutException` when its reply takes longer; the connection stays in use (issue #48).
- `RuvioClientOptions.ReadPolicy` (`ReadPolicy.Primary` by default, `PreferReplica`, `Replica`): a cluster client sends reads (commands the server flags `readonly` and not blocking) to one replica per shard, picked when the slot map loads. Pipelines go to replicas only when every command reads. A replica that cannot be opened or answers `LOADING` is skipped until the next load. `Ruvio.Client.AspNetCore` is unchanged.
- `ReplicaOfAsync(host, port)`, `ReplicaOfNoOneAsync(force)` and `GetRoleAsync()` for `REPLICAOF` and `ROLE`.
- `RuvioClientOptions.Seeds`: `host:port` seed addresses tried in order instead of `Host`/`Port`. A dropped standalone connection reconnects starting with the next seed; a cluster reloads its slot map from the other known nodes and then the seeds when no open socket answers. `Ruvio.Client.AspNetCore` binds a `Seeds` array from configuration.
- TTL on create: overloads that take a `TimeSpan ttl` and set the deadline only when the call creates the key (an existing key keeps its deadline; whole seconds are sent as `EX`, otherwise `PX`): `IncrementAsync`, `DecrementAsync`, `IncrementByAsync`, `DecrementByAsync`, `IncrementWithLimitAsync`, `IncrementByFloatAsync`, `IncrementHashFieldAsync`, `IncrementSortedSetAsync`, `SetBitAsync`, `AddSortedSetAsync`, `AddGeoAsync`, `AddStreamEntryAsync` and `JsonSetAsync`; `BloomInsertOptions.Ttl` for `BF.INSERT`. A zero or negative `ttl` throws `ArgumentOutOfRangeException`.
- New `ExpireCondition` (`Nx`, `Xx`, `Gt`, `Lt`) and `ExpireAsync` / `ExpireAtAsync` overloads that send it with `EXPIRE`, `PEXPIRE`, `EXPIREAT` or `PEXPIREAT`. The new `IRuvioClient` members have default implementations, so existing implementations keep compiling.
- `PopSortedSetByScoreAsync` and `MoveDueSortedSetAsync` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call. Not compiled or run in the cloud session that added them.
- New `TransferAsync(source, destination, amount, max)` and `TransferHashFieldAsync(key, from, to, amount, max)` for the server's `TRANSFER` and `HTRANSFER`: they return the two balances after the move, or `null` when nothing moved. Not built or tested in the environment this was written in (no .NET SDK).

- `LimitSlidingAsync`, `LimitTokensAsync`, `LimitSlidingWithRetryAsync` and `LimitTokensWithRetryAsync` for the `SLIDING` and `TOKENS` modes of `LIMIT`, and the `LimitOutcome` result (needs a server that has them). `IRuvioClient` gains four members.

## 0.3.4 - 2026-10-17

- Wave 2 of typed keywords: `StreamTrim` (`MaxLen` or `MinId`, `StreamTrimMode.Exact` `=` or `Approximate` `~`) for `XADD` and `XTRIM` (`AddStreamEntryAsync` and `TrimStreamAsync` overloads; the server does not accept `LIMIT`); `InfoSection` for `GetInfoAsync`; `ClientType` for `GetClientListAsync`; `StreamIds`, `ScoreBound` and `LexBound` constants and builders for the stream and sorted-set range arguments. `XTRIM`, `INFO` and `CLIENT LIST` write the keyword from a shared pre-encoded token. The client has no `CHANGES SUBSCRIBE` wrapper, so no change-type set was added.
- Typed keyword overloads: `GeoUnit` and `GeoOrder` for `GEODIST`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS` and `GEORADIUSBYMEMBER` (the order was always `ASC` before); `InsertPosition` for `LINSERT`; `ListSide` for `LMOVE`, `BLMOVE`, `LMPOP` and `BLMPOP`; `KeyType` for `SCAN ... TYPE`; `BitFieldStep`, `BitFieldType` and `BitFieldOverflow` for `BITFIELD` and `BITFIELD_RO`. The string and bool overloads are unchanged. The commands built inline (`GEODIST`, `GEORADIUSBYMEMBER`, `LINSERT`, `LMOVE`, `BLMOVE`) write the keyword from a shared pre-encoded token, with no string encoding per call.

## 0.3.3 - 2026-10-17

- A call that cannot be cancelled and finds nobody reading now reads its own reply instead of waiting for a reader to hand it over through the thread pool. User CPU per request fell 3-14% on small pipelines and cluster pipelines on a 4-shard local server.
- New `RuvioClientOptions.DedicatedReaderThread` (off by default; `DedicatedReaderThread` in ASP.NET Core configuration): each command socket keeps blocking I/O and gets a reader thread, skipping the async socket engine and a thread-pool hand-off per round trip. On a 4-shard local server it cut user CPU per request by about 45% for 16-command pipelines on one socket and by 25-35% for pipelines split over the shards, at the cost of one thread per socket. Pub/Sub sockets are unchanged.
- Cluster pipelines are cheaper: commands are assigned to nodes in one pass without a dictionary or upper-cased names, each node's commands are encoded once into a pooled frame, every node is written before any reply is awaited, and only error replies are checked for `MOVED`. Pipeline allocation per request fell by a fifth to a half; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- Fixed: an integer reply of `-9223372036854775808` (`long.MinValue`, for example from `INCRBY` or `DECRBY`) was rejected as "bad RESP integer" and closed the connection; it is now read as `long.MinValue`.
- Behaviour change: `RENAME` and `RENAMENX` whose keys hash to different slots now fail with a `CROSSSLOT` `RuvioException` before anything is sent, instead of being emulated with `TYPE`, a copy, `PEXPIRE` and `DEL`. Keep both keys in one slot with a hash tag.
- Behaviour change: any other command whose keys span slots and that is not split (`COPY`, `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ...) now fails with `CROSSSLOT` before it is sent. Before, it went to the first key's shard and the server answered.
- Behaviour change: in a pipeline (not a transaction), `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are split per slot like the plain command, and the merged reply sits at that command's index. Before, the whole command went to the first key's shard.
- Behaviour change: a command whose socket dropped before any reply byte is no longer sent again. Before, a single `GET`, `MGET`, `EXISTS`, `HGET`, `TTL` or `PTTL` was resent once on a new socket when `Reconnect` was on. The socket is still opened again for the next command; `RuvioException.ClosedBeforeReply` says when a read is safe to resend.
- `MOVED` points that one slot at the named node and retries there, instead of reloading the whole slot map every time. The map is reloaded when the named node is unknown and on a command's second and third `MOVED`; a fourth is returned. The `MOVED` that turns a plain connection into a cluster router counts as the first.
- `ExecuteBinaryAsync` in cluster mode: `DEL`, `UNLINK` and `EXISTS` with keys in several slots are split per slot and the counts summed (they threw `CROSSSLOT` before); `EVAL`/`EVALSHA` across slots fail with the same `CROSSSLOT` message as the string API.
- Cluster mode: each command's key positions now come from the server's command table (`Cluster/CommandKeyRules.g.cs`, generated from `clients/testdata/commands.json`) instead of hand-kept lists, and `KeyVectorTests` checks them against `clients/testdata/key_vectors.json`. This fixes routing for `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE`/`STOREDIST` (the destination key was missed), `GEOSEARCHSTORE` (only the destination was used, not the source), and `XGROUP` and `XINFO` (the subcommand was taken as the key instead of the stream name).

## 0.2.8 to 0.3.2

These notes were recorded together and shipped over these versions.

- Coverage: typed methods for every command in `docs/compatibility-matrix.json` (287/287), including `SET ... IFEQ`, `DELEX`, `LPUSHCAP`/`RPUSHCAP`, `BF.MADD/INSERT/INFO/CARD/MEXISTS`, `BF.RESERVE EXPANSION/NONSCALING`, `JSON.SET NX/XX`, `JSON.INDEX`/`JSON.FIND`, the remaining `JSON.*`, bitmaps, HyperLogLog, `SSCAN`/`ZSCAN`, `ZUNION`/`ZINTER`/`ZDIFF`, `BLMPOP`, `COMMAND`, `LATENCY`, `WAIT`, `AUTH` and `RUVIO`. New `IRuvioClient` members have default implementations that go through `ExecuteAsync`, so existing implementations keep compiling.
- Helpers on `IRuvioClient`: `TryAcquireLockAsync`/`AcquireLockAsync` (`RuvioLock`, `IAsyncDisposable`), `TryAcquireLeaseAsync`, `TryAcquireSemaphoreAsync` (`RuvioLease`), `RateLimitAsync`, `GetOrCreateAsync`, `ScanKeysAsync`/`ScanHashEntriesAsync`/`ScanSetMembersAsync`/`ScanSortedSetEntriesAsync` (`IAsyncEnumerable`), `PushRecentAsync`/`GetRecentAsync`, `SeenBeforeAsync`/`MaybeSeenAsync`.
- `ExecuteAsync(string[])` and every list-path command encode the array through a span instead of the `IReadOnlyList<string>` interface.
- `MatrixCoverageTests` and the live `LiveCommandTests`/`LiveHelperTests` (`RUVIO_TEST_ADDR`).
- Build: the source generator references Microsoft.CodeAnalysis.CSharp 4.8.0 (was 4.11.0), so it loads in the .NET 8.0.1xx SDK compiler; before, the generator was skipped there and the client did not compile (`RespVerbs` missing).
- Breaking: every command method on `RuvioClient` / `IRuvioClient` returns `ValueTask` or `ValueTask<T>` instead of `Task` / `Task<T>`. A pending command is a pooled object that is reused once it completes. Await each result **once** and do not keep it after awaiting. Do not call `.Result` / `GetAwaiter().GetResult()` on a result that has not completed. To use `Task.WhenAll` / `WhenAny` or a `Task`-based interface, call `.AsTask()` once. Usual `await client.GetAsync(key)` code only needs to be compiled again. On a local memory server with 16 callers, this cut allocations from about 1.1–1.4 KB to 56–336 B per command, and the 336 B left on `GET` is mostly the reply payload.
- Frequent replies are shared rather than allocated: null, `+OK`, `+PONG`, `+QUEUED`, the empty bulk string, and integers from -2 to 255. `RespValue` is immutable, so this is not visible to callers, but do not compare replies by reference.
- Typed commands no longer build a `string[]` or format integers into strings. Arguments are stored inline in a `RespCommand` struct, and numbers are written straight to the buffer as invariant UTF-8. Simple-string replies such as `+OK` are matched without a string. A `SET` (typed or raw) now allocates 0 B per command; a `GET` allocates only the reply payload. Passing a `null` string argument now throws `ArgumentNullException` immediately instead of returning a faulted task.
- Hash field TTLs: `ExpireHashFieldsAsync`, `ExpireHashFieldsAtAsync`, `GetHashFieldTimeToLiveAsync`, `GetHashFieldTimeToLiveMillisecondsAsync`, `GetHashFieldExpireTimeAsync`, `GetHashFieldExpireTimeMillisecondsAsync`, and `PersistHashFieldsAsync` send `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, and `HPERSIST`.
- `AcquireSemaphoreAsync` sends `SEMAPHORE`; `ReleaseAsync` frees a semaphore token as well as a lease. `IncrementWithLimitAsync` sends `INCRBY key delta MAX limit` and returns null when the limit would be passed.
- Sorted sets: `PopSortedSetMinAsync`, `PopSortedSetMaxAsync`, `PopSortedSetMinBlockingAsync`, `PopSortedSetMaxBlockingAsync`, `RemoveSortedSetRangeByRankAsync`, `RemoveSortedSetRangeByScoreAsync`, and `RemoveSortedSetRangeByLexAsync` send `ZPOPMIN`, `ZPOPMAX`, `BZPOPMIN`, `BZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, and `ZREMRANGEBYLEX`.
- Cluster mode: a single-key command (`GET`, `SET`, `INCR`, …) is routed from its inline arguments without building a key list, and each topology keeps one socket per node instead of looking it up by an `"host:port"` string on every call. Pipelines that hit one shard are sent as they are; multi-shard pipelines no longer use LINQ.
- Array, integer-array, scan, stream, and `GEOADD` replies and requests are built with exact-size arrays instead of LINQ or closures.
- A `GET` sent on a socket that closed before any reply byte is sent once more on a new socket, as `ExecuteAsync` already did. Before, the fast `GET` path threw instead.

## 0.2.7

- The library is AOT-compatible and trimmable. `Ruvio.Client.Tour` publishes a native binary.
- `GET`, `INCR`, and `GEOADD` on a standalone connection write the command name from a generated UTF-8 span.

## 0.2.6

- A dropped command or shard socket is opened again before the next command. The command that sees the drop fails. A single `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, or `PTTL` is sent once more when no reply byte arrived. Pipelines are not retried. Pub/Sub channels are subscribed again on the new socket.
- `ConnectionLost` and `ConnectionRestored` (`RuvioConnectionNotice`: host, port, subscriber, error). `ConnectionFailed` still fires on the drop.
- `RuvioClientOptions.Reconnect` (default true), `MaxReconnectAttempts`, `ReconnectBaseDelay`, `ReconnectMaxDelay`, and `ReconnectDelay`. `ReconnectDelay` returning null stops the retry and ignores `MaxReconnectAttempts`. `ReconnectAsync` opens the socket when automatic retry is off.

## 0.2.5

- Pub/Sub uses a dedicated socket. `SubscribeAsync(channel, handler)`, `Message`, and `ListenAsync` deliver payloads without a manual read loop on the command connection.
- `IsConnected` and `ConnectionFailed`.

## 0.2.4

- `DeleteAsync(key)` sends `DEL` for one key. The list overload remains for many keys.
- `PopListLeftBlockingAsync(key, timeout)` and `PopListRightBlockingAsync(key, timeout)` send `BLPOP` / `BRPOP` without a key list.

## 0.2.3

- A `MOVED` reply after a plain connect (or with discovery off) learns the slot map and retries, so `SET` on the seed port no longer throws.

## 0.2.2

- A sharded server is discovered from one seed address. Commands are routed by hash slot, `MOVED` is followed, and `MGET` / `MSET` / `DEL` split across shards.
- `GetClusterShardsAsync`, `GetClusterInfoAsync`, `GetClusterMyIdAsync`, `GetClusterKeySlotAsync`, `ReadOnlyAsync`, `ReadWriteAsync`.
- `GetHashSlot`, `IsCluster`, `ShardCount`, and `RuvioClientOptions.DiscoverCluster`.

## 0.2.1

- `AddSortedSetAsync(key, ("ada", 100), ("bob", 50))` takes `(member, score)` tuples, with or without `SortedSetAddFlags`.
- `AddSortedSetAsync(key, member, score)` adds one member and returns whether it was new.

## 0.2.0

- `ConnectAsync(host, port, database)`, `ConnectAsync(IPAddress, port, database)`, `ConnectAsync(IPEndPoint, database)`, and `RuvioClientOptions.Database`.
- `RuvioConnection.GetDatabaseAsync`, `FlushDatabaseAsync`, `FlushAllDatabasesAsync`, `SwapDatabasesAsync`, `MoveKeyAsync`.
- The option overloads in the table above, and `PingAsync(message)`.
- Breaking: `LoadFunctionAsync(body)` became `LoadFunctionAsync(name, body)`. The old form sent a request Ruvio always refused. `DeleteFunctionAsync` returns `Task<bool>`.
