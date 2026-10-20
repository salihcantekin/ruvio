# Changelog

Notable changes to the Go client `github.com/salihcantekin/ruvio/clients/go`. A Go version is the git tag `clients/go/vX.Y.Z`; move the "Unreleased" notes under the new version by hand when tagging. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `Options.DurabilityReplicas` / `Options.DurabilityTimeout` and `ClientDurability(ctx, replicas, timeout)` for `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`: the server holds a connection's replies until `n` replicas have the writes on disk. The option is sent on every new connection; nothing is sent by default.
- A cluster client now follows `-ASK slot host:port` (sends `ASKING` and the command to the named node, slot map unchanged) and retries `-TRYAGAIN` up to 3 times after 5, 10 and 20 ms. `-ASK` shares the 3-redirect budget with `-MOVED`; pipelines repair both the same way. The plain client is unchanged.
- **Behavior change:** `Options.CommandTimeout` is now 30 seconds when left 0 (was no limit); a negative value turns it off. A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus the timeout; a wait of 0, or one the client cannot parse, has no deadline; a pipeline takes its largest wait), and a Pub/Sub or `MONITOR` connection never times out while it waits for messages (issue #48 follow-up).
- The command timeout costs no allocation per call: the socket read deadline is set once per connection and the reader fails overdue requests (their late replies are still read and dropped), instead of `context.WithTimeout` per call. Measured: a GET with the default on went from 6 allocations (448 B) to 1 (64 B, the same as off), a SET from 5 (384 B) to 0. A cluster pipeline that spans nodes now gets the timeout too (issue #48 follow-up).
- `MoveSlots(ctx, first, last, shard)` and `MoveSlotsStatus(ctx)` for `RUVIO MOVESLOTS`, which moves a slot range to another shard of the same server.
- `HMSet(ctx, key, fieldValues...)` sends `HMSET` (multi-field `HSET` that replies `OK`; Redis marks it deprecated) (issue #26).
- `Options.CommandTimeout` bounds a call whose context has no deadline (it fails with `context.DeadlineExceeded`, the connection stays usable) (issue #48).
- `Options.ReadPolicy` (`ReadPrimary` by default, `ReadPreferReplica`, `ReadReplica`): a cluster client sends reads (commands the server flags readonly and not blocking) to one replica per shard, found in `CLUSTER SLOTS` and picked when the slot map loads. Pipelines go to replicas only when every command is a read; a replica that cannot be connected or answers `LOADING` is skipped until the next load. Replication is asynchronous, so reads can miss recent writes. With the default policy nothing changes.
- `ReplicaOf(ctx, host, port)`, `ReplicaOfNoOne(ctx, force)` and `Role(ctx)` for `REPLICAOF` and `ROLE`.
- `Options.Seeds`: `host:port` seed addresses tried in order instead of `Addr`. A dropped connection reconnects starting with the next seed; a cluster reloads its slot map from the other known nodes and then the seeds when no open socket answers. A custom `Dial` receives the seed being tried.
- TTL on create: `IncrWithTTL`, `DecrWithTTL`, `IncrByWithTTL`, `DecrByWithTTL`, `IncrByMaxWithTTL`, `IncrByFloatWithTTL`, `HIncrByWithTTL`, `ZIncrByWithTTL`, `SetBitWithTTL`, `JSONSetWithTTL` and `JSONSetModeWithTTL` send `EX` (whole seconds) or `PX` and set the deadline only when the call creates the key. New `TTL` field on `ZAddOptions`, `GeoAddOptions`, `XAddArgs` and `BFInsertOptions`. A zero or negative TTL returns the new `ErrInvalidTTL` without contacting the server.
- Conditional expiry: typed `ExpireCondition` (`ExpireNx`, `ExpireXx`, `ExpireGt`, `ExpireLt`) and `ExpireIf`, `PExpireIf`, `ExpireAtIf`, `PExpireAtIf`.
- `ZPopByScore` and `ZMoveDue` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `Transfer`, `TransferMax`, `HTransfer` and `HTransferMax` for the server's `TRANSFER` and `HTRANSFER`: both balances after the move, and `ok == false` when nothing moved.

- `LimitSliding`, `LimitTokens`, `LimitSlidingWithRetry` and `LimitTokensWithRetry` for the `SLIDING` and `TOKENS` modes of `LIMIT`, and the `LimitOutcome` result (needs a server that has them).

## v0.2.0 - 2026-10-17

Breaking for callers that pass a plain `string` variable where a typed keyword is now expected (see the first item); string literals still compile.

- Typed keyword constants (string types, so string literals still compile): `GeoUnit`, `GeoOrder`, `ListSide`, `ListPosition`, `ScanType`, `BitOperation`, `BitUnit`, `Aggregate`, `BitFieldOpName`, plus `BitSigned`/`BitUnsigned` helpers. They encode through pre-built RESP fragments (no per-call allocation). New `LInsertAt` and `BitCountUnit`; `LInsert(bool)` and `BitCountRange(bool)` are unchanged. Callers that pass a plain `string` variable to `GeoDist`, `LMove`, `BLMove`, `LMPop`, `BLMPop` or `BitOp`, or assign one to `GeoSearchQuery.Unit/Sort`, `GeoRadiusQuery.Unit/Sort`, `ZStore.Aggregate`, `ScanOptions.Type` or `BitFieldOp.Op`, need a conversion such as `ruvio.GeoUnit(s)`.
- Typed keywords, wave 2: `StreamTrim` (`TrimStrategy` MAXLEN/MINID, `TrimMode` `=`/`~`, `TrimMaxLen.Of`, `TrimMinID.OfID`) used by new `XAddArgs.Trim` and `XTrimBy` (`XTrim` unchanged); `InfoSection` with `InfoOf`; `ClientType` with `ClientListType`; constants `StreamIDNew/LastDelivered/NewMessages/Min/Max`, `ScoreNegInf/PosInf`, `LexMin/Max` and helpers `ScoreExclusive`, `LexInclusive`, `LexExclusive`. All encode through pre-built RESP fragments. The Go client has no `CHANGES SUBSCRIBE` API, so no change-type set was added.

## v0.1.1 - 2026-10-17

- A cluster pipeline spread over several nodes no longer allocates a map, per-node slices and a goroutine per node: commands are routed once into pooled scratch, every node is written before any reply is awaited, the calling goroutine reads the replies itself instead of handing them to each node's background reader, and only the reply slice is allocated (2.6-3.0× less CPU per request on a 4-shard local server). See `docs/evidence/2026-10-16-client-cluster-routing.md`.
- **Behaviour change:** a cluster command now follows up to 3 `MOVED` redirects (was 1) before the `MOVED` error is returned. Each `MOVED` patches that slot; the whole slot table is reloaded only when the named node is unknown or from the second `MOVED` of the same command on (before, any second moved slot since the last reload triggered a reload). Pipelined commands follow the same limit.
- **Behaviour change:** a pipeline holding `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS` or `TOUCH` whose keys span slots is no longer refused as a whole with `ErrCrossSlot`; the command is split per slot like the plain command and its merged reply sits at its index. Other cross-slot commands, and any cross-slot key inside a `MULTI` pipeline, still fail with `ErrCrossSlot` before anything is sent.
- Fixed: the `ErrCrossSlot` error for a pipeline could be built from an already recycled buffer (a panic or a wrong command name in the message).
- The cluster client now takes each command's key positions from the server's command table (`command_keys_gen.go`, generated from `clients/testdata/commands.json`) instead of hand-kept lists, and is checked against the shared `clients/testdata/key_vectors.json`. No routing bug was found: the old lists already gave the server's keys for all 1,062 vectors.

## v0.1.0

- First release.
