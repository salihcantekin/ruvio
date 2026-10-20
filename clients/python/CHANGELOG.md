# Changelog

Notable changes to the Python client `ruvio-client` on PyPI. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `client_durability(replicas, timeout=None)` sends `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`, and the `durability_replicas=` / `durability_timeout_ms=` options send it on every connection the sync, asyncio and cluster clients open, so replies wait until that many replicas have the writes on disk.
- The sync and asyncio cluster clients follow `-ASK slot host:port` (they send `ASKING` and the command to the named node in one write, and leave the slot map alone) and retry a command answered `-TRYAGAIN` up to 3 times after 5, 10 and 20 ms. `ASK` and `MOVED` share one budget of 3 redirects per command. `TRYAGAIN` stays a plain `ResponseError`; it is only looked for on error replies. A pipeline gets it as that command's error.
- **Behavior change:** `StreamEntry.fields` is now a list of `(field, value)` tuples in stored order instead of a `dict`, so a field name written twice (`XADD s * f 1 f 2`) is no longer collapsed to its last value; use `dict(entry.fields)` for the old shape. Applies to `xrange`, `xrevrange`, `xread`, `xreadgroup`, `xclaim`, `xautoclaim` and `xinfo_stream`. Fixed: `hrandfield(key, -n, withvalues=True)` returned each field once; it now returns all `n` pairs, repeats included.
- **Behavior change:** `socket_timeout` of the sync client is now 30.0 seconds by default (was `None`, no limit); `None` turns it off. A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus the timeout; a wait of 0, or one the client cannot parse, has no deadline; a pipeline takes its largest wait), and a Pub/Sub or `MONITOR` connection never times out while it waits for messages (issue #48 follow-up). The asyncio client still has no per-command timeout.
- `move_slots(first, last, shard)` and `move_slots_status()` for `RUVIO MOVESLOTS`, which moves a slot range to another shard of the same server, sync and asyncio.
- `hmset(key, mapping)` sends `HMSET` (multi-field `HSET` that replies `OK`; Redis marks it deprecated) (issue #26).
- Docs: `socket_timeout` is the command timeout; the README timeout table lists the matching option of every client (issue #48).
- Added `read_policy=` (`ReadPolicy.PRIMARY`, `PREFER_REPLICA`, `REPLICA`, or the strings `"primary"`, `"prefer-replica"`, `"replica"`) to the sync and asyncio cluster clients: commands the server flags read-only and not blocking go to one replica of the key's shard, picked when the slot table loads; a pipeline goes to replicas only when every command in it reads. A replica that cannot be connected or answers `LOADING` is skipped until the next load. Reads may miss recent writes (replication is asynchronous). The default keeps today's routing.
- `replicaof(host, port)`, `replicaof_no_one(force=False)` and `role()` for `REPLICAOF` and `ROLE`, sync and asyncio.
- Added `seeds=` (`"host:port"` strings or `(host, port)` tuples) to the sync and asyncio clients, `ClusterRuvio` and `from_url`: tried in order instead of `host`/`port`. A dropped connection reconnects starting with the next seed; a cluster reloads its slot map from the other known nodes and then the seeds when no open socket answers.
- Added keyword-only `ttl=` (seconds as `int`/`float`, or `timedelta`) to `incr`, `decr`, `incrby`, `decrby`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `zadd`, `geoadd`, `xadd`, `json_set`, `json_set_obj` and `bf_insert`. The deadline is set only when the call creates the key; an existing key keeps its deadline. Whole seconds send `EX`, a sub-second part sends `PX`; zero or negative raises `ValueError`.
- Added `ExpireCondition` (`NX`, `XX`, `GT`, `LT`) and the optional `condition=` argument on `expire`, `pexpire`, `expireat` and `pexpireat`. A key without a deadline counts as infinite; a failed condition returns `False`.
- `zpopbyscore` and `zmovedue` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `transfer(source, destination, amount, max=None)` and `htransfer(key, from_field, to_field, amount, max=None)` for the server's `TRANSFER` and `HTRANSFER`: they return a `(a, b)` tuple after the move, or `None` when nothing moved.

- `limit_sliding`, `limit_tokens`, `limit_sliding_with_retry` and `limit_tokens_with_retry` for the `SLIDING` and `TOKENS` modes of `LIMIT`, and the `LimitOutcome` result (needs a server that has them).

## 0.1.2 - 2026-10-17

- Added `StreamTrim` (`MAXLEN` or `MINID`, `=` or `~`, threshold) for `xadd(trim=...)` and `xtrim(trim=...)`, the `InfoSection`, `ClientType` and `ChangeType` enums (with `change_types()`), `StreamId`, `ScoreBound` and `LexBound` constants, and `score_exclusive()`, `lex_inclusive()` and `lex_exclusive()` bound builders. Existing string arguments keep working.
- Added typed keyword constants (`GeoUnit`, `GeoOrder`, `InsertPosition`, `ListSide`, `BitOperation`, `BitUnit`, `Aggregate`, `KeyType`, `BitFieldOp`, `Overflow`, `signed`, `unsigned`). They are `str` enums, so existing string arguments keep working; the encoder sends each member's pre-encoded bytes.

## 0.1.1 - 2026-10-17

- Cluster routing is cheaper: the command name and first key are read with one match per frame, a pipeline is routed in one pass into reused per-node buffers, every node is written before any is read, and a pipeline for one node goes out unchanged. Cluster CPU per pipelined request fell 8-17%; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- Fixed: with the optional `hiredis` parser installed, commands that answer `OK` (`set`, `select`, `auth`, `client_setname` and others) returned `False` instead of `True` when replies were not decoded (`decode_responses=False` or the `binary` view), because hiredis returns the reply as `b"OK"`.
- **Behavior change:** the cluster client now follows up to 3 `-MOVED` redirects per command (was 1) before raising `MovedError`. A command's first `MOVED` to a known node updates only that slot; the whole slot map is re-read only when the named node is unknown or the same command gets a second `MOVED` (before, any second `MOVED` since the last reload re-read it). Transactions follow the same limit.
- **Behavior change:** a cluster pipeline (not a transaction) containing `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` or `TOUCH` with keys in several slots is no longer refused with `CrossSlotError`: the command is split per slot like the plain command, and its place in the reply list holds the merged reply. A transaction must still use one slot.
- The cluster client now takes each command's key positions from the server's command table instead of its own hand-written lists, and is tested against the server's key vectors. No server command was routed by the wrong keys before; commands the server does not have (`BRPOPLPUSH`, `BZMPOP`, `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`, `LCS`, `ZINTERCARD`, `ZMPOP`, `ZRANGESTORE`, `LASTSAVE`, `QUIT`, `RESET`, `TIME`) now go by argument 1 like any other unknown command.

## 0.1.0

- First release.
