# Changelog

Notable changes to the Rust client crate `ruvio-client`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- A pipeline now waits for one `-TRYAGAIN` budget (the 635 ms schedule), shared by all its commands, instead of up to 635 ms for each refused command in turn; a command not yet retried when the budget has run out still gets one more try. A shorter `command_timeout` shortens the budget. Before, a pipeline of 100 commands on a frozen slot range could wait over a minute. A single command waits as before (issue #312).
- `renew(key, token, ttl_ms)` sends the server's `RENEW`: it extends a `LEASE` or `SEMAPHORE` holder by its token without releasing it (1 when the token is a live holder, 0 otherwise). Lease and permit handles gain `renew`. Needs a server that has `RENEW`.
- `Error::is_transient()` and `Error::may_have_run()` sort an error for a retry loop or policy of your own: transient means trying again later can succeed (`Error::Io`, or the replies `LOADING`, `MASTERDOWN`, `CLUSTERDOWN`, `TRYAGAIN`, `READONLY`, `NOREPLICAS`); may have run is false only for a server error reply other than `NOREPLICAS`. The same rules as the other clients. Nothing changes on the command path.
- `idem_abort(key, fingerprint, owner)` (and the raw `idempotency_abort`) sends `IDEM key ABORT fingerprint owner`: it removes a pending record of that owner and fingerprint so the next `BEGIN` acquires the key at once, and returns `false` for a missing key, a completed record or another owner. New `idempotent_abort_on_error`, the opt-in twin of `idempotent`: when the work fails it sends `ABORT` and returns the work's error (a failed abort is ignored). `idempotent` is unchanged. Needs a server that has `IDEM ... ABORT`.
- TLS: the new cargo feature `tls` (off by default; the default build still has no dependencies) adds `ClientOptions::tls: Option<TlsOptions>`. With it set every socket the client opens (first connect, reconnects, per-shard and replica sockets) runs over TLS with the certificate chain and host name checked. `TlsOptions` holds `ca_pem` (trust only this private CA; otherwise the public web roots), `client_cert_pem` and `client_key_pem` (mutual TLS), `server_name` (when the certificate names something other than the host) and `insecure_skip_verify` (off). The handshake runs inside the connect, so a refused certificate fails `connect_with` with `Error::Io`. Without the feature, `tls: Some(..)` fails the connect with `Error::Protocol`. Plain TCP is unchanged. `ClientOptions` built with a struct literal needs `..Default::default()`.
- `Client::eval_cached(script, keys, arguments)` runs a script by its SHA1 and sends the whole script only when the shard answers `NOSCRIPT` (`EVALSHA`, then `EVAL` once). The SHA1 is computed in the client; nothing changes for `eval` and `evalsha`.
- A cluster client now retries `-TRYAGAIN` up to 7 times, after waits doubling from 5 ms to 320 ms (635 ms in all), instead of 3 times after 5, 10 and 20 ms. A slot range moving between the nodes of a cluster is frozen for a few hundred milliseconds (about 170-240 ms in a local test), so the caller saw `TRYAGAIN` on every such move before. Only `TRYAGAIN` replies wait; other replies are unchanged.
- Fixed: following a failover after a `READONLY` reply skipped the node that refused in every round of the search, so a replica promoted a moment later (the usual case after the primary dies: the reconnect lands on the replica during the election) was used only once all rounds had run out, about 7 s with the default backoff. The refusing node is now skipped only while it still says `master` (a replaced primary does); once it answers `ROLE` as a replica, a later round takes it when it says `master`. Nothing changes while the connection is healthy.
- A standalone client follows a failover. On a reconnect with several seeds it asks them `ROLE` and connects to the one that answers `master` (a replica's reported primary is tried too), falling back to the old seed order; on a `READONLY` reply to a command it does the same and sends that command once more to the new primary, except inside a transaction it opened or in a pipeline. Nothing is sent while the connection is healthy. New `ClientOptions::follow_primary` (default `true`; `false` restores the old behavior) and `ClientOptions::sentinel_master`, which makes the seeds coordinator or Sentinel addresses asked with `SENTINEL get-master-addr-by-name`. `ClientOptions` built with a struct literal needs `..Default::default()`.
- `client_durability(replicas, timeout_ms)` sends `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`, and `ClientOptions::durability` sends it on every connection the client opens (including reconnects and per-shard sockets) right after `AUTH`; a refusal fails the connect. Default `None` sends nothing. `ClientOptions` built with a struct literal needs `..Default::default()`.
- A cluster client now follows `-ASK slot host:port`: it sends `ASKING` and the command to the named node in one write and returns the command's reply, without changing the slot map. `ASK` and `MOVED` share the limit of 3 redirects per command. A `-TRYAGAIN` reply is retried (see the entry above for the schedule) before it is returned. Normal replies are not affected.
- **Behavior change:** `ClientOptions::command_timeout` is now `Some(30 s)` by default (was `None`, no limit); `None` turns it off. It also applies after a reconnect and on new shard sockets, but not on Pub/Sub sockets, and `MONITOR` drops it from its socket. A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus the timeout; a wait of 0, or one the client cannot parse, has no deadline; a pipeline takes its largest wait), and a Pub/Sub or `MONITOR` connection never times out while it waits for messages (issue #48 follow-up).
- `move_slots(first, last, shard)` and `move_slots_status()` for `RUVIO MOVESLOTS`, which moves a slot range to another shard of the same server.
- `hmset(key, fields)` sends `HMSET` (multi-field `HSET` that replies `OK`; Redis marks it deprecated) (issue #26).
- `ClientOptions::command_timeout` applies `Client::set_read_timeout` to every command socket right after connecting (issue #48).
- `ClientOptions::read_policy` with `ReadPolicy::{Primary, PreferReplica, Replica}`: a cluster client sends read-only commands to a replica of the slot's shard (listed by the server in `CLUSTER SLOTS`), with a fallback to the primary under `PreferReplica`. The default keeps every command on the primaries. `ClientOptions` built with a struct literal needs `..Default::default()`.
- `replicaof(host, port)`, `replicaof_no_one(force)` and `role()` for `REPLICAOF` and `ROLE`.
- `ClientOptions::seeds`: a list of `(host, port)` seed addresses tried in order. A dropped standalone connection reconnects starting with the next seed, and a cluster slot map reload asks the other known nodes and the seeds when no open socket answers. `ClientOptions` built with a struct literal needs `..Default::default()`.
- TTL on create: a deadline set only when the call creates the key (an existing key keeps its deadline), sent as `EX` for whole seconds and `PX` otherwise.
  - New methods `incr_with_ttl`, `decr_with_ttl`, `incr_by_with_ttl`, `decr_by_with_ttl`, `incr_by_max_with_ttl`, `incr_by_float_with_ttl`, `hincr_by_with_ttl`, `zincr_by_with_ttl`, `setbit_with_ttl`, `xadd_with_ttl`, `xadd_trim_with_ttl`, `json_set_with_ttl` and, with `serde`, `json_set_value_with_ttl`.
  - `SortedSetAddOptions` (`zadd_with`, `zadd_incr`, `geo_add_with`) and `BloomInsertOptions` (`bf_insert`) gain `ttl: Option<Duration>`. Code that builds them with a struct literal needs `..Default::default()`.
- `ExpireCondition` (`Nx`, `Xx`, `Gt`, `Lt`) with `expire_if`, `pexpire_if`, `expire_at_if` and `pexpire_at_if`.
- `zpopbyscore` and `zmovedue` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `transfer` and `htransfer` for the server's `TRANSFER` and `HTRANSFER`: they return `Some((a, b))` after the move, or `None` when nothing moved.

- `limit_sliding`, `limit_tokens`, `limit_sliding_with_retry` and `limit_tokens_with_retry` for the `SLIDING` and `TOKENS` modes of `LIMIT`, and the `LimitOutcome` result (needs a server that has them).

## 0.4.0 - 2026-10-17

- New typed trim, `INFO`, `CLIENT LIST` and change-feed keywords (all constant strings, nothing formatted per call).
  - `StreamTrim` (`max_len` / `min_id` with `TrimMode::Exact` `=` or `Approximate` `~`, `TrimStrategy`) drives the new `xadd_trim` and `xtrim`; `xadd_maxlen`, `xtrim_maxlen` and `xtrim_minid` stay. The server does not accept `LIMIT`.
  - `InfoSection` and `info_section` (`memorybreakdown`, `commandcounters`, `prometheus`, ...); `info(Option<&str>)` stays.
  - `ClientType` and `client_list_type` for `CLIENT LIST TYPE`.
  - `ChangeType` and the `ChangeTypes` set (`to_wire()` gives the `TYPES` argument of `CHANGES SUBSCRIBE`).
  - Constants `stream_id::{NEW, LAST_DELIVERED, NEW_MESSAGES, MIN, MAX}` and `bound::{NEG_INF, POS_INF, LEX_MIN, LEX_MAX, score_exclusive, lex_inclusive, lex_exclusive}` for the range commands.

- **Breaking:** keyword arguments are typed enums that map to constant strings, so no keyword is formatted or allocated per call.
  - `geo_dist`, `geo_search`, `geo_search_store`, `geo_radius` and `geo_radius_by_member` take a `GeoUnit` (`Meters`, `Kilometers`, `Feet`, `Miles`) instead of `&str`; `GeoSearch.unit` is a `GeoUnit` and `GeoSearch::radius` takes one.
  - `GeoSearch.descending: Option<bool>` is now `order: Option<GeoOrder>` (`Asc`, `Desc`).
  - `linsert` takes an `InsertPosition` (`Before`, `After`) instead of `before: bool`.
  - `lmove`, `blmove`, `lmpop` and `blmpop` take a `ListSide` (`Left`, `Right`) instead of `from_left` / `to_left` booleans.
  - `bitfield` and `bitfield_ro` take `&[BitFieldOp]` (`Get`, `Set`, `IncrBy`, `Overflow`) with `BitFieldType`, `BitFieldOffset` and `BitFieldOverflow` instead of `&[&str]` words.
  - `scan` has a fourth argument, `Option<KeyType>`, for the `TYPE` filter; pass `None` for the old behaviour.
  - `BitOperation`, `BitUnit` and `Aggregate` gain `as_str` / `as_bytes`.

## 0.3.2 - 2026-10-17

- `Client::sharded_subscriber(channel)`: a Pub/Sub socket to the shard that owns a sharded channel, for `ssubscribe` on a cluster (a `subscriber()` socket reaches one shard only, so `SSUBSCRIBE` of a channel on another shard got `MOVED`).
- Cluster routing is cheaper: commands no longer allocate on the way to their node (no upper-cased name, key list or `host:port` string per command), connections are found by node index, and a pipeline spread over several nodes writes every node before reading any, so it costs one round trip instead of one per node. A node that fails mid-pipeline no longer leaves the other nodes' sockets out of step. On a 4-shard local server cluster user CPU per pipelined request fell 3.6-21×; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- **Behaviour change:** `RENAME` / `RENAMENX` whose keys hash to different slots now fail with
  `Error::Server("CROSSSLOT Keys in request do not hash to the same slot")` before anything is
  sent, instead of being emulated with `TYPE`, a copy, `PEXPIRE` and `DEL`. Put both keys under one
  hash tag.
- **Behaviour change:** any other command whose keys span slots and that is not split (`COPY`,
  `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ...) now fails with the same `CROSSSLOT` error
  before sending, instead of going to the first key's shard.
- **Behaviour change:** in `execute_many`, `MGET` / `MSET` / `DEL` / `EXISTS` / `UNLINK` / `TOUCH`
  with keys in several slots are split per slot like the plain command, and the merged reply sits
  at the command's index (they used to go whole to the first key's shard). Any other cross-slot
  command gets the `CROSSSLOT` error at its index; the rest of the pipeline still runs.
- A `MOVED` reply now points that slot at the named node and sends the command again there. The
  whole slot map is reloaded only when the node is unknown to the client, or on the command's
  second and third `MOVED` (it used to be reloaded on every `MOVED`). The slot keeps pointing at the
  named node even when the reloaded map names another owner. Still at most 3 redirects per
  command; the 4th `MOVED` is returned.
- The cluster client now takes each command's key positions from the server's command table
  (`src/command_keys.rs`, generated from `clients/testdata/commands.json`) instead of its own
  hand-kept lists, and is checked against `clients/testdata/key_vectors.json`. This fixes routing
  for `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE`/`STOREDIST` (the destination key was
  ignored) and `GEOSEARCHSTORE` (the source key was ignored).

## 0.2.6 to 0.3.1

These notes were recorded together and shipped over these versions.

- Typed methods for every matrix command (287/287): `DELEX`, `SET ... IFEQ`, `LPUSHCAP`/`RPUSHCAP`,
  the full `BF.*` and `JSON.*` families including `JSON.INDEX`/`JSON.FIND`, bitmaps, HyperLogLog,
  `SSCAN`/`ZSCAN`, `ZUNION`/`ZINTER`/`ZDIFF` and their stores, `DUMP`/`RESTORE`, `WAIT`, `COMMAND`,
  `LATENCY`, and the rest of the strings, sets, sorted sets and hashes.
- Helpers: `LockGuard`, `LeaseGuard`, `SemaphorePermit`, `rate_limit`, `get_or_create`,
  `idempotent`, `scan_iter`/`hscan_iter`/`sscan_iter`/`zscan_iter`, `push_recent`/`recent`,
  `seen_before`/`maybe_seen`.
- `execute_binary`, `set_bytes`, `hash_slot_bytes`, `RespValue::into_string` / `into_bytes`.
- Fewer allocations on existing commands: fixed-arity commands no longer build `Vec<String>`.

## 0.2.5

- `geo_add`, `geo_dist`, `geo_hash`, `geo_pos`, `geo_search`, `geo_search_store`, `geo_radius`, and `geo_radius_by_member`.

## 0.2.4

- A dropped command or shard socket is opened again before the next command. The command that sees the drop returns an error. Subscriptions on a dedicated Pub/Sub socket are sent again after it reopens.
- `on_connection_lost` and `on_connection_restored` take a `ConnectionNotice` (`host`, `port`, `subscriber`).
- `ClientOptions::reconnect` (default true), `max_reconnect_attempts`, `reconnect_base_delay`, `reconnect_max_delay`, `set_reconnect_delay`, and `reconnect`. `None` from the delay stops the retry. `max_reconnect_attempts` is not applied while a custom delay is set.

## 0.2.3

- `subscribe` / `psubscribe` / `ssubscribe` open a dedicated Pub/Sub socket so command traffic stays on the original connection. `subscriber()` still returns that extra socket if you want it separately. `listen` calls a callback for each delivery.

## 0.2.2

- `del_key`, `unlink_key`, `exists_key`, `watch_key`, `blpop_key`, and `brpop_key` take one key. The slice methods remain for many keys.

## 0.2.1

- A sharded server is discovered from one seed address. Commands are routed by hash slot, `MOVED` is followed, and `MGET` / `MSET` / `DEL` split across shards.
- `cluster_shards`, `cluster_info`, `cluster_myid`, `cluster_keyslot`, `readonly`, `readwrite`.
- `hash_slot`, `is_cluster`, `shard_count`, and `ClientOptions::discover_cluster`.

## 0.2.0

- Typed methods for every command Ruvio supports.
- `ClientOptions::database`, `connect_database`, `connect_ip`, `connect_addr`, `select`, `into_database`, and `database`.
- `set_expires_at` (`PXAT`), `set_keep_ttl` (`KEEPTTL`), `set_and_get` (`GET`), and `ping_message`.
- `expire` sends `PEXPIRE` when the duration is not whole seconds. It used to round up to the next second.
- `RespValue::as_integer`, `as_array`, and `is_null`.
- The minimum Rust version is 1.87.
