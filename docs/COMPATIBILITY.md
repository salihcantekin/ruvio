# Redis Compatibility Contract

Ruvio implements a tested subset of the Redis serialization protocol and
command semantics. This page describes the current contract. Anything not
listed as supported is unsupported, even if a parser accepts part of its
syntax.

## Native JSON (Ruvio extension)

`JSON.SET key path json` stores a validated JSON document as a distinct
`json` key type; `TYPE` reports `json` and ordinary string commands reject it
with WRONGTYPE. `JSON.GET key [path]` returns compact JSON as a bulk reply or
null when the key/path is absent. `JSON.SET` returns `OK` on change and null
when the target cannot be reached; a missing key can only be created at `$`.
`JSON.DEL key [path]` returns 1 when a field, array element, or the entire key
is removed, and 0 for absent keys/paths. `JSON.MERGE key path patch` (alias
`JMERGE`) applies RFC 7396 Merge Patch to the selected value: object members
merge recursively, null object members delete fields, and arrays and other
non-object patches replace the selected value. For example, merging
`{"a":null}` into `{"a":1}` produces `{}`, while merging the patch `null`
into that document replaces its root with JSON null. A missing key can only be
created at `$`; for an object patch, RFC 7396 treats its initial target as an
empty object (including removal of patch members set to null). A final object
field can be created; an absent intermediate path returns a null bulk reply.
A successful merge returns `OK`. This command is not RFC 6902 JSON Patch.
An omitted path means `$` for reads and deletes. An existing key of another
type gives WRONGTYPE; invalid JSON or path syntax gives an error. Root and path
updates preserve the key's TTL.

`JSON.TYPE`, `JSON.STRLEN`, `JSON.ARRLEN` and `JSON.OBJKEYS` read the value at a path (null
reply when the key or path is absent, WRONGTYPE when the value has another JSON type).
`JSON.NUMINCRBY`, `JSON.TOGGLE`, `JSON.ARRAPPEND` and `JSON.CLEAR` compute a replacement and
store it as a path `JSON.SET`, so they keep the TTL and are logged and replicated as path writes.
Replies are plain values, not RedisJSON's array wrappers for `$` paths. A path may also start
with `.` or `[` (RedisJSON's legacy form, `.` meaning the root); it is stored as the `$` path.
`JSON.SET key path[N] value` where `N` equals the array length appends (RedisJSON answers an out-of-range error); `JSON.ARRAPPEND` is logged as one small path record per item. Its cost grows with the array length (one byte scan, about 10 ns per element); for append-heavy accumulation use a list (`RPUSH`), which is constant time. `JSON.INDEX CREATE name PREFIX p PATH $.f [TYPE string|number|bool]`, `JSON.INDEX DROP name`, `JSON.INDEX LIST` and `JSON.FIND name VALUE v [COUNT n] [AFTER key]` are a Ruvio extension: an exact-equality index on one scalar field of the JSON keys with a prefix. The index is built in the background (`ERR index is building` until done), follows every write, delete, expiry, rename and eviction, is database 0 only, and keeps its postings in memory (rebuilt at startup). Definitions are reserved keys, visible to `KEYS`/`DBSIZE`, and `FLUSHDB`/`FLUSHALL` on database 0 drop them; `JSON.INDEX LIST` prints one line per shard. `JSON.MGET key... path` reads one path from several keys of one slot (a missing or non-JSON key gives null). Object members keep the order they were given (as in RedisJSON); a new
member goes last, a merged or replaced member keeps its position.

Paths are deliberately limited to `$`, `.field`, and zero-based `[N]` segments
(e.g. `$.users[0].name`); there are no wildcards, filters, recursive descent,
or automatic creation of intermediate objects/array slots. A final object
field may be created, and an existing array element may be replaced or
removed. Small documents remain compact JSON bytes: root reads borrow them,
simple path reads scan without building a document tree, and existing-path
updates splice changed bytes into a new compact document. Eligible documents
of at least 32 KiB retain one compact body with an internal path index and
small scalar patches: unchanged root reads borrow the body; patched root reads
stream its spans. Structural edits whose nearest indexed object or array is at most 8 KiB rewrite only that
span (its indexed descendants are dropped until the next rebuild); other structural edits
rebuild the document, and ineligible large shapes may use a bounded tree fallback. Validated path updates can record atomic
path-result mutations rather than a full-document WAL record; root updates
and key creation record the full resulting document, while path writes, merges and
deletes on an existing document (compact or indexed) record only the path and value. The
representation may switch as the document changes size or shape. Non-JSON keys
have no JSON-specific per-key metadata or indexing. The resulting document is
limited to 16 MiB.
JSON is recorded in the WAL as typed mutations and in snapshots as a compact
typed value; expiry, eviction, and replication follow the existing key behavior.
This extension is not a full RedisJSON implementation. [The JSON roadmap](ROADMAP.md#opt-in-document-and-search-features)
tracks measured performance gaps, acceptance tests, opt-in field indexes, and
full-text search; those are planned work, not part of this compatibility contract.

## Protocol

- Required baseline: RESP2 arrays of bulk strings for client commands.
- Keys and values are binary-safe.
- Pipelining is supported.
- Inline commands (a plain text line such as `PING` or `SET a "b c"`, quoted like Redis, up to 64 KiB) are accepted next to RESP arrays, so `telnet` and `nc` work.
- RESP3 is negotiated per connection with `HELLO 3` and switched back with
  `HELLO 2`. The default stays RESP2, and RESP2 replies are unchanged.
  RESP3 adds null (`_`), booleans, doubles (`ZSCORE`, `ZINCRBY`), maps
  (`HELLO`, `HGETALL`), and sets (`SMEMBERS`, `SINTER`, `SUNION`, `SDIFF`).
  `HELLO` accepts `AUTH username password` and `SETNAME` after the version.
  A bad version returns `NOPROTO` and changes nothing. A bad password leaves
  the previous protocol and name in place. `CLIENT GETNAME` and
  `CLIENT SETNAME` are connection-local. `CLIENT SETINFO LIB-NAME|LIB-VER value` answers `OK` and keeps nothing; it exists so client libraries that announce themselves at connect are not refused, and no first-party client sends it. `CLIENT TRACKING ON` requires RESP3
  and records `GET` and `MGET` on this shard, including misses. A completed
  write of a tracked key pushes one `invalidate` and forgets it. `OFF` clears
  that connection's keys. `REDIRECT`, `BCAST`, `PREFIX`, `OPTIN`, `OPTOUT`,
  and `NOLOOP` are rejected. Tracking does not cross shards. A blocked
  `BLPOP` that pops later is not an invalidation. A full push queue
  disconnects that connection. Attributes (`|`) are not produced.
- Sharded Pub/Sub is supported: `SPUBLISH`, `SSUBSCRIBE`, and `SUNSUBSCRIBE`.
  Channels in one command must hash to the same slot. Messages are delivered
  only to subscribers on that slot owner, are not stored, and are not written
  to the WAL. Classic `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, and `PUNSUBSCRIBE` stay on
  the shard that accepted the connection; a classic `PUBLISH` reaches
  subscribers on every shard of the process (see the Pub/Sub row below).
  Messages are not stored and not written to the WAL. Patterns use
  `*` and `?`. After any subscription, the connection accepts `SUBSCRIBE`,
  `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`,
  and `PING` until its subscription count returns to zero. Replies stay RESP2
  arrays, including on a `HELLO 3` connection. A subscriber that does not keep
  up is disconnected instead of stalling the shard. There is no sync wait for
  each subscriber.
- Keyspace notifications are off unless `notify-keyspace-events` is set
  (`RUVIO_NOTIFY_KEYSPACE_EVENTS` or `notify_keyspace_events` in TOML).
  The mask needs `K` or `E`, plus a class this build emits: `g` delete
  (also when a removal takes a hash, list, set or sorted set's last element),
  `$` string `SET`, `x` expired, `e` evicted, or `A` for those four.
  A client receives them by `SSUBSCRIBE` of the exact channel
  `__keyspace@<db>__:<key>` (payload is the event name) or
  `__keyevent@<db>__:<event>` (payload is the key), where `<db>` is the
  logical database of the key. Delivery is an `smessage`
  on the existing sharded queue. The value is not included. Events are not
  written to the WAL, so a crash or a full subscriber queue can drop them.
  With the mask empty, or with nobody subscribed to one of those channels,
  `SET`, `DEL`, and expire return without building a channel name. `GET` of
  a live key does not emit. The separate hot-key partition does not emit.
  List, hash, set, and sorted-set mutations do not emit. `PSUBSCRIBE` does not
  receive those notices. A per-subscription sync wait is not supported.
- Optional TLS. When `--tls-cert` / `--tls-key` are
  set, clients must speak TLS; plaintext RESP is rejected at handshake.
  Certificate files reload on the next handshake after their mtimes change.
  `tls_client_ca` checks a presented client certificate, and `tls_client_auth` requires one. Redis
  `stunnel`/`tls-auth-clients` modes are not supported.

Protocol input is currently limited to:

- at most 64 arguments per command,
- at most 1 MiB per key,
- at most 16 MiB per value,
- at most 64 MiB per encoded command,
- at most 1,024 responses accumulated in one Linux pipeline batch,
- at most 64 MiB in a Linux connection input buffer.

The defaults can be overridden with `RUVIO_MAX_ARGUMENTS`,
`RUVIO_MAX_KEY_BYTES`, `RUVIO_MAX_VALUE_BYTES`,
`RUVIO_MAX_COMMAND_BYTES`, `RUVIO_MAX_PIPELINE_COMMANDS`, and
`RUVIO_MAX_CONNECTION_BUFFER_BYTES`. Values are byte or command counts and
must be positive. The connection-buffer limit must be at least the command
limit; invalid combinations fail before the listener is opened.

Key, value, argument-count, and encoded-command limits are applied by the
hot parser and the general parser. The pipeline setting bounds responses
accumulated before an asynchronous write.

Connections default to a 300-second idle timeout, a 30-second inactive
partial-frame read timeout, and a 30-second blocked-write timeout. Override
them in milliseconds with `RUVIO_IDLE_TIMEOUT_MS`, `RUVIO_READ_TIMEOUT_MS`,
and `RUVIO_WRITE_TIMEOUT_MS`. Zero and invalid values fail startup. The idle
deadline applies while waiting for the next frame; after any partial frame is
buffered, the read deadline applies until more input arrives.

The process accepts at most 10,000 concurrent connections by default, with a
maximum of 256 from one source IP. Override these limits with
`RUVIO_MAX_CONNECTIONS` and `RUVIO_MAX_CONNECTIONS_PER_IP`. A per-IP or
per-user limit above the global limit is lowered to the global limit at
startup and a warning is logged. Excess connections receive
`ERR max number of clients reached` and are closed without creating a client
task or thread. Multi-shard Linux deployments share one process-wide admission
tracker.

`RUVIO_MAX_CONNECTIONS_PER_USER` / `--max-connections-per-user` /
`max_per_user` caps concurrent connections after `AUTH` for one ACL user
(`0` disables). Excess `AUTH` replies
`ERR max number of connections for this user reached` and the connection stays
unauthenticated. `RUVIO_MAX_COMMANDS_PER_SECOND` /
`--max-commands-per-second` / `max_commands_per_second` is a per-connection
token bucket (`0` disables). Excess commands reply
`ERR max commands per second exceeded` and are not executed. Burst equals one
second of tokens. The bucket lives on the client task; the shard owner does
not take a lock.

Per-connection response accumulation is bounded by
`RUVIO_MAX_OUTPUT_BUFFER_BYTES` (64 MiB by default). A pipeline sends its replies
once 64 KiB pile up, so the limit counts only bytes the client has not read yet.

Configuration can also be loaded from TOML with `--config`; environment and
CLI values override file values. See `docs/CONFIGURATION.md`. `SIGINT` and
`SIGTERM` stop listeners and allow active clients to drain for
`RUVIO_SHUTDOWN_GRACE_MS` (30 seconds by default). Idle connections, including
subscribers, are closed at once; a connection with a command in flight gets the
grace period. The process exits 0. `SIGHUP` is ignored.

## Clients

Application code should use the first-party SDKs: `ruvio-client` (Rust)
and `Ruvio.Client` / `Ruvio.Client.AspNetCore` (C#). They speak RESP2,
do not send `CLIENT SETINFO` or `HELLO` on connect, and route a sharded
server from one seed.

A dropped command or shard socket is opened again before the next
command. The command that discovers the drop fails, so the client does
not write it twice. The C# client sends one `GET`, `MGET`, `EXISTS`,
`HGET`, `TTL`, or `PTTL` again when the socket closes before any reply
byte. A pipeline is not retried. The Rust client does not retry that
command. Pub/Sub channels are subscribed again on the new
socket. Automatic retry is on by default (exponential wait, 8 attempts).
Turn it off and call `ReconnectAsync` / `reconnect` to own the timing, or
supply a delay callback; returning null / `None` stops the retry.
`ConnectionLost` / `ConnectionRestored` (C#) and `on_connection_lost` /
`on_connection_restored` (Rust) report the host, port, and whether the
socket is the Pub/Sub connection. `Ruvio.Client.AspNetCore` binds
`Reconnect`, `MaxReconnectAttempts`, `ReconnectBaseDelay`, and
`ReconnectMaxDelay` from configuration and can attach the events when the
singleton is created.

An unknown command name, a wrong argument count, or an unsupported
`CLIENT` subcommand such as `SETINFO` is an error reply. The connection
stays open. A malformed RESP frame is still `ERR protocol error` and
**closes the connection**.

CI runs the first-party `ruvio-client` and `Ruvio.Client` tests. See
`clients/README.md`. Application code should use those packages.

## Current Commands

| Family | Supported | Current limitations |
| --- | --- | --- |
| Connection | `PING`, `ECHO`, `COMMAND`, `RUVIO`, `AUTH`, `HELLO`, `SELECT`, `CLIENT ID`, `CLIENT LIST`, `CLIENT INFO`, `CLIENT KILL`, `SLOWLOG`, `LATENCY`, `CLIENT GETNAME`, `CLIENT SETNAME`, `CLIENT TRACKING`, `CLIENT DURABILITY` | `AUTH` accepts `password` or `username password` against configured ACL users. `HELLO` accepts no arguments, protocol `2` or `3`, then `AUTH` and `SETNAME`. `SELECT` accepts `0` to `databases - 1` (16 by default); cluster mode allows only `0`. See `docs/LOGICAL_DATABASES.md`. `CLIENT TRACKING` is `ON` or `OFF` on this shard and pushes one RESP3 `invalidate`. `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]` (a Ruvio extension) holds this connection's replies until `n` replicas have every record its shard wrote before them on disk, checked once per batch; on timeout (5000 ms by default) every reply of the batch is `-NOREPLICAS` and the writes stay, as with `WAIT`. `ruvio coordinator` fails over only to a replica that holds every such write, unless told otherwise (`docs/COORDINATOR.md`). `0` turns it off; refused inside `MULTI` and without replication. Attributes are not produced. `CLIENT SETINFO` and any other `CLIENT` subcommand are an error reply and the connection stays open. `RUVIO` with no arguments replies `PONG is taken`. `RUVIO OWNER` names the shard rule. `RUVIO LEASE BEGIN` and `RUVIO LEASE GRANT token coordinator voters epoch lease_ms` renew the primary lease for `ruvio coordinator` (`primary-lease`, see `docs/COORDINATOR.md`; `@admin @dangerous`). |
| Pub/Sub | `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` | Sharded channels are same-slot. A classic `PUBLISH` reaches subscribers on every shard of the process; its reply counts the receivers on the shard that took the command, as a Redis Cluster node answers. With several shards `PUBLISH` is refused inside `MULTI`, and a `PUBLISH` issued from a script reaches its own shard only. Deliveries to another shard are best effort: a shard that already holds 16,384 undelivered publishes, or 64 MiB of them, drops new ones and counts them in `INFO` `pubsub_cluster_dropped`. Not durable. Patterns use `*` and `?`. Keyspace notices use exact `__keyspace@<db>__` / `__keyevent@<db>__` channels via `SSUBSCRIBE` and are off by default. |
| ACL | `ACL WHOAMI`, `ACL USERS`, `ACL LIST`, `ACL GETUSER`, `ACL CAT`, `ACL SETUSER`, `ACL LOAD`, `ACL SAVE` | Same rule tokens as the config file, applied incrementally. `ACL SAVE` writes `security.acl_file` and `ACL LOAD` replaces the live directory from it. An empty path refuses both. An open connection keeps the rules from its last `AUTH`. `ACL LIST` redacts passwords. The file stores a BLAKE3 hash per password (`#<hex>`), never the password; `>password`, `#hash`, `<password` and `!hash` are accepted. An alias shares one rule with its command: `DECR`, `INCRBY` and `DECRBY` with `INCR`, `UNLINK` with `DEL`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT` with `EXPIRE`, `SETEX` and `PSETEX` with `SET`, `SUBSTR` with `GETRANGE`, `RPOPLPUSH` with `LMOVE`, `GEORADIUSBYMEMBER` with `GEORADIUS`, `JSON.FORGET` with `JSON.DEL`, `JMERGE` with `JSON.MERGE`; so `-setex` also denies `SET`. A `+parent|sub` or `-parent|sub` rule names one of `acl|cat`, `acl|getuser`, `acl|list`, `acl|load`, `acl|save`, `acl|setuser`, `acl|users`, `acl|whoami`, `client|kill`, `client|list`, `config|get`, `config|set`, `config|rewrite`, `ruvio|bigkeys`, `ruvio|hotkeys`; any other subcommand is refused (Redis's first-argument rules such as `+select|0` are not supported). A user whose keys are limited by patterns (`~app:*`) gets `NOPERM` for the commands that list or change the whole keyspace: `KEYS`, `SCAN`, `RANDOMKEY`, `FLUSHDB`, `FLUSHALL`, `SWAPDB` (plus `CHANGES`, `RUVIO HOTKEYS`, `RUVIO BIGKEYS` and keyspace notification channels); give the user `allkeys` (or `~*`) to allow them. A script (`EVAL`, `FCALL`) obeys the ACL of the user running it: a command or key the user may not use fails with `NOPERM`. No `ACL DRYRUN`. |
| Strings | `GET`, `GETEX`, `SET`, `SETV`, `MGET`, `MSET`, `GETSET`, `SETEX`, `PSETEX`, `SETNX`, `GETDEL`, `MSETNX`, `INCRBYFLOAT`, `GETRANGE`, `SUBSTR`, `SETRANGE`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `TAKE`, `TRANSFER`, `DELEX`, `DELEX` | `INCRBYFLOAT` computes in double precision and prints 16 significant digits (Redis uses `long double`). The result is stored as this rounded text, so digits beyond the 16th are lost and every later increment starts from the rounded value: increments below the 16th digit of a large counter can vanish. For money and balances use integers (`INCRBY`, `TAKE`, `TRANSFER`) in the smallest unit. `HINCRBYFLOAT` follows the same rule. `GETRANGE` on a range wholly before the start returns an empty string. `SETRANGE` strings are capped at 16 MiB. `INCRBY key n MAX limit` returns a null bulk and writes nothing when the result would pass `limit`. `INCR`, `DECR`, `INCRBY`, `DECRBY` and `INCRBYFLOAT` take `EX seconds` or `PX milliseconds`: the deadline is set only when the call creates the key, and an existing key keeps its deadline (see *TTL on create* in `docs/DATA_TYPES.md`). `TAKE` subtracts from an integer only when the key holds enough, and otherwise leaves it unchanged. `TRANSFER source destination amount [MAX cap]` moves a positive amount between two integer strings of one slot in one WAL record and replies `[source, destination]` after, or a null bulk (nothing written) when the source is missing or short or the destination would overflow or pass `cap`. `GETEX` returns a string and moves its deadline; `GET` does not. `SETV` stores a version only on keys written with it, and that write is a WAL record. A mismatch does not change the value. `INCR` does not accept a versioned string. `SET` supports `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`, `NX`, `XX`, and `GET`. `SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]` writes only when the string equals `expected` and otherwise returns a null bulk (it does not combine with `NX`, `XX` or `GET`; another type is WRONGTYPE). `SET key value IFGT|IFLT [EX|PX|EXAT|PXAT|KEEPTTL]` writes when the key is missing or `value` is greater (`IFGT`) or smaller (`IFLT`) than the stored signed 64-bit integer and otherwise returns a null bulk; a non-integer argument or stored string is an error and nothing is written (it does not combine with `NX`, `XX`, `GET` or `IFEQ`). `DELEX key IFEQ expected` deletes the string only when it matches and returns 1 or 0. `GET` returns the previous string, or nil, and does not overwrite a non-string. `MGET` returns nil for a wrong-type key. |
| Bitmaps | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`, `BITFIELD_RO` | Bitmaps are strings, at most 16 MiB (bit offset below 134,217,728). `SETBIT` rewrites the value (O(value size)), keeps an existing TTL and takes `EX`/`PX` for a bitmap it creates. `BITCOUNT` and `BITPOS` take `BYTE` or `BIT` ranges. `BITOP` supports `AND`, `OR`, `XOR`, `NOT`; an empty result deletes the destination. `BITFIELD` supports `GET`, `SET`, `INCRBY` and `OVERFLOW WRAP\|SAT\|FAIL` on `i1`..`i64` and `u1`..`u63` fields (bit offset or `#n`, lowercase type letter like Redis); a write extends the string to cover its field, rewrites the value (O(value size)) and keeps an existing TTL. `BITFIELD_RO` is `GET` only. |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE` | A HyperLogLog is a string with a `HYLL` header and 16,384 six-bit registers (12,304 bytes), hashed and estimated as Redis 7 does. Ruvio always writes the dense form and rejects Redis sparse values as not a valid HyperLogLog. Every write rewrites the whole value. The cached cardinality field is not used. |
| Cache fill | `FILLGET`, `FILLSET` | Ruvio extension. `FILLGET key fill_ms [WAIT wait_ms]` replies `[HIT, value]`, `[FILL, token]` to the one client that should fill, `[BUSY, nil]` while another fills, or `[STALE, value, token\|nil]` inside an `SWR` window. `FILLSET key value ttl_ms token [SWR stale_ms]` stores the value only with the live token (1, else 0). Markers live in memory only and lapse after `fill_ms`. With `WAIT wait_ms` a caller that would get `BUSY` is held (retry every 5 ms, no parked waiter) until the filler writes, the marker lapses (the waiter then becomes the filler) or the wait ends with `[BUSY, nil]`; `WAIT` is refused inside `MULTI` and scripts. |
| Keys | `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`, `COPY`, `DUMP`, `RESTORE`, `KEYS`, `RANDOMKEY`, `TOUCH`, `OBJECT`, `MEMORY`, `EXPIRETIME`, `PEXPIRETIME`, `SCAN`, `DBSIZE`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `MOVE`, `LEASE`, `RELEASE`, `RENEW`, `SEMAPHORE`, `LIMIT`, `ONCE`, `CHANGES` | Same-slot keys in multi-shard mode. `COPY`, `DUMP` and `RESTORE` also carry JSON documents (member order and TTL kept). `UNLINK` is synchronous (`DEL`). `SCAN`/`DBSIZE` are shard-local and follow the selected database. `SCAN` cursors walk the index slots in order: one call costs O(`COUNT`) whatever the keyspace size, a key that exists for the whole walk is returned at least once, and a key may repeat when the table was rebuilt between calls. `SCAN cursor … SLOT n` (a Ruvio extension) returns only the keys of hash slot `n` and runs on the shard that owns it, whichever port receives it; `COUNT` still bounds each call, so a page may be short or empty before the cursor comes back as 0. `CLUSTER COUNTKEYSINSLOT` and `GETKEYSINSLOT` are not provided: a slot has no key index, so they would walk the whole shard in one call. The `EXPIRE` family accepts `NX`, `XX`, `GT` or `LT`; a key without a deadline counts as infinite. `MOVE` is refused in cluster mode. `LEASE` returns a token for one key, or a null bulk while that token is still held. `RELEASE` drops the hold only when the token matches. `SEMAPHORE key max ttl_ms` returns a token while fewer than `max` holders are live, or a null bulk when full. Each holder has its own TTL. `RELEASE key token` frees one slot. `RENEW key token ttl_ms` moves the deadline of a live lease or slot to now + `ttl_ms` and returns 1, or 0 when the token no longer holds it; it never creates a holder or changes the token sequence. When the last holder expires or is released the key goes away. `LEASE` and `SEMAPHORE` tokens are fenced: a token is larger than every token an earlier holder of that key got, also after the key expired or was deleted, after a restart, a failover or a slot move to another group, so a stale token never matches a later holder and a fenced resource can refuse the older one. The top bits carry the generation (the replication epoch, or the cluster map version under a coordinator), the rest a counter with a clock floor; treat the token as an opaque 64-bit integer. `SETV` writes a versioned string: the first call creates version 1, and a later call must pass the current version. `LIMIT key max window_ms` returns the remaining allowance, or a null bulk when the window is full. `LIMIT key max window_ms SLIDING` weighs the previous window by how much of it still overlaps, so a burst at a window edge is not doubled. `LIMIT key capacity refill_ms TOKENS [COST n]` is a token bucket that refills over `refill_ms`. Both keep a fixed amount of state per key and write nothing when they refuse. `WITHRETRY` on either returns `[remaining, retry_after_ms]`, with remaining -1 when refused. A key keeps the mode it was created with. `ONCE key ttl_ms` returns 1 the first time and 0 until the deadline. `LIMIT`, `ONCE` and `IDEM` keep absolute wall-clock deadlines in the log; across a failover use `CLIENT DURABILITY` on the writing connection and keep the nodes' clocks synchronized (see `docs/IDEM.md`). `CHANGES START` arms an in-memory feed on this shard. Nothing is recorded before that, and the feed is empty after restart. `CHANGES SUBSCRIBE [FROM <generation>:<shard>:<seq>|NOW] [MATCH glob] [TYPES put,json,del,expire,rename,list,set,hash,zset,stream,flush,other] [VALUES] [COMMITTED]` is the durable feed (see `docs/CONFIGURATION.md`, *Change stream*). The generation is the shard's replication epoch; the older `<shard>:<seq>` form is still accepted as the current generation. After a failover, a cursor past the point where the new primary forked gets one `["rewound", cursor]` push naming the fork, then the stream continues from there; a cursor of a newer or unknown generation is refused. `COMMITTED` streams only records a `CLIENT DURABILITY` reply committed, so no event is taken back by a failover; it is refused on a shard that does not replicate. After `RUVIO MOVESLOTS`, the old owner's feed gets one `moved` event whatever the filters: its key is the slot range (`first-last`) and its value the cursor where the new owner's feed continues it (`<generation>:<shard>:<seq>`; `host:port:seq` for a range moved to another node, to resume there with `<shard>:<seq>`); the copy itself is not reported. |
| Databases | `FLUSHDB`, `FLUSHALL`, `SWAPDB` | `ASYNC` runs synchronously. Each is one WAL record; `SWAPDB` is O(1). Allowed inside `MULTI`, refused in scripts. `SWAPDB` is refused in cluster mode. |
| Idempotency | `IDEM` | Native `key BEGIN fingerprint owner ttl_ms` and `key COMPLETE fingerprint owner result`. Pending/completed state, fingerprint conflicts and binary result replay; completion checks the owner and preserves retention. Tokens: 1-128 bytes; retention: 1 ms-7 days; result: at most 4 MiB, also bounded by protocol value limits including metadata. One atomic WAL record includes the value and deadline. Packed ordinary string storage adds no fields to unused keys. Not exactly-once for external side effects. See [the full contract](IDEM.md). |
| Lists | `LPUSH`, `RPUSH`, `LPUSHCAP`, `RPUSHCAP`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM`, `BLPOP`, `BRPOP`, `LINSERT`, `LSET`, `LREM`, `LPOS`, `LPUSHX`, `RPUSHX`, `LMOVE`, `RPOPLPUSH`, `BLMOVE`, `LMPOP`, `BLMPOP` | `LPOP`/`RPOP` accept a positive `COUNT` and return an array. An empty list returns nil. At most 1,024 blocked clients per shard. `LINSERT`, `LSET` and `LREM` rewrite the list (O(N)) as one atomic WAL transaction. `BLMOVE` and `BLMPOP` retry every 5 ms instead of parking a waiter, and are refused inside `MULTI` and scripts. `RPOPLPUSH` uses the `lmove` ACL name. |
| Sets | `SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SINTERCARD`, `SCARD`, `SMEMBERS`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SSCAN` | Enumeration and algebra replies are sorted. `SPOP` without a count returns one member; with a count it returns an array. `SRANDMEMBER` samples without removing members: no count returns one bulk or nil, a count returns an array. A negative count (draws with repeats) is limited to 1,048,576 for `SRANDMEMBER`, `HRANDFIELD` and `ZRANDMEMBER`; a larger one returns `count is out of range`. Same-slot keys in multi-shard mode. |
| Hashes | `HSET`, `HMSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`, `HMGET`, `HEXISTS`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HTRANSFER`, `HRANDFIELD`, `HSETNX`, `HSTRLEN`, `HSCAN`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST` | Compact pairs until 64 fields or a 64-byte field/value; then a table. Enumeration order is unspecified, as in Redis. `HINCRBY` stores the decimal result and takes `EX`/`PX` for a hash it creates. `HMSET` is multi-field `HSET` that replies `OK`. `HSETNX` writes only a missing field. `HSTRLEN` returns the field length. `HSCAN` pages through the hash in storage order. Field TTLs follow Redis 7.4: `NX`, `XX`, `GT`, `LT`, and `FIELDS numfields`; replies per field are -2, 0, 1, or 2. `HSET` and `HDEL` drop a field TTL, `HINCRBY` keeps it. A hash command removes due fields of its key first, and the 100 ms maintenance tick removes the rest (at least `active_expire_keys` hashes a tick, more within a 5 ms budget). A hash whose every field is past its deadline is a missing key to every command (`TYPE`, `EXISTS`, `SCAN`, `KEYS`, reads and writes of any type) and is removed like an expired key, its delete logged. A hash without a field TTL stores nothing extra. |
| Sorted sets | `ZADD`, `ZCOUNT`, `ZLEXCOUNT`, `ZMSCORE`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZRANDMEMBER`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZREM`, `ZINCRBY`, `ZCARD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZPOPMIN`, `ZPOPMAX`, `ZPOPBYSCORE`, `ZMOVEDUE`, `BZPOPMIN`, `BZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZSCAN` | `ZPOPMIN`/`ZPOPMAX` take an optional count and reply with a flat member, score array. `BZPOPMIN`/`BZPOPMAX` park like `BLPOP`; `ZADD`, `ZINCRBY`, and `GEOADD` serve the oldest waiting client. `ZPOPBYSCORE key max [MIN min] [COUNT n] [WITHSCORES]` pops the members that are due (score within the bounds), lowest first, at most 10000 per call; `ZMOVEDUE source destination max [COUNT n]` moves them to the tail of a list in the same slot and returns how many moved. Pops and range removals are recorded as one `ZREM` of the removed members; `ZMOVEDUE` is one transaction. `ZADD` accepts `EX`/`PX` (a deadline for a key it creates), `NX`, `XX`, `GT`, `LT`, `CH`, and `INCR`. Without `CH` the reply counts new members. `INCR` returns the new score, or nil when the condition rejects it. `WITHSCORES` on range commands. `ZRANGE` accepts `BYSCORE`, `BYLEX`, `REV`, and `LIMIT`. `ZRANGEBYSCORE` supports `-inf`/`+inf`, exclusive `(` bounds, and `LIMIT`. Lex bounds are `-`, `+`, `[member`, and `(member`. Member order ignores the score. A negative `LIMIT` count returns the rest of the range. The `LIMIT` offset must be at least 0. |
| JSON | `JSON.SET`, `JSON.GET`, `JSON.MGET`, `JSON.DEL`, `JSON.MERGE`, `JMERGE`, `JSON.TYPE`, `JSON.STRLEN`, `JSON.ARRLEN`, `JSON.OBJKEYS`, `JSON.NUMINCRBY`, `JSON.TOGGLE`, `JSON.ARRAPPEND`, `JSON.CLEAR`, `JSON.OBJLEN`, `JSON.STRAPPEND`, `JSON.NUMMULTBY`, `JSON.ARRINSERT`, `JSON.ARRPOP`, `JSON.ARRTRIM`, `JSON.ARRINDEX`, `JSON.FORGET`, `JSON.INDEX`, `JSON.FIND` | Native extension described in the JSON section above: simple `.field` and `[N]` paths only. |
| Geospatial | `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUSBYMEMBER` | Stored as one sorted set. The score is the 52-bit geohash, so `TYPE` returns `zset`. `GEOADD` accepts `NX`, `XX`, and `CH`. `GEOSEARCH` accepts `FROMMEMBER`, `FROMLONLAT`, `BYRADIUS`, `BYBOX`, `ASC`, `DESC`, `COUNT`, `ANY`, `WITHDIST`, `WITHHASH`, and `WITHCOORD`. `GEORADIUS` and `GEORADIUSBYMEMBER` accept `STORE` and `STOREDIST`. |
| Bloom filters | `BF.RESERVE`, `BF.ADD`, `BF.EXISTS`, `BF.MADD`, `BF.MEXISTS`, `BF.INSERT`, `BF.INFO`, `BF.CARD` | RedisBloom wire shape; not certified module compatibility. A filter scales by default: when the last layer holds its capacity, a layer `expansion` times larger (default 2) with half the false-positive rate is added, and a lookup asks every layer. `BF.RESERVE key error capacity [EXPANSION n \| NONSCALING]` and `BF.INSERT … NONSCALING` keep one layer and answer an error once it is full. `BF.ADD` on a missing key creates error 0.01, capacity 100. A layer above 64 MiB is refused. `BF.MADD`/`BF.INSERT` write one WAL record for all items. Filters written before layers existed load as one scaling layer whose capacity and item count are estimated from the bits. A WAL with these records needs format minor 2 (reserve with a non-default expansion, batch add); an older server refuses it. |
| Streams | `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`, `XGROUP CREATE`, `XGROUP CREATECONSUMER`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS`, `XAUTOCLAIM`, `XSETID` | `XDEL` accepts many ids. `XRANGE` and `XREVRANGE` accept `COUNT`. `XREAD` is same-slot. `BLOCK` waits on the shard for a later entry and does not remove it; `BLOCK 0` waits until data or shutdown. `$` is the current end. `MAXLEN` and `MINID`, including `~` and `=`, both trim exactly. `XADD` and `XTRIM` accept `LIMIT count` after an approximate threshold (`MAXLEN ~ n`, `MINID ~ id`), as Redis does, and ignore it: the stream is always trimmed to the threshold, and `LIMIT` without `~` is a syntax error. `XGROUP CREATE` stores the group cursor. `XGROUP CREATECONSUMER` registers a consumer with no pending entry. `XGROUP DESTROY` removes it. `XGROUP SETID` moves the cursor. `XGROUP DELCONSUMER` drops that consumer's pending entries. `XREADGROUP` reads one stream with `>`. `BLOCK` delivers the new id to the oldest waiting consumer. `NOACK` advances the cursor without a pending entry. `XACK` drops ids from the pending list. `XPENDING` returns the summary, or an inclusive id range with optional `IDLE` and consumer. `XCLAIM` moves an idle pending id and accepts `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`, and `LASTID`. `XAUTOCLAIM` claims up to 100 entries when `COUNT` is not given and looks at no more than ten pending entries per one asked for, returning where it stopped. A consumer stays in its group after its entries are acknowledged or claimed by another consumer; `XINFO CONSUMERS` reports its idle time from the last entry it was handed, which a snapshot does not keep (it restarts at the latest pending delivery, or at zero). `XSETID` sets the last generated id without adding an entry. The id cannot move backward. `ENTRIESADDED` and `MAXDELETEDID` are not accepted. `DELAY` hides the new entry from `XREADGROUP` until that many milliseconds pass. The hold is a WAL record and a snapshot trailer, written only when a delay is set. |
| Introspection | `INFO`, `CONFIG GET`, `CONFIG SET`, `CONFIG REWRITE`, `SAVE`, `BGSAVE`, `WAIT`, `REPLICAOF`, `ROLE` | `INFO prometheus` returns Prometheus text. `CONFIG GET` lists Ruvio settings with `requirepass` redacted and shows live values. `CONFIG SET name value [name value ...]` changes the live settings (listed under *Changing settings at runtime* in `docs/CONFIGURATION.md`): memory, slowlog and key statistics, connection limits and timeouts, script limits and `requirepass`. A restart-only name (`shards`, `address`, `data-dir`, TLS files, `replication-*` and the rest) is refused by name, and one bad pair fails the whole call. Values follow the config file's rules, so byte counts are plain numbers (no `100mb`). Every shard applies the change before `OK`; connection settings apply to new connections only. It is not logged or replicated and is refused inside `MULTI` and scripts. `CONFIG REWRITE` writes the live settings into the config file the server started from. `CONFIG RESETSTAT` is not supported. `BGSAVE` is in-process, not a fork. `WAIT replicas milliseconds` blocks that client until that many replicas have synced this shard's `wal_last_sequence` (in `port_mode = "single"` every shard's; in per-shard mode also the shards this connection's writes were forwarded to), or the timeout. Timeout `0` waits until shutdown. The reply is how many replicas reached it. `REPLICAOF host port [YIELD]` and `REPLICAOF NO ONE [FORCE]` change every shard's replication role without a restart (see *Replication* below); `ROLE` answers per shard. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` | Same-slot, shard-local only. One WAL `Transaction` record. Empty `EXEC` is `*0`; a dirty `WATCH` is `*-1`. At most 1,024 queued commands and 128 watched keys. `EVAL`, `EVALSHA` and `FCALL` queue and run in their place at `EXEC`, inside the same WAL record; a queued `PUBLISH` is sent once `EXEC` is recorded (`DISCARD` sends nothing). Blocking commands, `SELECT`, `SPUBLISH` and the subscribe commands, `SCRIPT`/`FUNCTION` management, `SAVE`/`BGSAVE`, `AUTH`, `CONFIG SET` and cluster discovery are not queueable. |
| Scripts | `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`, `SCRIPT KILL`, `FCALL`, `FUNCTION LOAD`, `FUNCTION LIST`, `FUNCTION DELETE` | Lua 5.1 sandbox. Declared keys must be same-slot; a script with no keys runs on the connection's shard. `ruvio.call` may only touch those keys. `redis.call`, `redis.pcall`, `redis.error_reply` and `redis.status_reply` are the same functions under the name scripts written for Redis use; no other part of Redis's Lua API (`redis.log`, `cjson`, `redis.sha1hex`, `redis.set_repl`) exists. Scripts cannot call `CLIENT`, `FUNCTION` or `FCALL` (Redis refuses them too), nor `JSON.INDEX` or `CHANGES SUBSCRIBE`, which act beyond the script's shard. A shard keeps one Lua state and the compiled form of each cached script until `SCRIPT FLUSH`. Globals are read-only, as in Redis 7: assigning a global or changing `ruvio`, `string`, `table` or `math` fails with `Attempt to modify a readonly table`, so one script cannot change what a later one runs; use `local`. `getfenv` and `setfenv` are removed. Each run replaces `KEYS` and `ARGV`. A result or reply nested deeper than 200 tables fails with `reached lua stack limit`. A `ruvio.call` whose reply would take the shard's Lua state past its 128 MiB limit fails the whole script, even under `pcall`, and the script's writes are rolled back. Redis stops a long script with `SCRIPT KILL`; here `SCRIPT KILL` returns `NOTBUSY` and two limits stop it instead. The instruction quota (`scripts.max_instructions`) covers coroutines, and each coroutine created counts as 1,000 instructions, the most its start can run unseen, so with the default quota a script can create at most about 1,000 coroutines. Library calls whose work grows with the data are charged before they run: `string.rep`, `sub`, `upper`, `lower`, `reverse`, `format`, `tonumber`, plain `find` and `gsub` replacements one instruction per 64 bytes, `table.sort` one per comparison plus the bytes of the strings it compares, `table.concat` one per 4 elements plus the bytes it joins, separators included, `maxn` one per 4 entries, `table.insert` and `remove` at a position one per 16 slots moved. A wall-clock deadline (`scripts.max_time_ms`, 1,000 ms by default) bounds the rest, such as `s .. s`; it is checked every 1,000 instructions (more often once the Lua heap holds large values), by every charged call and by `ruvio.call`, so neither a single library call nor a single `..` is interrupted, and a script past it fails with `Lua script time limit exceeded`. Once either limit is reached, `pcall`, `xpcall` and `coroutine.resume` pass the error on instead of catching it, and an `xpcall` handler is not called. `string.find`, `match`, `gmatch` and `gsub` give Lua 5.1's results, but their matching work counts against the quota: a script may take 8 pattern-matching steps for each instruction of `max_instructions` (8 million by default, about 0.1 s), and one whose patterns backtrack past that fails with `Lua instruction quota exceeded`. A pattern nested deeper than 200 quantifiers or captures fails with `pattern too complex`, as in Lua 5.2. `unpack` over more values than the Lua stack holds fails with `too many results to unpack` (Lua 5.1 could overflow and crash); `table.insert` at a negative position fails with `position out of bounds`. `string.dump`, `load` and `loadstring` are removed. Script cache and function library are per shard and memory-only. Writes are one WAL batch; Lua is not replayed. `ruvio.pcall` returns every failure as an error table `{err = message}`, as `redis.pcall` does: ACL refusals, undeclared keys, commands not allowed in scripts and command errors alike; `ruvio.call` raises them. Earlier builds returned `false, message` for every failure, so a script that tested `if not ok` must check `.err` instead. |
| Cluster discovery | `CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER NODES`, `CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER KEYSLOT`, `READONLY`, `READWRITE` | Topology of the shards in one process, including ranges moved with `RUVIO MOVESLOTS`, and with `cluster_topology` the other groups of a multi-node cluster, each listed by its primary; their keys are answered with `MOVED` (see `docs/CONFIGURATION.md`). `RUVIO TOPOLOGY VERSION` returns the map version a node serves and `RUVIO TOPOLOGY SET map` installs a newer one (sent by `ruvio coordinator`). Hosts and ports come from `advertised_host` and `advertised_port`. Each range lists the shard's primary, then the replicas that announced an address and finished copying; a replica lists its primary first and itself second, so a client that knows only a replica still sends writes to the primary. Until a primary that announces its address accepts it, a replica of a multi-node cluster lists its group's primary as the cluster map names it (the map can lag one failover behind); without a map it lists itself. Node ids are the SHA-1 of the announced `host:port`. Replicas serve reads with or without `READONLY`; `READONLY` and `READWRITE` return `OK`. `CLUSTER KEYSLOT` also works with one shard; the others return `ERR cluster mode is disabled`. `INFO` reports `cluster_enabled`. The ACL command `cluster` is in `@slow` only, so application users can discover slots. There is no cluster bus or `ASK`; a range moved between a node's own shards gets no `MOVED`, one moved to another group does. See [THREAT_MODEL.md](THREAT_MODEL.md). |
| Slot moves | `RUVIO MOVESLOTS` | Ruvio only. `RUVIO MOVESLOTS first-last shard` moves a slot range to another shard of the same server while both keep serving, and answers `OK` once the move starts; `RUVIO MOVESLOTS STATUS` reports the running or last move as a map of strings. One move at a time, every slot of the range on one shard, refused while replication is on and in `MULTI` or scripts, `@admin @dangerous`. Clients that still route to the old shard are forwarded. A client blocked on a moved key gets `UNBLOCKED`, an `EXEC` whose watched keys moved runs nothing, and a pipelined command routed to the old shard at the hand-over may get `TRYAGAIN`. Between groups of a multi-node cluster, `RUVIO MIGRATE` moves a range instead, sent by `ruvio coordinator` (`COORDINATOR MOVE first last group shard`, or a sequence of such moves with `COORDINATOR REBALANCE [DRYRUN]`, see `docs/COORDINATOR.md`). The source node copies the range to the target shard over its client port while both keep serving, waits until the target's replicas hold the copy (`RUVIO MIGRATE REPLICAS id` on the target answers how many do not yet), then freezes it for the last changes: commands for the range get `TRYAGAIN` until the coordinators commit the move (the target must prepare within one second of the freeze, or the source cancels and serves the range again, and the coordinators start the move again) and a newer map gives the range to the target. The source then answers `MOVED`, its replicas and the target's follow through the log, and the source drops its copy in bounded steps. A failover before the commit cancels the move and the coordinators start it again. Needs `cluster_topology` and a primary; `@admin @dangerous`. |

## Data Semantics

- `SET` replaces a value of any existing type.
- Commands targeting an incompatible type return Redis-style `WRONGTYPE`.
- `INCR`, `DECR`, `INCRBY`, and `DECRBY` use signed 64-bit decimal integers.
- Invalid integers and overflow return an error without modifying the value.
- Missing counters begin at zero. Increments and `APPEND` keep an existing TTL;
  `GETSET` and `MSET` clear it, matching `SET`.
- `MGET` returns a nil bulk for a missing or wrong-type key. `GET`, `GETSET`,
  `APPEND`, and `STRLEN` return `WRONGTYPE` for a non-string key.
- `UNLINK` deletes synchronously, like `DEL`. `SCAN` and `DBSIZE` inspect the
  shard that received the command. `SCAN` cursors are slot positions in the
  shard's key table tagged with its rebuild count, not Redis dict cursors. A
  cursor from before a table rebuild restarts at slot 0, so keys can repeat but
  none is skipped. `COUNT` bounds the slots examined (10 per requested key),
  so a page can hold fewer keys than `COUNT`; `0` ends the walk.
- `SSCAN` and `HSCAN` on a large set or hash walk its table by bucket: a page
  costs `COUNT`, and the cursor stays valid across writes. If the table is
  rebuilt between pages (it grew or shrank) the walk starts again from the
  beginning, so a member can appear twice but never goes missing. Small
  collections and `ZSCAN` page by position (score order for `ZSCAN`), so a
  concurrent write can shift members between pages. `SRANDMEMBER`,
  `HRANDFIELD` and `ZRANDMEMBER` cost the requested count, not the collection
  size. A count returns members that sit next to each other in the table, so
  they are random between calls but not independent draws. `SPOP` picks the
  same way.
- `BLPOP` and `BRPOP` are shard-local. A push delivers the next value to the
  oldest waiter on that key before the remaining values are stored. At most
  1,024 waiters may be parked on one shard; excess clients receive
  `ERR max number of blocking clients reached`. Timeout `0` waits until a
  value arrives or the process shuts down.
- `MULTI`/`EXEC` is a per-connection queue plus `WATCH` optimistic locking, not
  an ACID transaction. Queued commands must hash to one slot even on a
  single-shard process; use a hash tag for multi-key `EXEC`. `EXEC` applies
  them in order, then records a single WAL batch. Persistence failure rolls
  the shard keys back. A `WATCH`ed key mutated by any connection, including
  the watcher, aborts `EXEC` with a null array. `DISCARD` drops the queue and
  leaves watches. Cross-shard two-phase commit is not supported.
- `EVAL` compiles Lua only at request time. Restart recovery applies the
  script's mutation batch and never re-runs Lua. `EVALSHA` after restart is
  `NOSCRIPT` until `SCRIPT LOAD` or `EVAL`. `math.random` is removed. Scripts
  cannot run inside `MULTI`. A Lua runtime error or refused script WAL batch
  restores the declared keys' values and their original absolute key expiration
  deadlines; rollback does not renew TTLs. Returning `ruvio.error_reply(...)`
  is a returned error value, not a Lua runtime exception.
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, and `PERSIST`
  are supported. Expired keys are absent. `SET` accepts `EX`, `PX`, `EXAT`,
  `PXAT`, `KEEPTTL`, `NX`, `XX`, and `GET`. `SET GET` returns the previous
  string and refuses a non-string. A relative
  expire of zero or less is an error. An absolute deadline in the past leaves
  the key absent and still replies `OK`.
- Durability is selected with `memory`, `snapshot`, `everysec`, or `always`.
  Redis RDB/AOF files are not compatible.

## Sharding Semantics

- Slots use the Redis CRC16 algorithm over 16,384 slots.
- Redis hash tags such as `{account42}` are supported.
- A request sent to the wrong shard is forwarded to the owner and answered on the same connection; only `SSUBSCRIBE`/`SPUBLISH` on the wrong shard receive `MOVED`.
- A multi-key request spanning slots receives `CROSSSLOT`.
- Topology is static. Membership, migration, and failover are not implemented.
  Asynchronous WAL tail replication is a separate primary and its replicas, not
  Redis Cluster replication.
- The current cluster behavior is experimental and must not be presented as a
  complete Redis Cluster implementation.

## Replication

Replication is an asynchronous WAL tail from one primary to up to 16 replicas.
Each replica has its own stream and its own sequence. The primary does not
wait for an acknowledgement, and a slow replica blocks only its own send. A
new replica does not replace an existing one. Past 16, the primary closes the
new connection. Automatic failover is `ruvio coordinator` ([COORDINATOR.md](COORDINATOR.md)), which
also answers the Sentinel discovery commands clients use. Without it an operator promotes a caught-up
replica with `REPLICAOF NO ONE` (or by restarting it as a writer) and points other
servers at a new primary with `REPLICAOF host port`; `ROLE` answers as in Redis,
listing each replica that announced its client address. Cluster clients can read from
replicas with a read policy ([CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md#reads-from-replicas)).
The steps are in [RECOVERY.md](RECOVERY.md#promote-a-replica-by-hand). Every
promotion raises an epoch: a replica that followed a newer primary does not follow
an older one, and an old primary that such a replica connects to stops taking
writes. `min-replicas-to-write` and `min-replicas-max-lag` work as in Redis. `WAIT replicas milliseconds`
blocks only that client until that many replicas have synced this shard's
`wal_last_sequence` to disk (in `port_mode = "single"` every shard's; in per-shard mode also the shards this connection's writes were forwarded to), or the timeout. Timeout `0` waits until shutdown.
The reply is how many replicas reached the sequence. A lagging replica does
not discard the write. `GET`, `SET`, and `INCR` do not wait. `WAIT` is not
queued inside `MULTI` and is not scriptable. A compacted
gap installs the primary checkpoint when one covers the replica, then the WAL
tail continues. A gap with no usable checkpoint still stops the replica. A
hot-key `SET` or `DEL` is copied into the hot store. `PUBLISH`
and `SPUBLISH` are copied live to replicas of the same shard and are not
replayed from the WAL. `CLIENT TRACKING` on a replica observes a copied write.
`FUNCTION LOAD` and `ACL SETUSER` are refused while replication is enabled. Scripts (`EVAL`, `EVALSHA`) run on a primary, and the writes they make are replicated; the script bodies are not, so a replica that becomes primary answers `NOSCRIPT` until a script is loaded again.

## Persistence Compatibility

Ruvio will use versioned Ruvio WAL and snapshot formats. Redis RDB and AOF files
are not wire-compatible with those formats. A future import/export tool may
translate data, but translation is not part of the storage engine's recovery
path.

## Machine-Readable Matrix

The command list, intentional differences, and first-party client versions
are published in [`compatibility-matrix.json`](compatibility-matrix.json).
`scripts/check_compatibility_matrix.py` fails when the JSON, this page, the
parser, and the listed coverage surfaces drift.

Run the Rust and C# client tests with `python3 scripts/run_client_suites.py`
against a live Ruvio process. See [`clients/README.md`](../clients/README.md).

## Compatibility Policy

- A command becomes supported only when parser, execution, response, wrong-type,
  limit, persistence, and differential tests pass.
- Supported behavior is the same on Linux, macOS, and Windows.
- Intentional Redis differences must be documented here before release.
- Removing supported behavior requires a major version change.
- Adding commands or RESP3 behavior is backward-compatible when existing RESP2
  replies are unchanged.
- Persisted-format compatibility is governed separately (see Package
  Versions below).

## Package Versions

Ruvio uses semantic versions for the server crate and the container tag.

- A patch release fixes behavior without adding commands or changing the
  on-disk format.
- A minor release may add commands and additive format fields. Existing RESP2
  replies and the current format major stay readable.
- Before 1.0, a minor release may still change unsupported behavior. Format
  major stays 1 until a breaking format change is announced.
  Every format change updates the v1 fixtures or adds the next fixture set.
- From 1.0, a release reads every data directory written by an earlier release
  in the same format major. A format-major bump ships with an offline migration
  or a documented export path, and the package major increases with it.
- Clients (`ruvio-client`, `Ruvio.Client`) version on their own. A server
  minor does not require a client upgrade when the commands that client uses
  are unchanged.
