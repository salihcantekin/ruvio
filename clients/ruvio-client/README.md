<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">ruvio-client <sub>Rust</sub></h1>

<p align="center">Blocking RESP2 client for <a href="https://ruviodb.com/">Ruvio</a>, with cluster routing and a typed method for every listed command.</p>

<p align="center">
  <a href="https://crates.io/crates/ruvio-client"><img alt="crates.io" src="https://img.shields.io/crates/v/ruvio-client?label=crates.io"></a>
  <a href="https://crates.io/crates/ruvio-client"><img alt="Downloads" src="https://img.shields.io/crates/d/ruvio-client?label=downloads"></a>
  <a href="https://docs.rs/ruvio-client"><img alt="docs.rs" src="https://img.shields.io/docsrs/ruvio-client?label=docs.rs"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/rust/">Rust guide</a> ·
  <a href="https://docs.rs/ruvio-client">docs.rs</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Blocking RESP2 client for [Ruvio](https://ruviodb.com/). Connecting does not send `CLIENT SETINFO` or `HELLO`. Give the client one address; it sends `CLUSTER SLOTS` and, on a sharded server, opens one socket per shard and routes by hash slot. `MOVED` is followed up to three times per command. `MGET` / `MSET` / `DEL` / `EXISTS` / `UNLINK` / `TOUCH` split across slots; other commands whose keys span slots fail with `CROSSSLOT` before anything is sent. Methods take `&mut self` and are named after the command they send. They cover every command in [`docs/compatibility-matrix.json`](../../docs/compatibility-matrix.json) (299/299, checked by `tests/coverage.rs`) plus the options that change what Ruvio does. `execute` and `execute_many` send any argument list, as `redis-cli` would; `execute_binary` does the same with raw byte arguments.

```rust
use std::time::Duration;
use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
db.set("session:ada", "hello")?;
let value = db.get_string("session:ada")?;
db.incr("visits")?;
db.expire("session:ada", Duration::from_secs(60))?;
db.hset("user:1", &[("name", "Ada"), ("plan", "pro")])?;
db.zadd("leaderboard", &[("player1", 100.0), ("player2", 80.0)])?;
let top = db.zrevrange_with_scores("leaderboard", 0, 9)?;
```

## Connecting

`hash_slot(key)` is the same 0–16383 slot Ruvio uses, including `{hash tags}`. Set `ClientOptions::discover_cluster` to `false` to stay on the seed socket. An incomplete, overlapping, or malformed `CLUSTER SLOTS` map fails discovery instead of silently using an ambiguous owner. A disconnected stale owner triggers a refresh from another connected node before sending the command; a dropped in-flight command is not replayed automatically. Cross-slot `MSET` / `DEL` are client-side operations, not atomic transactions: an error may leave some keys changed. Use a shared hash tag for atomic multi-key writes.

| In a cluster | What the client does |
| --- | --- |
| `MOVED` | Followed up to 3 times per command; the 4th `MOVED` is returned. Every `MOVED` points that slot at the named node and the command is sent again there. The whole slot map is reloaded first when that node is unknown to the client, and on the command's second and third `MOVED`; the slot still points at the named node afterwards. |
| `ASK` | Followed like `MOVED`, sharing its limit of 3 redirects per command: `ASKING` and the command go to the named node in one write and its reply is returned. The slot map is not changed, so the next command for that slot still goes to the map's owner. |
| `TRYAGAIN` | The command is sent again, routed by the current slot map, up to 7 times after sleeps of 5, 10, 20, 40, 80, 160 and 320 ms (635 ms in all); the 8th `TRYAGAIN` is returned. |
| `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK`, `TOUCH` across slots | Sent once per slot and merged: `MGET` keeps key order, counts are summed, `MSET` returns `OK`. |
| `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ... across slots | Not sent: `Error::Server("CROSSSLOT Keys in request do not hash to the same slot")`, the server's own reply. Put the keys under one hash tag. |
| `execute_many` (a pipeline) | Commands are grouped per node and replies come back in command order. A cross-slot split command is split as above and its merged reply sits at its index; any other cross-slot command gets the `CROSSSLOT` error at its index and the rest of the pipeline still runs. A pipeline holding `MULTI` goes to one node and must stay in one slot. |
| `execute_binary` | Routed by the second argument; no splitting or cross-slot check. |

### Seed addresses

`ClientOptions::seeds` lists `(host, port)` pairs to use instead of `host` and `port`. Connecting tries them in order and keeps the first that answers; a seed that cannot be reached is skipped, a refused `AUTH` stops at once. After a dropped connection each reconnect attempt tries every seed once, starting with the one after the address that dropped, so a client follows a primary that was moved by hand. In cluster mode the slot map comes from the first reachable seed; when a node is gone and no open socket answers `CLUSTER SLOTS`, the other known nodes and then the seeds are asked. The command that hits the drop still fails. The same rules hold in every client ([docs/CLIENT_BEHAVIOR.md](../../docs/CLIENT_BEHAVIOR.md)).

```rust
let mut client = Client::connect_with(ClientOptions {
    seeds: vec![("10.0.0.5".into(), 6379), ("10.0.0.6".into(), 6379)],
    ..ClientOptions::default()
})?;
```

### Following a failover

With two or more seeds a standalone client finds the primary again after a failover. On a reconnect it asks the seeds `ROLE` and connects to the one that answers `master` (a replica's reported primary is tried too); if none does it falls back to the first seed that answers. When a command is answered `READONLY` (the node was demoted) it does the same and sends that command once more to the new primary: `READONLY` is sent before the command runs, so the retry cannot apply it twice. It is not sent again inside a transaction the client opened, or in a pipeline, and a second `READONLY` is returned. The rounds wait with the reconnect backoff (`max_reconnect_attempts`, `reconnect_base_delay`, `reconnect_max_delay`); with `reconnect: false` there is one round. Nothing is sent while the connection is healthy, and a single address behaves as before. `ClientOptions::follow_primary = false` turns it off.

With `sentinel_master` set the seeds are coordinator (or Sentinel) addresses: the client asks `SENTINEL get-master-addr-by-name <name>`, checks the answer with `ROLE`, and connects there, at connect time, on reconnect and after `READONLY`. The same `AUTH` credentials go to the coordinators and the data node. The rules are the same in every client ([docs/CLIENT_BEHAVIOR.md](../../docs/CLIENT_BEHAVIOR.md#following-a-failover)).

```rust
let mut client = Client::connect_with(ClientOptions {
    seeds: vec![("10.0.0.5".into(), 6379), ("10.0.0.6".into(), 6379)],
    ..ClientOptions::default()
})?;
// Or with coordinators: seeds are their addresses.
let mut client = Client::connect_with(ClientOptions {
    seeds: vec![("10.0.0.9".into(), 26379)],
    sentinel_master: Some("main".into()),
    ..ClientOptions::default()
})?;
```

### Reads from replicas

`ClientOptions::read_policy` decides where a cluster client sends read-only commands. `ReadPolicy::Primary` (the default) keeps every command on the slot's primary. `ReadPolicy::PreferReplica` sends reads to one replica of the slot's shard, picked at random when the slot map loads, and falls back to the primary when none is known, it cannot be opened, or it answers `LOADING`. `ReadPolicy::Replica` sends reads only to replicas and fails with `Error::Protocol` ("no replica ...") when none is usable. Reads are the commands the server flags `readonly` and not blocking (`GET`, `MGET`, `HGETALL`, `ZRANGE`, ...); writes, transactions, blocking commands, scripts and Pub/Sub stay on the primaries, and a pipeline goes to replicas only when every command in it reads. Replica connections send `READONLY` when they open. Replication is asynchronous, so a read from a replica may miss recent writes, including your own. Standalone connections ignore the policy. The rules are the same in every client ([docs/CLIENT_BEHAVIOR.md](../../docs/CLIENT_BEHAVIOR.md#reads-from-replicas)).

```rust
use ruvio_client::{Client, ClientOptions, ReadPolicy};

let mut client = Client::connect_with(ClientOptions {
    host: "10.0.0.5".into(),
    port: 7000,
    read_policy: ReadPolicy::PreferReplica,
    ..ClientOptions::default()
})?;
```

A standalone client is bound to one logical database. Database 0 sends nothing after discovery. Any other index sends `SELECT` before the connect call returns. Cluster mode has only database 0.

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use ruvio_client::{Client, ClientOptions};

let mut cache = Client::connect_database("127.0.0.1", 6379, 2)?;
let mut sessions = Client::connect_ip(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379, 3)?;
let mut main = Client::connect_addr(SocketAddr::from(([10, 0, 0, 5], 6379)), 0)?;
let mut secured = Client::connect_with(ClientOptions {
    host: "10.0.0.5".into(),
    username: Some("app".into()),
    password: Some("secret".into()),
    database: 4,
    ..ClientOptions::default()
})?;
```

`select` switches the same connection, and `into_database` does the same and returns the client. `flushdb`, `flushall`, `swapdb`, and `move_key` send `FLUSHDB`, `FLUSHALL`, `SWAPDB`, and `MOVE`.

`ClientOptions::durability` (`Some(Durability { replicas, timeout_ms })`) makes every connection the client opens, including reconnects and the per-shard sockets of a cluster client, send `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]` right after `AUTH`. The server then holds that connection's replies until `n` replicas have the writes on disk, and answers `-NOREPLICAS` after the timeout (writes stay applied). A refusal, such as on a server without replication, fails the connect with `Error::Server`. The default `None` sends nothing. `client_durability(replicas, timeout_ms)` sets it on one connection; 0 turns it off.

A password is sent as `AUTH` only when `ClientOptions::password` is set. A server error reply leaves the connection usable. A broken read or write does not.

## Commands

`idempotency_begin(key, fingerprint, owner, ttl_ms)` sends native
`IDEM key BEGIN ...` and returns the state array with optional binary
result. `idempotency_complete(key, fingerprint, owner, result)` sends native
`COMPLETE` with a text result and returns 1 or 0. Only an acquired reservation
allows execution; a pending response must not be treated as permission to retry.
Neither completion nor replay renews retention. This is not exactly-once for
external effects; see [the native contract](../../docs/IDEM.md).

| Family | Methods |
| --- | --- |
| Ruvio jobs | `lease`, `release`, `semaphore`, `setv`, `limit`, `once`, `take`, `transfer`, `htransfer`, `getex`, `incr_by_max`, `changes_start`, `changes`, `idem_begin`, `idem_complete`, `idempotency_begin`, `idempotency_complete`, `fill_get`, `fill_get_wait` (`WAIT`), `fill_set` (`SWR`) |
| Ruvio extensions | `delex` (`DELEX key IFEQ v`), `set_if_eq`, `set_if_eq_keep_ttl` (`SET ... IFEQ`), `set_if_greater`, `set_if_less` (`SET ... IFGT` / `IFLT`), `lpushcap`, `rpushcap`, `json_index_create`, `json_index_drop`, `json_index_list`, `json_find`, `ruvio` |
| Strings | `get`, `get_string`, `set`, `set_with`, `set_ex`, `set_exat`, `set_expires_at`, `set_keep_ttl`, `set_and_get`, `set_bytes`, `setex`, `psetex`, `setnx`, `getdel`, `mget`, `mset`, `msetnx`, `getset`, `append`, `strlen`, `incr`, `incr_by`, `incr_by_float`, `decr`, `decr_by`, `getrange`, `substr`, `setrange` |
| Bitmaps / HyperLogLog | `setbit`, `getbit`, `bitcount`, `bitcount_range` (`BYTE`/`BIT`), `bitpos`, `bitpos_range`, `bitop`, `bitfield`, `bitfield_ro` (typed `BitFieldOp`, `BitFieldType`, `BitFieldOverflow`), `pfadd`, `pfcount`, `pfmerge` |
| Keys | `del`, `del_key`, `unlink`, `unlink_key`, `exists`, `exists_key`, `key_type`, `rename`, `rename_nx`, `copy`, `touch`, `keys`, `random_key`, `scan`, `scan_iter`, `dbsize`, `expire`, `pexpire`, `expire_at`, `expire_time`, `pexpire_time`, `ttl`, `pttl`, `persist`, `dump`, `restore`, `object_encoding`, `object_idletime`, `object_freq`, `memory_usage`, `memory_usage_samples` |
| Lists | `lpush`, `rpush`, `lpushx`, `rpushx`, `lpop`, `lpop_count`, `rpop`, `rpop_count`, `llen`, `lindex`, `lrange`, `ltrim`, `lset`, `linsert` (`InsertPosition`), `lrem`, `lpos`, `lpos_with`, `lpos_count`, `lmove`, `blmove`, `rpoplpush`, `lmpop`, `blmpop` (`ListSide`), `blpop`, `blpop_key`, `brpop`, `brpop_key` |
| Sets | `sadd`, `srem`, `sismember`, `smismember`, `scard`, `smembers`, `sinter`, `sintercard`, `sunion`, `sdiff`, `sinterstore`, `sunionstore`, `sdiffstore`, `smove`, `spop`, `spop_count`, `srandmember`, `srandmember_count`, `sscan`, `sscan_iter` |
| Hashes | `hset`, `hmset`, `hget`, `hdel`, `hlen`, `hgetall`, `hmget`, `hexists`, `hkeys`, `hvals`, `hincr_by`, `hincr_by_float`, `hsetnx`, `hstrlen`, `hrandfield`, `hrandfield_count`, `hrandfield_with_values`, `hscan`, `hscan_iter`, `hexpire`, `hexpire_with`, `hpexpire`, `hexpire_at`, `httl`, `hpttl`, `hexpiretime`, `hpexpiretime`, `hpersist` |
| Sorted sets | `zadd`, `zadd_with`, `zadd_incr`, `zincr_by`, `zrange`, `zrevrange`, `zrange_with_scores`, `zrevrange_with_scores`, `zrange_by_lex`, `zrange_query`, `zrange_query_with_scores` (`BYSCORE`/`BYLEX`/`REV`/`LIMIT`), `zrange_by_score`, `zrange_by_score_with_scores`, `zrangebylex`, `zrevrangebylex`, `zcount`, `zlexcount`, `zmscore`, `zrem`, `zcard`, `zscore`, `zrank`, `zrevrank`, `zpopmin`, `zpopmax`, `bzpopmin`, `bzpopmax`, `zremrangebyrank`, `zremrangebyscore`, `zremrangebylex`, `zunion`, `zinter`, `zdiff` (and `_with_scores`), `zunionstore`, `zinterstore`, `zdiffstore`, `zrandmember`, `zrandmember_count`, `zrandmember_with_scores`, `zscan`, `zscan_iter` |
| Geo (`GeoUnit`, `GeoOrder`) | `geo_add`, `geo_add_with`, `geo_dist`, `geo_hash`, `geo_pos`, `geo_search`, `geo_search_with` (`BYBOX`, `COUNT ANY`, `WITHDIST`/`WITHHASH`/`WITHCOORD`), `geo_search_store`, `geo_radius`, `geo_radius_by_member` |
| Bloom filters | `bf_reserve`, `bf_reserve_with` (`EXPANSION`/`NONSCALING`), `bf_add`, `bf_madd`, `bf_insert`, `bf_exists`, `bf_mexists`, `bf_info`, `bf_info_field`, `bf_card` |
| JSON | `json_get`, `json_mget`, `json_set`, `json_set_with` (`NX`/`XX`), `json_merge`, `jmerge`, `json_del`, `json_forget`, `json_type`, `json_strlen`, `json_strappend`, `json_arrlen`, `json_objkeys`, `json_objlen`, `json_numincrby`, `json_nummultby`, `json_toggle`, `json_arrappend`, `json_arrappend_many`, `json_arrinsert`, `json_arrpop`, `json_arrtrim`, `json_arrindex`, `json_clear`, `json_index_*`, `json_find` |
| Streams | `xadd`, `xadd_maxlen`, `xadd_delay`, `xadd_nomkstream`, `xlen`, `xrange`, `xrevrange`, `xdel`, `xtrim_maxlen`, `xtrim_minid`, `xadd_trim` / `xtrim` (`StreamTrim`: `MAXLEN`/`MINID`, `=`/`~`), constants `stream_id::*`, `xsetid`, `xread`, `xgroup_create`, `xgroup_create_consumer`, `xreadgroup`, `xgroup_destroy`, `xgroup_setid`, `xgroup_delconsumer`, `xack`, `xpending_summary`, `xpending`, `xclaim`, `xclaim_ids`, `xautoclaim`, `xinfo_stream`, `xinfo_groups`, `xinfo_consumers` |
| Transactions | `multi`, `exec`, `discard`, `watch`, `watch_key`, `unwatch` |
| Scripts and functions | `eval`, `evalsha`, `script_load`, `script_exists`, `script_flush`, `script_kill`, `fcall`, `function_load`, `function_list`, `function_delete` |
| Pub/Sub | `publish`, `spublish`, `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe`, `ssubscribe`, `sunsubscribe`, `next_message`, `read_message`, `subscriber`, `sharded_subscriber`, `listen` |
| Server | `ping`, `ping_message`, `echo`, `auth`, `auth_user`, `info`, `info_section` (`InfoSection`), `config_get`, `config_set`, `config_rewrite`, `replicaof`, `replicaof_no_one`, `role`, `move_slots`, `move_slots_status`, `save`, `bgsave`, `wait`, `select`, `flushdb`, `flushall`, `swapdb`, `move_key`, `hello`, `command_list`, `command_count`, `command_info`, `command_docs`, `latency_latest`, `slowlog_get`, `slowlog_len`, `slowlog_reset`, `client_id`, `client_info`, `client_list`, `client_list_type` (`ClientType`), `client_kill_id`, `client_kill_addr`, `client_getname`, `client_setname`, `client_tracking`, `cluster_slots`, `cluster_nodes`, `cluster_shards`, `cluster_info`, `cluster_myid`, `cluster_keyslot`, `readonly`, `readwrite` |
| ACL | `acl_whoami`, `acl_users`, `acl_list`, `acl_getuser`, `acl_cat`, `acl_setuser`, `acl_load`, `acl_save` |

`SortedSetAddOptions`, `StreamReadOptions`, `StreamPendingFilter`, and `StreamClaimOptions` carry the optional `ZADD`, `XREAD`/`XREADGROUP`, `XPENDING`, and `XCLAIM` arguments. `eval`, `evalsha`, and `fcall` take keys and arguments separately and send `keys.len()` as the key count.

Inside `MULTI` the server answers `QUEUED` instead of the typed reply, so queue commands with `execute` and read the results from `exec`:

```rust
db.watch(&["balance"])?;
db.multi()?;
db.execute(&["INCRBY", "balance", "10"])?;
db.execute(&["GET", "balance"])?;
let replies = db.exec()?; // None when a watched key changed
```

`subscribe` opens a second socket, the same way `Ruvio.Client` does. Keep `publish`, `GET`, and `PING` on the same client. `listen` (callback) or `next_message` reads that subscriber socket. `subscriber()` still returns the extra socket if you want it separately. The subscriber socket has no read timeout until you call `set_read_timeout`; command sockets start with `ClientOptions::command_timeout` (30 s by default, plus a blocking command's own wait, see [Timeouts](../README.md#timeouts)). On a cluster that socket reaches one shard; `sharded_subscriber(channel)` opens one to the shard that owns a sharded channel, for `ssubscribe`.

A dropped socket is opened again before the next command, including a shard socket. The command that discovers the drop returns an error. `on_connection_lost` and `on_connection_restored` report the change. Set `ClientOptions::reconnect` to false and call `reconnect` yourself, or `set_reconnect_delay` to own the wait. Returning `None` stops the retry.

```rust
db.subscribe(&["news"])?;
assert_eq!(db.ping()?, "PONG");
db.listen(|message| println!("{}", String::from_utf8_lossy(&message.payload)))?;
```

## Helpers

Thin layers over the native commands, the same set every Ruvio client ships.

```rust
use std::time::Duration;
use ruvio_client::{CacheOptions, Client, Error, IdemOutcome};

// Distributed lock: SET key token NX PX, renew with SET ... IFEQ, release with DELEX IFEQ.
// The guard derefs to the client and releases on drop; it never deletes another holder's lock.
if let Some(mut lock) = db.try_lock("lock:report", Duration::from_secs(30))? {
    lock.set("report:status", "running")?;
    lock.renew(Duration::from_secs(30))?;
} // DELEX lock:report IFEQ <token>
let waited = db.lock("lock:report", Duration::from_secs(30), Duration::from_secs(2))?; // polls
db.with_lock("lock:job", Duration::from_secs(30), |db| db.incr("jobs"))?;

// Fenced lease and semaphore permits (RELEASE on drop).
if let Some(lease) = db.lease_guard("leader", Duration::from_secs(10))? {
    let fence = lease.fence(); // pass it to the protected resource
}
let permit = db.semaphore_permit("downloads", 4, Duration::from_secs(60))?;

// Rate limit: LIMIT key max window -> RateLimit { allowed, remaining }.
let verdict = db.rate_limit("login:ada", 5, Duration::from_secs(60))?;
// Smoother: sliding window, or a token bucket that says how long to wait.
let bucket = db.limit_tokens_with_retry("api:key-7", 10, 1_000, 2)?; // LimitOutcome { allowed, remaining, retry_after_ms }

// Cache-aside with stampede protection on FILLGET / FILLSET.
let page = db.get_or_create("page:home", Duration::from_secs(60), || {
    Ok::<_, Error>(render_home())
})?;

// Run once with IDEM BEGIN / COMPLETE.
match db.idempotent("pay:42", "fingerprint", "worker-1", Duration::from_secs(3600), || {
    Ok::<_, Error>(charge_card()?)
})? {
    IdemOutcome::Executed(result) | IdemOutcome::Replayed(result) => { /* reply */ }
    IdemOutcome::Pending | IdemOutcome::Conflict => { /* retry later / reject */ }
}

// Lazy scan iterators (SCAN / HSCAN / SSCAN / ZSCAN), one page in memory at a time.
for key in db.scan_iter(Some("session:*"), Some(500)) { let key = key?; }

// Capped "recent items" list (LPUSHCAP) and Bloom "seen before" (BF.MADD / BF.MEXISTS).
db.push_recent("recent:ada", 50, "item:9")?;
let last_ten = db.recent("recent:ada", 10)?;
let seen = db.seen_before("emails:seen", &["a@x", "b@x"])?; // [false, false] the first time
```

`get_or_create` polls plain `FILLGET` with a 2–50 ms backoff on `BUSY` instead of sending
`FILLGET ... WAIT`, which would park the connection; `fill_get_wait` is there when you want the
server-side wait. `CacheOptions` sets the fill timeout, the stale-while-revalidate window
(`FILLSET ... SWR`) and how long to wait before an `ErrorKind::TimedOut` error. Lock tokens come
from `new_token()` (128 random bits from std's hasher seeds, the clock, pid and a counter); they
identify the holder, they are not secrets. `acquire_lock`, `renew_lock` and `release_lock` take
your own token when the lock crosses processes.

With the `serde` feature, `json_set_value`, `json_set_value_with`, `json_merge_value`,
`json_arrappend_value`, `json_get_value`, `json_arrpop_value` and `json_find_value` take and return
typed values.

## TTL on create

Commands that can create a key take a deadline that is set only when the call creates the key; an existing key keeps the deadline it has. A `Duration` is sent as `EX` for whole seconds and as `PX` otherwise; zero is refused with `Error::Protocol` before anything is sent.

```rust,no_run
use ruvio_client::*;
use std::time::Duration;

fn run(client: &mut Client) -> Result<(), Error> {
    // A per-minute counter: the first hit starts the minute, later hits keep it.
    let hits = client.incr_with_ttl("hits:ada", Duration::from_secs(60))?;

    // Sorted set, geo, Bloom filter: a `ttl` field on the options.
    let options = SortedSetAddOptions { ttl: Some(Duration::from_secs(3600)), ..Default::default() };
    client.zadd_with("board", options, &[("ada", 120.0)])?;

    // EXPIRE with a condition (NX, XX, GT or LT).
    client.expire_if("hits:ada", Duration::from_secs(120), ExpireCondition::Gt)?;
    Ok(())
}
```

Methods: `incr_with_ttl`, `decr_with_ttl`, `incr_by_with_ttl`, `decr_by_with_ttl`, `incr_by_max_with_ttl`, `incr_by_float_with_ttl`, `hincr_by_with_ttl`, `zincr_by_with_ttl`, `setbit_with_ttl`, `xadd_with_ttl`, `xadd_trim_with_ttl`, `json_set_with_ttl` (and `json_set_value_with_ttl` with the `serde` feature). `SortedSetAddOptions` (`zadd_with`, `zadd_incr`, `geo_add_with`) and `BloomInsertOptions` (`bf_insert`) have a `ttl` field. `expire_if`, `pexpire_if`, `expire_at_if` and `pexpire_at_if` take an `ExpireCondition`; a key without a deadline counts as an infinite one, and a failed condition returns `false`.

## Binary values

`get` returns bytes. `set_bytes`, `restore` (for `dump` payloads), `idem_complete` and
`execute_binary` send raw bytes; everything else takes `&str`. On a cluster `execute_binary`
routes by its second argument and does not split multi-key commands.

## Performance notes

- Fixed-arity commands borrow their arguments into a stack array and format numbers into a stack
  buffer, so `GET`, `INCR`, `EXPIRE`, `DELEX`, `ZCOUNT` and the like allocate nothing but the reply
  value. Variadic commands allocate one `Vec<&str>`, never owned copies of the arguments.
- Each connection keeps one encode buffer and one buffered reader. Replies are parsed in place;
  bulk strings move into the returned `String`/`Vec<u8>` without a second copy.
- The client is synchronous and `&mut self`: one connection per thread (or a pool) is the scaling
  model. `execute_many` pipelines a batch in one write. There is no multiplexing of concurrent
  callers on one socket, unlike the C# client.

`RUVIO_ADDR=127.0.0.1:6379 cargo run --release --example alloc_bench` prints allocations, bytes
and time per command, codec-only encode/parse cost, and 1- and 4-thread GET/SET throughput. Numbers
from a 4-vCPU container, release build, one-shard server on loopback (noisy, shared machine):

| Measurement | alloc/op | time |
| --- | --- | --- |
| encode `SET k v` (no I/O) | 0 | 0.03–0.04 µs |
| parse `:int` / `+OK` / `$32` / `*3` (no I/O) | 0 / 1 / 1 / 2 | 0.03 / 0.05 / 0.06 / 0.15–0.23 µs |
| `GET` small, `GET` missing | 1 (the value), 0 | round trip |
| `INCR`, `EXPIRE`, `DELEX`, `ZCOUNT`, `LIMIT` | 0 | round trip |
| `SET`, `SET PX`, `SET IFEQ PX` | 1 (the 2-byte `OK`) | round trip |
| `LPUSHCAP` 1 item / `BF.MADD` 2 / `ZUNION` 2 keys | 1 / 3 / 5 | round trip |
| serial round trip, any of the above | | 13–67 µs (two runs; the host was shared and noisy) |
| GET / SET, 1 thread | | 17k–60k ops/s (two runs) |
| GET / SET, 4 threads | | 40k–46k ops/s (one-shard server) |

Allocation counts are exact (counting allocator); times are loopback on a shared 4-vCPU container
and vary run to run, so compare them only within one run.

## Tests

`cargo test` runs the unit tests and `tests/coverage.rs`, which reads the compatibility matrix and
fails when a command has no typed method. `RUVIO_TEST_ADDR=127.0.0.1:16390 cargo test` also runs
`tests/live.rs` (databases 0 to 7) and `tests/live_ext.rs` (databases 0 and 8 to 15) against a server
on that address; they flush those databases. Without the variable the live tests return early.
`tests/failover_live.rs` needs the pair `scripts/run_with_servers.sh` starts (`RUVIO_FAILOVER_ADDRS`, `RUVIO_FAILOVER_REPLICATION`); `tests/follow_failover.rs` uses scripted nodes and needs nothing.

## Not done / not verified

- Cluster routing for the new commands (`BITOP`, `SINTERCARD`, `JSON.MGET`, `XGROUP`/`XINFO`,
  keyless `RUVIO`/`JSON.INDEX`/`JSON.FIND`) is unit-tested (`command_keys`), but not run against a
  live multi-shard server in this round (`tests/cluster_live.rs` needs `RUVIO_CLUSTER_ADDR`).
- `ZRANGE ... BYSCORE REV` and `BYLEX REV` take the high bound first, as in Redis (older server
  builds expected the low bound first). `zrange_query` sends bounds exactly as given.
- `GEORADIUS ... STORE`, `XADD ... MINID` and `XAUTOCLAIM ... COUNT/JUSTID` have no dedicated
  typed option; send them with `execute`.
- No multiplexed/async connection: concurrent callers need one client each.

## Changes

See [CHANGELOG.md](https://github.com/salihcantekin/ruvio/blob/main/clients/ruvio-client/CHANGELOG.md).

## Typed JSON values (`serde` feature)

```toml
ruvio-client = { version = "0.3", features = ["serde"] }
```

`json_set_value`, `json_merge_value`, `json_arrappend_value` take any `Serialize` value and
`json_get_value::<T>` returns any `DeserializeOwned`. `db.json_set_value("doc", "$.city", "izmir")`
sends `"izmir"` with quotes. Serializer settings are serde attributes on your type. To use another
serializer, serialize yourself and call the raw `json_set`, `json_merge`, `json_arrappend`.
