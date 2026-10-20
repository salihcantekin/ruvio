[Ruvio](../README.md) / [Docs](README.md) / Client behavior

# Client behavior

What the six clients do when a cluster redirects, a command spans slots, or a socket drops. They agree on most of it; the rows marked **differs** are the known differences left. Update this page in the same change as the client code. File references are relative to `clients/`.

## Shared test data

| File | Checked by |
| --- | --- |
| `testdata/slots.json` (key to hash slot, with hash tags and non-ASCII keys) | the server (`src/routing.rs`) and one test per client (`slot_vectors`) |
| `../docs/compatibility-matrix.json` (every command) | each client's coverage test |
| `testdata/commands.json` (arity, flags and key rule per command) | the server (`src/protocol/client_spec.rs`); `scripts/gen_client_keys.py` turns it into each client's key table |
| `testdata/key_vectors.json` (commands with the keys the server routes them by) | the server and one test per client (`key_vectors`) |
| `testdata/resp.json` (RESP wire vectors: `encode` maps a command's `args`, or `args_hex` for raw bytes, to the exact request bytes; `decode` maps reply bytes to a value `{"simple"}`, `{"error"}`, `{"int"}` (a decimal string beyond 2^53), `{"bulk"}`/`{"bulk_hex"}`, `{"null": true}` or `{"array": [...]}`; `incomplete` lists replies cut short, where the parser must wait for more bytes. Wire bytes are `bytes` (UTF-8 text) or `bytes_hex`. RESP2 only, because the Rust and C# clients do not parse RESP3) | one test per client (`resp_vectors`): every encoder and parser, Python with its own parser and hiredis when installed |
| `testdata/command_methods.json` (every matrix command to the method that sends it in each client: `{"commands": {"GET": {"rust": ["get", "get_string"], "csharp": "GetStringAsync", ...}}}`, sorted by command, one row per command; a field is one method name or a list when several methods send the command; a `Type.` prefix (Go `Tx.`/`PubSub.`, Node `Subscriber.`/`Transaction.`, Python `PubSub.`, C# `RuvioConnection.`) or Java `Class#` names a method off the main client) | each client's coverage test |

## Same in every client

| Topic | Behavior |
| --- | --- |
| Commands split across slots | `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are sent per slot and the replies merged (`MGET` keeps key order, counts are summed, `MSET` answers `OK`). Not atomic |
| Split commands inside a pipeline | Split the same way; the merged reply sits at the command's index and the other replies keep their order. A transaction must stay in one slot |
| Other commands across slots | `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL` and every other command that is not split fail with `CROSSSLOT` before anything is sent; nothing is emulated |
| `MOVED` | The slot is pointed at the named node and the command is sent there, at most 3 times; a fourth `MOVED` is returned. The whole slot map is reloaded only when the named node is unknown, or from the command's second `MOVED` on. Between the shards of one server Ruvio forwards a misrouted key instead (only a sharded Pub/Sub channel is redirected); a node of a multi-node cluster (`cluster_topology`) answers `MOVED` for another node's keys. Every client follows it live in `multi_node_live` tests (`RUVIO_MULTINODE_A_ADDR`, `RUVIO_MULTINODE_B_ADDR`, `RUVIO_MULTINODE_STALE_ADDR` from `scripts/run_with_servers.sh`: a client seeded with a node whose map is older is sent to the right node), besides the scripted fake-server tests |
| `ASK` | `ASKING` and the command are sent in one write to the named node, and the command's reply is returned; an error reply to `ASKING` is returned instead. The slot map is not changed, so the next command for that slot goes to the map's owner again. A node not yet known is opened. `ASK` and `MOVED` share one budget: at most 3 redirects per command, the fourth is returned. Ruvio itself does not send `ASK` (a slot range moves whole, see `TRYAGAIN`); clients follow it for Redis-compatible servers that do. Covered by scripted fake-server tests in every client |
| `TRYAGAIN` | Sent before anything runs: a slot range is being handed over, or the server's request queue is full. The command is routed again by the current slot map and sent up to 3 more times, after 5, 10 and 20 ms (a timer in async clients, never a blocked event loop or reader thread); the fourth `TRYAGAIN` is returned. Its budget is separate from the redirect budget. Commands in `MULTI`/`EXEC` and Pub/Sub are not retried. Python returns `TRYAGAIN` for a pipeline or a command split across slots to the caller; the other clients retry inside pipelines where they follow `MOVED`. A client blocked on a moved key gets `UNBLOCKED` |
| `NOREPLICAS` | Returned to the caller as a server error, never retried: a primary refuses writes because too few replicas acknowledge (`min-replicas-to-write`), because its lease from `ruvio coordinator` is not running (`primary-lease`, see [COORDINATOR.md](COORDINATOR.md#primary-lease)), or a `CLIENT DURABILITY` reply timed out (the write was applied) |
| Socket dropped before any reply byte | The command fails and is never sent again, reads included. The socket is opened again for the next command |
| Pipelines in a cluster | Grouped per node, replies returned in the original order |
| Seed addresses | A list of seed addresses can be given in place of the single host and port; with no list the host and port are used as before. Connecting tries the seeds in order, one connect attempt each, and keeps the first that answers. A seed that cannot be reached (refused, timed out) is skipped; a refused `AUTH` or `SELECT` stops at once. When none answers, the last connect error is returned |
| Reconnect with seeds (standalone) | Each reconnect attempt tries every seed once, starting with the seed after the address that dropped and ending with that address; the reconnect backoff runs between attempts, as with one address. The command that hit the drop still fails. A subscriber socket opened later starts at the address the command socket is on. With several seeds the address that answers `ROLE` with `master` comes first, see [Following a failover](#following-a-failover) |
| Slot map reload (cluster) | `CLUSTER SLOTS` comes from the first reachable seed. A reload asks the nodes that have an open socket first; when none answers, it opens every other known node and then every seed, one connect attempt each, until one returns a slot map. A node that cannot be opened again (after its reconnect attempts) triggers that reload, and the command goes to the owner the new map names; a keyless command whose node is gone goes to the first reachable node of the new map. Seeds are never asked while the known nodes answer, so the routing path does not change |
| Stream entry fields | Returned as `(name, value)` pairs in stored order; a name written twice comes back twice. Rust `Vec<(String, String)>`, C# `IReadOnlyList<KeyValuePair<string, string>>`, Go `XMessage.Values` (flat; `Map()` keeps the last value of a repeated name), Java `List<KeyValue<K, V>>`, Python a list of tuples, Node `[name, value][]`. `HRANDFIELD` with a negative count and `WITHVALUES` likewise returns every pair, repeats included |

## Reads from replicas

A cluster client can send read-only commands to replicas. The server lists each shard's replicas after its primary in `CLUSTER SLOTS` once they have finished copying (see [CONFIGURATION.md](CONFIGURATION.md)); the clients read them from there. Standalone connections ignore the policy.

| Topic | Behavior |
| --- | --- |
| Policies | `primary` (the default): every command goes to the slot's primary, as before. `prefer-replica`: reads go to a replica of the slot's shard when one is known and reachable, otherwise to the primary. `replica`: reads go to a replica; with none known or reachable they fail with the client's own error and a message containing "no replica" |
| Which commands are reads | Commands the server flags `readonly` and not `blocking` (from `testdata/commands.json`), such as `GET`, `MGET`, `HGETALL`, `ZRANGE`, `EXISTS`. Writes, keyless commands, transactions (`MULTI`/`EXEC`/`WATCH`), blocking commands, scripts, Pub/Sub, `SCAN` and the commands sent to every node stay on the primaries |
| Which replica | When the slot map loads, the client picks one replica per shard at random and keeps it until the next load, so many clients spread over the replicas while each client reads from one per shard |
| Split commands | Each per-slot part follows the same rule: `MGET` and `EXISTS` parts go to replicas, `DEL` and `MSET` parts to primaries |
| Pipelines | A pipeline goes to replicas only when every command in it is a read; a pipeline with a write is routed as before, so it never reads its own writes from a replica that has not received them |
| `READONLY` | Sent once on every replica connection right after it opens (and after a reconnect). Ruvio replicas serve reads without it; Redis replicas need it |
| Fallback | A replica that cannot be connected, or answers `LOADING` or `MASTERDOWN` (still copying, or its copy stalled), is skipped until the next slot map load; under `prefer-replica` the command goes to the primary, under `replica` it fails (that reply is returned). A socket that drops after a command was sent fails the command, as for every node |
| Staleness | Replication is asynchronous: a read from a replica may miss writes the primary already acknowledged, including the caller's own. `CLIENT TRACKING` on a replica connection invalidates when the write reaches that replica |
| Cost when unused | With `primary` the routing path does the same work as before plus one comparison of the policy; no replica connection is opened |

| Client | Option |
| --- | --- |
| Rust | `ClientOptions::read_policy`, `ReadPolicy::{Primary, PreferReplica, Replica}` |
| C# | `RuvioClientOptions.ReadPolicy`, `ReadPolicy.{Primary, PreferReplica, Replica}` |
| Go | `Options.ReadPolicy`, `ReadPrimary`, `ReadPreferReplica`, `ReadReplica` |
| Java | `RuvioOptions.Builder.readPolicy(ReadPolicy.{PRIMARY, PREFER_REPLICA, REPLICA})` |
| Node | `readPolicy: "primary" \| "prefer-replica" \| "replica"` |
| Python | `read_policy=ReadPolicy.{PRIMARY, PREFER_REPLICA, REPLICA}` or `"primary"`, `"prefer-replica"`, `"replica"` |

The live tests start a 2-shard primary and its 2-shard replica (`RUVIO_REPLICATED_ADDR`, `RUVIO_REPLICA_ADDR` from `scripts/run_with_servers.sh`) and check through the replica's `INFO get_commands` that a read went to the replica.

## Following a failover

After a failover the primary is another node. A standalone client with several seeds, or with a coordinator name, finds it again instead of staying on a node that is now a replica. All six clients do the same; this section is the contract. It applies to standalone connections only: a cluster client (a multi-node slot map) keeps its `MOVED`/`ASK` rules and does not run any of this.

| Client | Follow option (default on) | Coordinator option (default off) |
| --- | --- | --- |
| Rust | `ClientOptions::follow_primary` | `ClientOptions::sentinel_master: Option<String>` |
| C# | `RuvioClientOptions.FollowPrimary` | `RuvioClientOptions.SentinelMaster` |
| Go | `Options.DisableFollowPrimary` (zero value keeps it on) | `Options.SentinelMaster` |
| Java | `RuvioOptions.Builder.followPrimary(boolean)` | `RuvioOptions.Builder.sentinelMaster(String)` |
| Node | `followPrimary: boolean` | `sentinelMaster: string` |
| Python | `follow_primary=True` | `sentinel_master=None` |

Following is active when the option is on, the connection is standalone, and there are at least two seeds or a coordinator name is set. With one seed and no name nothing changes: reconnect and `READONLY` behave as in the tables above. Off restores exactly the behavior of those tables.

### Terms

A probe is a short-lived socket made only to ask one question. It connects (bounded by the connect timeout), sends `AUTH` as configured (nothing else: no `SELECT`, no durability, no subscriptions), sends one command, reads one reply, and closes. The reply wait of a probe is the smaller of the connect timeout and the command timeout (the connect timeout when the command timeout is off), so a hung seed costs one bounded wait and never the 30 s command timeout. A probe that cannot connect, times out, drops, or gets an error reply counts as "no answer" for that address. A refused `AUTH` is not "no answer": it fails the operation with that error at once, as connecting does.

`ROLE` is read from the reply array: first element `master` means primary; `slave` means replica, with element 2 the host and element 3 the port (an integer; a string is accepted) it reports for its primary. A replica that reports no host or port 0 reports nothing. Any other reply is "no answer". Ruvio's replica reports the primary's replication address, which is not always its client address; Redis-compatible servers report the client address. The reported address is therefore only a hint: it is probed like a seed and ignored when it does not answer `ROLE`.

### Finding the primary (one round)

Input: the seed list, a start index, an optional address to avoid. Output: the address of a node that answered `master`, or none.

1. Build a queue of the seeds in order, starting at the start index and wrapping around. The "seen" set holds the seeds.
2. Take the next address. Probe it with `ROLE`.
3. `master` and the address is not the one to avoid: the round ends with this address. (The avoided address is the one that just answered `READONLY`; a primary that was replaced can still say `master`.)
4. `slave` from the avoided address: nothing is avoided any more, in this round and the rounds after it of the same search. The node was a replica when it refused, so a later `master` from it means it was promoted.
5. `slave` with a reported address that is not in the seen set: add it to the seen set and to the end of the queue. Everything else (no answer, an avoided master, a reported address already seen) just moves on.
6. When the queue is empty, the round ends with none.

With a coordinator name the seeds are coordinators instead of data nodes. For each seed in order, probe it with `SENTINEL get-master-addr-by-name <name>`; an array of host and port (a string port) names an address, a nil or error reply is "no answer". Probe the named address with `ROLE` as above: if it answers `master` (and is not the avoided address) the round ends with it, otherwise the next coordinator is asked; `slave` from the avoided address ends the avoiding as in step 4. Replica hints are not followed in this mode. The same `AUTH` credentials go to coordinators and data nodes.

A round sends at most two probes per distinct address (a seed and, once, a reported address) and is bounded by the probe timeouts. No state of the connection is kept in a probe.

### On reconnect

When a standalone connection is found broken and `reconnect` is on, each reconnect attempt that follows runs one round (start index: the seed after the one that dropped; nothing avoided). If a primary is found, a normal connection (with `AUTH`, durability, `SELECT`, subscriptions restored as before) is opened to it. If that open fails with an I/O error, or the round found none, the attempt falls back to the existing seed order (first seed that answers, as in "Reconnect with seeds"): a client may then sit on a replica during an election and still serve reads, and its next write meets `READONLY` below. The reconnect backoff and attempt limits are unchanged. The command that hit the drop is still not sent again. The connection-lost and connection-restored hooks report the old and the new address. The first connect keeps its order (first seed that answers), except with a coordinator name, where the first connect asks the coordinators (one round, no waiting between rounds) and fails with the client's own protocol error naming the master when none names a primary.

### On `READONLY`

The reply to a command on a standalone connection is an error whose text is `READONLY` alone or `READONLY` followed by a space. Only then:

1. Run rounds to find the primary, avoiding the address that answered while it still says `master` (step 4 above: once it says `slave`, a later round takes it when it is promoted, so a client that reconnected to a replica during an election writes as soon as that replica is promoted instead of after the last round). Between rounds wait with the reconnect backoff (Rust and C#: their attempt count, base and maximum delay; Go, Java, Node and Python: up to 8 rounds, waits of 100 ms doubling to a 2 s cap, 7.1 s in all, in a timer or sleep that never blocks other work). With `reconnect` off, one round.
2. When a primary is found, open a normal connection to it (as on reconnect), replace the old socket, and report lost and restored.
3. Retry rule: the refused command is sent once more on the new connection, because `READONLY` is sent before the command runs, so it was not applied. The retry's reply, an error included (a second `READONLY` too), goes to the caller; there is never a third send and no new round for the retry.
4. When no round finds a primary, the original `READONLY` error is returned. The connection stays where it was.

Not retried (the connection still moves to the primary, so the next command works):

- A command sent inside a transaction the client opened (`MULTI` acknowledged, no `EXEC` or `DISCARD` since): its earlier queued commands are gone with the old socket, so sending only this one would break the transaction. The `READONLY` error is returned and the client is no longer in a transaction.
- Commands of a pipeline: a `READONLY` reply at an index of a pipeline is returned at that index; nothing follows and nothing is resent.
- Any other error, `LOADING`, `MASTERDOWN` and `NOREPLICAS` included, and any reply that is not `READONLY`.
- A command that failed with an I/O error (the "Socket dropped" rule stands: never sent again).

### Cost and timeouts

While the connection is healthy nothing is sent and nothing is checked on the success path: the only added work is looking at the text of an error reply that is already being returned. Python and Node check it where they already inspect error replies. A client that tracks the transaction flag does it only where it already looks at command names, and sets it on the `MULTI` reply. Probes are the only extra connections, and only during a reconnect or after a `READONLY` reply. Existing command timeouts are not changed; the added wait is bounded by (rounds x probes x probe timeout) plus the backoff, about 7 s of backoff in the default setup.

### Tests

Every client has a scripted test (nodes that answer `ROLE`, refuse writes with `READONLY`, and name a primary for `SENTINEL`) for: the retry once, the second refusal, the refusing replica taken once it is promoted (with seeds and with a coordinator name), a refusing node that keeps saying `master` never taken, the transaction, the reconnect order, the reported primary, the coordinator name, the option off, no extra commands while healthy, and a hung seed bounded. Every client also has a live test on the pair `scripts/run_with_servers.sh` starts (`RUVIO_FAILOVER_ADDRS`, the two client addresses, primary first; `RUVIO_FAILOVER_REPLICATION`, their replication addresses): connect with both as seeds, write, wait until the replica holds the key, `REPLICAOF NO ONE` on the replica, `REPLICAOF host port` (the new primary's replication address) on the old primary, and check that the next write through the client succeeds within 5 s and lands on the new primary; that the same situation with the option off is `READONLY`; and that after `CLIENT KILL ID` on the client's socket the reconnect is on the node whose `ROLE` is `master` before any write. Without the variables the live test returns early. Poll with deadlines, never fixed sleeps. No coordinator process is needed.

Every client also has a live test under real coordinators: `scripts/run_with_servers.sh` starts a group of two data nodes (`RUVIO_COORDINATOR_GROUP_ADDRS`, primary first, and their pids in `RUVIO_COORDINATOR_GROUP_PIDS`) watched by three `ruvio coordinator` processes (`RUVIO_COORDINATOR_ADDRS`, the group named in `RUVIO_COORDINATOR_MASTER`; ping 200 ms, down after 1 s, no secret). The test writes through a client with the coordinator option and through one with the two data nodes as seeds, waits until the replica holds both writes, kills the primary with SIGKILL, and has both clients write at once until each succeeds; each first write must come within 5 s of the kill (the writes keep trying for 15 s, so a slow one fails on the bound, not on a timeout). Then it checks that the surviving node answers `master` and holds both writes. The kill is not undone, so the group serves one test per run of the script. Measured on a Mac with a debug server: the coordinator client wrote again about 0.9-1.6 s after the kill. The seeds client took about 5 s (Java) to 7.2 s (the others) while the rounds avoided the refusing replica even once it was the primary; since it is taken once it is promoted (step 4 of the round), it writes about as soon as the coordinator client (1.56 s for both in one Linux run of the Rust test).

## Differences

| Topic | Rust | C# | Go | Java | Node | Python |
| --- | --- | --- | --- | --- | --- | --- |
| **differs**: a non-split command whose keys span slots, inside a pipeline | `CROSSSLOT` error at its index; the rest runs | sent to the first key's shard; the server answers | the whole pipeline fails with `ErrCrossSlot` before sending | `CROSSSLOT` error at its index; the rest runs | `CROSSSLOT` error at its index; the rest runs | the pipeline is refused before sending |
| **differs**: the `CROSSSLOT` error | `Error::Server` with the server's text | `RuvioException`, text ends "(checked by the client, nothing was sent)" | `ErrCrossSlot` | `RuvioServerException`, text ends "(checked by the client, nothing was sent)" | `RuvioError`, code `CROSSSLOT` | `CrossSlotError` |
| **differs**: a node that cannot be opened, under a pipeline | the slot map is reloaded and the pipeline is routed on the new map | the pipeline fails; the next command reloads | the pipeline fails; the next command reloads | the pipeline fails; the next command reloads | the pipeline fails, as do split and fan-out parts; the next command reloads | the pipeline fails, as do fan-out commands, `scan` and transactions; the next command reloads |
| seed option | `ClientOptions::seeds`, `(host, port)` pairs | `RuvioClientOptions.Seeds`, `host:port` strings | `Options.Seeds`, `host:port` strings | `RuvioOptions.Builder.seeds`, `host:port` strings | `seeds`, `host:port` strings; with `path` a `TypeError` | `seeds=`, `host:port` strings or `(host, port)` tuples |
| **differs**: a replica answers `LOADING` or `MASTERDOWN` to a read inside a pipeline (`prefer-replica`) | returned as that command's error | the read is sent again to the primary | returned as that command's error | the read is sent again to the primary | the read is sent again to the primary | returned as that command's error |
| **differs**: wait between seed rounds | the reconnect backoff | the reconnect backoff | none: the next command makes the next round | none: the next command makes the next round | none: the next command makes the next round | none: the next command makes the next round |
| **differs**: reconnect option | `ClientOptions::reconnect` (default on), attempts and backoff settings | `RuvioClientOptions.Reconnect` (default on), attempts and backoff settings | always on, no option | always on, no option | `reconnect` (default on) | always on, no option |
| follow-failover options | `follow_primary`, `sentinel_master` | `FollowPrimary`, `SentinelMaster` | `DisableFollowPrimary`, `SentinelMaster` | `followPrimary`, `sentinelMaster` | `followPrimary`, `sentinelMaster` | `follow_primary`, `sentinel_master` |

## Where it lives

| Client | Routing and redirects | Key rules (interpreter, and the generated table) |
| --- | --- | --- |
| Rust | `ruvio-client/src/cluster.rs` | `ruvio-client/src/cluster.rs`, `command_keys.rs` |
| C# | `csharp/Ruvio.Client/Cluster/ClusterRouter.cs` | `csharp/Ruvio.Client/Cluster/ClusterKeys.cs`, `CommandKeyRules.g.cs` |
| Go | `go/cluster.go`, `go/cluster_route.go`, `go/cluster_pipeline.go` | `go/cluster_keys.go`, `command_keys_gen.go` |
| Java | `java/.../internal/ClusterRouter.java`, `RoutedRequest.java` | `java/.../internal/ClusterKeys.java`, `CommandKeyRules.java` |
| Node | `node/src/cluster.ts` | `node/src/cluster.ts`, `command-keys.ts` |
| Python | `python/ruvio/cluster.py`, `python/ruvio/asyncio/cluster.py` | `python/ruvio/_cluster.py`, `_command_keys.py` |

## Routing cost

A cluster client pays for routing on every command, so the routing path follows the same shape in every client. The goal: per request, at most 1.3× the user-space CPU of the plain single-connection path at the same pipeline depth, and no allocation per command once the client is warm. Splitting a pipeline over N nodes also means N writes and N reads instead of one; that system-call cost is inherent and is not counted against the goal.

| Step | Rule |
| --- | --- |
| Topology | A node list plus a 16384-entry table of node indexes, replaced as a whole on refresh. A node's connection is found by its index, never by building a `host:port` string per command |
| Command name and keys | Found from the caller's arguments (or the already encoded frame) without allocating: a case-insensitive compare against the key table, and a direct path for the common "first argument is the key" rule |
| Slot | Hash tag and CRC16 computed in place over the key bytes |
| One command | After the connection is picked, the same send path as the plain client. Nothing is copied in case a `MOVED` arrives; a redirect re-sends from the caller's arguments |
| Replies | Only error replies are checked for `MOVED`, by prefix |
| Pipelines | One pass stores a node index per command in a reused buffer. If every command goes to one node, the plain pipeline path runs unchanged. Otherwise each command is encoded once, straight into its node's write buffer; every node is written before any is read (or the nodes run concurrently), and each reply is stored at its command's index |
| Split commands and transactions | Keep their own path (`MGET`/`MSET`/... split per slot, a transaction pinned to one node); the rules above are for ordinary commands |

Before and after numbers per client: [evidence/2026-10-16-client-cluster-routing.md](evidence/2026-10-16-client-cluster-routing.md). Each client's benchmark tool has the same plain, cluster and one-node rows (`cluster_cost` example, `--cluster-cost`, `RoutingCostBench`, `bench:cluster-cost`, `bench.py --cluster-cost`, Go `TestClusterCost`).
