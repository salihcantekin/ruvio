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
| `ASK` | Not followed; no client sends `ASKING`. Ruvio moves slots only between the shards of one process (`RUVIO MOVESLOTS`) and forwards a command sent to the old shard, so the server does not send `ASK` or `MOVED` for moved slots. A command routed to the old shard at the moment of the hand-over may get `TRYAGAIN` (send it again), and a client blocked on a moved key gets `UNBLOCKED` |
| Socket dropped before any reply byte | The command fails and is never sent again, reads included. The socket is opened again for the next command |
| Pipelines in a cluster | Grouped per node, replies returned in the original order |
| Seed addresses | A list of seed addresses can be given in place of the single host and port; with no list the host and port are used as before. Connecting tries the seeds in order, one connect attempt each, and keeps the first that answers. A seed that cannot be reached (refused, timed out) is skipped; a refused `AUTH` or `SELECT` stops at once. When none answers, the last connect error is returned |
| Reconnect with seeds (standalone) | Each reconnect attempt tries every seed once, starting with the seed after the address that dropped and ending with that address; the reconnect backoff runs between attempts, as with one address. The command that hit the drop still fails. A subscriber socket opened later starts at the address the command socket is on |
| Slot map reload (cluster) | `CLUSTER SLOTS` comes from the first reachable seed. A reload asks the nodes that have an open socket first; when none answers, it opens every other known node and then every seed, one connect attempt each, until one returns a slot map. A node that cannot be opened again (after its reconnect attempts) triggers that reload, and the command goes to the owner the new map names; a keyless command whose node is gone goes to the first reachable node of the new map. Seeds are never asked while the known nodes answer, so the routing path does not change |

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
