[Ruvio](../README.md) / [Docs](README.md) / Coordinator

# Automatic failover with `ruvio coordinator`

`ruvio coordinator` is a separate process, started from the same binary, that watches replication
groups (a primary and its replicas) and fails a group over when its primary stops answering. It holds
no data and serves no data commands. Run three (or five) of them; they agree through Raft on which
member of each group is the primary, and only a majority can change it.

Without coordinators nothing changes: a primary and its replicas run as described in
[RECOVERY.md](RECOVERY.md), and promotion is a person running `REPLICAOF NO ONE`.

## What happens on a failure

1. Every coordinator connects to every member each `ping_interval_ms` and reads `ROLE`, `CONFIG GET
   shards` and `INFO replication` (the synced sequence of every shard).
2. When the Raft leader among the coordinators has not heard from the primary for `down_after_ms`, it
   asks the others. The primary is down when a majority of coordinators (the leader included) have not
   heard from it within `down_after_ms`. One coordinator's own network trouble cannot start a failover.
3. The leader picks the replica that answers, is not still copying its primary (`LOADING`), and holds the
   most of the history (the largest sum of `wal_durable_sequence` over its shards); ties go to the member
   listed first.
4. It records "primary = that member, epoch + 1" in the Raft log (a majority must accept it), sends the
   member `REPLICAOF NO ONE`, points the other members at it with `REPLICAOF host port`, and publishes
   `+switch-master` to subscribers.
5. A member that answers again and disagrees with the record is corrected: an old primary is made a
   replica of the new one. It continues from its own log when it wrote nothing after the new primary
   forked, and takes a full copy otherwise (see *Write history* in [RECOVERY.md](RECOVERY.md)).

If the leader coordinator dies, the remaining ones elect a new leader (one to two seconds) and carry on.
Commands to data nodes are repeated on every tick until the members agree with the record, so a failover
interrupted half way is finished by the next leader.

Measured on one Mac with three data nodes and three coordinators (`ping_interval_ms` 200,
`down_after_ms` 1000, debug build): the new primary took writes 1.2 seconds after the old one was killed;
with the coordinator leader killed first, 3.2–4 seconds.

## What can be lost, and split brain

Replication is asynchronous. Writes the old primary acknowledged but had not yet shipped are not on the
new primary. In a test that wrote continuously while the primary was killed, 0–1 of about 225
acknowledged writes were missing afterwards, always the last ones. A client that cannot lose a write
waits for it with `WAIT 1 timeout_ms`.

A primary cut off from the coordinators but still reachable by clients keeps taking writes until it
learns it was replaced. Two settings on the data nodes bound that window:

- `min-replicas-to-write 1` with `min-replicas-max-lag` (seconds): once its replicas follow the new
  primary, the old one refuses writes with `NOREPLICAS` within the lag. In the partition test it refused
  0.1 seconds after the failover.
- The epoch: a replica that follows the new primary refuses the old one, and the old one stops taking
  writes as soon as such a replica connects to it.

Writes the old primary took in that window are lost when it rejoins. Use `min-replicas-to-write 1` (or
higher) in every group a coordinator manages.

A lost `LEASE` or `SEMAPHORE` grant does not come back as the same token: tokens carry the cluster
map's version, which grows with every failover and every committed move, so the new primary's tokens
are larger than any the old one handed out. A resource that keeps the largest token it has seen
refuses a holder whose grant was lost. Without a coordinator the generation is the replication epoch;
with a hand-written map and no coordinator, it is the larger of the epoch and the map's `version`.

## Setting it up

Each data node needs `replication.listen` (so a promoted replica can serve replicas) and a WAL-backed
durability profile. Start the primary with `replication.role = "primary"` and the replicas with
`replica`; the coordinators take over from there.

`coordinator.toml` (one per coordinator; only `id`, `listen` and `data_dir` differ):

```toml
id = 1                                  # unique, listed in peers
listen = "0.0.0.0:26400"                # coordinators, Raft and the Sentinel API share this port
data_dir = "/var/lib/ruvio-coordinator" # Raft log, vote and snapshot
peers = ["1@ruvio-1:26400", "2@ruvio-2:26400", "3@ruvio-3:26400"]
secret = "change-me"                    # shared by every coordinator; RUVIO_COORDINATOR_SECRET overrides it
ping_interval_ms = 500                  # default 500
down_after_ms = 3000                    # default 3000
auth = "password"                       # how coordinators log in to data nodes; "user password" works too

[[groups]]
name = "main"
members = [
  { address = "ruvio-1:6379", replication = "ruvio-1:26379" },
  { address = "ruvio-2:6379", replication = "ruvio-2:26379" },
  { address = "ruvio-3:6379", replication = "ruvio-3:26379" },
]
```

```bash
ruvio coordinator --config coordinator.toml
```

The coordinator is part of the `ruvio` binary in every release (Linux, macOS, Windows) and in the Docker
image; there is nothing extra to download. For a binary install,
[deploy/coordinator.toml](../deploy/coordinator.toml) is a starting config and
[deploy/ruvio-coordinator.service](../deploy/ruvio-coordinator.service) a systemd unit next to
`ruvio.service`: on each of three machines run one data node and one coordinator.

- `address` is the member's RESP address (shard 0's port), the one clients use. `replication` is its
  `replication.listen` base address, the one `REPLICAOF` names. Host names are resolved on every
  connection, so container names work.
- On the first start the coordinators record as primary the member that answers `ROLE` as `master`; while
  none or several do, they wait and change nothing.
- The data-node user in `auth` needs `ROLE`, `INFO`, `CONFIG` (`GET`) and `REPLICAOF`.
- Put the coordinators on different machines, usually next to the data nodes. Three coordinators survive
  the loss of one; five survive two. A single coordinator works for tests but cannot fail over when it is
  the one that fails.
- [deploy/compose.failover.yml](../deploy/compose.failover.yml) runs three data nodes and three coordinators
  with Docker Compose; it needs `RUVIO_COORDINATOR_SECRET` and `RUVIO_REPLICATION_PASSWORD` (a replication
  listener on a public address requires a password), and `RUVIO_IMAGE` to use an image built from this
  repository. Checked on Docker 29 (arm64): killing the primary container made another member primary
  in 3.8 s (`down_after_ms` 3000), the restarted old primary came back as a replica, and with the
  coordinator leader killed too the remaining two failed over again in 3.6 s. The coordinators answer
  with the containers' names (`ruvio-2 6379`), so Sentinel clients must run on the same Docker network.

## Finding the primary from a client

Coordinators answer the part of the Redis Sentinel protocol clients use, so a client with Sentinel
support connects to the coordinators with the group name as the service name:

| Command | Answer |
| --- | --- |
| `SENTINEL get-master-addr-by-name group` | `[host, port]` of the agreed primary, or a null array |
| `SENTINEL masters`, `SENTINEL master group` | The primary as a field list: `name`, `ip`, `port`, `flags` (`master`, `s_down` when this coordinator cannot reach it), `last-ok-ping-reply`, `num-slaves`, `quorum`, `num-other-sentinels`, `down-after-milliseconds`, `config-epoch` |
| `SENTINEL replicas group` (`slaves`) | The other members, with `slave-repl-offset` (their synced sequence) |
| `SENTINEL sentinels group` | The other coordinators |
| `SUBSCRIBE +switch-master` | `group old-host old-port new-host new-port` on every failover |
| `ROLE` | `["sentinel", [groups]]` |
| `INFO` | `coordinator_id`, `leader_id`, `is_leader`, and `group_<name>:primary=…,epoch=…` |
| `AUTH secret` | Required first when `secret` is set; `NOAUTH` otherwise |

Checked with redis-py's `Sentinel` (5.2): it discovers the primary, writes through it, and writes to
the new primary about 1.2 seconds after the old one is killed (`scripts/coordinator_faults.py client`).
Other libraries were not tried. Ruvio's own clients do not speak Sentinel yet; after a failover they reach the new primary through their
seed addresses (see [CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md)) and `ROLE`.

## Security

The coordinator port carries the Raft traffic and the Sentinel API. With `secret` set, a coordinator
refuses calls from coordinators that do not send it, and clients must `AUTH` with it. Without `secret`
anyone who reaches the port can read the topology and send votes: always set it, and keep the port on a
private network. The secret travels in clear text; use a private network or a tunnel between machines.

## A multi-node cluster

Several groups can share the hash slots, each group on its own machines (see `cluster_topology` in
[CONFIGURATION.md](CONFIGURATION.md)). Give each group its `shards` and `slots` in `coordinator.toml`:

```toml
[[groups]]
name = "a"
shards = 2
slots = ["0-8191"]
members = [ { address = "a1:6379", replication = "a1:26379" }, { address = "a2:6379", replication = "a2:26379" } ]

[[groups]]
name = "b"
shards = 2
slots = ["8192-16383"]
members = [ { address = "b1:6379", replication = "b1:26379" }, { address = "b2:6379", replication = "b2:26379" } ]
```

Either every group has `shards` or none does, every slot belongs to one group, and a group may
have no slots yet (`slots = []`). Start every data node
with a `cluster_topology` file holding the same groups and slots (version 1, each group named by its
first member) and its own `cluster_group`. Once every group has an agreed primary the coordinators send
each member the map with `RUVIO TOPOLOGY SET`, each group named by its agreed primary; its version is 1
plus the sum of the groups' epochs plus the ranges moved between groups, so every failover and every move
raises it. A node keeps the newest map it received
in its data directory (`TOPOLOGY`) and starts from the newer of that file and `cluster_topology`. After
a failover in one group, the other groups' nodes answer that group's keys with `MOVED` naming the new
primary within a ping or two. A node refuses a map that gives its group a slot whose data is not there
(see *Moving slots between groups*). The data-node user in `auth` also needs `RUVIO` for this; a member that refuses
the map is logged at most once a minute. The version only counts failovers and moves, so an edit of the groups in
`coordinator.toml` (an address, the order) does not reach nodes that already serve that version; this
version of the coordinator does not handle such edits.

## Moving slots between groups

`COORDINATOR MOVE first last group shard` (on the leader, after `AUTH secret`) moves the slots
`first..=last` to shard `shard` of `group` while both groups keep serving them, and answers the move's
id. Every slot of the range must be on one shard of one group; a group may start with `slots = []` and
receive ranges this way. One move runs at a time.

1. The leader records the move (Raft), then tells the target group's primary to expect the range on that
   shard (`RUVIO MIGRATE IMPORT`) and the source group's primary to send it (`RUVIO MIGRATE EXPORT`).
2. The source copies the range to the target shard over its client port, in batches, then the keys
   written meanwhile, until few are left. Clients keep using the source.
3. The source freezes the range: its commands get `TRYAGAIN`, which every Ruvio client retries after 5,
   10 and 20 ms. It sends the last changes and the target syncs them to its log. The source then keeps
   the frozen range on disk (`MIGRATE_OUT`), so a restart cannot serve it again on its own.
4. The leader waits until the target's connected replicas hold the prepared copy (at most 10 s, or the
   move is cancelled and started again), commits the move only if neither group failed over since it
   began, and sends the new map to the target's primary, then the source's, then
   every other member. The target serves the range; the source answers `MOVED` naming the target and
   drops its copy in bounded steps. Each range of the new map names its shard (`"100-199@0"`), so the
   other ranges stay on the shards that hold their data.

Replicas of both groups follow the move from their primary's log. `COORDINATOR STATUS` reports the
running move, or the last one this leader drove, with the number of moved ranges; `COORDINATOR CANCEL`
ends the running move before its commit, and the source serves the range again (the end is recorded
in Raft before the nodes are told, so a late cancel never undoes a committed move). Send cancels to the
coordinators, not with `RUVIO MIGRATE CANCEL` to the nodes. Before a failover the leader sends the
newest map to the replica it promotes. A failover of either
group before the commit cancels the move and starts it again with the new primaries (up to three
times); a new leader takes over a move its predecessor began. After the commit nothing undoes the move:
move the range back with another `COORDINATOR MOVE`. The copy rate of a source is capped by
`migration_max_bytes_per_sec` in its configuration (0, the default, means no cap); the frozen hand-over
is never slowed down. Nodes need durability `memory`, `everysec` or `always` (not `snapshot`).

## Limits of this version

- Groups are declared in the file; replicas are not discovered from the primary yet.
- The coordinators do not change membership at run time (adding a coordinator means restarting all of
  them with the new `peers`).
- No primary leases: an isolated primary is stopped by `min-replicas-to-write`, not by the coordinators.
- Ranges move only when an operator asks (`COORDINATOR MOVE`); nothing rebalances by load yet.

## Tests

- `tests/coordinator_live.rs`: three data nodes and three coordinators as real processes: failover after
  the primary is killed, the old primary returning as a replica, agreement kept across a restart of every
  coordinator, no failover without a majority, and the secret; and two groups of a multi-node cluster,
  where a failover in one changes the other's `MOVED` answers, and a range moves from one to the other
  while a client keeps writing a key of it (every acknowledged write is on the new owner).
- `tests/multi_node_live.rs`: the node side of a move driven by hand, and a prepared move kept across
  restarts of both nodes until it is cancelled.
- `scripts/coordinator_faults.py target/debug/ruvio [scenario ...]`: a TCP proxy between the coordinators
  and each data node; `partition` (primary cut from the coordinators only), `leader` (the coordinator
  leader killed, then the primary), `minority`, `restart`, `loss` (acknowledged writes lost under load),
  `client` (redis-py's Sentinel client; skipped when redis-py is not installed).
