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
   shards` and `INFO replication` (the synced and the committed sequence of every shard).
2. When the Raft leader among the coordinators has not heard from the primary for `down_after_ms`, it
   asks the others. The primary is down when a majority of coordinators (the leader included) have not
   heard from it within `down_after_ms`. One coordinator's own network trouble cannot start a failover.
3. The leader picks the replica that answers, is not still copying its primary (`LOADING`), and holds the
   most of the history (the largest sum of `wal_durable_sequence` over its shards); ties go to the member
   listed first. With `CLIENT DURABILITY` in use it must also hold every committed write (see below).
4. It records "primary = that member, epoch + 1" in the Raft log (a majority must accept it), sends the
   member `REPLICAOF NO ONE`, points the other members at it with `REPLICAOF host port`, and publishes
   `+switch-master` to subscribers.
5. A member that answers again and disagrees with the record is corrected: an old primary is made a
   replica of the new one. It continues from its own log when it wrote nothing after the new primary
   forked, and takes a full copy otherwise (see *Write history* in [RECOVERY.md](RECOVERY.md)).

If the leader coordinator dies, the remaining ones elect a new leader (one to two seconds) and carry on.
Commands to data nodes are repeated on every tick until the members agree with the record, so a failover
interrupted half way is finished by the next leader.

When the coordinators are partitioned from each other, only the side with a majority acts. The Raft
library keeps a leader that lost its majority believing it leads, so before each tick's work on the data
nodes (a failover, pointing members at the agreed primary, sending the cluster map, a step of a slot
move) and before `COORDINATOR FAILOVER group FORCE`, `MOVE` and `CANCEL`, the leader confirms with a
heartbeat that a majority of coordinators still follows it. One confirmation per tick is shared by
everything in that tick; it waits at most one `ping_interval_ms` (capped at 1 second, below the election
timeout). A leader cut off from the others, even one that still reaches every data node, therefore does
nothing to them: it logs one warning, those commands answer `ERR this leader cannot reach a majority of
coordinators; try again`, and it acts again (logging that once) when the majority answers it, or follows
the leader the majority elected meanwhile. The other side, if it is a majority, elects a new leader and
fails over as usual.

Measured on one Mac with three data nodes and three coordinators (`ping_interval_ms` 200,
`down_after_ms` 1000, debug build): the new primary took writes 1.2 seconds after the old one was killed;
with the coordinator leader killed first, 3.2–4 seconds.

## What can be lost, and split brain

Replication is asynchronous. Writes the old primary acknowledged but had not yet shipped are not on the
new primary. In a test that wrote continuously while the primary was killed, 0–1 of about 225
acknowledged writes were missing afterwards, always the last ones. A client that cannot lose a write
turns on `CLIENT DURABILITY REPLICAS 1` for its connection: every reply then waits until a replica
holds the writes before it on disk, and becomes `NOREPLICAS` if none does within the timeout (default
5000 ms; the writes stay applied). The client libraries have a connect option that sends it on every
connection. `WAIT 1 timeout_ms` after a write does the same for one write at a time.

A failover keeps such writes as long as the coordinators learned that they were committed (see
*What can still lose data* below for the shapes where they cannot). The primary records, per shard, the highest sequence it released a
durable reply for (`replication_committed_sequence` in `INFO`) and passes it to its replicas; every
coordinator reads it from the members' `INFO replication` on each ping and keeps the highest value it
has seen per group and shard. The leader promotes only a replica whose `wal_durable_sequence` is at
least that value in every shard, and among those the one holding the most. When no answering replica
qualifies, it does not fail over: it logs a warning on every tick and waits, and the group has no
writer meanwhile. Two ways out, both losing the records the promoted replica lacks (the leader logs
which, per shard):

- `unclean_failover = true` in `coordinator.toml` promotes the best replica anyway, every time.
- `COORDINATOR FAILOVER group FORCE` on the leader (after `AUTH secret`) does it once, now, and answers
  the promoted member's address. It is refused while the primary still answers the leader, and by other
  coordinators with `ERR not the leader; send it to coordinator <id>`.

After a failover to a replica that held every committed record, the values still apply, so the next
failover also waits for a replica that caught up. After an unclean one those records are gone and the
new primary reuses their sequence numbers: the old values are dropped, and the stale ones replicas keep
reporting are ignored until the new primary commits past them. Which of the two applies is decided
once, by the leader that promotes, and recorded in Raft with the promotion. The leader also writes
the values through Raft when they rise: at most once a second, one entry for all groups, only the
shards that rose. Every coordinator starts from that record, and a new leader acts only once it has
applied the log of its own term, so a new leader, or coordinators that all restarted while the
primary and the replica holding the records are down, still wait for a replica that has them. A value
that rose in the last second before the leader died may not be recorded yet; the other coordinators
also learn the values from the members on every ping. A recorded value that can never be reached
again (for example a primary restarted at the same epoch with durability `memory`, whose sequences
start over) stays until a failover with `FORCE` or `unclean_failover`, also across coordinator
restarts. Coordinators' `INFO` shows the recorded values as `recorded=` and the known ones as
`committed=` on the group's line. Groups whose clients never use `CLIENT DURABILITY` report 0
everywhere, write nothing, and fail over as before.

The coordinators learn committed values only from the members they reach: the primary, and the replicas
following it (the primary passes the value on to them). A write acknowledged with `CLIENT DURABILITY
REPLICAS k` is on at least `k` replicas, but when those replicas are cut off from the coordinators
together with the primary, the coordinators never see that value, count the replica they can reach as
complete, and promote it. With `REPLICAS` equal to the number of replicas in the group every replica
holds every acknowledged write, so whichever one is promoted has it; with fewer, an acknowledged write
can be lost in that shape (see the table below).

`IDEM`, `ONCE`, `LIMIT` and `LEASE` are ordinary writes in this respect: they survive a failover exactly
when an acknowledged durable write does, and the last ones can be lost without `CLIENT DURABILITY`.
Their deadlines are wall-clock times, so the nodes' clocks must agree. See "Across a failover" in
[IDEM.md](IDEM.md) for the details and a lock recipe.

### An old primary that keeps taking writes

A primary that does not learn it was replaced keeps taking writes, and they are lost when it rejoins as
a replica. It learns in three ways:

- A coordinator reaches it again and makes it a replica of the new primary (within a ping).
- The epoch: a replica that follows the new primary refuses the old one, and the old one stops taking
  writes as soon as such a replica connects to it.
- `min-replicas-to-write` with `min-replicas-max-lag` (seconds): the old primary refuses writes with
  `NOREPLICAS` once fewer replicas than the first value acknowledged within the second. It checks
  every 100 ms, so it refuses at most `min-replicas-max-lag` + 0.1 seconds after the last replica it
  needed stopped acknowledging.

How long the window lasts depends on which replicas the old primary still reaches:

- **Cut off from the coordinators only.** Its replicas still reach the coordinators and are pointed at
  the new primary, so they stop acknowledging the old one. With `min-replicas-to-write 1` it then
  refuses writes within the lag; in the partition test it refused 0.1 seconds after the failover. With
  the default `min-replicas-to-write 0` it keeps taking writes until a coordinator reaches it again.
- **Cut off together with one of its replicas**, while another replica stays with the coordinators.
  That replica is promoted, and the one that stayed with the old primary keeps acknowledging it, so
  `min-replicas-to-write 1` stays satisfied and the old primary takes writes for the whole partition.
  Only `min-replicas-to-write` equal to the number of replicas (every replica) bounds it: the old
  primary then refuses writes at most `min-replicas-max-lag` + 0.1 seconds after the cut. The
  promotion comes `down_after_ms` after the coordinators' last answered ping of the primary, which is
  normally less than one `ping_interval_ms` before the cut (longer when a ping to another member is
  slow). With the defaults (`min-replicas-max-lag` 10, `down_after_ms` 3000, `ping_interval_ms` 500)
  both primaries take writes for up to about 7.5 seconds. Keep the lag well below `down_after_ms`, for
  example `min-replicas-max-lag 2` with `down_after_ms 4000`, so the old primary stops first. The price:
  losing any one replica stops writes in the group.

Writes the old primary took in the window are lost in both shapes. Use `min-replicas-to-write 1` (or
higher) in every group a coordinator manages, and the number of replicas or the primary lease where a
split must stay bounded.

### Primary lease

With `primary_lease_ms` set on the coordinators and `primary-lease yes` on the data nodes, an old
primary stops taking writes before a replica is promoted, whatever replicas it still reaches. Both
are off by default.

- Every `ping_interval_ms`, each coordinator renews the lease of the member its own record names
  as the primary, each group on its own task and connection, so a slow or silent member elsewhere
  never delays it: `RUVIO LEASE BEGIN` (the node notes the instant on its own clock and answers a
  token), then `RUVIO LEASE GRANT token coordinator voters epoch lease_ms`. The lease runs
  `lease_ms` from the BEGIN instant, so a grant that arrives late only shortens it. A grant for an
  older epoch than a running grant the node holds is refused, a newer epoch drops the older grants,
  and a node that is a replica refuses grants. A grant that ended is forgotten and refuses nothing,
  so coordinators that start over at a lower epoch (a lost `data_dir`, or the Raft files deleted to
  go back a release) are taken at most one lease later. The majority is counted against the largest
  number of coordinators (`voters`, at most 255) that a running grant of the current epoch names,
  each coordinator's latest grant setting its count; a new epoch starts the count again.
- A writer (a primary, or a node with no replication role) takes writes only while grants from a
  majority of the coordinators run for more than 300 ms more. It checks this on the same 100 ms
  tick as `min-replicas-to-write`, so nothing is added to the commands themselves; otherwise
  writes get `NOREPLICAS Primary lease expired: too few coordinators renewed it.` (the code
  clients already handle as `NOREPLICAS`). A primary starts refusing, and so does a promoted
  replica, until the first grants arrive (one ping after its promotion). The 300 ms cover the
  writes taken between two checks and a stall of the node of up to about 200 ms.
- When the leader no longer hears from a primary, it asks every coordinator two questions: does
  it hear the primary, and has its last grant to it aged past the lease stretched for clock drift,
  times (100 + d) / (100 - d) with d the `primary_lease_drift_percent` (default 10), plus 200 ms
  (2645 ms for a 2000 ms lease), by that coordinator's own settings. A coordinator that answers "ended" promises not to grant that
  member again until the leader confirms that its record still names it; the leader answers
  only after any failover it is deciding is recorded, and only once a majority of coordinators
  confirmed it still leads. A failover goes ahead when a majority no longer hears the primary
  and a majority reports its lease ended. So a coordinator that reported the lease ended renews
  it only for a primary the record still names after that, and any majority that granted the old
  primary shares a coordinator with the majority that reported it ended. `COORDINATOR FAILOVER
  group FORCE` waits for this too: it is refused while a majority still hears the primary and
  grants leases, or while its lease runs.
- A coordinator that just started reports every lease as granted at its start, and grants
  nothing until the leader confirmed the primary.
- `primary_lease_ms` must be at least three times `ping_interval_ms`, at least `ping_interval_ms`
  plus 500, and at most 60000, and the stretched lease plus 200 ms must fit in `down_after_ms`, so
  detection is not delayed: with the defaults (`ping_interval_ms` 500, `down_after_ms` 3000),
  `primary_lease_ms = 2000`. `primary_lease_drift_percent` must be below 100, and at most 255
  coordinators can grant a lease.

What it assumes and what it does not cover:

- Every coordinator's and data node's clock runs at most `primary_lease_drift_percent` faster or
  slower than real time. The stretch covers the worst pair: a node clock that runs slow by d holds
  the lease for up to `lease / (1 - d)` of real time, which a coordinator clock that runs fast by d
  measures as `lease * (1 + d) / (1 - d)`. The clocks need not agree on the time. On Linux a data node measures
  the lease with `CLOCK_BOOTTIME` and the coordinators with the monotonic clock, so a data node
  host that is suspended (laptop sleep, VM suspend) counts the suspended time and wakes with its
  lease expired; on other platforms the node's clock may stop while suspended.
- Writes the old primary takes between the cut and the end of its lease (at most `primary_lease_ms`
  + 0.1 s) are still lost when it rejoins. The lease bounds the window; only `CLIENT DURABILITY
  REPLICAS` equal to the number of replicas keeps acknowledged writes in every shape below.
- A primary whose process stalls (paused VM, swapping, a long `SIGSTOP`) for longer than the 300 ms
  margin, with the stall starting just after a check that found the lease running, may apply writes
  on resuming before its next check closes the gate. Each shard checks on its 100 ms maintenance
  tick, which is due at once after such a stall, but the connections' tasks may run first: the
  writes already waiting on its connections, and those that arrive before the check runs, can be
  applied. This is normally one round of reads, but it has no fixed bound in time or count.
- When a majority of the coordinators are down or cannot reach the primary, every primary stops
  taking writes after its lease: a coordinator outage becomes a write outage.
- Anyone with `ruvio|lease` on a data node can send `RUVIO LEASE BEGIN`: the node keeps the last 64
  BEGIN tokens of all connections together, so a flood of them makes the coordinators' grants
  arrive with unknown tokens and lets the lease run out. Give it only to the coordinators' user.

Turning it on:

1. Upgrade the data nodes (they accept `RUVIO LEASE` and ignore grants while `primary-lease` is off).
   The coordinators' data-node user needs `RUVIO` (`ruvio|lease`).
2. Upgrade all coordinators together (see *Upgrading*), then set `primary_lease_ms` on each and
   restart them one at a time. Mixed versions do not work with the lease: a coordinator of an older
   version neither grants nor answers the new lease query, so a failover waits for a majority that
   does (it never promotes on a missing answer) and its missing grants count against the majority;
   and an older leader cannot decode the confirmation a newer coordinator asks before it grants, so
   that coordinator grants nothing. Coordinators with the lease off still ask the others and wait for
   a lease any of them grants.
3. Last, the data nodes enforce it. Check `INFO replication` on the primary: `primary_lease_grants`
   should equal the number of coordinators and `primary_lease_remaining_ms` stay well above 300.
   Then `CONFIG SET primary-lease yes` on every data node (and `primary_lease = true` in their files,
   or `RUVIO_PRIMARY_LEASE=yes`).

Done the other way round, the primaries refuse writes until the coordinators grant them.

Turning it off: first `CONFIG SET primary-lease no` on every data node (and in their files), then set
`primary_lease_ms = 0` on the coordinators, one at a time. In the other order the primary stops
writing once a majority stopped granting.

Changing `primary_lease_ms` or `primary_lease_drift_percent`: each coordinator reports a lease as
ended by its own settings. To lengthen the lease (or raise the drift), change it on every
coordinator; to shorten it (or lower the drift), restart the coordinators one at a time and wait the
old lease length after each restart, so no coordinator reports a lease ended by the new, shorter
length while a grant of the old length runs.

Changing the number of coordinators (all restarted with the new `peers`, see *Limits of this
version*): each grant names its sender's coordinator count, and a primary needs a majority of the
largest count among its running grants. Adding coordinators raises the bar at once. After removing
some, the grants of the larger count keep the higher bar until they end, at most `primary_lease_ms`
after the last one was sent; until then the primary refuses writes unless a majority of the old count
still grants it. To avoid that pause, turn `primary-lease` off on the data nodes for the change.

Checked with Docker (debug build, `ping_interval_ms` 500, `down_after_ms` 3000, `primary_lease_ms`
2000, `min-replicas-to-write 1` on every node), the old primary and one replica on one network, the
other replica and the coordinators on another, a client writing to the old primary throughout. Without
the lease the old primary took writes for the whole partition and about 475 acknowledged writes were
lost when it rejoined. With it, the old primary refused writes about 1.2 to 1.3 seconds after the
second coordinator lost it, the replica was promoted 1.3 to 1.8 seconds after that, and the writes
taken before the refusal (70 to 80) were lost.

### What can still lose data

`k` is the `CLIENT DURABILITY REPLICAS` value of the writing connection, `r` the number of replicas in
the group, `L` the `primary_lease_ms`.

| Failure | Without the lease | With the primary lease | Writes with `CLIENT DURABILITY REPLICAS k` | What the operator can do |
| --- | --- | --- | --- | --- |
| Primary process or machine dies | The last writes it had not shipped are lost (0–1 of about 225 in the test above) | Same; the failover also waits until the lease counts as ended, which fits in `down_after_ms` | None lost while a replica holding them answers: the coordinators learn the committed value from the primary or from the replicas, and promote only a replica holding it; otherwise the group waits | Use `k` ≥ 1 where a lost write matters; leave `unclean_failover` off |
| Primary cut off from the coordinators only, its replicas still reach the coordinators | Writes the old primary takes until it learns it was replaced are lost: within `min-replicas-max-lag` + 0.1 s with `min-replicas-to-write` ≥ 1, until the partition heals with 0 | It stops at most `L` + 0.1 s after its last grant, before the promotion; the writes it took until then are lost | None lost with `k = r`. With smaller `k`, a write acknowledged between the promotion and the moment the replicas are pointed at the new primary can be lost (with the lease the old primary has stopped by then) | `min-replicas-to-write 1` or more, or the lease; `k = r` for writes that must not be lost |
| Primary and one replica cut off together from the other replicas and the coordinators | The old primary takes writes for the whole partition unless `min-replicas-to-write` = `r`; with `r` it stops within `min-replicas-max-lag` + 0.1 s. Everything it took is lost | It stops at most `L` + 0.1 s after its last grant, before the promotion; the writes it took until then are lost | With `k < r` acknowledged writes can be lost (with the lease, only those taken before it stopped): the replica that stayed acknowledges them, the coordinators never learn the committed value, and the promoted replica lacks them. With `k = r` none: the old primary cannot get a reply released without the cut-off replica | The lease; or `min-replicas-to-write` = `r` with `min-replicas-max-lag` well below `down_after_ms` (for example 2 with `down_after_ms` 4000); `k = r` |
| A majority of coordinators down, or cut off from the primary | Nothing is lost by it; data nodes keep serving, but a primary that fails meanwhile is not replaced | Every primary they cannot renew stops taking writes after `L`; nothing is lost | Same as without the lease | Run three or five coordinators on different machines; without the lease, promote by hand with `REPLICAOF NO ONE` while none runs; with it, `CONFIG SET primary-lease no` on the primary to write without coordinators |
| The primary's process or VM paused longer than `down_after_ms` | A replica is promoted; when the old primary resumes it may take writes until it learns it was replaced (as in the rows above), and they are lost | The promotion waits for the lease; on resuming, the old primary may apply writes already waiting before its next check, and those writes are lost; a stall shorter than 300 ms loses nothing | None lost with `k = r`: the replicas follow the new primary and do not acknowledge the old one | `min-replicas-to-write` ≥ 1, or the lease |
| Clocks running at different rates | Timeouts are measured with each process's own monotonic clock, so a rate difference only shifts when a failover starts | The lease is safe while every clock runs within `primary_lease_drift_percent` of real time; beyond it both primaries can take writes for a moment | Same as without the lease | Keep clocks synchronized (NTP), also for the wall-clock deadlines of `IDEM`, `ONCE`, `LIMIT`, `LEASE` and `SEMAPHORE` |
| Coordinators of different versions | A newer leader falls back to the older query for coordinators that do not answer the lease query, so failovers work as before | Older coordinators neither grant nor report grants: failovers wait for a majority of the new version, and writes stop if fewer than a majority grant | Same as without the lease | Upgrade every coordinator before turning the lease on |

A `CHANGES SUBSCRIBE` consumer may already have seen records a failover takes back. Its cursor
carries the generation (the replication epoch) of the log it read, so when it resumes on the new
primary with a cursor past the fork, the stream first sends `["rewound", cursor]` with the fork in
the new generation: undo or reload what was applied after it. A consumer that subscribes with
`COMMITTED` gets only records a `CLIENT DURABILITY` reply committed, which a coordinator's failover
keeps (unless it was forced, `unclean_failover` is set, or the commit happened where the coordinators
could not see it, as in the table above), so it does not see one taken back; it sees nothing until durable writes commit. See
[CONFIGURATION.md](CONFIGURATION.md), *Change stream*.

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
unclean_failover = false                # default false: wait for a replica with every committed write
primary_lease_ms = 0                    # default 0 (off); 2000 with the defaults above, see "Primary lease"
primary_lease_drift_percent = 10        # default 10: how much faster or slower than real time a clock may run; below 100
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
- The data-node user in `auth` needs `ROLE`, `INFO`, `CONFIG` (`GET`) and `REPLICAOF`, and `RUVIO` with
  `primary_lease_ms`.
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
| `INFO` | `coordinator_id`, `leader_id`, `is_leader`, and `group_<name>:primary=…,epoch=…` (with `,recorded=…` (the Raft record) and `,committed=…` (what this coordinator knows) per shard, joined by `/`, once a durable reply was released) |
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

## Upgrading

Upgrade all coordinators together: stop every one, replace the binary, start them again. A release
that records committed sequences in Raft writes log entries older coordinators cannot read; an older
coordinator that receives one cannot follow the log, and one restarted on such a log refuses to start.
The newer release reads the log, vote and snapshot an older one wrote. Data nodes are not affected,
except that with `primary-lease yes` on them, writes stop once the lease runs out while fewer than a
majority of coordinators run; turn it off on the nodes first (see *Primary lease*) if that matters.

A rolling upgrade stalls: once a coordinator of the newer release leads and records a committed
sequence or a failover, the older ones cannot take the entry, and while fewer than a majority run the
newer release nothing more is agreed (no failover, no move) until the rest are upgraded.

To go back to an older release, roll forward if at all possible. Otherwise, only for groups without a
cluster map (no `shards`/`slots`, no moved ranges): stop every coordinator, delete `raft-log.json`,
`raft-vote.json` and `raft-snapshot.json` from each one's `data_dir`, and start the older release
while every group has exactly one member answering as primary. The coordinators then start as on a
first start: they record that member at epoch 1 and lose the committed sequences (they learn them again
from the members). With a cluster map do not do this: the map's version and the moved ranges are in
the same state, and starting it over sends nodes an older map than they hold.

## Limits of this version

- Groups are declared in the file; replicas are not discovered from the primary yet.
- The coordinators do not change membership at run time (adding a coordinator means restarting all of
  them with the new `peers`).
- Ranges move only when an operator asks (`COORDINATOR MOVE`); nothing rebalances by load yet.

## Tests

- `tests/coordinator_live.rs`: three data nodes and three coordinators as real processes: failover after
  the primary is killed, the old primary returning as a replica, agreement kept across a restart of every
  coordinator, no failover without a majority, a leader cut off from the other coordinators that
  neither fails over nor corrects a member while it still reaches the data nodes, and the secret; the
  committed sequences kept across a restart of every coordinator while the primary and the replica
  holding them are down (only `FORCE` promotes the other replica); and two groups of a multi-node cluster,
  where a failover in one changes the other's `MOVED` answers, and a range moves from one to the other
  while a client keeps writing a key of it (every acknowledged write is on the new owner). A primary
  cut off from the coordinators by a proxy while a client keeps writing to it: with the lease it
  takes no write once a replica is promoted, without it it keeps taking them; and a member that
  accepts connections but never answers does not hold up the primary's lease.
- `tests/multi_node_live.rs`: the node side of a move driven by hand, and a prepared move kept across
  restarts of both nodes until it is cancelled.
- `scripts/coordinator_faults.py target/debug/ruvio [scenario ...]`: a TCP proxy between the coordinators
  and each data node; `partition` (primary cut from the coordinators only), `leader` (the coordinator
  leader killed, then the primary), `minority`, `restart`, `loss` (acknowledged writes lost under load),
  `client` (redis-py's Sentinel client; skipped when redis-py is not installed).
