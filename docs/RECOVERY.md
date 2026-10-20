# Recovery, Backup, and Integrity

Ruvio stores durable state under a single data directory. Recovery is
conservative: the server becomes ready only after every shard has a valid
checkpoint (when one exists) and a continuous WAL replay. Inspection and backup
use the same validators as startup, but they never mutate WAL files.

Frozen v1
bytes live in `tests/fixtures/persistence/v1/`.

## Recovery-Point Objective

The recovery point is determined by the configured durability profile. These
are contractual bounds, not measured SLAs on a particular disk.

| Profile | Acknowledged writes that may be lost | Typical window |
| --- | --- | --- |
| `memory` | All in-memory state | Entire process lifetime |
| `snapshot` | Writes after the last durable `CURRENT` | Last checkpoint interval, plus in-flight `SAVE` |
| `everysec` | Writes accepted by the kernel but not yet synchronized | One `sync_interval_ms` / `RUVIO_WAL_SYNC_INTERVAL_MS` / `--wal-sync-interval-ms` (default 1000). `60000` is about one minute. Storage or scheduler stalls can widen the window. |
| `always` | Only a write that was never acknowledged | Torn final WAL record. Acknowledged writes must survive process and OS restart when the filesystem honors the documented sync semantics. |

A lost connection around a write is an unknown outcome. Replay applies a WAL
record that reached the acknowledgement boundary even if the client never saw
the reply.

Blocked list waiters, output buffers, and in-flight client sessions are not
persisted.

Startup cuts a damaged tail of the last WAL segment (a partial record, a bad
final checksum, or a zeroed record length followed by leftover bytes) and logs
how many bytes it dropped. It cuts only when no intact record continuing the
log follows the damage. If one does, the damage is in the middle of synced
records, so startup stops with an error naming the segment, the damaged offset
and the offset of the intact record, instead of dropping them silently. To start
anyway without the records from the damaged offset on, move the segment aside
or truncate it at that offset (`truncate -s <offset> <segment>`), after taking a
copy. Damage in an earlier segment always stops startup.

When an `always` batch's `fdatasync` fails, its clients get an error and the
shard refuses writes until a restart (see
[CONFIGURATION.md](CONFIGURATION.md)). The batch is also cut from the log and
is not sent to replicas, so the restart does not bring back writes that were
refused. This holds only for the restart after a crash or kill: the refused
writes were already applied in memory, and a graceful shutdown writes its final
checkpoint from memory, so they survive it. Two cases keep part of a refused
batch in the log: records before a segment rotation inside the batch (the
rotation synced them), and every batch synced by the sync worker, which leaves
the log as it is and lets startup replay what reached the disk.

Under `always`, scripts (`EVAL`, `EVALSHA`, `FCALL`) and `MULTI`/`EXEC` share a
connection's batch sync and behave like plain writes in it (except when one port
forwards commands between shards: there each one is synced on its own). When that sync
fails, their client is cut off without a reply and the shard refuses writes
until a restart, but what the script or transaction changed stays visible to
readers in memory until then, like any write of the refused batch. A failed
write of their record (not a failed sync) is different: it is refused before
the reply, the script or transaction is rolled back in memory, and its client
gets `MISCONF` on a connection that stays open. A transaction that runs
`FLUSHDB`, `FLUSHALL` or `SWAPDB` is synced on its own.

Under `everysec` they also join the connection's batch: their record is staged
and written together with the batch's other records (usually one `write` per
pipeline) before any of its replies leaves, so a reply still means the record
reached the kernel. With port forwarding, and for a transaction that runs
`FLUSHDB`, `FLUSHALL` or `SWAPDB`, each one is written on its own. Their record
is not written through, so a failed write of the batch does not roll them back:
it meets them like the plain writes of the batch. The client is cut off without
a reply, what the script or transaction changed stays visible to readers in
memory, and the shard keeps the record and writes it again once the log resumes
(the next write after the disk accepts writes again). A kill before that loses
it, as it loses the batch's plain writes; afterwards it is in the log like any
other record.

Any failed write (for lack of disk space or another error, not a failed
`fdatasync`) works this way for every record: the shard keeps the records it
could not write and writes them again once the disk accepts writes. Another client's batch whose records were already in the
file may get an error from the same failure, yet those records can reappear
after recovery or a restart. Its outcome is unknown, like a connection lost
around a write; it is not lost.

A write whose WAL record would not fit in one segment (larger than
`wal_segment_bytes`, see [CONFIGURATION.md](CONFIGURATION.md)) is refused with
`ERR write batch exceeds wal_segment_bytes` before it is applied. It is not in
memory or in the log, so recovery has nothing to replay for it, and the shard
keeps taking writes.

Native `IDEM` does persist its pending/completed state, fingerprint, owner,
binary result and original absolute deadline. Each transition is one WAL opcode
53 record; a torn completion leaves the last valid pending record rather than a
partly completed value without TTL. Snapshots reuse ordinary strings and key
deadlines. Upgrade replicas and recovery tools before writing this opcode; older
readers reject it. Retention, eviction and the RPO above still apply and do not
provide exactly-once external side effects. See [IDEM.md](IDEM.md).

## Moved slot ranges

`RUVIO MOVESLOTS` records the slot ranges it moved between shards in the `SLOTS` file of
the data directory (written to a temporary file, synced and renamed, so a restart reads
either the old or the new file). Each entry names the range, the source and target shards,
the target's first log sequence of the copy and the source's first sequence after the
hand-over; an entry without the latter is a move that did not finish. Without the file every
shard owns the slots the formula gives it.

At start the slot table takes the finished moves, and each shard's recovery keeps, per slot,
only what the shard held while it owned the slot: copies of an unfinished move are skipped,
and keys of a range that left the shard are dropped after replay. The copy is logged on the
target as `SlotCopy` records (WAL format minor 4), and the target syncs its log before the
move is recorded, so a crash at any step leaves each key on exactly one shard. A damaged
`SLOTS` file, or one written for another shard count, stops startup instead of falling back
to the formula. Back the file up with the rest of the directory. A server with a `SLOTS` file
refuses to start with replication on.

### Ranges moving between groups

A multi-node cluster moves ranges between groups (`COORDINATOR MOVE`, see `docs/COORDINATOR.md`) with
the same file. The target shard logs a `SlotsIncoming` record when a range begins to arrive, its copies
as `SlotCopy` records, and a second `SlotsIncoming` record once the whole copy is synced (prepared);
the `SLOTS` file names the range as gained from another node. A restart before the prepared record
skips the copies. A prepared range is kept, not served, until a cluster map gives it to the group,
which completes it (a `SlotsArrived` record, WAL format minor 6, so replicas complete it from the
log too), or `RUVIO MIGRATE CANCEL` drops it (a third `SlotsIncoming` record). The source logs a `SlotsGiven`
record when a newer map gave the range away; its copy is dropped after replay. Both records are WAL
format minor 5, and replicas follow both sides from them. While the map has not decided, each side
also keeps the move in a small file of its data directory (`MIGRATE_IN` on the target, `MIGRATE_OUT`
on the source): after a restart the source keeps the range frozen (`TRYAGAIN`) and both still accept
`RUVIO MIGRATE CANCEL` with the move's id. A replica restarted with such a history keeps it with
replication on.

## Recovery-Time Objective

Startup time is the time to acquire `LOCK`, validate `CURRENT` and every
referenced snapshot, load the `CLOCK` floor, and replay WAL records strictly
above each shard's snapshot sequence.

There is no published numerical RTO until dedicated hardware soak and
fault-injection gates complete. Operators bound restart time by checkpointing
regularly so WAL replay stays short. `SAVE`, `BGSAVE`, and
`snapshot_interval_ms` publish a durable generation and then delete WAL
segments covered by that generation.

## Read-Only Inspection

`ruvio-ctl check` and `ruvio-ctl inspect` validate a data directory without
taking `LOCK` and without truncating a torn final WAL record. They report:

- the persisted clock floor, if `CLOCK` exists
- the committed checkpoint generation
- per-shard snapshot sequence, replayed WAL records, torn-tail bytes, and the
  number of live keys after snapshot restore plus replay

Mid-log corruption, sequence gaps, digest mismatches, and unsupported versions
fail the command. A torn tail on the last segment is reported and left
untouched. Only the server's writable recovery path truncates that tail.

```bash
ruvio-ctl check --data-dir ./data
ruvio-ctl inspect --data-dir ./data
```

## Backup

`ruvio-ctl backup` takes an exclusive `LOCK` on the source directory, then
copies `CURRENT`, `CLOCK`, `checkpoints/`, `wal/`, and any per-shard
`REPLICA_CATCHUP_####` markers. It does not copy `LOCK`. The destination must
be empty. A backup of an incomplete replica retains its promotion guard on
restore; `check` validates its persistence files but does not certify that its
replication catch-up finished.

```bash
ruvio-ctl backup --data-dir ./data --dest ./backup --signer ada --sign-key ./backup.key
```

The copy writes `SIGNATURE` next to the persistence files. It names the signer
and stores a blake3 digest of every copied file. `--sign-key`, or
`RUVIO_BACKUP_KEY` when the flag is absent, adds a keyed digest over that
list. A snapshot checksum can be rebuilt by anyone who rewrites the bytes.
The keyed digest cannot. `restore` and `check` recompute the digests. A
signed backup refuses to restore until the same key is passed. A backup
written without a key still records the signer and the digests. `--signer`
is one word and defaults to `USER`.

The server does not read `SIGNATURE`. It is not copied into the restored data
directory.

Stop or quiesce writers when you need a backup that matches a specific
acknowledged sequence. A live `always` or `everysec` backup can still include a
torn tail; `check` reports those bytes instead of repairing them.

## Restore

`ruvio-ctl restore` validates the source, locks the destination, and copies the
same persistence files. It refuses a destination that already contains
`CURRENT`, `CLOCK`, `wal/`, `checkpoints/`, or a replica catch-up marker unless
`--force` is passed.

```bash
ruvio-ctl restore --source ./backup --data-dir ./data
ruvio-ctl restore --source ./backup --data-dir ./data --force
```

Do not restore into a directory owned by a running server. After restore, start
`ruvio` with the same durability profile and shard count that produced the
backup. The server acquires `LOCK` and runs the conservative startup recovery
path.

## Upgrade

An upgrade keeps the data directory and replaces the `ruvio` binary. Shard
count, durability profile, and format major must stay the same. A data directory whose
shards moved slots (`RUVIO MOVESLOTS`) has WAL format minor 4 records and a `SLOTS` file, so
it cannot go back to a release before slot moves. One that took part in a move between groups has
format minor 5 records and cannot go back to a release before such moves; one that received a range
from another group after an upgrade to a release with the `SlotsArrived` record (format minor 6)
cannot go back to a release before that record: the older release refuses the newer log segment at
start. To go back, restore the backup taken before the upgrade (step 2 below), or give the node a
full copy from a primary of the older release with an empty data directory. Changing
`shards` is a new directory, not an in-place upgrade.

1. Stop `ruvio`. `backup` takes an exclusive `LOCK` and fails immediately if the
   server still holds it.
2. `ruvio-ctl backup --data-dir /var/lib/ruvio --dest /var/backups/ruvio-<version>`.
3. `ruvio-ctl check --data-dir /var/backups/ruvio-<version>`.
4. Install the new binary. Package versions in one format major read the files
   the previous release wrote. See `docs/COMPATIBILITY.md`.
5. Start `ruvio` with the same `--config`. The process binds only after
   checkpoint validation and WAL replay.
6. `redis-cli PING`, then `INFO server` and confirm `ruvio_readiness:ready`
   and a known key.

With replication, a primary and its replicas must run the same release when the
replication handshake changed between the two (the release notes say so); see
*Promote a replica by hand* below. To upgrade such a group without losing
acknowledged writes, either stop every member and run the steps above on each
(primary first when starting again), or switch over to upgraded replicas: stop
writes on the primary (`CONFIG SET min-replicas-to-write` above its replica count),
wait until every replica holds what the primary has (steps 2 and 3 of *Promote a
replica by hand*), upgrade and restart the replicas, stop the primary, promote one replica
(`REPLICAOF NO ONE` on it, `REPLICAOF host port` on the others), and start the old
primary with the new binary as a replica. Under `ruvio coordinator` the coordinators
do the promotion; see [COORDINATOR.md](COORDINATOR.md#data-nodes), which also
covers the order for the coordinators themselves.

Host and cluster examples live in `deploy/`. Give the process one core per
shard. Set `LimitNOFILE` above `max_global` (the example uses 65535). Size the
volume for the data set plus WAL until the next checkpoint. `INFO prometheus` is the
metrics read. It is not a readiness probe. Readiness
is the RESP port opening, because the listen happens after recovery.
`INFO server` reports `ruvio_health:ok` and `ruvio_readiness:ready` once that
listen has started.

## Promote a replica by hand

To have this done automatically when a primary stops answering, run coordinators; see
[COORDINATOR.md](COORDINATOR.md). The steps below are what they automate.

The process does not elect a primary. You stop writers, wait until the replica's disk matches the primary's durable WAL, then make the replica the writer: with `REPLICAOF NO ONE` while it runs, or by restarting its data directory as the writer. Either way every shard is promoted. Shard `i` is the RESP port plus `i`, and the same offset on the replication port.

`redis-cli -p <port> INFO` prints a `# Replication` block:

| Field | Meaning |
| --- | --- |
| `replication_role` | `off`, `primary`, or `replica` |
| `wal_last_sequence` | Last WAL record this process appended |
| `wal_durable_sequence` | Last WAL record this process synced to disk |
| `replication_replicas` | Replica sockets connected to this primary. `0` on a replica |
| `replication_replica_sequence` | Oldest sequence this primary has written to a connected replica socket |
| `replication_send_lag` | `wal_durable_sequence` minus that oldest send |
| `slave0`, `slave1`, ... | One line per replica that announced its client address and finished copying: `ip`, `port`, `state=online`, `offset` (acknowledged sequence), `lag` (seconds since its last acknowledgement) |

`replication_send_lag` `0` means the bytes left the primary. The copy you can serve is the replica's `wal_durable_sequence`.

1. Stop clients from writing to the primary. A replica answers `READONLY` to a write. Clients with a replica read policy may keep reading from replicas meanwhile; those reads can miss the last writes.
2. On each primary shard, wait until `wal_last_sequence` equals `wal_durable_sequence`. With `everysec` this is the sync interval. With `always` they already match after an acknowledged write.
3. On each replica shard, wait until `wal_durable_sequence` equals that same number. `replication_send_lag` `0` on the primary is not this check.
4. If the replica's replication stream stopped because a WAL prefix was missing and no checkpoint covered it, do not treat it as complete. Its memory is the generation it had when the stream stopped. An interrupted snapshot/WAL catch-up leaves a durable per-shard `REPLICA_CATCHUP_0000` (and corresponding shard-numbered) marker. Reads return `LOADING`, then `MASTERDOWN` after 30 seconds if recovery stalls; neither response exposes partial data. While still configured as a replica, the process retries a fresh snapshot when the primary returns. Do not remove these marker files manually.
5. **Without a restart:** send `REPLICAOF NO ONE` to the replica. Every shard applies what it already received, forks a new history one epoch up and takes writes; with `replication.listen` set it also ships to replicas. A shard that has not finished copying (a catch-up marker) refuses it; `REPLICAOF NO ONE FORCE` promotes it anyway, with the same meaning as `--force-promote-incomplete` below. A replica whose every shard is still copying answers `LOADING` to every command, so it is promoted with a restart. The role lasts until the process restarts: change `replication.role` in the config file too. Then go to step 7.

   **With a restart:** stop the replica process and keep its data directory.
6. Start that directory as the new writer. One process, no further replicas:

```toml
[replication]
role = "off"
```

   A new primary that should accept replicas:

```toml
[replication]
role = "primary"
listen = "127.0.0.1:26379"
```

   Keep `profile` as `everysec` or `always`, and keep `shards` the same. `memory` and `snapshot` cannot replicate. If **any** shard still has a catch-up marker, a normal writer start fails. If you explicitly prefer access to the recoverable, possibly incomplete data over waiting for a complete replica, record the chosen replica's per-shard sequences, **fence/stop the former primary**, change the role to `off` or `primary`, then start the writer once with:

```bash
ruvio --config ./ruvio.toml --force-promote-incomplete
```

   The flag is CLI-only, not a config or environment setting, and is not saved to the config file. It bypasses the completion gate, **not** WAL/checkpoint corruption checks. The marker remains on disk: a later writer restart needs another explicit force decision. Force does not restore missing writes; keys, JSON documents, or transactions not copied before the failure may be absent. An incomplete replica cannot use this flag while its role is still `replica`.
7. Point clients at the new process's RESP address. Point the other replicas at the new primary's replication address, with `REPLICAOF host port` or a restart with the new `replication.primary`: each continues from its own log when it stopped at or before the point where the new primary took over (see *Write history*), and takes a full copy otherwise. `ROLE` on each shard shows `master` or `slave` and the address it follows.
8. Leave the old primary stopped. Starting it again next to the new writer forks the history. A replica that already follows the new primary refuses the old one (its epoch is lower), and the old primary refuses such a replica. Take a backup of the old directory if you still need it, then retire it.

### Write history

A primary creates a random write-history id (`HISTORY` in its data directory) the
first time it starts. Each replica shard remembers the id of the primary it follows
(`UPSTREAM_0000`, `UPSTREAM_0001`, ... one file per shard) and sends it when it asks
to resume from its own log. A shard records the id only once its log really
continues that history: when the primary accepts a resume, or after a snapshot
from it has been installed. A crash while a snapshot downloads leaves the old
record, so the next start takes a full copy again. If the primary now has a
different id, it refuses the resume and the replica takes a full copy instead of
merging two unrelated logs. A replica and primary must also run the same shard
count.

The id changes when the primary's history does:

- A wiped data directory (including `HISTORY`) starts a new id.
- `ruvio-ctl restore` removes `HISTORY` and every `UPSTREAM*` and `LINEAGE*` file from the
  destination (backups never contain them), so a restored directory starts a new
  id on its first start and its former replicas take a full copy. This matters
  because a restored primary reuses sequence numbers its replicas may already
  hold: with the old id, a replica that is ahead would be refused only until the
  primary wrote past it, then resume and mix the two logs.
- If you restore by copying files yourself, delete `HISTORY`, `UPSTREAM*` and
  `LINEAGE*` in the restored directory before starting it.

A directory written by an older build has one shared `UPSTREAM` file. Shards that
have no file of their own then claim no history, so each takes one full copy and
records its own id.

#### Promotion keeps the replicas' logs

A writer shard keeps its lineage in `LINEAGE_0000`, `LINEAGE_0001`, ... (one file
per shard): its history id, the history it forked from, the sequence where it
forked, and an epoch. A shard whose last role was replica (it has an `UPSTREAM_####`
record and no lineage) is being promoted. On that first start as a writer it takes
a new id, records the history it followed as its parent, its last logged sequence
as the fork, and the followed primary's epoch plus one. The file is written before
the shard accepts a write and is not changed later; following a primary again
removes it.

A promoted primary lets a replica resume from its own log when the replica followed
the parent history and stopped at or before the fork. A replica past the fork holds
writes from the old primary that the new one never saw, so it takes a full copy.
Without this every other replica took a full copy after a promotion.

The epoch only grows. Replicas send the highest epoch they have followed, and the
primary sends its own:

- A primary refuses a replica that has followed a higher epoch (`Stale`): the
  replica already follows a primary that replaced this one.
- A replica does not follow a primary with a lower epoch than the one it followed.

A shard whose last role was replica forks one epoch up whenever it starts as a
writer, also when that start was only a restart with a primary's configuration
and not a promotion. Pointed at the real primary afterwards with plain
`REPLICAOF host port`, it would claim that newer epoch and fence the real
primary. `REPLICAOF host port YIELD` (a Ruvio extension, what `ruvio coordinator`
sends) makes every shard claim no epoch to the primary it is pointed at: it
follows that primary whatever epoch it wrote or followed, and takes a full copy
when its log does not continue the primary's history. The epoch recorded on disk
is not lowered, so a restart before the primary accepts the shard forks above it
as before. `YIELD` trusts that the named primary is the current one: pointed at
an old primary, the shard drops the newer writes it held and does not fence it.
Plain `REPLICAOF` keeps the fencing above.

`INFO` does not show the lineage yet. A shard of a server that never replicated
(role `off`, no `UPSTREAM_####`) writes no lineage file.

Primary and replicas must run the same Ruvio version. The replication handshake
carries a version; a primary refuses any other version with a `Version` status and
logs a warning naming both versions, and a replica logs a warning when the primary
refuses it or does not answer its handshake (an older primary cannot parse it and
closes the link). Upgrade the primary and its replicas together (see *Upgrade*).

`REPLICAOF host port` makes a running primary or replica follow another primary
without a restart: writes answer `READONLY` at once, and every shard resumes from
its own log or takes a full copy by the rules above. A former primary that wrote
past the point where its replacement forked loses those writes. `REPLICAOF` lasts
until the process restarts; the config file's role applies again after that.

`min-replicas-to-write` limits how long a primary cut off from its replicas keeps
taking writes (see [CONFIGURATION.md](CONFIGURATION.md)).
A client can block with `WAIT replicas milliseconds` until that many replicas
have synced this shard's `wal_last_sequence` (in `port_mode = "single"` every shard's; in per-shard mode also the shards this connection's writes were forwarded to). The integer reply is the number
that reached it. Timeout `0` waits until the process shuts down.

## Operator Checklist

1. Confirm the durability profile matches the recovery point you need.
2. Keep checkpoints frequent enough that WAL replay time is acceptable.
3. Run `ruvio-ctl check` on backups before you need them.
4. Restore to a stopped data directory, then start the server and verify a
   known key.
5. Treat full-disk, permission, and process-kill drills as hardware gates; file
   corruption and torn-tail cases are covered by automated tests.
