# Advanced Resilience Tests

This page describes what Ruvio's crash, restart and disk-fault testing covers, what it
checks, how the checker itself is tested, and what it has found. It is an overview. For the
commands that run these tests and the full test map, see [TESTING.md](TESTING.md).

## How it works

`scripts/model_check.py` is a model-based randomized checker. It starts the real `ruvio`
binary and drives it like this:

- Several client connections send random commands over a small keyspace, so commands often
  touch the same keys. One of the connections is an ACL-limited user.
- A reference model computes the expected reply to every command. Live replies are compared
  with it as they arrive.
- Commands running at the same time never share a key, so each key has one known order of
  operations. Commands on different keys overlap freely.
- Each epoch ends with a crash, a restart, a live check or an injected fault. After every
  restart the checker searches for a consistent cut of the acknowledged history that explains
  the recovered keyspace.

A recovered keyspace passes only when all of the following hold:

| Rule | Meaning |
| --- | --- |
| Prefix per key | Each key's value equals the model after some prefix of that key's operations |
| All or nothing | A write that touched several keys (MULTI, EVAL, RENAME, FLUSHDB) is either fully present or fully absent |
| No holes | If an included write was sent after another write on the same shard was acknowledged, the earlier write is included too |
| No resurrection | Deleted or expired keys do not come back |
| Profile bound | The durability profile sets how much acknowledged history must survive (see below) |

What each durability profile has to keep after a crash:

| Profile | Recovered state must contain |
| --- | --- |
| `memory` | Nothing: the keyspace is empty after a restart |
| `everysec` | Every write acknowledged before the last sync window |
| `always` | Every acknowledged write |
| `snapshot` | Every write acknowledged before the last successful `SAVE` |

A clean shutdown (SIGTERM) must keep every acknowledged write in every profile except
`memory`.

The workload covers strings and counters, key and field TTLs, lists, hashes with field TTLs,
sets, sorted sets, streams, MULTI/EXEC, EVAL, RENAME, FLUSHDB, SAVE and BGSAVE, BLPOP with a
pusher, and commands the ACL user is not allowed to run.

## Fault model

| Fault | How it is produced | Configurations |
| --- | --- | --- |
| Process kill | `kill -9` of the server, then restart | All |
| Graceful stop | SIGTERM, then restart | All |
| Disk I/O error | `EIO` on write or fsync, injected through an LD_PRELOAD shim | WAL-backed and snapshot, not replica |
| Disk full | `ENOSPC` on write, through the same shim | WAL-backed and snapshot, not replica |
| Partial write | A write completes half its buffer, then the next call fails, leaving a torn record on disk | WAL-backed and snapshot, not replica |
| Fault target | WAL files, checkpoint files, or both (a `SAVE` is sent so the checkpoint path runs during the fault) | As above |
| Power loss | The shim records every WAL write range and every successful sync; after `kill -9` the bytes written since a file's last sync are zeroed | `everysec`, `always` |
| Torn WAL write | The server dies after writing a random prefix of one WAL write. Most tears wait for an idle server, send one MULTI of writes or a RENAME, and cut the first, second or third WAL write after it, so a crash can land between the records of one multi-key write | `everysec`, `always` |
| Replica crash | `kill -9` or SIGTERM of the replica, then restart while the primary keeps its data | Replica configs |
| Memory pressure | A `maxmemory` limit with the `noeviction` policy that the workload reaches, so writes are refused with OOM | `maxmemory` |

## What the checker verifies

| Check | Failure it reports |
| --- | --- |
| Replies | Any reply that differs from the reference model |
| Recovered state | No consistent cut of acknowledged writes explains the keyspace, or the profile bound is not met |
| No OK after a failed sync | A write acknowledged after a WAL fsync failed on its shard |
| Sticky refusal | A shard accepts writes again after a WAL fsync failure, even once the disk recovers; it must refuse writes until restart. After a failed write (not sync), writes must resume once the disk recovers |
| SAVE | `SAVE` replies OK although a checkpoint write or sync failed while it ran; `SAVE` fails without a fault |
| Hangs | Commands get no reply during or after a disk fault |
| Replica convergence | The replica does not reach the primary's state in time. Converged also means the same `DBSIZE`: reads hide a logically expired key, but a replica keeps it until the primary's delete arrives |
| Atomicity | Part of a MULTI, EVAL, RENAME or FLUSHDB is recovered without the rest |
| Memory limit | A command that only frees memory (DEL, UNLINK, LPOP, HDEL, SREM, RENAME and similar) is refused with OOM |
| Crashes | The server exits on its own |

A failure writes a repro directory with the seed, the command to repeat the run, the expected
and actual state, the last commands with their replies, and the server log, config and fault
log.

## Configurations

| Name | Setup |
| --- | --- |
| `memory-1` | No persistence, one shard |
| `everysec-1` | WAL synced every second, one shard |
| `everysec-2` | WAL synced every second, two shards |
| `always-1` | WAL synced before every acknowledgement, one shard, sync on the shard thread |
| `always-worker` | As `always-1`, with a separate sync worker thread |
| `always-2` | `always`, two shards |
| `snapshot-1` | Checkpoints only, one shard |
| `snapshot-2` | Checkpoints only, two shards |
| `replica` | Primary and replica, `everysec` |
| `replica-always` | Primary and replica, `always` |
| `maxmemory` | `everysec` with a `noeviction` memory limit the workload reaches |

## Checking the checker

A checker that passes everything proves nothing, so its ability to find bugs is measured.

**Mutation testing.** `scripts/model_check_mutants/` holds 12 patches, each a small,
realistic bug planted in the server. A runner applies each one to a scratch worktree, builds
it, and runs the checker with fixed seeds over the configurations where the bug can show. The
unmodified server is run the same way as a control. In the latest measurement (October 2026: three seeds of 60 seconds per bug, plus
tear-heavy runs for the multi-record bugs) the checker catches 10 of the 12:

| Planted bug | Caught | By |
| --- | --- | --- |
| `always` acknowledges a small batch before its fsync | Yes | Power-loss simulation |
| A failed WAL fsync is treated like a failed write and writes resume | Yes | Sticky-refusal probe after a sync fault |
| Keys removed by active expiry are not logged as deletes | Yes | Recovered state and replica convergence |
| A push onto an existing list is missed by a running checkpoint | Yes | Recovered state after restart |
| `SAVE` replies OK when its checkpoint failed | Yes | SAVE check under a checkpoint fault |
| EXEC logged as one record per command instead of one transaction | Yes | Torn WAL writes (atomicity) |
| `INCR` from 9 returns 11 | Yes | Live reply comparison |
| Shutdown skips the final checkpoint | Yes | Recovered state after SIGTERM (snapshot) |
| HSET stores fields before an OOM refusal | Yes | Live replies and recovered state under `maxmemory` |
| Hash field TTLs dropped when a checkpoint is loaded | Yes | Recovered state after restart |
| RENAME logged as a delete plus a set | Only in tear-heavy runs | Torn WAL writes; a default mix rarely cuts between the two records |
| A reconnecting replica applies its last frame twice | No | Needs a replication link drop without a process restart, which the fault model does not produce |

**False alarms.** On the unmodified server the checker's false-alarm rate is about 0.7 per
hour of run time. Every known cause has been fixed in the checker.

**Seeded smoke tests.** `tests/model_check_live.rs` runs a short seeded checker session that
must pass, and a run against a server that loses writes, which must be reported as a state
failure.

**Redis comparison.** Reply differences the checker found were compared with Redis and
fixed to match: a command refused inside MULTI (for example by ACL) now aborts the
transaction with `EXECABORT`; MOVE and SWAPDB with invalid arguments inside MULTI are queued
and fail only at EXEC; and a push that wakes a blocked BLPOP client replies with the list
length before the hand-off.

## Bugs found and fixed

These classes of bugs were found by this testing and are fixed:

| Area | Bug |
| --- | --- |
| Replication | A replica restarted while receiving writes could apply some of them twice: list elements came back doubled, and a repeated stream entry stopped startup |
| Hash field TTL | A hash whose fields had all expired stayed visible to TYPE, EXISTS, SCAN and KEYS, and made other writes reply WRONGTYPE, until a background sweep reached it |
| Checkpoints | `SAVE` with two shards could wait up to two minutes when a checkpoint write failed, instead of failing at once as with one shard |
| Rollback | A failed script or refused MULTI could drop a key from a running checkpoint, so a restart lost it |
| Rollback | The same rollback path could lose a hash's field expiry, so expired fields were returned |
| Transactions | A command refused while queued in MULTI did not abort the transaction; EXEC ran the rest |
| Checkpoints | RELEASE of a lease or semaphore during a checkpoint could write the wrong value into the snapshot |
| Memory limit | Deletes and other commands that only free memory were refused on a full `noeviction` store |
| WAL failures | After a failed `always` sync, a write the client was refused could still reach a checkpoint and come back after a restart |
| WAL failures | A SAVE or segment rotation whose sync failed left refused `always` writes on disk, so a restart replayed them |
| WAL failures | Writes forwarded between shards in one batch could be acknowledged although their log records had been cut |
| Power loss | A final WAL segment whose header never reached disk stopped startup with "invalid WAL magic" instead of being dropped as a torn tail |
| Startup | Loading a checkpoint with hash field expiries or semaphores used the wall clock instead of the replay clock, so startup could fail |
| Replication | A replica slot was not freed when the link dropped, so the primary refused new replicas once the limit was reached |
| Replication | After a primary restart, frames the old link had queued arrived again on the new link; the replica refused them and stopped following for good. It now skips them and takes a fresh copy on a real gap |
| Process kill | Under `everysec`, a write could be acknowledged while its record still waited in memory behind another connection's open WAL group, so a `kill -9` lost it |
| Blocking pops | A pop served by a push whose `always` sync then failed still returned the element, although the push was refused and gone after a restart |
| Transactions | A MULTI forwarded to another shard could still run part of its commands after its batch's WAL sync failed |
| WAL failures | A forwarded push that only fed a waiting pop lost its reply when its batch's sync failed, although nothing it wrote needed the log |

## Other suites behind these guarantees

| Suite | What it covers |
| --- | --- |
| `killed_process_recovers_at_crash_sites` (`src/persistence/crash.rs`) | A child process killed at fixed points: WAL append, sync and rotation, checkpoint write, rename and manifest |
| `a_kill_at_any_point_recovers_every_acknowledged_write_as_a_prefix` (`tests/process_live.rs`) | Kill at a random point recovers acknowledged writes as a prefix |
| `tests/recovery_live.rs` | Restart and recovery of the real binary |
| `tests/storage_properties.rs` and property tests in `src/` | Store operations match a reference map (proptest) |
| `tests/concurrency_model.rs` | A loom model of the shutdown and queue-admission counters (one test; not the shard code) |
| `fuzz/` | Fuzzing of the RESP and JSON parsers (cargo-fuzz) |

## Known limits

| Not covered | Notes |
| --- | --- |
| Failover | Promotion is manual in these runs. Automatic failover with `ruvio coordinator` has its own fault runs: `scripts/coordinator_faults.py` (partitions through a TCP proxy, a coordinator leader killed, no majority, restart, loss under load with and without `CLIENT DURABILITY`) and `tests/coordinator_live.rs`; see [COORDINATOR.md](COORDINATOR.md) |
| Power loss beyond WAL contents | Directory entries and checkpoint files are not part of the power-loss model |
| Failover and `IDEM`, `ONCE`, `LIMIT`, `LEASE` | Not run with a promoted replica. By design, a write the old primary had not shipped is lost unless its connection used `CLIENT DURABILITY`; deadlines are wall-clock times, so clocks must agree. See [IDEM.md](IDEM.md) |
| Network partitions | Replica tests kill and restart processes; they do not cut or delay the link alone. With coordinators, an old primary cut off together with one of its replicas keeps taking writes for the whole partition unless `min-replicas-to-write` counts every replica or the primary lease is on, and `CLIENT DURABILITY REPLICAS 1` does not keep acknowledged writes in that shape; see *What can still lose data* in [COORDINATOR.md](COORDINATOR.md#what-can-still-lose-data) |
| Part of the command surface | The checker's workload has no MOVE, SWAPDB, multiple databases or JSON commands |
| Platforms | Disk faults, power loss and torn writes use an LD_PRELOAD shim, so they run on Linux only |
| Disk faults on replicas | Replica configurations do not inject disk faults |
| Hot keys with `appendfsync always` | A hot-key write is applied to the shared hot store before its batch is synced. If that sync then fails, the client gets an error, but readers can still see the value until a restart; checkpoints stop until then. Writes to a log that has already failed are refused before they are applied. The checker has no hot-keys configuration |

## Running it

Linux, Python 3 and a C compiler (for the shim) are needed. A one-hour run over all
configurations:

```bash
cargo build --profile release-fast --bin ruvio
python3 scripts/model_check.py --seed 1 --duration 3600 --config all \
    --bin target/release-fast/ruvio --results target/model-check --keep-going
```

Options, the mutation runner and how to read a failure are in
[TESTING.md](TESTING.md#long-model-check-sessions).
