# Upgrade and rollback between two releases (2026-11-27)

Tool: `scripts/upgrade_rollback.py`. Everything ran as local processes in one Linux container
(4 cores), debug builds of the previous `main` (`a096fc7`) and of `dev` (`3976f14`), plus the
published `ruvio-0.3.4` Linux x64 release binary (SHA-256 checked against its `SHA256SUMS`) for
the case without coordinators. Each node used one shard, `durability = always`, and a group of
three nodes ran with three coordinators (`ping_interval_ms` 200, `down_after_ms` 1000). A writer
sent `SET` with `CLIENT DURABILITY REPLICAS 1` the whole time and followed the primary through the
coordinators; a key counts as acknowledged only on `+OK`. All scenarios together took under two
minutes, 64 checks, none failed.

## What was checked

| Scenario | Result |
| --- | --- |
| Coordinators upgraded together while the writer ran | They agreed on the same primary at once; the longest pause in acknowledged writes was 0.02 s |
| Both replicas moved to the new binary while the old primary kept writing (mixed window) | The new replicas followed the old primary; no writer error in the window |
| `COORDINATOR SWITCHOVER` while the primary still ran the old release | Refused with `ERR the primary refused to pause writes: ERR RUVIO expects zero or one argument`; the old primary stayed in charge and kept writing (longest pause 0.02 s) |
| Old primary stopped, an upgraded replica promoted by the coordinators | Writes resumed after 1.1 to 1.2 s |
| Old primary restarted on the new binary as a replica | It followed the new primary; after the upgrade `SWITCHOVER` back to it worked (first try refused for 1 s while it was not yet a complete replica, then accepted; one `NOREPLICAS ... paused for a switchover` reply, longest write pause 0.1 s) |
| Acknowledged writes at the end | Every acknowledged write was on all three nodes (1243 of 1245 attempts acknowledged; the two others were not acknowledged and are not counted) |
| Wrong order: primary on the new binary first, replicas still old (no coordinators) | The old replicas followed the new primary and kept their data; the upgrade then finished with every key on every node |
| Rollback by restoring the backups taken before the upgrade, coordinator Raft files removed | Old binaries started on the restored data, the old coordinators started and agreed on node 0; every write acknowledged before the backup was on all nodes; every write made after the backup was gone, as expected from a restore; the group took durable writes again |
| Old coordinators started on the new release's Raft log without clearing it | Not a difference between these two builds: both write the same log format; the coordinators started and agreed. The documented clearing was still needed only for older logs |
| Old binary started directly on the new release's data | These two builds share WAL format minor 6, so it started (and changed the directory); no refusal to observe |
| `0.3.4` (WAL minor 3, no coordinator) to the new binary | The new binary opened the data and held every acknowledged write; the replica followed; new segments have minor 6 |
| `0.3.4` started on the upgraded data | Refused at start with `unsupported persistence format: WAL minor version 6`; the directory was byte-identical afterwards |
| `0.3.4` after restoring the backup | Started, held exactly the writes of the backup (the later 1000 were gone), the replica followed |
| New replica of a `0.3.4` primary | Not possible: `replica refused: primary and replicas must run the same Ruvio version (replication handshake version mismatch)` on both sides; the primary kept its data |

## Not measured

- A published release with a different WAL or Raft format from the new one together with coordinators (no such release has coordinators).
- Several shards per node, `primary_lease_ms`, TLS, ACL, and data sets larger than a few thousand keys.
- Real network latency or real disks: everything ran on the container's local disk without faults.
- Timings are from a debug build in one container; they show order of magnitude only.
