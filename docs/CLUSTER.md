[Ruvio](../README.md) / [Docs](README.md) / Cluster and failover

# Shards, replication, cluster and failover

Ruvio grows in four steps. Each step is optional and adds one thing on top of the one before; a single process with one shard needs none of them. This page names the pieces and says where each one is described in full.

| Step | What it adds | Started with | Details |
| --- | --- | --- | --- |
| Shards | Several cores in one process, each owning a range of the 16,384 hash slots | `shards = N` | [Connection model](CONNECTION_MODEL.md#shards) |
| Replication group | A primary and its replicas: copies of the same slots on other machines | `[replication]` `role`, `listen`, `primary` | [Configuration](CONFIGURATION.md), [Recovery](RECOVERY.md#promote-a-replica-by-hand) |
| Automatic failover | Three or five `ruvio coordinator` processes that pick a new primary when one stops answering | `ruvio coordinator` | [Automatic failover](COORDINATOR.md) |
| Multi-node cluster | Several replication groups, each owning part of the slots, and slot moves between them | `cluster_topology`, `cluster_group`, coordinator `[[groups]]` | [Automatic failover: a multi-node cluster](COORDINATOR.md#a-multi-node-cluster) |

## Shards

One process runs one thread per shard; shard `i` listens on the bind port plus `i`. A key belongs to the shard that owns `hash_slot(key)`, as in Redis Cluster. A command sent to the wrong shard of the process is forwarded to the right one and answered on the same connection, so a client that knows nothing about slots still works; cluster-aware clients read `CLUSTER SLOTS` and send each key to its owner directly, which is faster. Commands with several keys need them in one slot (a hash tag). Measured scaling: [shard scaling record](evidence/2026-10-26-azure-shard-scaling.md).

## Replication

A replica copies its primary shard by shard, from the write-ahead log, over a separate replication port. Replication is asynchronous: the primary answers before a replica has the write. Replicas serve reads; once they have finished copying, the primary lists them in `CLUSTER SLOTS`, so the clients can read from them with a read policy ([Client behavior: reads from replicas](CLIENT_BEHAVIOR.md#reads-from-replicas)).

A connection that must not lose an acknowledged write to a failover asks for it with `CLIENT DURABILITY REPLICAS n`: its replies wait until `n` replicas hold the writes on disk ([Configuration](CONFIGURATION.md), [what can be lost](COORDINATOR.md#what-can-be-lost-and-split-brain)). Without it a failover can lose the last writes a primary acknowledged, as in Redis.

## Failover

Without coordinators, promotion is a person running `REPLICAOF NO ONE` on a replica ([Recovery](RECOVERY.md#promote-a-replica-by-hand)). With coordinators, a majority of them agrees (through Raft) that a primary is down, checks that the chosen replica answers, promotes it and points the other members at it. The coordinators hold no data and serve no data commands. An optional primary lease (`primary_lease`, off by default) stops an old, cut-off primary from taking writes once the coordinators stop renewing it.

Clients follow a failover on their own: with several seed addresses, or with the coordinators' address and a group name, all six first-party clients find the new primary after a dropped connection or a `READONLY` reply ([Client behavior: following a failover](CLIENT_BEHAVIOR.md#following-a-failover)).

## Multi-node cluster

Several replication groups share the hash slots, each group on its own machines. Every data node starts with the same `cluster_topology` map and its own `cluster_group`; the coordinators keep the map current after each failover or slot move and send it to every node. A node answers keys of another group with `MOVED` naming that group's primary, and a range being moved with `TRYAGAIN` while it is frozen, so cluster clients follow or retry. `COORDINATOR MOVE` moves a slot range between groups while both serve, and `COORDINATOR REBALANCE` plans and runs moves that even out the groups.

## Where to go next

- Run it: [Automatic failover with `ruvio coordinator`](COORDINATOR.md), setting up, upgrading and limits.
- What a failure costs: [Recovery, backup and integrity](RECOVERY.md), [Resilience tests](RESILIENCE.md).
- Hours of load and injected faults on a three-node cluster: [long-run tests](https://ruviodb.com/testing/).
