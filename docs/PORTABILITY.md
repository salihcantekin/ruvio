# Portability Strategy

Ruvio treats operating-system integration as an adapter, not as part of the storage model.

## Portable Core

The following layers must compile without OS-specific dependencies:

- key ownership and shard storage,
- command and response types,
- protocol parsing and encoding,
- key-to-shard routing,
- expiration and eviction policy,
- conformance and property tests.

These layers must not expose Tokio, file descriptors, handles, or platform socket types in their public APIs.

## Runtime

The process runs one adapter: Tokio `current_thread` plus the OS multiplexer
(epoll, kqueue, or IOCP). Parse, apply, and write run on the shard owner.
`shards > 1` starts one owner thread per shard on consecutive ports.

## Containers

Docker is a packaging boundary, not an operating-system abstraction. Linux
containers still depend on the host kernel, seccomp profile, CPU topology,
and cgroup limits.

`Dockerfile` and `Dockerfile.linux` both build that runtime. Neither needs
`seccomp=unconfined`.

## Support Matrix

| Target | Evented |
| --- | --- |
| Linux host | Yes |
| Linux container | Yes |
| macOS | Yes |
| Windows | Yes |

CI runs the same tests on macOS, Linux, and Windows.

Windows uses the same evented process, through IOCP. It is a supported host, not a separate adapter.

Multi-shard mode uses consecutive ports with cluster discovery, and forwards a misrouted command to its owner, rather than silently placing independent keyspaces behind `SO_REUSEPORT`. This preserves deterministic ownership. Static `CLUSTER SLOTS` and `CLUSTER NODES` discovery and separate, asynchronous per-shard WAL-tail replication (including checkpoint resync for a covered gap) are implemented. There is no cluster bus. `RUVIO MOVESLOTS` moves slots between the shards of one process, and `ruvio coordinator` does the failover of a primary and its replicas and the slot moves between machines ([COORDINATOR.md](COORDINATOR.md)).