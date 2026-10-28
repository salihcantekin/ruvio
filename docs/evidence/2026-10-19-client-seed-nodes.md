[Ruvio](../../README.md) / [Docs](../README.md) / Evidence / Client seed addresses

# Client seed addresses: live failover and routing cost

The change touches only the connect, reconnect and slot map reload paths of the six clients ([CLIENT_BEHAVIOR.md](../CLIENT_BEHAVIOR.md)); the server is unchanged.

## Live failover

`scripts/seed_failover.py` starts two standalone servers holding `seed:name` = `a` and `b`, runs a client probe with both as seeds, stops the first server and checks that the probe reads `b` afterwards. Debug server build, macOS (Apple silicon), 2026-10-19.

| Client | Probe | Result |
| --- | --- | --- |
| Rust | `clients/ruvio-client/examples/seed_probe.rs` | `first a, after b` |
| C# | `clients/csharp/Ruvio.Client.SeedProbe` | `first a, after b` |
| Go | `clients/go/internal/seedprobe` | `first a, after b` |
| Java | `clients/java/src/test/java/.../SeedProbe.java` | `first a, after b` |
| Node | `clients/node/bench/seed-probe.ts` | `first a, after b` |
| Python | `clients/python/benchmarks/seed_probe.py` | `first a, after b` |

Before this change no client took a second address, so the same stop left every client retrying the stopped server.

## Routing cost

Each client's routing-cost tool ([2026-10-16-client-cluster-routing.md](2026-10-16-client-cluster-routing.md)) against one release-fast server with 4 shards and `port_mode = "per-shard"`, `FLUSHALL` before each run, the base commit and the change one after the other, one run each. User CPU nanoseconds per request, base → change:

| Client | one-node GET P=16 | one-node SET P=64 | cluster GET P=64 | plain GET P=16 | allocations / bytes per request |
| --- | --- | --- | --- | --- | --- |
| Rust | 145 → 146 | 141 → 144 | 215 → 269 | 109 → 117 | same |
| C# | 705 → 798 | 260 → 255 | 699 → 645 | 1026 → 833 | same or lower |
| Go | 483 → 320 | 157 → 155 | 316 → 339 | 482 → 446 | same |
| Java | 324 → 332 | 148 → 120 | 340 → 342 | 246 → 254 | same or lower |
| Node | 586 → 728 | 336 → 390 | 638 → 358 | 621 → 785 | not counted by the tool |
| Python | 1384 → 1354 | 1174 → 1169 | 1088 → 1070 | 758 → 740 | not counted by the tool |

Other benchmarks were running on the same machine during the Rust, C#, Go, Java and Node runs (load average about 10), so their CPU numbers move in both directions, including rows whose code did not change (`plain`). The Python run was taken alone and moves by 2% or less. Allocations and bytes per request did not grow in any client. A quiet repeat on a dedicated machine was not made.
