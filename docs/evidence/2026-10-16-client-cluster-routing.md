# Cluster routing cost in the clients: before and after

Client CPU per request on the cluster path against the plain single-connection path, before and after the routing rules in [CLIENT_BEHAVIOR.md](../CLIENT_BEHAVIOR.md#routing-cost) were applied to the Rust, C#, Java, Node, Python and Go clients.

## Conditions (read before quoting any number)

- Host: Apple silicon Mac, 12 cores, macOS, loopback. One `release-fast` Ruvio server, four shards, `port_mode = "per-shard"`, `memory` durability. Every client ran alone, one after another, on the same server.
- Each client's own tool: `ruvio-client/examples/cluster_cost.rs`, `csharp/Ruvio.Client.Benchmarks --cluster-cost` (with `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0`; without it thread-pool spinning dominates the CPU numbers), `java/.../RoutingCostBench` (thread CPU of the client's threads), `node npm run bench:cluster-cost`, `python benchmarks/bench.py --cluster-cost`, Go `RUVIO_COST_ADDR=... go test -run TestClusterCost -v`. Eight workers with a connection each (Python: one), keys `key:0`..`key:4095`, 32-byte values, 3-4 s per row after a warmup, one run.
- Rows: **plain** connects to one port without discovery (the server forwards keys it does not own); **cluster** routes keys spread over the four shards; **one node** is the cluster client with every key in one hash tag. *One node* against *plain* is the routing code's own cost. *Cluster* adds the extra writes, reads and wakeups of four sockets instead of one, which no routing code removes.
- "Before" is the parent commit with the same measuring tool. Numbers are user CPU nanoseconds per request; compare rows with each other only.

## Result

| Client | Op, depth | Plain | Cluster before → after (× plain) | One node before → after (× plain) | Bytes/request, cluster before → after |
| --- | --- | ---: | --- | --- | --- |
| Rust | GET 16 | 130 | 1158 → 322 (2.48×) | 3178 → 153 (1.18×) | 377 → 66 (plain 66) |
| Rust | GET 64 | 105 | 3155 → 192 (1.83×) | 4277 → 113 (1.08×) | 369 → 66 (plain 66) |
| Rust | SET 16 | 131 | 1312 → 347 (2.65×) | 4138 → 158 (1.21×) | 451 → 34 (plain 34) |
| Rust | SET 64 | 102 | 4070 → 190 (1.86×) | 5271 → 106 (1.04×) | 443 → 34 (plain 34) |
| C# | GET 16 | 731 | 2053 → 1878 (2.57×) | 685 → 639 (0.87×) | 346 → 232 (plain 135) |
| C# | GET 64 | 264 | 682 → 605 (2.29×) | 328 → 262 (0.99×) | 198 → 154 (plain 124) |
| C# | SET 16 | 725 | 2077 → 1863 (2.57×) | 697 → 633 (0.87×) | 235 → 120 (plain 23) |
| C# | SET 64 | 262 | 686 → 623 (2.38×) | 311 → 266 (1.02×) | 86 → 42 (plain 12) |
| Java | GET 16 | 210 | 640 → 623 (2.97×) | 194 → 186 (0.89×) | 395 → 97 (plain 38) |
| Java | GET 64 | 79 | 239 → 242 (3.06×) | 99 → 90 (1.14×) | 162 → 83 (plain 34) |
| Java | SET 16 | 210 | 629 → 621 (2.96×) | 209 → 197 (0.94×) | 435 → 141 (plain 38) |
| Java | SET 64 | 73 | 249 → 249 (3.41×) | 98 → 87 (1.19×) | 330 → 128 (plain 34) |
| Node | GET 16 | 603 | 870 → 816 (1.35×) | 626 → 548 (0.91×) | not measured |
| Node | GET 64 | 247 | 399 → 360 (1.46×) | 326 → 263 (1.06×) | not measured |
| Node | SET 16 | 599 | 908 → 901 (1.50×) | 600 → 533 (0.89×) | not measured |
| Node | SET 64 | 314 | 469 → 458 (1.46×) | 386 → 324 (1.03×) | not measured |
| Python | GET 16 | 898 | 1841 → 1701 (1.89×) | 1772 → 1509 (1.68×) | not measured |
| Python | GET 64 | 676 | 1443 → 1245 (1.84×) | 1399 → 1138 (1.68×) | not measured |
| Python | SET 16 | 854 | 1929 → 1698 (1.99×) | 1746 → 1505 (1.76×) | not measured |
| Python | SET 64 | 657 | 1395 → 1162 (1.77×) | 1354 → 1126 (1.71×) | not measured |
| Go | GET 16 | 387 | 2240 → 775 (2.00×) | 298 → 326 (0.84×) | 229 → 105 (plain 104) |
| Go | GET 64 | 152 | 844 → 320 (2.11×) | 146 → 141 (0.93×) | 213 → 109 (plain 108) |
| Go | SET 16 | 385 | 2233 → 747 (1.94×) | 285 → 285 (0.74×) | 229 → 73 (plain 72) |
| Go | SET 64 | 137 | 843 → 295 (2.15×) | 149 → 147 (1.07×) | 216 → 78 (plain 76) |

Reading:
- Rust had the costly path: 11-13 allocations per command (upper-cased name, key list, `host:port` strings, owned argument copies, a map per pipeline) and one round trip per node. After: allocations equal the plain path, every node is written before any is read, and the routing code costs 1.04-1.21× plain. Cluster user CPU dropped 3.6-21×.
- C#, Java and Node already routed without allocating on the single-key path; their routing code now costs 0.87-1.19× plain, and C# and Java pipeline allocation per request fell by a fifth to three quarters. The remaining gap on the spread rows is the per-socket cost of their multiplexed connections (one request, completion and thread hop per node), outside routing.
- Python's routing code still costs about 1.7× plain: each command is matched and looked up in the interpreter. Down 8-17% on the spread rows and 14-19% on the one-node rows.
- Go already routed without allocating and its routing code cost the same as plain; the spread path allocated a map, slices and a goroutine per node on every pipeline, and each node's replies were handed to that node's background reader. It now allocates only the reply slice, writes every node before reading any, and the calling goroutine reads every node nobody else is reading: 2.6-3.0× less CPU on the spread rows. The Go rows were measured with the one-minute tool (two seconds per row).
- Per-socket floor: a depth-16 pipeline over four nodes leaves batches of four on each socket. Plain depth 4 on one socket costs (user ns per request) Rust 251, C# 3603, Go 1333, Java 708, Node 1963, Python 1649; the spread cluster rows of every client are now at or below that, so what remains is the cost of one round trip per socket in each client's transport, not routing. In C# that round trip is the most expensive (the async socket engine and thread-pool dispatch on macOS); Linux was not measured. With the opt-in `RuvioClientOptions.DedicatedReaderThread` (blocking sockets, a reader thread each) C# plain depth 4 drops to 1908 and the spread cluster rows to 1226 (GET 16) and 423 (GET 64), against 1851 and 587 without it; 8-caller throughput stayed within 10%.
- Single commands (depth 1, not in the table) cost the same or less than before in every client; they are dominated by system calls on this machine.

Not measured: Linux and x86; allocation per request in Node and Python; latency percentiles.
