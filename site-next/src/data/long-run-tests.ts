// Long runs of a multi-node cluster, shown on /testing/. One entry per run worth showing; newest first.
// The tool and its plans are in tests/soak/ of the repository; numbers come from each run's status.json
// and check.json.

export type LongRun = {
  date: string;
  title: string;
  environment: string;
  duration: string;
  tested: string[];
  result: string[];
};

export const longRuns: LongRun[] = [
  {
    date: "2026-10-26",
    title: "Throughput as shards are added",
    environment: "Azure F16as_v6 (16 cores): servers on 8 cores, load on the other 8",
    duration: "24 minutes, 3 rounds of every row",
    tested: [
      "SET and GET at 1, 4 and 8 shards with a cluster-aware client",
      "A client that does not route: every connection on one port, the server forwards each command",
      "One shard: SET, GET, INCR, LPUSH and LPOP",
    ],
    result: [
      "4 shards: 6.8M SET and 5.4M GET per second, against 1.25M and 1.5M on one shard",
      "One port: 4 shards 0.88M SET / 1.05M GET, 8 shards 1.22M SET per second, up from 0.65M after the forwarding fix",
      "One shard: SET, GET and INCR at 1.09M, LPUSH 0.92M, LPOP 0.86M per second at pipeline 16; the full tables are on the benchmarks page",
    ],
  },
  {
    date: "2026-10-26",
    title: "Memory under a steady load",
    environment: "Azure, 3 nodes (4 vCPU, 16 GB each) and a load machine",
    duration: "2 hours",
    tested: [
      "5.1 million reads and writes on a data set of fixed size, so memory growth cannot come from the data",
      "Faults while the load runs: network delay and packet loss, replicas killed and restarted",
      "Process memory and the server's own count of its data, sampled every 10 seconds",
    ],
    result: [
      "Writes that waited for both replicas: none lost, duplicated or out of order",
      "Every fault recovered, the slowest in 30 seconds",
      "The server's count of its data stayed flat; each process stayed under 17 MB, growing at most 0.6 MB an hour",
    ],
  },
  {
    date: "2026-10-26",
    title: "Failures under load",
    environment: "Azure, 3 nodes (4 vCPU, 16 GB each) and a load machine",
    duration: "3 hours",
    tested: [
      "9.1 million counter, list and register operations, every one recorded and checked at the end",
      "23 faults of seven kinds: primary, replica and coordinator leader killed, a whole machine killed, a replica frozen, a machine cut off the network, network delay and packet loss",
    ],
    result: [
      "Writes that waited for both replicas: none lost, duplicated, invented or out of order",
      "Every fault recovered, the slowest in 33 seconds",
      "Without the replica wait, a failover can lose the last acknowledged writes, as documented for that mode",
    ],
  },
];
