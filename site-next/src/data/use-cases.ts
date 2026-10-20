// The use cases shown on /use-cases/, in page order. Other pages (integrations, home) link to
// them by id, so an id is part of the site's URLs: keep it stable.
export type Ref = { label: string; href: string };
export type UseCase = { id: string; nav: string; title: string; problem: string; code: string; body: string; note?: string; refs: Ref[] };

const commands = "/docs/commands/";

export const cases: UseCase[] = [
  {
    id: "rate-limits",
    nav: "Rate limits",
    title: "Rate limits and quotas",
    problem: "A fixed window per API key, per user, or per tenant, in one command on the shard that owns the key.",
    code: `LIMIT api:key-7 100 60000
# 99    -> 99 calls left in this 60 s window
# (nil) -> window is full, nothing was counted

LIMIT api:key-7 100 60000 SLIDING
LIMIT api:key-7 10 1000 TOKENS COST 2 WITHRETRY
# [8, 0]   -> 8 tokens left, go ahead
# [-1, 150] -> refused, retry in 150 ms`,
    body: "<code>LIMIT key max window_ms</code> returns the remaining allowance, or a null bulk when the window is full. A rejected call is not counted. The window is the key's deadline, so a quiet key disappears by itself. <code>SLIDING</code> weighs the previous window by how much of it still overlaps, so a burst at a window edge is not doubled. <code>TOKENS</code> is a token bucket that refills continuously and takes <code>COST</code> tokens per call. Both keep a few bytes per key, and <code>WITHRETRY</code> adds the wait to the reply for a <code>Retry-After</code> header. In WAL-backed modes the accepted call is a log record.",
    note: "A key keeps the mode it was created with. The sliding window is an estimate from two counters, not an exact log of calls.",
    refs: [{ label: "LIMIT", href: commands }],
  },
  {
    id: "locks",
    nav: "Locks and fencing",
    title: "Locks, leader election and fencing",
    problem: "One worker at a time holds a named resource; others get a null reply until it frees or expires.",
    code: `LEASE job:nightly-report 30000
# token -> you hold it
# (nil) -> someone else holds it
RENEW job:nightly-report 17 30000
# 1     -> deadline moved to now + 30 s (0 if the token no longer holds it)
RELEASE job:nightly-report 17
# 1     -> released (0 if the token no longer matches)`,
    body: "Each grant returns a token that grows. Send it with writes to the thing you protect and have that side reject older tokens. A long job calls <code>RENEW</code> with its token to keep the lock without releasing it. Tokens are opaque 64-bit integers that only grow.",
    note: "Tokens never go backwards, also after expiry, a restart or a failover. For a lock that survives a failover, take it on a connection that sent CLIENT DURABILITY REPLICAS 1, keep the nodes' clocks synchronized, and have the protected resource reject a token lower than the last one it saw.",
    refs: [{ label: "LEASE, RENEW and RELEASE", href: commands }],
  },
  {
    id: "workers",
    nav: "Worker concurrency",
    title: "Concurrency limits for workers",
    problem: "At most N holders at once: image resizers, export jobs, calls to a slow upstream.",
    code: `SEMAPHORE upstream:billing 8 15000
# 1700000000123 -> a slot, this is your token
# (nil)         -> all 8 slots are taken
RELEASE upstream:billing 1700000000123`,
    body: "Every holder has its own TTL, so a crashed worker frees its slot when the TTL ends. The key is removed when the last holder leaves.",
    note: "Pick a TTL longer than the longest normal job, and release as soon as you finish.",
    refs: [{ label: "SEMAPHORE", href: commands }],
  },
  {
    id: "inventory",
    nav: "Inventory and seats",
    title: "Inventory, tickets and seat counts",
    problem: "Sell from a counter without overselling, with no script and no read-then-write race.",
    code: `SET stock:ticket-42 500
TAKE stock:ticket-42 2
# 498   -> took 2, 498 left
# (nil) -> not enough, the key is unchanged

INCRBY seats:room-9 1 MAX 120
# 57    -> joined
# (nil) -> room is full, nothing was written

INCR hits:user-7:2026-10-19 EX 86400
# 1     -> first hit, the day's deadline is set
# 2, 3… -> later hits leave the deadline alone`,
    body: "<code>TAKE</code> subtracts only when the key holds enough. <code>INCRBY … MAX</code> adds only while the result stays at or below the cap. Both answer with a null bulk and write nothing when they refuse. <code>EX</code> or <code>PX</code> on <code>INCR</code> and <code>INCRBY</code> sets a deadline only when the call creates the counter, so a daily counter needs one command.",
    note: "A refused call writes nothing, neither to the key nor to the WAL.",
    refs: [{ label: "TAKE", href: commands }, { label: "INCRBY MAX", href: commands }, { label: "TTL on create", href: "/docs/data-types/" }],
  },
  {
    id: "balances",
    nav: "Credits and wallets",
    title: "Credits, points and wallet moves",
    problem: "Move an amount from one balance to another only when the first one has enough, with no script and no retry loop.",
    code: `SET {user:42}:wallet 50
TRANSFER {user:42}:wallet {user:42}:escrow 30
# 1) 20  -> wallet after
# 2) 30  -> escrow after
# (nil)  -> not enough, nothing changed

HTRANSFER account:7 available held 30 MAX 1000
# 1) 20  -> available after
# 2) 30  -> held after`,
    body: "<code>TRANSFER</code> moves an amount between two integer keys, <code>HTRANSFER</code> between two fields of one hash. Both balances change in one WAL record, and a refusal (short source, or a destination past <code>MAX</code>) answers with a null bulk and writes nothing.",
    note: "TRANSFER keys must share a slot; a hash tag such as {user:42} keeps them together.",
    refs: [{ label: "TRANSFER", href: commands }, { label: "HTRANSFER", href: commands }],
  },
  {
    id: "idempotency",
    nav: "Idempotent APIs",
    title: "Idempotent APIs and payments",
    problem: "A retry of the same request replays the stored answer instead of running the handler again.",
    code: `IDEM order:42 BEGIN request-digest attempt-1 60000
# ["acquired"]                 -> run the handler once
IDEM order:42 COMPLETE request-digest attempt-1 saved-result
# 1
IDEM order:42 BEGIN request-digest attempt-2 60000
# ["completed","saved-result"]`,
    body: "<code>BEGIN</code> answers <code>acquired</code>, <code>pending</code>, <code>conflict</code> (same key, different fingerprint), or <code>completed</code> with the saved result. For plain once-only work such as a webhook delivery id, <code>ONCE key ttl_ms</code> is the lighter tool: 1 the first time, 0 until the deadline.",
    note: "This does not make an outside effect exactly-once. A handler can crash before <code>COMPLETE</code>, so the payment provider still needs its own idempotency key.",
    refs: [{ label: "IDEM", href: "/docs/idem/" }, { label: "ONCE", href: commands }],
  },
  {
    id: "versions",
    nav: "Lost-update protection",
    title: "Lost-update protection",
    problem: "Two editors open the same record; only the one holding the current version may save.",
    code: `SETV profile:ada "{...}"
# 1     -> version 1 created
SETV profile:ada "{...}" 1
# 2     -> accepted, now version 2
SETV profile:ada "{...}" 1
# error -> stale version, value unchanged`,
    body: "Optimistic concurrency without <code>WATCH</code> or a script. Only keys written with <code>SETV</code> carry a version.",
    note: "A plain <code>SET</code> replaces a versioned string.",
    refs: [{ label: "SETV", href: commands }],
  },
  {
    id: "watermarks",
    nav: "Watermarks",
    title: "Watermarks and best values",
    problem: "Keep the newest timestamp, the highest version or the lowest price, no matter in which order writers arrive.",
    code: `SET watermark:orders 1700000200 IFGT
# OK    -> written (key was missing or smaller)
SET watermark:orders 1700000100 IFGT
# (nil) -> stored number is greater, nothing written
SET price:best 499 IFLT PX 3600000
# OK    -> new lowest price, expires in an hour`,
    body: "<code>SET key number IFGT</code> writes only when the number is greater than the stored integer (<code>IFLT</code> for smaller) and takes the usual expiry options. One command replaces a read-compare-write script.",
    note: "Integers only. A stored value that is not an integer is an error and is left unchanged.",
    refs: [{ label: "SET", href: commands }],
  },
  {
    id: "sessions",
    nav: "Sessions",
    title: "Sessions and sliding expiry",
    problem: "Reading a session keeps it alive; an idle one expires on its own.",
    code: `SET session:9f3 "{...}" PX 1800000
GETEX session:9f3 1800000
# "{...}" and the deadline is now 30 min from now`,
    body: "<code>GETEX</code> returns the string and moves its deadline in one step, so there is no separate <code>EXPIRE</code> call. <code>GET</code> does not move a deadline.",
    refs: [{ label: "GETEX", href: commands }],
  },
  {
    id: "cache",
    nav: "Cache fills",
    title: "Cache fills without stampedes",
    problem: "When a hot key expires, one caller refills it and the others wait or serve the stale copy.",
    code: `FILLGET product:42 5000 WAIT 200
# [HIT, value]          -> served from memory
# [FILL, token]         -> you refill it
# [BUSY, nil]           -> someone else is filling
# [STALE, value, token] -> stale copy, and you refresh it (SWR window)
# [STALE, value, nil]   -> stale copy, another caller is refreshing
FILLSET product:42 "{...}" 60000 <token> SWR 30000
# 1 -> stored (0 if the token is no longer live)`,
    body: "One round trip decides who loads from the database. <code>WAIT</code> holds a caller that would get <code>BUSY</code> until the filler writes, so there is no retry loop in the client.",
    note: "Fill markers live in memory only and lapse after <code>fill_ms</code>. <code>WAIT</code> is refused inside <code>MULTI</code> and scripts.",
    refs: [{ label: "FILLGET and FILLSET", href: commands }],
  },
  {
    id: "delayed",
    nav: "Delayed jobs",
    title: "Delayed jobs and queues",
    problem: "Schedule work for later with a stream and a consumer group, without a separate scheduler.",
    code: `XGROUP CREATE jobs workers $ MKSTREAM
XADD jobs DELAY 30000 * kind send-reminder user 42
XREADGROUP GROUP workers w1 COUNT 10 BLOCK 5000 STREAMS jobs >
# nil until the 30 s delay has passed; the worker keeps reading in a loop
XACK jobs workers <id>

ZADD due:jobs 1700000300 job:9 1700000360 job:10
ZPOPBYSCORE due:jobs 1700000330 COUNT 10
# job:9 -> due and removed in the same call
ZMOVEDUE due:jobs queue:ready 1700000400 COUNT 100
# 1     -> moved to the tail of the list queue:ready`,
    body: "The entry is stored right away. <code>XREADGROUP</code> skips it until the delay passes, and <code>XREAD</code> can still see it. Pending entries, <code>XCLAIM</code> and <code>XAUTOCLAIM</code> work as in Redis streams. For a plain schedule keep the run time as the score of a sorted set: <code>ZPOPBYSCORE</code> returns the members that are due and removes them, and <code>ZMOVEDUE</code> moves them to a list in one recorded step, up to 10000 per call.",
    note: "The hold is a WAL record and is written only when a delay is set.",
    refs: [{ label: "XADD DELAY", href: commands }, { label: "ZPOPBYSCORE and ZMOVEDUE", href: commands }],
  },
  {
    id: "changes",
    nav: "Change feeds",
    title: "Change feeds",
    problem: "React to writes (cache invalidation, search indexing, audit) by reading a feed instead of polling.",
    code: `CHANGES SUBSCRIBE FROM NOW MATCH user:* TYPES put,del
# a durable stream of changes, resumable with FROM <shard>:<seq>`,
    body: "The durable feed is built from the WAL and can resume from a saved position, with optional key patterns, types and values. <code>CHANGES START</code> is the lighter in-memory feed of up to 1024 notes per shard.",
    note: "The stream needs a durability profile with a WAL and is refused on a replica. It takes over its connection, so give it a connection of its own. Limits and retention are under <em>Change stream</em> in the configuration reference.",
    refs: [{ label: "CHANGES", href: commands }, { label: "Change stream settings", href: "/docs/configuration/#change-stream" }],
  },
  {
    id: "replica-reads",
    nav: "Reads from replicas",
    title: "Scale reads with replicas",
    problem: "Send read-heavy traffic to replicas while writes stay on the primaries.",
    code: `CLUSTER SLOTS
# 1) 0, 8191,     [10.0.0.5, 7000, <id>], [10.0.0.6, 7000, <id>]
# 2) 8192, 16383, [10.0.0.5, 7001, <id>], [10.0.0.6, 7001, <id>]
#    each range: the primary shard, then its replicas
ROLE
# master, <sequence>, [[10.0.0.6, 7000, <acknowledged>]]`,
    body: "A replica tells its primary where clients reach it. Once it has finished copying, the primary lists it after its own shard in <code>CLUSTER SLOTS</code>, <code>CLUSTER SHARDS</code>, <code>CLUSTER NODES</code>, <code>ROLE</code> and <code>INFO replication</code>. Every Ruvio client takes a read policy: <code>primary</code> (the default), <code>prefer-replica</code> or <code>replica</code>. Reads go to one replica per shard, picked when the slot map loads; writes, transactions, blocking commands and any pipeline with a write stay on the primary. A replica that is down or still copying is skipped.",
    note: "Replication is asynchronous: a read from a replica can miss a write the primary already acknowledged, including your own. Set <code>advertised_host</code> on replicas behind NAT or in containers; <code>replication.announced = false</code> keeps a replica out of the topology.",
    refs: [{ label: "Client behavior", href: "/docs/client-behavior/#reads-from-replicas" }, { label: "Replication settings", href: "/docs/configuration/" }],
  },
  {
    id: "rebalance",
    nav: "Rebalance a busy shard",
    title: "Move slots off a busy shard",
    problem: "One shard takes most of the traffic while the other cores idle.",
    code: `RUVIO MOVESLOTS 0-2047 3
# OK: slots 0-2047 start moving to shard 3 (shards count from 0)
RUVIO MOVESLOTS STATUS
# state copying, keys 120544, ...  then cleaning, then done
CLUSTER SLOTS
# 0-2047 now on shard 3`,
    body: "Both shards keep serving the range while it moves. The source copies it in bounded steps, sends the keys written meanwhile again, and hands over in one short step; clients that still route to the old shard are forwarded, and pick up the new owner on their next slot map load. With a log the move survives a crash at any step, and the old shard's <code>CHANGES</code> feed says where the range continues.",
    note: "Slots move between the shards of one server, one move at a time, and not while replication is on. The hand-over pauses the source shard briefly (well under a millisecond without a log, around 15 ms with one on a Mac, mostly the log sync). A server that moved slots writes WAL records an older release cannot read.",
    refs: [{ label: "RUVIO MOVESLOTS", href: commands }, { label: "Moved slot ranges", href: "/docs/recovery/#moved-slot-ranges" }],
  },
  {
    id: "multi-node",
    nav: "Spread over machines",
    title: "Spread the slots over several machines",
    problem: "One machine is not enough for the writes or the data.",
    code: `# topology.toml, the same on every node
version = 1

[[groups]]
name = "a"
address = "10.0.0.5:7000"
shards = 4
slots = ["0-8191"]

[[groups]]
name = "b"
address = "10.0.0.6:7000"
shards = 4
slots = ["8192-16383"]

# on 10.0.0.5: cluster_topology = "topology.toml", cluster_group = "a"
GET user:1      # a key of group b, sent to 10.0.0.5
# -MOVED 10778 10.0.0.6:7001`,
    body: "Each group (a primary and its replicas) serves the slots the map gives it, split over its own shards, and answers <code>MOVED</code> with the owning shard's address for every other slot, so cluster clients go straight to the right machine. <code>CLUSTER SLOTS</code> describes the whole map on every node. With <code>ruvio coordinator</code> managing the groups, every node gets a newer map after a failover, so <code>MOVED</code> names the new primary.",
    note: "The map is fixed when the cluster is created: slots do not move between machines yet, and a node that holds keys of slots the map serves elsewhere does not start. Keyless commands, <code>WAIT</code> and classic <code>PUBLISH</code> stay on the node that receives them; use <code>SPUBLISH</code> for channels the whole cluster shares.",
    refs: [{ label: "Multi-node settings", href: "/docs/configuration/" }, { label: "Coordinator", href: "/docs/coordinator/#a-multi-node-cluster" }],
  },
  {
    id: "data",
    nav: "Leaderboards, geo, JSON",
    title: "Leaderboards, geo and documents",
    problem: "The data types you already use, plus JSON documents with an exact-match index.",
    code: `ZADD leaderboard 1200 ada 980 grace
ZRANGE leaderboard 0 9 REV WITHSCORES
# cities was filled earlier with GEOADD
GEOSEARCH cities FROMLONLAT 13.36 38.11 BYRADIUS 50 km COUNT 10
JSON.SET user:7 $ '{"plan":"pro","country":"TR"}'
JSON.INDEX CREATE by_plan PREFIX user: PATH $.plan TYPE string
JSON.FIND by_plan VALUE pro COUNT 20
# ERR index is building -> the first build runs in the background; retry`,
    body: "Strings, lists, sets, hashes, sorted sets, streams, geo, Bloom filters and JSON are covered. <code>JSON.INDEX</code> is an exact-equality index on one scalar field; it works on database 0 and keeps its postings in memory.",
    refs: [{ label: "Data types", href: "/docs/data-types/" }, { label: "Commands", href: commands }],
  },
];

/** Short name of a use case, for links from other pages. */
export const caseName = (id: string) => cases.find((c) => c.id === id)?.nav ?? id;
