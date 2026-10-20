# Ruvio Product Roadmap

Ruvio's target is a production-grade, Redis-compatible, shared-nothing data
store: memory-first for low latency, with configurable on-disk durability and a
clear recovery story. Redis protocol compatibility is a means to ecosystem
compatibility; identical internals are not a goal.

This document lists the open work. Finished work is in the changelogs
([server](../CHANGELOG.md) and each package's `CHANGELOG.md`) and in
[COMPATIBILITY](COMPATIBILITY.md); the roadmap as it stood with its finished
items is in git history (`git show e6eb171:docs/ROADMAP.md`). A finished item
leaves this file in the change that finishes it, with its acceptance tests and
a note on whether it changes wire, persistence, or operational compatibility.

Every change follows [Performance and memory](PERFORMANCE.md). A change that
slows a command or raises RSS for the same data does not land.

## Priorities

- `P0` required before a production preview
- `P1` required before a stable standalone release
- `P2` required for a production cluster
- `P3` later work that does not gate standalone or cluster

## Opt-in document and search features

- [ ] **Hybrid large-document JSON representation, conditional on measured gains.**
  Keep the compact bytes for small documents and non-JSON keys unchanged. An
  isolated October 2026 prototype compared the existing compact path code with
  a parsed `serde_json::Value` tree (three in-process timing runs; RSS from
  separate processes). On a 1 MiB document with one large string, existing-path
  updates ran at ~14k/s on compact bytes, ~20M/s on the tree *without* a WAL
  record, but only ~3.9k/s when the tree was serialized in full per update.
  For a 144 KiB document with 4,000 fields, 465 copies consumed ~67 MiB as
  compact bytes versus ~292 MiB as trees. These are component measurements,
  not server throughput or a production memory guarantee. A size-only switch
  with the existing full-document `JsonPut` is therefore not acceptable.
  The current implementation keeps small JSON compact. Above 32 KiB (lowered from 256 KiB after the 2026-10-09 Mac run, see J2),
  documents with at most 128 indexed paths and a large string retain one
  compact body plus small scalar field patches (up to 64 patches of at most
  8 KiB each before rebuilding). Unchanged root reads borrow
  the original body; patched root reads stream its spans. Scalar path reads and
  writes use internal byte-range indexes (not user-facing search indexes).
  Structural edits rebuild the body;
  documents not eligible for compact indexing use the bounded tree fallback.
  Both variants record validated path-result WAL mutations. The original tree
  design measured 1 MiB field SET at ~1.11M/s versus ~13.6k/s on the compact
  baseline and ~373k/s on RedisJSON, but its root GET was ~2,303/s versus
  ~6,250/s compact (RedisJSON ~2,347/s) and RSS increased ~84,640 KiB for
  64 documents versus ~77,104 KiB compact. Wide 140 KiB documents remain
  compact; 64 KiB documents remain slower than RedisJSON.
  Everysec/always restart smoke tests pass on the final release binary. A
  missing replica path target is rejected before WAL append and requests a
  primary checkpoint; catch-up replays pinned WAL from that checkpoint to
  an explicit watermark with expiration deferred and replica requests temporarily
  returning `LOADING`. Mixed transactions, expiry followed by PERSIST, restart,
  and catch-up while the primary keeps writing have targeted tests; WAL history
  is read in bounded batches. Interrupted catch-up now leaves a durable marker:
  restart retries a fresh snapshot or fails closed, and incomplete manual
  promotion needs explicit one-shot `--force-promote-incomplete`. Single- and
  multi-shard recovery and forced writer startup have targeted tests. Direct
  tree-to-RESP serialization alone only reached ~2,485/s root GET and
  ~84,608 KiB RSS versus ~6,098/s and ~77,088 KiB on the compact baseline.
  Indexed compact storage instead reached ~6,098/s root GET versus ~6,173/s
  compact and ~76,496 KiB RSS versus ~77,088 KiB (64 × 1 MiB, 3 rounds,
  25 clients, pipeline 8). After 50 field patches, root GET measured ~5,102/s
  in one 500-request sample. In separate 100,000-request runs, field SET
  reached ~1.10M/s versus ~13.3k/s compact; field GET ~1.27M/s versus
  ~16.3k/s. Five 100,000-request plain GET/SET rounds showed no clear
  non-JSON regression. These same-machine results are not a production
  guarantee. A two-process
  `everysec` deployment smoke test (`cargo build --release` then
  `python3 scripts/json_replica_backlog_smoke.py`) compacted the replica's old
  WAL cursor, recovered from snapshot sequence 2,001 through >38 MiB of
  retained WAL, and converged at sequence 47,001 while the primary accepted
  another 10,000 JSON path writes (~16 s on the test machine). The script
  verifies final document content and clearance of catch-up markers. Leave
  acceptance open for representative indexed and tree-fallback mixes, repeated
  structural edits, longer durability/replication trials, and sustained
  slow-replica/disk-retention limits: a lagging replica can prolong primary
  disk retention without a fixed cap. The [2026-10-07 RedisJSON comparison](evidence/2026-10-07-benchmark-results.md#native-json-vs-redisjson-2026-10-07)
  on build `7aebdaf` confirms that 1 MiB scalar field GET/SET and root GET
  outpace RedisJSON for the tested shape, but 64 KiB field operations trail it
  by roughly 2–3× and 1 MiB structural `JSON.MERGE` is roughly 72× slower
  because it rebuilds the document. Prioritize representative fallback and
  structural-edit workloads, transient peak RSS, and longer replication and
  slow-replica retention trials before closing this acceptance item.
  Remaining work, ordered by the measured gaps (the benchmark above is the
  baseline, not a universal RedisJSON compatibility or performance target):
  - [ ] **P0 — Profile and improve mid-size field operations.** For the tested
    64 KiB shape, GET/SET/MERGE reach ~196k/~135k/~139k requests/s versus
    RedisJSON ~435k/~417k/~455k. Profile compact path scanning and copying;
    evaluate a measured, shape-aware index or patch threshold rather than
    enabling indexes for every JSON value or lowering 256 KiB unconditionally.
    Re-run paired throughput and p99 on 1 KiB, 64 KiB, threshold-adjacent,
    1 MiB, and wide/array-heavy documents. Preserve small-document wins,
    correct path semantics, and no JSON overhead on ordinary keys.
  - [ ] **P0 — Make structural edits cheaper without regressing scalar paths.**
    The 1 MiB nested `JSON.MERGE` runs ~5.6k/s versus RedisJSON ~400k/s
    because it rebuilds the body; optimize affected subtrees/spans where
    semantics permit instead of assuming every merge can be a scalar patch.
    Include root and nested merge, deletion via RFC 7396 null, missing paths,
    arrays, repeated edits beyond the 64-patch cap, and fallback documents.
    Compare throughput, p99, root reads, and peak RSS against this baseline.
    Keep the existing atomic, replayable path-result WAL and replica behavior.
  - [ ] **P0 — Validate the hybrid boundary and mixed workloads.** Exercise
    eligible indexed documents alongside tree fallback, small JSON, and
    non-JSON GET/SET and other hot commands, both with and without JSON data;
    measure throughput, tail latency, steady and transient RSS, and the cost
    of patch compaction and repeated structural edits. Include many keys,
    multiple shards, large payloads, TTL/eviction, transactions, and restart.
    Do not treat the 20,000-key RSS sample or a single hot document as a
    general memory/performance guarantee.
  - [ ] **P1 — Extend durability and replica acceptance.** Run longer
    `everysec`/`always` WAL and snapshot/restart trials with mixed JSON writes,
    structural edits, and a writer that continues during catch-up; compare
    primary and replica content, sequence, TTL, and recovery after interruption.
    Preserve fail-closed loading and the explicit incomplete-promotion guard.
    Measure prolonged slow-replica WAL retention and disk use, then define and
    test a bounded retention/backpressure/resync policy before promising a
    disk bound; never silently promote an incomplete replica.
- [ ] **Selected JSON field indexes.** Each explicitly named index selects a
  binary key prefix and one simple JSON path. The first version supports exact
  equality of scalar JSON values only (no ranges, wildcard paths, or implicit
  indexing). Queries return bounded pages with a continuation cursor, not
  unlimited result sets. They must aggregate results across shards; a
  shard-local result alone must not be presented as a complete result. Persist
  index definitions, rebuild derived postings after WAL/snapshot restoration, and
  update them for changes, expiry, eviction, overwrite, delete, and rename.
  Unindexed keys must have no per-key index/metadata overhead.
- [ ] **Full-text search.** Build on the opt-in indexing lifecycle rather
  than scanning all documents or automatically indexing every string.
  Specify tokenization and consistency before adding commands.

## Milestone 2 — Persistence and Recovery

### Durable Log

- [ ] **P1 — Prototype bounded, sequence-based group commit without blocking
  the shard executor on disk synchronization.**
  - Motivation: standalone native LIMIT/IDEM now share the hot-write durability
    barrier, but `always` still synchronizes on the shard runtime thread. The
    remaining concurrent GET/SET gap is not claimed fixed. Establish the
    disk-wait contribution before committing to this architectural change.
  - Preserve per-shard mutation ordering and the central execution-path/session
    grouping eligibility rule. Do not infer eligibility from a write flag;
    review blocking, script rollback, transaction and administrative boundaries.
  - Prototype behind an opt-in configuration with the current implementation
    remaining the default. Initial sync-only prototype:
    `sync_worker` (see CONFIGURATION.md). It offloads grouped sync
    waits only; append, fallback and rotation remain synchronous. This backlog
    item stays unchecked until the broader safety and measurement gates pass.
    Short repeated local/Linux evidence is recorded in
    [the scenario results](evidence/2026-10-03-dotnet-scenario-lab.md#experimental-sync-worker-isolated-repeated-linux-probe):
    Linux concurrent throughput/tails improved, while macOS p99 regressed.
    Defaults remain unchanged; full client/API and cross-platform gates are open.
  - Use a long-lived persistence worker with explicit file/buffer ownership,
    reusable buffers and bounded handoff. Do not spawn a thread, task, timer or
    allocate a completion object per command. Determine worker count from
    measurements rather than automatically adding one worker per connection.
  - Publish a monotonic durable sequence only after successful synchronization.
    Pending response batches carry their required sequence; release them only
    when it is durable. Reads observing unsynchronized mutations must obey the
    same observed-write durability contract, not only mutation replies.
  - At low load, submit promptly without an artificial batching delay. Under
    load, coalesce ready writes naturally while I/O is in progress. Bound bytes,
    batches and outstanding acknowledgements; apply explicit backpressure when
    disk cannot keep up, including slow-client/output-buffer limits.
  - Integrate append-before-apply safety, segment rotation, checkpoints,
    replication durable offsets, expiry/eviction and database isolation. Never
    hold a shard borrow or persistence mutex across a worker wait. Specify what
    remains shard-owned versus worker-owned; offloading fsync alone must not
    leave append blocked by a worker-held lock.
  - Surface write/sync/worker failures and fail pending acknowledgements without
    success-shaped replies or ambiguous write replay. Define disconnect,
    cancellation and graceful shutdown/drain behavior with bounded deadlines.
  - Add concurrency-model tests for publication/wakeup races and fault-injection
    tests for partial writes, sync failure, disk full, rotation, worker failure,
    shutdown and crash/restart. Cover write-then-read visibility, scripts,
    transactions and native commands across both runtime backends.
  - Measure against the unchanged default on the same Linux host: one and many
    clients, pipelines, GET/SET/INCR/DEL, native IDEM/LIMIT and reviewed fallback
    commands; test `memory`, `everysec` and `always` separately. Record p50/p95/p99,
    achieved throughput, fsync/batch sizes, queue bytes, allocations, CPU, RSS
    and GC where clients apply. Include a fresh Redis AOF-always reference.
  - Acceptance: preserve durability/error behavior, bounded memory and the
    memory-mode hot path; demonstrate repeatable concurrent durable gains with
    no material single-client regression. If gains do not justify complexity,
    keep the default and do not promote the prototype. Set numeric regression
    budgets from baseline measurements before implementation.

## Milestone 3 — Memory Lifecycle and Cache Semantics

### Memory Limits and Eviction

- [ ] **P0 — Bring steady-state string RSS into the same band as Redis for the
  same keyspace.**
  Short runs since the 2026-09-28 Cherry soak are below Redis (total RSS
  0.77× for 16-byte and 0.84× for 128-byte strings in
  [the 2026-10-11 comparison](evidence/2026-10-11-azure-command-comparison.md#memory)).
  The 15-minute four-core soak that found the gap
  ([record](evidence/2026-09-28-string-memory-density.md)) has not been
  repeated, so the long-running cache claim stays blocked until that key mix
  is measured again.
- [ ] **P1 — Next cycle is throughput, then only the memory still outside the
  band.**
  Do not start another RSS rewrite before a `SET`/`GET`/`INCR` pass on this
  layout. A first native, single-shard, memory-only pass on 2026-10-07
  (`python3 scripts/redis_compare.py --tests set,get,incr --rounds 3
  --memory-keys 100000`) measured Ruvio medians of 2.54M SET, 2.72M GET,
  3.19M INCR/s versus local Redis 2.07M/2.28M/2.26M; 100k distinct
  64-byte strings added 8.7 MiB versus 13.4 MiB RSS. Samples varied
  substantially, and the benchmark uses a small fixed keyspace for commands;
  this is a **baseline**, not evidence of a new optimization or a reason to
  close the separate long-running soak gap. A follow-up paired LRU read probe
  with 100k distinct keys and concurrent random GET/SET PX churn found a
  small improvement after eliminating a redundant live-key lookup; its
  measurements and limits are in `docs/evidence/2026-10-07-benchmark-results.md`.
  Broader key/value distributions, durability modes and longer runs still
  need profiling. A packed record log stays a later option if the repeated
  soak is still above about 1.5× Redis. Docker Desktop numbers are not that soak.

## Milestone 4 — Redis Compatibility for Real Workloads

- [ ] **P3 — Evaluate Pub/Sub and Streams as separate product milestones.**
  Sharded Pub/Sub is in: `SPUBLISH`, `SSUBSCRIBE`, and `SUNSUBSCRIBE` stay on
  the channel's slot owner and are not durable. Exact keyspace notices for
  `SET`, `DEL`, expire, and eviction ride that queue when
  `notify-keyspace-events` is on and a client is subscribed. Classic
  `PUBLISH` / `SUBSCRIBE` and `PSUBSCRIBE` stay on the shard that accepted
  the connection. They are not broadcast and not durable. A sync delivery
  wait is not implemented. A narrow stream slice is in:
  `XADD`, `XLEN`, `XRANGE`, `XDEL`, and `XTRIM`, with the assigned id stored
  in the WAL. `MAXLEN ~` and `MINID ~` are exact. The approximate trim option `LIMIT` is not accepted.
  `XREAD BLOCK` waits on the shard.
  `XGROUP CREATE`, `XGROUP CREATECONSUMER`, `XGROUP DESTROY`, `XGROUP SETID`,
  `XGROUP DELCONSUMER`,
  `XREADGROUP` with `>`, `NOACK`, `XACK`, `XPENDING`, and `XCLAIM`
  keep the group cursor in the WAL and in snapshots. `XPENDING` can list
  the pending rows. `XCLAIM` accepts `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`,
  `JUSTID`, and `LASTID`.
  `XREADGROUP BLOCK` waits for a later `XADD` and delivers that id.
  `XINFO STREAM`, `XINFO GROUPS`, and `XINFO CONSUMERS` read that stored
  stream. They do not add a counter on `XADD`. `XSETID` moves the last
  generated id forward without storing an entry. `XADD DELAY` keeps the
  entry out of `XREADGROUP` until the delay passes, and stores that hold
  in the WAL and in snapshots. `LEASE` / `RELEASE` and `SETV` keep their
  token or version only on keys that use them. `CHANGES START` arms an
  in-memory note ring on that shard; nothing is recorded before it, and
  the ring is empty after restart.

## Milestone 5 — Storage Engine Performance

- [ ] **P0 — Establish repeatable bare-metal Linux baselines with isolated
  server and load-generator CPUs.**
  Blocked: no dedicated bare-metal host in CI. Methodology lives in
  `docs/BENCHMARKING.md`; container results are not release evidence.
- [ ] **P0 — Record throughput, p50/p95/p99/p99.9, CPU, RSS, allocations, cache
  misses, context switches, and saturation behavior.**
  Blocked on the same bare-metal host. The regression harness records
  throughput and selected percentiles on whatever machine it runs.
- [ ] **P1 — Profile realistic key/value distributions, churn, TTL, and mixed
  commands, not only fixed-size GET/SET.**
- [ ] **P1 — Evaluate shard-local open-addressing indexes against `AHashMap`.**
  Do this after the string entry is small. Packing today's `Entry` tighter
  does not remove the `Value` enum or the extra heap blocks. See
  `docs/evidence/2026-09-28-string-memory-density.md`.
- [ ] **P1 — Evaluate arenas/compact handles while preserving deletion and
  snapshot correctness.**
- [ ] **P1 — Extend bounded pooling to collection elements only when profiles
  justify the complexity.**
- [ ] **P1 — Remove avoidable Bloom-filter and sorted-set hot-path allocations.**
- [ ] **P1 — Integrate SIMD only into measured production bottlenecks with
  scalar and architecture-specific conformance tests.**
- [ ] **P1 — Evaluate registered/fixed buffers, batching, and queue depths on
  supported Linux kernels.**
- [ ] **P1 — Define performance regression budgets and enforce stable
  microbenchmarks in CI without relying on noisy absolute throughput.**
- [ ] **P3 — Evaluate a custom binary protocol separately from Redis
  compatibility; do not complicate RESP paths without a concrete use case.**

## Milestone 6 — Security and Multi-Tenancy Basics

### Exposure hardening

- [ ] **P3 — Identity-provider authentication.** Garnet `Aad` and `AclWithAad`.
  After stable standalone. Do not block the RESP password and ACL path on it.
- [ ] **P3 — Encrypt the data directory at rest.** This is a data-directory format change.
  Host permissions remain the control until then.
- [ ] **P2 — Authenticate a future cluster bus before it exists.** Garnet
  `--cluster-username` and `--cluster-password`. Design the identity with the
  bus, not after it.

## Milestone 7 — Production Cluster

### Membership and Routing

- [ ] **P2 — Define authenticated internal cluster protocol and stable node
  identities.**
- [ ] **P2 — Add cluster membership, epochs, topology persistence, and
  convergence rules.**
- [ ] **P2 — Implement complete `CLUSTER SLOTS`/`NODES` semantics and a real
  cluster bus endpoint.**
- [ ] **P2 — Add online slot migration with importing/migrating states and
  bounded transfer queues.** Between the shards of one process this is done:
  `RUVIO MOVESLOTS` (#50). Between machines: `COORDINATOR MOVE` (#23).
- [ ] **P2 — Add hot-key and skew observability before automated rebalancing.**
- [ ] **P2 — Define cross-slot command behavior; do not hide distributed
  coordination behind local-looking semantics.**

### Replication and Failover

- [ ] **P2 — Establish full cluster synchronization and node replacement.**
  Covered-gap checkpoint transfer is not an unconditional full-sync guarantee
  for any replica state or a way to join/migrate cluster members online.
- [ ] **P2 — Add complete replication lag, backlog, reconnect, and disk-pressure
  controls.** Per-shard send lag, a disk WAL backlog and reconnect exist; the
  remaining operational controls and failure evidence are not complete.
- [ ] **P2 — Define consistency and acknowledgement policies for asynchronous
  and quorum-backed writes.**
- [x] **P2 — Add failure detection and leader/primary election with a documented
  split-brain model.** `ruvio coordinator` (three or five, Raft): majority-confirmed
  detection, failover with `REPLICAOF`, the old primary demoted on return; split
  brain bounded by `min-replicas-to-write` and the epoch. See
  [COORDINATOR.md](COORDINATOR.md).
- [ ] **P2 — Prove failover with deterministic fault injection, partitions,
  delayed packets, process crashes, and disk loss.**
- [ ] **P2 — Add rolling-upgrade and mixed-version compatibility tests.**

## Milestone 8 — Packaging and Release Readiness

- [ ] **P1 — Publish benchmark methodology and repeated bare-metal results.**
- [ ] **P1 — Define beta/stable release gates, incident policy, and support
  boundaries.**

## Milestone 9 — Ruvio UI

- [ ] **P3 — Publish host executables.** Build the same binary locally for
  Linux, macOS, and Windows. No shell and no Redis CLI. The server's own
  binaries are already attached to GitHub Releases; the UI is not yet.

## Release Gates

### Production Preview

- [ ] All `P0` standalone, persistence, integrity, expiration, memory, security,
  and packaging items are complete.
  Remaining hardware-only P0s: full-disk and permission fault injection, and a
  24-hour soak, and the repeated string soak in Milestone 3. The Cherry EPYC
  `kill -9` SET harness and the single-host performance record are in
  [the bare-metal record](evidence/2026-09-28-bare-metal.md). See
  [RELEASE_EVIDENCE](RELEASE_EVIDENCE.md).
- [ ] No known acknowledged-write loss in crash testing under the documented
  durability contract.
- [ ] Twenty-four-hour mixed-workload soak passes with bounded memory.
  Blocked on a 24-hour dedicated soak host.
- [ ] Performance is measured on bare-metal Linux and published without
  cherry-picked runs.

### Stable Standalone

- [ ] All `P1` items selected for the release are complete.
- [ ] Security review and operational failure drills are complete.
  Unknown names, wrong arity, `CONFIG RESETSTAT`, `SHUTDOWN`, `DEBUG`, and
  `CLUSTER MEET` reply with an error and keep the socket on the evented
  runtime. Process-kill, full-disk, and live permission drills stay
  blocked on a dedicated host.
- [ ] Tail-latency and recovery objectives are documented and met.

### Stable Cluster

- [ ] All `P2` membership, migration, replication, and failover items are
  complete.
- [ ] Partition and split-brain behavior matches the published consistency
  model.
- [ ] Rolling upgrades, node replacement, resharding, and disaster recovery
  pass automated fault-injection suites.
