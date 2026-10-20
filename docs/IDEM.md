# Native idempotency (`IDEM`)

`IDEM` coordinates bounded request deduplication on the shard that owns a
key. It is a native command, not a Lua script or an HTTP-only feature. `ONCE`
remains the lighter TTL marker and its behavior is unchanged.

## Wire contract

```text
IDEM key BEGIN fingerprint owner ttl_ms
IDEM key COMPLETE fingerprint owner result
```

The key is always argument 1, so normal single-key hash-slot routing applies.
Actions are case-insensitive. Fingerprint and owner are opaque, binary-safe
values of 1-128 bytes. The server compares them; it does not compute a request
fingerprint or authenticate an owner token. Use a stable cryptographic digest of
all relevant request inputs, an unguessable per-attempt ownership token, and a
key scoped to the authenticated tenant/principal and operation.

`BEGIN` requires an integer retention of **1-604800000 milliseconds** (7 days).
The reply is a RESP array of bulk strings:

| Reply | Meaning |
| --- | --- |
| `["acquired"]` | Key was missing/expired; this attempt owns a new pending record. |
| `["pending"]` | Same fingerprint, but the operation has not completed. Do not run it again, even if the owner token matches. |
| `["conflict"]` | The live record has a different fingerprint. |
| `["completed", result]` | Same fingerprint; replay the saved result bytes, including an empty result. |

Only the acquired case writes anything. Every other case leaves the record and
original deadline unchanged; passing a longer TTL on retry does not extend it.
The fingerprint check also applies to completed records.

`COMPLETE` returns integer **1** when it publishes the result for a live pending
record with the exact owner and fingerprint. An identical repeat of a completed
record (same owner, fingerprint and result bytes) also returns 1 without a write.
It returns **0** for missing/expired records, owner/fingerprint mismatch, or an
attempt to replace an already completed result. There is no automatic takeover,
abort/delete, renewal, or overwrite action. Completion never extends retention.

The result is binary-safe and limited to **4 MiB**, including any serialization
the caller adds. The configured protocol value limit must also accommodate the
stored record: result bytes + token lengths + 11 bytes. Invalid syntax, bounds,
wrong types, malformed records, or a record whose TTL was removed are errors;
they do not silently create a new operation.

Example (illustrative tokens; generate secure tokens in applications):

```text
IDEM order:42 BEGIN request-digest attempt-token 60000
# ["acquired"] -- execute the operation once
IDEM order:42 COMPLETE request-digest attempt-token saved-result
# 1
IDEM order:42 BEGIN request-digest another-attempt 60000
# ["completed", "saved-result"]
```

## Memory, execution and durability

On the evented server, native IDEM/LIMIT commands participate in the same
WAL group-commit barrier as hot writes. With `always`, buffered successful
responses are flushed only after synchronization succeeds; concurrent ready
clients may share a sync. Grouping changes scheduling, not retention, atomicity
or the durability contract.

The evented runtime uses a central, allocation-free eligibility rule for both
hot and general parsing paths. The reviewed hot variants and standalone native
LIMIT/IDEM operations are eligible; transaction and Pub/Sub sessions are not.
Other general-parser commands close the group before dispatch by default.
Eligibility is execution-path-specific, not inferred from a command's name or
write flag. Adding a command requires reviewing blocking, rollback and reply
ordering; this rule does not automatically extend grouping to all writes.

No fields are added to `Entry`, `Value`, the shard, or the ordinary hot-command
parser. There is no global idempotency map, owner registry, background worker,
per-key timer, or script VM invocation. Keys that never use this feature carry
no idempotency metadata. Existing expiry and memory admission/eviction mechanisms
are reused.

A used key is one packed string record: 11 header bytes, fingerprint, owner, and
only after completion the result. With the HTTP provider's 64-byte hex digest and
32-byte owner, the pending payload is 107 bytes, plus ordinary key/value and
deadline storage. There are no per-field hash names, JSON or Base64 in the server
format. `TYPE` reports `string`; `GET` exposes the internal record, not just the
result. Reserve this namespace exclusively for this command. `SET`, `DEL`,
`PERSIST`, expiry edits and other ordinary writes can destroy its guarantees.
Use ACL key restrictions and `+idem` to limit access; `@write` also includes
the command.

Transitions are O(token length + result size); pending/conflict probes compare
bounded tokens without copying a stored result. A completed `BEGIN` borrows result
bytes directly for response encoding. Publication builds one packed buffer and
uses the existing byte-store write path; durable mode has its normal WAL buffers.
Wire output still incurs transport costs. Identical `COMPLETE` compares the result
but does not allocate a new stored record or emit another WAL mutation.

Both creation and completion record the value **and absolute deadline together**
in canonical `IdempotencyPut` WAL opcode **53**, before changing the shard.
Replication uses that same record; recovery preserves owner, state, result and
deadline. A torn completion cannot recover as a completed record without its TTL.
Snapshots use the existing string/deadline format. Database isolation, same-slot
transactions, snapshot journaling, write admission and eviction still apply.
Upgrade servers, replicas and recovery tools before issuing this command: old
readers do not understand opcode 53. Existing WAL opcodes are unchanged.

## What it does not guarantee

This is **not exactly-once execution in an external database, payment service or
message broker**. A process may perform its side effect and crash before
`COMPLETE`, leaving pending state. A handler can outlive retention. Eviction,
operator deletion, persistence loss or asynchronous replica failover can admit
the operation again or lose a completed result.

Use retention longer than the operation/retry horizon, appropriate durability,
non-evicting state and sufficient capacity. Irreversible effects still require
downstream deduplication, transactions or an outbox using a durable operation ID.
A lost BEGIN reply must not cause the business operation to be re-executed.
A lost COMPLETE reply is uncertain; a later BEGIN can retrieve the result if it
committed. Clients do not automatically retry ambiguous writes.

## Across a failover

`IDEM`, `ONCE` and `LIMIT` need no extra mechanism. Their state is written to the
log and shipped to replicas like any write, so what a replica holds is what the
primary had shipped. Replication is asynchronous: without durability, the last
writes before a primary dies can be missing on the new primary. Then an `ONCE`
key may be gone (the action can run twice), a `LIMIT` window may miss a few
counts (usually acceptable), and an `IDEM` entry or its result may be gone (the
request runs again). A connection that writes these commands with
`CLIENT DURABILITY REPLICAS 1` (or more) gets its replies only after that many
replicas hold the writes on disk, and `ruvio coordinator` promotes only a
replica that has every such write; see [COORDINATOR.md](COORDINATOR.md). On
`-NOREPLICAS` the write is applied but not confirmed: treat it as uncertain.
Durability is not on by default, because every reply then waits for a replica;
turn it on for the connections where a lost write matters.

The deadlines of these commands, and of `LEASE` and `SEMAPHORE`, are absolute
wall-clock times (Unix milliseconds) stored in the log. A new primary whose clock
is behind the old one's extends holds and windows; one that is ahead ends them
early. Keep the clocks of the nodes synchronized (NTP or similar).

A lock that stays safe across a failover:

1. `LEASE` on a connection that sent `CLIENT DURABILITY REPLICAS 1`. Tokens are
   fenced: a later holder always gets a larger token, also after a failover.
2. The holder sends its token with every write to the protected resource.
3. The resource remembers the highest token it has seen and rejects a lower one.

## Clients and HTTP integration

C# exposes `IRuvioClient.BeginIdempotencyAsync` (raw RESP state/result) and
`CompleteIdempotencyAsync` (boolean acknowledgment). The latter accepts a text
result; use `ExecuteBinaryAsync` with the wire command for arbitrary result/token
bytes. Rust exposes `Client::idempotency_begin` and `idempotency_complete` for text
arguments, returning a raw RESP result and integer acknowledgment respectively.
Other RESP clients can send the native command without loading scripts.

`Ruvio.AspNetCore.Idempotency` now calls these native methods. It continues to own
authentication scope, HTTP fingerprinting, bounded request/response capture and
response serialization. The server remains HTTP-agnostic. See the package README
for authorization ordering and response replay limits.

The earlier, unpublished Lua implementation stored hashes. The HTTP key prefix
is deliberately retained rather than silently starting a second deduplication
domain. Native commands fail closed on those old hash records; they do not
overwrite or migrate uncertain operations. For an existing deployment, stop new
admissions and drain the old handlers/retry retention before switching all
instances. Do not delete keys or change namespace merely to bypass this barrier.
There is no fallback to Lua on older servers.
