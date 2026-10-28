---
name: ruvio-performance-rules
description: Use whenever code is written or changed in the Ruvio server, a client or a .NET integration; states the speed, memory and modern-code rules a change must keep.
---

# Performance, memory and code standards

Ruvio is a performance product. Every command is at least as fast as Redis and uses no more
memory than Redis. A command that falls below 0.9x of Redis is a bug to work on. The rule
covers the clients and integrations too: a cost a client adds to every command is paid by the
user even when the server is fast. `docs/PERFORMANCE.md` and the hot-path section of
`CONTRIBUTING.md` are the reference; this skill is the checklist.

## 1. The decision rule

- A change stays only when it does not make a command slower and does not raise RSS for the
  same data. Correctness is required; a slower or larger result is not an improvement.
- One noisy sample is neither a regression nor a win; repeat it (`ruvio-measure` skill).
  macOS numbers are not evidence for x86 Linux.
- An unused feature costs nothing. What cannot be measured here is written down as
  "not measured", not chased through CI runs.
- A large structural change (hot path, persistence format, shard model, multi-file refactor)
  starts with a short impact analysis that the maintainer approves before coding. Refactor
  only where the hot path is clearly not touched.

## 2. Server (Rust)

- **Hot path (GET/SET/INCR):** with a feature off, no extra index lookup, no second copy, no
  lock another shard needs. `Value` is at most 24 bytes; `Entry` does not grow.
- **Allocation:** no needless `Vec`, `String`, `clone`, loop or scan on a command path.
  Arguments are borrowed from the input frame; replies are written into the output buffer.
- **New state:** an `Option<Box<_>>`, a side table or a boxed `Value` arm, created on first
  use. The size ceiling is not raised.
- **Bounded work:** work that grows with N is capped by a COUNT, done incrementally or moved
  off the shard. One call never walks the whole keyspace.
- **One representation:** a collection or reply lives in one form; a second index raises RSS.
- **Architecture:** one owner per shard in an evented process. Cross-shard work is a message.
  No second runtime, no `io_threads`, no io_uring (measured and rejected). A write is recorded
  before it is applied; a rejected write does not reach the WAL. `PUBLISH` never stalls a shard.
- **Rejected designs** (do not retry without a new measurement that beats the current layout):
  the list under "Changes that were measured and rejected" in `docs/PERFORMANCE.md`.
- **Proof:** `cargo test --test zero_alloc` (0 allocations for GET/SET/INCR/DEL+SET on a warm
  shard), `cargo test --test layout` (size ceilings), and on Linux the instruction-count bench
  (`cargo bench --bench hot_path`, +2% gate). Without valgrind, write "not measured".
- **Modern Rust:** edition 2024, a `// SAFETY:` comment on every `unsafe` block, clippy clean,
  `cargo fmt`. New `unsafe` only in a measured bottleneck.

## 3. Clients (all of them)

Design rule: multiplexing over one connection, coalesced writes, pre-encoded command names,
in-place parsing, zero runtime dependencies.

- A command frame is written from pooled or stack memory; the command name is not re-encoded
  on every call.
- No intermediate array or list on the single-key GET/INCR path (no `string[]` in C#).
- A reply is converted to the needed type once, with no intermediate copy.
- Socket operations have timeouts; after a read timeout the connection is marked broken so
  replies cannot shift.
- Cluster routing computes the slot from the command's key positions (the generated key
  tables from `clients/testdata/commands.json`).

| Language | Baseline | Modern standard |
| --- | --- | --- |
| Rust (`clients/ruvio-client`) | edition 2024, MSRV 1.87 | clippy clean, borrowing, errors as `Result` |
| C# (`clients/csharp`, `integrations/dotnet`) | net8.0, nullable on, AOT compatible | `Span<T>`/`Memory<T>`, `ArrayPool`, `ValueTask`, source generators, no reflection, no NativeAOT JSON warnings |
| Go (`clients/go`) | go 1.22 | `context.Context` first, `sync.Pool`, no dependencies, `go vet` clean |
| Java (`clients/java`) | release 17 | `CompletableFuture`, records, `ByteBuffer`, no needless boxing |
| Python (`clients/python`) | 3.9+ | type hints and `py.typed`, the same API for sync and asyncio, `bytes`/`memoryview` |
| Node/TypeScript (`clients/node`) | Node 18+, ES2022 | strict TypeScript, pooled `Buffer`s, ESM, `async`/`await` |

## 4. Tests

- Every testable function is tested in every case: correct reply, error, boundary, no
  connection.
- Names: Rust and Python snake_case, Go `TestXxx`, C# PascalCase, Java camelCase, Node
  descriptive sentences.
- No test hangs or takes minutes (`ruvio-test-reliability` skill).

## 5. Not done if

- `zero_alloc` or `layout` is red;
- the hot path gained a branch or lookup for a feature that is off;
- a client added an allocation per command or a runtime dependency;
- a change that needs a measurement is described as "faster" without one.

## Example

Adding GETEX to the C# client: the command name is pre-encoded through
`clients/csharp/Ruvio.Client/RespVerbs.txt`, the frame is written from `ArrayPool`, the method
returns `ValueTask<byte[]?>`, and PascalCase tests cover the reply, a missing key and
cancellation.
