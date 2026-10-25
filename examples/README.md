[Ruvio](../README.md) / Examples

# Examples and probes

Small programs that exercise one part of the server in-process or against a running instance. They are for contributors; the client examples are on the [site](https://ruviodb.com/examples/).

| File | What it measures or shows |
| --- | --- |
| `02_simd_search.rs`, `03_zero_copy.rs` | SIMD search and zero-copy parsing experiments |
| `04_store_smoke.rs` | Basic `KvShard` operations |
| `05_eviction_pressure.rs` | In-process eviction pressure |
| `06_expiry_backlog.rs` | In-process expiry backlog |
| `07_expiry_live.py`, `08_expiry_sustained.py` | Live expiry probes against a built server |
| `09_building_blocks_tour.py` | A few-second tour of `LEASE`/`RENEW`, `LIMIT`, `IDEM`, `XADD DELAY`, `ZMOVEDUE`, `SET IFGT` and `CHANGES SUBSCRIBE` against a running server (deletes only its own `tour:` keys) |
| `big_payload_cost.rs`, `cmd_cost.rs`, `del_cost.rs`, `heavy_cmd_cost.rs` | Shard-thread cost of one command, large payloads, collection drops and O(N) multi-key commands |
| `hot_count.rs` | Instructions retired per call (macOS only; use `cargo test --lib --bins --tests` rather than all targets on Linux) |
| `mem_per_key.rs` | Heap bytes per key in a bare shard |

Each file starts with its own run command.
