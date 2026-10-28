[Ruvio](../README.md) / Tests

# Integration tests

Run from the repository root.

```bash
cargo test --lib --bins --tests
```

| File | Covers |
| --- | --- |
| `zero_alloc.rs`, `layout.rs`, `memory_budget.rs` | Zero-cost rules: no allocation on the hot path, size ceilings, memory budgets |
| `parse_matrix.rs`, `store_workflow.rs`, `concurrency_model.rs` | Command parsing, store behavior, shard concurrency |
| `storage_properties.rs`, `recovery_live.rs`, `import_live.rs`, `changes_live.rs` | Persistence, recovery, import and the change feed against a live server |
| `scenario-lab/` | A two-host load lab for the .NET integrations; see its [README](scenario-lab/README.md) |
| `soak/` | Long runs of a multi-node cluster with faults and an operation-history checker, locally or on Azure; see its [README](soak/README.md) |

[docs/TESTING.md](../docs/TESTING.md) explains the test layers and what CI runs.
