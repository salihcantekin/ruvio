# Persistence Format v1 Fixtures

These hex files are the frozen on-disk contract for format major `1`, minor `0`.
A new encoder must produce the same bytes. A new decoder must accept them.

Regenerate only when the format itself changes:

```bash
RUVIO_WRITE_FIXTURES=1 cargo test --lib writes_v1
```

Then review the diff. Changing a fixture is a persistence compatibility event
and must be called out in the release notes.

| File | Contents |
| --- | --- |
| `wal-header.hex` | WAL segment header, created-at `1700000000000` |
| `wal-unsupported-major.hex` | Same header with major `2`; decoder must reject it |
| `wal-*.hex` | One WAL record per mutation type, including both `ZADD` encodings and `Transaction` |
| `current.hex` | `CURRENT` generation `1` |
| `clock.hex` | `CLOCK` floor `1700000000000` |
| `snapshot-empty.hex` | Empty shard snapshot |
| `snapshot-all-types.hex` | Bytes, list, set, hash, zset, Bloom, and an already-expired TTL key |
| `manifest.hex` | Manifest for `snapshot-all-types.hex` as `shard-0000.snap` |

## Records added after minor 0

`wal-minor<N>-*.hex` hold one record for each record type added by format minor `N`
(1 to 6), and `wal-header-minor<N>.hex` the segment header of that minor. They were written
with the encoder of the release that added the minor and are checked byte for byte
(`later_minor_wal_record_fixtures_stay_stable`). A new record type or minor needs fixtures
here; regenerate with the command above.

`../data-0.3.4/` is a whole data directory written by the 0.3.4 server (minor 3, two shards,
durability `always`): a checkpoint from `SAVE` and a WAL tail. `tests/persistence_compat_live.rs`
opens it with the current server. It is never regenerated: it stands for what an older
release left on disk.
