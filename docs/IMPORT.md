# Moving data from Redis: `ruvio-ctl import`

```
ruvio-ctl import --from redis://[user:password@]host:6379[/db] --to ruvio://host:6379[/db] \
  [--match 'user:*'] [--batch 500] [--replace] [--dry-run] [--verify]
```

The tool scans the source with `SCAN`, reads each key by type (`GET`, `LRANGE`, `SSCAN`,
`HSCAN`, `ZSCAN`, `XRANGE`), writes it to the target with ordinary commands and keeps the
remaining TTL (`PTTL` then `PEXPIRE`). It needs no RDB file and works with any source version
that speaks RESP2 (streams need Redis 6.2+ for the paging syntax).

- Keys that already exist in the target are skipped and counted, so a second run is safe.
  `--replace` rewrites them: each copy is built under a temporary key in the same slot
  (`{key}:import-tmp`, or `key:import-tmp` for a key with a hash tag) and renamed over the target's
  key, so a crash leaves the old value or the new one with its TTL, never a half-written key. A
  crash can leave one `:import-tmp` key behind, which the next run overwrites. A key with braces
  but no hash tag (`a{}b`) has no same-slot name and is replaced in place.
- `--dry-run` scans and counts; the target is not written.
- `--verify` reads every copied key back from the target and compares it with what was read
  from the source (sets, hashes and sorted sets compare without order). The exit code is 1 when
  any key differs.
- Passwords: set `RUVIO_IMPORT_SOURCE_PASSWORD` and `RUVIO_IMPORT_TARGET_PASSWORD` instead of writing
  `user:password@` in the URL, which shows in the process list (the tool warns when it finds a
  password in a URL). Keep the user name in the URL with an empty password (`redis://user:@host:6379`); without a user, leave out the credentials. Both connections
  are plain TCP without TLS, so run the import on a trusted network or through a tunnel.
- `--match` limits the keys; `--db` is part of the URL (`redis://host/3`).

What is **not** moved: stream consumer groups and pending entries, Lua scripts and functions,
ACL users, module types, access statistics (idle time, frequency). Hashes with field TTLs lose
the field deadlines (the source is read with `HSCAN`).

Consistency: `SCAN` is not a snapshot. A key that changes while the scan runs arrives with the
old or the new value, and a key created after the scan passed its slot is missed. Stop writes
to the source for a clean copy, or run the tool a second time with `--replace`.

Memory: one key is held in memory at a time, so a single multi-gigabyte collection needs that
much RAM in the tool. Throughput on millions of keys and the `SCAN` load on the source were
not measured.

`DUMP` and `RESTORE` in Ruvio use Ruvio's own payload and cannot read Redis's `DUMP` output.
