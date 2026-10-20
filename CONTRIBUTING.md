# Contributing

Issues and pull requests are in English.

A pull request is one change that you can explain. Say what behavior changes, how you checked it, and which test covers it. A drive-by that rewrites an unrelated file, restyles a module, or stacks several ideas is closed.

Pull requests target `dev`. A pull request merges once the required checks pass; it does not have to be updated with `dev` first. The same checks run again on `dev` after each merge, so a break between two pull requests shows up there.

## License

By opening a pull request you agree that your contribution is licensed under the license of the part you change: AGPL-3.0 for the server and the operator UI (`clients/ruvio-ui/`), MIT for the SDKs in `clients/` and for `integrations/`. The maintainer may also offer the server under other terms, including a commercial license; if you are not comfortable with that, say so in the pull request before it is merged.

## Generated changes

An assistant may help you write the patch. You still own it. Before opening the request you can answer, in your own words:

- what the code does
- why this place in the server is the right place
- what you ran

A patch you cannot explain, or a batch of unrelated edits produced by a prompt, is closed without a line-by-line review. "An assistant suggested it" is not a reason to merge.

## The hot path

The shard that owns a key parses, records, applies, and replies. A feature costs memory or CPU only on keys that use it.

- `GET`, `SET`, and `INCR` do not gain an extra index lookup for a feature that is off.
- `Value` stays at most 24 bytes. Do not widen `Entry`.
- Record a write before applying it. A rejected write does not go into the WAL.
- Do not stall the shard on `PUBLISH`.
- The process is evented. Do not add another runtime.

## Checks

[docs/DEVELOPING.md](docs/DEVELOPING.md) maps the code and lists every place a new command, setting or client method has to reach. From the repository root:

```bash
cargo fmt --all --check
dotnet run -c Release --project scripts/csharp-format   # C# blank-line and line-break rules (--check to only report)
scripts/check.sh          # standards tests, matrix drift, zero-cost guards; add --install-hook to run it before every push
cargo test --lib
```

The C# rules (`scripts/csharp-format/Program.cs` has the full list): a statement after a closing `}` and every `if`, loop or `switch` start after one blank line; the statement after an `if` or loop and a `return` that is not first in its block get one too; an `if`, `else` or loop body never shares the header's line; there is never more than one blank line.

A behavior change needs a test that fails without the change. Docs that state a number come from a dated measurement record in `docs/evidence/`. Do not write an unmeasured "N times faster" claim.

Security defects go through [SECURITY.md](SECURITY.md), not a public issue or pull request.
