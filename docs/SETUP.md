# Setup

## Arch Linux: AUR package

On Arch Linux, install the prebuilt package using an AUR helper such as `yay` or `paru`:

```bash
yay -S ruvio-bin
# or
paru -S ruvio-bin
```

This installs `ruvio`, `ruvio-ctl`, sample configuration in `/etc/ruvio/ruvio.toml`, and configures systemd users and directories:

```bash
# Start and enable Ruvio as a system service
sudo systemctl enable --now ruvio
```

## Linux: prebuilt binaries

Each [GitHub Release](https://github.com/salihcantekin/ruvio/releases/latest) carries ready-to-run archives for
Linux x64 and Linux arm64 (plus macOS arm64 and Windows x64). An archive holds two programs, `ruvio.example.toml`,
the README and the license; `SHA256SUMS` lists the checksum of every archive. No Rust toolchain is needed.

| Program | What it does |
| --- | --- |
| `ruvio` | The server. Speaks RESP2 on `127.0.0.1:6379` by default. `ruvio --help` lists every flag. |
| `ruvio-ctl` | Offline tool for a data directory: `check`, `inspect`, `backup`, `restore`, and `import` from a running Redis. |

Download, verify and unpack (use `linux-arm64` on an arm64 host):

```bash
v=0.4.0
curl -LO https://github.com/salihcantekin/ruvio/releases/download/ruvio-$v/ruvio-$v-linux-x64.tar.gz
curl -LO https://github.com/salihcantekin/ruvio/releases/download/ruvio-$v/SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
tar xzf ruvio-$v-linux-x64.tar.gz
cd ruvio-$v-linux-x64
```

Run the server. Pick the durability profile at startup; `memory` keeps nothing on disk, `everysec` and `always`
write a log under `--data-dir` (see [CONFIGURATION.md](CONFIGURATION.md) and [RECOVERY.md](RECOVERY.md)):

```bash
./ruvio --requirepass change-me --data-dir ./data --durability everysec
redis-cli -a change-me PING
```

Settings can also come from a TOML file: copy `ruvio.example.toml` to `ruvio.toml` next to the binary, or pass
`--config /path/to/ruvio.toml`. Command-line flags and `RUVIO_*` environment variables override the file.

`ruvio-ctl` works on a data directory. `check` and `inspect` only read it and take no lock. `backup` and
`restore` lock the directory, so stop the server that owns it first:

```bash
./ruvio-ctl check   --data-dir ./data      # validate checkpoints and the WAL, change nothing
./ruvio-ctl inspect --data-dir ./data      # same checks, with clock, checkpoint and per-shard detail
./ruvio-ctl backup  --data-dir ./data --dest ./backup
./ruvio-ctl restore --source ./backup --data-dir ./data
```

A backup can be signed with a key file (`--sign-key`, or `RUVIO_BACKUP_KEY`); restoring a signed backup needs the
same key.

`ruvio-ctl import` copies keys with their TTLs from a running Redis (or Ruvio) into a running Ruvio and can read
them back to verify; see [IMPORT.md](IMPORT.md):

```bash
./ruvio-ctl import --from redis://127.0.0.1:6379/0 --to ruvio://127.0.0.1:6380/0 --verify
```

Before raising connection counts, raise the file descriptor limit with `ulimit -n`. The process uses epoll, and
pinning a shard to a core works best when the machine is not heavily oversubscribed.

On macOS, a binary downloaded by a browser is quarantined; `xattr -d com.apple.quarantine ruvio ruvio-ctl` clears
it (downloads with `curl` are not). On Windows, unzip `ruvio-<version>-windows-x64.zip` and run `ruvio.exe`.

The sections below build Ruvio from source.

## Common Requirements

- Install the Rust toolchain with `rustup`.
- The portable core and examples support Linux, macOS, and Windows.

## macOS

macOS builds and runs the same server as Linux and Windows. I/O uses kqueue.

On macOS, install the command-line tools and accept the Xcode license before building:

```bash
xcode-select --install
sudo xcodebuild -license accept
```

The license command requires your administrator password and must be run directly in an interactive Terminal.

## Windows

Install Rust with the `x86_64-pc-windows-msvc` toolchain and Visual Studio Build Tools with the **Desktop development with C++** workload. Then run the common Cargo commands from PowerShell or Windows Terminal.

The storage core, parser, SIMD example and evented server build on Windows. The server uses Tokio's IOCP support; the former Glommio runtime is no longer built on any platform.

## Rust Toolchain

The repository pins the stable channel through `rust-toolchain.toml`. The current SIMD examples use stable architecture intrinsics; nightly Rust is not required.

Recommended commands:

```bash
rustup toolchain install stable
rustup component add rustfmt clippy --toolchain stable
cargo install cargo-deny --locked
```

## Build and Run

```bash
cargo build
cargo test --all-targets
cargo fmt --check
cargo clippy --all-targets --all-features
cargo deny check
python3 scripts/test_supply_chain.py
cargo run --bin ruvio
cargo run --bin ruvio-ctl -- check --data-dir ./data
cargo run --example 04_store_smoke
```

Run the cross-platform test entry points from the repository root:

```powershell
.\scripts\test.ps1
.\scripts\test.ps1 -Clippy
.\scripts\test.ps1 -NativeResilience
.\scripts\test.ps1 -DockerResilience
```

```bash
./scripts/test.sh
./scripts/test.sh --clippy
./scripts/test.sh --native-resilience
./scripts/test.sh --docker-resilience
```

The default runs formatting, all-target Rust tests, and a store smoke example.
Add `-Clippy` / `--clippy` to run Clippy. Native resilience runs process-kill
recovery on the host platform. Docker resilience builds a fresh Linux image
and runs bounded fault scenarios. Docker Desktop must be in Linux-container
mode. Do not run the Docker filesystem scenarios on the host data directory.

The experimental two-host .NET integration/performance lab is documented in
[`tests/scenario-lab/README.md`](../tests/scenario-lab/README.md).

Run one test area with `scripts/test_area.sh <area>`. Areas are `store`,
`protocol`, `engine`, `persistence`, `server`, `client-rust`,
`client-csharp`, and `bench`. No area runs `cargo test --lib`. The client
areas need a live server at `RUVIO_HOST`:`RUVIO_PORT`. `bench` runs the
redis-benchmark scripts in [BENCHMARKING.md](BENCHMARKING.md), not
`cargo test`.

In another terminal, test the local RESP2 server:

```bash
redis-cli PING
redis-cli SET greeting hello
redis-cli GET greeting
redis-cli DEL greeting
```

First-party clients are listed in
[docs/compatibility-matrix.json](compatibility-matrix.json). After the server
is listening:

```bash
python3 scripts/run_client_suites.py
```

Without `redis-cli`, send a raw RESP2 `PING`:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6379
```

The response is `+PONG`. Override the listener with an environment variable, for example `RUVIO_ADDR=0.0.0.0:6380 cargo run --bin ruvio`.

Run the examples:

```bash
cargo run --example 02_simd_search
cargo run --example 03_zero_copy
cargo run --example 04_store_smoke
```

Linux, macOS, and Windows all run the same evented process. Tokio uses epoll, kqueue, or IOCP.

Run four shard owners on ports `6379` through `6382`:

```bash
RUVIO_SHARDS=4 \
RUVIO_ADVERTISE_HOST=127.0.0.1 \
cargo run --release --bin ruvio
```

Any port serves every key: a misrouted command is forwarded to its owner. `redis-cli -c` or a cluster-aware client connects to each shard directly and skips that hop. Multi-core mode is cluster-inspired rather than a complete Redis Cluster implementation.

`cargo build` is the evented server. `--no-default-features` drops mimalloc and keeps the same runtime.

## Docker

The image runs the evented server as a non-root user:

```bash
docker build -t ruvio .
docker run --rm -p 6379:6379 -e RUVIO_REQUIREPASS=change-me ruvio
```

The image has a `HEALTHCHECK` that runs `ruvio-ctl ping` (a RESP `PING` to `RUVIO_ADDR`'s port on
127.0.0.1; a `NOAUTH` reply counts as alive). With TLS on, a plaintext `PING` cannot work: start the
container with `--health-cmd 'ruvio-ctl ping --connect-only'` (it only opens the connection).

The base images in both Dockerfiles are pinned by digest; Dependabot (`.github/dependabot.yml`,
weekly) opens a PR when a digest moves, so security updates still arrive.

For replicas, automatic failover or a cluster in containers, the
[setup wizard](https://ruviodb.com/get-started/wizard/) writes the Compose file. A coordinator container
needs its own health check, because the image's pings the data port, which a coordinator does not open:
`ruvio-ctl ping --address 127.0.0.1:<coordinator port>`. `ruvio --check-config` checks a container's
settings without starting it ([Configuration](CONFIGURATION.md#checking-settings-before-a-start)).

`Dockerfile.linux` is the same runtime, used by the benchmark script. It does not need `seccomp=unconfined`.

## Operator UI

The browser UI is the `salihcantekin/ruvio-ui` image. `linux`, `latest`, and
`0.1.0` are the same image. `RUVIO_UI_TARGET_HOST` and `RUVIO_UI_TARGET_PORT`
are the Ruvio server, the same address as `RUVIO_ADDR`. A default process is
`127.0.0.1:6379`. Port `8080` is only the UI.

```bash
docker pull salihcantekin/ruvio-ui:linux
docker run --rm -p 8080:8080 \
  -e RUVIO_UI_TARGET_HOST=127.0.0.1 \
  -e RUVIO_UI_TARGET_PORT=6379 \
  salihcantekin/ruvio-ui:linux
```

The UI connects to Ruvio from inside the container, so `127.0.0.1` there is the
container itself. For a Ruvio binary on the same machine, for a multi-shard
server, and for the firewall and password settings, see
[UI_CONNECTIONS](UI_CONNECTIONS.md).

From a checkout, the compose file builds that same image next to a server:

```bash
cd clients/ruvio-ui
docker compose up --build
```

Open `http://127.0.0.1:8080`. Sign-in is the server ACL user. An empty user
and password connect as `default`. Named users that must survive a restart
belong in `ruvio.toml` under `[[security.users]]`. `ACL SETUSER` from the
Users screen changes only the running process.

Without Docker, build the screens and run the binary:

```bash
cd clients/ruvio-ui/web
npm install
npm run build
cd ..
cargo run --release
```

`RUVIO_UI_ADDR` changes the HTTP bind (default `127.0.0.1:8080`).
`RUVIO_UI_TARGET_HOST`, `RUVIO_UI_TARGET_PORT`, and
`RUVIO_UI_TARGET_PASSWORD` pre-fill the form. The same notes are on the
project site’s UI page.

## Performance Tips

- Benchmark release builds with `RUSTFLAGS="-C target-cpu=native"` only when binaries will remain on compatible CPUs.
- Use cache-aligned buffers for hot metadata and avoid sharing mutable state across executors.
- Keep the zero-copy wire format stable so the network layer can parse frames in place.
- Measure before and after each optimization; record tail latency, throughput, CPU use, and allocation rate.