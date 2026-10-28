# Scanner image for https://github.com/salihcantekin/ruvio (the Ruvio server).
#
# The audit runs offline, so this build installs the pinned toolchain, fetches
# every crate, and compiles the server binaries and the test targets.
# threat_model.md, next to this file, says where to look and how to run things.
FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git python3 \
    && rm -rf /var/lib/apt/lists/*

# scanner contract: the checkout lives inside the image, at /src.
# Dockerfile.dockerignore next to this file replaces the root .dockerignore, which
# leaves out docs/ and clients/ that the library tests include at compile time.
COPY . /src
WORKDIR /src

# Installs the toolchain named in rust-toolchain.toml and every crate, so
# nothing is fetched later.
RUN cargo fetch --locked

# Debug build with debug info: the server, the admin tool, and the test targets.
# A step that fails fails the build.
RUN cargo build --locked --bin ruvio --bin ruvio-ctl \
    && cargo test --locked --lib --no-run

# Run the library unit tests so the log shows the image works, without letting
# a failure abort the build.
RUN (timeout 600 cargo test --locked --lib || echo "WARNING: cargo test reported failures")

# Everything is fetched. From here on cargo must not wait for a registry.
ENV CARGO_NET_OFFLINE=true
