FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --locked --release --bin ruvio --bin ruvio-ctl

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587

LABEL org.opencontainers.image.title="Ruvio" \
      org.opencontainers.image.description="Ruvio server: a memory-first key-value store that speaks the Redis protocol (RESP2)." \
      org.opencontainers.image.source="https://github.com/salihcantekin/ruvio" \
      org.opencontainers.image.url="https://ruviodb.com/" \
      org.opencontainers.image.documentation="https://ruviodb.com/" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.authors="Salih Cantekin"

RUN useradd --system --uid 10001 --create-home ruvio \
    && mkdir -p /data /etc/ruvio \
    && chown ruvio:ruvio /data /etc/ruvio

COPY --from=builder /src/target/release/ruvio /usr/local/bin/ruvio
COPY --from=builder /src/target/release/ruvio-ctl /usr/local/bin/ruvio-ctl
COPY LICENSE /usr/local/share/ruvio/LICENSE
COPY licenses/THIRD_PARTY_NOTICES /usr/local/share/ruvio/THIRD_PARTY_NOTICES

USER ruvio
# Public bind with protected mode off refuses to start until RUVIO_REQUIREPASS
# is set. Protocol CI sets RUVIO_ALLOW_UNAUTHENTICATED=true instead.
ENV RUVIO_ADDR=0.0.0.0:6379
ENV RUVIO_DATA_DIR=/data
ENV RUVIO_PROTECTED_MODE=false
ENV RUVIO_CONFIG=/data/ruvio.toml
VOLUME ["/data"]
EXPOSE 6379
# PING over the RESP port. With TLS on, override with `ruvio-ctl ping --connect-only`.
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=3 CMD ["ruvio-ctl", "ping"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/ruvio"]
