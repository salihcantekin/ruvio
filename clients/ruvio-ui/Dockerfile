FROM node:26-bookworm-slim@sha256:add5c17c14752c54d2f3d519ec46a3815684d41c33f198bcb09fb998f7dcbf68 AS web

WORKDIR /web
COPY ruvio-ui/web/package.json ruvio-ui/web/package-lock.json ./
RUN npm ci
COPY ruvio-ui/web/ ./
RUN npm run build

FROM rust:1.99.1-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS rust

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY ruvio-client /src/ruvio-client
COPY ruvio-ui/Cargo.toml ruvio-ui/Cargo.lock /src/ruvio-ui/
COPY ruvio-ui/src /src/ruvio-ui/src
COPY --from=web /web/dist /src/ruvio-ui/web/dist
WORKDIR /src/ruvio-ui
RUN cargo build --locked --release

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3

LABEL org.opencontainers.image.title="Ruvio UI" \
      org.opencontainers.image.description="Browser operator console for a Ruvio server." \
      org.opencontainers.image.source="https://github.com/salihcantekin/ruvio" \
      org.opencontainers.image.url="https://ruviodb.com/" \
      org.opencontainers.image.documentation="https://ruviodb.com/" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.authors="Salih Cantekin"

RUN useradd --system --uid 10001 --create-home ruvio
COPY --from=rust /src/ruvio-ui/target/release/ruvio-ui /usr/local/bin/ruvio-ui
COPY ruvio-ui/LICENSE /usr/local/share/ruvio-ui/LICENSE
USER ruvio
ENV RUVIO_UI_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ruvio-ui"]
