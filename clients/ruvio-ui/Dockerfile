FROM node:26-bookworm-slim@sha256:bc0a2afd3b74ce12222fc8bab99271e66257f021078f5f4a5d687a77d280c1d9 AS web

WORKDIR /web
COPY ruvio-ui/web/package.json ruvio-ui/web/package-lock.json ./
RUN npm ci
COPY ruvio-ui/web/ ./
RUN npm run build

FROM rust:1.99.1-slim-bookworm@sha256:24707e70a4f1e23d9a7b25202ec43126d28a52294b96aad5c670df1fd1f1d400 AS rust

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

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

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
