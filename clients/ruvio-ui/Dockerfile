FROM node:22-bookworm-slim@sha256:de170b39439eb0e5a3444ad7d60b015614a381e10fecae2e30ec9e735122f38d AS web

WORKDIR /web
COPY ruvio-ui/web/package.json ruvio-ui/web/package-lock.json ./
RUN npm ci
COPY ruvio-ui/web/ ./
RUN npm run build

FROM rust:1.99.0-slim-bookworm@sha256:2c3a22f0a5533ea2dd5a16627bc841228151faa2d4de2644ac9987e4a2f1f2fa AS rust

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

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587

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
