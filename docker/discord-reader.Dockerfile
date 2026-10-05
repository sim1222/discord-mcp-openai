# syntax=docker/dockerfile:1
# discord-reader-daemon: the only container that holds a Discord credential.

ARG RUST_IMAGE=rust:1.98.1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS builder
WORKDIR /src

# Warm the dependency cache before copying sources.
COPY Cargo.toml Cargo.lock ./
COPY crates/discord-api/Cargo.toml crates/discord-api/Cargo.toml
COPY crates/discord-store/Cargo.toml crates/discord-store/Cargo.toml
COPY crates/discord-reader-daemon/Cargo.toml crates/discord-reader-daemon/Cargo.toml
COPY crates/discord-reader-mcp/Cargo.toml crates/discord-reader-mcp/Cargo.toml
RUN mkdir -p crates/discord-api/src crates/discord-store/src \
        crates/discord-reader-daemon/src crates/discord-reader-mcp/src \
    && echo "" > crates/discord-api/src/lib.rs \
    && echo "" > crates/discord-store/src/lib.rs \
    && echo "" > crates/discord-reader-daemon/src/lib.rs \
    && echo "fn main() {}" > crates/discord-reader-daemon/src/main.rs \
    && echo "" > crates/discord-reader-mcp/src/lib.rs \
    && echo "fn main() {}" > crates/discord-reader-mcp/src/main.rs \
    && cargo build --release -p discord-reader-daemon || true

COPY crates ./crates
RUN rm -f crates/discord-reader-mcp/src/lib.rs \
    && touch crates/discord-api/src/lib.rs crates/discord-store/src/lib.rs \
        crates/discord-reader-daemon/src/lib.rs crates/discord-reader-daemon/src/main.rs \
        crates/discord-reader-mcp/src/main.rs \
    && cargo build --release -p discord-reader-daemon

FROM ${RUNTIME_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 reader \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --no-create-home \
        --shell /usr/sbin/nologin reader \
    && mkdir -p /data /run/discord-reader \
    && chown reader:reader /data /run/discord-reader \
    && chmod 0750 /data /run/discord-reader

COPY --from=builder /src/target/release/discord-reader-daemon /usr/local/bin/discord-reader-daemon
COPY docker/entrypoint-reader.sh /usr/local/bin/entrypoint-reader.sh
RUN chmod 0755 /usr/local/bin/entrypoint-reader.sh

# The container starts as root only long enough to copy the root-owned Docker
# secret to /tmp; entrypoint-reader.sh then drops to uid 10001 (reader) and
# exec's the daemon. The daemon itself never runs as root.
ENTRYPOINT ["/usr/local/bin/entrypoint-reader.sh"]
CMD ["/usr/local/bin/discord-reader-daemon"]
