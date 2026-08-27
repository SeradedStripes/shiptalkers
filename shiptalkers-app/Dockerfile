FROM rust:1.95-alpine3.22 AS dev

RUN apk add --no-cache build-base

RUN cargo install cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-w", "shiptalkers-app/src", "-w", "shiptalkers-lib/src", "-x", "run"]

# ---------------------------------------------------------------------------
# Production build. The default target is the `runtime` stage, so a plain
# `docker build .` / the CI workflow produce the production image.
# Build from the workspace root so the root Cargo.lock is the single lock.
#
# Dependency caching is done with cargo-chef: `prepare` distills the
# workspace into recipe.json (only changes when manifests change), and
# `cook` compiles all dependencies in a layer that stays cached while
# source edits flow through.

FROM lukemathwalker/cargo-chef:latest-rust-1.95.0-alpine3.22 AS chef

RUN apk add --no-cache build-base

WORKDIR /app

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --locked

FROM alpine:3.22 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    curl \
    su-exec \
    && addgroup -S -g 10001 app \
    && adduser -S -G app -u 10001 app \
    && mkdir -p /data

WORKDIR /app

COPY --from=builder /app/target/release/ship-talkers /usr/local/bin/ship-talkers
COPY shiptalkers-app/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER root
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["ship-talkers"]
