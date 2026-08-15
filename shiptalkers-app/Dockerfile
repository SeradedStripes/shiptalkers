FROM rust:1.95-alpine3.22 AS dev

RUN apk add --no-cache build-base

RUN cargo install cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-w", "shiptalkers-app/src", "-x", "run"]

# ---------------------------------------------------------------------------
# Production build. The default target is the `runtime` stage, so a plain
# `docker build .` / the CI workflow produce the production image.
# Build from the workspace root so the root Cargo.lock is the single lock.

FROM rust:1.95-alpine3.22 AS builder

RUN apk add --no-cache build-base

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY shiptalkers-app/Cargo.toml ./shiptalkers-app/Cargo.toml
RUN mkdir -p shiptalkers-app/src \
    && echo "fn main() {}" > shiptalkers-app/src/main.rs \
    && echo "pub fn f() {}" > shiptalkers-app/src/lib.rs \
    && cargo build --release --locked \
    && cargo clean -p ship-talkers --release \
    && rm -rf shiptalkers-app/src

COPY shiptalkers-app/src ./shiptalkers-app/src
COPY shiptalkers-app/templates ./shiptalkers-app/templates
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
