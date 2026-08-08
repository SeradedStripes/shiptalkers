FROM rust:1.95-alpine3.22 AS dev

RUN apk add --no-cache \
    build-base \
    pkgconfig \
    openssl-dev

RUN cargo install cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-w", "src", "-x", "run"]

# ---------------------------------------------------------------------------
# Production build. The default target is the `runtime` stage, so a plain
# `docker build .` / the CI workflow produce the production image.

FROM rust:1.95-alpine3.22 AS builder

RUN apk add --no-cache \
    build-base \
    pkgconfig \
    openssl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked \
    && cargo clean -p ship-talkers --release \
    && rm -rf src

COPY src ./src
COPY templates ./templates
RUN cargo build --release --locked

FROM alpine:3.22 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    openssl \
    curl \
    && addgroup -S app \
    && adduser -S -G app app \
    && mkdir -p /data \
    && chown app:app /data

WORKDIR /app

COPY --from=builder /app/target/release/ship-talkers /usr/local/bin/ship-talkers
COPY src/website/static ./static

USER app
EXPOSE 3000

CMD ["ship-talkers"]
