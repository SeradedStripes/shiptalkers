FROM rust:1.95-alpine3.22 AS dev

RUN apk add --no-cache build-base

RUN cargo install cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-w", "shiptalkers-scraper/src", "-w", "shiptalkers-lib/src", "-x", "run", "-p", "ship-talkers-scraper"]

# ---------------------------------------------------------------------------
# Production build. The default target is the `runtime` stage, so a plain
# `docker build .` / a CI workflow produce the production image.
# Build from the workspace root so the root Cargo.lock is the single lock.

FROM rust:1.95-alpine3.22 AS chef

RUN apk add --no-cache build-base \
    && cargo install cargo-chef --locked

WORKDIR /app

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --locked --bin ship-talkers-scraper

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -G app -u 10001 app

COPY --from=builder /app/target/release/ship-talkers-scraper /usr/local/bin/ship-talkers-scraper

USER app
CMD ["ship-talkers-scraper"]