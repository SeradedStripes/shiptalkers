FROM rust:1.89-slim AS dev

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libfontconfig1-dev \
    g++ \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-w", "src", "-x", "run"]

# ---------------------------------------------------------------------------
# Production build. The default target is the `runtime` stage, so a plain
# `docker build .` / the CI workflow produce the slim production image.

FROM rust:1.89-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libfontconfig1-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked \
    && cargo clean -p ship-talkers --release \
    && rm -rf src

COPY src ./src
COPY templates ./templates
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libfontconfig1 \
    libfreetype6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r app && useradd -r -g app app \
    && mkdir -p /data && chown app:app /data

WORKDIR /app

COPY --from=builder /app/target/release/ship-talkers /usr/local/bin/ship-talkers
COPY src/website/static ./static

USER app
EXPOSE 3000

CMD ["ship-talkers"]
