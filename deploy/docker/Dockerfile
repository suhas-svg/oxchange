# One image with every Oxchange service binary; compose picks the command.
# cargo-chef caches dependency builds, so code-only changes rebuild quickly.

FROM rust:1.90-bookworm AS chef
# librdkafka is compiled from source by rdkafka-sys.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential pkg-config zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --all-features --recipe-path recipe.json
COPY . .
RUN cargo build --release --all-features --bins \
 && mkdir -p /out \
 && find target/release -maxdepth 1 -type f -executable -exec cp {} /out/ \;

# `docker build --target lint .` runs clippy over the Kafka-gated code too,
# which host builds (without the kafka feature) never compile.
FROM builder AS lint
RUN rustup component add clippy && cargo clippy --release --all-features --all-targets -- -D warnings

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --home /nonexistent oxchange \
 && mkdir -p /var/lib/matching && chown oxchange /var/lib/matching
COPY --from=builder /out/ /usr/local/bin/
USER oxchange
ENV RUST_LOG=info
