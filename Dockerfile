# Build stage
FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data

WORKDIR /app

COPY --from=builder /app/target/release/mw-mission /usr/local/bin/mw-mission

ENV MW_MISSION_ADDR=0.0.0.0:4410
ENV MW_MISSION_DB=/data/mw-mission.db

VOLUME ["/data"]
EXPOSE 4410

ENTRYPOINT ["mw-mission"]
