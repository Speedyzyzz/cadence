# Multi-stage production build for Cadence
FROM rust:1.82-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libopus-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml ./
COPY src ./src
COPY tests ./tests

# Build optimized binary with native LTO
RUN cargo build --release
RUN cp target/release/cadence /usr/local/bin/cadence

# Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libopus0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/cadence /usr/local/bin/cadence

ENV RUST_LOG=info
ENV CADENCE_METRICS_ADDR=0.0.0.0:9091
ENV CADENCE_INGRESS_ADDR=0.0.0.0:8080

EXPOSE 8080 9091

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD curl -f http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/cadence"]
