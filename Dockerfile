# Stage 1: Rust release builder with native libopus
FROM rust:1.82-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libopus-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/cadence
COPY Cargo.toml build.rs* ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests

RUN cargo build --release --features opus-encoding

# Stage 2: Minimal Debian Bookworm runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 \
    libssl3 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --shell /usr/sbin/nologin cadence
COPY --from=builder /usr/src/cadence/target/release/cadence /usr/local/bin/cadence

USER cadence
ENV RUST_LOG=info
ENV CADENCE_INGRESS_ADDR=0.0.0.0:8080
ENV CADENCE_METRICS_ADDR=0.0.0.0:9091

EXPOSE 8080 9091
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/cadence"]
