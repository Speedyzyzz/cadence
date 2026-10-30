#!/usr/bin/env bash
# scripts/verify_all.sh
# Complete 6-Stage Production Verification Pipeline for Cadence.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v cargo &>/dev/null && [[ -f "$HOME/.cargo/env" ]]; then
  source "$HOME/.cargo/env"
fi

export RUSTFLAGS="-C target-cpu=native"
export CADENCE_INGRESS_ADDR="127.0.0.1:8080"
export CADENCE_METRICS_ADDR="127.0.0.1:9091"
export PYTHONPATH="$ROOT_DIR"

if [[ "$(uname -s)" == "Darwin" ]] && [[ -d "/opt/homebrew/opt/opus/lib/pkgconfig" ]]; then
  export PKG_CONFIG_PATH="/opt/homebrew/opt/opus/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

echo "=================================================================="
echo " [1/6] Running Pure-Rust & libopus FFI Hermetic Chaos Test Suites"
echo "=================================================================="
cargo test --release -- --nocapture
cargo test --release --features opus-encoding -- --nocapture

echo "=================================================================="
echo " [2/6] Running Phase 3 Scaling Subsystems Suite (Trunk/Engine/Drain)"
echo "=================================================================="
cargo test --release --test scale_subsystems -- --nocapture

echo "=================================================================="
echo " [3/6] Running 10,000-Stream Concurrency & Hot-Splice Benchmark"
echo "=================================================================="
cargo bench --bench scale_10k

echo "=================================================================="
echo " [4/6] Booting Unified Cadence Edge Server (:8080) & Metrics (:9091)"
echo "=================================================================="
cargo build --release --features opus-encoding
./target/release/cadence &
CADENCE_PID=$!

cleanup() {
  if kill -0 "$CADENCE_PID" >/dev/null 2>&1; then
    kill "$CADENCE_PID" >/dev/null 2>&1 || true
    wait "$CADENCE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in {1..50}; do
  if curl -fsS "http://${CADENCE_INGRESS_ADDR}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

echo "Liveness (/healthz)  : $(curl -fsS "http://${CADENCE_INGRESS_ADDR}/healthz")"
echo "Readiness (/readyz)  : $(curl -fsS "http://${CADENCE_INGRESS_ADDR}/readyz")"
echo "Weather (/v1/weather): $(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/weather")"

echo "=================================================================="
echo " [5/6] Verifying Live Chaos Stall API & Acoustic Flight Recorder WAVs"
echo "=================================================================="
CHAOS_JSON="$(curl -fsS -X POST "http://${CADENCE_INGRESS_ADDR}/v1/chaos/stall")"
echo "Chaos API Response   : ${CHAOS_JSON}"
SPLICED_BYTES="$(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/flight-recorder/spliced.wav" | wc -c | tr -d ' ')"
NAIVE_BYTES="$(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/flight-recorder/naive.wav" | wc -c | tr -d ' ')"
echo "Flight Recorder WAVs : spliced.wav=${SPLICED_BYTES} bytes, naive.wav=${NAIVE_BYTES} bytes"

python3 -c "import websockets" 2>/dev/null || pip3 install --quiet websockets
python3 scripts/smoke_live_providers.py

echo "=================================================================="
echo " [6/6] Verifying Prometheus Telemetry Exposition (:9091/metrics)"
echo "=================================================================="
curl -fsS "http://${CADENCE_METRICS_ADDR}/metrics" | grep -E "cadence_(failover|saved|splice|dropped|egress)"

echo "=================================================================="
echo " ALL 6 CADENCE PRODUCTION INFRASTRUCTURE STAGES PASSED"
echo "=================================================================="
