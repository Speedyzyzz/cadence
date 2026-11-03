#!/usr/bin/env bash
# scripts/verify_all.sh
# Complete 7-Stage Production Verification Pipeline for Cadence.

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
echo " [1/7] Running Pure-Rust & libopus FFI Hermetic Chaos Test Suites"
echo "=================================================================="
cargo test --release -- --nocapture
cargo test --release --features opus-encoding -- --nocapture

echo "=================================================================="
echo " [2/7] Running Phase 3 Scaling Subsystems Suite (Trunk/Engine/Drain)"
echo "=================================================================="
cargo test --release --test scale_subsystems -- --nocapture

echo "=================================================================="
echo " [3/7] Running Backchannel (450ms LLM Masking) & Binary WS Tap Suite"
echo "=================================================================="
cargo test --release --test backchannel_and_tap -- --nocapture

echo "=================================================================="
echo " [4/7] Running 10,000-Stream Concurrency & Hot-Splice Benchmark"
echo "=================================================================="
cargo bench --bench scale_10k

echo "=================================================================="
echo " [5/7] Booting Unified Cadence Edge Server (:8080) & Metrics (:9091)"
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
echo " [6/7] Verifying Chaos Stall API, LLM Backchannel API & Flight WAVs"
echo "=================================================================="
CHAOS_JSON="$(curl -fsS -X POST "http://${CADENCE_INGRESS_ADDR}/v1/chaos/stall")"
LLM_STALL_JSON="$(curl -fsS -X POST "http://${CADENCE_INGRESS_ADDR}/v1/chaos/llm-stall")"
echo "Chaos Socket Stall   : ${CHAOS_JSON}"
echo "Chaos LLM Stall (BC) : ${LLM_STALL_JSON}"
SPLICED_BYTES="$(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/flight-recorder/spliced.wav" | wc -c | tr -d ' ')"
NAIVE_BYTES="$(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/flight-recorder/naive.wav" | wc -c | tr -d ' ')"
echo "Flight Recorder WAVs : spliced.wav=${SPLICED_BYTES} bytes, naive.wav=${NAIVE_BYTES} bytes"

python3 -c "import websockets" 2>/dev/null || pip3 install --quiet websockets
python3 scripts/smoke_live_providers.py

echo "=================================================================="
echo " [7/7] Verifying Prometheus Telemetry Exposition (:9091/metrics)"
echo "=================================================================="
curl -fsS "http://${CADENCE_METRICS_ADDR}/metrics" | grep -E "cadence_(failover|saved|splice|dropped|egress|backchannel)"

echo "=================================================================="
echo " ALL 7 CADENCE PRODUCTION INFRASTRUCTURE STAGES PASSED"
echo "=================================================================="
