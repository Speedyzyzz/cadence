#!/usr/bin/env bash
# scripts/verify_all.sh
# Complete turnkey verification pipeline for Cadence v2.

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
echo " [1/5] Running Pure-Rust & libopus FFI Hermetic Chaos Test Suites"
echo "=================================================================="
cargo test --release -- --nocapture
cargo test --release --features opus-encoding -- --nocapture

echo "=================================================================="
echo " [2/5] Running 10,000-Stream Concurrency & Hot-Splice Benchmark"
echo "=================================================================="
cargo bench --bench scale_10k

echo "=================================================================="
echo " [3/5] Booting Cadence Multi-Session Ingress (:8080) & Metrics (:9091)"
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

# Wait up to 5 seconds for /healthz readiness
for _ in {1..50}; do
  if curl -fsS "http://${CADENCE_INGRESS_ADDR}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

echo "Healthcheck response : $(curl -fsS "http://${CADENCE_INGRESS_ADDR}/healthz")"
echo "Active sessions JSON : $(curl -fsS "http://${CADENCE_INGRESS_ADDR}/v1/sessions")"

echo "=================================================================="
echo " [4/5] Running Python LiveKit Plugin E2E WebSocket Smoke Test"
echo "=================================================================="
python3 -c "import websockets" 2>/dev/null || pip3 install --quiet websockets
python3 scripts/smoke_live_providers.py

echo "=================================================================="
echo " [5/5] Verifying Prometheus Telemetry Exposition (:9091/metrics)"
echo "=================================================================="
curl -fsS "http://${CADENCE_METRICS_ADDR}/metrics" | grep -E "cadence_(failover|saved|splice|dropped|egress)"

echo "=================================================================="
echo " ALL CADENCE V2 PRODUCTION CHECKS PASSED"
echo "=================================================================="
