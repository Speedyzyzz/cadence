//! src/main.rs
//! Production daemon entrypoint for the Cadence Real-Time Voice Failover Proxy.
//! Boots the Prometheus telemetry plane (:9091), the Sharded Single-Ticker Engine,
//! the Multi-Session HTTP/WebSocket Ingress & Oscilloscope Server (:8080), and
//! coordinates zero-downtime SIGTERM draining.

use cadence::protocol::MonotonicClock;
use cadence::server::{spawn_ingress_server, CadenceServerState};
use cadence::telemetry::{spawn_prometheus_http_exporter, TelemetryCollector};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());

    let metrics_bind =
        std::env::var("CADENCE_METRICS_ADDR").unwrap_or_else(|_| "127.0.0.1:9091".to_string());
    let ingress_bind =
        std::env::var("CADENCE_INGRESS_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    let _metrics_handle =
        spawn_prometheus_http_exporter(&metrics_bind, Arc::clone(&telemetry)).await?;
    info!("Prometheus telemetry plane listening on http://{metrics_bind}/metrics");

    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let server_state = CadenceServerState::new(clock, Arc::clone(&telemetry), num_cores);

    let _ingress_handle = spawn_ingress_server(&ingress_bind, Arc::clone(&server_state)).await?;

    // Wait for Ctrl+C / SIGTERM and execute zero-drop utterance draining before exiting
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal; initiating zero-downtime drain coordinator...");
    server_state
        .drain
        .initiate_drain(Duration::from_secs(10))
        .await;
    server_state.engine.shutdown().await;
    info!("Cadence daemon drained and exited cleanly.");
    Ok(())
}
