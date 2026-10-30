//! src/drain.rs
//! Subsystem 4: Zero-Downtime SIGTERM Drain Coordinator.
//! Manages node readiness (`/readyz`), tracks in-flight utterances via RAII guards,
//! emits `{"type": "reconnect_hint", "drain": true}` frames to connected WebSocket clients
//! between utterances, and blocks process exit until all in-flight speech synthesis finishes.

use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tracing::info;

pub struct DrainCoordinator {
    draining: AtomicBool,
    active_ws_connections: AtomicUsize,
    inflight_utterances: AtomicUsize,
    drain_tx: broadcast::Sender<String>,
    all_drained_notify: Notify,
}

impl Default for DrainCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainCoordinator {
    pub fn new() -> Self {
        let (drain_tx, _) = broadcast::channel(256);
        Self {
            draining: AtomicBool::new(false),
            active_ws_connections: AtomicUsize::new(0),
            inflight_utterances: AtomicUsize::new(0),
            drain_tx,
            all_drained_notify: Notify::new(),
        }
    }

    /// Returns `true` if the node is accepting new WebSocket calls (`/readyz` -> `200 OK`).
    /// Returns `false` once `initiate_drain()` has been triggered (`/readyz` -> `503 Draining`).
    #[inline(always)]
    pub fn is_ready(&self) -> bool {
        !self.draining.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub fn subscribe_drain_hints(&self) -> broadcast::Receiver<String> {
        self.drain_tx.subscribe()
    }

    /// Acquires an RAII guard for an active WebSocket connection.
    pub fn track_ws_connection(self: &Arc<Self>) -> WsConnectionGuard {
        self.active_ws_connections.fetch_add(1, Ordering::AcqRel);
        WsConnectionGuard {
            coordinator: Arc::clone(self),
        }
    }

    /// Acquires an RAII guard for an in-flight TTS utterance.
    pub fn track_inflight_utterance(self: &Arc<Self>) -> UtteranceDrainGuard {
        self.inflight_utterances.fetch_add(1, Ordering::AcqRel);
        UtteranceDrainGuard {
            coordinator: Arc::clone(self),
        }
    }

    pub fn inflight_utterance_count(&self) -> usize {
        self.inflight_utterances.load(Ordering::Acquire)
    }

    pub fn active_connection_count(&self) -> usize {
        self.active_ws_connections.load(Ordering::Acquire)
    }

    /// Initiates graceful node draining (`SIGTERM` handler):
    /// 1. Marks `/readyz` as unhealthy (`503`) so the load balancer stops routing new calls.
    /// 2. Broadcasts `{"type": "reconnect_hint", "drain": true}` to all connected clients.
    /// 3. Waits up to `max_wait` for all in-flight utterances to reach `is_final_chunk`.
    pub async fn initiate_drain(&self, max_wait: Duration) -> bool {
        if !self.draining.swap(true, Ordering::AcqRel) {
            let hint_payload = json!({
                "type": "reconnect_hint",
                "drain": true,
                "reason": "node_rolling_update"
            })
            .to_string();

            let _ = self.drain_tx.send(hint_payload);
            info!(
                inflight_utterances = self.inflight_utterance_count(),
                active_connections = self.active_connection_count(),
                "Initiated graceful SIGTERM drain; broadcasted reconnect_hint to clients"
            );
        }

        if self.inflight_utterance_count() == 0 {
            return true;
        }

        tokio::time::timeout(max_wait, async {
            while self.inflight_utterance_count() > 0 {
                self.all_drained_notify.notified().await;
            }
        })
        .await
        .is_ok()
    }
}

pub struct WsConnectionGuard {
    coordinator: Arc<DrainCoordinator>,
}

impl Drop for WsConnectionGuard {
    fn drop(&mut self) {
        self.coordinator
            .active_ws_connections
            .fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct UtteranceDrainGuard {
    coordinator: Arc<DrainCoordinator>,
}

impl Drop for UtteranceDrainGuard {
    fn drop(&mut self) {
        let prev = self
            .coordinator
            .inflight_utterances
            .fetch_sub(1, Ordering::AcqRel);
        if prev == 1 && self.coordinator.is_draining() {
            self.coordinator.all_drained_notify.notify_waiters();
        }
    }
}
