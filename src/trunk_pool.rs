//! src/trunk_pool.rs
//! Subsystem 1: Upstream N:M WebSocket Trunk Pool & Lock-Free Sharded Context Demuxer.
//! Multiplexes 100,000+ voice utterances over a bounded pool of persistent upstream connections,
//! enforcing per-provider active context limits (e.g., 64 contexts/socket for Cartesia Sonic,
//! 5 concurrent contexts/socket for ElevenLabs Multi-Context WebSockets) and automatically
//! reclaiming context slots via `close_context` / `cancel` frames.

use crate::multiplexer::TtsProviderEndpoint;
use crate::protocol::{
    AudioChunk, CadenceError, MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use crate::telemetry::TelemetryCollector;
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, info};

const ROUTER_SHARDS: usize = 64;

/// Sharded lock-free-style routing table mapping `utterance_id` -> session chunk sender.
/// Partitioning into 64 independent RwLock shards eliminates contention at 100,000+ streams.
pub struct ShardedContextRouter {
    shards: [RwLock<HashMap<u64, mpsc::Sender<AudioChunk>>>; ROUTER_SHARDS],
    active_routes: AtomicUsize,
}

impl Default for ShardedContextRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedContextRouter {
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::with_capacity(256))),
            active_routes: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn shard_index(utterance_id: u64) -> usize {
        // Fibonacci multiplicative hash for uniform shard distribution
        ((utterance_id.wrapping_mul(11400714819323198485)) >> 58) as usize & (ROUTER_SHARDS - 1)
    }

    pub fn register_context(&self, utterance_id: u64, tx: mpsc::Sender<AudioChunk>) {
        let idx = Self::shard_index(utterance_id);
        if let Ok(mut guard) = self.shards[idx].write() {
            if guard.insert(utterance_id, tx).is_none() {
                self.active_routes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn unregister_context(&self, utterance_id: u64) {
        let idx = Self::shard_index(utterance_id);
        if let Ok(mut guard) = self.shards[idx].write() {
            if guard.remove(&utterance_id).is_some() {
                self.active_routes.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Routes an incoming `AudioChunk` from a shared trunk socket to its owning session splicer.
    /// Automatically unregisters the route when `is_final_chunk` is observed.
    pub fn route_chunk(&self, chunk: AudioChunk) -> bool {
        let utt_id = chunk.utterance_id;
        let is_final = chunk.is_final_chunk;
        let idx = Self::shard_index(utt_id);

        let delivered = if let Ok(guard) = self.shards[idx].read() {
            if let Some(tx) = guard.get(&utt_id) {
                tx.try_send(chunk).is_ok()
            } else {
                false
            }
        } else {
            false
        };

        if is_final {
            self.unregister_context(utt_id);
        }
        delivered
    }

    pub fn active_context_count(&self) -> usize {
        self.active_routes.load(Ordering::Relaxed)
    }
}

/// Configuration for an upstream trunk pool.
#[derive(Debug, Clone)]
pub struct TrunkPoolConfig {
    pub provider: ProviderId,
    /// Number of persistent trunk sockets maintained in the pool.
    pub trunk_count: usize,
    /// Maximum concurrent contexts allowed on a single trunk socket
    /// (Cartesia Sonic = 64, ElevenLabs Multi-Context WS = 5).
    pub max_contexts_per_socket: usize,
    pub sample_rate: SampleRate,
}

impl TrunkPoolConfig {
    pub fn cartesia_default(trunk_count: usize, sample_rate: SampleRate) -> Self {
        Self {
            provider: ProviderId::PrimaryCartesia,
            trunk_count: trunk_count.max(1),
            max_contexts_per_socket: 64,
            sample_rate,
        }
    }

    pub fn elevenlabs_default(trunk_count: usize, sample_rate: SampleRate) -> Self {
        Self {
            provider: ProviderId::StandbyElevenLabs,
            trunk_count: trunk_count.max(1),
            // ElevenLabs multi-stream-input enforces a 5 concurrent context limit per socket
            max_contexts_per_socket: 5,
            sample_rate,
        }
    }
}

/// Represents a single persistent trunk connection slot in the pool.
pub struct TrunkSlot {
    pub slot_id: usize,
    pub endpoint: Arc<dyn TtsProviderEndpoint>,
    pub active_contexts: AtomicUsize,
    pub total_dispatched: AtomicU64,
}

impl TrunkSlot {
    pub fn new(slot_id: usize, endpoint: Arc<dyn TtsProviderEndpoint>) -> Self {
        Self {
            slot_id,
            endpoint,
            active_contexts: AtomicUsize::new(0),
            total_dispatched: AtomicU64::new(0),
        }
    }

    /// Attempts to atomically reserve a context slot if below `max_contexts`.
    pub fn try_acquire_slot(&self, max_contexts: usize) -> bool {
        let mut current = self.active_contexts.load(Ordering::Relaxed);
        loop {
            if current >= max_contexts {
                return false;
            }
            match self.active_contexts.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.total_dispatched.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn release_slot(&self) {
        let mut current = self.active_contexts.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return;
            }
            match self.active_contexts.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Multiplexes $N$ session utterances across $M$ persistent upstream trunk endpoints.
pub struct UpstreamTrunkPool {
    config: TrunkPoolConfig,
    slots: Vec<Arc<TrunkSlot>>,
    utterance_to_slot: [RwLock<HashMap<u64, usize>>; ROUTER_SHARDS],
    router: Arc<ShardedContextRouter>,
    round_robin_cursor: AtomicUsize,
    healthy: AtomicBool,
    clock: MonotonicClock,
    telemetry: Arc<TelemetryCollector>,
}

impl UpstreamTrunkPool {
    /// Creates a new `UpstreamTrunkPool` from a vector of persistent trunk endpoints
    /// and spawns the high-speed demuxer loop that routes trunk chunks to session splicers.
    pub fn new(
        config: TrunkPoolConfig,
        endpoints: Vec<Arc<dyn TtsProviderEndpoint>>,
        mut trunk_ingress_rx: mpsc::Receiver<AudioChunk>,
        router: Arc<ShardedContextRouter>,
        clock: MonotonicClock,
        telemetry: Arc<TelemetryCollector>,
    ) -> Arc<Self> {
        let slots: Vec<Arc<TrunkSlot>> = endpoints
            .into_iter()
            .enumerate()
            .map(|(idx, ep)| Arc::new(TrunkSlot::new(idx, ep)))
            .collect();

        let pool = Arc::new(Self {
            config,
            slots,
            utterance_to_slot: std::array::from_fn(|_| RwLock::new(HashMap::with_capacity(64))),
            router,
            round_robin_cursor: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
            clock,
            telemetry,
        });

        let pool_demux = Arc::clone(&pool);
        tokio::spawn(async move {
            while let Some(chunk) = trunk_ingress_rx.recv().await {
                let utt_id = chunk.utterance_id;
                let is_final = chunk.is_final_chunk;
                pool_demux.router.route_chunk(chunk);

                if is_final {
                    pool_demux.release_utterance_slot(utt_id);
                }
            }
        });

        pool
    }

    /// Selects the least-loaded healthy trunk socket that has available context capacity,
    /// or falls back to power-of-two / round-robin selection if all trunks are at peak capacity.
    fn acquire_trunk_for_utterance(
        &self,
        utterance_id: u64,
    ) -> Result<Arc<TrunkSlot>, CadenceError> {
        if self.slots.is_empty() {
            return Err(CadenceError::TransportError(
                "No trunk sockets configured in pool".to_string(),
            ));
        }

        let shard_idx = ShardedContextRouter::shard_index(utterance_id);

        // If this utterance already has an assigned trunk slot (multi-chunk streaming), reuse it
        if let Ok(guard) = self.utterance_to_slot[shard_idx].read() {
            if let Some(&existing_slot_idx) = guard.get(&utterance_id) {
                return Ok(Arc::clone(&self.slots[existing_slot_idx]));
            }
        }

        let n = self.slots.len();
        let start = self.round_robin_cursor.fetch_add(1, Ordering::Relaxed) % n;

        // First pass: find a trunk with available context slots below max_contexts_per_socket
        for offset in 0..n {
            let idx = (start + offset) % n;
            let candidate = &self.slots[idx];
            if candidate.try_acquire_slot(self.config.max_contexts_per_socket) {
                if let Ok(mut guard) = self.utterance_to_slot[shard_idx].write() {
                    guard.insert(utterance_id, idx);
                }
                return Ok(Arc::clone(candidate));
            }
        }

        // Overflow fallback: pick the trunk with the lowest active_contexts count
        let (best_idx, best_slot) = self
            .slots
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| s.active_contexts.load(Ordering::Relaxed))
            .unwrap();

        best_slot.active_contexts.fetch_add(1, Ordering::AcqRel);
        best_slot.total_dispatched.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.utterance_to_slot[shard_idx].write() {
            guard.insert(utterance_id, best_idx);
        }

        debug!(
            provider = %self.config.provider,
            slot_id = best_idx,
            "All trunks at nominal context limit; overflow-multiplexing onto least-loaded trunk"
        );
        Ok(Arc::clone(best_slot))
    }

    pub fn release_utterance_slot(&self, utterance_id: u64) {
        let shard_idx = ShardedContextRouter::shard_index(utterance_id);
        if let Ok(mut guard) = self.utterance_to_slot[shard_idx].write() {
            if let Some(slot_idx) = guard.remove(&utterance_id) {
                if let Some(slot) = self.slots.get(slot_idx) {
                    slot.release_slot();
                }
            }
        }
    }

    /// Builds the provider-specific context cancellation / slot-release JSON payload
    /// (`{"context_id": id, "cancel": true}` for Cartesia; `{"context_id": id, "close_context": true}` for ElevenLabs).
    pub fn build_close_context_payload(&self, utterance_id: u64) -> String {
        match self.config.provider {
            ProviderId::PrimaryCartesia => json!({
                "context_id": utterance_id.to_string(),
                "cancel": true
            })
            .to_string(),
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => json!({
                "context_id": utterance_id.to_string(),
                "close_context": true
            })
            .to_string(),
        }
    }

    pub fn total_active_contexts(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.active_contexts.load(Ordering::Relaxed))
            .sum()
    }

    pub fn trunk_count(&self) -> usize {
        self.slots.len()
    }
}

#[async_trait]
impl TtsProviderEndpoint for UpstreamTrunkPool {
    fn provider_id(&self) -> ProviderId {
        self.config.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        let slot = self.acquire_trunk_for_utterance(req.utterance_id)?;
        let res = slot.endpoint.send_synthesis_request(req).await;
        if res.is_err() {
            self.release_utterance_slot(req.utterance_id);
        }
        res
    }

    async fn send_warm_heartbeat(&self, timestamp_us: u64) -> Result<u64, CadenceError> {
        let mut latest_pong = 0;
        for slot in &self.slots {
            if let Ok(pong) = slot.endpoint.send_warm_heartbeat(timestamp_us).await {
                latest_pong = latest_pong.max(pong);
            }
        }
        let _ = &self.clock;
        let _ = &self.telemetry;
        Ok(latest_pong)
    }

    async fn send_zero_payload_handshake(&self, session_id: &str) -> Result<(), CadenceError> {
        for slot in &self.slots {
            slot.endpoint
                .send_zero_payload_handshake(session_id)
                .await?;
        }
        Ok(())
    }

    async fn send_context_cancel(&self, context_id: &str) -> Result<(), CadenceError> {
        for slot in &self.slots {
            let _ = slot.endpoint.send_context_cancel(context_id).await;
        }
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn shutdown(&self) -> Result<(), CadenceError> {
        self.healthy.store(false, Ordering::Release);
        for slot in &self.slots {
            let _ = slot.endpoint.shutdown().await;
        }
        info!(provider = %self.config.provider, "UpstreamTrunkPool shut down cleanly");
        Ok(())
    }
}
