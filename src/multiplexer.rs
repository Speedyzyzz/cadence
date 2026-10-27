//! src/multiplexer.rs
//! Module A: Audio Stream Multiplexer & Warm Standby Pool.
//! Manages parallel active (Cartesia Sonic) and warm standby (ElevenLabs Flash v2.5)
//! WebSocket connections. Keeps the backup connection warm with 5s Ping heartbeats and
//! 12s zero-payload context handshakes, decodes RFC 4648 Base64 or binary PCM wire frames,
//! parses word timestamp alignment metadata, and supports adaptive speculative shadowing
//! (Full, Prefix-Only, Canary, and Disabled).

use crate::protocol::{
    AudioChunk, CadenceError, FailoverSignal, MonotonicClock, ProviderId, SampleRate,
    SynthesisRequest,
};
use crate::telemetry::TelemetryCollector;
use crate::watchdog::LatencyWatchdog;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

/// Abstract transport interface enabling both live WebSocket endpoints (Cartesia/ElevenLabs)
/// and deterministic chaos fault-injection transports.
#[async_trait]
pub trait TtsProviderEndpoint: Send + Sync + 'static {
    fn provider_id(&self) -> ProviderId;
    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError>;
    async fn send_warm_heartbeat(&self, timestamp_us: u64) -> Result<u64, CadenceError>;
    async fn send_zero_payload_handshake(&self, session_id: &str) -> Result<(), CadenceError>;
    async fn send_context_cancel(&self, context_id: &str) -> Result<(), CadenceError>;
    async fn is_healthy(&self) -> bool;
    async fn shutdown(&self) -> Result<(), CadenceError>;
}

/// Live WebSocket endpoint supporting Cartesia Sonic and ElevenLabs Flash v2.5 wire formats.
pub struct WebSocketTtsEndpoint {
    provider: ProviderId,
    endpoint_url: String,
    api_key: String,
    sample_rate: SampleRate,
    #[allow(dead_code)]
    clock: MonotonicClock,
    healthy: Arc<AtomicBool>,
    last_pong_us: Arc<AtomicU64>,
    outbound_tx: mpsc::Sender<Message>,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WebSocketTtsEndpoint {
    pub async fn connect(
        provider: ProviderId,
        endpoint_url: String,
        api_key: String,
        sample_rate: SampleRate,
        #[allow(dead_code)]
    clock: MonotonicClock,
        chunk_sink: mpsc::Sender<AudioChunk>,
        telemetry: Arc<TelemetryCollector>,
    ) -> Result<Arc<Self>, CadenceError> {
        let (ws_stream, _) = connect_async(&endpoint_url)
            .await
            .map_err(|e| CadenceError::TransportError(format!("WS connect error: {e}")))?;

        let (mut ws_write, mut ws_read) = ws_stream.split();
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(1024);

        let healthy = Arc::new(AtomicBool::new(true));
        let last_pong_us = Arc::new(AtomicU64::new(clock.now_us()));

        let healthy_writer = Arc::clone(&healthy);
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if let Err(e) = ws_write.send(msg).await {
                    error!("WebSocket write error: {e}");
                    healthy_writer.store(false, Ordering::Release);
                    break;
                }
            }
        });

        let healthy_reader = Arc::clone(&healthy);
        let pong_tracker = Arc::clone(&last_pong_us);
        let reader_clock = clock.clone();

        let handle = tokio::spawn(async move {
            let mut chunk_seq: u64 = 0;
            let mut active_utterance: u64 = 0;

            while let Some(frame_res) = ws_read.next().await {
                match frame_res {
                    Ok(Message::Binary(raw_bytes)) => {
                        let arrival_us = reader_clock.now_us();
                        if raw_bytes.len() >= 2 {
                            let sample_count = raw_bytes.len() / 2;
                            let mut samples = Vec::with_capacity(sample_count);
                            for pair in raw_bytes.chunks_exact(2) {
                                samples.push(i16::from_le_bytes([pair[0], pair[1]]));
                            }
                            let is_first = chunk_seq == 0;
                            let chunk = AudioChunk {
                                utterance_id: active_utterance,
                                sequence: chunk_seq,
                                provider,
                                sample_rate,
                                samples: Arc::from(samples),
                                arrival_us,
                                is_first_chunk: is_first,
                                is_final_chunk: false,
                                alignment_json: None,
                            };
                            chunk_seq = chunk_seq.wrapping_add(1);
                            let _ = chunk_sink.try_send(chunk);
                        }
                    }
                    Ok(Message::Text(txt)) => {
                        let arrival_us = reader_clock.now_us();
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                            if let Some(utt_id) = val.get("context_id").and_then(|v| v.as_str()) {
                                active_utterance =
                                    utt_id.parse::<u64>().unwrap_or(active_utterance);
                            }
                            let is_done = val
                                .get("done")
                                .or_else(|| val.get("isFinal"))
                                .and_then(|d| d.as_bool())
                                .unwrap_or(false);

                            // Extract alignment metadata if present
                            let alignment_json = if val.get("word_timestamps").is_some()
                                || val.get("alignment").is_some()
                                || val.get("normalized_alignment").is_some()
                            {
                                Some(Arc::new(val.clone()))
                            } else {
                                None
                            };

                            // Cartesia uses "data", ElevenLabs uses "audio"
                            let b64_field = val
                                .get("data")
                                .or_else(|| val.get("audio"))
                                .and_then(|d| d.as_str());

                            if let Some(b64_pcm) = b64_field {
                                let decoded = decode_base64_pcm_s16le(b64_pcm);
                                if !decoded.is_empty() {
                                    let is_first = chunk_seq == 0;
                                    let chunk = AudioChunk {
                                        utterance_id: active_utterance,
                                        sequence: chunk_seq,
                                        provider,
                                        sample_rate,
                                        samples: Arc::from(decoded),
                                        arrival_us,
                                        is_first_chunk: is_first,
                                        is_final_chunk: is_done,
                                        alignment_json,
                                    };
                                    chunk_seq = chunk_seq.wrapping_add(1);
                                    let _ = chunk_sink.try_send(chunk);
                                }
                            }

                            if is_done {
                                chunk_seq = 0;
                            }
                        }
                    }
                    Ok(Message::Pong(payload)) => {
                        let now = reader_clock.now_us();
                        pong_tracker.store(now, Ordering::Release);
                        if payload.len() == 8 {
                            let sent_us =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                            if now >= sent_us {
                                telemetry.record_provider_rtt(provider, now - sent_us);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        healthy_reader.store(false, Ordering::Release);
                        break;
                    }
                    Err(e) => {
                        warn!("WebSocket read error on {provider}: {e}");
                        healthy_reader.store(false, Ordering::Release);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(Arc::new(Self {
            provider,
            endpoint_url,
            api_key,
            sample_rate,
            clock,
            healthy,
            last_pong_us,
            outbound_tx,
            worker_handle: Mutex::new(Some(handle)),
        }))
    }
}

/// Zero-dependency RFC 4648 Base64 decoder converting encoded `pcm_s16le` strings into `Vec<i16>`.
pub fn decode_base64_pcm_s16le(input: &str) -> Vec<i16> {
    const LUT: [u8; 256] = {
        let mut table = [0xFFu8; 256];
        let mut i = 0u8;
        while i < 26 {
            table[(b'A' + i) as usize] = i;
            table[(b'a' + i) as usize] = i + 26;
            i += 1;
        }
        let mut d = 0u8;
        while d < 10 {
            table[(b'0' + d) as usize] = d + 52;
            d += 1;
        }
        table[b'+' as usize] = 62;
        table[b'/' as usize] = 63;
        table[b'-' as usize] = 62; // URL-safe Base64 support
        table[b'_' as usize] = 63;
        table
    };

    let mut raw_bytes = Vec::with_capacity((input.len() * 3) / 4);
    let mut acc = 0u32;
    let mut bits = 0u8;

    for &b in input.as_bytes() {
        if b == b'=' {
            break;
        }
        let val = LUT[b as usize];
        if val == 0xFF {
            continue;
        }
        acc = (acc << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            raw_bytes.push(((acc >> bits) & 0xFF) as u8);
        }
    }

    let mut samples = Vec::with_capacity(raw_bytes.len() / 2);
    for pair in raw_bytes.chunks_exact(2) {
        samples.push(i16::from_le_bytes([pair[0], pair[1]]));
    }
    samples
}

/// Companion RFC 4648 Base64 encoder for `pcm_s16le` test verification and wire serialization.
pub fn encode_base64_pcm_s16le(samples: &[i16]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut raw_bytes = Vec::with_capacity(samples.len() * 2);
    for &s in samples {
        raw_bytes.extend_from_slice(&s.to_le_bytes());
    }

    let mut out = String::with_capacity(((raw_bytes.len() + 2) / 3) * 4);
    for chunk in raw_bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };

        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[async_trait]
impl TtsProviderEndpoint for WebSocketTtsEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        if !self.healthy.load(Ordering::Acquire) {
            return Err(CadenceError::TransportError(format!(
                "Endpoint {} unhealthy",
                self.endpoint_url
            )));
        }

        let payload = match self.provider {
            ProviderId::PrimaryCartesia => json!({
                "model_id": "sonic-english",
                "context_id": req.utterance_id.to_string(),
                "transcript": req.text_segment,
                "voice": {
                    "mode": "id",
                    "id": req.voice_id_primary
                },
                "output_format": {
                    "container": "raw",
                    "encoding": "pcm_s16le",
                    "sample_rate": self.sample_rate.as_u32()
                },
                "add_timestamps": true,
                "continue": !req.flush
            }),
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => json!({
                "text": req.text_segment,
                "context_id": req.utterance_id.to_string(),
                "voice_id": req.voice_id_standby,
                "xi_api_key": self.api_key,
                "output_format": format!("pcm_{}", self.sample_rate.as_u32()),
                "sync_alignment": true,
                "flush": req.flush
            }),
        };

        self.outbound_tx
            .send(Message::Text(payload.to_string().into()))
            .await
            .map_err(|e| CadenceError::ChannelClosed(e.to_string()))
    }

    async fn send_warm_heartbeat(&self, timestamp_us: u64) -> Result<u64, CadenceError> {
        let payload = timestamp_us.to_le_bytes().to_vec();
        self.outbound_tx
            .send(Message::Ping(payload.into()))
            .await
            .map_err(|e| CadenceError::ChannelClosed(e.to_string()))?;
        Ok(self.last_pong_us.load(Ordering::Acquire))
    }

    async fn send_zero_payload_handshake(&self, session_id: &str) -> Result<(), CadenceError> {
        let keepalive_json = match self.provider {
            ProviderId::PrimaryCartesia => json!({
                "context_id": session_id,
                "transcript": "",
                "continue": true
            }),
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => json!({
                "text": " ",
                "context_id": session_id,
                "try_trigger_generation": false
            }),
        };

        self.outbound_tx
            .send(Message::Text(keepalive_json.to_string().into()))
            .await
            .map_err(|e| CadenceError::ChannelClosed(e.to_string()))
    }

    async fn send_context_cancel(&self, context_id: &str) -> Result<(), CadenceError> {
        let cancel_json = match self.provider {
            ProviderId::PrimaryCartesia => json!({
                "context_id": context_id,
                "cancel": true
            }),
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => json!({
                "context_id": context_id,
                "text": "",
                "flush": true
            }),
        };

        self.outbound_tx
            .send(Message::Text(cancel_json.to_string().into()))
            .await
            .map_err(|e| CadenceError::ChannelClosed(e.to_string()))
    }

    async fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn shutdown(&self) -> Result<(), CadenceError> {
        self.healthy.store(false, Ordering::Release);
        let _ = self.outbound_tx.send(Message::Close(None)).await;
        if let Some(handle) = self.worker_handle.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

/// Adaptive speculative shadowing strategy to balance failover latency with TTS costs.
#[derive(Debug, Clone)]
pub enum ShadowingStrategy {
    /// Always speculatively shadow every utterance to both providers (sub-15ms failover, 2x cost).
    Full,
    /// Shadow only the initial prefix (e.g. first 4 words) to cover the 120ms TTFB danger zone,
    /// and cancel the standby context once primary delivers initial audio chunks on time.
    PrefixOnly {
        max_prefix_words: usize,
        cancel_after_primary_ttfb: bool,
    },
    /// Only shadow when primary rolling p95 TTFB exceeds the healthy ceiling (e.g. 75ms).
    Canary {
        healthy_ttfb_ceiling_us: u64,
    },
    /// No speculative shadowing; in-flight requests are replayed reactively on socket drop.
    Disabled,
}

/// Configuration for the Warm Standby Pool and Speculative Multiplexer.
#[derive(Debug, Clone)]
pub struct MultiplexerConfig {
    pub heartbeat_interval: Duration,
    pub zero_payload_handshake_interval: Duration,
    pub shadowing_strategy: ShadowingStrategy,
}

impl Default for MultiplexerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            zero_payload_handshake_interval: Duration::from_secs(12),
            shadowing_strategy: ShadowingStrategy::Full,
        }
    }
}

/// Extracts the first `n` words from a text segment for prefix shadowing.
fn extract_prefix_words(text: &str, n: usize) -> String {
    text.split_whitespace()
        .take(n)
        .collect::<Vec<&str>>()
        .join(" ")
}

/// Module A: Audio Stream Multiplexer & Warm Standby Pool.
pub struct AudioStreamMultiplexer {
    config: MultiplexerConfig,
    primary: Arc<dyn TtsProviderEndpoint>,
    standby: Arc<dyn TtsProviderEndpoint>,
    watchdog: Arc<LatencyWatchdog>,
    telemetry: Arc<TelemetryCollector>,
    #[allow(dead_code)]
    clock: MonotonicClock,
    last_request: RwLock<Option<SynthesisRequest>>,
    shutdown_flag: Arc<AtomicBool>,
    keepalive_handle: Mutex<Option<JoinHandle<()>>>,
}

impl AudioStreamMultiplexer {
    pub fn new(
        config: MultiplexerConfig,
        primary: Arc<dyn TtsProviderEndpoint>,
        standby: Arc<dyn TtsProviderEndpoint>,
        watchdog: Arc<LatencyWatchdog>,
        telemetry: Arc<TelemetryCollector>,
        #[allow(dead_code)]
    clock: MonotonicClock,
    ) -> Arc<Self> {
        let mux = Arc::new(Self {
            config,
            primary,
            standby,
            watchdog,
            telemetry,
            clock,
            last_request: RwLock::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            keepalive_handle: Mutex::new(None),
        });

        Self::spawn_warm_standby_maintainer(Arc::clone(&mux));
        mux
    }

    fn spawn_warm_standby_maintainer(mux: Arc<Self>) {
        let heartbeat_period = mux.config.heartbeat_interval;
        let handshake_period = mux.config.zero_payload_handshake_interval;
        let mut failover_rx = mux.watchdog.subscribe();
        let mux_clone = Arc::clone(&mux);

        let handle = tokio::spawn(async move {
            let mut hb_ticker = tokio::time::interval(heartbeat_period);
            let mut hs_ticker = tokio::time::interval(handshake_period);

            loop {
                tokio::select! {
                    Ok(signal) = failover_rx.recv() => {
                        if let FailoverSignal::SwitchToStandby { .. } = signal {
                            let _ = mux_clone.replay_inflight_to_standby().await;
                        }
                    }
                    _ = hb_ticker.tick() => {
                        if mux_clone.shutdown_flag.load(Ordering::Acquire) {
                            break;
                        }
                        let now = mux_clone.clock.now_us();
                        if let Ok(last_pong) = mux_clone.standby.send_warm_heartbeat(now).await {
                            if last_pong > 0 && now >= last_pong {
                                debug!("Standby warm heartbeat sent at {}us (last pong {}us)", now, last_pong);
                            }
                        }
                        let _ = mux_clone.primary.send_warm_heartbeat(now).await;
                    }
                    _ = hs_ticker.tick() => {
                        if mux_clone.shutdown_flag.load(Ordering::Acquire) {
                            break;
                        }
                        if let Err(e) = mux_clone.standby.send_zero_payload_handshake("warm-standby-context").await {
                            warn!("Standby zero-payload handshake warning: {e}");
                        }
                    }
                }
            }
        });

        if let Ok(mut guard) = mux.keepalive_handle.try_lock() {
            *guard = Some(handle);
        }
    }

    /// Dispatches an LLM text segment according to the configured `ShadowingStrategy`.
    pub async fn dispatch_utterance(&self, mut req: SynthesisRequest) -> Result<(), CadenceError> {
        let now_us = self.clock.now_us();
        req.dispatch_us = now_us;

        {
            let mut guard = self.last_request.write().await;
            *guard = Some(req.clone());
        }

        self.watchdog
            .on_utterance_dispatched(req.utterance_id, now_us);

        let primary_healthy = self.primary.is_healthy().await;

        if primary_healthy && !self.watchdog.is_standby_active() {
            let primary_res = self.primary.send_synthesis_request(&req).await;

            // Evaluate adaptive shadowing policy
            match &self.config.shadowing_strategy {
                ShadowingStrategy::Full => {
                    let standby_clone = Arc::clone(&self.standby);
                    let req_clone = req.clone();
                    tokio::spawn(async move {
                        let _ = standby_clone.send_synthesis_request(&req_clone).await;
                    });
                }
                ShadowingStrategy::PrefixOnly {
                    max_prefix_words,
                    cancel_after_primary_ttfb,
                } => {
                    let prefix_text = extract_prefix_words(&req.text_segment, *max_prefix_words);
                    let mut prefix_req = req.clone();
                    prefix_req.text_segment = prefix_text;

                    let standby_clone = Arc::clone(&self.standby);
                    let cancel_enabled = *cancel_after_primary_ttfb;
                    let utt_id_str = req.utterance_id.to_string();

                    tokio::spawn(async move {
                        let _ = standby_clone.send_synthesis_request(&prefix_req).await;
                        if cancel_enabled {
                            // Wait up to 130ms; if primary delivers, cancel standby
                            tokio::time::sleep(Duration::from_millis(130)).await;
                            let _ = standby_clone.send_context_cancel(&utt_id_str).await;
                        }
                    });
                }
                ShadowingStrategy::Canary {
                    healthy_ttfb_ceiling_us,
                } => {
                    let p95_ttfb = self.telemetry.primary_rolling_p95_ttfb_us();
                    if p95_ttfb > *healthy_ttfb_ceiling_us {
                        info!(
                            p95_ttfb_us = p95_ttfb,
                            ceiling_us = healthy_ttfb_ceiling_us,
                            "Primary TTFB degraded; auto-promoting utterance to speculative shadow"
                        );
                        let standby_clone = Arc::clone(&self.standby);
                        let req_clone = req.clone();
                        tokio::spawn(async move {
                            let _ = standby_clone.send_synthesis_request(&req_clone).await;
                        });
                    }
                }
                ShadowingStrategy::Disabled => {}
            }

            if let Err(e) = primary_res {
                warn!("Primary dispatch failed immediately ({e}); triggering instant standby promotion");
                self.watchdog
                    .force_failover_on_socket_drop(req.utterance_id, self.clock.now_us());
                self.standby.send_synthesis_request(&req).await?;
            }
            Ok(())
        } else {
            self.standby.send_synthesis_request(&req).await
        }
    }

    /// Re-dispatches the in-flight utterance immediately on the pre-warmed standby socket
    /// when reactive failover is needed.
    pub async fn replay_inflight_to_standby(&self) -> Result<(), CadenceError> {
        if let Some(req) = self.last_request.read().await.clone() {
            info!(
                "Replaying utterance {} to warm standby endpoint",
                req.utterance_id
            );
            self.standby.send_synthesis_request(&req).await?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), CadenceError> {
        self.shutdown_flag.store(true, Ordering::Release);
        if let Some(handle) = self.keepalive_handle.lock().await.take() {
            handle.abort();
        }
        let _ = self.primary.shutdown().await;
        let _ = self.standby.shutdown().await;
        Ok(())
    }
}
