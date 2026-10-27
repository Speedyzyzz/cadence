//! src/server.rs
//! Multi-session Axum WebSocket Ingress Server for Cadence.
//! Accepts concurrent voice agent connections, manages per-session isolated
//! pipelines (Watchdog, Splicer, Multiplexer), streams raw PCM audio chunks
//! to downstream agents (LiveKit Agents, Pipecat), and serves /healthz, /metrics,
//! and /v1/sessions management endpoints.

use crate::multiplexer::{
    AudioStreamMultiplexer, MultiplexerConfig, ShadowingStrategy, TtsProviderEndpoint,
    WebSocketTtsEndpoint,
};
use crate::protocol::{
    AudioChunk, CadenceError, MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use crate::splicer::StatefulSplicer;
use crate::telemetry::TelemetryCollector;
use crate::watchdog::{LatencyWatchdog, WatchdogThresholds};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize)]
pub struct SessionMeta {
    pub session_id: String,
    pub created_at_us: u64,
    pub active_provider: String,
}

#[derive(Clone)]
pub struct AppState {
    pub telemetry: Arc<TelemetryCollector>,
    pub clock: MonotonicClock,
    pub sessions: Arc<DashMap<String, SessionMeta>>,
    pub default_primary_ws: String,
    pub default_standby_ws: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientSynthesizeMsg {
    pub r#type: Option<String>,
    pub utterance_id: u64,
    pub text: String,
    #[serde(default)]
    pub voice_id_primary: String,
    #[serde(default)]
    pub voice_id_standby: String,
    #[serde(default = "default_flush")]
    pub flush: bool,
}

fn default_flush() -> bool {
    true
}

/// Builds the production Axum HTTP + WebSocket router.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health_check))
        .route("/metrics", get(metrics_handler))
        .route("/v1/sessions", get(sessions_handler))
        .route("/v1/tts/stream", get(ws_tts_stream_handler))
        .with_state(state)
}

/// Liveness probe endpoint.
async fn health_check() -> (StatusCode, &'static str) {
    (StatusCode::OK, "cadence_healthy\n")
}

/// Prometheus exposition format exporter.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = state.telemetry.render_prometheus_metrics();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        metrics,
    )
}

/// Active session inventory endpoint.
async fn sessions_handler(State(state): State<AppState>) -> Json<Vec<SessionMeta>> {
    let list: Vec<SessionMeta> = state.sessions.iter().map(|kv| kv.value().clone()).collect();
    Json(list)
}

/// WebSocket TTS stream handler.
async fn ws_tts_stream_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let session_id = headers
        .get("X-Cadence-Session-Id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("sess-{}", state.clock.now_us()));

    ws.on_upgrade(move |socket| handle_session_socket(socket, session_id, state))
}

/// Local fallback sine-wave endpoint used when external WebSocket URLs are not reachable.
struct LocalLoopbackEndpoint {
    provider: ProviderId,
    sample_rate: SampleRate,
    clock: MonotonicClock,
    chunk_tx: mpsc::Sender<AudioChunk>,
    healthy: AtomicBool,
}

#[async_trait::async_trait]
impl TtsProviderEndpoint for LocalLoopbackEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        let tx = self.chunk_tx.clone();
        let clock = self.clock.clone();
        let rate = self.sample_rate;
        let provider = self.provider;
        let utt_id = req.utterance_id;

        tokio::spawn(async move {
            let samples_per_10ms = rate.samples_for_ms(10);
            for seq in 0..10u64 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let chunk = AudioChunk {
                    utterance_id: utt_id,
                    sequence: seq,
                    provider,
                    sample_rate: rate,
                    samples: Arc::from(vec![2000i16; samples_per_10ms]),
                    arrival_us: clock.now_us(),
                    is_first_chunk: seq == 0,
                    is_final_chunk: seq == 9,
                    alignment_json: None,
                };
                if tx.send(chunk).await.is_err() {
                    break;
                }
            }
        });
        Ok(())
    }

    async fn send_warm_heartbeat(&self, timestamp_us: u64) -> Result<u64, CadenceError> {
        Ok(timestamp_us)
    }

    async fn send_zero_payload_handshake(&self, _session_id: &str) -> Result<(), CadenceError> {
        Ok(())
    }

    async fn send_context_cancel(&self, _context_id: &str) -> Result<(), CadenceError> {
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn shutdown(&self) -> Result<(), CadenceError> {
        self.healthy.store(false, Ordering::Release);
        Ok(())
    }
}

/// Core per-session WebSocket connection actor loop.
async fn handle_session_socket(socket: WebSocket, session_id: String, state: AppState) {
    info!(session_id = %session_id, "New Cadence WebSocket session connected");

    state.sessions.insert(
        session_id.clone(),
        SessionMeta {
            session_id: session_id.clone(),
            created_at_us: state.clock.now_us(),
            active_provider: "cartesia_primary".to_string(),
        },
    );

    let (mut client_ws_tx, mut client_ws_rx) = socket.split();

    // Create session-isolated audio pipelines
    let (ingress_chunk_tx, ingress_chunk_rx) = mpsc::channel(2048);
    let (splicer_chunk_tx, splicer_chunk_rx) = mpsc::channel(2048);
    let (egress_tx, mut egress_rx) = mpsc::channel(2048);

    let (watchdog, failover_rx) = LatencyWatchdog::new(
        WatchdogThresholds::default(),
        state.clock.clone(),
        Arc::clone(&state.telemetry),
    );
    let (tap_h, poll_h) = watchdog.spawn_monitor_tasks(ingress_chunk_rx, splicer_chunk_tx);

    let splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0xCADE_0001,
        state.clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&state.telemetry),
    );
    let splicer_h = splicer.spawn_realtime_loop(splicer_chunk_rx, failover_rx, egress_tx);

    // Initialize TTS endpoints: try connecting to configured WS endpoints; fall back to loopback mock
    let primary_endpoint: Arc<dyn TtsProviderEndpoint> =
        if !state.default_primary_ws.is_empty() {
            match WebSocketTtsEndpoint::connect(
                ProviderId::PrimaryCartesia,
                state.default_primary_ws.clone(),
                std::env::var("CARTESIA_API_KEY").unwrap_or_default(),
                SampleRate::Hz24000,
                state.clock.clone(),
                ingress_chunk_tx.clone(),
                Arc::clone(&state.telemetry),
            )
            .await
            {
                Ok(ep) => ep,
                Err(e) => {
                    warn!("Primary WS connection failed ({e}); using local loopback synthesizer");
                    Arc::new(LocalLoopbackEndpoint {
                        provider: ProviderId::PrimaryCartesia,
                        sample_rate: SampleRate::Hz24000,
                        clock: state.clock.clone(),
                        chunk_tx: ingress_chunk_tx.clone(),
                        healthy: AtomicBool::new(true),
                    })
                }
            }
        } else {
            Arc::new(LocalLoopbackEndpoint {
                provider: ProviderId::PrimaryCartesia,
                sample_rate: SampleRate::Hz24000,
                clock: state.clock.clone(),
                chunk_tx: ingress_chunk_tx.clone(),
                healthy: AtomicBool::new(true),
            })
        };

    let standby_endpoint: Arc<dyn TtsProviderEndpoint> =
        if !state.default_standby_ws.is_empty() {
            match WebSocketTtsEndpoint::connect(
                ProviderId::StandbyElevenLabs,
                state.default_standby_ws.clone(),
                std::env::var("ELEVENLABS_API_KEY").unwrap_or_default(),
                SampleRate::Hz24000,
                state.clock.clone(),
                ingress_chunk_tx.clone(),
                Arc::clone(&state.telemetry),
            )
            .await
            {
                Ok(ep) => ep,
                Err(e) => {
                    warn!("Standby WS connection failed ({e}); using local loopback synthesizer");
                    Arc::new(LocalLoopbackEndpoint {
                        provider: ProviderId::StandbyElevenLabs,
                        sample_rate: SampleRate::Hz24000,
                        clock: state.clock.clone(),
                        chunk_tx: ingress_chunk_tx,
                        healthy: AtomicBool::new(true),
                    })
                }
            }
        } else {
            Arc::new(LocalLoopbackEndpoint {
                provider: ProviderId::StandbyElevenLabs,
                sample_rate: SampleRate::Hz24000,
                clock: state.clock.clone(),
                chunk_tx: ingress_chunk_tx,
                healthy: AtomicBool::new(true),
            })
        };

    let multiplexer = AudioStreamMultiplexer::new(
        MultiplexerConfig {
            heartbeat_interval: std::time::Duration::from_secs(5),
            zero_payload_handshake_interval: std::time::Duration::from_secs(12),
            shadowing_strategy: ShadowingStrategy::Full,
        },
        primary_endpoint,
        standby_endpoint,
        Arc::clone(&watchdog),
        Arc::clone(&state.telemetry),
        state.clock.clone(),
    );

    let session_id_egress = session_id.clone();
    let mux_ref = Arc::clone(&multiplexer);

    // Egress loop: streams 10ms PCM audio frames down to the client WebSocket
    let mut egress_send_task = tokio::spawn(async move {
        while let Some(frame) = egress_rx.recv().await {
            // Forward raw PCM s16le bytes as binary WebSocket frame
            let mut pcm_bytes = Vec::with_capacity(frame.pcm_samples.len() * 2);
            for &s in &frame.pcm_samples {
                pcm_bytes.extend_from_slice(&s.to_le_bytes());
            }

            if client_ws_tx.send(Message::Binary(pcm_bytes)).await.is_err() {
                break;
            }

            // If a hot-splice occurred in this frame, also emit telemetry event JSON
            if frame.spliced_in_this_frame {
                let event_json = serde_json::json!({
                    "type": "failover_event",
                    "session_id": session_id_egress,
                    "active_provider": frame.active_provider.to_string(),
                    "pts_us": frame.pts_us,
                    "phase_offset_samples": frame.phase_offset_samples,
                });
                let _ = client_ws_tx.send(Message::Text(event_json.to_string())).await;
            }
        }
    });

    // Ingress loop: reads client "synthesize" requests and dispatches to multiplexer
    let session_id_in = session_id.clone();
    let clock_in = state.clock.clone();
    let mut ingress_recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = client_ws_rx.next().await {
            match msg {
                Message::Text(txt) => {
                    if let Ok(req_val) = serde_json::from_str::<ClientSynthesizeMsg>(&txt) {
                        let synth_req = SynthesisRequest {
                            session_id: session_id_in.clone(),
                            utterance_id: req_val.utterance_id,
                            text_segment: req_val.text,
                            voice_id_primary: req_val.voice_id_primary,
                            voice_id_standby: req_val.voice_id_standby,
                            dispatch_us: clock_in.now_us(),
                            flush: req_val.flush,
                        };
                        if let Err(e) = mux_ref.dispatch_utterance(synth_req).await {
                            error!("Dispatch error in session {session_id_in}: {e}");
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // Wait until either task completes (disconnect or error)
    tokio::select! {
        _ = (&mut egress_send_task) => ingress_recv_task.abort(),
        _ = (&mut ingress_recv_task) => egress_send_task.abort(),
    }

    // Teardown session resources
    tap_h.abort();
    poll_h.abort();
    splicer_h.abort();
    let _ = multiplexer.shutdown().await;

    state.sessions.remove(&session_id);
    info!(session_id = %session_id, "Cadence session terminated cleanly");
}

/// Spawns the production Axum HTTP and WebSocket ingress server on the specified bind address.
pub async fn run_ingress_server(
    bind_addr: &str,
    telemetry: Arc<TelemetryCollector>,
    clock: MonotonicClock,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState {
        telemetry,
        clock,
        sessions: Arc::new(DashMap::new()),
        default_primary_ws: std::env::var("CARTESIA_WS_URL")
            .unwrap_or_else(|_| "wss://api.cartesia.ai/tts/websocket".to_string()),
        default_standby_ws: std::env::var("ELEVENLABS_WS_URL")
            .unwrap_or_else(|_| "wss://api.elevenlabs.io/v1/text-to-speech".to_string()),
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!("Cadence Ingress Server active on http://{}", bind_addr);
    info!("  WebSocket TTS route: ws://{}/v1/tts/stream", bind_addr);
    info!("  Liveness probe:      http://{}/healthz", bind_addr);
    info!("  Prometheus metrics:  http://{}/metrics", bind_addr);

    axum::serve(listener, app).await?;
    Ok(())
}
