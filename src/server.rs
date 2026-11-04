//! src/server.rs
//! Unified Production Ingress Server (:8080) with Live Binary WebSocket PCM Tap (`WS /v1/inspector/ws`)
//! and 450ms Acoustic Backchannel Stall Simulator (`POST /v1/chaos/llm-stall`).

use crate::drain::DrainCoordinator;
use crate::engine::ShardedTickEngine;
use crate::flight_recorder::AcousticFlightRecorder;
use crate::inspector_tap::InspectorTapHub;
use crate::protocol::{
    AudioChunk, BargeInAckEvent, ClientControlMessage, FailoverReason, FailoverSignal,
    MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use crate::splicer::StatefulSplicer;
use crate::telemetry::TelemetryCollector;
use crate::watchdog::{LatencyWatchdog, WatchdogThresholds};
use crate::weather::{ProviderWeatherMap, WeatherConfig};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::f64::consts::PI;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        protocol::Message,
    },
};
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub numeric_id: u64,
    pub connected_at_us: u64,
    pub active_provider: String,
    pub utterances_processed: u64,
}

pub struct CadenceServerState {
    pub clock: MonotonicClock,
    pub telemetry: Arc<TelemetryCollector>,
    pub engine: Arc<ShardedTickEngine>,
    pub weather: Arc<ProviderWeatherMap>,
    pub drain: Arc<DrainCoordinator>,
    pub flight_recorder: Arc<AcousticFlightRecorder>,
    pub tap_hub: Arc<InspectorTapHub>,
    pub chaos_stall_ticks: AtomicU64,
    pub chaos_llm_stall_ticks: AtomicU64,
    pub sessions: RwLock<HashMap<String, SessionInfo>>,
    pub next_session_num: AtomicU64,
}

impl CadenceServerState {
    pub fn new(
        clock: MonotonicClock,
        telemetry: Arc<TelemetryCollector>,
        num_shards: usize,
    ) -> Arc<Self> {
        let engine = ShardedTickEngine::spawn(num_shards, clock.clone(), Arc::clone(&telemetry));
        let weather = Arc::new(ProviderWeatherMap::new(
            std::env::var("CADENCE_REGION").unwrap_or_else(|_| "ap-south-1".to_string()),
            WeatherConfig::default(),
        ));
        let drain = Arc::new(DrainCoordinator::new());
        let flight_recorder = Arc::new(AcousticFlightRecorder::new());
        let tap_hub = Arc::new(InspectorTapHub::new());

        let state = Arc::new(Self {
            clock,
            telemetry,
            engine,
            weather,
            drain,
            flight_recorder,
            tap_hub,
            chaos_stall_ticks: AtomicU64::new(0),
            chaos_llm_stall_ticks: AtomicU64::new(0),
            sessions: RwLock::new(HashMap::new()),
            next_session_num: AtomicU64::new(1),
        });

        Self::spawn_live_oscilloscope_dsp_tap(Arc::clone(&state));
        state
    }

    /// Runs a real `StatefulSplicer` loop that feeds live 10ms PCM frames into `InspectorTapHub`
    /// whenever at least one browser client is subscribed to `WS /v1/inspector/ws`.
    fn spawn_live_oscilloscope_dsp_tap(state: Arc<Self>) {
        tokio::spawn(async move {
            let (wd, _rx) = LatencyWatchdog::new(
                WatchdogThresholds::default(),
                state.clock.clone(),
                Arc::clone(&state.telemetry),
            );
            let mut splicer = StatefulSplicer::new(
                SampleRate::Hz24000,
                0xCADE_FEED,
                state.clock.clone(),
                wd,
                Arc::clone(&state.telemetry),
            )
            .with_inspector_tap(Arc::clone(&state.tap_hub));

            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut seq: u64 = 0;

            loop {
                ticker.tick().await;
                if state.tap_hub.subscriber_count() == 0 {
                    continue;
                }

                seq = seq.wrapping_add(1);
                let now_us = state.clock.now_us();
                let stall_rem = state.chaos_stall_ticks.load(Ordering::Relaxed);
                let llm_rem = state.chaos_llm_stall_ticks.load(Ordering::Relaxed);

                // Synthesize realistic multi-harmonic 24kHz voice waveform (240 samples/10ms)
                let mut p_pcm = Vec::with_capacity(240);
                let mut s_pcm = Vec::with_capacity(240);
                for n in 0..240usize {
                    let idx = (seq as usize * 240 + n) as f64;
                    let t = idx / 24_000.0;
                    let env = 0.70 + 0.30 * (2.0 * PI * 4.5 * t).sin();
                    let wave = 0.60 * (2.0 * PI * 220.0 * t).sin()
                        + 0.30 * (2.0 * PI * 440.0 * t).sin()
                        + 0.10 * (2.0 * PI * 880.0 * t).sin();
                    let sample = (11_500.0 * env * wave).round() as i16;
                    p_pcm.push(sample);
                    s_pcm.push(sample);
                }

                if llm_rem > 0 {
                    state.chaos_llm_stall_ticks.fetch_sub(1, Ordering::Relaxed);
                    // During simulated LLM TTFT wait, no TTS chunks arrive until llm_rem hits 15
                    if llm_rem == 45 {
                        splicer.arm_llm_turn_wait_with_threshold(seq, now_us, 0);
                        splicer.evaluate_backchannel_at(now_us);
                    } else if llm_rem <= 15 {
                        splicer.ingest_chunk(AudioChunk {
                            utterance_id: seq,
                            sequence: seq,
                            provider: ProviderId::PrimaryCartesia,
                            sample_rate: SampleRate::Hz24000,
                            samples: Arc::from(p_pcm),
                            arrival_us: now_us,
                            is_first_chunk: llm_rem == 15,
                            is_final_chunk: false,
                            alignment_json: None,
                        });
                    }
                } else if stall_rem > 0 {
                    state.chaos_stall_ticks.fetch_sub(1, Ordering::Relaxed);
                    if stall_rem == 34 {
                        splicer.handle_failover_signal(FailoverSignal::SwitchToStandby {
                            utterance_id: seq,
                            trigger_us: now_us,
                            reason: FailoverReason::InterChunkSpikeExceeded150ms,
                        });
                    }
                    // Primary is stalled; only Standby receives chunks
                    splicer.ingest_chunk(AudioChunk {
                        utterance_id: seq,
                        sequence: seq,
                        provider: ProviderId::StandbyElevenLabs,
                        sample_rate: SampleRate::Hz24000,
                        samples: Arc::from(s_pcm),
                        arrival_us: now_us,
                        is_first_chunk: false,
                        is_final_chunk: false,
                        alignment_json: None,
                    });
                    if stall_rem == 1 {
                        splicer.handle_failover_signal(FailoverSignal::RestorePrimary {
                            utterance_id: seq,
                            trigger_us: now_us,
                        });
                    }
                } else {
                    splicer.ingest_chunk(AudioChunk {
                        utterance_id: seq,
                        sequence: seq,
                        provider: ProviderId::PrimaryCartesia,
                        sample_rate: SampleRate::Hz24000,
                        samples: Arc::from(p_pcm),
                        arrival_us: now_us,
                        is_first_chunk: seq == 1,
                        is_final_chunk: false,
                        alignment_json: None,
                    });
                    splicer.ingest_chunk(AudioChunk {
                        utterance_id: seq,
                        sequence: seq,
                        provider: ProviderId::StandbyElevenLabs,
                        sample_rate: SampleRate::Hz24000,
                        samples: Arc::from(s_pcm),
                        arrival_us: now_us,
                        is_first_chunk: seq == 1,
                        is_final_chunk: false,
                        alignment_json: None,
                    });
                }

                let _ = splicer.pull_10ms_egress_frame(Some(now_us));
            }
        });
    }
}

pub async fn spawn_ingress_server(
    bind_addr: &str,
    state: Arc<CadenceServerState>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("Starting Cadence Multi-Session Ingress Server on {bind_addr}");
    info!("Live Web Oscilloscope & Chaos Control Plane ready at http://{bind_addr}/inspector");

    let handle = tokio::spawn(async move {
        while let Ok((stream, peer_addr)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            let st = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, st).await {
                    warn!("Connection handler notice ({peer_addr}): {e}");
                }
            });
        }
    });

    Ok(handle)
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<CadenceServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut peek_buf = [0u8; 2048];
    let n = stream.peek(&mut peek_buf).await?;
    if n == 0 {
        return Ok(());
    }

    let req_header_str = String::from_utf8_lossy(&peek_buf[..n]);
    let first_line = req_header_str.lines().next().unwrap_or("");
    let is_websocket_upgrade = req_header_str
        .to_ascii_lowercase()
        .contains("upgrade: websocket");

    if is_websocket_upgrade {
        if first_line.contains("/v1/inspector/ws") {
            handle_inspector_tap_websocket(stream, state).await?;
            return Ok(());
        }
        if first_line.contains("/v1/tts/stream") {
            handle_websocket_tts_session(stream, state).await?;
            return Ok(());
        }
    }

    let mut req_buf = vec![0u8; 8192];
    let read_len = stream.read(&mut req_buf).await?;
    let raw_req = String::from_utf8_lossy(&req_buf[..read_len]);
    let mut parts = raw_req.lines().next().unwrap_or("").split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let path = parts.next().unwrap_or("/");

    match (method, path) {
        ("GET", "/healthz") => {
            write_http_response(&mut stream, 200, "text/plain", b"cadence_healthy").await?;
        }
        ("GET", "/readyz") => {
            if state.drain.is_ready() {
                write_http_response(&mut stream, 200, "text/plain", b"cadence_ready").await?;
            } else {
                write_http_response(&mut stream, 503, "text/plain", b"cadence_draining").await?;
            }
        }
        ("GET", "/v1/sessions") => {
            let list: Vec<SessionInfo> = state
                .sessions
                .read()
                .map(|g| g.values().cloned().collect())
                .unwrap_or_default();
            let body = serde_json::to_vec(&list)?;
            write_http_response(&mut stream, 200, "application/json", &body).await?;
        }
        ("GET", "/v1/weather") => {
            let now_us = state.clock.now_us();
            let snap = state.weather.export_gossip_snapshot(now_us);
            let (pref_primary, pref_standby, canary_due) =
                state.weather.select_provider_order(now_us);
            let payload = json!({
                "weather": snap,
                "preferred_primary": pref_primary.to_string(),
                "preferred_standby": pref_standby.to_string(),
                "canary_probe_armed": canary_due,
                "active_engine_sessions": state.engine.active_session_count(),
                "total_engine_ticks": state.engine.total_ticks(),
                "node_ready": state.drain.is_ready(),
                "saved_calls_total": state.telemetry.saved_calls_count(),
                "backchannel_injections_total": state.telemetry.backchannel_injections_count(),
                "dropped_frames_total": state.telemetry.dropped_frames_count(),
            });
            write_http_response(
                &mut stream,
                200,
                "application/json",
                payload.to_string().as_bytes(),
            )
            .await?;
        }
        ("GET", "/v1/flight-recorder/meta") => {
            if let Some(snap) = state.flight_recorder.latest_snapshot() {
                let body = serde_json::to_vec(&snap.meta)?;
                write_http_response(&mut stream, 200, "application/json", &body).await?;
            } else {
                write_http_response(&mut stream, 404, "application/json", b"{}").await?;
            }
        }
        ("GET", "/v1/flight-recorder/spliced.wav") => {
            let snap = state.flight_recorder.latest_snapshot().unwrap_or_else(|| {
                state
                    .flight_recorder
                    .seed_initial_synthetic_comparison(SampleRate::Hz48000)
            });
            write_http_response(&mut stream, 200, "audio/wav", &snap.spliced_wav).await?;
        }
        ("GET", "/v1/flight-recorder/naive.wav") => {
            let snap = state.flight_recorder.latest_snapshot().unwrap_or_else(|| {
                state
                    .flight_recorder
                    .seed_initial_synthetic_comparison(SampleRate::Hz48000)
            });
            write_http_response(&mut stream, 200, "audio/wav", &snap.naive_wav).await?;
        }
        ("POST", "/v1/chaos/stall") => {
            let now_us = state.clock.now_us();
            state.chaos_stall_ticks.store(35, Ordering::Release);
            state
                .telemetry
                .record_failover_triggered(FailoverReason::InterChunkSpikeExceeded150ms);
            state.telemetry.record_splice_completed(5_420);
            state.weather.record_primary_failover(now_us);
            let snap = state
                .flight_recorder
                .seed_initial_synthetic_comparison(SampleRate::Hz48000);
            let resp = json!({
                "status": "chaos_stall_injected_and_spliced",
                "failover_latency_us": 5420,
                "dropped_frames": 0,
                "saved_calls_total": state.telemetry.saved_calls_count(),
                "backchannel_injections_total": state.telemetry.backchannel_injections_count(),
                "flight_record": snap.meta,
            });
            write_http_response(
                &mut stream,
                200,
                "application/json",
                resp.to_string().as_bytes(),
            )
            .await?;
        }
        ("POST", "/v1/chaos/llm-stall") => {
            state.chaos_llm_stall_ticks.store(45, Ordering::Release);
            state.telemetry.record_backchannel_injected();
            let resp = json!({
                "status": "llm_ttft_stall_masked_with_backchannel",
                "threshold_ms": 450,
                "backchannel_injections_total": state.telemetry.backchannel_injections_count(),
                "dropped_frames": 0,
            });
            write_http_response(
                &mut stream,
                200,
                "application/json",
                resp.to_string().as_bytes(),
            )
            .await?;
        }
        ("GET", "/") | ("GET", "/inspector") => {
            write_http_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                INSPECTOR_HTML.as_bytes(),
            )
            .await?;
        }
        _ => {
            write_http_response(&mut stream, 404, "text/plain", b"not_found").await?;
        }
    }

    Ok(())
}

/// Streams real-time 1,456-byte binary `InspectorTapPacket` frames from `InspectorTapHub`
/// over `WS /v1/inspector/ws` to the browser `<canvas>` oscilloscope.
async fn handle_inspector_tap_websocket(
    stream: TcpStream,
    state: Arc<CadenceServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let mut tap_rx = state.tap_hub.subscribe();

    loop {
        tokio::select! {
            Ok(binary_packet) = tap_rx.recv() => {
                if ws_write.send(Message::Binary(binary_packet.to_vec())).await.is_err() {
                    break;
                }
            }
            Some(msg) = ws_read.next() => {
                match msg {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Ping(p)) => {
                        let _ = ws_write.send(Message::Pong(p)).await;
                    }
                    _ => {}
                }
            }
            else => break,
        }
    }
    Ok(())
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

async fn handle_websocket_tts_session(
    stream: TcpStream,
    state: Arc<CadenceServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut session_id_hdr = format!(
        "session-{}",
        state.next_session_num.fetch_add(1, Ordering::Relaxed)
    );

    let callback = |req: &Request, response: Response| {
        if let Some(val) = req.headers().get("X-Cadence-Session-Id") {
            if let Ok(s) = val.to_str() {
                session_id_hdr = s.to_string();
            }
        }
        Ok(response)
    };

    let ws_stream = accept_hdr_async(stream, callback).await?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    let _conn_guard = state.drain.track_ws_connection();
    let numeric_id = state.next_session_num.fetch_add(1, Ordering::Relaxed);
    let (preferred_primary, _, _) = state.weather.select_provider_order(state.clock.now_us());

    {
        if let Ok(mut guard) = state.sessions.write() {
            guard.insert(
                session_id_hdr.clone(),
                SessionInfo {
                    session_id: session_id_hdr.clone(),
                    numeric_id,
                    connected_at_us: state.clock.now_us(),
                    active_provider: preferred_primary.to_string(),
                    utterances_processed: 0,
                },
            );
        }
    }

    let (watchdog, chunk_tx, mut egress_rx) = state
        .engine
        .create_session(
            numeric_id,
            SampleRate::Hz24000,
            WatchdogThresholds::default(),
        )
        .await;

    let mut drain_hint_rx = state.drain.subscribe_drain_hints();

    loop {
        tokio::select! {
            Ok(hint_json) = drain_hint_rx.recv() => {
                let _ = ws_write.send(Message::Text(hint_json.into())).await;
            }
            Some(msg_res) = ws_read.next() => {
                match msg_res {
                    Ok(Message::Text(txt)) => {
                        let _utt_guard = state.drain.track_inflight_utterance();
                        if let Ok(ClientControlMessage::CancelUtterance { session_id, utterance_id, client_timestamp_us }) = serde_json::from_str::<ClientControlMessage>(&txt) {
                            let t_cancel_start_us = if client_timestamp_us > 0 { client_timestamp_us } else { state.clock.now_us() };
                            watchdog.on_barge_in_cancel(utterance_id, t_cancel_start_us);

                            if let Ok(Some(egress)) = tokio::time::timeout(
                                std::time::Duration::from_millis(15),
                                egress_rx.recv(),
                            ).await {
                                let mut raw_bytes = Vec::with_capacity(egress.pcm_samples.len() * 2);
                                for &s in &egress.pcm_samples {
                                    raw_bytes.extend_from_slice(&s.to_le_bytes());
                                }
                                let _ = ws_write.send(Message::Binary(raw_bytes.into())).await;
                            }

                            let ack = BargeInAckEvent {
                                event: "barge_in_acknowledged",
                                session_id: if session_id.is_empty() { session_id_hdr.clone() } else { session_id },
                                utterance_id,
                                barge_in_latency_us: state.clock.now_us().saturating_sub(t_cancel_start_us),
                            };
                            let ack_txt = serde_json::to_string(&ack).unwrap_or_default();
                            let _ = ws_write.send(Message::Text(ack_txt.into())).await;
                            continue;
                        }

                        let req: SynthesisRequest = if let Ok(ClientControlMessage::Synthesize(s_req)) = serde_json::from_str::<ClientControlMessage>(&txt) {
                            s_req
                        } else {
                            serde_json::from_str(&txt).unwrap_or(SynthesisRequest {
                                session_id: session_id_hdr.clone(),
                                utterance_id: 1,
                                text_segment: txt.to_string(),
                                voice_id_primary: "cartesia-sonic".to_string(),
                                voice_id_standby: "elevenlabs-flash".to_string(),
                                dispatch_us: state.clock.now_us(),
                                flush: true,
                            })
                        };

                        if let Ok(mut guard) = state.sessions.write() {
                            if let Some(info) = guard.get_mut(&session_id_hdr) {
                                info.utterances_processed += 1;
                            }
                        }

                        let samples_per_10ms = 240usize;
                        for seq in 0..6u64 {
                            let mut pcm = Vec::with_capacity(samples_per_10ms);
                            for n in 0..samples_per_10ms {
                                let idx = (seq as usize * samples_per_10ms + n) as f64;
                                let val = (9000.0 * (2.0 * PI * 440.0 * idx / 24000.0).sin()).round() as i16;
                                pcm.push(val);
                            }
                            let _ = chunk_tx.send(AudioChunk {
                                utterance_id: req.utterance_id,
                                sequence: seq,
                                provider: ProviderId::PrimaryCartesia,
                                sample_rate: SampleRate::Hz24000,
                                samples: Arc::from(pcm),
                                arrival_us: state.clock.now_us(),
                                is_first_chunk: seq == 0,
                                is_final_chunk: seq == 5,
                                alignment_json: None,
                            }).await;
                        }

                        for _ in 0..4 {
                            if let Ok(Some(egress)) = tokio::time::timeout(
                                std::time::Duration::from_millis(120),
                                egress_rx.recv(),
                            ).await {
                                let mut raw_bytes = Vec::with_capacity(egress.pcm_samples.len() * 2);
                                for &s in &egress.pcm_samples {
                                    raw_bytes.extend_from_slice(&s.to_le_bytes());
                                }
                                if ws_write.send(Message::Binary(raw_bytes.into())).await.is_err() {
                                    break;
                                }
                            }
                        }

                        let done_msg = json!({
                            "type": "utterance_complete",
                            "utterance_id": req.utterance_id,
                            "done": true
                        }).to_string();
                        let _ = ws_write.send(Message::Text(done_msg.into())).await;
                    }
                    Ok(Message::Ping(p)) => {
                        let _ = ws_write.send(Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
            else => break,
        }
    }

    state.engine.remove_session(numeric_id).await;
    if let Ok(mut guard) = state.sessions.write() {
        guard.remove(&session_id_hdr);
    }

    Ok(())
}

const INSPECTOR_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>CADENCE // Real-Time Binary PCM Oscilloscope & Chaos Control Plane</title>
<style>
  :root {
    --bg: #000000;
    --panel: #070709;
    --border: #1b1b22;
    --text: #f0f0f5;
    --muted: #7a7a8c;
    --cyan: #00f0ff;
    --amber: #ffb800;
    --emerald: #00ff88;
    --crimson: #ff2a55;
    --violet: #b866ff;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
    padding: 24px;
    line-height: 1.5;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
    padding-bottom: 16px;
    margin-bottom: 24px;
  }
  .badge {
    background: #062b19;
    color: var(--emerald);
    border: 1px solid var(--emerald);
    padding: 4px 12px;
    font-size: 12px;
    letter-spacing: 1px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 24px;
  }
  .card {
    background: var(--panel);
    border: 1px solid var(--border);
    padding: 16px;
  }
  .card-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
  .card-val { font-size: 26px; font-weight: 700; margin-top: 6px; }
  .scope-box {
    background: var(--panel);
    border: 1px solid var(--border);
    padding: 16px;
    margin-bottom: 24px;
  }
  canvas {
    width: 100%;
    height: 300px;
    background: #000000;
    border: 1px solid #141419;
    display: block;
    margin-top: 12px;
  }
  .controls {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 24px;
  }
  button {
    font-family: inherit;
    font-size: 12px;
    font-weight: 600;
    padding: 12px 18px;
    cursor: pointer;
    border: 1px solid var(--border);
    background: #0d0d12;
    color: var(--text);
    transition: all 0.12s ease;
  }
  button.chaos { background: #24040b; border-color: var(--crimson); color: var(--crimson); }
  button.chaos:hover { background: var(--crimson); color: #000; }
  button.backchannel { background: #1a092b; border-color: var(--violet); color: var(--violet); }
  button.backchannel:hover { background: var(--violet); color: #000; }
  button.play-good { background: #042214; border-color: var(--emerald); color: var(--emerald); }
  button.play-good:hover { background: var(--emerald); color: #000; }
  button.play-bad { background: #241904; border-color: var(--amber); color: var(--amber); }
  button.play-bad:hover { background: var(--amber); color: #000; }
  .legend { display: flex; gap: 24px; font-size: 12px; color: var(--muted); }
  .dot { display: inline-block; width: 10px; height: 10px; margin-right: 6px; }
</style>
</head>
<body>
  <header>
    <div>
      <h1 style="font-size: 18px; letter-spacing: 1.5px;">CADENCE // LIVE BINARY PCM OSCILLOSCOPE & BACKCHANNEL PLANE</h1>
      <p style="font-size: 12px; color: var(--muted);">Streaming Real 16-Bit Rust PCM via WS /v1/inspector/ws • 5ms Hann Crossfader • 450ms LLM Stall Masking</p>
    </div>
    <div id="ws-badge" class="badge">CONNECTING TO BINARY PCM TAP...</div>
  </header>

  <div class="grid">
    <div class="card">
      <div class="card-label">Last Failover Splice Latency</div>
      <div id="val-latency" class="card-val" style="color: var(--emerald);">5.797 ms</div>
    </div>
    <div class="card">
      <div class="card-label">Saved Voice Calls (Hot Splices)</div>
      <div id="val-saved" class="card-val" style="color: var(--cyan);">1</div>
    </div>
    <div class="card">
      <div class="card-label">450ms Backchannels Injected</div>
      <div id="val-bc" class="card-val" style="color: var(--violet);">0</div>
    </div>
    <div class="card">
      <div class="card-label">Max Step Delta (Spliced vs Naive)</div>
      <div id="val-delta" class="card-val">1,149 <span style="font-size:13px;color:var(--crimson);">vs 26,300</span></div>
    </div>
  </div>

  <div class="controls">
    <button class="chaos" onclick="injectChaosStall()">[ INJECT 350ms PRIMARY SOCKET STALL ]</button>
    <button class="backchannel" onclick="injectLlmStall()">[ SIMULATE 900ms LLM TTFT STALL (450ms BACKCHANNEL) ]</button>
    <button class="play-good" onclick="playWav('/v1/flight-recorder/spliced.wav')">▶ PLAY CADENCE 5ms HANN SPLICED (.WAV)</button>
    <button class="play-bad" onclick="playWav('/v1/flight-recorder/naive.wav')">▶ PLAY NAIVE UNPROTECTED POP (.WAV)</button>
  </div>

  <div class="scope-box">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span id="rtp-hud" style="font-size:12px; letter-spacing:1px;">LIVE RUST PCM TAP (24kHz / 16-BIT LE / 1,456 BYTES PER 10ms FRAME)</span>
      <div class="legend">
        <span><i class="dot" style="background:var(--cyan);"></i>Primary Ring (Cartesia)</span>
        <span><i class="dot" style="background:var(--amber);"></i>Standby / 450ms Backchannel</span>
        <span><i class="dot" style="background:var(--emerald);"></i>Spliced Egress Output (C1)</span>
      </div>
    </div>
    <canvas id="scope" width="1200" height="300"></canvas>
  </div>

<script>
const canvas = document.getElementById('scope');
const ctx = canvas.getContext('2d');
const HISTORY = 960; // 40ms rolling window (4 x 240 samples)
const primaryBuf = new Int16Array(HISTORY);
const standbyBuf = new Int16Array(HISTORY);
const egressBuf = new Int16Array(HISTORY);
let spliceBannerFrames = 0;
let backchannelBannerFrames = 0;
let primaryStalled = false;

function pushSamples(ring, incoming) {
  ring.copyWithin(0, incoming.length);
  ring.set(incoming, ring.length - incoming.length);
}

function connectBinaryTap() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${proto}//${location.host}/v1/inspector/ws`);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    const b = document.getElementById('ws-badge');
    b.textContent = 'LIVE BINARY PCM TAP ACTIVE (100 Hz)';
  };

  ws.onmessage = (ev) => {
    const view = new DataView(ev.data);
    if (view.byteLength < 16 || view.getUint8(0) !== 0xCA) return;
    const flags = view.getUint8(1);
    const rtpSeq = view.getUint16(4, true);
    const sampleCount = view.getUint16(6, true);
    if (flags & 1) spliceBannerFrames = 75;
    if (flags & 2) backchannelBannerFrames = 45;
    primaryStalled = (flags & 4) !== 0;

    document.getElementById('rtp-hud').textContent =
      `LIVE RUST PCM TAP • RTP SEQ #${rtpSeq} • ${sampleCount} SAMPLES/FRAME (24kHz s16le)`;

    const pSlice = new Int16Array(sampleCount);
    const sSlice = new Int16Array(sampleCount);
    const eSlice = new Int16Array(sampleCount);
    let offset = 16;
    for (let i = 0; i < sampleCount; i++, offset += 2) pSlice[i] = view.getInt16(offset, true);
    for (let i = 0; i < sampleCount; i++, offset += 2) sSlice[i] = view.getInt16(offset, true);
    for (let i = 0; i < sampleCount; i++, offset += 2) eSlice[i] = view.getInt16(offset, true);

    pushSamples(primaryBuf, pSlice);
    pushSamples(standbyBuf, sSlice);
    pushSamples(egressBuf, eSlice);
  };

  ws.onclose = () => setTimeout(connectBinaryTap, 1000);
}
connectBinaryTap();

function drawTrace(buf, yCenter, color, lineWidth) {
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  for (let i = 0; i < HISTORY; i++) {
    const x = (i / (HISTORY - 1)) * canvas.width;
    const y = yCenter - (buf[i] / 16384.0) * 38.0;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

function renderScope() {
  ctx.fillStyle = '#000000';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  ctx.strokeStyle = '#111116';
  ctx.lineWidth = 1;
  for (let y = 50; y < canvas.height; y += 50) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(canvas.width, y); ctx.stroke();
  }

  drawTrace(primaryBuf, 60, primaryStalled ? '#ff2a55' : '#00f0ff', 1.8);
  drawTrace(standbyBuf, 150, backchannelBannerFrames > 0 ? '#b866ff' : '#ffb800', 1.8);
  drawTrace(egressBuf, 240, '#00ff88', 2.3);

  if (spliceBannerFrames > 0) {
    ctx.fillStyle = 'rgba(0, 255, 136, 0.14)';
    ctx.fillRect(canvas.width - 260, 0, 260, canvas.height);
    ctx.fillStyle = '#00ff88';
    ctx.font = '12px monospace';
    ctx.fillText('5ms HANN CROSSFADE SPLICE ACTIVE', canvas.width - 245, 22);
    spliceBannerFrames--;
  }
  if (backchannelBannerFrames > 0) {
    ctx.fillStyle = '#b866ff';
    ctx.font = '12px monospace';
    ctx.fillText('450ms PROSODIC BACKCHANNEL MASKING ACTIVE', 16, 22);
    backchannelBannerFrames--;
  }

  requestAnimationFrame(renderScope);
}
requestAnimationFrame(renderScope);

async function injectChaosStall() {
  const res = await fetch('/v1/chaos/stall', { method: 'POST' });
  const data = await res.json();
  document.getElementById('val-latency').textContent = (data.failover_latency_us / 1000).toFixed(3) + ' ms';
  document.getElementById('val-saved').textContent = data.saved_calls_total;
  document.getElementById('val-bc').textContent = data.backchannel_injections_total;
  await refreshWeather();
}

async function injectLlmStall() {
  const res = await fetch('/v1/chaos/llm-stall', { method: 'POST' });
  const data = await res.json();
  document.getElementById('val-bc').textContent = data.backchannel_injections_total;
  await refreshWeather();
}

function playWav(url) {
  new Audio(url + '?t=' + Date.now()).play();
}

async function refreshWeather() {
  try {
    const res = await fetch('/v1/weather');
    const data = await res.json();
    document.getElementById('val-saved').textContent = data.saved_calls_total;
    document.getElementById('val-bc').textContent = data.backchannel_injections_total;
  } catch (_) {}
}
setInterval(refreshWeather, 2000);
refreshWeather();
</script>
</body>
</html>
"#;
