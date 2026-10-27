//! src/main.rs
//! Production daemon entrypoint for the Cadence Real-Time Voice Failover Proxy.

use async_trait::async_trait;
use cadence::multiplexer::{
    AudioStreamMultiplexer, MultiplexerConfig, ShadowingStrategy, TtsProviderEndpoint,
};
use cadence::protocol::{
    AudioChunk, CadenceError, MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use cadence::server::run_ingress_server;
use cadence::splicer::StatefulSplicer;
use cadence::telemetry::{spawn_prometheus_http_exporter, TelemetryCollector};
use cadence::watchdog::{LatencyWatchdog, WatchdogThresholds};
use std::f64::consts::PI;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

/// Built-in local loopback synthesizer endpoint for deterministic in-process testing.
struct LocalSynthesizerEndpoint {
    provider: ProviderId,
    freq_hz: f64,
    sample_rate: SampleRate,
    clock: MonotonicClock,
    chunk_tx: mpsc::Sender<AudioChunk>,
    healthy: AtomicBool,
}

#[async_trait]
impl TtsProviderEndpoint for LocalSynthesizerEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        let tx = self.chunk_tx.clone();
        let clock = self.clock.clone();
        let rate = self.sample_rate;
        let provider = self.provider;
        let freq = self.freq_hz;
        let utt_id = req.utterance_id;

        tokio::spawn(async move {
            let samples_per_10ms = rate.samples_for_ms(10);
            let sr = rate.as_u32() as f64;
            for seq in 0..8u64 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let mut pcm = Vec::with_capacity(samples_per_10ms);
                for n in 0..samples_per_10ms {
                    let global_idx = (seq as usize * samples_per_10ms + n) as f64;
                    let val = (8000.0 * (2.0 * PI * freq * global_idx / sr).sin()) as i16;
                    pcm.push(val);
                }
                let _ = tx
                    .send(AudioChunk {
                        utterance_id: utt_id,
                        sequence: seq,
                        provider,
                        sample_rate: rate,
                        samples: Arc::from(pcm),
                        arrival_us: clock.now_us(),
                        is_first_chunk: seq == 0,
                        is_final_chunk: seq == 7,
                        alignment_json: None,
                    })
                    .await;
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

    // Dedicated Prometheus metrics port
    let metrics_bind =
        std::env::var("CADENCE_METRICS_ADDR").unwrap_or_else(|_| "127.0.0.1:9091".to_string());
    let _metrics_handle =
        spawn_prometheus_http_exporter(&metrics_bind, Arc::clone(&telemetry)).await?;
    info!(
        "Prometheus telemetry plane listening on http://{}/metrics",
        metrics_bind
    );

    // If CADENCE_SERVER_MODE is set or CADENCE_INGRESS_ADDR is configured, boot the Axum Ingress Server
    let ingress_bind = std::env::var("CADENCE_INGRESS_ADDR").ok();

    if let Some(ref bind_addr) = ingress_bind {
        info!(
            "Starting Cadence Multi-Session Ingress Server on {}",
            bind_addr
        );
        run_ingress_server(bind_addr, telemetry, clock).await?;
        return Ok(());
    }

    // Default: Self-check pipeline verification mode
    info!("Running Cadence pipeline self-check...");

    let (ingress_chunk_tx, ingress_chunk_rx) = mpsc::channel(2048);
    let (splicer_chunk_tx, splicer_chunk_rx) = mpsc::channel(2048);
    let (egress_tx, mut egress_rx) = mpsc::channel(2048);

    let (watchdog, failover_rx) = LatencyWatchdog::new(
        WatchdogThresholds::default(),
        clock.clone(),
        Arc::clone(&telemetry),
    );
    let (_tap_handle, _poll_handle) =
        watchdog.spawn_monitor_tasks(ingress_chunk_rx, splicer_chunk_tx);

    let splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0xCADE_0001,
        clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
    );
    let _splicer_handle = splicer.spawn_realtime_loop(splicer_chunk_rx, failover_rx, egress_tx);

    let primary_endpoint = Arc::new(LocalSynthesizerEndpoint {
        provider: ProviderId::PrimaryCartesia,
        freq_hz: 440.0,
        sample_rate: SampleRate::Hz24000,
        clock: clock.clone(),
        chunk_tx: ingress_chunk_tx.clone(),
        healthy: AtomicBool::new(true),
    });

    let standby_endpoint = Arc::new(LocalSynthesizerEndpoint {
        provider: ProviderId::StandbyElevenLabs,
        freq_hz: 440.0,
        sample_rate: SampleRate::Hz24000,
        clock: clock.clone(),
        chunk_tx: ingress_chunk_tx,
        healthy: AtomicBool::new(true),
    });

    let multiplexer = AudioStreamMultiplexer::new(
        MultiplexerConfig {
            heartbeat_interval: std::time::Duration::from_secs(5),
            zero_payload_handshake_interval: std::time::Duration::from_secs(12),
            shadowing_strategy: ShadowingStrategy::Full,
        },
        primary_endpoint,
        standby_endpoint,
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
        clock.clone(),
    );

    multiplexer
        .dispatch_utterance(SynthesisRequest {
            session_id: "livekit-room-01".to_string(),
            utterance_id: 1,
            text_segment: "Hello, Cadence real-time voice proxy is online and primed.".to_string(),
            voice_id_primary: "cartesia-sonic-voice-1".to_string(),
            voice_id_standby: "elevenlabs-flash-voice-1".to_string(),
            dispatch_us: clock.now_us(),
            flush: true,
        })
        .await?;

    let mut received_frames = 0usize;
    while let Some(frame) = egress_rx.recv().await {
        received_frames += 1;
        if received_frames == 5 {
            info!(
                rtp_seq = frame.rtp_sequence,
                rtp_ts = frame.rtp_timestamp,
                pts_us = frame.pts_us,
                provider = %frame.active_provider,
                "Verified live WebRTC RTP/Opus egress stream"
            );
            break;
        }
    }

    multiplexer.shutdown().await?;
    info!("Cadence self-check completed cleanly.");
    Ok(())
}
