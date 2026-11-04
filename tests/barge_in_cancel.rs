//! tests/barge_in_cancel.rs
//! Verifies sub-5ms Barge-In (`CancelUtterance`) handling, 5ms raised-cosine fade-to-silence
//! waveform continuity, and monotonic utterance watermark fencing against late network packets.

use async_trait::async_trait;
use cadence::multiplexer::{AudioStreamMultiplexer, MultiplexerConfig, TtsProviderEndpoint};
use cadence::protocol::{
    AudioChunk, CadenceError, MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use cadence::splicer::StatefulSplicer;
use cadence::telemetry::TelemetryCollector;
use cadence::watchdog::{LatencyWatchdog, WatchdogThresholds};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct BargeInMockEndpoint {
    provider: ProviderId,
    cancelled_id: AtomicU64,
    healthy: AtomicBool,
}

impl BargeInMockEndpoint {
    fn new(provider: ProviderId) -> Arc<Self> {
        Arc::new(Self {
            provider,
            cancelled_id: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        })
    }
}

#[async_trait]
impl TtsProviderEndpoint for BargeInMockEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, _req: &SynthesisRequest) -> Result<(), CadenceError> {
        Ok(())
    }

    async fn send_context_cancel(&self, _context_id: &str) -> Result<(), CadenceError> {
        Ok(())
    }

    async fn send_cancel_utterance(
        &self,
        _session_id: &str,
        utterance_id: u64,
    ) -> Result<(), CadenceError> {
        self.cancelled_id.store(utterance_id, Ordering::Release);
        Ok(())
    }

    async fn send_warm_heartbeat(&self, timestamp_us: u64) -> Result<u64, CadenceError> {
        Ok(timestamp_us)
    }

    async fn send_zero_payload_handshake(&self, _session_id: &str) -> Result<(), CadenceError> {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sub_5ms_barge_in_fade_to_silence_and_late_packet_fencing() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());

    let (ingress_chunk_tx, ingress_chunk_rx) = mpsc::channel(1024);
    let (splicer_chunk_tx, splicer_chunk_rx) = mpsc::channel(1024);
    let (egress_tx, mut egress_rx) = mpsc::channel(1024);

    let (watchdog, failover_rx) = LatencyWatchdog::new(
        WatchdogThresholds::default(),
        clock.clone(),
        Arc::clone(&telemetry),
    );
    let (_tap_handle, _poll_handle) =
        watchdog.spawn_monitor_tasks(ingress_chunk_rx, splicer_chunk_tx);

    let splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0xBA86_E100,
        clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
    );
    let _splicer_handle = splicer.spawn_realtime_loop(splicer_chunk_rx, failover_rx, egress_tx);

    let primary = BargeInMockEndpoint::new(ProviderId::PrimaryCartesia);
    let standby = BargeInMockEndpoint::new(ProviderId::StandbyElevenLabs);

    let mux = AudioStreamMultiplexer::new(
        MultiplexerConfig::default(),
        Arc::clone(&primary) as Arc<dyn TtsProviderEndpoint>,
        Arc::clone(&standby) as Arc<dyn TtsProviderEndpoint>,
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
        clock.clone(),
    );

    // 1. Prime 60ms (6 x 10ms frames) of high-amplitude constant-envelope audio (12,000 i16)
    // for Utterance #77 so we can verify exact Hann window attenuation to zero.
    for seq in 0..6u64 {
        ingress_chunk_tx
            .send(AudioChunk {
                utterance_id: 77,
                sequence: seq,
                provider: ProviderId::PrimaryCartesia,
                sample_rate: SampleRate::Hz24000,
                samples: Arc::from(vec![12_000i16; 240]),
                arrival_us: clock.now_us(),
                is_first_chunk: seq == 0,
                is_final_chunk: false,
                alignment_json: None,
            })
            .await
            .unwrap();
    }

    // Pull first steady-state frame to confirm playback is active at amplitude 12,000
    let steady_frame = tokio::time::timeout(Duration::from_millis(50), egress_rx.recv())
        .await
        .expect("Steady-state frame must arrive")
        .unwrap();
    assert_eq!(steady_frame.pcm_samples[0], 12_000);
    assert!(!steady_frame.barge_in_faded);

    // 2. Trigger user Barge-In (`cancel_utterance`) for Utterance #77
    let t_barge_start_us = clock.now_us();
    mux.cancel_utterance("livekit-barge-session", 77)
        .await
        .expect("Barge-in cancel must succeed");

    // 3. Receive the immediate 5ms Hann fade-to-silence frame
    let fade_frame = tokio::time::timeout(Duration::from_millis(10), egress_rx.recv())
        .await
        .expect("Fade-to-silence frame must be emitted immediately")
        .unwrap();
    let t_barge_done_us = clock.now_us();
    let barge_in_elapsed_us = t_barge_done_us.saturating_sub(t_barge_start_us);

    println!(
        "Barge-In Execution Latency: {} us ({:.3} ms)",
        barge_in_elapsed_us,
        barge_in_elapsed_us as f64 / 1000.0
    );

    // ASSERTION A: Sub-5ms (5,000 us) barge-in execution SLA
    assert!(
        barge_in_elapsed_us < 5_000,
        "Barge-in took {}us, exceeding the 5,000us (5ms) SLA",
        barge_in_elapsed_us
    );
    assert!(fade_frame.barge_in_faded);

    // ASSERTION B: First 5ms (samples 0..120) smoothly attenuate from 12,000 down to 0
    assert_eq!(fade_frame.pcm_samples[0], 12_000);
    assert!(
        fade_frame.pcm_samples[60] > 5_500 && fade_frame.pcm_samples[60] < 6_500,
        "Midpoint of 5ms Hann fade should be ~6,000, got {}",
        fade_frame.pcm_samples[60]
    );
    assert_eq!(fade_frame.pcm_samples[119], 0);

    // ASSERTION C: Remaining 5ms (samples 120..240) are exact digital silence (0i16)
    assert!(
        fade_frame.pcm_samples[120..240].iter().all(|&s| s == 0),
        "Second half of barge-in frame must be exact digital silence"
    );

    // ASSERTION D: Inject a late-arriving network packet for cancelled Utterance #77
    // and verify the monotonic watermark fence drops it completely.
    ingress_chunk_tx
        .send(AudioChunk {
            utterance_id: 77,
            sequence: 99,
            provider: ProviderId::PrimaryCartesia,
            sample_rate: SampleRate::Hz24000,
            samples: Arc::from(vec![15_000i16; 240]),
            arrival_us: clock.now_us(),
            is_first_chunk: false,
            is_final_chunk: true,
            alignment_json: None,
        })
        .await
        .unwrap();

    let res = tokio::time::timeout(Duration::from_millis(25), egress_rx.recv()).await;
    assert!(
        res.is_err(),
        "Late chunks from cancelled utterance #77 must be fenced and never emitted"
    );

    assert_eq!(telemetry.barge_in_count(), 1);
    mux.shutdown().await.unwrap();
}
