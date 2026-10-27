//! tests/chaos_failover.rs
//! Hermetic Chaos Test Suite for Cadence.
//!
//! Test 1: `test_chaos_350ms_primary_stall_zero_drop_sub_18ms_failover`
//!   Injects a 350ms network stall on the Primary (Cartesia) socket mid-sentence and asserts:
//!   (a) Failover & 5ms crossfade splicing complete in under 18ms (18,000us).
//!   (b) Zero audio frames are dropped.
//!   (c) Egress audio remains contiguous, phase-aligned, and free of acoustic clicks.
//!
//! Test 2: `test_temporal_playhead_sync_and_predictive_jitter_underrun`
//!   Verifies that `standby_playhead_debt` keeps the shadow standby stream temporally
//!   aligned with primary playback (preventing repeated syllables on splice), verifies
//!   predictive jitter underrun failover when B(t_now) <= 15ms, and verifies RFC 4648 Base64.
//!
//! Test 3: `test_wav_artifact_generation`
//!   Synthesizes and writes two comparison WAV files to `tests/artifacts/`:
//!   1. `naive_concatenation_with_pop.wav` (with an abrupt acoustic step discontinuity)
//!   2. `cadence_5ms_hann_spliced.wav` (with Cadence's 5ms phase-aligned Hann crossfade)

use async_trait::async_trait;
use cadence::multiplexer::{
    decode_base64_pcm_s16le, encode_base64_pcm_s16le, AudioStreamMultiplexer, MultiplexerConfig,
    ShadowingStrategy, TtsProviderEndpoint,
};
use cadence::protocol::{
    AudioChunk, CadenceError, FailoverReason, FailoverSignal, MonotonicClock, ProviderId,
    SampleRate, SynthesisRequest,
};
use cadence::splicer::{CrossfadeDspEngine, StatefulSplicer};
use cadence::telemetry::TelemetryCollector;
use cadence::watchdog::{LatencyWatchdog, WatchdogThresholds};
use cadence::wav::write_wav_file;
use std::f64::consts::PI;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Deterministic mock TTS endpoint that synthesizes continuous phase-coherent 440Hz harmonic
/// audio frames and supports injecting a precise 350ms network stall on command.
struct ChaosMockTtsEndpoint {
    provider: ProviderId,
    sample_rate: SampleRate,
    clock: MonotonicClock,
    chunk_tx: mpsc::Sender<AudioChunk>,
    inject_stall_ms: AtomicU64,
    healthy: AtomicBool,
}

impl ChaosMockTtsEndpoint {
    fn new(
        provider: ProviderId,
        sample_rate: SampleRate,
        clock: MonotonicClock,
        chunk_tx: mpsc::Sender<AudioChunk>,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider,
            sample_rate,
            clock,
            chunk_tx,
            inject_stall_ms: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        })
    }

    fn arm_network_stall(&self, stall_ms: u64) {
        self.inject_stall_ms.store(stall_ms, Ordering::Release);
    }
}

#[async_trait]
impl TtsProviderEndpoint for ChaosMockTtsEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        let tx = self.chunk_tx.clone();
        let clock = self.clock.clone();
        let rate = self.sample_rate;
        let provider = self.provider;
        let utt_id = req.utterance_id;
        let stall_ms = self.inject_stall_ms.load(Ordering::Acquire);

        tokio::spawn(async move {
            let samples_per_chunk = rate.samples_for_ms(10); // 240 samples @ 24kHz
            let sr = rate.as_u32() as f64;
            let fundamental_hz = 440.0;
            let amplitude = 10_000.0;

            let total_chunks = if utt_id == 101 { 8u64 } else { 50u64 };

            for seq in 0..total_chunks {
                if provider == ProviderId::PrimaryCartesia && stall_ms > 0 && seq == 4 {
                    tokio::time::sleep(Duration::from_millis(stall_ms)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                let mut pcm = Vec::with_capacity(samples_per_chunk);
                for n in 0..samples_per_chunk {
                    let sample_index = (seq as usize * samples_per_chunk + n) as f64;
                    let phase = 2.0 * PI * fundamental_hz * (sample_index / sr);
                    let sample_val = (amplitude * phase.sin()).round() as i16;
                    pcm.push(sample_val);
                }

                let chunk = AudioChunk {
                    utterance_id: utt_id,
                    sequence: seq,
                    provider,
                    sample_rate: rate,
                    samples: Arc::from(pcm),
                    arrival_us: clock.now_us(),
                    is_first_chunk: seq == 0,
                    is_final_chunk: seq + 1 == total_chunks,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_chaos_350ms_primary_stall_zero_drop_sub_18ms_failover() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());

    let (ingress_chunk_tx, ingress_chunk_rx) = mpsc::channel(4096);
    let (splicer_chunk_tx, splicer_chunk_rx) = mpsc::channel(4096);
    let (egress_tx, mut egress_rx) = mpsc::channel(4096);

    let thresholds = WatchdogThresholds {
        max_ttfb_us: 120_000,
        max_inter_chunk_spike_us: 150_000,
        min_splice_reserve_us: 15_000,
        poll_interval_us: 1_000,
    };

    let (watchdog, failover_rx) =
        LatencyWatchdog::new(thresholds, clock.clone(), Arc::clone(&telemetry));
    let mut watchdog_observer_rx = watchdog.subscribe();

    let (_tap_handle, _poll_handle) =
        watchdog.spawn_monitor_tasks(ingress_chunk_rx, splicer_chunk_tx);

    let splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0xCAFE_BABE,
        clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
    );
    let _splicer_handle = splicer.spawn_realtime_loop(splicer_chunk_rx, failover_rx, egress_tx);

    let primary = ChaosMockTtsEndpoint::new(
        ProviderId::PrimaryCartesia,
        SampleRate::Hz24000,
        clock.clone(),
        ingress_chunk_tx.clone(),
    );
    let standby = ChaosMockTtsEndpoint::new(
        ProviderId::StandbyElevenLabs,
        SampleRate::Hz24000,
        clock.clone(),
        ingress_chunk_tx,
    );

    let mux = AudioStreamMultiplexer::new(
        MultiplexerConfig {
            heartbeat_interval: Duration::from_secs(2),
            zero_payload_handshake_interval: Duration::from_secs(5),
            shadowing_strategy: ShadowingStrategy::Full,
        },
        Arc::clone(&primary) as Arc<dyn TtsProviderEndpoint>,
        Arc::clone(&standby) as Arc<dyn TtsProviderEndpoint>,
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
        clock.clone(),
    );

    // Phase 1: Dispatch Utterance 1 (Healthy baseline warm-up)
    mux.dispatch_utterance(SynthesisRequest {
        session_id: "chaos-session-1".to_string(),
        utterance_id: 101,
        text_segment: "Establishing steady-state voice stream on primary Cartesia provider."
            .to_string(),
        voice_id_primary: "cartesia-sonic".to_string(),
        voice_id_standby: "elevenlabs-flash".to_string(),
        dispatch_us: clock.now_us(),
        flush: false,
    })
    .await
    .expect("Utterance 1 dispatch must succeed");

    tokio::time::sleep(Duration::from_millis(120)).await;

    // Drain warm-up baseline frames so the verification window focuses on the stall & failover
    while egress_rx.try_recv().is_ok() {}

    // Phase 2: Arm a 350ms network stall on Primary and dispatch Utterance 2
    primary.arm_network_stall(350);

    mux.dispatch_utterance(SynthesisRequest {
        session_id: "chaos-session-1".to_string(),
        utterance_id: 102,
        text_segment: "Injecting 350ms network stall mid-sentence to verify hot splice."
            .to_string(),
        voice_id_primary: "cartesia-sonic".to_string(),
        voice_id_standby: "elevenlabs-flash".to_string(),
        dispatch_us: clock.now_us(),
        flush: true,
    })
    .await
    .expect("Utterance 2 dispatch must succeed");

    let failover_signal =
        tokio::time::timeout(Duration::from_millis(600), watchdog_observer_rx.recv())
            .await
            .expect("Watchdog must trigger failover during 350ms primary stall")
            .expect("Broadcast channel must not close");

    let trigger_us = match failover_signal {
        FailoverSignal::SwitchToStandby { trigger_us, .. } => trigger_us,
        other => panic!("Expected SwitchToStandby signal, got {other:?}"),
    };

    let mut collected_frames = Vec::with_capacity(35);
    let mut splice_completed_wall_us: Option<u64> = None;

    for _ in 0..35 {
        let frame = tokio::time::timeout(Duration::from_millis(100), egress_rx.recv())
            .await
            .expect("Egress stream must never stall")
            .expect("Egress channel must remain open");

        if frame.spliced_in_this_frame && splice_completed_wall_us.is_none() {
            splice_completed_wall_us = Some(clock.now_us());
        }
        collected_frames.push(frame);
    }

    let splice_done_us =
        splice_completed_wall_us.expect("Splicer must emit a crossfaded splice frame");
    let failover_execution_us = splice_done_us.saturating_sub(trigger_us);
    let failover_execution_ms = failover_execution_us as f64 / 1000.0;

    println!("=== CADENCE HERMETIC CHAOS TEST REPORT ===");
    println!("Watchdog Trigger Timestamp : {} us", trigger_us);
    println!("Splice Completed Timestamp : {} us", splice_done_us);
    println!(
        "Failover Execution Latency : {:.3} ms ({} us)",
        failover_execution_ms, failover_execution_us
    );
    println!("Total Frames Verified      : {}", collected_frames.len());
    println!(
        "Prometheus Metrics Snapshot:\n{}",
        telemetry.render_prometheus_metrics()
    );

    for (i, frame) in collected_frames.iter().enumerate() {
        println!(
            "Frame idx={:02} seq={} spliced={} provider={:?} first_val={} last_val={}",
            i, frame.rtp_sequence, frame.spliced_in_this_frame, frame.active_provider, frame.pcm_samples[0], frame.pcm_samples[239]
        );
    }

    // ASSERTION A: Failover occurs in under 18 milliseconds (18,000 microseconds)
    assert!(
        failover_execution_us < 18_000,
        "Failover took {}us ({:.2}ms), exceeding the 18,000us (18ms) SLA",
        failover_execution_us,
        failover_execution_ms
    );

    // ASSERTION B: Zero audio frames are dropped
    assert_eq!(
        telemetry.dropped_frames_count(),
        0,
        "Expected zero dropped audio frames"
    );
    assert_eq!(
        telemetry.saved_calls_count(),
        1,
        "Expected saved_calls_total counter to equal 1"
    );

    // ASSERTION C: Output audio stream is strictly contiguous, playable, and click-free
    for window in collected_frames.windows(2) {
        let prev = &window[0];
        let curr = &window[1];

        assert_eq!(
            curr.rtp_sequence,
            prev.rtp_sequence.wrapping_add(1),
            "Discontiguous RTP sequence numbers detected: {} -> {}",
            prev.rtp_sequence,
            curr.rtp_sequence
        );

        assert_eq!(
            curr.pts_us - prev.pts_us,
            10_000,
            "Discontiguous PTS timestamps detected: {} -> {}",
            prev.pts_us,
            curr.pts_us
        );

        assert_eq!(
            curr.rtp_timestamp.wrapping_sub(prev.rtp_timestamp),
            480,
            "Discontiguous RTP clock advance"
        );

        assert_eq!(curr.pcm_samples.len(), 240);

        let boundary_step =
            (curr.pcm_samples[0] as i32 - *prev.pcm_samples.last().unwrap() as i32).abs();
        assert!(
            boundary_step < 6_500,
            "Acoustic click detected at frame boundary: step delta = {}",
            boundary_step
        );

        for sample_pair in curr.pcm_samples.windows(2) {
            let step = (sample_pair[1] as i32 - sample_pair[0] as i32).abs();
            assert!(
                step < 6_500,
                "Intra-frame acoustic discontinuity detected: step delta = {}",
                step
            );
        }
    }

    mux.shutdown().await.expect("Clean shutdown");
}

#[test]
fn test_temporal_playhead_sync_and_predictive_jitter_underrun() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());
    let (watchdog, _failover_rx) = LatencyWatchdog::new(
        WatchdogThresholds::default(),
        clock.clone(),
        Arc::clone(&telemetry),
    );

    let mut splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0x1234_5678,
        clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
    );

    // 1. Verify RFC 4648 Base64 PCM s16le round-trip fidelity
    let test_pcm: Vec<i16> = (-120..120).map(|x| x * 100).collect();
    let encoded_b64 = encode_base64_pcm_s16le(&test_pcm);
    let decoded_pcm = decode_base64_pcm_s16le(&encoded_b64);
    assert_eq!(test_pcm, decoded_pcm, "Base64 PCM round-trip must be exact");

    // 2. Verify Temporal Playhead Synchronization (`standby_playhead_debt`)
    for seq in 0..6u64 {
        let val = ((seq + 1) * 1000) as i16;
        let samples: Arc<[i16]> = Arc::from(vec![val; 240]);

        splicer.ingest_chunk(AudioChunk {
            utterance_id: 1,
            sequence: seq,
            provider: ProviderId::PrimaryCartesia,
            sample_rate: SampleRate::Hz24000,
            samples: Arc::clone(&samples),
            arrival_us: (seq + 1) * 10_000,
            is_first_chunk: seq == 0,
            is_final_chunk: false,
            alignment_json: None,
        });

        splicer.ingest_chunk(AudioChunk {
            utterance_id: 1,
            sequence: seq,
            provider: ProviderId::StandbyElevenLabs,
            sample_rate: SampleRate::Hz24000,
            samples,
            arrival_us: (seq + 1) * 10_000,
            is_first_chunk: seq == 0,
            is_final_chunk: false,
            alignment_json: None,
        });
    }

    // Pull 3 normal frames (seq 0, 1, 2 -> values 1000, 2000, 3000) from Primary
    let f0 = splicer.pull_10ms_egress_frame(None).unwrap();
    let f1 = splicer.pull_10ms_egress_frame(None).unwrap();
    let f2 = splicer.pull_10ms_egress_frame(None).unwrap();
    assert_eq!(f0.pcm_samples[0], 1000);
    assert_eq!(f1.pcm_samples[0], 2000);
    assert_eq!(f2.pcm_samples[0], 3000);

    // Trigger failover before Frame 3 (which has value 4000 in both Primary and Standby)
    splicer.handle_failover_signal(FailoverSignal::SwitchToStandby {
        utterance_id: 1,
        trigger_us: 35_000,
        reason: FailoverReason::InterChunkSpikeExceeded150ms,
    });

    let f3_spliced = splicer.pull_10ms_egress_frame(Some(35_000)).unwrap();
    assert!(f3_spliced.spliced_in_this_frame);
    assert_eq!(
        f3_spliced.pcm_samples[239], 4000,
        "Standby playhead must be synchronized to frame 3 (4000), not frame 0 (1000)"
    );

    // 3. Verify Predictive Jitter Underrun Trigger when B(t_now) <= 15ms
    watchdog.on_utterance_dispatched(2, 100_000);
    watchdog.observe_chunk(&AudioChunk {
        utterance_id: 2,
        sequence: 0,
        provider: ProviderId::PrimaryCartesia,
        sample_rate: SampleRate::Hz24000,
        samples: Arc::from(vec![500i16; 240]),
        arrival_us: 120_000,
        is_first_chunk: true,
        is_final_chunk: false,
        alignment_json: None,
    });
    watchdog.observe_chunk(&AudioChunk {
        utterance_id: 2,
        sequence: 1,
        provider: ProviderId::PrimaryCartesia,
        sample_rate: SampleRate::Hz24000,
        samples: Arc::from(vec![500i16; 240]),
        arrival_us: 210_000,
        is_first_chunk: false,
        is_final_chunk: false,
        alignment_json: None,
    });

    watchdog.set_primary_buffer_occupancy_us(10_000);

    let signal = watchdog
        .evaluate_at(352_000)
        .expect("Predictive jitter underrun must trigger before 150ms hard stall");
    assert_eq!(
        signal,
        FailoverSignal::SwitchToStandby {
            utterance_id: 2,
            trigger_us: 352_000,
            reason: FailoverReason::PredictiveJitterUnderrunRisk,
        }
    );
}

#[test]
fn test_wav_artifact_generation() {
    let artifacts_dir = Path::new("tests/artifacts");
    std::fs::create_dir_all(artifacts_dir).expect("Failed to create tests/artifacts dir");

    let sample_rate = 24000u32;
    let sr_f = sample_rate as f64;
    let total_samples = (sample_rate as f64 * 1.5) as usize; // 1.5 seconds of audio

    // 1. Generate naive concatenation audio with an abrupt phase discontinuity pop at t = 0.75s
    let mut naive_samples = Vec::with_capacity(total_samples);
    let splice_sample = total_samples / 2;

    for i in 0..total_samples {
        if i < splice_sample {
            // 440 Hz primary wave ending high
            let phase = 2.0 * PI * 440.0 * (i as f64 / sr_f);
            naive_samples.push((12000.0 * phase.sin()).round() as i16);
        } else if i == splice_sample {
            // Abrupt step pop discontinuity (+12000 to -12000)
            naive_samples.push(-12000);
        } else {
            // Standby stream starting 180 degrees out of phase
            let phase = 2.0 * PI * 440.0 * ((i - splice_sample) as f64 / sr_f) + PI;
            naive_samples.push((12000.0 * phase.sin()).round() as i16);
        }
    }

    let naive_path = artifacts_dir.join("naive_concatenation_with_pop.wav");
    write_wav_file(&naive_path, &naive_samples, sample_rate)
        .expect("Failed to write naive_concatenation_with_pop.wav");

    // 2. Generate Cadence spliced audio using the 5ms phase-aligned Hann crossfader
    let dsp = CrossfadeDspEngine::new(SampleRate::Hz24000);
    let fade_len = dsp.fade_samples(); // 120 samples (5ms)

    let mut cadence_samples = Vec::with_capacity(total_samples);
    // Write pre-splice primary samples
    for i in 0..(splice_sample - fade_len / 2) {
        let phase = 2.0 * PI * 440.0 * (i as f64 / sr_f);
        cadence_samples.push((12000.0 * phase.sin()).round() as i16);
    }

    // Primary tail & standby head for crossfade
    let primary_tail: Vec<i16> = (0..fade_len)
        .map(|n| {
            let idx = (splice_sample - fade_len / 2 + n) as f64;
            (12000.0 * (2.0 * PI * 440.0 * idx / sr_f).sin()).round() as i16
        })
        .collect();

    let standby_head_raw: Vec<i16> = (0..(fade_len + 48))
        .map(|n| {
            let phase = 2.0 * PI * 440.0 * (n as f64 / sr_f);
            (12000.0 * phase.sin()).round() as i16
        })
        .collect();

    let optimal_lag = dsp.find_optimal_phase_offset(&primary_tail, &standby_head_raw);
    let standby_aligned = &standby_head_raw[optimal_lag..optimal_lag + fade_len];
    let crossfaded = dsp.crossfade_5ms(&primary_tail, standby_aligned);
    cadence_samples.extend_from_slice(&crossfaded);

    // Remaining post-splice standby stream
    let remaining = total_samples.saturating_sub(cadence_samples.len());
    for n in 0..remaining {
        let idx = (optimal_lag + fade_len + n) as f64;
        let phase = 2.0 * PI * 440.0 * (idx / sr_f);
        cadence_samples.push((12000.0 * phase.sin()).round() as i16);
    }

    let cadence_path = artifacts_dir.join("cadence_5ms_hann_spliced.wav");
    write_wav_file(&cadence_path, &cadence_samples, sample_rate)
        .expect("Failed to write cadence_5ms_hann_spliced.wav");

    assert!(naive_path.exists());
    assert!(cadence_path.exists());
    assert!(std::fs::metadata(&naive_path).unwrap().len() > 10_000);
    assert!(std::fs::metadata(&cadence_path).unwrap().len() > 10_000);

    println!("Generated WAV artifacts:");
    println!("  - {:?}", naive_path);
    println!("  - {:?}", cadence_path);
}
