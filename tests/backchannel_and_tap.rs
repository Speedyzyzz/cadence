//! tests/backchannel_and_tap.rs
//! Hermetic Verification Suite for Upgrade #1 (Live Binary WebSocket PCM Tap)
//! and Upgrade #2 (450ms Acoustic Backchannel Injection + 5ms Hann Crossfade into Live TTS).

use cadence::inspector_tap::{InspectorTapHub, InspectorTapPacket};
use cadence::protocol::{AudioChunk, MonotonicClock, ProviderId, SampleRate};
use cadence::splicer::StatefulSplicer;
use cadence::telemetry::TelemetryCollector;
use cadence::watchdog::{LatencyWatchdog, WatchdogThresholds};
use std::f64::consts::PI;
use std::sync::Arc;

#[test]
fn test_450ms_backchannel_injection_and_5ms_hann_crossfade_to_live_tts_with_binary_tap() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());
    let tap_hub = Arc::new(InspectorTapHub::new());
    let mut tap_rx = tap_hub.subscribe();

    let (watchdog, _failover_rx) = LatencyWatchdog::new(
        WatchdogThresholds::default(),
        clock.clone(),
        Arc::clone(&telemetry),
    );

    let mut splicer = StatefulSplicer::new(
        SampleRate::Hz24000,
        0xBC00_0001,
        clock.clone(),
        Arc::clone(&watchdog),
        Arc::clone(&telemetry),
    )
    .with_inspector_tap(Arc::clone(&tap_hub));

    // Phase 1: Turn starts at t = 100,000us. Arm 450ms (450,000us) LLM TTFT backchannel timer.
    splicer.arm_llm_turn_wait(1, 100_000);

    // At t = 540,000us (440ms elapsed), threshold (450ms) is NOT yet reached -> returns false
    assert!(!splicer.evaluate_backchannel_at(540_000));
    assert!(!splicer.is_backchannel_active());

    // At t = 550,000us (450ms elapsed), threshold IS breached -> triggers prosodic backchannel!
    assert!(splicer.evaluate_backchannel_at(550_000));
    assert!(splicer.is_backchannel_active());
    assert_eq!(telemetry.backchannel_injections_count(), 1);

    // Phase 2: Pull 20 frames (200ms of backchannel audio while LLM is still thinking)
    let mut emitted_frames = Vec::new();
    for _ in 0..20 {
        let frame = splicer
            .pull_10ms_egress_frame(None)
            .expect("Backchannel must emit contiguous 10ms frames during LLM wait");
        assert_eq!(frame.active_provider, ProviderId::SyntheticComfortNoise);
        assert_eq!(frame.pcm_samples.len(), 240);
        emitted_frames.push(frame);
    }

    // Verify the Binary Inspector Tap emitted exact 1,456-byte packets matching the egress frames
    let raw_tap_bytes = tap_rx
        .try_recv()
        .expect("InspectorTapHub must broadcast binary PCM packets");
    assert_eq!(
        raw_tap_bytes.len(),
        16 + (240 * 2 * 3),
        "Binary tap packet at 24kHz must be exactly 1,456 bytes"
    );
    let decoded_tap = InspectorTapPacket::decode_binary(raw_tap_bytes)
        .expect("Binary tap packet must decode cleanly");
    assert!(decoded_tap.backchannel_active);
    assert_eq!(decoded_tap.egress_pcm, emitted_frames[0].pcm_samples);

    // Phase 3: At t = 750ms, LLM emits first tokens and Primary Cartesia delivers 40ms of speech.
    // Splicer must automatically arm a 5ms Hann crossfade from the backchannel into Primary TTS!
    for seq in 0..4u64 {
        let mut pcm = Vec::with_capacity(240);
        for n in 0..240usize {
            let idx = (seq as usize * 240 + n) as f64;
            let val = (8_500.0 * (2.0 * PI * 220.0 * (idx / 24_000.0)).sin()).round() as i16;
            pcm.push(val);
        }
        splicer.ingest_chunk(AudioChunk {
            utterance_id: 1,
            sequence: seq,
            provider: ProviderId::PrimaryCartesia,
            sample_rate: SampleRate::Hz24000,
            samples: Arc::from(pcm),
            arrival_us: 750_000 + (seq * 10_000),
            is_first_chunk: seq == 0,
            is_final_chunk: false,
            alignment_json: None,
        });
    }

    // Pull the transition frame: must execute the 5ms Hann crossfade and switch to PrimaryCartesia!
    let transition_frame = splicer
        .pull_10ms_egress_frame(None)
        .expect("Transition frame must emit");
    assert!(
        transition_frame.spliced_in_this_frame,
        "Transition from backchannel to live TTS must execute 5ms Hann crossfade"
    );
    assert_eq!(
        transition_frame.active_provider,
        ProviderId::PrimaryCartesia
    );
    assert!(
        !splicer.is_backchannel_active(),
        "Backchannel must deactivate after crossfading into live TTS"
    );
    emitted_frames.push(transition_frame);

    // Pull remaining Primary TTS frames and verify strict RTP/PTS contiguity and zero clicks
    for _ in 0..3 {
        emitted_frames.push(splicer.pull_10ms_egress_frame(None).unwrap());
    }

    for window in emitted_frames.windows(2) {
        let prev = &window[0];
        let curr = &window[1];
        assert_eq!(curr.rtp_sequence, prev.rtp_sequence.wrapping_add(1));
        assert_eq!(curr.pts_us - prev.pts_us, 10_000);

        let boundary_step =
            (curr.pcm_samples[0] as i32 - *prev.pcm_samples.last().unwrap() as i32).abs();
        assert!(
            boundary_step < 6_500,
            "Acoustic click detected at backchannel->TTS splice boundary: step={boundary_step}"
        );
    }
}
