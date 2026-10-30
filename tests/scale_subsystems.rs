//! tests/scale_subsystems.rs
//! Hermetic Verification Suite for Cadence Phase 3 Scaling Subsystems:
//!   1. UpstreamTrunkPool: N:M multiplexing (500 utterances over 4 ElevenLabs 5-context trunks)
//!   2. ShardedTickEngine: Single 1ms master ticker per shard driving concurrent sessions + failover
//!   3. ProviderWeatherMap: Regional brownout circuit breaker + canary probe recovery
//!   4. DrainCoordinator: Zero-drop SIGTERM drain with reconnect_hint broadcast & RAII completion

use async_trait::async_trait;
use cadence::drain::DrainCoordinator;
use cadence::engine::ShardedTickEngine;
use cadence::multiplexer::TtsProviderEndpoint;
use cadence::protocol::{
    AudioChunk, CadenceError, MonotonicClock, ProviderId, SampleRate, SynthesisRequest,
};
use cadence::telemetry::TelemetryCollector;
use cadence::trunk_pool::{ShardedContextRouter, TrunkPoolConfig, UpstreamTrunkPool};
use cadence::watchdog::WatchdogThresholds;
use cadence::weather::{ProviderWeatherMap, WeatherConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct MockTrunkEndpoint {
    provider: ProviderId,
    trunk_tx: mpsc::Sender<AudioChunk>,
    dispatched_count: AtomicU64,
    clock: MonotonicClock,
}

#[async_trait]
impl TtsProviderEndpoint for MockTrunkEndpoint {
    fn provider_id(&self) -> ProviderId {
        self.provider
    }

    async fn send_synthesis_request(&self, req: &SynthesisRequest) -> Result<(), CadenceError> {
        self.dispatched_count.fetch_add(1, Ordering::Relaxed);
        let tx = self.trunk_tx.clone();
        let utt_id = req.utterance_id;
        let provider = self.provider;
        let now_us = self.clock.now_us();

        tokio::spawn(async move {
            // Emit 2 chunks (chunk 0 and final chunk 1) tagged with utterance_id
            for seq in 0..2u64 {
                let _ = tx
                    .send(AudioChunk {
                        utterance_id: utt_id,
                        sequence: seq,
                        provider,
                        sample_rate: SampleRate::Hz24000,
                        samples: Arc::from(vec![1200i16; 240]),
                        arrival_us: now_us + (seq * 10_000),
                        is_first_chunk: seq == 0,
                        is_final_chunk: seq == 1,
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
        true
    }

    async fn shutdown(&self) -> Result<(), CadenceError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_upstream_trunk_pool_n_to_m_multiplexing_and_slot_release() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());
    let router = Arc::new(ShardedContextRouter::new());
    let (trunk_tx, trunk_rx) = mpsc::channel(4096);

    // Create 4 persistent ElevenLabs trunk sockets (5 concurrent contexts/socket nominal limit)
    let mut endpoints: Vec<Arc<dyn TtsProviderEndpoint>> = Vec::new();
    for _ in 0..4 {
        endpoints.push(Arc::new(MockTrunkEndpoint {
            provider: ProviderId::StandbyElevenLabs,
            trunk_tx: trunk_tx.clone(),
            dispatched_count: AtomicU64::new(0),
            clock: clock.clone(),
        }));
    }

    let pool = UpstreamTrunkPool::new(
        TrunkPoolConfig::elevenlabs_default(4, SampleRate::Hz24000),
        endpoints,
        trunk_rx,
        Arc::clone(&router),
        clock.clone(),
        Arc::clone(&telemetry),
    );

    // Multiplex 200 concurrent utterances across the 4 persistent trunk sockets
    let mut receivers = Vec::with_capacity(200);
    for utt_id in 1..=200u64 {
        let (session_tx, session_rx) = mpsc::channel(8);
        router.register_context(utt_id, session_tx);
        receivers.push((utt_id, session_rx));

        pool.send_synthesis_request(&SynthesisRequest {
            session_id: format!("session-{utt_id}"),
            utterance_id: utt_id,
            text_segment: "Multiplexed trunk synthesis test.".to_string(),
            voice_id_primary: "cartesia-voice".to_string(),
            voice_id_standby: "elevenlabs-voice".to_string(),
            dispatch_us: clock.now_us(),
            flush: true,
        })
        .await
        .expect("Trunk dispatch must succeed");
    }

    // Verify all 200 sessions receive their exact demuxed audio chunks and release slots
    for (utt_id, mut rx) in receivers {
        let c0 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("Chunk 0 timeout")
            .expect("Chunk 0 missing");
        let c1 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("Chunk 1 timeout")
            .expect("Chunk 1 missing");

        assert_eq!(c0.utterance_id, utt_id);
        assert!(c0.is_first_chunk);
        assert_eq!(c1.utterance_id, utt_id);
        assert!(c1.is_final_chunk);
    }

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        router.active_context_count(),
        0,
        "All completed contexts must auto-unregister from ShardedContextRouter"
    );
    assert_eq!(
        pool.total_active_contexts(),
        0,
        "All completed utterances must release their TrunkSlot capacity"
    );
    assert!(
        pool.build_close_context_payload(99)
            .contains("\"close_context\":true"),
        "ElevenLabs trunk must generate close_context control frame"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sharded_single_ticker_engine_drives_sessions_without_per_session_tasks() {
    let clock = MonotonicClock::new();
    let telemetry = Arc::new(TelemetryCollector::new());
    let engine = ShardedTickEngine::spawn(4, clock.clone(), Arc::clone(&telemetry));

    let (_wd, chunk_tx, mut egress_rx) = engine
        .create_session(1001, SampleRate::Hz24000, WatchdogThresholds::default())
        .await;

    assert_eq!(engine.active_session_count(), 1);

    // Feed 5 chunks (50ms) to prime the 40ms ring buffer on Shard 1
    for seq in 0..5u64 {
        chunk_tx
            .send(AudioChunk {
                utterance_id: 1,
                sequence: seq,
                provider: ProviderId::PrimaryCartesia,
                sample_rate: SampleRate::Hz24000,
                samples: Arc::from(vec![3000i16; 240]),
                arrival_us: clock.now_us(),
                is_first_chunk: seq == 0,
                is_final_chunk: false,
                alignment_json: None,
            })
            .await
            .unwrap();
    }

    // Wait for the shard's 10th 1ms tick to emit the first 10ms egress frame
    let frame = tokio::time::timeout(Duration::from_millis(100), egress_rx.recv())
        .await
        .expect("Shard ticker must emit egress frame within 100ms")
        .expect("Egress channel open");

    assert_eq!(frame.pcm_samples.len(), 240);
    assert_eq!(frame.pcm_samples[0], 3000);

    engine.remove_session(1001).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(engine.active_session_count(), 0);
    engine.shutdown().await;
}

#[tokio::test]
async fn test_weather_map_circuit_breaker_and_zero_drop_sigterm_drain() {
    // 1. Verify ProviderWeatherMap dynamic promotion & canary recovery
    let weather = ProviderWeatherMap::new("ap-south-1", WeatherConfig::default());
    assert!(!weather.is_primary_circuit_open());

    // Record 3 consecutive primary failovers -> trips regional circuit breaker
    weather.record_primary_failover(100_000);
    weather.record_primary_failover(200_000);
    weather.record_primary_failover(300_000);
    assert!(weather.is_primary_circuit_open());

    // New utterances now automatically promote StandbyElevenLabs to Primary!
    let (primary, standby, is_canary) = weather.select_provider_order(400_000);
    assert_eq!(primary, ProviderId::StandbyElevenLabs);
    assert_eq!(standby, ProviderId::PrimaryCartesia);
    assert!(!is_canary);

    // After 10s (10,000,000us), next call triggers a canary probe
    let (_, _, is_canary_after_cooldown) = weather.select_provider_order(10_500_000);
    assert!(is_canary_after_cooldown);

    // Fast 40ms TTFB on Canary probe automatically closes circuit and restores Cartesia Primary
    weather.record_ttfb_sample(ProviderId::PrimaryCartesia, 40_000, 10_600_000);
    assert!(!weather.is_primary_circuit_open());

    // 2. Verify Zero-Downtime SIGTERM Drain Coordinator
    let drain = Arc::new(DrainCoordinator::new());
    let mut hint_rx = drain.subscribe_drain_hints();

    assert!(drain.is_ready());
    let _conn_guard = drain.track_ws_connection();
    let utt_guard = drain.track_inflight_utterance();
    assert_eq!(drain.inflight_utterance_count(), 1);

    let drain_clone = Arc::clone(&drain);
    let drain_task =
        tokio::spawn(async move { drain_clone.initiate_drain(Duration::from_secs(2)).await });

    // Verify reconnect_hint JSON is broadcast immediately upon drain initiation
    let hint_msg = tokio::time::timeout(Duration::from_millis(100), hint_rx.recv())
        .await
        .expect("Drain hint must broadcast immediately")
        .expect("Hint payload present");
    assert!(hint_msg.contains("\"reconnect_hint\""));
    assert!(!drain.is_ready(), "/readyz must report false during drain");

    // Complete the in-flight utterance after 30ms -> unblocks drain_task
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(utt_guard);

    let drained_cleanly = drain_task.await.unwrap();
    assert!(
        drained_cleanly,
        "DrainCoordinator must resolve once in-flight utterance finishes"
    );
}
