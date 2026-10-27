//! src/watchdog.rs
//! Module C: Failure Detector & Latency Watchdog.
//! Continuously samples chunk arrival timestamps, RFC 3550 EWMA jitter, splicer buffer
//! occupancy B(t_now), and Time-to-First-Byte (TTFB). Triggers a lock-free
//! atomic state transition to the warm standby stream within 1ms when:
//!   1. Primary TTFB exceeds 120ms after an LLM token dispatch.
//!   2. Mid-utterance inter-chunk arrival gap exceeds 150ms.
//!   3. Predictive jitter underrun risk: Δt_gap + 2.5 * J_i > 150ms
//!      while splicer buffer reserve B(t_now) <= 15ms.
//!   4. Primary WebSocket drops unexpectedly.

use crate::protocol::{AudioChunk, FailoverReason, FailoverSignal, MonotonicClock, ProviderId};
use crate::telemetry::TelemetryCollector;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WatchdogState {
    PrimaryActive = 0,
    SplicingToStandby = 1,
    StandbyActive = 2,
}

impl WatchdogState {
    #[inline(always)]
    pub fn from_u8(val: u8) -> Self {
        match val {
            0 => WatchdogState::PrimaryActive,
            1 => WatchdogState::SplicingToStandby,
            _ => WatchdogState::StandbyActive,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WatchdogThresholds {
    /// Maximum allowed Time-to-First-Byte after LLM token dispatch (120,000 us = 120ms).
    pub max_ttfb_us: u64,
    /// Maximum allowed inter-chunk arrival stall during streaming (150,000 us = 150ms).
    pub max_inter_chunk_spike_us: u64,
    /// Minimum required splicer buffer reserve before predictive jitter triggers failover (15,000 us = 15ms).
    pub min_splice_reserve_us: u64,
    /// Watchdog evaluation polling interval (1,000 us = 1ms).
    pub poll_interval_us: u64,
}

impl Default for WatchdogThresholds {
    fn default() -> Self {
        Self {
            max_ttfb_us: 120_000,
            max_inter_chunk_spike_us: 150_000,
            min_splice_reserve_us: 15_000,
            poll_interval_us: 1_000,
        }
    }
}

/// Module C: Lock-Free Failure Detector & Latency Watchdog.
pub struct LatencyWatchdog {
    thresholds: WatchdogThresholds,
    state: AtomicU8,
    active_utterance_id: AtomicU64,
    utterance_dispatch_us: AtomicU64,
    awaiting_first_byte: AtomicBool,
    in_active_utterance: AtomicBool,
    last_primary_chunk_us: AtomicU64,
    last_expected_duration_us: AtomicU64,
    ewma_jitter_us: AtomicU64,
    primary_buffer_occupancy_us: AtomicU64,
    failover_tx: broadcast::Sender<FailoverSignal>,
    clock: MonotonicClock,
    telemetry: Arc<TelemetryCollector>,
}

impl LatencyWatchdog {
    pub fn new(
        thresholds: WatchdogThresholds,
        clock: MonotonicClock,
        telemetry: Arc<TelemetryCollector>,
    ) -> (Arc<Self>, broadcast::Receiver<FailoverSignal>) {
        let (failover_tx, failover_rx) = broadcast::channel(64);
        let watchdog = Arc::new(Self {
            thresholds,
            state: AtomicU8::new(WatchdogState::PrimaryActive as u8),
            active_utterance_id: AtomicU64::new(0),
            utterance_dispatch_us: AtomicU64::new(0),
            awaiting_first_byte: AtomicBool::new(false),
            in_active_utterance: AtomicBool::new(false),
            last_primary_chunk_us: AtomicU64::new(0),
            last_expected_duration_us: AtomicU64::new(10_000),
            ewma_jitter_us: AtomicU64::new(0),
            // Initialize to 40,000us (40ms) nominal holdback target
            primary_buffer_occupancy_us: AtomicU64::new(40_000),
            failover_tx,
            clock,
            telemetry,
        });

        (watchdog, failover_rx)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FailoverSignal> {
        self.failover_tx.subscribe()
    }

    #[inline(always)]
    pub fn current_state(&self) -> WatchdogState {
        WatchdogState::from_u8(self.state.load(Ordering::Acquire))
    }

    #[inline(always)]
    pub fn is_standby_active(&self) -> bool {
        self.current_state() != WatchdogState::PrimaryActive
    }

    /// Updates the unread occupancy B(t_now) of the primary 40ms ring buffer in microseconds.
    #[inline(always)]
    pub fn set_primary_buffer_occupancy_us(&self, occupancy_us: u64) {
        self.primary_buffer_occupancy_us
            .store(occupancy_us, Ordering::Release);
    }

    #[inline(always)]
    pub fn primary_buffer_occupancy_us(&self) -> u64 {
        self.primary_buffer_occupancy_us.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn ewma_jitter_us(&self) -> u64 {
        self.ewma_jitter_us.load(Ordering::Acquire)
    }

    /// Called by the Multiplexer when an LLM token segment is dispatched upstream.
    pub fn on_utterance_dispatched(&self, utterance_id: u64, dispatch_us: u64) {
        self.active_utterance_id
            .store(utterance_id, Ordering::Release);
        self.utterance_dispatch_us
            .store(dispatch_us, Ordering::Release);
        self.awaiting_first_byte.store(true, Ordering::Release);
        self.in_active_utterance.store(true, Ordering::Release);
    }

    /// Observes an incoming audio chunk, updating TTFB metrics and RFC 3550 EWMA jitter.
    pub fn observe_chunk(&self, chunk: &AudioChunk) {
        let arrival_us = chunk.arrival_us;

        if chunk.provider == ProviderId::PrimaryCartesia {
            if self.awaiting_first_byte.swap(false, Ordering::AcqRel) {
                let dispatch_us = self.utterance_dispatch_us.load(Ordering::Acquire);
                if arrival_us >= dispatch_us && dispatch_us > 0 {
                    let ttfb_us = arrival_us - dispatch_us;
                    self.telemetry
                        .record_ttfb(ProviderId::PrimaryCartesia, ttfb_us);
                }
            } else {
                let prev_arrival = self.last_primary_chunk_us.load(Ordering::Acquire);
                let expected_dur = self.last_expected_duration_us.load(Ordering::Acquire);
                if prev_arrival > 0 && arrival_us >= prev_arrival {
                    let delta = arrival_us - prev_arrival;
                    let transit_deviation = delta.abs_diff(expected_dur);

                    // RFC 3550 EWMA: J_i = J_{i-1} + (|d_i| - J_{i-1}) / 16
                    let prev_jitter = self.ewma_jitter_us.load(Ordering::Acquire);
                    let updated_jitter = if transit_deviation >= prev_jitter {
                        prev_jitter + (transit_deviation - prev_jitter) / 16
                    } else {
                        prev_jitter - (prev_jitter - transit_deviation) / 16
                    };

                    self.ewma_jitter_us.store(updated_jitter, Ordering::Release);
                    self.telemetry.record_jitter(updated_jitter);
                }
            }

            self.last_primary_chunk_us
                .store(arrival_us, Ordering::Release);
            self.last_expected_duration_us
                .store(chunk.duration_us(), Ordering::Release);

            if chunk.is_final_chunk {
                self.in_active_utterance.store(false, Ordering::Release);
            }
        } else if chunk.provider == ProviderId::StandbyElevenLabs && chunk.is_first_chunk {
            let dispatch_us = self.utterance_dispatch_us.load(Ordering::Acquire);
            if arrival_us >= dispatch_us && dispatch_us > 0 {
                self.telemetry
                    .record_ttfb(ProviderId::StandbyElevenLabs, arrival_us - dispatch_us);
            }
        }
    }

    /// Evaluates latency and predictive jitter thresholds at monotonic timestamp `now_us`.
    pub fn evaluate_at(&self, now_us: u64) -> Option<FailoverSignal> {
        if self.current_state() != WatchdogState::PrimaryActive {
            return None;
        }

        if !self.in_active_utterance.load(Ordering::Acquire) {
            return None;
        }

        let utterance_id = self.active_utterance_id.load(Ordering::Acquire);

        // Trigger Condition 1: Time-to-First-Byte (TTFB) > 120ms after LLM token dispatch
        if self.awaiting_first_byte.load(Ordering::Acquire) {
            let dispatch_us = self.utterance_dispatch_us.load(Ordering::Acquire);
            if dispatch_us > 0
                && now_us.saturating_sub(dispatch_us) > self.thresholds.max_ttfb_us
            {
                return self.try_trigger_failover(
                    utterance_id,
                    now_us,
                    FailoverReason::TtfbExceeded120ms,
                );
            }
        } else {
            // Trigger Condition 2: Hard 150ms inter-chunk stall OR predictive jitter underrun
            let last_chunk_us = self.last_primary_chunk_us.load(Ordering::Acquire);
            if last_chunk_us > 0 {
                let silence_gap_us = now_us.saturating_sub(last_chunk_us);
                if silence_gap_us > self.thresholds.max_inter_chunk_spike_us {
                    return self.try_trigger_failover(
                        utterance_id,
                        now_us,
                        FailoverReason::InterChunkSpikeExceeded150ms,
                    );
                }

                let jitter_us = self.ewma_jitter_us.load(Ordering::Acquire);
                let buffer_us = self.primary_buffer_occupancy_us.load(Ordering::Acquire);
                let predicted_gap_us = silence_gap_us + (jitter_us * 5) / 2;

                if predicted_gap_us > self.thresholds.max_inter_chunk_spike_us
                    && buffer_us <= self.thresholds.min_splice_reserve_us
                {
                    return self.try_trigger_failover(
                        utterance_id,
                        now_us,
                        FailoverReason::PredictiveJitterUnderrunRisk,
                    );
                }
            }
        }

        None
    }

    pub fn force_failover_on_socket_drop(
        &self,
        utterance_id: u64,
        now_us: u64,
    ) -> Option<FailoverSignal> {
        self.try_trigger_failover(
            utterance_id,
            now_us,
            FailoverReason::PrimarySocketClosed,
        )
    }

    fn try_trigger_failover(
        &self,
        utterance_id: u64,
        trigger_us: u64,
        reason: FailoverReason,
    ) -> Option<FailoverSignal> {
        if self
            .state
            .compare_exchange(
                WatchdogState::PrimaryActive as u8,
                WatchdogState::StandbyActive as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            let signal = FailoverSignal::SwitchToStandby {
                utterance_id,
                trigger_us,
                reason,
            };
            warn!(
                utterance_id,
                trigger_us,
                %reason,
                "Watchdog atomic state transition: PrimaryActive -> StandbyActive"
            );
            self.telemetry.record_failover_triggered(reason);
            let _ = self.failover_tx.send(signal);
            Some(signal)
        } else {
            None
        }
    }

    /// Spawns a dedicated 1ms-resolution background watchdog task and chunk interceptor tap.
    pub fn spawn_monitor_tasks(
        self: &Arc<Self>,
        mut ingress_chunk_rx: mpsc::Receiver<AudioChunk>,
        splicer_chunk_tx: mpsc::Sender<AudioChunk>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        let wd_tap = Arc::clone(self);
        let tap_handle = tokio::spawn(async move {
            while let Some(chunk) = ingress_chunk_rx.recv().await {
                wd_tap.observe_chunk(&chunk);
                if splicer_chunk_tx.send(chunk).await.is_err() {
                    break;
                }
            }
        });

        let wd_poll = Arc::clone(self);
        let poll_interval = std::time::Duration::from_micros(self.thresholds.poll_interval_us);
        let poll_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now_us = wd_poll.clock.now_us();
                wd_poll.evaluate_at(now_us);
            }
        });

        (tap_handle, poll_handle)
    }
}
