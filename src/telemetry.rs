//! src/telemetry.rs
//! Module D: Lock-Free Prometheus Telemetry Plane & Event Emitter.
//! Tracks provider RTT, TTFB, failover switch count by reason, saved calls, jitter
//! percentiles (p50, p95, p99), rolling TTFB for canary shadowing decisions, and splice
//! latency using wait-free 64-bucket microsecond histograms.

use crate::protocol::{FailoverReason, MonotonicClock, ProviderId};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

const HISTOGRAM_BUCKETS: usize = 64;
const ROLLING_TTFB_WINDOW: usize = 32;

/// Lock-free fixed-bucket latency & jitter histogram with microsecond resolution.
/// First 32 buckets use 500us steps (0..16ms); remaining 32 buckets use 4ms steps up to 144ms+.
pub struct LockFreeHistogram {
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
    upper_bounds_us: [u64; HISTOGRAM_BUCKETS],
    total_count: AtomicU64,
    sum_us: AtomicU64,
}

impl Default for LockFreeHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LockFreeHistogram {
    pub fn new() -> Self {
        let mut upper_bounds_us = [0u64; HISTOGRAM_BUCKETS];
        for (i, bound) in upper_bounds_us.iter_mut().enumerate() {
            if i < 32 {
                *bound = ((i as u64) + 1) * 500;
            } else {
                *bound = 16_000 + ((i as u64 - 31) * 4_000);
            }
        }

        let buckets = std::array::from_fn(|_| AtomicU64::new(0));
        Self {
            buckets,
            upper_bounds_us,
            total_count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn record(&self, value_us: u64) {
        let idx = self
            .upper_bounds_us
            .partition_point(|&bound| bound < value_us)
            .min(HISTOGRAM_BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(value_us, Ordering::Relaxed);
    }

    /// Computes exact bucketed percentile (`0.50`, `0.95`, `0.99`) in microseconds.
    pub fn percentile_us(&self, quantile: f64) -> u64 {
        let count = self.total_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        let target_rank = ((count as f64) * quantile.clamp(0.0, 1.0)).ceil() as u64;
        let mut cumulative = 0u64;
        for i in 0..HISTOGRAM_BUCKETS {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= target_rank {
                return self.upper_bounds_us[i];
            }
        }
        self.upper_bounds_us[HISTOGRAM_BUCKETS - 1]
    }

    pub fn count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    pub fn sum_us(&self) -> u64 {
        self.sum_us.load(Ordering::Relaxed)
    }
}

/// Fixed-capacity ring buffer of recent TTFB values for rolling percentile computation.
/// Used by the Canary shadowing strategy to detect primary degradation trends.
pub struct RollingTtfbRing {
    values: [AtomicU64; ROLLING_TTFB_WINDOW],
    write_idx: AtomicUsize,
    count: AtomicUsize,
}

impl Default for RollingTtfbRing {
    fn default() -> Self {
        Self::new()
    }
}

impl RollingTtfbRing {
    pub fn new() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            write_idx: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, ttfb_us: u64) {
        let idx = self.write_idx.fetch_add(1, Ordering::Relaxed) % ROLLING_TTFB_WINDOW;
        self.values[idx].store(ttfb_us, Ordering::Relaxed);
        let current_count = self.count.load(Ordering::Relaxed);
        if current_count < ROLLING_TTFB_WINDOW {
            self.count.store(current_count + 1, Ordering::Relaxed);
        }
    }

    /// Computes the p95 TTFB from the rolling window. Returns 0 if fewer than 4 samples.
    pub fn rolling_p95_us(&self) -> u64 {
        let n = self.count.load(Ordering::Relaxed);
        if n < 4 {
            return 0;
        }

        let mut vals: Vec<u64> = (0..n)
            .map(|i| self.values[i].load(Ordering::Relaxed))
            .collect();
        vals.sort_unstable();

        let idx_95 = ((n as f64 * 0.95).ceil() as usize).saturating_sub(1).min(n - 1);
        vals[idx_95]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub timestamp_us: u64,
    pub event_type: String,
    pub details: String,
}

/// Module D: Lock-Free Prometheus Telemetry Collector & Event Emitter.
pub struct TelemetryCollector {
    primary_rtt_us: AtomicU64,
    standby_rtt_us: AtomicU64,
    primary_ttfb_hist: LockFreeHistogram,
    standby_ttfb_hist: LockFreeHistogram,
    primary_rolling_ttfb: RollingTtfbRing,
    jitter_hist: LockFreeHistogram,
    splice_duration_hist: LockFreeHistogram,
    failover_switch_total: AtomicU64,
    failover_ttfb_total: AtomicU64,
    failover_spike_total: AtomicU64,
    failover_predictive_jitter_total: AtomicU64,
    failover_socket_drop_total: AtomicU64,
    saved_calls_total: AtomicU64,
    egress_frames_total: AtomicU64,
    dropped_frames_total: AtomicU64,
    event_tx: broadcast::Sender<TelemetryEvent>,
}

impl Default for TelemetryCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryCollector {
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            primary_rtt_us: AtomicU64::new(0),
            standby_rtt_us: AtomicU64::new(0),
            primary_ttfb_hist: LockFreeHistogram::new(),
            standby_ttfb_hist: LockFreeHistogram::new(),
            primary_rolling_ttfb: RollingTtfbRing::new(),
            jitter_hist: LockFreeHistogram::new(),
            splice_duration_hist: LockFreeHistogram::new(),
            failover_switch_total: AtomicU64::new(0),
            failover_ttfb_total: AtomicU64::new(0),
            failover_spike_total: AtomicU64::new(0),
            failover_predictive_jitter_total: AtomicU64::new(0),
            failover_socket_drop_total: AtomicU64::new(0),
            saved_calls_total: AtomicU64::new(0),
            egress_frames_total: AtomicU64::new(0),
            dropped_frames_total: AtomicU64::new(0),
            event_tx,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.event_tx.subscribe()
    }

    #[inline(always)]
    pub fn record_provider_rtt(&self, provider: ProviderId, rtt_us: u64) {
        match provider {
            ProviderId::PrimaryCartesia => self.primary_rtt_us.store(rtt_us, Ordering::Relaxed),
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => {
                self.standby_rtt_us.store(rtt_us, Ordering::Relaxed)
            }
        }
    }

    #[inline(always)]
    pub fn record_ttfb(&self, provider: ProviderId, ttfb_us: u64) {
        match provider {
            ProviderId::PrimaryCartesia => {
                self.primary_ttfb_hist.record(ttfb_us);
                self.primary_rolling_ttfb.push(ttfb_us);
            }
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => {
                self.standby_ttfb_hist.record(ttfb_us);
            }
        }
    }

    /// Returns the rolling p95 TTFB for the primary provider over the last 32 utterances.
    /// Used by the Canary shadowing strategy to decide when to auto-escalate.
    pub fn primary_rolling_p95_ttfb_us(&self) -> u64 {
        self.primary_rolling_ttfb.rolling_p95_us()
    }

    #[inline(always)]
    pub fn record_jitter(&self, jitter_us: u64) {
        self.jitter_hist.record(jitter_us);
    }

    pub fn record_failover_triggered(&self, reason: FailoverReason) {
        self.failover_switch_total.fetch_add(1, Ordering::Relaxed);
        match reason {
            FailoverReason::TtfbExceeded120ms => {
                self.failover_ttfb_total.fetch_add(1, Ordering::Relaxed);
            }
            FailoverReason::InterChunkSpikeExceeded150ms => {
                self.failover_spike_total.fetch_add(1, Ordering::Relaxed);
            }
            FailoverReason::PredictiveJitterUnderrunRisk => {
                self.failover_predictive_jitter_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            FailoverReason::PrimarySocketClosed => {
                self.failover_socket_drop_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let _ = self.event_tx.send(TelemetryEvent {
            timestamp_us: MonotonicClock::wall_clock_us(),
            event_type: "failover_triggered".to_string(),
            details: format!("reason={reason}"),
        });
    }

    pub fn record_splice_completed(&self, splice_duration_us: u64) {
        self.splice_duration_hist.record(splice_duration_us);
        self.saved_calls_total.fetch_add(1, Ordering::Relaxed);

        let _ = self.event_tx.send(TelemetryEvent {
            timestamp_us: MonotonicClock::wall_clock_us(),
            event_type: "hot_splice_completed".to_string(),
            details: format!("splice_duration_us={splice_duration_us}"),
        });
    }

    #[inline(always)]
    pub fn increment_egress_frames(&self) {
        self.egress_frames_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn increment_dropped_frames(&self) {
        self.dropped_frames_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failover_count(&self) -> u64 {
        self.failover_switch_total.load(Ordering::Relaxed)
    }

    pub fn saved_calls_count(&self) -> u64 {
        self.saved_calls_total.load(Ordering::Relaxed)
    }

    pub fn dropped_frames_count(&self) -> u64 {
        self.dropped_frames_total.load(Ordering::Relaxed)
    }

    pub fn egress_frames_count(&self) -> u64 {
        self.egress_frames_total.load(Ordering::Relaxed)
    }

    pub fn jitter_percentiles_us(&self) -> (u64, u64, u64) {
        (
            self.jitter_hist.percentile_us(0.50),
            self.jitter_hist.percentile_us(0.95),
            self.jitter_hist.percentile_us(0.99),
        )
    }

    /// Exports all metrics in standard Prometheus Exposition Format (`text/plain; version=0.0.4`).
    pub fn render_prometheus_metrics(&self) -> String {
        let mut out = String::with_capacity(4096);
        let (j_p50, j_p95, j_p99) = self.jitter_percentiles_us();
        let (s_p50, s_p95, s_p99) = (
            self.splice_duration_hist.percentile_us(0.50),
            self.splice_duration_hist.percentile_us(0.95),
            self.splice_duration_hist.percentile_us(0.99),
        );

        let _ = writeln!(
            out,
            "# HELP cadence_provider_rtt_microseconds Upstream WebSocket Ping/Pong RTT in microseconds."
        );
        let _ = writeln!(out, "# TYPE cadence_provider_rtt_microseconds gauge");
        let _ = writeln!(
            out,
            "cadence_provider_rtt_microseconds{{provider=\"cartesia_primary\"}} {}",
            self.primary_rtt_us.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "cadence_provider_rtt_microseconds{{provider=\"elevenlabs_standby\"}} {}",
            self.standby_rtt_us.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cadence_ttfb_microseconds Primary TTFB percentiles in microseconds."
        );
        let _ = writeln!(out, "# TYPE cadence_ttfb_microseconds summary");
        let _ = writeln!(
            out,
            "cadence_ttfb_microseconds{{provider=\"cartesia_primary\",quantile=\"0.5\"}} {}",
            self.primary_ttfb_hist.percentile_us(0.50)
        );
        let _ = writeln!(
            out,
            "cadence_ttfb_microseconds{{provider=\"cartesia_primary\",quantile=\"0.95\"}} {}",
            self.primary_ttfb_hist.percentile_us(0.95)
        );
        let _ = writeln!(
            out,
            "cadence_ttfb_microseconds{{provider=\"cartesia_primary\",quantile=\"0.99\"}} {}",
            self.primary_ttfb_hist.percentile_us(0.99)
        );
        let _ = writeln!(
            out,
            "cadence_ttfb_rolling_p95_microseconds{{provider=\"cartesia_primary\"}} {}",
            self.primary_rolling_p95_ttfb_us()
        );

        let _ = writeln!(
            out,
            "# HELP cadence_failover_switches_total Total number of hot-splice failovers triggered."
        );
        let _ = writeln!(out, "# TYPE cadence_failover_switches_total counter");
        let _ = writeln!(
            out,
            "cadence_failover_switches_total{{reason=\"ttfb_exceeded_120ms\"}} {}",
            self.failover_ttfb_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "cadence_failover_switches_total{{reason=\"inter_chunk_spike_150ms\"}} {}",
            self.failover_spike_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "cadence_failover_switches_total{{reason=\"predictive_jitter_underrun\"}} {}",
            self.failover_predictive_jitter_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "cadence_failover_switches_total{{reason=\"primary_socket_closed\"}} {}",
            self.failover_socket_drop_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP cadence_saved_calls_total Total voice sessions saved via zero-drop hot splice."
        );
        let _ = writeln!(out, "# TYPE cadence_saved_calls_total counter");
        let _ = writeln!(out, "cadence_saved_calls_total {}", self.saved_calls_count());

        let _ = writeln!(
            out,
            "# HELP cadence_egress_frames_total Total egress audio frames emitted."
        );
        let _ = writeln!(out, "# TYPE cadence_egress_frames_total counter");
        let _ = writeln!(
            out,
            "cadence_egress_frames_total {}",
            self.egress_frames_count()
        );

        let _ = writeln!(
            out,
            "# HELP cadence_dropped_audio_frames_total Total dropped egress audio frames."
        );
        let _ = writeln!(out, "# TYPE cadence_dropped_audio_frames_total counter");
        let _ = writeln!(
            out,
            "cadence_dropped_audio_frames_total {}",
            self.dropped_frames_count()
        );

        let _ = writeln!(
            out,
            "# HELP cadence_jitter_microseconds Audio chunk arrival jitter percentiles."
        );
        let _ = writeln!(out, "# TYPE cadence_jitter_microseconds summary");
        let _ = writeln!(
            out,
            "cadence_jitter_microseconds{{quantile=\"0.5\"}} {j_p50}"
        );
        let _ = writeln!(
            out,
            "cadence_jitter_microseconds{{quantile=\"0.95\"}} {j_p95}"
        );
        let _ = writeln!(
            out,
            "cadence_jitter_microseconds{{quantile=\"0.99\"}} {j_p99}"
        );
        let _ = writeln!(
            out,
            "cadence_jitter_microseconds_sum {}",
            self.jitter_hist.sum_us()
        );
        let _ = writeln!(
            out,
            "cadence_jitter_microseconds_count {}",
            self.jitter_hist.count()
        );

        let _ = writeln!(
            out,
            "# HELP cadence_splice_latency_microseconds Time from watchdog trigger to completed 5ms crossfade."
        );
        let _ = writeln!(out, "# TYPE cadence_splice_latency_microseconds summary");
        let _ = writeln!(
            out,
            "cadence_splice_latency_microseconds{{quantile=\"0.5\"}} {s_p50}"
        );
        let _ = writeln!(
            out,
            "cadence_splice_latency_microseconds{{quantile=\"0.95\"}} {s_p95}"
        );
        let _ = writeln!(
            out,
            "cadence_splice_latency_microseconds{{quantile=\"0.99\"}} {s_p99}"
        );

        out
    }
}

/// Spawns a lightweight, zero-dependency HTTP `/metrics` exporter on the given TCP bind address.
pub async fn spawn_prometheus_http_exporter(
    bind_addr: &str,
    telemetry: Arc<TelemetryCollector>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let handle = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let metrics_body = telemetry.render_prometheus_metrics();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                metrics_body.len(),
                metrics_body
            );
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
        }
    });
    Ok(handle)
}
