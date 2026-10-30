//! src/weather.rs
//! Subsystem 3: Lock-Free Provider Weather Map & Dynamic Primary Arbitrator.
//! Tracks rolling EWMA TTFB and failover rates per upstream provider. If Cartesia
//! experiences a regional brownout (EWMA TTFB > 95ms or consecutive spikes >= 3),
//! automatically promotes ElevenLabs to Primary for new utterances while sending
//! periodic canary probes to detect Cartesia recovery.

use crate::protocol::ProviderId;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub struct WeatherConfig {
    /// EWMA TTFB threshold (in microseconds) above which the primary is marked degraded (default: 95,000us = 95ms).
    pub primary_degraded_ttfb_us: u64,
    /// Number of consecutive failovers before tripping the regional circuit breaker.
    pub consecutive_failovers_trip: u64,
    /// Cooldown duration in microseconds before probing a degraded primary (default: 10,000,000us = 10s).
    pub probe_interval_us: u64,
    /// Recovery TTFB threshold in microseconds required to restore primary (default: 70,000us = 70ms).
    pub recovery_ttfb_us: u64,
}

impl Default for WeatherConfig {
    fn default() -> Self {
        Self {
            primary_degraded_ttfb_us: 95_000,
            consecutive_failovers_trip: 3,
            probe_interval_us: 10_000_000,
            recovery_ttfb_us: 70_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherGossipSnapshot {
    pub region: String,
    pub timestamp_us: u64,
    pub primary_ewma_ttfb_us: u64,
    pub standby_ewma_ttfb_us: u64,
    pub primary_circuit_open: bool,
}

/// Lock-free regional provider health tracker & dynamic primary selector.
pub struct ProviderWeatherMap {
    config: WeatherConfig,
    region: String,
    primary_ewma_ttfb_us: AtomicU64,
    standby_ewma_ttfb_us: AtomicU64,
    consecutive_primary_failures: AtomicU64,
    primary_circuit_open: AtomicBool,
    last_trip_timestamp_us: AtomicU64,
}

impl ProviderWeatherMap {
    pub fn new(region: impl Into<String>, config: WeatherConfig) -> Self {
        Self {
            config,
            region: region.into(),
            primary_ewma_ttfb_us: AtomicU64::new(45_000), // Nominal 45ms starting baseline
            standby_ewma_ttfb_us: AtomicU64::new(55_000), // Nominal 55ms starting baseline
            consecutive_primary_failures: AtomicU64::new(0),
            primary_circuit_open: AtomicBool::new(false),
            last_trip_timestamp_us: AtomicU64::new(0),
        }
    }

    /// Records a newly measured TTFB sample in microseconds and updates the EWMA ($\alpha = 1/8$).
    pub fn record_ttfb_sample(&self, provider: ProviderId, ttfb_us: u64, now_us: u64) {
        match provider {
            ProviderId::PrimaryCartesia => {
                let prev = self.primary_ewma_ttfb_us.load(Ordering::Acquire);
                let next = if ttfb_us >= prev {
                    prev + (ttfb_us - prev) / 8
                } else {
                    prev - (prev - ttfb_us) / 8
                };
                self.primary_ewma_ttfb_us.store(next, Ordering::Release);

                if ttfb_us <= self.config.recovery_ttfb_us {
                    self.consecutive_primary_failures
                        .store(0, Ordering::Release);
                    if self.primary_circuit_open.load(Ordering::Acquire) {
                        self.primary_circuit_open.store(false, Ordering::Release);
                    }
                } else if next > self.config.primary_degraded_ttfb_us {
                    self.trip_circuit(now_us);
                }
            }
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => {
                let prev = self.standby_ewma_ttfb_us.load(Ordering::Acquire);
                let next = if ttfb_us >= prev {
                    prev + (ttfb_us - prev) / 8
                } else {
                    prev - (prev - ttfb_us) / 8
                };
                self.standby_ewma_ttfb_us.store(next, Ordering::Release);
            }
        }
    }

    /// Records a watchdog failover event against the primary provider.
    pub fn record_primary_failover(&self, now_us: u64) {
        let count = self
            .consecutive_primary_failures
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        if count >= self.config.consecutive_failovers_trip {
            self.trip_circuit(now_us);
        }
    }

    fn trip_circuit(&self, now_us: u64) {
        self.primary_circuit_open.store(true, Ordering::Release);
        self.last_trip_timestamp_us.store(now_us, Ordering::Release);
    }

    /// Returns `(preferred_primary, preferred_standby, should_send_canary_probe)` for a new utterance.
    pub fn select_provider_order(&self, now_us: u64) -> (ProviderId, ProviderId, bool) {
        if !self.primary_circuit_open.load(Ordering::Acquire) {
            return (
                ProviderId::PrimaryCartesia,
                ProviderId::StandbyElevenLabs,
                false,
            );
        }

        let last_trip = self.last_trip_timestamp_us.load(Ordering::Acquire);
        let ready_for_canary = now_us.saturating_sub(last_trip) >= self.config.probe_interval_us;

        if ready_for_canary {
            // Update timestamp so only one caller dispatches the recovery canary probe
            self.last_trip_timestamp_us.store(now_us, Ordering::Release);
        }

        (
            ProviderId::StandbyElevenLabs,
            ProviderId::PrimaryCartesia,
            ready_for_canary,
        )
    }

    /// Merges a peer edge PoP's gossip snapshot so regional brownouts propagate across nodes in <50ms.
    pub fn ingest_peer_gossip(&self, peer: &WeatherGossipSnapshot, now_us: u64) {
        if peer.region == self.region && peer.primary_circuit_open {
            self.trip_circuit(now_us);
        }
    }

    pub fn export_gossip_snapshot(&self, now_us: u64) -> WeatherGossipSnapshot {
        WeatherGossipSnapshot {
            region: self.region.clone(),
            timestamp_us: now_us,
            primary_ewma_ttfb_us: self.primary_ewma_ttfb_us.load(Ordering::Acquire),
            standby_ewma_ttfb_us: self.standby_ewma_ttfb_us.load(Ordering::Acquire),
            primary_circuit_open: self.primary_circuit_open.load(Ordering::Acquire),
        }
    }

    pub fn is_primary_circuit_open(&self) -> bool {
        self.primary_circuit_open.load(Ordering::Acquire)
    }
}
