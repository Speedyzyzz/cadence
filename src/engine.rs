//! src/engine.rs
//! Subsystem 2: Sharded Single-Ticker Core-Pinned Engine.
//! Eliminates per-session Tokio timer task overhead by partitioning 100,000+ active
//! voice sessions across $K$ hardware-aligned shards. Each shard runs a single 1ms
//! master ticker that linearly sweeps its contiguous session slab.

use crate::protocol::{AudioChunk, EgressAudioFrame, FailoverSignal, MonotonicClock, SampleRate};
use crate::splicer::StatefulSplicer;
use crate::telemetry::TelemetryCollector;
use crate::watchdog::{LatencyWatchdog, WatchdogThresholds};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

/// Commands sent to a specific Engine Shard to register/remove sessions without locking other shards.
pub enum ShardCommand {
    RegisterSession {
        session_id: u64,
        sample_rate: SampleRate,
        ssrc: u32,
        watchdog: Arc<LatencyWatchdog>,
        chunk_rx: mpsc::Receiver<AudioChunk>,
        egress_tx: mpsc::Sender<EgressAudioFrame>,
        ack_tx: tokio::sync::oneshot::Sender<()>,
    },
    RemoveSession {
        session_id: u64,
    },
    Shutdown,
}

struct ManagedSession {
    session_id: u64,
    watchdog: Arc<LatencyWatchdog>,
    splicer: StatefulSplicer,
    chunk_rx: mpsc::Receiver<AudioChunk>,
    egress_tx: mpsc::Sender<EgressAudioFrame>,
    pending_trigger_us: Option<u64>,
}

/// Handle to a running Sharded Single-Ticker Engine.
pub struct ShardedTickEngine {
    num_shards: usize,
    shard_cmd_txs: Vec<mpsc::Sender<ShardCommand>>,
    #[allow(dead_code)]
    shard_handles: Vec<JoinHandle<()>>,
    active_sessions: Arc<AtomicUsize>,
    total_ticks_executed: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
    clock: MonotonicClock,
    telemetry: Arc<TelemetryCollector>,
}

impl ShardedTickEngine {
    /// Spawns `num_shards` worker loops (typically equal to physical CPU cores).
    /// Each shard runs a single 1ms ticker driving all sessions assigned to that shard.
    pub fn spawn(
        num_shards: usize,
        clock: MonotonicClock,
        telemetry: Arc<TelemetryCollector>,
    ) -> Arc<Self> {
        let num_shards = num_shards.max(1);
        let mut shard_cmd_txs = Vec::with_capacity(num_shards);
        let mut shard_handles = Vec::with_capacity(num_shards);
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let total_ticks_executed = Arc::new(AtomicU64::new(0));
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        for shard_id in 0..num_shards {
            let (cmd_tx, cmd_rx) = mpsc::channel::<ShardCommand>(4096);
            shard_cmd_txs.push(cmd_tx);

            let shard_clock = clock.clone();
            let shard_telemetry = Arc::clone(&telemetry);
            let shard_active = Arc::clone(&active_sessions);
            let shard_ticks = Arc::clone(&total_ticks_executed);
            let shard_shutdown = Arc::clone(&shutdown_flag);

            let handle = tokio::spawn(async move {
                Self::run_shard_loop(
                    shard_id,
                    cmd_rx,
                    shard_clock,
                    shard_telemetry,
                    shard_active,
                    shard_ticks,
                    shard_shutdown,
                )
                .await;
            });

            shard_handles.push(handle);
        }

        info!(
            num_shards,
            "Spawned ShardedTickEngine (single 1ms master timer per shard)"
        );

        Arc::new(Self {
            num_shards,
            shard_cmd_txs,
            shard_handles,
            active_sessions,
            total_ticks_executed,
            shutdown_flag,
            clock,
            telemetry,
        })
    }

    async fn run_shard_loop(
        _shard_id: usize,
        mut cmd_rx: mpsc::Receiver<ShardCommand>,
        clock: MonotonicClock,
        telemetry: Arc<TelemetryCollector>,
        active_sessions: Arc<AtomicUsize>,
        total_ticks_executed: Arc<AtomicU64>,
        shutdown_flag: Arc<AtomicBool>,
    ) {
        // Contiguous vector of sessions for L1/L2 cache-friendly linear iteration
        let mut sessions: Vec<ManagedSession> = Vec::with_capacity(2048);
        let mut id_to_index: HashMap<u64, usize> = HashMap::with_capacity(2048);

        let mut ticker = tokio::time::interval(Duration::from_millis(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut tick_counter: u64 = 0;

        loop {
            tokio::select! {
                biased;

                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ShardCommand::RegisterSession {
                            session_id,
                            sample_rate,
                            ssrc,
                            watchdog,
                            chunk_rx,
                            egress_tx,
                            ack_tx,
                        } => {
                            if !id_to_index.contains_key(&session_id) {
                                let splicer = StatefulSplicer::new(
                                    sample_rate,
                                    ssrc,
                                    clock.clone(),
                                    Arc::clone(&watchdog),
                                    Arc::clone(&telemetry),
                                );
                                let idx = sessions.len();
                                sessions.push(ManagedSession {
                                    session_id,
                                    watchdog,
                                    splicer,
                                    chunk_rx,
                                    egress_tx,
                                    pending_trigger_us: None,
                                });
                                id_to_index.insert(session_id, idx);
                                active_sessions.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = ack_tx.send(());
                        }
                        ShardCommand::RemoveSession { session_id } => {
                            if let Some(idx) = id_to_index.remove(&session_id) {
                                sessions.swap_remove(idx);
                                if idx < sessions.len() {
                                    let moved_id = sessions[idx].session_id;
                                    id_to_index.insert(moved_id, idx);
                                }
                                active_sessions.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        ShardCommand::Shutdown => {
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if shutdown_flag.load(Ordering::Acquire) {
                        break;
                    }

                    tick_counter = tick_counter.wrapping_add(1);
                    total_ticks_executed.fetch_add(1, Ordering::Relaxed);
                    let is_10ms_egress_tick = tick_counter % 10 == 0;
                    let now_us = clock.now_us();

                    let mut dead_sessions: Vec<u64> = Vec::new();

                    // Linearly sweep all sessions on this shard
                    for session in sessions.iter_mut() {
                        // 1. Drain all newly arrived audio chunks for this session without blocking
                        while let Ok(chunk) = session.chunk_rx.try_recv() {
                            session.watchdog.observe_chunk(&chunk);
                            session.splicer.ingest_chunk(chunk);
                        }

                        // 2. Evaluate 1ms Watchdog latency & predictive jitter equations
                        if let Some(signal) = session.watchdog.evaluate_at(now_us) {
                            if let FailoverSignal::SwitchToStandby { trigger_us, .. } = signal {
                                session.pending_trigger_us = Some(trigger_us);
                            }
                            session.splicer.handle_failover_signal(signal);
                        }

                        // 3. On every 10th tick (10ms cadence), pull and emit the RTP/Opus egress frame
                        if is_10ms_egress_tick {
                            let trig = session.pending_trigger_us.take();
                            if let Some(frame) = session.splicer.pull_10ms_egress_frame(trig) {
                                if session.egress_tx.try_send(frame).is_err() {
                                    dead_sessions.push(session.session_id);
                                }
                            }
                        }
                    }

                    // Clean up any disconnected sessions using O(1) swap_remove
                    for dead_id in dead_sessions {
                        if let Some(idx) = id_to_index.remove(&dead_id) {
                            sessions.swap_remove(idx);
                            if idx < sessions.len() {
                                let moved_id = sessions[idx].session_id;
                                id_to_index.insert(moved_id, idx);
                            }
                            active_sessions.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    /// Registers a new voice session onto its deterministically hashed CPU shard
    /// and returns `(Arc<LatencyWatchdog>, mpsc::Sender<AudioChunk>, mpsc::Receiver<EgressAudioFrame>)`.
    pub async fn create_session(
        &self,
        session_id: u64,
        sample_rate: SampleRate,
        thresholds: WatchdogThresholds,
    ) -> (
        Arc<LatencyWatchdog>,
        mpsc::Sender<AudioChunk>,
        mpsc::Receiver<EgressAudioFrame>,
    ) {
        let shard_idx = (session_id as usize) % self.num_shards;
        let (chunk_tx, chunk_rx) = mpsc::channel(512);
        let (egress_tx, egress_rx) = mpsc::channel(512);

        let (watchdog, _unused_rx) =
            LatencyWatchdog::new(thresholds, self.clock.clone(), Arc::clone(&self.telemetry));

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let _ = self.shard_cmd_txs[shard_idx]
            .send(ShardCommand::RegisterSession {
                session_id,
                sample_rate,
                ssrc: 0xCADE_0000u32.wrapping_add(session_id as u32),
                watchdog: Arc::clone(&watchdog),
                chunk_rx,
                egress_tx,
                ack_tx,
            })
            .await;
        let _ = ack_rx.await;

        (watchdog, chunk_tx, egress_rx)
    }

    pub async fn remove_session(&self, session_id: u64) {
        let shard_idx = (session_id as usize) % self.num_shards;
        let _ = self.shard_cmd_txs[shard_idx]
            .send(ShardCommand::RemoveSession { session_id })
            .await;
    }

    pub fn active_session_count(&self) -> usize {
        self.active_sessions.load(Ordering::Relaxed)
    }

    pub fn total_ticks(&self) -> u64 {
        self.total_ticks_executed.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        for tx in &self.shard_cmd_txs {
            let _ = tx.send(ShardCommand::Shutdown).await;
        }
    }
}
