//! src/splicer.rs
//! Module B: Stateful Splicer, 40ms Holdback Ring Buffer, RMS Loudness Normalizer,
//! 450ms Acoustic Backchannel Injector, Binary Oscilloscope Tap, and Phase-Aligned 5ms Hann Crossfader.

use crate::alignment::WordAlignmentIndex;
use crate::backchannel::{
    BackchannelController, BackchannelPhase, DEFAULT_BACKCHANNEL_THRESHOLD_US,
};
use crate::inspector_tap::{InspectorTapHub, InspectorTapPacket};
use crate::protocol::{
    AudioChunk, EgressAudioFrame, FailoverSignal, MonotonicClock, ProviderId, RtpOpusPacketizer,
    SampleRate,
};
use crate::telemetry::TelemetryCollector;
use crate::watchdog::LatencyWatchdog;
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

/// Fixed-capacity circular sample buffer holding PCM 16-bit samples for a single provider.
pub struct PcmRingBuffer {
    buf: VecDeque<i16>,
    max_capacity_samples: usize,
}

impl PcmRingBuffer {
    pub fn new(max_capacity_samples: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(max_capacity_samples),
            max_capacity_samples,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn push_samples(&mut self, samples: &[i16]) {
        for &s in samples {
            if self.buf.len() >= self.max_capacity_samples {
                self.buf.pop_front();
            }
            self.buf.push_back(s);
        }
    }

    pub fn pop_exact_or_pad(&mut self, count: usize, last_sample: &mut i16) -> Vec<i16> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(s) = self.buf.pop_front() {
                *last_sample = s;
                out.push(s);
            } else {
                *last_sample = ((*last_sample as i32 * 31) / 32) as i16;
                out.push(*last_sample);
            }
        }
        out
    }

    pub fn peek_slice(&self, count: usize) -> Vec<i16> {
        self.buf.iter().take(count).copied().collect()
    }

    pub fn discard_samples(&mut self, count: usize) {
        let to_drop = count.min(self.buf.len());
        for _ in 0..to_drop {
            self.buf.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

/// Matches the Root-Mean-Square (RMS) acoustic energy of the incoming stream
/// to the active tail, preventing a 3dB-6dB loudness jump during provider or backchannel splices.
/// Returns the applied linear gain factor $G \in [0.4, 2.5]$.
pub fn normalize_standby_rms_gain(primary_tail: &[i16], standby_slice: &mut [i16]) -> f64 {
    if primary_tail.is_empty() || standby_slice.is_empty() {
        return 1.0;
    }

    let p_mean_sq: f64 = primary_tail
        .iter()
        .map(|&s| (s as f64) * (s as f64))
        .sum::<f64>()
        / (primary_tail.len() as f64);

    let s_mean_sq: f64 = standby_slice
        .iter()
        .map(|&s| (s as f64) * (s as f64))
        .sum::<f64>()
        / (standby_slice.len() as f64);

    let p_rms = p_mean_sq.sqrt();
    let s_rms = s_mean_sq.sqrt();

    // Guard against silence in standby or primary (< -60 dBFS)
    if s_rms < 32.0 || p_rms < 32.0 {
        return 1.0;
    }

    // Target linear gain G = RMS_primary / RMS_standby, clamped to [0.4x, 2.5x] (-8dB to +8dB)
    let raw_gain = p_rms / s_rms;
    let clamped_gain = raw_gain.clamp(0.4, 2.5);

    for s in standby_slice.iter_mut() {
        let scaled = (*s as f64) * clamped_gain;
        *s = scaled.round().clamp(-32768.0, 32767.0) as i16;
    }

    clamped_gain
}

/// Precomputed Raised-Cosine (Hann) 5ms crossfade window weights and phase correlator.
pub struct CrossfadeDspEngine {
    fade_samples: usize,
    max_phase_search_samples: usize,
    weights_out: Vec<f64>,
    weights_in: Vec<f64>,
}

impl CrossfadeDspEngine {
    pub fn new(sample_rate: SampleRate) -> Self {
        let fade_samples = sample_rate.samples_for_ms(5);
        let max_phase_search_samples = sample_rate.samples_for_ms(2);

        let mut weights_out = Vec::with_capacity(fade_samples);
        let mut weights_in = Vec::with_capacity(fade_samples);

        let denom = (fade_samples.saturating_sub(1)).max(1) as f64;
        for n in 0..fade_samples {
            let w_out = 0.5 * (1.0 + (PI * (n as f64) / denom).cos());
            let w_in = 1.0 - w_out;
            weights_out.push(w_out);
            weights_in.push(w_in);
        }

        Self {
            fade_samples,
            max_phase_search_samples,
            weights_out,
            weights_in,
        }
    }

    #[inline(always)]
    pub fn fade_samples(&self) -> usize {
        self.fade_samples
    }

    pub fn find_optimal_phase_offset(&self, primary_tail: &[i16], standby_head: &[i16]) -> usize {
        let n_fade = self.fade_samples.min(primary_tail.len());
        if n_fade == 0 || standby_head.len() <= n_fade {
            return 0;
        }

        let max_lag = self
            .max_phase_search_samples
            .min(standby_head.len().saturating_sub(n_fade));

        let mut best_lag = 0usize;
        let mut best_corr = i64::MIN;

        for lag in 0..=max_lag {
            let mut corr: i64 = 0;
            for i in (0..n_fade).step_by(2) {
                let p = primary_tail[i] as i64;
                let s = standby_head[i + lag] as i64;
                corr += p * s;
            }
            if corr > best_corr {
                best_corr = corr;
                best_lag = lag;
            }
        }

        best_lag
    }

    pub fn crossfade_5ms(&self, primary_tail: &[i16], standby_aligned: &[i16]) -> Vec<i16> {
        let len = self
            .fade_samples
            .min(primary_tail.len())
            .min(standby_aligned.len());
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let p = primary_tail[i] as f64;
            let s = standby_aligned[i] as f64;
            let blended = (self.weights_out[i] * p + self.weights_in[i] * s).round();
            let clamped = blended.clamp(i16::MIN as f64, i16::MAX as f64) as i16;
            out.push(clamped);
        }

        out
    }
}

/// Module B: Stateful Splicer & 40ms Jitter Buffer with Backchannel & Binary WS Tap.
pub struct StatefulSplicer {
    sample_rate: SampleRate,
    frame_duration_ms: u32,
    frame_samples: usize,
    holdback_target_samples: usize,
    primary_ring: PcmRingBuffer,
    standby_ring: PcmRingBuffer,
    standby_playhead_debt: usize,
    primary_alignment: WordAlignmentIndex,
    standby_alignment: WordAlignmentIndex,
    primary_samples_egressed: usize,
    dsp: CrossfadeDspEngine,
    packetizer: RtpOpusPacketizer,
    active_provider: ProviderId,
    pending_splice: bool,
    pending_backchannel_to_tts_splice: bool,
    primed: bool,
    last_output_sample: i16,
    pub last_rms_gain: f64,
    next_pts_us: u64,
    clock: MonotonicClock,
    watchdog: Arc<LatencyWatchdog>,
    telemetry: Arc<TelemetryCollector>,
    backchannel: BackchannelController,
    inspector_tap: Option<Arc<InspectorTapHub>>,
}

impl StatefulSplicer {
    pub fn new(
        sample_rate: SampleRate,
        ssrc: u32,
        clock: MonotonicClock,
        watchdog: Arc<LatencyWatchdog>,
        telemetry: Arc<TelemetryCollector>,
    ) -> Self {
        let frame_duration_ms = 10;
        let frame_samples = sample_rate.samples_for_ms(frame_duration_ms);
        let holdback_target_samples = sample_rate.samples_for_ms(40);
        let max_ring_samples = sample_rate.samples_for_ms(500);

        Self {
            sample_rate,
            frame_duration_ms,
            frame_samples,
            holdback_target_samples,
            primary_ring: PcmRingBuffer::new(max_ring_samples),
            standby_ring: PcmRingBuffer::new(max_ring_samples),
            standby_playhead_debt: 0,
            primary_alignment: WordAlignmentIndex::new(sample_rate),
            standby_alignment: WordAlignmentIndex::new(sample_rate),
            primary_samples_egressed: 0,
            dsp: CrossfadeDspEngine::new(sample_rate),
            packetizer: RtpOpusPacketizer::new(ssrc, 111),
            active_provider: ProviderId::PrimaryCartesia,
            pending_splice: false,
            pending_backchannel_to_tts_splice: false,
            primed: false,
            last_output_sample: 0,
            last_rms_gain: 1.0,
            next_pts_us: 0,
            clock,
            watchdog,
            telemetry,
            backchannel: BackchannelController::new(sample_rate),
            inspector_tap: None,
        }
    }

    /// Attaches a real-time binary WebSocket tap hub (`WS /v1/inspector/ws`) to this splicer.
    pub fn with_inspector_tap(mut self, tap: Arc<InspectorTapHub>) -> Self {
        self.inspector_tap = Some(tap);
        self
    }

    pub fn set_inspector_tap(&mut self, tap: Arc<InspectorTapHub>) {
        self.inspector_tap = Some(tap);
    }

    /// Arms the 450ms LLM TTFT backchannel timer at the start of a user-to-agent turn.
    pub fn arm_llm_turn_wait(&mut self, utterance_id: u64, turn_start_us: u64) {
        self.backchannel.arm_turn_wait(
            utterance_id,
            turn_start_us,
            DEFAULT_BACKCHANNEL_THRESHOLD_US,
        );
    }

    /// Arms the LLM TTFT backchannel timer with a custom microsecond threshold.
    pub fn arm_llm_turn_wait_with_threshold(
        &mut self,
        utterance_id: u64,
        turn_start_us: u64,
        threshold_us: u64,
    ) {
        self.backchannel
            .arm_turn_wait(utterance_id, turn_start_us, threshold_us);
    }

    #[inline(always)]
    pub fn is_backchannel_active(&self) -> bool {
        self.backchannel.is_active()
    }

    #[inline(always)]
    pub fn last_rms_gain(&self) -> f64 {
        self.last_rms_gain
    }

    #[inline(always)]
    fn sync_watchdog_buffer_occupancy(&self) {
        let occupancy_us = self.sample_rate.samples_to_us(self.primary_ring.len());
        self.watchdog.set_primary_buffer_occupancy_us(occupancy_us);
    }

    #[inline(always)]
    fn settle_standby_playhead_debt(&mut self) {
        if self.standby_playhead_debt > 0 && !self.standby_ring.is_empty() {
            let drop_count = self.standby_playhead_debt.min(self.standby_ring.len());
            self.standby_ring.discard_samples(drop_count);
            self.standby_playhead_debt -= drop_count;
        }
    }

    /// Evaluates at timestamp `now_us` whether the 450ms LLM TTFT threshold has been breached.
    pub fn evaluate_backchannel_at(&mut self, now_us: u64) -> bool {
        let triggered = self
            .backchannel
            .evaluate_and_maybe_trigger(now_us, self.primary_ring.is_empty());
        if triggered {
            self.primed = true;
            self.telemetry.record_backchannel_injected();
            info!(
                now_us,
                "450ms LLM TTFT stall detected; injecting prosodic backchannel audio"
            );
        }
        triggered
    }

    /// Ingests an upstream PCM chunk into the corresponding provider ring buffer.
    /// If the backchannel filler is currently playing, arms a 5ms Hann crossfade from the
    /// backchannel tail into the newly arrived live TTS stream.
    pub fn ingest_chunk(&mut self, chunk: AudioChunk) {
        let chunk_samples_len = chunk.samples.len();

        match chunk.provider {
            ProviderId::PrimaryCartesia => {
                if let Some(ref al_json) = chunk.alignment_json {
                    self.primary_alignment.ingest_cartesia_timestamps(al_json);
                }
                self.primary_alignment
                    .advance_sample_count(chunk_samples_len);
                self.primary_ring.push_samples(&chunk.samples);

                if self.backchannel.is_active() {
                    self.pending_backchannel_to_tts_splice = true;
                } else if matches!(
                    self.backchannel.phase(),
                    BackchannelPhase::ArmedWaitingForLlm { .. }
                ) {
                    // Real TTS arrived before the 450ms deadline; cancel the backchannel timer
                    self.backchannel.cancel();
                }

                if !self.primed && self.primary_ring.len() >= self.holdback_target_samples {
                    self.primed = true;
                }
                self.sync_watchdog_buffer_occupancy();
            }
            ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => {
                if let Some(ref al_json) = chunk.alignment_json {
                    self.standby_alignment.ingest_elevenlabs_alignment(al_json);
                }
                self.standby_alignment
                    .advance_sample_count(chunk_samples_len);
                self.standby_ring.push_samples(&chunk.samples);
                self.settle_standby_playhead_debt();
                if !self.primed && self.standby_ring.len() >= self.holdback_target_samples {
                    self.primed = true;
                }
            }
        }
    }

    /// Processes a control-plane failover signal from the Latency Watchdog.
    pub fn handle_failover_signal(&mut self, signal: FailoverSignal) {
        match signal {
            FailoverSignal::SwitchToStandby {
                utterance_id,
                trigger_us,
                reason,
            } => {
                if self.active_provider != ProviderId::StandbyElevenLabs {
                    self.pending_splice = true;
                    self.primed = true;
                    info!(
                        utterance_id,
                        trigger_us,
                        %reason,
                        primary_buffered = self.primary_ring.len(),
                        standby_buffered = self.standby_ring.len(),
                        standby_debt = self.standby_playhead_debt,
                        "Splicer armed for 5ms crossfade transition to warm standby"
                    );
                }
            }
            FailoverSignal::RestorePrimary {
                utterance_id,
                trigger_us,
            } => {
                if self.active_provider != ProviderId::PrimaryCartesia {
                    self.active_provider = ProviderId::PrimaryCartesia;
                    self.pending_splice = false;
                    self.standby_playhead_debt = 0;
                    self.primary_samples_egressed = 0;
                    self.primary_alignment.reset();
                    self.standby_alignment.reset();
                    info!(utterance_id, trigger_us, "Restored primary provider stream");
                }
            }
        }
    }

    /// Produces a single 10ms egress frame, executing backchannel emission, backchannel->TTS
    /// 5ms Hann crossfading, or Primary->Standby 5ms Hann crossfading as needed.
    pub fn pull_10ms_egress_frame(&mut self, trigger_us: Option<u64>) -> Option<EgressAudioFrame> {
        let now_us = self.clock.now_us();
        self.evaluate_backchannel_at(now_us);

        if !self.primed {
            return None;
        }

        // Capture peek snapshots for the live oscilloscope tap before popping
        let has_tap = self
            .inspector_tap
            .as_ref()
            .map(|t| t.subscriber_count() > 0)
            .unwrap_or(false);

        let tap_primary_snapshot = if has_tap {
            self.primary_ring.peek_slice(self.frame_samples)
        } else {
            Vec::new()
        };
        let tap_standby_snapshot = if has_tap {
            self.standby_ring.peek_slice(self.frame_samples)
        } else {
            Vec::new()
        };

        let mut spliced_in_this_frame = false;
        let mut backchannel_in_this_frame = false;
        let mut phase_offset_samples = 0usize;
        let mut frame_provider = self.active_provider;
        let pcm_samples: Vec<i16>;

        if self.pending_backchannel_to_tts_splice && self.primary_ring.len() >= self.frame_samples {
            // Seamless 5ms Hann crossfade from active Backchannel into newly arrived Primary TTS
            let fade_len = self.dsp.fade_samples();
            let remainder_len = self.frame_samples.saturating_sub(fade_len);

            let backchannel_tail = self.backchannel.take_tail_and_complete(fade_len);

            let search_window = fade_len + self.sample_rate.samples_for_ms(2);
            let primary_peek = self.primary_ring.peek_slice(search_window);
            let optimal_lag = self
                .dsp
                .find_optimal_phase_offset(&backchannel_tail, &primary_peek);
            phase_offset_samples = optimal_lag;

            if optimal_lag > 0 {
                self.primary_ring.discard_samples(optimal_lag);
            }

            let mut primary_window = self
                .primary_ring
                .pop_exact_or_pad(fade_len + remainder_len, &mut self.last_output_sample);

            self.last_rms_gain = normalize_standby_rms_gain(&backchannel_tail, &mut primary_window);

            let mut crossfaded = self
                .dsp
                .crossfade_5ms(&backchannel_tail, &primary_window[0..fade_len]);
            crossfaded.extend_from_slice(&primary_window[fade_len..]);

            if let Some(&last) = crossfaded.last() {
                self.last_output_sample = last;
            }

            self.pending_backchannel_to_tts_splice = false;
            spliced_in_this_frame = true;
            backchannel_in_this_frame = true;
            frame_provider = ProviderId::PrimaryCartesia;
            self.primary_samples_egressed += self.frame_samples;
            self.sync_watchdog_buffer_occupancy();

            pcm_samples = crossfaded;
        } else if self.backchannel.is_active() {
            // Stream 10ms of prosodic backchannel while still waiting for LLM first TTS chunks
            let out = self.backchannel.pull_samples(self.frame_samples);
            if let Some(&last) = out.last() {
                self.last_output_sample = last;
            }
            backchannel_in_this_frame = true;
            frame_provider = ProviderId::SyntheticComfortNoise;
            pcm_samples = out;
        } else if self.pending_splice {
            // First check if prosodic word-boundary alignment is available
            let search_window = self.dsp.fade_samples() + self.sample_rate.samples_for_ms(2);
            let standby_peek = self.standby_ring.peek_slice(search_window);

            let word_splice_point = self.standby_alignment.find_splice_point(
                self.primary_samples_egressed,
                &self.primary_alignment,
                if standby_peek.is_empty() {
                    None
                } else {
                    Some(&standby_peek)
                },
            );

            if let Some(sp) = word_splice_point {
                // If word alignment found an offset, align standby ring read position
                if sp.standby_sample_offset > 0
                    && sp.standby_sample_offset < self.standby_ring.len()
                {
                    self.standby_ring.discard_samples(sp.standby_sample_offset);
                }
            } else {
                // Fallback: Settle standby playhead debt to synchronize read pointers
                self.settle_standby_playhead_debt();
            }

            let fade_len = self.dsp.fade_samples();
            let remainder_len = self.frame_samples.saturating_sub(fade_len);

            let primary_tail = self
                .primary_ring
                .pop_exact_or_pad(fade_len, &mut self.last_output_sample);

            let standby_peek_search = self
                .standby_ring
                .peek_slice(fade_len + self.sample_rate.samples_for_ms(2));
            let optimal_lag = self
                .dsp
                .find_optimal_phase_offset(&primary_tail, &standby_peek_search);
            phase_offset_samples = optimal_lag;

            if optimal_lag > 0 {
                self.standby_ring.discard_samples(optimal_lag);
            }

            let mut standby_fade_head = self
                .standby_ring
                .pop_exact_or_pad(fade_len, &mut self.last_output_sample);

            let mut standby_remainder = self
                .standby_ring
                .pop_exact_or_pad(remainder_len, &mut self.last_output_sample);

            let mut combined_standby = Vec::with_capacity(fade_len + remainder_len);
            combined_standby.extend_from_slice(&standby_fade_head);
            combined_standby.extend_from_slice(&standby_remainder);
            self.last_rms_gain = normalize_standby_rms_gain(&primary_tail, &mut combined_standby);
            standby_fade_head.copy_from_slice(&combined_standby[..fade_len]);
            standby_remainder.copy_from_slice(&combined_standby[fade_len..]);

            let mut crossfaded = self.dsp.crossfade_5ms(&primary_tail, &standby_fade_head);
            crossfaded.extend_from_slice(&standby_remainder);

            if let Some(&last) = crossfaded.last() {
                self.last_output_sample = last;
            }

            self.active_provider = ProviderId::StandbyElevenLabs;
            frame_provider = ProviderId::StandbyElevenLabs;
            self.pending_splice = false;
            spliced_in_this_frame = true;
            self.primary_ring.clear();
            self.standby_playhead_debt = 0;
            self.primary_samples_egressed += self.frame_samples;
            self.sync_watchdog_buffer_occupancy();

            let splice_done_us = self.clock.now_us();
            if let Some(t_trig) = trigger_us {
                let splice_latency_us = splice_done_us.saturating_sub(t_trig);
                self.telemetry.record_splice_completed(splice_latency_us);
            } else {
                self.telemetry.record_splice_completed(0);
            }

            pcm_samples = crossfaded;
        } else {
            pcm_samples = match self.active_provider {
                ProviderId::PrimaryCartesia => {
                    let had_real_primary = !self.primary_ring.is_empty();
                    let out = self
                        .primary_ring
                        .pop_exact_or_pad(self.frame_samples, &mut self.last_output_sample);
                    self.primary_samples_egressed += self.frame_samples;

                    // Keep standby_ring playhead temporally synchronized with primary_ring
                    // while preserving a 2ms lookbehind window for phase-alignment search.
                    let lookbehind = self.sample_rate.samples_for_ms(2);
                    if had_real_primary && self.standby_ring.len() > self.frame_samples + lookbehind
                    {
                        self.standby_ring.discard_samples(self.frame_samples);
                    }
                    self.sync_watchdog_buffer_occupancy();
                    out
                }
                ProviderId::StandbyElevenLabs | ProviderId::SyntheticComfortNoise => {
                    let out = self
                        .standby_ring
                        .pop_exact_or_pad(self.frame_samples, &mut self.last_output_sample);
                    self.primary_samples_egressed += self.frame_samples;
                    out
                }
            };
        }

        let pts_us = self.next_pts_us;
        self.next_pts_us += (self.frame_duration_ms as u64) * 1_000;

        let (rtp_sequence, rtp_timestamp, rtp_packet) =
            self.packetizer
                .packetize(&pcm_samples, self.sample_rate, spliced_in_this_frame);

        self.telemetry.increment_egress_frames();

        if let Some(ref tap) = self.inspector_tap {
            if tap.subscriber_count() > 0 {
                let standby_or_bc = if backchannel_in_this_frame {
                    pcm_samples.clone()
                } else {
                    tap_standby_snapshot
                };
                tap.publish_packet(&InspectorTapPacket {
                    spliced_in_this_frame,
                    backchannel_active: backchannel_in_this_frame,
                    primary_stalled: self.active_provider == ProviderId::StandbyElevenLabs,
                    active_provider: frame_provider,
                    rtp_sequence,
                    sample_count: self.frame_samples as u16,
                    pts_us,
                    primary_pcm: tap_primary_snapshot,
                    standby_pcm: standby_or_bc,
                    egress_pcm: pcm_samples.clone(),
                });
            }
        }

        Some(EgressAudioFrame {
            rtp_sequence,
            rtp_timestamp,
            pts_us,
            sample_rate: self.sample_rate,
            active_provider: frame_provider,
            spliced_in_this_frame,
            phase_offset_samples,
            pcm_samples,
            rtp_packet,
        })
    }

    pub fn spawn_realtime_loop(
        mut self,
        mut chunk_rx: mpsc::Receiver<AudioChunk>,
        mut failover_rx: broadcast::Receiver<FailoverSignal>,
        egress_tx: mpsc::Sender<EgressAudioFrame>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                self.frame_duration_ms as u64,
            ));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

            let mut last_trigger_us: Option<u64> = None;

            loop {
                tokio::select! {
                    biased;

                    Ok(signal) = failover_rx.recv() => {
                        if let FailoverSignal::SwitchToStandby { trigger_us, .. } = signal {
                            last_trigger_us = Some(trigger_us);
                        }
                        self.handle_failover_signal(signal);
                    }

                    Some(chunk) = chunk_rx.recv() => {
                        self.ingest_chunk(chunk);
                        while let Ok(extra_chunk) = chunk_rx.try_recv() {
                            self.ingest_chunk(extra_chunk);
                        }
                    }

                    _ = ticker.tick() => {
                        if let Some(frame) = self.pull_10ms_egress_frame(last_trigger_us.take()) {
                            if egress_tx.send(frame).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        })
    }
}
