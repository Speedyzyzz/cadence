//! src/backchannel.rs
//! Upgrade #2: Acoustic Backchannel Injector (LLM TTFT Stall Masking).
//! Monitors elapsed turn time while awaiting LLM Time-to-First-Token (TTFT).
//! If no TTS chunk arrives within 450ms (450,000us), streams a pre-cached,
//! voice-matched prosodic backchannel ("Hmm..." / natural breath intake) and
//! triggers a 5ms phase-aligned Hann crossfade into the real TTS stream when it arrives.

use crate::protocol::SampleRate;
use std::f64::consts::PI;
use std::sync::Arc;

/// Default LLM TTFT stall threshold before injecting a prosodic backchannel (450,000us = 450ms).
pub const DEFAULT_BACKCHANNEL_THRESHOLD_US: u64 = 450_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackchannelPreset {
    SubtleBreathAndHmm,
    WarmThinkingHmm,
}

/// Pre-rendered mono 16-bit PCM backchannel audio clip stored in lock-free shared memory.
#[derive(Debug, Clone)]
pub struct BackchannelClip {
    pub preset: BackchannelPreset,
    pub sample_rate: SampleRate,
    pub samples: Arc<[i16]>,
}

impl BackchannelClip {
    /// Synthesizes a natural 650ms prosodic backchannel ("inhale breath + warm closed-mouth 'Hmm...'")
    /// using a 3-formant vocal tract envelope ($F_0 = 135\text{ Hz}$, $F_1 = 280\text{ Hz}$, $F_2 = 920\text{ Hz}$)
    /// with smooth raised-cosine attack and decay so it sounds natural and never clicks.
    pub fn generate_default(sample_rate: SampleRate) -> Self {
        let total_samples = sample_rate.samples_for_ms(650);
        let breath_samples = sample_rate.samples_for_ms(140);
        let pause_samples = sample_rate.samples_for_ms(40);
        let sr = sample_rate.as_u32() as f64;

        let mut pcm = Vec::with_capacity(total_samples);
        let mut lfsr: u32 = 0xACE1_u32;

        for n in 0..total_samples {
            let t = (n as f64) / sr;

            if n < breath_samples {
                // Phase 1 (0..140ms): Soft band-limited aspiration breath intake
                let env = (PI * (n as f64) / (breath_samples as f64)).sin();
                let bit = ((lfsr >> 0) ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5)) & 1;
                lfsr = (lfsr >> 1) | (bit << 15);
                let noise = ((lfsr as i32) - 32768) as f64 / 32768.0;
                let formant_filter = (2.0 * PI * 520.0 * t).sin();
                let val = (1_400.0 * env * noise * formant_filter).round() as i16;
                pcm.push(val);
            } else if n < breath_samples + pause_samples {
                // Phase 2 (140..180ms): Brief glottal closure before vocalization
                pcm.push(0);
            } else {
                // Phase 3 (180..650ms): Warm nasalized "Hmm..." with slight pitch inflection
                let v_idx = n - (breath_samples + pause_samples);
                let v_len = total_samples - (breath_samples + pause_samples);
                let progress = (v_idx as f64) / (v_len as f64);

                // Smooth Hann attack/release envelope
                let env = (PI * progress).sin().powf(0.75);
                // Subtle prosodic pitch contour (135Hz -> 148Hz -> 132Hz)
                let f0 = 135.0 + 13.0 * (PI * progress).sin();
                let f1 = 280.0;
                let f2 = 920.0;

                let wave = 0.65 * (2.0 * PI * f0 * t).sin()
                    + 0.25 * (2.0 * PI * f1 * t).sin()
                    + 0.10 * (2.0 * PI * f2 * t).sin();

                let sample = (6_800.0 * env * wave).round() as i16;
                pcm.push(sample);
            }
        }

        Self {
            preset: BackchannelPreset::SubtleBreathAndHmm,
            sample_rate,
            samples: Arc::from(pcm),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackchannelPhase {
    Idle,
    ArmedWaitingForLlm {
        utterance_id: u64,
        turn_start_us: u64,
        threshold_us: u64,
    },
    ActiveStreaming {
        utterance_id: u64,
        cursor_samples: usize,
    },
}

/// Stateful per-session controller managing 450ms LLM stall detection and backchannel sample emission.
pub struct BackchannelController {
    clip: BackchannelClip,
    phase: BackchannelPhase,
    injections_count: u64,
}

impl BackchannelController {
    pub fn new(sample_rate: SampleRate) -> Self {
        Self {
            clip: BackchannelClip::generate_default(sample_rate),
            phase: BackchannelPhase::Idle,
            injections_count: 0,
        }
    }

    #[inline(always)]
    pub fn phase(&self) -> BackchannelPhase {
        self.phase
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self.phase, BackchannelPhase::ActiveStreaming { .. })
    }

    #[inline(always)]
    pub fn injections_count(&self) -> u64 {
        self.injections_count
    }

    /// Arms the backchannel watchdog when a conversational turn starts (awaiting LLM first token + TTS).
    pub fn arm_turn_wait(&mut self, utterance_id: u64, turn_start_us: u64, threshold_us: u64) {
        self.phase = BackchannelPhase::ArmedWaitingForLlm {
            utterance_id,
            turn_start_us,
            threshold_us,
        };
    }

    /// Evaluates whether the LLM TTFT threshold (e.g. 450ms) has been breached with zero TTS audio.
    /// Returns `true` on the exact tick when backchannel injection transitions to `ActiveStreaming`.
    pub fn evaluate_and_maybe_trigger(&mut self, now_us: u64, primary_ring_empty: bool) -> bool {
        if let BackchannelPhase::ArmedWaitingForLlm {
            utterance_id,
            turn_start_us,
            threshold_us,
        } = self.phase
        {
            if primary_ring_empty && now_us.saturating_sub(turn_start_us) >= threshold_us {
                self.phase = BackchannelPhase::ActiveStreaming {
                    utterance_id,
                    cursor_samples: 0,
                };
                self.injections_count += 1;
                return true;
            }
        }
        false
    }

    /// Pulls `count` PCM samples from the active backchannel clip.
    pub fn pull_samples(&mut self, count: usize) -> Vec<i16> {
        match &mut self.phase {
            BackchannelPhase::ActiveStreaming { cursor_samples, .. } => {
                let mut out = Vec::with_capacity(count);
                let total = self.clip.samples.len();
                for _ in 0..count {
                    if *cursor_samples < total {
                        out.push(self.clip.samples[*cursor_samples]);
                        *cursor_samples += 1;
                    } else {
                        out.push(0);
                    }
                }
                out
            }
            _ => vec![0; count],
        }
    }

    /// Extracts a `fade_len` (5ms) tail from the currently playing backchannel clip and resets to `Idle`
    /// so `StatefulSplicer` can execute a phase-aligned 5ms Hann crossfade into the newly arrived live TTS audio.
    pub fn take_tail_and_complete(&mut self, fade_len: usize) -> Vec<i16> {
        let tail = self.pull_samples(fade_len);
        self.phase = BackchannelPhase::Idle;
        tail
    }

    pub fn cancel(&mut self) {
        self.phase = BackchannelPhase::Idle;
    }
}
