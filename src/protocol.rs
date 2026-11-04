//! src/protocol.rs
//! Core audio primitives, microsecond timestamp arithmetic, and RFC 3550 RTP / RFC 6716 Opus framing.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CadenceError {
    #[error("Transport socket closed or failed: {0}")]
    TransportError(String),
    #[error("Channel backpressure overflow or closed: {0}")]
    ChannelClosed(String),
    #[error("Sample rate mismatch: expected {expected}Hz, got {actual}Hz")]
    SampleRateMismatch { expected: u32, actual: u32 },
    #[error("Invalid audio frame configuration: {0}")]
    InvalidFrame(String),
    #[error("Serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ProviderId {
    PrimaryCartesia = 0,
    StandbyElevenLabs = 1,
    SyntheticComfortNoise = 2,
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderId::PrimaryCartesia => write!(f, "cartesia_primary"),
            ProviderId::StandbyElevenLabs => write!(f, "elevenlabs_standby"),
            ProviderId::SyntheticComfortNoise => write!(f, "comfort_noise_plc"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleRate {
    Hz24000 = 24000,
    Hz48000 = 48000,
}

impl SampleRate {
    #[inline(always)]
    pub const fn as_u32(self) -> u32 {
        self as u32
    }

    #[inline(always)]
    pub const fn samples_for_ms(self, ms: u32) -> usize {
        ((self as u32 * ms) / 1000) as usize
    }

    #[inline(always)]
    pub const fn samples_to_us(self, samples: usize) -> u64 {
        ((samples as u64) * 1_000_000) / (self as u64)
    }
}

/// Epoch-anchored monotonic clock for microsecond timestamp generation.
#[derive(Debug, Clone)]
pub struct MonotonicClock {
    epoch: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    #[inline(always)]
    pub fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    #[inline(always)]
    pub fn wall_clock_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    }
}

/// Raw 16-bit signed little-endian mono PCM audio chunk with microsecond telemetry metadata.
#[derive(Debug, Clone)]
pub struct AudioChunk {
    pub utterance_id: u64,
    pub sequence: u64,
    pub provider: ProviderId,
    pub sample_rate: SampleRate,
    pub samples: Arc<[i16]>,
    pub arrival_us: u64,
    pub is_first_chunk: bool,
    pub is_final_chunk: bool,
    /// Optional word/character alignment metadata from the TTS provider's JSON frame.
    pub alignment_json: Option<Arc<serde_json::Value>>,
}

impl AudioChunk {
    #[inline(always)]
    pub fn duration_us(&self) -> u64 {
        self.sample_rate.samples_to_us(self.samples.len())
    }
}

/// Outbound WebRTC egress frame containing contiguous PCM samples and RFC 3550 RTP/Opus packet.
#[derive(Debug, Clone)]
pub struct EgressAudioFrame {
    pub rtp_sequence: u16,
    pub rtp_timestamp: u32,
    pub pts_us: u64,
    pub sample_rate: SampleRate,
    pub active_provider: ProviderId,
    pub spliced_in_this_frame: bool,
    pub barge_in_faded: bool,
    pub phase_offset_samples: usize,
    pub pcm_samples: Vec<i16>,
    pub rtp_packet: Bytes,
}

/// Request dispatched from the LLM orchestrator to synthesize a text segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynthesisRequest {
    pub session_id: String,
    pub utterance_id: u64,
    pub text_segment: String,
    pub voice_id_primary: String,
    pub voice_id_standby: String,
    pub dispatch_us: u64,
    pub flush: bool,
}

/// Control-plane command emitted by the Watchdog to the Multiplexer and Splicer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverSignal {
    SwitchToStandby {
        utterance_id: u64,
        trigger_us: u64,
        reason: FailoverReason,
    },
    RestorePrimary {
        utterance_id: u64,
        trigger_us: u64,
    },
    CancelUtterance {
        utterance_id: u64,
        cancel_us: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailoverReason {
    TtfbExceeded120ms,
    InterChunkSpikeExceeded150ms,
    PredictiveJitterUnderrunRisk,
    PrimarySocketClosed,
}

impl fmt::Display for FailoverReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailoverReason::TtfbExceeded120ms => write!(f, "ttfb_exceeded_120ms"),
            FailoverReason::InterChunkSpikeExceeded150ms => write!(f, "inter_chunk_spike_150ms"),
            FailoverReason::PredictiveJitterUnderrunRisk => write!(f, "predictive_jitter_underrun"),
            FailoverReason::PrimarySocketClosed => write!(f, "primary_socket_closed"),
        }
    }
}

/// Dual egress mode configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressMode {
    /// Raw PCM s16le frames — for local WebSocket proxy to LiveKit Agents / Pipecat.
    RawPcmProxy,
    /// Compressed RFC 6716 Opus frames inside RFC 3550 RTP packets.
    /// Only available when compiled with `--features opus-encoding`.
    NativeOpusRtp,
}

/// Deterministic RFC 3550 RTP + RFC 6716 Opus frame packetizer for WebRTC egress.
/// Maintains a monotonic 48kHz RTP clock advance per RFC 7587 regardless of whether
/// the upstream synthesis stream is 24kHz or 48kHz.
pub struct RtpOpusPacketizer {
    ssrc: u32,
    sequence_number: u16,
    rtp_timestamp: u32,
    payload_type: u8,
    egress_mode: EgressMode,
    #[cfg(feature = "opus-encoding")]
    opus_encoder: Option<OpusFrameEncoder>,
}

/// Opus frame encoder using direct libopus FFI via pkg-config linkage.
/// Only compiled when `--features opus-encoding` is active.
#[cfg(feature = "opus-encoding")]
pub struct OpusFrameEncoder {
    encoder_ptr: *mut u8,
    encode_buffer: Vec<u8>,
}

#[cfg(feature = "opus-encoding")]
unsafe impl Send for OpusFrameEncoder {}
#[cfg(feature = "opus-encoding")]
unsafe impl Sync for OpusFrameEncoder {}

#[cfg(feature = "opus-encoding")]
#[link(name = "opus")]
extern "C" {
    fn opus_encoder_create(fs: i32, channels: i32, application: i32, error: *mut i32) -> *mut u8;
    fn opus_encode(
        st: *mut u8,
        pcm: *const i16,
        frame_size: i32,
        data: *mut u8,
        max_data_bytes: i32,
    ) -> i32;
    fn opus_encoder_destroy(st: *mut u8);
}

#[cfg(feature = "opus-encoding")]
impl OpusFrameEncoder {
    pub fn new() -> Result<Self, CadenceError> {
        let mut error: i32 = 0;
        let encoder_ptr = unsafe {
            opus_encoder_create(
                48000, // 48kHz
                1,     // mono
                2048,  // OPUS_APPLICATION_VOIP
                &mut error,
            )
        };
        if encoder_ptr.is_null() || error != 0 {
            return Err(CadenceError::InvalidFrame(format!(
                "opus_encoder_create failed with error code {error}"
            )));
        }
        Ok(Self {
            encoder_ptr,
            encode_buffer: vec![0u8; 4000],
        })
    }

    pub fn encode_frame(&mut self, pcm_48k: &[i16]) -> Result<&[u8], CadenceError> {
        let frame_size = pcm_48k.len() as i32;
        let encoded_len = unsafe {
            opus_encode(
                self.encoder_ptr,
                pcm_48k.as_ptr(),
                frame_size,
                self.encode_buffer.as_mut_ptr(),
                self.encode_buffer.len() as i32,
            )
        };
        if encoded_len < 0 {
            return Err(CadenceError::InvalidFrame(format!(
                "opus_encode returned error {encoded_len}"
            )));
        }
        Ok(&self.encode_buffer[..encoded_len as usize])
    }
}

#[cfg(feature = "opus-encoding")]
impl Drop for OpusFrameEncoder {
    fn drop(&mut self) {
        if !self.encoder_ptr.is_null() {
            unsafe {
                opus_encoder_destroy(self.encoder_ptr);
            }
        }
    }
}

impl RtpOpusPacketizer {
    pub fn new(ssrc: u32, payload_type: u8) -> Self {
        Self {
            ssrc,
            sequence_number: 1000,
            rtp_timestamp: 48_000,
            payload_type,
            egress_mode: EgressMode::RawPcmProxy,
            #[cfg(feature = "opus-encoding")]
            opus_encoder: None,
        }
    }

    pub fn with_egress_mode(mut self, mode: EgressMode) -> Self {
        self.egress_mode = mode;
        #[cfg(feature = "opus-encoding")]
        if mode == EgressMode::NativeOpusRtp {
            self.opus_encoder = OpusFrameEncoder::new().ok();
        }
        self
    }

    /// Upsamples 24kHz mono PCM to 48kHz mono PCM using linear midpoint interpolation.
    pub fn ensure_48khz(samples: &[i16], rate: SampleRate) -> Vec<i16> {
        match rate {
            SampleRate::Hz48000 => samples.to_vec(),
            SampleRate::Hz24000 => {
                let mut out = Vec::with_capacity(samples.len() * 2);
                for i in 0..samples.len() {
                    let current = samples[i];
                    let next = if i + 1 < samples.len() {
                        samples[i + 1]
                    } else {
                        current
                    };
                    let interp = ((current as i32 + next as i32) / 2) as i16;
                    out.push(current);
                    out.push(interp);
                }
                out
            }
        }
    }

    /// Encapsulates a 10ms or 20ms PCM slice into an RFC 3550 RTP packet.
    /// In RawPcmProxy mode: Opus TOC header + raw PCM payload.
    /// In NativeOpusRtp mode (with opus-encoding feature): real libopus compressed payload.
    pub fn packetize(
        &mut self,
        pcm_samples: &[i16],
        sample_rate: SampleRate,
        marker: bool,
    ) -> (u16, u32, Bytes) {
        let samples_48k = Self::ensure_48khz(pcm_samples, sample_rate);
        let rtp_samples_advance = samples_48k.len() as u32;

        let seq = self.sequence_number;
        let ts = self.rtp_timestamp;

        self.sequence_number = self.sequence_number.wrapping_add(1);
        self.rtp_timestamp = self.rtp_timestamp.wrapping_add(rtp_samples_advance);

        // Attempt real Opus encoding if feature is enabled and encoder is available
        #[cfg(feature = "opus-encoding")]
        if self.egress_mode == EgressMode::NativeOpusRtp {
            if let Some(ref mut encoder) = self.opus_encoder {
                if let Ok(opus_payload) = encoder.encode_frame(&samples_48k) {
                    let mut buf = BytesMut::with_capacity(12 + opus_payload.len());
                    buf.put_u8(0x80);
                    let m_pt = if marker {
                        0x80 | (self.payload_type & 0x7F)
                    } else {
                        self.payload_type & 0x7F
                    };
                    buf.put_u8(m_pt);
                    buf.put_u16(seq);
                    buf.put_u32(ts);
                    buf.put_u32(self.ssrc);
                    buf.put_slice(opus_payload);
                    return (seq, ts, buf.freeze());
                }
            }
        }

        // Fallback: RawPcmProxy mode — 12-byte RTP header + 1-byte Opus TOC + raw PCM
        let mut buf = BytesMut::with_capacity(12 + 1 + samples_48k.len() * 2);

        // Byte 0: V=2 (0b10), P=0, X=0, CC=0 -> 0x80
        buf.put_u8(0x80);
        // Byte 1: M (Marker bit) | Payload Type (e.g., 111 for Opus)
        let m_pt = if marker {
            0x80 | (self.payload_type & 0x7F)
        } else {
            self.payload_type & 0x7F
        };
        buf.put_u8(m_pt);
        buf.put_u16(seq);
        buf.put_u32(ts);
        buf.put_u32(self.ssrc);

        // Opus TOC byte: Config=28 (Fullband 48kHz, 10ms), Stereo=0, Code=0 -> 0xE0
        buf.put_u8(0xE0);
        for &sample in &samples_48k {
            buf.put_i16_le(sample);
        }

        (seq, ts, buf.freeze())
    }
}

/// Network impairment profile for glass-to-glass benchmarking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkProfile {
    /// Pristine Fiber / LAN (RTT ~8ms, Jitter ~1.2ms, 0% stall rate)
    FiberLan,
    /// Congested Wi-Fi (RTT ~38ms, Jitter ~18ms, 6% spike probability)
    CongestedWifi,
    /// 4G Cellular Handover (RTT ~75ms, Jitter ~35ms, 18% 220ms+ stall probability)
    Cellular4gHandover,
}

impl fmt::Display for NetworkProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkProfile::FiberLan => write!(f, "fiber_lan"),
            NetworkProfile::CongestedWifi => write!(f, "congested_wifi"),
            NetworkProfile::Cellular4gHandover => write!(f, "cellular_4g_handover"),
        }
    }
}

/// Client audio output hardware profile (T8 - T7 physical playout latency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HardwareOutputProfile {
    /// Wired headphones or built-in CoreAudio/WASAPI DAC (~12ms buffer)
    WiredOrBuiltinDac,
    /// Bluetooth LE Audio / LC3 low-latency profile (~42ms buffer)
    BluetoothLowLatency,
    /// Standard Bluetooth AAC / SBC codec buffer (~145ms buffer)
    BluetoothAacSbc,
}

impl fmt::Display for HardwareOutputProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HardwareOutputProfile::WiredOrBuiltinDac => write!(f, "wired_dac_12ms"),
            HardwareOutputProfile::BluetoothLowLatency => write!(f, "bt_le_audio_42ms"),
            HardwareOutputProfile::BluetoothAacSbc => write!(f, "bt_aac_sbc_145ms"),
        }
    }
}

/// 9-Boundary Glass-to-Glass Conversational Turn Telemetry Span (T0 -> T8).
/// Client timestamps (T0, T1, T2, T6, T7, T8) are recorded in client-monotonic microseconds.
/// Server timestamps (T3, T4, T5) are recorded in server-monotonic microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnLifecycleSpan {
    pub turn_id: u64,
    pub network_profile: NetworkProfile,
    pub hardware_profile: HardwareOutputProfile,
    /// T0 (Client): Last voiced audio frame captured by microphone
    pub t0_mic_last_voiced_us: u64,
    /// T1 (Client): VAD fires endpointing decision (speech_final = true)
    pub t1_vad_endpoint_us: u64,
    /// T2 (Client): Audio tokens egress via WebRTC DataChannel / WebSocket
    pub t2_client_egress_us: u64,
    /// T3 (Server): Server receives final user turn packet
    pub t3_server_ingress_us: u64,
    /// T4 (Server): Model generates first linguistic token (TTFT)
    pub t4_llm_first_token_us: u64,
    /// T5 (Server): Neural vocoder + Cadence Splicer emits first egress audio packet
    pub t5_tts_first_audio_egress_us: u64,
    /// T6 (Client): Client receives first audio packet from network
    pub t6_client_audio_ingress_us: u64,
    /// T7 (Client): AudioWorklet schedules 128-sample playback quantum (2.67ms budget)
    pub t7_worklet_scheduled_us: u64,
    /// T8 (Client): Audio wave physically exits speaker DAC (CoreAudio/WASAPI + Bluetooth)
    pub t8_dac_acoustic_out_us: u64,
    /// Whether Cadence hot-spliced to the warm standby provider during this turn
    pub failover_spliced: bool,
}

impl TurnLifecycleSpan {
    /// T1 - T0: End-of-turn VAD silence detection overhead (Client clock domain).
    #[inline(always)]
    pub const fn vad_endpointing_us(&self) -> u64 {
        self.t1_vad_endpoint_us
            .saturating_sub(self.t0_mic_last_voiced_us)
    }

    /// T2 - T1: Client WebRTC DataChannel / WebSocket framing delay (Client clock domain).
    #[inline(always)]
    pub const fn client_framing_us(&self) -> u64 {
        self.t2_client_egress_us
            .saturating_sub(self.t1_vad_endpoint_us)
    }

    /// T4 - T3: LLM Time-to-First-Token (Server clock domain).
    #[inline(always)]
    pub const fn llm_ttft_us(&self) -> u64 {
        self.t4_llm_first_token_us
            .saturating_sub(self.t3_server_ingress_us)
    }

    /// T5 - T4: Upstream TTS vocoder TTFB + Cadence 40ms splicer holdback (Server clock domain).
    #[inline(always)]
    pub const fn vocoder_and_splice_us(&self) -> u64 {
        self.t5_tts_first_audio_egress_us
            .saturating_sub(self.t4_llm_first_token_us)
    }

    /// Clock-skew-invariant two-way network transit delay: (T6 - T2) - (T5 - T3).
    #[inline(always)]
    pub const fn network_transit_us(&self) -> u64 {
        let client_window = self
            .t6_client_audio_ingress_us
            .saturating_sub(self.t2_client_egress_us);
        let server_window = self
            .t5_tts_first_audio_egress_us
            .saturating_sub(self.t3_server_ingress_us);
        client_window.saturating_sub(server_window)
    }

    /// T7 - T6: Client jitter buffer + 2.67ms AudioWorklet quantum scheduling (Client clock domain).
    #[inline(always)]
    pub const fn worklet_scheduling_us(&self) -> u64 {
        self.t7_worklet_scheduled_us
            .saturating_sub(self.t6_client_audio_ingress_us)
    }

    /// T8 - T7: OS audio driver (CoreAudio/WASAPI) + physical Bluetooth codec buffer (Client clock domain).
    #[inline(always)]
    pub const fn hardware_dac_us(&self) -> u64 {
        self.t8_dac_acoustic_out_us
            .saturating_sub(self.t7_worklet_scheduled_us)
    }

    /// T7 - T0: Total software-controllable glass-to-worklet latency (excludes physical Bluetooth/DAC).
    #[inline(always)]
    pub const fn software_pipeline_us(&self) -> u64 {
        self.t7_worklet_scheduled_us
            .saturating_sub(self.t0_mic_last_voiced_us)
    }

    /// T8 - T0: True end-to-end physical mouth-to-ear latency (Client clock domain).
    #[inline(always)]
    pub const fn total_mouth_to_ear_us(&self) -> u64 {
        self.t8_dac_acoustic_out_us
            .saturating_sub(self.t0_mic_last_voiced_us)
    }
}

/// Inbound WebSocket message envelope supporting both synthesis requests and sub-5ms barge-in cancellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientControlMessage {
    Synthesize(SynthesisRequest),
    CancelUtterance {
        session_id: String,
        utterance_id: u64,
        #[serde(default)]
        client_timestamp_us: u64,
    },
}

/// Outbound JSON control-plane acknowledgment emitted by the server on barge-in completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BargeInAckEvent {
    pub event: &'static str,
    pub session_id: String,
    pub utterance_id: u64,
    pub barge_in_latency_us: u64,
}
