//! src/inspector_tap.rs
//! Upgrade #1: Lock-Free Binary WebSocket PCM Tap (`WS /v1/inspector/ws`).
//! Broadcasts real-time 10ms 3-channel PCM snapshots (Primary, Standby/Backchannel,
//! and Spliced Egress) directly from `StatefulSplicer` to the browser oscilloscope.

use crate::protocol::ProviderId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::broadcast;

pub const TAP_MAGIC_BYTE: u8 = 0xCA;
pub const TAP_FLAG_SPLICED: u8 = 0b0000_0001;
pub const TAP_FLAG_BACKCHANNEL: u8 = 0b0000_0010;
pub const TAP_FLAG_PRIMARY_STALLED: u8 = 0b0000_0100;

/// Decoded representation of a single 10ms 3-trace oscilloscope frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectorTapPacket {
    pub spliced_in_this_frame: bool,
    pub backchannel_active: bool,
    pub primary_stalled: bool,
    pub active_provider: ProviderId,
    pub rtp_sequence: u16,
    pub sample_count: u16,
    pub pts_us: u64,
    pub primary_pcm: Vec<i16>,
    pub standby_pcm: Vec<i16>,
    pub egress_pcm: Vec<i16>,
}

impl InspectorTapPacket {
    /// Serializes the 3-channel 10ms PCM snapshot into a compact little-endian binary frame.
    /// For 240 samples (10ms @ 24kHz), total payload is `16 + (240 * 2 * 3) = 1,456 bytes`.
    pub fn encode_binary(&self) -> Bytes {
        let n = self.sample_count as usize;
        let mut buf = BytesMut::with_capacity(16 + n * 6);

        let mut flags = 0u8;
        if self.spliced_in_this_frame {
            flags |= TAP_FLAG_SPLICED;
        }
        if self.backchannel_active {
            flags |= TAP_FLAG_BACKCHANNEL;
        }
        if self.primary_stalled {
            flags |= TAP_FLAG_PRIMARY_STALLED;
        }

        buf.put_u8(TAP_MAGIC_BYTE);
        buf.put_u8(flags);
        buf.put_u8(self.active_provider as u8);
        buf.put_u8(0); // Reserved alignment byte
        buf.put_u16_le(self.rtp_sequence);
        buf.put_u16_le(self.sample_count);
        buf.put_u64_le(self.pts_us);

        for i in 0..n {
            buf.put_i16_le(*self.primary_pcm.get(i).unwrap_or(&0));
        }
        for i in 0..n {
            buf.put_i16_le(*self.standby_pcm.get(i).unwrap_or(&0));
        }
        for i in 0..n {
            buf.put_i16_le(*self.egress_pcm.get(i).unwrap_or(&0));
        }

        buf.freeze()
    }

    /// Decodes a binary tap packet for hermetic test verification.
    pub fn decode_binary(mut raw: Bytes) -> Option<Self> {
        if raw.len() < 16 {
            return None;
        }
        let magic = raw.get_u8();
        if magic != TAP_MAGIC_BYTE {
            return None;
        }
        let flags = raw.get_u8();
        let provider_byte = raw.get_u8();
        let _reserved = raw.get_u8();
        let rtp_sequence = raw.get_u16_le();
        let sample_count = raw.get_u16_le();
        let pts_us = raw.get_u64_le();

        let n = sample_count as usize;
        if raw.remaining() < n * 6 {
            return None;
        }

        let mut primary_pcm = Vec::with_capacity(n);
        for _ in 0..n {
            primary_pcm.push(raw.get_i16_le());
        }
        let mut standby_pcm = Vec::with_capacity(n);
        for _ in 0..n {
            standby_pcm.push(raw.get_i16_le());
        }
        let mut egress_pcm = Vec::with_capacity(n);
        for _ in 0..n {
            egress_pcm.push(raw.get_i16_le());
        }

        let active_provider = match provider_byte {
            0 => ProviderId::PrimaryCartesia,
            1 => ProviderId::StandbyElevenLabs,
            _ => ProviderId::SyntheticComfortNoise,
        };

        Some(Self {
            spliced_in_this_frame: (flags & TAP_FLAG_SPLICED) != 0,
            backchannel_active: (flags & TAP_FLAG_BACKCHANNEL) != 0,
            primary_stalled: (flags & TAP_FLAG_PRIMARY_STALLED) != 0,
            active_provider,
            rtp_sequence,
            sample_count,
            pts_us,
            primary_pcm,
            standby_pcm,
            egress_pcm,
        })
    }
}

/// Wait-free broadcast hub for streaming real-time binary PCM tap frames to `/v1/inspector/ws`.
pub struct InspectorTapHub {
    tx: broadcast::Sender<Bytes>,
}

impl Default for InspectorTapHub {
    fn default() -> Self {
        Self::new()
    }
}

impl InspectorTapHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(512);
        Self { tx }
    }

    #[inline(always)]
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.tx.subscribe()
    }

    /// Publishes a 3-channel PCM frame only if at least one oscilloscope client or test is subscribed.
    #[inline(always)]
    pub fn publish_packet(&self, packet: &InspectorTapPacket) {
        if self.tx.receiver_count() > 0 {
            let _ = self.tx.send(packet.encode_binary());
        }
    }
}
