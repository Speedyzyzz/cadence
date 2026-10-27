# Cadence

[![Cadence CI](https://github.com/cadence-audio/cadence/actions/workflows/ci.yml/badge.svg)](https://github.com/cadence-audio/cadence/actions)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

**Open-Core Zero-Downtime Failover Proxy & DSP Splicing Plane for Real-Time Voice AI Agents**

Cadence is an ultra-low-latency media and control-plane proxy placed between client WebRTC SFUs (LiveKit, Agora, Daily) and upstream streaming Text-to-Speech (TTS) providers (Cartesia Sonic, ElevenLabs Flash v2.5, Deepgram Aura).

When a primary TTS provider suffers a Time-to-First-Byte (TTFB) stall exceeding **120ms**, a mid-utterance inter-chunk arrival gap exceeding **150ms**, a predictive jitter buffer underrun, or a dropped WebSocket, Cadence hot-splices the audio stream to a warm standby provider in **under 15ms** with **zero dropped frames** and **$C^1$ waveform continuity** (no clicks, no pops, and no phase cancellation).

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Client ["Client Voice AI Orchestrator"]
        Agent["Voice AI Agent\n(LiveKit Agents / Pipecat / Retell / Vapi)"]
        Stream["LLM Text Clause Stream\n(SynthesisRequest)"]
        Agent --> Stream
    end

    subgraph Cadence ["Cadence Real-Time Voice Proxy"]
        Ingress["Axum WebSocket Server\n(:8080/v1/tts/stream)"]
        Mux["Module A: Speculative Multiplexer\n& Warm Standby Pool"]
        
        Primary["Cartesia Sonic WS\n(Active PCM Stream)"]
        Standby["ElevenLabs Flash WS\n(5s Heartbeat + Context Warm)"]
        
        Watchdog["Module C: 1ms Latency Watchdog\n- TTFB > 120ms\n- Spike > 150ms\n- J_i + Gap > 150ms & B <= 15ms"]
        
        Ring["40ms Pre-Allocated Ring Buffer\n(960 samples @ 24kHz)"]
        WordAlign["WordAlignmentIndex\n(Prosodic Word-Boundary Sync)"]
        Hann["Module B: 5ms Raised-Cosine Splicer\n(SIMD Cross-Correlation Phase Align)"]
        Packetizer["RTP / Opus Framing\n(Constant SSRC, Monotonic PTS)"]
        Telemetry["Module D: Lock-Free Prometheus Telemetry\n(:9091/metrics)"]
        
        Stream --> Ingress --> Mux
        Mux -->|Active Synthesis| Primary
        Mux -->|Speculative Shadow / Prefix| Standby
        
        Primary -.->|Chunk Arrival| Watchdog
        Primary -->|PCM Chunks| Ring
        Standby -->|PCM Chunks| Ring
        
        Primary -.->|word_timestamps| WordAlign
        Standby -.->|alignment.characters| WordAlign
        
        Watchdog -->|Atomic CAS Transition| Hann
        Ring --> Hann
        WordAlign --> Hann
        Hann --> Packetizer
        Packetizer --> Telemetry
    end

    subgraph Egress ["Downstream Audio Sink"]
        WebRTC["WebRTC Audio Track / SFU\n(LiveKit Room / Browser PeerConnection)"]
        Packetizer -->|Contiguous 10ms PCM / RTP| WebRTC
    end
```

---

## The Mathematics of Cadence

### 1. TTFB Hard Ceiling
Let $t_{\text{dispatch}}$ be the monotonic microsecond timestamp when an LLM text clause is dispatched to the TTS WebSocket. At time $t_{\text{now}}$, instantaneous TTFB is:

$$\Delta t_{\text{TTFB}}(t_{\text{now}}) = t_{\text{now}} - t_{\text{dispatch}}$$

If no audio chunk has arrived for the active clause and $\Delta t_{\text{TTFB}}(t_{\text{now}}) > \tau_{\text{TTFB}}$ ($120,000\ \mu\text{s}$), the watchdog executes an atomic Compare-And-Swap (CAS) to promote the warm standby socket.

### 2. RFC 3550 Sliding-Window Jitter & Predictive Underrun
For chunk $i$ containing $N_i$ samples at sample rate $f_s$, nominal acoustic duration is $D_i = \frac{N_i \cdot 10^6}{f_s}\ \mu\text{s}$. Inter-arrival variation is $d_i = \vert \Delta t_i - D_{i-1} \vert$. Jitter is smoothed using RFC 3550 EWMA with $\alpha = \frac{1}{16}$:

$$J_i = J_{i-1} + \frac{1}{16}(d_i - J_{i-1})$$

A mid-stream failover triggers if either the hard spike ceiling is breached or the predictive buffer underrun formula triggers:

$$\Delta t_{\text{gap}}(t_{\text{now}}) > 150\text{ms} \quad \lor \quad \left( \Delta t_{\text{gap}}(t_{\text{now}}) + 2.5 \cdot J_i > 150\text{ms} \ \land \ B(t_{\text{now}}) \le 15\text{ms} \right)$$

where $B(t_{\text{now}})$ is the current unread occupancy of the 40ms holdback ring buffer.

### 3. Phase-Aligned 5ms Raised-Cosine (Hann) Crossfade
Given $N_{\text{fade}} = 0.005 \times f_s$ (120 samples @ 24kHz), Cadence computes the normalized cross-correlation over a 2ms search window $\delta \in [0, L_{\text{max}}]$ to prevent destructive phase cancellation:

$$\delta^* = \arg\max_{\delta \in [0, L_{\text{max}}]} \sum_{n=0}^{N_{\text{fade}}-1} P[n] \cdot S[n + \delta]$$

Output samples $Y[n]$ are blended via pre-computed Hann weights:

$$w_{\text{out}}[n] = \frac{1}{2}\left[1 + \cos\left(\frac{\pi n}{N_{\text{fade}}-1}\right)\right], \quad w_{\text{in}}[n] = 1 - w_{\text{out}}[n]$$

$$Y[n] = \text{clamp}_{[-32768, 32767]}\left(\text{round}\left(w_{\text{out}}[n]\cdot P[n] + w_{\text{in}}[n]\cdot S[n + \delta^*]\right)\right)$$

Both boundaries have zero first derivatives ($\left.\frac{dw}{dn}\right\vert_{n=0} = \left.\frac{dw}{dn}\right\vert_{n=N_{\text{fade}}-1} = 0$), guaranteeing $C^1$ continuity without acoustic clicks.

---

## Key Features

- **Sub-15ms Failover SLA**: Under 18ms guaranteed (typically 8–12ms) from provider socket stall to blended standby audio emission.
- **Word-Boundary Prosodic Alignment (`src/alignment.rs`)**: Parses Cartesia word timestamps and ElevenLabs character alignment. Automatically snaps the splice point to the nearest low-energy RMS valley (inter-syllable dip), preventing repeated or clipped syllables when providers speak at different rates.
- **Adaptive Shadowing Strategies (`src/multiplexer.rs`)**:
  - `Full`: Continuous speculative shadowing for ultra-critical calls.
  - `PrefixOnly`: Shadows only the first 4 words / 250ms to protect against TTFB spikes, canceling the backup context once primary delivers on time (reduces standby token costs by ~80%).
  - `Canary`: Automatically escalates to shadow mode only when the primary provider's rolling p95 TTFB degrades past 75ms.
- **Multi-Session Axum Ingress Server (`src/server.rs`)**: Handles concurrent connections on `ws://0.0.0.0:8080/v1/tts/stream` with isolated per-session pipelines and clean connection lifecycle management.
- **Wait-Free Lock-Free Telemetry (`src/telemetry.rs`)**: 64-bucket microsecond histograms tracking provider RTT, TTFB, jitter percentiles (p50, p95, p99), and splice durations on `:9091/metrics`.
- **Dual Egress Framing (`src/protocol.rs`)**: Supports raw 10ms PCM s16le streaming for voice AI frameworks (LiveKit/Pipecat) and feature-gated libopus RFC 6716 compressed RTP packets.

---

## Quickstart

### 1. Build & Run from Source

```bash
# Build release binary
cargo build --release

# Run self-check in-process pipeline test
cargo run --release

# Run multi-session server on port 8080 with metrics on 9091
CADENCE_INGRESS_ADDR=127.0.0.1:8080 cargo run --release
```

### 2. Run the Hermetic Chaos Test Suite

Injects a deterministic 350ms network stall on the primary socket mid-sentence, asserts zero dropped frames and sub-18ms failover, and writes audio comparison WAV files:

```bash
cargo test --release --test chaos_failover -- --nocapture
```

### 3. Docker Compose with Prometheus & Grafana

```bash
docker compose up -d
```

- **Cadence Ingress**: `ws://localhost:8080/v1/tts/stream`
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (admin / cadence)

---

## Python Client (LiveKit & Pipecat Integration)

Install the lightweight client:

```bash
pip install ./clients/python
```

Usage in your agent:

```python
import asyncio
from cadence_client import CadenceClient

async def main():
    async with CadenceClient(url="ws://127.0.0.1:8080/v1/tts/stream") as client:
        await client.synthesize(
            utterance_id=1,
            text="Hello! Your reservation is confirmed.",
            voice_id_primary="sonic-english-1",
            voice_id_standby="elevenlabs-flash-1",
        )
        async for pcm_frame in client.stream_audio():
            # 10ms raw PCM s16le bytes (480 bytes at 24kHz mono)
            print(f"Received frame: {len(pcm_frame)} bytes")

asyncio.run(main())
```

---

## Production Deployment & Kernel Tuning

### Linux Kernel Parameters (`/etc/sysctl.d/99-cadence-realtime.conf`)

```ini
# 64 MB maximum socket receive/send buffers for high-density WebRTC streams
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.core.rmem_default = 16777216
net.core.wmem_default = 16777216

# Low-latency busy polling
net.core.busy_poll = 50
net.core.busy_read = 50

# TCP stack optimization for upstream WebSocket connection pool
net.ipv4.tcp_rmem = 4096 131072 67108864
net.ipv4.tcp_wmem = 4096 131072 67108864
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_fastopen = 3

# High file descriptor ceiling
fs.file-max = 2097152
```

Apply immediately:
```bash
sudo sysctl --system
```

### Systemd Real-Time Service (`/etc/systemd/system/cadence.service`)

Configures POSIX FIFO real-time priority 80 with unlimited memory locking:

```ini
[Unit]
Description=Cadence Real-Time Voice Failover Proxy
After=network-online.target

[Service]
Type=simple
User=cadence
Group=cadence
Environment="RUST_LOG=info"
Environment="CADENCE_INGRESS_ADDR=0.0.0.0:8080"
Environment="CADENCE_METRICS_ADDR=0.0.0.0:9091"
ExecStart=/usr/local/bin/cadence
Restart=always

LimitNOFILE=1048576
LimitMEMLOCK=infinity
LimitRTPRIO=99
CPUSchedulingPolicy=fifo
CPUSchedulingPriority=80

[Install]
WantedBy=multi-user.target
```

---

## Prometheus Metrics Reference

| Metric | Type | Description |
|---|---|---|
| `cadence_provider_rtt_microseconds` | Gauge | Upstream WebSocket Ping/Pong RTT in $\mu\text{s}$ per provider |
| `cadence_ttfb_microseconds` | Summary | Time-to-First-Byte distribution ($p50, p95, p99$) |
| `cadence_ttfb_rolling_p95_microseconds` | Gauge | Rolling 32-sample p95 TTFB used for Canary auto-escalation |
| `cadence_failover_switches_total` | Counter | Total failovers partitioned by reason (`ttfb_exceeded_120ms`, `inter_chunk_spike_150ms`, `predictive_jitter_underrun`, `primary_socket_closed`) |
| `cadence_saved_calls_total` | Counter | Total voice sessions saved via zero-drop hot splice |
| `cadence_egress_frames_total` | Counter | Total 10ms audio frames emitted |
| `cadence_dropped_audio_frames_total` | Counter | Dropped audio frames (always 0 in healthy Cadence operation) |
| `cadence_jitter_microseconds` | Summary | Chunk arrival jitter percentiles ($p50, p95, p99$) |
| `cadence_splice_latency_microseconds` | Summary | Time from watchdog trigger to completed 5ms crossfade ($p50, p95, p99$) |

---

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
