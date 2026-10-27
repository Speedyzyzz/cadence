"""
livekit_cadence
~~~~~~~~~~~~~~~
LiveKit Agents and Pipecat plugin for the Cadence Zero-Downtime Voice Failover Proxy.
Decouples LLM speech dispatch from upstream TTS latency spikes, streaming contiguous
10ms PCM audio frames via WebSocket with sub-15ms failover guarantees.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import json
import logging
import struct
import uuid
from typing import AsyncGenerator, AsyncIterator, Optional, Union

import websockets

logger = logging.getLogger("livekit.plugins.cadence")


def _connect_ws(url: str, headers: Optional[dict] = None, **kwargs):
    sig = inspect.signature(websockets.connect)
    if headers:
        if "additional_headers" in sig.parameters:
            kwargs["additional_headers"] = headers
        elif "extra_headers" in sig.parameters:
            kwargs["extra_headers"] = headers
    return websockets.connect(url, **kwargs)


@dataclass
class CadenceTTSOptions:
    endpoint_url: str = "ws://127.0.0.1:8080/v1/tts/stream"
    primary_voice: str = "cartesia-sonic-1"
    standby_voice: str = "elevenlabs-flash-1"
    sample_rate: int = 24000
    flush_on_interruption: bool = True
    session_id: Optional[str] = None


@dataclass
class RawPcmFrame:
    """Represents a 10ms uncompressed mono PCM audio frame from Cadence."""
    pcm_bytes: bytes
    sample_rate: int
    pts_us: int
    rtp_sequence: int
    active_provider: str
    spliced: bool

    @property
    def duration_ms(self) -> float:
        # 16-bit mono = 2 bytes per sample
        samples = len(self.pcm_bytes) // 2
        return (samples * 1000.0) / self.sample_rate


class CadenceStreamSession:
    """
    Asynchronous WebSocket session to the Cadence Multi-Session Ingress Server.
    Manages session handshake, dispatching synthesis clauses, and consuming
    continuous 10ms PCM audio frames.
    """

    def __init__(self, options: Optional[CadenceTTSOptions] = None) -> None:
        self.opts = options or CadenceTTSOptions()
        self.session_id = self.opts.session_id or f"lk-sess-{uuid.uuid4().hex[:12]}"
        self._ws: Optional[object] = None
        self._closed = False
        self._seq_counter = 0

    async def connect(self) -> CadenceStreamSession:
        headers = {
            "X-Cadence-Session-Id": self.session_id,
        }
        self._ws = await _connect_ws(
            self.opts.endpoint_url,
            headers=headers,
            ping_interval=10,
            ping_timeout=5,
        )
        logger.info("Connected to Cadence proxy at %s (session: %s)", self.opts.endpoint_url, self.session_id)
        return self

    async def __aenter__(self) -> CadenceStreamSession:
        return await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def send_synthesis(
        self,
        text: str,
        utterance_id: Optional[int] = None,
        flush: Optional[bool] = None,
    ) -> int:
        """Dispatches an LLM text clause to Cadence for speculative synthesis."""
        if self._ws is None or self._closed:
            raise RuntimeError("CadenceStreamSession is not connected")

        if utterance_id is None:
            self._seq_counter += 1
            utterance_id = self._seq_counter

        is_flush = self.opts.flush_on_interruption if flush is None else flush

        payload = {
            "type": "synthesize",
            "utterance_id": utterance_id,
            "text": text,
            "voice_id_primary": self.opts.primary_voice,
            "voice_id_standby": self.opts.standby_voice,
            "flush": is_flush,
        }
        await self._ws.send(json.dumps(payload))
        return utterance_id

    async def receive_frames(self) -> AsyncGenerator[RawPcmFrame, None]:
        """
        Yields contiguous RawPcmFrame audio frames received from Cadence.
        Logs control-plane JSON messages and failover events.
        """
        if self._ws is None:
            raise RuntimeError("CadenceStreamSession is not connected")

        while not self._closed:
            try:
                msg = await self._ws.recv()
                if isinstance(msg, bytes):
                    # Cadence binary frame format:
                    # [0..2]: rtp_seq (uint16_be)
                    # [2..6]: pts_us (uint32_be or uint64_be)
                    # [6..8]: sample_rate (uint16_be)
                    # [8]: active_provider (uint8: 0=Cartesia, 1=ElevenLabs)
                    # [9]: spliced (uint8: 0 or 1)
                    # [10..]: raw PCM s16le bytes
                    if len(msg) >= 12:
                        rtp_seq, pts_us_low, sr, provider_id, spliced = struct.unpack(">HIHBB", msg[:10])
                        pcm_bytes = msg[10:]
                        provider_str = "cartesia_primary" if provider_id == 0 else "elevenlabs_standby"
                        yield RawPcmFrame(
                            pcm_bytes=pcm_bytes,
                            sample_rate=sr,
                            pts_us=pts_us_low,
                            rtp_sequence=rtp_seq,
                            active_provider=provider_str,
                            spliced=bool(spliced),
                        )
                    else:
                        yield RawPcmFrame(
                            pcm_bytes=msg,
                            sample_rate=self.opts.sample_rate,
                            pts_us=0,
                            rtp_sequence=0,
                            active_provider="cadence",
                            spliced=False,
                        )
                elif isinstance(msg, str):
                    try:
                        event = json.loads(msg)
                        if event.get("event") == "failover_triggered":
                            logger.warning(
                                "Cadence hot failover triggered on session %s: reason=%s",
                                self.session_id,
                                event.get("reason"),
                            )
                    except json.JSONDecodeError:
                        pass
            except websockets.exceptions.ConnectionClosed:
                break

    async def close(self) -> None:
        self._closed = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


# LiveKit Agents optional plugin adapter
try:
    from livekit.agents import tts

    class CadenceChunkedStream(tts.ChunkedStream):
        def __init__(self, tts_instance: CadenceTTS, text: str) -> None:
            super().__init__(tts=tts_instance, text=text)
            self._session: Optional[CadenceStreamSession] = None

        async def _run(self) -> None:
            self._session = CadenceStreamSession(self._tts.opts)
            await self._session.connect()
            try:
                await self._session.send_synthesis(self._text)
                async for frame in self._session.receive_frames():
                    self._event_ch.send_nowait(
                        tts.SynthesizedAudio(
                            request_id=self._session.session_id,
                            data=frame.pcm_bytes,
                            sample_rate=frame.sample_rate,
                            num_channels=1,
                        )
                    )
            finally:
                await self._session.close()

    class CadenceSynthesizeStream(tts.SynthesizeStream):
        def __init__(self, tts_instance: CadenceTTS) -> None:
            super().__init__(tts=tts_instance)
            self._session: Optional[CadenceStreamSession] = None

        async def _run(self) -> None:
            self._session = CadenceStreamSession(self._tts.opts)
            await self._session.connect()
            try:
                async for text_segment in self._input_ch:
                    await self._session.send_synthesis(text_segment)
                    async for frame in self._session.receive_frames():
                        self._event_ch.send_nowait(
                            tts.SynthesizedAudio(
                                request_id=self._session.session_id,
                                data=frame.pcm_bytes,
                                sample_rate=frame.sample_rate,
                                num_channels=1,
                            )
                        )
            finally:
                await self._session.close()

    class CadenceTTS(tts.TTS):
        def __init__(
            self,
            options: Optional[CadenceTTSOptions] = None,
            *,
            api_key: Optional[str] = None,
        ) -> None:
            super().__init__(
                capabilities=tts.TTSCapabilities(streaming=True),
                sample_rate=options.sample_rate if options else 24000,
                num_channels=1,
            )
            self.opts = options or CadenceTTSOptions()

        def stream(self) -> CadenceSynthesizeStream:
            return CadenceSynthesizeStream(self)

        def synthesize(self, text: str) -> CadenceChunkedStream:
            return CadenceChunkedStream(self, text)

except ImportError:
    # LiveKit agents package is not installed; standalone mode active
    class CadenceTTS:  # type: ignore
        def __init__(self, options: Optional[CadenceTTSOptions] = None) -> None:
            self.opts = options or CadenceTTSOptions()

    class CadenceChunkedStream:  # type: ignore
        pass

    class CadenceSynthesizeStream:  # type: ignore
        pass
