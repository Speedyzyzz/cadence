"""
cadence_client
~~~~~~~~~~~~~~
Minimal async Python WebSocket client for streaming TTS through the Cadence
zero-downtime failover proxy. Compatible with LiveKit Agents and Pipecat.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
from typing import AsyncGenerator, Optional, Tuple, Union
import websockets


def _connect_ws(url: str, headers: dict, **kwargs):
    sig = inspect.signature(websockets.connect)
    if headers:
        if "additional_headers" in sig.parameters:
            kwargs["additional_headers"] = headers
        elif "extra_headers" in sig.parameters:
            kwargs["extra_headers"] = headers
    return websockets.connect(url, **kwargs)


class CadenceClient:
    """
    Async client for Cadence Voice Failover Proxy.
    
    Usage:
        async with CadenceClient(url="ws://127.0.0.1:8080/v1/tts/stream") as client:
            await client.synthesize(
                utterance_id=1,
                text="Hello from LiveKit agent!",
                voice_id_primary="sonic-english-1",
                voice_id_standby="elevenlabs-flash-1",
            )
            async for chunk in client.stream_audio():
                # chunk is 10ms raw PCM s16le bytes (480 bytes at 24kHz)
                await audio_sink.write(chunk)
    """

    def __init__(
        self,
        url: str = "ws://127.0.0.1:8080/v1/tts/stream",
        session_id: Optional[str] = None,
        extra_headers: Optional[dict] = None,
    ) -> None:
        self.url = url
        self.session_id = session_id or f"py-sess-{uuid.uuid4().hex[:12]}"
        self.extra_headers = extra_headers or {}
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def connect(self) -> CadenceClient:
        headers = {
            "X-Cadence-Session-Id": self.session_id,
            **self.extra_headers,
        }
        self._ws = await _connect_ws(self.url, headers=headers)
        return self

    async def __aenter__(self) -> CadenceClient:
        return await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def synthesize(
        self,
        utterance_id: int,
        text: str,
        voice_id_primary: str = "cartesia-sonic-1",
        voice_id_standby: str = "elevenlabs-flash-1",
        flush: bool = True,
    ) -> None:
        """Dispatches an LLM token segment / clause to Cadence."""
        if self._ws is None:
            raise RuntimeError("CadenceClient is not connected. Call .connect() or use 'async with'.")

        payload = {
            "type": "synthesize",
            "utterance_id": utterance_id,
            "text": text,
            "voice_id_primary": voice_id_primary,
            "voice_id_standby": voice_id_standby,
            "flush": flush,
        }
        await self._ws.send(json.dumps(payload))

    async def stream_audio(self) -> AsyncGenerator[bytes, None]:
        """
        Yields raw 10ms PCM s16le audio frames received from Cadence.
        Discards control-plane JSON messages (or logs them).
        """
        if self._ws is None:
            raise RuntimeError("CadenceClient is not connected.")

        while True:
            try:
                msg = await self._ws.recv()
                if isinstance(msg, bytes):
                    yield msg
                else:
                    # JSON event frame (e.g. failover notification)
                    pass
            except websockets.exceptions.ConnectionClosed:
                break

    async def close(self) -> None:
        """Closes the WebSocket connection."""
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
