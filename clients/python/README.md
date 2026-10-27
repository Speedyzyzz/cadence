# cadence-client

Async Python client for Cadence: the open-core zero-downtime failover proxy for real-time voice AI agents.

## Installation

```bash
pip install cadence-client
```

## Quickstart

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
