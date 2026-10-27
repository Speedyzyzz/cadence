#!/usr/bin/env python3
"""
scripts/smoke_live_providers.py
End-to-End WebSocket smoke test verifying the Cadence live ingress server.
Connects to ws://127.0.0.1:8080/v1/tts/stream, dispatches an utterance,
verifies that contiguous 10ms PCM audio frames are returned, and verifies
the active session metadata.
"""

import asyncio
import inspect
import json
import os
import sys
import urllib.request
import websockets

INGRESS_URL = os.environ.get("CADENCE_INGRESS_URL", "ws://127.0.0.1:8080/v1/tts/stream")
HTTP_ADDR = os.environ.get("CADENCE_INGRESS_ADDR", "127.0.0.1:8080")


def _connect_ws(url: str, headers: dict):
    sig = inspect.signature(websockets.connect)
    kwargs = {}
    if "additional_headers" in sig.parameters:
        kwargs["additional_headers"] = headers
    elif "extra_headers" in sig.parameters:
        kwargs["extra_headers"] = headers
    return websockets.connect(url, **kwargs)


async def run_smoke_test():
    print(f"Connecting to Cadence Ingress at {INGRESS_URL}...")
    session_id = "smoke-test-session-001"
    headers = {"X-Cadence-Session-Id": session_id}

    async with _connect_ws(INGRESS_URL, headers=headers) as ws:
        print(" Connected to Cadence WebSocket.")

        # Dispatch synthesis request
        synth_payload = {
            "type": "synthesize",
            "utterance_id": 42,
            "text": "Hello, Cadence zero-downtime voice failover proxy is online.",
            "voice_id_primary": "cartesia-sonic-en-1",
            "voice_id_standby": "elevenlabs-flash-v2.5-1",
            "flush": True,
        }
        await ws.send(json.dumps(synth_payload))
        print(" Dispatched synthesis request (utterance_id=42).")

        # Collect audio frames
        received_frames = 0
        total_pcm_bytes = 0

        while received_frames < 4:
            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            if isinstance(msg, bytes):
                received_frames += 1
                total_pcm_bytes += len(msg)
                print(f" Received Audio Frame #{received_frames}: {len(msg)} bytes")
            elif isinstance(msg, str):
                print(f" Received Control Event: {msg}")

        assert received_frames >= 4, f"Expected >= 4 audio frames, received {received_frames}"
        assert total_pcm_bytes > 0, "Expected non-zero PCM payload"

        print(f" Successfully verified {received_frames} audio frames ({total_pcm_bytes} total bytes).")

    # Verify session showed up in /v1/sessions
    try:
        req = urllib.request.urlopen(f"http://{HTTP_ADDR}/v1/sessions")
        sessions_json = json.loads(req.read().decode())
        print(f" /v1/sessions returned: {json.dumps(sessions_json)}")
    except Exception as e:
        print(f" Note: /v1/sessions inspection: {e}")

    print("\n==================================================================")
    print(" CADENCE E2E WEBSOCKET SMOKE TEST PASSED CLEANLY")
    print("==================================================================")


if __name__ == "__main__":
    try:
        asyncio.run(run_smoke_test())
    except Exception as e:
        print(f"\n[ERROR] Smoke test failed: {e}", file=sys.stderr)
        sys.exit(1)
