"""Cadence Zero-Downtime TTS Failover Client & LiveKit Agents Plugin."""

from .livekit_cadence import CadenceStreamSession, CadenceTTSOptions, RawPcmFrame

__all__ = ["CadenceStreamSession", "CadenceTTSOptions", "RawPcmFrame"]

try:
    from .livekit_cadence import CadenceChunkedStream, CadenceSynthesizeStream, CadenceTTS

    __all__.extend(["CadenceTTS", "CadenceChunkedStream", "CadenceSynthesizeStream"])
except ImportError:
    pass
