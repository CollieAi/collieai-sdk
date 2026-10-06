"""Typed error mapping: each wire error code -> its SDK exception, fatal (no
retry), and the session is failed afterward."""
import httpx
import pytest

from collieai import (
    ChunkIdempotencyConflict,
    ChunkPolicyChanged,
    ChunkSequenceConflict,
    ChunkSessionFinished,
    ChunkSessionUnrecoverable,
    ChunkStreamingUnsupported,
)
from conftest import FakeChunkBackend, error_response

_CASES = [
    (409, "chunk_session_finished", ChunkSessionFinished),
    (409, "chunk_sequence_conflict", ChunkSequenceConflict),
    (409, "chunk_idempotency_conflict", ChunkIdempotencyConflict),
    (409, "chunk_session_unrecoverable", ChunkSessionUnrecoverable),
    (409, "chunk_policy_changed", ChunkPolicyChanged),
    (400, "chunk_streaming_unsupported", ChunkStreamingUnsupported),
]


@pytest.mark.parametrize("status,code,exc_cls", _CASES)
@pytest.mark.asyncio
async def test_fatal_code_maps_to_exception(build_client, status, code, exc_cls):
    backend = FakeChunkBackend()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            calls["n"] += 1
            return error_response(status, code, message=f"{code} happened")
        return backend.handler(request)

    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        with pytest.raises(exc_cls) as ei:
            await session.push("x")
        # Fatal: raised on the first attempt, no retry.
        assert calls["n"] == 1
        # Carries the wire code + status for programmatic handling.
        assert ei.value.code == code
        assert ei.value.status_code == status
        # The session is failed; further use raises.
        assert session._closed is True
