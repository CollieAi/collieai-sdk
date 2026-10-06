"""Retry + idempotency: same-sequence replay, transient handling, exhaustion."""
from __future__ import annotations

import httpx
import pytest

from collieai import ChunkQuotaExceeded, ChunkResolutionUnavailable, ChunkRetryExhausted
from conftest import FakeChunkBackend, body_of, error_response


def _flaky_handler(backend: FakeChunkBackend, fail_times: int, failure: httpx.Response | Exception):
    """Chunk handler that fails the first `fail_times` chunk submits, then
    delegates to the backend (success)."""
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            state["calls"] += 1
            backend.chunk_requests.append(body_of(request))
            if state["calls"] <= fail_times:
                if isinstance(failure, Exception):
                    raise failure
                return failure
            body = body_of(request)
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": [{"content": body["content"], "blocked": False, "final": False, "triggered_rules": []}],
                "finished": False,
            })
        return backend.handler(request)

    return handler


@pytest.mark.asyncio
async def test_network_timeout_retries_same_sequence(build_client):
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 1, httpx.ConnectTimeout("boom"))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        result = await session.push("hello")

    seqs = [c["sequence"] for c in backend.chunk_requests]
    assert seqs == [0, 0]            # same sequence retried
    assert result.sequence == 0
    assert result.emits[0].text == "hello"
    assert session._next_sequence == 1  # advanced once, after the confirmed accept


@pytest.mark.asyncio
async def test_filter_timeout_retries_then_succeeds(build_client):
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 1, error_response(504, "chunk_filter_timeout"))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        result = await session.push("hi")
    assert [c["sequence"] for c in backend.chunk_requests] == [0, 0]
    assert result.accepted is True


@pytest.mark.asyncio
async def test_retry_budget_exhausted(build_client):
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 99, error_response(504, "chunk_filter_timeout"))
    collie = build_client(handler, max_retries=3)
    async with collie.streaming.session(input="p") as session:
        with pytest.raises(ChunkRetryExhausted):
            await session.push("hi")
    assert len(backend.chunk_requests) == 3  # 3 attempts, all same sequence
    assert all(c["sequence"] == 0 for c in backend.chunk_requests)


@pytest.mark.asyncio
async def test_429_retry_after_zero_retries_immediately(build_client):
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 1, error_response(429, "rate_limited", **{"Retry-After": "0"}))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        result = await session.push("hi")
    assert result.accepted is True
    assert len(backend.chunk_requests) == 2


@pytest.mark.asyncio
async def test_429_without_retry_after_raises_quota(build_client):
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 99, error_response(429, "rate_limited"))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        with pytest.raises(ChunkQuotaExceeded):
            await session.push("hi")
    # Fatal on the first attempt — no retry.
    assert len(backend.chunk_requests) == 1


@pytest.mark.asyncio
async def test_resolution_unavailable_retries_then_succeeds(build_client):
    """503 chunk_resolution_unavailable is an outage — retried, not fatal."""
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 1, error_response(503, "chunk_resolution_unavailable"))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        result = await session.push("hi")
    assert [c["sequence"] for c in backend.chunk_requests] == [0, 0]
    assert result.accepted is True


@pytest.mark.asyncio
async def test_resolution_unavailable_exhaustion_names_the_condition(build_client):
    """When the outage outlasts the budget, the exhaustion's __cause__ is the
    typed ChunkResolutionUnavailable, not a generic CollieAPIError."""
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 99, error_response(503, "chunk_resolution_unavailable"))
    collie = build_client(handler, max_retries=3)
    async with collie.streaming.session(input="p") as session:
        with pytest.raises(ChunkRetryExhausted) as ei:
            await session.push("hi")
    cause = ei.value.__cause__
    assert isinstance(cause, ChunkResolutionUnavailable)
    assert cause.code == "chunk_resolution_unavailable"
    assert cause.status_code == 503


@pytest.mark.asyncio
async def test_replay_returns_emits_once(build_client):
    """A transient failure then success yields the safe emit exactly once."""
    backend = FakeChunkBackend()
    handler = _flaky_handler(backend, 1, error_response(503, "chunk_persistence_unavailable"))
    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        result = await session.push("only-once")
    assert [e.text for e in result.emits] == ["only-once"]
