"""session.stream_events: SSE consumption, resume/dedup, idle-timeout, to_sse."""
import httpx
import pytest

from collieai import (
    Blocked,
    ChunkSessionUnrecoverable,
    CollieAPIError,
    Finished,
    SafeDelta,
    StreamInterrupted,
    to_sse,
)


def _sse(*frames: str) -> bytes:
    return ("".join(frames)).encode()


def _chunk_frame(
    event_id: str, content: str, *, blocked: bool = False, block_message: str = None
) -> str:
    data = {"sequence": 0, "content": content, "blocked": blocked,
            "block_message": block_message, "final": False, "triggered_rules": []}
    import json
    return f"id: {event_id}\nevent: chunk\ndata: {json.dumps(data)}\n\n"


def _end_frame(reason: str) -> str:
    return f'event: end\ndata: {{"reason": "{reason}"}}\n\n'


async def _open_session(build_client, sse_handler):
    """Create a session (job_stream) and route GET /stream to sse_handler."""
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path == "/v1/jobs/job_stream/stream":
            return sse_handler(request)
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="prompt")
    await session.__aenter__()
    return session


async def _collect(aiter):
    return [e async for e in aiter]


@pytest.mark.asyncio
async def test_stream_events_yields_deltas_and_finished(build_client):
    def sse(_request):
        return httpx.Response(200, content=_sse(
            _chunk_frame("0-0", "hello"),
            ": keepalive\n\n",                 # ignored
            _chunk_frame("1-0", " world"),
            _end_frame("final"),
        ))

    session = await _open_session(build_client, sse)
    events = await _collect(session.stream_events())
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert deltas == ["hello", " world"]
    assert isinstance(events[-1], Finished)
    assert events[-1].finish_reason == "final"


@pytest.mark.asyncio
async def test_stream_events_blocked_terminal(build_client):
    def sse(_request):
        return httpx.Response(200, content=_sse(
            _chunk_frame("0-0", "", blocked=True, block_message="Custom rule block message"),
            _end_frame("blocked"),
        ))

    session = await _open_session(build_client, sse)
    events = await _collect(session.stream_events())
    assert isinstance(events[-1], Blocked)
    assert events[-1].block_message == "Custom rule block message"  # F1
    assert not any(isinstance(e, Finished) for e in events)


@pytest.mark.asyncio
async def test_stream_events_upstream_error_raises(build_client):
    def sse(_request):
        return httpx.Response(200, content=_sse(
            _chunk_frame("0-0", "partial"),
            _end_frame("upstream_error"),
        ))

    session = await _open_session(build_client, sse)
    with pytest.raises(CollieAPIError) as ei:
        await _collect(session.stream_events())
    assert ei.value.code == "upstream_error"


@pytest.mark.asyncio
async def test_session_unrecoverable_is_fatal_not_resumed(build_client):
    """session_unrecoverable must raise (not auto-resume) even with the default
    auto_resume=True."""
    calls = {"n": 0}

    def sse(_request):
        calls["n"] += 1
        return httpx.Response(200, content=_sse(
            _chunk_frame("0-0", "partial"),
            _end_frame("session_unrecoverable"),
        ))

    session = await _open_session(build_client, sse)
    with pytest.raises(ChunkSessionUnrecoverable):
        await _collect(session.stream_events())
    assert calls["n"] == 1  # did not reconnect


@pytest.mark.asyncio
async def test_malformed_chunk_frame_raises_invalid_response(build_client):
    def sse(_request):
        return httpx.Response(200, content=b"id: 0-0\nevent: chunk\ndata: not-json\n\n")

    session = await _open_session(build_client, sse)
    with pytest.raises(CollieAPIError) as ei:
        await _collect(session.stream_events())
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_malformed_end_frame_raises_invalid_response(build_client):
    def sse(_request):
        return httpx.Response(200, content=b"event: end\ndata: not-json\n\n")

    session = await _open_session(build_client, sse)
    with pytest.raises(CollieAPIError) as ei:
        await _collect(session.stream_events())
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_idle_timeout_is_typed_resumable_event(build_client):
    def sse(_request):
        return httpx.Response(200, content=_sse(
            _chunk_frame("0-0", "hello"),
            _end_frame("idle_timeout"),
        ))

    session = await _open_session(build_client, sse)
    events = await _collect(session.stream_events(auto_resume=False))
    assert isinstance(events[-1], StreamInterrupted)
    assert events[-1].reason == "idle_timeout"
    assert events[-1].resumable is True
    assert events[-1].last_event_id == "0-0"


@pytest.mark.asyncio
async def test_reconnect_resumes_without_duplicate(build_client):
    """First connection drops after '0-0'; on resume the server replays '0-0'
    and adds '1-0' — the SDK must not re-emit the replayed delta."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path == "/v1/jobs/job_stream/stream":
            calls["n"] += 1
            if calls["n"] == 1:
                # delta then abrupt close (no terminal frame) -> disconnect
                return httpx.Response(200, content=_sse(_chunk_frame("0-0", "hello")))
            # resume: Last-Event-ID echoed; server replays 0-0 then continues
            assert request.headers.get("Last-Event-ID") == "0-0"
            return httpx.Response(200, content=_sse(
                _chunk_frame("0-0", "hello"),     # replay — must be deduped
                _chunk_frame("1-0", " world"),
                _end_frame("final"),
            ))
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="prompt")
    await session.__aenter__()
    events = await _collect(session.stream_events())

    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert deltas == ["hello", " world"]          # "hello" exactly once
    assert any(isinstance(e, StreamInterrupted) for e in events)
    assert isinstance(events[-1], Finished)
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_transport_timeout_maps_to_idle(build_client):
    def sse(_request):
        raise httpx.ReadTimeout("idle")

    session = await _open_session(build_client, sse)
    events = await _collect(session.stream_events(auto_resume=False))
    assert len(events) == 1
    assert isinstance(events[0], StreamInterrupted)
    assert events[0].reason == "idle_timeout"


@pytest.mark.asyncio
async def test_stream_events_404_raises(build_client):
    def sse(_request):
        return httpx.Response(404, json={"error": {"message": "gone", "type": "not_found"}})

    session = await _open_session(build_client, sse)
    with pytest.raises(CollieAPIError) as ei:
        await _collect(session.stream_events())
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_mint_stream_token_returns_token_and_url(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "POST" and path == "/v1/jobs/job_stream/stream-token":
            return httpx.Response(200, json={"stream_token": "tok_abc", "expires_in": 60})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="x")
    await session.__aenter__()
    st = await session.mint_stream_token()
    assert st.token == "tok_abc"
    assert st.expires_in == 60
    assert st.url == "http://test/v1/jobs/job_stream/stream?stream_token=tok_abc"


@pytest.mark.asyncio
async def test_mint_stream_token_missing_token_raises(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if path.endswith("/stream-token"):
            return httpx.Response(200, json={"expires_in": 60})  # no stream_token
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="x")
    await session.__aenter__()
    with pytest.raises(CollieAPIError) as ei:
        await session.mint_stream_token()
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
@pytest.mark.parametrize("expires_in", [None, "soon", 0, -5, True, 0.5, 60.0])
async def test_mint_stream_token_invalid_expires_in_raises(build_client, expires_in):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if path.endswith("/stream-token"):
            body = {"stream_token": "tok"}
            if expires_in is not None:
                body["expires_in"] = expires_in
            return httpx.Response(200, json=body)
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="x")
    await session.__aenter__()
    with pytest.raises(CollieAPIError) as ei:
        await session.mint_stream_token()
    assert ei.value.code == "invalid_response"


def test_to_sse_formats_frames():
    assert to_sse(SafeDelta(text="hi", sequence=2)) == 'event: delta\ndata: {"text": "hi", "sequence": 2}\n\n'
    assert to_sse(Finished(finish_reason="stop")).startswith("event: finished\n")
    assert to_sse(StreamInterrupted(reason="disconnect")).startswith("event: interrupted\n")


def test_to_sse_serializes_buffered_fallback():
    from collieai import BufferedFallback

    frame = to_sse(BufferedFallback(reason="preset_buffered"))
    assert frame.startswith("event: buffered_fallback\n")
    assert "preset_buffered" in frame
    assert "buffer_then_show" in frame


def test_to_sse_input_blocked_carries_context_verdict():
    import json
    from collieai import ContextModerationResult, InputBlocked

    ev = InputBlocked(
        block_message="ctx",
        blocked_by="context",
        context=ContextModerationResult(
            status="blocked", blocked=True, triggering_pointer="/transaction/title",
            triggering_rule_type="lightweight_model",
        ),
    )
    frame = to_sse(ev)
    assert frame.startswith("event: input_blocked\n")
    data = json.loads(frame.split("data: ", 1)[1])
    assert data["blocked_by"] == "context"
    assert data["context"]["status"] == "blocked"
    assert data["context"]["triggering_pointer"] == "/transaction/title"


def test_to_sse_finished_carries_context_verdict():
    import json
    from collieai import ContextModerationResult

    ev = Finished(
        finish_reason="stop", blocked_by="none",
        context=ContextModerationResult(status="degraded", inference_degraded=True),
    )
    frame = to_sse(ev)
    assert frame.startswith("event: finished\n")
    data = json.loads(frame.split("data: ", 1)[1])
    assert data["context"]["status"] == "degraded"
    assert data["context"]["inference_degraded"] is True


def test_to_sse_finished_without_context_omits_keys():
    import json

    data = json.loads(to_sse(Finished(finish_reason="stop")).split("data: ", 1)[1])
    assert "context" not in data
    assert "blocked_by" not in data
