"""F4: structured debug_logger events (chunk / chunk.retry / moderate.input /
preflight) carry the plan's recommended observability fields. Telemetry
callbacks remain deferred — these events are the supported hook."""
import httpx
import pytest

from conftest import FakeChunkBackend, body_of, error_response


class FakeLogger:
    def __init__(self):
        self.records = []  # (message, extra-dict)

    def debug(self, msg, *args, extra=None, **kwargs):
        self.records.append((msg, extra or {}))

    def by_event(self, name):
        return [extra for msg, extra in self.records if msg == f"collieai {name}"]


@pytest.mark.asyncio
async def test_chunk_events_carry_recommended_fields(build_client):
    backend = FakeChunkBackend()
    log = FakeLogger()
    collie = build_client(backend.handler, debug_logger=log)

    async with collie.streaming.session(input="prompt") as session:
        await session.push("hello")
        await session.finish()

    chunk_events = log.by_event("chunk")
    assert len(chunk_events) == 2  # push + finish
    first = chunk_events[0]
    assert first["job_id"] == "job_test"
    assert first["sequence"] == 0
    assert first["chunk_chars"] == 5
    assert first["emit_chars"] == 5  # FakeChunkBackend echoes content
    assert first["retry_count"] == 0
    assert first["blocked"] is False
    assert first["finished"] is False
    assert isinstance(first["latency_ms"], float)
    assert first["triggered_rule_ids"] == []
    assert chunk_events[1]["finished"] is True


@pytest.mark.asyncio
async def test_retry_emits_retry_event_and_retry_count(build_client):
    state = {"chunk_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        state["chunk_calls"] += 1
        if state["chunk_calls"] == 1:
            return error_response(503, "transient")
        body = body_of(request)
        return httpx.Response(200, json={
            "sequence": body["sequence"], "accepted": True,
            "emits": [], "finished": False,
        })

    log = FakeLogger()
    collie = build_client(handler, debug_logger=log)
    session = collie.streaming.session(input="p")
    await session.__aenter__()
    await session.push("x")

    retries = log.by_event("chunk.retry")
    assert len(retries) == 1
    assert retries[0]["attempt"] == 1 and retries[0]["sequence"] == 0
    assert log.by_event("chunk")[0]["retry_count"] == 1


@pytest.mark.asyncio
async def test_moderate_input_event(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_mod"})
        return httpx.Response(200, json={
            "job_id": "job_mod", "status": "completed",
            "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
        })

    log = FakeLogger()
    collie = build_client(handler, debug_logger=log)
    await collie.moderate.input("hi", conversation_id="conv-1")

    events = log.by_event("moderate.input")
    assert len(events) == 1
    event = events[0]
    assert event["job_id"] == "job_mod"
    assert event["blocked"] is False
    assert event["conversation_id"] == "conv-1"
    assert isinstance(event["latency_ms"], float)


@pytest.mark.asyncio
async def test_raising_logger_never_breaks_sdk_calls(build_client):
    """Debug logging is best-effort: a customer logger whose debug() raises
    must not fail calls that already succeeded on the wire. All debug events
    flow through the same AsyncCollie._debug choke point; the 503-then-200
    handler makes the first push genuinely hit the chunk.retry call site
    INSIDE _post_chunk's retry loop — a raise there would otherwise abort
    the retry instead of just losing a log line."""
    class ExplodingLogger:
        def debug(self, *args, **kwargs):
            raise RuntimeError("customer logger bug")

    state = {"chunk_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        state["chunk_calls"] += 1
        if state["chunk_calls"] == 1:
            return error_response(503, "transient")
        body = body_of(request)
        is_final = bool(body.get("is_final"))
        return httpx.Response(200, json={
            "sequence": body["sequence"], "accepted": True,
            "emits": [{"content": body.get("content", ""), "blocked": False,
                       "final": is_final, "triggered_rules": []}],
            "finished": is_final,
        })

    collie = build_client(handler, debug_logger=ExplodingLogger())
    session = collie.streaming.session(input="p")
    await session.__aenter__()
    result = await session.push("hello")
    final = await session.finish()

    assert state["chunk_calls"] == 3  # failed + retried push, then finish
    assert result.emits[0].text == "hello"
    assert final.finished is True


@pytest.mark.asyncio
async def test_preflight_event_with_cached_flag(build_client):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "mode": "streaming", "recommended_client_behavior": "stream",
            "project_id": "proj_1",
            "valid_until": "2099-01-01T00:00:00Z", "rules": [],
        })

    log = FakeLogger()
    collie = build_client(handler, debug_logger=log)
    await collie.streaming.preflight()
    await collie.streaming.preflight()  # second call served from cache

    events = log.by_event("preflight")
    assert [e["cached"] for e in events] == [False, True]
    assert events[0]["mode"] == "streaming"
    assert events[0]["project_id"] == "proj_1"
