"""Low-level streaming session: sequencing, finalization, terminal-on-block,
serialization."""
import httpx
import pytest

from collieai import (
    ChunkSessionFinished,
    CollieAPIError,
    CollieConnectionError,
    CollieError,
    ConcurrentSessionUseError,
)
from conftest import FakeChunkBackend, body_of


@pytest.mark.asyncio
async def test_push_assigns_monotonic_sequences_and_finish(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)

    async with collie.streaming.session(input="prompt") as session:
        r0 = await session.push("alpha")
        r1 = await session.push("beta")
        rf = await session.finish()

    assert [c["sequence"] for c in backend.chunk_requests] == [0, 1, 2]
    assert r0.sequence == 0 and r1.sequence == 1 and rf.sequence == 2
    assert r0.emits[0].text == "alpha"
    # finish sends an empty final chunk carrying the finish_reason (F5);
    # non-final pushes never carry one.
    assert backend.chunk_requests[-1]["is_final"] is True
    assert backend.chunk_requests[-1]["content"] == ""
    assert backend.chunk_requests[-1]["finish_reason"] == "stop"
    assert all("finish_reason" not in c for c in backend.chunk_requests[:-1])
    assert rf.finished is True
    assert session.last_sequence == 2


@pytest.mark.asyncio
async def test_custom_finish_reason_transmitted(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)

    async with collie.streaming.session(input="prompt") as session:
        await session.push("alpha")
        await session.finish(finish_reason="length")

    assert backend.chunk_requests[-1]["finish_reason"] == "length"


def _unconfirmed_backend(chunk_body_fn):
    """Backend whose 2xx chunk responses are built by chunk_body_fn(request_body)."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.url.path == "/v1/jobs/job_stream/chunks":
            return httpx.Response(200, json=chunk_body_fn(body_of(request)))
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})
    return handler


@pytest.mark.asyncio
async def test_accepted_false_fails_session_without_advancing(build_client):
    """A 2xx body with accepted=false is a contract violation: typed
    invalid_response, no sequence advance, session failed."""
    collie = build_client(_unconfirmed_backend(lambda b: {
        "sequence": b["sequence"], "accepted": False, "emits": [], "finished": False,
    }))
    session = collie.streaming.session(input="p")
    await session.__aenter__()
    with pytest.raises(CollieAPIError) as exc:
        await session.push("x")
    assert exc.value.code == "invalid_response"
    assert session.last_sequence == -1          # never advanced
    with pytest.raises(CollieError, match="closed"):
        await session.push("y")                 # session is failed/closed


@pytest.mark.asyncio
async def test_wrong_sequence_echo_fails_session(build_client):
    """A 2xx body echoing a different sequence (a stale/cached response for
    another chunk) must fail the session — advancing on it could replay
    another chunk's safe text to the end user."""
    collie = build_client(_unconfirmed_backend(lambda b: {
        "sequence": b["sequence"] + 7, "accepted": True,
        "emits": [{"content": "stale", "blocked": False, "final": False, "triggered_rules": []}],
        "finished": False,
    }))
    session = collie.streaming.session(input="p")
    await session.__aenter__()
    with pytest.raises(CollieAPIError) as exc:
        await session.push("x")
    assert exc.value.code == "invalid_response"
    assert session.last_sequence == -1          # the stale emit was never surfaced


@pytest.mark.asyncio
async def test_customer_never_sets_sequence_and_no_webhook_url(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="prompt") as session:
        await session.push("x")

    # Job created without a webhook_url (Slice 0 — no placeholder URLs).
    assert "webhook_url" not in backend.created_jobs[0]
    assert backend.created_jobs[0]["message_input"] == "prompt"
    # Every request is attributed to the session SDK method.
    assert set(backend.origins) == {"streaming.session"}


@pytest.mark.asyncio
async def test_finish_is_idempotent(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="prompt") as session:
        await session.push("x")
        first = await session.finish()
        second = await session.finish()

    assert second is first  # cached terminal result returned, no re-submit
    finals = [c for c in backend.chunk_requests if c["is_final"]]
    assert len(finals) == 1


@pytest.mark.asyncio
async def test_block_is_terminal(build_client):
    backend = FakeChunkBackend()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            body = body_of(request)
            return httpx.Response(200, json={
                "sequence": body["sequence"],
                "accepted": True,
                "emits": [{"content": "", "blocked": True, "final": True, "triggered_rules": []}],
                "finished": True,
            })
        return backend.handler(request)

    collie = build_client(handler)
    async with collie.streaming.session(input="prompt") as session:
        result = await session.push("bad")
        assert result.finished is True
        assert result.emits[0].blocked is True
        # Session is terminal — further pushes raise without hitting the wire.
        with pytest.raises(ChunkSessionFinished):
            await session.push("more")


@pytest.mark.asyncio
async def test_concurrent_push_raises(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="prompt") as session:
        await session._lock.acquire()
        try:
            with pytest.raises(ConcurrentSessionUseError):
                await session.push("x")
        finally:
            session._lock.release()


@pytest.mark.asyncio
async def test_context_manager_creates_job(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="prompt") as session:
        assert session.job_id == "job_test"
    assert len(backend.created_jobs) == 1


@pytest.mark.asyncio
async def test_empty_ids_omitted_on_create(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(
        input="p", conversation_id="", correlation_id=""
    ):
        pass
    assert "conversation_id" not in backend.created_jobs[0]
    assert "correlation_id" not in backend.created_jobs[0]


@pytest.mark.asyncio
async def test_input_job_id_sent_when_provided(build_client):
    """Manual sessions can forward their own gate's job id; the create body
    carries it alongside message_input (the prompt stays — the chunk
    path's guard context reads it)."""
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="prompt", input_job_id="job_gate"):
        pass
    assert backend.created_jobs[0]["input_job_id"] == "job_gate"
    assert backend.created_jobs[0]["message_input"] == "prompt"


@pytest.mark.asyncio
async def test_input_job_id_omitted_by_default_and_when_empty(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="p"):
        pass
    async with collie.streaming.session(input="p", input_job_id=""):
        pass
    assert "input_job_id" not in backend.created_jobs[0]
    assert "input_job_id" not in backend.created_jobs[1]


@pytest.mark.asyncio
async def test_session_project_id_mismatch_raises(build_client):
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)  # client project_id is "proj_1"
    with pytest.raises(ValueError):
        collie.streaming.session(input="p", project_id="other_project")


@pytest.mark.asyncio
async def test_job_create_transport_error_wrapped(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("down")

    collie = build_client(handler)
    with pytest.raises(CollieConnectionError):
        async with collie.streaming.session(input="p"):
            pass


@pytest.mark.asyncio
async def test_empty_job_create_body_fails_fast(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202)  # 2xx, no body

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        async with collie.streaming.session(input="p"):
            pass
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_job_create_without_job_id_raises_api_error(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"status": "processing_inbound"})  # no job_id

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        async with collie.streaming.session(input="p"):
            pass
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_malformed_chunk_response_raises_api_error(build_client):
    backend = FakeChunkBackend()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            return httpx.Response(200, content=b"<<not json>>")
        return backend.handler(request)

    collie = build_client(handler)
    async with collie.streaming.session(input="p") as session:
        with pytest.raises(CollieAPIError):
            await session.push("x")
        assert session._closed is True


# --------------------------------------------------------------------------
# Mutation-kill tests (#26 streaming target).
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_create_body_carries_ids_and_request_id(build_client):
    """The create body's conversation/correlation keys and the captured
    x-request-id are wire contract (survivors 432-435/443 renamed keys,
    nulled values and dropped the request id — nothing failed)."""
    state = {"created": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            state["created"].append(body_of(request))
            return httpx.Response(
                202, json={"job_id": "job_test"},
                headers={"x-request-id": "rid-42"},
            )
        return FakeChunkBackend().handler(request)

    collie = build_client(handler)
    session = collie.streaming.session(
        input="p", conversation_id="conv-9", correlation_id="corr-9",
    )
    async with session:
        pass

    body = state["created"][0]
    assert body["conversation_id"] == "conv-9"
    assert body["correlation_id"] == "corr-9"
    assert session.request_id == "rid-42"


@pytest.mark.asyncio
async def test_push_after_finish_raises_session_closed(build_client):
    """finish() closes the session durably (survivor 605 flipped the
    _closed write) — a late push must be a loud error, not a wire call."""
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    async with collie.streaming.session(input="p") as session:
        await session.push("a")
    from collieai import CollieError
    with pytest.raises(CollieError, match="closed"):
        await session.push("late")
