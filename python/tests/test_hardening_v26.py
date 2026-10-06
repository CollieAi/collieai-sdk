"""Twenty-second-review follow-ups (v26): a transport-owned builtin TimeoutError
during a chunk POST must be RETRIED, not misread as ceiling exhaustion (on
Python 3.11+ builtin TimeoutError IS asyncio.TimeoutError, so the outer
wait_for's except would otherwise claim it). Plus the PBC-13 chunk error-body
read-failure test that was missing."""
import asyncio
import time

import httpx
import pytest

from collieai import AsyncCollie, ChunkRetryExhausted, CollieConnectionError


def test_pbc_15a_default_time_seam_is_monotonic():
    """[PBC-15a] The default time seam is `time.monotonic` — a monotonic source,
    never the wall clock (`time.time`)."""
    c = AsyncCollie(api_key="clai_test", base_url="http://test")
    assert c._monotonic is time.monotonic
    a = c._monotonic()
    assert c._monotonic() >= a


async def _instant(_seconds: float) -> None:
    return None


class _TimeoutChunkTransport(httpx.AsyncBaseTransport):
    """The chunk POST raises a TRANSPORT-owned timeout of a given class; create OK."""
    def __init__(self, exc_type: type[BaseException]) -> None:
        self.posts = 0
        self._exc_type = exc_type

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            self.posts += 1
            raise self._exc_type("transport socket timeout")
        return httpx.Response(202, json={"job_id": "job_stream"})


# Both timeout classes: builtin TimeoutError AND asyncio.TimeoutError. They are
# the SAME class from Python 3.11 but DISTINCT on 3.9/3.10 — the version-safe
# case (v27), where catching only the builtin let the asyncio one escape as
# ceiling exhaustion.
@pytest.mark.parametrize("exc_type", [TimeoutError, asyncio.TimeoutError],
                         ids=["builtin", "asyncio"])
@pytest.mark.asyncio
async def test_chunk_transport_timeouterror_is_retried_not_ceiling_exhaustion(exc_type):
    t = _TimeoutChunkTransport(exc_type)
    collie = AsyncCollie(
        api_key="clai_test", base_url="http://test", project_id="proj_1",
        transport=t, max_retries=3, retry_max_per_chunk_s=100.0, _sleep=_instant,
    )
    try:
        # A transport timeout is retryable; with a generous ceiling the limiter
        # is max_retries=3 → THREE POSTs, then exhaustion. The bug produced ONE
        # POST (builtin TimeoutError caught by the ceiling's except).
        with pytest.raises(ChunkRetryExhausted):
            await collie._post_chunk(
                "/v1/jobs/j/chunks",
                {"sequence": 0, "content": "x", "is_final": False},
                sdk_origin="test",
            )
        assert t.posts == 3, f"expected 3 retried POSTs, got {t.posts}"
    finally:
        await collie.aclose()


class _ResetBody(httpx.AsyncByteStream):
    """A response body that raises mid-read (a socket dropped after the headers)."""
    async def __aiter__(self):
        raise httpx.ReadError("connection reset mid-body")
        yield b""  # pragma: no cover — makes this an async generator


class _ChunkErrorBodyResetTransport(httpx.AsyncBaseTransport):
    """A non-2xx chunk whose ERROR body resets mid-read; create OK."""
    def __init__(self) -> None:
        self.posts = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/chunks"):
            self.posts += 1
            return httpx.Response(400, stream=_ResetBody())
        return httpx.Response(202, json={"job_id": "job_stream"})


@pytest.mark.asyncio
async def test_chunk_error_body_read_failure_is_retried_not_fatal():
    # PBC-13: a 400 whose error body resets mid-read means the response never
    # fully arrived — a retryable transport failure, not a fatal HTTP 400. (The
    # v25 matrix wrongly claimed MockTransport couldn't model this.)
    t = _ChunkErrorBodyResetTransport()
    collie = AsyncCollie(
        api_key="clai_test", base_url="http://test", project_id="proj_1",
        transport=t, max_retries=2, retry_max_per_chunk_s=100.0, _sleep=_instant,
    )
    try:
        with pytest.raises(ChunkRetryExhausted):
            await collie._post_chunk(
                "/v1/jobs/j/chunks",
                {"sequence": 0, "content": "x", "is_final": False},
                sdk_origin="test",
            )
        assert t.posts == 2, f"expected 2 retried POSTs, got {t.posts}"
    finally:
        await collie.aclose()


class _CreateAsyncioTimeoutTransport(httpx.AsyncBaseTransport):
    """The (first) create POST raises asyncio.TimeoutError."""
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise asyncio.TimeoutError("transport timeout on create")


class _StatusAsyncioTimeoutTransport(httpx.AsyncBaseTransport):
    """Create OK; the status GET raises asyncio.TimeoutError."""
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            raise asyncio.TimeoutError("transport timeout on status")
        return httpx.Response(202, json={"job_id": "job_t", "status": "processing_inbound"})


@pytest.mark.asyncio
async def test_create_transport_asyncio_timeouterror_is_connection_error():
    """A transport asyncio.TimeoutError on job creation is a connection error,
    not a raw asyncio.TimeoutError (v28; 3.9/3.10 make it a DISTINCT class that
    the builtin-only catch missed)."""
    collie = AsyncCollie(api_key="clai_test", base_url="http://test",
                         transport=_CreateAsyncioTimeoutTransport(), _sleep=_instant)
    try:
        with pytest.raises(CollieConnectionError):
            await collie.moderate.input(prompt="x")
    finally:
        await collie.aclose()


@pytest.mark.parametrize("loop", ["moderation", "poll_job"])
@pytest.mark.asyncio
async def test_status_transport_asyncio_timeouterror_is_connection_not_poll_timeout(loop):
    """A transport asyncio.TimeoutError on the status GET is a connection error,
    NOT the flow's poll-timeout — the outer wait_for's except must not claim it
    (v28; both poll loops)."""
    collie = AsyncCollie(api_key="clai_test", base_url="http://test", project_id="proj_1",
                         transport=_StatusAsyncioTimeoutTransport(), _sleep=_instant)
    try:
        with pytest.raises(CollieConnectionError):
            if loop == "moderation":
                await collie.moderate.input(prompt="x", poll_interval_s=0.01)
            else:
                await collie._poll_job("job_t", sdk_origin="test",
                                       terminal=frozenset({"completed"}),
                                       failure=frozenset({"failed"}),
                                       poll_interval_s=0.01)
    finally:
        await collie.aclose()
