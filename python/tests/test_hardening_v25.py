"""Twenty-first-review follow-ups (v25): the chunk ceiling made an ABSOLUTE
wall-clock bound (not a per-phase httpx timeout), the transport timeout allowed
to pass through untouched (None / httpx.Timeout), huge-int knob validation, and
the shared verdict canon driven END-TO-END through the protect_stream wrapper."""
import asyncio
import json
import pathlib

import httpx
import pytest

from collieai import AsyncCollie, ChunkRetryExhausted, InputBlocked

_VERDICT_CASES = json.loads(
    (pathlib.Path(__file__).resolve().parents[2] / "conformance" / "verdict_cases.json").read_text()
)


# -- finding 1: the ceiling is an ABSOLUTE bound that cancels a hung POST -----

class _HangingTransport(httpx.AsyncBaseTransport):
    """Never responds — the request hangs until the SDK cancels it."""
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        raise AssertionError("unreachable")  # pragma: no cover


@pytest.mark.asyncio
async def test_hung_chunk_post_is_cancelled_by_the_absolute_ceiling():
    # A per-phase httpx timeout could not cap this (the connect phase alone
    # would get the full value); asyncio.wait_for(remaining) bounds the WHOLE
    # POST, so a hung request is cancelled at the ceiling. Real wall clock —
    # the ceiling is 50 ms, so the test resolves in ~50 ms.
    collie = AsyncCollie(
        api_key="clai_test", base_url="http://test", project_id="proj_1",
        transport=_HangingTransport(), retry_max_per_chunk_s=0.05,
    )
    try:
        with pytest.raises(ChunkRetryExhausted):
            await collie._post_chunk(
                "/v1/jobs/j/chunks",
                {"sequence": 0, "content": "x", "is_final": False},
                sdk_origin="test",
            )
    finally:
        await collie.aclose()


# -- finding 1: the transport timeout passes through untouched ----------------

@pytest.mark.parametrize("timeout", [None, httpx.Timeout(5.0), httpx.Timeout(connect=1.0, read=2.0, write=2.0, pool=1.0)])
def test_transport_timeout_configs_are_accepted(timeout):
    # The v24 numeric validation wrongly rejected timeout=None and
    # httpx.Timeout(...); they must pass through to httpx untouched.
    client = AsyncCollie(api_key="clai_test", base_url="http://test", timeout=timeout)
    assert client._timeout is timeout


# -- finding 6: a huge-int knob is a ValueError, not an OverflowError ---------

@pytest.mark.parametrize("kwargs", [
    {"retry_max_per_chunk_s": 10 ** 1000},
    {"base_backoff_s": 10 ** 1000, "max_backoff_s": 10 ** 1001},
    {"max_backoff_s": 10 ** 1000},
])
def test_huge_int_knob_raises_valueerror_not_overflow(kwargs):
    with pytest.raises(ValueError):
        AsyncCollie(api_key="clai_test", base_url="http://test", **kwargs)


# -- finding 5: the verdict canon gates the provider END-TO-END ---------------

def _canon_stream_handler(job: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = json.loads(request.content) if request.content else {}
            return httpx.Response(202, json={
                "job_id": "job_mod" if body.get("inbound_only") else "job_stream",
            })
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={**job, "job_id": "job_mod"})
        if path == "/v1/jobs/job_stream/chunks":
            body = json.loads(request.content)
            is_final = bool(body.get("is_final"))
            content = body.get("content", "")
            emits = ([{"content": content, "blocked": False, "final": is_final, "triggered_rules": []}]
                     if (content or is_final) else [])
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})
    return handler


@pytest.mark.parametrize("case", _VERDICT_CASES["cases"], ids=lambda c: c["name"])
@pytest.mark.asyncio
async def test_verdict_canon_gates_provider_end_to_end(case):
    async def _instant(_seconds: float) -> None:
        return None

    collie = AsyncCollie(
        api_key="clai_test", base_url="http://test", project_id="proj_1",
        transport=httpx.MockTransport(_canon_stream_handler(case["job"])),
        _sleep=_instant,
    )
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        yield "hello"

    try:
        events = [
            e async for e in collie.streaming.protect_stream(
                input="x", raw_stream_factory=factory,
            )
        ]
    finally:
        await collie.aclose()

    if case["expect"]["provider_runs"]:
        assert calls["n"] == 1, case["name"]
        assert not any(isinstance(e, InputBlocked) for e in events), case["name"]
    else:
        assert calls["n"] == 0, case["name"]
        assert isinstance(events[0], InputBlocked), case["name"]
