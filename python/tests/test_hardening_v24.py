"""Twentieth-review follow-ups (v24): the verdict fail-safe, chunk-ceiling and
option-validation fixes the .NET SDK got in v22/v23, brought to Python parity —
plus the SHARED cross-SDK verdict canon so the same defect can't reappear in one
language after being fixed in another."""
import json
import pathlib

import httpx
import pytest

from collieai import (
    ChunkRetryExhausted,
    InputBlocked,
)
from conftest import body_of

_VERDICT_CASES = json.loads(
    (pathlib.Path(__file__).resolve().parents[2] / "conformance" / "verdict_cases.json").read_text()
)


def _verdict_handler(job: dict):
    """POST /v1/jobs -> a created inbound job; GET -> the canonical job JSON."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_v", "status": "processing_inbound"})
        if request.method == "GET" and request.url.path == "/v1/jobs/job_v":
            return httpx.Response(200, json={**job, "job_id": "job_v"})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})
    return handler


# -- finding 1 (+6): shared cross-SDK verdict-resolution canon ---------------

@pytest.mark.parametrize("case", _VERDICT_CASES["cases"], ids=lambda c: c["name"])
@pytest.mark.asyncio
async def test_verdict_canon_matches_the_shared_contract(build_client, case):
    """Every vector in sdk/conformance/verdict_cases.json resolves to the same
    fail-safe verdict here as in the Node/.NET replays — an ambiguous/empty
    inbound_result or a root context block never fail-opens."""
    collie = build_client(_verdict_handler(case["job"]))
    result = await collie.moderate.input(prompt="x", poll_interval_s=0.01)
    expect = case["expect"]
    assert result.allowed is expect["allowed"], case["name"]
    assert result.blocked is expect["blocked"], case["name"]
    if "block_message" in expect:
        assert result.block_message == expect["block_message"], case["name"]


# -- finding 1: the streaming wrapper gates on the fail-safe verdict ----------

@pytest.mark.asyncio
async def test_empty_inbound_result_blocks_the_provider(build_client):
    """A completed job with an EMPTY inbound_result must NOT run the provider —
    the old `allowed = inbound.get("allowed", not blocked)` fail-opened it."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_mod"})
        if request.method == "GET" and request.url.path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {}})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        yield "never"

    events = [e async for e in collie.streaming.protect_stream(input="hi", raw_stream_factory=factory)]
    assert calls["n"] == 0
    assert len(events) == 1
    assert isinstance(events[0], InputBlocked)


# -- finding 2: a 2xx chunk that lands after the ceiling is rejected ----------

@pytest.mark.asyncio
async def test_late_success_chunk_is_rejected(build_client):
    """A single chunk POST whose landing overruns the retry ceiling must raise
    ChunkRetryExhausted, not return the stale success (parity with .NET/Node)."""
    mono = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.url.path == "/v1/jobs/job_stream/chunks":
            mono[0] = 0.2  # elapsed jumps past the 0.1 s ceiling mid-flight
            body = body_of(request)
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True, "emits": [], "finished": True,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler, retry_max_per_chunk_s=0.1, _monotonic=lambda: mono[0])
    with pytest.raises(ChunkRetryExhausted):
        async with collie.streaming.session(input="prompt") as session:
            await session.push("hello")


# -- finding 5: numeric options validated at construction --------------------

@pytest.mark.parametrize("kwargs", [
    {"max_retries": 0},
    {"max_retries": 1.5},
    {"max_retries": True},          # bool is an int subclass — reject it
    {"retry_max_per_chunk_s": -1},
    {"retry_max_per_chunk_s": 0},
    {"base_backoff_s": -1},
    {"max_backoff_s": float("inf")},
    {"base_backoff_s": 2.0, "max_backoff_s": 1.0},  # max < base
    # `timeout` is DELIBERATELY not validated (v25) — it's the httpx transport
    # timeout and must pass through, so no {"timeout": ...} cases here.
])
def test_bad_numeric_option_is_rejected_at_construction(build_client, kwargs):
    with pytest.raises(ValueError):
        build_client(lambda request: httpx.Response(200, json={}), **kwargs)
