"""streaming.preflight + protect_stream(require_streaming=True)."""
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from collieai import (
    BufferedFallbackRequired,
    PlanNotEntitled,
    ProjectNotFound,
)
from conftest import body_of


def _future(seconds: int = 60) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _past() -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()


def make_preflight_handler(*, mode="streaming", behavior="stream", reason=None,
                           valid_until=None, count=None, rules=None):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/streaming/preflight":
            if count is not None:
                count["n"] += 1
            return httpx.Response(200, json={
                "mode": mode,
                "recommended_client_behavior": behavior,
                "project_id": "proj_1",
                "streaming_mode": "auto",
                "reason": reason,
                "reason_detail": reason,
                "valid_until": valid_until,
                "rules": rules or [],
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})
    return handler


# --------------------------------------------------------------------------
# preflight() verdicts
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_preflight_streaming(build_client):
    handler = make_preflight_handler(mode="streaming", behavior="stream", valid_until=_future())
    collie = build_client(handler)
    cap = await collie.streaming.preflight()
    assert cap.mode == "streaming"
    assert cap.recommended_client_behavior == "stream"
    assert cap.reason is None


@pytest.mark.asyncio
async def test_preflight_buffered(build_client):
    handler = make_preflight_handler(
        mode="buffered", behavior="buffer_then_show",
        reason="rule_requires_full_context", valid_until=_future(),
    )
    collie = build_client(handler)
    cap = await collie.streaming.preflight()
    assert cap.mode == "buffered"
    assert cap.recommended_client_behavior == "buffer_then_show"
    assert cap.reason == "rule_requires_full_context"


@pytest.mark.asyncio
async def test_preflight_unsupported(build_client):
    handler = make_preflight_handler(
        mode="unsupported", behavior="fail_fast", reason="project_not_found",
    )  # no valid_until (not cacheable)
    collie = build_client(handler)
    cap = await collie.streaming.preflight()
    assert cap.mode == "unsupported"
    assert cap.recommended_client_behavior == "fail_fast"
    assert cap.reason == "project_not_found"


@pytest.mark.asyncio
async def test_preflight_with_rule_breakdown(build_client):
    handler = make_preflight_handler(
        mode="streaming", behavior="stream", valid_until=_future(),
        rules=[{"rule_id": "r1", "rule_name": "Block secret", "rule_type": "regex",
                "decision": "block", "monitoring": False, "streaming_supported": True}],
    )
    collie = build_client(handler)
    cap = await collie.streaming.preflight()
    assert len(cap.rules) == 1
    assert cap.rules[0].streaming_supported is True


# --------------------------------------------------------------------------
# caching
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_preflight_caches_within_validity(build_client):
    count = {"n": 0}
    handler = make_preflight_handler(valid_until=_future(60), count=count)
    collie = build_client(handler)
    await collie.streaming.preflight()
    await collie.streaming.preflight()
    assert count["n"] == 1  # second call served from cache


@pytest.mark.asyncio
async def test_preflight_refetches_when_expired(build_client):
    count = {"n": 0}
    handler = make_preflight_handler(valid_until=_past(), count=count)
    collie = build_client(handler)
    await collie.streaming.preflight()
    await collie.streaming.preflight()
    assert count["n"] == 2  # expired -> re-fetched


@pytest.mark.asyncio
async def test_preflight_force_refresh_bypasses_cache(build_client):
    count = {"n": 0}
    handler = make_preflight_handler(valid_until=_future(60), count=count)
    collie = build_client(handler)
    await collie.streaming.preflight()
    await collie.streaming.preflight(force_refresh=True)
    assert count["n"] == 2


@pytest.mark.asyncio
async def test_force_refresh_unsupported_evicts_stale_cache(build_client):
    """force_refresh that flips streaming -> unsupported must evict the cached
    streaming verdict, not leave it behind."""
    state = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/streaming/preflight":
            state["n"] += 1
            if state["n"] == 1:
                return httpx.Response(200, json={
                    "mode": "streaming", "recommended_client_behavior": "stream",
                    "project_id": "proj_1", "valid_until": _future(60), "rules": []})
            return httpx.Response(200, json={
                "mode": "unsupported", "recommended_client_behavior": "fail_fast",
                "project_id": "proj_1", "reason": "project_not_found",
                "valid_until": None, "rules": []})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    assert (await collie.streaming.preflight()).mode == "streaming"             # cached
    assert (await collie.streaming.preflight(force_refresh=True)).mode == "unsupported"  # evicts
    assert (await collie.streaming.preflight()).mode == "unsupported"           # not stale
    assert state["n"] == 3  # third call re-fetched (cache was evicted)


# --------------------------------------------------------------------------
# protect_stream(require_streaming=True)
# --------------------------------------------------------------------------
def _factory(deltas, state=None):
    async def factory():
        if state is not None:
            state["calls"] += 1
        for d in deltas:
            yield d
    return factory


@pytest.mark.asyncio
async def test_require_streaming_buffered_raises_before_factory(build_client):
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/streaming/preflight":
            return httpx.Response(200, json={
                "mode": "buffered", "recommended_client_behavior": "buffer_then_show",
                "project_id": "proj_1", "streaming_mode": "buffered",
                "reason": "preset_buffered", "reason_detail": "buffered",
                "valid_until": _future(), "rules": [],
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    with pytest.raises(BufferedFallbackRequired):
        async for _ in collie.streaming.protect_stream(
            input="hi", raw_stream_factory=_factory(["x"], state), require_streaming=True,
        ):
            pass
    assert state["calls"] == 0  # provider never invoked


@pytest.mark.asyncio
async def test_require_streaming_unsupported_raises_preflight_error(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/streaming/preflight":
            return httpx.Response(200, json={
                "mode": "unsupported", "recommended_client_behavior": "fail_fast",
                "project_id": "proj_1", "streaming_mode": "auto",
                "reason": "plan_not_entitled", "reason_detail": "no entitlement",
                "valid_until": None, "rules": [],
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    with pytest.raises(PlanNotEntitled):
        async for _ in collie.streaming.protect_stream(
            input="hi", raw_stream_factory=_factory(["x"]), require_streaming=True,
        ):
            pass


@pytest.mark.asyncio
async def test_require_streaming_ok_streams(build_client):
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/streaming/preflight":
            return httpx.Response(200, json={
                "mode": "streaming", "recommended_client_behavior": "stream",
                "project_id": "proj_1", "streaming_mode": "auto",
                "reason": None, "reason_detail": None, "valid_until": _future(), "rules": [],
            })
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            return httpx.Response(202, json={"job_id": "job_mod" if body.get("inbound_only") else "job_stream"})
        if path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {
                "allowed": True, "blocked": False, "triggered_rules": []}})
        if path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            is_final = bool(body.get("is_final"))
            content = body.get("content", "")
            emits = [{"content": content, "blocked": False, "final": is_final, "triggered_rules": []}] if (content or is_final) else []
            return httpx.Response(200, json={"sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    from collieai import SafeDelta
    events = [e async for e in collie.streaming.protect_stream(
        input="hi", raw_stream_factory=_factory(["hello"], state), require_streaming=True,
    )]
    assert state["calls"] == 1
    assert "".join(e.text for e in events if isinstance(e, SafeDelta)) == "hello"


@pytest.mark.asyncio
async def test_default_does_not_preflight(build_client):
    """require_streaming=False (default) must not call the preflight endpoint."""
    seen = {"preflight": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/streaming/preflight":
            seen["preflight"] += 1
            return httpx.Response(200, json={"mode": "streaming", "recommended_client_behavior": "stream",
                                             "project_id": "proj_1", "valid_until": _future(), "rules": []})
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            return httpx.Response(202, json={"job_id": "job_mod" if body.get("inbound_only") else "job_stream"})
        if path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {
                "allowed": True, "blocked": False, "triggered_rules": []}})
        if path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            is_final = bool(body.get("is_final"))
            content = body.get("content", "")
            emits = [{"content": content, "blocked": False, "final": is_final, "triggered_rules": []}] if (content or is_final) else []
            return httpx.Response(200, json={"sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    async for _ in collie.streaming.protect_stream(input="hi", raw_stream_factory=_factory(["x"])):
        pass
    assert seen["preflight"] == 0


# --------------------------------------------------------------------------
# Forward compatibility of the preflight wire shape
# --------------------------------------------------------------------------
# The server adds fields to this response without an SDK release —
# `execution_role` on `rules[]` shipped 2026-07-26 and was modelled here a day
# later. A strict model would have turned every preflight into a validation
# error in between.
#
# Tolerance here is `ConfigDict(extra="ignore")`; in the Node SDK it is a
# hand-written allowlist mapper, and in .NET it is System.Text.Json's default
# Skip. THREE different mechanisms, none of them previously pinned, so one can
# regress without the others noticing — a zod migration or an AOT source-gen
# build would each break only their own.
#
# Modelling `execution_role` MOVED the frontier rather than closing it: the
# unknown thing is now a fifth ROLE, not an unmodelled field. That is why the
# field is typed `str` and not a `Literal` — see the model — and why the
# unknown-VALUE case below is pinned alongside the unknown-field one.


@pytest.mark.asyncio
async def test_an_unknown_rule_field_does_not_break_deserialization(build_client):
    collie = build_client(make_preflight_handler(rules=[{
        "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
        "decision": "mask", "monitoring": True,
        "streaming_supported": False, "fallback_reason": "monitor_mode",
        "execution_role": "stream_observed",      # live server-side today
        "some_future_field": {"nested": [1, 2, 3]},
    }]))

    cap = await collie.streaming.preflight()

    assert len(cap.rules) == 1
    assert cap.rules[0].rule_id == "r1"
    assert cap.rules[0].fallback_reason == "monitor_mode"
    assert cap.rules[0].execution_role == "stream_observed"


@pytest.mark.asyncio
async def test_a_role_this_sdk_has_never_heard_of_still_deserializes(build_client):
    """The field is `str`, not a `Literal`. A fifth server-side role must reach
    the caller verbatim so it can be handled as unknown, not raise here."""
    collie = build_client(make_preflight_handler(rules=[{
        "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
        "decision": "mask", "monitoring": True,
        "streaming_supported": False, "fallback_reason": None,
        "execution_role": "enforce_midflight_v2",
    }]))

    cap = await collie.streaming.preflight()

    assert cap.rules[0].execution_role == "enforce_midflight_v2"


@pytest.mark.asyncio
async def test_a_rule_from_an_old_server_has_no_role_either(build_client):
    """Round 3, T-04. The ABSENT-field case — an old server that has never
    heard of `execution_role` — was the one forward-compat scenario with no
    assertion in any SDK suite: a default of `"postflight_observed"` instead
    of `None` survived everything. Absence and explicit null must be the
    same answer."""
    collie = build_client(make_preflight_handler(rules=[{
        "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
        "decision": "mask", "monitoring": True,
        "streaming_supported": False, "fallback_reason": "monitor_mode",
        # no execution_role key at all
    }]))

    cap = await collie.streaming.preflight()

    assert cap.rules[0].execution_role is None


@pytest.mark.asyncio
async def test_the_wire_user_agent_names_this_sdk_and_version(build_client):
    """Round 3, T-05. Node pins VERSION==package.json==User-Agent and .NET
    pins assembly->Wire.SdkVersion->UserAgent; this suite pinned nothing on
    the wire. The package==constant half is structurally safe (hatch reads
    `_version.py`), so the format string is the only thing left to drift."""
    from collieai._version import __version__

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("user-agent")
        return make_preflight_handler()(request)

    collie = build_client(handler)
    await collie.streaming.preflight()

    assert seen["ua"] == f"collieai-python/{__version__}"


@pytest.mark.asyncio
async def test_an_unplannable_rule_reports_no_role(build_client):
    """Null is a real answer, not a missing one: the server sends it for a rule
    it could not plan at all. It must not become the empty string."""
    collie = build_client(make_preflight_handler(rules=[{
        "rule_id": "r1", "rule_name": "n", "rule_type": "who_knows",
        "decision": "mask", "monitoring": False,
        "streaming_supported": False, "fallback_reason": "unknown_rule_type",
        "execution_role": None,
    }]))

    cap = await collie.streaming.preflight()

    assert cap.rules[0].execution_role is None


@pytest.mark.asyncio
async def test_an_unknown_top_level_field_does_not_break_it_either(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "mode": "streaming", "recommended_client_behavior": "stream",
            "project_id": "proj_1", "streaming_mode": "auto",
            "reason": None, "reason_detail": None, "valid_until": None,
            "rules": [],
            "observer_summary": {"stream_observed": 2},
        })

    collie = build_client(handler)

    assert (await collie.streaming.preflight()).mode == "streaming"
