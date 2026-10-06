"""moderate.input: verdict mapping, polling, failure handling, and request
metadata/attribution."""
import httpx
import pytest

from collieai import CollieAPIError, CollieConnectionError, ModerationError
from conftest import body_of


def make_mod_handler(*, statuses, inbound_result=None, job_id="job_m", capture=None,
                     context_result=None, blocked_by=None):
    """Backend that returns `statuses` on successive GETs (last one repeats),
    attaching `inbound_result` (+ optional context_result/blocked_by) once
    terminal."""
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            if capture is not None:
                capture["create_body"] = body_of(request)
                capture["origin"] = request.headers.get("X-CollieAi-SDK-Origin")
            return httpx.Response(202, json={"job_id": job_id, "status": "processing_inbound"})
        if request.method == "GET" and path == f"/v1/jobs/{job_id}":
            idx = min(state["gets"], len(statuses) - 1)
            state["gets"] += 1
            status = statuses[idx]
            payload = {"job_id": job_id, "status": status, "created_at": "2026-01-01T00:00:00Z"}
            if status in ("completed", "inbound_blocked"):
                payload["inbound_result"] = inbound_result or {}
                if context_result is not None:
                    payload["context_result"] = context_result
                if blocked_by is not None:
                    payload["blocked_by"] = blocked_by
            return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    handler.state = state
    return handler


@pytest.mark.asyncio
async def test_input_allowed(build_client):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "filtered_content": "hello", "triggered_rules": []},
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="hello")
    assert result.allowed is True
    assert result.blocked is False
    assert result.original_text == "hello"
    assert result.filtered_text == "hello"
    assert result.job_id == "job_m"


@pytest.mark.asyncio
async def test_input_blocked(build_client):
    handler = make_mod_handler(
        statuses=["inbound_blocked"],
        inbound_result={
            "allowed": False, "blocked": True, "block_message": "PII detected",
            "triggered_rules": [{"rule_id": "r1", "rule_name": "PII", "rule_type": "regex", "decision": "block"}],
        },
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="my ssn is 123")
    assert result.blocked is True
    assert result.allowed is False
    assert result.block_message == "PII detected"
    assert len(result.triggered_rules) == 1
    assert result.triggered_rules[0].rule_name == "PII"


@pytest.mark.asyncio
async def test_input_sends_structured_context(build_client):
    """Structured context is sent on the job-create body (as JSON)."""
    capture = {}
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.input(
        prompt="summarize my last payment",
        context={"transaction": {"title": "hi"}},
        context_format="json",
    )
    body = capture["create_body"]
    assert body["message_input"] == "summarize my last payment"
    assert body["context"] == {"transaction": {"title": "hi"}}
    assert body["context_format"] == "json"


@pytest.mark.asyncio
async def test_input_string_context_with_format(build_client):
    capture = {}
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.input(prompt="hi", context="raw blob", context_format="text")
    body = capture["create_body"]
    assert body["context"] == "raw blob"
    assert body["context_format"] == "text"


@pytest.mark.asyncio
async def test_input_omits_context_when_absent(build_client):
    """No context -> the field is absent (omitted = policy default applies)."""
    capture = {}
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.input(prompt="hello")
    body = capture["create_body"]
    assert "context" not in body
    assert "context_format" not in body


@pytest.mark.asyncio
async def test_input_rejects_non_finite_context(build_client):
    """NaN/Infinity isn't strict JSON -> rejected locally (cross-SDK parity)."""
    import math

    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", context={"amount": math.nan})


@pytest.mark.asyncio
async def test_input_rejects_non_string_dict_key(build_client):
    """A non-string key would be coerced (1 -> '1'), corrupting attribution."""
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", context={1: "x"})


@pytest.mark.asyncio
async def test_input_rejects_unsupported_type(build_client):
    """Non-JSON Python objects (e.g. a set) are rejected, not silently mangled."""
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", context={"tags": {1, 2, 3}})


@pytest.mark.asyncio
async def test_input_polls_until_terminal(build_client):
    handler = make_mod_handler(
        statuses=["processing_inbound", "processing_inbound", "completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="hi", poll_interval_s=0.01)
    assert result.allowed is True
    assert handler.state["gets"] == 3


@pytest.mark.asyncio
async def test_input_failed_raises(build_client):
    handler = make_mod_handler(statuses=["failed"])
    collie = build_client(handler)
    with pytest.raises(ModerationError):
        await collie.moderate.input(prompt="hi")


@pytest.mark.asyncio
async def test_input_sends_metadata_origin_and_no_webhook(build_client):
    capture: dict = {}
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.input(
        prompt="hi", conversation_id="conv-1", correlation_id="turn-1",
    )
    body = capture["create_body"]
    assert body["message_input"] == "hi"
    assert body["inbound_only"] is True
    assert body["conversation_id"] == "conv-1"
    assert body["correlation_id"] == "turn-1"
    assert "webhook_url" not in body          # never send a placeholder
    assert capture["origin"] == "moderate.input"


@pytest.mark.asyncio
async def test_empty_ids_are_omitted(build_client):
    capture: dict = {}
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.input(prompt="hi", conversation_id="", correlation_id="")
    body = capture["create_body"]
    assert "conversation_id" not in body
    assert "correlation_id" not in body


@pytest.mark.asyncio
async def test_project_id_mismatch_raises(build_client):
    handler = make_mod_handler(statuses=["completed"], inbound_result={})
    collie = build_client(handler)  # client project_id is "proj_1"
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", project_id="other_project")


@pytest.mark.asyncio
async def test_transport_error_wrapped_as_collie_error(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("upstream down")

    collie = build_client(handler)
    with pytest.raises(CollieConnectionError):
        await collie.moderate.input(prompt="hi")


@pytest.mark.asyncio
async def test_per_call_project_id_without_client_project_raises(build_client):
    handler = make_mod_handler(statuses=["completed"], inbound_result={})
    collie = build_client(handler, project_id=None)  # client has no project
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", project_id="proj")


@pytest.mark.asyncio
async def test_matching_per_call_project_id_is_accepted(build_client):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)  # client project_id == "proj_1"
    result = await collie.moderate.input(prompt="hi", project_id="proj_1")
    assert result.allowed is True


@pytest.mark.asyncio
async def test_non_object_poll_body_fails_fast(build_client):
    """A valid-JSON-but-non-object poll response is a typed error, not an
    indefinite poll until timeout."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "job_m"})
        return httpx.Response(200, json=["unexpected", "array"])

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi", timeout_s=5.0)
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_empty_create_body_fails_fast(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202)  # 2xx, no body

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi")
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_empty_poll_body_fails_fast(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "job_m"})
        return httpx.Response(200)  # empty poll body

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi", timeout_s=5.0)
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_malformed_inbound_result_raises_api_error(build_client):
    handler = make_mod_handler(statuses=["completed"], inbound_result="not-an-object")
    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi")
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_create_without_job_id_raises_api_error(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"status": "processing_inbound"})  # no job_id

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi")
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_missing_inbound_result_raises_api_error(build_client):
    """A terminal job with no inbound_result is malformed, not 'allowed'."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "job_m"})
        return httpx.Response(200, json={"status": "completed"})  # no inbound_result

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi")
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_malformed_triggered_rules_raises_api_error(build_client):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": False, "blocked": True, "triggered_rules": ["not-a-dict"]},
    )
    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.moderate.input(prompt="hi")
    assert ei.value.code == "invalid_response"


# ---------------------------------------------------------------------------
# Context analysis typed result
# ---------------------------------------------------------------------------

CONTEXT_STATUSES = [
    "not_provided", "disabled", "not_run", "clean", "monitored", "blocked", "degraded",
]


@pytest.mark.parametrize("status", CONTEXT_STATUSES)
@pytest.mark.asyncio
async def test_context_status_parsed_for_every_enum(build_client, status):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        context_result={"status": status, "blocked": status == "blocked"},
        blocked_by="context" if status == "blocked" else "none",
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="hi", context={"a": "b"})
    assert result.context is not None
    assert result.context.status == status
    assert result.blocked_by == ("context" if status == "blocked" else "none")


@pytest.mark.asyncio
async def test_context_block_detail(build_client):
    handler = make_mod_handler(
        statuses=["inbound_blocked"],
        inbound_result={
            "allowed": False, "blocked": True, "block_message": "ctx blocked",
            "triggered_rules": [],
        },
        context_result={
            "status": "blocked", "blocked": True, "block_message": "ctx blocked",
            "triggering_pointer": "/transaction/title", "triggering_rule_id": "r1",
            "triggering_rule_type": "lightweight_model",
        },
        blocked_by="context",
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="summarize", context={"transaction": {"title": "x"}})
    assert result.blocked is True               # aggregate inbound verdict
    assert result.blocked_by == "context"
    assert result.context.status == "blocked"
    assert result.context.triggering_pointer == "/transaction/title"
    assert result.context.triggering_rule_type == "lightweight_model"


@pytest.mark.asyncio
async def test_context_degraded_markers(build_client):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        context_result={
            "status": "degraded", "parse_degraded": False,
            "limit_exceeded": True, "inference_degraded": True,
        },
        blocked_by="none",
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="hi", context={"a": "b"})
    assert result.context.status == "degraded"
    assert result.context.limit_exceeded is True
    assert result.context.inference_degraded is True


@pytest.mark.asyncio
async def test_no_context_result_means_none(build_client):
    handler = make_mod_handler(
        statuses=["completed"],
        inbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.context is None
    assert result.blocked_by is None


# --- moderate.output --------------------------------------------------------

def make_out_handler(*, statuses, outbound_result=None, job_id="job_o",
                     capture=None, create_json=None):
    """Backend for message_output-only jobs: returns `statuses` on successive
    GETs (last repeats), attaching `outbound_result` once terminal.
    ``create_json`` overrides the job-create response body."""
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            if capture is not None:
                capture["create_body"] = body_of(request)
                capture["origin"] = request.headers.get("X-CollieAi-SDK-Origin")
            payload = create_json or {"job_id": job_id, "status": "processing_outbound"}
            return httpx.Response(202, json=payload)
        if request.method == "GET" and path == f"/v1/jobs/{job_id}":
            idx = min(state["gets"], len(statuses) - 1)
            state["gets"] += 1
            status = statuses[idx]
            payload = {"job_id": job_id, "status": status, "created_at": "2026-01-01T00:00:00Z"}
            if status in ("completed", "outbound_blocked") and outbound_result is not None:
                payload["outbound_result"] = outbound_result
            return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    handler.state = state
    return handler


@pytest.mark.asyncio
async def test_output_allowed_carries_filtered_text(build_client):
    """Masked output rides filtered_text — the text the caller must send."""
    handler = make_out_handler(
        statuses=["completed"],
        outbound_result={
            "allowed": True, "blocked": False,
            "filtered_content": "your card is [MASKED]", "triggered_rules": [
                {"rule_id": "r9", "rule_name": "pii-mask", "rule_type": "regex", "decision": "mask"},
            ],
        },
    )
    collie = build_client(handler)
    result = await collie.moderate.output(response="your card is 4111-1111")
    assert result.allowed is True
    assert result.blocked is False
    assert result.original_text == "your card is 4111-1111"
    assert result.filtered_text == "your card is [MASKED]"
    assert result.triggered_rules[0].rule_name == "pii-mask"
    assert result.job_id == "job_o"


@pytest.mark.asyncio
async def test_output_blocked(build_client):
    handler = make_out_handler(
        statuses=["outbound_blocked"],
        outbound_result={
            "allowed": False, "blocked": True, "block_message": "Response blocked",
            "triggered_rules": [
                {"rule_id": "r1", "rule_name": "leak", "rule_type": "regex", "decision": "block"},
            ],
        },
    )
    collie = build_client(handler)
    result = await collie.moderate.output(response="the FORBIDDEN text")
    assert result.blocked is True
    assert result.allowed is False
    assert result.block_message == "Response blocked"


@pytest.mark.parametrize(
    ("status", "outbound_result", "expect_allowed"),
    [
        # The v24 fail-safe table (sdk-plan § Output Moderation), row by row.
        ("completed", {"allowed": True, "blocked": False}, True),
        # Empty result object: ambiguous -> blocked, never fail-open.
        ("completed", {}, False),
        # `allowed` absent with blocked False: still ambiguous -> blocked.
        ("completed", {"blocked": False}, False),
        # Explicit allowed False without blocked: hard block.
        ("completed", {"allowed": False}, False),
        # Terminal status says blocked; a contradicting body must not win.
        ("outbound_blocked", {"allowed": True, "blocked": False}, False),
        # allowed must be a real bool True, not a truthy value.
        ("completed", {"allowed": 1, "blocked": False}, False),
    ],
)
@pytest.mark.asyncio
async def test_output_verdict_fail_safe_table(build_client, status, outbound_result, expect_allowed):
    handler = make_out_handler(statuses=[status], outbound_result=outbound_result)
    collie = build_client(handler)
    result = await collie.moderate.output(response="text")
    assert result.allowed is expect_allowed
    assert result.blocked is (not expect_allowed)


@pytest.mark.asyncio
async def test_output_missing_outbound_result_raises_api_error(build_client):
    """A terminal job without its result object is malformed — typed error,
    never an implicit allow."""
    handler = make_out_handler(statuses=["completed"], outbound_result=None)
    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as excinfo:
        await collie.moderate.output(response="text")
    assert excinfo.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_output_malformed_triggered_rules_raises_api_error(build_client):
    handler = make_out_handler(
        statuses=["completed"],
        outbound_result={"allowed": True, "blocked": False, "triggered_rules": ["not-a-rule"]},
    )
    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as excinfo:
        await collie.moderate.output(response="text")
    assert excinfo.value.code == "invalid_response"


@pytest.mark.parametrize("dead_status", ["failed", "expired"])
@pytest.mark.asyncio
async def test_output_dead_job_raises_moderation_error(build_client, dead_status):
    """Same error taxonomy as moderate.input: one `except ModerationError`
    must cover both methods' died-without-verdict case."""
    handler = make_out_handler(statuses=[dead_status])
    collie = build_client(handler)
    with pytest.raises(ModerationError):
        await collie.moderate.output(response="text")


@pytest.mark.asyncio
async def test_output_poll_timeout_maps_to_moderation_error(build_client, monkeypatch):
    handler = make_out_handler(statuses=["processing_outbound"])
    collie = build_client(handler)

    async def fake_poll_job(*a, **k):
        raise CollieAPIError("Polling job job_o timed out after 30.0s", code="poll_timeout")

    monkeypatch.setattr(collie, "_poll_job", fake_poll_job)
    with pytest.raises(ModerationError):
        await collie.moderate.output(response="text")


@pytest.mark.asyncio
async def test_output_sends_message_output_only_with_origin(build_client):
    """The create body is message_output-only: no message_input, no
    inbound_only, and structurally no context — plus the new origin value."""
    capture = {}
    handler = make_out_handler(
        statuses=["completed"],
        outbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.output(
        response="hello", conversation_id="conv1", correlation_id="turn1"
    )
    body = capture["create_body"]
    assert body["message_output"] == "hello"
    assert "message_input" not in body
    assert "inbound_only" not in body
    assert "context" not in body
    assert "webhook_url" not in body
    assert body["conversation_id"] == "conv1"
    assert body["correlation_id"] == "turn1"
    assert capture["origin"] == "moderate.output"


@pytest.mark.asyncio
async def test_output_has_no_context_parameter(build_client):
    """The no-context decision is structural: passing context is a TypeError,
    not a silently dropped kwarg."""
    handler = make_out_handler(
        statuses=["completed"],
        outbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
    )
    collie = build_client(handler)
    with pytest.raises(TypeError):
        await collie.moderate.output(response="hello", context={"a": "b"})


@pytest.mark.asyncio
async def test_output_empty_ids_are_omitted(build_client):
    capture = {}
    handler = make_out_handler(
        statuses=["completed"],
        outbound_result={"allowed": True, "blocked": False, "triggered_rules": []},
        capture=capture,
    )
    collie = build_client(handler)
    await collie.moderate.output(response="hello", conversation_id="", correlation_id="")
    body = capture["create_body"]
    assert "conversation_id" not in body
    assert "correlation_id" not in body


@pytest.mark.asyncio
async def test_output_project_id_mismatch_raises(build_client):
    handler = make_out_handler(statuses=["completed"])
    collie = build_client(handler, project_id="proj_A")
    with pytest.raises(ValueError):
        await collie.moderate.output(response="hello", project_id="proj_B")


@pytest.mark.asyncio
async def test_output_create_without_job_id_raises_api_error(build_client):
    handler = make_out_handler(statuses=["completed"], create_json={"status": "accepted"})
    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as excinfo:
        await collie.moderate.output(response="hello")
    assert excinfo.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_output_invalid_knobs_fail_before_any_request(build_client):
    """Knob validation precedes job creation — no side effects on bad input."""
    capture = {}
    handler = make_out_handler(statuses=["completed"], capture=capture)
    collie = build_client(handler)
    with pytest.raises(ValueError):
        await collie.moderate.output(response="hello", timeout_s=-1)
    assert "create_body" not in capture
