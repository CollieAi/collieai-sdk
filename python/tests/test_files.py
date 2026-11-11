"""File analysis (preview): the files client, attachments on moderate.input,
FileAnalysisUnavailable and ModerationTimeout. Test names
match the Node and .NET suites."""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import httpx
import pytest

from collieai import (
    CollieAPIError,
    FileAnalysisUnavailable,
    FileObject,
    ModerationError,
    ModerationTimeout,
)

from conftest import body_of, error_response
from test_poll_backoff import VClock, make_client, timed_job_handler

FILE = {
    "id": "file_0123456789abcdef0123456789abcdef", "object": "file", "bytes": 5, "sha256": "ab" * 32,
    "filename": "a.pdf", "declared_media_type": "application/pdf", "state": "uploaded", "retain": False,
    "created_at": "2026-11-10T12:00:00Z", "expires_at": "2026-11-10T13:00:00Z",
}
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def _job(status: str = "completed", **extra: Any) -> Dict[str, Any]:
    return {"job_id": "job_f", "status": status,
            "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []}, **extra}


def _jobs_handler(seen: List[httpx.Request], job: Dict[str, Any]):
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_f", "status": "processing_inbound"})
        return httpx.Response(200, json=job)
    return handler


# ------------------------------------------------------------------ files


async def test_upload_sends_multipart_with_a_length_and_retain(build_client, tmp_path):
    path = tmp_path / "a.pdf"
    path.write_bytes(b"%PDF-")
    seen: List[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={**FILE, "retain": True})

    collie = build_client(handler)
    got = await collie.files.upload(path, retain=True)
    assert isinstance(got, FileObject) and got.id == FILE["id"] and got.retain is True
    (req,) = seen
    assert (req.method, req.url.path) == ("POST", "/v1/files")
    assert req.headers["content-type"].startswith("multipart/form-data")
    assert int(req.headers["content-length"]) == len(req.content)  # the API refuses a body without one
    body = req.content
    assert b'name="file"; filename="a.pdf"' in body and b"%PDF-" in body
    assert b'name="retain"' in body and b"true" in body
    assert req.headers["x-collieai-sdk-origin"] == "files"


async def test_get_and_delete(build_client):
    seen: List[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204) if request.method == "DELETE" else httpx.Response(200, json=FILE)

    collie = build_client(handler)
    assert (await collie.files.get(FILE["id"])).state == "uploaded"
    assert await collie.files.delete(FILE["id"]) is None  # 204, no body
    assert [(r.method, r.url.path) for r in seen] == [("GET", f"/v1/files/{FILE['id']}"),
                                                      ("DELETE", f"/v1/files/{FILE['id']}")]


async def test_file_analysis_unavailable_on_its_code_only(build_client, tmp_path):
    path = tmp_path / "a.txt"
    path.write_bytes(b"x")
    collie = build_client(lambda r: error_response(422, "file_analysis_unavailable"))
    with pytest.raises(FileAnalysisUnavailable) as caught:
        await collie.files.upload(path)
    assert isinstance(caught.value, CollieAPIError) and caught.value.status_code == 422
    other = build_client(lambda r: error_response(422, "invalid_retain"))
    with pytest.raises(CollieAPIError) as caught:
        await other.files.upload(path)
    assert not isinstance(caught.value, FileAnalysisUnavailable)


async def test_a_malformed_file_object_is_a_typed_error(build_client):
    collie = build_client(lambda r: httpx.Response(200, json={"id": "x"}))
    with pytest.raises(CollieAPIError) as caught:
        await collie.files.get("x")
    assert caught.value.code == "invalid_response"


# ---------------------------------------------------------- attachments


async def test_attachments_carry_the_deadline_computed_before_the_post(build_client):
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job(file_result={"status": "rejected", "files": []})),
                          timeout=20.0, _utcnow=lambda: NOW)
    result = await collie.moderate.input("summarize", attachments=[FILE["id"]], timeout_s=10.0)
    body = body_of(seen[0])
    assert body["attachments"] == [{"file_id": FILE["id"]}]
    assert body["client_now"] == NOW.isoformat()
    assert body["client_deadline_at"] == (NOW + timedelta(seconds=30)).isoformat()  # request timeout + timeout_s
    assert body["message_input"] == "summarize" and "attachments_budget_s" not in body
    assert result.file_result == {"status": "rejected", "files": []}


@pytest.mark.parametrize("timeout", [None, httpx.Timeout(10.0, read=None)])
@pytest.mark.parametrize("timeout_s,budget_s", [(30.0, 30.0), (5.0, 5.0), (120.0, 90.0)])
async def test_an_unbounded_request_sends_the_poll_budget(build_client, timeout, timeout_s, budget_s):
    # No moment the call gives up, so no deadline; the poll budget is known
    # and goes as the relative budget, capped at the API's 90 s.
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job(file_result={"status": "rejected"})),
                          timeout=timeout, _utcnow=lambda: NOW)
    await collie.moderate.input("summarize", attachments=[FILE["id"]], timeout_s=timeout_s)
    body = body_of(seen[0])
    assert body["attachments"] == [{"file_id": FILE["id"]}]
    assert "client_deadline_at" not in body and "client_now" not in body
    assert body["attachments_budget_s"] == budget_s


async def test_the_attachments_are_copied_when_the_call_starts(build_client):
    # Emptying the caller's list mid-call must not switch off the check for
    # a server that ignored the attachments.
    files = [FILE["id"]]
    seen: List[httpx.Request] = []
    inner = _jobs_handler(seen, _job())

    def handler(request: httpx.Request) -> httpx.Response:
        files.clear()
        return inner(request)

    collie = build_client(handler)
    with pytest.raises(FileAnalysisUnavailable):
        await collie.moderate.input("hi", attachments=files)


async def test_without_attachments_the_body_is_unchanged(build_client):
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job()), _utcnow=lambda: NOW)
    result = await collie.moderate.input("hi")
    assert body_of(seen[0]) == {"message_input": "hi", "inbound_only": True}
    assert result.file_result is None


async def test_an_attachment_only_check(build_client):
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job(file_result={"status": "rejected"})), _utcnow=lambda: NOW)
    result = await collie.moderate.input(attachments=[FILE["id"]])
    body = body_of(seen[0])
    assert "message_input" not in body and body["inbound_only"] is True
    assert result.original_text == "" and result.allowed is True


@pytest.mark.parametrize("kwargs,exc", [
    ({}, ValueError),
    ({"attachments": []}, ValueError),
    ({"attachments": "file_x"}, TypeError),
    ({"attachments": [""]}, ValueError),
])
async def test_refused_before_any_request(build_client, kwargs, exc):
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job()))
    with pytest.raises(exc):
        await collie.moderate.input(**kwargs)
    assert seen == []


async def test_a_missing_file_result_is_file_analysis_unavailable(build_client):
    seen: List[httpx.Request] = []
    collie = build_client(_jobs_handler(seen, _job()))  # a server that ignored the attachments
    with pytest.raises(FileAnalysisUnavailable) as caught:
        await collie.moderate.input("hi", attachments=[FILE["id"]])
    assert caught.value.code == "file_result_missing"


async def test_attachments_refused_by_the_deployment(build_client):
    collie = build_client(lambda r: error_response(422, "file_analysis_unavailable"))
    with pytest.raises(FileAnalysisUnavailable):
        await collie.moderate.input("hi", attachments=[FILE["id"]])


# ---------------------------------------------------------------- timeout


async def test_sdk_timeout_raises_moderation_timeout_subclass(build_client, monkeypatch):
    """The SDK's own poll budget running out raises ModerationTimeout
    — a ModerationError, so existing handlers keep catching it — with job_id,
    on moderate.input and moderate.output alike."""
    clock = VClock()
    slow = timed_job_handler(clock, 60.0, job_id="job_slow")
    with pytest.raises(ModerationTimeout) as caught:
        await make_client(build_client, clock, slow).moderate.input(prompt="hi", timeout_s=5.0)
    assert isinstance(caught.value, ModerationError) and caught.value.job_id == "job_slow"
    assert "timed out" in str(caught.value)

    def out_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"job_id": "job_o", "status": "processing_outbound"})

    collie = build_client(out_handler)

    async def poll_times_out(*a, **k):
        raise CollieAPIError("Polling job job_o timed out after 5.0s", code="poll_timeout")

    monkeypatch.setattr(collie, "_poll_job", poll_times_out)
    with pytest.raises(ModerationTimeout) as caught:
        await collie.moderate.output(response="text", timeout_s=5.0)
    assert caught.value.job_id == "job_o" and isinstance(caught.value, ModerationError)
