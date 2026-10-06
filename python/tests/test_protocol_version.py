"""F9: X-CollieAi-Streaming-Protocol negotiation — the SDK warns once per
client when the server speaks a newer protocol; same-or-older versions and
malformed/missing headers stay silent."""
import warnings

import httpx
import pytest


def _job_backend(protocol_header=None):
    headers = {}
    if protocol_header is not None:
        headers["X-CollieAi-Streaming-Protocol"] = protocol_header

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_x"}, headers=headers)
        if request.url.path == "/v1/jobs/job_x/chunks":
            import json
            body = json.loads(request.content)
            return httpx.Response(200, headers=headers, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": [], "finished": bool(body.get("is_final")),
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    return handler


@pytest.mark.asyncio
async def test_newer_server_protocol_warns_exactly_once(build_client):
    collie = build_client(_job_backend(protocol_header="2"))
    session = collie.streaming.session(input="p")

    with pytest.warns(UserWarning) as record:
        await session.__aenter__()      # job create carries the header
        await session.push("a")         # would warn again without the latch
        await session.push("b")

    protocol_warnings = [
        w for w in record if "streaming protocol 2" in str(w.message)
    ]
    assert len(protocol_warnings) == 1
    assert "supports up to 1" in str(protocol_warnings[0].message)


@pytest.mark.asyncio
@pytest.mark.parametrize("header", [None, "1", "0", "garbage", "1.5"])
async def test_same_older_missing_or_malformed_is_silent(build_client, header):
    collie = build_client(_job_backend(protocol_header=header))
    session = collie.streaming.session(input="p")

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning fails the test
        await session.__aenter__()
        await session.push("a")


@pytest.mark.asyncio
async def test_sse_response_header_also_warns(build_client):
    """A mixed-fleet rollout can serve the SSE from a newer pod than the one
    that handled job create — the SSE response itself must be checked too."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_x"})  # old pod: no header
        if request.url.path == "/v1/jobs/job_x/stream":
            return httpx.Response(
                200,
                headers={"X-CollieAi-Streaming-Protocol": "2"},  # newer pod
                content=b'event: end\ndata: {"reason": "final"}\n\n',
            )
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})

    collie = build_client(handler)
    session = collie.streaming.session(input="p")
    await session.__aenter__()

    with pytest.warns(UserWarning, match="streaming protocol 2"):
        async for _event in session.stream_events():
            pass
