"""Shared test helpers: an AsyncCollie wired to an httpx.MockTransport, plus a
programmable fake backend for the chunk/job endpoints."""
from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional

import httpx
import pytest
import pytest_asyncio

from collieai import AsyncCollie


async def _instant_sleep(_seconds: float) -> None:
    """No-op sleep so retry tests don't wait in real time."""
    return None


@pytest_asyncio.fixture
async def build_client():
    clients: List[AsyncCollie] = []

    def _build(handler: Callable[[httpx.Request], httpx.Response], **kwargs: Any) -> AsyncCollie:
        project_id = kwargs.pop("project_id", "proj_1")
        sleep = kwargs.pop("_sleep", _instant_sleep)  # overridable (virtual clocks)
        client = AsyncCollie(
            api_key="clai_test",
            base_url="http://test",
            project_id=project_id,
            transport=httpx.MockTransport(handler),
            _sleep=sleep,
            **kwargs,
        )
        clients.append(client)
        return client

    yield _build
    for client in clients:
        await client.aclose()


def body_of(request: httpx.Request) -> Dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content)


class FakeChunkBackend:
    """Simulates POST /v1/jobs (create) and POST /v1/jobs/{id}/chunks.

    Each submitted chunk echoes its content as a single safe emit. ``finished``
    is True for the final chunk. Records every chunk body for assertions.
    """

    def __init__(self, job_id: str = "job_test") -> None:
        self.job_id = job_id
        self.created_jobs: List[Dict[str, Any]] = []
        self.chunk_requests: List[Dict[str, Any]] = []
        self.origins: List[Optional[str]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            self.created_jobs.append(body_of(request))
            self.origins.append(request.headers.get("X-CollieAi-SDK-Origin"))
            return httpx.Response(202, json={"job_id": self.job_id, "status": "processing_inbound"})
        if request.method == "POST" and path == f"/v1/jobs/{self.job_id}/chunks":
            body = body_of(request)
            self.chunk_requests.append(body)
            self.origins.append(request.headers.get("X-CollieAi-SDK-Origin"))
            is_final = bool(body.get("is_final"))
            return httpx.Response(200, json={
                "sequence": body["sequence"],
                "accepted": True,
                "emits": [{
                    "content": body.get("content", ""),
                    "blocked": False,
                    "final": is_final,
                    "triggered_rules": [],
                }],
                "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "not found", "type": "not_found"}})


def error_response(status: int, code: str, message: str = "err", **headers: str) -> httpx.Response:
    return httpx.Response(
        status,
        headers=headers,
        json={"error": {"message": message, "type": code, "code": code}},
    )
