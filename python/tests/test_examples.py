"""F10 (plan §"Test Matrix" — example tests): drive examples/fastapi_app.py
end-to-end through an ASGI transport with a mocked CollieAi backend and a
fake (duck-typed) OpenAI client. Asserts the example's product promise:

- the HTTP response contains ONLY SDK-released text, never raw provider text;
- blocked output stops the app stream with the block message;
- blocked input never invokes the provider.

Requires fastapi (and nothing else beyond the SDK): skipped where absent.
"""
from types import SimpleNamespace

import httpx
import pytest

pytest.importorskip("fastapi")

from test_protect_stream import make_backend  # noqa: E402 — reuse the mock backend


class FakeOpenAIClient:
    """Duck-typed stand-in for openai.AsyncOpenAI — the adapter only touches
    `chat.completions.create(..., stream=True)` and the chunk shape."""

    def __init__(self, deltas):
        self.create_calls = 0
        outer = self

        class _Completions:
            async def create(self, **kwargs):
                outer.create_calls += 1
                assert kwargs.get("stream") is True

                async def _stream():
                    for text in deltas:
                        yield SimpleNamespace(
                            choices=[SimpleNamespace(delta=SimpleNamespace(content=text))]
                        )

                return _stream()

        self.chat = SimpleNamespace(completions=_Completions())


@pytest.fixture
def example_app(monkeypatch, build_client):
    """Import the example app and rewire its module globals to fakes."""
    monkeypatch.setenv("COLLIEAI_API_KEY", "clai_test")
    monkeypatch.setenv("COLLIEAI_PROJECT_ID", "proj_1")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    fastapi_app = pytest.importorskip("examples.fastapi_app")

    def _wire(provider_deltas, **backend_opts):
        handler, _state = make_backend(**backend_opts)
        fake_provider = FakeOpenAIClient(provider_deltas)
        monkeypatch.setattr(fastapi_app, "collie", build_client(handler))
        monkeypatch.setattr(fastapi_app, "openai_client", fake_provider)
        return fastapi_app.app, fake_provider

    return _wire


async def _post_chat(app, prompt="hi"):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://example") as hc:
        return await hc.post("/chat", json={"prompt": prompt})


@pytest.mark.asyncio
async def test_example_streams_only_sdk_safe_deltas(example_app):
    """Raw provider text must never reach the HTTP response — only the
    backend-released (here: masked-to-SAFE) text does."""
    app, provider = example_app(["Hello ", "RAW-SECRET tail"], mask_to="SAFE")

    resp = await _post_chat(app)

    assert resp.status_code == 200
    assert "RAW" not in resp.text and "Hello" not in resp.text
    assert resp.text != "" and resp.text.replace("SAFE", "") == ""
    assert provider.create_calls == 1


@pytest.mark.asyncio
async def test_example_blocked_output_stops_stream_with_message(example_app):
    app, _provider = example_app(["something disallowed"], block_output=True)

    resp = await _post_chat(app)

    assert resp.text == "Custom rule block message"


@pytest.mark.asyncio
async def test_example_blocked_input_never_calls_provider(example_app):
    app, provider = example_app(["never generated"], input_blocked=True)

    resp = await _post_chat(app)

    assert resp.text == "input blocked"
    assert provider.create_calls == 0
