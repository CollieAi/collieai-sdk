"""collieai.adapters: OpenAI + Anthropic stream adapters (text extraction,
stream cleanup, lazy factories, composition with protect_stream)."""
import json
from types import SimpleNamespace

import httpx
import pytest

from collieai import SafeDelta
from collieai.adapters.anthropic import anthropic_factory, anthropic_text_deltas
from collieai.adapters.openai import openai_factory, openai_text_deltas


async def _aiter(items):
    for it in items:
        yield it


class _ClosableStream:
    """Async iterable that tracks aclose()/close() — like a provider stream."""

    def __init__(self, events, close_attr="aclose"):
        self._events = events
        self.closed = False
        setattr(self, close_attr, self._mark)

    async def _mark(self):
        self.closed = True

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for e in self._events:
            yield e


def _echo_handler(request: httpx.Request) -> httpx.Response:
    """Minimal backend: input check passes; chunks echo content as safe emits."""
    path = request.url.path
    if request.method == "POST" and path == "/v1/jobs":
        body = json.loads(request.content)
        return httpx.Response(202, json={
            "job_id": "job_mod" if body.get("inbound_only") else "job_stream"})
    if path == "/v1/jobs/job_mod":
        return httpx.Response(200, json={"status": "completed", "inbound_result": {
            "allowed": True, "blocked": False, "triggered_rules": []}})
    if path == "/v1/jobs/job_stream/chunks":
        body = json.loads(request.content)
        is_final = bool(body.get("is_final"))
        content = body.get("content", "")
        emits = [{"content": content, "blocked": False, "final": is_final, "triggered_rules": []}] if (content or is_final) else []
        return httpx.Response(200, json={"sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final})
    return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})


# --------------------------------------------------------------------------
# OpenAI
# --------------------------------------------------------------------------
def _oai_chunk(content):
    return SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=content))])


@pytest.mark.asyncio
async def test_openai_text_deltas_extracts_and_skips():
    events = [
        _oai_chunk(None),                # role-only delta
        _oai_chunk("Hello"),
        _oai_chunk(""),                   # empty content
        _oai_chunk(", world"),
        SimpleNamespace(choices=[]),      # usage / no-choices chunk
    ]
    out = [t async for t in openai_text_deltas(_aiter(events))]
    assert out == ["Hello", ", world"]


@pytest.mark.asyncio
async def test_openai_text_deltas_closes_on_early_exit():
    stream = _ClosableStream([_oai_chunk("a"), _oai_chunk("b")])
    gen = openai_text_deltas(stream)
    assert await gen.__anext__() == "a"
    await gen.aclose()
    assert stream.closed is True


@pytest.mark.asyncio
async def test_openai_factory_lazy_and_composes(build_client):
    opened = {"n": 0}

    class _Client:
        def __init__(self):
            async def create(**kwargs):
                opened["n"] += 1
                return _aiter([_oai_chunk("Hel"), _oai_chunk("lo")])
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))

    factory = openai_factory(_Client(), model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}])
    assert opened["n"] == 0  # building the factory opens nothing

    collie = build_client(_echo_handler)
    events = [e async for e in collie.streaming.protect_stream(input="hi", raw_stream_factory=factory)]
    assert "".join(e.text for e in events if isinstance(e, SafeDelta)) == "Hello"
    assert opened["n"] == 1


@pytest.mark.asyncio
async def test_openai_factory_closes_stream_on_early_exit():
    stream = _ClosableStream([_oai_chunk("a")])

    class _Client:
        def __init__(self):
            async def create(**kwargs):
                return stream
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))

    gen = openai_factory(_Client(), model="m", messages=[])()
    assert await gen.__anext__() == "a"
    await gen.aclose()
    assert stream.closed is True


@pytest.mark.asyncio
async def test_openai_factory_forces_stream_over_caller_kwargs():
    # A caller-supplied stream=False must be overridden (not raise a
    # duplicate-keyword TypeError when iteration starts).
    seen = {}

    class _Client:
        def __init__(self):
            async def create(**kwargs):
                seen.update(kwargs)
                return _aiter([_oai_chunk("x")])
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))

    factory = openai_factory(_Client(), model="m", messages=[], stream=False)
    out = [t async for t in factory()]
    assert out == ["x"]
    assert seen["stream"] is True  # forced on


# --------------------------------------------------------------------------
# Anthropic
# --------------------------------------------------------------------------
def _anthropic_text(text):
    return SimpleNamespace(type="content_block_delta", delta=SimpleNamespace(type="text_delta", text=text))


def _anthropic_json(partial):
    return SimpleNamespace(type="content_block_delta", delta=SimpleNamespace(type="input_json_delta", partial_json=partial))


@pytest.mark.asyncio
async def test_anthropic_text_deltas_extracts_and_skips():
    events = [
        SimpleNamespace(type="message_start"),
        SimpleNamespace(type="content_block_start"),
        _anthropic_text("Hello"),
        _anthropic_json('{"x":'),        # tool-use JSON delta — skipped
        _anthropic_text(", world"),
        SimpleNamespace(type="content_block_stop"),
        SimpleNamespace(type="message_delta"),
        SimpleNamespace(type="message_stop"),
    ]
    out = [t async for t in anthropic_text_deltas(_aiter(events))]
    assert out == ["Hello", ", world"]


@pytest.mark.asyncio
async def test_anthropic_text_deltas_closes_on_early_exit():
    stream = _ClosableStream([_anthropic_text("a"), _anthropic_text("b")], close_attr="close")
    gen = anthropic_text_deltas(stream)
    assert await gen.__anext__() == "a"
    await gen.aclose()
    assert stream.closed is True


def test_anthropic_factory_requires_max_tokens():
    # max_tokens is required (no silent default that could truncate output).
    with pytest.raises(TypeError):
        anthropic_factory(object(), model="m", messages=[])


@pytest.mark.asyncio
async def test_anthropic_factory_passes_max_tokens_and_composes(build_client):
    opened = {"n": 0}

    class _Client:
        def __init__(self):
            async def create(**kwargs):
                opened["n"] += 1
                assert kwargs["max_tokens"] == 512   # Anthropic requires it
                assert kwargs["stream"] is True
                return _aiter([_anthropic_text("Hel"), _anthropic_text("lo")])
            self.messages = SimpleNamespace(create=create)

    factory = anthropic_factory(
        _Client(), model="claude-3-5-sonnet-latest", max_tokens=512,
        messages=[{"role": "user", "content": "hi"}],
    )
    assert opened["n"] == 0

    collie = build_client(_echo_handler)
    events = [e async for e in collie.streaming.protect_stream(input="hi", raw_stream_factory=factory)]
    assert "".join(e.text for e in events if isinstance(e, SafeDelta)) == "Hello"
    assert opened["n"] == 1


@pytest.mark.asyncio
async def test_anthropic_factory_forces_stream_over_caller_kwargs():
    # A caller-supplied stream=False must be overridden (not raise a
    # duplicate-keyword TypeError when iteration starts).
    seen = {}

    class _Client:
        def __init__(self):
            async def create(**kwargs):
                seen.update(kwargs)
                return _aiter([_anthropic_text("x")])
            self.messages = SimpleNamespace(create=create)

    factory = anthropic_factory(_Client(), model="m", max_tokens=8, messages=[], stream=False)
    out = [t async for t in factory()]
    assert out == ["x"]
    assert seen["stream"] is True  # forced on
    assert seen["max_tokens"] == 8
