"""OpenAI provider adapter (``collieai[openai]``).

Normalizes the OpenAI Python SDK's streaming chunks into plain text deltas so
they flow through ``collie.streaming.protect_stream(...)`` /
``protect_buffered(...)``. Deliberately thin: text-delta extraction only — your
provider configuration and API key management stay yours.

This module does NOT import ``openai`` (it duck-types the client/stream), so it
imports fine without the provider installed; the ``collieai[openai]`` extra just
ensures the OpenAI SDK is present for you to pass a client in.
"""
from __future__ import annotations

from typing import Any, AsyncIterable, AsyncIterator, Callable, Dict, List

from ._common import aclose_stream


async def openai_text_deltas(stream: AsyncIterable[Any]) -> AsyncIterator[str]:
    """Yield each non-empty content delta from an OpenAI chat-completion stream.

    ``stream`` is what ``await client.chat.completions.create(..., stream=True)``
    returns. Role-only chunks, empty deltas, and tool-call-only chunks are
    skipped — only assistant text is yielded. The underlying stream is closed
    when iteration ends or is abandoned early.
    """
    try:
        async for chunk in stream:
            choices = getattr(chunk, "choices", None)
            if not choices:
                continue
            delta = getattr(choices[0], "delta", None)
            content = getattr(delta, "content", None) if delta is not None else None
            if content:
                yield content
    finally:
        await aclose_stream(stream)


def openai_factory(
    client: Any,
    *,
    model: str,
    messages: List[Dict[str, str]],
    **create_kwargs: Any,
) -> Callable[[], AsyncIterator[str]]:
    """Build a zero-arg ``raw_stream_factory`` for ``protect_stream`` /
    ``protect_buffered``.

        from collieai.adapters.openai import openai_factory

        factory = openai_factory(
            async_openai_client,
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        async for event in collie.streaming.protect_stream(
            input=prompt, raw_stream_factory=factory,
        ):
            ...

    The factory opens the OpenAI stream **lazily** — only when CollieAi calls
    it, i.e. after the input check passes — so a blocked input never spends
    tokens. CollieAi chunk retries never re-invoke it.
    """

    async def _stream() -> AsyncIterator[str]:
        # Caller extras first, then forced contract fields override — a
        # caller-supplied stream=False can't break streaming (and won't raise a
        # duplicate-keyword TypeError). Node parity.
        kwargs = dict(create_kwargs)
        kwargs.update(model=model, messages=messages, stream=True)
        response = await client.chat.completions.create(**kwargs)
        deltas = openai_text_deltas(response)
        try:
            async for text in deltas:
                yield text
        finally:
            await deltas.aclose()

    return _stream
