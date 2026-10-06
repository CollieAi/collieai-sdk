"""Anthropic provider adapter (``collieai[anthropic]``).

Normalizes the Anthropic Python SDK's raw streaming events into plain text
deltas. Text comes from ``content_block_delta`` events whose delta carries
``.text`` (``text_delta``); ``input_json_delta`` (tool use), block start/stop,
and message start/delta/stop events are skipped.

Like the OpenAI adapter, this does NOT import ``anthropic`` — it duck-types the
client/stream; the ``collieai[anthropic]`` extra just ensures the SDK is present.
"""
from __future__ import annotations

from typing import Any, AsyncIterable, AsyncIterator, Callable, Dict, List

from ._common import aclose_stream


async def anthropic_text_deltas(stream: AsyncIterable[Any]) -> AsyncIterator[str]:
    """Yield each non-empty text delta from an Anthropic Messages stream.

    ``stream`` is what ``await client.messages.create(..., stream=True)``
    returns. Only ``content_block_delta`` text is yielded; tool-use JSON deltas
    and lifecycle events are skipped. The underlying stream is closed when
    iteration ends or is abandoned early.
    """
    try:
        async for event in stream:
            if getattr(event, "type", None) != "content_block_delta":
                continue
            delta = getattr(event, "delta", None)
            text = getattr(delta, "text", None) if delta is not None else None
            if text:
                yield text
    finally:
        await aclose_stream(stream)


def anthropic_factory(
    client: Any,
    *,
    model: str,
    messages: List[Dict[str, str]],
    max_tokens: int,
    **create_kwargs: Any,
) -> Callable[[], AsyncIterator[str]]:
    """Build a zero-arg ``raw_stream_factory`` for ``protect_stream`` /
    ``protect_buffered`` from an Anthropic async client.

        from collieai.adapters.anthropic import anthropic_factory

        factory = anthropic_factory(
            async_anthropic_client,
            model="claude-3-5-sonnet-latest",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

    Opens the stream lazily (after the input check passes); CollieAi chunk
    retries never re-invoke it.
    """

    async def _stream() -> AsyncIterator[str]:
        # Caller extras first, then forced contract fields override — a
        # caller-supplied stream=False can't break streaming (and won't raise a
        # duplicate-keyword TypeError). Node parity.
        kwargs = dict(create_kwargs)
        kwargs.update(model=model, max_tokens=max_tokens, messages=messages, stream=True)
        response = await client.messages.create(**kwargs)
        deltas = anthropic_text_deltas(response)
        try:
            async for text in deltas:
                yield text
        finally:
            await deltas.aclose()

    return _stream
