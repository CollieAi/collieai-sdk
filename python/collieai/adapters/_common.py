"""Shared helpers for provider adapters."""
from __future__ import annotations

import inspect
from typing import Any


async def aclose_stream(stream: Any) -> None:
    """Best-effort close a provider stream (``aclose`` or ``close``, sync or
    async). Abandoning an ``async for`` does NOT close the iterator, so without
    this an early exit (block / disconnect / cancellation) could leave the
    provider stream — and paid generation — running."""
    for name in ("aclose", "close"):
        fn = getattr(stream, name, None)
        if fn is None:
            continue
        try:
            result = fn()
            if inspect.isawaitable(result):
                await result
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
        return
