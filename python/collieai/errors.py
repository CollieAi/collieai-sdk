"""Typed exception hierarchy for the CollieAi SDK.

Mirrors the SDK's typed exception hierarchy contract.
Customers catch these instead of parsing error strings; each maps to a raw API
error code or a client-side precondition. The full tree is defined here (not
just the codes Slice 1 emits) so the public surface is stable across slices.
"""
from __future__ import annotations

from typing import Optional


class CollieError(Exception):
    """Base for every error raised by the SDK."""


class CollieAPIError(CollieError):
    """An unexpected or unmapped HTTP error from the API (including a malformed
    response body). Carries the HTTP status and the wire error code/message
    when present."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class CollieConnectionError(CollieError):
    """A transport-level failure (timeout, connection refused/reset, DNS, …)
    before a usable HTTP response was received. Wraps the underlying
    ``httpx`` error as ``__cause__``."""


# --- Preflight (Slice 3; defined now so the surface is stable) ---------------
class PreflightError(CollieError):
    """Base for streaming-preflight failures."""


class ProjectNotFound(PreflightError):
    ...


class StreamingFeatureDisabled(PreflightError):
    ...


class PlanNotEntitled(PreflightError):
    ...


class UnknownRuleType(PreflightError):
    ...


class PolicyNotStreamable(PreflightError):
    ...


# --- High-level wrapper preconditions (Slice 2) ------------------------------
class BufferedFallbackRequired(CollieError):
    """Raised when the caller required true streaming but the policy buffers."""


class ProviderStreamFactoryRequired(CollieError):
    """Raised when high-level streaming is called with an already-started
    provider stream while input checking is enabled."""


class MaskedInputError(CollieError):
    """The input policy MASKED this prompt, and the zero-arg provider factory
    cannot receive the masked text — proceeding would send the ORIGINAL
    (unmasked) prompt to the model, breaking the documented contract that
    masked content never reaches it. Remedy: call ``moderate.input(...)``
    yourself, build the provider stream over ``result.filtered_text`` (it can
    legitimately be ``""`` — a full wipe), and pass ``input_result=result``
    to the wrapper; the reused-result path is exempt from this check because
    the caller demonstrably holds the filtered text."""


# --- Chunk submission --------------------------------------------------------
class ChunkError(CollieError):
    """Base for chunk-submission errors. Carries the wire code + HTTP status
    when derived from an API response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class ChunkRetryExhausted(ChunkError):
    """The per-chunk retry budget/ceiling was exhausted on a retryable error."""


class ChunkQuotaExceeded(ChunkError):
    """429 with no usable Retry-After (missing, negative, or beyond the
    configured ceiling)."""


class ChunkPolicyChanged(ChunkError):
    """409 chunk_policy_changed — fatal; create a new job for the new policy."""


class ChunkSessionUnrecoverable(ChunkError):
    """409 chunk_session_unrecoverable — fatal; the stream can't be repaired."""


class ChunkSessionFinished(ChunkError):
    """409 chunk_session_finished — the session is already terminal."""


class ChunkIdempotencyConflict(ChunkError):
    """409 chunk_idempotency_conflict — same sequence replayed with different
    content (a caller bug; the SDK normally prevents this)."""


class ChunkSequenceConflict(ChunkError):
    """409 chunk_sequence_conflict — a sequence gap (a caller bug; the SDK
    owns sequence numbers)."""


class ChunkConcurrentSubmit(ChunkError):
    """409 chunk_concurrent_submit — a server-side in-flight collision
    (retryable; the SDK also guards against this client-side)."""


class ChunkStreamingUnsupported(ChunkError):
    """400 chunk_streaming_unsupported — the policy can't be served by the
    streaming engine; use the synchronous job endpoint instead."""


class ChunkResolutionUnavailable(ChunkError):
    """503 chunk_resolution_unavailable — the server could not RESOLVE the
    policy at all (a resolver dependency was unreachable), as opposed to the
    policy being unable to stream. Retryable: the SDK retries the same
    sequence, and when the outage outlasts the retry budget this is the
    ``__cause__`` of the ``ChunkRetryExhausted`` it raises."""


class ConcurrentSessionUseError(CollieError):
    """Overlapping push()/finish() calls were attempted on one session.
    Chunk submission must be serial per session."""


class ModerationError(CollieError):
    """moderate.input / moderate.output ended in an unexpected terminal state
    (failed/expired) or timed out before producing a verdict."""
