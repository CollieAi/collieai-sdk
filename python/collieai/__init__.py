"""CollieAi Python SDK.

Safe customer-owned LLM streaming and pre-generation input moderation. See the
README for the integration guide.

    from collieai import AsyncCollie

    collie = AsyncCollie(api_key="clai_...", project_id="project_123")
"""
from ._client import AsyncCollie
from ._version import __version__
from .errors import (
    BufferedFallbackRequired,
    MaskedInputError,
    ChunkConcurrentSubmit,
    ChunkError,
    ChunkIdempotencyConflict,
    ChunkPolicyChanged,
    ChunkQuotaExceeded,
    ChunkResolutionUnavailable,
    ChunkRetryExhausted,
    ChunkSequenceConflict,
    ChunkSessionFinished,
    ChunkSessionUnrecoverable,
    ChunkStreamingUnsupported,
    CollieAPIError,
    CollieConnectionError,
    CollieError,
    ConcurrentSessionUseError,
    ModerationError,
    PlanNotEntitled,
    PolicyNotStreamable,
    PreflightError,
    ProjectNotFound,
    ProviderStreamFactoryRequired,
    StreamingFeatureDisabled,
    UnknownRuleType,
)
from .models import (
    Blocked,
    BufferedFallback,
    BufferedResult,
    ChunkResult,
    ContextModerationResult,
    ContextStatus,
    Finished,
    InputBlocked,
    InputModerationResult,
    OutputModerationResult,
    RuleCapability,
    SafeDelta,
    SafeEmit,
    StreamEvent,
    StreamingCapability,
    StreamInterrupted,
    StreamToken,
    TriggeredRule,
)
from .streaming import StreamingSession, to_sse

__all__ = [
    "__version__",
    "AsyncCollie",
    "StreamingSession",
    "to_sse",
    # models
    "ChunkResult",
    "ContextModerationResult",
    "ContextStatus",
    "InputModerationResult",
    "OutputModerationResult",
    "SafeEmit",
    "TriggeredRule",
    "BufferedResult",
    "StreamingCapability",
    "RuleCapability",
    "StreamToken",
    # events
    "StreamEvent",
    "SafeDelta",
    "Blocked",
    "InputBlocked",
    "Finished",
    "BufferedFallback",
    "StreamInterrupted",
    # errors
    "CollieError",
    "CollieAPIError",
    "CollieConnectionError",
    "PreflightError",
    "ProjectNotFound",
    "StreamingFeatureDisabled",
    "PlanNotEntitled",
    "UnknownRuleType",
    "PolicyNotStreamable",
    "BufferedFallbackRequired",
    "MaskedInputError",
    "ProviderStreamFactoryRequired",
    "ChunkError",
    "ChunkRetryExhausted",
    "ChunkQuotaExceeded",
    "ChunkPolicyChanged",
    "ChunkSessionUnrecoverable",
    "ChunkSessionFinished",
    "ChunkIdempotencyConflict",
    "ChunkSequenceConflict",
    "ChunkConcurrentSubmit",
    "ChunkStreamingUnsupported",
    "ChunkResolutionUnavailable",
    "ConcurrentSessionUseError",
    "ModerationError",
]
