"""Typed response models for the CollieAi SDK.

Pydantic models with ``extra="ignore"`` so forward-compatible server fields
don't break older SDKs. ``SafeEmit.text`` is aliased from the wire's
``content`` key so customers read ``emit.text`` regardless of the transport.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class TriggeredRule(BaseModel):
    """A rule that fired during filtering."""

    model_config = ConfigDict(extra="ignore")

    rule_id: str = ""
    rule_name: str = ""
    rule_type: str = ""
    decision: str = ""
    monitoring: bool = False
    match_info: Optional[Dict[str, Any]] = None


# Closed status enum for ContextModerationResult.status. Kept as a plain str on
# the model (not a Literal) so a newer server
# status doesn't break an older SDK; these are the documented values.
ContextStatus = Literal[
    "not_provided", "disabled", "not_run", "clean", "monitored", "blocked", "degraded",
]


class ContextModerationResult(BaseModel):
    """The context-analysis verdict, present when
    the policy analyzes context. ``status`` is the closed enum; ``blocked_by`` on
    the parent says whether a block came from the prompt or the context."""

    model_config = ConfigDict(extra="ignore")

    status: str = "not_provided"
    blocked: bool = False
    block_message: Optional[str] = None
    # RFC 6901 JSON Pointer of the triggering leaf (a path, never a value).
    triggering_pointer: Optional[str] = None
    triggering_rule_id: Optional[str] = None
    triggering_rule_type: Optional[str] = None
    # Degraded-coverage markers (§7/§8): coverage was incomplete.
    parse_degraded: bool = False
    limit_exceeded: bool = False
    inference_degraded: bool = False


class InputModerationResult(BaseModel):
    """Result of ``collie.moderate.input(...)``."""

    model_config = ConfigDict(extra="ignore")

    allowed: bool
    blocked: bool
    block_message: Optional[str] = None
    original_text: str
    filtered_text: Optional[str] = None
    triggered_rules: List[TriggeredRule] = Field(default_factory=list)
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    # Context analysis. blocked_by ∈
    # {"prompt", "context", "none"}; context is the typed verdict (None on a job
    # without context analysis).
    blocked_by: Optional[str] = None
    context: Optional[ContextModerationResult] = None


class OutputModerationResult(BaseModel):
    """Result of ``collie.moderate.output(...)``.

    No ``context``/``blocked_by``: context is an input surface (the server
    rejects an outbound-only job carrying one) and output moderation has a
    single surface, so there is nothing to attribute between."""

    model_config = ConfigDict(extra="ignore")

    allowed: bool
    blocked: bool
    block_message: Optional[str] = None
    original_text: str
    # The MASKED output when masking rules fired. Send this, not the
    # original — a caller that ignores it silently defeats outbound
    # masking (PII redaction etc.).
    filtered_text: Optional[str] = None
    triggered_rules: List[TriggeredRule] = Field(default_factory=list)
    job_id: Optional[str] = None
    request_id: Optional[str] = None


class SafeEmit(BaseModel):
    """One CollieAi-released emit for a submitted chunk. ``text`` is the safe
    content the customer may forward to the end user."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    text: str = Field(default="", alias="content")
    blocked: bool = False
    # Customer-facing message for blocked emits (the blocking rule's
    # configured message or the backend default). None on non-block emits.
    block_message: Optional[str] = None
    final: bool = False
    triggered_rules: List[Dict[str, Any]] = Field(default_factory=list)


class ChunkResult(BaseModel):
    """Response from one ``session.push(...)`` / ``session.finish(...)``."""

    model_config = ConfigDict(extra="ignore")

    sequence: int
    accepted: bool
    emits: List[SafeEmit] = Field(default_factory=list)
    finished: bool
    # Populated by the SDK from the session / response headers, not the body.
    job_id: Optional[str] = None
    request_id: Optional[str] = None


# ---------------------------------------------------------------------------
# High-level streaming events (Slice 2). ``protect_stream`` yields these; the
# ``type`` field discriminates them. Policy decisions are events, not
# exceptions — only infrastructure failures raise.
# ---------------------------------------------------------------------------


class SafeDelta(BaseModel):
    """A CollieAi-released text delta — safe to forward to the end user."""

    type: Literal["delta"] = "delta"
    text: str
    sequence: int
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    conversation_id: Optional[str] = None
    correlation_id: Optional[str] = None


class Blocked(BaseModel):
    """A rule blocked the output mid-stream. Terminal."""

    type: Literal["blocked"] = "blocked"
    block_message: Optional[str] = None
    triggered_rules: List[TriggeredRule] = Field(default_factory=list)
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    conversation_id: Optional[str] = None
    correlation_id: Optional[str] = None


class InputBlocked(BaseModel):
    """The input was blocked before any provider output was consumed. Terminal."""

    type: Literal["input_blocked"] = "input_blocked"
    block_message: Optional[str] = None
    triggered_rules: List[TriggeredRule] = Field(default_factory=list)
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    conversation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    # Context analysis: blocked_by ∈
    # {"prompt","context","none"} and the typed context verdict, so a wrapper
    # caller can tell a context block from a prompt block and show the pointer.
    blocked_by: Optional[str] = None
    context: Optional[ContextModerationResult] = None


class Finished(BaseModel):
    """The stream finished normally. Terminal."""

    type: Literal["finished"] = "finished"
    finish_reason: Optional[str] = None
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    conversation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    # The INPUT-phase context verdict, carried on
    # the success terminal too so a monitored/degraded-but-allowed context is
    # observable, not only on a block. None when no context analysis ran.
    blocked_by: Optional[str] = None
    context: Optional[ContextModerationResult] = None


class BufferedFallback(BaseModel):
    """The policy cannot safely stream; the caller should switch to a buffered
    UX. Part of the event vocabulary; `protect_stream(require_streaming=True)`
    raises `BufferedFallbackRequired` rather than yielding this, so it is not
    emitted by the current flows."""

    type: Literal["buffered_fallback"] = "buffered_fallback"
    reason: str
    recommended_client_behavior: Literal["buffer_then_show"] = "buffer_then_show"


class StreamInterrupted(BaseModel):
    """An SSE subscription was interrupted (idle timeout or disconnect) but can
    be resumed from ``last_event_id``. With ``auto_resume`` the SDK reconnects
    after emitting this; otherwise iteration stops here and the caller may
    resume by passing ``last_event_id`` back to ``stream_events``."""

    type: Literal["interrupted"] = "interrupted"
    reason: str  # "idle_timeout" | "disconnect"
    resumable: bool = True
    last_event_id: Optional[str] = None
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    conversation_id: Optional[str] = None
    correlation_id: Optional[str] = None


StreamEvent = Union[
    SafeDelta, Blocked, InputBlocked, Finished, BufferedFallback, StreamInterrupted
]


class BufferedResult(BaseModel):
    """Result of ``protect_buffered(...)`` — the policy was checked over the
    full response rather than streamed."""

    model_config = ConfigDict(extra="ignore")

    blocked: bool
    block_message: Optional[str] = None
    filtered_text: Optional[str] = None
    # True iff the block happened on the input (before generation), not output.
    input_blocked: bool = False
    triggered_rules: List[TriggeredRule] = Field(default_factory=list)
    job_id: Optional[str] = None
    request_id: Optional[str] = None
    # Context analysis — populated on an input
    # block so a buffered caller sees a context block + the pointer/rule detail.
    blocked_by: Optional[str] = None
    context: Optional[ContextModerationResult] = None


# ---------------------------------------------------------------------------
# Streaming preflight (Slice 3). Mirrors the backend StreamingCapability so the
# SDK can decide token-stream vs buffered UX before calling the provider.
# ---------------------------------------------------------------------------


class RuleCapability(BaseModel):
    """Per-rule streaming support, for showing *which* rule forces buffering."""

    model_config = ConfigDict(extra="ignore")

    rule_id: str
    rule_name: str
    rule_type: str
    decision: str
    monitoring: bool
    streaming_supported: bool
    fallback_reason: Optional[str] = None
    # Which of four roles the rule plays in a streaming request:
    # `enforce_streaming`, `enforce_postflight`, `stream_observed`,
    # `postflight_observed`. `streaming_supported` cannot express this — it is
    # false for EVERY monitor rule, so a stream-capable observer and a
    # full-context one look identical without this field.
    #
    # Deliberately `str`, not a `Literal`: this SDK CONSUMES the value, and a
    # server that adds a fifth role must not break deserialization on an older
    # client. Compare against the four names above and treat anything else as
    # unknown rather than as an error.
    #
    # Three limits inherited from the server. It is per-RULE and ignores
    # policy-level gates, so a rule can report `stream_observed` while the
    # request still buffers — read `StreamingCapability.mode` for whether the
    # request streams at all. It reflects the project's `streaming_mode`,
    # so a project set to `buffered` sees full-context roles throughout. And
    # it describes the rule's role ON THE SERVER THAT ANSWERED: servers with
    # monitor streaming deployed run monitor rules as observers and the
    # request streams; an older server still buffers any monitor policy and
    # answers `mode="buffered"`, `reason="monitor_mode"` until upgraded.
    # Either way, `mode` is the delivery verdict — never this field.
    # `None` when the rule cannot be planned at all (unregistered type, or a
    # handler that raises), since such a rule has no role.
    execution_role: Optional[str] = None


class StreamingCapability(BaseModel):
    """Result of ``streaming.preflight(...)``."""

    model_config = ConfigDict(extra="ignore")

    mode: Literal["streaming", "buffered", "unsupported"]
    recommended_client_behavior: Literal["stream", "buffer_then_show", "fail_fast"]
    project_id: str
    streaming_mode: Optional[str] = None
    # `reason` is a stable machine code; left as str (not Literal) so a new
    # server reason doesn't break older SDKs. `reason_detail` is human-readable.
    reason: Optional[str] = None
    reason_detail: Optional[str] = None
    valid_until: Optional[datetime] = None
    rules: List[RuleCapability] = Field(default_factory=list)


class StreamToken(BaseModel):
    """A minted browser SSE stream token (``session.mint_stream_token()``).

    Hand only ``token`` (or the ready-to-use ``url``) to the browser — keep your
    API key server-side. Re-mint before ``expires_in`` seconds elapse.
    """

    model_config = ConfigDict(extra="ignore")

    token: str
    expires_in: int
    url: str  # GET {base}/v1/jobs/{id}/stream?stream_token=<token>
