"""Low-level customer-owned streaming session.

``collie.streaming.session(...)`` wraps the raw chunk-ingestion protocol: it
creates the job, owns the monotonic ``sequence`` numbers, serializes submits,
retries transient failures with the SAME sequence (via the client), and exposes
typed results. The customer pushes upstream-model text and forwards only the
returned safe emits.

The low-level session does NOT run input filtering — advanced callers who want
input safety must call ``collie.moderate.input(...)`` themselves. The
higher-level helpers in this module (``protect_stream`` / ``protect_buffered``)
add the bundled input check, batching, and preflight.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import time
from datetime import datetime, timezone

import httpx
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    Optional,
)

from ._pacing import PollPacer
from .errors import (
    BufferedFallbackRequired,
    ChunkSessionFinished,
    ChunkSessionUnrecoverable,
    CollieAPIError,
    CollieError,
    ConcurrentSessionUseError,
    MaskedInputError,
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
    Finished,
    InputBlocked,
    InputModerationResult,
    SafeDelta,
    StreamEvent,
    StreamingCapability,
    StreamInterrupted,
    StreamToken,
    TriggeredRule,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._client import AsyncCollie

_SDK_ORIGIN = "streaming.session"
_PROTECT_STREAM_ORIGIN = "protect_stream"
_PROTECT_BUFFERED_ORIGIN = "protect_buffered"
_PREFLIGHT_ORIGIN = "streaming.preflight"

# Force a fresh preflight when less than this much validity remains, so a
# token-stream UI never starts under a policy that may already have changed.
_PREFLIGHT_MIN_VALIDITY_S = 5.0

# Map an unsupported preflight reason to its typed exception.
_PREFLIGHT_ERRORS: "dict[str, type]" = {
    "project_not_found": ProjectNotFound,
    "plan_not_entitled": PlanNotEntitled,
    "unknown_rule_type": UnknownRuleType,
    "streaming_feature_disabled": StreamingFeatureDisabled,
    "policy_not_streamable": PolicyNotStreamable,
}


def _reject_masked_input(original: str, filtered: Optional[str]) -> None:
    """Fail closed when the wrapper's OWN input gate masked the prompt
    (round 5): the zero-arg provider factory closes over the ORIGINAL text,
    so proceeding would stream unmasked content to the model — breaking the
    documented promise that masked values never reach it. `!=` (not
    truthiness): a full wipe to `""` is a real mask verdict. Only the
    wrapper-owned gate is checked — the `input_result` path is exempt
    because the caller demonstrably holds `filtered_text` and owns the
    factory's contents."""
    if filtered is not None and filtered != original:
        raise MaskedInputError(
            "the input policy MASKED this prompt; the zero-arg provider "
            "factory would stream the ORIGINAL (unmasked) text to the "
            "model. Call moderate.input yourself, build the provider "
            "stream over result.filtered_text, and pass "
            "input_result=result to the wrapper."
        )


def _preflight_error(capability: StreamingCapability) -> PreflightError:
    cls = _PREFLIGHT_ERRORS.get(capability.reason or "", PreflightError)
    return cls(capability.reason_detail or capability.reason or "streaming unavailable")

# Default batching thresholds of the high-level stream wrapper: flush on any of these, plus on provider end.
_DEFAULT_FLUSH_INTERVAL_S = 0.05
_DEFAULT_MAX_DELTAS = 16
_DEFAULT_MAX_CHARS = 2048

# Terminal / failure job statuses for the buffered (outbound-only-job) path.
# An outbound-only job never enters the inbound phase, so its terminals are
# completed (passed) or outbound_blocked.
_BUFFERED_TERMINAL = frozenset({"completed", "outbound_blocked"})
_BUFFERED_FAILURE = frozenset({"failed", "expired"})


async def _iter_sse(response: httpx.Response):
    """Parse an SSE response into (event_id, event_type, data) tuples.

    Comment lines (``: keepalive``) are ignored. ``data`` is the concatenation
    of all ``data:`` lines in an event, joined by newlines.
    """
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if line == "":  # event boundary
            if event_type is not None or data_lines:
                yield event_id, event_type or "message", "\n".join(data_lines)
            event_id, event_type, data_lines = None, None, []
            continue
        if line.startswith(":"):  # comment / keepalive
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_type = value
        elif field == "data":
            data_lines.append(value)
        elif field == "id":
            event_id = value
    if event_type is not None or data_lines:  # flush a final unterminated event
        yield event_id, event_type or "message", "\n".join(data_lines)


def _context_payload(event: Any) -> Dict[str, Any]:
    """blocked_by + the serialized context verdict for an SSE relay frame, so a
    browser sees the context block/pointer/rule and degraded markers. Keys are
    omitted when absent, keeping frames minimal."""
    out: Dict[str, Any] = {}
    blocked_by = getattr(event, "blocked_by", None)
    context = getattr(event, "context", None)
    if blocked_by is not None:
        out["blocked_by"] = blocked_by
    if context is not None:
        out["context"] = context.model_dump()
    return out


def to_sse(event: StreamEvent) -> str:
    """Re-encode a stream event as an SSE frame, for relaying to a browser from
    your own endpoint. Every event type is serialized, including
    ``StreamInterrupted`` (as ``event: interrupted``) — skip those before
    calling this if you don't want reconnect notices on the wire."""
    if isinstance(event, SafeDelta):
        payload = {"text": event.text, "sequence": event.sequence}
        name = "delta"
    elif isinstance(event, Blocked):
        payload = {"block_message": event.block_message}
        name = "blocked"
    elif isinstance(event, InputBlocked):
        payload = {"block_message": event.block_message, **_context_payload(event)}
        name = "input_blocked"
    elif isinstance(event, Finished):
        payload = {"finish_reason": event.finish_reason, **_context_payload(event)}
        name = "finished"
    elif isinstance(event, StreamInterrupted):
        payload = {"reason": event.reason, "resumable": event.resumable}
        name = "interrupted"
    elif isinstance(event, BufferedFallback):
        payload = {
            "reason": event.reason,
            "recommended_client_behavior": event.recommended_client_behavior,
        }
        name = "buffered_fallback"
    else:  # pragma: no cover - defensive
        payload = {}
        name = getattr(event, "type", "message")
    return f"event: {name}\ndata: {json.dumps(payload)}\n\n"


def _resolve_provider_iterator(provider: Any) -> AsyncIterator[str]:
    """Validate that a factory's return value is a usable async iterator and
    return its iterator, mapping every misuse to ProviderStreamFactoryRequired.

    Handles the common mistakes: an ``async def`` factory (returns a coroutine —
    closed here so it doesn't emit "coroutine was never awaited"), a sync
    iterable (list/etc.), and an object whose ``__aiter__`` doesn't yield a real
    async iterator.
    """
    if inspect.iscoroutine(provider):
        provider.close()  # avoid RuntimeWarning: coroutine was never awaited
        raise ProviderStreamFactoryRequired(
            "raw_stream_factory must be a callable that RETURNS an async iterable, "
            "not an async function. Wrap it: `lambda: my_async_stream()`."
        )
    if inspect.isawaitable(provider):
        raise ProviderStreamFactoryRequired(
            "raw_stream_factory returned an awaitable, not an async iterable."
        )
    aiter_method = getattr(provider, "__aiter__", None)
    if aiter_method is None:
        raise ProviderStreamFactoryRequired(
            "raw_stream_factory must return an async iterable of text deltas; got "
            f"{type(provider).__name__}."
        )
    try:
        iterator = aiter_method()
    except Exception as exc:  # noqa: BLE001 - any __aiter__ failure is one error class here
        raise ProviderStreamFactoryRequired(
            "raw_stream_factory's __aiter__() raised."
        ) from exc
    if not callable(getattr(iterator, "__anext__", None)):
        raise ProviderStreamFactoryRequired(
            "raw_stream_factory's __aiter__() did not return an async iterator "
            "(no callable __anext__)."
        )
    return iterator


async def _aclose_if_possible(source: Any) -> None:
    """Close an async iterable's generator if it supports ``aclose`` — stops
    the customer's provider stream (and any paid LLM work) when we stop early."""
    aclose = getattr(source, "aclose", None)
    if aclose is None:
        return
    try:
        await aclose()
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


async def _anext_or_stop(iterator: AsyncIterator[str]) -> "tuple[Optional[str], bool]":
    """Return (delta, stopped). Wraps StopAsyncIteration into a sentinel so the
    batcher can await it through asyncio.wait without the exception escaping."""
    try:
        return await iterator.__anext__(), False
    except StopAsyncIteration:
        return None, True


async def _batch_deltas(
    iterator: AsyncIterator[str],
    *,
    flush_interval_s: float,
    max_deltas: int,
    max_chars: int,
) -> AsyncIterator[str]:
    """Coalesce provider deltas into batches. Flush when the buffer reaches
    ``max_deltas`` or ``max_chars``, when ``flush_interval_s`` elapses since the
    first buffered delta, or when the provider ends. Empty windows are skipped.

    Takes an already-resolved async iterator (see ``_resolve_provider_iterator``).
    """
    buf: list[str] = []
    chars = 0
    deltas = 0
    window_start = 0.0
    pending: Optional[asyncio.Future] = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(_anext_or_stop(iterator))
            if buf:
                remaining = flush_interval_s - (time.monotonic() - window_start)
                if remaining <= 0:
                    yield "".join(buf)
                    buf, chars, deltas = [], 0, 0
                    continue
                done, _ = await asyncio.wait({pending}, timeout=remaining)
                if not done:  # flush timer elapsed before the next delta
                    yield "".join(buf)
                    buf, chars, deltas = [], 0, 0
                    continue
            else:
                await asyncio.wait({pending})

            delta, stopped = pending.result()
            pending = None
            if stopped:
                break
            if delta:
                if not buf:
                    window_start = time.monotonic()
                buf.append(delta)
                chars += len(delta)
                deltas += 1
                if deltas >= max_deltas or chars >= max_chars:
                    yield "".join(buf)
                    buf, chars, deltas = [], 0, 0
        if buf:
            yield "".join(buf)
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            # Await the cancellation so the in-flight provider read fully
            # unwinds BEFORE the caller closes the provider. Otherwise
            # provider.aclose() can hit "asynchronous generator is already
            # running" and the close is silently swallowed.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pending


class StreamingClient:
    def __init__(self, client: "AsyncCollie") -> None:
        self._c = client
        self._preflight_cache: Dict[Optional[str], StreamingCapability] = {}

    async def preflight(
        self,
        *,
        project_id: Optional[str] = None,
        force_refresh: bool = False,
    ) -> StreamingCapability:
        """Resolve whether the project's policy can stream output, so the caller
        can choose a token-stream UI, a buffered "checking..." UI, or fail fast
        — before calling the LLM. Results are cached until ``valid_until``;
        pass ``force_refresh=True`` to bypass the cache.

        Unlike the job helpers, ``project_id`` here IS sent to the backend (the
        preflight endpoint validates it against the API key), so it is not
        rejected when the client was built without a project_id.
        """
        return await self._resolve_capability(
            project_id, min_validity_s=0.0, force_refresh=force_refresh
        )

    async def _resolve_capability(
        self,
        project_id: Optional[str],
        *,
        min_validity_s: float,
        force_refresh: bool,
    ) -> StreamingCapability:
        pid = project_id if project_id is not None else self._c.project_id
        if not force_refresh:
            cached = self._preflight_cache.get(pid)
            if cached is not None and cached.valid_until is not None:
                remaining = (cached.valid_until - datetime.now(timezone.utc)).total_seconds()
                if remaining > min_validity_s:
                    self._c._debug(
                        "preflight",
                        project_id=pid, mode=cached.mode,
                        fallback_reason=cached.reason, cached=True,
                    )
                    return cached

        body: Dict[str, Any] = {}
        if pid is not None:
            body["project_id"] = pid
        data, resp = await self._c._request_json(
            "POST", "/v1/streaming/preflight", json=body, sdk_origin=_PREFLIGHT_ORIGIN
        )
        try:
            capability = StreamingCapability.model_validate(data)
        except Exception as exc:
            raise CollieAPIError(
                "API returned a malformed preflight response",
                status_code=resp.status_code,
                code="invalid_response",
            ) from exc
        # Only cache cacheable results — the backend omits valid_until (and
        # sends no-store) for transient unsupported verdicts. When a fresh
        # fetch is NOT cacheable, evict any prior entry so a later call can't
        # return a stale verdict (e.g. force_refresh that flips streaming →
        # unsupported).
        if capability.valid_until is not None:
            self._preflight_cache[pid] = capability
        else:
            self._preflight_cache.pop(pid, None)
        self._c._debug(
            "preflight",
            project_id=pid, mode=capability.mode,
            fallback_reason=capability.reason, cached=False,
        )
        return capability

    def session(
        self,
        *,
        input: str,
        project_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        input_job_id: Optional[str] = None,
    ) -> "StreamingSession":
        """Open a streaming session as an async context manager.

        ``input`` is the prompt context output rules may use; it is sent as the
        job's ``message_input``. ``project_id`` is validated against the client's
        project (the API key scopes the project); it is not sent in the body.

        ``input_job_id`` is the job id of a ``moderate.input`` call that
        already gated exactly this ``input``: the server verifies the
        reference (same project, identical text, completed and not blocked)
        and skips re-filtering the prompt on the session job — one inbound
        pass per turn instead of two. An invalid reference is a request-time
        error, never a silent second pass. Omit it (or run against an older
        server, which ignores the field) and the session job is filtered as
        before.

        Manual sessions do NOT auto-retry the claim protocol: a
        ``CollieAPIError`` with code ``input_gate_stale`` means the policy
        moved (or the gate aged out) — call ``moderate.input`` again (with
        the same context, if any) and open a new session with the fresh
        job id; ``input_gate_unverifiable`` means the claim cannot be
        verified right now — retry WITHOUT ``input_job_id`` (re-gating
        will not help); ``input_gate_claimed`` means the gate was already
        consumed by another session. ``protect_stream`` handles this
        ladder automatically, but ONLY when it runs its own gate — with an
        external ``input_result`` the typed errors surface to your code by
        design (the wrapper cannot re-check a context it never saw).
        """
        self._c._check_project(project_id)
        return StreamingSession(
            self._c,
            input=input,
            conversation_id=conversation_id,
            correlation_id=correlation_id,
            input_job_id=input_job_id,
        )

    def protect_stream(
        self,
        *,
        input: str,
        raw_stream_factory: Callable[[], AsyncIterable[str]],
        project_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        context: Any = None,
        context_format: Optional[str] = None,
        check_input: bool = True,
        input_result: Optional[InputModerationResult] = None,
        require_streaming: bool = False,
        flush_interval_s: float = _DEFAULT_FLUSH_INTERVAL_S,
        max_deltas: int = _DEFAULT_MAX_DELTAS,
        max_chars: int = _DEFAULT_MAX_CHARS,
    ) -> AsyncIterator[StreamEvent]:
        """Stream provider output through CollieAi, yielding only safe events.

        Returns an async iterator of events (``SafeDelta`` / ``Blocked`` /
        ``InputBlocked`` / ``Finished``). A policy block is an event, not an
        exception. ``raw_stream_factory`` is a zero-arg callable returning an
        async iterable of text deltas; it is called **exactly once**, only after
        the input check passes — CollieAi chunk retries never re-invoke it.

        With ``require_streaming=True`` the policy's streamability is checked via
        preflight first: if it must buffer, ``BufferedFallbackRequired`` is
        raised; if it can't be served at all, a ``PreflightError`` is raised —
        both before the provider is invoked. Default (``False``) streams without
        a preflight (use ``streaming.preflight`` + ``protect_buffered`` to branch
        explicitly).

        ``input_result`` is a previously obtained ``moderate.input`` result
        (its ``original_text`` must equal ``input`` and ``allowed`` must be
        True). It requires ``check_input=True`` (the default) — combining it
        with ``check_input=False`` is a contradiction and raises (2.1; it
        was silently ignored before, which also silently dropped the claim).
        The wrapper forwards the result's ``job_id`` as the single-use
        session claim; on the claim's 409s (``input_gate_stale`` /
        ``input_gate_unverifiable`` / ``input_gate_claimed``) there is
        deliberately NO automatic remedy on this path — the result may carry
        a context the wrapper never saw, so the typed errors surface to your
        code. The masked-input fail-closed is also exempt here: you
        demonstrably hold ``filtered_text`` and own what the factory
        streams.

        Argument validation (factory type, project, ``input_result``) happens
        synchronously here, before the iterator is returned, so misuse surfaces
        immediately rather than on first iteration.
        """
        self._c._check_project(project_id)
        if not callable(raw_stream_factory):
            raise ProviderStreamFactoryRequired(
                "raw_stream_factory must be a zero-arg callable returning an "
                "async iterable, not an already-started stream — otherwise the "
                "provider may run before the input check can block it."
            )
        if input_result is not None:
            if not check_input:
                # Round-10 review: this combination was silently ignored —
                # no gate reuse, no claim, the duplicate pass survived.
                # A contradiction must be loud, not a footgun.
                raise ValueError(
                    "input_result requires check_input=True: with "
                    "check_input=False the result would be silently ignored "
                    "(no claim sent, the session re-filters the prompt)."
                )
            if input_result.original_text != input or not input_result.allowed:
                raise ValueError(
                    "input_result must correspond to this input and be allowed "
                    "(original_text must match and allowed must be True)."
                )
            # A precomputed input_result skips the input gate, so the wrapper
            # can't verify the passed context matches the context that produced
            # that result — reject rather than analyze (or silently drop) it.
            if context is not None or context_format is not None:
                raise ValueError(
                    "context cannot be combined with a precomputed input_result: "
                    "the wrapper can't verify the context matches the precomputed "
                    "result. Omit context when reusing an already-computed result, "
                    "or call moderate.input with the desired context."
                )
        return self._protect_stream_impl(
            input=input,
            raw_stream_factory=raw_stream_factory,
            project_id=project_id,
            conversation_id=conversation_id,
            correlation_id=correlation_id,
            context=context,
            context_format=context_format,
            check_input=check_input,
            input_result=input_result,
            require_streaming=require_streaming,
            flush_interval_s=flush_interval_s,
            max_deltas=max_deltas,
            max_chars=max_chars,
        )

    async def _protect_stream_impl(
        self,
        *,
        input: str,
        raw_stream_factory: Callable[[], AsyncIterable[str]],
        project_id: Optional[str],
        conversation_id: Optional[str],
        correlation_id: Optional[str],
        context: Any,
        context_format: Optional[str],
        check_input: bool,
        input_result: Optional[InputModerationResult],
        require_streaming: bool,
        flush_interval_s: float,
        max_deltas: int,
        max_chars: int,
    ) -> AsyncIterator[StreamEvent]:
        ctx = {"conversation_id": conversation_id, "correlation_id": correlation_id}

        # 0. require_streaming: refuse to start a token-stream UI under a policy
        #    that buffers (or can't be served), before any provider spend.
        if require_streaming:
            capability = await self._resolve_capability(
                project_id,
                min_validity_s=_PREFLIGHT_MIN_VALIDITY_S,
                force_refresh=False,
            )
            if capability.mode == "buffered":
                raise BufferedFallbackRequired(
                    capability.reason_detail
                    or capability.reason
                    or "policy requires buffered response checking"
                )
            if capability.mode != "streaming":
                raise _preflight_error(capability)

        # 1. Input check (default). Block before any provider spend.
        # The INPUT-phase context verdict, carried to the success terminal too
        # (Finished) so monitored/degraded-but-allowed context is observable —
        # captured here because `result` is reassigned by session.push below.
        input_blocked_by: Optional[str] = None
        input_context: Optional[ContextModerationResult] = None
        gate_job_id: Optional[str] = None
        if check_input:
            result = input_result or await self._c.moderate._run(
                input,
                context=context,
                context_format=context_format,
                conversation_id=conversation_id,
                correlation_id=correlation_id,
                sdk_origin=_PROTECT_STREAM_ORIGIN,
            )
            input_blocked_by = result.blocked_by
            input_context = result.context
            # The gate's job id becomes the session job's input_job_id: the
            # server-verified proof that this exact prompt already passed
            # inbound filtering, so the session job skips its second pass.
            gate_job_id = result.job_id
            # Gate provider spend on the FAIL-SAFE verdict (v24): not-allowed OR
            # blocked. `blocked` alone is not enough — an ambiguous result now
            # resolves to allowed=False/blocked=True, but a defensively-checked
            # `not allowed` keeps parity with the .NET wrapper.
            if not result.allowed or result.blocked:
                yield InputBlocked(
                    block_message=result.block_message,
                    triggered_rules=result.triggered_rules,
                    job_id=result.job_id,
                    request_id=result.request_id,
                    blocked_by=result.blocked_by,
                    context=result.context,
                    **ctx,
                )
                return
            if input_result is None:
                _reject_masked_input(input, result.filtered_text)

        # 2. Create the Collie session FIRST so an enforceable job exists before
        #    the provider opens, then call the factory (exactly once) INSIDE the
        #    protected block so any opened provider stream is always closed —
        #    even if it eagerly starts paid LLM work on construction.
        session = StreamingSession(
            self._c,
            input=input,
            conversation_id=conversation_id,
            correlation_id=correlation_id,
            sdk_origin=_PROTECT_STREAM_ORIGIN,
            input_job_id=gate_job_id,
        )
        try:
            await session.__aenter__()
        except CollieAPIError as exc:
            # The claim-refusal protocol (server contract §10.2). Two 409
            # codes, two remedies, all resolved BEFORE the provider starts:
            #
            # - input_gate_stale: the policy moved (or the gate aged out) —
            #   re-gate ONCE with the same input and context. A SECOND stale
            #   on the retried create is POSITIVE PROOF the policy is
            #   churning faster than we can verify — raising is the only
            #   verdict-before-provider answer (round 5; the round-4
            #   downgrade would have raced the provider against a proven
            #   drift). A second 409 of the UNVERIFIABLE kind (pin outage
            #   began mid-turn) downgrades: see below.
            # - input_gate_unverifiable: a policy pin is missing (outage) —
            #   re-gating cannot help, downgrade to a claimless session:
            #   our own gate ran milliseconds ago with the full context.
            #   This is the DOCUMENTED availability carve-out: the claimless
            #   session restores the pre-claim (v1) contract, whose async
            #   second pass can land after provider start — accepted for
            #   the outage case ONLY, where no evidence says the fresh
            #   verdict is wrong (unlike a proven drift).
            #
            # The ladder is bounded (at most one re-gate, one downgrade) —
            # never a loop. input_gate_claimed and every other code
            # propagate untouched.
            if exc.code not in ("input_gate_stale", "input_gate_unverifiable"):
                raise
            if input_result is not None:
                # An EXTERNAL input_result may have been produced WITH a
                # context this wrapper never saw (context+input_result is
                # rejected at the API edge precisely because the pairing is
                # unverifiable), so neither a prompt-only re-gate nor a
                # silent downgrade is an honest remedy. Surface the typed
                # error before any provider spend; the caller re-gates with
                # its own context.
                raise

            def _claimless_session() -> "StreamingSession":
                return StreamingSession(
                    self._c,
                    input=input,
                    conversation_id=conversation_id,
                    correlation_id=correlation_id,
                    sdk_origin=_PROTECT_STREAM_ORIGIN,
                )

            if exc.code == "input_gate_unverifiable":
                session = _claimless_session()
                await session.__aenter__()
            else:
                regate = await self._c.moderate._run(
                    input,
                    context=context,
                    context_format=context_format,
                    conversation_id=conversation_id,
                    correlation_id=correlation_id,
                    sdk_origin=_PROTECT_STREAM_ORIGIN,
                )
                input_blocked_by = regate.blocked_by
                input_context = regate.context
                if not regate.allowed or regate.blocked:
                    # The CURRENT policy blocks this input — the stale 409
                    # did its job: the verdict arrived before provider spend.
                    yield InputBlocked(
                        block_message=regate.block_message,
                        triggered_rules=regate.triggered_rules,
                        job_id=regate.job_id,
                        request_id=regate.request_id,
                        blocked_by=regate.blocked_by,
                        context=regate.context,
                        **ctx,
                    )
                    return
                _reject_masked_input(input, regate.filtered_text)
                session = StreamingSession(
                    self._c,
                    input=input,
                    conversation_id=conversation_id,
                    correlation_id=correlation_id,
                    sdk_origin=_PROTECT_STREAM_ORIGIN,
                    input_job_id=regate.job_id,
                )
                try:
                    await session.__aenter__()
                except CollieAPIError as exc2:
                    if exc2.code != "input_gate_unverifiable":
                        # A second STALE is proven churn — never race the
                        # provider against it; every other code propagates.
                        raise
                    session = _claimless_session()
                    await session.__aenter__()
        provider: Any = None
        iterator: Optional[AsyncIterator[str]] = None
        batcher: Optional[AsyncIterator[str]] = None
        try:
            provider = raw_stream_factory()
            iterator = _resolve_provider_iterator(provider)
            batcher = _batch_deltas(
                iterator,
                flush_interval_s=flush_interval_s,
                max_deltas=max_deltas,
                max_chars=max_chars,
            )
            blocked = False
            finished_early = False
            async for batch in batcher:
                result = await session.push(batch)
                for event in self._emits_to_events(result, session, ctx):
                    yield event
                    if isinstance(event, Blocked):
                        blocked = True
                if blocked:
                    break
                if result.finished:
                    # Backend finalized the session on a push without a block
                    # — don't re-process emits via finish(), but still close
                    # with a terminal event below.
                    finished_early = True
                    break
            if not blocked and not finished_early:
                final = await session.finish()
                for event in self._emits_to_events(final, session, ctx):
                    yield event
                    if isinstance(event, Blocked):
                        blocked = True
            if not blocked:
                # Invariant: every stream ends with a terminal event —
                # Blocked above, InputBlocked before the provider opened,
                # Finished here (including the finished-early path).
                yield Finished(
                    finish_reason="stop",
                    job_id=session.job_id,
                    request_id=session.request_id,
                    blocked_by=input_blocked_by,
                    context=input_context,
                    **ctx,
                )
        finally:
            if batcher is not None:
                await batcher.aclose()
            if iterator is not None:
                await _aclose_if_possible(iterator)
            # Also close the original provider when it's a distinct object (its
            # __aiter__ returned a separate iterator), or when validation failed
            # before an iterator was resolved — so paid LLM work always stops.
            if provider is not None and provider is not iterator:
                await _aclose_if_possible(provider)
            # Finalize the job iff real output was streamed but it never reached
            # its own terminal (e.g. upstream raised mid-stream). A factory
            # failure before any chunk is a no-op here (see abort()) — left to
            # expire, not faked as completed-empty.
            await session.abort()
            await session.close()

    @staticmethod
    def _emits_to_events(result: ChunkResult, session: "StreamingSession", ctx: dict):
        """Translate one chunk result's emits into stream events. Raw provider
        text is never surfaced — only CollieAi-released ``emit.text``."""
        events: list[StreamEvent] = []
        for emit in result.emits:
            if emit.blocked:
                events.append(Blocked(
                    block_message=emit.block_message,
                    triggered_rules=[TriggeredRule.model_validate(r) for r in emit.triggered_rules],
                    job_id=session.job_id,
                    request_id=result.request_id,
                    **ctx,
                ))
                break  # a block is terminal
            if emit.text:
                events.append(SafeDelta(
                    text=emit.text,
                    sequence=result.sequence,
                    job_id=session.job_id,
                    request_id=result.request_id,
                    **ctx,
                ))
        return events

    async def protect_buffered(
        self,
        *,
        input: str,
        raw_stream_factory: Callable[[], AsyncIterable[str]],
        project_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        context: Any = None,
        context_format: Optional[str] = None,
        check_input: bool = True,
        input_result: Optional[InputModerationResult] = None,
        timeout_s: float = 30.0,
    ) -> BufferedResult:
        """Buffered fallback: check the input, consume the **whole** provider
        response, submit it for output filtering, and return the final
        allowed/masked/blocked result. Never yields raw provider deltas.

        Follows the same input-precheck + factory contract as
        ``protect_stream``: the provider is not called until the input
        passes. ``input_result`` requires ``check_input=True`` (combining it
        with ``check_input=False`` raises, same as on ``protect_stream``) —
        but unlike the streaming path it is a TRUSTED-CLIENT reuse, not the
        claim protocol: the buffered path opens no streaming session and
        sends no ``input_job_id``, so the server neither verifies nor
        consumes the result — no ``input_gate_*`` 409s can occur here, and
        freshness/single-use are the caller's responsibility.
        """
        # Knob validation BEFORE any side effect: without this, an invalid
        # timeout_s would surface only in _poll_job — after the provider was
        # consumed and the outbound job created. 0.05 is _poll_job's
        # poll_interval_s default (not exposed here).
        PollPacer.validate(0.05, timeout_s)
        self._c._check_project(project_id)
        if not callable(raw_stream_factory):
            raise ProviderStreamFactoryRequired(
                "raw_stream_factory must be a zero-arg callable returning an "
                "async iterable, not an already-started stream."
            )
        if input_result is not None and not check_input:
            raise ValueError(
                "input_result requires check_input=True: with "
                "check_input=False the result would be silently ignored."
            )
        if input_result is not None and (
            input_result.original_text != input or not input_result.allowed
        ):
            raise ValueError(
                "input_result must correspond to this input and be allowed."
            )
        # A precomputed input_result skips the input gate; the wrapper can't
        # verify the passed context matches it (see protect_stream).
        if input_result is not None and (context is not None or context_format is not None):
            raise ValueError(
                "context cannot be combined with a precomputed input_result: "
                "the wrapper can't verify the context matches the precomputed "
                "result. Omit context when reusing an already-computed result, "
                "or call moderate.input with the desired context."
            )

        # 1. Input gate.
        # The INPUT-phase context verdict, surfaced on the result whether or not
        # the input blocked (a monitored/degraded context still allows the
        # request but is worth reporting).
        input_blocked_by: Optional[str] = None
        input_context: Optional[ContextModerationResult] = None
        if check_input:
            result = input_result or await self._c.moderate._run(
                input,
                context=context,
                context_format=context_format,
                conversation_id=conversation_id,
                correlation_id=correlation_id,
                sdk_origin=_PROTECT_BUFFERED_ORIGIN,
            )
            input_blocked_by = result.blocked_by
            input_context = result.context
            if not result.allowed or result.blocked:  # fail-safe gate (v24)
                return BufferedResult(
                    blocked=True,
                    input_blocked=True,
                    block_message=result.block_message,
                    triggered_rules=result.triggered_rules,
                    job_id=result.job_id,
                    request_id=result.request_id,
                    blocked_by=result.blocked_by,
                    context=result.context,
                )
            if input_result is None:
                _reject_masked_input(input, result.filtered_text)

        # 2. Consume the entire provider response (factory runs once).
        provider = raw_stream_factory()
        iterator: Optional[AsyncIterator[str]] = None
        parts: list[str] = []
        try:
            iterator = _resolve_provider_iterator(provider)
            while True:
                try:
                    delta = await iterator.__anext__()
                except StopAsyncIteration:
                    break
                if delta:
                    parts.append(delta)
        finally:
            if iterator is not None:
                await _aclose_if_possible(iterator)
            if provider is not iterator:  # distinct object, or validation failed
                await _aclose_if_possible(provider)
        full_text = "".join(parts)

        # 3. Submit the full response for outbound filtering (no webhook), poll.
        body: Dict[str, Any] = {"message_output": full_text}
        if conversation_id:
            body["conversation_id"] = conversation_id
        if correlation_id:
            body["correlation_id"] = correlation_id
        data, resp = await self._c._request_json(
            "POST", "/v1/jobs", json=body, sdk_origin=_PROTECT_BUFFERED_ORIGIN
        )
        job_id = data.get("job_id")
        if not job_id:
            raise CollieAPIError(
                "Job creation did not return a job_id", code="invalid_response"
            )
        request_id = self._c._request_id(resp)
        job = await self._c._poll_job(
            job_id,
            sdk_origin=_PROTECT_BUFFERED_ORIGIN,
            terminal=_BUFFERED_TERMINAL,
            failure=_BUFFERED_FAILURE,
            timeout_s=timeout_s,
        )
        buffered = self._buffered_result(job, job_id, request_id)
        # Carry the input-phase context verdict onto the (output) result too.
        buffered.blocked_by = input_blocked_by
        buffered.context = input_context
        return buffered

    @staticmethod
    def _buffered_result(job: Dict[str, Any], job_id: str, request_id: Optional[str]) -> BufferedResult:
        """The v24 fail-safe verdict table applied to ``outbound_result``,
        mirroring ``Moderation._to_output_result`` line for line (tech-debt
        #22 closed the buffered gap): a terminal job without its result
        object is malformed (typed error, never an implicit allow); NOT
        blocked requires an EXPLICIT ``allowed is True`` and no hard-block
        signal; ambiguity — an empty object, an absent ``allowed`` —
        resolves to blocked. ``BufferedResult`` keeps its blocked-only
        surface; ``blocked`` is the strict complement of resolved allow."""
        try:
            outbound = job.get("outbound_result")
            if not isinstance(outbound, dict):
                # A terminal outbound job must carry its result object.
                raise ValueError("missing or non-object outbound_result")
            status_blocked = job.get("status") == "outbound_blocked"
            outbound_allowed = outbound.get("allowed")
            hard_blocked = (
                status_blocked
                or bool(outbound.get("blocked"))
                or outbound_allowed is False
            )
            allowed = outbound_allowed is True and not hard_blocked
            return BufferedResult(
                blocked=not allowed,
                block_message=outbound.get("block_message"),
                filtered_text=outbound.get("filtered_content"),
                input_blocked=False,
                triggered_rules=[
                    TriggeredRule.model_validate(r)
                    for r in (outbound.get("triggered_rules") or [])
                ],
                job_id=job_id,
                request_id=request_id,
            )
        except CollieError:
            raise
        except Exception as exc:
            raise CollieAPIError(
                "API returned a malformed buffered result", code="invalid_response"
            ) from exc


class StreamingSession:
    def __init__(
        self,
        client: "AsyncCollie",
        *,
        input: str,
        conversation_id: Optional[str],
        correlation_id: Optional[str],
        sdk_origin: str = _SDK_ORIGIN,
        input_job_id: Optional[str] = None,
    ) -> None:
        self._c = client
        self._input = input
        self._conversation_id = conversation_id
        self._correlation_id = correlation_id
        self._input_job_id = input_job_id
        # Attributes job + chunk requests to the public method that created the
        # session ("streaming.session", or "protect_stream" via the wrapper).
        self._sdk_origin = sdk_origin

        self.job_id: Optional[str] = None
        self.request_id: Optional[str] = None
        self.last_sequence: int = -1

        self._next_sequence: int = 0
        self._finished: bool = False
        self._closed: bool = False
        self._final_result: Optional[ChunkResult] = None
        self._last_result: Optional[ChunkResult] = None
        self._lock = asyncio.Lock()

    # -- context manager -----------------------------------------------------
    async def __aenter__(self) -> "StreamingSession":
        await self._create_job()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def _create_job(self) -> None:
        if self.job_id is not None:
            return
        # Omit empty strings, not just None: at the SDK boundary "" == unset.
        body: Dict[str, Any] = {"message_input": self._input}
        if self._conversation_id:
            body["conversation_id"] = self._conversation_id
        if self._correlation_id:
            body["correlation_id"] = self._correlation_id
        if self._input_job_id:
            body["input_job_id"] = self._input_job_id
        data, resp = await self._c._request_json(
            "POST", "/v1/jobs", json=body, sdk_origin=self._sdk_origin
        )
        self.job_id = data.get("job_id")
        self.request_id = self._c._request_id(resp)
        if not self.job_id:
            raise CollieAPIError(
                "Job creation did not return a job_id", code="invalid_response"
            )

    # -- public API ----------------------------------------------------------
    async def push(self, content: str) -> ChunkResult:
        """Submit one upstream-model chunk; returns the safe emits for it."""
        if self._closed:
            raise CollieError("Session is closed")
        if self._finished:
            raise ChunkSessionFinished(
                "Session already finished; create a new session to stream again.",
                code="chunk_session_finished",
            )
        if self._lock.locked():
            raise ConcurrentSessionUseError(
                "Overlapping push() detected; chunk submission must be serial per session."
            )
        async with self._lock:
            return await self._submit(content=content, is_final=False)

    async def finish(self, finish_reason: str = "stop") -> ChunkResult:
        """Send the terminal chunk exactly once. Idempotent: repeated calls (or
        a call after a mid-stream block) return the cached terminal result
        without re-submitting.

        ``finish_reason`` (e.g. ``"stop"``, ``"length"``) is transmitted on
        the terminal chunk and persisted to the audit log
        (``request_logs.finish_reason``). Empty values are omitted ("" ==
        unset, like the other opaque metadata fields).
        """
        if self._closed:
            raise CollieError("Session is closed")
        if self._finished:
            return self._final_result or self._last_result  # type: ignore[return-value]
        if self._lock.locked():
            raise ConcurrentSessionUseError(
                "Overlapping finish() detected; chunk submission must be serial per session."
            )
        async with self._lock:
            if self._finished:
                return self._final_result or self._last_result  # type: ignore[return-value]
            result = await self._submit(
                content="", is_final=True, finish_reason=finish_reason
            )
            self._final_result = result
            return result

    async def close(self) -> None:
        """Mark the session unusable. Does not finalize the stream — call
        ``finish()`` for that. Safe to call multiple times."""
        self._closed = True

    async def abort(self) -> None:
        """Best-effort: finalize the backend job if real output was streamed but
        the stream didn't reach its own terminal (e.g. the upstream model raised
        mid-stream, or the caller abandoned ``protect_stream``) — so a no-webhook
        job isn't left dangling until expiry.

        Only finalizes when ``last_sequence >= 0`` (at least one chunk was
        accepted). A job that never streamed a chunk (the factory raised, or
        upstream setup failed before any output) is deliberately NOT finalized:
        ``is_final`` is the protocol's only terminal and would mark the job
        *completed with empty output*, masking the failure in the audit log.
        Such a job is left to expire instead. (A true cancel/upstream-error
        terminal would need a backend protocol addition.) No-op if no job, the
        stream already finished, or it was closed by a chunk error. Swallows
        errors — the job expires regardless."""
        if (
            self.job_id is None
            or self._finished
            or self._closed
            or self.last_sequence < 0
        ):
            return
        try:
            result = await self._submit(content="", is_final=True)
            self._final_result = result
        except Exception:
            pass  # best-effort cleanup

    async def mint_stream_token(self) -> StreamToken:
        """Mint a short-lived, job-scoped token for browser SSE subscription.

        Backend convenience helper: returns the token plus a ready-to-use
        ``url`` the browser can open with ``EventSource``. Keep your API key
        server-side and hand the browser only the token; re-mint before
        ``expires_in`` seconds elapse.
        """
        if self.job_id is None:
            raise CollieError("mint_stream_token requires a created session (no job_id).")
        data, _ = await self._c._request_json(
            "POST",
            f"/v1/jobs/{self.job_id}/stream-token",
            sdk_origin=self._sdk_origin,
        )
        token = data.get("stream_token")
        expires_in = data.get("expires_in")
        if not isinstance(token, str) or not token:
            raise CollieAPIError(
                "stream-token response missing or invalid stream_token",
                code="invalid_response",
            )
        # expires_in is an integer lifetime: require exactly int (excludes bool,
        # float like 0.5, and NaN) and positive — else it's a malformed response.
        if type(expires_in) is not int or expires_in <= 0:
            raise CollieAPIError(
                "stream-token response missing or invalid expires_in",
                code="invalid_response",
            )
        base = self._c.base_url.rstrip("/")
        url = f"{base}/v1/jobs/{self.job_id}/stream?stream_token={token}"
        return StreamToken(token=token, expires_in=int(expires_in), url=url)

    async def stream_events(
        self,
        *,
        last_event_id: Optional[str] = None,
        auto_resume: bool = True,
        max_reconnects: int = 5,
        idle_timeout_s: float = 60.0,
    ) -> AsyncIterator[StreamEvent]:
        """Subscribe to this job's CollieAi SSE stream, yielding safe events.

        Yields ``SafeDelta`` / ``Blocked`` / ``Finished`` for content and
        terminal frames, and ``StreamInterrupted`` on idle-timeout / disconnect.
        With ``auto_resume`` (default) the SDK reconnects from the last seen
        event id — replayed frames are deduped, so no visible delta is repeated.
        Set ``auto_resume=False`` to stop at the first interruption and resume
        yourself later by passing its ``last_event_id`` back here.
        """
        if self.job_id is None:
            raise CollieError("stream_events requires a created session (no job_id).")

        ctx = {
            "conversation_id": self._conversation_id,
            "correlation_id": self._correlation_id,
        }
        url = f"/v1/jobs/{self.job_id}/stream"
        seen_ids: set[str] = set()
        current_last_id = last_event_id
        reconnects = 0

        while True:
            headers = {"X-CollieAi-SDK-Origin": self._sdk_origin}
            if current_last_id is not None:
                headers["Last-Event-ID"] = current_last_id
            reason = "disconnect"
            terminal = False
            try:
                async with self._c._client.stream(
                    "GET",
                    url,
                    headers=headers,
                    timeout=httpx.Timeout(idle_timeout_s, connect=idle_timeout_s),
                ) as response:
                    # The SSE response is part of the streaming protocol
                    # surface too — check it even though job-create usually
                    # warned first: a mixed-fleet rollout can serve the SSE
                    # from a newer pod than the create.
                    self._c._check_protocol(response)
                    if response.status_code >= 400:
                        await response.aread()
                        raise self._c._api_error(response)
                    async for ev_id, ev_type, data in _iter_sse(response):
                        if ev_id is not None:
                            current_last_id = ev_id
                        if ev_type == "chunk":
                            if ev_id is not None and ev_id in seen_ids:
                                continue  # replayed after resume — don't re-emit
                            if ev_id is not None:
                                seen_ids.add(ev_id)
                            try:
                                payload = json.loads(data) if data else {}
                                is_blocked = bool(payload.get("blocked"))
                                block_message = payload.get("block_message") or None
                                triggered = [
                                    TriggeredRule.model_validate(r)
                                    for r in (payload.get("triggered_rules") or [])
                                ]
                                text = payload.get("content", "")
                                sequence = int(payload.get("sequence", -1))
                            except Exception as exc:
                                raise CollieAPIError(
                                    "Malformed SSE chunk frame", code="invalid_response"
                                ) from exc
                            if is_blocked:
                                yield Blocked(
                                    block_message=block_message,
                                    triggered_rules=triggered,
                                    job_id=self.job_id,
                                    **ctx,
                                )
                                terminal = True
                                break
                            if text:
                                yield SafeDelta(
                                    text=text,
                                    sequence=sequence,
                                    job_id=self.job_id,
                                    **ctx,
                                )
                        elif ev_type == "end":
                            try:
                                end_reason = (json.loads(data) if data else {}).get("reason")
                            except Exception as exc:
                                raise CollieAPIError(
                                    "Malformed SSE end frame", code="invalid_response"
                                ) from exc
                            if end_reason in ("final", "session_finished"):
                                yield Finished(
                                    finish_reason=end_reason, job_id=self.job_id, **ctx
                                )
                                terminal = True
                                break
                            if end_reason == "blocked":
                                terminal = True  # Blocked already emitted via the chunk
                                break
                            if end_reason == "upstream_error":
                                raise CollieAPIError(
                                    "Upstream stream error", code="upstream_error"
                                )
                            if end_reason == "session_unrecoverable":
                                # Fatal — no future commit can repair this stream;
                                # must NOT auto-resume. Start a new job.
                                raise ChunkSessionUnrecoverable(
                                    "The stream entered an unrecoverable state; "
                                    "create a new job to continue.",
                                    code="session_unrecoverable",
                                )
                            # idle_timeout (or unknown) → resumable interruption
                            reason = "idle_timeout" if end_reason == "idle_timeout" else "disconnect"
                            break
            except httpx.TimeoutException:
                reason = "idle_timeout"
            except httpx.TransportError:
                reason = "disconnect"

            if terminal:
                return

            yield StreamInterrupted(
                reason=reason,
                resumable=True,
                last_event_id=current_last_id,
                job_id=self.job_id,
                **ctx,
            )
            if not auto_resume:
                return
            reconnects += 1
            if reconnects > max_reconnects:
                return

    # -- internals -----------------------------------------------------------
    async def _submit(
        self,
        *,
        content: str,
        is_final: bool,
        finish_reason: Optional[str] = None,
    ) -> ChunkResult:
        await self._create_job()
        sequence = self._next_sequence
        body: Dict[str, Any] = {
            "sequence": sequence, "content": content, "is_final": is_final,
        }
        # Only on the terminal chunk, and omit empty ("" == unset).
        if is_final and finish_reason:
            body["finish_reason"] = finish_reason
        started = self._c._monotonic()
        try:
            resp, attempts = await self._c._post_chunk(
                f"/v1/jobs/{self.job_id}/chunks", body, sdk_origin=self._sdk_origin
            )
        except CollieError:
            # The chunk could not be confirmed (fatal error or retry exhausted).
            # The job is no longer usable; fail the session. Do NOT advance the
            # sequence — it was never accepted.
            self._closed = True
            raise

        try:
            result = ChunkResult.model_validate(self._c._json_or_error(resp))
        except CollieError:
            self._closed = True
            raise
        except Exception as exc:  # malformed shape (valid JSON, wrong fields)
            self._closed = True
            raise CollieAPIError(
                "API returned a malformed chunk response",
                status_code=resp.status_code,
                code="invalid_response",
            ) from exc
        result.job_id = self.job_id
        result.request_id = self._c._request_id(resp) or self.request_id

        # A 2xx body must confirm THIS chunk: accepted, echoing the submitted
        # sequence. Anything else (accepted=false, or a stale/cached response
        # for a different sequence) is a contract violation — advancing on it
        # could desync the sequence or replay another chunk's safe text to the
        # end user. Fail the session instead. (Mirrors the .NET SDK's check.)
        if not result.accepted or result.sequence != sequence:
            self._closed = True
            raise CollieAPIError(
                "API returned an unconfirmed chunk response "
                f"(accepted={result.accepted}, sequence={result.sequence}, "
                f"expected sequence={sequence})",
                status_code=resp.status_code,
                code="invalid_response",
            )

        # Advance only after a confirmed accept (fresh or idempotent replay).
        self.last_sequence = sequence
        self._next_sequence = sequence + 1
        self._last_result = result
        if result.finished:
            self._finished = True
        self._c._debug(
            "chunk",
            job_id=self.job_id,
            request_id=result.request_id,
            sequence=sequence,
            chunk_chars=len(content),
            emit_chars=sum(len(e.text) for e in result.emits),
            blocked=any(e.blocked for e in result.emits),
            finished=result.finished,
            retry_count=attempts - 1,
            latency_ms=round((self._c._monotonic() - started) * 1000, 1),
            conversation_id=self._conversation_id,
            correlation_id=self._correlation_id,
            triggered_rule_ids=[
                r.get("rule_id")
                for e in result.emits
                for r in e.triggered_rules
                if r.get("rule_id")
            ],
        )
        return result
