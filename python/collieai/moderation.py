"""Input and output moderation — the standalone safety checks.

``collie.moderate.input(...)`` lets a customer check a user prompt before
calling their LLM, without implementing webhooks. MVP transport: create an
``inbound_only`` job (no webhook) and poll it to a terminal inbound state.

``collie.moderate.output(...)`` is the sibling for assistant text produced
OUTSIDE a ``protect_stream``/``protect_buffered`` wrapper (proactive
notifications, escalations): a ``message_output``-only
job polled to a terminal outbound state, evaluated by OUTPUT-direction rules.
"""
from __future__ import annotations

import asyncio
import math
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

from ._pacing import PollPacer
from .errors import (
    CollieAPIError,
    CollieError,
    FileAnalysisUnavailable,
    ModerationError,
    ModerationTimeout,
)
from .models import (
    ContextModerationResult,
    InputModerationResult,
    OutputModerationResult,
    TriggeredRule,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._client import AsyncCollie

_SDK_ORIGIN = "moderate.input"
_SDK_ORIGIN_OUTPUT = "moderate.output"
# Terminal inbound states that carry a verdict.
_TERMINAL_VERDICT = {"completed", "inbound_blocked"}
# The API's upper bound for a job's relative attachments budget.
_MAX_ATTACHMENTS_BUDGET_S = 90.0
# Terminal states that mean the job died without a usable verdict.
_TERMINAL_FAILURE = {"failed", "expired"}
# The outbound sets — identical to protect_buffered's, because a
# message_output-only job is the same job shape.
_TERMINAL_OUTPUT_VERDICT = frozenset({"completed", "outbound_blocked"})
_TERMINAL_OUTPUT_FAILURE = frozenset({"failed", "expired"})


def _assert_json_safe_context(value: Any, path: str = "context") -> None:
    """Reject context that wouldn't serialize to faithful, deterministic JSON.

    Guards the cross-SDK boundary and the stable
    JSON-Pointer attribution Slice 1 relies on: only JSON data is allowed — a
    ``dict`` with **string** keys, ``list``/``tuple``, ``str``, finite
    ``int``/``float``, ``bool``, ``None``. Non-string dict keys (which ``json``
    would silently coerce, e.g. ``1`` -> ``"1"``, ``None`` -> ``"null"``),
    non-finite floats, and any other object type raise ``ValueError``.
    """
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):  # bool is a subclass of int — handled above
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"{path} contains a non-finite number (NaN/Infinity), "
                "which is not valid JSON"
            )
        return
    if isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            _assert_json_safe_context(item, f"{path}[{i}]")
        return
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(
                    f"{path} has a non-string key {k!r} ({type(k).__name__}); JSON "
                    "object keys must be strings — silent coercion would corrupt "
                    "context attribution"
                )
            _assert_json_safe_context(v, f"{path}.{k}")
        return
    raise ValueError(
        f"{path} contains an unsupported type ({type(value).__name__}); context "
        "must be JSON data (dict with string keys / list / str / int / float / "
        "bool / None)"
    )


class ModerationClient:
    def __init__(self, client: "AsyncCollie") -> None:
        self._c = client

    async def input(
        self,
        prompt: Optional[str] = None,
        *,
        context: Any = None,
        context_format: Optional[str] = None,
        project_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        poll_interval_s: float = 0.05,
        timeout_s: float = 30.0,
        attachments: Optional[Sequence[str]] = None,
    ) -> InputModerationResult:
        """Check ``prompt`` against the project's input rules.

        Returns an :class:`InputModerationResult`. A policy block is a normal
        result (``blocked=True``), not an exception. Raises
        :class:`ModerationError` if the job dies (failed/expired) or polling
        times out. ``project_id`` is validated against the client's project (the
        API key scopes the project); it is not sent in the body.

        ``context`` is the structured data (``dict``/``list``) or raw string a
        customer assembles alongside the prompt; it is sent as a second input
        surface. A structured value travels as
        JSON; ``context_format`` (``"auto"``/``"json"``/``"text"``) is a parsing
        hint for a *raw string* and is omitted when unset so the policy default
        applies. Until context analysis is enabled server-side, context is
        accepted but inert, and this result is unchanged.

        ``attachments`` (preview) are ids of files uploaded with
        ``collie.files.upload``; they are checked with the prompt, and the
        result's ``file_result`` summarizes them. ``prompt`` may be omitted
        when there are attachments. With attachments the request carries the
        moment this call gives up (``client_deadline_at``, from the request
        timeout plus ``timeout_s``) and the client's clock (``client_now``);
        with no request timeout (``timeout=None``, or an ``httpx.Timeout``
        without a read timeout) it carries neither, only ``timeout_s`` as the
        files' budget (``attachments_budget_s``, at most 90 s).
        Raises :class:`FileAnalysisUnavailable` when the deployment does not
        analyze files, and :class:`ModerationTimeout` (a
        :class:`ModerationError`) when ``timeout_s`` runs out.
        """
        self._c._check_project(project_id)
        return await self._run(
            prompt,
            context=context,
            context_format=context_format,
            conversation_id=conversation_id,
            correlation_id=correlation_id,
            sdk_origin=_SDK_ORIGIN,
            poll_interval_s=poll_interval_s,
            timeout_s=timeout_s,
            attachments=attachments,
        )

    async def output(
        self,
        response: str,
        *,
        project_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        poll_interval_s: float = 0.05,
        timeout_s: float = 30.0,
    ) -> OutputModerationResult:
        """Check ``response`` (assistant/LLM output) against the project's
        OUTPUT rules — the standalone check for text produced outside a
        ``protect_*`` wrapper: proactive notifications, escalation messages,
        any side channel. Routing such text through
        ``moderate.input`` evaluates it with INBOUND rules: output-safety and
        masking rules silently never run, and injection detectors false-block
        assistant-style imperatives.

        A policy block is a normal result (``blocked=True``), not an
        exception. ``filtered_text`` carries the MASKED output — send it, not
        the original. Raises :class:`ModerationError` if the job dies
        (failed/expired) or polling times out.

        No ``context``/``context_format`` parameters: context is an input
        surface (with context analysis enabled server-side, an outbound-only
        job carrying one is rejected with 400). Context-aware output
        filtering is ``protect_buffered`` / ``protect_stream``, where the
        input pass carries it.
        """
        self._c._check_project(project_id)
        # Knob validation BEFORE the job is created (fail fast, no side effects).
        PollPacer.validate(poll_interval_s, timeout_s)
        started = self._c._monotonic()
        # Omit empty strings, not just None: at the SDK boundary "" == unset.
        body: Dict[str, Any] = {"message_output": response}
        if conversation_id:
            body["conversation_id"] = conversation_id
        if correlation_id:
            body["correlation_id"] = correlation_id

        data, resp = await self._c._request_json(
            "POST", "/v1/jobs", json=body, sdk_origin=_SDK_ORIGIN_OUTPUT
        )
        job_id = data.get("job_id")
        request_id = self._c._request_id(resp)
        if not job_id:
            raise CollieAPIError(
                "Job creation did not return a job_id", code="invalid_response"
            )

        try:
            job = await self._c._poll_job(
                job_id,
                sdk_origin=_SDK_ORIGIN_OUTPUT,
                terminal=_TERMINAL_OUTPUT_VERDICT,
                failure=_TERMINAL_OUTPUT_FAILURE,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
            )
        except CollieAPIError as exc:
            # Keep moderate.input's error taxonomy: a job that died or a poll
            # that timed out is a ModerationError on BOTH moderation methods,
            # so one `except ModerationError` covers the pair. Everything
            # else (transport, HTTP, malformed bodies) stays typed as-is.
            if exc.code == "poll_timeout":
                raise ModerationTimeout(
                    f"moderate.output timed out after {timeout_s}s "
                    f"waiting for job {job_id}",
                    job_id=job_id,
                ) from exc
            if exc.code == "job_failed":
                # Keep the actual terminal status in the message (input
                # parity): _poll_job's text is "ended in terminal state 'X'".
                raise ModerationError(f"{exc} before an output verdict") from exc
            raise
        result = self._to_output_result(response, job_id, request_id, job)
        self._c._debug(
            "moderate.output",
            job_id=job_id,
            request_id=result.request_id,
            blocked=result.blocked,
            latency_ms=round((self._c._monotonic() - started) * 1000, 1),
            conversation_id=conversation_id,
            correlation_id=correlation_id,
            triggered_rule_ids=[
                r.rule_id for r in result.triggered_rules if r.rule_id
            ],
        )
        return result

    @staticmethod
    def _to_output_result(
        response: str,
        job_id: str,
        request_id: Optional[str],
        jdata: Dict[str, Any],
    ) -> OutputModerationResult:
        """The v24 fail-safe verdict table applied to ``outbound_result``
        (sdk-plan § Output Moderation): a terminal job without its result
        object is malformed (typed error, never an implicit allow); ALLOWED
        requires an EXPLICIT ``allowed is True`` and no hard-block signal;
        ambiguity resolves to blocked."""
        try:
            outbound = jdata.get("outbound_result")
            if not isinstance(outbound, dict):
                raise ValueError("missing or non-object outbound_result")
            blocked = bool(outbound.get("blocked"))
            triggered = [
                TriggeredRule.model_validate(r)
                for r in (outbound.get("triggered_rules") or [])
            ]
            status_blocked = jdata.get("status") == "outbound_blocked"
            outbound_allowed = outbound.get("allowed")
            hard_blocked = (
                status_blocked or blocked or outbound_allowed is False
            )
            allowed = outbound_allowed is True and not hard_blocked
            return OutputModerationResult(
                allowed=allowed,
                blocked=not allowed,
                block_message=outbound.get("block_message"),
                original_text=response,
                filtered_text=outbound.get("filtered_content"),
                triggered_rules=triggered,
                job_id=job_id,
                request_id=request_id,
            )
        except CollieError:
            raise
        except Exception as exc:
            raise CollieAPIError(
                "API returned a malformed moderation result",
                code="invalid_response",
            ) from exc

    async def _run(
        self,
        prompt: Optional[str],
        *,
        context: Any = None,
        context_format: Optional[str] = None,
        conversation_id: Optional[str],
        correlation_id: Optional[str],
        sdk_origin: str,
        poll_interval_s: float = 0.05,
        timeout_s: float = 30.0,
        attachments: Optional[Sequence[str]] = None,
    ) -> InputModerationResult:
        """Shared input-check transport. ``sdk_origin`` attributes the requests
        to the public method that triggered them — ``moderate.input`` directly,
        or ``protect_stream`` / ``protect_buffered`` when called from a wrapper."""
        # Knob validation BEFORE the job is created (fail fast, no side effects).
        PollPacer.validate(poll_interval_s, timeout_s)
        attachments = _validate_attachments(prompt, attachments)
        started = self._c._monotonic()
        # Omit empty strings, not just None: at the SDK boundary "" == unset.
        body: Dict[str, Any] = (
            {"message_input": prompt, "inbound_only": True} if prompt is not None else {"inbound_only": True}
        )
        if attachments:
            # File analysis: the files, and the moment this call
            # gives up — computed BEFORE the POST from the request timeout plus
            # the poll budget (which starts after it), with the client's clock
            # so the server can correct the deadline for skew. An unbounded
            # request has no such moment; the poll budget is still known, and
            # it starts no earlier than the server's receipt — sent as the
            # relative budget instead of the server's fixed default.
            body["attachments"] = [{"file_id": f} for f in attachments]
            bound_s = self._c._request_bound_s()
            if bound_s is not None:
                now = self._c._utcnow()
                body["client_now"] = now.isoformat()
                body["client_deadline_at"] = (now + timedelta(seconds=bound_s + timeout_s)).isoformat()
            else:
                body["attachments_budget_s"] = min(timeout_s, _MAX_ATTACHMENTS_BUDGET_S)
        # Context-analysis input surface. A
        # structured value (dict/list) goes on the wire as JSON automatically;
        # a raw string is sent as-is. `is not None` (not truthiness) so an
        # explicit empty dict/list/string is still sent. context_format is sent
        # only when set, so an omitted value falls back to the policy default.
        if context is not None:
            # Reject anything that wouldn't serialize to faithful, deterministic
            # JSON — non-finite numbers, non-string dict keys (json would coerce
            # 1 -> "1", silently corrupting Slice-1 JSON-Pointer attribution),
            # and unsupported Python objects. Same strict contract as the other
            # SDKs.
            _assert_json_safe_context(context)
            body["context"] = context
        if context_format:
            body["context_format"] = context_format
        if conversation_id:
            body["conversation_id"] = conversation_id
        if correlation_id:
            body["correlation_id"] = correlation_id

        data, resp = await self._c._request_json(
            "POST", "/v1/jobs", json=body, sdk_origin=sdk_origin
        )
        job_id = data.get("job_id")
        request_id = self._c._request_id(resp)
        if not job_id:
            raise CollieAPIError(
                "Job creation did not return a job_id", code="invalid_response"
            )

        # Pacing per the Poll Backoff Contract (see _pacing.PollPacer). The
        # budget starts AFTER job creation, matching the prior deadline
        # semantics. Flow-specific errors stay here; the pacer owns timing.
        pacer = PollPacer(
            interval_s=poll_interval_s,
            timeout_s=timeout_s,
            monotonic=self._c._monotonic,
            rand=self._c._rand,
        )

        def _timeout_error() -> ModerationError:
            return ModerationTimeout(
                f"moderate.input timed out after {timeout_s}s waiting for job {job_id}",
                job_id=job_id,
            )

        while True:
            remaining = pacer.remaining()
            if remaining <= 0:
                raise _timeout_error()
            # Cooperative wall-clock bound on the in-flight GET (PBC-08: a suppressing transport can outrun it; PBC-07 no-late-result is the hard guarantee) (see
            # _client._poll_job: httpx timeouts are per-phase and cannot cap
            # total request time). Only this bound firing is this flow's
            # poll-timeout; transport failures stay CollieConnectionError.
            try:
                jresp, jdata = await asyncio.wait_for(
                    self._c._poll_request(
                        f"/v1/jobs/{job_id}", sdk_origin=sdk_origin
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError as exc:
                raise _timeout_error() from exc
            except CollieAPIError as exc:
                # Late-landing typed HTTP errors are rejected by the strict
                # boundary too (see _client._poll_job).
                if pacer.remaining() <= 0:
                    raise _timeout_error() from exc
                raise
            # Strict acceptance boundary (v5 — see _client._poll_job): a
            # response landing at/after the deadline is rejected, never
            # accepted late.
            if pacer.remaining() <= 0:
                raise _timeout_error()
            if jdata is None:  # 429 — pace, continue, or surface the typed 429
                sleep_s = pacer.sleep_for_429(self._c._parse_retry_after(jresp))
                if sleep_s is None:
                    raise self._c._api_error(jresp)
                await self._c._sleep(sleep_s)
                continue
            status = jdata.get("status")
            if status in _TERMINAL_VERDICT:
                result = self._to_result(
                    prompt, job_id, request_id or self._c._request_id(jresp), jdata,
                    attachments_sent=bool(attachments),
                )
                self._c._debug(
                    "moderate.input",
                    job_id=job_id,
                    request_id=result.request_id,
                    blocked=result.blocked,
                    latency_ms=round((self._c._monotonic() - started) * 1000, 1),
                    conversation_id=conversation_id,
                    correlation_id=correlation_id,
                    triggered_rule_ids=[
                        r.rule_id for r in result.triggered_rules if r.rule_id
                    ],
                )
                return result
            if status in _TERMINAL_FAILURE:
                raise ModerationError(
                    f"Job {job_id} ended in terminal state '{status}' before an input verdict"
                )
            await self._c._sleep(
                pacer.next_sleep(suggested_poll_ms=jdata.get("suggested_poll_ms"))
            )

    @staticmethod
    def _to_result(
        prompt: Optional[str],
        job_id: str,
        request_id: Optional[str],
        jdata: Dict[str, Any],
        *,
        attachments_sent: bool = False,
    ) -> InputModerationResult:
        # A terminal job with a malformed inbound_result / triggered_rules must
        # surface as a typed error, not a raw AttributeError / ValidationError.
        try:
            inbound = jdata.get("inbound_result")
            if not isinstance(inbound, dict):
                # A terminal job must carry its result object; a missing/None
                # inbound_result is malformed, not an implicit "allowed".
                raise ValueError("missing or non-object inbound_result")
            blocked = bool(inbound.get("blocked"))
            triggered = [
                TriggeredRule.model_validate(r)
                for r in (inbound.get("triggered_rules") or [])
            ]
            # Context analysis (§4.1, §5.5): context_result + blocked_by live on
            # the job-status root, not inside inbound_result. None on a job
            # without context analysis.
            context_raw = jdata.get("context_result")
            context = (
                ContextModerationResult.model_validate(context_raw)
                if isinstance(context_raw, dict)
                else None
            )
            # FAIL-SAFE verdict resolution (v24 — cross-SDK parity with the .NET
            # v23 fix). A wrapper gating on `blocked` must never fail-open on an
            # ambiguous/empty inbound_result or a root context block. HARD-BLOCKED
            # if ANY of: the terminal status says so, inbound.blocked,
            # inbound.allowed is explicitly False, OR a context_result block (a
            # context block gates provider spend too; it lives on the job-status
            # root, not inside inbound_result). ALLOWED requires an EXPLICIT
            # allowed is True AND not hard-blocked — an ABSENT allowed no longer
            # defaults to allow (the old `inbound.get("allowed", not blocked)`
            # let a completed job with an empty inbound_result fail-open). `blocked`
            # is the strict complement so an ambiguous verdict surfaces blocked=True.
            status_blocked = jdata.get("status") == "inbound_blocked"
            context_blocked = context is not None and context.blocked
            inbound_allowed = inbound.get("allowed")
            hard_blocked = (
                status_blocked or blocked or inbound_allowed is False or context_blocked
            )
            allowed = inbound_allowed is True and not hard_blocked
            # Surface the context-block reason when the inbound result carries no
            # message of its own (finding 6 parity): a context block otherwise
            # leaves block_message None and the caller shows nothing.
            block_message = inbound.get("block_message")
            if block_message is None and context_blocked:
                block_message = context.block_message
            file_result = jdata.get("file_result")
            if attachments_sent and not isinstance(file_result, dict):
                # The server finished the input check without analyzing the
                # files — one that does not know attachments ignores them.
                raise FileAnalysisUnavailable(
                    f"job {job_id} finished its input check without a file_result",
                    code="file_result_missing",
                )
            return InputModerationResult(
                allowed=allowed,
                blocked=not allowed,
                block_message=block_message,
                original_text=prompt or "",
                filtered_text=inbound.get("filtered_content"),
                triggered_rules=triggered,
                job_id=job_id,
                request_id=request_id,
                blocked_by=jdata.get("blocked_by"),
                context=context,
                file_result=file_result if isinstance(file_result, dict) else None,
            )
        except CollieError:
            raise
        except Exception as exc:
            raise CollieAPIError(
                "API returned a malformed moderation result",
                code="invalid_response",
            ) from exc


def _validate_attachments(prompt: Optional[str], attachments: Optional[Sequence[str]]) -> list:
    """Refused locally, before any request: no prompt and no attachments, a
    non-string or empty file id. Returns a copy: the caller's list may change
    while the call runs."""
    files = list(attachments or ())
    if isinstance(attachments, str):
        raise TypeError("attachments must be a list of file ids, not a string")
    if prompt is None and not files:
        raise ValueError("moderate.input needs a prompt, attachments, or both")
    if any(not isinstance(f, str) or not f for f in files):
        raise ValueError("attachments must be non-empty file id strings")
    return files
