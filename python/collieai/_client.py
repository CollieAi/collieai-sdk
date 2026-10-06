"""The async CollieAi client and its HTTP/retry engine.

One shared ``httpx.AsyncClient`` is reused for keep-alive across all calls.
Chunk submission goes through ``_post_chunk``, which owns the retry +
idempotency policy of the SDK contract ("Retry and Idempotency"): retry the exact same sequence on transient failures with
exponential backoff + jitter, bounded by an attempt budget and a wall-clock
ceiling, and map every wire error code to a typed exception.
"""
from __future__ import annotations

import asyncio
import math
import random
import time
import warnings
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import httpx

from ._pacing import PollPacer
from ._version import __version__
from .errors import (
    ChunkConcurrentSubmit,
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
)
from .moderation import ModerationClient
from .streaming import StreamingClient

DEFAULT_BASE_URL = "https://app.collieai.io"
DEFAULT_MAX_CONNECTIONS = 100

# Highest X-CollieAi-Streaming-Protocol version this SDK understands (F9).
# The backend stamps its version on streaming-surface responses; if the
# server is newer, the SDK warns once per client instead of failing.
SUPPORTED_STREAMING_PROTOCOL = 1

# Transport-timeout exception classes, shared by ALL request primitives. Both
# builtin TimeoutError AND asyncio.TimeoutError are included: they are a DISTINCT
# class before Python 3.11 (the SAME class from 3.11 on), and the SDK supports
# >=3.9. Catching only the builtin let a transport-raised asyncio.TimeoutError
# escape on 3.9/3.10 — raw from create, and misread as the deadline by the poll
# loop's outer wait_for on the status GET.
_TRANSPORT_TIMEOUT_EXC = (
    httpx.TimeoutException,
    httpx.TransportError,
    TimeoutError,
    asyncio.TimeoutError,
)

# Wire error code -> fatal exception class (raised immediately, no retry).
_FATAL_CHUNK_CODES: Dict[str, type] = {
    "chunk_session_finished": ChunkSessionFinished,
    "chunk_sequence_conflict": ChunkSequenceConflict,
    "chunk_idempotency_conflict": ChunkIdempotencyConflict,
    "chunk_session_unrecoverable": ChunkSessionUnrecoverable,
    "chunk_policy_changed": ChunkPolicyChanged,
    "chunk_streaming_unsupported": ChunkStreamingUnsupported,
}


@dataclass
class _RetryDecision:
    fatal: bool
    exc: Exception
    retry_after: Optional[float] = None  # explicit wait (seconds) for 429


class AsyncCollie:
    """Async client for the CollieAi API.

    ``project_id`` is the project the API key is scoped to. It is accepted for
    API-shape parity (and forwarded to ``streaming.preflight`` in a later
    slice); job creation derives the project from the API key, so it is not
    sent in the job body.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        project_id: Optional[str] = None,
        *,
        timeout: float = 30.0,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        limits: Optional[httpx.Limits] = None,
        transport: Optional[httpx.BaseTransport] = None,
        max_retries: int = 3,
        retry_max_per_chunk_s: float = 10.0,
        base_backoff_s: float = 0.25,
        max_backoff_s: float = 4.0,
        debug_logger: Optional[Any] = None,
        # Test seams — injectable clock/sleep/rng so retry and poll-pacing
        # tests stay deterministic.
        _sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        _monotonic: Optional[Callable[[], float]] = None,
        _rand: Optional[Callable[[], float]] = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url
        self.project_id = project_id

        # Numeric RETRY knobs validated up front (v24; v25 hardening). A bad value
        # must fail at construction, not deep inside a chunk submit (max_retries=0
        # looped forever; a negative ceiling/backoff had nonsense semantics).
        # `timeout` is DELIBERATELY not validated here — it is the httpx transport
        # timeout and must pass through untouched, including `None` (no timeout)
        # and an `httpx.Timeout(...)` object (v25 — the v24 numeric check wrongly
        # rejected both).
        def _finite_positive(value: Any) -> bool:
            # bool is an int subclass — reject it. A huge Python int overflows the
            # float() conversion `math.isfinite` performs, so guard it: an
            # un-representable magnitude is treated as non-finite (invalid), never
            # a raw OverflowError.
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return False
            try:
                return math.isfinite(float(value)) and value > 0
            except (OverflowError, ValueError):
                return False

        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 1:
            raise ValueError(f"max_retries must be an integer >= 1, got {max_retries!r}")
        for _name, _val in (
            ("retry_max_per_chunk_s", retry_max_per_chunk_s),
            ("base_backoff_s", base_backoff_s),
            ("max_backoff_s", max_backoff_s),
        ):
            if not _finite_positive(_val):
                raise ValueError(f"{_name} must be a finite number > 0, got {_val!r}")
        if max_backoff_s < base_backoff_s:
            raise ValueError(
                f"max_backoff_s ({max_backoff_s}) must be >= base_backoff_s ({base_backoff_s})"
            )

        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_ceiling_s = retry_max_per_chunk_s
        self._base_backoff = base_backoff_s
        self._max_backoff = max_backoff_s
        self._debug_logger = debug_logger
        self._sleep = _sleep or asyncio.sleep
        self._monotonic = _monotonic or time.monotonic
        self._rand = _rand or random.random
        self._protocol_warned = False

        if limits is None:
            limits = httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            )
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": f"collieai-python/{__version__}",
            },
            timeout=timeout,
            limits=limits,
            transport=transport,
        )

        self.moderate = ModerationClient(self)
        self.streaming = StreamingClient(self)

    # -- lifecycle -----------------------------------------------------------
    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncCollie":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # -- low-level helpers (used by sub-clients) -----------------------------
    def _debug(self, event: str, **fields: Any) -> None:
        """Emit one structured debug record (no-op without ``debug_logger``).

        Events: ``chunk`` (per accepted chunk: job_id, request_id, sequence,
        chunk_chars, emit_chars, blocked, finished, retry_count, latency_ms,
        conversation_id, correlation_id, triggered_rule_ids), ``chunk.retry``
        (attempt, delay_s, sequence), ``moderate.input`` (job_id, request_id,
        blocked, latency_ms, ids, triggered_rule_ids), ``preflight``
        (project_id, mode, fallback_reason, cached). Only safe emits and
        policy metadata are logged — never raw provider text.

        Best-effort: a customer logger that raises must never fail an SDK
        call that already succeeded on the wire, so exceptions are swallowed.
        """
        if self._debug_logger is None:
            return
        try:
            self._debug_logger.debug(f"collieai {event}", extra=fields)
        except Exception:
            pass

    def _check_protocol(self, resp: httpx.Response) -> None:
        """Warn once per client when the server speaks a newer streaming
        protocol than this SDK was built for (``X-CollieAi-Streaming-Protocol``,
        F9). Missing or malformed header values are ignored — older servers
        don't send it."""
        if self._protocol_warned:
            return
        raw = resp.headers.get("x-collieai-streaming-protocol")
        if raw is None:
            return
        try:
            server = int(raw)
        except (TypeError, ValueError):
            return
        if server > SUPPORTED_STREAMING_PROTOCOL:
            self._protocol_warned = True
            warnings.warn(
                f"CollieAi server speaks streaming protocol {server}; this "
                f"collieai SDK supports up to {SUPPORTED_STREAMING_PROTOCOL}. "
                "Upgrade the collieai package to avoid contract drift.",
                UserWarning,
                stacklevel=4,
            )

    @staticmethod
    def _request_id(resp: httpx.Response) -> Optional[str]:
        return resp.headers.get("x-request-id") or None

    @staticmethod
    def _parse_error(resp: httpx.Response) -> Tuple[Optional[str], Optional[str]]:
        """Return (wire_code, message) from the OpenAI-compatible error envelope."""
        try:
            body = resp.json()
        except Exception:
            return None, None
        if not isinstance(body, dict):
            return None, None
        err = body.get("error")
        if not isinstance(err, dict):
            return None, None
        return (err.get("type") or err.get("code")), err.get("message")

    def _api_error(self, resp: httpx.Response) -> CollieAPIError:
        code, message = self._parse_error(resp)
        return CollieAPIError(
            message or f"HTTP {resp.status_code}",
            status_code=resp.status_code,
            code=code,
        )

    @staticmethod
    def _json_or_error(resp: httpx.Response) -> Any:
        """Parse a (2xx) response body or raise a typed CollieAPIError instead
        of letting a raw JSON-decode error escape."""
        try:
            return resp.json()
        except Exception as exc:  # noqa: BLE001 — any decode failure is one class of error to the caller
            raise CollieAPIError(
                "API returned a malformed (non-JSON) response",
                status_code=resp.status_code,
                code="invalid_response",
            ) from exc

    def _check_project(self, project_id: Optional[str]) -> None:
        """Validate a per-call project_id. The API key scopes the project and
        job creation does not send a project_id, so a per-call value is only
        meaningful as a consistency check against the client's project. Reject
        anything we can't honor rather than silently ignore it."""
        if project_id is None:
            return
        if self.project_id is None:
            raise ValueError(
                "project_id was passed per-call but the client was constructed "
                "without one, so it can't be validated or applied — the API key "
                "determines the project. Set project_id on AsyncCollie(...)."
            )
        if project_id != self.project_id:
            raise ValueError(
                f"project_id={project_id!r} does not match the client's "
                f"project_id={self.project_id!r}; the API key determines the "
                f"project, so these must agree (or omit the argument)."
            )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        sdk_origin: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], httpx.Response]:
        """Single request (no chunk-retry). Raises a typed CollieError on
        transport failure, non-2xx status, or a malformed body — never a raw
        httpx/JSON exception."""
        try:
            resp = await self._client.request(
                method,
                url,
                json=json,
                params=params,
                headers={"X-CollieAi-SDK-Origin": sdk_origin},
            )
        except _TRANSPORT_TIMEOUT_EXC as exc:
            # A transport-raised timeout (builtin OR asyncio.TimeoutError — see
            # _TRANSPORT_TIMEOUT_EXC) is a transport failure, not the deadline;
            # normalized here so a poll loop's outer wait_for catch never misreads
            # it as its own timeout (v28: builtin-only escaped on 3.9/3.10).
            raise CollieConnectionError(f"Request to {url} failed: {exc}") from exc
        self._check_protocol(resp)
        if resp.status_code >= 400:
            raise self._api_error(resp)
        if not resp.content:
            # Every endpoint reached through _request_json (job create, job
            # status poll) must return a JSON object. An empty 2xx body is
            # malformed — fail fast as a typed error instead of returning {},
            # which would poll until timeout or look like a "missing job_id".
            raise CollieAPIError(
                "API returned an empty body where a JSON object was expected",
                status_code=resp.status_code,
                code="invalid_response",
            )
        data = self._json_or_error(resp)
        if not isinstance(data, dict):
            # Valid JSON, wrong shape (e.g. a bare array/string). Fail fast as a
            # typed error rather than coercing to {} — coercion would make a
            # poller wait until timeout instead of surfacing the problem.
            raise CollieAPIError(
                "API returned a non-object JSON body",
                status_code=resp.status_code,
                code="invalid_response",
            )
        return data, resp

    async def _poll_request(
        self,
        url: str,
        *,
        sdk_origin: str,
    ) -> Tuple[httpx.Response, Optional[Dict[str, Any]]]:
        """Status-poll transport primitive (Poll Backoff Contract).

        Returns ``(response, body_dict)`` for 2xx and ``(response, None)`` for
        429 — the poll loop owns the 429 pacing decision and needs the
        ``Retry-After`` header, which the ordinary error path discards. Any
        other >=400 raises the usual typed error; transport failures raise
        ``CollieConnectionError``. The client's configured transport timeout
        applies untouched; the poll loop bounds the CALL to this primitive
        with a COOPERATIVE wall-clock deadline (``asyncio.wait_for``; PBC-08) — httpx
        timeouts are per-phase and cannot cap total request time.
        """
        try:
            resp = await self._client.request(
                "GET",
                url,
                headers={"X-CollieAi-SDK-Origin": sdk_origin},
            )
        except _TRANSPORT_TIMEOUT_EXC as exc:
            # Transport timeout (builtin OR asyncio.TimeoutError) normalized to a
            # connection error INSIDE the primitive — before the outer wait_for —
            # so the loop's `except asyncio.TimeoutError` means ONLY its own
            # deadline (v28: a transport asyncio.TimeoutError was misread as
            # poll-timeout on 3.9/3.10 where the classes differ).
            raise CollieConnectionError(f"Request to {url} failed: {exc}") from exc
        self._check_protocol(resp)
        if resp.status_code == 429:
            return resp, None
        if resp.status_code >= 400:
            raise self._api_error(resp)
        if not resp.content:
            raise CollieAPIError(
                "API returned an empty body where a JSON object was expected",
                status_code=resp.status_code,
                code="invalid_response",
            )
        data = self._json_or_error(resp)
        if not isinstance(data, dict):
            raise CollieAPIError(
                "API returned a non-object JSON body",
                status_code=resp.status_code,
                code="invalid_response",
            )
        return resp, data

    async def _poll_job(
        self,
        job_id: str,
        *,
        sdk_origin: str,
        terminal: "frozenset[str]",
        failure: "frozenset[str]",
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.05,
    ) -> Dict[str, Any]:
        """Poll GET /v1/jobs/{job_id} until a terminal status, returning the
        job body. Raises CollieAPIError on a failure status or on timeout.

        Pacing per the Poll Backoff Contract (see ``_pacing.PollPacer``): a
        literal warm phase, a jittered doubling ramp, ``suggested_poll_ms``
        server pacing, a respectful 429 contract, and a true wall-clock
        budget.
        """
        pacer = PollPacer(
            interval_s=poll_interval_s,
            timeout_s=timeout_s,
            monotonic=self._monotonic,
            rand=self._rand,
        )

        def _timeout_error() -> CollieAPIError:
            return CollieAPIError(
                f"Polling job {job_id} timed out after {timeout_s}s",
                code="poll_timeout",
            )

        while True:
            remaining = pacer.remaining()
            if remaining <= 0:
                raise _timeout_error()
            # The poll budget is absolute wall clock: the in-flight GET is
            # bounded by cancellation at the deadline (httpx timeouts are
            # per-phase — connect/read/write/pool — and cannot cap total
            # request time; the client's configured transport timeout still
            # applies untouched underneath). Taxonomy is structural: only
            # this bound firing is the flow's poll-timeout; transport
            # failures stay CollieConnectionError regardless of the clock;
            # caller cancellation propagates as CancelledError.
            try:
                resp, data = await asyncio.wait_for(
                    self._poll_request(
                        f"/v1/jobs/{job_id}", sdk_origin=sdk_origin
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError as exc:
                raise _timeout_error() from exc
            except CollieAPIError as exc:
                # Strict acceptance applies to EVERY late-landing HTTP
                # response: a typed HTTP error (4xx/5xx, malformed body)
                # arriving at/after the deadline is this flow's poll-timeout
                # too. Connection errors propagate untouched — transport
                # taxonomy is clock-independent.
                if pacer.remaining() <= 0:
                    raise _timeout_error() from exc
                raise
            # Strict acceptance boundary (v5): wait_for's guarantee is not
            # unconditional — a task that suppresses (or outruns) the
            # cancellation returns its result on 3.9-3.11 — so a response
            # landing at/after the deadline is rejected, never accepted late.
            if pacer.remaining() <= 0:
                raise _timeout_error()
            if data is None:  # 429 — pace, continue, or surface
                sleep_s = pacer.sleep_for_429(self._parse_retry_after(resp))
                if sleep_s is None:
                    raise self._api_error(resp)
                await self._sleep(sleep_s)
                continue
            status = data.get("status")
            if status in terminal:
                return data
            if status in failure:
                raise CollieAPIError(
                    f"Job {job_id} ended in terminal state '{status}'",
                    code="job_failed",
                )
            await self._sleep(
                pacer.next_sleep(suggested_poll_ms=data.get("suggested_poll_ms"))
            )

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with equal-jitter (attempt is 1-based)."""
        raw = min(self._max_backoff, self._base_backoff * (2 ** (attempt - 1)))
        return raw * 0.5 + random.uniform(0, raw * 0.5)

    @staticmethod
    def _parse_retry_after(resp: httpx.Response) -> Optional[float]:
        """RFC 9110 delay-seconds: a non-negative decimal integer (1*DIGIT),
        strictly. ``float()`` would admit ``1.5``, ``1e2``, ``Infinity`` and
        diverge from the other SDKs; the HTTP-date form and anything malformed
        are unusable and take the headerless (pressure-floor) path."""
        value = resp.headers.get("retry-after")
        if value is None:
            return None
        if not (value.isascii() and value.isdigit()):
            return None
        return float(value)

    def _classify_chunk_error(self, resp: httpx.Response) -> _RetryDecision:
        code, message = self._parse_error(resp)
        status = resp.status_code

        if status == 429:
            retry_after = self._parse_retry_after(resp)
            if retry_after is not None and retry_after <= self._retry_ceiling_s:
                return _RetryDecision(
                    fatal=False,
                    exc=ChunkQuotaExceeded(message or "rate limited", status_code=429, code=code),
                    retry_after=retry_after,
                )
            return _RetryDecision(
                fatal=True,
                exc=ChunkQuotaExceeded(
                    message or "rate limited (no usable Retry-After)",
                    status_code=429,
                    code=code,
                ),
            )

        fatal_cls = _FATAL_CHUNK_CODES.get(code or "")
        if fatal_cls is not None:
            return _RetryDecision(
                fatal=True,
                exc=fatal_cls(message or code or "chunk error", status_code=status, code=code),
            )

        # Retryable: server-side in-flight collision, or any 5xx (timeout /
        # persistence blip). Retried with the SAME sequence + content.
        if code == "chunk_concurrent_submit":
            return _RetryDecision(
                fatal=False,
                exc=ChunkConcurrentSubmit(message or "concurrent submit", status_code=status, code=code),
            )
        if code == "chunk_resolution_unavailable":
            # 503: a resolver dependency was unreachable — an outage, not a
            # policy shape. Same retry treatment as any 5xx, but typed so the
            # exhaustion cause names the condition. Keyed on the code, not the
            # status, like `chunk_concurrent_submit` above.
            return _RetryDecision(
                fatal=False,
                exc=ChunkResolutionUnavailable(
                    message or "policy resolution unavailable", status_code=status, code=code
                ),
            )
        if status in (502, 503, 504):
            return _RetryDecision(
                fatal=False,
                exc=CollieAPIError(message or f"HTTP {status}", status_code=status, code=code),
            )

        return _RetryDecision(fatal=True, exc=self._api_error(resp))

    async def _post_chunk(
        self,
        url: str,
        body: Dict[str, Any],
        *,
        sdk_origin: str,
    ) -> Tuple[httpx.Response, int]:
        """POST a chunk with retry+idempotency. Returns ``(response, attempts)``
        for the first 2xx response (attempts is 1-based, so retries =
        attempts - 1), or raises a typed error. The same ``body`` (sequence +
        content) is sent on every attempt so the server's idempotent replay
        returns the cached result."""
        attempts = 0
        start = self._monotonic()
        last_error: Optional[Exception] = None
        while True:
            attempts += 1
            # STRICT ceiling as a TOTAL wall-clock budget across attempts (cooperatively bounded per attempt, PBC-12)
            # (v25). The whole POST — connect, send, and body — is bounded by
            # `asyncio.wait_for(remaining)`; the client's configured transport
            # timeout passes through UNTOUCHED underneath. httpx's `timeout=` is
            # per-phase (connect/read/write/pool applied separately) and cannot
            # cap total request time — the v24 `min(self._timeout, remaining)`
            # let a slow multi-phase POST outrun the ceiling and also clobbered
            # `timeout=None` / `httpx.Timeout(...)` configs. Check before the
            # attempt, and again after a 2xx so a late landing (e.g. a
            # virtual-clock jump with no real delay) is rejected, not returned.
            remaining = self._retry_ceiling_s - (self._monotonic() - start)
            if remaining <= 0:
                raise ChunkRetryExhausted(
                    f"Chunk retry ceiling ({self._retry_ceiling_s}s) exceeded",
                    code="chunk_retry_exhausted",
                ) from last_error
            async def _do_post() -> httpx.Response:
                try:
                    return await self._client.post(
                        url,
                        json=body,
                        headers={"X-CollieAi-SDK-Origin": sdk_origin},
                    )
                except (TimeoutError, asyncio.TimeoutError) as exc:
                    # A TRANSPORT-owned timeout — builtin TimeoutError OR
                    # asyncio.TimeoutError (a DISTINCT class before Python 3.11;
                    # the same class from 3.11 on) — must be normalized to an
                    # httpx transport timeout so the outer wait_for's
                    # `except asyncio.TimeoutError` means ONLY the ceiling fired
                    # and this attempt is RETRIED (v27: catching just the builtin
                    # let a transport asyncio.TimeoutError escape as ceiling
                    # exhaustion on 3.9/3.10). A wait_for cancellation injects
                    # CancelledError (a BaseException, not caught here), so this
                    # never swallows the ceiling.
                    raise httpx.TimeoutException(str(exc)) from exc

            try:
                resp = await asyncio.wait_for(_do_post(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                # The whole remaining budget was consumed by this one attempt —
                # the total ceiling is spent, so there is nothing left to retry.
                raise ChunkRetryExhausted(
                    f"Chunk retry ceiling ({self._retry_ceiling_s}s) exceeded",
                    code="chunk_retry_exhausted",
                ) from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                delay = self._backoff(attempts)
            else:
                self._check_protocol(resp)
                if resp.status_code < 400:
                    # A 2xx that LANDED at/after the ceiling is rejected (v24): a
                    # slow single attempt must not return a late success.
                    if (self._monotonic() - start) >= self._retry_ceiling_s:
                        raise ChunkRetryExhausted(
                            f"Chunk retry ceiling ({self._retry_ceiling_s}s) exceeded",
                            code="chunk_retry_exhausted",
                        ) from last_error
                    return resp, attempts
                decision = self._classify_chunk_error(resp)
                if decision.fatal:
                    raise decision.exc
                last_error = decision.exc
                delay = (
                    decision.retry_after
                    if decision.retry_after is not None
                    else self._backoff(attempts)
                )

            if attempts >= self._max_retries:
                raise ChunkRetryExhausted(
                    f"Chunk retry budget exhausted after {attempts} attempts",
                    code="chunk_retry_exhausted",
                ) from last_error
            if (self._monotonic() - start) + delay > self._retry_ceiling_s:
                raise ChunkRetryExhausted(
                    f"Chunk retry ceiling ({self._retry_ceiling_s}s) exceeded",
                    code="chunk_retry_exhausted",
                ) from last_error
            self._debug(
                "chunk.retry",
                attempt=attempts, delay_s=delay, sequence=body.get("sequence"),
            )
            await self._sleep(delay)
